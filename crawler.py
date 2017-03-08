import os.path
import asyncio
from peony import PeonyClient

from helpers import loadj, writej, append_listj

SEED_NAMES = ['dailykos', 'thinkprogress', 'HuffingtonPost', 'voxdotcom', 'nytimes', 'washingtonpost', 'politico', 'USATODAY', 'StephensWSJ', 'WSJ', 'arthurbrooks', 'EWErickson', 'nypost', 'BreitbartNews', 'RealAlexJones']
MAX_SEED_FOLLOWER_COUNT = 300000
MIN_SEED_FOLLOWER_COUNT = 20000
PREFIX = '/Users/fernand/theoutgroup/'

class ClientPool:
    def __init__(self, api_creds):
        self._index = 0
        self._clients = clients = [
            PeonyClient(consumer_key=creds['consumer_key'],
                        consumer_secret=creds['consumer_secret'],
                        access_token=creds['access_token'],
                        access_token_secret=creds['access_token_secret'])
            for creds in api_creds
        ]
        self.client_num = len(self._clients)

    def get_client(self):
        client = self._clients[self._index]

        self._index += 1
        if self._index >= self.client_num:
            self._index = 0

        return client

class FetchQueue:
    def __init__(self, num_consumers):
        self._queue = asyncio.Queue()

        # Start a bunch of queue consumers.
        self._consumers = []
        for i in range(num_consumers):
            self._consumers.append(asyncio.ensure_future(self._consume()))

    async def done(self):
        await self._queue.join()
        for c in self._consumers:
            c.cancel()
        return

    def enqueue(self, function, user_id, client):
        self._queue.put_nowait({
            'function': function,
            'user_id': user_id,
            'client': client
        })
        return

    async def _consume(self):
        while True:
            task = await self._queue.get()
            await task['function'](task['user_id'], task['client'])
            self._queue.task_done()


def get_api_creds():
    return loadj('api_creds.json')

async def get_user_by_name(screen_name, client):
    res = await client.api.users.lookup.get(screen_name=screen_name)
    user = res[0]
    file_path = PREFIX + user['id_str'] + '_user'
    writej(user, file_path, overwrite=False)
    return user

async def get_user_by_id(user_id, client):
    file_path = PREFIX + user_id + '_user'
    obj = loadj(file_path)
    if obj is not None:
        return obj

    res = await client.api.users.lookup.get(user_id=user_id)
    writej(res[0], file_path)
    return res[0]

async def get_friends(user_id, screen_name, client):
    friends_ids = client.api.friends.ids.get.iterator.with_cursor(
        user_id=user_id,
        count=3000
    )

    friends = []
    try:
        async for data in friends_ids:
            friends.extend([str(f) for f in data.ids])

        file_path = PREFIX + str(user_id) +'_'+ screen_name + '_friends'
        writej(friends, file_path, overwrite=False)
    except Exception as e:
        print(e)

    return friends

async def write_timeline(user_id, client):
    file_path = PREFIX + user_id + '_timeline'
    try:
        response = await client.api.statuses.user_timeline.get(
            id=user_id,
            exclude_replies=True,
            count=200
        )
    except Exception as e:
        print(e)
        return

    tweets = []
    for t in response:
        tweets.append(t)
    append_listj(tweets, file_path)
    return

async def write_followers(user_id, client):
    file_path = PREFIX + user_id + '_followers'
    if os.path.exists(file_path):
        return

    print(f'Getting followers for {user_id}...')
    followers_ids = client.api.followers.ids.get.iterator.with_cursor(
        id=user_id,
        count=MAX_SEED_FOLLOWER_COUNT
    )

    try:
        followers = []
        async for data in followers_ids:
            followers.extend(data.ids)

        writej(followers, file_path, overwrite=False)
    except Exception as e:
        print(e)

    print(f'Done getting followers for {user_id}.')
    return

async def process_seed(seed_name, pool, f_queue, t_queue):
    seed_user = await get_user_by_name(seed_name, pool.get_client())
    seed_friends = await get_friends(seed_user['id_str'], seed_name, pool.get_client())

    # Filtering also applies to seeds.
    seed_friends.append(seed_user['id_str'])

    friends_to_fetch = []
    for friend_id in seed_friends:
        friend_user = await get_user_by_id(friend_id, pool.get_client())
        if friend_user['followers_count'] >= MIN_SEED_FOLLOWER_COUNT and friend_user['followers_count'] <= MAX_SEED_FOLLOWER_COUNT:
            friends_to_fetch.append(friend_id)

    # Get all the friends' followers.
    print(f'{len(friends_to_fetch)} friends to fetch for {seed_name}.')
    for friend_id in friends_to_fetch:
        if f_queue is not None:
            f_queue.enqueue(write_followers, friend_id, pool.get_client())
        if t_queue is not None:
            t_queue.enqueue(write_timeline, friend_id, pool.get_client())

    return

async def run(fetch_followers=True, fetch_timelines=True):
    api_creds = get_api_creds()
    pool = ClientPool(api_creds)
    to_wait = []
    f_queue = None
    if fetch_followers:
        f_queue = FetchQueue(len(api_creds))
        to_wait.append(f_queue)
    t_queue = None
    if fetch_timelines:
        t_queue = FetchQueue(len(api_creds))
        to_wait.append(t_queue)
    await asyncio.wait([process_seed(seed_name, pool, f_queue, t_queue) for seed_name in SEED_NAMES])
    await asyncio.wait([q.done() for q in to_wait])
    return

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(run(False, True))
    loop.close()

from collections import defaultdict
from datetime import datetime
import pytz
import asyncio
from aiohttp import web, ClientSession

from helpers import loadj
from crawler import PREFIX

MIN_DATE = '2017-03-01'
MAX_LINKS = 25
BLACKLIST = ['http://foxrad.io', 'https://news.google.com', 'http://bb4sp.com', 'http://www.wibc.com']
USER_IDS = loadj(PREFIX + 'user_ids')

def setup_routes(app):
    app.router.add_post('/feed', get_feed)

async def get_feed(request):
    users = await request.json()
    ids = [USER_IDS[name] for name in users]
    links = await get_links(ids)
    return web.json_response(links)

async def get_links(ids):
    all_links = defaultdict(lambda:0)
    for user_id in ids:
        f_path = PREFIX + user_id + '_timeline'
        try:
            links = extract_links_from_timeline(loadj(f_path), include_retweets=False)
            for l in links:
                all_links[l] += 1
        except Exception as e:
            print(e)
    ordered = sorted(all_links, key=all_links.get, reverse=True)
    counts = [all_links[l] for l in ordered[:MAX_LINKS]]
    print(counts)
    async with ClientSession() as session:
        links = await follow_links(ordered[:MAX_LINKS], session)
        return links

def extract_links_from_timeline(tweets, include_retweets=True):
    if tweets is None:
        return []
    links = []
    for t in tweets:
        if to_date(t['created_at']) >= MIN_DATE:
            if include_retweets and 'retweeted_status' in t:
                url_list = t['retweeted_status']['entities']['urls']
            else:
                url_list = t['entities']['urls']
            for url_object in url_list:
                url = url_object['expanded_url']
                if not any([url.startswith(s) for s in BLACKLIST]):
                    links.append(url)
    return links

async def follow_links(links, session):
    futures = []
    for link in links:
        futures.append(asyncio.ensure_future(follow_link(link, session)))
    res = await asyncio.gather(*futures)
    filtered_links = []
    for url in res:
        if not any([url.startswith(s) for s in BLACKLIST]):
            filtered_links.append(url)
    return filtered_links

async def follow_link(link, session):
    async with session.get(link) as resp:
        return resp.url

def to_date(tw_timestamp):
    return datetime.strptime(tw_timestamp,'%a %b %d %H:%M:%S +0000 %Y').replace(tzinfo=pytz.UTC).strftime('%Y-%m-%d')

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    app = web.Application(loop=loop)
    setup_routes(app)
    web.run_app(app, host='127.0.0.1', port=5000)

import os
import os.path

from crawler import PREFIX
from helpers import loadj

# We might want to ignore a few seeds we crawled.
FILTERED = ['NickKristof', 'tylercowen', 'ezraklein', 'allidoisowen']

def get_user_names():
    return loadj(PREFIX + 'user_names')

def get_friend_whitelist(user_names):
    friend_whitelist = set()
    files = [f for f in os.listdir(PREFIX) if f.find('_friends')>0]
    for f_name in files:
        user_id, screen_name, _ = f_name.split('_')
        if screen_name not in FILTERED:
            friend_whitelist.add(user_id)
            friends = loadj(PREFIX + f_name)
            for friend_id in friends:
                # We might have updated the friends list but not
                # re-crawled yet.
                if friend_id in user_names:
                    friend_whitelist.add(friend_id)
    return friend_whitelist

def get_trimmable_nodes():
    return frozenset(loadj('to_trim.json'))

def filter_distances(whitelist, user_names, to_trim):
    f_path = PREFIX + 'filtered_distances'

    if os.path.exists(f_path):
        os.remove(f_path)

    with open(PREFIX + 'distances') as f, open(f_path, 'w') as w:
        for l in f:
            id1, id2, _ = l.strip().split(',')
            if id1 in whitelist and id2 in whitelist and not user_names[id1] in to_trim and not user_names[id2] in to_trim:
                w.write(l)


if __name__ == '__main__':
    user_names = get_user_names()
    whitelist = get_friend_whitelist(user_names)
    to_trim = get_trimmable_nodes()
    filter_distances(whitelist, user_names, to_trim)

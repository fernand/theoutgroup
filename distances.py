import os
from collections import defaultdict
from tqdm import tqdm

from crawler import PREFIX
from helpers import loadj, writej

def get_followers():
    followers = {}
    files = [f for f in os.listdir(PREFIX) if f.find('_followers')>0]
    for f_name in tqdm(files):
        user_id = f_name.split('_')[0]
        followers[user_id] = frozenset(loadj(PREFIX+f_name))
    return followers

def get_distances():
    dist = defaultdict(lambda: {})
    with open(PREFIX + 'distances') as f:
        for l in f:
            id1, id2, d = l.strip().split(',')
            dist[id1][id2] = float(d)
    return dist

def calc_distances(dist, followers):
    users = list(followers.keys())
    for i in tqdm(range(len(users))):
        i_user_id = users[i]
        i_followers = followers[i_user_id]
        if i_user_id not in dist:
            dist[i_user_id] = {}
        for j in range(i):
            j_user_id = users[j]
            j_followers = followers[j_user_id]
            if j_user_id not in dist[i_user_id]:
                len_inter = len(i_followers.intersection(j_followers))
                dist[i_user_id][j_user_id] = len_inter / (len(i_followers) + len(j_followers) - len_inter)
    return dist

def write_distances(dist):
    f_path = PREFIX+'distances'
    if os.path.exists(f_path):
        os.remove(f_path)
    with open(f_path, 'w') as f:
        for f1, f1d in dist.items():
            for f2, d in f1d.items():
                f.write('{0},{1},{2:.6f}\n'.format(f1, f2, d))

def write_user_names():
    user_names = {}
    files = [f for f in os.listdir(PREFIX) if f.find('_user')>0]
    for f_name in files:
        try:
            obj = loadj(PREFIX+f_name)
            user_names[obj['id_str']] = obj['screen_name']
        except Exception as e:
            print(f_name, e)
    writej(user_names, PREFIX + 'user_names')

    # Write the reverse dict as well.
    user_ids = {}
    for user_id, name in user_names.items():
        user_ids[name] = user_id
    writej(user_ids, PREFIX + 'user_ids')


if __name__ == '__main__':
    followers = get_followers()
    dist = get_distances()
    dist = calc_distances(dist, followers)
    write_distances(dist)
    write_user_names()

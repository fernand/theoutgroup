import os
import os.path
from collections import defaultdict
from tqdm import tqdm
from xxhash import xxh64
from datasketch import MinHash

from crawler import PREFIX
from helpers import loadj, writej

def make_minhash(items):
    m = MinHash(num_perm=128, hashobj=xxh64)
    for e in items:
        m.update(e.to_bytes(8, 'little'))
    return m

def get_followers():
    followers = {}
    followers_min = {}
    files = [f for f in os.listdir(PREFIX) if f.find('_followers')>0]
    for f_name in tqdm(files):
        user_id = f_name.split('_')[0]
        fs = loadj(PREFIX+f_name)
        followers[user_id] = frozenset(fs)
        followers_min[user_id] = make_minhash(fs)
    return (followers, followers_min)

def get_distances():
    dist = defaultdict(lambda: {})
    with open(PREFIX + 'distances') as f:
        for l in f:
            id1, id2, d = l.strip().split(',')
            dist[id1][id2] = float(d)
    return dist

def calc_distances(dist, fs, fs_min):
    users = list(fs.keys())
    for i in tqdm(range(len(users))):
        i_user_id = users[i]
        i_fs = fs[i_user_id]
        i_fs_min = fs_min[i_user_id]
        if i_user_id not in dist:
            dist[i_user_id] = {}
        for j in range(i):
            j_user_id = users[j]
            j_fs = fs[j_user_id]
            j_fs_min = fs_min[i_user_id]
            if j_user_id not in dist[i_user_id] and i_fs_min.jaccard(j_fs_min)>0.03:
                len_inter = len(i_fs.intersection(j_fs))
                dist[i_user_id][j_user_id] = len_inter / (len(i_fs) + len(j_fs) - len_inter)
    return dist

def write_distances(dist):
    f_path = PREFIX+'distances2'
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
    fs, fs_min = get_followers()
    # dist = get_distances()
    dist = {}
    dist = calc_distances(dist, fs, fs_min)
    write_distances(dist)
    write_user_names()

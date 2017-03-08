import numpy as np

from crawler import PREFIX

def get_aggs():
    dist = []
    with open(PREFIX+'filtered_distances') as f:
        for l in f:
            name1, name2, d = l.strip().split(',')
            dist.append(float(d))

    return np.array(dist)

def print_stats(dist):
    print(f'mean: {np.mean(dist)}')
    print(f'stdev: {np.std(dist)}')
    print(f'median: {np.percentile(dist, 50)}')
    print(f'max: {np.max(dist)}')
    for p in range(90, 100, 1):
        print(f'{p}th percentile: {np.percentile(dist, p)}')

if __name__ == '__main__':
    dist = get_aggs()
    print_stats(dist)

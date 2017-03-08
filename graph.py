import os.path
from collections import defaultdict
from networkx import Graph

from crawler import PREFIX, SEED_NAMES
from filter_distances import FILTERED
from helpers import writej, loadj

MIN_DIST = 0.05
MIN_DEGREE = 15

KRISTOF_RECS = frozenset(['DouthatNYT', 'MJGerson', 'StephensWSJ', 'JoeNBC', 'peggynoonannyc', 'reihan', 'arthurbrooks', 'ayaan', 'eliotacohen', 'Heritage', 'danielpipes', 'nfergus'])
def kristof_val(name):
    if name in KRISTOF_RECS:
        return 1
    else:
        return 0

def get_user_names():
    return loadj(PREFIX + 'user_names')

def get_provenance(user_names):
    prov = defaultdict(lambda:[])
    files = [f for f in os.listdir(PREFIX) if f.find('_friends')>0]
    for f_name in files:
        user_id, screen_name, _ = f_name.split('_')
        if screen_name not in FILTERED:
            friends = loadj(PREFIX + f_name)
            for f in friends:
                prov[f].append(screen_name)
    return prov

def get_edges():
    edges = []
    with open(PREFIX+'filtered_distances') as f:
        for l in f:
            fields = l.strip().split(',')
            d = float(fields[2])
            if d >= MIN_DIST:
                edges.append((fields[0], fields[1], d))
    return edges

def get_graph(edges):
    G = Graph()
    G.add_weighted_edges_from(edges)
    nodes = G.nodes()
    print(f'Num initial nodes after MIN_DIST: {len(nodes)}')
    for n in nodes:
        if G.degree(n) < MIN_DEGREE:
            G.remove_node(n)
    print(f'Num nodes after degree filter: {len(G.nodes())}')
    return G

def write_graph(G, user_names, prov):
    res = {'nodes': [], 'links': []}
    for n in G.nodes():
        res['nodes'].append({
            'id': user_names[n],
            'friends': prov[n],
            'color': kristof_val(user_names[n])
        })
    for e in G.edges():
        res['links'].append({
            'source': user_names[e[0]],
            'target': user_names[e[1]],
            'value': G[e[0]][e[1]]['weight']
        })
    writej(res, 'outgroup.json')

if __name__ == '__main__':
    user_names = get_user_names()
    edges = get_edges()
    prov = get_provenance(user_names)
    G = get_graph(edges)
    write_graph(G, user_names, prov)

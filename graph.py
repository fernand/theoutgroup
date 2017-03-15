import os
from collections import defaultdict
from networkx import Graph

from crawler import PREFIX
from helpers import writej, loadj

MIN_DIST = 0.05
MIN_DEGREE = 15

MEDIA = loadj('media.json')
NEOCONSERVATIVES = frozenset([p.lower() for p in ['arthurbrooks', 'dpletka', 'FredBarnes', 'davidfrum', 'JonahNRO', 'brithume', 'krauthammer', 'BillKristol', 'JoshuaMuravchik', 'DanielPipes', 'jpodhoretz', 'JRubinBlogger', 'mrubin1971', 'jonathans_tobin']])

def get_user_names():
    return loadj(PREFIX + 'user_names')

def get_provenance():
    media_prov = defaultdict(lambda:'')
    is_conservative = defaultdict(lambda:False)
    files = [f for f in os.listdir(PREFIX) if f.find('_user')>0]
    for f_name in files:
        user = loadj(PREFIX + f_name)
        for media, matches in MEDIA.items():
            for match in matches:
                user_desc = user['description'].lower()
                screen_name = user['screen_name']
                # NYT bestseller authors throw things off.
                if match in user_desc and screen_name not in media_prov and 'bestsell' not in user_desc:
                    media_prov[screen_name] = media
                if 'conservative' in user_desc:
                    is_conservative[screen_name] = True
    return media_prov, is_conservative

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

def write_graph(G, user_names, media_prov, is_conservative):
    res = {'nodes': [], 'links': []}
    media = list(MEDIA.keys())[::-1]
    for n in G.nodes():
        user_name = user_names[n]
        if media_prov[user_name] == '':
            media_index = 0
        else:
            media_index = media.index(media_prov[user_name])+1
        if user_name.lower() in NEOCONSERVATIVES:
            neoconservative = True
        else:
            neoconservative = False
        res['nodes'].append({
            'id': user_name,
            'media': media_prov[user_name],
            'media_index': media_index,
            'neoconservative': neoconservative,
            'conservative': is_conservative[user_name]
        })
    for e in G.edges():
        res['links'].append({
            'source': user_names[e[0]],
            'target': user_names[e[1]],
            'value': G[e[0]][e[1]]['weight']
        })
    writej(res, 'website/outgroup.json')

if __name__ == '__main__':
    user_names = get_user_names()
    edges = get_edges()
    media_prov, is_conservative = get_provenance()
    G = get_graph(edges)
    write_graph(G, user_names, media_prov, is_conservative)

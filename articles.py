import os
from collections import defaultdict
from multiprocessing import Pool
from newspaper import Article
import newspaper.nlp

from helpers import loadj, writej
from links import get_user_links
from crawler import PREFIX

NUM_WORKERS = 16
KEYWORDS_PATH = PREFIX + 'links_keywords.json'
USER_IDS = loadj(PREFIX + 'user_ids')

def gen_keywords():
    if not os.path.exists(KEYWORDS_PATH):
        graph_names = [n['id'] for n in loadj('website/outgroup.json')['nodes']]
        user_ids = [USER_IDS[name] for name in graph_names]
        user_links = get_user_links(user_ids)
        all_links = []
        for user_name, links in user_links.items():
            for l in links:
                all_links.append((user_name, l))
        p = Pool(NUM_WORKERS)
        kw_tuples = p.starmap(get_keywords_pmap, all_links)
        keywords = defaultdict(lambda:{'users':set()})
        for user_name, l, kws in kw_tuples:
            if l is not None:
                keywords[l]['kws'] = kws
                keywords[l]['users'].add(user_name)
        for l in keywords:
            keywords[l]['users'] = list(keywords[l]['users'])
        writej(keywords, KEYWORDS_PATH)

def get_keywords_pmap(user_name, url):
    link_info = get_article_info(url)
    if len(link_info['keywords']) > 0:
        return (user_name, link_info['link'], link_info['keywords'])
    else:
        return (None, None, None)

def get_article_info(url):
    try:
        a = Article(url)
        a.download()
        a.parse()
        a.nlp()
        return {'keywords': a.keywords, 'link': a.canonical_link}
    except:
        return {'keywords': [], 'link': a.canonical_link}

def load_keywords():
    kws = loadj(KEYWORDS_PATH)
    keywords = defaultdict(lambda:{})
    for l, v in kws.items():
        keywords[l]['kws'] = frozenset(v['kws'])
        keywords[l]['users'] = v['users']
    return keywords

def split_words(s):
    words = newspaper.nlp.split_words(s)
    return [w for w in words if w not in newspaper.nlp.stopwords]

if __name__ == "__main__":
    gen_keywords()

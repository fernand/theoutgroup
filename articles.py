import os
from collections import defaultdict
from multiprocessing import Pool
from newspaper import Article
import newspaper.nlp as nlp
from timeoutcontext import timeout

from helpers import loadj, writej
from links import get_user_links
from crawler import PREFIX

NUM_WORKERS = 16
KEYWORDS_PATH = PREFIX + 'links_keywords.json'
PARSED_LINKS_PATH = PREFIX + 'parsed_links.json'
USER_IDS = loadj(PREFIX + 'user_ids')

def gen_keywords():
    if os.path.exists(KEYWORDS_PATH):
        keywords = load_keywords()
    else:
        keywords = defaultdict(lambda:{'users': set()})

    # We're keeping a cache of already parsed links and also
    # are storing their matched canonical link to ensure that we can
    # add a user to the set of an already parsed link.
    if os.path.exists(PARSED_LINKS_PATH):
        parsed_links = loadj(PARSED_LINKS_PATH)
    else:
        parsed_links = {}

    graph_names = [n['id'] for n in loadj('website/outgroup.json')['nodes']]
    user_ids = [USER_IDS[name] for name in graph_names]
    user_links = get_user_links(user_ids)
    all_links = []
    for user_name, links in user_links.items():
        for l in links:
            # Don't process the link if we already did.
            if l not in parsed_links:
                all_links.append((user_name, l))
            elif parsed_links[l] != '':
                keywords[parsed_links[l]]['users'].add(user_name)

    p = Pool(NUM_WORKERS)
    kw_tuples = p.starmap(get_keywords_pmap, all_links)
    for user_name, c_link, l, kws, p_time in kw_tuples:
        if c_link is not None:
            parsed_links[l] = c_link
            keywords[c_link]['kws'] = kws
            keywords[c_link]['time'] = p_time
            keywords[c_link]['users'].add(user_name)
        else:
            parsed_links[l] = ''
    # Make the keywords dict serializable.
    for c_link in keywords:
        keywords[c_link]['users'] = list(keywords[c_link]['users'])
        keywords[c_link]['kws'] = list(keywords[c_link]['kws'])

    writej(keywords, KEYWORDS_PATH)
    writej(parsed_links, PARSED_LINKS_PATH)

def get_keywords_pmap(user_name, url):
    try:
        link_info = get_article_info(url)
    except TimeoutError:
        return (None, None, None, None, None)

    if len(link_info['keywords']) > 0:
        return (user_name, link_info['c_link'], url, link_info['keywords'], link_info['published_time'])
    else:
        return (None, None, None, None, None)

@timeout(3)
def get_article_info(url):
    try:
        a = Article(url, fetch_images=False)
        a.download()
        a.parse()
        # Not doing a.nlp() to be more efficient.
        text_keyws = list(nlp.keywords(a.text).keys())
        title_keyws = list(nlp.keywords(a.title).keys())
        keyws = list(set(title_keyws + text_keyws))

        if 'published_time' in a.meta_data['article']:
            published_time = a.meta_data['article']['published_time']
        else:
            published_time = ''
        return {'keywords': keyws, 'c_link': a.canonical_link, 'published_time': published_time}
    except:
        return {'keywords': [], 'c_link': a.canonical_link, 'published_time': ''}

def load_keywords():
    kws = loadj(KEYWORDS_PATH)
    keywords = defaultdict(lambda:{'users': set()})
    for l, v in kws.items():
        keywords[l]['users'] = set(v['users'])
        keywords[l]['kws'] = frozenset(v['kws'])
    return keywords

def split_words(s):
    words = nlp.split_words(s)
    return [w for w in words if w not in nlp.stopwords]

if __name__ == "__main__":
    gen_keywords()

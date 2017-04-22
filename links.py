from datetime import datetime
from collections import defaultdict
import pytz
from tqdm import tqdm

from crawler import PREFIX
from helpers import loadj

MIN_DATE = '2017-04-01'
USER_NAMES = loadj(PREFIX + 'user_names')

def get_user_links(ids):
    user_links = {}
    print('Extracting links from user timelines...')
    for user_id in tqdm(ids):
        f_path = PREFIX + user_id + '_timeline'
        try:
            links = extract_links_from_timeline(loadj(f_path), include_retweets=False)
            user_links[USER_NAMES[user_id]] = links
        except Exception as e:
            print(e)
    return user_links

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
                links.append(url)
    return links

def to_date(tw_timestamp):
    return datetime.strptime(tw_timestamp,'%a %b %d %H:%M:%S +0000 %Y').replace(tzinfo=pytz.UTC).strftime('%Y-%m-%d')

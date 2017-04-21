from collections import defaultdict
import asyncio
from aiohttp import web, ClientSession
import aiohttp_cors

from helpers import loadj
from crawler import PREFIX
from links import get_links
import articles

MAX_LINKS = 25
BLACKLIST = ['http://foxrad.io', 'https://news.google.com', 'http://bb4sp.com', 'http://www.wibc.com']
USER_IDS = loadj(PREFIX + 'user_ids')
KEYWORDS = articles.load_keywords()

def load_keywords():
    return articles.load_keywords(articles.KEYWORDS_PATH)

def setup_routes(cors, app):
    app.router.add_post('/feed', get_feed)
    resource = cors.add(app.router.add_resource('/similar'))
    route = cors.add(
        resource.add_route('POST', get_similar), {
            "*": aiohttp_cors.ResourceOptions(
                allow_credentials=True,
                expose_headers='*',
                allow_headers='*'
            )
    })

async def get_feed(request):
    users = await request.json()
    ids = [USER_IDS[name] for name in users]
    filtered_links = await filter_links(links.get_links(ids))
    return web.json_response(filtered_links)

# TODO: order by provenance coverage and not just isec.
async def get_similar(request):
    data = await request.json()
    current_kws = articles.get_keywords(data['url'])
    matches = defaultdict(lambda:{})
    for l, v in KEYWORDS.items():
        isec = len(v['kws'].intersection(current_kws))
        if isec >= 4:
            matches[l]['isec'] = isec
            matches[l]['users'] = v['users']
    return web.json_response(matches)

async def filter_links(all_links):
    ordered = sorted(all_links, key=all_links.get, reverse=True)
    # counts = [all_links[l] for l in ordered[:MAX_LINKS]]
    async with ClientSession() as session:
        links = await follow_links(ordered[:MAX_LINKS], session)
        return links

async def follow_links(links, session):
    futures = []
    print(f'{len(links)} to follow.')
    for link in links:
        futures.append(asyncio.ensure_future(follow_link(link, session)))
    print('Following links...')
    res = await asyncio.gather(*futures)
    print('Done following links.')
    filtered_links = []
    for url in res:
        if not any([url.startswith(s) for s in BLACKLIST]):
            filtered_links.append(url)
    return filtered_links

async def follow_link(link, session):
    async with session.get(link) as resp:
        return resp.url

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    app = web.Application(loop=loop)
    cors = aiohttp_cors.setup(app)
    setup_routes(cors, app)
    web.run_app(app, host='127.0.0.1', port=5000)

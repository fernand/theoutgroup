from collections import defaultdict
import asyncio
from aiohttp import web
import aiohttp_cors
import logging

from crawler import PREFIX
from helpers import loadj, writej
import spectrum
import articles

CORS = {
    "*": aiohttp_cors.ResourceOptions(
        allow_credentials=True,
        expose_headers='*',
        allow_headers='*'
    )
}

async def similar(request):
    data = await request.json()
    url = data['url'].split('?')[0]
    # Keyword data can potentially be in two places.
    if url in app['db']:
        keywords = app['db'][url]['kws']
    elif url in app['link_cache']:
        keywords = app['link_cache'][url]
    else:
        keywords =  articles.get_article_info(url)['keywords']
        app['link_cache'][url] = keywords
    return web.json_response(spectrum.get_articles(app['db'], app['indices'], keywords, url))

async def search(request):
    data = await request.json()
    keywords = articles.split_words(data['query'])
    return web.json_response(spectrum.get_articles(app['db'], app['indices'], keywords, None))

def build_indices(app):
    tmp = defaultdict(lambda:set())
    app['indices'] = {}
    for l in app['db']:
        for kw in app['db'][l]['kws']:
            tmp[kw].add(l)
    for kw in tmp:
        app['indices'][kw] = list(tmp[kw])

def setup_routes(cors, app):
    for (endpoint, handler) in [('/similar', similar), ('/search', search)]:
        resource = cors.add(app.router.add_resource(endpoint))
        route = cors.add(resource.add_route('POST', handler), CORS)

def setup_data(app):
    app['db'] = None
    app['link_cache'] = loadj(PREFIX + 'link_cache.json')
    if app['link_cache'] is None:
        app['link_cache'] = {}
    app['db_refresh'] = app.loop.create_task(schedule(app, refresh_db, 10*60))
    app['flush_cache'] = app.loop.create_task(schedule(app, flush_cache, 15*60))
    app.on_cleanup.append(cancel_sched)

async def schedule(app, task, interval):
    while True:
        task(app)
        await asyncio.sleep(interval)

def refresh_db(app):
    app['db'] = articles.load_keywords()
    build_indices(app)

def flush_cache(app):
    writej(app['link_cache'], PREFIX + 'link_cache.json')

def cancel_sched(app):
    app['db_refresh'].cancel()
    app['flush_cache'].cancel()

if __name__ == "__main__":
    logging.basicConfig(filename='/root/server.log', level=logging.INFO)
    loop = asyncio.get_event_loop()
    app = web.Application(loop=loop)
    cors = aiohttp_cors.setup(app)
    setup_routes(cors, app)
    setup_data(app)
    web.run_app(app, port=5000, access_log_format='%a %t %Tf "%r" %s "%{Referrer}i" "%{User-Agent}i"')

from collections import defaultdict
import asyncio
from aiohttp import web, ClientSession
import aiohttp_cors

import articles

KEYWORDS = articles.load_keywords()

def setup_routes(cors, app):
    resource = cors.add(app.router.add_resource('/similar'))
    route = cors.add(
        resource.add_route('POST', get_similar), {
            "*": aiohttp_cors.ResourceOptions(
                allow_credentials=True,
                expose_headers='*',
                allow_headers='*'
            )
    })

# TODO: order by provenance coverage and not just isec.
async def get_similar(request):
    data = await request.json()
    current_kws = articles.get_article_info(data['url'])['keywords']
    matches = defaultdict(lambda:{})
    for l, v in KEYWORDS.items():
        isec = len(v['kws'].intersection(current_kws))
        if isec >= 4:
            matches[l]['isec'] = isec
            matches[l]['users'] = v['users']
    ordered = sorted(matches.items(), key=lambda k_v: len(k_v[1]['users']), reverse=True)
    return web.json_response([{'url': k_v[0], 'isec': k_v[1]['isec'], 'users': k_v[1]['users']} for k_v in ordered])

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    app = web.Application(loop=loop)
    cors = aiohttp_cors.setup(app)
    setup_routes(cors, app)
    web.run_app(app, host='127.0.0.1', port=5000)

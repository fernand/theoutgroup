import math
from collections import defaultdict
import asyncio
from aiohttp import web
import aiohttp_cors

from helpers import loadj
import articles

CORS = {
    "*": aiohttp_cors.ResourceOptions(
        allow_credentials=True,
        expose_headers='*',
        allow_headers='*'
    )
}

KEYWORDS = articles.load_keywords()
POSITION = loadj('node_positions.json')
MIN_ISEC_ARTICLE = 3
MIN_USERS = 3

U = (1, -0.3601689858)
UU = math.sqrt(U[0]*U[0]+U[1]*U[1])
U = (U[0]/UU, U[1]/UU)
M = (461.3168343280106, 445.9243631476138)

# We're capping the political spectrum to -350, 350.
# Resolution is 0.1.
def clamp(scalar):
    scalar = scalar / 350
    return int(scalar / 0.1) / 10

async def similar(request):
    data = await request.json()
    keywords = articles.get_article_info(data['url'])['keywords']
    return web.json_response(get_articles(keywords, from_article=True))

async def search(request):
    data = await request.json()
    keywords = articles.split_words(data['query'])
    return web.json_response(get_articles(keywords, from_article=False))

def get_articles(kws, from_article):
    if len(kws) == 0 or len(kws) > 20:
        return []

    if from_article:
        min_isec = MIN_ISEC_ARTICLE
    else:
        min_isec = len(kws)
    matches = defaultdict(lambda:{})
    for l, v in KEYWORDS.items():
        isec = len(v['kws'].intersection(kws))
        users = v['users']
        if isec >= min_isec and len(users) >= MIN_USERS:
            matches[l]['isec'] = isec
            matches[l]['users'] = users
            avg_pos = (sum([POSITION[u]['x'] for u in v['users']]) / len(users),
                sum([POSITION[u]['y'] for u in v['users']]) / len(users))
            avg_pos = (avg_pos[0] - M[0], avg_pos[1] - M[1])
            scalar = U[0] * avg_pos[0] + U[1] * avg_pos[1]
            matches[l]['scalar'] = clamp(scalar)

    ordered = sorted(matches.items(), key=lambda k_v: k_v[1]['scalar'])
    return [{
        'url': k_v[0],
        'isec': k_v[1]['isec'],
        'num_users': len(k_v[1]['users']),
        'scalar': k_v[1]['scalar']
    } for k_v in ordered]

def setup_routes(cors, app):
    for (endpoint, handler) in [('/similar', similar), ('/search', search)]:
        resource = cors.add(app.router.add_resource(endpoint))
        route = cors.add(resource.add_route('POST', handler), CORS)

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    app = web.Application(loop=loop)
    cors = aiohttp_cors.setup(app)
    setup_routes(cors, app)
    web.run_app(app, host='127.0.0.1', port=5000)

import math

from helpers import loadj

POSITION = loadj('node_positions.json')

U = (1, -0.3601689858)
UU = math.sqrt(U[0]*U[0]+U[1]*U[1])
U = (U[0]/UU, U[1]/UU)
M = (461.3168343280106, 445.9243631476138)

RES = 0.25
RANGE = [i/100 for i in range(int(-1*100), int((1+RES)*100), int(RES*100))]

MIN_ISEC_ARTICLE = 3
MIN_USERS = 3
MAX_RETURNED_ARTICLES = 5

# We're capping the political spectrum to -350, 350.
def clamp(scalar):
    scalar = scalar / 350
    return int(scalar / RES) * RES

def get_scalar(users):
    avg_pos = (sum([POSITION[u]['x'] for u in users]) / len(users),
        sum([POSITION[u]['y'] for u in users]) / len(users))
    avg_pos = (avg_pos[0] - M[0], avg_pos[1] - M[1])
    scalar = U[0] * avg_pos[0] + U[1] * avg_pos[1]
    return clamp(scalar)

def get_articles(db, indices, kws, from_article):
    if len(kws) == 0 or len(kws) > 20:
        return []

    if from_article is not None:
        min_isec = MIN_ISEC_ARTICLE
    else:
        min_isec = len(kws)

    matches = {}
    for i in RANGE:
        matches[i] = {}

    candidate_links = []
    for kw in kws:
        if kw in indices:
            candidate_links.extend(indices[kw])

    for l in candidate_links:
        v = db[l]
        isec = len(v['kws'].intersection(kws))
        users = v['users']
        if 'title' in v:
            title = v['title']
        else:
            title = l
        # TODO: filter to last month
        if isec >= min_isec and len(users) >= MIN_USERS and from_article != l:
            scalar = get_scalar(users)
            matches[scalar][l] = {
                'isec': isec,
                'num_users': len(users),
                'scalar': scalar,
                'title': title
            }

    # Group articles by score.
    # Pick one article per score, order by isec and user count.
    # If end result set is <5 enrich by picking secondary scoring articles.
    res = []
    secondary = []
    for scalar in RANGE:
        ordered = sorted(matches[scalar].items(), key=lambda k_v: (k_v[1]['isec'], k_v[1]['num_users']), reverse=True)
        if len(ordered) >= 1:
            res.append({
                'url': ordered[0][0],
                'isec': ordered[0][1]['isec'],
                'num_users': ordered[0][1]['num_users'],
                'title': ordered[0][1]['title'],
                'scalar': scalar,
            })
        if len(ordered) >= 2:
            secondary.append({
                'url': ordered[1][0],
                'isec': ordered[1][1]['isec'],
                'num_users': ordered[1][1]['num_users'],
                'title': ordered[1][1]['title'],
                'scalar': scalar
            })

    if len(res) < MAX_RETURNED_ARTICLES:
        for a in secondary:
            res.append(a)

    # Re-sort res
    return sorted(res, key=lambda a: a['scalar'])

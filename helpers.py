import os
import json

def writej(obj, f_path, overwrite=True):
    if os.path.exists(f_path):
        if overwrite:
            os.remove(f_path)
        else:
            return

    with open(f_path, 'w') as f:
        json.dump(obj, f)

def loadj(f_path):
    if not os.path.exists(f_path):
        return None
    else:
        with open(f_path) as f:
            return json.load(f)

def append_listj(obj, f_path, el_id='id'):
    if not os.path.exists(f_path):
        return writej(obj, f_path)
    else:
        try:
            curr_list = loadj(f_path)
        except Exception as e:
            print(e, f_path)
            curr_list = []
        ids = frozenset([e[el_id] for e in curr_list])
        for e in obj:
            if not e[el_id] in ids:
                curr_list.append(e)
        return writej(curr_list, f_path)

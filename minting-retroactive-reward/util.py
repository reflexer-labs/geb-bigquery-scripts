import json
import requests
import pandas as pd

def get_safe_owners(graph_url, block_number):
    query = '''
        query {{
        safeHandlerOwners(first: 1000, skip: {}, block: {{number:{}}}) {{
          id  
          owner {{
            address
          }}  
        }}  
        }}  
        ''' 

    n = 0
    results = []
    while True:
        r = requests.post(graph_url, json = {'query':query.format(n*1000, block_number)})
        try:
            s = json.loads(r.content)['data']['safeHandlerOwners']
        except:
            print(json.loads(r.content))
            break
        results.extend([(block_number, x['id'], x['owner']['address']) for x in s])
        n += 1
        if len(s) < 1000:
            break
    return pd.DataFrame(results, columns=['block', 'safe', 'owner'])

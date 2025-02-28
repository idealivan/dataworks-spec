import sys
import warnings

import json

warnings.filterwarnings('ignore')

if __name__ == '__main__':
    """
    content = sys.argv[1]
    args = sys.argv[2]
    args: 
    """
    content = sys.argv[1]
    params = sys.argv[2]
    content = content + "\n add something"
    # output to invoker
    # need a json with key _code_
    # a json with _params_ if add params
    updated_content = {'_code_': content}
    print(json.dumps(updated_content))
    updated_params = {'_params_': 'key1=value1,key2=value2'}
    print(json.dumps(updated_params))

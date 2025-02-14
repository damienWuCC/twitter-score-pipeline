import http.client
import json
import time
import logging
import os
from dotenv import load_dotenv

# 加载环境变量
load_dotenv()

# 从环境变量获取配置
API_KEY = os.getenv('TWITTER_API_KEY')
API_HOST = os.getenv('TWITTER_API_HOST')
MAX_RETRIES = 3
RETRY_DELAY = 5

def make_request(endpoint, params):
    retries = 0
    while retries < MAX_RETRIES:
        try:
            conn = http.client.HTTPSConnection(API_HOST)
            headers = {
                'x-rapidapi-key': API_KEY,
                'x-rapidapi-host': API_HOST
            }
            url = f"{endpoint}?{params}" if params else endpoint
            conn.request("GET", url, headers=headers)
            res = conn.getresponse()
            if res.status == 200:
                data = res.read()
                return json.loads(data.decode("utf-8"))
            else:
                logging.error(f"Error: Received status code {res.status} info {res.reason}")
                raise Exception(f"Error: Received status code {res.status} info {res.reason}")
        except Exception as e:
            logging.error(f"Exception occurred: {e}")
            retries += 1
            if retries < MAX_RETRIES:
                logging.info(f"Retrying... ({retries}/{MAX_RETRIES})")
                time.sleep(RETRY_DELAY)
            else:
                logging.error("Max retries reached. Giving up.")
                return None

def get_following_id_list(username, count=5000, cursor=None):
    params = f"username={username}&count={count}"
    if cursor:
        params += f"&cursor={cursor}"
    response = make_request("/following-ids", params)
    if response:
        return response['ids'], response.get('next_cursor', None)
    return [], None

def get_user_info_by_ids(user_ids):
    ids_str = ",".join(map(str, user_ids))
    response = make_request("/get-users", f"users={ids_str}")
    if response:
        return response['result']['data']['users']
    return []

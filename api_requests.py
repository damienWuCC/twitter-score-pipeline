import http.client
import json
import time

API_KEY = "5eefd5425emsh1eea69ca15f38a9p1b55fbjsn50754f660585"
API_HOST = "twitter241.p.rapidapi.com"
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
            conn.request("GET", f"{endpoint}?{params}", headers=headers)
            res = conn.getresponse()
            if res.status == 200:
                data = res.read()
                return json.loads(data.decode("utf-8"))
            else:
                print(f"Error: Received status code {res.status}")
        except Exception as e:
            print(f"Exception occurred: {e}")
        retries += 1
        time.sleep(RETRY_DELAY)
    return None

def get_following_list(username):
    response = make_request("/followings", f"user={username}&count=20")
    if response:
        following = response['timeline']['instructions'][2]['entries']
        return [{
            'rest_id': entry['content']['itemContent']['user_results']['result']['rest_id'],
            'screen_name': entry['content']['itemContent']['user_results']['result']['legacy']['screen_name'],
            'followers_count': entry['content']['itemContent']['user_results']['result']['legacy']['followers_count'],
            'friends_count': entry['content']['itemContent']['user_results']['result']['legacy']['friends_count'],
            'profile_image_url_https': entry['content']['itemContent']['user_results']['result']['legacy']['profile_image_url_https']
        } for entry in following if entry['content']['entryType'] == 'TimelineTimelineItem']
    return []

def get_recent_tweets(rest_id, count=200):
    response = make_request("/user-tweets", f"user_id={rest_id}&count={count}")
    if response:
        return response['tweets']
    return []

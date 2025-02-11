import networkx as nx
from api_requests import get_recent_tweets

def build_relationship_graph(kol_list, followers_list):
    G = nx.Graph()
    combined_list = kol_list + followers_list
    for user in combined_list:
        rest_id = user['rest_id']
        tweets = get_recent_tweets(rest_id)
        for tweet in tweets:
            mentioned_users = tweet.get('mentions', [])
            for mentioned_user in mentioned_users:
                G.add_edge(user['screen_name'], mentioned_user)
    return G

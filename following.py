from api_requests import get_following_id_list, get_user_info_by_ids
import logging
import json

def build_secondary_kol_ids_list(kol_list, kol_ids):
    secondary_kol_id_list = {}
    for kol in kol_list:
        screen_name = kol['username']
        ids, cursor = get_following_id_list(screen_name)
        logging.warning(f"fetch num for {screen_name}: {len(ids)}")
        ids = [id for id in ids if id not in kol_ids]
        for id in ids:
            if id not in secondary_kol_id_list:
                secondary_kol_id_list[id] = []
            secondary_kol_id_list[id].append(screen_name)
        logging.warning(f"All num for {screen_name}: {len(secondary_kol_id_list.keys())}")
        while cursor and cursor != '0' and cursor != -1:
            ids, cursor = get_following_id_list(screen_name, cursor=cursor)
            ids = [id for id in ids if id not in kol_ids]
            for id in ids:
                if id not in secondary_kol_id_list:
                    secondary_kol_id_list[id] = []
                secondary_kol_id_list[id].append(screen_name)
    
    # 将 secondary_kol_id_list 写入本地 JSON 文件
    with open('secondary_kol_id_list.json', 'w', encoding='utf-8') as f:
        json.dump(secondary_kol_id_list, f, ensure_ascii=False, indent=4)
        
    return secondary_kol_id_list

def build_secondary_kol_list():
    # 获取用户详细信息
    user_info_list = []
    with open('secondary_kol_id_list.json', 'r', encoding='utf-8') as f:
        id_list = json.load(f)
    secondary_kol_ids = list(id_list.keys())
    for i in range(0, len(secondary_kol_ids), 200):
        user_ids = secondary_kol_ids[i:i+200]
        logging.warning(f"start fetching {i}")
        user_info = get_user_info_by_ids(user_ids)
        logging.warning(f"fetch num for {i}: {len(user_info)}")
        for user in user_info:
            user['result']['followed_by'] = id_list[user['result']['rest_id']]
        logging.warning(f"example {user_info[0]}")

        user_details = [{
            'id': user['result']['rest_id'],
            'username': user['result']['legacy']['screen_name'],
            'followers_count': user['result']['legacy']['followers_count'],
            'following_count': user['result']['legacy']['friends_count'],
            'avatar': user['result']['legacy']['profile_image_url_https'],
            'is_blue_verified': user['result']["is_blue_verified"],
            'statuses_count': user['result']['legacy']["statuses_count"],
            'listed_count': user['result']['legacy']["listed_count"],
            'followed_by': user['result']['followed_by']
        } for user in user_info_list]
        
        # 将用户详细信息写入本地 JSON 文件
        with open('secondary_kol_list.json', 'w', encoding='utf-8') as f:
            json.dump(user_details, f, ensure_ascii=False, indent=4)
            
        user_info_list.extend(user_info)

    # 处理用户详细信息
    user_details = [{
        'id': user['result']['rest_id'],
        'username': user['result']['legacy']['screen_name'],
        'followers_count': user['result']['legacy']['followers_count'],
        'following_count': user['result']['legacy']['friends_count'],
        'avatar': user['result']['legacy']['profile_image_url_https'],
        'is_blue_verified': user['result']["is_blue_verified"],
        'statuses_count': user['result']['legacy']["statuses_count"],
        'listed_count': user['result']['legacy']["listed_count"],
        'followed_by': user['result']['followed_by']
    } for user in user_info_list]
    
    # 将用户详细信息写入本地 JSON 文件
    with open('secondary_kol_list.json', 'w', encoding='utf-8') as f:
        json.dump(user_details, f, ensure_ascii=False, indent=4)

    return user_info_list

def build_secondary_kol_list_test(kol_list, kol_ids):
    secondary_kol_list = []
    screen_name = kol_list[0]['username']
    ids, cursor = get_following_id_list(screen_name)
    ids = [id for id in ids if id not in kol_ids]
    secondary_kol_list.extend(ids)
    while cursor:
        ids, cursor = get_following_id_list(screen_name, cursor=cursor)
        ids = [id for id in ids if id not in kol_ids]
        secondary_kol_list.extend(ids)
    return secondary_kol_list

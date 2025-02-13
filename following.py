import os
from api_requests import get_following_id_list, get_user_info_by_ids
import logging
import json
from concurrent.futures import ThreadPoolExecutor, as_completed

def ensure_output_dir():
    """确保输出目录存在"""
    output_dir = os.path.join(os.path.dirname(__file__), 'output')
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    return output_dir

def build_secondary_kol_ids_list(kol_list, kol_ids):
    output_dir = ensure_output_dir()
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
    
    # 修改保存路径
    output_path = os.path.join(output_dir, 'secondary_kol_id_list.json')
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(secondary_kol_id_list, f, ensure_ascii=False, indent=4)
        
    return secondary_kol_id_list

def fetch_user_info_chunk(chunk_id, user_ids, id_list, total_chunks):
    output_dir = ensure_output_dir()
    all_user_details = []  # 存储所有处理过的用户详情
    invalid_user_info_list = []
    total_users = len(user_ids)
    processed_users = 0
    
    logging.warning(f"Chunk {chunk_id + 1}/{total_chunks} started, processing {total_users} users")
    
    for i in range(0, len(user_ids), 100):
        batch_ids = user_ids[i:i+100]
        current_batch_size = len(batch_ids)
        logging.warning(f"Chunk {chunk_id + 1}/{total_chunks}: Processing batch {i//100 + 1}, users {i+1}-{i+current_batch_size} of {total_users}")
        
        user_info = get_user_info_by_ids(batch_ids)
        processed_users += current_batch_size
        progress = (processed_users / total_users) * 100
        
        user_info_list = []
        for user in user_info:
            if 'result' not in user:
                invalid_user_info_list.append(user)
                continue
            user['result']['followed_by'] = id_list[user['result']['rest_id']]
            user_info_list.append(user)

        # 处理用户详细信息
        batch_user_details = [{
            'id': user['result']['rest_id'],
            'username': user['result']['legacy']['screen_name'],
            'followers_count': user['result']['legacy']['followers_count'],
            'following_count': user['result']['legacy']['friends_count'],
            'avatar': user['result']['legacy']['profile_image_url_https'],
            'is_blue_verified': user['result']["is_blue_verified"],
            'statuses_count': user['result']['legacy']["statuses_count"],
            'listed_count': user['result']['legacy']["listed_count"],
            'followed_by': user['result']['followed_by']
        } for user in user_info_list if 'result' in user]
        
        all_user_details.extend(batch_user_details)
        logging.warning(f"Chunk {chunk_id + 1}/{total_chunks}: Progress {progress:.1f}% - Processed {processed_users}/{total_users} users")

    # 一次性写入所有用户详情
    output_path = os.path.join(output_dir, f'secondary_kol_list_{chunk_id}.json')
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(all_user_details, f, ensure_ascii=False, indent=4)

    # 一次性写入所有无效用户信息
    invalid_output_path = os.path.join(output_dir, f'invalid_user_info_{chunk_id}.json')
    with open(invalid_output_path, 'w', encoding='utf-8') as f:
        json.dump(invalid_user_info_list, f, ensure_ascii=False, indent=4)

    logging.warning(f"Chunk {chunk_id + 1}/{total_chunks} completed. Total processed: {processed_users} users")
    return processed_users

def build_secondary_kol_list():
    output_dir = ensure_output_dir()
    with open(os.path.join(output_dir, 'filtered_kol_id_list_3.json'), 'r', encoding='utf-8') as f:
        id_list = json.load(f)
    secondary_kol_ids = list(id_list.keys())
    total_users = len(secondary_kol_ids)
    
    logging.warning(f"Starting processing for {total_users} users")
    
    # 分成多个并行任务
    chunk_size = len(secondary_kol_ids) // 5
    chunks = [secondary_kol_ids[i:i + chunk_size] for i in range(0, len(secondary_kol_ids), chunk_size)]
    total_chunks = len(chunks)
    
    logging.warning(f"Divided into {total_chunks} chunks, approximately {chunk_size} users per chunk")
    
    total_processed = 0
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(fetch_user_info_chunk, idx, chunk, id_list, total_chunks) 
                  for idx, chunk in enumerate(chunks)]
        
        for future in as_completed(futures):
            chunk_processed = future.result()
            total_processed += chunk_processed
            overall_progress = (total_processed / total_users) * 100
            logging.warning(f"Overall progress: {overall_progress:.1f}% - Total processed: {total_processed}/{total_users}")
    
    logging.warning("All processing completed!")
    # 在处理完成后调用合并函数
    merge_secondary_kol_lists()

def merge_secondary_kol_lists():
    """合并所有secondary_kol_list_{chunk_id}.json文件的数据到一个文件中"""
    output_dir = ensure_output_dir()
    all_users = []
    file_counts = {}
    
    # 查找所有secondary_kol_list文件
    for filename in os.listdir(output_dir):
        if filename.startswith('secondary_kol_list_') and filename.endswith('.json'):
            file_path = os.path.join(output_dir, filename)
            logging.warning(f"Processing file: {filename}")
            
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    users = json.load(f)
                    if isinstance(users, list):
                        file_counts[filename] = len(users)
                        all_users.extend(users)
                        logging.warning(f"Added {len(users)} users from {filename}")
            except json.JSONDecodeError as e:
                logging.error(f"Error parsing {filename}: {str(e)}")
                continue
    
    # 打印每个文件的数据量
    logging.warning("\n各文件数据统计:")
    for filename, count in file_counts.items():
        logging.warning(f"{filename}: {count} 条数据")
    logging.warning(f"合并前总数据量: {len(all_users)} 条")
    
    # 按ID去重
    unique_users = {}
    for user in all_users:
        user_id = user['id']
        if user_id not in unique_users:
            unique_users[user_id] = user
    
    merged_users = list(unique_users.values())
    logging.warning(f"去重后数据量: {len(merged_users)} 条")
    logging.warning(f"重复数据量: {len(all_users) - len(merged_users)} 条")
    
    # 保存合并后的数据
    merged_file_path = os.path.join(output_dir, 'merged_secondary_kol_list.json')
    with open(merged_file_path, 'w', encoding='utf-8') as f:
        json.dump(merged_users, f, ensure_ascii=False, indent=4)
    
    logging.warning(f"Merged data saved to: {merged_file_path}")
    return merged_users
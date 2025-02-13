import os
import json

def ensure_output_dir():
    output_dir = 'output'
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    return output_dir

def analyze_following_distribution(min_following_count=None):
    """分析并统计被关注次数的分布情况"""
    output_dir = ensure_output_dir()
    # 从output目录读取文件
    with open(os.path.join(output_dir, 'secondary_kol_id_list.json'), 'r', encoding='utf-8') as f:
        id_list = json.load(f)
    
    # 统计每个ID被关注的次数
    following_count_dist = {}
    filtered_id_list = {}
    
    for user_id, followed_by in id_list.items():
        count = len(followed_by)
        # 统计分布
        if count not in following_count_dist:
            following_count_dist[count] = 0
        following_count_dist[count] += 1
        
        # 过滤数据
        if min_following_count and count >= min_following_count:
            filtered_id_list[user_id] = followed_by
    
    # 排序并输出统计结果
    sorted_dist = dict(sorted(following_count_dist.items()))
    
    # 将分布结果写入output目录
    with open(os.path.join(output_dir, 'following_distribution.json'), 'w', encoding='utf-8') as f:
        json.dump(sorted_dist, f, ensure_ascii=False, indent=4)
    
    # 如果有过滤条件，将过滤后的数据写入output目录
    if min_following_count:
        filtered_output_path = os.path.join(output_dir, f'filtered_kol_id_list_{min_following_count}.json')
        with open(filtered_output_path, 'w', encoding='utf-8') as f:
            json.dump(filtered_id_list, f, ensure_ascii=False, indent=4)
    
    # 打印统计结果
    print("\n关注次数分布统计:")
    print("被关注次数 : ID数量")
    for count, num_ids in sorted_dist.items():
        print(f"{count} : {num_ids}")
    
    return sorted_dist, filtered_id_list if min_following_count else None

def get_filtered_following_list(min_following_count=3):
    """获取被关注次数大于等于指定次数的KOL列表"""
    _, filtered_list = analyze_following_distribution(min_following_count)
    return filtered_list

from api_requests import get_following_list
import csv

def load_kol_screen_names(file_path):
    kol_screen_names = set()
    with open(file_path, mode='r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        for row in reader:
            kol_screen_names.add(row['ScreenName'])
    return kol_screen_names

def build_secondary_kol_list(kol_list, kol_screen_names):
    secondary_kol_list = set()
    kol_set = set(kol_list)
    for username in kol_list:
        following = get_following_list(username)
        for user in following:
            if user['screen_name'] not in kol_set and user['screen_name'] not in kol_screen_names:
                secondary_kol_list.add(user['screen_name'])
    return list(secondary_kol_list)

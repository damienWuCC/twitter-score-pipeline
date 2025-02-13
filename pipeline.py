from pyspark.sql import SparkSession
import networkx as nx
import sqlite3
from following import build_secondary_kol_list, build_secondary_kol_ids_list
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

def load_kol_list(file_path):
    spark = SparkSession.builder.appName("KOLPipeline").getOrCreate()
    kol_df = spark.read.csv(file_path, header=True)
    kol_list = kol_df.select("username", "id", "followers_count", "following_count", "avatar").rdd.map(lambda row: {
        'username': row['username'],
        'id': row['id'],
        'followers_count': row['followers_count'],
        'following_count': row['following_count'],
        'avatar': row['avatar']
    }).collect()
    return kol_list

def calculate_weighted_scores(graph):
    scores = nx.pagerank(graph)
    return scores

def save_scores_to_db(scores, db_connection):
    for username, score in scores.items():
        db_connection.execute("INSERT INTO scores (username, score) VALUES (?, ?)", (username, score))

def create_db_connection():
    connection = sqlite3.connect('kol_scores.db')
    return connection

def main():
    kol_list = load_kol_list('./user_data.csv')
    kol_ids = set([row['id'] for row in kol_list])
    logging.info("kol ids", kol_ids)
    secondary_kol_list = build_secondary_kol_list()

if __name__ == "__main__":
    main()

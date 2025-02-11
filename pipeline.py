from pyspark.sql import SparkSession
import networkx as nx
import sqlite3
from following import build_secondary_kol_list, load_kol_screen_names
from tweets import build_relationship_graph

# 1. 准备一个500 KOL list的核心账号列表
def load_kol_list(file_path):
    spark = SparkSession.builder.appName("KOLPipeline").getOrCreate()
    kol_df = spark.read.csv(file_path, header=True)
    kol_list = kol_df.select("ScreenName", "rest_id").rdd.map(lambda row: {'screen_name': row['ScreenName'], 'rest_id': row['rest_id']}).collect()
    return kol_list

# 5. 计算各个节点的加权得分然后入库
def calculate_weighted_scores(graph):
    scores = nx.pagerank(graph)
    return scores

def save_scores_to_db(scores, db_connection):
    # 假设有一个数据库连接对象
    for username, score in scores.items():
        db_connection.execute("INSERT INTO scores (username, score) VALUES (?, ?)", (username, score))

def create_db_connection():
    connection = sqlite3.connect('kol_scores.db')
    return connection

def main():
    kol_list = load_kol_list('./top500.csv')
    kol_screen_names = load_kol_screen_names('./top500.csv')
    secondary_kol_list = build_secondary_kol_list([kol['screen_name'] for kol in kol_list], kol_screen_names)
    relationship_graph = build_relationship_graph(kol_list, secondary_kol_list)
    scores = calculate_weighted_scores(relationship_graph)
    db_connection = create_db_connection()
    save_scores_to_db(scores, db_connection)

if __name__ == "__main__":
    main()

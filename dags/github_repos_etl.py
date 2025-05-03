from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import pandas as pd
import psycopg2
import os
from dotenv import load_dotenv
load_dotenv()

def extract_popular_repos():
    languages = ['python', 'javascript', 'java', 'go']
    repos_data = []
    
    for lang in languages:
        url = 'https://api.github.com/search/repositories'
        params = {
            'q': f'language:{lang} stars:>1000',
            'sort': 'stars',
            'per_page': 20
        }
        headers = {'Authorization': f'token {os.getenv("GITHUB_TOKEN")}'}
        
        response = requests.get(url, headers=headers, params=params)
        for repo in response.json()['items']:
            repos_data.append({
                'name': repo['full_name'],
                'language': lang,
                'stars': repo['stargazers_count'],
                'forks': repo['forks_count'],
                'open_issues': repo['open_issues_count'],
                'last_updated': repo['updated_at']
            })
    
    df = pd.DataFrame(repos_data)
    df['stars_forks_ratio'] = df['stars'] / df['forks']
    df.to_csv('/data/popular_repos.csv', index=False)

def load_popular_repos():
    conn = psycopg2.connect(host='postgres', dbname='etl_project', user='airflow', password='airflow')
    cur = conn.cursor()
    
    cur.execute('''
    CREATE TABLE IF NOT EXISTS popular_repos (
        name TEXT PRIMARY KEY,
        language TEXT,
        stars INTEGER,
        forks INTEGER,
        open_issues INTEGER,
        last_updated TIMESTAMP,
        stars_forks_ratio NUMERIC(10,2)
    ''')
    
    with open('/data/popular_repos.csv', 'r') as f:
        next(f)
        cur.copy_expert("COPY popular_repos FROM STDIN WITH CSV", f)
    conn.commit()
    cur.close()
    conn.close()

dag = DAG(
    'github_popular_repos',
    schedule_interval='@weekly',
    default_args={
        'start_date': datetime(2023, 1, 1),
        'retries': 3,
        'retry_delay': timedelta(minutes=5)
    },
    catchup=False
)

with dag:
    extract = PythonOperator(task_id='extract_popular_repos', python_callable=extract_popular_repos)
    load = PythonOperator(task_id='load_popular_repos', python_callable=load_popular_repos)
    extract >> load
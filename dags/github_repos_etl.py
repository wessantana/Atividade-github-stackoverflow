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
    import json  # útil para debug em caso de erro
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

        # Verifica erro na resposta
        if response.status_code != 200:
            print(f"Erro na API do GitHub ({response.status_code}): {response.text}")
            continue

        data = response.json()
        if 'items' not in data:
            print(f"Resposta inesperada da API para {lang}: {json.dumps(data)}")
            continue

        for repo in data['items']:
            forks = repo['forks_count'] if repo['forks_count'] != 0 else 1  # evita divisão por zero
            repos_data.append({
                'name': repo['full_name'],
                'language': lang,
                'stars': repo['stargazers_count'],
                'forks': repo['forks_count'],
                'open_issues': repo['open_issues_count'],
                'last_updated': repo['updated_at'],
                'stars_forks_ratio': repo['stargazers_count'] / forks
            })

    if repos_data:
        df = pd.DataFrame(repos_data)
        df.to_csv('/data/popular_repos.csv', index=False)
    else:
        print("Nenhum dado foi extraído da API.")


def load_popular_repos():
    conn = psycopg2.connect(host='postgres', dbname='airflow', user='airflow', password='airflow')
    cur = conn.cursor()
    
    cur.execute('''
    CREATE TABLE IF NOT EXISTS popular_repos (
        name TEXT PRIMARY KEY,
        language TEXT,
        stars INTEGER,
        forks INTEGER,
        open_issues INTEGER,
        last_updated TIMESTAMP,
        stars_forks_ratio NUMERIC(10,2))
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
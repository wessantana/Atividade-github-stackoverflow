from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import pandas as pd
import psycopg2
import os
from dotenv import load_dotenv
load_dotenv()

def extract_and_analyze_github_activity():
    """Extrai e analisa dados de atividade de repositórios do GitHub"""
    repos = ['tensorflow/tensorflow', 'microsoft/vscode', 'pytorch/pytorch']
    analysis_results = []
    
    for repo in repos:
        commits_url = f'https://api.github.com/repos/{repo}/stats/commit_activity'
        token = os.getenv('GITHUB_TOKEN')
        headers = {
            'Accept': 'application/vnd.github.v3+json',
            'Authorization': f'token {token}'
        }
        
        repo_url = f'https://api.github.com/repos/{repo}'
        repo_data = requests.get(repo_url, headers=headers).json()
        
        commits_data = requests.get(commits_url, headers=headers).json()
        if not isinstance(commits_data, list):
            continue
            
        total_commits = sum(week['total'] for week in commits_data)
        avg_commits = total_commits / len(commits_data) if commits_data else 0
        
        issues_url = f'https://api.github.com/repos/{repo}/issues?state=all'
        issues_data = requests.get(issues_url, headers=headers).json()
        
        analysis_results.append({
            'repository': repo,
            'stars': repo_data.get('stargazers_count', 0),
            'forks': repo_data.get('forks_count', 0),
            'total_commits_last_year': total_commits,
            'avg_weekly_commits': round(avg_commits, 2),
            'open_issues': repo_data.get('open_issues_count', 0),
            'language': repo_data.get('language', 'Unknown'),
            'last_updated': repo_data.get('updated_at', ''),
            'analysis_date': datetime.now().strftime('%Y-%m-%d')
        })
    
    df = pd.DataFrame(analysis_results)
    df['commits_per_star'] = df['total_commits_last_year'] / df['stars']
    df['activity_score'] = (df['avg_weekly_commits'] * 0.6 + df['open_issues'] * 0.4)
    
    df.to_csv('/data/github_analysis.csv', index=False)

def load_analysis_to_db():
    """Carrega os resultados analisados no banco de dados"""
    conn = psycopg2.connect(
        host='postgres',
        dbname='etl_project',
        user='airflow',
        password='airflow'
    )
    cur = conn.cursor()
    
    cur.execute('''
    CREATE TABLE IF NOT EXISTS repo_analysis (
        repository TEXT PRIMARY KEY,
        stars INTEGER,
        forks INTEGER,
        total_commits_last_year INTEGER,
        avg_weekly_commits NUMERIC(10,2),
        open_issues INTEGER,
        language TEXT,
        last_updated TIMESTAMP,
        analysis_date DATE,
        commits_per_star NUMERIC(10,4),
        activity_score NUMERIC(10,2)
    )
    ''')
    
    df = pd.read_csv('/data/github_analysis.csv')
    for _, row in df.iterrows():
        cur.execute('''
        INSERT INTO repo_analysis VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (repository) DO UPDATE SET
            stars = EXCLUDED.stars,
            forks = EXCLUDED.forks,
            total_commits_last_year = EXCLUDED.total_commits_last_year,
            avg_weekly_commits = EXCLUDED.avg_weekly_commits,
            open_issues = EXCLUDED.open_issues,
            language = EXCLUDED.language,
            last_updated = EXCLUDED.last_updated,
            analysis_date = EXCLUDED.analysis_date,
            commits_per_star = EXCLUDED.commits_per_star,
            activity_score = EXCLUDED.activity_score
        ''', tuple(row))
    
    conn.commit()
    cur.close()
    conn.close()

default_args = {
    'owner': 'airflow',
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'github_repo_analysis',
    default_args=default_args,
    description='Análise semanal de repositórios GitHub',
    schedule_interval='@weekly',
    start_date=datetime(2023, 1, 1),
    catchup=False
)

with dag:
    extract_and_analyze = PythonOperator(
        task_id='extract_and_analyze_github_activity',
        python_callable=extract_and_analyze_github_activity
    )
    
    load_analysis = PythonOperator(
        task_id='load_analysis_to_db',
        python_callable=load_analysis_to_db
    )
    
    extract_and_analyze >> load_analysis
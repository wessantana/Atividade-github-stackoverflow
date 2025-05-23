from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime, timedelta
import pandas as pd
import psycopg2
import os
from dotenv import load_dotenv
load_dotenv()

def analyze_correlation():
    conn = psycopg2.connect(host='postgres', dbname='airflow', user='airflow', password='airflow')
    language_tag_map = {
    'python': 'python',
    'javascript': 'javascript',
    'typescript': 'typescript',
    'c#': 'c-sharp',
    'c++': 'c++',
    'java': 'java',
    'php': 'php',
    'go': 'go',
    'ruby': 'ruby',
    'rust': 'rust',
    'kotlin': 'kotlin',
    'swift': 'swift',
    }

    
    github_df = pd.read_sql('''
        SELECT language, AVG(stars) as avg_stars, 
               SUM(total_commits_last_year) as total_commits
        FROM public.repo_analysis
        GROUP BY language
    ''', conn)
    
    stack_df = pd.read_sql('''
        SELECT tags,
               COUNT(*) as question_count,
               AVG(answer_count) as avg_answers,
               AVG(score) as avg_score,
               AVG(CASE WHEN is_answered THEN 1 ELSE 0 END) as answer_ratio
        FROM public.stackoverflow_questions
        GROUP BY tags
    ''', conn)
    
    github_df['language'] = github_df['language'].str.lower().str.strip()
    stack_df['tags'] = stack_df['tags'].str.lower().str.strip()

    github_df = github_df[github_df['language'].isin(language_tag_map.keys())].copy()
    github_df['tag'] = github_df['language'].map(language_tag_map)

    merged_df = pd.merge(
        github_df,
        stack_df,
        left_on='tag',
        right_on='tags',
        how='inner'
    )
    
    merged_df['questions_per_star'] = merged_df['question_count'] / merged_df['avg_stars']
    merged_df['commits_per_question'] = merged_df['total_commits'] / merged_df['question_count']
    merged_df.drop(columns=['tags', 'tag'], inplace=True)

    merged_df.to_csv('/data/correlation_analysis.csv', index=False)
    conn.close()

def load_correlation_data():
    conn = psycopg2.connect(host='postgres', dbname='airflow', user='airflow', password='airflow')
    cur = conn.cursor()
    
    cur.execute('''

        CREATE TABLE IF NOT EXISTS public.github_stack_correlation (
            id SERIAL PRIMARY KEY,
            language TEXT,
            avg_stars NUMERIC(10,2),
            total_commits INTEGER,
            question_count INTEGER,
            avg_answers NUMERIC(10,2),
            avg_score NUMERIC(10,2),
            answer_ratio NUMERIC(10,4),
            questions_per_star NUMERIC(10,4),
            commits_per_question NUMERIC(10,2)
        );
    ''')
    
    with open('/data/correlation_analysis.csv', 'r') as f:
        next(f)
        cur.copy_expert("COPY github_stack_correlation (language, avg_stars, total_commits, question_count, avg_answers, avg_score, answer_ratio, questions_per_star, commits_per_question) FROM STDIN WITH CSV", f)
        conn.commit()
        cur.close()
        conn.close()

dag = DAG(
    'github_stack_correlation',
    schedule_interval='@weekly',
    default_args={
        'start_date': datetime(2023, 1, 1),
        'retries': 2,
        'retry_delay': timedelta(minutes=5)
    },
    catchup=False
)

with dag:
    wait_for_repo_analysis = ExternalTaskSensor(
        task_id='wait_for_repo_analysis',
        external_dag_id='github_repo_analysis',
        external_task_id='load_analysis_to_db',
        mode='reschedule',
        timeout=3600
    )

    wait_for_stack_analysis = ExternalTaskSensor(
        task_id='wait_for_stack_analysis',
        external_dag_id='stackoverflow_analysis',
        external_task_id='load_stackoverflow_data',
        mode='reschedule',
        timeout=3600
    )

    analyze = PythonOperator(task_id='analyze_correlation', python_callable=analyze_correlation)
    load = PythonOperator(task_id='load_correlation_data', python_callable=load_correlation_data)

    [wait_for_repo_analysis, wait_for_stack_analysis] >> analyze >> load

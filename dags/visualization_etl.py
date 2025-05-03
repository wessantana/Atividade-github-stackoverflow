from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv
load_dotenv()

def generate_visualization_data():
    conn = psycopg2.connect(host='postgres', dbname='etl_project', user='airflow', password='airflow')
    
    dashboard_data = pd.read_sql('''
        SELECT 
            r.language,
            AVG(r.stars) as avg_stars,
            AVG(r.avg_weekly_commits) as avg_commits,
            COUNT(s.question_id) as question_count,
            AVG(s.answer_ratio) as answer_ratio,
            c.questions_per_star
        FROM repo_analysis r
        LEFT JOIN stackoverflow_questions s ON r.language = ANY(string_to_array(s.tags, ', '))
        LEFT JOIN github_stack_correlation c ON r.language = c.language
        GROUP BY r.language, c.questions_per_star
    ''', conn)
    
    engine = create_engine('postgresql://airflow:airflow@postgres/etl_project')
    dashboard_data.to_sql('dashboard_data', engine, if_exists='replace', index=False)
    conn.close()

dag = DAG(
    'visualization_pipeline',
    schedule_interval='@weekly',
    default_args={'start_date': datetime(2023, 1, 1)},
    catchup=False
)

with dag:
    generate_viz_data = PythonOperator(
        task_id='generate_visualization_data',
        python_callable=generate_visualization_data
    )
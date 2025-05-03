from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import pandas as pd
import psycopg2
import os
from dotenv import load_dotenv
load_dotenv()

def analyze_stackoverflow_questions():
    tags = ['python', 'javascript', 'java', 'docker']
    questions_data = []
    
    for tag in tags:
        url = 'https://api.stackexchange.com/2.3/questions'
        params = {
            'tagged': tag,
            'site': 'stackoverflow',
            'pagesize': 50,
            'sort': 'creation',
            'key': os.getenv('STACKOVERFLOW_KEY')
        }
        
        response = requests.get(url, params=params)
        for question in response.json()['items']:
            questions_data.append({
                'question_id': question['question_id'],
                'title': question['title'],
                'tags': ', '.join(question['tags']),
                'view_count': question['view_count'],
                'answer_count': question['answer_count'],
                'score': question['score'],
                'creation_date': datetime.fromtimestamp(question['creation_date']).strftime('%Y-%m-%d'),
                'is_answered': question['is_answered']
            })
    
    df = pd.DataFrame(questions_data)
    df['answer_ratio'] = df['answer_count'] / df['view_count']
    df.to_csv('/data/stackoverflow_questions.csv', index=False)

def load_stackoverflow_data():
    conn = psycopg2.connect(host='postgres', dbname='etl_project', user='airflow', password='airflow')
    cur = conn.cursor()
    
    cur.execute('''
    CREATE TABLE IF NOT EXISTS stackoverflow_questions (
        question_id INTEGER PRIMARY KEY,
        title TEXT,
        tags TEXT,
        view_count INTEGER,
        answer_count INTEGER,
        score INTEGER,
        creation_date DATE,
        is_answered BOOLEAN,
        answer_ratio NUMERIC(10,4))
    ''')
    
    with open('/data/stackoverflow_questions.csv', 'r') as f:
        next(f)
        cur.copy_expert("COPY stackoverflow_questions FROM STDIN WITH CSV", f)
    conn.commit()
    cur.close()
    conn.close()

dag = DAG(
    'stackoverflow_analysis',
    schedule_interval='@daily',
    default_args={
        'start_date': datetime(2023, 1, 1),
        'retries': 3,
        'retry_delay': timedelta(minutes=5)
    },
    catchup=False
)

with dag:
    extract = PythonOperator(task_id='analyze_stackoverflow', python_callable=analyze_stackoverflow_questions)
    load = PythonOperator(task_id='load_stackoverflow_data', python_callable=load_stackoverflow_data)
    extract >> load
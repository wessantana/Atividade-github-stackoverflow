from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import pandas as pd
import psycopg2
import os
import csv
from dotenv import load_dotenv
load_dotenv()

def analyze_stackoverflow_questions():
    """Extrai e analisa perguntas do StackOverflow por tags"""
    tags = ['python', 'javascript', 'java', 'docker']
    questions_data = []
    api_key = os.getenv('STACKOVERFLOW_KEY')

    for tag in tags:
        url = 'https://api.stackexchange.com/2.3/questions'
        params = {
            'tagged': tag,
            'site': 'stackoverflow',
            'pagesize': 50,
            'sort': 'creation',
            'key': api_key
        }
        resp = requests.get(url, params=params)
        data = resp.json().get('items', [])

        for q in data:
            questions_data.append({
                'question_id': q.get('question_id'),
                'title': q.get('title'),
                'tags': ','.join(q.get('tags', [])),
                'view_count': q.get('view_count', 0),
                'answer_count': q.get('answer_count', 0),
                'score': q.get('score', 0),
                'creation_date': datetime.utcfromtimestamp(q.get('creation_date')).strftime('%Y-%m-%d'),
                'is_answered': q.get('is_answered', False)
            })

    df = pd.DataFrame(questions_data)
    if df.empty:
        print("[WARN] Nenhum dado coletado para StackOverflow.")
        return
    df['answer_ratio'] = df.apply(
        lambda row: row['answer_count'] / row['view_count'] if row['view_count'] > 0 else 0,
        axis=1
    )
    csv_path = '/data/stackoverflow_questions.csv'
    try:
        df.to_csv(csv_path, index=False)
        print(f"[INFO] CSV salvo em: {csv_path}")
    except Exception as e:
        print(f"[ERROR] Falha ao salvar CSV: {e}")


def load_stackoverflow_data():
    """Carrega o CSV de perguntas no banco de dados"""
    csv_path = '/data/stackoverflow_questions.csv'
    if not os.path.exists(csv_path):
        print(f"[WARN] CSV não encontrado: {csv_path}")
        return

    conn = psycopg2.connect(host='postgres', port=5432, dbname='airflow', user='airflow', password='airflow')
    cur = conn.cursor()

    # Remove tabela existente para evitar conflitos
    cur.execute("DROP TABLE IF EXISTS public.stackoverflow_questions CASCADE;")
    cur.execute('''
        CREATE TABLE public.stackoverflow_questions (
            question_id INTEGER PRIMARY KEY,
            title TEXT,
            tags TEXT,
            view_count INTEGER,
            answer_count INTEGER,
            score INTEGER,
            creation_date DATE,
            is_answered BOOLEAN,
            answer_ratio NUMERIC(10,4)
        )
    ''')

    with open(csv_path, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            qid = int(row['question_id'])
            created = row['creation_date']
            score = int(row['score'])
            views = int(row['view_count'])
            answers = int(row['answer_count'])
            answered = row['is_answered'].lower() == 'true'
            ratio = float(row['answer_ratio'])

            cur.execute('''
                INSERT INTO public.stackoverflow_questions (
                    question_id, title, tags, view_count,
                    answer_count, score, creation_date, is_answered, answer_ratio
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (question_id) DO NOTHING
            ''', (qid, row['title'], row['tags'], views, answers, score, created, answered, ratio))

    conn.commit()
    cur.close()
    conn.close()

# Definição do DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'stackoverflow_analysis',
    default_args=default_args,
    description='Extrai e carrega dados do StackOverflow por tags',
    schedule_interval='@daily',
    catchup=False
)

with dag:
    extract = PythonOperator(
        task_id='analyze_stackoverflow_questions',
        python_callable=analyze_stackoverflow_questions
    )
    load = PythonOperator(
        task_id='load_stackoverflow_data',
        python_callable=load_stackoverflow_data
    )

    extract >> load
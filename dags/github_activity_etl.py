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
    token = os.getenv('GITHUB_TOKEN')
    headers = {
        'Accept': 'application/vnd.github.v3+json',
        'Authorization': f'token {token}'
    }

    print("[INFO] Buscando repositórios mais ativos...")
    search_url = 'https://api.github.com/search/repositories?q=pushed:>2024-12-01+stars:>1000&sort=updated&order=desc&per_page=75'
    search_resp = requests.get(search_url, headers=headers)

    if search_resp.status_code != 200:
        print(f"[ERROR] Erro ao buscar repositórios ativos: {search_resp.json().get('message')}")
        return

    repos_data = search_resp.json().get('items', [])
    repos = [repo['full_name'] for repo in repos_data]

    if not repos:
        print("[WARN] Nenhum repositório encontrado.")
        return

    analysis_results = []

    for repo in repos:
        print(f"[INFO] Analisando {repo}")
        repo_resp = requests.get(f'https://api.github.com/repos/{repo}', headers=headers)
        repo_data = repo_resp.json()

        if 'message' in repo_data:
            print(f"[ERROR] Erro ao acessar dados do repositório {repo}: {repo_data['message']}")
            continue

        # otimizar isso. Provavelmente não é necessário fazer outra requisição.
        commits_resp = requests.get(f'https://api.github.com/repos/{repo}/stats/commit_activity', headers=headers)

        if commits_resp.status_code == 202:
            print(f"[INFO] Dados de commits para {repo} ainda estão sendo processados. Pulando...")
            continue

        commits_data = commits_resp.json()

        if commits_data:
            total_commits = sum(week.get('total', 0) for week in commits_data)
            avg_commits = total_commits / len(commits_data)
        else:
            msg = commits_data.get('message') if isinstance(commits_data, dict) else 'Resposta inválida'
            print(f"[WARN] Erro ao obter commits de {repo}: {msg}")
            total_commits, avg_commits = 0, 0

        analysis_results.append({
            'repository': repo,
            'stars': repo_data.get('stargazers_count', 0),
            'forks': repo_data.get('forks_count', 0),
            'total_commits_last_year': total_commits,
            'avg_weekly_commits': round(avg_commits, 2),
            'open_issues': repo_data.get('open_issues_count', 0),
            'language': repo_data.get('language', 'Unknown'),
            'last_updated': repo_data.get('updated_at'),
            'analysis_date': datetime.utcnow().date()
        })

    df = pd.DataFrame(analysis_results).drop_duplicates()
    if df.empty:
        print("[WARN] DataFrame vazio! Nenhum dado foi coletado.")
        return

    df['commits_per_star'] = df.apply(
        lambda row: row['total_commits_last_year'] / row['stars'] if row['stars'] > 0 else 0,
        axis=1
    )
    df['activity_score'] = df['avg_weekly_commits'] * 0.6 + df['open_issues'] * 0.4

    csv_path = '/data/github_analysis.csv'
    try:
        df.to_csv(csv_path, index=False)
        print(f"[INFO] CSV criado com sucesso em: {csv_path}")
    except Exception as e:
        print(f"[ERROR] Falha ao salvar CSV: {e}")


def load_analysis_to_db():
    """Carrega os resultados analisados no banco de dados"""
    conn = psycopg2.connect(host='postgres', port=5432, dbname='airflow', user='airflow', password='airflow')
    cur = conn.cursor()

    cur.execute('''
        CREATE TABLE IF NOT EXISTS public.repo_analysis (
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
        );

        DO $$
        BEGIN
            IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE tablename = 'repo_analysis' AND indexname = 'repo_analysis_pkey') THEN
                CREATE INDEX repo_analysis_pkey ON public.repo_analysis (repository);
            END IF;
        END $$;
    ''')

    csv_path = '/data/github_analysis.csv'
    if not os.path.exists(csv_path):
        print(f"[WARN] Arquivo CSV não encontrado em: {csv_path}")
        conn.commit(); cur.close(); conn.close(); return

    df = pd.read_csv(csv_path)
    for _, row in df.iterrows():
        last_updated = None if pd.isna(row['last_updated']) else row['last_updated']
        cur.execute('''
        INSERT INTO public.repo_analysis (
            repository, stars, forks, total_commits_last_year,
            avg_weekly_commits, open_issues, language,
            last_updated, analysis_date, commits_per_star, activity_score
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
        ''', (
            row['repository'], row['stars'], row['forks'],
            row['total_commits_last_year'], row['avg_weekly_commits'], row['open_issues'],
            row['language'], last_updated, row['analysis_date'], row['commits_per_star'],
            row['activity_score']
        ))

    conn.commit(); cur.close(); conn.close()


default_args = {'owner': 'airflow', 'retries': 3, 'retry_delay': timedelta(minutes=5)}

dag = DAG(
    'github_repo_analysis', default_args=default_args,
    description='Análise semanal de repositórios GitHub', schedule_interval='@weekly',
    start_date=datetime(2023, 1, 1), catchup=False
)

with dag:
    extract_and_analyze = PythonOperator(
        task_id='extract_and_analyze_github_activity', python_callable=extract_and_analyze_github_activity
    )
    load_analysis = PythonOperator(
        task_id='load_analysis_to_db', python_callable=load_analysis_to_db
    )
    extract_and_analyze >> load_analysis

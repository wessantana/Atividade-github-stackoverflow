# Projeto de Análise ETL: GitHub + Stack Overflow

![Airflow](https://img.shields.io/badge/Apache%20Airflow-017CEE?logo=apacheairflow&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-4169E1?logo=postgresql&logoColor=white)
![Python](https://img.shields.io/badge/Python-3776AB?logo=python&logoColor=white)

Pipeline ETL para análise integrada de atividade de desenvolvimento de software e comunidades técnicas.

## 📋 Sumário

- [Arquitetura do Projeto](#-arquitetura-do-projeto)
- [Pré-requisitos](#-pré-requisitos)
- [Configuração](#-configuração)
- [Execução](#-execução)
- [Estrutura dos DAGs](#-estrutura-dos-dags)
- [Dashboard e Visualização](#-dashboard-e-visualização)
- [Testes](#-testes)
- [Melhorias Futuras](#-melhorias-futuras)
- [Contribuição](#-contribuição)

## 🌐 Arquitetura do Projeto

```mermaid
graph LR
    A[GitHub API] --> B[Extract]
    C[Stack Overflow API] --> B
    B --> D[Transform]
    D --> E[(PostgreSQL)]
    E --> F[Visualização]
    F --> G[Power BI/Tableau]
🛠️ Pré-requisitos
Docker 20.10+

Docker Compose 2.5+

Contas de desenvolvedor:

GitHub Personal Access Token

Stack Apps API Key

⚙️ Configuração
Crie um arquivo .env na raiz do projeto:

ini
# Tokens de API
GITHUB_TOKEN=seu_token_aqui
STACKOVERFLOW_KEY=sua_key_aqui

# Configurações do PostgreSQL
POSTGRES_USER=airflow
POSTGRES_PASSWORD=airflow
POSTGRES_DB=etl_project
Inicie os containers:

bash
docker-compose up -d --build
🚀 Execução
Acesse a interface do Airflow:

http://localhost:8080
Credenciais: admin / admin

DAGs disponíveis:

github_popular_repos (semanal)

stackoverflow_analysis (diário)

github_stack_correlation (semanal)

📊 Estrutura dos DAGs
github_repos_etl.py
python
"""
Extrai repositórios populares por linguagem:
- Input: GitHub API
- Métricas: stars, forks, issues
- Output: Tabela `popular_repos`
"""
correlation_etl.py
python
"""
Correlaciona dados GitHub/Stack Overflow:
- Input: `repo_analysis` + `stackoverflow_questions`
- Métricas: questions_per_star, commits_per_question
- Output: Tabela `github_stack_correlation`
"""
📈 Dashboard e Visualização
Conecte seu Power BI/Tableau ao PostgreSQL:

ini
Server: localhost
Database: etl_project
User: airflow
Password: airflow
Queries úteis:

sql
-- Top linguagens por atividade
SELECT language, AVG(activity_score) as avg_activity
FROM repo_analysis
GROUP BY language;
🧪 Testes
Para executar testes de qualidade de dados:

bash
docker exec -it airflow_webserver python -m pytest tests/
Exemplo de teste:

python
# tests/test_data_quality.py
def test_repo_analysis_schema():
    conn = psycopg2.connect(...)
    df = pd.read_sql("SELECT * FROM repo_analysis LIMIT 1", conn)
    assert {'repository', 'stars', 'language'}.issubset(df.columns)
🔮 Melhorias Futuras
Adicionar monitoramento com Prometheus

Implementar alertas para falhas nas APIs

Adicionar suporte a Elasticsearch

🤝 Contribuição
Faça um fork do projeto

Crie uma branch (git checkout -b feature/nova-feature)

Commit suas mudanças (git commit -m 'Adiciona nova funcionalidade')

Push para a branch (git push origin feature/nova-feature)

Abra um Pull Request

Nota: Documentação atualizada em {{DATA_ATUAL}} por {{SEU_NOME}}


### Partes para Completar Posteriormente:

1. **Seção "Equipe"**:
   ```markdown
   ## 👥 Equipe

   | Nome          | Função               | Contato               |
   |---------------|----------------------|-----------------------|
   | Exemplo       | Eng. de Dados        | exemplo@empresa.com   |
Exemplos de Dashboards:

markdown
## 📊 Exemplos de Visualização

![Dashboard](caminho/para/imagem.png)
Guia de Troubleshooting:

markdown
## 🛠 Troubleshooting

### Erro: "API GitHub retornou 403"
- Verifique se o token tem permissões suficientes
- Confira o rate limit com:
  ```bash
  curl -H "Authorization: token $GITHUB_TOKEN" https://api.github.com/rate_limit
Métricas de Negócio:

markdown
## 💡 Insights de Negócio

Métricas-chave:
- Crescimento semanal de repositórios Python
- Tempo médio de resposta no Stack Overflow
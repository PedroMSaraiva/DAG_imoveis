
import os
import datetime
import pendulum

import pandas as pd
from airflow.sdk import task, dag
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.models import Variable

from loguru import logger as logging
# Definir argumentos padrão do DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': pendulum.datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': pendulum.duration(minutes=5),
}

DAGS_FOLDER = os.environ.get('AIRFLOW_HOME', '/opt/airflow') + '/dags'

# Criar o DAG
@dag(
    dag_id='imoveis_data_pipeline',
    default_args=default_args,
    description='Pipeline para carregar dados de imóveis (CSV e Excel) para PostgreSQL',
    dagrun_timeout=pendulum.duration(hours=1),
    catchup=False,
    tags=['imoveis', 'postgresql', 'pandas'],
)



def ProcessDataPipeline():
    
    conn_id = "anuncios_pg_conn"

    create_anuncios_table = SQLExecuteQueryOperator(
        task_id="create_anuncios_table",
        conn_id=conn_id,
        sql="table_anuncios.sql",
    )

    create_analise_table = SQLExecuteQueryOperator(
        task_id="create_analise_table",
        conn_id=conn_id,
        sql="table_analise.sql",
    )
    
    @task
    def get_anuncios():
        """
        Carrega dados do anuncios.csv para a tabela anuncios no PostgreSQL
        usando pandas com integração nativa do Airflow
        """
        
        file_path = f"{DAGS_FOLDER}/anuncios.csv"
        
        try:
            df_anuncios = pd.read_csv(file_path, delimiter=';', encoding='latin-1')
            
            logging.info(f"CSV carregado com sucesso. Shape: {df_anuncios.shape}")
            logging.info(f"Colunas encontradas: {df_anuncios.columns.tolist()}")
            
            # Obter conexão PostgreSQL usando PostgresHook
            postgres_hook = PostgresHook(postgres_conn_id=conn_id)
            engine = postgres_hook.get_sqlalchemy_engine()
            
            # Usar pandas to_sql para inserir dados diretamente
            df_anuncios.to_sql(
                name='anuncios',
                con=engine,
                if_exists='replace',  # Substitui a tabela se existir
                index=False,          # Não incluir índice do pandas
                method='multi',       # Inserção em lote para melhor performance
                chunksize=1000       # Processar em lotes de 1000 registros
            )
            
            logging.info(f"Dados inseridos com sucesso na tabela 'anuncios'")
            logging.info(f"Total de registros inseridos: {len(df_anuncios)}")
            
            return f"Sucesso: {len(df_anuncios)} registros inseridos na tabela anuncios"
            
        except Exception as e:
            logging.error(f"Erro ao carregar dados do anuncios.csv: {str(e)}")
            raise e


    @task
    def get_analise():
        """
        Carrega dados do exemplo_analise.xlsx para a tabela analise no PostgreSQL
        usando pandas com integração nativa do Airflow
        """
        
        file_path = f"{DAGS_FOLDER}/exemplo_analise.xlsx"
        
        try:
            df_analise = pd.read_excel(file_path)
            
            logging.info(f"Excel carregado com sucesso. Shape: {df_analise.shape}")
            logging.info(f"Colunas encontradas: {df_analise.columns.tolist()}")
            
            # Renomear colunas para snake_case para compatibilidade com PostgreSQL
            df_analise.columns = df_analise.columns.str.lower().str.replace(' ', '_').str.replace('\n', '')
            
            # Obter conexão PostgreSQL usando PostgresHook
            postgres_hook = PostgresHook(postgres_conn_id=conn_id)
            engine = postgres_hook.get_sqlalchemy_engine()
            
            # Usar pandas to_sql para inserir dados diretamente
            df_analise.to_sql(
                name='analise',
                con=engine,
                if_exists='replace',  # Substitui a tabela se existir
                index=False,         # Não incluir índice do pandas
                method='multi',      # Inserção em lote para melhor performance
                chunksize=1000      # Processar em lotes de 1000 registros
            )
            
            logging.info(f"Dados inseridos com sucesso na tabela 'analise'")
            logging.info(f"Total de registros inseridos: {len(df_analise)}")
            
            return f"Sucesso: {len(df_analise)} registros inseridos na tabela analise"
            
        except Exception as e:
            logging.error(f"Erro ao carregar dados do exemplo_analise.xlsx: {str(e)}")
            raise e
        
    create_anuncios_table >> get_anuncios() >> create_analise_table >> get_analise()

dag = ProcessDataPipeline()
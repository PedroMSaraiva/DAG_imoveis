import requests
import pandas as pd
import pendulum

from airflow.sdk import task, dag
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.models import Variable
from airflow.utils.log.logging_mixin import LoggingMixin
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
from PIL import Image, ImageEnhance, ImageFilter
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager




default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 10, 22),
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

@dag(
    dag_id='crawler_pipeline',
    default_args=default_args,
    description='Pipeline para  lizar o Web Scraping e enriquecimento de imagens',
    dagrun_timeout=pendulum.duration(hours=1),
    catchup=False,
    tags=['crawler', 'postgresql', 'pandas'],
)

def crawl_pipenline():

    conn_id = "tutorial_pg_conn"

    
    log = LoggingMixin().log


    def get_data_postgress(slq_code : str):
        postgres_hook = PostgresHook(postgres_conn_id=conn_id)
        conn = postgres_hook.get_conn()
        cursor = conn.cursor()

        cursor.execute(slq_code)
        resultados = cursor.fetchall()

        return resultados
    

    def post_data_postgress(data, table_name : str):
        postgres_hook = PostgresHook(postgres_conn_id=conn_id)
        engine = postgres_hook.get_sqlalchemy_engine()
        data.to_sql(
                name=table_name,
                con=engine,
                if_exists='replace',  # Substitui a tabela se existir
                index=False,          # Não incluir índice do pandas
                method='multi',       # Inserção em lote para melhor performance
                chunksize=1000       # Processar em lotes de 1000 registros
            )


    @task
    def crawl_sites():
        resultados = get_data_postgress("SELECT url FROM anuncios;")
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service)


        for site in resultados:
            url = site[0]
            if not url.startswith("http"):
                url = "http://" + url

            try:
                driver.get(url)
                html = driver.page_source  
                print(html[:1000])  
            finally:
                driver.quit()

            soup = BeautifulSoup(html, "html.parser")

            log.info(f"Dados extraidos com o soup: {soup}")

            data = []
            """for item in soup.select(".card-imovel"):
                titulo = item.select_one(".titulo").text.strip()
                preco = item.select_one(".preco").text.strip()
                data.append({"titulo": titulo, "preco": preco})
                
                post_data_postgress(data, "anuncios_coletados")"""

           
    @task
    def image_extraction():
        resultados_consulta = get_data_postgress("SELECT imagens FROM anuncios;")

        for img_path in resultados_consulta:
            with Image.open(img_path[0]) as img:
                # Converte para RGB (garante 3 canais)
                img = img.convert("RGB")

                # Redimensiona para 224x224
                img = img.resize((224, 224))

                # Melhora contraste e brilho (ajuste leve)
                img = ImageEnhance.Contrast(img).enhance(1.1)
                img = ImageEnhance.Brightness(img).enhance(1.05)

                # Filtro leve de nitidez
                img = img.filter(ImageFilter.SHARPEN)

                # post_data_postgress(data, "anuncios_coletados")
    

    crawl_sites() >> image_extraction()



dag = crawl_pipenline()
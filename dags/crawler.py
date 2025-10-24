import requests
import pandas as pd
import pendulum
import time

from airflow.sdk import task, dag
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.models import Variable
from airflow.utils.log.logging_mixin import LoggingMixin
from datetime import datetime, timedelta

from PIL import Image, ImageEnhance, ImageFilter
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from io import BytesIO
from urllib.parse import quote




default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 10, 22),
    "retries": 0,
    "retry_delay": timedelta(minutes=0.1),
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


    def image_to_bytes(img):
        buffer = BytesIO()
        img.save(buffer, format="JPEG")
        return buffer.getvalue()


    def get_data_postgress(query : str):
        postgres_hook = PostgresHook(postgres_conn_id=conn_id)
        engine = postgres_hook.get_sqlalchemy_engine()

        return pd.read_sql(query, engine)
    

    def post_data_postgress(data, table_name : str):
        postgres_hook = PostgresHook(postgres_conn_id=conn_id)
        engine = postgres_hook.get_sqlalchemy_engine()

        if isinstance(data, list):
            df = pd.DataFrame(data)
        else:
            df = data

        df.to_sql(
                name=table_name,
                con=engine,
                if_exists='replace',  # Substitui a tabela se existir
                index=False,          # Não incluir índice do pandas
                method='multi',       # Inserção em lote para melhor performance
                chunksize=1000       # Processar em lotes de 1000 registros
            )
        

    def selenium(url):
        options = Options()
        options.add_argument("--headless=new")
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--no-sandbox")
        options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                            "AppleWebKit/537.36 (KHTML, like Gecko) "
                            "Chrome/120.0.0.0 Safari/537.36")
        
        driver = webdriver.Remote(
                command_executor="http://chrome:4444/wd/hub",
                options=options
        )  

        try: 
            driver.get(url)

            time.sleep(1)

            html = driver.page_source   
            titulo = driver.title

            url_info = []

            for tag in ["h1", "h2", "h3"]:
                headers = [el.text for el in driver.find_elements(By.TAG_NAME, tag) if el.text.strip()]
                if headers:
                    for h in headers:
                        url_info.append(h) 
        finally:
            driver.quit()  

        return url_info
    

    def image_extraction(images):
        img_list = []
    

        for item in images:
            item = item.strip("{}")
            images_extracted = [u.strip() for u in item.split(',')]

            for url in images_extracted:
                response = requests.get(url, timeout=10)

                try:
                    with Image.open(BytesIO(response.content)) as img:
                        img = img.convert("RGB")
                        img = img.resize((224, 224))
                        img = ImageEnhance.Contrast(img).enhance(1.1)
                        img = ImageEnhance.Brightness(img).enhance(1.05)
                        img = img.filter(ImageFilter.SHARPEN)
                        img = image_to_bytes(img)
                        img_list.append(img)
                except Exception as e:
                    log.warning(f"PIL não conseguiu identificar imagem: {response.content}")
                    placeholder = Image.new("RGB", (224, 224), color=(0, 0, 0))
                    placeholder = image_to_bytes(placeholder)
                    img_list.append(placeholder)
                    continue

       
        return img_list


    @task
    def crawl_sites():
        df = get_data_postgress("""
            SELECT url, titulo, descricao, imagens
            FROM anuncios;
        """)

        urls = df["url"].tolist()
        titulos = df["titulo"].tolist()
        descricoes = df["descricao"].tolist()
        imagens = df["imagens"].tolist() 

        data = []

        image_list = image_extraction(imagens)

        for url, titulo, descricao, img in zip(urls, titulos, descricoes, image_list):
            if not url.startswith("http"):
                url = "http://" + url

            html = selenium(url)  # executa o scraping para essa URL específica

            data.append({
                "titulo": titulo,
                "description": descricao,
                "scrapping": html,
                "url": url,
                "images": img
            })

        post_data_postgress(data, "anuncios_coletados")


    crawl_sites()

dag = crawl_pipenline()
import torch
import timm
import pandas as pd
import pendulum
from io import BytesIO
from PIL import Image
from torchvision import transforms
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
from airflow.sdk import task, dag

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

def multimodal_ai():

    def get_data_postgres(query: str, conn_id: str = "tutorial_pg_conn") -> pd.DataFrame:
        """Busca dados no PostgreSQL e retorna como DataFrame."""
        postgres_hook = PostgresHook(postgres_conn_id=conn_id)
        engine = postgres_hook.get_sqlalchemy_engine()
        return pd.read_sql(query, engine)


    def load_timm_model(model_name: str = "resnet18", pretrained: bool = True):
        """Carrega modelo da biblioteca TIMM."""
        model = timm.create_model(model_name, pretrained=pretrained)
        model.eval()  # modo inferência
        return model


    def preprocess_image(img_bytes: bytes) -> torch.Tensor:
        """Transforma bytes de imagem em tensor normalizado."""


        image = Image.open(BytesIO(img_bytes)).convert("RGB")
        return image  # adiciona dimensão batch


    def predict_images(model, df: pd.DataFrame) -> pd.DataFrame:
        """Executa predição com o modelo timm nas imagens da tabela."""
        preds = []

        for _, row in df.iterrows():
            try:
                img_tensor = preprocess_image(row["images"])
                with torch.no_grad():
                    output = model(img_tensor)
                    predicted_class = torch.argmax(output, dim=1).item()
                preds.append(predicted_class)
            except Exception as e:
                preds.append(None)
                print(f"Erro ao processar imagem: {e}")

        df["pred_class"] = preds
        return df

    @task
    def run_pipeline():
        """Executa o pipeline completo:
        1. Busca dados do Postgres
        2. Carrega modelo TIMM
        3. Gera predições
        4. Retorna DataFrame enriquecido
        """
        print("🔹 Iniciando pipeline...")

        df = get_data_postgres("""
            SELECT url, titulo, descricao, imagens
            FROM anuncios_coletados;
        """)

        if df.empty:
            print("⚠️ Nenhum dado encontrado na tabela.")
            return

        print(f"✅ {len(df)} registros carregados do banco.")

        model = load_timm_model("resnet18")

        print("🧠 Gerando predições...")
        df_pred = predict_images(model, df)

        print(df_pred[["titulo", "pred_class"]].head())

        print("✅ Pipeline finalizado com sucesso.")
        
        df_pred.to_csv("kaggle", index=False)

    run_pipeline()
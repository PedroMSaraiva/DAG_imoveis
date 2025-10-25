import pandas as pd
import pendulum
import base64
import json
import os
from io import BytesIO
from PIL import Image
import google.generativeai as genai
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from datetime import datetime, timedelta
from airflow.sdk import task, dag
#from dotenv import load_dotenv

#load_dotenv()   

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 10, 22),
    "retries": 0,
    "retry_delay": timedelta(minutes=0.1),
}

@dag(
    dag_id='inferencia_ai_pipeline',
    default_args=default_args,
    description='Pipeline para  lizar a inferencia de imagens com AI',
    dagrun_timeout=pendulum.duration(hours=1),
    catchup=False,
    tags=['ai', 'postgresql', 'pandas'],
)

def multimodal_ai():

    def get_gemini_api_key():
        """Obtém a API key do Gemini de forma segura."""
        # Opção 1: Variável de ambiente
        api_key = os.getenv('GEMINI_API_KEY')
        if api_key:
            return api_key
        
        # Opção 2: Airflow Variable
        try:
            api_key = Variable.get("gemini_api_key")
            if api_key:
                return api_key
        except Exception:
            pass
        
        return None

    def get_data_postgres(query: str, conn_id: str = "tutorial_pg_conn") -> pd.DataFrame:
        """Busca dados no PostgreSQL e retorna como DataFrame."""
        postgres_hook = PostgresHook(postgres_conn_id=conn_id)
        engine = postgres_hook.get_sqlalchemy_engine()
        return pd.read_sql(query, engine)


    def configure_gemini_api(api_key: str):
        """Configura a API do Gemini."""
        genai.configure(api_key=api_key)
        return genai.GenerativeModel('gemini-1.5-flash')


    def encode_image_to_base64(img_bytes: bytes) -> str:
        """Converte bytes de imagem para base64."""
        try:
            image = Image.open(BytesIO(img_bytes)).convert("RGB")
            # Redimensiona a imagem se necessário para economizar tokens
            image.thumbnail((1024, 1024), Image.Resampling.LANCZOS)
            
            buffer = BytesIO()
            image.save(buffer, format='JPEG', quality=85)
            img_base64 = base64.b64encode(buffer.getvalue()).decode('utf-8')
            return img_base64
        except Exception as e:
            print(f"Erro ao processar imagem: {e}")
            return None


    def classify_images_with_gemini(model, df: pd.DataFrame, api_key: str) -> pd.DataFrame:
        """Classifica imagens usando a API do Gemini."""
        classifications = []
        
        for _, row in df.iterrows():
            try:
                # Processa todas as imagens da linha
                images_data = row["images"]
                
                # Se images_data é uma string JSON, converte para lista
                if isinstance(images_data, str):
                    try:
                        images_list = json.loads(images_data)
                    except json.JSONDecodeError:
                        images_list = [images_data]  # Assume que é uma única URL
                elif isinstance(images_data, bytes):
                    images_list = [images_data]
                else:
                    images_list = images_data if isinstance(images_data, list) else [images_data]
                
                # Prepara as imagens para o Gemini
                gemini_images = []
                for img_data in images_list:
                    if isinstance(img_data, bytes):
                        img_base64 = encode_image_to_base64(img_data)
                        if img_base64:
                            gemini_images.append({
                                'mime_type': 'image/jpeg',
                                'data': img_base64
                            })
                
                if not gemini_images:
                    classifications.append({
                        "classification": "Erro: Nenhuma imagem válida encontrada",
                        "confidence": 0.0,
                        "details": "Não foi possível processar as imagens"
                    })
                    continue
                
                # Prepara o prompt para classificação
                prompt = f"""
                Analise estas imagens de imóveis e forneça uma classificação detalhada.
                
                Título do anúncio: {row.get('titulo', 'N/A')}
                Descrição: {row.get('descricao', 'N/A')}
                
                Por favor, classifique este imóvel considerando:
                1. Tipo de imóvel (casa, apartamento, terreno, etc.)
                2. Qualidade geral das imagens
                3. Estado de conservação
                4. Características visíveis (piscina, garagem, jardim, etc.)
                5. Estimativa de valor (baixo, médio, alto)
                
                Responda em formato JSON com os campos:
                - classification: classificação principal
                - confidence: nível de confiança (0.0 a 1.0)
                - details: detalhes adicionais
                - property_type: tipo de imóvel
                - estimated_value: estimativa de valor
                """
                
                # Configura o modelo Gemini
                gemini_model = configure_gemini_api(api_key)
                
                # Envia para o Gemini
                response = gemini_model.generate_content([prompt] + gemini_images)
                
                # Tenta extrair JSON da resposta
                try:
                    response_text = response.text
                    # Remove markdown se presente
                    if response_text.startswith('```json'):
                        response_text = response_text.replace('```json', '').replace('```', '').strip()
                    
                    classification_result = json.loads(response_text)
                    classifications.append(classification_result)
                    
                except json.JSONDecodeError:
                    # Se não conseguir fazer parse do JSON, usa a resposta como texto
                    classifications.append({
                        "classification": response.text[:200],  # Limita o tamanho
                        "confidence": 0.5,
                        "details": "Resposta não estruturada",
                        "property_type": "Não identificado",
                        "estimated_value": "Não determinado"
                    })
                
            except Exception as e:
                print(f"Erro ao processar linha: {e}")
                classifications.append({
                    "classification": f"Erro: {str(e)[:100]}",
                    "confidence": 0.0,
                    "details": "Erro no processamento",
                    "property_type": "Erro",
                    "estimated_value": "Erro"
                })
        
        # Adiciona as classificações ao DataFrame
        df["gemini_classification"] = [c["classification"] for c in classifications]
        df["gemini_confidence"] = [c["confidence"] for c in classifications]
        df["gemini_details"] = [c["details"] for c in classifications]
        df["property_type"] = [c.get("property_type", "N/A") for c in classifications]
        df["estimated_value"] = [c.get("estimated_value", "N/A") for c in classifications]
        
        return df

    @task
    def run_pipeline():
        """Executa o pipeline completo:
        1. Busca dados do Postgres
        2. Configura API do Gemini
        3. Classifica imagens com Gemini
        4. Retorna DataFrame enriquecido
        """
        print("🔹 Iniciando pipeline com Gemini...")

        # Busca dados do PostgreSQL
        df = get_data_postgres("""
            SELECT url, titulo, descricao, imagens
            FROM anuncios_coletados;
        """)

        if df.empty:
            print("⚠️ Nenhum dado encontrado na tabela.")
            return

        print(f"✅ {len(df)} registros carregados do banco.")

        # Configuração da API do Gemini
        api_key = get_gemini_api_key()
        
        if not api_key:
            print("⚠️ API key do Gemini não configurada!")
            print("Configure uma das opções:")
            print("1. Variável de ambiente: export GEMINI_API_KEY='sua_key'")
            print("2. Airflow Variable: airflow variables set gemini_api_key 'sua_key'")
            return

        print("🧠 Classificando imagens com Gemini...")
        df_classified = classify_images_with_gemini(None, df, api_key)

        # Mostra resultados
        print("\n📊 Resultados da classificação:")
        print(df_classified[["titulo", "property_type", "estimated_value", "gemini_confidence"]].head())

        print("✅ Pipeline finalizado com sucesso.")
        
        # Salva resultados
        df_classified.to_csv("classificacoes_gemini.csv", index=False)
        print("💾 Resultados salvos em 'classificacoes_gemini.csv'")

    run_pipeline()
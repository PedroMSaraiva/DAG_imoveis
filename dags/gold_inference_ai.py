import pandas as pd
import pendulum
import base64
import json
import os
from io import BytesIO
from PIL import Image
import google.generativeai as genai
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
from airflow.sdk import task, dag, Variable
import logging
from dotenv import load_dotenv

load_dotenv()   

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
            logging.info("API key do Gemini encontrada na variável de ambiente.")
            return api_key
        
        # Opção 2: Airflow Variable
        try:
            api_key = Variable.get("GEMINI_API_KEY")
            if api_key:
                logging.info("API key do Gemini encontrada na Airflow Variable.")
                return api_key
        except Exception:
            pass
        
        logging.error("API key do Gemini não configurada!")
        logging.error("Configure uma das opções:")
        logging.error("1. Variável de ambiente: export GEMINI_API_KEY='sua_key'")
        logging.error("2. Airflow Variable: airflow variables set gemini_api_key 'sua_key'")
        return None

    def get_data_postgres(query: str, conn_id: str = "tutorial_pg_conn") -> pd.DataFrame:
        """Busca dados no PostgreSQL e retorna como DataFrame."""
        postgres_hook = PostgresHook(postgres_conn_id=conn_id)
        engine = postgres_hook.get_sqlalchemy_engine()
        logging.info("Conexão PostgreSQL estabelecida com sucesso.")
        return pd.read_sql(query, engine)
    
    def post_data_postgress(data, table_name : str, conn_id: str = "tutorial_pg_conn"):
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


    def configure_gemini_api(api_key: str):
        """Configura a API do Gemini."""
        genai.configure(api_key=api_key)
        logging.info("API do Gemini configurada com sucesso.")
        return genai.GenerativeModel('gemini-2.5-flash')


    def encode_image_to_base64(img_bytes: bytes) -> str:
        """Converte bytes de imagem para base64."""
        try:
            image = Image.open(BytesIO(img_bytes)).convert("RGB")
                # Redimensiona a imagem se necessário para economizar tokens
            image.thumbnail((1024, 1024), Image.Resampling.LANCZOS)
            
            buffer = BytesIO()
            image.save(buffer, format='JPEG', quality=85)
            img_base64 = base64.b64encode(buffer.getvalue()).decode('utf-8')
            logging.info("Imagem convertida para base64 com sucesso.")
            return img_base64
        except Exception as e:
            logging.error(f"Erro ao processar imagem: {e}")
            return None


    def classify_images_with_gemini(model, df: pd.DataFrame, api_key: str) -> pd.DataFrame:
        """Classifica imagens usando a API do Gemini."""
        classifications = []
        
        for idx, row in df.iterrows():
            try:
                logging.info(f"Processando registro {idx + 1}/{len(df)}: {row.get('titulo', 'N/A')[:50]}...")
                
                # Processa todas as imagens da linha
                images_data = row["images"]
                logging.info(f"Tipo de dados de imagem: {type(images_data)}")
                
                # Se images_data é uma string JSON, converte para lista
                if isinstance(images_data, str):
                    try:
                        images_list = json.loads(images_data)
                        logging.info(f"Imagens JSON parseadas: {len(images_list)} imagens")
                    except json.JSONDecodeError:
                        images_list = [images_data]  # Assume que é uma única URL
                        logging.info("Tratando como URL única")
                elif isinstance(images_data, bytes):
                    images_list = [images_data]
                    logging.info("Imagem em bytes detectada")
                else:
                    images_list = images_data if isinstance(images_data, list) else [images_data]
                    logging.info(f"Lista de imagens: {len(images_list)} imagens")
                
                # Prepara as imagens para o Gemini
                gemini_images = []
                for i, img_data in enumerate(images_list):
                    # CORREÇÃO: Converter string hex para bytes
                    if isinstance(img_data, str):
                        img_bytes = convert_hex_string_to_bytes(img_data)
                        if img_bytes:
                            img_base64 = encode_image_to_base64(img_bytes)
                            if img_base64:
                                gemini_images.append({
                                    'mime_type': 'image/jpeg',
                                    'data': img_base64
                                })
                    elif isinstance(img_data, bytes):
                        img_base64 = encode_image_to_base64(img_data)
                        if img_base64:
                            gemini_images.append({
                                'mime_type': 'image/jpeg',
                                'data': img_base64
                            })
                    else:
                        logging.warning(f"Imagem {i+1} não é bytes ou string hex: {type(img_data)}")
                
                logging.info(f"Total de imagens preparadas para Gemini: {len(gemini_images)}")
                
                if not gemini_images:
                    logging.warning("Nenhuma imagem válida encontrada para este registro")
                    classifications.append({
                        "estrutura": "Sem imagem",
                        "esquadrias": "Sem imagem", 
                        "piso": "Sem imagem",
                        "forro": "Sem imagem",
                        "instalacao_eletrica": "Sem imagem",
                        "instalacao_sanitaria": "Sem imagem",
                        "revestimento_interno": "Sem imagem",
                        "acabamento_interno": "Sem imagem",
                        "revestimento_externo": "Sem imagem",
                        "acabamento_externo": "Sem imagem",
                        "cobertura": "Sem imagem",
                        "confidence": 0.0
                    })
                    continue
                
                # Prepara o prompt para classificação baseada nas diretrizes da Prefeitura
                prompt = f"""
                Analise estas imagens de imóveis e classifique cada característica conforme as diretrizes da Prefeitura.
                
                Título do anúncio: {row.get('titulo', 'N/A')}
                Descrição: {row.get('description', 'N/A')}
                HTML do anúncio: {row.get('scrapping', 'N/A')}
                
                IMPORTANTE: Retorne o NOME da classificação, não apenas o número de pontos!

                Classifique cada uma das seguintes características do imóvel:
                
                01 - Estrutura: Alvenaria(3), Concreto(5), Mista(5), Madeira Tratada(3), Metálica(5), Adobe/Taipa/Rudimentar(1)
                02 - Esquadrias: Ferro(2), Alumínio(4), Madeira(3), Rústica(1), Especial(5), Sem(0)
                03 - Piso: Cerâmica(4), Cimento(3), Taco(2), Tijolo(1), Terra(0), Especial/Porcelanato(5)
                04 - Forro: Laje(4), Madeira(3), Gesso Simples/PVC(2), Especial(5), Sem(0)
                05 - Instalação Elétrica: Embutida(5), Semi Embutida(3), Externa(1), Sem(0)
                06 - Instalação Sanitária: Interna(3), Completa(4), Mais de uma(5), Externa(2), Sem(0)
                07 - Revestimento Interno: Reboco(2), Massa(3), Material Cerâmico(4), Especial(5), Sem(0)
                08 - Acabamento Interno: Pintura Lavável(3), Pintura Simples(2), Caiação(1), Especial(5), Sem(0)
                09 - Revestimento Externo: Reboco(1), Massa(2), Material Cerâmico(2), Especial(4), Sem(0)
                10 - Acabamento Externo: Pintura Lavável(2), Pintura Simples(1), Caiação(1), Especial(5), Sem(0)
                11 - Cobertura: Telha de Barro(4), Fibrocimento(3), Alumínio(4), Zinco(4), Laje(4), Palha(1), Especial(5), Sem(0)
                
                Responda em formato JSON com os campos:
                - estrutura: nome da estrutura (ex: "Concreto", "Alvenaria")
                - esquadrias: nome das esquadrias (ex: "Alumínio", "Ferro")
                - piso: nome do piso (ex: "Cerâmica", "Cimento")
                - forro: nome do forro (ex: "Laje", "Madeira")
                - instalacao_eletrica: nome da instalação elétrica (ex: "Embutida", "Externa")
                - instalacao_sanitaria: nome da instalação sanitária (ex: "Completa", "Interna")
                - revestimento_interno: nome do revestimento interno (ex: "Massa", "Reboco")
                - acabamento_interno: nome do acabamento interno (ex: "Pintura Lavável", "Pintura Simples")
                - revestimento_externo: nome do revestimento externo (ex: "Massa", "Reboco")
                - acabamento_externo: nome do acabamento externo (ex: "Pintura Lavável", "Pintura Simples")
                - cobertura: nome da cobertura (ex: "Telha de Barro", "Laje")
                - confidence: nível de confiança geral (0.0 a 1.0)
                
                EXEMPLO de resposta esperada:
                {{
                  "estrutura": "Concreto",
                  "esquadrias": "Alumínio",
                  "piso": "Cerâmica",
                  "forro": "Laje",
                  "instalacao_eletrica": "Embutida",
                  "instalacao_sanitaria": "Completa",
                  "revestimento_interno": "Massa",
                  "acabamento_interno": "Pintura Lavável",
                  "revestimento_externo": "Massa",
                  "acabamento_externo": "Pintura Lavável",
                  "cobertura": "Telha de Barro",
                  "confidence": 0.85
                }}
                """
                
                # Configura o modelo Gemini
                gemini_model = configure_gemini_api(api_key)
                
                # Envia para o Gemini
                logging.info("Enviando requisição para o Gemini...")
                response = gemini_model.generate_content([prompt] + gemini_images)
                
                logging.info(f"Resposta recebida do Gemini: {response.text[:200]}...")
                
                # Tenta extrair JSON da resposta
                try:
                    response_text = response.text
                    # Remove markdown se presente
                    if response_text.startswith('```json'):
                        response_text = response_text.replace('```json', '').replace('```', '').strip()
                    
                    classification_result = json.loads(response_text)
                    logging.info(f"JSON parseado com sucesso: {classification_result}")
                    classifications.append(classification_result)
                    
                except json.JSONDecodeError as e:
                    logging.error(f"Erro ao fazer parse do JSON: {e}")
                    logging.error(f"Resposta completa: {response.text}")
                    # Se não conseguir fazer parse do JSON, usa valores padrão
                    classifications.append({
                        "estrutura": "Parse Error",
                        "esquadrias": "Parse Error", 
                        "piso": "Parse Error",
                        "forro": "Parse Error",
                        "instalacao_eletrica": "Parse Error",
                        "instalacao_sanitaria": "Parse Error",
                        "revestimento_interno": "Parse Error",
                        "acabamento_interno": "Parse Error",
                        "revestimento_externo": "Parse Error",
                        "acabamento_externo": "Parse Error",
                        "cobertura": "Parse Error",
                        "confidence": 0.5
                    })
                
            except Exception as e:
                logging.error(f"Erro ao processar linha: {e}")
                classifications.append({
                    "estrutura": "Erro",
                    "esquadrias": "Erro", 
                    "piso": "Erro",
                    "forro": "Erro",
                    "instalacao_eletrica": "Erro",
                    "instalacao_sanitaria": "Erro",
                    "revestimento_interno": "Erro",
                    "acabamento_interno": "Erro",
                    "revestimento_externo": "Erro",
                    "acabamento_externo": "Erro",
                    "cobertura": "Erro",
                    "confidence": 0.0
                })
        
        # Adiciona as classificações individuais ao DataFrame
        df["estrutura"] = [c.get("estrutura", "N/A") for c in classifications]
        df["esquadrias"] = [c.get("esquadrias", "N/A") for c in classifications]
        df["piso"] = [c.get("piso", "N/A") for c in classifications]
        df["forro"] = [c.get("forro", "N/A") for c in classifications]
        df["instalacao_eletrica"] = [c.get("instalacao_eletrica", "N/A") for c in classifications]
        df["instalacao_sanitaria"] = [c.get("instalacao_sanitaria", "N/A") for c in classifications]
        df["revestimento_interno"] = [c.get("revestimento_interno", "N/A") for c in classifications]
        df["acabamento_interno"] = [c.get("acabamento_interno", "N/A") for c in classifications]
        df["revestimento_externo"] = [c.get("revestimento_externo", "N/A") for c in classifications]
        df["acabamento_externo"] = [c.get("acabamento_externo", "N/A") for c in classifications]
        df["cobertura"] = [c.get("cobertura", "N/A") for c in classifications]
        df["gemini_confidence"] = [c.get("confidence", 0.0) for c in classifications]
        
        return df

    def calculate_score(row):
        """Calcula a pontuação total baseada nas diretrizes da Prefeitura."""
        
        # Dicionário de pontuações conforme diretrizes
        scores = {
            # 01 - Estrutura
            "estrutura": {
                "Alvenaria": 3, "Concreto": 5, "Mista": 5, "Madeira Tratada": 3, 
                "Metálica": 5, "Adobe": 1, "Taipa": 1, "Rudimentar": 1
            },
            # 02 - Esquadrias
            "esquadrias": {
                "Ferro": 2, "Alumínio": 4, "Madeira": 3, "Rústica": 1, 
                "Especial": 5, "Sem": 0
            },
            # 03 - Piso
            "piso": {
                "Cerâmica": 4, "Cimento": 3, "Taco": 2, "Tijolo": 1, 
                "Terra": 0, "Especial": 5, "Porcelanato": 5
            },
            # 04 - Forro
            "forro": {
                "Laje": 4, "Madeira": 3, "Gesso Simples": 2, "PVC": 2, 
                "Especial": 5, "Sem": 0
            },
            # 05 - Instalação Elétrica
            "instalacao_eletrica": {
                "Embutida": 5, "Semi Embutida": 3, "Externa": 1, "Sem": 0
            },
            # 06 - Instalação Sanitária
            "instalacao_sanitaria": {
                "Interna": 3, "Completa": 4, "Mais de uma": 5, "Externa": 2, "Sem": 0
            },
            # 07 - Revestimento Interno
            "revestimento_interno": {
                "Reboco": 2, "Massa": 3, "Material Cerâmico": 4, "Especial": 5, "Sem": 0
            },
            # 08 - Acabamento Interno
            "acabamento_interno": {
                "Pintura Lavável": 3, "Pintura Simples": 2, "Caiação": 1, "Especial": 5, "Sem": 0
            },
            # 09 - Revestimento Externo
            "revestimento_externo": {
                "Reboco": 1, "Massa": 2, "Material Cerâmico": 2, "Especial": 4, "Sem": 0
            },
            # 10 - Acabamento Externo
            "acabamento_externo": {
                "Pintura Lavável": 2, "Pintura Simples": 1, "Caiação": 1, "Especial": 5, "Sem": 0
            },
            # 11 - Cobertura
            "cobertura": {
                "Telha de Barro": 4, "Fibrocimento": 3, "Alumínio": 4, "Zinco": 4, 
                "Laje": 4, "Palha": 1, "Especial": 5, "Sem": 0
            }
        }
        
        total_score = 0
        score_details = {}
        
        for characteristic, score_dict in scores.items():
            value = str(row.get(characteristic, "")).strip()
            
            # Busca por correspondência exata primeiro
            points = score_dict.get(value, 0)
            
            # Se não encontrou correspondência exata, busca parcial (case insensitive)
            if points == 0:
                for key, point_value in score_dict.items():
                    if key.lower() in value.lower():
                        points = point_value
                        break
            
            # Se ainda não encontrou e é um número, tenta usar como pontuação direta
            if points == 0 and value.isdigit():
                points = int(value)
                logging.info(f"Usando pontuação direta para {characteristic}: {points}")
            
            # Se não encontrou correspondência, assume 0
            if points == 0 and value and value not in ["N/A", "Não identificado", "Erro", "Parse Error"]:
                logging.warning(f"Classificação não encontrada para {characteristic}: {value}")
            
            total_score += points
            score_details[characteristic] = points
        
        return total_score, score_details

    def convert_hex_string_to_bytes(hex_string):
        """Converte string hexadecimal para bytes."""
        try:
            # Remove o prefixo \x se existir e converte
            if isinstance(hex_string, str):
                # Método 1: Se está como \xffd8ffe0...
                if hex_string.startswith('\\x'):
                    hex_string = hex_string.replace('\\x', '')
                    return bytes.fromhex(hex_string)
                # Método 2: Se já está em formato correto
                else:
                    return hex_string.encode('latin1')
            return hex_string
        except Exception as e:
            logging.error(f"Erro ao converter hex para bytes: {e}")
            return None

    @task
    def run_pipeline():
        """Executa o pipeline completo:
        1. Busca dados do Postgres
        2. Configura API do Gemini
        3. Classifica imagens com Gemini
        4. Retorna DataFrame enriquecido
        """
        logging.info("🔹 Iniciando pipeline com Gemini...")

        # Busca dados do PostgreSQL
        df = get_data_postgres("""
            SELECT url, titulo, description, images, scrapping
            FROM anuncios_coletados;
        """)

        if df.empty:
            logging.error("⚠️ Nenhum dado encontrado na tabela.")
            return

        logging.info(f"✅ {len(df)} registros carregados do banco.")

        # Configuração da API do Gemini
        api_key = get_gemini_api_key()
        
        if not api_key:
            logging.error("⚠️ API key do Gemini não configurada!")
            logging.error("Configure uma das opções:")
            logging.error("1. Variável de ambiente: export GEMINI_API_KEY='sua_key'")
            logging.error("2. Airflow Variable: airflow variables set gemini_api_key 'sua_key'")
            return

        logging.info("🧠 Classificando imagens com Gemini...")
        # Limitar para debug - processar apenas os primeiros 3 registros
        df_sample = df.head(3).copy()
        #df_sample = df.copy() - Descomente para processar todos os registros
        logging.info(f"Processando apenas {len(df_sample)} registros para debug")
        df_classified = classify_images_with_gemini(None, df_sample, api_key)

        # Calcula pontuações
        logging.info("🧮 Calculando pontuações baseadas nas diretrizes da Prefeitura...")
        
        scores = []
        score_details_list = []
        
        for _, row in df_classified.iterrows():
            total_score, score_details = calculate_score(row)
            scores.append(total_score)
            score_details_list.append(score_details)
        
        # Adiciona as pontuações ao DataFrame
        df_classified["pontuacao_total"] = scores
        
        # Adiciona detalhes das pontuações
        for characteristic in ["estrutura", "esquadrias", "piso", "forro", "instalacao_eletrica", 
                              "instalacao_sanitaria", "revestimento_interno", "acabamento_interno", 
                              "revestimento_externo", "acabamento_externo", "cobertura"]:
            df_classified[f"pontos_{characteristic}"] = [sd[characteristic] for sd in score_details_list]
        
        logging.info(f"✅ Pontuações calculadas. Pontuação média: {sum(scores)/len(scores):.2f}")
        logging.info(f"📊 Pontuação mínima: {min(scores)}, máxima: {max(scores)}")
        
        df_with_scores = df_classified

        # Mostra resultados
        logging.info("\n📊 Resultados da classificação:")
        logging.info(df_with_scores[["titulo", "pontuacao_total", "estrutura", "piso", "cobertura"]].head())

        logging.info("✅ Pipeline finalizado com sucesso.")
        output_dir = "/opt/airflow/data"
        os.makedirs(output_dir, exist_ok=True)
        # Salva resultados na pasta data/ do host
        output_path = os.path.join(output_dir, "classificacoes_gemini_com_pontuacao.csv")
        df_with_scores.to_csv(output_path, index=False)
        logging.info(f"💾 Resultados salvos em '{output_path}'")

        post_data_postgress(df_with_scores, "anuncios_resultados")
        
        return df_with_scores

    run_pipeline()

dag = multimodal_ai()
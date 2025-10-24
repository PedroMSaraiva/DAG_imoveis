FROM apache/airflow:2.9.0-python3.12

# Troque para o usuário airflow antes da instalação
USER airflow

# Use o ambiente virtual já configurado no container
RUN pip install torch torchvision timm

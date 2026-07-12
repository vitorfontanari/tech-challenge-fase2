import boto3
import pandas as pd

s3 = boto3.client("s3")

# Bronze -> local
s3.download_file("vitor-tech-challenge-bronze", "alunos/alunos_streaming.parquet", "alunos_bronze.parquet")
s3.download_file("vitor-tech-challenge-bronze", "municipio/br_bd_diretorios_brasil_municipio.parquet", "municipio_ponte.parquet")

df_alunos = pd.read_parquet("alunos_bronze.parquet")
df_ponte = pd.read_parquet("municipio_ponte.parquet")

# Junta para trazer sigla_uf
df_alunos_silver = pd.merge(
    df_alunos,
    df_ponte[["id_municipio", "sigla_uf"]],
    on="id_municipio",
    how="left"
)

# Checa se algum município não encontrou correspondência
sem_correspondencia = df_alunos_silver["sigla_uf"].isna().sum()
print(f"Alunos sem sigla_uf correspondente: {sem_correspondencia}")

# Validação de duplicidade na chave correta
duplicatas = df_alunos_silver.duplicated(subset=["id_aluno", "ano"])
if duplicatas.any():
    raise ValueError(f"Encontradas {duplicatas.sum()} linhas duplicadas na chave id_aluno+ano")

print(f"OK: {df_alunos_silver.shape[0]} linhas, sem duplicidade na chave id_aluno+ano")

# Local -> Silver
df_alunos_silver.to_parquet("alunos_silver.parquet")
s3.upload_file("alunos_silver.parquet", "vitor-tech-challenge-silver", "alunos/alunos.parquet")
print("Silver de 'alunos' gravada com sucesso.")
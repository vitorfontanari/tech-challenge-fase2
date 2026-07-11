import boto3
import pandas as pd

s3 = boto3.client("s3")

# Baixa as duas fontes da Bronze
s3.download_file("vitor-tech-challenge-bronze", "municipio/br_inep_avaliacao_alfabetizacao_municipio.parquet", "municipio_bronze.parquet")
s3.download_file("vitor-tech-challenge-bronze", "municipio/br_bd_diretorios_brasil_municipio.parquet", "municipio_ponte.parquet")

df_municipio = pd.read_parquet("municipio_bronze.parquet")
df_ponte = pd.read_parquet("municipio_ponte.parquet")

print(df_municipio.columns.tolist())
print(df_ponte.columns.tolist())
print(df_municipio.head(5))
print(df_ponte.head(5))

print(f"Linhas em df_municipio: {df_municipio.shape[0]}")
print(f"Linhas em df_ponte: {df_ponte.shape[0]}")
print(f"IDs únicos em df_ponte: {df_ponte['id_municipio'].nunique()}")

# Junta as duas usando id_municipio como chave
df_municipio_silver = pd.merge(
    df_municipio,
    df_ponte[["id_municipio", "sigla_uf"]],
    on="id_municipio",
    how="left"
)

# Checa quantos municípios não encontraram correspondência (chave não bateu)
sem_correspondencia = df_municipio_silver["sigla_uf"].isna().sum()
print(f"Municípios sem sigla_uf correspondente: {sem_correspondencia}")

# Validação de duplicidade (mesmo padrão da uf)
duplicatas = df_municipio_silver.duplicated(subset=["ano", "id_municipio", "rede"])
if duplicatas.any():
    raise ValueError(f"Encontradas {duplicatas.sum()} linhas duplicadas na chave ano+id_municipio+rede")

print(f"OK: {df_municipio_silver.shape[0]} linhas, sem duplicidade na chave ano+id_municipio+rede")

df_municipio_silver.to_parquet("municipio_silver.parquet")
s3.upload_file("municipio_silver.parquet", "vitor-tech-challenge-silver", "municipio/municipio.parquet")
print("Silver de 'municipio' gravada com sucesso.")
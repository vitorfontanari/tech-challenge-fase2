import boto3
import pandas as pd

s3 = boto3.client("s3")

# Bronze -> local
s3.download_file("vitor-tech-challenge-bronze", "uf/br_inep_avaliacao_alfabetizacao_uf.parquet", "uf_bronze.parquet")
df_uf = pd.read_parquet("uf_bronze.parquet")

# Validação de consistência: chave composta não pode ter duplicata
duplicatas = df_uf.duplicated(subset=["ano", "sigla_uf", "rede"])
if duplicatas.any():
    raise ValueError(f"Encontradas {duplicatas.sum()} linhas duplicadas na chave ano+sigla_uf+rede")

print(f"OK: {df_uf.shape[0]} linhas validadas, sem duplicidade na chave composta.")

# Local -> Silver
df_uf.to_parquet("uf_silver.parquet")
s3.upload_file("uf_silver.parquet", "vitor-tech-challenge-silver", "uf/uf.parquet")

print("Silver de 'uf' gravada com sucesso.")
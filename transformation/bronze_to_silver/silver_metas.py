import boto3
import pandas as pd

s3 = boto3.client("s3")

def processar_meta_silver(nome_tabela, colunas_chave):
    arquivo_bronze = f"{nome_tabela}_bronze.parquet"
    s3.download_file(
        "vitor-tech-challenge-bronze",
        f"{nome_tabela}/br_inep_avaliacao_alfabetizacao_{nome_tabela}.parquet",
        arquivo_bronze
    )
    df = pd.read_parquet(arquivo_bronze)

    duplicatas = df.duplicated(subset=colunas_chave)
    if duplicatas.any():
        raise ValueError(f"{nome_tabela}: {duplicatas.sum()} linhas duplicadas na chave {colunas_chave}")

    print(f"OK: {nome_tabela} - {df.shape[0]} linhas, sem duplicidade na chave {colunas_chave}")

    arquivo_silver = f"{nome_tabela}_silver.parquet"
    df.to_parquet(arquivo_silver)
    s3.upload_file(arquivo_silver, "vitor-tech-challenge-silver", f"{nome_tabela}/{nome_tabela}.parquet")

processar_meta_silver("meta_alfabetizacao_brasil", ["ano", "rede"])
processar_meta_silver("meta_alfabetizacao_uf", ["ano", "sigla_uf", "rede"])
processar_meta_silver("meta_alfabetizacao_municipio", ["ano", "id_municipio", "rede"])
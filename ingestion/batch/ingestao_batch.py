import basedosdados as bd
import boto3 # biblioteca que "conversa" com S3

s3 = boto3.client("s3")

def ingerir_tabela(nome_dataset, nome_tabela):
    query = f"select * from `basedosdados.{nome_dataset}.{nome_tabela}`"
    df = bd.read_sql(query, billing_project_id="1012043720930")

    arquivo_local = f"{nome_dataset}_{nome_tabela}.parquet"
    df.to_parquet(arquivo_local)

    caminho_s3 = f"{nome_tabela}/{nome_dataset}_{nome_tabela}.parquet"
    s3.upload_file(arquivo_local, "vitor-tech-challenge-bronze", caminho_s3)

    print(f"OK: {nome_dataset}.{nome_tabela}")

tabelas = ["uf", "municipio", "meta_alfabetizacao_brasil", "meta_alfabetizacao_uf", "meta_alfabetizacao_municipio"]

for nome_tabela in tabelas:
    ingerir_tabela("br_inep_avaliacao_alfabetizacao", nome_tabela)

ingerir_tabela("br_bd_diretorios_brasil", "municipio")

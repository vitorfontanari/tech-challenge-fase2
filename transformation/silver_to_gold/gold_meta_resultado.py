import boto3
import pandas as pd

s3 = boto3.client("s3")

s3.download_file("vitor-tech-challenge-silver", "municipio/municipio.parquet", "municipio_silver.parquet")
s3.download_file("vitor-tech-challenge-silver", "meta_alfabetizacao_municipio/meta_alfabetizacao_municipio.parquet", "meta_alfabetizacao_municipio.parquet")

df_municipio = pd.read_parquet("municipio_silver.parquet")
df_meta_alfabetizacao = pd.read_parquet("meta_alfabetizacao_municipio.parquet")

mapa_rede = {"Municipal": "3", "Pública": "6"}
df_meta_alfabetizacao["rede"] = df_meta_alfabetizacao["rede"].map(mapa_rede)

nao_mapeados = df_meta_alfabetizacao["rede"].isna().sum()
print(f"Valores de rede não mapeados: {nao_mapeados}")

df_meta_resultado = pd.merge(
    df_meta_alfabetizacao,
    df_municipio[["id_municipio", "ano", "rede", "sigla_uf"]],
    on=["id_municipio", "ano", "rede"],
    how="left"
)

def pegar_meta_do_ano(linha):
    if linha["ano"] < 2024:
        return None
    coluna_meta = f"meta_alfabetizacao_{linha['ano']}"
    return linha[coluna_meta]

df_meta_resultado["meta_do_ano"] = df_meta_resultado.apply(pegar_meta_do_ano, axis=1)

df_meta_resultado["bateu_meta"] = df_meta_resultado["taxa_alfabetizacao"] >= df_meta_resultado["meta_do_ano"]
df_meta_resultado.loc[df_meta_resultado["meta_do_ano"].isna(), "bateu_meta"] = None

print(df_meta_resultado[["ano", "taxa_alfabetizacao", "meta_do_ano", "bateu_meta"]].head(15))

print(df_meta_resultado.loc[1, ["ano", "meta_alfabetizacao_2024", "meta_alfabetizacao_2025", "meta_alfabetizacao_2026"]])

df_gold_meta_resultado = df_meta_resultado[[
    "id_municipio", "sigla_uf", "ano", "taxa_alfabetizacao", "meta_do_ano", "bateu_meta"
]].rename(columns={"taxa_alfabetizacao": "indicador_alfabetizacao"})

df_gold_meta_resultado.to_parquet("gold_meta_resultado.parquet")
s3 = boto3.client("s3")
s3.upload_file("gold_meta_resultado.parquet", "vitor-tech-challenge-gold", "meta_resultado/meta_resultado.parquet")
print("Gold 'meta_resultado' gravada com sucesso.")
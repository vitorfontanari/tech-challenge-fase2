import boto3
import pandas as pd
import os

s3 = boto3.client("s3")

# --- BRONZE/SILVER -> LOCAL: baixa as fontes já tratadas na Silver ---
s3.download_file("vitor-tech-challenge-silver", "municipio/municipio.parquet", "municipio_silver.parquet")
s3.download_file("vitor-tech-challenge-silver", "meta_alfabetizacao_municipio/meta_alfabetizacao_municipio.parquet", "meta_alfabetizacao_municipio.parquet")

# --- Função auxiliar: sobe uma pasta particionada inteira para o S3, ---
# --- preservando a estrutura de subpastas (ano=2023/, ano=2024/, etc.) ---
def upload_pasta_particionada(pasta_local, bucket, prefixo_s3):
    for raiz, _, arquivos in os.walk(pasta_local):          # percorre a pasta e subpastas
        for arquivo in arquivos:                              # para cada arquivo encontrado
            caminho_local = os.path.join(raiz, arquivo)        # caminho completo no disco
            caminho_relativo = os.path.relpath(caminho_local, pasta_local)  # só a parte "ano=2024/arquivo.parquet"
            caminho_s3 = f"{prefixo_s3}/{caminho_relativo}"    # monta o destino no bucket
            s3.upload_file(caminho_local, bucket, caminho_s3)
    print(f"OK: pasta '{pasta_local}' enviada para s3://{bucket}/{prefixo_s3}/")

# --- Carrega os Parquets baixados em DataFrames ---
df_municipio = pd.read_parquet("municipio_silver.parquet")
df_meta_alfabetizacao = pd.read_parquet("meta_alfabetizacao_municipio.parquet")

# --- Padroniza a coluna 'rede': tabelas de Meta usam texto ('Municipal', 'Pública'), ---
# --- as demais usam código numérico decodificado via tabela 'dicionario' ---
mapa_rede = {"Municipal": "3", "Pública": "6"}
df_meta_alfabetizacao["rede"] = df_meta_alfabetizacao["rede"].map(mapa_rede)

nao_mapeados = df_meta_alfabetizacao["rede"].isna().sum()
print(f"Valores de rede não mapeados: {nao_mapeados}")

# --- JOIN: traz sigla_uf para a tabela de metas, casando pela chave composta ---
# --- (ano+id_municipio+rede) para evitar explosão de linhas ou casamento errado ---
df_meta_resultado = pd.merge(
    df_meta_alfabetizacao,
    df_municipio[["id_municipio", "ano", "rede", "sigla_uf"]],
    on=["id_municipio", "ano", "rede"],
    how="left"
)

# --- Para cada linha, decide qual coluna de meta usar baseado no ano; ---
# --- 2023 não tem meta definida na fonte, então retorna None ---
def pegar_meta_do_ano(linha):
    if linha["ano"] < 2024:
        return None
    coluna_meta = f"meta_alfabetizacao_{linha['ano']}"
    return linha[coluna_meta]

df_meta_resultado["meta_do_ano"] = df_meta_resultado.apply(pegar_meta_do_ano, axis=1)

# --- Calcula se bateu a meta; força None onde não havia meta (em vez de False) ---
df_meta_resultado["bateu_meta"] = df_meta_resultado["taxa_alfabetizacao"] >= df_meta_resultado["meta_do_ano"]
df_meta_resultado.loc[df_meta_resultado["meta_do_ano"].isna(), "bateu_meta"] = None

print(df_meta_resultado[["ano", "taxa_alfabetizacao", "meta_do_ano", "bateu_meta"]].head(15))
print(df_meta_resultado.loc[1, ["ano", "meta_alfabetizacao_2024", "meta_alfabetizacao_2025", "meta_alfabetizacao_2026"]])

# --- Tabela Gold 1: comparação entre meta e resultado, por município ---
df_gold_meta_resultado = df_meta_resultado[[
    "id_municipio", "sigla_uf", "ano", "taxa_alfabetizacao", "meta_do_ano", "bateu_meta"
]].rename(columns={"taxa_alfabetizacao": "indicador_alfabetizacao"})

# --- Tabela Gold 2: indicador de alfabetização por município (sem a parte de meta) ---
df_indicador_municipio = df_meta_resultado[["id_municipio", "sigla_uf", "ano", "taxa_alfabetizacao"]].rename(columns={"taxa_alfabetizacao": "indicador_alfabetizacao"})

# --- Tabela Gold 3: evolução temporal do indicador, agregado por UF e ano (média dos municípios) ---
df_evolucao_uf = df_meta_resultado.groupby(["sigla_uf", "ano"])["taxa_alfabetizacao"].mean().reset_index().rename(columns={"taxa_alfabetizacao": "indicador_alfabetizacao"})

# --- Grava as 3 tabelas Gold particionadas por ano, e sobe cada uma para o S3 ---
df_gold_meta_resultado.to_parquet("gold_meta_resultado", partition_cols=["ano"])
upload_pasta_particionada("gold_meta_resultado", "vitor-tech-challenge-gold", "meta_resultado")

df_indicador_municipio.to_parquet("gold_indicador_municipio", partition_cols=["ano"])
upload_pasta_particionada("gold_indicador_municipio", "vitor-tech-challenge-gold", "indicador_municipio")

df_evolucao_uf.to_parquet("gold_evolucao_uf", partition_cols=["ano"])
upload_pasta_particionada("gold_evolucao_uf", "vitor-tech-challenge-gold", "evolucao_uf")
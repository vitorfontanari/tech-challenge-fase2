import boto3
import json
import time
import pandas as pd

kinesis = boto3.client("kinesis")
s3 = boto3.client("s3")


stream_name = "stream-dados-alunos"

response = kinesis.describe_stream(StreamName=stream_name)
shards = response["StreamDescription"]["Shards"]


registros_coletados = []

for shard in shards:
    shard_id = shard["ShardId"]
    print(f"Lendo shard: {shard_id}")

    shard_iterator = kinesis.get_shard_iterator(
        StreamName=stream_name,
        ShardId=shard_id,
        ShardIteratorType="TRIM_HORIZON"
    )["ShardIterator"]

    tentativas_vazias = 0

    while tentativas_vazias < 3:
        resultado = kinesis.get_records(ShardIterator=shard_iterator, Limit=1000)
        records = resultado["Records"]

        if records:
            for r in records:
                registro = json.loads(r["Data"])
                registros_coletados.append(registro)
            tentativas_vazias = 0
            print(f"  Lidos {len(records)} registros. Total geral: {len(registros_coletados)}")
        else:
            tentativas_vazias += 1

        shard_iterator = resultado["NextShardIterator"]
        time.sleep(1)

df_alunos = pd.DataFrame(registros_coletados)
df_alunos.to_parquet("alunos_streaming.parquet")

print(df_alunos.shape)

s3.upload_file("alunos_streaming.parquet", "vitor-tech-challenge-bronze", "alunos/alunos_streaming.parquet")



print(f"Total final coletado: {len(registros_coletados)}")
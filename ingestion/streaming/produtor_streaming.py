import basedosdados as bd
import boto3
import json

kinesis = boto3.client("kinesis")

query = "SELECT * FROM `basedosdados.br_inep_avaliacao_alfabetizacao.alunos` LIMIT 3000"
df = bd.read_sql(query, billing_project_id="1012043720930")

for indice, linha in df.iterrows():
    registro = linha.to_dict()
    kinesis.put_record(
        StreamName="stream-dados-alunos",
        Data=json.dumps(registro),
        PartitionKey="alunos"
    )
    if indice % 500 == 0:
        print(f"Progresso: {indice} registros enviados")
# TECH CHALLENGE FASE 2

## 1. Contexto do Problema
- O que é o Compromisso Nacional Criança Alfabetizada e o Indicador Criança Alfabetizada
- Por que esse dado importa (política pública, ponto de corte 743 pontos)

## 2. Arquitetura da Solução
- Diagrama da pipeline (pode reaproveitar/adaptar o que fizemos juntos)
- Descrição de cada camada (Bronze/Silver/Gold) e o que acontece em cada uma
- Fluxo de dados: fonte → ingestão → data lake → transformação → consumo

## 3. Decisões Arquiteturais (trade-offs)
- Por que batch para uf, município e metas e streaming para Dados de alunos?
- Por que Lambda para ingestão leve e Glue para transformação pesada
- Achados reais da exploração (chave município↔UF, sobreposição de `rede`) e como isso influenciou o design da Silver

## 4. Tecnologias Utilizadas
- Lista de serviços AWS + justificativa de cada escolha

## 5. Qualidade de Dados
- Quais validações foram implementadas e por quê
- # Identificado que as tabelas do dataset temático não continham uma chave direta entre município e UF; resolvi isso incorporando a tabela de referência br_bd_diretorios_brasil.municipio, que atua como tabela-ponte entre as duas granularidades.

## 6. Monitoramento e FinOps
- O que é monitorado
- Decisões de particionamento/Parquet e estimativa de custo

## 7. Aplicação em IA
- Como a camada Gold poderia alimentar modelos preditivos de alfabetização

## 8. Como rodar / estrutura do repositório

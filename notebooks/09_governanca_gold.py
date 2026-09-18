# Databricks notebook source
display(spark.sql("""
    SELECT
      COUNT(*)                                                      AS recuperou_algum_minuto,
      SUM(CASE WHEN atraso_chegada_min <= 0 THEN 1 ELSE 0 END)      AS chegou_adiantado_ou_no_horario,
      SUM(CASE WHEN atraso_chegada_min >  0 THEN 1 ELSE 0 END)      AS chegou_atrasado_mesmo_assim,
      SUM(CASE WHEN atraso_chegada_min > 15 THEN 1 ELSE 0 END)      AS chegou_atrasado_mais_de_15
    FROM voebem.gold.obt_voos
    WHERE minutos_recuperados > 0
"""))

# COMMAND ----------

display(spark.sql("""
    SELECT situacao_voo,
           COUNT(*)                                                AS voos,
           SUM(CASE WHEN partida_pontual IS NULL THEN 1 ELSE 0 END) AS partida_pontual_null
    FROM voebem.gold.obt_voos GROUP BY situacao_voo
"""))

# COMMAND ----------

COMENTARIOS_OBT = {
    # ---- companhia ----
    "icao_empresa":           "Codigo ICAO de tres letras da companhia que operou a etapa. Use nome_companhia para exibir; este codigo serve para filtro exato.",
    "nome_companhia":         "Razao social da companhia aerea. Quando o codigo nao existe no cadastro da ANAC, traz COMPANHIA NAO CADASTRADA seguida do codigo, em vez de vazio.",
    "numero_voo":             "Numero comercial do voo divulgado pela companhia. Nao e identificador unico: o mesmo numero se repete todos os dias.",

    # ---- operacao ----
    "codigo_di":              "Codigo de autorizacao da etapa (DI) publicado pela ANAC. O codigo 1 aparece no dado e nao consta na tabela oficial de descricoes.",
    "descricao_di":           "Tipo da etapa por extenso: regular, extra, de retorno, charter, fretamento. Quando o codigo nao esta catalogado pela ANAC, diz isso explicitamente.",
    "codigo_tipo_linha":      "Codigo do tipo de linha da ANAC: N e C domesticas, I e G internacionais.",
    "descricao_tipo_linha":   "Tipo de linha por extenso, combinando escopo e natureza da operacao: Domestica Mista, Internacional Cargueira, etc.",
    "escopo_voo":             "Classificacao de negocio do voo em Domestico ou Internacional, derivada do tipo de linha. E a coluna certa para comparar os dois universos.",

    # ---- origem ----
    "icao_origem":            "Codigo ICAO do aeroporto de partida. Use nome_aeroporto_origem para exibir.",
    "nome_aeroporto_origem":  "Nome do aeroporto de partida. Aeroporto estrangeiro nao consta no cadastro da ANAC e aparece como AEROPORTO FORA DO CADASTRO ANAC seguido do codigo.",
    "municipio_origem":       "Municipio do aeroporto de partida. Vazio para aeroporto estrangeiro, que nao esta no cadastro brasileiro.",
    "uf_origem":              "Unidade federativa do aeroporto de partida, escrita POR EXTENSO (Sao Paulo, Ceara), como a ANAC publica. Nao e a sigla.",
    "pais_origem":            "Brasil ou Exterior, deduzido do prefixo do codigo ICAO. Serve para separar operacao domestica de internacional pelo lado do aeroporto.",

    # ---- destino ----
    "icao_destino":           "Codigo ICAO do aeroporto de chegada. Use nome_aeroporto_destino para exibir.",
    "nome_aeroporto_destino": "Nome do aeroporto de chegada. Mesma regra de fallback do aeroporto de origem.",
    "municipio_destino":      "Municipio do aeroporto de chegada. Vazio para aeroporto estrangeiro.",
    "uf_destino":             "Unidade federativa do aeroporto de chegada, por extenso.",
    "pais_destino":           "Brasil ou Exterior para o aeroporto de chegada.",

    # ---- rota ----
    "rota_icao":              "Rota no formato ORIGEM - DESTINO usando codigos ICAO. E a chave estavel para agrupar por rota.",
    "rota_municipios":        "Rota no formato municipio de origem - municipio de destino, para leitura humana. Aeroporto estrangeiro aparece pelo codigo ICAO, porque nao tem municipio no cadastro.",

    # ---- tempo ----
    "partida_prevista":       "Data e hora que a companhia programou para a partida, na hora local do aeroporto de origem.",
    "partida_prevista_data":  "Data programada da partida. Use para series diarias e para recortar periodo.",
    "partida_prevista_hora":  "Hora e minuto programados da partida, no formato HH:mm, para leitura.",
    "hora_partida_prevista":  "Hora cheia programada da partida, de 0 a 23. E a coluna certa para analisar o efeito cascata do atraso ao longo do dia.",
    "dia_semana":             "Dia da semana da partida programada, por extenso e em minusculas (domingo a sabado).",
    "mes_referencia":         "Primeiro dia do mes da partida programada, para agregacao mensal. E nulo nos voos que nao tem horario previsto informado.",
    "partida_real":           "Data e hora em que a aeronave efetivamente partiu. Nulo em voo cancelado.",
    "chegada_prevista":       "Data e hora programadas para a chegada, na hora local do aeroporto de destino.",
    "chegada_real":           "Data e hora em que a aeronave efetivamente pousou. Nulo em voo cancelado.",

    # ---- metricas ----
    "atraso_partida_min":     "Atraso de partida em minutos: horario real menos programado. Negativo significa que saiu adiantado. Nulo quando o voo foi cancelado, quando nao ha horario programado, ou quando o valor esta fora da faixa plausivel.",
    "atraso_chegada_min":     "Atraso de chegada em minutos: horario real menos programado. Negativo significa que pousou adiantado. Mesmas regras de nulo do atraso de partida.",
    "minutos_recuperados":    "Minutos que a etapa recuperou no ar: atraso de partida menos atraso de chegada. Positivo significa que chegou MENOS ATRASADA do que saiu, e NAO que chegou no horario - um voo pode recuperar 20 minutos e ainda assim pousar atrasado. Negativo significa que perdeu ainda mais tempo depois de decolar.",
    "atraso_fora_de_faixa":   "Verdadeiro quando o atraso calculado estava fora da faixa plausivel (menos de -2h ou mais de 24h), sinal de erro de data na origem. A linha continua contando como voo, mas as tres metricas de atraso foram anuladas.",
    "partida_pontual":        "Verdadeiro quando a partida atrasou 15 minutos ou menos, criterio de pontualidade deste projeto. Falso significa atraso maior que 15 minutos. Nulo significa que NAO DA para avaliar - voo cancelado ou sem horario programado - e nunca deve ser contado como atraso.",
    "chegada_pontual":        "Verdadeiro quando a chegada atrasou 15 minutos ou menos. Mesma regra de nulo da pontualidade de partida.",

    # ---- situacao ----
    "situacao_voo":           "Situacao informada pela companhia: REALIZADO quando a etapa aconteceu, CANCELADO quando nao aconteceu.",
    "voo_realizado":          "Verdadeiro quando a etapa foi realizada. Use como denominador de metricas operacionais.",
    "voo_cancelado":          "Verdadeiro quando a etapa foi cancelada. Voo cancelado NAO entra em nenhuma media de atraso, porque nao tem horario real; use esta coluna para taxa de cancelamento.",

    "_processado_em":         "Auditoria: momento em que esta linha foi construida na camada gold.",
}

for coluna, comentario in COMENTARIOS_OBT.items():
    spark.sql(f"ALTER TABLE voebem.gold.obt_voos ALTER COLUMN {coluna} COMMENT '{comentario}'")

print(f"{len(COMENTARIOS_OBT)} colunas comentadas em gold.obt_voos")

# COMMAND ----------

COMENTARIOS_DIM = {
    "icao_aeroporto":      "Codigo ICAO do aeroporto. Chave da dimensao, serve tanto para origem quanto para destino do fato.",
    "nome_aeroporto":      "Nome do aeroporto. Traz fallback textual com o codigo quando o aeroporto nao esta no cadastro da ANAC.",
    "municipio_aeroporto": "Municipio onde o aeroporto esta localizado. Vazio para aeroporto estrangeiro.",
    "uf_aeroporto":        "Unidade federativa por extenso, como a ANAC publica. Nao e a sigla.",
    "pais_aeroporto":      "Brasil ou Exterior, deduzido do prefixo ICAO. Regra de negocio criada na gold.",
    "no_cadastro_anac":    "Verdadeiro quando o aeroporto existe no cadastro de aerodromos publicos da ANAC. Falso e o esperado para aeroporto estrangeiro, e nao indica erro.",
    "_processado_em":      "Auditoria: momento da construcao da dimensao.",
}

COMENTARIOS_FATO = dict(COMENTARIOS_OBT)
COMENTARIOS_FATO["icao_origem"]  = "Codigo ICAO do aeroporto de partida. Chave para gold.dim_aeroporto."
COMENTARIOS_FATO["icao_destino"] = "Codigo ICAO do aeroporto de chegada. Chave para gold.dim_aeroporto."
COMENTARIOS_FATO["rota"] = "Rota no formato ORIGEM - DESTINO usando codigos ICAO."
COMENTARIOS_FATO["cadastro_companhia"] = "De qual cadastro da ANAC veio a companhia: nacional ou estrangeira. Nulo quando o codigo nao tem cadastro."
COLUNAS_FATO = [c for c in COMENTARIOS_FATO if c not in (
    "nome_aeroporto_origem", "municipio_origem", "uf_origem", "pais_origem",
    "nome_aeroporto_destino", "municipio_destino", "uf_destino", "pais_destino",
    "rota_icao", "rota_municipios")]

for coluna in COLUNAS_FATO:
    spark.sql(f"ALTER TABLE voebem.gold.fato_voos ALTER COLUMN {coluna} COMMENT '{COMENTARIOS_FATO[coluna]}'")
print(f"{len(COLUNAS_FATO)} colunas comentadas em gold.fato_voos")

for coluna, comentario in COMENTARIOS_DIM.items():
    spark.sql(f"ALTER TABLE voebem.gold.dim_aeroporto ALTER COLUMN {coluna} COMMENT '{comentario}'")
print(f"{len(COMENTARIOS_DIM)} colunas comentadas em gold.dim_aeroporto")

# COMMAND ----------

TABELAS_GOLD = {
    "voebem.gold.obt_voos": (
        "Gold - One Big Table de voos da ANAC, desnormalizada e desenhada para consumo por agente de IA. "
        "Uma linha por etapa de voo, com nomes ja resolvidos e metricas prontas: responde as perguntas de "
        "negocio do projeto sem nenhum JOIN. Criterio de pontualidade: 15 minutos. "
        "Voo cancelado nao tem metrica de atraso.",
        {"camada": "gold", "dominio": "aviacao", "consumo": "genie", "grao": "etapa_de_voo", "padrao": "obt"},
    ),
    "voebem.gold.fato_voos": (
        "Gold - fato de voos no grao de uma linha por etapa, com companhia e codigos de operacao como "
        "dimensoes degeneradas. E aqui que nascem as regras de negocio: pontualidade a 15 minutos, "
        "escopo domestico/internacional e as decisoes sobre a quarentena. "
        "Contagem = silver.vra menos 41 duplicatas exatas.",
        {"camada": "gold", "dominio": "aviacao", "consumo": "bi", "grao": "etapa_de_voo", "padrao": "fato"},
    ),
    "voebem.gold.dim_aeroporto": (
        "Gold - dimensao de aeroporto, servindo origem e destino do fato. Construida a partir dos codigos "
        "presentes no fato e enriquecida pelo cadastro da ANAC, para cobrir 100 por cento do fato inclusive "
        "os aeroportos estrangeiros, que a ANAC nao cadastra.",
        {"camada": "gold", "dominio": "aviacao", "consumo": "bi", "grao": "aeroporto", "padrao": "dimensao"},
    ),
}

for tabela, (comentario, tags) in TABELAS_GOLD.items():
    spark.sql(f"COMMENT ON TABLE {tabela} IS '{comentario}'")
    pares = ", ".join(f"'{k}' = '{v}'" for k, v in tags.items())
    spark.sql(f"ALTER TABLE {tabela} SET TAGS ({pares})")
    print(f"{tabela}: comentario + {len(tags)} tags")

# COMMAND ----------

display(spark.sql("""
    SELECT table_schema, table_name,
           COUNT(*)                                                         AS colunas,
           SUM(CASE WHEN comment IS NULL OR comment = '' THEN 1 ELSE 0 END) AS sem_comentario
    FROM voebem.information_schema.columns
    WHERE table_schema IN ('silver', 'gold')
    GROUP BY table_schema, table_name
    ORDER BY table_schema, table_name
"""))

# COMMAND ----------

display(spark.sql("""
    SELECT table_name, tag_name, tag_value
    FROM voebem.information_schema.table_tags
    WHERE schema_name = 'gold'
    ORDER BY table_name, tag_name
"""))

# COMMAND ----------

display(spark.sql("""
    SELECT
      COALESCE(nullif(source_table_full_name, ''), '(arquivo no volume)') AS origem,
      target_table_full_name                                             AS destino
    FROM system.access.table_lineage
    WHERE target_table_full_name LIKE 'voebem.%'
      AND event_date >= current_date() - 7
    GROUP BY 1, 2
    ORDER BY destino, origem
"""))
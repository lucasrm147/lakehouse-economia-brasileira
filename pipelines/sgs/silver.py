# =============================================================================
# pipelines/sgs/silver.py: camada Silver, agora declarativa
# Substitui o notebook 03_silver: tipagem + regras de qualidade + dedup/MERGE.
# =============================================================================

from pyspark import pipelines as dp
from pyspark.sql import functions as F

SILVER = "workspace.silver.sgs_series"


# -----------------------------------------------------------------------------
# PASSO 1: view temporária com a tipagem e as regras de qualidade
# temporary_view = etapa intermediária que existe só dentro do pipeline
# (não vira tabela no catálogo). Serve de "esteira" entre Bronze e Silver.
#
# Expectations = regras de qualidade declaradas junto com os dados:
#   expect_or_drop(nome, condição) -> descarta a linha que não cumpre a regra
#   expect(nome, condição)         -> mantém a linha, só conta a violação
# O pipeline mostra na tela quantas linhas cada regra pegou (aba Data quality).
# É a versão declarativa da quarentena do Módulo 3.
# -----------------------------------------------------------------------------
@dp.temporary_view(name="sgs_series_tipada")
@dp.expect_or_drop("serie_valida", "serie_codigo IS NOT NULL")
@dp.expect_or_drop("data_valida", "data_referencia IS NOT NULL")
@dp.expect_or_drop("valor_valido", "valor IS NOT NULL")
@dp.expect("valor_positivo", "valor > 0")  # IPCA pode ser negativo (deflação): só alerta
def sgs_series_tipada():
    # readStream.table: lê a Bronze de forma incremental (só o que é novo).
    # A tipagem é a mesma do 03_silver: try_cast vira NULL se falhar,
    # e aí as expectations acima descartam a linha.
    return spark.readStream.table("workspace.bronze.sgs_series").select(
        F.expr("try_cast(serie_codigo AS INT)").alias("serie_codigo"),
        "serie_nome",
        F.expr("CAST(try_to_timestamp(data, 'dd/MM/yyyy') AS DATE)").alias("data_referencia"),
        F.expr("try_cast(valor AS DECIMAL(18,6))").alias("valor"),
        F.expr("try_cast(extraido_em AS TIMESTAMP)").alias("extraido_em"),
        "_arquivo_origem",
    )


# -----------------------------------------------------------------------------
# PASSO 2: a tabela Silver de destino
# create_streaming_table só declara que a tabela existe. Quem a preenche
# é o fluxo de Auto CDC logo abaixo.
# -----------------------------------------------------------------------------
dp.create_streaming_table(
    name=SILVER,
    comment="Séries do SGS/BCB tipadas e deduplicadas: 1 linha por série e data",
)

# -----------------------------------------------------------------------------
# PASSO 3: Auto CDC (Change Data Capture)
# Faz o papel do window + row_number + MERGE do 03_silver, em poucas linhas:
#   keys        -> a chave que identifica um registro (série + data)
#   sequence_by -> a coluna que diz qual versão é a mais nova
#   SCD tipo 1  -> guarda só a versão mais recente (sobrescreve a antiga).
#                  O tipo 2 guardaria o histórico de todas as versões.
# -----------------------------------------------------------------------------
dp.create_auto_cdc_flow(
    target=SILVER,                      # para onde vão os dados
    source="sgs_series_tipada",         # de onde vêm (a view do passo 1)
    keys=["serie_codigo", "data_referencia"],
    sequence_by=F.col("extraido_em"),
    stored_as_scd_type=1,
)
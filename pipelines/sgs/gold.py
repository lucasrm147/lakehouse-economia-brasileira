# =============================================================================
# pipelines/sgs/gold.py: camada Gold, agora declarativa
# Mesma lógica do notebook 04_gold (veja os comentários detalhados lá).
# Diferença: em vez de write.saveAsTable, cada função DEVOLVE o DataFrame,
# e o pipeline grava e atualiza a tabela sozinho.
# =============================================================================

from pyspark import pipelines as dp
from pyspark.sql import Window
from pyspark.sql import functions as F

SILVER = "workspace.silver.sgs_series"


def inicio_do_mes(coluna):
    return F.date_trunc("MONTH", coluna).cast("date")


# materialized_view = tabela que guarda o RESULTADO de uma consulta e é
# recalculada quando a origem muda. Ideal para agregações como as da Gold.
# Note o spark.read (batch) e não readStream: a Gold olha a Silver inteira.
@dp.materialized_view(
    name="workspace.gold.dolar_mensal",
    comment="Dólar comercial (venda): resumo mensal",
)
def dolar_mensal():
    return (
        spark.read.table(SILVER)
        .filter(F.col("serie_codigo") == 1)
        .groupBy(inicio_do_mes(F.col("data_referencia")).alias("mes"))
        .agg(
            F.round(F.avg("valor"), 4).alias("cotacao_media"),
            F.min("valor").alias("cotacao_minima"),
            F.max("valor").alias("cotacao_maxima"),
            F.max_by("valor", "data_referencia").alias("cotacao_fechamento"),  # último dia do mês
            F.count("*").alias("dias_com_cotacao"),
        )
        .withColumn(
            "variacao_mensal_pct",
            F.round(
                (F.col("cotacao_fechamento") / F.lag("cotacao_fechamento").over(Window.orderBy("mes")) - 1) * 100,
                2,
            ),
        )
        # mês corrente = parcial
        .withColumn("mes_completo", F.col("mes") < inicio_do_mes(F.current_date()))
    )


@dp.materialized_view(
    name="workspace.gold.juro_real_mensal",
    comment="Selic meta x IPCA 12 meses e juro real ex-post",
)
def juro_real_mensal():
    silver = spark.read.table(SILVER)

    # Selic do último dia de cada mês
    selic = (
        silver.filter(F.col("serie_codigo") == 432)
        .groupBy(inicio_do_mes(F.col("data_referencia")).alias("mes"))
        .agg(F.max_by("valor", "data_referencia").alias("selic_meta_aa"))
    )

    # IPCA acumulado 12 meses, composto: exp(soma(log(1 + taxa))) - 1
    janela_12m = Window.orderBy("mes").rowsBetween(-11, 0)
    ipca = (
        silver.filter(F.col("serie_codigo") == 433)
        .select(inicio_do_mes(F.col("data_referencia")).alias("mes"), F.col("valor").alias("ipca_mes"))
        .withColumn("ipca_12m", (F.exp(F.sum(F.log1p(F.col("ipca_mes") / 100)).over(janela_12m)) - 1) * 100)
        .withColumn("meses_na_janela", F.count("*").over(janela_12m))
    )

    # Junta pelo mês e aplica Fisher: (1 + Selic) / (1 + IPCA) - 1
    return (
        selic.join(ipca, "mes")
        .filter(F.col("meses_na_janela") == 12)
        .select(
            "mes",
            "selic_meta_aa",
            "ipca_mes",
            F.round("ipca_12m", 2).alias("ipca_12m"),
            F.round(((1 + F.col("selic_meta_aa") / 100) / (1 + F.col("ipca_12m") / 100) - 1) * 100, 2).alias("juro_real_aa"),
        )
    )
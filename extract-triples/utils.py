def coalesce(*values):
    """Return the first non-None value or None if all values are None"""
    return next((v for v in values if v is not None), None)


TRIPLE_STORE = 'main.triplestore.raw_quads'
def save_graph(spark, triples, graph_name:str, table_name = TRIPLE_STORE):
    from pyspark.sql.functions import lit
    triples_df = spark.createDataFrame(triples, schema="s STRING, p STRING, o STRING")
    quads_df = triples_df.withColumn('g',lit(f'ex:{graph_name}'))
    quads_df.write.saveAsTable(TRIPLE_STORE,mode='append')
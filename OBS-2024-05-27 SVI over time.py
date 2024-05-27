# Databricks notebook source
from pyspark.sql.functions import col, split

# COMMAND ----------

pdf_geo = (spark.read.table("mimi_ws_1.cdc.svi_county_y2010")
            .select("fips","shape").toPandas())
pdf_geo["longitude"] = pdf_geo["shape"].apply(lambda x: [float(y) for y in x[1:-1].split(",")][0])
pdf_geo["latitude"] = pdf_geo["shape"].apply(lambda x: [float(y) for y in x[1:-1].split(",")][1])

# COMMAND ----------

df_geo = spark.createDataFrame(pdf_geo.loc[:,["fips", "latitude", "longitude"]])

# COMMAND ----------

df = (spark.read.table("mimi_ws_1.cdc.svi_county_multiyears")
        .filter(col("svi") >= 0.9985)
        .join(df_geo, on="fips", how="left"))

# COMMAND ----------

df.display()

# COMMAND ----------



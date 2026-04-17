base_path = "Files/prod-outbound/"

folders = [f.name.replace("/", "") for f in mssparkutils.fs.ls(base_path) if f.isDir]

from pyspark.sql.functions import current_timestamp

for folder in folders:
    path = base_path + folder + "/*.json"
    df = spark.read.option("multiline", "true").json(path)
    df = df.withColumn("ingestion_time", current_timestamp())
    table_path = f"Tables/outbound_{folder}"
    df.write.format("delta").mode("append").save(table_path)

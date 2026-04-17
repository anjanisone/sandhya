storage_account_name = "stflcsftp"
container_name = "prod-outbound"
access_key = "<YOUR_ACCESS_KEY>"

spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net",
    access_key
)

base_path = f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net/"

folders = [f.name.replace("/", "") for f in dbutils.fs.ls(base_path) if f.isDir()]

from pyspark.sql.functions import current_timestamp

for folder in folders:
    path = base_path + folder + "/*.json"
    df = spark.read.option("multiline", "true").json(path)
    df = df.withColumn("ingestion_time", current_timestamp())
    table_name = f"outbound_{folder}"
    df.write.format("delta").mode("overwrite").saveAsTable(table_name)

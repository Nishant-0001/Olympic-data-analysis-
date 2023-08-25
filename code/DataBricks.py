from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType, DoubleType, BooleanType, DateType
from pyspark.sql.functions import avg
configs = {"fs.azure.account.auth.type": "OAuth",
"fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
"fs.azure.account.oauth2.client.id": "1a341cbe-d54f-4e20-adb0-62b957ef8947",
"fs.azure.account.oauth2.client.secret":"unU8Q~ZXWalRSGWz~bJ2cZKEieRHIdqPjV2BSabg",
"fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/4afcaf18-e417-4791-9b62-457b3caf4863/oauth2/token"}


dbutils.fs.mount(
    source="abfss://tokyo-olympic-data@tokyoolympics31.dfs.core.windows.net", # container@storageacc
    mount_point="/mnt/tokyoolymic",
 extra_configs=configs)
%fs
ls "/mnt/tokyoolymic"
Athletes=spark.read.format("csv").option("header","True").option("inferSchema","True").load("/mnt/tokyoolymic/raw-data/athletes.csv")
Coaches=spark.read.format("csv").option("header","True").option("inferSchema","True").load("/mnt/tokyoolymic/raw-data/coaches.csv")
EntriesGender=spark.read.format("csv").option("header","True").option("inferSchema","True").load("/mnt/tokyoolymic/raw-data/entriesGender.csv")
Medals=spark.read.format("csv").option("header","True").option("inferSchema","True").load("/mnt/tokyoolymic/raw-data/medals.csv")
Teams=spark.read.format("csv").option("header","True").option("inferSchema","True").load("/mnt/tokyoolymic/raw-data/teams.csv")
%fs
ls "/mnt/tokyoolymic"
Athletes=spark.read.format("csv").option("header","True").option("inferSchema","True").load("/mnt/tokyoolymic/raw-data/athletes.csv")
Coaches=spark.read.format("csv").option("header","True").option("inferSchema","True").load("/mnt/tokyoolymic/raw-data/coaches.csv")
EntriesGender=spark.read.format("csv").option("header","True").option("inferSchema","True").load("/mnt/tokyoolymic/raw-data/entriesGender.csv")
Medals=spark.read.format("csv").option("header","True").option("inferSchema","True").load("/mnt/tokyoolymic/raw-data/medals.csv")
Teams=spark.read.format("csv").option("header","True").option("inferSchema","True").load("/mnt/tokyoolymic/raw-data/teams.csv")
Athletes.show()
Coaches.show()
EntriesGender.show()
Medals.show()
Teams.show()
Athletes.show()
Athletes.printSchema()
Coaches.show()
Coaches.printSchema()
EntriesGender.show()
EntriesGender.printSchema()
EntriesGender = EntriesGender.withColumn("Female",col("Female").cast(IntegerType()))\
    .withColumn("Male",col("Male").cast(IntegerType()))\
    .withColumn("Total",col("Total").cast(IntegerType()))
Medals.printSchema()
Medals.show()
Athletes.repartition(1).write.mode("overwrite").option("header",'true').csv("/mnt/tokyoolymic/transform-data/athletes")
Coaches.repartition(1).write.mode("overwrite").option("header","true").csv("/mnt/tokyoolymic/transform-data/coaches")
EntriesGender.repartition(1).write.mode("overwrite").option("header","true").csv("/mnt/tokyoolymic/transform-data/entriesgender")
Medals.repartition(1).write.mode("overwrite").option("header","true").csv("/mnt/tokyoolymic/transform-data/medals")
Teams.repartition(1).write.mode("overwrite").option("header","true").csv("/mnt/tokyoolymic/transform-data/teams")

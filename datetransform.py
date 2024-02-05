from pyspark.sql import SparkSession
from pyspark.sql.functions import split

spark=SparkSession.builder.appName("Date Transformations").getOrCreate()

data = [("A", "1–1/1990"),
        ("B", "1\2/1990"),
        ("C", "1–3/1990"),
        ("D", "1\4\1990")
        ]

schema = ["Name","DOB"]

df = spark.createDataFrame(data, schema)

#df.count()

split_date = split(df["DOB"],"[\\\\/]")


df_final=df.withColumn("Year",split_date.getItem(0)) \
        .withColumn("Month",split_date.getItem(1)) \
        .withColumn("Date",split_date.getItem(2))

df_final.show()
from pyspark.sql import SparkSession
from pyspark.sql import Row

# Initialize Spark session
spark = SparkSession.builder.appName("example").getOrCreate()

# Create data
data = [
    Row(id=1, name="Muhammad", age=22),
    Row(id=2, name="Abdullah", age=24),
    Row(id=3, name="Ahmed", age=44),
    Row(id=4, name="John", age=55)
]

# Create DataFrame
df = spark.createDataFrame(data)

# Show DataFrame
df.show()

df.printSchema()
from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("example").getOrCreate()

# Read CSV file into DataFrame
store_df = spark.read.csv('store.csv')

# Show DataFrame
store_df.show()


from pyspark.sql import SparkSession
from pyspark.sql.types import *

# Define the schema
Schema = StructType([
    StructField('Store', StringType(), nullable=True),
    StructField('StoreType', StringType(), nullable=True),
    StructField('Assortment', StringType(), nullable=True),
    StructField('CompetitionDistance', FloatType(), nullable=True),
    StructField('CompetitionOpenSinceMonth', IntegerType(), nullable=True),
    StructField('CompetitionOpenSinceYear', IntegerType(), nullable=True),
    StructField('Promo2', IntegerType(), nullable=True),
    StructField('Promo2SinceWeek', IntegerType(), nullable=True),
    StructField('Promo2SinceYear', IntegerType(), nullable=True),
    StructField('PromoInterval', StringType(), nullable=True)
])

# Initialize Spark session
spark = SparkSession.builder.appName("example").getOrCreate()

# Read CSV file into DataFrame with schema
df = spark.read.option("header", True).schema(Schema).csv('store.csv')

# Show DataFrame
df.show()

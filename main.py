from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder.getOrCreate()
shows = spark.read.json("Data/silicon-valley.json")
# shows.printSchema()
print(shows.columns)

array_subset = shows.select("name", "genres")

array_subset = array_subset.select(
    "name",
    array_subset.genres[0].alias("dot_and_index"), 
    F.col("genres")[0].alias("col_and_index"),
    array_subset.genres.getItem(0).alias("dot_and_method"), 
    F.col("genres").getItem(0).alias("col_and_method"),
)

# array_subset.show()
array_subset_repeated = array_subset.select(
    "name",
    F.lit("Comedy").alias("one"),
    F.lit("Horror").alias("two"),
    F.lit("Drama").alias("three"),
    F.col("dot_and_index"),
).select(
    "name",
    F.array("one", "two", "three").alias("Some_Genres"),
    F.array_repeat("dot_and_index", 5).alias("Repeated_Genres"),

array_subset_repeated.show(1, False)

array_subset_repeated.select(
    "name", F.size("Some_Genres"), F.size("Repeated_Genres")
).show()

array_subset_repeated.select(
    "name", F.array_distinct("Some_Genres"), F.array_distinct("Repeated_Genres")
).show(1, False)
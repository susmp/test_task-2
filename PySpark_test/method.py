from pyspark.sql import SparkSession


def get_dataframe(products, categories):
    spark = SparkSession.builder.appName("ProductCategory").getOrCreate()
    result_df = products.join(categories, on="CategoryName", how="left")
    result_df = result_df.fillna("No Category", subset=["ProductName"])
    return result_df.select("ProductName", "CategoryName")
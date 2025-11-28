# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "3c68c896-0e93-457c-a839-0b6bb6cdf54b",
# META       "default_lakehouse_name": "New_Lakehouse",
# META       "default_lakehouse_workspace_id": "6904e35a-433d-433e-ab41-c6aa168a616e"
# META     }
# META   }
# META }

# CELL ********************

 from pyspark.sql.types import StructType, IntegerType, StringType, DoubleType

 # define the schema
 schema = StructType() \
 .add("ProductID", IntegerType(), True) \
 .add("ProductName", StringType(), True) \
 .add("Category", StringType(), True) \
 .add("ListPrice", DoubleType(), True)

 df = spark.read.format("csv").option("header","true").schema(schema).load("Files/products/products.csv")
 # df now is a Spark DataFrame containing CSV data from "Files/products/products.csv".
 display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.write.format("delta").saveAsTable("managed_products")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.write.format("delta").saveAsTable("external_product",path="abfss://Abhishek_Demo_POC@onelake.dfs.fabric.microsoft.com/New_Lakehouse.Lakehouse/Files/external_products")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC DESCRIBE formatted managed_products

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

 %%sql
 DESCRIBE FORMATTED external_product;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC create table products 
# MAGIC using DELTA 
# MAGIC location 'Files/external_products' 

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC select * from products;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC update products
# MAGIC set ListPrice = ListPrice * 0.9
# MAGIC where category = 'Mountain Bikes';

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC Describe History products;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

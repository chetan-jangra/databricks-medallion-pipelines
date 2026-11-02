# Databricks notebook source
# MAGIC %md
# MAGIC E-Commerce Sales Data Pipeline

# COMMAND ----------

data = [
 (101,"Laptop",50000,"North"),
 (102,"Phone",30000,"South"),
 (103,None,25000,"North"),
 (104,"Tablet",None,"East"),
 (101,"Laptop",50000,"North"),
 (105,"Headphone",10000,"West")
]

bronze_df = spark.createDataFrame(
    data,
    schema = ["order_id", "product", "amount", "region"]
)
bronze_df.show()

# COMMAND ----------

bronze_df.write.format("delta") \
    .mode("overwrite") \
        .save("/Volumes/workspace/default/my_volume/delta/bronze_sales/sales")

# COMMAND ----------

bronze_df = spark.read.format("delta") \
    .load("/Volumes/workspace/default/my_volume/delta/bronze_sales/sales")


# COMMAND ----------

silver_df = bronze_df.dropDuplicates()

# COMMAND ----------

silver_df = silver_df.fillna({
    "product":"Unknown",
    "amount": 0
})

# COMMAND ----------

from pyspark.sql.functions import col

silver_df = silver_df.withColumn(
    "Tax",
    col("amount") * 0.18
)

# COMMAND ----------

from pyspark.sql.functions import when,col

silver_df = silver_df.withColumn(
    "category",
    when(col("amount")>= 40000,"Premium") 
    .otherwise("Standard")
)

# COMMAND ----------

from pyspark.sql.functions import sum 
silver_df.agg(sum("amount").alias("total_salary")).show()

# COMMAND ----------

silver_df.write.format("delta") \
    .mode("overwrite") \
        .save("/Volumes/workspace/default/my_volume/delta/silver_sales/sales")

# COMMAND ----------

gold_df = spark.read.format("delta") \
    .load("/Volumes/workspace/default/my_volume/delta/silver_sales/sales")

# COMMAND ----------

gold_df.groupBy("region") \
    .agg(sum("amount").alias("revenue")) \
        .show()

# COMMAND ----------

gold_df.groupBy("product") \
    .agg(sum("amount").alias("revenue")) \
        .show()

# COMMAND ----------

from pyspark.sql.functions import * 
from pyspark.sql.window import Window

window_spec = Window.partitionBy("region") \
    .orderBy(col("amount").desc())

rank_df = gold_df.withColumn(
    "rnk",
    dense_rank().over(window_spec)
)

rank_df.filter(
    col("rnk") == 1
).show()

# COMMAND ----------

gold_df.write.format("delta") \
    .mode("overwrite") \
        .save("/Volumes/workspace/default/my_volume/delta/gold_sales/sales")

# COMMAND ----------

bronze_df.show()

# COMMAND ----------

silver_df.show()

# COMMAND ----------

gold_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Customer Analytics Pipeline

# COMMAND ----------

data = [
 (1,"John",5000),
 (2,"Jane",12000),
 (3,"Mike",8000),
 (4,"Sara",25000),
 (5,"Tom",None),
 (2,"Jane",12000)
]

columns = [
 "customer_id",
 "customer_name",
 "purchase_amount"
]

bronze_df = spark.createDataFrame(
    data,
    columns
)

bronze_df.write.format("delta") \
    .mode("overwrite") \
        .save("/Volumes/workspace/default/my_volume/delta/bronze_customers/customers")

# COMMAND ----------

silver_df = spark.read.format("delta") \
    .load("/Volumes/workspace/default/my_volume/delta/bronze_customers/customers")


# COMMAND ----------

silver_df = silver_df.dropDuplicates()

# COMMAND ----------

silver_df = silver_df.fillna({
    "purchase_amount":0
})

# COMMAND ----------

from pyspark.sql.functions import when,col 
silver_df = silver_df.withColumn(
    "segment",
    when(col("purchase_amount") >=20000,"VIP") 
    .when(col("purchase_amount") >=10000,"Premium")
    .otherwise("Regular")
)

# COMMAND ----------

silver_df.write.format("delta") \
    .mode("overwrite") \
        .save("/Volumes/workspace/default/my_volume/delta/silver_customers/customers")  
display(silver_df)

# COMMAND ----------

from pyspark.sql.functions import avg

gold_avg_df = silver_df.agg(avg("purchase_amount").alias("avg_spend"))

# COMMAND ----------

gold_segment_df = silver_df.groupBy("segment").count()

# COMMAND ----------

from pyspark.sql.functions import sum 

gold_revenue_df = silver_df.groupBy("segment") \
    .agg(sum("purchase_amount").alias("revenue"))
    

# COMMAND ----------

from pyspark.sql.window import Window 
from pyspark.sql.functions import dense_rank

window_spec = Window.orderBy(
    col("purchase_amount").desc()
)

rank_df = silver_df.withColumn(
    "customer_rank",
    dense_rank().over(window_spec)
)

rank_df.show()

# COMMAND ----------

rank_df.filter(
    col("customer_rank") ==1
).show()

# COMMAND ----------

gold_df = rank_df 

gold_df.write.format("delta") \
    .mode("overwrite") \
        .save("/Volumes/workspace/default/my_volume/delta/gold_customers/customers")
display(gold_df)

# COMMAND ----------

display(silver_df)

# COMMAND ----------

display(bronze_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Banking Transaction Analytics Pipeline

# COMMAND ----------

data = [
 (101,"John",5000),
 (102,"Jane",25000),
 (103,"Mike",120000),
 (104,"Sara",8000),
 (105,"Tom",70000),
 (103,"Mike",120000),
 (106,"Alex",None)
]

columns = [
 "transaction_id",
 "customer_name",
 "amount"
]

bronze_df = spark.createDataFrame(
    data,
    columns
)

bronze_df.write.format("delta") \
    .mode("overwrite") \
        .save("/Volumes/workspace/default/my_volume/delta/bronze_Banking/bank_transactions")
display(bronze_df)

# COMMAND ----------

silver_df = spark.read.format("delta") \
    .load("/Volumes/workspace/default/my_volume/delta/bronze_Banking/bank_transactions")

# COMMAND ----------

silver_df = silver_df.dropDuplicates()

# COMMAND ----------

silver_df = silver_df.fillna({
    "amount": 0
})

# COMMAND ----------

from pyspark.sql.functions import when,col 

silver_df = silver_df.withColumn(
    "risk_categories",
    when(col("amount") >= 100000,"High Risk")
    .when(col("amount") >= 50000,"Medium Risk")
    .otherwise("Low Risk")
)

# COMMAND ----------

silve_df = silver_df.withColumn(
    "transcation_fee",
    col("amount") * 0.02
)

# COMMAND ----------

silver_df.write.format("delta") \
    .mode("overwrite") \
        .option("overwriteSchema", "true") \
            .save("/Volumes/workspace/default/my_volume/delta/silver_Banking/bank_transactions")

# COMMAND ----------

gold_source_df = spark.read.format("delta") \
    .load("/Volumes/workspace/default/my_volume/delta/silver_Banking/bank_transactions")


# COMMAND ----------

from pyspark.sql.functions import sum 

gold_total_df = gold_source_df.agg(sum("amount").alias("total_amount"))

# COMMAND ----------

gold_risk_df = gold_source_df.groupBy("risk_categories").count()

# COMMAND ----------

gold_revenue_df = gold_source_df.groupBy("risk_categories") \
    .agg(sum("amount")).alias("total_amount")

# COMMAND ----------

from pyspark.sql.window import Window 
from pyspark.sql.functions import dense_rank,col

window_spec = Window.orderBy(
    col("amount").desc()
)

rank_df = silver_df.withColumn(
    "customer_rank",
    dense_rank().over(window_spec)
)

rank_df.show()

# COMMAND ----------

rank_df.filter(
    col("customer_rank") == 1
).show()

# COMMAND ----------

gold_df = rank_df
gold_df.write.format("delta") \
    .mode("overwrite") \
        .save("/Volumes/workspace/default/my_volume/delta/gold_Banking/bank_transactions")

# COMMAND ----------

display(gold_df)

# COMMAND ----------

gold_source_df.printSchema()

# COMMAND ----------

gold_source_df.show()

# COMMAND ----------

display(silver_df)

# COMMAND ----------

display(bronze_df)

# COMMAND ----------

gold_source_df.printSchema()

# COMMAND ----------

gold_risk_df.show()

# COMMAND ----------

gold_revenue_df.show()

# COMMAND ----------

rank_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Employee HR Analytics Pipeline

# COMMAND ----------

data = [
 (101,"John","IT",70000),
 (102,"Jane","HR",50000),
 (103,"Mike","IT",90000),
 (104,"Sara","Finance",120000),
 (105,"Tom","HR",60000),
 (106,"Alex","Finance",None),
 (103,"Mike","IT",90000)
]

columns = [
 "emp_id",
 "emp_name",
 "department",
 "salary"
]

bronze_df = spark.createDataFrame(
    data,
    columns
)

bronze_df.write.format("delta") \
    .mode("overwrite") \
        .save("/Volumes/workspace/default/my_volume/delta/bronze_HR/employees")
display(bronze_df)

# COMMAND ----------

silver_df = spark.read.format("delta") \
    .load("/Volumes/workspace/default/my_volume/delta/bronze_HR/employees")

# COMMAND ----------

silver_df = silver_df.dropDuplicates()

# COMMAND ----------

silver_df = silver_df.fillna({
    "salary":0
})

# COMMAND ----------

from pyspark.sql.functions import when,col 

silver_df = silver_df.withColumn(
    "salary_band",
    when(col("salary") >= 100000,"Executive")
    .when(col("salary") >= 70000,"Senior")
    .otherwise("Junior")
)

# COMMAND ----------

silver_df.write.format("delta") \
    .mode("overwrite") \
        .save("/Volumes/workspace/default/my_volume/delta/silver_HR/employees")

# COMMAND ----------

gold_source_df = spark.read.format("delta") \
    .load("/Volumes/workspace/default/my_volume/delta/silver_HR/employees")

# COMMAND ----------

from pyspark.sql.functions import avg

gold_avg_df = gold_source_df.groupBy("department") \
    .agg(avg("salary").alias("avg_salary"))


# COMMAND ----------

gold_count_df = gold_source_df.groupBy("department").count()

# COMMAND ----------

gold_band_df = gold_source_df.groupBy("salary_band").count()

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import dense_rank 

window_spec = Window.partitionBy("department") \
    .orderBy(col("salary").desc())

rank_df = gold_source_df.withColumn(
    "dept_rank",
    dense_rank().over(window_spec)
)

# COMMAND ----------

rank_df.filter(
    col("dept_rank") ==1
).show()

# COMMAND ----------

gold_df = rank_df 

gold_df.write.format("delta") \
    .mode("overwrite") \
        .save("/Volumes/workspace/default/my_volume/delta/gold_HR/employees")

# COMMAND ----------

display(bronze_df)

# COMMAND ----------

display(silver_df)

# COMMAND ----------

display(gold_df)

# COMMAND ----------

display(gold_source_df)

# COMMAND ----------

display(gold_avg_df)
display(gold_count_df)
display(gold_band_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Retail Inventory Analytics Pipeline

# COMMAND ----------

data = [
 (101,"Laptop",50,50000,"Delhi"),
 (102,"Phone",120,30000,"Mumbai"),
 (103,"Tablet",20,20000,"Delhi"),
 (104,"Monitor",10,15000,"Chennai"),
 (105,"Keyboard",200,2000,"Mumbai"),
 (103,"Tablet",20,20000,"Delhi"),
 (106,"Mouse",None,1000,"Delhi")
]

columns = [
 "product_id",
 "product_name",
 "stock_qty",
 "unit_price",
 "warehouse"
]
bronze_df = spark.createDataFrame(
    data,
    columns
)

bronze_df.write.format("delta") \
    .mode("overwrite") \
        .save("/Volumes/workspace/default/my_volume/delta/bronze_inventory/products")
display(bronze_df)

# COMMAND ----------

silver_df = spark.read.format("delta") \
    .load("/Volumes/workspace/default/my_volume/delta/bronze_inventory/products")


# COMMAND ----------

silver_df = silver_df.dropDuplicates()

# COMMAND ----------

silver_df = silver_df.fillna({
    "stock_qty":0
})

# COMMAND ----------

from pyspark.sql.functions import when,col

silver_df = silver_df.withColumn(
    "stock_status",
    when(col("stock_qty") <= 20,"Low Stock")
    .otherwise("Sufficient Stock")
)

# COMMAND ----------

silver_df = silver_df.withColumn(
    "inverntory_value",
    col("stock_qty") * col("unit_price")
)

# COMMAND ----------

silver_df.write.format("delta") \
    .mode("overwrite") \
        .option("overwriteSchema", "true") \
            .save("/Volumes/workspace/default/my_volume/delta/silver_inventory/products")

# COMMAND ----------

display(silver_df)

# COMMAND ----------

gold_source_df = spark.read.format("delta") \
    .load("/Volumes/workspace/default/my_volume/delta/silver_inventory/products")   

# COMMAND ----------

from pyspark.sql.functions import sum

gold_inventory_df = gold_source_df.groupBy("warehouse") \
    .agg(sum("inverntory_value").alias("total_inventory_value"))


# COMMAND ----------

gold_stock_df = gold_source_df.groupBy("stock_status").count()

# COMMAND ----------

gold_warehouse_df = gold_source_df.groupBy("warehouse").count()

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import (
    row_number,
    rank,
    dense_rank
)

window_spec = Window.orderBy(col("inverntory_value").desc())

row_df = gold_source_df.withColumn(
    "row_num",
    row_number().over(window_spec)
)

rank_df = gold_source_df.withColumn(
    "rank_num",
    rank().over(window_spec)
)

dense_df = gold_source_df.withColumn(
    "dense_rank",
    dense_rank().over(window_spec)
)

# COMMAND ----------

dense_df.filter(
    col("dense_rank") == 1
).show()

# COMMAND ----------

gold_df = dense_df 

gold_df.write.format("delta") \
    .mode("overwrite") \
        .option("overwriteSchema", "true") \
            .save("/Volumes/workspace/default/my_volume/delta/gold_inventory/products")

# COMMAND ----------

display(gold_df)
display(gold_stock_df)
display(gold_warehouse_df)
display(gold_inventory_df)
display(row_df)
display(rank_df)
display(dense_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Healthcare Analytics Pipeline

# COMMAND ----------

data = [
 (101,"John","Cardiology",50000),
 (102,"Jane","Neurology",120000),
 (103,"Mike","Orthopedics",30000),
 (104,"Sara","Cardiology",200000),
 (105,"Tom","Neurology",80000),
 (106,"Alex","Orthopedics",None),
 (102,"Jane","Neurology",120000)
]

columns = [
 "patient_id",
 "patient_name",
 "department",
 "treatment_cost"
]

bronze_df = spark.createDataFrame(
    data,
    columns
)

bronze_df.write.format("delta") \
    .mode("overwrite") \
        .save("/Volumes/workspace/default/my_volume/delta/bronze_healthcare/patients")

display(bronze_df)

# COMMAND ----------

silver_df = spark.read.format("delta") \
    .load("/Volumes/workspace/default/my_volume/delta/bronze_healthcare/patients")

# COMMAND ----------

silver_df = silver_df.dropDuplicates()

# COMMAND ----------

silver_df = silver_df.fillna({
    "treatment_cost": 0
})

# COMMAND ----------

from pyspark.sql.functions import when,col

silver_df = silver_df.withColumn(
    "risk_category",
    when(col("treatment_cost") >= 150000,"Critical")
    .when(col("treatment_cost") >= 80000,"High Risk")
    .otherwise("Normal")
)

# COMMAND ----------

silver_df.write.format("delta") \
    .mode("overwrite") \
        .save("/Volumes/workspace/default/my_volume/delta/silver_healthcare/patients")
display(silver_df)


# COMMAND ----------

gold_source_df = spark.read.format("delta")\
    .load("/Volumes/workspace/default/my_volume/delta/silver_healthcare/patients")


# COMMAND ----------

from pyspark.sql.functions import avg

gold_avg_df = gold_source_df.groupBy("department") \
    .agg(avg("treatment_cost").alias("avg_treatment_cost"))

# COMMAND ----------

gold_count_df = gold_source_df.groupBy("department").count()

# COMMAND ----------

gold_risk_df = gold_source_df.groupBy("risk_category").count()

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import dense_rank,col

window_spec = Window.partitionBy("department") \
    .orderBy(col("treatment_cost").desc())

rank_df = gold_source_df.withColumn(
    "dept_rank",
    dense_rank().over(window_spec)
)


# COMMAND ----------

rank_df.filter(
    col("dept_rank")==1
).show()

# COMMAND ----------

gold_df = rank_df

gold_df.write.format("delta") \
    .mode("overwrite") \
        .save("/Volumes/workspace/default/my_volume/delta/gold_healthcare/patients")

# COMMAND ----------

display(gold_df)

# COMMAND ----------

display(gold_avg_df)
display(gold_count_df)
display(gold_risk_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Telecom Customer Churn Analytics Pipeline

# COMMAND ----------

data = [
 (101,"John",24,800,0),
 (102,"Jane",12,600,1),
 (103,"Mike",36,1200,0),
 (104,"Sara",6,400,1),
 (105,"Tom",48,1500,0),
 (106,"Alex",18,None,1),
 (102,"Jane",12,600,1)
]

columns = [
 "customer_id",
 "customer_name",
 "tenure_months",
 "monthly_bill",
 "churn"
]

bronze_df = spark.createDataFrame(
    data,
    columns
)

bronze_df.write.format("delta") \
    .mode("overwrite") \
        .save("/Volumes/workspace/default/my_volume/delta/bronze_telecom/customers")
display(bronze_df)

# COMMAND ----------

silver_df = spark.read.format("delta") \
    .load("/Volumes/workspace/default/my_volume/delta/bronze_telecom/customers")


# COMMAND ----------

silver_df = silver_df.dropDuplicates()

# COMMAND ----------

silver_df = silver_df.fillna({
    "monthly_bill": 0
})


# COMMAND ----------

from pyspark.sql.functions import when,col

silver_df = silver_df.withColumn(
    "customer_segment",
    when(col("monthly_bill") >= 1000,"Premium")
    .when(col("monthly_bill") >=500,"Standard")
    .otherwise("Basic")
)

# COMMAND ----------

silver_df = silver_df.withColumn(
    "annual_revenue",
    col("monthly_bill") * 12
)

# COMMAND ----------

silver_df.write.format("delta") \
    .mode("overwrite") \
        .save("/Volumes/workspace/default/my_volume/delta/silver_telecom/customers")

# COMMAND ----------

gold_source_df = spark.read.format("delta") \
    .load("/Volumes/workspace/default/my_volume/delta/silver_telecom/customers")


# COMMAND ----------

gold_segment_df = gold_source_df.groupBy("customer_segment").count()

# COMMAND ----------

from pyspark.sql.functions import sum 

gold_revenue_df = gold_source_df.groupBy("customer_segment") \
    .agg(sum("annual_revenue").alias("total_annual_revenue"))


# COMMAND ----------

gold_churn_df = gold_source_df.groupBy("churn").count()


# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import dense_rank,col

window_spec = Window.partitionBy("customer_segment") \
    .orderBy(col("annual_revenue").desc())

rank_df = gold_source_df.withColumn(
    "Segment_rank",
    dense_rank().over(window_spec)
)

# COMMAND ----------

rank_df.filter(
    col("Segment_rank") ==1
).show()

# COMMAND ----------

gold_df = rank_df 

gold_df.write.format("delta") \
    .mode("overwrite") \
        .save("/Volumes/workspace/default/my_volume/delta/gold_telecom/customers")

# COMMAND ----------

display(gold_df)

# COMMAND ----------

display(gold_revenue_df)
display(gold_segment_df)
display(gold_churn_df)


# COMMAND ----------


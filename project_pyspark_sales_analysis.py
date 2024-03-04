# Databricks notebook source
spark

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
import calendar

# COMMAND ----------

# DBTITLE 1,Sales_DF
myschema_sales=StructType([StructField("product_id",IntegerType(),True),
                      StructField("customer_id",StringType(),True),
                      StructField("order_date",DateType(),True),
                      StructField("location",StringType(),True),
                      StructField("source_order",StringType(),True)
                      ])
sales_df=spark.read.format("csv").option("inferSchema","True").schema(myschema_sales).load("/FileStore/tables/sales_csv.txt")
display(sales_df)

# COMMAND ----------

sales_df=sales_df.withColumn("order_month",date_format(sales_df.order_date,'MMMM'))
sales_df=sales_df.withColumn("order_quarter",quarter(sales_df.order_date))
sales_df=sales_df.withColumn("order_year",year(sales_df.order_date))

display(sales_df)


# COMMAND ----------

# DBTITLE 1,Menu_DF
myschema_menu=StructType([StructField("product_id",IntegerType(),True),
                          StructField("product_name",StringType(),True),
                          StructField("price",StringType(),True)])
menu_df=spark.read.format("csv").option("inferSchema","True").schema(myschema_menu).load("/FileStore/tables/menu_csv.txt")

display(menu_df)

# COMMAND ----------

# DBTITLE 1,Total Amount Spent by Each Customer
innerJoin_df=sales_df.join(menu_df,menu_df.product_id==sales_df.product_id,"inner").groupBy('customer_id').agg(sum("price").alias("Total_Amt")).orderBy('customer_id')
innerJoin_df.show()

# COMMAND ----------

# DBTITLE 1,Total Amount Spent by Each Food Category
innerJoin_df=sales_df.join(menu_df,menu_df.product_id==sales_df.product_id,"inner").groupBy('customer_id').agg(sum("price").alias("Total_Amt")).orderBy('customer_id')
display(innerJoin_df)

# COMMAND ----------

# DBTITLE 1,Total Amount of sales in each month
innerJoin_df=sales_df.join(menu_df,menu_df.product_id==sales_df.product_id,"inner").groupBy('order_month','product_name').agg(sum("price").alias("Total_Amt")).orderBy('order_month','product_name')
display(innerJoin_df)

# COMMAND ----------

# DBTITLE 1,Yearly Sales
innerJoin_df=sales_df.join(menu_df,menu_df.product_id==sales_df.product_id,"inner").groupBy('order_year',"customer_id").agg(sum("price").alias("Total_Amt")).orderBy(col('order_year').desc(),col('customer_id'))
display(innerJoin_df)

# COMMAND ----------

# DBTITLE 1,Total Number of order by each category
innerJoin_df=sales_df.join(menu_df,menu_df.product_id==sales_df.product_id,"inner").groupBy('Product_name').agg(count("*").alias("Total_Orders")).orderBy(col('Product_name'))
display(innerJoin_df)

# COMMAND ----------

# DBTITLE 1,Frequency of customer visited in Restaturant
# innerJoin_df=sales_df.join(menu_df,menu_df.product_id==sales_df.product_id,"inner").filter(col('source_order')=='Restaurant').groupBy('customer_id').agg(countDistinct('order_date').alias("Total_Amt")).orderBy(col('customer_id').asc())
# innerJoin_df.show()
innerJoin_df=sales_df.join(menu_df,menu_df.product_id==sales_df.product_id,"inner").groupBy('source_order').agg(countDistinct('order_date').alias("Total_Orders"))
display(innerJoin_df)

# COMMAND ----------

innerJoin_df=sales_df.join(menu_df,menu_df.product_id==sales_df.product_id,"inner").groupBy('location','product_name').agg(countDistinct('order_date').alias("Total_Orders_CountryWise"))
display(innerJoin_df)

from pyspark.sql.functions import *
from pyspark.sql.types import *
from typing import List
from pyspark.sql import DataFrame
from pyspark.sql.window import Window
from delta.tables import DeltaTable

import os
import sys

current_dir = os.getcwd()

df_cust = spark.read.table('pysparkdbt.bronze.customers')
display(df_cust.count())
display(df_cust)

df_cust = df_cust.withColumn("domain", split(col('email'),'@')[1])

df_cust = df_cust.withColumn("phone_number", regexp_replace("phone_number", r"[^0-9]", ""))
display(df_cust)

df_cust = df_cust.withColumn("full_name", concat_ws(" ", col('first_name'), col('last_name')))
df_cust = df_cust.drop('first_name', 'last_name')
display(df_cust)

class transformations:
    def dedup(self, df, dedup_cols:List, cdc:str):
        df = df.withColumn("dedupKey", concat(*dedup_cols))
        df = df.withColumn("dedupCounts", row_number().over(Window.partitionBy("dedupKey").orderBy(desc(cdc))))
        df = df.filter(col('dedupCounts') == 1)
        df = df.drop("dedup.Key", "dedupCounts")

        return df
    
    def process_timestamp(self, df):
        df = df.withColumn("process_timestamp", current_timestamp())

        return df
    
    def upsert(self, df, key_cols, table, cdc):
        merge_condition = " AND ".join([f"src.{i}= trg.{i}" for i in key_cols])
        dlt_obj = DeltaTable.forName(spark, f"pysparkdbt.silver.{table}")
        dlt_obj.alias("trg").merge(df.alias("src"), merge_condition)\
            .whenMatchedUpdateAll(condition = f"src.last_updated_timestamp >= trg.{cdc}")\
            .whenNotMatchedInsertAll()\
            .execute()
        return 1

cust_obj = transformations()

cust_df_trns = cust_obj.dedup(df_cust, ['customer_id'], 'last_updated_timestamp')
display(cust_df_trns)

if not spark.catalog.tableExists("pysparkdbt.silver.customers"):
    
    df_cust.write.format("delta")\
        .mode("append")\
            .saveAsTable("pysparkdbt.silver.customers")

else:
    cust_obj.upsert(df_cust, ['customer_id'], 'customers', 'last_updated_timestamp')

%md
### **DRIVERS**

df_drivers = spark.read.table("pysparkdbt.bronze.drivers")
display(df_drivers)

df_drivers = df_drivers.withColumn("phone_number", regexp_replace("phone_number", r"[^0-9]", ""))

df_drivers = df_drivers.withColumn("full_name", concat_ws(" ", col('first_name'), col('last_name')))
df_drivers = df_drivers.drop('first_name', 'last_name')
display(df_drivers)

driver_obj = transformations()

df_driver = driver_obj.dedup(df_drivers, ['driver_id'], 'last_updated_timestamp')

if not spark.catalog.tableExists("pysparkdbt.silver.drivers"):
    df_drivers.write.format("delta")\
        .mode("append")\
        .saveAsTable("pysparkdbt.silver.drivers")
else:
    driver_obj.upsert(df_drivers, ['driver_id'], 'drivers', 'last_updated_timestamp')

%md
### **LOCATIONS**

df_loc = spark.read.table("pysparkdbt.bronze.locations")
display(df_loc)

loc_obj = transformations()
df_loc = loc_obj.dedup(df_loc, ['location_id'], 'last_updated_timestamp')
df_loc = loc_obj.process_timestamp(df_loc)

if not spark.catalog.tableExists("pysparkdbt.silver.locations"):
    df_loc.write.format("delta")\
        .mode("append")\
        .saveAsTable("pysparkdbt.silver.locations")
else:
    loc_obj.upsert(df_loc, ['location_id'], 'locations', 'last_updated_timestamp')

%md
### **PAYMENTS**

df_pay = spark.read.table("pysparkdbt.bronze.payments")
display(df_pay)

df_pay = df_pay.withColumn("online_payment_status",
            when( ((col('payment_method') =='Card') & (col('payment_status') == 'success')), "online-success")
            .when( ((col('payment_method') == 'Card') & (col('payment_status') == 'failed')), "online-failed")
            .when( ((col('payment_method')=='Card') & (col('payment_status') == 'pending')), "online-pending")
            .otherwise("offline")
            )

pay_obj = transformations()

df_pay = pay_obj.dedup(df_pay, ['payment_id'], 'last_updated_timestamp')
df_pay = pay_obj.process_timestamp(df_pay) 

if not spark.catalog.tableExists("pysparkdbt.silver.payments"):
    df_pay.write.format("delta")\
        .mode("append")\
        .saveAsTable("pysparkdbt.silver.payments")
else:
    pay_obj.upsert(df_pay, ['payment_id'], 'payments', 'last_updated_timestamp')

%md
### **VEHICLES**

df_veh = spark.read.table("pysparkdbt.bronze.vehicles")
display(df_veh)

df_veh = df_veh.withColumn("make", upper(col("make")))
display(df_veh)

veh_obj = transformations()

df_veh = veh_obj.dedup(df_veh, ['vehicle_id'], 'last_updated_timestamp')
df_veh = veh_obj.process_timestamp(df_veh) 

if not spark.catalog.tableExists("pysparkdbt.silver.vehicles"):
    df_veh.write.format("delta")\
        .mode("append")\
        .saveAsTable("pysparkdbt.silver.vehicles")
else:
    veh_obj.upsert(df_veh, ['vehicle_id'], 'vehicles', 'last_updated_timestamp')

%md
### **TRIPS**

df_trips = spark.read.table("pysparkdbt.bronze.trips")
display(df_trips)

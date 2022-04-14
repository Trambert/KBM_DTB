# Databricks notebook source
Source = dbutils.widgets.get("Source")
Object = dbutils.widgets.get("Object")
deltaDB = dbutils.widgets.get("DeltaDatabase")
save_path = '/mnt/curated/'+Source+'/'+Object

table_name = Object
display(spark.sql("DROP TABLE IF EXISTS " + deltaDB + "." + table_name))
display(spark.sql("CREATE TABLE " + deltaDB + "." + table_name + " USING DELTA LOCATION '" + save_path + "'"))

# Databricks notebook source
upload_path = '/mnt/raw/SAEBUS/REC_ARRET/*/*/*/*.parquet'
df = spark.read.parquet(upload_path)
print(df.count())
df2 = df.drop_duplicates()
print(df2.count())


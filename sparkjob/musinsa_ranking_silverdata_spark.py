from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType, DateType
from pyspark.sql.functions import col, explode, lit

from pyspark.conf import SparkConf
from datetime import datetime, timedelta

import logging
import argparse

from modules.musinsa_mappingtable import SEXUAL_CATEGORY_DYNAMIC_PARAMS, mapping_2depth_kor

def create_spark_session():
    # SparkConf 설정
    conf = SparkConf()
    conf.set("spark.hadoop.fs.s3a.impl")
    conf.set("spark.hadoop.fs.s3a.access.key")
    conf.set("spark.hadoop.fs.s3a.secret.key",)
    conf.set("spark.hadoop.fs.s3a.endpoint", "s3.ap-northeast-2.amazonaws.com")  # 리전 엔드포인트 수정
    conf.set("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
    conf.set("spark.executor.memory", "4g")
    conf.set("spark.driver.memory", "2g")
    conf.set("spark.sql.parquet.compression.codec", "snappy")  # Parquet 파일 압축 설정
    # SparkSession 생성
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    return spark

def making_ranking_table(spark, json_path, master_category_code, today_date):
    df = spark.read.json(json_path)
    items_df = None
    for i in range(1, 18):
        temp_df = (
            df.select(explode(col("data.modules")[i]["items"]).alias("item"))
            .select(
                col("item.id").cast("bigint").alias("product_id"),
                col("item.info.onClickBrandName.eventLog.ga4.payload.index").cast("int").alias("ranking")
            )
        )
        temp_df = temp_df.withColumn("category", lit(master_category_code).cast("string"))
        temp_df = temp_df.withColumn("platform", lit("musinsa").cast("string"))
        temp_df = temp_df.withColumn("date", lit(today_date).cast("string"))
        
        if items_df is None:
            items_df = temp_df
        else:
            items_df = items_df.union(temp_df)
    
    items_df = items_df.dropna()
    
    # 해당 Dataframe을 쓸 때, parquet 크기가 128MB가 안넘어가기에, 1개의 파일로 저장
    items_df = items_df.coalesce(1)
    
    return items_df

def main():
    spark = create_spark_session()

    # 오늘 날짜 - 날짜 path
    today_date = (datetime.now()).strftime("%Y-%m-%d")

    for sexual_dct in SEXUAL_CATEGORY_DYNAMIC_PARAMS:
        # category1depth(성별) 추출
        sexual = list(sexual_dct['SEXUAL'].items())[0][1]
        
        category = sexual_dct['CATEGORIES']
        # category2depth 추출
        for categorydepth in category:
            categories = list(categorydepth.items())[0]
            category2depth = mapping_2depth_kor(categories[0])
            
            # category3depth 추출
            for detailcategories in categories[1]:
                detail_category = list(detailcategories.items())[0]
                category3depth = detail_category[0]

                # category4depth 추출
                for detail_category4 in detail_category[1].values():
                    category4depth = detail_category4
                    
                    # 공통 path
                    file_name = f"{category3depth}/{sexual}_{category2depth}_{category3depth}_{category4depth}"
                    
                    # input - filepath 조합
                    input_path = f"s3a://project4-raw-data/{today_date}/Musinsa/RankingData/{file_name}.json"
                
                    # output - filepath 조합
                    table_output_path = f"s3a://project4-silver-data/{today_date}/Musinsa/RankingData/{file_name}.parquet"
                    productids_output_path = f"s3a://project4-silver-data/{today_date}/Musinsa/RankingData/{file_name}.json"
                    
                    master_category_code = f"{sexual}-{category2depth}-{category3depth}"
                    
                    cleaned_df = making_ranking_table(spark, input_path ,master_category_code, today_date)
                    product_ids_df = cleaned_df.select("product_id")
                    
                    cleaned_df.write.mode("overwrite").parquet(table_output_path)
                    product_ids_df.write.mode("overwrite").json(productids_output_path)
                    
if __name__ == "__main__":
    main()
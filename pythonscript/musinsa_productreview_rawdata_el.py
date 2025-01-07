import argparse
import json
import os
import re

import requests
import pyarrow.fs as fs

from modules.config import Musinsa_Config

import modules.s3_module as s3_module

url = "https://goods.musinsa.com/api2/review/v1/view/list"

params = {
    "page" : 0,
    "pageSize" : 20,
    "sort" : "new",
    "myFilter" : "false",
    "hasPhoto" : "false",
    "isExperience" : "false"
}

today_date = Musinsa_Config.today_date

bronze_bucket = "project4-bronze-data"

def el_productreview(product_id, s3_key):
    params["goodsNo"] = product_id
    response = requests.get(url, headers=Musinsa_Config.HEADERS, params=params)
    data = response.json()['data']
    
    s3_module.upload_json_to_s3(bronze_bucket, s3_key, data)
        
def main():
    # argument
    parser = argparse.ArgumentParser(description="category2depth/category3depth")
    parser.add_argument('category3depth', type=str, help='sexual')
    parser.add_argument('category4depth_list', type=str, help='category')
    
    args = parser.parse_args()
    
    category3depth = args.category3depth
    category4depth_list = json.loads(args.category4depth_list)
    
    # product_id list 불러오기
    bucket_path = "project4-silver-data/"
    file_key = f"{today_date}/Musinsa/ProductDetailData/{category3depth}/"
    
    s3 = s3_module.connect_s3fs()
    
    base_path = bucket_path + file_key
    
    files = s3.get_file_info(fs.FileSelector(base_dir=base_path, recursive=True))
    
    # request
    for category4depth in category4depth_list:
        directory_pattern = re.compile(rf'.*{category4depth}\.parquet$')
        directories = [file for file in files if file.type == fs.FileType.Directory and directory_pattern.match(file.path)]
        
        product_ids = []
        for directory in directories:
            temp_ids = s3_module.get_product_ids(directory.path)
            product_ids += temp_ids
        
        for product_id in set(product_ids):
            output_path = f"{today_date}/Musinsa/ProductReviewData/{category3depth}/{category4depth}/{product_id}.json"
            el_productreview(product_id, output_path)
            
if __name__ == "__main__":
    main()
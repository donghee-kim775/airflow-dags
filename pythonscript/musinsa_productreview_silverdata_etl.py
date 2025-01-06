import argparse
import json
from datetime import datetime
import os
import re

import requests
import pandas as pd

from s3_validate import get_file_list


url = "https://goods.musinsa.com/api2/review/v1/view/list"

headers = {
    "accept": "application/json, text/plain, */*",
    "accept-encoding": "gzip, deflate, br, zstd",
    "accept-language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
    "origin": "https://www.musinsa.com",
    "referer": "https://www.musinsa.com/",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
}

params = {
    "page" : 0,
    "pageSize" : 20,
    "sort" : "new",
    "myFilter" : "false",
    "hasPhoto" : "false",
    "isExperience" : "false"
}

today_date = datetime.today().strftime("%Y%m%d")

bucket_path = "project4-silver-data/"

def et_productreview(product_id):
    params["goodsNo"] = product_id
    response = requests.get(url, headers=headers, params=params)
    data = response.json()['data']
    
    review_data_list = []
    for review in data['list'][:20]:
        review_data = {
            "product_id" : product_id,
            "review_content" : review['content'],
            "review_rating" : review['grade'],
            "review_date" : review['createDate'],
            "reviewer_height" : review['userProfileInfo']['userHeight'],
            "reviewer_weight" : review['userProfileInfo']['userWeight'],
            "selected_option" : review['goodsOption'],
        }
        review_data_list.append(review_data)
        
    return pd.DataFrame(review_data_list)

def main():
    parser = argparse.ArgumentParser(description="category2depth/category3depth")
    parser.add_argument('category3depth', type=str, help='sexual')
    parser.add_argument('category4depth_list', type=str, help='category')
    
    args = parser.parse_args()

    category3depth = args.category3depth
    category4depth_list = json.loads(args.category4depth_list)
    file_key = f"2025-01-04/Musinsa/ProductDetailData/{category3depth}/"
    
    path = bucket_path + file_key
    files = get_file_list(path)
    
    for category4depth in category4depth_list:
        pattern = re.compile(rf'.*_({category4depth})\.parquet$')
        parquet_files = [f"s3://{file.path}" for file in files if pattern.match(file.path)]
        df_list = [pd.read_parquet(file, storage_options=aws_storage_options) for file in parquet_files]
        merge_df = pd.concat(df_list)
        
        product_ids = merge_df['product_id'].tolist()
        for product_id in product_ids:
            review_df = et_productreview(product_id)
            print(review_df)
            
if __name__ == "__main__":
    main()
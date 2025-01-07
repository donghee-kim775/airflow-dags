import argparse
import json
from datetime import datetime
import os
import re

import requests
import pandas as pd
import pyarrow.fs as fs

from pythonscript.modules.config import musinsa_config

url = "https://goods.musinsa.com/api2/review/v1/view/list"

params = {
    "page" : 0,
    "pageSize" : 20,
    "sort" : "new",
    "myFilter" : "false",
    "hasPhoto" : "false",
    "isExperience" : "false"
}

today_date = datetime.today().strftime("%Y-%m-%d")

bucket_path = "project4-silver-data/"

def et_productreview(product_id):
    params["goodsNo"] = product_id
    response = requests.get(url, headers=musinsa_config.headers, params=params)
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
    file_key = f"{today_date}/Musinsa/ProductDetailData/{category3depth}/"
    
    path = bucket_path + file_key
    
    aws_storage_options = {
        "key" : os.getenv('AWS_ACCESS_KEY_ID'),
        "secret" : os.getenv('AWS_SECRET_ACCESS_KEY')
    }
    
    s3 = fs.S3FileSystem(
            access_key=os.getenv('AWS_ACCESS_KEY_ID'),
            secret_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
            region='ap-northeast-2',  # 올바른 리전 설정 (default : us-east-1)
            endpoint_override='https://s3.ap-northeast-2.amazonaws.com'  # 엔드포인트 설정    
    )
    
    files = s3.get_file_info(fs.FileSelector(base_dir=path, recursive=True))
    
    for category4depth in category4depth_list:
        pattern = re.compile(rf'.*_({category4depth})\.parquet$')
        parquet_files = [f"s3://{file.path}" for file in files if pattern.match(file.path)]
        df_list = [pd.read_parquet(file, storage_options=aws_storage_options) for file in parquet_files]
        merge_df = pd.concat(df_list)
        
        # 남 녀 중복 product_id 제거
        product_ids = set(merge_df['product_id'].tolist())
        
        output_path = f"s3://{bucket_path}{today_date}/Musinsa/ReviewData/{category3depth}/{category4depth}.parquet"
        
        all_reviews_df_list = []
            
        for product_id in product_ids:
            review_df = et_productreview(product_id)
            print(review_df)
            all_reviews_df_list.append(review_df)
            
        all_reviews_df = pd.concat(all_reviews_df_list)
        all_reviews_df.to_parquet(output_path, storage_options=aws_storage_options)
        
if __name__ == "__main__":
    main()
    
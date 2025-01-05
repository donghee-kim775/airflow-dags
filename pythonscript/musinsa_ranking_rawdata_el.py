import logging
import requests
import os
import json
from s3_validate import connect_s3, validate_and_upload_s3_file

from datetime import datetime, timedelta

import argparse

# s3 connect
aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
region_name='ap-northeast-2'

s3_client = connect_s3(aws_access_key_id, aws_secret_access_key, region_name)

# url
url = "https://api.musinsa.com/api2/hm/v2/pans/ranking/sections/199?"

# headers
headers = {
    "accept": "application/json, text/plain, */*",
    "accept-encoding": "gzip, deflate, br, zstd",
    "accept-language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
    "origin": "https://www.musinsa.com",
    "referer": "https://www.musinsa.com/",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
}

# params
params = {
    "storeCode" : "musinsa",
    "period" : "DAILY",
    "ageBand" : "AGE_BAND_ALL"
}

# today_date
today_date = datetime.now().strftime("%Y-%m-%d")

# mapping table - 2depth 카테고리를 이용해 airflow에서 task를 분기 중
# task 명은 영어로 만들어야 함. 하지만 파일 적재는 한국어로 통일해서 적재 계획 (4depth가 한국어)
# => 2depth 카테고리를 한국어로 바꿔야 함
def mapping_2depth_kor(depth2category):
    if depth2category == 'top':
        return '상의'
    elif depth2category == 'bottom':
        return '하의'
    elif depth2category == 'shoes':
        return '신발'
    elif depth2category == 'outer':
        return '아우터'
    
def main():
    parser = argparse.ArgumentParser(description="sexual/category param")
    parser.add_argument('sexual', type=str, help='sexual')
    parser.add_argument('category_data', type=str, help='category')

    args = parser.parse_args()

    sexual_data = json.loads(args.sexual)
    category_data = json.loads(args.category_data)

    params['gf'] = sexual_data[0]

    category2depth = mapping_2depth_kor(category_data[0])
    
    logging.info(f"Category : {sexual_data[0]}_{category_data[0]} Crawler Start")
   
    for category_info in category_data[1]:
        category3depth = list(category_info.items())[0]
        
        for category4code, category4name in category3depth[1].items():
            params['categoryCode'] = category4code
            response = requests.get(url, headers=headers, params=params)
            
            if response.status_code == 200:
                response_json = response.json()
                logging.info("response 200")
            
            bucket_name = 'project4-raw-data'
            file_name = f"{today_date}/Musinsa/RankingData/{category3depth[0]}/{sexual_data[1]}_{category2depth}_{category3depth[0]}_{category4name}.json"
    
            validate_and_upload_s3_file(s3_client, bucket_name, file_name, response_json)
  
if __name__ == "__main__":
    main()
    
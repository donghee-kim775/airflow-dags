import requests
import pandas as pd

from bs4 import BeautifulSoup

import argparse
import re
import os
from datetime import datetime, timedelta
import json

# 헤더 설정
headers = {
    "accept": "application/json, text/plain, */*",
    "accept-encoding": "gzip, deflate, br, zstd",
    "accept-language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
    "origin": "https://www.musinsa.com",
    "referer": "https://www.musinsa.com/",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "sec-ch-ua": '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"'
}

aws_storage_options = {
    "key" : os.getenv('AWS_ACCESS_KEY_ID'),
    "secret" : os.getenv('AWS_SECRET_ACCESS_KEY')
}

def mapping_2depth_kor(depth2category):
    if depth2category == 'top':
        return '상의'
    elif depth2category == 'bottom':
        return '하의'
    elif depth2category == 'shoes':
        return '신발'
    elif depth2category == 'outer':
        return '아우터'

# parsing 안되는 경우
def get_text_or_none(element):
    return element.text if element else None

def get_content_or_none(element):
    return element['content'] if element else None

# product_id 추출
def get_product_ids(bucket_path, file_key, aws_storage_options):
    file_path = bucket_path + file_key
    df = pd.read_parquet(file_path, storage_options=aws_storage_options)
    return df['product_id'].tolist()

# product detail parsing => DataFrame Record
def et_product_detail(master_category, depth4category, product_id):
    url = f"https://www.musinsa.com/products/{product_id}"
    
    response = requests.get(url, headers=headers)
    
    soup = BeautifulSoup(response.text, features="html.parser")

    title_text = soup.find('title').text
    product_name = re.sub(r' - 사이즈 & 후기.*', '', title_text)

    # brand naeme_kr, brand name_en
    brand_name_kr = soup.find('meta', {'property': 'product:brand'}).get('content')
    brand_name_en = soup.find('div', class_='sc-11x022e-0 hzZrPp')
    brand_name_en = brand_name_en.find('a', href=True)['href'].split('brand/')[1] if brand_name_en else None

    original_price = get_content_or_none(soup.find('meta', {'property': 'product:price:normal_price'}))
    final_price = get_content_or_none(soup.find('meta', {'property': 'product:price:amount'}))
    discount_rate = get_content_or_none(soup.find('meta', {'property': 'product:price:discount_rate'}))

    try:
        # review_count, review_avg_ratin
        json_data = json.loads(soup.find('script', {'type': 'application/ld+json'}).string)
        # ratingValue와 reviewCount 값 추출
        review_count = json_data['aggregateRating']['reviewCount']
        review_avg_rating = json_data['aggregateRating']['ratingValue']
    except:
        review_count = None
        review_avg_rating = None
    
    url = f"https://like.musinsa.com/like/api/v2/liketypes/goods/counts"

    payload = {
        "relationIds": [product_id]
    }
    
    try:
        response = requests.post(url, headers=headers, json=payload).json()

        like_counting = response['data']['contents']['items'][0]['count']
    except:
        like_counting = None
    
    created_at = datetime.now().strftime('%Y-%m-%d')
    # data => dict
    data = {
        "platform": 'Musinsa',
        "master_category": master_category,
        "small_category": depth4category,
        "product_id": product_id,
        "product_name": product_name,
        "brand_name_kr": brand_name_kr,
        "brand_name_en": brand_name_en,
        "original_price": original_price,
        "final_price": final_price,
        "discount_rate": discount_rate,
        "review_count": review_count,
        "review_avg_rating": review_avg_rating,
        "like_counting": like_counting,
        "created_at": created_at,
    }

    return data
    
def main():
    # argment parsing
    parser = argparse.ArgumentParser(description="sexual/category param")
    parser.add_argument('sexual', type=str, help='sexual')
    parser.add_argument('category_data', type=str, help='category')
    
    args = parser.parse_args()

    sexual_data = json.loads(args.sexual)
    category_data = json.loads(args.category_data)
    
    print(sexual_data)
    print(category_data)

    category2depth = mapping_2depth_kor(category_data[0])
    bucket_path = "s3a://project4-silver-data/"
    today_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    
    for category_info in category_data[1]:
        category3depth = list(category_info.items())[0]
        
        for category4depth in category3depth[1].values():
            read_file_path = f"{today_date}/Musinsa/RankingData/{category3depth[0]}/{sexual_data[1]}_{category2depth}_{category3depth[0]}_{category4depth}.parquet"
            product_list = get_product_ids(bucket_path, read_file_path, aws_storage_options)
            
            record_list = []
            
            for product_id in product_list:
                master_category = f"{sexual_data[1]}-{category2depth}-{category3depth[0]}"
                record_dict = et_product_detail(master_category, category4depth, product_id)
                record_list.append(record_dict)
            
            merged_df = pd.DataFrame(record_list)
            write_file_path = f"{today_date}/Musinsa/ProductDetailData/{category3depth[0]}/{category4depth}/{sexual_data[1]}_{category2depth}_{category3depth[0]}_{category4depth}.parquet"
            write_file_path = bucket_path + write_file_path
            merged_df.to_parquet(write_file_path, storage_options=aws_storage_options)

if __name__ == "__main__":
    main()
    
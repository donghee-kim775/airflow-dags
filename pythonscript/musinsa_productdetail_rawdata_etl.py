import pandas as pd

import argparse

from selenium import webdriver
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.firefox.service import Service
from selenium.webdriver import FirefoxOptions

from bs4 import BeautifulSoup

import os
from datetime import datetime, timedelta
import json

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
def et_product_detail_(html, master_category, depth4category, product_id):
    soup = BeautifulSoup(html, 'html')

    product_name = get_text_or_none(soup.find('span', class_='text-lg font-medium break-all flex-1 font-pretendard'))

    brand_name_kr = get_content_or_none(soup.find('meta', {'property': 'product:brand'}))
    brand_name_en = soup.find('div', class_='sc-11x022e-0 hzZrPp')
    brand_name_en = brand_name_en.find('a', href=True)['href'].split('brand/')[1] if brand_name_en else None

    original_price = get_content_or_none(soup.find('meta', {'property': 'product:price:normal_price'}))
    final_price = get_content_or_none(soup.find('meta', {'property': 'product:price:amount'}))
    discount_rate = get_content_or_none(soup.find('meta', {'property': 'product:price:discount_rate'}))

    review_data = json.loads(soup.find('script', {'type': 'application/ld+json'}).string) if soup.find('script', {'type': 'application/ld+json'}) else {}
    review_count = review_data.get('aggregateRating', {}).get('reviewCount', None)
    review_avg_rating = review_data.get('aggregateRating', {}).get('ratingValue', None)

    like_counting = get_text_or_none(soup.select_one('#root > div.sc-1f8zq2z-0.SRIds > div.sc-1wsabwr-0.cytPTm > div > div > span'))
    
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

    # dcit => DataFrame Record
    df = pd.DataFrame([data])
    return df
    
def get_product_detail(product_id, driver):
    url = f"https://www.musinsa.com/products/{producit_id}"
    driver.get(url)

    html = driver.page_source
    df = et_product_detail_(html, master_category, depth4category, product_id)
    
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
    
    # Firefox driver Setting
    opts = FirefoxOptions()
    opts.add_argument("--headless")
    driver = webdriver.Firefox(options=opts)
    
    category2depth = mapping_2depth_kor(category_data[0])
    bucket_path = "s3a://project4-silver-data/"
    today_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    
    for category_info in category_data[1]:
        category3depth = list(category_info.items())[0]
        
        for category4depth in category3depth[1].values():
            file_name = f"{today_date}/Musinsa/RankingData/{category3depth[0]}/{sexual_data[1]}_{category2depth}_{category3depth[0]}_{category4depth}.json"
            product_list = get_product_ids(bucket_path, file_name, aws_storage_options)
            print(f"{sexual_data[1]}_{category2depth}_{category3depth[0]}_{category4depth}")
            print(product_list)
            

if __name__ == "__main__":
    main()
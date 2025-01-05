import pandas as pd

import argparse

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver import FirefoxOptions

from bs4 import BeautifulSoup

import re
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
def et_product_detail(driver, master_category, depth4category, product_id):
    url = f"https://www.musinsa.com/products/{product_id}"
    driver.get(url)

    html = driver.page_source
    
    soup = BeautifulSoup(html, features="html.parser")

    title_text = soup.find('title').text
    product_name = re.sub(r' - 사이즈 & 후기.*', '', title_text)

    brand_name_kr = get_content_or_none(soup.find('meta', {'property': 'product:brand'}))
    brand_name_en = soup.find('div', class_='sc-11x022e-0 hzZrPp')
    brand_name_en = brand_name_en.find('a', href=True)['href'].split('brand/')[1] if brand_name_en else None

    original_price = get_content_or_none(soup.find('meta', {'property': 'product:price:normal_price'}))
    final_price = get_content_or_none(soup.find('meta', {'property': 'product:price:amount'}))
    discount_rate = get_content_or_none(soup.find('meta', {'property': 'product:price:discount_rate'}))

    review_count = driver.find_element(By.XPATH, '/html/body/div[1]/main/div[1]/div[1]/div[14]/div[2]/div[1]/div/div[4]/div[7]/span/text()[1]').text
    review_avg_rating = driver.find_element(By.XPATH, '/html/body/div[1]/main/div[1]/div[1]/div[2]/div[7]/div/span[1]').text
    
    like_counting = driver.find_element(By.XPATH, '//*[@id="root"]/div[1]/div[19]/div/div/span').text
    
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
    # DataFrame 출력 옵션 설정
    pd.set_option('display.max_rows', None)  # 모든 행 표시
    pd.set_option('display.max_columns', None)  # 모든 열 표시
    pd.set_option('display.width', 1000)  # 출력 폭 설정
    
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
            file_name = f"{today_date}/Musinsa/RankingData/{category3depth[0]}/{sexual_data[1]}_{category2depth}_{category3depth[0]}_{category4depth}.parquet"
            product_list = get_product_ids(bucket_path, file_name, aws_storage_options)
            
            record_list = []
            
            for product_id in product_list:
                master_category = f"{sexual_data[1]}-{category2depth}-{category3depth[0]}"
                record_dict = et_product_detail(driver, master_category, category4depth, product_id)
                record_list.append(record_dict)
            
            merged_df = pd.DataFrame(record_list)
            print(merged_df)

if __name__ == "__main__":
    main()
    
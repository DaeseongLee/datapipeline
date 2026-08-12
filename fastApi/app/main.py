import os
import io
import json
from datetime import datetime
from typing import List
from fastapi import FastAPI, BackgroundTasks, HTTPException
import pandas as pd
from minio import Minio
import requests
from bs4 import BeautifulSoup

app = FastAPI(title="Coupang Price Tracker Ingestion Engine")

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD")
# BUCKET_NAME = os.getenv("MINIO_BUCKET_NAME")
SPARK_MASTER_URL = os.getenv("SPARK_MASTER_URL")

# 1. MinIO 클라이언트 설정
minio_client = Minio(
    endpoint=MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False
)

BUCKET_NAME = "raw-coupang-prices"

TARGET_PRODUCTS = [
    {"id": "1271064981", "name": "달걀 30개", "url": "https://www.coupang.com/vp/products/1271064981?vendorItemId=70272921582"},
    {"id": "6854321599", "name": "두부 800g", "url": "https://www.coupang.com/vp/products/6854321599?vendorItemId=83532120765"},
    {"id": "1275124832", "name": "콩나물 500g", "url": "https://www.coupang.com/vp/products/1275124832?vendorItemId=70278175584"}
]

def fetch_coupang_price(product: dict) -> dict:
    """
    쿠팡 상품 페이지에서 가격 데이터를 수집하는 함수
    (실제 구현 시 Playwright/Selenium 등을 활용하면 더 안정적입니다)
    """
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7"
    }
    now = datetime.now()
    
    # 예시: HTTP 요청 (실제 환경에서는 셀레니움/플레이라이트 추천)
    response = requests.get(product["url"], headers=headers)
    soup = BeautifulSoup(response.text, "html.parser")
    price_element = soup.select_one(".final-price > .price-amount")
    if price_element:
        price_text = price_element.text.replace(",", "").replace("원", "")
        price = int(price_text)
    else:
        print("[경고] .final-price > .price-amount 태그를 찾지 못했습니다.")
        price = 0

    # 테스트용 데이터 구조 예시
    return {
        "product_id": product["id"],
        "product_name": product["name"],
        "price": price,               # 수집된 현재가
        "is_out_of_stock": False,      # 품절 여부
        "collected_at": now.strftime("%Y-%m-%d %H:%M:%S"),
        "date": now.strftime("%Y-%m-%d"),
        "hour": now.strftime("%H")
    }

def process_and_upload_to_minio():
    """관심 상품들의 가격을 수집하여 MinIO에 Parquet로 적재"""
    collected_data = []
    
    for prod in TARGET_PRODUCTS:
        try:
            data = fetch_coupang_price(prod)
            if data:
                collected_data.append(data)
        except Exception as e:
            print(f"Error collecting {prod['name']}: {e}")

    if not collected_data:
        return

    # Pandas DataFrame 변환
    df = pd.DataFrame(collected_data)
    
    # Parquet 메모리 버퍼 생성
    parquet_buffer = io.BytesIO()
    df.to_parquet(parquet_buffer, index=False, engine="pyarrow")
    parquet_buffer.seek(0)
    
    # MinIO 저장 경로 (년-월-일/시간 파티셔닝)
    today_str = datetime.now().strftime("%Y-%m-%d")
    hour_str = datetime.now().strftime("%H")
    timestamp = int(datetime.now().timestamp())
    
    object_name = f"year_month_day={today_str}/hour={hour_str}/prices_{timestamp}.parquet"
    
    # MinIO 버킷 확인 및 생성
    if not minio_client.bucket_exists(BUCKET_NAME):
        minio_client.make_bucket(BUCKET_NAME)
        
    # MinIO 적재
    minio_client.put_object(
        bucket_name=BUCKET_NAME,
        object_name=object_name,
        data=parquet_buffer,
        length=len(parquet_buffer.getvalue()),
        content_type="application/octet-stream"
    )
    print(f"[MinIO Ingest Success] Path: {BUCKET_NAME}/{object_name}")

@app.post("/trigger/collect")
def trigger_collection(background_tasks: BackgroundTasks):
    """주기적(예: APScheduler, Airflow) 또는 수동으로 수집을 실행하는 API"""
    background_tasks.add_task(process_and_upload_to_minio)
    return {"message": "Coupang price collection started in background."}
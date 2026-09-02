import os
import io
from datetime import datetime, timedelta
from typing import Optional
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
import httpx
import pandas as pd
from minio import Minio
import anyio

# 환경변수 로드
KAMIS_CERT_KEY = os.getenv("KAMIS_CERT_KEY")
KAMIS_CERT_ID = os.getenv("KAMIS_CERT_ID")
KAMIS_BASE_URL = os.getenv("KAMIS_BASE_URL")  

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD")

BUCKET_NAME = "daily-price-by-category"

minio_client: Optional[Minio] = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """앱 시작 시 MinIO 클라이언트를 초기화하고 버킷을 미리 생성합니다."""
    global minio_client
    minio_client = Minio(
        endpoint=MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False
    )
    if not minio_client.bucket_exists(BUCKET_NAME):
        minio_client.make_bucket(BUCKET_NAME)
    yield


app = FastAPI(title="KAMIS 연동 서비스", lifespan=lifespan)


# 1. BytesIO 스트림 객체와 크기를 직접 받도록 수정
def _upload_to_minio(object_name: str, buffer: io.BytesIO, buffer_size: int):
    """MinIO 업로드를 수행하는 동기 헬퍼 함수 (스트림 기반)"""
    minio_client.put_object(
        bucket_name=BUCKET_NAME,
        object_name=object_name,
        data=buffer,            # BytesIO 스트림 전달
        length=buffer_size,      # 데이터 바이트 크기
        content_type="application/octet-stream"
    )


@app.get("/api/prices/daily")
async def get_daily_price(
    product_cls_code: str = "02",      # 01: 소매, 02: 도매
    item_category_code: str = "100",  # 100: 식량작물, 200: 채소류 등
    country_code: str = "1101",       # 1101: 서울 (지역코드)
    reg_day: Optional[str] = None     # YYYY-MM-DD (미입력 시 어제)
):
    """
    KAMIS 일자별 가격 정보를 조회하여 MinIO에 Parquet으로 적재하는 엔드포인트
    """
    if not reg_day:
        yesterday = datetime.now() - timedelta(days=1)
        reg_day = yesterday.strftime("%Y-%m-%d")

    params = {
        "action": "dailyPriceByCategoryList",
        "p_cert_key": KAMIS_CERT_KEY,
        "p_cert_id": KAMIS_CERT_ID,
        "p_returntype": "json",
        "p_product_cls_code": product_cls_code,
        "p_item_category_code": item_category_code,
        "p_country_code": country_code,
        "p_regday": reg_day,
        "p_convert_kg_yn": "N"
    }

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }

    now = datetime.now()
    timestamp = int(now.timestamp())
    object_name = f"reg_date={reg_day}/hour={now.strftime('%H')}/prices_{item_category_code}_{timestamp}.parquet"

    async with httpx.AsyncClient(headers=headers, follow_redirects=True) as client:
        try:
            response = await client.get(KAMIS_BASE_URL, params=params, timeout=10.0)
            
            if response.status_code != 200:
                raise HTTPException(
                    status_code=response.status_code, 
                    detail="KAMIS API 서버 응답 에러"
                )

            response_body = response.json()

            if not isinstance(response_body, dict):
                raise HTTPException(
                    status_code=400,
                    detail=f"KAMIS API 응답 형식이 올바르지 않습니다: {response_body}"
                )

            data_field = response_body.get("data")
            items = []

            if isinstance(data_field, dict):
                items = data_field.get("item", [])
            elif isinstance(data_field, list) and len(data_field) > 0:
                if isinstance(data_field[0], dict):
                    items = data_field[0].get("item", [])

            if not items:
                return {
                    "message": "조회된 데이터가 없거나 휴무일입니다.", 
                    "reg_day": reg_day,
                    "raw_response": response_body
                }

            # DataFrame 변환 및 Parquet 메모리 버퍼 생성
            df = pd.DataFrame(items)
            parquet_buffer = io.BytesIO()
            df.to_parquet(parquet_buffer, index=False, engine="pyarrow")
            
            # 2. 바이트 크기 측정 후 포인터(커서)를 맨 앞으로 이동
            buffer_size = parquet_buffer.tell()
            parquet_buffer.seek(0)

            # 3. parquet_bytes 대신 parquet_buffer와 buffer_size 전달
            try:
                await anyio.to_thread.run_sync(
                    _upload_to_minio, object_name, parquet_buffer, buffer_size
                )
            except Exception as e:
                raise HTTPException(
                    status_code=500,
                    detail=f"MinIO 저장 실패: {str(e)}"
                )

            return {
                "status": "success",
                "bucket": BUCKET_NAME,
                "path": object_name,
                "rows_ingested": len(df)
            }

        except httpx.RequestError as exc:
            raise HTTPException(
                status_code=500, 
                detail=f"KAMIS API 호출 중 네트워크 에러 발생: {exc}"
            )
        
def _read_parquet_from_minio(object_name: str) -> pd.DataFrame:
    response = minio_client.get_object(BUCKET_NAME, object_name)
    try:
        data_bytes = response.read()
        # 바이너리 Parquet 데이터를 Pandas DataFrame으로 로드
        df = pd.read_parquet(io.BytesIO(data_bytes))
        return df
    finally:
        response.close()
        response.release_conn()


@app.get("/api/prices/daily/minio")
async def get_minio_object(object_name: str):
    """
    MinIO에 저장된 Parquet 파일 경로(object_name)를 받아 데이터를 조회하는 엔드포인트
    예시 object_name: reg_date=2026-09-01/hour=18/prices_100_1725264000.parquet
    """
    try:
        # 비동기 스레드 풀에서 MinIO 읽기 실행
        df = await anyio.to_thread.run_sync(_read_parquet_from_minio, object_name)
        
        # DataFrame을 Dict/JSON 형태로 변환하여 반환
        return {
            "status": "success",
            "object_name": object_name,
            "total_count": len(df),
            "data": df.to_dict(orient="records")
        }
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"MinIO 데이터 읽기 실패: {str(e)}"
        )
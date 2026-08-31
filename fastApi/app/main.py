import os
from fastapi import FastAPI, HTTPException
import httpx

app = FastAPI(title="KAMIS 연동 서비스")

KAMIS_CERT_KEY = os.getenv("KAMIS_CERT_KEY")
KAMIS_CERT_ID = os.getenv("KAMIS_CERT_ID")
KAMIS_BASE_URL = os.getenv("KAMIS_BASE_URL")  


@app.get("/api/prices/daily")
async def get_daily_price(
    product_cls_code: str = "02",  # 01: 소매, 02: 도매
    item_category_code: str = "100",  # 100: 식량작물, 200: 채소류 등
    country_code: str = "1101",  # 1101: 서울 (지역코드)
):
    """
    KAMIS 일자별 가격 정보를 조회하는 FastAPI 엔드포인트
    """
    # 1. KAMIS API에 전달할 필수/선택 파라미터 정의
    params = {
        "action":"dailyPriceByCategoryList",
        "p_cert_key": KAMIS_CERT_KEY,
        "p_cert_id": KAMIS_CERT_ID,
        "p_returntype": "json",  # 반환 형식 (json 또는 xml)
        "p_product_cls_code": product_cls_code,
        "p_item_category_code": item_category_code,
        "p_country_code": country_code,
        "p_regday": "2026-08-30",
        "p_convert_kg_yn": "N"
    }

    # 2. httpx 비동기 클라이언트를 이용해 KAMIS 서버로 요청 보내기
    async with httpx.AsyncClient() as client:
        try:
            response = await client.get(KAMIS_BASE_URL, params=params, timeout=10.0)
            
            # 응답 상태 코드 확인
            if response.status_code != 200:
                raise HTTPException(
                    status_code=response.status_code, 
                    detail="KAMIS API 서버 응답 에러"
                )

            # JSON 변환
            data = response.json()
            return data

        except httpx.RequestError as exc:
            raise HTTPException(
                status_code=500, 
                detail=f"KAMIS API 호출 중 네트워크 에러 발생: {exc}"
            )
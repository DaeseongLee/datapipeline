import os
from fastapi import FastAPI, UploadFile, File, HTTPException

from fastapi.responses import StreamingResponse
from minio import Minio
from minio.error import S3Error

from pyspark.sql import SparkSession
from contextlib import asynccontextmanager

from datetime import timedelta
import io

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD")
BUCKET_NAME = os.getenv("MINIO_BUCKET_NAME")
SPARK_MASTER_URL = os.getenv("SPARK_MASTER_URL")

minio_client = Minio(
    endpoint=MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False  # HTTPS를 사용하는 경우 True로 변경
)

spark_session = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    global spark_session

    try:
        found = minio_client.bucket_exists(BUCKET_NAME)
        if not found:
            minio_client.make_bucket(BUCKET_NAME)
            print(f"Bucket '{BUCKET_NAME}' created successfully.")
        else:
            print(f"Bucket '{BUCKET_NAME}' already exists.")
    except S3Error as e:
        print(f"Error connecting to MinIO: {e}")

    try:
        spark_session = SparkSession.builder \
            .appName("FastAPI-Spark-Pipeline") \
            .master(SPARK_MASTER_URL) \
            .getOrCreate()
        print(f"⚡ Connected to Spark Master: {SPARK_MASTER_URL}")
    except Exception as e:
        print(f"❌ Failed to connect Spark Master: {e}")

    yield  # 앱 작동 중

    # [3] 서버 종료 시 SparkSession 정리
    if spark_session:
        spark_session.stop()
        print("🛑 Spark Session Stopped.")

app = FastAPI(title="FastAPI MinIO & Spark Integration", lifespan=lifespan)


# 2. fast-api health 체크
@app.get("/health")
def hello():
    return {"health": 'OK'}

# 3. 파일 업로드 API
@app.post("/upload")
async def upload_file(file: UploadFile = File(...)):
    try:
        # 파일 데이터를 메모리에 읽기
        contents = await file.read()
        file_size = len(contents)
        data_stream = io.BytesIO(contents)

        content_type = file.content_type
        if content_type == "text/plain":
            content_type = "text/plain; charset=utf-8"

        # MinIO에 파일 업로드 (put_object)
        minio_client.put_object(
            bucket_name=BUCKET_NAME,
            object_name=file.filename,
            data=data_stream,
            length=file_size,
            content_type=content_type
        )

        return {
            "message": "File uploaded successfully",
            "filename": file.filename,
            "size": file_size
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Upload failed: {str(e)}")


# 4. 파일 다운로드 API (스트리밍 반환)
@app.get("/download/{filename}")
def download_file(filename: str):
    try:
        # MinIO에서 객체 가져오기 (get_object)
        response = minio_client.get_object(BUCKET_NAME, filename)
        
        # FastAPI StreamingResponse로 파일 스트리밍 반환
        return StreamingResponse(
            response.stream(32 * 1024),  # 32KB 청크 단위
            media_type="application/octet-stream",
            headers={"Content-Disposition": f"attachment; filename={filename}"}
        )
    except S3Error as e:
        raise HTTPException(status_code=404, detail="File not found")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# 5. 임시 다운로드 URL 생성 API (Presigned URL)
# 프론트엔드에서 S3/MinIO에 직접 접근하여 다운로드할 수 있는 1회성/기간제 URL
@app.get("/url/{filename}")
def get_presigned_url(filename: str):
    try:
        # 1시간(3600초) 동안 유효한 Presigned URL 생성
        url = minio_client.presigned_get_object(
            bucket_name=BUCKET_NAME,
            object_name=filename,
            expires=timedelta(hours=1),
            response_headers={
                "response-content-type": "text/plain; charset=utf-8"
            }
        )

        return {"url": url}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/spark-check")
def check_spark():
    if not spark_session:  # 👈 세션 연결 확인 예외 처리
        raise HTTPException(status_code=503, detail="Spark session is not connected")
    
    # 간단한 분산 데이터 처리 테스트
    data = [("FastAPI", 100), ("Spark", 200), ("MinIO", 300)]
    df = spark_session.createDataFrame(data, ["Name", "Value"])
    
    total_count = df.count()
    result = [row.asDict() for row in df.collect()]
    
    return {
        "status": "success",
        "total_rows": total_count,
        "data": result
    }

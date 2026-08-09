from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import StreamingResponse
from minio import Minio
from minio.error import S3Error
from datetime import timedelta
import io

app = FastAPI(title="FastAPI MinIO Integration")

# 1. MinIO 클라이언트 설정
# (실무에서는 os.getenv() 등을 사용하여 환경변수로 관리하는 것이 좋습니다)
MINIO_ENDPOINT = "minio:9000"  # MinIO 서버 주소 (http:// 제외)
LOCALHOST_ENDPOINT = "localhost:9000"
MINIO_ACCESS_KEY = "admin"     # Access Key
MINIO_SECRET_KEY = "password1234"     # Secret Key
BUCKET_NAME = "my-bucket"

minio_client = Minio(
    endpoint=MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False  # HTTPS를 사용하는 경우 True로 변경
)


# 2) Presigned URL 생성전용 (브라우저 접근용 localhost:9000 서명 계산)
# presigned_client = Minio(
#     endpoint=LOCALHOST_ENDPOINT,
#     access_key=MINIO_ACCESS_KEY,
#     secret_key=MINIO_SECRET_KEY,
#     region="us-east-1",
#     secure=False
# )

@app.get("/health")
def hello():
    return {"health": 'OK'}

# 2. 서버 시작 시 버킷 자동 생성 확인 (Startup Event)
@app.on_event("startup")
def startup_event():
    try:
        found = minio_client.bucket_exists(BUCKET_NAME)
        if not found:
            minio_client.make_bucket(BUCKET_NAME)
            print(f"Bucket '{BUCKET_NAME}' created successfully.")
        else:
            print(f"Bucket '{BUCKET_NAME}' already exists.")
    except S3Error as e:
        print(f"Error connecting to MinIO: {e}")


# 3. 파일 업로드 API
@app.post("/upload")
async def upload_file(file: UploadFile = File(...)):
    try:
        # 파일 데이터를 메모리에 읽기
        contents = await file.read()
        file_size = len(contents)
        data_stream = io.BytesIO(contents)

        # MinIO에 파일 업로드 (put_object)
        minio_client.put_object(
            bucket_name=BUCKET_NAME,
            object_name=file.filename,
            data=data_stream,
            length=file_size,
            content_type=file.content_type
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
            expires=timedelta(hours=1)
        )

        return {"url": url}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
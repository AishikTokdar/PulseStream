import io
from typing import Optional
from minio import Minio
from minio.error import S3Error

from app.core.config import settings


class MinIOClient:
    def __init__(self) -> None:
        self.client = Minio(
            endpoint=settings.MINIO_ENDPOINT,
            access_key=settings.MINIO_ACCESS_KEY,
            secret_key=settings.MINIO_SECRET_KEY,
            secure=settings.MINIO_SECURE,
        )
        self.bucket_name = settings.MINIO_BUCKET_NAME

    def ensure_bucket_exists(self) -> None:
        try:
            if not self.client.bucket_exists(self.bucket_name):
                self.client.make_bucket(self.bucket_name)
        except Exception as e:
            print(f"MinIO bucket initialization error/warning: {e}")

    def upload_file_bytes(
        self, object_name: str, data: bytes, content_type: str = "application/json"
    ) -> str:
        self.ensure_bucket_exists()
        data_stream = io.BytesIO(data)
        data_len = len(data)
        self.client.put_object(
            bucket_name=self.bucket_name,
            object_name=object_name,
            data=data_stream,
            length=data_len,
            content_type=content_type,
        )
        return f"{settings.MINIO_ENDPOINT}/{self.bucket_name}/{object_name}"

    def get_file_bytes(self, object_name: str) -> Optional[bytes]:
        try:
            response = self.client.get_object(self.bucket_name, object_name)
            return response.read()
        except S3Error:
            return None
        finally:
            response.close()
            response.release_conn()


minio_client = MinIOClient()

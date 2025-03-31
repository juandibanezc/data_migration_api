import os
from dotenv import load_dotenv
from pydantic_settings import BaseSettings

load_dotenv()

class Settings(BaseSettings):
    PROJECT_NAME: str = "FastAPI Data Migration"
    API_VERSION: str = "v1"
    
    # Database Config
    DATABASE_URL: str = os.getenv("DATABASE_URL")

    # AWS S3 Config
    S3_REGION: str = os.getenv("S3_REGION")
    S3_BUCKET_NAME: str = os.getenv("S3_BUCKET_NAME")
    AWS_ACCESS_KEY_ID: str = os.getenv("AWS_ACCESS_KEY_ID")
    AWS_SECRET_ACCESS_KEY: str = os.getenv("AWS_SECRET_ACCESS_KEY")
    
    # API Auth
    API_KEY: str = os.getenv("API_KEY")

    class Config:
        case_sensitive = True

settings = Settings()
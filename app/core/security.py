from fastapi import HTTPException, Security
from fastapi.security.api_key import APIKeyHeader
from app.core.config import settings
from typing import Annotated

API_KEY_NAME = "X-API-KEY"

api_key_header = APIKeyHeader(name=API_KEY_NAME, auto_error=True)

def get_api_key(api_key: str = Security(api_key_header)) -> str:
  """
  Validate the provided API key against the configured API key.

  Args:
    api_key (str): The API key provided in the request header.

  Returns:
    str: The validated API key.

  Raises:
    HTTPException: If the provided API key is invalid.
  """
  if api_key != settings.API_KEY:
    raise HTTPException(status_code=403, detail="Invalid API Key")
  return api_key

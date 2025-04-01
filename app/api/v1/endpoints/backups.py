import traceback

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from app.core.database import get_db
from app.core.security import get_api_key
from app.services.backup_service import backup_database, restore_table
from app.core.logging_config import logger

router = APIRouter()

@router.post("/create_backup")
def backup_endpoint(db: Session = Depends(get_db), api_key: str = Depends(get_api_key)):
    """
    API Endpoint to create backup files of the database tables in Avro format.
    """
    return backup_database(db)

@router.post("/restore/{table_name}")
def restore_table_endpoint(table_name: str, db: Session = Depends(get_db), api_key: str = Depends(get_api_key)):
    """
    API Endpoint to restore a table from its backup.
    """
    try:
        return restore_table(db, table_name)
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail="Internal Server Error")

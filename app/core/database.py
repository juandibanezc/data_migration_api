from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
from typing import Generator
from app.core.config import settings

# Create the SQLAlchemy engine
engine = create_engine(settings.DATABASE_URL, pool_size=10, max_overflow=20)

# Create a session factory
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Base class for ORM models
Base = declarative_base()

def get_db() -> Generator[Session]:
  """
  Dependency function for getting a database session.

  Yields:
    Session: A SQLAlchemy database session.
  """
  db = SessionLocal()
  try:
    yield db
  finally:
    db.close()

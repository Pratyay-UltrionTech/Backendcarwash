import sys
import os
from pathlib import Path

# Add backend to sys.path
sys.path.append(str(Path.cwd()))

from app.config import Settings

# Mock env
os.environ["ADMIN_ID"] = "test@example.com"
os.environ["ADMIN_PASSWORD"] = "test"
os.environ["POSTGRES_HOST"] = "postgresql-db-dev-1.postgres.database.azure.com"
os.environ["DATABASE_URL"] = "sqlite:///./sql_app.db"

s = Settings()
print(f"URL: {s.sqlalchemy_database_uri()}")

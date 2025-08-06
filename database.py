from sqlalchemy import create_engine, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import os
from urllib.parse import quote_plus

# Database configuration
DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    # Fallback para desarrollo local
    DATABASE_URL = "sqlite:///./insightgrid.db"
    print("⚠️ Using SQLite fallback database")
else:
    print("✅ Using PostgreSQL database from environment")

# Handle Railway PostgreSQL URL format
if DATABASE_URL.startswith("postgresql://"):
    DATABASE_URL = DATABASE_URL.replace("postgresql://", "postgresql+psycopg2://", 1)

# Create engine with appropriate settings
if DATABASE_URL.startswith("sqlite"):
    engine = create_engine(
        DATABASE_URL,
        connect_args={"check_same_thread": False},
        echo=False  # Set to True for SQL debugging
    )
else:
    engine = create_engine(
        DATABASE_URL,
        echo=False,  # Set to True for SQL debugging
        pool_pre_ping=True,
        pool_recycle=300,
        connect_args={
            "sslmode": "require",
            "connect_timeout": 10,
        } if "railway" in DATABASE_URL else {}
    )

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

def get_db():
    """Dependency to get database session"""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def check_db_health():
    """Check if database is accessible"""
    try:
        with engine.connect() as connection:
            connection.execute(text("SELECT 1"))
        return True
    except Exception as e:
        print(f"❌ Database health check failed: {str(e)}")
        return False

def init_db():
    """Initialize database with retries"""
    max_retries = 3
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            print(f"🔄 Database initialization attempt {retry_count + 1}/{max_retries}")
            
            # Test connection first
            with engine.connect() as connection:
                connection.execute(text("SELECT 1"))
            print("✅ Database connection successful")
            
            # Import models to ensure they're registered
            from models import User, Company, Tool, ProcessedFile
            
            # Create all tables
            Base.metadata.create_all(bind=engine)
            print("✅ Database tables created/verified")
            
            # Add new columns if they don't exist (for existing databases)
            try:
                with engine.connect() as connection:
                    # Check if guide_pdf columns exist
                    if DATABASE_URL.startswith("sqlite"):
                        # SQLite approach
                        result = connection.execute(text("PRAGMA table_info(tools)"))
                        columns = [row[1] for row in result.fetchall()]
                        
                        if 'guide_pdf' not in columns:
                            print("🔄 Adding guide_pdf column to tools table...")
                            connection.execute(text("ALTER TABLE tools ADD COLUMN guide_pdf BLOB"))
                            connection.commit()
                            
                        if 'guide_pdf_filename' not in columns:
                            print("🔄 Adding guide_pdf_filename column to tools table...")
                            connection.execute(text("ALTER TABLE tools ADD COLUMN guide_pdf_filename VARCHAR(255)"))
                            connection.commit()
                    else:
                        # PostgreSQL approach
                        result = connection.execute(text("""
                            SELECT column_name 
                            FROM information_schema.columns 
                            WHERE table_name = 'tools'
                        """))
                        columns = [row[0] for row in result.fetchall()]
                        
                        if 'guide_pdf' not in columns:
                            print("🔄 Adding guide_pdf column to tools table...")
                            connection.execute(text("ALTER TABLE tools ADD COLUMN guide_pdf BYTEA"))
                            connection.commit()
                            
                        if 'guide_pdf_filename' not in columns:
                            print("🔄 Adding guide_pdf_filename column to tools table...")
                            connection.execute(text("ALTER TABLE tools ADD COLUMN guide_pdf_filename VARCHAR(255)"))
                            connection.commit()
                            
                    print("✅ Database schema updated successfully")
                    
            except Exception as e:
                print(f"⚠️ Schema update warning: {str(e)} (this is normal for new databases)")
            
            return True
            
        except Exception as e:
            retry_count += 1
            print(f"❌ Database initialization failed (attempt {retry_count}): {str(e)}")
            
            if retry_count >= max_retries:
                print(f"❌ Database initialization failed after {max_retries} attempts")
                return False
            
            import time
            time.sleep(2 ** retry_count)  # Exponential backoff
    
    return False

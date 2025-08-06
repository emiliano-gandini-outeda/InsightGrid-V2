from sqlalchemy import Column, Integer, String, Boolean, DateTime, ForeignKey, LargeBinary, Table, Text, JSON
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from database import Base
import hashlib
from datetime import datetime

# Tabla de asociación Many-to-Many entre User y Company
user_company_association = Table(
    'user_company',
    Base.metadata,
    Column('user_id', Integer, ForeignKey('users.id'), primary_key=True),
    Column('company_id', Integer, ForeignKey('companies.id'), primary_key=True)
)

# Tabla de asociación Many-to-Many entre herramientas de vinculación y herramientas de procesamiento
linking_tool_association = Table(
    'linking_tool_processing_tools',
    Base.metadata,
    Column('linking_tool_id', Integer, ForeignKey('tools.id'), primary_key=True),
    Column('processing_tool_id', Integer, ForeignKey('tools.id'), primary_key=True)
)

class User(Base):
    __tablename__ = "users"
    
    id = Column(Integer, primary_key=True, index=True)
    username = Column(String(100), unique=True, index=True, nullable=False)
    email = Column(String(255), unique=True, index=True, nullable=False)
    password_hash = Column(String(255), nullable=True)  # Nullable para usuarios SSO
    is_admin = Column(Boolean, default=False, nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    
    # Relaciones
    companies = relationship("Company", secondary=user_company_association, back_populates="users")
    processed_files = relationship("ProcessedFile", back_populates="user", cascade="all, delete-orphan")
    
    def set_password(self, password: str):
        """Hashear y establecer contraseña"""
        self.password_hash = hashlib.sha256(password.encode()).hexdigest()
    
    def check_password(self, password: str) -> bool:
        """Verificar contraseña"""
        if not self.password_hash:
            return False
        return self.password_hash == hashlib.sha256(password.encode()).hexdigest()
    
    @classmethod
    def create_from_sso(cls, email: str, name: str = None):
        """Crear usuario desde SSO"""
        username = email.split('@')[0] if not name else name.replace(' ', '_').lower()
        return cls(
            username=username,
            email=email,
            is_admin=False
        )

class Company(Base):
    __tablename__ = "companies"
    
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(255), nullable=False)  # Nombre display
    folder_name = Column(String(100), unique=True, nullable=False)  # Nombre de carpeta física
    description = Column(Text, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    
    # Relaciones
    users = relationship("User", secondary=user_company_association, back_populates="companies")
    tools = relationship("Tool", back_populates="company", cascade="all, delete-orphan")

class Tool(Base):
    __tablename__ = "tools"
    
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(255), nullable=False)  # Nombre display
    filename = Column(String(255), nullable=False)  # Archivo físico
    description = Column(Text, nullable=True)
    company_id = Column(Integer, ForeignKey("companies.id"), nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    
    # Nuevos campos para herramientas de vinculación
    tool_type = Column(String(50), nullable=False, default="procesamiento")  # "procesamiento" o "vinculacion"
    total_files = Column(Integer, nullable=True)  # Número total de archivos que acepta (2-6)
    file_config = Column(JSON, nullable=True)  # Configuración de archivos: qué posiciones están vinculadas
    
    # NUEVO: Campo para PDF de guía
    guide_pdf = Column(LargeBinary, nullable=True)  # PDF de guía almacenado en BD
    guide_pdf_filename = Column(String(255), nullable=True)  # Nombre original del PDF
    
    # Relaciones
    company = relationship("Company", back_populates="tools")
    processed_files = relationship("ProcessedFile", back_populates="tool", cascade="all, delete-orphan")
    
    # Relación Many-to-Many para herramientas de vinculación
    linked_processing_tools = relationship(
        "Tool",
        secondary=linking_tool_association,
        primaryjoin=id == linking_tool_association.c.linking_tool_id,
        secondaryjoin=id == linking_tool_association.c.processing_tool_id,
        back_populates="linking_tools"
    )
    
    linking_tools = relationship(
        "Tool",
        secondary=linking_tool_association,
        primaryjoin=id == linking_tool_association.c.processing_tool_id,
        secondaryjoin=id == linking_tool_association.c.linking_tool_id,
        back_populates="linked_processing_tools"
    )

class ProcessedFile(Base):
    __tablename__ = "processed_files"
    
    id = Column(Integer, primary_key=True, index=True)
    original_filename = Column(String(255), nullable=False)
    processed_filename = Column(String(255), nullable=False)
    file_data = Column(LargeBinary, nullable=False)  # Archivo procesado en BD
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    tool_id = Column(Integer, ForeignKey("tools.id"), nullable=False)
    processed_at = Column(DateTime(timezone=True), server_default=func.now())
    file_size = Column(Integer, nullable=False)
    
    # Nuevos campos para herramientas de vinculación
    input_files_info = Column(JSON, nullable=True)  # Información de archivos de entrada para herramientas de vinculación
    
    # Relaciones
    user = relationship("User", back_populates="processed_files")
    tool = relationship("Tool", back_populates="processed_files")

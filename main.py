from fastapi import FastAPI, Request, Depends, HTTPException, UploadFile, File, Form
from fastapi.responses import HTMLResponse, RedirectResponse, Response, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.middleware.sessions import SessionMiddleware
from sqlalchemy.orm import Session
from database import get_db, init_db, check_db_health, engine
from models import User, Company, Tool, ProcessedFile
import os
import tempfile
import importlib.util
import sys
from datetime import datetime
import json
from typing import List, Dict, Optional
import importlib

# Import admin routes
from admin_routes import router as admin_router
from auth.sso import router as sso_router

# Dynamic import system with error handling
MODULE_PREFIX_A = os.environ.get("MODULE_PREFIX_A")

# Dynamic import system with error handling
MODULE_PREFIX_A = os.environ.get("MODULE_PREFIX_A")
print(f"🔍 DEBUG: MODULE_PREFIX_A = {MODULE_PREFIX_A}")

def import_process_file(module_name):
    try:
        if not MODULE_PREFIX_A:
            raise ImportError(f"MODULE_PREFIX_A environment variable not set")
        full_module = f"{MODULE_PREFIX_A}.{module_name}"
        print(f"🔍 DEBUG: Importing from: {full_module}")
        module = importlib.import_module(full_module)
        print(f"🔍 DEBUG: Module imported successfully: {module}")
        processor = getattr(module, "process_file")
        print(f"🔍 DEBUG: Processor function found: {processor}")
        return processor
    except Exception as e:
        print(f"⚠️ Warning: Could not import {module_name}: {str(e)}")
        import traceback
        traceback.print_exc()
        return None

def import_process_file(module_name):
    try:
        if not MODULE_PREFIX_A:
            raise ImportError(f"MODULE_PREFIX_A environment variable not set")
        return getattr(importlib.import_module(f"{MODULE_PREFIX_A}.{module_name}"), "process_file")
    except Exception as e:
        print(f"⚠️ Warning: Could not import {module_name}: {str(e)}")
        return None

def import_process_multiple_files(module_name):
    try:
        if not MODULE_PREFIX_A:
            raise ImportError(f"MODULE_PREFIX_A environment variable not set")
        return getattr(importlib.import_module(f"{MODULE_PREFIX_A}.{module_name}"), "process_files")
    except Exception as e:
        print(f"⚠️ Warning: Could not import {module_name}: {str(e)}")
        return None

# Lazy-loaded processors - only import when needed
_processors_cache = {}

def get_processor(processor_name, module_name, is_multiple=False):
    """Get processor function with lazy loading and caching"""
    if processor_name not in _processors_cache:
        try:
            if is_multiple:
                _processors_cache[processor_name] = import_process_multiple_files(module_name)
            else:
                _processors_cache[processor_name] = import_process_file(module_name)
        except Exception as e:
            print(f"❌ Error loading processor {processor_name}: {str(e)}")
            _processors_cache[processor_name] = None
    
    return _processors_cache[processor_name]

# Initialize processors with lazy loading
def get_process_balance():
    return get_processor("balance", "balance_proyectado")

def get_process_facturacion():
    return get_processor("facturacion", "facturacion")

def get_process_inventario():
    return get_processor("inventario", "inventario")

def get_process_ventas():
    return get_processor("ventas", "ventas")

def get_process_ventas_csv():
    return get_processor("ventas_csv", "ventas-csv")

def get_process_lista_precios():
    return get_processor("lista_precios", "lista_precios")

def get_process_cruce_ventas():
    return get_processor("cruce_ventas", "cruce_ventas", is_multiple=True)

def get_process_vendedores():
    return get_processor("vendedores", "vendedores")

def get_process_vendedor_vinculado():
    return get_processor("vendedor_vinculado", "vendedor_vinculado", is_multiple=True)

def get_process_utilidades():
    return get_processor("utilidades", "utilidades")

from contextlib import asynccontextmanager
import uvicorn

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    print("🚀 Starting EGO Project...")

    # Initialize database with retries
    try:
        print("🔄 Initializing database...")
        if init_db():
            print("✅ Database initialized successfully")
            try:
                await create_initial_data()
                print("✅ Initial data created successfully")
            except Exception as e:
                print(f"⚠️ Initial data creation failed: {str(e)}, but continuing...")
            print("✅ Application started successfully")
        else:
            print("⚠️ Database initialization failed, but continuing in degraded mode...")
            print("⚠️ Some features may not work properly")
    except Exception as e:
        print(f"⚠️ Startup error: {str(e)}")
        print("⚠️ Application starting in degraded mode...")

    # Ensure required directories exist
    try:
        for directory in ["uploads", "downloads", "static", "templates"]:
            os.makedirs(directory, exist_ok=True)
        print("✅ Required directories verified")
    except Exception as e:
        print(f"⚠️ Directory creation error: {str(e)}")

    yield

    # Shutdown
    print("🛑 Shutting down EGO Project...")
    try:
        # Close database connections
        engine.dispose()
        print("✅ Database connections closed")
    except Exception as e:
        print(f"⚠️ Shutdown error: {str(e)}")
    print("✅ Shutdown completed")

app = FastAPI(
    title="InsightGrid - EGO Project",
    description="Sistema de procesamiento de datos empresariales",
    version="1.0.0",
    lifespan=lifespan
)

# Secret key for sessions - change in production
SECRET_KEY = os.getenv("SECRET_KEY")
app.add_middleware(SessionMiddleware, secret_key=SECRET_KEY)

# Include routers
app.include_router(sso_router, prefix="/sso", tags=["Authentication"])
app.include_router(admin_router, prefix="/admin", tags=["Administration"])

# Create necessary directories
os.makedirs("uploads", exist_ok=True)
os.makedirs("downloads", exist_ok=True)
os.makedirs("static", exist_ok=True)
os.makedirs("templates", exist_ok=True)

# Mount static files
app.mount("/static", StaticFiles(directory="static"), name="static")

# Templates
templates = Jinja2Templates(directory="templates")

PROCESSORS = {
    "balance-proyectado": lambda: get_process_balance(),
    "facturacion": lambda: get_process_facturacion(),
    "inventario": lambda: get_process_inventario(),
    "ventas": lambda: get_process_ventas(),
    "ventas-csv": lambda: get_process_ventas_csv(),
    "lista-precios": lambda: get_process_lista_precios(),
    "cruce-ventas": lambda: get_process_cruce_ventas(),
    "vendedores": lambda: get_process_vendedores(),
    "vendedor-vinculado": lambda: get_process_vendedor_vinculado(),
    "utilidades": lambda: get_process_utilidades()
}

async def create_initial_data():
    """Crear datos iniciales en la base de datos"""
    from database import SessionLocal
    try:
        db = SessionLocal()
        try:
            # Crear usuario admin por defecto
            admin_user = db.query(User).filter(User.username == "emiliano_admin").first()
            if not admin_user:
                admin_user = User(
                    username="emiliano_admin",
                    email="emiliano.outeda@gmail.com",
                    is_admin=True
                )
                admin_user.set_password("admin123")
                db.add(admin_user)
                db.commit()
                print("✅ Admin user created")
        finally:
            db.close()
    except Exception as e:
        print(f"❌ Error creating initial data: {str(e)}")

@app.get("/health")
async def health_check():
    """Health check endpoint for Railway"""
    health_status = {
        "status": "healthy",
        "service": "ego-project", 
        "version": "1.0.0",
        "timestamp": datetime.utcnow().isoformat(),
        "checks": {}
    }

    try:
        # Check database health
        try:
            db_healthy = check_db_health()
            health_status["checks"]["database"] = {
                "status": "healthy" if db_healthy else "unhealthy",
                "message": "Database connection successful" if db_healthy else "Database connection failed"
            }
        except Exception as e:
            health_status["checks"]["database"] = {
                "status": "unhealthy",
                "message": f"Database check error: {str(e)}"
            }
            db_healthy = False
    
        # Check file system
        try:
            # Test if we can write to uploads directory
            test_file = "uploads/.health_check"
            with open(test_file, 'w') as f:
                f.write("health_check")
            os.remove(test_file)
            health_status["checks"]["filesystem"] = {
                "status": "healthy",
                "message": "File system accessible"
            }
            fs_healthy = True
        except Exception as e:
            health_status["checks"]["filesystem"] = {
                "status": "unhealthy", 
                "message": f"File system error: {str(e)}"
            }
            fs_healthy = False
    
        # Overall status
        if db_healthy and fs_healthy:
            health_status["status"] = "healthy"
        elif db_healthy or fs_healthy:
            health_status["status"] = "degraded"
        else:
            health_status["status"] = "unhealthy"
        
        # Return appropriate HTTP status
        status_code = 200 if health_status["status"] in ["healthy", "degraded"] else 503
    
        return JSONResponse(
            content=health_status,
            status_code=status_code
        )
    
    except Exception as e:
        print(f"❌ Health check failed with exception: {str(e)}")
        return JSONResponse(
            content={
                "status": "unhealthy",
                "service": "ego-project",
                "error": f"Health check exception: {str(e)}",
                "version": "1.0.0",
                "timestamp": datetime.utcnow().isoformat()
            },
            status_code=503
        )

@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    """Página principal del dashboard"""
    user = request.session.get("user")
    return templates.TemplateResponse("dashboard.html", {"request": request, "user": user})

@app.get("/api/user/companies")
async def get_user_companies(request: Request, db: Session = Depends(get_db)):
    """Obtener empresas y herramientas del usuario autenticado"""
    try:
        user_session = request.session.get("user")
        if not user_session:
            raise HTTPException(status_code=401, detail="No autorizado")
    
        # Obtener usuario de la base de datos
        user = db.query(User).filter(User.email == user_session["email"]).first()
        if not user:
            # Crear usuario si no existe (desde SSO)
            user = User(
                username=user_session.get("name", user_session["email"].split('@')[0]),
                email=user_session["email"],
                is_admin=False
            )
            db.add(user)
            db.commit()
            db.refresh(user)
            print(f"✅ New user created: {user.email}")
    
        # Verificar permisos
        if user.is_admin:
            companies = db.query(Company).all()
            print(f"👑 Admin user - returning all {len(companies)} companies")
        else:
            companies = user.companies
            print(f"👤 Regular user - {len(companies)} companies assigned")
    
        # Mapear datos para el frontend
        companies_data = []
        for company in companies:
            tool_mapping = {
                "balance_proyectado.py": "balance-proyectado",
                "facturacion.py": "facturacion", 
                "inventario.py": "inventario",
                "ventas.py": "ventas",
                "ventas-csv.py": "ventas-csv",  # ← Asegúrate que esto esté así
                "lista_precios.py": "lista-precios",
                "cruce_ventas.py": "cruce-ventas",
                "vendedores.py": "vendedores",  
                "vendedor_vinculado.py": "vendedor-vinculado",
                "utilidades.py": "utilidades"
            }
        
            tools_data = []
            for tool in company.tools:
                # Asegurar que tool_type existe y tiene un valor por defecto
                tool_type = tool.tool_type if hasattr(tool, 'tool_type') and tool.tool_type else "procesamiento"
            
                tool_key = tool_mapping.get(tool.filename, tool.filename.replace('.py', '').replace('_', '-'))
                tools_data.append({
                    "id": tool.id,
                    "name": tool.name,
                    "filename": tool.filename,
                    "key": tool_key,
                    "tool_type": tool_type
                })
                
                print(f"🔧 Tool: {tool.name} (ID: {tool.id}) - Filename: {tool.filename} - Type: {tool_type} - Key: {tool_key}")
        
            companies_data.append({
                "id": company.id,
                "name": company.name,
                "folder_name": company.folder_name,
                "tools": tools_data
            })
    
        print(f"📤 Returning {len(companies_data)} companies to frontend")
        return companies_data
    
    except HTTPException:
        raise
    except Exception as e: 
        print(f"❌ Error getting user companies: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error interno del servidor: {str(e)}")

@app.get("/api/tools/{tool_id}/config")
async def get_tool_config(tool_id: int, request: Request, db: Session = Depends(get_db)):
    """Obtener configuración de una herramienta de vinculación"""
    try:
        user_session = request.session.get("user")
        if not user_session:
            raise HTTPException(status_code=401, detail="No autorizado")
    
        tool = db.query(Tool).filter(Tool.id == tool_id).first()
        if not tool:
            raise HTTPException(status_code=404, detail="Herramienta no encontrada")
    
        tool_type = tool.tool_type if hasattr(tool, 'tool_type') and tool.tool_type else "procesamiento"
        if tool_type != "vinculacion":
            raise HTTPException(status_code=400, detail="Esta herramienta no es de vinculación")
    
        # Get linked tools information
        linked_tools = []
        if hasattr(tool, 'linked_processing_tools'):
            for linked_tool in tool.linked_processing_tools:
                linked_tools.append({
                    "id": linked_tool.id,
                    "name": linked_tool.name,
                    "filename": linked_tool.filename
                })
    
        return {
            "total_files": tool.total_files if hasattr(tool, 'total_files') and tool.total_files else 2,
            "file_config": tool.file_config if hasattr(tool, 'file_config') and tool.file_config else {},
            "linked_tools": linked_tools
        }
    
    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ Error getting tool config: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error interno del servidor: {str(e)}")

@app.get("/api/tools/{tool_id}/history")
async def get_tool_history(tool_id: int, request: Request, db: Session = Depends(get_db)): 
    """Obtener historial de archivos procesados para una herramienta"""
    try:
        user_session = request.session.get("user")
        if not user_session:
            raise HTTPException(status_code=401, detail="No autorizado")
    
        user = db.query(User).filter(User.email == user_session["email"]).first()
        if not user:
            raise HTTPException(status_code=404, detail="Usuario no encontrado")
    
        # Obtener archivos del usuario para esta herramienta
        files = db.query(ProcessedFile).filter(
            ProcessedFile.user_id == user.id,
            ProcessedFile.tool_id == tool_id
        ).order_by(ProcessedFile.processed_at.desc()).all()

        print(f"📋 Found {len(files)} files in history for tool {tool_id} and user {user.username}")
    
        files_data = []
        for file in files:
            files_data.append({
                "id": file.id,
                "original_filename": file.original_filename,
                "processed_filename": file.processed_filename,
                "processed_at": file.processed_at.isoformat(),
                "file_size": file.file_size,
                "input_files_info": file.input_files_info if hasattr(file, 'input_files_info') else None
            })
    
        return files_data
    
    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ Error getting tool history: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error interno del servidor: {str(e)}")

@app.get("/api/tools/{tool_id}/processed-files")
async def get_tool_processed_files(tool_id: int, request: Request, db: Session = Depends(get_db)):
    """Obtener archivos procesados de una herramienta específica para herramientas de vinculación"""
    try:
        user_session = request.session.get("user")
        if not user_session:
            raise HTTPException(status_code=401, detail="No autorizado")
    
        user = db.query(User).filter(User.email == user_session["email"]).first()
        if not user:
            raise HTTPException(status_code=404, detail="Usuario no encontrado")
    
        # Obtener archivos procesados de esta herramienta SOLO del usuario actual
        files = db.query(ProcessedFile).filter(
            ProcessedFile.tool_id == tool_id,
            ProcessedFile.user_id == user.id  # Solo archivos del usuario actual
        ).order_by(ProcessedFile.processed_at.desc()).limit(50).all()
    
        files_data = []
        for file in files:
            files_data.append({
                "id": file.id,
                "processed_filename": file.processed_filename,
                "processed_at": file.processed_at.isoformat(),
                "user_username": file.user.username,
                "file_size": file.file_size
            })
    
        return files_data
    
    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ Error getting tool processed files: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error interno del servidor: {str(e)}")

@app.get("/api/files/history/{tool_id}")
async def get_file_history(tool_id: int, request: Request, db: Session = Depends(get_db)):
    """Obtener historial de archivos procesados para una herramienta"""
    try:
        user_session = request.session.get("user")
        if not user_session:
            raise HTTPException(status_code=401, detail="No autorizado")
    
        user = db.query(User).filter(User.email == user_session["email"]).first()
        if not user:
            raise HTTPException(status_code=404, detail="Usuario no encontrado")
    
        # Obtener archivos del usuario para esta herramienta
        files = db.query(ProcessedFile).filter(
            ProcessedFile.user_id == user.id,
            ProcessedFile.tool_id == tool_id
        ).order_by(ProcessedFile.processed_at.desc()).all()
    
        files_data = []
        for file in files:
            files_data.append({
                "id": file.id,
                "original_filename": file.original_filename,
                "processed_filename": file.processed_filename,
                "processed_at": file.processed_at.isoformat(),
                "file_size": file.file_size,
                "input_files_info": file.input_files_info if hasattr(file, 'input_files_info') else None
            })
    
        return files_data
    
    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ Error getting file history: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error interno del servidor: {str(e)}")

@app.get("/api/files/download/{file_id}")
async def download_file(file_id: int, request: Request, db: Session = Depends(get_db)):
    """Descargar archivo procesado"""
    try:
        user_session = request.session.get("user")
        if not user_session:
            raise HTTPException(status_code=401, detail="No autorizado")
    
        user = db.query(User).filter(User.email == user_session["email"]).first()
        if not user:
            raise HTTPException(status_code=404, detail="Usuario no encontrado")
    
        # Obtener archivo (solo del usuario actual)
        file_obj = db.query(ProcessedFile).filter(
            ProcessedFile.id == file_id,
            ProcessedFile.user_id == user.id
        ).first()

        if not file_obj:
            print(f"❌ File not found: ID {file_id} for user {user.id}")
            raise HTTPException(status_code=404, detail="Archivo no encontrado")

        print(f"📥 Downloading file: {file_obj.processed_filename} (ID: {file_id}) for user: {user.username}")
    
        return Response(
            content=file_obj.file_data,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": f"attachment; filename={file_obj.processed_filename}"}
        )
    
    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ Error downloading file: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error interno del servidor: {str(e)}")

@app.post("/api/tools/{tool_id}/process")
async def process_tool_file(
    tool_id: int,
    request: Request,
    file: UploadFile = File(...),
    db: Session = Depends(get_db)
):
    """Procesar archivo con herramienta de procesamiento específica"""
    try:
        user_session = request.session.get("user")
        if not user_session:
            raise HTTPException(status_code=401, detail="No autorizado")
    
        user = db.query(User).filter(User.email == user_session["email"]).first()
        if not user:
            raise HTTPException(status_code=404, detail="Usuario no encontrado")
    
        # Obtener herramienta de la BD
        tool_obj = db.query(Tool).filter(Tool.id == tool_id).first()
        if not tool_obj:
            raise HTTPException(status_code=404, detail="Herramienta no encontrada")
    
        tool_type_db = tool_obj.tool_type if hasattr(tool_obj, 'tool_type') and tool_obj.tool_type else "procesamiento"
        if tool_type_db != "procesamiento":
            raise HTTPException(status_code=400, detail="Esta herramienta no es de procesamiento")
    
        # Validar archivo
        if file.size > 10 * 1024 * 1024:  # 10MB
            raise HTTPException(status_code=400, detail="Archivo demasiado grande (máximo 10MB)")
    
        # Mapeo de herramientas a procesadores
        tool_mapping = {
            "balance_proyectado.py": "balance-proyectado",
            "facturacion.py": "facturacion", 
            "inventario.py": "inventario",
            "ventas.py": "ventas",
            "ventas-csv.py": "ventas-csv",  # ← Asegúrate que esto esté así
            "lista_precios.py": "lista-precios",
            "vendedores.py": "vendedores",
            "utilidades.py": "utilidades"
        }
    
        tool_key = tool_mapping.get(tool_obj.filename)
        print(f"🔍 DEBUG: Tool filename: '{tool_obj.filename}'")
        print(f"🔍 DEBUG: Mapped tool key: '{tool_key}'")
        print(f"🔍 DEBUG: Available processors: {list(PROCESSORS.keys())}")
        
        if not tool_key or tool_key not in PROCESSORS:
            raise HTTPException(status_code=400, detail=f"Procesador no encontrado para la herramienta '{tool_obj.filename}'. Key: '{tool_key}'")
    
        print(f"🔧 Processing file with tool: {tool_obj.name} (ID: {tool_id}) - Processor: {tool_key}")
    
        # Generar nombre de archivo procesado basado en el original
        original_name = os.path.splitext(file.filename)[0]  # Nombre sin extensión
        processed_filename = f"{original_name}_PROCESADO.xlsx"
    
        # Crear archivo temporal
        with tempfile.NamedTemporaryFile(delete=False, suffix=f".{file.filename.split('.')[-1]}") as temp_file:
            content = await file.read()
            temp_file.write(content)
            temp_file_path = temp_file.name
    
        try:
            # Procesar archivo with lazy loading
            processor_func = PROCESSORS[tool_key]
            print(f"🔍 DEBUG: Processor function: {processor_func}")
            
            processor = processor_func()
            print(f"🔍 DEBUG: Processor result: {processor}")
        
            if processor is None:
                raise HTTPException(status_code=500, detail=f"Procesador '{tool_key}' no disponible. Verifique la configuración del módulo.")
        
            print(f"🔍 DEBUG: Calling processor with: {temp_file_path}")
            output_path = processor(temp_file_path, file.filename)

            # Si el processor devuelve tupla → usar solo el primer elemento
            if isinstance(output_path, tuple):
                output_path = output_path[0]

            # Si devuelve bytes (XLSX en memoria) → guardarlo como archivo
            if isinstance(output_path, (bytes, bytearray)):
                import tempfile
                fd, temp_output_path = tempfile.mkstemp(suffix=".xlsx")
                os.close(fd)
                with open(temp_output_path, "wb") as f:
                    f.write(output_path)
                output_path = temp_output_path

            # Convertir siempre a string (por si viene como PathObject)
            output_path = str(output_path)

            print(f"🔍 DEBUG: Processor output path: {output_path}")
        
            # Leer archivo procesado
            with open(output_path, 'rb') as processed_file:
                processed_data = processed_file.read()
        
            # Guardar en base de datos con el nombre consistente
            processed_file_obj = ProcessedFile(
                original_filename=file.filename,
                processed_filename=processed_filename,
                file_data=processed_data,
                user_id=user.id,
                tool_id=tool_obj.id,
                file_size=len(processed_data)
            )
            db.add(processed_file_obj)
            db.commit()
        
            print(f"✅ File processed successfully: {file.filename} -> {processed_filename}")
        
            # Limpiar archivos temporales
            os.unlink(temp_file_path)
            os.unlink(output_path)
        
            return Response(
                content=processed_data,
                media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                headers={"Content-Disposition": f"attachment; filename={processed_filename}"}
            )
        
        except Exception as e:
            # Limpiar archivo temporal en caso de error
            if os.path.exists(temp_file_path):
                os.unlink(temp_file_path)
            print(f"❌ Error in processor: {str(e)}")
            import traceback
            traceback.print_exc()
            raise e
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ Error processing file: {str(e)}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error procesando archivo: {str(e)}")

@app.post("/api/tools/{tool_id}/process-linking")
async def process_linking_files(
    tool_id: int,
    request: Request,
    db: Session = Depends(get_db)
):
    """Procesar archivos con herramienta de vinculación"""
    try:
        user_session = request.session.get("user")
        if not user_session:
            raise HTTPException(status_code=401, detail="No autorizado")
        
        user = db.query(User).filter(User.email == user_session["email"]).first()
        if not user:
            raise HTTPException(status_code=404, detail="Usuario no encontrado")
        
        # Get tool from database
        tool_obj = db.query(Tool).filter(Tool.id == tool_id).first()
        if not tool_obj:
            raise HTTPException(status_code=404, detail="Herramienta no encontrada")
        
        tool_type_db = tool_obj.tool_type if hasattr(tool_obj, 'tool_type') and tool_obj.tool_type else "procesamiento"
        if tool_type_db != "vinculacion":
            raise HTTPException(status_code=400, detail="Esta herramienta no es de vinculación")
        
        # Parse form data to get files
        form_data = await request.form()
        input_files = []
        input_files_info = []
        temp_files = []
        
        try:
            total_files = tool_obj.total_files if hasattr(tool_obj, 'total_files') and tool_obj.total_files else 2
            
            for i in range(total_files):
                # Check for processed file first
                processed_file_id = form_data.get(f"processed_file_{i}")
                upload_file = form_data.get(f"upload_file_{i}")
                
                if processed_file_id:
                    # Handle processed file
                    processed_file = db.query(ProcessedFile).filter(
                        ProcessedFile.id == int(processed_file_id)
                    ).first()
                    
                    if processed_file:
                        # Create temporary file from processed data
                        with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as temp_file:
                            temp_file.write(processed_file.file_data)
                            temp_file_path = temp_file.name
                            temp_files.append(temp_file_path)
                        
                        input_files.append(temp_file_path)
                        input_files_info.append({
                            "filename": processed_file.processed_filename,
                            "source": "processed",
                            "source_tool": processed_file.tool.name,
                            "source_user": processed_file.user.username
                        })
                        print(f"✅ Added processed file: {processed_file.processed_filename}")
                
                elif upload_file and hasattr(upload_file, 'filename'):
                    # Handle uploaded file
                    # Create temporary file
                    with tempfile.NamedTemporaryFile(delete=False, suffix=f".{upload_file.filename.split('.')[-1]}") as temp_file:
                        content = await upload_file.read()
                        temp_file.write(content)
                        temp_file_path = temp_file.name
                        temp_files.append(temp_file_path)
                    
                    input_files.append(temp_file_path)
                    input_files_info.append({
                        "filename": upload_file.filename,
                        "source": "upload"
                    })
                    print(f"✅ Added uploaded file: {upload_file.filename}")
            
            if len(input_files) != total_files:
                raise HTTPException(status_code=400, detail=f"Se requieren {total_files} archivos, se recibieron {len(input_files)}")
            
            # Load and execute the linking tool
            tool_module_path = os.path.join(tool_obj.company.folder_name, tool_obj.filename)
            
            if not os.path.exists(tool_module_path):
                raise HTTPException(status_code=404, detail="Archivo de herramienta no encontrado")
            
            # Import the tool module dynamically
            spec = importlib.util.spec_from_file_location("linking_tool", tool_module_path)
            tool_module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(tool_module)
            
            # Execute the linking function
            if hasattr(tool_module, 'process_files'):
                output_path = tool_module.process_files(input_files)
            else:
                raise HTTPException(status_code=500, detail="Función process_files no encontrada en la herramienta")
            
            # Read processed file
            with open(output_path, 'rb') as processed_file:
                processed_data = processed_file.read()
            
            # Save to database
            processed_file_obj = ProcessedFile(
                original_filename=f"vinculacion_{datetime.now().strftime('%Y%m%d_%H%M%S')}_PROCESADO",
                processed_filename=os.path.basename(output_path),
                file_data=processed_data,
                user_id=user.id,
                tool_id=tool_obj.id,
                file_size=len(processed_data)
            )
            
            # Add input_files_info if the column exists
            if hasattr(ProcessedFile, 'input_files_info'):
                processed_file_obj.input_files_info = input_files_info
            
            db.add(processed_file_obj)
            db.commit()
            
            print(f"✅ Linking tool processed successfully: {len(input_files)} files -> {os.path.basename(output_path)}")
            
            # Clean up temporary files
            for temp_file in temp_files:
                if os.path.exists(temp_file):
                    os.unlink(temp_file)
            if os.path.exists(output_path):
                os.unlink(output_path)
            
            return Response(
                content=processed_data,
                media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                headers={"Content-Disposition": f"attachment; filename={os.path.basename(output_path)}"}
            )
            
        except Exception as e:
            # Clean up temporary files in case of error
            for temp_file in temp_files:
                if os.path.exists(temp_file):
                    os.unlink(temp_file)
            raise e
            
    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ Error processing linking tool: {str(e)}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error procesando herramienta de vinculación: {str(e)}")

def get_current_user_auth(request: Request, db: Session = Depends(get_db)):
    """Get current user from session with authentication check"""
    user_session = request.session.get("user")
    if not user_session:
        raise HTTPException(status_code=401, detail="No autorizado")
    
    user = db.query(User).filter(User.email == user_session["email"]).first()
    if not user:
        raise HTTPException(status_code=404, detail="Usuario no encontrado")
    
    return user

@app.get("/api/tools/{tool_id}/has-guide")
async def check_tool_guide(
    tool_id: int,
    request: Request,
    db: Session = Depends(get_db)
):
    """Check if tool has a PDF guide"""
    user = get_current_user_auth(request, db)
    
    try:
        tool = db.query(Tool).filter(Tool.id == tool_id).first()
        if not tool:
            raise HTTPException(status_code=404, detail="Herramienta no encontrada")
        
        # Check if user has access to this tool's company
        if not user.is_admin and tool.company not in user.companies:
            raise HTTPException(status_code=403, detail="No tienes acceso a esta herramienta")
        
        has_guide = bool(tool.guide_pdf) if hasattr(tool, 'guide_pdf') else False
        filename = tool.guide_pdf_filename if hasattr(tool, 'guide_pdf_filename') and tool.guide_pdf_filename else None
        
        return {
            "has_guide": has_guide,
            "filename": filename
        }
        
    except HTTPException:
        ře
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error interno: {str(e)}")

@app.get("/api/tools/{tool_id}/view-guide")
async def view_tool_guide(
    tool_id: int,
    request: Request,
    db: Session = Depends(get_db)
):
    """View PDF guide for a tool"""
    user = get_current_user_auth(request, db)
    
    try:
        tool = db.query(Tool).filter(Tool.id == tool_id).first()
        if not tool:
            raise HTTPException(status_code=404, detail="Herramienta no encontrada")
        
        # Check if user has access to this tool's company
        if not user.is_admin and tool.company not in user.companies:
            raise HTTPException(status_code=403, detail="No tienes acceso a esta herramienta")
        
        if not hasattr(tool, 'guide_pdf') or not tool.guide_pdf:
            raise HTTPException(status_code=404, detail="PDF no encontrado")
        
        filename = tool.guide_pdf_filename if hasattr(tool, 'guide_pdf_filename') and tool.guide_pdf_filename else "guia.pdf"
        
        return Response(
            content=tool.guide_pdf,
            media_type="application/pdf",
            headers={"Content-Disposition": f"inline; filename={filename}"}
        )
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error interno: {str(e)}")

@app.get("/api/tools/{tool_id}/download-guide")
async def download_tool_guide(
    tool_id: int,
    request: Request,
    db: Session = Depends(get_db)
):
    """Download PDF guide for a tool"""
    user = get_current_user_auth(request, db)
    
    try:
        tool = db.query(Tool).filter(Tool.id == tool_id).first()
        if not tool:
            raise HTTPException(status_code=404, detail="Herramienta no encontrada")
        
        # Check if user has access to this tool's company
        if not user.is_admin and tool.company not in user.companies:
            raise HTTPException(status_code=403, detail="No tienes acceso a esta herramienta")
        
        if not hasattr(tool, 'guide_pdf') or not tool.guide_pdf:
            raise HTTPException(status_code=404, detail="PDF no encontrado")
        
        filename = tool.guide_pdf_filename if hasattr(tool, 'guide_pdf_filename') and tool.guide_pdf_filename else "guia.pdf"
        
        return Response(
            content=tool.guide_pdf,
            media_type="application/pdf",
            headers={"Content-Disposition": f"attachment; filename={filename}"}
        )
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error interno: {str(e)}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
from fastapi import APIRouter, Request, Depends, HTTPException, Form, UploadFile, File
from fastapi.responses import HTMLResponse, RedirectResponse, Response
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session
from database import get_db
from models import User, Company, Tool, ProcessedFile
import os
import json

router = APIRouter()
templates = Jinja2Templates(directory="templates")

def require_admin(request: Request, db: Session = Depends(get_db)):
    """Middleware to verify that the user is an administrator"""
    user_session = request.session.get("user")
    if not user_session:
        raise HTTPException(status_code=401, detail="No autorizado")
    
    user = db.query(User).filter(User.email == user_session["email"]).first()
    if not user or not user.is_admin:
        raise HTTPException(status_code=403, detail="Acceso denegado - Se requieren permisos de administrador")
    
    return user

@router.get("/", response_class=HTMLResponse)
async def admin_panel(request: Request, db: Session = Depends(get_db)):
    """Main administration panel"""
    admin_user = require_admin(request, db)
    
    # Get data for the admin panel
    users = db.query(User).all()
    companies = db.query(Company).all()
    tools = db.query(Tool).all()
    
    return templates.TemplateResponse("admin.html", {
        "request": request,
        "admin_user": admin_user,
        "users": users,
        "companies": companies,
        "tools": tools
    })

@router.get("/api/files")
async def get_all_files(request: Request, db: Session = Depends(get_db)):
    """Retrieve all processed files for the admin panel"""
    admin_user = require_admin(request, db)
    
    try:
        # Obtener todos los archivos con información relacionada
        files = db.query(ProcessedFile).join(User).join(Tool).join(Company).all()
        
        files_data = []
        for file in files:
            # Safe access to tool_type
            tool_type = file.tool.tool_type if hasattr(file.tool, 'tool_type') and file.tool.tool_type else "procesamiento"
            
            files_data.append({
                "id": file.id,
                "original_filename": file.original_filename,
                "processed_filename": file.processed_filename,
                "file_size": file.file_size,
                "processed_at": file.processed_at.isoformat(),
                "user_id": file.user_id,
                "user_username": file.user.username,
                "user_email": file.user.email,
                "tool_id": file.tool_id,
                "tool_name": file.tool.name,
                "tool_filename": file.tool.filename,
                "tool_type": tool_type,
                "company_id": file.tool.company_id,
                "company_name": file.tool.company.name,
                "company_folder": file.tool.company.folder_name,
                "input_files_info": file.input_files_info if hasattr(file, 'input_files_info') else None
            })
        
        # Ordenar por fecha de procesamiento (más recientes primero)
        files_data.sort(key=lambda x: x["processed_at"], reverse=True)
        
        return files_data
        
    except Exception as e:
        print(f"❌ Error getting files: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error al obtener archivos: {str(e)}")

@router.get("/api/files/{file_id}/download")
async def download_admin_file(
    file_id: int,
    request: Request,
    db: Session = Depends(get_db)
):
    """Descargar archivo procesado desde el panel de administración"""
    admin_user = require_admin(request, db)
    
    try:
        # Obtener archivo
        file_obj = db.query(ProcessedFile).filter(ProcessedFile.id == file_id).first()
        
        if not file_obj:
            raise HTTPException(status_code=404, detail="Archivo no encontrado")
        
        return Response(
            content=file_obj.file_data,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": f"attachment; filename={file_obj.processed_filename}"}
        )
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ Error downloading file: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error al descargar archivo: {str(e)}")

@router.post("/users/create")
async def create_user(
    request: Request,
    email: str = Form(...),
    is_admin: bool = Form(False),
    db: Session = Depends(get_db)
):
    """Crear nuevo usuario admin (solo requiere email para SSO)"""
    require_admin(request, db)
    
    # Verificar que no exista el usuario
    existing_user = db.query(User).filter(User.email == email).first()
    
    if existing_user:
        raise HTTPException(status_code=400, detail="Usuario ya existe")
    
    # Crear usuario (username se genera del email)
    username = email.split('@')[0]
    user = User(username=username, email=email, is_admin=is_admin)
    
    db.add(user)
    db.commit()
    
    return RedirectResponse(url="/admin", status_code=302)

@router.delete("/users/{user_id}")
async def delete_user(user_id: int, request: Request, db: Session = Depends(get_db)):
    """Eliminar usuario (proteger owner y admin actual)"""
    admin_user = require_admin(request, db)
    
    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        raise HTTPException(status_code=404, detail="Usuario no encontrado")
    
    # Proteger al admin actual
    if user.id == admin_user.id:
        raise HTTPException(status_code=403, detail="No se puede eliminar tu propio usuario")
    
    db.delete(user)
    db.commit()
    
    return {"message": "Usuario eliminado correctamente"}

@router.post("/companies/create")
async def create_company(
    request: Request,
    name: str = Form(...),
    folder_name: str = Form(...),
    db: Session = Depends(get_db)
):
    """Crear nueva empresa"""
    require_admin(request, db)
    
    # Verificar que no exista la carpeta
    existing_company = db.query(Company).filter(Company.folder_name == folder_name).first()
    if existing_company:
        raise HTTPException(status_code=400, detail="Nombre de carpeta ya existe")
    
    # Crear empresa
    company = Company(name=name, folder_name=folder_name)
    db.add(company)
    db.commit()
    
    # Crear estructura de carpetas física
    try:
        folder_path = os.path.join(".", folder_name)
        os.makedirs(folder_path, exist_ok=True)
        
        # Crear archivo __init__.py vacío
        init_file = os.path.join(folder_path, "__init__.py")
        with open(init_file, "w") as f:
            f.write(f"# Módulo de herramientas para {name}\n")
    except Exception as e:
        print(f"⚠️ Warning: Could not create folder structure: {e}")
    
    return RedirectResponse(url="/admin", status_code=302)

@router.get("/api/companies/{company_id}/processing-tools")
async def get_company_processing_tools(
    company_id: int,
    request: Request,
    db: Session = Depends(get_db)
):
    """Obtener herramientas de procesamiento de una empresa para herramientas de vinculación"""
    require_admin(request, db)
    
    try:
        # Obtener todas las herramientas de la empresa
        all_tools = db.query(Tool).filter(Tool.company_id == company_id).all()
        
        print(f"🔍 Company {company_id}: Found {len(all_tools)} total tools")
        
        processing_tools = []
        for tool in all_tools:
            # Safe access to tool_type - default to 'procesamiento' if not set
            tool_type = tool.tool_type if hasattr(tool, 'tool_type') and tool.tool_type else "procesamiento"
            print(f"  📋 Tool: {tool.name} - Type: {tool_type}")
            
            if tool_type == "procesamiento":
                processing_tools.append({
                    "id": tool.id,
                    "name": tool.name,
                    "filename": tool.filename
                })
        
        print(f"✅ Returning {len(processing_tools)} processing tools for company {company_id}")
        return processing_tools
        
    except Exception as e:
        print(f"❌ Error getting processing tools: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error al obtener herramientas: {str(e)}")

@router.post("/companies/{company_id}/tools/create")
async def create_tool(
    company_id: int,
    request: Request,
    name: str = Form(...),
    filename: str = Form(...),
    tool_type: str = Form("procesamiento"),
    total_files: int = Form(None),
    linked_tools: str = Form(""),
    file_config: str = Form(""),
    db: Session = Depends(get_db)
):
    """Crear nueva herramienta para una empresa"""
    require_admin(request, db)
    
    try:
        print(f"🔧 Creating tool: {name} ({tool_type}) for company {company_id}")
        print(f"📝 Form data: filename={filename}, total_files={total_files}, linked_tools={linked_tools}")
        
        company = db.query(Company).filter(Company.id == company_id).first()
        if not company:
            raise HTTPException(status_code=404, detail="Empresa no encontrada")
        
        # Validar que el filename termine en .py
        if not filename.endswith('.py'):
            raise HTTPException(status_code=400, detail="El nombre del archivo debe terminar en .py")
        
        # Verificar que no exista una herramienta con el mismo nombre de archivo
        existing_tool = db.query(Tool).filter(
            Tool.filename == filename,
            Tool.company_id == company_id
        ).first()
        
        if existing_tool:
            raise HTTPException(status_code=400, detail="Ya existe una herramienta con ese nombre de archivo")
        
        # Validaciones específicas para herramientas de vinculación
        linked_tool_ids = []
        file_config_data = {}
        
        if tool_type == "vinculacion":
            print("🔗 Processing linking tool...")
            
            # Validar total_files
            if not total_files or total_files < 2 or total_files > 6:
                raise HTTPException(status_code=400, detail="Las herramientas de vinculación deben tener entre 2 y 6 archivos")
            
            # Validar linked_tools
            if not linked_tools:
                raise HTTPException(status_code=400, detail="Debe seleccionar al menos 2 herramientas de procesamiento")
            
            try:
                linked_tool_ids = [int(x.strip()) for x in linked_tools.split(",") if x.strip()]
                print(f"🔗 Linked tool IDs: {linked_tool_ids}")
                
                if len(linked_tool_ids) < 2:
                    raise HTTPException(status_code=400, detail="Debe seleccionar al menos 2 herramientas de procesamiento")
                
                if len(linked_tool_ids) > total_files:
                    raise HTTPException(status_code=400, detail="No puede tener más herramientas vinculadas que archivos totales")
                
                # Verificar que todas las herramientas pertenezcan a la misma empresa y sean de procesamiento
                linked_tools_check = db.query(Tool).filter(
                    Tool.id.in_(linked_tool_ids),
                    Tool.company_id == company_id
                ).all()
                
                if len(linked_tools_check) != len(linked_tool_ids):
                    raise HTTPException(status_code=400, detail="Todas las herramientas vinculadas deben pertenecer a la misma empresa")
                
                # Verificar que sean herramientas de procesamiento
                for tool_check in linked_tools_check:
                    tool_type_check = tool_check.tool_type if hasattr(tool_check, 'tool_type') and tool_check.tool_type else "procesamiento"
                    if tool_type_check != "procesamiento":
                        raise HTTPException(status_code=400, detail="Solo se pueden vincular herramientas de procesamiento")
                
            except ValueError as e:
                print(f"❌ Error parsing linked tools: {e}")
                raise HTTPException(status_code=400, detail="IDs de herramientas inválidos")
            
            # Procesar file_config con nombres personalizados
            try:
                if file_config:
                    file_config_data = json.loads(file_config)
                    print(f"📋 Using custom file config: {file_config_data}")
                else:
                    # Generar configuración automática con nombres de herramientas
                    file_config_data = {}
                    linked_tools_info = db.query(Tool).filter(Tool.id.in_(linked_tool_ids)).all()
                    
                    # Primero asignar archivos vinculados con nombres de herramientas
                    for i, tool_id in enumerate(linked_tool_ids):
                        tool_info = next((t for t in linked_tools_info if t.id == tool_id), None)
                        tool_name = tool_info.name if tool_info else f"Herramienta {tool_id}"
                        file_config_data[str(i)] = {
                            "type": "linked", 
                            "tool_id": tool_id,
                            "name": tool_name  # Usar el nombre de la herramienta como nombre del archivo
                        }
                    
                    # Para archivos adicionales sin herramienta asociada
                    for i in range(len(linked_tool_ids), total_files):
                        additional_file_number = i - len(linked_tool_ids) + 1
                        file_config_data[str(i)] = {
                            "type": "upload",
                            "name": f"Archivo adicional {additional_file_number}"
                        }
            
                print(f"📋 Final file config: {file_config_data}")
                        
            except json.JSONDecodeError as e:
                print(f"❌ Error parsing file config: {e}")
                raise HTTPException(status_code=400, detail="Configuración de archivos inválida")
        
        # Crear herramienta en BD
        tool_data = {
            "name": name,
            "filename": filename,
            "company_id": company_id,
            "tool_type": tool_type
        }
        
        if tool_type == "vinculacion":
            tool_data["total_files"] = total_files
            tool_data["file_config"] = file_config_data
        
        print(f"💾 Creating tool with data: {tool_data}")
        
        tool = Tool(**tool_data)
        db.add(tool)
        db.commit()
        db.refresh(tool)
        
        print(f"✅ Tool created with ID: {tool.id}")
        
        # Agregar relaciones para herramientas de vinculación
        if tool_type == "vinculacion" and linked_tool_ids:
            print(f"🔗 Adding relationships for {len(linked_tool_ids)} tools...")
            for tool_id in linked_tool_ids:
                processing_tool = db.query(Tool).filter(Tool.id == tool_id).first()
                if processing_tool:
                    tool.linked_processing_tools.append(processing_tool)
                    print(f"  ✅ Linked tool {processing_tool.name}")
            db.commit()
            print("✅ All relationships added")
        
        # Crear archivo Python template
        try:
            file_path = os.path.join(".", company.folder_name, filename)
            if not os.path.exists(file_path):
                if tool_type == "procesamiento":
                    template_content = f'''"""
Herramienta: {name}
Empresa: {company.name}
Tipo: Procesamiento
"""
import pandas as pd
import os
from datetime import datetime

def process_file(input_path: str) -> str:
    """
    Procesar archivo de entrada y retornar ruta del archivo procesado
    
    Args:
        input_path: Ruta del archivo a procesar
        
    Returns:
        str: Ruta del archivo procesado
    """
    try:
        # Leer archivo de entrada
        if input_path.endswith('.csv'):
            df = pd.read_csv(input_path)
        else:
            df = pd.read_excel(input_path)
        
        # IMPLEMENTAR LÓGICA DE PROCESAMIENTO AQUÍ
        # Ejemplo: agregar columna con timestamp
        df['processed_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        # Generar archivo de salida
        output_filename = f"processed_{name.lower().replace(' ', '_')}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        output_path = os.path.join("downloads", output_filename)
        
        # Crear directorio de salida si no existe
        os.makedirs("downloads", exist_ok=True)
        
        # Guardar archivo procesado
        df.to_excel(output_path, index=False)
        
        return output_path
        
    except Exception as e:
        raise Exception(f"Error procesando archivo: {{str(e)}}")
'''
                else:  # vinculacion
                    template_content = f'''"""
Herramienta: {name}
Empresa: {company.name}
Tipo: Vinculación
Archivos totales: {total_files}
"""
import pandas as pd
import os
from datetime import datetime
from typing import List, Dict

def process_files(file_paths: List[str], file_info: List[Dict]) -> str:
    """
    Procesar múltiples archivos y combinarlos en uno solo
    
    Args:
        file_paths: Lista de rutas de archivos a procesar
        file_info: Lista con información de cada archivo (nombre, tipo, etc.)
        
    Returns:
        str: Ruta del archivo procesado combinado
    """
    try:
        dataframes = []
        
        # Procesar cada archivo
        for i, (file_path, info) in enumerate(zip(file_paths, file_info)):
            print(f"Procesando archivo {{i+1}}: {{info.get('name', 'Sin nombre')}}")
            
            # Leer archivo
            if file_path.endswith('.csv'):
                df = pd.read_csv(file_path)
            else:
                df = pd.read_excel(file_path)
            
            # Agregar columna identificadora del origen
            df['source_file'] = info.get('name', f'Archivo {{i+1}}')
            df['file_index'] = i + 1
            
            dataframes.append(df)
        
        # IMPLEMENTAR LÓGICA DE COMBINACIÓN AQUÍ
        # Ejemplo: concatenar todos los DataFrames
        combined_df = pd.concat(dataframes, ignore_index=True)
        
        # Agregar información de procesamiento
        combined_df['processed_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        combined_df['total_files_processed'] = len(file_paths)
        
        # Generar archivo de salida
        output_filename = f"combined_{name.lower().replace(' ', '_')}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        output_path = os.path.join("downloads", output_filename)
        
        # Crear directorio de salida si no existe
        os.makedirs("downloads", exist_ok=True)
        
        # Guardar archivo combinado
        combined_df.to_excel(output_path, index=False)
        
        return output_path
        
    except Exception as e:
        raise Exception(f"Error procesando archivos: {{str(e)}}")
'''
                
                with open(file_path, "w", encoding="utf-8") as f:
                    f.write(template_content)
                
                print(f"✅ Template file created: {file_path}")
        except Exception as e:
            print(f"⚠️ Warning: Could not create template file: {e}")
        
        return RedirectResponse(url="/admin", status_code=302)
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ Error creating tool: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error al crear herramienta: {str(e)}")

@router.post("/tools/{tool_id}/upload-pdf")
async def upload_tool_pdf(
    tool_id: int,
    request: Request,
    pdf_file: UploadFile = File(...),
    db: Session = Depends(get_db)
):
    """Subir PDF de guía para una herramienta"""
    require_admin(request, db)
    
    try:
        # Verificar que sea un PDF
        if not pdf_file.filename.lower().endswith('.pdf'):
            raise HTTPException(status_code=400, detail="Solo se permiten archivos PDF")
        
        # Verificar tamaño (10MB máximo)
        content = await pdf_file.read()
        if len(content) > 10 * 1024 * 1024:  # 10MB
            raise HTTPException(status_code=400, detail="El archivo es demasiado grande (máximo 10MB)")
        
        # Obtener herramienta
        tool = db.query(Tool).filter(Tool.id == tool_id).first()
        if not tool:
            raise HTTPException(status_code=404, detail="Herramienta no encontrada")
        
        # Guardar PDF en la base de datos
        tool.guide_pdf = content
        tool.guide_pdf_filename = pdf_file.filename
        
        db.commit()
        
        return {"message": "PDF subido correctamente"}
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ Error uploading PDF: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error al subir PDF: {str(e)}")

@router.get("/tools/{tool_id}/view-pdf")
async def view_tool_pdf(
    tool_id: int,
    request: Request,
    db: Session = Depends(get_db)
):
    """Ver PDF de guía de una herramienta"""
    require_admin(request, db)
    
    try:
        tool = db.query(Tool).filter(Tool.id == tool_id).first()
        if not tool or not tool.guide_pdf:
            raise HTTPException(status_code=404, detail="PDF no encontrado")
        
        return Response(
            content=tool.guide_pdf,
            media_type="application/pdf",
            headers={"Content-Disposition": f"inline; filename={tool.guide_pdf_filename}"}
        )
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ Error viewing PDF: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error al ver PDF: {str(e)}")

@router.get("/tools/{tool_id}/download-pdf")
async def download_tool_pdf(
    tool_id: int,
    request: Request,
    db: Session = Depends(get_db)
):
    """Descargar PDF de guía de una herramienta"""
    require_admin(request, db)
    
    try:
        tool = db.query(Tool).filter(Tool.id == tool_id).first()
        if not tool or not tool.guide_pdf:
            raise HTTPException(status_code=404, detail="PDF no encontrado")
        
        return Response(
            content=tool.guide_pdf,
            media_type="application/pdf",
            headers={"Content-Disposition": f"attachment; filename={tool.guide_pdf_filename}"}
        )
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ Error downloading PDF: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error al descargar PDF: {str(e)}")

tool_mapping = {
    "balance_proyectado.py": "balance-proyectado",
    "facturacion.py": "facturacion",
    "inventario.py": "inventario",
    "ventas.py": "ventas",
    "lista_precios.py": "lista-precios",
    "cruce_ventas.py": "cruce-ventas"  # Agregar esta línea
}

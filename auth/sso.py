from fastapi import APIRouter, Request, Response, Depends
from fastapi.responses import RedirectResponse, JSONResponse
from fastapi_sso.sso.google import GoogleSSO
from sqlalchemy.orm import Session
from database import get_db, SessionLocal
from models import User
import os
from dotenv import load_dotenv

load_dotenv()

router = APIRouter()

# Variables de entorno
ENVIRONMENT = os.getenv("ENVIRONMENT", "production")
GOOGLE_CLIENT_ID = os.getenv("GOOGLE_CLIENT_ID")
GOOGLE_CLIENT_SECRET = os.getenv("GOOGLE_CLIENT_SECRET")

def get_base_url():
    """Determinar la URL base correctamente"""
    # 1. Variable personalizada (recomendado para Railway)
    custom_url = os.getenv("APP_URL")
    if custom_url:
        return custom_url.rstrip('/')
    
    # 2. Variables de Railway
    railway_domain = os.getenv("RAILWAY_PUBLIC_DOMAIN")
    if railway_domain:
        return f"https://{railway_domain}"
    
    # 3. Hardcoded para Railway (última opción)
    if ENVIRONMENT == "production":
        return "https://web-production-ee88.up.railway.app"
    
    # 4. Desarrollo local
    return "http://127.0.0.1:8000"

# Configurar URLs
BASE_URL = get_base_url()
REDIRECT_URI = f"{BASE_URL}/sso/callback"
ALLOW_INSECURE = ENVIRONMENT != "production"

print(f"🔐 SSO Config - Environment: {ENVIRONMENT}")
print(f"🔗 Base URL: {BASE_URL}")
print(f"🔗 Redirect URI: {REDIRECT_URI}")
print(f"🔒 Allow Insecure: {ALLOW_INSECURE}")

# Verificar variables requeridas
if not GOOGLE_CLIENT_ID or not GOOGLE_CLIENT_SECRET:
    print("❌ ERROR: GOOGLE_CLIENT_ID y GOOGLE_CLIENT_SECRET son requeridos")
    raise ValueError("Faltan credenciales de Google OAuth")

# Configurar Google SSO
try:
    google_sso = GoogleSSO(
        client_id=GOOGLE_CLIENT_ID,
        client_secret=GOOGLE_CLIENT_SECRET,
        redirect_uri=REDIRECT_URI,
        allow_insecure_http=ALLOW_INSECURE
    )
    print("✅ Google SSO configured successfully")
except Exception as e:
    print(f"❌ Error configuring Google SSO: {e}")
    raise

@router.get("/login")
async def login():
    """Redirige al usuario a la página de login de Google"""
    try:
        print(f"🚀 Initiating SSO login - Redirect URI: {REDIRECT_URI}")
        return await google_sso.get_login_redirect()
    except Exception as e:
        print(f"❌ Error in SSO login: {e}")
        return RedirectResponse(url="/?error=sso_config_error", status_code=302)

@router.get("/callback")
async def callback(request: Request):
    """Maneja la respuesta de Google después del login"""
    try:
        print(f"🔄 Processing SSO callback from: {request.url}")
        
        # Verificar y procesar el token de Google
        user_data = await google_sso.verify_and_process(request)
        print(f"📥 User data received: {user_data.email}")
        
        # Guardar/actualizar usuario en base de datos
        db = SessionLocal()
        try:
            user = db.query(User).filter(User.email == user_data.email).first()
            
            if not user:
                # Crear nuevo usuario
                username = (
                    user_data.display_name or 
                    user_data.first_name or 
                    user_data.email.split('@')[0]
                )
                user = User(
                    username=username.replace(' ', '_').lower(),
                    email=user_data.email,
                    is_admin=False  # Por defecto no es admin
                )
                db.add(user)
                db.commit()
                db.refresh(user)
                print(f"✅ New user created: {user.email}")
            else:
                print(f"✅ Existing user logged in: {user.email}")
            
            # Guardar en sesión
            request.session["user"] = {
                "name": user_data.display_name or user_data.first_name or user.username,
                "email": user_data.email,
                "is_admin": user.is_admin,
                "picture": getattr(user_data, 'picture', '')
            }
            
            print(f"💾 Session saved for user: {user.email}")
            
        except Exception as db_error:
            print(f"❌ Database error: {db_error}")
            db.rollback()
            raise
        finally:
            db.close()
        
        return RedirectResponse(url="/", status_code=302)
        
    except Exception as e:
        print(f"❌ Error en callback SSO: {e}")
        print(f"🔍 Request URL: {request.url}")
        print(f"🔍 Request params: {dict(request.query_params)}")
        return RedirectResponse(url="/?error=login_failed", status_code=302)

@router.get("/logout")
async def logout(request: Request):
    """Elimina la información del usuario de la sesión"""
    user_session = request.session.get("user")
    if user_session:
        print(f"👋 User logged out: {user_session.get('email', 'unknown')}")
    
    request.session.clear()
    return RedirectResponse(url="/", status_code=302)

@router.get("/api/user")
async def get_user(request: Request):
    """API endpoint para obtener información del usuario actual"""
    user_session = request.session.get("user")
    if user_session:
        # Obtener información actualizada de la base de datos
        db = SessionLocal()
        try:
            user = db.query(User).filter(User.email == user_session["email"]).first()
            if user:
                # Actualizar información de sesión con datos actuales de BD
                user_session.update({
                    "is_admin": user.is_admin,
                    "username": user.username
                })
                request.session["user"] = user_session
                return JSONResponse(user_session)
            else:
                # Usuario no encontrado en BD, limpiar sesión
                request.session.clear()
                return JSONResponse({"error": "User not found"}, status_code=404)
        except Exception as e:
            print(f"❌ Error getting user info: {e}")
            return JSONResponse({"error": "Database error"}, status_code=500)
        finally:
            db.close()
    
    return JSONResponse({"error": "Unauthorized"}, status_code=401)

@router.get("/debug")
async def debug_config(request: Request):
    """Endpoint para debugging de configuración SSO"""
    return JSONResponse({
        "environment": ENVIRONMENT,
        "base_url": BASE_URL,
        "redirect_uri": REDIRECT_URI,
        "allow_insecure": ALLOW_INSECURE,
        "has_client_id": bool(GOOGLE_CLIENT_ID),
        "has_client_secret": bool(GOOGLE_CLIENT_SECRET),
        "env_vars": {
            "APP_URL": os.getenv("APP_URL"),
            "RAILWAY_PUBLIC_DOMAIN": os.getenv("RAILWAY_PUBLIC_DOMAIN"),
            "ENVIRONMENT": ENVIRONMENT
        },
        "request_info": {
            "host": request.headers.get("host"),
            "scheme": request.url.scheme,
            "x_forwarded_proto": request.headers.get("x-forwarded-proto"),
            "full_url": str(request.url)
        }
    })
import pandas as pd
import os
from datetime import datetime
import numpy as np

def process_files(input_files: list) -> str:
    """
    Procesar múltiples archivos para cruce de ventas usando merge
    
    Args:
        input_files: Lista de rutas de archivos a procesar (4 archivos)
        
    Returns:
        str: Ruta del archivo procesado
    """
    try:
        if len(input_files) != 4:
            raise Exception(f"Se requieren exactamente 4 archivos, se recibieron {len(input_files)}")
        
        print(f"🔗 Procesando cruce de ventas con {len(input_files)} archivos...")
        
        # Leer todos los archivos
        dataframes = []
        for i, input_path in enumerate(input_files):
            print(f"📖 Leyendo archivo {i+1}: {input_path}")
            try:
                if input_path.endswith('.csv'):
                    if i == 0:  # PRIMER ARCHIVO: Saltar primera fila (headers)
                        df = pd.read_csv(input_path, header=None, skiprows=1)
                        print(f"   📋 Archivo 1: Saltando primera fila (headers)")
                    else:
                        df = pd.read_csv(input_path, header=None)
                else:
                    if i == 0:  # PRIMER ARCHIVO: Saltar primera fila (headers)
                        df = pd.read_excel(input_path, header=None, skiprows=1)
                        print(f"   📋 Archivo 1: Saltando primera fila (headers)")
                    else:
                        df = pd.read_excel(input_path, header=None)
                
                dataframes.append(df)
                print(f"   ✅ Archivo {i+1} leído exitosamente: {df.shape[0]} filas, {df.shape[1]} columnas")
            except Exception as e:
                print(f"   ❌ Error leyendo archivo {i+1}: {str(e)}")
                raise Exception(f"Error leyendo archivo {i+1}: {str(e)}")
        
        if len(dataframes) != 4:
            raise Exception(f"Error: Solo se pudieron leer {len(dataframes)} archivos de 4 requeridos")
        
        archivo1, archivo2, archivo3, archivo4 = dataframes
        
        # Verificar que el archivo 1 tenga columna K (índice 10)
        if archivo1.shape[1] < 11:
            raise Exception("El primer archivo debe tener al menos 11 columnas (hasta la columna K)")
        
        print("🔍 Iniciando proceso de cruce con merge...")
        
        # PASO 1: Preparar archivo base (archivo1) - SOLO LOS DATOS PUROS (ya sin headers)
        print("📋 Paso 1: Preparando archivo base (datos puros, sin headers)")
        resultado_final = archivo1.copy()
        
        # Crear columna de merge basada en columna K (índice 10) del archivo1
        resultado_final['merge_key_k'] = resultado_final.iloc[:, 10].fillna('').astype(str).str.strip().str.upper()
        
        # Crear columna de merge basada en columna A (índice 0) del archivo1 para archivo4
        resultado_final['merge_key_a'] = resultado_final.iloc[:, 0].fillna('').astype(str).str.strip().str.upper()
        
        print(f"   ✅ Archivo base preparado: {resultado_final.shape[0]} filas, {resultado_final.shape[1]} columnas")
        
        # PASO 2: Merge con archivo 2 (columnas C-G donde columna A coincida con columna K de archivo1)
        print("📋 Paso 2: Merge con archivo 2 (columnas C-G)")
        if archivo2.shape[1] >= 7:
            # Preparar archivo2 para merge
            archivo2_prep = archivo2.copy()
            archivo2_prep['merge_key'] = archivo2_prep.iloc[:, 0].fillna('').astype(str).str.strip().str.upper()
            
            # Seleccionar solo las columnas C-G (índices 2-6) y la clave de merge
            archivo2_merge = archivo2_prep.iloc[:, [2, 3, 4, 5, 6]].copy()
            archivo2_merge['merge_key'] = archivo2_prep['merge_key']
            
            # Renombrar columnas para evitar conflictos
            archivo2_merge.columns = ['Archivo2_C', 'Archivo2_D', 'Archivo2_E', 'Archivo2_F', 'Archivo2_G', 'merge_key']
            
            # Realizar merge
            resultado_final = resultado_final.merge(
                archivo2_merge, 
                left_on='merge_key_k', 
                right_on='merge_key', 
                how='left'
            )
            
            # Limpiar columna temporal
            resultado_final = resultado_final.drop('merge_key', axis=1)
            
            print(f"   ✅ Merge con archivo 2 completado: {resultado_final.shape[0]} filas, {resultado_final.shape[1]} columnas")
        else:
            print("   ❌ Archivo 2: No tiene suficientes columnas")
        
        # PASO 3: Merge con archivo 3 (columnas C-G donde columna A coincida con columna K de archivo1)
        print("📋 Paso 3: Merge con archivo 3 (columnas C-G)")
        if archivo3.shape[1] >= 7:
            # Preparar archivo3 para merge
            archivo3_prep = archivo3.copy()
            archivo3_prep['merge_key'] = archivo3_prep.iloc[:, 0].fillna('').astype(str).str.strip().str.upper()
            
            # Seleccionar solo las columnas C-G (índices 2-6) y la clave de merge
            archivo3_merge = archivo3_prep.iloc[:, [2, 3, 4, 5, 6]].copy()
            archivo3_merge['merge_key'] = archivo3_prep['merge_key']
            
            # Renombrar columnas para evitar conflictos
            archivo3_merge.columns = ['Archivo3_C', 'Archivo3_D', 'Archivo3_E', 'Archivo3_F', 'Archivo3_G', 'merge_key']
            
            # Realizar merge
            resultado_final = resultado_final.merge(
                archivo3_merge, 
                left_on='merge_key_k', 
                right_on='merge_key', 
                how='left'
            )
            
            # Limpiar columna temporal
            resultado_final = resultado_final.drop('merge_key', axis=1)
            
            print(f"   ✅ Merge con archivo 3 completado: {resultado_final.shape[0]} filas, {resultado_final.shape[1]} columnas")
        else:
            print("   ❌ Archivo 3: No tiene suficientes columnas")
        
        # PASO 4: Merge con archivo 4 (columnas E-H desde fila 8, donde columna A coincida con columna A de archivo1)
        print("📋 Paso 4: Merge con archivo 4 (columnas E-H desde fila 8)")
        if archivo4.shape[1] >= 8 and archivo4.shape[0] >= 8:
            # Tomar desde la fila 8 (índice 7)
            archivo4_desde_fila8 = archivo4.iloc[7:].copy()
            
            if not archivo4_desde_fila8.empty:
                # Preparar archivo4 para merge
                archivo4_prep = archivo4_desde_fila8.copy()
                archivo4_prep['merge_key'] = archivo4_prep.iloc[:, 0].fillna('').astype(str).str.strip().str.upper()
                
                # Seleccionar solo las columnas E-H (índices 4-7) y la clave de merge
                archivo4_merge = archivo4_prep.iloc[:, [4, 5, 6, 7]].copy()
                archivo4_merge['merge_key'] = archivo4_prep['merge_key']
                
                # Renombrar columnas para evitar conflictos
                archivo4_merge.columns = ['Archivo4_E', 'Archivo4_F', 'Archivo4_G', 'Archivo4_H', 'merge_key']
                
                # Realizar merge usando columna A del archivo1
                resultado_final = resultado_final.merge(
                    archivo4_merge, 
                    left_on='merge_key_a', 
                    right_on='merge_key', 
                    how='left'
                )
                
                # Limpiar columna temporal
                resultado_final = resultado_final.drop('merge_key', axis=1)
                
                print(f"   ✅ Merge con archivo 4 completado: {resultado_final.shape[0]} filas, {resultado_final.shape[1]} columnas")
            else:
                print("   ❌ Archivo 4: No hay datos desde la fila 8")
        else:
            print("   ❌ Archivo 4: No tiene suficientes columnas o filas")
        
        # PASO 5: Limpiar columnas temporales de merge
        print("📋 Paso 5: Limpiando columnas temporales")
        resultado_final = resultado_final.drop(['merge_key_k', 'merge_key_a'], axis=1, errors='ignore')
        
        print(f"   ✅ Datos cruzados listos: {resultado_final.shape[0]} filas, {resultado_final.shape[1]} columnas")
        
        # PASO 6: Definir títulos y ajustar al número de columnas
        print("📋 Paso 6: Preparando títulos para la estructura final")
        
        # Definir los títulos en el orden especificado
        titulos = [
            "ID del Cliente",
            "Cliente", 
            "Tipo de Documento",
            "Serie del Documento",
            "ID del Documento",
            "Total exento",
            "Total neto",
            "Total IVA",
            "Red",
            "Total por cliente",
            "ID del Articulo",
            "Articulo",
            "Cantidad del Articulo",
            "Precio del Articulo (Diario de Ventas)",
            "Descuento por Aritculo",
            "Total por Articulo (Diario de Ventas)",
            "Fecha del Documento",
            "Precio de Venta en Pesos",
            "Precio de Venta en Dolares",
            "Precio de Compra en Pesos",
            "Precio de Compra en Dolares",
            "Stock",
            "IVA (%)",
            "Proveedor",
            "Marca",
            "Categoria",
            "Seccion",
            "Ciudad",
            "Departamento",
            "Categoria",
            "Vendedor"
        ]
        
        # Ajustar títulos al número exacto de columnas de datos
        num_columnas_datos = resultado_final.shape[1]
        
        if len(titulos) < num_columnas_datos:
            # Agregar títulos vacíos si faltan
            titulos_ajustados = titulos + [f'Columna_{i}' for i in range(len(titulos), num_columnas_datos)]
        elif len(titulos) > num_columnas_datos:
            # Truncar títulos si sobran
            titulos_ajustados = titulos[:num_columnas_datos]
        else:
            titulos_ajustados = titulos
        
        print(f"   📋 Títulos ajustados: {len(titulos_ajustados)} títulos para {num_columnas_datos} columnas de datos")
        
        # PASO 7: Crear estructura final SIMPLE - Títulos arriba, datos abajo
        print("📋 Paso 7: Creando estructura final - Títulos en fila 1, datos puros debajo")

        # Obtener datos puros como lista de listas (SIN HEADERS ORIGINALES)
        datos_puros = resultado_final.values.tolist()
        
        # Crear lista final: SOLO títulos personalizados + datos puros
        datos_finales = [titulos_ajustados] + datos_puros
        
        # Crear DataFrame simple
        df_final = pd.DataFrame(datos_finales)
        
        print(f"   ✅ Estructura final creada:")
        print(f"   📊 Total: {df_final.shape[0]} filas × {df_final.shape[1]} columnas")
        print(f"   📊 Fila 1: Títulos personalizados únicos")
        print(f"   📊 Filas 2-{df_final.shape[0]}: {len(datos_puros)} filas de datos puros (sin headers originales)")
        
        # Generar archivo de salida
        output_filename = f"Cruce_Ventas_{datetime.now().strftime('%d_%m_%Y_%H%M%S')}.xlsx"
        output_path = os.path.join("downloads", output_filename)
        
        # Crear directorio de salida si no existe
        os.makedirs("downloads", exist_ok=True)
        
        print(f"💾 Guardando archivo en: {output_path}")
        
        # Guardar archivo final sin headers ni índices de pandas
        df_final.to_excel(output_path, index=False, header=False, engine='openpyxl')
        
        # Verificar que el archivo se creó correctamente
        if os.path.exists(output_path):
            file_size = os.path.getsize(output_path)
            print(f"✅ Archivo creado exitosamente: {output_path} ({file_size} bytes)")
        else:
            raise Exception("El archivo no se pudo crear")
        
        print(f"✅ Cruce de ventas completado exitosamente!")
        print(f"📋 Estructura: Fila 1 = Títulos únicos, Filas 2+ = Datos cruzados SIN headers originales")
        print(f"💾 Archivo guardado: {output_path}")
        
        return output_path
        
    except Exception as e:
        print(f"❌ Error en cruce de ventas: {str(e)}")
        import traceback
        traceback.print_exc()
        raise Exception(f"Error procesando cruce de ventas: {str(e)}")

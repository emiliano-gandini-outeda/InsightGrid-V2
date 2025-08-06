import os
import pandas as pd
import tempfile

def process_file(filepath, original_filename=None):
    """
    Procesa un archivo de lista de precios extrayendo información de artículos
    
    Args:
        filepath (str): Ruta del archivo a procesar
        original_filename (str, optional): Nombre original del archivo (para casos donde filepath es temporal)
        
    Returns:
        str: Ruta del archivo procesado
    """
    try:
        print(f"🚀 Iniciando procesamiento de: {filepath}")
        
        # Verificar que el archivo existe
        if not os.path.exists(filepath):
            raise RuntimeError(f"El archivo no existe: {filepath}")
        
        # Leer el archivo según su extensión
        ext = os.path.splitext(filepath)[1].lower()
        print(f"📄 Extensión detectada: {ext}")
        
        if ext == ".csv":
            print("📖 Leyendo archivo CSV...")
            df = pd.read_csv(filepath, header=None, encoding='utf-8')
        elif ext in [".xls", ".xlsx"]:
            print(f"📖 Leyendo archivo Excel ({ext})...")
            # Especificar engine para compatibilidad
            if ext == ".xls":
                df = pd.read_excel(filepath, header=None, engine='xlrd')
            else:
                df = pd.read_excel(filepath, header=None, engine='openpyxl')
        else:
            raise RuntimeError(f"Formato de archivo no soportado: {ext}")
        
        print(f"✅ Archivo leído correctamente")

        output_rows = []
        
        print(f"📋 Iniciando procesamiento de datos...")

        # Mapeo de columnas (pandas usa índice 0, por lo que restamos 1)
        # A=0, B=1, C=2, D=3, E=4, F=5, G=6, H=7, I=8, J=9, K=10, L=11, M=12, N=13, O=14, P=15, Q=16, R=17, S=18, T=19, U=20, V=21, W=22, X=23
        columnas_indices = {
            'id_articulo': 1,      # Columna B
            'nombre_articulo': 4,   # Columna E
            'precio_venta_pesos': 10,  # Columna K
            'precio_venta_dolares': 12,  # Columna M
            'precio_compra_pesos': 16,   # Columna Q
            'precio_compra_dolares': 19, # Columna T
            'stock': 23            # Columna X
        }
        
        print(f"🔍 Archivo tiene {len(df)} filas y {len(df.columns)} columnas")
        print(f"📊 Procesando desde la fila 10 (índice 9)...")

        # Procesar desde la línea 10 (índice 9)
        i = 9
        filas_procesadas = 0
        
        while i < len(df):
            # Verificar si la columna B (ID Articulo) está vacía
            try:
                id_articulo_cell = df.iloc[i, columnas_indices['id_articulo']] if columnas_indices['id_articulo'] < len(df.columns) else None
                
                if pd.isna(id_articulo_cell) or str(id_articulo_cell).strip() == "":
                    i += 1
                    continue

                # Extraer todos los datos como strings
                fila_datos = []
                
                for campo in ['id_articulo', 'nombre_articulo', 'precio_venta_pesos', 
                             'precio_venta_dolares', 'precio_compra_pesos', 
                             'precio_compra_dolares', 'stock']:
                    
                    col_index = columnas_indices[campo]
                    
                    if col_index < len(df.columns):
                        valor_celda = df.iloc[i, col_index]
                        
                        if pd.isna(valor_celda):
                            fila_datos.append("")
                        else:
                            # Convertir todo a string y limpiar espacios
                            fila_datos.append(str(valor_celda).strip())
                    else:
                        fila_datos.append("")
                
                output_rows.append(fila_datos)
                filas_procesadas += 1
                
            except Exception as e:
                print(f"⚠️ Error en fila {i+1}: {str(e)}")
            
            i += 1
        
        print(f"✅ Procesadas {filas_procesadas} filas con datos válidos")

        # Definir las columnas del archivo de salida
        columnas_salida = [
            "ID Articulo",
            "Nombre Articulo", 
            "Precio Venta Pesos",
            "Precio Venta Dolares",
            "Precio Compra Pesos",
            "Precio Compra Dolares",
            "Stock"
        ]

        # Crear DataFrame con los datos procesados
        if not output_rows:
            raise RuntimeError("No se encontraron datos válidos para procesar. Verifica que el archivo tenga datos en la columna B a partir de la fila 10.")
        
        df_resultado = pd.DataFrame(output_rows, columns=columnas_salida)
        print(f"📋 DataFrame creado con {len(df_resultado)} filas")

        # Generar nombre del archivo de salida usando el nombre original
        if original_filename:
            # Usar el nombre original proporcionado
            original_name = os.path.splitext(original_filename)[0]
        else:
            # Usar el nombre del archivo actual
            original_name = os.path.splitext(os.path.basename(filepath))[0]
            
        output_filename = f"{original_name}_PROCESADO.xlsx"
        output_path = os.path.join(tempfile.gettempdir(), output_filename)

        # Guardar el archivo procesado
        df_resultado.to_excel(output_path, index=False)
        print(f"💾 Archivo guardado en: {output_path}")
        
        # Verificar que el archivo se creó correctamente
        if not os.path.exists(output_path):
            raise RuntimeError(f"No se pudo crear el archivo de salida: {output_path}")
        
        return output_path

    except Exception as e:
        print(f"❌ Error en process_file: {str(e)}")
        print(f"❌ Tipo de error: {type(e).__name__}")
        import traceback
        print(f"❌ Traceback: {traceback.format_exc()}")
        raise RuntimeError(f"Error procesando archivo lista de precios: {str(e)}")
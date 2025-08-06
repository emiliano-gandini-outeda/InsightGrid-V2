import pandas as pd
import os
from datetime import datetime
import numpy as np

def process_files(input_files: list) -> str:
    """
    Procesar 3 archivos para vinculación triple
    
    Args:
        input_files: Lista de rutas de archivos a procesar (3 archivos)
        
    Returns:
        str: Ruta del archivo procesado
    """
    try:
        if len(input_files) != 3:
            raise Exception(f"Se requieren exactamente 3 archivos, se recibieron {len(input_files)}")
        
        print(f"🔗 Procesando vinculación triple con {len(input_files)} archivos...")
        
        # Leer todos los archivos
        dataframes = []
        for i, input_path in enumerate(input_files):
            print(f"📖 Leyendo archivo {i+1}: {input_path}")
            try:
                if input_path.endswith('.csv'):
                    df = pd.read_csv(input_path, header=None)
                else:
                    df = pd.read_excel(input_path, header=None)
                
                # CORRECCIÓN: Ignorar la primera línea (línea 1) - empezar desde línea 2
                if len(df) > 1:
                    df = df.iloc[1:].reset_index(drop=True)  # Saltar primera fila y resetear índices
                    print(f"   📋 Primera línea ignorada, procesando desde línea 2")
                else:
                    print(f"   ⚠️ Archivo {i+1} tiene solo 1 línea, no se puede ignorar la primera")
                
                # Limpiar datos vacíos
                df = df.dropna(how='all')  # Eliminar filas completamente vacías
                df = df.fillna('')  # Rellenar NaN con strings vacíos
                
                dataframes.append(df)
                print(f"   ✅ Archivo {i+1} leído exitosamente: {df.shape[0]} filas, {df.shape[1]} columnas (después de ignorar línea 1)")
                
                # Debug: mostrar primeras filas (que ahora son las líneas 2, 3, 4 del archivo original)
                print(f"   📋 Primeras 3 filas del archivo {i+1} (líneas 2-4 del archivo original):")
                for idx in range(min(3, len(df))):
                    row_data = [str(df.iloc[idx, j])[:20] if j < df.shape[1] else '' for j in range(min(6, df.shape[1]))]
                    print(f"      Fila {idx+1}: {row_data}")
                    
            except Exception as e:
                print(f"   ❌ Error leyendo archivo {i+1}: {str(e)}")
                raise Exception(f"Error leyendo archivo {i+1}: {str(e)}")
        
        if len(dataframes) != 3:
            raise Exception(f"Error: Solo se pudieron leer {len(dataframes)} archivos de 3 requeridos")
        
        archivo1, archivo2, archivo3 = dataframes
        
        print("🔍 Iniciando proceso de vinculación triple...")
        
        # PASO 1: Tomar columnas A a F del archivo 2
        print("📋 Paso 1: Extrayendo columnas A-F del archivo 2")
        if archivo2.shape[1] < 6:
            raise Exception("El archivo 2 debe tener al menos 6 columnas (A-F)")
        
        # Extraer columnas A-F (índices 0-5) del archivo 2
        archivo2_data = archivo2.iloc[:, 0:6].copy()
        archivo2_data.columns = ['A2_ColA', 'A2_ColB', 'A2_ColC', 'A2_ColD', 'A2_ColE', 'A2_ColF']
        
        # Limpiar y preparar datos del archivo 2
        archivo2_data = archivo2_data.astype(str)
        # Usar columna C del archivo 2 para búsqueda
        archivo2_data['key_search'] = archivo2_data['A2_ColC'].str.strip().str.upper()
        
        print(f"   ✅ Archivo 2 - Columnas A-F extraídas: {archivo2_data.shape[0]} filas")
        print(f"   📋 Muestra de datos del archivo 2:")
        for idx in range(min(3, len(archivo2_data))):
            print(f"      Fila {idx+1}: A={archivo2_data.iloc[idx]['A2_ColA'][:15]}, B={archivo2_data.iloc[idx]['A2_ColB'][:15]}, C={archivo2_data.iloc[idx]['A2_ColC'][:15]}")
        
        # PASO 2: Buscar coincidencias entre columna C del archivo 2 y columna A del archivo 1
        print("📋 Paso 2: Buscando coincidencias entre columna C del archivo 2 y columna A del archivo 1")
        
        if archivo1.shape[1] < 7:
            raise Exception("El archivo 1 debe tener al menos 7 columnas (A, C-G)")
        
        # Preparar archivo 1 - tomar columna A y columnas C-G
        archivo1_data = pd.DataFrame()
        archivo1_data['A1_ColA'] = archivo1.iloc[:, 0].astype(str)  # Columna A
        archivo1_data['A1_ColC'] = archivo1.iloc[:, 2].astype(str)  # Columna C
        archivo1_data['A1_ColD'] = archivo1.iloc[:, 3].astype(str)  # Columna D
        archivo1_data['A1_ColE'] = archivo1.iloc[:, 4].astype(str)  # Columna E
        archivo1_data['A1_ColF'] = archivo1.iloc[:, 5].astype(str)  # Columna F
        archivo1_data['A1_ColG'] = archivo1.iloc[:, 6].astype(str)  # Columna G
        # Usar columna A del archivo 1 para búsqueda
        archivo1_data['key_search'] = archivo1_data['A1_ColA'].str.strip().str.upper()
        
        print(f"   ✅ Archivo 1 - Columnas A,C-G extraídas: {archivo1_data.shape[0]} filas")
        print(f"   📋 Muestra de datos del archivo 1:")
        for idx in range(min(3, len(archivo1_data))):
            print(f"      Fila {idx+1}: A={archivo1_data.iloc[idx]['A1_ColA'][:15]}, C={archivo1_data.iloc[idx]['A1_ColC'][:15]}")
        
        # Realizar merge entre archivo 2 (columna C) y archivo 1 (columna A)
        resultado_paso2 = archivo2_data.merge(
            archivo1_data,
            on='key_search',
            how='inner'  # Solo coincidencias
        )
        
        print(f"   ✅ Coincidencias encontradas entre columna C del archivo 2 y columna A del archivo 1: {resultado_paso2.shape[0]} registros")
        
        if resultado_paso2.empty:
            print("   ⚠️ No se encontraron coincidencias entre columna C del archivo 2 y columna A del archivo 1")
            print("   📋 Claves de búsqueda en archivo 2 - columna C (primeras 5):")
            for idx in range(min(5, len(archivo2_data))):
                print(f"      '{archivo2_data.iloc[idx]['key_search']}'")
            print("   📋 Claves de búsqueda en archivo 1 - columna A (primeras 5):")
            for idx in range(min(5, len(archivo1_data))):
                print(f"      '{archivo1_data.iloc[idx]['key_search']}'")
        
        # PASO 3: Extraer primera palabra de columna B del archivo 2 y buscar en archivo 3
        print("📋 Paso 3: Procesando vinculación con archivo 3")
        
        def extraer_primera_palabra(texto):
            try:
                if pd.isna(texto) or str(texto).strip() == '':
                    return ''
                return str(texto).strip().split()[0].upper()
            except:
                return ''
        
        # Extraer primera palabra de columna B del resultado del paso 2
        resultado_paso2['primera_palabra_b2'] = resultado_paso2['A2_ColB'].apply(extraer_primera_palabra)
        
        print(f"   📋 Primeras palabras extraídas de columna B archivo 2:")
        palabras_unicas = resultado_paso2['primera_palabra_b2'].unique()[:5]
        for palabra in palabras_unicas:
            print(f"      '{palabra}'")
        
        # Procesar archivo 3
        if archivo3.shape[1] >= 8:
            # Preparar archivo 3 - columnas A-G y primera palabra de columna H
            archivo3_data = pd.DataFrame()
            for i in range(7):  # Columnas A-G
                col_name = f'A3_Col{chr(65+i)}'  # A3_ColA, A3_ColB, etc.
                archivo3_data[col_name] = archivo3.iloc[:, i].astype(str)
            
            # Extraer primera palabra de columna H
            archivo3_data['primera_palabra_h3'] = archivo3.iloc[:, 7].apply(extraer_primera_palabra)
            
            print(f"   ✅ Archivo 3 - Columnas A-G extraídas: {archivo3_data.shape[0]} filas")
            print(f"   📋 Primeras palabras de columna H archivo 3:")
            palabras_h3 = archivo3_data['primera_palabra_h3'].unique()[:5]
            for palabra in palabras_h3:
                print(f"      '{palabra}'")
            
            # Realizar merge final
            resultado_final = resultado_paso2.merge(
                archivo3_data,
                left_on='primera_palabra_b2',
                right_on='primera_palabra_h3',
                how='left'  # Mantener todos los registros del paso anterior
            )
            
            print(f"   ✅ Vinculación con archivo 3 completada: {resultado_final.shape[0]} registros")
            
        else:
            print("   ⚠️ Archivo 3 no tiene columna H, continuando sin vinculación con archivo 3")
            resultado_final = resultado_paso2.copy()
        
        # PASO 4: Limpiar columnas temporales y organizar resultado
        print("📋 Paso 4: Organizando resultado final")
        
        # Eliminar columnas de búsqueda temporales
        columnas_a_eliminar = ['key_search', 'primera_palabra_b2', 'primera_palabra_h3']
        for col in columnas_a_eliminar:
            if col in resultado_final.columns:
                resultado_final = resultado_final.drop(col, axis=1)
        
        # Organizar columnas en el orden especificado: Archivo 2 → Archivo 1 → Archivo 3
        cols_archivo2 = [col for col in resultado_final.columns if col.startswith('A2_')]
        cols_archivo1 = [col for col in resultado_final.columns if col.startswith('A1_')]
        cols_archivo3 = [col for col in resultado_final.columns if col.startswith('A3_')]
        
        orden_columnas = cols_archivo2 + cols_archivo1 + cols_archivo3
        resultado_final = resultado_final[orden_columnas]
        
        print(f"   ✅ Columnas organizadas: {len(cols_archivo2)} del archivo 2, {len(cols_archivo1)} del archivo 1, {len(cols_archivo3)} del archivo 3")
        
        # PASO 5: Crear títulos descriptivos (ESTOS SE MANTIENEN - NO SE BORRAN)
        print("📋 Paso 5: Creando títulos descriptivos")
        
        titulos_finales = []
        
        # Títulos para archivo 2 (A-F)
        titulos_finales.extend([
            'ID Vendedor', 'Nombre Vendedor', 'ID Articulo', 
            'Descripcion Articulo', 'Cantidad', 'Monto Sin IVA'
        ])
        
        # Títulos para archivo 1 (A, C-G)
        titulos_finales.extend([
            'ID Articulo', 'Precio Venta Pesos', 'Precio Venta Dolares', 
            'Precio Compra Pesos', 'Precio Compra Dolares', 'Stock'
        ])
        
        # Títulos para archivo 3 (A-G) si existen
        if cols_archivo3:
            titulos_finales.extend([
                'ID Cliente', 'Nombre', 'RUC', 
                'Razon Social', 'Ciudad - Juan', 'Departamento - Juan', 'Categoria - Juan'
            ])
        
        # Ajustar títulos al número exacto de columnas
        num_columnas = resultado_final.shape[1]
        if len(titulos_finales) > num_columnas:
            titulos_finales = titulos_finales[:num_columnas]
        elif len(titulos_finales) < num_columnas:
            titulos_finales.extend([f'Columna_Extra_{i}' for i in range(len(titulos_finales), num_columnas)])
        
        print(f"   ✅ Títulos descriptivos creados: {len(titulos_finales)} títulos para {num_columnas} columnas")
        print(f"   📋 IMPORTANTE: Los títulos descriptivos se mantienen en el archivo final")
        
        # PASO 6: Crear estructura final con datos reales
        print("📋 Paso 6: Creando archivo final")
        
        if resultado_final.empty:
            raise Exception("No se generaron datos vinculados. Verifique que los archivos tengan datos coincidentes.")
        
        # Convertir datos a lista de listas
        datos_vinculados = resultado_final.values.tolist()
        
        # IMPORTANTE: Crear estructura final manteniendo los títulos descriptivos
        # Los títulos descriptivos van en la primera fila, seguidos de los datos
        estructura_final = [titulos_finales] + datos_vinculados
        
        # Crear DataFrame final
        df_final = pd.DataFrame(estructura_final)
        
        print(f"   ✅ Estructura final creada:")
        print(f"   📊 Total: {df_final.shape[0]} filas × {df_final.shape[1]} columnas")
        print(f"   📊 Fila 1: Títulos descriptivos (MANTENIDOS)")
        print(f"   📊 Filas 2-{df_final.shape[0]}: {len(datos_vinculados)} filas de datos vinculados")
        print(f"   📋 NOTA: Se ignoraron las líneas 1 de los archivos originales, pero se mantuvieron los títulos descriptivos")
        
        # Mostrar muestra de datos finales
        print(f"   📋 Muestra de datos finales (primeras 2 filas de datos):")
        for idx in range(min(2, len(datos_vinculados))):
            fila_muestra = [str(val)[:15] for val in datos_vinculados[idx][:6]]  # Primeras 6 columnas
            print(f"      Fila datos {idx+1}: {fila_muestra}")
        
        # Generar archivo de salida
        output_filename = f"Diario de Ventas x Vendedor - Vinculado_{datetime.now().strftime('%d_%m_%Y_%H%M%S')}.xlsx"
        output_path = os.path.join("downloads", output_filename)
        
        # Crear directorio de salida si no existe
        os.makedirs("downloads", exist_ok=True)
        
        print(f"💾 Guardando archivo en: {output_path}")
        
        # Guardar archivo final (SIN header=False para mantener los títulos descriptivos)
        df_final.to_excel(output_path, index=False, header=False, engine='openpyxl')
        
        # Verificar que el archivo se creó correctamente
        if os.path.exists(output_path):
            file_size = os.path.getsize(output_path)
            print(f"✅ Archivo creado exitosamente: {output_path} ({file_size} bytes)")
        else:
            raise Exception("El archivo no se pudo crear")
        
        print(f"✅ Vinculación triple completada exitosamente!")
        print(f"📋 Estructura: Archivo 2 (A-F) → Archivo 1 (A,C-G) → Archivo 3 (A-G)")
        print(f"📊 Lógica de vinculación: Archivo2_ColC ↔ Archivo1_ColA, luego Archivo2_ColB(1ª palabra) ↔ Archivo3_ColH(1ª palabra)")
        print(f"📊 Procesamiento: Se ignoró línea 1 de cada archivo, se mantuvieron títulos descriptivos")
        print(f"📊 Datos procesados: {len(datos_vinculados)} filas vinculadas")
        print(f"💾 Archivo guardado: {output_path}")
        
        return output_path
        
    except Exception as e:
        print(f"❌ Error en vinculación triple: {str(e)}")
        import traceback
        traceback.print_exc()
        raise Exception(f"Error procesando vinculación triple: {str(e)}")

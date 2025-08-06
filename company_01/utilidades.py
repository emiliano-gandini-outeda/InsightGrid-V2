import os
import pandas as pd
import re

def process_file(filepath):
    """
    Procesa un archivo de análisis de ventas (.csv, .xls, .xlsx) y genera un reporte
    con cálculo de costo promedio.
    
    Args:
        filepath (str): Ruta al archivo a procesar
        
    Returns:
        str: Ruta del archivo procesado generado
        
    Raises:
        RuntimeError: Si hay error en el procesamiento
    """
    try:
        print(f"🔄 Procesando archivo de utilidades: {filepath}")
        
        # Determinar extensión y leer archivo
        ext = os.path.splitext(filepath)[1].lower()
        if ext == ".csv":
            df = pd.read_csv(filepath, header=None)
        elif ext in [".xls", ".xlsx"]:
            df = pd.read_excel(filepath, header=None)
        else:
            raise ValueError(f"Formato de archivo no soportado: {ext}")

        print(f"📊 Archivo leído: {len(df)} filas, {len(df.columns)} columnas")
        
        output_rows = []
        filas_procesadas = 0

        # Iterar sobre las filas del dataframe comenzando desde la línea 8 (índice 7)
        for i in range(7, len(df)):
            try:
                # Extraer datos de cada columna (A=0, B=1, C=2, etc.)
                articulo = str(df.iloc[i, 0]).strip() if pd.notna(df.iloc[i, 0]) else ""
                descripcion = str(df.iloc[i, 1]).strip() if pd.notna(df.iloc[i, 1]) and len(df.columns) > 1 else ""
                
                # Saltar filas vacías o sin artículo válido
                if not articulo or articulo.lower() in ["nan", "none", ""]:
                    continue
                
                # Procesar valores numéricos de las columnas C a G (índices 2 a 6)
                stock_actual = parse_numeric_value(df.iloc[i, 2] if len(df.columns) > 2 else None)
                unidades_vendidas = parse_numeric_value(df.iloc[i, 3] if len(df.columns) > 3 else None)
                importe_venta = parse_numeric_value(df.iloc[i, 4] if len(df.columns) > 4 else None)
                costo_venta_directo = parse_numeric_value(df.iloc[i, 5] if len(df.columns) > 5 else None)
                utilidad_neta = parse_numeric_value(df.iloc[i, 6] if len(df.columns) > 6 else None)
                
                # Calcular costo promedio: Costo Venta Directo / Unidades Vendidas
                if unidades_vendidas > 0:
                    costo_promedio = round(costo_venta_directo / unidades_vendidas, 2)
                else:
                    costo_promedio = 0.00
                
                # Formatear todos los valores a 2 decimales
                stock_actual = round(stock_actual, 2)
                unidades_vendidas = round(unidades_vendidas, 2)
                importe_venta = round(importe_venta, 2)
                costo_venta_directo = round(costo_venta_directo, 2)
                utilidad_neta = round(utilidad_neta, 2)
                
                # Crear fila de salida con todas las columnas solicitadas
                fila = [
                    articulo,
                    descripcion,
                    stock_actual,
                    unidades_vendidas,
                    importe_venta,
                    costo_venta_directo,
                    utilidad_neta,
                    costo_promedio  # Nueva columna calculada
                ]
                output_rows.append(fila)
                filas_procesadas += 1
                
            except Exception as e:
                print(f"⚠️ Error procesando fila {i+1}: {str(e)}")
                continue

        if not output_rows:
            raise RuntimeError("No se encontraron datos válidos para procesar")

        # Definir columnas del reporte final
        columnas = [
            "Articulo",
            "Descripcion", 
            "Stock Actual",
            "Unidades Vendidas",
            "Importe Venta | Venta Promedio",
            "Costo Venta Directo",
            "Utilidad Neta",
            "Costo Promedio"  # Nueva columna
        ]

        # Crear DataFrame resultado
        df_resultado = pd.DataFrame(output_rows, columns=columnas)

        # Generar ruta de salida con el sufijo _PROCESADO
        original_dir = os.path.dirname(filepath)
        original_name = os.path.splitext(os.path.basename(filepath))[0]
        output_filename = f"{original_name}_PROCESADO.xlsx"
        output_path = os.path.join(original_dir, output_filename)

        # Guardar archivo Excel con formato específico
        with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
            df_resultado.to_excel(writer, index=False, sheet_name='Utilidades_Procesado')
            
            # Obtener la hoja de trabajo para aplicar formato
            worksheet = writer.sheets['Utilidades_Procesado']
            
            # Ajustar ancho de columnas
            for column in worksheet.columns:
                max_length = 0
                column_letter = column[0].column_letter
                for cell in column:
                    try:
                        if len(str(cell.value)) > max_length:
                            max_length = len(str(cell.value))
                    except:
                        pass
                adjusted_width = min(max_length + 2, 50)
                worksheet.column_dimensions[column_letter].width = adjusted_width

        print(f"✅ Archivo procesado exitosamente: {output_filename}")
        print(f"📈 Total de registros procesados: {filas_procesadas}")
        print(f"💾 Archivo guardado en: {output_path}")
        
        return output_path

    except Exception as e:
        error_msg = f"Error procesando archivo de utilidades: {str(e)}"
        print(f"❌ {error_msg}")
        raise RuntimeError(error_msg)


def parse_numeric_value(valor):
    """
    Convierte un valor a float, manejando diferentes formatos numéricos.
    
    Args:
        valor: Valor a convertir
        
    Returns:
        float: Valor numérico convertido o 0.0 si no se puede convertir
    """
    if pd.isna(valor) or valor is None:
        return 0.0
    
    valor_str = str(valor).strip()
    
    if valor_str == "" or valor_str == "-" or valor_str.lower() == "nan":
        return 0.0
    
    # Remover espacios y caracteres no numéricos excepto puntos, comas y signos
    valor_str = re.sub(r'[^\d.,-]', '', valor_str)
    
    if not valor_str:
        return 0.0
    
    # Formato argentino con separador de miles (.) y decimales (,): 1.234,56
    if re.match(r'^\d{1,3}(\.\d{3})*,\d{1,2}$', valor_str):
        try:
            return float(valor_str.replace(".", "").replace(",", "."))
        except:
            return 0.0
    
    # Formato con coma como separador de miles y punto como decimal: 1,234.56
    if re.match(r'^\d{1,3}(,\d{3})*\.\d{1,2}$', valor_str):
        try:
            return float(valor_str.replace(",", ""))
        except:
            return 0.0
    
    # Formato solo con coma como decimal: 123,45
    if re.match(r'^\d+,\d{1,2}$', valor_str):
        try:
            return float(valor_str.replace(",", "."))
        except:
            return 0.0
    
    # Intentar conversión directa
    try:
        return float(valor_str)
    except:
        return 0.0


def validate_input_file(filepath):
    """
    Valida que el archivo de entrada tenga el formato correcto.
    
    Args:
        filepath (str): Ruta al archivo a validar
        
    Returns:
        bool: True si el archivo es válido
        
    Raises:
        ValueError: Si el archivo no es válido
    """
    if not os.path.exists(filepath):
        raise ValueError("El archivo no existe")
    
    ext = os.path.splitext(filepath)[1].lower()
    if ext not in ['.csv', '.xls', '.xlsx']:
        raise ValueError("Formato de archivo no soportado. Use .csv, .xls o .xlsx")
    
    try:
        if ext == ".csv":
            df = pd.read_csv(filepath, header=None, nrows=10)
        else:
            df = pd.read_excel(filepath, header=None, nrows=10)
        
        if len(df.columns) < 7:
            raise ValueError("El archivo debe tener al menos 7 columnas (A-G)")
        
        if len(df) < 8:
            raise ValueError("El archivo debe tener al menos 8 filas de datos")
        
        return True
        
    except Exception as e:
        raise ValueError(f"Error validando archivo: {str(e)}")


# Ejemplo de uso y testing
if __name__ == "__main__":
    print("🚀 Procesador de Utilidades - EGO Project")
    print("=" * 50)
    
    # Ejemplo de cómo usar la función
    archivo_entrada = input("Ingresa la ruta del archivo a procesar: ").strip()
    
    if not archivo_entrada:
        print("❌ Error: Debe especificar una ruta de archivo.")
        exit(1)
    
    try:
        # Validar archivo de entrada
        validate_input_file(archivo_entrada)
        print("✅ Archivo de entrada validado correctamente")
        
        # Procesar archivo
        archivo_salida = process_file(archivo_entrada)
        print(f"🎉 ¡Procesamiento completado exitosamente!")
        print(f"📁 Archivo generado: {archivo_salida}")
        
    except ValueError as e:
        print(f"❌ Error de validación: {e}")
    except RuntimeError as e:
        print(f"❌ Error durante el procesamiento: {e}")
    except Exception as e:
        print(f"❌ Error inesperado: {e}")

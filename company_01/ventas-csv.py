import pandas as pd
import os
import re
from io import BytesIO

def clean_numeric_value(value):
    """Convierte un valor numérico a float con 2 decimales, maneja formato con comas"""
    try:
        if value is None or pd.isna(value):
            return 0.00
        
        str_value = str(value).strip()
        
        if str_value == '' or str_value.lower() in ['nan', 'none', 'null']:
            return 0.00
        
        # Remover comas de miles (ej: "7,904.56" -> "7904.56")
        str_value = str_value.replace(',', '')
        
        try:
            float_val = float(str_value)
            return round(float_val, 2)
        except (ValueError, TypeError):
            return 0.00
            
    except (ValueError, TypeError):
        return 0.00

def clean_value_as_string(value):
    """Convierte cualquier valor a string limpio"""
    try:
        if value is None or pd.isna(value):
            return ""
        
        str_value = str(value).strip()
        
        if str_value.lower() in ['nan', 'none', 'null']:
            return ""
        
        if str_value.endswith('.0'):
            str_value = str_value[:-2]
            
        return str_value
    except Exception:
        return ""

def extract_client_data(client_value):
    """Extrae ID del cliente y razón social (ej: '18068 Andres Martinez')"""
    try:
        str_value = str(client_value).strip()
        space_index = str_value.find(' ')
        if space_index > 0:
            id_cliente = str_value[:space_index]
            razon_social = str_value[space_index + 1:].strip()
            return id_cliente, razon_social
        return str_value, ""
    except Exception:
        return "", ""

def parse_date(date_str):
    """Convierte fecha del formato dd/mm/aaaa hh:mm:ss a dd/mm/aa"""
    try:
        str_value = str(date_str).strip()
        
        # Patrón: dd/mm/aaaa hh:mm:ss
        pattern = r'(\d{1,2})/(\d{1,2})/(\d{4})\s+(\d{1,2}):(\d{1,2}):(\d{1,2})'
        match = re.search(pattern, str_value)
        
        if match:
            day, month, year, _, _, _ = match.groups()
            year_short = year[-2:]
            return f"{day.zfill(2)}/{month.zfill(2)}/{year_short}"
        
        return ""
    except Exception:
        return ""

def find_value_after_marker(row_values, marker):
    """Busca un marcador en la lista y devuelve el valor siguiente"""
    try:
        for i, val in enumerate(row_values):
            if str(val).strip() == marker:
                if i + 1 < len(row_values):
                    return row_values[i + 1], i + 1
        return None, None
    except Exception:
        return None, None

def process_csv_diario_ventas(file_path):
    """
    Procesa archivos CSV con formato 'Diario de Ventas Detallado'
    """
    try:
        print("\n" + "="*80)
        print("PROCESANDO CSV DIARIO DE VENTAS")
        print("="*80)
        
        # Leer CSV sin header
        encodings = ['utf-8', 'latin-1', 'iso-8859-1', 'cp1252']
        df = None
        
        for encoding in encodings:
            try:
                df = pd.read_csv(file_path, encoding=encoding, header=None)
                print(f"✓ Archivo leído con encoding: {encoding}")
                break
            except UnicodeDecodeError:
                continue
        
        if df is None:
            raise ValueError("No se pudo leer el archivo CSV con ninguna codificación soportada")
        
        processed_data = []
        
        print(f"Total de filas en CSV: {len(df)}")
        
        for index, row in df.iterrows():
            try:
                # Convertir la fila a lista
                row_values = row.tolist()
                
                print(f"\n--- Procesando fila {index + 1} ---")
                
                # Buscar "Fecha :" como punto de referencia
                fecha_raw, fecha_idx = find_value_after_marker(row_values, "Fecha :")
                
                if fecha_idx is None:
                    print(f"⚠ Fila {index + 1}: No se encontró 'Fecha :', saltando...")
                    continue
                
                print(f"DEBUG: 'Fecha :' encontrado en índice {fecha_idx - 1}, fecha = {fecha_raw}")
                
                fecha_procesada = parse_date(fecha_raw)
                
                cliente_raw = row_values[fecha_idx + 1] if fecha_idx + 1 < len(row_values) else None
                id_cliente, razon_social = extract_client_data(cliente_raw)
                
                tipo_doc = clean_value_as_string(row_values[fecha_idx + 2] if fecha_idx + 2 < len(row_values) else None)
                serie_doc = clean_value_as_string(row_values[fecha_idx + 3] if fecha_idx + 3 < len(row_values) else None)
                id_doc = clean_value_as_string(row_values[fecha_idx + 4] if fecha_idx + 4 < len(row_values) else None)
                exento = clean_numeric_value(row_values[fecha_idx + 5] if fecha_idx + 5 < len(row_values) else None)
                neto = clean_numeric_value(row_values[fecha_idx + 8] if fecha_idx + 8 < len(row_values) else None)
                iva = clean_numeric_value(row_values[fecha_idx + 9] if fecha_idx + 9 < len(row_values) else None)
                red = clean_numeric_value(row_values[fecha_idx + 10] if fecha_idx + 10 < len(row_values) else None)
                total = clean_numeric_value(row_values[fecha_idx + 11] if fecha_idx + 11 < len(row_values) else None)
                id_articulo = clean_value_as_string(row_values[fecha_idx + 13] if fecha_idx + 13 < len(row_values) else None)
                detalle = clean_value_as_string(row_values[fecha_idx + 14] if fecha_idx + 14 < len(row_values) else None)
                cantidad = clean_numeric_value(row_values[fecha_idx + 15] if fecha_idx + 15 < len(row_values) else None)
                precio_unit = clean_numeric_value(row_values[fecha_idx + 16] if fecha_idx + 16 < len(row_values) else None)
                desc1 = clean_numeric_value(row_values[fecha_idx + 17] if fecha_idx + 17 < len(row_values) else None)
                desc2 = clean_numeric_value(row_values[fecha_idx + 18] if fecha_idx + 18 < len(row_values) else None)
                desc3 = clean_numeric_value(row_values[fecha_idx + 19] if fecha_idx + 19 < len(row_values) else None)
                total_desc = clean_numeric_value(row_values[fecha_idx + 21] if fecha_idx + 21 < len(row_values) else None)
                
                print(f"DEBUG: ID Articulo = '{id_articulo}'")
                print(f"DEBUG: Detalle = '{detalle}'")
                print(f"DEBUG: Cantidad = {cantidad}")
                
                # Crear registro
                registro = {
                    'ID del Cliente': id_cliente,
                    'Razon Social': razon_social,
                    'Tipo de Documento': tipo_doc,
                    'Serie del Documento': serie_doc,
                    'ID del Documento': id_doc,
                    'Fecha': fecha_procesada,
                    'Exento': exento,
                    'Total Neto sin IVA': neto,
                    'IVA Total del Documento': iva,
                    'Total del Documento con IVA Incluido': total,
                    'Reduccion': red,
                    'ID de Articulo': id_articulo,
                    'Detalle de Articulo': detalle,
                    'Cantidad Comprada': cantidad,
                    'Precio Unitario': precio_unit,
                    'Descuento 1 (%)': desc1,
                    'Descuento 2 (%)': desc2,
                    'Descuento 3 (%)': desc3,
                    'Total con Descuentos': total_desc
                }
                
                processed_data.append(registro)
                print(f"✓ Registro procesado: Cliente {id_cliente}, Doc {id_doc}, Art '{id_articulo}'")
                
            except Exception as e:
                print(f"✗ Error en fila {index + 1}: {str(e)}")
                continue
        
        print(f"\n{'='*80}")
        print(f"✅ PROCESAMIENTO COMPLETADO: {len(processed_data)} registros procesados")
        print(f"{'='*80}\n")
        
        if not processed_data:
            raise ValueError("No se encontraron datos válidos en el archivo")
        
        return processed_data
        
    except Exception as e:
        raise Exception(f"Error al procesar CSV Diario de Ventas: {str(e)}")

def process_file(filepath, original_filename=None):
    """
    Procesa un archivo CSV de ventas diarias
    
    Args:
        filepath (str): Ruta del archivo a procesar
        original_filename (str, optional): Nombre original del archivo
        
    Returns:
        str: Ruta del archivo procesado
    """
    try:
        print(f"🚀 Iniciando procesamiento de: {filepath}")
        
        # Verificar que el archivo existe
        if not os.path.exists(filepath):
            raise RuntimeError(f"El archivo no existe: {filepath}")
        
        # Procesar el archivo CSV
        processed_data = process_csv_diario_ventas(filepath)
        
        if not processed_data:
            raise RuntimeError("No se encontraron datos válidos en el archivo")
        
        print(f"✅ Datos procesados: {len(processed_data)} registros")
        
        # Crear DataFrame
        df = pd.DataFrame(processed_data)
        
        # Orden de columnas
        column_order = [
            'ID del Cliente',
            'Razon Social',
            'Tipo de Documento',
            'Serie del Documento',
            'ID del Documento',
            'Fecha',
            'Exento',
            'Total Neto sin IVA',
            'IVA Total del Documento',
            'Total del Documento con IVA Incluido',
            'Reduccion',
            'ID de Articulo',
            'Detalle de Articulo',
            'Cantidad Comprada',
            'Precio Unitario',
            'Descuento 1 (%)',
            'Descuento 2 (%)',
            'Descuento 3 (%)',
            'Total con Descuentos'
        ]
        df = df[column_order]
        
        # Generar nombre del archivo de salida usando el nombre original
        if original_filename:
            # Usar el nombre original proporcionado
            original_name = os.path.splitext(original_filename)[0]
        else:
            # Usar el nombre del archivo actual
            original_name = os.path.splitext(os.path.basename(filepath))[0]
            
        output_filename = f"{original_name}_PROCESADO.xlsx"
        output_path = os.path.join(os.path.dirname(filepath), output_filename)
        
        # Guardar el archivo procesado
        with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
            df.to_excel(writer, sheet_name='Ventas Procesadas', index=False)
            
            # Ajustar ancho de columnas
            worksheet = writer.sheets['Ventas Procesadas']
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
        raise RuntimeError(f"Error procesando archivo de ventas CSV: {str(e)}")

# Función para compatibilidad con el sistema web (no es necesaria para el funcionamiento básico)
def process_sales_data_for_webapp(file_bytes, original_filename):
    """
    Función específica para aplicaciones web
    """
    import tempfile
    
    try:
        # Crear archivo temporal
        with tempfile.NamedTemporaryFile(delete=False, suffix='.csv') as temp_file:
            temp_file.write(file_bytes)
            temp_file_path = temp_file.name
        
        try:
            # Procesar el archivo usando la función principal
            output_path = process_file(temp_file_path, original_filename)
            
            # Leer el archivo procesado
            with open(output_path, 'rb') as f:
                processed_bytes = f.read()
            
            output_filename = os.path.basename(output_path)
            
            return processed_bytes, output_filename
            
        finally:
            # Limpiar archivos temporales
            if os.path.exists(temp_file_path):
                os.unlink(temp_file_path)
            if os.path.exists(output_path):
                os.unlink(output_path)
            
    except Exception as e:
        raise Exception(f"Error al procesar archivo para aplicación web: {str(e)}")

# Función principal para pruebas
def main():
    """Función principal para probar el procesamiento"""
    try:
        file_path = input("Ingrese la ruta del archivo CSV (Diario de Ventas): ").strip()
        
        if not os.path.exists(file_path):
            print("Error: El archivo no existe.")
            return
        
        output_path = process_file(file_path)
        print(f"\n¡Procesamiento completado exitosamente!")
        print(f"Archivo guardado en: {output_path}")
        
    except Exception as e:
        print(f"Error durante el procesamiento: {str(e)}")

if __name__ == "__main__":
    main()
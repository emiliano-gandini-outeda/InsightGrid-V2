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
    
    Busca marcadores específicos en lugar de usar posiciones fijas
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
                
                # A partir del índice de fecha, extraer valores en POSICIONES EXACTAS
                # Basado en el ejemplo:
                # "Fecha :", 15/10/2025 00:00:00, "18068 Andres Martinez", "Vta.Cred.", "A", 63201, 0.00, , , 441.80, 97.20, 0.00, 539.00, , "RE296", "Durazno...", 2.00, 60.00, 1.00, 2.00, 3.00, "%", 120.00
                
                # Posiciones relativas desde fecha_idx:
                # 0: fecha
                # 1: cliente
                # 2: tipo_doc
                # 3: serie
                # 4: id_doc
                # 5: exento
                # 6: vacío
                # 7: vacío  
                # 8: neto
                # 9: iva
                # 10: red
                # 11: total
                # 12: vacío
                # 13: id_articulo
                # 14: detalle_articulo
                # 15: cantidad
                # 16: precio_unit
                # 17: desc1
                # 18: desc2
                # 19: desc3
                # 20: "%"
                # 21: total_desc
                
                fecha_procesada = parse_date(fecha_raw)
                
                cliente_raw = row_values[fecha_idx + 1] if fecha_idx + 1 < len(row_values) else None
                id_cliente, razon_social = extract_client_data(cliente_raw)
                
                tipo_doc = clean_value_as_string(row_values[fecha_idx + 2] if fecha_idx + 2 < len(row_values) else None)
                serie_doc = clean_value_as_string(row_values[fecha_idx + 3] if fecha_idx + 3 < len(row_values) else None)
                id_doc = clean_value_as_string(row_values[fecha_idx + 4] if fecha_idx + 4 < len(row_values) else None)
                exento = clean_numeric_value(row_values[fecha_idx + 5] if fecha_idx + 5 < len(row_values) else None)
                # Saltar índices 6 y 7 (vacíos)
                neto = clean_numeric_value(row_values[fecha_idx + 8] if fecha_idx + 8 < len(row_values) else None)
                iva = clean_numeric_value(row_values[fecha_idx + 9] if fecha_idx + 9 < len(row_values) else None)
                red = clean_numeric_value(row_values[fecha_idx + 10] if fecha_idx + 10 < len(row_values) else None)
                total = clean_numeric_value(row_values[fecha_idx + 11] if fecha_idx + 11 < len(row_values) else None)
                # Saltar índice 12 (vacío)
                id_articulo = clean_value_as_string(row_values[fecha_idx + 13] if fecha_idx + 13 < len(row_values) else None)
                detalle = clean_value_as_string(row_values[fecha_idx + 14] if fecha_idx + 14 < len(row_values) else None)
                cantidad = clean_numeric_value(row_values[fecha_idx + 15] if fecha_idx + 15 < len(row_values) else None)
                precio_unit = clean_numeric_value(row_values[fecha_idx + 16] if fecha_idx + 16 < len(row_values) else None)
                desc1 = clean_numeric_value(row_values[fecha_idx + 17] if fecha_idx + 17 < len(row_values) else None)
                desc2 = clean_numeric_value(row_values[fecha_idx + 18] if fecha_idx + 18 < len(row_values) else None)
                desc3 = clean_numeric_value(row_values[fecha_idx + 19] if fecha_idx + 19 < len(row_values) else None)
                # Saltar índice 20 (símbolo "%")
                total_desc = clean_numeric_value(row_values[fecha_idx + 21] if fecha_idx + 21 < len(row_values) else None)
                
                print(f"DEBUG: ID Articulo = '{id_articulo}' (índice {fecha_idx + 13})")
                print(f"DEBUG: Detalle = '{detalle}' (índice {fecha_idx + 14})")
                print(f"DEBUG: Cantidad = {cantidad} (índice {fecha_idx + 15})")
                print(f"DEBUG: Precio Unit = {precio_unit} (índice {fecha_idx + 16})")
                print(f"DEBUG: Desc1 = {desc1}, Desc2 = {desc2}, Desc3 = {desc3}")
                print(f"DEBUG: Total Desc = {total_desc}")
                
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
                import traceback
                traceback.print_exc()
                continue
        
        print(f"\n{'='*80}")
        print(f"✅ PROCESAMIENTO COMPLETADO: {len(processed_data)} registros procesados")
        print(f"{'='*80}\n")
        
        if not processed_data:
            raise ValueError("No se encontraron datos válidos en el archivo")
        
        return processed_data
        
    except Exception as e:
        raise Exception(f"Error al procesar CSV Diario de Ventas: {str(e)}")

def process_file_diario_ventas(file_path, return_bytes=False):
    """
    Función principal compatible con el flujo del script original
    
    Args:
        file_path: Ruta del archivo CSV a procesar
        return_bytes: Si True, devuelve bytes del archivo en lugar de guardarlo
    
    Returns:
        Si return_bytes=True: tupla (bytes_data, filename)
        Si return_bytes=False: ruta del archivo guardado
    """
    try:
        # Procesar el archivo
        processed_data = process_csv_diario_ventas(file_path)
        
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
        
        # Generar nombre de archivo de salida
        base_filename = os.path.splitext(os.path.basename(file_path))[0]
        output_filename = f"{base_filename}_PROCESADO.xlsx"
        
        if return_bytes:
            # Devolver como bytes para aplicaciones web
            output_buffer = BytesIO()
            with pd.ExcelWriter(output_buffer, engine='openpyxl') as writer:
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
            
            output_buffer.seek(0)
            return output_buffer.getvalue(), output_filename
        else:
            # Guardar archivo localmente
            output_path = os.path.join(os.path.dirname(file_path), output_filename)
            
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
            
            return output_path
            
    except Exception as e:
        raise Exception(f"Error al procesar el archivo: {str(e)}")

def process_sales_data_for_webapp_diario(file_bytes, original_filename):
    """
    Función específica para aplicaciones web (compatible con el script original)
    
    Args:
        file_bytes: Bytes del archivo subido
        original_filename: Nombre del archivo original
    
    Returns:
        Tupla (bytes_data, output_filename) del archivo procesado
    """
    import tempfile
    
    try:
        # Crear archivo temporal
        with tempfile.NamedTemporaryFile(delete=False, suffix='.csv') as temp_file:
            temp_file.write(file_bytes)
            temp_file_path = temp_file.name
        
        try:
            # Procesar el archivo
            processed_data = process_csv_diario_ventas(temp_file_path)
            
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
            
            # Generar nombre de archivo
            base_filename = os.path.splitext(original_filename)[0]
            output_filename = f"{base_filename}_PROCESADO.xlsx"
            
            # Generar archivo en memoria
            output_buffer = BytesIO()
            with pd.ExcelWriter(output_buffer, engine='openpyxl') as writer:
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
            
            output_buffer.seek(0)
            return output_buffer.getvalue(), output_filename
            
        finally:
            # Limpiar archivo temporal
            os.unlink(temp_file_path)
            
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
        
        output_path = process_file_diario_ventas(file_path)
        print(f"\n¡Procesamiento completado exitosamente!")
        print(f"Archivo guardado en: {output_path}")
        
    except Exception as e:
        print(f"Error durante el procesamiento: {str(e)}")

if __name__ == "__main__":
    main()
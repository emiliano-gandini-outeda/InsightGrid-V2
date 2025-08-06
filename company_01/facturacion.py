import os
import pandas as pd
import re

# Diario de Facturacion

def process_file(filepath):
    try:
        ext = os.path.splitext(filepath)[1].lower()
        if ext == ".csv":
            df = pd.read_csv(filepath, header=None)
        else:
            df = pd.read_excel(filepath, header=None)

        output_rows = []
        i = 0

        while i < len(df):
            try:
                celda_a_raw = df.iloc[i, 0]
                celda_a = str(celda_a_raw).strip() if pd.notna(celda_a_raw) else ""
            except:
                celda_a = ""

            if celda_a not in ["Vta.Cred.", "Nota Cred.", "Vta.Cont."]:
                i += 1
                continue

            tipo_documento = celda_a
            serie_documento = safe_get_string(df, i, 9)
            numero_documento = safe_get_integer(df, i, 12)
            fecha_documento = safe_get_date(df, i, 21)
            cliente = safe_get_string(df, i, 34)
            descuento_porcentaje = safe_get_float(df, i, 52)
            descuento_pesos = safe_get_float(df, i, 60)
            total_pesos = safe_get_float(df, i, 67)

            i += 1
            if i >= len(df):
                break

            cae_nro = safe_get_integer(df, i, 7)
            cae_venc = safe_get_date(df, i, 26)
            cae_serie = safe_get_string(df, i, 44)
            cae_numero_documento = safe_get_integer(df, i, 49)
            cae_estado = safe_get_string(df, i, 64)

            i += 1
            while i < len(df):
                codigo_articulo = safe_get_string(df, i, 1)
                if not codigo_articulo:
                    try:
                        next_celda_a = str(df.iloc[i, 0]).strip()
                        if next_celda_a in ["Vta.Cred.", "Nota Cred.", "Vta.Cont."]:
                            break
                    except:
                        pass
                    i += 1
                    continue

                articulo = safe_get_string(df, i, 17)
                cantidad_articulo = safe_get_float(df, i, 41)
                precio_unitario = safe_get_float(df, i, 47)

                fila = [
                    cliente,
                    tipo_documento,
                    serie_documento,
                    numero_documento,
                    fecha_documento,
                    cae_nro,
                    cae_serie,
                    cae_numero_documento,
                    cae_estado,
                    codigo_articulo,
                    articulo,
                    cantidad_articulo,
                    precio_unitario,
                    total_pesos,
                    descuento_porcentaje,
                    descuento_pesos
                ]
                output_rows.append(fila)
                i += 1

        columnas = [
            "Cliente",
            "Tipo de Documento",
            "Serie del Documento",
            "Numero del documento",
            "Fecha del documento",
            "CAE Nro",
            "CAE Serie",
            "CAE Numero de documento",
            "CAE Estado",
            "Codigo Articulo",
            "Articulo",
            "Cantidad articulo",
            "Precio unitario Listas",
            "Total en pesos",
            "Descuento en %",
            "Descuento en pesos"
        ]

        df_resultado = pd.DataFrame(output_rows, columns=columnas)

        # Generar ruta de salida en el mismo directorio del archivo original
        original_dir = os.path.dirname(filepath)
        original_name = os.path.splitext(os.path.basename(filepath))[0]
        output_filename = f"{original_name}_PROCESADO.xlsx"
        output_path = os.path.join(original_dir, output_filename)

        df_resultado.to_excel(output_path, index=False)
        return output_path

    except Exception as e:
        raise RuntimeError(f"Error procesando archivo de facturación: {str(e)}")

def safe_get_string(df, row, col):
    try:
        if row < len(df) and col < len(df.columns):
            value = df.iloc[row, col]
            if pd.notna(value):
                return str(value).strip()
        return ""
    except:
        return ""

def safe_get_integer(df, row, col):
    try:
        if row < len(df) and col < len(df.columns):
            value = df.iloc[row, col]
            if pd.notna(value):
                if isinstance(value, str):
                    clean_value = value.strip().replace(".", "").replace(",", ".")
                    return int(float(clean_value))
                return int(value)
        return 0
    except:
        return 0

def safe_get_float(df, row, col):
    try:
        if row < len(df) and col < len(df.columns):
            value = df.iloc[row, col]
            if pd.notna(value):
                if isinstance(value, str):
                    clean_value = value.strip().replace(".", "").replace(",", ".")
                    return float(clean_value)
                return float(value)
        return 0.0
    except:
        return 0.0

def safe_get_date(df, row, col):
    try:
        if row < len(df) and col < len(df.columns):
            value = df.iloc[row, col]
            if pd.notna(value):
                if isinstance(value, str):
                    try:
                        date_obj = pd.to_datetime(value, dayfirst=True)
                        return date_obj.strftime("%d/%m/%Y")
                    except:
                        return value.strip()
                else:
                    try:
                        return pd.to_datetime(value).strftime("%d/%m/%Y")
                    except:
                        return str(value)
        return ""
    except:
        return ""
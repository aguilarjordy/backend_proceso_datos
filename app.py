from flask import Flask, request, jsonify
from flask_cors import CORS
from supabase import create_client, Client
from dotenv import load_dotenv
import datetime
import os
import json
import pandas as pd

# ------------------------------------------------------------
# 🔧 CONFIGURACIÓN INICIAL
# ------------------------------------------------------------
load_dotenv()

app = Flask(__name__)
CORS(app)

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

# Inicializar cliente de Supabase
try:
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
    print("✅ Cliente Supabase inicializado correctamente.")
except Exception as e:
    print(f"❌ Error al inicializar Supabase: {e}")

# ------------------------------------------------------------
# 🚀 1. DATASETS
# ------------------------------------------------------------
@app.route('/api/datasets', methods=['GET'])
def get_datasets():
    response = supabase.table("datasets").select("*").order("fecha_carga", desc=True).execute()
    return jsonify(response.data)


@app.route('/api/datasets', methods=['POST'])
def create_dataset():
    """Registra un nuevo dataset subido (CSV o JSON)."""
    try:
        if 'file' in request.files:
            file = request.files['file']
            filename = file.filename

            temp_path = os.path.join("temp", filename)
            os.makedirs("temp", exist_ok=True)
            file.save(temp_path)

            # Leer CSV o JSON
            if filename.endswith('.csv'):
                try:
                    df = pd.read_csv(temp_path, encoding='utf-8')
                except UnicodeDecodeError:
                    df = pd.read_csv(temp_path, encoding='latin-1')
            elif filename.endswith('.json'):
                df = pd.read_json(temp_path, encoding='utf-8')
            else:
                return jsonify({"message": "Formato no soportado. Solo CSV o JSON."}), 400

            num_filas, num_columnas = df.shape

            storage_path = f"datasets/{filename}"
            with open(temp_path, "rb") as f:
                supabase.storage.from_("datasets").upload(
                    storage_path,
                    f,
                    file_options={"content-type": "text/csv"}
                )

            response = supabase.table("datasets").insert({
                "nombre": filename,
                "ruta_almacenamiento": storage_path,
                "num_filas": num_filas,
                "num_columnas": num_columnas,
                "metadata_json": {},
                "fecha_carga": datetime.datetime.now().isoformat()
            }).execute()

            os.remove(temp_path)
            return jsonify({
                "message": "Dataset subido y registrado correctamente.",
                "id": response.data[0]["id"],
                "nombre": filename
            }), 201

        elif request.is_json:
            data = request.get_json()
            if not data or 'nombre' not in data:
                return jsonify({"message": "Faltan datos requeridos (nombre)."}), 400

            response = supabase.table("datasets").insert({
                "nombre": data['nombre'],
                "ruta_almacenamiento": data.get('ruta_almacenamiento'),
                "num_filas": data.get('num_filas'),
                "num_columnas": data.get('num_columnas'),
                "metadata_json": data.get('metadata_json', {}),
                "fecha_carga": datetime.datetime.now().isoformat()
            }).execute()

            return jsonify({
                "message": "Dataset creado exitosamente desde JSON.",
                "id": response.data[0]['id'],
                "nombre": response.data[0]['nombre']
            }), 201

        else:
            return jsonify({"message": "Debe enviar un archivo o un JSON válido."}), 400

    except Exception as e:
        return jsonify({"message": f"Error al crear dataset: {str(e)}"}), 500


# ------------------------------------------------------------
# 🚀 2. LIMPIEZAS DE DATOS
# ------------------------------------------------------------
@app.route('/api/limpiezas', methods=['GET'])
def get_limpiezas():
    response = supabase.table("limpiezas_datos").select("*").order("fecha_limpieza", desc=True).execute()
    return jsonify(response.data)


@app.route('/api/limpiezas', methods=['POST'])
def limpiar_dataset_multiple():
    try:
        data = request.get_json()
        dataset_id = data.get("dataset_id")
        tipos_limpieza = data.get("tipos_limpieza", [])

        if not dataset_id or not tipos_limpieza:
            return jsonify({"error": "Debe incluir dataset_id y una lista de tipos_limpieza."}), 400

        dataset_res = supabase.table("datasets").select("*").eq("id", dataset_id).single().execute()
        if not dataset_res.data:
            return jsonify({"error": "Dataset no encontrado."}), 404

        dataset = dataset_res.data
        ruta_dataset = dataset["ruta_almacenamiento"]

        # Descargar dataset original
        response = supabase.storage.from_("datasets").download(ruta_dataset)
        os.makedirs("temp", exist_ok=True)
        temp_path = f"temp/{os.path.basename(ruta_dataset)}"
        with open(temp_path, "wb") as f:
            f.write(response)

        try:
            df = pd.read_csv(temp_path, on_bad_lines='skip', encoding='utf-8')
        except UnicodeDecodeError:
            df = pd.read_csv(temp_path, on_bad_lines='skip', encoding='latin-1')

        operaciones_realizadas = []
        total_afectados = 0
        duplicados_guardados = pd.DataFrame()

        # Aplicar limpiezas
        for limpieza in tipos_limpieza:
            tipo = limpieza.get("tipo")
            parametros = limpieza.get("parametros", {})
            afectados = 0

            if tipo == "duplicados":
                antes = len(df)
                duplicados_guardados = df[df.duplicated()]
                df = df.drop_duplicates()
                afectados = antes - len(df)

            elif tipo == "nulos":
                nulos_antes = df.isnull().sum().sum()
                for col in df.columns:
                    if df[col].dtype == object:
                        df[col] = df[col].fillna("N/A")
                    else:
                        df[col] = df[col].fillna(0)
                afectados = int(nulos_antes)

            elif tipo == "outliers":
                columnas = parametros.get("columnas", list(df.select_dtypes(include=['float64', 'int64']).columns))
                umbral = parametros.get("umbral", 1.5)
                antes = len(df)
                for col in columnas:
                    Q1 = df[col].quantile(0.25)
                    Q3 = df[col].quantile(0.75)
                    IQR = Q3 - Q1
                    lower, upper = Q1 - umbral * IQR, Q3 + umbral * IQR
                    df = df[(df[col] >= lower) & (df[col] <= upper)]
                afectados = antes - len(df)

            operaciones_realizadas.append({
                "tipo": tipo,
                "parametros": parametros,
                "afectados": int(afectados)
            })
            total_afectados += int(afectados)

        # Guardar dataset limpio
        clean_filename = f"clean_multi_{os.path.basename(ruta_dataset)}"
        clean_path = f"temp/{clean_filename}"
        df.to_csv(clean_path, index=False)
        ruta_storage_clean = f"clean/{clean_filename}"

        with open(clean_path, "rb") as f:
            supabase.storage.from_("datasets").upload(
                ruta_storage_clean,
                f,
                file_options={"content-type": "text/csv"}
            )

        # Guardar duplicados si existen
        ruta_storage_dups = None
        if not duplicados_guardados.empty:
            dup_filename = f"duplicates_{os.path.basename(ruta_dataset)}"
            dup_path = f"temp/{dup_filename}"
            duplicados_guardados.to_csv(dup_path, index=False)
            ruta_storage_dups = f"duplicates/{dup_filename}"

            with open(dup_path, "rb") as fdup:
                supabase.storage.from_("datasets").upload(
                    ruta_storage_dups,
                    fdup,
                    file_options={"content-type": "text/csv"}
                )

            # 🟢 Insertar duplicados en la tabla duplicados_datasets
            supabase.table("duplicados_datasets").insert({
                "dataset_id": int(dataset_id),
                "ruta_duplicados": ruta_storage_dups,
                "num_registros": len(duplicados_guardados)
            }).execute()

        # Generar URLs públicas para ambos archivos
        public_url_clean = supabase.storage.from_("datasets").get_public_url(ruta_storage_clean)
        public_url_dups = supabase.storage.from_("datasets").get_public_url(ruta_storage_dups) if ruta_storage_dups else None

        # Insertar registro principal en limpiezas_datos
        limpieza_insert = supabase.table("limpiezas_datos").insert({
            "dataset_id": int(dataset_id),
            "tipo_limpieza": "multiple",
            "parametros_usados": json.dumps(operaciones_realizadas, default=str),
            "num_registros_afectados": int(total_afectados),
            "ruta_dataset_limpio": ruta_storage_clean,
            "ruta_duplicados": ruta_storage_dups,
            "estado": "Completada"
        }).execute()

        # ✅ Retornar URLs públicas (para que el frontend muestre las tablas correctamente)
        return jsonify({
            "message": "Limpieza múltiple completada correctamente.",
            "operaciones": operaciones_realizadas,
            "total_afectados": int(total_afectados),
            "ruta_dataset_limpio": public_url_clean,
            "ruta_duplicados": public_url_dups,
            "limpieza_id": int(limpieza_insert.data[0]["id"]) if limpieza_insert.data else None
        }), 200

    except Exception as e:
        import traceback
        print("❌ ERROR DETALLADO EN LIMPIEZA:")
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500


# ------------------------------------------------------------
# 🚀 3. ENTRENAMIENTOS
# ------------------------------------------------------------
@app.route('/api/entrenamientos', methods=['POST'])
def create_entrenamiento():
    data = request.get_json()
    required_fields = ['limpieza_id', 'tipo_modelo']
    if not all(field in data for field in required_fields):
        return jsonify({"message": "Faltan limpieza_id y tipo_modelo."}), 400

    try:
        response = supabase.table("entrenamientos").insert({
            "limpieza_id": data['limpieza_id'],
            "tipo_modelo": data['tipo_modelo'],
            "epocas": data.get('epocas'),
            "batch_size": data.get('batch_size'),
            "learning_rate": data.get('learning_rate'),
            "operaciones_limpieza": data.get('operaciones_limpieza', []),
            "estado": 'En Curso',
            "fecha_inicio": datetime.datetime.now().isoformat()
        }).execute()

        return jsonify({
            "message": "Entrenamiento registrado exitosamente.",
            "entrenamiento_id": response.data[0]['id'],
            "estado": response.data[0]['estado']
        }), 201

    except Exception as e:
        return jsonify({"message": f"Error al registrar entrenamiento: {str(e)}"}), 500


@app.route('/api/entrenamientos/<int:entrenamiento_id>', methods=['GET'])
def get_entrenamiento(entrenamiento_id):
    try:
        entrenamiento = supabase.table("entrenamientos").select("*, limpiezas_datos(*, datasets(nombre))").eq("id", entrenamiento_id).limit(1).execute()

        if not entrenamiento.data:
            return jsonify({"message": f"Entrenamiento con ID {entrenamiento_id} no encontrado."}), 404

        datos = entrenamiento.data[0]
        resultado = supabase.table("resultados_metricas").select("*").eq("entrenamiento_id", entrenamiento_id).limit(1).execute()

        detalles = {
            "id": datos["id"],
            "tipo_modelo": datos["tipo_modelo"],
            "estado": datos["estado"],
            "fecha_inicio": datos["fecha_inicio"],
            "fecha_fin": datos["fecha_fin"],
            "dataset_nombre": datos["limpiezas_datos"]["datasets"]["nombre"] if "limpiezas_datos" in datos else "N/A",
            "metricas": resultado.data[0] if resultado.data else {}
        }

        return jsonify(detalles)

    except Exception as e:
        return jsonify({"message": f"Error al obtener detalles del entrenamiento: {str(e)}"}), 500

# ✅ Nuevo endpoint para la pestaña "Historial"
@app.route('/api/entrenamientos/all', methods=['GET'])
def get_all_entrenamientos():
    try:
        response = supabase.table("entrenamientos").select("*, limpiezas_datos(*, datasets(nombre))").order("fecha_inicio", desc=True).execute()
        return jsonify(response.data)
    except Exception as e:
        return jsonify({"message": f"Error al obtener entrenamientos: {str(e)}"}), 500

# ------------------------------------------------------------
# 🚀 4. RESULTADOS Y MÉTRICAS
# ------------------------------------------------------------
@app.route('/api/resultados', methods=['POST'])
def create_resultado():
    data = request.get_json()
    required = ['entrenamiento_id', 'accuracy', 'f1_score', 'loss_final']
    if not all(field in data for field in required):
        return jsonify({"message": "Faltan campos requeridos."}), 400

    try:
        res = supabase.table("resultados_metricas").insert({
            "entrenamiento_id": data['entrenamiento_id'],
            "accuracy": data['accuracy'],
            "f1_score": data['f1_score'],
            "loss_final": data['loss_final'],
            "grafico_accuracy_f1": data.get('grafico_accuracy_f1'),
            "grafico_loss": data.get('grafico_loss'),
            "modelo_guardado": data.get('modelo_guardado')
        }).execute()

        supabase.table("entrenamientos").update({
            "estado": "Finalizado",
            "fecha_fin": datetime.datetime.now().isoformat()
        }).eq("id", data['entrenamiento_id']).execute()

        return jsonify({
            "message": "Resultados guardados y entrenamiento finalizado.",
            "resultado_id": res.data[0]['id']
        }), 201

    except Exception as e:
        return jsonify({"message": f"Error al guardar resultados: {str(e)}"}), 500


# ------------------------------------------------------------
# 🚀 INICIO DE APLICACIÓN
# ------------------------------------------------------------
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)

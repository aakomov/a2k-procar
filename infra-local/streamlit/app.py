
import os
import streamlit as st
import mlflow.sklearn
import pandas as pd

# ── Креды MinIO — прописываем ДО любого обращения к MLflow/boto ──
os.environ["AWS_ACCESS_KEY_ID"]      = "minio"        # ваш ключ
os.environ["AWS_SECRET_ACCESS_KEY"]  = "minio123"        # ваш секрет
os.environ["MLFLOW_S3_ENDPOINT_URL"] = "http://192.168.49.1:9091"  # ваш MinIO
os.environ["AWS_DEFAULT_REGION"]     = "us-east-1"

# ── Настройка MLflow ──
import mlflow
mlflow.set_tracking_uri("http://localhost:5000")

FEATURES = [
    "cpu_usage", "ram_usage", "disk_io", "network_errors",
    "temperature", "uptime_days", "failed_requests",
    "load_index", "is_old_server", "error_rate"
]

st.title("🖥️ Server Failure Predictor")
st.markdown("Введите метрики сервера — модель предскажет вероятность сбоя")

@st.cache_resource
def load_model():
    # Вставьте ваш model_uri из ячейки обучения
    model_uri = "runs:/e35a34a8839c4940a9e85346fb1797ab/model"
    return mlflow.sklearn.load_model(model_uri)

try:
    model = load_model()
    st.success("✅ Модель загружена")
except Exception as e:
    st.error(f"❌ Ошибка загрузки модели: {e}")
    st.stop()

col1, col2 = st.columns(2)
with col1:
    cpu     = st.slider("CPU Usage %",     0, 100, 45)
    ram     = st.slider("RAM Usage %",     0, 100, 55)
    disk    = st.slider("Disk IO %",       0, 100, 30)
    net_err = st.slider("Network Errors",  0, 100, 5)
    temp    = st.slider("Temperature C",   30, 95, 55)
with col2:
    uptime  = st.slider("Uptime (days)",   0, 365, 30)
    failed  = st.slider("Failed Requests", 0, 200, 10)

# Производные признаки — те же что при обучении
load_index = (cpu + ram) / 2
is_old     = int(uptime > 90)
error_rate = net_err / (failed + 1)

features = pd.DataFrame([{
    "cpu_usage": cpu, "ram_usage": ram, "disk_io": disk,
    "network_errors": net_err, "temperature": temp,
    "uptime_days": uptime, "failed_requests": failed,
    "load_index": load_index, "is_old_server": is_old,
    "error_rate": error_rate
}])

if st.button("🔍 Предсказать"):
    prob = model.predict_proba(features)[0][1]
    pred = model.predict(features)[0]

    if pred == 1:
        st.error(f"⚠️ СБОЙ ОЖИДАЕТСЯ | Вероятность: {prob:.1%}")
    else:
        st.success(f"✅ Сервер в норме | Вероятность сбоя: {prob:.1%}")

    st.progress(float(prob))
    
    with st.expander("Детали запроса"):
        st.dataframe(features)

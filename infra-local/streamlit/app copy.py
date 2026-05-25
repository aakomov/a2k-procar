
import streamlit as st
import mlflow.sklearn
import pandas as pd

st.title("🖥️ Server Failure Predictor")
st.markdown("Введите метрики сервера — модель предскажет вероятность сбоя")

# Загружаем модель один раз при старте
@st.cache_resource
def load_model():
    mlflow.set_tracking_uri("http://localhost:5000")
    return mlflow.sklearn.load_model("models:/server-failure-model/1")

model = load_model()

# Интерфейс — слайдеры для каждой метрики
col1, col2 = st.columns(2)
with col1:
    cpu     = st.slider("CPU Usage %",      0, 100, 45)
    ram     = st.slider("RAM Usage %",      0, 100, 55)
    disk    = st.slider("Disk IO %",        0, 100, 30)
    net_err = st.slider("Network Errors",   0, 100, 5)
    temp    = st.slider("Temperature °C",   30, 95, 55)
with col2:
    uptime  = st.slider("Uptime (days)",    0, 365, 30)
    failed  = st.slider("Failed Requests",  0, 200, 10)

# Считаем производные признаки (те же что при обучении!)
load_index  = (cpu + ram) / 2
is_old      = int(uptime > 90)
error_rate  = net_err / (failed + 1)

features = pd.DataFrame([{
    "cpu_usage": cpu, "ram_usage": ram, "disk_io": disk,
    "network_errors": net_err, "temperature": temp,
    "uptime_days": uptime, "failed_requests": failed,
    "load_index": load_index, "is_old_server": is_old, "error_rate": error_rate
}])

if st.button("Предсказать"):
    prob = model.predict_proba(features)[0][1]
    pred = model.predict(features)[0]
    
    if pred == 1:
        st.error(f"⚠️ СБОЙ ОЖИДАЕТСЯ | Вероятность: {prob:.1%}")
    else:
        st.success(f"✅ Сервер в норме | Вероятность сбоя: {prob:.1%}")
    
    st.progress(float(prob))
    st.dataframe(features)

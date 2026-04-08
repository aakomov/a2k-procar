# Определение признаков (feature definition)

import os

from datetime import timedelta

import numpy as np
import pandas as pd

from feast import (
    Entity,
    FeatureService,
    FeatureView,
    Field,
    FileSource,
    PushSource,
    RequestSource,
)
from feast.feature_logging import LoggingConfig
from feast.infra.offline_stores.file_source import FileLoggingDestination
from feast.on_demand_feature_view import on_demand_feature_view
from feast.types import Float32, Float64, Int64

REPO_PATH = os.path.dirname(os.path.abspath(__file__))
DATA_PATH = os.path.join(REPO_PATH, "data")

# Определяем сущность для водителя (первичный ключ)
driver = Entity(name="driver", join_keys=["driver_id"])

# Коннект до источника
driver_stats_source = FileSource(
    name="driver_hourly_stats_source",
    path=os.path.join(DATA_PATH, "driver_stats.parquet"),
    timestamp_field="event_timestamp",
    created_timestamp_column="created",
)

# Определяем Feature View
driver_stats_fv = FeatureView(
    # Уникальное имя
    name="driver_hourly_stats",
    entities=[driver],
    ttl=timedelta(days=1),
    schema=[
        Field(name="conv_rate", dtype=Float32),
        Field(name="acc_rate", dtype=Float32),
        Field(name="avg_daily_trips", dtype=Int64, description="Среднее количество поездок в день"),
    ],
    online=True,
    source=driver_stats_source,
    # Задаем тег
    tags={"team": "driver_performance"},
)

# Определяем источник данных запроса
input_request = RequestSource(
    name="vals_to_add",
    schema=[
        Field(name="val_to_add", dtype=Int64),
        Field(name="val_to_add_2", dtype=Int64),
    ],
)

# Определяем представление признаков по требованию
@on_demand_feature_view(
    sources=[driver_stats_fv, input_request],
    schema=[
        Field(name="conv_rate_plus_val1", dtype=Float64),
        Field(name="conv_rate_plus_val2", dtype=Float64),
    ],
)
def transformed_conv_rate(inputs: pd.DataFrame) -> pd.DataFrame:
    df = pd.DataFrame()
    df["conv_rate_plus_val1"] = inputs["conv_rate"] + inputs["val_to_add"]
    df["conv_rate_plus_val2"] = inputs["conv_rate"] + inputs["val_to_add_2"]
    return df


# Группировка признаков в версию модели
driver_activity_v1 = FeatureService(
    name="driver_activity_v1",
    features=[
        driver_stats_fv[["conv_rate"]],  # Выборка подмножества признаков из представления
        transformed_conv_rate,  # Выборка всех признаков из представления
    ],
    logging_config=LoggingConfig(
        destination=FileLoggingDestination(path=DATA_PATH)
    ),
)
driver_activity_v2 = FeatureService(
    name="driver_activity_v2", features=[driver_stats_fv, transformed_conv_rate]
)

# Определение способа отправки данных (офлайн, онлайн или обоих типов) в Feast
driver_stats_push_source = PushSource(
    name="driver_stats_push_source",
    batch_source=driver_stats_source,
)

# Отправка признаков в онлайн-хранилище
driver_stats_fresh_fv = FeatureView(
    name="driver_hourly_stats_fresh",
    entities=[driver],
    ttl=timedelta(days=1),
    schema=[
        Field(name="conv_rate", dtype=Float32),
        Field(name="acc_rate", dtype=Float32),
        Field(name="avg_daily_trips", dtype=Int64),
    ],
    online=True,
    source=driver_stats_push_source,  # Использование источника push source
    tags={"team": "driver_performance"},
)

# Feature View: Driver activity stats
driver_activity_fv = FeatureView(
    name="driver_activity_stats",
    entities=[driver],
    ttl=timedelta(days=1),
    schema=[
        Field(name="avg_daily_trips", dtype=Int64),
    ],
    online=True,
    source=driver_stats_source,
    tags={"team": "driver_activity"},
)

# Feature View: Driver conversion stats
driver_conversion_fv = FeatureView(
    name="driver_conversion_stats",
    entities=[driver],
    ttl=timedelta(days=1),
    schema=[
        Field(name="conv_rate", dtype=Float32),
        Field(name="acc_rate", dtype=Float32),
    ],
    online=True,
    source=driver_stats_source,
    tags={"team": "driver_conversion"},
)

@on_demand_feature_view(
    sources=[driver_stats_fv],
    schema=[
        Field(name="conv_rate_per_trip", dtype=Float64),
        Field(name="adjusted_conv_rate", dtype=Float64),
    ],
)
def realtime_driver_metrics(inputs: pd.DataFrame) -> pd.DataFrame:
    df = pd.DataFrame()
    # Безопасно делим conv_rate на avg_daily_trips
    df["conv_rate_per_trip"] = inputs["conv_rate"] / (inputs["avg_daily_trips"] + 1e-5)
    # adjusted_conv_rate — просто пример: conv_rate с прибавкой среднего acc_rate
    df["adjusted_conv_rate"] = inputs["conv_rate"] + 0.1 * inputs["acc_rate"]
    return df

# Определяем представление признаков по требованию, которое может генерировать
# новые признаки на основе существующих представлений и признаков из RequestSource
@on_demand_feature_view(
    sources=[driver_stats_fresh_fv, input_request],  # использует свежую версию Feature View
    schema=[
        Field(name="conv_rate_plus_val1", dtype=Float64),
        Field(name="conv_rate_plus_val2", dtype=Float64),
    ],
)
def transformed_conv_rate_fresh(inputs: pd.DataFrame) -> pd.DataFrame:
    df = pd.DataFrame()
    df["conv_rate_plus_val1"] = inputs["conv_rate"] + inputs["val_to_add"]
    df["conv_rate_plus_val2"] = inputs["conv_rate"] + inputs["val_to_add_2"]
    return df


driver_activity_v3 = FeatureService(
    name="driver_activity_v3",
    features=[driver_stats_fresh_fv, transformed_conv_rate_fresh],
)

@on_demand_feature_view(
    sources=[driver_stats_fv],  # Используем существующий Feature View
    schema=[
        Field(name="combined_rating", dtype=Float64),
        Field(name="performance_score", dtype=Float64),
    ],
)
def driver_performance_metrics(inputs: pd.DataFrame) -> pd.DataFrame:
    df = pd.DataFrame()

    # Рассчитываем комбинированный рейтинг как взвешенную сумму
    # conv_rate имеет вес 0.6, acc_rate имеет вес 0.4
    df["combined_rating"] = (inputs["conv_rate"] * 0.6 + inputs["acc_rate"] * 0.4)

    # Рассчитываем показатель эффективности на основе среднего количества поездок
    # и комбинированного рейтинга
    df["performance_score"] = (df["combined_rating"] * np.log1p(inputs["avg_daily_trips"]))

    return df

# Обновляем существующий FeatureService, добавляя новые метрики
driver_activity_v4 = FeatureService(
    name="driver_activity_v4",
    features=[
        driver_stats_fv,
        driver_performance_metrics  # Добавляем новые метрики в сервис
    ]
)

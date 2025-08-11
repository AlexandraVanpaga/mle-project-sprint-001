import pendulum
from airflow.decorators import dag, task
from airflow.utils.trigger_rule import TriggerRule
from sqlalchemy import Table, MetaData, Column, Integer, Boolean, Float, UniqueConstraint
import pandas as pd
import numpy as np
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import get_current_context

# Центр Москвы
moscow_lat = 55.7539
moscow_lon = 37.6208

def haversine(lat1, lon1, lat2, lon2):
    R = 6371  # км
    lat1_rad, lon1_rad = np.radians(lat1), np.radians(lon1)
    lat2_rad, lon2_rad = np.radians(lat2), np.radians(lon2)
    dlat = lat2_rad - lat1_rad
    dlon = lon2_rad - lon1_rad
    a = np.sin(dlat / 2)**2 + np.cos(lat1_rad) * np.cos(lat2_rad) * np.sin(dlon / 2)**2
    c = 2 * np.arcsin(np.sqrt(a))
    return R * c

def fill_zeros_by_group_then_global(df, group_col, fill_cols):
    for col in fill_cols:
        df[col] = df[col].replace(0, np.nan)
        df[col] = df.groupby(group_col)[col].transform(lambda x: x.fillna(x.mean()))
        df[col] = df[col].fillna(df[col].mean())
    return df

def manual_filter(df: pd.DataFrame) -> pd.DataFrame:
    return df[
        (df["price"] >= 430_000) & (df["price"] <= 20_000_000) &
        (df["total_area"] <= 170) &
        (df["rooms"] <= 10) &
        (df["living_area"] <= 150) &
        (df["kitchen_area"] <= 50) &
        (df["ceiling_height"] <= 4) &
        (df["floor"] >= 1) & (df["floor"] <= 50) &
        (df["floors_total"] <= 60) &
        (df["flats_count"] <= 1000) &
        (df["distance_from_moscow_center"] <= 70) &
        (df["building_type_int"] <= 6) &
        (df["build_year"] >= 1900) & (df["build_year"] <= 2025)
    ]

@dag(
    schedule='@once',
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=["ETL"]
)
def prepare_clean_housing_dataset():

    @task()
    def create_table():
        hook = PostgresHook('destination_db')
        engine = hook.get_sqlalchemy_engine()

        metadata = MetaData()
        clean_housing = Table(
            'clean_housing_db',
            metadata,
            Column('id', Integer, primary_key=True, autoincrement=True),
            Column('flat_id', Integer, nullable=False),
            Column('building_id', Integer, nullable=False),
            Column('build_year', Integer),
            Column('building_type_int', Integer),
            Column('latitude', Float),
            Column('longitude', Float),
            Column('ceiling_height', Float),
            Column('flats_count', Integer),
            Column('floors_total', Integer),
            Column('has_elevator', Boolean),
            Column('floor', Integer),
            Column('kitchen_area', Float),
            Column('living_area', Float),
            Column('rooms', Integer),
            Column('is_apartment', Boolean),
            Column('studio', Boolean),
            Column('total_area', Float),
            Column('price', Float),
            Column('distance_from_moscow_center', Float),
            UniqueConstraint('flat_id', name='uq_flat_id_clean')
        )

        # создаёт таблицу, только если её ещё нет
        metadata.create_all(engine, checkfirst=True)

    @task()
    def extract():
        hook = PostgresHook('destination_db')
        conn = hook.get_conn()
        sql = """
        SELECT
          f.id AS flat_id,
          f.building_id AS building_id,
          f.floor,
          f.kitchen_area,
          f.living_area,
          f.rooms,
          f.is_apartment,
          f.studio,
          f.total_area,
          f.price,
          b.build_year,
          b.building_type_int,
          b.latitude,
          b.longitude,
          b.ceiling_height,
          b.flats_count,
          b.floors_total,
          b.has_elevator
        FROM flats AS f
        LEFT JOIN buildings AS b ON f.building_id = b.id
        """
        data = pd.read_sql(sql, conn)
        conn.close()
        return data

    @task()
    def transform(df: pd.DataFrame):
        # Заполняем нули в living_area и kitchen_area
        fill_cols = ['living_area', 'kitchen_area']
        df = fill_zeros_by_group_then_global(df, 'building_id', fill_cols)

        # Удаляем дубликаты
        df_no_id = df.drop(columns=['flat_id', 'building_id'])  # исключаем id для поиска дубликатов
        duplicated_mask = df_no_id.duplicated(keep='first')
        duplicate_indices = df_no_id[duplicated_mask].index
        df_cleaned = df.drop(index=duplicate_indices).reset_index(drop=True)

        # Считаем расстояние от центра Москвы
        df_cleaned['distance_from_moscow_center'] = haversine(
            df_cleaned['latitude'], df_cleaned['longitude'], moscow_lat, moscow_lon)

        # Ручная фильтрация
        df_manual = manual_filter(df_cleaned)

        return df_manual

    @task()
    def load(data: pd.DataFrame):
        hook = PostgresHook('destination_db')
        hook.insert_rows(
            table="clean_housing_db",
            rows=data.values.tolist(),
            target_fields=data.columns.tolist(),
            replace=True,
            replace_index=['flat_id']
        )

    @task(trigger_rule=TriggerRule.ALL_SUCCESS)
    def send_success_message():
        context = get_current_context()
        from steps.messages import send_telegram_success_message
        send_telegram_success_message(context)

    @task(trigger_rule=TriggerRule.ONE_FAILED)
    def send_failure_message():
        context = get_current_context()
        from steps.messages import send_telegram_failure_message
        send_telegram_failure_message(context)

    created = create_table()
    extracted = extract()
    transformed = transform(extracted)
    loaded = load(transformed)

    [created, extracted, transformed, loaded] >> send_success_message()
    [created, extracted, transformed, loaded] >> send_failure_message()


prepare_clean_housing_dataset()

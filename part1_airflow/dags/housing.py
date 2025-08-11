import pendulum
from airflow.decorators import dag, task
from airflow.utils.trigger_rule import TriggerRule
from sqlalchemy import Table, MetaData, Column, Integer, Boolean, Float, UniqueConstraint
import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import get_current_context


@dag(
    schedule='@once',
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=["ETL"]
)
def prepare_housing_dataset():

    @task()
    def create_table():
        hook = PostgresHook('destination_db')
        engine = hook.get_sqlalchemy_engine()

        metadata = MetaData()
        df_housing = Table(
            'df_housing',
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
            UniqueConstraint('flat_id', name='uq_flat_id')
        )
        metadata.create_all(engine)

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
    def transform(data: pd.DataFrame):
        return data

    @task()
    def load(data: pd.DataFrame):
        hook = PostgresHook('destination_db')
        hook.insert_rows(
            table="df_housing",
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

    # Основной pipeline
    created = create_table()
    extracted = extract()
    transformed = transform(extracted)
    loaded = load(transformed)

    # Уведомления
    [created, extracted, transformed, loaded] >> send_success_message()
    [created, extracted, transformed, loaded] >> send_failure_message()


prepare_housing_dataset()

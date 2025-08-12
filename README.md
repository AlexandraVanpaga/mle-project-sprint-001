# Проект 1 спринта
Здравствуйте!

Здесь пометки по функциям и переменным окружения.

Название бакета: s3-student-mle-20250729-0060996a6e-freetrack 

Юзер: mle-user@epd17jq103qvj9div3dc

Пути до файлов с кодом DAGs и названия функций:

- Первый DAG - /home/mle-user/mle-project-sprint-001/part1_airflow/dags/housing.py
 
Функции:

def create_table(): - создание таблицы в персональной БД с оригинальным датасетом

def extract(): - забираем оригинальный датасет из БД

def transform(data: pd.DataFrame): - функция для трансформаций, пока в ней не делаем ничего

def load(data: pd.DataFrame): загружаем в БД


- Второй DAG - /home/mle-user/mle-project-sprint-001/part1_airflow/dags/clean_housing.py

def haversine(lat1, lon1, lat2, lon2): - функция для определения расположения жилья

def fill_zeros_by_group_then_global(df, group_col, fill_cols): - заполнялка нулей в колонках с площадаями кухонь и жилыми площадями

def manual_filter(df: pd.DataFrame) -> pd.DataFrame: - фильтрация выбросов с кастомными границами (оказалось, что это сработало лучше всего, т.к. данные шумные и таргет скошенный)

def prepare_clean_housing_dataset(): - функция для подготовки очищенных данных

def extract(): - забираем созданную в первом DAG

def transform(df: pd.DataFrame): - трансформации по очистке

def load(data: pd.DataFrame): - загружаем в БД очищенные данные

Пути до файлов с конфигурацией DVC-пайплайна dvc.yaml, params.yaml, dvc.lock:

- Первый этап (сбор данных): /home/mle-user/mle-project-sprint-001/part2_dvc/scripts/data.py

- Второй этап (обучение модели): /home/mle-user/mle-project-sprint-001/part2_dvc/scripts/fit.py

- Третий этап (оценка с тремя этапами кросс-валидации): /home/mle-user/mle-project-sprint-001/part2_dvc/scripts/evaluate.py

- Путь до dvc.lock: /home/mle-user/mle-project-sprint-001/part2_dvc/dvc.lock

Наблюдения: использовались облегченные модели и 3 фолда с n_jobs = 1, т.к. на более тяжелых конфигурациях ВМ падает. Так как ВМ падала,  коммиты совершались вместе с поэтапными обновлениями, иначе был риск снова все делать с нуля.

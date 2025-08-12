import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler, FunctionTransformer
from sklearn.ensemble import RandomForestRegressor
import yaml
import os
import joblib
import numpy as np

def bool_to_int(X):
    return X.astype(int)

def fit_model():
    # Читаем параметры
    with open('params.yaml', 'r') as fd:
        params = yaml.safe_load(fd)

    # Загружаем данные
    data = pd.read_csv('data/initial_data.csv')

    # Определяем булевые и числовые колонки (пример, адаптируй под свои)
    bool_cols = ['has_elevator', 'is_apartment']
    num_cols = [col for col in data.columns if col not in bool_cols + [params['target_col']]]

    # Препроцессор
    preprocessor = ColumnTransformer(
        transformers=[
            ('bool', FunctionTransformer(bool_to_int), bool_cols),
            ('num', StandardScaler(), num_cols)
        ],
        remainder='drop',
        verbose_feature_names_out=False
    )

    # Разделяем признаки и таргет
    X = data[bool_cols + num_cols]
    y = np.log1p(data[params['target_col']])  # Логарифмируем таргет

    # Модель с параметрами из YAML
    model_params = params.get('model_params', {})
    model = RandomForestRegressor(**model_params)

    # Пайплайн
    pipeline = Pipeline([
        ('preprocessor', preprocessor),
        ('regressor', model)
    ])

    # Обучаем
    pipeline.fit(X, y)

    # Сохраняем модель
    os.makedirs('models', exist_ok=True)
    joblib.dump(pipeline, 'models/fitted_model.pkl')

if __name__ == '__main__':
    fit_model()

import pandas as pd
from sklearn.model_selection import StratifiedKFold, cross_validate
import joblib
import json
import yaml
import os

def evaluate_model():
    with open('params.yaml', 'r') as fd:
        params = yaml.safe_load(fd)

    # Загрузка данных и модели
    data = pd.read_csv('data/initial_data.csv')
    pipeline = joblib.load('models/fitted_model.pkl')

    # Кросс-валидация
    cv_strategy = StratifiedKFold(n_splits=params['n_splits'])
    cv_res = cross_validate(
        pipeline,
        data,
        data[params['target_col']],
        cv=cv_strategy,
        n_jobs=params['n_jobs'],
        scoring=params['metrics']
    )

    # Сохранение среднего значения метрик
    averaged_results = {key: round(value.mean(), 3) for key, value in cv_res.items()}
    os.makedirs('cv_results', exist_ok=True)
    with open('cv_results/cv_res.json', 'w') as f:
        json.dump(averaged_results, f, indent=2)

if __name__ == '__main__':
    evaluate_model()
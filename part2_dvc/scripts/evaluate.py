import pandas as pd
from sklearn.model_selection import KFold, cross_validate
import joblib
import json
import yaml
import os
import numpy as np
from sklearn.metrics import make_scorer, mean_squared_error, mean_absolute_error, r2_score

def rmse_exp(y_log_true, y_log_pred):
    y_true = np.expm1(y_log_true)
    y_pred = np.expm1(y_log_pred)
    return np.sqrt(mean_squared_error(y_true, y_pred))

def mae_exp(y_log_true, y_log_pred):
    y_true = np.expm1(y_log_true)
    y_pred = np.expm1(y_log_pred)
    return mean_absolute_error(y_true, y_pred)

def r2_exp(y_log_true, y_log_pred):
    y_true = np.expm1(y_log_true)
    y_pred = np.expm1(y_log_pred)
    return r2_score(y_true, y_pred)

# Определяем функцию bool_to_int (так как она используется в пайплайне)
def bool_to_int(X):
    return X.astype(int)

def evaluate_model():
    # Загружаем параметры
    with open('params.yaml', 'r') as fd:
        params = yaml.safe_load(fd)

    # Загружаем данные и модель
    data = pd.read_csv('data/initial_data.csv')
    pipeline = joblib.load('models/fitted_model.pkl')

    X = data.drop(columns=[params['target_col']])
    y = np.log1p(data[params['target_col']])  # Логарифмируем таргет для метрик

    # Настраиваем кросс-валидацию
    cv_strategy = KFold(n_splits=params['n_splits'], shuffle=True, random_state=42)

    # Объявляем метрики с преобразованием обратно в реальный масштаб - рубли
    scoring = {
        'rmse': make_scorer(rmse_exp, greater_is_better=False),
        'mae': make_scorer(mae_exp, greater_is_better=False),
        'r2': make_scorer(r2_exp)
    }

    cv_results = cross_validate(
        pipeline,
        X,
        y,
        cv=cv_strategy,
        scoring=scoring,
        n_jobs=params['n_jobs'],
        return_train_score=False
    )

    # Усредняем и инвертируем знаки для RMSE и MAE
    results = {
        'rmse': -cv_results['test_rmse'].mean(),
        'mae': -cv_results['test_mae'].mean(),
        'r2': cv_results['test_r2'].mean()
    }

    # Сохраняем результаты
    os.makedirs('cv_results', exist_ok=True)
    with open('cv_results/cv_res.json', 'w') as f:
        json.dump({k: round(v, 3) for k, v in results.items()}, f, indent=2)

    print("CV results:", results)

if __name__ == '__main__':
    evaluate_model()

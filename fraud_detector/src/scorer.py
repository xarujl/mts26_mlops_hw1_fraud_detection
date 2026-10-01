import os
import pandas as pd
import logging
from catboost import CatBoostClassifier

# Настройка логгера
logger = logging.getLogger(__name__)

# Define optimal threshold
MODEL_PATH = os.getenv('MODEL_PATH', './models/my_catboost.cbm')
MODEL_THRESHOLD = float(os.getenv('MODEL_THRESHOLD', '0.98'))


def load_model(model_path=MODEL_PATH):
    if not os.path.exists(model_path):
        bundled_model = '/app/models/my_catboost.cbm'
        if os.path.exists(bundled_model):
            logger.info('Shared model not found; using bundled model %s', bundled_model)
            model_path = bundled_model
    logger.info('Loading model from %s', model_path)
    model = CatBoostClassifier(task_type='CPU', thread_count=1, verbose=False)
    model.load_model(model_path)
    logger.info('Model loaded successfully')
    return model


def make_pred(dt, model, source_info="kafka"):

    # Меняем формат категориальных фичей на string перед скорингом
    expected_categorical = ['hour',
                            'year',
                            'month',
                            'day_of_month',
                            'day_of_week',
                            'gender_cat',
                            'merch_cat',
                            'cat_id_cat',
                            'one_city_cat',
                            'us_state_cat',
                            'jobs_cat']
    for col in expected_categorical:
        if col in dt.columns:
            dt[col] = dt[col].astype(str)

    # Calculate score
    scores = model.predict_proba(dt)[:, 1]
    submission = pd.DataFrame({
        'score': scores,
        'fraud_flag': (scores > MODEL_THRESHOLD).astype(int)
    })
    logger.info(f'Prediction complete for data from {source_info}')

    # Return proba for positive class
    return submission

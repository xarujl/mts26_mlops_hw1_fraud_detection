import logging
import os
import sys

import pandas as pd
from catboost import CatBoostClassifier

sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))
from preprocessing import load_train_data, run_preproc

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)


def main():
    data_path = os.getenv('TRAIN_DATA_PATH', './train_data/train.csv')
    logger.info('Preparing up to rows from %s', data_path)

    raw = pd.read_csv(data_path)
    if 'target' not in raw:
        raise ValueError("Training data must contain a 'target' column")

    # The same feature reference is used by training and online inference.
    reference = load_train_data()
    features = run_preproc(reference, raw.drop(columns=['target']))
    target = raw['target'].astype(int)

    model = CatBoostClassifier(
        iterations=int(os.getenv('TRAIN_ITERATIONS', '300')),
        depth=8,
        learning_rate=0.1,
        loss_function='Logloss',
        task_type='CPU',
        thread_count=1,
        verbose=50,
        random_seed=42,
    )
    categorical_features = features.select_dtypes(include=['object', 'category']).columns.tolist()
    model.fit(features, target, cat_features=categorical_features)

    model_path = os.getenv('MODEL_PATH', './models/my_catboost.cbm')
    os.makedirs(os.path.dirname(model_path), exist_ok=True)
    model.save_model(model_path)
    logger.info('Model saved to %s', model_path)


if __name__ == '__main__':
    main()

# Import standard libraries
import pandas as pd
import numpy as np
import logging
import os

# Import extra modules
from sklearn.impute import SimpleImputer 

logger = logging.getLogger(__name__)
RANDOM_STATE = 42

def add_time_features(df):
    logger.debug('Adding time features...')
    df['transaction_time'] = pd.to_datetime(df['transaction_time'])
    dt = df['transaction_time'].dt
    df['hour'] = dt.hour
    df['year'] = dt.year
    df['month'] = dt.month
    df['day_of_month'] = dt.day
    df['day_of_week'] = dt.dayofweek
    df.drop(columns='transaction_time', inplace=True)
    return df


def cat_encode(train, input_df, col):
    
    logger.debug('Encoding category: %s', col)
    new_col = col + '_cat'
    mapping = train[[col, new_col]].drop_duplicates()
    
    # Merge to initial dataset
    input_df = input_df.merge(mapping, how='left', on=col).drop(columns=col)
    
    return input_df


def add_distance_features(df):
    logger.debug('Calculating distances...')
    # Vectorized great-circle (haversine) distance is equivalent for this use
    # case and keeps preprocessing practical on a training dataset.
    lat1 = np.radians(pd.to_numeric(df['lat'], errors='coerce'))
    lon1 = np.radians(pd.to_numeric(df['lon'], errors='coerce'))
    lat2 = np.radians(pd.to_numeric(df['merchant_lat'], errors='coerce'))
    lon2 = np.radians(pd.to_numeric(df['merchant_lon'], errors='coerce'))
    delta_lat = lat2 - lat1
    delta_lon = lon2 - lon1
    haversine = np.sin(delta_lat / 2) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin(delta_lon / 2) ** 2
    df['distance'] = 6371.0088 * 2 * np.arcsin(np.sqrt(np.clip(haversine, 0, 1)))
    return df.drop(columns=['lat', 'lon', 'merchant_lat', 'merchant_lon'])


# Calculate means for encoding at docker container start
def load_train_data():

    logger.info('Loading training data...')

    # Define column types
    target_col = 'target'
    categorical_cols = ['gender', 'merch', 'cat_id', 'one_city', 'us_state', 'jobs']
    n_cats = 50

    # Import Train dataset
    train = pd.read_csv(os.getenv('TRAIN_DATA_PATH', './train_data/train.csv'),)
    train = train.drop(columns=['name_1', 'name_2', 'street', 'post_code'], errors='ignore')
    logger.info('Raw train data imported. Shape: %s', train.shape)

    # Add some simple time features
    train = add_time_features(train)

    for col in categorical_cols:
        new_col = col + '_cat'

        # Get table of categories
        temp_df = train\
            .groupby(col, dropna=False)[[target_col]]\
            .count()\
            .sort_values(target_col, ascending=False)\
            .reset_index()\
            .set_axis([col, 'count'], axis=1)\
            .reset_index()
        temp_df['index'] = temp_df.apply(lambda x: np.nan if pd.isna(x[col]) else x['index'], axis=1)
        temp_df[new_col] = ['cat_NAN' if pd.isna(x) else 'cat_' + str(x) if x < n_cats else f'cat_{n_cats}+' for x in temp_df['index']]

        train = train.merge(temp_df[[col, new_col]], how='left', on=col)
    
    # Calculate distance between a client and a merchant
    train = add_distance_features(train)

    logger.info('Train data processed. Shape: %s', train.shape)

    return train


# Main preprocessing function
def run_preproc(train, input_df):

    # Define column types
    target_col = 'target'
    categorical_cols = ['gender', 'merch', 'cat_id', 'one_city', 'us_state', 'jobs']
    continuous_cols = ['amount', 'population_city']
    drop_col = ['name_1', 'name_2', 'street', 'post_code']
    input_df = input_df.drop(columns=drop_col)
    
    # Run category encoding
    for col in categorical_cols:
        input_df = cat_encode(train, input_df, col)

    logger.info('Categorical merging completed. Output shape: %s', input_df.shape)
    
    # Add some simple time features
    input_df = add_time_features(input_df)

    logger.info('Added time features. Output shape: %s', input_df.shape)

    categorical_cols = [x + '_cat' for x in categorical_cols]
    categorical_cols.extend(['hour', 'year', 'month', 'day_of_month', 'day_of_week'])
    
    # Run mean ecoding for categorical variables
    for col in categorical_cols:
        # Fill empty values of categorical columns with some default category
        input_df[col] = input_df[col].fillna('cat_NAN')
    
        # Create table of means
        means_tb = train.groupby(col)[[target_col]].mean()\
                        .reset_index().rename(columns={target_col:f'{col}_mean_enc'})
        
        # Join to datasets
        input_df = input_df.merge(means_tb, how='left', on=col)

    logger.info('Categorical mean encoding completed. Output shape: %s', input_df.shape)

    # Calculate distance between a client and a merchant
    input_df = add_distance_features(input_df)
    continuous_cols.extend(['distance'])

    # Impute empty values with mean value
    imputer = SimpleImputer(missing_values=np.nan, strategy='mean') 
    imputer = imputer.fit(train[continuous_cols])

    output_df = pd.concat([
        input_df.drop(columns=continuous_cols),
        pd.DataFrame(imputer.transform(input_df.copy()[continuous_cols]), columns=continuous_cols)
    ], axis=1)

    # Add log transformation
    for col in continuous_cols:
        output_df[col + '_log'] = np.log(output_df[col] + 1)
        output_df.drop(columns=col, inplace=True)
        
    logger.info('Continuous features preprocessing completed. Output shape: %s', output_df.shape)
    
    # Return resulting dataset
    return output_df

import pandas as pd
import xgboost as xgb
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error, mean_squared_error
import logging

data = pd.read_parquet('/home/cloud_user/Stock-Market-Nasdaq/data/feacher_ing/etfs.parquet')

# Assume `data` is loaded as a Pandas DataFrame
data['Date'] = pd.to_datetime(data['Date'])
data.set_index('Date', inplace=True)

# Remove rows with NaN values
data.dropna(inplace=True)

# Select features and target
features = ['vol_moving_avg', 'adj_close_rolling_med']
target = 'Volume'

X = data[features]
y = data[target]

# Split data into train and test sets
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# Create an XGBoost model
model = xgb.XGBRegressor(n_estimators=100, random_state=42)

# Train the model
model.fit(X_train, y_train)

# Make predictions on test data
y_pred = model.predict(X_test)

# Calculate the Mean Absolute Error and Mean Squared Error
threshold = 0.01
mae = mean_absolute_error(y_test, y_pred)
mse = mean_squared_error(y_test, y_pred)
absolute_percentage_diff = abs(y_test - y_pred) / y_test
accurate_predictions = absolute_percentage_diff[absolute_percentage_diff <= threshold].count()
accuracy = (accurate_predictions / len(y_test)) * 100

# Define the version number
version = '1.0'

# Save the trained model to disk with the version
model.save_model(f'/home/cloud_user/Stock-Market-Nasdaq/model/model_{version}.bin')

# Set up logging
logging.basicConfig(filename=f'/home/cloud_user/Stock-Market-Nasdaq/model/training_metrics_{version}.log', level=logging.INFO)
logger = logging.getLogger()

# Log training metrics
logger.info(f'MAE: {mae}')
logger.info(f'MSE: {mse}')
logger.info(f'Accuracy: {accuracy}%')

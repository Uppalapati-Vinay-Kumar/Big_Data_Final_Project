"""
Predicts track popularity using data fetched from Cassandra and a RandomForest model.

1. **Data Fetching**: Retrieves track features and popularity from the `silver_tracks` table in Cassandra.
2. **Data Preprocessing**: 
   - Normalizes continuous features using `StandardScaler`.
   - Encodes categorical features using `OneHotEncoder`.
3. **Model Training**: Trains a `RandomForestRegressor` to predict track popularity.
4. **Model Evaluation**: Calculates R², Mean Squared Error (MSE), and Root Mean Squared Error (RMSE) for performance evaluation.
5. **Model Saving**: Saves the trained model as `track_popularity_model.joblib`.

### Dependencies:
- `pandas`
- `numpy`
- `scikit-learn`
- `joblib`
- `cassandra_connection` (custom connection module)
"""

import pandas as pd
import numpy as np
from cassandra_connection import get_session
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import r2_score, mean_squared_error
from joblib import dump


session = get_session()

print("Fetching data from Cassandra...")
cql_query = """
SELECT danceability, energy, key, loudness, mode, 
       speechiness, acousticness, liveness, valence, tempo, time_signature, popularity
FROM silver_tracks;
"""

try:
    rows = session.execute(cql_query)
    data = [dict(row._asdict()) for row in rows]
    print("Data fetched successfully!")
except Exception as e:
    print(f"Error fetching data from Cassandra: {e}")
    exit()

df = pd.DataFrame(data)
print(df.head(5))
print(df.shape)


X = df[['danceability', 'energy', 'key', 'loudness', 'mode', 
        'speechiness', 'acousticness', 'liveness', 'valence', 'tempo', 'time_signature']]
y = df['popularity']  

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

preprocessor = ColumnTransformer(
    transformers=[
        ('num', StandardScaler(), ['danceability', 'energy', 'loudness', 'speechiness', 'acousticness', 
                                   'liveness', 'valence', 'tempo']),  # continuous features
        ('cat', OneHotEncoder(), ['key', 'mode', 'time_signature'])  # categorical features
    ])

model = Pipeline(steps=[
    ('preprocessor', preprocessor),
    ('regressor', RandomForestRegressor(n_estimators=100, random_state=42))
])

# Training the model
model.fit(X_train, y_train)

# Model predictions
y_pred = model.predict(X_test)

# Evaluating the model
r2 = r2_score(y_test, y_pred)
mse = mean_squared_error(y_test, y_pred)
rmse = np.sqrt(mse)

print(f'R² Score: {r2}')
print(f'Mean Squared Error: {mse}')
print(f'Root Mean Squared Error: {rmse}')

# Saving the model
dump(model, 'track_popularity_model.joblib')  

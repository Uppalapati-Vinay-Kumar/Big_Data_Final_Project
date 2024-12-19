"""
Classifies track genres using data fetched from Cassandra and a RandomForest classifier.

1. **Data Fetching**: Retrieves track features and genre information from the `silver_tracks` table in Cassandra.
2. **Data Preprocessing**:
   - Scales continuous features using `StandardScaler`.
   - One-hot encodes categorical features (`key`, `mode`, `time_signature`) using `OneHotEncoder`.
3. **Class Weighting**: Computes balanced class weights for handling class imbalances during training.
4. **Model Training**: Trains a `RandomForestClassifier` to predict track genre.
5. **Model Evaluation**: Evaluates the model using accuracy and a classification report.
6. **Model Saving**: Saves the trained classifier model as `track_genre_model.joblib`.

### Dependencies:
- `pandas`
- `scikit-learn`
- `joblib`
- `cassandra_connection` (custom connection module)
"""

import pandas as pd
from cassandra_connection import get_session
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score, classification_report
from sklearn.utils.class_weight import compute_class_weight
from joblib import dump


session = get_session()

print("Fetching data from Cassandra...")
cql_query = """
SELECT danceability, energy, key, loudness, mode, 
       speechiness, acousticness, liveness, valence, tempo, time_signature, track_genre
FROM silver_tracks;
"""

try:
    rows = session.execute(cql_query)
    data = [dict(row._asdict()) for row in rows]
    print("Data fetched successfully!")
except Exception as e:
    print(f"Error fetching data from Cassandra: {e}")
    exit()

# Step 3: Convert Data to DataFrame
df = pd.DataFrame(data)
print(df.head(5))
print(df.shape)


X = df[['danceability', 'energy', 'key', 'loudness', 'mode', 
        'speechiness', 'acousticness', 'liveness', 'valence', 'tempo', 'time_signature']]

y = df['track_genre']  

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)


preprocessor = ColumnTransformer(
    transformers=[
        ('num', StandardScaler(), ['danceability', 'energy', 'loudness', 'speechiness', 'acousticness', 
                                   'liveness', 'valence', 'tempo']),  # continuous features
        ('cat', OneHotEncoder(), ['key', 'mode', 'time_signature'])  # categorical features
    ])

class_weights = compute_class_weight('balanced', classes=df['track_genre'].unique(), y=df['track_genre'])
class_weight_dict = dict(zip(df['track_genre'].unique(), class_weights))

model = Pipeline(steps=[
    ('preprocessor', preprocessor),
    ('classifier', RandomForestClassifier(n_estimators=100, random_state=42, class_weight=class_weight_dict))
])

# Training the model
model.fit(X_train, y_train)


# Model Evaluation
y_pred_class = model.predict(X_test)
accuracy = accuracy_score(y_test, y_pred_class)
print(f'Accuracy: {accuracy}')
print('Classification Report:')
print(classification_report(y_test, y_pred_class))

#Saving model
dump(model, 'track_genre_model.joblib')


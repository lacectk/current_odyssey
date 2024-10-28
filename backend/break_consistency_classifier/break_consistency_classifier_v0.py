import pandas as pd
import psycopg2
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.impute import SimpleImputer
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report, confusion_matrix
from sklearn.cluster import KMeans
import os


class WaveConsistencyClassifier:
    def __init__(self, target_column: str = None):
        """
        Initialize the classifier with database parameters and optional target column.
        If no target column is provided, unsupervised clustering will be used.
        """
        # Set up database parameters from environment variables
        self.db_params = {
            "dbname": "wave_data",
            "user": os.getenv("DB_USER"),
            "password": os.getenv("DB_PASSWORD"),
            "host": os.getenv("DB_HOST"),
        }
        self.target_column = target_column
        self.conn = None
        self.model = None
        self.scaler = StandardScaler()
        self.imputer = SimpleImputer(strategy="mean")  # Imputer for missing values

    def connect_to_db(self):
        """
        Establish a connection to the PostgreSQL database.
        """
        try:
            self.conn = psycopg2.connect(**self.db_params)
            print("Database connection established.")
        except Exception as e:
            print(f"Failed to connect to the database: {e}")

    def load_data_from_db(self):
        """
        Load wave data from the 'wave_data' table in the database.
        """
        if not self.conn:
            print("No active database connection.")
            return None

        try:
            query = """
                SELECT datetime, wvht, dpd, apd, mwd
                FROM wave_data;
            """
            df = pd.read_sql(query, self.conn)

            if df.empty:
                print("No data found in the 'wave_data' table.")
                return None

            print(f"Loaded {len(df)} records from the 'wave_data' table.")
            return df

        except Exception as e:
            print(f"Error reading from database: {e}")
            return None

    def prepare_data(self, df):
        """
        Prepare the data for training or clustering, handling missing values.
        """
        # Define features
        X = df[["wvht", "dpd", "apd", "mwd"]]

        # Impute missing values in features
        X_imputed = self.imputer.fit_transform(X)

        if self.target_column and self.target_column in df.columns:
            y = df[self.target_column]
            # Split the data into train and test sets
            X_train, X_test, y_train, y_test = train_test_split(
                X_imputed, y, test_size=0.2, random_state=42
            )
            # Standardize features
            X_train_scaled = self.scaler.fit_transform(X_train)
            X_test_scaled = self.scaler.transform(X_test)

            return X_train_scaled, X_test_scaled, y_train, y_test
        else:
            # If no target column, only scale the features for clustering
            X_scaled = self.scaler.fit_transform(X_imputed)
            return X_scaled, df

    def train_random_forest(self, X_train, y_train):
        """
        Train a Random Forest model on the training data.
        """
        self.model = RandomForestClassifier(n_estimators=100, random_state=42)
        self.model.fit(X_train, y_train)

    def evaluate_model(self, X_test, y_test):
        """
        Evaluate the trained model on the test data.
        """
        if self.model is None:
            print("No model found. Train a model before evaluation.")
            return

        y_pred = self.model.predict(X_test)
        print("Confusion Matrix:\n", confusion_matrix(y_test, y_pred))
        print("\nClassification Report:\n", classification_report(y_test, y_pred))

    def perform_clustering(self, X):
        """
        Apply K-Means clustering to identify consistency levels.
        """
        kmeans = KMeans(n_clusters=3, random_state=42)
        clusters = kmeans.fit_predict(X)
        return clusters

    def label_consistency(self, row):
        """
        Define initial rule-based labeling for wave consistency.
        """
        if row["WVHT"] > 2.0 and row["DPD"] > 10:
            return "High Consistency"
        elif 1.0 <= row["WVHT"] <= 2.0 and 6 <= row["DPD"] <= 10:
            return "Moderate Consistency"
        else:
            return "Low Consistency"

    def run(self):
        """
        Execute the full workflow: data loading from DB, preparation, training, or clustering.
        """
        self.connect_to_db()

        df = self.load_data_from_db()
        if df is None:
            return

        if self.target_column:
            # Supervised Learning
            X_train, X_test, y_train, y_test = self.prepare_data(df)

            if X_train is not None:
                # Train and evaluate the model
                self.train_random_forest(X_train, y_train)
                self.evaluate_model(X_test, y_test)
        else:
            # Unsupervised Clustering
            X, df = self.prepare_data(df)

            if X is not None:
                # Perform clustering
                clusters = self.perform_clustering(X)
                df["Consistency_Cluster"] = clusters

                # Apply rule-based labeling
                df["Initial_Consistency_Label"] = df.apply(
                    self.label_consistency, axis=1
                )

                # Output the labeled data
                print(
                    df[
                        [
                            "WVHT",
                            "DPD",
                            "APD",
                            "MWD",
                            "Consistency_Cluster",
                            "Initial_Consistency_Label",
                        ]
                    ].head()
                )

        self.close_db_connection()

    def predict(self, wave_data):
        """
        Predict the wave consistency category for new wave data, handling missing values.
        :param wave_data: A dictionary with keys 'WVHT', 'DPD', 'APD', 'MWD'.
        :return: Predicted category or consistency cluster.
        """
        if not self.model:
            print("No model is trained yet. Train a model before prediction.")
            return None

        # Handle missing values by using imputer
        wave_features = [
            [
                wave_data.get("WVHT", None),
                wave_data.get("DPD", None),
                wave_data.get("APD", None),
                wave_data.get("MWD", None),
            ]
        ]
        wave_features_imputed = self.imputer.transform(wave_features)
        wave_features_scaled = self.scaler.transform(wave_features_imputed)

        if self.target_column:
            prediction = self.model.predict(wave_features_scaled)
        else:
            # For unsupervised, use the clustering model
            kmeans = KMeans(n_clusters=3, random_state=42)
            kmeans.fit(wave_features_scaled)
            prediction = kmeans.predict(wave_features_scaled)

        return prediction

    def close_db_connection(self):
        """
        Close the database connection.
        """
        if self.conn:
            self.conn.close()
            print("Database connection closed.")


# Example usage:
if __name__ == "__main__":
    # Initialize the classifier with the target column for supervised classification
    classifier = WaveConsistencyClassifier(target_column="consistency")

    # Run the workflow
    classifier.run()

    # Example prediction with new data, allowing missing values
    new_wave_data = {"wvht": 1.5, "dpd": None, "apd": 7.5, "mwd": 220}
    prediction = classifier.predict(new_wave_data)
    print(f"Predicted wave consistency: {prediction}")

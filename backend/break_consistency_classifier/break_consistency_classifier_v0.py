import pandas as pd
from sqlalchemy import create_engine
from sklearn.preprocessing import StandardScaler
from sklearn.impute import SimpleImputer
from sklearn.cluster import KMeans
import matplotlib.pyplot as plt
import os


class WaveConsistencyClustering:
    def __init__(self):
        # Set up database connection string using SQLAlchemy
        user = os.getenv("DB_USER")
        password = os.getenv("DB_PASSWORD")
        host = os.getenv("DB_HOST")
        dbname = "wave_data"
        self.db_url = f"postgresql://{user}:{password}@{host}/{dbname}"
        self.engine = None
        self.scaler = StandardScaler()
        self.imputer = SimpleImputer(strategy="mean")

    def connect_to_db(self):
        """
        Establish a connection to the PostgreSQL database using SQLAlchemy.
        """
        try:
            self.engine = create_engine(self.db_url)
            print("Database connection established.")
        except Exception as e:
            print(f"Failed to connect to the database: {e}")

    def load_data_from_db(self):
        """
        Load wave data from the 'wave_data' table in the database.
        """
        if not self.engine:
            print("No active database connection.")
            return None

        try:
            query = """
                SELECT station_id, datetime, wvht, dpd, apd, mwd
                FROM wave_data;
            """
            df = pd.read_sql(query, self.engine)

            if df.empty:
                print("No data found in the 'wave_data' table.")
                return None

            print(f"Loaded {len(df)} records from the 'wave_data' table.")
            return df

        except Exception as e:
            print(f"Error reading from database: {e}")
            return None

    def preprocess_data(self, X):
        """
        Impute missing values and standardize the data.
        """
        # Impute missing values with mean
        X_imputed = self.imputer.fit_transform(X)

        # Standardize the data
        X_scaled = self.scaler.fit_transform(X_imputed)

        return X_scaled

    def find_optimal_clusters(self, X):
        """
        Use the Elbow method to determine the optimal number of clusters.
        """
        inertia = []
        cluster_range = range(2, 11)

        for k in cluster_range:
            kmeans = KMeans(n_clusters=k, random_state=42)
            kmeans.fit(X)
            inertia.append(kmeans.inertia_)

        plt.figure(figsize=(10, 6))
        plt.plot(cluster_range, inertia, marker="o")
        plt.xlabel("Number of clusters")
        plt.ylabel("Inertia")
        plt.title("Elbow Method for Optimal Clusters")
        plt.show()

        optimal_k = int(
            input("Enter the optimal number of clusters (based on the Elbow plot): ")
        )
        return optimal_k

    def cluster_data(self, X, n_clusters):
        """
        Apply K-Means clustering to the data.
        """
        kmeans = KMeans(n_clusters=n_clusters, random_state=42)
        clusters = kmeans.fit_predict(X)
        return clusters

    def save_clusters_to_db(self, df):
        """
        Save the clustered data to a new table in the database.
        """
        try:
            df.to_sql(
                "wave_consistency_clusters",
                self.engine,
                if_exists="replace",
                index=False,
            )
            print("Clustered data saved to 'wave_consistency_clusters' table.")
        except Exception as e:
            print(f"Error saving to database: {e}")

    def generate_per_station_chart(self, df):
        """
        Generate and save per station_id charts of consistency.
        """
        station_ids = df["station_id"].unique()

        for station_id in station_ids:
            station_data = df[df["station_id"] == station_id]
            plt.figure(figsize=(10, 6))
            plt.hist(
                station_data["Cluster"],
                bins=range(station_data["Cluster"].nunique() + 1),
                alpha=0.7,
            )
            plt.xlabel("Cluster")
            plt.ylabel("Frequency")
            plt.title(f"Wave Consistency Clusters for Station {station_id}")
            plt.grid(True)
            plt.savefig(f"wave_consistency_station_{station_id}.png")
            plt.close()

    def run(self):
        """
        Execute the full workflow: data loading, preprocessing, clustering, saving, and chart generation.
        """
        self.connect_to_db()

        df = self.load_data_from_db()
        if df is None:
            return

        # Extract features and preprocess the data
        X = df[["wvht", "dpd", "apd", "mwd"]]
        X_scaled = self.preprocess_data(X)

        # Determine the optimal number of clusters
        n_clusters = self.find_optimal_clusters(X_scaled)

        # Apply K-Means clustering
        df["Cluster"] = self.cluster_data(X_scaled, n_clusters)

        # Save clustered data to the database
        self.save_clusters_to_db(df)

        # Generate and save per station_id charts
        self.generate_per_station_chart(df)


# Example usage:
if __name__ == "__main__":
    # Initialize and run the clustering workflow
    clustering = WaveConsistencyClustering()
    clustering.run()

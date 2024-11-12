from airflow.models import User
from airflow.utils.db import create_default_connections
from airflow import settings
import os
from dotenv import load_dotenv


def setup_airflow():
    load_dotenv()

    session = settings.Session()

    # Create admin user if it doesn't exist
    if (
        not session.query(User)
        .filter(User.username == os.getenv("AIRFLOW_ADMIN_USER"))
        .first()
    ):
        user = User(
            username=os.getenv("AIRFLOW_ADMIN_USER"),
            password=os.getenv("AIRFLOW_ADMIN_PASSWORD"),
            email=os.getenv("AIRFLOW_ADMIN_EMAIL"),
            first_name=os.getenv("AIRFLOW_ADMIN_FIRSTNAME"),
            last_name=os.getenv("AIRFLOW_ADMIN_LASTNAME"),
            roles=[session.query(Role).filter(Role.name == "Admin").first()],
        )
        session.add(user)
        session.commit()

    create_default_connections()
    session.close()


if __name__ == "__main__":
    setup_airflow()

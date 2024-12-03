from dotenv import load_dotenv
from dagster._cli import main

# Load environment variables from .env
load_dotenv()

# Start Dagster CLI
if __name__ == "__main__":
    main()

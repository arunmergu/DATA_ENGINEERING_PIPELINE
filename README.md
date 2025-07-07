# DATA_ENGINEERING_PIPELINE

## 🚀 Getting Started

These instructions will get you a copy of the project up and running on your local machine for development and testing purposes.

### Prerequisites

Before you begin, ensure you have the following installed on your local machine:

* **Git:** Essential for cloning the project repository.
    * [Download Git](https://git-scm.com/downloads)
* **Docker Desktop:** This includes both Docker Engine (to run containers) and Docker Compose (to manage multi-container applications).
    * [Download Docker Desktop](https://www.docker.com/products/docker-desktop/)
    * Ensure Docker Desktop is running before proceeding (you should see its icon in your system tray/menu bar).

### Installation & Local Run

Follow these steps to get your data pipeline running locally:

1.  **Clone the Repository:**
    Open your terminal or command prompt and clone the project:
    ```bash
    git clone repo.git
    ```
    Then navigate into the cloned project directory:
    ```bash
    cd repo
    ```

2.  **Make the `data` directory writable:**
    The Docker container needs write permissions to create the output SQLite database within the `data/` folder.
    ```bash
    chmod -R 777 data
    ```
    *(Note: On Windows, file permissions are handled differently. Docker Desktop typically manages this, or you might need to ensure your user has full control over the `data` folder in File Explorer properties.)*

3.  **Set up Local Environment Variables:**
    For local development, it's best practice to manage environment variables (like database connection strings) using a `.env` file. This file is excluded from version control by your `.gitignore`.

    Create a new file named `.env` in the **root of your `DATA_ENGINEERING_PIPELINE` directory** (where `docker-compose.yml` is located) and add the following content:
    ```ini
    # .env
    DB_URL=sqlite:///./data/bynd_pipeline.db
    DB_TABLE_NAME=customers_and_transactions
    ```

4.  **Run the Data Pipeline:**
    From the root of your project directory, execute the following command. This will:
    * Build the Docker image for your `data-pipeline` service (if it hasn't been built before or if the `Dockerfile` has changed).
    * Start the `data-pipeline` container.
    * The pipeline will run, process the `mock_dataset.csv` from the `data/` folder, and save the resulting `bynd_pipeline.db` file back into your local `data/` folder.
    ```bash
    docker compose up --build
    ```
    You will see the pipeline's logs directly in your terminal. Once the pipeline completes its run, the container will exit.

5.  **Verify the Output:**
    After the command finishes, check your local `DATA_ENGINEERING_PIPELINE/data/` directory. You should find the newly created or updated `bynd_pipeline.db` file there. You can use a SQLite browser (like [DB Browser for SQLite](https://sqlitebrowser.org/)) to inspect its contents.

---
# Use a lightweight Python 3.11 image as the base
FROM python:3.11-slim-bookworm

# Set the working directory inside the container
WORKDIR /app

# Copy the requirements file into the container and install dependencies.
# This step is placed early to leverage Docker's build cache.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
RUN pip install --no-cache-dir --upgrade --force-reinstall numpy pandas


# Copy your entire application source code into the container.
# This includes 'src' directory, and importantly, the 'data' directory
# which contains your 'mock_dataset.csv'.
COPY src ./src
COPY data ./data

# Define the command that will be executed when the container starts.
# This runs your main pipeline script.
CMD ["python", "-m", "src.main"]
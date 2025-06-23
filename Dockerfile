FROM python:3.10-slim-buster

# Set the working directory in the container
WORKDIR /app

# Copy the requirements file and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the entire application code into the container
COPY . .

# Expose the port your Flask app will run on (e.g., 5000)
EXPOSE 5000

# Command to run the Flask application using Gunicorn (recommended for production)
CMD ["python", "app.py"]
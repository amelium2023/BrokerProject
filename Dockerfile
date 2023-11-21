# Use an official Python runtime as a parent image
FROM python:3.10

# Set the working directory in the container
WORKDIR /app

# Copy the current directory contents into the container at /usr/src/app
COPY . .

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

RUN pip install mysql-connector-python
RUN pip install boto3
RUN pip install pytz

# Run the Python application when the container launches
CMD ["python", "main.py"]

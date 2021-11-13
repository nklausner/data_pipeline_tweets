# Use an official Python runtime as a parent image
FROM python:3.9

# Set the working directory to /app_folder
WORKDIR /app_folder

# Copy the requirements file into the container at /app_folder
# (a text files with all the libraries you want to install)
COPY . /app_folder

# Install any needed packages specified in requirements.txt
RUN pip3 install --trusted-host pypi.python.org -r requirements.txt

# Run app.py when the container launches
CMD ["python", "tweet_listener.py"]

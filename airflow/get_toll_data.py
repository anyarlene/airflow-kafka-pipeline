import requests
import os

# URL to download from
url = "https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Final%20Assignment/tolldata.tgz"

# Destination path
destination = "/Users/any-arleneniyubahwe/Desktop/private2/my-git/ejo/airflow-kafka-pipeline/airflow/dags/finalassignment/tolldata.tgz"

# Ensure the directory exists
directory = os.path.dirname(destination)
if not os.path.exists(directory):
    os.makedirs(directory)

# Make the request to download the file
response = requests.get(url, stream=True)

# Ensure the response is valid
if response.status_code == 200:
    # Write the file to the destination
    with open(destination, 'wb') as file:
        for chunk in response.iter_content(chunk_size=8192):
            file.write(chunk)
    print("File downloaded successfully!")
else:
    print(f"Failed to download the file. HTTP Status Code: {response.status_code}")
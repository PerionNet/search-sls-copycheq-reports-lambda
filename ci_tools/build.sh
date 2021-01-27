#!/bin/bash

# apt-get update
# apt-get install -y git
# echo PATH="root/.serverless/bin:${PATH}" >> /root/.bashrc
# sudo su
# pip3 install discover
# PATH="/root/.serverless/bin:${PATH}"
whoami
printenv
pip3 install -r requirements.txt
cp sonar-scanner.properties /root/sonar-scanner/conf/sonar-scanner.properties
ls -l && coverage run -m unittest tests/* discover && coverage xml

echo "Running Sonar on $SERVICE_NAME:$VERSION_NUMBER"
sonar-scanner \
    -Dsonar.projectName=$SERVICE_NAME \
    -Dsonar.projectVersion=$VERSION_NUMBER
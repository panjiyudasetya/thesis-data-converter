# Data Converter

A GitHub repository to generate criteria dataset of the user with social phobia. It is used for academic purposes in Universitas Kristen Maranatha to complete a Master's Degree in Computer Science.

## High level overview

<p align="center">
  <img src="https://github.com/panjiyudasetya/thesis-data-converter/assets/21379421/8388514c-425f-4bc9-813c-e20ce3132295" alt="Architecture Design"/>
</p>

This module pulls data from Metabase, transforms them into a criteria dataset of the user with social phobia, and loads them into local storage.

## How to run the project
All our components run in Docker containers. Development orchestration is handled by _docker-compose_. Therefore, installing Docker on your machine is required. Regarding installation guidelines, please follow the particular links below:

For machines running **MacOS** you can follow steps explained [here](https://docs.docker.com/docker-for-mac/install/)

For machines running **Linux (Ubuntu)** you can follow steps explained [here](https://docs.docker.com/desktop/install/linux-install/)

Please also ensure that _docker-compose_ command is installed.

Once the Docker installation is configured correctly, please follow these steps:
- Copy-paste `.env.example` as `.env`
- Fill in the secret variables to use the Metabase API. It consists of the Metabase URL, Metabase's encrypted username, and its password.
- Fill in the secret key. This is your private key to decrypt the Metabase's username and password.

#!/bin/bash

# Clear the contents of the .env file
> .env

# Append new values to the .env file
echo "POSTGRES_DATABASE=$(azd env get-value POSTGRES_DATABASE)" >> .env
echo "POSTGRES_HOST=$(azd env get-value POSTGRES_HOST)" >> .env
echo "POSTGRES_SSL=$(azd env get-value POSTGRES_SSL)" >> .env
echo "POSTGRES_USERNAME=$(azd env get-value POSTGRES_USERNAME)" >> .env

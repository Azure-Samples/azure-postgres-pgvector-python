# Clear the contents of the .env file
Set-Content -Path .env -Value ""

# Append new values to the .env file
$postgresDatabase = azd env get-value POSTGRES_DATABASE
$postgresHost = azd env get-value POSTGRES_HOST
$postgresSSL = azd env get-value POSTGRES_SSL
$postgresUsername = azd env get-value POSTGRES_USERNAME

Add-Content -Path .env -Value "POSTGRES_DATABASE=$postgresDatabase"
Add-Content -Path .env -Value "POSTGRES_HOST=$postgresHost"
Add-Content -Path .env -Value "POSTGRES_SSL=$postgresSSL"
Add-Content -Path .env -Value "POSTGRES_USERNAME=$postgresUsername"

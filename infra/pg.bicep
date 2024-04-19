metadata description = 'Creates an Azure App Service in an existing Azure App Service plan.'
param name string
param location string = resourceGroup().location
param tags object = {}

@allowed([
  'Password'
  'EntraOnly'
])
param authType string = 'Password'

param administratorLogin string = ''
@secure()
param administratorLoginPassword string = ''

@description('The Object ID of the Azure AD admin.')
param aadAdminObjectid string

@description('Azure AD admin name.')
param aadAdminName string

@description('Azure AD admin Type')
@allowed([
  'User'
  'Group'
  'ServicePrincipal'
])
param aadAdminType string = 'User'

param databaseNames array = []
param allowAzureIPsFirewall bool = false
param allowAllIPsFirewall bool = false
param allowedSingleIPs array = []

// PostgreSQL version
param version string
param storage object

var authProperties = authType == 'Password' ? {
  administratorLogin: administratorLogin
  administratorLoginPassword: administratorLoginPassword
  authConfig: {
    passwordAuth: 'Enabled'
  }
} : {
  authConfig: {
    activeDirectoryAuth: 'Enabled'
    passwordAuth: 'Disabled'
  }
}

resource postgresServer 'Microsoft.DBforPostgreSQL/flexibleServers@2023-03-01-preview' = {
  name: name
  location: location
  tags: tags

  properties: union(authProperties, {
    version: version
    storage: storage
    sku: {
      name: 'Standard_D2ds_v4'
      tier: 'GeneralPurpose'
    }
    highAvailability: {
      mode: 'Disabled'
    }
  })

  resource database 'databases' = [for name in databaseNames: {
    name: name
  }]
}

resource firewall_all 'Microsoft.DBforPostgreSQL/flexibleServers/firewallRules@2023-03-01-preview' = if (allowAllIPsFirewall) {
  parent: postgresServer
  name: 'allow-all-IPs'
  properties: {
    startIpAddress: '0.0.0.0'
    endIpAddress: '255.255.255.255'
  }
}

resource firewall_azure 'Microsoft.DBforPostgreSQL/flexibleServers/firewallRules@2023-03-01-preview' = if (allowAzureIPsFirewall) {
  parent: postgresServer
  name: 'allow-all-azure-internal-IPs'
  properties: {
    startIpAddress: '0.0.0.0'
    endIpAddress: '0.0.0.0'
  }
}

@batchSize(1)
resource firewall_single 'Microsoft.DBforPostgreSQL/flexibleServers/firewallRules@2023-03-01-preview' = [for ip in allowedSingleIPs: {
  parent: postgresServer
  name: 'allow-single-${replace(ip, '.', '')}'
  properties: {
    startIpAddress: ip
    endIpAddress: ip
  }
}]

// Workaround issue https://github.com/Azure/bicep-types-az/issues/1507
resource configurations 'Microsoft.DBforPostgreSQL/flexibleServers/configurations@2023-03-01-preview' = {
  name: 'azure.extensions'
  parent: postgresServer
  properties: {
    value: 'vector'
    source: 'user-override'
  }
  dependsOn: [
    firewall_all, firewall_azure, firewall_single
  ]
}



// This must be created *after* the server is created - it cannot be a nested child resource
resource addAddUser 'Microsoft.DBforPostgreSQL/flexibleServers/administrators@2023-03-01-preview' = {
  name: aadAdminObjectid // Pass my principal ID
  parent: postgresServer
  properties: {
    tenantId: subscription().tenantId
    principalType: aadAdminType // User
    principalName: aadAdminName // UserRole
  }
  dependsOn: [
    firewall_all, firewall_azure, firewall_single, configurations
  ]
}


output POSTGRES_DOMAIN_NAME string = postgresServer.properties.fullyQualifiedDomainName

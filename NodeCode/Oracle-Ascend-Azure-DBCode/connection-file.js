var config = {
    user: 'ascendadmin',
    password: 'Dragon@2019',
    server: 'ascenddb.database.windows.net',
    database: 'AZDB-USCON-ASCEND-NPD',
    encrypt: true,
    connectionTimeout: 300000,
    requestTimeout: 15000,
    pool: {
        idleTimeoutMillis: 300000,
        max: 100
    }

};

const schemaName = 'ASCEND';
//const serverName = 'http://sazas-uscon-ascend-api-npd.azurewebsites.net';

module.exports = {
    //config,
    schemaName,
    getconnection: function () {
        var DB = process.env.WEBSITE_HOSTNAME
        //console.log('DB Value: ' + DB);
        if (DB == 'azas-uscon-ascend-api-npd.azurewebsites.net') {
            configurations = {
                dbConfig:{
                    user: 'ascendadmin',
                    password: 'Dragon@2019',
                    server: 'ascenddb.database.windows.net',
                    database: 'AZDB-USCON-ASCEND-NPD',
                    encrypt: true,
                    connectionTimeout: 300000,
                    requestTimeout: 15000,
                    pool: {
                        idleTimeoutMillis: 300000,
                        max: 100
                    }
                },
                createTeamsTenancyId:'36da45f1-dd2c-4d1f-af13-5abe46b99921',
                createTeamsClientId:'9aada26d-0c9d-4450-8a55-77ff28aa0acf',
				createTeamsClientSecret:'JlNNcjgueUxJaHlJL3FidVRtc0NkK2I2OXo6T1JXaGEkXQ==',
				timeoutValue:'2000000',
				maxAttemptsValue:'5',
                retryDelayValue:'100000',
                storageAccountName:'azasusconstorageaccount',
                storageAccountKey:'5FKV6btgea0iKfltOWVB+rMMahCJil+DcuV7YEtUnPZ9YOnxX81M8wKl9uPHZlGHv8AsCb99tr8PP4j8OkJpiQ==',
                storageContainerName:'dev',
                storageContainerPath:'https://azasusconstorageaccount.blob.core.windows.net'
            };
        }
        else if (DB == 'sazas-uscon-ascend-api-npd.azurewebsites.net') {
            configurations = {
                dbConfig:{
                    user: 'ascendadmin',
                    password: 'Deloitte@123',
                    server: 'sascenddb.database.windows.net',
                    database: 'ascenddb',
                    encrypt: true,
                    connectionTimeout: 300000,
                    requestTimeout: 15000,
                    pool: {
                        idleTimeoutMillis: 300000,
                        max: 100
                    }
                },
                createTeamsTenancyId:'36da45f1-dd2c-4d1f-af13-5abe46b99921',
                createTeamsClientId:'87677853-759b-4df4-b7dc-ae2a35aa8c38',
				createTeamsClientSecret:'MUhQQCswMV07SVZYZipeM3VbWjNLSkF8VCtnJERDYkVxKA==',
				timeoutValue:'2000000',
				maxAttemptsValue:'5',
                retryDelayValue:'100000',
                storageAccountName:'',
                storageAccountKey:'',
                storageContainerName:'',
                storageContainerPath:''

                };
        }
        else if (DB == 'azas-uscon-ascend-api-prod.azurewebsites.net') {
            configurations = {
                dbConfig:{
                    user: 'ascendadmin',
                    password: '***********',
                    server: 'pascenddb.database.windows.net',
                    database: 'ascenddb',
                    encrypt: true,
                    connectionTimeout: 300000,
                    requestTimeout: 15000,
                    pool: {
                        idleTimeoutMillis: 300000,
                        max: 100
                    }
                },
                createTeamsTenancyId:'36da45f1-dd2c-4d1f-af13-5abe46b99921',
                createTeamsClientId:'1c8e756b-9560-4737-ada0-636b617a8b3d',
				createTeamsClientSecret:'dD8yKkh2STZMOkU+UDlZLSlGKFohSHQvMzA0KHUqcEkyZw==',
				timeoutValue:'2000000',
				maxAttemptsValue:'5',
                retryDelayValue:'100000',
                storageAccountName:'',
                storageAccountKey:'',
                storageContainerName:'',
                storageContainerPath:''
            };
        }
        else {
            console.log('Error in path');
        }
        return (configurations);
    }
}
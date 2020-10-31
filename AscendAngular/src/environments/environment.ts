// This file can be replaced during build by using the `fileReplacements` array.
// `ng build --prod` replaces `environment.ts` with `environment.prod.ts`.
// The list of file replacements can be found in `angular.json`.

export const environment = {
  production: false,
  isLocal: true,
  BASE_URL: 'https://sazas-uscon-ascend-api-npd.azurewebsites.net',//'http://172.16.25.60:8000',
  AUTH_CONFIG: {
    clientID: '7bae4c1a-c751-4163-8c16-b5b9f904ba67',
    authority: "https://login.microsoftonline.com/36da45f1-dd2c-4d1f-af13-5abe46b99921",
    redirectUri: 'https://soascend.deloitte.com',
    cacheLocation : "sessionStorage",
    postLogoutRedirectUri: "https://soascend.deloitte.com",
    navigateToLoginRequestUrl: false,
    consentScopes: [ "user.read","offline_access"],
    unprotectedResources: ["https://www.microsoft.com/en-us/"],
    logger: (logLevel, message, piiEnabled) => undefined,
    correlationId: '1234',
    piiLoggingEnabled: true
  },
  AUTH_RESOURCE_LINK: {
    self: "https://graph.microsoft.com/v1.0/me",
    users: "https://graph.microsoft.com/v1.0/users",
    webAPI: "https://sazas-uscon-ascend-api-npd.azurewebsites.net/user_impersonation"
  },
  LOCAL_AUTH: {
    userId: 'aknayak@deloitte.com',
    username: 'Akshay Nayak'
  },
  LOCAL_USERS: []
};

/*export const environment = {
  production: false,
  isLocal: true,
  BASE_URL: 'https://devapi.npdoracleascend.deloitte.com',//'http://172.16.25.60:8000',

  AUTH_CONFIG: {
    clientID: 'f06af652-f635-43d1-a5f7-b3c7fe3b6771',
    authority: "https://login.microsoftonline.com/36da45f1-dd2c-4d1f-af13-5abe46b99921",
    redirectUri: 'https://doascend.deloitte.com',
    cacheLocation: "sessionStorage",
    postLogoutRedirectUri: "https://doascend.deloitte.com",
    navigateToLoginRequestUrl: false,
    consentScopes: ["user.read", "offline_access"],
    unprotectedResources: ["https://www.microsoft.com/en-us/"],
    logger: (logLevel, message, piiEnabled) => undefined,
    correlationId: '1234',
    piiLoggingEnabled: true
  },
  AUTH_RESOURCE_LINK: {
    self: "https://graph.microsoft.com/v1.0/me",
    users: "https://graph.microsoft.com/v1.0/users",
    webAPI: "https://devapi.npdoracleascend.deloitte.com/user_impersonation"
  },
  LOCAL_AUTH: {
    userId: 'aknayak@deloitte.com',
    username: 'Akshay Nayak'
  },
  LOCAL_USERS: [
    { userId: "ascendadmin@xyz.com", userName: "User1 (Ascend Admin)", access: "SU" },
    { userId: "projectadmin@xyz.com", userName: "User2 (Project Admin)", access: "PA" },
    { userId: "aknayak@deloitte.com", userName: "User3 (Team Member)", access: "TM" }
  ]
};*/

/*
 * For easier debugging in development mode, you can import the following file
 * to ignore zone related error stack frames such as `zone.run`, `zoneDelegate.invokeTask`.
 *
 * This import should be commented out in production mode because it will have a negative impact
 * on performance if an error is thrown.
 */
// import 'zone.js/dist/zone-error';  // Included with Angular CLI.

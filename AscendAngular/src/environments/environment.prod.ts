export const environment = {
    production: false,
    isLocal: false,
    BASE_URL: 'https://azas-uscon-ascend-api-prod.azurewebsites.net',//'http://172.16.25.60:8000',

    AUTH_CONFIG: {
      clientID: '68cf3ece-fceb-430b-a6dd-2963289381ef',
      authority: "https://login.microsoftonline.com/36da45f1-dd2c-4d1f-af13-5abe46b99921",
      redirectUri: 'https://ascend.deloitte.com',
      cacheLocation : "sessionStorage",
      postLogoutRedirectUri: "https://ascend.deloitte.com",
      navigateToLoginRequestUrl: false,
      consentScopes: ["https://azas-uscon-ascend-api-prod.azurewebsites.net/user_impersonation"],
      unprotectedResources: ["https://www.microsoft.com/en-us/"],
      logger: (logLevel, message, piiEnabled) => undefined,
      correlationId: '1234',
      piiLoggingEnabled: true
    },
    AUTH_RESOURCE_LINK: {
      self: "https://graph.microsoft.com/v1.0/me",
      users: "https://graph.microsoft.com/v1.0/users",
      webAPI: "https://azas-uscon-ascend-api-prod.azurewebsites.net/user_impersonation"
    },
    LOCAL_AUTH: {
      userId: '',
      username: ''
    },
    LOCAL_USERS: []
  };
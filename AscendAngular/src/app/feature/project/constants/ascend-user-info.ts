import { UserInfo } from 'src/app/shared/constants/ascend-user-project-info';

export class SSOUSERI {
    businessPhones: string[];
    displayName: string;
    givenName: string;
    jobTitle: string;
    mail: string;
    mobilePhone: string;
    officeLocation: string;
    preferredLanguage: string;
    surname: string;
    userPrincipalName: string;
    id: string;
}

export class User {
    userId: String;
    userName: String;
    ssoUser: SSOUSERI
    projectDetails: UserInfo;

    constructor(userId?: String, name?: String) {
        this.userId = userId; 
        this.userName = name;
        this.ssoUser = new SSOUSERI();
    }

    setssoUserDetails(ssoUser: SSOUSERI) {
        this.ssoUser = ssoUser;
    }

    setProjectDetails(userInfo: UserInfo) {
        this.projectDetails = userInfo;
    }
}
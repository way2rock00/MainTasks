import { NgModule } from "@angular/core";

import { MsalModule, MsalConfig } from '@azure/msal-angular';

import { environment } from 'src/environments/environment';
  
/*
export const protectedResourceMap: [string, string[]][] = [
    [environment.AUTH_RESOURCE_LINK.self, ['user.read']],
    [environment.AUTH_RESOURCE_LINK.users, ['user.read']]
];
*/

export const protectedResourceMap:[string, string[]][]=[
    [environment.AUTH_RESOURCE_LINK.self, ['user.read']],
    [environment.AUTH_RESOURCE_LINK.users, ['user.read']],
    [`${environment.BASE_URL}/userInfo`,[environment.AUTH_RESOURCE_LINK.webAPI]],
    [`${environment.BASE_URL}/userlist`,[environment.AUTH_RESOURCE_LINK.webAPI]]
];


const isIE = window.navigator.userAgent.indexOf('MSIE ') > -1 || window.navigator.userAgent.indexOf('Trident/') > -1;


const AUTH_CONFIG: MsalConfig = ({
    ...(environment.AUTH_CONFIG),
        validateAuthority: true,
        loadFrameTimeout:30000,
        storeAuthStateInCookie: isIE,
        protectedResourceMap
} as MsalConfig)


@NgModule({
    imports: [MsalModule.forRoot(AUTH_CONFIG)],
    exports: [MsalModule]
})

export class AuthenticationModule {}
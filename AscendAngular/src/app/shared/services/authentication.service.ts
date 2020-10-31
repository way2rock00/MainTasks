import { Injectable } from "@angular/core";
import { Subscription, forkJoin, Observable } from 'rxjs';
import { BroadcastService, MsalService } from '@azure/msal-angular';
import { HttpClient } from '@angular/common/http';
import { User, SSOUSERI } from 'src/app/feature/project/constants/ascend-user-info';
import { MessagingService } from './messaging.service';
import { BUS_MESSAGE_KEY } from '../constants/message-bus';
import { environment } from '../../../environments/environment';
import { UserInfo } from '../constants/ascend-user-project-info';


@Injectable({
    providedIn: 'root'
})
export class AuthenticationService {
    userData: User;

    private fetchInProgress: boolean;
    private authListeners: Subscription[] = [];

    constructor(
        private authService: MsalService,
        private broadcastService: BroadcastService,
        private messagingService: MessagingService,
        private http: HttpClient
    ) {}

    /**
     *  trigger this method to initialize authentication flow
     *  when we trigger this from local, auth is not required, send a user to mimick the flow
     *  @param user 
     */
    initializeAuth(user?: any): void {
        console.log('In initializeAuth');
        if (environment.isLocal) {
            this.triggerLocalAuth(user);
        } else {
            this.triggerSSOAuth();
        }
    }

    /**
     * @returns Logged in User and details,
     * better use this.messagingService.subscribe(BUS_MESSAGE_KEY.USER_DETAILS, () => {});
     */
    getUser() {
        console.log('In getUser');
        return this.userData;
    }

    /**
     * triggers the login flow
     */
    login() {
        //this.authService.loginRedirect();
        console.log('Trying to log in now');
        this.authService.loginRedirect([environment.AUTH_RESOURCE_LINK.webAPI, "openid", "profile","user.read"]);
    }

    /**
     * logout user
     */
    logout() {
        this.authService.logout();
        this.messagingService.publish(BUS_MESSAGE_KEY.USER_DETAILS, null);
    }

    loginInProgress():boolean {
        return (this.fetchInProgress || this.authService.loginInProgress());
    }

    /**
     * unsubscribe all listeners
     */
    cleanListerners() {
        for (let subscriber of this.authListeners) {
            if (subscriber) {
                subscriber.unsubscribe();
            }
        }

        this.broadcastService.getMSALSubject().next(1);
    }

    /**
     * 
     * @param user - the user with which to update the user details
     */
    private triggerLocalAuth(user: User): void {
        console.log('Trigerring Local Login');
        if (!user || !user.userId) {
            this.userData = new User(environment.LOCAL_AUTH.userId, environment.LOCAL_AUTH.username);
        } else {
            this.userData = new User(user.userId, user.userName);
        }
        this.getUserProjectDetails();
    }


    /**
     * checks if user present, else triggers the login flow
     */
    private triggerSSOAuth(): void {
        console.log('Trigerring SSO Login');
        const user = this.authService.getUser();
        console.log(user)
        console.log('Printing ID_TOKEN NULL CHECK');
        if(user!=null)
        console.log(user.idToken);
        if (user) {
            this.userData = new User(user.displayableId, user.name)
            this.setupAuthListeners();

            //if we need sso forkJoin this call
            //this.getUserProfile();
            this.getUserProjectDetails();
        } else {
            this.login();
        }
    }

    /**
     * setsup the auth listeners
     *  failure
     *  success
     *  acquireTokenFailure
     */
    private setupAuthListeners() {
        /* -- first clean the listeners if any -- */
        this.cleanListerners();
        this.listenLoginFailure();
        this.listenLoginSuccess();
        this.listenAcquireTokenFailure();
    }

    /**
     * check if the login failed
     * todo: Create a 401 and redirect
     */
    private listenLoginFailure() {
        console.log('Registering listenLoginFailure');
        /* -- 401-- */
        this.authListeners
        .push(
            this.broadcastService
            .subscribe("msal:loginFailure", () => this.messagingService.publish(BUS_MESSAGE_KEY.USER_DETAILS, null))
        );
    }

    /**
     * login is success
     * get user
     */
    private listenLoginSuccess() {
        console.log('Registering listenLoginSuccess');
        /*this.authListeners
        .push(
            this.broadcastService
            .subscribe("msal:loginSuccess", () => this.getUserProfile())
        );*/
        this.authListeners
        .push(
            this.broadcastService
            .subscribe("msal:loginSuccess", (payload) =>{ 
                console.log('Printing Payload obtained after successfull login')
                console.log(payload);
                this.getUserProfile();
            })
        );
    }

    /**
     * if token failed, handle the failure
     */
    private listenAcquireTokenFailure() {
        console.log('Registering listenAcquireTokenFailure');
        //will work for acquireTokenSilent and acquireTokenPopup
        this.authListeners
        .push(
            this.broadcastService
            .subscribe("msal:acquireTokenFailure", (payload) => this.handleTokenFailure(payload))
        )
    }

    /**
     * 
     * @param tokenFailureResponse - the token failure response
     */
    private handleTokenFailure(tokenFailureResponse) {
        console.log('Token failure custom message check')
        console.log(tokenFailureResponse)
        console.log(this.authService.loginInProgress());
        if (tokenFailureResponse.errorDesc.indexOf("consent_required") !== -1 || tokenFailureResponse.errorDesc.indexOf("interaction_required") != -1) {
            if(!this.authService.loginInProgress()) {
                //check for acquire silect token 
                this.authService.acquireTokenPopup(["user.read"]).then((token) => {
                    this.getUserProfile();
                }, (error) => {
                });
            }
            
        }
    }

    /**
     * fetch the user details from SSO
     * this may not be needed and hence not invoked
     */
    private getUserProfile() {
        console.log('In getUserProfile')
        return this.http.get<SSOUSERI>(environment.AUTH_RESOURCE_LINK.self)
        .subscribe(
                data => {
                    this.userData.setssoUserDetails(data);
                    return data;
                }, 
                error => console.error(" Http get request to MS Graph failed" + JSON.stringify(error)
            )
        );
    }

    /**
     * get the project details for the logged in user, this decides many access levels
     */
    private getUserProjectDetails(): void {
        console.log('In getUserProjectDetails')
        this.fetchInProgress = true;
        const url = `${environment.BASE_URL}/userInfo/${this.userData.userId}`;
        console.log('URL: '+url)
        this.http.get<UserInfo[]>(url)
        .subscribe(
            (data: UserInfo[]) => {
                console.log('Getting user data successful');
                if (data && data.length) {
                    this.fetchInProgress = false;
                    this.userData.setProjectDetails(data[0]);
                    this.messagingService.publish(BUS_MESSAGE_KEY.USER_DETAILS, this.userData);
                    return data[0];
                } else {
                    this.messagingService.publish(BUS_MESSAGE_KEY.USER_DETAILS, this.userData);
                    return {};
                }
            },
            error =>  {
                console.error("UNABLE to fetch user data:Error Message:"+error);
                this.fetchInProgress = false;
            }
        )
    }
}
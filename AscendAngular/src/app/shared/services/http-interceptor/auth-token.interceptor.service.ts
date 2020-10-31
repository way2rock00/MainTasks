import { Injectable } from "@angular/core";
import { HttpEvent, HttpHandler, HttpInterceptor, HttpRequest, HttpErrorResponse } from "@angular/common/http";
import { Observable, throwError } from "rxjs";
import { catchError } from "rxjs/operators";
import { MsalService } from '@azure/msal-angular';
import { environment } from 'src/environments/environment';
import { fromPromise } from 'rxjs/internal-compatibility';

@Injectable()
export class AuthTokenInterceptor implements HttpInterceptor {

    //readonly SCOPE = ["user.read"];
     SCOPE = [environment.AUTH_RESOURCE_LINK.webAPI];

    constructor(
        private msalAuthService: MsalService
    ) {}



    intercept(req: HttpRequest<any>, next: HttpHandler):Observable<HttpEvent<any>> {
        if (!environment.isLocal && req.url.indexOf('graph.microsoft.com')==-1 ) { 
            console.log('In Intercept')
            return fromPromise(
                this.getTokenFromMSAL(req.url)
                .then(function (token) {
                    var JWT = "Bearer " + token;
                    console.log(req.headers)
                    return req.clone({
                        setHeaders: {
                            Authorization: JWT,
                        },
                    });
                })
            ).mergeMap(
                (req) => {
                    console.log('In Merge Map')
                    return next.handle(req).pipe(

                        catchError((err) => {
                            console.log(err)
                            if (err instanceof HttpErrorResponse && err.status == 401) {
                                console.error('ACTION TODO: SHOW A GENERIC 401 MODAL');
                            }
                            return throwError(err);
                        })
                    ) 
                }
            );
            
        } else {
            return next.handle(req).pipe(
                catchError((err) => {
                    return throwError(err);
                })
            )
        }
    }

    private async getTokenFromMSAL(url): Promise<any> {
        return new Promise((resolve, reject) => {
            console.log('In getTokenFromMSAL')
            if(url.indexOf('graph.microsoft.com')>-1)
                this.SCOPE = ["user.read"];
            if (this.msalAuthService.getUser()) {                
                this.msalAuthService.acquireTokenSilent(this.SCOPE)
                    .then(token => {
                        console.log('Custom ***** Token:'+token);
                        resolve(token);
                    })
                    .catch(err => {
                        // could also check if err instance of InteractionRequiredAuthError if you can import the class.
                       console.log('error details ' + err.name);
                        this.msalAuthService.acquireTokenPopup(this.SCOPE)
                        if (err.name === "InteractionRequiredAuthError") {
                            this.msalAuthService.acquireTokenPopup(this.SCOPE)
                            .then(token => {
                                // get access token from response
                                resolve(token);
                            })
                            .catch(err => {
                                console.error( `401 - Couldn't fetch access token!`);
                                reject('401');
                            });
                        }
                    });
            } else {
                console.error( `401 - Couldn't fetch access token - Log in the USER!`);
                reject('401');
            }
        })
    }
}
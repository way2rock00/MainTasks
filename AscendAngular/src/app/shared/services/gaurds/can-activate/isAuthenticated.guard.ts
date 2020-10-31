import { Injectable } from '@angular/core';
import { CanActivate, ActivatedRouteSnapshot, RouterStateSnapshot, Router } from '@angular/router';
import { Observable, of } from 'rxjs';

import { AuthenticationService } from '../../authentication.service';
import { MessagingService } from '../../messaging.service';
import { BUS_MESSAGE_KEY } from 'src/app/shared/constants/message-bus';

@Injectable()
export class IsAuthenticatedGuard implements CanActivate {
    constructor(
        private authService: AuthenticationService,
        private messagingServive: MessagingService,
        private router: Router
    ) {}

    canActivate(route: ActivatedRouteSnapshot, state: RouterStateSnapshot): Observable<boolean> {
        return new Observable((observer) => {
            console.log('In canActivate. Start')
            this.messagingServive
            .subscribe(BUS_MESSAGE_KEY.USER_DETAILS,
                (user) => {
                    console.log(JSON.stringify(user));
                    if (user) {
                        console.log('User object found');
                        if (user.userId && user.projectDetails) {
                            observer.next(true);
                        } else {
                            this.router.navigateByUrl('/unauthorized');
                        }
                    } else if (!this.authService.loginInProgress()) {
                        console.log('User object not found');
                        this.authService.initializeAuth();
                    }
                }
            )
        });
    }
}
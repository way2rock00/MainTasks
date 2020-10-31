import { Injectable } from '@angular/core';
import { CanActivate, ActivatedRouteSnapshot, RouterStateSnapshot, Router } from '@angular/router';
import { Observable, of } from 'rxjs';

import { AuthenticationService } from '../../authentication.service';

@Injectable()
export class IsAdminGuard implements CanActivate {
    constructor(
        private authService: AuthenticationService,
        private router: Router
    ) {}

    canActivate(route: ActivatedRouteSnapshot, state: RouterStateSnapshot): Observable<boolean> {
        const user = this.authService.getUser();
        if (user && user.projectDetails && user.projectDetails.isAscendAdmin === "true") {
            return of(true)
        } else {
            this.router.navigateByUrl('/unauthorized');
        }
    }
}
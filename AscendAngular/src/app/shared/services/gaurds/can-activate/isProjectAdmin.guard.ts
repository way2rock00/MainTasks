import { Injectable } from '@angular/core';
import { CanActivate, ActivatedRouteSnapshot, RouterStateSnapshot, Router } from '@angular/router';
import { Observable, of } from 'rxjs';

import { AuthenticationService } from '../../authentication.service';

@Injectable()
export class IsProjectAdminGuard implements CanActivate {

    constructor(
        private authService: AuthenticationService,
        private router: Router
    ) {}

    canActivate(route: ActivatedRouteSnapshot, state: RouterStateSnapshot): Observable<boolean> {
        const user = this.authService.getUser();
        if (user && user.projectDetails 
            && (user.projectDetails.isAscendAdmin === "true" || this.isProjectAdmin(user.projectDetails.projectInfo, route.params['projectId'])) 
        ) {
            return of(true)
        } else  {
            this.router.navigateByUrl('/unauthorized');
        }
    }

    isProjectAdmin(projectDetails: any[], projectId: string) {
        let isProjectAdmin = false;

        for (let project of projectDetails) {
            if (project.projectId == projectId) {
                isProjectAdmin = project.projectRole === 'PROJECT_ADMIN'
                break;
            }
        }

        return isProjectAdmin;
    }
}
import { Component, OnInit } from '@angular/core';
import { Router } from '@angular/router';
import { environment } from 'src/environments/environment';
import { AuthenticationService } from 'src/app/shared/services/authentication.service';

@Component({
  selector: 'app-user-selection',
  templateUrl: './user-selection.component.html',
  styleUrls: ['./user-selection.component.scss']
})
export class UserSelectionComponent {

  user : {userId: string};

  defaultUserId: string;
  defaultUserName: string;

  userDetails = environment.LOCAL_USERS;

  constructor(
    private router: Router,
    private authService: AuthenticationService
  ) { 
    this.defaultUserId = environment.LOCAL_AUTH.userId;
    this.defaultUserName = environment.LOCAL_AUTH.username;
  }

  next(){
    if (this.user) {
      this.authService.initializeAuth(this.user)
    }
    this.router.navigate(['/welcome']);
  }

}

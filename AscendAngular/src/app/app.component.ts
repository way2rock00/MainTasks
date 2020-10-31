import { Component, OnInit, OnDestroy } from '@angular/core';
import { MessagingService } from './shared/services/messaging.service';
import { BehaviorSubject } from 'rxjs';
import { BUS_MESSAGE_KEY } from './shared/constants/message-bus';
import { AuthenticationService } from './shared/services/authentication.service';
import { Router } from '@angular/router';

@Component({
  selector: 'app-root',
  templateUrl: './app.component.html',
  styleUrls: ['./app.component.scss']
})
export class AppComponent implements OnInit, OnDestroy {
  showLoader : BehaviorSubject<boolean>;
  userData   : BehaviorSubject<boolean>;

  constructor(
    private router: Router,
    private messagingService: MessagingService,
    private authService: AuthenticationService) {}

  ngOnInit() {
    this.showLoader = this.messagingService.getBus(BUS_MESSAGE_KEY.LOADER);
    this.userData   = this.messagingService.getBus(BUS_MESSAGE_KEY.USER_DETAILS);

    this.messagingService.publish(BUS_MESSAGE_KEY.LOADER, false);
    this.authService.initializeAuth();
  }

  ngOnDestroy() {
    this.authService.cleanListerners();
  }
}

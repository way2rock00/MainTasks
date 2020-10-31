import { CryptUtilService } from './../../../shared/services/crypt-util.service';
import { Component, OnInit } from '@angular/core';
import { Router } from '@angular/router';

@Component({
  selector: 'app-welcome',
  templateUrl: './welcome.component.html',
  styleUrls: ['./welcome.component.scss']
})
export class WelcomeComponent implements OnInit {

  constructor(private router: Router
    , private cryptUtilService: CryptUtilService) { }

  ngOnInit() {
  }

  start(route) {
    localStorage.clear();
    this.cryptUtilService.sessionClear();
    this.router.navigate([route]);
  }

}

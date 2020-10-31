import { Component, OnInit, Input, Output, EventEmitter } from "@angular/core";
import { SharedService } from "src/app/shared/services/shared.service";
import { MatDialog } from "@angular/material";
import { Router, NavigationEnd } from '@angular/router';
import { Subscription } from 'rxjs';
import { UtilService } from 'src/app/shared/services/util.service';
import { CryptUtilService } from 'src/app/shared/services/crypt-util.service';

@Component({
  selector: 'app-launch-journey-tab-group',
  templateUrl: './launch-journey-tab-group.component.html',
  styleUrls: ['./launch-journey-tab-group.component.scss']
})
export class LaunchJourneyTabGroupComponent implements OnInit {

  @Input() layoutConfig: any;
  @Output() tabChanged = new EventEmitter();

  @Input() phaseName: any;//tab changes
  @Input() stopName: any;//tab changes

  bgColor: string;
  tabs: any[];
  tabCount: string = ""; //Filter changes
  currentTab: any; //Filter changes
  routerSubscription: Subscription; //Filter changes

  constructor(
    private sharedService: SharedService,
    public dialog: MatDialog,
    private router: Router,
    private cryptUtilService: CryptUtilService, //tab changes
    private utilService: UtilService //tab changes
  ) { }

  ngOnInit() {

    this.bgColor = this.layoutConfig.right.colorScheme;
    // this.tabs = this.layoutConfig.right.tabs;

    //Filter changes
    //Get current tab from router link initially for first load
    // this.setCurrentTab();

    // this.routerSubscription = this.router.events.subscribe(val => {
    //   if (val instanceof NavigationEnd) {
    //     this.setCurrentTab();
    //   }
    // });

    //tab changes
    let serviceData = this.cryptUtilService.getItem(this.phaseName + this.stopName, 'SESSION') || [];

    if (serviceData.length == 0) {
      this.utilService.getTabInfo(`/allTabs/${this.phaseName}/${this.stopName}`).subscribe(data => {
        this.tabs = data[0].tab;
        this.cryptUtilService.setItem(this.phaseName + this.stopName, data[0].tab, 'SESSION');
        this.setCurrentTab();
      });
    }

    else {
      this.tabs = serviceData;
      this.setCurrentTab();
    }

    //Filter changes, clear tab count if any
    this.sharedService.tabCountEvent.emit("");
    //Filter changes, subscribe to tabcount
    this.sharedService.tabCountEvent.subscribe(e => { this.tabCount = e });

  }

  //Filter changes
  setCurrentTab() {
    this.currentTab = this.tabs.find(t => decodeURI(this.router.url) == t.tabURL); //tab changes
    this.tabChanged.emit(this.currentTab.tabCode);//Emit TabCode
    this.sharedService.selectedTabEvent.emit(this.currentTab);
    this.tabCount = "";
  }

  //tab changes
  setCurrentTabNew(tab) {
    this.currentTab = tab;
    // this.tabChanged.emit(this.currentTab.tabCode);//Emit TabCode
    this.sharedService.selectedTabEvent.emit(this.currentTab);
    this.tabCount = "";
  }

  ngOnDestroy() {
    //tab changes
    // this.routerSubscription.unsubscribe();
  }

}

import { Subscription } from 'rxjs';
import { Component, Input, OnInit, ViewChild, ViewEncapsulation } from '@angular/core';
import { TAB_SAVE_CONST } from 'src/app/shared/constants/tab-change-save-dialog';
import { SharedService } from 'src/app/shared/services/shared.service';
import { MatTabGroup } from '@angular/material';

@Component({
  selector: 'app-activity-tabs',
  templateUrl: './activity-tabs.component.html',
  styleUrls: ['./activity-tabs.component.scss'],
  encapsulation: ViewEncapsulation.None
})
export class ActivityTabsComponent implements OnInit {

  @Input() buttonDetails;
  @Input() functionPackageURL;
  @Input() activity;
  @Input() urlParams;
  @Input() colorScheme;
  @Input() textColorScheme;

  @ViewChild('tabs', { static: false }) tabGroup: MatTabGroup;

  currentTabCode: string;
  defaultTabIndex: number = 0;

  tabCount: any = ''; //tab count change

  subscription: Subscription;
  tabCountSubscription: Subscription;//tab count change
  tabChangeSubscription: Subscription;

  constructor(private sharedService: SharedService) { }

  ngOnInit() {
    this.subscription = this.sharedService.dataChangeEvent.subscribe(data => {
      if (data.type == 2 && data.source == TAB_SAVE_CONST.TAB_CHANGE)
        this.currentTabCode = this.buttonDetails[data.data.index].code;
    });

    //tab count change
    this.tabCountSubscription = this.sharedService.tabCountEvent.subscribe(data => {
      this.tabCount = data;
    });

    this.tabChangeSubscription = this.sharedService.docAddEvent.subscribe(data => {
      if (data == 'FAILED') {
        this.tabGroup.selectedIndex = this.defaultTabIndex;
      }
    })
  }

  ngOnChanges() {
    this.tabCount = '';
    if (this.urlParams.routedFrom.toUpperCase() != 'IIDR') {
      this.currentTabCode = this.buttonDetails[0].code;
      for (let i = 0; i < this.buttonDetails.length; i++) {
        if (this.buttonDetails[i].code.toUpperCase() == this.urlParams.tabCode.toUpperCase()) {
          this.defaultTabIndex = i;
          this.currentTabCode = this.urlParams.tabCode;
        }
      }
    } else {
      this.currentTabCode = this.buttonDetails[0].code;
    }
  }

  tabChange(tab) {
    this.tabCount = '';
    if (this.sharedService.toggled.toUpperCase() == 'TOGGLED') {
      let dataChangeEventObj = {
        source: TAB_SAVE_CONST.TAB_CHANGE,
        data: tab,
        type: 1
      }
      this.sharedService.dataChangeEvent.emit(dataChangeEventObj);
    } else {
      this.defaultTabIndex = tab.index;
      this.currentTabCode = this.buttonDetails[tab.index].code;
    }
  }

  ngOnDestroy() {
    this.subscription.unsubscribe();
    this.tabCountSubscription.unsubscribe();
  }
}

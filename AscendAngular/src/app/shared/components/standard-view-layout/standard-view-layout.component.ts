import { SharedService } from 'src/app/shared/services/shared.service';
import { Component, Input, OnInit } from '@angular/core';
import { LAYOUT_TYPE, LAYOUT_CONFIGURATION } from '../../constants/layout-constants';
import { Router } from '@angular/router';
import { BUS_MESSAGE_KEY } from '../../constants/message-bus';
import { MessagingService } from '../../services/messaging.service';

@Component({
  selector: 'app-standard-layout',
  templateUrl: './standard-view-layout.component.html',
  styleUrls: ['./standard-view-layout.component.scss']
})
export class StandardViewLayoutComponent implements OnInit {

  @Input()
  layout: LAYOUT_TYPE;
  @Input()
  layoutSubCat: any;
  @Input()
  filters: any;

  layoutConfig: any;

  shrinkRight: boolean;

  constructor(
    private router: Router,
    private messagingService: MessagingService,
    private sharedService: SharedService
  ) { }

  ngOnInit() {
    this.sharedService.selectedTabEvent.subscribe(data => {
      this.messagingService.publish(BUS_MESSAGE_KEY.STOP_NAME, this.layoutSubCat + data.tabCode);
    })
    this.layoutConfig = LAYOUT_CONFIGURATION[this.layout][this.layoutSubCat];
  }

  leftExpanded(isExpanded) {
    this.shrinkRight = isExpanded;
  }

  filterChanged(filter) {
    this.messagingService.publish(BUS_MESSAGE_KEY.GLOBAL_FILTER, filter);
  }

  goToPage(pageName: string) {
    this.router.navigate([`${pageName}`]);
  }
}

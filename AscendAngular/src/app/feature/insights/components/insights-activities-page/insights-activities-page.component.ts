import { Component, OnInit } from '@angular/core';
import { ActivatedRoute } from '@angular/router';
import { LAYOUT_CONFIGURATION, LAYOUT_INSIGHTS_SUB_NAV, LAYOUT_TYPE } from 'src/app/shared/constants/layout-constants';
import { SharedService } from 'src/app/shared/services/shared.service';

@Component({
  selector: 'app-insights-activities-page',
  templateUrl: './insights-activities-page.component.html',
  styleUrls: ['./insights-activities-page.component.scss']
})
export class InsightsActivitiesPageComponent implements OnInit {

  constructor(private activatedRoute: ActivatedRoute, private sharedService:SharedService) { }

  LAYOUT: LAYOUT_TYPE = LAYOUT_TYPE.INSIGHTS;
  SUB_NAV: LAYOUT_INSIGHTS_SUB_NAV;
  layoutConfig: any;
  routeLink:any;

  ngOnInit() {
    this.activatedRoute.paramMap.subscribe(params => {
      this.selectionChanged(params.get("stopName"));
    })

    this.sharedService.selectedPageEvent.emit(this.LAYOUT);
  }

  selectionChanged(stopName) {
    switch (stopName) {
      case 'discover':
        this.SUB_NAV = LAYOUT_INSIGHTS_SUB_NAV.DISCOVER;
        this.routeLink = '../../../insights/discover';
        break;
      case 'develop':
        this.SUB_NAV = LAYOUT_INSIGHTS_SUB_NAV.DEVELOP;
        this.routeLink = '../../../insights/develop';
        break;
      case 'estimate':
        this.SUB_NAV = LAYOUT_INSIGHTS_SUB_NAV.ESTIMATE;
        this.routeLink = '../../../insights/estimate';
        break;
      case 'establish':
        this.SUB_NAV = LAYOUT_INSIGHTS_SUB_NAV.ESTABLISH;
        this.routeLink = '../../../insights/establish';
        break;
      case 'create':
        this.SUB_NAV = LAYOUT_INSIGHTS_SUB_NAV.CREATE;
        this.routeLink = '../../../insights/create';
        break;
      default:
        break;
    }
    this.layoutConfig = LAYOUT_CONFIGURATION[this.LAYOUT][this.SUB_NAV];
  }


}

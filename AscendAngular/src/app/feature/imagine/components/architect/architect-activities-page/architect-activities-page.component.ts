import { Component, OnInit } from '@angular/core';
import { LAYOUT_CONFIGURATION, LAYOUT_TYPE, LAYOUT_IMAGINE_SUB_NAV } from 'src/app/shared/constants/layout-constants';

@Component({
  selector: 'app-architect-activities-page',
  templateUrl: './architect-activities-page.component.html',
  styleUrls: ['./architect-activities-page.component.scss']
})
export class ArchitectActivitiesPageComponent implements OnInit {

  LAYOUT: LAYOUT_TYPE = LAYOUT_TYPE.IMAGINE;
  SUB_NAV: LAYOUT_IMAGINE_SUB_NAV = LAYOUT_IMAGINE_SUB_NAV.ARCHITECT;
  layoutConfig = LAYOUT_CONFIGURATION[this.LAYOUT][this.SUB_NAV];

  constructor() { }

  ngOnInit() {
  }

}

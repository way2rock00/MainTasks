import { Component, OnInit, Input } from '@angular/core';

@Component({
  selector: 'app-info-insights-wheel',
  templateUrl: './info-insights-wheel.component.html',
  styleUrls: ['./info-insights-wheel.component.scss']
})
export class InfoInsightsWheelComponent implements OnInit {

  @Input() layoutSubCat: string;

  constructor() { }

  ngOnInit() {
  }

}

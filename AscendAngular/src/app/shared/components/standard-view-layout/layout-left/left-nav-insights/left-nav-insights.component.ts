import { Component, OnInit, Input } from '@angular/core';

@Component({
  selector: 'app-left-nav-insights',
  templateUrl: './left-nav-insights.component.html',
  styleUrls: ['./left-nav-insights.component.scss']
})
export class LeftNavInsightsComponent implements OnInit {
  @Input() layoutSubCat: string;
  constructor() { }

  ngOnInit() {
  }

}

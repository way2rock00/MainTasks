import { Component, OnInit, Input, Output, EventEmitter } from '@angular/core';

@Component({
  selector: 'app-left-insights-wheel',
  templateUrl: './left-insights-wheel.component.html',
  styleUrls: ['./left-insights-wheel.component.scss']
})
export class LeftInsightsWheelComponent implements OnInit {

  @Input() layoutSubCat: string;

  @Output() selectionChanged = new EventEmitter();

  constructor() { }

  ngOnInit() {
  }

  changeSelection(stopName){
    this.selectionChanged.emit(stopName);
  }

}

import { Component, OnInit, Input } from '@angular/core';

@Component({
  selector: 'app-info-run-wheel',
  templateUrl: './info-run-wheel.component.html',
  styleUrls: ['./info-run-wheel.component.scss']
})
export class InfoRunWheelComponent implements OnInit {

  @Input() layoutSubCat: string;

  constructor() { }

  ngOnInit() {
  }

}

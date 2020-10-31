import { Component, OnInit, Input } from '@angular/core';

@Component({
  selector: 'app-info-deliver-wheel',
  templateUrl: './info-deliver-wheel.component.html',
  styleUrls: ['./info-deliver-wheel.component.scss']
})
export class InfoDeliverWheelComponent implements OnInit {

  @Input() layoutSubCat: string;

  constructor() { }

  ngOnInit() {
  }

}

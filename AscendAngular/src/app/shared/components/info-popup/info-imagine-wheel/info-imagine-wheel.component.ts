import { Component, OnInit, Input } from '@angular/core';

@Component({
  selector: 'app-info-imagine-wheel',
  templateUrl: './info-imagine-wheel.component.html',
  styleUrls: ['./info-imagine-wheel.component.scss']
})
export class InfoImagineWheelComponent implements OnInit {

  @Input() layoutSubCat: string;

  constructor() { }

  ngOnInit() {
  }

}

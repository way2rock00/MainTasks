import { Component, OnInit, Input } from '@angular/core';

@Component({
  selector: 'app-left-imagine-wheel',
  templateUrl: './left-imagine-wheel.component.html',
  styleUrls: ['./left-imagine-wheel.component.scss']
})
export class LeftImagineWheelComponent implements OnInit {

  @Input() layoutSubCat: string;

  constructor() { }

  ngOnInit() {
  }

}

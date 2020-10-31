import { Component, OnInit,Input } from '@angular/core';

@Component({
  selector: 'app-left-nav-imagine',
  templateUrl: './left-nav-imagine.component.html',
  styleUrls: ['./left-nav-imagine.component.scss']
})
export class LeftNavImagineComponent implements OnInit {
 @Input() layoutSubCat: string;
  constructor() { }

  ngOnInit() {
  }

}

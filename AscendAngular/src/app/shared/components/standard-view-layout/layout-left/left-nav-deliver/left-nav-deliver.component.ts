import { Component, OnInit, Input } from '@angular/core';

@Component({
  selector: 'app-left-nav-deliver',
  templateUrl: './left-nav-deliver.component.html',
  styleUrls: ['./left-nav-deliver.component.scss']
})
export class LeftNavDeliverComponent implements OnInit {
 @Input() layoutSubCat: string;
  constructor() { }

  ngOnInit() {
  }

}

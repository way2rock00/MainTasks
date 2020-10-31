import { Component, OnInit, Input } from '@angular/core';

@Component({
  selector: 'app-amplifiers',
  templateUrl: './amplifiers.component.html',
  styleUrls: ['./amplifiers.component.scss']
})
export class AmplifiersComponent implements OnInit {

  @Input() array : any[];
  @Input() title : string;
  @Input() headColor: string;
  @Input() bodyColor : string;

  constructor() { }

  ngOnInit() {
  }

}

import { Component, OnInit, Input } from '@angular/core';

@Component({
  selector: 'app-digital-maturity',
  templateUrl: './digital-maturity.component.html',
  styleUrls: ['./digital-maturity.component.scss']
})
export class DigitalMaturityComponent implements OnInit {

  @Input() array : any[];
  @Input() title : string;
  @Input() headColor : string;
  @Input() bodyColor : string;

  constructor() { }

  ngOnInit() {
  }

}

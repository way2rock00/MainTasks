import { Component, OnInit, Input } from '@angular/core';

@Component({
  selector: 'app-impact',
  templateUrl: './impact.component.html',
  styleUrls: ['./impact.component.scss']
})
export class ImpactComponent implements OnInit {

  @Input() impactData: any;

  constructor() { }

  ngOnInit() {
  }

}

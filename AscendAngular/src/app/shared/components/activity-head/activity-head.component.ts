import { Component, OnInit, Input } from '@angular/core';

@Component({
  selector: 'app-activity-head',
  templateUrl: './activity-head.component.html',
  styleUrls: ['./activity-head.component.scss']
})
export class ActivityHeadComponent implements OnInit {

  @Input() title: string;
  @Input() subtitle: string;
  @Input() description: string;
  @Input() routeLink : string;

  constructor() { }

  ngOnInit() {
  }

}

import { Component, OnInit, Input } from '@angular/core';

@Component({
  selector: 'app-activity-deliverables',
  templateUrl: './activity-deliverables.component.html',
  styleUrls: ['./activity-deliverables.component.scss']
})
export class ActivityDeliverablesComponent implements OnInit {

  @Input() array : any[];
  @Input() title : string;
  @Input() headColor: string;
  @Input() bodyColor : string;

  constructor() { }

  ngOnInit() {
  }

}

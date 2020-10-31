import { Component, OnInit, Input } from '@angular/core';

@Component({
  selector: 'app-left-nav-run',
  templateUrl: './left-nav-run.component.html',
  styleUrls: ['./left-nav-run.component.scss']
})
export class LeftNavRunComponent implements OnInit {
  @Input() layoutSubCat: string;
  constructor() { }

  ngOnInit() {
  }

}

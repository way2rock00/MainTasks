import { Component, OnInit, Input } from '@angular/core';

@Component({
  selector: 'app-problem',
  templateUrl: './problem.component.html',
  styleUrls: ['./problem.component.scss']
})
export class ProblemComponent implements OnInit {

  @Input() statementData: any;
  @Input() descData: any;

  constructor() { }

  ngOnInit() {
  }

}

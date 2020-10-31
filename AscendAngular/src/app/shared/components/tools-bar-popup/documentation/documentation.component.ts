import { Component, OnInit, Input } from '@angular/core';

@Component({
  selector: 'app-documentation',
  templateUrl: './documentation.component.html',
  styleUrls: ['./documentation.component.scss']
})
export class DocumentationComponent implements OnInit {

  @Input() docData: any;

  constructor() { }

  ngOnInit() {
  }

  goto(link){
    window.open(link);
  }

}

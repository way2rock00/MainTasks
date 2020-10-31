import { Component, OnInit, Input } from '@angular/core';

@Component({
  selector: 'app-tech-stack',
  templateUrl: './tech-stack.component.html',
  styleUrls: ['./tech-stack.component.scss']
})
export class TechStackComponent implements OnInit {

  @Input() techstackData: any;

  constructor() { }

  ngOnInit() {
  }

  goto(link){
    if(link)
      window.open(link);
  }

}

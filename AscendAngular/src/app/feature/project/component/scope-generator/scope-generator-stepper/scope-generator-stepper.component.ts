import { Component, OnInit, Input, Output, EventEmitter } from '@angular/core';

@Component({
  selector: 'app-scope-generator-stepper',
  templateUrl: './scope-generator-stepper.component.html',
  styleUrls: ['./scope-generator-stepper.component.scss']
})
export class ScopeGeneratorStepperComponent {

  @Input()
  pits: any[];

  @Output()
  pitClicked: EventEmitter<any> = new EventEmitter<any>();

  onPitClicked(pit: any) {
    if(!pit.active)
      this.pitClicked.emit(pit);
  }

}

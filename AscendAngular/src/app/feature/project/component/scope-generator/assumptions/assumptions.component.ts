import { animate, style, transition, trigger } from '@angular/animations';
import { Component, EventEmitter, Input, OnInit, Output, ViewChild } from '@angular/core';
import { SCOPE_STEPPER_FORM_SEGMENT_TYPE } from '../../../constants/project-scope-generator/project-scope-stepper';
import { TechnologyComponent } from '../technology/technology.component';

const assumptions =
  [{
    "item": "Category 1",
    "id": 1,
    "children": [{
      "item": "sub category 1",
      "id": 101
    },
    {
      "item": "sub category 2",
      "id": 102
    }]
  },
  {
    "item": "Category 2",
    "id": 2,
    "children": [{
      "item": "sub category 1",
      "id": 201
    },
    {
      "item": "sub category 2",
      "id": 202
    }]
  }];

@Component({
  animations: [
    trigger(
      'enterAnimation', [
      transition(':enter', [
        style({ opacity: 0 }),
        animate('300ms', style({ opacity: 1 }))
      ]),
      transition(':leave', [
        style({ opacity: 1 }),
        animate('300ms', style({ opacity: 0 }))
      ])
    ]
    )
  ],
  selector: 'app-assumptions',
  templateUrl: './assumptions.component.html',
  styleUrls: ['./assumptions.component.scss']
})
export class AssumptionsComponent implements OnInit {

  @ViewChild('form', { static: false })
  currentFormElement: TechnologyComponent;

  @Input() savedData: any;
  @Input() allData: any;

  @Output()
  next: EventEmitter<any> = new EventEmitter<any>();

  @Output()
  prev: EventEmitter<any> = new EventEmitter<any>();
  readonly ASSUMPTIONS_TYPE = SCOPE_STEPPER_FORM_SEGMENT_TYPE.ASSUMPTIONS;

  constructor() { }

  ngOnInit() {
    // this.allData = assumptions;
  }

  onNext(clickedSegment?:any) {
    event.preventDefault();
    this.currentFormElement.onNext(clickedSegment)
  }

  onPrev() {
    this.prev.emit();
  }

  isValid(): boolean {
    return true;
  }

  onNextPit(updatedData, direction = 1) {
    let clickedSegment = updatedData.clickedSegment ? updatedData.clickedSegment : null;
    updatedData = clickedSegment ? updatedData.data : updatedData;
    if (clickedSegment)
      this.next.emit({ postData: updatedData, clickedSegment: clickedSegment });
    else
      this.next.emit(updatedData);
  }
}

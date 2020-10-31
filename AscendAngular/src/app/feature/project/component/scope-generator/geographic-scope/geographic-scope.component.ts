import { Component, EventEmitter, Input, OnInit, Output, ViewChild } from '@angular/core';
import { ScopeGeneratorTreeComponent } from '../scope-generator-tree/scope-generator-tree.component';
import { element } from 'protractor';

@Component({
  selector: 'app-geographic-scope',
  templateUrl: './geographic-scope.component.html',
  styleUrls: ['./geographic-scope.component.scss']
})
export class GeographicScopeComponent implements OnInit {

  @Input() geographicalScope: any;
  @Input() savedGeographicalScope: any;

  @Output()
  next: EventEmitter<any> = new EventEmitter<any>();

  @Output()
  prev: EventEmitter<any> = new EventEmitter<any>();

  step = 0;

  stop = 'geographicalScope'

  postData: any[] = [];

  formData: any;

  showError: boolean = false;

  @ViewChild('treeForm', { static: false })
  currentFormElement: ScopeGeneratorTreeComponent;

  constructor() {

  }

  ngOnInit() {
  }

  isValid() {
    return this.currentFormElement.checklistSelection.selected.length == 0 ? false : true;
  }

  onNext() {
    this.showError = true;
    if (this.isValid()) {
      this.showError = false;
      let savedRegionCountries = [];
      if (this.currentFormElement) {
        for (let element of this.geographicalScope.regionCountry) {
          if (this.currentFormElement.checklistSelection.selected.findIndex(t => t.id == element.id) >= 0) {
            let index = savedRegionCountries.push({
              id: element.id,
              item: element.item,
              children: []
            });

            for (let childLevel1 of element.children) {
              if (this.currentFormElement.checklistSelection.selected.findIndex(t => t.id == childLevel1.id) >= 0) {
                savedRegionCountries[index - 1].children.push(
                  {
                    id: childLevel1.id,
                    item: childLevel1.item
                  }
                )
              }
            }
          }
        }
      }
      this.formData = [];
      this.formData = { regionCountry: savedRegionCountries }

      this.postData.push({ projectType: [], geographicalScope: [this.formData], processScope: [], implementationApproach: [], phasePlanning: [] })
      this.next.emit(this.postData);
    }
  }

  onPrev() {
    this.prev.emit()
  }

  submit(data) {
    this.formData = data;
  }
}

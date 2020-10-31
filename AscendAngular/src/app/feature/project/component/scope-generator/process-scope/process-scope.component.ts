import { Component, EventEmitter, Input, OnInit, Output, ViewChild } from '@angular/core';
import { ScopeGeneratorTreeComponent } from '../scope-generator-tree/scope-generator-tree.component';

@Component({
  selector: 'app-process-scope',
  templateUrl: './process-scope.component.html',
  styleUrls: ['./process-scope.component.scss']
})
export class ProcessScopeComponent implements OnInit {

  @Input() processScope: any;
  @Input() savedProcessScope: any;
  @Output()
  next: EventEmitter<any> = new EventEmitter<any>();

  @Output()
  prev: EventEmitter<any> = new EventEmitter<any>();

  formData: any;

  postData: any[] = [];

  stop = 'processScope';

  @ViewChild('treeForm', { static: false })
  currentFormElement: ScopeGeneratorTreeComponent;

  constructor() { }

  showError: boolean = false;

  ngOnInit() {
  }

  isValid() {
    return this.currentFormElement.checklistSelection.selected.length == 0 ? false : true;
  }

  onNext() {
    this.showError = true;
    if (this.isValid()) {
      this.showError = false;
      let savedProcessScopes = [];
      if (this.currentFormElement) {
        for (let element of this.processScope.businessProcess) {
          if (this.currentFormElement.checklistSelection.selected.findIndex(t => t.id == element.id) >= 0) {
            let index = savedProcessScopes.push({
              id: element.id,
              item: element.item,
              children: []
            });

            for (let childLevel1 of element.children) {
              if (this.currentFormElement.checklistSelection.selected.findIndex(t => t.id == childLevel1.id) >= 0) {
                let index2 = savedProcessScopes[index - 1].children.push({
                  id: childLevel1.id,
                  item: childLevel1.item,
                  children: []
                });

                for (let childLevel2 of childLevel1.children) {
                  if (this.currentFormElement.checklistSelection.selected.findIndex(t => t.id == childLevel2.id) >= 0) {
                    savedProcessScopes[index - 1].children[index2 - 1].children.push(
                      {
                        id: childLevel2.id,
                        item: childLevel2.item,
                      }
                    )
                  }
                }

              }
            }

          }
        }
      }
      this.formData = [];
      this.formData = { businessProcess: savedProcessScopes }

      this.postData.push({ projectType: [], geographicalScope: [], processScope: [this.formData], implementationApproach: [], phasePlanning: [] })
      this.next.emit(this.postData);
    }

  }

  onPrev() {
    this.prev.emit();
  }

  submit(data) {
    this.formData = data;
  }

}

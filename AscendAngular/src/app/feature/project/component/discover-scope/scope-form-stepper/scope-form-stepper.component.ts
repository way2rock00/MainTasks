import { Component, OnInit } from '@angular/core';
import { GeneratescopeService } from '../../../service/generatescope.service';
import { MatDialog } from '@angular/material';
import { CommonDialogueBoxComponent } from 'src/app/shared/components/common-dialogue-box/common-dialogue-box.component';
import { ScopeGeneratorFormModel } from '../../../model/project-scope-generator/scope-generator-form.model';

@Component({
  selector: 'app-scope-form-stepper',
  templateUrl: './scope-form-stepper.component.html',
  styleUrls: ['./scope-form-stepper.component.scss']
})
export class ScopeFormStepperComponent implements OnInit {

  phaseData = [
    {
      name: "Engagement details"
    },
    {
      name: "Geographical scope"
    },
    {
      name: "Process scope"
    },
    {
      name: "Implementation Approach"
    },
    {
      name: "Phase Planning"
    }
  ];
  currentPhase = this.phaseData[0].name;

  constructor(private generateScope: GeneratescopeService,
    public dialog: MatDialog) { }

  ngOnInit() { }

  throwParent(phase) {
    this.currentPhase = phase;
  }

  prev(element) {
    element.step--
  }
  next(element) {
    element.step++;
  }

  submit(form: any) {

    let formInfo = new ScopeGeneratorFormModel(form.formData);
    /*this.generateScope.updateProjectScopeData(formInfo, 'UPDATE', form.formData.projectid).subscribe(data => {
      let res : any = data;

      if (res.MSG == 'SUCCESS') {
        this.dialog.open(CommonDialogueBoxComponent, {
          data: {
            from: 'GENERATE SCOPE',
            message: 'Successfully updated the Project Details.'
          }
        });
      }
      else {
        this.dialog.open(CommonDialogueBoxComponent, {
          data: {
            from: '',
            message: 'Error while updating the data. Error Message: ' + res.MSG + '.'
          }
        });
      }
    });*/
  }
}

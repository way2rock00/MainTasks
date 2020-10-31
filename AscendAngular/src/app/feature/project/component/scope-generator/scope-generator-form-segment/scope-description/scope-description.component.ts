import { Component, EventEmitter, Input, OnInit, Output, ViewChild, ViewEncapsulation } from '@angular/core';
import { NgForm } from '@angular/forms';
import { Router } from '@angular/router';
import { ScopeGeneratorFormModel } from 'src/app/feature/project/model/project-scope-generator/scope-generator-form.model';
import { GeneratescopeService } from './../../../../service/generatescope.service';

@Component({
  selector: 'app-scope-description',
  templateUrl: './scope-description.component.html',
  styleUrls: ['./scope-description.component.scss'],
  encapsulation: ViewEncapsulation.None,
})
export class ScopeDescriptionForm implements OnInit {

  @Input()
  formData: ScopeGeneratorFormModel;

  @Input() formOptions: any;

  @Output()
  next: EventEmitter<any> = new EventEmitter<any>();

  @Output()
  prev: EventEmitter<any> = new EventEmitter<any>();

  @Input()
  isEditable: boolean;

  @ViewChild('scopeForm', { static: false })
  ngForm: NgForm;

  isSubmitted: boolean = false;

  showError: boolean = false;

  constructor(private router: Router) { }

  complexityValues: string[] = ['Very High', 'High', 'Medium', 'Simple', 'Very Simple'];
  scopeListValues: string[] = ['Primary', 'Secondary', 'Not in scope'];
  
  serviceScope: any = [
    {
      "name": "Prepare - Value and Business Needs",
      "deloitteScope": "Primary",
      "clientScope": "Secondary",
      "complexity": "Simple"
    },
    {
      "name": "Project Management",
      "deloitteScope": "Secondary",
      "clientScope": "Primary",
      "complexity": "Very High"
    },
    {
      "name": "Organization Change Management",
      "deloitteScope": "Secondary",
      "clientScope": "Secondary",
      "complexity": "Very Simple"
    },
    {
      "name": "Design and Development",
      "deloitteScope": "Primary",
      "clientScope": "Primary",
      "complexity": "Very High"
    }
  ];
  

  ngOnInit() {
    for (let i in this.formOptions.additionalErpPackage) {
      if (this.formOptions.additionalErpPackage[i] == this.formData.erpPackage) {
        this.formOptions.additionalErpPackage.splice(i, 1);
      }
    }
  }

  onNext() {
    console.log('Scope tab:nextClicked');
    event.preventDefault();
    this.showError = true;
    if (this.isValid()) {
      this.isSubmitted = true;
      this.showError = false;
      this.next.emit(this.formData);
    }
  }

  navigate(route) {
    this.router.navigate([route]);
  }

  onPrev() {
    console.log('Scope tab:prevClicked');
    this.prev.emit();
  }

  isValid() {
    return (this.ngForm.valid)
  }

  erpChecked(event, value) {
    // console.log(event);
    if (event.checked) {
      this.formData.erpPackage.push(value);
    } else {
      this.formData.erpPackage = this.formData.erpPackage.filter(pack => pack !== value)
    }
  }

}

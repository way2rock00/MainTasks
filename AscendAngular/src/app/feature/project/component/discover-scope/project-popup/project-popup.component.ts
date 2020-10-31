import { Component, OnInit, Inject } from '@angular/core';
import { MatDialogRef, MAT_DIALOG_DATA } from '@angular/material';
import { UserProjectInfo } from 'src/app/shared/constants/ascend-project-info-type';

@Component({
  selector: 'app-project-popup',
  templateUrl: './project-popup.component.html',
  styleUrls: ['./project-popup.component.scss']
})
export class ProjectPopupComponent implements OnInit {

  data: any;
  constructor(public dialogRef: MatDialogRef<ProjectPopupComponent>,
    @Inject(MAT_DIALOG_DATA) public projectInfo: UserProjectInfo) { }

  ngOnInit() { }

  closePopup(){
    this.dialogRef.close();
  }
}

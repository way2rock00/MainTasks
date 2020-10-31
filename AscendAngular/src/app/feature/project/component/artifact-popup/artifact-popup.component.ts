import { Component, OnInit, Inject } from '@angular/core';
import { MatDialogRef, MAT_DIALOG_DATA } from '@angular/material';
import { Router } from '@angular/router';
import { environment } from 'src/environments/environment';

export interface ArtifactPopupData {
  contentId: string;
  contentName: string;
  contentType: string;
  phaseId: string;
  phase: string;
  stopId: string;
  stop: string;
  tabID: string;
  tabCode: string;
  tabArtifactCount: string;
  contentIcon: string;
}

@Component({
  selector: 'app-artifact-popup',
  templateUrl: './artifact-popup.component.html',
  styleUrls: ['./artifact-popup.component.scss']
})
export class ArtifactPopupComponent implements OnInit {
  entityList: ArtifactPopupData[] = [];
  tabCodeValue: string;

  constructor(public dialogRef: MatDialogRef<ArtifactPopupComponent>,
    @Inject(MAT_DIALOG_DATA) public data: any, private router: Router) { }

  ngOnInit() {
    this.entityList = this.data.contentList;
    this.tabCodeValue = this.entityList[0].tabCode.toLocaleLowerCase().replace('_', ' ');
    console.log(this.tabCodeValue);

  }

  goto(value) {
    if (value == 'EXCEL') {
      window.open(`${environment.BASE_URL}/projectNextGenExtract/${this.router.url.split('/')[3]}`)
    } else {
      window.open(`${environment.BASE_URL}/projectScopeDoc/${this.router.url.split('/')[3]}`)
    }
  }

  navigateToStop(artifactObj) {
    // this.router.navigate([""]);
    let tabURL = "activities/summary/" + artifactObj.phase + "/" + artifactObj.stop + "/" + artifactObj.contentId + "/" + artifactObj.tabCode
    // this.router.navigate([tabURL]);
    this.dialogRef.close(tabURL);
  }

  closePopup() {
    this.dialogRef.close();
  }
}

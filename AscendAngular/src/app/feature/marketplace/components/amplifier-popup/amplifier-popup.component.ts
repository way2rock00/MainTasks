import { Component, Inject, OnInit } from '@angular/core';
import { MatDialogRef, MAT_DIALOG_DATA } from '@angular/material';
import { environment } from 'src/environments/environment';
import { SharedService } from '../../../../shared/services/shared.service';
import { AmplifierPopupData } from './../../models/amplifier-popup.model';
import { ProjectGlobalInfoModel } from 'src/app/shared/model/project-global-info.model';
import { PassGlobalInfoService } from 'src/app/shared/services/pass-project-global-info.service';

@Component({
  selector: 'app-tools-bar-popup',
  templateUrl: './amplifier-popup.component.html',
  styleUrls: ['./amplifier-popup.component.scss']
})
export class AmplifierPopupComponent implements OnInit {

  toolsbarPopupData: any;
  toolsbarPopupDataURL: any = `${environment.BASE_URL}/toolinfo/`;
  projectGlobalInfo: ProjectGlobalInfoModel;

  constructor(private sharedService: SharedService,
    public dialogRef: MatDialogRef<AmplifierPopupComponent>,
    @Inject(MAT_DIALOG_DATA) public data: AmplifierPopupData,
    private globalData: PassGlobalInfoService) { }

  ngOnInit() {

    this.globalData.share.subscribe(x => this.projectGlobalInfo = x);

    this.sharedService.getData(this.toolsbarPopupDataURL + this.data.toolName).subscribe(data => {
      this.toolsbarPopupData = data;
    });
  }

  launchTool( URL ) {
    window.open(URL);
  }

  closePopup() {
    this.dialogRef.close();
  }

}

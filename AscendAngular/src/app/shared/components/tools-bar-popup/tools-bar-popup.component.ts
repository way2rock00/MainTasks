import { Component, Inject, OnInit } from '@angular/core';
import { MatDialogRef, MAT_DIALOG_DATA } from '@angular/material';
import { environment } from 'src/environments/environment';
import { SharedService } from '../../services/shared.service';
import { ToolsbarPopupData } from './../tools-bar/tools-bar.component';
import { ProjectGlobalInfoModel } from '../../model/project-global-info.model';
import { PassGlobalInfoService } from '../../services/pass-project-global-info.service';

@Component({
  selector: 'app-tools-bar-popup',
  templateUrl: './tools-bar-popup.component.html',
  styleUrls: ['./tools-bar-popup.component.scss']
})
export class ToolsBarPopupComponent implements OnInit {

  toolsbarPopupData: any;
  toolsbarPopupDataURL: any = `${environment.BASE_URL}/toolinfo/`;
  projectGlobalInfo: ProjectGlobalInfoModel;

  constructor(private sharedService: SharedService,
    public dialogRef: MatDialogRef<ToolsBarPopupComponent>,
    @Inject(MAT_DIALOG_DATA) public data: ToolsbarPopupData,
    private globalData: PassGlobalInfoService) { }

  ngOnInit() {

    this.globalData.share.subscribe(x => this.projectGlobalInfo = x);

    this.sharedService.getData(this.toolsbarPopupDataURL + this.data.toolName).subscribe(data => {
      this.toolsbarPopupData = data;
    });
  }

  launchTool(URL) {
    window.open(URL);
  }

  closePopup() {
    this.dialogRef.close();
  }
}

import { Component, Inject, OnInit } from '@angular/core';
import { MatDialogRef, MAT_DIALOG_DATA } from '@angular/material';
import { environment } from 'src/environments/environment';
import { SharedService } from '../../../../shared/services/shared.service';
import { MarketplacesolutionsPopupData } from './../../models/Marketplacesolutions-popup.model';
import { ProjectGlobalInfoModel } from 'src/app/shared/model/project-global-info.model';
import { PassGlobalInfoService } from 'src/app/shared/services/pass-project-global-info.service';
@Component({
  selector: 'app-marketplacesolutions-popup',
  templateUrl: './marketplacesolutions-popup.component.html',
  styleUrls: ['./marketplacesolutions-popup.component.scss']
})
export class MarketplacesolutionsPopupComponent implements OnInit {
  toolsbarPopupData: any;
  toolsbarPopupDataURL: any = `${environment.BASE_URL}/solutioninfo/`;
  projectGlobalInfo: ProjectGlobalInfoModel;
  constructor(private sharedService: SharedService,
    public dialogRef: MatDialogRef<MarketplacesolutionsPopupComponent>,
    @Inject(MAT_DIALOG_DATA) public data: MarketplacesolutionsPopupData,
    private globalData: PassGlobalInfoService) { }

  ngOnInit() {
    this.globalData.share.subscribe(x => this.projectGlobalInfo = x);

    this.sharedService.getData(this.toolsbarPopupDataURL + this.data.toolID).subscribe(data => {
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

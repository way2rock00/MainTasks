import { Component, OnInit, Inject } from '@angular/core';
import { environment } from 'src/environments/environment';
import { PassGlobalInfoService } from '../../services/pass-project-global-info.service';
import { SharedService } from '../../services/shared.service';
import { MatDialogRef, MAT_DIALOG_DATA } from '@angular/material';
import { PopupData } from '../standard-view-layout/layout-right/layout-right.component';

@Component({
  selector: 'app-info-popup',
  templateUrl: './info-popup.component.html',
  styleUrls: ['./info-popup.component.scss']
})
export class InfoPopupComponent implements OnInit {

  popupData: any;
  headbandColor: any;
  private popupDataURL: string = `${environment.BASE_URL}/phasestopinfo/`

  constructor(private sharedService: SharedService,
    public dialogRef: MatDialogRef<InfoPopupComponent>,
    @Inject(MAT_DIALOG_DATA) public data: PopupData
  ) { }

  ngOnInit() {

    this.headbandColor = this.data.headbandColor;

    this.sharedService.getData(this.popupDataURL + this.data.phaseName + '/' + this.data.stopName).subscribe(data => {
      this.popupData = data;
    });

  }

  close() {
    this.dialogRef.close();
  }

}

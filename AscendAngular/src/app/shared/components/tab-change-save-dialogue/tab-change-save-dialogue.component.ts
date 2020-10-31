import { CryptUtilService } from './../../services/crypt-util.service';
import { SharedService } from './../../services/shared.service';
import { Component, OnInit, Inject } from '@angular/core';
import { MAT_DIALOG_DATA, MatSnackBar, MatDialogRef } from '@angular/material';
import { TabChangeDialogData, TAB_SAVE_STATUS, TAB_SAVE_CONST } from '../../constants/tab-change-save-dialog';
import { TabChangeSaveDialogueService } from '../../services/tab-change-save-dialogue.service';
import { ProjectGlobalInfoModel } from '../../model/project-global-info.model';
import { PassGlobalInfoService } from '../../services/pass-project-global-info.service';

@Component({
  selector: 'app-tab-change-save-dialogue',
  templateUrl: './tab-change-save-dialogue.component.html',
  styleUrls: ['./tab-change-save-dialogue.component.scss']
})
export class TabChangeSaveDialogueComponent implements OnInit {

  tabContentsTemp: any[] = [];
  projectGlobalInfo: ProjectGlobalInfoModel;
  projectId: any;

  constructor(
    private tabChangeSaveDialogueService: TabChangeSaveDialogueService,
    private _snackBar: MatSnackBar,
    public dialogRef: MatDialogRef<TabChangeSaveDialogueComponent>,
    private sharedService: SharedService,
    @Inject(MAT_DIALOG_DATA) public data: TabChangeDialogData,
    private globalData: PassGlobalInfoService,
    private cryptUtilService: CryptUtilService
  ) { }

  ngOnInit() {

    this.dialogRef.disableClose = true;

    this.tabContentsTemp = this.cryptUtilService.getItem(this.data.sessionStorageLabel, 'SESSION');

    this.globalData.share.subscribe(data => {
      this.projectId = data.projectId;
    });

  }

  close() {
    this.emit();
    this.sharedService.docAddEvent.emit('RESET');
    this.dialogRef.close({ status: TAB_SAVE_STATUS.FAILED, tabContents: this.tabContentsTemp });
  }

  emit() {
    let body = {
      source: this.data.eventData.source,
      data: this.data.eventData.data,
      type: 2
    }

    // if (this.data.eventData.source == 'MENU') {
    //   body = {
    //     source: 'menuChangeEvent',
    //     data: this.data.eventData.data,
    //     type: 2
    //   }
    // } else if (this.data.eventData.source == 'ACTIVITY_FILTER') {
    //   body = {
    //     source: 'filterChangeEvent',
    //     data: this.data.eventData.data,
    //     type: 2
    //   }
    // } else if (this.data.eventData.source == 'ACTIVITY_BACK') {
    //   body = {
    //     source: 'activitiesBackEvent',
    //     data: null,
    //     type: 2
    //   }
    // } else if (this.data.eventData.source == 'TAB_CHANGE') {
    //   body = {
    //     source: 'tabChangeEvent',
    //     data: this.data.eventData.data,
    //     type: 2
    //   }
    // }
    // else if (this.data.eventData.source == 'NAVIGATION_BAR') {
    //   body = {
    //     source: 'NAVIGATION_BAR',
    //     data: this.data.eventData.data,
    //     type: 2
    //   }
    // }
    // else if (this.data.eventData.source == 'MEGA_MENU') {
    //   body = {
    //     source: 'MEGA_MENU',
    //     data: this.data.eventData.data,
    //     type: 2
    //   }
    // }
    this.sharedService.dataChangeEvent.emit(body)
  }

  save() {
    this.tabChangeSaveDialogueService
      .updateTabContents(this.data.tabContents, this.data.URL)
      .subscribe({
        next: res => {
          if (res.MSG == TAB_SAVE_STATUS.SUCCESS) {
            this.emit();
            this.sharedService.docAddEvent.emit('UPDATE');
            this.cryptUtilService.setItem(this.data.sessionStorageLabel, this.data.tabContents, 'SESSION');
            this.dialogRef.close({ status: TAB_SAVE_STATUS.SUCCESS });
            this._snackBar.open(
              "Successfully saved the " + this.data.tabName + " Tab Details",
              null,
              {
                duration: 3000
              }
            );
          } else {
            this.sharedService.docAddEvent.emit('FAILED');
            this.dialogRef.close({ status: TAB_SAVE_STATUS.FAILED, tabContents: this.tabContentsTemp });
            this._snackBar.open(
              "Failed to save the " + this.data.tabName + " Tab Details, please re-try after sometime",
              null,
              {
                duration: 3000
              }
            );
          }
        },
        error: error => {
          this.sharedService.docAddEvent.emit('FAILED');
          this.dialogRef.close({ status: TAB_SAVE_STATUS.FAILED, tabContents: this.tabContentsTemp });
          this._snackBar.open(
            "Failed to save the " + this.data.tabName + " Tab Details, please re-try after sometime; " + error.status + ": " + error.statusText,
            null,
            {
              duration: 5000
            }
          )
        }
      });
  }

}

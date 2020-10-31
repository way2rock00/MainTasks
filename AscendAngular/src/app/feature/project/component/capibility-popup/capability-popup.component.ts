import { Component, OnInit, Inject } from '@angular/core';
import { MatDialogRef, MAT_DIALOG_DATA } from '@angular/material';

export class CapabilityPopupData {
    imgURL: string;
}

@Component({
    // selector: 'app-project-summary',
    template: `<div class="multicolor-band">
                    <img class="rectangle" src="../../assets/bottom_color.png" />
                </div>
                <div class="close">
                    <img (click)="closePopup()" src="../../../../assets/cross-neg@3x.png" width="35px" />
                </div>
                <img [src]="imgURL">`,
    styleUrls : ['../../../../shared/components/tools-bar-popup/tools-bar-popup.component.scss'],
    styles: ['::ng-deep .toolsbarPopupStyle .mat-dialog-container{background-image: none !important; }']
})
export class CapabilityComponent implements OnInit {

    imgURL: string

    constructor(public dialogRef: MatDialogRef<CapabilityComponent>,
        @Inject(MAT_DIALOG_DATA) public data: CapabilityPopupData) { }

    ngOnInit() {
        this.imgURL = this.data.imgURL;
    }

    closePopup(){
        this.dialogRef.close();
    }
}
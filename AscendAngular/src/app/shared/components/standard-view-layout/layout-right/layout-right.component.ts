import { InfoPopupComponent } from './../../info-popup/info-popup.component';
import { Component, Input, EventEmitter, Output } from '@angular/core';
import { MatDialog } from '@angular/material';

export interface PopupData {
  stopName: string;
  phaseName: string;
  layoutSubCat: string;
  headbandColor: string;
}

@Component({
  selector: 'app-right-layout',
  templateUrl: './layout-right.component.html',
  styleUrls: ['./layout-right.component.scss']
})
export class LayoutRightComponent {

  @Input()
  title: string;

  @Input()
  shrink: boolean;

  @Input()
  layout: string;

  @Input()
  layoutSubCat: string;

  headbandColor: string;

  constructor(public dialog: MatDialog) { }

  infoPopupHandler() {
    switch (this.layout) {
      case 'imagine':
        this.headbandColor = 'rgb(2, 151, 169)';
        break;
      case 'insights':
        this.headbandColor = 'rgb(2, 86, 137)';
        break;
      case 'deliver':
        this.headbandColor = 'rgb(198, 215, 12)';
        break;
      case 'run':
        this.headbandColor = 'rgb(134, 188, 37)';
        break;
    }
    this.dialog.open(InfoPopupComponent, {
      data: {
        stopName: this.title,
        phaseName: this.layout,
        layoutSubCat: this.layoutSubCat,
        headbandColor: this.headbandColor
      },
      height: '650px',
      width: '1000px',
      panelClass: 'infoPopupStyle'
    });
  }

}

import { SharedService } from 'src/app/shared/services/shared.service';
import { Component, Input, OnInit } from '@angular/core';
import { Router } from '@angular/router';
import { LAYOUT_TYPE } from 'src/app/shared/constants/layout-constants';
import { MatDialog } from '@angular/material';
import { CommonDialogueBoxComponent } from 'src/app/shared/components/common-dialogue-box/common-dialogue-box.component';

@Component({
  selector: 'app-imagine',
  templateUrl: './imagine.component.html',
  styleUrls: ['./imagine.component.scss']
})
export class ImagineComponent implements OnInit {

  LAYOUT: LAYOUT_TYPE = LAYOUT_TYPE.IMAGINE;

  constructor(private router: Router, private sharedService: SharedService, private dialog: MatDialog) { }

  ngOnInit() {
  }

  goto(stop) {
    if (this.sharedService.filterSelected) {
      this.router.navigate(['/activities/iidr/' + this.LAYOUT + '/' + stop]);
    }
    else {
      this.dialog.open(CommonDialogueBoxComponent, {
        data: {
          from: '',
          message: 'Please select a package and one or more functions from the filter to proceed.'
        }
      });
    }
  }
}


import { Component, OnInit, Inject } from '@angular/core';
import { MatDialogRef, MAT_DIALOG_DATA } from '@angular/material';
import { SharedService } from '../../services/shared.service';
import { CommonDialogueBoxData } from '../../constants/common-dialogue-box';
import { Location } from '@angular/common';
import { Router } from '@angular/router';

@Component({
  selector: 'app-common-dialogue-box',
  templateUrl: './common-dialogue-box.component.html',
  styleUrls: ['./common-dialogue-box.component.scss']
})
export class CommonDialogueBoxComponent implements OnInit {

  constructor(
    public dialogRef: MatDialogRef<CommonDialogueBoxComponent>,
    private sharedService: SharedService,
    private router: Router,
    @Inject(MAT_DIALOG_DATA) public data: CommonDialogueBoxData,
    private location: Location
  ) { }

  ngOnInit() {

    this.dialogRef.disableClose = true;

  }

  close() {
    this.dialogRef.close();

    if (this.data.from == 'CREATE PROJECT') {
      this.location.back();
    } 
    else if (this.data.from == 'GENERATE SCOPE') {
      this.router.navigate(['/project/list']);
    }
  }
}

import { Component, OnInit } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { HttpErrorResponse } from '@angular/common/http';
import { Router } from '@angular/router';
import { environment } from 'src/environments/environment';
import { ProjectGlobalInfoModel } from 'src/app/shared/model/project-global-info.model';
import { PassGlobalInfoService } from 'src/app/shared/services/pass-project-global-info.service';
import { SharedService } from 'src/app/shared/services/shared.service';
import { MatDialog } from '@angular/material';
import { CommonDialogueBoxComponent } from 'src/app/shared/components/common-dialogue-box/common-dialogue-box.component';
import { MarketplaceService } from 'src/app/feature/marketplace/services/marketplace.service';//Momentum filter change
import { filterConstruct } from 'src/app/feature/marketplace/models/marketplace-filter-helper';//Momentum filter change
import { CapabilityComponent } from '../capibility-popup/capability-popup.component';
import { ArtifactPopupComponent } from '../artifact-popup/artifact-popup.component';
import { DomSanitizer } from '@angular/platform-browser';
import { CryptUtilService } from 'src/app/shared/services/crypt-util.service';
import { BUS_MESSAGE_KEY } from 'src/app/shared/constants/message-bus';
import { PROJECT_SUMMARY_MOMENTUM_CONST } from '../../constants/project-summary-momentum';

@Component({
  selector: 'app-project-summary',
  templateUrl: './project-summary.component.html',
  styleUrls: ['./project-summary.component.scss']
})
export class ProjectSummaryComponent implements OnInit {

  projectGlobalInfo: ProjectGlobalInfoModel;
  summaryScreen: any;
  selected: string = '';
  coreArtifactsData: any[] = [];
  changeManagementData: any[] = [];
  /* Momentum filter change */
  constructedFilter = { "childs": [] };
  checkedId: any[] = [];
  comingSoon = "Coming soon !!!"

  capabilityEdgeData = [];
  filteredCapabilityEdgeData: any[] = [];
  /* Momentum filter change */

  //Function based change
  selectedFunction: string;
  entityList: any[];

  constructor
    (
      private httpClient: HttpClient,
      private router: Router,
      private data: PassGlobalInfoService,
      private sharedService: SharedService,
      public dialog: MatDialog,
      private sanitizer: DomSanitizer,
      private cryptoUtilService: CryptUtilService,
      private marketplaceService: MarketplaceService // Momentum filter change
    ) {
    this.data.share.subscribe(x => this.projectGlobalInfo = x);
  }

  ngOnInit() {

    var url = '${environment.BASE_URL}/project/summary/' + this.projectGlobalInfo.projectId;

    let packageFunction = this.cryptoUtilService.getItem(BUS_MESSAGE_KEY.IIDR_FILTER + "_functionpackage_" + this.projectGlobalInfo.projectId, 'SESSION');
    let functionURL = packageFunction ? packageFunction.split('/')[2] : '0';

    this.sharedService.getData(`${environment.BASE_URL}/project/summary/${this.projectGlobalInfo.projectId}/${functionURL}`).subscribe(data => {
      if (data) {
        // FILL THE ARRAY WITH DATA.
        this.summaryScreen = data[0];
        this.functionChange(this.summaryScreen.artifacts[0].functionName, this.summaryScreen.artifacts[0].entityList);
      }

      this.sharedService.summaryFilterEvent.subscribe(data => {
        if (data == 'momentum') {
          this.switch(data);
        }
      });

      let momentumObj = PROJECT_SUMMARY_MOMENTUM_CONST.find( t => t.projectName.toLowerCase() == this.projectGlobalInfo.projectName.toLowerCase())
      this.capabilityEdgeData = momentumObj ? momentumObj.momentum : [];

      // var temp = this.summaryScreen.artifacts;
      // temp.sort((a, b) => a.artifactType.localeCompare(b.artifactType));
    });
    //Momentum filter change
    this.marketplaceService.getFilters().subscribe(data => {
      this.constructedFilter = filterConstruct(data);
    });
  }

  /* Momentum filter change */
  filterChangedEvent(event, filterId) {

    if (event.checked) {
      this.checkedId.push(filterId);
      this.selected = 'capabilityEdge'
    }
    else {
      this.checkedId.splice(this.checkedId.indexOf(filterId), 1);
      // if (this.checkedId.length == 0)
      //   this.selected = 'core'
    }

    this.filterValues();

  }

  filterValues() {
    this.filteredCapabilityEdgeData = [];
    for (let x of this.capabilityEdgeData) {
      if (x.filtersApplicable.find(value => this.checkedId.includes(value)))
        this.filteredCapabilityEdgeData.push(x);
    }
  }

  filterChecked(filterId) {
    return !!(this.checkedId.find(t => t == filterId));
  }

  getSafeURL(logoURL) {
    return this.sanitizer.bypassSecurityTrustResourceUrl(logoURL);
  }

  openDialog(imgURL) {
    if (imgURL)
      window.open(imgURL);

    // this.dialog.open(CapabilityComponent, {
    //   data: {
    //     imgURL: '../../../../../assets/' + imgURL
    //   },
    //   height: '650px',
    //   width: '1100px',
    //   panelClass: 'toolsbarPopupStyle',
    //   autoFocus: false
    // });
  }
  /* Momentum filter change */

  navigate(url) {
    this.router.navigate([url.toLowerCase()]);
  }

  switch(value) {
    if (value == 'core') {
      this.selected = 'core';
      this.checkedId = [];
      this.sharedService.summaryFilterEvent.emit('');
    } else if (value == 'ocm') {
      this.selected = 'ocm';
      this.checkedId = [];
      this.sharedService.summaryFilterEvent.emit('');
    } else if ('momentum') {
      this.selected = 'capabilityEdge';
      this.selectedFunction = '';
      this.filterValues();
    }

    //Momentum filter change
    // this.checkedId = [];
  }

  publish() {
    var formData = new FormData();
    var retVal = "1";
    formData.append("projectId", `${this.projectGlobalInfo.projectId}`);
    // console.log("In");
    this.httpClient.post(`${environment.BASE_URL}/publish/email/${this.projectGlobalInfo.projectId}`, formData).subscribe(
      data => {
        retVal = data as any;	 // FILL THE ARRAY WITH DATA.
        // console.log(retVal['MSG']);
        // alert(retVal['MSG']);
        this.dialog.open(CommonDialogueBoxComponent, {
          data: {
            from: 'PROJECT SUMMARY',
            message: retVal['MSG']
          }
        });
        /*if (retVal['MSG'] == 'SUCCESS')
          alert("Published succesfully");
        else
          alert("There was an error publishing. Please re-try");*/
      },
      (err: HttpErrorResponse) => {
        // console.log(err.message);
        // alert("There was an error publishing. Please re-try");
        this.dialog.open(CommonDialogueBoxComponent, {
          data: {
            from: 'PROJECT SUMMARY',
            message: 'There was an error publishing. Please re-try.'
          }
        });
      }
    );
  }

  //Function based change
  functionChange(functionName, entityList) {
    this.selectedFunction = functionName;
    this.entityList = entityList;
    this.selected = 'core';
    this.checkedId = [];
    this.sharedService.summaryFilterEvent.emit('');
  }
  //Function based change
  navigateToStop(entityObj) {
    if (entityObj.contentList.length > 1) {
      //open popup
      const ref = this.dialog.open(ArtifactPopupComponent, {
        data: {
          contentList: entityObj.contentList,
          selectedFunction: this.selectedFunction
        },
        height: '495px',
        width: '917px',
        panelClass: 'summaryPopupStyle',
        autoFocus: false
      });

      ref.afterClosed().subscribe(res => {
        if (res)
          this.router.navigate([res]);
      });
    }
    else {
      //navigate to stop
      let artifactObj = entityObj.contentList[0];
      let tabURL = "activities/summary/" + artifactObj.phase + "/" + artifactObj.stop + "/" + artifactObj.contentId + "/" + artifactObj.tabCode
      this.router.navigate([tabURL]);
    }
  }
  goHome(){
    this.router.navigate(['/home']);
  }
}

function getImgForIcon(): String {
  return "../../../assets/ps_dataconv.png";
}

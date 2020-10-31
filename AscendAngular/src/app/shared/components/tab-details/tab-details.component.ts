import { Component, OnInit, Input, OnDestroy, SimpleChange } from '@angular/core';
import { PassGlobalInfoService } from '../../services/pass-project-global-info.service';
import { ProjectGlobalInfoModel } from '../../model/project-global-info.model';
import { SharedService } from '../../services/shared.service';
import { MatDialog } from '@angular/material';
import { TabChangeSaveDialogueComponent } from '../tab-change-save-dialogue/tab-change-save-dialogue.component';
import { TAB_DETAILS_CONST } from '../../constants/tab-details-constant';

@Component({
  selector: 'app-tab-details',
  templateUrl: './tab-details.component.html',
  styleUrls: ['./tab-details.component.scss']
})
export class TabDetailsComponent implements OnInit, OnDestroy {

  @Input() getTabURL: string;
  @Input() postTabURL: string;
  @Input() tabName: string;
  @Input() tabCode: string;

  projectGlobalInfo: ProjectGlobalInfoModel;
  view: String;
  tabData: {
    tabName: string;
    tabContent: any[]
  }[] = [];
  contentLevel: number;
  // toggledEvent: string;
  changes: any;
  dataEventSubscription: any;
  comingSoon: boolean = false;
  
  constructor(private globalData: PassGlobalInfoService, private sharedService: SharedService, public dialog: MatDialog) { }

  ngOnInit() {

    this.globalData.share.subscribe(x => {
      this.projectGlobalInfo = x;
      this.view = this.projectGlobalInfo.viewMode;
    });

    this.dataEventSubscription = this.sharedService.dataChangeEvent.subscribe(data => {
      if (data.type == 1)
        this.postTabData(this.tabName, this.postTabURL, data)
    })

  }

  ngOnChanges(changes: { [propKey: string]: SimpleChange }) {

    //Handle tab change
    if (changes['tabName']) {
      this.changes = changes;
      this.tabName = changes['tabName'].currentValue;
      //let tabName = changes['tabName'].previousValue;
      // this.postTabData(tabName, changes['postTabURL'].previousValue);
      // this.contentLevel = 0; // clear content level from previous tab if any
      // this.tabData = [];
      // this.emitTabCount();
    }

    //Handle filter change
    if (changes['getTabURL']) {
      this.tabData = [];
      if (this.getTabURL) {
        this.sharedService.getData(this.getTabURL).subscribe(data => {
          this.tabData[0] = {
            tabName: this.tabName,
            tabContent: data
          };
          if (data) {
            this.comingSoon = false;
            this.contentLevel = this.tabData[0].tabContent[0].content_level;
            this.emitTabCount();
          }
          else{
            this.comingSoon = true
          }
        });
      }
    }
  }

  ngOnDestroy() {
    //this.postTabData(this.tabName, this.postTabURL);
    this.dataEventSubscription.unsubscribe();
  }

  attributePresent(level, attributeName) {
    return TAB_DETAILS_CONST.findIndex(t => t.level == level && t.tabCode == this.tabCode && t.node && t.node.includes(attributeName)) != -1;
  }

  postTabData(tabName, postAPI, data?) {
    if (tabName && this.sharedService.toggled == "TOGGLED") {
      const dialogref = this.dialog.open(TabChangeSaveDialogueComponent, {
        data: {
          tabName: this.tabName,
          sessionStorageLabel: this.tabName,
          tabContents: this.tabData,
          URL: `${postAPI}`,
          eventData: data
        }
      });
    }

    this.sharedService.toggled = "";
  }

  emitTabCount() {
    let totCount = 0;
    let selCount = 0;

    if (this.contentLevel == 1 && this.tabData && this.tabData[0].tabContent) {
      totCount = this.tabData[0].tabContent.filter(t => t.l1_doclink).length;
      selCount = this.tabData[0].tabContent.filter(t => t.l1_doclink && t.l1_enabledflag == 'Y').length;
    }
    else if (this.contentLevel > 1 && this.tabData[0].tabContent) {
      for (let level1 of this.tabData[0].tabContent) {

        if (this.contentLevel == 2 && level1.l2_group) {
          totCount += level1.l2_group.filter(t => t.l2_doclink).length;
          selCount += level1.l2_group.filter(t => t.l2_doclink && t.l2_enabledflag == 'Y').length;
        }

        else if (this.contentLevel > 2 && level1.l2_group) {
          for (let level2 of level1.l2_group) {
            if (this.contentLevel == 3 && level2.l3_group) {
              totCount += level2.l3_group.filter(t => t.l3_doclink).length;
              selCount += level2.l3_group.filter(t => t.l3_doclink && t.l3_enabledflag == 'Y').length;
            }
            else if (this.contentLevel > 3 && level2.l3_group) {
              for (let level3 of level2.l3_group) {
                if (this.contentLevel == 4 && level3.l4_group) {
                  totCount += level3.l4_group.filter(t => t.l4_doclink).length;
                  selCount += level3.l4_group.filter(t => t.l4_doclink && t.l4_enabledflag == 'Y').length;
                }
              }
            }
          }
        }
      }
    }
    let label = this.projectGlobalInfo.viewMode == 'PROJECT' ? selCount + '/' + totCount : totCount;
    this.sharedService.tabCountEvent.emit(' (' + label + ')');
  }

  isLastLevel(payload) {
    let line_type = true;
    for (let objKey in payload) {
      if (Array.isArray(payload[objKey])) {
        line_type = false;
        break;
      }
    }

    return line_type;
  }

  preview(doclink) {
    window.open(doclink);
    event.stopPropagation();
  }

  getLevelColor(enabledFlag) {
    let isAdded = (enabledFlag == 'N' && this.view == 'PROJECT')
    return { color: isAdded ? 'grey' : 'black', 'background-color': isAdded ? 'whitesmoke' : 'white' }
  }

  getToggleButtonLabel(flag, level, docLink) {
    let text = flag == 'Y' ? 'Remove' : 'Add';
    if (level < this.contentLevel)
      return text + " all"
    else if (level == this.contentLevel)
      return text
  }

  // Add/Remove click handler
  buttonClicked(topLevelObject, thatLevelObject, flag, refeshNeededFlag) {
    this.sharedService.toggled = "TOGGLED";
    flag = (flag == 'Y') ? 'N' : 'Y';
    this.toggle(thatLevelObject, flag);
    if (refeshNeededFlag == 'Y')
      this.refreshParent(topLevelObject);
    this.emitTabCount();
    event.stopPropagation();
  }

  //Update parent
  refreshParent(dataObj) {
    let enabledFlagAtLevel2 = false;
    let enabledFlagAtLevel3 = false;
    let enabledFlagAtLevel4 = false;

    let l2array = [];
    let l3array = [];
    let l4array = [];

    if (dataObj.l2_group) {
      // 'L2 Group Found'
      l2array = dataObj.l2_group;
      enabledFlagAtLevel2 = false;

      for (let i = 0; i < l2array.length; i++) {
        let level2Obj = l2array[i];
        if (level2Obj.l3_group) {
          // L3 Group Found
          l3array = level2Obj.l3_group;
          enabledFlagAtLevel3 = false;

          for (let j = 0; j < l3array.length; j++) {
            let level3Obj = l3array[j];
            if (level3Obj.l4_group) {
              //L4 group found
              l4array = level3Obj.l4_group;
              enabledFlagAtLevel4 = false;

              for (let k = 0; k < l4array.length; k++) {
                let level4Obj = l4array[k];
                enabledFlagAtLevel4 = false;

                if (level4Obj.l4_enabledflag == 'Y')
                  enabledFlagAtLevel4 = true;
              }

              if (enabledFlagAtLevel4 && level3Obj.l3_enabledflag != 'Y') {
                level3Obj.l3_enabledflag = 'Y'
                level3Obj.l3_changedflag = level3Obj.l3_changedflag == 'Y' ? 'N' : 'Y';
              }
              else if (!enabledFlagAtLevel4 && level3Obj.l3_enabledflag != 'N') {
                level3Obj.l3_enabledflag = 'N';
                level3Obj.l3_changedflag = level3Obj.l3_changedflag == 'Y' ? 'N' : 'Y';
              }

            }

            if (level3Obj.l3_enabledflag == 'Y')
              enabledFlagAtLevel3 = true;
          }

          if (enabledFlagAtLevel3 && level2Obj.l2_enabledflag != 'Y') {
            level2Obj.l2_enabledflag = 'Y' //Level 3 Checked
            level2Obj.l2_changedflag = level2Obj.l2_changedflag == 'Y' ? 'N' : 'Y';
          } else if (!enabledFlagAtLevel3 && level2Obj.l2_enabledflag != 'N') {
            level2Obj.l2_enabledflag = 'N' //Level 3 not Checked
            level2Obj.l2_changedflag = level2Obj.l2_changedflag == 'Y' ? 'N' : 'Y';
          }
        }
        if (level2Obj.l2_enabledflag == 'Y')
          enabledFlagAtLevel2 = true;
      }
      if (enabledFlagAtLevel2 && dataObj.l1_enabledflag != 'Y') {
        dataObj.l1_enabledflag = 'Y' //Level 2 Checked
        dataObj.l1_changedflag = dataObj.l1_changedflag == 'Y' ? 'N' : 'Y';
      } else if (!enabledFlagAtLevel2 && dataObj.l1_enabledflag != 'N') {
        dataObj.l1_enabledflag = 'N' //Level 2 not Checked
        dataObj.l1_changedflag = dataObj.l1_changedflag == 'Y' ? 'N' : 'Y';
      }
    }
  }

  //toggle enabled flag for current and child elements
  toggle(dataObj, flag) {

    if (dataObj.l1_enabledflag && dataObj.l1_enabledflag != flag) {
      dataObj.l1_enabledflag = flag;
      dataObj.l1_changedflag = dataObj.l1_changedflag == 'Y' ? 'N' : 'Y';
    }
    else if (dataObj.l2_enabledflag && dataObj.l2_enabledflag != flag) {
      dataObj.l2_enabledflag = flag;
      dataObj.l2_changedflag = dataObj.l2_changedflag == 'Y' ? 'N' : 'Y';
    }
    else if (dataObj.l3_enabledflag && dataObj.l3_enabledflag != flag) {
      dataObj.l3_enabledflag = flag;
      dataObj.l3_changedflag = dataObj.l3_changedflag == 'Y' ? 'N' : 'Y';
    }
    else if (dataObj.l4_enabledflag && dataObj.l4_enabledflag != flag) {
      dataObj.l4_enabledflag = flag;
      dataObj.l4_changedflag = dataObj.l4_changedflag == 'Y' ? 'N' : 'Y';
    }

    let array = [];
    let arrayFound = false;

    if (dataObj.l2_group) {
      arrayFound = true;
      array = dataObj.l2_group; //L2 Group Found
    }
    else if (dataObj.l3_group) {
      arrayFound = true;
      array = dataObj.l3_group; //L3 Group Found
    }
    else if (dataObj.l4_group) {
      arrayFound = true;
      array = dataObj.l4_group; //L4 Group Found
    }

    if (arrayFound) {
      for (let i = 0; i < array.length; i++) {
        this.toggle(array[i], flag);
      }
    }
  }
}
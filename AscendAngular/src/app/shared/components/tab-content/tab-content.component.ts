import { Component, OnInit, Input, SimpleChange } from '@angular/core';
import { MessagingService } from 'src/app/shared/services/messaging.service';
import { BUS_MESSAGE_KEY } from 'src/app/shared/constants/message-bus';
import { Subscription } from 'rxjs';
import { PassGlobalInfoService } from 'src/app/shared/services/pass-project-global-info.service';
import { UtilService } from 'src/app/shared/services/util.service';
import { SharedService } from 'src/app/shared/services/shared.service';
import { MatDialog } from '@angular/material';
import { TabChangeSaveDialogueComponent } from 'src/app/shared/components/tab-change-save-dialogue/tab-change-save-dialogue.component';

@Component({
  selector: 'app-tab-content',
  templateUrl: './tab-content.component.html',
  styleUrls: ['./tab-content.component.scss']
})
export class TabContentComponent implements OnInit {

  @Input() commonphaseName: any;//tab changes
  @Input() commonstopName: any;//tab changes
  @Input() commontab: any;//tab changes
  @Input() bgColor: string;

  filterSubscription: Subscription;
  projectGlobalInfo: any;
  view: any;
  tabData: any[] = [];
  advancedFilter: any[] = [];
  selectedAdvancedFilter: any[] = [];
  filters: any[] = []
  toggledEvent: string = "";

  elementLinkValue: any;

  constructor(private messagingService: MessagingService,
    private globalData: PassGlobalInfoService,
    private utilService: UtilService,
    private sharedService: SharedService,
    public dialog: MatDialog
  ) { }

  ngOnInit() {

    this.messagingService.publish(BUS_MESSAGE_KEY.FILTER_CHANGED, undefined);

    this.filterSubscription = this.messagingService
      .subscribe(BUS_MESSAGE_KEY.FILTER_CHANGED, data => { if (data) this.setFilters() });

    this.globalData.share.subscribe(x => {
      this.projectGlobalInfo = x;
      this.view = this.projectGlobalInfo.viewMode;
    });
  }

  ngOnChanges(changes: { [propKey: string]: SimpleChange }) {
    if (changes['commontab']) {
      let tab = changes['commontab'].previousValue;
      this.postTabData(tab);
      this.elementLinkValue = this.commontab.tabkeys[0];
    }

    this.tabData = [];
  }

  ngOnDestroy() {
    this.filterSubscription.unsubscribe();
    this.postTabData(this.commontab);
  }

  preview(doclink) {
    window.open(doclink);
    event.stopPropagation();
  }

  postTabData(tab) {
    if (tab && this.toggledEvent == "TOGGLED") {
      const dialogref = this.dialog.open(TabChangeSaveDialogueComponent, {
        data: {
          tabName: tab.tabName,
          sessionStorageLabel: tab.tabStorage,
          tabContents: this.tabData,
          URL: `${tab.serviceURL}`
        }
      });
    }

    this.toggledEvent = "";

  }

  emitTabCount() {
    let count;
    if (this.elementLinkValue.CONTENT_LEVEL == 1) {
      count = this.utilService.getL1TabCount('', this.tabData[0].tabContent, this.elementLinkValue.L1_ENABLED_FLAG, this.elementLinkValue.L1_DOC_LINK);
    }
    else if (this.elementLinkValue.CONTENT_LEVEL == 2) {
      count = this.utilService.getL2TabCount('', this.tabData[0].tabContent, this.elementLinkValue.L2_GROUP_NAME, this.elementLinkValue.L2_ENABLED_FLAG, this.elementLinkValue.L2_DOC_LINK)
    }
    else if (this.elementLinkValue.CONTENT_LEVEL == 3) {
      count = this.utilService.getL3TabCount('', this.tabData[0].tabContent, this.elementLinkValue.L3_GROUP_NAME, this.elementLinkValue.L3_ENABLED_FLAG, this.elementLinkValue.L2_GROUP_NAME, this.elementLinkValue.L3_DOC_LINK);
    }
    else if (!this.elementLinkValue.CONTENT_LEVEL){
      count = this.utilService.getTabLabel('', 0, 0);
    }

    this.sharedService.tabCountEvent.emit({ "tabName": this.commontab.tabName, "tabCount": count });
  }

  setFilters() {

    this.tabData = [];

    if (this.elementLinkValue.ADV_FILTER_APPLICABLE == 'Y')
      this.filters = [
        {
          filterValues: this.advancedFilter,
          selectedFilters: {
            L0: this.selectedAdvancedFilter,
            L1: []
          },
          type: this.elementLinkValue.ADV_FILTER_TYPE,
          title: { main: this.elementLinkValue.ADV_FILTER_TITLE, levels: [] },
          filterButtonTitle: this.elementLinkValue.ADV_FILTER_TITLE
        }
      ];


    this.utilService.getTabDataURL(this.commontab.serviceURL).subscribe(data => {
      this.tabData = this.utilService.formTabContents(data, this.commontab.tabName, this.commontab.tabStorage);
      if (this.elementLinkValue.ADV_FILTER_APPLICABLE == "Y") {
        if (this.elementLinkValue.ADV_FILTER_LEVEL == 1) {
          this.advancedFilter = this.utilService.formTechnologyFilter(this.tabData[0].tabContent, this.advancedFilter, this.elementLinkValue.ADV_FILTER_TAG);
          this.tabData[0].tabContent = this.utilService.technologyFilters(this.tabData[0].tabContent, this.selectedAdvancedFilter, this.elementLinkValue.ADV_FILTER_TAG);
        }
        else if (this.elementLinkValue.ADV_FILTER_LEVEL == 2) {
          this.advancedFilter = this.utilService.formadvancedFilter(this.tabData[0].tabContent, this.advancedFilter, this.elementLinkValue.L2_GROUP_NAME);
          this.tabData[0].tabContent = this.utilService.advancedFilters(this.tabData[0].tabContent, this.selectedAdvancedFilter, this.elementLinkValue.L2_GROUP_NAME);

        }
      }
      this.emitTabCount();
    });
  }

  eventEmit(e) {

    this.selectedAdvancedFilter = [];
    e.data.selectedfilterData.l0.map(p => {
      if (p.checked)
        this.selectedAdvancedFilter.push({ L0: p.L0 })
    });

    this.setFilters();
  }

  getLineType(payload) {
    let line_type = 'LINE';
    for (let objKey in payload) {
      if (Array.isArray(payload[objKey])) {
        line_type = 'ACCORDION'
      }
    }
    return line_type;
  }

  getStyle(flag){
    let isAdded = (flag =='N' && this.view == 'PROJECT')
    return { color: isAdded ? 'grey' : 'black', 'background-color': isAdded ? 'whitesmoke' : 'white' }
  }

  getLevelColor(dataObj){

    if(dataObj[this.elementLinkValue.L1_ENABLED_FLAG]){
      return this.getStyle(dataObj[this.elementLinkValue.L1_ENABLED_FLAG]) 
    }
    if(dataObj[this.elementLinkValue.L2_ENABLED_FLAG]){
      return this.getStyle(dataObj[this.elementLinkValue.L2_ENABLED_FLAG]) 
    }
    if(dataObj[this.elementLinkValue.L3_ENABLED_FLAG]){
      return this.getStyle(dataObj[this.elementLinkValue.L3_ENABLED_FLAG]) 
    }
  }

  // Add/Remove click handler
  buttonClicked(topLevelObject, thatLevelObject, flag, refeshNeededFlag) {
    this.toggledEvent = "TOGGLED";
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

    let l2array = [];
    let l3array = [];

    if (dataObj[this.elementLinkValue.L2_GROUP_NAME]) {
      // 'L2 Group Found'
      l2array = dataObj[this.elementLinkValue.L2_GROUP_NAME];
      enabledFlagAtLevel2 = false;

      for (let i = 0; i < l2array.length; i++) {
        let level2Obj = l2array[i];
        if (level2Obj[this.elementLinkValue.L3_GROUP_NAME]) {
          // L3 Group Found
          l3array = level2Obj[this.elementLinkValue.L3_GROUP_NAME];
          enabledFlagAtLevel3 = false;

          for (let j = 0; j < l3array.length; j++) {
            let level3Obj = l3array[j];    
            if (level3Obj[this.elementLinkValue.L3_ENABLED_FLAG] == 'Y')
              enabledFlagAtLevel3 = true;
          }

          if (enabledFlagAtLevel3) {          
            level2Obj[this.elementLinkValue.L2_ENABLED_FLAG] = 'Y' //Level 3 Checked
          } else {            
            level2Obj[this.elementLinkValue.L2_ENABLED_FLAG] = 'N' //Level 3 not Checked
          }
        }
        if (level2Obj[this.elementLinkValue.L2_ENABLED_FLAG] == 'Y')
          enabledFlagAtLevel2 = true;
      }
      if (enabledFlagAtLevel2) {
        dataObj[this.elementLinkValue.L1_ENABLED_FLAG] = 'Y' //Level 2 Checked
      } else {
        dataObj[this.elementLinkValue.L1_ENABLED_FLAG] = 'N' //Level 2 not Checked
      }
    }
  }

  //toggle enabled flag for current and child elements
  toggle(dataObj, flag) {

    if (dataObj[this.elementLinkValue.L1_ENABLED_FLAG]) {
      if (dataObj[this.elementLinkValue.L1_ENABLED_FLAG] != flag && this.elementLinkValue.CONTENT_LEVEL == 1)
        this.sharedService.docAddEvent.emit(flag)
      dataObj[this.elementLinkValue.L1_ENABLED_FLAG] = flag;
    }
    else if (dataObj[this.elementLinkValue.L2_ENABLED_FLAG]) {
      if (dataObj[this.elementLinkValue.L2_ENABLED_FLAG] != flag && this.elementLinkValue.CONTENT_LEVEL == 2)
        this.sharedService.docAddEvent.emit(flag)
      dataObj[this.elementLinkValue.L2_ENABLED_FLAG] = flag;
    }
    else if (dataObj[this.elementLinkValue.L3_ENABLED_FLAG]) {
      if (dataObj[this.elementLinkValue.L3_ENABLED_FLAG] != flag && this.elementLinkValue.CONTENT_LEVEL == 3)
        this.sharedService.docAddEvent.emit(flag)
      dataObj[this.elementLinkValue.L3_ENABLED_FLAG] = flag;
    }
    let array = [];
    let arrayFound = false;
    if (dataObj[this.elementLinkValue.L2_GROUP_NAME]) {
      arrayFound = true;
      array = dataObj[this.elementLinkValue.L2_GROUP_NAME]; //L2 Group Found
    } else
      if (dataObj[this.elementLinkValue.L3_GROUP_NAME]) {
        arrayFound = true;
        array = dataObj[this.elementLinkValue.L3_GROUP_NAME] //L3 Group Found
      }

    if (arrayFound) {
      for (let i = 0; i < array.length; i++) {
        this.toggle(array[i], flag);
      }
    }
  }
}
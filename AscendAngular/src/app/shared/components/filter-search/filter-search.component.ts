import { Subscription } from 'rxjs';
import { MessagingService } from 'src/app/shared/services/messaging.service';
import { CryptUtilService } from './../../services/crypt-util.service';
import { Component, OnInit, EventEmitter, Output, Input, Type, OnDestroy } from '@angular/core';
import { FilterOverlayRef } from '../filter-overlay/filter-overlay-ref';
import { PassGlobalInfoService } from "src/app/shared/services/pass-project-global-info.service";
import { ProjectGlobalInfoModel } from "src/app/shared/model/project-global-info.model";
import { AuthenticationService } from '../../services/authentication.service';
import { BUS_MESSAGE_KEY } from '../../constants/message-bus';

@Component({
  selector: 'app-filter-search',
  templateUrl: './filter-search.component.html',
  styleUrls: ['./filter-search.component.scss']
})
export class FilterSearchComponent implements OnInit, OnDestroy {

  searchText: string = "";
  commVar = "COMMON";
  selectAll = "Select all";
  isAscendAdmin: any = false;

  filterData: any[] = [];
  l1filterData: any[] = [];
  l2filterData: any[] = [];

  selectedL1filterData: any[] = [];
  selectedL2filterData: any[] = [];

  disabled: boolean;
  projectGlobalInfo: ProjectGlobalInfoModel;
  projectView: boolean;

  ssFilterData: any;
  ssL1FilterData: any;
  ssL2FilterData: any;

  ssL0Identifier: any = 'L0SelectedData' + this.filterOverlayRef.data.type;
  ssL1Identifier: any = 'L1SelectedData' + this.filterOverlayRef.data.type;
  ssL2Identifier: any = 'L2SelectedData' + this.filterOverlayRef.data.type;

  subscription: Subscription;

  constructor(private filterOverlayRef: FilterOverlayRef, private cryptUtilService: CryptUtilService, private globalData: PassGlobalInfoService,
    private userService: AuthenticationService, private messagingService: MessagingService) {
    this.filterData = filterOverlayRef.data.initSelectedItems;

    //below code is to add defualt seleted items to the selected Items list

    for (let i in this.filterOverlayRef.data.selectedfilterData.L0) {
      if (this.filterOverlayRef.data.selectedfilterData.L0[i].L0 != this.commVar && this.filterOverlayRef.data.selectedfilterData.L0[i].L0) {
        this.filterData.find((t) => t.L0 == this.filterOverlayRef.data.selectedfilterData.L0[i].L0).checked = true;
        this.refreshL1Data();
      }
    }

    for (let i in this.filterOverlayRef.data.selectedfilterData.L1) {

      if (this.filterOverlayRef.data.selectedfilterData.L1[i].L1 != this.commVar && this.filterOverlayRef.data.selectedfilterData.L1[i].L1) {
        this.selectedL1filterData.push(this.l1filterData.find((t) => t.L1 == this.filterOverlayRef.data.selectedfilterData.L1[i].L1));
        this.refreshL2Data();
      }

    }

    for (let i in this.filterOverlayRef.data.selectedfilterData.L2) {
      if (this.filterOverlayRef.data.selectedfilterData.L2[i].L2 != this.commVar && this.filterOverlayRef.data.selectedfilterData.L2[i].L2) {
        this.selectedL2filterData.push(this.l2filterData.find((t) => t.L2 == this.filterOverlayRef.data.selectedfilterData.L2[i].L2));
      }
    }
  }


  ngOnInit() {
    let self = this;

    this.subscription = this.messagingService
      .subscribe(BUS_MESSAGE_KEY.STOP_NAME, data => {
        this.ssL0Identifier += data;
        this.ssL1Identifier += data;
        this.ssL2Identifier += data;
        this.ssFilterData = this.cryptUtilService.getItem(this.ssL0Identifier, 'SESSION');
        this.ssL1FilterData = this.cryptUtilService.getItem(this.ssL1Identifier, 'SESSION');
        this.ssL2FilterData = this.cryptUtilService.getItem(this.ssL2Identifier, 'SESSION');
      })

    // if (this.ssFilterData) {
    //   this.filterData = this.ssFilterData;
    //   this.refreshL1Data();
    // }
    // if (this.ssL1FilterData) {
    //   this.selectedL1filterData = this.ssL1FilterData;
    //   this.refreshL2Data();
    // }
    // if (this.ssL2FilterData) {
    //   this.selectedL2filterData = this.ssL2FilterData
    // }

    //check if user is Ascend Admin
    this.isAscendAdmin = JSON.parse(this.userService.getUser().projectDetails.isAscendAdmin);

    this.globalData.share.subscribe(x => (this.projectGlobalInfo = x));
    this.projectView = (this.projectGlobalInfo.viewMode == "PROJECT" && this.filterOverlayRef.data.isGlobal);
    this.disabled = (this.projectView && this.projectGlobalInfo.role == "PROJECT_MEMBER" && (!this.isAscendAdmin));
  }

  close(e) {

    if (this.disabled) {
      this.filterOverlayRef.close({ closeType: "Cancel" });
    }
    else {
      this.cryptUtilService.setItem(this.ssL0Identifier, this.filterData, 'SESSION');
      this.cryptUtilService.setItem(this.ssL1Identifier, this.selectedL1filterData, 'SESSION');
      this.cryptUtilService.setItem(this.ssL2Identifier, this.selectedL2filterData, 'SESSION');
      this.filterOverlayRef.close({
        selectedfilterData: {
          l0: this.filterData,
          l1: this.selectedL1filterData,
          l2: this.selectedL2filterData
        },
        type: this.filterOverlayRef.data.type,
        closeType: "applied"
      })
    }
  }

  cancel(e) {
    this.resetFilter();
    this.filterOverlayRef.close({ closeType: "Cancel" });

  }

  l0ChangedEvent(e) {
    var self = this;

    console.log(e.source.name + this.filterOverlayRef.data.type)

    if (e.source.value != this.selectAll) {

      if (e.source.checked) {

        if (!this.filterData.find((t) => t.L0 == e.source.value).checked) {
          this.filterData.find((t) => t.L0 == e.source.value).checked = true;
        }
      }
      else {
        this.filterData.find((t) => t.L0 == e.source.value).checked = false;
      }
    }

    else {
      if (e.source.checked) {
        this.filterData.map(function (data) {
          if (!data.checked)
            data.checked = true;
        });
      }

      else {
        this.filterData.map(function (data) {
          return data.checked = false;
        });

      }
    }

    self.refreshL1Data();
    self.refreshL2Data();
  }

  refreshL1Data() {

    var self = this;
    this.l1filterData = [];

    self.filterData.map(function (p) {
      if (p.checked) {
        self.l1filterData = self.l1filterData.concat(p.L1Map);
      }
    });

    this.l1filterData = this.l1filterData.filter(function (e, index) {
      return ((e.L1 != self.commVar) && (self.l1filterData.indexOf(e) == index));
    });

    this.selectedL1filterData = this.selectedL1filterData.filter(function (l1data) {
      return self.l1filterData.find((t) => t.L1 == l1data.L1);
    });
  }

  l1ChangedEvent(e) {
    var self = this;
    console.log(e.source)
    if (e.source.value != this.selectAll) {
      if (e.source.checked) {
        if (this.selectedL1filterData.find((t) => t.L1 == e.source.value) == undefined) {
          this.selectedL1filterData.push(this.l1filterData.find((t) => t.L1 == e.source.value));
        }
      }
      else {
        // console.log("Unchecked Item, removing element");
        this.selectedL1filterData.splice(this.selectedL1filterData.indexOf(this.selectedL1filterData.find((t) => t.L1 == e.source.value)), 1)
      }

    }
    else {
      if (e.source.checked) {
        this.l1filterData.map(function (data) {
          if ((self.selectedL1filterData.find((t) => t.L1 == data.L1) == undefined) && (data.L1 != self.commVar)) {
            // console.log("Undefined Seleted Items List, Pushing element:");
            self.selectedL1filterData.push(data);
          }
        });
      }

      else {
        this.selectedL1filterData = [];
      }
    }

    self.refreshL2Data();
  }

  refreshL2Data() {
    // console.log(this.selectedL2filterData,'self.selectedL2filterData---->before');

    var self = this;
    self.l2filterData = [];

    this.selectedL1filterData.map(function (e) {
      self.l1filterData.map(function (p) {
        if ((e.L1 == p.L1) && e.L2Map) {
          self.l2filterData = self.l2filterData.concat(e.L2Map)
        }

      })
    });

    this.l2filterData = [...(new Set(this.l2filterData))];

    this.l2filterData = this.l2filterData.filter(function (e) {
      return (e != self.commVar);
    })

    this.selectedL2filterData = this.selectedL2filterData.filter(function (l1data) {
      console.log(self.l2filterData.find((t) => t == l1data) && (l1data != undefined))
      return (self.l2filterData.find((t) => t.L2 == l1data.L2) && (l1data != undefined));
    });
    // console.log(this.selectedL2filterData,"self.selectedL2filterData----->after");

  }

  l2ChangedEvent(e) {
    var self = this;
    console.log(e.source)
    if (e.source.value != this.selectAll) {
      if (e.source.checked) {
        if (this.selectedL2filterData.find((t) => t.L2 == e.source.value) == undefined) {
          this.selectedL2filterData.push(this.l2filterData.find((t) => t.L2 == e.source.value));
        }
      }
      else {
        // console.log("Unchecked Item, removing element");
        this.selectedL2filterData.splice(this.selectedL2filterData.indexOf(this.selectedL2filterData.find((t) => t.L2 == e.source.value)), 1)
      }
    }
    else {
      if (e.source.checked) {
        this.l2filterData.map(function (data) {
          if ((self.selectedL2filterData.find((t) => t.L2 == data.L2) == undefined) && data.L2 != self.commVar) {
            // console.log("Undefined Seleted Items List, Pushing element:");
            self.selectedL2filterData.push(data);
          }
        });

      }

      else {
        this.selectedL2filterData = [];
      }
    }

  }

  setL0Checked(currentID) {

    let counter = 0;
    let selectedCount = this.filterData.reduce((counter, { checked }) => checked ? counter + 1 : counter, 0)

    if (selectedCount == this.filterData.length)
      return true;
    else
      return false;
  }

  setL1Checked(currentID) {

    if (this.selectedL1filterData.length == this.l1filterData.length) {
      return true;
    }
    else if (this.selectedL1filterData.find((t) => t.L1 == currentID) == undefined) {
      return false;
    }
    else {
      return true;
    }
  }

  setL2Checked(currentID) {

    if (this.selectedL2filterData.length == this.l2filterData.length || this.disabled) {
      return true;
    }
    else if (this.selectedL2filterData.find((t) => t.L2 == currentID) == undefined) {
      return false;
    }
    else {
      return true;
    }
  }

  //Clear term types by user
  clearFilter() {
    this.searchText = "";
  }

  resetFilter() {
    if (!this.disabled) {
      this.filterData.map(function (data) {
        if (data.checked)
          data.checked = false;

      });

      this.refreshL1Data();
      this.refreshL2Data();

      this.selectedL1filterData = [];
      this.selectedL2filterData = [];
    }
  }

  onTextChange(e) {
    this.searchText = e.target.value;
  }

  ngOnDestroy() {
    this.subscription.unsubscribe();
  }

}

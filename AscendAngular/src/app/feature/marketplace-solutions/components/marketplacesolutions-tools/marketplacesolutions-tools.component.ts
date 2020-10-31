import { Component, OnInit, Input } from '@angular/core';
import { MarketplaceSolutionTools } from '../../models/marketplacesolutions-tools.model';
import { FormControl } from '@angular/forms';
import { Subscription } from 'rxjs';
import { MARKETPLACESOLUTIONS_DESCRIPTION } from '../../constants/marketplace-solutions-description';
import { MatDialog } from '@angular/material';
import { MarketplacesolutionsPopupComponent } from './../marketplacesolutions-popup/marketplacesolutions-popup.component';
import { ProjectGlobalInfoModel } from 'src/app/shared/model/project-global-info.model';
import { PassGlobalInfoService } from 'src/app/shared/services/pass-project-global-info.service';
@Component({
  selector: 'app-marketplacesolutions-tools',
  templateUrl: './marketplacesolutions-tools.component.html',
  styleUrls: ['./marketplacesolutions-tools.component.scss']
})
export class MarketplacesolutionsToolsComponent implements OnInit {
  @Input() tools: MarketplaceSolutionTools[];
  myControl = new FormControl('');
  filtererdTools: MarketplaceSolutionTools[];
  inputSubscription: Subscription;
  description: string = MARKETPLACESOLUTIONS_DESCRIPTION;
  projectGlobalInfo: ProjectGlobalInfoModel;
  constructor(public dialog: MatDialog, private data: PassGlobalInfoService) { }

  ngOnInit() {
     this.data.share.subscribe(x => this.projectGlobalInfo = x);
    //subscribe to input change
    this.inputSubscription = this.myControl.valueChanges.debounceTime(300).subscribe(data => {
      this._filter();
    })
  }
  ngOnChanges() {
    this._filter();
  }

  ngOnDestroy(){
    this.inputSubscription.unsubscribe();
  }

  openLink(link) {
    window.open(link);
    event.stopPropagation();
  }
   //filter tools based on input change
   _filter() {
    let value = this.myControl.value.toLowerCase();
    if (value) {
      this.filtererdTools = this.tools.filter(option => option.solutionName.toLowerCase().includes(value)).slice(0);
    }
    else {
      this.filtererdTools = this.tools.slice(0);
    }
  }
  openToolsbarPopup(tool: MarketplaceSolutionTools) {
     console.log(tool);
    this.dialog.open(MarketplacesolutionsPopupComponent, {
      data: {
        toolLaunchLink: tool.viewURL,
        toolID:tool.solutionId,
        toolName: tool.solutionName
      },
      height: '650px',
      width: '1100px',
      panelClass: 'toolsbarPopupStyle',
      autoFocus: false
    });
  }

}

import { Component, OnInit, Input } from '@angular/core';
import { MarketplaceTools } from '../../models/marketplace-tools.model';
import { FormControl } from '@angular/forms';
import { Subscription } from 'rxjs';
import { AMPLIFIER_DESCRIPTION } from '../../constants/amplifier-description';
import { MatDialog } from '@angular/material';
import { AmplifierPopupComponent } from './../amplifier-popup/amplifier-popup.component';
import { ProjectGlobalInfoModel } from 'src/app/shared/model/project-global-info.model';
import { PassGlobalInfoService } from 'src/app/shared/services/pass-project-global-info.service';
import { Router } from '@angular/router';

@Component({
  selector: 'app-marketplace-tools',
  templateUrl: './marketplace-tools.component.html',
  styleUrls: ['./marketplace-tools.component.scss']
})
export class MarketplaceToolsComponent implements OnInit {

  @Input() tools: MarketplaceTools[];
  myControl = new FormControl('');
  filtererdTools: MarketplaceTools[];
  inputSubscription: Subscription;
  description: string = AMPLIFIER_DESCRIPTION;
  projectGlobalInfo: ProjectGlobalInfoModel;

  constructor(public dialog: MatDialog, private data: PassGlobalInfoService, private router: Router) { }

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

  ngOnDestroy() {
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
      this.filtererdTools = this.tools.filter(option => option.toolName.toLowerCase().includes(value)).slice(0);
    }
    else {
      this.filtererdTools = this.tools.slice(0);
    }
  }

  openToolsbarPopup(tool: MarketplaceTools) {
    // console.log('Hello tool:'+tool);
    this.dialog.open(AmplifierPopupComponent, {
      data: {
        toolLaunchLink: tool.launchURL,
        toolDownloadLink: tool.DownloadURL,
        toolName: tool.toolName
      },
      height: '650px',
      width: '1100px',
      panelClass: 'toolsbarPopupStyle',
      autoFocus: false
    });
  }
}

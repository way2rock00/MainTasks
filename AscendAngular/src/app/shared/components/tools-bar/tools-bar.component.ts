import { ToolsBarPopupComponent } from './../tools-bar-popup/tools-bar-popup.component';
import { Component, OnInit, Input, OnChanges } from '@angular/core';
import { ToolsBarService } from '../../services/tools-bar.service';
import { MatDialog } from '@angular/material';

export interface ToolsbarPopupData {
  toolLaunchLink: string;
  toolName: string;
  toolDownloadLink: string;
}

@Component({
  selector: 'app-tools-bar',
  templateUrl: './tools-bar.component.html',
  styleUrls: ['./tools-bar.component.scss']
})
export class ToolsBarComponent implements OnInit, OnChanges {

  tools: any[];
  toolDescription: String = "";
  @Input() tabName: string;
  @Input() bgColor: string;

  CSS: any;

  constructor(private toolsBarService: ToolsBarService, public dialog: MatDialog) { }

  ngOnChanges() {
    // Get tools based on tab name
    this.tools = [];

    this.toolsBarService.getToolsDataURL().subscribe(data => {
      let tempTools = [];

      // Check for null output
      if (data == null) { data = [] }

      //Loop through the response to get tools based on tabname
      data.map(e => {
        if (e.Category == this.tabName)
          tempTools = tempTools.concat(e.data)
      });

      this.tools = tempTools;
    });

    this.CSS = { backgroundColor: this.bgColor }
  }

  ngOnInit() {


  }

  preview(toolLink) {
    window.open(toolLink);
    event.stopPropagation();
    //window.location.href = toolLink;
  }

  onHover(description) {
    this.toolDescription = description;
  }

  openToolsbarPopup(tool) {
    // console.log('Hello tool:'+tool);
    this.dialog.open(ToolsBarPopupComponent, {
      data: {
        toolLaunchLink: tool.launchURL,
        toolDownloadLink: tool.DownloadURL,
        toolName: tool.name
      },
      height: '650px',
      width: '1100px',
      panelClass: 'toolsbarPopupStyle',
      autoFocus: false
    });
  }
}

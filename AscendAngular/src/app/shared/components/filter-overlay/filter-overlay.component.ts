import { Component, OnInit,Input,ViewContainerRef,TemplateRef, Type } from '@angular/core';
//import { OverlayModule } from '@angular/cdk/overlay';
//import {Overlay,CdkOverlayOrigin, 
//  OverlayConfig, OverlayRef} from '@angular/cdk/overlay';
//import { OverlayReference } from '@angular/cdk/overlay/typings/overlay-reference';

import { FilterOverlayRef, FilterOverlayContent } from './filter-overlay-ref';

@Component({
  selector: 'app-filter-overlay',
  templateUrl: './filter-overlay.component.html',
  styleUrls: ['./filter-overlay.component.scss']
})
export class FilterOverlayComponent implements OnInit {
  renderMethod: 'template' | 'component' | 'text' = 'component';
  content: FilterOverlayContent;
  context;


  constructor(private filterOverlayRef: FilterOverlayRef) {
   }

  ngOnInit() {
    
    this.content = this.filterOverlayRef.content;

    if (typeof this.content === 'string') {
      this.renderMethod = 'text';
    }

    if (this.content instanceof TemplateRef) {
      this.renderMethod = 'template';
      this.context = {
        close: this.filterOverlayRef.close.bind(this.filterOverlayRef)
      }
    }
  }

  hideOverlay(){
  }
  
}

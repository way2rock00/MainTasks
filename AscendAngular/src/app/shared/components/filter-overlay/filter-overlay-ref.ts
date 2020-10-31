import { OverlayRef } from '@angular/cdk/overlay';
import { Subject } from 'rxjs';
import { FilterOverlayParams } from './filter-overlay.service';
import { TemplateRef, Type } from '@angular/core';

export type FilterOverlayCloseEvent<T = any> = {
  type: 'backdropClick' | 'close';
  data: T;
}
export type FilterOverlayContent = TemplateRef<any> | Type<any> | string;
export class FilterOverlayRef<T = any> { 
    private afterClosed = new Subject<FilterOverlayCloseEvent<T>>();
    afterClosed$ = this.afterClosed.asObservable();
  
    constructor(public overlay: OverlayRef,
      public content: FilterOverlayContent,
      public data: T) {
      overlay.backdropClick().subscribe(() => {
        this._close('backdropClick', null);
      });
    }
  
    close(data?: T) {
      this._close('close', data);
    }
  
    private _close(type: FilterOverlayCloseEvent['type'], data?: T) {
      this.overlay.dispose();
      this.afterClosed.next({
        type,
        data
      });
      this.afterClosed.complete();
    }
}
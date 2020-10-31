import { Injectable, Injector } from '@angular/core';
import { Overlay, ConnectionPositionPair, PositionStrategy, OverlayConfig } from '@angular/cdk/overlay';
import { PortalInjector, ComponentPortal } from '@angular/cdk/portal';
import { FilterOverlayRef, FilterOverlayContent } from './filter-overlay-ref'; 
import { FilterOverlayComponent } from './filter-overlay.component';

export type FilterOverlayParams<T> = {
    width?: string | number;
    height?: string | number;
    origin: HTMLElement;
    content: FilterOverlayContent;
    data?: T;
  }

  @Injectable({
    providedIn: 'root'
  })

  export class FilterOverlay{
    constructor(private overlay: Overlay, private injector: Injector) { }

    open<T>({ origin, content, data, width, height }: FilterOverlayParams<T>): FilterOverlayRef<T> {
        const overlayRef = this.overlay.create(this.getOverlayConfig({ origin, width, height }));
        const filterOverlayRef = new FilterOverlayRef<T>(overlayRef, content, data);
    
        const injector = this.createInjector(filterOverlayRef, this.injector);
        overlayRef.attach(new ComponentPortal(FilterOverlayComponent, null, injector));
    
        return filterOverlayRef;
      }
    
      private getOverlayConfig({ origin, width, height }): OverlayConfig {
        return new OverlayConfig({
          hasBackdrop: true,
          width,
          height,
          backdropClass: 'filter-overlay-backdrop',
          positionStrategy: this.getOverlayPosition(origin),
          scrollStrategy: this.overlay.scrollStrategies.reposition()
        });
      }
    
      private getOverlayPosition(origin: HTMLElement): PositionStrategy {
        const positionStrategy = this.overlay.position()
          .flexibleConnectedTo(origin)
          .withPositions(this.getPositions())
          .withFlexibleDimensions(false)
          .withPush(false);
    
        return positionStrategy;
      }
    
      createInjector(filterOverlayRef: FilterOverlayRef, injector: Injector) {
        const tokens = new WeakMap([[FilterOverlayRef, filterOverlayRef]]);
        return new PortalInjector(injector, tokens);
      }
    
      private getPositions(): ConnectionPositionPair[] {
        return [
          {
            originX: 'start',
            originY: 'bottom',
            overlayX: 'start',
            overlayY: 'bottom'
          },
          {
            originX: 'start',
            originY: 'bottom',
            overlayX: 'start',
            overlayY: 'top',
          },
        ]
      }
  }
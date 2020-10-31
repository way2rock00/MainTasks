import { NgModule } from '@angular/core';

/* -- THIRD PARTY -- **/
import { CarouselModule } from 'ngx-owl-carousel-o';
import { OverlayModule } from '@angular/cdk/overlay';

import { CdkTreeModule } from '@angular/cdk/tree';
import { DragDropModule } from '@angular/cdk/drag-drop';

/* -- ANGULAR MATERIAL -- */
import {
  MatButtonModule,
  MatMenuModule,
  MatIconModule,
  MatCardModule,
  MatTabsModule,
  MatSidenavModule,
  MatFormFieldModule,
  MatInputModule,
  MatTooltipModule,
  MatToolbarModule,
  MatCheckboxModule,
  MatSelectModule,
  MatAutocompleteModule,
  MatExpansionModule,
  MatGridListModule,
  MatDialogModule,
  MatTableModule,
  MatProgressBarModule,
  MatSnackBarModule,
  MatSortModule,
  MatBadgeModule,
  MatTreeModule,
  MatDividerModule,
  MatRadioModule,
  MatDatepickerModule,
  MatNativeDateModule,
} from '@angular/material';
import { NgGanttEditorModule } from 'ng-gantt';

const INCLUDE_MODULE = [
  /* -- ANGULAR MATERIAL -- */
  MatButtonModule,
  MatMenuModule,
  MatIconModule,
  MatCardModule,
  MatTabsModule,
  MatSidenavModule,
  MatSidenavModule,
  MatFormFieldModule,
  MatInputModule,
  MatTooltipModule,
  MatToolbarModule,
  MatCheckboxModule,
  MatSelectModule,
  MatAutocompleteModule,
  MatExpansionModule,
  MatGridListModule,
  MatDialogModule,
  MatTableModule,
  MatProgressBarModule,
  MatSnackBarModule,
  MatSortModule,
  MatBadgeModule,
  MatTreeModule,
  MatDividerModule,
  CdkTreeModule,
  MatRadioModule,
  DragDropModule,
  MatDatepickerModule,
  MatNativeDateModule,
  /*-- THIRD PARTY --*/
  CarouselModule,
  OverlayModule,  
  /* Gantt module */
  NgGanttEditorModule
]


@NgModule({
  imports: INCLUDE_MODULE,
  exports: INCLUDE_MODULE
})
export class ThirdPartyModule { }

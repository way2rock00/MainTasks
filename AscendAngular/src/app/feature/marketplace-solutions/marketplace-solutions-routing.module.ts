import { NgModule } from '@angular/core';
import { Routes, RouterModule } from '@angular/router';
import { MarketplacesolutionsComponent } from './components/marketplacesolutions/marketplacesolutions.component';


const routes: Routes = [{ path: '', component: MarketplacesolutionsComponent },];

@NgModule({
  imports: [RouterModule.forChild(routes)],
  exports: [RouterModule]
})
export class MarketplaceSolutionsRoutingModule { }

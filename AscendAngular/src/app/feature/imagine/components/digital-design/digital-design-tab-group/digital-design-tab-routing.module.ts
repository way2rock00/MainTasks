import { NgModule } from '@angular/core';
import { Routes, RouterModule } from '@angular/router';

import { BusinessProcessComponent } from './business-process/business-process.component';
import { UserStoriesComponent } from './user-stories/user-stories.component';
import { ErpConfigurationComponent } from './erp-configuration/erp-configuration.component';
import { BusinessSolutionComponent } from './business-solution/business-solution.component';
import { InterfacesComponent } from './interfaces/interfaces.component';
import { ReportsComponent } from './reports/reports.component';
import { KeyBusinessDecisionsComponent } from './key-business-decisions/key-business-decisions.component';

const routes: Routes = [
  { path: 'process flows', component: BusinessProcessComponent  },
  { path: 'user stories', component: UserStoriesComponent  },
  { path: 'erp configurations', component: ErpConfigurationComponent  },
  { path: 'business solutions', component: BusinessSolutionComponent  },
  { path: 'interfaces', component: InterfacesComponent  },
  { path: 'analytics & reports', component: ReportsComponent  },
  { path: 'key design decisions', component: KeyBusinessDecisionsComponent  },
];

@NgModule({
  imports: [RouterModule.forChild(routes)],
  exports: [RouterModule]
})
export class DigitalDesignTabRoutingModule { }

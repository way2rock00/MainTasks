import { NgModule } from '@angular/core';
import { Routes, RouterModule } from '@angular/router';

import { HomeComponent } from './components/home/home.component';
import { WelcomeComponent } from './components/welcome/welcome.component';
import { UserSelectionComponent } from './components/user-selection/user-selection.component';

import { environment} from '../../environments/environment';

const INITIAL_REDIRECT: string = environment.isLocal ? '/userselection' : '/welcome';

export const routes: Routes = [     
    { path: '', pathMatch: 'full', redirectTo: INITIAL_REDIRECT},
    { path: 'welcome', component: WelcomeComponent},
    { path: 'home', component: HomeComponent}
];

if (environment.isLocal) {
    routes.push({ path: 'userselection', component: UserSelectionComponent });
}

@NgModule({
    imports: [RouterModule.forChild(routes)],
    exports: [RouterModule]
})
  export class BaseRoutingModule { }
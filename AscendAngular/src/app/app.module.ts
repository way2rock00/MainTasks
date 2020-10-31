/* -- ANGULAR CORE -- */
import { NgModule } from '@angular/core';
import { BrowserModule } from '@angular/platform-browser';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';

/* -- APPLICATION MODULES -- */
import { SharedModule } from './shared/shared.module';
import { AppRoutingModule } from './app-routing.module';

import { AppComponent } from './app.component';
import { HttpClientModule } from '@angular/common/http';

import { HttpServiceHelper } from './types/common/HttpServiceHelper';
import { AuthenticationModule } from './authentication.module';



@NgModule({
  declarations: [
    AppComponent
  ],
  imports: [
    BrowserModule,
    BrowserAnimationsModule,
    HttpClientModule,
    AuthenticationModule,
    AppRoutingModule,
    SharedModule.forRoot()
  ],
  providers: [
    HttpServiceHelper
  ],
  bootstrap: [AppComponent],
  entryComponents: []
})

export class AppModule { }

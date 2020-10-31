import { Component, OnInit } from '@angular/core';
import { FormControl } from '@angular/forms';
import { Router } from '@angular/router';
import { Observable } from 'rxjs';
import { map, startWith } from 'rxjs/operators';
import { SharedService } from '../../services/shared.service';
import { PassGlobalInfoService } from '../../services/pass-project-global-info.service';
import { ProjectGlobalInfoModel } from '../../model/project-global-info.model';
import { environment } from 'src/environments/environment';

@Component({
  selector: 'app-nav-search',
  templateUrl: './nav-search.component.html',
  styleUrls: ['./nav-search.component.scss']
})
export class NavSearchComponent implements OnInit {

  colorSwitch: string = 'true';
  disabled: true;
  myControl = new FormControl();
  options: any[] = [];
  toHighlight: string;

  filteredOptions: Observable<string[]>;
  projectGlobalInfo: ProjectGlobalInfoModel;

  constructor(
    private router: Router,
    private sharedService: SharedService,
    private globalData: PassGlobalInfoService
  ) { }

  ngOnInit() {

    this.globalData.share.subscribe(data => {
      this.projectGlobalInfo = data;
      this.projectGlobalInfo.projectId = this.projectGlobalInfo.projectId ? this.projectGlobalInfo.projectId : '0';
    });

    this.sharedService.getData(`${environment.BASE_URL}/searchlinks/${this.projectGlobalInfo.projectId}`).subscribe(tabs => {
      this.options = tabs;
      this.filteredOptions = this.myControl.valueChanges
        .pipe(
          startWith(''),
          map(value => value.length > 1 ? this._filter(value) : [])
        );
    });
  }

  private _filter(value: string): any[] {
    const filterValue = value.toLowerCase();
    this.toHighlight = filterValue;
    return this.options.filter(option => (option.entityName + " - " + option.contentName).toLowerCase().includes(filterValue));
  }

  continueSearch(e) {
    console.log(e);
    this.myControl.setValue('');
    let URL = "activities/search/" + e.option.value.phase + "/" + e.option.value.stop + "/" + e.option.value.contentId + "/" + e.option.value.tabCode;
    this.router.navigate([URL]).then(data => {
      this.off();
    });
  }

  changeColor() {
    this.colorSwitch = 'false';
    document.getElementById("overlay").style.display = "block";
  }

  off() {
    this.colorSwitch = 'true';
    document.getElementById("overlay").style.display = "none";
  }
}
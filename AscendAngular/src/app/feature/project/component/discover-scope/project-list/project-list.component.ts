import { CryptUtilService } from './../../../../../shared/services/crypt-util.service';
import { Component, OnInit } from '@angular/core';
import { UserInfo } from '../../../constants/ascend-user-project-info';
import { MatDialog } from '@angular/material/dialog';
import { ProjectGlobalInfoModel } from 'src/app/shared/model/project-global-info.model';
import { PassGlobalInfoService } from 'src/app/shared/services/pass-project-global-info.service';
import { Router } from '@angular/router';
import { environment } from 'src/environments/environment';
import { Observable } from 'rxjs';
import { FormControl } from '@angular/forms';
import { startWith, map } from 'rxjs/operators';
import { DomSanitizer } from '@angular/platform-browser';
import { UserProjectInfo } from 'src/app/shared/constants/ascend-project-info-type';
import { ProjectPopupComponent } from '../project-popup/project-popup.component';
import { SharedService } from 'src/app/shared/services/shared.service';

@Component({
  selector: 'app-project-list',
  templateUrl: './project-list.component.html',
  styleUrls: ['./project-list.component.scss']
})
export class ProjectListComponent implements OnInit {

  projectInfo: any;
  projectGlobalInfo: ProjectGlobalInfoModel = new ProjectGlobalInfoModel();
  filteredOptions: Observable<UserProjectInfo[]>;
  myControl = new FormControl();
  projectMenuItems = [
    {
      label: 'Modify Project',
      route: ''
    },
    {
      label: 'Generate Timeline',
      route: '/project/timeline'
    }
  ]

  constructor(public dialog: MatDialog
    , private globalData: PassGlobalInfoService
    , private router: Router
    , private sharedService: SharedService
    , private sanitizer: DomSanitizer
  ) { }

  ngOnInit() {

    this.globalData.share.subscribe(x => {
      this.projectGlobalInfo = x;
      this.projectMenuItems[0].route = '/project/psg/' + this.projectGlobalInfo.projectId;
    });

    this.sharedService.getData(`${environment.BASE_URL}/projectdetailsPSG/` + this.projectGlobalInfo.projectId).subscribe(res => {
      this.projectInfo = res;
      this.myControl.setValue('');
    });

    this.filteredOptions = this.myControl.valueChanges.debounceTime(400)
      .pipe(
        startWith(''),
        map(value => this._filter(value))
      );
  }

  goBack() {
    this.router.navigate(['/project/list'])
  }

  public handleStaticResultSelected(result) {
    this.myControl.setValue(result.option.value.projectName);
  }

  private _filter(value: string): UserProjectInfo[] {
    const filterValue = value.toLowerCase();
    if (this.projectInfo)
      return this.projectInfo[0].projectType.filter(option => option.projectName.toLowerCase().indexOf(filterValue) >= 0);
    else
      return [];
  }

  changeColor() {
    document.getElementById("overlay").style.display = "block";
    event.stopPropagation();
  }

  off() {
    document.getElementById("overlay").style.display = "none";
  }

  goToAction(menuIem) {
    if (menuIem.route)
      this.router.navigate([`${menuIem.route}`]);
  }

  getSafeURL(logoURL) {
    return this.sanitizer.bypassSecurityTrustResourceUrl(logoURL);
  }

  openDialog(projectInfo): void {
    const dialogRef = this.dialog.open(ProjectPopupComponent, {
      width: '650px',
      data: projectInfo,
      panelClass: 'infoPopupStyle'
    });
  }
}

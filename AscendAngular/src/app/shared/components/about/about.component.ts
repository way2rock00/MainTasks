import { CryptUtilService } from './../../services/crypt-util.service';
import { Component, OnInit } from '@angular/core';
import { ProjectGlobalInfoModel } from '../../model/project-global-info.model';
import { PassGlobalInfoService } from '../../services/pass-project-global-info.service';
import { Router } from '@angular/router';

class AboutLinksModel {
  imageURL: string;
  imageCaption: string;
  route: string;
  newWindow ?: boolean;
}

@Component({
  selector: 'app-about',
  templateUrl: './about.component.html',
  styleUrls: ['./about.component.scss']
})
export class AboutComponent implements OnInit {

  links: AboutLinksModel[] = [
    {
      imageURL: "../../assets/images/Group_28.png",
      imageCaption: "See Demo",
      route: "https://www.km.deloitteresources.com/sites/live/_layouts/dtts.dr.kamdocumentforms/displayformredirect.aspx?ID=KMIP-6640939",
      newWindow: true
    }
    /*,
    {
      imageURL: "../../assets/images/Group_30.png",
      imageCaption: "Tutorials",
      route: "/tutorials"
    },
    {
      imageURL: "../../assets/images/Group_32.png",
      imageCaption: "Training materials",
      route: "/marketing"
    }*/
  ];

  projectGlobalInfo: ProjectGlobalInfoModel = new ProjectGlobalInfoModel();

  constructor(private router: Router
    , private globalData: PassGlobalInfoService
    , private cryptUtilService: CryptUtilService) { }

  ngOnInit() {
    this.globalData.share.subscribe(data => {
      this.projectGlobalInfo = data;
    });
  }

  goto(route, newWindow) {
    if (route) {
      if(newWindow)
        window.open(route)
      else
        this.router.navigate([route]);
    }
  }

  enterExploreMode() {
    if (this.projectGlobalInfo.viewMode != "EXPLORE") {
      this.projectGlobalInfo.viewMode = "EXPLORE";
      this.projectGlobalInfo.projectId = "0";
      this.projectGlobalInfo.uniqueId = "0";
      this.projectGlobalInfo.projectName = "";
      this.projectGlobalInfo.clientName = "";
      this.projectGlobalInfo.clientUrl = "";
      this.cryptUtilService.sessionClear();
      this.globalData.updateGlobalData(this.projectGlobalInfo);
    }
  }
}

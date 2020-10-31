import { Injectable } from '@angular/core';
import { Observable, of } from 'rxjs';
import { HttpClient } from '@angular/common/http';
import { environment } from 'src/environments/environment';
import { PassGlobalInfoService } from '../services/pass-project-global-info.service';
import { ProjectGlobalInfoModel } from '../model/project-global-info.model';
import { CryptUtilService } from './crypt-util.service';

@Injectable({
  providedIn: 'root'
})
export class FilterOverlayService {

  industries: any[] = [];
  sectors: any[] = [];
  regions: any[] = [];
  businessProcess: any[] = [];

  selectedIndustry: any[] = [];
  selectedSectors: any[] = [];
  selectedRegions: any[] = [];
  selectedL0: any[] = [];
  selectedL1: any[] = [];
  selectedL2: any[] = [];

  projectGlobalInfo: ProjectGlobalInfoModel;
  
  private industryURL = `${environment.BASE_URL}/architect/filterindustryv5/`;
  private regionURL = `${environment.BASE_URL}/architect/filterregionv5/`;
  private businessProcessURL = `${environment.BASE_URL}/architect/filterbusinessv5/`;

  constructor(private http: HttpClient, private globalData: PassGlobalInfoService, private cryptUtilService: CryptUtilService) {
    this.globalData.share.subscribe(x => {
      this.projectGlobalInfo = x;
      this.projectGlobalInfo.uniqueId = x.uniqueId?x.uniqueId:"0";
    });
   }

  //Filter changes
  getFilterDataURL(URL, tabCode?: any): Observable<any> {
    return this.http.get<any>(`${URL}${this.projectGlobalInfo.uniqueId}/${tabCode}`);
    // return this.http.get<any>(`${URL}${this.projectGlobalInfo.uniqueId}`);
  }

  async setFilterData(tabCode?: any, stopName?: any): Promise<any> {

    let self = this;

    let projectView = (this.projectGlobalInfo.viewMode == "PROJECT");
    let identifierTrailer = stopName + tabCode;

    // Filter changes
    await this.getFilterDataURL(this.industryURL, tabCode)
      .toPromise().then(data => {

        let selectedArray1 = [];
        let selectedArray2 = [];

        //clear existing filters
        this.industries = [];
        this.selectedIndustry = [];
        this.sectors = [];
        this.selectedSectors = [];

        if (data != null) {

          if (data[0].industryApplicable == "Y") { //Check if industry filter is applicable

            data[0].industryData.map(function (e) {

              let l1Filters = [];

              if (data[0].sectorApplicable == "Y") { //Check if sector filter is applicable

                e.sector.map(function (l1) {
                  l1Filters.push({ L1: l1.sectors, L1Id: l1.sectorId }); //Form sectors arrays
                  selectedArray2.push({ L1: l1.sectors, Id: l1.sectorId }); // Form selected sectors array
                });
              }

              self.industries.push({ L0: e.industry, L0Id: e.industryId, L1Map: l1Filters }); //Form industry array
              selectedArray1.push({ L0: e.industry, Id: e.industryId }); // Form selected industry
            });

            //session storage change
            let ssL0Identifier = 'L0SelectedData' + "I" + identifierTrailer;
            let ssL1Identifier = 'L1SelectedData' + "I" + identifierTrailer;

            let ssFilterData = this.cryptUtilService.getItem(ssL0Identifier, 'SESSION');
            let ssL1FilterData = this.cryptUtilService.getItem(ssL1Identifier, 'SESSION');

            if (projectView) {

              self.selectedIndustry = selectedArray1
              self.selectedSectors = selectedArray2
            }

            //session storage change
            if(ssFilterData){

              let resultArray = [];
              ssFilterData.map(function (p) {
                if (p.checked) resultArray.push({ L0: p.L0, Id: p.L0Id });
              });
              self.selectedIndustry = resultArray;
    
            }
    
            if(ssL1FilterData){
              let resultArray = [];
              ssL1FilterData.map(function (p) {
                resultArray.push({ L1: p.L1, Id: p.L1Id });
              });
              self.selectedSectors = resultArray;
            }
          }
        }
      });

    //Filter changes
    await this.getFilterDataURL(this.regionURL, tabCode)
      .toPromise().then(data => {
        let selectedArray1 = [];

        //clear existing filters
        this.regions = [];
        this.selectedRegions = [];

        if (data != null) {

          if (data[0].regionApplicable=="Y") {

            data[0].regionData.map(function (e) {
              self.regions.push({ L0: e.region, L0Id: e.regionId, L1Map: [] })
              selectedArray1.push({ L0: e.region, Id: e.regionId });
            });

            let ssL0Identifier = 'L0SelectedData' + "r" + identifierTrailer;
            let ssFilterData = this.cryptUtilService.getItem(ssL0Identifier, 'SESSION');

            if (projectView && self.selectedRegions.length == 0) {
              self.selectedRegions = selectedArray1;
            }

            if(ssFilterData){

              let resultArray = [];
              ssFilterData.map(function (p) {
                if (p.checked) resultArray.push({ L0: p.L0, Id: p.L0Id });
              });
              self.selectedRegions = resultArray;
    
            }
          }
        }      
      });

    //Filter changes
    await this.getFilterDataURL(this.businessProcessURL, tabCode)
      .toPromise().then(data => {

        let selectedArray1 = [];
        let selectedArray2 = [];
        let selectedArray3 = [];
        let array1 = [];

        //clear existing filters
        this.businessProcess = [];
        this.selectedL0 = [];
        this.selectedL1 = [];
        this.selectedL2 = [];

        if ( data != null) {
          //Check if L1 filter is applicable
          if (data[0].businessProcessL1Applicable == "Y") {

            data[0].businessProcessData.map(l0 => {

              let array2 = [];

              if (data[0].businessProcessL2Applicable == "Y") { //Check if L2 filter is applicable
                if(l0.L1Map){
                  l0.L1Map.map(l1 => {

                    let array3 = [];
  
                    if (data[0].businessProcessL3Applicable == "Y") { //Check if L3 filter is applicable
                      l1.L2Map.map(l2 => {
                        selectedArray3.push({ L2: l2.L2, Id: l2.L2Id }); //Push to L3 selected
                        array3.push(l2); //Push to L3 array
                      });
  
                    }
  
                    array2.push({ L1: l1.L1, L1Id: l1.L1Id, L2Map: array3 }); //Form L2 array
                    selectedArray2.push({ L1: l1.L1, Id: l1.L1Id }); //Push to L2 selected
  
                  });
                }
              }

              array1.push({ L0: l0.L0, L0Id: l0.L0Id, L1Map: array2 })  //Form L1 array
              selectedArray1.push({ L0: l0.L0, Id: l0.L0Id }); //Push to L1 selectedL1

            });
          }

          this.businessProcess = array1;

        }

        let ssL0Identifier = 'L0SelectedData' + "B" + identifierTrailer;
        let ssL1Identifier = 'L1SelectedData' + "B" + identifierTrailer;
        let ssL2Identifier = 'L2SelectedData' + "B" + identifierTrailer;

        let ssFilterData = this.cryptUtilService.getItem(ssL0Identifier, 'SESSION');
        let ssL1FilterData = this.cryptUtilService.getItem(ssL1Identifier, 'SESSION');
        let ssL2FilterData = this.cryptUtilService.getItem(ssL2Identifier, 'SESSION');


        if (projectView && self.selectedL0.length == 0) {

          self.selectedL0 = selectedArray1;
          self.selectedL1 = selectedArray2;
          self.selectedL2 = selectedArray3; // Filter change
        }

        if(ssFilterData){

          let resultArray = [];
          ssFilterData.map(function (p) {
            if (p.checked) resultArray.push({ L0: p.L0, Id: p.L0Id });
          });
          self.selectedL0 = resultArray;

        }

        if(ssL1FilterData){
          let resultArray = [];
          ssL1FilterData.map(function (p) {
            resultArray.push({ L1: p.L1, Id: p.L1Id });
          });
          self.selectedL1 = resultArray;
        }

        if(ssL2FilterData){
           let resultArray = [];
           ssL2FilterData.map(function (p) {
            resultArray.push({ L2: p.L2, Id: p.L2Id });
          });
          self.selectedL2 = resultArray;
        }
      });

    return tabCode;
  }

}

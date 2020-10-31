import { Component, OnInit } from '@angular/core';
import { Observable } from 'rxjs';
import { FormControl } from '@angular/forms';
import { environment } from 'src/environments/environment';
import { MatDialog } from '@angular/material';
import { SharedService } from '../../services/shared.service';
import { VideoPlayerComponent } from '../video-player/video-player.component';
import { startWith, map } from 'rxjs/operators';

class MarketingModel {
  heading: string;
  description: string;
  doclink: string;
  videolink: string;
}

@Component({
  selector: 'app-marketing-materials',
  templateUrl: './marketing-materials.component.html',
  styleUrls: ['./marketing-materials.component.scss']
})

export class MarketingMaterialsComponent implements OnInit {

  filteredOptions: Observable<string[]>;
  myControl = new FormControl();
  marketingData: MarketingModel[];
  constMarketingData: MarketingModel[];
  searchResult = '';
  staticList = [];
  marketingUrl: any = `${environment.BASE_URL}/marketingmaterials`

  constructor(public dialog: MatDialog, private sharedService: SharedService) { }

  ngOnInit() {
    this.initializeData();
  }

  openVideoPlayer(link) {
    if (link) {
      this.dialog.open(VideoPlayerComponent, {
        data: {
          videoLink: link
        },
        height: 'auto',
        width: 'auto',
        panelClass: 'videoPlayer',
        autoFocus: false
      });
    }
  }

  private _filter(value: string): string[] {
    const filterValue = value.toLowerCase();
    return this.staticList.filter(option => option.toLowerCase().includes(filterValue));
  }

  handleStaticResultSelected(result) {
    this.searchResult = result;
    let filter = this.searchResult;

    if (filter != '') {
      this.marketingData =
        this.constMarketingData.filter(function (obj) {
          return (obj.heading).toUpperCase().indexOf(filter.toUpperCase()) == -1 ? false : true;
        });
    } else {
      this.marketingData = this.constMarketingData;
    }
  }

  initializeData() {
    this.sharedService.getData(this.marketingUrl).subscribe(
      (data) => {
        if (data != null) {
          this.constMarketingData = data;
          this.marketingData = this.constMarketingData;
          this.initializeStaticList(this.marketingData);
        }
      }
    );

    this.filteredOptions = this.myControl.valueChanges
      .pipe(
        startWith(''),
        map(value => this._filter(value))
      );
  }

  initializeStaticList(array) {
    this.staticList = [];
    for (let index = 0; index < array.length; index++) {
      let element = array[index];
      this.staticList.push(element.heading);
    }
    this.filteredOptions = this.myControl.valueChanges
      .pipe(
        startWith(''),
        map(value => this._filter(value))
      );
  }

  handleFocusOutEvent(result) {
    this.searchResult = result.target.value;
    let filter = this.searchResult;
    if (filter != '') {
      this.marketingData =
        this.constMarketingData.filter(function (obj) {
          return (obj.heading).toUpperCase().indexOf(filter.toUpperCase()) == -1 ? false : true;
        });
    } else {
      this.marketingData = this.constMarketingData;
    }
  }

  openDocument(link) {
    if (link) {
      window.open(link);
    }
  }

}

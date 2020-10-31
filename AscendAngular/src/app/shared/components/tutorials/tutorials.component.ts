import { SharedService } from './../../services/shared.service';
import { VideoPlayerComponent } from './../video-player/video-player.component';
import { Component, OnInit } from '@angular/core';
import { Observable } from 'rxjs';
import { FormControl } from '@angular/forms';
import { MatDialog } from '@angular/material';
import { ToolsBarPopupComponent } from '../tools-bar-popup/tools-bar-popup.component';
import { environment } from 'src/environments/environment';
import { startWith, map } from 'rxjs/operators';

class TutorialsModel {
  heading: string;
  description: string;
  doclink: string;
  videolink: string;
}

@Component({
  selector: 'app-tutorials',
  templateUrl: './tutorials.component.html',
  styleUrls: ['./tutorials.component.scss']
})
export class TutorialsComponent implements OnInit {

  filteredOptions: Observable<string[]>;
  myControl = new FormControl();
  tutorialsData: TutorialsModel[];
  constTutorialsData: TutorialsModel[];
  searchResult = '';
  staticList = [];
  tutorialsUrl: any = `${environment.BASE_URL}/tutorials`

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
      this.tutorialsData =
        this.constTutorialsData.filter(function (obj) {
          return (obj.heading).toUpperCase().indexOf(filter.toUpperCase()) == -1 ? false : true;
        });
    } else {
      this.tutorialsData = this.constTutorialsData;
    }
  }

  initializeData() {
    this.sharedService.getData(this.tutorialsUrl).subscribe(
      (data) => {
        if (data != null) {
          this.constTutorialsData = data;
          this.tutorialsData = this.constTutorialsData;
          this.initializeStaticList(this.tutorialsData);
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
      this.tutorialsData =
        this.constTutorialsData.filter(function (obj) {
          return (obj.heading).toUpperCase().indexOf(filter.toUpperCase()) == -1 ? false : true;
        });
    } else {
      this.tutorialsData = this.constTutorialsData;
    }
  }

  openDocument(link) {
    if (link) {
      window.open(link);
    }
  }
}

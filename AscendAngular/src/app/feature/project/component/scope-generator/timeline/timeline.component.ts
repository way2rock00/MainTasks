import { Component, OnInit, ViewChild, ViewEncapsulation } from '@angular/core';
import { MatDialog } from '@angular/material/dialog';
import { Router } from '@angular/router';
import { GanttEditorComponent } from 'ng-gantt';

export interface DialogueBoxData {
  from: string;
  message: string;
}


@Component({
  selector: 'app-timeline',
  templateUrl: './timeline.component.html',
  styleUrls: ['./timeline.component.scss']
})
export class TimelineComponent implements OnInit {

  public editorOptions: any = {};
  public data: any;
  public data2: any;

  vUseSingleCell = '0';
  vShowRes = '0';
  vShowCost = '0';
  vShowComp = '0';
  vShowDur = '1';
  vShowStartDate = '1';
  vShowEndDate = '1';
  vShowPlanStartDate = '0';
  vShowPlanEndDate = '0';
  vShowEndWeekDate = '0';
  vShowTaskInfoLink = '0';
  vDebug = 'false';
  vEditable = 'false';
  vUseSort = 'false';
  vLang = 'en';
  delay = 10;
  vExport = '0';

  @ViewChild('editor', { static: true }) editor: GanttEditorComponent;
  @ViewChild('editorTwo', { static: true }) editorTwo: GanttEditorComponent;

  constructor(private router: Router, private dialog: MatDialog) {
  }

  goBack() {
    this.router.navigate(['/project/list'])
  }

  ngOnInit() {

    this.data = this.initialData();

    // below code gets the datasource for chart. send the get url as an arg. left blank for now
    // this.commonService.getData('').subscribe(data => {
    //   if (data)
    //     this.data = data;
    // })

    // this.data2 = [{
    //   'pID': 1,
    //   'pName': 'Define Chart API v2',
    //   'pStart': '',
    //   'pEnd': '',
    //   'pClass': 'ggroupblack',
    //   'pLink': '',
    //   'pMile': 0,
    //   'pRes': 'Brian',
    //   'pComp': 0,
    //   'pGroup': 1,
    //   'pParent': 0,
    //   'pOpen': 1,
    //   'pDepend': '',
    //   'pCaption': '',
    //   'pNotes': 'Some Notes text'
    // }];

    const vAdditionalHeaders = {
      // category: {
      //   title: 'Category'
      // },
      // sector: {
      //   title: 'Sector'
      // }
    };


    this.editorOptions = {
      vCaptionType: 'Complete',  // Set to Show Caption : None,Caption,Resource,Duration,Complete,
      vQuarterColWidth: 36,
      vDateTaskDisplayFormat: 'day dd month yyyy', // Shown in tool tip box
      vDayMajorDateDisplayFormat: 'mon yyyy - Week ww', // Set format to display dates in the "Major" header of the "Day" view
      vWeekMinorDateDisplayFormat: 'dd mon', // Set format to display dates in the "Minor" header of the "Week" view
      vLang: this.vLang,
      vUseSingleCell: this.vUseSingleCell,
      vShowRes: parseInt(this.vShowRes, 10),
      vShowCost: parseInt(this.vShowCost, 10),
      vShowComp: parseInt(this.vShowComp, 10),
      vShowDur: parseInt(this.vShowDur, 10),
      vShowStartDate: parseInt(this.vShowStartDate, 10),
      vShowEndDate: parseInt(this.vShowEndDate, 10),
      vShowPlanStartDate: parseInt(this.vShowPlanStartDate, 10),
      vShowPlanEndDate: parseInt(this.vShowPlanEndDate, 10),
      vShowTaskInfoLink: parseInt(this.vShowTaskInfoLink, 10), // Show link in tool tip (0/1)
      // Show/Hide the date for the last day of the week in header for daily view (1/0)
      vShowEndWeekDate: parseInt(this.vShowEndWeekDate, 10),
      vAdditionalHeaders: vAdditionalHeaders,
      vEvents: {
        taskname: console.log,
        res: console.log,
        dur: console.log,
        comp: console.log,
        start: console.log,
        end: console.log,
        planstart: console.log,
        planend: console.log,
        cost: console.log
      },
      vEventsChange: {
        taskname: this.editValue.bind(this, this.data),
        res: this.editValue.bind(this, this.data),
        dur: this.editValue.bind(this, this.data),
        comp: this.editValue.bind(this, this.data),
        start: this.editValue.bind(this, this.data),
        end: this.editValue.bind(this, this.data),
        planstart: this.editValue.bind(this, this.data),
        planend: this.editValue.bind(this, this.data),
        cost: this.editValue.bind(this, this.data)
      },
      vResources: [
        // { id: 0, name: 'Anybody' },
        // { id: 1, name: 'Mario' },
        // { id: 2, name: 'Henrique' },
        // { id: 3, name: 'Pedro' }
      ],
      vEventClickRow: console.log,
      vTooltipDelay: this.delay,
      vDebug: this.vDebug === 'true',
      vEditable: this.vEditable === 'true',
      vUseSort: this.vUseSort === 'true',
      vFormatArr: ['Day', 'Week', 'Month', 'Quarter'],
      vFormat: 'month',
    };
    this.editor.setOptions(this.editorOptions);
    // this.editorOptions.onChange = this.change.bind(this);
  }

  editValue(list, task, event, cell, column) {
    const found = list.find(item => item.pID == task.getOriginalID());
    if (!found) {
      return;
    } else {
      found[column] = event ? event.target.value : '';
    }
  }

  change() {
    console.log('change:', this.editor);
    console.log('change2:', this.editorTwo);
  }

  setLanguage(lang) {
    this.editorOptions.vLang = lang;
    this.editor.setOptions(this.editorOptions);
  }

  //function to save the changes
  save() {
    // this.commonService.postData('', this.data).subscribe(data => {
    //   if (data.status == 200) {
    //     this.dialog.open(CommonDialogueBoxComponent, {
    //       data: {
    //         from: '',
    //         message: 'Changes successfully saved'
    //       }
    //     });
    //   } else {
    //     this.dialog.open(CommonDialogueBoxComponent, {
    //       data: {
    //         from: '',
    //         message: 'Unexpected error. Please try again.'
    //       }
    //     });
    //   }
    // })
  }

  customLanguage() {
    // this.editorOptions.languages = {
    //   'pt-BR': {
    //     'auto': 'Automático testing'
    //   },
    //   'en': {
    //     'auto': 'Auto testing'
    //   }
    // };
    this.editor.setOptions(this.editorOptions);
  }

  changeObject() {
    this.data.randomNumber = Math.random() * 100;
  }

  changeData() {
    this.data = Object.assign({}, this.data,
      { randomNumber: Math.random() * 100 });
  }

  /**
   * Example on how get the json changed from the jsgantt
   */
  getData() {
    // const changedGantt = this.editor.get();
    // console.log(changedGantt);
  }

  clear() {
    const g = this.editor.getEditor();
    g.ClearTasks();
    g.Draw()
  }

  initialData() {
    return [
      {
        'pID': 1,
        'pName': 'Plan',
        'pStart': '2021-01-01',
        'pEnd': '2021-01-31',
        'pClass': 'ggroupblack',
        'pLink': '',
        'pMile': 0,
        'pRes': '',
        'pComp': 0,
        'pGroup': 1,
        'pParent': 0,
        'pOpen': 0,
        'pDepend': '',
        'pCaption': '',
        'pNotes': ''
      },
      {
        'pID': 11,
        'pName': 'Mobilize',
        'pStart': '2021-01-01',
        'pEnd': '2021-01-31',
        'pClass': 'gtaskpurple',
        'pLink': '',
        'pMile': 0,
        'pRes': 'Resource 1',
        'pComp': 0,
        'pGroup': 0,
        'pParent': 1,
        'pOpen': 0,
        'pDepend': '',
        'pCaption': '',
        'pNotes': ''
      },
      {
        'pID': 2,
        'pName': 'Design',
        'pStart': '2021-01-15',
        'pEnd': '2021-04-30',
        'pClass': 'ggroupblack',
        'pLink': '',
        'pMile': 0,
        'pRes': '',
        'pComp': 0,
        'pGroup': 1,
        'pParent': 0,
        'pOpen': 0,
        'pDepend': '',
        'pCaption': '',
        'pNotes': ''
      },
      {
        'pID': 21,
        'pName': 'COA Restructure / Global Design',
        'pStart': '2021-01-15',
        'pEnd': '2021-03-10',
        'pClass': 'gtaskgreen',
        'pLink': '',
        'pMile': 0,
        'pRes': 'Resource 2',
        'pComp': 0,
        'pGroup': 0,
        'pParent': 2,
        'pOpen': 0,
        'pDepend': '',
        'pCaption': '',
        'pNotes': ''
      },
      {
        'pID': 22,
        'pName': 'Design Sprint 0',
        'pStart': '2021-02-20',
        'pEnd': '2021-03-10',
        'pClass': 'gtaskgreen',
        'pLink': '',
        'pMile': 0,
        'pRes': 'Resource 3',
        'pComp': 0,
        'pGroup': 0,
        'pParent': 2,
        'pOpen': 0,
        'pDepend': '',
        'pCaption': '',
        'pNotes': ''
      },
      {
        'pID': 23,
        'pName': 'Design Sprint 1',
        'pStart': '2021-03-11',
        'pEnd': '2021-03-28',
        'pClass': 'gtaskgreen',
        'pLink': '',
        'pMile': 0,
        'pRes': 'Resource 4',
        'pComp': 0,
        'pGroup': 0,
        'pParent': 2,
        'pOpen': 0,
        'pDepend': '',
        'pCaption': '',
        'pNotes': ''
      },
      {
        'pID': 24,
        'pName': 'Design Sprint 2',
        'pStart': '2021-04-01',
        'pEnd': '2021-04-15',
        'pClass': 'gtaskgreen',
        'pLink': '',
        'pMile': 0,
        'pRes': 'Resource 5',
        'pComp': 0,
        'pGroup': 0,
        'pParent': 2,
        'pOpen': 0,
        'pDepend': '',
        'pCaption': '',
        'pNotes': ''
      },
      {
        'pID': 25,
        'pName': 'Design Sprint 3',
        'pStart': '2021-04-16',
        'pEnd': '2021-04-30',
        'pClass': 'gtaskgreen',
        'pLink': '',
        'pMile': 0,
        'pRes': 'Resource 6',
        'pComp': 0,
        'pGroup': 0,
        'pParent': 2,
        'pOpen': 0,
        'pDepend': '',
        'pCaption': '',
        'pNotes': ''
      },
      {
        'pID': 3,
        'pName': 'Build',
        'pStart': '2021-02-31',
        'pEnd': '2021-05-31',
        'pClass': 'ggroupblack',
        'pLink': '',
        'pMile': 0,
        'pRes': '',
        'pComp': 0,
        'pGroup': 1,
        'pParent': 0,
        'pOpen': 0,
        'pDepend': '',
        'pCaption': '',
        'pNotes': ''
      },
      {
        'pID': 31,
        'pName': 'Build task 1',
        'pStart': '2021-03-03',
        'pEnd': '2021-04-20',
        'pClass': 'gtaskpurple',
        'pLink': '',
        'pMile': 0,
        'pRes': 'Resource 7',
        'pComp': 0,
        'pGroup': 0,
        'pParent': 3,
        'pOpen': 0,
        'pDepend': '',
        'pCaption': '',
        'pNotes': ''
      },
      {
        'pID': 32,
        'pName': 'Build task 2',
        'pStart': '2021-04-21',
        'pEnd': '2021-05-10',
        'pClass': 'gtaskpurple',
        'pLink': '',
        'pMile': 0,
        'pRes': 'Resource 8',
        'pComp': 0,
        'pGroup': 0,
        'pParent': 3,
        'pOpen': 0,
        'pDepend': '',
        'pCaption': '',
        'pNotes': ''
      },
      {
        'pID': 33,
        'pName': 'Build task 3',
        'pStart': '2021-05-11',
        'pEnd': '2021-05-31',
        'pClass': 'gtaskpurple',
        'pLink': '',
        'pMile': 0,
        'pRes': 'Resource 9',
        'pComp': 0,
        'pGroup': 0,
        'pParent': 3,
        'pOpen': 0,
        'pDepend': '',
        'pCaption': '',
        'pNotes': ''
      },
      {
        'pID': 4,
        'pName': 'Test',
        'pStart': '2021-06-31',
        'pEnd': '2021-09-31',
        'pClass': 'ggroupblack',
        'pLink': '',
        'pMile': 0,
        'pRes': '',
        'pComp': 0,
        'pGroup': 1,
        'pParent': 0,
        'pOpen': 0,
        'pDepend': '',
        'pCaption': '',
        'pNotes': ''
      },
      {
        'pID': 44,
        'pName': 'SIT1',
        'pStart': '',
        'pEnd': '',
        'pClass': 'ggroupblack',
        'pLink': '',
        'pMile': 0,
        'pRes': '',
        'pComp': 0,
        'pGroup': 1,
        'pParent': 4,
        'pOpen': 0,
        'pDepend': '',
        'pCaption': '',
        'pNotes': ''
      },
      {
        'pID': 441,
        'pName': 'SIT 1 Instance Prep',
        'pStart': '2021-05-21',
        'pEnd': '2021-05-31',
        'pClass': 'gtaskblue',
        'pLink': '',
        'pMile': 0,
        'pRes': 'Resource 10',
        'pComp': 0,
        'pGroup': 0,
        'pParent': 44,
        'pOpen': 0,
        'pDepend': '',
        'pCaption': '',
        'pNotes': ''
      },
      {
        'pID': 442,
        'pName': 'SIT 1 Mock Conversion',
        'pStart': '2021-06-01',
        'pEnd': '2021-06-10',
        'pClass': 'gtaskblue',
        'pLink': '',
        'pMile': 0,
        'pRes': 'Resource 11',
        'pComp': 0,
        'pGroup': 0,
        'pParent': 44,
        'pOpen': 0,
        'pDepend': '',
        'pCaption': '',
        'pNotes': ''
      },
      {
        'pID': 443,
        'pName': 'SIT 1',
        'pStart': '2021-06-11',
        'pEnd': '2021-07-10',
        'pClass': 'gtaskblue',
        'pLink': '',
        'pMile': 0,
        'pRes': 'Resource 12',
        'pComp': 0,
        'pGroup': 0,
        'pParent': 44,
        'pOpen': 0,
        'pDepend': '',
        'pCaption': '',
        'pNotes': ''
      },
      {
        'pID': 444,
        'pName': 'SIT 1 Defect Fix',
        'pStart': '2021-07-11',
        'pEnd': '2021-07-20',
        'pClass': 'gtaskblue',
        'pLink': '',
        'pMile': 0,
        'pRes': 'Resource 13',
        'pComp': 0,
        'pGroup': 0,
        'pParent': 44,
        'pOpen': 0,
        'pDepend': '',
        'pCaption': '',
        'pNotes': ''
      },
      {
        'pID': 45,
        'pName': 'SIT2',
        'pStart': '',
        'pEnd': '',
        'pClass': 'ggroupblack',
        'pLink': '',
        'pMile': 0,
        'pRes': '',
        'pComp': 0,
        'pGroup': 1,
        'pParent': 4,
        'pOpen': 0,
        'pDepend': '',
        'pCaption': '',
        'pNotes': ''
      },
      {
        'pID': 451,
        'pName': 'SIT 2 Instance Prep',
        'pStart': '2021-07-11',
        'pEnd': '2021-07-20',
        'pClass': 'gtaskblue',
        'pLink': '',
        'pMile': 0,
        'pRes': 'Resource 14',
        'pComp': 0,
        'pGroup': 0,
        'pParent': 45,
        'pOpen': 0,
        'pDepend': '',
        'pCaption': '',
        'pNotes': ''
      },
      {
        'pID': 452,
        'pName': 'SIT 2 Mock Conversion',
        'pStart': '2021-07-21',
        'pEnd': '2021-07-31',
        'pClass': 'gtaskblue',
        'pLink': '',
        'pMile': 0,
        'pRes': 'Resource 15',
        'pComp': 0,
        'pGroup': 0,
        'pParent': 45,
        'pOpen': 0,
        'pDepend': '',
        'pCaption': '',
        'pNotes': ''
      },
      {
        'pID': 453,
        'pName': 'SIT 2',
        'pStart': '2021-08-01',
        'pEnd': '2021-08-31',
        'pClass': 'gtaskblue',
        'pLink': '',
        'pMile': 0,
        'pRes': 'Resource 16',
        'pComp': 0,
        'pGroup': 0,
        'pParent': 45,
        'pOpen': 0,
        'pDepend': '',
        'pCaption': '',
        'pNotes': ''
      },
      {
        'pID': 454,
        'pName': 'SIT 2 Defect Fix',
        'pStart': '2021-09-01',
        'pEnd': '2021-09-10',
        'pClass': 'gtaskblue',
        'pLink': '',
        'pMile': 0,
        'pRes': 'Resource 17',
        'pComp': 0,
        'pGroup': 0,
        'pParent': 45,
        'pOpen': 0,
        'pDepend': '',
        'pCaption': '',
        'pNotes': ''
      },
      {
        'pID': 46,
        'pName': 'UAT',
        'pStart': '',
        'pEnd': '',
        'pClass': 'ggroupblack',
        'pLink': '',
        'pMile': 0,
        'pRes': '',
        'pComp': 0,
        'pGroup': 1,
        'pParent': 4,
        'pOpen': 0,
        'pDepend': '',
        'pCaption': '',
        'pNotes': ''
      },
      {
        'pID': 461,
        'pName': 'UAT Instance Prep',
        'pStart': '2021-09-01',
        'pEnd': '2021-09-10',
        'pClass': 'gtaskblue',
        'pLink': '',
        'pMile': 0,
        'pRes': 'Resource 18',
        'pComp': 0,
        'pGroup': 0,
        'pParent': 46,
        'pOpen': 0,
        'pDepend': '',
        'pCaption': '',
        'pNotes': ''
      },
      {
        'pID': 462,
        'pName': 'UAT Mock Conversion',
        'pStart': '2021-09-11',
        'pEnd': '2021-09-20',
        'pClass': 'gtaskblue',
        'pLink': '',
        'pMile': 0,
        'pRes': 'Resource 19',
        'pComp': 0,
        'pGroup': 0,
        'pParent': 46,
        'pOpen': 0,
        'pDepend': '',
        'pCaption': '',
        'pNotes': ''
      },
      {
        'pID': 463,
        'pName': 'UAT',
        'pStart': '2021-09-21',
        'pEnd': '2021-10-20',
        'pClass': 'gtaskblue',
        'pLink': '',
        'pMile': 0,
        'pRes': 'Resource 20',
        'pComp': 0,
        'pGroup': 0,
        'pParent': 46,
        'pOpen': 0,
        'pDepend': '',
        'pCaption': '',
        'pNotes': ''
      },
      {
        'pID': 464,
        'pName': 'UAT Defect Fix',
        'pStart': '2021-10-21',
        'pEnd': '2021-10-31',
        'pClass': 'gtaskblue',
        'pLink': '',
        'pMile': 0,
        'pRes': 'Resource 21',
        'pComp': 0,
        'pGroup': 0,
        'pParent': 46,
        'pOpen': 0,
        'pDepend': '',
        'pCaption': '',
        'pNotes': ''
      },
      {
        'pID': 5,
        'pName': 'Deploy',
        'pStart': '2021-11-01',
        'pEnd': '2021-11-11',
        'pClass': 'ggroupblack',
        'pLink': '',
        'pMile': 0,
        'pRes': '',
        'pComp': 0,
        'pGroup': 1,
        'pParent': 0,
        'pOpen': 0,
        'pDepend': '',
        'pCaption': '',
        'pNotes': ''
      },
      {
        'pID': 51,
        'pName': 'Cutover',
        'pStart': '2021-11-01',
        'pEnd': '2021-11-10',
        'pClass': 'gtaskgreen',
        'pLink': '',
        'pMile': 0,
        'pRes': 'Resource 22',
        'pComp': 0,
        'pGroup': 0,
        'pParent': 5,
        'pOpen': 0,
        'pDepend': '',
        'pCaption': '',
        'pNotes': ''
      },
      {
        'pID': 52,
        'pName': 'Go Live',
        'pStart': '2021-11-11',
        'pEnd': '2021-11-11',
        'pClass': '',
        'pLink': '',
        'pMile': 1,
        'pRes': 'Resource 23',
        'pComp': 100,
        'pGroup': 0,
        'pParent': 5,
        'pOpen': 0,
        'pDepend': '',
        'pCaption': '',
        'pNotes': ''
      }
    ];
  }

}

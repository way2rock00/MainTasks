import { SharedService } from 'src/app/shared/services/shared.service';
import { Component, OnInit, Input } from '@angular/core';

@Component({
  selector: 'app-summary-filter',
  templateUrl: './summary-filter.component.html',
  styleUrls: ['./summary-filter.component.scss']
})
export class SummaryFilterComponent implements OnInit {

  @Input() filter: string;
  @Input() type: string;

  @Input() expanded: boolean;

  constructor(private sharedService: SharedService) { }

  ngOnInit() {
    this.sharedService.summaryFilterEvent.subscribe(data => {
      if (data != 'momentum') {
        if (this.expanded == true)
          this.expanded = false
        event.stopPropagation();
      }
    })
  }

  clicked() {
    this.expanded = !this.expanded;
    event.stopPropagation();
    this.sharedService.summaryFilterEvent.emit('momentum');
  }

}

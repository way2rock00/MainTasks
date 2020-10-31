import { Component, OnInit, Input } from '@angular/core';
import { DomSanitizer } from '@angular/platform-browser';


@Component({
  selector: 'app-home-description',
  templateUrl: './home-description.component.html',
  styleUrls: ['./home-description.component.scss']
})
export class HomeDescriptionComponent implements OnInit {
  @Input() view: String;
  @Input() projectName: any;
  @Input() clientName: any;
  @Input() clientLogoURL: any;
  constructor(private sanitizer: DomSanitizer) {}

  getSafeURL(logoURL) {
    return this.sanitizer.bypassSecurityTrustResourceUrl(logoURL);
      }

  ngOnInit() {
  }

}

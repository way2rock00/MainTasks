import { Component, OnInit, Input } from '@angular/core';
import { Router } from '@angular/router';

@Component({
  selector: 'app-home-carouselcard',
  templateUrl: './home-carouselcard.component.html',
  styleUrls: ['./home-carouselcard.component.scss']
})
export class HomeCarouselcardComponent implements OnInit {
  @Input() cardData: any;
  bottomBreakCount: number[];
  topBreakCount: number[];

  constructor(private router: Router) { }

  ngOnInit() {
  }

  goToPage(pageName: string) {
    this.router.navigate([`${pageName}`]);
  }

}

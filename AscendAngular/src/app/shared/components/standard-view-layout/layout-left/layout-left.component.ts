import { Component, Input, EventEmitter, Output, OnInit } from '@angular/core';
import { Router } from '@angular/router';
import { trigger, transition, style, animate } from '@angular/animations';
import { PassGlobalInfoService } from 'src/app/shared/services/pass-project-global-info.service';
import { ProjectGlobalInfoModel } from 'src/app/shared/model/project-global-info.model';

@Component({
    animations: [
        trigger(
            'enterAnimation', [
                transition(':enter', [
                    style({opacity: 0}),
                    animate('200ms', style({opacity: 1}))
                ])
            ]
        )
    ],
    selector: 'app-left-layout',
    templateUrl: './layout-left.component.html',
    styleUrls: ['./layout-left.component.scss']
})
export class LayoutLeftComponent implements OnInit {
    @Input()
    layout: string;

    @Input()
    layoutSubCat: string;

    @Input()
    layoutConfig: any;

    @Output()
    expanded: EventEmitter<boolean> = new EventEmitter();

    imageSize = "small-container";
    enlarged = false;
    rightContainerSize = "right-container-small";
    secondRightContainerSize = "second-right-container-small";
    bottomContainerSize = "bottom-container-small";

    globalProjectInfo: ProjectGlobalInfoModel;

    constructor(private router: Router, private globalDataService: PassGlobalInfoService) {}

    ngOnInit() {
        this.globalDataService.share.subscribe( data => {
            // console.log('####', data);
            this.globalProjectInfo = data;
        });
    }

    changeSize(){
        this.enlarged = !this.enlarged;

        if(!this.enlarged){
            this.imageSize= "small-container";
            this.rightContainerSize = "right-container-small";
            this.secondRightContainerSize = "second-right-container-small";
            this.bottomContainerSize = "bottom-container-small";
        } else {
            this.imageSize= "large-container";
            this.rightContainerSize = "right-container-large";
            this.secondRightContainerSize = "second-right-container-large";
            this.bottomContainerSize = "bottom-container-large";
        }

        this.expanded.emit(this.enlarged);
    }

    goToPage(pageName: string) {
        this.router.navigate([`${pageName}`]);
    }

}

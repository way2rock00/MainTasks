import { Component, Input, Output, EventEmitter } from "@angular/core";

@Component({
    selector: "app-project-stepper",
    templateUrl: "./project-stepper.component.html",
    styleUrls: ["./project-stepper.component.scss"]
})
export class ProjectStepper {
    @Input()
    pits: any[];

    @Output()
    pitClicked: EventEmitter<any> = new EventEmitter<any>();

    onPitClicked(pit: any) {
        this.pitClicked.emit(pit);
    }
}
import { Injectable } from "@angular/core";
import { HttpEvent, HttpHandler, HttpInterceptor, HttpRequest } from "@angular/common/http";
import { Observable, throwError } from "rxjs";
import { finalize, catchError } from "rxjs/operators";
import { MessagingService } from '../messaging.service';
import { BUS_MESSAGE_KEY } from '../../constants/message-bus';

@Injectable()
export class LoaderInterceptor implements HttpInterceptor {
    activeCalls: number = 0;

    constructor(private messagingService: MessagingService) { }
    intercept(req: HttpRequest<any>, next: HttpHandler): Observable<HttpEvent<any>> {
        this.messagingService.publish(BUS_MESSAGE_KEY.LOADER, true);
        ++this.activeCalls;
        return next.handle(req).pipe(
            catchError((err) => {
                return throwError(err);
            }),
            finalize(() => {
                --this.activeCalls;
                if (!this.activeCalls) {
                    this.messagingService.publish(BUS_MESSAGE_KEY.LOADER, false);
                }
            })
        );
    }
}
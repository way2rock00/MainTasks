import { Injectable } from "@angular/core";
import { BehaviorSubject, Subscription } from 'rxjs';

import { BUS_MESSAGE_KEY } from 'src/app/shared/constants/message-bus';

type subscribeCB = (data: any) => void

@Injectable(
    {
        providedIn: 'root'
    }
)
export class MessagingService {
    observableBus: {[messageKey: string]: BehaviorSubject<any>} = {};

    constructor() {
        /* -- this bus will be used for global filters -- */
        this.createBus(BUS_MESSAGE_KEY.GLOBAL_FILTER);
    }

    getBus(messageKey: BUS_MESSAGE_KEY):BehaviorSubject<any> {
        if (this.busExists(messageKey)) {
            return this.observableBus[messageKey];
        } else {
            return this.createBus(messageKey);
        }
    }

    publish(messageKey: BUS_MESSAGE_KEY, data: any) {
        if (this.busExists(messageKey)) {
            this.observableBus[messageKey].next(data);
        } else {
            this.createBus(messageKey, data).next(data);
        }
    }

    subscribe(messageKey: BUS_MESSAGE_KEY, cb: subscribeCB): Subscription {
        if (cb && typeof cb === "function") {
            const bus: BehaviorSubject<any> = this.getBus(messageKey);
            return bus.asObservable().subscribe((data: any) => {
                cb(data);
            })
        }
    }



    private busExists(messageKey: BUS_MESSAGE_KEY) {
        return !!this.observableBus[messageKey];
    }


    private createBus(messageKey: BUS_MESSAGE_KEY, data = null): BehaviorSubject<any> {
        const bus = new BehaviorSubject<any>(data);
        this.observableBus[messageKey] = bus;
        return bus;
    }
    
}
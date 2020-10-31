export class ScopeDetailsModel {

    businessProcess: string[]=[];
    businessProcessl2: string[]=[];
    userscount: number;
    region: string[]=[];
    scopeCountry: string[]=[];
    integrationPlatform: string[]=[];
    paas: string[]=[];
    scopeservice: string[]=[];

    constructor(savedData?: any) {
        if (savedData) {
            this.init(savedData);
        }
    }

    init(savedData) {
        this.businessProcess = (savedData.businessProcess || []);
        this.businessProcessl2 = (savedData.businessProcessl2 || []);
        this.userscount = savedData.userscount;
        this.region = (savedData.region || []);
        this.scopeCountry = (savedData.scopeCountry || []);
        this.integrationPlatform = (savedData.integrationPlatform || []);
        this.paas = (savedData.paas || []);
        this.scopeservice = (savedData.scopeservice || []);
    }

}
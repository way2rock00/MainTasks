export class ClientDetailsModel {

    clientName: string;
    businessDetails: string;
    revenue: string;
    industry: string[]=[];
    sector: string[]=[];
    clientid:string;

    constructor(savedData?: any) {
        if (savedData) {
            this.init(savedData)
        }
    }

    init(savedData) {
        this.clientName = savedData.clientName;
        this.businessDetails = savedData.businessDetails;
        this.revenue = savedData.revenue;
        this.industry = (savedData.industry || []);
        this.sector = (savedData.sector || []);
        this.clientid = savedData.clientid;
    }

}
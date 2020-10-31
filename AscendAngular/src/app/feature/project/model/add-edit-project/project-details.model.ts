export class ProjectDetailsModel {

    projectName: string;
    projectDescription: string;
    // country: string;
    leadPD: string;
    offering: string;
    offeringPortfolio: string;
    // projectType: string[]=[];
    chargeCode: string;
    projectManager: string; 
    makeAdmin: boolean;
    additionalAdmin: string;
    erpPackage: string[] = [];
    logoFile: File;
    documents: File;
    projectid:string;
    ppdfirstname:string;
    ppdlastname:string;
    ppdusername:string;
    ppdjobtitle:string;
    mgrfirstname:string;
    mgrlastname:string;
    mgrusername:string;
    mgrjobtitle:string;
    logoConsentFlag:boolean;
    clientName: string;
    revenue: string;
    industry: string;
    sector: string;
    clientid:string;
    scopeservice: string[]=[];
        
    constructor(savedData?: any) {
        if (savedData) {
            this.init(savedData)
        }
    }

    init(savedData) {
        this.projectName = savedData.projectName;
        this.projectDescription = savedData.projectDescription;
        this.leadPD = savedData.leadPD;
        this.offering = (savedData.offering || []);
        this.offeringPortfolio = (savedData.portfolio||[]);
        this.chargeCode = savedData.chargeCode;
        this.projectManager = savedData.projectManager;
        this.makeAdmin = savedData.makeAdmin;
        this.additionalAdmin = savedData.additionalAdmin;
        this.erpPackage = savedData.erpPackage;
        // this.country = savedData.country;
        // this.projectType = (savedData.projectType||[]);
        this.projectid = savedData.projectid;
        this.ppdfirstname = savedData.ppdfirstname;
        this.ppdlastname = savedData.ppdlastname;
        this.ppdusername = savedData.ppdusername;
        this.ppdjobtitle = savedData.ppdjobtitle;
        this.mgrfirstname = savedData.mgrfirstname;
        this.mgrlastname = savedData.mgrlastname;
        this.mgrusername = savedData.mgrusername;
        this.mgrjobtitle = savedData.mgrjobtitle;
        this.clientName = savedData.clientName;
        this.revenue = savedData.revenue;
        this.industry = (savedData.industry || '');
        this.sector = (savedData.sector || []);
        this.clientid = savedData.clientid;
        this.scopeservice = (savedData.scopeservice || '');
        
        if(savedData.logoConsentFlag == 'Y'){
            this.logoConsentFlag=true;
        }else{
            this.logoConsentFlag=false;
        }

    }
}
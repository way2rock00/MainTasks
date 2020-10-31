import { ProjectFormModel } from './project-form.model';

export class ApiProjectModel {
    action             : string;
    paas               : string[];
    leadPD             : string;
    region             : string[];
    sector             : string;
    country            : string;
    revenue            : string;
    industry           : string;
    offering           : string;
    portfolio          : string;
    chargeCode         : string;
    clientName         : string;
    erpPackage         : string[];
    userscount         : number;
    projectName        : string;
    // projectType        : string[];
    // scopeCountry       : string[];
    scopeservice       : string[];
    projectManager     : string;
    businessDetails    : string;
    businessProcess    : string[];
    businessProcessl2  : string[];
    functionalDomain   : string;
    projectManagement  : string;
    projectDescription : string;
    integrationPlatform: string[];
    makeadminflag      :string;
    projectid          :string;
    clientid          :string;
    ppdfirstname          :string;
    ppdlastname          :string;
    ppdusername          :string;
    ppdjobtitle          :string;
    mgrfirstname          :string;
    mgrlastname          :string;
    mgrusername          :string;
    mgrjobtitle          :string;    
    logoConsentFlag      :string;

    constructor(projectFormModel: ProjectFormModel) {
        /* --  Project Details -- */
        this.projectName        = projectFormModel.projectDetails.projectName;
        this.projectDescription = projectFormModel.projectDetails.projectDescription;
        // this.projectType        = projectFormModel.projectDetails.projectType;
        this.chargeCode         = projectFormModel.projectDetails.chargeCode;
        this.projectManager     = projectFormModel.projectDetails.projectManager;
        this.leadPD             = projectFormModel.projectDetails.leadPD;
        this.portfolio          = projectFormModel.projectDetails.offeringPortfolio;
        this.offering           = projectFormModel.projectDetails.offering;
        this.erpPackage         = projectFormModel.projectDetails.erpPackage;
        // this.country            = projectFormModel.projectDetails.country;
        this.makeadminflag      = 'Y';   
        this.projectid          = projectFormModel.projectDetails.projectid;
        this.ppdfirstname          = projectFormModel.projectDetails.ppdfirstname;
        this.ppdlastname          = projectFormModel.projectDetails.ppdlastname;
        this.ppdusername          = projectFormModel.projectDetails.ppdusername;
        this.ppdjobtitle          = projectFormModel.projectDetails.ppdjobtitle;
        this.mgrfirstname          = projectFormModel.projectDetails.mgrfirstname;
        this.mgrlastname          = projectFormModel.projectDetails.mgrlastname;
        this.mgrusername          = projectFormModel.projectDetails.mgrusername;
        this.mgrjobtitle          = projectFormModel.projectDetails.mgrjobtitle;
        if(projectFormModel.projectDetails.logoConsentFlag)
        this.logoConsentFlag     = "Y";
        else
        this.logoConsentFlag     = "N";
        
        this.clientName      = projectFormModel.projectDetails.clientName;        
        this.revenue         = projectFormModel.projectDetails.revenue;
        this.industry        = projectFormModel.projectDetails.industry;
        this.sector          = projectFormModel.projectDetails.sector;
        this.clientid        = projectFormModel.projectDetails.clientid;

        this.scopeservice        = projectFormModel.projectDetails.scopeservice;

        /* --  Client Details -- */
        // this.businessDetails = projectFormModel.clientDetails.businessDetails;

        /* --  Scope Details -- */
        // this.businessProcess     = projectFormModel.scopeDetails.businessProcess;
        // this.businessProcessl2   = projectFormModel.scopeDetails.businessProcessl2;
        // this.userscount          = projectFormModel.scopeDetails.userscount;
        // this.region              = projectFormModel.scopeDetails.region;
        // this.scopeCountry        = projectFormModel.scopeDetails.scopeCountry;
        // this.integrationPlatform = projectFormModel.scopeDetails.integrationPlatform;
        // this.paas                = projectFormModel.scopeDetails.paas;
        
    }
}
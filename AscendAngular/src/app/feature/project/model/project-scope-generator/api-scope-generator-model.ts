import { ScopeGeneratorFormModel } from './scope-generator-form.model';

export class ApiScopeGeneratorModel {
  action: string;
  extensions: string[];
  leadPD: string;
  sector: string;
  country: string;
  revenue: string;
  industry: string;
  primaryOffering: string;
  secondaryOffering: string[];
  primaryPortfolioOfferings: string;
  secondaryPortfolioOfferings: string[];
  primaryMemberFirm: string;
  secondaryMemberFirm: string;
  clientName: string;
  erpPackage: string[];
  projectName: string;
  projectTypeDetails: any[];
  projectManager: string;
  projectManagement: string;
  projectDescription: string;
  integrationPlatform: string[];
  projectId: string;
  clientid: string;
  clientGroup: string;
  ppdfirstname: string;
  ppdlastname: string;
  ppdusername: string;
  ppdjobtitle: string;
  mgrfirstname: string;
  mgrlastname: string;
  mgrusername: string;
  mgrjobtitle: string;
  usiEmd: string;
  usiEmdfirstname: string;
  usiEmdlastname: string;
  usiEmdusername: string;
  usiEmdjobtitle: string;
  usiGdm: string;
  usiGdmfirstname: string;
  usiGdmlastname: string;
  usiGdmusername: string;
  usiGdmjobtitle: string;
  leadQaPartner: string;
  usiQaReviewerfirstname: string;
  usiQaReviewerlastname: string;
  usiQaReviewerusername: string;
  usiQaReviewerjobtitle: string;
  usiQaReviewer: string;
  leadQaPartnerfirstname: string;
  leadQaPartnerlastname: string;
  leadQaPartnerusername: string;
  leadQaPartnerjobtitle: string;
  additionalErpPackage: string;

  constructor(projectFormModel: ScopeGeneratorFormModel) {

    this.projectName = projectFormModel.projectName;
    this.projectDescription = projectFormModel.projectDescription;
    this.projectTypeDetails = projectFormModel.projectTypeDetails;
    this.projectManager = projectFormModel.projectManager;
    this.leadPD = projectFormModel.leadPD;
    this.primaryPortfolioOfferings = projectFormModel.primaryPortfolioOfferings;
    this.secondaryPortfolioOfferings = projectFormModel.secondaryPortfolioOfferings;
    this.secondaryOffering = projectFormModel.secondaryOffering;
    this.primaryOffering = projectFormModel.primaryOffering;
    this.projectId = projectFormModel.projectId;
    this.ppdfirstname = projectFormModel.ppdfirstname;
    this.ppdlastname = projectFormModel.ppdlastname;
    this.ppdusername = projectFormModel.ppdusername;
    this.ppdjobtitle = projectFormModel.ppdjobtitle;

    this.mgrfirstname = projectFormModel.mgrfirstname;
    this.mgrlastname = projectFormModel.mgrlastname;
    this.mgrusername = projectFormModel.mgrusername;
    this.mgrjobtitle = projectFormModel.mgrjobtitle;

    this.primaryMemberFirm = projectFormModel.primaryMemberFirm;
    this.secondaryMemberFirm = projectFormModel.secondaryMemberFirm;

    this.usiEmd = projectFormModel.usiEmd;
    this.usiGdm = projectFormModel.usiGdm;
    this.usiQaReviewer = projectFormModel.usiQaReviewer;
    this.leadQaPartner = projectFormModel.leadQaPartner;

    this.usiEmdfirstname = projectFormModel.usiEmdfirstname;
    this.usiEmdlastname = projectFormModel.usiEmdlastname;
    this.usiEmdusername = projectFormModel.usiEmdusername;
    this.usiEmdjobtitle = projectFormModel.usiEmdjobtitle;

    this.usiGdmfirstname = projectFormModel.usiGdmfirstname;
    this.usiGdmlastname = projectFormModel.usiGdmlastname;
    this.usiGdmusername = projectFormModel.usiGdmusername;
    this.usiGdmjobtitle = projectFormModel.usiGdmjobtitle;

    this.usiQaReviewerfirstname = projectFormModel.usiQaReviewerfirstname;
    this.usiQaReviewerlastname = projectFormModel.usiQaReviewerlastname;
    this.usiQaReviewerusername = projectFormModel.usiQaReviewerusername;
    this.usiQaReviewerjobtitle = projectFormModel.usiQaReviewerjobtitle;

    this.leadQaPartnerfirstname = projectFormModel.leadQaPartnerfirstname;
    this.leadQaPartnerlastname = projectFormModel.leadQaPartnerlastname;
    this.leadQaPartnerusername = projectFormModel.leadQaPartnerusername;
    this.leadQaPartnerjobtitle = projectFormModel.leadQaPartnerjobtitle;

    /* --  Client Details -- */
    this.clientName = projectFormModel.clientName;
    this.revenue = projectFormModel.revenue;
    this.industry = projectFormModel.industry;
    this.sector = projectFormModel.sector;
    this.clientid = projectFormModel.clientid;
    this.clientGroup = projectFormModel.clientGroup

    /* --  Scope Details -- */
    this.additionalErpPackage = projectFormModel.additionalErpPackage;
    this.integrationPlatform = projectFormModel.integrationPlatform;
    this.extensions = projectFormModel.extensions;
    this.erpPackage = projectFormModel.erpPackage;
  }
}

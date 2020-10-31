
export class ScopeGeneratorFormModel {
  isEdit: boolean;
  projectId: string;
  clientName: string;
  revenue: string;
  industry: string;
  sector: string;
  clientid: string;
  clientGroup: string;
  projectName: string;
  projectDescription: string;
  leadPD: string;
  primaryOffering: string;
  secondaryOffering: string[] = [];
  primaryPortfolioOfferings: string;
  secondaryPortfolioOfferings: string[] = [];
  projectTypeDetails: any[] = [];
  primaryMemberFirm: string;
  secondaryMemberFirm: string;
  projectManager: string;
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

  integrationPlatform: string[] = [];
  extensions: string[] = [];
  erpPackage: string[] = [];
  additionalErpPackage: string;

  constructor(savedData?: any) {

    if (savedData) {
      //In case of Update Mode
      this.init(true, savedData.projectId, savedData);
    } else {
      //In case of Create Mode
      this.init(false, "-1");
    }

    // console.log(this);
  }

  init(isEditable, projectId: string, savedData?) {
    this.isEdit = isEditable;
    this.projectId = projectId;
    this.clientName = (savedData ? savedData.clientName : '');
    this.revenue = (savedData ? savedData.revenue : '');
    this.industry = (savedData ? savedData.industry : []);
    this.sector = (savedData ? savedData.sector : []);
    this.clientid = (savedData ? savedData.clientid : '');
    this.clientGroup = (savedData ? savedData.clientGroup : '');
    this.projectName = (savedData ? savedData.projectName : '');
    this.projectDescription = (savedData ? savedData.projectDescription : '');
    this.leadPD = (savedData ? savedData.leadPD : '');
    this.primaryOffering = (savedData ? savedData.offering : []);
    this.secondaryOffering = (savedData ? savedData.secondaryOffering : []);
    this.primaryPortfolioOfferings = (savedData ? savedData.portfolio : []);
    this.secondaryPortfolioOfferings = (savedData ? savedData.secondaryPortfolio : []);
    this.projectManager = (savedData ? savedData.projectManager : '');
    this.projectTypeDetails = (savedData ? savedData.projectTypeDetails : []);
    this.ppdfirstname = (savedData ? savedData.ppdfirstname : '');
    this.ppdlastname = (savedData ? savedData.ppdlastname : '');
    this.ppdusername = (savedData ? savedData.ppdusername : '');
    this.ppdjobtitle = (savedData ? savedData.ppdjobtitle : '');
    this.mgrfirstname = (savedData ? savedData.mgrfirstname : '');
    this.mgrusername = (savedData ? savedData.mgrusername : '');
    this.mgrlastname = (savedData ? savedData.mgrlastname : '');
    this.mgrjobtitle = (savedData ? savedData.mgrjobtitle : '');

    this.primaryMemberFirm = (savedData ? savedData.primaryMemberFirm : '');
    this.secondaryMemberFirm = (savedData ? savedData.secondaryMemberFirm : '');

    this.usiEmdfirstname = (savedData ? savedData.usiEmdfirstname : '');
    this.usiEmdlastname = (savedData ? savedData.usiEmdlastname : '');
    this.usiEmdusername = (savedData ? savedData.usiEmdusername : '');
    this.usiEmdjobtitle = (savedData ? savedData.usiEmdjobtitle : '');
    this.usiGdmfirstname = (savedData ? savedData.usiGdmfirstname : '');
    this.usiGdmlastname = (savedData ? savedData.usiGdmlastname : '');
    this.usiGdmusername = (savedData ? savedData.usiGdmusername : '');
    this.usiGdmjobtitle = (savedData ? savedData.usiGdmjobtitle : '');

    this.usiQaReviewerfirstname = (savedData ? savedData.usiQaReviewerfirstname : '');
    this.usiQaReviewerlastname = (savedData ? savedData.usiQaReviewerlastname : '');
    this.usiQaReviewerusername = (savedData ? savedData.usiQaReviewerusername : '');
    this.usiQaReviewerjobtitle = (savedData ? savedData.usiQaReviewerjobtitle : '');
    this.leadQaPartnerfirstname = (savedData ? savedData.leadQaPartnerfirstname : '');
    this.leadQaPartnerlastname = (savedData ? savedData.leadQaPartnerlastname : '');
    this.leadQaPartnerusername = (savedData ? savedData.leadQaPartnerusername : '');
    this.leadQaPartnerjobtitle = (savedData ? savedData.leadQaPartnerjobtitle : '');

    this.usiEmd = (savedData ? savedData.usiEmd : '');
    this.usiGdm = (savedData ? savedData.usiGdm : '');
    this.usiQaReviewer = (savedData ? savedData.usiQaReviewer : '');
    this.leadQaPartner = (savedData ? savedData.leadQaPartner : '');

    this.integrationPlatform = (savedData ? savedData.integrationPlatform : []);
    this.extensions = (savedData ? savedData.extensions : []);
    this.erpPackage = (savedData ? savedData.erpPackage : []);
    this.additionalErpPackage = (savedData ? savedData.additionalErpPackage : '');

  }
}

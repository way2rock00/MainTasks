import { ClientDetailsModel } from './client-details.model';
import { ProjectDetailsModel } from './project-details.model';
import { ScopeDetailsModel } from './scope-details.model';

export class ProjectFormModel {
    // clientDetails: ClientDetailsModel;
    isEdit: boolean;
    projectId: String;
    projectDetails: ProjectDetailsModel;
    // scopeDetails: ScopeDetailsModel;

    constructor(savedData?: any, projectId?: String) {
        // this.clientDetails = new ClientDetailsModel(savedData);
        this.projectDetails = new ProjectDetailsModel(savedData);
        // this.scopeDetails = new ScopeDetailsModel(savedData);

        if (savedData) {
            //In case of Update Mode
            this.init(true, projectId);
        }else{
            //In case of Create Mode
            this.init(false, "-1");
        }

        // console.log(this);
    }

    init(isEditable, projectId: String) {
        this.isEdit = isEditable;
        this.projectId = projectId;
    }
}

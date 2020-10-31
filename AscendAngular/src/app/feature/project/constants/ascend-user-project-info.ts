import { UserProjectInfo } from './ascend-project-info-type';

export class UserInfo {
    userId: String;
    isAscendAdmin: string;
    projectInfo: UserProjectInfo[];

    constructor(userId ,isAscendAdmin ,projectInfo ){
        this.userId = userId;
        this.isAscendAdmin = isAscendAdmin;
        this.projectInfo = projectInfo;

    }
}
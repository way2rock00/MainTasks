export interface TabChangeDialogData {
  tabName: string;
  sessionStorageLabel: string;
  URL: string;
  tabContents: any;
  eventData: any;
}

export enum TAB_SAVE_STATUS {
  SUCCESS = 'SUCCESS',
  FAILED = 'FAILED'
}

export enum TAB_SAVE_CONST{
  MENU = 'MENU',
  ACTIVITY_FILTER = 'ACTIVITY_FILTER',
  ACTIVITY_BACK = 'ACTIVITY_BACK',
  TAB_CHANGE = 'TAB_CHANGE',
  NAVIGATION_BAR = 'NAVIGATION_BAR',
  MEGA_MENU = 'MEGA_MENU'
}
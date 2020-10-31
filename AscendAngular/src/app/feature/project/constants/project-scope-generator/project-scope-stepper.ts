export enum SCOPE_STEPPER_API_ACTIONS {
  CREATE = 'CREATE',
  UPDATE = 'UPDATE',
  DELETE = 'DELETE'
}

export enum SCOPE_STEPPER_FORM_SEGMENT_TYPE {
  TYPE_OF_PROJECT = 'Engagement details',
  GENERAL_SCOPE = 'Scope',
  //GEOGRAPHIC_SCOPE = 'Geographical scope',
  //PROCESS_SCOPE = 'Process scope',
  IMPLEMENTATION_APPROACH = 'Implementation approach',
  PHASE_PLANNING = 'Phase planning',
  TECHNICAL_SCOPE = 'Technical scope',
  ASSUMPTIONS = 'Assumptions',
  REVIEW = 'Review'
}

export enum SCOPE_GENERATOR_API_ACTIONS {
  CREATE = 'CREATE',
  UPDATE = 'UPDATE',
  DELETE = 'DELETE'
}

export enum TYPE_OF_PROJECT_PIT_TABS {
  CLIENT_DESCRIPTION = 'Client description',
  PROJECT_DESCRIPTION = 'Project description'
}

export const TYPE_OF_PROJECT_PIT_FORM_SEGMENT = [
  {
    label: TYPE_OF_PROJECT_PIT_TABS.CLIENT_DESCRIPTION,
    active: true,
    crossed: false
  },
  {
    label: TYPE_OF_PROJECT_PIT_TABS.PROJECT_DESCRIPTION,
    active: false,
    crossed: false
  }
];

export enum SCOPE_PIT_TABS {
  SERVICE_SCOPE = 'Service',
  GEOGRAPHIC_SCOPE = 'Geography',
  PROCESS_SCOPE = 'Process',
  SYSTEM_SCOPE = 'System',
}

export const SCOPE_PIT_FORM_SEGMENT = [
  {
    label: SCOPE_PIT_TABS.SERVICE_SCOPE,
    active: true,
    crossed: false
  },
  {
    label: SCOPE_PIT_TABS.GEOGRAPHIC_SCOPE,
    active: false,
    crossed: false
  },
  {
    label: SCOPE_PIT_TABS.PROCESS_SCOPE,
    active: false,
    crossed: false
  },
  {
    label: SCOPE_PIT_TABS.SYSTEM_SCOPE,
    active: false,
    crossed: false
  }
];


export enum TECHNOLOGY_SCOPE_PIT_TABS {
  CONVERSION_SCOPE = 'Conversion',
  REPORTS_SCOPE = 'Report',
  INTERFACES_SCOPE = 'Interface',
  EXTENSION_SCOPE = 'Extension'
}

export const TECHNOLOGY_SCOPE_PIT_FORM_SEGMENT = [
  {
    label: TECHNOLOGY_SCOPE_PIT_TABS.CONVERSION_SCOPE,
    active: true,
    crossed: false
  },
  {
    label: TECHNOLOGY_SCOPE_PIT_TABS.REPORTS_SCOPE,
    active: false,
    crossed: false
  },
  {
    label: TECHNOLOGY_SCOPE_PIT_TABS.INTERFACES_SCOPE,
    active: false,
    crossed: false
  },
  {
    label: TECHNOLOGY_SCOPE_PIT_TABS.EXTENSION_SCOPE,
    active: false,
    crossed: false
  }
];

export const SYSTEM_SCOPE_LABEL = 'systemScope';

export enum IMPLEMENTATION_APPROACH_OPTIONS {
  PHASED = 'Phased',
  BIG_BANG = 'Big Bang'
}

export const SCOPE_STEPPER_FORM_SEGMENT = [
  {
    label: SCOPE_STEPPER_FORM_SEGMENT_TYPE.TYPE_OF_PROJECT,
    required: true,
    active: false,
    crossed: false,
    children: TYPE_OF_PROJECT_PIT_FORM_SEGMENT
  },
  {
    label: SCOPE_STEPPER_FORM_SEGMENT_TYPE.GENERAL_SCOPE,
    required: true,
    active: true,
    crossed: false,
    children: SCOPE_PIT_FORM_SEGMENT
  },
  {
    label: SCOPE_STEPPER_FORM_SEGMENT_TYPE.IMPLEMENTATION_APPROACH,
    active: false,
    crossed: false,
    required: true
  },
  {
    label: SCOPE_STEPPER_FORM_SEGMENT_TYPE.PHASE_PLANNING,
    active: false,
    crossed: false
  },
  {
    label: SCOPE_STEPPER_FORM_SEGMENT_TYPE.TECHNICAL_SCOPE,
    active: false,
    crossed: false,
    children: TECHNOLOGY_SCOPE_PIT_FORM_SEGMENT
  },
  {
    label: SCOPE_STEPPER_FORM_SEGMENT_TYPE.ASSUMPTIONS,
    active: false,
    crossed: false
  },
  {
    label: SCOPE_STEPPER_FORM_SEGMENT_TYPE.REVIEW,
    active: false,
    crossed: false
  }
];
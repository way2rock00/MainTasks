export enum PROJECT_API_ACTIONS {
    CREATE = 'CREATE',
    UPDATE = 'UPDATE',
    DELETE = 'DELETE'
}

export enum PROJECT_FORM_SEGMENT_TYPE {
    PROJECT_DETAILS = 'Project details',
    CLIENT_DETAILS  = 'Client details',
    SCOPE_DETAILS   = 'Scope details'
}

export const PROJECT_FORM_SEGMENT = [
    {
      label: PROJECT_FORM_SEGMENT_TYPE.PROJECT_DETAILS,
      active: true,
      crossed: false
    },
    {
      label: PROJECT_FORM_SEGMENT_TYPE.CLIENT_DETAILS,
      active: false,
      crossed: false
    },
    {
      label: PROJECT_FORM_SEGMENT_TYPE.SCOPE_DETAILS,
      active: false,
      crossed: false
    }
  ];
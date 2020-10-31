export const WSN_PROCESS_SCOPE_CONST: any = [
    { id: 1, processName: 'RTR', parent: 0, startDate: '2020-08-20', endDate: '2020-08-21' },
    { id: 2, processName: 'Manage Management Structure', parent: 1, startDate: '2020-08-20', endDate: '2020-08-21' },
    { id: 3, processName: 'Manage the General Ledger', parent: 1, startDate: '2020-08-20', endDate: '2020-08-21' },
    { id: 4, processName: 'Maintain Management Reporting Structure', parent: 2, startDate: '2020-08-20', endDate: '2020-08-21' },
    { id: 5, processName: 'Maintain Chart of Account', parent: 3, startDate: '2020-08-20', endDate: '2020-08-21' },
    { id: 6, processName: 'PTC', parent: 0, startDate: '2020-08-20', endDate: '2020-08-21' },
    { id: 7, processName: 'Project Management', parent: 6, startDate: '2020-08-20', endDate: '2020-08-21' },
    { id: 8, processName: 'Project Costing', parent: 6, startDate: '2020-08-20', endDate: '2020-08-21' },
    { id: 9, processName: 'Initiate Projects', parent: 8, startDate: '2020-08-20', endDate: '2020-08-21' },
    { id: 100, processName: 'Capture Costs - Labor', parent: 8, startDate: '2020-08-20', endDate: '2020-08-21' },
];

export enum WSN_STEPPER_FORM_SEGMENT_TYPE {
    WORKSHOP_APPROACH = 'Workshop Approach',
    PRIORITIES = 'Select Priorities',
}

export const WSN_STEPPER_FORM_SEGMENT = [
    {
        label: WSN_STEPPER_FORM_SEGMENT_TYPE.WORKSHOP_APPROACH,
        active: false,
        crossed: false
    },
    {
        label: WSN_STEPPER_FORM_SEGMENT_TYPE.PRIORITIES,
        active: false,
        crossed: false
    },
];

export enum WSN_GS_EVENT_ACTIONS {
    EDIT = 'edit',
    DELETE = 'delete',
    CLICK = 'click',
    ADD = 'add',
    DRAG = 'drag'
}

export const WSN_GS_COLORS_CONST: any = {
    red: {
        primary: '#ad2121',
        secondary: '#FAE3E3',
    },
    blue: {
        primary: '#1e90ff',
        secondary: '#D1E8FF',
    },
    yellow: {
        primary: '#e3bc08',
        secondary: '#FDF1BA',
    },
}

export const WSN_NAVIGATOR_CONST : any = [
    {
        id:1,
        label: 'Lorem ipsum dolor sit amet',
        eventDate: '16 May 2013',
        eventContent: `Lorem ipsum dolor sit amet, consectetur adipisicing elit. Odio ea necessitatibus quo velit natus
        cupiditate qui alias possimus ab praesentium nostrum quidem obcaecati nesciunt! Molestiae officiis
        voluptate excepturi rem veritatis eum aliquam qui laborum non ipsam ullam tempore reprehenderit
        illum eligendi cumque mollitia temporibus! Natus dicta qui est optio rerum.`
    },
    {
        id:2,
        label: 'Lorem ipsum dolor sit amet',
        eventDate: '15 May 2013',
        eventContent: `Lorem ipsum dolor sit amet, consectetur adipisicing elit. Odio ea necessitatibus quo velit natus
        cupiditate qui alias possimus ab praesentium nostrum quidem obcaecati nesciunt! Molestiae officiis
        voluptate excepturi rem veritatis eum aliquam qui laborum non ipsam ullam tempore reprehenderit
        illum eligendi cumque mollitia temporibus! Natus dicta qui est optio rerum.`
    },
    {
        id:3,
        label: 'Lorem ipsum dolor sit amet',
        eventDate: '14 May 2013',
        eventContent: `Lorem ipsum dolor sit amet, consectetur adipisicing elit. Odio ea necessitatibus quo velit natus
        cupiditate qui alias possimus ab praesentium nostrum quidem obcaecati nesciunt!`
    },
    {
        id:4,
        label: 'Lorem ipsum dolor sit amet',
        eventDate: '13 May 2013',
        eventContent: `Lorem ipsum dolor sit amet, consectetur adipisicing elit. Odio ea necessitatibus quo velit natus
        cupiditate qui alias possimus ab praesentium nostrum quidem obcaecati nesciunt! Molestiae officiis
        voluptate excepturi rem veritatis eum aliquam qui laborum non ipsam ullam tempore reprehenderit
        illum eligendi cumque mollitia temporibus! Natus dicta qui est optio rerum.`
    },
    {
        id:5,
        label: 'Lorem ipsum dolor sit amet',
        eventDate: '12 May 2013',
        eventContent: `Lorem ipsum dolor sit amet, consectetur adipisicing elit. Odio ea necessitatibus quo velit natus
        cupiditate qui alias possimus ab praesentium nostrum quidem obcaecati nesciunt!`
    },
    {
        id:6,
        label: 'Lorem ipsum dolor sit amet1',
        eventDate: '12 May 2013',
        eventContent: `Lorem ipsum dolor sit amet, consectetur adipisicing elit. Odio ea necessitatibus quo velit natus
        cupiditate qui alias possimus ab praesentium nostrum quidem obcaecati nesciunt!`
    },
]
export const DESIGN_LAYTOUT = {
    right: {
        title: 'Adapt leading practices',
        colorScheme: 'rgb(0,151,169,0.2)',
        textColorScheme: '#28b2c4',

        tabs: [
            {
                tabName: "Business solutions",
                tabURL: "/imagine/digital-design/business solutions",
                tabStorage: "BUSINESSSOLUTIONSJSONBKP",
                serviceURL: "/design/ownthegapv5/",
                tabCode: "BUSINESS_SOLUTIONS" // Corresponds to name field in ascend.entities table
            },
            {
                tabName: "Interfaces",
                tabURL: "/imagine/digital-design/interfaces",
                tabStorage: "INTERFACESJSONBKP",
                serviceURL: "/design/interfacesv5/",
                tabCode: "INTERFACES" // Corresponds to name field in ascend.entities table
            },
            {
                tabName: "Analytics/Reports",
                tabURL: "/imagine/digital-design/analytics & reports",
                tabStorage: "REPORTSJSONBKP",
                serviceURL: "/design/reportsv5/",
                tabCode: "ANALYTICS_REPORTS" // Corresponds to name field in ascend.entities table
            },
            {
                tabName: "Key design decisions",
                tabURL: "/imagine/digital-design/key design decisions",
                tabStorage: "KBDJSONBKP",
                serviceURL: "/design/kbdv5/",
                tabCode: "KEY_DESIGN_DECISIONS" // Corresponds to name field in ascend.entities table
                
            },
            {
                tabName: "User stories",
                tabURL: "/imagine/digital-design/user stories",
                tabStorage: "USERSTORIESJSONBKP",
                serviceURL: "/design/userstoriesv5/",
                tabCode: "USER_STORIES" // Corresponds to name field in ascend.entities table
            },
            {
                tabName: "Process flows",
                tabURL: "/imagine/digital-design/process flows",
                tabStorage: "BUSINESSPROCESSJSONBKP",
                serviceURL: "/design/businessprocessv5/",
                tabCode: "PROCESS_FLOWS" // Corresponds to name field in ascend.entities table
            },
            {
                tabName: "ERP configurations",
                tabURL: "/imagine/digital-design/erp configurations",
                tabStorage: "CONFIGURATIONJSONBKP",
                serviceURL: "/design/configurations/",
                tabCode: "ERP_CONFIGURATIONS" // Corresponds to name field in ascend.entities table
            },
            {
                tabName: "Deliverables",
                tabURL: "/imagine/refineuserstories/deliverables",
                tabStorage: "DELIVERABLESJSONBKP",
                serviceURL: "/refineuserstories/deliverables/",
                tabCode: "DELIVERABLES" // Corresponds to name field in ascend.entities table
            },
        ],
    },
    left: {
        title: 'Imagine',
        subTitle: 'Decide your personalized digital strategy',
        image: {
            normal: 'Imagine_wheel_small.svg',
            normalDimension: {
                left: '0%',
                height: '100',
                width: '100'
            },
            expandedDimension: {
                left: '3%',
                top: '45%',
                height: '100%',
                width: '100%'
            }
        },

        showLeftContent: false,

        leftBody : [
            {
                content: "Conduct design confirmation workshops (global and local)"
            },
            {
                content: "Perform fit/gap analysis and develop decision memos"
            },
            {
                content: "Finalize Key Design Decisions"
            },
            {
                content: "Consolidate updated process maps"
            },
            {
                content: "Develop future state solution strategy for extensions and RPA"
            }

        ]
    }
}

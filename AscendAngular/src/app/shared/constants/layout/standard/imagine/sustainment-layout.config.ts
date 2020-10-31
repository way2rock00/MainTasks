export const SUSTAINMENT_LAYTOUT = {
    right: {
        title: 'Refine user stories',
        colorScheme: 'rgb(0,151,169,0.2)',
        textColorScheme: '#28b2c4',
        tabs: [
            {
                tabName: "Deliverables",
                tabURL: "/imagine/refineuserstories/deliverables",
                tabStorage: "DELIVERABLESJSONBKP",
                serviceURL: "/refineuserstories/deliverables/",
                tabCode: "DELIVERABLES" // Corresponds to name field in ascend.entities table
            },
            {
                tabName: "User story library",
                tabURL: "/imagine/refineuserstories/user story library",
                tabStorage: "USERSTORYLIBRARYJSONBKP",
                serviceURL: "/refineuserstories/userstorylibrary/",
                tabCode: "USER_STORY_LIBRARY" // Corresponds to name field in ascend.entities table
            },
            {
                tabName: "Configuration workbooks",
                tabURL: "/imagine/refineuserstories/configuration workbooks",
                tabStorage: "CONFIGWORKBOOKSJSONBKP",
                serviceURL: "/refineuserstories/config/",
                tabCode: "CONFIG_WORKBOOKS" // Corresponds to name field in ascend.entities table
            },
        ]
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
        }
    }
}

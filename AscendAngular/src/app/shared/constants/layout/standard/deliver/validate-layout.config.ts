export const VALIDATE_LAYOUT = {
    right: {
        title: 'Validate',
        colorScheme: 'rgb(198, 215, 12, 0.2)',
        textColorScheme: 'rgb(198, 215, 12)',
        tabs: [
          {
            tabName: "Test scenarios and scripts",
            tabURL: "/deliver/validate/test scenarios and scripts",
            tabStorage: "TESTSCRIPTJSONBKP",
            serviceURL: "/validate/test/",
            tabCode: "TEST_SCENARIOS" // Corresponds to name field in ascend.entities table
          },
          {
            tabName: "Test automations",
            tabURL: "/deliver/validate/test automations",
            tabStorage: "TESTAUTOMATIONJSONBKP",
            serviceURL: "/validate/bots/",
            tabCode: "TEST_AUTOMATIONS" // Corresponds to name field in ascend.entities table
          }
        ],
    },
    left: {
        title: 'Deliver',
        subTitle: 'Build your capabilities and start your implementation',
        image: {
            normal: 'Deliver_wheel_small.svg',
            normalDimension: {
                height: '100%',
                width: '100%'
            },
            expandedDimension: {
                left: '2%',
                top: '37%',
                height: '37%',
                width: '37%'
            }
        },

        showLeftContent: false,

        leftBody : [
            {
                content: "Execute end to end test scenarios"
            },
            {
                content: "Perform data conversion validations and reconciliations"
            },
            {
                content: "Manage defect resolution process"
            },
            {
                content: "Apply Quarterly Release updates and perform regression testing"
            },
            {
                content: "Build/Configure reusable regression test bots"
            }
        ]

    }
};

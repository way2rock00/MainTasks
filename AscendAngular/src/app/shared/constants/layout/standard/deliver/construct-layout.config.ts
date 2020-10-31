export const CONSTRUCT_LAYOUT = {
    right: {
        title: 'Sprint',
        colorScheme: 'rgb(198, 215, 12, 0.2)',
        textColorScheme: 'rgb(198, 215, 12)',
        tabs: [
          {
            tabName: "Conversions",
            tabURL: "/deliver/construct/conversions",
            tabStorage: "CONVERSIONJSONBKP",
            serviceURL: "/construct/conversion/",
            tabCode: "CONVERSIONS" // Corresponds to name field in ascend.entities table
          },
          {
            tabName: "Development tools",
            tabURL: "/deliver/construct/development tools",
            tabStorage: "DEVTOOLSJSONBKP",
            serviceURL: "/construct/toolsv5/",
            tabCode: "DEVELOPMENT_TOOLS" // Corresponds to name field in ascend.entities table
          }
        ],
    },
    left: {
        title: 'Deliver',
        subTitle: 'Build your capabilities and start your implementation',
        image: {
            normal: 'Deliver_wheel_small.svg',
            normalDimension: {
                height: '65%',
                width: '65%'
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
                content: "Iterative Sprint Planning and Product Backlog Refinements"
            },
            {
                content: "Configure and Setup Cloud modules; Unit test setup"
            },
            {
                content: "Build FRICE components including PaaS extensions; Unit test code"
            },
            {
                content: "Schedule and conduct Sprint Review/Experience sessions"
            },
            {
                content: "Capture client feedback and observations"
            },
            {
                content: "Conduct Sprint Retrospective sessions"
            }
        ]

    }
}

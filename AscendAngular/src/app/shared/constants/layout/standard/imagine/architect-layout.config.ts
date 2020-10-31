export const ARCHITECT_LAYOUT = {
    right: {
        title: 'Apply design thinking',
        colorScheme: 'rgb(0,151,169,0.2)',
        activitiesColorScheme: 'rgba(0, 151, 169, 0.2)',
        textColorScheme: '#28b2c4',
        tabs: [
            {
                tabName: "Personas",
                tabURL: "/imagine/architect/personas",
                tabStorage: "PERSONASJSONBKP",
                serviceURL: "/architect/personas/",
                tabCode: "PERSONAS" // Corresponds to name field in ascend.entities table
            },
            {
                tabName: "Journey maps",
                tabURL: "/imagine/architect/journey maps",
                tabStorage: "JOURNEYMAPSJSONBKP",
                serviceURL: "/architect/journeymap/",
                tabCode: "JOURNEY_MAP" // Corresponds to name field in ascend.entities table
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
        headColor: '#0097a9',

        activities : [
            "Conduct Customer Experience Workshops",
            "Identify and Define Persona",
            "Document visual representation of an end-to-end journey of a persona highlighting key touch points, and visualizing moments that matter",
            "Consolidate Moments that Matter"
        ],

        deliverables:[
            "User Personas",
            "Customer Journey maps",
            "Moments that Matter"
       ],

        digitalMaturity : [
            "Client embraces shift in mindset by Design Thinking to enhance employee experience, design user-centric solutions, and power business performance"
        ],
        amplifiers:[
            {
                "name":"CxD",
                "progress":"0"
            }
        ],

        stopDescription:"Design Thinking defines the interactions between a worker / stakeholders and the organization encompassing the physical, digital and organizational work environment. The principles and techniques of design thinking are heavily emphasized in the Imagine phase",
    }
}

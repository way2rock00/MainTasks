export const DISCOVER_LAYTOUT = {
  right: {
    title: 'Discover scope',
    colorScheme: 'rgb(195, 212, 222)',
    activitiesColorScheme: 'rgb(195, 212, 222)',
    // textColorScheme: '#002fba'
    textColorScheme: 'rgba(0, 85, 135)'
  },
  left: {
    title: 'Insights',
    subTitle: 'Explore digital platform, ambitions and value it provides',
    image: {
      normal: 'Insights_wheel_small.svg',
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

    activities: [
      "Prioritize Business Process Taxonomy and Define Module Scope",
      "Review current state customizations",
      "Identify applicable Digital scope from library (RPA, IoT etc.)",
      "Define Target state solution ecosystem",
      "Recommend degree of transformation"
    ],
    deliverables: [
      "Prioritized Business Taxonomy",
      "Module Scope",
      "FRICE Scope",
      "Future state conceptual solution architecture (Cloud, integration, data, analytics and reporting"
    ],

    stopDescription: "Define scope components for Digital Maturity Journey",
    amplifiers: [
      {
        "name": "CloudCore",
        "progress": "2"
      },
      {
        "name": "Process Prioritization (ThinkTank)",
        "progress": "0"
      },
      {
        "name": "Deloitte Industry Solution Library",
        "progress": "0"
      }
    ],
    digitalMaturity: [
      "Digital Ambitions are aligned to the project scope and process taxonomy"
    ]
  }
}

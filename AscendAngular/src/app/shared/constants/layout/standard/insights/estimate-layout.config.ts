export const ESTIMATE_LAYTOUT = {
  right: {
    title: 'Estimate costs',
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
      "Run estimation using parametric estimator",
      "Develop effort and cost estimates (internal and external)",
      "Determine technology platform and Cloud subscription costs",
      "Develop target state operation costs"
    ],
    deliverables: [
      "Overall Program Costs",
      "Implementation Costs",
      "Target Operating Costs"
    ],

    stopDescription: "Utilize parametric estimation methods to determine overall program costs (client, Deloitte, 3rd party) to deliver scope and roadmap",
    amplifiers: [
      {
        "name": "Deloitte Parametric Estimation Tool",
        "progress": "2"
      }
    ],
    digitalMaturity: [
      "Client gains clarity on the overall program costs to deliver Digital Initiatives"
    ],
  }
}

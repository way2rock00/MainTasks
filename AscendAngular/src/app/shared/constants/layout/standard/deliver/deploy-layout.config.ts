export const DEPLOY_LAYOUT = {
  right: {
      title: 'Deploy',
      colorScheme: 'rgb(198, 215, 12, 0.2)',
      textColorScheme: 'rgb(198, 215, 12)',
      tabs: [
        {
          tabName: "Deploy",
          tabURL: "/deliver/deploy/deploy",
          tabStorage: "DEPLOYJSONBKP",
          serviceURL: "/deploy/misc/",
          tabCode: "DEPLOY" // Corresponds to name field in ascend.entities table
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
                content: "Rehearse and execute production cutover activities"
            },
            {
                content: "Facilitate Go-No/Go meetings"
            },
            {
                content: "Establish Stabilization approach and tools for incident management"
            },
            {
                content: "Define Optimization and Innovation roadmap (up to Go-Live+12 months)"
            }
        ]

  }
};

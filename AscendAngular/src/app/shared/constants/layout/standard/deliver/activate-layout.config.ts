export const ACTIVATE_LAYOUT = {
  right: {
    title: 'Activate advance organization',
    colorScheme: 'rgb(198, 215, 12, 0.2)',
    textColorScheme: 'rgb(198, 215, 12)',
    tabs: [
      {
        tabName: "Activate digital organization",
        tabURL: "/deliver/activate/activate digital organization",
        tabStorage: "ACTIVATEJSONBKP",
        serviceURL: "/activatedigitalorg/misc/",
        tabCode: "ACTIVATE_DIGITAL_ORGANIZATION" // Corresponds to name field in ascend.entities table
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

    leftBody: [
      {
        content: "Conduct Agile Bootcamp for Sprint and Release management"
      },
      {
        content: "Complete provisioning of Deliver phase tools such as Octane"
      },
      {
        content: "Nominate client personnel as Product Owner and Scrum Master"
      },
      {
        content: "Assess Digital Capability Maturity (as needed)"
      },
      {
        content: "Perform Sprint 1 Planning "
      },
      {
        content: "Setup meeting cadence for scrum meetings, etc."
      }
    ]

  }
};

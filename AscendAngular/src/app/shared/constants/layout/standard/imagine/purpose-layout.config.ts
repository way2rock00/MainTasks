export const PURPOSE_LAYTOUT = {
  right: {
      title: 'Define advance organization',
      colorScheme: 'rgb(0,151,169,0.2)',
      textColorScheme: '#28b2c4',
      tabs: [
        {
            tabName: "Define digital organization",
            tabURL: "/imagine/definedigitalorganization/define digital organization",
            tabStorage: "MISCJSONBKP",
            serviceURL: "/definedigitalorg/misc/",
            tabCode: "DEFINE_DIGITAL_ORGANIZATION" // Corresponds to name field in ascend.entities table
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

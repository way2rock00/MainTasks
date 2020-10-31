export const STABILIZE_LAYTOUT = {
  right: {
      title: 'Stabilize',
      colorScheme: 'rgb(134, 188, 37, 0.2)',
      textColorScheme: '#86bc25',
      tabs: [
        {
            tabName: "Stabilize",
            tabURL: "/run/stabilize/stabilize",
            tabStorage: "STABILIZEJSONBKP",
            serviceURL: "/stablize/misc/",
            tabCode: "STABILIZE" // Corresponds to name field in ascend.entities table
        },
      ]
  },
  left: {
      title: 'Run',
      subTitle: 'Become fully digital by implementing efficient and sustainable solutions',
      image: {
          normal: 'Run_wheel_small.svg',
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

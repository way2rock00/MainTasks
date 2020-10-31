export const OPTIMIZE_LAYTOUT = {
  right: {
      title: 'Optimize',
      colorScheme: 'rgb(134, 188, 37, 0.2)',
      textColorScheme: '#86bc25',
      tabs: [
        {
            tabName: "Regression testing",
            tabURL: "/run/optimize/regression testing",
            tabStorage: "REGRESSIONTESTJSONBKP",
            serviceURL: "/optimize/regressiontest/",
            tabCode: "REGRESSION_TESTING" // Corresponds to name field in ascend.entities table
        },
        {
            tabName: "ACE quarterly release insights",
            tabURL: "/run/optimize/ace quarterly release insights",
            tabStorage: "ACEJSONBKP",
            serviceURL: "/optimize/quarterlyinsights/",
            tabCode: "QUARTERLY_INSIGHTS" // Corresponds to name field in ascend.entities table
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

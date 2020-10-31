export function getL3Count(tabcontents: any) {
    let count = 0;
    tabcontents.map( L1 => {
      L1.L2grp.map(L2 => {  
        count+=L2.L3grp.length
      });
    });
    return count;
}

export function getUserStoriesCount(tabcontents: any) {
    let count = 0;
    tabcontents.map( L1 => {
      L1.journeymap.map(L2 => {  
        count+=L2.userstory.length
      });
    });
    return count;
}

export function getSolutionCount(tabcontents: any) {
    let count = 0;
    tabcontents.map( L1 => {count+=L1.solutiongrp.length});
    return count;
}

export function getdevelopToolsCount(tabcontents: any) {
    let count = 0;
    tabcontents.map( L1 => {count+=L1.toolgrp.length});
    return count;
}

export function getpersonasCount(tabcontents: any) {
  let count = 0;
  tabcontents.map( L1 => {count+=L1.L2Grp.length});
  return count;
}
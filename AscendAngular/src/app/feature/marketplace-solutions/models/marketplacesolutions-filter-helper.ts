function constructFilter(constructedFilter, filterObj, assigned) {

    if (filterObj.parent_id == null) {
        filterObj.childs = [];
        constructedFilter.childs.push(filterObj);
        /*if(constructedFilter.childs)
            constructedFilter.childs.push(filterObj);
        else{
            constructedFilter.childs=[];
            constructedFilter.childs.push(filterObj);
        }*/
        return true;
    } else if (filterObj.parent_id == constructedFilter.id) {
        filterObj.childs = [];
        constructedFilter.childs.push(filterObj);
        /*
        if(constructedFilter.childs)
            constructedFilter.childs.push(filterObj);
        else{
            constructedFilter.childs=[];
            constructedFilter.childs.push(filterObj);
        }*/
        return true;
    }
    else {
        // if

        for (let index = 0; index < constructedFilter.childs.length; index++) {
            let constructedObj = constructedFilter.childs[index];
            if (assigned == false) {
                assigned = constructFilter(constructedObj, filterObj, assigned);
            }
        }
        return false;
    }
}

export function filterConstruct(filterData) {
    // console.log("In create_payload");
    let constructedFilter = { "childs": [] };
    
    for (let index = 0; index < filterData.length; index++) {
        let filterObj = filterData[index];
        let assigned = false;
        constructFilter(constructedFilter, filterObj, assigned)
    }
    // console.log('Main:' + JSON.stringify(constructedFilter));
    return constructedFilter;
}

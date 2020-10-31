export class FilterArray {
    entityId: string;
    entityType: string;
    entityName: string;
    filterSelectType: string;
    changed?: string
    childValues: [
        {
            entityId: string;
            entityType: string;
            entityName: string;
            selectedFlag?: string;
        }
    ];
}

export class FilterContentModel {
    parentFilterId: string;
    filterLabel: string;
    filterId: string;
    advFilterApplicable?: string;
    filterValues: FilterArray[];
}

export class FilterData {
    title: string;
    readOnly?: boolean;
    l1Filter: FilterContentModel;
    l2Filter: FilterContentModel;
    l3Filter: FilterContentModel;
    l4Filter: FilterContentModel;
}

export enum FILTER_CUSTOM_CONSTANTS {
    IIDR_FILTER = 'iidr_filter',
    DELIVERABLES = 'deliverables',
    SINGLE = 'Single',
    MULTIPLE = 'Multiple',
    ADVANCED_FILTER = 'advanced_filter'
}

export function formFilterArray(data) {
    let filterComponents: FilterData[] = [];

    if (data) {
        let l1Filters = data.filter(t => (t.parentFilterId == null));

        for (let obj of l1Filters) {

            let l1Obj = obj;
            let l2Obj = formLevelObject(data, l1Obj);
            let l3Obj = formLevelObject(data, l2Obj);
            let l4Obj = formLevelObject(data, l3Obj);

            filterComponents.push({
                title: obj.filterLabel,
                l1Filter: l1Obj,
                l2Filter: l2Obj,
                l3Filter: l3Obj,
                l4Filter: l4Obj
            });
        }
    }

    return filterComponents;
}

function formLevelObject(data, parentElement) {

    let obj;

    if (parentElement) {
        obj = data.find(t => (t.parentFilterId) && t.parentFilterId == parentElement.filterId);
    }

    return obj ? obj : new FilterContentModel()
}
import { ArrayDataSource } from '@angular/cdk/collections';
import { NestedTreeControl } from '@angular/cdk/tree';
import { Component, EventEmitter, Input, OnInit, Output } from '@angular/core';
import { Subscription } from 'rxjs';
import { TAB_SAVE_CONST } from 'src/app/shared/constants/tab-change-save-dialog';
import { SharedService } from 'src/app/shared/services/shared.service';
import { ActivityNode } from '../activities/activities.component';

@Component({
  selector: 'app-activities-filter',
  templateUrl: './activities-filter.component.html',
  styleUrls: ['./activities-filter.component.scss']
})
export class ActivitiesFilterComponent implements OnInit {

  TREE_DATA: any;
  treeControl: any;
  dataSource: any;
  hasChild: any;
  @Input() filterData: any;
  @Input() filteredTree: any;
  @Input() urlParams: any;
  hierarchyDet: any;
  traversedTree: any[] = [];
  defaultObj: any[] = [];
  hierarchyId: any;
  result: any = {
    node: null,
    found: false
  }

  @Output() hierarchyDetEvent = new EventEmitter();

  subscription: Subscription;

  constructor(private sharedService: SharedService) { }

  ngOnInit() {
    this.subscription = this.sharedService.dataChangeEvent.subscribe(data => {
      if (data.type == 2 && data.source == TAB_SAVE_CONST.ACTIVITY_FILTER)
        this.getDetWrapperOnChanges(data.data)
    })
  }

  ngOnChanges() {
    this.result = {
      node: null,
      found: false
    }
    this.TREE_DATA = this.filteredTree;
    this.treeControl = new NestedTreeControl<ActivityNode>(node => node.children);
    this.dataSource = new ArrayDataSource(this.TREE_DATA);
    this.hasChild = (_: number, node: ActivityNode) => !!node.children && node.children.length > 0;
    if (this.filterData && this.filterData.length > 0 && this.filteredTree && this.filteredTree.length > 0) {
      if (this.urlParams.routedFrom.toUpperCase() == 'IIDR') {
        this.result = this.getInitialData(this.TREE_DATA[0], this.traversedTree, this.result);
        this.getDetWrapperOnChanges(this.result.node);
      } else {
        for (let i of this.TREE_DATA) {
          if (this.result.found == false) {
            this.result = this.getInitialData(i, this.traversedTree, this.result);
          }
        }
        this.getDetWrapperOnChanges(this.result.node);
      }
    } else {
      this.getDetailsForHierarchy(null);
    }
  }

  getInitialData(defaultObj, traversedTree, result) {
    traversedTree = this.constructTraversedTree(defaultObj);
    for (let i of this.filterData) {
      if (defaultObj.hierarchyId == i.parentHierarchyId && i.dataVisible.toUpperCase() == 'Y' && (this.urlParams.routedFrom.toUpperCase() == 'IIDR' || this.urlParams.activityId == i.entityId)) {
        result.found = true;
        result.node = defaultObj;
        return result;
      }
    }
    for (let j of this.filterData) {
      if (defaultObj.hierarchyId == j.parentHierarchyId && result.found == false)
        result = this.getInitialData(j, traversedTree, result);
    }
    return result;
  }

  getDetWrapperOnChanges(data) {
    for (let i of this.TREE_DATA) {
      this.collapseAll(i);
    }
    this.getDetailsForHierarchy(data);
  }

  getDetailsForHierarchy(node) {
    this.hierarchyDet = [];
    this.traversedTree = [];
    if (node) {
      this.constructTraversedTree(node);
      for (let i of this.filterData) {
        if (i.parentHierarchyId == node.hierarchyId && i.dataVisible.toUpperCase() == 'Y') {
          this.hierarchyDet.push(i);
        }
      }
    }
    this.hierarchyDetEvent.emit(this.hierarchyDet);
  }

  collapseAll(node) {
    node.expanded = false;
    for (let i of node.children) {
      this.collapseAll(i);
    }
  }

  getDetWrapper(node) {
    if (this.sharedService.toggled.toUpperCase() == 'TOGGLED') {
      let dataChangeEventObj = {
        source: TAB_SAVE_CONST.ACTIVITY_FILTER,
        data: node,
        type: 1
      }
      this.sharedService.dataChangeEvent.emit(dataChangeEventObj);
    } else {
      this.getDetWrapperOnChanges(node);
    }
  }

  getFontSize(node) {
    return node.entityType.toUpperCase() == 'FUNCTION' ? false : true;
  }

  constructTraversedTree(node) {
    for (let j of this.traversedTree) {
      if (j.level == node.level_value) {
        this.traversedTree.splice(this.traversedTree.indexOf(j), 1);
      }
    }

    this.traversedTree.push({
      name: node.entityName,
      level: node.level_value
    })

    node.expanded = true;

    for (let i of this.filterData) {
      if (node.parentHierarchyId == i.hierarchyId && i.level_value > 1)
        this.constructTraversedTree(i);
    }
    return this.traversedTree;
  }

  getSelectedState(entityName) {
    return this.traversedTree.find(t => entityName.toUpperCase() == t.name.toUpperCase());
  }

  ngOnDestroy() {
    this.subscription.unsubscribe();
  }

}

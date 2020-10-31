import { Component, OnInit, Input, Output, EventEmitter } from '@angular/core';
import { SelectionModel } from '@angular/cdk/collections';
import { FlatTreeControl } from '@angular/cdk/tree';
import { MatTreeFlatDataSource, MatTreeFlattener } from '@angular/material/tree';
import { TodoItemFlatNode, TodoItemNode } from '../../../model/project-scope-generator/scope-generator-common.model';
import { MatCheckboxChange } from '@angular/material';

@Component({
  selector: 'app-scope-generator-tree',
  templateUrl: './scope-generator-tree.component.html',
  styleUrls: ['./scope-generator-tree.component.scss']
})
export class ScopeGeneratorTreeComponent implements OnInit {

  showError: boolean = false;

  @Input() savedData: any;
  @Input() allData: any;
  @Input() stop: any;

  @Output()
  next: EventEmitter<any> = new EventEmitter<any>();

  @Output()
  prev: EventEmitter<any> = new EventEmitter<any>();    

  /** Map from flat node to nested node. This helps us finding the nested node to be modified */
  flatNodeMap = new Map<TodoItemFlatNode, TodoItemNode>();

  /** Map from nested node to flattened node. This helps us to keep the same object for selection */
  nestedNodeMap = new Map<TodoItemNode, TodoItemFlatNode>();

  /** A selected parent node to be inserted */
  selectedParent: TodoItemFlatNode | null = null;

  /** The new item's name */
  newItemName = '';

  treeControl: FlatTreeControl<TodoItemFlatNode>;

  treeFlattener: MatTreeFlattener<TodoItemNode, TodoItemFlatNode>;

  dataSource: MatTreeFlatDataSource<TodoItemNode, TodoItemFlatNode>;

  /** The selection for checklist */
  checklistSelection = new SelectionModel<TodoItemFlatNode>(true /* multiple */);

  selectedNodes: any[] = [];

  constructor() {

  }

  ngOnInit() {

    this.treeFlattener = new MatTreeFlattener(this.transformer, this.getLevel,
      this.isExpandable, this.getChildren);
    this.treeControl = new FlatTreeControl<TodoItemFlatNode>(this.getLevel, this.isExpandable);
    this.dataSource = new MatTreeFlatDataSource(this.treeControl, this.treeFlattener);
    this.dataSource.data = this.allData;

    if (this.savedData) {
      for (let element of this.savedData) {
        let expandable = !!(element.children && element.children.length > 0);
        this.addSelectedNode(element, expandable, 0)
        if (element.children && element.children.length > 0)
          for (let level1 of element.children) {
            let expandedL1 = !!(level1.children && level1.children.length > 0);
            this.addSelectedNode(level1, expandedL1, 1)

            if (level1.children && level1.children.length > 0) {
              for (let level2 of level1.children) {
                let expandedL2 = !!(level2.children && level2.children.length > 0);
                this.addSelectedNode(level2, expandedL2, 2)
              }
            }
          }
      }
    }
  }

  addSelectedNode(element, expandable, level) {
    let node = this.treeControl.dataNodes.find(t => t.item == element.item && t.expandable == expandable && t.level == level);
    this.toggle(node);
  }

  getLevel = (node: TodoItemFlatNode) => node.level;

  isExpandable = (node: TodoItemFlatNode) => node.expandable;

  getChildren = (node: TodoItemNode): TodoItemNode[] => node.children;

  hasChild = (_: number, _nodeData: TodoItemFlatNode) => _nodeData.expandable;

  hasNoContent = (_: number, _nodeData: TodoItemFlatNode) => _nodeData.item === '';

  /**
  * Transformer to convert nested node to flat node. Record the nodes in maps for later use.
  */
  transformer = (node: TodoItemNode, level: number) => {
    const existingNode = this.nestedNodeMap.get(node);
    const flatNode = existingNode && existingNode.item === node.item
      ? existingNode
      : new TodoItemFlatNode();
    flatNode.item = node.item;
    flatNode.level = level;
    flatNode.id = node.id;
    flatNode.expandable = !!node.children;
    this.flatNodeMap.set(flatNode, node);
    this.nestedNodeMap.set(node, flatNode);
    return flatNode;
  }

  /** Whether all the descendants of the node are selected */
  descendantsAllSelected(node: TodoItemFlatNode): boolean {
    const descendants = this.treeControl.getDescendants(node);
    let selected = descendants.every(child => this.checklistSelection.isSelected(child));
    if (selected && (!this.checklistSelection.isSelected(node)))
      this.checklistSelection.toggle(node);
    return selected;
  }

  /** Whether part of the descendants are selected */
  descendantsPartiallySelected(node: TodoItemFlatNode): boolean {
    const descendants = this.treeControl.getDescendants(node);
    const result = descendants.some(child => this.checklistSelection.isSelected(child));
    if (result && (!this.checklistSelection.isSelected(node)))
      this.checklistSelection.toggle(node);
    return result && !this.descendantsAllSelected(node);
  }

  /** Toggle the to-do item selection. Select/deselect all the descendants node */
  todoItemSelectionToggle(node: TodoItemFlatNode, event: MatCheckboxChange): void {

    if (event.checked)
      this.checklistSelection.isSelected(node) ? '' : this.toggle(node);
    else
      this.checklistSelection.isSelected(node) ? this.toggle(node) : '';

    const descendants = this.treeControl.getDescendants(node);
    this.checklistSelection.isSelected(node)
      ? this.checklistSelection.select(...descendants)
      : this.checklistSelection.deselect(...descendants);
  }

  toggle(node) {
    if (node) {
      this.checklistSelection.toggle(node);
    }
  }


  isValid() {
    return true;
  }

  onSubmit(){
    
  }

  onNext() {
    event.preventDefault();
    this.showError = true;
    //if (this.isValid()) {
      this.next.emit();
      this.showError = false;
    //}
  }

  onPrev() {
    this.prev.emit();
  }

}

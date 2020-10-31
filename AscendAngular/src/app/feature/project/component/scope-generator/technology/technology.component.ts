import { Component, OnInit, Output, EventEmitter, Input, ViewChild, Injectable, ViewEncapsulation } from '@angular/core';
import { NgForm, FormControl } from '@angular/forms';
import { TECHNOLOGY_SCOPE_PIT_TABS, SCOPE_PIT_TABS, SCOPE_STEPPER_FORM_SEGMENT_TYPE }
  from '../../../constants/project-scope-generator/project-scope-stepper';
import { BehaviorSubject } from 'rxjs';
import { FlatTreeControl } from '@angular/cdk/tree';
import { MatTreeFlattener, MatTreeFlatDataSource } from '@angular/material';
import { SelectionModel } from '@angular/cdk/collections';

/**
 * Node for to-do item
 */
export class nestedNode {
  children?: nestedNode[];
  item: string;
  id: number
  type: string
  system: any
  volume: number
}

/** Flat to-do item node with expandable and level information */
export class flattenedNode {
  item: string;
  id: number
  type: string
  system: any
  volume: number
  level: number;
  selected: boolean;
  expandable: boolean;
  // checked: boolean;
}

const TREE_DATA_MAIN = [{
  "item": "RTR",
  "id": 1,
  "type": null,
  "system": null,
  "volume": null,
  "children": [
    {
      "item": "General Ledger",
      "id": 100,
      "type": null,
      "system": null,
      "volume": null,
      "children": [
        {
          "item": "GL Balances",
          "type": "FBDI",
          "id": 1000,
          "system": [
            {
              "name": "JPMC"
            },
            {
              "name": "Concur"
            }
          ],
          "volume": 2000
        },
        {
          "item": "Trial Balance",
          "type": "FBDI",
          "id": 1000,
          "system": [
            {
              "name": "JPMC"
            },
            {
              "name": "Concur"
            }
          ],
          "volume": 2000
        }

      ]
    },
    {
      "item": "AP",
      "id": 100,
      "type": null,
      "system": null,
      "volume": null,
      "children": [
        {
          "item": "AP Balances",
          "type": "FBDI",
          "id": 1000,
          "system": [
            {
              "name": "JPMC"
            },
            {
              "name": "Concur"
            }
          ],
          "volume": 2000
        },
        {
          "item": "AP Trial Balance",
          "type": "FBDI",
          "id": 1000,
          "system": [
            {
              "name": "JPMC"
            },
            {
              "name": "Concur"
            }
          ],
          "volume": 2000
        }

      ]
    }
  ]
}];


/**
 * Checklist database, it can build a tree structured Json object.
 * Each node in Json object represents a to-do item or a category.
 * If a node is a category, it has children items and new items can be added under the category.
 */
@Injectable()
export class ChecklistDatabase {
  dataChange = new BehaviorSubject<nestedNode[]>([]);

  get data(): nestedNode[] { return this.dataChange.value; }

  treeData: nestedNode[] = [];

  constructor() {
    this.initialize();
  }

  initialize() {
    // Build the tree nodes from Json object. The result is a list of `nestedNode` with nested
    //     file node as children.
    //const data = this.buildFileTree(TREE_DATA, 0);
    const data = TREE_DATA_MAIN;
    // console.log('Main Data');
    // console.log(data);
    // Notify the change.
    //this.dataChange.next(data);
  }

  updateData(data) {
    console.log('Updating tree data');
    this.treeData = data;
    this.dataChange.next(data);
  }

  /**
   * Build the file structure tree. The `value` is the Json object, or a sub-tree of a Json object.
   * The return value is the list of `nestedNode`.
   */
  buildFileTree(obj: { [key: string]: any }, level: number): nestedNode[] {
    return Object.keys(obj).reduce<nestedNode[]>((accumulator, key) => {
      const value = obj[key];
      const node = new nestedNode();
      node.item = key;

      if (value != null) {
        if (typeof value === 'object') {
          node.children = this.buildFileTree(value, level + 1);
        } else {
          node.item = value;
        }
      }

      return accumulator.concat(node);
    }, []);
  }

  /** Add an item to to-do list */
  insertItem(parent: nestedNode, name: string) {
    if (parent.children) {
      parent.children.unshift({ item: name } as nestedNode);
      this.dataChange.next(this.data);
    }
  }

  updateItem(node: nestedNode, name: string) {
    node.item = name;
    this.dataChange.next(this.data);
  }

  public filter(filterText: string) {
    let filteredTreeData;
    if (filterText) {
      filteredTreeData = this.treeData.filter(d => d.item.toLocaleLowerCase().indexOf(filterText.toLocaleLowerCase()) > -1);
    } else {
      filteredTreeData = this.treeData;
    }
    // Notify the change.
    this.dataChange.next(filteredTreeData);
  }
}

@Component({
  selector: 'app-technology',
  templateUrl: './technology.component.html',
  styleUrls: ['./technology.component.scss'],
  providers: [ChecklistDatabase],
  encapsulation: ViewEncapsulation.None
})
export class TechnologyComponent implements OnInit {

  @Output()
  next: EventEmitter<any> = new EventEmitter<any>();

  @Output()
  prev: EventEmitter<any> = new EventEmitter<any>();

  @Input()
  isEditable: boolean;

  @Input()
  tabName: string;

  @Input()
  treeLevel: string;

  @Input()
  allData: any;

  @Input()
  savedData: any;

  @Input()
  inputAdditionalData: any;

  @Input()
  otherData: any;

  @ViewChild('systemForm', { static: false })
  systemForm: NgForm;

  @ViewChild('serviceForm', { static: false })
  serviceForm: NgForm;

  @ViewChild('clientForm', { static: false })
  ngForm: NgForm;

  showError: boolean = false;

  private newAttribute: any = {
    complexity: "",
    description: "",
    item: "",
    // system: [],
    // type: "",
    // volume: "",
    new: true,
    selected: true
  };

  @Output()
  onSubmitForm: EventEmitter<any> = new EventEmitter<any>();


  /** Map from flat node to nested node. This helps us finding the nested node to be modified */
  flatNodeMap = new Map<flattenedNode, nestedNode>();

  /** Map from nested node to flattened node. This helps us to keep the same object for selection */
  nestedNodeMap = new Map<nestedNode, flattenedNode>();

  /** A selected parent node to be inserted */
  selectedParent: flattenedNode | null = null;

  /** The new item's name */
  newItemName = '';

  treeControl: FlatTreeControl<flattenedNode>;

  treeFlattener: MatTreeFlattener<nestedNode, flattenedNode>;

  dataSource: MatTreeFlatDataSource<nestedNode, flattenedNode>;

  /** The selection for checklist */
  checklistSelection = new SelectionModel<flattenedNode>(true /* multiple */);

  selectedItems = ["GL Balances"];

  //Variables to control the tree components
  addNewElementAllowed: boolean;
  showVolumeColumn: boolean;
  showSystemsColumn: boolean;
  showTypeColumn: boolean;
  showComplexityColumn: boolean;
  systemData: [];

  techScopeData: any[] = [];
  newRow: any = { new: true, selected: true };
  newRowIndex = -1;

  constructor(private _database: ChecklistDatabase) {
  }

  getCheckedData(data) {
    for (let i of this.savedData) {
      if (i.name == data) {
        return true;
      } else {
        return false;
      }
    }
  }

  enableTable(children) {
    let enable = false;
    for (let i of children) {
      if (i.children && i.children.length > 0) {
        enable = true;
        break;
      }
    }
    return enable;
  }

  doTechInitialSelection(data, children, node?) {
    if (data) {
      if (!node) {
        for (let i = 0; i < data.length; i++) {
          if (children && children.length > 0) {
            for (let j of children) {
              if (j.item == data[i].item) {
                data[i].selected = true;
                for (let k of Object.keys(j)) {
                  data[i][k] = j[k]
                }
              } else {
                if (data[i].children) {
                  this.doTechInitialSelection(data[i].children, children);
                }
                // else {
                //   data[i].selected = false;
                // }
              }
            }
          } else {
            if (data[i].children) {
              this.doTechInitialSelection(data[i].children, children);
            }
            // else {
            //   data[i].selected = false;
            // }
          }
        }
      } else {
        for (let i of data) {
          if (i.item == node.item) {
            if (this.showSystemsColumn) {
              this.newAttribute.system = [];
            }
            if (this.showTypeColumn) {
              this.newAttribute.type = '';
            }
            if (this.showVolumeColumn) {
              this.newAttribute.volume = '';
            }
            i.children.push(this.newAttribute);
            this.newAttribute = {
              complexity: "",
              description: "",
              item: "",
              // system: [],
              // type: "",
              // volume: "",
              new: true,
              selected: true
            };
            if (this.showSystemsColumn) {
              this.newAttribute.system = [];
            }
            if (this.showTypeColumn) {
              this.newAttribute.type = '';
            }
            if (this.showVolumeColumn) {
              this.newAttribute.volume = '';
            }
            this.doTechInitialSelection(this.techScopeData, null)
          } else {
            this.doTechInitialSelection(i.children, null, node);
          }
        }
      }
    }
  }

  systemScopeData: any[] = [{ system: [], description: [] }];

  serviceScopeData: any[] = [];

  doServiceScopeInitialSelection(allData, savedData) {
    if (allData && savedData) {
      for (let i of allData) {
        for (let j of savedData) {
          if (j.name == i.name) {
            i.selected = true;
            for (let k of Object.keys(j)) {
              if (k != 'name' && k != 'selected') {
                i[k] = j[k]
              }
            }
          }
        }
      }
    }
  }

  doInitialSelection(savedData) {
    let childCollection = [];
    this.collectLeafLevelNodes(savedData, childCollection);
    for (let i of childCollection) {
      this.todoLeafItemSelectionToggle(i);
    }
  }

  collectLeafLevelNodes(savedData, childCollection) {
    console.log('**********Printing main collection to work on********');
    console.log(savedData);
    if (savedData) {
      if (this.tabName.toUpperCase() != 'SYSTEM') {
        for (var elementL1 of savedData) {
          if (!elementL1.children)
            childCollection.push(elementL1)
          else {
            for (var elementL2 of elementL1.children) {
              if (!elementL2.children)
                childCollection.push(elementL2)
              else {
                for (var elementL3 of elementL2.children) {
                  childCollection.push(elementL3)
                }
              }
            }
          }
        }
      } else {
        if (savedData.system) {
          for (var elementL1 of savedData.system) {
            if (!elementL1.children)
              childCollection.push(elementL1)
            else {
              for (var elementL2 of elementL1.children) {
                if (!elementL2.children)
                  childCollection.push(elementL2)
                else {
                  for (var elementL3 of elementL2.children) {
                    childCollection.push(elementL3)
                  }
                }
              }
            }
          }
        }
      }
    }
  }


  ngOnInit() {
  }

  changeView(expandElement, collapseElement) {
    expandElement._elementRef.nativeElement.classList.toggle("visible");
    collapseElement._elementRef.nativeElement.classList.toggle("visible");
  }

  ngOnChanges() {
    if (!this.tabName || this.tabName != SCOPE_PIT_TABS.SERVICE_SCOPE) {
      this.treeFlattener = new MatTreeFlattener(this.transformer, this.getLevel,
        this.isExpandable, this.getChildren);
      this.treeControl = new FlatTreeControl<flattenedNode>(this.getLevel, this.isExpandable);
      this.dataSource = new MatTreeFlatDataSource(this.treeControl, this.treeFlattener);

      this._database.dataChange.subscribe(data => {
        this.dataSource.data = data;
      });
    }

    if (this.treeLevel && this.allData) {
      if (this.treeLevel == '3' && this.allData.length > 0) {
        let techChildren = [];
        this.collectLeafLevelNodes(this.savedData, techChildren);
        this.techScopeData = JSON.parse(JSON.stringify(this.allData[0].dataTree));
        this.doTechInitialSelection(this.techScopeData, techChildren);
        for (let i of this.techScopeData) {
          let overAllCount = 0;
          if (i.children) {
            for (let j of i.children) {
              let count = 0;
              if (j.children) {
                for (let k of j.children) {
                  if (k.selected) {
                    count++
                  }
                }
              }
              if (count == j.children.length) {
                j.selected = true
                overAllCount++;
              }
            }
          }
          if (overAllCount == i.children.length) {
            i.selected = true;
          }
        }
      } else if (this.treeLevel == '2') {
        if (this.tabName.toUpperCase() == 'SERVICE') {
          for (let i of this.allData.dataTree) {
            this.serviceScopeData.push({
              name: i,
              deloitteScope: '',
              clientScope: '',
              complexity: ''
            })
          }
          this.doServiceScopeInitialSelection(this.serviceScopeData, this.savedData)
        }
      }
    }

    if (this.inputAdditionalData) {
      this.systemData = this.inputAdditionalData[0].value;
      /*for(var element of this.inputAdditionalData[0].value){
        //this.systemData.push(element.item);

      }*/
      //this.systemData.push(element.item);
    }

    if (this.allData) {
      if (this.tabName.toUpperCase() == 'SYSTEM')
        this._database.updateData(JSON.parse(JSON.stringify(this.allData.systemScope[0].systems)));
      else if (this.treeLevel == '3' && this.allData[0]) {
        this._database.updateData(JSON.parse(JSON.stringify(this.allData[0].dataTree)));
      }
      else
        this._database.updateData(JSON.parse(JSON.stringify(this.allData)));
    }

    //this.foodForm.setValue(["test2","test4"]);
    if (this.treeLevel != "3") {
      this.doInitialSelection(this.savedData);
    }

    this.initializeFlags();
  }

  onSubmit() {

  }

  toggleAll(tabRow) {
    tabRow.selected = !tabRow.selected;
    if (tabRow.children) {
      for (let i of tabRow.children) {
        i.selected = tabRow.selected;
        if (i.children) {
          for (let j of i.children) {
            j.selected = !tabRow.selected;
            this.todoLeafItemSelectionToggle(j);
          }
        }
      }
    }
  }

  initializeFlags() {
    this.setAddNewElementAllowed();
    this.setShowVolumeColumn();
    this.setShowTypeColumn();
    this.setShowSystemsColumn();
    this.setShowComplexityColumn();
  }

  setShowComplexityColumn() {
    this.showComplexityColumn = false;
    if (this.tabName == TECHNOLOGY_SCOPE_PIT_TABS.REPORTS_SCOPE ||
      this.tabName == TECHNOLOGY_SCOPE_PIT_TABS.INTERFACES_SCOPE ||
      this.tabName == TECHNOLOGY_SCOPE_PIT_TABS.CONVERSION_SCOPE ||
      this.tabName == TECHNOLOGY_SCOPE_PIT_TABS.EXTENSION_SCOPE
    ) {
      this.showComplexityColumn = true;
    }
  }

  setAddNewElementAllowed() {
    this.addNewElementAllowed = false;
    if (this.tabName == TECHNOLOGY_SCOPE_PIT_TABS.REPORTS_SCOPE ||
      this.tabName == TECHNOLOGY_SCOPE_PIT_TABS.EXTENSION_SCOPE ||
      this.tabName == TECHNOLOGY_SCOPE_PIT_TABS.INTERFACES_SCOPE ||
      this.tabName == SCOPE_STEPPER_FORM_SEGMENT_TYPE.ASSUMPTIONS
    ) {
      this.addNewElementAllowed = true;
    }
  }
  setShowVolumeColumn() {
    this.showVolumeColumn = false;
    if (this.tabName == TECHNOLOGY_SCOPE_PIT_TABS.CONVERSION_SCOPE) {
      this.showVolumeColumn = true;
    }
  }
  setShowSystemsColumn() {
    this.showSystemsColumn = false;
    if (this.tabName == TECHNOLOGY_SCOPE_PIT_TABS.CONVERSION_SCOPE ||
      this.tabName == TECHNOLOGY_SCOPE_PIT_TABS.INTERFACES_SCOPE) {
      this.showSystemsColumn = true;
    }

  }
  setShowTypeColumn() {
    this.showTypeColumn = false;
    if (this.tabName == TECHNOLOGY_SCOPE_PIT_TABS.REPORTS_SCOPE ||
      this.tabName == TECHNOLOGY_SCOPE_PIT_TABS.EXTENSION_SCOPE ||
      this.tabName == TECHNOLOGY_SCOPE_PIT_TABS.INTERFACES_SCOPE
    ) {
      this.showTypeColumn = true;
    }
  }

  getLevel = (node: flattenedNode) => node.level;

  isExpandable = (node: flattenedNode) => node.expandable;

  getChildren = (node: nestedNode): nestedNode[] => node.children;

  hasChild = (_: number, _nodeData: flattenedNode) => _nodeData.expandable;

  hasNoContent = (_: number, _nodeData: flattenedNode) => _nodeData.item === '';

  /**
   * Transformer to convert nested node to flat node. Record the nodes in maps for later use.
   */
  transformer = (node: nestedNode, level: number) => {
    // console.log('In transformer');
    const existingNode = this.nestedNodeMap.get(node);
    const flatNode = existingNode && existingNode.item === node.item
      ? existingNode
      : new flattenedNode();
    // console.log(node, 'node');
    flatNode.item = node.item;
    flatNode.level = level;
    flatNode.type = node.type;
    flatNode.volume = node.volume;
    flatNode.system = node.system;
    // console.log(flatNode, 'flatNode');
    if (node.children && node.children.length > 0)
      flatNode.expandable = true;
    else
      flatNode.expandable = false;

    //flatNode.expandable = !!node.children?.length;
    this.flatNodeMap.set(flatNode, node);
    this.nestedNodeMap.set(node, flatNode);
    return flatNode;
  }

  /** Whether all the descendants of the node are selected. */
  descendantsAllSelected(node: flattenedNode): boolean {
    const descendants = this.treeControl.getDescendants(node);
    const descAllSelected = descendants.length > 0 && descendants.every(child => {
      return this.checklistSelection.isSelected(child);
    });
    return descAllSelected;
  }

  /** Whether part of the descendants are selected */
  descendantsPartiallySelected(node: flattenedNode): boolean {
    const descendants = this.treeControl.getDescendants(node);
    const result = descendants.some(child => this.checklistSelection.isSelected(child));
    return result && !this.descendantsAllSelected(node);
  }

  /** Toggle the to-do item selection. Select/deselect all the descendants node */
  todoItemSelectionToggle(node: flattenedNode): void {
    // console.log('Printing selection:');
    // console.log(node);
    this.checklistSelection.toggle(node);
    const descendants = this.treeControl.getDescendants(node);
    this.checklistSelection.isSelected(node)
      ? this.checklistSelection.select(...descendants)
      : this.checklistSelection.deselect(...descendants);

    // Force update for the parent
    descendants.forEach(child => this.checklistSelection.isSelected(child));
    this.checkAllParentsSelection(node);
  }

  /** Toggle a leaf to-do item selection. Check all the parents to see if they changed */
  todoLeafItemSelectionToggle(node): void {

    node.selected = !node.selected;
    if (this.treeControl) {
      for (let j of this.treeControl.dataNodes) {
        if (j.item == node.item) {
          if (j.selected != null || j.selected != undefined) {
            j.selected = !j.selected;
          }

          this.checklistSelection.toggle(j);
          this.checkAllParentsSelection(j);
        }
      }
    }
  }

  /* Checks all the parents when a leaf node is selected/unselected */
  checkAllParentsSelection(node: flattenedNode): void {

    let parent: flattenedNode | null = this.getParentNode(node);
    while (parent) {
      this.checkRootNodeSelection(parent);
      parent = this.getParentNode(parent);
    }
  }

  /** Check root node checked state and change it accordingly */
  checkRootNodeSelection(node: flattenedNode): void {

    const nodeSelected = this.checklistSelection.isSelected(node);
    const descendants = this.treeControl.getDescendants(node);
    const descAllSelected = descendants.length > 0 && descendants.every(child => {
      return this.checklistSelection.isSelected(child);
    });
    if (nodeSelected && !descAllSelected) {
      this.checklistSelection.deselect(node);
    } else if (!nodeSelected && descAllSelected) {
      this.checklistSelection.select(node);
    }
  }

  /* Get the parent node of a node */
  getParentNode(node: flattenedNode): flattenedNode | null {
    const currentLevel = this.getLevel(node);

    if (currentLevel < 1) {
      return null;
    }

    const startIndex = this.treeControl.dataNodes.indexOf(node) - 1;

    for (let i = startIndex; i >= 0; i--) {
      const currentNode = this.treeControl.dataNodes[i];

      if (this.getLevel(currentNode) < currentLevel) {
        return currentNode;
      }
    }
    return null;
  }

  /** Select the category so we can insert the new item. */
  addNewItem(node: flattenedNode) {
    // console.log('Trying to add new item');
    // console.log(node);
    const parentNode = this.flatNodeMap.get(node);
    this._database.insertItem(parentNode!, '');
    this.treeControl.expand(node);
  }

  saveRow(node) {
    let element = node.children.find(t => t.item == this.newRow.module);
    if (element) {
      if ('module' in this.newRow) delete this.newRow.module;
      element.children.push(this.newRow);
    } else {
      node.children.push(this.newRow)
    }
    this.newRow = { new: true, selected: true }
    console.log(node);
    this.newRowIndex = -1;
  }

  clearRow() {
    this.newRowIndex = -1;
    this.newRow = { new: true, selected: true };
  }

  addNewRow(node) {
    this.doTechInitialSelection(this.techScopeData, null, node)
  }

  removeRow(node, index) {
    node.children.splice(index, 1);
  }

  /** Save the node to database */
  saveNode(node: flattenedNode, itemValue: string) {

    // node.checked = true;

    const nestedNode = this.flatNodeMap.get(node);

    this._database.updateItem(nestedNode!, itemValue);
    // this.todoLeafItemSelectionToggle(node)
  }

  getUpdatedData(data) {
    //console.log(allData);
    //console.log(this._database.data);
    //var copyAllData = JSON.parse(JSON.stringify(allData));
    if (data) {
      var copyAllData = JSON.parse(JSON.stringify(data));
    }
    //console.log(copyAllData);
    var updatedData = [];
    if (copyAllData)
      for (var elementL1 of copyAllData) {
        if (!elementL1.children) {
          if ((this.checklistSelection.selected.find(t => t.item == elementL1.item) && this.tabName.toUpperCase() != "SERVICE") || elementL1.selected) {
            updatedData.push(elementL1);
            if ('selected' in elementL1) {
              delete elementL1.selected;
            }
          }
        }
        else {
          var l1Obj = elementL1;
          let tempL2Collection = [];

          for (var elementL2 of elementL1.children) {

            if (!elementL2.children) {
              if ((this.checklistSelection.selected.find(t => t.item == elementL2.item)) || elementL2.selected) {
                tempL2Collection.push(elementL2);
                if ('selected' in elementL2) {
                  delete elementL2.selected;
                }
                // if ('new' in elementL2) {
                //   delete elementL2.new;
                // }
              }
            }
            else {
              var l2Obj = elementL2;
              let tempL3Collection = [];
              for (var elementL3 of elementL2.children) {
                if ((this.checklistSelection.selected.find(t => t.item == elementL3.item) && this.treeLevel != '3') || elementL3.selected) {
                  tempL3Collection.push(elementL3);
                  if ('selected' in elementL3) {
                    delete elementL3.selected;
                  }
                  // if ('new' in elementL3) {
                  //   delete elementL3.new;
                  // }
                }
                if ('selected' in elementL3) {
                  delete elementL3.selected;
                }
              }
              if (tempL3Collection.length > 0) {
                l2Obj.children = tempL3Collection;
                tempL2Collection.push(l2Obj);
              }
            }
            if ('selected' in elementL2) {
              delete elementL2.selected;
            }
          }

          //obj.children = [];
          if (tempL2Collection.length > 0) {
            l1Obj.children = tempL2Collection;
            updatedData.push(l1Obj);
          }
        }
        if ('selected' in elementL1) {
          delete elementL1.selected;
        }
      }
    //console.log('Updated Data:******************');
    //console.log(updatedData);
    return updatedData;
  }

  onNext(clickedSegment?:any) {
    this.showError = true;
    if (this.isValid()) {
      //this.next.emit(this.formData);
      this.showError = false;
      let updatedData;
      if (this.treeLevel == '2') {
        if (this.tabName.toUpperCase() != 'SERVICE') {
          if (this.tabName.toUpperCase() == 'SYSTEM') {
            updatedData = this.getUpdatedData(this.allData.systemScope[0].systems);
          } else {
            updatedData = this.getUpdatedData(this.dataSource.data);
          }
        }
        else
          updatedData = this.getUpdatedData(this.serviceScopeData);
      }
      else {
        updatedData = this.getUpdatedData(this.techScopeData);
      }

      if (this.tabName.toUpperCase() == 'SYSTEM') {
        let systemUpdatedData = [];
        systemUpdatedData = this.savedData;
        systemUpdatedData['system'] = updatedData;
        updatedData = [];
        updatedData.push(systemUpdatedData);
      } else if (this.tabName.toUpperCase() == 'ASSUMPTIONS') {
        let assumptionUpdatedData = { assumption: [] };
        assumptionUpdatedData.assumption = updatedData;
        updatedData = [];
        updatedData.push(assumptionUpdatedData);
      }
      if(clickedSegment)
        this.next.emit({data:updatedData, clickedSegment: clickedSegment});
      else
        this.next.emit(updatedData);
    } else {
      //We will put code here to show the error message.
      this.showError = true;
      const firstElementWithError = document.querySelector('.scope-error,.ng-invalid');
      if (firstElementWithError)
        firstElementWithError.scrollIntoView({ behavior: 'smooth' });
    }
  }

  selectService(data) {
    data.selected = false
    for (let i of Object.keys(data)) {
      if (data[i] != '') {
        data.selected = true;
      }
    }
    // if (this.treeLevel == '3' && data.selected) {
    //   data.selected = false;
    //   this.todoLeafItemSelectionToggle(data);
    // }
  }

  isValid() {
    return this.tabName.toUpperCase() == 'SERVICE' ? this.serviceScopeData.find(t => t.selected) : this.tabName.toUpperCase() != 'ASSUMPTIONS' ? (this.tabName.toUpperCase() == 'SYSTEM' ? this.systemForm.valid && this.checklistSelection.selected.length > 0 : true) : true;
  }

  onPrev() {
    this.prev.emit();
  }

  toggle(data) {
    data.selected = !data.selected;
  }

  filterChanged(filterText: string) {
    this._database.filter(filterText);
    if (filterText) {
      this.treeControl.expandAll();
    } else {
      this.treeControl.collapseAll();
    }
  }

}

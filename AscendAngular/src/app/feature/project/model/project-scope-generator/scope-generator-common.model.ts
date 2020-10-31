export class TodoItemNode {
    item: string;
    children?: TodoItemNode[];
    id: any;
  }

  /** Flat to-do item node with expandable and level information */
  export class TodoItemFlatNode {
    item: string;
    level: number;
    expandable: boolean;
    id: any;
  }

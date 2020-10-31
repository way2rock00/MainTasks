import { Pipe,PipeTransform } from '@angular/core';
@Pipe({
name: 'searchIndustry'
})

export class SearchIndustryPipe implements PipeTransform {
    transform(items: any[], searchText: string ): any[]{
        //// console.log("items:");
        //// console.log( items);
        //// console.log("searchText=" + searchText);
        if(!items) return [];
        if(!searchText ) return items;

        searchText = searchText.toLowerCase();
        // console.log(items);

        return items.filter(it=> {
            return it.L0.toLowerCase().includes(searchText);
        });
    }
}

import { Pipe, PipeTransform } from '@angular/core';
import { DomSanitizer } from '@angular/platform-browser';

@Pipe({
  name: 'highlightText'
})
export class HighlightTextPipe implements PipeTransform {

  constructor(private sanitized: DomSanitizer) {}

  transform(text: string, search: string, ctrlValue: string): any {
    const pattern = search
      .replace(/[\-\[\]\/\{\}\(\)\*\+\?\.\\\^\$\|]/g, "\\$&")
      .split(' ')
      .filter(t => t.length > 0)
      .join('|');
    const regex = new RegExp(pattern, 'gi');
    let value = (search && ctrlValue) ? text.replace(regex, match => `<span style='font-weight:700'>${match}</span>`) : text
    return this.sanitized.bypassSecurityTrustHtml(value);
  }

}

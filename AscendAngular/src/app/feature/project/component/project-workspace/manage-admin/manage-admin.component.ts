import { Component, Inject , ViewChild , OnInit } from '@angular/core';
import { MatDialogRef, MAT_DIALOG_DATA} from '@angular/material/dialog';
import { MatTable, MatTableDataSource} from '@angular/material/table';

import { ManageAdminService } from '../../../service/manageAdmin.service';
import { MatSort } from '@angular/material';
import { NgForm, FormControl, Validators } from '@angular/forms';
import { debounceTime, tap, switchMap, finalize, startWith, map } from 'rxjs/operators';
import { Observable } from 'rxjs';
import { HttpServiceHelper } from '../../../../../types/common/HttpServiceHelper'
import { HttpParams } from '@angular/common/http';
import { User } from '../../../constants/ascend-user-info';


@Component({
  selector: 'app-manage-admin',
  templateUrl: './manage-admin.component.html',
  styleUrls: ['./manage-admin.component.scss']
})
export class ManageAdminComponent implements OnInit  {

    existingAdminError = '';

    currentAdminList: MatTableDataSource<any>;
    displayedColumns: string[] = ['name', 'userId', 'action'];
    @ViewChild(MatSort, {static: false}) sort: MatSort;

    adminEmail = new FormControl('',[Validators.required, Validators.pattern('[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,4}$')]);
    isLoading = false;
    errorMsg: string;
    url = 'https://graph.microsoft.com/v1.0/users';
    filteredUser: any=[];
    showDeleteConfirmation : boolean;
    showAddConfirmation : boolean;
    constructor (
        private dialogRef: MatDialogRef<ManageAdminComponent>,
        @Inject(MAT_DIALOG_DATA) public data: any,
        private manageAdminService: ManageAdminService
        ,private httpService: HttpServiceHelper
    ) {}

    //Initialize the table.
    ngOnInit() {
        this.showAddConfirmation = false;
        this.showDeleteConfirmation = false;
        this.getAdmin();

        this.adminEmail.valueChanges
        .pipe(
          debounceTime(500),
          tap(() => {
            this.errorMsg = '';
            // this.filteredUser = [];
            this.isLoading = true;
          }),
          // tslint:disable-next-line: max-line-length
          switchMap(value => {
            if (value !== '') {
              //console.log('inside if');
              return this.httpService.httpGetRequestWithParams(this.url, new HttpParams().set('$filter', "((startswith(displayName,'" + value + "') or startswith(mail,'" + value + "')) and userType eq 'Member')"))
                .pipe(
                  finalize(() => {
                    this.isLoading = false;
                  }),
                )
            } else {
              //console.log('inside else');
              // if no value is present, return null
              return Observable.of(false);
            }
          }
          )
        ) 
        .subscribe((datas: any) => {
            console.log('inside subscribe');
            console.log(datas)
            //const usersList: User[] = [];
            this.filteredUser = [];
            if (datas) {
              let obj = datas;
              // tslint:disable-next-line: prefer-for-of
              for (let index = 0; index < obj.value.length; index++) {
                const user: User = new User();
                console.log('*****TEST******');
                console.log(obj.value[index]);
                let email = obj.value[index].mail;
                console.log(' Email:'+email);
                if(email && (email.includes('deloitte.') || email.includes('DELOITTE.')) ){
                    user.userId = obj.value[index].mail;
                    user.userName = obj.value[index].displayName;
                    user.ssoUser.displayName = obj.value[index].displayName;
                    user.ssoUser.givenName = obj.value[index].givenName;
                    user.ssoUser.surname = obj.value[index].surname;
                    user.ssoUser.jobTitle = obj.value[index].jobTitle;
                    console.log('UserId'+user.userId+'UserName:'+user.userName);
                    this.filteredUser.push(user);
                }
              }
            }else{
            }
             
          }); 
    }

    getAdmin() {
        this.manageAdminService.getAdmins()
        .subscribe(data => {
            this.currentAdminList = new MatTableDataSource(data);
            setTimeout( () => {
                // console.log('SORT', this.sort);
                this.currentAdminList.sort = this.sort
            });

        });
    }

    //
    filterUser(value) {
        this.currentAdminList.filter = value.trim().toLowerCase();
    }

    addAdmin(form: NgForm) {
        if (this.isNewAdminValid(form)) {
            
            console.log('***********TEST****************');
            console.log(this.filteredUser);
            let postData = {};
            postData['action'] = 'CREATE';
            postData['data'] = this.filteredUser[0];
            console.log(JSON.stringify(postData));
            this.manageAdminService.createDeleteAdmin(postData)
            .subscribe((data: any) => {
                if (data.MSG === 'SUCCESS') {
                    form.resetForm();
                    this.getAdmin();
                    //confirmation code
                    this.showDeleteConfirmation = false;
                    this.showAddConfirmation = true;

                } else {
                    this.existingAdminError = 'Error adding user';
                }
            });
        }
    }

    isNewAdminValid(form: NgForm) {
        console.log('In isNewAdminValid');
        console.log(form.control)
        console.log(this.currentAdminList)
        console.log(this.adminEmail.invalid);
        console.log(this.adminEmail.value);
        console.log(this.adminEmail.errors);
        

        this.existingAdminError = '';
        /* 
        Changed as per new implementation logic.
        if (!form.valid) {
            const errors = form.control.controls['email'].errors;
            this.existingAdminError = errors.required ? 'Admin email is required' : 'Invalid email id'
        } else {
            for (let admin of this.currentAdminList.data) {
                if (admin.userId.toLowerCase() === form.value.email.toLowerCase()) {
                    this.existingAdminError = "User is already an email."
                    break;
                }
            }
        }*/
        if (this.adminEmail.invalid) {
          const errors = this.adminEmail.errors;
          this.existingAdminError = errors.required ? 'Admin email is required' : 'Invalid email id'
      } else {
          for (let admin of this.currentAdminList.data) {
              if (admin.userId.toLowerCase() === this.adminEmail.value.toLowerCase()) {
                  this.existingAdminError = "User is already an email."
                  break;
              }
          }
      }
        return !this.existingAdminError;
    }

    deleteAdmin(emailId: any) {

        console.log('emailId:'+emailId+'')
        let postData = {};
        let data = {};
        data['userId'] = emailId;
        postData['action'] = 'DELETE';
        postData['data'] = data;
        console.log(JSON.stringify(postData));

        this.manageAdminService.createDeleteAdmin(postData)
        .subscribe((data: any) => {
            if (data.MSG === 'SUCCESS') {
                this.getAdmin();
                this.showAddConfirmation = false;
                this.showDeleteConfirmation = true;
            } else {
                this.existingAdminError = 'Error adding user';
            }
        });
    }

    cancel() {
      this.dialogRef.close();
    }
}

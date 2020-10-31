import { Injectable } from '@angular/core';
import * as CryptoJS from 'crypto-js';
import * as SecureStorage from 'secure-web-storage';

@Injectable({
  providedIn: 'root'
})
export class CryptUtilService {

  public lsSecureStorage = new SecureStorage(localStorage, {
    hash(key) {
      key = CryptoJS.SHA256(key, 'Secure salt that keeps the info safe');

      return key.toString();
    },
    encrypt(data) {
      data = CryptoJS.AES.encrypt(data, 'Secure salt that keeps the info safe');

      data = data.toString();

      return data;
    },
    decrypt(data) {
      data = CryptoJS.AES.decrypt(data, 'Secure salt that keeps the info safe');

      data = data.toString(CryptoJS.enc.Utf8);

      return data;
    }
  });

  public ssSecureStorage = new SecureStorage(sessionStorage, {
    hash(key) {
      key = CryptoJS.SHA256(key, 'Secure salt that keeps the info safe');

      return key.toString();
    },
    encrypt(data) {
      data = CryptoJS.AES.encrypt(data, 'Secure salt that keeps the info safe');

      data = data.toString();

      return data;
    },
    decrypt(data) {
      data = CryptoJS.AES.decrypt(data, 'Secure salt that keeps the info safe');

      data = data.toString(CryptoJS.enc.Utf8);

      return data;
    }
  });

  sessionIds: string[] = this.getItem('sessionIds', 'SESSION') || [];

  constructor() { }

  setItem(key, value, storageOption) {
    this.removeItem(key, storageOption);
    if (storageOption == 'LOCAL') {
      this.lsSecureStorage.setItem(key, value);
    } else {
      if (this.sessionIds.find(t => key == t) == undefined) {
        this.sessionIds.push(key);
        this.ssSecureStorage.setItem('sessionIds', this.sessionIds);
      }
      this.ssSecureStorage.setItem(key, value);
    }
  }

  getItem(key, storageOption) {
    if (storageOption == 'LOCAL') {
      return this.lsSecureStorage.getItem(key);
    } else {
      return this.ssSecureStorage.getItem(key);
    }
  }

  removeItem(item, storageOption) {
    if (storageOption == 'LOCAL') {
      this.lsSecureStorage.removeItem(item);
    } else {
      this.ssSecureStorage.removeItem(item);
    }
  }

  sessionClear() {
    if (this.sessionIds.length > 0) {
      for (let i of this.sessionIds) {
        this.ssSecureStorage.removeItem(i);
      }
      this.removeItem('sessionIds', 'SESSION');
    }
  }

  // makeid(length) {
  //   let result = '';
  //   let characters = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
  //   let charactersLength = characters.length;
  //   for (let i = 0; i < length; i++) {
  //     result += characters.charAt(Math.floor(Math.random() * charactersLength));
  //   }
  //   return result;
  // }
}

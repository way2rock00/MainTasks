var connection = require('./connection-file');
//const request = require('request');
var request = require('requestretry');
var sql = require("mssql");
var nodemailer = require('nodemailer');
var projectService = require('./project-service');
//let accessTokenVal = 'Bearer eyJ0eXAiOiJKV1QiLCJub25jZSI6InM1V2RSYjVxTTNDdG12R2JuUHFWOXNJaVloNnVfc01LeTM3d2ZIdWZkNUkiLCJhbGciOiJSUzI1NiIsIng1dCI6IllNRUxIVDBndmIwbXhvU0RvWWZvbWpxZmpZVSIsImtpZCI6IllNRUxIVDBndmIwbXhvU0RvWWZvbWpxZmpZVSJ9.eyJhdWQiOiIwMDAwMDAwMy0wMDAwLTAwMDAtYzAwMC0wMDAwMDAwMDAwMDAiLCJpc3MiOiJodHRwczovL3N0cy53aW5kb3dzLm5ldC8zNmRhNDVmMS1kZDJjLTRkMWYtYWYxMy01YWJlNDZiOTk5MjEvIiwiaWF0IjoxNTg1Njg1MDA1LCJuYmYiOjE1ODU2ODUwMDUsImV4cCI6MTU4NTY4ODYxNSwiYWNjdCI6MCwiYWNyIjoiMCIsImFpbyI6IkFWUUFxLzhQQUFBQVV5UGM2R2hhT2tKMzBvejg4cXA2bUN6VXFVMXdtNnp0Zmg4Vk9wU05JR1R5NGNucE9Fd3RSWklDQmhndVRBdnJ0aWhkbHpScTZNV1JERkZLcnliUzlsZGVYZlluY2VKcFltZlMxVk9USEVJPSIsImFtciI6WyJ3aWEiLCJtZmEiXSwiYXBwX2Rpc3BsYXluYW1lIjoiVVMgT3JhY2xlIEFzY2VuZCBOT05QUk9EIiwiYXBwaWQiOiJmMDZhZjY1Mi1mNjM1LTQzZDEtYTVmNy1iM2M3ZmUzYjY3NzEiLCJhcHBpZGFjciI6IjEiLCJjb250cm9scyI6WyJhcHBfcmVzIl0sImNvbnRyb2xzX2F1ZHMiOlsiMDAwMDAwMDMtMDAwMC0wMDAwLWMwMDAtMDAwMDAwMDAwMDAwIiwiMDAwMDAwMDMtMDAwMC0wZmYxLWNlMDAtMDAwMDAwMDAwMDAwIl0sImZhbWlseV9uYW1lIjoiTWlzaHJhIiwiZ2l2ZW5fbmFtZSI6IkhpbWFuc2h1IiwiaXBhZGRyIjoiNDkuMzIuNC4yMzYiLCJuYW1lIjoiTWlzaHJhLCBIaW1hbnNodSIsIm9pZCI6IjMwODRiZDc2LWQ4NmItNGE3ZS05NjA3LTI2MGY2NDE2ODFmOCIsIm9ucHJlbV9zaWQiOiJTLTEtNS0yMS0yMzg0NDcyNzYtMTA0MDg2MTkyMy0xODUwOTUyNzg4LTE5MDcyOTciLCJwbGF0ZiI6IjMiLCJwdWlkIjoiMTAwMzAwMDA5RDU4QzNFRiIsInNjcCI6Ikdyb3VwLlJlYWRXcml0ZS5BbGwgU2l0ZXMuUmVhZFdyaXRlLkFsbCBVc2VyLlJlYWQgVXNlci5SZWFkLkFsbCBVc2VyLlJlYWRCYXNpYy5BbGwgcHJvZmlsZSBvcGVuaWQgZW1haWwiLCJzdWIiOiJYWm9pcXhwY1Zua2R1ZEFyYlNudlhEYjlwVUVsNFg1aGpEajRiRjdXX25ZIiwidGlkIjoiMzZkYTQ1ZjEtZGQyYy00ZDFmLWFmMTMtNWFiZTQ2Yjk5OTIxIiwidW5pcXVlX25hbWUiOiJoaW1taXNocmFAZGVsb2l0dGUuY29tIiwidXBuIjoiaGltbWlzaHJhQGRlbG9pdHRlLmNvbSIsInV0aSI6ImM4TmkwdFpza2s2NkdlbzRBX0lLQUEiLCJ2ZXIiOiIxLjAiLCJ4bXNfc3QiOnsic3ViIjoiZmVmVTlxa1gxLUFsVWtseU5JRW9zRTV3c0hYQmdSd2tZbWRIVGE1bElnZyJ9LCJ4bXNfdGNkdCI6MTQwNTY5NDM4OH0.IbxoLQGBs7Ft-2pdyUZtIvXTQxc6YlyQ5U4zLfaQVr6SZI9gtQMG2_VxiPuBmkd5uDOPD-w6CAj-jQdWmOPby2IJ85P-I1ta32WIzQ7Xm9LY6urb7BUrptv_Jxa0kglg9WiS6X8O27EAAOxgYmDKoabMKvMADyRwy_sEIakFT6apiuJt3OQ67-mCGTHeNXsOd69eTkO8kE9w-1BaOtwd_dWXFIZ-zHSSz7bpGGiKVnGnsWgpe6gdc17XQd0al6i87T8OKoBZo5SUboTOs8BYoHeA4FqUFlcLvlexyke6MlLc5CnIfPyERpvJm2z-1AoQJCvztg7e3Lp2GjJXZ-jQTQ';

function sleep(ms) {
  //   console.log("Sleeping :p");

}


copyFileOrFolderStructure = function (accessTokenVal, srcGrpId, srcItemId, destDrvLoc, destitemId, item) {

  return new Promise((resolve, reject) => {
  var outString = '';
    var copyOptions = {
      'method': 'POST',
      'url': 'https://graph.microsoft.com/v1.0' + '/groups/' + srcGrpId + '/drive/items/' + srcItemId + '/copy',
      'forever': true,
      'timeout': connection.getconnection().timeoutValue,
      'maxAttempts': connection.getconnection().maxAttemptsValue,  // (default) try 5 times
      'retryDelay': connection.getconnection().retryDelayValue, // (default) wait for 5s before trying again
      'retrySrategy': 'request.RetryStrategies.HTTPOrNetworkError',
      'headers': {
        'Authorization': 'Bearer ' + accessTokenVal,
        'Content-Type': 'application/json'
      },
      'body': '{"parentReference":{ "driveId": "' + destDrvLoc + '", "id": "' + destitemId + '"}}'
    };
  //console.log(item+':item:'+'');
    request(copyOptions, function (errorC, responseC) {
      if (errorC) throw new Error(errorC);
  //if (responseC.statusMessage === 'Accepted')
  if(responseC){
    //if (responseC.statusCode != 202){
    //console.log(item+':item:'+JSON.stringify(responseC));
    //console.log('outString in message:'+JSON.stringify(responseC.statusCode)+':::'+JSON.stringify(JSON.parse(responseC.body).message));
    outString = item+'::';
    //console.log('outString in item:'+outString);
    outString = outString+JSON.stringify(responseC.statusCode);
    //console.log('outString in statusCode:'+outString);
    //var result = JSON.parse(responseC.body);
    //console.log('outString in result:'+result)
    //outString+='::'+JSON.stringify(result.message);
    //console.log('outString in copy:'+outString);

    //}



    if(outString.includes("429")) {
        console.log('429 detected. Retrying after');
        setTimeout(function() {
          copyFileOrFolderStructure(accessTokenVal, srcGrpId, srcItemId, destDrvLoc, destitemId, item)
        }, 10000);

      }


    if(outString.includes("202"))
    {
      console.log('outString in 202:'+outString);
      resolve();
    }
    else if(outString.includes("429"))
    {
      console.log('outString in 429:'+outString);
      resolve();
    } if(outString.includes("504"))
    {
      console.log('outString in 504:'+outString);
      resolve();
    }
    else
    {
    console.log('outString in output:'+outString);
    resolve(outString);
    }

  }

    });

  });
};

function getFileParameters(accessTokenVal, item, srcGrpId, destDriveId, destGroupId) {

  //let localItem = item.replace('Content', 'Content/Core Content');
  //console.log(localItem);
  let localItem = item;

  return new Promise((resolve, reject) => {

    let options = {
      'method': 'GET',
      'url': 'https://graph.microsoft.com/v1.0/groups/' + srcGrpId + '/drive/root:' + encodeURIComponent(localItem),
      'forever': true,
      'timeout': connection.getconnection().timeoutValue,
      'maxAttempts': connection.getconnection().maxAttemptsValue,  // (default) try 5 times
      'retryDelay': connection.getconnection().retryDelayValue, // (default) wait for 5s before trying again
      'retrySrategy': 'request.RetryStrategies.HTTPOrNetworkError',
      'headers': {
        'Authorization': 'Bearer ' + accessTokenVal,
        'Content-Type': 'application/json'
      }
    };

  //   setTimeout( () => {}, 5000 );
    request(options, function (error, response) {
      // if (error) throw new Error(error);
      if (error)
        {
        console.error(`Could not send request to API: ${error.message}`);
        resolve (error.message);
        } else {
      let respJson = response.body;
      let srcItemId = JSON.parse(respJson).id;

      let destItemNm = localItem.replace('Content/Core Content', 'General/Core Content');
      destItemNm = destItemNm.substring(0, destItemNm.lastIndexOf("/") + 1)

      var destOptions = {
        'method': 'GET',
        'url': 'https://graph.microsoft.com/v1.0/groups/' + destGroupId + '/drive/root:' + encodeURIComponent(destItemNm),
        'forever': true,
        'timeout': connection.getconnection().timeoutValue,
        'maxAttempts': connection.getconnection().maxAttemptsValue,  // (default) try 5 times
        'retryDelay': connection.getconnection().retryDelayValue, // (default) wait for 5s before trying again
        'retrySrategy': 'request.RetryStrategies.HTTPOrNetworkError',
        'headers': {
          'Authorization': 'Bearer ' + accessTokenVal,
          'Content-Type': 'application/json'
        }
      };
      //console.log('destOptions  '+JSON.stringify(destOptions));
      request(destOptions, function (error1, response1) {
        if (error) throw new Error(error1);
        let respJson1 = response1.body;
        let destItemId = JSON.parse(respJson1).id;


        resolve(copyFileOrFolderStructure(accessTokenVal, srcGrpId, srcItemId, destDriveId, destItemId,item));

      });
        }

    });
  });
};
module.exports = {

  getGraphToken: function (idToken, clientID, clientSecret) {

    //console.log('Input:2');
    return new Promise((resolve, reject) => {

      var options = {
        'method': 'POST',
        'url': 'https://login.microsoftonline.com/'+connection.getconnection().createTeamsTenancyId+'/oauth2/v2.0/token',
        'headers': {
          'Content-Type': 'application/x-www-form-urlencoded'
        },
        form: {
          'grant_type': 'urn:ietf:params:oauth:grant-type:jwt-bearer',
          'client_id': clientID,
          'assertion': idToken,
          'client_secret': clientSecret,
          'scope': 'openid',
          'requested_token_use': 'on_behalf_of'
        }
      };
      request(options, function (error, response) {
        if (error) throw new Error(error);
        //console.log('Hi');
        var result = JSON.parse(response.body);
        //console.log(response.body);
        resolve(result.access_token)
        //res.send(result.access_token);
      });
    });
  },

getUserId: function (idToken,  clientID, clientSecret) {

    console.log('getUserId:');
    return new Promise((resolve, reject) => {

      var options = {
        'method': 'GET',
        'url': 'https://graph.microsoft.com/v1.0/me',
        'headers': {
          'Authorization': idToken
        }
      };
      request(options, function (error, response) {
        if (error) throw new Error(error);
        console.log('getUserId response:'+JSON.stringify(response));
        var result = JSON.parse(response.body);
        console.log(result.id);
        resolve(result.id)
        //res.send(result.access_token);
      });
    });
  },

  getDummyStructure: function (token) {

    //console.log('Input 2: '+token);
    return new Promise((resolve, reject) => {

      var options = {
        'method': 'GET',
        'url': 'https://graph.microsoft.com/v1.0/groups?$filter=displayName eq \'Deloitte Ascend Staging Content\'',
        'headers': {
          'Authorization': 'Bearer ' + token
        }
      };
      request(options, function (error, response) {
        if (error) throw new Error(error);
        //console.log('Hi');
        var result = JSON.parse(response.body);
        //console.log(response.body);
        resolve(result)
        //res.send(result.access_token);
      });
    });
  },

  getjoinedTeams: function (token, userID) {

    console.log('userID get joined teams:' + userID);
    return new Promise((resolve, reject) => {

      var options = {
        'method': 'GET',
        'url': 'https://graph.microsoft.com/v1.0/users/' + userID,
        'headers': {
          'Authorization': 'Bearer ' + token
        }
      };
      request(options, function (error, response) {
        if (error) throw new Error(error);
        //console.log('Hi');
        var result = JSON.parse(response.body);
        //console.log(response.body);
        resolve(result)
        //res.send(result.access_token);
      });
    });
  },

  createGroup: function (token, projectName, projectManager, OwnerID) {

    console.log('Create Group:' + OwnerID);
    return new Promise((resolve, reject) => {
      var now = new Date().getTime();
      var options = {
        'method': 'POST',
        'url': 'https://graph.microsoft.com/v1.0/groups',
        'headers': {
          'Authorization': 'Bearer ' + token,
          'Content-Type': 'application/json'
        },
        body: JSON.stringify({
          "description": projectName,
          "displayName": projectName,
          "groupTypes": ["Unified"],
          "mailEnabled": false,
          "mailNickname": 'Dummy' + '_' + now,
          "securityEnabled": true,
          "owners@odata.bind": ["https://graph.microsoft.com/v1.0/users/" + OwnerID]
        })

      };
      console.log('options:' + JSON.stringify(options));
      request(options, function (error, response) {
        if (error) throw new Error(error);
        console.log('Hi: ' + error);
        var result = JSON.parse(response.body);
        console.log("Hello Mansi0");
        setTimeout( () => resolve( result ), 60000 );
        //resolve(result)
        //res.send(result.access_token);
      });
    });
  },

  createTeam: function (token, groupID) {
    //app.use(delay(20000));
  //   var now = new Date().getTime();
  //   while (new Date().getTime() < now + 60000) {
  //      //console.log('Hello');
  //     }
    console.log('IN createTeam:' + groupID);
    var linkName = 'https://graph.microsoft.com/v1.0/groups/' + groupID + '/team';

    console.log('linkName1:' + linkName);
    return new Promise((resolve, reject) => {
      var options = {
        'method': 'PUT',
        'url': linkName,
        'headers':
        {

          'Content-Type': 'application/json',
          'Authorization': 'Bearer ' + token
        },
        body: JSON.stringify({ "memberSettings": { "allowCreateUpdateChannels": true }, "messagingSettings": { "allowUserEditMessages": true, "allowUserDeleteMessages": true }, "funSettings": { "allowGiphy": true, "giphyContentRating": "strict" } })
      };
      console.log('options:' + JSON.stringify(options));
      request(options, function (error, response) {
        if (error) throw new Error(error);

        console.log('create teamm response: '+JSON.stringify(response));
        //var result = JSON.parse(response);
        //console.log(response.body);
        resolve(response);
        //res.send(result.access_token);
      });
    });
  },

  GetDriveDetailsforGroup: function (token, groupID) {
    //app.use(delay(20000));

    console.log('IN GetDriveDetailsforGroup:' + groupID);
    var linkName = 'https://graph.microsoft.com/v1.0/groups/' + groupID + '/drives';

    console.log('GetDriveDetailsforGroup linkName1:' + linkName);
    return new Promise((resolve, reject) => {
      var options = {
        'method': 'GET',
        'url': linkName,
        'headers':
        {
          'Authorization': 'Bearer ' + token
        }
      };
      //console.log('options:'+JSON.stringify(options));
      request(options, function (error, response) {
        if (error) throw new Error(error);
        //console.log('error: ');
        var result = JSON.parse(response.body);
        //console.log(response.body);
        //resolve(result)
        setTimeout( () => resolve( result ), 20000 );
        //res.send(result.access_token);
      });
    });
  },

  GettingFileDetailswithinTestTeamFolder: function (token, driveID) {
    //app.use(delay(20000));

    console.log('IN GettingFileDetailswithinTestTeamFolder:' + driveID);
    var linkName = 'https://graph.microsoft.com/v1.0/drives/' + driveID + '/root:/General';

    console.log('linkName1:' + linkName);
    return new Promise((resolve, reject) => {
      var options = {
        'method': 'GET',
        'url': linkName,
        'headers':
        {
          'Authorization': 'Bearer ' + token
        }
      };
      //console.log('options:'+JSON.stringify(options));
      request(options, function (error, response) {
        if (error) throw new Error(error);
        //console.log('error: ');
        var result = JSON.parse(response.body);
        console.log(response.body);
        resolve(result)
        //res.send(result.access_token);
      });
    });
  },

  AddMemberToTeam: function (token, groupId, MemberId) {

    //console.log('Input:'+OwnerID);
    return new Promise((resolve, reject) => {

      var options = {
        'method': 'POST',
        'url': 'https://graph.microsoft.com/v1.0/groups/' + groupId + '/members/$ref',
        'headers': {
          'Authorization': 'Bearer ' + token,
          'Content-Type': 'application/json'
        },
        body: JSON.stringify({ "@odata.id": "https://graph.microsoft.com/v1.0/directoryObjects/" + MemberId })

      };
      request(options, function (error, response) {
        if (error) throw new Error(error);
        console.log('Hi1');
        //var result = JSON.parse(response.body);
        //console.log(response.body);

        resolve("Success")
        //res.send(result.access_token);
      });
    });
  },


  postemailserviceGraph: function (projectId, linkURL) {

    return new Promise((resolve, reject) => {
      var conn = new sql.ConnectionPool(connection.getconnection().dbConfig)

      conn.connect()
        // Successfull connection
        .then(function () {

          // Create request instance, passing in connection instance
          var request = new sql.Request(conn);
          // var req = new sql.Request(conn);
          request.input('myval', sql.VarChar, projectId);


          request.query('select a.project_name, a.project_manager, b.client_name, b.industry, c.user_name FROM ascend.PROJECTS a, ascend.CLIENTS b, ascend.USERS c WHERE a.project_manager = c.user_id AND a.client_id = b.client_id AND a.end_date IS NULL AND b.end_date IS NULL AND a.project_id = @myval').then(function (recordset) {


            const myObjStr = JSON.parse(JSON.stringify(recordset));
            const myObjStr1 = JSON.parse(JSON.stringify(myObjStr.recordsets));
            const myObjStr2 = JSON.parse(JSON.stringify(myObjStr1[0]));
            const project_name = myObjStr2[0].project_name;
            const project_manager = myObjStr2[0].project_manager;
            const client_name = myObjStr2[0].client_name;
            const industry = myObjStr2[0].industry;


            console.log(myObjStr2);
            //console.log(myObjStr2[0].project_name);
            let subj = ('Ascend MS Teams' + project_name + ' successfully created!!!');

            let message = ('<p>Hello ' + myObjStr2[0].user_name + ',<br><br>Your project\’s Microsoft Teams site has been successfully created! Project team members that are assigned in Ascend have been added and Ascend artifacts which are selected by you are available on the Teams site.<br><br><p>Please verify access to the <a href="'+linkURL+'">Teams</a> and check underlying artifacts by logging in to the Microsoft Teams application and accessing the Team described. Please login a support request to AscendNerveCenter@deloitte.com in case of any issues.</p><br><br><p>Please allow up-to 1 working day for our team to get back on this issue.</p><br><br>Details<br><br></p>' +
              '<table border="1">' +
              '<thead>' +
              '<th> Project Name </th>' +
              '<th> Microsoft Team Name </th>' +
              '<th> Project Manager </th>' +
              '<th> Client Name </th>' +
              '<th> Industry </th>' +
              '</thead>'
            );

            message += (
              '<tr>' +
              '<td>' + project_name + '</td>' +
              '<td>' + project_name + '</td>' +
              '<td>' + project_manager + '</td>' +
              '<td>' + client_name + '</td>' +
              '<td>' + industry + '</td>' +
              /*...*/
              '</tr>'

            );

            message += '</table><br><br>';
            var transporter = nodemailer.createTransport({
             service: "gmail",
              secure: false, // true for 465, false for other ports
              auth: {
                user: 'ascendusernew@gmail.com',
                pass: 'Welcome@12345'
              }
            });

           var mailOptions = {
              from: 'ascenduserdeloitte@gmail.com',
              to: 'AscendNerveCenter@deloitte.com',
              //to: 'himmishra@deloitte.com',
              cc: myObjStr2[0].project_manager,
              subject: subj,
              html: message
            };

            transporter.sendMail(mailOptions, function (error, info) {
              if (error) {
                console.log(error);
                //request.send('{\"MSG\":\"ERROR -' + error + '\"}');
              } else {
                console.log('Email sent: ' + info.response);
                //request.send('{\"MSG\":\"SUCCESS\"}');
              };
            });
            //res.send('{\"MSG\":\"SUCCESS\"}');
            resolve('Mail Sent');
          })
            // Handle sql statement execution errors
            .catch(function (err) {
              console.log("err1: " + err);
              //conn.close();
              //resolve(null);
            })

        })
        // Handle connection errors
        .catch(function (err) {
          console.log("err2: " + err);
          //conn.close();
          //resolve(null);
        });

    });
  },

  postemailerrorserviceGraph: function (projectId, errormessage) {

    return new Promise((resolve, reject) => {
      var conn = new sql.ConnectionPool(connection.getconnection().dbConfig)
      //var conn = new sql.ConnectionPool(connection.config)
      console.log("Error Message : " + errormessage);
      conn.connect()
        // Successfull connection
        .then(function () {

          // Create request instance, passing in connection instance
          var request = new sql.Request(conn);
          // var req = new sql.Request(conn);
          request.input('myval', sql.VarChar, projectId);


          request.query('select a.project_name, a.project_manager, b.client_name, b.industry, c.user_name FROM ascend.PROJECTS a, ascend.CLIENTS b, ascend.USERS c WHERE a.project_manager = c.user_id AND a.client_id = b.client_id AND a.end_date IS NULL AND b.end_date IS NULL AND a.project_id = @myval').then(function (recordset) {


            const myObjStr = JSON.parse(JSON.stringify(recordset));
            const myObjStr1 = JSON.parse(JSON.stringify(myObjStr.recordsets));
            const myObjStr2 = JSON.parse(JSON.stringify(myObjStr1[0]));
            console.log(myObjStr2);
            //console.log(myObjStr2[0].project_name);
            let message = ('<p>Nerve Center Team,<br><br>The following project has errored out. Please find the error details below :<br><br>Details<br><br></p>' +
              '<table border="1">' +
              '<thead>' +
              '<th> Project Name </th>' +
              '<th> Project Manager </th>' +
              '<th> Client Name </th>' +
              '<th> Industry </th>' +
              '</thead>'
            );

            message += (
              '<tr>' +
              '<td>' + myObjStr2[0].project_name + '</td>' +
              '<td>' + myObjStr2[0].project_manager + '</td>' +
              '<td>' + myObjStr2[0].client_name + '</td>' +
              '<td>' + myObjStr2[0].industry + '</td>' +
              /*...*/
              '</tr>'

            );

            if (errormessage.length > 0) {
              console.log('Error messages:' + errormessage.length);
              message += '</table><br><br>' + 'Error messages:'
              for (var i = 0; i < errormessage.length; i++) {
                message += (
                  '<br><br><p>' + errormessage[i] + '</p>'
                );
              }
            }

            /*if (errorItemAttr.length>0)
            {message += '</table><br><br>' +'The below files have erroredd out while copying:'
              for (var i = 0; i < errorItemAttr.length; i++) {
                message += (
                '<br><br><p>' +errorItemAttr[i]+'</p>'
              );
              }
            }*/

            message += myObjStr2[0].user_name + '<br><p>This is an auto-generated email from an unmonitored mailbox. Please do not reply to this mail.</p>';
            var transporter = nodemailer.createTransport({
              service: "gmail",
              secure: false, // true for 465, false for other ports
              auth: {
                user: 'ascendusernew@gmail.com',
                pass: 'Welcome@12345'
              }
            });

            var mailOptions = {
              from: 'ascenduserdeloitte@gmail.com',
              to: 'AscendNerveCenter@deloitte.com',
              //to: 'himmishra@deloitte.com',
              cc: myObjStr2[0].project_manager,
              subject: 'Ascend Registration',
              html: message
            };

            transporter.sendMail(mailOptions, function (error, info) {
              if (error) {
                console.log(error);
                //request.send('{\"MSG\":\"ERROR -' + error + '\"}');
              } else {
                console.log('Email sent: ' + info.response);
                //request.send('{\"MSG\":\"SUCCESS\"}');
              };
            });
            //res.send('{\"MSG\":\"SUCCESS\"}');
            resolve('Mail Sent');
          })
            // Handle sql statement execution errors
            .catch(function (err) {
              console.log("err1: " + err);
              //conn.close();
              //resolve(null);
            })

        })
        // Handle connection errors
        .catch(function (err) {
          console.log("err2: " + err);
          //conn.close();
          //resolve(null);
        });

    });
  },

  GetSourceFolderDetails: function (token, driveID) {
    //app.use(delay(20000));

    console.log('IN GetSourceFolderDetails:' + driveID);
    var linkName = 'https://graph.microsoft.com/v1.0/drives/' + driveID + '/root:/General/Core Content - Dummy (New Structure Only)/Core Content';

    console.log('linkName1:' + linkName);
    return new Promise((resolve, reject) => {
      var options = {
        'method': 'GET',
        'url': linkName,
        'headers':
        {
          'Authorization': 'Bearer ' + token
        }
      };
      //console.log('options:'+JSON.stringify(options));
      request(options, function (error, response) {
        if (error) throw new Error(error);
        //console.log('error: ');
        var result = JSON.parse(response.body);
        //console.log(response.body);
        resolve(result)
        //res.send(result.access_token);
      });
    });
  },

  copyFolderStruct: function (accessTokenVal, srcGrpId, srcItemId, destDrvLoc, destitemId) {
    //app.use(delay(20000));

    console.log('IN copyFolderStruct');
    console.log('IN srcGrpId: ' + srcGrpId);
    console.log('IN srcItemId: ' + srcItemId);
    console.log('IN destDrvLoc: ' + destDrvLoc);
    console.log('IN destitemId: ' + destitemId);

    var linkName = 'https://graph.microsoft.com/v1.0/groups/' + srcGrpId + '/drive/items/' + srcItemId + '/copy';

    console.log('linkName1:' + linkName);
    return new Promise((resolve, reject) => {
      var options = {
        'method': 'POST',
        'url': linkName,
        'headers': {
          'Authorization': 'Bearer ' + accessTokenVal,
          'Content-Type': 'application/json'
        },
        body: JSON.stringify({ "parentReference": { "driveId": destDrvLoc, "id": destitemId } })
      };
      console.log('copyFolderStruct options:' + JSON.stringify(options));
      request(options, function (error, response) {
        if (error) throw new Error(error);
        //console.log('error: ');
        //var result = JSON.parse(response.body);
        //console.log(result);

        setTimeout( () => resolve( "Copied Folder Structure Successfully" ), 120000 );
      //   resolve("hello")
        //res.send(result.access_token);
      });
    });
  },

  GetSourceFileDetails: function (token, srcGrpId, destDriveId, destGroupId, projectId) {
    //app.use(delay(20000));
    console.log('IN GetSourceFileDetails token: ' + token);
    console.log('IN GetSourceFileDetails srcGrpId: ' + srcGrpId);
    console.log('IN GetSourceFileDetails destDriveId: ' + destDriveId);
    console.log('IN GetSourceFileDetails destGroupId: ' + destGroupId);
    console.log('IN GetSourceFileDetails projectId: ' + projectId);
    //let projectId = '20';
    /*let srcGrpId = '5b764185-80d4-492c-864a-ae1ad8535f5b';
    let destDriveId = 'b!zZ2vaZsgVk-Hl8EURbAP3VtNA7vD8PJGr9gecj-poYJOdR2YkxiyRK97n4mRb5B3';
    let destGroupId = 'f7a01ede-f1ea-4951-ab8d-806140e0e17d';*/
    let errorItemArr = [];
  let errorFileArr = [];
    return new Promise((resolve, reject) => {
      projectService.getProjectdocumentsid(projectId).then(
        (data) => {
          console.log('In testService Code:');
          //console.log(data);
          //res.send(data);

          //if (error) throw new Error(error);
          let stringResp = data;
          //console.log('String 0 ' + stringResp);
          stringResp = stringResp.replace(/\\/g, "");
          console.log('String 1 ' + stringResp);
          let respArr = [];
          let locationArr = [];
    //let itemId1;
          respArr = JSON.parse(stringResp)[0]['documents'];
    console.log('Documents array: ' + respArr);

          console.log("respArr Length: "+ respArr.length);
          let PromiseArr = [];
          let LocPrmiseArr = [];
          let itemId;
    var promises = respArr.map(async (itemId) => {

    var result = await getFileParameters(token, itemId, srcGrpId, destDriveId, destGroupId);
    return new Promise((res, rej) => {res(result)})

    })

    Promise.all(promises)
    .then( function(values )  {

      return values.filter(function(value) { return typeof value !== 'undefined';});
    })
    .then(function(values) {
      console.log('Output:'+values);
      resolve(values);
    });

      }
    )
      .catch(function (err) {
        console.log(err);
        conn.close();
        return null;
      });
  });


},


  getDBTeamStatus: function (projectId) {
    let message;
    return new Promise((resolve, reject) => {
      var conn = new sql.ConnectionPool(connection.getconnection().dbConfig);
      conn.connect()
        // Successfull connection
        .then(function () {
          console.log('qwertyui: ' + projectId);
          // Create request instance, passing in connection instance
          var request = new sql.Request(conn);
          // var req = new sql.Request(conn);
          request.input('myval', sql.VarChar, projectId);
          // console.log('myval: '+ myval);
          request.query('select a.team_creation_status from ascend.PROJECTS a where a.end_date IS NULL AND a.project_id = @myval').then(function (recordset) {
            console.log('test1: ' + JSON.stringify(recordset));
            const myObjStr = JSON.parse(JSON.stringify(recordset));
            const myObjStr1 = JSON.parse(JSON.stringify(myObjStr.recordsets));
            const myObjStr2 = JSON.parse(JSON.stringify(myObjStr1[0]));
            project_status = myObjStr2[0].team_creation_status;

            if (project_status === 'NOT_INITIATED' || project_status === null) {
              request.query('UPDATE ascend.PROJECTS  SET team_creation_status = \'INITIATED\' where end_date IS NULL AND project_id = @myval').then(function (recordset) {
                console.log('test2: ' + project_status);
                // resolve(message);
                // getDBTeamStatus(projectId);
              });
              message = project_status;
            }
            else {
              // getDBTeamStatus(projectId);

              message = project_status;
            };

            resolve(message);

          })


        })


        // Handle sql statement execution errors
        .catch(function (err) {
          console.log("err1: " + err);
          conn.close();
          return null;
        })

        // Handle connection errors
        .catch(function (err) {
          console.log("err2: " + err);
          conn.close();
          return null;
        });
    });


  },





  UpdateDBTeamStatus: function (projectId) {
    let message;
    return new Promise((resolve, reject) => {
      var conn = new sql.ConnectionPool(connection.getconnection().dbConfig);
      conn.connect()
        // Successfull connection
        .then(function () {
          console.log('qwertyui: ' + projectId);
          // Create request instance, passing in connection instance
          var request = new sql.Request(conn);
          // var req = new sql.Request(conn);
          request.input('myval', sql.VarChar, projectId);
          // console.log('myval: '+ myval);
              request.query('UPDATE ascend.PROJECTS  SET team_creation_status = \'NOT_INITIATED\' where end_date IS NULL AND project_id = @myval').then(function (recordset) {
                // console.log('test2: ' + project_status);
                // resolve(message);
                // getDBTeamStatus(projectId);
              });
              // message = project_status;

            conn.close();
            resolve('Done');

          })


        })


        // Handle sql statement execution errors
        .catch(function (err) {
          console.log("err1: " + err);
          conn.close();
          return null;
        })

        // Handle connection errors
        .catch(function (err) {
          console.log("err2: " + err);
          conn.close();
          return null;
        });


  }

}

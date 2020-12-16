var connection = require('./connection-file');
var sql = require("mssql");
const { Readable } = require('stream');
const util = require('util');
const storage = require('azure-storage');


module.exports = {
  postemailservice: function (projectId) {

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
            console.log(myObjStr2);
            //console.log(myObjStr2[0].project_name);
            let message = ('<p>Nerve Center Team,<br><br>The following project has requested creation of a project area. Please create a dedicated team site for this project and upload all the selected artifacts.<br><br>Details<br><br></p>' +
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

            message += '</table><br><br>' + myObjStr2[0].user_name + '<br><br><p>Thank you for your request of project creation. The nerve center team will work on the request and notify you on completion.</p><br><br><br><br><br><p>This is an auto-generated email from an unmonitored mailbox. Please do not reply to this mail.</p>';
            var transporter = nodemailer.createTransport({
              service: "gmail",
              secure: false, // true for 465, false for other ports
              auth: {
                user: 'ascenduserdeloitte@gmail.com',
                pass: 'Welcome@12345'
              }
            });

            var mailOptions = {
              from: 'ascenduserdeloitte@gmail.com',
              // to: 'AscendNerveCenter@deloitte.com',
              to: 'mohananishselvaraj@gmail.com',
              // cc: myObjStr2[0].project_manager,
              subject: 'Ascend Registration',
              html: message
            };

            transporter.sendMail(mailOptions, function (error, info) {
              if (error) {
                console.log(error);
                request.send('{\"MSG\":\"ERROR -' + err + '\"}');
              } else {
                console.log('Email sent: ' + info.response);
                request.send('{\"MSG\":\"SUCCESS\"}');
              };
            });
            //res.send('{\"MSG\":\"SUCCESS\"}');
            resolve('Mail Sent');
          })
            // Handle sql statement execution errors
            .catch(function (err) {
              console.log(err);
              conn.close();
              resolve(null);
            })

        })
        // Handle connection errors
        .catch(function (err) {
          console.log(err);
          conn.close();
          resolve(null);
        });

    });
  }
  ,
  postSuperUserList: function (jsondata) {
    return new Promise((resolve, reject) => {


      var conn = new sql.ConnectionPool(connection.getconnection().dbConfig)
      conn.connect()
        // Successfull connection
        .then(function () {
          console.log('In connection successfull');
          console.log('In updateData');
          console.log(`jsondata :` + jsondata);
          console.log(`jsondata.length :` + jsondata.length);
          let transaction = new sql.Transaction(conn);
          transaction.begin().then(async function () {
            console.log('In transaction begin successful');
            let request = new sql.Request(transaction);
            let action = jsondata.action;
            let data = jsondata.data;
            let iExceptionFlag = false;
            try {
              //console.log(`jsondata[0].action :` + jsondata[0].action);
              console.log(`jsondata[0].action :` + action);
              //if (jsondata[0].action == 'DELETE') {
                if (action == 'DELETE') {
                // let updatestring = `UPDATE ` + connection.schemaName + `.USER_ROLES SET end_date = GETDATE() where
                //                                                            role_id = 1 and project_id = -1 and user_id = '`+ userId + `'`;
                let deletedstring = `delete from ` + connection.schemaName + `.user_roles where user_id = '` + data.userId + `'` +
                ` and role_id in (select role_id from ` + connection.schemaName + `.roles where UPPER(role_code) = UPPER('SUPER_USER') and is_active = 'Y')`;
                console.log('deletedstring: ' + deletedstring);
                await request.query(deletedstring);

                let updatestring = `UPDATE ` + connection.schemaName + `.USERS SET isadmin = 'false' where user_id = '`+ data.userId + `'`;
                console.log('updatestring: ' + updatestring);
                await request.query(updatestring);
              }
              else {
                // let deletedtring = `delete from ` + connection.schemaName + `.USER_ROLES where role_id = 1 and project_id = -1 and user_id = '` + userId + `'`;
                let deletedtring = `delete from ` + connection.schemaName + `.USERS where user_id = '` + data.userId + `'`;
                console.log('deletedtring: ' + deletedtring);
                await request.query(deletedtring);
                // let insertstring = `insert into ` + connection.schemaName + `.USER_ROLES(user_id, role_id, project_id) VALUES ('` + userId + `', 1 , -1 )`;
                let insertstring = `insert into ` + connection.schemaName + `.USERS(user_id,first_name,last_name,user_name,email_id,job_title,start_date,isadmin) VALUES ('` + data.userId + `','`+data.ssoUser.givenName + `','`+ data.ssoUser.surname+ `','`+data.ssoUser.displayName+ `','`+ data.userId+`','`+data.ssoUser.jobTitle+`',GETDATE(),'true')`;
                console.log('insertstring: ' + insertstring);
                await request.query(insertstring);
              };

              if (action != 'DELETE') {

                let insertuserrolestring = `INSERT INTO ` + connection.schemaName + `.user_roles (
                    user_id
                    ,project_id
                    ,role_id
                    ,created_on
                    )
                SELECT user_id
                    ,project_id
                    ,(
                        SELECT role_id
                        FROM ` + connection.schemaName + `.roles
                        WHERE role_code = 'SUPER_USER'
                        )
                    ,SYSDATETIME()
                FROM (
                    SELECT a.project_id
                        ,b.user_id
                    FROM ` + connection.schemaName + `.projects a
                        ,` + connection.schemaName + `.users b
                    WHERE b.isadmin = 'true'
                    ) a
                WHERE NOT EXISTS (
                        SELECT 1
                        FROM ` + connection.schemaName + `.user_roles b
                        WHERE a.project_id = b.project_id
                            AND a.user_id = b.user_id
                            AND b.role_id = (
                                SELECT role_id
                                FROM ` + connection.schemaName + `.roles
                                WHERE role_code = 'SUPER_USER'
                                )
                        )`;
                  console.log('insertuserrolestring: ' + insertuserrolestring);
                  await request.query(insertuserrolestring);

              }



            }
            catch (e) {
              iExceptionFlag = true;
              console.log('Exception:' + e);
            }
            console.log('Printing the exception')
            console.log(iExceptionFlag)
            if (iExceptionFlag) {
              transaction.rollback().then(function () {
                console.log('In rollback:then');
                conn.close();
                resolve({ "MSG": "Error while inserting data into database" });
              })
                .catch(function (err) {
                  console.log('In rollback:catch');
                  conn.close();
                  resolve({ "MSG": "Error while rolling back transaction" });
                });
            } else {
              transaction.commit().then(function () {
                console.log('In commit:then');
                conn.close();
                resolve({ "MSG": "SUCCESS" });
              })
                .catch(function (err) {
                  console.log('In commit:catch');
                  conn.close();
                  resolve({ "MSG": "Error while commiting transaction" });
                });
            }


          })
            .catch(function (err) {
              console.log('In transaction begin catch:' + err);
              conn.close();
              resolve({ "MSG": "Error while creating transaction object" });
            })

        })
        .catch(function (err) {
          console.log('In connection catch catch:' + err)
          conn.close();
          resolve({ "MSG": "Error while creating connection object" });
        })

    })
  }
  ,
  postMembersupdate: function (jsondata) {
    return new Promise((resolve, reject) => {


      var conn = new sql.ConnectionPool(connection.getconnection().dbConfig)
      conn.connect()
        // Successfull connection
        .then(function () {
          console.log('In connection successfull');
          console.log('In updateData');
          console.log(jsondata);
          console.log(jsondata.length);
          let transaction = new sql.Transaction(conn);
          transaction.begin().then(async function () {
            console.log('In transaction begin successful');
            let request = new sql.Request(transaction);
            var v_project_id;
            var members;
            for (var i = 0; i < jsondata.length; i++) {
              members = jsondata[i].members;
              v_project_id = jsondata[i].projectId;
              console.log(`v_project_id :` + v_project_id);
            };
            let iExceptionFlag = false;
            try {
              for (var j = 0; j < members.length; j++) {
                if (members[j].action == 'UPDATE') {
                  let updatestring = `update ` + connection.schemaName + `.user_roles set role_id = (select role_id from `
                    + connection.schemaName + `.roles where role_code = '` + members[j].projectRole + `' ),last_updated_on= GETDATE()
                                                                                                                          where role_id IN (select role_id from ascend.roles where role_code<> 'SUPER_USER') and user_id = '`+ members[j].userId + `' and project_id = ` + v_project_id;
                  console.log('updatestring:' + updatestring);
                  await request.query(updatestring);
                }
                else if (members[j].action == 'CREATE') {
                  let seperator = `','`;

                  let createuserstring = `insert into ` + connection.schemaName + `.users (user_id, first_name, last_name, user_name, email_id, is_active, designation, created_on, last_updated_on, isadmin, job_title, start_date, end_date)`+
                  `select '` + members[j].userId + `','` + members[j].firstName + `','` + members[j].lastName + `','` + members[j].displayName + `','` + members[j].userId + `'`+
                  `, 'Y',null,getdate(),null,'false','`+members[j].jobTitle+`',getdate(),null `+
                  `where not exists (select 1 from ` + connection.schemaName + `.users where user_id = '`+members[j].userId+`')`;
                  console.log('createuserstring:' + createuserstring);
                  await request.query(createuserstring);

                  let createstring = `insert into ` + connection.schemaName + `.user_roles (user_id, role_id, project_id, created_on, last_updated_on) select '` + members[j].userId + `', (select role_id from ` + connection.schemaName + `.roles
                                                                                                                           where role_code = '`+ members[j].projectRole + `'),` + v_project_id + `, GETDATE() , GETDATE() `;
                  console.log('createstring:' + createstring);
                  await request.query(createstring);
                }
                else {
                  let updatestringfi = `update ` + connection.schemaName + `.user_roles set end_date = GETDATE() where
                  role_id IN (select role_id from ascend.roles where role_code<> 'SUPER_USER') and user_id = '`
                    + members[j].userId + `' and role_id in (select role_id from ` + connection.schemaName + `.roles where role_code = '`
                    + members[j].projectRole + `' ) and project_id = ` + v_project_id;
                  console.log('updatestringfi:' + updatestringfi);
                  await request.query(updatestringfi);
                };
              };
            }
            catch (e) {
              iExceptionFlag = true;
              console.log('Exception:' + e);
            }
            console.log('Printing the exception')
            console.log(iExceptionFlag)
            if (iExceptionFlag) {
              transaction.rollback().then(function () {
                console.log('In rollback:then');
                conn.close();
                resolve({ "MSG": "Error while inserting data into database" });
              })
                .catch(function (err) {
                  console.log('In rollback:catch');
                  conn.close();
                  resolve({ "MSG": "Error while rolling back transaction" });
                });
            } else {
              transaction.commit().then(function () {
                console.log('In commit:then');
                conn.close();
                resolve({ "MSG": "SUCCESS" });
              })
                .catch(function (err) {
                  console.log('In commit:catch');
                  conn.close();
                  resolve({ "MSG": "Error while commiting transaction" });
                });
            }


          })
            .catch(function (err) {
              console.log('In transaction begin catch:' + err);
              conn.close();
              resolve({ "MSG": "Error while creating transaction object" });
            })

        })
        .catch(function (err) {
          console.log('In connection catch catch:' + err)
          conn.close();
          resolve({ "MSG": "Error while creating connection object" });
        })

    })
  }
  ,

  getSuperUserListSource: function () {

    return new Promise((resolve, reject) => {
      var conn = new sql.ConnectionPool(connection.getconnection().dbConfig)
      conn.connect()
        // Successfull connection
        .then(function () {
          // Create request instance, passing in connection instance
          var request = new sql.Request(conn);
          //request.input('myval',sql.VarChar,projectid);

          request.query(`WITH t (data)
          AS (select b.user_name AS name,
                     b.user_id AS userId
          from ascend.USERS b
          WHERE b.end_date is null
          AND b.isadmin = 'true'
          AND (b.start_date IS NULL OR b.start_date <= GETDATE())
          AND (b.end_date IS NULL OR b.end_date >= GETDATE())
          FOR JSON PATH)
          SELECT data
          FROM t`)
            .then(function (recordsets) {
              let res = JSON.parse(JSON.stringify(recordsets.recordset[0]));
              //console.log(a);
              //console.log('Printing data.2.1');
              console.log(res.data);
              conn.close();
              var printdetails = res.data;
              var printfinal = printdetails.replace(/\\\"/g, '');
              //resolve(res.data);
              resolve(printfinal);
            })
            // Handle sql statement execution errors
            .catch(function (err) {
              console.log(err);
              conn.close();

            })
        })
        // Handle connection errors
        .catch(function (err) {
          console.log(err);
          conn.close();
          resolve(null);
        });

    });
  }
  ,
  getScopeDetailsPagefun: function () {

    return new Promise((resolve, reject) => {
      var conn = new sql.ConnectionPool(connection.getconnection().dbConfig)

      conn.connect()
        // Successfull connection
        .then(function () {

          // Create request instance, passing in connection instance
          var req = new sql.Request(conn);


          // Call mssql's query method passing in params
          //req.query("select * from ascend.projects where project_id in (1)")

          /*req.query(`with t(data) as  (select project_id 'project_id'
          ,a.description + a.project_type 'type'
    from ascend.projects a
    where project_id in (1,2)
    for json path) select data from t*/
          /*req.query(`with t (data) as (
            select
            (select CONCAT('[', STRING_AGG( '"' + name + '"',','),']')   from ascend.paas) paas,
            (select CONCAT('[', STRING_AGG( '"' + name + '"',','),']')   from ascend.region_new) region,
            (select CONCAT('[', STRING_AGG( '"' + name + '"',','),']')   from ascend.country_new) country,
            (select CONCAT('[', STRING_AGG( '"' + service_name + '"',','),']')   from ascend.scope_services) scopeOfServices,
            (select CONCAT('[', STRING_AGG( '"' + L1 + '"',','),']') from (select distinct L1 from ascend.business_processes_new) LL) businessProcesses,
            (select CONCAT('[', STRING_AGG( '"' + name + '"',','),']')   from ascend.integration_types) integrationPlatform
            for json path)
            select  replace(replace(replace(replace(data,'"[','['),']"',']'),'[{','{'),'}]','}') as data from t`)*/
            req.query(`with t (data) as (
              select
              (select CONCAT('[', STRING_AGG( '"' + name + '"',','),']')   from ascend.paas) paas,
              (select CONCAT('[', STRING_AGG( '"' + name + '"',','),']')   from ascend.region_new) region,
              (select CONCAT('[', STRING_AGG( '"' + name + '"',','),']')   from ascend.country_new) country,
              (select CONCAT('[', STRING_AGG( '"' + service_name + '"',','),']')   from ascend.scope_services) scopeOfServices,
              (select distinct L1 processareal1,
              (select CONCAT('[', STRING_AGG( '"' + L2 + '"',','),']')
                  from (select distinct L2 from ascend.business_processes_new b where a.L1=b.L1)LL
              ) processareal2 from ascend.business_processes_new a for json path) businessprocessL1L2,
              (select CONCAT('[', STRING_AGG( '"' + name + '"',','),']')   from ascend.integration_types) integrationPlatform
              for json path)
              select  replace(replace(substring(data,2,len(data)-2),'"[','['),']"',']') as data from t
              `)
            .then(function (recordset) {

              let res = JSON.parse(JSON.stringify(recordset.recordset[0]));

              console.log(res.data);

              conn.close();
              var printde = res.data;
              //var printf = printd.replace(/\\\"/g, ''); ///\\/g
              var printfe = printde.replace(/\\/g, "");
              resolve(printfe);
            })
            // Handle sql statement execution errors
            .catch(function (err) {
              console.log(err);
              conn.close();
              resolve(null);
            })

        })
        // Handle connection errors
        .catch(function (err) {
          console.log(err);
          conn.close();
          resolve(null);
        });

    });
  }
  ,
  getClientDetailsPageSource: function () {

    return new Promise((resolve, reject) => {
      var conn = new sql.ConnectionPool(connection.getconnection().dbConfig)
      conn.connect()
        // Successfull connection
        .then(function () {
          // Create request instance, passing in connection instance
          var request = new sql.Request(conn);
          //request.input('myval',sql.VarChar,projectid);

          request.query(`with t (data) as 
(select (select distinct industry,
            (select CONCAT('[', STRING_AGG( '"' + sector + '"',','),']') from (select distinct sector from ascend.industries_new b where a.industry=b.industry)SS
            ) sector
            from ascend.industries_new a for json path,include_null_values) industrySector, 
            ( select (
            '["<$1B","$1B to $2.5B","$2.5B to $5B","$5B to $10B","10B+"]'
            )companyRevenue  ) companyRevenue  
             for json path,include_null_values,without_array_wrapper)
 select  replace(replace(replace(data,'"[','['),']"',']'),'\','') as data from t`)
            .then(function (recordsets) {
              let res = JSON.parse(JSON.stringify(recordsets.recordset[0]));
              //console.log(a);
              //console.log('Printing data.2.1');
              console.log(res.data);
              conn.close();
              var printdetails = res.data;
              //var printfinal = printdetails.replace(/\\\"/g, ''); //replace(/\\/g, "");
              //resolve(res.data);
              var printfinal = printdetails.replace(/\\/g, '');
              resolve(printfinal);
            })
            // Handle sql statement execution errors
            .catch(function (err) {
              console.log(err);
              conn.close();

            })
        })
        // Handle connection errors
        .catch(function (err) {
          console.log(err);
          conn.close();
          resolve(null);
        });

    });
  },
  getProjectDetailsPagefunc: function () {

    return new Promise((resolve, reject) => {
      var conn = new sql.ConnectionPool(connection.getconnection().dbConfig)

      conn.connect()
        // Successfull connection
        .then(function () {

          // Create request instance, passing in connection instance
          var req = new sql.Request(conn);


          // Call mssql's query method passing in params
         //req.query("select * from ascend.projects where project_id in (1)")

          /*req.query(`with t(data) as  (select project_id 'project_id'
          ,a.description + a.project_type 'type'
    from ascend.projects a
    where project_id in (1,2)
    for json path) select data from t*/
          req.query(`with t (data)
          as
          (
          select
          (select user_id 'userId',  a.user_name 'userName' from ascend.users a
          WHERE a.designation = 'D'--IN ('M','SM') )
          for json path ) leadPD,
          (select CONCAT('[', STRING_AGG( '"' + name + '"',','),']')   from ascend.country_new ) country,
          (select CONCAT('[', STRING_AGG( '"' + name + '"',','),']')   from (select distinct name from ascend.erp_new) c ) erpPackage,
          (select user_id 'userId',  a.user_name 'userName' from ascend.users a
          WHERE a.designation IN ('M','SM')
          for json path ) managersList,
          (
              select distinct portfolio as portfolioName,
          (select CONCAT('[', STRING_AGG( '"' + name + '"',','),']')
          from ascend.source_offering_new b where a.portfolio=b.portfolio ) offeringName
          from ascend.source_offering_new a
          for json path
          ) portfolioOfferings
          for json path)
          select  replace(replace(substring(data,2,len(data)-2),'"[','['),']"',']') as data from t`)
            .then(function (recordset) {

              let res = JSON.parse(JSON.stringify(recordset.recordset[0]));

              console.log(res.data);

              conn.close();
              var printde = res.data;
              //var printf = printd.replace(/\\\"/g, ''); ///\\/g
              var printfe = printde.replace(/\\/g, "");
              resolve(printfe);
            })
            // Handle sql statement execution errors
            .catch(function (err) {
              console.log(err);
              conn.close();
              resolve(null);
            })

        })
        // Handle connection errors
        .catch(function (err) {
          console.log(err);
          conn.close();
          resolve(null);
        });

    });
  },
  updateData: function (data) {
    return new Promise((resolve, reject) => {
      console.log('In updateData');
      var conn = new sql.ConnectionPool(connection.getconnection().dbConfig)
      var userId = "arcjoshi@deloitte.com";
      conn.connect()
        // Successfull connection
        .then(function () {
          console.log('In connection successfull');

          /*var ps = new sql.PreparedStatement(conn);
          let promises = []
          ps.input('colVal',sql.VarChar);
          ps.prepare('insert into test values (@colVal,@colVal)').then((data)=>{
            for(let i =0;i<2;i++){
              let string = 'T'+i;
              console.log('In for loop:'+string);
              promises.push(ps.execute({'colVal':string}));
            }
          })*/

          var request = new sql.Request(conn);
          let promises = []
          for (let i = 0; i < 3; i++) {
            console.log('In loop:' + i);
            let string = 'T' + i;
            if (i == 1) {
              promises.push(request.query(`insert into test values('TESTSS','TEST1')`));
            } else {
              promises.push(request.query(`insert into test values('TEST','TEST1')`));
            }
          }
          Promise.all(promises).then(function (data) {

            console.log('In Success:' + data);
            console.log(data);
            resolve(null)
          }

          )
            .catch(function (err) {

              console.log('In error:' + err);
              resolve(null)
            })

        })
        // Handle connection errors
        .catch(function (err) {
          console.log('In connection error');
          console.log(err);
          conn.close();
          resolve(null);
        });
    });
  },

  getProjectsummaryid: function (projectid) {

    return new Promise((resolve, reject) => {
      var conn = new sql.ConnectionPool(connection.getconnection().dbConfig)
      conn.connect()
        // Successfull connection
        .then(function () {
          // Create request instance, passing in connection instance
          var request = new sql.Request(conn);
          request.input('myval', sql.VarChar, projectid);

          request.query(`with t(data) as (select (SELECT p1.project_name 'projectName'
,
(case  
when p1.description is null then
'Below is a summary list, by type, of applicable deliverables and amplifiers that have been added to the project. Once you click on “Publish to Teams” they will be added to the MS Teams site. You cannot undo this action. To view/update your selection, please select a tile below and you will be taken to that section.'
else p1.description
end)
 as projectDesc
          ,(CASE 
            WHEN p1.logo_consent_flag = 'Y' THEN '` +  connection.getconnection().storageContainerPath+`/`+ connection.getconnection().storageContainerName+`/'+c.logo 
            ELSE '../../../assets/GoogleLogo.png'
            END) 'clientLogo'
          ,p1.team_creation_status 'teamCreationStatus'
          ,(select u.user_name AS name
         ,'dummy.jpg' profilePic
         , u.job_title AS designation
  from ascend.projects p
  ,ascend.user_roles ur
  ,ascend.users u
  ,ascend.roles r
  where p.project_id = ur.project_id
  and ur.user_id = u.user_id
  and r.role_code = 'PROJECT_ADMIN'
  and r.role_id = ur.role_id
  and p.project_id = p1.project_id
          for json path,include_null_values) 'Admins'
      ,( select * from (
		select e.display_name 'artifactType'
        ,e.phase_name 'artifactPhase'
        ,e.stop_name 'artifactStop'
        ,e.route 'artifactLink'
		,e.ocm_entity 'ocmEntity'
        ,(select count_big(0)
          from ascend.project_workspace a,
		  ascend.documents           doc
          where 1=1
		  and a.project_id = p1.project_id
        and a.entity_name = e.name
		and a.entity_value=doc.name			   
		and doc.type = 'Activate Digital Org'
		) 'artifactCount'
  ,e.icon_name 'artifactIconPath'
  from ascend.entities e
  where  1=1  AND e.name ='ACTIVATE_DIGITAL_ORGANIZATION'
  and e.show_entity='Y'
  union
  select e.display_name 'artifactType'
        ,e.phase_name 'artifactPhase'
        ,e.stop_name 'artifactStop'
        ,e.route 'artifactLink'
		,e.ocm_entity 'ocmEntity'
        ,(select count_big(0)
          from ascend.project_workspace a,
		  ascend.documents           doc
          where 1=1
		  and a.project_id = p1.project_id
        and a.entity_name = e.name
		and a.entity_value=doc.name			   
		and doc.type = 'Analytics and Report'
		) 'artifactCount'
  ,e.icon_name 'artifactIconPath'
  from ascend.entities e
  where  1=1  AND e.name ='ANALYTICS_REPORTS'
  and e.show_entity='Y'
  union
  select e.display_name 'artifactType'
        ,e.phase_name 'artifactPhase'
        ,e.stop_name 'artifactStop'
        ,e.route 'artifactLink'
		,e.ocm_entity 'ocmEntity'
        ,(select count_big(0)
          from ascend.project_workspace a,
		  ascend.documents           doc
          where 1=1
		  and a.project_id = p1.project_id
        and a.entity_name = e.name
		and a.entity_value=doc.name			   
		and doc.type = 'Business Solutions'
		) 'artifactCount'
  ,e.icon_name 'artifactIconPath'
  from ascend.entities e
  where  1=1  AND e.name ='BUSINESS_SOLUTIONS'
  and e.show_entity='Y'
    union
  select e.display_name 'artifactType'
        ,e.phase_name 'artifactPhase'
        ,e.stop_name 'artifactStop'
        ,e.route 'artifactLink'
		,e.ocm_entity 'ocmEntity'
        ,(select count_big(0)
          from ascend.project_workspace a,
		  ascend.documents           doc
          where 1=1
		  and a.project_id = p1.project_id
        and a.entity_name = e.name
		and a.entity_value=doc.name			   
		and doc.type = 'Config Workbooks'
		) 'artifactCount'
  ,e.icon_name 'artifactIconPath'
  from ascend.entities e
  where  1=1  AND e.name ='CONFIG_WORKBOOKS' 
  and e.show_entity='Y'
union
  select e.display_name 'artifactType'
        ,e.phase_name 'artifactPhase'
        ,e.stop_name 'artifactStop'
        ,e.route 'artifactLink'
		,e.ocm_entity 'ocmEntity'
        ,(select count_big(0)
          from ascend.project_workspace a,
		  ascend.documents           doc
          where 1=1
		  and a.project_id = p1.project_id
        and a.entity_name = e.name
		and a.entity_value=doc.name			   
		and doc.type = 'Conversion'
		) 'artifactCount'
  ,e.icon_name 'artifactIconPath'
  from ascend.entities e
  where  1=1  AND e.name ='CONVERSIONS'  
and e.show_entity='Y'  
  union
  select e.display_name 'artifactType'
        ,e.phase_name 'artifactPhase'
        ,e.stop_name 'artifactStop'
        ,e.route 'artifactLink'
		,e.ocm_entity 'ocmEntity'
        ,(select count_big(0)
          from ascend.project_workspace a,
		  ascend.documents           doc
          where 1=1
		  and a.project_id = p1.project_id
        and a.entity_name = e.name
		and a.entity_value=doc.name			   
		and doc.type = 'Define Digital Org'
		) 'artifactCount'
  ,e.icon_name 'artifactIconPath'
  from ascend.entities e
  where  1=1  AND e.name ='DEFINE_DIGITAL_ORGANIZATION' 
  and e.show_entity='Y'
union
  select e.display_name 'artifactType'
        ,e.phase_name 'artifactPhase'
        ,e.stop_name 'artifactStop'
        ,e.route 'artifactLink'
		,e.ocm_entity 'ocmEntity'
        ,(select count_big(0)
          from ascend.project_workspace a,
		  ascend.documents           doc
          where 1=1
		  and a.project_id = p1.project_id
        and a.entity_name = e.name
		and a.entity_value=doc.name			   
		and doc.type = 'Deliverables'
		) 'artifactCount'
  ,e.icon_name 'artifactIconPath'
  from ascend.entities e
  where  1=1  AND e.name ='DELIVERABLES'  
and e.show_entity='Y'  
  union
  select e.display_name 'artifactType'
        ,e.phase_name 'artifactPhase'
        ,e.stop_name 'artifactStop'
        ,e.route 'artifactLink'
		,e.ocm_entity 'ocmEntity'
        ,(select count_big(0)
          from ascend.project_workspace a,
		  ascend.documents           doc
          where 1=1
		  and a.project_id = p1.project_id
        and a.entity_name = e.name
		and a.entity_value=doc.name			   
		and doc.type = 'Deploy'
		) 'artifactCount'
  ,e.icon_name 'artifactIconPath'
  from ascend.entities e
  where  1=1  AND e.name ='DEPLOY'  
  and e.show_entity='Y'
   union
  select e.display_name 'artifactType'
        ,e.phase_name 'artifactPhase'
        ,e.stop_name 'artifactStop'
        ,e.route 'artifactLink'
		,e.ocm_entity 'ocmEntity'
        ,(select count_big(0)
          from ascend.project_workspace a,
		  ascend.documents           doc
          where 1=1
		  and a.project_id = p1.project_id
        and a.entity_name = e.name
		and a.entity_value=doc.name			   
		and doc.type = 'Regression Testing'
		) 'artifactCount'
  ,e.icon_name 'artifactIconPath'
  from ascend.entities e
  where  1=1  AND e.name ='REGRESSION_TESTING'   
  and e.show_entity='Y'
  union
  select e.display_name 'artifactType'
        ,e.phase_name 'artifactPhase'
        ,e.stop_name 'artifactStop'
        ,e.route 'artifactLink'
		,e.ocm_entity 'ocmEntity'
        ,(select count_big(0)
          from ascend.project_workspace a,
		  ascend.documents           doc
          where 1=1
		  and a.project_id = p1.project_id
        and a.entity_name = e.name
		and a.entity_value=doc.name			   
		and doc.type = 'Interfaces'
		) 'artifactCount'
  ,e.icon_name 'artifactIconPath'
  from ascend.entities e
  where  1=1  AND e.name ='INTERFACES'  
  and e.show_entity='Y'
 union
  select e.display_name 'artifactType'
        ,e.phase_name 'artifactPhase'
        ,e.stop_name 'artifactStop'
        ,e.route 'artifactLink'
		,e.ocm_entity 'ocmEntity'
        ,(select count_big(0)
          from ascend.project_workspace a,
		  ascend.documents           doc
          where 1=1
		  and a.project_id = p1.project_id
        and a.entity_name = e.name
		and a.entity_value=doc.name			   
		and doc.type = 'Stabilize'
		) 'artifactCount'
  ,e.icon_name 'artifactIconPath'
  from ascend.entities e
  where  1=1  AND e.name ='STABILIZE'
and e.show_entity='Y'  
union
  select e.display_name 'artifactType'
        ,e.phase_name 'artifactPhase'
        ,e.stop_name 'artifactStop'
        ,e.route 'artifactLink'
		,e.ocm_entity 'ocmEntity'
        ,(select count_big(0)
          from ascend.project_workspace a,
		  ascend.documents           doc
          where 1=1
		  and a.project_id = p1.project_id
        and a.entity_name = e.name
		and a.entity_value=doc.name	
		and doc.category=a.l3		
		and doc.type = 'Test Scripts'
		) 'artifactCount'
  ,e.icon_name 'artifactIconPath'
  from ascend.entities e
  where  1=1  AND e.name ='TEST_SCENARIOS' 
and e.show_entity='Y'  
  union
  select e.display_name 'artifactType'
        ,e.phase_name 'artifactPhase'
        ,e.stop_name 'artifactStop'
        ,e.route 'artifactLink'
		,e.ocm_entity 'ocmEntity'
        ,(select count_big(distinct pws.entity_value)
          from ascend.project_workspace pws,
		  ASCEND.automation_bots       a
          where 1=1
		  and pws.project_id = p1.project_id
        and pws.entity_name = e.name		   
		 AND a.bot_name = pws.entity_value
                                    AND a.process_area_l2 = pws.L2
                                    AND a.process_area_l1 = pws.L3
								and a.url is not null
		) 'artifactCount'
  ,e.icon_name 'artifactIconPath'
  from ascend.entities e
  where  1=1  AND e.name ='TEST_AUTOMATIONS' 
and e.show_entity='Y'  
    union
	select e.display_name 'artifactType'
        ,e.phase_name 'artifactPhase'
        ,e.stop_name 'artifactStop'
        ,e.route 'artifactLink'
		,e.ocm_entity 'ocmEntity'
        ,(select count_big(DISTINCT A.L2)
          from ascend.project_workspace a,
		  ascend.documents           doc
          where 1=1
		  and a.project_id = p1.project_id
		  and a.entity_name = e.name
		  and a.l2 =doc.name
          AND a.L1 = doc.category
          AND doc.type ='Journey Maps'                                 
		) 'artifactCount'
  ,e.icon_name 'artifactIconPath'
  from ascend.entities e
  where 1=1 
   AND e.name ='JOURNEY_MAP'
   and e.show_entity='Y'
   union
	select e.display_name 'artifactType'
        ,e.phase_name 'artifactPhase'
        ,e.stop_name 'artifactStop'
        ,e.route 'artifactLink'
		,e.ocm_entity 'ocmEntity'
        ,(select count_big(0)
          from ascend.project_workspace a,
		  ascend.documents           doc
          where 1=1
		  and a.project_id = p1.project_id
		  and a.entity_name = e.name
		  and a.l2 =doc.name
          AND a.L1 = doc.category
          AND doc.type ='Personas'                                 
		) 'artifactCount'
  ,e.icon_name 'artifactIconPath'
  from ascend.entities e
  where 1=1 
   AND e.name ='PERSONAS'
   and e.show_entity='Y'
      union
	select e.display_name 'artifactType'
        ,e.phase_name 'artifactPhase'
        ,e.stop_name 'artifactStop'
        ,e.route 'artifactLink'
		,e.ocm_entity 'ocmEntity'
        ,(select count_big(distinct l2)
          from ascend.project_workspace a,
		  ascend.documents           doc
          where 1=1
		  and a.project_id = p1.project_id
		  and a.entity_name = e.name
		  and a.l2 =doc.name
          AND a.L1 = doc.category
          AND doc.type ='User Story'                                 
		) 'artifactCount'
  ,e.icon_name 'artifactIconPath'
  from ascend.entities e
  where 1=1 
   AND e.name ='USER_STORIES'
   and e.show_entity='Y'
     union
	select e.display_name 'artifactType'
        ,e.phase_name 'artifactPhase'
        ,e.stop_name 'artifactStop'
        ,e.route 'artifactLink'
		,e.ocm_entity 'ocmEntity'
        ,(select count_big(distinct a.l2)
          from ascend.project_workspace a,
		  ascend.documents           doc
          where 1=1
		  and a.project_id = p1.project_id
		  and a.entity_name = e.name
		  and a.l2 =doc.name
          AND a.entity_value = doc.category
          AND doc.type ='Refine User Story'                                 
		) 'artifactCount'
  ,e.icon_name 'artifactIconPath'
  from ascend.entities e
  where 1=1 
   AND e.name ='USER_STORY_LIBRARY'
   and e.show_entity='Y'
  union
	select e.display_name 'artifactType'
        ,e.phase_name 'artifactPhase'
        ,e.stop_name 'artifactStop'
        ,e.route 'artifactLink'
		,e.ocm_entity 'ocmEntity'
        ,(select count_big(distinct a.l2)
          from ascend.project_workspace a,
		  ascend.documents           doc
          where 1=1
		  and a.project_id = p1.project_id
        and a.entity_name = e.name
		and a.l2    =doc.name
        AND a.L3    = doc.category
        AND doc.type IN ('KDD')
                          --and a.project_id=c.project_id                     
		) 'artifactCount'
  ,e.icon_name 'artifactIconPath'
  from ascend.entities e
  where 1=1 
   AND e.name in ( 'KEY_DESIGN_DECISIONS')
   and e.show_entity='Y'
	union
	select e.display_name 'artifactType'
        ,e.phase_name 'artifactPhase'
        ,e.stop_name 'artifactStop'
        ,e.route 'artifactLink'
		,e.ocm_entity 'ocmEntity'
        ,(select count_big(distinct l2)
          from ascend.project_workspace a,
		  ascend.documents           doc
          where 1=1
		  and a.project_id = p1.project_id
        and a.entity_name = e.name
        and a.l2           =doc.name
        AND a.entity_value = doc.category
         AND doc.type='Process Flows'
		) 'artifactCount'
  ,e.icon_name 'artifactIconPath'
  from ascend.entities e
  where 1=1 
   AND e.name in ( 'PROCESS_FLOWS')
   and e.show_entity='Y'
   union
	select e.display_name 'artifactType'
        ,e.phase_name 'artifactPhase'
        ,e.stop_name 'artifactStop'
        ,e.route 'artifactLink'
		,e.ocm_entity 'ocmEntity'
        ,(select count_big(0)
          from ascend.project_workspace a,
		  ascend.documents           doc
          where 1=1
		  and a.project_id = p1.project_id
        and a.entity_name = e.name
        and a.entity_value           =doc.name
         AND doc.type='Quarterly Insights'
		) 'artifactCount'
  ,e.icon_name 'artifactIconPath'
  from ascend.entities e
  where 1=1 
   AND e.name in ( 'QUARTERLY_INSIGHTS')
   and e.show_entity='Y'
   union
select e.display_name 'artifactType'
        ,e.phase_name 'artifactPhase'
        ,e.stop_name 'artifactStop'
        ,e.route 'artifactLink'
		,e.ocm_entity 'ocmEntity'
        ,(select count_big(a.entity_value)
          from ascend.project_workspace a,
		  ascend.tools_accelerators_new doc
          where 1=1
		  and a.entity_value =doc.tool_name
		   AND ISNULL(doc.project_phase,'X') = ISNULL(a.L3,'X')
		   and a.project_id=p1.project_id
		   and doc.hosted_url is not null
		) 'artifactCount'
  ,e.icon_name 'artifactIconPath'
  from ascend.entities e
  where 1=1 
   AND e.name in ( 'DEVELOPMENT_TOOLS')	
   and e.show_entity='Y'
      union
select e.display_name 'artifactType'
        ,e.phase_name 'artifactPhase'
        ,e.stop_name 'artifactStop'
        ,e.route 'artifactLink'
		,e.ocm_entity 'ocmEntity'
        ,(select count_big(0)
          from ascend.project_workspace a,
		  ascend.configuration_workbooks doc
          where 1=1
		    and a.entity_value =doc.name
            and doc.module     = a.l2
			and a.project_id=p1.project_id
			and doc.doc_link is not null
		) 'artifactCount'
  ,e.icon_name 'artifactIconPath'
  from ascend.entities e
  where 1=1 
   AND e.name in ( 'ERP_CONFIGURATIONS')	
   and e.show_entity='Y'
   -- Ocm Changes
union
	select e.display_name 'artifactType'
        ,e.phase_name 'artifactPhase'
        ,e.stop_name 'artifactStop'
        ,e.route 'artifactLink'
		,e.ocm_entity 'ocmEntity'
        ,(select count_big(distinct l1)
          from ascend.project_workspace a,
		  ascend.documents           doc
          where 1=1
		  and a.project_id = p1.project_id
        and a.entity_name = e.name
        and a.l1           =doc.name
         AND doc.type='Launch journey OCM'
		) 'artifactCount'
  ,e.icon_name 'artifactIconPath'
  from ascend.entities e
  where 1=1 
   AND e.name in ( 'LAUNCH_JOURNEY_OCM')
   and e.show_entity='Y'
union
	select e.display_name 'artifactType'
        ,e.phase_name 'artifactPhase'
        ,e.stop_name 'artifactStop'
        ,e.route 'artifactLink'
		,e.ocm_entity 'ocmEntity'
        ,(select count_big(distinct l1)
          from ascend.project_workspace a,
		  ascend.documents           doc
          where 1=1
		  and a.project_id = p1.project_id
        and a.entity_name = e.name
        and a.l1           =doc.name
         AND doc.type='Refine user stories OCM'
		) 'artifactCount'
  ,e.icon_name 'artifactIconPath'
  from ascend.entities e
  where 1=1 
   AND e.name in ( 'REFINE_USER_STORIES_OCM')
   and e.show_entity='Y'
union
	select e.display_name 'artifactType'
        ,e.phase_name 'artifactPhase'
        ,e.stop_name 'artifactStop'
        ,e.route 'artifactLink'
		,e.ocm_entity 'ocmEntity'
        ,(select count_big(distinct l1)
          from ascend.project_workspace a,
		  ascend.documents           doc
          where 1=1
		  and a.project_id = p1.project_id
        and a.entity_name = e.name
        and a.l1           =doc.name
         AND doc.type='Construct OCM'
		) 'artifactCount'
  ,e.icon_name 'artifactIconPath'
  from ascend.entities e
  where 1=1 
   AND e.name in ( 'CONSTRUCT_OCM')
   and e.show_entity='Y'
union
	select e.display_name 'artifactType'
        ,e.phase_name 'artifactPhase'
        ,e.stop_name 'artifactStop'
        ,e.route 'artifactLink'
		,e.ocm_entity 'ocmEntity'
        ,(select count_big(distinct l1)
          from ascend.project_workspace a,
		  ascend.documents           doc
          where 1=1
		  and a.project_id = p1.project_id
        and a.entity_name = e.name
        and a.l1           =doc.name
         AND doc.type='Deploy OCM'
		) 'artifactCount'
  ,e.icon_name 'artifactIconPath'
  from ascend.entities e
  where 1=1 
   AND e.name in ( 'DEPLOY_OCM')
   and e.show_entity='Y'
	) t for json path,include_null_values) 'artifacts'
   from ascend.projects p1,
   ascend.clients c
   where p1.project_id = @myval
   and p1.client_id = c.client_id
   for json path))
   select data from t`)
            .then(function (recordsets) {
              let res = JSON.parse(JSON.stringify(recordsets.recordset[0]));
              //console.log(a);
              //console.log('Printing data.2.1');
              console.log(res.data);
              var printda = res.data;
              var printfa = printda.replace(/\\/g, "");
              conn.close();
              resolve(printda);
            })
            // Handle sql statement execution errors
            .catch(function (err) {
              console.log(err);
              conn.close();

            })
        })
        // Handle connection errors
        .catch(function (err) {
          console.log(err);
          conn.close();
          resolve(null);
        });

    });
 }
  ,
  getProjectDetailsProject: function (projectid) {

    return new Promise((resolve, reject) => {
      var conn = new sql.ConnectionPool(connection.getconnection().dbConfig)
      conn.connect()
        // Successfull connection
        .then(function () {
          // Create request instance, passing in connection instance
          var request = new sql.Request(conn);
          request.input('projectid', sql.VarChar, projectid);

          request.query(`with t (data) AS
          (
                      select c.project_id 'projectid',
                          d.client_id 'clientid',
                          (SELECT  CONCAT('[', STRING_AGG( '"' + paas + '"',','),']')
                                    FROM (select distinct paas from ASCEND.project_paas WHERE project_id = @projectid) d ) paas,
                          'UPDATE' 'action',
                           c.engagement_ppd 'leadPD',
                           ppduser.first_name 'ppdfirstname',
                           ppduser.last_name 'ppdlastname',
                           ppduser.user_name 'ppdusername',
                           ppduser.job_title 'ppdjobtitle',
                          (SELECT  CONCAT('[', STRING_AGG(('"' + region + '"') , ','),']')
                           FROM (select distinct region from  ascend.project_region where  project_id = @projectid ) d  ) region,
                          (SELECT  CONCAT('[', STRING_AGG(('"' + sector + '"'), ','), ']')
                                                                                        from (select distinct sector FROM ascend.project_sector where  project_id = @projectid ) d) sector,
                           c.country 'country',
                           d.revenue_millions 'revenue',
                          (SELECT  CONCAT('[', STRING_AGG(('"' + industry + '"') , ','),']')
                           FROM (select distinct industry from ascend.project_industry where  project_id = @projectid ) d) industry,
                          (SELECT  CONCAT('[', STRING_AGG(('"' + offering + '"') , ','), ']')
                           FROM ( select distinct offering from  ascend.project_offerings where  project_id = @projectid ) d ) offering,
                           (SELECT  CONCAT('[', STRING_AGG(('"' + portfolio + '"') , ','),']')
                           FROM (select distinct portfolio from ascend.project_portfolio where  project_id = @projectid ) d) portfolio,
                           c.wbs_code 'chargeCode',
                           d.client_name 'clientName',
                           (SELECT  CONCAT('[', STRING_AGG(('"' + erp + '"') , ','), ']')
                            FROM (select distinct erp from ascend.project_erp where  project_id = @projectid ) d ) erpPackage,
                           c.scope_users 'userscount',
                            c.project_name 'projectName',
                           (SELECT  CONCAT('[', STRING_AGG(('"' + project_type + '"') ,','),  ']')
                           FROM (select distinct project_type from ascend.project_type_creation where  project_id = @projectid ) d) projectType,
                           (SELECT  CONCAT('[', STRING_AGG(('"' + scope_country + '"') ,','),  ']')
                           FROM ( select distinct scope_country from ascend.project_scope_countries where  project_id = @projectid ) d) scopeCountry,
                           (SELECT  CONCAT('[', STRING_AGG(('"' + service_name + '"') ,','),  ']')
                           FROM ( select distinct service_name from ascend.projects_scope_services where  project_id = @projectid ) d) scopeservice,
                           c.makeadminflag  'makeadminflag',
                            c.addtionaladmin 'addtionalAdmin',
                            d.logo 'clientLogoPath',
                            c.project_manager  'projectManager',
                            mgruser.first_name 'mgrfirstname',
                            mgruser.last_name 'mgrlastname',
                            mgruser.user_name 'mgrusername',
                            mgruser.job_title 'mgrjobtitle',
                            d.business_details 'businessDetails',
                            (SELECT  CONCAT('[', STRING_AGG(('"' + l1 + '"') ,','),  ']')
                           FROM (select distinct l1 from ascend.project_scope_business_process where  project_id = @projectid ) d ) businessProcess,
                           (SELECT  CONCAT('[', STRING_AGG(('"' + l2 + '"') ,','),  ']')
                           FROM (select distinct l2 from ascend.project_scope_business_processl2 where  project_id = @projectid ) d ) businessProcessl2,
                           NULL 'functionalDomain',
                           NULL 'projectManagement',
                           c.description 'projectDescription',
                           (SELECT  CONCAT('[', STRING_AGG(('"' + integration_platform + '"') ,','),  ']')
                           FROM (select distinct integration_platform from ascend.project_integration_platform where  project_id = @projectid ) d)  integrationPlatform,
                           c.logo_consent_flag 'logoConsentFlag'
          from        ascend.projects c,
                      ascend.clients d,
                      ascend.users ppduser,
                      ascend.users mgruser
          where 1=1
          AND c.client_id = d.client_id
          and c.engagement_ppd = ppduser.user_id
         and c.project_manager = mgruser.user_id
                  AND (
                      (c.end_date IS NULL)
                      OR (c.end_date > GETDATE())
                      )
                  and c.project_id=@projectid
          for json path,include_null_values
          )
          select  replace(replace(replace(data,'"[','['),']"',']'),'\','') as data from t`)
            .then(function (recordsets) {
              let res = JSON.parse(JSON.stringify(recordsets.recordset[0]));
              //console.log(a);
              //console.log('Printing data.2.1');
              console.log(res.data);
              conn.close();
              var printd = res.data;
              //var printf = printd.replace(/\\\"/g, ''); ///\\/g
              if (printd)
              var printf = printd.replace(/\\"/g, '"');
              //printf= printf.replace(/\\\\/g, "\\");
              resolve(printf);
              //resolve(res.data);
            })
            // Handle sql statement execution errors
            .catch(function (err) {
              console.log(err);
              conn.close();

            })
        })
        // Handle connection errors
        .catch(function (err) {
          console.log(err);
          conn.close();
          resolve(null);
        });

    });
  },
  getProjectdocumentsid: function (projectid) {

    return new Promise((resolve, reject) => {
      var conn = new sql.ConnectionPool(connection.getconnection().dbConfig)
      conn.connect()
        // Successfull connection
        .then(function () {
          // Create request instance, passing in connection instance
          var request = new sql.Request(conn);
          request.input('myval', sql.VarChar, projectid);

          request.query(`with t(data) AS
(
    select                distinct
        CONCAT('[', STRING_AGG( '"' + REPLACE(cast(b.doc_link as NVARCHAR(MAX)),'https://amedeloitte.sharepoint.com/sites/Ascend-MSTeamsCollaboration/Shared Documents','') + '"',','),']') documents
    from
        (
            select
                doc.doc_link + '/'+ doc.file_name doc_link
                , doc.file_name
                , a.project_id
            FROM
                ascend.documents           doc
                , ascend.project_workspace a
            where
                1                 =1
                and a.entity_value=doc.name
                AND doc.type not in ('Journey Maps'
                                     ,'Personas'
                                     ,'Refine User Story'
                                     ,'User Story'
                                     ,'Process Flows'
                                     ,'KDD')
                --and a.project_id=c.project_id
                AND a.entity_name in ('ACTIVATE_DIGITAL_ORGANIZATION'
                                      ,'ANALYTICS_REPORTS'
                                      ,'BUSINESS_SOLUTIONS'
                                      ,'CONFIG_WORKBOOKS'
                                      ,'CONVERSIONS'
                                      ,'DEFINE_DIGITAL_ORGANIZATION'
                                      ,'DELIVERABLES'
                                      ,'DEPLOY'
                                      ,'INTERFACES'
                                      ,'STABILIZE'
                                      ,'TEST_AUTOMATIONS'
                                      ,'TEST_SCENARIOS'
                                      ,'DEPLOY'
                                      ,'STABILIZE'
                                      ,'REGRESSION_TESTING'
                                      ,'CONTINUE_DIGITAL_ORGANIZATION'
                                      ,'LAUNCH_JOURNEY_OCM'
                                      ,'REFINE_USER_STORIES_OCM'
                                      ,'CONSTRUCT_OCM'
                                      ,'DEPLOY_OCM'
                                      ,'CONSTRUCT_DELIVERABLES'
                                      ,'VALIDATE_DELIVERABLES'
                                      ,'LAUNCH_JOURNEY')
            UNION
            select
                doc.doc_link + '/'+ doc.file_name doc_link
                , doc.file_name
                , a.project_id
            FROM
                ascend.documents           doc
                , ascend.project_workspace a
            where
                1        =1
                and a.l2 =doc.name
                AND a.L1 = doc.category
                AND doc.type IN ('Journey Maps'
                                 ,'Personas'
                                 ,'Refine User Story'
                                 ,'User Story')
                --and a.project_id=c.project_id
                AND a.entity_name in ( 'JOURNEY_MAP'
                                      ,'PERSONAS'
                                      ,'USER_STORIES'
                                      ,'USER_STORY_LIBRARY')
            UNION
            select
                doc.doc_link + '/'+ doc.file_name doc_link
                , doc.file_name
                , a.project_id
            FROM
                ascend.documents           doc
                , ascend.project_workspace a
            where
                1           =1
                and a.l2    =doc.name
                AND a.L3    = doc.category
                AND doc.type='KDD'
                --and a.project_id=c.project_id
                AND a.entity_name in ( 'KEY_DESIGN_DECISIONS' )
            UNION
            select
                doc.doc_link + '/'+ doc.file_name doc_link
                , doc.file_name
                , a.project_id
            FROM
                ascend.documents           doc
                , ascend.project_workspace a
            where
                1                  =1
                and a.l2           =doc.name
                AND a.entity_value = doc.category
                --and a.project_id=c.project_id
                AND doc.type='Process Flows'
                AND a.entity_name in ( 'PROCESS_FLOWS' )
            UNION
            select
                doc.hosted_url doc_link
                , NULL         file_name
                , a.project_id
            FROM
                ascend.tools_accelerators_new doc
                , ascend.project_workspace    a
            where
                1                  =1
                and a.entity_value =doc.tool_name
                AND a.entity_name in ( 'DEVELOPMENT_TOOLS' )
            UNION
            select
                doc.doc_link
                , NULL file_name
                , a.project_id
            FROM
                ascend.configuration_workbooks doc
                , ascend.project_workspace     a
            where
                1                  =1
                and a.entity_value =doc.name
                and doc.module     = a.l2
                AND a.entity_name in ( 'ERP_CONFIGURATIONS' )
        )
                          b
        , ascend.projects c
    where
        c.project_id    =@myval
        and b.project_id=c.project_id for json path
        , include_null_values
)
select
    replace(replace(replace(data,'\',''),'"[','['),']"',']') as data
from
    t
`)
            .then(function (recordsets) {
              let res = JSON.parse(JSON.stringify(recordsets.recordset[0]));
              //console.log(recordsets.recordset[0]);
              //console.log('Printing data.2.1');
              console.log(res.data);
              var printdaa = res.data;
              if (printdaa)
              var printfaa = printdaa.replace(/\\"/g, "");
              conn.close();
             resolve(printdaa);
            })
            // Handle sql statement execution errors
            .catch(function (err) {
              console.log(err);
              conn.close();

            })
        })
        // Handle connection errors
        .catch(function (err) {
          console.log(err);
          conn.close();
          resolve(null);
        });

    });
  }
  ,
  uploadLogo: async function(filename,data,datalength){
    console.log('In uploadLogo: filename:'+filename+' datalength:'+datalength);
    console.log('data='+data);
    const readableInstanceStream = new Readable({
      read() {
        this.push(data);
        this.push(null);
      }
    });
    console.log('imagedata=' + readableInstanceStream);
    //const blobService = storage.createBlobService('azasusconstorageaccount','5FKV6btgea0iKfltOWVB+rMMahCJil+DcuV7YEtUnPZ9YOnxX81M8wKl9uPHZlGHv8AsCb99tr8PP4j8OkJpiQ==');
    const blobService = storage.createBlobService(connection.getconnection().storageAccountName,connection.getconnection().storageAccountKey);
    //console.log(blobService);
    
    const createContainerAsync = util.promisify(blobService.createContainerIfNotExists).bind(blobService);
    const uploadBlobAsync = util.promisify(blobService.createBlockBlobFromStream).bind(blobService);

    const containerName = connection.getconnection().storageContainerName;

    try {
        // This makes an actual service call to the Azure Storage service. 
        // Unless this call fails, the container will have been created.
        await createContainerAsync(containerName); 
        // This transfers data in the file to the blob on the service.
        var uploadResult = await uploadBlobAsync(containerName, filename, readableInstanceStream,datalength);
        //console.log('uploadResult:');
        //console.log(uploadResult);
        
        if (uploadResult) {
            console.log("blob uploaded");
        }
      }
    catch (err) {
        console.log('In uploadLogo Error');
        console.log(err.message);
    }

},
 postprojectdetailupload: function (val,projectLogoFileName) {
    return new Promise((resolve, reject) => {
      var dem = '~|';
      //var jsondata = JSON.parse(jsondata.data);
      //console.log(jsondata[0].action);

      var conn = new sql.ConnectionPool(connection.getconnection().dbConfig)
      conn.connect()
        // Successfull connection
        .then(function () {
          console.log('In connection successfull');
          console.log('In updateData');
          console.log('Val  Value: ' +val);
			var jsondata = JSON.parse(val);
            console.log(jsondata.length);
			let iExceptionFlag = false;
			let iProjclientflag = false;
			var proj_client_count ;
			//var projname = jsondata[0].projectName;
			//var clientname = jsondata[0].clientName;
			var request = new sql.Request(conn);
			request.input('projname', sql.VarChar, jsondata[0].projectName);
			request.input('clientName', sql.VarChar, jsondata[0].clientName);
			console.log('Project and client name check');
			console.log('project name :' + jsondata[0].projectName);
			console.log('clientName name :' + jsondata[0].clientName);
			request.query(`select count(1) as data
from ascend.projects
where project_name=@projname
and client_id in (select client_id from ascend.clients
where client_name=@clientName)`)
            .then(function (recordset) {
				console.log(recordset.recordset[0].data);

			let proj_client_count = JSON.parse(JSON.stringify(recordset.recordset[0].data));
			console.log( 'proj_client_count :' + proj_client_count);
			//console.log('proj_client_count[0].clientcount :' + proj_client_count.clientcount);
			//console.log('proj_client_count[0].project_count :' + proj_client_count.project_count);
			 console.log('jsondata[0].action :' + jsondata[0].action);
			console.log('Before If statement');
			 if ( proj_client_count !=0 && jsondata[0].action==	'CREATE')
			 {
				 				 console.log('names are unique');
								 iProjclientflag = true;
			 }
			else
			{
								console.log('Inside else statement');

				var request = new sql.Request(conn);
			  let transaction = new sql.Transaction(conn);
			  transaction.begin().then(async function () {
              console.log('In transaction begin successful');
              let request = new sql.Request(transaction);

              try {

                  console.log(jsondata[0].action);
                  //console.log(jsondata[0].industry.length);

                  for (var i = 0; i < jsondata.length; i++) {

                    var industries;
                    var sector;
                    var businessProcess;
                    var businessProcessl2;
                    var integrationPlatform;
                    var paas;
                    var scopeservice;
                    //var country; --unused
                    var scopeCountry;
                    var region;
                    var offering;
                    var portfolio;
                    var erpPackage;
                    var projectType;

					console.log('again jsondata[0].action : ' + jsondata[0].action);

                    if(jsondata[0].action != 'DELETE')
                    {

                    for (var j = 0; j < jsondata[i].industry.length; j++) {
                      if (j == 0) {
                        industries = jsondata[i].industry[j];
                      } else {
                        industries = industries + dem + jsondata[i].industry[j];
                      }
                    }

                    for (var j = 0; j < jsondata[i].sector.length; j++) {
                      if (j == 0) {
                        sector = jsondata[i].sector[j];
                      } else {
                        sector = sector + dem + jsondata[i].sector[j];
                      }
                    }

                    for (var j = 0; j < jsondata[i].businessProcess.length; j++) {
                      if (j == 0) {
                        businessProcess = jsondata[i].businessProcess[j];
                      } else {
                        businessProcess = businessProcess + dem + jsondata[i].businessProcess[j];
                      }
                    }

                    for (var j = 0; j < jsondata[i].businessProcessl2.length; j++) {
                      if (j == 0) {
                        businessProcessl2 = jsondata[i].businessProcessl2[j];
                      } else {
                        businessProcessl2 = businessProcessl2 + dem + jsondata[i].businessProcessl2[j];
                      }
                    }


                    for (var j = 0; j < jsondata[i].integrationPlatform.length; j++) {
                      if (j == 0) {
                        integrationPlatform = jsondata[i].integrationPlatform[j];
                      } else {
                        integrationPlatform = integrationPlatform + dem + jsondata[i].integrationPlatform[j];
                      }
                    }

                    for (var j = 0; j < jsondata[i].paas.length; j++) {
                      if (j == 0) {
                        paas = jsondata[i].paas[j];
                      } else {
                        paas = paas + dem + jsondata[i].paas[j];
                      }
                    }

                    for (var j = 0; j < jsondata[i].scopeservice.length; j++) {
                      if (j == 0) {
                        scopeservice = jsondata[i].scopeservice[j];
                      } else {
                        scopeservice = scopeservice + dem + jsondata[i].scopeservice[j];
                      }
                    }

                    for (var j = 0; j < jsondata[i].region.length; j++) {
                      if (j == 0) {
                        region = jsondata[i].region[j];
                      } else {
                        region = region + dem + jsondata[i].region[j];
                      }
                    }

                    for (var j = 0; j < jsondata[i].scopeCountry.length; j++) {
                      if (j == 0) {
                        scopeCountry = jsondata[i].scopeCountry[j];
                      } else {
                        scopeCountry = scopeCountry + dem + jsondata[i].scopeCountry[j];
                      }
                    }
                    for (var j = 0; j < jsondata[i].offering.length; j++) {
                      if (j == 0) {
                        offering = jsondata[i].offering[j];
                      } else {
                        offering = offering + dem + jsondata[i].offering[j];
                      }
                    }

                    for (var j = 0; j < jsondata[i].portfolio.length; j++) {
                      if (j == 0) {
                        portfolio = jsondata[i].portfolio[j];
                      } else {
                       portfolio = portfolio + dem + jsondata[i].portfolio[j];
                      }
                    }

                    for (var j = 0; j < jsondata[i].erpPackage.length; j++) {
                      if (j == 0) {
                        erpPackage = jsondata[i].erpPackage[j];
                      } else {
                        erpPackage = erpPackage + dem + jsondata[i].erpPackage[j];
                      }
                    }

                    for (var j = 0; j < jsondata[i].projectType.length; j++) {
                      if (j == 0) {
                        projectType = jsondata[i].projectType[j];
                      } else {
                        projectType = projectType + dem + jsondata[i].projectType[j];
                      }
                    }

                }

                    console.log('here');

                    console.log(industries);
                    console.log(sector);
                    console.log(businessProcess);
                    console.log(businessProcessl2);
                    console.log(integrationPlatform);
                    console.log(paas);
                    console.log(scopeservice);
                    //console.log(country);
                    console.log(scopeCountry);
                    console.log(region);
                    console.log(offering);
                    console.log(portfolio);
                    console.log(erpPackage);
                    console.log(projectType);

                    console.log(jsondata[i].projectid);
                    console.log(jsondata[i].clientid);

                    let v_action = jsondata[i].action;
                    if (jsondata[i].projectid)
                    {
                      v_project_id = jsondata[i].projectid;
                    }
                    else
                    {
                      v_project_id = 0;
                    };

                    if (jsondata[i].clientid)
                    {
                      v_client_id = jsondata[i].clientid;
                    }
                    else
                    {
                      v_client_id = 0;
                    };

                    let v_logoconsentflag;

                    if (jsondata[i].logoConsentFlag)
                    {
                      v_logoconsentflag = jsondata[i].logoConsentFlag;
                    }
                    else
                    {
                      v_logoconsentflag = 'N';
                    };

                    let v_projectLogoFileName =null;
                    let v_makeadminflag='Y';
                    let v_addtladmin=null;

                    let upsertstring1 = `EXEC ascend.projectDetails_create_and_update '`
                   +jsondata[i].action+`'`+`,`
                    +v_project_id+`,`
                    +v_client_id+`,`
                    +`'`+jsondata[i].businessDetails+`'`+`,`
                    +`'`+businessProcess+`'`+`,`
                    +`'`+businessProcessl2+`'`+`,`
                    +`'`+jsondata[i].chargeCode+`'`+`,`
                    +`'`+jsondata[i].clientName+`'`+`,`
                    +`'`+jsondata[i].country+`'`+`,`
                    +`'`+erpPackage+`'`+`,`
                    +`'`+jsondata[i].userscount+`'`+`,`
                    +`'`+industries+`'`+`,`
                    +`'`+jsondata[i].leadPD+`'`+`,`
                    +`'`+offering+`'`+`,`
                    +`'`+portfolio+`'`+`,`
                    +`'`+jsondata[i].projectDescription+`'`+`,`
                    +`'`+jsondata[i].projectManagement+`'`+`,`
                    +`'`+jsondata[i].projectManager+`'`+`,`
                    +`'`+jsondata[i].projectName.replace(/'/g,"''")+`'`+`,`
                    +`'`+projectType+`'`+`,`
                    +`'`+region+`'`+`,`
                    +`'`+jsondata[i].revenue+`'`+`,`
                    +`'`+sector+`'`+`,`
                    +`'`+integrationPlatform+`'`+`,`
                    +`'`+paas+`'`+`,`
                    +`'`+scopeservice+`'`+`,`
                    +`'`+scopeCountry+`'`+`,`
                    +`'`+v_makeadminflag+`'`+`,`
                    +`'`+v_addtladmin+`'`+`,`
                    +`'`+projectLogoFileName+`'`+`,`
                    +`'`+jsondata[i].mgrfirstname+`'`+`,`
                    +`'`+jsondata[i].mgrlastname+`'`+`,`
                    +`'`+jsondata[i].mgrusername+`'`+`,`
                    +`'`+jsondata[i].mgrjobtitle+`'`+`,`
                    +`'`+jsondata[i].ppdfirstname+`'`+`,`
                    +`'`+jsondata[i].ppdlastname+`'`+`,`
                    +`'`+jsondata[i].ppdusername+`'`+`,`
                    +`'`+jsondata[i].ppdjobtitle+`'`+`,`
                    +`'`+v_logoconsentflag+`'`
                    ;

                    console.log('upsertstring1: ' + upsertstring1);
                    await request.query(upsertstring1);
                };

              } catch (e) {
                iExceptionFlag = true;
                console.log('Exception:' + e);
              }
              console.log('Printing the exception flag')
              console.log(iExceptionFlag)
              if (iExceptionFlag) {
                transaction.rollback().then(function () {
                    console.log('In rollback:then');
                    conn.close();
                    resolve({
                      "MSG": "Procedure execution failed"
                    });
                  })
                  .catch(function (err) {
                    console.log('In rollback:catch');
                    conn.close();
                    resolve({
                      "MSG": "Error while rolling back transaction"
                    });
                  });
              } else {
                transaction.commit().then(function () {
                    console.log('In commit:then');
                    conn.close();
                    resolve({
                      "MSG": "SUCCESS"
                    });
                  })
                  .catch(function (err) {
                    console.log('In commit:catch');
                    conn.close();
                    resolve({
                      "MSG": "Error while commiting transaction"
                    });
                  });
              }


            })

			.catch(function (err) {
              console.log('In transaction begin catch:' + err);
              conn.close();
              resolve({
                "MSG": "Error while creating transaction object"
              });
            })
			}
			if (iProjclientflag) {
					console.log('Project name and client name already exists');
                    //conn.close();
                    resolve({
                      "MSG": "Project name and client name already exists"
                    });
			}
		});
        })

        .catch(function (err) {
          console.log('In connection catch catch:' + err)
          conn.close();
          resolve({
            "MSG": "Error while creating connection object"
         });
        })

    })
  }
}
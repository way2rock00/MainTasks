var connection = require('./connection-file');
var sql = require("mssql");

module.exports = {
    /************************************************************** *
        ALL METHODS SHOULD BE ADDED IN THIS module.exports AFTER getSampleService
        METHOD.
    /************************************************************** */
// This function connects to a SQL server, executes a SELECT statement,
// and displays the results in the console.
getUserinfouser:function(userid) {

    return new Promise((resolve, reject) => {
        var conn = new sql.ConnectionPool(connection.getconnection().dbConfig)

        conn.connect()
        // Successfull connection
        .then(function () {

          // Create request instance, passing in connection instance
          var req = new sql.Request(conn);
    req.input('myval', sql.VarChar, userid );
             req.query(`with t (data)
             as (select a.user_id as userid ,
             a.isadmin as isAscendAdmin,
             (
             SELECT
               c.project_id as projectId
             , b.id as userroleId
             , c.project_name as projectName
             , d.client_name as clientName
             , (CASE 
                      WHEN c.logo_consent_flag = 'Y' THEN '` +  connection.getconnection().storageContainerPath+`/`+ connection.getconnection().storageContainerName+`/'+d.logo 
                      ELSE '../../../assets/GoogleLogo.png'
                      END)
                      as clientLogoURL
             , d.business_details as clientDesc
             , c.erp_package as erpPackage
             , c.project_manager as manager
             , (select user_name from ascend.users where user_id=c.project_manager) managerName
             , c.engagement_ppd as leadppd
             , e.role_code as projectRole
             , ISNULL(c.logo_consent_flag,'N') as logoconsentflag
                 FROM
                 ascend.USER_ROLES b
             ,ascend.PROJECTS c
             ,ascend.CLIENTS d
             ,ascend.ROLES e
                 WHERE 1=1
             AND b.user_id = a.user_id
             AND b.project_id = c.project_id
             AND c.client_id = d.client_id
             AND (
               b.end_date IS NULL
               OR b.end_date > GETDATE()
               )
             AND e.role_id = b.role_id
             AND (c.end_date IS NULL OR c.end_date > GETDATE()) 
             /* Below condition is to avoid duplicate projects seen on landing page 
                if user is PROJECT ADMIN and ASCEND ADMIN at same time. Below condition
                will exclude such records and fetch only 1 record.*/
             AND not exists (select 1 from ascend.USER_ROLES ur
                                           ,ascend.roles rol 
             where ur.project_id = b.project_id
               and ur.user_id = b.user_id
               and rol.role_code <> 'SUPER_USER'
               and rol.role_id = ur.role_id
               and ur.id <> b.id
			     AND (
               ur.end_date IS NULL
               OR ur.end_date > GETDATE()
               )
             )  
             FOR JSON PATH, INCLUDE_NULL_VALUES) projectInfo
             from ascend.USERS a
             WHERE a.user_id = @myval
             FOR JSON AUTO
             )
             select REPLACE(data,'\','') as data from t`)

          .then(function (recordset) {
            let res = JSON.parse(JSON.stringify(recordset.recordset[0]));
            console.log(res.data);
            conn.close();
            resolve(res.data);
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
getUserlistSource:function() {

    return new Promise((resolve, reject) => {
        console.log('Debug1')
        var conn = new sql.ConnectionPool(connection.getconnection().dbConfig)
        console.log('Debug2')
        conn.connect()
        // Successfull connection
        .then(function () {
            console.log('Debug1')
          // Create request instance, passing in connection instance
          var request = new sql.Request(conn);
          //request.input('myval',sql.VarChar,projectid);
          console.log('Debug4')
          request.query(`WITH t (data)
AS (select user_id as userId, concat(first_name,' ',last_name) as userName from ascend.USERS FOR JSON PATH)
SELECT data
FROM t `)
          .then(function (recordsets) {
            console.log('Debug5')
                let res = JSON.parse(JSON.stringify(recordsets.recordset[0]));
                //console.log(a);
                //console.log('Printing data.2.1');
                console.log(res.data);
                conn.close();
				var printdetails = res.data ;
				var printfinal = printdetails.replace(/\\\"/g, '');
                //resolve(res.data);
				resolve(printfinal);
              })
            // Handle sql statement execution errors
            .catch(function (err) {
                console.log('Debug6:'+err)
                console.log(err);
                conn.close();

            })
        })
        // Handle connection errors
        .catch(function (err) {
            console.log('Debug7:'+err)
          console.log(err);
          conn.close();
          resolve(null);
        });

    });
}
   ,
   getProjectMembersProject:function(projectid) {

           return new Promise((resolve, reject) => {
               var conn = new sql.ConnectionPool(connection.getconnection().dbConfig)

               conn.connect()
               // Successfull connection
               .then(function () {

                 // Create request instance, passing in connection instance
                 var req = new sql.Request(conn);
   			  // req.input('myval', sql.VarChar, 'aknayak@deloitte.com' );
   				req.input('myval', sql.VarChar, projectid );

                 // Call mssql's query method passing in params
                 //req.query("select * from ascend.projects where project_id in (1)")

                 /*req.query(`with t(data) as  (select project_id 'project_id'
                 ,a.description + a.project_type 'type'
           from ascend.projects a
           where project_id in (1,2)
           for json path) select data from t*/
                    req.query(`with t (data) as (select c.project_id as projectId
                      ,members.action
                      ,members.userId
                      ,members.userName
                      ,members.projectRole
                      ,members.firstName
                      ,members.lastName
                      ,members.displayName
                      ,members.jobTitle
                    from ascend.PROJECTS c,
         (SELECT a.user_id as userId
                , a.user_name as userName
                       , e.role_code as projectRole
                       , b.project_id
                       , 'UPDATE' as action
                 , a.first_name firstName
                 , a.last_name lastName
                 ,a.user_name displayName
                 ,a.job_title jobTitle
         FROM               ascend.USERS a
                                  ,ascend.USER_ROLES b
                                  ,ascend.ROLES e
                    WHERE a.user_id = b.user_id
                                  AND e.role_id = b.role_id
                                  AND e.role_code <> 'SUPER_USER'
                                  AND (
                                                b.end_date IS NULL
                                                OR b.end_date > GETDATE()
                                                ) )members
         where 1=1
         AND members.project_id = c.project_id
         AND c.project_id = @myval
         FOR JSON AUTO,include_null_values )
         select data from t`)

                 .then(function (recordset) {
                     //console.log('Printing data');
                   //console.log(recordset);
                   //console.log('Printing data.1');
                   //console.log(recordset.recordset[0]);
                   //console.log('Printing data.1.11');
                   //console.log(typeof(recordset.recordset[0]));
                   //console.log('Printing data.2');
                   let res = JSON.parse(JSON.stringify(recordset.recordset[0]));
                   //console.log(a);
                   //console.log('Printing data.2.1');
                   console.log(res.data);
                   //console.log('Printing data:End');

                    var printde = res.data ;
   				//var printf = printd.replace(/\\\"/g, ''); ///\\/g
   				var printfe = printde.replace(/\\/g, "");

                   conn.close();
                   //resolve(res.data);
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
}
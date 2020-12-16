var connection = require('./connection-file');
var sql = require("mssql");
var fs = require("fs");

var api_helper = require("./team-api-helper");
var request = require('requestretry');



module.exports = {
    /************************************************************** *
        ALL METHODS SHOULD BE ADDED IN THIS module.exports AFTER getSampleService
        METHOD.
    /************************************************************** */
// This function connects to a SQL server, executes a SELECT statement,
// and displays the results in the console.
GetAuthendication:function(userrolid,emailid,idToken,clientID,clientSecret) {
	return new Promise((resolve, reject) => {
	//var hostname1 = 'test';
	var db =  process.env.WEBSITE_HOSTNAME;
	console.log('db :' + db);
	//db='azas-uscon-ascend-api-npd.azurewebsites.net';
if (db == 'azas-uscon-ascend-api-npd.azurewebsites.net')
	{
		console.log('Instance is running in local host');
		resolve(null);
	}
	else
	{
		if ( userrolid != null )
		{
		var conn = new sql.ConnectionPool(connection.getconnection().dbConfig);
			console.log('Inside the main');
			conn.connect()
			// Successfull connection
			.then(function () {
			// Create request instance, passing in connection instance
			var req = new sql.Request(conn);
				req.input('userrolid', sql.VarChar, userrolid );
				req.input('emailid', sql.VarChar, emailid );
				console.log('Inside the main 1' );
				req.query(`with t (data) as (
sele	ct user_id  from ascend.user_roles
					where id=@userrolid)
sele	ct * from t`
				).then(function (recordset) {
				//let res = JSON.parse(JSON.stringify(recordset.recordset[0].user_id));
				console.log(JSON.parse(JSON.stringify(recordset.recordset[0].data)));
				emailid = JSON.parse(JSON.stringify(recordset.recordset[0].data));
				console.log('emailid : ' + emailid);
				//resolve(null);
					})
		.catch(function(err) {
				console.log(err);
				conn.close();
				return null;
			});
			});
		}
	//// Newly adding

	console.log('idToken: ' + idToken);
    if (idToken) {
    idToken = idToken.replace('Bearer ', '');
    }
	//var clientID='';
	//var clientSecret='';
    api_helper.getGraphToken(idToken, clientID, clientSecret)
        .then(response => {

            console.log('response: ' + response);

			var options = {
                'method': 'GET',
                'url': 'https://graph.microsoft.com/v1.0/me',
                'headers': {
                    'SdkVersion': 'postman-graph/v1.0',
                    'Authorization': response
                }
            };
            console.log(options);
            request(options, function(error, response) {
                if (error) throw new Error(error);
                console.log(response.body);
                var jsonContent = JSON.parse(response.body);
                console.log("Mail:", jsonContent.mail);
				//jsonContent.mail='mohanks@deloitte.com';
                if (jsonContent.mail == emailid ) {  //added emailid
                    //res.send(null);
					console.log('authorized User');
					resolve(null);
                } else {
                    //throw new Error('Unauthorized User');
                    //res.send('Unauthorized User');
					console.log('Unauthorized User');
					resolve('Unauthorized User');
                }

            });

        })
        .catch(function(err) {
            console.log(err);
            conn.close();
            return null;
        });
	}
	});

},
gettoolbar:function() {

    return new Promise((resolve, reject) => {
        var conn = new sql.ConnectionPool(connection.getconnection().dbConfig)

        conn.connect()
        // Successfull connection
        .then(function () {

          // Create request instance, passing in connection instance
          var req = new sql.Request(conn);
             req.query(`with t(data) as (SELECT a.tool_category 'Category'
      ,(select a.name 'name'
      ,b.description 'description'
	  ,(case when b.hosted_url is null
       then 'N'
       else 'Y'
       END) launchApplicable,
b.hosted_url launchURL,
b.download_applicable DownloadApplicable,
b.download_url DownloadURL
      for json path,include_null_values) data
      from
      ascend.tools a,ascend.tools_accelerators_new b
where a.name=b.tool_name
for json path)
select data from t`)
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
},

getmarketingmaterials:function() {

    return new Promise((resolve, reject) => {
        var conn = new sql.ConnectionPool(connection.getconnection().dbConfig)

        conn.connect()
        // Successfull connection
        .then(function () {

          // Create request instance, passing in connection instance
          var req = new sql.Request(conn);
             req.query(`with t(data) as (
select name heading,description,doclink,videolink from ascend.marketing_tutorials
where phase='Marketing'
for json path,include_null_values)
select data from t

`)
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
},
getAlltabsPhaseStop:function(phasename,stopname) {

    return new Promise((resolve, reject) => {
        var conn = new sql.ConnectionPool(connection.getconnection().dbConfig)

        conn.connect()
        // Successfull connection
        .then(function () {

          // Create request instance, passing in connection instance
          var req = new sql.Request(conn);
		  req.input('phasename', sql.VarChar, phasename);
		  req.input('stopname', sql.VarChar, stopname);

             req.query(`with l3tab as
(select tab_name tabname,
tab_URL tabURL,
tab_storage tabStorage,
service_URL serviceURL,
tab_code tabCode,
tab_sequence tabSequence,
tab_key,
tab_key_value
from ascend.phase_stop_tabs a
where phase_name=@phasename
and stop_name=@stopname
), --/allTabs/Imagine/Adapt leading practices
l2tab(data) as
(
select distinct tabName,tabURL,tabStorage,serviceURL,tabCode, tabSequence,(
select CONCAT('[{', STRING_AGG( '"' +  tab_key + '" : "' + tab_key_value + '"',','),'}]')
from l3tab a
where a.tabname=b.tabname
and a.tabURL=b.tabURL
and a.tabStorage=b.tabStorage
and a.serviceURL=b.serviceURL
and a.tabCode=b.tabCode
and a.tabSequence=b.tabSequence
) tabkeys
from l3tab b
order by tabSequence
for json path,include_null_values
),
l1tab(data) as
(select data as tab from l2tab for json path,include_null_values)
select
replace(replace(replace(data,'\',''),'"[','['),']"',']') as data  from l1tab`)
          .then(function (recordset) {
            let res = JSON.parse(JSON.stringify(recordset.recordset[0]));
			 var printresults = res.data;
              //var printf = printd.replace(/\\\"/g, ''); ///\\/g
              printresults = printresults.replace(/\\/g, "");
            console.log(printresults);
            conn.close();
            resolve(printresults);
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

gettutorials:function() {

    return new Promise((resolve, reject) => {
        var conn = new sql.ConnectionPool(connection.getconnection().dbConfig)

        conn.connect()
        // Successfull connection
        .then(function () {

          // Create request instance, passing in connection instance
          var req = new sql.Request(conn);
             req.query(`with t(data) as (
select name heading,description,doclink,videolink from ascend.marketing_tutorials
where phase='Tutorials'
for json path,include_null_values)
select data from t`)
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
},

getsearchlinks:function() {

    return new Promise((resolve, reject) => {
        var conn = new sql.ConnectionPool(connection.getconnection().dbConfig)

        conn.connect()
        // Successfull connection
        .then(function () {

          // Create request instance, passing in connection instance
          var req = new sql.Request(conn);
             req.query(`with t(data) as (select a.value 'value'
     ,a.route 'route'
     ,a.L0 'L0'
     ,a.l1 'L1'
     ,a.l2 'L2'
     ,a.industry 'industry'
     ,a.sector 'sector'
     ,a.region 'region'
from
  ascend.search_links a
for json path,include_null_values )
select data from t`)
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
getClientLogoprojectId:function(projectid) {

    return new Promise((resolve, reject) => {
        var conn = new sql.ConnectionPool(connection.getconnection().dbConfig)

        conn.connect()
        // Successfull connection
        .then(function () {

          // Create request instance, passing in connection instance
          var req = new sql.Request(conn);
          req.input('myval', sql.VarChar, projectid );
          console.log('In Common - getClientLogoprojectId :');
             req.query(`with t (data) as (SELECT c.logo
FROM ascend.CLIENTS c
,ascend.PROJECTS p
WHERE p.client_id = c.client_id
AND p.end_date IS NULL
AND c.end_date IS NULL
AND p.project_id = @myval )
select data from t`)

          .then(function (recordset) {
            let res = JSON.parse(JSON.stringify(recordset.recordset[0]));
            console.log('In Common - Error 1 :');
            console.log(res.data);
            conn.close();
            resolve(res.data);
          })
          // Handle sql statement execution errors
          .catch(function (err) {
            console.log('In Common - Error 2 :');
            console.log(err);
            conn.close();
            resolve(null);
          })

        })
        // Handle connection errors
        .catch(function (err) {
          console.log('In Common - Error 3 :');
          console.log(err);
          conn.close();
          resolve(null);
        });

    });
} ,

/*
getFilterBusiness:function(userroleid) {

    return new Promise((resolve, reject) => {
        var conn = new sql.ConnectionPool(connection.getconnection().dbConfig)

        conn.connect()
        // Successfull connection
        .then(function () {
            console.log('In getFilterBusiness:2');
          // Create request instance, passing in connection instance
          var req = new sql.Request(conn);
          req.input('myval', sql.VarChar, userroleid );
             req.query(`with l6group AS
             (SELECT DISTINCT
                         business_processes_new.L1 AS l0
                   ,business_processes_new_l1.id AS l0id
                   ,business_processes_new.L2 AS l1
                   ,business_processes_new_l2.id AS l1id
                   , business_processes_new.L3
                          AS  'L2'
                                 ,
                                     business_processes_new_l3.id AS 'L2Id'

                 FROM
                       ascend.business_processes_new ,
                        ascend.business_processes_new_l1,
                        ascend.business_processes_new_l2,
                      ascend.business_processes_new_l3
                 WHERE 1=1
                     AND business_processes_new.L1 = business_processes_new_l1.l1
                     AND business_processes_new.L2 = business_processes_new_l2.l2
                     AND business_processes_new.L3 = business_processes_new_l3.l3 ),
                 l5group as (
                     select distinct  c.l0,c.l0id,c.l1,c.l1id,
                             (
                                 select L2,L2Id
                                 from l6group b
                                 where 1=1
                                 and  c.l0 = b.l0
                                  and c.l0id = b.l0id
                                  and c.l1 = b.l1
                                  and c.l1id = b.l1id
                                  for json path
                             ) L2Map
                     from
                     l6group c
                 ),
             l4group as (
                 select distinct c.L0 'L0' ,c.L0id 'L0Id',
                     (
                         select L1 'L1',L1id 'L1Id',L2Map
                         from l5group b
                         where 1=1
                         and  c.l0 = b.l0
                         and c.l0id = b.l0id
               and ( 1=1
                   AND (b.l1 IN (SELECT
                           x.l2
                         FROM
                           ascend.project_scope_business_processl2 x,
                           ascend.user_roles y,
                           ascend.roles z
                         WHERE
                           1 = 1
                             AND x.project_id = y.project_id
                             AND y.role_id = z.role_id
                             AND @myval <> 0
                             AND y.ID = @myval )
                         OR (@myval = 0)) )
                         for json path
                     ) L1Map
                 from l5group c
             )
             select * from l4group a2
             where
      ( 1=1
             AND (a2.l0 IN (SELECT
                             a.l1
                         FROM
                             ascend.project_scope_business_process a,
                             ascend.user_roles b,
                             ascend.roles c
                         WHERE
                             1 = 1
                                 AND a.project_id = b.project_id
                                 AND b.role_id = c.role_id
                                 AND @myval <> 0
                                 AND b.ID = @myval )
                         OR (@myval = 0)) )
             for json path`)
          .then(function (recordset) {
            let res = JSON.parse(JSON.stringify(recordset.recordset[0]));
            console.log(res.data);
            conn.close();
            for(key in res){
                resolve(res[key]);
              }
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
getFilterIndustry:function(userroleid) {

    return new Promise((resolve, reject) => {
        var conn = new sql.ConnectionPool(connection.getconnection().dbConfig)

        conn.connect()
        // Successfull connection
        .then(function () {

          // Create request instance, passing in connection instance
          var req = new sql.Request(conn);
          req.input('myval', sql.VarChar, userroleid );
             req.query(`with l3group as
             (SELECT distinct a.industry AS industry
                 ,b.id rownum
                 //,JSON_ARRAYAGG(JSON_OBJECT('sectorId', c.id, 'sectors', a.sector)) AS json_sector
                 , c.id, a.sector
                 FROM (
                 SELECT DISTINCT industry
                   ,sector
                 FROM ascend.INDUSTRIES_NEW
                 ) a
                 ,ascend.INDUSTRIES_NEW_I b
                 ,ascend.INDUSTRIES_NEW_S c
               WHERE 1=1
                     and a.INDUSTRY = b.INDUSTRY
                 AND c.SECTOR = a.SECTOR
                     -------------------------------
                     AND (A.INDUSTRY IN (SELECT DISTINCT ISNULL(e.industry, 'COMMON') AS industry

                 FROM
                     ASCEND.projects a,
                     ASCEND.user_roles b,
                     ASCEND.roles c,
                     ASCEND.clients d,
                     ASCEND.project_industry e
                   WHERE
                         1=1
                         AND a.project_id = b.project_id
                         AND a.project_id = e.project_id
                         AND d.client_id = a.client_id
                         AND b.role_id = c.role_id

                         AND 0 <> @myval
                         AND b.id = @myval) OR @myval=0)

                     AND (A.SECTOR IN (SELECT DISTINCT ISNULL(e.sector, 'COMMON') AS industry

                 FROM
                     ASCEND.projects a,
                     ASCEND.user_roles b,
                     ASCEND.roles c,
                     ASCEND.clients d,
                     ASCEND.project_sector e
                   WHERE
                         1=1
                         AND a.project_id = b.project_id
                         AND a.project_id = e.project_id
                         AND d.client_id = a.client_id
                         AND b.role_id = c.role_id
                         AND 0 <> @myval
                         AND b.id = @myval) OR @myval=0)

                     ) ,
             l4group as (
                 select distinct industry,rownum 'industryId',
                 (
                     select id sectorId,sector sectors from l3group B
                     where a.rownum=b.rownum
                     and a.industry=b.industry
                     for json path
                 ) sector
                  from l3group a
             )
               select * from l4group for json path`)
          .then(function (recordset) {
            let res = JSON.parse(JSON.stringify(recordset.recordset[0]));
            console.log(res.data);
            conn.close();
            for(key in res){
                resolve(res[key]);
              }
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
getFilterRegion:function(userroleid) {

 return new Promise((resolve, reject) => {
     var conn = new sql.ConnectionPool(connection.getconnection().dbConfig)

     conn.connect()
     // Successfull connection
     .then(function () {

       // Create request instance, passing in connection instance
       var req = new sql.Request(conn);
 req.input('myval', sql.VarChar, userroleid );
          req.query(`with  table_query (data)
as
(select a1.description as regionId ,a1.name as region
FROM ascend.region_new a1
     where (a1.name in
(select distinct d.region
from ascend.projects a,ascend.user_roles b,ascend.project_region d
where
1=1
and a.project_id = b.project_id
and a.project_id = d.project_id
     and @myval <> 0
     and b.id = @myval
      )) or (@myval = 0)
FOR JSON PATH)
select data from table_query
`)

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

*/
getFilterBusiness:function(userroleid,tabname) {

  return new Promise((resolve, reject) => {
      console.log('In getFilterBusiness');
      var conn = new sql.ConnectionPool(connection.getconnection().dbConfig)

      conn.connect()
      // Successfull connection
      .then(function () {
          console.log('In getFilterBusiness:2');
        // Create request instance, passing in connection instance
        var req = new sql.Request(conn);
        req.input('myval', sql.VarChar, userroleid );
              req.input('tabname', sql.VarChar, tabname );

           req.query(`with l6group AS
           (SELECT DISTINCT
                       business_processes_new.L1 AS l0
                 ,business_processes_new_l1.id AS l0id
                 ,business_processes_new.L2 AS l1
                 ,business_processes_new_l2.id AS l1id
                 , CONCAT (
                       business_processes_new.L3_prefix
                       ,' '
                       ,business_processes_new.L3
                       ) AS  'L2'
                               ,
                                   business_processes_new_l3.id AS 'L2Id'

               FROM
                     ascend.business_processes_new ,
                      ascend.business_processes_new_l1,
                      ascend.business_processes_new_l2,
                    ascend.business_processes_new_l3
               WHERE 1=1
                   AND business_processes_new.L1 = business_processes_new_l1.l1
                   AND business_processes_new.L2 = business_processes_new_l2.l2
                   AND business_processes_new.L3 = business_processes_new_l3.l3 ),
               l5group as (
                   select distinct  c.l0,c.l0id,c.l1,c.l1id,
                           (
                               select L2,L2Id
                               from l6group b
                               where 1=1
                               and  c.l0 = b.l0
                                and c.l0id = b.l0id
                                and c.l1 = b.l1
                                and c.l1id = b.l1id
                                for json path
                           ) L2Map
                   from
                   l6group c
               ),
           l4group as (
               select distinct c.L0 'L0' ,c.L0id 'L0Id',
                   (
                       select L1 'L1',L1id 'L1Id',L2Map
                       from l5group b
                       where 1=1
                       and  c.l0 = b.l0
                       and c.l0id = b.l0id
              ------------------------------- Added
             and ( 1=1
                 AND (b.l1 IN (SELECT
                         x.l2
                       FROM
                         ascend.project_scope_business_processl2 x,
                         ascend.user_roles y,
                         ascend.roles z
                       WHERE
                         1 = 1
                           AND x.project_id = y.project_id
                           AND y.role_id = z.role_id
                           AND @myval <> 0
                           AND y.ID = @myval )
                       OR (@myval = 0)) )

                ------------------------------- Added
                       for json path
                   ) L1Map
               from l5group c
           ),
l3grp (data) as (
           select * from l4group a2
           where
    ( 1=1
           AND (a2.l0 IN (SELECT
                           a.l1
                       FROM
                           ascend.project_scope_business_process a,
                           ascend.user_roles b,
                           ascend.roles c
                       WHERE
                           1 = 1
                               AND a.project_id = b.project_id
                               AND b.role_id = c.role_id
                               AND @myval <> 0
                               AND b.ID = @myval )
                       OR (@myval = 0)) )
           for json path ),
final_query(data) as (
select data businessProcessData,
(select l1 from ascend.entities where name=@tabname) as 'businessProcessL1Applicable' ,
(select l2 from ascend.entities where name=@tabname) as 'businessProcessL2Applicable' ,
(select l3 from ascend.entities where name=@tabname) as 'businessProcessL3Applicable'
from l3grp
for json path
)
SELECT data FROM final_query`)
        .then(function (recordset) {
          console.log('In getFilterBusiness:3');
          console.log(recordset.recordset[0]);
          let res = JSON.parse(JSON.stringify(recordset.recordset[0]));
          console.log(res.data);
          conn.close();
          for(key in res){
              resolve(res[key]);
            }
        })
        // Handle sql statement execution errors
        .catch(function (err) {
          console.log('In getFilterBusiness:4'+err);
          console.log(err);
          conn.close();
          resolve(null);
        })

      })
      // Handle connection errors
      .catch(function (err) {
          console.log('In getFilterBusiness:5:'+err);
        console.log(err);
        conn.close();
        resolve(null);
      });

  });
}
,
getFilterIndustry:function(userroleid,tabname) {

  return new Promise((resolve, reject) => {
      var conn = new sql.ConnectionPool(connection.getconnection().dbConfig)

      conn.connect()
      // Successfull connection
      .then(function () {

        // Create request instance, passing in connection instance
        var req = new sql.Request(conn);
        req.input('myval', sql.VarChar, userroleid );
    req.input('tabname', sql.VarChar, tabname );

           req.query(`with l3group as
           (SELECT distinct a.industry AS industry
               ,b.id rownum
               , c.id, a.sector
               FROM (
               SELECT DISTINCT industry
                 ,sector
               FROM ascend.INDUSTRIES_NEW
               ) a
               ,ascend.INDUSTRIES_NEW_I b
               ,ascend.INDUSTRIES_NEW_S c
             WHERE 1=1
                   and a.INDUSTRY = b.INDUSTRY
               AND c.SECTOR = a.SECTOR
                   -------------------------------
                   AND (A.INDUSTRY IN (SELECT DISTINCT ISNULL(e.industry, 'COMMON') AS industry

               FROM
                   ASCEND.projects a,
                   ASCEND.user_roles b,
                   ASCEND.roles c,
                   ASCEND.clients d,
                   ASCEND.project_industry e
                 WHERE
                       1=1
                       AND a.project_id = b.project_id
                       AND a.project_id = e.project_id
                       AND d.client_id = a.client_id
                       AND b.role_id = c.role_id

                       AND 0 <> @myval
                       AND b.id = @myval) OR @myval=0)

                   AND (A.SECTOR IN (SELECT DISTINCT ISNULL(e.sector, 'COMMON') AS industry

               FROM
                   ASCEND.projects a,
                   ASCEND.user_roles b,
                   ASCEND.roles c,
                   ASCEND.clients d,
                   ASCEND.project_sector e
                 WHERE
                       1=1
                       AND a.project_id = b.project_id
                       AND a.project_id = e.project_id
                       AND d.client_id = a.client_id
                       AND b.role_id = c.role_id
                       AND 0 <> @myval
                       AND b.id = @myval) OR @myval=0)

                   ) ,
           l4group(data) as (
               select distinct industry,rownum 'industryId',
               (
                   select id sectorId,sector sectors from l3group B
                   where a.rownum=b.rownum
                   and a.industry=b.industry
                   for json path
               ) sector
                from l3group a for json path
           ),
           l5grp (data) as
           (
             select data industryData,
             (select industry from ascend.entities where name=@tabname) as 'industryApplicable',
              (select sector from ascend.entities where name=@tabname) as 'sectorApplicable'
              from l4group for json path )
             select data  from l5grp`)
        .then(function (recordset) {
          let res = JSON.parse(JSON.stringify(recordset.recordset[0]));
          console.log(res.data);
          conn.close();
          for(key in res){
              resolve(res[key]);
            }
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
getFilterRegion:function(userroleid,tabname) {

  return new Promise((resolve, reject) => {
      var conn = new sql.ConnectionPool(connection.getconnection().dbConfig)

      conn.connect()
      // Successfull connection
      .then(function () {

        // Create request instance, passing in connection instance
        var req = new sql.Request(conn);
  req.input('myval', sql.VarChar, userroleid );
   req.input('tabname', sql.VarChar, tabname );

           req.query(`with  table_query (data)
 as
 (select a1.description as regionId ,a1.name as region
 FROM ascend.region_new a1
      where (a1.name in
 (select distinct d.region
 from ascend.projects a,ascend.user_roles b,ascend.project_region d
 where
 1=1
 and a.project_id = b.project_id
 and a.project_id = d.project_id
      and @myval <> 0
    and b.id = @myval
       )) or (@myval = 0)
 FOR JSON PATH),
 final_query (data) as (
 select data regionData,(select region from ascend.entities where name=@tabname) as 'regionApplicable' from table_query
 for json path,INCLUDE_NULL_VALUES)
 select data from final_query`)
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
gettoolsinfo: function (toolname) {

  return new Promise((resolve, reject) => {
    var conn = new sql.ConnectionPool(connection.getconnection().dbConfig)

    conn.connect()
      // Successfull connection
      .then(function () {

        // Create request instance, passing in connection instance
        var req = new sql.Request(conn);

    req.input('toolnameval', sql.VarChar, toolname);

        req.query(`with t (data) as
( select a.description ,(select    'Sales Decks' as 'saleslinkName',
                              a.sales_deck_link 'saleslink',
                              'Installation Documents' as 'installationlinkName',
                              a.installation_doc_link 'installationLink',
                              'Contact Us' as 'contactlinkName',
                              a.contact_link 'contactlink'
                              for json path) as  'documentation',
                              (
                                  select tech_stack_value 'techstacklinkName',
                                  techstacklink
                                  from ascend.tools_tech_stack b
                                  where a.tool_id=b.tool_id
                                  for json path
                              ) 'techStack' ,
                              (select CONCAT('[', STRING_AGG( '"' + impact_value + '"',','),']')
                               from ascend.tools_impact_value_proposition b
                               where a.tool_id=b.tool_id) impact,
                                (select CONCAT('[', STRING_AGG( '"' + b.problem_statements + '"',','),']')
                               from ascend.tools_problem_statement_summary b
                               where a.tool_id=b.tool_id) problemStatement
from ascend.tools_accelerators_new a
  where  tool_name= @toolnameval
for json path
)
select replace(replace(replace(data,'"[','['),']"',']'),'\','') as data from t
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
}
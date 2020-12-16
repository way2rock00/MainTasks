var connection = require('./connection-file');
var sql = require("mssql");

module.exports = {
    /************************************************************** *
        ALL METHODS SHOULD BE ADDED IN THIS module.exports AFTER getSampleService
        METHOD.
    /************************************************************** */
	getstablizemisc: function(userroleid, industry, sector, region, l1, l2, l3) {

        return new Promise((resolve, reject) => {
            var conn = new sql.ConnectionPool(connection.getconnection().dbConfig)

            conn.connect()
                // Successfull connection
                .then(function () {

                    // Create request instance, passing in connection instance
                    var req = new sql.Request(conn);

                    req.input('userroleid', sql.VarChar, userroleid);
                    req.input('industry', sql.VarChar, industry);
                    req.input('sector', sql.VarChar, sector);
                    req.input('region', sql.VarChar, region);
                    req.input('l1', sql.VarChar, l1);
                    req.input('l2', sql.VarChar, l2);
                    req.input('l3', sql.VarChar, l3);

                    req.query(
                      `with
                      l2group AS  (SELECT DISTINCT c.L1name L1value,c.L1description,
                                                        (SELECT d.L2name L2value,
                                                            d.L2description L2linkname,
                                                            (SELECT CONCAT(b1.doc_link, '/', b1.file_name)
                                                             FROM ASCEND.documents b1
                                                            WHERE d.L2name = b1.name
                                                              AND b1.type = 'Stabilize') L2doclink,
                                                            (CASE
                                                              WHEN ((SELECT COUNT(1)
                                                                  FROM ASCEND.project_workspace pws
                                                                    ,ASCEND.user_roles ur
                                                                  WHERE ur.ID = @userroleid
                                                                                              and ur.project_id = pws.project_id
                                                                                              AND pws.entity_name = 'STABILIZE'
                                                                  AND d.l2name = pws.L2
                                                                  AND d.l1name = pws.L1) = 1)
                                                              THEN 'Y' else 'N'
                                                              END) L2enabledflag
                                                         FROM ( select distinct L1name,L1description,L2name,L2description,L3name,region,industry,sector,process_area_l1,process_area_l2,process_area_l3 from ascend.stabilize ) d
                                                        WHERE c.l1name = d.l1name
														and (@industry = '0' OR ISNULL(d.industry,'COMMON') = 'COMMON' OR d.industry in (select ind.industry
                                                                                                                  from ascend.industries_new_i ind,
                                                                                                                  (select value from STRING_SPLIT(@industry, ',')) s
                                                                                                                  where s.value = ind.id) )
                                                          and (@sector = '0' OR ISNULL(d.sector,'COMMON') = 'COMMON' OR d.sector in (select sec.sector
                                                                                                                  from ascend.industries_new_s sec,
                                                                                                                  (select value from STRING_SPLIT(@sector, ',')) s
                                                                                                                  where s.value = sec.id) )
                                                          and (@region = '0' OR ISNULL(d.region,'Core') = 'Core' OR d.region in (select reg.NAME
                                                                                                                  from ascend.region_new reg,
                                                                                                                  (select value from STRING_SPLIT(@region, ',')) s
                                                                                                                  where s.value = reg.DESCRIPTION) )
                                                          and (@l1 = '0'  OR upper(d.process_area_l1) = 'COMMON'  OR ISNULL(d.process_area_l1,'0') = '0' OR d.process_area_l1 in (select l1.l1
                                                                                                                              from ascend.business_processes_new_l1 l1,
                                                                                                                              (select value from STRING_SPLIT(@l1, ',')) s
                                                                                                                              where s.value = l1.id))
                                                          and (@l2 = '0' OR upper(d.process_area_l2) = 'COMMON'  OR ISNULL(d.process_area_l2,'0') = '0' OR d.process_area_l2 in (select l2.l2
                                                                                                                              from ascend.business_processes_new_l2 l2,
                                                                                                                              (select value from STRING_SPLIT(@l2, ',')) s
                                                                                                                              where s.value = l2.id))
                                                          and (@l3 = '0'  OR upper(d.process_area_l3) = 'COMMON' OR ISNULL(d.process_area_l3,'0') = '0' OR d.process_area_l3 in (select l3.l3
                                                                                                                              from ascend.business_processes_new_l3 l3,
                                                                                                                              (select value from STRING_SPLIT(@l3, ',')) s
                                                                                                                              where s.value = l3.id))
                                                          FOR JSON PATH, INCLUDE_NULL_VALUES) AS L2grp
                                               FROM  ( select distinct L1name,L1description,L2name,L2description,L3name,region,industry,sector,process_area_l1,process_area_l2,process_area_l3 from ascend.stabilize )	c
                                        where 1=1
                                       and (@industry = '0' OR ISNULL(c.industry,'COMMON') = 'COMMON' OR c.industry in (select ind.industry
                                                                                                                  from ascend.industries_new_i ind,
                                                                                                                  (select value from STRING_SPLIT(@industry, ',')) s
                                                                                                                  where s.value = ind.id) )
                                                          and (@sector = '0' OR ISNULL(c.sector,'COMMON') = 'COMMON' OR c.sector in (select sec.sector
                                                                                                                  from ascend.industries_new_s sec,
                                                                                                                  (select value from STRING_SPLIT(@sector, ',')) s
                                                                                                                  where s.value = sec.id) )
                                                          and (@region = '0' OR ISNULL(c.region,'Core') = 'Core' OR c.region in (select reg.NAME
                                                                                                                  from ascend.region_new reg,
                                                                                                                  (select value from STRING_SPLIT(@region, ',')) s
                                                                                                                  where s.value = reg.DESCRIPTION) )
                                                          and (@l1 = '0'  OR upper(c.process_area_l1) = 'COMMON'  OR ISNULL(c.process_area_l1,'0') = '0' OR c.process_area_l1 in (select l1.l1
                                                                                                                              from ascend.business_processes_new_l1 l1,
                                                                                                                              (select value from STRING_SPLIT(@l1, ',')) s
                                                                                                                              where s.value = l1.id))
                                                          and (@l2 = '0' OR upper(c.process_area_l2) = 'COMMON'  OR ISNULL(c.process_area_l2,'0') = '0' OR c.process_area_l2 in (select l2.l2
                                                                                                                              from ascend.business_processes_new_l2 l2,
                                                                                                                              (select value from STRING_SPLIT(@l2, ',')) s
                                                                                                                              where s.value = l2.id))
                                                          and (@l3 = '0'  OR upper(c.process_area_l3) = 'COMMON' OR ISNULL(c.process_area_l3,'0') = '0' OR c.process_area_l3 in (select l3.l3
                                                                                                                              from ascend.business_processes_new_l3 l3,
                                                                                                                              (select value from STRING_SPLIT(@l3, ',')) s
                                                                                                                              where s.value = l3.id))                             
                                                       )
                          ,
                      t AS (SELECT e.L1value L1value, null L1linkname,
                                                   null L1doclink,
                                                   (CASE WHEN ((SELECT COUNT(1)
                                                        FROM ASCEND.project_workspace pws
                                                          ,ASCEND.user_roles ur
                                                        WHERE ur.ID = @userroleid
                                                                          AND ur.project_id = pws.project_id
                                                                          AND pws.entity_name = 'STABILIZE'
                                                        AND e.L1value = pws.L1) >= 1)
                                                     THEN 'Y' else 'N'
                                                     END) L1enabledflag,
                                                  e.l2grp 'L2grp'
                                              FROM l2group e
                                              )
                                      SELECT * FROM t FOR JSON PATH, INCLUDE_NULL_VALUES`
                    )

                        .then(function (recordset) {
                            let res = JSON.parse(JSON.stringify(recordset.recordset[0]));
                            console.log(res.data);
                            conn.close();
                            //resolve(res.data);
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
    },
	getregressiontest: function(userroleid, industry, sector, region, l1, l2, l3) {
        return new Promise((resolve, reject) => {
            var conn = new sql.ConnectionPool(connection.getconnection().dbConfig)

            conn.connect()
                // Successfull connection
                .then(function () {

                    // Create request instance, passing in connection instance
                    var req = new sql.Request(conn);

                    req.input('userroleid', sql.VarChar, userroleid);
                    req.input('industry', sql.VarChar, industry);
                    req.input('sector', sql.VarChar, sector);
                    req.input('region', sql.VarChar, region);
                    req.input('l1', sql.VarChar, l1);
                    req.input('l2', sql.VarChar, l2);
                    req.input('l3', sql.VarChar, l3);

                    req.query(
                      `with
                      l2group AS  (SELECT DISTINCT c.L1name L1value,c.L1description,
                                                        (SELECT d.L2name L2value,
                                                            d.L2description L2linkname,
                                                            (SELECT CONCAT(b1.doc_link, '/', b1.file_name)
                                                             FROM ASCEND.documents b1
                                                            WHERE d.L2name = b1.name
                                                              AND b1.type = 'Regression Testing') L2doclink,
                                                            (CASE
                                                              WHEN ((SELECT COUNT(1)
                                                                  FROM ASCEND.project_workspace pws
                                                                    ,ASCEND.user_roles ur
                                                                  WHERE ur.ID = @userroleid
                                                                                              and ur.project_id = pws.project_id
                                                                                              AND pws.entity_name = 'REGRESSION_TESTING'
                                                                  AND d.l2name = pws.L2
                                                                  AND d.l1name = pws.L1) = 1)
                                                              THEN 'Y' else 'N'
                                                              END) L2enabledflag
                                                         FROM ( select distinct L1name,L1description,L2name,L2description,L3name,region,industry,sector,process_area_l1,process_area_l2,process_area_l3 from ascend.regression_test ) d
                                                        WHERE c.l1name = d.l1name
														 and (@industry = '0' OR ISNULL(d.industry,'COMMON') = 'COMMON' OR d.industry in (select ind.industry
                                                                                                                  from ascend.industries_new_i ind,
                                                                                                                  (select value from STRING_SPLIT(@industry, ',')) s
                                                                                                                  where s.value = ind.id) )
                                                          and (@sector = '0' OR ISNULL(d.sector,'COMMON') = 'COMMON' OR d.sector in (select sec.sector
                                                                                                                  from ascend.industries_new_s sec,
                                                                                                                  (select value from STRING_SPLIT(@sector, ',')) s
                                                                                                                  where s.value = sec.id) )
                                                          and (@region = '0' OR ISNULL(d.region,'Core') = 'Core' OR d.region in (select reg.NAME
                                                                                                                  from ascend.region_new reg,
                                                                                                                  (select value from STRING_SPLIT(@region, ',')) s
                                                                                                                  where s.value = reg.DESCRIPTION) )
                                                          and (@l1 = '0'  OR upper(d.process_area_l1) = 'COMMON'  OR ISNULL(d.process_area_l1,'0') = '0' OR d.process_area_l1 in (select l1.l1
                                                                                                                              from ascend.business_processes_new_l1 l1,
                                                                                                                              (select value from STRING_SPLIT(@l1, ',')) s
                                                                                                                              where s.value = l1.id))
                                                          and (@l2 = '0' OR upper(d.process_area_l2) = 'COMMON'  OR ISNULL(d.process_area_l2,'0') = '0' OR d.process_area_l2 in (select l2.l2
                                                                                                                              from ascend.business_processes_new_l2 l2,
                                                                                                                              (select value from STRING_SPLIT(@l2, ',')) s
                                                                                                                              where s.value = l2.id))
                                                          and (@l3 = '0'  OR upper(d.process_area_l3) = 'COMMON' OR ISNULL(d.process_area_l3,'0') = '0' OR d.process_area_l3 in (select l3.l3
                                                                                                                              from ascend.business_processes_new_l3 l3,
                                                                                                                              (select value from STRING_SPLIT(@l3, ',')) s
                                                                                                                              where s.value = l3.id))        
                                                          FOR JSON PATH, INCLUDE_NULL_VALUES) AS L2grp
                                               FROM  ( select distinct L1name,L1description,L2name,L2description,L3name,region,industry,sector,process_area_l1,process_area_l2,process_area_l3 from ascend.regression_test )	c
                                        where 1=1
                                       and (@industry = '0' OR ISNULL(c.industry,'COMMON') = 'COMMON' OR c.industry in (select ind.industry
                                                                                                                  from ascend.industries_new_i ind,
                                                                                                                  (select value from STRING_SPLIT(@industry, ',')) s
                                                                                                                  where s.value = ind.id) )
                                                          and (@sector = '0' OR ISNULL(c.sector,'COMMON') = 'COMMON' OR c.sector in (select sec.sector
                                                                                                                  from ascend.industries_new_s sec,
                                                                                                                  (select value from STRING_SPLIT(@sector, ',')) s
                                                                                                                  where s.value = sec.id) )
                                                          and (@region = '0' OR ISNULL(c.region,'Core') = 'Core' OR c.region in (select reg.NAME
                                                                                                                  from ascend.region_new reg,
                                                                                                                  (select value from STRING_SPLIT(@region, ',')) s
                                                                                                                  where s.value = reg.DESCRIPTION) )
                                                          and (@l1 = '0'  OR upper(c.process_area_l1) = 'COMMON'  OR ISNULL(c.process_area_l1,'0') = '0' OR c.process_area_l1 in (select l1.l1
                                                                                                                              from ascend.business_processes_new_l1 l1,
                                                                                                                              (select value from STRING_SPLIT(@l1, ',')) s
                                                                                                                              where s.value = l1.id))
                                                          and (@l2 = '0' OR upper(c.process_area_l2) = 'COMMON'  OR ISNULL(c.process_area_l2,'0') = '0' OR c.process_area_l2 in (select l2.l2
                                                                                                                              from ascend.business_processes_new_l2 l2,
                                                                                                                              (select value from STRING_SPLIT(@l2, ',')) s
                                                                                                                              where s.value = l2.id))
                                                          and (@l3 = '0'  OR upper(c.process_area_l3) = 'COMMON' OR ISNULL(c.process_area_l3,'0') = '0' OR c.process_area_l3 in (select l3.l3
                                                                                                                              from ascend.business_processes_new_l3 l3,
                                                                                                                              (select value from STRING_SPLIT(@l3, ',')) s
                                                                                                                              where s.value = l3.id))                             
                                                       )
                          ,
                      t AS (SELECT e.L1value L1value, null L1linkname,
                                                   null L1doclink,
                                                   (CASE WHEN ((SELECT COUNT(1)
                                                        FROM ASCEND.project_workspace pws
                                                          ,ASCEND.user_roles ur
                                                        WHERE ur.ID = @userroleid
                                                                          AND ur.project_id = pws.project_id
                                                                          AND pws.entity_name = 'REGRESSION_TESTING'
                                                        AND e.L1value = pws.L1) >= 1)
                                                     THEN 'Y' else 'N'
                                                     END) L1enabledflag,
                                                  e.l2grp 'L2grp'
                                              FROM l2group e
                                              )
                                      SELECT * FROM t FOR JSON PATH, INCLUDE_NULL_VALUES`
                    )

                        .then(function (recordset) {
                            let res = JSON.parse(JSON.stringify(recordset.recordset[0]));
                            console.log(res.data);
                            conn.close();
                            //resolve(res.data);
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
    },
	getquarterlyinsights: function(userroleid, industry, sector, region, l1, l2, l3) {

        return new Promise((resolve, reject) => {
            var conn = new sql.ConnectionPool(connection.getconnection().dbConfig)

            conn.connect()
                // Successfull connection
                .then(function () {

                    // Create request instance, passing in connection instance
                    var req = new sql.Request(conn);

                    req.input('userroleid', sql.VarChar, userroleid);
                    req.input('industry', sql.VarChar, industry);
                    req.input('sector', sql.VarChar, sector);
                    req.input('region', sql.VarChar, region);
                    req.input('l1', sql.VarChar, l1);
                    req.input('l2', sql.VarChar, l2);
                    req.input('l3', sql.VarChar, l3);

                    req.query(
                      `with
                      l2group AS  (SELECT DISTINCT c.L1name L1value,c.L1description,
                                                        (SELECT d.L2name L2value,
                                                            d.L2description L2linkname,
                                                            (SELECT CONCAT(b1.doc_link, '/', b1.file_name)
                                                             FROM ASCEND.documents b1
                                                            WHERE d.L2name = b1.name
                                                              AND b1.type = 'Quarterly Insights') L2doclink,
                                                            (CASE
                                                              WHEN ((SELECT COUNT(1)
                                                                  FROM ASCEND.project_workspace pws
                                                                    ,ASCEND.user_roles ur
                                                                  WHERE ur.ID = @userroleid
                                                                                              and ur.project_id = pws.project_id
                                                                                              AND pws.entity_name = 'QUARTERLY_INSIGHTS'
                                                                  AND d.l2name = pws.L2
                                                                  AND d.l1name = pws.L1) = 1)
                                                              THEN 'Y' else 'N'
                                                              END) L2enabledflag
                                                         FROM ( select distinct L1name,L1description,L2name,L2description,L3name,region,industry,sector,process_area_l1,process_area_l2,process_area_l3  from ascend.quarterly_insights ) d
                                                        WHERE c.l1name = d.l1name
														     and (@industry = '0' OR ISNULL(d.industry,'COMMON') = 'COMMON' OR d.industry in (select ind.industry
                                                                                                                  from ascend.industries_new_i ind,
                                                                                                                  (select value from STRING_SPLIT(@industry, ',')) s
                                                                                                                  where s.value = ind.id) )
                                                          and (@sector = '0' OR ISNULL(d.sector,'COMMON') = 'COMMON' OR d.sector in (select sec.sector
                                                                                                                  from ascend.industries_new_s sec,
                                                                                                                  (select value from STRING_SPLIT(@sector, ',')) s
                                                                                                                  where s.value = sec.id) )
                                                          and (@region = '0' OR ISNULL(d.region,'Core') = 'Core' OR d.region in (select reg.NAME
                                                                                                                  from ascend.region_new reg,
                                                                                                                  (select value from STRING_SPLIT(@region, ',')) s
                                                                                                                  where s.value = reg.DESCRIPTION) )
                                                          and (@l1 = '0'  OR upper(d.process_area_l1) = 'COMMON'  OR ISNULL(d.process_area_l1,'0') = '0' OR d.process_area_l1 in (select l1.l1
                                                                                                                              from ascend.business_processes_new_l1 l1,
                                                                                                                              (select value from STRING_SPLIT(@l1, ',')) s
                                                                                                                              where s.value = l1.id))
                                                          and (@l2 = '0' OR upper(d.process_area_l2) = 'COMMON'  OR ISNULL(d.process_area_l2,'0') = '0' OR d.process_area_l2 in (select l2.l2
                                                                                                                              from ascend.business_processes_new_l2 l2,
                                                                                                                              (select value from STRING_SPLIT(@l2, ',')) s
                                                                                                                              where s.value = l2.id))
                                                          and (@l3 = '0'  OR upper(d.process_area_l3) = 'COMMON' OR ISNULL(d.process_area_l3,'0') = '0' OR d.process_area_l3 in (select l3.l3
                                                                                                                              from ascend.business_processes_new_l3 l3,
                                                                                                                              (select value from STRING_SPLIT(@l3, ',')) s
                                                                                                                              where s.value = l3.id)) 
                                                          FOR JSON PATH, INCLUDE_NULL_VALUES) AS L2grp
                                               FROM  ( select distinct L1name,L1description,L2name,L2description,L3name,region,industry,sector,process_area_l1,process_area_l2,process_area_l3  from ascend.quarterly_insights )	c
                                        where 1=1
                                       and (@industry = '0' OR ISNULL(c.industry,'COMMON') = 'COMMON' OR c.industry in (select ind.industry
                                                                                                                  from ascend.industries_new_i ind,
                                                                                                                  (select value from STRING_SPLIT(@industry, ',')) s
                                                                                                                  where s.value = ind.id) )
                                                          and (@sector = '0' OR ISNULL(c.sector,'COMMON') = 'COMMON' OR c.sector in (select sec.sector
                                                                                                                  from ascend.industries_new_s sec,
                                                                                                                  (select value from STRING_SPLIT(@sector, ',')) s
                                                                                                                  where s.value = sec.id) )
                                                          and (@region = '0' OR ISNULL(c.region,'Core') = 'Core' OR c.region in (select reg.NAME
                                                                                                                  from ascend.region_new reg,
                                                                                                                  (select value from STRING_SPLIT(@region, ',')) s
                                                                                                                  where s.value = reg.DESCRIPTION) )
                                                          and (@l1 = '0'  OR upper(c.process_area_l1) = 'COMMON'  OR ISNULL(c.process_area_l1,'0') = '0' OR c.process_area_l1 in (select l1.l1
                                                                                                                              from ascend.business_processes_new_l1 l1,
                                                                                                                              (select value from STRING_SPLIT(@l1, ',')) s
                                                                                                                              where s.value = l1.id))
                                                          and (@l2 = '0' OR upper(c.process_area_l2) = 'COMMON'  OR ISNULL(c.process_area_l2,'0') = '0' OR c.process_area_l2 in (select l2.l2
                                                                                                                              from ascend.business_processes_new_l2 l2,
                                                                                                                              (select value from STRING_SPLIT(@l2, ',')) s
                                                                                                                              where s.value = l2.id))
                                                          and (@l3 = '0'  OR upper(c.process_area_l3) = 'COMMON' OR ISNULL(c.process_area_l3,'0') = '0' OR c.process_area_l3 in (select l3.l3
                                                                                                                              from ascend.business_processes_new_l3 l3,
                                                                                                                              (select value from STRING_SPLIT(@l3, ',')) s
                                                                                                                              where s.value = l3.id))                             
                                                       )
                          ,
                      t AS (SELECT e.L1value L1value, null L1linkname,
                                                   null L1doclink,
                                                   (CASE WHEN ((SELECT COUNT(1)
                                                        FROM ASCEND.project_workspace pws
                                                          ,ASCEND.user_roles ur
                                                        WHERE ur.ID = @userroleid
                                                                          AND ur.project_id = pws.project_id
                                                                          AND pws.entity_name = 'QUARTERLY_INSIGHTS'
                                                        AND e.L1value = pws.L1) >= 1)
                                                     THEN 'Y' else 'N'
                                                     END) L1enabledflag,
                                                  e.l2grp 'L2grp'
                                              FROM l2group e
                                              )
                                      SELECT * FROM t FOR JSON PATH, INCLUDE_NULL_VALUES`
                    )

                        .then(function (recordset) {
                            let res = JSON.parse(JSON.stringify(recordset.recordset[0]));
                            console.log(res.data);
                            conn.close();
                            //resolve(res.data);
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
    },
  poststablizemisc:function(jsondata,projectId) {
        return new Promise((resolve, reject) =>
        {
          console.log('In updateData');
          console.log(jsondata);
          console.log('projectId:'+projectId);
    
          var conn = new sql.ConnectionPool(connection.getconnection().dbConfig)
          conn.connect()
          // Successful connection
          .then(function () {
            console.log('In connection successful');
    
            let transaction = new sql.Transaction(conn);
            transaction.begin().then(async function(){
              console.log('In transaction begin successful');
              let request = new sql.Request(transaction);
              let iExceptionFlag = false;
              let errorMessage = '';
                      try{
                        let deleteString = 'DELETE FROM '+connection.schemaName+'.PROJECT_WORKSPACE where project_id = '+projectId+` AND entity_name = 'STABILIZE'`;
                        console.log('deleteString:'+deleteString);
                        await request.query(deleteString);
                        console.log('jsondata.length:'+jsondata.length);
                        for (var i = 0; i < jsondata.length; i++) {
                          tabContent = jsondata[i].tabContent;
                          console.log('tabContent.length:'+tabContent.length);
                          for (var l = 0; l < tabContent.length; l++) {
                            L2grp = tabContent[l].L2grp;
                            L1value = tabContent[l].L1value;
                            for (var j = 0; j < L2grp.length; j++) {
                              L2value = L2grp[j].L2value;
                                if (L2grp[j].L2enabledflag == 'Y') {
                                      console.log('L1value:'+L1value+' L2value'+L2value);
                                      let entityName = 'STABILIZE';
                                      let entityTable = 'STABILIZE';
                                      let seperator = `','`;
                                      let endBlock = `')`;
                                      let insertString = `insert into `+connection.schemaName+`.PROJECT_WORKSPACE (project_id, entity_name, entity_value, ENTITY_TABLE, L2, L1) VALUES ('`+projectId+
                                                          seperator+entityName+seperator+L2grp[j].L2value+seperator+entityTable+seperator+L2value+seperator+L1value+endBlock;
                                      console.log('insertString:'+insertString);
                                      await request.query(insertString);
                                  }
                                
                              }
                            }
                        }
                    }
                    catch(e){
                      iExceptionFlag = true;
                      errorMessage = e;
                      console.log('Exception:'+e);
                    }
                    console.log(iExceptionFlag)
                    if(iExceptionFlag){
                      transaction.rollback().then(function(){
                        console.log('In rollback:then');
                        conn.close();
                        resolve({"MSG":"Error while inserting data into database:"+errorMessage});
                      })
                      .catch(function(err){
                        console.log('In rollback:catch');
                        conn.close();
                        resolve({"MSG":"Error while rolling back transaction:"+errorMessage});
                      });
                    }else{
                      transaction.commit().then(function(){
                        console.log('In commit:then');
                        conn.close();
                        resolve({"MSG":"SUCCESS"});
                      })
                      .catch(function(err){
                        console.log('In commit:catch');
                        conn.close();
                        resolve({"MSG":"Error while commiting transaction:"+errorMessage});
                      });
                    }
    
    
              })
              .catch(function(err){
                console.log('In transaction begin catch:'+err);
                conn.close();
                resolve({"MSG":"Error while creating transaction object:"+errorMessage});
              })
    
          })
          .catch(function(err){
            console.log('In connection catch catch:'+err)
            conn.close();
            resolve({"MSG":"Error while creating connection object:"+errorMessage});
          })
    
      })
    },    
  postregressiontest:function(jsondata,projectId) {
        return new Promise((resolve, reject) =>
        {
          console.log('In updateData');
          console.log(jsondata);
          console.log('projectId:'+projectId);
    
          var conn = new sql.ConnectionPool(connection.getconnection().dbConfig)
          conn.connect()
          // Successful connection
          .then(function () {
            console.log('In connection successful');
    
            let transaction = new sql.Transaction(conn);
            transaction.begin().then(async function(){
              console.log('In transaction begin successful');
              let request = new sql.Request(transaction);
              let iExceptionFlag = false;
              let errorMessage = '';
                      try{
                        let deleteString = 'DELETE FROM '+connection.schemaName+'.PROJECT_WORKSPACE where project_id = '+projectId+` AND entity_name = 'REGRESSION_TESTING'`;
                        console.log('deleteString:'+deleteString);
                        await request.query(deleteString);
                        console.log('jsondata.length:'+jsondata.length);
                        for (var i = 0; i < jsondata.length; i++) {
                          tabContent = jsondata[i].tabContent;
                          console.log('tabContent.length:'+tabContent.length);
                          for (var l = 0; l < tabContent.length; l++) {
                            L2grp = tabContent[l].L2grp;
                            L1value = tabContent[l].L1value;
                            for (var j = 0; j < L2grp.length; j++) {
                              L2value = L2grp[j].L2value;
                                if (L2grp[j].L2enabledflag == 'Y') {
                                      console.log('L1value:'+L1value+' L2value'+L2value);
                                      let entityName = 'REGRESSION_TESTING';
                                      let entityTable = 'REGRESSION_TEST';
                                      let seperator = `','`;
                                      let endBlock = `')`;
                                      let insertString = `insert into `+connection.schemaName+`.PROJECT_WORKSPACE (project_id, entity_name, entity_value, ENTITY_TABLE, L2, L1) VALUES ('`+projectId+
                                                          seperator+entityName+seperator+L2grp[j].L2value+seperator+entityTable+seperator+L2value+seperator+L1value+endBlock;
                                      console.log('insertString:'+insertString);
                                      await request.query(insertString);
                                  }
                                
                              }
                            }
                        }
                    }
                    catch(e){
                      iExceptionFlag = true;
                      errorMessage = e;
                      console.log('Exception:'+e);
                    }
                    console.log(iExceptionFlag)
                    if(iExceptionFlag){
                      transaction.rollback().then(function(){
                        console.log('In rollback:then');
                        conn.close();
                        resolve({"MSG":"Error while inserting data into database:"+errorMessage});
                      })
                      .catch(function(err){
                        console.log('In rollback:catch');
                        conn.close();
                        resolve({"MSG":"Error while rolling back transaction:"+errorMessage});
                      });
                    }else{
                      transaction.commit().then(function(){
                        console.log('In commit:then');
                        conn.close();
                        resolve({"MSG":"SUCCESS"});
                      })
                      .catch(function(err){
                        console.log('In commit:catch');
                        conn.close();
                        resolve({"MSG":"Error while commiting transaction:"+errorMessage});
                      });
                    }
    
    
              })
              .catch(function(err){
                console.log('In transaction begin catch:'+err);
                conn.close();
                resolve({"MSG":"Error while creating transaction object:"+errorMessage});
              })
    
          })
          .catch(function(err){
            console.log('In connection catch catch:'+err)
            conn.close();
            resolve({"MSG":"Error while creating connection object:"+errorMessage});
          })
    
      })
    },    
  postquarterlyinsights:function(jsondata,projectId) {
        return new Promise((resolve, reject) =>
        {
          console.log('In updateData');
          console.log(jsondata);
          console.log('projectId:'+projectId);
    
          var conn = new sql.ConnectionPool(connection.getconnection().dbConfig)
          conn.connect()
          // Successful connection
          .then(function () {
            console.log('In connection successful');
    
            let transaction = new sql.Transaction(conn);
            transaction.begin().then(async function(){
              console.log('In transaction begin successful');
              let request = new sql.Request(transaction);
              let iExceptionFlag = false;
              let errorMessage = '';
                      try{
                        let deleteString = 'DELETE FROM '+connection.schemaName+'.PROJECT_WORKSPACE where project_id = '+projectId+` AND entity_name = 'QUARTERLY_INSIGHTS'`;
                        console.log('deleteString:'+deleteString);
                        await request.query(deleteString);
                        console.log('jsondata.length:'+jsondata.length);
                        for (var i = 0; i < jsondata.length; i++) {
                          tabContent = jsondata[i].tabContent;
                          console.log('tabContent.length:'+tabContent.length);
                          for (var l = 0; l < tabContent.length; l++) {
                            L2grp = tabContent[l].L2grp;
                            L1value = tabContent[l].L1value;
                            for (var j = 0; j < L2grp.length; j++) {
                              L2value = L2grp[j].L2value;
                                if (L2grp[j].L2enabledflag == 'Y') {
                                      console.log('L1value:'+L1value+' L2value'+L2value);
                                      let entityName = 'QUARTERLY_INSIGHTS';
                                      let entityTable = 'QUARTERLY_INSIGHTS';
                                      let seperator = `','`;
                                      let endBlock = `')`;
                                      let insertString = `insert into `+connection.schemaName+`.PROJECT_WORKSPACE (project_id, entity_name, entity_value, ENTITY_TABLE, L2, L1) VALUES ('`+projectId+
                                                          seperator+entityName+seperator+L2grp[j].L2value+seperator+entityTable+seperator+L2value+seperator+L1value+endBlock;
                                      console.log('insertString:'+insertString);
                                      await request.query(insertString);
                                  }
                                
                              }
                            }
                        }
                    }
                    catch(e){
                      iExceptionFlag = true;
                      errorMessage = e;
                      console.log('Exception:'+e);
                    }
                    console.log(iExceptionFlag)
                    if(iExceptionFlag){
                      transaction.rollback().then(function(){
                        console.log('In rollback:then');
                        conn.close();
                        resolve({"MSG":"Error while inserting data into database:"+errorMessage});
                      })
                      .catch(function(err){
                        console.log('In rollback:catch');
                        conn.close();
                        resolve({"MSG":"Error while rolling back transaction:"+errorMessage});
                      });
                    }else{
                      transaction.commit().then(function(){
                        console.log('In commit:then');
                        conn.close();
                        resolve({"MSG":"SUCCESS"});
                      })
                      .catch(function(err){
                        console.log('In commit:catch');
                        conn.close();
                        resolve({"MSG":"Error while commiting transaction:"+errorMessage});
                      });
                    }
    
    
              })
              .catch(function(err){
                console.log('In transaction begin catch:'+err);
                conn.close();
                resolve({"MSG":"Error while creating transaction object:"+errorMessage});
              })
    
          })
          .catch(function(err){
            console.log('In connection catch catch:'+err)
            conn.close();
            resolve({"MSG":"Error while creating connection object:"+errorMessage});
          })
    
      })
    },
	
	
getContinueDigitalOrg: function(userroleid, industry, sector, region, l1, l2, l3) {

  return new Promise((resolve, reject) => {
      var conn = new sql.ConnectionPool(connection.getconnection().dbConfig)

      conn.connect()
          // Successfull connection
          .then(function () {

              // Create request instance, passing in connection instance
              var req = new sql.Request(conn);

              req.input('userroleid', sql.VarChar, userroleid);
              req.input('industry', sql.VarChar, industry);
              req.input('sector', sql.VarChar, sector);
              req.input('region', sql.VarChar, region);
              req.input('l1', sql.VarChar, l1);
              req.input('l2', sql.VarChar, l2);
              req.input('l3', sql.VarChar, l3);

              req.query(
                `with
                l2group AS  (SELECT DISTINCT c.L1name L1value,
                                            (SELECT d.L2name L2value,
                                                d.L2description L2linkname,
                                                (SELECT CONCAT(b1.doc_link, '/', b1.file_name)
                                                 FROM ASCEND.documents b1
                                                WHERE d.L2name = b1.name
                                                  AND b1.type = 'Continue Digital Org') L2doclink,
                                                (CASE
                                                  WHEN ((SELECT COUNT(1)
                                                      FROM ASCEND.project_workspace pws
                                                        ,ASCEND.user_roles ur
                                                      WHERE ur.ID = @userroleid
                                                                                  and ur.project_id = pws.project_id
                                                                                  AND pws.entity_name = 'CONTINUE_DIGITAL_ORGANIZATION'
                                                      AND d.l2name = pws.L2
                                                      AND d.l1name = pws.L1) = 1)
                                                  THEN 'Y' else  'N'
                                                  END) L2enabledflag
                                             FROM ( select distinct L1name,L1description,L2name,L2description,L3name,region,industry,sector,process_area_l1,process_area_l2,process_area_l3,phase,stop from ascend.deliverables where stop='Continue digital organization'
                                            AND phase= 'Run') d
                                            WHERE c.l1name = d.l1name
                                            AND d.stop='Continue digital organization'
                                            AND d.phase= 'Run'
											and (@industry = '0' OR ISNULL(d.industry,'COMMON') = 'COMMON' OR d.industry in (select ind.industry
                                                                                                      from ascend.industries_new_i ind,
                                                                                                      (select value from STRING_SPLIT(@industry, ',')) s
                                                                                                      where s.value = ind.id) )
                                              and (@sector = '0' OR ISNULL(d.sector,'COMMON') = 'COMMON' OR d.sector in (select sec.sector
                                                                                                      from ascend.industries_new_s sec,
                                                                                                      (select value from STRING_SPLIT(@sector, ',')) s
                                                                                                      where s.value = sec.id) )
                                              and (@region = '0' OR ISNULL(d.region,'Core') = 'Core' OR d.region in (select reg.NAME
                                                                                                      from ascend.region_new reg,
                                                                                                      (select value from STRING_SPLIT(@region, ',')) s
                                                                                                      where s.value = reg.DESCRIPTION) )
                                              and (@l1 = '0'  OR upper(d.process_area_l1) = 'COMMON'  OR ISNULL(d.process_area_l1,'0') = '0' OR d.process_area_l1 in (select l1.l1
                                                                                                                  from ascend.business_processes_new_l1 l1,
                                                                                                                  (select value from STRING_SPLIT(@l1, ',')) s
                                                                                                                  where s.value = l1.id))
                                              and (@l2 = '0' OR upper(d.process_area_l2) = 'COMMON'  OR ISNULL(d.process_area_l2,'0') = '0' OR d.process_area_l2 in (select l2.l2
                                                                                                                  from ascend.business_processes_new_l2 l2,
                                                                                                                  (select value from STRING_SPLIT(@l2, ',')) s
                                                                                                                  where s.value = l2.id))
                                              and (@l3 = '0'  OR upper(d.process_area_l3) = 'COMMON' OR ISNULL(d.process_area_l3,'0') = '0' OR d.process_area_l3 in (select l3.l3
                                                                                                                  from ascend.business_processes_new_l3 l3,
                                                                                                                  (select value from STRING_SPLIT(@l3, ',')) s
                                                                                                                  where s.value = l3.id))
                                              FOR JSON PATH, INCLUDE_NULL_VALUES) AS L2grp
                                   FROM ( select distinct L1name,L1description,L2name,L2description,L3name,region,industry,sector,process_area_l1,process_area_l2,process_area_l3 from ascend.deliverables where stop='Continue digital organization'
                                            AND phase= 'Run') c
                           where 1=1
                           and (@industry = '0' OR ISNULL(c.industry,'COMMON') = 'COMMON' OR c.industry in (select ind.industry
                                                                                                      from ascend.industries_new_i ind,
                                                                                                      (select value from STRING_SPLIT(@industry, ',')) s
                                                                                                      where s.value = ind.id) )
                                              and (@sector = '0' OR ISNULL(c.sector,'COMMON') = 'COMMON' OR c.sector in (select sec.sector
                                                                                                      from ascend.industries_new_s sec,
                                                                                                      (select value from STRING_SPLIT(@sector, ',')) s
                                                                                                      where s.value = sec.id) )
                                              and (@region = '0' OR ISNULL(c.region,'Core') = 'Core' OR c.region in (select reg.NAME
                                                                                                      from ascend.region_new reg,
                                                                                                      (select value from STRING_SPLIT(@region, ',')) s
                                                                                                      where s.value = reg.DESCRIPTION) )
                                              and (@l1 = '0'  OR upper(c.process_area_l1) = 'COMMON'  OR ISNULL(c.process_area_l1,'0') = '0' OR c.process_area_l1 in (select l1.l1
                                                                                                                  from ascend.business_processes_new_l1 l1,
                                                                                                                  (select value from STRING_SPLIT(@l1, ',')) s
                                                                                                                  where s.value = l1.id))
                                              and (@l2 = '0' OR upper(c.process_area_l2) = 'COMMON'  OR ISNULL(c.process_area_l2,'0') = '0' OR c.process_area_l2 in (select l2.l2
                                                                                                                  from ascend.business_processes_new_l2 l2,
                                                                                                                  (select value from STRING_SPLIT(@l2, ',')) s
                                                                                                                  where s.value = l2.id))
                                              and (@l3 = '0'  OR upper(c.process_area_l3) = 'COMMON' OR ISNULL(c.process_area_l3,'0') = '0' OR c.process_area_l3 in (select l3.l3
                                                                                                                  from ascend.business_processes_new_l3 l3,
                                                                                                                  (select value from STRING_SPLIT(@l3, ',')) s
                                                                                                                  where s.value = l3.id))
                )
                ,
                t AS (SELECT e.L1value L1value,null L1linkname,
                                       null L1doclink,
                                       (CASE WHEN ((SELECT COUNT(1)
                                            FROM ASCEND.project_workspace pws
                                              ,ASCEND.user_roles ur
                                            WHERE ur.ID = @userroleid
                                                              AND ur.project_id = pws.project_id
                                                              AND pws.entity_name = 'CONTINUE_DIGITAL_ORGANIZATION'
                                            AND e.L1value = pws.L1) >= 1)
                                         THEN 'Y' else 'N'
                                         END) L1enabledflag,
                                      e.l2grp 'L2grp'
                                  FROM l2group e
                                  )
                          SELECT * FROM t FOR JSON PATH, INCLUDE_NULL_VALUES`
              )

                  .then(function (recordset) {
                      let res = JSON.parse(JSON.stringify(recordset.recordset[0]));
                      console.log(res.data);
                      conn.close();
                      //resolve(res.data);
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
  },
  
  postcontinuedigitalorg:function(jsondata,projectId) {
    return new Promise((resolve, reject) =>
    {
      console.log('In updateData');
      console.log(jsondata);
      console.log('projectId:'+projectId);

      var conn = new sql.ConnectionPool(connection.getconnection().dbConfig)
      conn.connect()
      // Successful connection
      .then(function () {
        console.log('In connection successful');

        let transaction = new sql.Transaction(conn);
        transaction.begin().then(async function(){
          console.log('In transaction begin successful');
          let request = new sql.Request(transaction);
          let iExceptionFlag = false;
          let errorMessage = '';
                  try{
                    let deleteString = 'DELETE FROM '+connection.schemaName+'.PROJECT_WORKSPACE where project_id = '+projectId+` AND entity_name = 'CONTINUE_DIGITAL_ORGANIZATION'`;
                    console.log('deleteString:'+deleteString);
                    await request.query(deleteString);
                    console.log('jsondata.length:'+jsondata.length);
                    for (var i = 0; i < jsondata.length; i++) {
					  tabContent = jsondata[i].tabContent;
					  console.log('tabContent.length:'+tabContent.length);
                      for (var l = 0; l < tabContent.length; l++) {
                        L2grp = tabContent[l].L2grp;
                        L1value = tabContent[l].L1value;
                        for (var j = 0; j < L2grp.length; j++) {
                          L2value = L2grp[j].L2value;
                            //L2value = L2Grp[j].decisionname;
                            if (L2grp[j].L2enabledflag == 'Y') {
                                  console.log('L1value:'+L1value+' L2value'+L2value);
                                  let entityName = 'CONTINUE_DIGITAL_ORGANIZATION';
                                  let entityTable = 'CONTINUE_DIGITAL_ORG';
                                  let seperator = `','`;
                                  let endBlock = `')`;
                                  let insertString = `insert into `+connection.schemaName+`.PROJECT_WORKSPACE (project_id, entity_name, entity_value, ENTITY_TABLE, L2, L1) VALUES ('`+projectId+
                                                      seperator+entityName+seperator+L2grp[j].L2value+seperator+entityTable+seperator+L2value+seperator+L1value+endBlock;
                                  console.log('insertString:'+insertString);
                                  await request.query(insertString);
                              }

                          }
                        }
                    }
                }
                catch(e){
                  iExceptionFlag = true;
                  errorMessage = e;
                  console.log('Exception:'+e);
                }
                console.log(iExceptionFlag)
                if(iExceptionFlag){
                  transaction.rollback().then(function(){
                    console.log('In rollback:then');
                    conn.close();
                    resolve({"MSG":"Error while inserting data into database:"+errorMessage});
                  })
                  .catch(function(err){
                    console.log('In rollback:catch');
                    conn.close();
                    resolve({"MSG":"Error while rolling back transaction:"+errorMessage});
                  });
                }else{
                  transaction.commit().then(function(){
                    console.log('In commit:then');
                    conn.close();
                    resolve({"MSG":"SUCCESS"});
                  })
                  .catch(function(err){
                    console.log('In commit:catch');
                    conn.close();
                    resolve({"MSG":"Error while commiting transaction:"+errorMessage});
                  });
                }


          })
          .catch(function(err){
            console.log('In transaction begin catch:'+err);
            conn.close();
            resolve({"MSG":"Error while creating transaction object:"+errorMessage});
          })

      })
      .catch(function(err){
        console.log('In connection catch catch:'+err)
        conn.close();
        resolve({"MSG":"Error while creating connection object:"+errorMessage});
      })

  })
  }
}
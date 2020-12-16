var connection = require('./connection-file');
var sql = require("mssql");

module.exports = {
    /************************************************************** *
        ALL METHODS SHOULD BE ADDED IN THIS module.exports AFTER getSampleService
        METHOD.
    /************************************************************** */
getDesignDevTools: function (userroleid, industry, sector, region, l1, l2, l3) {

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

            req.query(`with L3Tab as (SELECT distinct a3.project_phase
                 ,(select a.tool_name 'toolname'
                         ,a.description 'description'
                         ,(CASE
                              WHEN ((SELECT count(1)
                                      FROM ASCEND.project_workspace pws
                                          ,ASCEND.user_roles ur
                                      WHERE ur.ID = @userroleid
                                      and ur.project_id = pws.project_id
                                      AND a.tool_name = pws.entity_value
                                      and pws.entity_name = 'DEVELOPMENT_TOOLS'
                                      AND ISNULL(a3.project_phase,'X') = ISNULL(pws.L3,'X')
                                  ) = 1)
                              THEN 'Y' else 'N'
                              END) 'toolenabledFlag'
                          ,a.tool_name 'toollinkname'
                         ,a.hosted_url 'tooldoclink'
                      from ASCEND.tools_accelerators_new a
                          where a.project_phase = a3.project_phase
						   and (@industry = '0' OR ISNULL(a.industry,'COMMON') = 'COMMON' OR a.industry in (select ind.industry
                                  from ascend.industries_new_i ind,
                                  (select value from STRING_SPLIT(@industry, ',')) s
                                  where s.value = ind.id) )
                  and (@sector = '0' OR ISNULL(a.sector,'COMMON') = 'COMMON' OR a.sector in (select sec.sector
                                  from ascend.industries_new_s sec,
                                  (select value from STRING_SPLIT(@sector, ',')) s
                                  where s.value = sec.id) )
                  and (@region = '0' OR ISNULL(a.region,'Core') = 'Core' OR a.region in (select reg.NAME
                                  from ascend.region_new reg,
                                  (select value from STRING_SPLIT(@region, ',')) s
                                  where s.value = reg.DESCRIPTION) )
                  and (@l1 = '0' OR upper(a.process_area_l1) = 'COMMON' OR ISNULL(a.process_area_l1,'0') = '0' OR a.process_area_l1 in (select l1.l1
                                              from ascend.business_processes_new_l1 l1,
                                              (select value from STRING_SPLIT(@l1, ',')) s
                                              where s.value = l1.id))
                  and (@l2 = '0' OR upper(a.process_area_l2) = 'COMMON' OR ISNULL(a.process_area_l2,'0') = '0' OR a.process_area_l2 in (select l2.l2
                                              from ascend.business_processes_new_l2 l2,
                                              (select value from STRING_SPLIT(@l2, ',')) s
                                              where s.value = l2.id))
                  and (@l3 = '0' OR upper(a.process_area_l3) = 'COMMON' OR ISNULL(a.process_area_l3,'0') = '0' OR a.process_area_l3 in (select l3.l3
                                              from ascend.business_processes_new_l3 l3,
                                              (select value from STRING_SPLIT(@l3, ',')) s
                                              where s.value = l3.id))
                      for json path,include_null_values) 'toolgrp'
                  FROM ASCEND.tools_accelerators_new a3
                  where 1=1
                 and (@industry = '0' OR ISNULL(a3.industry,'COMMON') = 'COMMON' OR a3.industry in (select ind.industry
                                  from ascend.industries_new_i ind,
                                  (select value from STRING_SPLIT(@industry, ',')) s
                                  where s.value = ind.id) )
                  and (@sector = '0' OR ISNULL(a3.sector,'COMMON') = 'COMMON' OR a3.sector in (select sec.sector
                                  from ascend.industries_new_s sec,
                                  (select value from STRING_SPLIT(@sector, ',')) s
                                  where s.value = sec.id) )
                  and (@region = '0' OR ISNULL(a3.region,'Core') = 'Core' OR a3.region in (select reg.NAME
                                  from ascend.region_new reg,
                                  (select value from STRING_SPLIT(@region, ',')) s
                                  where s.value = reg.DESCRIPTION) )
                  and (@l1 = '0' OR upper(a3.process_area_l1) = 'COMMON' OR ISNULL(a3.process_area_l1,'0') = '0' OR a3.process_area_l1 in (select l1.l1
                                              from ascend.business_processes_new_l1 l1,
                                              (select value from STRING_SPLIT(@l1, ',')) s
                                              where s.value = l1.id))
                  and (@l2 = '0' OR upper(a3.process_area_l2) = 'COMMON' OR ISNULL(a3.process_area_l2,'0') = '0' OR a3.process_area_l2 in (select l2.l2
                                              from ascend.business_processes_new_l2 l2,
                                              (select value from STRING_SPLIT(@l2, ',')) s
                                              where s.value = l2.id))
                  and (@l3 = '0' OR upper(a3.process_area_l3) = 'COMMON' OR ISNULL(a3.process_area_l3,'0') = '0' OR a3.process_area_l3 in (select l3.l3
                                              from ascend.business_processes_new_l3 l3,
                                              (select value from STRING_SPLIT(@l3, ',')) s
                                              where s.value = l3.id))
                  ),
    t as (select distinct a.project_phase 'L1value'
                              ,(CASE
                                      WHEN ((SELECT count(1)
                                              FROM ASCEND.project_workspace pws
                                                  ,ASCEND.user_roles ur
                                              WHERE ur.ID = @userroleid
                                              and ur.project_id = pws.project_id
                                              and pws.entity_name = 'DEVELOPMENT_TOOLS'
                                              AND ISNULL(a.project_phase,'X') = ISNULL(pws.L3,'X')
                                          ) >= 1)
                                      THEN 'Y' else 'N'
                                      END)      'L1enabledflag'
                                  ,a.project_phase 'L1linkname'
                                  ,null 'L1doclink'
                                  ,a.toolgrp 'toolgrp'
                          from L3Tab a
                          )
      select * from t for json path,include_null_values`)

              .then(function (recordset) {
                let res = JSON.parse(JSON.stringify(recordset.recordset[0]));
                console.log(res.data);
                conn.close();
                //resolve(res.data);
                for (key in res) {
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
getConstructConversions: function(userroleid, industry, sector, region, l1, l2, l3) {

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

                req.query(`with L3Tab as (SELECT distinct a3.process_area_l1
                               ,a3.oracle_module
                               ,a3.alliance_product
                               ,a3.toolkit_link
                               ,(select a.object_name conversionname
                                       ,a.object_description description
                                       ,(CASE
                                            WHEN ((SELECT count(1)
                                                    FROM ASCEND.project_workspace pws
                                                        ,ASCEND.user_roles ur
                                                    WHERE ur.ID = @userroleid
                                                    and ur.project_id = pws.project_id
                                                    and pws.entity_name = 'CONVERSIONS'
                                                    AND a.object_name = pws.entity_value
                                                    AND ISNULL(a.oracle_module,'X') = ISNULL(pws.L2,'X')
                                                    AND ISNULL(a.process_area_l1,'X') = ISNULL(pws.L3,'X')
                                                ) = 1)
                                            THEN 'Y' else 'N'
                                            END) conversionenabledFlag
                                        ,a.object_name conversionlinkname
                                       ,(select concat(b1.doc_link, '/', b1.file_name)
                                           from ascend.documents b1
                                           where b1.name = a.object_name
                                             and b1.type = 'Conversion'
                                           ) conversiondoclink
                                    from ASCEND.tech_object_library a
                                        where a.process_area_l1 = a3.process_area_l1
                                        and a.oracle_module = a3.oracle_module
                                 and isnull(a.alliance_product,'X') = isnull(a3.alliance_product,'X')
                                        and isnull(a.toolkit_link,'X') = isnull(a3.toolkit_link,'X')
										 and a.object_type = 'Conversions'
                                    and (@industry = '0' OR ISNULL(a.industry,'COMMON') = 'COMMON' OR a.industry in (select ind.industry
                                                                                            from ascend.industries_new_i ind,
                                                                                            (select value from STRING_SPLIT(@industry, ',')) s
                                                                                            where s.value = ind.id) )
                                    and (@sector = '0' OR ISNULL(a.sector,'COMMON') = 'COMMON' OR a.sector in (select sec.sector
                                                                                            from ascend.industries_new_s sec,
                                                                                            (select value from STRING_SPLIT(@sector, ',')) s
                                                                                            where s.value = sec.id) )
                                    and (@region = '0' OR ISNULL(a.region,'Core') = 'Core' OR a.region in (select reg.NAME
                                                                                            from ascend.region_new reg,
                                                                                            (select value from STRING_SPLIT(@region, ',')) s
                                                                                            where s.value = reg.DESCRIPTION) )
                                    and (@l1 = '0' OR upper(a.process_area_l1) = 'COMMON'  OR ISNULL(a.process_area_l1,'0') = '0' OR a.process_area_l1 in (select l1.l1
                                                                                                        from ascend.business_processes_new_l1 l1,
                                                                                                        (select value from STRING_SPLIT(@l1, ',')) s
                                                                                                        where s.value = l1.id))
                                    and (@l2 = '0' OR upper(a.process_area_l2) = 'COMMON' OR ISNULL(a.process_area_l2,'0') = '0' OR a.process_area_l2 in (select l2.l2
                                                                                                        from ascend.business_processes_new_l2 l2,
                                                                                                        (select value from STRING_SPLIT(@l2, ',')) s
                                                                                                        where s.value = l2.id))
                                    and (@l3 = '0' OR upper(a.process_area_l3) = 'COMMON' OR ISNULL(a.process_area_l3,'0') = '0' OR a.process_area_l3 in (select l3.l3
                                                                                                        from ascend.business_processes_new_l3 l3,
                                                                                                        (select value from STRING_SPLIT(@l3, ',')) s
                                                                                                        where s.value = l3.id))
                                    for json path,include_null_values) AS L3Grp
                                FROM ASCEND.tech_object_library a3
                                    /*,ascend.documents b1*/
                                where /*b1.type = 'Conversion'
                                    and b1.name = a3.object_name */
                                    1 = 1
                				    and a3.object_type = 'Conversions'
                                    and (@industry = '0' OR ISNULL(a3.industry,'COMMON') = 'COMMON' OR a3.industry in (select ind.industry
                                                                                            from ascend.industries_new_i ind,
                                                                                            (select value from STRING_SPLIT(@industry, ',')) s
                                                                                            where s.value = ind.id) )
                                    and (@sector = '0' OR ISNULL(a3.sector,'COMMON') = 'COMMON' OR a3.sector in (select sec.sector
                                                                                            from ascend.industries_new_s sec,
                                                                                            (select value from STRING_SPLIT(@sector, ',')) s
                                                                                            where s.value = sec.id) )
                                    and (@region = '0' OR ISNULL(a3.region,'Core') = 'Core' OR a3.region in (select reg.NAME
                                                                                            from ascend.region_new reg,
                                                                                            (select value from STRING_SPLIT(@region, ',')) s
                                                                                            where s.value = reg.DESCRIPTION) )
                                    and (@l1 = '0' OR upper(a3.process_area_l1) = 'COMMON' OR ISNULL(a3.process_area_l1,'0') = '0' OR a3.process_area_l1 in (select l1.l1
                                                                                                        from ascend.business_processes_new_l1 l1,
                                                                                                        (select value from STRING_SPLIT(@l1, ',')) s
                                                                                                        where s.value = l1.id))
                                    and (@l2 = '0' OR upper(a3.process_area_l1) = 'COMMON' OR ISNULL(a3.process_area_l2,'0') = '0' OR a3.process_area_l2 in (select l2.l2
                                                                                                        from ascend.business_processes_new_l2 l2,
                                                                                                        (select value from STRING_SPLIT(@l2, ',')) s
                                                                                                        where s.value = l2.id))
                                    and (@l3 = '0' OR upper(a3.process_area_l1) = 'COMMON' OR ISNULL(a3.process_area_l3,'0') = '0' OR a3.process_area_l3 in (select l3.l3
                                                                                                        from ascend.business_processes_new_l3 l3,
                                                                                                        (select value from STRING_SPLIT(@l3, ',')) s
                                                                                                        where s.value = l3.id))
                                ),
                  L2Tab as (select distinct a2.process_area_l1
                                            ,a2.alliance_product
                                            ,(select a.oracle_module L2value
                                                ,(CASE
                                                    WHEN ((SELECT count(1)
                                                            FROM ASCEND.project_workspace pws
                                                                ,ASCEND.user_roles ur
                                                            WHERE ur.ID = @userroleid
                                                            and ur.project_id = pws.project_id
                                                            and pws.entity_name = 'CONVERSIONS'
                                                            AND ISNULL(a.oracle_module,'X') = ISNULL(pws.L2,'X')
                                                            AND ISNULL(a.process_area_l1,'X') = ISNULL(pws.L3,'X')
                                                        ) >= 1)
                                                    THEN 'Y' else 'N'
                                                    END) L2enabledflag
                                                ,a.oracle_module L2linkname
                                                ,(select concat(b1.doc_link, '/', b1.file_name)
                                                        from ascend.documents b1
                                                        where b1.name = a.oracle_module
                                                            and b1.type = 'Conversion'
                                                ) L2doclink
                                                ,a.L3Grp AS L3grp
                                        from L3Tab a
                                        where a.process_area_l1 = a2.process_area_l1
                                          and ISNULL(a.alliance_product,'X') = ISNULL(a2.alliance_product,'X')
                                            for json path,include_null_values) AS L2Grp
                    from L3Tab a2),
                t as (select a1.process_area_l1 L1value
                      ,(CASE
                        WHEN ((SELECT count(1)
                                FROM ASCEND.project_workspace pws
                                    ,ASCEND.user_roles ur
                                WHERE ur.ID = @userroleid
                                and ur.project_id = pws.project_id
                                and pws.entity_name = 'CONVERSIONS'
                                AND ISNULL(a1.process_area_l1,'X') = ISNULL(pws.L3,'X')
                            ) >= 1)
                        THEN 'Y' else 'N'
                        END)  L1enabledflag
                       ,a1.process_area_l1 L1linkname
                       ,(select concat(b1.doc_link, '/', b1.file_name)
                                           from ascend.documents b1
                                           where b1.name = a1.process_area_l1
                                             and b1.type = 'Conversion'
                        ) L1doclink
                        ,a1.alliance_product conversiontype
                        ,a1.L2Grp AS L2grp
                from L2Tab a1
                )
                select * from t for json path,include_null_values`)

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
getValidateTestScenarios: function(userroleid, industry, sector, region, l1, l2, l3) {

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

                req.query(`
with L3Tab as (SELECT distinct a3.process_area_l1
                               ,a3.process_area_l2
                               ,(select a.test_scenario 'testname'
                                       ,a.test_scenario 'description'
                                       ,(CASE
                                            WHEN ((SELECT count(1)
                                                    FROM ASCEND.project_workspace pws
                                                        ,ASCEND.user_roles ur
                                                    WHERE ur.ID = @userroleid
                                                    and ur.project_id = pws.project_id
                                                    and pws.entity_name = 'TEST_SCENARIOS'
                                                    AND a.test_scenario = pws.entity_value
                                                    AND ISNULL(a.process_area_l2,'X') = ISNULL(pws.L2,'X')
                                                    AND ISNULL(a.process_area_l1,'X') = ISNULL(pws.L3,'X')
                                                ) = 1)
                                            THEN 'Y' else 'N'
                                            END) 'testenabledFlag'
                                        ,a.test_scenario 'testlinkname'
                                       ,(select concat(b1.doc_link, '/', b1.file_name)
                                           from ascend.documents b1
                                           where b1.name = a.test_scenario
                                             AND b1.category = a.process_area_l1
                                             and b1.type = 'Test Scripts'
                                           ) 'testdoclink'
                                    from ASCEND.test_scenario_new a
                                        where a.process_area_l1 = a3.process_area_l1
                                        and a.process_area_l2 = a3.process_area_l2
										 and (@industry = '0' OR ISNULL(a.industry,'COMMON') = 'COMMON' OR a.industry in (select ind.industry
                                                                                        from ascend.industries_new_i ind,
                                                                                        (select value from STRING_SPLIT(@industry, ',')) s
                                                                                        where s.value = ind.id) )
                                and (@sector = '0' OR ISNULL(a.sector,'COMMON') = 'COMMON' OR a.sector in (select sec.sector
                                                                                        from ascend.industries_new_s sec,
                                                                                        (select value from STRING_SPLIT(@sector, ',')) s
                                                                                        where s.value = sec.id) )
                                and (@region = '0' OR ISNULL(a.region,'Core') = 'Core' OR a.region in (select reg.NAME
                                                                                        from ascend.region_new reg,
                                                                                        (select value from STRING_SPLIT(@region, ',')) s
                                                                                        where s.value = reg.DESCRIPTION) )
                                and (@l1 = '0' OR upper(a.process_area_l1) = 'COMMON' OR ISNULL(a.process_area_l1,'0') = '0' OR a.process_area_l1 in (select l1.l1
                                                                                                    from ascend.business_processes_new_l1 l1,
                                                                                                    (select value from STRING_SPLIT(@l1, ',')) s
                                                                                                    where s.value = l1.id))
                                and (@l2 = '0' OR upper(a.process_area_l2) = 'COMMON' OR ISNULL(a.process_area_l2,'0') = '0' OR a.process_area_l2 in (select l2.l2
                                                                                                    from ascend.business_processes_new_l2 l2,
                                                                                                    (select value from STRING_SPLIT(@l2, ',')) s
                                                                                                    where s.value = l2.id))
                                and (@l3 = '0' OR upper(a.process_area_l3) = 'COMMON' OR ISNULL(a.process_area_l3,'0') = '0' OR a.process_area_l3 in (select l3.l3
                                      from ascend.business_processes_new_l3 l3,
                                      (select value from STRING_SPLIT(@l3, ',')) s
                                      where s.value = l3.id))
                                    for json path,include_null_values) 'L3Grp'
                                FROM ASCEND.test_scenario_new a3
                                    /*,ascend.documents b1*/
                                where /*b1.type = 'Test Scripts'
                                 and b1.name = a3.test_scenario*/
                                 1 = 1
                                and (@industry = '0' OR ISNULL(a3.industry,'COMMON') = 'COMMON' OR a3.industry in (select ind.industry
                                                                                        from ascend.industries_new_i ind,
                                                                                        (select value from STRING_SPLIT(@industry, ',')) s
                                                                                        where s.value = ind.id) )
                                and (@sector = '0' OR ISNULL(a3.sector,'COMMON') = 'COMMON' OR a3.sector in (select sec.sector
                                                                                        from ascend.industries_new_s sec,
                                                                                        (select value from STRING_SPLIT(@sector, ',')) s
                                                                                        where s.value = sec.id) )
                                and (@region = '0' OR ISNULL(a3.region,'Core') = 'Core' OR a3.region in (select reg.NAME
                                                                                        from ascend.region_new reg,
                                                                                        (select value from STRING_SPLIT(@region, ',')) s
                                                                                        where s.value = reg.DESCRIPTION) )
                                and (@l1 = '0' OR upper(a3.process_area_l1) = 'COMMON' OR ISNULL(a3.process_area_l1,'0') = '0' OR a3.process_area_l1 in (select l1.l1
                                                                                                    from ascend.business_processes_new_l1 l1,
                                                                                                    (select value from STRING_SPLIT(@l1, ',')) s
                                                                                                    where s.value = l1.id))
                                and (@l2 = '0' OR upper(a3.process_area_l2) = 'COMMON' OR ISNULL(a3.process_area_l2,'0') = '0' OR a3.process_area_l2 in (select l2.l2
                                                                                                    from ascend.business_processes_new_l2 l2,
                                                                                                    (select value from STRING_SPLIT(@l2, ',')) s
                                                                                                    where s.value = l2.id))
                                and (@l3 = '0' OR upper(a3.process_area_l3) = 'COMMON' OR ISNULL(a3.process_area_l3,'0') = '0' OR a3.process_area_l3 in (select l3.l3
                                      from ascend.business_processes_new_l3 l3,
                                      (select value from STRING_SPLIT(@l3, ',')) s
                                      where s.value = l3.id))
                                ),
                  L2Tab as (select distinct a2.process_area_l1
                                            ,(select a.process_area_l2 'L2value'
                                                ,(CASE
                                                    WHEN ((SELECT count(1)
                                                            FROM ASCEND.project_workspace pws
                                                                ,ASCEND.user_roles ur
                                                            WHERE ur.ID = @userroleid
                                                            and ur.project_id = pws.project_id
                                                            and pws.entity_name = 'TEST_SCENARIOS'
                                                            AND ISNULL(a.process_area_l2,'X') = ISNULL(pws.L2,'X')
                                                            AND ISNULL(a.process_area_l1,'X') = ISNULL(pws.L3,'X')
                                                        ) >= 1)
                                                    THEN 'Y' else 'N'
                                                    END)      'L2enabledflag'
                                                ,a.process_area_l2 'L2linkname'
                                                ,(select concat(b1.doc_link, '/', b1.file_name)
                                                        from ascend.documents b1
                                                        where b1.name = a.process_area_l2
                                                            and b1.type = 'Test Scripts'
                                                ) 'L2doclink'
                                                ,a.L3Grp 'L3grp'
                                        from L3Tab a
                                        where a.process_area_l1 = a2.process_area_l1
                                            for json path,include_null_values) 'L2Grp'
                    from L3Tab a2),
                t as (select a1.process_area_l1 'L1value'
                      ,(CASE
                        WHEN ((SELECT count(1)
                                FROM ASCEND.project_workspace pws
                                    ,ASCEND.user_roles ur
                                WHERE ur.ID = @userroleid
                                and ur.project_id = pws.project_id
                                and pws.entity_name = 'TEST_SCENARIOS'
                                AND ISNULL(a1.process_area_l1,'X') = ISNULL(pws.L3,'X')
                            ) >= 1)
                        THEN 'Y' else 'N'
                        END)  'L1enabledflag'
                       ,(select concat(b1.doc_link, '/', b1.file_name)
                                           from ascend.documents b1
                                           where b1.name = a1.process_area_l1
                                             and b1.type = 'Test Scripts'
                        ) 'L1doclink'
                        ,a1.L2Grp 'L2grp'
                from L2Tab a1
                )
                select * from t
                for json path, include_null_values`)

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
getValidateTestBots: function(userroleid, industry, sector, region, l1, l2, l3) {

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

              req.query(`with L3Tab as (SELECT distinct a3.process_area_l1
                ,a3.process_area_l2
                ,a3.technology
                ,(select a.bot_name 'botname'
                        ,a.bot_name 'description'
                        ,(CASE
                            WHEN ((SELECT count(1)
                                    FROM ASCEND.project_workspace pws
                                        ,ASCEND.user_roles ur
                                    WHERE ur.ID = @userroleid
                                    and ur.project_id = pws.project_id
                                    and pws.entity_name = 'TEST_AUTOMATIONS'
                                    AND a.bot_name = pws.entity_value
                                    AND ISNULL(a.process_area_l2,'X') = ISNULL(pws.L2,'X')
                                    AND ISNULL(a.process_area_l1,'X') = ISNULL(pws.L3,'X')
                                ) = 1)
                            THEN 'Y' else 'N'
                            END) 'botenabledFlag'
                        ,a.bot_name 'botlinkname'
                        ,a.url 'botdoclink'
                    from ASCEND.automation_bots a
                        where a.process_area_l1 = a3.process_area_l1
                        and a.process_area_l2 = a3.process_area_l2
                        and a.technology = a3.technology
						and (@industry = '0' OR ISNULL(a.industry,'COMMON') = 'COMMON' OR a.industry in (select ind.industry
                                                                                                    from ascend.industries_new_i ind,
                                                                                                    (select value from STRING_SPLIT(@industry, ',')) s
                                                                                                    where s.value = ind.id) )
                and (@sector = '0' OR ISNULL(a.sector,'COMMON') = 'COMMON' OR a.sector in (select sec.sector
                                                                                              from ascend.industries_new_s sec,
                                                                                              (select value from STRING_SPLIT(@sector, ',')) s
                                                                                              where s.value = sec.id) )
                and (@region = '0' OR ISNULL(a.region,'Core') = 'Core' OR a.region in (select reg.NAME
                                                                                          from ascend.region_new reg,
                                                                                          (select value from STRING_SPLIT(@region, ',')) s
                                                                                          where s.value = reg.DESCRIPTION) )
                and (@l1 = '0' OR upper(a.process_area_l1) = 'COMMON' OR ISNULL(a.process_area_l1,'0') = '0' OR a.process_area_l1 in (select l1.l1
                                                                                                                                          from ascend.business_processes_new_l1 l1,
                                                                                                                                          (select value from STRING_SPLIT(@l1, ',')) s
                                                                                                                                          where s.value = l1.id))
                and (@l2 = '0' OR upper(a.process_area_l2) = 'COMMON' OR ISNULL(a.process_area_l2,'0') = '0' OR a.process_area_l2 in (select l2.l2
                                                                                                                                          from ascend.business_processes_new_l2 l2,
                                                                                                                                          (select value from STRING_SPLIT(@l2, ',')) s
                                                                                                                                          where s.value = l2.id))
                and (@l3 = '0' OR upper(a.process_area_l3) = 'COMMON' OR ISNULL(a.process_area_l3,'0') = '0' OR a.process_area_l3 in (select l3.l3
                                                                                                                                          from ascend.business_processes_new_l3 l3,
                                                                                                                                          (select value from STRING_SPLIT(@l3, ',')) s
                                                                                                                                          where s.value = l3.id))
                    for json path,include_null_values) 'L3Grp'
                FROM ASCEND.automation_bots a3
                WHERE 1 = 1
                and (@industry = '0' OR ISNULL(a3.industry,'COMMON') = 'COMMON' OR a3.industry in (select ind.industry
                                                                                                    from ascend.industries_new_i ind,
                                                                                                    (select value from STRING_SPLIT(@industry, ',')) s
                                                                                                    where s.value = ind.id) )
                and (@sector = '0' OR ISNULL(a3.sector,'COMMON') = 'COMMON' OR a3.sector in (select sec.sector
                                                                                              from ascend.industries_new_s sec,
                                                                                              (select value from STRING_SPLIT(@sector, ',')) s
                                                                                              where s.value = sec.id) )
                and (@region = '0' OR ISNULL(a3.region,'Core') = 'Core' OR a3.region in (select reg.NAME
                                                                                          from ascend.region_new reg,
                                                                                          (select value from STRING_SPLIT(@region, ',')) s
                                                                                          where s.value = reg.DESCRIPTION) )
                and (@l1 = '0' OR upper(a3.process_area_l1) = 'COMMON' OR ISNULL(a3.process_area_l1,'0') = '0' OR a3.process_area_l1 in (select l1.l1
                                                                                                                                          from ascend.business_processes_new_l1 l1,
                                                                                                                                          (select value from STRING_SPLIT(@l1, ',')) s
                                                                                                                                          where s.value = l1.id))
                and (@l2 = '0' OR upper(a3.process_area_l2) = 'COMMON' OR ISNULL(a3.process_area_l2,'0') = '0' OR a3.process_area_l2 in (select l2.l2
                                                                                                                                          from ascend.business_processes_new_l2 l2,
                                                                                                                                          (select value from STRING_SPLIT(@l2, ',')) s
                                                                                                                                          where s.value = l2.id))
                and (@l3 = '0' OR upper(a3.process_area_l3) = 'COMMON' OR ISNULL(a3.process_area_l3,'0') = '0' OR a3.process_area_l3 in (select l3.l3
                                                                                                                                          from ascend.business_processes_new_l3 l3,
                                                                                                                                          (select value from STRING_SPLIT(@l3, ',')) s
                                                                                                                                          where s.value = l3.id))
                ),
L2Tab as (select distinct a2.process_area_l1
                            ,a2.technology
                            ,(select a.process_area_l2 'L2value'
                                ,(CASE
                                    WHEN ((SELECT count(1)
                                            FROM ASCEND.project_workspace pws
                                                ,ASCEND.user_roles ur
                                            WHERE ur.ID = @userroleid
                                            and ur.project_id = pws.project_id
                                            and pws.entity_name = 'TEST_AUTOMATIONS'
                                            AND ISNULL(a.process_area_l2,'X') = ISNULL(pws.L2,'X')
                                            AND ISNULL(a.process_area_l1,'X') = ISNULL(pws.L3,'X')
                                        ) >= 1)
                                    THEN 'Y' else 'N'
                                    END)      'L2enabledflag'
                                ,a.process_area_l2 'L2linkname'
                                ,NULL 'L2doclink'
                                ,a.L3Grp 'L3grp'
                        from L3Tab a
                        where ISNULL(a.process_area_l1,'X') = ISNULL(a2.process_area_l1,'X')
                        and a.technology = a2.technology
                            for json path,include_null_values) 'L2Grp'
    from L3Tab a2),
t as (select a1.process_area_l1 'L1value'
    ,(CASE
        WHEN ((SELECT count(1)
                FROM ASCEND.project_workspace pws
                    ,ASCEND.user_roles ur
                WHERE ur.ID = @userroleid
                and ur.project_id = pws.project_id
                and pws.entity_name = 'TEST_AUTOMATIONS'
                AND ISNULL(a1.process_area_l1,'X') = ISNULL(pws.L3,'X')
            ) >= 1)
        THEN 'Y' else 'N'
        END)  'L1enabledflag'
        ,NULL 'L1doclink'
        ,NULL 'L1linkname'
        ,a1.technology 'technology'
        ,a1.L2Grp 'L2grp'
from L2Tab a1
)
select * from t
for json path,include_null_values`)

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
getDeploy: function(userroleid, industry, sector, region, l1, l2, l3) {

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

              req.query(`with
              l2group AS  (SELECT DISTINCT c.L1name L1value,
                                              (SELECT d.L2name L2value,
                                                  d.L2description L2linkname,
                                                  (SELECT CONCAT(b1.doc_link, '/', b1.file_name)
                                                   FROM ASCEND.documents b1
                                                  WHERE d.L2name = b1.name
                                                    AND b1.type = 'Deploy') L2doclink,
                                                  (CASE
                                                    WHEN ((SELECT COUNT(1)
                                                        FROM ASCEND.project_workspace pws
                                                          ,ASCEND.user_roles ur
                                                        WHERE ur.ID = @userroleid
                                                                                    and ur.project_id = pws.project_id
                                                                                    AND pws.entity_name = 'DEPLOY'
                                                        AND d.l2name = pws.L2
                                                        AND d.l1name = pws.L1) = 1)
                                                    THEN 'Y' else 'N'
                                                    END) L2enabledflag
                                               FROM ( select distinct L1name,L2name,L2description,L3name,region,industry,sector,process_area_l1,process_area_l2,process_area_l3 from ascend.deliverables where stop='Deploy' and phase = 'Deliver' )  d
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
                                     FROM ( select distinct L1name,L2name,L2description,L3name,region,industry,sector,process_area_l1,process_area_l2,process_area_l3 from ascend.deliverables where stop='Deploy' and phase = 'Deliver') c
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
                                                                AND pws.entity_name = 'DEPLOY'
                                              AND e.L1value = pws.L1) >= 1)
                                           THEN 'Y' else 'N'
                                           END) L1enabledflag,
                                        e.l2grp 'L2grp'
                                    FROM l2group e
                                    )
                            SELECT * FROM t FOR JSON PATH, INCLUDE_NULL_VALUES`)
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
getActivateDigitalOrg: function(userroleid, industry, sector, region, l1, l2, l3) {

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

              req.query(`with
              l2group AS  (SELECT DISTINCT c.L1name L1value,
                                              (SELECT d.L2name L2value,
                                                  d.L2description L2linkname,
                                                  (SELECT CONCAT(b1.doc_link, '/', b1.file_name)
                                                   FROM ASCEND.documents b1
                                                  WHERE d.L2name = b1.name
                                                    AND b1.type = 'Activate Digital Org') L2doclink,
                                                  (CASE
                                                    WHEN ((SELECT COUNT(1)
                                                        FROM ASCEND.project_workspace pws
                                                          ,ASCEND.user_roles ur
                                                        WHERE ur.ID = @userroleid
                                                                                    and ur.project_id = pws.project_id
                                                                                    AND pws.entity_name = 'ACTIVATE_DIGITAL_ORGANIZATION'
                                                        AND d.l2name = pws.L2
                                                        AND d.l1name = pws.L1) = 1)
                                                    THEN 'Y' else 'N'
                                                    END) L2enabledflag
                                               FROM ( select distinct L1name,L2name,L2description,L3name,region,industry,sector,process_area_l1,process_area_l2,process_area_l3 from ascend.activate_digital_org ) d
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
                                     FROM  ( select distinct L1name,L2name,L2description,L3name,region,industry,sector,process_area_l1,process_area_l2,process_area_l3 from ascend.activate_digital_org )	c
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
                                                                AND pws.entity_name = 'ACTIVATE_DIGITAL_ORGANIZATION'
                                              AND e.L1value = pws.L1) >= 1)
                                           THEN 'Y' else 'N'
                                           END) L1enabledflag,
                                        e.l2grp 'L2grp'
                                    FROM l2group e
                                    )
                            SELECT * FROM t FOR JSON PATH, INCLUDE_NULL_VALUES`)
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
postConstructConversions:function(jsondata,projectId) {
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
                    let deleteString = 'DELETE FROM '+connection.schemaName+'.PROJECT_WORKSPACE where project_id = '+projectId+` AND entity_name = 'CONVERSIONS'`;
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
                          L3grp = L2grp[j].L3grp;
                          for (var k = 0; k < L3grp.length; k++) {
                            conversionname = L3grp[k].conversionname;
                            if (L3grp[k].conversionenabledFlag == 'Y') {
                                  console.log('L1value:'+L1value+' L2value'+L2value+' conversionname:'+conversionname);
                                  let entityName = 'CONVERSIONS';
                                  let entityTable = 'TECH_OBJECT_LIBRARY';
                                  let seperator = `','`;
                                  let endBlock = `')`;
                                  let insertString = `insert into `+connection.schemaName+`.PROJECT_WORKSPACE (project_id, entity_name, entity_value, ENTITY_TABLE, L2, L3) VALUES ('`+projectId+
                                                      seperator+entityName+seperator+L3grp[k].conversionname+seperator+entityTable+seperator+L2value+seperator+L1value+endBlock;
                                  console.log('insertString:'+insertString);
                                  await request.query(insertString);
                              }
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
postValidateTestBots:function(jsondata,projectId) {
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
                    let deleteString = 'DELETE FROM '+connection.schemaName+'.PROJECT_WORKSPACE where project_id = '+projectId+` AND entity_name = 'TEST_AUTOMATIONS'`;
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
                          L3grp = L2grp[j].L3grp;
                          for (var k = 0; k < L3grp.length; k++) {
                            botname = L3grp[k].botname;
                            if (L3grp[k].botenabledFlag == 'Y') {
                                  console.log('L1value:'+L1value+' L2value'+L2value+' botname:'+botname);
                                  let entityName = 'TEST_AUTOMATIONS';
                                  let entityTable = 'AUTOMATION_BOTS';
                                  let seperator = `','`;
                                  let endBlock = `')`;
                                  let insertString = `insert into `+connection.schemaName+`.PROJECT_WORKSPACE (project_id, entity_name, entity_value, ENTITY_TABLE, L2, L3) VALUES ('`+projectId+
                                                      seperator+entityName+seperator+L3grp[k].botname+seperator+entityTable+seperator+L2value+seperator+L1value+endBlock;
                                  console.log('insertString:'+insertString);
                                  await request.query(insertString);
                              }
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
postValidateTestScenarios:function(jsondata,projectId) {
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
                    let deleteString = 'DELETE FROM '+connection.schemaName+'.PROJECT_WORKSPACE where project_id = '+projectId+` AND entity_name = 'TEST_SCENARIOS'`;
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
                          L3grp = L2grp[j].L3grp;
                          for (var k = 0; k < L3grp.length; k++) {
                            testname = L3grp[k].testname;
                            if (L3grp[k].testenabledFlag == 'Y') {
                                  console.log('L1value:'+L1value+' L2value'+L2value+' testname:'+testname);
                                  let entityName = 'TEST_SCENARIOS';
                                  let entityTable = 'TEST_SCENARIO_NEW';
                                  let seperator = `','`;
                                  let endBlock = `')`;
                                  let insertString = `insert into `+connection.schemaName+`.PROJECT_WORKSPACE (project_id, entity_name, entity_value, ENTITY_TABLE, L2, L3) VALUES ('`+projectId+
                                                      seperator+entityName+seperator+L3grp[k].testname+seperator+entityTable+seperator+L2value+seperator+L1value+endBlock;
                                  console.log('insertString:'+insertString);
                                  await request.query(insertString);
                              }
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
postactivatedigitalorg:function(jsondata,projectId) {
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
                  let deleteString = 'DELETE FROM '+connection.schemaName+'.PROJECT_WORKSPACE where project_id = '+projectId+` AND entity_name = 'ACTIVATE_DIGITAL_ORGANIZATION'`;
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
                                let entityName = 'ACTIVATE_DIGITAL_ORGANIZATION';
                                let entityTable = 'ACTIVATE_DIGITAL_ORG';
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
postdeploy:function(jsondata,projectId) {
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
                  let deleteString = 'DELETE FROM '+connection.schemaName+'.PROJECT_WORKSPACE where project_id = '+projectId+` AND entity_name = 'DEPLOY'`;
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
                                let entityName = 'DEPLOY';
                                let entityTable = 'DEPLOY';
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
postDesignDevTools: function (jsondata, projectId) {
  return new Promise((resolve, reject) => {
    console.log('In updateData');
    console.log(jsondata);
    console.log('projectId:' + projectId);

    var conn = new sql.ConnectionPool(connection.getconnection().dbConfig)
    conn.connect()
      // Successful connection
      .then(function () {
        console.log('In connection successful');

        let transaction = new sql.Transaction(conn);
        transaction.begin().then(async function () {
          console.log('In transaction begin successful');
          let request = new sql.Request(transaction);
          let iExceptionFlag = false;
          let errorMessage = '';
          try {
            let deleteString = 'DELETE FROM ' + connection.schemaName + '.PROJECT_WORKSPACE where project_id = ' + projectId + ` AND entity_name = 'DEVELOPMENT_TOOLS'`;
            console.log('deleteString:' + deleteString);
            await request.query(deleteString);
            console.log('jsondata.length:' + jsondata.length);
            for (var i = 0; i < jsondata.length; i++) {
              tabContent = jsondata[i].tabContent;
              console.log('tabContent.length:' + tabContent.length);
              for (var l = 0; l < tabContent.length; l++) {
                L1value = tabContent[l].L1value;
                toolgrp = tabContent[l].toolgrp;
                for (var j = 0; j < toolgrp.length; j++) {
                  toolname = toolgrp[j].toolname;
                  if (toolgrp[j].toolenabledFlag == 'Y') {
                    console.log('L1value:' + L1value + ' toolname:' + toolname);
                    let entityName = 'DEVELOPMENT_TOOLS';
                    let entityTable = 'TOOLS_ACCELERATORS_NEW';
                    let seperator = `','`;
                    let endBlock = `')`;
                    let insertString = `insert into ` + connection.schemaName + `.PROJECT_WORKSPACE (project_id, entity_name, entity_value, ENTITY_TABLE, L3) VALUES ('` + projectId +
                      seperator + entityName + seperator + toolgrp[j].toolname + seperator + entityTable + seperator + L1value + endBlock;
                    console.log('insertString:' + insertString);
                    await request.query(insertString);
                  }
                }
              }
            }
          }
          catch (e) {
            iExceptionFlag = true;
            errorMessage = e;
            console.log('Exception:' + e);
          }
          console.log(iExceptionFlag)
          if (iExceptionFlag) {
            transaction.rollback().then(function () {
              console.log('In rollback:then');
              conn.close();
              resolve({ "MSG": "Error while inserting data into database:" + errorMessage });
            })
              .catch(function (err) {
                console.log('In rollback:catch');
                conn.close();
                resolve({ "MSG": "Error while rolling back transaction:" + errorMessage });
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
                resolve({ "MSG": "Error while commiting transaction:" + errorMessage });
              });
          }


        })
          .catch(function (err) {
            console.log('In transaction begin catch:' + err);
            conn.close();
            resolve({ "MSG": "Error while creating transaction object:" + errorMessage });
          })

      })
      .catch(function (err) {
        console.log('In connection catch catch:' + err)
        conn.close();
        resolve({ "MSG": "Error while creating connection object:" + errorMessage });
      })

  })
},


getActivateDigitalOrganizationOCM: function(userroleid, industry, sector, region, l1, l2, l3) {

    return new Promise((resolve, reject) => {
        var conn = new sql.ConnectionPool(connection.getconnection().dbConfig)

        conn.connect()
            // Successful connection
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
                  `SELECT L1name 'L1value',L1name 'L1linkname',
                  (SELECT CONCAT(b1.doc_link, '/', b1.file_name)
                            FROM ASCEND.documents b1
                           WHERE e.L1name = b1.name
                             AND b1.type = 'Activate digital organization OCM') L1doclink,
                  (CASE WHEN ((SELECT COUNT(1)
                       FROM ASCEND.project_workspace pws
                         ,ASCEND.user_roles ur
                       WHERE ur.ID = @userroleid
                                         AND ur.project_id = pws.project_id
                                         AND pws.entity_name = 'ACTIVATE_DIGITAL_ORGANIZATION_OCM'
                       AND e.L1name = pws.L1) = 1)
                    THEN 'Y' else  'N'
                    END) L1enabledflag
             FROM ascend.change_management e
      where 1=1
	  and e.phase = 'Deliver'
	  AND e.stop = 'Activate digital organization'
      and (@industry = '0' OR ISNULL(e.industry,'COMMON') = 'COMMON' OR e.industry in (select ind.industry
                                                                                 from ascend.industries_new_i ind,
                                                                                 (select value from STRING_SPLIT(@industry, ',')) s
                                                                                 where s.value = ind.id) )
                         and (@sector = '0' OR ISNULL(e.sector,'COMMON') = 'COMMON' OR e.sector in (select sec.sector
                                                                                 from ascend.industries_new_s sec,
                                                                                 (select value from STRING_SPLIT(@sector, ',')) s
                                                                                 where s.value = sec.id) )
                         and (@region = '0' OR ISNULL(e.region,'Core') = 'Core' OR e.region in (select reg.NAME
                                                                                 from ascend.region_new reg,
                                                                                 (select value from STRING_SPLIT(@region, ',')) s
                                                                                 where s.value = reg.DESCRIPTION) )
                         and (@l1 = '0'  OR upper(e.process_area_l1) = 'COMMON'  OR ISNULL(e.process_area_l1,'0') = '0' OR e.process_area_l1 in (select l1.l1
                                                                                             from ascend.business_processes_new_l1 l1,
                                                                                             (select value from STRING_SPLIT(@l1, ',')) s
                                                                                             where s.value = l1.id))
                         and (@l2 = '0' OR upper(e.process_area_l2) = 'COMMON'  OR ISNULL(e.process_area_l2,'0') = '0' OR e.process_area_l2 in (select l2.l2
                                                                                             from ascend.business_processes_new_l2 l2,
                                                                                             (select value from STRING_SPLIT(@l2, ',')) s
                                                                                             where s.value = l2.id))
                         and (@l3 = '0'  OR upper(e.process_area_l3) = 'COMMON' OR ISNULL(e.process_area_l3,'0') = '0' OR e.process_area_l3 in (select l3.l3
                                                                                             from ascend.business_processes_new_l3 l3,
                                                                                             (select value from STRING_SPLIT(@l3, ',')) s
                                                                                             where s.value = l3.id))
FOR JSON PATH, INCLUDE_NULL_VALUES`
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

postActivateDigitalOrganizationOCM:function(jsondata,projectId) {
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
                    let deleteString = 'DELETE FROM '+connection.schemaName+'.PROJECT_WORKSPACE where project_id = '+projectId+` AND entity_name = 'ACTIVATE_DIGITAL_ORGANIZATION_OCM'`;
                    console.log('deleteString:'+deleteString);
                    await request.query(deleteString);
                    console.log('jsondata.length:'+jsondata.length);
                    for (var i = 0; i < jsondata.length; i++) {
					  tabContent = jsondata[i].tabContent;
					  console.log('tabContent.length:'+tabContent.length);
					  for (var l = 0; l < tabContent.length; l++) {
                      L1value = tabContent[l].L1value;
                      if (tabContent[l].L1enabledflag == 'Y') {
                                  console.log('L1value:'+tabContent[l].L1value);
                                  let entityName = 'ACTIVATE_DIGITAL_ORGANIZATION_OCM';
                                  let entityTable = 'CHANGE_MANAGEMENT';
                                  let seperator = `','`;
                                  let endBlock = `')`;
                                  let insertString = `insert into `+connection.schemaName+`.PROJECT_WORKSPACE (project_id, entity_name, entity_value, ENTITY_TABLE, L1) VALUES ('`+projectId+
                                                      seperator+entityName+seperator+tabContent[l].L1value+seperator+entityTable+seperator+L1value+endBlock;
                                  console.log('insertString:'+insertString);
                                  await request.query(insertString);
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

getConstructOCM: function(userroleid, industry, sector, region, l1, l2, l3) {

    return new Promise((resolve, reject) => {
        var conn = new sql.ConnectionPool(connection.getconnection().dbConfig)

        conn.connect()
            // Successful connection
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
                  `SELECT L1name 'L1value',L1name 'L1linkname',
                  (SELECT CONCAT(b1.doc_link, '/', b1.file_name)
                            FROM ASCEND.documents b1
                           WHERE e.L1name = b1.name
                             AND b1.type = 'Construct OCM') L1doclink,
                  (CASE WHEN ((SELECT COUNT(1)
                       FROM ASCEND.project_workspace pws
                         ,ASCEND.user_roles ur
                       WHERE ur.ID = @userroleid
                                         AND ur.project_id = pws.project_id
                                         AND pws.entity_name = 'CONSTRUCT_OCM'
                       AND e.L1name = pws.L1) = 1)
                    THEN 'Y' else  'N'
                    END) L1enabledflag
             FROM ascend.change_management e
      where 1=1
	  and e.phase = 'Deliver'
	  AND e.stop = 'Construct'
      and (@industry = '0' OR ISNULL(e.industry,'COMMON') = 'COMMON' OR e.industry in (select ind.industry
                                                                                 from ascend.industries_new_i ind,
                                                                                 (select value from STRING_SPLIT(@industry, ',')) s
                                                                                 where s.value = ind.id) )
                         and (@sector = '0' OR ISNULL(e.sector,'COMMON') = 'COMMON' OR e.sector in (select sec.sector
                                                                                 from ascend.industries_new_s sec,
                                                                                 (select value from STRING_SPLIT(@sector, ',')) s
                                                                                 where s.value = sec.id) )
                         and (@region = '0' OR ISNULL(e.region,'Core') = 'Core' OR e.region in (select reg.NAME
                                                                                 from ascend.region_new reg,
                                                                                 (select value from STRING_SPLIT(@region, ',')) s
                                                                                 where s.value = reg.DESCRIPTION) )
                         and (@l1 = '0'  OR upper(e.process_area_l1) = 'COMMON'  OR ISNULL(e.process_area_l1,'0') = '0' OR e.process_area_l1 in (select l1.l1
                                                                                             from ascend.business_processes_new_l1 l1,
                                                                                             (select value from STRING_SPLIT(@l1, ',')) s
                                                                                             where s.value = l1.id))
                         and (@l2 = '0' OR upper(e.process_area_l2) = 'COMMON'  OR ISNULL(e.process_area_l2,'0') = '0' OR e.process_area_l2 in (select l2.l2
                                                                                             from ascend.business_processes_new_l2 l2,
                                                                                             (select value from STRING_SPLIT(@l2, ',')) s
                                                                                             where s.value = l2.id))
                         and (@l3 = '0'  OR upper(e.process_area_l3) = 'COMMON' OR ISNULL(e.process_area_l3,'0') = '0' OR e.process_area_l3 in (select l3.l3
                                                                                             from ascend.business_processes_new_l3 l3,
                                                                                             (select value from STRING_SPLIT(@l3, ',')) s
                                                                                             where s.value = l3.id))
FOR JSON PATH, INCLUDE_NULL_VALUES`
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

postConstructOCM:function(jsondata,projectId) {
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
                    let deleteString = 'DELETE FROM '+connection.schemaName+'.PROJECT_WORKSPACE where project_id = '+projectId+` AND entity_name = 'CONSTRUCT_OCM'`;
                    console.log('deleteString:'+deleteString);
                    await request.query(deleteString);
                    console.log('jsondata.length:'+jsondata.length);
                    for (var i = 0; i < jsondata.length; i++) {
					  tabContent = jsondata[i].tabContent;
					  console.log('tabContent.length:'+tabContent.length);
					  for (var l = 0; l < tabContent.length; l++) {
                      L1value = tabContent[l].L1value;
                      if (tabContent[l].L1enabledflag == 'Y') {
                                  console.log('L1value:'+tabContent[l].L1value);
                                  let entityName = 'CONSTRUCT_OCM';
                                  let entityTable = 'CHANGE_MANAGEMENT';
                                  let seperator = `','`;
                                  let endBlock = `')`;
                                  let insertString = `insert into `+connection.schemaName+`.PROJECT_WORKSPACE (project_id, entity_name, entity_value, ENTITY_TABLE, L1) VALUES ('`+projectId+
                                                      seperator+entityName+seperator+tabContent[l].L1value+seperator+entityTable+seperator+L1value+endBlock;
                                  console.log('insertString:'+insertString);
                                  await request.query(insertString);
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

getValidateOCM: function(userroleid, industry, sector, region, l1, l2, l3) {

    return new Promise((resolve, reject) => {
        var conn = new sql.ConnectionPool(connection.getconnection().dbConfig)

        conn.connect()
            // Successful connection
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
                  `SELECT L1name 'L1value',L1name 'L1linkname',
                  (SELECT CONCAT(b1.doc_link, '/', b1.file_name)
                            FROM ASCEND.documents b1
                           WHERE e.L1name = b1.name
                             AND b1.type = 'Validate OCM') L1doclink,
                  (CASE WHEN ((SELECT COUNT(1)
                       FROM ASCEND.project_workspace pws
                         ,ASCEND.user_roles ur
                       WHERE ur.ID = @userroleid
                                         AND ur.project_id = pws.project_id
                                         AND pws.entity_name = 'VALIDATE_OCM'
                       AND e.L1name = pws.L1) = 1)
                    THEN 'Y' else  'N'
                    END) L1enabledflag
             FROM ascend.change_management e
      where 1=1
	  and e.phase = 'Deliver'
	  AND e.stop = 'Validate'
      and (@industry = '0' OR ISNULL(e.industry,'COMMON') = 'COMMON' OR e.industry in (select ind.industry
                                                                                 from ascend.industries_new_i ind,
                                                                                 (select value from STRING_SPLIT(@industry, ',')) s
                                                                                 where s.value = ind.id) )
                         and (@sector = '0' OR ISNULL(e.sector,'COMMON') = 'COMMON' OR e.sector in (select sec.sector
                                                                                 from ascend.industries_new_s sec,
                                                                                 (select value from STRING_SPLIT(@sector, ',')) s
                                                                                 where s.value = sec.id) )
                         and (@region = '0' OR ISNULL(e.region,'Core') = 'Core' OR e.region in (select reg.NAME
                                                                                 from ascend.region_new reg,
                                                                                 (select value from STRING_SPLIT(@region, ',')) s
                                                                                 where s.value = reg.DESCRIPTION) )
                         and (@l1 = '0'  OR upper(e.process_area_l1) = 'COMMON'  OR ISNULL(e.process_area_l1,'0') = '0' OR e.process_area_l1 in (select l1.l1
                                                                                             from ascend.business_processes_new_l1 l1,
                                                                                             (select value from STRING_SPLIT(@l1, ',')) s
                                                                                             where s.value = l1.id))
                         and (@l2 = '0' OR upper(e.process_area_l2) = 'COMMON'  OR ISNULL(e.process_area_l2,'0') = '0' OR e.process_area_l2 in (select l2.l2
                                                                                             from ascend.business_processes_new_l2 l2,
                                                                                             (select value from STRING_SPLIT(@l2, ',')) s
                                                                                             where s.value = l2.id))
                         and (@l3 = '0'  OR upper(e.process_area_l3) = 'COMMON' OR ISNULL(e.process_area_l3,'0') = '0' OR e.process_area_l3 in (select l3.l3
                                                                                             from ascend.business_processes_new_l3 l3,
                                                                                             (select value from STRING_SPLIT(@l3, ',')) s
                                                                                             where s.value = l3.id))
FOR JSON PATH, INCLUDE_NULL_VALUES`
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

postValidateOCM:function(jsondata,projectId) {
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
                    let deleteString = 'DELETE FROM '+connection.schemaName+'.PROJECT_WORKSPACE where project_id = '+projectId+` AND entity_name = 'VALIDATE_OCM'`;
                    console.log('deleteString:'+deleteString);
                    await request.query(deleteString);
                    console.log('jsondata.length:'+jsondata.length);
                    for (var i = 0; i < jsondata.length; i++) {
					  tabContent = jsondata[i].tabContent;
					  console.log('tabContent.length:'+tabContent.length);
					  for (var l = 0; l < tabContent.length; l++) {
                      L1value = tabContent[l].L1value;
                      if (tabContent[l].L1enabledflag == 'Y') {
                                  console.log('L1value:'+tabContent[l].L1value);
                                  let entityName = 'VALIDATE_OCM';
                                  let entityTable = 'CHANGE_MANAGEMENT';
                                  let seperator = `','`;
                                  let endBlock = `')`;
                                  let insertString = `insert into `+connection.schemaName+`.PROJECT_WORKSPACE (project_id, entity_name, entity_value, ENTITY_TABLE, L1) VALUES ('`+projectId+
                                                      seperator+entityName+seperator+tabContent[l].L1value+seperator+entityTable+seperator+L1value+endBlock;
                                  console.log('insertString:'+insertString);
                                  await request.query(insertString);
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

getDeployOCM: function(userroleid, industry, sector, region, l1, l2, l3) {

    return new Promise((resolve, reject) => {
        var conn = new sql.ConnectionPool(connection.getconnection().dbConfig)

        conn.connect()
            // Successful connection
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
                  `SELECT L1name 'L1value',L1name 'L1linkname',
                  (SELECT CONCAT(b1.doc_link, '/', b1.file_name)
                            FROM ASCEND.documents b1
                           WHERE e.L1name = b1.name
                             AND b1.type = 'Deploy OCM') L1doclink,
                  (CASE WHEN ((SELECT COUNT(1)
                       FROM ASCEND.project_workspace pws
                         ,ASCEND.user_roles ur
                       WHERE ur.ID = @userroleid
                                         AND ur.project_id = pws.project_id
                                         AND pws.entity_name = 'DEPLOY_OCM'
                       AND e.L1name = pws.L1) = 1)
                    THEN 'Y' else  'N'
                    END) L1enabledflag
             FROM ascend.change_management e
      where 1=1
	  and e.phase = 'Deliver'
	  AND e.stop = 'Deploy'
      and (@industry = '0' OR ISNULL(e.industry,'COMMON') = 'COMMON' OR e.industry in (select ind.industry
                                                                                 from ascend.industries_new_i ind,
                                                                                 (select value from STRING_SPLIT(@industry, ',')) s
                                                                                 where s.value = ind.id) )
                         and (@sector = '0' OR ISNULL(e.sector,'COMMON') = 'COMMON' OR e.sector in (select sec.sector
                                                                                 from ascend.industries_new_s sec,
                                                                                 (select value from STRING_SPLIT(@sector, ',')) s
                                                                                 where s.value = sec.id) )
                         and (@region = '0' OR ISNULL(e.region,'Core') = 'Core' OR e.region in (select reg.NAME
                                                                                 from ascend.region_new reg,
                                                                                 (select value from STRING_SPLIT(@region, ',')) s
                                                                                 where s.value = reg.DESCRIPTION) )
                         and (@l1 = '0'  OR upper(e.process_area_l1) = 'COMMON'  OR ISNULL(e.process_area_l1,'0') = '0' OR e.process_area_l1 in (select l1.l1
                                                                                             from ascend.business_processes_new_l1 l1,
                                                                                             (select value from STRING_SPLIT(@l1, ',')) s
                                                                                             where s.value = l1.id))
                         and (@l2 = '0' OR upper(e.process_area_l2) = 'COMMON'  OR ISNULL(e.process_area_l2,'0') = '0' OR e.process_area_l2 in (select l2.l2
                                                                                             from ascend.business_processes_new_l2 l2,
                                                                                             (select value from STRING_SPLIT(@l2, ',')) s
                                                                                             where s.value = l2.id))
                         and (@l3 = '0'  OR upper(e.process_area_l3) = 'COMMON' OR ISNULL(e.process_area_l3,'0') = '0' OR e.process_area_l3 in (select l3.l3
                                                                                             from ascend.business_processes_new_l3 l3,
                                                                                             (select value from STRING_SPLIT(@l3, ',')) s
                                                                                             where s.value = l3.id))
FOR JSON PATH, INCLUDE_NULL_VALUES`
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

postDeployOCM:function(jsondata,projectId) {
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
                    let deleteString = 'DELETE FROM '+connection.schemaName+'.PROJECT_WORKSPACE where project_id = '+projectId+` AND entity_name = 'DEPLOY_OCM'`;
                    console.log('deleteString:'+deleteString);
                    await request.query(deleteString);
                    console.log('jsondata.length:'+jsondata.length);
                    for (var i = 0; i < jsondata.length; i++) {
					  tabContent = jsondata[i].tabContent;
					  console.log('tabContent.length:'+tabContent.length);
					  for (var l = 0; l < tabContent.length; l++) {
                      L1value = tabContent[l].L1value;
                      if (tabContent[l].L1enabledflag == 'Y') {
                                  console.log('L1value:'+tabContent[l].L1value);
                                  let entityName = 'DEPLOY_OCM';
                                  let entityTable = 'CHANGE_MANAGEMENT';
                                  let seperator = `','`;
                                  let endBlock = `')`;
                                  let insertString = `insert into `+connection.schemaName+`.PROJECT_WORKSPACE (project_id, entity_name, entity_value, ENTITY_TABLE, L1) VALUES ('`+projectId+
                                                      seperator+entityName+seperator+tabContent[l].L1value+seperator+entityTable+seperator+L1value+endBlock;
                                  console.log('insertString:'+insertString);
                                  await request.query(insertString);
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

getconstructdeliverables: function(userroleid, industry, sector, region, l1, l2, l3) {

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
                                                  AND b1.type = 'Construct Deliverables') L2doclink,
                                                (CASE
                                                  WHEN ((SELECT COUNT(1)
                                                      FROM ASCEND.project_workspace pws
                                                        ,ASCEND.user_roles ur
                                                      WHERE ur.ID = @userroleid
                                                                                  and ur.project_id = pws.project_id
                                                                                  AND pws.entity_name = 'CONSTRUCT_DELIVERABLES'
                                                      AND d.l2name = pws.L2
                                                      AND d.l1name = pws.L1) = 1)
                                                  THEN 'Y' else  'N'
                                                  END) L2enabledflag
                                             FROM ( select distinct L1name,L1description,L2name,L2description,L3name,region,industry,sector,process_area_l1,process_area_l2,process_area_l3 from ascend.deliverables where stop='Construct'
                                            AND phase= 'Deliver') d
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
                                   FROM ( select distinct L1name,L1description,L2name,L2description,L3name,region,industry,sector,process_area_l1,process_area_l2,process_area_l3 from ascend.deliverables where stop='Construct'
                                            AND phase= 'Deliver') c
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
                                                              AND pws.entity_name = 'CONSTRUCT_DELIVERABLES'
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

  postconstructdeliverables:function(jsondata,projectId) {
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
                    let deleteString = 'DELETE FROM '+connection.schemaName+'.PROJECT_WORKSPACE where project_id = '+projectId+` AND entity_name = 'CONSTRUCT_DELIVERABLES'`;
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
                                  let entityName = 'CONSTRUCT_DELIVERABLES';
                                  let entityTable = 'DELIVERABLES';
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


getvalidatedeliverables: function(userroleid, industry, sector, region, l1, l2, l3) {

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
                                                  AND b1.type = 'Validate Deliverables') L2doclink,
                                                (CASE
                                                  WHEN ((SELECT COUNT(1)
                                                      FROM ASCEND.project_workspace pws
                                                        ,ASCEND.user_roles ur
                                                      WHERE ur.ID = @userroleid
                                                                                  and ur.project_id = pws.project_id
                                                                                  AND pws.entity_name = 'VALIDATE_DELIVERABLES'
                                                      AND d.l2name = pws.L2
                                                      AND d.l1name = pws.L1) = 1)
                                                  THEN 'Y' else  'N'
                                                  END) L2enabledflag
                                             FROM ( select distinct L1name,L1description,L2name,L2description,L3name,region,industry,sector,process_area_l1,process_area_l2,process_area_l3 from ascend.deliverables where stop='Validate'
                                            AND phase= 'Deliver') d
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
                                   FROM ( select distinct L1name,L1description,L2name,L2description,L3name,region,industry,sector,process_area_l1,process_area_l2,process_area_l3 from ascend.deliverables where stop='Validate'
                                            AND phase= 'Deliver') c
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
                                                              AND pws.entity_name = 'VALIDATE_DELIVERABLES'
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

  postvalidatedeliverables:function(jsondata,projectId) {
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
                    let deleteString = 'DELETE FROM '+connection.schemaName+'.PROJECT_WORKSPACE where project_id = '+projectId+` AND entity_name = 'VALIDATE_DELIVERABLES'`;
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
                                  let entityName = 'VALIDATE_DELIVERABLES';
                                  let entityTable = 'DELIVERABLES';
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
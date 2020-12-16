var connection = require('./connection-file');
var sql = require("mssql");

module.exports = {
  /************************************************************** *
      ALL METHODS SHOULD BE ADDED IN THIS module.exports AFTER getSampleService
      METHOD.
  /************************************************************** */

  getArchitectBusinessProcess: function (userroleid, industry, sector, region, l1, l2, l3) {

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
           `with L3Tab as (SELECT distinct a3.l1
                   ,a3.l2
                   ,(select a.l3 'description'
                           ,a.l3 'businessprocesslinkname'
                           ,(CASE
                                WHEN ((SELECT count(1)
                                        FROM ASCEND.project_workspace pws
                                            ,ASCEND.user_roles ur
                                        WHERE ur.ID = '`+ userroleid + `'
                                        and ur.project_id = pws.project_id
                                        AND a.l1 = pws.entity_value
                                        AND pws.entity_name = 'PROCESS_FLOWS'
                                        AND a.l2 = pws.L2
                                        AND a.l3 = pws.L3
                                    ) = 1)
                                THEN 'Y' else 'N'
                                END) 'businessprocessenabledflag'
                           ,(select concat(b1.doc_link, '/', b1.file_name)
                               from ascend.documents b1
                               where b1.name = a.l3
                                 and b1.type = 'Process Flows'
                               ) 'businessprocessdoclink'
                        from ASCEND.business_processes_new a
                    where a3.l1 = a.L1
                      and a3.l2 = a.l2
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
                                        and (@l1 = '0'  OR upper(a.l1) = 'COMMON' OR ISNULL(a.L1,'0') = '0' OR a.L1 in (select l1.l1
                                                                                                            from ascend.business_processes_new_l1 l1,
                                                                                                            (select value from STRING_SPLIT(@l1, ',')) s
                                                                                                            where s.value = l1.id))
                                        and (@l2 = '0'  OR upper(a.l2) = 'COMMON' OR ISNULL(a.L2,'0') = '0' OR a.L2 in (select l2.l2
                                                                                                            from ascend.business_processes_new_l2 l2,
                                                                                                            (select value from STRING_SPLIT(@l2, ',')) s
                                                                                                            where s.value = l2.id))
                                        and (@l3 = '0'  OR upper(a.l3) = 'COMMON' OR ISNULL(a.L3,'0') = '0' OR a.L3 in (select l3.l3
                                                                                                            from ascend.business_processes_new_l3 l3,
                                                                                                            (select value from STRING_SPLIT(@l3, ',')) s
                                                                                                            where s.value = l3.id))			  
                   for json path,include_null_values) 'L3Grp'
    FROM ASCEND.business_processes_new a3
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
                                        and (@l1 = '0'  OR upper(a3.l1) = 'COMMON' OR ISNULL(a3.L1,'0') = '0' OR a3.L1 in (select l1.l1
                                                                                                            from ascend.business_processes_new_l1 l1,
                                                                                                            (select value from STRING_SPLIT(@l1, ',')) s
                                                                                                            where s.value = l1.id))
                                        and (@l2 = '0'  OR upper(a3.l2) = 'COMMON' OR ISNULL(a3.L2,'0') = '0' OR a3.L2 in (select l2.l2
                                                                                                            from ascend.business_processes_new_l2 l2,
                                                                                                            (select value from STRING_SPLIT(@l2, ',')) s
                                                                                                            where s.value = l2.id))
                                        and (@l3 = '0'  OR upper(a3.l3) = 'COMMON' OR ISNULL(a3.L3,'0') = '0' OR a3.L3 in (select l3.l3
                                                                                                            from ascend.business_processes_new_l3 l3,
                                                                                                            (select value from STRING_SPLIT(@l3, ',')) s
                                                                                                            where s.value = l3.id))
    ),
      L2Tab as (select distinct a2.L1
           ,(select a.L2 'L2value'
                    ,(CASE
                                WHEN ((SELECT count(1)
                                        FROM ASCEND.project_workspace pws
                                            ,ASCEND.user_roles ur
                                        WHERE ur.ID = @userroleid
                                        and ur.project_id = pws.project_id
                                        AND pws.entity_name = 'PROCESS_FLOWS'
                                        AND a.l1 = pws.entity_value
                                        AND a.l2 = pws.L2
                                    ) >= 1)
                                THEN 'Y' else 'N'
                                END)      'L2enabledflag'
                    ,a.L2 'L2linkname'
                    ,(select concat(b1.doc_link, '/', b1.file_name)
                               from ascend.documents b1
                               where b1.name = a.l2
                                 and b1.type = 'Process Flows'
                      ) 'L2doclink'
                    ,a.L3Grp 'L3grp'
              from L3Tab a
              where a.L1 = a2.l1
                for json path,include_null_values) 'L2Grp'
    from L3Tab a2),
    t as (select a1.L1 'L1value'
          ,(CASE
            WHEN ((SELECT count(1)
                    FROM ASCEND.project_workspace pws
                        ,ASCEND.user_roles ur
                    WHERE ur.ID = @userroleid
                    and ur.project_id = pws.project_id
                    AND pws.entity_name = 'PROCESS_FLOWS'
                    AND a1.l1 = pws.entity_value
                ) >= 1)
            THEN 'Y' else 'N'
            END) 'L1enabledflag'
           ,a1.L1 'L1linkname'
           ,(select concat(b1.doc_link, '/', b1.file_name)
                               from ascend.documents b1
                               where b1.name = a1.l1
                                 and b1.type = 'Process Flows'
            ) 'L1doclink'
            ,a1.L2Grp 'L2grp'
    from L2Tab a1
    )
    select * from t
for json path,include_null_values`
          )

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
  getArchitectKbd: function (userroleid, industry, sector, region, l1, l2, l3) {

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

          req.query(`WITH l3group AS (SELECT DISTINCT a.process_area_l1 l1,
                								 a.process_area_l2 l2,
                								(SELECT b.decision_name decisionname,
                										b.decision_impact decisionimpact,
                										b.decision_description description,
                										b.decision_name decisionelinkname,
                										(SELECT CONCAT(b1.doc_link, '/', b1.file_name)
                										   FROM ASCEND.documents b1
                										  WHERE b.decision_name = b1.name
                										    AND b1.type = 'KDD') decisiondoclink,
                										(CASE
                											WHEN ((SELECT COUNT(1)
                													FROM ASCEND.project_workspace pws
                														,ASCEND.user_roles ur
                													WHERE ur.ID = '`+ userroleid + `'
                													and ur.project_id = pws.project_id
                                                                    AND b.decision_name = pws.entity_value
                                                                    AND pws.entity_name = 'KEY_DESIGN_DECISIONS'
                													AND b.process_area_l2 = pws.L2
                													AND b.process_area_l1 = pws.L3) = 1)
                											THEN 'Y' else 'N'
                											END) decisionenabledFlag
                								   FROM ASCEND.key_business_decision_new b
                								  WHERE a.process_area_l1 = b.process_area_l1
                								    AND a.process_area_l2 = b.process_area_l2
								and (@industry = '0' OR ISNULL(b.industry,'COMMON') = 'COMMON' OR b.industry in (select ind.industry
                                                                                            from ascend.industries_new_i ind,
                                                                                            (select value from STRING_SPLIT(@industry, ',')) s
                                                                                            where s.value = ind.id) )
                                    and (@sector = '0' OR ISNULL(b.sector,'COMMON') = 'COMMON' OR b.sector in (select sec.sector
                                                                                            from ascend.industries_new_s sec,
                                                                                            (select value from STRING_SPLIT(@sector, ',')) s
                                                                                            where s.value = sec.id) )
                                    and (@region = '0' OR ISNULL(b.region,'Core') = 'Core' OR b.region in (select reg.NAME
                                                                                            from ascend.region_new reg,
                                                                                            (select value from STRING_SPLIT(@region, ',')) s
                                                                                            where s.value = reg.DESCRIPTION) )
                                    and (@l1 = '0'  OR upper(b.process_area_l1) = 'COMMON' OR ISNULL(b.process_area_l1,'0') = '0' OR b.process_area_l1 in (select l1.l1
                                                                                                        from ascend.business_processes_new_l1 l1,
                                                                                                        (select value from STRING_SPLIT(@l1, ',')) s
                                                                                                        where s.value = l1.id))
                                    and (@l2 = '0'  OR upper(b.process_area_l2) = 'COMMON' OR ISNULL(b.process_area_l2,'0') = '0' OR b.process_area_l2 in (select l2.l2
                                                                                                        from ascend.business_processes_new_l2 l2,
                                                                                                        (select value from STRING_SPLIT(@l2, ',')) s
                                                                                                        where s.value = l2.id))
                                    and (@l3 = '0'  OR upper(b.process_area_l3) = 'COMMON' OR ISNULL(b.process_area_l3,'0') = '0' OR b.process_area_l3 in (select l3.l3
                                                                                                        from ascend.business_processes_new_l3 l3,
                                                                                                        (select value from STRING_SPLIT(@l3, ',')) s
                                                                                                        where s.value = l3.id))													
                								    FOR JSON PATH, INCLUDE_NULL_VALUES) AS l3grp
                				FROM ASCEND.key_business_decision_new a
                			   WHERE 1=1
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
                                    and (@l1 = '0'  OR upper(a.process_area_l1) = 'COMMON' OR ISNULL(a.process_area_l1,'0') = '0' OR a.process_area_l1 in (select l1.l1
                                                                                                        from ascend.business_processes_new_l1 l1,
                                                                                                        (select value from STRING_SPLIT(@l1, ',')) s
                                                                                                        where s.value = l1.id))
                                    and (@l2 = '0'  OR upper(a.process_area_l2) = 'COMMON' OR ISNULL(a.process_area_l2,'0') = '0' OR a.process_area_l2 in (select l2.l2
                                                                                                        from ascend.business_processes_new_l2 l2,
                                                                                                        (select value from STRING_SPLIT(@l2, ',')) s
                                                                                                        where s.value = l2.id))
                                    and (@l3 = '0'  OR upper(a.process_area_l3) = 'COMMON' OR ISNULL(a.process_area_l3,'0') = '0' OR a.process_area_l3 in (select l3.l3
                                                                                                        from ascend.business_processes_new_l3 l3,
                                                                                                        (select value from STRING_SPLIT(@l3, ',')) s
                                                                                                        where s.value = l3.id))
																															   ),
                     l2group AS  (SELECT DISTINCT c.l1 L1value,
                								  (SELECT d.l2 L2value,
                										  d.l2 L2linkname,
                										  (SELECT CONCAT(b1.doc_link, '/', b1.file_name)
                											 FROM ASCEND.documents b1
                											WHERE d.l2 = b1.name
                											  AND b1.type = 'KDD') L2doclink,
                										  (CASE
                											  WHEN ((SELECT COUNT(1)
                														FROM ASCEND.project_workspace pws
                															,ASCEND.user_roles ur
                														WHERE ur.ID = @userroleid
                                                                        and ur.project_id = pws.project_id
                                                                        AND pws.entity_name = 'KEY_DESIGN_DECISIONS'
                														AND d.l2 = pws.L2
                														AND d.l1 = pws.L3) >= 1)
                											  THEN 'Y' else 'N'
                											  END) L2enabledflag,
                										 d.l3grp 'L3grp'
                									 FROM l3group d
                									WHERE c.l1 = d.l1
                									  FOR JSON PATH, INCLUDE_NULL_VALUES) AS L2grp
                				 FROM l3group c),
                	t AS (SELECT e.L1value L1value,
                					   (SELECT CONCAT(b1.doc_link, '/', b1.file_name)
                						  FROM ASCEND.documents b1
                						 WHERE e.L1value = b1.name
                						   AND b1.type = 'KDD') L1doclink,
                					   CONCAT('/assets/documents/', e.L1value) L1linkname,
                					   (CASE WHEN ((SELECT COUNT(1)
                									FROM ASCEND.project_workspace pws
                										,ASCEND.user_roles ur
                									WHERE ur.ID = @userroleid
                                                    AND ur.project_id = pws.project_id
                                                    AND pws.entity_name = 'KEY_DESIGN_DECISIONS'
                									AND e.L1value = pws.L3) >= 1)
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
  getDesignInterface: function (userroleid, industry, sector, region, l1, l2, l3) {

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
                ,(select a.object_name 'interfacename'
                       ,a.object_description 'description'
                       ,(CASE
                            WHEN ((SELECT count(1)
                                    FROM ASCEND.project_workspace pws
                                        ,ASCEND.user_roles ur
                                    WHERE ur.ID = '`+ userroleid + `'
                                    and ur.project_id = pws.project_id
                                    and pws.entity_name = 'INTERFACES'
                                    AND a.object_name = pws.entity_value
                                    AND ISNULL(a.oracle_module,'X') = ISNULL(pws.L2,'X')
                                    AND ISNULL(a.process_area_l1,'X') = ISNULL(pws.L3,'X')
                                ) = 1)
                            THEN 'Y' else 'N'
                            END) 'interfaceenabledFlag'
                        ,a.object_name 'interfacelinkname'
                       ,(select concat(b1.doc_link, '/', b1.file_name)
                           from ascend.documents b1
                           where b1.name = a.object_name
                             and b1.type = 'Interfaces'
                           ) 'interfacedoclink'
                        ,(case when a.data_flow_direction = 'IN'
                                then 'Inbound'
                                else 'Outbound'
                            end) 'interfacetype'
                    from ASCEND.tech_object_library a
                        where a.process_area_l1 = a3.process_area_l1
                        and a.oracle_module = a3.oracle_module
                        and a.alliance_product = a3.alliance_product
                        and isnull(a.toolkit_link,'X') = isnull(a3.toolkit_link,'X')
						and a.object_Type = 'Interface'
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
                                    and (@l1 = '0'  OR upper(a.process_area_l1) = 'COMMON'  OR ISNULL(a.process_area_l1,'0') = '0' OR a.process_area_l1 in (select l1.l1
                                                                                                        from ascend.business_processes_new_l1 l1,
                                                                                                        (select value from STRING_SPLIT(@l1, ',')) s
                                                                                                        where s.value = l1.id))
                                    and (@l2 = '0'   OR upper(a.process_area_l2) = 'COMMON' OR ISNULL(a.process_area_l2,'0') = '0' OR a.process_area_l2 in (select l2.l2
                                                                                                        from ascend.business_processes_new_l2 l2,
                                                                                                        (select value from STRING_SPLIT(@l2, ',')) s
                                                                                                        where s.value = l2.id))
                                    and (@l3 = '0'   OR upper(a.process_area_l3) = 'COMMON' OR ISNULL(a.process_area_l3,'0') = '0' OR a.process_area_l3 in (select l3.l3
                                                                                                        from ascend.business_processes_new_l3 l3,
                                                                                                        (select value from STRING_SPLIT(@l3, ',')) s
                                                                                                        where s.value = l3.id))
                    for json path,include_null_values) 'L3Grp'
                FROM ASCEND.tech_object_library a3
                   /* ,ascend.documents b1 */
                where 1=1

                /*b1.type = 'Interfaces'
                 and b1.name = a3.object_name */
	and a3.object_Type = 'Interface'
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
                                    and (@l1 = '0'  OR upper(a3.process_area_l1) = 'COMMON'  OR ISNULL(a3.process_area_l1,'0') = '0' OR a3.process_area_l1 in (select l1.l1
                                                                                                        from ascend.business_processes_new_l1 l1,
                                                                                                        (select value from STRING_SPLIT(@l1, ',')) s
                                                                                                        where s.value = l1.id))
                                    and (@l2 = '0'   OR upper(a3.process_area_l2) = 'COMMON' OR ISNULL(a3.process_area_l2,'0') = '0' OR a3.process_area_l2 in (select l2.l2
                                                                                                        from ascend.business_processes_new_l2 l2,
                                                                                                        (select value from STRING_SPLIT(@l2, ',')) s
                                                                                                        where s.value = l2.id))
                                    and (@l3 = '0'   OR upper(a3.process_area_l3) = 'COMMON' OR ISNULL(a3.process_area_l3,'0') = '0' OR a3.process_area_l3 in (select l3.l3
                                                                                                        from ascend.business_processes_new_l3 l3,
                                                                                                        (select value from STRING_SPLIT(@l3, ',')) s
                                                                                                        where s.value = l3.id))
                ),
                L2Tab as (select distinct a2.process_area_l1
                            ,a2.alliance_product
                            ,(select a.oracle_module 'L2value'
                                ,(CASE
                                    WHEN ((SELECT count(1)
                                            FROM ASCEND.project_workspace pws
                                                ,ASCEND.user_roles ur
                                            WHERE ur.ID = @userroleid
                                            and ur.project_id = pws.project_id
                                            and pws.entity_name = 'INTERFACES'
                                            AND ISNULL(a.oracle_module,'X') = ISNULL(pws.L2,'X')
                                            AND ISNULL(a.process_area_l1,'X') = ISNULL(pws.L3,'X')
                                        ) >= 1)
                                    THEN 'Y' else 'N'
                                    END)      'L2enabledflag'
                                ,a.oracle_module 'L2linkname'
                                ,(select concat(b1.doc_link, '/', b1.file_name)
                                        from ascend.documents b1
                                        where b1.name = a.oracle_module
                                            and b1.type = 'INTERFACES'
                                ) 'L2doclink'
                                ,a.L3Grp 'L3grp'
                        from L3Tab a
                        where a.process_area_l1 = a2.process_area_l1
                          and a.alliance_product = a2.alliance_product
                            for json path,include_null_values) 'L2Grp'
                from L3Tab a2),
                t as (select a1.process_area_l1 'L1value'
                ,(CASE
                WHEN ((SELECT count(1)
                FROM ASCEND.project_workspace pws
                    ,ASCEND.user_roles ur
                WHERE ur.ID = @userroleid
                and ur.project_id = pws.project_id
                and pws.entity_name = 'INTERFACES'
                AND ISNULL(a1.process_area_l1,'X') = ISNULL(pws.L3,'X')
                ) >= 1)
                THEN 'Y' else 'N'
                END)  'L1enabledflag'
                ,concat('/assets/documents/', a1.process_area_l1) 'L1linkname'
                ,(select concat(b1.doc_link, '/', b1.file_name)
                           from ascend.documents b1
                           where b1.name = a1.process_area_l1
                             and b1.type = 'Interfaces'
                ) 'L1doclink'
                ,a1.alliance_product 'technology'
                ,a1.L2Grp 'L2grp'
                from L2Tab a1
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
  getDesignReports: function (userroleid, industry, sector, region, l1, l2, l3) {

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
               ,(select a.object_name reportname
                       ,a.object_description description
                       ,(CASE
                            WHEN ((SELECT count(1)
                                    FROM ASCEND.project_workspace pws
                                        ,ASCEND.user_roles ur
                                    WHERE ur.ID = '`+ userroleid + `'
                                    and ur.project_id = pws.project_id
                                    and pws.entity_name = 'ANALYTICS_REPORTS'
                                    AND a.object_name = pws.entity_value
                                    AND ISNULL(a.oracle_module,'X') = ISNULL(pws.L2,'X')
                                    AND ISNULL(a.process_area_l1,'X') = ISNULL(pws.L3,'X')
                                ) = 1)
                            THEN 'Y' else 'N'
                            END) reportenabledFlag
                        ,a.object_name reportlinkname
                       ,(select concat(b1.doc_link, '/', b1.file_name)
                           from ascend.documents b1
                           where b1.name = a.object_name
                             and b1.type = 'Analytics and Report'
                           ) reportdoclink
                        ,'' reporttype
                    from ASCEND.tech_object_library a
                        where a.process_area_l1 = a3.process_area_l1
                        and a.oracle_module = a3.oracle_module
                        and a.alliance_product = a3.alliance_product
                        and ISNULL(a.toolkit_link,'X') = ISNULL(a3.toolkit_link,'X')
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
                                    and (@l1 = '0'  OR upper(a.process_area_l1) = 'COMMON'  OR ISNULL(a.process_area_l1,'0') = '0' OR a.process_area_l1 in (select l1.l1
                                                                                                        from ascend.business_processes_new_l1 l1,
                                                                                                        (select value from STRING_SPLIT(@l1, ',')) s
                                                                                                        where s.value = l1.id))
                                    and (@l2 = '0' OR upper(a.process_area_l2) = 'COMMON'  OR ISNULL(a.process_area_l2,'0') = '0' OR a.process_area_l2 in (select l2.l2
                                                                                                        from ascend.business_processes_new_l2 l2,
                                                                                                        (select value from STRING_SPLIT(@l2, ',')) s
                                                                                                        where s.value = l2.id))
                                    and (@l3 = '0'  OR upper(a.process_area_l3) = 'COMMON' OR ISNULL(a.process_area_l3,'0') = '0' OR a.process_area_l3 in (select l3.l3
                                                                                                        from ascend.business_processes_new_l3 l3,
                                                                                                        (select value from STRING_SPLIT(@l3, ',')) s
                                                                                                        where s.value = l3.id))						
                    for json path,include_null_values) AS L3Grp
                FROM ASCEND.tech_object_library a3
                    /*,ascend.documents b1*/
                where
                    /*b1.type = 'Analytics and Report'
					and b1.name = a3.object_name
                    and*/
                    a3.object_type = 'Report'
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
                                    and (@l1 = '0'  OR upper(a3.process_area_l1) = 'COMMON'  OR ISNULL(a3.process_area_l1,'0') = '0' OR a3.process_area_l1 in (select l1.l1
                                                                                                        from ascend.business_processes_new_l1 l1,
                                                                                                        (select value from STRING_SPLIT(@l1, ',')) s
                                                                                                        where s.value = l1.id))
                                    and (@l2 = '0' OR upper(a3.process_area_l2) = 'COMMON'  OR ISNULL(a3.process_area_l2,'0') = '0' OR a3.process_area_l2 in (select l2.l2
                                                                                                        from ascend.business_processes_new_l2 l2,
                                                                                                        (select value from STRING_SPLIT(@l2, ',')) s
                                                                                                        where s.value = l2.id))
                                    and (@l3 = '0'  OR upper(a3.process_area_l3) = 'COMMON' OR ISNULL(a3.process_area_l3,'0') = '0' OR a3.process_area_l3 in (select l3.l3
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
                                            and pws.entity_name = 'ANALYTICS_REPORTS'
                                            AND ISNULL(a.oracle_module,'X') = ISNULL(pws.L2,'X')
                                            AND ISNULL(a.process_area_l1,'X') = ISNULL(pws.L3,'X')
                                        ) >= 1)
                                    THEN 'Y' else 'N'
                                    END) L2enabledflag
                                ,a.oracle_module L2linkname
                                ,(select concat(b1.doc_link, '/', b1.file_name)
                                        from ascend.documents b1
                                        where b1.name = a.oracle_module
                                            and b1.type = 'Analytics and Report'
                                ) L2doclink
                                ,a.L3Grp AS L3grp
                        from L3Tab a
                        where a.process_area_l1 = a2.process_area_l1
                          and a.alliance_product = a2.alliance_product
                            for json path,include_null_values) AS L2Grp
    from L3Tab a2),
t as (select a1.process_area_l1 L1value
      ,(CASE
        WHEN ((SELECT count(1)
                FROM ASCEND.project_workspace pws
                    ,ASCEND.user_roles ur
                WHERE ur.ID = @userroleid
                and ur.project_id = pws.project_id
                and pws.entity_name = 'ANALYTICS_REPORTS'
                AND ISNULL(a1.process_area_l1,'X') = ISNULL(pws.L3,'X')
            ) >= 1)
        THEN 'Y' else 'N'
        END)  L1enabledflag
       ,concat('/assets/documents/', a1.process_area_l1) L1linkname
       ,(select concat(b1.doc_link, '/', b1.file_name)
                           from ascend.documents b1
                           where b1.name = a1.process_area_l1
                             and b1.type = 'Analytics and Report'
        ) L1doclink
        ,a1.alliance_product technology
        ,a1.L2Grp AS L2grp
from L2Tab a1
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
  getDesignBusinessSolutions: function (userroleid, industry, sector, region, l1, l2, l3) {

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
                               ,(select a.solution_name 'solutionname'
                                       ,a.solution_overview 'description'
                                       ,a.problem_statement 'problemstatement'
                                       ,a.oracle_gap 'oraclegap'
                                       ,a.business_value 'businessvalue'
                                       ,(CASE
                                            WHEN ((SELECT count(1)
                                                    FROM ASCEND.project_workspace pws
                                                        ,ASCEND.user_roles ur
                                                    WHERE ur.ID = '`+ userroleid + `'
                                                    and ur.project_id = pws.project_id
                                                    AND a.solution_name = pws.entity_value
                                                    and pws.entity_name = 'BUSINESS_SOLUTIONS'
                                                    AND ISNULL(a.process_area_l1,'X') = ISNULL(pws.L3,'X')
                                                ) = 1)
                                            THEN 'Y' else 'N'
                                            END) 'solutionenabledFlag'
                                        ,a.solution_name 'solutionlinkname'
                                       ,(select concat(b1.doc_link, '/', b1.file_name)
                                           from ascend.documents b1
                                           where b1.name = a.solution_name
                                             and b1.type = 'Business Solutions'
                                           ) 'solutiondoclink'
                                    from ASCEND.solution_gap a
                                        where a.process_area_l1 = a3.process_area_l1
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
                                    for json path,include_null_values) 'solutiongrp'
                                FROM ASCEND.solution_gap a3
                                    /*,ascend.documents b1 */
                                where /*b1.type = 'Business Solutions'
                                    and b1.name = a3.solution_name */
                                    1=1
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
                                and (@l1 = '0' OR upper(a3.process_area_l1) = 'COMMON'  OR ISNULL(a3.process_area_l1,'0') = '0' OR a3.process_area_l1 in (select l1.l1
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
                  t as (select distinct a.process_area_l1 'L1value'
                                            ,(CASE
                                                    WHEN ((SELECT count(1)
                                                            FROM ASCEND.project_workspace pws
                                                                ,ASCEND.user_roles ur
                                                            WHERE ur.ID = @userroleid
                                                            and ur.project_id = pws.project_id
                                                            and pws.entity_name = 'BUSINESS_SOLUTIONS'
                                                            AND ISNULL(a.process_area_l1,'X') = ISNULL(pws.L3,'X')
                                                        ) >= 1)
                                                    THEN 'Y' else 'N'
                                                    END)      'L1enabledflag'
                                                ,a.process_area_l1 'L1linkname'
                                                ,null 'L1doclink'
                                                ,a.solutiongrp 'solutiongrp'

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
  postArchitectKbd: function (jsondata, projectId) {
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
              let deleteString = 'DELETE FROM ' + connection.schemaName + '.PROJECT_WORKSPACE where project_id = ' + projectId + ` AND entity_name = 'KEY_DESIGN_DECISIONS'`;
              console.log('deleteString:' + deleteString);
              await request.query(deleteString);
              console.log('jsondata.length:' + jsondata.length);
              for (var i = 0; i < jsondata.length; i++) {
                tabContent = jsondata[i].tabContent;
                console.log('tabContent.length:' + tabContent.length);
                for (var l = 0; l < tabContent.length; l++) {
                  L2grp = tabContent[l].L2grp;
                  L1value = tabContent[l].L1value;
                  for (var j = 0; j < L2grp.length; j++) {
                    L2value = L2grp[j].L2value;
                    L3grp = L2grp[j].L3grp;
                    for (var k = 0; k < L3grp.length; k++) {
                      decisionname = L3grp[k].decisionname;
                      if (L3grp[k].decisionenabledFlag == 'Y') {
                        console.log('L1value:' + L1value + ' L2value' + L2value + ' decisionname:' + decisionname);
                        let entityName = 'KEY_DESIGN_DECISIONS';
                        let entityTable = 'KEY_BUSINESS_DECISION_NEW';
                        let seperator = `','`;
                        let endBlock = `')`;
                        let insertString = `insert into ` + connection.schemaName + `.PROJECT_WORKSPACE (project_id, entity_name, entity_value, ENTITY_TABLE, L2, L3) VALUES ('` + projectId +
                          seperator + entityName + seperator + L3grp[k].decisionname + seperator + entityTable + seperator + L2value + seperator + L1value + endBlock;
                        console.log('insertString:' + insertString);
                        await request.query(insertString);
                      }
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
  postArchitectBusinessProcess: function (jsondata, projectId) {
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
              let deleteString = 'DELETE FROM ' + connection.schemaName + '.PROJECT_WORKSPACE where project_id = ' + projectId + ` AND entity_name = 'PROCESS_FLOWS'`;
              console.log('deleteString:' + deleteString);
              await request.query(deleteString);
              console.log('jsondata.length:' + jsondata.length);
              for (var i = 0; i < jsondata.length; i++) {
                tabContent = jsondata[i].tabContent;
                console.log('tabContent.length:' + tabContent.length);
                for (var l = 0; l < tabContent.length; l++) {
                  L2grp = tabContent[l].L2grp;
                  L1value = tabContent[l].L1value;
                  for (var j = 0; j < L2grp.length; j++) {
                    L2value = L2grp[j].L2value;
                    L3grp = L2grp[j].L3grp;
                    for (var k = 0; k < L3grp.length; k++) {
                      description = L3grp[k].description;
                      if (L3grp[k].businessprocessenabledflag == 'Y') {
                        console.log('L1value:' + L1value + ' L2value' + L2value + ' description:' + description);
                        let entityName = 'PROCESS_FLOWS';
                        let entityTable = 'BUSINESS_PROCESSES_NEW';
                        let seperator = `','`;
                        let endBlock = `')`;
                        let insertString = `insert into ` + connection.schemaName + `.PROJECT_WORKSPACE (project_id, entity_name, entity_value, ENTITY_TABLE, L2, L3) VALUES ('` + projectId +
                          seperator + entityName + seperator + L1value + seperator + entityTable + seperator + L2value + seperator + L3grp[k].description + endBlock;
                        console.log('insertString:' + insertString);
                        await request.query(insertString);
                      }
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
  postDesignInterface: function (jsondata, projectId) {
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
              let deleteString = 'DELETE FROM ' + connection.schemaName + '.PROJECT_WORKSPACE where project_id = ' + projectId + ` AND entity_name = 'INTERFACES'`;
              console.log('deleteString:' + deleteString);
              await request.query(deleteString);
              console.log('jsondata.length:' + jsondata.length);
              for (var i = 0; i < jsondata.length; i++) {
                tabContent = jsondata[i].tabContent;
                console.log('tabContent.length:' + tabContent.length);
                for (var l = 0; l < tabContent.length; l++) {
                  L2grp = tabContent[l].L2grp;
                  L1value = tabContent[l].L1value;
                  for (var j = 0; j < L2grp.length; j++) {
                    L2value = L2grp[j].L2value;
                    L3grp = L2grp[j].L3grp;
                    for (var k = 0; k < L3grp.length; k++) {
                      interfacename = L3grp[k].interfacename;
                      if (L3grp[k].interfaceenabledFlag == 'Y') {
                        console.log('L1value:' + L1value + ' L2value' + L2value + ' interfacename:' + interfacename);
                        let entityName = 'INTERFACES';
                        let entityTable = 'TECH_OBJECT_LIBRARY';
                        let seperator = `','`;
                        let endBlock = `')`;
                        let insertString = `insert into ` + connection.schemaName + `.PROJECT_WORKSPACE (project_id, entity_name, entity_value, ENTITY_TABLE, L2, L3) VALUES ('` + projectId +
                          seperator + entityName + seperator + L3grp[k].interfacename + seperator + entityTable + seperator + L2value + seperator + L1value + endBlock;
                        console.log('insertString:' + insertString);
                        await request.query(insertString);
                      }
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
  postDesignReports: function (jsondata, projectId) {
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
              let deleteString = 'DELETE FROM ' + connection.schemaName + '.PROJECT_WORKSPACE where project_id = ' + projectId + ` AND entity_name = 'ANALYTICS_REPORTS'`;
              console.log('deleteString:' + deleteString);
              await request.query(deleteString);
              console.log('jsondata.length:' + jsondata.length);
              for (var i = 0; i < jsondata.length; i++) {
                tabContent = jsondata[i].tabContent;
                console.log('tabContent.length:' + tabContent.length);
                for (var l = 0; l < tabContent.length; l++) {
                  L2grp = tabContent[l].L2grp;
                  L1value = tabContent[l].L1value;
                  for (var j = 0; j < L2grp.length; j++) {
                    L2value = L2grp[j].L2value;
                    L3grp = L2grp[j].L3grp;
                    for (var k = 0; k < L3grp.length; k++) {
                      reportname = L3grp[k].reportname;
                      if (L3grp[k].reportenabledFlag == 'Y') {
                        console.log('L1value:' + L1value + ' L2value' + L2value + ' reportname:' + reportname);
                        let entityName = 'ANALYTICS_REPORTS';
                        let entityTable = 'TECH_OBJECT_LIBRARY';
                        let seperator = `','`;
                        let endBlock = `')`;
                        let insertString = `insert into ` + connection.schemaName + `.PROJECT_WORKSPACE (project_id, entity_name, entity_value, ENTITY_TABLE, L2, L3) VALUES ('` + projectId +
                          seperator + entityName + seperator + L3grp[k].reportname + seperator + entityTable + seperator + L2value + seperator + L1value + endBlock;
                        console.log('insertString:' + insertString);
                        await request.query(insertString);
                      }
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
  postDesignBusinessSolutions: function (jsondata, projectId) {
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
              let deleteString = 'DELETE FROM ' + connection.schemaName + '.PROJECT_WORKSPACE where project_id = ' + projectId + ` AND entity_name = 'BUSINESS_SOLUTIONS'`;
              console.log('deleteString:' + deleteString);
              await request.query(deleteString);
              console.log('jsondata.length:' + jsondata.length);
              for (var i = 0; i < jsondata.length; i++) {
                tabContent = jsondata[i].tabContent;
                console.log('tabContent.length:' + tabContent.length);
                for (var l = 0; l < tabContent.length; l++) {
                  L1value = tabContent[l].L1value;
                  solutiongrp = tabContent[l].solutiongrp;
                  for (var j = 0; j < solutiongrp.length; j++) {
                    solutionname = solutiongrp[j].solutionname;
                    if (solutiongrp[j].solutionenabledFlag == 'Y') {
                      console.log('L1value:' + L1value + ' solutionname:' + solutionname);
                      let entityName = 'BUSINESS_SOLUTIONS';
                      let entityTable = 'SOLUTION_GAP';
                      let seperator = `','`;
                      let endBlock = `')`;
                      let insertString = `insert into ` + connection.schemaName + `.PROJECT_WORKSPACE (project_id, entity_name, entity_value, ENTITY_TABLE, L3) VALUES ('` + projectId +
                        seperator + entityName + seperator + solutiongrp[j].solutionname + seperator + entityTable + seperator + L1value + endBlock;
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
  getUserStories: function (userroleid, industry, sector, region, l1, l2, l3) {

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

          req.query(`with l3tab AS (
            select   distinct persona,l1,
                                (select
                                (select a1.TEST_SCRIPT_ID 'testscripts' for json path) userstorytestscript,
                                  a1.USER_STORY_NAME 'userstorydescription',                                             
                                                     CONCAT('User Story : ',a1.USER_STORY_ID)
                                                       'userstoryname',
                                                      null 'userstorydoclink',
                                                      null 'userstorylinkname',
                                                      (
                                              CASE
                                                WHEN (
                                                    (
                                                      SELECT COUNT(1)
                                                      FROM ASCEND.project_workspace pws
                                                        ,ASCEND.user_roles ur
                                                      WHERE ur.ID = @userroleid
                                                        AND ur.project_id = pws.project_id
                                                        AND pws.entity_name='USER_STORIES'
                                  AND pws.entity_value = CONCAT('User Story : ',a1.USER_STORY_ID)
                                  AND ISNULL(pws.l2,'X') = ISNULL(a1.PERSONA,'X')
                                  AND ISNULL(pws.l1,'X') = ISNULL(a1.l1,'X')
                                                      ) = 1
                                                    )
                                                  THEN 'Y'
                                                ELSE 'N'
                                                END
                                              ) 'userstoryenabledflag'
                                              from
                                          ascend.user_stories_new a1
                            where a1.persona=b.persona
                          and a1.l1=b.l1					  
                          for json path, INCLUDE_NULL_VALUES
                            ) L3grp
                    from  ascend.user_stories_new b
                          where 1=1
                          and (@industry = '0' OR ISNULL(b.industry,'COMMON') = 'COMMON' OR b.industry in (select ind.industry
                                                                                                                from ascend.industries_new_i ind,
                                                                                                                (select value from STRING_SPLIT(@industry, ',')) s
                                                                                                                where s.value = ind.id) )
                            and (@sector = '0' OR ISNULL(b.sector,'COMMON') = 'COMMON' OR b.sector in (select sec.sector
                                                                                                          from ascend.industries_new_s sec,
                                                                                                          (select value from STRING_SPLIT(@sector, ',')) s
                                                                                                          where s.value = sec.id) )
                            and (@region = '0' OR ISNULL(b.region,'Core') = 'Core' OR b.region in (select reg.NAME
                                                                                                      from ascend.region_new reg,
                                                                                                      (select value from STRING_SPLIT(@region, ',')) s
                                                                                                      where s.value = reg.DESCRIPTION) )
                            and (@l1 = '0' OR upper(b.l1) = 'COMMON' OR ISNULL(b.l1,'0') = '0' OR b.l1 in (select l1.l1
                                                                                                                                                      from ascend.business_processes_new_l1 l1,
                                                                                                                                                      (select value from STRING_SPLIT(@l1, ',')) s
                                                                                                                                                      where s.value = l1.id))
                            and (@l2 = '0' OR upper(b.l2) = 'COMMON' OR ISNULL(b.l2,'0') = '0' OR b.l2 in (select l2.l2
                                                                                                                                                      from ascend.business_processes_new_l2 l2,
                                                                                                                                                      (select value from STRING_SPLIT(@l2, ',')) s
                                                                                                                                                      where s.value = l2.id))
                            and (@l3 = '0' OR upper(b.l3) = 'COMMON' OR ISNULL(b.l3,'0') = '0' OR b.l3 in (select l3.l3
                                                                                                                                                      from ascend.business_processes_new_l3 l3,
                                                                                                                                                      (select value from STRING_SPLIT(@l3, ',')) s
                                                                                                                                                      where s.value = l3.id))
                                          )
            ,
            l2tab as (
            select distinct l1 'L1value',(
                        CASE
                          WHEN (
                              (
                                SELECT COUNT(1)
                                FROM ASCEND.project_workspace pws
                                  ,ASCEND.user_roles ur
                                WHERE ur.ID = @userroleid
                                  AND ur.project_id = pws.project_id
                                  -- AND b.decision_name = pws.entity_value
                                  AND pws.entity_name='USER_STORIES'
                                  --AND a.PERSONA = pws.L2
                                  --and CONCAT ('Journey Map : ',a.PERSONA) = pws.L3
                       -- AND ISNULL(pws.l2,'X') = ISNULL(b.PERSONA,'X')
                        AND ISNULL(pws.l1,'X') = ISNULL(b.l1,'X')
                                ) >= 1
                              )
                            THEN 'Y'
                          ELSE 'N'
                          END
                        ) 'L1enabledflag', null 'L1doclink',
                    null 'L1linkname',(
                select persona 'L2value' ,
                (
                          SELECT CONCAT (
                              b1.doc_link
                              ,'/'
                              ,b1.file_name
                              ) 'personadoclink'
                          FROM ASCEND.documents b1
                          WHERE a.PERSONA = b1.NAME
                          and b1.category=a.L1
                            AND b1.type = 'User Story'
                        ) 'L2doclink' ,
                         (
                        CASE
                          WHEN (
                              (
                                SELECT COUNT(1)
                                FROM ASCEND.project_workspace pws
                                  ,ASCEND.user_roles ur
                                WHERE ur.ID = @userroleid
                                  AND ur.project_id = pws.project_id
                                  -- AND b.decision_name = pws.entity_value
                                  AND pws.entity_name='USER_STORIES'
                                  AND ISNULL(pws.l2,'X') = ISNULL(a.PERSONA,'X')
                                  AND ISNULL(pws.l1,'X') = ISNULL(a.l1,'X')
                                  --AND b.process_area_l1 = pws.L3
                                ) >= 1
                              )
                            THEN 'Y'
                          ELSE 'N'
                          END
                        ) 'L2enabledflag',persona 'L2linkname',L3grp
                from l3tab A
                where a.l1=b.l1
                for json path, INCLUDE_NULL_VALUES
            ) L2grp
             from l3tab b
            )
            select * from l2tab for json path, INCLUDE_NULL_VALUES`)
            .then(function (recordset) {
              let res = JSON.parse(JSON.stringify(recordset.recordset[0]));
              console.log(res.data);

              // var printyda = res.data ;
              //var printyfa = printyda.replace(/\\/g, "");
              conn.close();
              // resolve(res.data);
              for (key in res) {
                resolve(res[key]);
              }
              //resolve(printyfa);
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
  // getUserStoryLibrary: function (userroleid, industry, sector, region, l1, l2, l3) {

  //   return new Promise((resolve, reject) => {
  //     var conn = new sql.ConnectionPool(connection.getconnection().dbConfig)

  //     conn.connect()
  //       // Successfull connection
  //       .then(function () {

  //         // Create request instance, passing in connection instance
  //         var req = new sql.Request(conn);

  //         req.input('userroleid', sql.VarChar, userroleid);
  //         req.input('industry', sql.VarChar, industry);
  //         req.input('sector', sql.VarChar, sector);
  //         req.input('region', sql.VarChar, region);
  //         req.input('l1', sql.VarChar, l1);
  //         req.input('l2', sql.VarChar, l2);
  //         req.input('l3', sql.VarChar, l3);

  //         req.query(`with l3tab AS (
  //           select   distinct persona,l1,
  //                               (select
  //                               (select a1.TEST_SCRIPT_ID 'testscripts' for json path) userstorytestscript,
  //                                 a1.USER_STORY_NAME 'userstorydescription',CONCAT (
  //                                                     'User Story : '
  //                                                     ,a1.USER_STORY_ID
  //                                                     ) 'userstoryname',
  //                                                     null 'userstorydoclink',
  //                                                     null 'userstorylinkname',
  //                                                     (
  //                                             CASE
  //                                               WHEN (
  //                                                   (
  //                                                     SELECT COUNT(1)
  //                                                     FROM ASCEND.project_workspace pws
  //                                                       ,ASCEND.user_roles ur
  //                                                     WHERE ur.ID = @userroleid
  //                                                       AND ur.project_id = pws.project_id
  //                                                       AND pws.entity_name='USER_STORY_LIBRARY'
  //                                 AND pws.entity_value = CONCAT ('User Story : ',a1.USER_STORY_ID)
  //                                 AND ISNULL(pws.l2,'X') = ISNULL(a1.PERSONA,'X')
  //                                 AND ISNULL(pws.l1,'X') = ISNULL(a1.l1,'X')
  //                                                     ) = 1
  //                                                   )
  //                                                 THEN 'Y'
  //                                               ELSE 'N'
  //                                               END
  //                                             ) 'userstoryenabledflag'
  //                                             from
  //                                         ascend.user_story_library a1
  //                           where a1.persona=b.persona
  //                         and a1.l1=b.l1
  //                         for json path, INCLUDE_NULL_VALUES
  //                           ) L3grp
  //                   from  ascend.user_story_library b
  //                         where 1=1
  //                         and (@industry = '0' OR ISNULL(b.industry,'COMMON') = 'COMMON' OR b.industry in (select ind.industry
  //                                                                                                               from ascend.industries_new_i ind,
  //                                                                                                               (select value from STRING_SPLIT(@industry, ',')) s
  //                                                                                                               where s.value = ind.id) )
  //                           and (@sector = '0' OR ISNULL(b.sector,'COMMON') = 'COMMON' OR b.sector in (select sec.sector
  //                                                                                                         from ascend.industries_new_s sec,
  //                                                                                                         (select value from STRING_SPLIT(@sector, ',')) s
  //                                                                                                         where s.value = sec.id) )
  //                           and (@region = '0' OR ISNULL(b.region,'Core') = 'Core' OR b.region in (select reg.NAME
  //                                                                                                     from ascend.region_new reg,
  //                                                                                                     (select value from STRING_SPLIT(@region, ',')) s
  //                                                                                                     where s.value = reg.DESCRIPTION) )
  //                           and (@l1 = '0' OR upper(b.l1) = 'COMMON' OR ISNULL(b.l1,'0') = '0' OR b.l1 in (select l1.l1
  //                                                                                                                                                     from ascend.business_processes_new_l1 l1,
  //                                                                                                                                                     (select value from STRING_SPLIT(@l1, ',')) s
  //                                                                                                                                                     where s.value = l1.id))
  //                           and (@l2 = '0' OR upper(b.l2) = 'COMMON' OR ISNULL(b.l2,'0') = '0' OR b.l2 in (select l2.l2
  //                                                                                                                                                     from ascend.business_processes_new_l2 l2,
  //                                                                                                                                                     (select value from STRING_SPLIT(@l2, ',')) s
  //                                                                                                                                                     where s.value = l2.id))
  //                           and (@l3 = '0' OR upper(b.l3) = 'COMMON' OR ISNULL(b.l3,'0') = '0' OR b.l3 in (select l3.l3
  //                                                                                                                                                     from ascend.business_processes_new_l3 l3,
  //                                                                                                                                                     (select value from STRING_SPLIT(@l3, ',')) s
  //                                                                                                                                                     where s.value = l3.id))
  //                                         )
  //           ,
  //           l2tab as (
  //           select distinct l1 'L1value',(
  //                       CASE
  //                         WHEN (
  //                             (
  //                               SELECT COUNT(1)
  //                               FROM ASCEND.project_workspace pws
  //                                 ,ASCEND.user_roles ur
  //                               WHERE ur.ID = @userroleid
  //                                 AND ur.project_id = pws.project_id
  //                                 -- AND b.decision_name = pws.entity_value
  //                                 AND pws.entity_name='USER_STORY_LIBRARY'
  //                                 --AND a.PERSONA = pws.L2
  //                                 --and CONCAT ('Journey Map : ',a.PERSONA) = pws.L3
  //                      -- AND ISNULL(pws.l2,'X') = ISNULL(b.PERSONA,'X')
  //                       AND ISNULL(pws.l1,'X') = ISNULL(b.l1,'X')
  //                               ) >= 1
  //                             )
  //                           THEN 'Y'
  //                         ELSE 'N'
  //                         END
  //                       ) 'L1enabledflag', null 'L1doclink',
  //                   null 'L1linkname',(
  //               select persona 'L2value' ,
  //               (
  //                         SELECT CONCAT (
  //                             b1.doc_link
  //                             ,'/'
  //                             ,b1.file_name
  //                             ) 'personadoclink'
  //                         FROM ASCEND.documents b1
  //                         WHERE a.PERSONA = b1.NAME
  //                         and b1.category=a.L1
  //                           AND b1.type = 'User Story'
  //                       ) 'L2doclink' ,
  //                        (
  //                       CASE
  //                         WHEN (
  //                             (
  //                               SELECT COUNT(1)
  //                               FROM ASCEND.project_workspace pws
  //                                 ,ASCEND.user_roles ur
  //                               WHERE ur.ID = @userroleid
  //                                 AND ur.project_id = pws.project_id
  //                                 -- AND b.decision_name = pws.entity_value
  //                                 AND pws.entity_name='USER_STORY_LIBRARY'
  //                                 AND ISNULL(pws.l2,'X') = ISNULL(a.PERSONA,'X')
  //                                 AND ISNULL(pws.l1,'X') = ISNULL(a.l1,'X')
  //                                 --AND b.process_area_l1 = pws.L3
  //                               ) >= 1
  //                             )
  //                           THEN 'Y'
  //                         ELSE 'N'
  //                         END
  //                       ) 'L2enabledflag',persona 'L2linkname',L3grp
  //               from l3tab A
  //               where a.l1=b.l1
  //               for json path, INCLUDE_NULL_VALUES
  //           ) L2grp
  //            from l3tab b
  //           )
  //           select * from l2tab for json path, INCLUDE_NULL_VALUES`)
  //           .then(function (recordset) {
  //             let res = JSON.parse(JSON.stringify(recordset.recordset[0]));
  //             console.log(res.data);

  //             // var printyda = res.data ;
  //             //var printyfa = printyda.replace(/\\/g, "");
  //             conn.close();
  //             // resolve(res.data);
  //             for (key in res) {
  //               resolve(res[key]);
  //             }
  //             //resolve(printyfa);
  //           })
  //           // Handle sql statement execution errors
  //           .catch(function (err) {
  //             console.log(err);
  //             conn.close();
  //             resolve(null);
  //           })

  //       })
  //       // Handle connection errors
  //       .catch(function (err) {
  //         console.log(err);
  //         conn.close();
  //         resolve(null);
  //       });

  //   });
  // },
  getConstructERPConfigurations: function(userroleid, industry, sector, region, l1, l2, l3) {

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

                req.query(`WITH l3group AS (SELECT DISTINCT  a.industry AS industry,
                  a.sector AS sector,
                  a.L1 AS L1,
                  a.L2 AS L2,
                  a.L3 AS L3,
                  a.region AS region,
                  a.module AS module,
                (SELECT b.name description,
                                             b.NAME workbooklinkname,
                                             b.doc_link workbookdoclink,
                                             (CASE
                                                            WHEN ((SELECT COUNT(1)
                                                                                         FROM ASCEND.project_workspace pws
                                                                                                       ,ASCEND.user_roles ur
                                                                                         WHERE ur.ID = '`+ userroleid + `'
and ur.project_id = pws.project_id
AND pws.entity_name = 'ERP_CONFIGURATIONS'
                                                                                         AND b.name = pws.entity_value
                                                                                         AND b.module = pws.L2
                                                                                         AND b.l1 = pws.L3) = 1)
                                                            THEN 'Y' else 'N'
                                                            END) workbookenabledflag
                   FROM ASCEND.configuration_workbooks b
                  WHERE a.l1 = b.l1
                    AND a.l2 = b.l2
                               AND a.l3 = b.l3
                               AND a.industry = b.industry
                               AND a.sector = b.sector
                               AND a.region = b.region
                               AND a.module = b.module
							   and (@industry = '0' OR ISNULL(b.industry,'COMMON') = 'COMMON' OR b.industry in (select ind.industry
from ascend.industries_new_i ind,
(select value from STRING_SPLIT(@industry, ',')) s
where s.value = ind.id) )
and (@sector = '0' OR ISNULL(b.sector,'COMMON') = 'COMMON' OR b.sector in (select sec.sector
from ascend.industries_new_s sec,
(select value from STRING_SPLIT(@sector, ',')) s
where s.value = sec.id) )
and (@region = '0' OR ISNULL(b.region,'Core') = 'Core' OR b.region in (select reg.NAME
from ascend.region_new reg,
(select value from STRING_SPLIT(@region, ',')) s
where s.value = reg.DESCRIPTION) )
and (@l1 = '0' OR ISNULL(b.l1,'0') = '0' OR b.l1 = 'COMMON' OR b.l1 in (select l1.l1
from ascend.business_processes_new_l1 l1,
(select value from STRING_SPLIT(@l1, ',')) s
where s.value = l1.id))
and (@l2 = '0' OR ISNULL(b.l2,'0') = '0' OR b.l2 = 'COMMON' OR b.l2 in (select l2.l2
from ascend.business_processes_new_l2 l2,
(select value from STRING_SPLIT(@l2, ',')) s
where s.value = l2.id))
and (@l3 = '0' OR ISNULL(b.l3,'0') = '0' OR b.l3 = 'COMMON' OR b.l3 in (select l3.l3
from ascend.business_processes_new_l3 l3,
(select value from STRING_SPLIT(@l3, ',')) s
where s.value = l3.id))
                    FOR JSON PATH, INCLUDE_NULL_VALUES) AS L3grp
FROM ASCEND.configuration_workbooks a
WHERE 1=1
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
and (@l1 = '0' OR ISNULL(a.l1,'0') = '0' OR a.l1 = 'COMMON' OR a.l1 in (select l1.l1
from ascend.business_processes_new_l1 l1,
(select value from STRING_SPLIT(@l1, ',')) s
where s.value = l1.id))
and (@l2 = '0' OR ISNULL(a.l2,'0') = '0' OR a.l2 = 'COMMON' OR a.l2 in (select l2.l2
from ascend.business_processes_new_l2 l2,
(select value from STRING_SPLIT(@l2, ',')) s
where s.value = l2.id))
and (@l3 = '0' OR ISNULL(a.l3,'0') = '0' OR a.l3 = 'COMMON' OR a.l3 in (select l3.l3
from ascend.business_processes_new_l3 l3,
(select value from STRING_SPLIT(@l3, ',')) s
where s.value = l3.id))
),
l2group AS  (SELECT DISTINCT c.industry AS industry,
c.sector AS sector,
c.L1 AS L1,
c.L2 AS L2,
c.L3 AS L3,
c.region AS region,
(SELECT d.module L2value,
d.module L2linkname,
NULL L2doclink,
(CASE
WHEN ((SELECT COUNT(1)
FROM ASCEND.project_workspace pws
,ASCEND.user_roles ur
WHERE ur.ID = @userroleid
AND pws.entity_name = 'ERP_CONFIGURATIONS'
and ur.project_id = pws.project_id
AND d.module = pws.L2
AND d.l1 = pws.L3) >= 1)
THEN 'Y' else 'N'
END) L2enabledflag,
d.L3grp
FROM l3group d
WHERE c.l1 = d.l1
AND c.l2 = d.l2
AND c.l3 = d.l3
AND c.industry = d.industry
AND c.sector = d.sector
AND c.region = d.region
FOR JSON PATH, INCLUDE_NULL_VALUES) AS L2grp
FROM l3group c),
t(data) AS  (SELECT e.l1 L1,
NULL L1doclink,
NULL L1linkname,
(CASE WHEN ((SELECT COUNT(1)
FROM ASCEND.project_workspace pws
,ASCEND.user_roles ur
WHERE ur.ID = @userroleid
AND ur.project_id = pws.project_id
AND pws.entity_name = 'ERP_CONFIGURATIONS'
AND e.l1 = pws.L3) >= 1)
THEN 'Y' else 'N'
END) L1enabledflag,
e.L2grp
FROM l2group e
FOR JSON PATH, INCLUDE_NULL_VALUES)
SELECT data FROM t`)
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
  getJourneymap: function(userroleid, industry, sector, region, l1, l2, l3) {

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
  l4tab as
  (select   distinct b.l1,b.persona,b.JOURNEY_MAP_STEP from ascend.journey_map_new b
  where 1=1
  and (@industry = '0' OR ISNULL(b.industry,'COMMON') = 'COMMON' OR b.industry in (select ind.industry
                                                                                                from ascend.industries_new_i ind,
                                                                                                (select value from STRING_SPLIT(@industry, ',')) s
                                                                                                where s.value = ind.id) )
            and (@sector = '0' OR ISNULL(b.sector,'COMMON') = 'COMMON' OR b.sector in (select sec.sector
                                                                                          from ascend.industries_new_s sec,
                                                                                          (select value from STRING_SPLIT(@sector, ',')) s
                                                                                          where s.value = sec.id) )
            and (@region = '0' OR ISNULL(b.region,'Core') = 'Core' OR b.region in (select reg.NAME
                                                                                      from ascend.region_new reg,
                                                                                      (select value from STRING_SPLIT(@region, ',')) s
                                                                                      where s.value = reg.DESCRIPTION) )
            and (@l1 = '0' OR upper(b.l1) = 'COMMON' OR ISNULL(b.l1,'0') = '0' OR b.l1 in (select l1.l1
                                                                                                                                      from ascend.business_processes_new_l1 l1,
                                                                                                                                      (select value from STRING_SPLIT(@l1, ',')) s
                                                                                                                                      where s.value = l1.id))
            and (@l2 = '0' OR upper(b.l2) = 'COMMON' OR ISNULL(b.l2,'0') = '0' OR b.l2 in (select l2.l2
                                                                                                                                      from ascend.business_processes_new_l2 l2,
                                                                                                                                      (select value from STRING_SPLIT(@l2, ',')) s
                                                                                                                                      where s.value = l2.id))
            and (@l3 = '0' OR upper(b.l3) = 'COMMON' OR ISNULL(b.l3,'0') = '0' OR b.l3 in (select l3.l3
                                                                                                                                      from ascend.business_processes_new_l3 l3,
                                                                                                                                      (select value from STRING_SPLIT(@l3, ',')) s
                                                                                                                                      where s.value = l3.id))
  ),
  l3tab AS
  (
  select distinct b.l1 'L1value',b.persona,
    (select  a.JOURNEY_MAP_STEP 'journeymapname' ,
            a.JOURNEY_MAP_STEP 'journeymapnamedesc',
            (CASE
                        WHEN ((SELECT count(1)
                                FROM ASCEND.project_workspace pws
                                    ,ASCEND.user_roles ur
                                WHERE ur.ID = @userroleid
                                and ur.project_id = pws.project_id
                                and pws.entity_name = 'JOURNEY_MAP'
                                AND a.JOURNEY_MAP_STEP = pws.entity_value
                                AND ISNULL(a.persona,'X') = ISNULL(pws.L2,'X')
                                AND ISNULL(a.l1,'X') = ISNULL(pws.L1,'X')
                            ) = 1)
                        THEN 'Y' else 'N'
                        END) 'JouneyenabledFlag',
             null 'journeymaplinkname',
             null 'journeymapdoclink'
        from l4tab a
        where ISNULL(a.l1,'X') = ISNULL(b.l1,'X')
        and ISNULL(a.persona,'X') = ISNULL(b.persona,'X')
                for json path,include_null_values) 'L3Grp'
  from l4tab b
  ),
  L2tab as
  (
    select distinct L1value, (CASE
    WHEN ((SELECT count(1)
            FROM ASCEND.project_workspace pws
                ,ASCEND.user_roles ur
            WHERE ur.ID = @userroleid
            and ur.project_id = pws.project_id
            and pws.entity_name = 'JOURNEY_MAP'
            AND ISNULL(A.l1value,'X') = ISNULL(pws.L1,'X')

        ) >= 1)
    THEN 'Y' else 'N'
    END)  'L1enabledflag',
    null 'L1doclink',
    null 'L1linkname',
        (
            select persona L2value,
                    (CASE
                                WHEN ((SELECT count(1)
                                        FROM ASCEND.project_workspace pws
                                            ,ASCEND.user_roles ur
                                        WHERE ur.ID = @userroleid
                                        and ur.project_id = pws.project_id
                                        and pws.entity_name = 'JOURNEY_MAP'
                                       AND ISNULL(b.persona,'X') = ISNULL(pws.L2,'X')
                                        AND ISNULL(b.L1Value,'X') = ISNULL(pws.L1,'X')
                                    ) >= 1)
                                THEN 'Y' else 'N'
                                END)      'L2enabledflag',
                        persona 'L2linkname',
                        (
          select concat(b1.doc_link, '/', b1.file_name)
           from ascend.documents b1
                            where 1=1
                            and b1.type = 'Journey Maps'
                            and b1.name=b.persona
            and b1.category=b.l1value
                        ) 'L2doclink',
                        L3Grp
            from L3tab B
            where a.l1value=b.l1value
            for json path,include_null_values
        ) 'L2Grp'
  from  L3tab a
  )
  select * from L2tab
  for json path,include_null_values`
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
  getPersonas: function(userroleid, industry, sector, region, l1, l2, l3) {

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
                  `with L3tab
                  as
                  (
                  select distinct process_area_l1,persona
                  from  ascend.personas_new a
                  where 1=1
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
                  )
                  ,
                  L2tab as
                  (
                  select distinct a.process_area_l1 L1value,(CASE
                    WHEN ((SELECT count(1)
                            FROM ASCEND.project_workspace pws
                                ,ASCEND.user_roles ur
                            WHERE ur.ID =  @userroleid
                            and ur.project_id = pws.project_id
                            and pws.entity_name = 'PERSONAS'
                            AND ISNULL(a.process_area_l1,'X') = ISNULL(pws.L1,'X')
                        ) >= 1)
                    THEN 'Y' else 'N'
                    END)  'L1enabledflag',
                    null 'L1doclink',
                    null 'L1linkname',
                        (
                            select distinct  persona L2value,
                                    (CASE
                                                WHEN ((SELECT count(1)
                                                        FROM ASCEND.project_workspace pws
                                                            ,ASCEND.user_roles ur
                                                        WHERE ur.ID = @userroleid
                                                        and ur.project_id = pws.project_id
                                                        and pws.entity_name = 'PERSONAS'
                                      AND ISNULL( pws.entity_value,'X') = ISNULL(b.persona,'X')
                                                       AND ISNULL(b.persona,'X') = ISNULL(pws.L2,'X')
                                                        AND ISNULL(b.process_area_l1,'X') = ISNULL(pws.L1,'X')
                                                    ) >= 1)
                                                THEN 'Y' else 'N'
                                                END)      'L2enabledflag',
                                        null 'L2linkname',
                                        (
                          select concat(b1.doc_link, '/', b1.file_name)
                           from ascend.documents b1
                                            where 1=1
                                            and b1.type = 'Personas'
                                            and b1.name=b.persona
                            and b1.category=b.process_area_l1
                                        ) 'L2doclink'
                            from ascend.personas_new B
                            where a.process_area_l1=b.process_area_l1
                            for json path,include_null_values
                        ) 'L2Grp'
                  from L3tab a
                  )
                  select * from L2tab
                  for json path,include_null_values`
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
  getPhasestopinfo: function(userroleid, phasename, stopname) {

    return new Promise((resolve, reject) => {
        var conn = new sql.ConnectionPool(connection.getconnection().dbConfig)

        conn.connect()
            // Successfull connection
            .then(function () {

                // Create request instance, passing in connection instance
                var req = new sql.Request(conn);

                //req.input('userroleid', sql.VarChar, userroleid);
                req.input('phasename', sql.VarChar, phasename);
                req.input('stopname', sql.VarChar, stopname);
                req.query(
                  `with
l4grp as
(select * from
ascend.project_phase_stops
where 1=1
and phase= @phasename
and stops= @stopname
)
,
l3grp (data) as
(
select (select CONCAT('[', STRING_AGG( '"' + b.activities + '"',','),']')
                from ascend.phase_stops_activities b
                where b.activity_id in ( select a.activity_id from l4grp a
               ) ) activities,
        ( select CONCAT('[', STRING_AGG( '"' + b.objectives + '"',','),']')
                from ascend.phase_stops_objectives B
                where 1=1
                 and b.objective_id in ( select a.objective_id from l4grp a
               )
           ) objectives ,
        ( select CONCAT('[', STRING_AGG( '"' + b.outcomes + '"',','),']')
                from ascend.phase_stops_outcomes b
                where 1=1
                and b.outcome_id in ( select a.outcome_id from l4grp a
               )
        ) outcomes
        for json path
)
select replace(replace(replace(data,'\',''),'"[','['),']"',']') data from l3grp
`)

                    .then(function (recordset) {
                        let res = JSON.parse(JSON.stringify(recordset.recordset[0]));
                        console.log(res.data);
                        conn.close();
          console.log(res.data);
          var printda = res.data ;
          var printfa = printda.replace(/\\/g, "");
          conn.close();
          resolve(printfa);
                        //resolve(res.data);
                       // for(key in res){
                         // resolve(res[key]);
                        //}
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
  getRefineUserstories: function (userroleid, industry, sector, region, l1, l2, l3) {

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

          req.query(`with l3tab AS (
            select   distinct persona,l1,
                                (select
                                (select a1.TEST_SCRIPT_ID 'testscripts' for json path) userstorytestscript,
                                  a1.USER_STORY_NAME 'userstorydescription',concat('User Story : ',a1.USER_STORY_ID)
                                                       'userstoryname',
                                                      null 'userstorydoclink',
                                                      null 'userstorylinkname',
                                                      (
                                              CASE
                                                WHEN (
                                                    (
                                                      SELECT COUNT(1)
                                                      FROM ASCEND.project_workspace pws
                                                        ,ASCEND.user_roles ur
                                                      WHERE ur.ID = @userroleid
                                                        AND ur.project_id = pws.project_id
                                                        AND pws.entity_name='USER_STORY_LIBRARY'
                                  /*AND pws.entity_value = CONCAT ('User Story : ',a1.USER_STORY_ID)
                                  AND ISNULL(pws.l2,'X') = ISNULL(a1.PERSONA,'X')
                                  AND ISNULL(pws.l1,'X') = ISNULL(a1.l1,'X')*/
								  and pws.entity_value = a1.l1
								  and pws.l2=a1.PERSONA
								  and pws.l3=concat('User Story : ',a1.USER_STORY_ID)
                                                      ) = 1
                                                    )
                                                  THEN 'Y'
                                                ELSE 'N'
                                                END
                                              ) 'userstoryenabledflag'
                                              from
                                          ascend.user_story_library a1
                            where a1.persona=b.persona
                          and a1.l1=b.l1						  
                          for json path, INCLUDE_NULL_VALUES
                            ) L3grp
                    from  ascend.user_story_library b
                          where 1=1
                          and (@industry = '0' OR ISNULL(b.industry,'COMMON') = 'COMMON' OR b.industry in (select ind.industry
                                                                                                                from ascend.industries_new_i ind,
                                                                                                                (select value from STRING_SPLIT(@industry, ',')) s
                                                                                                                where s.value = ind.id) )
                            and (@sector = '0' OR ISNULL(b.sector,'COMMON') = 'COMMON' OR b.sector in (select sec.sector
                                                                                                          from ascend.industries_new_s sec,
                                                                                                          (select value from STRING_SPLIT(@sector, ',')) s
                                                                                                          where s.value = sec.id) )
                            and (@region = '0' OR ISNULL(b.region,'Core') = 'Core' OR b.region in (select reg.NAME
                                                                                                      from ascend.region_new reg,
                                                                                                      (select value from STRING_SPLIT(@region, ',')) s
                                                                                                      where s.value = reg.DESCRIPTION) )
                            and (@l1 = '0' OR upper(b.l1) = 'COMMON' OR ISNULL(b.l1,'0') = '0' OR b.l1 in (select l1.l1
                                                                                                                                                      from ascend.business_processes_new_l1 l1,
                                                                                                                                                      (select value from STRING_SPLIT(@l1, ',')) s
                                                                                                                                                      where s.value = l1.id))
                           and (@l2 = '0' OR upper(b.l2) = 'COMMON' OR ISNULL(b.l2,'0') = '0' OR b.l2 in (select l2.l2
                                                                                                                                                      from ascend.business_processes_new_l2 l2,
                                                                                                                                                      (select value from STRING_SPLIT(@l2, ',')) s
                                                                                                                                                      where s.value = l2.id))
                            and (@l3 = '0' OR upper(b.l3) = 'COMMON' OR ISNULL(b.l3,'0') = '0' OR b.l3 in (select l3.l3
                                                                                                                                                      from ascend.business_processes_new_l3 l3,
                                                                                                                                                      (select value from STRING_SPLIT(@l3, ',')) s
                                                                                                                                                      where s.value = l3.id))
                                          )
            ,
            l2tab as (
            select distinct l1 'L1value',(
                        CASE
                          WHEN (
                              (
                                SELECT COUNT(1)
                                FROM ASCEND.project_workspace pws
                                  ,ASCEND.user_roles ur
                                WHERE ur.ID = @userroleid
                                  AND ur.project_id = pws.project_id
                                  AND pws.entity_name='USER_STORY_LIBRARY'
						        and pws.entity_value = b.l1
								 -- and pws.l2=b.PERSONA
                                ) >= 1
                              )
                            THEN 'Y'
                          ELSE 'N'
                          END
                        ) 'L1enabledflag', null 'L1doclink',
                    null 'L1linkname',(
                select persona 'L2value' ,
                (
                          SELECT CONCAT (
                              b1.doc_link
                              ,'/'
                              ,b1.file_name
                              ) 'personadoclink'
                          FROM ASCEND.documents b1
                          WHERE a.PERSONA = b1.NAME
                          and b1.category=a.L1
                            AND b1.type = 'Refine User Story'
                        ) 'L2doclink' ,
                         (
                        CASE
                          WHEN (
                              (
                                SELECT COUNT(1)
                                FROM ASCEND.project_workspace pws
                                  ,ASCEND.user_roles ur
                                WHERE ur.ID = @userroleid
                                  AND ur.project_id = pws.project_id
                                  -- AND b.decision_name = pws.entity_value
                                  AND pws.entity_name='USER_STORY_LIBRARY'
                                  and pws.entity_value = a.l1
								 and pws.l2=a.PERSONA
								  --and pws.l3=CONCAT ('User Story : ',a1.USER_STORY_ID)
                                  --AND b.process_area_l1 = pws.L3
                                ) >= 1
                              )
                            THEN 'Y'
                          ELSE 'N'
                          END
                        ) 'L2enabledflag',persona 'L2linkname',L3grp
                from l3tab A
                where a.l1=b.l1
                for json path, INCLUDE_NULL_VALUES
            ) L2grp
             from l3tab b
            )
            select * from l2tab for json path, INCLUDE_NULL_VALUES`)
            .then(function (recordset) {
              let res = JSON.parse(JSON.stringify(recordset.recordset[0]));
              console.log(res.data);

              // var printyda = res.data ;
              //var printyfa = printyda.replace(/\\/g, "");
              conn.close();
              // resolve(res.data);
              for (key in res) {
                resolve(res[key]);
              }
              //resolve(printyfa);
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
  getDefineDigitalOrg: function(userroleid, industry, sector, region, l1, l2, l3) {

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
                                                  AND b1.type = 'Define Digital Org') L2doclink,
                                                (CASE
                                                  WHEN ((SELECT COUNT(1)
                                                      FROM ASCEND.project_workspace pws
                                                        ,ASCEND.user_roles ur
                                                      WHERE ur.ID = @userroleid
                                                                                  and ur.project_id = pws.project_id
                                                                                  AND pws.entity_name = 'DEFINE_DIGITAL_ORGANIZATION'
                                                      AND d.l2name = pws.L2
                                                      AND d.l1name = pws.L1) = 1)
                                                  THEN 'Y' else  'N'
                                                  END) L2enabledflag
                                             FROM ascend.define_digital_org d
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
                                   FROM ( select distinct L1name,L1description,L2name,L2description,L3name,region,industry,sector,process_area_l1,process_area_l2,process_area_l3 from ascend.define_digital_org )c
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
                                                              AND pws.entity_name = 'DEFINE_DIGITAL_ORGANIZATION'
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
  getrefineuserstoriesdeliverables: function(userroleid, industry, sector, region, l1, l2, l3) {

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
                                                  AND b1.type = 'Deliverables') L2doclink,
                                                (CASE
                                                  WHEN ((SELECT COUNT(1)
                                                      FROM ASCEND.project_workspace pws
                                                        ,ASCEND.user_roles ur
                                                      WHERE ur.ID = @userroleid
                                                      and ur.project_id = pws.project_id
                                                      AND pws.entity_name = 'REFINE_USER_STORIES_DELIVERABLES'
                                                      AND d.l2name = pws.L2
                                                      AND d.l1name = pws.L1) = 1)
                                                  THEN 'Y' else  'N'
                                                  END) L2enabledflag
                                             FROM ascend.deliverables d
                                            WHERE c.l1name = d.l1name
                                            AND d.stop='Refine User Stories'
                                            AND d.phase= 'Imagine'
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
                                   FROM ( select distinct L1name,L1description,L2name,L2description,L3name,region,industry,sector,process_area_l1,process_area_l2,process_area_l3 from ascend.deliverables where stop='Refine User Stories'
                                            AND phase= 'Imagine') c
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
                                                              AND pws.entity_name = 'REFINE_USER_STORIES_DELIVERABLES'
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
  getrefineuserstoriesconfig: function(userroleid, industry, sector, region, l1, l2, l3) {

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
                                                      AND b1.type = 'Config Workbooks') L2doclink,
                                                    (CASE
                                                      WHEN ((SELECT COUNT(1)
                                                          FROM ASCEND.project_workspace pws
                                                            ,ASCEND.user_roles ur
                                                          WHERE ur.ID = @userroleid
                                                                                      and ur.project_id = pws.project_id
                                                                                      AND pws.entity_name = 'CONFIG_WORKBOOKS'
                                                          AND d.l2name = pws.L2
                                                          AND d.l1name = pws.L1) = 1)
                                                      THEN 'Y' else 'N'
                                                      END) L2enabledflag
                                                 FROM ascend.config_workbooks d
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
                                       FROM  ( select distinct L1name,L1description,L2name,L2description,L3name,
									   region,industry,sector,process_area_l1,process_area_l2,process_area_l3 
									   from ascend.config_workbooks )	c
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
                                                                  AND pws.entity_name = 'CONFIG_WORKBOOKS'
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
  postdefinedigitalorg:function(jsondata,projectId) {
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
                    let deleteString = 'DELETE FROM '+connection.schemaName+'.PROJECT_WORKSPACE where project_id = '+projectId+` AND entity_name = 'DEFINE_DIGITAL_ORGANIZATION'`;
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
                                  let entityName = 'DEFINE_DIGITAL_ORGANIZATION';
                                  let entityTable = 'DEFINE_DIGITAL_ORG';
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
  postrefineuserstories:function(jsondata,projectId) {
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
                    let deleteString = 'DELETE FROM ' + connection.schemaName + '.PROJECT_WORKSPACE where project_id = ' + projectId + ` AND entity_name = 'USER_STORY_LIBRARY'`;
              console.log('deleteString:' + deleteString);
              await request.query(deleteString);
              console.log('jsondata.length:' + jsondata.length);
              for (var i = 0; i < jsondata.length; i++) {
                tabContent = jsondata[i].tabContent;
                console.log('tabContent.length:' + tabContent.length);
                for (var l = 0; l < tabContent.length; l++) {
                  L2grp = tabContent[l].L2grp;
                  L1value = tabContent[l].L1value;
				  console.log('L1value ' + L1value);
                  for (var j = 0; j < L2grp.length; j++) {
                    L2value = L2grp[j].L2value;
                    L3grp = L2grp[j].L3grp;
					//console.log('L2value ' + L2value);
					//console.log('L3grp.length ' + L3grp.length);
                    for (var k = 0; k < L3grp.length; k++) {
                      description = L3grp[k].userstoryname;
					  console.log('description ' + description);
                      if (L3grp[k].userstoryenabledflag == 'Y') {
                        console.log('L1value:' + L1value + ' L2value' + L2value + ' description:' + description);
                        let entityName = 'USER_STORY_LIBRARY';
                        let entityTable = 'USER_STORY_LIBRARY';
                        let seperator = `','`;
                        let endBlock = `')`;
                        let insertString = `insert into ` + connection.schemaName + `.PROJECT_WORKSPACE (project_id, entity_name, entity_value, ENTITY_TABLE, L2, L3) VALUES ('` + projectId +
                          seperator + entityName + seperator + L1value + seperator + entityTable + seperator + L2value + seperator + L3grp[k].userstoryname + endBlock;
                        console.log('insertString:' + insertString);
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
  postrefineuserstoriesdeliverables:function(jsondata,projectId) {
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
                    let deleteString = 'DELETE FROM '+connection.schemaName+'.PROJECT_WORKSPACE where project_id = '+projectId+` AND entity_name = 'REFINE_USER_STORIES_DELIVERABLES'`;
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
                                  let entityName = 'REFINE_USER_STORIES_DELIVERABLES';
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
  postrefineuserstoriesconfig:function(jsondata,projectId) {
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
                    let deleteString = 'DELETE FROM '+connection.schemaName+'.PROJECT_WORKSPACE where project_id = '+projectId+` AND entity_name = 'CONFIG_WORKBOOKS'`;
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
                                  let entityName = 'CONFIG_WORKBOOKS';
                                  let entityTable = 'CONFIG_WORKBOOKS';
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
  postPersonas:function(jsondata,projectId) {
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
                    let deleteString = 'DELETE FROM '+connection.schemaName+'.PROJECT_WORKSPACE where project_id = '+projectId+` AND entity_name = 'PERSONAS'`;
                    console.log('deleteString:'+deleteString);
                    await request.query(deleteString);
                    console.log('jsondata.length:'+jsondata.length);
                    for (var i = 0; i < jsondata.length; i++) {
					  tabContent = jsondata[i].tabContent;
					  console.log('tabContent.length:'+tabContent.length);
                      for (var l = 0; l < tabContent.length; l++) {
                        L2Grp = tabContent[l].L2Grp;
                        L1value = tabContent[l].L1value;
                        for (var j = 0; j < L2Grp.length; j++) {
                          L2value = L2Grp[j].L2value;
                            //L2value = L2Grp[j].decisionname;
                            if (L2Grp[j].L2enabledflag == 'Y') {
                                  console.log('L1value:'+L1value+' L2value'+L2value);
                                  let entityName = 'PERSONAS';
                                  let entityTable = 'PERSONAS_NEW';
                                  let seperator = `','`;
                                  let endBlock = `')`;
                                  let insertString = `insert into `+connection.schemaName+`.PROJECT_WORKSPACE (project_id, entity_name, entity_value, ENTITY_TABLE, L2,L1) VALUES ('`+projectId+
                                                      seperator+entityName+seperator+L2Grp[j].L2value+seperator+entityTable+seperator+L2value+seperator+L1value+endBlock;
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
  postJourneyMap:function(jsondata,projectId) {
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
                    let deleteString = 'DELETE FROM '+connection.schemaName+'.PROJECT_WORKSPACE where project_id = '+projectId+` AND entity_name = 'JOURNEY_MAP'`;
                    console.log('deleteString:'+deleteString);
                    await request.query(deleteString);
                    console.log('jsondata.length:'+jsondata.length);
                    for (var i = 0; i < jsondata.length; i++) {
                      tabContent = jsondata[i].tabContent;
                      console.log('tabContent.length:'+tabContent.length);
                      for (var l = 0; l < tabContent.length; l++) {
                        L2Grp = tabContent[l].L2Grp;
                        L1value = tabContent[l].L1value;
                        for (var j = 0; j < L2Grp.length; j++) {
                          L2value = L2Grp[j].L2value;
                          L3Grp = L2Grp[j].L3Grp;
                          for (var k = 0; k < L3Grp.length; k++) {
                            journeymapname = L3Grp[k].journeymapname;
                            if (L3Grp[k].JouneyenabledFlag == 'Y') {
                                  console.log('L1value:'+L1value+' L2value'+L2value+' journeymapname:'+journeymapname);
                                  let entityName = 'JOURNEY_MAP';
                                  let entityTable = 'JOURNEY_MAP_NEW';
                                  let seperator = `','`;
                                  let endBlock = `')`;
                                  let insertString = `insert into `+connection.schemaName+`.PROJECT_WORKSPACE (project_id, entity_name, entity_value, ENTITY_TABLE, L2, L1) VALUES ('`+projectId+
                                                      seperator+entityName+seperator+L3Grp[k].journeymapname+seperator+entityTable+seperator+L2value+seperator+L1value+endBlock;
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
  postUserStories:function(jsondata,projectId) {
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
                    let deleteString = 'DELETE FROM '+connection.schemaName+'.PROJECT_WORKSPACE where project_id = '+projectId+` AND entity_name = 'USER_STORIES'`;
                    console.log('deleteString:'+deleteString);
                    await request.query(deleteString);
                    console.log('jsondata.length:'+jsondata.length);
                    for (var i = 0; i < jsondata.length; i++) {
                     tabContent = jsondata[i].tabContent;
					  console.log('tabContent:'+ tabContent);
                      console.log('tabContent.length:'+tabContent.length);
                      for (var l = 0; l < tabContent.length; l++) {
                        L2grp = tabContent[l].L2grp;
                        L1value = tabContent[l].L1value;
                        for (var j = 0; j < L2grp.length; j++) {
                          L2value = L2grp[j].L2value;
                          L3grp = L2grp[j].L3grp;
                          for (var k = 0; k < L3grp.length; k++) {
                            userstoryname = L3grp[k].userstoryname;
                            if (L3grp[k].userstoryenabledflag == 'Y') {
                                  console.log('L1value:'+L1value+' L2value'+L2value+' userstoryname:'+userstoryname);
                                  let entityName = 'USER_STORIES';
                                  let entityTable = 'USER_STORIES_NEW';
                                  let seperator = `','`;
                                  let endBlock = `')`;
                                  let insertString = `insert into `+connection.schemaName+`.PROJECT_WORKSPACE (project_id, entity_name, entity_value, ENTITY_TABLE, L2, L1) VALUES ('`+projectId+
                                                      seperator+entityName+seperator+L3grp[k].userstoryname+seperator+entityTable+seperator+L2value+seperator+L1value+endBlock;
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
  // postUserStoryLibrary:function(jsondata,projectId) {
  //   return new Promise((resolve, reject) =>
  //   {
  //     console.log('In postUserStoryLibrary');
  //     console.log(jsondata);
  //     console.log('projectId:'+projectId);

  //     var conn = new sql.ConnectionPool(connection.getconnection().dbConfig)
  //     conn.connect()
  //     // Successful connection
  //     .then(function () {
  //       console.log('In connection successful');

  //       let transaction = new sql.Transaction(conn);
  //       transaction.begin().then(async function(){
  //         console.log('In transaction begin successful');
  //         let request = new sql.Request(transaction);
  //         let iExceptionFlag = false;
  //         let errorMessage = '';
  //                 try{
  //                   let deleteString = 'DELETE FROM '+connection.schemaName+'.PROJECT_WORKSPACE where project_id = '+projectId+` AND entity_name = 'USER_STORY_LIBRARY'`;
  //                   console.log('deleteString:'+deleteString);
  //                   await request.query(deleteString);
  //                   console.log('jsondata.length:'+jsondata.length);
  //                   for (var i = 0; i < jsondata.length; i++) {
  //                    tabContent = jsondata[i].tabContent;
	// 				  console.log('tabContent:'+ tabContent);
  //                     console.log('tabContent.length:'+tabContent.length);
  //                     for (var l = 0; l < tabContent.length; l++) {
  //                       L2grp = tabContent[l].L2grp;
  //                       L1value = tabContent[l].L1value;
  //                       for (var j = 0; j < L2grp.length; j++) {
  //                         L2value = L2grp[j].L2value;
  //                         L3grp = L2grp[j].L3grp;
  //                         for (var k = 0; k < L3grp.length; k++) {
  //                           userstoryname = L3grp[k].userstoryname;
  //                           if (L3grp[k].userstoryenabledflag == 'Y') {
  //                                 console.log('L1value:'+L1value+' L2value'+L2value+' userstoryname:'+userstoryname);
  //                                 let entityName = 'USER_STORY_LIBRARY';
  //                                 let entityTable = 'USER_STORY_LIBRARY';
  //                                 let seperator = `','`;
  //                                 let endBlock = `')`;
  //                                 let insertString = `insert into `+connection.schemaName+`.PROJECT_WORKSPACE (project_id, entity_name, entity_value, ENTITY_TABLE, L2, L1) VALUES ('`+projectId+
  //                                                     seperator+entityName+seperator+L3grp[k].userstoryname+seperator+entityTable+seperator+L2value+seperator+L1value+endBlock;
  //                                 console.log('insertString:'+insertString);
  //                                 await request.query(insertString);
  //                             }
  //                           }
  //                         }
  //                       }
  //                   }
  //               }
  //               catch(e){
  //                 iExceptionFlag = true;
  //                 errorMessage = e;
  //                 console.log('Exception:'+e);
  //               }
  //               console.log(iExceptionFlag)
  //               if(iExceptionFlag){
  //                 transaction.rollback().then(function(){
  //                   console.log('In rollback:then');
  //                   conn.close();
  //                   resolve({"MSG":"Error while inserting data into database:"+errorMessage});
  //                 })
  //                 .catch(function(err){
  //                   console.log('In rollback:catch');
  //                   conn.close();
  //                   resolve({"MSG":"Error while rolling back transaction:"+errorMessage});
  //                 });
  //               }else{
  //                 transaction.commit().then(function(){
  //                   console.log('In commit:then');
  //                   conn.close();
  //                   resolve({"MSG":"SUCCESS"});
  //                 })
  //                 .catch(function(err){
  //                   console.log('In commit:catch');
  //                   conn.close();
  //                   resolve({"MSG":"Error while commiting transaction:"+errorMessage});
  //                 });
  //               }


  //         })
  //         .catch(function(err){
  //           console.log('In transaction begin catch:'+err);
  //           conn.close();
  //           resolve({"MSG":"Error while creating transaction object:"+errorMessage});
  //         })

  //     })
  //     .catch(function(err){
  //       console.log('In connection catch catch:'+err)
  //       conn.close();
  //       resolve({"MSG":"Error while creating connection object:"+errorMessage});
  //     })

  // })
  // },
  postConstructERPConfigurations:function(jsondata,projectId) {
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
                let deleteString = 'DELETE FROM '+connection.schemaName+'.PROJECT_WORKSPACE where project_id = '+projectId+` AND entity_name = 'ERP_CONFIGURATIONS'`;
                console.log('deleteString:'+deleteString);
                await request.query(deleteString);
                console.log('jsondata.length:'+jsondata.length);
                for (var i = 0; i < jsondata.length; i++) {
                    tabContent = jsondata[i].tabContent;
                    console.log('tabContent.length:'+tabContent.length);
                    for (var l = 0; l < tabContent.length; l++) {
                    L2grp = tabContent[l].L2grp;
                    L1value = tabContent[l].L1;
                    for (var j = 0; j < L2grp.length; j++) {
                        L2value = L2grp[j].L2value;
                        L3grp = L2grp[j].L3grp;
                        for (var k = 0; k < L3grp.length; k++) {
                        description = L3grp[k].description;
                        if (L3grp[k].workbookenabledflag == 'Y') {
                                console.log('L1value:'+L1value+' L2value'+L2value+' description:'+description);
                                let entityName = 'ERP_CONFIGURATIONS';
                                let entityTable = 'CONFIGURATION_WORKBOOKS';
                                let seperator = `','`;
                                let endBlock = `')`;
                                let insertString = `insert into `+connection.schemaName+`.PROJECT_WORKSPACE (project_id, entity_name, entity_value, ENTITY_TABLE, L2, L3) VALUES ('`+projectId+
                                                    seperator+entityName+seperator+L3grp[k].description+seperator+entityTable+seperator+L2value+seperator+L1value+endBlock;
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

getLaunchJourneyOCM: function(userroleid, industry, sector, region, l1, l2, l3) {

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
                             AND b1.type = 'Launch journey OCM') L1doclink,
                  (CASE WHEN ((SELECT COUNT(1)
                       FROM ASCEND.project_workspace pws
                         ,ASCEND.user_roles ur
                       WHERE ur.ID = @userroleid
                                         AND ur.project_id = pws.project_id
                                         AND pws.entity_name = 'LAUNCH_JOURNEY_OCM'
                       AND e.L1name = pws.L1) = 1)
                    THEN 'Y' else  'N'
                    END) L1enabledflag
             FROM ascend.change_management e
      where 1=1
	  and e.phase = 'Imagine'
	  AND e.stop = 'Launch journey'
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
  
postLaunchJourneyOCM:function(jsondata,projectId) {
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
                    let deleteString = 'DELETE FROM '+connection.schemaName+'.PROJECT_WORKSPACE where project_id = '+projectId+` AND entity_name = 'LAUNCH_JOURNEY_OCM'`;
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
                                  let entityName = 'LAUNCH_JOURNEY_OCM';
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
  
getRefineUserStoriesOCM: function(userroleid, industry, sector, region, l1, l2, l3) {

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
                             AND b1.type = 'Refine user stories OCM') L1doclink,
                  (CASE WHEN ((SELECT COUNT(1)
                       FROM ASCEND.project_workspace pws
                         ,ASCEND.user_roles ur
                       WHERE ur.ID = @userroleid
                                         AND ur.project_id = pws.project_id
                                         AND pws.entity_name = 'REFINE_USER_STORIES_OCM'
                       AND e.L1name = pws.L1) = 1)
                    THEN 'Y' else  'N'
                    END) L1enabledflag
             FROM ascend.change_management e
      where 1=1
	  and e.phase = 'Imagine'
	  AND e.stop = 'Refine user stories'
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

postRefineUserStoriesOCM:function(jsondata,projectId) {
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
                    let deleteString = 'DELETE FROM '+connection.schemaName+'.PROJECT_WORKSPACE where project_id = '+projectId+` AND entity_name = 'REFINE_USER_STORIES_OCM'`;
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
                                  let entityName = 'REFINE_USER_STORIES_OCM';
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
  
getLaunchJourney: function(userroleid, industry, sector, region, l1, l2, l3) {

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
                `with
                l2group AS  (SELECT DISTINCT c.L1name L1value,
                                            (SELECT d.L2name L2value,
                                                d.L2description L2linkname,
                                                (SELECT CONCAT(b1.doc_link, '/', b1.file_name)
                                                 FROM ASCEND.documents b1
                                                WHERE d.L2name = b1.name
                                                  AND b1.type = 'Launch Journey') L2doclink,
                                                (CASE
                                                  WHEN ((SELECT COUNT(1)
                                                      FROM ASCEND.project_workspace pws
                                                        ,ASCEND.user_roles ur
                                                      WHERE ur.ID = @userroleid
                                                                                  and ur.project_id = pws.project_id
                                                                                  AND pws.entity_name = 'LAUNCH_JOURNEY'
                                                      AND d.l2name = pws.L2
                                                      AND d.l1name = pws.L1) = 1)
                                                  THEN 'Y' else  'N'
                                                  END) L2enabledflag
                                             FROM ascend.deliverables d
                                            WHERE c.l1name = d.l1name
                                            AND d.stop='Launch journey'
                                            AND d.phase= 'Imagine'
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
                                   FROM ( select distinct L1name,L1description,L2name,L2description,L3name,region,industry,sector,process_area_l1,process_area_l2,process_area_l3 from ascend.deliverables where stop='Launch journey'
                                            AND phase= 'Imagine') c
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
                                                              AND pws.entity_name = 'LAUNCH_JOURNEY'
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
  
  postLaunchJourney:function(jsondata,projectId) {
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
                    let deleteString = 'DELETE FROM '+connection.schemaName+'.PROJECT_WORKSPACE where project_id = '+projectId+` AND entity_name = 'LAUNCH_JOURNEY'`;
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
                                  let entityName = 'LAUNCH_JOURNEY';
                                  let entityTable = 'LAUNCH_JOURNEY';
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
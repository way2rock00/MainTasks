var connection = require('./connection-file');
var sql = require("mssql");


module.exports = {
getMarketPlaceFilter: function () {

    return new Promise((resolve, reject) => {
      var conn = new sql.ConnectionPool(connection.getconnection().dbConfig)
      conn.connect()
        // Successfull connection
        .then(function () {
          // Create request instance, passing in connection instance
          var request = new sql.Request(conn);
          //request.input('myval',sql.VarChar,projectid);

          request.query(`WITH cte_org AS (
    SELECT       
        id, 
        parent_id,
        filter_category,
        1 level_value
		--,
        --'' childs
     --   'Label' level_type
    FROM       
        ascend.marketplace_filters
    WHERE parent_id IS NULL
    UNION ALL
    SELECT 
        e.id, 
        e.parent_id,
        e.filter_category,
        o.level_value + 1 level_value
--        'Checkbox' level_type
    FROM 
        ascend.marketplace_filters e
        ,cte_org o 
        where o.id = e.parent_id
),
t (data) as (SELECT * FROM cte_org 
order by level_value 
for json path, include_null_values)
select * from t`)
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
  },
  getMarketPlaceTools: function () {

    return new Promise((resolve, reject) => {
      var conn = new sql.ConnectionPool(connection.getconnection().dbConfig)
      conn.connect()
        // Successfull connection
        .then(function () {
          // Create request instance, passing in connection instance
          var request = new sql.Request(conn);
          //request.input('myval',sql.VarChar,projectid);

          request.query(`with t(data) as (select 
tool_name toolName,
tool_id toolId,
tools_tile_description toolDescription,
(case when hosted_url is null
       then 'N'
       else 'Y'
       END)
launchApplicable,
hosted_url launchURL,
download_applicable DownloadApplicable,
download_url DownloadURL,
tool_icons toolIcon,
'['+filters_applicable+']' filtersApplicable
from ascend.tools_accelerators_new
where show_entity='Y'
for json path, include_null_values)
select replace(replace(replace(replace(data,'"[','['),']"',']'),'\n',''),'\r','') data from t`)
            .then(function (recordsets) {
              let res = JSON.parse(JSON.stringify(recordsets.recordset[0]));
              //console.log(a);
              //console.log('Printing data.2.1');
              console.log(res.data);
              conn.close();
              var printdetails = res.data;
              var printfinal = printdetails.replace(/\n\"/g, '');
              //resolve(res.data);
              resolve(printdetails);
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
}
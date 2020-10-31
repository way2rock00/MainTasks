--Begin Revision History
--<<<
-- 15-Aug-2016 10:41:36 C9914584 /main/18
-- 
--<<<
-- 17-Aug-2016 07:54:27 C9914584 /main/19
-- 
-- <<<
-- 17-Aug-2016 08:16:48 C9914584 /main/20
-- 
-- <<<
-- 06-Oct-2016 01:59:46 E9974449 /main/21
-- 
-- <<<
-- 09-Nov-2016 02:31:01 E9974449 /main/22
-- 
-- <<<
-- 09-Nov-2016 03:57:39 E9974449 /main/23
-- 
-- <<<
-- 11-Nov-2016 02:58:03 E9974449 /main/24
-- 
-- <<<
-- 09-Jan-2017 02:11:24 E9974449 /main/25
-- 
-- <<<
-- 18-Jan-2017 07:16:39 E9974449 /main/26
-- 
-- <<<
-- 19-Jan-2017 05:02:53 E9974449 /main/27
-- 
-- <<<
--End Revision History  
create or replace PACKAGE BODY xxar_br_invoices_pkg
----------------------------------------------------------------------------------------------------------------------------------
--    Owner        : EATON CORPORATION.
--    Application  : Account Receivables
--    Schema       : APPS
--    Compile AS   : APPS
--    File Name    : XXAR_BR_INVOICES_PKG.pkb
--    Date         : 15-Jan-2014
--    Author       : Chetan  Kanungo
--    Description  : Package Body for AR Invoices Conversion.
--    Version      : $ETNHeader: /CCSTORE/ccweb/E9974449/E9974449_AR_TOP_view/vobs/AR_TOP/xxar/12.0.0/install/xxar_br_invoices_pkg.pkb /main/27 19-Jan-2017 05:02:53 E9974449  $
--
--    Parameters   :
--    piv_run_mode        : Control the program excution for VALIDATE CONVERSION
--
--    pin_batch_id        : List all unique batches from staging table , this will
--                          be NULL for first Conversion Run.
--    piv_process_records : Conditionally available only when pin_batch_id is populated.
--          Otherwise this will be disabled and defaulted to ALL
--    piv_operating_unit  : 11i Operating Unit. If provided then program will
--          run only for specified Operating unit
--    piv_gl_date    : GL date for which the conversion is done. This
--          date will be considered only during VALIDATE mode
--          for transaction other than service contracts
--
--    Change History
--  ===============================================================================================================================
--    v1.0    Chetan Kanungo    15-Aug-2015    Initial Creation
--    v1.1    Chetan Kanungo    15-Nov-2015    CR#346150
--    v1.2    Piyush Ojha       15-FEB-2016    Defect#5392 While creating dummy bank account, Not able to fetch customer site
--                                             details when multiple bill to sites exists for a customer.
--                                             Resolution : Pass Site orig system reference and Org id in the query.
--    v1.3    Piyush Ojha       16-FEB-2016    Defect#5411 To populate Tax information correctly on the staging table.
--                                             Added ORG_ID and LEG_LINE_TYPE='TAX'
--    v1.4    Piyush Ojha       16-FEB-2016    Defect#5393 Avoid CM transactions from payment term validation
--    v1.5    Preeti Sethi      02-MAR-2016    Initialised Variable l_valid_flag in Validate_Invoice procedure to capture Distribution error message.
--    v1.6    Piyush Ojha       27-MAR-2016    Defect 5440 restricted header records leg_process_flag<>'D' to not pull duplicate invoice records.
--                                             Used xxetn_map_unit_v VIEW instead of xxetn_map_unit table
--    v1.7    Piyush Ojha       27-MAR-2016    Defect 5452 once BR created updating process flag as B - BR Completed
--    v1.8    Piyush Ojha       29-MAR-2016    Modified drawee code to log error in case if drawee creation failed. As of now, code is not marking
--                                             record as E even though drawee creation had fail.
--    v1.9    Preeti Sethi      04-Apr-2016    Corrected the spelling of Promissory from Promisorry.
--    v1.10   Bhaskar Pedipina  09-Aug-2016    Mock4 Defect# 9297, Cross reference not defined for customer
--    v1.11   Piyush Ojha       09-Aug-2016    Mock4 Defect# 9420, removed OKS transactions and null one variable when more than once customer exist
--    v1.12   Bhaskar Pedipina  16-Aug-2016    Mock4 Defect# 9453, Post Processing Error :character string buffer too small
--    v1.13   Piyush Ojha       12-Sep-2016    Mock4 CR# 408850 Defect 9594 Now performing adjustment on the basis of BR assignment amount 
--											   through new column : leg_br_amount. Pull procedure to include New column leg_br_amount.
--											   Tie back program to update lines correctly.
--											   Defect 9653 In case of multiple instalment use Distinct in Update_br procedure to avoid
--											   too many rows exception
--											   For 'ERR_INT' error code modified update query , to use correct request id on staging table too.
--											   Missing Org_id  included to avoid "The Bill To address id must exist in Oracle Receivables, 
--												and it must be assigned to the Bill To customer" 
--											   Tie_back procedure - correction to update correct records with required error message
--											   Code bug , adding receipt method to customer site if still it is already attached			
--	  v1.14   Piyush Ojha      09-Nov-2016     Mock 4.5 CR# 408850 passing 11i Br number in Invoice Comments cloumn in Ra interface lines all.
--    v1.15   Piyush Ojha      09-Jan-2017     Mock 5 Defect 13077 Dummy bank account not getting created for customer having multiple sites
--    =============================================================================================================================
AS
   -- Declaration of global variables

   -- WHO columns
   g_request_id               NUMBER       DEFAULT fnd_global.conc_request_id;
   g_prog_appl_id             NUMBER          DEFAULT fnd_global.prog_appl_id;
   g_conc_program_id          NUMBER       DEFAULT fnd_global.conc_program_id;
   g_user_id                  NUMBER               DEFAULT fnd_global.user_id;
   g_login_id                 NUMBER              DEFAULT fnd_global.login_id;
   g_last_updated_by          NUMBER               DEFAULT fnd_global.user_id;
   g_last_update_login        NUMBER              DEFAULT fnd_global.login_id;
   g_sysdate         CONSTANT DATE                                 := SYSDATE;
   --Count variables
   g_total_count              NUMBER                                DEFAULT 0;
   g_total_dist_count         NUMBER                                DEFAULT 0;
   g_loaded_count             NUMBER                                DEFAULT 0;
   g_loaded_dist_count        NUMBER                                DEFAULT 0;
   g_failed_count             NUMBER                                DEFAULT 0;
   g_failed_dist_count        NUMBER                                DEFAULT 0;
   --Table type Index
   g_line_idx                 NUMBER                                   := 1;
   g_dist_idx                 NUMBER                                   := 1;
   g_err_indx                 NUMBER                                   := 0;
   -- Program parameters
   g_retcode                  NUMBER                                   := 0;
   g_errbuff                  VARCHAR2 (20)                      := 'SUCCESS';
   g_run_mode                 VARCHAR2 (100);
   g_process_records          VARCHAR2 (100);
   g_gl_date                  DATE;
   --Program level
   g_batch_id                 NUMBER;
   g_new_batch_id             NUMBER;
   g_new_run_seq_id           NUMBER;
   g_batch_source             ar_batch_sources_all.NAME%TYPE  := 'CONVERSION';
   g_interface_line_context   VARCHAR2 (240)                       := 'Eaton';
   g_debug_err                VARCHAR2 (2000);
   g_log_level                NUMBER                                   := 1;
   --Lookup names
   g_ou_lookup                fnd_lookup_types_tl.lookup_type%TYPE
                                                       := 'ETN_COMMON_OU_MAP';
   g_pmt_term_lookup          fnd_lookup_types_tl.lookup_type%TYPE
                                                    := 'ETN_AR_PAYMENT_TERMS';
   g_trx_type_lookup          fnd_lookup_types_tl.lookup_type%TYPE
                                                 := 'ETN_AR_TRANSACTION_TYPE';
   g_tax_code_lookup          fnd_lookup_types_tl.lookup_type%TYPE
                                                := 'ETN_OTC_TAX_CODE_MAPPING';

   TYPE g_invoice_rec IS TABLE OF xxconv.xxar_br_invoices_stg%ROWTYPE
      INDEX BY BINARY_INTEGER;

   g_invoice                  g_invoice_rec;

   TYPE g_invoice_det_rec IS TABLE OF xxconv.xxar_br_invoices_stg%ROWTYPE
      INDEX BY BINARY_INTEGER;

   g_invoice_details          g_invoice_det_rec;

   TYPE g_invoice_dist_rec IS TABLE OF xxconv.xxar_br_invoices_dist_stg%ROWTYPE
      INDEX BY BINARY_INTEGER;

   g_invoice_dist             g_invoice_dist_rec;
   g_index                    NUMBER                                   := 1;
   g_err_lmt                  NUMBER
                             := fnd_profile.VALUE ('ETN_FND_ERROR_TAB_LIMIT');
   g_leg_operating_unit       VARCHAR2 (240);
   g_leg_trasaction_type      VARCHAR2 (240);
   g_error_tab                xxetn_common_error_pkg.g_source_tab_type;
   g_direction                VARCHAR2 (240)               := 'LEGACY-TO-R12';
   g_coa_error       CONSTANT VARCHAR2 (30)                        := 'Error';
   g_coa_processed   CONSTANT VARCHAR2 (30)                    := 'Processed';
   g_period_set_name          VARCHAR2 (100)             := 'ETN Corp Calend';
   g_log                      VARCHAR2 (1);
   -- Global variable for printing log
   g_instance                 VARCHAR2 (100)                         := 'R12';
   g_conc_request_id          VARCHAR2 (100)    := fnd_global.conc_request_id;
   g_usr_id                   NUMBER           := NVL (fnd_global.user_id,
                                                       -1);
   g_now                      DATE                                 := SYSDATE;
   g_process_flag             VARCHAR2 (10)                            := 'I';

   ---pull data --------------
   PROCEDURE LOG (p_comment VARCHAR2)
   IS
   BEGIN
      IF g_log = 'Y'
      THEN
         fnd_file.put_line (fnd_file.LOG, p_comment);
      --    etn_api_pkg.insert_log(p_comment);
      END IF;
   END LOG;

   FUNCTION is_request_id_valid (
      p_pull_from_inv_table   IN   VARCHAR2,
      p_rqst_id               IN   NUMBER
   )
      RETURN BOOLEAN
   IS
      v_header_count   NUMBER;
      v_dist_count     NUMBER;
   BEGIN
      SELECT COUNT (1)
        INTO v_header_count
        FROM xxar_br_invoices_stg@apps_to_issc11i.tcc.etn.com
       WHERE leg_request_id = p_rqst_id;

      IF v_header_count = 0
      THEN
         RETURN FALSE;
      END IF;

      RETURN TRUE;
   END is_request_id_valid;

   PROCEDURE register_error (
      p_orgz_id     IN   ra_customer_trx_all.org_id%TYPE,
      p_orgz_name   IN   xxetn_extn_errors.operating_unit_name%TYPE
            DEFAULT NULL,
      p_err_code    IN   xxetn_extn_errors.ERROR_CODE%TYPE,
      p_err_msg     IN   xxetn_extn_errors.error_message%TYPE,
      p_table       IN   xxetn_extn_errors.schema_table_name%TYPE
   )
   IS
      v_conc_prog_name        VARCHAR2 (2000)
                         := 'Eaton Unify Open BR Invoices Conversion Program';
      v_extn_package_name     VARCHAR2 (200)
                                         := 'ETN_BR_OPEN_INVOICES_CONVERSION';
      v_extn_procedure_name   VARCHAR2 (20)   := 'PULL';
      v_org_name              VARCHAR2 (200);
      v_error                 VARCHAR2 (4000);
      PRAGMA AUTONOMOUS_TRANSACTION;
   BEGIN
      IF p_orgz_name IS NULL
      THEN
         IF p_orgz_id != -1
         THEN
            BEGIN
               SELECT NAME
                 INTO v_org_name
                 FROM hr_all_organization_units
                WHERE organization_id = p_orgz_id;
            EXCEPTION
               WHEN OTHERS
               THEN
                  register_error
                     (p_orgz_id       => -1,
                      p_err_code      => '0',
                      p_err_msg       =>    'Register Error : Error getting Org Name for orgz_id '
                                         || p_orgz_id
                                         || '. Error was '
                                         || SQLERRM,
                      p_table         => 'Not Applicable'
                     );
            END;
         ELSE
            v_org_name := 'Not Applicable';
         END IF;
      END IF;

      INSERT INTO xxetn_extn_errors
                  (record_id, conc_request_id, creation_date,
                   created_by, concurrent_program_name, operating_unit_name,
                   set_of_books_name, fa_books_name, leg_request_id,
                   leg_seq_num, leg_source_system, source_database,
                   ERROR_CODE, error_message, schema_table_name,
                   extn_package_name, extn_procedure_name, attribute1,
                   attribute2, attribute3, attribute4, attribute5,
                   attribute6, attribute7, attribute8, attribute9,
                   attribute10, attribute11, attribute12, attribute13,
                   attribute14, attribute15
                  )
           VALUES (xxetn_extn_errors_s.NEXTVAL, g_conc_request_id, g_now,
                   g_usr_id, v_conc_prog_name, v_org_name,
                   NULL, NULL, NULL                           -- used for Pull
                                   ,
                   NULL                                       -- used for Pull
                       , NULL                                 -- used for Pull
                             , g_instance,
                   p_err_code, SUBSTR (p_err_msg, 1, 2000), p_table,
                   v_extn_package_name, v_extn_procedure_name, NULL,
                   NULL, NULL, NULL, NULL,
                   NULL, NULL, NULL, NULL,
                   NULL, NULL, NULL, NULL,
                   NULL, NULL
                  );

      COMMIT;
   END register_error;

   FUNCTION get_instance
      RETURN VARCHAR2
   IS
      v_instance_name   v$instance.instance_name%TYPE;
   BEGIN
      SELECT SYS_CONTEXT ('USERENV', 'INSTANCE_NAME')
        INTO v_instance_name
        FROM DUAL;

      RETURN v_instance_name;
   EXCEPTION
      WHEN OTHERS
      THEN
         RETURN '>>Error-Getting-Instance<<';
         register_error
                      (p_orgz_id       => -1,
                       p_err_code      => '0',
                       p_err_msg       => 'get_instance : Error fetching instance',
                       p_table         => 'Not Applicable'
                      );
   END get_instance;

   PROCEDURE audit_trail (
      p_mode                 VARCHAR2,
      p_leg_source_system    VARCHAR2 DEFAULT NULL,
      p_leg_request_id       NUMBER DEFAULT NULL,
      p_parameter            VARCHAR2 DEFAULT NULL,
      p_extract_table_name   VARCHAR2 DEFAULT NULL,
      p_insert_table_name    VARCHAR2,
      p_records_inserted     NUMBER DEFAULT NULL,
      p_records_purged       NUMBER DEFAULT NULL
   )
   IS
      v_conc_prog_name         VARCHAR2 (1000)
                         := 'Eaton Unify Open BR Invoices Conversion Program';
      v_conc_prog_short_name   VARCHAR2 (200)
                                         := 'ETN_BR_OPEN_INVOICES_CONVERSION';
      v_extn_package_name      VARCHAR2 (100)  := 'XXAR_BR_INVOICES_PKG';
      v_source_database        VARCHAR2 (200)  := get_instance;
      PRAGMA AUTONOMOUS_TRANSACTION;
   BEGIN
      IF p_mode = 'START'
      THEN
         LOG ('Starting Audit trail for ' || p_insert_table_name);

         INSERT INTO xxetn_extn_audit
                     (audit_id, conc_request_id,
                      creation_start_date, created_by, last_updated_by,
                      concurrent_program_name, program_short_code,
                      program_parameters, leg_request_id, leg_source_system,
                      extract_schema_table_name, insert_schema_table_name,
                      extn_package_name, extn_procedure_name,
                      request_status, source_database, action
                     )
              VALUES (xxetn_extn_audit_s.NEXTVAL, g_conc_request_id,
                      SYSDATE, g_usr_id, g_usr_id,
                      v_conc_prog_name, v_conc_prog_short_name,
                      p_parameter, p_leg_request_id, p_leg_source_system,
                      p_extract_table_name, p_insert_table_name,
                      v_extn_package_name, 'INSERT',
                      'In Progress', v_source_database, 'INSERT'
                     );
      ELSIF p_mode = 'END'
      THEN
         LOG ('End of Audit trail for ' || p_insert_table_name);

         UPDATE xxetn_extn_audit
            SET creation_end_date = SYSDATE,
                records_extracted = p_records_inserted,
                records_inserted = p_records_inserted,
                records_pulled = NULL,
                records_purged = NULL,
                request_status = 'Completed'
          WHERE concurrent_program_name = v_conc_prog_name
            AND insert_schema_table_name = p_insert_table_name
            AND conc_request_id = g_conc_request_id;
      END IF;

      COMMIT;
   EXCEPTION
      WHEN OTHERS
      THEN
         register_error
            (p_orgz_id       => -1,
             p_err_code      => '0',
             p_err_msg       =>    'Audit Trail : Error while updating xxetn_extn_audit. Error was '
                                || SQLERRM,
             p_table         => 'Not Applicable'
            );
   END audit_trail;

   PROCEDURE pull (
      p_errbuf       OUT      VARCHAR2,
      p_retcode      OUT      VARCHAR2,
      p_request_id   IN       NUMBER,
      p_debug        IN       VARCHAR2
   )
   IS
      v_db_link      VARCHAR2 (1000);
      v_total_rows   NUMBER          := 0;
   BEGIN
      g_instance := 'R12';

      IF p_debug = 'YES'
      THEN
         g_log := 'Y';
      END IF;

      LOG ('BEGIN => PULL running on ' || g_instance);
      v_db_link := '@apps_to_issc11i.tcc.etn.com';

      IF NOT is_request_id_valid
                          ('xxar_br_invoices_stg@apps_to_issc11i.tcc.etn.com',
                           p_request_id
                          )
      THEN
         LOG (   'No records found for Request ID : '
              || p_request_id
              || ' in the source system'
             );
         p_errbuf :=
               'No records found for Request ID : '
            || p_request_id
            || ' in the source system';
         p_retcode := 1;
         RETURN;
      END IF;

      LOG (   'Data is about to be inserted into '
           || 'xxar_br_invoices_ext_r12'
           || ' and '
           || 'xxar_br_invoices_dist_ext_r12'
          );
      LOG (   'Data is about to be fetched FROM '
           || 'xxar_br_invoices_stg@apps_to_issc11i.tcc.etn.com'
           || ' and '
           || 'xxar_br_invoices_dist_stg@apps_to_issc11i.tcc.etn.com'
          );
      audit_trail
         ('START',
          p_leg_source_system       => 'ISSC',
          p_leg_request_id          => p_request_id,
          p_parameter               =>    'p_pull_from => '
                                       || 'ISSC'
                                       || ' ; p_request_id => '
                                       || p_request_id,
          p_extract_table_name      => 'xxar_br_invoices_stg@apps_to_issc11i.tcc.etn.com',
          p_insert_table_name       => 'xxar_br_invoices_ext_r12'
         );

      BEGIN
         LOG (   'Invoice insertion is starting at '
              || TO_CHAR (SYSDATE, 'DD-MON-YYYY  hh:mi:ss')
             );

         INSERT INTO xxar_br_invoices_ext_r12
                     (interface_txn_id                                    -- 1
                                      ,
                      leg_batch_source_name                               -- 2
                                           ,
                      leg_customer_number                                 -- 3
                                         ,
                      leg_bill_to_address                                 -- 4
                                         ,
                      leg_ship_to_address                                 -- 5
                                         ,
                      leg_currency_code                                   -- 6
                                       ,
                      leg_cust_trx_type_name                              -- 7
                                            ,
                      leg_line_amount                                     -- 8
                                     ,
                      leg_trx_date                                        -- 9
                                  ,
                      leg_tax_code                                       -- 10
                                  ,
                      leg_tax_rate                                       -- 11
                                  ,
                      leg_conversion_date                                -- 12
                                         ,
                      leg_conversion_rate                                -- 13
                                         ,
                      leg_term_name                                      -- 14
                                   ,
                      leg_set_of_books_name                              -- 15
                                           ,
                      leg_operating_unit                                 -- 16
                                        ,
                      leg_header_attribute_category                      -- 17
                                                   ,
                      leg_header_attribute1                              -- 18
                                           ,
                      leg_header_attribute2                              -- 19
                                           ,
                      leg_header_attribute3                              -- 20
                                           ,
                      leg_header_attribute4                              -- 21
                                           ,
                      leg_header_attribute5                              -- 22
                                           ,
                      leg_header_attribute6                              -- 23
                                           ,
                      leg_header_attribute7                              -- 24
                                           ,
                      leg_header_attribute8                              -- 25
                                           ,
                      leg_header_attribute9                              -- 26
                                           ,
                      leg_header_attribute10                             -- 27
                                            ,
                      leg_header_attribute11                             -- 28
                                            ,
                      leg_header_attribute12                             -- 29
                                            ,
                      leg_header_attribute13                             -- 30
                                            ,
                      leg_header_attribute14                             -- 31
                                            ,
                      leg_header_attribute15                             -- 32
                                            ,
                      leg_purchase_order                                 -- 33
                                        ,
                      leg_trx_number                                     -- 34
                                    ,
                      leg_line_number                                    -- 35
                                     ,
                      leg_comments                                       -- 36
                                  ,
                      leg_due_date                                       -- 37
                                  ,
                      leg_inv_amount_due_original                        -- 38
                                                 ,
                      leg_inv_amount_due_remaining                       -- 39
                                                  ,
                      leg_line_type                                      -- 40
                                   ,
                      leg_interface_line_context                         -- 41
                                                ,
                      leg_interface_line_attribute1                      -- 42
                                                   ,
                      leg_interface_line_attribute2                      -- 43
                                                   ,
                      leg_interface_line_attribute3                      -- 44
                                                   ,
                      leg_interface_line_attribute4                      -- 45
                                                   ,
                      leg_interface_line_attribute5                      -- 46
                                                   ,
                      leg_interface_line_attribute6                      -- 47
                                                   ,
                      leg_interface_line_attribute7                      -- 48
                                                   ,
                      leg_interface_line_attribute8                      -- 49
                                                   ,
                      leg_interface_line_attribute9                      -- 50
                                                   ,
                      leg_interface_line_attribute10                     -- 51
                                                    ,
                      leg_interface_line_attribute11                     -- 52
                                                    ,
                      leg_interface_line_attribute12                     -- 53
                                                    ,
                      leg_interface_line_attribute13                     -- 54
                                                    ,
                      leg_interface_line_attribute14                     -- 55
                                                    ,
                      leg_interface_line_attribute15                     -- 56
                                                    ,
                      leg_cust_trx_line_id                               -- 57
                                          ,
                      leg_link_to_cust_trx_line_id                       -- 58
                                                  ,
                      leg_header_gdf_attr_category                       -- 59
                                                  ,
                      leg_header_gdf_attribute1                          -- 60
                                               ,
                      leg_header_gdf_attribute2                          -- 61
                                               ,
                      leg_header_gdf_attribute3                          -- 62
                                               ,
                      leg_header_gdf_attribute4                          -- 63
                                               ,
                      leg_header_gdf_attribute5                          -- 64
                                               ,
                      leg_header_gdf_attribute6                          -- 65
                                               ,
                      leg_header_gdf_attribute7                          -- 66
                                               ,
                      leg_header_gdf_attribute8                          -- 67
                                               ,
                      leg_header_gdf_attribute9                          -- 68
                                               ,
                      leg_header_gdf_attribute10                         -- 69
                                                ,
                      leg_header_gdf_attribute11                         -- 70
                                                ,
                      leg_header_gdf_attribute12                         -- 71
                                                ,
                      leg_header_gdf_attribute13                         -- 72
                                                ,
                      leg_header_gdf_attribute14                         -- 73
                                                ,
                      leg_header_gdf_attribute15                         -- 74
                                                ,
                      leg_header_gdf_attribute16                         -- 75
                                                ,
                      leg_header_gdf_attribute17                         -- 76
                                                ,
                      leg_header_gdf_attribute18                         -- 77
                                                ,
                      leg_header_gdf_attribute19                         -- 78
                                                ,
                      leg_header_gdf_attribute20                         -- 79
                                                ,
                      leg_header_gdf_attribute21                         -- 80
                                                ,
                      leg_header_gdf_attribute22                         -- 81
                                                ,
                      leg_header_gdf_attribute23                         -- 82
                                                ,
                      leg_header_gdf_attribute24                         -- 83
                                                ,
                      leg_header_gdf_attribute25                         -- 84
                                                ,
                      leg_header_gdf_attribute26                         -- 85
                                                ,
                      leg_header_gdf_attribute27                         -- 86
                                                ,
                      leg_header_gdf_attribute28                         -- 87
                                                ,
                      leg_header_gdf_attribute29                         -- 88
                                                ,
                      leg_header_gdf_attribute30                         -- 89
                                                ,
                      leg_line_gdf_attr_category                         -- 90
                                                ,
                      leg_line_gdf_attribute1                            -- 91
                                             ,
                      leg_line_gdf_attribute2                            -- 92
                                             ,
                      leg_line_gdf_attribute3                            -- 93
                                             ,
                      leg_line_gdf_attribute4                            -- 94
                                             ,
                      leg_line_gdf_attribute5                            -- 95
                                             ,
                      leg_line_gdf_attribute6                            -- 96
                                             ,
                      leg_line_gdf_attribute7                            -- 97
                                             ,
                      leg_line_gdf_attribute8                            -- 98
                                             ,
                      leg_line_gdf_attribute9                            -- 99
                                             ,
                      leg_line_gdf_attribute10                          -- 100
                                              ,
                      leg_line_gdf_attribute11                          -- 101
                                              ,
                      leg_line_gdf_attribute12                          -- 102
                                              ,
                      leg_line_gdf_attribute13                          -- 103
                                              ,
                      leg_line_gdf_attribute14                          -- 104
                                              ,
                      leg_line_gdf_attribute15                          -- 105
                                              ,
                      leg_line_gdf_attribute16                          -- 106
                                              ,
                      leg_line_gdf_attribute17                          -- 107
                                              ,
                      leg_line_gdf_attribute18                          -- 108
                                              ,
                      leg_line_gdf_attribute19                          -- 109
                                              ,
                      leg_line_gdf_attribute20                          -- 110
                                              ,
                      leg_reason_code                                   -- 111
                                     ,
                      leg_source_system                                 -- 112
                                       ,
                      leg_quantity                                      -- 113
                                  ,
                      leg_quantity_ordered                              -- 114
                                          ,
                      leg_unit_selling_price                            -- 115
                                            ,
                      leg_unit_standard_price                           -- 116
                                             ,
                      leg_ship_date_actual                              -- 117
                                          ,
                      leg_fob_point                                     -- 118
                                   ,
                      leg_ship_via                                      -- 119
                                  ,
                      leg_waybill_number                                -- 120
                                        ,
                      leg_sales_order_line                              -- 121
                                          ,
                      leg_sales_order_date                              -- 122
                                          ,
                      leg_sales_order_source                            -- 123
                                            ,
                      leg_sales_order_revision                          -- 124
                                              ,
                      leg_purchase_order_revision                       -- 125
                                                 ,
                      leg_purchase_order_date                           -- 126
                                             ,
                      leg_memo_line_name                                -- 127
                                        ,
                      leg_internal_notes                                -- 128
                                        ,
                      leg_ussgl_trx_code_context                        -- 129
                                                ,
                      leg_uom_name                                      -- 130
                                  ,
                      leg_request_id                                    -- 131
                                    ,
                      leg_seq_num                                       -- 132
                                 ,
                      leg_process_flag                                  -- 133
                                      ,
                      leg_reference_line_id                             -- 134
                                           ,
                      leg_customer_trx_id                               -- 135
                                         ,
                      creation_date                   -- 136    DATE NOT NULL,
                                   ,
                      created_by                   -- 137     NUMBER NOT NULL,
                                ,
                      last_update_date                -- 138    DATE NOT NULL,
                                      ,
                      last_updated_by               -- 139    NUMBER NOT NULL,
                                     ,
                      leg_sales_order                                   -- 140
                                     ,
                      leg_gl_date                                       -- 141
                                 ,
                      leg_agreement_name                                -- 142
                                        ,
                      leg_vat_tax_name                                  -- 143
                                      ,
                      vat_tax_id                                        -- 144
                                --        ,    leg_location                        -- 145
                      ,
                      request_id, 
					  leg_br_openrec_ledger, 
					  leg_br_openrec_org,
                      leg_br_or_openreceipt_num, 
					  leg_br_amount, --added for v1.13 
					  leg_br_or_rec_maturity_date,
                      leg_br_or_rec_issue_date)
            SELECT interface_txn_id, leg_batch_source_name,
                   leg_customer_number, leg_bill_to_address,
                   leg_ship_to_address, leg_currency_code,
                   leg_cust_trx_type_name, leg_line_amount, leg_trx_date,
                   leg_tax_code, leg_tax_rate, leg_conversion_date,
                   leg_conversion_rate, leg_term_name, leg_set_of_books_name,
                   leg_operating_unit, leg_header_attribute_category,
                   leg_header_attribute1, leg_header_attribute2,
                   leg_header_attribute3, leg_header_attribute4,
                   leg_header_attribute5, leg_header_attribute6,
                   leg_header_attribute7, leg_header_attribute8,
                   leg_header_attribute9, leg_header_attribute10,
                   leg_header_attribute11, leg_header_attribute12,
                   leg_header_attribute13, leg_header_attribute14,
                   leg_header_attribute15, leg_purchase_order, leg_trx_number,
                   leg_line_number, leg_comments, leg_due_date,
                   leg_inv_amount_due_original, leg_inv_amount_due_remaining,
                   leg_line_type, leg_interface_line_context,
                   leg_interface_line_attribute1,
                   leg_interface_line_attribute2,
                   leg_interface_line_attribute3,
                   leg_interface_line_attribute4,
                   leg_interface_line_attribute5,
                   leg_interface_line_attribute6,
                   leg_interface_line_attribute7,
                   leg_interface_line_attribute8,
                   leg_interface_line_attribute9,
                   leg_interface_line_attribute10,
                   leg_interface_line_attribute11,
                   leg_interface_line_attribute12,
                   leg_interface_line_attribute13,
                   leg_interface_line_attribute14,
                   leg_interface_line_attribute15, leg_cust_trx_line_id,
                   leg_link_to_cust_trx_line_id, leg_header_gdf_attr_category,
                   leg_header_gdf_attribute1, leg_header_gdf_attribute2,
                   leg_header_gdf_attribute3, leg_header_gdf_attribute4,
                   leg_header_gdf_attribute5, leg_header_gdf_attribute6,
                   leg_header_gdf_attribute7, leg_header_gdf_attribute8,
                   leg_header_gdf_attribute9, leg_header_gdf_attribute10,
                   leg_header_gdf_attribute11, leg_header_gdf_attribute12,
                   leg_header_gdf_attribute13, leg_header_gdf_attribute14,
                   leg_header_gdf_attribute15, leg_header_gdf_attribute16,
                   leg_header_gdf_attribute17, leg_header_gdf_attribute18,
                   leg_header_gdf_attribute19, leg_header_gdf_attribute20,
                   leg_header_gdf_attribute21, leg_header_gdf_attribute22,
                   leg_header_gdf_attribute23, leg_header_gdf_attribute24,
                   leg_header_gdf_attribute25, leg_header_gdf_attribute26,
                   leg_header_gdf_attribute27, leg_header_gdf_attribute28,
                   leg_header_gdf_attribute29, leg_header_gdf_attribute30,
                   leg_line_gdf_attr_category, leg_line_gdf_attribute1,
                   leg_line_gdf_attribute2, leg_line_gdf_attribute3,
                   leg_line_gdf_attribute4, leg_line_gdf_attribute5,
                   leg_line_gdf_attribute6, leg_line_gdf_attribute7,
                   leg_line_gdf_attribute8, leg_line_gdf_attribute9,
                   leg_line_gdf_attribute10, leg_line_gdf_attribute11,
                   leg_line_gdf_attribute12, leg_line_gdf_attribute13,
                   leg_line_gdf_attribute14, leg_line_gdf_attribute15,
                   leg_line_gdf_attribute16, leg_line_gdf_attribute17,
                   leg_line_gdf_attribute18, leg_line_gdf_attribute19,
                   leg_line_gdf_attribute20, leg_reason_code,
                   leg_source_system, leg_quantity, leg_quantity_ordered,
                   leg_unit_selling_price, leg_unit_standard_price,
                   leg_ship_date_actual, leg_fob_point, leg_ship_via,
                   leg_waybill_number, leg_sales_order_line,
                   leg_sales_order_date, leg_sales_order_source,
                   leg_sales_order_revision, leg_purchase_order_revision,
                   leg_purchase_order_date, leg_memo_line_name,
                   leg_internal_notes, leg_ussgl_trx_code_context,
                   leg_uom_name, leg_request_id, leg_seq_num, 'V',
                   leg_reference_line_id, leg_customer_trx_id, creation_date,
                   created_by, last_updated_date, last_updated_by,
                   leg_sales_order, leg_gl_date, leg_agreement_name,
                   leg_vat_tax_name, vat_tax_id
                                               --        ,    leg_location
                   , request_id, leg_br_openrec_ledger, leg_br_openrec_org,
                   leg_br_or_openreceipt_num, 
				   leg_br_amount, --added for v1.13
				   leg_br_or_rec_maturity_date,
                   leg_br_or_rec_issue_date
              FROM xxar_br_invoices_stg@apps_to_issc11i.tcc.etn.com
             WHERE leg_request_id = p_request_id
       AND leg_process_flag <>'D' /*added for v1.6*/ ;

         v_total_rows := SQL%ROWCOUNT;
         LOG (   'Invoice insertion is completed; '
              || v_total_rows
              || ' rows inserted at '
              || TO_CHAR (SYSDATE, 'DD-MON-YYYY  hh:mi:ss')
             );
         audit_trail ('END',
                      p_insert_table_name      => 'xxar_br_invoices_ext_r12',
                      p_records_inserted       => v_total_rows
                     );
      EXCEPTION
         WHEN OTHERS
         THEN
            LOG (   'Main : Error while inserting into Staging table '
                 || 'xxar_br_invoices_dist_ext_r12'
                 || ' : '
                 || SQLERRM
                );
            register_error
               (p_orgz_id       => -1,
                p_err_code      => SQLCODE,
                p_err_msg       =>    'Main : Error while inserting into Staging table '
                                   || 'xxar_br_invoices_ext_r12'
                                   || ' : '
                                   || SQLERRM,
                p_table         => 'xxar_br_invoices_stg@apps_to_issc11i.tcc.etn.com'
               );
            p_errbuf :=
                  'Conurrent program completed with error. Check table xxetn_extn_errors for conc_request_id '
               || g_conc_request_id;
            p_retcode := 2;
            RETURN;
      END;

      audit_trail
         ('START',
          p_leg_source_system       => 'ISSC',
          p_leg_request_id          => p_request_id,
          p_parameter               =>    'p_pull_from => '
                                       || 'ISSC'
                                       || ' ; p_request_id => '
                                       || p_request_id,
          p_extract_table_name      => 'xxar_br_invoices_stg@apps_to_issc11i.tcc.etn.com',
          p_insert_table_name       => 'xxar_br_invoices_dist_ext_r12'
         );

      BEGIN
         LOG (   'Distribution Isertion started at '
              || TO_CHAR (SYSDATE, 'DD-MON-YYYY  hh:mi:ss')
             );

         INSERT INTO xxar_br_invoices_dist_ext_r12
                     (interface_txn_id                                   -- :1
                                      ,
                      leg_customer_trx_id                                -- :2
                                         ,
                      leg_cust_trx_line_id                               -- :3
                                          ,
                      leg_cust_trx_line_gl_dist_id                       -- :4
                                                  ,
                      leg_percent                                        -- :5
                                 --        ,    leg_cust_trx_type_name             -- :6
                      ,
                      leg_account_class                                  -- :7
                                       ,
                      leg_dist_segment1                                  -- :8
                                       ,
                      leg_dist_segment2                                  -- :9
                                       ,
                      leg_dist_segment3                                 -- :10
                                       ,
                      leg_dist_segment4                                 -- :11
                                       ,
                      leg_dist_segment5                                 -- :12
                                       ,
                      leg_dist_segment6                                 -- :13
                                       ,
                      leg_dist_segment7                                 -- :14
                                       ,
                      leg_org_name                                      -- :15
                                  ,
                      leg_accounted_amount                              -- :16
                                          ,
                      creation_date                                     -- :17
                                   ,
                      created_by                                        -- :18
                                ,
                      last_update_date                                  -- :19
                                      ,
                      last_updated_by                                   -- :20
                                     ,
                      leg_source_system                                 -- :21
                                       ,
                      leg_request_id                                    -- :22
                                    ,
                      leg_seq_num                                       -- :23
                                 ,
                      leg_process_flag                                  -- :24
                                      ,
                      request_id)
            SELECT interface_txn_id, leg_customer_trx_id,
                   leg_cust_trx_line_id, leg_cust_trx_line_gl_dist_id,
                   leg_percent
                              --            ,    leg_cust_trx_type_name
                   , leg_account_class, leg_dist_segment1, leg_dist_segment2,
                   leg_dist_segment3, leg_dist_segment4, leg_dist_segment5,
                   leg_dist_segment6, leg_dist_segment7, leg_org_name,
                   leg_accounted_amount, creation_date, created_by,
                   last_update_date, last_updated_by, leg_source_system,
                   leg_request_id, leg_seq_num, 'V', request_id
              FROM xxar_br_invoices_dist_stg@apps_to_issc11i.tcc.etn.com
             WHERE leg_request_id = p_request_id;

         v_total_rows := SQL%ROWCOUNT;
         LOG (   'Distribution INSERTion completed; '
              || v_total_rows
              || ' rows inserted at '
              || TO_CHAR (SYSDATE, 'DD-MON-YYYY  hh:mi:ss')
             );
         audit_trail ('END',
                      p_insert_table_name      => 'xxar_br_invoices_dist_ext_r12',
                      p_records_inserted       => v_total_rows
                     );
      EXCEPTION
         WHEN OTHERS
         THEN
            register_error
               (p_orgz_id       => -1,
                p_err_code      => SQLCODE,
                p_err_msg       =>    'Main : Error while inserting into Staging table : '
                                   || 'xxar_br_invoices_dist_ext_r12'
                                   || ' : '
                                   || SQLERRM,
                p_table         => 'xxar_br_invoices_stg@apps_to_issc11i.tcc.etn.com'
               );
            p_errbuf :=
                  'Conurrent program completed with error. Check table xxetn_extn_errors for conc_request_id '
               || g_conc_request_id;
            p_retcode := 2;
      END;

      LOG (   'Pull process completed at '
           || TO_CHAR (SYSDATE, 'DD-MON-YYYY  hh:mi:ss')
          );
   END pull;

--------pull data end-----------
    -- ========================
-- Procedure: PRINT_LOG_MESSAGE
-- =============================================================================
--   This procedure is used to write message to log file.
-- =============================================================================
   PROCEDURE print_log_message (piv_message IN VARCHAR2)
   IS
   BEGIN
      IF NVL (g_request_id, 0) > 0
      THEN
         fnd_file.put_line (fnd_file.LOG, piv_message);
      END IF;
   EXCEPTION
      WHEN OTHERS
      THEN
         NULL;
   END print_log_message;

 -- ========================
-- Procedure: LOAD_INVOICE
-- =============================================================================
--   This procedure is used to load invoice lines data from extraction staging table
--   to conversion staging table when program is run in LOAD mode
-- =============================================================================
   PROCEDURE load_invoice (
      pov_ret_stats   OUT NOCOPY   VARCHAR2,
      pov_err_msg     OUT NOCOPY   VARCHAR2
   )
   IS
      TYPE leg_br_invoice_rec IS RECORD (
         interface_txn_id                 xxar_br_invoices_ext_r12.interface_txn_id%TYPE,
         batch_id                         xxar_br_invoices_ext_r12.batch_id%TYPE,
         load_id                          xxar_br_invoices_ext_r12.load_id%TYPE,
         run_sequence_id                  xxar_br_invoices_ext_r12.run_sequence_id%TYPE,
         leg_batch_source_name            xxar_br_invoices_ext_r12.leg_batch_source_name%TYPE,
         leg_customer_number              xxar_br_invoices_ext_r12.leg_customer_number%TYPE,
         leg_bill_to_address              xxar_br_invoices_ext_r12.leg_bill_to_address%TYPE,
         leg_ship_to_address              xxar_br_invoices_ext_r12.leg_ship_to_address%TYPE,
         leg_currency_code                xxar_br_invoices_ext_r12.leg_currency_code%TYPE,
         leg_cust_trx_type_name           xxar_br_invoices_ext_r12.leg_cust_trx_type_name%TYPE,
         leg_line_amount                  xxar_br_invoices_ext_r12.leg_line_amount%TYPE,
         leg_trx_date                     xxar_br_invoices_ext_r12.leg_trx_date%TYPE,
         leg_tax_code                     xxar_br_invoices_ext_r12.leg_tax_code%TYPE,
         leg_tax_rate                     xxar_br_invoices_ext_r12.leg_tax_rate%TYPE,
         leg_conversion_date              xxar_br_invoices_ext_r12.leg_conversion_date%TYPE,
         leg_conversion_rate              xxar_br_invoices_ext_r12.leg_conversion_rate%TYPE,
         leg_term_name                    xxar_br_invoices_ext_r12.leg_term_name%TYPE,
         leg_set_of_books_name            xxar_br_invoices_ext_r12.leg_set_of_books_name%TYPE,
         leg_operating_unit               xxar_br_invoices_ext_r12.leg_operating_unit%TYPE,
         leg_header_attribute_category    xxar_br_invoices_ext_r12.leg_header_attribute_category%TYPE,
         leg_header_attribute1            xxar_br_invoices_ext_r12.leg_header_attribute1%TYPE,
         leg_header_attribute2            xxar_br_invoices_ext_r12.leg_header_attribute2%TYPE,
         leg_header_attribute3            xxar_br_invoices_ext_r12.leg_header_attribute3%TYPE,
         leg_header_attribute4            xxar_br_invoices_ext_r12.leg_header_attribute4%TYPE,
         leg_header_attribute5            xxar_br_invoices_ext_r12.leg_header_attribute5%TYPE,
         leg_header_attribute6            xxar_br_invoices_ext_r12.leg_header_attribute6%TYPE,
         leg_header_attribute7            xxar_br_invoices_ext_r12.leg_header_attribute7%TYPE,
         leg_header_attribute8            xxar_br_invoices_ext_r12.leg_header_attribute8%TYPE,
         leg_header_attribute9            xxar_br_invoices_ext_r12.leg_header_attribute9%TYPE,
         leg_header_attribute10           xxar_br_invoices_ext_r12.leg_header_attribute10%TYPE,
         leg_header_attribute11           xxar_br_invoices_ext_r12.leg_header_attribute11%TYPE,
         leg_header_attribute12           xxar_br_invoices_ext_r12.leg_header_attribute12%TYPE,
         leg_header_attribute13           xxar_br_invoices_ext_r12.leg_header_attribute13%TYPE,
         leg_header_attribute14           xxar_br_invoices_ext_r12.leg_header_attribute14%TYPE,
         leg_header_attribute15           xxar_br_invoices_ext_r12.leg_header_attribute15%TYPE,
         leg_reference_line_id            xxar_br_invoices_ext_r12.leg_reference_line_id%TYPE,
         leg_purchase_order               xxar_br_invoices_ext_r12.leg_purchase_order%TYPE,
         leg_trx_number                   xxar_br_invoices_ext_r12.leg_trx_number%TYPE,
         leg_line_number                  xxar_br_invoices_ext_r12.leg_line_number%TYPE,
         leg_comments                     xxar_br_invoices_ext_r12.leg_comments%TYPE,
         leg_due_date                     xxar_br_invoices_ext_r12.leg_due_date%TYPE,
         leg_inv_amount_due_original      xxar_br_invoices_ext_r12.leg_inv_amount_due_original%TYPE,
         leg_inv_amount_due_remaining     xxar_br_invoices_ext_r12.leg_inv_amount_due_remaining%TYPE,
         leg_line_type                    xxar_br_invoices_ext_r12.leg_line_type%TYPE,
         leg_interface_line_context       xxar_br_invoices_ext_r12.leg_interface_line_context%TYPE,
         leg_interface_line_attribute1    xxar_br_invoices_ext_r12.leg_interface_line_attribute1%TYPE,
         leg_interface_line_attribute2    xxar_br_invoices_ext_r12.leg_interface_line_attribute2%TYPE,
         leg_interface_line_attribute3    xxar_br_invoices_ext_r12.leg_interface_line_attribute3%TYPE,
         leg_interface_line_attribute4    xxar_br_invoices_ext_r12.leg_interface_line_attribute4%TYPE,
         leg_interface_line_attribute5    xxar_br_invoices_ext_r12.leg_interface_line_attribute5%TYPE,
         leg_interface_line_attribute6    xxar_br_invoices_ext_r12.leg_interface_line_attribute6%TYPE,
         leg_interface_line_attribute7    xxar_br_invoices_ext_r12.leg_interface_line_attribute7%TYPE,
         leg_interface_line_attribute8    xxar_br_invoices_ext_r12.leg_interface_line_attribute8%TYPE,
         leg_interface_line_attribute9    xxar_br_invoices_ext_r12.leg_interface_line_attribute9%TYPE,
         leg_interface_line_attribute10   xxar_br_invoices_ext_r12.leg_interface_line_attribute10%TYPE,
         leg_interface_line_attribute11   xxar_br_invoices_ext_r12.leg_interface_line_attribute11%TYPE,
         leg_interface_line_attribute12   xxar_br_invoices_ext_r12.leg_interface_line_attribute12%TYPE,
         leg_interface_line_attribute13   xxar_br_invoices_ext_r12.leg_interface_line_attribute13%TYPE,
         leg_interface_line_attribute14   xxar_br_invoices_ext_r12.leg_interface_line_attribute14%TYPE,
         leg_interface_line_attribute15   xxar_br_invoices_ext_r12.leg_interface_line_attribute15%TYPE,
         leg_customer_trx_id              xxar_br_invoices_ext_r12.leg_customer_trx_id%TYPE,
         trx_type                         xxar_br_invoices_ext_r12.trx_type%TYPE,
         leg_cust_trx_line_id             xxar_br_invoices_ext_r12.leg_cust_trx_line_id%TYPE,
         leg_link_to_cust_trx_line_id     xxar_br_invoices_ext_r12.leg_link_to_cust_trx_line_id%TYPE,
         leg_header_gdf_attr_category     xxar_br_invoices_ext_r12.leg_header_gdf_attr_category%TYPE,
         leg_header_gdf_attribute1        xxar_br_invoices_ext_r12.leg_header_gdf_attribute1%TYPE,
         leg_header_gdf_attribute2        xxar_br_invoices_ext_r12.leg_header_gdf_attribute2%TYPE,
         leg_header_gdf_attribute3        xxar_br_invoices_ext_r12.leg_header_gdf_attribute3%TYPE,
         leg_header_gdf_attribute4        xxar_br_invoices_ext_r12.leg_header_gdf_attribute4%TYPE,
         leg_header_gdf_attribute5        xxar_br_invoices_ext_r12.leg_header_gdf_attribute5%TYPE,
         leg_header_gdf_attribute6        xxar_br_invoices_ext_r12.leg_header_gdf_attribute6%TYPE,
         leg_header_gdf_attribute7        xxar_br_invoices_ext_r12.leg_header_gdf_attribute7%TYPE,
         leg_header_gdf_attribute8        xxar_br_invoices_ext_r12.leg_header_gdf_attribute8%TYPE,
         leg_header_gdf_attribute9        xxar_br_invoices_ext_r12.leg_header_gdf_attribute9%TYPE,
         leg_header_gdf_attribute10       xxar_br_invoices_ext_r12.leg_header_gdf_attribute10%TYPE,
         leg_header_gdf_attribute11       xxar_br_invoices_ext_r12.leg_header_gdf_attribute11%TYPE,
         leg_header_gdf_attribute12       xxar_br_invoices_ext_r12.leg_header_gdf_attribute12%TYPE,
         leg_header_gdf_attribute13       xxar_br_invoices_ext_r12.leg_header_gdf_attribute13%TYPE,
         leg_header_gdf_attribute14       xxar_br_invoices_ext_r12.leg_header_gdf_attribute14%TYPE,
         leg_header_gdf_attribute15       xxar_br_invoices_ext_r12.leg_header_gdf_attribute15%TYPE,
         leg_header_gdf_attribute16       xxar_br_invoices_ext_r12.leg_header_gdf_attribute16%TYPE,
         leg_header_gdf_attribute17       xxar_br_invoices_ext_r12.leg_header_gdf_attribute17%TYPE,
         leg_header_gdf_attribute18       xxar_br_invoices_ext_r12.leg_header_gdf_attribute18%TYPE,
         leg_header_gdf_attribute19       xxar_br_invoices_ext_r12.leg_header_gdf_attribute19%TYPE,
         leg_header_gdf_attribute20       xxar_br_invoices_ext_r12.leg_header_gdf_attribute20%TYPE,
         leg_header_gdf_attribute21       xxar_br_invoices_ext_r12.leg_header_gdf_attribute21%TYPE,
         leg_header_gdf_attribute22       xxar_br_invoices_ext_r12.leg_header_gdf_attribute22%TYPE,
         leg_header_gdf_attribute23       xxar_br_invoices_ext_r12.leg_header_gdf_attribute23%TYPE,
         leg_header_gdf_attribute24       xxar_br_invoices_ext_r12.leg_header_gdf_attribute24%TYPE,
         leg_header_gdf_attribute25       xxar_br_invoices_ext_r12.leg_header_gdf_attribute25%TYPE,
         leg_header_gdf_attribute26       xxar_br_invoices_ext_r12.leg_header_gdf_attribute26%TYPE,
         leg_header_gdf_attribute27       xxar_br_invoices_ext_r12.leg_header_gdf_attribute27%TYPE,
         leg_header_gdf_attribute28       xxar_br_invoices_ext_r12.leg_header_gdf_attribute28%TYPE,
         leg_header_gdf_attribute29       xxar_br_invoices_ext_r12.leg_header_gdf_attribute29%TYPE,
         leg_header_gdf_attribute30       xxar_br_invoices_ext_r12.leg_header_gdf_attribute30%TYPE,
         leg_line_gdf_attr_category       xxar_br_invoices_ext_r12.leg_line_gdf_attr_category%TYPE,
         leg_line_gdf_attribute1          xxar_br_invoices_ext_r12.leg_line_gdf_attribute1%TYPE,
         leg_line_gdf_attribute2          xxar_br_invoices_ext_r12.leg_line_gdf_attribute2%TYPE,
         leg_line_gdf_attribute3          xxar_br_invoices_ext_r12.leg_line_gdf_attribute3%TYPE,
         leg_line_gdf_attribute4          xxar_br_invoices_ext_r12.leg_line_gdf_attribute4%TYPE,
         leg_line_gdf_attribute5          xxar_br_invoices_ext_r12.leg_line_gdf_attribute5%TYPE,
         leg_line_gdf_attribute6          xxar_br_invoices_ext_r12.leg_line_gdf_attribute6%TYPE,
         leg_line_gdf_attribute7          xxar_br_invoices_ext_r12.leg_line_gdf_attribute7%TYPE,
         leg_line_gdf_attribute8          xxar_br_invoices_ext_r12.leg_line_gdf_attribute8%TYPE,
         leg_line_gdf_attribute9          xxar_br_invoices_ext_r12.leg_line_gdf_attribute9%TYPE,
         leg_line_gdf_attribute10         xxar_br_invoices_ext_r12.leg_line_gdf_attribute10%TYPE,
         leg_line_gdf_attribute11         xxar_br_invoices_ext_r12.leg_line_gdf_attribute11%TYPE,
         leg_line_gdf_attribute12         xxar_br_invoices_ext_r12.leg_line_gdf_attribute12%TYPE,
         leg_line_gdf_attribute13         xxar_br_invoices_ext_r12.leg_line_gdf_attribute13%TYPE,
         leg_line_gdf_attribute14         xxar_br_invoices_ext_r12.leg_line_gdf_attribute14%TYPE,
         leg_line_gdf_attribute15         xxar_br_invoices_ext_r12.leg_line_gdf_attribute15%TYPE,
         leg_line_gdf_attribute16         xxar_br_invoices_ext_r12.leg_line_gdf_attribute16%TYPE,
         leg_line_gdf_attribute17         xxar_br_invoices_ext_r12.leg_line_gdf_attribute17%TYPE,
         leg_line_gdf_attribute18         xxar_br_invoices_ext_r12.leg_line_gdf_attribute18%TYPE,
         leg_line_gdf_attribute19         xxar_br_invoices_ext_r12.leg_line_gdf_attribute19%TYPE,
         leg_line_gdf_attribute20         xxar_br_invoices_ext_r12.leg_line_gdf_attribute20%TYPE,
         leg_reason_code                  xxar_br_invoices_ext_r12.leg_reason_code%TYPE,
         leg_source_system                xxar_br_invoices_ext_r12.leg_source_system%TYPE,
         leg_quantity                     xxar_br_invoices_ext_r12.leg_quantity%TYPE,
         leg_quantity_ordered             xxar_br_invoices_ext_r12.leg_quantity_ordered%TYPE,
         leg_unit_selling_price           xxar_br_invoices_ext_r12.leg_unit_selling_price%TYPE,
         leg_unit_standard_price          xxar_br_invoices_ext_r12.leg_unit_standard_price%TYPE,
         leg_ship_date_actual             xxar_br_invoices_ext_r12.leg_ship_date_actual%TYPE,
         leg_fob_point                    xxar_br_invoices_ext_r12.leg_fob_point%TYPE,
         leg_ship_via                     xxar_br_invoices_ext_r12.leg_ship_via%TYPE,
         leg_waybill_number               xxar_br_invoices_ext_r12.leg_waybill_number%TYPE,
         leg_sales_order_line             xxar_br_invoices_ext_r12.leg_sales_order_line%TYPE,
         leg_sales_order                  xxar_br_invoices_ext_r12.leg_sales_order%TYPE,
         leg_gl_date                      xxar_br_invoices_ext_r12.leg_gl_date%TYPE,
         leg_sales_order_date             xxar_br_invoices_ext_r12.leg_sales_order_date%TYPE,
         leg_sales_order_source           xxar_br_invoices_ext_r12.leg_sales_order_source%TYPE,
         leg_sales_order_revision         xxar_br_invoices_ext_r12.leg_sales_order_revision%TYPE,
         leg_purchase_order_revision      xxar_br_invoices_ext_r12.leg_purchase_order_revision%TYPE,
         leg_purchase_order_date          xxar_br_invoices_ext_r12.leg_purchase_order_date%TYPE,
         leg_agreement_name               xxar_br_invoices_ext_r12.leg_agreement_name%TYPE,
         leg_agreement_id                 xxar_br_invoices_ext_r12.leg_agreement_id%TYPE,
         leg_memo_line_name               xxar_br_invoices_ext_r12.leg_memo_line_name%TYPE,
         leg_internal_notes               xxar_br_invoices_ext_r12.leg_internal_notes%TYPE,
         leg_ussgl_trx_code_context       xxar_br_invoices_ext_r12.leg_ussgl_trx_code_context%TYPE,
         leg_uom_name                     xxar_br_invoices_ext_r12.leg_uom_name%TYPE,
         leg_vat_tax_name                 xxar_br_invoices_ext_r12.leg_vat_tax_name%TYPE,
         leg_sales_tax_name               xxar_br_invoices_ext_r12.leg_sales_tax_name%TYPE,
         leg_request_id                   xxar_br_invoices_ext_r12.leg_request_id%TYPE,
         leg_seq_num                      xxar_br_invoices_ext_r12.leg_seq_num%TYPE,
         leg_process_flag                 xxar_br_invoices_ext_r12.leg_process_flag%TYPE,
         currency_code                    xxar_br_invoices_ext_r12.currency_code%TYPE,
         cust_trx_type_name               xxar_br_invoices_ext_r12.cust_trx_type_name%TYPE,
         line_type                        xxar_br_invoices_ext_r12.line_type%TYPE,
         set_of_books_id                  xxar_br_invoices_ext_r12.set_of_books_id%TYPE,
         trx_number                       xxar_br_invoices_ext_r12.trx_number%TYPE,
         line_number                      xxar_br_invoices_ext_r12.line_number%TYPE,
         gl_date                          xxar_br_invoices_ext_r12.gl_date%TYPE,
         memo_line_id                     NUMBER,
         description                      xxar_br_invoices_ext_r12.description%TYPE,
         header_attribute_category        xxar_br_invoices_ext_r12.header_attribute_category%TYPE,
         header_attribute1                xxar_br_invoices_ext_r12.header_attribute1%TYPE,
         header_attribute2                xxar_br_invoices_ext_r12.header_attribute2%TYPE,
         header_attribute3                xxar_br_invoices_ext_r12.header_attribute3%TYPE,
         header_attribute4                xxar_br_invoices_ext_r12.header_attribute4%TYPE,
         header_attribute5                xxar_br_invoices_ext_r12.header_attribute5%TYPE,
         header_attribute6                xxar_br_invoices_ext_r12.header_attribute6%TYPE,
         header_attribute7                xxar_br_invoices_ext_r12.header_attribute7%TYPE,
         header_attribute8                xxar_br_invoices_ext_r12.header_attribute8%TYPE,
         header_attribute9                xxar_br_invoices_ext_r12.header_attribute9%TYPE,
         header_attribute10               xxar_br_invoices_ext_r12.header_attribute10%TYPE,
         header_attribute11               xxar_br_invoices_ext_r12.header_attribute11%TYPE,
         header_attribute12               xxar_br_invoices_ext_r12.header_attribute12%TYPE,
         header_attribute13               xxar_br_invoices_ext_r12.header_attribute13%TYPE,
         header_attribute14               xxar_br_invoices_ext_r12.header_attribute14%TYPE,
         header_attribute15               xxar_br_invoices_ext_r12.header_attribute15%TYPE,
         interface_line_context           xxar_br_invoices_ext_r12.interface_line_context%TYPE,
         interface_line_attribute1        xxar_br_invoices_ext_r12.interface_line_attribute1%TYPE,
         interface_line_attribute2        xxar_br_invoices_ext_r12.interface_line_attribute2%TYPE,
         interface_line_attribute3        xxar_br_invoices_ext_r12.interface_line_attribute3%TYPE,
         interface_line_attribute4        xxar_br_invoices_ext_r12.interface_line_attribute4%TYPE,
         interface_line_attribute5        xxar_br_invoices_ext_r12.interface_line_attribute5%TYPE,
         interface_line_attribute6        xxar_br_invoices_ext_r12.interface_line_attribute6%TYPE,
         interface_line_attribute7        xxar_br_invoices_ext_r12.interface_line_attribute7%TYPE,
         interface_line_attribute8        xxar_br_invoices_ext_r12.interface_line_attribute8%TYPE,
         interface_line_attribute9        xxar_br_invoices_ext_r12.interface_line_attribute9%TYPE,
         interface_line_attribute10       xxar_br_invoices_ext_r12.interface_line_attribute10%TYPE,
         interface_line_attribute11       xxar_br_invoices_ext_r12.interface_line_attribute11%TYPE,
         interface_line_attribute12       xxar_br_invoices_ext_r12.interface_line_attribute12%TYPE,
         interface_line_attribute13       xxar_br_invoices_ext_r12.interface_line_attribute13%TYPE,
         interface_line_attribute14       xxar_br_invoices_ext_r12.interface_line_attribute14%TYPE,
         interface_line_attribute15       xxar_br_invoices_ext_r12.interface_line_attribute15%TYPE,
         system_bill_customer_id          xxar_br_invoices_ext_r12.system_bill_customer_id%TYPE,
         system_bill_customer_ref         xxar_br_invoices_ext_r12.system_bill_customer_ref%TYPE,
         system_bill_address_id           xxar_br_invoices_ext_r12.system_bill_address_id%TYPE,
         system_bill_address_ref          xxar_br_invoices_ext_r12.system_bill_address_ref%TYPE,
         system_bill_contact_id           xxar_br_invoices_ext_r12.system_bill_contact_id%TYPE,
         system_ship_customer_id          xxar_br_invoices_ext_r12.system_ship_customer_id%TYPE,
         system_ship_customer_ref         xxar_br_invoices_ext_r12.system_ship_customer_ref%TYPE,
         system_ship_address_id           xxar_br_invoices_ext_r12.system_ship_address_id%TYPE,
         system_ship_address_ref          xxar_br_invoices_ext_r12.system_ship_address_ref%TYPE,
         system_ship_contact_id           xxar_br_invoices_ext_r12.system_ship_contact_id%TYPE,
         system_sold_customer_id          xxar_br_invoices_ext_r12.system_sold_customer_id%TYPE,
         system_sold_customer_ref         xxar_br_invoices_ext_r12.system_sold_customer_ref%TYPE,
         term_name                        xxar_br_invoices_ext_r12.term_name%TYPE,
         ou_name                          xxar_br_invoices_ext_r12.ou_name%TYPE,
         conversion_type                  xxar_br_invoices_ext_r12.conversion_type%TYPE,
         conversion_date                  xxar_br_invoices_ext_r12.conversion_date%TYPE,
         conversion_rate                  xxar_br_invoices_ext_r12.conversion_rate%TYPE,
         trx_date                         xxar_br_invoices_ext_r12.trx_date%TYPE,
         batch_source_name                xxar_br_invoices_ext_r12.batch_source_name%TYPE,
         purchase_order                   xxar_br_invoices_ext_r12.purchase_order%TYPE,
         sales_order_date                 xxar_br_invoices_ext_r12.sales_order_date%TYPE,
         sales_order                      xxar_br_invoices_ext_r12.sales_order%TYPE,
         reference_line_id                NUMBER,
         term_id                          xxar_br_invoices_ext_r12.term_id%TYPE,
         org_id                           xxar_br_invoices_ext_r12.org_id%TYPE,
         transaction_type_id              xxar_br_invoices_ext_r12.transaction_type_id%TYPE,
         tax_regime_code                  xxar_br_invoices_ext_r12.tax_regime_code%TYPE,
         tax_code                         xxar_br_invoices_ext_r12.tax_code%TYPE,
         tax                              xxar_br_invoices_ext_r12.tax%TYPE,
         tax_status_code                  xxar_br_invoices_ext_r12.tax_status_code%TYPE,
         tax_rate_code                    xxar_br_invoices_ext_r12.tax_rate_code%TYPE,
         tax_jurisdiction_code            xxar_br_invoices_ext_r12.tax_jurisdiction_code%TYPE,
         tax_rate                         xxar_br_invoices_ext_r12.tax_rate%TYPE,
         adjustment_amount                xxar_br_invoices_ext_r12.adjustment_amount%TYPE,
         inv_amount_due_original          xxar_br_invoices_ext_r12.inv_amount_due_original%TYPE,
         inv_amount_due_remaining         xxar_br_invoices_ext_r12.inv_amount_due_remaining%TYPE,
         link_to_line_context             xxar_br_invoices_ext_r12.link_to_line_context%TYPE,
         link_to_line_attribute1          xxar_br_invoices_ext_r12.link_to_line_attribute1%TYPE,
         link_to_line_attribute2          xxar_br_invoices_ext_r12.link_to_line_attribute2%TYPE,
         link_to_line_attribute3          xxar_br_invoices_ext_r12.link_to_line_attribute3%TYPE,
         link_to_line_attribute4          xxar_br_invoices_ext_r12.link_to_line_attribute4%TYPE,
         link_to_line_attribute5          xxar_br_invoices_ext_r12.link_to_line_attribute5%TYPE,
         link_to_line_attribute6          xxar_br_invoices_ext_r12.link_to_line_attribute6%TYPE,
         link_to_line_attribute7          xxar_br_invoices_ext_r12.link_to_line_attribute7%TYPE,
         link_to_line_attribute8          xxar_br_invoices_ext_r12.link_to_line_attribute8%TYPE,
         link_to_line_attribute9          xxar_br_invoices_ext_r12.link_to_line_attribute9%TYPE,
         link_to_line_attribute10         xxar_br_invoices_ext_r12.link_to_line_attribute10%TYPE,
         link_to_line_attribute11         xxar_br_invoices_ext_r12.link_to_line_attribute11%TYPE,
         link_to_line_attribute12         xxar_br_invoices_ext_r12.link_to_line_attribute12%TYPE,
         link_to_line_attribute13         xxar_br_invoices_ext_r12.link_to_line_attribute13%TYPE,
         link_to_line_attribute14         xxar_br_invoices_ext_r12.link_to_line_attribute14%TYPE,
         link_to_line_attribute15         xxar_br_invoices_ext_r12.link_to_line_attribute15%TYPE,
         header_gdf_attr_category         xxar_br_invoices_ext_r12.header_gdf_attr_category%TYPE,
         header_gdf_attribute1            xxar_br_invoices_ext_r12.header_gdf_attribute1%TYPE,
         header_gdf_attribute2            xxar_br_invoices_ext_r12.header_gdf_attribute2%TYPE,
         header_gdf_attribute3            xxar_br_invoices_ext_r12.header_gdf_attribute3%TYPE,
         header_gdf_attribute4            xxar_br_invoices_ext_r12.header_gdf_attribute4%TYPE,
         header_gdf_attribute5            xxar_br_invoices_ext_r12.header_gdf_attribute5%TYPE,
         header_gdf_attribute6            xxar_br_invoices_ext_r12.header_gdf_attribute6%TYPE,
         header_gdf_attribute7            xxar_br_invoices_ext_r12.header_gdf_attribute7%TYPE,
         header_gdf_attribute8            xxar_br_invoices_ext_r12.header_gdf_attribute8%TYPE,
         header_gdf_attribute9            xxar_br_invoices_ext_r12.header_gdf_attribute9%TYPE,
         header_gdf_attribute10           xxar_br_invoices_ext_r12.header_gdf_attribute10%TYPE,
         header_gdf_attribute11           xxar_br_invoices_ext_r12.header_gdf_attribute11%TYPE,
         header_gdf_attribute12           xxar_br_invoices_ext_r12.header_gdf_attribute12%TYPE,
         header_gdf_attribute13           xxar_br_invoices_ext_r12.header_gdf_attribute13%TYPE,
         header_gdf_attribute14           xxar_br_invoices_ext_r12.header_gdf_attribute14%TYPE,
         header_gdf_attribute15           xxar_br_invoices_ext_r12.header_gdf_attribute15%TYPE,
         header_gdf_attribute16           xxar_br_invoices_ext_r12.header_gdf_attribute16%TYPE,
         header_gdf_attribute17           xxar_br_invoices_ext_r12.header_gdf_attribute17%TYPE,
         header_gdf_attribute18           xxar_br_invoices_ext_r12.header_gdf_attribute18%TYPE,
         header_gdf_attribute19           xxar_br_invoices_ext_r12.header_gdf_attribute19%TYPE,
         header_gdf_attribute20           xxar_br_invoices_ext_r12.header_gdf_attribute20%TYPE,
         header_gdf_attribute21           xxar_br_invoices_ext_r12.header_gdf_attribute21%TYPE,
         header_gdf_attribute22           xxar_br_invoices_ext_r12.header_gdf_attribute22%TYPE,
         header_gdf_attribute23           xxar_br_invoices_ext_r12.header_gdf_attribute23%TYPE,
         header_gdf_attribute24           xxar_br_invoices_ext_r12.header_gdf_attribute24%TYPE,
         header_gdf_attribute25           xxar_br_invoices_ext_r12.header_gdf_attribute25%TYPE,
         header_gdf_attribute26           xxar_br_invoices_ext_r12.header_gdf_attribute26%TYPE,
         header_gdf_attribute27           xxar_br_invoices_ext_r12.header_gdf_attribute27%TYPE,
         header_gdf_attribute28           xxar_br_invoices_ext_r12.header_gdf_attribute28%TYPE,
         header_gdf_attribute29           xxar_br_invoices_ext_r12.header_gdf_attribute29%TYPE,
         header_gdf_attribute30           xxar_br_invoices_ext_r12.header_gdf_attribute30%TYPE,
         line_gdf_attr_category           xxar_br_invoices_ext_r12.line_gdf_attr_category%TYPE,
         line_gdf_attribute1              xxar_br_invoices_ext_r12.line_gdf_attribute1%TYPE,
         line_gdf_attribute2              xxar_br_invoices_ext_r12.line_gdf_attribute2%TYPE,
         line_gdf_attribute3              xxar_br_invoices_ext_r12.line_gdf_attribute3%TYPE,
         line_gdf_attribute4              xxar_br_invoices_ext_r12.line_gdf_attribute4%TYPE,
         line_gdf_attribute5              xxar_br_invoices_ext_r12.line_gdf_attribute5%TYPE,
         line_gdf_attribute6              xxar_br_invoices_ext_r12.line_gdf_attribute6%TYPE,
         line_gdf_attribute7              xxar_br_invoices_ext_r12.line_gdf_attribute7%TYPE,
         line_gdf_attribute8              xxar_br_invoices_ext_r12.line_gdf_attribute8%TYPE,
         line_gdf_attribute9              xxar_br_invoices_ext_r12.line_gdf_attribute9%TYPE,
         line_gdf_attribute10             xxar_br_invoices_ext_r12.line_gdf_attribute10%TYPE,
         line_gdf_attribute11             xxar_br_invoices_ext_r12.line_gdf_attribute11%TYPE,
         line_gdf_attribute12             xxar_br_invoices_ext_r12.line_gdf_attribute12%TYPE,
         line_gdf_attribute13             xxar_br_invoices_ext_r12.line_gdf_attribute13%TYPE,
         line_gdf_attribute14             xxar_br_invoices_ext_r12.line_gdf_attribute14%TYPE,
         line_gdf_attribute15             xxar_br_invoices_ext_r12.line_gdf_attribute15%TYPE,
         line_gdf_attribute16             xxar_br_invoices_ext_r12.line_gdf_attribute16%TYPE,
         line_gdf_attribute17             xxar_br_invoices_ext_r12.line_gdf_attribute17%TYPE,
         line_gdf_attribute18             xxar_br_invoices_ext_r12.line_gdf_attribute18%TYPE,
         line_gdf_attribute19             xxar_br_invoices_ext_r12.line_gdf_attribute19%TYPE,
         line_gdf_attribute20             xxar_br_invoices_ext_r12.line_gdf_attribute20%TYPE,
         line_amount                      xxar_br_invoices_ext_r12.line_amount%TYPE,
         reason_code                      xxar_br_invoices_ext_r12.reason_code%TYPE,
         reason_code_meaning              xxar_br_invoices_ext_r12.reason_code_meaning%TYPE,
         REFERENCE                        xxar_br_invoices_ext_r12.REFERENCE%TYPE,
         comments                         xxar_br_invoices_ext_r12.comments%TYPE,
         creation_date                    xxar_br_invoices_ext_r12.creation_date%TYPE,
         created_by                       xxar_br_invoices_ext_r12.created_by%TYPE,
         last_updated_date                xxar_br_invoices_ext_r12.last_update_date%TYPE,
         last_updated_by                  xxar_br_invoices_ext_r12.last_updated_by%TYPE,
         last_update_login                xxar_br_invoices_ext_r12.last_update_login%TYPE,
         program_application_id           xxar_br_invoices_ext_r12.program_application_id%TYPE,
         program_id                       xxar_br_invoices_ext_r12.program_id%TYPE,
         program_update_date              xxar_br_invoices_ext_r12.program_update_date%TYPE,
         request_id                       xxar_br_invoices_ext_r12.request_id%TYPE,
         process_flag                     xxar_br_invoices_ext_r12.process_flag%TYPE,
         ERROR_TYPE                       xxar_br_invoices_ext_r12.ERROR_TYPE%TYPE,
         attribute_category               xxar_br_invoices_ext_r12.attribute_category%TYPE,
         attribute1                       xxar_br_invoices_ext_r12.attribute1%TYPE,
         attribute2                       xxar_br_invoices_ext_r12.attribute2%TYPE,
         attribute3                       xxar_br_invoices_ext_r12.attribute3%TYPE,
         attribute4                       xxar_br_invoices_ext_r12.attribute4%TYPE,
         attribute5                       xxar_br_invoices_ext_r12.attribute5%TYPE,
         attribute6                       xxar_br_invoices_ext_r12.attribute6%TYPE,
         attribute7                       xxar_br_invoices_ext_r12.attribute7%TYPE,
         attribute8                       xxar_br_invoices_ext_r12.attribute8%TYPE,
         attribute9                       xxar_br_invoices_ext_r12.attribute9%TYPE,
         attribute10                      xxar_br_invoices_ext_r12.attribute10%TYPE,
         attribute11                      xxar_br_invoices_ext_r12.attribute11%TYPE,
         attribute12                      xxar_br_invoices_ext_r12.attribute12%TYPE,
         attribute13                      xxar_br_invoices_ext_r12.attribute13%TYPE,
         attribute14                      xxar_br_invoices_ext_r12.attribute14%TYPE,
         attribute15                      xxar_br_invoices_ext_r12.attribute15%TYPE,
         vat_tax_id                       xxar_br_invoices_ext_r12.vat_tax_id%TYPE,
         sales_tax_id                     xxar_br_invoices_ext_r12.sales_tax_id%TYPE,
         uom_name                         xxar_br_invoices_ext_r12.uom_name%TYPE,
         ussgl_transaction_code_context   xxar_br_invoices_ext_r12.ussgl_transaction_code_context%TYPE,
         internal_notes                   xxar_br_invoices_ext_r12.internal_notes%TYPE,
         ship_date_actual                 xxar_br_invoices_ext_r12.ship_date_actual%TYPE,
         fob_point                        xxar_br_invoices_ext_r12.fob_point%TYPE,
         ship_via                         xxar_br_invoices_ext_r12.ship_via%TYPE,
         waybill_number                   xxar_br_invoices_ext_r12.waybill_number%TYPE,
         sales_order_line                 xxar_br_invoices_ext_r12.sales_order_line%TYPE,
         sales_order_source               xxar_br_invoices_ext_r12.sales_order_source%TYPE,
         sales_order_revision             xxar_br_invoices_ext_r12.sales_order_revision%TYPE,
         purchase_order_revision          xxar_br_invoices_ext_r12.purchase_order_revision%TYPE,
         purchase_order_date              xxar_br_invoices_ext_r12.purchase_order_date%TYPE,
         agreement_name                   xxar_br_invoices_ext_r12.agreement_name%TYPE,
         agreement_id                     xxar_br_invoices_ext_r12.agreement_id%TYPE,
         memo_line_name                   xxar_br_invoices_ext_r12.memo_line_name%TYPE,
         quantity                         xxar_br_invoices_ext_r12.quantity%TYPE,
         quantity_ordered                 xxar_br_invoices_ext_r12.quantity_ordered%TYPE,
         unit_selling_price               xxar_br_invoices_ext_r12.unit_selling_price%TYPE,
         unit_standard_price              xxar_br_invoices_ext_r12.unit_standard_price%TYPE,
         amount_includes_tax_flag         VARCHAR2 (1),
         taxable_flag                     VARCHAR2 (1),
         leg_req_id                       xxar_br_invoices_ext_r12.request_id%TYPE,
         func_curr                        VARCHAR2 (30),
         ledger_id                        NUMBER,
         invoicing_rule_id                NUMBER,
         br_openrec_ledger                xxar_br_invoices_ext_r12.leg_br_openrec_ledger%TYPE,
         br_openrec_org                   xxar_br_invoices_ext_r12.leg_br_openrec_org%TYPE,
         br_or_openreceipt_num            xxar_br_invoices_ext_r12.leg_br_or_openreceipt_num%TYPE,
		 br_amount						  xxar_br_invoices_ext_r12.leg_br_amount%TYPE,--added for v1.13
         br_or_rec_maturity_date          xxar_br_invoices_ext_r12.leg_br_or_rec_maturity_date%TYPE,
         br_or_rec_issue_date             xxar_br_invoices_ext_r12.leg_br_or_rec_issue_date%TYPE
      );

      TYPE leg_invoice_tbl IS TABLE OF leg_br_invoice_rec
         INDEX BY BINARY_INTEGER;

      l_leg_invoice_tbl   leg_invoice_tbl;
      l_err_record        NUMBER;

      CURSOR cur_leg_invoices
      IS
         SELECT xil.interface_txn_id, xil.batch_id, xil.load_id,
                xil.run_sequence_id, xil.leg_batch_source_name,
                xil.leg_customer_number, xil.leg_bill_to_address,
                xil.leg_ship_to_address, xil.leg_currency_code,
                xil.leg_cust_trx_type_name, xil.leg_line_amount,
                xil.leg_trx_date, xil.leg_tax_code, xil.leg_tax_rate,
                xil.leg_conversion_date, xil.leg_conversion_rate,
                xil.leg_term_name, xil.leg_set_of_books_name,
                xil.leg_operating_unit, xil.leg_header_attribute_category,
                xil.leg_header_attribute1, NULL, NULL,
                xil.leg_header_attribute4, NULL, NULL, NULL,
                xil.leg_header_attribute8, NULL, NULL, NULL, NULL, NULL, NULL,
                NULL, xil.leg_reference_line_id, xil.leg_purchase_order,
                xil.leg_trx_number, xil.leg_line_number, xil.leg_comments,
                xil.leg_due_date, xil.leg_inv_amount_due_original,
                xil.leg_inv_amount_due_remaining, xil.leg_line_type,
                xil.leg_interface_line_context,
                xil.leg_interface_line_attribute1,
                xil.leg_interface_line_attribute2, NULL, NULL, NULL, NULL,
                NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
                xil.leg_customer_trx_id, xil.trx_type,
                xil.leg_cust_trx_line_id, xil.leg_link_to_cust_trx_line_id,
                xil.leg_header_gdf_attr_category,
                xil.leg_header_gdf_attribute1, xil.leg_header_gdf_attribute2,
                xil.leg_header_gdf_attribute3, xil.leg_header_gdf_attribute4,
                xil.leg_header_gdf_attribute5, xil.leg_header_gdf_attribute6,
                xil.leg_header_gdf_attribute7, xil.leg_header_gdf_attribute8,
                xil.leg_header_gdf_attribute9, xil.leg_header_gdf_attribute10,
                xil.leg_header_gdf_attribute11,
                xil.leg_header_gdf_attribute12,
                xil.leg_header_gdf_attribute13,
                xil.leg_header_gdf_attribute14,
                xil.leg_header_gdf_attribute15,
                xil.leg_header_gdf_attribute16,
                xil.leg_header_gdf_attribute17,
                xil.leg_header_gdf_attribute18,
                xil.leg_header_gdf_attribute19,
                xil.leg_header_gdf_attribute20,
                xil.leg_header_gdf_attribute21,
                xil.leg_header_gdf_attribute22,
                xil.leg_header_gdf_attribute23,
                xil.leg_header_gdf_attribute24,
                xil.leg_header_gdf_attribute25,
                xil.leg_header_gdf_attribute26,
                xil.leg_header_gdf_attribute27,
                xil.leg_header_gdf_attribute28,
                xil.leg_header_gdf_attribute29,
                xil.leg_header_gdf_attribute30,
                xil.leg_line_gdf_attr_category, xil.leg_line_gdf_attribute1,
                xil.leg_line_gdf_attribute2, xil.leg_line_gdf_attribute3,
                xil.leg_line_gdf_attribute4, xil.leg_line_gdf_attribute5,
                xil.leg_line_gdf_attribute6, xil.leg_line_gdf_attribute7,
                xil.leg_line_gdf_attribute8, xil.leg_line_gdf_attribute9,
                xil.leg_line_gdf_attribute10, xil.leg_line_gdf_attribute11,
                xil.leg_line_gdf_attribute12, xil.leg_line_gdf_attribute13,
                xil.leg_line_gdf_attribute14, xil.leg_line_gdf_attribute15,
                xil.leg_line_gdf_attribute16, xil.leg_line_gdf_attribute17,
                xil.leg_line_gdf_attribute18, xil.leg_line_gdf_attribute19,
                xil.leg_line_gdf_attribute20, xil.leg_reason_code,
                xil.leg_source_system, xil.leg_quantity,
                xil.leg_quantity_ordered, xil.leg_unit_selling_price,
                xil.leg_unit_standard_price, xil.leg_ship_date_actual,
                xil.leg_fob_point, xil.leg_ship_via, xil.leg_waybill_number,
                xil.leg_sales_order_line, xil.leg_sales_order,
                xil.leg_gl_date, xil.leg_sales_order_date,
                xil.leg_sales_order_source, xil.leg_sales_order_revision,
                xil.leg_purchase_order_revision, xil.leg_purchase_order_date,
                xil.leg_agreement_name, xil.leg_agreement_id,
                xil.leg_memo_line_name, xil.leg_internal_notes,
                xil.leg_ussgl_trx_code_context, xil.leg_uom_name,
                xil.leg_vat_tax_name, xil.leg_sales_tax_name,
                xil.leg_request_id, xil.leg_seq_num, xil.leg_process_flag,
                xil.currency_code, xil.cust_trx_type_name, xil.line_type,
                xil.set_of_books_id, xil.trx_number, xil.line_number,
                xil.gl_date, NULL memo_line_id, xil.description, NULL, NULL,
                NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
                NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
                NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
                xil.system_bill_customer_id, xil.system_bill_customer_ref,
                xil.system_bill_address_id, xil.system_bill_address_ref,
                xil.system_bill_contact_id, xil.system_ship_customer_id,
                xil.system_ship_customer_ref, xil.system_ship_address_id,
                xil.system_ship_address_ref, xil.system_ship_contact_id,
                xil.system_sold_customer_id, xil.system_sold_customer_ref,
                xil.term_name, xil.ou_name, xil.conversion_type,
                xil.conversion_date, xil.conversion_rate, xil.trx_date,
                xil.batch_source_name, xil.purchase_order,
                xil.sales_order_date, xil.sales_order, NULL reference_line_id,
                xil.term_id, xil.org_id, xil.transaction_type_id,
                xil.tax_regime_code, xil.tax_code, xil.tax,
                xil.tax_status_code, xil.tax_rate_code,
                xil.tax_jurisdiction_code, xil.tax_rate,
                xil.adjustment_amount, xil.inv_amount_due_original,
                xil.inv_amount_due_remaining, NULL, NULL, NULL, NULL, NULL,
                NULL, NULL, NULL, NULL, NULL, xil.link_to_line_attribute10,
                xil.link_to_line_attribute11, xil.link_to_line_attribute12,
                xil.link_to_line_attribute13, xil.link_to_line_attribute14,
                xil.link_to_line_attribute15, xil.header_gdf_attr_category,
                xil.header_gdf_attribute1, xil.header_gdf_attribute2,
                xil.header_gdf_attribute3, xil.header_gdf_attribute4,
                xil.header_gdf_attribute5, xil.header_gdf_attribute6,
                xil.header_gdf_attribute7, xil.header_gdf_attribute8,
                xil.header_gdf_attribute9, xil.header_gdf_attribute10,
                xil.header_gdf_attribute11, xil.header_gdf_attribute12,
                xil.header_gdf_attribute13, xil.header_gdf_attribute14,
                xil.header_gdf_attribute15, xil.header_gdf_attribute16,
                xil.header_gdf_attribute17, xil.header_gdf_attribute18,
                xil.header_gdf_attribute19, xil.header_gdf_attribute20,
                xil.header_gdf_attribute21, xil.header_gdf_attribute22,
                xil.header_gdf_attribute23, xil.header_gdf_attribute24,
                xil.header_gdf_attribute25, xil.header_gdf_attribute26,
                xil.header_gdf_attribute27, xil.header_gdf_attribute28,
                xil.header_gdf_attribute29, xil.header_gdf_attribute30,
                xil.line_gdf_attr_category, xil.line_gdf_attribute1,
                xil.line_gdf_attribute2, xil.line_gdf_attribute3,
                xil.line_gdf_attribute4, xil.line_gdf_attribute5,
                xil.line_gdf_attribute6, xil.line_gdf_attribute7,
                xil.line_gdf_attribute8, xil.line_gdf_attribute9,
                xil.line_gdf_attribute10, xil.line_gdf_attribute11,
                xil.line_gdf_attribute12, xil.line_gdf_attribute13,
                xil.line_gdf_attribute14, xil.line_gdf_attribute15,
                xil.line_gdf_attribute16, xil.line_gdf_attribute17,
                xil.line_gdf_attribute18, xil.line_gdf_attribute19,
                xil.line_gdf_attribute20, xil.line_amount, xil.reason_code,
                xil.reason_code_meaning, xil.REFERENCE, xil.comments,
                SYSDATE creation_date, g_user_id created_by,
                SYSDATE last_update_date, g_user_id last_updated_by,
                g_login_id last_update_login, xil.program_application_id,
                xil.program_id, xil.program_update_date, NULL request_id,
                xil.process_flag, xil.ERROR_TYPE, xil.attribute_category,
                xil.attribute1, xil.attribute2, xil.attribute3,
                xil.attribute4, xil.attribute5, xil.attribute6,
                xil.attribute7, xil.attribute8, xil.attribute9,
                xil.attribute10, xil.attribute11, xil.attribute12,
                xil.attribute13, xil.attribute14, xil.attribute15,
                xil.vat_tax_id, xil.sales_tax_id, xil.uom_name,
                xil.ussgl_transaction_code_context, xil.internal_notes,
                xil.ship_date_actual, xil.fob_point, xil.ship_via,
                xil.waybill_number, xil.sales_order_line,
                xil.sales_order_source, xil.sales_order_revision,
                xil.purchase_order_revision, xil.purchase_order_date,
                xil.agreement_name, xil.agreement_id, xil.memo_line_name,
                xil.quantity, xil.quantity_ordered, xil.unit_selling_price,
                xil.unit_standard_price, NULL amount_includes_tax_flag,
                NULL taxable_flag, xil.request_id leg_req_id, NULL func_curr,
                NULL ledger_id, NULL invoicing_rule_id, leg_br_openrec_ledger,
                leg_br_openrec_org, leg_br_or_openreceipt_num,
				leg_br_amount,--added for v1.13
                leg_br_or_rec_maturity_date, leg_br_or_rec_issue_date
           FROM xxar_br_invoices_ext_r12 xil
          WHERE xil.leg_process_flag = 'V'
            AND NOT EXISTS (SELECT 1
                              FROM xxconv.xxar_br_invoices_stg xis
                             WHERE xis.interface_txn_id = xil.interface_txn_id);
   BEGIN
      pov_ret_stats := 'S';
      pov_err_msg := NULL;
      g_total_count := 0;
      g_failed_count := 0;

      --Open cursor to extract data from extraction staging table
      OPEN cur_leg_invoices;

      LOOP
         print_log_message ('Loading invoices lines');
         l_leg_invoice_tbl.DELETE;

         FETCH cur_leg_invoices
         BULK COLLECT INTO l_leg_invoice_tbl LIMIT 5000;

         --limit size of Bulk Collect

         -- Get Total Count
         g_total_count := g_total_count + l_leg_invoice_tbl.COUNT;
         EXIT WHEN l_leg_invoice_tbl.COUNT = 0;

         BEGIN
            -- Bulk Insert into Conversion table
            FORALL indx IN 1 .. l_leg_invoice_tbl.COUNT SAVE EXCEPTIONS
               INSERT INTO xxconv.xxar_br_invoices_stg
                    VALUES l_leg_invoice_tbl (indx);
         EXCEPTION
            WHEN OTHERS
            THEN
               print_log_message
                      ('Errors encountered while loading invoice lines data ');

               FOR l_indx_exp IN 1 .. SQL%BULK_EXCEPTIONS.COUNT
               LOOP
                  l_err_record :=
                     l_leg_invoice_tbl
                              (SQL%BULK_EXCEPTIONS (l_indx_exp).ERROR_INDEX).interface_txn_id;
                  pov_ret_stats := 'E';
                  fnd_file.put_line
                     (fnd_file.LOG,
                         'Record sequence (interface_txn_id) : '
                      || l_leg_invoice_tbl
                               (SQL%BULK_EXCEPTIONS (l_indx_exp).ERROR_INDEX).interface_txn_id
                     );
                  fnd_file.put_line
                       (fnd_file.LOG,
                           'Error Message : '
                        || SQLERRM
                               (-SQL%BULK_EXCEPTIONS (l_indx_exp).ERROR_CODE)
                       );

                  -- Updating Leg_process_flag to 'E' for failed records
                  UPDATE xxar_br_invoices_ext_r12 xil
                     SET xil.leg_process_flag = 'E',
                         xil.last_update_date = SYSDATE,
                         xil.last_updated_by = g_last_updated_by,
                         xil.last_update_login = g_last_update_login,
                         xil.program_id = g_conc_program_id,
                         xil.program_application_id = g_prog_appl_id,
                         xil.program_update_date = SYSDATE
                   WHERE xil.interface_txn_id = l_err_record
                     AND xil.leg_process_flag = 'V';

                  g_failed_count := g_failed_count + SQL%ROWCOUNT;
               END LOOP;
         END;
      END LOOP;

      CLOSE cur_leg_invoices;

      COMMIT;

      IF g_failed_count > 0
      THEN
         g_retcode := 1;
      END IF;

      g_loaded_count := g_total_count - g_failed_count;

      -- If records successfully posted to conversion staging table
      IF g_total_count > 0
      THEN
         print_log_message
            ('Updating process flag (leg_process_flag) in extraction table for processed records '
            );

         UPDATE xxar_br_invoices_ext_r12 xil
            SET xil.leg_process_flag = 'P',
                xil.last_update_date = SYSDATE,
                xil.last_updated_by = g_last_updated_by,
                xil.last_update_login = g_last_update_login,
                xil.program_id = g_conc_program_id,
                xil.program_application_id = g_prog_appl_id,
                xil.program_update_date = SYSDATE
          WHERE xil.leg_process_flag = 'V'
            AND EXISTS (SELECT 1
                          FROM xxconv.xxar_br_invoices_stg xis
                         WHERE xis.interface_txn_id = xil.interface_txn_id);

         COMMIT;
      -- Either no data to load from extraction table or records already exist in R12 staging table and hence not loaded
      ELSE
         print_log_message
            ('Either no data found for loading from extraction table or records already exist in R12 staging table and hence not loaded '
            );

         UPDATE xxar_br_invoices_ext_r12 xil
            SET xil.leg_process_flag = 'E',
                xil.last_update_date = SYSDATE,
                xil.last_updated_by = g_last_updated_by,
                xil.last_update_login = g_last_update_login,
                xil.program_id = g_conc_program_id,
                xil.program_application_id = g_prog_appl_id,
                xil.program_update_date = SYSDATE
          WHERE xil.leg_process_flag = 'V'
            AND EXISTS (SELECT 1
                          FROM xxconv.xxar_br_invoices_stg xis
                         WHERE xis.interface_txn_id = xil.interface_txn_id);

         g_retcode := 1;
         COMMIT;
      END IF;
   EXCEPTION
      WHEN OTHERS
      THEN
         g_retcode := 2;
         pov_ret_stats := 'E';
         pov_err_msg :=
               'ERROR : Error in load_invoice procedure'
            || SUBSTR (SQLERRM, 1, 150);
         ROLLBACK;
   END load_invoice;

 -- ========================
-- Procedure: LOAD_DISTRIBUTION
-- =============================================================================
--   This procedure is used to load distribution lines data from extraction staging table
--   to conversion staging table when program is run in LOAD mode
-- =============================================================================
   PROCEDURE load_distribution (
      pov_ret_stats   OUT NOCOPY   VARCHAR2,
      pov_err_msg     OUT NOCOPY   VARCHAR2
   )
   IS
      TYPE leg_br_invoice_dist_rec IS RECORD (
         interface_txn_id                 xxar_br_invoices_dist_ext_r12.interface_txn_id%TYPE,
         batch_id                         xxar_br_invoices_dist_ext_r12.batch_id%TYPE,
         load_id                          xxar_br_invoices_dist_ext_r12.load_id%TYPE,
         run_sequence_id                  xxar_br_invoices_dist_ext_r12.run_sequence_id%TYPE,
         leg_customer_trx_id              xxar_br_invoices_dist_ext_r12.leg_customer_trx_id%TYPE,
         leg_cust_trx_line_id             xxar_br_invoices_dist_ext_r12.leg_cust_trx_line_id%TYPE,
         leg_cust_trx_line_gl_dist_id     xxar_br_invoices_dist_ext_r12.leg_cust_trx_line_gl_dist_id%TYPE,
         leg_percent                      xxar_br_invoices_dist_ext_r12.leg_percent%TYPE,
         leg_account_class                xxar_br_invoices_dist_ext_r12.leg_account_class%TYPE,
         leg_dist_segment1                xxar_br_invoices_dist_ext_r12.leg_dist_segment1%TYPE,
         leg_dist_segment2                xxar_br_invoices_dist_ext_r12.leg_dist_segment2%TYPE,
         leg_dist_segment3                xxar_br_invoices_dist_ext_r12.leg_dist_segment3%TYPE,
         leg_dist_segment4                xxar_br_invoices_dist_ext_r12.leg_dist_segment4%TYPE,
         leg_dist_segment5                xxar_br_invoices_dist_ext_r12.leg_dist_segment5%TYPE,
         leg_dist_segment6                xxar_br_invoices_dist_ext_r12.leg_dist_segment6%TYPE,
         leg_dist_segment7                xxar_br_invoices_dist_ext_r12.leg_dist_segment7%TYPE,
         leg_org_name                     xxar_br_invoices_dist_ext_r12.leg_org_name%TYPE,
         leg_accounted_amount             xxar_br_invoices_dist_ext_r12.leg_accounted_amount%TYPE,
         leg_interface_line_context       xxar_br_invoices_dist_ext_r12.leg_interface_line_context%TYPE,
         leg_interface_line_attribute1    xxar_br_invoices_dist_ext_r12.leg_interface_line_attribute1%TYPE,
         leg_interface_line_attribute2    xxar_br_invoices_dist_ext_r12.leg_interface_line_attribute2%TYPE,
         leg_interface_line_attribute3    xxar_br_invoices_dist_ext_r12.leg_interface_line_attribute3%TYPE,
         leg_interface_line_attribute4    xxar_br_invoices_dist_ext_r12.leg_interface_line_attribute4%TYPE,
         leg_interface_line_attribute5    xxar_br_invoices_dist_ext_r12.leg_interface_line_attribute5%TYPE,
         leg_interface_line_attribute6    xxar_br_invoices_dist_ext_r12.leg_interface_line_attribute6%TYPE,
         leg_interface_line_attribute7    xxar_br_invoices_dist_ext_r12.leg_interface_line_attribute7%TYPE,
         leg_interface_line_attribute8    xxar_br_invoices_dist_ext_r12.leg_interface_line_attribute8%TYPE,
         leg_interface_line_attribute9    xxar_br_invoices_dist_ext_r12.leg_interface_line_attribute9%TYPE,
         leg_interface_line_attribute10   xxar_br_invoices_dist_ext_r12.leg_interface_line_attribute10%TYPE,
         leg_interface_line_attribute11   xxar_br_invoices_dist_ext_r12.leg_interface_line_attribute11%TYPE,
         leg_interface_line_attribute12   xxar_br_invoices_dist_ext_r12.leg_interface_line_attribute12%TYPE,
         leg_interface_line_attribute13   xxar_br_invoices_dist_ext_r12.leg_interface_line_attribute13%TYPE,
         leg_interface_line_attribute14   xxar_br_invoices_dist_ext_r12.leg_interface_line_attribute14%TYPE,
         leg_interface_line_attribute15   xxar_br_invoices_dist_ext_r12.leg_interface_line_attribute15%TYPE,
         interface_line_context           xxar_br_invoices_dist_ext_r12.interface_line_context%TYPE,
         interface_line_attribute1        xxar_br_invoices_dist_ext_r12.interface_line_attribute1%TYPE,
         interface_line_attribute2        xxar_br_invoices_dist_ext_r12.interface_line_attribute2%TYPE,
         interface_line_attribute3        xxar_br_invoices_dist_ext_r12.interface_line_attribute3%TYPE,
         interface_line_attribute4        xxar_br_invoices_dist_ext_r12.interface_line_attribute4%TYPE,
         interface_line_attribute5        xxar_br_invoices_dist_ext_r12.interface_line_attribute5%TYPE,
         interface_line_attribute6        xxar_br_invoices_dist_ext_r12.interface_line_attribute6%TYPE,
         interface_line_attribute7        xxar_br_invoices_dist_ext_r12.interface_line_attribute7%TYPE,
         interface_line_attribute8        xxar_br_invoices_dist_ext_r12.interface_line_attribute8%TYPE,
         interface_line_attribute9        xxar_br_invoices_dist_ext_r12.interface_line_attribute9%TYPE,
         interface_line_attribute10       xxar_br_invoices_dist_ext_r12.interface_line_attribute10%TYPE,
         interface_line_attribute11       xxar_br_invoices_dist_ext_r12.interface_line_attribute11%TYPE,
         interface_line_attribute12       xxar_br_invoices_dist_ext_r12.interface_line_attribute12%TYPE,
         interface_line_attribute13       xxar_br_invoices_dist_ext_r12.interface_line_attribute13%TYPE,
         interface_line_attribute14       xxar_br_invoices_dist_ext_r12.interface_line_attribute14%TYPE,
         interface_line_attribute15       xxar_br_invoices_dist_ext_r12.interface_line_attribute15%TYPE,
         dist_segment1                    xxar_br_invoices_dist_ext_r12.dist_segment1%TYPE,
         dist_segment2                    xxar_br_invoices_dist_ext_r12.dist_segment2%TYPE,
         dist_segment3                    xxar_br_invoices_dist_ext_r12.dist_segment3%TYPE,
         dist_segment4                    xxar_br_invoices_dist_ext_r12.dist_segment4%TYPE,
         dist_segment5                    xxar_br_invoices_dist_ext_r12.dist_segment5%TYPE,
         dist_segment6                    xxar_br_invoices_dist_ext_r12.dist_segment6%TYPE,
         dist_segment7                    xxar_br_invoices_dist_ext_r12.dist_segment7%TYPE,
         dist_segment8                    xxar_br_invoices_dist_ext_r12.dist_segment8%TYPE,
         dist_segment9                    xxar_br_invoices_dist_ext_r12.dist_segment9%TYPE,
         dist_segment10                   xxar_br_invoices_dist_ext_r12.dist_segment10%TYPE,
         accounted_amount                 xxar_br_invoices_dist_ext_r12.accounted_amount%TYPE,
         code_combination_id              xxar_br_invoices_dist_ext_r12.code_combination_id%TYPE,
         account_class                    xxar_br_invoices_dist_ext_r12.account_class%TYPE,
         PERCENT                          xxar_br_invoices_dist_ext_r12.PERCENT%TYPE,
         org_id                           xxar_br_invoices_dist_ext_r12.org_id%TYPE,
         creation_date                    xxar_br_invoices_dist_ext_r12.creation_date%TYPE,
         created_by                       xxar_br_invoices_dist_ext_r12.created_by%TYPE,
         last_update_date                 xxar_br_invoices_dist_ext_r12.last_update_date%TYPE,
         last_updated_by                  xxar_br_invoices_dist_ext_r12.last_updated_by%TYPE,
         last_update_login                xxar_br_invoices_dist_ext_r12.last_update_login%TYPE,
         program_application_id           xxar_br_invoices_dist_ext_r12.program_application_id%TYPE,
         program_id                       xxar_br_invoices_dist_ext_r12.program_id%TYPE,
         program_update_date              xxar_br_invoices_dist_ext_r12.program_update_date%TYPE,
         request_id                       xxar_br_invoices_dist_ext_r12.request_id%TYPE,
         process_flag                     xxar_br_invoices_dist_ext_r12.process_flag%TYPE,
         ERROR_TYPE                       xxar_br_invoices_dist_ext_r12.ERROR_TYPE%TYPE,
         attribute_category               xxar_br_invoices_dist_ext_r12.attribute_category%TYPE,
         attribute1                       xxar_br_invoices_dist_ext_r12.attribute1%TYPE,
         attribute2                       xxar_br_invoices_dist_ext_r12.attribute2%TYPE,
         attribute3                       xxar_br_invoices_dist_ext_r12.attribute3%TYPE,
         attribute4                       xxar_br_invoices_dist_ext_r12.attribute4%TYPE,
         attribute5                       xxar_br_invoices_dist_ext_r12.attribute5%TYPE,
         attribute6                       xxar_br_invoices_dist_ext_r12.attribute6%TYPE,
         attribute7                       xxar_br_invoices_dist_ext_r12.attribute7%TYPE,
         attribute8                       xxar_br_invoices_dist_ext_r12.attribute8%TYPE,
         attribute9                       xxar_br_invoices_dist_ext_r12.attribute9%TYPE,
         attribute10                      xxar_br_invoices_dist_ext_r12.attribute10%TYPE,
         attribute11                      xxar_br_invoices_dist_ext_r12.attribute11%TYPE,
         attribute12                      xxar_br_invoices_dist_ext_r12.attribute12%TYPE,
         attribute13                      xxar_br_invoices_dist_ext_r12.attribute13%TYPE,
         attribute14                      xxar_br_invoices_dist_ext_r12.attribute14%TYPE,
         attribute15                      xxar_br_invoices_dist_ext_r12.attribute15%TYPE,
         leg_source_system                xxar_br_invoices_dist_ext_r12.leg_source_system%TYPE,
         leg_request_id                   xxar_br_invoices_dist_ext_r12.leg_request_id%TYPE,
         leg_seq_num                      xxar_br_invoices_dist_ext_r12.leg_seq_num%TYPE,
         leg_process_flag                 xxar_br_invoices_dist_ext_r12.leg_process_flag%TYPE
      );

      TYPE leg_dist_tbl IS TABLE OF leg_br_invoice_dist_rec
         INDEX BY BINARY_INTEGER;

      l_leg_dist_tbl   leg_dist_tbl;
      l_err_record     NUMBER;

      CURSOR cur_leg_dist
      IS
         SELECT interface_txn_id, batch_id, load_id, run_sequence_id,
                leg_customer_trx_id, leg_cust_trx_line_id,
                leg_cust_trx_line_gl_dist_id, leg_percent, leg_account_class,
                leg_dist_segment1, leg_dist_segment2, leg_dist_segment3,
                leg_dist_segment4, leg_dist_segment5, leg_dist_segment6,
                leg_dist_segment7, leg_org_name, leg_accounted_amount,
                leg_interface_line_context, leg_interface_line_attribute1,
                leg_interface_line_attribute2, leg_interface_line_attribute3,
                leg_interface_line_attribute4, leg_interface_line_attribute5,
                leg_interface_line_attribute6, leg_interface_line_attribute7,
                leg_interface_line_attribute8, leg_interface_line_attribute9,
                leg_interface_line_attribute10,
                leg_interface_line_attribute11,
                leg_interface_line_attribute12,
                leg_interface_line_attribute13,
                leg_interface_line_attribute14,
                leg_interface_line_attribute15, interface_line_context,
                interface_line_attribute1, interface_line_attribute2,
                interface_line_attribute3, interface_line_attribute4,
                interface_line_attribute5, interface_line_attribute6,
                interface_line_attribute7, interface_line_attribute8,
                interface_line_attribute9, interface_line_attribute10,
                interface_line_attribute11, interface_line_attribute12,
                interface_line_attribute13, interface_line_attribute14,
                interface_line_attribute15, dist_segment1, dist_segment2,
                dist_segment3, dist_segment4, dist_segment5, dist_segment6,
                dist_segment7, dist_segment8, dist_segment9, dist_segment10,
                accounted_amount, code_combination_id, account_class, PERCENT,
                org_id, SYSDATE creation_date, g_user_id created_by,
                SYSDATE last_update_date, g_user_id last_updated_by,
                g_login_id last_update_login, program_application_id,
                program_id, program_update_date, request_id, process_flag,
                ERROR_TYPE, attribute_category, attribute1, attribute2,
                attribute3, attribute4, attribute5, attribute6, attribute7,
                attribute8, attribute9, attribute10, attribute11, attribute12,
                attribute13, attribute14, attribute15, leg_source_system,
                leg_request_id, leg_seq_num, leg_process_flag
           FROM xxar_br_invoices_dist_ext_r12 xil
          WHERE xil.leg_process_flag = 'V'
            AND xil.leg_account_class <> 'ROUND'                 --performance
            AND xil.leg_account_class <> 'UNEARN'
            AND NOT EXISTS (SELECT 1
                              FROM xxconv.xxar_br_invoices_dist_stg xis
                             WHERE xis.interface_txn_id = xil.interface_txn_id);
   BEGIN
      pov_ret_stats := 'S';
      pov_err_msg := NULL;
      g_total_dist_count := 0;
      g_failed_dist_count := 0;

      --Open cursor to extract data from extraction staging table for distributions
      OPEN cur_leg_dist;

      LOOP
         print_log_message ('Loading distribution lines');
         l_leg_dist_tbl.DELETE;

         FETCH cur_leg_dist
         BULK COLLECT INTO l_leg_dist_tbl LIMIT 5000;

         --limit size of Bulk Collect

         -- Get Total Count
         g_total_dist_count := g_total_dist_count + l_leg_dist_tbl.COUNT;
         EXIT WHEN l_leg_dist_tbl.COUNT = 0;

         BEGIN
            -- Bulk Insert into Conversion table
            FORALL indx IN 1 .. l_leg_dist_tbl.COUNT SAVE EXCEPTIONS
               INSERT INTO xxconv.xxar_br_invoices_dist_stg
                    VALUES l_leg_dist_tbl (indx);
         EXCEPTION
            WHEN OTHERS
            THEN
               print_log_message
                  ('Errors encountered while loading distribution lines data '
                  );

               FOR l_indx_exp IN 1 .. SQL%BULK_EXCEPTIONS.COUNT
               LOOP
                  l_err_record :=
                     l_leg_dist_tbl
                              (SQL%BULK_EXCEPTIONS (l_indx_exp).ERROR_INDEX).interface_txn_id;
                  pov_ret_stats := 'E';
                  fnd_file.put_line
                     (fnd_file.LOG,
                         'Record sequence : '
                      || l_leg_dist_tbl
                               (SQL%BULK_EXCEPTIONS (l_indx_exp).ERROR_INDEX).interface_txn_id
                     );
                  fnd_file.put_line
                       (fnd_file.LOG,
                           'Error Message : '
                        || SQLERRM
                               (-SQL%BULK_EXCEPTIONS (l_indx_exp).ERROR_CODE)
                       );

                  -- Updating Leg_process_flag to 'E' for failed records
                  UPDATE xxar_br_invoices_dist_ext_r12 xil
                     SET xil.leg_process_flag = 'E',
                         xil.last_update_date = SYSDATE,
                         xil.last_updated_by = g_last_updated_by,
                         xil.last_update_login = g_last_update_login,
                         xil.program_id = g_conc_program_id,
                         xil.program_application_id = g_prog_appl_id,
                         xil.program_update_date = SYSDATE
                   WHERE xil.interface_txn_id = l_err_record
                     AND xil.leg_process_flag = 'V';

                  g_failed_dist_count := g_failed_dist_count + SQL%ROWCOUNT;
               END LOOP;
         END;
      END LOOP;

      CLOSE cur_leg_dist;

      COMMIT;

      IF g_failed_dist_count > 0
      THEN
         g_retcode := 1;
      END IF;

      g_loaded_dist_count := g_total_dist_count - g_failed_dist_count;

      -- If records successfully posted to conversion staging table
      IF g_total_dist_count > 0
      THEN
         print_log_message
            ('Updating process flag (leg_process_flag) in extraction table for processed records '
            );

         UPDATE xxar_br_invoices_dist_ext_r12 xil
            SET xil.leg_process_flag = 'P',
                xil.last_update_date = SYSDATE,
                xil.last_updated_by = g_last_updated_by,
                xil.last_update_login = g_last_update_login,
                xil.program_id = g_conc_program_id,
                xil.program_application_id = g_prog_appl_id,
                xil.program_update_date = SYSDATE
          WHERE xil.leg_process_flag = 'V'
            AND EXISTS (SELECT 1
                          FROM xxconv.xxar_br_invoices_dist_stg xis
                         WHERE xis.interface_txn_id = xil.interface_txn_id);

         --performance
         UPDATE xxar_br_invoices_dist_ext_r12 xil
            SET xil.leg_process_flag = 'P',
                xil.last_update_date = SYSDATE,
                xil.last_updated_by = g_last_updated_by,
                xil.last_update_login = g_last_update_login,
                xil.program_id = g_conc_program_id,
                xil.program_application_id = g_prog_appl_id,
                xil.program_update_date = SYSDATE
          WHERE xil.leg_process_flag = 'V'
            AND xil.leg_account_class = 'ROUND'
            AND NVL (leg_account_class, 'A') <> 'UNEARN';

         COMMIT;
      ELSE
         -- Either no data to load from extraction table or records already exist in R12 staging table and hence not loaded
         print_log_message
            ('Either no data found for loading from extraction table or records already exist in R12 staging table and hence not loaded '
            );

         UPDATE xxar_br_invoices_dist_ext_r12 xil
            SET xil.leg_process_flag = 'E',
                xil.last_update_date = SYSDATE,
                xil.last_updated_by = g_last_updated_by,
                xil.last_update_login = g_last_update_login,
                xil.program_id = g_conc_program_id,
                xil.program_application_id = g_prog_appl_id,
                xil.program_update_date = SYSDATE
          WHERE xil.leg_process_flag = 'V'
            AND EXISTS (SELECT 1
                          FROM xxconv.xxar_br_invoices_dist_stg xis
                         WHERE xis.interface_txn_id = xil.interface_txn_id);

         g_retcode := 1;
         COMMIT;
      END IF;
   EXCEPTION
      WHEN OTHERS
      THEN
         g_retcode := 2;
         pov_ret_stats := 'E';
         pov_err_msg :=
               'ERROR : Error in load_distribution procedure'
            || SUBSTR (SQLERRM, 1, 150);
         ROLLBACK;
   END load_distribution;

-- ========================
 -- Procedure: PRINT_STATS_P
 -- =============================================================================
 --   This procedure is used to print statistics after end of validate,
 --   conversion and reconcile mode
 -- =============================================================================
   PROCEDURE print_stats_p
   IS
      l_tot_inv    NUMBER := 0;
      l_err_inv    NUMBER := 0;
      l_val_inv    NUMBER := 0;
      l_int_inv    NUMBER := 0;
      l_conv_inv   NUMBER := 0;
   BEGIN
      fnd_file.put_line
                 (fnd_file.output,
                  'Program Name : Eaton UNIFY BR Invoices Conversion Program'
                 );
      fnd_file.put_line (fnd_file.output,
                         'Request ID   : ' || TO_CHAR (g_request_id)
                        );
      fnd_file.put_line (fnd_file.output,
                            'Report Date  : '
                         || TO_CHAR (SYSDATE, 'DD-MON-RRRR HH24:MI:SS')
                        );
      fnd_file.put_line
         (fnd_file.output,
          '============================================================================================='
         );
      fnd_file.put_line (fnd_file.output, CHR (10));
      fnd_file.put_line (fnd_file.output, 'Parameters');
      fnd_file.put_line (fnd_file.output,
                         '---------------------------------------------'
                        );
      fnd_file.put_line (fnd_file.output, 'Run Mode        : ' || g_run_mode);
      fnd_file.put_line (fnd_file.output, 'Batch ID        : ' || g_batch_id);
      fnd_file.put_line (fnd_file.output,
                         'Reprocess records    : ' || g_process_records
                        );
      fnd_file.put_line (fnd_file.output,
                         'Operating Unit    : ' || g_leg_operating_unit
                        );
      fnd_file.put_line (fnd_file.output,
                         'Transaction Type    : ' || g_leg_trasaction_type
                        );
      fnd_file.put_line (fnd_file.output, 'GL Date        : ' || g_gl_date);
      fnd_file.put_line (fnd_file.output, CHR (10));
      fnd_file.put_line
         (fnd_file.output,
          '============================================================================================='
         );
      fnd_file.put_line (fnd_file.output,
                         'Statistics (' || g_run_mode || '):');
      fnd_file.put_line
         (fnd_file.output,
          '============================================================================================='
         );

      IF NVL (g_total_count, 0) > 0 OR NVL (g_total_dist_count, 0) > 0
      THEN
         fnd_file.put_line (fnd_file.output, 'Invoices Lines');
         fnd_file.put_line
                (fnd_file.output,
                 '----------------------------------------------------------'
                );
         fnd_file.put_line (fnd_file.output,
                               'Records Submitted                   : '
                            || g_total_count
                           );
         fnd_file.put_line (fnd_file.output,
                               'Records Loaded                      : '
                            || g_loaded_count
                           );
         fnd_file.put_line (fnd_file.output,
                               'Records Errored                     : '
                            || g_failed_count
                           );
         fnd_file.put_line (fnd_file.output, CHR (10));
         fnd_file.put_line (fnd_file.output, 'Distribution Lines');
         fnd_file.put_line
                (fnd_file.output,
                 '-----------------------------------------------------------'
                );
         fnd_file.put_line (fnd_file.output,
                               'Records Submitted                   : '
                            || g_total_dist_count
                           );
         fnd_file.put_line (fnd_file.output,
                               'Records Loaded                      : '
                            || g_loaded_dist_count
                           );
         fnd_file.put_line (fnd_file.output,
                               'Records Errored                     : '
                            || g_failed_dist_count
                           );
      ELSE
         SELECT COUNT (1)
           INTO l_tot_inv
           FROM xxconv.xxar_br_invoices_stg xis
          WHERE xis.batch_id = NVL (g_new_batch_id, xis.batch_id)
            AND xis.run_sequence_id =
                                   NVL (g_new_run_seq_id, xis.run_sequence_id);

         SELECT COUNT (1)
           INTO l_err_inv
           FROM xxconv.xxar_br_invoices_stg xis
          WHERE xis.batch_id = NVL (g_new_batch_id, xis.batch_id)
            AND xis.run_sequence_id =
                                   NVL (g_new_run_seq_id, xis.run_sequence_id)
            AND xis.process_flag = 'E';

         SELECT COUNT (1)
           INTO l_val_inv
           FROM xxconv.xxar_br_invoices_stg xis
          WHERE xis.batch_id = NVL (g_new_batch_id, xis.batch_id)
            AND xis.run_sequence_id =
                                   NVL (g_new_run_seq_id, xis.run_sequence_id)
            AND xis.process_flag = 'V';

         SELECT COUNT (1)
           INTO l_int_inv
           FROM xxconv.xxar_br_invoices_stg xis
          WHERE xis.batch_id = NVL (g_new_batch_id, xis.batch_id)
            AND xis.run_sequence_id =
                                   NVL (g_new_run_seq_id, xis.run_sequence_id)
            AND xis.process_flag = 'P';

         SELECT COUNT (1)
           INTO l_conv_inv
           FROM xxconv.xxar_br_invoices_stg xis
          WHERE xis.batch_id = NVL (g_new_batch_id, xis.batch_id)
            AND xis.run_sequence_id =
                                   NVL (g_new_run_seq_id, xis.run_sequence_id)
            AND xis.process_flag = 'C';

         fnd_file.put_line (fnd_file.output, 'For Invoice Lines:');
         fnd_file.put_line
                 (fnd_file.output,
                  '----------------------------------------------------------'
                 );
         fnd_file.put_line (fnd_file.output,
                               'Records Submitted                     : '
                            || l_tot_inv
                           );
         fnd_file.put_line (fnd_file.output,
                               'Records Validated                     : '
                            || l_val_inv
                           );
         fnd_file.put_line (fnd_file.output,
                               'Records Errored                       : '
                            || l_err_inv
                           );
         fnd_file.put_line (fnd_file.output,
                               'Records Interfaced                    : '
                            || l_int_inv
                           );
         fnd_file.put_line (fnd_file.output,
                               'Records Completed                     : '
                            || l_conv_inv
                           );
         fnd_file.put_line (fnd_file.output, ' ');
         l_tot_inv := 0;
         l_val_inv := 0;
         l_err_inv := 0;
         l_int_inv := 0;
         l_conv_inv := 0;

         SELECT COUNT (1)
           INTO l_tot_inv
           FROM xxconv.xxar_br_invoices_dist_stg xis
          WHERE xis.batch_id = NVL (g_new_batch_id, xis.batch_id)
            AND xis.run_sequence_id =
                                   NVL (g_new_run_seq_id, xis.run_sequence_id)
            AND NVL (leg_account_class, 'A') <> 'ROUND'
            AND NVL (leg_account_class, 'A') <> 'UNEARN';

         SELECT COUNT (1)
           INTO l_err_inv
           FROM xxconv.xxar_br_invoices_dist_stg xis
          WHERE xis.batch_id = NVL (g_new_batch_id, xis.batch_id)
            AND xis.run_sequence_id =
                                   NVL (g_new_run_seq_id, xis.run_sequence_id)
            AND xis.process_flag = 'E'
            AND NVL (leg_account_class, 'A') <> 'ROUND'
            AND NVL (leg_account_class, 'A') <> 'UNEARN';

         SELECT COUNT (1)
           INTO l_val_inv
           FROM xxconv.xxar_br_invoices_dist_stg xis
          WHERE xis.batch_id = NVL (g_new_batch_id, xis.batch_id)
            AND xis.run_sequence_id =
                                   NVL (g_new_run_seq_id, xis.run_sequence_id)
            AND xis.process_flag = 'V'
            AND NVL (leg_account_class, 'A') <> 'ROUND'
            AND NVL (leg_account_class, 'A') <> 'UNEARN';

         SELECT COUNT (1)
           INTO l_int_inv
           FROM xxconv.xxar_br_invoices_dist_stg xis
          WHERE xis.batch_id = NVL (g_new_batch_id, xis.batch_id)
            AND xis.run_sequence_id =
                                   NVL (g_new_run_seq_id, xis.run_sequence_id)
            AND xis.process_flag = 'P'
            AND NVL (leg_account_class, 'A') <> 'ROUND'
            AND NVL (leg_account_class, 'A') <> 'UNEARN';

         SELECT COUNT (1)
           INTO l_conv_inv
           FROM xxconv.xxar_br_invoices_dist_stg xis
          WHERE xis.batch_id = NVL (g_new_batch_id, xis.batch_id)
            AND xis.run_sequence_id =
                                   NVL (g_new_run_seq_id, xis.run_sequence_id)
            AND xis.process_flag = 'C'
            AND NVL (leg_account_class, 'A') <> 'ROUND'
            AND NVL (leg_account_class, 'A') <> 'UNEARN';

         fnd_file.put_line (fnd_file.output, 'For Distribution Lines:');
         fnd_file.put_line
                 (fnd_file.output,
                  '----------------------------------------------------------'
                 );
         fnd_file.put_line (fnd_file.output,
                               'Records Submitted                     : '
                            || l_tot_inv
                           );
         fnd_file.put_line (fnd_file.output,
                               'Records Validated                     : '
                            || l_val_inv
                           );
         fnd_file.put_line (fnd_file.output,
                               'Records Errored                       : '
                            || l_err_inv
                           );
         fnd_file.put_line (fnd_file.output,
                               'Records Interfaced                    : '
                            || l_int_inv
                           );
         fnd_file.put_line (fnd_file.output,
                               'Records Completed                     : '
                            || l_conv_inv
                           );
      END IF;

      fnd_file.put_line (fnd_file.output, CHR (10));
      fnd_file.put_line
         (fnd_file.output,
          '==================================================================================================='
         );
   EXCEPTION
      WHEN OTHERS
      THEN
         NULL;
   END print_stats_p;

   -- ========================
-- Procedure: PRE_VALIDATE_INVOICE
-- =============================================================================
--   This procedure is used to do pre validations of functional setups
--   before the actual conversion run.
-- =============================================================================
   PROCEDURE pre_validate_invoice
   IS
      l_ou_map          NUMBER;
      l_batch_source    NUMBER;
      l_pmt_terms_map   NUMBER;
      l_trx_type_map    NUMBER;
      l_tax_code_map    NUMBER;
      l_err_msg         VARCHAR2 (2000);
      l_memo_line       NUMBER;
   BEGIN
        -- Check whether operating unit cross reference exists
      /*  BEGIN
           SELECT 1
             INTO l_ou_map
             FROM apps.fnd_lookup_types_tl flt
            WHERE flt.LANGUAGE = USERENV('LANG')
              AND UPPER(flt.lookup_type) = g_ou_lookup;
        EXCEPTION
           WHEN NO_DATA_FOUND THEN
              --          l_err_code  := 'ETN_AR_PRE_VALIDATE_ERROR';
              l_err_msg := 'Error : Exception in Pre-validation Procedure. Lookup ' ||
                           g_ou_lookup ||
                           ' not defined for Operating unit cross reference';
              print_log_message(l_err_msg);
              g_retcode := 1;
           WHEN OTHERS THEN
              g_retcode := 1;
              l_err_msg := 'Error : Exception in Pre-validation Procedure. For lookup ' ||
                           g_ou_lookup || ':' || SUBSTR(SQLERRM, 1, 150);
              print_log_message(l_err_msg);
        END;  */

      -- Check whether payment terms cross reference exists
      BEGIN
         SELECT 1
           INTO l_pmt_terms_map
           FROM apps.fnd_lookup_types_tl flt
          WHERE flt.LANGUAGE = USERENV ('LANG')
            AND UPPER (flt.lookup_type) = g_pmt_term_lookup;

         print_log_message (   'BR INVOICEPRE-g_pmt_term_lookup'
                            || l_pmt_terms_map
                            || 'pre_validate_invoice'
                           );
      EXCEPTION
         WHEN NO_DATA_FOUND
         THEN
            g_retcode := 1;
            l_err_msg :=
                  'Error : Exception in Pre-validation Procedure. Lookup '
               || g_pmt_term_lookup
               || ' not defined for payment terms cross reference';
            print_log_message (l_err_msg);
         WHEN OTHERS
         THEN
            g_retcode := 1;
            l_err_msg :=
                  'Error : Exception in Pre-validation Procedure. For lookup '
               || g_pmt_term_lookup
               || ': '
               || SUBSTR (SQLERRM, 1, 150);
            print_log_message (l_err_msg);
      END;

      -- Check whether transaction type cross reference exists
      BEGIN
         SELECT 1
           INTO l_trx_type_map
           FROM apps.fnd_lookup_types_tl flt
          WHERE flt.LANGUAGE = USERENV ('LANG')
            AND UPPER (flt.lookup_type) = g_trx_type_lookup;

         print_log_message (   'BR INVOICEPRE-g_trx_type_lookup'
                            || l_trx_type_map
                            || 'pre_validate_invoice'
                           );
      EXCEPTION
         WHEN NO_DATA_FOUND
         THEN
            g_retcode := 1;
            l_err_msg :=
                  'Error : Exception in Pre-validation Procedure. Lookup '
               || g_trx_type_lookup
               || ' not defined for transaction types cross reference';
            print_log_message (l_err_msg);
         WHEN OTHERS
         THEN
            g_retcode := 1;
            l_err_msg :=
                  'Error : Exception in Pre-validation Procedure. For lookup '
               || g_trx_type_lookup
               || ': '
               || SUBSTR (SQLERRM, 1, 150);
            print_log_message (l_err_msg);
      END;

      -- Check whether tax code cross reference exists
      BEGIN
         SELECT 1
           INTO l_tax_code_map
           FROM apps.fnd_lookup_types_tl flt
          WHERE flt.LANGUAGE = USERENV ('LANG')
            AND UPPER (flt.lookup_type) = g_tax_code_lookup;

         print_log_message (   'BR INVOICEPRE-g_tax_code_lookup'
                            || l_tax_code_map
                            || 'pre_validate_invoice'
                           );
      EXCEPTION
         WHEN NO_DATA_FOUND
         THEN
            g_retcode := 1;
            l_err_msg :=
                  'Error : Exception in Pre-validation Procedure. Lookup '
               || g_tax_code_lookup
               || ' not defined for tax code cross reference';
            print_log_message (l_err_msg);
         WHEN OTHERS
         THEN
            g_retcode := 1;
            l_err_msg :=
                  'Error : Exception in Pre-validation Procedure. For lookup '
               || g_tax_code_lookup
               || ': '
               || SUBSTR (SQLERRM, 1, 150);
            print_log_message (l_err_msg);
      END;

      -- Check whether memo line is created for conversion freight
      BEGIN
         SELECT 1
           INTO l_memo_line
           FROM ar_memo_lines_all_tl
          WHERE LANGUAGE = USERENV ('LANG')
            AND UPPER (NAME) LIKE '%CONVERSION%FREIGHT%'
            AND ROWNUM = 1;
      EXCEPTION
         WHEN NO_DATA_FOUND
         THEN
            g_retcode := 1;
            l_err_msg :=
               'Error : Exception in Pre-validation Procedure. Memo line for conversion freight not defined for freight line creation';
            print_log_message (l_err_msg);
         WHEN OTHERS
         THEN
            g_retcode := 1;
            l_err_msg :=
                  'Error : Exception in Pre-validation Procedure while checking memo line for conversion freight. '
               || SUBSTR (SQLERRM, 1, 150);
            print_log_message (l_err_msg);
      END;

      IF l_err_msg IS NULL
      THEN
         print_log_message ('Prevalidation Proess completed successfully.');
      END IF;
   EXCEPTION
      WHEN OTHERS
      THEN
         l_err_msg :=
               'Error : Exception in Pre-validation Procedure. '
            || SUBSTR (SQLERRM, 1, 150);
         print_log_message (l_err_msg);
         g_retcode := 2;
   END pre_validate_invoice;

   -- ========================
-- Procedure: log_errors
-- =============================================================================
--   This procedure will log the errors in the error report using error
--   framework
-- =============================================================================
   PROCEDURE log_errors (
      pin_transaction_id        IN       NUMBER DEFAULT NULL,
      piv_source_keyname1       IN       xxetn_common_error.source_keyname1%TYPE
            DEFAULT NULL,
      piv_source_keyvalue1      IN       xxetn_common_error.source_keyvalue1%TYPE
            DEFAULT NULL,
      piv_source_keyname2       IN       xxetn_common_error.source_keyname2%TYPE
            DEFAULT NULL,
      piv_source_keyvalue2      IN       xxetn_common_error.source_keyvalue2%TYPE
            DEFAULT NULL,
      piv_source_keyname3       IN       xxetn_common_error.source_keyname3%TYPE
            DEFAULT NULL,
      piv_source_keyvalue3      IN       xxetn_common_error.source_keyvalue3%TYPE
            DEFAULT NULL,
      piv_source_keyname4       IN       xxetn_common_error.source_keyname4%TYPE
            DEFAULT NULL,
      piv_source_keyvalue4      IN       xxetn_common_error.source_keyvalue4%TYPE
            DEFAULT NULL,
      piv_source_keyname5       IN       xxetn_common_error.source_keyname5%TYPE
            DEFAULT NULL,
      piv_source_keyvalue5      IN       xxetn_common_error.source_keyvalue5%TYPE
            DEFAULT NULL,
      piv_source_column_name    IN       xxetn_common_error.source_column_name%TYPE
            DEFAULT NULL,
      piv_source_column_value   IN       xxetn_common_error.source_column_value%TYPE
            DEFAULT NULL,
      piv_source_table          IN       xxetn_common_error.source_table%TYPE
            DEFAULT NULL,
      piv_error_type            IN       xxetn_common_error.ERROR_TYPE%TYPE,
      piv_error_code            IN       xxetn_common_error.ERROR_CODE%TYPE,
      piv_error_message         IN       xxetn_common_error.error_message%TYPE,
      pov_return_status         OUT      VARCHAR2,
      pov_error_msg             OUT      VARCHAR2
   )
   IS
      l_return_status   VARCHAR2 (1);
      l_error_message   VARCHAR2 (2000);
   BEGIN
      --  xxetn_debug_pkg.add_debug ( p_err_msg );
      g_err_indx := g_err_indx + 1;
      g_error_tab (g_err_indx).source_table :=
                               NVL (piv_source_table, 'XXAR_BR_INVOICES_STG');
      g_error_tab (g_err_indx).interface_staging_id := pin_transaction_id;
      g_error_tab (g_err_indx).source_keyname1 := piv_source_keyname1;
      g_error_tab (g_err_indx).source_keyvalue1 := piv_source_keyvalue1;
      g_error_tab (g_err_indx).source_keyname2 := piv_source_keyname2;
      g_error_tab (g_err_indx).source_keyvalue2 := piv_source_keyvalue2;
      g_error_tab (g_err_indx).source_keyname3 := piv_source_keyname3;
      g_error_tab (g_err_indx).source_keyvalue3 := piv_source_keyvalue3;
      g_error_tab (g_err_indx).source_keyname4 := piv_source_keyname4;
      g_error_tab (g_err_indx).source_keyvalue4 := piv_source_keyvalue4;
      g_error_tab (g_err_indx).source_keyname5 := piv_source_keyname5;
      g_error_tab (g_err_indx).source_keyvalue5 := piv_source_keyvalue5;
      g_error_tab (g_err_indx).source_column_name := piv_source_column_name;
      g_error_tab (g_err_indx).source_column_value := piv_source_column_value;
      g_error_tab (g_err_indx).ERROR_TYPE := piv_error_type;
      g_error_tab (g_err_indx).ERROR_CODE := piv_error_code;
      g_error_tab (g_err_indx).error_message := piv_error_message;

      IF MOD (g_err_indx, g_err_lmt) = 0
      THEN
         xxetn_common_error_pkg.add_error
                                     (pov_return_status        => l_return_status,
                                      pov_error_msg            => l_error_message,
                                      pi_source_tab            => g_error_tab,
                                      pin_batch_id             => g_new_batch_id,
                                      pin_run_sequence_id      => g_new_run_seq_id
                                     );
         g_error_tab.DELETE;
         g_err_indx := 0;
         pov_return_status := l_return_status;
         pov_error_msg := l_error_message;
      END IF;

      print_log_message ('p_err_msg:' || piv_error_message);
   EXCEPTION
      WHEN OTHERS
      THEN
         xxetn_debug_pkg.add_debug
                      (   'Error: Exception occured in log_errors procedure '
                       || SUBSTR (SQLERRM, 1, 150)
                      );
   END log_errors;

   --
-- ========================
-- Procedure: ASSIGN_BATCH_ID
-- =============================================================================
--   This procedure assigns batch id
-- =============================================================================
--
   PROCEDURE assign_batch_id (
      p_return_status   OUT   VARCHAR2,
      p_error_code      OUT   VARCHAR2,
      p_error_message   OUT   VARCHAR2
   )
   IS
      PRAGMA AUTONOMOUS_TRANSACTION;
   BEGIN
      -- g_batch_id NULL is considered a fresh run
      IF g_batch_id IS NULL
      THEN
         -- print_log_message ('assign_batch_id g_batch_id IS NULL');
         print_log_message
                         (   ' inside assign_batch_id New Batch ID        : '
                          || g_new_batch_id
                         );
         print_log_message (   'inside assign_batch_id New Run Sequence ID : '
                            || g_new_run_seq_id
                           );

         --  print_log_message (' g_leg_operating_unit       : ' || g_leg_operating_unit);
         -- print_log_message (' g_leg_trasaction_type' || g_leg_trasaction_type);
         UPDATE xxconv.xxar_br_invoices_stg
            SET process_flag = 'N',
                batch_id = g_new_batch_id,
                run_sequence_id = g_new_run_seq_id,
                last_update_date = SYSDATE,
                last_updated_by = g_user_id,
                last_update_login = g_login_id,
                program_application_id = g_prog_appl_id,
                program_id = g_conc_program_id,
                program_update_date = SYSDATE,
                request_id = g_request_id
          WHERE batch_id IS NULL
            AND UPPER (leg_operating_unit) =
                        UPPER (NVL (g_leg_operating_unit, leg_operating_unit))
            AND UPPER (leg_cust_trx_type_name) =
                   UPPER (NVL (g_leg_trasaction_type, leg_cust_trx_type_name));

         p_error_message :=
                    'Updated staging table, Update Count : ' || SQL%ROWCOUNT;
         print_log_message ('Message :' || p_error_message);

         UPDATE xxconv.xxar_br_invoices_dist_stg xids
            SET xids.process_flag = 'N',
                xids.batch_id = g_new_batch_id,
                xids.run_sequence_id = g_new_run_seq_id,
                xids.last_update_date = SYSDATE,
                xids.last_updated_by = g_user_id,
                xids.last_update_login = g_login_id,
                xids.program_application_id = g_prog_appl_id,
                xids.program_id = g_conc_program_id,
                xids.program_update_date = SYSDATE,
                xids.request_id = g_request_id
          WHERE xids.batch_id IS NULL
            AND UPPER (xids.leg_org_name) =
                         UPPER (NVL (g_leg_operating_unit, xids.leg_org_name))
            AND EXISTS (
                   SELECT 1
                     FROM xxconv.xxar_br_invoices_stg xis
                    WHERE xids.leg_customer_trx_id = xis.leg_customer_trx_id
                      AND xis.batch_id = g_new_batch_id);

         p_error_message :=
               'Updated Distribution staging table, Update Count : '
            || SQL%ROWCOUNT;
         print_log_message ('Message:' || p_error_message);
      ELSE
         UPDATE xxconv.xxar_br_invoices_stg
            SET process_flag = 'N',
                run_sequence_id = g_new_run_seq_id,
                last_update_date = SYSDATE,
                last_updated_by = g_user_id,
                last_update_login = g_login_id,
                program_application_id = g_prog_appl_id,
                program_id = g_conc_program_id,
                program_update_date = SYSDATE,
                request_id = g_request_id
          WHERE batch_id = g_new_batch_id
            AND (   g_process_records = 'ALL'
                    AND (process_flag IN ('N', 'E'))
                 OR g_process_records = 'ERROR' AND (process_flag = 'E')
                 OR g_process_records = 'UNPROCESSED'
                    AND (process_flag = 'N')
                )
            AND NVL (ERROR_TYPE, 'NO_ERR_TYPE') <> 'ERR_IMP'
            AND UPPER (leg_cust_trx_type_name) =
                   UPPER (NVL (g_leg_trasaction_type, leg_cust_trx_type_name))
            AND UPPER (leg_operating_unit) =
                        UPPER (NVL (g_leg_operating_unit, leg_operating_unit));

         UPDATE xxconv.xxar_br_invoices_dist_stg xids
            SET xids.process_flag = 'N',
                xids.run_sequence_id = g_new_run_seq_id,
                xids.last_update_date = SYSDATE,
                xids.last_updated_by = g_user_id,
                xids.last_update_login = g_login_id,
                xids.program_application_id = g_prog_appl_id,
                xids.program_id = g_conc_program_id,
                xids.program_update_date = SYSDATE,
                xids.request_id = g_request_id
          WHERE xids.batch_id = g_new_batch_id
            AND (       g_process_records = 'ALL'
                    AND (xids.process_flag IN ('N', 'E'))
                 OR g_process_records = 'ERROR' AND (xids.process_flag = 'E')
                 OR     g_process_records = 'UNPROCESSED'
                    AND (xids.process_flag = 'N')
                )
            AND UPPER (xids.leg_org_name) =
                         UPPER (NVL (g_leg_operating_unit, xids.leg_org_name))
            AND NVL (xids.ERROR_TYPE, 'NO_ERR_TYPE') <> 'ERR_IMP'
            AND EXISTS (
                   SELECT 1
                     FROM xxconv.xxar_br_invoices_stg xis
                    WHERE xids.leg_customer_trx_id = xis.leg_customer_trx_id
                      AND xis.batch_id = g_new_batch_id);

         p_error_message :=
                    'Updated staging table, Update Count : ' || SQL%ROWCOUNT;
      END IF;                                                    -- g_batch_id

      COMMIT;
      p_return_status := fnd_api.g_ret_sts_success;
   EXCEPTION
      WHEN OTHERS
      THEN
         p_return_status := fnd_api.g_ret_sts_error;
         p_error_code := 'ETN_AR_ASSIGN_BATCH_ERROR';
         p_error_message :=
               'Error : Exception in assign_batch_id Procedure. '
            || SUBSTR (SQLERRM, 1, 150);
   END assign_batch_id;

   -- ========================
-- Procedure: PRINT_LOG1_MESSAGE
-- =============================================================================
--   This procedure is used to write message to log file if log level is set to 1.
-- =============================================================================
   PROCEDURE print_log1_message (piv_message IN VARCHAR2)
   IS
   BEGIN
      IF NVL (g_request_id, 0) > 0 AND g_log_level > 0
      THEN
         fnd_file.put_line (fnd_file.LOG, piv_message);
      END IF;
   EXCEPTION
      WHEN OTHERS
      THEN
         NULL;
   END print_log1_message;

   -- ========================
-- Function: CHECK_MANDATORY
-- =============================================================================
--   This function is used to perform NULL check on mandatory fields
-- =============================================================================
   FUNCTION check_mandatory (
      pin_trx_id         IN   NUMBER DEFAULT NULL,
      pin_cust_trx_id    IN   NUMBER DEFAULT NULL,
      piv_column_value   IN   VARCHAR2,
      piv_column_name    IN   VARCHAR2,
      piv_table_name     IN   VARCHAR2 DEFAULT 'XXAR_BR_INVOICES_STG'
   )
      RETURN BOOLEAN
   IS
      l_err_code         VARCHAR2 (40);
      l_err_msg          VARCHAR2 (2000);
      l_log_ret_status   VARCHAR2 (50);
      l_log_err_msg      VARCHAR2 (2000);
   BEGIN
      IF TRIM (piv_column_value) IS NULL
      THEN
         l_err_code := 'ETN_BR_MANDATORY_NOT_ENTERED';
         l_err_msg := 'Error: Mandatory column not entered. ';
         print_log1_message (l_err_msg || piv_column_name);
         g_retcode := 1;
         log_errors (pin_transaction_id           => pin_trx_id,
                     piv_source_column_name       => 'LEGACY_CUSTOMER_TRX_ID',
                     piv_source_column_value      => pin_cust_trx_id,
                     piv_source_keyname1          => piv_column_name,
                     piv_source_keyvalue1         => piv_column_value,
                     piv_error_type               => 'ERR_VAL',
                     piv_error_code               => l_err_code,
                     piv_error_message            => l_err_msg,
                     pov_return_status            => l_log_ret_status,
                     piv_source_table             => piv_table_name,
                     pov_error_msg                => l_log_err_msg
                    );
         RETURN TRUE;
      ELSE
         --   print_log1_message ('Mandatory check passed for ' || piv_column_name);
         RETURN FALSE;
      END IF;
   EXCEPTION
      WHEN OTHERS
      THEN
         g_retcode := 2;
         l_err_code := 'ETN_BR_PROCEDURE_EXCEPTION';
         l_err_msg := 'Error while checking mandatory column ';
         log_errors (pin_transaction_id           => pin_trx_id,
                     piv_source_column_name       => 'LEGACY_CUSTOMER_TRX_ID',
                     piv_source_column_value      => pin_cust_trx_id,
                     piv_source_keyname1          => piv_column_name,
                     piv_source_keyvalue1         => piv_column_value,
                     piv_error_type               => 'ERR_VAL',
                     piv_error_code               => l_err_code,
                     piv_error_message            => l_err_msg,
                     piv_source_table             => piv_table_name,
                     pov_return_status            => l_log_ret_status,
                     pov_error_msg                => l_log_err_msg
                    );
         RETURN TRUE;
   END check_mandatory;

/*
   -- ========================
-- Procedure: VALIDATE_AMOUNT
-- =============================================================================
--   This procedure will validate amount
-- =============================================================================
   PROCEDURE validate_amount (
      pin_amount             IN       NUMBER,
      piv_trx_type           IN       VARCHAR2,
      pin_interface_txn_id   IN       NUMBER,
      pov_valid_flag         OUT      VARCHAR2
   )
   IS
      l_err_code         VARCHAR2 (40);
      l_err_msg          VARCHAR2 (2000);
      l_log_ret_status   VARCHAR2 (50);
      l_log_err_msg      VARCHAR2 (2000);
   BEGIN
      IF pin_amount IS NOT NULL
      THEN
         print_log_message ('validate_amount procedure');

         -- Check whether amount is not negative for Invoices and debit memos
         IF piv_trx_type IN ('INV') AND pin_amount < 0
         THEN
            g_retcode := 1;
            l_err_code := 'ETN_AR_TRX_AMOUNT_ERROR';
            l_err_msg :=
               'Transaction is of Invoice or Debit Memo but amount is less than 0';
            pov_valid_flag := 'E';
            log_errors (pin_transaction_id           => pin_interface_txn_id,
                        piv_source_keyname1          => 'Transaction Amount',
                        piv_source_keyvalue1         => pin_amount,
                        piv_source_column_name       => 'INTERFACE_TXN_ID',
                        piv_source_column_value      => pin_interface_txn_id,
                        piv_error_type               => 'ERR_VAL',
                        piv_error_code               => l_err_code,
                        piv_error_message            => l_err_msg,
                        pov_return_status            => l_log_ret_status,
                        pov_error_msg                => l_log_err_msg
                       );
         -- Check whether amount is not positive for credit memos
         END IF;
      END IF;
   EXCEPTION
      WHEN OTHERS
      THEN
         l_err_code := 'ETN_AR_PROCEDURE_EXCEPTION';
         l_err_msg :=
               'Error : Error validating tranasction amount for interface transaction id '
            || pin_interface_txn_id
            || SUBSTR (SQLERRM, 1, 150);
         g_retcode := 2;
         pov_valid_flag := 'E';
         log_errors (pin_transaction_id           => pin_interface_txn_id,
                     piv_source_keyname1          => 'Transaction Amount',
                     piv_source_keyvalue1         => pin_amount,
                     piv_error_type               => 'ERR_VAL',
                     piv_source_column_name       => 'INTERFACE_TXN_ID',
                     piv_source_column_value      => pin_interface_txn_id,
                     piv_error_code               => l_err_code,
                     piv_error_message            => l_err_msg,
                     pov_return_status            => l_log_ret_status,
                     pov_error_msg                => l_log_err_msg
                    );
   END validate_amount;*/

   -- ========================
-- Procedure: VALIDATE_ACCOUNTS
-- =============================================================================
--   This procedure validates all
--   the account related information
-- =============================================================================
   PROCEDURE validate_accounts (
      p_in_txn_id   IN       NUMBER,
      p_in_seg1     IN       VARCHAR2,
      p_in_seg2     IN       VARCHAR2,
      p_in_seg3     IN       VARCHAR2,
      p_in_seg4     IN       VARCHAR2,
      p_in_seg5     IN       VARCHAR2,
      p_in_seg6     IN       VARCHAR2,
      p_in_seg7     IN       VARCHAR2,
      x_out_acc     OUT      xxetn_common_pkg.g_rec_type,
      x_out_ccid    OUT      NUMBER
   )
   IS
      l_in_rec           xxetn_coa_mapping_pkg.g_coa_rec_type := NULL;
      x_ccid             NUMBER                               := NULL;
      x_out_rec          xxetn_coa_mapping_pkg.g_coa_rec_type := NULL;
      x_msg              VARCHAR2 (4000)                      := NULL;
      x_status           VARCHAR2 (50)                        := NULL;
      l_in_seg_rec       xxetn_common_pkg.g_rec_type          := NULL;
      x_err              VARCHAR2 (4000)                      := NULL;
      l_err_code         VARCHAR2 (40)                        := NULL;
      l_err_msg          VARCHAR2 (2000)                      := NULL;
      l_log_ret_status   VARCHAR2 (50);
      l_log_err_msg      VARCHAR2 (2000);
   BEGIN
      x_out_acc := NULL;
      x_out_ccid := NULL;
      xxetn_debug_pkg.add_debug
                      (piv_debug_msg      => 'Validate accounts procedure called ');
      l_in_rec.segment1 := p_in_seg1;
      l_in_rec.segment2 := p_in_seg2;
      l_in_rec.segment3 := p_in_seg3;
      l_in_rec.segment4 := p_in_seg4;
      l_in_rec.segment5 := p_in_seg5;
      l_in_rec.segment6 := p_in_seg6;
      l_in_rec.segment7 := p_in_seg7;
      xxetn_coa_mapping_pkg.get_code_combination (g_direction,
                                                  NULL,
                                                  SYSDATE,
                                                  l_in_rec,
                                                  x_out_rec,
                                                  x_status,
                                                  x_msg
                                                 );

      IF x_status = g_coa_processed
      THEN
         l_in_seg_rec.segment1 := x_out_rec.segment1;
         l_in_seg_rec.segment2 := x_out_rec.segment2;
         l_in_seg_rec.segment3 := x_out_rec.segment3;
         l_in_seg_rec.segment4 := x_out_rec.segment4;
         l_in_seg_rec.segment5 := x_out_rec.segment5;
         l_in_seg_rec.segment6 := x_out_rec.segment6;
         l_in_seg_rec.segment7 := x_out_rec.segment7;
         l_in_seg_rec.segment8 := x_out_rec.segment8;
         l_in_seg_rec.segment9 := x_out_rec.segment9;
         l_in_seg_rec.segment10 := x_out_rec.segment10;
         xxetn_common_pkg.get_ccid (l_in_seg_rec, x_ccid, x_err);

         IF x_err IS NULL
         THEN
            x_out_acc.segment1 := x_out_rec.segment1;
            x_out_acc.segment2 := x_out_rec.segment2;
            x_out_acc.segment3 := x_out_rec.segment3;
            x_out_acc.segment4 := x_out_rec.segment4;
            x_out_acc.segment5 := x_out_rec.segment5;
            x_out_acc.segment6 := x_out_rec.segment6;
            x_out_acc.segment7 := x_out_rec.segment7;
            x_out_acc.segment8 := x_out_rec.segment8;
            x_out_acc.segment9 := x_out_rec.segment9;
            x_out_acc.segment10 := x_out_rec.segment10;
            x_out_ccid := x_ccid;
         -- print_log1_message ('Account information successfully derived ');
         ELSE
            l_err_code := 'ETN_BR_INCORRECT_ACCOUNT_INFORMATION';
            l_err_msg :=
                  'Error : Following error in COA transformation : ' || x_err;
            print_log1_message (l_err_msg || 'leg_dist_segment1');
            g_retcode := 1;
            log_errors (pin_transaction_id           => p_in_txn_id,
                        piv_source_column_name       => 'SEGMENT1',
                        piv_source_column_value      => p_in_seg1,
                        piv_error_type               => 'ERR_VAL',
                        piv_error_code               => l_err_code,
                        piv_error_message            => l_err_msg,
                        pov_return_status            => l_log_ret_status,
                        piv_source_table             => 'XXAR_BR_INVOICES_DIST_STG',
                        pov_error_msg                => l_log_err_msg
                       );
         END IF;
      ELSIF x_status = g_coa_error
      THEN
         l_err_code := 'ETN_BR_INCORRECT_ACCOUNT_INFORMATION';
         l_err_msg :=
                  'Error : Following error in COA transformation : ' || x_msg;
         print_log1_message (l_err_msg || 'leg_dist_segment1');
         g_retcode := 1;
         log_errors (pin_transaction_id           => p_in_txn_id,
                     piv_source_column_name       => 'SEGMENT1',
                     piv_source_column_value      => p_in_seg1,
                     piv_error_type               => 'ERR_VAL',
                     piv_error_code               => l_err_code,
                     piv_error_message            => l_err_msg,
                     pov_return_status            => l_log_ret_status,
                     piv_source_table             => 'XXAR_BR_INVOICES_DIST_STG',
                     pov_error_msg                => l_log_err_msg
                    );
      END IF;

      xxetn_debug_pkg.add_debug
                         (piv_debug_msg      => 'Validate accounts procedure ends ');
   EXCEPTION
      WHEN OTHERS
      THEN
         g_retcode := 2;
         l_err_code := 'ETN_BR_PROCEDURE_EXCEPTION';
         l_err_msg := 'Error while deriving accounting information ';
         log_errors (pin_transaction_id           => p_in_txn_id,
                     piv_source_column_name       => 'SEGMENT1',
                     piv_source_column_value      => p_in_seg1,
                     piv_error_type               => 'ERR_VAL',
                     piv_error_code               => l_err_code,
                     piv_error_message            => l_err_msg,
                     piv_source_table             => 'XXAR_BR_INVOICES_DIST_STG',
                     pov_return_status            => l_log_ret_status,
                     pov_error_msg                => l_log_err_msg
                    );
   END validate_accounts;

   -- ========================
-- Procedure: CREATE BANK
-- =============================================================================
--   This procedure is used to run generic validations for invoice lines
--   and distribution lines
-- =============================================================================
   PROCEDURE create_bank (
      pv_country           IN       VARCHAR2,
      pv_bank_ret_status   OUT      VARCHAR2
   )
   IS
      l_status_flag             VARCHAR2 (1);
      l_error_message           VARCHAR2 (500);
      l_return_status_out       VARCHAR2 (1);
      l_msg_count_out           NUMBER;
      l_msg_data_out            VARCHAR2 (1000);
      l_msg_index_out           NUMBER;
      l_bank_id                 NUMBER;
      l_location_id             NUMBER;
      l_party_site_id           NUMBER;
      l_party_site_number       NUMBER;
      l_org_contact_id          NUMBER;
      l_org_party_id            NUMBER;
      l_email_cont_point_id     NUMBER;
      l_phone_cont_point_id     NUMBER;
      l_bank_msg_data           VARCHAR2 (2000);
      l_loc_msg_data            VARCHAR2 (2000);
      l_party_site_msg_data     VARCHAR2 (2000);
      l_org_cont_msg_data       VARCHAR2 (2000);
      l_phone_cont_msg_data     VARCHAR2 (2000);
      l_email_cont_msg_data     VARCHAR2 (2000);
      l_bank_ret_status         VARCHAR2 (50)           := pv_bank_ret_status;
      l_loc_ret_status          VARCHAR2 (50);
      l_site_ret_status         VARCHAR2 (50);
      l_state_ret_status        VARCHAR2 (50);
      l_upd_ret_status          VARCHAR2 (50);
      l_org_cont_ret_status     VARCHAR2 (50);
      l_phone_cont_ret_status   VARCHAR2 (50);
      l_email_cont_ret_status   VARCHAR2 (50);
      l_log_ret_stats           VARCHAR2 (50);
      l_log_err_msg             VARCHAR2 (2000);
      l_retcode                 VARCHAR2 (1);
      l_err_code                VARCHAR2 (40);
      l_err_msg                 VARCHAR2 (2000);
      l_msg_count               NUMBER;
      l_extbank_rec_type        iby_ext_bankacct_pub.extbank_rec_type;
      l_result_rec              iby_fndcpt_common_pub.result_rec_type;
-- Error Table Record Type
      source_rec                xxetn_common_error_pkg.g_source_rec_type;
   BEGIN
      l_extbank_rec_type.object_version_number := 1.0;
      l_extbank_rec_type.bank_name := 'DUMMYBR' || pv_country;
      l_extbank_rec_type.bank_number := '9999';
      l_extbank_rec_type.institution_type := 'BANK';
      l_extbank_rec_type.country_code := pv_country;
      l_extbank_rec_type.description := 'Dummy BR Bank ' || pv_country;
      --create_banks_rec.leg_description;
      l_extbank_rec_type.bank_alt_name := 'DUMMYBR' || pv_country;
                                    --  create_banks_rec.leg_bank_name_alt;
      --Call API to create external banks
      iby_ext_bankacct_pub.create_ext_bank
                                       (p_api_version        => 1.0,
                                        p_init_msg_list      => fnd_api.g_true,
                                        p_ext_bank_rec       => l_extbank_rec_type,
                                        x_bank_id            => l_bank_id,
                                        x_return_status      => l_bank_ret_status,
                                        x_msg_count          => l_msg_count,
                                        x_msg_data           => l_bank_msg_data,
                                        x_response           => l_result_rec
                                       );

      IF l_bank_ret_status = fnd_api.g_ret_sts_success
      THEN
         l_err_code := 'DUMMY BANK CREATION';
         l_err_msg := 'SUCCESS : DUMMY BANK CREATED : ' || l_bank_msg_data;
         print_log1_message (l_err_msg);
      END IF;

      IF l_msg_count > 0
      THEN
         FOR i IN 1 .. l_msg_count
         LOOP
            l_bank_msg_data :=
               fnd_msg_pub.get (p_msg_index      => i,
                                p_encoded        => fnd_api.g_false
                               );
            print_log1_message (l_bank_msg_data);
         END LOOP;
      END IF;

      COMMIT;
   EXCEPTION
      WHEN OTHERS
      THEN
         l_err_code := 'DUMMY BANK CREATION';
         l_err_msg :=
               'Error : Following error in DUMMY BANK CREATION : '
            || l_bank_msg_data;
         print_log1_message (l_err_msg);
   END create_bank;

   -- ========================
-- Procedure: CREATE BRANCH
-- =============================================================================
--   This procedure is used to run generic validations for invoice lines
--   and distribution lines
-- =============================================================================
   PROCEDURE create_branch (
      pv_country           IN       VARCHAR2,
      pv_bank_ret_status   OUT      VARCHAR2
   )
   IS
      l_status_flag             VARCHAR2 (1);
      l_error_message           VARCHAR2 (500);
      l_return_status_out       VARCHAR2 (1);
      l_msg_count_out           NUMBER;
      l_msg_data_out            VARCHAR2 (1000);
      l_msg_index_out           NUMBER;
      l_bank_id                 NUMBER;
      l_branch_id               NUMBER;
      l_location_id             NUMBER;
      l_party_site_id           NUMBER;
      l_party_site_number       NUMBER;
      l_org_contact_id          NUMBER;
      l_org_party_id            NUMBER;
      l_email_cont_point_id     NUMBER;
      l_phone_cont_point_id     NUMBER;
      l_branch_msg_data         VARCHAR2 (2000);
      l_loc_msg_data            VARCHAR2 (2000);
      l_party_site_msg_data     VARCHAR2 (2000);
      l_org_cont_msg_data       VARCHAR2 (2000);
      l_phone_cont_msg_data     VARCHAR2 (2000);
      l_email_cont_msg_data     VARCHAR2 (2000);
      l_branch_ret_status       VARCHAR2 (50)           := pv_bank_ret_status;
      l_loc_ret_status          VARCHAR2 (50);
      l_site_ret_status         VARCHAR2 (50);
      l_state_ret_status        VARCHAR2 (50);
      l_upd_ret_status          VARCHAR2 (50);
      l_org_cont_ret_status     VARCHAR2 (50);
      l_phone_cont_ret_status   VARCHAR2 (50);
      l_email_cont_ret_status   VARCHAR2 (50);
      l_log_ret_stats           VARCHAR2 (50);
      l_log_err_msg             VARCHAR2 (2000);
      l_loc_msg_count           NUMBER;
      l_retcode                 VARCHAR2 (1);
      l_err_code                VARCHAR2 (40);
      l_err_msg                 VARCHAR2 (2000);
      l_msg_count               NUMBER;
      l_extbranch_rec           iby_ext_bankacct_pub.extbankbranch_rec_type;
      l_result_rec              iby_fndcpt_common_pub.result_rec_type;
      l_br_location_rec         hz_location_v2pub.location_rec_type;
      l_br_party_site_rec       hz_party_site_v2pub.party_site_rec_type;
      l_bank_party_id           NUMBER;
-- Error Table Record Type
      source_rec                xxetn_common_error_pkg.g_source_rec_type;
   BEGIN
      SELECT bank_party_id
        INTO l_bank_party_id
        FROM ce_banks_v
       WHERE home_country = pv_country
         AND bank_name LIKE 'DUMMYBR' || pv_country || '%'
         AND ROWNUM = 1;

      l_extbranch_rec.bch_object_version_number := 1.0;
      l_extbranch_rec.branch_name := 'DUMMYBRBRANCH' || pv_country;
      l_extbranch_rec.bank_party_id := l_bank_party_id;
      l_extbranch_rec.branch_number := '99999';
      l_extbranch_rec.branch_type := 'OTHER';
      l_extbranch_rec.alternate_branch_name :=
                                           'Dummy BR Branch for' || pv_country;
      -- DBMS_OUTPUT.put_line ('l_branch_ret_status:' || l_branch_ret_status);
      iby_ext_bankacct_pub.create_ext_bank_branch
                                    (p_api_version              => 1.0,
                                     p_init_msg_list            => fnd_api.g_true,
                                     p_ext_bank_branch_rec      => l_extbranch_rec,
                                     x_branch_id                => l_branch_id,
                                     x_return_status            => l_branch_ret_status,
                                     x_msg_count                => l_msg_count,
                                     x_msg_data                 => l_branch_msg_data,
                                     x_response                 => l_result_rec
                                    );

      IF l_branch_ret_status = fnd_api.g_ret_sts_success
      THEN
         l_err_code := 'DUMMY BANK BRANCH CREATION';
         l_err_msg :=
              'SUCCESS : DUMMY BANK BRANCH CREATED : ' || l_branch_ret_status;
         print_log1_message (l_err_msg);
      END IF;

      IF l_msg_count > 0
      THEN
         FOR i IN 1 .. l_msg_count
         LOOP
            l_branch_msg_data :=
               fnd_msg_pub.get (p_msg_index      => i,
                                p_encoded        => fnd_api.g_false
                               );
            print_log1_message (l_branch_msg_data);
         END LOOP;
      END IF;

      l_br_location_rec.orig_system_reference := l_branch_id;
      l_br_location_rec.country := pv_country;
      l_br_location_rec.address1 := 'Dummy1';
      l_br_location_rec.address2 := 'Dummy2';
      l_br_location_rec.city := 'Dummy' || pv_country;
      --       l_br_location_rec.postal_code :=
                   --                       create_branches_rec.leg_zip;
          --   l_br_location_rec.state := create_branches_rec.leg_state;
           --  l_br_location_rec.province :=
             --                        create_branches_rec.leg_province;
      l_br_location_rec.created_by_module := 'CE';
      -- l_br_location_rec.county              :=
       --  create_branches_rec.county;
      hz_location_v2pub.create_location (p_init_msg_list      => fnd_api.g_false,
                                         p_location_rec       => l_br_location_rec,
                                         x_location_id        => l_location_id,
                                         x_return_status      => l_loc_ret_status,
                                         x_msg_count          => l_msg_count,
                                         x_msg_data           => l_loc_msg_data
                                        );
      print_log1_message (   'l_loc_ret_status:'
                          || l_loc_ret_status
                          || 'location id:'
                          || l_location_id
                         );
      COMMIT;
   EXCEPTION
      WHEN OTHERS
      THEN
         print_log1_message ('Error BANK BRANCH:' || SQLERRM);
   END create_branch;

   -- ========================
-- Procedure: CREATE EXTERNAL ACCOUNT
-- =============================================================================
--   This procedure is used to run generic validations for invoice lines
--   and distribution lines
-- =============================================================================
   PROCEDURE create_account (
      pv_party_id             IN       NUMBER,
      pv_country              IN       VARCHAR2,
      pv_bank_acct_id         OUT      iby_ext_bank_accounts.ext_bank_account_id%TYPE,
      pv_acct_currency_code   IN       VARCHAR2,
      pv_account_ret_status   OUT      VARCHAR2
   )
   IS
      l_status_flag                VARCHAR2 (1);
      l_error_message              VARCHAR2 (500);
      l_return_status_out          VARCHAR2 (1);
      l_msg_count_out              NUMBER;
      l_msg_data_out               VARCHAR2 (1000);
      l_msg_index_out              NUMBER;
      l_bank_id                    NUMBER;
      l_branch_id                  NUMBER;
      l_account_id                 NUMBER;
      l_account_msg_data           VARCHAR2 (2000);
      l_loc_msg_data               VARCHAR2 (2000);
      l_party_site_msg_data        VARCHAR2 (2000);
      l_account_ret_status         VARCHAR2 (50);
      l_loc_ret_status             VARCHAR2 (50);
      l_site_ret_status            VARCHAR2 (50);
      l_state_ret_status           VARCHAR2 (50);
      l_upd_ret_status             VARCHAR2 (50);
      l_log_ret_stats              VARCHAR2 (50);
      l_log_err_msg                VARCHAR2 (2000);
      l_loc_msg_count              NUMBER;
      l_retcode                    VARCHAR2 (1);
      l_err_code                   VARCHAR2 (40);
      l_err_msg                    VARCHAR2 (2000);
      l_payee_err_msg              VARCHAR2 (2000);
      l_msg_count                  NUMBER;
      l_payee_msg_count            NUMBER;
      l_start_date                 DATE;
      l_end_date                   DATE;
      l_ext_bank_acct_rec          iby_ext_bankacct_pub.extbankacct_rec_type;
      l_instrument_rec             iby_fndcpt_setup_pub.pmtinstrument_rec_type;
      l_payee_rec                  iby_fndcpt_common_pub.payercontext_rec_type;
      l_assignment_attribs_rec     iby_fndcpt_setup_pub.pmtinstrassignment_rec_type;
      l_bank_acct_id               iby_ext_bank_accounts.ext_bank_account_id%TYPE;
      l_temp_ext_bank_acct_id      iby_ext_bank_accounts.ext_bank_account_id%TYPE;
      l_temp_ext_bank_acct_count   NUMBER;
      l_bank_acct_count            NUMBER;
      l_ext_payee_id               NUMBER;
      l_assign_id                  NUMBER;
      l_result_rec                 iby_fndcpt_common_pub.result_rec_type;
      l_payee_result_rec           iby_fndcpt_common_pub.result_rec_type;
      l_priority                   NUMBER;
      l_bank_acct_priority         NUMBER;
      l_bank_acct_priority_count   NUMBER;
      l_joint_acct_owner_id        NUMBER;
      l_payee_ret_status           VARCHAR2 (50);
      l_owner_check                NUMBER;
-- Error Table Record Type
      source_rec                   xxetn_common_error_pkg.g_source_rec_type;
      l_ext_acct_seq               NUMBER                                := 0;
   BEGIN
      l_account_ret_status := NULL;

      SELECT bank_party_id, branch_party_id
        INTO l_ext_bank_acct_rec.bank_id, l_ext_bank_acct_rec.branch_id
        FROM ce_bank_branches_v
       WHERE bank_home_country = pv_country
         AND bank_branch_name LIKE 'DUMMYBRBRANCH' || pv_country || '%'
         AND ROWNUM = 1;

      BEGIN
         SELECT xxconv.xxar_br_ext_bank_acct_s.NEXTVAL
           INTO l_ext_acct_seq
           FROM DUAL;
      EXCEPTION
         WHEN OTHERS
         THEN
            l_ext_acct_seq := 0;
      END;

      l_ext_bank_acct_rec.object_version_number := 1.0;
      l_ext_bank_acct_rec.acct_owner_party_id := pv_party_id;
      -- Customer Party Id      --    Added V1.17
      l_ext_bank_acct_rec.bank_account_num := 'DUMMYBRACCT' || l_ext_acct_seq;
      l_ext_bank_acct_rec.bank_account_name :=
                                 'DUMMYBRACCT' || pv_country || l_ext_acct_seq;
      l_ext_bank_acct_rec.start_date := '01-JAN-2001';
      l_ext_bank_acct_rec.currency := NULL ; --pv_acct_currency_code;
      l_ext_bank_acct_rec.country_code := pv_country;
      l_ext_bank_acct_rec.alternate_acct_name :=
                      'Dummy Bank Account for' || pv_country || l_ext_acct_seq;
      l_ext_bank_acct_rec.acct_type := 'BANK';
      print_log1_message ('Bank id : ' || l_ext_bank_acct_rec.bank_id);
      print_log1_message ('branch_id id : ' || l_ext_bank_acct_rec.branch_id);
      print_log1_message (   'Bank id : '
                          || l_ext_bank_acct_rec.acct_owner_party_id
                         );
      print_log1_message ('Bank id : ' || l_ext_bank_acct_rec.bank_account_num);
      print_log1_message ('Bank id : '
                          || l_ext_bank_acct_rec.bank_account_name
                         );
      print_log1_message ('Bank id : ' || l_ext_bank_acct_rec.start_date);
      print_log1_message ('Bank id : ' || l_ext_bank_acct_rec.currency);
      print_log1_message (   'Bank id : '
                          || l_ext_bank_acct_rec.alternate_acct_name
                         );
      print_log1_message ('Bank id : ' || l_ext_bank_acct_rec.acct_type);
      iby_ext_bankacct_pub.create_ext_bank_acct
                                  (p_api_version            => 1.0,
                                   p_init_msg_list          => fnd_api.g_true,
                                   p_ext_bank_acct_rec      => l_ext_bank_acct_rec,
                                   x_acct_id                => l_bank_acct_id,
                                   x_return_status          => l_account_ret_status,
                                   x_msg_count              => l_msg_count,
                                   x_msg_data               => l_account_msg_data,
                                   x_response               => l_result_rec
                                  );

      IF l_account_ret_status = fnd_api.g_ret_sts_success
      THEN
         l_err_code := 'DUMMY BANK ACCOUNT CREATION';
         l_err_msg :=
               'SUCCESS : DUMMY BANK ACCOUNT CREATED : '
            || l_account_ret_status
            || 'l_bank_acct_id:'
            || l_bank_acct_id;
         print_log1_message (l_err_msg);
      END IF;

      IF l_msg_count > 0
      THEN
         FOR i IN 1 .. l_msg_count
         LOOP
            l_account_msg_data :=
               fnd_msg_pub.get (p_msg_index      => i,
                                p_encoded        => fnd_api.g_false
                               );
            print_log1_message ('l_branch_msg_data:' || l_account_msg_data);
         END LOOP;
      END IF;

      pv_bank_acct_id := l_bank_acct_id;
      pv_account_ret_status := l_account_ret_status;
      COMMIT;
   EXCEPTION
      WHEN OTHERS
      THEN
         print_log1_message
                           (   'Error while creating external BANK  ACCOUNT:'
                            || SQLERRM
                           );
   END create_account;

-- ========================
-- Procedure: VALIDATE_INVOICE
-- =============================================================================
--   This procedure is used to run generic validations for invoice lines
--   and distribution lines
-- =============================================================================
--
   PROCEDURE validate_invoice (piv_period_name IN VARCHAR2)
   IS
      l_err_code                    VARCHAR2 (40);
      l_err_msg                     VARCHAR2 (2000);
      l_upd_ret_status              VARCHAR2 (50)                     := NULL;
      l_log_ret_status              VARCHAR2 (50)                     := NULL;
      l_log_err_msg                 VARCHAR2 (2000);
      l_customer_id                 xxconv.xxar_br_invoices_stg.system_bill_customer_id%TYPE;
      l_trx_number                  xxconv.xxar_br_invoices_stg.trx_number%TYPE
                                                                        := -1;
      l_trx_type_id                 xxconv.xxar_br_invoices_stg.transaction_type_id%TYPE;
      l_org_id                      xxconv.xxar_br_invoices_stg.org_id%TYPE;
      l_line_number                 xxconv.xxar_br_invoices_stg.leg_line_number%TYPE;
      l_valid_flag                  VARCHAR2 (1)                       := 'Y';
      l_ledger_id                   gl_ledgers.ledger_id%TYPE;
      l_validate_flag               VARCHAR2 (1)                       := 'S';
      l_validate_line_flag          VARCHAR2 (1)                       := 'S';
      --      l_dist_flag                   VARCHAR2 (1);
      l_leg_int_line_attribute1     xxconv.xxar_br_invoices_stg.leg_interface_line_attribute1%TYPE;
      l_leg_int_line_attribute2     xxconv.xxar_br_invoices_stg.leg_interface_line_attribute2%TYPE;
      l_leg_int_line_attribute3     xxconv.xxar_br_invoices_stg.leg_interface_line_attribute3%TYPE;
      l_leg_int_line_attribute4     xxconv.xxar_br_invoices_stg.leg_interface_line_attribute4%TYPE;
      l_leg_int_line_attribute5     xxconv.xxar_br_invoices_stg.leg_interface_line_attribute5%TYPE;
      l_leg_int_line_attribute6     xxconv.xxar_br_invoices_stg.leg_interface_line_attribute6%TYPE;
      l_leg_int_line_attribute7     xxconv.xxar_br_invoices_stg.leg_interface_line_attribute7%TYPE;
      l_leg_int_line_attribute8     xxconv.xxar_br_invoices_stg.leg_interface_line_attribute8%TYPE;
      l_leg_int_line_attribute9     xxconv.xxar_br_invoices_stg.leg_interface_line_attribute9%TYPE;
      l_leg_int_line_attribute10    xxconv.xxar_br_invoices_stg.leg_interface_line_attribute10%TYPE;
      l_leg_int_line_attribute11    xxconv.xxar_br_invoices_stg.leg_interface_line_attribute11%TYPE;
      l_leg_int_line_attribute12    xxconv.xxar_br_invoices_stg.leg_interface_line_attribute12%TYPE;
      l_leg_int_line_attribute13    xxconv.xxar_br_invoices_stg.leg_interface_line_attribute13%TYPE;
      l_leg_int_line_attribute14    VARCHAR2 (240);
      l_leg_int_line_attribute15    VARCHAR2 (240);
      l_src_concatenated_segments   VARCHAR2 (1000);
      l_tgt_concatenated_segments   VARCHAR2 (1000);
      l_inv_flag                    VARCHAR2 (1);
      l_cm_status_flag              VARCHAR2 (1);
      l_gl_date                     DATE;
      x_out_acc_rec                 xxetn_common_pkg.g_rec_type;
      x_ccid                        NUMBER;
      l_rec_flag                    VARCHAR2 (1)                       := 'N';
      l_rev_flag                    VARCHAR2 (1)                       := 'N';
      l_assign_flag                 VARCHAR2 (1)                       := 'N';
      l_rec_dist_idx                NUMBER                            := NULL;
      l_rec_int_line_attribute1     xxconv.xxar_br_invoices_stg.leg_interface_line_attribute1%TYPE;
      l_rec_int_line_attribute2     xxconv.xxar_br_invoices_stg.leg_interface_line_attribute2%TYPE;
      l_rec_int_line_attribute3     xxconv.xxar_br_invoices_stg.leg_interface_line_attribute3%TYPE;
      l_rec_int_line_attribute4     xxconv.xxar_br_invoices_stg.leg_interface_line_attribute4%TYPE;
      l_rec_int_line_attribute5     xxconv.xxar_br_invoices_stg.leg_interface_line_attribute5%TYPE;
      l_rec_int_line_attribute6     xxconv.xxar_br_invoices_stg.leg_interface_line_attribute6%TYPE;
      l_rec_int_line_attribute7     xxconv.xxar_br_invoices_stg.leg_interface_line_attribute7%TYPE;
      l_rec_int_line_attribute8     xxconv.xxar_br_invoices_stg.leg_interface_line_attribute8%TYPE;
      l_rec_int_line_attribute9     xxconv.xxar_br_invoices_stg.leg_interface_line_attribute9%TYPE;
      l_rec_int_line_attribute10    xxconv.xxar_br_invoices_stg.leg_interface_line_attribute10%TYPE;
      l_rec_int_line_attribute11    xxconv.xxar_br_invoices_stg.leg_interface_line_attribute11%TYPE;
      l_rec_int_line_attribute12    xxconv.xxar_br_invoices_stg.leg_interface_line_attribute12%TYPE;
      l_rec_int_line_attribute13    xxconv.xxar_br_invoices_stg.leg_interface_line_attribute13%TYPE;
      l_rec_int_line_attribute14    VARCHAR2 (240);
      l_rec_int_line_attribute15    VARCHAR2 (240);
      l_dist_cust_trx_id            NUMBER;
      l_dist_org_id                 NUMBER;
      l_limit                       NUMBER                            := 1000;
      l_line_limit                  NUMBER                            := 1000;
      l_dist_limit                  NUMBER                            := 1000;
      l_leg_line_amount             NUMBER                               := 0;
      l_r12_org_id                  NUMBER;
      l_ou_name                     hr_operating_units.NAME%TYPE;
      l_sob_id                      NUMBER;
      l_func_curr                   gl_ledgers.currency_code%TYPE;
      l_r12_cust_id                 NUMBER;
      l_bill_to_addr                NUMBER;
      l_ship_to_addr                NUMBER;
      l_trx_type_name               VARCHAR2 (100);
      l_gl_error                    VARCHAR2 (1);
      l_valid_cust_flag             VARCHAR2 (1);
      l_curr_code                   VARCHAR2 (30);
      l_trx_type                    ra_cust_trx_types_all.NAME%TYPE;
      l_cm_term_error               VARCHAR2 (1);
      l_term_name                   ra_terms_tl.NAME%TYPE;
      l_term_id                     NUMBER;
      l_org_name                    hr_operating_units.NAME%TYPE;
      l_gl_status                   gl_period_statuses.closing_status%TYPE;
      l_tax_code_r12                zx_rates_b.tax_rate_code%TYPE;
      l_tax_r12                     zx_rates_b.tax%TYPE;
      l_tax_regime_code             zx_rates_b.tax_regime_code%TYPE;
      l_tax_rate_code               zx_rates_b.tax_rate_code%TYPE;
      l_tax                         zx_rates_b.tax%TYPE;
      l_tax_status_code             zx_rates_b.tax_status_code%TYPE;
      l_tax_jurisdiction_code       zx_rates_b.tax_jurisdiction_code%TYPE;
      l_header_attr4                VARCHAR2 (240);
      l_header_attr8                VARCHAR2 (240);
      --Ver 1.6 changes start
      l_oper_unit                   xxetn_map_unit.operating_unit%TYPE;
      l_rec                         xxetn_map_util.g_input_rec;
      --Ver 1.6 changes end

      --Ver 1.7 changes start
      l_inv_rule_id                 NUMBER;
      l_acc_rule_id                 NUMBER;
      l_acc_rule_exists             NUMBER                               := 0;
      l_inv_rule_err                NUMBER                               := 0;
      l_inv_rule_err_msg            VARCHAR2 (500);
      l_valerr_cnt                  NUMBER                               := 1;
      --Ver 1.7 changes end

      --Ver 1.10 changes start
      l_batch_error                 VARCHAR2 (1);
      l_batch_source                VARCHAR2 (240);
      l_source_status               NUMBER;
      l_plant_credit_office         VARCHAR2 (240);
      --Ver 1.10 changes end
      l_cust_site_use_rec           hz_cust_account_site_v2pub.cust_site_use_rec_type;
      l_customer_profile_rec        hz_customer_profile_v2pub.customer_profile_rec_type;
      l_drawee_to_addr              NUMBER;
      l_count                       NUMBER;
      l_country                     VARCHAR2 (240);
      l_bnk_brnch                   NUMBER;
      l_bnk                         NUMBER;
      l_acct_currency_code          VARCHAR2 (240);
      l_bank_ret_status             VARCHAR2 (900);
      l_acct_prty_id                NUMBER;
      l_account_site_use_id         NUMBER;
      l_bank_branch_ret_status      VARCHAR2 (900);
      l_account_ret_status          VARCHAR2 (900);
      l_pymt_site_use_id            NUMBER;
      l_receipt_method_id           NUMBER;
      l_site_use_id                 NUMBER;
      l_return_status               VARCHAR2 (2000);
      l_msg_count                   NUMBER;
      l_msg_data                    VARCHAR2 (2000);
    l_msg              VARCHAR2 (2000); --added for v1.8
      --DUMMY ACCOUNT CREATION---
      l_ext_bank_acct_rec           iby_ext_bankacct_pub.extbankacct_rec_type;
      l_bank_acct_id                iby_ext_bank_accounts.ext_bank_account_id%TYPE;
      l_account_msg_data            VARCHAR2 (2000);
      l_result_rec                  iby_fndcpt_common_pub.result_rec_type;
      l_instrument_rec              iby_fndcpt_setup_pub.pmtinstrument_rec_type;
      l_payee_rec                   iby_fndcpt_common_pub.payercontext_rec_type;
      l_assignment_attribs_rec      iby_fndcpt_setup_pub.pmtinstrassignment_rec_type;
      l_payee_ret_status            VARCHAR2 (50);
      l_payee_msg_count             NUMBER;
      l_payee_err_msg               VARCHAR2 (2000);
      l_assign_id                   NUMBER;
      l_payee_result_rec            iby_fndcpt_common_pub.result_rec_type;
      --RECEPT METHOD---
      l_pay_method_rec              hz_payment_method_pub.payment_method_rec_type;
      l_cust_receipt_method_id      NUMBER;

    --v1.2 Defect 5392
    l_site_orig_sys_ref          hz_cust_acct_sites_all.orig_system_reference%type;

      TYPE l_inv_rec IS RECORD (
         leg_trx_number                  xxconv.xxar_br_invoices_stg.leg_trx_number%TYPE,
         leg_currency_code               xxconv.xxar_br_invoices_stg.leg_currency_code%TYPE,
         leg_customer_number             xxconv.xxar_br_invoices_stg.leg_customer_number%TYPE,
         leg_bill_to_address             xxconv.xxar_br_invoices_stg.leg_bill_to_address%TYPE,
         leg_ship_to_address             xxconv.xxar_br_invoices_stg.leg_ship_to_address%TYPE,
         leg_term_name                   xxconv.xxar_br_invoices_stg.leg_term_name%TYPE,
         leg_operating_unit              xxconv.xxar_br_invoices_stg.leg_operating_unit%TYPE,
         leg_trx_date                    xxconv.xxar_br_invoices_stg.leg_trx_date%TYPE,
         leg_gl_date                     xxconv.xxar_br_invoices_stg.leg_gl_date%TYPE,
         leg_batch_source_name           xxconv.xxar_br_invoices_stg.leg_batch_source_name%TYPE,
         leg_customer_trx_id             xxconv.xxar_br_invoices_stg.leg_customer_trx_id%TYPE,
         leg_cust_trx_type_name          xxconv.xxar_br_invoices_stg.leg_cust_trx_type_name%TYPE,
         leg_source_system               xxconv.xxar_br_invoices_stg.leg_source_system%TYPE,
         leg_purchase_order              xxconv.xxar_br_invoices_stg.leg_purchase_order%TYPE,
         leg_header_attribute_category   xxconv.xxar_br_invoices_stg.leg_header_attribute_category%TYPE,
         leg_header_attribute1           xxconv.xxar_br_invoices_stg.leg_header_attribute1%TYPE,
         leg_header_attribute2           xxconv.xxar_br_invoices_stg.leg_header_attribute2%TYPE,
         leg_header_attribute3           xxconv.xxar_br_invoices_stg.leg_header_attribute3%TYPE,
         leg_header_attribute4           xxconv.xxar_br_invoices_stg.leg_header_attribute4%TYPE,
         leg_header_attribute5           xxconv.xxar_br_invoices_stg.leg_header_attribute5%TYPE,
         leg_header_attribute6           xxconv.xxar_br_invoices_stg.leg_header_attribute6%TYPE,
         leg_header_attribute7           xxconv.xxar_br_invoices_stg.leg_header_attribute7%TYPE,
         leg_header_attribute8           xxconv.xxar_br_invoices_stg.leg_header_attribute8%TYPE,
         leg_header_attribute9           xxconv.xxar_br_invoices_stg.leg_header_attribute9%TYPE,
         leg_header_attribute10          xxconv.xxar_br_invoices_stg.leg_header_attribute10%TYPE,
         leg_header_attribute11          xxconv.xxar_br_invoices_stg.leg_header_attribute11%TYPE,
         leg_header_attribute12          xxconv.xxar_br_invoices_stg.leg_header_attribute12%TYPE,
         leg_header_attribute13          xxconv.xxar_br_invoices_stg.leg_header_attribute13%TYPE,
         leg_header_attribute14          xxconv.xxar_br_invoices_stg.leg_header_attribute14%TYPE,
         leg_header_attribute15          xxconv.xxar_br_invoices_stg.leg_header_attribute15%TYPE
      );

      TYPE l_inv_tab IS TABLE OF l_inv_rec;

      val_inv_rec                   l_inv_tab;

      TYPE l_inv_det_tab IS TABLE OF xxconv.xxar_br_invoices_stg%ROWTYPE;

      val_inv_det_rec               l_inv_det_tab;

      TYPE l_dist_rec IS RECORD (
         interface_txn_id               xxconv.xxar_br_invoices_dist_stg.interface_txn_id%TYPE,
         leg_percent                    xxconv.xxar_br_invoices_dist_stg.leg_percent%TYPE,
         leg_account_class              xxconv.xxar_br_invoices_dist_stg.leg_account_class%TYPE,
         leg_dist_segment1              xxconv.xxar_br_invoices_dist_stg.leg_dist_segment1%TYPE,
         leg_dist_segment2              xxconv.xxar_br_invoices_dist_stg.leg_dist_segment2%TYPE,
         leg_dist_segment3              xxconv.xxar_br_invoices_dist_stg.leg_dist_segment3%TYPE,
         leg_dist_segment4              xxconv.xxar_br_invoices_dist_stg.leg_dist_segment4%TYPE,
         leg_dist_segment5              xxconv.xxar_br_invoices_dist_stg.leg_dist_segment5%TYPE,
         leg_dist_segment6              xxconv.xxar_br_invoices_dist_stg.leg_dist_segment6%TYPE,
         leg_dist_segment7              xxconv.xxar_br_invoices_dist_stg.leg_dist_segment7%TYPE,
         leg_org_name                   xxconv.xxar_br_invoices_dist_stg.leg_org_name%TYPE,
         leg_operating_unit             xxconv.xxar_br_invoices_stg.leg_operating_unit%TYPE,
         org_id                         xxconv.xxar_br_invoices_stg.org_id%TYPE,
         leg_customer_trx_id            xxconv.xxar_br_invoices_dist_stg.leg_customer_trx_id%TYPE,
         leg_cust_trx_line_id           xxconv.xxar_br_invoices_dist_stg.leg_cust_trx_line_id%TYPE,
         leg_cust_trx_line_gl_dist_id   xxconv.xxar_br_invoices_dist_stg.leg_cust_trx_line_gl_dist_id%TYPE,
         leg_accounted_amount           xxconv.xxar_br_invoices_dist_stg.leg_accounted_amount%TYPE,
         interface_line_context         xxconv.xxar_br_invoices_stg.interface_line_context%TYPE,
         interface_line_attribute1      xxconv.xxar_br_invoices_stg.interface_line_attribute1%TYPE,
         interface_line_attribute2      xxconv.xxar_br_invoices_stg.interface_line_attribute2%TYPE,
         interface_line_attribute3      xxconv.xxar_br_invoices_stg.interface_line_attribute3%TYPE,
         interface_line_attribute4      xxconv.xxar_br_invoices_stg.interface_line_attribute4%TYPE,
         interface_line_attribute5      xxconv.xxar_br_invoices_stg.interface_line_attribute5%TYPE,
         interface_line_attribute6      xxconv.xxar_br_invoices_stg.interface_line_attribute6%TYPE,
         interface_line_attribute7      xxconv.xxar_br_invoices_stg.interface_line_attribute7%TYPE,
         interface_line_attribute8      xxconv.xxar_br_invoices_stg.interface_line_attribute8%TYPE,
         interface_line_attribute9      xxconv.xxar_br_invoices_stg.interface_line_attribute9%TYPE,
         interface_line_attribute10     xxconv.xxar_br_invoices_stg.interface_line_attribute10%TYPE,
         interface_line_attribute11     xxconv.xxar_br_invoices_stg.interface_line_attribute11%TYPE,
         interface_line_attribute12     xxconv.xxar_br_invoices_stg.interface_line_attribute12%TYPE,
         interface_line_attribute13     xxconv.xxar_br_invoices_stg.interface_line_attribute13%TYPE,
         interface_line_attribute14     xxconv.xxar_br_invoices_stg.interface_line_attribute14%TYPE,
         interface_line_attribute15     xxconv.xxar_br_invoices_stg.interface_line_attribute15%TYPE,
         leg_cust_trx_type_name         xxconv.xxar_br_invoices_stg.leg_cust_trx_type_name%TYPE,
         leg_trx_number                 xxconv.xxar_br_invoices_stg.leg_trx_number%TYPE,
         leg_line_type                  xxconv.xxar_br_invoices_stg.leg_line_type%TYPE
      );

      TYPE l_dist_tab IS TABLE OF l_dist_rec;

      val_dist_rec                  l_dist_tab;

      TYPE l_line_rec IS RECORD (
         leg_line_amount              NUMBER,
         interface_line_attribute1    VARCHAR2 (240),
         interface_line_attribute15   VARCHAR2 (240)
      );

      TYPE l_line_dff_tab IS TABLE OF l_line_rec
         INDEX BY VARCHAR2 (100);

      l_line_dff_rec                l_line_dff_tab;

      CURSOR val_inv_cur
      IS
         SELECT   leg_trx_number, leg_currency_code, leg_customer_number,
                  leg_bill_to_address, leg_ship_to_address, leg_term_name,
                  leg_operating_unit, leg_trx_date, leg_gl_date,
                  leg_batch_source_name, leg_customer_trx_id,
                  leg_cust_trx_type_name, leg_source_system,
                  leg_purchase_order, leg_header_attribute_category,
                  leg_header_attribute1, leg_header_attribute2,
                  leg_header_attribute3, leg_header_attribute4,
                  leg_header_attribute5, leg_header_attribute6,
                  leg_header_attribute7, leg_header_attribute8,
                  leg_header_attribute9, leg_header_attribute10,
                  leg_header_attribute11, leg_header_attribute12,
                  leg_header_attribute13, leg_header_attribute14,
                  leg_header_attribute15
             FROM xxconv.xxar_br_invoices_stg
            WHERE 1 = 1
              AND process_flag = 'N'
              AND NVL (ERROR_TYPE, 'NO_ERR_TYPE') <> 'ERR_IMP'
              AND batch_id = g_new_batch_id
         GROUP BY leg_trx_number,
                  leg_currency_code,
                  leg_customer_number,
                  leg_bill_to_address,
                  leg_ship_to_address,
                  leg_term_name,
                  leg_operating_unit,
                  leg_trx_date,
                  leg_gl_date,
                  leg_batch_source_name,
                  leg_customer_trx_id,
                  leg_cust_trx_type_name,
                  leg_source_system,
                  leg_purchase_order,
                  leg_header_attribute_category,
                  leg_header_attribute1,
                  leg_header_attribute2,
                  leg_header_attribute3,
                  leg_header_attribute4,
                  leg_header_attribute5,
                  leg_header_attribute6,
                  leg_header_attribute7,
                  leg_header_attribute8,
                  leg_header_attribute9,
                  leg_header_attribute10,
                  leg_header_attribute11,
                  leg_header_attribute12,
                  leg_header_attribute13,
                  leg_header_attribute14,
                  leg_header_attribute15
         ORDER BY leg_customer_trx_id;

      CURSOR val_inv_det_cur
      IS
         SELECT   *
             FROM xxconv.xxar_br_invoices_stg
            WHERE process_flag IN ('N', 'E')
              AND NVL (ERROR_TYPE, 'NO_ERR_TYPE') <> 'ERR_IMP'
              AND batch_id = g_new_batch_id
              AND run_sequence_id = g_new_run_seq_id
         ORDER BY leg_trx_number, leg_line_type, leg_line_number;

      CURSOR val_dist_cur
      IS
         SELECT   xds.interface_txn_id, xds.leg_percent,
                  xds.leg_account_class, xds.leg_dist_segment1,
                  xds.leg_dist_segment2, xds.leg_dist_segment3,
                  xds.leg_dist_segment4, xds.leg_dist_segment5,
                  xds.leg_dist_segment6, xds.leg_dist_segment7,
                  xds.leg_org_name, xis.leg_operating_unit, xis.org_id,
                  xds.leg_customer_trx_id, xds.leg_cust_trx_line_id,
                  xds.leg_cust_trx_line_gl_dist_id, xds.leg_accounted_amount,
                  xis.interface_line_context, xis.interface_line_attribute1,
                  xis.interface_line_attribute2,
                  xis.interface_line_attribute3,
                  xis.interface_line_attribute4,
                  xis.interface_line_attribute5,
                  xis.interface_line_attribute6,
                  xis.interface_line_attribute7,
                  xis.interface_line_attribute8,
                  xis.interface_line_attribute9,
                  xis.interface_line_attribute10,
                  xis.interface_line_attribute11,
                  xis.interface_line_attribute12,
                  xis.interface_line_attribute13,
                  xis.interface_line_attribute14,
                  xis.interface_line_attribute15, xis.leg_cust_trx_type_name,
                  xis.leg_trx_number, xis.leg_line_type
             FROM xxconv.xxar_br_invoices_dist_stg xds,
                  xxconv.xxar_br_invoices_stg xis
            WHERE xds.process_flag = 'N'
              AND NVL (xds.ERROR_TYPE, 'NO_ERR_TYPE') <> 'ERR_IMP'
              AND xds.batch_id = g_new_batch_id
              AND xds.run_sequence_id = g_new_run_seq_id         --performance
              AND xds.leg_customer_trx_id = xis.leg_customer_trx_id
              AND xds.leg_cust_trx_line_id = xis.leg_cust_trx_line_id
              AND xds.leg_account_class NOT IN ('REC', 'ROUND', 'UNEARN')
              AND xis.process_flag = 'V'
         UNION
         SELECT   xds.interface_txn_id, xds.leg_percent,
                  xds.leg_account_class, xds.leg_dist_segment1,
                  xds.leg_dist_segment2, xds.leg_dist_segment3,
                  xds.leg_dist_segment4, xds.leg_dist_segment5,
                  xds.leg_dist_segment6, xds.leg_dist_segment7,
                  xds.leg_org_name, NULL, NULL, xds.leg_customer_trx_id,
                  xds.leg_cust_trx_line_id, xds.leg_cust_trx_line_gl_dist_id,
                  xds.leg_accounted_amount, NULL, NULL, NULL, NULL, NULL,
                  NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
                  NULL, NULL, NULL, NULL
             FROM xxconv.xxar_br_invoices_dist_stg xds
            WHERE xds.process_flag = 'N'
              AND NVL (xds.ERROR_TYPE, 'NO_ERR_TYPE') <> 'ERR_IMP'
              AND xds.batch_id = g_new_batch_id
              AND xds.run_sequence_id = g_new_run_seq_id         --performance
              AND xds.leg_account_class = 'REC'
              --   AND    xds.leg_org_name = xis.leg_operating_unit
              AND EXISTS (
                     SELECT 1
                       FROM xxconv.xxar_br_invoices_stg xis
                      WHERE xds.leg_customer_trx_id = xis.leg_customer_trx_id
                        AND xis.process_flag = 'V')
         ORDER BY leg_customer_trx_id;

      CURSOR org_cur
      IS
         SELECT DISTINCT leg_dist_segment1
                    FROM xxconv.xxar_br_invoices_dist_stg
                   WHERE batch_id = g_new_batch_id
                     AND run_sequence_id = g_new_run_seq_id
                     AND leg_account_class = 'REC';

      --Ver 1.10 Changes end
      --Ver1.6 Changes end
      CURSOR customer_cur
      IS
         SELECT DISTINCT leg_customer_number, leg_bill_to_address,
                         leg_ship_to_address, org_id
                    FROM xxconv.xxar_br_invoices_stg
                   WHERE batch_id = g_new_batch_id
                     AND run_sequence_id = g_new_run_seq_id;

      CURSOR currency_cur
      IS
         SELECT DISTINCT leg_currency_code
                    FROM xxconv.xxar_br_invoices_stg
                   WHERE batch_id = g_new_batch_id
                     AND run_sequence_id = g_new_run_seq_id;

      CURSOR trx_type_cur
      IS
         SELECT DISTINCT leg_cust_trx_type_name, org_id
                    FROM xxconv.xxar_br_invoices_stg
                   WHERE batch_id = g_new_batch_id
                     AND run_sequence_id = g_new_run_seq_id;

      CURSOR term_cur
      IS
         SELECT DISTINCT leg_term_name
                    FROM xxconv.xxar_br_invoices_stg
                   WHERE batch_id = g_new_batch_id
                     AND run_sequence_id = g_new_run_seq_id;

      --          process_flag in ( 'N','E')
      CURSOR gl_date_cur
      IS
         SELECT DISTINCT leg_gl_date, ledger_id
                    FROM xxconv.xxar_br_invoices_stg
                   WHERE batch_id = g_new_batch_id
                     AND run_sequence_id = g_new_run_seq_id
                     AND leg_cust_trx_type_name LIKE '%OKS%';

      CURSOR tax_cur
      IS
         SELECT DISTINCT leg_tax_code, org_id
                    FROM xxconv.xxar_br_invoices_stg
                   WHERE batch_id = g_new_batch_id
                     AND run_sequence_id = g_new_run_seq_id;

      --Ver1.7 changes start
      CURSOR accounting_cur
      IS
         SELECT DISTINCT leg_agreement_name
                    FROM xxconv.xxar_br_invoices_stg
                   WHERE batch_id = g_new_batch_id
                     AND run_sequence_id = g_new_run_seq_id;
   --Ver1.7 changes end
   BEGIN
------------------------------------------------------------------ --perf
      --print_log_message ('Validating R12 operating unit ' || 'start');
      FOR org_rec IN org_cur
      LOOP
         l_r12_org_id := NULL;
         l_ou_name := NULL;
         l_sob_id := NULL;
         l_func_curr := NULL;
         l_ledger_id := NULL;
         l_org_name := NULL;
         l_gl_error := 'Y';
         l_batch_error := 'Y';
         l_batch_source := NULL;

         BEGIN
            -- print_log_message ('Validating R12 operating unit ' || 'test');
             --            print_log_message ('Validating legacy operating unit ' || org_rec.leg_operating_unit);
            l_rec.site := org_rec.leg_dist_segment1;
            l_org_name := xxetn_map_util.get_value (l_rec).operating_unit;

            --   print_log_message ('Validating R12 operating unit ' || 'test');
            IF l_org_name IS NULL
            THEN
               FOR r_org_ref_err_rec IN
                  (SELECT interface_txn_id
                     FROM xxconv.xxar_br_invoices_dist_stg xis
                    WHERE leg_customer_trx_id IN (
                             SELECT leg_customer_trx_id
                               FROM xxconv.xxar_br_invoices_dist_stg xis
                              WHERE leg_dist_segment1 =
                                                    org_rec.leg_dist_segment1
                                AND batch_id = g_new_batch_id
                                AND run_sequence_id = g_new_run_seq_id
                                AND leg_account_class = 'REC')
                      AND batch_id = g_new_batch_id
                      AND run_sequence_id = g_new_run_seq_id)
               --Ver 1.10 Changes end
               LOOP
                  l_err_code := 'ETN_BR_OPERATING UNIT_ERROR';
                  l_err_msg :=
                     'Error : Cross reference not defined for operating unit in XXETN_MAP_UNIT table';
                  g_retcode := 1;
                  log_errors
                     (pin_transaction_id           => r_org_ref_err_rec.interface_txn_id,
                      piv_source_column_name       => 'Legacy Segment 1',
                      piv_source_column_value      => org_rec.leg_dist_segment1,
                      piv_error_type               => 'ERR_VAL',
                      piv_error_code               => l_err_code,
                      piv_error_message            => l_err_msg,
                      pov_return_status            => l_log_ret_status,
                      pov_error_msg                => l_log_err_msg
                     );
               END LOOP;

               UPDATE xxconv.xxar_br_invoices_dist_stg
                  SET process_flag = 'E',
                      ERROR_TYPE = 'ERR_VAL',
                      last_update_date = SYSDATE,
                      last_updated_by = g_last_updated_by,
                      last_update_login = g_login_id
                WHERE leg_customer_trx_id IN (
                         SELECT leg_customer_trx_id
                           FROM xxconv.xxar_br_invoices_dist_stg xis
                          WHERE leg_dist_segment1 = org_rec.leg_dist_segment1
                            AND batch_id = g_new_batch_id
                            AND run_sequence_id = g_new_run_seq_id
                            AND leg_account_class = 'REC')
                  AND batch_id = g_new_batch_id
                  AND run_sequence_id = g_new_run_seq_id;

               UPDATE xxconv.xxar_br_invoices_stg xis
                  SET xis.process_flag = 'E',
                      xis.ERROR_TYPE = 'ERR_VAL',
                      xis.last_update_date = SYSDATE,
                      xis.last_updated_by = g_last_updated_by,
                      xis.last_update_login = g_login_id
                WHERE xis.leg_customer_trx_id IN (
                         SELECT xds.leg_customer_trx_id
                           FROM xxconv.xxar_br_invoices_dist_stg xds
                          WHERE xds.leg_customer_trx_id IN (
                                   SELECT xds1.leg_customer_trx_id
                                     FROM xxconv.xxar_br_invoices_dist_stg xds1
                                    WHERE xds1.leg_dist_segment1 =
                                                     org_rec.leg_dist_segment1
                                      AND xds1.batch_id = g_new_batch_id
                                      AND xds1.run_sequence_id =
                                                              g_new_run_seq_id
                                      AND xds1.leg_account_class = 'REC')
                            AND xds.batch_id = g_new_batch_id
                            AND xds.run_sequence_id = g_new_run_seq_id)
                  AND xis.batch_id = g_new_batch_id
                  AND xis.run_sequence_id = g_new_run_seq_id;

               -- ver1.10 changes end
               COMMIT;
            END IF;
         EXCEPTION
            WHEN OTHERS
            THEN
               l_err_code := 'ETN_BR_PROCEDURE_EXCEPTION';
               l_err_msg :=
                     'Error : Error updating staging table for operating unit'
                  || SUBSTR (SQLERRM, 1, 150);
               g_retcode := 2;

               FOR r_org_ref_err_rec1 IN
                  (SELECT interface_txn_id
                     FROM xxconv.xxar_br_invoices_dist_stg xis
                    WHERE leg_customer_trx_id IN (
                             SELECT leg_customer_trx_id
                               FROM xxconv.xxar_br_invoices_dist_stg xis
                              WHERE leg_dist_segment1 =
                                                     org_rec.leg_dist_segment1
                                AND batch_id = g_new_batch_id
                                AND run_sequence_id = g_new_run_seq_id
                                AND leg_account_class = 'REC')
                      AND batch_id = g_new_batch_id
                      AND run_sequence_id = g_new_run_seq_id)
               LOOP
                  --Ver 1.10 Changes end
                  log_errors
                     (pin_transaction_id           => r_org_ref_err_rec1.interface_txn_id,
                      piv_source_column_name       => 'Legacy Segment 1',
                      piv_source_column_value      => org_rec.leg_dist_segment1,
                      piv_error_type               => 'ERR_VAL',
                      piv_error_code               => l_err_code,
                      piv_error_message            => l_err_msg,
                      pov_return_status            => l_log_ret_status,
                      pov_error_msg                => l_log_err_msg
                     );
               END LOOP;

               UPDATE xxconv.xxar_br_invoices_dist_stg
                  SET process_flag = 'E',
                      ERROR_TYPE = 'ERR_VAL',
                      last_update_date = SYSDATE,
                      last_updated_by = g_last_updated_by,
                      last_update_login = g_login_id
                WHERE leg_customer_trx_id IN (
                         SELECT leg_customer_trx_id
                           FROM xxconv.xxar_br_invoices_dist_stg xis
                          WHERE leg_dist_segment1 = org_rec.leg_dist_segment1
                            AND batch_id = g_new_batch_id
                            AND run_sequence_id = g_new_run_seq_id
                            AND leg_account_class = 'REC')
                  AND batch_id = g_new_batch_id
                  AND run_sequence_id = g_new_run_seq_id;

               UPDATE xxconv.xxar_br_invoices_stg xis
                  SET xis.process_flag = 'E',
                      xis.ERROR_TYPE = 'ERR_VAL',
                      xis.last_update_date = SYSDATE,
                      xis.last_updated_by = g_last_updated_by,
                      xis.last_update_login = g_login_id
                WHERE xis.leg_customer_trx_id IN (
                         SELECT xds.leg_customer_trx_id
                           FROM xxconv.xxar_br_invoices_dist_stg xds
                          WHERE xds.leg_customer_trx_id IN (
                                   SELECT xds1.leg_customer_trx_id
                                     FROM xxconv.xxar_br_invoices_dist_stg xds1
                                    WHERE xds1.leg_dist_segment1 =
                                                     org_rec.leg_dist_segment1
                                      AND xds1.batch_id = g_new_batch_id
                                      AND xds1.run_sequence_id =
                                                              g_new_run_seq_id
                                      AND xds1.leg_account_class = 'REC')
                            AND xds.batch_id = g_new_batch_id
                            AND xds.run_sequence_id = g_new_run_seq_id)
                  AND xis.batch_id = g_new_batch_id
                  AND xis.run_sequence_id = g_new_run_seq_id;

               -- ver1.10 changes end
               COMMIT;
         END;

         -- Check whether R12 operating unit in mapping table is already setup
         BEGIN
            IF l_org_name IS NOT NULL
            THEN
               print_log_message (   'Validating R12 operating unit '
                                  || l_org_name
                                 );

               SELECT hou.organization_id, hou.NAME, hou.set_of_books_id,
                      gll.currency_code, gll.ledger_id
                 INTO l_r12_org_id, l_ou_name, l_sob_id,
                      l_func_curr, l_ledger_id
                 FROM apps.hr_operating_units hou, gl_ledgers gll
                WHERE UPPER (hou.NAME) = UPPER (l_org_name)
                  AND hou.set_of_books_id = gll.ledger_id(+)
                  AND TRUNC (NVL (hou.date_to, SYSDATE)) >= TRUNC (SYSDATE);
            END IF;

            IF l_ledger_id IS NOT NULL
            THEN
               l_gl_error := 'N';

               BEGIN
                  SELECT 1
                    INTO l_gl_status
                    FROM gl_periods glp, gl_period_statuses gps
                   WHERE UPPER (glp.period_name) = UPPER (gps.period_name)
                     AND glp.period_set_name =
                                    NVL (g_period_set_name, 'ETN Corp Calend')
                     AND g_gl_date BETWEEN glp.start_date AND glp.end_date
                     AND gps.application_id =
                                 (SELECT fap.application_id
                                    FROM fnd_application_vl fap
                                   WHERE fap.application_short_name = 'SQLGL')
                     AND gps.closing_status = 'O'
                     AND ledger_id = l_ledger_id;
               EXCEPTION
                  WHEN NO_DATA_FOUND
                  THEN
                     l_gl_error := 'Y';
                     l_err_code := 'ETN_BR_GL_PERIOD_ERROR';
                     g_retcode := 1;
                     l_err_msg :=
                        'GL Period not open/defined for GL date '
                        || g_gl_date;

                     FOR r_gl_per_err_rec IN
                        (SELECT interface_txn_id
                           FROM xxconv.xxar_br_invoices_dist_stg xis
                          WHERE leg_customer_trx_id IN (
                                   SELECT leg_customer_trx_id
                                     FROM xxconv.xxar_br_invoices_dist_stg xis
                                    WHERE leg_dist_segment1 =
                                                     org_rec.leg_dist_segment1
                                      AND batch_id = g_new_batch_id
                                      AND run_sequence_id = g_new_run_seq_id
                                      AND leg_account_class = 'REC')
                            AND batch_id = g_new_batch_id
                            AND run_sequence_id = g_new_run_seq_id)
                     LOOP
                        -- ver1.10 changes end
                        log_errors
                           (pin_transaction_id           => r_gl_per_err_rec.interface_txn_id,
                            piv_source_keyname1          => 'GL Period date',
                            piv_source_keyvalue1         => g_gl_date,
                            piv_source_column_name       => 'R12 Operating Unit',
                            piv_source_column_value      => l_org_name,
                            piv_error_type               => 'ERR_VAL',
                            piv_error_code               => l_err_code,
                            piv_error_message            => l_err_msg,
                            pov_return_status            => l_log_ret_status,
                            pov_error_msg                => l_log_err_msg
                           );
                     END LOOP;

                     UPDATE xxconv.xxar_br_invoices_dist_stg
                        SET process_flag = 'E',
                            ERROR_TYPE = 'ERR_VAL',
                            last_update_date = SYSDATE,
                            last_updated_by = g_last_updated_by,
                            last_update_login = g_login_id
                      WHERE leg_customer_trx_id IN (
                               SELECT leg_customer_trx_id
                                 FROM xxconv.xxar_br_invoices_dist_stg xis
                                WHERE leg_dist_segment1 =
                                                     org_rec.leg_dist_segment1
                                  AND batch_id = g_new_batch_id
                                  AND run_sequence_id = g_new_run_seq_id
                                  AND leg_account_class = 'REC')
                        AND batch_id = g_new_batch_id
                        AND run_sequence_id = g_new_run_seq_id;

                     UPDATE xxconv.xxar_br_invoices_stg xis
                        SET xis.process_flag = 'E',
                            xis.ERROR_TYPE = 'ERR_VAL',
                            xis.last_update_date = SYSDATE,
                            xis.last_updated_by = g_last_updated_by,
                            xis.last_update_login = g_login_id
                      WHERE xis.leg_customer_trx_id IN (
                               SELECT xds.leg_customer_trx_id
                                 FROM xxconv.xxar_br_invoices_dist_stg xds
                                WHERE xds.leg_customer_trx_id IN (
                                         SELECT xds1.leg_customer_trx_id
                                           FROM xxconv.xxar_br_invoices_dist_stg xds1
                                          WHERE xds1.leg_dist_segment1 =
                                                     org_rec.leg_dist_segment1
                                            AND xds1.batch_id = g_new_batch_id
                                            AND xds1.run_sequence_id =
                                                              g_new_run_seq_id
                                            AND xds1.leg_account_class = 'REC')
                                  AND xds.batch_id = g_new_batch_id
                                  AND xds.run_sequence_id = g_new_run_seq_id)
                        AND xis.batch_id = g_new_batch_id
                        AND xis.run_sequence_id = g_new_run_seq_id;
                  -- ver1.10 changes end
                  WHEN OTHERS
                  THEN
                     l_gl_error := 'Y';
                     g_retcode := 2;
                     l_err_code := 'ETN_BR_PROCEDURE_EXCEPTION';
                     l_err_msg :=
                           'Error : Error validating gl period '
                        || g_gl_date
                        || SUBSTR (SQLERRM, 1, 150);

                     FOR r_gl_per_err1_rec IN
                        (SELECT interface_txn_id
                           FROM xxconv.xxar_br_invoices_dist_stg xis
                          WHERE leg_customer_trx_id IN (
                                   SELECT leg_customer_trx_id
                                     FROM xxconv.xxar_br_invoices_dist_stg xis
                                    WHERE leg_dist_segment1 =
                                                     org_rec.leg_dist_segment1
                                      AND batch_id = g_new_batch_id
                                      AND run_sequence_id = g_new_run_seq_id
                                      AND leg_account_class = 'REC')
                            AND batch_id = g_new_batch_id
                            AND run_sequence_id = g_new_run_seq_id)
                     LOOP
                        -- ver1.10 changes end
                        log_errors
                           (pin_transaction_id           => r_gl_per_err1_rec.interface_txn_id,
                            piv_source_keyname1          => 'GL Period date',
                            piv_source_keyvalue1         => g_gl_date,
                            piv_source_column_name       => 'R12 Operating Unit',
                            piv_source_column_value      => l_org_name,
                            piv_error_type               => 'ERR_VAL',
                            piv_error_code               => l_err_code,
                            piv_error_message            => l_err_msg,
                            pov_return_status            => l_log_ret_status,
                            pov_error_msg                => l_log_err_msg
                           );
                     END LOOP;

                     UPDATE xxconv.xxar_br_invoices_dist_stg
                        SET process_flag = 'E',
                            ERROR_TYPE = 'ERR_VAL',
                            last_update_date = SYSDATE,
                            last_updated_by = g_last_updated_by,
                            last_update_login = g_login_id
                      WHERE leg_customer_trx_id IN (
                               SELECT leg_customer_trx_id
                                 FROM xxconv.xxar_br_invoices_dist_stg xis
                                WHERE leg_dist_segment1 =
                                                     org_rec.leg_dist_segment1
                                  AND batch_id = g_new_batch_id
                                  AND run_sequence_id = g_new_run_seq_id
                                  AND leg_account_class = 'REC')
                        AND batch_id = g_new_batch_id
                        AND run_sequence_id = g_new_run_seq_id;

                     UPDATE xxconv.xxar_br_invoices_stg xis
                        SET xis.process_flag = 'E',
                            xis.ERROR_TYPE = 'ERR_VAL',
                            xis.last_update_date = SYSDATE,
                            xis.last_updated_by = g_last_updated_by,
                            xis.last_update_login = g_login_id
                      WHERE xis.leg_customer_trx_id IN (
                               SELECT xds.leg_customer_trx_id
                                 FROM xxconv.xxar_br_invoices_dist_stg xds
                                WHERE xds.leg_customer_trx_id IN (
                                         SELECT xds1.leg_customer_trx_id
                                           FROM xxconv.xxar_br_invoices_dist_stg xds1
                                          WHERE xds1.leg_dist_segment1 =
                                                     org_rec.leg_dist_segment1
                                            AND xds1.batch_id = g_new_batch_id
                                            AND xds1.run_sequence_id =
                                                              g_new_run_seq_id
                                            AND xds1.leg_account_class = 'REC')
                                  AND xds.batch_id = g_new_batch_id
                                  AND xds.run_sequence_id = g_new_run_seq_id)
                        AND xis.batch_id = g_new_batch_id
                        AND xis.run_sequence_id = g_new_run_seq_id;
               -- ver1.10 changes end
               END;
            END IF;

                  -- Check batch source
            --      BEGIN
            l_batch_source := NULL;
            l_plant_credit_office := NULL;

            IF l_org_name IS NOT NULL AND l_r12_org_id IS NOT NULL
            THEN
               print_log_message (   'Deriving and validating batch source '
                                  || l_org_name
                                 );

               SELECT DECODE (xmu.ar_credit_office,
                              NULL, org_rec.leg_dist_segment1,
                              xmu.ar_credit_office
                             )
                 INTO l_plant_credit_office
                --FROM xxetn_map_unit xmu commented for v1.6
        FROM xxetn_map_unit_v xmu --added for v1.6
                WHERE operating_unit = l_org_name
                  AND xmu.site = org_rec.leg_dist_segment1;
            -- l_batch_source :=
               --               g_batch_source || ' ' || l_plant_credit_office;
            END IF;

            IF l_plant_credit_office IS NOT NULL
            THEN
               l_batch_error := 'N';

               BEGIN
                  SELECT NAME
                    INTO l_batch_source
                    FROM ra_batch_sources_all
                   WHERE  NAME LIKE 'CONVERSION%BR%' || l_plant_credit_office     -- NAME LIKE 'BR%CONVERSION%' || l_plant_credit_office
                     AND status = 'A'
                     AND org_id = l_r12_org_id;
               EXCEPTION
                  WHEN NO_DATA_FOUND
                  THEN
                     l_batch_error := 'Y';
                     l_err_code := 'ETN_BR_BATCH_SOURCE_ERROR';
                     g_retcode := 1;
                     l_err_msg :=
                           'Batch Source '
                        || l_batch_source
                        || ' not defined for R12 operating unit '
                        || l_org_name;

                     /*FOR r_bs_per_err_rec IN
                        (SELECT interface_txn_id
                           FROM xxconv.xxar_br_invoices_dist_stg xis
                          WHERE leg_customer_trx_id IN (
                                   SELECT leg_customer_trx_id
                                     FROM xxconv.xxar_br_invoices_dist_stg xis
                                    WHERE leg_dist_segment1 =
                                                     org_rec.leg_dist_segment1
                                      AND batch_id = g_new_batch_id
                                      AND run_sequence_id = g_new_run_seq_id
                                      AND leg_account_class = 'REC')
                            AND batch_id = g_new_batch_id
                            AND run_sequence_id = g_new_run_seq_id) */

          FOR r_bs_per_err_rec IN (SELECT interface_txn_id
                                                FROM xxconv.xxar_br_invoices_dist_stg xis
                                               WHERE leg_dist_segment1 = org_rec.leg_dist_segment1
                             AND leg_account_class = 'REC'
                  AND leg_org_name NOT IN ('OU US AR', 'OU CA AR')
                                                 AND batch_id =
                                                     g_new_batch_id
                                                 AND run_sequence_id =
                                                     g_new_run_seq_id)
                     LOOP
                        log_errors
                           (pin_transaction_id           => r_bs_per_err_rec.interface_txn_id,
                            piv_source_keyname1          => 'R12 Operating Unit',
                            piv_source_keyvalue1         => l_org_name,
                            piv_source_column_name       => 'Batch Source',
                            piv_source_column_value      => l_batch_source,
                            piv_error_type               => 'ERR_VAL',
                            piv_error_code               => l_err_code,
                            piv_error_message            => l_err_msg,
                            pov_return_status            => l_log_ret_status,
                            pov_error_msg                => l_log_err_msg
                           );
                     END LOOP;

                     UPDATE xxconv.xxar_br_invoices_dist_stg
                        SET process_flag = 'E',
                            ERROR_TYPE = 'ERR_VAL',
                            last_update_date = SYSDATE,
                            last_updated_by = g_last_updated_by,
                            last_update_login = g_login_id
                      WHERE  leg_dist_segment1 = org_rec.leg_dist_segment1
              AND leg_account_class = 'REC'
                  AND leg_org_name NOT IN ('OU US AR', 'OU CA AR')
                        AND batch_id = g_new_batch_id
                        AND run_sequence_id = g_new_run_seq_id;
            /*leg_customer_trx_id IN (
                               SELECT leg_customer_trx_id
                                 FROM xxconv.xxar_br_invoices_dist_stg xis
                                WHERE leg_dist_segment1 =
                                                     org_rec.leg_dist_segment1
                                  AND batch_id = g_new_batch_id
                                  AND run_sequence_id = g_new_run_seq_id
                                  AND leg_account_class = 'REC')
                        AND batch_id = g_new_batch_id
                        AND run_sequence_id = g_new_run_seq_id;*/

                     UPDATE xxconv.xxar_br_invoices_stg xis
                        SET xis.process_flag = 'E',
                            xis.ERROR_TYPE = 'ERR_VAL',
                            xis.last_update_date = SYSDATE,
                            xis.last_updated_by = g_last_updated_by,
                            xis.last_update_login = g_login_id
                      WHERE EXISTS
                      (SELECT 1
                               FROM xxconv.xxar_br_invoices_dist_stg  xds1
                              WHERE xds1.leg_dist_segment1 =
                                    org_rec.leg_dist_segment1
                                AND xds1.leg_customer_trx_id =
                                    xis.leg_customer_trx_id
                                AND xds1.batch_id = g_new_batch_id
                                AND xds1.run_sequence_id = g_new_run_seq_id
                                AND xds1.leg_account_class = 'REC'
                  AND xds1.leg_org_name NOT IN ('OU US AR', 'OU CA AR')
                  )
                        AND xis.batch_id = g_new_batch_id
                        AND xis.run_sequence_id = g_new_run_seq_id;

            /*xis.leg_customer_trx_id IN (
                               SELECT xds.leg_customer_trx_id
                                 FROM xxconv.xxar_br_invoices_dist_stg xds
                                WHERE xds.leg_customer_trx_id IN (
                                         SELECT xds1.leg_customer_trx_id
                                           FROM xxconv.xxar_br_invoices_dist_stg xds1
                                          WHERE xds1.leg_dist_segment1 =
                                                     org_rec.leg_dist_segment1
                                            AND xds1.batch_id = g_new_batch_id
                                            AND xds1.run_sequence_id =
                                                              g_new_run_seq_id
                                            AND xds1.leg_account_class = 'REC')
                                  AND xds.batch_id = g_new_batch_id
                                  AND xds.run_sequence_id = g_new_run_seq_id)
                        AND xis.batch_id = g_new_batch_id
                        AND xis.run_sequence_id = g_new_run_seq_id;*/


                  WHEN OTHERS
                  THEN
                     l_batch_error := 'Y';
                     g_retcode := 2;
                     l_err_code := 'ETN_AR_PROCEDURE_EXCEPTION';
                     l_err_msg :=
                           'Error : Error validating batch source '
                        || l_batch_source
                        || SUBSTR (SQLERRM, 1, 150);

                     FOR r_bs_per_err1_rec IN
                        (SELECT interface_txn_id
                           FROM xxconv.xxar_br_invoices_dist_stg xis
                          WHERE leg_customer_trx_id IN (
                                   SELECT leg_customer_trx_id
                                     FROM xxconv.xxar_br_invoices_dist_stg xis
                                    WHERE leg_dist_segment1 =
                                                     org_rec.leg_dist_segment1
                                      AND batch_id = g_new_batch_id
                                      AND run_sequence_id = g_new_run_seq_id
                                      AND leg_account_class = 'REC')
                            AND batch_id = g_new_batch_id
                            AND run_sequence_id = g_new_run_seq_id)
                     LOOP
                        log_errors
                           (pin_transaction_id           => r_bs_per_err1_rec.interface_txn_id,
                            piv_source_keyname1          => 'R12 Operating Unit',
                            piv_source_keyvalue1         => l_org_name,
                            piv_source_column_name       => 'Batch Source',
                            piv_source_column_value      => l_batch_source,
                            piv_error_type               => 'ERR_VAL',
                            piv_error_code               => l_err_code,
                            piv_error_message            => l_err_msg,
                            pov_return_status            => l_log_ret_status,
                            pov_error_msg                => l_log_err_msg
                           );
                     END LOOP;

                     UPDATE xxconv.xxar_br_invoices_dist_stg
                        SET process_flag = 'E',
                            ERROR_TYPE = 'ERR_VAL',
                            last_update_date = SYSDATE,
                            last_updated_by = g_last_updated_by,
                            last_update_login = g_login_id
                      WHERE leg_customer_trx_id IN (
                               SELECT leg_customer_trx_id
                                 FROM xxconv.xxar_br_invoices_dist_stg xis
                                WHERE leg_dist_segment1 =
                                                     org_rec.leg_dist_segment1
                                  AND batch_id = g_new_batch_id
                                  AND run_sequence_id = g_new_run_seq_id
                                  AND leg_account_class = 'REC')
                        AND batch_id = g_new_batch_id
                        AND run_sequence_id = g_new_run_seq_id;

                     UPDATE xxconv.xxar_br_invoices_stg xis
                        SET xis.process_flag = 'E',
                            xis.ERROR_TYPE = 'ERR_VAL',
                            xis.last_update_date = SYSDATE,
                            xis.last_updated_by = g_last_updated_by,
                            xis.last_update_login = g_login_id
                      WHERE xis.leg_customer_trx_id IN (
                               SELECT xds.leg_customer_trx_id
                                 FROM xxconv.xxar_br_invoices_dist_stg xds
                                WHERE xds.leg_customer_trx_id IN (
                                         SELECT xds1.leg_customer_trx_id
                                           FROM xxconv.xxar_br_invoices_dist_stg xds1
                                          WHERE xds1.leg_dist_segment1 =
                                                     org_rec.leg_dist_segment1
                                            AND xds1.batch_id = g_new_batch_id
                                            AND xds1.run_sequence_id =
                                                              g_new_run_seq_id
                                            AND xds1.leg_account_class = 'REC')
                                  AND xds.batch_id = g_new_batch_id
                                  AND xds.run_sequence_id = g_new_run_seq_id)
                        AND xis.batch_id = g_new_batch_id
                        AND xis.run_sequence_id = g_new_run_seq_id;
               END;
            END IF;

            -- ver1.10 changes end
          --  IF NVL (l_gl_error, 'N') = 'N' AND NVL (l_batch_error, 'N') = 'N'
         IF l_r12_org_id IS NOT NULL
            THEN
               UPDATE xxconv.xxar_br_invoices_stg
                  SET org_id = l_r12_org_id,
                      set_of_books_id = l_sob_id,
                      func_curr = l_func_curr,
                      ledger_id = l_ledger_id,
                      batch_source_name = l_batch_source,
                      header_attribute9 = l_plant_credit_office
                WHERE leg_customer_trx_id IN (
                         SELECT leg_customer_trx_id
                           FROM xxconv.xxar_br_invoices_dist_stg
                          WHERE leg_dist_segment1 = org_rec.leg_dist_segment1
                            AND batch_id = g_new_batch_id
                            AND run_sequence_id = g_new_run_seq_id
               AND leg_account_class = 'REC'
               AND leg_org_name NOT IN ('OU US AR', 'OU CA AR') )
                  AND batch_id = g_new_batch_id
                  AND run_sequence_id = g_new_run_seq_id;
            END IF;

            COMMIT;
         EXCEPTION
            WHEN NO_DATA_FOUND
            THEN
               g_retcode := 1;

               FOR r_r12_org_err_rec IN
                  (SELECT interface_txn_id
                     FROM xxconv.xxar_br_invoices_dist_stg xis
                    WHERE leg_customer_trx_id IN (
                             SELECT leg_customer_trx_id
                               FROM xxconv.xxar_br_invoices_dist_stg xis
                              WHERE leg_dist_segment1 =
                                                    org_rec.leg_dist_segment1
                                AND batch_id = g_new_batch_id
                                AND run_sequence_id = g_new_run_seq_id
                                AND leg_account_class = 'REC')
                      AND batch_id = g_new_batch_id
                      AND run_sequence_id = g_new_run_seq_id)
               LOOP
                  -- ver1.10 changes end
                  l_err_code := 'ETN_AR_OPERATING UNIT_ERROR';
                  l_err_msg := 'Error : Operating unit not setup';
                  log_errors
                     (pin_transaction_id           => r_r12_org_err_rec.interface_txn_id,
                      piv_source_column_name       => 'R12 Operating Unit',
                      piv_source_column_value      => l_org_name,
                      piv_error_type               => 'ERR_VAL',
                      piv_error_code               => l_err_code,
                      piv_error_message            => l_err_msg,
                      pov_return_status            => l_log_ret_status,
                      pov_error_msg                => l_log_err_msg
                     );
               END LOOP;

               UPDATE xxconv.xxar_br_invoices_dist_stg
                  SET process_flag = 'E',
                      ERROR_TYPE = 'ERR_VAL',
                      last_update_date = SYSDATE,
                      last_updated_by = g_last_updated_by,
                      last_update_login = g_login_id
                WHERE leg_customer_trx_id IN (
                         SELECT leg_customer_trx_id
                           FROM xxconv.xxar_br_invoices_dist_stg xis
                          WHERE leg_dist_segment1 = org_rec.leg_dist_segment1
                            AND batch_id = g_new_batch_id
                            AND run_sequence_id = g_new_run_seq_id
                            AND leg_account_class = 'REC')
                  AND batch_id = g_new_batch_id
                  AND run_sequence_id = g_new_run_seq_id;

               UPDATE xxconv.xxar_br_invoices_stg xis
                  SET xis.process_flag = 'E',
                      xis.ERROR_TYPE = 'ERR_VAL',
                      xis.last_update_date = SYSDATE,
                      xis.last_updated_by = g_last_updated_by,
                      xis.last_update_login = g_login_id
                WHERE xis.leg_customer_trx_id IN (
                         SELECT xds.leg_customer_trx_id
                           FROM xxconv.xxar_br_invoices_dist_stg xds
                          WHERE xds.leg_customer_trx_id IN (
                                   SELECT xds1.leg_customer_trx_id
                                     FROM xxconv.xxar_br_invoices_dist_stg xds1
                                    WHERE xds1.leg_dist_segment1 =
                                                     org_rec.leg_dist_segment1
                                      AND xds1.batch_id = g_new_batch_id
                                      AND xds1.run_sequence_id =
                                                              g_new_run_seq_id
                                      AND xds1.leg_account_class = 'REC')
                            AND xds.batch_id = g_new_batch_id
                            AND xds.run_sequence_id = g_new_run_seq_id)
                  AND xis.batch_id = g_new_batch_id
                  AND xis.run_sequence_id = g_new_run_seq_id;
            -- ver1.10 changes end
            WHEN OTHERS
            THEN
               l_err_code := 'ETN_BR_PROCEDURE_EXCEPTION';
               g_retcode := 2;
               l_err_msg :=
                     'Error : Error fetching R12 operating unit'
                  || SUBSTR (SQLERRM, 1, 150);

               FOR r_r12_org_err1_rec IN
                  (SELECT interface_txn_id
                     FROM xxconv.xxar_br_invoices_dist_stg xis
                    WHERE leg_customer_trx_id IN (
                             SELECT leg_customer_trx_id
                               FROM xxconv.xxar_br_invoices_dist_stg xis
                              WHERE leg_dist_segment1 =
                                                     org_rec.leg_dist_segment1
                                AND batch_id = g_new_batch_id
                                AND run_sequence_id = g_new_run_seq_id
                                AND leg_account_class = 'REC')
                      AND batch_id = g_new_batch_id
                      AND run_sequence_id = g_new_run_seq_id)
               LOOP
                  -- ver1.10 changes end
                  log_errors
                     (pin_transaction_id           => r_r12_org_err1_rec.interface_txn_id,
                      piv_source_column_name       => 'R12 Operating Unit',
                      piv_source_column_value      => l_org_name,
                      piv_error_type               => 'ERR_VAL',
                      piv_error_code               => l_err_code,
                      piv_error_message            => l_err_msg,
                      pov_return_status            => l_log_ret_status,
                      pov_error_msg                => l_log_err_msg
                     );
               END LOOP;

               UPDATE xxconv.xxar_br_invoices_dist_stg
                  SET process_flag = 'E',
                      ERROR_TYPE = 'ERR_VAL',
                      last_update_date = SYSDATE,
                      last_updated_by = g_last_updated_by,
                      last_update_login = g_login_id
                WHERE leg_customer_trx_id IN (
                         SELECT leg_customer_trx_id
                           FROM xxconv.xxar_br_invoices_dist_stg xis
                          WHERE leg_dist_segment1 = org_rec.leg_dist_segment1
                            AND batch_id = g_new_batch_id
                            AND run_sequence_id = g_new_run_seq_id
                            AND leg_account_class = 'REC')
                  AND batch_id = g_new_batch_id
                  AND run_sequence_id = g_new_run_seq_id;

               UPDATE xxconv.xxar_br_invoices_stg xis
                  SET xis.process_flag = 'E',
                      xis.ERROR_TYPE = 'ERR_VAL',
                      xis.last_update_date = SYSDATE,
                      xis.last_updated_by = g_last_updated_by,
                      xis.last_update_login = g_login_id
                WHERE xis.leg_customer_trx_id IN (
                         SELECT xds.leg_customer_trx_id
                           FROM xxconv.xxar_br_invoices_dist_stg xds
                          WHERE xds.leg_customer_trx_id IN (
                                   SELECT xds1.leg_customer_trx_id
                                     FROM xxconv.xxar_br_invoices_dist_stg xds1
                                    WHERE xds1.leg_dist_segment1 =
                                                     org_rec.leg_dist_segment1
                                      AND xds1.batch_id = g_new_batch_id
                                      AND xds1.run_sequence_id =
                                                              g_new_run_seq_id
                                      AND xds1.leg_account_class = 'REC')
                            AND xds.batch_id = g_new_batch_id
                            AND xds.run_sequence_id = g_new_run_seq_id)
                  AND xis.batch_id = g_new_batch_id
                  AND xis.run_sequence_id = g_new_run_seq_id;
         END;
      END LOOP;

      COMMIT;

      FOR term_rec IN term_cur
      LOOP
         l_term_name := NULL;
         l_term_id := NULL;

         BEGIN
            /* print_log_message (   'Validating legacy payment term '
                                || term_rec.leg_term_name
                               );*/
            SELECT TRIM (flv.description)
              INTO l_term_name
              FROM fnd_lookup_values flv
             WHERE TRIM (UPPER (flv.meaning)) =
                                         TRIM (UPPER (term_rec.leg_term_name))
               AND flv.LANGUAGE = USERENV ('LANG')
               AND flv.enabled_flag = 'Y'
               AND UPPER (flv.lookup_type) = g_pmt_term_lookup
               AND TRUNC (SYSDATE) BETWEEN TRUNC (NVL (flv.start_date_active,
                                                       SYSDATE
                                                      )
                                                 )
                                       AND TRUNC (NVL (flv.end_date_active,
                                                       SYSDATE
                                                      )
                                                 );
         EXCEPTION
            WHEN NO_DATA_FOUND
            THEN
               g_retcode := 1;
               print_log_message
                               (   'Error in Validating legacy payment term '
                                || term_rec.leg_term_name
                               );

               FOR r_term_ref_err_rec IN
                  (SELECT interface_txn_id
                     FROM xxconv.xxar_br_invoices_stg xis
                    WHERE --NVL (leg_term_name, 1) =
                            --                   NVL (term_rec.leg_term_name, 1) --commented for v1.4
             leg_term_name = term_rec.leg_term_name --added for v1.4
                      AND batch_id = g_new_batch_id
                      AND run_sequence_id = g_new_run_seq_id)
               LOOP
                  l_err_code := 'ETN_BR_PMT_TERM_ERROR';
                  l_err_msg :=
                       'Error : Cross reference not defined for payment term';
                  log_errors
                     (pin_transaction_id           => r_term_ref_err_rec.interface_txn_id,
                      piv_source_column_name       => 'Legacy Payment Term',
                      piv_source_column_value      => term_rec.leg_term_name,
                      piv_error_type               => 'ERR_VAL',
                      piv_error_code               => l_err_code,
                      piv_error_message            => l_err_msg,
                      pov_return_status            => l_log_ret_status,
                      pov_error_msg                => l_log_err_msg
                     );
               END LOOP;

               UPDATE xxconv.xxar_br_invoices_stg
                  SET process_flag = 'E',
                      ERROR_TYPE = 'ERR_VAL',
                      last_update_date = SYSDATE,
                      last_updated_by = g_last_updated_by,
                      last_update_login = g_login_id
                WHERE --NVL (leg_term_name, 1) = NVL (term_rec.leg_term_name, 1) --commented for v1.4
          leg_term_name = term_rec.leg_term_name --added for v1.4
                  AND batch_id = g_new_batch_id
                  AND run_sequence_id = g_new_run_seq_id;

               COMMIT;
            WHEN OTHERS
            THEN
               l_err_code := 'ETN_BR_PROCEDURE_EXCEPTION';
               g_retcode := 2;
               l_err_msg :=
                     'Error : Error updating staging table for payment term'
                  || SUBSTR (SQLERRM, 1, 150);
               log_errors (
                           --   pin_transaction_id           =>  pin_trx_id
                           piv_source_column_name       => 'Legacy Payment Term',
                           piv_source_column_value      => term_rec.leg_term_name,
                           piv_error_type               => 'ERR_VAL',
                           piv_error_code               => l_err_code,
                           piv_error_message            => l_err_msg,
                           pov_return_status            => l_log_ret_status,
                           pov_error_msg                => l_log_err_msg
                          );

               UPDATE xxconv.xxar_br_invoices_stg
                  SET process_flag = 'E',
                      ERROR_TYPE = 'ERR_VAL',
                      last_update_date = SYSDATE,
                      last_updated_by = g_last_updated_by,
                      last_update_login = g_login_id
                WHERE --NVL (leg_term_name, 1) = NVL (term_rec.leg_term_name, 1) --commented for v1.40
          leg_term_name = term_rec.leg_term_name--added for v1.4
                  AND batch_id = g_new_batch_id
                  AND run_sequence_id = g_new_run_seq_id;
         END;

         BEGIN
            IF l_term_name IS NOT NULL
            THEN
               --   print_log_message ('Validating R12 term name ' || l_term_name);
               SELECT rtm.term_id
                 INTO l_term_id
                 FROM ra_terms_tl rtm
                WHERE UPPER (rtm.NAME) = UPPER (l_term_name)
                  AND rtm.LANGUAGE = USERENV ('LANG');
            END IF;

            UPDATE xxconv.xxar_br_invoices_stg
               SET term_id = l_term_id,
                   term_name = l_term_name
             WHERE leg_term_name = term_rec.leg_term_name
               AND batch_id = g_new_batch_id
               AND run_sequence_id = g_new_run_seq_id;

            COMMIT;
         EXCEPTION
            WHEN NO_DATA_FOUND
            THEN
               g_retcode := 1;

               FOR r_r12_term_err_rec IN
                  (SELECT interface_txn_id
                     FROM xxconv.xxar_br_invoices_stg xis
                    WHERE leg_term_name = term_rec.leg_term_name
                      AND batch_id = g_new_batch_id
                      AND run_sequence_id = g_new_run_seq_id)
               LOOP
                  l_err_code := 'ETN_BR_PMT_TERM_ERROR';
                  l_err_msg := 'Error : Payment term not setup in R12';
                  log_errors
                     (pin_transaction_id           => r_r12_term_err_rec.interface_txn_id,
                      piv_source_column_name       => 'R12 payment term',
                      piv_source_column_value      => l_term_name,
                      piv_error_type               => 'ERR_VAL',
                      piv_error_code               => l_err_code,
                      piv_error_message            => l_err_msg,
                      pov_return_status            => l_log_ret_status,
                      pov_error_msg                => l_log_err_msg
                     );
               END LOOP;

               UPDATE xxconv.xxar_br_invoices_stg
                  SET process_flag = 'E',
                      ERROR_TYPE = 'ERR_VAL',
                      last_update_date = SYSDATE,
                      last_updated_by = g_last_updated_by,
                      last_update_login = g_login_id
                WHERE leg_term_name = term_rec.leg_term_name
                  AND batch_id = g_new_batch_id
                  AND run_sequence_id = g_new_run_seq_id;
            WHEN OTHERS
            THEN
               l_err_code := 'ETN_BR_PROCEDURE_EXCEPTION';
               g_retcode := 2;
               l_err_msg :=
                     'Error : Error fetching R12 Payment term'
                  || SUBSTR (SQLERRM, 1, 150);

               FOR r_r12_term_err1_rec IN
                  (SELECT interface_txn_id
                     FROM xxconv.xxar_br_invoices_stg xis
                    WHERE leg_term_name = term_rec.leg_term_name
                      AND batch_id = g_new_batch_id
                      AND run_sequence_id = g_new_run_seq_id)
               LOOP
                  log_errors
                     (pin_transaction_id           => r_r12_term_err1_rec.interface_txn_id,
                      piv_source_column_name       => 'R12 payment term',
                      piv_source_column_value      => l_term_name,
                      piv_error_type               => 'ERR_VAL',
                      piv_error_code               => l_err_code,
                      piv_error_message            => l_err_msg,
                      pov_return_status            => l_log_ret_status,
                      pov_error_msg                => l_log_err_msg
                     );
               END LOOP;

               UPDATE xxconv.xxar_br_invoices_stg
                  SET process_flag = 'E',
                      ERROR_TYPE = 'ERR_VAL',
                      last_update_date = SYSDATE,
                      last_updated_by = g_last_updated_by,
                      last_update_login = g_login_id
                WHERE leg_term_name = term_rec.leg_term_name
                  AND batch_id = g_new_batch_id
                  AND run_sequence_id = g_new_run_seq_id;
         END;
      END LOOP;

      COMMIT;

      FOR customer_rec IN customer_cur
      LOOP
         -- BEGIN
         IF customer_rec.org_id IS NULL
         THEN
            print_log1_message (   'Customer validation not done for '
                                || customer_rec.leg_customer_number
                                || ' as R12 Org Id not present'
                               );
         ELSE
            l_r12_cust_id := NULL;
            l_bill_to_addr := NULL;
            l_drawee_to_addr := NULL;
            l_pymt_site_use_id := NULL;
            l_receipt_method_id := NULL;
            l_ship_to_addr := NULL;
            l_valid_cust_flag := 'N';

           FND_FILE.PUT_LINE(FND_FILE.LOG,
                     'Starting for Org Id :'||customer_rec.org_id
                     ||' leg customer number:'||customer_rec.leg_customer_number
                     ||'  l_r12_cust_id : '||l_r12_cust_id);
           
            BEGIN
               SELECT DISTINCT hca.cust_account_id
                          INTO l_r12_cust_id
                          FROM apps.hz_cust_accounts_all hca,
                               apps.hz_cust_acct_sites_all hcas,
                               apps.hz_cust_site_uses_all hcsu
                         WHERE hcsu.cust_acct_site_id = hcas.cust_acct_site_id
                           AND hcas.cust_account_id = hca.cust_account_id
                  --        AND hcsu.location =TRIM (customer_rec.leg_bill_to_address)
                           AND hcsu.location  = substr(customer_rec.leg_bill_to_address,1,instr(customer_rec.leg_bill_to_address,'|')-1)
                           AND NVL (hcas.org_id, 1) =
                                                  NVL (customer_rec.org_id, 1)
                           AND hca.status = 'A'
                           AND hcsu.status = 'A'
                           AND hcas.status = 'A'
                           AND hcsu.site_use_code = 'BILL_TO';
                        /*   AND hca.orig_system_reference LIKE
                                     '%'
                                  || (TRIM (customer_rec.leg_customer_number)
                                     ) || '%';  */  -- Commented for v1.10

               l_valid_cust_flag := 'Y';
             EXCEPTION
               WHEN NO_DATA_FOUND
               THEN
                  l_valid_cust_flag := 'N';
                  g_retcode := 1;
                  l_r12_cust_id :=NULL; --v1.11

                  FOR r_r12_cust_err_rec IN
                     (SELECT interface_txn_id
                        FROM xxconv.xxar_br_invoices_stg xis
                       WHERE leg_customer_number =
                                             customer_rec.leg_customer_number
                         AND leg_bill_to_address =   customer_rec.leg_bill_to_address
						 AND org_id = customer_rec.org_id --added for v1.13
                         AND batch_id = g_new_batch_id
                         AND run_sequence_id = g_new_run_seq_id)
                  LOOP
                     l_err_code := 'ETN_BR_BILL_CUSTOMER_ERROR';
                     l_err_msg :=
                           'Error : Cross reference not defined for customer';
                     log_errors
                        (pin_transaction_id           => r_r12_cust_err_rec.interface_txn_id,
                         piv_source_column_name       => 'Legacy Customer Number',
                         piv_source_column_value      => customer_rec.leg_customer_number,
                         piv_error_type               => 'ERR_VAL',
                         piv_error_code               => l_err_code,
                         piv_error_message            => l_err_msg,
                         pov_return_status            => l_log_ret_status,
                         pov_error_msg                => l_log_err_msg
                        );
                  END LOOP;

                  UPDATE xxconv.xxar_br_invoices_stg
                     SET process_flag = 'E',
                         ERROR_TYPE = 'ERR_VAL',
                         last_update_date = SYSDATE,
                         last_updated_by = g_last_updated_by,
                         last_update_login = g_login_id
                   WHERE leg_customer_number = customer_rec.leg_customer_number
                     AND leg_bill_to_address = customer_rec.leg_bill_to_address
					 AND org_id = customer_rec.org_id --added for v1.13
                     AND batch_id = g_new_batch_id
                     AND run_sequence_id = g_new_run_seq_id;
               WHEN OTHERS
               THEN
                  l_valid_cust_flag := 'N';
                  l_r12_cust_id :=NULL; --v1.11 
                  g_retcode := 2;
                  l_err_code := 'ETN_BR_PROCEDURE_EXCEPTION';
                  l_err_msg :=
                        'Error : Error while fetching customer cross reference'
                     || SUBSTR (SQLERRM, 1, 150);
                  print_log1_message (l_err_msg);
                   FND_FILE.PUT_LINE(FND_FILE.LOG,
                     'Org Id :'||customer_rec.org_id
                     ||'leg customer number:'||customer_rec.leg_customer_number
                     ||'Bill to address location: '||substr(customer_rec.leg_bill_to_address,1,instr(customer_rec.leg_bill_to_address,'|')-1)
                     ||'  l_r12_cust_id :'||l_r12_cust_id
                     ||'  l_valid_cust_flag :'||l_valid_cust_flag );

                  FOR r_r12_cust_err1_rec IN
                     (SELECT interface_txn_id
                        FROM xxconv.xxar_br_invoices_stg xis
                       WHERE leg_customer_number = customer_rec.leg_customer_number
                         AND  leg_bill_to_address = customer_rec.leg_bill_to_address
						 AND org_id = customer_rec.org_id --added for v1.13
                         AND batch_id = g_new_batch_id
                         AND run_sequence_id = g_new_run_seq_id)
                  LOOP
                     log_errors
                        (pin_transaction_id           => r_r12_cust_err1_rec.interface_txn_id,
                         piv_source_column_name       => 'Legacy Customer Number',
                         piv_source_column_value      => customer_rec.leg_customer_number,
                         piv_error_type               => 'ERR_VAL',
                         piv_error_code               => l_err_code,
                         piv_error_message            => l_err_msg,
                         pov_return_status            => l_log_ret_status,
                         pov_error_msg                => l_log_err_msg
                        );
                  END LOOP;

                  UPDATE xxconv.xxar_br_invoices_stg
                     SET process_flag = 'E',
                         ERROR_TYPE = 'ERR_VAL',
                         last_update_date = SYSDATE,
                         last_updated_by = g_last_updated_by,
                         last_update_login = g_login_id
                   WHERE leg_customer_number = customer_rec.leg_customer_number
                     AND leg_bill_to_address = customer_rec.leg_bill_to_address
					 AND org_id = customer_rec.org_id --added for v1.13
                     AND batch_id = g_new_batch_id
                     AND run_sequence_id = g_new_run_seq_id;
            END;

        IF  l_r12_cust_id IS NOT NULL
        THEN
        -- bill to
            BEGIN
               SELECT hcas.cust_acct_site_id
                 INTO l_bill_to_addr
                 FROM apps.hz_cust_acct_sites_all hcas,
                      apps.hz_cust_site_uses_all hcsu
                WHERE hcsu.cust_acct_site_id = hcas.cust_acct_site_id
               --  AND hcsu.location = TRIM (customer_rec.leg_bill_to_address)
                  AND hcsu.location  = substr(customer_rec.leg_bill_to_address,1,instr(customer_rec.leg_bill_to_address,'|')-1)
                  AND NVL (hcas.org_id, 1) = NVL (customer_rec.org_id, 1)
                  AND hcsu.status = 'A'
                  AND hcas.status = 'A'
                  AND hcsu.site_use_code = 'BILL_TO'
                  AND hcas.cust_account_id = l_r12_cust_id;

               --ver 1.8 changes end
               l_valid_cust_flag := 'Y';
            EXCEPTION --added below as per CR#346150
              WHEN TOO_MANY_ROWS
              THEN
               SELECT hcas.cust_acct_site_id
                 INTO l_bill_to_addr
                 FROM apps.hz_cust_acct_sites_all hcas,
                      apps.hz_cust_site_uses_all hcsu
                WHERE hcsu.cust_acct_site_id = hcas.cust_acct_site_id
               --  AND hcsu.location = TRIM (customer_rec.leg_bill_to_address)
                  AND hcsu.location = substr(customer_rec.leg_bill_to_address,1,instr(customer_rec.leg_bill_to_address,'|')-1)
                  AND hcas.orig_system_reference = TRIM(SUBSTR(customer_rec.leg_bill_to_address ,INSTR(customer_rec.leg_bill_to_address ,'|' )+1 ))
                  AND NVL (hcas.org_id, 1) = NVL (customer_rec.org_id, 1)
                  AND hcsu.status = 'A'
                  AND hcas.status = 'A'
                  AND hcsu.site_use_code = 'BILL_TO'
                  AND hcas.cust_account_id = l_r12_cust_id;

              WHEN NO_DATA_FOUND
              THEN
                  g_retcode := 1;
                  l_valid_cust_flag := 'N';

                  FOR r_r12_bill_err_rec IN
                     (SELECT interface_txn_id
                        FROM xxconv.xxar_br_invoices_stg xis
                       WHERE leg_customer_number =
                                             customer_rec.leg_customer_number
                         AND leg_bill_to_address =
                                              customer_rec.leg_bill_to_address
						 AND org_id = customer_rec.org_id --added for v1.13
                         AND batch_id = g_new_batch_id
                         AND run_sequence_id = g_new_run_seq_id)
                  LOOP
                     l_err_code := 'ETN_BR_BILL_CUSTOMER_ERROR';
                     l_err_msg :=
                        'Error : Cross reference not defined for bill to customer';
                     log_errors
                        (pin_transaction_id           => r_r12_bill_err_rec.interface_txn_id,
                         piv_source_column_name       => 'Legacy Customer number/ Legacy Bill to address',
                         piv_source_column_value      =>    customer_rec.leg_customer_number
                                                         || ' / '
                                                         || customer_rec.leg_bill_to_address,
                         piv_error_type               => 'ERR_VAL',
                         piv_error_code               => l_err_code,
                         piv_error_message            => l_err_msg,
                         pov_return_status            => l_log_ret_status,
                         pov_error_msg                => l_log_err_msg
                        );
                  END LOOP;

                  UPDATE xxconv.xxar_br_invoices_stg
                     SET process_flag = 'E',
                         ERROR_TYPE = 'ERR_VAL',
                         last_update_date = SYSDATE,
                         last_updated_by = g_last_updated_by,
                         last_update_login = g_login_id
                   WHERE leg_customer_number =
                                              customer_rec.leg_customer_number
                     AND leg_bill_to_address =
                                              customer_rec.leg_bill_to_address
					 AND org_id = customer_rec.org_id --added for v1.13
                     AND batch_id = g_new_batch_id
                     AND run_sequence_id = g_new_run_seq_id;
               WHEN OTHERS
               THEN
                  l_valid_cust_flag := 'N';
                  g_retcode := 2;
                  l_err_code := 'ETN_BR_PROCEDURE_EXCEPTION';
                  l_err_msg :=
                        'Error : Error while fetching bill to customer cross reference'
                     || SUBSTR (SQLERRM, 1, 150);
                  print_log1_message (l_err_msg);

                  FOR r_r12_bill_err1_rec IN
                     (SELECT interface_txn_id
                        FROM xxconv.xxar_br_invoices_stg xis
                       WHERE leg_customer_number =
                                              customer_rec.leg_customer_number
                         AND leg_bill_to_address =
                                              customer_rec.leg_bill_to_address
						 AND org_id = customer_rec.org_id --added for v1.13
                         AND batch_id = g_new_batch_id
                         AND run_sequence_id = g_new_run_seq_id)
                  LOOP
                     log_errors
                        (pin_transaction_id           => r_r12_bill_err1_rec.interface_txn_id,
                         piv_source_column_name       => 'Legacy Customer number/ Legacy Bill to address',
                         piv_source_column_value      =>    customer_rec.leg_customer_number
                                                         || ' / '
                                                         || customer_rec.leg_bill_to_address,
                         piv_error_type               => 'ERR_VAL',
                         piv_error_code               => l_err_code,
                         piv_error_message            => l_err_msg,
                         pov_return_status            => l_log_ret_status,
                         pov_error_msg                => l_log_err_msg
                        );
                  END LOOP;

                  UPDATE xxconv.xxar_br_invoices_stg
                     SET process_flag = 'E',
                         ERROR_TYPE = 'ERR_VAL',
                         last_update_date = SYSDATE,
                         last_updated_by = g_last_updated_by,
                         last_update_login = g_login_id
                   WHERE leg_customer_number =
                                              customer_rec.leg_customer_number
                     AND leg_bill_to_address =
                                              customer_rec.leg_bill_to_address
					 AND org_id = customer_rec.org_id --added for v1.13
                     AND batch_id = g_new_batch_id
                     AND run_sequence_id = g_new_run_seq_id;
            END;
            FND_FILE.PUT_LINE(FND_FILE.LOG,
                     'Org Id :'||customer_rec.org_id
                     ||'leg customer number:'||customer_rec.leg_customer_number
                     ||'Bill to address :'||customer_rec.leg_bill_to_address
                     ||'l_r12_cust_id:'||l_r12_cust_id
                     ||'Cust account site id :'||l_bill_to_addr
                     ||'l_valid_cust_flag :'||l_valid_cust_flag );
            ---drawee --------
            IF l_bill_to_addr IS NOT NULL
            THEN
               BEGIN
                  /*    print_log1_message ('BR INVOICE-drawee validation'||
                                  l_bill_to_addr||
                                  'start'
                                 );*/
                  SELECT hcas.cust_acct_site_id
                    INTO l_drawee_to_addr
                    FROM apps.hz_cust_acct_sites_all hcas,
                         apps.hz_cust_site_uses_all hcsu
                   WHERE hcsu.cust_acct_site_id = hcas.cust_acct_site_id
                 --    AND hcsu.location = TRIM (customer_rec.leg_bill_to_address)
                     AND hcsu.location  = substr(customer_rec.leg_bill_to_address,1,instr(customer_rec.leg_bill_to_address,'|')-1)
                     AND NVL (hcas.org_id, 1) = NVL (customer_rec.org_id, 1)
                     AND hcsu.status = 'A'
                     AND hcas.status = 'A'
                     AND hcsu.site_use_code = 'DRAWEE'
                     AND  hcsu.cust_acct_site_id  = l_bill_to_addr
                     AND hcas.cust_account_id = l_r12_cust_id;

                  --        l_valid_cust_flag := 'Y';
                  IF l_drawee_to_addr IS NOT NULL
                  THEN
                     print_log1_message
                             (   'Drawee is already created for bill to site'
                              || 'bill to--'
                              || l_bill_to_addr
                              || '--- Drawee Address --'
                              || l_drawee_to_addr
                             );
                  END IF;

               EXCEPTION
                  WHEN NO_DATA_FOUND
                  THEN
                     print_log1_message (   'BR INVOICE'
                                         || 'no data Found   ---'
                                         || 'start creating drawee--'
                                         || 'cust account id----' ||l_r12_cust_id
                     ||'for the bill to site ----'||l_bill_to_addr
                                        );

                     FOR drw_rec IN
                        (SELECT DISTINCT hcas.cust_acct_site_id,
                                         hcsu.primary_flag, hcsu.LOCATION,
                                         hcsu.site_use_code,
                                         hcsu.created_by_module,
                                         hcsu.ship_via, hcsu.territory_id,
                                         hcsu.gl_id_rec, hcsu.gl_id_rev,
                                         hcsu.gl_id_tax, hcsu.gl_id_freight,
                                         hcsu.gl_id_clearing,
                                         hcsu.gl_id_unbilled,
                                         hcsu.gl_id_unearned
                                    FROM apps.hz_cust_acct_sites_all hcas,
                                         apps.hz_cust_site_uses_all hcsu
                                   WHERE hcsu.cust_acct_site_id =
                                                        hcas.cust_acct_site_id
                                     --AND hcsu.LOCATION = TRIM (customer_rec.leg_bill_to_address)
                                      AND hcsu.location  = substr(customer_rec.leg_bill_to_address,1,instr(customer_rec.leg_bill_to_address,'|')-1)
                                     AND NVL (hcas.org_id, 1) =     NVL (customer_rec.org_id, 1)
                                     AND hcsu.status = 'A'
                                     AND hcas.status = 'A'
                                     AND hcsu.site_use_code = 'BILL_TO'
                                     AND hcas.cust_account_id = l_r12_cust_id)
                     LOOP
                        l_cust_site_use_rec.cust_acct_site_id :=
                                                    drw_rec.cust_acct_site_id;
                        l_cust_site_use_rec.site_use_code := 'DRAWEE';
                        l_cust_site_use_rec.primary_flag :=
                                                         drw_rec.primary_flag;
                        l_cust_site_use_rec.LOCATION := drw_rec.LOCATION;
                        l_cust_site_use_rec.payment_term_id := l_term_id;
                        --Added v1.27
                        l_cust_site_use_rec.created_by_module :=
                                                    drw_rec.created_by_module;
                        l_cust_site_use_rec.ship_via := drw_rec.ship_via;
                        l_cust_site_use_rec.territory_id :=
                                                         drw_rec.territory_id;
                        l_cust_site_use_rec.gl_id_rec := drw_rec.gl_id_rec;
                        l_cust_site_use_rec.gl_id_rev := drw_rec.gl_id_rev;
                        l_cust_site_use_rec.gl_id_tax := drw_rec.gl_id_tax;
                        l_cust_site_use_rec.gl_id_freight :=
                                                        drw_rec.gl_id_freight;
                        l_cust_site_use_rec.gl_id_clearing :=
                                                       drw_rec.gl_id_clearing;
                        l_cust_site_use_rec.gl_id_unbilled :=
                                                       drw_rec.gl_id_unbilled;
                        l_cust_site_use_rec.gl_id_unearned :=
                                                       drw_rec.gl_id_unearned;
                     END LOOP;

                     /* print_log1_message  ('BR INVOICE-site_profile_rec'||
                                  l_drawee_to_addr||
                                  'site profile'
                                 );*/
                     FOR site_profile_rec IN
                        (SELECT NVL (collector_id, 0) collector_id,
                                credit_checking, tolerance, discount_terms,
                                dunning_letters, interest_charges,
                                credit_balance_statements, credit_hold,
                                credit_rating, risk_code, override_terms,
                                dunning_letter_set_id, interest_period_days,
                                NVL (payment_grace_days,
                                     0) payment_grace_days,
                                discount_grace_days,
                                NVL (statement_cycle_id,
                                     0) statement_cycle_id, account_status,
                                percent_collectable, tax_printing_option,
                                charge_on_finance_charge_flag, clearing_days,
                                cons_inv_flag, cons_inv_type,
                                lockbox_matching_option,
                                credit_classification, created_by_module
                           FROM hz_customer_profiles
                          WHERE cust_account_id = l_r12_cust_id
                            AND site_use_id IS NOT NULL
                            AND ROWNUM = 1)
                     LOOP
                        l_customer_profile_rec.collector_id :=
                                                site_profile_rec.collector_id;
                        l_customer_profile_rec.credit_checking :=
                                             site_profile_rec.credit_checking;
                        l_customer_profile_rec.tolerance :=
                                                   site_profile_rec.tolerance;
                        l_customer_profile_rec.discount_terms :=
                                              site_profile_rec.discount_terms;
                        l_customer_profile_rec.dunning_letters :=
                                             site_profile_rec.dunning_letters;
                        l_customer_profile_rec.interest_charges :=
                                            site_profile_rec.interest_charges;
                        l_customer_profile_rec.credit_balance_statements :=
                                   site_profile_rec.credit_balance_statements;
                        l_customer_profile_rec.credit_hold :=
                                                 site_profile_rec.credit_hold;
                        l_customer_profile_rec.credit_rating :=
                                               site_profile_rec.credit_rating;
                        l_customer_profile_rec.risk_code :=
                                                   site_profile_rec.risk_code;
                        l_customer_profile_rec.override_terms :=
                                              site_profile_rec.override_terms;
                        --   l_customer_profile_rec.dunning_letters :=site_profile_rec.leg_dunning_letters;
                        l_customer_profile_rec.interest_period_days :=
                                        site_profile_rec.interest_period_days;
                        l_customer_profile_rec.payment_grace_days :=
                                          site_profile_rec.payment_grace_days;
                        l_customer_profile_rec.discount_grace_days :=
                                         site_profile_rec.discount_grace_days;
                        l_customer_profile_rec.statement_cycle_id :=
                                          site_profile_rec.statement_cycle_id;
                        l_customer_profile_rec.send_statements := 'Y';
                        l_customer_profile_rec.account_status :=
                                              site_profile_rec.account_status;
                        l_customer_profile_rec.percent_collectable :=
                                         site_profile_rec.percent_collectable;
                        l_customer_profile_rec.tax_printing_option :=
                                         site_profile_rec.tax_printing_option;
                        l_customer_profile_rec.charge_on_finance_charge_flag :=
                               site_profile_rec.charge_on_finance_charge_flag;
                        l_customer_profile_rec.clearing_days :=
                                               site_profile_rec.clearing_days;
                        l_customer_profile_rec.cons_inv_flag :=
                                               site_profile_rec.cons_inv_flag;
                        l_customer_profile_rec.cons_inv_type :=
                                               site_profile_rec.cons_inv_type;
                        l_customer_profile_rec.lockbox_matching_option :=
                                     site_profile_rec.lockbox_matching_option;
                        l_customer_profile_rec.credit_classification :=
                                       site_profile_rec.credit_classification;
                        l_customer_profile_rec.created_by_module :=
                                           site_profile_rec.created_by_module;
                        l_customer_profile_rec.cons_bill_level := NULL;
                     END LOOP;

                     hz_cust_account_site_v2pub.create_cust_site_use
                                                      ('T',
                                                       l_cust_site_use_rec,
                                                       l_customer_profile_rec,
                                                       fnd_api.g_true,
                                                       fnd_api.g_false,
                                                       l_site_use_id,
                                                       l_return_status,
                                                       l_msg_count,
                                                       l_msg_data
                                                      );
                         print_log1_message
                             (   'Drawee  creation status  --'
                              || l_msg_data  ||'use id------'||l_site_use_id
                             );

                     IF l_return_status = 'S'
                     THEN
                        l_valid_cust_flag := 'Y';

                        print_log1_message
                             (   'Drawee  creation status  --'
                              || l_return_status
                             );
                     ELSE --added for v1.8
                          l_valid_cust_flag := 'N';
                        IF l_msg_count > 1
                        THEN
                          FOR i IN 1 .. (l_msg_count - 1)
                          LOOP
                            l_msg :=
                              apps.fnd_msg_pub.get (apps.fnd_msg_pub.g_next,
                                                             apps.fnd_api.g_false
                                                            );
                            l_msg_data := l_msg_data || '-' || l_msg;
                          END LOOP;
                        ELSE
                          l_msg_data := apps.fnd_msg_pub.get (apps.fnd_msg_pub.g_first,
                                        apps.fnd_api.g_false
                                        );
                        END IF;
                        print_log1_message('Error while creating drawee business purpose :');
                        print_log1_message (l_msg_data);
                        l_err_msg:=l_msg_data;
                        l_err_code := 'ETN_BR_DRAWEE_EXCEPTION';

                        FOR r_r12_bill_err1_rec IN
                          (SELECT interface_txn_id
                             FROM xxconv.xxar_br_invoices_stg xis
                            WHERE leg_customer_number =customer_rec.leg_customer_number
                              AND leg_bill_to_address =customer_rec.leg_bill_to_address
							  AND org_id = customer_rec.org_id --added for v1.13
                              AND batch_id = g_new_batch_id
                              AND run_sequence_id = g_new_run_seq_id)
                        LOOP
                          log_errors
                            (pin_transaction_id           => r_r12_bill_err1_rec.interface_txn_id,
                            piv_source_column_name       => 'Legacy Customer number/ Legacy drawee to address',
                            piv_source_column_value      =>    customer_rec.leg_customer_number
                                                                        || ' / '
                                                                        || customer_rec.leg_bill_to_address,
                            piv_error_type               => 'ERR_VAL',
                            piv_error_code               => l_err_code,
                            piv_error_message            => l_err_msg,
                            pov_return_status            => l_log_ret_status,
                            pov_error_msg                => l_log_err_msg
                            );
                        END LOOP;

                        UPDATE xxconv.xxar_br_invoices_stg
                           SET process_flag = 'E',
                               ERROR_TYPE = 'ERR_VAL',
                               last_update_date = SYSDATE,
                               last_updated_by = g_last_updated_by,
                               last_update_login = g_login_id
                         WHERE leg_customer_number =
                                     customer_rec.leg_customer_number
                           AND leg_bill_to_address =
                                       customer_rec.leg_bill_to_address
						   AND org_id = customer_rec.org_id --added for v1.13
                           AND batch_id = g_new_batch_id
                           AND run_sequence_id = g_new_run_seq_id;

                     END IF; ---end of l_return_status = 'S'
                     --end of  v1.8
                  WHEN OTHERS
                  THEN
                     l_valid_cust_flag := 'N';
                     g_retcode := 2;
                     l_err_code := 'ETN_BR_PROCEDURE_EXCEPTION';
                     l_err_msg :=
                           'Error : Error while fetching drawee to customer cross reference'
                        || SUBSTR (SQLERRM, 1, 150);
                     print_log1_message (l_err_msg);

                     FOR r_r12_bill_err1_rec IN
                        (SELECT interface_txn_id
                           FROM xxconv.xxar_br_invoices_stg xis
                          WHERE leg_customer_number =
                                              customer_rec.leg_customer_number
                            AND leg_bill_to_address =
                                              customer_rec.leg_bill_to_address
							AND org_id = customer_rec.org_id --added for v1.13
                            AND batch_id = g_new_batch_id
                            AND run_sequence_id = g_new_run_seq_id)
                     LOOP
                        log_errors
                           (pin_transaction_id           => r_r12_bill_err1_rec.interface_txn_id,
                            piv_source_column_name       => 'Legacy Customer number/ Legacy drawee to address',
                            piv_source_column_value      =>    customer_rec.leg_customer_number
                                                            || ' / '
                                                            || customer_rec.leg_bill_to_address,
                            piv_error_type               => 'ERR_VAL',
                            piv_error_code               => l_err_code,
                            piv_error_message            => l_err_msg,
                            pov_return_status            => l_log_ret_status,
                            pov_error_msg                => l_log_err_msg
                           );
                     END LOOP;

                     UPDATE xxconv.xxar_br_invoices_stg
                        SET process_flag = 'E',
                            ERROR_TYPE = 'ERR_VAL',
                            last_update_date = SYSDATE,
                            last_updated_by = g_last_updated_by,
                            last_update_login = g_login_id
                      WHERE leg_customer_number =
                                              customer_rec.leg_customer_number
                        AND leg_bill_to_address =
                                              customer_rec.leg_bill_to_address
						AND org_id = customer_rec.org_id --added for v1.13
                        AND batch_id = g_new_batch_id
                        AND run_sequence_id = g_new_run_seq_id;
               END;
            END IF;  --IF l_bill_to_addr IS NOT NULL

        BEGIN
               /*   print_log1_message  ('BR INVOICE-validting payment method'||
                                      l_pymt_site_use_id||
                                      'site profile'
                                     );*/
            BEGIN
               SELECT hcsu.site_use_id
                 INTO l_pymt_site_use_id
                 FROM apps.hz_cust_acct_sites_all hcas,
                      apps.hz_cust_site_uses_all hcsu
                WHERE hcsu.cust_acct_site_id = hcas.cust_acct_site_id
              --    AND hcsu.location = TRIM (customer_rec.leg_bill_to_address)
                  AND hcsu.location  = substr(customer_rec.leg_bill_to_address,1,instr(customer_rec.leg_bill_to_address,'|')-1)
                  AND NVL (hcas.org_id, 1) = NVL (customer_rec.org_id, 1)
                  AND hcsu.status = 'A'
                  AND hcas.status = 'A'
                  AND hcsu.site_use_code = 'BILL_TO'
                  AND hcas.cust_account_id = l_r12_cust_id;

              EXCEPTION
              WHEN TOO_MANY_ROWS THEN

                SELECT hcsu.site_use_id
                  INTO l_pymt_site_use_id
                  FROM apps.hz_cust_acct_sites_all hcas,
                       apps.hz_cust_site_uses_all hcsu
                 WHERE hcsu.cust_acct_site_id = hcas.cust_acct_site_id
                 --    AND hcsu.location = TRIM (customer_rec.leg_bill_to_address)
                   AND hcsu.location  = substr(customer_rec.leg_bill_to_address,1,instr(customer_rec.leg_bill_to_address,'|')-1)
                   AND hcas.orig_system_reference =  TRIM(SUBSTR(customer_rec.leg_bill_to_address ,INSTR(customer_rec.leg_bill_to_address ,'|' )+1 ))
                   AND NVL (hcas.org_id, 1) = NVL (customer_rec.org_id, 1)
                   AND hcsu.status = 'A'
                   AND hcas.status = 'A'
                   AND hcsu.site_use_code = 'BILL_TO'
                   AND hcas.cust_account_id = l_r12_cust_id;

            END;
			
			FND_FILE.put_line(fnd_file.log,'Checking if receipt is already attached for  l_r12_cust_id is : ' ||l_r12_cust_id
			||' for site use id : ' ||l_pymt_site_use_id ||' and for l_r12_org_id : '|| l_r12_org_id
			||' and main org id : ' || customer_rec.org_id );
			
               l_receipt_method_id := NULL;

               BEGIN
                  SELECT acrm.receipt_method_id
                    INTO l_receipt_method_id
                    FROM ar_cust_receipt_methods_v acrm,
                         ar_receipt_methods arm,
                         ra_cust_trx_types_all rtype,
                         ar_receipt_classes arc
                   WHERE acrm.customer_id = l_r12_cust_id              --91970
                     AND acrm.site_use_id = l_pymt_site_use_id        --164035
                     AND arm.receipt_method_id = acrm.receipt_method_id
                     AND NVL (acrm.end_date, SYSDATE) >= SYSDATE
                     AND rtype.cust_trx_type_id = arm.br_cust_trx_type_id
                     AND rtype.TYPE = 'BR'
                     AND NVL (rtype.end_date, SYSDATE) >= SYSDATE
                     AND org_id = customer_rec.org_id -- modified for v1.13  l_r12_org_id
                     AND arm.receipt_class_id =arc.receipt_class_id
                     AND arc.name ='CNV Promissory Note Receipt' --v1.9
                     AND ROWNUM=1;

               EXCEPTION
                  WHEN OTHERS
                  THEN
                     l_receipt_method_id := NULL;
               END;

               FND_FILE.put_line(fnd_file.log,'Existing Receipt Method id is : ' ||l_receipt_method_id ||' for site use id : ' ||l_pymt_site_use_id);
               IF l_receipt_method_id IS NULL
               THEN
						 BEGIN
							SELECT DISTINCT arm.receipt_method_id
							  INTO l_receipt_method_id
							  FROM ar_receipt_methods arm,
								   ra_cust_trx_types_all rtta,
								   ar_receipt_classes arc
							 WHERE rtta.cust_trx_type_id =  arm.br_cust_trx_type_id
							   AND rtta.org_id = customer_rec.org_id
							   AND rtta.TYPE = 'BR'
							   AND NVL (arm.end_date, SYSDATE) >= SYSDATE
							   AND arm.receipt_class_id =arc.receipt_class_id
							   AND arc.name ='CNV Promissory Note Receipt'; --v1.9

						  EXCEPTION
						   WHEN NO_DATA_FOUND
						   THEN
							  l_err_code := 'ETN_BR_PROCEDURE_EXCEPTION';
							  l_err_msg :=
									'Error : Error while adding the receipt method is not attached to the org'||'-'||customer_rec.org_id
								 || SUBSTR (SQLERRM, 1, 150);
							  print_log1_message (l_err_msg);

								  FOR r_r12_bill_err1_rec IN
									 (SELECT interface_txn_id
										FROM xxconv.xxar_br_invoices_stg xis
									   WHERE xis. org_id = customer_rec.org_id   --For Receipt method changes
									   /* AND  leg_customer_number =
															  customer_rec.leg_customer_number*/
										 AND leg_bill_to_address =
															  customer_rec.leg_bill_to_address
										 AND org_id = customer_rec.org_id --added for v1.13
										 AND batch_id = g_new_batch_id
										 AND run_sequence_id = g_new_run_seq_id)
								  LOOP
									 log_errors
										(pin_transaction_id           => r_r12_bill_err1_rec.interface_txn_id,
										 piv_source_column_name       => 'Legacy Customer number/ Legacy drawee to address',
										 piv_source_column_value      =>    customer_rec.leg_customer_number
																		 || ' / '
																		 || customer_rec.leg_bill_to_address,
										 piv_error_type               => 'ERR_VAL',
										 piv_error_code               => l_err_code,
										 piv_error_message            => l_err_msg,
										 pov_return_status            => l_log_ret_status,
										 pov_error_msg                => l_log_err_msg
										);
								  END LOOP;

								 UPDATE xxconv.xxar_br_invoices_stg
									 SET process_flag = 'E',
										 ERROR_TYPE = 'ERR_VAL',
										 last_update_date = SYSDATE,
										 last_updated_by = g_last_updated_by,
										 last_update_login = g_login_id
								   WHERE   org_id = customer_rec.org_id --leg_customer_number =   customer_rec.leg_customer_number
									 AND leg_bill_to_address =
															  customer_rec.leg_bill_to_address
									 AND org_id = customer_rec.org_id --added for v1.13
									 AND batch_id = g_new_batch_id
									 AND run_sequence_id = g_new_run_seq_id;

								 COMMIT;
								 END;

                  l_pay_method_rec.cust_account_id := l_r12_cust_id;
                  l_pay_method_rec.receipt_method_id := l_receipt_method_id;
                  l_pay_method_rec.primary_flag := 'Y';
                  l_pay_method_rec.site_use_id := l_pymt_site_use_id;
                  l_pay_method_rec.start_date := '01-JAN-2001';
                  l_pay_method_rec.end_date := NULL;
				  
				  FND_FILE.put_line(fnd_file.log,'Before calling api Receipt Method id is : ' ||l_receipt_method_id ||' for site use id : ' ||l_pymt_site_use_id);

                  IF l_receipt_method_id IS NOT NULL
                  THEN
                     -- print_log1_message ('Attaching the receipt mthod at site level calling API')  ;
                     hz_payment_method_pub.create_payment_method
                        (p_init_msg_list               => fnd_api.g_true,
                         p_payment_method_rec          => l_pay_method_rec,
                         x_cust_receipt_method_id      => l_cust_receipt_method_id,
                         x_return_status               => l_return_status,
                         x_msg_count                   => l_msg_count,
                         x_msg_data                    => l_msg_data
                        );
                  ELSE
                     print_log1_message
                               ('Receipt mthod at site level already defined');
                  END IF;

                  IF l_return_status = 'S'
                  THEN
                     print_log1_message
                        ('Attaching the receipt mthod at site level successfully'
                        );
                  ELSE
                     print_log1_message
                        (   'ERROR:Attaching the receipt mthod at site level not successfully'
                         || l_msg_data
                        );

                     IF l_msg_count > 0
                     THEN
                        FOR i IN 1 .. l_msg_count
                        LOOP
                           l_payee_err_msg :=
                              fnd_msg_pub.get (p_msg_index      => i,
                                               p_encoded        => fnd_api.g_false
                                              );
                           print_log1_message
                              (   'ERROR:Attaching the receipt mthod at site level not successfully--'
                               || l_msg_count
                               || l_msg_data
                              );
                        END LOOP;
                     END IF;
                  END IF;
               ELSE
                  print_log1_message
                         ('Receipt method is already attached at site level.');
               END IF;                      --- if l_receipt_method_id is null
            EXCEPTION
               WHEN NO_DATA_FOUND
               THEN
                  --    l_valid_cust_flag := 'N';
                      --      g_retcode := 2;
                  l_err_code := 'ETN_BR_PROCEDURE_EXCEPTION';
                  l_err_msg :=
                        'Error : Error while adding the receipt method to the  customer site reference'
                     || SUBSTR (SQLERRM, 1, 150);
                  print_log1_message (l_err_msg);

                  FOR r_r12_bill_err1_rec IN
                     (SELECT interface_txn_id
                        FROM xxconv.xxar_br_invoices_stg xis
                       WHERE leg_customer_number =
                                              customer_rec.leg_customer_number
                         AND leg_bill_to_address =
                                              customer_rec.leg_bill_to_address
						 AND org_id = customer_rec.org_id --added for v1.13
                         AND batch_id = g_new_batch_id
                         AND run_sequence_id = g_new_run_seq_id)
                  LOOP
                     log_errors
                        (pin_transaction_id           => r_r12_bill_err1_rec.interface_txn_id,
                         piv_source_column_name       => 'Legacy Customer number/ Legacy drawee to address',
                         piv_source_column_value      =>    customer_rec.leg_customer_number
                                                         || ' / '
                                                         || customer_rec.leg_bill_to_address,
                         piv_error_type               => 'ERR_VAL',
                         piv_error_code               => l_err_code,
                         piv_error_message            => l_err_msg,
                         pov_return_status            => l_log_ret_status,
                         pov_error_msg                => l_log_err_msg
                        );
                  END LOOP;

                     UPDATE xxconv.xxar_br_invoices_stg
                     SET process_flag = 'E',
                         ERROR_TYPE = 'ERR_VAL',
                         last_update_date = SYSDATE,
                         last_updated_by = g_last_updated_by,
                         last_update_login = g_login_id
                   WHERE   org_id = customer_rec.org_id --leg_customer_number =   customer_rec.leg_customer_number
                     AND leg_bill_to_address =
                                              customer_rec.leg_bill_to_address
                     AND batch_id = g_new_batch_id
                     AND run_sequence_id = g_new_run_seq_id;

           COMMIT;
            END;

            --  external account creation--
            BEGIN
               /*    print_log1_message (   'BR INVOICE  validating dummy account '
                                       || '-'
                                       || 'START'
                                      );*/
               BEGIN
                  l_count := NULL;

                  SELECT COUNT (1)
                    INTO l_count
                    FROM iby_ext_bank_accounts ACCOUNT,
                         iby_pmt_instr_uses_all acc_instr,
                         iby_external_payers_all ext_payer,
                         hz_cust_acct_sites_all hcas,
                         hz_cust_site_uses_all hcsu
                   WHERE 1 = 1
                     AND ACCOUNT.ext_bank_account_id = acc_instr.instrument_id
                     AND acc_instr.ext_pmt_party_id = ext_payer.ext_payer_id
                     AND ext_payer.acct_site_use_id = hcsu.site_use_id
                     AND hcas.cust_account_id = ext_payer.cust_account_id
                     AND hcsu.cust_acct_site_id = hcas.cust_acct_site_id
					 AND hcas.cust_acct_site_id = l_bill_to_addr --added for v1.15 defect 13077
                     AND hcsu.site_use_code = 'BILL_TO'
                     AND hcas.org_id = hcsu.org_id
                     AND ext_payer.cust_account_id = l_r12_cust_id
                    AND acc_instr.end_date is  null
                     AND ACCOUNT.bank_account_num LIKE 'DUMMYBRACCT%';
               EXCEPTION
                  WHEN OTHERS
                  THEN
                     l_err_code :=
                              'no dummy bank  external account details found';
                     l_err_msg :=
                        'Error : Following error while checking the external bak account information : ';
                     print_log1_message (l_err_msg);
               END;

               IF l_count IS NULL OR l_count = 0
               THEN
                  BEGIN
                     l_country := NULL;
                     l_bnk := NULL;

                     SELECT DISTINCT country
                                INTO l_country
                                FROM hz_cust_acct_sites_all hcas,
                                     hz_cust_site_uses_all hcsu,
                                     hz_party_sites hps,
                                     hz_locations hl
                               WHERE hcsu.cust_acct_site_id =
                                                        hcas.cust_acct_site_id
                                 AND hcsu.site_use_code = 'BILL_TO'
                                 AND hcsu.status = 'A'
                                 AND hcas.status = 'A'
                                 AND hcas.party_site_id = hps.party_site_id
                                 AND hl.location_id = hps.location_id
                                 AND cust_account_id = l_r12_cust_id;

                     SELECT COUNT (1)
                       INTO l_bnk
                       FROM ce_banks_v cb
                      WHERE cb.home_country = l_country
                        AND cb.bank_name LIKE 'DUMMYBR%';
                  EXCEPTION
                     WHEN OTHERS
                     THEN
                        l_err_code := 'no dummy bank  details found';
                        l_err_msg :=
                              'Error : Following error while checking the external bak account information : '
                           || l_bnk;
                        print_log1_message (l_err_msg || l_bnk);
                  END;

                  l_bnk_brnch := NULL;

                  IF l_bnk IS NULL OR l_bnk = 0
                  THEN
                     create_bank (l_country, l_bank_ret_status);
                  ELSE
                     print_log1_message (   'For the cust account'
                                         || '-'
                                         || l_r12_cust_id
                                         || 'dummy bank is already defined'
                                        );
                     l_bank_ret_status := 'S';
                  END IF;

                  ---checking if bank branch is already created
                  BEGIN
                     l_bnk_brnch := NULL;

                     SELECT COUNT (1)
                       INTO l_bnk_brnch
                       FROM ce_bank_branches_v cbb
                      WHERE cbb.bank_home_country = l_country
                        AND bank_branch_name LIKE 'DUMMYBR%';
                  EXCEPTION
                     WHEN OTHERS
                     THEN
                        l_err_code := 'no dummy bank branch  details found';
                        l_err_msg :=
                              'Error : Following error while checking the external bak branch information : '
                           || l_bnk_brnch;
                        print_log1_message (l_err_msg || l_bnk_brnch);
                  END;

                  --end of checking if bank branch is already created
                  IF     l_bank_ret_status = fnd_api.g_ret_sts_success
                     AND (l_bnk_brnch IS NULL OR l_bnk_brnch = 0)
                  THEN
                     create_branch (l_country, l_bank_branch_ret_status);
                  ELSE
                     print_log1_message
                                      (   'For the cust account'
                                       || '-'
                                       || l_r12_cust_id
                                       || 'dummy bank branch is already defined'
                                      );
                     l_bank_branch_ret_status := 'S';
                  END IF;

          l_site_orig_sys_ref:=NUll;--added v1.2 Defect 5392

          IF l_bank_branch_ret_status = fnd_api.g_ret_sts_success
          THEN
            BEGIN
                --getting bill to site orig system reference
                l_site_orig_sys_ref:=substr(customer_rec.leg_bill_to_address,instr(customer_rec.leg_bill_to_address,'|',1)+1);
                --getting bill to site orig system reference
                BEGIN --added v1.2 Defect 5392
                    SELECT hca.party_id, hcsu.site_use_id
                      INTO l_acct_prty_id, l_account_site_use_id
                      FROM hz_cust_accounts_all hca,
                           hz_cust_acct_sites_all hcas,
                           hz_cust_site_uses_all hcsu
                     WHERE hca.cust_account_id = hcas.cust_account_id
                       AND hcsu.cust_acct_site_id = hcas.cust_acct_site_id
                       AND hcsu.site_use_code = 'BILL_TO'
                       AND hcsu.status = 'A'
                       AND hcas.status = 'A'
                       AND hca.cust_account_id = l_r12_cust_id
                       --AND hcas.orig_system_reference = l_site_orig_sys_ref --added for v1.2 Defect 5392 --commented for v1.15 as orig system reference was getting updated with timestamp
		       AND hcas.cust_acct_site_id = l_bill_to_addr --added for v1.15 defect 13077
                       AND hcas.org_id = customer_rec.org_id; --added for v1.2 Defect 5392

                  EXCEPTION --added v1.2 Defect 5392
                  WHEN OTHERS THEN
                  print_log1_message
                                        (   'Error: For the cust account'
                                         || '-'
                                         || l_r12_cust_id
                                         || 'while creating dummy bank account not able to fetch site details : '
                       || SQLERRM
                                        );
                  l_acct_prty_id:= NULL;
                  l_account_site_use_id := NULL;
                END; --added v1.2 Defect 5392

                SELECT gl.currency_code
                  INTO l_acct_currency_code
                  FROM hr_operating_units hou, gl_ledgers gl
                 WHERE gl.ledger_id = hou.set_of_books_id
                   AND hou.organization_id = customer_rec.org_id;

                        create_account (l_acct_prty_id,
                                        l_country,
                                        l_bank_acct_id,
                                        l_acct_currency_code,
                                        l_account_ret_status
                                       );
                        l_payee_rec.payment_function := 'CUSTOMER_PAYMENT';
                        l_payee_rec.cust_account_id := l_r12_cust_id;
                        l_payee_rec.party_id := l_acct_prty_id;
                        l_payee_rec.account_site_id := l_account_site_use_id;
                        l_payee_rec.org_id := customer_rec.org_id;
                        l_payee_rec.org_type := 'OPERATING_UNIT';
                        l_instrument_rec.instrument_id := l_bank_acct_id;
                        l_instrument_rec.instrument_type := 'BANKACCOUNT';
                        l_assignment_attribs_rec.priority := 1;
                        l_assignment_attribs_rec.start_date := '01-JAN-2001';
                        l_assignment_attribs_rec.instrument :=
                                                              l_instrument_rec;
                        iby_fndcpt_setup_pub.set_payer_instr_assignment
                            (p_api_version             => 1.0,
                             p_init_msg_list           => fnd_api.g_true,
                             p_commit                  => fnd_api.g_false,
                             x_return_status           => l_payee_ret_status,
                             x_msg_count               => l_payee_msg_count,
                             x_msg_data                => l_payee_err_msg,
                             p_payer                   => l_payee_rec,
                             p_assignment_attribs      => l_assignment_attribs_rec,
                             x_assign_id               => l_assign_id,
                             x_response                => l_payee_result_rec
                            );
                     EXCEPTION
                        WHEN OTHERS
                        THEN
                           l_err_code :=
                              'no dummy bank external bank account   details fetch';
                           l_err_msg :=
                                 'Error : Following error while checking the external bank account information for the ste : '
                              || l_account_site_use_id;
                           print_log1_message (   l_err_msg
                                               || l_account_site_use_id
                                              );
            END;

                     IF l_payee_ret_status = fnd_api.g_ret_sts_success
                     THEN
                        print_log1_message
                           (   'For Cust Account Id '
                            || '-'
                            || l_r12_cust_id
                            || ' External bank account is successfully attached.'
                           );
                     ELSE
                        print_log1_message
                           (   'For Cust Account Id '
                            || '-'
                            || l_r12_cust_id
                            || ' Error: External bank account attachment Failed..'
                           );
                     END IF;
          ELSE
                     print_log1_message
                        (   'For Cust Account : '
                         || '-'
                         || l_r12_cust_id
                         || 'ERROR : creating the  bank  account assignment  as branch is already created or API statsus is not success'
                        );
          END IF;  --IF l_bank_branch_ret_status = fnd_api.g_ret_sts_success
        ELSE
                  print_log1_message
                           (   'For the Cust Account  '
                            || '-'
                            || l_r12_cust_id
                            || ' dummy external bank account is already created'
                           );
        END IF;
        ---if l_count is null or l_count=0 if external bank account already assigned to customer bill to site

        COMMIT;
            EXCEPTION
            WHEN OTHERS
            THEN
                  l_valid_cust_flag := 'N';
                  g_retcode := 2;
                  l_err_code := 'ETN_BR_PROCEDURE_EXCEPTION';
                  l_err_msg :=
                        'Error : Error while fetching  to BANK ,BRANCH ,ACCOUNT cross reference'
                     || SUBSTR (SQLERRM, 1, 150);
                  print_log1_message (l_err_msg);

                  FOR r_r12_bill_err1_rec IN
                     (SELECT interface_txn_id
                        FROM xxconv.xxar_br_invoices_stg xis
                       WHERE leg_customer_number =
                                              customer_rec.leg_customer_number
                         AND leg_bill_to_address =
                                              customer_rec.leg_bill_to_address
						 AND org_id = customer_rec.org_id --added for v1.13
                         AND batch_id = g_new_batch_id
                         AND run_sequence_id = g_new_run_seq_id)
                  LOOP
                     log_errors
                        (pin_transaction_id      => r_r12_bill_err1_rec.interface_txn_id,
                         piv_source_column_name  => 'Legacy Customer number/ Legacy drawee to address',
                         piv_source_column_value => customer_rec.leg_customer_number
                                                    || ' / '|| customer_rec.leg_bill_to_address,
                         piv_error_type          => 'ERR_VAL',
                         piv_error_code          => l_err_code,
                         piv_error_message       => l_err_msg,
                         pov_return_status       => l_log_ret_status,
                         pov_error_msg           => l_log_err_msg
                        );
                  END LOOP;

                  UPDATE xxconv.xxar_br_invoices_stg
                     SET process_flag = 'E',
                         ERROR_TYPE = 'ERR_VAL',
                         last_update_date = SYSDATE,
                         last_updated_by = g_last_updated_by,
                         last_update_login = g_login_id
                   WHERE leg_customer_number =
                                              customer_rec.leg_customer_number
                     AND leg_bill_to_address =
                                              customer_rec.leg_bill_to_address
					 AND org_id = customer_rec.org_id --added for v1.13
                     AND batch_id = g_new_batch_id
                     AND run_sequence_id = g_new_run_seq_id;
        END;

    ELSE   --IF  l_r12_cust_id IS NOT NULL
        l_err_msg :=
                    'Error : Custmer is not Created in the R12'
                    || SUBSTR (SQLERRM, 1, 150);

        FOR r_r12_ship_err_rec IN
                        (SELECT interface_txn_id
                           FROM xxconv.xxar_br_invoices_stg xis
                          WHERE leg_customer_number =
                                             customer_rec.leg_customer_number
                            AND leg_ship_to_address =
                                              customer_rec.leg_ship_to_address
							AND org_id = customer_rec.org_id --added for v1.13
                            AND batch_id = g_new_batch_id
                            AND run_sequence_id = g_new_run_seq_id)
                     LOOP
                        l_err_code := 'ETN_BR_SHIP_CUSTOMER_ERROR';
                        l_err_msg :=
                           'Error : Error : Custmer is not Created in the R12';
                        log_errors
                           (pin_transaction_id           => r_r12_ship_err_rec.interface_txn_id,
                            piv_source_column_name       => 'Legacy Customer number/ Legacy ship to address',
                            piv_source_column_value      =>    customer_rec.leg_customer_number
                                                            || ' / '
                                                            || customer_rec.leg_ship_to_address,
                            piv_error_type               => 'ERR_VAL',
                            piv_error_code               => l_err_code,
                            piv_error_message            => l_err_msg,
                            pov_return_status            => l_log_ret_status,
                            pov_error_msg                => l_log_err_msg
                           );
                     END LOOP;
    END IF;

-----------
   -- ship to
            IF customer_rec.leg_ship_to_address IS NOT NULL
            THEN
               BEGIN
                  SELECT hcas.cust_acct_site_id
                    INTO l_ship_to_addr
                    FROM apps.hz_cust_acct_sites_all hcas,
                         apps.hz_cust_site_uses_all hcsu
                   WHERE hcsu.cust_acct_site_id = hcas.cust_acct_site_id
                     AND hcsu.LOCATION =
                                       TRIM (customer_rec.leg_ship_to_address)
                     AND NVL (hcas.org_id, 1) = NVL (customer_rec.org_id, 1)
                     AND hcsu.status = 'A'
                     AND hcas.status = 'A'
                     AND hcsu.site_use_code = 'SHIP_TO'
                     AND hcas.cust_account_id = l_r12_cust_id;

                  --ver1.8 changes end
                  l_valid_cust_flag := 'Y';
               EXCEPTION
                  WHEN NO_DATA_FOUND
                  THEN
                     l_valid_cust_flag := 'N';
                     g_retcode := 1;

                     FOR r_r12_ship_err_rec IN
                        (SELECT interface_txn_id
                           FROM xxconv.xxar_br_invoices_stg xis
                          WHERE leg_customer_number =
                                             customer_rec.leg_customer_number
                            AND leg_ship_to_address =
                                              customer_rec.leg_ship_to_address
							AND org_id = customer_rec.org_id --added for v1.13
                            AND batch_id = g_new_batch_id
                            AND run_sequence_id = g_new_run_seq_id)
                     LOOP
                        l_err_code := 'ETN_BR_SHIP_CUSTOMER_ERROR';
                        l_err_msg :=
                           'Error : Cross reference not defined for ship to customer';
                        log_errors
                           (pin_transaction_id           => r_r12_ship_err_rec.interface_txn_id,
                            piv_source_column_name       => 'Legacy Customer number/ Legacy ship to address',
                            piv_source_column_value      =>    customer_rec.leg_customer_number
                                                            || ' / '
                                                            || customer_rec.leg_ship_to_address,
                            piv_error_type               => 'ERR_VAL',
                            piv_error_code               => l_err_code,
                            piv_error_message            => l_err_msg,
                            pov_return_status            => l_log_ret_status,
                            pov_error_msg                => l_log_err_msg
                           );
                     END LOOP;

                     UPDATE xxconv.xxar_br_invoices_stg
                        SET process_flag = 'E',
                            ERROR_TYPE = 'ERR_VAL',
                            last_update_date = SYSDATE,
                            last_updated_by = g_last_updated_by,
                            last_update_login = g_login_id
                      WHERE leg_customer_number =
                                              customer_rec.leg_customer_number
                        AND leg_ship_to_address =
                                              customer_rec.leg_ship_to_address
						AND org_id = customer_rec.org_id --added for v1.13
                        AND batch_id = g_new_batch_id
                        AND run_sequence_id = g_new_run_seq_id;
                  WHEN OTHERS
                  THEN
                     l_valid_cust_flag := 'N';
                     l_err_code := 'ETN_BR_PROCEDURE_EXCEPTION';
                     g_retcode := 2;
                     l_err_msg :=
                           'Error : Error while fetching bill to customer cross reference'
                        || SUBSTR (SQLERRM, 1, 150);
                     print_log1_message (l_err_msg);

                     FOR r_r12_ship_err1_rec IN
                        (SELECT interface_txn_id
                           FROM xxconv.xxar_br_invoices_stg xis
                          WHERE leg_customer_number =
                                              customer_rec.leg_customer_number
                            AND leg_ship_to_address =
                                              customer_rec.leg_ship_to_address
							AND org_id = customer_rec.org_id --added for v1.13
                            AND batch_id = g_new_batch_id
                            AND run_sequence_id = g_new_run_seq_id)
                     LOOP
                        log_errors
                           (pin_transaction_id           => r_r12_ship_err1_rec.interface_txn_id,
                            piv_source_column_name       => 'Legacy Customer number/ Legacy ship to address',
                            piv_source_column_value      =>    customer_rec.leg_customer_number
                                                            || ' / '
                                                            || customer_rec.leg_ship_to_address,
                            piv_error_type               => 'ERR_VAL',
                            piv_error_code               => l_err_code,
                            piv_error_message            => l_err_msg,
                            pov_return_status            => l_log_ret_status,
                            pov_error_msg                => l_log_err_msg
                           );
                     END LOOP;

                     UPDATE xxconv.xxar_br_invoices_stg
                        SET process_flag = 'E',
                            ERROR_TYPE = 'ERR_VAL',
                            last_update_date = SYSDATE,
                            last_updated_by = g_last_updated_by,
                            last_update_login = g_login_id
                      WHERE leg_customer_number =
                                              customer_rec.leg_customer_number
                        AND leg_ship_to_address =
                                              customer_rec.leg_ship_to_address
						AND org_id = customer_rec.org_id --added for v1.13
                        AND batch_id = g_new_batch_id
                        AND run_sequence_id = g_new_run_seq_id;
               END;
            ELSE
               l_valid_cust_flag := 'Y';
            END IF;

            IF l_valid_cust_flag = 'Y'
            THEN
               UPDATE xxconv.xxar_br_invoices_stg
                  SET system_ship_address_id = l_ship_to_addr,
                      system_bill_customer_id = l_r12_cust_id,
                      system_bill_address_id = l_bill_to_addr
                WHERE leg_customer_number = customer_rec.leg_customer_number
                  AND leg_bill_to_address = customer_rec.leg_bill_to_address
				  AND org_id = customer_rec.org_id -- Added for v1.13
                  AND NVL (leg_ship_to_address, 'NO SHIP') =
                         NVL (customer_rec.leg_ship_to_address,
                              NVL (leg_ship_to_address, 'NO SHIP')
                             )
                  AND batch_id = g_new_batch_id
                  AND run_sequence_id = g_new_run_seq_id;

               COMMIT;
            END IF;

            --       END LOOP;
            COMMIT;
         --
         END IF;
      END LOOP;

      COMMIT;

      FOR currency_rec IN currency_cur
      LOOP
         l_curr_code := NULL;

         BEGIN
            /*  print_log_message (   'Validating legacy currency code '
                                 || currency_rec.leg_currency_code
                                );*/
            SELECT 1
              INTO l_curr_code
              FROM fnd_currencies fc
             WHERE fc.currency_code = currency_rec.leg_currency_code
               AND fc.enabled_flag = 'Y'
               AND fc.currency_flag = 'Y'
               AND TRUNC (SYSDATE) BETWEEN TRUNC (NVL (fc.start_date_active,
                                                       SYSDATE
                                                      )
                                                 )
                                       AND TRUNC (NVL (fc.end_date_active,
                                                       SYSDATE
                                                      )
                                                 );

            UPDATE xxconv.xxar_br_invoices_stg
               SET currency_code = currency_rec.leg_currency_code
             WHERE leg_currency_code = currency_rec.leg_currency_code
               AND batch_id = g_new_batch_id
               AND run_sequence_id = g_new_run_seq_id;

            COMMIT;
         EXCEPTION
            WHEN NO_DATA_FOUND
            THEN
               FOR r_curr_err_rec IN
                  (SELECT interface_txn_id
                     FROM xxconv.xxar_br_invoices_stg xis
                    WHERE leg_currency_code = currency_rec.leg_currency_code
                      AND batch_id = g_new_batch_id
                      AND run_sequence_id = g_new_run_seq_id)
               LOOP
                  l_err_code := 'ETN_BR_CURRENCY_ERROR';
                  g_retcode := 1;
                  l_err_msg := 'Error : Currency not found in R12 ';
                  print_log1_message (   l_err_msg
                                      || currency_rec.leg_currency_code
                                     );
                  log_errors
                     (pin_transaction_id           => r_curr_err_rec.interface_txn_id,
                      piv_source_column_name       => 'Legacy currency code',
                      piv_source_column_value      => currency_rec.leg_currency_code,
                      piv_error_type               => 'ERR_VAL',
                      piv_error_code               => l_err_code,
                      piv_error_message            => l_err_msg,
                      pov_return_status            => l_log_ret_status,
                      pov_error_msg                => l_log_err_msg
                     );
               END LOOP;

               UPDATE xxconv.xxar_br_invoices_stg
                  SET process_flag = 'E',
                      ERROR_TYPE = 'ERR_VAL',
                      last_update_date = SYSDATE,
                      last_updated_by = g_last_updated_by,
                      last_update_login = g_login_id
                WHERE leg_currency_code = currency_rec.leg_currency_code
                  AND batch_id = g_new_batch_id
                  AND run_sequence_id = g_new_run_seq_id;

               COMMIT;
            WHEN OTHERS
            THEN
               l_err_code := 'ETN_BR_PROCEDURE_EXCEPTION';
               g_retcode := 2;
               l_err_msg :=
                     'Error : Error validating currency '
                  || SUBSTR (SQLERRM, 1, 150);
               print_log1_message (l_err_msg || currency_rec.leg_currency_code);

               FOR r_curr_err1_rec IN
                  (SELECT interface_txn_id
                     FROM xxconv.xxar_br_invoices_stg xis
                    WHERE leg_currency_code = currency_rec.leg_currency_code
                      AND batch_id = g_new_batch_id
                      AND run_sequence_id = g_new_run_seq_id)
               LOOP
                  log_errors
                     (pin_transaction_id           => r_curr_err1_rec.interface_txn_id,
                      piv_source_column_name       => 'Legacy currency code',
                      piv_source_column_value      => currency_rec.leg_currency_code,
                      piv_error_type               => 'ERR_VAL',
                      piv_error_code               => l_err_code,
                      piv_error_message            => l_err_msg,
                      pov_return_status            => l_log_ret_status,
                      pov_error_msg                => l_log_err_msg
                     );
               END LOOP;

               UPDATE xxconv.xxar_br_invoices_stg
                  SET process_flag = 'E',
                      ERROR_TYPE = 'ERR_VAL',
                      last_update_date = SYSDATE,
                      last_updated_by = g_last_updated_by,
                      last_update_login = g_login_id
                WHERE leg_currency_code = currency_rec.leg_currency_code
                  AND batch_id = g_new_batch_id
                  AND run_sequence_id = g_new_run_seq_id;
         END;
      END LOOP;

      COMMIT;

      FOR trx_type_rec IN trx_type_cur
      LOOP
         l_trx_type := NULL;
         l_trx_type_id := NULL;
         l_trx_type_name := NULL;
         l_cm_term_error := 'N';

         BEGIN
            /* print_log_message (   'Validating legacy transaction type '
                                || trx_type_rec.leg_cust_trx_type_name
                               );*/
            SELECT TRIM (flv.description)
              INTO l_trx_type
              FROM fnd_lookup_values flv
             WHERE TRIM (UPPER (flv.meaning)) =
                            TRIM (UPPER (trx_type_rec.leg_cust_trx_type_name))
               AND flv.LANGUAGE = USERENV ('LANG')
               AND flv.enabled_flag = 'Y'
           --    AND UPPER (flv.lookup_type) = g_trx_type_lookup
         AND flv.lookup_type = g_trx_type_lookup
               AND TRUNC (SYSDATE) BETWEEN TRUNC (NVL (flv.start_date_active,
                                                       SYSDATE
                                                      )
                                                 )
                                       AND TRUNC (NVL (flv.end_date_active,
                                                       SYSDATE
                                                      )
                                                 );
         EXCEPTION
            WHEN NO_DATA_FOUND
            THEN
               g_retcode := 1;

               FOR r_trx_type_ref_err_rec IN
                  (SELECT interface_txn_id
                     FROM xxconv.xxar_br_invoices_stg xis
                    WHERE leg_cust_trx_type_name =
                                          trx_type_rec.leg_cust_trx_type_name
                      AND batch_id = g_new_batch_id
                      AND run_sequence_id = g_new_run_seq_id)
               LOOP
                  l_err_code := 'ETN_BR_TRX_TYPE_ERROR';
                  l_err_msg :=
                     'Error : Cross reference not defined for transaction type';
                  log_errors
                     (pin_transaction_id           => r_trx_type_ref_err_rec.interface_txn_id,
                      piv_source_column_name       => 'Legacy Transaction type',
                      piv_source_column_value      => trx_type_rec.leg_cust_trx_type_name,
                      piv_error_type               => 'ERR_VAL',
                      piv_error_code               => l_err_code,
                      piv_error_message            => l_err_msg,
                      pov_return_status            => l_log_ret_status,
                      pov_error_msg                => l_log_err_msg
                     );
               END LOOP;

               UPDATE xxconv.xxar_br_invoices_stg
                  SET process_flag = 'E',
                      ERROR_TYPE = 'ERR_VAL',
                      last_update_date = SYSDATE,
                      last_updated_by = g_last_updated_by,
                      last_update_login = g_login_id
                WHERE leg_cust_trx_type_name =
                                           trx_type_rec.leg_cust_trx_type_name
                  AND org_id = trx_type_rec.org_id
                  AND batch_id = g_new_batch_id
                  AND run_sequence_id = g_new_run_seq_id;

               COMMIT;
            WHEN OTHERS
            THEN
               l_err_code := 'ETN_BR_PROCEDURE_EXCEPTION';
               g_retcode := 2;
               l_err_msg :=
                     'Error : Error deriving transaction type from cross reference'
                  || SUBSTR (SQLERRM, 1, 150);

               FOR r_trx_type_ref_err1_rec IN
                  (SELECT interface_txn_id
                     FROM xxconv.xxar_br_invoices_stg xis
                    WHERE leg_cust_trx_type_name =
                                           trx_type_rec.leg_cust_trx_type_name
                      AND batch_id = g_new_batch_id
                      AND run_sequence_id = g_new_run_seq_id)
               LOOP
                  log_errors
                     (pin_transaction_id           => r_trx_type_ref_err1_rec.interface_txn_id,
                      piv_source_column_name       => 'Legacy Transaction type',
                      piv_source_column_value      => trx_type_rec.leg_cust_trx_type_name,
                      piv_error_type               => 'ERR_VAL',
                      piv_error_code               => l_err_code,
                      piv_error_message            => l_err_msg,
                      pov_return_status            => l_log_ret_status,
                      pov_error_msg                => l_log_err_msg
                     );
               END LOOP;

               UPDATE xxconv.xxar_br_invoices_stg
                  SET process_flag = 'E',
                      ERROR_TYPE = 'ERR_VAL',
                      last_update_date = SYSDATE,
                      last_updated_by = g_last_updated_by,
                      last_update_login = g_login_id
                WHERE leg_cust_trx_type_name =
                                           trx_type_rec.leg_cust_trx_type_name
                  AND org_id = trx_type_rec.org_id
                  AND batch_id = g_new_batch_id
                  AND run_sequence_id = g_new_run_seq_id;
         END;

         BEGIN
            IF l_trx_type IS NOT NULL
            THEN
               print_log_message (   'Validating R12 transaction type '
                                  || l_trx_type
                                 );

               SELECT rct.cust_trx_type_id, rct.TYPE
                 INTO l_trx_type_id, l_trx_type_name
                 FROM ra_cust_trx_types_all rct
                WHERE UPPER (rct.NAME) = UPPER (l_trx_type)
                  AND org_id = trx_type_rec.org_id
                  AND TRUNC (SYSDATE) BETWEEN TRUNC (NVL (rct.start_date,
                                                          SYSDATE
                                                         )
                                                    )
                                          AND TRUNC (NVL (rct.end_date,
                                                          SYSDATE
                                                         )
                                                    );

               IF l_trx_type_name IN ('CB')
               THEN
                  g_retcode := 1;

                  FOR r_chgbck_err_rec IN
                     (SELECT interface_txn_id
                        FROM xxconv.xxar_br_invoices_stg xis
                       WHERE leg_cust_trx_type_name =
                                          trx_type_rec.leg_cust_trx_type_name
                         AND batch_id = g_new_batch_id
                         AND run_sequence_id = g_new_run_seq_id)
                  LOOP
                     l_err_code := 'ETN_BR_TRX_TYPE_ERROR';
                     l_err_msg :=
                              'Error : Invalid transaction type: Chargeback ';
                     log_errors
                        (pin_transaction_id           => r_chgbck_err_rec.interface_txn_id,
                         piv_source_column_name       => 'R12 Transaction type',
                         piv_source_column_value      => l_trx_type,
                         piv_error_type               => 'ERR_VAL',
                         piv_error_code               => l_err_code,
                         piv_error_message            => l_err_msg,
                         pov_return_status            => l_log_ret_status,
                         pov_error_msg                => l_log_err_msg
                        );
                  END LOOP;

                  UPDATE xxconv.xxar_br_invoices_stg
                     SET process_flag = 'E',
                         ERROR_TYPE = 'ERR_VAL',
                         last_update_date = SYSDATE,
                         last_updated_by = g_last_updated_by,
                         last_update_login = g_login_id
                   WHERE leg_cust_trx_type_name =
                                           trx_type_rec.leg_cust_trx_type_name
                     AND org_id = trx_type_rec.org_id
                     AND batch_id = g_new_batch_id
                     AND run_sequence_id = g_new_run_seq_id;
               ELSE
                  IF l_trx_type_name IN ('CM')
                  THEN
                     l_cm_term_error := 'N';

                     FOR r_cm_err_rec IN
                        (SELECT interface_txn_id, leg_term_name
                           FROM xxconv.xxar_br_invoices_stg xis
                          WHERE leg_cust_trx_type_name =
                                          trx_type_rec.leg_cust_trx_type_name
                            AND org_id = trx_type_rec.org_id
                            AND leg_term_name IS NOT NULL
                            AND batch_id = g_new_batch_id
                            AND run_sequence_id = g_new_run_seq_id)
                     LOOP
                        l_cm_term_error := 'Y';
                        l_err_code := 'ETN_BR_PMT_TERM_ERROR';
                        g_retcode := 1;
                        l_err_msg :=
                           'Error : Payment term is NOT NULL for Credit memo transaction';
                        log_errors
                           (pin_transaction_id           => r_cm_err_rec.interface_txn_id,
                            piv_source_column_name       => 'Legacy Payment term',
                            piv_source_column_value      => r_cm_err_rec.leg_term_name,
                            piv_error_type               => 'ERR_VAL',
                            piv_error_code               => l_err_code,
                            piv_error_message            => l_err_msg,
                            pov_return_status            => l_log_ret_status,
                            pov_error_msg                => l_log_err_msg
                           );
                     END LOOP;

                     UPDATE xxconv.xxar_br_invoices_stg
                        SET process_flag = 'E',
                            ERROR_TYPE = 'ERR_VAL',
                            last_update_date = SYSDATE,
                            last_updated_by = g_last_updated_by,
                            last_update_login = g_login_id
                      WHERE leg_cust_trx_type_name =
                                           trx_type_rec.leg_cust_trx_type_name
                        AND org_id = trx_type_rec.org_id
                        AND leg_term_name IS NOT NULL
                        AND batch_id = g_new_batch_id
                        AND run_sequence_id = g_new_run_seq_id;
                  END IF;

                  IF NVL (l_cm_term_error, 'N') = 'N'
                  THEN
                     UPDATE xxconv.xxar_br_invoices_stg
                        SET transaction_type_id = l_trx_type_id,
                            trx_type = l_trx_type_name,
                            cust_trx_type_name = l_trx_type
                      WHERE leg_cust_trx_type_name =
                                           trx_type_rec.leg_cust_trx_type_name
                        AND org_id = trx_type_rec.org_id
                        AND batch_id = g_new_batch_id
                        AND run_sequence_id = g_new_run_seq_id;

                     COMMIT;
                  END IF;
               END IF;
            END IF;
         EXCEPTION
            WHEN NO_DATA_FOUND
            THEN
               FOR r_r12_trx_type_err_rec IN
                  (SELECT interface_txn_id
                     FROM xxconv.xxar_br_invoices_stg xis
                    WHERE leg_cust_trx_type_name =
                                          trx_type_rec.leg_cust_trx_type_name
                      AND org_id = trx_type_rec.org_id
                      AND batch_id = g_new_batch_id
                      AND run_sequence_id = g_new_run_seq_id)
               LOOP
                  l_err_code := 'ETN_BR_TRX_TYPE_ERROR';
                  l_err_msg :=
                        'Error : Transaction type not setup in R12 for organization id '
                     || trx_type_rec.org_id;
                  g_retcode := 1;
                  log_errors
                     (pin_transaction_id           => r_r12_trx_type_err_rec.interface_txn_id,
                      piv_source_column_name       => 'R12 Transaction type',
                      piv_source_column_value      => l_trx_type,
                      piv_error_type               => 'ERR_VAL',
                      piv_error_code               => l_err_code,
                      piv_error_message            => l_err_msg,
                      pov_return_status            => l_log_ret_status,
                      pov_error_msg                => l_log_err_msg
                     );
               END LOOP;

               UPDATE xxconv.xxar_br_invoices_stg
                  SET process_flag = 'E',
                      ERROR_TYPE = 'ERR_VAL',
                      last_update_date = SYSDATE,
                      last_updated_by = g_last_updated_by,
                      last_update_login = g_login_id
                WHERE leg_cust_trx_type_name =
                                           trx_type_rec.leg_cust_trx_type_name
                  AND org_id = trx_type_rec.org_id
                  AND batch_id = g_new_batch_id
                  AND run_sequence_id = g_new_run_seq_id;
            WHEN OTHERS
            THEN
               l_err_code := 'ETN_BR_PROCEDURE_EXCEPTION';
               l_err_msg :=
                     'Error : Error fetching/ updating R12 Transaction type'
                  || SUBSTR (SQLERRM, 1, 150);
               g_retcode := 2;

               FOR r_r12_trx_type_err1_rec IN
                  (SELECT interface_txn_id
                     FROM xxconv.xxar_br_invoices_stg xis
                    WHERE leg_cust_trx_type_name =
                                           trx_type_rec.leg_cust_trx_type_name
                      AND org_id = trx_type_rec.org_id
                      AND batch_id = g_new_batch_id
                      AND run_sequence_id = g_new_run_seq_id)
               LOOP
                  log_errors
                     (pin_transaction_id           => r_r12_trx_type_err1_rec.interface_txn_id,
                      piv_source_column_name       => 'R12 Transaction type',
                      piv_source_column_value      => l_trx_type,
                      piv_error_type               => 'ERR_VAL',
                      piv_error_code               => l_err_code,
                      piv_error_message            => l_err_msg,
                      pov_return_status            => l_log_ret_status,
                      pov_error_msg                => l_log_err_msg
                     );
               END LOOP;

               UPDATE xxconv.xxar_br_invoices_stg
                  SET process_flag = 'E',
                      ERROR_TYPE = 'ERR_VAL',
                      last_update_date = SYSDATE,
                      last_updated_by = g_last_updated_by,
                      last_update_login = g_login_id
                WHERE leg_cust_trx_type_name =
                                           trx_type_rec.leg_cust_trx_type_name
                  AND org_id = trx_type_rec.org_id
                  AND batch_id = g_new_batch_id
                  AND run_sequence_id = g_new_run_seq_id;
         END;
      END LOOP;

      COMMIT;

      FOR gl_date_rec IN gl_date_cur
      LOOP
         BEGIN
            /*  print_log_message (   'Validating gl date for Service contracts '
                                 || gl_date_rec.leg_gl_date
                                );*/
            SELECT 1
              INTO l_gl_status
              FROM gl_periods glp, gl_period_statuses gps
             WHERE UPPER (glp.period_name) = UPPER (gps.period_name)
               AND glp.period_set_name =
                                    NVL (g_period_set_name, 'ETN Corp Calend')
               AND gl_date_rec.leg_gl_date BETWEEN glp.start_date AND glp.end_date
               AND gps.application_id =
                                 (SELECT fap.application_id
                                    FROM fnd_application_vl fap
                                   WHERE fap.application_short_name = 'SQLGL')
               AND gps.closing_status = 'O'
               AND ledger_id = gl_date_rec.ledger_id;
         EXCEPTION
            WHEN NO_DATA_FOUND
            THEN
               l_gl_error := 'Y';
               l_err_code := 'ETN_BR_GL_PERIOD_ERROR';
               g_retcode := 1;
               l_err_msg :=
                     'GL Period is not open/defined for SERVICE CONTRACTS for GL date '
                  || gl_date_rec.leg_gl_date;

               FOR r_gl_oks_err_rec IN (SELECT interface_txn_id
                                          FROM xxconv.xxar_br_invoices_stg xis
                                         WHERE leg_gl_date =
                                                       gl_date_rec.leg_gl_date
                                           AND batch_id = g_new_batch_id
                                           AND run_sequence_id =
                                                              g_new_run_seq_id)
               LOOP
                  log_errors
                     (pin_transaction_id           => r_gl_oks_err_rec.interface_txn_id,
                      piv_source_column_name       => 'GL Period date',
                      piv_source_column_value      => gl_date_rec.leg_gl_date,
                      piv_error_type               => 'ERR_VAL',
                      piv_error_code               => l_err_code,
                      piv_error_message            => l_err_msg,
                      pov_return_status            => l_log_ret_status,
                      pov_error_msg                => l_log_err_msg
                     );
               END LOOP;

               UPDATE xxconv.xxar_br_invoices_stg
                  SET process_flag = 'E',
                      ERROR_TYPE = 'ERR_VAL',
                      last_update_date = SYSDATE,
                      last_updated_by = g_last_updated_by,
                      last_update_login = g_login_id
                WHERE leg_gl_date = gl_date_rec.leg_gl_date
                  AND batch_id = g_new_batch_id
                  AND run_sequence_id = g_new_run_seq_id;
            WHEN OTHERS
            THEN
               l_gl_error := 'Y';
               g_retcode := 2;
               l_err_code := 'ETN_BR_PROCEDURE_EXCEPTION';
               l_err_msg :=
                     'Error : Error validating gl period for SERVICE CONTRACTS '
                  || gl_date_rec.leg_gl_date
                  || SUBSTR (SQLERRM, 1, 150);

               FOR r_gl_oks_err1_rec IN
                  (SELECT interface_txn_id
                     FROM xxconv.xxar_br_invoices_stg xis
                    WHERE leg_gl_date = gl_date_rec.leg_gl_date
                      AND batch_id = g_new_batch_id
                      AND run_sequence_id = g_new_run_seq_id)
               LOOP
                  log_errors
                     (pin_transaction_id           => r_gl_oks_err1_rec.interface_txn_id,
                      piv_source_column_name       => 'GL Period date',
                      piv_source_column_value      => gl_date_rec.leg_gl_date,
                      piv_error_type               => 'ERR_VAL',
                      piv_error_code               => l_err_code,
                      piv_error_message            => l_err_msg,
                      pov_return_status            => l_log_ret_status,
                      pov_error_msg                => l_log_err_msg
                     );
               END LOOP;

               UPDATE xxconv.xxar_br_invoices_stg
                  SET process_flag = 'E',
                      ERROR_TYPE = 'ERR_VAL',
                      last_update_date = SYSDATE,
                      last_updated_by = g_last_updated_by,
                      last_update_login = g_login_id
                WHERE leg_gl_date = gl_date_rec.leg_gl_date
                  AND batch_id = g_new_batch_id
                  AND run_sequence_id = g_new_run_seq_id;
         END;
      END LOOP;

      COMMIT;

      --ver 1.7 changes start
      BEGIN
         SELECT rul.rule_id
           INTO l_inv_rule_id
           FROM ra_rules rul
          WHERE UPPER (rul.NAME) = 'ADVANCE INVOICE'
            AND rul.status = 'A'
            AND rul.TYPE = 'I';
      EXCEPTION
         WHEN OTHERS
         THEN
            l_inv_rule_err := 1;
            g_retcode := 2;
            l_inv_rule_err_msg :=
                  'Error while deriving invoicing rule '
               || SUBSTR (SQLERRM, 1, 150);
      END;

      FOR accounting_rec IN accounting_cur
      LOOP
         IF accounting_rec.leg_agreement_name IS NOT NULL
         THEN
            l_acc_rule_id := NULL;
            l_acc_rule_exists := 1;

            BEGIN
               --check if the currency code exists in the system
               SELECT rul.rule_id
                 INTO l_acc_rule_id
                 FROM ra_rules rul
                WHERE rul.NAME = accounting_rec.leg_agreement_name
                  AND rul.status = 'A'
                  AND rul.TYPE = 'A';

               UPDATE xxconv.xxar_br_invoices_stg
                  SET agreement_id = l_acc_rule_id,
                      invoicing_rule_id = l_inv_rule_id
                WHERE leg_agreement_name = accounting_rec.leg_agreement_name
                  AND batch_id = g_new_batch_id
                  AND run_sequence_id = g_new_run_seq_id;
            EXCEPTION
               WHEN NO_DATA_FOUND
               THEN
                  l_valerr_cnt := 2;
                  g_retcode := 1;
                  print_log_message ('Accounting rule not found');
                  l_err_code := 'ETN_AP_INVALID_ACC_RULE';
                  l_err_msg := 'Accounting rule is not Valid';
               WHEN OTHERS
               THEN
                  l_valerr_cnt := 2;
                  g_retcode := 2;
                  print_log_message
                                (   'In When others of accounting rule check'
                                 || SQLERRM
                                );
                  l_err_code := 'ETN_AP_INVALID_ACC_RULE';
                  l_err_msg :=
                        'Error while deriving accounting rule '
                     || SUBSTR (SQLERRM, 1, 150);
            END;

            IF l_valerr_cnt = 2
            THEN
               UPDATE xxconv.xxar_br_invoices_stg
                  SET process_flag = 'E',
                      ERROR_TYPE = 'ERR_VAL',
                      last_update_date = SYSDATE,
                      last_updated_by = g_last_updated_by,
                      last_update_login = g_login_id
                WHERE leg_agreement_name = accounting_rec.leg_agreement_name
                  AND batch_id = g_new_batch_id
                  AND run_sequence_id = g_new_run_seq_id;

               FOR r_acc_err_rec IN
                  (SELECT interface_txn_id
                     FROM xxconv.xxar_br_invoices_stg xis
                    WHERE leg_agreement_name =
                                             accounting_rec.leg_agreement_name
                      AND batch_id = g_new_batch_id
                      AND run_sequence_id = g_new_run_seq_id)
               LOOP
                  log_errors
                     (pin_transaction_id           => r_acc_err_rec.interface_txn_id,
                      piv_source_column_name       => 'Accounting rule',
                      piv_source_column_value      => accounting_rec.leg_agreement_name,
                      piv_error_type               => 'ERR_VAL',
                      piv_error_code               => l_err_code,
                      piv_error_message            => l_err_msg,
                      pov_return_status            => l_log_ret_status,
                      pov_error_msg                => l_log_err_msg
                     );
               END LOOP;
            END IF;
         END IF;
      END LOOP;

      COMMIT;

      IF l_acc_rule_exists = 1 AND l_inv_rule_err = 1
      THEN
         print_log_message (l_inv_rule_err_msg);
         l_err_code := 'ETN_AP_INVALID_INV_RULE';
         l_err_msg := l_inv_rule_err_msg;
         g_retcode := 1;
         log_errors (piv_source_column_name       => 'Invoicing rule',
                     piv_source_column_value      => 'Advance Invoice',
                     piv_error_type               => 'ERR_VAL',
                     piv_error_code               => l_err_code,
                     piv_error_message            => l_err_msg,
                     pov_return_status            => l_log_ret_status,
                     pov_error_msg                => l_log_err_msg
                    );
      END IF;

      --ver 1.7 changes end
      FOR tax_rec IN tax_cur
      LOOP
         l_tax_code_r12 := NULL;
         l_tax_r12 := NULL;
         l_tax_regime_code := NULL;
         l_tax_rate_code := NULL;
         l_tax := NULL;
         l_tax_status_code := NULL;
         l_tax_jurisdiction_code := NULL;

         IF tax_rec.leg_tax_code IS NOT NULL AND tax_rec.org_id IS NOT NULL
         THEN
            BEGIN
               /* print_log_message (   'Validating legacy tax code '
                                   || tax_rec.leg_tax_code
                                  );*/
               SELECT flv.description
                 INTO l_tax_code_r12
                 FROM apps.fnd_lookup_values flv
                WHERE TRIM (UPPER (flv.meaning)) =
                                           TRIM (UPPER (tax_rec.leg_tax_code))
                  AND flv.enabled_flag = 'Y'
                  AND UPPER (flv.lookup_type) = g_tax_code_lookup
                  AND TRUNC (SYSDATE) BETWEEN TRUNC
                                                  (NVL (flv.start_date_active,
                                                        SYSDATE
                                                       )
                                                  )
                                          AND TRUNC (NVL (flv.end_date_active,
                                                          SYSDATE
                                                         )
                                                    )
                  AND flv.LANGUAGE = USERENV ('LANG');
            EXCEPTION
               WHEN NO_DATA_FOUND
               THEN
                  FOR r_tax_ref_err_rec IN
                     (SELECT interface_txn_id
                        FROM xxconv.xxar_br_invoices_stg xis
                       WHERE leg_tax_code = tax_rec.leg_tax_code
                         AND batch_id = g_new_batch_id
             AND org_id = tax_rec.org_id --v1.3
             AND leg_line_type ='TAX'  --v1.3
                         AND run_sequence_id = g_new_run_seq_id
             )
                  LOOP
                     l_err_code := 'ETN_BR_TAX_ERROR';
                     g_retcode := 1;
                     l_err_msg :=
                           'Error : Cross reference not defined for tax code';
                     log_errors
                        (pin_transaction_id           => r_tax_ref_err_rec.interface_txn_id,
                         piv_source_column_name       => 'Legacy Tax code',
                         piv_source_column_value      => tax_rec.leg_tax_code,
                         piv_error_type               => 'ERR_VAL',
                         piv_error_code               => l_err_code,
                         piv_error_message            => l_err_msg,
                         pov_return_status            => l_log_ret_status,
                         pov_error_msg                => l_log_err_msg
                        );
                  END LOOP;

                  UPDATE xxconv.xxar_br_invoices_stg
                     SET process_flag = 'E',
                         ERROR_TYPE = 'ERR_VAL',
                         last_update_date = SYSDATE,
                         last_updated_by = g_last_updated_by,
                         last_update_login = g_login_id
                   WHERE leg_tax_code = tax_rec.leg_tax_code
                     AND batch_id = g_new_batch_id
           AND org_id = tax_rec.org_id --v1.3
           AND leg_line_type ='TAX' --v1.3
                     AND run_sequence_id = g_new_run_seq_id;

                  COMMIT;
               WHEN OTHERS
               THEN
                  l_err_code := 'ETN_BR_PROCEDURE_EXCEPTION';
                  g_retcode := 2;
                  l_err_msg :=
                        'Error : Error validating tax '
                     || SUBSTR (SQLERRM, 1, 150);

                  FOR r_tax_ref_err1_rec IN
                     (SELECT interface_txn_id
                        FROM xxconv.xxar_br_invoices_stg xis
                       WHERE leg_tax_code = tax_rec.leg_tax_code
                         AND batch_id = g_new_batch_id
             AND org_id = tax_rec.org_id --v1.3
             AND leg_line_type ='TAX' --v1.3
                         AND run_sequence_id = g_new_run_seq_id)
                  LOOP
                     log_errors
                        (pin_transaction_id           => r_tax_ref_err1_rec.interface_txn_id,
                         piv_source_column_name       => 'Legacy Tax code',
                         piv_source_column_value      => tax_rec.leg_tax_code,
                         piv_error_type               => 'ERR_VAL',
                         piv_error_code               => l_err_code,
                         piv_error_message            => l_err_msg,
                         pov_return_status            => l_log_ret_status,
                         pov_error_msg                => l_log_err_msg
                        );
                  END LOOP;

                  UPDATE xxconv.xxar_br_invoices_stg
                     SET process_flag = 'E',
                         ERROR_TYPE = 'ERR_VAL',
                         last_update_date = SYSDATE,
                         last_updated_by = g_last_updated_by,
                         last_update_login = g_login_id
                   WHERE leg_tax_code = tax_rec.leg_tax_code
                     AND batch_id = g_new_batch_id
           AND org_id = tax_rec.org_id --v1.3
           AND leg_line_type ='TAX' --v1.3
                     AND run_sequence_id = g_new_run_seq_id;
            END;
         END IF;

         BEGIN
            IF l_tax_code_r12 IS NOT NULL AND tax_rec.org_id IS NOT NULL
            THEN
               /*print_log_message ('Validating R12 tax code ' || l_tax_code_r12
                                 );*/
               SELECT DISTINCT zrb.tax, zrb.tax_regime_code,
                               zrb.tax_rate_code, zrb.tax,
                               zrb.tax_status_code, zrb.tax_jurisdiction_code
                          INTO l_tax_r12, l_tax_regime_code,
                               l_tax_rate_code, l_tax,
                               l_tax_status_code, l_tax_jurisdiction_code
                          FROM zx_accounts za,
                               hr_operating_units hrou,
                               gl_ledgers gl,
                               fnd_id_flex_structures fifs,
                               zx_rates_b zrb
                              -- zx_regimes_b zb v1.3 commented as not used anywhere
                         WHERE za.internal_organization_id =
                                                          hrou.organization_id
                           AND gl.ledger_id = za.ledger_id
                           AND fifs.application_id =
                                  (SELECT fap.application_id
                                     FROM fnd_application_vl fap
                                    WHERE fap.application_short_name = 'SQLGL')
                           AND fifs.id_flex_code = 'GL#'
                           AND fifs.id_flex_num = gl.chart_of_accounts_id
                           AND zrb.tax_rate_id = za.tax_account_entity_id
                           AND za.tax_account_entity_code = 'RATES'
                           AND zrb.tax_rate_code = l_tax_code_r12
                           AND hrou.organization_id = tax_rec.org_id
                          /* AND TRUNC (SYSDATE) --v1.12  Modified as this was returning multiple rows
                                  BETWEEN TRUNC (NVL (zb.effective_from,
                                                      SYSDATE
                                                     )
                                                )
                                      AND TRUNC (NVL (zb.effective_to,
                                                      SYSDATE)
                                                );*/
               AND TRUNC(SYSDATE) --v1.3 adding
                BETWEEN TRUNC(NVL(zrb.effective_from,
                          SYSDATE
                          )
                        )
                    AND TRUNC(NVL(zrb.effective_to,
                          SYSDATE
                          )
                        )
            AND NVL(zrb.active_flag, 'N') = 'Y';--v1.3 adding

            END IF;

            UPDATE xxconv.xxar_br_invoices_stg
               SET tax_code = l_tax_r12,
                   tax_regime_code = l_tax_regime_code,
                   tax_rate_code = l_tax_rate_code,
                   tax = l_tax,
                   tax_status_code = l_tax_status_code,
                   tax_jurisdiction_code = l_tax_jurisdiction_code
             WHERE leg_tax_code = tax_rec.leg_tax_code
               AND batch_id = g_new_batch_id
         AND org_id = tax_rec.org_id  --v1.3
         AND leg_line_type ='TAX'  --v1.3
               AND run_sequence_id = g_new_run_seq_id;

            COMMIT;
         EXCEPTION
            WHEN NO_DATA_FOUND
            THEN
               FOR r_r12_tax_err_rec IN
                  (SELECT interface_txn_id
                     FROM xxconv.xxar_br_invoices_stg xis
                    WHERE leg_tax_code = tax_rec.leg_tax_code
                      AND batch_id = g_new_batch_id
            AND org_id = tax_rec.org_id --v1.3
            AND leg_line_type ='TAX' --v1.3
                      AND run_sequence_id = g_new_run_seq_id)
               LOOP
                  l_err_code := 'ETN_BR_TAX_ERROR';
                  l_err_msg := 'Error : R12 set up not done for tax code';
                  g_retcode := 1;
                  log_errors
                     (pin_transaction_id           => r_r12_tax_err_rec.interface_txn_id,
                      piv_source_column_name       => 'R12 tax code',
                      piv_source_column_value      => l_tax_code_r12,
                      piv_error_type               => 'ERR_VAL',
                      piv_error_code               => l_err_code,
                      piv_error_message            => l_err_msg,
                      pov_return_status            => l_log_ret_status,
                      pov_error_msg                => l_log_err_msg
                     );
               END LOOP;

               UPDATE xxconv.xxar_br_invoices_stg
                  SET process_flag = 'E',
                      ERROR_TYPE = 'ERR_VAL',
                      last_update_date = SYSDATE,
                      last_updated_by = g_last_updated_by,
                      last_update_login = g_login_id
                WHERE leg_tax_code = tax_rec.leg_tax_code
                  AND batch_id = g_new_batch_id
          AND org_id = tax_rec.org_id --v1.3
          AND leg_line_type ='TAX' --v1.3
                  AND run_sequence_id = g_new_run_seq_id;
            WHEN OTHERS
            THEN
               l_err_code := 'ETN_BR_PROCEDURE_EXCEPTION';
               l_err_msg :=
                  'Error : Error validating tax ' || SUBSTR (SQLERRM, 1, 150);
               g_retcode := 2;

               FOR r_r12_tax_err1_rec IN
                  (SELECT interface_txn_id
                     FROM xxconv.xxar_br_invoices_stg xis
                    WHERE leg_tax_code = tax_rec.leg_tax_code
                      AND batch_id = g_new_batch_id
            AND org_id = tax_rec.org_id --v1.3
            AND leg_line_type ='TAX'--v1.3
                      AND run_sequence_id = g_new_run_seq_id)
               LOOP
                  log_errors
                     (pin_transaction_id           => r_r12_tax_err1_rec.interface_txn_id,
                      piv_source_column_name       => 'R12 tax code',
                      piv_source_column_value      => l_tax_code_r12,
                      piv_error_type               => 'ERR_VAL',
                      piv_error_code               => l_err_code,
                      piv_error_message            => l_err_msg,
                      pov_return_status            => l_log_ret_status,
                      pov_error_msg                => l_log_err_msg
                     );
               END LOOP;

               UPDATE xxconv.xxar_br_invoices_stg
                  SET process_flag = 'E',
                      ERROR_TYPE = 'ERR_VAL',
                      last_update_date = SYSDATE,
                      last_updated_by = g_last_updated_by,
                      last_update_login = g_login_id
                WHERE leg_tax_code = tax_rec.leg_tax_code
                  AND batch_id = g_new_batch_id
          AND org_id = tax_rec.org_id --v1.3
          AND leg_line_type ='TAX' --v1.3
                  AND run_sequence_id = g_new_run_seq_id;
         END;
      END LOOP;

      COMMIT;

      UPDATE xxconv.xxar_br_invoices_stg
         SET trx_number = leg_trx_number,
             process_flag = DECODE (process_flag, 'E', 'E', 'N'),
             gl_date =
                DECODE (INSTR (leg_cust_trx_type_name, 'OKS'),
                        0, g_gl_date,
                        leg_gl_date
                       ),
             --Ver1.5 Changes for DFF rationalization end
             trx_date = leg_trx_date
                                    --,batch_source_name = 'CONVERSION'
      ,
             purchase_order = leg_purchase_order,
             last_update_date = SYSDATE,
             last_updated_by = g_last_updated_by,
             last_update_login = g_login_id
       WHERE batch_id = g_new_batch_id AND run_sequence_id = g_new_run_seq_id;

      COMMIT;

      OPEN val_inv_det_cur;

      LOOP
         FETCH val_inv_det_cur
         BULK COLLECT INTO val_inv_det_rec LIMIT l_line_limit;

         EXIT WHEN val_inv_det_rec.COUNT = 0;

         FOR l_line_cnt IN 1 .. val_inv_det_rec.COUNT
         LOOP
            l_valid_flag := 'Y';
            l_customer_id :=
                         val_inv_det_rec (l_line_cnt).system_bill_customer_id;
            --l_trx_type_id := val_inv_det_rec(l_line_cnt).transaction_type_id;
            l_org_id := val_inv_det_rec (l_line_cnt).org_id;
            l_line_number := val_inv_det_rec (l_line_cnt).leg_line_number;
            --          g_int_line_att := xxar_intattribute_s.NEXTVAL;
            l_ledger_id := NULL;
            -- Initialize Loop Variables
            l_err_code := NULL;
            l_err_msg := NULL;
            l_upd_ret_status := NULL;
            l_validate_line_flag := NULL;
            l_inv_flag := NULL;
            l_cm_status_flag := NULL;
            l_leg_int_line_attribute1 := NULL;
            l_leg_int_line_attribute2 := NULL;
            l_leg_int_line_attribute3 := NULL;
            l_leg_int_line_attribute4 := NULL;
            l_leg_int_line_attribute5 := NULL;
            l_leg_int_line_attribute6 := NULL;
            l_leg_int_line_attribute7 := NULL;
            l_leg_int_line_attribute8 := NULL;
            l_leg_int_line_attribute9 := NULL;
            l_leg_int_line_attribute10 := NULL;
            l_leg_int_line_attribute11 := NULL;
            l_leg_int_line_attribute12 := NULL;
            l_leg_int_line_attribute13 := NULL;
            l_leg_int_line_attribute14 := NULL;
            l_leg_int_line_attribute15 := NULL;
            l_leg_line_amount := NULL;

            --Ver1.5 start
            IF l_trx_number <> val_inv_det_rec (l_line_cnt).trx_number
            THEN
               l_header_attr4 := NULL;
               l_header_attr8 := NULL;

               --        l_header_attr1 :=
               IF val_inv_det_rec (l_line_cnt).leg_interface_line_context =
                                                   'PLANT SHIPMENTS (EUROPE)'
               THEN
                  l_header_attr8 :=
                     val_inv_det_rec (l_line_cnt).leg_interface_line_attribute15;
               ELSIF val_inv_det_rec (l_line_cnt).leg_interface_line_context IN
                                                         ('328697', '328698')
               THEN
                  l_header_attr4 :=
                     val_inv_det_rec (l_line_cnt).leg_interface_line_attribute14;
               END IF;

               l_trx_number := val_inv_det_rec (l_line_cnt).trx_number;
            END IF;

            --Ver1.5 end

            -- If currency on transaction and functional currency do not match the conversion rate is required
            IF     NVL (val_inv_det_rec (l_line_cnt).leg_currency_code, 'A') <>
                             NVL (val_inv_det_rec (l_line_cnt).func_curr, 'A')
               --1.4 Added by Rohit D for FOT
               AND val_inv_det_rec (l_line_cnt).leg_currency_code IS NOT NULL
               AND val_inv_det_rec (l_line_cnt).func_curr IS NOT NULL
            THEN
               IF check_mandatory
                     (pin_trx_id            => val_inv_det_rec (l_line_cnt).interface_txn_id,
                      piv_column_value      => val_inv_det_rec (l_line_cnt).leg_conversion_date,
                      piv_column_name       => 'Conversion date'
                     )
               THEN
                  l_valid_flag := 'E';
               END IF;

               IF check_mandatory
                     (pin_trx_id            => val_inv_det_rec (l_line_cnt).interface_txn_id,
                      piv_column_value      => val_inv_det_rec (l_line_cnt).leg_conversion_rate,
                      piv_column_name       => 'Conversion rate'
                     )
               THEN
                  l_valid_flag := 'E';
               END IF;
            ELSE
               IF val_inv_det_rec (l_line_cnt).leg_conversion_date IS NOT NULL
               THEN
                  l_valid_flag := 'E';
                  l_err_code := 'ETN_BR_CONVERSION_DATE_ERROR';
                  g_retcode := 1;
                  l_err_msg :=
                     'Conversion date must be null since legacy currency is same as R12 currency';
                  log_errors
                     (pin_transaction_id           => val_inv_det_rec
                                                                   (l_line_cnt).interface_txn_id,
                      piv_source_column_name       => 'Legacy currency code',
                      piv_source_column_value      => val_inv_det_rec
                                                                   (l_line_cnt).leg_currency_code,
                      piv_source_keyname1          => 'R12 currency code',
                      piv_source_keyvalue1         => val_inv_det_rec
                                                                   (l_line_cnt).currency_code,
                      piv_error_type               => 'ERR_VAL',
                      piv_error_code               => l_err_code,
                      piv_error_message            => l_err_msg,
                      pov_return_status            => l_log_ret_status,
                      pov_error_msg                => l_log_err_msg
                     );
               END IF;

               IF NVL (val_inv_det_rec (l_line_cnt).leg_conversion_rate, 1) <>
                                                                             1
               THEN
                  l_valid_flag := 'E';
                  l_err_code := 'ETN_BR_CONVERSION_RATE_ERROR';
                  g_retcode := 1;
                  l_err_msg :=
                     'Conversion Rate must be null since legacy currency is same as R12 currency';
                  log_errors
                     (pin_transaction_id           => val_inv_det_rec
                                                                   (l_line_cnt).interface_txn_id,
                      piv_source_column_name       => 'Legacy currency code',
                      piv_source_column_value      => val_inv_det_rec
                                                                   (l_line_cnt).leg_currency_code,
                      piv_source_keyname1          => 'R12 currency code',
                      piv_source_keyvalue1         => val_inv_det_rec
                                                                   (l_line_cnt).currency_code,
                      piv_error_type               => 'ERR_VAL',
                      piv_error_code               => l_err_code,
                      piv_error_message            => l_err_msg,
                      pov_return_status            => l_log_ret_status,
                      pov_error_msg                => l_log_err_msg
                     );
               END IF;
            END IF;

            /*  validate_amount (val_inv_det_rec (l_line_cnt).leg_line_amount,
                               val_inv_det_rec (l_line_cnt).trx_type,
                               val_inv_det_rec (l_line_cnt).interface_txn_id,
                               l_validate_line_flag
                              );

              IF l_validate_line_flag = 'E'
              THEN
                 l_valid_flag := 'E';
              END IF;*/
            g_invoice_details (g_line_idx).interface_line_context :=
                                                      g_interface_line_context;
            g_invoice_details (g_line_idx).header_attribute_category :=
                                                      g_interface_line_context;

            IF val_inv_det_rec (l_line_cnt).leg_interface_line_context =
                                                    'PLANT SHIPMENTS (EUROPE)'
            THEN
               IF val_inv_det_rec (l_line_cnt).leg_interface_line_attribute1 IS NOT NULL
               THEN
                  g_invoice_details (g_line_idx).interface_line_attribute1 :=
                     val_inv_det_rec (l_line_cnt).leg_interface_line_attribute1;
               ELSE
                  g_invoice_details (g_line_idx).interface_line_attribute1 :=
                                  val_inv_det_rec (l_line_cnt).leg_trx_number;
               END IF;

               g_invoice_details (g_line_idx).header_attribute_category :=
                                                      g_interface_line_context;
               g_invoice_details (g_line_idx).header_attribute8 :=
                                                                l_header_attr8;
            ELSIF val_inv_det_rec (l_line_cnt).leg_interface_line_context IN
                                                         ('328697', '328698')
            THEN
               IF val_inv_det_rec (l_line_cnt).leg_interface_line_attribute2 IS NOT NULL
               THEN
                  g_invoice_details (g_line_idx).interface_line_attribute1 :=
                     val_inv_det_rec (l_line_cnt).leg_interface_line_attribute2;
               ELSE
                  g_invoice_details (g_line_idx).interface_line_attribute1 :=
                                  val_inv_det_rec (l_line_cnt).leg_trx_number;
               END IF;

               g_invoice_details (g_line_idx).header_attribute_category :=
                                                      g_interface_line_context;
               g_invoice_details (g_line_idx).header_attribute4 :=
                                                                l_header_attr4;
            ELSE
               g_invoice_details (g_line_idx).interface_line_attribute1 :=
                                  val_inv_det_rec (l_line_cnt).leg_trx_number;
            END IF;

            g_invoice_details (g_line_idx).interface_line_attribute15 :=
                                                   xxar_intattribute_s.NEXTVAL;

            IF g_invoice_details (g_line_idx).leg_source_system = 'ISSC'
            THEN
               IF g_invoice_details (g_line_idx).leg_header_attribute14 IS NOT NULL
               THEN
                  IF g_invoice_details (g_line_idx).leg_header_attribute14 IN
                                                             ('A', 'D', 'S')
                  THEN
                     g_invoice_details (g_line_idx).header_attribute1 :=
                        g_invoice_details (g_line_idx).leg_header_attribute14;
                  ELSE
                     g_invoice_details (g_line_idx).header_attribute1 := NULL;
                  END IF;
               END IF;
            ELSE                                                        -- FSC
               IF g_invoice_details (g_line_idx).leg_header_attribute6 IS NOT NULL
               THEN
                  IF g_invoice_details (g_line_idx).leg_header_attribute6 IN
                                                                  ('A', 'D')
                  THEN
                     g_invoice_details (g_line_idx).header_attribute1 :=
                         g_invoice_details (g_line_idx).leg_header_attribute6;
                  END IF;
               ELSE
                  IF g_invoice_details (g_line_idx).leg_header_attribute14 =
                                                                          'F'
                  THEN
                     g_invoice_details (g_line_idx).header_attribute1 := 'S';
                  ELSE
                     g_invoice_details (g_line_idx).header_attribute1 := 'I';
                  END IF;
               END IF;
            END IF;

            --Ver 1.9 changes start
            l_line_dff_rec (val_inv_det_rec (l_line_cnt).leg_cust_trx_line_id).leg_line_amount :=
                                g_invoice_details (g_line_idx).leg_line_amount;
            l_line_dff_rec (val_inv_det_rec (l_line_cnt).leg_cust_trx_line_id).interface_line_attribute1 :=
                      g_invoice_details (g_line_idx).interface_line_attribute1;
            l_line_dff_rec (val_inv_det_rec (l_line_cnt).leg_cust_trx_line_id).interface_line_attribute15 :=
                     g_invoice_details (g_line_idx).interface_line_attribute15;
            --Ver 1.9 changes end
            --Ver1.5 Changes after DFF rationalization end
            g_invoice_details (g_line_idx).amount_includes_tax_flag := 'N';

            IF val_inv_det_rec (l_line_cnt).leg_line_type = 'TAX'
            THEN
               l_validate_line_flag := NULL;
               g_invoice_details (g_line_idx).amount_includes_tax_flag :=
                                                                         NULL;
            END IF;

            -- Line type must either be line or tax or freight
            IF     val_inv_det_rec (l_line_cnt).leg_line_type <> 'LINE'
               AND val_inv_det_rec (l_line_cnt).leg_line_type <> 'TAX'
               AND val_inv_det_rec (l_line_cnt).leg_line_type <> 'FREIGHT'
            THEN
               l_err_code := 'ETN_BR_LINE_TYPE_EXCEPTION';
               g_retcode := 1;
               l_err_msg :=
                  'Error : Invalid line type. Line type must either be LINE, TAX or FREIGHT ';
               l_valid_flag := 'E';
               log_errors
                  (pin_transaction_id           => val_inv_det_rec (l_line_cnt).interface_txn_id,
                   piv_source_column_name       => 'Legacy line type',
                   piv_source_column_value      => val_inv_det_rec (l_line_cnt).leg_line_type,
                   piv_error_type               => 'ERR_VAL',
                   piv_error_code               => l_err_code,
                   piv_error_message            => l_err_msg,
                   pov_return_status            => l_log_ret_status,
                   pov_error_msg                => l_log_err_msg
                  );
            END IF;

            -- If line type is Tax the tax code must be provided
            IF val_inv_det_rec (l_line_cnt).leg_line_type = 'TAX'
            --            AND val_inv_det_rec(l_line_cnt).tax_code IS NULL
            THEN
               IF check_mandatory
                     (pin_trx_id            => val_inv_det_rec (l_line_cnt).interface_txn_id,
                      piv_column_value      => val_inv_det_rec (l_line_cnt).leg_tax_code,
                      piv_column_name       => 'Tax Code'
                     )
               THEN
                  l_valid_flag := 'E';
               END IF;

               -- Set the link_to_line_attributes of the tax line equal to interface_line_attributes of the invoice line linked to it
               -- link_to_line_context will be hardcoded to Conversion
               IF val_inv_det_rec (l_line_cnt).leg_link_to_cust_trx_line_id IS NOT NULL
               THEN
                  BEGIN
                     l_leg_line_amount :=
                        l_line_dff_rec
                           (val_inv_det_rec (l_line_cnt).leg_link_to_cust_trx_line_id
                           ).leg_line_amount;
                     l_leg_int_line_attribute1 :=
                        l_line_dff_rec
                           (val_inv_det_rec (l_line_cnt).leg_link_to_cust_trx_line_id
                           ).interface_line_attribute1;
                     l_leg_int_line_attribute15 :=
                        l_line_dff_rec
                           (val_inv_det_rec (l_line_cnt).leg_link_to_cust_trx_line_id
                           ).interface_line_attribute15;
                  EXCEPTION
                     WHEN NO_DATA_FOUND
                     THEN
                        l_err_code := 'ETN_TAX_LINK_ERROR';
                        l_err_msg :=
                           'Error : Cannot find invoice line corresponding to tax line ';
                        l_valid_flag := 'E';
                        g_retcode := 1;
                        log_errors
                           (pin_transaction_id           => val_inv_det_rec
                                                                   (l_line_cnt).interface_txn_id,
                            piv_source_column_name       => 'Legacy link_to_customer_trx_line_id',
                            piv_source_column_value      => val_inv_det_rec
                                                                   (l_line_cnt).leg_link_to_cust_trx_line_id,
                            piv_error_type               => 'ERR_VAL',
                            piv_error_code               => l_err_code,
                            piv_error_message            => l_err_msg,
                            pov_return_status            => l_log_ret_status,
                            pov_error_msg                => l_log_err_msg
                           );
                     WHEN OTHERS
                     THEN
                        l_err_code := 'ETN_BR_PROCEDURE_EXCEPTION';
                        l_err_msg :=
                              'Error : Error linking tax with invoice line '
                           || SUBSTR (SQLERRM, 1, 150);
                        l_valid_flag := 'E';
                        g_retcode := 2;
                        log_errors
                           (pin_transaction_id           => val_inv_det_rec
                                                                   (l_line_cnt).interface_txn_id,
                            piv_source_column_name       => 'Legacy link_to_customer_trx_line_id',
                            piv_source_column_value      => val_inv_det_rec
                                                                   (l_line_cnt).leg_link_to_cust_trx_line_id,
                            piv_error_type               => 'ERR_VAL',
                            piv_error_code               => l_err_code,
                            piv_error_message            => l_err_msg,
                            pov_return_status            => l_log_ret_status,
                            pov_error_msg                => l_log_err_msg
                           );
                  END;

                  IF l_leg_line_amount = 0
                  THEN
                     g_invoice_details (g_line_idx).tax_rate := 1;
                  ELSE
                     g_invoice_details (g_line_idx).tax_rate :=
                          (val_inv_det_rec (l_line_cnt).leg_line_amount * 100
                          )
                        / l_leg_line_amount;
                  --- 1.4 Changed for FOT issue Rohit D
                  END IF;

                  g_invoice_details (g_line_idx).link_to_line_context :=
                                                      g_interface_line_context;
                  g_invoice_details (g_line_idx).link_to_line_attribute1 :=
                                                     l_leg_int_line_attribute1;
                  g_invoice_details (g_line_idx).link_to_line_attribute15 :=
                                                    l_leg_int_line_attribute15;
               END IF;
            END IF;

            -- Set the link_to_line_attributes of the tax line equal to interface_line_attributes of the invoice line linked to it
            -- link_to_line_context will be hardcoded to Conversion
            IF     val_inv_det_rec (l_line_cnt).trx_type = 'CM'
               AND val_inv_det_rec (l_line_cnt).leg_reference_line_id IS NOT NULL
            THEN
               BEGIN
                  SELECT 'Y'
                    INTO l_inv_flag
                    FROM xxconv.xxar_br_invoices_stg
                   WHERE leg_cust_trx_line_id =
                            val_inv_det_rec (l_line_cnt).leg_reference_line_id
                     AND org_id = val_inv_det_rec (l_line_cnt).org_id
                     AND ROWNUM = 1;
               EXCEPTION
                  WHEN OTHERS
                  THEN
                     l_inv_flag := 'N';
               END;

               BEGIN
                  SELECT 'S'
                    INTO l_cm_status_flag
                    FROM ra_customer_trx_all rct,
                         xxconv.xxar_br_invoices_stg xis,
                         ra_cust_trx_types_all rctt
                   WHERE rct.org_id = xis.org_id
                     AND rct.trx_number = xis.leg_trx_number
                     AND rct.org_id = rctt.org_id
                     AND rct.cust_trx_type_id = rctt.cust_trx_type_id
                     AND xis.trx_type = rctt.TYPE
                     AND xis.org_id = val_inv_det_rec (l_line_cnt).org_id
                     AND xis.leg_cust_trx_line_id =
                            val_inv_det_rec (l_line_cnt).leg_reference_line_id
                     AND ROWNUM = 1;
               EXCEPTION
                  WHEN OTHERS
                  THEN
                     l_cm_status_flag := 'E';
               --Error: yet to be converted
               END;

               IF l_inv_flag = 'Y' AND l_cm_status_flag = 'E'
               THEN
                  NULL;
                  l_valid_flag := 'E';
               ELSIF l_cm_status_flag = 'S'
               THEN
                  g_invoice_details (g_line_idx).reference_line_id :=
                           val_inv_det_rec (l_line_cnt).leg_reference_line_id;
               ELSE
                  NULL;
                  l_valid_flag := 'E';
               END IF;
            END IF;

            IF val_inv_det_rec (l_line_cnt).leg_line_type = 'FREIGHT'
            THEN
               g_invoice_details (g_line_idx).line_type := 'LINE';

               BEGIN
                  SELECT memo_line_id,
                         description
                    INTO g_invoice_details (g_line_idx).memo_line_id,
                         g_invoice_details (g_line_idx).description
                    FROM ar_memo_lines_all_tl
                   WHERE LANGUAGE = USERENV ('LANG')
                     AND UPPER (NAME) LIKE '%CONVERSION%FREIGHT%'
                     AND org_id = val_inv_det_rec (l_line_cnt).org_id;
               EXCEPTION
                  WHEN NO_DATA_FOUND
                  THEN
                     l_err_code := 'ETN_MEMO_LINE_ERROR';
                     l_err_msg :=
                        'Error : Cannot find memo line to create freight lines ';
                     l_valid_flag := 'E';
                     g_retcode := 1;
                     log_errors
                        (pin_transaction_id           => val_inv_det_rec
                                                                   (l_line_cnt).interface_txn_id,
                         piv_source_column_name       => 'MEMO LINE NAME',
                         piv_source_column_value      => 'Conversion Freight',
                         piv_error_type               => 'ERR_VAL',
                         piv_error_code               => l_err_code,
                         piv_error_message            => l_err_msg,
                         pov_return_status            => l_log_ret_status,
                         pov_error_msg                => l_log_err_msg
                        );
                  WHEN OTHERS
                  THEN
                     l_err_code := 'ETN_AR_PROCEDURE_EXCEPTION';
                     l_err_msg :=
                           'Error : Error linking tax with invoice line '
                        || SUBSTR (SQLERRM, 1, 150);
                     l_valid_flag := 'E';
                     g_retcode := 2;
                     log_errors
                        (pin_transaction_id           => val_inv_det_rec
                                                                   (l_line_cnt).interface_txn_id,
                         piv_source_column_name       => 'Legacy link_to_customer_trx_line_id',
                         piv_source_column_value      => val_inv_det_rec
                                                                   (l_line_cnt).leg_link_to_cust_trx_line_id,
                         piv_error_type               => 'ERR_VAL',
                         piv_error_code               => l_err_code,
                         piv_error_message            => l_err_msg,
                         pov_return_status            => l_log_ret_status,
                         pov_error_msg                => l_log_err_msg
                        );
               END;
            ELSE
               g_invoice_details (g_line_idx).line_type :=
                                   val_inv_det_rec (l_line_cnt).leg_line_type;
               g_invoice_details (g_line_idx).description :=
                                                       'Converted Net Amount';
            END IF;

            --        IF NVL(l_valid_flag, 'Y') = 'Y' AND l_dist_flag = 'Y' THEN
            IF NVL (l_valid_flag, 'Y') = 'Y'
            THEN
               g_invoice_details (g_line_idx).process_flag := 'V';
            ELSE
               g_invoice_details (g_line_idx).process_flag := 'E';
               g_invoice_details (g_line_idx).ERROR_TYPE := 'ERR_VAL';
               g_retcode := 1;
            END IF;

            g_invoice_details (g_line_idx).interface_txn_id :=
                                 val_inv_det_rec (l_line_cnt).interface_txn_id;
            g_invoice_details (g_line_idx).line_number :=
                                      val_inv_det_rec (l_line_cnt).line_number;
            g_invoice_details (g_line_idx).conversion_type := 'User';
            g_invoice_details (g_line_idx).conversion_date :=
                              val_inv_det_rec (l_line_cnt).leg_conversion_date;
            g_invoice_details (g_line_idx).conversion_rate :=
                     NVL (val_inv_det_rec (l_line_cnt).leg_conversion_rate, 1);
            g_invoice_details (g_line_idx).line_amount :=
                                  val_inv_det_rec (l_line_cnt).leg_line_amount;
            g_invoice_details (g_line_idx).reason_code :=
                                  val_inv_det_rec (l_line_cnt).leg_reason_code;
            g_invoice_details (g_line_idx).taxable_flag := 'N';
            g_invoice_details (g_line_idx).comments :=
                                     val_inv_det_rec (l_line_cnt).leg_comments;
            g_invoice_details (g_line_idx).attribute_category :=
                               val_inv_det_rec (l_line_cnt).attribute_category;
            g_invoice_details (g_line_idx).attribute1 :=
                                       val_inv_det_rec (l_line_cnt).attribute1;
            g_invoice_details (g_line_idx).attribute2 :=
                                       val_inv_det_rec (l_line_cnt).attribute2;
            g_invoice_details (g_line_idx).attribute3 :=
                                       val_inv_det_rec (l_line_cnt).attribute3;
            g_invoice_details (g_line_idx).attribute4 :=
                                       val_inv_det_rec (l_line_cnt).attribute4;
            g_invoice_details (g_line_idx).attribute5 :=
                                       val_inv_det_rec (l_line_cnt).attribute5;
            g_invoice_details (g_line_idx).attribute6 :=
                                       val_inv_det_rec (l_line_cnt).attribute6;
            g_invoice_details (g_line_idx).attribute7 :=
                                       val_inv_det_rec (l_line_cnt).attribute7;
            g_invoice_details (g_line_idx).attribute8 :=
                                       val_inv_det_rec (l_line_cnt).attribute8;
            g_invoice_details (g_line_idx).attribute9 :=
                                       val_inv_det_rec (l_line_cnt).attribute9;
            g_invoice_details (g_line_idx).attribute10 :=
                                      val_inv_det_rec (l_line_cnt).attribute10;
            g_invoice_details (g_line_idx).attribute11 :=
                                      val_inv_det_rec (l_line_cnt).attribute11;
            g_invoice_details (g_line_idx).attribute12 :=
                                      val_inv_det_rec (l_line_cnt).attribute12;
            g_invoice_details (g_line_idx).attribute13 :=
                                      val_inv_det_rec (l_line_cnt).attribute13;
            g_invoice_details (g_line_idx).attribute14 :=
                                      val_inv_det_rec (l_line_cnt).attribute14;
            g_invoice_details (g_line_idx).attribute15 :=
                                      val_inv_det_rec (l_line_cnt).attribute15;
            g_invoice_details (g_line_idx).header_gdf_attr_category :=
                     val_inv_det_rec (l_line_cnt).leg_header_gdf_attr_category;
            g_invoice_details (g_line_idx).header_gdf_attribute1 :=
                        val_inv_det_rec (l_line_cnt).leg_header_gdf_attribute1;
            g_invoice_details (g_line_idx).header_gdf_attribute2 :=
                        val_inv_det_rec (l_line_cnt).leg_header_gdf_attribute2;
            g_invoice_details (g_line_idx).header_gdf_attribute3 :=
                        val_inv_det_rec (l_line_cnt).leg_header_gdf_attribute3;
            g_invoice_details (g_line_idx).header_gdf_attribute4 :=
                        val_inv_det_rec (l_line_cnt).leg_header_gdf_attribute4;
            g_invoice_details (g_line_idx).header_gdf_attribute5 :=
                        val_inv_det_rec (l_line_cnt).leg_header_gdf_attribute5;
            g_invoice_details (g_line_idx).header_gdf_attribute6 :=
                        val_inv_det_rec (l_line_cnt).leg_header_gdf_attribute6;
            g_invoice_details (g_line_idx).header_gdf_attribute7 :=
                        val_inv_det_rec (l_line_cnt).leg_header_gdf_attribute7;
            g_invoice_details (g_line_idx).header_gdf_attribute8 :=
                        val_inv_det_rec (l_line_cnt).leg_header_gdf_attribute8;
            g_invoice_details (g_line_idx).header_gdf_attribute9 :=
                        val_inv_det_rec (l_line_cnt).leg_header_gdf_attribute9;
            g_invoice_details (g_line_idx).header_gdf_attribute10 :=
                       val_inv_det_rec (l_line_cnt).leg_header_gdf_attribute10;
            g_invoice_details (g_line_idx).header_gdf_attribute11 :=
                       val_inv_det_rec (l_line_cnt).leg_header_gdf_attribute11;
            g_invoice_details (g_line_idx).header_gdf_attribute12 :=
                       val_inv_det_rec (l_line_cnt).leg_header_gdf_attribute12;
            g_invoice_details (g_line_idx).header_gdf_attribute13 :=
                       val_inv_det_rec (l_line_cnt).leg_header_gdf_attribute13;
            g_invoice_details (g_line_idx).header_gdf_attribute14 :=
                       val_inv_det_rec (l_line_cnt).leg_header_gdf_attribute14;
            g_invoice_details (g_line_idx).header_gdf_attribute15 :=
                       val_inv_det_rec (l_line_cnt).leg_header_gdf_attribute15;
            g_invoice_details (g_line_idx).header_gdf_attribute16 :=
                       val_inv_det_rec (l_line_cnt).leg_header_gdf_attribute16;
            g_invoice_details (g_line_idx).header_gdf_attribute17 :=
                       val_inv_det_rec (l_line_cnt).leg_header_gdf_attribute17;
            g_invoice_details (g_line_idx).header_gdf_attribute18 :=
                       val_inv_det_rec (l_line_cnt).leg_header_gdf_attribute18;
            g_invoice_details (g_line_idx).header_gdf_attribute19 :=
                       val_inv_det_rec (l_line_cnt).leg_header_gdf_attribute19;
            g_invoice_details (g_line_idx).header_gdf_attribute20 :=
                       val_inv_det_rec (l_line_cnt).leg_header_gdf_attribute20;
            g_invoice_details (g_line_idx).header_gdf_attribute21 :=
                       val_inv_det_rec (l_line_cnt).leg_header_gdf_attribute21;
            g_invoice_details (g_line_idx).header_gdf_attribute22 :=
                       val_inv_det_rec (l_line_cnt).leg_header_gdf_attribute22;
            g_invoice_details (g_line_idx).header_gdf_attribute23 :=
                       val_inv_det_rec (l_line_cnt).leg_header_gdf_attribute23;
            g_invoice_details (g_line_idx).header_gdf_attribute24 :=
                       val_inv_det_rec (l_line_cnt).leg_header_gdf_attribute24;
            g_invoice_details (g_line_idx).header_gdf_attribute25 :=
                       val_inv_det_rec (l_line_cnt).leg_header_gdf_attribute25;
            g_invoice_details (g_line_idx).header_gdf_attribute26 :=
                       val_inv_det_rec (l_line_cnt).leg_header_gdf_attribute26;
            g_invoice_details (g_line_idx).header_gdf_attribute27 :=
                       val_inv_det_rec (l_line_cnt).leg_header_gdf_attribute27;
            g_invoice_details (g_line_idx).header_gdf_attribute28 :=
                       val_inv_det_rec (l_line_cnt).leg_header_gdf_attribute28;
            g_invoice_details (g_line_idx).header_gdf_attribute29 :=
                       val_inv_det_rec (l_line_cnt).leg_header_gdf_attribute29;
            g_invoice_details (g_line_idx).header_gdf_attribute30 :=
                       val_inv_det_rec (l_line_cnt).leg_header_gdf_attribute30;
            g_invoice_details (g_line_idx).line_gdf_attr_category :=
                       val_inv_det_rec (l_line_cnt).leg_line_gdf_attr_category;
            g_invoice_details (g_line_idx).line_gdf_attribute1 :=
                          val_inv_det_rec (l_line_cnt).leg_line_gdf_attribute1;
            g_invoice_details (g_line_idx).line_gdf_attribute2 :=
                          val_inv_det_rec (l_line_cnt).leg_line_gdf_attribute2;
            g_invoice_details (g_line_idx).line_gdf_attribute3 :=
                          val_inv_det_rec (l_line_cnt).leg_line_gdf_attribute3;
            g_invoice_details (g_line_idx).line_gdf_attribute4 :=
                          val_inv_det_rec (l_line_cnt).leg_line_gdf_attribute4;
            g_invoice_details (g_line_idx).line_gdf_attribute5 :=
                          val_inv_det_rec (l_line_cnt).leg_line_gdf_attribute5;
            g_invoice_details (g_line_idx).line_gdf_attribute6 :=
                          val_inv_det_rec (l_line_cnt).leg_line_gdf_attribute6;
            g_invoice_details (g_line_idx).line_gdf_attribute7 :=
                          val_inv_det_rec (l_line_cnt).leg_line_gdf_attribute7;
            g_invoice_details (g_line_idx).line_gdf_attribute8 :=
                          val_inv_det_rec (l_line_cnt).leg_line_gdf_attribute8;
            g_invoice_details (g_line_idx).line_gdf_attribute9 :=
                          val_inv_det_rec (l_line_cnt).leg_line_gdf_attribute9;
            g_invoice_details (g_line_idx).line_gdf_attribute10 :=
                         val_inv_det_rec (l_line_cnt).leg_line_gdf_attribute10;
            g_invoice_details (g_line_idx).line_gdf_attribute11 :=
                         val_inv_det_rec (l_line_cnt).leg_line_gdf_attribute11;
            g_invoice_details (g_line_idx).line_gdf_attribute12 :=
                         val_inv_det_rec (l_line_cnt).leg_line_gdf_attribute12;
            g_invoice_details (g_line_idx).line_gdf_attribute13 :=
                         val_inv_det_rec (l_line_cnt).leg_line_gdf_attribute13;
            g_invoice_details (g_line_idx).line_gdf_attribute14 :=
                         val_inv_det_rec (l_line_cnt).leg_line_gdf_attribute14;
            g_invoice_details (g_line_idx).line_gdf_attribute15 :=
                         val_inv_det_rec (l_line_cnt).leg_line_gdf_attribute15;
            g_invoice_details (g_line_idx).line_gdf_attribute16 :=
                         val_inv_det_rec (l_line_cnt).leg_line_gdf_attribute16;
            g_invoice_details (g_line_idx).line_gdf_attribute17 :=
                         val_inv_det_rec (l_line_cnt).leg_line_gdf_attribute17;
            g_invoice_details (g_line_idx).line_gdf_attribute18 :=
                         val_inv_det_rec (l_line_cnt).leg_line_gdf_attribute18;
            g_invoice_details (g_line_idx).line_gdf_attribute19 :=
                         val_inv_det_rec (l_line_cnt).leg_line_gdf_attribute19;
            g_invoice_details (g_line_idx).line_gdf_attribute20 :=
                         val_inv_det_rec (l_line_cnt).leg_line_gdf_attribute20;
            g_invoice_details (g_line_idx).sales_order_date :=
                             val_inv_det_rec (l_line_cnt).leg_sales_order_date;
            g_invoice_details (g_line_idx).sales_order :=
                                  val_inv_det_rec (l_line_cnt).leg_sales_order;
            g_invoice_details (g_line_idx).uom_name :=
                                     val_inv_det_rec (l_line_cnt).leg_uom_name;
            g_invoice_details (g_line_idx).ussgl_transaction_code_context :=
                       val_inv_det_rec (l_line_cnt).leg_ussgl_trx_code_context;
            g_invoice_details (g_line_idx).internal_notes :=
                               val_inv_det_rec (l_line_cnt).leg_internal_notes;
            g_invoice_details (g_line_idx).ship_date_actual :=
                             val_inv_det_rec (l_line_cnt).leg_ship_date_actual;
            g_invoice_details (g_line_idx).fob_point :=
                                    val_inv_det_rec (l_line_cnt).leg_fob_point;
            g_invoice_details (g_line_idx).ship_via :=
                                     val_inv_det_rec (l_line_cnt).leg_ship_via;
            g_invoice_details (g_line_idx).waybill_number :=
                               val_inv_det_rec (l_line_cnt).leg_waybill_number;
            g_invoice_details (g_line_idx).sales_order_line :=
                             val_inv_det_rec (l_line_cnt).leg_sales_order_line;
            g_invoice_details (g_line_idx).sales_order_source :=
                           val_inv_det_rec (l_line_cnt).leg_sales_order_source;
            g_invoice_details (g_line_idx).sales_order_revision :=
                         val_inv_det_rec (l_line_cnt).leg_sales_order_revision;
            g_invoice_details (g_line_idx).purchase_order :=
                               val_inv_det_rec (l_line_cnt).leg_purchase_order;
            g_invoice_details (g_line_idx).purchase_order_revision :=
                      val_inv_det_rec (l_line_cnt).leg_purchase_order_revision;
            g_invoice_details (g_line_idx).purchase_order_date :=
                          val_inv_det_rec (l_line_cnt).leg_purchase_order_date;
            g_invoice_details (g_line_idx).quantity :=
                                     val_inv_det_rec (l_line_cnt).leg_quantity;
            g_invoice_details (g_line_idx).quantity_ordered :=
                             val_inv_det_rec (l_line_cnt).leg_quantity_ordered;
            g_invoice_details (g_line_idx).unit_selling_price :=
                           val_inv_det_rec (l_line_cnt).leg_unit_selling_price;
            g_invoice_details (g_line_idx).unit_standard_price :=
                          val_inv_det_rec (l_line_cnt).leg_unit_standard_price;
            g_line_idx := g_line_idx + 1;
         END LOOP;
      END LOOP;

      CLOSE val_inv_det_cur;

      IF g_invoice_details.EXISTS (1)
      THEN
         FORALL l_indx IN 1 .. g_invoice_details.COUNT
            -- LOOP
            UPDATE xxconv.xxar_br_invoices_stg
               SET line_number = g_invoice_details (l_indx).line_number,
                   line_type = g_invoice_details (l_indx).line_type,
                   description = g_invoice_details (l_indx).description,
                   conversion_type =
                                    g_invoice_details (l_indx).conversion_type,
                   conversion_date =
                                    g_invoice_details (l_indx).conversion_date,
                   conversion_rate =
                                    g_invoice_details (l_indx).conversion_rate,
                   line_amount = g_invoice_details (l_indx).line_amount,
                   --                   process_flag = g_invoice_details (l_indx).process_flag,
                   process_flag =
                      DECODE (process_flag,
                              'E', 'E',
                              g_invoice_details (l_indx).process_flag
                             ),
                   ERROR_TYPE = g_invoice_details (l_indx).ERROR_TYPE,
                   reason_code = g_invoice_details (l_indx).reason_code,
                   comments = g_invoice_details (l_indx).comments,
                   reference_line_id =
                                  g_invoice_details (l_indx).reference_line_id,
                   memo_line_id = g_invoice_details (l_indx).memo_line_id,
                   attribute_category =
                                 g_invoice_details (l_indx).attribute_category,
                   attribute1 = g_invoice_details (l_indx).attribute1,
                   attribute2 = g_invoice_details (l_indx).attribute2,
                   attribute3 = g_invoice_details (l_indx).attribute3,
                   attribute4 = g_invoice_details (l_indx).attribute4,
                   attribute5 = g_invoice_details (l_indx).attribute5,
                   attribute6 = g_invoice_details (l_indx).attribute6,
                   attribute7 = g_invoice_details (l_indx).attribute7,
                   attribute8 = g_invoice_details (l_indx).attribute8,
                   attribute9 = g_invoice_details (l_indx).attribute9,
                   attribute10 = g_invoice_details (l_indx).attribute10,
                   attribute11 = g_invoice_details (l_indx).attribute11,
                   attribute12 = g_invoice_details (l_indx).attribute12,
                   attribute13 = g_invoice_details (l_indx).attribute13,
                   attribute14 = g_invoice_details (l_indx).attribute14,
                   attribute15 = g_invoice_details (l_indx).attribute15,
                   interface_line_context =
                             g_invoice_details (l_indx).interface_line_context,
                   interface_line_attribute1 =
                          g_invoice_details (l_indx).interface_line_attribute1,
                   interface_line_attribute15 =
                         g_invoice_details (l_indx).interface_line_attribute15,
                   link_to_line_context =
                               g_invoice_details (l_indx).link_to_line_context,
                   link_to_line_attribute1 =
                            g_invoice_details (l_indx).link_to_line_attribute1,
                   header_attribute_category =
                          g_invoice_details (l_indx).header_attribute_category,
                   header_attribute1 =
                                  g_invoice_details (l_indx).header_attribute1,
                   header_attribute8 =
                                  g_invoice_details (l_indx).header_attribute8,
                   header_attribute4 =
                                  g_invoice_details (l_indx).header_attribute4,
                   link_to_line_attribute15 =
                           g_invoice_details (l_indx).link_to_line_attribute15,
                   header_gdf_attr_category =
                           g_invoice_details (l_indx).header_gdf_attr_category,
                   header_gdf_attribute1 =
                              g_invoice_details (l_indx).header_gdf_attribute1,
                   header_gdf_attribute2 =
                              g_invoice_details (l_indx).header_gdf_attribute2,
                   header_gdf_attribute3 =
                              g_invoice_details (l_indx).header_gdf_attribute3,
                   header_gdf_attribute4 =
                              g_invoice_details (l_indx).header_gdf_attribute4,
                   header_gdf_attribute5 =
                              g_invoice_details (l_indx).header_gdf_attribute5,
                   header_gdf_attribute6 =
                              g_invoice_details (l_indx).header_gdf_attribute6,
                   header_gdf_attribute7 =
                              g_invoice_details (l_indx).header_gdf_attribute7,
                   header_gdf_attribute8 =
                              g_invoice_details (l_indx).header_gdf_attribute8,
                   header_gdf_attribute9 =
                              g_invoice_details (l_indx).header_gdf_attribute9,
                   header_gdf_attribute10 =
                             g_invoice_details (l_indx).header_gdf_attribute10,
                   header_gdf_attribute11 =
                             g_invoice_details (l_indx).header_gdf_attribute11,
                   header_gdf_attribute12 =
                             g_invoice_details (l_indx).header_gdf_attribute12,
                   header_gdf_attribute13 =
                             g_invoice_details (l_indx).header_gdf_attribute13,
                   header_gdf_attribute14 =
                             g_invoice_details (l_indx).header_gdf_attribute14,
                   header_gdf_attribute15 =
                             g_invoice_details (l_indx).header_gdf_attribute15,
                   header_gdf_attribute16 =
                             g_invoice_details (l_indx).header_gdf_attribute16,
                   header_gdf_attribute17 =
                             g_invoice_details (l_indx).header_gdf_attribute17,
                   header_gdf_attribute18 =
                             g_invoice_details (l_indx).header_gdf_attribute18,
                   header_gdf_attribute19 =
                             g_invoice_details (l_indx).header_gdf_attribute19,
                   header_gdf_attribute20 =
                             g_invoice_details (l_indx).header_gdf_attribute20,
                   header_gdf_attribute21 =
                             g_invoice_details (l_indx).header_gdf_attribute21,
                   header_gdf_attribute22 =
                             g_invoice_details (l_indx).header_gdf_attribute22,
                   header_gdf_attribute23 =
                             g_invoice_details (l_indx).header_gdf_attribute23,
                   header_gdf_attribute24 =
                             g_invoice_details (l_indx).header_gdf_attribute24,
                   header_gdf_attribute25 =
                             g_invoice_details (l_indx).header_gdf_attribute25,
                   header_gdf_attribute26 =
                             g_invoice_details (l_indx).header_gdf_attribute26,
                   header_gdf_attribute27 =
                             g_invoice_details (l_indx).header_gdf_attribute27,
                   header_gdf_attribute28 =
                             g_invoice_details (l_indx).header_gdf_attribute28,
                   header_gdf_attribute29 =
                             g_invoice_details (l_indx).header_gdf_attribute29,
                   header_gdf_attribute30 =
                             g_invoice_details (l_indx).header_gdf_attribute30,
                   line_gdf_attr_category =
                             g_invoice_details (l_indx).line_gdf_attr_category,
                   line_gdf_attribute1 =
                                g_invoice_details (l_indx).line_gdf_attribute1,
                   line_gdf_attribute2 =
                                g_invoice_details (l_indx).line_gdf_attribute2,
                   line_gdf_attribute3 =
                                g_invoice_details (l_indx).line_gdf_attribute3,
                   line_gdf_attribute4 =
                                g_invoice_details (l_indx).line_gdf_attribute4,
                   line_gdf_attribute5 =
                                g_invoice_details (l_indx).line_gdf_attribute5,
                   line_gdf_attribute6 =
                                g_invoice_details (l_indx).line_gdf_attribute6,
                   line_gdf_attribute7 =
                                g_invoice_details (l_indx).line_gdf_attribute7,
                   line_gdf_attribute8 =
                                g_invoice_details (l_indx).line_gdf_attribute8,
                   line_gdf_attribute9 =
                                g_invoice_details (l_indx).line_gdf_attribute9,
                   line_gdf_attribute10 =
                               g_invoice_details (l_indx).line_gdf_attribute10,
                   line_gdf_attribute11 =
                               g_invoice_details (l_indx).line_gdf_attribute11,
                   line_gdf_attribute12 =
                               g_invoice_details (l_indx).line_gdf_attribute12,
                   line_gdf_attribute13 =
                               g_invoice_details (l_indx).line_gdf_attribute13,
                   line_gdf_attribute14 =
                               g_invoice_details (l_indx).line_gdf_attribute14,
                   line_gdf_attribute15 =
                               g_invoice_details (l_indx).line_gdf_attribute15,
                   line_gdf_attribute16 =
                               g_invoice_details (l_indx).line_gdf_attribute16,
                   line_gdf_attribute17 =
                               g_invoice_details (l_indx).line_gdf_attribute17,
                   line_gdf_attribute18 =
                               g_invoice_details (l_indx).line_gdf_attribute18,
                   line_gdf_attribute19 =
                               g_invoice_details (l_indx).line_gdf_attribute19,
                   line_gdf_attribute20 =
                               g_invoice_details (l_indx).line_gdf_attribute20,
                   tax_rate = g_invoice_details (l_indx).tax_rate,
                   amount_includes_tax_flag =
                           g_invoice_details (l_indx).amount_includes_tax_flag,
                   taxable_flag = g_invoice_details (l_indx).taxable_flag,
                   sales_order_date =
                                   g_invoice_details (l_indx).sales_order_date,
                   sales_order = g_invoice_details (l_indx).sales_order,
                   uom_name = g_invoice_details (l_indx).uom_name,
                   ussgl_transaction_code_context =
                      g_invoice_details (l_indx).ussgl_transaction_code_context,
                   internal_notes = g_invoice_details (l_indx).internal_notes,
                   ship_date_actual =
                                   g_invoice_details (l_indx).ship_date_actual,
                   fob_point = g_invoice_details (l_indx).fob_point,
                   ship_via = g_invoice_details (l_indx).ship_via,
                   waybill_number = g_invoice_details (l_indx).waybill_number,
                   sales_order_line =
                                   g_invoice_details (l_indx).sales_order_line,
                   sales_order_source =
                                 g_invoice_details (l_indx).sales_order_source,
                   sales_order_revision =
                               g_invoice_details (l_indx).sales_order_revision,
                   purchase_order_revision =
                            g_invoice_details (l_indx).purchase_order_revision,
                   purchase_order = g_invoice_details (l_indx).purchase_order,
                   purchase_order_date =
                                g_invoice_details (l_indx).purchase_order_date,
                   quantity = g_invoice_details (l_indx).quantity,
                   quantity_ordered =
                                   g_invoice_details (l_indx).quantity_ordered,
                   unit_selling_price =
                                 g_invoice_details (l_indx).unit_selling_price,
                   unit_standard_price =
                                g_invoice_details (l_indx).unit_standard_price,
                   last_update_date = SYSDATE,
                   last_updated_by = g_last_updated_by,
                   last_update_login = g_login_id
             WHERE interface_txn_id =
                                   g_invoice_details (l_indx).interface_txn_id;
      END IF;

      FOR r_invline_err_rec IN (SELECT DISTINCT xis.leg_customer_trx_id,
                                                xis.leg_trx_number
                                           FROM xxconv.xxar_br_invoices_stg xis
                                          WHERE xis.process_flag = 'E'
                                            AND xis.batch_id = g_new_batch_id
                      AND xis.run_sequence_id = g_new_run_seq_id)
      LOOP
         UPDATE xxconv.xxar_br_invoices_stg
            SET process_flag = 'E',
                ERROR_TYPE = 'ERR_VAL',
                last_update_date = SYSDATE,
                last_updated_by = g_last_updated_by,
                last_update_login = g_login_id
          WHERE leg_customer_trx_id = r_invline_err_rec.leg_customer_trx_id
            AND batch_id = g_new_batch_id
           AND run_sequence_id = g_new_run_seq_id ;

         l_err_code := 'ETN_INVOICE_ERROR';
         l_err_msg :=
            'Error : Erroring out remaining lines since one of the lines is in error';
         print_log_message (   'For legacy transaction number: '
                            || r_invline_err_rec.leg_trx_number
                           );
         print_log_message (l_err_msg);
         log_errors
            (
             --   pin_transaction_id           =>  r_dist_err_rec.interface_txn_id
             piv_error_type               => 'ERR_VAL',
             piv_source_column_name       => 'LEGACY_CUSTOMER_TRX_ID',
             piv_source_column_value      => r_invline_err_rec.leg_customer_trx_id,
             piv_source_keyname1          => 'LEGACY_TRX_NUMBER',
             piv_source_keyvalue1         => r_invline_err_rec.leg_trx_number,
             piv_error_code               => l_err_code,
             piv_error_message            => l_err_msg,
             pov_return_status            => l_log_ret_status,
             pov_error_msg                => l_log_err_msg
            );
      END LOOP;

      COMMIT;
      l_dist_cust_trx_id := -1;

      OPEN val_dist_cur;

      LOOP
         FETCH val_dist_cur
         BULK COLLECT INTO val_dist_rec LIMIT l_dist_limit;

         EXIT WHEN val_dist_rec.COUNT = 0;

         FOR l_dist_cnt IN 1 .. val_dist_rec.COUNT
         LOOP
           l_valid_flag  := 'Y';     -- added for v1.5
            x_out_acc_rec := NULL;
            x_ccid := NULL;

            IF l_dist_cust_trx_id <>
                                val_dist_rec (l_dist_cnt).leg_customer_trx_id
            THEN
               l_rec_flag := 'N';
               l_rev_flag := 'N';
               l_assign_flag := 'N';
               l_rec_dist_idx := NULL;
               l_rec_int_line_attribute1 := NULL;
               l_rec_int_line_attribute2 := NULL;
               l_rec_int_line_attribute3 := NULL;
               l_rec_int_line_attribute4 := NULL;
               l_rec_int_line_attribute5 := NULL;
               l_rec_int_line_attribute6 := NULL;
               l_rec_int_line_attribute7 := NULL;
               l_rec_int_line_attribute8 := NULL;
               l_rec_int_line_attribute9 := NULL;
               l_rec_int_line_attribute10 := NULL;
               l_rec_int_line_attribute11 := NULL;
               l_rec_int_line_attribute12 := NULL;
               l_rec_int_line_attribute13 := NULL;
               l_rec_int_line_attribute14 := NULL;
               l_rec_int_line_attribute15 := NULL;
            END IF;

            IF     val_dist_rec (l_dist_cnt).leg_cust_trx_type_name LIKE
                                                                       '%OKS%'
               AND val_dist_rec (l_dist_cnt).leg_account_class NOT IN
                                                        ('REV', 'REC', 'TAX')
            THEN
               print_log_message
                      (   'For legacy transaction number: '
                       || val_dist_rec (l_dist_cnt).leg_trx_number
                       || ' distribution entry ignored since it is of class '
                       || val_dist_rec (l_dist_cnt).leg_account_class
                       || ' and not REC or REV'
                      );
            ELSE
               IF check_mandatory
                     (pin_trx_id            => val_dist_rec (l_dist_cnt).interface_txn_id,
                      piv_column_value      => val_dist_rec (l_dist_cnt).leg_dist_segment1,
                      piv_column_name       => 'Distribution segment1',
                      piv_table_name        => 'xxconv.xxar_br_invoices_dist_stg'
                     )
               THEN
                  l_valid_flag := 'E';
               END IF;

               -- Verify Distribution segment2 is NOT NULL
               IF check_mandatory
                     (pin_trx_id            => val_dist_rec (l_dist_cnt).interface_txn_id,
                      piv_column_value      => val_dist_rec (l_dist_cnt).leg_dist_segment2,
                      piv_column_name       => 'Distribution segment2',
                      piv_table_name        => 'xxconv.xxar_br_invoices_dist_stg'
                     )
               THEN
                  l_valid_flag := 'E';
               END IF;

               -- Verify Distribution segment3 is NOT NULL
               IF check_mandatory
                     (pin_trx_id            => val_dist_rec (l_dist_cnt).interface_txn_id,
                      piv_column_value      => val_dist_rec (l_dist_cnt).leg_dist_segment3,
                      piv_column_name       => 'Distribution segment3',
                      piv_table_name        => 'xxconv.xxar_br_invoices_dist_stg'
                     )
               THEN
                  l_valid_flag := 'E';
               END IF;

               -- Verify Distribution segment4 is NOT NULL
               IF check_mandatory
                     (pin_trx_id            => val_dist_rec (l_dist_cnt).interface_txn_id,
                      piv_column_value      => val_dist_rec (l_dist_cnt).leg_dist_segment4,
                      piv_column_name       => 'Distribution segment4',
                      piv_table_name        => 'xxconv.xxar_br_invoices_dist_stg'
                     )
               THEN
                  l_valid_flag := 'E';
               END IF;

               -- Verify Distribution segment5 is NOT NULL
               IF check_mandatory
                     (pin_trx_id            => val_dist_rec (l_dist_cnt).interface_txn_id,
                      piv_column_value      => val_dist_rec (l_dist_cnt).leg_dist_segment5,
                      piv_column_name       => 'Distribution segment5',
                      piv_table_name        => 'xxconv.xxar_br_invoices_dist_stg'
                     )
               THEN
                  l_valid_flag := 'E';
               END IF;

               -- Verify Distribution segment6 is NOT NULL
               IF check_mandatory
                     (pin_trx_id            => val_dist_rec (l_dist_cnt).interface_txn_id,
                      piv_column_value      => val_dist_rec (l_dist_cnt).leg_dist_segment6,
                      piv_column_name       => 'Distribution segment6',
                      piv_table_name        => 'xxconv.xxar_br_invoices_dist_stg'
                     )
               THEN
                  l_valid_flag := 'E';
               END IF;

               -- Verify Distribution segment7 is NOT NULL
               IF check_mandatory
                     (pin_trx_id            => val_dist_rec (l_dist_cnt).interface_txn_id,
                      piv_column_value      => val_dist_rec (l_dist_cnt).leg_dist_segment7,
                      piv_column_name       => 'Distribution segment7',
                      piv_table_name        => 'xxconv.xxar_br_invoices_dist_stg'
                     )
               THEN
                  l_valid_flag := 'E';
               END IF;

               IF val_dist_rec (l_dist_cnt).leg_account_class NOT IN ('REC')
               THEN
                  IF UPPER (val_dist_rec (l_dist_cnt).leg_org_name) <>
                         UPPER (val_dist_rec (l_dist_cnt).leg_operating_unit)
                  THEN
                     l_valid_flag := 'E';
                  ELSE
                     g_invoice_dist (g_dist_idx).org_id :=
                                             val_dist_rec (l_dist_cnt).org_id;
                  END IF;
               END IF;

               validate_accounts (val_dist_rec (l_dist_cnt).interface_txn_id,
                                  val_dist_rec (l_dist_cnt).leg_dist_segment1,
                                  val_dist_rec (l_dist_cnt).leg_dist_segment2,
                                  val_dist_rec (l_dist_cnt).leg_dist_segment3,
                                  val_dist_rec (l_dist_cnt).leg_dist_segment4,
                                  val_dist_rec (l_dist_cnt).leg_dist_segment5,
                                  val_dist_rec (l_dist_cnt).leg_dist_segment6,
                                  val_dist_rec (l_dist_cnt).leg_dist_segment7,
                                  x_out_acc_rec,
                                  x_ccid
                                 );

               IF x_ccid IS NULL
               THEN
                  l_valid_flag := 'E';
               END IF;

               g_invoice_dist (g_dist_idx).dist_segment1 :=
                                                        x_out_acc_rec.segment1;
               g_invoice_dist (g_dist_idx).dist_segment2 :=
                                                        x_out_acc_rec.segment2;
               g_invoice_dist (g_dist_idx).dist_segment3 :=
                                                        x_out_acc_rec.segment3;
               g_invoice_dist (g_dist_idx).dist_segment4 :=
                                                        x_out_acc_rec.segment4;
               g_invoice_dist (g_dist_idx).dist_segment5 :=
                                                        x_out_acc_rec.segment5;
               g_invoice_dist (g_dist_idx).dist_segment6 :=
                                                        x_out_acc_rec.segment6;
               g_invoice_dist (g_dist_idx).dist_segment7 :=
                                                        x_out_acc_rec.segment7;
               g_invoice_dist (g_dist_idx).dist_segment8 :=
                                                        x_out_acc_rec.segment8;
               g_invoice_dist (g_dist_idx).dist_segment9 :=
                                                        x_out_acc_rec.segment9;
               g_invoice_dist (g_dist_idx).dist_segment10 :=
                                                       x_out_acc_rec.segment10;
               g_invoice_dist (g_dist_idx).code_combination_id := x_ccid;
               g_invoice_dist (g_dist_idx).interface_line_context :=
                                                      g_interface_line_context;
               g_invoice_dist (g_dist_idx).interface_line_attribute1 :=
                           val_dist_rec (l_dist_cnt).interface_line_attribute1;
               g_invoice_dist (g_dist_idx).interface_line_attribute15 :=
                          val_dist_rec (l_dist_cnt).interface_line_attribute15;
               g_invoice_dist (g_dist_idx).accounted_amount :=
                                val_dist_rec (l_dist_cnt).leg_accounted_amount;
               g_invoice_dist (g_dist_idx).interface_txn_id :=
                                    val_dist_rec (l_dist_cnt).interface_txn_id;
               g_invoice_dist (g_dist_idx).PERCENT :=
                                         val_dist_rec (l_dist_cnt).leg_percent;

               IF val_dist_rec (l_dist_cnt).leg_account_class = 'FREIGHT'
               THEN
                  g_invoice_dist (g_dist_idx).account_class := 'REV';
               ELSE
                  g_invoice_dist (g_dist_idx).account_class :=
                                  val_dist_rec (l_dist_cnt).leg_account_class;
               END IF;

               IF l_valid_flag = 'E'
               THEN
                  g_invoice_dist (g_dist_idx).process_flag := 'E';
                  g_invoice_dist (g_dist_idx).ERROR_TYPE := 'ERR_VAL';
                  g_retcode := 1;
               ELSE
                  g_invoice_dist (g_dist_idx).process_flag := 'V';
               END IF;

               l_dist_cust_trx_id :=
                                 val_dist_rec (l_dist_cnt).leg_customer_trx_id;

               IF val_dist_rec (l_dist_cnt).leg_account_class = 'REC'
               THEN
                  l_rec_dist_idx := g_dist_idx;
                  l_rec_flag := 'Y';
               END IF;

               IF     val_dist_rec (l_dist_cnt).leg_account_class = 'REV'
                  AND l_assign_flag = 'N'
                  AND val_dist_rec (l_dist_cnt).leg_line_type = 'LINE'
               THEN
                  l_rec_int_line_attribute1 :=
                          val_dist_rec (l_dist_cnt).interface_line_attribute1;
                  l_rec_int_line_attribute15 :=
                         val_dist_rec (l_dist_cnt).interface_line_attribute15;
                  l_dist_org_id := val_dist_rec (l_dist_cnt).org_id;
                  l_rev_flag := 'Y';
               END IF;

               IF     l_rec_flag = 'Y'
                  AND l_rev_flag = 'Y'
                  AND l_rec_dist_idx IS NOT NULL
                  AND l_assign_flag = 'N'
               THEN
                  g_invoice_dist (l_rec_dist_idx).interface_line_context :=
                                                     g_interface_line_context;
                  g_invoice_dist (l_rec_dist_idx).interface_line_attribute1 :=
                                                    l_rec_int_line_attribute1;
                  g_invoice_dist (l_rec_dist_idx).interface_line_attribute15 :=
                                                   l_rec_int_line_attribute15;
                  g_invoice_dist (l_rec_dist_idx).org_id := l_dist_org_id;
                  l_dist_org_id := NULL;
                  l_assign_flag := 'Y';
               END IF;

               g_dist_idx := g_dist_idx + 1;
            END IF;
         END LOOP;
      END LOOP;

      IF g_invoice_dist.EXISTS (1)
      THEN
         FORALL l_indx IN 1 .. g_invoice_dist.COUNT
            -- LOOP
            UPDATE xxconv.xxar_br_invoices_dist_stg
               SET dist_segment1 = g_invoice_dist (l_indx).dist_segment1,
                   dist_segment2 = g_invoice_dist (l_indx).dist_segment2,
                   dist_segment3 = g_invoice_dist (l_indx).dist_segment3,
                   dist_segment4 = g_invoice_dist (l_indx).dist_segment4,
                   dist_segment5 = g_invoice_dist (l_indx).dist_segment5,
                   dist_segment6 = g_invoice_dist (l_indx).dist_segment6,
                   dist_segment7 = g_invoice_dist (l_indx).dist_segment7,
                   dist_segment8 = g_invoice_dist (l_indx).dist_segment8,
                   dist_segment9 = g_invoice_dist (l_indx).dist_segment9,
                   dist_segment10 = g_invoice_dist (l_indx).dist_segment10,
                   code_combination_id =
                                   g_invoice_dist (l_indx).code_combination_id,
                   interface_line_context =
                                g_invoice_dist (l_indx).interface_line_context,
                   interface_line_attribute1 =
                             g_invoice_dist (l_indx).interface_line_attribute1,
                   interface_line_attribute15 =
                            g_invoice_dist (l_indx).interface_line_attribute15,
                   accounted_amount = g_invoice_dist (l_indx).accounted_amount,
                   account_class = g_invoice_dist (l_indx).account_class,
                   org_id = g_invoice_dist (l_indx).org_id,
                   PERCENT = g_invoice_dist (l_indx).PERCENT,
                   process_flag = g_invoice_dist (l_indx).process_flag,
                   ERROR_TYPE = g_invoice_dist (l_indx).ERROR_TYPE,
                   last_update_date = SYSDATE,
                   last_updated_by = g_last_updated_by,
                   last_update_login = g_login_id
             WHERE interface_txn_id = g_invoice_dist (l_indx).interface_txn_id;
      --END LOOP;
      END IF;

      FOR r_dist_err_rec IN (SELECT DISTINCT xds.leg_customer_trx_id,
                                             xds.process_flag
                                        FROM xxconv.xxar_br_invoices_dist_stg xds
                                       WHERE xds.process_flag IN ('N', 'E')
                                         AND xds.batch_id = g_new_batch_id
                     AND xds.run_sequence_id = g_new_run_seq_id
                                         AND DECODE (xds.process_flag,
                                                     'E', NVL (xds.ERROR_TYPE,
                                                               'A'
                                                              ),
                                                     'ERR_VAL'
                                                    ) = 'ERR_VAL')
      LOOP
         g_retcode := 1;

         UPDATE xxconv.xxar_br_invoices_dist_stg
            SET process_flag = 'E',
                ERROR_TYPE = 'ERR_VAL',
                last_update_date = SYSDATE,
                last_updated_by = g_last_updated_by,
                last_update_login = g_login_id
          WHERE leg_customer_trx_id = r_dist_err_rec.leg_customer_trx_id
            AND batch_id = g_new_batch_id
      AND run_sequence_id = g_new_run_seq_id;

         IF r_dist_err_rec.process_flag <> 'N'
         THEN
            UPDATE xxconv.xxar_br_invoices_stg
               SET process_flag = 'E',
                   ERROR_TYPE = 'ERR_VAL',
                   last_update_date = SYSDATE,
                   last_updated_by = g_last_updated_by,
                   last_update_login = g_login_id
             WHERE leg_customer_trx_id = r_dist_err_rec.leg_customer_trx_id
               AND batch_id = g_new_batch_id
         AND run_sequence_id = g_new_run_seq_id;

            l_err_code := 'ETN_INVOICE_ERROR';
            l_err_msg :=
               'Error : Erroring out lines since corresponding distribution is in error';
            log_errors
               (
                --   pin_transaction_id           =>  r_dist_err_rec.interface_txn_id
                piv_error_type               => 'ERR_VAL',
                piv_source_column_name       => 'LEGACY_CUSTOMER_TRX_ID',
                piv_source_column_value      => r_dist_err_rec.leg_customer_trx_id,
                piv_error_code               => l_err_code,
                piv_error_message            => l_err_msg,
                pov_return_status            => l_log_ret_status,
                pov_error_msg                => l_log_err_msg
               );
         END IF;

         l_err_code := 'ETN_DISTRIBUTION_ERROR';

         IF r_dist_err_rec.process_flag = 'N'
         THEN
            l_err_msg :=
               'Error : Erroring distribution since corresponding invoice line in error ';
         ELSE
            l_err_msg :=
               'Error : Erroring distribution since another related distribution in error ';
         END IF;

         log_errors
              (  --pin_transaction_id      => r_dist_err_rec.interface_txn_id,
               piv_error_type               => 'ERR_VAL',
               piv_source_column_name       => 'LEGACY_CUSTOMER_TRX_ID',
               piv_source_column_value      => r_dist_err_rec.leg_customer_trx_id,
               piv_error_code               => l_err_code,
               piv_error_message            => l_err_msg,
               pov_return_status            => l_log_ret_status,
               piv_source_table             => 'XXAR_BR_INVOICES_DIST_STG',
               pov_error_msg                => l_log_err_msg
              );
      END LOOP;

      COMMIT;
   EXCEPTION
      WHEN OTHERS
      THEN
         print_log_message (   'Error : Backtace : '
                            || DBMS_UTILITY.format_error_backtrace
                           );
         g_retcode := 2;
         l_err_code := 'ETN_BR_PROCEDURE_EXCEPTION';
         l_err_msg :=
               'Error : Exception in validate_invoice Procedure. '
            || SUBSTR (SQLERRM, 1, 150);
         log_errors (
                     -- pin_transaction_id           =>  pin_trx_id
                     -- , piv_source_column_name     =>  'Legacy link_to_customer_trx_line_id'
                     --  , piv_source_column_value    =>  val_inv_det_rec(l_line_cnt).leg_link_to_cust_trx_line_id
                     piv_error_type         => 'ERR_VAL',
                     piv_error_code         => l_err_code,
                     piv_error_message      => l_err_msg,
                     pov_return_status      => l_log_ret_status,
                     pov_error_msg          => l_log_err_msg
                    );
   END validate_invoice;

   --ver1.11
   PROCEDURE group_tax_lines
   IS
      CURSOR get_tax_lines_cur
      IS
         SELECT   batch_id, load_id, leg_due_date, leg_cust_trx_type_name,
                  leg_inv_amount_due_remaining, leg_customer_trx_id,
                  run_sequence_id, program_application_id, program_id,
                  program_update_date, request_id, interface_line_context,
                  link_to_line_context, link_to_line_attribute1,
                  link_to_line_attribute2, link_to_line_attribute3,
                  link_to_line_attribute4, link_to_line_attribute5,
                  link_to_line_attribute6, link_to_line_attribute7,
                  link_to_line_attribute8, link_to_line_attribute9,
                  link_to_line_attribute10, link_to_line_attribute11,
                  link_to_line_attribute12, link_to_line_attribute13,
                  link_to_line_attribute14, link_to_line_attribute15,
                  batch_source_name, set_of_books_id, memo_line_id,
                  line_type, description, currency_code, cust_trx_type_name,
                  transaction_type_id, term_name, term_id,
                  system_bill_customer_ref, system_bill_customer_id,
                  system_bill_address_ref, system_bill_address_id,
                  system_ship_address_id
                                        --,system_ship_address_id
                                        --,system_bill_customer_id
                  , purchase_order, reason_code, header_attribute_category,
                  header_attribute1, header_attribute4, header_attribute8,
                  attribute_category, attribute1, attribute2, attribute3,
                  attribute4, attribute5, attribute6, attribute7, attribute8,
                  attribute9, attribute10, attribute11, attribute12,
                  attribute13, attribute14, attribute15,
                  header_gdf_attr_category, header_gdf_attribute1,
                  header_gdf_attribute2, header_gdf_attribute3,
                  header_gdf_attribute4, header_gdf_attribute5,
                  header_gdf_attribute6, header_gdf_attribute7,
                  header_gdf_attribute8, header_gdf_attribute9,
                  header_gdf_attribute10, header_gdf_attribute11,
                  header_gdf_attribute12, header_gdf_attribute13,
                  header_gdf_attribute14, header_gdf_attribute15,
                  header_gdf_attribute16, header_gdf_attribute17,
                  header_gdf_attribute18, header_gdf_attribute19,
                  header_gdf_attribute20, header_gdf_attribute21,
                  header_gdf_attribute22, header_gdf_attribute23,
                  header_gdf_attribute24, header_gdf_attribute25,
                  header_gdf_attribute26, header_gdf_attribute27,
                  header_gdf_attribute28, header_gdf_attribute29,
                  header_gdf_attribute30, line_gdf_attr_category,
                  line_gdf_attribute1, line_gdf_attribute2,
                  line_gdf_attribute3, line_gdf_attribute4,
                  line_gdf_attribute5, line_gdf_attribute6,
                  line_gdf_attribute7, line_gdf_attribute8,
                  line_gdf_attribute9, line_gdf_attribute10,
                  line_gdf_attribute11, line_gdf_attribute12,
                  line_gdf_attribute13, line_gdf_attribute14,
                  line_gdf_attribute15, line_gdf_attribute16,
                  line_gdf_attribute17, line_gdf_attribute18,
                  line_gdf_attribute19, line_gdf_attribute20, trx_date,
                  gl_date, trx_number, line_number, tax_code,
                  tax_regime_code, tax_rate_code, tax, tax_status_code,
                  tax_jurisdiction_code, amount_includes_tax_flag,
                  taxable_flag, sales_order_date, sales_order, uom_name,
                  ussgl_transaction_code_context, internal_notes,
                  ship_date_actual, fob_point, ship_via, waybill_number,
                  sales_order_line, sales_order_source, sales_order_revision,
                  purchase_order_revision, agreement_id, purchase_order_date,
                  invoicing_rule_id, org_id, conversion_type,
                  conversion_rate, conversion_date, SUM (quantity) quantity,
                  SUM (quantity_ordered) quantity_ordered,
                  SUM (unit_selling_price) unit_selling_price,
                  SUM (unit_standard_price) unit_standard_price,
                  SUM (tax_rate) tax_rate,
                  MAX (interface_line_attribute1) interface_line_attribute1,
                  MAX (interface_line_attribute15)
                                                  interface_line_attribute15,
                  SUM (line_amount) line_amount, MAX (comments) comments
             --MAX(interface_txn_id) interface_txn_id
             --,xxar_invoices_ext_r12_s.nextval interface_txn_id
         FROM     xxconv.xxar_br_invoices_stg
            WHERE process_flag = 'V'
              AND batch_id = g_new_batch_id
              AND leg_line_type = 'TAX'
         GROUP BY batch_id,
                  load_id,
                  leg_due_date,
                  leg_cust_trx_type_name,
                  leg_inv_amount_due_remaining,
                  leg_customer_trx_id,
                  run_sequence_id,
                  program_application_id,
                  program_id,
                  program_update_date,
                  request_id,
                  interface_line_context,
                  link_to_line_context,
                  link_to_line_attribute1,
                  link_to_line_attribute2,
                  link_to_line_attribute3,
                  link_to_line_attribute4,
                  link_to_line_attribute5,
                  link_to_line_attribute6,
                  link_to_line_attribute7,
                  link_to_line_attribute8,
                  link_to_line_attribute9,
                  link_to_line_attribute10,
                  link_to_line_attribute11,
                  link_to_line_attribute12,
                  link_to_line_attribute13,
                  link_to_line_attribute14,
                  link_to_line_attribute15,
                  batch_source_name,
                  set_of_books_id,
                  memo_line_id,
                  line_type,
                  description,
                  currency_code,
                  cust_trx_type_name,
                  transaction_type_id,
                  term_name,
                  term_id,
                  system_bill_customer_ref,
                  system_bill_customer_id,
                  system_bill_address_ref,
                  system_bill_address_id,
                  system_ship_address_id
                                        --,system_ship_address_id
                                        --,system_bill_customer_id
         ,
                  purchase_order,
                  reason_code,
                  header_attribute_category,
                  header_attribute1,
                  header_attribute4,
                  header_attribute8,
                  attribute_category,
                  attribute1,
                  attribute2,
                  attribute3,
                  attribute4,
                  attribute5,
                  attribute6,
                  attribute7,
                  attribute8,
                  attribute9,
                  attribute10,
                  attribute11,
                  attribute12,
                  attribute13,
                  attribute14,
                  attribute15,
                  header_gdf_attr_category,
                  header_gdf_attribute1,
                  header_gdf_attribute2,
                  header_gdf_attribute3,
                  header_gdf_attribute4,
                  header_gdf_attribute5,
                  header_gdf_attribute6,
                  header_gdf_attribute7,
                  header_gdf_attribute8,
                  header_gdf_attribute9,
                  header_gdf_attribute10,
                  header_gdf_attribute11,
                  header_gdf_attribute12,
                  header_gdf_attribute13,
                  header_gdf_attribute14,
                  header_gdf_attribute15,
                  header_gdf_attribute16,
                  header_gdf_attribute17,
                  header_gdf_attribute18,
                  header_gdf_attribute19,
                  header_gdf_attribute20,
                  header_gdf_attribute21,
                  header_gdf_attribute22,
                  header_gdf_attribute23,
                  header_gdf_attribute24,
                  header_gdf_attribute25,
                  header_gdf_attribute26,
                  header_gdf_attribute27,
                  header_gdf_attribute28,
                  header_gdf_attribute29,
                  header_gdf_attribute30,
                  line_gdf_attr_category,
                  line_gdf_attribute1,
                  line_gdf_attribute2,
                  line_gdf_attribute3,
                  line_gdf_attribute4,
                  line_gdf_attribute5,
                  line_gdf_attribute6,
                  line_gdf_attribute7,
                  line_gdf_attribute8,
                  line_gdf_attribute9,
                  line_gdf_attribute10,
                  line_gdf_attribute11,
                  line_gdf_attribute12,
                  line_gdf_attribute13,
                  line_gdf_attribute14,
                  line_gdf_attribute15,
                  line_gdf_attribute16,
                  line_gdf_attribute17,
                  line_gdf_attribute18,
                  line_gdf_attribute19,
                  line_gdf_attribute20,
                  trx_date,
                  gl_date,
                  trx_number,
                  line_number,
                  tax_code,
                  tax_regime_code,
                  tax_rate_code,
                  tax,
                  tax_status_code,
                  tax_jurisdiction_code,
                  amount_includes_tax_flag,
                  taxable_flag,
                  sales_order_date,
                  sales_order,
                  uom_name,
                  ussgl_transaction_code_context,
                  internal_notes,
                  ship_date_actual,
                  fob_point,
                  ship_via,
                  waybill_number,
                  sales_order_line,
                  sales_order_source,
                  sales_order_revision,
                  purchase_order_revision,
                  agreement_id,
                  purchase_order_date,
                  invoicing_rule_id,
                  org_id,
                  conversion_type,
                  conversion_rate,
                  conversion_date
           HAVING COUNT (1) > 1;

      CURSOR get_tax_dist_cur (
         p_link_to_line_attribute1      IN   VARCHAR2,
         p_link_to_line_attribute15     IN   VARCHAR2,
         p_tax_regime_code              IN   VARCHAR2,
         p_tax_rate_code                IN   VARCHAR2,
         p_tax                          IN   VARCHAR2,
         p_tax_status_code              IN   VARCHAR2,
         p_tax_jurisdiction_code        IN   VARCHAR2,
         p_interface_line_attribute1    IN   VARCHAR2,
         p_interface_line_attribute15   IN   VARCHAR2,
         p_tax_line_amount              IN   NUMBER
      )
      IS
         SELECT   xds.batch_id, xds.load_id, xds.leg_customer_trx_id,
                  xds.run_sequence_id, xds.program_application_id,
                  xds.program_id, xds.program_update_date, xds.request_id,
                  xds.code_combination_id, xds.org_id, xds.dist_segment1,
                  xds.dist_segment2, xds.dist_segment3, xds.dist_segment4,
                  xds.dist_segment5, xds.dist_segment6, xds.dist_segment7,
                  xds.dist_segment8, xds.dist_segment9, xds.dist_segment10,
                  xds.interface_line_context,
                  p_interface_line_attribute1 interface_line_attribute1,
                  p_interface_line_attribute15 interface_line_attribute15,
                  xds.account_class
                                   --,ROUND((SUM(xds.accounted_amount) *100)/p_tax_line_amount, 4) tax_dist_per
                  ,
                  SUM (xds.accounted_amount) accounted_amount
             --MAX(xds.interface_txn_id) interface_txn_id
             --,xxar_invoices_dist_ext_r12_s.nextval interface_txn_id
         FROM     xxconv.xxar_br_invoices_dist_stg xds,
                  xxconv.xxar_br_invoices_stg xis
            WHERE xds.leg_customer_trx_id = xis.leg_customer_trx_id
              AND xds.leg_cust_trx_line_id = xis.leg_cust_trx_line_id
              AND NVL (xds.leg_account_class, 'A') = 'TAX'
              AND xds.process_flag = 'V'
              AND xds.batch_id = g_new_batch_id
              AND xis.process_flag = 'X'
              AND NVL (xis.link_to_line_attribute1, 'NO VALUE') =
                                   NVL (p_link_to_line_attribute1, 'NO VALUE')
              AND NVL (xis.link_to_line_attribute15, 'NO VALUE') =
                                  NVL (p_link_to_line_attribute15, 'NO VALUE')
              AND NVL (xis.tax_regime_code, 'NO VALUE') =
                                           NVL (p_tax_regime_code, 'NO VALUE')
              AND NVL (xis.tax_rate_code, 'NO VALUE') =
                                             NVL (p_tax_rate_code, 'NO VALUE')
              AND NVL (xis.tax, 'NO VALUE') = NVL (p_tax, 'NO VALUE')
              AND NVL (xis.tax_status_code, 'NO VALUE') =
                                           NVL (p_tax_status_code, 'NO VALUE')
              AND NVL (xis.tax_jurisdiction_code, 'NO VALUE') =
                                     NVL (p_tax_jurisdiction_code, 'NO VALUE')
         GROUP BY xds.batch_id,
                  xds.load_id,
                  xds.leg_customer_trx_id,
                  xds.run_sequence_id,
                  xds.program_application_id,
                  xds.program_id,
                  xds.program_update_date,
                  xds.request_id,
                  xds.code_combination_id,
                  xds.org_id,
                  xds.dist_segment1,
                  xds.dist_segment2,
                  xds.dist_segment3,
                  xds.dist_segment4,
                  xds.dist_segment5,
                  xds.dist_segment6,
                  xds.dist_segment7,
                  xds.dist_segment8,
                  xds.dist_segment9,
                  xds.dist_segment10,
                  xds.interface_line_context,
                  p_interface_line_attribute1,
                  p_interface_line_attribute15,
                  xds.account_class;

      l_tax_insert_flag   VARCHAR2 (1);
      l_err               VARCHAR2 (1000);
   BEGIN
      FOR get_tax_lines_rec IN get_tax_lines_cur
      LOOP
         BEGIN
            SAVEPOINT tax;
            l_tax_insert_flag := NULL;

            INSERT INTO xxconv.xxar_br_invoices_stg
                        (interface_txn_id,
                         batch_id,
                         load_id,
                         leg_customer_trx_id,
                         run_sequence_id, creation_date,
                         created_by, last_update_date, last_updated_by,
                         last_update_login,
                         program_application_id,
                         program_id,
                         program_update_date,
                         request_id, process_flag,
                         interface_line_context,
                         link_to_line_context,
                         link_to_line_attribute1,
                         link_to_line_attribute2,
                         link_to_line_attribute3,
                         link_to_line_attribute4,
                         link_to_line_attribute5,
                         link_to_line_attribute6,
                         link_to_line_attribute7,
                         link_to_line_attribute8,
                         link_to_line_attribute9,
                         link_to_line_attribute10,
                         link_to_line_attribute11,
                         link_to_line_attribute12,
                         link_to_line_attribute13,
                         link_to_line_attribute14,
                         link_to_line_attribute15,
                         batch_source_name,
                         set_of_books_id,
                         memo_line_id,
                         line_type,
                         description,
                         currency_code,
                         cust_trx_type_name,
                         transaction_type_id,
                         term_name,
                         term_id,
                         system_bill_customer_ref,
                         system_bill_customer_id,
                         system_bill_address_ref,
                         system_bill_address_id,
                         system_ship_address_id
                                               --,system_ship_address_id
                                               --,system_bill_customer_id
            ,
                         purchase_order,
                         reason_code,
                         header_attribute_category,
                         header_attribute1,
                         header_attribute4,
                         header_attribute8,
                         attribute_category,
                         attribute1,
                         attribute2,
                         attribute3,
                         attribute4,
                         attribute5,
                         attribute6,
                         attribute7,
                         attribute8,
                         attribute9,
                         attribute10,
                         attribute11,
                         attribute12,
                         attribute13,
                         attribute14,
                         attribute15,
                         header_gdf_attr_category,
                         header_gdf_attribute1,
                         header_gdf_attribute2,
                         header_gdf_attribute3,
                         header_gdf_attribute4,
                         header_gdf_attribute5,
                         header_gdf_attribute6,
                         header_gdf_attribute7,
                         header_gdf_attribute8,
                         header_gdf_attribute9,
                         header_gdf_attribute10,
                         header_gdf_attribute11,
                         header_gdf_attribute12,
                         header_gdf_attribute13,
                         header_gdf_attribute14,
                         header_gdf_attribute15,
                         header_gdf_attribute16,
                         header_gdf_attribute17,
                         header_gdf_attribute18,
                         header_gdf_attribute19,
                         header_gdf_attribute20,
                         header_gdf_attribute21,
                         header_gdf_attribute22,
                         header_gdf_attribute23,
                         header_gdf_attribute24,
                         header_gdf_attribute25,
                         header_gdf_attribute26,
                         header_gdf_attribute27,
                         header_gdf_attribute28,
                         header_gdf_attribute29,
                         header_gdf_attribute30,
                         line_gdf_attr_category,
                         line_gdf_attribute1,
                         line_gdf_attribute2,
                         line_gdf_attribute3,
                         line_gdf_attribute4,
                         line_gdf_attribute5,
                         line_gdf_attribute6,
                         line_gdf_attribute7,
                         line_gdf_attribute8,
                         line_gdf_attribute9,
                         line_gdf_attribute10,
                         line_gdf_attribute11,
                         line_gdf_attribute12,
                         line_gdf_attribute13,
                         line_gdf_attribute14,
                         line_gdf_attribute15,
                         line_gdf_attribute16,
                         line_gdf_attribute17,
                         line_gdf_attribute18,
                         line_gdf_attribute19,
                         line_gdf_attribute20,
                         trx_date,
                         gl_date,
                         trx_number,
                         line_number,
                         tax_code,
                         tax_regime_code,
                         tax_rate_code,
                         tax,
                         tax_status_code,
                         tax_jurisdiction_code,
                         amount_includes_tax_flag,
                         taxable_flag,
                         sales_order_date,
                         sales_order,
                         uom_name,
                         ussgl_transaction_code_context,
                         internal_notes,
                         ship_date_actual,
                         fob_point,
                         ship_via,
                         waybill_number,
                         sales_order_line,
                         sales_order_source,
                         sales_order_revision,
                         purchase_order_revision,
                         agreement_id,
                         purchase_order_date,
                         invoicing_rule_id,
                         org_id,
                         conversion_type,
                         conversion_rate,
                         conversion_date,
                         quantity_ordered,
                         unit_selling_price,
                         unit_standard_price,
                         tax_rate,
                         interface_line_attribute1,
                         interface_line_attribute15,
                         line_amount,
                         comments,
                         leg_inv_amount_due_remaining,
                         leg_trx_number,
                         leg_currency_code,
                         leg_line_type,
                         leg_due_date,
                         leg_cust_trx_type_name,
                         leg_line_amount
                        )
                 VALUES (xxar_invoices_ext_r12_s.NEXTVAL
                                                        --get_tax_lines_rec.interface_txn_id
            ,
                         get_tax_lines_rec.batch_id,
                         get_tax_lines_rec.load_id,
                         get_tax_lines_rec.leg_customer_trx_id,
                         get_tax_lines_rec.run_sequence_id, SYSDATE,
                         g_user_id, SYSDATE, g_user_id,
                         g_login_id,
                         get_tax_lines_rec.program_application_id,
                         get_tax_lines_rec.program_id,
                         get_tax_lines_rec.program_update_date,
                         get_tax_lines_rec.request_id, 'V',
                         get_tax_lines_rec.interface_line_context,
                         get_tax_lines_rec.link_to_line_context,
                         get_tax_lines_rec.link_to_line_attribute1,
                         get_tax_lines_rec.link_to_line_attribute2,
                         get_tax_lines_rec.link_to_line_attribute3,
                         get_tax_lines_rec.link_to_line_attribute4,
                         get_tax_lines_rec.link_to_line_attribute5,
                         get_tax_lines_rec.link_to_line_attribute6,
                         get_tax_lines_rec.link_to_line_attribute7,
                         get_tax_lines_rec.link_to_line_attribute8,
                         get_tax_lines_rec.link_to_line_attribute9,
                         get_tax_lines_rec.link_to_line_attribute10,
                         get_tax_lines_rec.link_to_line_attribute11,
                         get_tax_lines_rec.link_to_line_attribute12,
                         get_tax_lines_rec.link_to_line_attribute13,
                         get_tax_lines_rec.link_to_line_attribute14,
                         get_tax_lines_rec.link_to_line_attribute15,
                         get_tax_lines_rec.batch_source_name,
                         get_tax_lines_rec.set_of_books_id,
                         get_tax_lines_rec.memo_line_id,
                         get_tax_lines_rec.line_type,
                         get_tax_lines_rec.description,
                         get_tax_lines_rec.currency_code,
                         get_tax_lines_rec.cust_trx_type_name,
                         get_tax_lines_rec.transaction_type_id,
                         get_tax_lines_rec.term_name,
                         get_tax_lines_rec.term_id,
                         get_tax_lines_rec.system_bill_customer_ref,
                         get_tax_lines_rec.system_bill_customer_id,
                         get_tax_lines_rec.system_bill_address_ref,
                         get_tax_lines_rec.system_bill_address_id,
                         get_tax_lines_rec.system_ship_address_id
                                                                 --,get_tax_lines_rec.system_ship_address_id
                                                                 --,get_tax_lines_rec.system_bill_customer_id
            ,
                         get_tax_lines_rec.purchase_order,
                         get_tax_lines_rec.reason_code,
                         get_tax_lines_rec.header_attribute_category,
                         get_tax_lines_rec.header_attribute1,
                         get_tax_lines_rec.header_attribute4,
                         get_tax_lines_rec.header_attribute8,
                         get_tax_lines_rec.attribute_category,
                         get_tax_lines_rec.attribute1,
                         get_tax_lines_rec.attribute2,
                         get_tax_lines_rec.attribute3,
                         get_tax_lines_rec.attribute4,
                         get_tax_lines_rec.attribute5,
                         get_tax_lines_rec.attribute6,
                         get_tax_lines_rec.attribute7,
                         get_tax_lines_rec.attribute8,
                         get_tax_lines_rec.attribute9,
                         get_tax_lines_rec.attribute10,
                         get_tax_lines_rec.attribute11,
                         get_tax_lines_rec.attribute12,
                         get_tax_lines_rec.attribute13,
                         get_tax_lines_rec.attribute14,
                         get_tax_lines_rec.attribute15,
                         get_tax_lines_rec.header_gdf_attr_category,
                         get_tax_lines_rec.header_gdf_attribute1,
                         get_tax_lines_rec.header_gdf_attribute2,
                         get_tax_lines_rec.header_gdf_attribute3,
                         get_tax_lines_rec.header_gdf_attribute4,
                         get_tax_lines_rec.header_gdf_attribute5,
                         get_tax_lines_rec.header_gdf_attribute6,
                         get_tax_lines_rec.header_gdf_attribute7,
                         get_tax_lines_rec.header_gdf_attribute8,
                         get_tax_lines_rec.header_gdf_attribute9,
                         get_tax_lines_rec.header_gdf_attribute10,
                         get_tax_lines_rec.header_gdf_attribute11,
                         get_tax_lines_rec.header_gdf_attribute12,
                         get_tax_lines_rec.header_gdf_attribute13,
                         get_tax_lines_rec.header_gdf_attribute14,
                         get_tax_lines_rec.header_gdf_attribute15,
                         get_tax_lines_rec.header_gdf_attribute16,
                         get_tax_lines_rec.header_gdf_attribute17,
                         get_tax_lines_rec.header_gdf_attribute18,
                         get_tax_lines_rec.header_gdf_attribute19,
                         get_tax_lines_rec.header_gdf_attribute20,
                         get_tax_lines_rec.header_gdf_attribute21,
                         get_tax_lines_rec.header_gdf_attribute22,
                         get_tax_lines_rec.header_gdf_attribute23,
                         get_tax_lines_rec.header_gdf_attribute24,
                         get_tax_lines_rec.header_gdf_attribute25,
                         get_tax_lines_rec.header_gdf_attribute26,
                         get_tax_lines_rec.header_gdf_attribute27,
                         get_tax_lines_rec.header_gdf_attribute28,
                         get_tax_lines_rec.header_gdf_attribute29,
                         get_tax_lines_rec.header_gdf_attribute30,
                         get_tax_lines_rec.line_gdf_attr_category,
                         get_tax_lines_rec.line_gdf_attribute1,
                         get_tax_lines_rec.line_gdf_attribute2,
                         get_tax_lines_rec.line_gdf_attribute3,
                         get_tax_lines_rec.line_gdf_attribute4,
                         get_tax_lines_rec.line_gdf_attribute5,
                         get_tax_lines_rec.line_gdf_attribute6,
                         get_tax_lines_rec.line_gdf_attribute7,
                         get_tax_lines_rec.line_gdf_attribute8,
                         get_tax_lines_rec.line_gdf_attribute9,
                         get_tax_lines_rec.line_gdf_attribute10,
                         get_tax_lines_rec.line_gdf_attribute11,
                         get_tax_lines_rec.line_gdf_attribute12,
                         get_tax_lines_rec.line_gdf_attribute13,
                         get_tax_lines_rec.line_gdf_attribute14,
                         get_tax_lines_rec.line_gdf_attribute15,
                         get_tax_lines_rec.line_gdf_attribute16,
                         get_tax_lines_rec.line_gdf_attribute17,
                         get_tax_lines_rec.line_gdf_attribute18,
                         get_tax_lines_rec.line_gdf_attribute19,
                         get_tax_lines_rec.line_gdf_attribute20,
                         get_tax_lines_rec.trx_date,
                         get_tax_lines_rec.gl_date,
                         get_tax_lines_rec.trx_number,
                         get_tax_lines_rec.line_number,
                         get_tax_lines_rec.tax_code,
                         get_tax_lines_rec.tax_regime_code,
                         get_tax_lines_rec.tax_rate_code,
                         get_tax_lines_rec.tax,
                         get_tax_lines_rec.tax_status_code,
                         get_tax_lines_rec.tax_jurisdiction_code,
                         get_tax_lines_rec.amount_includes_tax_flag,
                         get_tax_lines_rec.taxable_flag,
                         get_tax_lines_rec.sales_order_date,
                         get_tax_lines_rec.sales_order,
                         get_tax_lines_rec.uom_name,
                         get_tax_lines_rec.ussgl_transaction_code_context,
                         get_tax_lines_rec.internal_notes,
                         get_tax_lines_rec.ship_date_actual,
                         get_tax_lines_rec.fob_point,
                         get_tax_lines_rec.ship_via,
                         get_tax_lines_rec.waybill_number,
                         get_tax_lines_rec.sales_order_line,
                         get_tax_lines_rec.sales_order_source,
                         get_tax_lines_rec.sales_order_revision,
                         get_tax_lines_rec.purchase_order_revision,
                         get_tax_lines_rec.agreement_id,
                         get_tax_lines_rec.purchase_order_date,
                         get_tax_lines_rec.invoicing_rule_id,
                         get_tax_lines_rec.org_id,
                         get_tax_lines_rec.conversion_type,
                         get_tax_lines_rec.conversion_rate,
                         get_tax_lines_rec.conversion_date,
                         get_tax_lines_rec.quantity_ordered,
                         get_tax_lines_rec.unit_selling_price,
                         get_tax_lines_rec.unit_standard_price,
                         get_tax_lines_rec.tax_rate,
                         get_tax_lines_rec.interface_line_attribute1,
                         get_tax_lines_rec.interface_line_attribute15,
                         get_tax_lines_rec.line_amount,
                         get_tax_lines_rec.comments,
                         get_tax_lines_rec.leg_inv_amount_due_remaining,
                         get_tax_lines_rec.trx_number,
                         get_tax_lines_rec.currency_code,
                         get_tax_lines_rec.line_type,
                         get_tax_lines_rec.leg_due_date,
                         get_tax_lines_rec.leg_cust_trx_type_name,
                         get_tax_lines_rec.line_amount
                        );

            UPDATE xxconv.xxar_br_invoices_stg
               SET process_flag = 'X',
                   last_update_date = SYSDATE,
                   last_updated_by = g_last_updated_by,
                   last_update_login = g_login_id
             WHERE batch_id = g_new_batch_id
               AND run_sequence_id = g_new_run_seq_id
               AND leg_line_type = 'TAX'
               AND leg_tax_code IS NOT NULL
               AND NVL (link_to_line_attribute1, 'NO VALUE') =
                      NVL (get_tax_lines_rec.link_to_line_attribute1,
                           'NO VALUE'
                          )
               AND NVL (link_to_line_attribute15, 'NO VALUE') =
                      NVL (get_tax_lines_rec.link_to_line_attribute15,
                           'NO VALUE'
                          )
               AND NVL (tax_regime_code, 'NO VALUE') =
                           NVL (get_tax_lines_rec.tax_regime_code, 'NO VALUE')
               AND NVL (tax_rate_code, 'NO VALUE') =
                             NVL (get_tax_lines_rec.tax_rate_code, 'NO VALUE')
               AND NVL (tax, 'NO VALUE') =
                                       NVL (get_tax_lines_rec.tax, 'NO VALUE')
               AND NVL (tax_status_code, 'NO VALUE') =
                           NVL (get_tax_lines_rec.tax_status_code, 'NO VALUE')
               AND NVL (tax_jurisdiction_code, 'NO VALUE') =
                      NVL (get_tax_lines_rec.tax_jurisdiction_code,
                           'NO VALUE');

            COMMIT;
            l_tax_insert_flag := 'S';
         EXCEPTION
            WHEN OTHERS
            THEN
               l_err := SQLERRM;
               ROLLBACK TO tax;
               l_tax_insert_flag := 'E';
         END;

         IF l_tax_insert_flag = 'S'
         THEN
            FOR get_tax_dist_rec IN
               get_tax_dist_cur
                               (get_tax_lines_rec.link_to_line_attribute1,
                                get_tax_lines_rec.link_to_line_attribute15,
                                get_tax_lines_rec.tax_regime_code,
                                get_tax_lines_rec.tax_rate_code,
                                get_tax_lines_rec.tax,
                                get_tax_lines_rec.tax_status_code,
                                get_tax_lines_rec.tax_jurisdiction_code,
                                get_tax_lines_rec.interface_line_attribute1,
                                get_tax_lines_rec.interface_line_attribute15,
                                get_tax_lines_rec.line_amount
                               )
            LOOP
               BEGIN
                  INSERT INTO xxconv.xxar_br_invoices_dist_stg
                              (accounted_amount,
                               code_combination_id,
                               org_id,
                               leg_customer_trx_id,
                               dist_segment1,
                               dist_segment2,
                               dist_segment3,
                               dist_segment4,
                               dist_segment5,
                               dist_segment6,
                               dist_segment7,
                               dist_segment8,
                               dist_segment9,
                               dist_segment10,
                               interface_line_context,
                               interface_line_attribute1,
                               interface_line_attribute15,
                               account_class
                                            --  ,PERCENT
                  ,
                               interface_txn_id,
                               batch_id,
                               load_id,
                               run_sequence_id, creation_date,
                               created_by, last_update_date,
                               last_updated_by, last_update_login,
                               program_application_id,
                               program_id,
                               program_update_date,
                               request_id, process_flag
                              )
                       VALUES (get_tax_dist_rec.accounted_amount,
                               get_tax_dist_rec.code_combination_id,
                               get_tax_dist_rec.org_id,
                               get_tax_dist_rec.leg_customer_trx_id,
                               get_tax_dist_rec.dist_segment1,
                               get_tax_dist_rec.dist_segment2,
                               get_tax_dist_rec.dist_segment3,
                               get_tax_dist_rec.dist_segment4,
                               get_tax_dist_rec.dist_segment5,
                               get_tax_dist_rec.dist_segment6,
                               get_tax_dist_rec.dist_segment7,
                               get_tax_dist_rec.dist_segment8,
                               get_tax_dist_rec.dist_segment9,
                               get_tax_dist_rec.dist_segment10,
                               get_tax_dist_rec.interface_line_context,
                               get_tax_dist_rec.interface_line_attribute1,
                               get_tax_dist_rec.interface_line_attribute15,
                               get_tax_dist_rec.account_class
                                                             -- ,get_tax_dist_rec.tax_dist_per
                  ,
                               xxar_invoices_dist_ext_r12_s.NEXTVAL
                                                                   --get_tax_dist_rec.interface_txn_id
                  ,
                               get_tax_dist_rec.batch_id,
                               get_tax_dist_rec.load_id,
                               get_tax_dist_rec.run_sequence_id, SYSDATE,
                               g_user_id, SYSDATE,
                               g_user_id, g_login_id,
                               get_tax_dist_rec.program_application_id,
                               get_tax_dist_rec.program_id,
                               get_tax_dist_rec.program_update_date,
                               get_tax_dist_rec.request_id, 'V'
                              );

                  UPDATE xxconv.xxar_br_invoices_dist_stg xds
                     SET xds.process_flag = 'X',
                         xds.last_update_date = SYSDATE,
                         xds.last_updated_by = g_last_updated_by,
                         xds.last_update_login = g_login_id
                   WHERE xds.batch_id = g_new_batch_id
                     AND xds.run_sequence_id = g_new_run_seq_id
                     AND xds.leg_account_class = 'TAX'
                     AND (xds.interface_line_attribute1,
                          xds.interface_line_attribute15
                         ) IN (
                            SELECT xis.interface_line_attribute1,
                                   xis.interface_line_attribute15
                              FROM xxconv.xxar_br_invoices_stg xis
                             WHERE xis.process_flag = 'X'
                               AND NVL (xis.link_to_line_attribute1,
                                        'NO VALUE'
                                       ) =
                                      NVL
                                         (get_tax_lines_rec.link_to_line_attribute1,
                                          'NO VALUE'
                                         )
                               AND NVL (xis.link_to_line_attribute15,
                                        'NO VALUE'
                                       ) =
                                      NVL
                                         (get_tax_lines_rec.link_to_line_attribute15,
                                          'NO VALUE'
                                         )
                               AND NVL (xis.tax_regime_code, 'NO VALUE') =
                                      NVL (get_tax_lines_rec.tax_regime_code,
                                           'NO VALUE'
                                          )
                               AND NVL (xis.tax_rate_code, 'NO VALUE') =
                                      NVL (get_tax_lines_rec.tax_rate_code,
                                           'NO VALUE'
                                          )
                               AND NVL (xis.tax, 'NO VALUE') =
                                       NVL (get_tax_lines_rec.tax, 'NO VALUE')
                               AND NVL (xis.tax_status_code, 'NO VALUE') =
                                      NVL (get_tax_lines_rec.tax_status_code,
                                           'NO VALUE'
                                          )
                               AND NVL (xis.tax_jurisdiction_code, 'NO VALUE') =
                                      NVL
                                         (get_tax_lines_rec.tax_jurisdiction_code,
                                          'NO VALUE'
                                         ));
               EXCEPTION
                  WHEN OTHERS
                  THEN
                     l_err := SQLERRM;
                     ROLLBACK TO tax;
               END;
            END LOOP;

            COMMIT;
         END IF;
      END LOOP;
   END group_tax_lines;

-- ========================
-- Procedure: update_status
-- =============================================================================
--   This procedure is used to update staging table with appropriate status
-- =============================================================================
   PROCEDURE update_status (
      pin_interface_txn_id   IN       NUMBER,
      piv_process_flag       IN       VARCHAR2 DEFAULT NULL,
      piv_err_type           IN       VARCHAR2,
      pov_return_status      OUT      VARCHAR2,
      pov_error_code         OUT      VARCHAR2,
      pov_error_message      OUT      VARCHAR2
   )
   IS
      PRAGMA AUTONOMOUS_TRANSACTION;
      l_log_ret_status   VARCHAR2 (50);
      l_log_err_msg      VARCHAR2 (2000);
   BEGIN
      --  print_log_message ('update_status procedure');
      UPDATE xxconv.xxar_br_invoices_stg
         SET process_flag = NVL (piv_process_flag, process_flag),
             run_sequence_id = g_new_run_seq_id,
             ERROR_TYPE = piv_err_type,
             last_update_date = SYSDATE,
             last_updated_by = g_last_updated_by,
             last_update_login = g_login_id,
			 request_id        = g_request_id -- Added for v1.13
       WHERE interface_txn_id = pin_interface_txn_id;

      COMMIT;
      pov_return_status := fnd_api.g_ret_sts_success;
   EXCEPTION
      WHEN OTHERS
      THEN
         pov_return_status := fnd_api.g_ret_sts_error;
         pov_error_code := 'ETN_BR_UPDATE_STATUS_ERROR';
         pov_error_message :=
               'Error : Error updating staging table for entity '
            || ' , record '
            || pin_interface_txn_id
            || SUBSTR (SQLERRM, 1, 150);
         g_retcode := 2;
         log_errors (pin_transaction_id           => pin_interface_txn_id,
                     piv_source_column_name       => 'PROCESS_FLAG',
                     piv_source_column_value      => piv_process_flag,
                     piv_error_type               => piv_err_type,
                     piv_error_code               => pov_error_code,
                     piv_error_message            => pov_error_message,
                     pov_return_status            => l_log_ret_status,
                     pov_error_msg                => l_log_err_msg
                    );
   END update_status;

--
-- ========================
-- Procedure: UPDATE_DIST_STATUS
-- =============================================================================
--   This procedure is used to update distribution staging table with appropriate status
-- =============================================================================
   PROCEDURE update_dist_status (
      pin_interface_txn_id   IN       NUMBER,
      piv_process_flag       IN       VARCHAR2 DEFAULT NULL,
      piv_err_type           IN       VARCHAR2,
      pov_return_status      OUT      VARCHAR2,
      pov_error_code         OUT      VARCHAR2,
      pov_error_message      OUT      VARCHAR2
   )
   IS
      PRAGMA AUTONOMOUS_TRANSACTION;
      l_log_ret_status   VARCHAR2 (50);
      l_log_err_msg      VARCHAR2 (2000);
   BEGIN
      --    print_log_message ('update_dist_status procedure');
      UPDATE xxconv.xxar_br_invoices_dist_stg
         SET process_flag = NVL (piv_process_flag, process_flag),
             ERROR_TYPE = piv_err_type,
             run_sequence_id = g_new_run_seq_id,
             last_update_date = SYSDATE,
             last_updated_by = g_last_updated_by,
             last_update_login = g_login_id,
			 request_id        = g_request_id -- Added for v1.13
       WHERE interface_txn_id = pin_interface_txn_id;

      COMMIT;
      pov_return_status := fnd_api.g_ret_sts_success;
   EXCEPTION
      WHEN OTHERS
      THEN
         pov_return_status := fnd_api.g_ret_sts_error;
         pov_error_code := 'ETN_AR_UPDATE_STATUS_ERROR';
         pov_error_message :=
               'Error : Error updating staging table for entity '
            || ' , record '
            || pin_interface_txn_id
            || SUBSTR (SQLERRM, 1, 150);
         g_retcode := 2;
         log_errors (pin_transaction_id           => pin_interface_txn_id,
                     piv_source_column_name       => 'PROCESS_FLAG',
                     piv_source_column_value      => piv_process_flag,
                     piv_error_type               => piv_err_type,
                     piv_error_code               => pov_error_code,
                     piv_error_message            => pov_error_message,
                     piv_source_table             => 'XXAR_BR_INVOICES_DIST_STG',
                     pov_return_status            => l_log_ret_status,
                     pov_error_msg                => l_log_err_msg
                    );
   END update_dist_status;

-- ========================
  -- Procedure: CREATE_INVOICE
  -- =============================================================================
  --   This procedure insert records in interface table
  -- =============================================================================
  --
   PROCEDURE create_invoice
   IS
      l_err_code         VARCHAR2 (40);
      l_err_msg          VARCHAR2 (2000);
      l_upd_ret_status   VARCHAR2 (50)   := NULL;
      l_log_ret_status   VARCHAR2 (50)   := NULL;
      l_log_err_msg      VARCHAR2 (2000);

      CURSOR create_inv_cur
      IS
         SELECT *
           FROM xxconv.xxar_br_invoices_stg
          WHERE process_flag = 'V' AND batch_id = g_new_batch_id;

      CURSOR create_dist_cur
      IS
         SELECT xds.*
           FROM xxconv.xxar_br_invoices_dist_stg xds,
                xxconv.xxar_br_invoices_stg xis
          WHERE xds.leg_customer_trx_id = xis.leg_customer_trx_id
            AND xds.leg_cust_trx_line_id = xis.leg_cust_trx_line_id
            AND NVL (xds.leg_account_class, 'A') <> 'REC'
            AND xds.process_flag = 'V'
            AND xds.batch_id = g_new_batch_id
            AND xis.process_flag = 'P'
         UNION
         SELECT xds.*
           FROM xxconv.xxar_br_invoices_dist_stg xds,
                xxconv.xxar_br_invoices_stg xis
          WHERE xds.leg_customer_trx_id = xis.leg_customer_trx_id
            AND xds.leg_account_class = 'REC'
            AND xds.process_flag = 'V'
            AND xds.batch_id = g_new_batch_id
            AND xis.process_flag = 'P'
         UNION
         --1.11
         SELECT xds.*
           FROM xxconv.xxar_br_invoices_dist_stg xds,
                xxconv.xxar_br_invoices_stg xis
          WHERE xds.leg_customer_trx_id = xis.leg_customer_trx_id
            AND xds.account_class = 'TAX'
            AND xis.line_type = 'TAX'
            AND xds.leg_account_class IS NULL
            AND xds.process_flag = 'V'
            AND xds.batch_id = g_new_batch_id
            AND xis.process_flag = 'P';
   BEGIN
      FOR create_inv_rec IN create_inv_cur
      LOOP
         BEGIN
            INSERT INTO apps.ra_interface_lines_all
                        (                               --interface_line_id ,
                         interface_line_context,
                         interface_line_attribute1,
                         --   interface_line_attribute2,
                         --Ver 1.5 Commented for DFF BR related Informaion
                         interface_line_attribute3,
                         interface_line_attribute4,
                         interface_line_attribute5,
                         interface_line_attribute6,
                         interface_line_attribute7,
                         --           interface_line_attribute8,
                          --          interface_line_attribute9,
                          --          interface_line_attribute10,
                           --         interface_line_attribute11,
                           --         interface_line_attribute12,
                           --         interface_line_attribute13,
                           --         interface_line_attribute14,
                         interface_line_attribute15,
                         link_to_line_context,
                         link_to_line_attribute1,
                         link_to_line_attribute2,
                         link_to_line_attribute3,
                         link_to_line_attribute4,
                         link_to_line_attribute5,
                         link_to_line_attribute6,
                         link_to_line_attribute7,
                         link_to_line_attribute8,
                         link_to_line_attribute9,
                         link_to_line_attribute10,
                         link_to_line_attribute11,
                         link_to_line_attribute12,
                         link_to_line_attribute13,
                         link_to_line_attribute14,
                         link_to_line_attribute15,
                         batch_source_name,
                         set_of_books_id,
                         memo_line_id,
                         line_type,
                         description,
                         currency_code,
                         amount,
                         cust_trx_type_name,
                         cust_trx_type_id,
                         term_name, term_id,
                         orig_system_bill_customer_ref,
                         orig_system_bill_customer_id,
                         orig_system_bill_address_ref,
                         orig_system_bill_address_id,
                         --orig_system_ship_customer_ref,
                         orig_system_ship_address_id,
                         orig_system_ship_customer_id,
                         conversion_type,
                         conversion_rate,
                         conversion_date,
                         purchase_order,
                         reason_code,
                         comments,
                         header_attribute_category,
                         header_attribute1
                                          --, header_attribute2
                                          --, header_attribute3
            ,
                            --         header_attribute3,
                           --          header_attribute4,
                          --           header_attribute5,
                          --           header_attribute6,
                         --            header_attribute7,
                                     --chetan --leg_br_or_openreceipt_num
                         header_attribute8,
                         header_attribute9
                                          --, header_attribute10
                                          --, header_attribute11
                                          --, header_attribute12
                                          --, header_attribute13
                                          --, header_attribute14
                                          --, header_attribute15
            ,
                         attribute_category,
                         attribute1,
                         attribute2,
                         attribute3,
                         attribute4,
                         attribute5,
                         attribute6,
                         attribute7,
                         attribute8,
                         attribute9,
                         attribute10,
                         attribute11,
                         attribute12,
                         attribute13,
                         attribute14,
                         attribute15,
                         header_gdf_attr_category,
                         header_gdf_attribute1,
                         header_gdf_attribute2,
                         header_gdf_attribute3,
                         header_gdf_attribute4,
                         header_gdf_attribute5,
                         header_gdf_attribute6,
                         header_gdf_attribute7,
                         header_gdf_attribute8,
                         header_gdf_attribute9,
                         header_gdf_attribute10,
                         header_gdf_attribute11,
                         header_gdf_attribute12,
                         header_gdf_attribute13,
                         header_gdf_attribute14,
                         header_gdf_attribute15,
                         header_gdf_attribute16,
                         header_gdf_attribute17,
                         header_gdf_attribute18,
                         header_gdf_attribute19,
                         header_gdf_attribute20,
                         header_gdf_attribute21,
                         header_gdf_attribute22,
                         header_gdf_attribute23,
                         header_gdf_attribute24,
                         header_gdf_attribute25,
                         header_gdf_attribute26,
                         header_gdf_attribute27,
                         header_gdf_attribute28,
                         header_gdf_attribute29,
                         header_gdf_attribute30,
                         line_gdf_attr_category,
                         line_gdf_attribute1,
                         line_gdf_attribute2,
                         line_gdf_attribute3,
                         line_gdf_attribute4,
                         line_gdf_attribute5,
                         line_gdf_attribute6,
                         line_gdf_attribute7,
                         line_gdf_attribute8,
                         line_gdf_attribute9,
                         line_gdf_attribute10,
                         line_gdf_attribute11,
                         line_gdf_attribute12,
                         line_gdf_attribute13,
                         line_gdf_attribute14,
                         line_gdf_attribute15,
                         line_gdf_attribute16,
                         line_gdf_attribute17,
                         line_gdf_attribute18,
                         line_gdf_attribute19,
                         line_gdf_attribute20,
                         trx_date, gl_date,
                         trx_number,
                         line_number,
                         tax_code,
                         tax_regime_code,
                         tax_rate_code, tax,
                         tax_status_code,
                         tax_jurisdiction_code,
                         amount_includes_tax_flag,
                         taxable_flag,
                         sales_order_date,
                         sales_order,
                         uom_name,
                         ussgl_transaction_code_context,
                         internal_notes,
                         ship_date_actual,
                         fob_point, ship_via,
                         waybill_number,
                         sales_order_line,
                         sales_order_source,
                         sales_order_revision,
                         purchase_order_revision,
                          --Ver1.7 changes start
                          /*
                                      purchase_order_date,
                          agreement_name,
                                      agreement_id,
                          accounting_rule_id
                         ,rule_start_date
                         ,invoicing_rule_id
                         ,
                         */
                          --Ver1.7 changes end
                         quantity,
                         quantity_ordered,
                         unit_selling_price,
                         unit_standard_price,
                         org_id, creation_date,
                         created_by, last_update_date,
                         last_updated_by, last_update_login,
                         tax_rate              ---1.4 Added by Rohit D for FOT
                        )
                 VALUES (                   --ra_customer_trx_lines_s.NEXTVAL,
                         create_inv_rec.interface_line_context,
                         create_inv_rec.interface_line_attribute1,
                         --  create_inv_rec.interface_line_attribute2,
                          --Ver 1.1 DFF BR related Informaion start
                         create_inv_rec.leg_br_openrec_ledger,
                                   --create_inv_rec.interface_line_attribute3,
                         create_inv_rec.leg_br_openrec_org,
                                   --create_inv_rec.interface_line_attribute4,
                         SUBSTRB(create_inv_rec.leg_br_or_openreceipt_num,1,150),
                                   --create_inv_rec.interface_line_attribute5, added substr for v1.15
                         TO_CHAR (create_inv_rec.leg_br_or_rec_maturity_date,
                                  'DD-MON-YY'
                                 ),
                                   --create_inv_rec.interface_line_attribute6,
                         TO_CHAR (create_inv_rec.leg_br_or_rec_issue_date,
                                  'DD-MON-YY'
                                 ),
                                   --create_inv_rec.interface_line_attribute7,
                         --Ver 1.1 DFF BR related Informaion end
                                   --          create_inv_rec.interface_line_attribute8,
                                 --            create_inv_rec.interface_line_attribute9,
                                --             create_inv_rec.interface_line_attribute10,
                               --              create_inv_rec.interface_line_attribute11,
                              --               create_inv_rec.interface_line_attribute12,
                                --             create_inv_rec.interface_line_attribute13,
                              --               create_inv_rec.interface_line_attribute14,
                         create_inv_rec.interface_line_attribute15,
                         create_inv_rec.link_to_line_context,
                         create_inv_rec.link_to_line_attribute1,
                         create_inv_rec.link_to_line_attribute2,
                         create_inv_rec.link_to_line_attribute3,
                         create_inv_rec.link_to_line_attribute4,
                         create_inv_rec.link_to_line_attribute5,
                         create_inv_rec.link_to_line_attribute6,
                         create_inv_rec.link_to_line_attribute7,
                         create_inv_rec.link_to_line_attribute8,
                         create_inv_rec.link_to_line_attribute9,
                         create_inv_rec.link_to_line_attribute10,
                         create_inv_rec.link_to_line_attribute11,
                         create_inv_rec.link_to_line_attribute12,
                         create_inv_rec.link_to_line_attribute13,
                         create_inv_rec.link_to_line_attribute14,
                         create_inv_rec.link_to_line_attribute15,
                         create_inv_rec.batch_source_name,
                         create_inv_rec.set_of_books_id,
                         create_inv_rec.memo_line_id,
                         create_inv_rec.line_type,
                         create_inv_rec.description,
                         create_inv_rec.currency_code,
                         create_inv_rec.line_amount,
                         create_inv_rec.cust_trx_type_name,
                         create_inv_rec.transaction_type_id,
                         create_inv_rec.term_name, create_inv_rec.term_id,
                         create_inv_rec.system_bill_customer_ref,
                         create_inv_rec.system_bill_customer_id,
                         create_inv_rec.system_bill_address_ref,
                         create_inv_rec.system_bill_address_id,
                         create_inv_rec.system_ship_address_id,
                         DECODE (create_inv_rec.system_ship_address_id,
                                 NULL, NULL,
                                 create_inv_rec.system_bill_customer_id
                                ),
                         create_inv_rec.conversion_type,
                         create_inv_rec.conversion_rate,
                         create_inv_rec.conversion_date,
                         create_inv_rec.purchase_order,
                         create_inv_rec.reason_code,
                         -- create_inv_rec.comments, --commented for v1.14						
 			 create_inv_rec.leg_br_or_openreceipt_num, --added for v1.14 Br numbed passed in comments
                         create_inv_rec.header_attribute_category,
                         create_inv_rec.header_attribute1,
                                    --ver 1.5 changes start
                                    --create_inv_rec.header_attribute2,
                           /*        create_inv_rec.leg_br_openrec_ledger,
                         --            create_inv_rec.leg_br_openrec_org,
                         --           create_inv_rec.leg_br_or_openreceipt_num,
                                    TO_CHAR (create_inv_rec.leg_br_or_rec_maturity_date,
                                             'DD-MON-YY'
                                            ),
                                    TO_CHAR (create_inv_rec.leg_br_or_rec_issue_date,
                                             'DD-MON-YY'
                                            ),*/
                         create_inv_rec.header_attribute8,
                         create_inv_rec.header_attribute9,
                         --create_inv_rec.header_attribute10,
                         --create_inv_rec.header_attribute11,
                         --create_inv_rec.header_attribute12,
                         --create_inv_rec.header_attribute13,
                         --create_inv_rec.header_attribute14,
                         --create_inv_rec.header_attribute15,
                         --ver 1.5 changes end
                         create_inv_rec.attribute_category,
                         create_inv_rec.attribute1,
                         create_inv_rec.attribute2,
                         create_inv_rec.attribute3,
                         create_inv_rec.attribute4,
                         create_inv_rec.attribute5,
                         create_inv_rec.attribute6,
                         create_inv_rec.attribute7,
                         create_inv_rec.attribute8,
                         create_inv_rec.attribute9,
                         create_inv_rec.attribute10,
                         create_inv_rec.attribute11,
                         create_inv_rec.attribute12,
                         create_inv_rec.attribute13,
                         create_inv_rec.attribute14,
                         create_inv_rec.attribute15,
                         create_inv_rec.header_gdf_attr_category,
                         create_inv_rec.header_gdf_attribute1,
                         create_inv_rec.header_gdf_attribute2,
                         create_inv_rec.header_gdf_attribute3,
                         create_inv_rec.header_gdf_attribute4,
                         create_inv_rec.header_gdf_attribute5,
                         create_inv_rec.header_gdf_attribute6,
                         create_inv_rec.header_gdf_attribute7,
                         create_inv_rec.header_gdf_attribute8,
                         create_inv_rec.header_gdf_attribute9,
                         create_inv_rec.header_gdf_attribute10,
                         create_inv_rec.header_gdf_attribute11,
                         create_inv_rec.header_gdf_attribute12,
                         create_inv_rec.header_gdf_attribute13,
                         create_inv_rec.header_gdf_attribute14,
                         create_inv_rec.header_gdf_attribute15,
                         create_inv_rec.header_gdf_attribute16,
                         create_inv_rec.header_gdf_attribute17,
                         create_inv_rec.header_gdf_attribute18,
                         create_inv_rec.header_gdf_attribute19,
                         create_inv_rec.header_gdf_attribute20,
                         create_inv_rec.header_gdf_attribute21,
                         create_inv_rec.header_gdf_attribute22,
                         create_inv_rec.header_gdf_attribute23,
                         create_inv_rec.header_gdf_attribute24,
                         create_inv_rec.header_gdf_attribute25,
                         create_inv_rec.header_gdf_attribute26,
                         create_inv_rec.header_gdf_attribute27,
                         create_inv_rec.header_gdf_attribute28,
                         create_inv_rec.header_gdf_attribute29,
                         create_inv_rec.header_gdf_attribute30,
                         create_inv_rec.line_gdf_attr_category,
                         create_inv_rec.line_gdf_attribute1,
                         create_inv_rec.line_gdf_attribute2,
                         create_inv_rec.line_gdf_attribute3,
                         create_inv_rec.line_gdf_attribute4,
                         create_inv_rec.line_gdf_attribute5,
                         create_inv_rec.line_gdf_attribute6,
                         create_inv_rec.line_gdf_attribute7,
                         create_inv_rec.line_gdf_attribute8,
                         create_inv_rec.line_gdf_attribute9,
                         create_inv_rec.line_gdf_attribute10,
                         create_inv_rec.line_gdf_attribute11,
                         create_inv_rec.line_gdf_attribute12,
                         create_inv_rec.line_gdf_attribute13,
                         create_inv_rec.line_gdf_attribute14,
                         create_inv_rec.line_gdf_attribute15,
                         create_inv_rec.line_gdf_attribute16,
                         create_inv_rec.line_gdf_attribute17,
                         create_inv_rec.line_gdf_attribute18,
                         create_inv_rec.line_gdf_attribute19,
                         create_inv_rec.line_gdf_attribute20,
                         create_inv_rec.trx_date, create_inv_rec.gl_date,
                         create_inv_rec.trx_number,
                         create_inv_rec.line_number,
                         create_inv_rec.tax_code,
                         create_inv_rec.tax_regime_code,
                         create_inv_rec.tax_rate_code, create_inv_rec.tax,
                         create_inv_rec.tax_status_code,
                         create_inv_rec.tax_jurisdiction_code,
                         create_inv_rec.amount_includes_tax_flag,
                         create_inv_rec.taxable_flag,
                         create_inv_rec.sales_order_date,
                         create_inv_rec.sales_order,
                         create_inv_rec.uom_name,
                         create_inv_rec.ussgl_transaction_code_context,
                         create_inv_rec.internal_notes,
                         create_inv_rec.ship_date_actual,
                         create_inv_rec.fob_point, create_inv_rec.ship_via,
                         create_inv_rec.waybill_number,
                         create_inv_rec.sales_order_line,
                         create_inv_rec.sales_order_source,
                         create_inv_rec.sales_order_revision,
                         create_inv_rec.purchase_order_revision,
                          --ver1.7 changes start
                          /*                         create_inv_rec.purchase_order_date,
                          create_inv_rec.agreement_name,
                                      create_inv_rec.agreement_id,
                          create_inv_rec.agreement_id
                         ,create_inv_rec.purchase_order_date
                         ,create_inv_rec.invoicing_rule_id
                         ,
                         */
                          --ver1.7 changes end
                         create_inv_rec.quantity,
                         create_inv_rec.quantity_ordered,
                         create_inv_rec.unit_selling_price,
                         create_inv_rec.unit_standard_price,
                         create_inv_rec.org_id, SYSDATE,
                         apps.fnd_global.user_id, SYSDATE,
                         apps.fnd_global.user_id, apps.fnd_global.login_id,
                         create_inv_rec.tax_rate
                        ---1.4 Added by Rohit D for FOT
                        );

            update_status
                     (pin_interface_txn_id      => create_inv_rec.interface_txn_id,
                      piv_process_flag          => 'P',
                      piv_err_type              => NULL,
                      pov_return_status         => l_upd_ret_status
                                                                   -- OUT
            ,
                      pov_error_code            => l_err_code
                                                             -- OUT
            ,
                      pov_error_message         => l_err_msg
                     -- OUT
                     );
         EXCEPTION
            WHEN OTHERS
            THEN
               g_retcode := 1;
               l_err_code := 'ETN_BR_CREATE_EXCEPTION';
               l_err_msg :=
                     'Error : Exception in create_invoice Procedure for invoice lines. '
                  || SUBSTR (SQLERRM, 1, 150);
               log_errors
                  (pin_transaction_id           => create_inv_rec.interface_txn_id,
                   piv_source_column_name       => 'LEGACY_CUSTOMER_TRX_ID',
                   piv_source_column_value      => create_inv_rec.leg_customer_trx_id,
                   piv_error_type               => 'ERR_INT',
                   piv_error_code               => l_err_code,
                   piv_error_message            => l_err_msg,
                   pov_return_status            => l_log_ret_status,
                   pov_error_msg                => l_log_err_msg
                  );
               update_status
                     (pin_interface_txn_id      => create_inv_rec.interface_txn_id,
                      piv_process_flag          => 'E',
                      piv_err_type              => 'ERR_INT',
                      pov_return_status         => l_upd_ret_status
                                                                   -- OUT
               ,
                      pov_error_code            => l_err_code
                                                             -- OUT
               ,
                      pov_error_message         => l_err_msg
                     -- OUT
                     );
         END;
      END LOOP;

      FOR r_createline_err_rec IN (SELECT DISTINCT xis.leg_customer_trx_id,
                                                   xis.leg_trx_number
                                              FROM xxconv.xxar_br_invoices_stg xis
                                             WHERE xis.process_flag IN ('E')
                                               AND xis.batch_id =
                                                                g_new_batch_id
                                               AND NVL (xis.ERROR_TYPE, 'A') =
                                                                     'ERR_INT')
      LOOP
         g_retcode := 1;

         UPDATE xxconv.xxar_br_invoices_stg
            SET process_flag = 'E',
                ERROR_TYPE = 'ERR_INT',
                run_sequence_id = g_new_run_seq_id,
                last_update_date = SYSDATE,
                last_updated_by = g_last_updated_by,
                last_update_login = g_login_id,
				request_id        = g_request_id -- Added for v1.13
          WHERE leg_customer_trx_id = r_createline_err_rec.leg_customer_trx_id
            AND batch_id = g_new_batch_id;

         l_err_code := 'ETN_INVOICE_ERROR';
         l_err_msg :=
            'Error : Erroring out remaining lines since one of the lines is in error while inserting in ra_interface_lines_all';
         print_log_message (   'For legacy transaction number: '
                            || r_createline_err_rec.leg_trx_number
                           );
         print_log_message (l_err_msg);
         log_errors
              (
               --   pin_transaction_id           =>  r_dist_err_rec.interface_txn_id
               piv_error_type               => 'ERR_INT',
               piv_source_column_name       => 'TRX_NUMBER',
               piv_source_column_value      => r_createline_err_rec.leg_trx_number,
               piv_error_code               => l_err_code,
               piv_error_message            => l_err_msg,
               pov_return_status            => l_log_ret_status,
               pov_error_msg                => l_log_err_msg
              );
      END LOOP;

      COMMIT;

      FOR r_rila_err_rec IN (SELECT DISTINCT xis.interface_line_attribute15
                                        FROM xxconv.xxar_br_invoices_stg xis
                                       WHERE xis.process_flag IN ('E')
                                         AND xis.batch_id = g_new_batch_id
                                         AND NVL (xis.ERROR_TYPE, 'A') =
                                                                     'ERR_INT')
      LOOP
         DELETE FROM ra_interface_lines_all
               WHERE interface_line_attribute15 =
                                    r_rila_err_rec.interface_line_attribute15;
      END LOOP;

      COMMIT;

      FOR create_dist_rec IN create_dist_cur
      LOOP
         BEGIN
            INSERT INTO apps.ra_interface_distributions_all
                        (                               --interface_line_id ,
                         amount,
                         code_combination_id,
                         org_id,
                         segment1,
                         segment2,
                         segment3,
                         segment4,
                         segment5,
                         segment6,
                         segment7,
                         segment8,
                         segment9,
                         segment10,
                         interface_line_context,
                         interface_line_attribute1,
                         /*--Ver 1.5 Commented for DFF rationalization start
                         interface_line_attribute2,
                                           interface_line_attribute3,
                                           interface_line_attribute4,
                                           interface_line_attribute5,
                                           interface_line_attribute6,
                                           interface_line_attribute7,
                                           interface_line_attribute8,
                                           interface_line_attribute9,
                                           interface_line_attribute10,
                                           interface_line_attribute11,
                                           interface_line_attribute12,
                                           interface_line_attribute13,
                                           interface_line_attribute14,
                         */
                         --Ver 1.5 Commented for DFF rationalization end
                         interface_line_attribute15,
                         account_class,
                         PERCENT
                                --,interface_line_id
            ,            creation_date,
                         created_by, last_update_date,
                         last_updated_by, last_update_login
                        )
                 VALUES (                   --ra_customer_trx_lines_s.currval,
                         create_dist_rec.accounted_amount,
                         create_dist_rec.code_combination_id,
                         create_dist_rec.org_id,
                         create_dist_rec.dist_segment1,
                         create_dist_rec.dist_segment2,
                         create_dist_rec.dist_segment3,
                         create_dist_rec.dist_segment4,
                         create_dist_rec.dist_segment5,
                         create_dist_rec.dist_segment6,
                         create_dist_rec.dist_segment7,
                         create_dist_rec.dist_segment8,
                         create_dist_rec.dist_segment9,
                         create_dist_rec.dist_segment10,
                         create_dist_rec.interface_line_context,
                         create_dist_rec.interface_line_attribute1,
                         /*--Ver 1.5 Commented for DFF rationalization start
                         create_dist_rec.interface_line_attribute2,
                         create_dist_rec.interface_line_attribute3,
                         create_dist_rec.interface_line_attribute4,
                         create_dist_rec.interface_line_attribute5,
                         create_dist_rec.interface_line_attribute6,
                         create_dist_rec.interface_line_attribute7,
                         create_dist_rec.interface_line_attribute8,
                         create_dist_rec.interface_line_attribute9,
                         create_dist_rec.interface_line_attribute10,
                         create_dist_rec.interface_line_attribute11,
                         create_dist_rec.interface_line_attribute12,
                         create_dist_rec.interface_line_attribute13,
                         create_dist_rec.interface_line_attribute14,
                         */--Ver 1.5 Commented for DFF rationalization end
                         create_dist_rec.interface_line_attribute15,
                         create_dist_rec.account_class,
                         create_dist_rec.PERCENT
                                                --,xxar_interface_line_s.currval
            ,            SYSDATE,
                         apps.fnd_global.user_id, SYSDATE,
                         apps.fnd_global.user_id, apps.fnd_global.login_id
                        );

            update_dist_status
                    (pin_interface_txn_id      => create_dist_rec.interface_txn_id,
                     piv_process_flag          => 'P',
                     piv_err_type              => NULL,
                     pov_return_status         => l_upd_ret_status,
                     pov_error_code            => l_err_code,
                     pov_error_message         => l_err_msg
                    );
         EXCEPTION
            WHEN OTHERS
            THEN
               g_retcode := 1;
               l_err_code := 'ETN_AR_CREATE_EXCEPTION';
               l_err_msg :=
                     'Error : Exception in create_invoice Procedure for distributions. '
                  || SUBSTR (SQLERRM, 1, 150);
               log_errors (
                           -- pin_interface_txn_id      => create_dist_rec.interface_txn_id,
                           -- , piv_source_column_name     =>  'Legacy link_to_customer_trx_line_id'
                           --  , piv_source_column_value    =>  val_inv_det_rec(l_line_cnt).leg_link_to_cust_trx_line_id
                           piv_error_type         => 'ERR_INT',
                           piv_error_code         => l_err_code,
                           piv_error_message      => l_err_msg,
                           piv_source_table       => 'XXAR_BR_INVOICES_DIST_STG',
                           pov_return_status      => l_log_ret_status,
                           pov_error_msg          => l_log_err_msg
                          );
               update_dist_status
                    (pin_interface_txn_id      => create_dist_rec.interface_txn_id,
                     piv_process_flag          => 'E',
                     piv_err_type              => 'ERR_INT',
                     pov_return_status         => l_upd_ret_status,
                     pov_error_code            => l_err_code,
                     pov_error_message         => l_err_msg
                    );
         END;
      END LOOP;

      FOR r_createdist_err_rec IN (SELECT DISTINCT xds.leg_customer_trx_id,
                                                   xds.process_flag
                                              FROM xxconv.xxar_br_invoices_dist_stg xds
                                             WHERE xds.process_flag IN
                                                                   ('V', 'E')
                                               AND xds.batch_id =
                                                                g_new_batch_id
                                               AND DECODE
                                                        (xds.process_flag,
                                                         'E', NVL
                                                              (xds.ERROR_TYPE,
                                                               'A'
                                                              ),
                                                         'ERR_INT'
                                                        ) = 'ERR_INT')
      --AND NVL(xds.leg_account_class, 'A') <> 'ROUND') --performance
      LOOP
         UPDATE xxconv.xxar_br_invoices_dist_stg
            SET process_flag = 'E',
                ERROR_TYPE = 'ERR_INT',
                run_sequence_id = g_new_run_seq_id,
                last_update_date = SYSDATE,
                last_updated_by = g_last_updated_by,
                last_update_login = g_login_id,
				request_id        = g_request_id -- Added for v1.13
          WHERE leg_customer_trx_id = r_createdist_err_rec.leg_customer_trx_id
            AND batch_id = g_new_batch_id;

         g_retcode := 1;

         DELETE FROM ra_interface_distributions_all
               WHERE interface_line_attribute14 =
                                      r_createdist_err_rec.leg_customer_trx_id;

         DELETE FROM ra_interface_lines_all
               WHERE interface_line_attribute14 =
                                      r_createdist_err_rec.leg_customer_trx_id;

         IF r_createdist_err_rec.process_flag <> 'V'
         THEN
            UPDATE xxconv.xxar_br_invoices_stg
               SET process_flag = 'E',
                   ERROR_TYPE = 'ERR_INT',
                   run_sequence_id = g_new_run_seq_id,
                   last_update_date = SYSDATE,
                   last_updated_by = g_last_updated_by,
                   last_update_login = g_login_id,
				   request_id        = g_request_id -- Added for v1.13
             WHERE leg_customer_trx_id =
                                      r_createdist_err_rec.leg_customer_trx_id
               AND batch_id = g_new_batch_id;

            l_err_code := 'ETN_DISTRIBUTION_ERROR';
            l_err_msg :=
               'Error : Erroring out invoice lines since distribution is in error while inserting in ra_interface_distributions_all';
            -- print_log_message ('For legacy transaction number: '||r_createdist_err_rec.leg_trx_number);
            print_log_message (l_err_msg);
            log_errors
               (
                --   pin_transaction_id           =>  r_dist_err_rec.interface_txn_id
                piv_error_type               => 'ERR_INT',
                piv_source_column_name       => 'LEGACY_CUSTOMER_TRX_ID',
                piv_source_column_value      => r_createdist_err_rec.leg_customer_trx_id,
                piv_error_code               => l_err_code,
                piv_error_message            => l_err_msg,
                pov_return_status            => l_log_ret_status,
                pov_error_msg                => l_log_err_msg
               );
         END IF;

         l_err_code := 'ETN_INVOICE_ERROR';

         IF r_createdist_err_rec.process_flag = 'V'
         THEN
            l_err_msg :=
               'Error : Erroring distribution since corresponding invoice line in error while inserting in ra_interface_lines_all ';
         ELSE
            l_err_msg :=
               'Error : Erroring distribution since another related distribution in error while inserting in ra_interface_distributions_all';
         END IF;

         -- print_log_message ('For legacy transaction number: '||r_createdist_err_rec.leg_trx_number);
         print_log_message (l_err_msg);
         log_errors
            (
             --   pin_transaction_id           =>  r_dist_err_rec.interface_txn_id
             piv_error_type               => 'ERR_INT',
             piv_source_column_name       => 'LEGACY_CUSTOMER_TRX_ID',
             piv_source_column_value      => r_createdist_err_rec.leg_customer_trx_id,
             piv_source_table             => 'XXAR_BR_INVOICES_STG',
             piv_error_code               => l_err_code,
             piv_error_message            => l_err_msg,
             pov_return_status            => l_log_ret_status,
             pov_error_msg                => l_log_err_msg
            );
      END LOOP;

      COMMIT;

      FOR r_rida_err_rec IN (SELECT DISTINCT xds.interface_line_attribute15
                                        FROM xxconv.xxar_br_invoices_dist_stg xds
                                       WHERE xds.process_flag = 'E'
                                         AND xds.batch_id = g_new_batch_id
                                         AND NVL (xds.ERROR_TYPE, 'A') =
                                                                     'ERR_INT')
      --AND NVL(xds.leg_account_class, 'A') <> 'ROUND') --performance
      LOOP
         DELETE FROM ra_interface_distributions_all
               WHERE interface_line_attribute15 =
                                    r_rida_err_rec.interface_line_attribute15;

         DELETE FROM ra_interface_lines_all
               WHERE interface_line_attribute15 =
                                    r_rida_err_rec.interface_line_attribute15;
      END LOOP;

      COMMIT;
   EXCEPTION
      WHEN OTHERS
      THEN
         g_retcode := 2;
         l_err_code := 'ETN_BR_PROCEDURE_EXCEPTION';
         l_err_msg :=
               'Error : Exception in create_invoice Procedure. '
            || SUBSTR (SQLERRM, 1, 150);
         log_errors (
                     -- pin_transaction_id           =>  pin_trx_id
                     -- , piv_source_column_name     =>  'Legacy link_to_customer_trx_line_id'
                     --  , piv_source_column_value    =>  val_inv_det_rec(l_line_cnt).leg_link_to_cust_trx_line_id
                     piv_error_type         => 'ERR_INT',
                     piv_error_code         => l_err_code,
                     piv_error_message      => l_err_msg,
                     pov_return_status      => l_log_ret_status,
                     pov_error_msg          => l_log_err_msg
                    );
   END create_invoice;

 --
-- ========================
-- Procedure: UPDATE_DUEDATE
-- =============================================================================
--   This procedure update due date for invoices where due date in 11i is
--   is different from R12
-- =============================================================================
--
   PROCEDURE update_duedate (
      pov_errbuf     OUT NOCOPY      VARCHAR2,
      pon_retcode    OUT NOCOPY      NUMBER,
      piv_dummy1     IN              VARCHAR2,
      pin_batch_id   IN              NUMBER
   )
   IS
      l_err_msg   VARCHAR2 (2000);
	  l_due_count NUMBER; --added for v1.13
	  
	    -- v1.13 changes
    CURSOR fetch_pay_schdle_cur IS
      SELECT apsa2.payment_schedule_id, xis.leg_due_date
        FROM xxconv.xxar_br_invoices_stg        xis,
             ar_payment_schedules_all apsa2,
             ra_customer_trx_all      rcta
       WHERE 1 = 1
         AND rcta.customer_trx_id = apsa2.customer_trx_id
         AND rcta.trx_number = xis.leg_trx_number
         AND rcta.org_id = xis.org_id
         AND rcta.cust_trx_type_id = xis.transaction_type_id
         AND xis.process_flag = 'C'
         AND xis.batch_id = pin_batch_id
         AND rcta.interface_header_attribute15 = xis.interface_line_attribute15
         AND apsa2.due_date <> xis.leg_due_date;
		 
   BEGIN
   l_due_count := 0; --added for v1.13
     /* UPDATE ar_payment_schedules_all apsa
         SET due_date =
                (SELECT MAX (xis.leg_due_date)
                   FROM xxconv.xxar_br_invoices_stg xis,
                        ar_payment_schedules_all apsa1,
                        ra_customer_trx_all rcta
                  WHERE apsa1.payment_schedule_id = apsa.payment_schedule_id
                    AND apsa1.customer_trx_id = rcta.customer_trx_id
                    AND rcta.trx_number = xis.leg_trx_number
                    AND rcta.org_id = xis.org_id
                    AND rcta.cust_trx_type_id = xis.transaction_type_id
                    AND xis.process_flag = 'C'
                    AND xis.batch_id = pin_batch_id
                   -- AND xis.leg_cust_trx_type_name NOT LIKE '%OKS%' --v1.11
                   ),
             last_update_date = SYSDATE,
             last_updated_by = g_last_updated_by,
             last_update_login = g_login_id
       WHERE payment_schedule_id IN (
                SELECT apsa2.payment_schedule_id
                  FROM xxconv.xxar_br_invoices_stg xis,
                       ar_payment_schedules_all apsa2,
                       ra_customer_trx_all rcta
                 WHERE apsa2.payment_schedule_id = apsa.payment_schedule_id
                   AND apsa2.customer_trx_id = rcta.customer_trx_id
                   AND rcta.trx_number = xis.leg_trx_number
                   AND rcta.org_id = xis.org_id
                   AND rcta.cust_trx_type_id = xis.transaction_type_id
                   AND xis.process_flag = 'C'
                   AND xis.batch_id = pin_batch_id
                   --AND xis.leg_cust_trx_type_name NOT LIKE '%OKS%' 1.11
                   ); */
				   
		FOR fetch_pay_schdle_rec IN fetch_pay_schdle_cur LOOP
    
				  UPDATE ar_payment_schedules_all
					 SET due_date          = fetch_pay_schdle_rec.leg_due_date,
						 last_update_date  = g_sysdate,
						 last_updated_by   = g_last_updated_by,
						 last_update_login = g_login_id
				  WHERE payment_schedule_id = fetch_pay_schdle_rec.payment_schedule_id;
				   
				   l_due_count := l_due_count+1;
    
		END LOOP;

 print_log_message ('+ Updated Due Date on Recs = ' || l_due_count);
           Commit;

     -- print_log_message
       --     (   '+ Updated Due Date on Recs transaction type not like OKS  = '
        --     || SQL%ROWCOUNT
         --   );
--v1.11commenting as OKS is getting handled through service contracts
      /*UPDATE ar_payment_schedules_all apsa
         SET due_date =
                (SELECT MAX (xis.leg_due_date)
                   FROM xxconv.xxar_br_invoices_stg xis,
                        ar_payment_schedules_all apsa1,
                        ra_customer_trx_all rcta
                  WHERE apsa1.payment_schedule_id = apsa.payment_schedule_id
                    AND apsa1.customer_trx_id = rcta.customer_trx_id
                    AND rcta.trx_number = xis.leg_trx_number
                    AND rcta.org_id = xis.org_id
                    AND rcta.cust_trx_type_id = xis.transaction_type_id
                    AND xis.process_flag = 'C'
                    AND xis.batch_id = pin_batch_id
                    AND xis.leg_cust_trx_type_name LIKE '%OKS%'
                    AND apsa1.due_date =
                           (SELECT MAX (apsa.due_date)
                              FROM ar_payment_schedules_all apsa
                             WHERE apsa.customer_trx_id =
                                                         apsa1.customer_trx_id)),
             last_update_date = SYSDATE,
             last_updated_by = g_last_updated_by,
             last_update_login = g_login_id
       WHERE payment_schedule_id IN (
                SELECT apsa2.payment_schedule_id
                  FROM xxconv.xxar_br_invoices_stg xis,
                       ar_payment_schedules_all apsa2,
                       ra_customer_trx_all rcta
                 WHERE apsa2.payment_schedule_id = apsa.payment_schedule_id
                   AND apsa2.customer_trx_id = rcta.customer_trx_id
                   AND rcta.trx_number = xis.leg_trx_number
                   AND rcta.org_id = xis.org_id
                   AND rcta.cust_trx_type_id = xis.transaction_type_id
                   AND xis.process_flag = 'C'
                   AND xis.batch_id = pin_batch_id
                   AND xis.leg_cust_trx_type_name LIKE '%OKS%'
                   AND apsa2.due_date =
                          (SELECT MAX (apsa3.due_date)
                             FROM ar_payment_schedules_all apsa3
                            WHERE apsa2.customer_trx_id =
                                                         apsa3.customer_trx_id));
          Commit;*/

   --   print_log_message ('+ Updated Due Date on Recs = ' || SQL%ROWCOUNT);
      pov_errbuf := g_errbuff;
      pon_retcode := g_retcode;
      print_log_message
                  (   '-   PROCEDURE : Update Due Date Program for batch id: '
                   || pin_batch_id
                  );
      print_log_message (   'Update Due Date Ends at: '
                         || TO_CHAR (g_sysdate, 'DD-MON-YYYY HH24:MI:SS')
                        );
      print_log_message ('---------------------------------------------');
   EXCEPTION
      WHEN OTHERS
      THEN
         pov_errbuf :=
               'Error : Main program procedure encounter error. '
            || SUBSTR (SQLERRM, 1, 150);
         pon_retcode := 2;
         print_log_message (   'In Due Date update when others'
                            || SUBSTR (SQLERRM, 1, 150)
                           );
   END update_duedate;

-- ========================
-- Procedure: BR  maturity date ,ledger , due date Program
-- =============================================================================
   PROCEDURE update_br (
      pov_errbuf     OUT NOCOPY      VARCHAR2,
      pon_retcode    OUT NOCOPY      NUMBER,
      p_request_id   IN              NUMBER
   )
   IS
      ---defining cursors
      CURSOR cur_legbr_details (p_req_id NUMBER)
      IS
         SELECT DISTINCT rctl.customer_trx_id,
                         rctl.interface_line_attribute3 br_plant,
                         rctl.interface_line_attribute5 br_trx,
                         TO_DATE
                               (rctl.interface_line_attribute6,
                                'DD-MON-RRRR'
                               ) maturity_date,
                         TO_DATE (rctl.interface_line_attribute7,
                                  'DD-MON-RRRR'
                                 ) issue_date,
                         rct.trx_number, rct.org_id ,
             leg_br.leg_customer_number
                    FROM ra_customer_trx_all rct,
                         ra_customer_trx_lines_all rctl,
                         (SELECT DISTINCT leg_customer_number ,leg_trx_number, org_id
                                     FROM xxconv.xxar_br_invoices_stg xxbr
                                    WHERE xxbr.leg_request_id =
                                                          p_req_id
                                                                  /*53046254*/
                                      AND xxbr.leg_process_flag <> 'C'  ) leg_br
                   WHERE rct.trx_number = leg_br.leg_trx_number --'CKIV806848'
                     AND rct.org_id = leg_br.org_id
                     AND rctl.customer_trx_id = rct.customer_trx_id;

      l_err_msg          VARCHAR2 (2000);
      l_newbr_trx_id     NUMBER                                := 0;
      l_newbr_number     ra_customer_trx_all.trx_number%TYPE;
      l_br_dff           VARCHAR2 (150)                        := NULL;
	  --l_leg_br_num     VARCHAR2 (150)                        := NULL;  --v1.12
	  l_leg_br_num       VARCHAR2 (1760)                       := NULL;  --v1.12
      l_br_plant         VARCHAR2 (150)                        := NULL;
      l_br_maturity_dt   DATE;
      l_br_issue_dt      DATE;
   BEGIN
      --fetching leg request id data
      -- Update Maturity date and issue date of BR
      -- Update DFF context ,BR number and Plant Number
      FOR cur_legbr_rec IN cur_legbr_details (p_request_id)
      LOOP
         BEGIN
            SELECT DISTINCT rctbr.customer_trx_id, rctbr.trx_number, ---added distinct for v1.13 to get unique records in case of multiple instalment invoice
                   rctbr.attribute_category, rctbr.comments br_number,-- rctbr.attribute2 br_number, commented for v1.13 
                   rctbr.attribute3 br_plant
              INTO l_newbr_trx_id, l_newbr_number,
                   l_br_dff, l_leg_br_num,
                   l_br_plant
              FROM ra_customer_trx_all rctbr,
                   ra_cust_trx_types_all rctt,
                   ra_customer_trx_lines_all rctlbr
             WHERE rctt.cust_trx_type_id = rctbr.cust_trx_type_id
               AND rctt.TYPE = 'BR'
               AND rctbr.org_id = rctt.org_id
               AND rctlbr.customer_trx_id = rctbr.customer_trx_id
               AND rctbr.org_id = rctlbr.org_id
               AND rctlbr.br_ref_customer_trx_id =
                                                 cur_legbr_rec.customer_trx_id;
         EXCEPTION
            WHEN OTHERS
            THEN
               l_newbr_trx_id := NULL;
         END;

         IF l_newbr_trx_id IS NOT NULL
         THEN
    --  l_br_maturity_dt := cur_legbr_rec.maturity_date;
            --BR exists
      BEGIN
      SELECT  MAX(leg_br_or_rec_issue_date), MAX(LEG_BR_OR_REC_MATURITY_DATE)
      INTO l_br_issue_dt ,l_br_maturity_dt
             FROM  xxconv.xxar_br_invoices_stg
       WHERE leg_br_openrec_ledger =cur_legbr_rec.br_plant   AND leg_request_id  = p_request_id
       AND leg_customer_number IN ( cur_legbr_rec.leg_customer_number);

              fnd_file.put_line (fnd_file.LOG,
                                  'l_br_maturity_dt  ' || l_br_maturity_dt);

   fnd_file.put_line (fnd_file.LOG,
                                  'l_br_issue_dt  ' || l_br_issue_dt);

       fnd_file.put_line (fnd_file.LOG,
                                  'request id   ' || p_request_id);
       fnd_file.put_line (fnd_file.LOG,
                                  'customer Number  ' || cur_legbr_rec.leg_customer_number);

       fnd_file.put_line (fnd_file.LOG,
                                  'plant  ' || cur_legbr_rec.br_plant );

       EXCEPTION
            WHEN OTHERS
             THEN
                l_br_maturity_dt := cur_legbr_rec.maturity_date;
                   l_br_issue_dt := cur_legbr_rec.issue_date;

       END;

       --     l_br_maturity_dt := cur_legbr_rec.maturity_date;
      --      l_br_issue_dt := cur_legbr_rec.issue_date;

            IF (l_br_plant IS NULL OR l_br_plant <> cur_legbr_rec.br_plant)
            THEN
               l_br_plant := cur_legbr_rec.br_plant;
               fnd_file.put_line (fnd_file.LOG,
                                  'BR Plant Number :  ' || l_br_plant
                                 );
            END IF;

            IF l_br_dff <> 'Eaton BR' OR l_br_dff IS NULL
            THEN
               l_br_dff := 'Eaton BR';
               fnd_file.put_line (fnd_file.LOG,
                                  'BR attribute_category  ' || l_br_dff
                                 );
            END IF;

            IF l_leg_br_num IS NULL
            THEN
               --when first time updating 
			   --l_leg_br_num := SUBSTR (cur_legbr_rec.br_trx, 1, 150);  --v1.12
			   l_leg_br_num := SUBSTR (cur_legbr_rec.br_trx, 1, 1760);   --v1.12
               fnd_file.put_line (fnd_file.LOG,
                                  'BR NUMBER  ' || l_leg_br_num);

      --- updating br number first time
      ---update BR
            BEGIN
               UPDATE ra_customer_trx_all
                  SET term_due_date = l_br_maturity_dt,
                      trx_date = l_br_issue_dt,               -- BR Issue Date
                      --attribute_category = l_br_dff,    --v1.12
                      --attribute2 = l_leg_br_num,        --v1.12
			-- comments = l_leg_br_num,            --v1.12 commented for v1.14
                      attribute9 = l_br_plant
                WHERE customer_trx_id = l_newbr_trx_id;
        commit;

               fnd_file.put_line (fnd_file.output,
                                     '+ Update plant  and old BR = '
                                  || SQL%ROWCOUNT
                                 );
            EXCEPTION
               WHEN OTHERS
               THEN
                  fnd_file.put_line
                          (fnd_file.LOG,
                              'Exception: Occured while updating BR trx id :'
                           || l_newbr_trx_id
                           || ' with error msg '
                           || SQLERRM
                          );
            END;
      --end of update BR


            ELSIF l_leg_br_num  like '%'||cur_legbr_rec.br_trx||'%'
            THEN

               --l_leg_br_num := l_leg_br_num;
               fnd_file.put_line (fnd_file.LOG,
                                  'BR NUMBER  ' || l_leg_br_num);
                  --update is not required
            ELSE

               -- when old br exists
               l_leg_br_num :=
                   SUBSTR( l_leg_br_num || '-'
                  || SUBSTR (cur_legbr_rec.br_trx, 1, 1760),1,1760);

               fnd_file.put_line (fnd_file.LOG,
                                     'already existing BR NUMBER  '
                                  || l_leg_br_num
                                 );

                 --- updating br number first time
      ---update BR
            BEGIN
               UPDATE ra_customer_trx_all
                  SET term_due_date = l_br_maturity_dt,
                      trx_date = l_br_issue_dt,               -- BR Issue Date
                      --attribute_category = l_br_dff,              --v1.12
					  --attribute2 = SUBSTR(l_leg_br_num, 1,150) ,  --v1.12
			--		  comments   = SUBSTR(l_leg_br_num, 1,1760),    --v1.12 commented for v1.14
                      attribute9 = l_br_plant
                WHERE customer_trx_id = l_newbr_trx_id;
        commit;

               fnd_file.put_line (fnd_file.output,
                                     '+ Update plant  and old BR = '
                                  || SQL%ROWCOUNT
                                 );
            EXCEPTION
               WHEN OTHERS
               THEN
                  fnd_file.put_line
                          (fnd_file.LOG,
                              'Exception: Occured while updating BR trx id :'
                           || l_newbr_trx_id
                           || ' with error msg '
                           || SQLERRM
                          );
            END;
      --end of update BR
            END IF;

            ---update BR
            BEGIN
               UPDATE ra_customer_trx_all
                  SET term_due_date = l_br_maturity_dt,
                      trx_date = l_br_issue_dt,               -- BR Issue Date
                      --attribute_category = l_br_dff,            --v1.12
					  --attribute2 = l_leg_br_num,                --v1.12
		     -- comments   = SUBSTR(l_leg_br_num, 1,1760),  --v1.12 commented for v1.14
                      attribute9 = l_br_plant
                WHERE customer_trx_id = l_newbr_trx_id;
        commit;

               fnd_file.put_line (fnd_file.output,
                                     '+ Update plant  and old BR = '
                                  || SQL%ROWCOUNT
                                 );
            EXCEPTION
               WHEN OTHERS
               THEN
                  fnd_file.put_line
                          (fnd_file.LOG,
                              'Exception: Occured while updating BR trx id :'
                           || l_newbr_trx_id
                           || ' with error msg '
                           || SQLERRM
                          );
            END;

            ---updating processed records in staging table
            /*BEGIN
               fnd_file.put_line (fnd_file.LOG,
                                  'BR NUMBER  ' || cur_legbr_rec.trx_number
                                 );
               fnd_file.put_line (fnd_file.LOG,
                                  'Org Id  ' || cur_legbr_rec.org_id
                                 );

               UPDATE xxconv.xxar_br_invoices_stg
                  SET leg_process_flag = 'C'
                WHERE leg_request_id = p_request_id
                  AND leg_trx_number = cur_legbr_rec.trx_number
                  AND org_id = cur_legbr_rec.org_id;
          Commit;

               fnd_file.put_line (fnd_file.output,
                                     '+ Update process flag in STG flag = '
                                  || SQL%ROWCOUNT
                                 );
               fnd_file.put_line
                      (fnd_file.output,
                       'BR maturity date and due date  is update successfuly '
                      );
            EXCEPTION
               WHEN OTHERS
               THEN
                  fnd_file.put_line
                     (fnd_file.LOG,
                         'Exception: Occured while updating Leg Process flag for trx number :'
                      || cur_legbr_rec.trx_number
                      || ' with error msg '
                      || SQLERRM
                     );
            END;*/ --commented for v1.7
      --add for v1.7
      BEGIN
              --updating invoice distribution table

               UPDATE xxconv.xxar_br_invoices_dist_stg
                  SET leg_process_flag = 'C'
                WHERE leg_request_id = p_request_id
                  AND leg_customer_trx_id in (
                      SELECT DISTINCT leg_customer_trx_id
                        FROM xxconv.xxar_br_invoices_stg
                       WHERE leg_request_id = p_request_id
                         AND leg_trx_number = cur_legbr_rec.trx_number
                         AND org_id = cur_legbr_rec.org_id );
          Commit;

            EXCEPTION
               WHEN OTHERS
               THEN
                  fnd_file.put_line
                     (fnd_file.LOG,
                         'Exception: Occured while updating dist stg Process flag for trx number :'
                      || cur_legbr_rec.trx_number
                      || ' with error msg '
                      || SQLERRM
                     );
            END;

      --updating header table

      BEGIN
              --updating invoice distribution table

               UPDATE xxconv.xxar_br_invoices_stg
                  SET leg_process_flag = 'C'
                WHERE leg_request_id = p_request_id
                  AND leg_trx_number = cur_legbr_rec.trx_number
                  AND org_id = cur_legbr_rec.org_id;
          Commit;

            EXCEPTION
               WHEN OTHERS
               THEN
                  fnd_file.put_line
                     (fnd_file.LOG,
                         'Exception: Occured while updating  stg Process flag for trx number :'
                      || cur_legbr_rec.trx_number
                      || ' with error msg '
                      || SQLERRM
                     );
            END;
      --end for v1.7
         ELSE
            ---BR Does not exists
            fnd_file.put_line
                    (fnd_file.LOG,
                        'R12 BR Number doesnot exists for Legacy BR Number :'
                     || cur_legbr_rec.trx_number
                    );
         END IF;                              -- IF l_newbr_trx_id IS NOT NULL
      END LOOP;                                               ---end mail loop

      fnd_file.put_line (fnd_file.LOG,
                         '+ Updated BR  Recs = ' || SQL%ROWCOUNT
                        );
      fnd_file.put_line
                      (fnd_file.LOG,
                       'BR maturity date and due date  is update successfuly '
                      );
   EXCEPTION
      WHEN OTHERS
      THEN
         pov_errbuf :=
               'Error : Main program procedure encounter error. '
            || SUBSTR (SQLERRM, 1, 150);
         pon_retcode := 2;
         print_log_message (   'In BR maturity date fails update when others-' ||l_leg_br_num
                            || SUBSTR (SQLERRM, 1, 150)
                           );
   END update_br;

-- ========================
-- Procedure: BR  Report Program
-- =============================================================================
   PROCEDURE br_report (
      pov_errbuf     OUT NOCOPY      VARCHAR2,
      pon_retcode    OUT NOCOPY      NUMBER,
      p_request_id   IN              NUMBER
   )
   IS
      ---defining cursors
      CURSOR cur_legbr_details (p_req_id NUMBER)
      IS
         SELECT DISTINCT rctl.interface_line_attribute3 br_plant,
                         (SELECT DISTINCT NAME
                                     FROM hr_operating_units
                                    WHERE organization_id =
                                                           rct.org_id)
                                                                     ou_name,
                         trx_br.new_br, trx_br.br_number old_br,
                         rct.trx_number r12_invoice_number,
                         leg_br.leg_trx_number old_invoice_number,
                         leg_br.leg_inv_amount_due_original
                                                  old_balance_invoice_amount,
                         aps.amount_due_original new_balance_invoice_amount,
                         leg_br.leg_customer_number old_customer_number,
                         hca.account_number new_customer_number,
                         rctl.interface_line_attribute6 maturity_date,
                         rctl.interface_line_attribute7 issue_date,
                         trx_br.trx_date, trx_br.term_due_date
                    FROM ra_customer_trx_all rct,
                         ra_customer_trx_lines_all rctl,
                         hz_cust_accounts_all hca,
                         ar_payment_schedules_all aps,
                         (SELECT DISTINCT leg_bill_to_address,
                                          leg_customer_trx_id,
                                          leg_customer_number,
                                          leg_line_amount, leg_trx_number,
                                          leg_br_openrec_ledger,
                                          leg_inv_amount_due_original,
                                          leg_inv_amount_due_remaining,
                                          leg_br_or_openreceipt_num,
                                          leg_br_or_rec_issue_date,
                                          leg_br_or_rec_maturity_date, org_id
                                     FROM xxconv.xxar_br_invoices_stg xxbr
                                    WHERE xxbr.leg_request_id = p_req_id
                                      /*53046975*/
                                      AND xxbr.leg_process_flag = 'C') leg_br,
                         (SELECT rctbr.customer_trx_id,
                                 rctbr.trx_number new_br,
                                 rctbr.attribute_category,
                                 rctbr.comments br_number, --rctbr.attribute2 br_number,commented for v1.13
                                 rctbr.attribute3 br_plant,
                                 br_ref_customer_trx_id, br_amount,
                                 rctbr.org_id, rctbr.trx_date,
                                 rctbr.term_due_date
                            FROM ra_customer_trx_all rctbr,
                                 ra_cust_trx_types_all rctt,
                                 ra_customer_trx_lines_all rctlbr
                           WHERE rctt.cust_trx_type_id =
                                                        rctbr.cust_trx_type_id
                             AND rctt.TYPE = 'BR'
                             AND rctbr.org_id = rctt.org_id
                             AND rctlbr.customer_trx_id =
                                                         rctbr.customer_trx_id
                             AND rctbr.org_id = rctlbr.org_id
                                                             -- AND rctlbr.br_ref_customer_trx_id = leg_br.customer_trx_id
                         ) trx_br
                   WHERE rct.trx_number = leg_br.leg_trx_number --'CKIV806848'
                     AND rct.bill_to_customer_id = hca.cust_account_id
                     AND aps.customer_trx_id = rct.customer_trx_id
                     AND rct.org_id = aps.org_id
                     AND rct.org_id = leg_br.org_id
                     AND trx_br.br_ref_customer_trx_id = rctl.customer_trx_id
                     AND trx_br.org_id = rct.org_id
                     AND rctl.customer_trx_id = rct.customer_trx_id
                                                                   --  and rct.trx_number = 'IV806848'
                                                                   -- and trx_br.new_br =1194
      ;
   BEGIN
      fnd_file.put_line (fnd_file.output,
                            'PLANT_NUMBER'
                         || CHR (9)
                         || 'OPERATING UNIT'
                         || CHR (9)
                         || '11i BR'
                         || CHR (9)
                         || 'R12 BR'
                         || CHR (9)
                         || '11i INVOICE'
                         || CHR (9)
                         || 'R12 INVOICE'
                         || CHR (9)
                         || '11i INVOICE AMOUNT'
                         || CHR (9)
                         || 'R12 INVOICE AMOUNT'
                         || CHR (9)
                         || '11 CUSTOMER NUMBER'
                         || CHR (9)
                         || 'R12 CUSTOMER NUMBER'
                         || CHR (9)
                         || '11i MATURITY DATE'
                         || CHR (9)
                         || 'R12 MATURITY DATE'
                         || CHR (9)
                         || '11i ISSUE DATE'
                         || CHR (9)
                         || 'R12 ISSUE DATE'
                         || CHR (9)
                        );

      FOR cur_legbr_rec IN cur_legbr_details (p_request_id)
      LOOP
         fnd_file.put_line (fnd_file.output,
                               cur_legbr_rec.br_plant
                            || CHR (9)
                            || cur_legbr_rec.ou_name
                            || CHR (9)
                            || cur_legbr_rec.old_br
                            || CHR (9)
                            || cur_legbr_rec.new_br
                            || CHR (9)
                            || cur_legbr_rec.old_invoice_number
                            || CHR (9)
                            || cur_legbr_rec.r12_invoice_number
                            || CHR (9)
                            || cur_legbr_rec.old_balance_invoice_amount
                            || CHR (9)
                            || cur_legbr_rec.new_balance_invoice_amount
                            || CHR (9)
                            || cur_legbr_rec.old_customer_number
                            || CHR (9)
                            || cur_legbr_rec.new_customer_number
                            || CHR (9)
                            || cur_legbr_rec.maturity_date
                            || CHR (9)
                            || cur_legbr_rec.term_due_date
                            || CHR (9)
                            || cur_legbr_rec.issue_date
                            || CHR (9)
                            || cur_legbr_rec.trx_date
                            || CHR (9)
                           );
      END LOOP;
   EXCEPTION
      WHEN OTHERS
      THEN
         fnd_file.put_line (fnd_file.output,
                            'Unable to Retrieve the BR data''s '
                           );
   END br_report;


   -- ========================
-- Procedure: Update the End end of external Bank Account receipt method program
-- =============================================================================
PROCEDURE Update_ext_bnk_acct (
   pov_errbuf            OUT NOCOPY      VARCHAR2,
   pon_retcode           OUT NOCOPY      NUMBER,
   p_br_openrec_ledger   IN              xxar_br_invoices_stg.leg_br_openrec_ledger%TYPE
)
IS
   l_ext_bank_acct_rec    iby_ext_bankacct_pub.extbankacct_rec_type;
   l_account_ret_status   VARCHAR2 (50);
   l_msg_count            NUMBER;
   l_account_msg_data     VARCHAR2 (2000);
   l_result_rec           iby_fndcpt_common_pub.result_rec_type;
   l_pay_method_rec              hz_payment_method_pub.payment_method_rec_type;
   l_cust_receipt_method_id      NUMBER;
   l_receipt_method_id           NUMBER;
   l_return_status               VARCHAR2 (2000);
    l_msg_data                    VARCHAR2 (2000);
  l_end_date                    DATE ;
  l_org_id                         NUMBER;

   CURSOR cur_ext_acct (
      pv_br_openrec_ledger   xxar_br_invoices_stg.leg_br_openrec_ledger%TYPE
   )
   IS
      SELECT DISTINCT hca.party_id, account.bank_account_num,
                      cb.bank_party_id, cb.branch_party_id, hcsu.site_use_id,
                   account.ext_bank_account_id
            , account.start_date,
                      account.bank_account_name_alt,
                      account.bank_account_type, hl.country, br.org_id ,hca.cust_account_id
                 FROM iby_ext_bank_accounts account,
                      iby_pmt_instr_uses_all acc_instr,
                      iby_external_payers_all ext_payer,
                      hz_cust_acct_sites_all hcas,
                      hz_cust_site_uses_all hcsu,
                      hz_cust_accounts_all hca,
                      hz_party_sites hps,
                      hz_locations hl,
                      ce_bank_branches_v cb,
                      xxconv.xxar_br_invoices_stg br
                WHERE 1 = 1
                  AND account.ext_bank_account_id = acc_instr.instrument_id
                  AND acc_instr.ext_pmt_party_id = ext_payer.ext_payer_id
          AND acc_instr.end_date IS  NULL
                  AND ext_payer.acct_site_use_id = hcsu.site_use_id
                  AND hcas.cust_account_id = ext_payer.cust_account_id
                  AND hcsu.cust_acct_site_id = hcas.cust_acct_site_id
                  AND hcsu.site_use_code = 'BILL_TO'
                  AND hcas.org_id = hcsu.org_id
                  AND hca.cust_account_id = hcas.cust_account_id
                  AND hcas.party_site_id = hps.party_site_id
                  AND hcsu.status = 'A'
                  AND hcas.status = 'A'
                  AND hl.location_id = hps.location_id
                  AND cb.bank_home_country = hl.country
                  AND cb.bank_name LIKE 'DUMMYBR%'
                  AND cb.bank_branch_name LIKE 'DUMMYBR%'
                  AND ACCOUNT.bank_account_num LIKE 'DUMMYBRACCT%'
                  AND NVL (hcas.org_id, 1) = NVL (br.org_id, 1)
                 -- AND hcsu.LOCATION = TRIM (br.leg_bill_to_address)
          AND hcsu.location  = substr(br.leg_bill_to_address,1,instr(br.leg_bill_to_address,'|')-1)
                  AND leg_br_openrec_ledger = nvl(pv_br_openrec_ledger,leg_br_openrec_ledger)
                  AND br.leg_process_flag  IN ('C' ,'V')
           --   AND br.leg_customer_number = '50162638'
            --      AND hca.orig_system_reference LIKE
                             --     '%' || (TRIM (br.leg_customer_number))
                                  --|| '%' commented for v1.13
								  ;

BEGIN
   FOR cur_ext_acct_rec IN cur_ext_acct (p_br_openrec_ledger)
   LOOP
   --receipt  varible start------
      l_org_id  :=cur_ext_acct_rec.org_id;
    l_pay_method_rec.cust_account_id := cur_ext_acct_rec.cust_account_id;
    l_pay_method_rec.site_use_id := cur_ext_acct_rec.site_use_id;

               BEGIN


          FOR CUR_REC IN (SELECT acrm.receipt_method_id ,acrm.end_date
                    FROM ar_cust_receipt_methods_v acrm,
                         ar_receipt_methods arm,
                         ra_cust_trx_types_all rtype,
              ar_receipt_classes arc
                   WHERE acrm.customer_id = cur_ext_acct_rec.cust_account_id              --91970
                     AND acrm.site_use_id = cur_ext_acct_rec.site_use_id       --164035
                     AND arm.receipt_method_id = acrm.receipt_method_id
                     AND NVL (acrm.end_date, SYSDATE) >= SYSDATE
                     AND rtype.cust_trx_type_id = arm.br_cust_trx_type_id
                     AND rtype.TYPE = 'BR'
                     AND NVL (rtype.end_date, SYSDATE) >= SYSDATE
             AND org_id = l_org_id
           AND arm.receipt_class_id =arc.receipt_class_id
             AND arc.NAME ='CNV Promissory Note Receipt') --v1.9

           LOOP



         IF  CUR_REC.receipt_method_id IS NOT NULL AND  CUR_REC.end_date IS NULL
         THEN

                    UPDATE ar_cust_receipt_methods_v
          SET end_date =SYSDATE
          WHERE customer_id = cur_ext_acct_rec.cust_account_id
          AND site_use_id  =cur_ext_acct_rec.site_use_id
                    AND  receipt_method_id = CUR_REC.receipt_method_id
          AND end_date is null;

          COMMIT;

      fnd_file.put_line (fnd_file.output, 'receipt method updated   - successfully for receipt_method_id --'||CUR_REC.receipt_method_id   );

         ELSE
                  fnd_file.put_line (fnd_file.output,
                            'Eaton Open BR receipt method End Date'
                        );

         END IF ;

         END LOOP; -- end of CUR_REC

       EXCEPTION
          WHEN OTHERS
          THEN
                fnd_file.put_line (fnd_file.LOG,
                            'receipt method is not updated :'
                         || l_return_status
                         || ' with error msg '
                         || SQLERRM
                        );

     END ;
    --end ---------

        BEGIN

          UPDATE iby_pmt_instr_uses_all
           SET end_date =SYSDATE
           WHERE instrument_id IN (SELECT  ext_bank_account_id
             FROM  iby_ext_bank_accounts account,   iby_external_payers_all ext_payer,      hz_cust_acct_sites_all hcas,           hz_cust_site_uses_all hcsu
               WHERE 1 = 1
                     AND ext_pmt_party_id = ext_payer.ext_payer_id
                     AND ext_payer.acct_site_use_id = hcsu.site_use_id
                     AND hcas.cust_account_id = ext_payer.cust_account_id
                     AND hcsu.cust_acct_site_id = hcas.cust_acct_site_id
                     AND hcsu.site_use_code = 'BILL_TO'
                     AND hcas.org_id = hcsu.org_id
                     AND account.ext_bank_account_id =cur_ext_acct_rec.ext_bank_account_id);

          COMMIT;

           EXCEPTION
          WHEN OTHERS
          THEN
                    fnd_file.put_line (fnd_file.output,
                            'l_account_ret_status-- '
                         || l_account_ret_status
                         || 'Bank Account ID Record updated in iby_pmt_instr_uses_all  -'
                         || cur_ext_acct_rec.ext_bank_account_id
                        );

     END ;

    ---bank account -------
      l_ext_bank_acct_rec.object_version_number := 1.0;
      l_ext_bank_acct_rec.acct_owner_party_id := cur_ext_acct_rec.party_id;
      l_ext_bank_acct_rec.bank_account_num :=
                                            cur_ext_acct_rec.bank_account_num;

      l_ext_bank_acct_rec.bank_id := cur_ext_acct_rec.bank_party_id;

      l_ext_bank_acct_rec.branch_id := cur_ext_acct_rec.branch_party_id;

      l_ext_bank_acct_rec.start_date := cur_ext_acct_rec.start_date;

      l_ext_bank_acct_rec.end_date := SYSDATE;
  --  l_ext_bank_acct_rec.end_date := NULL;

      l_ext_bank_acct_rec.currency := NULL;
      l_ext_bank_acct_rec.country_code := cur_ext_acct_rec.country;

      l_ext_bank_acct_rec.alternate_acct_name :=
                                       cur_ext_acct_rec.bank_account_name_alt;

      l_ext_bank_acct_rec.acct_type := cur_ext_acct_rec.bank_account_type;

      l_ext_bank_acct_rec.bank_account_id :=  cur_ext_acct_rec.ext_bank_account_id;




      iby_ext_bankacct_pub.update_ext_bank_acct
                                 (p_api_version            => 1.0,
                                  p_init_msg_list          => fnd_api.g_true,
                                  p_ext_bank_acct_rec      => l_ext_bank_acct_rec,
                                  x_return_status          => l_account_ret_status,
                                  x_msg_count              => l_msg_count,
                                  x_msg_data               => l_account_msg_data,
                                  x_response               => l_result_rec
                                 );


          COMMIT;

    fnd_file.put_line (fnd_file.output,
                            'Eaton Open BR Externanl Bank Account End Date Program  Output'
                        );

      fnd_file.put_line (fnd_file.output,
                            'l_account_ret_status-- '
                         || l_account_ret_status
                         || 'Bank Account ID Record updated   -'
                         || cur_ext_acct_rec.ext_bank_account_id
             ||'---'||l_account_msg_data
                        );
       l_receipt_method_id     := NULL;

   END LOOP;

   IF l_msg_count > 0
   THEN
      FOR i IN 1 .. l_msg_count
      LOOP
         l_account_msg_data :=
             fnd_msg_pub.get (p_msg_index      => i,
                              p_encoded        => fnd_api.g_false);
         fnd_file.put_line (fnd_file.LOG,
                            'l_branch_msg_data :' || l_account_msg_data
                           );
      END LOOP;
   END IF;



   COMMIT;
   --end ---

EXCEPTION
   WHEN OTHERS
   THEN
      fnd_file.put_line (fnd_file.LOG,
                            'External Bank account is not updated :'
                         || l_account_msg_data
                         || ' with error msg '
                         || SQLERRM
                        );
END Update_ext_bnk_acct;


-- ========================
-- Procedure: TIE_BACK
-- =============================================================================
--   This procedure to tie back the process status after Autoinvoice program is complete
-- =============================================================================
--
   PROCEDURE tie_back (
      pov_errbuf     OUT NOCOPY      VARCHAR2,
      pon_retcode    OUT NOCOPY      NUMBER,
      piv_dummy1     IN              VARCHAR2,
      pin_batch_id   IN              NUMBER
   )
   IS
      l_err_msg          VARCHAR2 (4000);
      l_error_flag       VARCHAR2 (10);
      l_return_status    VARCHAR2 (200)  := NULL;
      l_log_ret_status   VARCHAR2 (50);
      l_log_err_msg      VARCHAR2 (2000);
	  l_interface_status ra_interface_lines_all.interface_status%TYPE; -- v1.13

      CURSOR tie_back_cur
      IS
         SELECT   *
             FROM xxconv.xxar_br_invoices_stg
            WHERE (   process_flag = 'P'
                   OR (process_flag = 'E' AND ERROR_TYPE = 'ERR_IMP')
                  )
              AND batch_id = pin_batch_id
         ORDER BY leg_trx_number;

      CURSOR interface_error_cur (p_interface_line_attribute15 IN VARCHAR2)
      IS
         SELECT /*+ INDEX (ril XX_RA_INTERFACE_LINES_N11) */
				ril.interface_line_id, rie.MESSAGE_TEXT
           FROM ra_interface_errors_all rie, ra_interface_lines_all ril
          WHERE ril.interface_line_id = rie.interface_line_id
            AND ril.interface_line_attribute15 = p_interface_line_attribute15
            AND ril.interface_line_context = 'Eaton';
   BEGIN
      print_log_message (   'Tie Back Starts at: '
                         || TO_CHAR (g_sysdate, 'DD-MON-YYYY HH24:MI:SS')
                        );
      print_log_message ('+ Start of Tie Back + ' || pin_batch_id);
      g_new_run_seq_id := xxetn_run_sequences_s.NEXTVAL;
      xxetn_common_error_pkg.g_run_seq_id := g_new_run_seq_id;

      FOR tie_back_rec IN tie_back_cur
      LOOP
         print_log_message (   'Interface Transaction Id = '
                            || tie_back_rec.interface_txn_id
                           );
         l_error_flag := NULL;

         FOR interface_error_rec IN
            interface_error_cur (tie_back_rec.interface_line_attribute15)
         LOOP
            print_log_message (   'In error loop: Interface line Id = '
                               || interface_error_rec.interface_line_id
                              );
            print_log_message (   'In error loop: Message Text - '
                               || interface_error_rec.MESSAGE_TEXT
                              );
            l_error_flag := 'E';
            log_errors (pin_transaction_id           => tie_back_rec.interface_txn_id,
                        piv_source_column_name       => 'Interface Error',
                        piv_source_column_value      => NULL,
                        piv_error_type               => 'ERR_IMP',
                        piv_error_code               => 'ETN_AR_BR_INVOICE_CREATION_FAILED',
                        piv_error_message            => interface_error_rec.MESSAGE_TEXT,
                        pov_return_status            => l_log_ret_status,
                        pov_error_msg                => l_log_err_msg
                       );
         END LOOP;

         IF l_error_flag = 'E'
         THEN
            UPDATE xxconv.xxar_br_invoices_stg
               SET process_flag = 'E',
                   ERROR_TYPE = 'ERR_IMP',
                   run_sequence_id = g_new_run_seq_id,
                   last_update_date = SYSDATE,
                   last_updated_by = g_last_updated_by,
                   last_update_login = g_login_id,
				   request_id        = g_request_id -- added for v1.13
             WHERE interface_txn_id = tie_back_rec.interface_txn_id;

            UPDATE xxconv.xxar_br_invoices_dist_stg
               SET process_flag = 'E',
                   ERROR_TYPE = 'ERR_IMP',
                   run_sequence_id = g_new_run_seq_id,
                   last_update_date = SYSDATE,
                   last_updated_by = g_last_updated_by,
                   last_update_login = g_login_id,
				   request_id        = g_request_id -- added for v1.13
             WHERE leg_customer_trx_id = tie_back_rec.leg_customer_trx_id
               AND process_flag <> 'X'
               --     AND leg_cust_trx_line_id = tie_back_rec.leg_cust_trx_line_id
               AND batch_id = pin_batch_id;
         ELSE
		  -- Added below for v1.13 
				l_interface_status := NULL;
				BEGIN
				  SELECT rila.interface_status
					INTO l_interface_status
					FROM ra_interface_lines_all rila
				   WHERE rila.interface_line_attribute15 =
						 tie_back_rec.interface_line_attribute15
					 AND rila.interface_line_context = g_interface_line_context;
				EXCEPTION
				  WHEN OTHERS THEN
					l_interface_status := NULL;
				END;
				IF l_interface_status = 'P' THEN
				-- Added above for v1.13
						UPDATE xxconv.xxar_br_invoices_stg
						   SET process_flag = 'C',
							   run_sequence_id = g_new_run_seq_id,
							   last_update_date = SYSDATE,
							   last_updated_by = g_last_updated_by,
							   last_update_login = g_login_id,
							   request_id        = g_request_id -- added for v1.13
						 WHERE interface_txn_id = tie_back_rec.interface_txn_id;

						UPDATE xxconv.xxar_br_invoices_dist_stg
						   SET process_flag = 'C',
							   run_sequence_id = g_new_run_seq_id,
							   last_update_date = SYSDATE,
							   last_updated_by = g_last_updated_by,
							   last_update_login = g_login_id,
							   request_id        = g_request_id -- added for v1.13
						 WHERE leg_customer_trx_id = tie_back_rec.leg_customer_trx_id
						  -- AND leg_cust_trx_line_id = tie_back_rec.leg_cust_trx_line_id commented for V1.13
						   AND process_flag <> 'X'
						   AND batch_id = pin_batch_id;
				ELSE
					  -- consider this as error record because of one of parent line failing
					  UPDATE xxconv.xxar_br_invoices_stg
						 SET process_flag      = 'E',
							 ERROR_TYPE        = 'ERR_IMP',
							 run_sequence_id   = g_new_run_seq_id,
							 last_update_date  = g_sysdate,
							 last_updated_by   = g_last_updated_by,
							 last_update_login = g_login_id,
							 request_id        = g_request_id -- added for v1.13
					   WHERE interface_txn_id = tie_back_rec.interface_txn_id;
					   
					  UPDATE xxconv.xxar_br_invoices_dist_stg
						 SET process_flag      = 'E',
							 ERROR_TYPE        = 'ERR_IMP',
							 run_sequence_id   = g_new_run_seq_id,
							 last_update_date  = g_sysdate,
							 last_updated_by   = g_last_updated_by,
							 last_update_login = g_login_id,
							 request_id        = g_request_id -- added for v1.13
					   WHERE leg_customer_trx_id = tie_back_rec.leg_customer_trx_id
						 AND process_flag <> 'X'
							--     AND leg_cust_trx_line_id = tie_back_rec.leg_cust_trx_line_id commented for v1.13
						 AND batch_id = pin_batch_id;
				END IF; --end of IF l_interface_status = 'P' 
				-- Added above for v1.13 
		 END IF; --end of  IF l_error_flag = 'E'
      END LOOP;

      IF g_error_tab.COUNT > 0
      THEN
         xxetn_common_error_pkg.add_error
                                     (pov_return_status        => l_return_status
                                                                                 -- OUT
         ,
                                      pov_error_msg            => l_err_msg
                                                                           -- OUT
         ,
                                      pi_source_tab            => g_error_tab,
                                      pin_batch_id             => pin_batch_id,
                                      pin_run_sequence_id      => g_new_run_seq_id
                                     );
         g_error_tab.DELETE;
      END IF;

      pon_retcode := g_retcode;
      pov_errbuf := g_errbuff;
      COMMIT;
   EXCEPTION
      WHEN OTHERS
      THEN
         g_retcode := 2;
         g_errbuff := 'Failed During Tie Back';
         print_log_message (   'In Tie Back when others'
                            || SUBSTR (SQLERRM, 1, 150)
                           );
   END tie_back;

----------------------------
--MAIN PROCEDURE
 ----------------------------
   PROCEDURE main (
      pov_errbuf             OUT NOCOPY      VARCHAR2,
      pon_retcode            OUT NOCOPY      NUMBER,
      piv_run_mode           IN              VARCHAR2,
      piv_dummy1             IN              VARCHAR2,
      piv_operating_unit     IN              VARCHAR2,
      piv_transaction_type   IN              VARCHAR2,
      pin_batch_id           IN              NUMBER,
      piv_dummy              IN              VARCHAR2,
      piv_process_records    IN              VARCHAR2,
      piv_gl_date            IN              VARCHAR2,
      piv_period_set_name    IN              VARCHAR2,
      piv_request_id         IN              NUMBER DEFAULT '0'
   )
   IS
      l_debug_on              BOOLEAN;
      l_return_status         VARCHAR2 (200)                          := NULL;
      l_err_code              VARCHAR2 (40)                           := NULL;
      l_err_msg               VARCHAR2 (2000)                         := NULL;
      l_err_excep             EXCEPTION;
      l_warn_excep            EXCEPTION;
      l_log_ret_status        VARCHAR2 (50)                           := NULL;
      l_log_err_msg           VARCHAR2 (2000);
      l_debug_err             VARCHAR2 (2000);
      l_load_ret_stats        VARCHAR2 (1);
      l_dist_load_ret_stats   VARCHAR2 (1);
      l_print_ret_stats       VARCHAR2 (1);
      l_print_err_msg         VARCHAR2 (1000);
      l_load_err_msg          VARCHAR2 (1000);
      l_dist_load_err_msg     VARCHAR2 (1000);
      l_conv_batch_ou         xxar_conv_batches.leg_operating_unit%TYPE;
      l_conv_batch_txn        xxar_conv_batches.leg_transaction_type%TYPE;
      l_err_ret_status        VARCHAR2 (1);
      l_error_message         VARCHAR2 (2000);
      ---PULL VARIABLE ----
      l_errbuf                VARCHAR2 (2000)                         := NULL;
      l_retcode               VARCHAR2 (2000)                         := NULL;
      lp_request_id           NUMBER                        := piv_request_id;
      l_debug                 VARCHAR2 (20)                          := 'YES';
------------------
   BEGIN
      xxetn_debug_pkg.initialize_debug
                             (pov_err_msg           => g_debug_err,
                              piv_program_name      => 'ETN_BR_INVOICE_CONVERSION'
                             );
      xxetn_debug_pkg.add_debug ('Program Parameters');
      xxetn_debug_pkg.add_debug
                              ('---------------------------------------------');
      xxetn_debug_pkg.add_debug ('Run Mode        : ' || piv_run_mode);
      xxetn_debug_pkg.add_debug ('Batch ID        : ' || pin_batch_id);
      xxetn_debug_pkg.add_debug (   'Reprocess records     : '
                                 || piv_process_records
                                );
      xxetn_debug_pkg.add_debug (   'Legacy Operating Unit : '
                                 || piv_operating_unit
                                );
      xxetn_debug_pkg.add_debug (   'Legacy Transaction Type : '
                                 || piv_transaction_type
                                );
      xxetn_debug_pkg.add_debug ('GL Date        : ' || piv_gl_date);
      print_log_message ('Program Parameters');
      print_log_message ('---------------------------------------------');
      print_log_message ('Run Mode        : ' || piv_run_mode);
      print_log_message ('Batch ID        : ' || pin_batch_id);
      print_log_message ('Reprocess records    : ' || piv_process_records);
      print_log_message ('Legacy Operating Unit : ' || piv_operating_unit);
      print_log_message ('Legacy Transaction Type : ' || piv_transaction_type);
      print_log_message ('GL Date        : ' || piv_gl_date);
      print_log_message ('Request Id        : ' || piv_request_id);
      print_log_message ('');
      g_run_mode := piv_run_mode;
      g_batch_id := pin_batch_id;
      g_process_records := piv_process_records;
      g_leg_operating_unit := piv_operating_unit;
      g_leg_trasaction_type := piv_transaction_type;

      IF piv_gl_date IS NOT NULL
      THEN
         g_gl_date := apps.fnd_date.canonical_to_date (piv_gl_date);
      END IF;

      g_period_set_name := piv_period_set_name;

----------------------------------------------------------------------------------------------------------
-- Program run in run mode = 'LOAD'
-- Data will be loaded from extraction table which was populated by Eaton into the R12 staging tables for conversion in R12
-- Data will be loaded in xxar_br_invoices_stg and xxconv.xxar_br_invoices_dist_stg
----------------------------------------------------------------------------------------------------------
      IF g_run_mode = 'PULL-DATA'
      THEN
         print_log_message ('Calling procedure load_invoice');
         print_log_message ('');
         pull (l_errbuf, l_retcode, lp_request_id, 'YES');
      ELSIF UPPER (piv_run_mode) = 'LOAD-DATA'
      THEN
         print_log_message ('Calling procedure load_invoice');
         print_log_message ('');
         load_invoice (pov_ret_stats      => l_load_ret_stats,
                       pov_err_msg        => l_load_err_msg
                      );
         pon_retcode := g_retcode;
         print_log_message ('Calling procedure load_distribution');
         print_log_message ('');
         load_distribution (pov_ret_stats      => l_dist_load_ret_stats,
                            pov_err_msg        => l_dist_load_err_msg
                           );
         pon_retcode := g_retcode;
         print_stats_p;

         IF l_load_ret_stats <> 'S'
         THEN
            print_log_message (   'Error in procedure load_invoice'
                               || l_load_err_msg
                              );
            print_log_message ('');
            RAISE l_warn_excep;
         END IF;

         IF l_dist_load_ret_stats <> 'S'
         THEN
            print_log_message (   'Error in procedure load_distribution'
                               || l_dist_load_err_msg
                              );
            print_log_message ('');
            RAISE l_warn_excep;
         END IF;
      ELSIF UPPER (piv_run_mode) = 'PRE-VALIDATE'
      THEN
         print_log_message ('Calling procedure pre_validate_invoice');
         print_log_message ('');
         pre_validate_invoice;
         pon_retcode := g_retcode;
      ELSIF UPPER (piv_run_mode) = 'VALIDATE'
      THEN
         IF g_batch_id IS NOT NULL AND piv_process_records IS NULL
         THEN
            l_err_code := 'ETN_BR_CHECK_PARAMETER';
            l_err_msg :=
               'Parameter batch ID provided but Parameter Reprocess records is NULL';
            log_errors (piv_error_type         => 'ERR_VAL',
                        piv_error_code         => l_err_code,
                        piv_error_message      => l_err_msg,
                        pov_return_status      => l_log_ret_status,
                        pov_error_msg          => l_log_err_msg
                       );
            RAISE l_err_excep;
         END IF;

         IF g_batch_id IS NULL AND piv_process_records IS NOT NULL
         THEN
            l_err_code := 'ETN_BR_CHECK_PARAMETER';
            l_err_msg :=
               'Parameter Reprocess records is provided but Parameter batch id is null';
            log_errors (piv_error_type         => 'ERR_VAL',
                        piv_error_code         => l_err_code,
                        piv_error_message      => l_err_msg,
                        pov_return_status      => l_log_ret_status,
                        pov_error_msg          => l_log_err_msg
                       );
            RAISE l_err_excep;
         END IF;

         IF g_batch_id IS NULL AND g_gl_date IS NULL
         THEN
            l_err_code := 'ETN_BR_CHECK_PARAMETER';
            l_err_msg := 'GL date cannot be NULL';
            log_errors (piv_error_type         => 'ERR_VAL',
                        piv_error_code         => l_err_code,
                        piv_error_message      => l_err_msg,
                        pov_return_status      => l_log_ret_status,
                        pov_error_msg          => l_log_err_msg
                       );
            RAISE l_err_excep;
         END IF;

         IF g_leg_operating_unit IS NOT NULL AND g_batch_id IS NOT NULL
         THEN
            BEGIN
               SELECT leg_operating_unit
                 INTO l_conv_batch_ou
                 FROM xxar_conv_batches
                WHERE batch_id = g_batch_id;

               IF NVL (l_conv_batch_ou, piv_operating_unit) <>
                                                            piv_operating_unit
               THEN
                  l_err_code := 'ETN_AR_CHECK_PARAMETER';
                  l_err_msg :=
                     'Operating unit used while processing batch is different from operating unit provided while reprocessing batch';
                  log_errors (piv_error_type         => 'ERR_VAL',
                              piv_error_code         => l_err_code,
                              piv_error_message      => l_err_msg,
                              pov_return_status      => l_log_ret_status,
                              pov_error_msg          => l_log_err_msg
                             );
                  RAISE l_err_excep;
               END IF;
            EXCEPTION
               WHEN NO_DATA_FOUND
               THEN
                  l_err_code := 'ETN_AR_CHECK_PARAMETER';
                  l_err_msg := 'Batch number provided is not a valid batch ';
                  log_errors (piv_error_type         => 'ERR_VAL',
                              piv_error_code         => l_err_code,
                              piv_error_message      => l_err_msg,
                              pov_return_status      => l_log_ret_status,
                              pov_error_msg          => l_log_err_msg
                             );
                  RAISE l_err_excep;
            END;
         END IF;

         IF g_leg_trasaction_type IS NOT NULL AND g_batch_id IS NOT NULL
         THEN
            BEGIN
               SELECT leg_transaction_type
                 INTO l_conv_batch_txn
                 FROM xxar_conv_batches
                WHERE batch_id = g_batch_id;

               IF NVL (l_conv_batch_txn, piv_transaction_type) <>
                                                          piv_transaction_type
               THEN
                  l_err_code := 'ETN_BR_CHECK_PARAMETER';
                  l_err_msg :=
                     'Transaction Type used while processing batch is different from Transaction Type provided while reprocessing batch';
                  log_errors (piv_error_type         => 'ERR_VAL',
                              piv_error_code         => l_err_code,
                              piv_error_message      => l_err_msg,
                              pov_return_status      => l_log_ret_status,
                              pov_error_msg          => l_log_err_msg
                             );
                  RAISE l_err_excep;
               END IF;
            EXCEPTION
               WHEN NO_DATA_FOUND
               THEN
                  l_err_code := 'ETN_BR_CHECK_PARAMETER';
                  l_err_msg := 'Batch number provided is not a valid batch ';
                  log_errors (piv_error_type         => 'ERR_VAL',
                              piv_error_code         => l_err_code,
                              piv_error_message      => l_err_msg,
                              pov_return_status      => l_log_ret_status,
                              pov_error_msg          => l_log_err_msg
                             );
                  RAISE l_err_excep;
            END;
         END IF;

         IF g_batch_id IS NULL
         THEN
            g_new_batch_id := xxetn_batches_s.NEXTVAL;

            BEGIN
               INSERT INTO xxar_conv_batches
                    VALUES (g_new_batch_id, NULL,
                                                  --g_new_run_seq_id,
                            'BR_INVOICE_CONVERSION', g_leg_operating_unit,
                            SYSDATE, SYSDATE, g_user_id, SYSDATE, g_user_id,
                            g_login_id, g_leg_trasaction_type);

               COMMIT;
            EXCEPTION
               WHEN OTHERS
               THEN
                  l_err_code := 'ETN_BR_BATCH_MONITOR';
                  l_err_msg :=
                              'Error inserting record in XXAR_CONV_BATCHES  ';
                  log_errors (piv_error_type         => 'ERR_VAL',
                              piv_error_code         => l_err_code,
                              piv_error_message      => l_err_msg,
                              pov_return_status      => l_log_ret_status,
                              pov_error_msg          => l_log_err_msg
                             );
                  RAISE l_err_excep;
            END;
         ELSE
            g_new_batch_id := g_batch_id;
         END IF;

         g_new_run_seq_id := xxetn_run_sequences_s.NEXTVAL;
         xxetn_debug_pkg.add_debug ('New Batch ID        : ' || g_new_batch_id);
         xxetn_debug_pkg.add_debug (   'New Run Sequence ID : '
                                    || g_new_run_seq_id
                                   );
         print_log_message ('New Batch ID        : ' || g_new_batch_id);
         print_log_message ('New Run Sequence ID : ' || g_new_run_seq_id);
         xxetn_debug_pkg.add_debug
                              ('---------------------------------------------');
         xxetn_debug_pkg.add_debug ('PROCEDURE: assign_batch_id' || CHR (10));
         -- Call procedure to assign batch IDs
         l_err_code := NULL;
         l_err_msg := NULL;
         assign_batch_id (l_return_status, l_err_code, l_err_msg);

         IF l_return_status = fnd_api.g_ret_sts_error
         THEN
            log_errors (piv_error_type         => 'ERR_VAL',
                        piv_error_code         => l_err_code,
                        piv_error_message      => l_err_msg,
                        pov_return_status      => l_log_ret_status,
                        pov_error_msg          => l_log_err_msg
                       );
            print_log_message ('Exiting Program');
            xxetn_debug_pkg.add_debug ('Exiting Program..');
            RETURN;
         END IF;

         xxetn_debug_pkg.add_debug
                              ('---------------------------------------------');
         xxetn_debug_pkg.add_debug ('PROCEDURE: Validate Invoice' || CHR (10));
           --duplicate_check;
         --    populate_list;
         validate_invoice (piv_period_set_name);
         --ver1.11 start
         group_tax_lines;

         --ver1.11 end
         IF g_error_tab.COUNT > 0
         THEN
            xxetn_common_error_pkg.add_error
                                     (pov_return_status        => l_err_ret_status,
                                      pov_error_msg            => l_error_message,
                                      pi_source_tab            => g_error_tab,
                                      pin_batch_id             => g_new_batch_id,
                                      pin_run_sequence_id      => g_new_run_seq_id
                                     );
         END IF;

         print_stats_p;
         pon_retcode := g_retcode;
      ELSIF UPPER (piv_run_mode) = 'CONVERSION'
      THEN
         IF g_leg_operating_unit IS NOT NULL
         THEN
            print_log_message
               ('Parameter Legacy Operating Unit will not be considered for this mode '
               );
            g_leg_operating_unit := NULL;
         END IF;

         IF g_leg_trasaction_type IS NOT NULL
         THEN
            print_log_message
               ('Parameter Legacy Transaction Type will not be considered for this mode '
               );
            g_leg_trasaction_type := NULL;
         END IF;

         IF g_process_records IS NOT NULL
         THEN
            print_log_message
               ('Parameter Reprocess Records will not be considered for this mode '
               );
            g_process_records := NULL;
         END IF;

         IF g_gl_date IS NOT NULL
         THEN
            print_log_message
                   ('Parameter GL Date will not be considered for this mode ');
            g_gl_date := NULL;
         END IF;

         IF g_batch_id IS NULL
         THEN
            l_err_code := 'ETN_AR_CHECK_PARAMETER';
            l_err_msg :=
                  'Parameter Batch Id is mandatory for run mode "CONVERSION"';
            print_log_message (l_err_msg);
            log_errors (piv_error_type         => 'ERR_VAL',
                        piv_error_code         => l_err_code,
                        piv_error_message      => l_err_msg,
                        pov_return_status      => l_log_ret_status,
                        pov_error_msg          => l_log_err_msg
                       );
            RAISE l_err_excep;
         END IF;

         g_new_batch_id := g_batch_id;
         g_new_run_seq_id := xxetn_run_sequences_s.NEXTVAL;
         xxetn_debug_pkg.add_debug
                              ('---------------------------------------------');
         xxetn_debug_pkg.add_debug ('PROCEDURE: create_invoice' || CHR (10));
         create_invoice;

         IF g_error_tab.COUNT > 0
         THEN
            xxetn_common_error_pkg.add_error
                                     (pov_return_status        => l_err_ret_status,
                                      pov_error_msg            => l_error_message,
                                      pi_source_tab            => g_error_tab,
                                      pin_batch_id             => g_new_batch_id,
                                      pin_run_sequence_id      => g_new_run_seq_id
                                     );
         END IF;

         print_stats_p;
         pon_retcode := g_retcode;
      ELSIF UPPER (piv_run_mode) = 'RECONCILE'
      THEN
         print_log_message ('In Reconciliation Mode');

         IF g_leg_operating_unit IS NOT NULL
         THEN
            print_log_message
               ('Parameter Legacy Operating Unit will not be considered for this mode '
               );
            g_leg_operating_unit := NULL;
         END IF;

         IF g_leg_trasaction_type IS NOT NULL
         THEN
            print_log_message
               ('Parameter Legacy Transaction Type will not be considered for this mode '
               );
            g_leg_trasaction_type := NULL;
         END IF;

         IF g_process_records IS NOT NULL
         THEN
            print_log_message
               ('Parameter Reprocess Records will not be considered for this mode '
               );
            g_process_records := NULL;
         END IF;

         IF g_gl_date IS NOT NULL
         THEN
            print_log_message
                   ('Parameter GL Date will not be considered for this mode ');
            g_gl_date := NULL;
         END IF;

         g_new_batch_id := g_batch_id;
         g_new_run_seq_id := NULL;
         print_stats_p;
      END IF;                                               -- IF piv_run_mode
   EXCEPTION
      WHEN l_warn_excep
      THEN
         print_log_message
                       (   'Main program procedure encounter user exception '
                        || SUBSTR (SQLERRM, 1, 150)
                       );
         pov_errbuf :=
               'Error : Main program procedure encounter user exception. '
            || SUBSTR (SQLERRM, 1, 150);
         pon_retcode := 1;
      WHEN l_err_excep
      THEN
         print_log_message
                       (   'Main program procedure encounter user exception '
                        || SUBSTR (SQLERRM, 1, 150)
                       );
         pov_errbuf :=
               'Error : Main program procedure encounter user exception. '
            || SUBSTR (SQLERRM, 1, 150);
         pon_retcode := 2;
      WHEN OTHERS
      THEN
         print_log_message (   'Main program procedure encounter error '
                            || SUBSTR (SQLERRM, 1, 150)
                           );
         pov_errbuf :=
               'Error : Main program procedure encounter error. '
            || SUBSTR (SQLERRM, 1, 150);
         pon_retcode := 2;
   END main;
END xxar_br_invoices_pkg;
/

--Begin Revision History
--<<<
-- 02-May-2016 14:06:16 C9916945 /main/19
-- 
--<<<
-- 11-Aug-2016 16:55:09 C9916945 /main/20
-- 
-- <<<
-- 12-Aug-2016 04:36:37 C9904310 /main/21
-- 
-- <<<
-- 21-Sep-2016 05:42:49 C9914584 /main/22
-- 
-- <<<
-- 21-Nov-2016 08:57:34 C9914584 /main/23
-- 
-- <<<
-- 05-Dec-2016 21:59:23 E9974449 /main/24
-- 
-- <<<
-- 09-Dec-2016 14:52:21 E9974449 /main/25
-- 
-- <<<
-- 09-Dec-2016 15:37:04 E9974449 /main/26
-- 
-- <<<
-- 13-Dec-2016 11:53:09 C9904310 /main/27
-- 
-- <<<
-- 20-Dec-2016 05:49:42 E9974449 /main/28
-- 
-- <<<
-- 16-Jan-2017 00:38:32 E5251321 /main/29
-- 
-- <<<
--End Revision History  
CREATE OR REPLACE PACKAGE BODY xxar_open_claim_pkg
----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--    Owner        : EATON CORPORATION.
--    Application  : Account Receivables
--    Schema       : APPS
--    Compile AS   : APPS
--    File Name    : XXAR_OPEN_CLAIM_PKG.pkb
--    Date         : 21-Jan-2014
--    Author       : Vikas Srivastava
--    Description  : Package Body for Claims conversion
--
--    Version      : $ETNHeader: /CCSTORE/ccweb/E5251321/E5251321_UNIFY_AR_TOP_2/vobs/AR_TOP/xxar/12.0.0/install/XXAR_OPEN_CLAIM_PKG.pkb /main/29 16-Jan-2017 00:38:32 E5251321  $
--
--    Parameters  :
--
--    Change History
--  ================================================================================================================================================================================================
--    v1.0    Vikas Srivastava     21-Jan-2014  Creation
--    v1.1    Seema Machado        15-May-2014  Updated for FOT changes
--    v1.2    Seema Machado        4-Aug-2014   Updated for DFF rationalization changes
--    v1.3    Seema Machado        13-Aug-2014  Updated for ETN_MAP_UNIT changes
--    v1.4    Amit Rathi           25-Mar-2015  Changes from CR 283417 (search code with CR# to see changes)
--                                              Changed for Batch source derivation,factoring,OU derivation from REC account
--    v1.5    Seema Machado        01-Apr-2015  Fix for changes made in ver1.2 to initialize l_int_line_attr1
--    v1.6    Amit Rathi           02-Apr-2015  CR to be raised,make taxbale_flag = N so tax is not calculated
--    v1.7    Sarvesh Barve        05-Oct-2015  CR328721 - Following changes,
--                                              1.7.1.Deriving OU for NFSC based on interface_header_attribute1 (Defect# 2006)? NAFSC only
--                                              1.7.2.Map interface_header_attribute fields from 11i to R12 (Defect# 2887)    - NAFSC, ISSC, SASC
--                                              1.7.3.Map plant to header_attribute9 (Defect#2890)                            - NAFSC, ISSC, SASC
--    v1.8    Sarvesh Barve        26-Oct-2015  CR342077 - Accounting for US and Canada Transactions based on customer type
--    v1.9    Sarvesh Barve        16-Nov-2015  CR346150 - Additional condition to derive R12 Bill To customer based on Orig System Reference at site
--                                                         Added logic to derive warehouse for Brazil transactions.
--    v1.10   Kulraj Singh         29-Jan-2016  Defect# 5080. Batch Source name modified for Claims
--                                              Added lookup XXAR_CUST_CNV_BR_OU_MAP instead of Eaton EPS OU reference
--                                              Modified cursor tie_back_cur in Proc tie_back to include import error records
--                                              Modified customer/bill to site validation queries to use Site ORIG_SYS_REF instead of site use bill-location
--    v1.11   Abhijit Pande        12-Feb-2016  Modified code for brazil scenario where same trx number exists in operating unit. Defect# 5350
--                                              Claims : AutoInvoice Error : The total distribution amount for a transaction line must equal the transaction amount
--    v1.12   Kulraj Singh         19-Feb-2016  Updated assign_batch_id proc to include only 'ERR_VAL' error records.
--                                              Updated TIE_BACK proc to additionally check success records in ra_interface_lines_all before updating 'W' status
--                                              Updated create_receipts proc, cursor create_receipts_cur updated to include ERR_API records for re-processing
--                                              Update create_receipts proc, modified select statement to validate duplicate receipt
--    v1.13   Kulraj Singh         11-Mar-2016  Updated xxetn_map_unit to xxetn_map_unit_v.
--    v1.14   Sarvesh Barve        15-Mar-2016  CR 373764 Defect 4886 - Change to replicate the invoice level REFNO details to receipt application level
--                                              CR 373764 Defect 4886 - Reason code for NAFSC to be derived from Header_Attribute5 
--                                              CR 373764 Defect 4886 - Reference#, Reason Code, Comments and Owner for ISSC XXXX-XXXXXX-XXXX-XXX
--    v1.15   Sarvesh Barve        30-Mar-2016  CR 373764 Defect 4875 - For NAFSC US and CA records, segment6 and 7 changes for IC customers.
--    v1.16   Sarvesh Barve        07-Apr-2016  CR 373764 Defect 4989 - For NAFSC US and CA records, both REV and REC lines to be considered to update seg2.
--    v1.17   Sarvesh Barve        08-Apr-2016  CR 373764 Removing reference to leg_customer_number during customer derivation
--    v1.18   Sarvesh Barve        11-Aug-2016  Defect# 9428 Removal of NVL and use of Substring of ISSC Attribute2 due to leg_comments width more than 150 char
--    v1.19   Sarvesh Barve        14-Sep-2016  Defect# 9489 Mock4 Claims - Claims that did not convert from DMs to the Claims Module.
--    v1.20   Bhaskar Pedipina     19-Sep-2016  Changes done in update_duedate procedure for performance issue
--    v1.21   Bhaskar Pedipina     21-Nov-2016  Mock$ Defect# 9489 Uncommented org_id condition in create_receipts procedure
--    v1.22   Piyush Ojha          05-Dec-2016  Mock 5 Defect 12844 ORA-06502: PL/SQL: numeric or value error: character string buffer too small
--                                              Used SUBSTRB as it was giving issue and more character length in create_receipts procedure
--    v1.23   Piyush Ojha          09-Dec-2016  Mock 5 Defect 12769 Removing bug of CR 373764 Defect 4875 as This should only be implemented for OU US AR and OU CA AR.
--    v1.24   Kulraj Singh         13-Dec-2016  MOCK 5 Defect# 12858. Corrected BULK collect statement in procedure bulk_errors
--    v1.25   Piyush Ojha          20-Dec-2016  Mock 5 Defect# 12981 Corrected select statement to identify only DM transaction
--    v1.26   Kamalraj N           03-Jan-2017  Mock 5 Defect# 12864 For Inter-Company Transactions - Segment 3 of accounting converted as 11411 for the majority of items -- 
--    =================================================================================================================================================================================
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
AS
   -- global variables
   g_request_id                        NUMBER
                                           DEFAULT fnd_global.conc_request_id;
   g_prog_appl_id                      NUMBER DEFAULT fnd_global.prog_appl_id;
   g_conc_program_id                   NUMBER
                                           DEFAULT fnd_global.conc_program_id;
   g_user_id                           NUMBER      DEFAULT fnd_global.user_id;
   g_login_id                          NUMBER     DEFAULT fnd_global.login_id;
   g_org_id                            NUMBER       DEFAULT fnd_global.org_id;
   g_set_of_books_id                   NUMBER
                               DEFAULT fnd_profile.VALUE ('GL_SET_OF_BKS_ID');
   g_retcode                           NUMBER                            := 0;
   g_errbuff                           VARCHAR2 (20)             := 'SUCCESS';
   g_sysdate                  CONSTANT DATE                        := SYSDATE;
--   g_batch_source             CONSTANT VARCHAR2 (30)          := 'CONVERSION';
   g_source_table             CONSTANT VARCHAR2 (30)  := 'XXAR_OPENCLAIM_STG';
--Ver 1.2 Changes start
   --g_interface_line_context CONSTANT VARCHAR2 ( 30 ) := 'Conversion';
   g_interface_line_context   CONSTANT VARCHAR2 (30)               := 'Eaton';
--Ver 1.2 Changes end
   g_description              CONSTANT VARCHAR2 (240)
                                                  := 'Converted Claim Amount';
   g_line_type                CONSTANT VARCHAR2 (30)                := 'LINE';
   g_quantity                 CONSTANT NUMBER                            := 1;
   g_receipt_type             CONSTANT VARCHAR2 (30)            := 'Standard';
   g_state                    CONSTANT VARCHAR2 (30)             := 'Cleared';
   g_receipt_amount           CONSTANT NUMBER                            := 0;
   g_amount_applied           CONSTANT NUMBER                            := 0;
   g_application_rerefence    CONSTANT VARCHAR2 (240)              := 'CLAIM';
   g_ricew_id                 CONSTANT VARCHAR2 (10)             := 'CNV-005';
   g_acct_rec                 CONSTANT VARCHAR2 (10)                 := 'REC';
   g_acct_rev                 CONSTANT VARCHAR2 (10)                 := 'REV';
   g_init_msg_list            CONSTANT VARCHAR2 (20)        := fnd_api.g_true;
   g_run_seq_id                        NUMBER;
   g_run_mode                          VARCHAR2 (100);
   g_process_records                   VARCHAR2 (100);
   g_gl_date                           DATE;
   g_err_code                          VARCHAR2 (100);
   g_err_message                       VARCHAR2 (2000);
   g_batch_id                          NUMBER;
   g_new_batch_id                      NUMBER;
   g_indx                              NUMBER                            := 0;
   g_limit                             NUMBER
                             := fnd_profile.VALUE ('ETN_FND_ERROR_TAB_LIMIT');
   l_source_tab                        xxetn_common_error_pkg.g_source_tab_type;
   g_intf_staging_id                   xxetn_common_error.interface_staging_id%TYPE;
   g_failed_count                      NUMBER;
   g_total_count                       NUMBER;
   -- COA
   g_direction                         VARCHAR2 (240)      := 'LEGACY-TO-R12';
   g_coa_error                CONSTANT VARCHAR2 (30)               := 'Error';
   g_coa_processed            CONSTANT VARCHAR2 (30)           := 'Processed';
   --Error Types
   g_err_val                  CONSTANT VARCHAR2 (10)             := 'ERR_VAL';
   g_err_imp                  CONSTANT VARCHAR2 (10)             := 'ERR_IMP';
   g_err_int                  CONSTANT VARCHAR2 (10)             := 'ERR_INT';
   g_err_api                  CONSTANT VARCHAR2 (10)             := 'ERR_API';
   --Process Flags
   g_error                    CONSTANT VARCHAR2 (1)                    := 'E';
   g_complete                 CONSTANT VARCHAR2 (1)                    := 'C';
   g_new                      CONSTANT VARCHAR2 (1)                    := 'N';
   g_interface                CONSTANT VARCHAR2 (1)                    := 'P';
   g_valid                    CONSTANT VARCHAR2 (1)                    := 'V';
   g_waiting                  CONSTANT VARCHAR2 (1)                    := 'W';
   g_obsolete                 CONSTANT VARCHAR2 (1)                    := 'X';

-- ========================
-- Procedure: print_log_message
-- =============================================================================
--   This procedure is used to write message to log file.
-- =============================================================================
--  Input Parameters :
--    piv_error_message         : Message which needs to  be written in log file
--  Output Parameters :
--  Return     : Not applicable
-- -----------------------------------------------------------------------------
   PROCEDURE print_log_message (piv_error_message IN VARCHAR2)
   IS
   BEGIN
      xxetn_debug_pkg.add_debug (piv_debug_msg => piv_error_message);
   END;

-- ========================
-- Procedure: log_errors
-- =============================================================================
--   This procedure is used to write errors to error framework.
-- =============================================================================
--  Input Parameters :
--  piv_source_column_name  :  Source column
--  piv_source_column_value :  Source Column value
--  piv_error_type          :  Error type
--  piv_error_code          :  Error Code
--  piv_error_message       :  Error Message

   --  Output Parameters :
--  pov_return_status : Return Status
--  pov_error_msg     : Return Error
--  Return            : Not applicable
-- -----------------------------------------------------------------------------
   PROCEDURE log_errors (
      pov_return_status         OUT NOCOPY      VARCHAR2,
      pov_error_msg             OUT NOCOPY      VARCHAR2,
      piv_source_column_name    IN              xxetn_common_error.source_column_name%TYPE
            DEFAULT NULL,
      piv_source_column_value   IN              xxetn_common_error.source_column_value%TYPE
            DEFAULT NULL,
      piv_error_type            IN              xxetn_common_error.ERROR_TYPE%TYPE,
      piv_error_code            IN              xxetn_common_error.ERROR_CODE%TYPE,
      piv_error_message         IN              xxetn_common_error.error_message%TYPE
   )
   IS
      l_return_status     VARCHAR2 (1);
      l_error_message     VARCHAR2 (2000);
      process_exception   EXCEPTION;
   BEGIN
      xxetn_debug_pkg.add_debug ('p_err_msg: ' || piv_source_column_name);
      xxetn_debug_pkg.add_debug ('g_limit: ' || g_limit);
      xxetn_debug_pkg.add_debug ('g_indx: ' || g_indx);
      g_indx := g_indx + 1;
      l_source_tab (g_indx).source_table := g_source_table;
      l_source_tab (g_indx).interface_staging_id := g_intf_staging_id;
      l_source_tab (g_indx).source_keyname1 := NULL;
      l_source_tab (g_indx).source_keyvalue1 := NULL;
      l_source_tab (g_indx).source_keyname2 := NULL;
      l_source_tab (g_indx).source_keyvalue2 := NULL;
      l_source_tab (g_indx).source_keyname3 := NULL;
      l_source_tab (g_indx).source_keyvalue3 := NULL;
      l_source_tab (g_indx).source_keyname4 := NULL;
      l_source_tab (g_indx).source_keyvalue4 := NULL;
      l_source_tab (g_indx).source_keyname5 := NULL;
      l_source_tab (g_indx).source_keyvalue5 := NULL;
      l_source_tab (g_indx).source_column_name := piv_source_column_name;
      l_source_tab (g_indx).source_column_value := piv_source_column_value;
      l_source_tab (g_indx).ERROR_TYPE := piv_error_type;
      l_source_tab (g_indx).ERROR_CODE := piv_error_code;
      l_source_tab (g_indx).error_message := piv_error_message;

      IF MOD (g_indx, g_limit) = 0
      THEN
         xxetn_common_error_pkg.add_error
                                       (pov_return_status      => l_return_status,
                                        -- OUT
                                        pov_error_msg          => l_error_message,
                                        -- OUT
                                        pi_source_tab          => l_source_tab,
                                        -- IN  G_SOURCE_TAB_TYPE
                                        pin_batch_id           => g_new_batch_id
                                       );
         l_source_tab.DELETE;
         pov_return_status := l_return_status;
         pov_error_msg := l_error_message;
         print_log_message (   'Calling xxetn_common_error_pkg.add_error '
                            || l_return_status
                           );
         g_indx := 0;
      END IF;
   EXCEPTION
      WHEN OTHERS
      THEN
         g_retcode := 1;
         print_log_message
                      (   'Error: Exception occured in log_errors procedure '
                       || SUBSTR (SQLERRM, 1, 240)
                      );
   END log_errors;

-- =============================================================================
-- Procedure: log_bulk_errors
-- =============================================================================
--   Common procedure to insert bulk error records into common error table
-- =============================================================================
--  Input Parameters :
--    p_source_tab_type   : Error Table Type (xxetn_common_error_pkg.g_source_tab_type)
--  Output Parameters :
--    pov_return_status    :
--    pov_error_message    : Error message in case of any failure
-- -----------------------------------------------------------------------------
   PROCEDURE log_bulk_errors (
      p_source_tab_type   IN       xxetn_common_error_pkg.g_source_tab_type,
      pov_return_status   OUT      VARCHAR2,
      pov_error_msg       OUT      VARCHAR2
   )
   IS
      l_return_status   VARCHAR2 (1)    := NULL;
      l_error_message   VARCHAR2 (2000) := NULL;
   BEGIN
      --calling error log API
      xxetn_common_error_pkg.add_error (pov_return_status      => l_return_status,
                                        pov_error_msg          => l_error_message,
                                        pi_source_tab          => p_source_tab_type,
                                        pin_batch_id           => g_new_batch_id
                                       );
      pov_return_status := l_return_status;
      pov_error_msg := l_error_message;
   EXCEPTION
      WHEN OTHERS
      THEN
         g_retcode := 1;
         print_log_message
                 (   'Error: Exception occured in log_bulk_errors procedure '
                  || SUBSTR (SQLERRM, 1, 240)
                 );
   END log_bulk_errors;

--
-- ========================
-- Procedure: print_stat
-- =============================================================================
--   This procedure is used to print stats in different conversion modes
-- =============================================================================
--  Input Parameters :
--  pin_batch_id     : Batch Id
--  Output Parameters :
--  None
--  Return            : Not applicable
-- -----------------------------------------------------------------------------
--
   PROCEDURE print_stat (pin_batch_id IN NUMBER)
   IS
      l_pass_val1   NUMBER := 0;
      l_pass_val2   NUMBER := 0;
      l_pass_val3   NUMBER := 0;
      l_pass_val4   NUMBER := 0;
      l_rec_obs     NUMBER := 0;
      l_err_val     NUMBER := 0;
      l_err_val1    NUMBER := 0;
      l_err_val2    NUMBER := 0;
      l_tot_val     NUMBER := 0;
   BEGIN
      print_log_message (' + Print_stat + ');
      print_log_message ('Program Name : Eaton Open Claim Conversion Program');
      fnd_file.put_line (fnd_file.output,
                         'Program Name : Eaton Open Claim Conversion Program'
                        );
      fnd_file.put_line (fnd_file.output,
                         'Request ID   : ' || TO_CHAR (g_request_id)
                        );
      fnd_file.put_line (fnd_file.output,
                            'Report Date  : '
                         || TO_CHAR (SYSDATE, 'DD-MON-RRRR HH:MI:SS AM')
                        );
      fnd_file.put_line
         (fnd_file.output,
          '-------------------------------------------------------------------------------------------------'
         );
      fnd_file.put_line (fnd_file.output, 'Parameters  : ');
      fnd_file.put_line (fnd_file.output,
                         'Run Mode              : ' || g_run_mode
                        );
      fnd_file.put_line (fnd_file.output,
                         'Batch ID              : ' || g_batch_id
                        );
      fnd_file.put_line (fnd_file.output,
                         'Process records       : ' || g_process_records
                        );
      fnd_file.put_line (fnd_file.output,
                         'GL Date               : ' || g_gl_date
                        );
      fnd_file.put_line
         (fnd_file.output,
          '==================================================================================================='
         );
      fnd_file.put_line (fnd_file.output,
                         'Statistics (' || g_run_mode || '):');

      BEGIN
         --Count of total records
         SELECT COUNT (1)
           INTO l_tot_val
           FROM xxar_openclaim_stg xos
          WHERE xos.batch_id = NVL (pin_batch_id, xos.batch_id)
            AND xos.run_sequence_id = NVL (g_run_seq_id, xos.run_sequence_id);

         --Count of error records during validation
         SELECT COUNT (1)
           INTO l_err_val
           FROM xxar_openclaim_stg xos
          WHERE xos.batch_id = NVL (pin_batch_id, xos.batch_id)
            AND xos.run_sequence_id = NVL (g_run_seq_id, xos.run_sequence_id)
            AND xos.process_flag = g_error
            AND xos.ERROR_TYPE = g_err_val;

         --Count of error records during interface
         SELECT COUNT (1)
           INTO l_err_val1
           FROM xxar_openclaim_stg xos
          WHERE xos.batch_id = NVL (pin_batch_id, xos.batch_id)
            AND xos.run_sequence_id = NVL (g_run_seq_id, xos.run_sequence_id)
            AND xos.process_flag = g_error
            AND xos.ERROR_TYPE = g_err_int;

         --Count of error records during import
         SELECT COUNT (1)
           INTO l_err_val2
           FROM xxar_openclaim_stg xos
          WHERE xos.batch_id = NVL (pin_batch_id, xos.batch_id)
            AND xos.run_sequence_id = NVL (g_run_seq_id, xos.run_sequence_id)
            AND xos.process_flag = g_error
            AND xos.ERROR_TYPE = g_err_imp;

         --Count of records in obsolete status
         SELECT COUNT (1)
           INTO l_rec_obs
           FROM xxar_openclaim_stg xos
          WHERE xos.batch_id = NVL (pin_batch_id, xos.batch_id)
            AND xos.run_sequence_id = NVL (g_run_seq_id, xos.run_sequence_id)
            AND xos.process_flag = g_obsolete;

         --Count of Valid records
         SELECT COUNT (1)
           INTO l_pass_val1
           FROM xxar_openclaim_stg xos
          WHERE xos.batch_id = NVL (pin_batch_id, xos.batch_id)
            AND xos.run_sequence_id = NVL (g_run_seq_id, xos.run_sequence_id)
            AND xos.process_flag IN (g_valid);

         --count of interface records
         SELECT COUNT (1)
           INTO l_pass_val2
           FROM xxar_openclaim_stg xos
          WHERE xos.batch_id = NVL (pin_batch_id, xos.batch_id)
            AND xos.run_sequence_id = NVL (g_run_seq_id, xos.run_sequence_id)
            AND xos.process_flag IN (g_interface);

         --count of records waiting for receipt creation
         SELECT COUNT (1)
           INTO l_pass_val3
           FROM xxar_openclaim_stg xos
          WHERE xos.batch_id = NVL (pin_batch_id, xos.batch_id)
            AND xos.run_sequence_id = NVL (g_run_seq_id, xos.run_sequence_id)
            AND xos.process_flag IN (g_waiting);

         --count of completed records
         SELECT COUNT (1)
           INTO l_pass_val4
           FROM xxar_openclaim_stg xos
          WHERE xos.batch_id = NVL (pin_batch_id, xos.batch_id)
            AND xos.run_sequence_id = NVL (g_run_seq_id, xos.run_sequence_id)
            AND xos.process_flag IN (g_complete);
      EXCEPTION
         WHEN OTHERS
         THEN
            g_retcode := 2;
            print_log_message ('In When Others of Print Stat sqls ' || SQLERRM
                              );
      END;

      --Printing stats in output file
      fnd_file.put_line (fnd_file.output,
                            'Records Submitted                     : '
                         || l_tot_val
                        );
      fnd_file.put_line (fnd_file.output,
                            'Records Validated                     : '
                         || l_pass_val1
                        );
      fnd_file.put_line (fnd_file.output,
                            'Records Errored During Validation     : '
                         || l_err_val
                        );
      fnd_file.put_line (fnd_file.output,
                            'Records Interfaced                    : '
                         || l_pass_val2
                        );
      fnd_file.put_line (fnd_file.output,
                            'Records Errored During interface      : '
                         || l_err_val1
                        );
      fnd_file.put_line (fnd_file.output,
                            'Records Waiting Receipt Creation      : '
                         || l_pass_val3
                        );
      fnd_file.put_line (fnd_file.output,
                            'Records Errored During import         : '
                         || l_err_val2
                        );
      fnd_file.put_line (fnd_file.output,
                            'Records Obsoleted                     : '
                         || l_rec_obs
                        );
      fnd_file.put_line (fnd_file.output,
                            'Records Completed                     : '
                         || l_pass_val4
                        );
      fnd_file.put_line (fnd_file.output, CHR (10));
      fnd_file.put_line
         (fnd_file.output,
          '==================================================================================================='
         );
      print_log_message (' - Print_stat - ');
   EXCEPTION
      WHEN OTHERS
      THEN
         g_retcode := 2;
         print_log_message ('In When Others of Print Stat ' || SQLERRM);
   END print_stat;

--283417 changes starts
-- ========================
-- Procedure: get_batch_source
-- =============================================================================
--   This procedure is to get batch source
-- =============================================================================
--  Input Parameters :
--   piv_eaton_site      : Eaton Ledger/Site(segment1 of COA)

   --  Output Parameters :
--  pov_batch_source : Derived Batch Source
--  pov_status    : Return Status
-- -----------------------------------------------------------------------------
--
   PROCEDURE get_batch_source (
      piv_eaton_site     IN       VARCHAR2,
      pov_batch_source   OUT      VARCHAR2,
      pov_site_credit_off OUT VARCHAR2,
      pov_status         OUT      VARCHAR2,
      pov_err_message    OUT      VARCHAR
   )
   IS

   BEGIN
      pov_status := 'S';
      pov_batch_source := NULL;
      pov_site_credit_off := NULL;
      pov_err_message := NULL;

      SELECT NVL (ar_credit_office, site) , site
           --site  -- v1.7.3 commented the line above and considering only the site column
        INTO pov_batch_source, pov_site_credit_off
        FROM xxetn_map_unit_v   -- v1.13
       WHERE site = piv_eaton_site;

       -- pov_batch_source :=  'CONVERSION ' || pov_batch_source;  -- commented for v1.10
       pov_batch_source :=  'CNV CLAIM ' || pov_batch_source;       -- addded for v1.10

   EXCEPTION
      WHEN OTHERS
      THEN
         pov_status := 'F';
         pov_err_message :=
               'Exception when deriving Batch Source from XXETN_MAP_UNIT_V for Eaton Site/Ledger :'
            || piv_eaton_site
            || ':, '
            || SUBSTR (SQLERRM, 1, 100);
   END get_batch_source;

   --283417 changes ends
--
-- ========================
-- Procedure: validate_batch_source
-- =============================================================================
--   This procedure is to validate batch source
-- =============================================================================
--  Input Parameters :
--   pin_org_id      : Operating Unit Id

   --  Output Parameters :
--  pon_error_cnt    : Return Status
-- -----------------------------------------------------------------------------
--
   PROCEDURE validate_batch_source (
      pin_org_id         IN       NUMBER,
      piv_batch_source   IN       VARCHAR2,
      pon_error_cnt      OUT      NUMBER
   )
   IS
      l_record_cnt   NUMBER;
   BEGIN
      print_log_message (   '   PROCEDURE : validate_batch_source = '
                         || piv_batch_source
                        );
      print_log_message (' Org Id = ' || pin_org_id);
      l_record_cnt := 0;

      BEGIN
         SELECT 1
           INTO l_record_cnt
           FROM ra_batch_sources_all rbsa
          WHERE rbsa.NAME = piv_batch_source
            AND rbsa.status = 'A'
            AND rbsa.org_id = pin_org_id;
      EXCEPTION
         WHEN NO_DATA_FOUND
         THEN
            l_record_cnt := 2;
            print_log_message (   'In No Data found of batch source check-'
                               || piv_batch_source
                               || SQLERRM
                              );
         WHEN OTHERS
         THEN
            l_record_cnt := 2;
            print_log_message (   'In When others of batch source check-'
                               || piv_batch_source
                               || SQLERRM
                              );
      END;

      IF l_record_cnt > 1
      THEN
         pon_error_cnt := 2;
      END IF;
   EXCEPTION
      WHEN OTHERS
      THEN
         g_retcode := 2;
         pon_error_cnt := 2;
         print_log_message (   'In Exception validate batch source-'
                            || piv_batch_source
                            || SQLERRM
                           );
   END validate_batch_source;

--v1.1 Changes start
--
-- ========================
-- Procedure: VALIDATE_ACCOUNTS
-- =============================================================================
--   This procedure validates all
--   the account related information
-- =============================================================================
   PROCEDURE validate_accounts (
      p_in_seg1       IN       VARCHAR2,
      p_in_seg2       IN       VARCHAR2,
      p_in_seg3       IN       VARCHAR2,
      p_in_seg4       IN       VARCHAR2,
      p_in_seg5       IN       VARCHAR2,
      p_in_seg6       IN       VARCHAR2,
      p_in_seg7       IN       VARCHAR2,
      p_in_leg_op_unit IN      VARCHAR2, --v1.8 added
      p_in_header_attr1 IN     VARCHAR2, --v1.8 added
      p_in_leg_account_class IN VARCHAR2, --v1.8 added
      p_in_cust_type  IN       VARCHAR2, --v1.8 added
      p_in_cust_id    IN       NUMBER, --v1.16 added
      x_out_acc       OUT      xxetn_common_pkg.g_rec_type,
      x_out_ccid      OUT      NUMBER,
      pon_error_cnt   OUT      NUMBER,
      pov_err_msg     OUT      VARCHAR2
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
      l_record_cnt       NUMBER;
      --v1.8 changes start
      l_in_seg1 VARCHAR2(240);
      l_in_seg3 VARCHAR2(240);
      --v1.8 changes end
    --v1.16 changes start
      l_in_seg6 VARCHAR2(240);
      l_in_seg7 VARCHAR2(240);
      --v1.16 changes end
   BEGIN
      x_out_acc := NULL;
      x_out_ccid := NULL;
      pov_err_msg := NULL;
      l_in_seg1 :=  NULL;
      l_in_seg3 :=  NULL;
    l_in_seg6 :=  NULL; --v1.16
    l_in_seg7 :=  NULL; --v1.16

    --v1.8 changes start
      l_in_seg1 := p_in_seg1 ;
      l_in_seg3 := p_in_seg3 ;

      IF NVL(p_in_leg_op_unit,'-XXX') IN ('OU US AR', 'OU CA AR') THEN
           IF p_in_leg_account_class = 'REC' or p_in_leg_account_class = 'REV' --v1.16 added the condition for REV line 
       THEN  --that means its REC line
              l_in_seg1 := SUBSTR(p_in_header_attr1, -4);
           END IF;
      
           --IF l_in_seg3 = '11410' THEN --v1.26 commented for Defect#12864
		   IF l_in_seg3 IN ('11410', '11411') THEN --v1.26 Added for Defect#12864
              IF NVL(p_in_cust_type, 'R') = 'I' THEN
			     --Intercompany customer
                 l_in_seg3 := '15310';
              ELSIF NVL(p_in_cust_type, 'R') = 'R' THEN
			     -- Trade customer
                 l_in_seg3 := '11411';
              END IF;
           END IF;
       
       --v1.16 changes start
       IF NVL(p_in_cust_type, 'R') = 'I' AND p_in_cust_id IS NOT NULL THEN 
          
        -- deriving segment7 from first 4 digit of account name --
        SELECT substr(account_name,1,4)
        INTO  l_in_seg7
        FROM apps.hz_cust_accounts_all
        WHERE 1=1
        AND  cust_account_id = p_in_cust_id ; 
        
        -- deriving segment6 from ETN Map unit --
        
        BEGIN
        SELECT le_number
        INTO l_in_seg6
        FROM apps.xxetn_map_unit_v
        WHERE 1=1
        AND  site = l_in_seg7 ; 
        
        EXCEPTION 
          WHEN OTHERS THEN 
        l_record_cnt := 2;
                pov_err_msg :=
                       'Error : While deriving LE_NUMBER for IC Customer for SITE :' || l_in_seg6;
                print_log_message (pov_err_msg);
        
        END ; 
        --v1.16 changes end
       
       END IF ; 
       
      END IF;
      --v1.8 changes end
    
    

      xxetn_debug_pkg.add_debug
                      (piv_debug_msg      => 'Validate accounts procedure called ');

      l_in_rec.segment1 := l_in_seg1; --v1.8 added
      l_in_rec.segment2 := p_in_seg2;
      l_in_rec.segment3 := l_in_seg3; --v1.8 added
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
     
         --v1.16 changes start
         
             --l_in_seg_rec.segment6 := x_out_rec.segment6;
             --l_in_seg_rec.segment7 := x_out_rec.segment7;
         
         --IF NVL(p_in_cust_type, 'R') = 'I' THEN  commented for v1.23 as it should implemented only for US and CA
		 -- added below condition of OU US AR AND OU CA AR for v1.23
		 IF (NVL(p_in_cust_type, 'R') = 'I' AND (NVL(p_in_leg_op_unit,'-XXX') IN ('OU US AR', 'OU CA AR'))) THEN
           l_in_seg_rec.segment6 := l_in_seg6;
           l_in_seg_rec.segment7 := l_in_seg7;
         ELSE
           l_in_seg_rec.segment6 := x_out_rec.segment6;
           l_in_seg_rec.segment7 := x_out_rec.segment7;
         END IF;
         
         --v1.16 changes end 
     
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
            --v1.16 changes start
          
            -- x_out_acc.segment6 := x_out_rec.segment6;
            -- x_out_acc.segment7 := x_out_rec.segment7;
     
       -- IF NVL(p_in_cust_type, 'R') = 'I' THEN commented for v1.23 as it should implemented only for US and CA
		 -- added below condition of OU US AR AND OU CA AR for v1.23
		 IF (NVL(p_in_cust_type, 'R') = 'I' AND (NVL(p_in_leg_op_unit,'-XXX') IN ('OU US AR', 'OU CA AR'))) THEN
          x_out_acc.segment6 := l_in_seg6;
              x_out_acc.segment7 := l_in_seg7;
        ELSE
          x_out_acc.segment6 := x_out_rec.segment6;
              x_out_acc.segment7 := x_out_rec.segment7;
        END IF;
        
        --v1.16 changes end 
            
            x_out_acc.segment8 := x_out_rec.segment8;
            x_out_acc.segment9 := x_out_rec.segment9;
            x_out_acc.segment10 := x_out_rec.segment10;
            x_out_ccid := x_ccid;
            print_log_message ('Account information successfully derived ');
         ELSE
            l_record_cnt := 2;
            pov_err_msg :=
                   'Error : Following error in COA transformation :' || x_err;
            print_log_message (pov_err_msg);
         END IF;
      ELSIF x_status = g_coa_error
      THEN
         l_record_cnt := 2;
         pov_err_msg :=
                   'Error : Following error in COA transformation :' || x_msg;
         print_log_message (pov_err_msg);
      END IF;

      xxetn_debug_pkg.add_debug
                         (piv_debug_msg      => 'Validate accounts procedure ends ');

      IF l_record_cnt > 1
      THEN
         pon_error_cnt := 2;
      END IF;
   EXCEPTION
      WHEN OTHERS
      THEN
         g_retcode := 2;
         pon_error_cnt := 2;
         print_log_message ('In Exception validate coa accounts ' || SQLERRM);
   END validate_accounts;

--v1.1 Changes end

   --
-- ========================
-- Procedure: validate_gl_period
-- =============================================================================
--   This procedure validate gl period
-- =============================================================================
--  Input Parameters :
--  pin_sob_id       : Set of book id

   --  Output Parameters :
--  pon_error_cnt    : Return Status
-- -----------------------------------------------------------------------------
--
   PROCEDURE validate_gl_period (pin_sob_id IN NUMBER, pon_error_cnt OUT NUMBER)
   IS
      l_record_cnt   NUMBER;
   BEGIN
      print_log_message ('   PROCEDURE : validate_gl_period ' || g_gl_date);
      l_record_cnt := 0;

      BEGIN
         SELECT 1
           INTO l_record_cnt
           FROM gl_period_statuses gps,
                fnd_application fa,
                gl_ledgers gl
          WHERE gl.accounted_period_type = gps.period_type
            AND gl.ledger_id = gps.ledger_id
            AND fa.application_short_name = 'SQLGL'
            AND fa.application_id = gps.application_id
            AND gps.set_of_books_id = pin_sob_id
            AND gps.closing_status = 'O'
            AND gps.adjustment_period_flag = g_new
            AND g_gl_date BETWEEN gps.start_date AND gps.end_date;
      EXCEPTION
         WHEN NO_DATA_FOUND
         THEN
            l_record_cnt := 2;
            print_log_message ('In No Data found of gl period check'
                               || SQLERRM
                              );
         WHEN OTHERS
         THEN
            l_record_cnt := 2;
            print_log_message ('In When others of gl period check' || SQLERRM);
      END;

      IF l_record_cnt > 1
      THEN
         pon_error_cnt := 2;
      END IF;
   EXCEPTION
      WHEN OTHERS
      THEN
         g_retcode := 2;
         pon_error_cnt := 2;
         print_log_message ('In Exception validate gl period' || SQLERRM);
   END validate_gl_period;

--
-- ========================
-- Procedure: validate_ar_period
-- =============================================================================
--   This procedure validate ar period
-- =============================================================================
--  Input Parameters :
--  pin_sob_id       : Set of book id

   --  Output Parameters :
--  pon_error_cnt    : Return Status
-- -----------------------------------------------------------------------------
--
   PROCEDURE validate_ar_period (pin_sob_id IN NUMBER, pon_error_cnt OUT NUMBER)
   IS
      l_record_cnt   NUMBER;
   BEGIN
      print_log_message ('   PROCEDURE : validate_ar_period ' || g_gl_date);
      l_record_cnt := 0;

      BEGIN
         SELECT 1
           INTO l_record_cnt
           FROM gl_period_statuses gps,
                fnd_application fa,
                gl_ledgers gl
          WHERE gl.accounted_period_type = gps.period_type
            AND gl.ledger_id = gps.ledger_id
            AND fa.application_short_name = 'AR'
            AND fa.application_id = gps.application_id
            AND gps.set_of_books_id = pin_sob_id
            AND gps.closing_status = 'O'
            AND gps.adjustment_period_flag = g_new
            AND g_gl_date BETWEEN gps.start_date AND gps.end_date;
      EXCEPTION
         WHEN NO_DATA_FOUND
         THEN
            l_record_cnt := 2;
            print_log_message ('In No Data found of ar period check'
                               || SQLERRM
                              );
         WHEN OTHERS
         THEN
            l_record_cnt := 2;
            print_log_message ('In When others of ar period check' || SQLERRM);
      END;

      IF l_record_cnt > 1
      THEN
         pon_error_cnt := 2;
      END IF;
   EXCEPTION
      WHEN OTHERS
      THEN
         g_retcode := 2;
         pon_error_cnt := 2;
         print_log_message ('In Exception validate ar period' || SQLERRM);
   END validate_ar_period;

--
-- ========================
-- Procedure: validate_reason_code
-- =============================================================================
--   This procedure validate reason code
-- =============================================================================
--  Input Parameters :
--  piv_reason_code  : 11i reason code
--  pin_org_id       : Org id
--  Output Parameters :
--  pov_reason_code  : R12 reason code
--  pon_error_cnt    : Return Status
-- -----------------------------------------------------------------------------
--
   PROCEDURE validate_reason_code (
      piv_reason_code   IN       VARCHAR2,
      pin_org_id        IN       NUMBER,
      pov_reason_code   OUT      VARCHAR2,
      pon_error_cnt     OUT      NUMBER,
      pov_error_msg     OUT      VARCHAR2
   )
   IS
      l_reason_code           ozf_reason_codes_all_tl.reason_code_id%TYPE;
      l_reason_code_lkp       fnd_lookup_values.description%TYPE;
      l_reason_lkp   CONSTANT VARCHAR2 (240) := 'ETN_OTC_REASON_CODE_MAPPING';
   BEGIN
      print_log_message (   '   PROCEDURE : validate_reason_code = '
                         || piv_reason_code
                        );
      print_log_message ('   PROCEDURE : Org Id               = '
                         || pin_org_id
                        );
      l_reason_code_lkp := NULL;
      l_reason_code := NULL;
      pov_error_msg := NULL;

    
      BEGIN
         SELECT flv.description
           INTO l_reason_code_lkp
           FROM fnd_lookup_values flv
          WHERE flv.lookup_type = l_reason_lkp
            AND flv.enabled_flag = 'Y'
--v1.1 Changes start
--   AND    TRIM(flv.meaning) = piv_reason_code
            AND UPPER (TRIM (flv.meaning)) =
                                      UPPER (NVL (piv_reason_code, 'DEFAULT'))
--v1.1 Changes end
            AND flv.LANGUAGE = USERENV ('LANG')
            AND TRUNC (SYSDATE) BETWEEN NVL (flv.start_date_active,
                                             TRUNC (SYSDATE)
                                            )
                                    AND NVL (flv.end_date_active,
                                             TRUNC (SYSDATE)
                                            );
      
    EXCEPTION
         WHEN NO_DATA_FOUND
         THEN
            l_reason_code_lkp := NULL;
            print_log_message
                           (   'In No Data found of reason code lookup check'
                            || SQLERRM
                           );
            pov_error_msg :=
                  'Unable to derive R12 reason code from reason code Lookup';
         WHEN OTHERS
         THEN
            l_reason_code_lkp := NULL;
            print_log_message
                             (   'In When others of reason code lookup check'
                              || SQLERRM
                             );
            pov_error_msg :=
                  'Not able to derive R12 reason code from reason code Lookup';
      END;

      print_log_message ('Reason Code from lookup = ' || l_reason_code_lkp);

      IF l_reason_code_lkp IS NOT NULL
      THEN
         BEGIN
            SELECT orcat.reason_code_id
              INTO l_reason_code
              FROM ozf_reason_codes_all_tl orcat
             WHERE orcat.NAME = l_reason_code_lkp
               AND orcat.org_id = pin_org_id
               AND orcat.LANGUAGE = USERENV ('LANG');
         EXCEPTION
            WHEN NO_DATA_FOUND
            THEN
               l_reason_code := NULL;
               print_log_message
                           (   'In No Data found of reason code lookup check'
                            || SQLERRM
                           );
               pov_error_msg := 'Error: Reason code is not valid';
            WHEN OTHERS
            THEN
               l_reason_code := NULL;
               print_log_message
                             (   'In When others of reason code lookup check'
                              || SQLERRM
                             );
               pov_error_msg := 'Error: Reason code is not valid';
         END;
      END IF;

      pov_reason_code := TO_CHAR (l_reason_code);
      print_log_message ('  R12 Reason Code = ' || pov_reason_code);

      IF l_reason_code IS NULL
      THEN
         pon_error_cnt := 2;
      END IF;
   EXCEPTION
      WHEN OTHERS
      THEN
         g_retcode := 2;
         pon_error_cnt := 2;
         print_log_message ('In Exception validate reason code' || SQLERRM);
   END validate_reason_code;

--
-- ========================
-- Procedure: validate_currency_code
-- =============================================================================
--   This procedure to validate currency code
-- =============================================================================
--  Input Parameters :
--   piv_currency_code : Currency Code
--  Output Parameters  :
--  pon_error_cnt      : Return Status
-- -----------------------------------------------------------------------------
--
   PROCEDURE validate_currency_code (
      piv_currency_code   IN       VARCHAR2,
      pon_error_cnt       OUT      NUMBER
   )
   IS
      l_record_cnt   NUMBER;
   BEGIN
      print_log_message (   '   PROCEDURE : validate_currency_code = '
                         || piv_currency_code
                        );
      l_record_cnt := 0;

      BEGIN
         SELECT 1
           INTO l_record_cnt
           FROM fnd_currencies fc
          WHERE fc.currency_code = piv_currency_code
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
      EXCEPTION
         WHEN NO_DATA_FOUND
         THEN
            l_record_cnt := 2;
            print_log_message (   'In No Data found of currency code check'
                               || SQLERRM
                              );
         WHEN OTHERS
         THEN
            l_record_cnt := 2;
            print_log_message (   'In When others of currency code check'
                               || SQLERRM
                              );
      END;

      IF l_record_cnt > 1
      THEN
         pon_error_cnt := 2;
      END IF;
   EXCEPTION
      WHEN OTHERS
      THEN
         g_retcode := 2;
         pon_error_cnt := 2;
         print_log_message ('In Exception validate currency code' || SQLERRM);
   END validate_currency_code;

--
-- ========================
-- Procedure: get_ledger_currency
-- =============================================================================
--   This procedure to get ledger currency
-- =============================================================================
--  Input Parameters :
--  pin_sob_id      : Set of book id
--  Output Parameters   :
--  piv_ledger_currency : Ledger Currency
--  pon_error_cnt       : Return Status
-- -----------------------------------------------------------------------------
--
   PROCEDURE get_ledger_currency (
      pin_sob_id            IN       NUMBER,
      piv_ledger_currency   OUT      VARCHAR2,
      pon_error_cnt         OUT      NUMBER
   )
   IS
      l_curr_code    gl_ledgers.currency_code%TYPE;
      l_record_cnt   NUMBER;
   BEGIN
      print_log_message ('   PROCEDURE : get_ledger_currency = ' || pin_sob_id
                        );
      l_record_cnt := 0;
      l_curr_code := NULL;

      BEGIN
         SELECT gl.currency_code
           INTO l_curr_code
           FROM gl_ledgers gl,
                fnd_currencies fc
          WHERE fc.currency_code = gl.currency_code
            AND fc.enabled_flag = 'Y'
            AND fc.currency_flag = 'Y'
            AND TRUNC (SYSDATE) BETWEEN TRUNC (NVL (fc.start_date_active,
                                                    SYSDATE
                                                   )
                                              )
                                    AND TRUNC (NVL (fc.end_date_active,
                                                    SYSDATE
                                                   )
                                              )
            AND gl.ledger_id = pin_sob_id;
      EXCEPTION
         WHEN NO_DATA_FOUND
         THEN
            l_record_cnt := 2;
            l_curr_code := NULL;
            print_log_message
                         (   'In No Data found of ledger currency code check'
                          || SQLERRM
                         );
         WHEN OTHERS
         THEN
            l_record_cnt := 2;
            l_curr_code := NULL;
            print_log_message
                           (   'In When others of ledger currency code check'
                            || SQLERRM
                           );
      END;

      piv_ledger_currency := l_curr_code;
      print_log_message (' Ledger Currency Code:= ' || piv_ledger_currency);

      IF l_record_cnt > 1
      THEN
         pon_error_cnt := 2;
      END IF;
   EXCEPTION
      WHEN OTHERS
      THEN
         g_retcode := 2;
         pon_error_cnt := 2;
         print_log_message (   'In Exception validate ledger currency code'
                            || SQLERRM
                           );
   END get_ledger_currency;

--
-- ========================
-- Procedure: validate_receipt_method
-- =============================================================================
--   This procedure to validate receipt method
-- =============================================================================
--  Input Parameters :
--   piv_receipt_method : 11i receipt method
--   pid_receipt_date   : Receipt date

   --  Output Parameters :
--  pov_receipt_method : R12 receipt method
--  pon_method_id      : Receipt method id
--  pon_error_cnt      : Return Status
-- -----------------------------------------------------------------------------
--
   PROCEDURE validate_receipt_method (
      piv_receipt_method   IN       VARCHAR2,
      pid_receipt_date     IN       DATE,
      pov_receipt_method   OUT      VARCHAR2,
      pon_method_id        OUT      NUMBER,
      pon_error_cnt        OUT      NUMBER,
      pov_error_msg        OUT      VARCHAR2
   )
   IS
      l_record_cnt             NUMBER;
      l_receipt_method         fnd_lookup_values.description%TYPE;
      l_method_id              NUMBER;
      l_receipt_lkp   CONSTANT VARCHAR2 (30)       := 'ETN_AR_RECEIPT_METHOD';
   BEGIN
      print_log_message (   '   PROCEDURE : validate_receipt_method = '
                         || piv_receipt_method
                        );
      l_record_cnt := 0;
      l_receipt_method := NULL;
      l_method_id := NULL;
      pov_error_msg := NULL;

      BEGIN
         SELECT flv.description
           INTO l_receipt_method
           FROM fnd_lookup_values flv
          WHERE flv.lookup_type = l_receipt_lkp
            AND flv.enabled_flag = 'Y'
            AND TRIM (flv.meaning) = piv_receipt_method
            AND flv.LANGUAGE = USERENV ('LANG')
            AND TRUNC (SYSDATE) BETWEEN NVL (flv.start_date_active,
                                             TRUNC (SYSDATE)
                                            )
                                    AND NVL (flv.end_date_active,
                                             TRUNC (SYSDATE)
                                            );
      EXCEPTION
         WHEN NO_DATA_FOUND
         THEN
            l_record_cnt := 2;
            l_receipt_method := NULL;
            print_log_message
                        (   'In No Data found of receipt method lookup check'
                         || SQLERRM
                        );
            pov_error_msg :=
               'Not able to derive R12 receipt method from receipt method Lookup';
         WHEN OTHERS
         THEN
            l_record_cnt := 2;
            l_receipt_method := NULL;
            print_log_message
                          (   'In When others of receipt method lookup check'
                           || SQLERRM
                          );
            pov_error_msg :=
               'Not able to derive R12 receipt method from receipt method Lookup';
      END;

      IF l_receipt_method IS NOT NULL
      THEN
         BEGIN
            SELECT arm.receipt_method_id
              INTO l_method_id
              FROM ar_receipt_methods arm
             WHERE arm.NAME = l_receipt_method
               AND TRUNC (pid_receipt_date) BETWEEN NVL (arm.start_date,
                                                         TRUNC (SYSDATE)
                                                        )
                                                AND NVL (arm.end_date,
                                                         TRUNC (SYSDATE)
                                                        );
         EXCEPTION
            WHEN NO_DATA_FOUND
            THEN
               l_record_cnt := 2;
               l_method_id := NULL;
               print_log_message
                            (   'In No Data found of receipt method id check'
                             || SQLERRM
                            );
               pov_error_msg := 'Error: Receipt Method is not valid';
            WHEN OTHERS
            THEN
               l_record_cnt := 2;
               l_method_id := NULL;
               print_log_message
                              (   'In When others of receipt method id check'
                               || SQLERRM
                              );
               pov_error_msg := 'Error: Receipt Method is not valid';
         END;
      END IF;

      print_log_message ('Receipt Method = ' || l_receipt_method);
      print_log_message ('Method id = ' || l_method_id);
      pov_receipt_method := l_receipt_method;
      pon_method_id := l_method_id;

      IF l_record_cnt > 1
      THEN
         pon_error_cnt := 2;
      END IF;
   EXCEPTION
      WHEN OTHERS
      THEN
         g_retcode := 2;
         pon_error_cnt := 2;
         print_log_message ('In Exception validate receipt method' || SQLERRM);
   END validate_receipt_method;

--
-- ========================
-- Procedure: validate_customer_info
-- =============================================================================
--   This procedure to validate customer info
-- =============================================================================
--  Input Parameters :
--  piv_cust_num     : Legacy customer Number
--  piv_bill_to_code : Legacy Bill to code
--  piv_ship_to_code : Legacy Ship to code
--  pin_org_id       : Org Id

   --  Output Parameters :
-- pon_cust_id       : R12 customer id
-- pon_bill_to_id    : R12 bill to id
-- pon_ship_to_id    : R12 ship to id
-- pon_error_cnt    : Return Status
-- -----------------------------------------------------------------------------
--
   PROCEDURE validate_customer_info (
      piv_cust_num       IN       VARCHAR2,
      piv_bill_to_code   IN       VARCHAR2,
      piv_ship_to_code   IN       VARCHAR2,
      pin_org_id         IN       NUMBER,
      piv_operating_unit IN       VARCHAR2,  --v1.9 added
      piv_leg_op_unit    IN       VARCHAR2,  --v1.9 added
      piv_leg_source_system IN      VARCHAR2,  --v1.9 added
      pon_cust_id        OUT      NUMBER,
      pon_bill_to_id     OUT      NUMBER,
      pon_ship_to_id     OUT      NUMBER,
      pov_customer_type  OUT      VARCHAR2,  --v1.8 added
      pon_error_cnt      OUT      NUMBER
   )
   IS
      l_plant        VARCHAR2 (100);  -- v1.10
      l_record_cnt   NUMBER;
      l_cust_id      NUMBER;
      l_bill_to_id   NUMBER;
      l_ship_to_id   NUMBER;
      l_customer_type apps.hz_cust_accounts_all.customer_type%TYPE; --v1.8 added

      l_leg_bill_to_addr VARCHAR2(240);  --v1.9 added
      l_leg_orig_sys_ref VARCHAR2(240);  --v1.9 added

   BEGIN
      print_log_message ('+   PROCEDURE : validate_customer_info +');
      print_log_message (' Legacy Cusotmer Number = ' || piv_cust_num);
      print_log_message (' Legacy Bill To Number  = ' || piv_bill_to_code);
      print_log_message (' Legacy Ship To Number  = ' || piv_ship_to_code);
      l_record_cnt := 0;
      l_cust_id := NULL;
      l_bill_to_id := NULL;
      l_ship_to_id := NULL;
      l_customer_type := NULL ; --v1.8 added

    l_leg_bill_to_addr := NULL; --v1.9 added
    l_leg_orig_sys_ref := NULL; --v1.9 added

    BEGIN
      print_log_message (' Fetch R12 customer detail from cross ref');

      l_leg_bill_to_addr := TRIM(SUBSTR(piv_bill_to_code,1,(INSTR(piv_bill_to_code,'|')-1)));  --v1.9 added
      l_leg_orig_sys_ref := TRIM(SUBSTR(piv_bill_to_code,(INSTR(piv_bill_to_code,'|')+1)));    --v1.9 added

      print_log_message (' l_leg_bill_to_addr - ' || l_leg_bill_to_addr );   
      print_log_message (' l_leg_orig_sys_ref - ' || l_leg_orig_sys_ref );        

     /*
         SELECT hca.cust_account_id
   INTO   l_cust_id
         FROM   hz_cust_accounts_all hca
         WHERE  hca.account_number = piv_cust_num
         AND    hca.status = 'A';
*/

      /** Added for v1.10 **/
      l_plant := NULL;
      BEGIN
         SELECT meaning
         INTO l_plant
         FROM fnd_lookup_values
         WHERE lookup_type = 'XXAR_CUST_CNV_BR_OU_MAP'
         AND enabled_flag = 'Y'
         AND UPPER(description) = UPPER(piv_operating_unit)
         AND TRUNC (SYSDATE) BETWEEN NVL (start_date_active, SYSDATE - 1)
         AND NVL (end_date_active, SYSDATE + 1 )
         AND LANGUAGE = USERENV ('LANG');
      EXCEPTION
         WHEN OTHERS THEN
            print_log_message ('When Others of verifying plant in lookup XXAR_CUST_CNV_BR_OU_MAP. Msg: ' || SQLERRM );
      END;


      --IF UPPER(piv_operating_unit) = UPPER('Eaton EPS OU') THEN  --v1.9 added
      IF NVL(l_plant,'XXX') = '4470' THEN  --v1.10 added

           SELECT DISTINCT hca.cust_account_id
                       , hca.customer_type --v1.8 added
                       INTO l_cust_id
                       , l_customer_type --v1.8 added
                    FROM apps.hz_cust_accounts_all hca,
                         apps.hz_cust_acct_sites_all hcas,
                         apps.hz_cust_site_uses_all hcsu
                   WHERE hcsu.cust_acct_site_id = hcas.cust_acct_site_id
                     AND hcas.cust_account_id = hca.cust_account_id
                     --AND hcsu.LOCATION = TRIM (piv_bill_to_code) --v1.9 commented
           AND hcsu.LOCATION = TRIM (l_leg_bill_to_addr) --v1.9 added
                     AND NVL (hcas.org_id, 1) = NVL (pin_org_id, 1)
                     AND hca.status = 'A'
                     AND hcsu.status = 'A'
                     AND hcas.status = 'A'
                     AND hcsu.site_use_code = 'BILL_TO'
                     --AND hca.orig_system_reference LIKE '%' || (TRIM (piv_cust_num)) || '%'  --v1.17 Commented for removing reference to leg_customer_number
                     AND hcas.orig_system_reference =   'EPS.'||REGEXP_SUBSTR (l_leg_orig_sys_ref ,'[^."]+',6); --v1.9 added

         ELSIF piv_leg_source_system = 'NAFSC' AND UPPER(piv_leg_op_unit)  NOT IN ('OU USD 1775 TCO', 'OU MXN CORP') THEN --v1.9 added
           SELECT DISTINCT hca.cust_account_id
                       , hca.customer_type --v1.8 added
                       INTO l_cust_id
                       , l_customer_type --v1.8 added
                    FROM apps.hz_cust_accounts_all hca,
                         apps.hz_cust_acct_sites_all hcas
                         --apps.hz_cust_site_uses_all hcsu   -- commented for v1.10
                   --WHERE hcsu.cust_acct_site_id = hcas.cust_acct_site_id   -- commented for v1.10
                     WHERE hcas.cust_account_id = hca.cust_account_id
                     --AND hcsu.LOCATION = TRIM (piv_bill_to_code) --v1.9 commented
                     --AND hcsu.LOCATION = TRIM (l_leg_bill_to_addr) --v1.9 added, commented for v1.10
                     AND hcas.orig_system_reference = l_leg_bill_to_addr  -- added for v1.10
                     AND NVL (hcas.org_id, 1) = NVL (pin_org_id, 1)
                     AND hca.status = 'A'
                     --AND hcsu.status = 'A' -- commented for v1.10
                     AND hcas.status = 'A';
                     --AND hcsu.site_use_code = 'BILL_TO'  -- commented for v1.10
                     --AND hca.orig_system_reference LIKE
                       --                     '%' || (TRIM (piv_cust_num))
                         --                   || '%'           
           --;
     ELSE 
       SELECT DISTINCT hca.cust_account_id
                       , hca.customer_type --v1.8 added
                       INTO l_cust_id
                       , l_customer_type --v1.8 added
                    FROM apps.hz_cust_accounts_all hca,
                         apps.hz_cust_acct_sites_all hcas,
                         apps.hz_cust_site_uses_all hcsu
                   WHERE hcsu.cust_acct_site_id = hcas.cust_acct_site_id
                     AND hcas.cust_account_id = hca.cust_account_id
                     --AND hcsu.LOCATION = TRIM (piv_bill_to_code) --v1.9 commented
                     AND hcsu.LOCATION = TRIM (l_leg_bill_to_addr) --v1.9 added
                     AND NVL (hcas.org_id, 1) = NVL (pin_org_id, 1)
                     AND hca.status = 'A'
                     AND hcsu.status = 'A'
                     AND hcas.status = 'A'
                     AND hcsu.site_use_code = 'BILL_TO'
                     --AND hca.orig_system_reference LIKE '%' || (TRIM (piv_cust_num))  || '%' --v1.17 Commented for removing reference to leg_customer_number
           AND hcas.orig_system_reference = l_leg_orig_sys_ref --v1.9 added
       ;
      END IF ; --v1.9 added
    EXCEPTION
         WHEN NO_DATA_FOUND
         THEN
            l_record_cnt := 2;
            l_cust_id := NULL;
            print_log_message (   'In No Data found of Customer Id check'
                               || SQLERRM
                              );
         WHEN OTHERS
         THEN
            l_record_cnt := 2;
            l_cust_id := NULL;
            print_log_message ('In When others of Customer Id check'
                               || SQLERRM
                              );
      END;

      print_log_message ('R12 Customer Id = ' || l_cust_id);

      IF l_cust_id IS NOT NULL
      THEN

         BEGIN
             /*           SELECT hcsu.cust_acct_site_id
                        INTO   l_bill_to_id
                        FROM   hz_cust_site_uses_all hcsu
                             , hz_cust_acct_sites_all hcas
                        WHERE  hcsu.location = piv_bill_to_code
                        AND    hcsu.org_id = pin_org_id
                        AND    hcsu.status = 'A'
                        AND    hcsu.site_use_code = 'BILL_TO'
                        AND    hcsu.cust_acct_site_id = hcas.cust_acct_site_id
                        AND    hcas.org_id = pin_org_id
                        AND    hcas.status = 'A'
                        AND    hcas.cust_account_id = l_cust_id;
            */

         -- Start of additional logic for v1.9 --
         -- Added the below in the exception section for v1.9 --
         -- Only if the initial query is not able to fetch single BILL TO --
         -- the below logic will be used --

         --IF UPPER(piv_operating_unit) = UPPER('Eaton EPS OU') THEN  --v1.9 added
         IF NVL(l_plant,'XXX') = '4470' THEN  --v1.10 added

              SELECT hcas.cust_acct_site_id
                      INTO l_bill_to_id
                      FROM apps.hz_cust_acct_sites_all hcas,
                           apps.hz_cust_site_uses_all hcsu
                     WHERE hcsu.cust_acct_site_id = hcas.cust_acct_site_id
                 --AND hcsu.LOCATION = TRIM (piv_bill_to_code)
                       AND hcsu.LOCATION = TRIM (l_leg_bill_to_addr)
                       AND NVL (hcas.org_id, 1) = NVL (pin_org_id, 1)
                       AND hcsu.org_id = pin_org_id
                       AND hcsu.status = 'A'
                       AND hcas.status = 'A'
                       AND hcsu.site_use_code = 'BILL_TO'
                       AND hcas.cust_account_id = l_cust_id
                       AND hcas.orig_system_reference = 'EPS.'||REGEXP_SUBSTR (l_leg_orig_sys_ref ,'[^."]+',6)
                      ;

              ELSIF piv_leg_source_system = 'NAFSC' AND UPPER(piv_leg_op_unit) NOT IN ('OU USD 1775 TCO', 'OU MXN CORP') THEN --v1.9 added
         
              SELECT hcas.cust_acct_site_id
                      INTO l_bill_to_id
                      FROM apps.hz_cust_acct_sites_all hcas
                           --apps.hz_cust_site_uses_all hcsu  -- commented for v1.10
                     --WHERE hcsu.cust_acct_site_id = hcas.cust_acct_site_id -- commented for v1.10
                 --AND hcsu.LOCATION = TRIM (piv_bill_to_code)
                       --AND hcsu.LOCATION = TRIM (l_leg_bill_to_addr)  -- commented for v1.10
                       WHERE hcas.orig_system_reference = l_leg_bill_to_addr  -- added for v1.10
                       AND NVL (hcas.org_id, 1) = NVL (pin_org_id, 1)
                       --AND hcsu.org_id = pin_org_id
                       --AND hcsu.status = 'A' -- commented for v1.10
                       AND hcas.status = 'A'
                       --AND hcsu.site_use_code = 'BILL_TO' -- commented for v1.10
                       AND hcas.cust_account_id = l_cust_id
                       ;
         
              ELSE
              
              SELECT hcas.cust_acct_site_id
                      INTO l_bill_to_id
                      FROM apps.hz_cust_acct_sites_all hcas,
                           apps.hz_cust_site_uses_all hcsu
                     WHERE hcsu.cust_acct_site_id = hcas.cust_acct_site_id
                 --AND hcsu.LOCATION = TRIM (piv_bill_to_code)
                       AND hcsu.LOCATION = TRIM (l_leg_bill_to_addr)
                       AND NVL (hcas.org_id, 1) = NVL (pin_org_id, 1)
                       AND hcsu.org_id = pin_org_id
                       AND hcsu.status = 'A'
                       AND hcas.status = 'A'
                       AND hcsu.site_use_code = 'BILL_TO'
                       AND hcas.cust_account_id = l_cust_id
                       AND hcas.orig_system_reference = l_leg_orig_sys_ref --v1.9 added
             ;        

              END IF ;

        EXCEPTION
            WHEN NO_DATA_FOUND
            THEN
               l_record_cnt := 2;
               l_bill_to_id := NULL;
               print_log_message (   'In No Data found of Bill to Id check'
                                  || SQLERRM
                                 );
            WHEN OTHERS
            THEN
         
                    l_record_cnt := 2;
                    l_bill_to_id := NULL;
                    print_log_message (   'In When others of Bill to Id check'
                                       || SQLERRM
                                      );

        END ;
        
         IF piv_ship_to_code IS NOT NULL
         THEN
            BEGIN
               SELECT hcsu.cust_acct_site_id
                 INTO l_ship_to_id
                 FROM hz_cust_site_uses_all hcsu,
                      hz_cust_acct_sites_all hcas
                WHERE hcsu.LOCATION = piv_ship_to_code
                  AND hcsu.org_id = pin_org_id
                  AND hcsu.status = 'A'
                  AND hcsu.site_use_code = 'SHIP_TO'
                  AND hcsu.cust_acct_site_id = hcas.cust_acct_site_id
                  AND hcas.org_id = pin_org_id
                  AND hcas.status = 'A'
                  AND hcas.cust_account_id = l_cust_id;
            EXCEPTION
               WHEN NO_DATA_FOUND
               THEN
                  l_record_cnt := 2;
                  l_ship_to_id := NULL;
                  print_log_message
                                   (   'In No Data found of Ship to Id check'
                                    || SQLERRM
                                   );
               WHEN OTHERS
               THEN
                  l_record_cnt := 2;
                  l_ship_to_id := NULL;
                  print_log_message (   'In When others of Ship to Id check'
                                     || SQLERRM
                                    );
            END;
         END IF;
      END IF ; 

      print_log_message ('Bill to Id = ' || l_bill_to_id);
      print_log_message ('Ship to Id = ' || l_ship_to_id);
      pon_cust_id := l_cust_id;
      pon_bill_to_id := l_bill_to_id;
      pon_ship_to_id := l_ship_to_id;
    pov_customer_type := l_customer_type ; --v1.8 added

      IF l_record_cnt > 1
      THEN
         pon_error_cnt := 2;
         l_record_cnt := 2;
      END IF;

      print_log_message ('-   PROCEDURE : validate_customer_info -');
   EXCEPTION
      WHEN OTHERS
      THEN
         g_retcode := 2;
         pon_error_cnt := 2;
         print_log_message (   'In Exception validate_customer_info check'
                            || SQLERRM
                           );
   END validate_customer_info;

--
-- ========================
-- Procedure: validate_transaction_type
-- =============================================================================
--   This procedure to validate transaction type
-- =============================================================================
--  Input Parameters :
--  piv_trx_type     : 11i transaction type
--  pin_org_id       : org id
--  Output Parameters :
--  pov_trx_type     : R12 transaction type
--  pon_trx_type_id  : R12 transaction Type id
--  pon_error_cnt    : Return Status
-- -----------------------------------------------------------------------------
--
   PROCEDURE validate_transaction_type (
      piv_trx_type      IN       VARCHAR2,
      pin_org_id        IN       NUMBER,
      pov_trx_type      OUT      VARCHAR2,
      pon_trx_type_id   OUT      NUMBER,
      pon_error_cnt     OUT      NUMBER,
      pov_error_msg     OUT      VARCHAR2
   )
   IS
      l_record_cnt         NUMBER;
      l_trx_type           fnd_lookup_values.description%TYPE;
      l_trx_type_id        NUMBER;
      l_trx_lkp   CONSTANT VARCHAR2 (30)         := 'ETN_AR_TRANSACTION_TYPE';
   BEGIN
      print_log_message (   '   PROCEDURE : validate_transaction_type = '
                         || piv_trx_type
                        );
      print_log_message (' Org Id = ' || pin_org_id);
      l_record_cnt := 0;
      l_trx_type := NULL;
      l_trx_type_id := NULL;
      pov_error_msg := NULL;

      BEGIN
         SELECT flv.description
           INTO l_trx_type
           FROM fnd_lookup_values flv
          WHERE flv.lookup_type = l_trx_lkp
            AND TRIM (flv.meaning) = piv_trx_type
            AND flv.LANGUAGE = USERENV ('LANG')
            AND TRUNC (SYSDATE) BETWEEN NVL (flv.start_date_active,
                                             TRUNC (SYSDATE)
                                            )
                                    AND NVL (flv.end_date_active,
                                             TRUNC (SYSDATE)
                                            );
      EXCEPTION
         WHEN NO_DATA_FOUND
         THEN
            l_record_cnt := 2;
            l_trx_type := NULL;
            print_log_message
                      (   'In No Data found of transaction type lookup check'
                       || SQLERRM
                      );
            pov_error_msg :=
               'Not able to derive R12 transaction type from transaction type Lookup';
         WHEN OTHERS
         THEN
            l_record_cnt := 2;
            l_trx_type := NULL;
            print_log_message
                        (   'In When others of transaction type lookup check'
                         || SQLERRM
                        );
            pov_error_msg :=
               'Not able to derive R12 transaction type from transaction type Lookup';
      END;

      print_log_message ('R12 Transaction Type = ' || l_trx_type);

      IF l_trx_type IS NOT NULL
      THEN
         BEGIN
            SELECT rctt.cust_trx_type_id
              INTO l_trx_type_id
              FROM ra_cust_trx_types_all rctt
             WHERE rctt.org_id = pin_org_id
               AND rctt.NAME = l_trx_type
               AND TRUNC (SYSDATE) BETWEEN NVL (rctt.start_date,
                                                TRUNC (SYSDATE)
                                               )
                                       AND NVL (rctt.end_date,
                                                TRUNC (SYSDATE));
         EXCEPTION
            WHEN NO_DATA_FOUND
            THEN
               l_record_cnt := 2;
               l_trx_type_id := NULL;
               print_log_message
                             (   'In No Data found of transaction type check'
                              || SQLERRM
                             );
               pov_error_msg := 'Error: Transaction Type is not valid';
            WHEN OTHERS
            THEN
               l_record_cnt := 2;
               l_trx_type_id := NULL;
               print_log_message
                               (   'In When others of transaction type check'
                                || SQLERRM
                               );
               pov_error_msg := 'Error: Transaction Type is not valid';
         END;
      END IF;

      print_log_message ('Transaction Type = ' || l_trx_type);
      print_log_message ('Transaction Type Id = ' || l_trx_type_id);
      pon_trx_type_id := l_trx_type_id;
      pov_trx_type := l_trx_type;

      IF l_record_cnt > 1
      THEN
         pon_error_cnt := 2;
         l_record_cnt := 2;
      END IF;
   EXCEPTION
      WHEN OTHERS
      THEN
         g_retcode := 2;
         pon_error_cnt := 2;
         print_log_message ('In Exception Transa check' || SQLERRM);
   END validate_transaction_type;

 --CR 283417  changes starts
-- ========================
-- Procedure: get_trx_rec_acct_segment1
-- =============================================================================
--   This procedure to get rec account segment1 of invoice
-- =============================================================================
--  Input Parameters :
--   pi_openclaim_stg_rec : Staging Table record

   --  Output Parameters :
   --  pov_rec_segment1    : R12 Operating Unit
   --  pov_error_msg       : Error Message
   --  pov_status          : status

   -- -----------------------------------------------------------------------------
--
   PROCEDURE get_trx_rec_acct_segment1 (
      pi_openclaim_stg_rec   IN       xxconv.xxar_openclaim_stg%ROWTYPE,
      pov_rec_segment1       OUT      VARCHAR2,
      pov_error_msg          OUT      VARCHAR2,
      pov_status             OUT      VARCHAR2
   )
   IS
      l_err_message   VARCHAR2 (2000) := NULL;
   BEGIN
      pov_rec_segment1 := NULL;
      pov_status := 'S';
      pov_error_msg := NULL;

      BEGIN
         SELECT DISTINCT leg_dist_segment1
                    INTO pov_rec_segment1
                    FROM xxar_openclaim_stg xos
                   WHERE xos.leg_cust_trx_type_name =
                                   pi_openclaim_stg_rec.leg_cust_trx_type_name
                     AND xos.leg_trx_number =
                                           pi_openclaim_stg_rec.leg_trx_number
                     AND xos.leg_account_class = g_acct_rec              --REC
                     AND xos.leg_operating_unit =
                                       pi_openclaim_stg_rec.leg_operating_unit
                     AND xos.batch_id = pi_openclaim_stg_rec.batch_id;
      EXCEPTION
         WHEN NO_DATA_FOUND
         THEN
            pov_status := 'F';
            pov_error_msg :=
               'No record found for receivable(REC) account segment1 for the transaction';
         WHEN TOO_MANY_ROWS
         THEN
            pov_status := 'F';
            pov_error_msg :=
               'Multiple records found for receivable(REC) account segment1 for the transaction';
         WHEN OTHERS
         THEN
            pov_status := 'F';
            pov_error_msg :=
                  'Error when looking up receivable(REC) account segment1 for the transaction-'
               || SUBSTR (SQLERRM, 1, 100);
      END;
   END get_trx_rec_acct_segment1;

--CR 283417  changes ends
--
-- ========================
-- Procedure: validate_operating_unit
-- =============================================================================
--   This procedure to validate operating unit
-- =============================================================================
--  Input Parameters :
--   piv_operating_unit : 11i operating unit

   --  Output Parameters :
--  pov_operating_unit  : R12 Operating Unit
--  pon_org_id          : Org Id
--  pon_sob_id          : Set Of Book Id
--  pon_error_cnt    : Return Status
-- -----------------------------------------------------------------------------
--

   --Ver 1.3 Changes start
/*
   PROCEDURE validate_operating_unit (
     piv_operating_unit  IN      VARCHAR2
   , pov_operating_unit  OUT     VARCHAR2
   , pon_org_id          OUT     NUMBER
   , pon_sob_id          OUT     NUMBER
   , pon_error_cnt       OUT     NUMBER
   , pov_error_msg       OUT     VARCHAR2
   )
   IS
      l_record_cnt             NUMBER;
      l_operating_unit         fnd_lookup_values.description%TYPE;
      l_org_id                 NUMBER;
      l_sob_id                 NUMBER;
      l_ou_lkp    CONSTANT     VARCHAR2 ( 30 ) := 'ETN_COMMON_OU_MAP';
   BEGIN
      print_log_message ('   PROCEDURE : validate_operating_unit = ' || piv_operating_unit );
      l_record_cnt := 0;
      l_operating_unit := NULL;
      l_org_id    := NULL;
      l_sob_id    := NULL;
      pov_error_msg := NULL;

      BEGIN
         SELECT flv.description
         INTO   l_operating_unit
         FROM   fnd_lookup_values flv
         WHERE  flv.lookup_type = l_ou_lkp
         AND    TRIM(flv.meaning) = piv_operating_unit
         AND    flv.language = USERENV ( 'LANG' )
         AND    TRUNC ( SYSDATE ) BETWEEN NVL (
                                           flv.start_date_active
                                         , TRUNC ( SYSDATE )
                                         )
                                      AND NVL (
                                           flv.end_date_active
                                         , TRUNC ( SYSDATE )
                                         );
      EXCEPTION
         WHEN NO_DATA_FOUND
         THEN
            l_record_cnt := 2;
            l_operating_unit := NULL;
            print_log_message ('In No Data found of common OU lookup check' || SQLERRM );
            pov_error_msg    := 'Not able to derive R12 operating unit from operating unit Lookup';
         WHEN OTHERS
         THEN
            l_record_cnt := 2;
            l_operating_unit := NULL;
            print_log_message ('In When others of common OU lookup check' || SQLERRM );
            pov_error_msg    := 'Not able to derive R12 operating unit from operating unit Lookup';
      END;

      IF l_operating_unit IS NOT NULL
      THEN
         BEGIN
            SELECT hou.organization_id
                 , hou.set_of_books_id
            INTO   l_org_id
                 , l_sob_id
            FROM   hr_operating_units hou
            WHERE  hou.name = l_operating_unit
            AND    TRUNC ( SYSDATE ) BETWEEN NVL (
                                              hou.date_from
                                            , TRUNC ( SYSDATE )
                                            )
                                         AND NVL (
                                              hou.date_to
                                            , TRUNC ( SYSDATE )
                                            );
         EXCEPTION
            WHEN NO_DATA_FOUND
            THEN
               l_record_cnt := 2;
               l_org_id    := NULL;
               l_sob_id    := NULL;
               print_log_message ('In No Data found of operating unit check' || SQLERRM );
               pov_error_msg    := 'Error: Operating Unit is invalid';
            WHEN OTHERS
            THEN
               l_record_cnt := 2;
               l_org_id    := NULL;
               l_sob_id    := NULL;
               print_log_message ('In When others of operating unit check' || SQLERRM );
               pov_error_msg    := 'Error: Operating Unit is invalid';
         END;
      END IF;

      print_log_message ('Operating Unit = ' || l_operating_unit );
      print_log_message ('Org Id = ' || l_org_id );
      print_log_message ('SOB Id = ' || l_sob_id );
      pon_org_id  := l_org_id;
      pov_operating_unit := l_operating_unit;
      pon_sob_id  := l_sob_id;

      IF l_record_cnt > 1
      THEN
         pon_error_cnt := 2;
      END IF;
   EXCEPTION
      WHEN OTHERS
      THEN
         g_retcode   := 2;
         pon_error_cnt := 2;
         print_log_message ('In Exception Operating Unit check' || SQLERRM );
   END validate_operating_unit;
*/
   PROCEDURE validate_operating_unit (
      piv_leg_seg1         IN       VARCHAR2,
      pov_operating_unit   OUT      VARCHAR2,
      pon_org_id           OUT      NUMBER,
      pon_sob_id           OUT      NUMBER,
      pon_error_cnt        OUT      NUMBER,
      pov_error_msg        OUT      VARCHAR2
   )
   IS
      l_record_cnt       NUMBER;
      l_operating_unit   xxetn_map_unit.operating_unit%TYPE;
      l_org_id           NUMBER;
      l_sob_id           NUMBER;
      l_rec              xxetn_map_util.g_input_rec;
   BEGIN
      print_log_message
           (   '   PROCEDURE : validate_operating_unit for legacy segment1= '
            || piv_leg_seg1
           );
      l_record_cnt := 0;
      l_operating_unit := NULL;
      l_org_id := NULL;
      l_sob_id := NULL;
      pov_error_msg := NULL;

      BEGIN
         l_rec.site := piv_leg_seg1;
         l_operating_unit := xxetn_map_util.get_value (l_rec).operating_unit;
      EXCEPTION
         WHEN OTHERS
         THEN
            l_record_cnt := 2;
            l_operating_unit := NULL;
            print_log_message (   'In When others of common OU lookup check'
                               || SQLERRM
                              );
            pov_error_msg :=
               'Not able to derive R12 operating unit from operating unit Lookup';
      END;

      IF l_operating_unit IS NOT NULL
      THEN
         BEGIN
            SELECT hou.organization_id,
                   hou.set_of_books_id
              INTO l_org_id,
                   l_sob_id
              FROM hr_operating_units hou
             WHERE hou.NAME = l_operating_unit
               AND TRUNC (SYSDATE) BETWEEN NVL (hou.date_from,
                                                TRUNC (SYSDATE))
                                       AND NVL (hou.date_to, TRUNC (SYSDATE));
         EXCEPTION
            WHEN NO_DATA_FOUND
            THEN
               l_record_cnt := 2;
               l_org_id := NULL;
               l_sob_id := NULL;
               print_log_message
                               (   'In No Data found of operating unit check'
                                || SQLERRM
                               );
               pov_error_msg := 'Error: Operating Unit is invalid';
            WHEN OTHERS
            THEN
               l_record_cnt := 2;
               l_org_id := NULL;
               l_sob_id := NULL;
               print_log_message (   'In When others of operating unit check'
                                  || SQLERRM
                                 );
               pov_error_msg := 'Error: Operating Unit is invalid';
         END;
    ELSE 
       l_record_cnt := 2;
         l_operating_unit := NULL;
         print_log_message (   'In When others of common OU lookup check'
                            || SQLERRM
                           );
         pov_error_msg :=
            'Not able to derive R12 operating unit from operating unit Lookup';
    
      END IF;

      print_log_message ('Operating Unit = ' || l_operating_unit);
      print_log_message ('Org Id = ' || l_org_id);
      print_log_message ('SOB Id = ' || l_sob_id);
      pon_org_id := l_org_id;
      pov_operating_unit := l_operating_unit;
      pon_sob_id := l_sob_id;

      IF l_record_cnt > 1
      THEN
         pon_error_cnt := 2;
      END IF;
   EXCEPTION
      WHEN OTHERS
      THEN
         g_retcode := 2;
         pon_error_cnt := 2;
         print_log_message ('In Exception Operating Unit check' || SQLERRM);
   END validate_operating_unit;

--Ver 1.2 Changes end

   --
-- ========================
-- Procedure: validate_coa
-- =============================================================================
--   This procedure to validate coa
-- =============================================================================
--  Input Parameters :
--   piv_segment1-7  : Lgeacy distribution segments

   --  Output Parameters :
--  Dist segment 1-10: R12 distribution segments
--   pon_error_cnt    : Return Status
-- -----------------------------------------------------------------------------
--
   PROCEDURE validate_coa (
      piv_leg_segment1    IN       VARCHAR2,
      piv_leg_segment2    IN       VARCHAR2,
      piv_leg_segment3    IN       VARCHAR2,
      piv_leg_segment4    IN       VARCHAR2,
      piv_leg_segment5    IN       VARCHAR2,
      piv_leg_segment6    IN       VARCHAR2,
      piv_leg_segment7    IN       VARCHAR2,
      piv_leg_segment8    IN       VARCHAR2,
      piv_leg_segment9    IN       VARCHAR2,
      piv_leg_segment10   IN       VARCHAR2,
      piv_segment1        OUT      VARCHAR2,
      piv_segment2        OUT      VARCHAR2,
      piv_segment3        OUT      VARCHAR2,
      piv_segment4        OUT      VARCHAR2,
      piv_segment5        OUT      VARCHAR2,
      piv_segment6        OUT      VARCHAR2,
      piv_segment7        OUT      VARCHAR2,
      piv_segment8        OUT      VARCHAR2,
      piv_segment9        OUT      VARCHAR2,
      piv_segment10       OUT      VARCHAR2,
      pon_error_cnt       OUT      NUMBER
   )
   IS
      l_record_cnt   NUMBER;
   BEGIN
      print_log_message ('+   PROCEDURE : validate_coa +');
      print_log_message (' Legacy Segment1 = ' || piv_leg_segment1);
      print_log_message (' Legacy Segment2 = ' || piv_leg_segment2);
      print_log_message (' Legacy Segment3 = ' || piv_leg_segment3);
      print_log_message (' Legacy Segment4 = ' || piv_leg_segment4);
      print_log_message (' Legacy Segment5 = ' || piv_leg_segment5);
      print_log_message (' Legacy Segment6 = ' || piv_leg_segment6);
      print_log_message (' Legacy Segment7 = ' || piv_leg_segment7);
      print_log_message (' Legacy Segment8 = ' || piv_leg_segment8);
      print_log_message (' Legacy Segment9 = ' || piv_leg_segment9);
      print_log_message (' Legacy Segment10 = ' || piv_leg_segment10);
      l_record_cnt := 0;
      piv_segment1 := piv_leg_segment1;
      piv_segment2 := piv_leg_segment2;
      piv_segment3 := piv_leg_segment3;
      piv_segment4 := piv_leg_segment4;
      piv_segment5 := piv_leg_segment5;
      piv_segment6 := piv_leg_segment6;
      piv_segment7 := piv_leg_segment7;
      piv_segment8 := piv_leg_segment8;
      piv_segment9 := piv_leg_segment9;
      piv_segment10 := piv_leg_segment10;
      print_log_message (' Segment1 = ' || piv_segment1);
      print_log_message (' Segment2 = ' || piv_segment2);
      print_log_message (' Segment3 = ' || piv_segment3);
      print_log_message (' Segment4 = ' || piv_segment4);
      print_log_message (' Segment5 = ' || piv_segment5);
      print_log_message (' Segment6 = ' || piv_segment6);
      print_log_message (' Segment7 = ' || piv_segment7);
      print_log_message (' Segment8 = ' || piv_segment8);
      print_log_message (' Segment9 = ' || piv_segment9);
      print_log_message (' Segment10 = ' || piv_segment10);

      IF l_record_cnt > 1
      THEN
         pon_error_cnt := 2;
      END IF;

      print_log_message ('-   PROCEDURE : validate_coa -');
   EXCEPTION
      WHEN OTHERS
      THEN
         g_retcode := 2;
         pon_error_cnt := 2;
         print_log_message ('In Exception validate COA' || SQLERRM);
   END validate_coa;

--
-- ========================
-- Procedure: validate_payment_term
-- =============================================================================
--   This procedure to validate payment term
-- =============================================================================
--  Input Parameters :
--   piv_payment_term : 11i Payment term

   --  Output Parameters :
--  pov_payment_term  : R12 payment term
--   pon_term_id      : term id
--   pon_error_cnt    : Return Status
-- -----------------------------------------------------------------------------
--
   PROCEDURE validate_payment_term (
      piv_payment_term   IN       VARCHAR2,
      pov_payment_term   OUT      VARCHAR2,
      pon_term_id        OUT      NUMBER,
      pon_error_cnt      OUT      NUMBER,
      pov_error_msg      OUT      VARCHAR2
   )
   IS
      l_record_cnt             NUMBER;
      l_term_name              fnd_lookup_values.description%TYPE;
      l_term_id                NUMBER;
      l_payment_lkp   CONSTANT VARCHAR2 (30)        := 'ETN_AR_PAYMENT_TERMS';
   BEGIN
      print_log_message (   '   PROCEDURE : validate_payment_term = '
                         || piv_payment_term
                        );
      l_record_cnt := 0;
      l_term_name := NULL;
      l_term_id := NULL;
      pov_error_msg := NULL;

      BEGIN
         SELECT flv.description
           INTO l_term_name
           FROM fnd_lookup_values flv
          WHERE flv.lookup_type = l_payment_lkp
            AND flv.enabled_flag = 'Y'
            AND TRIM (flv.meaning) = piv_payment_term
            AND flv.LANGUAGE = USERENV ('LANG')
            AND TRUNC (SYSDATE) BETWEEN NVL (flv.start_date_active,
                                             TRUNC (SYSDATE)
                                            )
                                    AND NVL (flv.end_date_active,
                                             TRUNC (SYSDATE)
                                            );
      EXCEPTION
         WHEN NO_DATA_FOUND
         THEN
            l_record_cnt := 2;
            l_term_name := NULL;
            pov_error_msg :=
               'Not able to derive R12 payment term from payment term Lookup';
            print_log_message
                          (   'In No Data found of payment term lookup check'
                           || SQLERRM
                          );
         WHEN OTHERS
         THEN
            l_record_cnt := 2;
            l_term_name := NULL;
            pov_error_msg :=
               'Not able to derive R12 payment term from payment term Lookup';
            print_log_message
                            (   'In When others of payment term lookup check'
                             || SQLERRM
                            );
      END;

      IF l_term_name IS NOT NULL
      THEN
         BEGIN
            SELECT rt.term_id
              INTO l_term_id
              FROM ra_terms rt
             WHERE rt.NAME = l_term_name
               AND TRUNC (NVL (rt.end_date_active, SYSDATE)) >=
                                                               TRUNC (SYSDATE);
         EXCEPTION
            WHEN NO_DATA_FOUND
            THEN
               l_record_cnt := 2;
               l_term_id := NULL;
               pov_error_msg := 'Error: Payment term is invalid';
               print_log_message (   'In No Data found of payment term check'
                                  || SQLERRM
                                 );
            WHEN OTHERS
            THEN
               l_record_cnt := 2;
               l_term_id := NULL;
               pov_error_msg := 'Error: Payment term is invalid';
               print_log_message (   'In When others of payment term check'
                                  || SQLERRM
                                 );
         END;
      END IF;

      print_log_message ('Payment Term = ' || l_term_name);
      print_log_message ('Term Id = ' || l_term_id);
      pov_payment_term := l_term_name;
      pon_term_id := l_term_id;

      IF l_record_cnt > 1
      THEN
         pon_error_cnt := 2;
      END IF;
   EXCEPTION
      WHEN OTHERS
      THEN
         g_retcode := 2;
         pon_error_cnt := 2;
         print_log_message ('In Exception validate payment term' || SQLERRM);
   END validate_payment_term;

--
-- ========================
-- Procedure: mandatory_value_check
-- =============================================================================
--   This procedure to do mandatory value check
-- =============================================================================
--  Input Parameters :
--  piv_cust_num
--  piv_bill_address
--  pin_amount
--  pid_trx_date
--  pid_receipt_date
--  piv_term_name
--  piv_operating_unit
--  piv_currency_code
--  piv_reason_code
--  piv_receipt_method
--  piv_trx_number
--  piv_segment1
--  piv_segment2
--  piv_segment3
--  piv_segment4
--  piv_segment5
--  piv_segment6
--  piv_segment7

   --  Output Parameters :
--   pon_error_cnt    : Return Status
-- -----------------------------------------------------------------------------
--
   PROCEDURE mandatory_value_check (
      piv_cust_num         IN       VARCHAR2,
      piv_bill_address     IN       VARCHAR2,
      pin_amount           IN       NUMBER,
      pid_trx_date         IN       DATE,
      pid_receipt_date     IN       DATE,
      piv_term_name        IN       VARCHAR2,
      piv_operating_unit   IN       VARCHAR2,
      piv_currency_code    IN       VARCHAR2,
      piv_reason_code      IN       VARCHAR2,
      piv_receipt_method   IN       VARCHAR2,
      piv_trx_number       IN       VARCHAR2,
      piv_segment1         IN       VARCHAR2,
      piv_segment2         IN       VARCHAR2,
      piv_segment3         IN       VARCHAR2,
      piv_segment4         IN       VARCHAR2,
      piv_segment5         IN       VARCHAR2,
      piv_segment6         IN       VARCHAR2,
      piv_segment7         IN       VARCHAR2,
      pon_error_cnt        OUT      NUMBER
   )
   IS
      l_record_cnt       NUMBER;
      l_err_msg          VARCHAR2 (2000);
      l_log_ret_status   VARCHAR2 (50);
      l_log_err_msg      VARCHAR2 (2000);
      l_err_code         VARCHAR2 (40)   := NULL;
   BEGIN
      print_log_message ('   PROCEDURE : mandatory_value_check');
      l_record_cnt := 0;
      l_err_msg := NULL;
      l_log_ret_status := NULL;
      l_log_err_msg := NULL;
      l_err_code := NULL;

      IF piv_cust_num IS NULL
      THEN
         l_record_cnt := 2;
         l_err_code := 'ETN_AR_MANDATORY_VALUE_NULL';
         l_err_msg := 'Error: Customer Number is NULL on this record.';
         log_errors (pov_return_status            => l_log_ret_status,  -- OUT
                     pov_error_msg                => l_log_err_msg,     -- OUT
                     piv_source_column_name       => 'Customer Number',
                     piv_source_column_value      => NULL,
                     piv_error_type               => g_err_val,
                     piv_error_code               => l_err_code,
                     piv_error_message            => l_err_msg
                    );
      END IF;

      IF piv_bill_address IS NULL
      THEN
         l_record_cnt := 2;
         l_err_code := 'ETN_AR_MANDATORY_VALUE_NULL';
         l_err_msg := 'Error: Bill to address is NULL on this record.';
         log_errors (pov_return_status            => l_log_ret_status,   --OUT
                     pov_error_msg                => l_log_err_msg,      --OUT
                     piv_source_column_name       => 'Bill To Address',
                     piv_source_column_value      => NULL,
                     piv_error_type               => g_err_val,
                     piv_error_code               => l_err_code,
                     piv_error_message            => l_err_msg
                    );
      END IF;

      IF pin_amount IS NULL
      THEN
         l_record_cnt := 2;
         l_err_code := 'ETN_AR_MANDATORY_VALUE_NULL';
         l_err_msg := 'Error: Amount is NULL on this record.';
         log_errors (pov_return_status            => l_log_ret_status,   --OUT
                     pov_error_msg                => l_log_err_msg,      --OUT
                     piv_source_column_name       => 'Amount',
                     piv_source_column_value      => NULL,
                     piv_error_type               => g_err_val,
                     piv_error_code               => l_err_code,
                     piv_error_message            => l_err_msg
                    );
      END IF;

      IF pid_trx_date IS NULL
      THEN
         l_record_cnt := 2;
         l_err_code := 'ETN_AR_MANDATORY_VALUE_NULL';
         l_err_msg := 'Error: Transaction Date is NULL on this record.';
         log_errors (pov_return_status            => l_log_ret_status,   --OUT
                     pov_error_msg                => l_log_err_msg,      --OUT
                     piv_source_column_name       => 'Transaction Date',
                     piv_source_column_value      => NULL,
                     piv_error_type               => g_err_val,
                     piv_error_code               => l_err_code,
                     piv_error_message            => l_err_msg
                    );
      END IF;

      IF pid_receipt_date IS NULL
      THEN
         l_record_cnt := 2;
         l_err_code := 'ETN_AR_MANDATORY_VALUE_NULL';
         l_err_msg := 'Error: Receipt Date is NULL on this record.';
         log_errors (pov_return_status            => l_log_ret_status,   --OUT
                     pov_error_msg                => l_log_err_msg,      --OUT
                     piv_source_column_name       => 'Receipt Date',
                     piv_source_column_value      => NULL,
                     piv_error_type               => g_err_val,
                     piv_error_code               => l_err_code,
                     piv_error_message            => l_err_msg
                    );
      END IF;

      IF piv_term_name IS NULL
      THEN
         l_record_cnt := 2;
         l_err_code := 'ETN_AR_MANDATORY_VALUE_NULL';
         l_err_msg := 'Error: Payment term is NULL on this record.';
         log_errors (pov_return_status            => l_log_ret_status,   --OUT
                     pov_error_msg                => l_log_err_msg,      --OUT
                     piv_source_column_name       => 'Payment Term',
                     piv_source_column_value      => NULL,
                     piv_error_type               => g_err_val,
                     piv_error_code               => l_err_code,
                     piv_error_message            => l_err_msg
                    );
      END IF;

      IF piv_operating_unit IS NULL
      THEN
         l_record_cnt := 2;
         l_err_code := 'ETN_AR_MANDATORY_VALUE_NULL';
         l_err_msg := 'Error: Operating Unit is NULL on this record.';
         log_errors (pov_return_status            => l_log_ret_status,   --OUT
                     pov_error_msg                => l_log_err_msg,      --OUT
                     piv_source_column_name       => 'Operating Unit',
                     piv_source_column_value      => NULL,
                     piv_error_type               => g_err_val,
                     piv_error_code               => l_err_code,
                     piv_error_message            => l_err_msg
                    );
      END IF;

      IF piv_currency_code IS NULL
      THEN
         l_record_cnt := 2;
         l_err_code := 'ETN_AR_MANDATORY_VALUE_NULL';
         l_err_msg := 'Error: Currency code is NULL on this record.';
         log_errors (pov_return_status            => l_log_ret_status,   --OUT
                     pov_error_msg                => l_log_err_msg,      --OUT
                     piv_source_column_name       => 'Currency Code',
                     piv_source_column_value      => NULL,
                     piv_error_type               => g_err_val,
                     piv_error_code               => l_err_code,
                     piv_error_message            => l_err_msg
                    );
      END IF;

      -- Commented by Abhijit to bypass the reason code validation. Reason code will de defaulted if it is null
      -- 12-Jun-2014
      /* IF piv_reason_code IS NULL
       THEN
          l_record_cnt := 2;
          l_err_code  := 'ETN_AR_MANDATORY_VALUE_NULL';
          l_err_msg   := 'Error: Reason Code is NULL on this record.';
          log_errors (
            pov_return_status       => l_log_ret_status                                           -- OUT
          , pov_error_msg           => l_log_err_msg                                              -- OUT
          , piv_source_column_name  => 'Reason Code'
          , piv_source_column_value => NULL
          , piv_error_type          => g_err_val
          , piv_error_code          => l_err_code
          , piv_error_message       => l_err_msg
          );
       END IF; */
      IF piv_receipt_method IS NULL
      THEN
         l_record_cnt := 2;
         l_err_code := 'ETN_AR_MANDATORY_VALUE_NULL';
         l_err_msg := 'Error: Receipt Method is NULL on this record.';
         log_errors (pov_return_status            => l_log_ret_status,   --OUT
                     pov_error_msg                => l_log_err_msg,      --OUT
                     piv_source_column_name       => 'Receipt Method',
                     piv_source_column_value      => NULL,
                     piv_error_type               => g_err_val,
                     piv_error_code               => l_err_code,
                     piv_error_message            => l_err_msg
                    );
      END IF;

      IF piv_trx_number IS NULL
      THEN
         l_record_cnt := 2;
         l_err_code := 'ETN_AR_MANDATORY_VALUE_NULL';
         l_err_msg := 'Error: Trx Number is NULL on this record.';
         log_errors (pov_return_status            => l_log_ret_status,   --OUT
                     pov_error_msg                => l_log_err_msg,      --OUT
                     piv_source_column_name       => 'Trx Number',
                     piv_source_column_value      => NULL,
                     piv_error_type               => g_err_val,
                     piv_error_code               => l_err_code,
                     piv_error_message            => l_err_msg
                    );
      END IF;

      IF piv_segment1 IS NULL
      THEN
         l_record_cnt := 2;
         l_err_code := 'ETN_AR_MANDATORY_VALUE_NULL';
         l_err_msg := 'Error: Distribution Segment1 is NULL on this record.';
         log_errors (pov_return_status            => l_log_ret_status,   --OUT
                     pov_error_msg                => l_log_err_msg,      --OUT
                     piv_source_column_name       => 'Segment1',
                     piv_source_column_value      => NULL,
                     piv_error_type               => g_err_val,
                     piv_error_code               => l_err_code,
                     piv_error_message            => l_err_msg
                    );
      END IF;

      IF piv_segment2 IS NULL
      THEN
         l_record_cnt := 2;
         l_err_code := 'ETN_AR_MANDATORY_VALUE_NULL';
         l_err_msg := 'Error: Distribution Segment2 is NULL on this record.';
         log_errors (pov_return_status            => l_log_ret_status,   --OUT
                     pov_error_msg                => l_log_err_msg,      --OUT
                     piv_source_column_name       => 'Segment2',
                     piv_source_column_value      => NULL,
                     piv_error_type               => g_err_val,
                     piv_error_code               => l_err_code,
                     piv_error_message            => l_err_msg
                    );
      END IF;

      IF piv_segment3 IS NULL
      THEN
         l_record_cnt := 2;
         l_err_code := 'ETN_AR_MANDATORY_VALUE_NULL';
         l_err_msg := 'Error: Distribution Segment3 is NULL on this record.';
         log_errors (pov_return_status            => l_log_ret_status,   --OUT
                     pov_error_msg                => l_log_err_msg,      --OUT
                     piv_source_column_name       => 'Segment3',
                     piv_source_column_value      => NULL,
                     piv_error_type               => g_err_val,
                     piv_error_code               => l_err_code,
                     piv_error_message            => l_err_msg
                    );
      END IF;

      IF piv_segment4 IS NULL
      THEN
         l_record_cnt := 2;
         l_err_code := 'ETN_AR_MANDATORY_VALUE_NULL';
         l_err_msg := 'Error: Distribution Segment4 is NULL on this record.';
         log_errors (pov_return_status            => l_log_ret_status,   --OUT
                     pov_error_msg                => l_log_err_msg,      --OUT
                     piv_source_column_name       => 'Segment4',
                     piv_source_column_value      => NULL,
                     piv_error_type               => g_err_val,
                     piv_error_code               => l_err_code,
                     piv_error_message            => l_err_msg
                    );
      END IF;

      IF piv_segment5 IS NULL
      THEN
         l_record_cnt := 2;
         l_err_code := 'ETN_AR_MANDATORY_VALUE_NULL';
         l_err_msg := 'Error: Distribution Segment5 is NULL on this record.';
         log_errors (pov_return_status            => l_log_ret_status,   --OUT
                     pov_error_msg                => l_log_err_msg,      --OUT
                     piv_source_column_name       => 'Segment5',
                     piv_source_column_value      => NULL,
                     piv_error_type               => g_err_val,
                     piv_error_code               => l_err_code,
                     piv_error_message            => l_err_msg
                    );
      END IF;

      IF piv_segment6 IS NULL
      THEN
         l_record_cnt := 2;
         l_err_code := 'ETN_AR_MANDATORY_VALUE_NULL';
         l_err_msg := 'Error: Distribution Segment6 is NULL on this record.';
         log_errors (pov_return_status            => l_log_ret_status,   --OUT
                     pov_error_msg                => l_log_err_msg,      --OUT
                     piv_source_column_name       => 'Segment6',
                     piv_source_column_value      => NULL,
                     piv_error_type               => g_err_val,
                     piv_error_code               => l_err_code,
                     piv_error_message            => l_err_msg
                    );
      END IF;

      IF piv_segment7 IS NULL
      THEN
         l_record_cnt := 2;
         l_err_code := 'ETN_AR_MANDATORY_VALUE_NULL';
         l_err_msg := 'Error: Distribution Segment7 is NULL on this record.';
         log_errors (pov_return_status            => l_log_ret_status,   --OUT
                     pov_error_msg                => l_log_err_msg,      --OUT
                     piv_source_column_name       => 'Segment7',
                     piv_source_column_value      => NULL,
                     piv_error_type               => g_err_val,
                     piv_error_code               => l_err_code,
                     piv_error_message            => l_err_msg
                    );
      END IF;

      IF l_record_cnt > 1
      THEN
         pon_error_cnt := 2;
      END IF;
   EXCEPTION
      WHEN OTHERS
      THEN
         g_retcode := 2;
         pon_error_cnt := 2;
         print_log_message (   'In Exception mandatory_value_check check'
                            || SQLERRM
                           );
   END mandatory_value_check;

--
-- ========================
-- Procedure: duplicate_check
-- =============================================================================
--   This procedure to do duplicate check
-- =============================================================================
--  Input Parameters :
--   piv_trx_type
--   piv_trx_number
--   piv_operating_unit
--   piv_account_class
--  Output Parameters :
--   pon_error_cnt    : Return Status
-- -----------------------------------------------------------------------------
--
   PROCEDURE duplicate_check (
      piv_trx_type         IN       VARCHAR2,
      piv_trx_number       IN       VARCHAR2,
      piv_operating_unit   IN       VARCHAR2,
      piv_account_class    IN       VARCHAR2,
      pon_error_cnt        OUT      NUMBER
   )
   IS
      l_record_cnt   NUMBER;
   BEGIN
      print_log_message ('   PROCEDURE : duplicate_check');
      l_record_cnt := 0;

      BEGIN
         SELECT COUNT (1)
           INTO l_record_cnt
           FROM xxar_openclaim_stg xos
          WHERE xos.leg_cust_trx_type_name = piv_trx_type
            AND xos.leg_trx_number = piv_trx_number
            AND xos.leg_account_class = piv_account_class
            AND xos.leg_operating_unit = piv_operating_unit
            AND xos.batch_id = g_new_batch_id;
      EXCEPTION
         WHEN NO_DATA_FOUND
         THEN
            l_record_cnt := 2;
            print_log_message ('In No Data found of Duplicate check'
                               || SQLERRM
                              );
         WHEN OTHERS
         THEN
            l_record_cnt := 2;
            print_log_message ('In When others of Duplicate check' || SQLERRM);
      END;

      IF l_record_cnt > 1
      THEN
         pon_error_cnt := 2;
      END IF;
   EXCEPTION
      WHEN OTHERS
      THEN
         g_retcode := 2;
         pon_error_cnt := 2;
         print_log_message ('In Exception Duplicate check' || SQLERRM);
   END duplicate_check;

--
-- ========================
-- Procedure: bulk_errors
-- =============================================================================
--   This procedure to mark record in error in case corresponding account class record is in error
-- =============================================================================
--  Input Parameters :
--  Output Parameters :
-- -----------------------------------------------------------------------------
--
   PROCEDURE bulk_errors
   IS
      l_error_tab_type   xxetn_common_error_pkg.g_source_tab_type;
      l_return_status    VARCHAR2 (1);
      l_error_message    VARCHAR2 (2000);
   BEGIN
      print_log_message ('In Bulk error procedure');

      --bulk collecting all records where are to be marked in error
      SELECT g_source_table,                                    --source_table
             --NULL,                                               --C9988598 :- leg_customer_trx_id, commented for v1.24
             interface_txn_id,                          --interface_staging_id
             NULL,                                           --source_keyname1
             NULL,                                          --source_keyvalue1
             NULL,                                           --source_keyname2
             NULL,                                          --source_keyvalue2
             NULL,                                           --source_keyname3
             NULL,                                          --source_keyvalue3
             NULL,                                           --source_keyname4
             NULL,                                          --source_keyvalue4
             NULL,                                           --source_keyname5
             NULL,                                          --source_keyvalue5
             NULL,                                        --source_column_name
             NULL,                                       --source_column_value
             g_err_val,                                           --error_type
             'ETN_CORRESPONDING_RECORD_ERROR',                    --error_code
             'Error: Corresponding account class record is in error',
             --error_message
             NULL,                                                  --severity
             NULL,                                         --proposed_solution
             NULL,                                                      --stage
             NULL                                          -- interface load id , added for v1.24
      BULK COLLECT INTO l_error_tab_type
        FROM xxar_openclaim_stg xos
       WHERE xos.process_flag = g_valid
         AND xos.batch_id = g_new_batch_id
         AND EXISTS (
                SELECT 1
                  FROM xxar_openclaim_stg xos1
                 WHERE xos1.leg_cust_trx_type_name =
                                                    xos.leg_cust_trx_type_name
                   AND xos1.leg_trx_number = xos.leg_trx_number
                   AND xos1.leg_account_class <> xos.leg_account_class
                   AND xos1.process_flag = g_error
                   AND xos1.leg_operating_unit = xos.leg_operating_unit
                   AND xos1.batch_id = xos.batch_id);

      print_log_message ('l_error_tab_type.COUNT ' || l_error_tab_type.COUNT);

      IF l_error_tab_type.COUNT > 0
      THEN
         log_bulk_errors (p_source_tab_type      => l_error_tab_type,
                          pov_return_status      => l_return_status,
                          pov_error_msg          => l_error_message
                         );
      END IF;

      --updating records to error, where corressponding account class record is in error
      UPDATE xxar_openclaim_stg xos
         SET process_flag = g_error,
             ERROR_TYPE = g_err_val,
             last_updated_date = g_sysdate,
             last_updated_by = g_user_id,
             last_update_login = g_login_id,
             request_id = g_request_id       
       WHERE batch_id = g_new_batch_id
         AND run_sequence_id = g_run_seq_id
         AND xos.process_flag = g_valid
         AND EXISTS (
                SELECT 1
                  FROM xxetn_common_error
                 WHERE source_table = g_source_table
                   AND interface_staging_id = xos.interface_txn_id
                   AND batch_id = xos.batch_id
                   AND run_sequence_id = xos.run_sequence_id);

      COMMIT;
   EXCEPTION
      WHEN OTHERS
      THEN
         g_retcode := 2;
         print_log_message ('In Exception bulk error procedure' || SQLERRM);
   END bulk_errors;

--
-- ========================
-- Procedure: invalid_check
-- =============================================================================
--   This procedure to do invalid check
-- =============================================================================
--  Input Parameters :
--   piv_trx_type
--   piv_trx_number
--   piv_operating_unit
--   piv_account_class
--  Output Parameters :
--   pon_error_cnt    : Return Status
--   pov_error_msg    : Error message
-- -----------------------------------------------------------------------------
--
   PROCEDURE invalid_check (
      piv_trx_type         IN       VARCHAR2,
      piv_trx_number       IN       VARCHAR2,
      piv_operating_unit   IN       VARCHAR2,
      piv_account_class    IN       VARCHAR2,
      pon_error_cnt        OUT      NUMBER,
      pov_error_msg        OUT      VARCHAR2
   )
   IS
      l_record_cnt   NUMBER;
   BEGIN
      print_log_message ('   PROCEDURE : invalid_check');
      print_log_message ('   Account class = ' || piv_account_class);
      l_record_cnt := 0;
      pov_error_msg := NULL;

      IF piv_account_class = g_acct_rec
      THEN
         BEGIN
            SELECT COUNT (1)
              INTO l_record_cnt
              FROM xxar_openclaim_stg xos
             WHERE xos.leg_cust_trx_type_name = piv_trx_type
               AND xos.leg_trx_number = piv_trx_number
               AND xos.leg_account_class = g_acct_rev
               AND xos.leg_operating_unit = piv_operating_unit
               AND xos.batch_id = g_new_batch_id;
         EXCEPTION
            WHEN NO_DATA_FOUND
            THEN
               l_record_cnt := 2;
               print_log_message (   'In No Data found of Invalid check'
                                  || SQLERRM
                                 );
               pov_error_msg :=
                  'Error: No Corresponding different account class record for this transaction';
            WHEN OTHERS
            THEN
               l_record_cnt := 2;
               print_log_message ('In When others of Invalid check' || SQLERRM
                                 );
               pov_error_msg :=
                  'Error: Multiple Corresponding different account class record for this transaction';
         END;
      ELSIF piv_account_class = g_acct_rev
      THEN
         BEGIN
            SELECT COUNT (1)
              INTO l_record_cnt
              FROM xxar_openclaim_stg xos
             WHERE xos.leg_cust_trx_type_name = piv_trx_type
               AND xos.leg_trx_number = piv_trx_number
               AND xos.leg_account_class = g_acct_rec
               AND xos.leg_operating_unit = piv_operating_unit
               AND xos.batch_id = g_new_batch_id;
         EXCEPTION
            WHEN NO_DATA_FOUND
            THEN
               l_record_cnt := 2;
               pov_error_msg :=
                  'Error: No Corresponding different account class record for this transaction';
               print_log_message (   'In No Data found of Invalid check'
                                  || SQLERRM
                                 );
            WHEN OTHERS
            THEN
               l_record_cnt := 2;
               print_log_message ('In When others of Invalid check' || SQLERRM
                                 );
               pov_error_msg :=
                  'Error: Multiple Corresponding different account class record for this transaction';
         END;
      END IF;

      IF l_record_cnt = 0
      THEN
         pon_error_cnt := 2;
         pov_error_msg :=
            'Error: No Corresponding different account class record for this transaction';
      END IF;

      IF l_record_cnt > 1
      THEN
         pon_error_cnt := 2;
         pov_error_msg :=
            'Error: Multiple Corresponding different account class record for this transaction';
      END IF;
   EXCEPTION
      WHEN OTHERS
      THEN
         g_retcode := 2;
         pon_error_cnt := 2;
         pov_error_msg :=
                       'Error: In outer exception of invalid check procedure';
         print_log_message ('In Exception Invalid check' || SQLERRM);
   END invalid_check;

--
-- ========================
-- Procedure: GET_DATA
-- =============================================================================
--   This procedure to load data from extraction table to staging table
-- =============================================================================
--  Input Parameters :
--    None
--  Output Parameters :
-- -----------------------------------------------------------------------------
--
   PROCEDURE get_data
   IS
      l_return_status    VARCHAR2 (1);
      l_error_message    VARCHAR2 (2000);
      l_last_stmt        NUMBER;
      l_log_err_msg      VARCHAR2 (2000);
      l_log_ret_status   VARCHAR2 (50);

      TYPE claims_ext_rec IS RECORD (
         leg_customer_trx_id             xxar_openclaim_ext_r12.leg_customer_trx_id%TYPE, --C9988598

         interface_txn_id                xxar_openclaim_ext_r12.interface_txn_id%TYPE,
         batch_id                        xxar_openclaim_ext_r12.batch_id%TYPE,
         run_sequence_id                 xxar_openclaim_ext_r12.run_sequence_id%TYPE,
         leg_customer_number             xxar_openclaim_ext_r12.leg_customer_number%TYPE,
         leg_bill_to_address             xxar_openclaim_ext_r12.leg_bill_to_address%TYPE,
         leg_ship_to_address             xxar_openclaim_ext_r12.leg_ship_to_address%TYPE,
         leg_currency_code               xxar_openclaim_ext_r12.leg_currency_code%TYPE,
         leg_amount                      xxar_openclaim_ext_r12.leg_amount%TYPE,
         leg_cust_trx_type_name          xxar_openclaim_ext_r12.leg_cust_trx_type_name%TYPE,
         leg_trx_date                    xxar_openclaim_ext_r12.leg_trx_date%TYPE,
         leg_conversion_date             xxar_openclaim_ext_r12.leg_conversion_date%TYPE,
         leg_conversion_rate             xxar_openclaim_ext_r12.leg_conversion_rate%TYPE,
         leg_term_name                   xxar_openclaim_ext_r12.leg_term_name%TYPE,
         leg_operating_unit              xxar_openclaim_ext_r12.leg_operating_unit%TYPE,
         leg_header_attribute_category   xxar_openclaim_ext_r12.leg_header_attribute_category%TYPE,
         leg_header_attribute1           xxar_openclaim_ext_r12.leg_header_attribute1%TYPE,
         leg_header_attribute2           xxar_openclaim_ext_r12.leg_header_attribute2%TYPE,
         leg_header_attribute3           xxar_openclaim_ext_r12.leg_header_attribute3%TYPE,
         leg_header_attribute4           xxar_openclaim_ext_r12.leg_header_attribute4%TYPE,
         leg_sales_order                 xxar_openclaim_ext_r12.leg_sales_order%TYPE,
         leg_purchase_order              xxar_openclaim_ext_r12.leg_purchase_order%TYPE,
         leg_trx_number                  xxar_openclaim_ext_r12.leg_trx_number%TYPE,
         leg_gl_date                     xxar_openclaim_ext_r12.leg_gl_date%TYPE,
         leg_conversion_type             xxar_openclaim_ext_r12.leg_conversion_type%TYPE,
         leg_account_class               xxar_openclaim_ext_r12.leg_account_class%TYPE,
         leg_dist_segment1               xxar_openclaim_ext_r12.leg_dist_segment1%TYPE,
         leg_dist_segment2               xxar_openclaim_ext_r12.leg_dist_segment2%TYPE,
         leg_dist_segment3               xxar_openclaim_ext_r12.leg_dist_segment3%TYPE,
         leg_dist_segment4               xxar_openclaim_ext_r12.leg_dist_segment4%TYPE,
         leg_dist_segment5               xxar_openclaim_ext_r12.leg_dist_segment5%TYPE,
         leg_dist_segment6               xxar_openclaim_ext_r12.leg_dist_segment6%TYPE,
         leg_dist_segment7               xxar_openclaim_ext_r12.leg_dist_segment7%TYPE,
         leg_dist_segment8               xxar_openclaim_ext_r12.leg_dist_segment8%TYPE,
         leg_dist_segment9               xxar_openclaim_ext_r12.leg_dist_segment9%TYPE,
         leg_dist_segment10              xxar_openclaim_ext_r12.leg_dist_segment10%TYPE,
         leg_reason_code                 xxar_openclaim_ext_r12.leg_reason_code%TYPE,
         leg_reference                   xxar_openclaim_ext_r12.leg_reference%TYPE,
         leg_comments                    xxar_openclaim_ext_r12.leg_comments%TYPE,
         leg_receipt_date                xxar_openclaim_ext_r12.leg_receipt_date%TYPE,
         leg_receipt_gl_date             xxar_openclaim_ext_r12.leg_receipt_gl_date%TYPE,
         leg_receipt_maturity_date       xxar_openclaim_ext_r12.leg_receipt_maturity_date%TYPE,
         leg_receipt_number              xxar_openclaim_ext_r12.leg_receipt_number%TYPE,
         leg_receipt_method              xxar_openclaim_ext_r12.leg_receipt_method%TYPE,
         leg_claim_owner                 xxar_openclaim_ext_r12.leg_claim_owner%TYPE,
         leg_due_date                    xxar_openclaim_ext_r12.leg_due_date%TYPE,
         customer_trx_line_id            xxar_openclaim_ext_r12.customer_trx_line_id%TYPE,
         trx_number                      xxar_openclaim_ext_r12.trx_number%TYPE,
         TYPE                            xxar_openclaim_ext_r12.TYPE%TYPE,
         line_number                     xxar_openclaim_ext_r12.line_number%TYPE,
         line_type                       xxar_openclaim_ext_r12.line_type%TYPE,
         description                     xxar_openclaim_ext_r12.description%TYPE,
         header_attribute_category       xxar_openclaim_ext_r12.header_attribute_category%TYPE,
         header_attribute1               xxar_openclaim_ext_r12.header_attribute1%TYPE,
         header_attribute2               xxar_openclaim_ext_r12.header_attribute2%TYPE,
         header_attribute3               xxar_openclaim_ext_r12.header_attribute3%TYPE,
         header_attribute4               xxar_openclaim_ext_r12.header_attribute4%TYPE,
         interface_header_attribute1     xxar_openclaim_ext_r12.interface_header_attribute1%TYPE, --C9988598
     interface_header_attribute5     xxar_openclaim_ext_r12.interface_header_attribute5%TYPE, --v1.14 Reason Code for NAFSC
         interface_line_context          xxar_openclaim_ext_r12.interface_line_context%TYPE,
         interface_line_attribute1       xxar_openclaim_ext_r12.interface_line_attribute1%TYPE,
         interface_line_attribute2       xxar_openclaim_ext_r12.interface_line_attribute2%TYPE,
         interface_line_attribute3       xxar_openclaim_ext_r12.interface_line_attribute3%TYPE,
         interface_line_attribute4       xxar_openclaim_ext_r12.interface_line_attribute4%TYPE,
         interface_line_attribute5       xxar_openclaim_ext_r12.interface_line_attribute5%TYPE,
         interface_line_attribute6       xxar_openclaim_ext_r12.interface_line_attribute6%TYPE,
         interface_line_attribute7       xxar_openclaim_ext_r12.interface_line_attribute7%TYPE,
         interface_line_attribute8       xxar_openclaim_ext_r12.interface_line_attribute8%TYPE,
         interface_line_attribute9       xxar_openclaim_ext_r12.interface_line_attribute9%TYPE,
         interface_line_attribute10      xxar_openclaim_ext_r12.interface_line_attribute10%TYPE,
         interface_line_attribute11      xxar_openclaim_ext_r12.interface_line_attribute11%TYPE,
         interface_line_attribute12      xxar_openclaim_ext_r12.interface_line_attribute12%TYPE,
         interface_line_attribute13      xxar_openclaim_ext_r12.interface_line_attribute13%TYPE,
         interface_line_attribute14      xxar_openclaim_ext_r12.interface_line_attribute14%TYPE,
         interface_line_attribute15      xxar_openclaim_ext_r12.interface_line_attribute15%TYPE,
         system_bill_customer_id         xxar_openclaim_ext_r12.system_bill_customer_id%TYPE,
         system_bill_customer_ref        xxar_openclaim_ext_r12.system_bill_customer_ref%TYPE,
         system_bill_address_id          xxar_openclaim_ext_r12.system_bill_address_id%TYPE,
         system_bill_address_ref         xxar_openclaim_ext_r12.system_bill_address_ref%TYPE,
         system_bill_contact_id          xxar_openclaim_ext_r12.system_bill_contact_id%TYPE,
         system_ship_customer_id         xxar_openclaim_ext_r12.system_ship_customer_id%TYPE,
         system_ship_customer_ref        xxar_openclaim_ext_r12.system_ship_customer_ref%TYPE,
         system_ship_address_id          xxar_openclaim_ext_r12.system_ship_address_id%TYPE,
         system_ship_address_ref         xxar_openclaim_ext_r12.system_ship_address_ref%TYPE,
         system_ship_contact_id          xxar_openclaim_ext_r12.system_ship_contact_id%TYPE,
         system_sold_customer_id         xxar_openclaim_ext_r12.system_sold_customer_id%TYPE,
         system_sold_customer_ref        xxar_openclaim_ext_r12.system_sold_customer_ref%TYPE,
         term_name                       xxar_openclaim_ext_r12.term_name%TYPE,
         ou_name                         xxar_openclaim_ext_r12.ou_name%TYPE,
         conversion_type                 xxar_openclaim_ext_r12.conversion_type%TYPE,
         conversion_date                 xxar_openclaim_ext_r12.conversion_date%TYPE,
         conversion_rate                 xxar_openclaim_ext_r12.conversion_rate%TYPE,
         trx_date                        xxar_openclaim_ext_r12.trx_date%TYPE,
         batch_source_name               xxar_openclaim_ext_r12.batch_source_name%TYPE,
         purchase_order                  xxar_openclaim_ext_r12.purchase_order%TYPE,
         purchase_order_date             xxar_openclaim_ext_r12.purchase_order_date%TYPE,
         sales_order_date                xxar_openclaim_ext_r12.sales_order_date%TYPE,
         sales_order                     xxar_openclaim_ext_r12.sales_order%TYPE,
         sales_order_line                xxar_openclaim_ext_r12.sales_order_line%TYPE,
         term_id                         xxar_openclaim_ext_r12.term_id%TYPE,
         org_id                          xxar_openclaim_ext_r12.org_id%TYPE,
         transaction_type_id             xxar_openclaim_ext_r12.transaction_type_id%TYPE,
         gl_date                         xxar_openclaim_ext_r12.gl_date%TYPE,
         account_class                   xxar_openclaim_ext_r12.account_class%TYPE,
         dist_segment1                   xxar_openclaim_ext_r12.dist_segment1%TYPE,
         dist_segment2                   xxar_openclaim_ext_r12.dist_segment2%TYPE,
         dist_segment3                   xxar_openclaim_ext_r12.dist_segment3%TYPE,
         dist_segment4                   xxar_openclaim_ext_r12.dist_segment4%TYPE,
         dist_segment5                   xxar_openclaim_ext_r12.dist_segment5%TYPE,
         dist_segment6                   xxar_openclaim_ext_r12.dist_segment6%TYPE,
         dist_segment7                   xxar_openclaim_ext_r12.dist_segment7%TYPE,
         dist_segment8                   xxar_openclaim_ext_r12.dist_segment8%TYPE,
         dist_segment9                   xxar_openclaim_ext_r12.dist_segment9%TYPE,
         dist_segment10                  xxar_openclaim_ext_r12.dist_segment10%TYPE,
         amount                          xxar_openclaim_ext_r12.amount%TYPE,
         reason_code                     xxar_openclaim_ext_r12.reason_code%TYPE,
         REFERENCE                       xxar_openclaim_ext_r12.REFERENCE%TYPE,
         comments                        xxar_openclaim_ext_r12.comments%TYPE,
         receipt_method                  xxar_openclaim_ext_r12.receipt_method%TYPE,
         receipt_method_id               xxar_openclaim_ext_r12.receipt_method_id%TYPE,
         cash_receipt_id                 xxar_openclaim_ext_r12.cash_receipt_id%TYPE,
         creation_date                   xxar_openclaim_ext_r12.creation_date%TYPE,
         created_by                      xxar_openclaim_ext_r12.created_by%TYPE,
         last_updated_date               xxar_openclaim_ext_r12.last_updated_date%TYPE,
         last_updated_by                 xxar_openclaim_ext_r12.last_updated_by%TYPE,
         last_update_login               xxar_openclaim_ext_r12.last_update_login%TYPE,
         program_application_id          xxar_openclaim_ext_r12.program_application_id%TYPE,
         program_id                      xxar_openclaim_ext_r12.program_id%TYPE,
         program_update_date             xxar_openclaim_ext_r12.program_update_date%TYPE,
         request_id                      xxar_openclaim_ext_r12.request_id%TYPE,
         process_flag                    xxar_openclaim_ext_r12.process_flag%TYPE,
         ERROR_TYPE                      xxar_openclaim_ext_r12.ERROR_TYPE%TYPE,
         attribute_category              xxar_openclaim_ext_r12.attribute_category%TYPE,
         attribute1                      xxar_openclaim_ext_r12.attribute1%TYPE,
         attribute2                      xxar_openclaim_ext_r12.attribute2%TYPE,
         attribute3                      xxar_openclaim_ext_r12.attribute3%TYPE,
         attribute4                      xxar_openclaim_ext_r12.attribute4%TYPE,
         attribute5                      xxar_openclaim_ext_r12.attribute5%TYPE,
         attribute6                      xxar_openclaim_ext_r12.attribute6%TYPE,
         attribute7                      xxar_openclaim_ext_r12.attribute7%TYPE,
         attribute8                      xxar_openclaim_ext_r12.attribute8%TYPE,
         attribute9                      xxar_openclaim_ext_r12.attribute9%TYPE,
         attribute10                     xxar_openclaim_ext_r12.attribute10%TYPE,
         attribute11                     xxar_openclaim_ext_r12.attribute11%TYPE,
         attribute12                     xxar_openclaim_ext_r12.attribute12%TYPE,
         attribute13                     xxar_openclaim_ext_r12.attribute13%TYPE,
         attribute14                     xxar_openclaim_ext_r12.attribute14%TYPE,
         attribute15                     xxar_openclaim_ext_r12.attribute15%TYPE,
         leg_source_system               xxar_openclaim_ext_r12.leg_source_system%TYPE,
         leg_request_id                  xxar_openclaim_ext_r12.leg_request_id%TYPE,
         leg_seq_num                     xxar_openclaim_ext_r12.leg_seq_num%TYPE,
         leg_process_flag                xxar_openclaim_ext_r12.leg_process_flag%TYPE
      );

      -- PLSQL Table based on Record Type
      TYPE claims_ext_tbl IS TABLE OF claims_ext_rec
         INDEX BY BINARY_INTEGER;

      l_claims_ext_tbl   claims_ext_tbl;
      l_err_record       NUMBER;

      -- Extraction Table cursor
         -- Extraction Table cursor
      CURSOR ext_claims_cur
      IS
         SELECT
                xoer.leg_customer_trx_id, --C9988598
                xoer.interface_txn_id,
                xoer.batch_id,
                xoer.run_sequence_id,
                xoer.leg_customer_number,
                xoer.leg_bill_to_address,
                xoer.leg_ship_to_address,
                xoer.leg_currency_code,
                xoer.leg_amount,
                xoer.leg_cust_trx_type_name,
                xoer.leg_trx_date,
                xoer.leg_conversion_date,
                xoer.leg_conversion_rate,
                xoer.leg_term_name,
                xoer.leg_operating_unit,
                xoer.leg_header_attribute_category,
                xoer.leg_header_attribute1,
                xoer.leg_header_attribute2,
                xoer.leg_header_attribute3,
                xoer.leg_header_attribute4,
                xoer.leg_sales_order,
                xoer.leg_purchase_order,
                xoer.leg_trx_number,
                xoer.leg_gl_date,
                xoer.leg_conversion_type,
                xoer.leg_account_class,
                xoer.leg_dist_segment1,
                xoer.leg_dist_segment2,
                xoer.leg_dist_segment3,
                xoer.leg_dist_segment4,
                xoer.leg_dist_segment5,
                xoer.leg_dist_segment6,
                xoer.leg_dist_segment7,
                xoer.leg_dist_segment8,
                xoer.leg_dist_segment9,
                xoer.leg_dist_segment10,
                xoer.leg_reason_code,
                xoer.leg_reference,
                xoer.leg_comments,
                xoer.leg_receipt_date,
                xoer.leg_receipt_gl_date,
                xoer.leg_receipt_maturity_date,
                xoer.leg_receipt_number,
                xoer.leg_receipt_method,
                xoer.leg_claim_owner,
                xoer.leg_due_date,
                xoer.customer_trx_line_id,
                xoer.trx_number,
                xoer.TYPE,
                xoer.line_number,
                xoer.line_type,
                xoer.description,
                xoer.header_attribute_category,
                xoer.header_attribute1,
                xoer.header_attribute2,
                xoer.header_attribute3,
                xoer.header_attribute4,
                xoer.interface_header_attribute1, --C9988598
        xoer.interface_header_attribute5, --v1.14 reason code for NAFSC
                xoer.interface_line_context,
                xoer.interface_line_attribute1,
                xoer.interface_line_attribute2,
                xoer.interface_line_attribute3,
                xoer.interface_line_attribute4,
                xoer.interface_line_attribute5,
                xoer.interface_line_attribute6,
                xoer.interface_line_attribute7,
                xoer.interface_line_attribute8,
                xoer.interface_line_attribute9,
                xoer.interface_line_attribute10,
                xoer.interface_line_attribute11,
                xoer.interface_line_attribute12,
                xoer.interface_line_attribute13,
                xoer.interface_line_attribute14,
                xoer.interface_line_attribute15,
                xoer.system_bill_customer_id,
                xoer.system_bill_customer_ref,
                xoer.system_bill_address_id,
                xoer.system_bill_address_ref,
                xoer.system_bill_contact_id,
                xoer.system_ship_customer_id,
                xoer.system_ship_customer_ref,
                xoer.system_ship_address_id,
                xoer.system_ship_address_ref,
                xoer.system_ship_contact_id,
                xoer.system_sold_customer_id,
                xoer.system_sold_customer_ref,
                xoer.term_name,
                xoer.ou_name,
                xoer.conversion_type,
                xoer.conversion_date,
                xoer.conversion_rate,
                xoer.trx_date,
                xoer.batch_source_name,
                xoer.purchase_order,
                xoer.purchase_order_date,
                xoer.sales_order_date,
                xoer.sales_order,
                xoer.sales_order_line,
                xoer.term_id,
                xoer.org_id,
                xoer.transaction_type_id,
                xoer.gl_date,
                xoer.account_class,
                xoer.dist_segment1,
                xoer.dist_segment2,
                xoer.dist_segment3,
                xoer.dist_segment4,
                xoer.dist_segment5,
                xoer.dist_segment6,
                xoer.dist_segment7,
                xoer.dist_segment8,
                xoer.dist_segment9,
                xoer.dist_segment10,
                xoer.amount,
                xoer.reason_code,
                xoer.REFERENCE,
                xoer.comments,
                xoer.receipt_method,
                xoer.receipt_method_id,
                xoer.cash_receipt_id,
                g_sysdate,
                g_user_id,
                g_sysdate,
                g_user_id,
                g_login_id,
                g_prog_appl_id,
                g_conc_program_id,
                g_sysdate,
                g_request_id,
                g_new,
                xoer.ERROR_TYPE,
                xoer.attribute_category,
                xoer.attribute1,
                xoer.attribute2,
                xoer.attribute3,
                xoer.attribute4,
                xoer.attribute5,
                xoer.attribute6,
                xoer.attribute7,
                xoer.attribute8,
                xoer.attribute9,
                xoer.attribute10,
                xoer.attribute11,
                xoer.attribute12,
                xoer.attribute13,
                xoer.attribute14,
                xoer.attribute15,
                xoer.leg_source_system,
                xoer.leg_request_id,
                xoer.leg_seq_num,
                xoer.leg_process_flag
           FROM xxar_openclaim_ext_r12 xoer
          WHERE xoer.leg_process_flag = g_valid
            AND NOT EXISTS (
                            SELECT 1
                              FROM xxar_openclaim_stg xos
                             WHERE xos.interface_txn_id =
                                                         xoer.interface_txn_id);
   BEGIN
      print_log_message ('+ Start of Get Data +');
      l_return_status := fnd_api.g_ret_sts_success;
      l_error_message := NULL;
      g_total_count := 0;
      g_failed_count := 0;

      -- Open Cursor
      OPEN ext_claims_cur;

      LOOP
         l_claims_ext_tbl.DELETE;

         FETCH ext_claims_cur
         BULK COLLECT INTO l_claims_ext_tbl LIMIT 1000;

         --limit size of Bulk Collect

         -- Get Total Count
         g_total_count := g_total_count + l_claims_ext_tbl.COUNT;
         EXIT WHEN l_claims_ext_tbl.COUNT = 0;

         BEGIN
            -- Bulk Insert into Conversion table
            FORALL indx IN 1 .. l_claims_ext_tbl.COUNT SAVE EXCEPTIONS
               INSERT INTO xxar_openclaim_stg
                           (
                            leg_customer_trx_id, --C9988598
                            interface_txn_id,
                            batch_id,
                            run_sequence_id,
                            leg_customer_number,
                            leg_bill_to_address,
                            leg_ship_to_address,
                            leg_currency_code,
                            leg_amount,
                            leg_cust_trx_type_name,
                            leg_trx_date,
                            leg_conversion_date,
                            leg_conversion_rate,
                            leg_term_name,
                            leg_operating_unit,
                            leg_header_attribute_category,
                            leg_header_attribute1,
                            leg_header_attribute2,
                            leg_header_attribute3,
                            leg_header_attribute4,
                            leg_sales_order,
                            leg_purchase_order,
                            leg_trx_number,
                            leg_gl_date,
                            leg_conversion_type,
                            leg_account_class,
                            leg_dist_segment1,
                            leg_dist_segment2,
                            leg_dist_segment3,
                            leg_dist_segment4,
                            leg_dist_segment5,
                            leg_dist_segment6,
                            leg_dist_segment7,
                            leg_dist_segment8,
                            leg_dist_segment9,
                            leg_dist_segment10,
                            leg_reason_code,
                            leg_reference,
                            leg_comments,
                            leg_receipt_date,
                            leg_receipt_gl_date,
                            leg_receipt_maturity_date,
                            leg_receipt_number,
                            leg_receipt_method,
                            leg_claim_owner,
                            leg_due_date,
                            customer_trx_line_id,
                            trx_number,
                            TYPE,
                            line_number,
                            line_type,
                            description,
                            header_attribute_category,
                            header_attribute1,
                            header_attribute2,
                            header_attribute3,
                            header_attribute4
-- Ver1.2 Changes start
/*                       , interface_line_context
                       , interface_line_attribute1
                       , interface_line_attribute2
                       , interface_line_attribute3
                       , interface_line_attribute4
                       , interface_line_attribute5
                       , interface_line_attribute6
                       , interface_line_attribute7
                       , interface_line_attribute8
                       , interface_line_attribute9
                       , interface_line_attribute10
                       , interface_line_attribute11
                       , interface_line_attribute12
                       , interface_line_attribute13
                       , interface_line_attribute14
                       , interface_line_attribute15
*/
-- Ver1.2 Changes end
               ,
                            interface_header_attribute1, --C9988598
                            system_bill_customer_id,
                            system_bill_customer_ref,
                            system_bill_address_id,
                            system_bill_address_ref,
                            system_bill_contact_id,
                            system_ship_customer_id,
                            system_ship_customer_ref,
                            system_ship_address_id,
                            system_ship_address_ref,
                            system_ship_contact_id,
                            system_sold_customer_id,
                            system_sold_customer_ref,
                            term_name,
                            ou_name,
                            conversion_type,
                            conversion_date,
                            conversion_rate,
                            trx_date,
                            batch_source_name,
                            purchase_order,
                            purchase_order_date,
                            sales_order_date,
                            sales_order,
                            sales_order_line,
                            term_id,
                            org_id,
                            transaction_type_id,
                            gl_date,
                            account_class,
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
                            amount,
                            reason_code,
                            REFERENCE,
                            comments,
                            receipt_method,
                            receipt_method_id,
                            cash_receipt_id,
                            creation_date,
                            created_by,
                            last_updated_date,
                            last_updated_by,
                            last_update_login,
                            program_application_id,
                            program_id,
                            program_update_date,
                            request_id,
                            process_flag,
                            ERROR_TYPE,
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
                            leg_source_system,
                            leg_request_id,
                            leg_seq_num,
                            leg_process_flag
                           )
                    VALUES (
                            l_claims_ext_tbl (indx).leg_customer_trx_id, --C9988598
                            l_claims_ext_tbl (indx).interface_txn_id,
                            l_claims_ext_tbl (indx).batch_id,
                            l_claims_ext_tbl (indx).run_sequence_id,
                            l_claims_ext_tbl (indx).leg_customer_number,
                            l_claims_ext_tbl (indx).leg_bill_to_address,
                            l_claims_ext_tbl (indx).leg_ship_to_address,
                            l_claims_ext_tbl (indx).leg_currency_code,
                            l_claims_ext_tbl (indx).leg_amount,
                            l_claims_ext_tbl (indx).leg_cust_trx_type_name,
                            l_claims_ext_tbl (indx).leg_trx_date,
                            l_claims_ext_tbl (indx).leg_conversion_date,
                            l_claims_ext_tbl (indx).leg_conversion_rate,
                            l_claims_ext_tbl (indx).leg_term_name,
                            l_claims_ext_tbl (indx).leg_operating_unit,
                            l_claims_ext_tbl (indx).leg_header_attribute_category,
                            l_claims_ext_tbl (indx).leg_header_attribute1,
                            l_claims_ext_tbl (indx).leg_header_attribute2,
                            l_claims_ext_tbl (indx).leg_header_attribute3,
                            l_claims_ext_tbl (indx).leg_header_attribute4,
                            l_claims_ext_tbl (indx).leg_sales_order,
                            l_claims_ext_tbl (indx).leg_purchase_order,
                            l_claims_ext_tbl (indx).leg_trx_number,
                            l_claims_ext_tbl (indx).leg_gl_date,
                            l_claims_ext_tbl (indx).leg_conversion_type,
                            l_claims_ext_tbl (indx).leg_account_class,
                            l_claims_ext_tbl (indx).leg_dist_segment1,
                            l_claims_ext_tbl (indx).leg_dist_segment2,
                            l_claims_ext_tbl (indx).leg_dist_segment3,
                            l_claims_ext_tbl (indx).leg_dist_segment4,
                            l_claims_ext_tbl (indx).leg_dist_segment5,
                            l_claims_ext_tbl (indx).leg_dist_segment6,
                            l_claims_ext_tbl (indx).leg_dist_segment7,
                            l_claims_ext_tbl (indx).leg_dist_segment8,
                            l_claims_ext_tbl (indx).leg_dist_segment9,
                            l_claims_ext_tbl (indx).leg_dist_segment10,
                            --l_claims_ext_tbl (indx).leg_reason_code, -- v1.14 reason code for NAFSC 
              l_claims_ext_tbl (indx).interface_header_attribute5, --v1.14 reason code for NAFSC 
                            l_claims_ext_tbl (indx).leg_reference,
                            l_claims_ext_tbl (indx).leg_comments,
                            l_claims_ext_tbl (indx).leg_receipt_date,
                            l_claims_ext_tbl (indx).leg_receipt_gl_date,
                            l_claims_ext_tbl (indx).leg_receipt_maturity_date,
                            l_claims_ext_tbl (indx).leg_receipt_number,
                            l_claims_ext_tbl (indx).leg_receipt_method,
                            l_claims_ext_tbl (indx).leg_claim_owner,
                            l_claims_ext_tbl (indx).leg_due_date,
                            l_claims_ext_tbl (indx).customer_trx_line_id,
                            l_claims_ext_tbl (indx).trx_number,
                            l_claims_ext_tbl (indx).TYPE,
                            l_claims_ext_tbl (indx).line_number,
                            l_claims_ext_tbl (indx).line_type,
                            l_claims_ext_tbl (indx).description,
                            l_claims_ext_tbl (indx).header_attribute_category,
                            l_claims_ext_tbl (indx).header_attribute1,
                            l_claims_ext_tbl (indx).header_attribute2,
                            l_claims_ext_tbl (indx).header_attribute3,
                            l_claims_ext_tbl (indx).header_attribute4
-- Ver1.2 Changes start
/*                        , l_claims_ext_tbl (indx).interface_line_context
                        , l_claims_ext_tbl (indx).interface_line_attribute1
                        , l_claims_ext_tbl (indx).interface_line_attribute2
                        , l_claims_ext_tbl (indx).interface_line_attribute3
                        , l_claims_ext_tbl (indx).interface_line_attribute4
                        , l_claims_ext_tbl (indx).interface_line_attribute5
                        , l_claims_ext_tbl (indx).interface_line_attribute6
                        , l_claims_ext_tbl (indx).interface_line_attribute7
                        , l_claims_ext_tbl (indx).interface_line_attribute8
                        , l_claims_ext_tbl (indx).interface_line_attribute9
                        , l_claims_ext_tbl (indx).interface_line_attribute10
                        , l_claims_ext_tbl (indx).interface_line_attribute11
                        , l_claims_ext_tbl (indx).interface_line_attribute12
                        , l_claims_ext_tbl (indx).interface_line_attribute13
                        , l_claims_ext_tbl (indx).interface_line_attribute14
                        , l_claims_ext_tbl (indx).interface_line_attribute15
*/
-- Ver1.2 Changes end
               ,
                            l_claims_ext_tbl (indx).interface_header_attribute1, --C9988598
                            l_claims_ext_tbl (indx).system_bill_customer_id,
                            l_claims_ext_tbl (indx).system_bill_customer_ref,
                            l_claims_ext_tbl (indx).system_bill_address_id,
                            l_claims_ext_tbl (indx).system_bill_address_ref,
                            l_claims_ext_tbl (indx).system_bill_contact_id,
                            l_claims_ext_tbl (indx).system_ship_customer_id,
                            l_claims_ext_tbl (indx).system_ship_customer_ref,
                            l_claims_ext_tbl (indx).system_ship_address_id,
                            l_claims_ext_tbl (indx).system_ship_address_ref,
                            l_claims_ext_tbl (indx).system_ship_contact_id,
                            l_claims_ext_tbl (indx).system_sold_customer_id,
                            l_claims_ext_tbl (indx).system_sold_customer_ref,
                            l_claims_ext_tbl (indx).term_name,
                            l_claims_ext_tbl (indx).ou_name,
                            l_claims_ext_tbl (indx).conversion_type,
                            l_claims_ext_tbl (indx).conversion_date,
                            l_claims_ext_tbl (indx).conversion_rate,
                            l_claims_ext_tbl (indx).trx_date,
                            l_claims_ext_tbl (indx).batch_source_name,
                            l_claims_ext_tbl (indx).purchase_order,
                            l_claims_ext_tbl (indx).purchase_order_date,
                            l_claims_ext_tbl (indx).sales_order_date,
                            l_claims_ext_tbl (indx).sales_order,
                            l_claims_ext_tbl (indx).sales_order_line,
                            l_claims_ext_tbl (indx).term_id,
                            l_claims_ext_tbl (indx).org_id,
                            l_claims_ext_tbl (indx).transaction_type_id,
                            l_claims_ext_tbl (indx).gl_date,
                            l_claims_ext_tbl (indx).account_class,
                            l_claims_ext_tbl (indx).dist_segment1,
                            l_claims_ext_tbl (indx).dist_segment2,
                            l_claims_ext_tbl (indx).dist_segment3,
                            l_claims_ext_tbl (indx).dist_segment4,
                            l_claims_ext_tbl (indx).dist_segment5,
                            l_claims_ext_tbl (indx).dist_segment6,
                            l_claims_ext_tbl (indx).dist_segment7,
                            l_claims_ext_tbl (indx).dist_segment8,
                            l_claims_ext_tbl (indx).dist_segment9,
                            l_claims_ext_tbl (indx).dist_segment10,
                            l_claims_ext_tbl (indx).amount,
                            l_claims_ext_tbl (indx).reason_code,
                            l_claims_ext_tbl (indx).REFERENCE,
                            l_claims_ext_tbl (indx).comments,
                            l_claims_ext_tbl (indx).receipt_method,
                            l_claims_ext_tbl (indx).receipt_method_id,
                            l_claims_ext_tbl (indx).cash_receipt_id,
                            g_sysdate,
                            g_user_id,
                            g_sysdate,
                            g_user_id,
                            g_login_id,
                            g_prog_appl_id,
                            g_conc_program_id,
                            g_sysdate,
                            g_request_id,
                            g_new,
                            l_claims_ext_tbl (indx).ERROR_TYPE,
                            l_claims_ext_tbl (indx).attribute_category,
                            l_claims_ext_tbl (indx).attribute1,
                            l_claims_ext_tbl (indx).attribute2,
                            l_claims_ext_tbl (indx).attribute3,
                            l_claims_ext_tbl (indx).attribute4,
                            l_claims_ext_tbl (indx).attribute5,
                            l_claims_ext_tbl (indx).attribute6,
                            l_claims_ext_tbl (indx).attribute7,
                            l_claims_ext_tbl (indx).attribute8,
                            l_claims_ext_tbl (indx).attribute9,
                            l_claims_ext_tbl (indx).attribute10,
                            l_claims_ext_tbl (indx).attribute11,
                            l_claims_ext_tbl (indx).attribute12,
                            l_claims_ext_tbl (indx).attribute13,
                            l_claims_ext_tbl (indx).attribute14,
                            l_claims_ext_tbl (indx).attribute15,
                            l_claims_ext_tbl (indx).leg_source_system,
                            l_claims_ext_tbl (indx).leg_request_id,
                            l_claims_ext_tbl (indx).leg_seq_num,
                            l_claims_ext_tbl (indx).leg_process_flag
                           );
         EXCEPTION
            WHEN OTHERS
            THEN
               FOR l_indx_exp IN 1 .. SQL%BULK_EXCEPTIONS.COUNT
               LOOP
                  l_err_record :=
                     l_claims_ext_tbl
                              (SQL%BULK_EXCEPTIONS (l_indx_exp).ERROR_INDEX).interface_txn_id;
                  g_retcode := '1';
                  fnd_file.put_line
                     (fnd_file.LOG,
                         'Record sequence : '
                      || l_claims_ext_tbl
                               (SQL%BULK_EXCEPTIONS (l_indx_exp).ERROR_INDEX).interface_txn_id
                     );
                  fnd_file.put_line
                       (fnd_file.LOG,
                           'Error Message : '
                        || SQLERRM
                               (-SQL%BULK_EXCEPTIONS (l_indx_exp).ERROR_CODE)
                       );

                  -- Updating Leg_process_flag to 'E' for failed records
                  UPDATE xxar_openclaim_ext_r12 xoer
                     SET leg_process_flag = g_error,
                         last_updated_by = g_user_id,
                         last_update_login = g_login_id,
                         last_updated_date = SYSDATE
                   WHERE xoer.interface_txn_id = l_err_record
                     AND xoer.leg_process_flag = g_valid;

                  g_failed_count := g_failed_count + SQL%ROWCOUNT;
                  l_error_message :=
                        l_error_message
                     || ' ~~ '
                     || SQLERRM
                               (-SQL%BULK_EXCEPTIONS (l_indx_exp).ERROR_CODE);
               END LOOP;
         END;
      END LOOP;

      CLOSE ext_claims_cur;                                    -- Close Cursor

      -- Update Successful records in Extraction Table
      UPDATE xxar_openclaim_ext_r12 stg2
         SET leg_process_flag = g_interface,
             last_updated_by = g_user_id,
             last_update_login = g_login_id,
             last_updated_date = SYSDATE
       WHERE leg_process_flag = g_valid
         AND EXISTS (SELECT 1
                       FROM xxar_openclaim_stg stg1
                      WHERE stg1.interface_txn_id = stg2.interface_txn_id);

      COMMIT;
      fnd_file.put_line (fnd_file.output,
                         'Program Name : Eaton Open Claim Conversion Program'
                        );
      fnd_file.put_line (fnd_file.output,
                         'Request ID   : ' || TO_CHAR (g_request_id)
                        );
      fnd_file.put_line (fnd_file.output,
                            'Report Date  : '
                         || TO_CHAR (SYSDATE, 'DD-MON-RRRR HH:MI:SS AM')
                        );
      fnd_file.put_line
         (fnd_file.output,
          '-------------------------------------------------------------------------------------------------'
         );
      fnd_file.put_line (fnd_file.output, 'Parameters  : ');
      fnd_file.put_line (fnd_file.output,
                         '---------------------------------------------'
                        );
      fnd_file.put_line (fnd_file.output,
                         'Run Mode              : ' || g_run_mode
                        );
      fnd_file.put_line (fnd_file.output,
                         'Batch ID            : ' || g_batch_id
                        );
      fnd_file.put_line (fnd_file.output,
                         'Process records     : ' || g_process_records
                        );
      fnd_file.put_line (fnd_file.output,
                         'GL Date             : ' || g_gl_date
                        );
      fnd_file.put_line
         (fnd_file.output,
          '==================================================================================================='
         );
      fnd_file.put_line (fnd_file.output,
                         'Statistics (' || g_run_mode || '):');
      fnd_file.put_line (fnd_file.output,
                         'Records Submitted       : ' || g_total_count
                        );
      fnd_file.put_line (fnd_file.output,
                            'Records Extracted       : '
                         || (g_total_count - g_failed_count)
                        );
      fnd_file.put_line (fnd_file.output,
                         'Records Errored         : ' || g_failed_count
                        );
      fnd_file.put_line (fnd_file.output, CHR (10));
      fnd_file.put_line
         (fnd_file.output,
          '==================================================================================================='
         );
      print_log_message ('- Start of Get Data -');
   EXCEPTION
      WHEN OTHERS
      THEN
         g_retcode := 2;
         l_error_message :=
               SUBSTR ('In Exception while loading data' || SQLERRM, 1, 1999);
         print_log_message (l_error_message);
         print_log_message (' - get_data -');
         ROLLBACK;
   END get_data;

--
-- ========================
-- Procedure: TIE_BACK
-- =============================================================================
--   This procedure is used to tie back any errors in standard import.
--   It also updates process flag in staging table after import
-- =============================================================================
--  Input Parameters :
--  pin_batch_id      : Batch id
--  Output Parameters :
--  pov_errbuf        : Return error
--  pon_retcode       : Return status
-- -----------------------------------------------------------------------------
--
   PROCEDURE tie_back (
      pov_errbuf     OUT NOCOPY      VARCHAR2,
      pon_retcode    OUT NOCOPY      NUMBER,
      pin_batch_id   IN              NUMBER               -- NULL / <BATCH_ID>
   )
   IS
      l_err_msg          VARCHAR2 (4000);
      l_error_flag       VARCHAR2 (10);
      l_return_status    VARCHAR2 (200)  := NULL;
      l_log_ret_status   VARCHAR2 (50);
      l_log_err_msg      VARCHAR2 (2000);
      l_interface_status ra_interface_lines_all.interface_status%TYPE;   -- v1.12

      --cursor to fetch all successfully inserted records
      CURSOR tie_back_cur
      IS
         SELECT   *
             FROM xxar_openclaim_stg xos
            WHERE (xos.process_flag = g_interface
              OR (xos.process_flag = g_error AND xos.error_type = g_err_imp))  -- v1.10
              AND xos.batch_id = pin_batch_id
         ORDER BY leg_source_system, leg_operating_unit, LEG_CUSTOMER_TRX_ID, xos.leg_trx_number; -- v1.11

      --cursor to fetch error details
      CURSOR interface_error_cur (p_in_attr IN VARCHAR2)
      IS
         SELECT ril.interface_line_id,
                rie.MESSAGE_TEXT
           FROM ra_interface_errors_all rie,
                ra_interface_lines_all ril
          WHERE ril.interface_line_id = rie.interface_line_id
--Ver 1.2 Changes start
--         AND    ril.interface_line_attribute1 = p_in_attr
            AND ril.interface_line_attribute15 = p_in_attr
--Ver 1.2 Changes end
            AND ril.interface_line_context = g_interface_line_context;
   BEGIN
      xxetn_debug_pkg.initialize_debug
                             (pov_err_msg           => l_err_msg,
                              piv_program_name      => 'ETN_AR_CLAIM_TIEBACK_CONV'
                             );
      print_log_message (   'Tie Back Starts at: '
                         || TO_CHAR (g_sysdate, 'DD-MON-YYYY HH24:MI:SS')
                        );
      print_log_message ('+ Start of Tie Back + ' || pin_batch_id);
      g_run_seq_id := xxetn_run_sequences_s.NEXTVAL;
      xxetn_common_error_pkg.g_run_seq_id := g_run_seq_id;

      FOR tie_back_rec IN tie_back_cur
      LOOP
         print_log_message (   'Interface Transaction Id = '
                            || tie_back_rec.interface_txn_id
                           );
         print_log_message ('Account Class = ' || tie_back_rec.account_class);
         g_intf_staging_id := tie_back_rec.interface_txn_id;
         l_error_flag := NULL;

--Ver 1.2 Changes start
--         FOR interface_error_rec IN interface_error_cur ( tie_back_rec.interface_line_attribute1 )
         FOR interface_error_rec IN
            interface_error_cur (tie_back_rec.interface_line_attribute15)
--Ver 1.2 Changes end
         LOOP
            print_log_message (   'In error loop: Interface line Id = '
                               || interface_error_rec.interface_line_id
                              );
            print_log_message (   'In error loop: Message Text - '
                               || interface_error_rec.MESSAGE_TEXT
                              );
            l_error_flag := g_error;
            log_errors (pov_return_status            => l_log_ret_status
                                                                        -- OUT
            ,
                        pov_error_msg                => l_log_err_msg,   --OUT
                        piv_source_column_name       => 'Interface Error',
                        piv_source_column_value      => NULL,
                        piv_error_type               => g_err_imp,
                        piv_error_code               => 'ETN_AR_INVOICE_CREATION_FAILED',
                        piv_error_message            => interface_error_rec.MESSAGE_TEXT
                       );
         END LOOP;

         IF l_error_flag = g_error
         THEN
            UPDATE xxar_openclaim_stg xos
               SET process_flag = g_error,
                   ERROR_TYPE = g_err_imp,
                   run_sequence_id = g_run_seq_id,
                   last_updated_date = g_sysdate,
                   last_updated_by = g_user_id,
                   last_update_login = g_login_id,
                   request_id = g_request_id
             WHERE xos.interface_txn_id = tie_back_rec.interface_txn_id;

         ELSE

            /** Added below for v1.12 **/
            l_interface_status := NULL;
            BEGIN
               SELECT rila.interface_status
               INTO l_interface_status
               FROM ra_interface_lines_all rila
               WHERE rila.interface_line_attribute15 = tie_back_rec.interface_line_attribute15
               AND rila.interface_line_context = g_interface_line_context;
            EXCEPTION
               WHEN OTHERS THEN
                l_interface_status := NULL;
            END;

            IF l_interface_status ='P' THEN

            /** Added above for v1.12 **/

               UPDATE xxar_openclaim_stg xos
               SET process_flag = g_waiting,
                   ERROR_TYPE = NULL,
                   run_sequence_id = g_run_seq_id,
                   last_updated_date = g_sysdate,
                   last_updated_by = g_user_id,
                   last_update_login = g_login_id,
                   request_id = g_request_id
               WHERE xos.interface_txn_id = tie_back_rec.interface_txn_id;

            END IF;

         END IF;

         g_intf_staging_id := NULL;
      END LOOP;

      IF l_source_tab.COUNT > 0
      THEN
         xxetn_common_error_pkg.add_error
                                       (pov_return_status      => l_return_status
                                                                                 -- OUT
         ,
                                        pov_error_msg          => l_err_msg
                                                                           -- OUT
         ,
                                        pi_source_tab          => l_source_tab
                                                                              -- IN  G_SOURCE_TAB_TYPE
         ,
                                        pin_batch_id           => pin_batch_id
                                       );
         l_source_tab.DELETE;
      END IF;

      pon_retcode := g_retcode;
      pov_errbuf := g_errbuff;
      print_log_message ('- Start of Tie Back - ' || pin_batch_id);
      print_log_message
         ('+---------------------------------------------------------------------------+'
         );
      print_log_message (   'Tie Back Ends at: '
                         || TO_CHAR (g_sysdate, 'DD-MON-YYYY HH24:MI:SS')
                        );
      print_log_message ('---------------------------------------------');
   EXCEPTION
      WHEN OTHERS
      THEN
         g_retcode := 2;
         g_errbuff := 'Failed During Tie Back';
         print_log_message ('In Tie Back when others' || SQLERRM);
   END tie_back;

--
-- ========================
-- Procedure: create_receipts
-- =============================================================================
--   This procedure to create receipts
-- =============================================================================
--  Input Parameters :
--  pin_batch_id      : Batch id
--  Output Parameters :
--  pov_errbuf        : Standard output parameter for concurrent program
--  pon_retcode       : Standard output parameter for concurrent program
-- -----------------------------------------------------------------------------
--
   PROCEDURE create_receipts (
      pov_errbuf     OUT NOCOPY      VARCHAR2,
      pon_retcode    OUT NOCOPY      NUMBER,
      pin_batch_id   IN              NUMBER               -- NULL / <BATCH_ID>
   )
   IS
      l_receipt_id           NUMBER;
      l_status_flag          VARCHAR2 (1);
      l_error_message        VARCHAR2 (500);
      l_return_status_out    VARCHAR2 (1);
      l_msg_count_out        NUMBER;
      l_msg_data_out         VARCHAR2 (1000);
      l_msg_index_out        NUMBER;
      l_err_msg              VARCHAR2 (4000);
      l_log_ret_status       VARCHAR2 (50);
      l_log_err_msg          VARCHAR2 (2000);
      l_remit_bank_acct_id   NUMBER;
      l_customer_trx_id      NUMBER; -- v1.11
      l_apply_date           DATE;   -- v1.11
      p_attribute_rec        ar_receipt_api_pub.attribute_rec_type;  --v1.14
    l_customer_reference   VARCHAR2 (100);

      --cursor to fetch eligible records for receipt creation
      CURSOR create_receipts_cur
      IS
         SELECT   *
         FROM xxar_openclaim_stg xos
         WHERE (xos.process_flag = g_waiting OR (xos.process_flag = g_error AND xos.error_type = g_err_api))  -- v1.12
         AND xos.account_class = g_acct_rec
         AND xos.batch_id = pin_batch_id
         ORDER BY xos.leg_source_system, -- Added v1.11
                  xos.leg_customer_trx_id, -- Added v1.11
                  xos.leg_trx_number,
                  xos.leg_receipt_number;
                  --xos.account_class;     -- commented for v1.12
   BEGIN
      xxetn_debug_pkg.initialize_debug
                             (pov_err_msg           => l_err_msg,
                              piv_program_name      => 'ETN_AR_CLAIM_RECEIPT_CONV'
                             );
      print_log_message (   'Create Receipt Program Starter at: '
                         || TO_CHAR (g_sysdate, 'DD-MON-YYYY HH24:MI:SS')
                        );
      g_run_seq_id := xxetn_run_sequences_s.NEXTVAL;
      xxetn_common_error_pkg.g_run_seq_id := g_run_seq_id;
      print_log_message (   ' +  PROCEDURE : CREATE_RECEIPT batch id = '
                         || pin_batch_id
                         || ' + '
                        );

      FOR create_receipts_rec IN create_receipts_cur
      LOOP
         l_return_status_out := NULL;
         l_msg_count_out := NULL;
         l_msg_data_out := NULL;
         l_receipt_id := NULL;
         l_error_message := NULL;
         l_msg_index_out := NULL;
         l_status_flag := NULL;
         l_log_ret_status := NULL;
         l_log_err_msg := NULL;
         l_remit_bank_acct_id := NULL;
     l_customer_reference := NULL; --v1.14
         g_intf_staging_id := create_receipts_rec.interface_txn_id;

fnd_file.put_line(fnd_file.log,   'Interface Transaction Id = '
                            || create_receipts_rec.interface_txn_id
							||'Receipt Number = '
                            || create_receipts_rec.leg_receipt_number
							||'Org Id         =' 
							|| create_receipts_rec.org_id)	;

     
     --v1.14 changes start
     IF create_receipts_rec.leg_source_system !=  'ISSC'
         THEN 
       p_attribute_rec.attribute2 := create_receipts_rec.interface_line_attribute1 ;       --v1.14
       
       l_customer_reference := SUBSTR (create_receipts_rec.leg_comments,1,30) ; -- pass as is 
     
     ELSE 
      -- p_attribute_rec.attribute2 := TRIM ( SUBSTR(create_receipts_rec.leg_comments,1, INSTR(create_receipts_rec.leg_comments,'-',1,1)-1)) ; -- v1.18
       
      -- l_customer_reference := SUBSTR (  SUBSTR(create_receipts_rec.leg_comments,INSTR(create_receipts_rec.leg_comments,'-',1,2)+1 ) 
        --                              ,1
          --            ,100 
            --          ) ; -- Comment � Owner � XXXX-XXXXXX-XXXX-XXX, this details will be populated in customer reference field at receipt application level.
--commented for v1.22

p_attribute_rec.attribute2 := TRIM ( SUBSTRB(create_receipts_rec.leg_comments,1, INSTRB(create_receipts_rec.leg_comments,'-',1,1)-1)) ;
l_customer_reference := SUBSTRB (  SUBSTRB(create_receipts_rec.leg_comments,INSTRB(create_receipts_rec.leg_comments,'-',1,2)+1 ) 
     ,1
 ,100   ) ; --added for v1.22
     
     END IF ; 
     --v1.14 changes end
     
         print_log_message (   'Interface Transaction Id = '
                            || create_receipts_rec.interface_txn_id
                           );
         print_log_message (   'Receipt Number = '
                            || create_receipts_rec.leg_receipt_number
                           );
         print_log_message ('Org Id         =' || create_receipts_rec.org_id);
         print_log_message ('+ Checking Receipt Exists +');
fnd_file.put_line(fnd_file.log,  '+ Checking Receipt Exists +');

         BEGIN
            SELECT acpa.cash_receipt_id
            INTO l_receipt_id
            FROM ar_cash_receipts_all acpa
            WHERE acpa.receipt_number = create_receipts_rec.leg_receipt_number
            AND   acpa.receipt_date   = create_receipts_rec.leg_receipt_date    -- added for v1.12
            AND   acpa.amount         = g_receipt_amount                        -- added for v1.12
            AND  NVL(acpa.pay_from_customer, -99999) = NVL(create_receipts_rec.system_sold_customer_id, -99999)    -- added for v1.12
            AND acpa.org_id = create_receipts_rec.org_id                      -- Commented for v1.19  -- Uncommented the condtion for v1.21
            --AND  acpa.type   = p_type
            AND  acpa.status  NOT IN (
                 SELECT  arl.lookup_code FROM ar_lookups arl
                 WHERE   arl.lookup_type  = 'REVERSAL_CATEGORY_TYPE');   -- added for v1.12
         EXCEPTION
            WHEN NO_DATA_FOUND
            THEN
               l_receipt_id := NULL;
               print_log_message ('In No Data Found of receipt number check');
            WHEN OTHERS
            THEN
               l_receipt_id := NULL;
               print_log_message (   'In when others of receipt number check'
                                  || SQLERRM
                                 );
         END;

fnd_file.put_line(fnd_file.log,   'l_receipt_id = '
                            || l_receipt_id);

         print_log_message ('- Checking Receipt Exists -');

         BEGIN
            SELECT rma.remit_bank_acct_use_id
              INTO l_remit_bank_acct_id
              FROM ar_receipt_methods rm,
                   ce_bank_accounts cba,
                   ce_bank_acct_uses_all ba,
                   ar_receipt_method_accounts_all rma
             WHERE rm.receipt_method_id =
                                         create_receipts_rec.receipt_method_id
               AND (TO_DATE (create_receipts_rec.leg_receipt_date)
                       BETWEEN rm.start_date
                           AND NVL
                                 (rm.end_date,
                                  TO_DATE
                                         (create_receipts_rec.leg_receipt_date)
                                 )
                   )
               AND cba.account_classification = 'INTERNAL'
               AND NVL (ba.end_date,
                        TO_DATE (create_receipts_rec.leg_receipt_date) + 1
                       ) > TO_DATE (create_receipts_rec.leg_receipt_date)
               AND TO_DATE (create_receipts_rec.leg_receipt_date)
                      BETWEEN rma.start_date
                          AND NVL
                                (rma.end_date,
                                 TO_DATE (create_receipts_rec.leg_receipt_date)
                                )
               --AND  cba.currency_code = DECODE(cba.receipt_multi_currency_flag, 'Y', cba.currency_code, :p_currency_code)
               AND rm.receipt_method_id = rma.receipt_method_id
               AND rma.remit_bank_acct_use_id = ba.bank_acct_use_id
               AND ba.bank_account_id = cba.bank_account_id
               AND rma.org_id = create_receipts_rec.org_id;

            print_log_message ('l_remit_bank_acct_id: '
                               || l_remit_bank_acct_id
                              );

fnd_file.put_line(fnd_file.log,   'l_remit_bank_acct_id: '
                               || l_remit_bank_acct_id);
         EXCEPTION
            WHEN OTHERS
            THEN
               print_log_message (   'When Others of Remit Bank Acct Id: '
                                  || SQLERRM
                                 );
         END;

         IF l_receipt_id IS NULL
         THEN
            print_log_message ('Before receipt creation API call');
fnd_file.put_line(fnd_file.log,  'Before receipt creation API call');
            --Call Create Cash API
            arp_standard.enable_debug;
            ar_receipt_api_pub.create_cash
               (p_api_version                     => 1.0,
                p_init_msg_list                   => fnd_api.g_true,
                p_commit                          => fnd_api.g_false,
                p_validation_level                => fnd_api.g_valid_level_full,
                p_amount                          => g_receipt_amount,
                p_customer_id                     => create_receipts_rec.system_sold_customer_id,
                p_customer_receipt_reference      => create_receipts_rec.leg_reference
                                                                                      -- v1.1 Changes start - FOT issue
                                                                                             --, p_comments              => create_receipts_rec.leg_comments
                                                                                      -- v1.1 Changes end - FOT issue
            ,
                p_currency_code                   => create_receipts_rec.leg_currency_code,
                p_exchange_rate_type              => create_receipts_rec.conversion_type,
                p_exchange_rate                   => create_receipts_rec.conversion_rate,
                p_exchange_rate_date              => create_receipts_rec.conversion_date,
                p_receipt_number                  => create_receipts_rec.leg_receipt_number,
                p_maturity_date                   => create_receipts_rec.gl_date,
                p_receipt_method_id               => create_receipts_rec.receipt_method_id,
                p_org_id                          => create_receipts_rec.org_id,
                -- p_location                  => create_receipts_rec.LEG_BILL_TO_LOCATION,
                p_receipt_date                    => create_receipts_rec.leg_receipt_date,
                p_gl_date                         => create_receipts_rec.gl_date,
                p_cr_id                           => l_receipt_id,
                x_return_status                   => l_return_status_out,
                x_msg_count                       => l_msg_count_out,
                x_msg_data                        => l_msg_data_out,
                p_remittance_bank_account_id      => l_remit_bank_acct_id
               );
            print_log_message ('After receipt creation API call');
fnd_file.put_line(fnd_file.log,  'After receipt creation API call l_return_status_out : '
			|| l_return_status_out);
            l_error_message :=
                  l_error_message
               || ' Create Cash API Status '
               || l_return_status_out
               || ' cr id '
               || l_receipt_id;

            --Check API status is not successful
            IF l_return_status_out <> fnd_api.g_ret_sts_success
            THEN
               l_status_flag := g_error;

               --Extract error message from API
               IF l_msg_count_out > 1
               THEN
                  FOR i IN 1 .. l_msg_count_out
                  LOOP
                     fnd_msg_pub.get (p_msg_index          => i,
                                      p_encoded            => fnd_api.g_false,
                                      p_data               => l_msg_data_out,
                                      p_msg_index_out      => l_msg_index_out
                                     );
                     l_error_message :=
                           l_error_message
                        || ' Create Cash Return Status '
                        || l_return_status_out
                        || ' | '
                        || l_msg_data_out
                        || REPLACE (l_msg_index_out, CHR (0), NULL);
                  END LOOP;
               ELSIF l_msg_count_out = 1
               THEN
                  l_error_message :=
                     REPLACE (   l_error_message
                              || ' Create Cash Return Status '
                              || l_return_status_out
                              || ' | '
                              || l_msg_data_out,
                              CHR (0),
                              NULL
                             );
               END IF;

               log_errors
                  (pov_return_status            => l_log_ret_status     -- OUT
                                                                   ,
                   pov_error_msg                => l_log_err_msg        -- OUT
                                                                ,
                   piv_source_column_name       => 'Receipt Number',
                   piv_source_column_value      => create_receipts_rec.leg_receipt_number,
                   piv_error_type               => g_err_api,    -- modified for v1.12
                   piv_error_code               => 'ETN_AR_RECEIPT_CREATION_FAILED',
                   piv_error_message            => l_error_message
                  );
            END IF;

            print_log_message ('Message:' || l_error_message);
            print_log_message ('ret_status:' || l_return_status_out);
            print_log_message ('cr id:' || l_receipt_id);
         END IF;                                  --Receipt id already created

         IF l_receipt_id IS NOT NULL
         THEN
            l_error_message := NULL;
            l_return_status_out := NULL;
            l_msg_count_out := NULL;
            l_msg_data_out := NULL;
            l_msg_index_out := NULL;
            print_log_message ('Before Application API');
            print_log_message (g_application_rerefence);
            print_log_message (   'Reason code = '
                               || create_receipts_rec.reason_code
                              );
            print_log_message (   'commentsss'
                               || LENGTH (create_receipts_rec.leg_comments)
                              );
            print_log_message (   'create_receipts_rec.leg_trx_number'
                               || LENGTH (create_receipts_rec.leg_trx_number)
                              );
            print_log_message (   'create_receipts_rec.leg_receipt_date'
                               || LENGTH (create_receipts_rec.leg_receipt_date)
                              );
            print_log_message (   'create_receipts_rec.g_amount_applied'
                               || LENGTH (g_amount_applied)
                              );
            print_log_message (   'create_receipts_rec.gl_date'
                               || LENGTH (create_receipts_rec.gl_date)
                              );
            print_log_message (   'create_receipts_rec.org_id'
                               || LENGTH (create_receipts_rec.org_id)
                              );
            print_log_message
                             (   'create_receipts_rec.g_application_rerefence'
                              || LENGTH (g_application_rerefence)
                             );
            print_log_message (   'create_receipts_rec.reason_code'
                               || LENGTH (create_receipts_rec.reason_code)
                              );
            print_log_message (   'create_receipts_rec.leg_reason_code'
                               || LENGTH (create_receipts_rec.leg_reason_code)
                              );
            arp_standard.enable_debug;

fnd_file.put_line(fnd_file.log,   'Reason code = '
                               || create_receipts_rec.reason_code
							   || ' commentsss = '
                               || LENGTH (create_receipts_rec.leg_comments)
							   || ' create_receipts_rec.leg_trx_number= '
                               || LENGTH (create_receipts_rec.leg_trx_number)
							   || ' create_receipts_rec.leg_receipt_date= '
                               || LENGTH (create_receipts_rec.leg_receipt_date)
							   || 'create_receipts_rec.g_amount_applied'
                               || LENGTH (g_amount_applied)
							   || 'create_receipts_rec.gl_date'
                               || LENGTH (create_receipts_rec.gl_date)
							   || 'create_receipts_rec.org_id'
                               || LENGTH (create_receipts_rec.org_id)
							   || 'create_receipts_rec.g_application_rerefence'
                              || LENGTH (g_application_rerefence)
							  || 'create_receipts_rec.reason_code'
                               || LENGTH (create_receipts_rec.reason_code)
							   ||  'create_receipts_rec.reason_code'
                               || LENGTH (create_receipts_rec.reason_code)
							   ||  'create_receipts_rec.leg_reason_code'
                               || LENGTH (create_receipts_rec.leg_reason_code)) ;
      -- Start v1.11
      BEGIN
                 SELECT a.customer_trx_id
                   INTO l_customer_trx_id
                   FROM ra_customer_trx_all a,
                        ra_customer_trx_lines_all b ,
                        ra_cust_trx_line_gl_dist_all c,
						ra_batch_sources_all d --added for v1.25
                  WHERE a.trx_number           = create_receipts_rec.leg_trx_number
                    AND a.org_id               = create_receipts_rec.org_id
					--v1.25 adding below conditions to identify unique DM records only v1.25
					AND a.cust_trx_type_id 	   =  create_receipts_rec.transaction_type_id
					AND a.batch_source_id 		= d.batch_source_id
					AND a.org_id				= d.org_id
					AND d.name 				   = create_receipts_rec.batch_source_name
					--end of v1.25 
                    AND a.trx_date               = create_receipts_rec.leg_trx_date
                    AND a.customer_trx_id      = b.customer_trx_id
                    AND a.customer_trx_id      = c.customer_trx_id
                    AND b.customer_trx_line_id = c.customer_trx_line_id
                    AND gl_date                = create_receipts_rec.gl_date;

      EXCEPTION
          WHEN OTHERS THEN 
             l_customer_trx_id := null;
      END;
      
      IF create_receipts_rec.leg_trx_date > create_receipts_rec.leg_receipt_date THEN
         l_apply_date := create_receipts_rec.leg_trx_date;
      ELSE
         l_apply_date := create_receipts_rec.leg_receipt_date;
      END IF;
      
      -- END v1.11
      
            print_log_message (NVL (fnd_profile.VALUE ('AFLOG_ENABLED'), 'N'));
            ar_receipt_api_pub.APPLY
               (p_api_version                 => 1.0,
                p_init_msg_list               => fnd_api.g_true,
                p_commit                      => fnd_api.g_false,
                p_validation_level            => fnd_api.g_valid_level_full,
                p_customer_trx_id             => l_customer_trx_id,
                p_trx_number                  => create_receipts_rec.leg_trx_number,
                p_amount_applied              => g_amount_applied,
                --p_apply_date                  => create_receipts_rec.leg_receipt_date,
                p_apply_date                  => l_apply_date,  -- Added v1.11
                p_apply_gl_date               => create_receipts_rec.gl_date,
                p_installment                 => NULL,
                p_comments                    => NULL,
                p_org_id                      => create_receipts_rec.org_id,
                p_cash_receipt_id             => l_receipt_id,
                p_application_ref_type        => g_application_rerefence,
                p_application_ref_reason      => create_receipts_rec.reason_code,                
                --v1.14 changes start         
         -- v1.1 Changes start - FOT issue
--              p_customer_reference    => create_receipts_rec.leg_trx_number
               /* p_customer_reference          => SUBSTR
                                                    (create_receipts_rec.leg_comments,
                                                     1,
                                                     30
                                                    )
                                                     -- v1.1 Changes end - FOT issue
           */
                p_customer_reference          => l_customer_reference 
         ,
      
          --v1.14 changes end 
                p_customer_reason             => create_receipts_rec.leg_reason_code,
                p_attribute_rec               => p_attribute_rec,       --v1.14
                x_return_status               => l_return_status_out,
                x_msg_count                   => l_msg_count_out,
                x_msg_data                    => l_msg_data_out
               );
            print_log_message ('After Application API');
            l_error_message :=
                  l_error_message
               || ' Apply Cash API Status '
               || l_return_status_out
               || ' cr id '
               || l_receipt_id;
            print_log_message (   'ret_status Of application API:'
                               || l_return_status_out
                              );

            --Check API status is not successful
            IF l_return_status_out <> fnd_api.g_ret_sts_success
            THEN
               l_status_flag := g_error;

               IF l_msg_count_out > 1
               THEN
                  FOR i IN 1 .. l_msg_count_out
                  LOOP
                     fnd_msg_pub.get (p_msg_index          => i,
                                      p_encoded            => fnd_api.g_false,
                                      p_data               => l_msg_data_out,
                                      p_msg_index_out      => l_msg_index_out
                                     );
                     l_error_message :=
                           l_error_message
                        || ' Apply Cash Return Status '
                        || l_return_status_out
                        || ' | '
                        || l_msg_data_out
                        || REPLACE (l_msg_index_out, CHR (0), NULL);
                  END LOOP;
               ELSIF l_msg_count_out = 1
               THEN
                  l_error_message :=
                     REPLACE (   l_error_message
                              || ' Apply Cash Return Status '
                              || l_return_status_out
                              || ' | '
                              || l_msg_data_out,
                              CHR (0),
                              NULL
                             );
               END IF;

               print_log_message ('ret_status:' || l_return_status_out);
               log_errors
                  (pov_return_status            => l_log_ret_status     -- OUT
                                                                   ,
                   pov_error_msg                => l_log_err_msg        -- OUT
                                                                ,
                   piv_source_column_name       => 'Receipt Number',
                   piv_source_column_value      => create_receipts_rec.leg_receipt_number,
                   piv_error_type               => g_err_api,          -- modified for v1.12
                   piv_error_code               => 'ETN_AR_INVOICE_APPLICATION_FAILED',
                   piv_error_message            => l_error_message
                  );
            END IF;
         END IF;                                  --RECEIPT ID not null end if

         IF l_status_flag = g_error
         THEN
            BEGIN
               UPDATE xxar_openclaim_stg xos
                  SET process_flag = g_error,
                      error_type = g_err_api,   -- g_err_imp, modified for v1.12
                      run_sequence_id = g_run_seq_id,
                      cash_receipt_id = l_receipt_id,
                      last_updated_date = g_sysdate,
                      last_updated_by = g_user_id,
                      last_update_login = g_login_id,
                      request_id = g_request_id  
                WHERE xos.interface_txn_id =
                                          create_receipts_rec.interface_txn_id;

               UPDATE xxar_openclaim_stg xos
                  SET process_flag = g_error,
                      error_type = g_err_api,   -- g_err_imp, modified for v1.12
                      run_sequence_id = g_run_seq_id,
                      cash_receipt_id = l_receipt_id,
                      last_updated_date = g_sysdate,
                      last_updated_by = g_user_id,
                      last_update_login = g_login_id,
                      request_id = g_request_id  
                WHERE (xos.process_flag = g_waiting OR (xos.process_flag = g_error AND xos.error_type = g_err_api))  -- v1.12
                  AND xos.account_class = g_acct_rev
                  AND xos.transaction_type_id =
                                       create_receipts_rec.transaction_type_id
                  AND xos.leg_trx_number = create_receipts_rec.leg_trx_number
          AND xos.leg_source_system = create_receipts_rec.leg_source_system   -- Added v1.11
              AND NVL(xos.leg_customer_trx_id,999999999) = NVL(create_receipts_rec.leg_customer_trx_id, 999999999) -- Added v1.11
          AND xos.leg_operating_unit = create_receipts_rec.leg_operating_unit -- Added v1.11
                  AND xos.org_id = create_receipts_rec.org_id;
            EXCEPTION
               WHEN OTHERS
               THEN
                  print_log_message (   'Error in marking process flag to E'
                                     || SQLERRM
                                    );
            END;
         ELSE
            BEGIN
               UPDATE xxar_openclaim_stg xos
                  SET process_flag = g_complete,
                      error_type = NULL,
                      run_sequence_id = g_run_seq_id,
                      cash_receipt_id = l_receipt_id,
                      last_updated_date = g_sysdate,
                      last_updated_by = g_user_id,
                      last_update_login = g_login_id,
                      request_id = g_request_id  
                WHERE xos.interface_txn_id =
                                          create_receipts_rec.interface_txn_id;

               UPDATE xxar_openclaim_stg xos
                  SET process_flag = g_complete,
                      error_type = NULL,
                      run_sequence_id = g_run_seq_id,
                      cash_receipt_id = l_receipt_id,
                      last_updated_date = g_sysdate,
                      last_updated_by = g_user_id,
                      last_update_login = g_login_id,
                      request_id = g_request_id  
                WHERE (xos.process_flag = g_waiting OR (xos.process_flag = g_error AND xos.error_type = g_err_api))  -- v1.12
                  AND xos.account_class = g_acct_rev
                  AND xos.transaction_type_id =
                                       create_receipts_rec.transaction_type_id
                  AND xos.leg_source_system = create_receipts_rec.leg_source_system   -- Added v1.11
              AND NVL(xos.leg_customer_trx_id,999999999) = NVL(create_receipts_rec.leg_customer_trx_id,999999999)  -- Added v1.11
          AND xos.leg_operating_unit = create_receipts_rec.leg_operating_unit  -- Added v1.11   
                  AND xos.leg_trx_number = create_receipts_rec.leg_trx_number
                  AND xos.org_id = create_receipts_rec.org_id;
            EXCEPTION
               WHEN OTHERS
               THEN
                  print_log_message (   'Error in marking process flag to C'
                                     || SQLERRM
                                    );
            END;
         END IF;

         g_intf_staging_id := NULL;
      END LOOP;

      IF l_source_tab.COUNT > 0
      THEN
         xxetn_common_error_pkg.add_error
                                   (pov_return_status      => l_return_status_out
                                                                                 -- OUT
         ,
                                    pov_error_msg          => l_err_msg -- OUT
                                                                       ,
                                    pi_source_tab          => l_source_tab
                                                                          -- IN  G_SOURCE_TAB_TYPE
         ,
                                    pin_batch_id           => pin_batch_id
                                   );
         l_source_tab.DELETE;
      END IF;

      pov_errbuf := g_errbuff;
      pon_retcode := g_retcode;
      print_log_message (   ' -  PROCEDURE : CREATE_RECEIPT batch id = '
                         || pin_batch_id
                         || ' - '
                        );
      print_log_message (   'Create Receipt Program Ends at: '
                         || TO_CHAR (g_sysdate, 'DD-MON-YYYY HH24:MI:SS')
                        );
      print_log_message ('---------------------------------------------');
   EXCEPTION
      WHEN OTHERS
      THEN
         pov_errbuf :=
               'Error : Main program procedure encounter User Exception. '
            || SUBSTR (SQLERRM, 1, 150);
         pon_retcode := 2;
         print_log_message ('In create receipt when others' || SQLERRM);
   END create_receipts;

--
-- ========================
-- Procedure: validate_claim
-- =============================================================================
--   This procedure is to validate all staging records
-- =============================================================================
--  Input Parameters :
--    None
--  Output Parameters :
-- -----------------------------------------------------------------------------
--
   PROCEDURE validate_claim
   IS
      l_error_flag            VARCHAR2 (1);
      l_operating_unit        VARCHAR2 (240);
      l_org_id                NUMBER;
      l_payment_term          VARCHAR2 (30);
      l_term_id               NUMBER;
      l_trx_type              VARCHAR2 (240);
      l_trx_type_id           NUMBER;
      l_receipt_method        VARCHAR2 (30);
      l_ledger_curr           VARCHAR2 (30);
      l_method_id             NUMBER;
      l_reason_code           VARCHAR2 (240);
      l_segment1              VARCHAR2 (25);
      l_segment2              VARCHAR2 (25);
      l_segment3              VARCHAR2 (25);
      l_segment4              VARCHAR2 (25);
      l_segment5              VARCHAR2 (25);
      l_segment6              VARCHAR2 (25);
      l_segment7              VARCHAR2 (25);
      l_segment8              VARCHAR2 (25);
      l_segment9              VARCHAR2 (25);
      l_segment10             VARCHAR2 (25);
      l_cust_id               NUMBER;
      l_bill_to_id            NUMBER;
      l_ship_to_id            NUMBER;
      l_customer_type         apps.hz_cust_accounts_all.customer_type%TYPE;  --v1.8
      l_conv_type             VARCHAR2 (30);
      l_conv_rate             NUMBER;
      l_conv_date             DATE;
      l_error_cnt             NUMBER;
      l_sob_id                NUMBER;
      l_gl_error              VARCHAR2 (1);
      l_err_msg               VARCHAR2 (2000);
      l_log_ret_status        VARCHAR2 (50);
      l_log_err_msg           VARCHAR2 (2000);
      l_err_code              VARCHAR2 (40)                           := NULL;
      l_err_valid             VARCHAR2 (2000)                         := NULL;
--v1.1 Changes start
      x_out_acc_rec           xxetn_common_pkg.g_rec_type;
      x_ccid                  NUMBER;
--v1.1 Changes end
--v1.2 Changes start
      l_hdr_attribute1        VARCHAR2 (240);

      l_int_line_attr1        VARCHAR2 (240);
--v1.2 Changes end
 --CR 283417  changes starts
      l_rec_acct_segment1     VARCHAR2 (240);
      l_rec_segment1_err      VARCHAR2 (2000);
      l_rec_segment1_status   VARCHAR2 (1);
      l_batch_source          ra_interface_lines_all.batch_source_name%TYPE;
      l_hdr_attribute9         ra_interface_lines_all.header_attribute9%TYPE;
      l_hdr_attribute8        VARCHAR2 (240); --1.7.2
      l_warehouse_id          NUMBER ; --v1.9 added
      --CR 283417  changes ends
    l_in_reason_code        VARCHAR2 (240);

      --cursor is to fetch all records where process flag is 'N'
      CURSOR validate_claim_cur
      IS
         SELECT *
           FROM xxar_openclaim_stg xos
          WHERE xos.process_flag = g_new
          AND xos.batch_id = g_new_batch_id;
   BEGIN
      print_log_message (   '+   PROCEDURE : validate_claim for batch id = '
                         || g_new_batch_id
                        );
      l_error_cnt := 0;
      l_gl_error := NULL;

      FOR validate_claim_rec IN validate_claim_cur
      LOOP
         print_log_message (   'Validate Record, Record ID ='
                            || validate_claim_rec.interface_txn_id
                           );
         g_intf_staging_id := validate_claim_rec.interface_txn_id;
         l_error_flag := NULL;
         l_error_cnt := 0;
         l_operating_unit := NULL;
         l_org_id := NULL;
         l_payment_term := NULL;
         l_term_id := NULL;
         l_trx_type := NULL;
         l_trx_type_id := NULL;
         l_receipt_method := NULL;
         l_method_id := NULL;
         l_reason_code := NULL;
     l_in_reason_code := NULL;
         l_sob_id := NULL;
         l_segment1 := NULL;
         l_segment2 := NULL;
         l_segment3 := NULL;
         l_segment4 := NULL;
         l_segment5 := NULL;
         l_segment6 := NULL;
         l_segment7 := NULL;
         l_segment8 := NULL;
         l_segment9 := NULL;
         l_segment10 := NULL;
         l_cust_id := NULL;
         l_bill_to_id := NULL;
         l_ship_to_id := NULL;
     l_customer_type := NULL ; -- v1.8
         l_ledger_curr := NULL;
         l_conv_type := NULL;
         l_conv_rate := NULL;
         l_conv_date := NULL;
         l_error_cnt := 0;
         l_err_valid := NULL;
         l_batch_source := NULL;
--ver1.5 changes start
         l_int_line_attr1 := NULL;
--ver1.5 changes end
         l_warehouse_id := NULL ; --v1.9 added
         --procedure to check mandatory values are not missing
         mandatory_value_check (validate_claim_rec.leg_customer_number,
                                validate_claim_rec.leg_bill_to_address,
                                validate_claim_rec.leg_amount,
                                validate_claim_rec.leg_trx_date,
                                validate_claim_rec.leg_receipt_date,
                                validate_claim_rec.leg_term_name,
                                validate_claim_rec.leg_operating_unit,
                                validate_claim_rec.leg_currency_code,
                                validate_claim_rec.leg_reason_code,
                                validate_claim_rec.leg_receipt_method,
                                validate_claim_rec.leg_trx_number,
                                validate_claim_rec.leg_dist_segment1,
                                validate_claim_rec.leg_dist_segment2,
                                validate_claim_rec.leg_dist_segment3,
                                validate_claim_rec.leg_dist_segment4,
                                validate_claim_rec.leg_dist_segment5,
                                validate_claim_rec.leg_dist_segment6,
                                validate_claim_rec.leg_dist_segment7,
                                l_error_cnt
                               );

         IF l_error_cnt > 0
         THEN
            l_error_flag := 'Y';
         END IF;

         l_error_cnt := 0;
         --procedure to check duplicate records
         duplicate_check (validate_claim_rec.leg_cust_trx_type_name,
                          validate_claim_rec.leg_trx_number,
                          validate_claim_rec.leg_operating_unit,
                          validate_claim_rec.leg_account_class,
                          l_error_cnt
                         );

         IF l_error_cnt > 0
         THEN
            l_error_flag := 'Y';
            l_err_code := 'ETN_AR_DUPLICATE_ENTITY';
            l_err_msg := 'Error: Duplicate Record.';
            log_errors
               (pov_return_status            => l_log_ret_status        -- OUT
                                                                ,
                pov_error_msg                => l_log_err_msg           -- OUT
                                                             ,
                piv_source_column_name       =>    'Trx Type~'
                                                || 'Trx Number~'
                                                || 'Operating Unit~'
                                                || 'Account Class',
                piv_source_column_value      =>    validate_claim_rec.leg_cust_trx_type_name
                                                || '~'
                                                || validate_claim_rec.leg_trx_number
                                                || '~'
                                                || validate_claim_rec.leg_operating_unit
                                                || '~'
                                                || validate_claim_rec.leg_account_class,
                piv_error_type               => g_err_val,
                piv_error_code               => l_err_code,
                piv_error_message            => l_err_msg
               );
         END IF;

         l_error_cnt := 0;
         --procedure to check invalid records
         invalid_check (validate_claim_rec.leg_cust_trx_type_name,
                        validate_claim_rec.leg_trx_number,
                        validate_claim_rec.leg_operating_unit,
                        validate_claim_rec.leg_account_class,
                        l_error_cnt,
                        l_err_valid
                       );

         IF l_error_cnt > 0
         THEN
            l_error_flag := 'Y';
            l_err_code := 'ETN_AR_INVALID_RECORD';
            l_err_msg := l_err_valid;
            log_errors
               (pov_return_status            => l_log_ret_status        -- OUT
                                                                ,
                pov_error_msg                => l_log_err_msg           -- OUT
                                                             ,
                piv_source_column_name       =>    'Trx Type~'
                                                || 'Trx Number~'
                                                || 'Operating Unit~'
                                                || 'Account Class',
                piv_source_column_value      =>    validate_claim_rec.leg_cust_trx_type_name
                                                || '~'
                                                || validate_claim_rec.leg_trx_number
                                                || '~'
                                                || validate_claim_rec.leg_operating_unit
                                                || '~'
                                                || validate_claim_rec.leg_account_class,
                piv_error_type               => g_err_val,
                piv_error_code               => l_err_code,
                piv_error_message            => l_err_msg
               );
         END IF;

         IF validate_claim_rec.leg_term_name IS NOT NULL
         THEN
            l_error_cnt := 0;
            --Calling procedure to validate payment term
            validate_payment_term (validate_claim_rec.leg_term_name,
                                   l_payment_term,
                                   l_term_id,
                                   l_error_cnt,
                                   l_err_valid
                                  );

            IF l_error_cnt > 0
            THEN
               l_error_flag := 'Y';
               l_err_code := 'ETN_AR_INVALID_PAYMENT_TERM';
               l_err_msg := l_err_valid;
               log_errors
                  (pov_return_status            => l_log_ret_status     -- OUT
                                                                   ,
                   pov_error_msg                => l_log_err_msg        -- OUT
                                                                ,
                   piv_source_column_name       => 'Payment Term',
                   piv_source_column_value      => validate_claim_rec.leg_term_name,
                   piv_error_type               => g_err_val,
                   piv_error_code               => l_err_code,
                   piv_error_message            => l_err_msg
                  );
            END IF;
         END IF;

    /* For v1.8 moving the COA validation after customer validation and operating unit validation as
       v1.8 requires both
    */
/*
--v1.1 Changes start
         l_error_cnt := 0;
         l_err_msg := NULL;
/*
          --Calling procedure to validate COA
         validate_coa (
           validate_claim_rec.leg_dist_segment1
         , validate_claim_rec.leg_dist_segment2
         , validate_claim_rec.leg_dist_segment3
         , validate_claim_rec.leg_dist_segment4
         , validate_claim_rec.leg_dist_segment5
         , validate_claim_rec.leg_dist_segment6
         , validate_claim_rec.leg_dist_segment7
         , validate_claim_rec.leg_dist_segment8
         , validate_claim_rec.leg_dist_segment9
         , validate_claim_rec.leg_dist_segment10
         , l_segment1
         , l_segment2
         , l_segment3
         , l_segment4
         , l_segment5
         , l_segment6
         , l_segment7
         , l_segment8
         , l_segment9
         , l_segment10
         , l_error_cnt
         );

         validate_accounts (validate_claim_rec.leg_dist_segment1,
                            validate_claim_rec.leg_dist_segment2,
                            validate_claim_rec.leg_dist_segment3,
                            validate_claim_rec.leg_dist_segment4,
                            validate_claim_rec.leg_dist_segment5,
                            validate_claim_rec.leg_dist_segment6,
                            validate_claim_rec.leg_dist_segment7,
                            x_out_acc_rec,
                            x_ccid,
                            l_error_cnt,
                            l_err_msg
                           );

         IF x_ccid IS NULL
         THEN
            l_error_cnt := 1;
         ELSE
            l_segment1 := x_out_acc_rec.segment1;
            l_segment2 := x_out_acc_rec.segment2;
            l_segment3 := x_out_acc_rec.segment3;
            l_segment4 := x_out_acc_rec.segment4;
            l_segment5 := x_out_acc_rec.segment5;
            l_segment6 := x_out_acc_rec.segment6;
            l_segment7 := x_out_acc_rec.segment7;
            l_segment8 := x_out_acc_rec.segment8;
            l_segment9 := x_out_acc_rec.segment9;
            l_segment10 := x_out_acc_rec.segment10;
         END IF;

--v1.1 Changes end
         IF l_error_cnt > 0
         THEN
            l_error_flag := 'Y';
            l_err_code := 'ETN_AR_INVALID_COA';
            log_errors
               (pov_return_status            => l_log_ret_status        -- OUT
                                                                ,
                pov_error_msg                => l_log_err_msg           -- OUT
                                                             ,
                piv_source_column_name       =>    'Segment1'
                                                || '~'
                                                || 'Segment2'
                                                || '~'
                                                || 'Segment3'
                                                || '~'
                                                || 'Segment4'
                                                || '~'
                                                || 'Segment5'
                                                || '~'
                                                || 'Segment6'
                                                || '~'
                                                || 'Segment7'
                                                || '~'
                                                || 'Segment8'
                                                || '~'
                                                || 'Segment9'
                                                || '~'
                                                || 'Segment10',
                piv_source_column_value      =>    validate_claim_rec.leg_dist_segment1
                                                || '~'
                                                || validate_claim_rec.leg_dist_segment2
                                                || '~'
                                                || validate_claim_rec.leg_dist_segment3
                                                || '~'
                                                || validate_claim_rec.leg_dist_segment4
                                                || '~'
                                                || validate_claim_rec.leg_dist_segment5
                                                || '~'
                                                || validate_claim_rec.leg_dist_segment6
                                                || '~'
                                                || validate_claim_rec.leg_dist_segment7
                                                || '~'
                                                || validate_claim_rec.leg_dist_segment8
                                                || '~'
                                                || validate_claim_rec.leg_dist_segment9
                                                || '~'
                                                || validate_claim_rec.leg_dist_segment10,
                piv_error_type               => g_err_val,
                piv_error_code               => l_err_code,
                piv_error_message            => NVL (l_err_msg,
                                                     'Error: COA In Invalid'
                                                    )
               );
         END IF;

     */ -- v1.8 Commented above and moved after customer validation

         IF validate_claim_rec.leg_receipt_method IS NOT NULL
         THEN
            l_error_cnt := 0;
            --Calling procedure to validate receipt method
            validate_receipt_method (validate_claim_rec.leg_receipt_method,
                                     validate_claim_rec.leg_receipt_date,
                                     l_receipt_method,
                                     l_method_id,
                                     l_error_cnt,
                                     l_err_valid
                                    );

            IF l_error_cnt > 0
            THEN
               l_error_flag := 'Y';
               l_err_code := 'ETN_AR_INVALID_RECEIPT_METHOD';
               l_err_msg := l_err_valid;
               log_errors
                  (pov_return_status            => l_log_ret_status     -- OUT
                                                                   ,
                   pov_error_msg                => l_log_err_msg        -- OUT
                                                                ,
                   piv_source_column_name       => 'Receipt Method',
                   piv_source_column_value      => validate_claim_rec.leg_receipt_method,
                   piv_error_type               => g_err_val,
                   piv_error_code               => l_err_code,
                   piv_error_message            => l_err_msg
                  );
            END IF;
         END IF;

         IF validate_claim_rec.leg_operating_unit IS NOT NULL
         THEN
            l_error_cnt := 0;
            --CR 283417  changes starts
            l_rec_acct_segment1 := NULL;
            l_rec_segment1_err := NULL;
            l_rec_segment1_status := NULL;

            IF validate_claim_rec.leg_account_class = g_acct_rec and validate_claim_rec.leg_source_system = 'ISSC'
            THEN
               l_rec_acct_segment1 := validate_claim_rec.leg_dist_segment1;
            --pass as is
            --C9988598 : For FSC Records
            ELSIF validate_claim_rec.leg_source_system = 'NAFSC'
              THEN

              IF validate_claim_rec.leg_operating_unit IN (
                'ORGANIZATION_NAME',
                'OU ELECTRICAL CHILE',
                'OU TRUCK COMPONENTS BR',
                'OU C-H USA',
                'OU AUTOMOTIVE BR',
                'OU FLUID POWER BR',
                'OU CORPORATE USA',
                'OU ELECTRICAL BR',
                'OU SOUTH AMERICA HEADQUARTERS BR',
                'OU HYDRAULICS CHILE'
                )
                THEN
                l_rec_acct_segment1 := validate_claim_rec.leg_dist_segment1;
              ELSE
                l_rec_acct_segment1 := validate_claim_rec.interface_header_attribute1; --1.7.1
                l_rec_acct_segment1 := substr(l_rec_acct_segment1, -4);  --1.7.1
              END IF;

            ELSE
               --get REC account segment1 for REV lines for the transaction
               get_trx_rec_acct_segment1
                                 (pi_openclaim_stg_rec      => validate_claim_rec,
                                  pov_rec_segment1          => l_rec_acct_segment1,
                                  pov_error_msg             => l_rec_segment1_err,
                                  pov_status                => l_rec_segment1_status
                                 );

               IF l_rec_segment1_status <> 'S'
               THEN
                  l_error_flag := 'Y';
                  l_err_code := 'ETN_AR_INVALID_REC_SEGMENT1';
                  l_err_msg := l_rec_segment1_err;
                  log_errors
                     (pov_return_status            => l_log_ret_status  -- OUT
                                                                      ,
                      pov_error_msg                => l_log_err_msg     -- OUT
                                                                   ,
                      piv_source_column_name       =>    'Trx Type~'
                                                      || 'Trx Number~'
                                                      || 'Operating Unit~'
                                                      || 'Account Class',
                      piv_source_column_value      =>    validate_claim_rec.leg_cust_trx_type_name
                                                      || '~'
                                                      || validate_claim_rec.leg_trx_number
                                                      || '~'
                                                      || validate_claim_rec.leg_operating_unit
                                                      || '~'
                                                      || validate_claim_rec.leg_account_class,
                      piv_error_type               => g_err_val,
                      piv_error_code               => l_err_code,
                      piv_error_message            => l_err_msg
                     );

               END IF;
            END IF;

            --Calling procedure to validate operating unit
            validate_operating_unit (l_rec_acct_segment1,
                                     --validate_claim_rec.leg_dist_segment1,
                                     l_operating_unit,
                                     l_org_id,
                                     l_sob_id,
                                     l_error_cnt,
                                     l_err_valid
                                    );

            --CR 283417  changes ends
            IF l_error_cnt > 0
            THEN
               l_error_flag := 'Y';
               l_err_code := 'ETN_AR_INVALID_OPERATING_UNIT';
               l_err_msg := l_err_valid;
               log_errors
                  (pov_return_status            => l_log_ret_status     -- OUT
                                                                   ,
                   pov_error_msg                => l_log_err_msg        -- OUT
                                                                ,
                   piv_source_column_name       => 'Operating Unit',
                   piv_source_column_value      => validate_claim_rec.leg_operating_unit,
                   piv_error_type               => g_err_val,
                   piv_error_code               => l_err_code,
                   piv_error_message            => l_err_msg
                  );
            END IF;
            
         END IF;

         IF l_org_id IS NOT NULL
         THEN
            --v1.1 Changes start
                   --IF validate_claim_rec.leg_reason_code IS NOT NULL THEN
                --v1.1 Changes end
            l_error_cnt := 0;
            --Calling procedue to validate reason code
      
      --v1.14 changes start 
      
      IF validate_claim_rec.leg_source_system <> 'ISSC'
      THEN 
         l_in_reason_code := validate_claim_rec.leg_reason_code ;       
      ELSE 
         l_in_reason_code := TRIM(SUBSTR(validate_claim_rec.leg_comments,INSTR(validate_claim_rec.leg_comments,'-',1,1)+1,
                      (INSTR(validate_claim_rec.leg_comments,'-',1,2)-1 - INSTR(validate_claim_rec.leg_comments,'-',1,1))
                     )) ;  -- second string of leg_comments for ISSC       
      END IF ;      
      --v1.14 changes end 
      
           
      validate_reason_code (l_in_reason_code,
                                  l_org_id,
                                  l_reason_code,
                                  l_error_cnt,
                                  l_err_valid
                                 );

            IF l_error_cnt > 0
            THEN
               l_error_flag := 'Y';
               l_err_code := 'ETN_AR_INVALID_REASON_CODE';
               l_err_msg := l_err_valid;
               log_errors
                  (pov_return_status            => l_log_ret_status     -- OUT
                                                                   ,
                   pov_error_msg                => l_log_err_msg        -- OUT
                                                                ,
                   piv_source_column_name       => 'Reason Code',
                   piv_source_column_value      => validate_claim_rec.leg_reason_code,
                   piv_error_type               => g_err_val,
                   piv_error_code               => l_err_code,
                   piv_error_message            => l_err_msg
                  );
            END IF;

--v1.1 Changes start
            --END IF;
--v1.1 Changes end
            IF validate_claim_rec.leg_customer_number IS NOT NULL
            THEN
               l_error_cnt := 0;
               --Calling procedure to validate customer info
               validate_customer_info
                                     (validate_claim_rec.leg_customer_number,
                                      validate_claim_rec.leg_bill_to_address,
                                      validate_claim_rec.leg_ship_to_address,
                                      l_org_id,
                                      l_operating_unit, --v1.9 added
                                      validate_claim_rec.leg_operating_unit, --v1.9 added 
                                      validate_claim_rec.leg_source_system, --v1.9 added 
                                      l_cust_id,
                                      l_bill_to_id,
                                      l_ship_to_id,
                                      l_customer_type,  --v1.8 added
                                      l_error_cnt
                                     );

               IF l_error_cnt > 0
               THEN
                  l_error_flag := 'Y';
                  l_err_code := 'ETN_AR_INVALID_CUSTOMER_INFO';
                  l_err_msg := 'Error: Customer Details are not Valid.';
                  log_errors
                     (pov_return_status            => l_log_ret_status  -- OUT
                                                                      ,
                      pov_error_msg                => l_log_err_msg     -- OUT
                                                                   ,
                      piv_source_column_name       =>    'leg_customer_number'
                                                      || '~'
                                                      || 'leg_bill_to_address'
                                                      || '~'
                                                      || 'leg_ship_to_address',
                      piv_source_column_value      =>    validate_claim_rec.leg_customer_number
                                                      || '~'
                                                      || validate_claim_rec.leg_bill_to_address
                                                      || '~'
                                                      || validate_claim_rec.leg_ship_to_address,
                      piv_error_type               => g_err_val,
                      piv_error_code               => l_err_code,
                      piv_error_message            => l_err_msg
                     );
               END IF;
            END IF;

      -- v1.9 changes - Deriving warehouse id based on the R12 Operating Unit --
      -- This is required only for the Brazil transactions -- 
      
      IF l_operating_unit = 'OU ETN LTDA 0185 BRL' 
      THEN
         
         BEGIN
           SELECT organization_id
         INTO   l_warehouse_id
         FROM  apps.hr_organization_units
         WHERE name = 'IO VG VALINHOS BR DEFAULT' ;
         
         EXCEPTION
         WHEN OTHERS THEN

                  l_error_flag := 'Y';
                  l_err_code := 'ETN_AR_INVALID_WAREHOUSE_INFO';
                  l_err_msg := 'Error: Warehouse ID cannot be derived.';
                  log_errors
                     (pov_return_status            => l_log_ret_status  -- OUT
                                                                      ,
                      pov_error_msg                => l_log_err_msg     -- OUT
                                                                   ,
                      piv_source_column_name       =>    'operating_unit'
                                                      || '~'
                                                      || 'warehouse_name'
                                                      ,
                      piv_source_column_value      =>    'OU ETN LTDA 0185 BRL'
                                                      || '~'
                                                      || 'IO SAHQ BR DEFAULT' ,
                      piv_error_type               => g_err_val,
                      piv_error_code               => l_err_code,
                      piv_error_message            => l_err_msg
                     );         
         
         END ; 
      ELSIF l_operating_unit = 'OU ETN POWER SOL LTDA 4470 BRL'    
      THEN 
               
         BEGIN
           SELECT organization_id
         INTO   l_warehouse_id
         FROM  apps.hr_organization_units
         WHERE name = 'IO EPS SAO PAULO BR DEFAULT' ;
         
         EXCEPTION
         WHEN OTHERS THEN

                  l_error_flag := 'Y';
                  l_err_code := 'ETN_AR_INVALID_WAREHOUSE_INFO';
                  l_err_msg := 'Error: Warehouse ID cannot be derived.';
                  log_errors
                     (pov_return_status            => l_log_ret_status  -- OUT
                                                                      ,
                      pov_error_msg                => l_log_err_msg     -- OUT
                                                                   ,
                      piv_source_column_name       =>    'operating_unit'
                                                      || '~'
                                                      || 'warehouse_name'
                                                      ,
                      piv_source_column_value      =>    'OU ETN POWER SOL LTDA 4470 BRL'
                                                      || '~'
                                                      || 'IO EPS SAO PAULO BR DEFAULT' ,
                      piv_error_type               => g_err_val,
                      piv_error_code               => l_err_code,
                      piv_error_message            => l_err_msg
                     );         
         
         END ; 
      ELSE 
        l_warehouse_id := NULL ; 
      END IF ; 
                  
      -- v1.9 changes end --
      
        /* v1.8 moved the COA validation here below */

      --v1.1 Changes start
         l_error_cnt := 0;
         l_err_msg := NULL;
         /*
          --Calling procedure to validate COA
         validate_coa (
           validate_claim_rec.leg_dist_segment1
         , validate_claim_rec.leg_dist_segment2
         , validate_claim_rec.leg_dist_segment3
         , validate_claim_rec.leg_dist_segment4
         , validate_claim_rec.leg_dist_segment5
         , validate_claim_rec.leg_dist_segment6
         , validate_claim_rec.leg_dist_segment7
         , validate_claim_rec.leg_dist_segment8
         , validate_claim_rec.leg_dist_segment9
         , validate_claim_rec.leg_dist_segment10
         , l_segment1
         , l_segment2
         , l_segment3
         , l_segment4
         , l_segment5
         , l_segment6
         , l_segment7
         , l_segment8
         , l_segment9
         , l_segment10
         , l_error_cnt
         );
*/
         validate_accounts (validate_claim_rec.leg_dist_segment1,
                            validate_claim_rec.leg_dist_segment2,
                            validate_claim_rec.leg_dist_segment3,
                            validate_claim_rec.leg_dist_segment4,
                            validate_claim_rec.leg_dist_segment5,
                            validate_claim_rec.leg_dist_segment6,
                            validate_claim_rec.leg_dist_segment7,
                            validate_claim_rec.leg_operating_unit, --v1.8 added
                            validate_claim_rec.interface_header_attribute1, --v1.8 added
                            validate_claim_rec.leg_account_class, --v1.8 added
                            l_customer_type, --v1.8 added
              l_cust_id, --v1.16
                            x_out_acc_rec,
                            x_ccid,
                            l_error_cnt,
                            l_err_msg
                           );

         IF x_ccid IS NULL
         THEN
            l_error_cnt := 1;
         ELSE
            l_segment1 := x_out_acc_rec.segment1;
            l_segment2 := x_out_acc_rec.segment2;
            l_segment3 := x_out_acc_rec.segment3;
            l_segment4 := x_out_acc_rec.segment4;
            l_segment5 := x_out_acc_rec.segment5;
            l_segment6 := x_out_acc_rec.segment6;
            l_segment7 := x_out_acc_rec.segment7;
            l_segment8 := x_out_acc_rec.segment8;
            l_segment9 := x_out_acc_rec.segment9;
            l_segment10 := x_out_acc_rec.segment10;
         END IF;

--v1.1 Changes end
         IF l_error_cnt > 0
         THEN
            l_error_flag := 'Y';
            l_err_code := 'ETN_AR_INVALID_COA';
            log_errors
               (pov_return_status            => l_log_ret_status        -- OUT
                                                                ,
                pov_error_msg                => l_log_err_msg           -- OUT
                                                             ,
                piv_source_column_name       =>    'Segment1'
                                                || '~'
                                                || 'Segment2'
                                                || '~'
                                                || 'Segment3'
                                                || '~'
                                                || 'Segment4'
                                                || '~'
                                                || 'Segment5'
                                                || '~'
                                                || 'Segment6'
                                                || '~'
                                                || 'Segment7'
                                                || '~'
                                                || 'Segment8'
                                                || '~'
                                                || 'Segment9'
                                                || '~'
                                                || 'Segment10',
                piv_source_column_value      =>    validate_claim_rec.leg_dist_segment1
                                                || '~'
                                                || validate_claim_rec.leg_dist_segment2
                                                || '~'
                                                || validate_claim_rec.leg_dist_segment3
                                                || '~'
                                                || validate_claim_rec.leg_dist_segment4
                                                || '~'
                                                || validate_claim_rec.leg_dist_segment5
                                                || '~'
                                                || validate_claim_rec.leg_dist_segment6
                                                || '~'
                                                || validate_claim_rec.leg_dist_segment7
                                                || '~'
                                                || validate_claim_rec.leg_dist_segment8
                                                || '~'
                                                || validate_claim_rec.leg_dist_segment9
                                                || '~'
                                                || validate_claim_rec.leg_dist_segment10,
                piv_error_type               => g_err_val,
                piv_error_code               => l_err_code,
                piv_error_message            => NVL (l_err_msg,
                                                     'Error: COA In Invalid'
                                                    )
               );
         END IF;


            l_error_cnt := 0;
            --Calling procedure to validate gl period
            validate_gl_period (l_sob_id, l_error_cnt);

            IF l_error_cnt > 0
            THEN
               l_error_flag := 'Y';
               print_log_message ('GL Period is close. Input = ' || g_gl_date);
               l_err_code := 'ETN_AR_INVALID_GL_PERIOD';
               l_err_msg := 'Error: GL Period is not open.';
               log_errors (pov_return_status            => l_log_ret_status
                                                                           -- OUT
               ,
                           pov_error_msg                => l_log_err_msg
                                                                        -- OUT
               ,
                           piv_source_column_name       => 'GL Period',
                           piv_source_column_value      => g_gl_date,
                           piv_error_type               => g_err_val,
                           piv_error_code               => l_err_code,
                           piv_error_message            => l_err_msg
                          );
            END IF;

            l_error_cnt := 0;
            --Calling procedure to validate ar term
            validate_ar_period (l_sob_id, l_error_cnt);

            IF l_error_cnt > 0
            THEN
               l_error_flag := 'Y';
               print_log_message ('AR Period is close. Input = ' || g_gl_date);
               l_err_code := 'ETN_AR_INVALID_AR_PERIOD';
               l_err_msg := 'Error: AR Period is not open.';
               log_errors (pov_return_status            => l_log_ret_status
                                                                           -- OUT
               ,
                           pov_error_msg                => l_log_err_msg
                                                                        -- OUT
               ,
                           piv_source_column_name       => 'AR Period',
                           piv_source_column_value      => g_gl_date,
                           piv_error_type               => g_err_val,
                           piv_error_code               => l_err_code,
                           piv_error_message            => l_err_msg
                          );
            END IF;

            l_error_cnt := 0;
            --Calling procedure to validate transaction type
            validate_transaction_type
                                   (validate_claim_rec.leg_cust_trx_type_name,
                                    l_org_id,
                                    l_trx_type,
                                    l_trx_type_id,
                                    l_error_cnt,
                                    l_err_valid
                                   );

            IF l_error_cnt > 0
            THEN
               l_error_flag := 'Y';
               l_err_code := 'ETN_AR_INVALID_TRANSACTION_TYPE';
               l_err_msg := l_err_valid;
               log_errors
                  (pov_return_status            => l_log_ret_status     -- OUT
                                                                   ,
                   pov_error_msg                => l_log_err_msg        -- OUT
                                                                ,
                   piv_source_column_name       => 'Transaction Type',
                   piv_source_column_value      => validate_claim_rec.leg_cust_trx_type_name,
                   piv_error_type               => g_err_val,
                   piv_error_code               => l_err_code,
                   piv_error_message            => l_err_msg
                  );
            END IF;

--283417 Changes Starts
            l_rec_segment1_status := NULL;                   --reset variables
            l_rec_segment1_err := NULL;
            l_hdr_attribute9 := NULL;
            --Call proc to get batch source as per new logic
            get_batch_source (piv_eaton_site        => l_rec_acct_segment1,
                              pov_batch_source      => l_batch_source,
                              pov_site_credit_off   => l_hdr_attribute9,
                              pov_status            => l_rec_segment1_status,
                              pov_err_message       => l_rec_segment1_err
                             );

            IF l_rec_segment1_status <> 'S'
            THEN
               l_error_flag := 'Y';
               l_err_code := 'ETN_AR_BATCH_SOURCE_NOT_FOUND';
               l_err_msg := l_rec_segment1_err;
               log_errors
                  (pov_return_status            => l_log_ret_status     -- OUT
                                                                   ,
                   pov_error_msg                => l_log_err_msg        -- OUT
                                                                ,
                   piv_source_column_name       =>    'Trx Type~'
                                                   || 'Trx Number~'
                                                   || 'Operating Unit~'
                                                   || 'Account Class',
                   piv_source_column_value      =>    validate_claim_rec.leg_cust_trx_type_name
                                                   || '~'
                                                   || validate_claim_rec.leg_trx_number
                                                   || '~'
                                                   || validate_claim_rec.leg_operating_unit
                                                   || '~'
                                                   || validate_claim_rec.leg_account_class,
                   piv_error_type               => g_err_val,
                   piv_error_code               => l_err_code,
                   piv_error_message            => l_err_msg
                  );

            END IF;

--283417 Changes ends
            l_error_cnt := 0;
            --Calling procedure to validate batch source
            --validate_batch_source (l_org_id, l_error_cnt);--283417 changes
            validate_batch_source (pin_org_id            => l_org_id,
                                   piv_batch_source      => l_batch_source,
                                   pon_error_cnt         => l_error_cnt
                                  );                          --283417 changes

            IF l_error_cnt > 0
            THEN
               l_error_flag := 'Y';
               l_err_code := 'ETN_AR_INVALID_BATCH_SOURCE';
               l_err_msg := 'Error: Batch Source is not Valid.';
               log_errors (pov_return_status            => l_log_ret_status,
                           -- OUT
                           pov_error_msg                => l_log_err_msg,
                           -- OUT
                           piv_source_column_name       => 'Batch Source',
                           piv_source_column_value      => l_batch_source,
                           piv_error_type               => g_err_val,
                           piv_error_code               => l_err_code,
                           piv_error_message            => l_err_msg
                          );
            END IF;

            l_error_cnt := 0;
            --Calling procedure to get ledger currency
            get_ledger_currency (l_sob_id, l_ledger_curr, l_error_cnt);

            IF l_error_cnt > 0
            THEN
               l_error_flag := 'Y';
               l_err_code := 'ETN_AR_INVALID_LEDGER_CURRENCY';
               l_err_msg := 'Error: Currency set up at ledger is not active.';
               log_errors (pov_return_status            => l_log_ret_status,
                           -- OUT
                           pov_error_msg                => l_log_err_msg,
                           -- OUT
                           piv_source_column_name       => 'Ledger Currency',
                           piv_source_column_value      => NULL,
                           piv_error_type               => g_err_val,
                           piv_error_code               => l_err_code,
                           piv_error_message            => l_err_msg
                          );
            END IF;
         END IF;

         IF validate_claim_rec.leg_currency_code IS NOT NULL
         THEN
            l_error_cnt := 0;
            --Calling procedure to validate currency code
            validate_currency_code (validate_claim_rec.leg_currency_code,
                                    l_error_cnt
                                   );

            IF l_error_cnt > 0
            THEN
               l_error_flag := 'Y';
               l_err_code := 'ETN_AR_INVALID_CURRENCY_CODE';
               l_err_msg := 'Error: Currency Code is not Valid.';
               log_errors
                  (pov_return_status            => l_log_ret_status     -- OUT
                                                                   ,
                   pov_error_msg                => l_log_err_msg        -- OUT
                                                                ,
                   piv_source_column_name       => 'Currency Code',
                   piv_source_column_value      => validate_claim_rec.leg_currency_code,
                   piv_error_type               => g_err_val,
                   piv_error_code               => l_err_code,
                   piv_error_message            => l_err_msg
                  );
            END IF;

            IF l_ledger_curr IS NOT NULL
            THEN
               IF l_ledger_curr = validate_claim_rec.leg_currency_code
               THEN
                  l_conv_type := NULL;                              --'User';
                  l_conv_rate := NULL;                                   --1;
                  l_conv_date := NULL;
               ELSE
                  IF (   validate_claim_rec.leg_conversion_type IS NULL
                      OR validate_claim_rec.leg_conversion_date IS NULL
                      OR validate_claim_rec.leg_conversion_rate IS NULL
                     )
                  THEN
                     l_error_flag := 'Y';
                     print_log_message
                                    (' Conversion Type, Date or Rate IS NULL');
                     l_err_code := 'ETN_AR_INVALID_CONVERSION_RATE';
                     l_err_msg :=
                              'Error: Conversion Type, Date or Rate IS NULL.';
                     log_errors
                        (pov_return_status            => l_log_ret_status
                                                                         -- OUT
                     ,
                         pov_error_msg                => l_log_err_msg  -- OUT
                                                                      ,
                         piv_source_column_name       =>    'Conversion Type~'
                                                         || 'Conversion Date~'
                                                         || 'Conversion Rate',
                         piv_source_column_value      =>    validate_claim_rec.leg_conversion_type
                                                         || '~'
                                                         || validate_claim_rec.leg_conversion_date
                                                         || '~'
                                                         || validate_claim_rec.leg_conversion_rate,
                         piv_error_type               => g_err_val,
                         piv_error_code               => l_err_code,
                         piv_error_message            => l_err_msg
                        );
                  ELSE
                     l_conv_type := 'User';
                     l_conv_rate := validate_claim_rec.leg_conversion_rate;
                     l_conv_date := validate_claim_rec.leg_conversion_date;
                  END IF;
               END IF;
            END IF;                                   --ledger currency end if
         END IF;                                    --leg currency code end if

         IF g_gl_date < validate_claim_rec.leg_receipt_date
         THEN
            l_error_flag := 'Y';
            print_log_message
                            ('Maturity Date cannot be less than receipt date');
            l_err_code := 'ETN_AR_INVALID_MATURITY_DATE';
            l_err_msg :=
                     'Error: Maturity Date cannot be less than receipt date.';
            log_errors (pov_return_status            => l_log_ret_status,
                        -- OUT
                        pov_error_msg                => l_log_err_msg,   --OUT
                        piv_source_column_name       => 'Maturity Date',
                        piv_source_column_value      => g_gl_date,
                        piv_error_type               => g_err_val,
                        piv_error_code               => l_err_code,
                        piv_error_message            => l_err_msg
                       );
         END IF;

         IF validate_claim_rec.leg_trx_date <
                                           validate_claim_rec.leg_receipt_date
         THEN
            l_error_flag := 'Y';
            print_log_message
                         ('Transaction Date cannot be less than receipt date');
            l_err_code := 'ETN_AR_INVALID_TRX_DATE';
            l_err_msg :=
                  'Error: Transaction Date cannot be less than receipt date.';
            log_errors
                 (pov_return_status            => l_log_ret_status,      --OUT
                  pov_error_msg                => l_log_err_msg         -- OUT
                                                               ,
                  piv_source_column_name       => 'Transaction Date',
                  piv_source_column_value      => validate_claim_rec.leg_trx_date,
                  piv_error_type               => g_err_val,
                  piv_error_code               => l_err_code,
                  piv_error_message            => l_err_msg
                 );
         END IF;

         IF validate_claim_rec.leg_amount <= 0
         THEN
            l_error_flag := 'Y';
            print_log_message ('Claims Amount is less than or equal to zero');
            l_err_code := 'ETN_AR_INVALID_AMOUNT';
            l_err_msg :=
                        'Error: Claims Amount is less than or equal to zero.';
            log_errors
                   (pov_return_status            => l_log_ret_status    -- OUT
                                                                    ,
                    pov_error_msg                => l_log_err_msg       -- OUT
                                                                 ,
                    piv_source_column_name       => 'Amount',
                    piv_source_column_value      => validate_claim_rec.leg_amount,
                    piv_error_type               => g_err_val,
                    piv_error_code               => l_err_code,
                    piv_error_message            => l_err_msg
                   );
         END IF;

--Ver 1.2 Changes start
--CR 283417  changes starts
--AmitR commented for new logic on header_attribute1;;
       /*  IF NVL (validate_claim_rec.leg_header_attribute3, 'XX') IN
                                                                   ('A', 'D')
         THEN
            l_hdr_attribute1 := validate_claim_rec.leg_header_attribute3;
         ELSIF    (    NVL (validate_claim_rec.leg_header_attribute4, 'XX') =
                                                                           'F'
                   AND validate_claim_rec.leg_source_system = 'NAFSC'
                  )
               OR (    NVL (validate_claim_rec.leg_header_attribute4, 'XX') =
                                                                           'Y'
                   AND validate_claim_rec.leg_source_system = 'ISSC'
                  )
         THEN
            l_hdr_attribute1 := 'S';
         ELSE
            l_hdr_attribute1 := NULL;
         END IF;*/
         l_hdr_attribute1 := NULL;                                     --reset

         IF validate_claim_rec.leg_source_system = 'ISSC'
         THEN
            IF validate_claim_rec.leg_header_attribute3 IN ('A', 'D','S') -- 1.7.2
            THEN                                  --Approved/Disapproved Flag
               l_hdr_attribute1 := validate_claim_rec.leg_header_attribute3;
            ELSE
               /* -- 1.7.2
         IF validate_claim_rec.leg_header_attribute4 = 'Y'
               THEN                   --if not in A,D then use Factoring Flag
                  l_hdr_attribute1 := 'S';
               ELSE
         -- 1.7.2
         */
             l_hdr_attribute1 := NULL;
       l_hdr_attribute8 := NULL; --1.7.2
            -- END IF;  -- 1.7.2
            END IF;
         ELSIF validate_claim_rec.leg_source_system = 'NAFSC'
         THEN
            IF validate_claim_rec.leg_header_attribute3 IN ('A', 'D')
            THEN                                  --Approved/Disapproved Flag
               l_hdr_attribute1 := validate_claim_rec.leg_header_attribute3;
            ELSE
               IF validate_claim_rec.leg_header_attribute4 = 'F'
               THEN                   --if not in A,D then use Factoring Flag
                  l_hdr_attribute1 := 'S';
               ELSE
                  l_hdr_attribute1 := 'I';
               END IF;
            END IF;
         ELSE
            NULL;                 --do nothing if source is not in ISSC,NAFSC
         END IF;

         --CR 283417  changes ends
         IF validate_claim_rec.leg_header_attribute_category =
                                                    'PLANT SHIPMENTS (EUROPE)'
         THEN
            l_int_line_attr1 := validate_claim_rec.leg_header_attribute1;
      l_hdr_attribute8 := validate_claim_rec.header_attribute15; --v1.7.2

         ELSIF validate_claim_rec.leg_header_attribute_category IN
                                                             (328697, 328696) --v1.7.2
         THEN
            l_int_line_attr1 := validate_claim_rec.leg_header_attribute2;
     END IF;

         IF l_int_line_attr1 IS NULL
         THEN
            l_int_line_attr1 := validate_claim_rec.leg_trx_number;
         END IF;

--Ver 1.2 Changes end
         UPDATE xxar_openclaim_stg xos
            SET gl_date = g_gl_date,
                receipt_method = l_receipt_method,
                receipt_method_id = l_method_id,
                batch_source_name = l_batch_source,            --283417 change
                term_name = l_payment_term,
                term_id = l_term_id,
                reason_code = l_reason_code,
                transaction_type_id = l_trx_type_id,
                conversion_type = l_conv_type,
                conversion_rate = l_conv_rate,
                conversion_date = l_conv_date,
                org_id = l_org_id,
                dist_segment1 = l_segment1,
                dist_segment2 = l_segment2,
                dist_segment3 = l_segment3,
                dist_segment4 = l_segment4,
                dist_segment5 = l_segment5,
                dist_segment6 = l_segment6,
                dist_segment7 = l_segment7,
                dist_segment8 = l_segment8,
                dist_segment9 = l_segment9,
                dist_segment10 = l_segment10,
                system_sold_customer_id = l_cust_id,
--v1.1  Changes on 22-May-2014 start
                system_ship_customer_id =
                                  DECODE (l_ship_to_id,
                                          NULL, NULL,
                                          l_cust_id
                                         ),
--v1.1  Changes on 22-May-2014 end
                system_bill_address_id = l_bill_to_id,
                system_ship_address_id = l_ship_to_id,
                amount = leg_amount,
                account_class = validate_claim_rec.leg_account_class,
                -- v1.1 Changes start - FOT issue
                comments = leg_comments,
                -- v1.1 Changes end - FOT issue
                process_flag = DECODE (l_error_flag, 'Y', g_error, g_valid),
                ERROR_TYPE = DECODE (l_error_flag, 'Y', g_err_val, NULL),
                last_updated_date = g_sysdate,
                last_updated_by = g_user_id,
                last_update_login = g_login_id,
--Ver 1.2 Changes start
                interface_line_attribute1 = l_int_line_attr1,
                header_attribute1 = l_hdr_attribute1,

--Ver 1.2 Changes end
                header_attribute9 = l_hdr_attribute9, --283417 change
                header_attribute8 = l_hdr_attribute8,
        warehouse_id = l_warehouse_id ,--v1.9 changes
    request_id = g_request_id
         WHERE  xos.interface_txn_id = validate_claim_rec.interface_txn_id;

         g_intf_staging_id := NULL;
      END LOOP;

      COMMIT;
      bulk_errors;
      print_stat (g_new_batch_id);
      print_log_message (   '-   PROCEDURE : validate_receipt for batch id = '
                         || g_new_batch_id
                         || ' - '
                        );
   EXCEPTION
      WHEN OTHERS
      THEN
         g_retcode := 2;
         g_errbuff := 'Failed during Validate Claim';
         print_log_message ('In Validate claim when others' || SQLERRM);
   END validate_claim;

--
-- ========================
-- Procedure: update_duedate
-- =============================================================================
--   This procedure to update duedate on payment schedules
-- =============================================================================
--  Input Parameters :
--  pin_batch_id     : Batch Id
--  Output Parameters :
--  pov_errbuf       : Standard output parameter for concurrent program
--  pon_retcode      : Standard output parameter for concurrent program
-- -----------------------------------------------------------------------------
--
   PROCEDURE update_duedate (
             pov_errbuf   OUT NOCOPY VARCHAR2,
             pon_retcode  OUT NOCOPY NUMBER,
             pin_batch_id IN         NUMBER               -- NULL / <BATCH_ID>
   )
   IS
   
    -- Below cursor added for v1.20
   CURSOR due_date_cur
   IS
     SELECT xos.leg_due_date, apsa.payment_schedule_id
       FROM xxar_openclaim_stg xos,
            ar_payment_schedules_all apsa,
            ra_customer_trx_all rcta
      WHERE apsa.customer_trx_id = rcta.customer_trx_id
        AND rcta.trx_number = xos.leg_trx_number
        AND rcta.trx_date = xos.leg_trx_date
        AND rcta.org_id = xos.org_id
        AND rcta.cust_trx_type_id = xos.transaction_type_id
        AND xos.process_flag = g_complete
        AND xos.account_class = g_acct_rec
        AND xos.batch_id = pin_batch_id
		AND xos.leg_due_date <> apsa.due_date;
		
      l_err_msg   VARCHAR2 (2000);
	  l_count     NUMBER:=0;
   BEGIN
      xxetn_debug_pkg.initialize_debug
                             (pov_err_msg           => l_err_msg,
                              piv_program_name      => 'ETN_AR_CLAIM_DUEDATE_CONV'
                             );
      print_log_message (   '+   PROCEDURE : Update Due Date Program = +'
                         || pin_batch_id
                        );
      print_log_message (   'Update Due Date Starts at: '
                         || TO_CHAR (g_sysdate, 'DD-MON-YYYY HH24:MI:SS')
                        );
      
	  -- Below UPDATE statment commented for v1.20
	  /*
      --Hard update on payment schedule due date with the one received in staging table
      UPDATE ar_payment_schedules_all apsa
         SET due_date =
                (SELECT xos.leg_due_date
                   FROM xxar_openclaim_stg xos,
                        ar_payment_schedules_all apsa1,
                        ra_customer_trx_all rcta
                  WHERE apsa1.payment_schedule_id = apsa.payment_schedule_id
                    AND apsa1.customer_trx_id = rcta.customer_trx_id
                    AND rcta.trx_number = xos.leg_trx_number
          AND rcta.trx_date = xos.leg_trx_date
                    AND rcta.org_id = xos.org_id
                    AND rcta.cust_trx_type_id = xos.transaction_type_id
                    AND xos.process_flag = g_complete
                    AND xos.account_class = g_acct_rec
                    AND xos.batch_id = pin_batch_id
                    AND apsa1.due_date =
                           (SELECT MAX (apsa4.due_date)
                              FROM ar_payment_schedules_all apsa4
                             WHERE apsa4.customer_trx_id =
                                                         apsa1.customer_trx_id)),
             last_update_date = g_sysdate,
             last_updated_by = g_user_id,
             last_update_login = g_login_id,
       request_id = g_request_id
       WHERE apsa.payment_schedule_id IN (
                SELECT apsa2.payment_schedule_id
                  FROM xxar_openclaim_stg xos,
                       ar_payment_schedules_all apsa2,
                       ra_customer_trx_all rcta
                 WHERE apsa2.payment_schedule_id = apsa.payment_schedule_id
                   AND apsa2.customer_trx_id = rcta.customer_trx_id
                   AND rcta.trx_number = xos.leg_trx_number
           AND rcta.trx_date = xos.leg_trx_date
                   AND rcta.org_id = xos.org_id
                   AND rcta.cust_trx_type_id = xos.transaction_type_id
                   AND xos.process_flag = g_complete
                   AND xos.account_class = g_acct_rec
                   AND xos.batch_id = pin_batch_id
                   AND apsa2.due_date =
                          (SELECT MAX (apsa3.due_date)
                             FROM ar_payment_schedules_all apsa3
                            WHERE apsa3.customer_trx_id =
                                                         apsa2.customer_trx_id)); */
	  -- Changes start for v1.20
	  FOR due_date_rec IN due_date_cur
	  LOOP
	      UPDATE ar_payment_schedules_all
             SET due_date = due_date_rec.leg_due_date
                ,last_update_date  = g_sysdate
                ,last_updated_by   = g_user_id
                ,last_update_login = g_login_id
           WHERE payment_schedule_id = due_date_rec.payment_schedule_id;
		   
           l_count := l_count +1;		   
     END LOOP;
	 -- Changes end for v1.20

      --print_log_message ('+ Updated Due Date on Recs = ' || SQL%ROWCOUNT);
	  print_log_message ('+ Updated Due Date on Recs = ' || l_count);
      pov_errbuf := g_errbuff;
      pon_retcode := g_retcode;
      print_log_message (   '-   PROCEDURE : Update Due Date Program = -'
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
         print_log_message ('In Due Date update when others' || SQLERRM);
   END;

--
-- ========================
-- Procedure: create_claim
-- =============================================================================
--   This procedure is for inserting validating records into ra_interface_line_all
-- =============================================================================
--  Input Parameters :
--    None
--  Output Parameters :
-- -----------------------------------------------------------------------------
--
   PROCEDURE create_claim
   IS
      l_int_line_id      NUMBER;
      l_log_ret_status   VARCHAR2 (50);
      l_log_err_msg      VARCHAR2 (2000);
      l_error_ind        NUMBER;
      --Custom Exception
      l_process_excep    EXCEPTION;

      --Cursor for selecting all validated records
      CURSOR create_claim_cur
      IS
         SELECT   *
             FROM xxar_openclaim_stg xos
            WHERE xos.process_flag = g_valid AND xos.batch_id = g_new_batch_id
         ORDER BY leg_source_system,  -- Added v1.11
              leg_customer_trx_id,  -- Added v1.11
          leg_operating_unit, -- Added v1.11
          xos.leg_trx_number,
                  xos.account_class;
   BEGIN
      print_log_message (   '   PROCEDURE : create_claim batch id = '
                         || g_new_batch_id
                        );
      l_int_line_id := NULL;
      l_log_ret_status := NULL;
      l_log_err_msg := NULL;
      l_error_ind := 0;
      g_run_seq_id := xxetn_run_sequences_s.NEXTVAL;
      xxetn_common_error_pkg.g_run_seq_id := g_run_seq_id;
    FND_FILE.PUT_line(fnd_file.log, 'in create_claim');
      FOR create_claim_rec IN create_claim_cur
      LOOP
        FND_FILE.PUT_line(fnd_file.log, 'inside loop');
         BEGIN
            IF create_claim_rec.account_class = g_acct_rec
            THEN
                FND_FILE.PUT_line(fnd_file.log, 'int txn id=> ' || create_claim_rec.interface_txn_id);
               SAVEPOINT data_insert;
               l_error_ind := 0;
               print_log_message (   'In REC IF. Interface TXN ID '
                                  || create_claim_rec.interface_txn_id
                                 );
               l_int_line_id := xxar_intattribute_s.NEXTVAL;

               --inserting data in auto invoice interface tables
               INSERT INTO ra_interface_lines_all
                           (batch_source_name,
                            trx_number,
                            org_id,
                            interface_line_context,
                            interface_line_attribute1,
--Ver 1.2 changes start
/*               , interface_line_attribute2
                           , interface_line_attribute3
                           , interface_line_attribute4
                           , interface_line_attribute5
                           , interface_line_attribute6
                           , interface_line_attribute7
                           , interface_line_attribute8
                           , interface_line_attribute9
                           , interface_line_attribute10
                           , interface_line_attribute11
                           , interface_line_attribute12
                           , interface_line_attribute13
                           , interface_line_attribute14
  */
  --Ver 1.2 changes end
                            interface_line_attribute15,
                            term_id,
                            orig_system_bill_customer_id,
                            orig_system_bill_address_id,
                            orig_system_bill_contact_id,
                            orig_system_ship_customer_id,
                            orig_system_ship_address_id,
                            orig_system_ship_contact_id,
                            orig_system_sold_customer_id,
                            line_type,
                            line_number,
                            description,
                            quantity,
                            currency_code,
                            conversion_type,
                            conversion_date,
                            conversion_rate,
                            purchase_order,
                            sales_order,
                            amount,
                            cust_trx_type_id,
                            trx_date,
                            gl_date
                                   -- v1.1 Changes start - FOT issue
               ,
                            comments
                                    -- v1.1 Changes end - FOT issue
               ,
                            header_attribute_category,
                            header_attribute1,
                            header_attribute9, --283417 
              header_attribute8, --1.7.2
  --Ver 1.2 changes start
/*
               , header_attribute2
                           , header_attribute3
                           , header_attribute4
*/
  --Ver 1.2 changes end

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
                            reason_code,
                            created_by,
                            creation_date,
                            last_updated_by,
                            last_update_date,
                            last_update_login,
                            taxable_flag, --V1.6 change
              warehouse_id --v1.9 added
                           )
                    VALUES (create_claim_rec.batch_source_name,
                            create_claim_rec.leg_trx_number,
                            create_claim_rec.org_id,
                            g_interface_line_context,
--Ver 1.2 changes start
/*
                           , l_int_line_id
                           , create_claim_rec.interface_line_attribute2
                           , create_claim_rec.interface_line_attribute3
                           , create_claim_rec.interface_line_attribute4
                           , create_claim_rec.interface_line_attribute5
                           , create_claim_rec.interface_line_attribute6
                           , create_claim_rec.interface_line_attribute7
                           , create_claim_rec.interface_line_attribute8
                           , create_claim_rec.interface_line_attribute9
                           , create_claim_rec.interface_line_attribute10
                           , create_claim_rec.interface_line_attribute11
                           , create_claim_rec.interface_line_attribute12
                           , create_claim_rec.interface_line_attribute13
                           , create_claim_rec.interface_line_attribute14
                           , create_claim_rec.interface_line_attribute15
  */
  --Ver 1.2 changes end
                            create_claim_rec.interface_line_attribute1,
                            l_int_line_id,
                            create_claim_rec.term_id,
                            create_claim_rec.system_sold_customer_id,
                            create_claim_rec.system_bill_address_id,
                            NULL,
                            create_claim_rec.system_ship_customer_id,
                            create_claim_rec.system_ship_address_id,
                            NULL,
                            create_claim_rec.system_sold_customer_id,
                            g_line_type,
                            '1',
                            g_description,
                            g_quantity,
                            create_claim_rec.leg_currency_code,
                            NVL (create_claim_rec.conversion_type, 'User'),
                            create_claim_rec.conversion_date,
                            NVL (create_claim_rec.conversion_rate, 1),
                            create_claim_rec.leg_purchase_order,
                            create_claim_rec.leg_sales_order,
                            create_claim_rec.amount,
                            create_claim_rec.transaction_type_id,
                            create_claim_rec.leg_trx_date,
                            create_claim_rec.gl_date,
                            -- v1.1 Changes start - FOT issue
                            create_claim_rec.comments,
             -- v1.1 Changes end - FOT issue
--Ver 1.2 changes start
--                           , create_claim_rec.header_attribute_category
                            g_interface_line_context,
                            create_claim_rec.header_attribute1,
                            create_claim_rec.header_attribute9, --283417 

 /*                          , create_claim_rec.header_attribute2
                           , create_claim_rec.header_attribute3
                           , create_claim_rec.header_attribute4
*/
--Ver 1.2 changes end
                            create_claim_rec.header_attribute8, --1.7.2 
              create_claim_rec.attribute_category,
                            create_claim_rec.attribute1,
                            create_claim_rec.attribute2,
                            create_claim_rec.attribute3,
                            create_claim_rec.attribute4,
                            create_claim_rec.attribute5,
                            create_claim_rec.attribute6,
                            create_claim_rec.attribute7,
                            create_claim_rec.attribute8,
                            create_claim_rec.attribute9,
                            create_claim_rec.attribute10,
                            create_claim_rec.attribute11,
                            create_claim_rec.attribute12,
                            create_claim_rec.attribute13,
                            create_claim_rec.attribute14,
                            create_claim_rec.attribute15,
                            NULL,
                            g_user_id,
                            SYSDATE,
                            g_user_id,
                            SYSDATE,
                            g_login_id,
                            'N', --V1.6 change
              create_claim_rec.warehouse_id --v1.9 added
                           );
            END IF;

            print_log_message
                           (   'Before Distribution insert. Interface TXN ID '
                            || create_claim_rec.interface_txn_id
                           );
            print_log_message (   'Before Distribution insert. Account Class '
                               || create_claim_rec.account_class
                              );

            IF l_error_ind = 1
            THEN
               RAISE l_process_excep;
            END IF;

            --inserting records into distribution interface tables
            INSERT INTO ra_interface_distributions_all
                        (interface_distribution_id,
                         interface_line_id,
                         interface_line_context,
                         interface_line_attribute1,
-- Ver1.2 Changes start
/*                      , interface_line_attribute2
                        , interface_line_attribute3
                        , interface_line_attribute4
                        , interface_line_attribute5
                        , interface_line_attribute6
                        , interface_line_attribute7
                        , interface_line_attribute8
                        , interface_line_attribute10
                        , interface_line_attribute11
                        , interface_line_attribute12
                        , interface_line_attribute13
                        , interface_line_attribute14
                        , interface_line_attribute15
                        , interface_line_attribute9
*/
                         interface_line_attribute15,
-- Ver1.2 Changes end
                         account_class,
                         amount,
                         PERCENT,
                         interface_status,
                         request_id,
                         code_combination_id,
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
                         comments,
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
                         created_by,
                         creation_date,
                         last_updated_by,
                         last_update_date,
                         last_update_login,
                         org_id
                        )
                 VALUES (NULL,
                         NULL,
-- Ver1.2 Changes start
/*
            , g_interface_line_context
                        , l_int_line_id
                        , create_claim_rec.interface_line_attribute2
                        , create_claim_rec.interface_line_attribute3
                        , create_claim_rec.interface_line_attribute4
                        , create_claim_rec.interface_line_attribute5
                        , create_claim_rec.interface_line_attribute6
                        , create_claim_rec.interface_line_attribute7
                        , create_claim_rec.interface_line_attribute8
                        , create_claim_rec.interface_line_attribute10
                        , create_claim_rec.interface_line_attribute11
                        , create_claim_rec.interface_line_attribute12
                        , create_claim_rec.interface_line_attribute13
                        , create_claim_rec.interface_line_attribute14
                        , create_claim_rec.interface_line_attribute15
                        , create_claim_rec.interface_line_attribute9
*/
                         g_interface_line_context,
                         create_claim_rec.interface_line_attribute1,
                         l_int_line_id,
-- Ver1.2 Changes end
                         create_claim_rec.account_class,
                         create_claim_rec.amount,
                         100,
                         NULL,
                         NULL,
                         NULL,
                         create_claim_rec.dist_segment1,
                         create_claim_rec.dist_segment2,
                         create_claim_rec.dist_segment3,
                         create_claim_rec.dist_segment4,
                         create_claim_rec.dist_segment5,
                         create_claim_rec.dist_segment6,
                         create_claim_rec.dist_segment7,
                         create_claim_rec.dist_segment8,
                         create_claim_rec.dist_segment9,
                         create_claim_rec.dist_segment10,
                         NULL,
                         NULL,
                         NULL,
                         NULL,
                         NULL,
                         NULL,
                         NULL,
                         NULL,
                         NULL,
                         NULL,
                         NULL,
                         NULL,
                         NULL,
                         NULL,
                         NULL,
                         NULL,
                         NULL,
                         g_user_id,
                         SYSDATE,
                         g_user_id,
                         SYSDATE,
                         g_login_id,
                         create_claim_rec.org_id
                        );

            UPDATE xxar_openclaim_stg xos
               SET process_flag = g_interface,
-- Ver1.2 Changes start
--              , interface_line_attribute1 = l_int_line_id
                   interface_line_attribute15 = l_int_line_id,
-- Ver1.2 Changes end
                   run_sequence_id = g_run_seq_id,
                   last_updated_date = g_sysdate,
                   last_updated_by = g_user_id,
                   last_update_login = g_login_id,
           request_id = g_request_id
             WHERE xos.interface_txn_id = create_claim_rec.interface_txn_id;

            print_log_message
                  (   'After distribution insert. Interface transaction id = '
                   || create_claim_rec.interface_txn_id
                  );
         EXCEPTION
            WHEN l_process_excep
            THEN
               print_log_message ('In process exception' || SQLERRM);
               ROLLBACK TO data_insert;
               g_retcode := 1;
               log_errors (pov_return_status            => l_log_ret_status,
                           -- OUT
                           pov_error_msg                => l_log_err_msg,
                           -- OUT
                           piv_source_column_name       => NULL,
                           piv_source_column_value      => NULL,
                           piv_error_type               => g_err_int,
                           piv_error_code               => 'ETN_AR_STD_INSERT_FAILED',
                           piv_error_message            => SQLERRM
                          );

               UPDATE xxar_openclaim_stg xos
                  SET process_flag = g_error,
-- Ver1.2 Changes start
--                  , interface_line_attribute1 = l_int_line_id
                      interface_line_attribute15 = l_int_line_id,
-- Ver1.2 Changes end
                      run_sequence_id = g_run_seq_id,
                      ERROR_TYPE = g_err_int,
                      last_updated_date = g_sysdate,
                      last_updated_by = g_user_id,
                      last_update_login = g_login_id,
            request_id = g_request_id  -- Added v1.11
                WHERE xos.interface_txn_id = create_claim_rec.interface_txn_id;
            WHEN OTHERS
            THEN
               print_log_message ('In exception of Insert' || SQLERRM);
               ROLLBACK TO data_insert;
               g_retcode := 1;
               l_error_ind := 1;
               log_errors (pov_return_status            => l_log_ret_status,
                           -- OUT
                           pov_error_msg                => l_log_err_msg,
                           -- OUT
                           piv_source_column_name       => NULL,
                           piv_source_column_value      => NULL,
                           piv_error_type               => g_err_int,
                           piv_error_code               => 'ETN_AR_STD_INSERT_FAILED',
                           piv_error_message            => SQLERRM
                          );

               UPDATE xxar_openclaim_stg xos
                  SET process_flag = g_error,
-- Ver1.2 Changes start
--                 , interface_line_attribute1 = l_int_line_id
                      interface_line_attribute15 = l_int_line_id,
-- Ver1.2 Changes start
                      run_sequence_id = g_run_seq_id,
                      ERROR_TYPE = g_err_int,
                      last_updated_date = g_sysdate,
                      last_updated_by = g_user_id,
                      last_update_login = g_login_id,
            request_id = g_request_id -- Added v1.11
                WHERE xos.interface_txn_id = create_claim_rec.interface_txn_id;
         END;
      END LOOP;

      -- for printing conversion stats
      print_stat (g_new_batch_id);
   EXCEPTION
      WHEN OTHERS
      THEN
         g_retcode := 2;
         g_errbuff := 'Failed during Create Claim';
         print_log_message ('In create claim when others' || SQLERRM);
   END create_claim;

--
-- ========================
-- Procedure: pre_validate
-- =============================================================================
--   This procedure is called in pre validate mode. It checks for basic setups/lookups
-- =============================================================================
--  Input Parameters :
--    None
--  Output Parameters :
--  None
-- -----------------------------------------------------------------------------
--
   PROCEDURE pre_validate
   IS
      l_lookup_exists              NUMBER;
      l_payment_lkp       CONSTANT VARCHAR2 (30) := 'ETN_AR_PAYMENT_TERMS';
      l_receipt_lkp       CONSTANT VARCHAR2 (30) := 'ETN_AR_RECEIPT_METHOD';
      l_ar_trx_lkp        CONSTANT VARCHAR2 (30) := 'ETN_AR_TRANSACTION_TYPE';
      l_reason_code_lkp   CONSTANT VARCHAR2 (30)
                                             := 'ETN_OTC_REASON_CODE_MAPPING';
      l_ou_lkp            CONSTANT VARCHAR2 (30) := 'ETN_COMMON_OU_MAP';
   BEGIN
      l_lookup_exists := 0;
      print_log_message ('+   PROCEDURE : pre_validate +');
      print_log_message ('+ Checking Customer Cross Reference +');
      print_log_message ('- Checking Customer Cross Reference -');
      print_log_message ('+ Checking COA Cross Reference +');
      print_log_message ('- Checking COA Cross Reference -');
      print_log_message ('+ Checking Receipt Method Lookup +');
      l_lookup_exists := 0;

      BEGIN
         SELECT 1
           INTO l_lookup_exists
           FROM fnd_lookup_types flv
          WHERE flv.lookup_type = l_receipt_lkp;
      EXCEPTION
         WHEN NO_DATA_FOUND
         THEN
            l_lookup_exists := 0;
            print_log_message
                       (   ' In No Data found of Receipt Method lookup check'
                        || SQLERRM
                       );
         WHEN OTHERS
         THEN
            l_lookup_exists := 0;
            print_log_message
                         (   ' In when others of Receipt Method lookup check'
                          || SQLERRM
                         );
      END;

      IF l_lookup_exists = 0
      THEN
         g_retcode := 1;
         fnd_file.put_line (fnd_file.output,
                            'RECEIPT METHOD LOOKUP IS NOT SETUP'
                           );
      END IF;

      print_log_message ('- Checking Receipt Method Lookup -');
      print_log_message ('+ Checking Transaction Type Lookup +');
      --ETN_AR_TRANSACTION_TYPE
      l_lookup_exists := 0;

      BEGIN
         SELECT 1
           INTO l_lookup_exists
           FROM fnd_lookup_types flv
          WHERE flv.lookup_type = l_ar_trx_lkp;
      EXCEPTION
         WHEN NO_DATA_FOUND
         THEN
            l_lookup_exists := 0;
            print_log_message
                     (   ' In No Data found of Transaction Type Lookup check'
                      || SQLERRM
                     );
         WHEN OTHERS
         THEN
            l_lookup_exists := 0;
            print_log_message
                       (   ' In when others of Transaction Type lookup check'
                        || SQLERRM
                       );
      END;

      IF l_lookup_exists = 0
      THEN
         g_retcode := 1;
         fnd_file.put_line (fnd_file.output,
                            'Transaction Type lookup is not setup'
                           );
      END IF;

      print_log_message ('- Checking Transaction Type Lookup -');
      print_log_message ('+ Checking Reason code lookup +');
      --ETN_AR_REASON_CODES
      l_lookup_exists := 0;

      BEGIN
         SELECT 1
           INTO l_lookup_exists
           FROM fnd_lookup_types flv
          WHERE flv.lookup_type = l_reason_code_lkp;
      EXCEPTION
         WHEN NO_DATA_FOUND
         THEN
            l_lookup_exists := 0;
            print_log_message
                          (   ' In No Data found of Reason Code Lookup check'
                           || SQLERRM
                          );
         WHEN OTHERS
         THEN
            l_lookup_exists := 0;
            print_log_message
                            (   ' In when others of Reason Code lookup check'
                             || SQLERRM
                            );
      END;

      IF l_lookup_exists = 0
      THEN
         g_retcode := 1;
         fnd_file.put_line (fnd_file.output,
                            'Reason Code LOOKUP IS NOT SETUP' || SQLERRM
                           );
      END IF;

      print_log_message ('- Checking Reason code lookup -');
      print_log_message ('+ Checking Payment Term Lookup +');
      l_lookup_exists := 0;

      BEGIN
         SELECT 1
           INTO l_lookup_exists
           FROM fnd_lookup_types flv
          WHERE flv.lookup_type = l_payment_lkp;
      EXCEPTION
         WHEN NO_DATA_FOUND
         THEN
            l_lookup_exists := 0;
            print_log_message
                         (   ' In No Data found of Payment term lookup check'
                          || SQLERRM
                         );
         WHEN OTHERS
         THEN
            l_lookup_exists := 0;
            print_log_message
                           (   ' In when others of Payment term lookup check'
                            || SQLERRM
                           );
      END;

      IF l_lookup_exists = 0
      THEN
         g_retcode := 1;
         fnd_file.put_line (fnd_file.output,
                            'PAYMENT TERM LOOKUP IS NOT SETUP'
                           );
      END IF;

      print_log_message ('- Checking Payment Term Lookup -');
      print_log_message ('+ Checking Operating Unit Lookup +');
      l_lookup_exists := 0;

      BEGIN
         SELECT 1
           INTO l_lookup_exists
           FROM fnd_lookup_types flv
          WHERE flv.lookup_type = l_ou_lkp;
      EXCEPTION
         WHEN NO_DATA_FOUND
         THEN
            l_lookup_exists := 0;
            print_log_message
                            (   ' In No Data found of Common OU Lookup check'
                             || SQLERRM
                            );
         WHEN OTHERS
         THEN
            l_lookup_exists := 0;
            print_log_message (   ' In when others of Common OU lookup check'
                               || SQLERRM
                              );
      END;

      IF l_lookup_exists = 0
      THEN
         g_retcode := 1;
         fnd_file.put_line (fnd_file.output, 'COMMON OU LOOKUP IS NOT SETUP');
      END IF;

      print_log_message ('- Checking Operating Unit Lookup -');
      print_log_message ('-   PROCEDURE : pre_validate -');
   EXCEPTION
      WHEN OTHERS
      THEN
         g_retcode := 2;
         g_errbuff := 'Failed during Pre Validate';
         print_log_message ('In Pre Validate when others' || SQLERRM);
   END pre_validate;

--
-- ========================
-- Procedure: assign_batch_id
-- =============================================================================
--   This procedure assigns batch id
-- =============================================================================
--  Input Parameters :
--    None
--  Output Parameters :
--    None
-- -----------------------------------------------------------------------------
--
   PROCEDURE assign_batch_id
   IS
   BEGIN
      print_log_message (   '+   PROCEDURE : assign_batch_id - g_batch_id :'
                         || g_new_batch_id
                        );

      -- g_batch_id NULL is considered a fresh run
      IF g_batch_id IS NULL
      THEN
         UPDATE xxar_openclaim_stg xos
            SET batch_id = g_new_batch_id,
                run_sequence_id = g_run_seq_id,
                last_updated_date = SYSDATE,
                last_updated_by = g_user_id,
                last_update_login = g_login_id,
                program_application_id = g_prog_appl_id,
                program_id = g_conc_program_id,
                program_update_date = SYSDATE,
                request_id = g_request_id
          WHERE xos.batch_id IS NULL;

         print_log_message (   'Updated staging table where batch id is null'
                            || SQL%ROWCOUNT
                           );
      ELSE
--------------------------------------------
-- All : All the records in batch other than obsolete
--------------------------------------------
-- Error : Records where validated flag are 'E'
--------------------------------------------
-- Unprocessed : Records which are assigned
-- batch ID but are in new status
--------------------------------------------
         IF g_process_records = 'ALL'
         THEN
            UPDATE xxar_openclaim_stg xos
               SET process_flag = g_new,
                   ERROR_TYPE = NULL,
                   run_sequence_id = g_run_seq_id,
                   last_updated_date = SYSDATE,
                   last_updated_by = g_user_id,
                   last_update_login = g_login_id,
                   program_application_id = g_prog_appl_id,
                   program_id = g_conc_program_id,
                   program_update_date = SYSDATE,
                   request_id = g_request_id
             WHERE xos.batch_id = g_new_batch_id
             AND (xos.process_flag IN (g_new, g_valid)                          -- modified for v1.12
                  OR (xos.process_flag = g_error AND xos.error_type = g_err_val));   -- added for v1.12

            print_log_message
                          (   'Updated staging table where process record All'
                           || SQL%ROWCOUNT
                          );
         ELSIF g_process_records = 'ERROR'
         THEN
            UPDATE xxar_openclaim_stg xos
               SET process_flag = g_new,
                   ERROR_TYPE = NULL,
                   run_sequence_id = g_run_seq_id,
                   last_updated_date = SYSDATE,
                   last_updated_by = g_user_id,
                   last_update_login = g_login_id,
                   program_application_id = g_prog_appl_id,
                   program_id = g_conc_program_id,
                   program_update_date = SYSDATE,
                   request_id = g_request_id
             WHERE xos.batch_id = g_new_batch_id
             AND xos.process_flag = g_error
             AND xos.error_type = g_err_val; -- modified for v1.12

            print_log_message
                        (   'Updated staging table where process record Error'
                         || SQL%ROWCOUNT
                        );
         ELSIF g_process_records = 'UNPROCESSED'
         THEN
            UPDATE xxar_openclaim_stg xos
               SET run_sequence_id = g_run_seq_id,
                   last_updated_date = SYSDATE,
                   last_updated_by = g_user_id,
                   last_update_login = g_login_id,
                   program_application_id = g_prog_appl_id,
                   program_id = g_conc_program_id,
                   program_update_date = SYSDATE,
                   request_id = g_request_id
             WHERE xos.batch_id = g_new_batch_id
             AND xos.process_flag = g_new;

            print_log_message
                        (   'Updated staging table where process record Error'
                         || SQL%ROWCOUNT
                        );
         END IF;
      END IF;                                                    -- g_batch_id

      COMMIT;
   EXCEPTION
      WHEN OTHERS
      THEN
         g_retcode := 2;
         g_errbuff := 'Failed during Assign Batch Id';
         print_log_message (   'In When Other of Assign Batch Id procedure'
                            || SQLERRM
                           );
   END assign_batch_id;

--
-- ========================
-- Procedure: main
-- =============================================================================
--   This is a main public procedure, which will be invoked through concurrent
--   program.
--
--   This conversion program is used to validate all incoming data, convert claims data
--   from legacy system to Oracle 12 Debit Memos. 0$ AR receipt are created for
--   Debit Memos and Debit Memos are applied on created receipts..
--
-- =============================================================================
--
-- -----------------------------------------------------------------------------
--  Called By Concurrent Program: Eaton UNIFY Open Claim Conversion Program
-- -----------------------------------------------------------------------------
-- -----------------------------------------------------------------------------
--
--  Input Parameters :
--    piv_run_mode        : Control the program excution for LOAD DATA/ PRE VALIDATE/ VALIDATE / CONVERSION / RECONCILE
--    piv_hidden          : Dummy parameter
--    pin_batch_id        : List all unique batches from staging table , this will
--                        be NULL for first Conversion Run.
--    piv_dummy           : Dummy parameter
--    piv_process_records : Conditionally available only when P_BATCH_ID is popul-
--                        -ated. Otherwise this will be disabled and defaulted
--                        to ALL
--    piv_gl_date         : Input GL Date
--
--  Output Parameters :
--    p_errbuf          : Standard output parameter for concurrent program
--    p_retcode         : Standard output parameter for concurrent program
--
--  Return     : Not applicable
-- -----------------------------------------------------------------------------
   PROCEDURE main (
      pov_errbuf            OUT NOCOPY      VARCHAR2,
      pon_retcode           OUT NOCOPY      NUMBER,
      piv_run_mode          IN              VARCHAR2,
      -- PRE VALIDATE/VALIDATE/CONVERSION/RECONCILE
      piv_hidden            IN              VARCHAR2,        -- DUMMY VARIABLE
      pin_batch_id          IN              NUMBER,       -- NULL / <BATCH_ID>
      piv_dummy             IN              VARCHAR2,        -- DUMMY VARIABLE
      piv_process_records   IN              VARCHAR2,
      -- (A) ALL / (E) ERROR ONLY / (N) UNPROCESSED
      piv_gl_date           IN              VARCHAR2
   )
   IS
      l_return_status   VARCHAR2 (200)  := NULL;
      l_err_msg         VARCHAR2 (2000) := NULL;
      l_user_excp       EXCEPTION;
   BEGIN
      xxetn_debug_pkg.initialize_debug
                                (pov_err_msg           => l_err_msg,
                                 piv_program_name      => 'ETN_AR_OPEN_CLAIM_CONV'
                                );
      print_log_message (   'Started Claims Conversion at '
                         || TO_CHAR (g_sysdate, 'DD-MON-YYYY HH24:MI:SS')
                        );
      print_log_message
         ('+---------------------------------------------------------------------------+'
         );
      -- Check if debug logging is enabled/disabled
      g_run_mode := piv_run_mode;
      g_batch_id := pin_batch_id;
      g_process_records := piv_process_records;
      g_gl_date := TO_DATE (piv_gl_date, 'YYYY/MM/DD:HH24:MI:SS');
      -- Call Common Debug and Error Framework initialization
      print_log_message ('Program Parameters  : ');
      print_log_message ('---------------------------------------------');
      print_log_message ('Run Mode            : ' || g_run_mode);
      print_log_message ('Batch ID            : ' || pin_batch_id);
      print_log_message ('Process records     : ' || g_process_records);
      print_log_message ('GL Date             : ' || g_gl_date);
      print_log_message ('Operating Unit      : ' || g_org_id);

      IF piv_run_mode = 'LOAD-DATA'
      THEN
         print_log_message ('In Load Data Mode');
         get_data ();
      END IF;

      IF piv_run_mode = 'PRE-VALIDATE'
      THEN
         print_log_message ('In Pre-Validate Mode');
         pre_validate ();
      END IF;

      IF piv_run_mode = 'VALIDATE'
      THEN
         print_log_message ('In Validate Mode');

         IF g_gl_date IS NULL
         THEN
            print_log_message ('Exiting Program as GL Date is not passed');
            RAISE l_user_excp;
         END IF;

         g_run_seq_id := xxetn_run_sequences_s.NEXTVAL;
         xxetn_common_error_pkg.g_run_seq_id := g_run_seq_id;

         IF g_batch_id IS NULL
         THEN
            g_new_batch_id := xxetn_batches_s.NEXTVAL;
            print_log_message ('New Batch Id' || g_new_batch_id);
         ELSE
            g_new_batch_id := g_batch_id;
         END IF;

         assign_batch_id ();                      --API for assigning batch id

         IF g_retcode > 0
         THEN
            RAISE l_user_excp;
         END IF;

         validate_claim ();
      END IF;

      IF piv_run_mode = 'CONVERSION'
      THEN
         print_log_message ('In Conversion Mode');

         IF g_batch_id IS NULL
         THEN
            print_log_message
               ('Batch Id is required to run program in conversion mode to process record in R12.'
               );
            RAISE l_user_excp;
         ELSE
            g_new_batch_id := g_batch_id;
         END IF;

         g_run_seq_id := xxetn_run_sequences_s.NEXTVAL;
         xxetn_common_error_pkg.g_run_seq_id := g_run_seq_id;
         create_claim ();
      END IF;

      IF piv_run_mode = 'RECONCILE'
      THEN
         print_log_message ('In Reconciliation Mode');
         g_run_seq_id := NULL;
         print_stat (g_batch_id);
      END IF;

      IF l_source_tab.COUNT > 0
      THEN
         xxetn_common_error_pkg.add_error
                                       (pov_return_status      => l_return_status,
                                        -- OUT
                                        pov_error_msg          => l_err_msg,
                                        -- OUT
                                        pi_source_tab          => l_source_tab,
                                        -- IN  G_SOURCE_TAB_TYPE
                                        pin_batch_id           => g_new_batch_id
                                       );
         l_source_tab.DELETE;
      END IF;

      pon_retcode := g_retcode;
      pov_errbuf := g_errbuff;
      print_log_message
         ('+---------------------------------------------------------------------------+'
         );
      print_log_message (   'Claim Conversion Ends at: '
                         || TO_CHAR (g_sysdate, 'DD-MON-YYYY HH24:MI:SS')
                        );
      print_log_message ('---------------------------------------------');
   EXCEPTION
      WHEN l_user_excp
      THEN
         print_log_message (   'In User Exception of Main procedure. '
                            || SUBSTR (SQLERRM, 1, 150)
                           );
         pov_errbuf :=
               'Error : Main program procedure encounter User Exception. '
            || SUBSTR (SQLERRM, 1, 150);
         pon_retcode := 2;
      WHEN OTHERS
      THEN
         print_log_message (   'In When Others of Main procedure. '
                            || SUBSTR (SQLERRM, 1, 150)
                           );
         pov_errbuf :=
               'Error : Main program procedure encounter error. '
            || SUBSTR (SQLERRM, 1, 150);
         pon_retcode := 2;
   END main;
END xxar_open_claim_pkg;
/

SHOW ERRORS;

EXIT;
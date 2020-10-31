--Begin Revision History
--<<<
-- 16-Jan-2017 02:38:46 C9988598 /main/45
-- 
--<<<
-- 17-Mar-2017 04:52:21 E9974449 /main/46
-- 
-- <<<
--End Revision History  
CREATE OR REPLACE PACKAGE BODY XXAR_INVOICES_PKG
----------------------------------------------------------------------------------------------------------------------------------
--    Owner        : EATON CORPORATION.
--    Application  : Account Receivables
--    Schema       : APPS
--    Compile AS   : APPS
--    File Name    : XXAR_INVOICES_PKG.pkb
--    Date         : 15-Jan-2014
--    Author       : Seema Machado
--    Description  : Package Body for AR Invoices Conversion.
--    Version      : $ETNHeader: /CCSTORE/ccweb/E9974449/E9974449_AR_TOP_view/vobs/AR_TOP/xxar/12.0.0/install/xxar_invoices_pkg.pkb /main/46 17-Mar-2017 04:52:21 E9974449  $
--
--    Parameters   :
--    piv_run_mode        : Control the program excution for VALIDATE CONVERSION
--
--    pin_batch_id        : List all unique batches from staging table , this will
--                          be NULL for first Conversion Run.
--    piv_process_records : Conditionally available only when pin_batch_id is populated.
--                          Otherwise this will be disabled and defaulted to ALL
--    piv_operating_unit  : 11i Operating Unit. If provided then program will
--                      run only for specified Operating unit
--    piv_gl_date         :  GL date for which the conversion is done. This
--                      date will be considered only during VALIDATE mode
--                      for transaction other than service contracts
--
--    Change History
--  ===============================================================================================================================
--    v1.0    Seema Machado      15-Jan-2014     Initial Creation
--    v1.1    Seema Machado      12-Feb-2014     Invoice and distribution data will be received in separate tables and
--                                               data will come at line level instead of consolidated format.
--    v1.2    Seema Machado      18-Feb-2014     Due dates need to be updated in case of difference in legacy and R12 values
--                                               For partially paid invoices, invoices will be created with original amount
--                                               and adjustments will be created on the invoices to reflect the remaining amount.
--    v1.3    Seema Machado      23-May-2014     FOT Issue changes
--    v1.4    Rohit Devadiga     26-Jun-2014     FOT Issue changes
--    v1.5    Seema Machado      08-Aug-2014     DFF Rationalization changes
--    v1.6    Seema Machado      12-Aug-2014     ETN_MAP_UNIT OU cross reference changes
--    v1.7    Seema Machado      08-Sep-2014     Accounting rule changes
--    v1.8    Seema Machado      12-Sep-2014     Customer ref changes
--    v1.9    Seema Machado      28Oct-2014      Changes done to resolve ALM ticket 351 for DFF changes to Tax lines
--    v1.10   Seema Machado      19-Mar-2015     Changes done to accommodate the transaction source and considering plant from REC
--                                               as per CR283417
--    v1.11   Seema Machado      28-Mar-2015     Changes done to accommodate the tax CR272389
--    v1.12   Krishna Shenoy     08-May-2015     Changes for Mock2 Conversion
--    v1.13   Preeti Sethi       22-May-2015     Changes for MOCK2 Conversion
--    v1.14   Setu Shah          21-Jul-2015     the last '%' is uncommented
--                                               It was observed during the validation of OU US AR that there are a few
--                                               customer-Numbers available in R12's hca.orig_system_reference as
--                                               '<leg_customer_number>-1'. To allow the above query to find that customer it
--                                               was necessary to add '%' at the end. Hence the commented part was uncommented
--    v1.15   Seema Machado      31-Jul-2015     Changes done to fix defect 2923 to correct the tax lines grouping changes done as part of CR272389 (ver1.11)-ignoring DFF attributes --                                             while grouping
--    v1.16   Seema Machado      03-Aug-2015     Changes done to fix defect 2173 to ignore tax code that are inactive in ebTax
--    v1.17   Seema Machado      20-Aug-2015     Changes done to fix defect 2662 to consider org_id while updating R12 tax information
--    v1.18   Seema Machado      24-Aug-2015     Changes done to fix defect 2881 to consider leg_bill_to_address when logging customer cross-ref errors
--    v1.19   Seema Machado      11-Sep-2015     Changes done to fix defect 2974 for factoring flag issue
--    v1.20   Seema Machado      03-Sep-2015     Changes done to implement CR328721 for:
--                                   1.20.1:     Deriving OU for NFSC based on interface_header_attribute1
--                                   1.20.2:     Map DFFs fields from 11i to R12
--                                   1.20.3:     Map header_attribute9 to plant only
--    v1.21   Seema Machado      10-Sep-2015     Changes done to implement CR324836:
--                                   1.21.1:     Changes done to store distribution amount in new column dist_amount
--                                   1.21.2:     To recalculate distribution amount for inclusive tax issue.
--                                   1.21.3:     Mapping leg_amount_includes_tax_flag
--    v1.22   Seema Machado      16-Sep-2015     Changes done to fix defect 2460: Tax information must not be populated only LINE type records.
--    v1.23   Seema Machado      06-Oct-2015     Changes done to fix defect 2479: DFF mapping for Sales rep
--    v1.24   Shailesh Chaudhari 06-Oct-2015     Changes done for 305000: Warehouse ID for Brazil.
--    v1.25   Seema Machado      16-Oct-2015     Changes done to fix defect 2479: DFF mapping for batch source name
--    v1.26   Seema Machado      26-Oct-2015     Changes done to implement CR342077 for correcting REC accounting for NFSC invoices
--    v1.27   Seema Machado      13-Nov-2015     Changes done to implement CR346150 for adding additional condition to derive R12 customer based on orig sys ref at site
--    v1.28   Seema Machado      24-Nov-2015     Changes done to make warehouse id NULL for non-brazil invoices
--    v1.29   Piyush Ojha        02-DEC-2015     changes done to remove org id hard coding for Brazil ware house id changes.
--    v1.30   Kulraj Singh       03-Dec-2015     Added Index hints.
--    v1.31   Preeti Sethi       31-Dec-2015     Added AR Period instead of GL Period for Defect 4688
--    v1.32   Kulraj Singh       25-Jan-2016     Performance Tuning.
--    v1.33   Setu Shah          29-Jan-2016     Changed validation query to search customer/site for NAFSC (Else)
--                                               Earlier it used to match leg_customer_number. That reference was removed.
--                                               Instead of matching location of hz_cust_site_uses_all now we are matching
--                                               Orig_System_Ref of hz_cust_acct_sites_all
--    v1.34   Kulraj Singh       01-Feb-2016     Added lookup XXAR_CUST_CNV_BR_OU_MAP instead of Eaton EPS OU hardcoding
--                                               Defect# 5138
--                                               v1.33 changes done for bill to customer site validation as well
--    v1.35   Piyush Ojha        05-Feb-2016     Defect#5232 Due Date Update performance issue change. Putting if else condition
--                                               to avoid 'OKS' service contracts update statement which is getting stuck.
--    v1.36   Preeti Sethi       17-Mar-2016     Changes implemented for CR #371665 Defect 4073 to add receipt_method_id and receipt_method_name.
--    v1.37   Preeti Sethi       23-Mar-2016     Adding additonal check for CR #37665 to check whether receipt method is linked with R12 customer or not.
--    v1.38   Preeti Sethi       29-Mar-2016     Placed l_valid_flag derivation after deriving Receipt Method
--    v1.39   Seema Machado      29-Mar-2016     Changes done to accommodate CR371665
--                                   1.39.1:     Changes done to load payment schedule data in table xxar_inv_pmt_schedules_stg
--                                   1.39.2:     Changes done to populate leg_source_system when tax lines are getting merged
--                                               so that the adjustment and due date update logic works correctly
--                                   1.39.3:     Changes done to update due date based on payment schedule table xxar_inv_pmt_schedules_stg to handle
--                                               multiple due dates for multiple payment schedules
--    v1.40   Kulraj Singh       04-Apr-2016     Defect# 5388. Tie back updated to correctly mark successul records as 'C'.
--                                               Tie back: Consider REC lines during distribution staging table update
--    v1.41   Seema Machado      11-Apr-2016     Changes done to add leg_source_system condition to fix defect 6174
--    v1.42   Kulraj Singh       05-May-2016     CR# 376678/Defect#4875. Segment6 and Segment7 to be custom populated for IC customers for US/Canada based on customer account number
--    v1.43   Kulraj Singh       06-Jun-2016     CR# 386219/Defect# 6076/Defect# 7064. Validate multiple LEs in AR Invoice. Map ERP Customer# in header attribute3
--    v1.44   Kulraj Singh       15-Jun-2016     Incorporated Clear SQL Report review comments.
--    v1.45   Kulraj Singh       21-Jun-2016     Modified due_date proc and assign_batch_id for performance consideration.
--                                               Modified due_date proc to fix wrong due date update. Defect# 8258
--                                               Removed reference of leg_customer_number for non NAFSC OUs
--                                               Commented delete statements for RA_INTERFACE_DISTRIBUTIONS_ALL and RA_INTERFACE_LINES_ALL based on attribute14
--                                               Added condition of interface_line_context = 'Eaton' while deleting from both interface tables to improve performance
--                                               Assigned sysdate to global variable and used it instead
--    v1.46   Kulraj Singh       22-Jul-2016     Removed cursor fetch_x_invoices_cur and used direct update statement for marking lines in X status
--    v1.47   Kulraj Singh       27-Jul-2016     Added COMMIT and used leg_cust_trx_line_id in update statement for process_flag X
--    v1.48   Kulraj Singh       29-Aug-2016     Defect# 9376. Request id updated for error_type as 'ERR_INT' in staging table.
--                                               Defect# 9239. Removed validation/code related to Service Contracts with %OKS%.
--                                               Defect# 9542. Modified if statement to correct mapping of attribute14 with leg_batch_source
--                                               Defect# 9249. Change in Multi LE transactions check cursor.
--                                               Defect# 9404. Distribution amount calculation restricted to leg operating unit OU ELECTRICAL BR, OU FLUID POWER BR and OU TRUCK COMPONENTS BR
--                                               where amount_includes_tax_flag is Y.
--    v1.49   Preeti Sethi       14-Sep-2016     Defect#9210 : Attach receipt method to appropriate site
--    v1.50   Preeti Sethi       19-Sep-2016     Defect#9697 : Factoring flags are not converted properly for ISSC transactions. Pulling Leg_Header_Attribute14 column value from 11i to R12. As of now, it is coming as Null.
--    v1.51   Preeti Sethi       23-Sep-2016     Defect#9375  : Avoid validating orphan Distribution Lines.
--    v1.52   Piyush Ojha        18-OCT-2016     Mock 4 Correcting default warehouses name as per CV40 update by Daiany
--    v1.53   Preeti Sethi       24-Oct-2016     MOCK#4 : Using System_Bill_Customer_Id while checking instead of l_r12_cust_id variable while checking customer site is available in R12.
--    v1.54   Kulraj Singh       08-Nov-2016     Defect# 12122. Removed distinct from cursor cur_inclusive_trx in recalc_dist_amount to correctly calculate line amount if multiple tax lines have same amount
--                                               Added error handling for warehouse meaning derivation for Brazilian OUs if the value does not exist in lookup. This is done to avoid NULL warehouse id going into interface tables
--                                               un-commented amount_includes_tax_flag in cursor get_tax_lines_wrapper_cur to keep this cursor in sync with get_tax_lines_cur as otherwise it will cause incorrect grouping of tax lines
--    v1.55   Kulraj Singh       28-Nov-2016     Added commit after multi LE loop and orphan records loop to avoid table locks
--                                               Added REC in main query of fetching orphan records and used NOT EXISTS instead of NOT IN
--    v1.56   Piyush Ojha        14-Dec-2016     Removed Brazil OU's from Multiple legal entity check
--    v1.57   Preeti Sethi       16-Dec-2016     Defect#13005 - Modified condition from Multiple LE Cursor to exclude Brazil OU's.
--    v1.58   Kulraj Singh       21-Dec-2016     Defect# 12943 and 12944. Removed if condition around l_cm_term_error before updating R12 trx type, transaction type id.
--                                               It will be updated irrespective of success/error records
--    v1.59   Preeti Sethi       22-Dec-2016     Defect#13003 - Added Org_ID Condition to check whether receipt_method is attached to correct Org_Id.
--    v1.60   Preeti Sethi       16-Jan-2017     Defect#12840 -  Inter-Company Transactions - Segment 3 of accounting converted as 11411 for the majority of items as 15310.
--    v1.61   Piyush Ojha        17-Mar-2017     Defect#15613 Program logged error for operating unit but marked records as process flag V
--    =============================================================================================================================
 AS

--/CCSTORE/ccweb/C9988598/C9988598_view_R12_AR_INSTALL/vobs/AR_TOP/xxar/12.0.0/install/xxar_invoices_pkg.pkb /main/32  -- Declaration of global variables
  -- WHO columns
  g_request_id NUMBER DEFAULT fnd_global.conc_request_id;

  g_prog_appl_id NUMBER DEFAULT fnd_global.prog_appl_id;

  g_conc_program_id NUMBER DEFAULT fnd_global.conc_program_id;

  g_user_id NUMBER DEFAULT fnd_global.user_id;

  g_login_id NUMBER DEFAULT fnd_global.login_id;

  g_last_updated_by NUMBER DEFAULT fnd_global.user_id;

  g_last_update_login NUMBER DEFAULT fnd_global.login_id;

  g_sysdate CONSTANT DATE := SYSDATE;

  --Count variables
  g_total_count NUMBER DEFAULT 0;

  g_total_dist_count NUMBER DEFAULT 0;

  --ver1.39.1 changes start
  g_total_pmt_sch_count NUMBER DEFAULT 0;

  g_failed_pmt_sch_count NUMBER DEFAULT 0;

  g_loaded_pmt_sch_count NUMBER DEFAULT 0;

  --ver1.39.1 changes end
  g_loaded_count NUMBER DEFAULT 0;

  g_loaded_dist_count NUMBER DEFAULT 0;

  g_failed_count NUMBER DEFAULT 0;

  g_failed_dist_count NUMBER DEFAULT 0;

  --Table type Index
  g_line_idx NUMBER := 1;

  g_dist_idx NUMBER := 1;

  g_err_indx NUMBER := 0;

  -- Program parameters
  g_retcode NUMBER := 0;

  g_errbuff VARCHAR2(20) := 'SUCCESS';

  g_run_mode VARCHAR2(100);

  g_process_records VARCHAR2(100);

  g_gl_date DATE;

  --Program level
  g_batch_id NUMBER;

  g_new_batch_id NUMBER;

  g_new_run_seq_id NUMBER;

  g_batch_source ar_batch_sources_all.NAME%TYPE := 'CONVERSION';

  g_interface_line_context VARCHAR2(240) := 'Eaton';

  g_debug_err VARCHAR2(2000);

  g_log_level NUMBER := 0;

  --Lookup names
  g_ou_lookup fnd_lookup_types_tl.lookup_type%TYPE := 'ETN_COMMON_OU_MAP';

  g_pmt_term_lookup fnd_lookup_types_tl.lookup_type%TYPE := 'ETN_AR_PAYMENT_TERMS';

  g_trx_type_lookup fnd_lookup_types_tl.lookup_type%TYPE := 'ETN_AR_TRANSACTION_TYPE';

  g_tax_code_lookup fnd_lookup_types_tl.lookup_type%TYPE := 'ETN_OTC_TAX_CODE_MAPPING';

  TYPE g_invoice_rec IS TABLE OF xxar_invoices_stg%ROWTYPE INDEX BY BINARY_INTEGER;

  g_invoice g_invoice_rec;

  TYPE g_invoice_det_rec IS TABLE OF xxar_invoices_stg%ROWTYPE INDEX BY BINARY_INTEGER;

  g_invoice_details g_invoice_det_rec;

  TYPE g_invoice_dist_rec IS TABLE OF xxar_invoices_dist_stg%ROWTYPE INDEX BY BINARY_INTEGER;

  g_invoice_dist g_invoice_dist_rec;

  g_index NUMBER := 1;

  g_err_lmt NUMBER := fnd_profile.VALUE('ETN_FND_ERROR_TAB_LIMIT');

  g_leg_operating_unit VARCHAR2(240);

  g_leg_trasaction_type VARCHAR2(240);

  g_error_tab xxetn_common_error_pkg.g_source_tab_type;

  g_direction VARCHAR2(240) := 'LEGACY-TO-R12';

  g_coa_error CONSTANT VARCHAR2(30) := 'Error';

  g_coa_processed CONSTANT VARCHAR2(30) := 'Processed';

  g_period_set_name VARCHAR2(50);

  --
  -- ========================
  -- Procedure: PRINT_LOG_MESSAGE
  -- =============================================================================
  --   This procedure is used to write message to log file.
  -- =============================================================================
  PROCEDURE print_log_message(piv_message IN VARCHAR2) IS
  BEGIN
    IF (NVL(g_request_id, 0) > 0) AND xxetn_debug_pkg.isdebugon THEN
      fnd_file.put_line(fnd_file.LOG, piv_message);
      --NULL;  -- Commented for v1.44
    END IF;
  EXCEPTION
    WHEN OTHERS THEN
      --NULL;
      -- Added below for v1.44
      fnd_file.put_line(fnd_file.LOG,
                        'Error in Proc:print_log_message: ' || SQLERRM);
  END print_log_message;

  --
  -- ========================
  -- Procedure: PRINT_LOG1_MESSAGE
  -- =============================================================================
  --   This procedure is used to write message to log file if log level is set to 1.
  -- =============================================================================
  PROCEDURE print_log1_message(piv_message IN VARCHAR2) IS
  BEGIN
    IF NVL(g_request_id, 0) > 0 AND xxetn_debug_pkg.isdebugon THEN
      fnd_file.put_line(fnd_file.LOG, piv_message);
    END IF;
  EXCEPTION
    WHEN OTHERS THEN
      --NULL;
      -- Added below for v1.44
      fnd_file.put_line(fnd_file.LOG,
                        'Error in proc: print_log1_message: ' || SQLERRM);
  END print_log1_message;

  --
  -- ========================
  -- Procedure: log_errors
  -- =============================================================================
  --   This procedure will log the errors in the error report using error
  --   framework
  -- =============================================================================
  PROCEDURE log_errors(pin_transaction_id      IN NUMBER DEFAULT NULL,
                       piv_source_keyname1     IN xxetn_common_error.source_keyname1%TYPE DEFAULT NULL,
                       piv_source_keyvalue1    IN xxetn_common_error.source_keyvalue1%TYPE DEFAULT NULL,
                       piv_source_keyname2     IN xxetn_common_error.source_keyname2%TYPE DEFAULT NULL,
                       piv_source_keyvalue2    IN xxetn_common_error.source_keyvalue2%TYPE DEFAULT NULL,
                       piv_source_keyname3     IN xxetn_common_error.source_keyname3%TYPE DEFAULT NULL,
                       piv_source_keyvalue3    IN xxetn_common_error.source_keyvalue3%TYPE DEFAULT NULL,
                       piv_source_keyname4     IN xxetn_common_error.source_keyname4%TYPE DEFAULT NULL,
                       piv_source_keyvalue4    IN xxetn_common_error.source_keyvalue4%TYPE DEFAULT NULL,
                       piv_source_keyname5     IN xxetn_common_error.source_keyname5%TYPE DEFAULT NULL,
                       piv_source_keyvalue5    IN xxetn_common_error.source_keyvalue5%TYPE DEFAULT NULL,
                       piv_source_column_name  IN xxetn_common_error.source_column_name%TYPE DEFAULT NULL,
                       piv_source_column_value IN xxetn_common_error.source_column_value%TYPE DEFAULT NULL,
                       piv_source_table        IN xxetn_common_error.source_table%TYPE DEFAULT NULL,
                       piv_error_type          IN xxetn_common_error.ERROR_TYPE%TYPE,
                       piv_error_code          IN xxetn_common_error.ERROR_CODE%TYPE,
                       piv_error_message       IN xxetn_common_error.error_message%TYPE,
                       pov_return_status       OUT VARCHAR2,
                       pov_error_msg           OUT VARCHAR2) IS
    l_return_status VARCHAR2(1);
    l_error_message VARCHAR2(2000);
  BEGIN
    --  xxetn_debug_pkg.add_debug ( p_err_msg );
    g_err_indx := g_err_indx + 1;
    g_error_tab(g_err_indx).source_table := NVL(piv_source_table,
                                                'XXAR_INVOICES_STG');
    g_error_tab(g_err_indx).interface_staging_id := pin_transaction_id;
    g_error_tab(g_err_indx).source_keyname1 := piv_source_keyname1;
    g_error_tab(g_err_indx).source_keyvalue1 := piv_source_keyvalue1;
    g_error_tab(g_err_indx).source_keyname2 := piv_source_keyname2;
    g_error_tab(g_err_indx).source_keyvalue2 := piv_source_keyvalue2;
    g_error_tab(g_err_indx).source_keyname3 := piv_source_keyname3;
    g_error_tab(g_err_indx).source_keyvalue3 := piv_source_keyvalue3;
    g_error_tab(g_err_indx).source_keyname4 := piv_source_keyname4;
    g_error_tab(g_err_indx).source_keyvalue4 := piv_source_keyvalue4;
    g_error_tab(g_err_indx).source_keyname5 := piv_source_keyname5;
    g_error_tab(g_err_indx).source_keyvalue5 := piv_source_keyvalue5;
    g_error_tab(g_err_indx).source_column_name := piv_source_column_name;
    g_error_tab(g_err_indx).source_column_value := piv_source_column_value;
    g_error_tab(g_err_indx).ERROR_TYPE := piv_error_type;
    g_error_tab(g_err_indx).ERROR_CODE := piv_error_code;
    g_error_tab(g_err_indx).error_message := piv_error_message;
    IF MOD(g_err_indx, g_err_lmt) = 0 THEN
      xxetn_common_error_pkg.add_error(pov_return_status   => l_return_status,
                                       pov_error_msg       => l_error_message,
                                       pi_source_tab       => g_error_tab,
                                       pin_batch_id        => g_new_batch_id,
                                       pin_run_sequence_id => g_new_run_seq_id);
      g_error_tab.DELETE;
      g_err_indx        := 0;
      pov_return_status := l_return_status;
      pov_error_msg     := l_error_message;
    END IF;
    print_log_message('p_err_msg:' || piv_error_message);
  EXCEPTION
    WHEN OTHERS THEN
      xxetn_debug_pkg.add_debug('Error: Exception occured in log_errors procedure ' ||
                                SUBSTR(SQLERRM, 1, 150));
  END log_errors;

  --
  -- ========================
  -- Procedure: update_status
  -- =============================================================================
  --   This procedure is used to update staging table with appropriate status
  -- =============================================================================
  PROCEDURE update_status(pin_interface_txn_id IN NUMBER,
                          piv_process_flag     IN VARCHAR2 DEFAULT NULL,
                          piv_err_type         IN VARCHAR2,
                          pov_return_status    OUT VARCHAR2,
                          pov_error_code       OUT VARCHAR2,
                          pov_error_message    OUT VARCHAR2) IS
    PRAGMA AUTONOMOUS_TRANSACTION;
    l_log_ret_status VARCHAR2(50);
    l_log_err_msg    VARCHAR2(2000);
  BEGIN
    print_log_message('update_status procedure');
    UPDATE xxar_invoices_stg
       SET process_flag      = NVL(piv_process_flag, process_flag),
           run_sequence_id   = g_new_run_seq_id,
           ERROR_TYPE        = piv_err_type,
           last_update_date  = g_sysdate,
           last_updated_by   = g_last_updated_by,
           last_update_login = g_login_id,
           request_id        = g_request_id -- Added for v1.48,Defect# 9376
     WHERE interface_txn_id = pin_interface_txn_id;
    COMMIT;
    pov_return_status := fnd_api.g_ret_sts_success;
  EXCEPTION
    WHEN OTHERS THEN
      pov_return_status := fnd_api.g_ret_sts_error;
      pov_error_code    := 'ETN_AR_UPDATE_STATUS_ERROR';
      pov_error_message := 'Error : Error updating staging table for entity ' ||
                           ' , record ' || pin_interface_txn_id ||
                           SUBSTR(SQLERRM, 1, 150);
      print_log_message('Error in update_status' || pov_error_message);
      g_retcode := 2;
      log_errors(pin_transaction_id      => pin_interface_txn_id,
                 piv_source_column_name  => 'PROCESS_FLAG',
                 piv_source_column_value => piv_process_flag,
                 piv_error_type          => piv_err_type,
                 piv_error_code          => pov_error_code,
                 piv_error_message       => pov_error_message,
                 pov_return_status       => l_log_ret_status,
                 pov_error_msg           => l_log_err_msg);
  END update_status;

  --
  -- ========================
  -- Procedure: UPDATE_DIST_STATUS
  -- =============================================================================
  --   This procedure is used to update distribution staging table with appropriate status
  -- =============================================================================
  PROCEDURE update_dist_status(pin_interface_txn_id IN NUMBER,
                               piv_process_flag     IN VARCHAR2 DEFAULT NULL,
                               piv_err_type         IN VARCHAR2,
                               pov_return_status    OUT VARCHAR2,
                               pov_error_code       OUT VARCHAR2,
                               pov_error_message    OUT VARCHAR2) IS
    PRAGMA AUTONOMOUS_TRANSACTION;
    l_log_ret_status VARCHAR2(50);
    l_log_err_msg    VARCHAR2(2000);
  BEGIN
    print_log_message('update_dist_status procedure');
    UPDATE xxar_invoices_dist_stg
       SET process_flag      = NVL(piv_process_flag, process_flag),
           ERROR_TYPE        = piv_err_type,
           run_sequence_id   = g_new_run_seq_id,
           last_update_date  = g_sysdate,
           last_updated_by   = g_last_updated_by,
           last_update_login = g_login_id,
           request_id        = g_request_id -- Added for v1.48, Defect# 9376
     WHERE interface_txn_id = pin_interface_txn_id;
    COMMIT;
    pov_return_status := fnd_api.g_ret_sts_success;
  EXCEPTION
    WHEN OTHERS THEN
      pov_return_status := fnd_api.g_ret_sts_error;
      pov_error_code    := 'ETN_AR_UPDATE_STATUS_ERROR';
      pov_error_message := 'Error : Error updating staging table for entity ' ||
                           ' , record ' || pin_interface_txn_id ||
                           SUBSTR(SQLERRM, 1, 150);
      print_log_message('Error in UPDATE_DIST_STATUS' || pov_error_message ||
                        'Error Message' || pov_error_code);
      g_retcode := 2;
      log_errors(pin_transaction_id      => pin_interface_txn_id,
                 piv_source_column_name  => 'PROCESS_FLAG',
                 piv_source_column_value => piv_process_flag,
                 piv_error_type          => piv_err_type,
                 piv_error_code          => pov_error_code,
                 piv_error_message       => pov_error_message,
                 piv_source_table        => 'XXAR_INVOICES_DIST_STG',
                 pov_return_status       => l_log_ret_status,
                 pov_error_msg           => l_log_err_msg);
  END update_dist_status;

  --
  -- ========================
  -- Procedure: PRINT_STATS_P
  -- =============================================================================
  --   This procedure is used to print statistics after end of validate,
  --   conversion and reconcile mode
  -- =============================================================================
  PROCEDURE print_stats_p IS
    l_tot_inv  NUMBER := 0;
    l_err_inv  NUMBER := 0;
    l_val_inv  NUMBER := 0;
    l_int_inv  NUMBER := 0;
    l_conv_inv NUMBER := 0;
  BEGIN
    fnd_file.put_line(fnd_file.output,
                      'Program Name : Eaton AR Invoices Conversion Program');
    fnd_file.put_line(fnd_file.output,
                      'Request ID   : ' || TO_CHAR(g_request_id));
    fnd_file.put_line(fnd_file.output,
                      'Report Date  : ' ||
                      TO_CHAR(g_sysdate, 'DD-MON-RRRR HH24:MI:SS'));
    fnd_file.put_line(fnd_file.output,
                      '=============================================================================================');
    fnd_file.put_line(fnd_file.output, CHR(10));
    fnd_file.put_line(fnd_file.output, 'Parameters');
    fnd_file.put_line(fnd_file.output,
                      '---------------------------------------------');
    fnd_file.put_line(fnd_file.output, 'Run Mode        : ' || g_run_mode);
    fnd_file.put_line(fnd_file.output, 'Batch ID        : ' || g_batch_id);
    fnd_file.put_line(fnd_file.output,
                      'Reprocess records    : ' || g_process_records);
    fnd_file.put_line(fnd_file.output,
                      'Operating Unit    : ' || g_leg_operating_unit);
    fnd_file.put_line(fnd_file.output,
                      'Transaction Type    : ' || g_leg_trasaction_type);
    fnd_file.put_line(fnd_file.output, 'GL Date        : ' || g_gl_date);
    fnd_file.put_line(fnd_file.output, CHR(10));
    fnd_file.put_line(fnd_file.output,
                      '=============================================================================================');
    fnd_file.put_line(fnd_file.output,
                      'Statistics (' || g_run_mode || '):');
    fnd_file.put_line(fnd_file.output,
                      '=============================================================================================');
    IF NVL(g_total_count, 0) > 0 OR NVL(g_total_dist_count, 0) > 0 THEN
      fnd_file.put_line(fnd_file.output, 'Invoices Lines');
      fnd_file.put_line(fnd_file.output,
                        '----------------------------------------------------------');
      fnd_file.put_line(fnd_file.output,
                        'Records Submitted                   : ' ||
                        g_total_count);
      fnd_file.put_line(fnd_file.output,
                        'Records Loaded                      : ' ||
                        g_loaded_count);
      fnd_file.put_line(fnd_file.output,
                        'Records Errored                     : ' ||
                        g_failed_count);
      fnd_file.put_line(fnd_file.output, CHR(10));
      fnd_file.put_line(fnd_file.output, 'Distribution Lines');
      fnd_file.put_line(fnd_file.output,
                        '-----------------------------------------------------------');
      fnd_file.put_line(fnd_file.output,
                        'Records Submitted                   : ' ||
                        g_total_dist_count);
      fnd_file.put_line(fnd_file.output,
                        'Records Loaded                      : ' ||
                        g_loaded_dist_count);
      fnd_file.put_line(fnd_file.output,
                        'Records Errored                     : ' ||
                        g_failed_dist_count);
      --ver1.39.1 changes start
      fnd_file.put_line(fnd_file.output, CHR(10));
      fnd_file.put_line(fnd_file.output, 'Payment Schedule Lines');
      fnd_file.put_line(fnd_file.output,
                        '-----------------------------------------------------------');
      fnd_file.put_line(fnd_file.output,
                        'Records Submitted                   : ' ||
                        g_total_pmt_sch_count);
      fnd_file.put_line(fnd_file.output,
                        'Records Loaded                      : ' ||
                        g_loaded_pmt_sch_count);
      fnd_file.put_line(fnd_file.output,
                        'Records Errored                     : ' ||
                        g_failed_pmt_sch_count);
      --ver1.39.1 changes end
    ELSE
      SELECT COUNT(1)
        INTO l_tot_inv
        FROM xxar_invoices_stg xis
       WHERE xis.batch_id = NVL(g_new_batch_id, xis.batch_id)
         AND xis.run_sequence_id =
             NVL(g_new_run_seq_id, xis.run_sequence_id);
      SELECT COUNT(1)
        INTO l_err_inv
        FROM xxar_invoices_stg xis
       WHERE xis.batch_id = NVL(g_new_batch_id, xis.batch_id)
         AND xis.run_sequence_id =
             NVL(g_new_run_seq_id, xis.run_sequence_id)
         AND xis.process_flag = 'E';
      SELECT COUNT(1)
        INTO l_val_inv
        FROM xxar_invoices_stg xis
       WHERE xis.batch_id = NVL(g_new_batch_id, xis.batch_id)
         AND xis.run_sequence_id =
             NVL(g_new_run_seq_id, xis.run_sequence_id)
         AND xis.process_flag = 'V';
      SELECT COUNT(1)
        INTO l_int_inv
        FROM xxar_invoices_stg xis
       WHERE xis.batch_id = NVL(g_new_batch_id, xis.batch_id)
         AND xis.run_sequence_id =
             NVL(g_new_run_seq_id, xis.run_sequence_id)
         AND xis.process_flag = 'P';
      SELECT COUNT(1)
        INTO l_conv_inv
        FROM xxar_invoices_stg xis
       WHERE xis.batch_id = NVL(g_new_batch_id, xis.batch_id)
         AND xis.run_sequence_id =
             NVL(g_new_run_seq_id, xis.run_sequence_id)
         AND xis.process_flag = 'C';
      fnd_file.put_line(fnd_file.output, 'For Invoice Lines:');
      fnd_file.put_line(fnd_file.output,
                        '----------------------------------------------------------');
      fnd_file.put_line(fnd_file.output,
                        'Records Submitted                     : ' ||
                        l_tot_inv);
      fnd_file.put_line(fnd_file.output,
                        'Records Validated                     : ' ||
                        l_val_inv);
      fnd_file.put_line(fnd_file.output,
                        'Records Errored                       : ' ||
                        l_err_inv);
      fnd_file.put_line(fnd_file.output,
                        'Records Interfaced                    : ' ||
                        l_int_inv);
      fnd_file.put_line(fnd_file.output,
                        'Records Completed                     : ' ||
                        l_conv_inv);
      fnd_file.put_line(fnd_file.output, ' ');
      l_tot_inv  := 0;
      l_val_inv  := 0;
      l_err_inv  := 0;
      l_int_inv  := 0;
      l_conv_inv := 0;
      SELECT COUNT(1)
        INTO l_tot_inv
        FROM xxar_invoices_dist_stg xis
       WHERE xis.batch_id = NVL(g_new_batch_id, xis.batch_id)
         AND xis.run_sequence_id =
             NVL(g_new_run_seq_id, xis.run_sequence_id)
         AND NVL(leg_account_class, 'A') <> 'ROUND'
         AND NVL(leg_account_class, 'A') <> 'UNEARN';
      SELECT COUNT(1)
        INTO l_err_inv
        FROM xxar_invoices_dist_stg xis
       WHERE xis.batch_id = NVL(g_new_batch_id, xis.batch_id)
         AND xis.run_sequence_id =
             NVL(g_new_run_seq_id, xis.run_sequence_id)
         AND xis.process_flag = 'E'
         AND NVL(leg_account_class, 'A') <> 'ROUND'
         AND NVL(leg_account_class, 'A') <> 'UNEARN';
      SELECT COUNT(1)
        INTO l_val_inv
        FROM xxar_invoices_dist_stg xis
       WHERE xis.batch_id = NVL(g_new_batch_id, xis.batch_id)
         AND xis.run_sequence_id =
             NVL(g_new_run_seq_id, xis.run_sequence_id)
         AND xis.process_flag = 'V'
         AND NVL(leg_account_class, 'A') <> 'ROUND'
         AND NVL(leg_account_class, 'A') <> 'UNEARN';
      SELECT COUNT(1)
        INTO l_int_inv
        FROM xxar_invoices_dist_stg xis
       WHERE xis.batch_id = NVL(g_new_batch_id, xis.batch_id)
         AND xis.run_sequence_id =
             NVL(g_new_run_seq_id, xis.run_sequence_id)
         AND xis.process_flag = 'P'
         AND NVL(leg_account_class, 'A') <> 'ROUND'
         AND NVL(leg_account_class, 'A') <> 'UNEARN';
      SELECT COUNT(1)
        INTO l_conv_inv
        FROM xxar_invoices_dist_stg xis
       WHERE xis.batch_id = NVL(g_new_batch_id, xis.batch_id)
         AND xis.run_sequence_id =
             NVL(g_new_run_seq_id, xis.run_sequence_id)
         AND xis.process_flag = 'C'
         AND NVL(leg_account_class, 'A') <> 'ROUND'
         AND NVL(leg_account_class, 'A') <> 'UNEARN';
      fnd_file.put_line(fnd_file.output, 'For Distribution Lines:');
      fnd_file.put_line(fnd_file.output,
                        '----------------------------------------------------------');
      fnd_file.put_line(fnd_file.output,
                        'Records Submitted                     : ' ||
                        l_tot_inv);
      fnd_file.put_line(fnd_file.output,
                        'Records Validated                     : ' ||
                        l_val_inv);
      fnd_file.put_line(fnd_file.output,
                        'Records Errored                       : ' ||
                        l_err_inv);
      fnd_file.put_line(fnd_file.output,
                        'Records Interfaced                    : ' ||
                        l_int_inv);
      fnd_file.put_line(fnd_file.output,
                        'Records Completed                     : ' ||
                        l_conv_inv);
    END IF;
    fnd_file.put_line(fnd_file.output, CHR(10));
    fnd_file.put_line(fnd_file.output,
                      '===================================================================================================');
  EXCEPTION
    WHEN OTHERS THEN
      --NULL;
      -- Added below for v1.44
      fnd_file.put_line(fnd_file.LOG,
                        'Error in Proc:print_stats_p: ' || SQLERRM);
  END print_stats_p;

  --
  -- ========================
  -- Procedure: LOAD_INVOICE
  -- =============================================================================
  --   This procedure is used to load invoice lines data from extraction staging table
  --   to conversion staging table when program is run in LOAD mode
  -- =============================================================================
  PROCEDURE load_invoice(pov_ret_stats OUT NOCOPY VARCHAR2,
                         pov_err_msg   OUT NOCOPY VARCHAR2) IS
    /*   TYPE leg_invoice_tbl IS TABLE OF xxar_invoices_ext_r12%ROWTYPE
            INDEX BY BINARY_INTEGER;

       l_leg_invoice_tbl    leg_invoice_tbl;
    */
    TYPE leg_invoice_rec IS RECORD(
      interface_txn_id               xxar_invoices_ext_r12.interface_txn_id%TYPE,
      batch_id                       xxar_invoices_ext_r12.batch_id%TYPE,
      load_id                        xxar_invoices_ext_r12.load_id%TYPE,
      run_sequence_id                xxar_invoices_ext_r12.run_sequence_id%TYPE,
      leg_batch_source_name          xxar_invoices_ext_r12.leg_batch_source_name%TYPE,
      leg_customer_number            xxar_invoices_ext_r12.leg_customer_number%TYPE,
      leg_bill_to_address            xxar_invoices_ext_r12.leg_bill_to_address%TYPE,
      leg_ship_to_address            xxar_invoices_ext_r12.leg_ship_to_address%TYPE,
      leg_currency_code              xxar_invoices_ext_r12.leg_currency_code%TYPE,
      leg_cust_trx_type_name         xxar_invoices_ext_r12.leg_cust_trx_type_name%TYPE,
      leg_line_amount                xxar_invoices_ext_r12.leg_line_amount%TYPE,
      leg_trx_date                   xxar_invoices_ext_r12.leg_trx_date%TYPE,
      leg_tax_code                   xxar_invoices_ext_r12.leg_tax_code%TYPE,
      leg_tax_rate                   xxar_invoices_ext_r12.leg_tax_rate%TYPE,
      leg_conversion_date            xxar_invoices_ext_r12.leg_conversion_date%TYPE,
      leg_conversion_rate            xxar_invoices_ext_r12.leg_conversion_rate%TYPE,
      leg_term_name                  xxar_invoices_ext_r12.leg_term_name%TYPE,
      leg_set_of_books_name          xxar_invoices_ext_r12.leg_set_of_books_name%TYPE,
      leg_operating_unit             xxar_invoices_ext_r12.leg_operating_unit%TYPE,
      leg_header_attribute_category  xxar_invoices_ext_r12.leg_header_attribute_category%TYPE,
      leg_header_attribute1          xxar_invoices_ext_r12.leg_header_attribute1%TYPE,
      leg_header_attribute2          xxar_invoices_ext_r12.leg_header_attribute2%TYPE,
      leg_header_attribute3          xxar_invoices_ext_r12.leg_header_attribute3%TYPE,
      leg_header_attribute4          xxar_invoices_ext_r12.leg_header_attribute4%TYPE,
      leg_header_attribute5          xxar_invoices_ext_r12.leg_header_attribute5%TYPE,
      leg_header_attribute6          xxar_invoices_ext_r12.leg_header_attribute6%TYPE,
      leg_header_attribute7          xxar_invoices_ext_r12.leg_header_attribute7%TYPE,
      leg_header_attribute8          xxar_invoices_ext_r12.leg_header_attribute8%TYPE,
      leg_header_attribute9          xxar_invoices_ext_r12.leg_header_attribute9%TYPE,
      leg_header_attribute10         xxar_invoices_ext_r12.leg_header_attribute10%TYPE,
      leg_header_attribute11         xxar_invoices_ext_r12.leg_header_attribute11%TYPE,
      leg_header_attribute12         xxar_invoices_ext_r12.leg_header_attribute12%TYPE,
      leg_header_attribute13         xxar_invoices_ext_r12.leg_header_attribute13%TYPE,
      leg_header_attribute14         xxar_invoices_ext_r12.leg_header_attribute14%TYPE,
      leg_header_attribute15         xxar_invoices_ext_r12.leg_header_attribute15%TYPE,
      leg_reference_line_id          xxar_invoices_ext_r12.leg_reference_line_id%TYPE,
      leg_purchase_order             xxar_invoices_ext_r12.leg_purchase_order%TYPE,
      leg_trx_number                 xxar_invoices_ext_r12.leg_trx_number%TYPE,
      leg_line_number                xxar_invoices_ext_r12.leg_line_number%TYPE,
      leg_comments                   xxar_invoices_ext_r12.leg_comments%TYPE,
      leg_due_date                   xxar_invoices_ext_r12.leg_due_date%TYPE,
      leg_inv_amount_due_original    xxar_invoices_ext_r12.leg_inv_amount_due_original%TYPE,
      leg_inv_amount_due_remaining   xxar_invoices_ext_r12.leg_inv_amount_due_remaining%TYPE,
      leg_line_type                  xxar_invoices_ext_r12.leg_line_type%TYPE,
      leg_interface_line_context     xxar_invoices_ext_r12.leg_interface_line_context%TYPE,
      leg_interface_line_attribute1  xxar_invoices_ext_r12.leg_interface_line_attribute1%TYPE,
      leg_interface_line_attribute2  xxar_invoices_ext_r12.leg_interface_line_attribute2%TYPE,
      leg_interface_line_attribute3  xxar_invoices_ext_r12.leg_interface_line_attribute3%TYPE,
      leg_interface_line_attribute4  xxar_invoices_ext_r12.leg_interface_line_attribute4%TYPE,
      leg_interface_line_attribute5  xxar_invoices_ext_r12.leg_interface_line_attribute5%TYPE,
      leg_interface_line_attribute6  xxar_invoices_ext_r12.leg_interface_line_attribute6%TYPE,
      leg_interface_line_attribute7  xxar_invoices_ext_r12.leg_interface_line_attribute7%TYPE,
      leg_interface_line_attribute8  xxar_invoices_ext_r12.leg_interface_line_attribute8%TYPE,
      leg_interface_line_attribute9  xxar_invoices_ext_r12.leg_interface_line_attribute9%TYPE,
      leg_interface_line_attribute10 xxar_invoices_ext_r12.leg_interface_line_attribute10%TYPE,
      leg_interface_line_attribute11 xxar_invoices_ext_r12.leg_interface_line_attribute11%TYPE,
      leg_interface_line_attribute12 xxar_invoices_ext_r12.leg_interface_line_attribute12%TYPE,
      leg_interface_line_attribute13 xxar_invoices_ext_r12.leg_interface_line_attribute13%TYPE,
      leg_interface_line_attribute14 xxar_invoices_ext_r12.leg_interface_line_attribute14%TYPE,
      leg_interface_line_attribute15 xxar_invoices_ext_r12.leg_interface_line_attribute15%TYPE,
      leg_customer_trx_id            xxar_invoices_ext_r12.leg_customer_trx_id%TYPE,
      trx_type                       xxar_invoices_ext_r12.trx_type%TYPE,
      leg_cust_trx_line_id           xxar_invoices_ext_r12.leg_cust_trx_line_id%TYPE,
      leg_link_to_cust_trx_line_id   xxar_invoices_ext_r12.leg_link_to_cust_trx_line_id%TYPE,
      leg_header_gdf_attr_category   xxar_invoices_ext_r12.leg_header_gdf_attr_category%TYPE,
      leg_header_gdf_attribute1      xxar_invoices_ext_r12.leg_header_gdf_attribute1%TYPE,
      leg_header_gdf_attribute2      xxar_invoices_ext_r12.leg_header_gdf_attribute2%TYPE,
      leg_header_gdf_attribute3      xxar_invoices_ext_r12.leg_header_gdf_attribute3%TYPE,
      leg_header_gdf_attribute4      xxar_invoices_ext_r12.leg_header_gdf_attribute4%TYPE,
      leg_header_gdf_attribute5      xxar_invoices_ext_r12.leg_header_gdf_attribute5%TYPE,
      leg_header_gdf_attribute6      xxar_invoices_ext_r12.leg_header_gdf_attribute6%TYPE,
      leg_header_gdf_attribute7      xxar_invoices_ext_r12.leg_header_gdf_attribute7%TYPE,
      leg_header_gdf_attribute8      xxar_invoices_ext_r12.leg_header_gdf_attribute8%TYPE,
      leg_header_gdf_attribute9      xxar_invoices_ext_r12.leg_header_gdf_attribute9%TYPE,
      leg_header_gdf_attribute10     xxar_invoices_ext_r12.leg_header_gdf_attribute10%TYPE,
      leg_header_gdf_attribute11     xxar_invoices_ext_r12.leg_header_gdf_attribute11%TYPE,
      leg_header_gdf_attribute12     xxar_invoices_ext_r12.leg_header_gdf_attribute12%TYPE,
      leg_header_gdf_attribute13     xxar_invoices_ext_r12.leg_header_gdf_attribute13%TYPE,
      leg_header_gdf_attribute14     xxar_invoices_ext_r12.leg_header_gdf_attribute14%TYPE,
      leg_header_gdf_attribute15     xxar_invoices_ext_r12.leg_header_gdf_attribute15%TYPE,
      leg_header_gdf_attribute16     xxar_invoices_ext_r12.leg_header_gdf_attribute16%TYPE,
      leg_header_gdf_attribute17     xxar_invoices_ext_r12.leg_header_gdf_attribute17%TYPE,
      leg_header_gdf_attribute18     xxar_invoices_ext_r12.leg_header_gdf_attribute18%TYPE,
      leg_header_gdf_attribute19     xxar_invoices_ext_r12.leg_header_gdf_attribute19%TYPE,
      leg_header_gdf_attribute20     xxar_invoices_ext_r12.leg_header_gdf_attribute20%TYPE,
      leg_header_gdf_attribute21     xxar_invoices_ext_r12.leg_header_gdf_attribute21%TYPE,
      leg_header_gdf_attribute22     xxar_invoices_ext_r12.leg_header_gdf_attribute22%TYPE,
      leg_header_gdf_attribute23     xxar_invoices_ext_r12.leg_header_gdf_attribute23%TYPE,
      leg_header_gdf_attribute24     xxar_invoices_ext_r12.leg_header_gdf_attribute24%TYPE,
      leg_header_gdf_attribute25     xxar_invoices_ext_r12.leg_header_gdf_attribute25%TYPE,
      leg_header_gdf_attribute26     xxar_invoices_ext_r12.leg_header_gdf_attribute26%TYPE,
      leg_header_gdf_attribute27     xxar_invoices_ext_r12.leg_header_gdf_attribute27%TYPE,
      leg_header_gdf_attribute28     xxar_invoices_ext_r12.leg_header_gdf_attribute28%TYPE,
      leg_header_gdf_attribute29     xxar_invoices_ext_r12.leg_header_gdf_attribute29%TYPE,
      leg_header_gdf_attribute30     xxar_invoices_ext_r12.leg_header_gdf_attribute30%TYPE,
      leg_line_gdf_attr_category     xxar_invoices_ext_r12.leg_line_gdf_attr_category%TYPE,
      leg_line_gdf_attribute1        xxar_invoices_ext_r12.leg_line_gdf_attribute1%TYPE,
      leg_line_gdf_attribute2        xxar_invoices_ext_r12.leg_line_gdf_attribute2%TYPE,
      leg_line_gdf_attribute3        xxar_invoices_ext_r12.leg_line_gdf_attribute3%TYPE,
      leg_line_gdf_attribute4        xxar_invoices_ext_r12.leg_line_gdf_attribute4%TYPE,
      leg_line_gdf_attribute5        xxar_invoices_ext_r12.leg_line_gdf_attribute5%TYPE,
      leg_line_gdf_attribute6        xxar_invoices_ext_r12.leg_line_gdf_attribute6%TYPE,
      leg_line_gdf_attribute7        xxar_invoices_ext_r12.leg_line_gdf_attribute7%TYPE,
      leg_line_gdf_attribute8        xxar_invoices_ext_r12.leg_line_gdf_attribute8%TYPE,
      leg_line_gdf_attribute9        xxar_invoices_ext_r12.leg_line_gdf_attribute9%TYPE,
      leg_line_gdf_attribute10       xxar_invoices_ext_r12.leg_line_gdf_attribute10%TYPE,
      leg_line_gdf_attribute11       xxar_invoices_ext_r12.leg_line_gdf_attribute11%TYPE,
      leg_line_gdf_attribute12       xxar_invoices_ext_r12.leg_line_gdf_attribute12%TYPE,
      leg_line_gdf_attribute13       xxar_invoices_ext_r12.leg_line_gdf_attribute13%TYPE,
      leg_line_gdf_attribute14       xxar_invoices_ext_r12.leg_line_gdf_attribute14%TYPE,
      leg_line_gdf_attribute15       xxar_invoices_ext_r12.leg_line_gdf_attribute15%TYPE,
      leg_line_gdf_attribute16       xxar_invoices_ext_r12.leg_line_gdf_attribute16%TYPE,
      leg_line_gdf_attribute17       xxar_invoices_ext_r12.leg_line_gdf_attribute17%TYPE,
      leg_line_gdf_attribute18       xxar_invoices_ext_r12.leg_line_gdf_attribute18%TYPE,
      leg_line_gdf_attribute19       xxar_invoices_ext_r12.leg_line_gdf_attribute19%TYPE,
      leg_line_gdf_attribute20       xxar_invoices_ext_r12.leg_line_gdf_attribute20%TYPE,
      leg_reason_code                xxar_invoices_ext_r12.leg_reason_code%TYPE,
      leg_source_system              xxar_invoices_ext_r12.leg_source_system%TYPE,
      leg_quantity                   xxar_invoices_ext_r12.leg_quantity%TYPE,
      leg_quantity_ordered           xxar_invoices_ext_r12.leg_quantity_ordered%TYPE,
      leg_unit_selling_price         xxar_invoices_ext_r12.leg_unit_selling_price%TYPE,
      leg_unit_standard_price        xxar_invoices_ext_r12.leg_unit_standard_price%TYPE,
      leg_ship_date_actual           xxar_invoices_ext_r12.leg_ship_date_actual%TYPE,
      leg_fob_point                  xxar_invoices_ext_r12.leg_fob_point%TYPE,
      leg_ship_via                   xxar_invoices_ext_r12.leg_ship_via%TYPE,
      leg_waybill_number             xxar_invoices_ext_r12.leg_waybill_number%TYPE,
      leg_sales_order_line           xxar_invoices_ext_r12.leg_sales_order_line%TYPE,
      leg_sales_order                xxar_invoices_ext_r12.leg_sales_order%TYPE,
      leg_gl_date                    xxar_invoices_ext_r12.leg_gl_date%TYPE,
      leg_sales_order_date           xxar_invoices_ext_r12.leg_sales_order_date%TYPE,
      leg_sales_order_source         xxar_invoices_ext_r12.leg_sales_order_source%TYPE,
      leg_sales_order_revision       xxar_invoices_ext_r12.leg_sales_order_revision%TYPE,
      leg_purchase_order_revision    xxar_invoices_ext_r12.leg_purchase_order_revision%TYPE,
      leg_purchase_order_date        xxar_invoices_ext_r12.leg_purchase_order_date%TYPE,
      leg_agreement_name             xxar_invoices_ext_r12.leg_agreement_name%TYPE,
      leg_agreement_id               xxar_invoices_ext_r12.leg_agreement_id%TYPE,
      leg_memo_line_name             xxar_invoices_ext_r12.leg_memo_line_name%TYPE,
      leg_internal_notes             xxar_invoices_ext_r12.leg_internal_notes%TYPE,
      leg_ussgl_trx_code_context     xxar_invoices_ext_r12.leg_ussgl_trx_code_context%TYPE,
      leg_uom_name                   xxar_invoices_ext_r12.leg_uom_name%TYPE,
      leg_vat_tax_name               xxar_invoices_ext_r12.leg_vat_tax_name%TYPE,
      leg_sales_tax_name             xxar_invoices_ext_r12.leg_sales_tax_name%TYPE,
      leg_request_id                 xxar_invoices_ext_r12.leg_request_id%TYPE,
      leg_seq_num                    xxar_invoices_ext_r12.leg_seq_num%TYPE,
      leg_process_flag               xxar_invoices_ext_r12.leg_process_flag%TYPE,
      currency_code                  xxar_invoices_ext_r12.currency_code%TYPE,
      cust_trx_type_name             xxar_invoices_ext_r12.cust_trx_type_name%TYPE,
      line_type                      xxar_invoices_ext_r12.line_type%TYPE,
      set_of_books_id                xxar_invoices_ext_r12.set_of_books_id%TYPE,
      trx_number                     xxar_invoices_ext_r12.trx_number%TYPE,
      line_number                    xxar_invoices_ext_r12.line_number%TYPE,
      gl_date                        xxar_invoices_ext_r12.gl_date%TYPE,
      memo_line_id                   NUMBER,
      description                    xxar_invoices_ext_r12.description%TYPE,
      header_attribute_category      xxar_invoices_ext_r12.header_attribute_category%TYPE,
      header_attribute1              xxar_invoices_ext_r12.header_attribute1%TYPE,
      header_attribute2              xxar_invoices_ext_r12.header_attribute2%TYPE,
      header_attribute3              xxar_invoices_ext_r12.header_attribute3%TYPE,
      header_attribute4              xxar_invoices_ext_r12.header_attribute4%TYPE,
      header_attribute5              xxar_invoices_ext_r12.header_attribute5%TYPE,
      header_attribute6              xxar_invoices_ext_r12.header_attribute6%TYPE,
      header_attribute7              xxar_invoices_ext_r12.header_attribute7%TYPE,
      header_attribute8              xxar_invoices_ext_r12.header_attribute8%TYPE,
      header_attribute9              xxar_invoices_ext_r12.header_attribute9%TYPE,
      header_attribute10             xxar_invoices_ext_r12.header_attribute10%TYPE,
      header_attribute11             xxar_invoices_ext_r12.header_attribute11%TYPE,
      header_attribute12             xxar_invoices_ext_r12.header_attribute12%TYPE,
      header_attribute13             xxar_invoices_ext_r12.header_attribute13%TYPE,
      header_attribute14             xxar_invoices_ext_r12.header_attribute14%TYPE,
      header_attribute15             xxar_invoices_ext_r12.header_attribute15%TYPE,
      interface_line_context         xxar_invoices_ext_r12.interface_line_context%TYPE,
      interface_line_attribute1      xxar_invoices_ext_r12.interface_line_attribute1%TYPE,
      interface_line_attribute2      xxar_invoices_ext_r12.interface_line_attribute2%TYPE,
      interface_line_attribute3      xxar_invoices_ext_r12.interface_line_attribute3%TYPE,
      interface_line_attribute4      xxar_invoices_ext_r12.interface_line_attribute4%TYPE,
      interface_line_attribute5      xxar_invoices_ext_r12.interface_line_attribute5%TYPE,
      interface_line_attribute6      xxar_invoices_ext_r12.interface_line_attribute6%TYPE,
      interface_line_attribute7      xxar_invoices_ext_r12.interface_line_attribute7%TYPE,
      interface_line_attribute8      xxar_invoices_ext_r12.interface_line_attribute8%TYPE,
      interface_line_attribute9      xxar_invoices_ext_r12.interface_line_attribute9%TYPE,
      interface_line_attribute10     xxar_invoices_ext_r12.interface_line_attribute10%TYPE,
      interface_line_attribute11     xxar_invoices_ext_r12.interface_line_attribute11%TYPE,
      interface_line_attribute12     xxar_invoices_ext_r12.interface_line_attribute12%TYPE,
      interface_line_attribute13     xxar_invoices_ext_r12.interface_line_attribute13%TYPE,
      interface_line_attribute14     xxar_invoices_ext_r12.interface_line_attribute14%TYPE,
      interface_line_attribute15     xxar_invoices_ext_r12.interface_line_attribute15%TYPE,
      system_bill_customer_id        xxar_invoices_ext_r12.system_bill_customer_id%TYPE,
      system_bill_customer_ref       xxar_invoices_ext_r12.system_bill_customer_ref%TYPE,
      system_bill_address_id         xxar_invoices_ext_r12.system_bill_address_id%TYPE,
      system_bill_address_ref        xxar_invoices_ext_r12.system_bill_address_ref%TYPE,
      system_bill_contact_id         xxar_invoices_ext_r12.system_bill_contact_id%TYPE,
      system_ship_customer_id        xxar_invoices_ext_r12.system_ship_customer_id%TYPE,
      system_ship_customer_ref       xxar_invoices_ext_r12.system_ship_customer_ref%TYPE,
      system_ship_address_id         xxar_invoices_ext_r12.system_ship_address_id%TYPE,
      system_ship_address_ref        xxar_invoices_ext_r12.system_ship_address_ref%TYPE,
      system_ship_contact_id         xxar_invoices_ext_r12.system_ship_contact_id%TYPE,
      system_sold_customer_id        xxar_invoices_ext_r12.system_sold_customer_id%TYPE,
      system_sold_customer_ref       xxar_invoices_ext_r12.system_sold_customer_ref%TYPE,
      term_name                      xxar_invoices_ext_r12.term_name%TYPE,
      ou_name                        xxar_invoices_ext_r12.ou_name%TYPE,
      conversion_type                xxar_invoices_ext_r12.conversion_type%TYPE,
      conversion_date                xxar_invoices_ext_r12.conversion_date%TYPE,
      conversion_rate                xxar_invoices_ext_r12.conversion_rate%TYPE,
      trx_date                       xxar_invoices_ext_r12.trx_date%TYPE,
      batch_source_name              xxar_invoices_ext_r12.batch_source_name%TYPE,
      purchase_order                 xxar_invoices_ext_r12.purchase_order%TYPE,
      sales_order_date               xxar_invoices_ext_r12.sales_order_date%TYPE,
      sales_order                    xxar_invoices_ext_r12.sales_order%TYPE,
      reference_line_id              NUMBER,
      term_id                        xxar_invoices_ext_r12.term_id%TYPE,
      org_id                         xxar_invoices_ext_r12.org_id%TYPE,
      transaction_type_id            xxar_invoices_ext_r12.transaction_type_id%TYPE,
      tax_regime_code                xxar_invoices_ext_r12.tax_regime_code%TYPE,
      tax_code                       xxar_invoices_ext_r12.tax_code%TYPE,
      tax                            xxar_invoices_ext_r12.tax%TYPE,
      tax_status_code                xxar_invoices_ext_r12.tax_status_code%TYPE,
      tax_rate_code                  xxar_invoices_ext_r12.tax_rate_code%TYPE,
      tax_jurisdiction_code          xxar_invoices_ext_r12.tax_jurisdiction_code%TYPE,
      tax_rate                       xxar_invoices_ext_r12.tax_rate%TYPE,
      adjustment_amount              xxar_invoices_ext_r12.adjustment_amount%TYPE,
      inv_amount_due_original        xxar_invoices_ext_r12.inv_amount_due_original%TYPE,
      inv_amount_due_remaining       xxar_invoices_ext_r12.inv_amount_due_remaining%TYPE,
      link_to_line_context           xxar_invoices_ext_r12.link_to_line_context%TYPE,
      link_to_line_attribute1        xxar_invoices_ext_r12.link_to_line_attribute1%TYPE,
      link_to_line_attribute2        xxar_invoices_ext_r12.link_to_line_attribute2%TYPE,
      link_to_line_attribute3        xxar_invoices_ext_r12.link_to_line_attribute3%TYPE,
      link_to_line_attribute4        xxar_invoices_ext_r12.link_to_line_attribute4%TYPE,
      link_to_line_attribute5        xxar_invoices_ext_r12.link_to_line_attribute5%TYPE,
      link_to_line_attribute6        xxar_invoices_ext_r12.link_to_line_attribute6%TYPE,
      link_to_line_attribute7        xxar_invoices_ext_r12.link_to_line_attribute7%TYPE,
      link_to_line_attribute8        xxar_invoices_ext_r12.link_to_line_attribute8%TYPE,
      link_to_line_attribute9        xxar_invoices_ext_r12.link_to_line_attribute9%TYPE,
      link_to_line_attribute10       xxar_invoices_ext_r12.link_to_line_attribute10%TYPE,
      link_to_line_attribute11       xxar_invoices_ext_r12.link_to_line_attribute11%TYPE,
      link_to_line_attribute12       xxar_invoices_ext_r12.link_to_line_attribute12%TYPE,
      link_to_line_attribute13       xxar_invoices_ext_r12.link_to_line_attribute13%TYPE,
      link_to_line_attribute14       xxar_invoices_ext_r12.link_to_line_attribute14%TYPE,
      link_to_line_attribute15       xxar_invoices_ext_r12.link_to_line_attribute15%TYPE,
      header_gdf_attr_category       xxar_invoices_ext_r12.header_gdf_attr_category%TYPE,
      header_gdf_attribute1          xxar_invoices_ext_r12.header_gdf_attribute1%TYPE,
      header_gdf_attribute2          xxar_invoices_ext_r12.header_gdf_attribute2%TYPE,
      header_gdf_attribute3          xxar_invoices_ext_r12.header_gdf_attribute3%TYPE,
      header_gdf_attribute4          xxar_invoices_ext_r12.header_gdf_attribute4%TYPE,
      header_gdf_attribute5          xxar_invoices_ext_r12.header_gdf_attribute5%TYPE,
      header_gdf_attribute6          xxar_invoices_ext_r12.header_gdf_attribute6%TYPE,
      header_gdf_attribute7          xxar_invoices_ext_r12.header_gdf_attribute7%TYPE,
      header_gdf_attribute8          xxar_invoices_ext_r12.header_gdf_attribute8%TYPE,
      header_gdf_attribute9          xxar_invoices_ext_r12.header_gdf_attribute9%TYPE,
      header_gdf_attribute10         xxar_invoices_ext_r12.header_gdf_attribute10%TYPE,
      header_gdf_attribute11         xxar_invoices_ext_r12.header_gdf_attribute11%TYPE,
      header_gdf_attribute12         xxar_invoices_ext_r12.header_gdf_attribute12%TYPE,
      header_gdf_attribute13         xxar_invoices_ext_r12.header_gdf_attribute13%TYPE,
      header_gdf_attribute14         xxar_invoices_ext_r12.header_gdf_attribute14%TYPE,
      header_gdf_attribute15         xxar_invoices_ext_r12.header_gdf_attribute15%TYPE,
      header_gdf_attribute16         xxar_invoices_ext_r12.header_gdf_attribute16%TYPE,
      header_gdf_attribute17         xxar_invoices_ext_r12.header_gdf_attribute17%TYPE,
      header_gdf_attribute18         xxar_invoices_ext_r12.header_gdf_attribute18%TYPE,
      header_gdf_attribute19         xxar_invoices_ext_r12.header_gdf_attribute19%TYPE,
      header_gdf_attribute20         xxar_invoices_ext_r12.header_gdf_attribute20%TYPE,
      header_gdf_attribute21         xxar_invoices_ext_r12.header_gdf_attribute21%TYPE,
      header_gdf_attribute22         xxar_invoices_ext_r12.header_gdf_attribute22%TYPE,
      header_gdf_attribute23         xxar_invoices_ext_r12.header_gdf_attribute23%TYPE,
      header_gdf_attribute24         xxar_invoices_ext_r12.header_gdf_attribute24%TYPE,
      header_gdf_attribute25         xxar_invoices_ext_r12.header_gdf_attribute25%TYPE,
      header_gdf_attribute26         xxar_invoices_ext_r12.header_gdf_attribute26%TYPE,
      header_gdf_attribute27         xxar_invoices_ext_r12.header_gdf_attribute27%TYPE,
      header_gdf_attribute28         xxar_invoices_ext_r12.header_gdf_attribute28%TYPE,
      header_gdf_attribute29         xxar_invoices_ext_r12.header_gdf_attribute29%TYPE,
      header_gdf_attribute30         xxar_invoices_ext_r12.header_gdf_attribute30%TYPE,
      line_gdf_attr_category         xxar_invoices_ext_r12.line_gdf_attr_category%TYPE,
      line_gdf_attribute1            xxar_invoices_ext_r12.line_gdf_attribute1%TYPE,
      line_gdf_attribute2            xxar_invoices_ext_r12.line_gdf_attribute2%TYPE,
      line_gdf_attribute3            xxar_invoices_ext_r12.line_gdf_attribute3%TYPE,
      line_gdf_attribute4            xxar_invoices_ext_r12.line_gdf_attribute4%TYPE,
      line_gdf_attribute5            xxar_invoices_ext_r12.line_gdf_attribute5%TYPE,
      line_gdf_attribute6            xxar_invoices_ext_r12.line_gdf_attribute6%TYPE,
      line_gdf_attribute7            xxar_invoices_ext_r12.line_gdf_attribute7%TYPE,
      line_gdf_attribute8            xxar_invoices_ext_r12.line_gdf_attribute8%TYPE,
      line_gdf_attribute9            xxar_invoices_ext_r12.line_gdf_attribute9%TYPE,
      line_gdf_attribute10           xxar_invoices_ext_r12.line_gdf_attribute10%TYPE,
      line_gdf_attribute11           xxar_invoices_ext_r12.line_gdf_attribute11%TYPE,
      line_gdf_attribute12           xxar_invoices_ext_r12.line_gdf_attribute12%TYPE,
      line_gdf_attribute13           xxar_invoices_ext_r12.line_gdf_attribute13%TYPE,
      line_gdf_attribute14           xxar_invoices_ext_r12.line_gdf_attribute14%TYPE,
      line_gdf_attribute15           xxar_invoices_ext_r12.line_gdf_attribute15%TYPE,
      line_gdf_attribute16           xxar_invoices_ext_r12.line_gdf_attribute16%TYPE,
      line_gdf_attribute17           xxar_invoices_ext_r12.line_gdf_attribute17%TYPE,
      line_gdf_attribute18           xxar_invoices_ext_r12.line_gdf_attribute18%TYPE,
      line_gdf_attribute19           xxar_invoices_ext_r12.line_gdf_attribute19%TYPE,
      line_gdf_attribute20           xxar_invoices_ext_r12.line_gdf_attribute20%TYPE,
      line_amount                    xxar_invoices_ext_r12.line_amount%TYPE,
      reason_code                    xxar_invoices_ext_r12.reason_code%TYPE,
      reason_code_meaning            xxar_invoices_ext_r12.reason_code_meaning%TYPE,
      REFERENCE                      xxar_invoices_ext_r12.REFERENCE%TYPE,
      comments                       xxar_invoices_ext_r12.comments%TYPE,
      creation_date                  xxar_invoices_ext_r12.creation_date%TYPE,
      created_by                     xxar_invoices_ext_r12.created_by%TYPE,
      last_updated_date              xxar_invoices_ext_r12.last_updated_date%TYPE,
      last_updated_by                xxar_invoices_ext_r12.last_updated_by%TYPE,
      last_update_login              xxar_invoices_ext_r12.last_update_login%TYPE,
      program_application_id         xxar_invoices_ext_r12.program_application_id%TYPE,
      program_id                     xxar_invoices_ext_r12.program_id%TYPE,
      program_update_date            xxar_invoices_ext_r12.program_update_date%TYPE,
      request_id                     xxar_invoices_ext_r12.request_id%TYPE,
      process_flag                   xxar_invoices_ext_r12.process_flag%TYPE,
      ERROR_TYPE                     xxar_invoices_ext_r12.ERROR_TYPE%TYPE,
      attribute_category             xxar_invoices_ext_r12.attribute_category%TYPE,
      attribute1                     xxar_invoices_ext_r12.attribute1%TYPE,
      attribute2                     xxar_invoices_ext_r12.attribute2%TYPE,
      attribute3                     xxar_invoices_ext_r12.attribute3%TYPE,
      attribute4                     xxar_invoices_ext_r12.attribute4%TYPE,
      attribute5                     xxar_invoices_ext_r12.attribute5%TYPE,
      attribute6                     xxar_invoices_ext_r12.attribute6%TYPE,
      attribute7                     xxar_invoices_ext_r12.attribute7%TYPE,
      attribute8                     xxar_invoices_ext_r12.attribute8%TYPE,
      attribute9                     xxar_invoices_ext_r12.attribute9%TYPE,
      attribute10                    xxar_invoices_ext_r12.attribute10%TYPE,
      attribute11                    xxar_invoices_ext_r12.attribute11%TYPE,
      attribute12                    xxar_invoices_ext_r12.attribute12%TYPE,
      attribute13                    xxar_invoices_ext_r12.attribute13%TYPE,
      attribute14                    xxar_invoices_ext_r12.attribute14%TYPE,
      attribute15                    xxar_invoices_ext_r12.attribute15%TYPE,
      vat_tax_id                     xxar_invoices_ext_r12.vat_tax_id%TYPE,
      sales_tax_id                   xxar_invoices_ext_r12.sales_tax_id%TYPE,
      uom_name                       xxar_invoices_ext_r12.uom_name%TYPE,
      ussgl_transaction_code_context xxar_invoices_ext_r12.ussgl_transaction_code_context%TYPE,
      internal_notes                 xxar_invoices_ext_r12.internal_notes%TYPE,
      ship_date_actual               xxar_invoices_ext_r12.ship_date_actual%TYPE,
      fob_point                      xxar_invoices_ext_r12.fob_point%TYPE,
      ship_via                       xxar_invoices_ext_r12.ship_via%TYPE,
      waybill_number                 xxar_invoices_ext_r12.waybill_number%TYPE,
      sales_order_line               xxar_invoices_ext_r12.sales_order_line%TYPE,
      sales_order_source             xxar_invoices_ext_r12.sales_order_source%TYPE,
      sales_order_revision           xxar_invoices_ext_r12.sales_order_revision%TYPE,
      purchase_order_revision        xxar_invoices_ext_r12.purchase_order_revision%TYPE,
      purchase_order_date            xxar_invoices_ext_r12.purchase_order_date%TYPE,
      agreement_name                 xxar_invoices_ext_r12.agreement_name%TYPE,
      agreement_id                   xxar_invoices_ext_r12.agreement_id%TYPE,
      memo_line_name                 xxar_invoices_ext_r12.memo_line_name%TYPE,
      quantity                       xxar_invoices_ext_r12.quantity%TYPE,
      quantity_ordered               xxar_invoices_ext_r12.quantity_ordered%TYPE,
      unit_selling_price             xxar_invoices_ext_r12.unit_selling_price%TYPE,
      unit_standard_price            xxar_invoices_ext_r12.unit_standard_price%TYPE,
      amount_includes_tax_flag       VARCHAR2(1),
      taxable_flag                   VARCHAR2(1),
      leg_req_id                     xxar_invoices_ext_r12.request_id%TYPE,
      func_curr                      VARCHAR2(30),
      ledger_id                      NUMBER,
      invoicing_rule_id              NUMBER,
      -- V1.28 change
      leg_warehouse_id xxar_invoices_ext_r12.leg_warehouse_id%TYPE,
      warehouse_id     xxar_invoices_ext_r12.warehouse_id%TYPE,
      --v1.36 change start
      receipt_method_id       xxar_invoices_ext_r12.receipt_method_id%TYPE,
      leg_receipt_method_name xxar_invoices_ext_r12.leg_receipt_method_name%TYPE,
      receipt_method_name     xxar_invoices_ext_r12.receipt_method_name%TYPE,
      --v1.36 change ends
      --ver.1.20.1 changes start
      leg_interface_hdr_context     xxar_invoices_ext_r12.interface_header_context%TYPE,
      leg_interface_hdr_attribute1  xxar_invoices_ext_r12.interface_header_attribute1%TYPE,
      leg_interface_hdr_attribute2  xxar_invoices_ext_r12.interface_header_attribute2%TYPE,
      leg_interface_hdr_attribute3  xxar_invoices_ext_r12.interface_header_attribute3%TYPE,
      leg_interface_hdr_attribute4  xxar_invoices_ext_r12.interface_header_attribute4%TYPE,
      leg_interface_hdr_attribute5  xxar_invoices_ext_r12.interface_header_attribute5%TYPE,
      leg_interface_hdr_attribute6  xxar_invoices_ext_r12.interface_header_attribute6%TYPE,
      leg_interface_hdr_attribute7  xxar_invoices_ext_r12.interface_header_attribute7%TYPE,
      leg_interface_hdr_attribute8  xxar_invoices_ext_r12.interface_header_attribute8%TYPE,
      leg_interface_hdr_attribute9  xxar_invoices_ext_r12.interface_header_attribute9%TYPE,
      leg_interface_hdr_attribute10 xxar_invoices_ext_r12.interface_header_attribute10%TYPE,
      leg_interface_hdr_attribute11 xxar_invoices_ext_r12.interface_header_attribute11%TYPE,
      leg_interface_hdr_attribute12 xxar_invoices_ext_r12.interface_header_attribute12%TYPE,
      leg_interface_hdr_attribute13 xxar_invoices_ext_r12.interface_header_attribute13%TYPE,
      leg_interface_hdr_attribute14 xxar_invoices_ext_r12.interface_header_attribute14%TYPE,
      leg_interface_hdr_attribute15 xxar_invoices_ext_r12.interface_header_attribute15%TYPE,
      interface_header_context      VARCHAR2(150),
      interface_header_attribute1   VARCHAR2(150),
      interface_header_attribute2   VARCHAR2(150),
      interface_header_attribute3   VARCHAR2(150),
      interface_header_attribute4   VARCHAR2(150),
      interface_header_attribute5   VARCHAR2(150),
      interface_header_attribute6   VARCHAR2(150),
      interface_header_attribute7   VARCHAR2(150),
      interface_header_attribute8   VARCHAR2(150),
      interface_header_attribute9   VARCHAR2(150),
      interface_header_attribute10  VARCHAR2(150),
      interface_header_attribute11  VARCHAR2(150),
      interface_header_attribute12  VARCHAR2(150),
      interface_header_attribute13  VARCHAR2(150),
      interface_header_attribute14  VARCHAR2(150),
      interface_header_attribute15  VARCHAR2(150),
      credit_office                 VARCHAR2(240),
      --ver1.21.3 changes start
      leg_amount_includes_tax_flag xxar_invoices_ext_r12.leg_amount_include_tax_flag%TYPE,
      --ver1.21.3 changes end
      --ver.1.20.1 changes end
      --ver1.25 changes start
      country     VARCHAR2(240),
      site_status VARCHAR2(100),
      --ver1.25 changes end
      --ver1.26 changes start
      customer_type VARCHAR2(30),
      --ver1.26 changes end
      --ver1.27 changes start
      org_name VARCHAR2(240)
      --ver1.27 changes end
      );
    -----------------updated for including warehouse_id by Sherine V
    TYPE leg_invoice_tbl IS TABLE OF leg_invoice_rec INDEX BY BINARY_INTEGER;
    l_leg_invoice_tbl leg_invoice_tbl;
    l_err_record      NUMBER;
    ----------------------Updated for including warehouse_id by Sherine V----------------------------------------
    l_attr1 VARCHAR2(255);
    l_attr2 VARCHAR2(255);
    /*------For Brazil OU----

    select  ATTRIBUTE12, ATTRIBUTE13
     into l_attr1,l_attr2
    from FND_LOOKUP_TYPES a,
    FND_LOOKUP_VALUES b
    where a.lookup_type = b.lookup_type
    and a.lookup_type = 'BR_AR_WAREHOUSE_IDS'
    and b.attribute_category = 'BR_AR_WAREHOUSE_IDS'
    and language = 'US';
    */
    -----------------------Updated for including warehouse_id by Sherine V------------------------------------
    CURSOR cur_leg_invoices IS
      SELECT xil.interface_txn_id,
             xil.batch_id,
             xil.load_id,
             xil.run_sequence_id,
             xil.leg_batch_source_name,
             xil.leg_customer_number,
             xil.leg_bill_to_address,
             xil.leg_ship_to_address,
             xil.leg_currency_code,
             xil.leg_cust_trx_type_name,
             xil.leg_line_amount,
             xil.leg_trx_date,
             xil.leg_tax_code,
             xil.leg_tax_rate,
             xil.leg_conversion_date,
             xil.leg_conversion_rate,
             xil.leg_term_name,
             xil.leg_set_of_books_name,
             xil.leg_operating_unit,
             xil.leg_header_attribute_category,
             xil.leg_header_attribute1,
             NULL,
             NULL,
             xil.leg_header_attribute4,
             NULL
             --ver1.19 changes start
             --,NULL
            ,
             xil.leg_header_attribute6
             --ver1.19 changes end
            ,
             NULL
             --ver1.19 changes start
             --,xil.leg_header_attribute8
            ,
             NULL
             --ver1.19 changes end
            ,
             NULL,
             NULL,
             NULL,
             NULL,
             NULL,
             xil.leg_header_attribute14, --v1.50 Pulling Leg_Header_attribute14 column from 11i to R12
             NULL,
             xil.leg_reference_line_id,
             xil.leg_purchase_order,
             xil.leg_trx_number,
             xil.leg_line_number,
             xil.leg_comments,
             xil.leg_due_date,
             xil.leg_inv_amount_due_original,
             xil.leg_inv_amount_due_remaining,
             xil.leg_line_type,
             xil.leg_interface_line_context,
             xil.leg_interface_line_attribute1,
             xil.leg_interface_line_attribute2,
             NULL,
             NULL,
             NULL,
             NULL,
             NULL
             --ver1.23 changes start
             --,NULL
            ,
             xil.leg_interface_line_attribute8
             --ver1.23 changes start
            ,
             NULL,
             NULL,
             NULL,
             NULL,
             NULL
             --ver1.20.2 changes start
            ,
             xil.leg_interface_line_attribute14
             --ver1.20.2 changes end
            ,
             NULL,
             xil.leg_customer_trx_id,
             xil.trx_type,
             xil.leg_cust_trx_line_id,
             xil.leg_link_to_cust_trx_line_id,
             xil.leg_header_gdf_attr_category,
             xil.leg_header_gdf_attribute1,
             xil.leg_header_gdf_attribute2,
             xil.leg_header_gdf_attribute3,
             xil.leg_header_gdf_attribute4,
             xil.leg_header_gdf_attribute5,
             xil.leg_header_gdf_attribute6,
             xil.leg_header_gdf_attribute7,
             xil.leg_header_gdf_attribute8,
             xil.leg_header_gdf_attribute9,
             xil.leg_header_gdf_attribute10,
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
             xil.leg_line_gdf_attr_category,
             xil.leg_line_gdf_attribute1,
             xil.leg_line_gdf_attribute2,
             xil.leg_line_gdf_attribute3,
             xil.leg_line_gdf_attribute4,
             xil.leg_line_gdf_attribute5,
             xil.leg_line_gdf_attribute6,
             xil.leg_line_gdf_attribute7,
             xil.leg_line_gdf_attribute8,
             xil.leg_line_gdf_attribute9,
             xil.leg_line_gdf_attribute10,
             xil.leg_line_gdf_attribute11,
             xil.leg_line_gdf_attribute12,
             xil.leg_line_gdf_attribute13,
             xil.leg_line_gdf_attribute14,
             xil.leg_line_gdf_attribute15,
             xil.leg_line_gdf_attribute16,
             xil.leg_line_gdf_attribute17,
             xil.leg_line_gdf_attribute18,
             xil.leg_line_gdf_attribute19,
             xil.leg_line_gdf_attribute20,
             xil.leg_reason_code,
             xil.leg_source_system,
             xil.leg_quantity,
             xil.leg_quantity_ordered,
             xil.leg_unit_selling_price,
             xil.leg_unit_standard_price,
             xil.leg_ship_date_actual,
             xil.leg_fob_point,
             xil.leg_ship_via,
             xil.leg_waybill_number,
             xil.leg_sales_order_line,
             xil.leg_sales_order,
             xil.leg_gl_date,
             xil.leg_sales_order_date,
             xil.leg_sales_order_source,
             xil.leg_sales_order_revision,
             xil.leg_purchase_order_revision,
             xil.leg_purchase_order_date,
             xil.leg_agreement_name,
             xil.leg_agreement_id,
             xil.leg_memo_line_name,
             xil.leg_internal_notes,
             xil.leg_ussgl_trx_code_context,
             xil.leg_uom_name,
             xil.leg_vat_tax_name,
             xil.leg_sales_tax_name,
             xil.leg_request_id,
             xil.leg_seq_num,
             xil.leg_process_flag,
             xil.currency_code,
             xil.cust_trx_type_name,
             xil.line_type,
             xil.set_of_books_id,
             xil.trx_number,
             xil.line_number,
             xil.gl_date,
             NULL                               memo_line_id,
             xil.description,
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
             xil.system_bill_customer_id,
             xil.system_bill_customer_ref,
             xil.system_bill_address_id,
             xil.system_bill_address_ref,
             xil.system_bill_contact_id,
             xil.system_ship_customer_id,
             xil.system_ship_customer_ref,
             xil.system_ship_address_id,
             xil.system_ship_address_ref,
             xil.system_ship_contact_id,
             xil.system_sold_customer_id,
             xil.system_sold_customer_ref,
             xil.term_name,
             xil.ou_name,
             xil.conversion_type,
             xil.conversion_date,
             xil.conversion_rate,
             xil.trx_date,
             xil.batch_source_name,
             xil.purchase_order,
             xil.sales_order_date,
             xil.sales_order,
             NULL                               reference_line_id,
             xil.term_id,
             xil.org_id,
             xil.transaction_type_id,
             xil.tax_regime_code,
             xil.tax_code,
             xil.tax,
             xil.tax_status_code,
             xil.tax_rate_code,
             xil.tax_jurisdiction_code,
             xil.tax_rate,
             xil.adjustment_amount,
             xil.inv_amount_due_original,
             xil.inv_amount_due_remaining,
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
             xil.link_to_line_attribute10,
             xil.link_to_line_attribute11,
             xil.link_to_line_attribute12,
             xil.link_to_line_attribute13,
             xil.link_to_line_attribute14,
             xil.link_to_line_attribute15,
             xil.header_gdf_attr_category,
             xil.header_gdf_attribute1,
             xil.header_gdf_attribute2,
             xil.header_gdf_attribute3,
             xil.header_gdf_attribute4,
             xil.header_gdf_attribute5,
             xil.header_gdf_attribute6,
             xil.header_gdf_attribute7,
             xil.header_gdf_attribute8,
             xil.header_gdf_attribute9,
             xil.header_gdf_attribute10,
             xil.header_gdf_attribute11,
             xil.header_gdf_attribute12,
             xil.header_gdf_attribute13,
             xil.header_gdf_attribute14,
             xil.header_gdf_attribute15,
             xil.header_gdf_attribute16,
             xil.header_gdf_attribute17,
             xil.header_gdf_attribute18,
             xil.header_gdf_attribute19,
             xil.header_gdf_attribute20,
             xil.header_gdf_attribute21,
             xil.header_gdf_attribute22,
             xil.header_gdf_attribute23,
             xil.header_gdf_attribute24,
             xil.header_gdf_attribute25,
             xil.header_gdf_attribute26,
             xil.header_gdf_attribute27,
             xil.header_gdf_attribute28,
             xil.header_gdf_attribute29,
             xil.header_gdf_attribute30,
             xil.line_gdf_attr_category,
             xil.line_gdf_attribute1,
             xil.line_gdf_attribute2,
             xil.line_gdf_attribute3,
             xil.line_gdf_attribute4,
             xil.line_gdf_attribute5,
             xil.line_gdf_attribute6,
             xil.line_gdf_attribute7,
             xil.line_gdf_attribute8,
             xil.line_gdf_attribute9,
             xil.line_gdf_attribute10,
             xil.line_gdf_attribute11,
             xil.line_gdf_attribute12,
             xil.line_gdf_attribute13,
             xil.line_gdf_attribute14,
             xil.line_gdf_attribute15,
             xil.line_gdf_attribute16,
             xil.line_gdf_attribute17,
             xil.line_gdf_attribute18,
             xil.line_gdf_attribute19,
             xil.line_gdf_attribute20,
             xil.line_amount,
             xil.reason_code,
             xil.reason_code_meaning,
             xil.REFERENCE,
             xil.comments,
             g_sysdate                          creation_date,
             g_user_id                          created_by,
             g_sysdate                          last_update_date,
             g_user_id                          last_updated_by,
             g_login_id                         last_update_login,
             xil.program_application_id,
             xil.program_id,
             xil.program_update_date,
             NULL                               request_id,
             xil.process_flag,
             xil.ERROR_TYPE,
             xil.attribute_category,
             xil.attribute1,
             xil.attribute2,
             xil.attribute3,
             xil.attribute4,
             xil.attribute5,
             xil.attribute6,
             xil.attribute7,
             xil.attribute8,
             xil.attribute9,
             xil.attribute10,
             xil.attribute11,
             xil.attribute12,
             xil.attribute13,
             xil.attribute14,
             xil.attribute15,
             xil.vat_tax_id,
             xil.sales_tax_id,
             xil.uom_name,
             xil.ussgl_transaction_code_context,
             xil.internal_notes,
             xil.ship_date_actual,
             xil.fob_point,
             xil.ship_via,
             xil.waybill_number,
             xil.sales_order_line,
             xil.sales_order_source,
             xil.sales_order_revision,
             xil.purchase_order_revision,
             xil.purchase_order_date,
             xil.agreement_name,
             xil.agreement_id,
             xil.memo_line_name,
             xil.quantity,
             xil.quantity_ordered,
             xil.unit_selling_price,
             xil.unit_standard_price,
             NULL                               amount_includes_tax_flag,
             NULL                               taxable_flag,
             xil.request_id                     leg_req_id,
             NULL                               func_curr,
             NULL                               ledger_id,
             NULL                               invoicing_rule_id,
             xil.leg_warehouse_id --v1.29
            ,
             NULL                               warehouse_id
             --v1.36 change starts
            ,
             NULL                        receipt_method_id,
             xil.leg_receipt_method_name,
             NULL                        receipt_method_name
             --v1.36 change ends
             --ver.1.20.1 changes start
            ,
             xil.interface_header_context     leg_interface_hdr_context,
             xil.interface_header_attribute1  leg_interface_hdr_attribute1,
             xil.interface_header_attribute2  leg_interface_hdr_attribute2,
             xil.interface_header_attribute3  leg_interface_hdr_attribute3,
             xil.interface_header_attribute4  leg_interface_hdr_attribute4,
             xil.interface_header_attribute5  leg_interface_hdr_attribute5,
             xil.interface_header_attribute6  leg_interface_hdr_attribute6,
             xil.interface_header_attribute7  leg_interface_hdr_attribute7,
             xil.interface_header_attribute8  leg_interface_hdr_attribute8,
             xil.interface_header_attribute9  leg_interface_hdr_attribute9,
             xil.interface_header_attribute10 leg_interface_hdr_attribute10,
             xil.interface_header_attribute11 leg_interface_hdr_attribute11,
             xil.interface_header_attribute12 leg_interface_hdr_attribute12,
             xil.interface_header_attribute13 leg_interface_hdr_attribute13,
             xil.interface_header_attribute14 leg_interface_hdr_attribute14,
             xil.interface_header_attribute15 leg_interface_hdr_attribute15,
             NULL                             interface_header_context,
             NULL                             interface_header_attribute1,
             NULL                             interface_header_attribute2,
             NULL                             interface_header_attribute3,
             NULL                             interface_header_attribute4,
             NULL                             interface_header_attribute5,
             NULL                             interface_header_attribute6,
             NULL                             interface_header_attribute7,
             NULL                             interface_header_attribute8,
             NULL                             interface_header_attribute9,
             NULL                             interface_header_attribute10,
             NULL                             interface_header_attribute11,
             NULL                             interface_header_attribute12,
             NULL                             interface_header_attribute13,
             NULL                             interface_header_attribute14,
             NULL                             interface_header_attribute15,
             NULL                             credit_office
             --ver1.21.3 changes start
             --,NULL                               amount_includes_tax_flag
            ,
             xil.leg_amount_include_tax_flag leg_amount_includes_tax_flag
             --ver1.21.3 changes end
             --ver.1.20.1 changes end
             --ver1.25 changes start
            ,
             NULL country,
             NULL site_status,
             NULL customer_type
             --ver1.25 changes end
             --ver1.27 changes start
            ,
             NULL org_name
      --ver1.27 changes end
        FROM xxar_invoices_ext_r12 xil
       WHERE xil.leg_process_flag = 'V'
         AND NOT EXISTS
       (SELECT 1
                FROM xxar_invoices_stg xis
               WHERE xis.interface_txn_id = xil.interface_txn_id);
  BEGIN
    pov_ret_stats  := 'S';
    pov_err_msg    := NULL;
    g_total_count  := 0;
    g_failed_count := 0;
    /*

          select  ATTRIBUTE12, ATTRIBUTE13

     into l_attr1,l_attr2

    from FND_LOOKUP_TYPES a,

    FND_LOOKUP_VALUES b

    where a.lookup_type = b.lookup_type

    and a.lookup_type = 'BR_AR_WAREHOUSE_IDS'

    and b.attribute_category = 'BR_AR_WAREHOUSE_IDS'

    and language = 'US';*/
    --Open cursor to extract data from extraction staging table
    OPEN cur_leg_invoices;
    LOOP
      print_log_message('Loading invoices lines');
      l_leg_invoice_tbl.DELETE;
      FETCH cur_leg_invoices BULK COLLECT
        INTO l_leg_invoice_tbl LIMIT 5000;
      --limit size of Bulk Collect
      -- Get Total Count
      g_total_count := g_total_count + l_leg_invoice_tbl.COUNT;
      EXIT WHEN l_leg_invoice_tbl.COUNT = 0;
      BEGIN
        -- Bulk Insert into Conversion table
        FORALL indx IN 1 .. l_leg_invoice_tbl.COUNT SAVE EXCEPTIONS
          INSERT INTO xxar_invoices_stg VALUES l_leg_invoice_tbl (indx);
        /*

          ---------updating warehouse_id for Brazil-------

        for i in 1.. l_leg_invoice_tbl.COUNT

        loop

              update xxar_invoices_stg  xis

        set xis.warehouse_id=(select tag from fnd_lookup_values_vl

        where meaning=xis.warehouse_id

        --and lookup_type like 'BR_AR_WAREHOUSE_IDS'

        and xis.org_id in ('501','502'))

        where exists(

        select 1

        from fnd_lookup_values_vl

        where meaning=xis.warehouse_id

        --and lookup_type like 'BR_AR_WAREHOUSE_IDS'

        and xis.org_id in ('501','502'));



        commit;

        end loop;*/
        -----warehouseid updation---
      EXCEPTION
        WHEN OTHERS THEN
          print_log_message('Errors encountered while loading invoice lines data ');
          FOR l_indx_exp IN 1 .. SQL%BULK_EXCEPTIONS.COUNT LOOP
            l_err_record  := l_leg_invoice_tbl(SQL%BULK_EXCEPTIONS(l_indx_exp).ERROR_INDEX)
                             .interface_txn_id;
            pov_ret_stats := 'E';
            /*fnd_file.put_line(fnd_file.LOG,

                              'Record sequence (interface_txn_id) : ' || l_leg_invoice_tbl(SQL%BULK_EXCEPTIONS(l_indx_exp).ERROR_INDEX)

                              .interface_txn_id);

            fnd_file.put_line(fnd_file.LOG,

                              'Error Message : ' ||

                               SQLERRM(-SQL%BULK_EXCEPTIONS(l_indx_exp)

                                       .ERROR_CODE));

            */
            -- Updating Leg_process_flag to 'E' for failed records
            UPDATE xxar_invoices_ext_r12 xil
               SET xil.leg_process_flag       = 'E',
                   xil.last_updated_date      = g_sysdate,
                   xil.last_updated_by        = g_last_updated_by,
                   xil.last_update_login      = g_last_update_login,
                   xil.program_id             = g_conc_program_id,
                   xil.program_application_id = g_prog_appl_id,
                   xil.program_update_date    = g_sysdate
             WHERE xil.interface_txn_id = l_err_record
               AND xil.leg_process_flag = 'V';
            g_failed_count := g_failed_count + SQL%ROWCOUNT;
          END LOOP;
      END;
    END LOOP;
    CLOSE cur_leg_invoices;
    COMMIT;
    IF g_failed_count > 0 THEN
      g_retcode := 1;
    END IF;
    g_loaded_count := g_total_count - g_failed_count;
    -- If records successfully posted to conversion staging table
    IF g_total_count > 0 THEN
      print_log_message('Updating process flag (leg_process_flag) in extraction table for processed records ');
      UPDATE xxar_invoices_ext_r12 xil
         SET xil.leg_process_flag       = 'P',
             xil.last_updated_date      = g_sysdate,
             xil.last_updated_by        = g_last_updated_by,
             xil.last_update_login      = g_last_update_login,
             xil.program_id             = g_conc_program_id,
             xil.program_application_id = g_prog_appl_id,
             xil.program_update_date    = g_sysdate
       WHERE xil.leg_process_flag = 'V'
         AND EXISTS
       (SELECT 1
                FROM xxar_invoices_stg xis
               WHERE xis.interface_txn_id = xil.interface_txn_id);
      COMMIT;
      -- Either no data to load from extraction table or records already exist in R12 staging table and hence not loaded
    ELSE
      print_log_message('Either no data found for loading from extraction table or records already exist in R12 staging table and hence not loaded ');
      UPDATE xxar_invoices_ext_r12 xil
         SET xil.leg_process_flag       = 'E',
             xil.last_updated_date      = g_sysdate,
             xil.last_updated_by        = g_last_updated_by,
             xil.last_update_login      = g_last_update_login,
             xil.program_id             = g_conc_program_id,
             xil.program_application_id = g_prog_appl_id,
             xil.program_update_date    = g_sysdate
       WHERE xil.leg_process_flag = 'V'
         AND EXISTS
       (SELECT 1
                FROM xxar_invoices_stg xis
               WHERE xis.interface_txn_id = xil.interface_txn_id);
      g_retcode := 1;
      COMMIT;
    END IF;
  EXCEPTION
    WHEN OTHERS THEN
      g_retcode     := 2;
      pov_ret_stats := 'E';
      pov_err_msg   := 'ERROR : Error in load_invoice procedure' ||
                       SUBSTR(SQLERRM, 1, 150);
      ROLLBACK;
  END load_invoice;

  --
  -- ========================
  -- Procedure: LOAD_DISTRIBUTION
  -- =============================================================================
  --   This procedure is used to load distribution lines data from extraction staging table
  --   to conversion staging table when program is run in LOAD mode
  -- =============================================================================
  PROCEDURE load_distribution(pov_ret_stats OUT NOCOPY VARCHAR2,
                              pov_err_msg   OUT NOCOPY VARCHAR2) IS
    TYPE leg_invoice_dist_rec IS RECORD(
      interface_txn_id               xxar_invoices_dist_ext_r12.interface_txn_id%TYPE,
      batch_id                       xxar_invoices_dist_ext_r12.batch_id%TYPE,
      load_id                        xxar_invoices_dist_ext_r12.load_id%TYPE,
      run_sequence_id                xxar_invoices_dist_ext_r12.run_sequence_id%TYPE,
      leg_customer_trx_id            xxar_invoices_dist_ext_r12.leg_customer_trx_id%TYPE,
      leg_cust_trx_line_id           xxar_invoices_dist_ext_r12.leg_cust_trx_line_id%TYPE,
      leg_cust_trx_line_gl_dist_id   xxar_invoices_dist_ext_r12.leg_cust_trx_line_gl_dist_id%TYPE,
      leg_percent                    xxar_invoices_dist_ext_r12.leg_percent%TYPE,
      leg_account_class              xxar_invoices_dist_ext_r12.leg_account_class%TYPE,
      leg_dist_segment1              xxar_invoices_dist_ext_r12.leg_dist_segment1%TYPE,
      leg_dist_segment2              xxar_invoices_dist_ext_r12.leg_dist_segment2%TYPE,
      leg_dist_segment3              xxar_invoices_dist_ext_r12.leg_dist_segment3%TYPE,
      leg_dist_segment4              xxar_invoices_dist_ext_r12.leg_dist_segment4%TYPE,
      leg_dist_segment5              xxar_invoices_dist_ext_r12.leg_dist_segment5%TYPE,
      leg_dist_segment6              xxar_invoices_dist_ext_r12.leg_dist_segment6%TYPE,
      leg_dist_segment7              xxar_invoices_dist_ext_r12.leg_dist_segment7%TYPE,
      leg_org_name                   xxar_invoices_dist_ext_r12.leg_org_name%TYPE,
      leg_accounted_amount           xxar_invoices_dist_ext_r12.leg_accounted_amount%TYPE,
      leg_interface_line_context     xxar_invoices_dist_ext_r12.leg_interface_line_context%TYPE,
      leg_interface_line_attribute1  xxar_invoices_dist_ext_r12.leg_interface_line_attribute1%TYPE,
      leg_interface_line_attribute2  xxar_invoices_dist_ext_r12.leg_interface_line_attribute2%TYPE,
      leg_interface_line_attribute3  xxar_invoices_dist_ext_r12.leg_interface_line_attribute3%TYPE,
      leg_interface_line_attribute4  xxar_invoices_dist_ext_r12.leg_interface_line_attribute4%TYPE,
      leg_interface_line_attribute5  xxar_invoices_dist_ext_r12.leg_interface_line_attribute5%TYPE,
      leg_interface_line_attribute6  xxar_invoices_dist_ext_r12.leg_interface_line_attribute6%TYPE,
      leg_interface_line_attribute7  xxar_invoices_dist_ext_r12.leg_interface_line_attribute7%TYPE,
      leg_interface_line_attribute8  xxar_invoices_dist_ext_r12.leg_interface_line_attribute8%TYPE,
      leg_interface_line_attribute9  xxar_invoices_dist_ext_r12.leg_interface_line_attribute9%TYPE,
      leg_interface_line_attribute10 xxar_invoices_dist_ext_r12.leg_interface_line_attribute10%TYPE,
      leg_interface_line_attribute11 xxar_invoices_dist_ext_r12.leg_interface_line_attribute11%TYPE,
      leg_interface_line_attribute12 xxar_invoices_dist_ext_r12.leg_interface_line_attribute12%TYPE,
      leg_interface_line_attribute13 xxar_invoices_dist_ext_r12.leg_interface_line_attribute13%TYPE,
      leg_interface_line_attribute14 xxar_invoices_dist_ext_r12.leg_interface_line_attribute14%TYPE,
      leg_interface_line_attribute15 xxar_invoices_dist_ext_r12.leg_interface_line_attribute15%TYPE,
      interface_line_context         xxar_invoices_dist_ext_r12.interface_line_context%TYPE,
      interface_line_attribute1      xxar_invoices_dist_ext_r12.interface_line_attribute1%TYPE,
      interface_line_attribute2      xxar_invoices_dist_ext_r12.interface_line_attribute2%TYPE,
      interface_line_attribute3      xxar_invoices_dist_ext_r12.interface_line_attribute3%TYPE,
      interface_line_attribute4      xxar_invoices_dist_ext_r12.interface_line_attribute4%TYPE,
      interface_line_attribute5      xxar_invoices_dist_ext_r12.interface_line_attribute5%TYPE,
      interface_line_attribute6      xxar_invoices_dist_ext_r12.interface_line_attribute6%TYPE,
      interface_line_attribute7      xxar_invoices_dist_ext_r12.interface_line_attribute7%TYPE,
      interface_line_attribute8      xxar_invoices_dist_ext_r12.interface_line_attribute8%TYPE,
      interface_line_attribute9      xxar_invoices_dist_ext_r12.interface_line_attribute9%TYPE,
      interface_line_attribute10     xxar_invoices_dist_ext_r12.interface_line_attribute10%TYPE,
      interface_line_attribute11     xxar_invoices_dist_ext_r12.interface_line_attribute11%TYPE,
      interface_line_attribute12     xxar_invoices_dist_ext_r12.interface_line_attribute12%TYPE,
      interface_line_attribute13     xxar_invoices_dist_ext_r12.interface_line_attribute13%TYPE,
      interface_line_attribute14     xxar_invoices_dist_ext_r12.interface_line_attribute14%TYPE,
      interface_line_attribute15     xxar_invoices_dist_ext_r12.interface_line_attribute15%TYPE,
      dist_segment1                  xxar_invoices_dist_ext_r12.dist_segment1%TYPE,
      dist_segment2                  xxar_invoices_dist_ext_r12.dist_segment2%TYPE,
      dist_segment3                  xxar_invoices_dist_ext_r12.dist_segment3%TYPE,
      dist_segment4                  xxar_invoices_dist_ext_r12.dist_segment4%TYPE,
      dist_segment5                  xxar_invoices_dist_ext_r12.dist_segment5%TYPE,
      dist_segment6                  xxar_invoices_dist_ext_r12.dist_segment6%TYPE,
      dist_segment7                  xxar_invoices_dist_ext_r12.dist_segment7%TYPE,
      dist_segment8                  xxar_invoices_dist_ext_r12.dist_segment8%TYPE,
      dist_segment9                  xxar_invoices_dist_ext_r12.dist_segment9%TYPE,
      dist_segment10                 xxar_invoices_dist_ext_r12.dist_segment10%TYPE,
      accounted_amount               xxar_invoices_dist_ext_r12.accounted_amount%TYPE,
      code_combination_id            xxar_invoices_dist_ext_r12.code_combination_id%TYPE,
      account_class                  xxar_invoices_dist_ext_r12.account_class%TYPE,
      PERCENT                        xxar_invoices_dist_ext_r12.PERCENT%TYPE,
      org_id                         xxar_invoices_dist_ext_r12.org_id%TYPE,
      creation_date                  xxar_invoices_dist_ext_r12.creation_date%TYPE,
      created_by                     xxar_invoices_dist_ext_r12.created_by%TYPE,
      last_update_date               xxar_invoices_dist_ext_r12.last_update_date%TYPE,
      last_updated_by                xxar_invoices_dist_ext_r12.last_updated_by%TYPE,
      last_update_login              xxar_invoices_dist_ext_r12.last_update_login%TYPE,
      program_application_id         xxar_invoices_dist_ext_r12.program_application_id%TYPE,
      program_id                     xxar_invoices_dist_ext_r12.program_id%TYPE,
      program_update_date            xxar_invoices_dist_ext_r12.program_update_date%TYPE,
      request_id                     xxar_invoices_dist_ext_r12.request_id%TYPE,
      process_flag                   xxar_invoices_dist_ext_r12.process_flag%TYPE,
      ERROR_TYPE                     xxar_invoices_dist_ext_r12.ERROR_TYPE%TYPE,
      attribute_category             xxar_invoices_dist_ext_r12.attribute_category%TYPE,
      attribute1                     xxar_invoices_dist_ext_r12.attribute1%TYPE,
      attribute2                     xxar_invoices_dist_ext_r12.attribute2%TYPE,
      attribute3                     xxar_invoices_dist_ext_r12.attribute3%TYPE,
      attribute4                     xxar_invoices_dist_ext_r12.attribute4%TYPE,
      attribute5                     xxar_invoices_dist_ext_r12.attribute5%TYPE,
      attribute6                     xxar_invoices_dist_ext_r12.attribute6%TYPE,
      attribute7                     xxar_invoices_dist_ext_r12.attribute7%TYPE,
      attribute8                     xxar_invoices_dist_ext_r12.attribute8%TYPE,
      attribute9                     xxar_invoices_dist_ext_r12.attribute9%TYPE,
      attribute10                    xxar_invoices_dist_ext_r12.attribute10%TYPE,
      attribute11                    xxar_invoices_dist_ext_r12.attribute11%TYPE,
      attribute12                    xxar_invoices_dist_ext_r12.attribute12%TYPE,
      attribute13                    xxar_invoices_dist_ext_r12.attribute13%TYPE,
      attribute14                    xxar_invoices_dist_ext_r12.attribute14%TYPE,
      attribute15                    xxar_invoices_dist_ext_r12.attribute15%TYPE,
      leg_source_system              xxar_invoices_dist_ext_r12.leg_source_system%TYPE,
      leg_request_id                 xxar_invoices_dist_ext_r12.leg_request_id%TYPE,
      leg_seq_num                    xxar_invoices_dist_ext_r12.leg_seq_num%TYPE,
      leg_process_flag               xxar_invoices_dist_ext_r12.leg_process_flag%TYPE,
      --ver1.21.1 changes start
      dist_amount NUMBER
      --ver1.21.1 changes end
      );
    TYPE leg_dist_tbl IS TABLE OF leg_invoice_dist_rec INDEX BY BINARY_INTEGER;
    l_leg_dist_tbl leg_dist_tbl;
    l_err_record   NUMBER;
    CURSOR cur_leg_dist IS
      SELECT interface_txn_id,
             batch_id,
             load_id,
             run_sequence_id,
             leg_customer_trx_id,
             leg_cust_trx_line_id,
             leg_cust_trx_line_gl_dist_id,
             leg_percent,
             leg_account_class,
             leg_dist_segment1,
             leg_dist_segment2,
             leg_dist_segment3,
             leg_dist_segment4,
             leg_dist_segment5,
             leg_dist_segment6,
             leg_dist_segment7,
             leg_org_name,
             leg_accounted_amount,
             leg_interface_line_context,
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
             leg_interface_line_attribute15,
             interface_line_context,
             interface_line_attribute1,
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
             interface_line_attribute15,
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
             accounted_amount,
             code_combination_id,
             account_class,
             PERCENT,
             org_id,
             g_sysdate                      creation_date,
             g_user_id                      created_by,
             g_sysdate                      last_update_date,
             g_user_id                      last_updated_by,
             g_login_id                     last_update_login,
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
             leg_process_flag,
             NULL                           dist_amount
        FROM xxar_invoices_dist_ext_r12 xil
       WHERE xil.leg_process_flag = 'V'
            --      AND xil.leg_account_class <> 'REC'
         AND xil.leg_account_class <> 'ROUND' --performance
         AND xil.leg_account_class <> 'UNEARN'
         AND NOT EXISTS
       (SELECT 1
                FROM xxar_invoices_dist_stg xis
               WHERE xis.interface_txn_id = xil.interface_txn_id);
  BEGIN
    pov_ret_stats       := 'S';
    pov_err_msg         := NULL;
    g_total_dist_count  := 0;
    g_failed_dist_count := 0;
    --Open cursor to extract data from extraction staging table for distributions
    OPEN cur_leg_dist;
    LOOP
      print_log_message('Loading distribution lines');
      l_leg_dist_tbl.DELETE;
      FETCH cur_leg_dist BULK COLLECT
        INTO l_leg_dist_tbl LIMIT 5000;
      --limit size of Bulk Collect
      -- Get Total Count
      g_total_dist_count := g_total_dist_count + l_leg_dist_tbl.COUNT;
      EXIT WHEN l_leg_dist_tbl.COUNT = 0;
      BEGIN
        -- Bulk Insert into Conversion table
        FORALL indx IN 1 .. l_leg_dist_tbl.COUNT SAVE EXCEPTIONS
          INSERT INTO xxar_invoices_dist_stg VALUES l_leg_dist_tbl (indx);
      EXCEPTION
        WHEN OTHERS THEN
          print_log_message('Errors encountered while loading distribution lines data ');
          FOR l_indx_exp IN 1 .. SQL%BULK_EXCEPTIONS.COUNT LOOP
            l_err_record  := l_leg_dist_tbl(SQL%BULK_EXCEPTIONS(l_indx_exp).ERROR_INDEX)
                             .interface_txn_id;
            pov_ret_stats := 'E';
            /*

            fnd_file.put_line(fnd_file.LOG,

                              'Record sequence : ' || l_leg_dist_tbl(SQL%BULK_EXCEPTIONS(l_indx_exp).ERROR_INDEX)

                              .interface_txn_id);

            fnd_file.put_line(fnd_file.LOG,

                              'Error Message : ' ||

                               SQLERRM(-SQL%BULK_EXCEPTIONS(l_indx_exp)

                                       .ERROR_CODE));

            */
            -- Updating Leg_process_flag to 'E' for failed records
            UPDATE xxar_invoices_dist_ext_r12 xil
               SET xil.leg_process_flag       = 'E',
                   xil.last_update_date       = g_sysdate,
                   xil.last_updated_by        = g_last_updated_by,
                   xil.last_update_login      = g_last_update_login,
                   xil.program_id             = g_conc_program_id,
                   xil.program_application_id = g_prog_appl_id,
                   xil.program_update_date    = g_sysdate
             WHERE xil.interface_txn_id = l_err_record
               AND xil.leg_process_flag = 'V';
            g_failed_dist_count := g_failed_dist_count + SQL%ROWCOUNT;
          END LOOP;
      END;
    END LOOP;
    CLOSE cur_leg_dist;
    COMMIT;
    IF g_failed_dist_count > 0 THEN
      g_retcode := 1;
    END IF;
    g_loaded_dist_count := g_total_dist_count - g_failed_dist_count;
    -- If records successfully posted to conversion staging table
    IF g_total_dist_count > 0 THEN
      print_log_message('Updating process flag (leg_process_flag) in extraction table for processed records ');
      UPDATE xxar_invoices_dist_ext_r12 xil
         SET xil.leg_process_flag       = 'P',
             xil.last_update_date       = g_sysdate,
             xil.last_updated_by        = g_last_updated_by,
             xil.last_update_login      = g_last_update_login,
             xil.program_id             = g_conc_program_id,
             xil.program_application_id = g_prog_appl_id,
             xil.program_update_date    = g_sysdate
       WHERE xil.leg_process_flag = 'V'
         AND EXISTS
       (SELECT 1
                FROM xxar_invoices_dist_stg xis
               WHERE xis.interface_txn_id = xil.interface_txn_id);
      --performance
      UPDATE xxar_invoices_dist_ext_r12 xil
         SET xil.leg_process_flag       = 'P',
             xil.last_update_date       = g_sysdate,
             xil.last_updated_by        = g_last_updated_by,
             xil.last_update_login      = g_last_update_login,
             xil.program_id             = g_conc_program_id,
             xil.program_application_id = g_prog_appl_id,
             xil.program_update_date    = g_sysdate
       WHERE xil.leg_process_flag = 'V'
         AND xil.leg_account_class = 'ROUND'
         AND NVL(leg_account_class, 'A') <> 'UNEARN';
      COMMIT;
    ELSE
      -- Either no data to load from extraction table or records already exist in R12 staging table and hence not loaded
      print_log_message('Either no data found for loading from extraction table or records already exist in R12 staging table and hence not loaded ');
      UPDATE xxar_invoices_dist_ext_r12 xil
         SET xil.leg_process_flag       = 'E',
             xil.last_update_date       = g_sysdate,
             xil.last_updated_by        = g_last_updated_by,
             xil.last_update_login      = g_last_update_login,
             xil.program_id             = g_conc_program_id,
             xil.program_application_id = g_prog_appl_id,
             xil.program_update_date    = g_sysdate
       WHERE xil.leg_process_flag = 'V'
         AND EXISTS
       (SELECT 1
                FROM xxar_invoices_dist_stg xis
               WHERE xis.interface_txn_id = xil.interface_txn_id);
      g_retcode := 1;
      COMMIT;
    END IF;
  EXCEPTION
    WHEN OTHERS THEN
      g_retcode     := 2;
      pov_ret_stats := 'E';
      pov_err_msg   := 'ERROR : Error in load_distribution procedure' ||
                       SUBSTR(SQLERRM, 1, 150);
      ROLLBACK;
  END load_distribution;

  --ver1.39.1 changes start
  --
  -- ========================
  -- Procedure: LOAD_PMT_SCHEDULES
  -- =============================================================================
  --   This procedure is used to load payment schedules data from extraction staging table
  --   to conversion staging table when program is run in LOAD mode
  -- =============================================================================
  PROCEDURE load_pmt_schedules(pov_ret_stats OUT NOCOPY VARCHAR2,
                               pov_err_msg   OUT NOCOPY VARCHAR2) IS
    TYPE leg_inv_pmt_sch_rec IS RECORD(
      interface_txn_id          xxar_inv_pay_sch_ext_r12.interface_txn_id%TYPE,
      batch_id                  xxar_inv_pay_sch_ext_r12.batch_id%TYPE,
      load_id                   xxar_inv_pay_sch_ext_r12.load_id%TYPE,
      run_sequence_id           xxar_inv_pay_sch_ext_r12.run_sequence_id%TYPE,
      leg_source_system         xxar_inv_pay_sch_ext_r12.leg_source_system%TYPE,
      leg_operating_unit        xxar_inv_pay_sch_ext_r12.leg_operating_unit%TYPE,
      leg_customer_trx_id       xxar_inv_pay_sch_ext_r12.leg_customer_trx_id%TYPE,
      leg_trx_number            xxar_inv_pay_sch_ext_r12.leg_trx_number%TYPE,
      leg_invoice_currency_code xxar_inv_pay_sch_ext_r12.leg_invoice_currency_code%TYPE,
      leg_term_name             xxar_inv_pay_sch_ext_r12.leg_term_name%TYPE,
      leg_terms_sequence_number xxar_inv_pay_sch_ext_r12.leg_terms_sequence_number%TYPE,
      leg_due_date              xxar_inv_pay_sch_ext_r12.leg_due_date%TYPE,
      leg_amount_due_original   xxar_inv_pay_sch_ext_r12.leg_amount_due_original%TYPE,
      leg_amount_due_remaining  xxar_inv_pay_sch_ext_r12.leg_amount_due_remaining%TYPE,
      leg_payment_schedule_id   xxar_inv_pay_sch_ext_r12.leg_payment_schedule_id%TYPE,
      leg_gl_date               xxar_inv_pay_sch_ext_r12.leg_gl_date%TYPE,
      leg_number_of_due_dates   xxar_inv_pay_sch_ext_r12.leg_number_of_due_dates%TYPE,
      leg_amount_in_dispute     xxar_inv_pay_sch_ext_r12.leg_amount_in_dispute%TYPE,
      leg_dispute_date          xxar_inv_pay_sch_ext_r12.leg_dispute_date%TYPE,
      leg_seq_num               xxar_inv_pay_sch_ext_r12.leg_seq_num%TYPE,
      leg_request_id            xxar_inv_pay_sch_ext_r12.leg_request_id%TYPE,
      leg_process_flag          xxar_inv_pay_sch_ext_r12.leg_process_flag%TYPE,
      leg_attribute_category    xxar_inv_pay_sch_ext_r12.leg_attribute_category%TYPE,
      leg_attribute1            xxar_inv_pay_sch_ext_r12.leg_attribute1%TYPE,
      leg_attribute2            xxar_inv_pay_sch_ext_r12.leg_attribute2%TYPE,
      leg_attribute3            xxar_inv_pay_sch_ext_r12.leg_attribute3%TYPE,
      leg_attribute4            xxar_inv_pay_sch_ext_r12.leg_attribute4%TYPE,
      leg_attribute5            xxar_inv_pay_sch_ext_r12.leg_attribute5%TYPE,
      operating_unit            xxar_inv_pay_sch_ext_r12.operating_unit%TYPE,
      org_id                    xxar_inv_pay_sch_ext_r12.org_id%TYPE,
      customer_trx_id           xxar_inv_pay_sch_ext_r12.customer_trx_id%TYPE,
      term_name                 xxar_inv_pay_sch_ext_r12.term_name%TYPE,
      term_id                   xxar_inv_pay_sch_ext_r12.term_id%TYPE,
      creation_date             xxar_inv_pay_sch_ext_r12.creation_date%TYPE,
      created_by                xxar_inv_pay_sch_ext_r12.created_by%TYPE,
      last_update_date          xxar_inv_pay_sch_ext_r12.last_update_date%TYPE,
      last_updated_by           xxar_inv_pay_sch_ext_r12.last_updated_by%TYPE,
      last_update_login         NUMBER,
      program_application_id    NUMBER,
      program_id                NUMBER,
      program_update_date       DATE,
      request_id                xxar_inv_pay_sch_ext_r12.request_id%TYPE,
      process_flag              xxar_inv_pay_sch_ext_r12.process_flag%TYPE,
      error_type                xxar_inv_pay_sch_ext_r12.error_type%TYPE,
      future_use_1              xxar_inv_pmt_schedule_stg.future_use_1%TYPE,
      future_use_2              xxar_inv_pmt_schedule_stg.future_use_2%TYPE,
      future_use_3              xxar_inv_pmt_schedule_stg.future_use_3%TYPE,
      future_use_4              xxar_inv_pmt_schedule_stg.future_use_4%TYPE,
      future_use_5              xxar_inv_pmt_schedule_stg.future_use_5%TYPE,
      future_use_6              xxar_inv_pmt_schedule_stg.future_use_6%TYPE,
      future_use_7              xxar_inv_pmt_schedule_stg.future_use_7%TYPE,
      future_use_8              xxar_inv_pmt_schedule_stg.future_use_8%TYPE,
      future_use_9              xxar_inv_pmt_schedule_stg.future_use_9%TYPE,
      future_use_10             xxar_inv_pmt_schedule_stg.future_use_10%TYPE);
    TYPE leg_pmt_sch_tbl IS TABLE OF leg_inv_pmt_sch_rec INDEX BY BINARY_INTEGER;
    l_leg_pmt_sch_tbl leg_pmt_sch_tbl;
    l_err_record      NUMBER;
    CURSOR cur_leg_pmt_sch IS
      SELECT interface_txn_id,
             batch_id,
             load_id,
             run_sequence_id,
             leg_source_system,
             leg_operating_unit,
             leg_customer_trx_id,
             leg_trx_number,
             leg_invoice_currency_code,
             leg_term_name,
             leg_terms_sequence_number,
             leg_due_date,
             leg_amount_due_original,
             leg_amount_due_remaining,
             leg_payment_schedule_id,
             leg_gl_date,
             leg_number_of_due_dates,
             leg_amount_in_dispute,
             leg_dispute_date,
             leg_seq_num,
             leg_request_id,
             leg_process_flag,
             leg_attribute_category,
             leg_attribute1,
             leg_attribute2,
             leg_attribute3,
             leg_attribute4,
             leg_attribute5,
             operating_unit,
             org_id,
             customer_trx_id,
             term_name,
             term_id,
             g_sysdate                 creation_date,
             g_user_id                 created_by,
             g_sysdate                 last_update_date,
             g_user_id                 last_updated_by,
             g_login_id                last_update_login,
             NULL                      program_application_id,
             NULL                      program_id,
             NULL                      program_update_date,
             request_id,
             process_flag,
             error_type,
             NULL                      future_use_1,
             NULL                      future_use_2,
             NULL                      future_use_3,
             NULL                      future_use_4,
             NULL                      future_use_5,
             NULL                      future_use_6,
             NULL                      future_use_7,
             NULL                      future_use_8,
             NULL                      future_use_9,
             NULL                      future_use_10
        FROM xxar_inv_pay_sch_ext_r12 xil
       WHERE xil.leg_process_flag = 'V'
         AND NOT EXISTS
       (SELECT 1
                FROM xxar_inv_pmt_schedule_stg xis
               WHERE xis.interface_txn_id = xil.interface_txn_id);
  BEGIN
    pov_ret_stats          := 'S';
    pov_err_msg            := NULL;
    g_total_pmt_sch_count  := 0;
    g_failed_pmt_sch_count := 0;
    --Open cursor to extract data from extraction staging table for distributions
    OPEN cur_leg_pmt_sch;
    LOOP
      print_log_message('Loading payment schedule data');
      l_leg_pmt_sch_tbl.DELETE;
      FETCH cur_leg_pmt_sch BULK COLLECT
        INTO l_leg_pmt_sch_tbl LIMIT 5000;
      --limit size of Bulk Collect
      -- Get Total Count
      g_total_pmt_sch_count := g_total_pmt_sch_count +
                               l_leg_pmt_sch_tbl.COUNT;
      EXIT WHEN l_leg_pmt_sch_tbl.COUNT = 0;
      BEGIN
        -- Bulk Insert into Conversion table
        FORALL indx IN 1 .. l_leg_pmt_sch_tbl.COUNT SAVE EXCEPTIONS
          INSERT INTO xxar_inv_pmt_schedule_stg
          VALUES l_leg_pmt_sch_tbl
            (indx);
      EXCEPTION
        WHEN OTHERS THEN
          print_log_message('Errors encountered while loading payment schedules data ');
          FOR l_indx_exp IN 1 .. SQL%BULK_EXCEPTIONS.COUNT LOOP
            l_err_record  := l_leg_pmt_sch_tbl(SQL%BULK_EXCEPTIONS(l_indx_exp).ERROR_INDEX)
                             .interface_txn_id;
            pov_ret_stats := 'E';
            /*
            fnd_file.put_line(fnd_file.LOG,
                              'Record sequence : ' || l_leg_pmt_sch_tbl(SQL%BULK_EXCEPTIONS(l_indx_exp).ERROR_INDEX)
                              .interface_txn_id);
            fnd_file.put_line(fnd_file.LOG,
                              'Error Message : ' ||
                               SQLERRM(-SQL%BULK_EXCEPTIONS(l_indx_exp)
                                       .ERROR_CODE));
            */
            -- Updating Leg_process_flag to 'E' for failed records
            UPDATE xxar_inv_pay_sch_ext_r12 xil
               SET xil.leg_process_flag = 'E',
                   xil.last_update_date = g_sysdate,
                   xil.last_updated_by  = g_last_updated_by
            --   ,xil.last_update_login      = g_last_update_login
            --    ,xil.program_id             = g_conc_program_id
            --                        ,xil.program_application_id = g_prog_appl_id
            --                        ,xil.program_update_date    = SYSDATE
             WHERE xil.interface_txn_id = l_err_record
               AND xil.leg_process_flag = 'V';
            g_failed_pmt_sch_count := g_failed_pmt_sch_count + SQL%ROWCOUNT;
          END LOOP;
      END;
    END LOOP;
    CLOSE cur_leg_pmt_sch;
    COMMIT;
    IF g_failed_pmt_sch_count > 0 THEN
      g_retcode := 1;
    END IF;
    g_loaded_pmt_sch_count := g_total_pmt_sch_count -
                              g_failed_pmt_sch_count;
    -- If records successfully posted to conversion staging table
    IF g_total_pmt_sch_count > 0 THEN
      print_log_message('Updating process flag (leg_process_flag) in extraction table for processed records ');
      UPDATE xxar_inv_pay_sch_ext_r12 xil
         SET xil.leg_process_flag = 'P',
             xil.last_update_date = g_sysdate,
             xil.last_updated_by  = g_last_updated_by
      --  ,xil.last_update_login      = g_last_update_login
      --  ,xil.program_id             = g_conc_program_id
      --  ,xil.program_application_id = g_prog_appl_id
      --  ,xil.program_update_date    = g_sysdate
       WHERE xil.leg_process_flag = 'V'
         AND EXISTS
       (SELECT 1
                FROM xxar_inv_pmt_schedule_stg xis
               WHERE xis.interface_txn_id = xil.interface_txn_id);
      COMMIT;
    ELSE
      -- Either no data to load from extraction table or records already exist in R12 staging table and hence not loaded
      print_log_message('Either no data found for loading from extraction table or records already exist in R12 staging table and hence not loaded ');
      UPDATE xxar_inv_pay_sch_ext_r12 xil
         SET xil.leg_process_flag = 'E',
             xil.last_update_date = g_sysdate,
             xil.last_updated_by  = g_last_updated_by
      --  ,xil.last_update_login      = g_last_update_login
      --   ,xil.program_id             = g_conc_program_id
      --   ,xil.program_application_id = g_prog_appl_id
      --  ,xil.program_update_date    = g_sysdate
       WHERE xil.leg_process_flag = 'V'
         AND EXISTS
       (SELECT 1
                FROM xxar_inv_pmt_schedule_stg xis
               WHERE xis.interface_txn_id = xil.interface_txn_id);
      g_retcode := 1;
      COMMIT;
    END IF;
  EXCEPTION
    WHEN OTHERS THEN
      g_retcode     := 2;
      pov_ret_stats := 'E';
      pov_err_msg   := 'ERROR : Error in load_pmt_schedules procedure' ||
                       SUBSTR(SQLERRM, 1, 150);
      ROLLBACK;
  END load_pmt_schedules;

  --ver1.39.1 changes end
  --
  -- ========================
  -- Procedure: PRE_VALIDATE_INVOICE
  -- =============================================================================
  --   This procedure is used to do pre validations of functional setups
  --   before the actual conversion run.
  -- =============================================================================
  PROCEDURE pre_validate_invoice IS
    l_ou_map        NUMBER;
    l_batch_source  NUMBER;
    l_pmt_terms_map NUMBER;
    l_trx_type_map  NUMBER;
    l_tax_code_map  NUMBER;
    l_err_msg       VARCHAR2(2000);
    l_memo_line     NUMBER;
  BEGIN
    -- Check whether operating unit cross reference exists
    BEGIN
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
        /*

                  log_errors ( p_err_type              => 'ERR_PREVAL'

                      , p_err_code            =>  l_err_code

                      , p_err_msg            =>  l_err_msg

                      , p_return_status        =>  l_log_ret_status

                      , p_error_message        =>  l_log_err_msg

                     );

        */
      WHEN OTHERS THEN
        g_retcode := 1;
        l_err_msg := 'Error : Exception in Pre-validation Procedure. For lookup ' ||
                     g_ou_lookup || ':' || SUBSTR(SQLERRM, 1, 150);
        print_log_message(l_err_msg);
    END;
    -- Check whether payment terms cross reference exists
    BEGIN
      SELECT 1
        INTO l_pmt_terms_map
        FROM apps.fnd_lookup_types_tl flt
       WHERE flt.LANGUAGE = USERENV('LANG')
            --  AND UPPER(flt.lookup_type) = g_pmt_term_lookup;       SS : Removed UPPER so that index can kick in
         AND flt.lookup_type = g_pmt_term_lookup;
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        g_retcode := 1;
        l_err_msg := 'Error : Exception in Pre-validation Procedure. Lookup ' ||
                     g_pmt_term_lookup ||
                     ' not defined for payment terms cross reference';
        print_log_message(l_err_msg);
      WHEN OTHERS THEN
        g_retcode := 1;
        l_err_msg := 'Error : Exception in Pre-validation Procedure. For lookup ' ||
                     g_pmt_term_lookup || ': ' || SUBSTR(SQLERRM, 1, 150);
        print_log_message(l_err_msg);
    END;
    -- Check whether transaction type cross reference exists
    BEGIN
      SELECT 1
        INTO l_trx_type_map
        FROM apps.fnd_lookup_types_tl flt
       WHERE flt.LANGUAGE = USERENV('LANG')
            --  AND UPPER(flt.lookup_type) = g_trx_type_lookup;       SS Removed UPPER so that index can be used.
         AND flt.lookup_type = g_trx_type_lookup;
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        g_retcode := 1;
        l_err_msg := 'Error : Exception in Pre-validation Procedure. Lookup ' ||
                     g_trx_type_lookup ||
                     ' not defined for transaction types cross reference';
        print_log_message(l_err_msg);
      WHEN OTHERS THEN
        g_retcode := 1;
        l_err_msg := 'Error : Exception in Pre-validation Procedure. For lookup ' ||
                     g_trx_type_lookup || ': ' || SUBSTR(SQLERRM, 1, 150);
        print_log_message(l_err_msg);
    END;
    -- Check whether tax code cross reference exists
    BEGIN
      SELECT 1
        INTO l_tax_code_map
        FROM apps.fnd_lookup_types_tl flt
       WHERE flt.LANGUAGE = USERENV('LANG')
            --  AND UPPER(flt.lookup_type) = g_tax_code_lookup
         AND flt.lookup_type = g_tax_code_lookup;
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        g_retcode := 1;
        l_err_msg := 'Error : Exception in Pre-validation Procedure. Lookup ' ||
                     g_tax_code_lookup ||
                     ' not defined for tax code cross reference';
        print_log_message(l_err_msg);
      WHEN OTHERS THEN
        g_retcode := 1;
        l_err_msg := 'Error : Exception in Pre-validation Procedure. For lookup ' ||
                     g_tax_code_lookup || ': ' || SUBSTR(SQLERRM, 1, 150);
        print_log_message(l_err_msg);
    END;
    -- Check whether memo line is created for conversion freight
    BEGIN
      SELECT 1
        INTO l_memo_line
        FROM ar_memo_lines_all_tl
       WHERE LANGUAGE = USERENV('LANG')
         AND UPPER(NAME) LIKE '%CONVERSION%FREIGHT%'
         AND ROWNUM = 1;
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        g_retcode := 1;
        l_err_msg := 'Error : Exception in Pre-validation Procedure. Memo line for conversion freight not defined for freight line creation';
        print_log_message(l_err_msg);
      WHEN OTHERS THEN
        g_retcode := 1;
        l_err_msg := 'Error : Exception in Pre-validation Procedure while checking memo line for conversion freight. ' ||
                     SUBSTR(SQLERRM, 1, 150);
        print_log_message(l_err_msg);
    END;
    --ver 1.10 changes start
    /*

          -- Check whether batch source defined for each operating unit

          IF l_ou_map IS NOT NULL

          THEN

             FOR r_ou_rec IN (SELECT hou.organization_id, hou.NAME

                                FROM hr_operating_units hou, fnd_lookup_values flv

                               WHERE TRIM (UPPER (flv.meaning)) = TRIM (UPPER (hou.NAME))

                                 AND flv.LANGUAGE = USERENV ('LANG')

                                 AND flv.enabled_flag = 'Y'

                                 AND UPPER (flv.lookup_type) = g_ou_lookup

                                 AND TRUNC (SYSDATE) BETWEEN TRUNC (NVL (flv.start_date_active, SYSDATE))

                                                         AND TRUNC (NVL (flv.end_date_active, SYSDATE))

                                 AND TRUNC (NVL (hou.date_to, SYSDATE)) >= TRUNC (SYSDATE))

             LOOP

                BEGIN

                   SELECT 1

                     INTO l_batch_source

                     FROM ra_batch_sources_all

                    WHERE UPPER (NAME) = g_batch_source AND org_id = r_ou_rec.organization_id;

                EXCEPTION

                   WHEN NO_DATA_FOUND

                   THEN

                      g_retcode := 1;

                      l_err_msg :=

                            'Error : Exception in Pre-validation Procedure. Batch source '

                         || g_batch_source

                         || ' not defined for operating unit '

                         || r_ou_rec.NAME;

                      print_log_message (l_err_msg);

                   WHEN OTHERS

                   THEN

                      g_retcode := 1;

                      l_err_msg :=

                            'Error : Exception in Pre-validation Procedure. For Batch source'

                         || g_batch_source

                         || ' and operating unit '

                         || r_ou_rec.NAME

                         || ' : '

                         || SUBSTR (SQLERRM, 1, 150);

                      print_log_message (l_err_msg);

                END;

             END LOOP;

          END IF;

    */
    --ver 1.10 changes end
  EXCEPTION
    WHEN OTHERS THEN
      l_err_msg := 'Error : Exception in Pre-validation Procedure. ' ||
                   SUBSTR(SQLERRM, 1, 150);
      print_log_message(l_err_msg);
      g_retcode := 2;
  END pre_validate_invoice;

  --
  -- ========================
  -- Function: CHECK_MANDATORY
  -- =============================================================================
  --   This function is used to perform NULL check on mandatory fields
  -- =============================================================================
  FUNCTION check_mandatory(pin_trx_id       IN NUMBER DEFAULT NULL,
                           pin_cust_trx_id  IN NUMBER DEFAULT NULL,
                           piv_column_value IN VARCHAR2,
                           piv_column_name  IN VARCHAR2,
                           piv_table_name   IN VARCHAR2 DEFAULT 'XXAR_INVOICES_STG')
    RETURN BOOLEAN IS
    l_err_code       VARCHAR2(40);
    l_err_msg        VARCHAR2(2000);
    l_log_ret_status VARCHAR2(50);
    l_log_err_msg    VARCHAR2(2000);
  BEGIN
    IF TRIM(piv_column_value) IS NULL THEN
      l_err_code := 'ETN_AR_MANDATORY_NOT_ENTERED';
      l_err_msg  := 'Error: Mandatory column not entered. ';
      print_log1_message(l_err_msg || piv_column_name);
      g_retcode := 1;
      log_errors(pin_transaction_id      => pin_trx_id,
                 piv_source_column_name  => 'LEGACY_CUSTOMER_TRX_ID',
                 piv_source_column_value => pin_cust_trx_id,
                 piv_source_keyname1     => piv_column_name,
                 piv_source_keyvalue1    => piv_column_value,
                 piv_error_type          => 'ERR_VAL',
                 piv_error_code          => l_err_code,
                 piv_error_message       => l_err_msg,
                 pov_return_status       => l_log_ret_status,
                 piv_source_table        => piv_table_name,
                 pov_error_msg           => l_log_err_msg);
      RETURN TRUE;
    ELSE
      print_log1_message('Mandatory check passed for ' || piv_column_name);
      RETURN FALSE;
    END IF;
  EXCEPTION
    WHEN OTHERS THEN
      g_retcode  := 2;
      l_err_code := 'ETN_AR_PROCEDURE_EXCEPTION';
      l_err_msg  := 'Error while checking mandatory column ';
      log_errors(pin_transaction_id      => pin_trx_id,
                 piv_source_column_name  => 'LEGACY_CUSTOMER_TRX_ID',
                 piv_source_column_value => pin_cust_trx_id,
                 piv_source_keyname1     => piv_column_name,
                 piv_source_keyvalue1    => piv_column_value,
                 piv_error_type          => 'ERR_VAL',
                 piv_error_code          => l_err_code,
                 piv_error_message       => l_err_msg,
                 piv_source_table        => piv_table_name,
                 pov_return_status       => l_log_ret_status,
                 pov_error_msg           => l_log_err_msg);
      RETURN TRUE;
  END check_mandatory;

  --
  -- ========================
  -- Procedure: DUPLICATE_CHECK
  -- =============================================================================
  --   This procedure will check for duplicate records in invoices lines and
  --   distributions table
  -- =============================================================================
  PROCEDURE duplicate_check
  --                    (  p_return_status    OUT    VARCHAR2
    --                   , p_error_message    OUT    VARCHAR2)
   IS
    l_err_code       VARCHAR2(40);
    l_err_msg        VARCHAR2(2000);
    l_log_ret_status VARCHAR2(50);
    l_log_err_msg    VARCHAR2(2000);
  BEGIN
    UPDATE xxar_invoices_stg x1
       SET x1.process_flag = 'E1'
     WHERE x1.batch_id = g_new_batch_id
       AND x1.process_flag = DECODE(g_process_records, 'E', 'E', 'N')
       AND x1.ROWID >
           (SELECT MIN(ROWID)
              FROM xxar_invoices_stg x2
             WHERE NVL(x2.leg_cust_trx_line_id, 0) =
                   NVL(x1.leg_cust_trx_line_id, 0)
               AND x2.leg_customer_trx_id = x1.leg_customer_trx_id);
    --      AND UPPER (x1.leg_operating_unit) = UPPER (NVL (g_leg_operating_unit, x1.leg_operating_unit))
    --         AND UPPER (x1.leg_cust_trx_type_name) = UPPER (NVL (g_leg_trasaction_type, x1.leg_cust_trx_type_name));
    FOR r_dup_err_rec IN (SELECT interface_txn_id,
                                 leg_trx_number,
                                 leg_line_number
                            FROM xxar_invoices_stg
                           WHERE batch_id = g_new_batch_id
                             AND process_flag = 'E1') LOOP
      l_err_code := 'ETN_AR_DUPLICATE_ENTITY';
      l_err_msg  := 'Error : Duplicate records present at invoice lines';
      g_retcode  := 1;
      log_errors(pin_transaction_id      => r_dup_err_rec.interface_txn_id,
                 piv_source_column_name  => 'TRANSACTION_NUMBER',
                 piv_source_column_value => r_dup_err_rec.leg_trx_number,
                 piv_source_keyname1     => 'LEGACY_LINE_NUMBER',
                 piv_source_keyvalue1    => r_dup_err_rec.leg_line_number,
                 piv_error_type          => 'ERR_VAL',
                 piv_error_code          => l_err_code,
                 piv_error_message       => l_err_msg,
                 pov_return_status       => l_log_ret_status,
                 pov_error_msg           => l_log_err_msg);
      UPDATE xxar_invoices_stg
         SET process_flag      = 'E',
             ERROR_TYPE        = 'ERR_VAL',
             last_update_date  = g_sysdate,
             last_updated_by   = g_last_updated_by,
             last_update_login = g_login_id
       WHERE interface_txn_id = r_dup_err_rec.interface_txn_id;
      COMMIT;
    END LOOP;
    UPDATE xxar_invoices_dist_stg x1
       SET x1.process_flag = 'E1'
     WHERE x1.batch_id = g_new_batch_id
       AND x1.process_flag = DECODE(g_process_records, 'E', 'E', 'N')
          --         AND UPPER (x1.leg_org_name) = UPPER (NVL (g_leg_operating_unit, x1.leg_org_name))
       AND x1.ROWID > (SELECT MIN(ROWID)
                         FROM xxar_invoices_dist_stg x2
                        WHERE x2.leg_cust_trx_line_gl_dist_id =
                              x1.leg_cust_trx_line_gl_dist_id
                          AND NVL(x2.leg_cust_trx_line_id, 0) =
                              NVL(x1.leg_cust_trx_line_id, 0));
    FOR r_dup_err_rec IN (SELECT interface_txn_id,
                                 leg_cust_trx_line_gl_dist_id,
                                 leg_account_class
                            FROM xxar_invoices_dist_stg
                           WHERE batch_id = g_new_batch_id
                             AND process_flag = 'E1') LOOP
      l_err_code := 'ETN_AR_DUPLICATE_ENTITY';
      l_err_msg  := 'Error : Duplicate records present at distribution lines';
      g_retcode  := 1;
      log_errors(pin_transaction_id      => r_dup_err_rec.interface_txn_id,
                 piv_source_column_name  => 'LEGACY GL_DIST_ID',
                 piv_source_column_value => r_dup_err_rec.leg_cust_trx_line_gl_dist_id,
                 piv_source_keyname1     => 'ACCOUNT_CLASS',
                 piv_source_keyvalue1    => r_dup_err_rec.leg_account_class,
                 piv_source_table        => 'XXAR_INVOICES_DIST_STG',
                 piv_error_type          => 'ERR_VAL',
                 piv_error_code          => l_err_code,
                 piv_error_message       => l_err_msg,
                 pov_return_status       => l_log_ret_status,
                 pov_error_msg           => l_log_err_msg);
      UPDATE xxar_invoices_stg
         SET process_flag      = 'E',
             ERROR_TYPE        = 'ERR_VAL',
             last_update_date  = g_sysdate,
             last_updated_by   = g_last_updated_by,
             last_update_login = g_login_id
       WHERE interface_txn_id = r_dup_err_rec.interface_txn_id;
      COMMIT;
    END LOOP;
  EXCEPTION
    WHEN OTHERS THEN
      l_err_code := 'ETN_AR_PROCEDURE_EXCEPTION';
      l_err_msg  := 'Unexcepted Error in procedure duplicate_check ' ||
                    SUBSTR(SQLERRM, 1, 150);
      g_retcode  := 2;
      log_errors(piv_error_type    => 'ERR_VAL',
                 piv_source_table  => 'XXAR_INVOICES_DIST_STG',
                 piv_error_code    => l_err_code,
                 piv_error_message => l_err_msg,
                 pov_return_status => l_log_ret_status,
                 pov_error_msg     => l_log_err_msg);
  END duplicate_check;

  /*
  --
  -- ========================
  -- Procedure: VALIDATE_OPERATING_UNIT
  -- =============================================================================
  --   This procedure will validate operating unit
  -- =============================================================================
     PROCEDURE validate_operating_unit (pin_trx_id IN NUMBER, piv_ou_name IN VARCHAR2, pon_ledger_id OUT NUMBER, pov_valid_flag OUT VARCHAR2)
     IS
        l_org_name         fnd_lookup_values.description%TYPE;
        l_err_code         VARCHAR2 (40);
        l_err_msg          VARCHAR2 (2000);
        l_log_ret_status   VARCHAR2 (50);
        l_log_err_msg      VARCHAR2 (2000);
     BEGIN
        -- Check whether legacy to R12 operating unit mapping exists
        BEGIN
           IF piv_ou_name IS NOT NULL
           THEN
              print_log_message ('VALIDATE_OPERATING_UNIT procedure');
              SELECT TRIM (flv.description)
                INTO l_org_name
                FROM fnd_lookup_values flv
               WHERE TRIM (UPPER (flv.meaning)) = TRIM (UPPER (piv_ou_name))
                 AND flv.LANGUAGE = USERENV ('LANG')
                 AND flv.enabled_flag = 'Y'
                 AND UPPER (flv.lookup_type) = g_ou_lookup
                 AND TRUNC (SYSDATE) BETWEEN TRUNC (NVL (flv.start_date_active, SYSDATE)) AND TRUNC (NVL (flv.end_date_active, SYSDATE));
           END IF;
        EXCEPTION
           WHEN NO_DATA_FOUND
           THEN
              l_err_code := 'ETN_AR_OPERATING UNIT_ERROR';
              l_err_msg := 'Error : Cross reference not defined for operating unit';
              pov_valid_flag := 'E';
              log_errors (
                          --   pin_transaction_id           =>  pin_trx_id
                          piv_source_keyname1       => 'Operating Unit',
                          piv_source_keyvalue1      => piv_ou_name,
                          piv_source_column_name          => 'LEGACY_CUSTOMER_TRX_ID',
                          piv_source_column_value         => pin_trx_id,
                          piv_error_type               => 'ERR_VAL',
                          piv_error_code               => l_err_code,
                          piv_error_message            => l_err_msg,
                          pov_return_status            => l_log_ret_status,
                          pov_error_msg                => l_log_err_msg
                         );
           WHEN OTHERS
           THEN
              l_err_code := 'ETN_AR_PROCEDURE_EXCEPTION';
              l_err_msg := 'Error : Error updating staging table for operating unit' || SUBSTR (SQLERRM, 1, 150);
              pov_valid_flag := 'E';
              log_errors (
                          --   pin_transaction_id           =>  pin_trx_id
                          piv_source_keyname1       => 'Operating Unit',
                          piv_source_keyvalue1      => piv_ou_name,
                          piv_source_column_name          => 'LEGACY_CUSTOMER_TRX_ID',
                          piv_source_column_value         => pin_trx_id,
                          piv_error_type               => 'ERR_VAL',
                          piv_error_code               => l_err_code,
                          piv_error_message            => l_err_msg,
                          pov_return_status            => l_log_ret_status,
                          pov_error_msg                => l_log_err_msg
                         );
        END;
        -- Check whether R12 operating unit in mapping table is already setup
        BEGIN
           IF l_org_name IS NOT NULL
           THEN
              print_log_message ('VALIDATE_OPERATING_UNIT procedure');
              SELECT hou.organization_id, hou.NAME, hou.set_of_books_id, gll.currency_code,
                     gll.ledger_id
                INTO g_invoice (g_index).org_id, g_invoice (g_index).ou_name, g_invoice (g_index).set_of_books_id, g_invoice (g_index).currency_code,
                     pon_ledger_id
                FROM apps.hr_operating_units hou, gl_ledgers gll
               WHERE UPPER (hou.NAME) = UPPER (l_org_name) AND hou.set_of_books_id = gll.ledger_id(+)
                     AND TRUNC (NVL (hou.date_to, SYSDATE)) >= TRUNC (SYSDATE);
           END IF;
        EXCEPTION
           WHEN NO_DATA_FOUND
           THEN
              l_err_code := 'ETN_AR_OPERATING UNIT_ERROR';
              l_err_msg := 'Error : Operating unit not setup';
              pov_valid_flag := 'E';
              log_errors (
                          --   pin_transaction_id           =>  pin_trx_id
                          piv_source_keyname1       => 'Operating Unit',
                          piv_source_keyvalue1      => piv_ou_name,
                          piv_source_column_name       => 'LEGACY_CUSTOMER_TRX_ID',
                          piv_source_column_value      => pin_trx_id,
                          piv_error_type               => 'ERR_VAL',
                          piv_error_code               => l_err_code,
                          piv_error_message            => l_err_msg,
                          pov_return_status            => l_log_ret_status,
                          pov_error_msg                => l_log_err_msg
                         );
           WHEN OTHERS
           THEN
              l_err_code := 'ETN_AR_PROCEDURE_EXCEPTION';
              l_err_msg := 'Error : Error fetching R12 operating unit' || SUBSTR (SQLERRM, 1, 150);
              pov_valid_flag := 'E';
              log_errors (
                          -- pin_transaction_id           =>  pin_trx_id
                          piv_source_keyname1       => 'Operating Unit',
                          piv_source_keyvalue1      => piv_ou_name,
                          piv_source_column_name       => 'LEGACY_CUSTOMER_TRX_ID',
                          piv_source_column_value      => pin_trx_id,
                          piv_error_type               => 'ERR_VAL',
                          piv_error_code               => l_err_code,
                          piv_error_message            => l_err_msg,
                          pov_return_status            => l_log_ret_status,
                          pov_error_msg                => l_log_err_msg
                         );
        END;
     END validate_operating_unit;
  */ -- Perf
  /*
  --
  -- ========================
  -- Procedure: VALIDATE_CUSTOMER_DETAILS
  -- =============================================================================
  --   This procedure will validate customer information
  -- =============================================================================
     PROCEDURE validate_customer_details (
        pin_trx_id            IN       NUMBER,
        piv_customer_number   IN       VARCHAR2,
        piv_bill_to_addr      IN       VARCHAR2,
        piv_ship_to_addr      IN       VARCHAR2,
        pin_org_id            IN       NUMBER,
        pov_valid_flag        OUT      VARCHAR2
     )
     IS
        l_err_code         VARCHAR2 (40);
        l_err_msg          VARCHAR2 (2000);
        l_log_ret_status   VARCHAR2 (50);
        l_log_err_msg      VARCHAR2 (2000);
        l_cust_id         NUMBER;
     BEGIN
        print_log1_message ('VALIDATE_CUSTOMER_DETAILS procedure');
        IF piv_customer_number IS NOT NULL
        THEN
           BEGIN
              SELECT hca.cust_account_id
                INTO g_invoice (g_index).system_bill_customer_id
                FROM apps.hz_cust_accounts_all hca
               WHERE hca.orig_system_reference = TRIM (piv_customer_number)
           --AND NVL (hca.org_id, 1) = NVL (pin_org_id, 1)
           AND hca.status = 'A';
           l_cust_id := g_invoice (g_index).system_bill_customer_id;
           EXCEPTION
              WHEN NO_DATA_FOUND
              THEN
                 l_err_code := 'ETN_AR_BILL_CUSTOMER_ERROR';
                 l_err_msg := 'Error : Cross reference not defined for bill to customer';
                 print_log1_message (l_err_msg);
                 pov_valid_flag := 'E';
                 log_errors (
                             -- pin_transaction_id           =>  pin_trx_id
                             piv_source_column_name       => 'LEGACY_CUSTOMER_TRX_ID',
                             piv_source_column_value      => pin_trx_id,
                             piv_source_keyname1          => 'Legacy Customer Number',
                             piv_source_keyvalue1         => piv_customer_number,
                             piv_error_type               => 'ERR_VAL',
                             piv_error_code               => l_err_code,
                             piv_error_message            => l_err_msg,
                             pov_return_status            => l_log_ret_status,
                             pov_error_msg                => l_log_err_msg
                            );
              WHEN OTHERS
              THEN
                 l_err_code := 'ETN_AR_PROCEDURE_EXCEPTION';
                 l_err_msg := 'Error : Error while fetching bill to customer cross reference' || SUBSTR (SQLERRM, 1, 150);
                 print_log1_message (l_err_msg);
                 pov_valid_flag := 'E';
                 log_errors (
                             --  pin_transaction_id           =>  pin_trx_id
                             piv_source_column_name       => 'LEGACY_CUSTOMER_TRX_ID',
                             piv_source_column_value      => pin_trx_id,
                             piv_source_keyname1          => 'Legacy Customer Number',
                             piv_source_keyvalue1         => piv_customer_number,
                             piv_error_type               => 'ERR_VAL',
                             piv_error_code               => l_err_code,
                             piv_error_message            => l_err_msg,
                             pov_return_status            => l_log_ret_status,
                             pov_error_msg                => l_log_err_msg
                            );
           END;
        END IF;
        IF piv_bill_to_addr IS NOT NULL
        THEN
           BEGIN
              SELECT hcas.cust_acct_site_id
                INTO g_invoice (g_index).system_bill_address_id
                FROM apps.hz_cust_acct_sites_all hcas
               , apps.hz_cust_site_uses_all hcsu
               WHERE hcsu.cust_acct_site_id = hcas.cust_acct_site_id
             AND hcas.orig_system_reference = TRIM (piv_bill_to_addr)
             AND NVL (hcas.org_id, 1) = NVL (pin_org_id, 1)
                 AND hcsu.status = 'A'
                 AND hcas.status = 'A'
                 AND hcsu.site_use_code = 'BILL_TO'
                 AND hcas.cust_account_id = l_cust_id;
           EXCEPTION
              WHEN NO_DATA_FOUND
              THEN
                 l_err_code := 'ETN_AR_BILL_TO_SITE_ERROR';
                 l_err_msg := 'Error : Cross reference not defined for bill to site';
                 print_log1_message (l_err_msg);
                 pov_valid_flag := 'E';
                 log_errors (
                             -- pin_transaction_id           =>  pin_trx_id
                             piv_source_column_name       => 'LEGACY_CUSTOMER_TRX_ID',
                             piv_source_column_value      => pin_trx_id,
                             piv_source_keyname1          => 'Legacy Bill to Site code',
                             piv_source_keyvalue1         => piv_bill_to_addr,
                             piv_error_type               => 'ERR_VAL',
                             piv_error_code               => l_err_code,
                             piv_error_message            => l_err_msg,
                             pov_return_status            => l_log_ret_status,
                             pov_error_msg                => l_log_err_msg
                            );
              WHEN OTHERS
              THEN
                 l_err_code := 'ETN_AR_PROCEDURE_EXCEPTION';
                 l_err_msg := 'Error : Error while fetching bill to site cross reference' || SUBSTR (SQLERRM, 1, 150);
                 print_log1_message (l_err_msg);
                 pov_valid_flag := 'E';
                 log_errors (
                             --   pin_transaction_id           =>  pin_trx_id
                             piv_source_column_name       => 'LEGACY_CUSTOMER_TRX_ID',
                             piv_source_column_value      => pin_trx_id,
                             piv_source_keyname1          => 'Legacy Bill to Site code',
                             piv_source_keyvalue1         => piv_bill_to_addr,
                             piv_error_type               => 'ERR_VAL',
                             piv_error_code               => l_err_code,
                             piv_error_message            => l_err_msg,
                             pov_return_status            => l_log_ret_status,
                             pov_error_msg                => l_log_err_msg
                            );
           END;
        END IF;
        IF piv_ship_to_addr IS NOT NULL
        THEN
           BEGIN
              SELECT hcas.cust_acct_site_id
                INTO g_invoice (g_index).system_ship_address_id
                FROM apps.hz_cust_acct_sites_all hcas
               , apps.hz_cust_site_uses_all hcsu
               WHERE hcsu.cust_acct_site_id = hcas.cust_acct_site_id
             AND hcas.orig_system_reference = TRIM (piv_ship_to_addr)
                 AND hcsu.status = 'A'
                 AND hcas.status = 'A'
                 AND hcsu.site_use_code = 'SHIP_TO'
             AND NVL (hcas.org_id, 1) = NVL (pin_org_id, 1)
                 AND hcas.cust_account_id = l_cust_id;
           EXCEPTION
              WHEN NO_DATA_FOUND
              THEN
                 l_err_code := 'ETN_AR_SHIP_TO_SITE_ERROR';
                 l_err_msg := 'Error : Cross reference not defined for ship to site';
                 print_log1_message (l_err_msg);
                 pov_valid_flag := 'E';
                 log_errors (
                             -- pin_transaction_id           =>  pin_trx_id
                             piv_source_column_name       => 'LEGACY_CUSTOMER_TRX_ID',
                             piv_source_column_value      => pin_trx_id,
                             piv_source_keyname1          => 'Legacy Ship to Site code',
                             piv_source_keyvalue1         => piv_ship_to_addr,
                             piv_error_type               => 'ERR_VAL',
                             piv_error_code               => l_err_code,
                             piv_error_message            => l_err_msg,
                             pov_return_status            => l_log_ret_status,
                             pov_error_msg                => l_log_err_msg
                            );
              WHEN OTHERS
              THEN
                 l_err_code := 'ETN_AR_PROCEDURE_EXCEPTION';
                 l_err_msg := 'Error : Error while fetching bill to site cross reference' || SUBSTR (SQLERRM, 1, 150);
                 print_log1_message (l_err_msg);
                 pov_valid_flag := 'E';
                 log_errors (
                             -- pin_transaction_id           =>  pin_trx_id
                             piv_source_column_name       => 'LEGACY_CUSTOMER_TRX_ID',
                             piv_source_column_value      => pin_trx_id,
                             piv_source_keyname1          => 'Legacy Ship to Site code',
                             piv_source_keyvalue1         => piv_ship_to_addr,
                             piv_error_type               => 'ERR_VAL',
                             piv_error_code               => l_err_code,
                             piv_error_message            => l_err_msg,
                             pov_return_status            => l_log_ret_status,
                             pov_error_msg                => l_log_err_msg
                            );
           END;
        ELSE
           g_invoice (g_index).system_ship_address_id := NULL;
        END IF;
  --      p_return_status := fnd_api.g_ret_sts_success;
     EXCEPTION
        WHEN OTHERS
        THEN
           l_err_code := 'ETN_AR_PROCEDURE_EXCEPTION';
           l_err_msg := 'Error : Error while validating customer information' || SUBSTR (SQLERRM, 1, 150);
           pov_valid_flag := 'E';
           log_errors (
  --           pin_transaction_id           =>  pin_trx_id
                       piv_error_type               => 'ERR_VAL',
                       piv_source_column_name       => 'LEGACY_CUSTOMER_TRX_ID',
                       piv_source_column_value      => pin_trx_id,
                       piv_error_code               => l_err_code,
                       piv_error_message            => l_err_msg,
                       pov_return_status            => l_log_ret_status,
                       pov_error_msg                => l_log_err_msg
                      );
     END validate_customer_details;
  */ -- Perf
  /*
  --
  -- ========================
  -- Procedure: VALIDATE_PAYMENT_TERMS
  -- =============================================================================
  --   This procedure will validate payment term information
  -- =============================================================================
     PROCEDURE validate_payment_terms (pin_trx_id IN NUMBER, piv_term_name IN VARCHAR2, pov_valid_flag OUT VARCHAR2)
     IS
        l_err_code         VARCHAR2 (40);
        l_err_msg          VARCHAR2 (2000);
        l_log_ret_status   VARCHAR2 (50);
        l_log_err_msg      VARCHAR2 (2000);
        l_term_name        ra_terms_tl.NAME%TYPE;
     BEGIN
        -- Check whether legacy to R12 payment term mapping exists
        BEGIN
           IF piv_term_name IS NOT NULL
           THEN
          IF g_invoice (g_index).trx_type = 'CM'
          THEN
              l_err_code := 'ETN_AR_PMT_TERM_ERROR';
              l_err_msg := 'Error : Payment term is NOT NULL for Credit memo transaction';
              pov_valid_flag := 'E';
              log_errors (
                  --  pin_transaction_id           =>  pin_trx_id
                  piv_source_column_name          => 'LEGACY_CUSTOMER_TRX_ID',
                  piv_source_column_value         => pin_trx_id,
                  piv_source_keyname1          => 'Legacy Payment Term',
                  piv_source_keyvalue1         => piv_term_name,
                  piv_error_type               => 'ERR_VAL',
                  piv_error_code               => l_err_code,
                  piv_error_message            => l_err_msg,
                  pov_return_status            => l_log_ret_status,
                  pov_error_msg                => l_log_err_msg
                     );
          ELSE
              print_log_message ('validate_payment_terms procedure');
              SELECT TRIM (flv.description)
                INTO l_term_name
                FROM fnd_lookup_values flv
               WHERE TRIM (UPPER (flv.meaning)) = TRIM (UPPER (piv_term_name))
                 AND flv.LANGUAGE = USERENV ('LANG')
                 AND flv.enabled_flag = 'Y'
                 AND UPPER (flv.lookup_type) = g_pmt_term_lookup
                 AND TRUNC (SYSDATE) BETWEEN TRUNC (NVL (flv.start_date_active, SYSDATE)) AND TRUNC (NVL (flv.end_date_active, SYSDATE));
          END IF;
           ELSIF g_invoice (g_index).trx_type <> 'CM' THEN
              l_err_code := 'ETN_AR_PMT_TERM_ERROR';
              l_err_msg := 'Error : Payment term is NULL for the transaction';
              pov_valid_flag := 'E';
              log_errors (
                          --  pin_transaction_id           =>  pin_trx_id
                          piv_source_column_name          => 'LEGACY_CUSTOMER_TRX_ID',
                          piv_source_column_value         => pin_trx_id,
                          piv_source_keyname1          => 'Legacy Payment Term',
                          piv_source_keyvalue1         => piv_term_name,
                          piv_error_type               => 'ERR_VAL',
                          piv_error_code               => l_err_code,
                          piv_error_message            => l_err_msg,
                          pov_return_status            => l_log_ret_status,
                          pov_error_msg                => l_log_err_msg
                         );
       END IF;
        EXCEPTION
           WHEN NO_DATA_FOUND
           THEN
              l_err_code := 'ETN_AR_PMT_TERM_ERROR';
              l_err_msg := 'Error : Cross reference not defined for payment term';
              pov_valid_flag := 'E';
              log_errors (
                          --  pin_transaction_id           =>  pin_trx_id
                          piv_source_column_name          => 'LEGACY_CUSTOMER_TRX_ID',
                          piv_source_column_value         => pin_trx_id,
                          piv_source_keyname1          => 'Legacy Payment Term',
                          piv_source_keyvalue1         => piv_term_name,
                          piv_error_type               => 'ERR_VAL',
                          piv_error_code               => l_err_code,
                          piv_error_message            => l_err_msg,
                          pov_return_status            => l_log_ret_status,
                          pov_error_msg                => l_log_err_msg
                         );
           WHEN OTHERS
           THEN
              l_err_code := 'ETN_AR_PROCEDURE_EXCEPTION';
              l_err_msg := 'Error : Error updating staging table for payment term' || SUBSTR (SQLERRM, 1, 150);
              pov_valid_flag := 'E';
              log_errors (
  --               pin_transaction_id           =>  pin_trx_id
                          piv_error_type               => 'ERR_VAL',
                          piv_source_column_name          => 'LEGACY_CUSTOMER_TRX_ID',
                          piv_source_column_value         => pin_trx_id,
                          piv_source_keyname1          => 'Legacy Payment Term',
                          piv_source_keyvalue1         => piv_term_name,
                          piv_error_code               => l_err_code,
                          piv_error_message            => l_err_msg,
                          pov_return_status            => l_log_ret_status,
                          pov_error_msg                => l_log_err_msg
                         );
        END;
        -- Check whether R12 payment term in mapping has been setup
        BEGIN
           IF l_term_name IS NOT NULL
           THEN
              SELECT rtm.term_id
                INTO g_invoice (g_index).term_id
                FROM ra_terms_tl rtm
               WHERE UPPER (rtm.NAME) = UPPER (l_term_name) AND rtm.LANGUAGE = USERENV ('LANG');
           END IF;
        EXCEPTION
           WHEN NO_DATA_FOUND
           THEN
              l_err_code := 'ETN_AR_PMT_TERM_ERROR';
              l_err_msg := 'Error : Payment term not setup in R12';
              pov_valid_flag := 'E';
              log_errors (
                          --  pin_transaction_id           =>  pin_trx_id
                          piv_source_keyname1       => 'R12 Payment Term',
                          piv_source_keyvalue1      => l_term_name,
                          piv_source_column_name          => 'LEGACY_CUSTOMER_TRX_ID',
                          piv_source_column_value         => pin_trx_id,
                          piv_error_type               => 'ERR_VAL',
                          piv_error_code               => l_err_code,
                          piv_error_message            => l_err_msg,
                          pov_return_status            => l_log_ret_status,
                          pov_error_msg                => l_log_err_msg
                         );
           WHEN OTHERS
           THEN
              l_err_code := 'ETN_AR_PROCEDURE_EXCEPTION';
              l_err_msg := 'Error : Error fetching R12 Payment term' || SUBSTR (SQLERRM, 1, 150);
              pov_valid_flag := 'E';
              log_errors (
                          --  pin_transaction_id           =>  pin_trx_id
                          piv_source_keyname1       => 'R12 Payment Term',
                          piv_source_keyvalue1      => l_term_name,
                          piv_source_column_name          => 'LEGACY_CUSTOMER_TRX_ID',
                          piv_source_column_value         => pin_trx_id,
                          piv_error_type               => 'ERR_VAL',
                          piv_error_code               => l_err_code,
                          piv_error_message            => l_err_msg,
                          pov_return_status            => l_log_ret_status,
                          pov_error_msg                => l_log_err_msg
                         );
        END;
     EXCEPTION
        WHEN OTHERS
        THEN
           l_err_code := 'ETN_AR_PROCEDURE_EXCEPTION';
           l_err_msg := 'Error : Error fetching R12 Payment term' || SUBSTR (SQLERRM, 1, 150);
           pov_valid_flag := 'E';
           log_errors (
                       -- pin_transaction_id           =>  pin_trx_id
                       piv_source_keyname1       => 'Legacy Payment Term',
                       piv_source_keyvalue1      => piv_term_name,
                       piv_error_type               => 'ERR_VAL',
                       piv_source_column_name          => 'LEGACY_CUSTOMER_TRX_ID',
                       piv_source_column_value         => pin_trx_id,
                       piv_error_code               => l_err_code,
                       piv_error_message            => l_err_msg,
                       pov_return_status            => l_log_ret_status,
                       pov_error_msg                => l_log_err_msg
                      );
     END validate_payment_terms;
  */ -- Perf
  /*
  --
  -- ========================
  -- Procedure: VALIDATE_TRX_TYPE
  -- =============================================================================
  --   This procedure will validate transaction type
  -- =============================================================================
     PROCEDURE validate_trx_type (pin_trx_id IN NUMBER, piv_trx_type IN VARCHAR2, piv_source_name IN VARCHAR2, pov_valid_flag OUT VARCHAR2)
     IS
        l_err_code         VARCHAR2 (40);
        l_err_msg          VARCHAR2 (2000);
        l_log_ret_status   VARCHAR2 (50);
        l_log_err_msg      VARCHAR2 (2000);
        l_trx_type         ra_cust_trx_types_all.NAME%TYPE;
     BEGIN
        -- Check whether legacy to R12 transaction type mapping exists
        BEGIN
           IF piv_trx_type IS NOT NULL
           THEN
              print_log_message ('validate_trx_type procedure');
              SELECT TRIM (flv.description)
                INTO l_trx_type
                FROM fnd_lookup_values flv
               WHERE TRIM (UPPER (flv.meaning)) = TRIM (UPPER (piv_trx_type))
                 AND flv.LANGUAGE = USERENV ('LANG')
                 AND flv.enabled_flag = 'Y'
                 AND UPPER (flv.lookup_type) = g_trx_type_lookup
                 AND TRUNC (SYSDATE) BETWEEN TRUNC (NVL (flv.start_date_active, SYSDATE)) AND TRUNC (NVL (flv.end_date_active, SYSDATE));
           END IF;
        EXCEPTION
           WHEN NO_DATA_FOUND
           THEN
              l_err_code := 'ETN_AR_TRX_TYPE_ERROR';
              l_err_msg := 'Error : Cross reference not defined for transaction type';
              pov_valid_flag := 'E';
              log_errors (
                          --   pin_transaction_id           =>  pin_trx_id
                          piv_source_column_name          => 'LEGACY_CUSTOMER_TRX_ID',
                          piv_source_column_value         => pin_trx_id,
                          piv_source_keyname1       => 'Legacy Transaction type',
                          piv_source_keyvalue1      => piv_trx_type,
                          piv_error_type               => 'ERR_VAL',
                          piv_error_code               => l_err_code,
                          piv_error_message            => l_err_msg,
                          pov_return_status            => l_log_ret_status,
                          pov_error_msg                => l_log_err_msg
                         );
           WHEN OTHERS
           THEN
              l_err_code := 'ETN_AR_PROCEDURE_EXCEPTION';
              l_err_msg := 'Error : Error deriving transaction type' || SUBSTR (SQLERRM, 1, 150);
              pov_valid_flag := 'E';
              log_errors (
                          -- pin_transaction_id           =>  pin_trx_id
                          piv_source_column_name          => 'LEGACY_CUSTOMER_TRX_ID',
                          piv_source_column_value         => pin_trx_id,
                          piv_source_keyname1       => 'Legacy Transaction type',
                          piv_source_keyvalue1      => piv_trx_type,
                          piv_error_type               => 'ERR_VAL',
                          piv_error_code               => l_err_code,
                          piv_error_message            => l_err_msg,
                          pov_return_status            => l_log_ret_status,
                          pov_error_msg                => l_log_err_msg
                         );
        END;
        -- Check whether R12 transaction type in mapping is already setup
        BEGIN
           IF l_trx_type IS NOT NULL
           THEN
              SELECT rct.cust_trx_type_id, rct.TYPE
                INTO g_invoice (g_index).transaction_type_id, g_invoice (g_index).trx_type
                FROM ra_cust_trx_types rct
               WHERE UPPER (rct.NAME) = UPPER (l_trx_type)
                 AND TRUNC (SYSDATE) BETWEEN TRUNC (NVL (rct.start_date, SYSDATE)) AND TRUNC (NVL (rct.end_date, SYSDATE));
              IF g_invoice (g_index).trx_type IN ('CB')
              THEN
              l_err_code := 'ETN_AR_TRX_TYPE_ERROR';
              l_err_msg := 'Error : Invalid transaction type: Chargeback ';
                 pov_valid_flag := 'E';
                 log_errors (
                             -- pin_transaction_id           =>  pin_trx_id
                             piv_source_keyname1       => 'R12 Transaction type',
                             piv_source_keyvalue1      => l_trx_type,
                             piv_error_type               => 'ERR_VAL',
                             piv_source_column_name          => 'LEGACY_CUSTOMER_TRX_ID',
                             piv_source_column_value         => pin_trx_id,
                             piv_error_code               => l_err_code,
                             piv_error_message            => l_err_msg,
                             pov_return_status            => l_log_ret_status,
                             pov_error_msg                => l_log_err_msg
                            );
              END IF;
              IF g_invoice (g_index).trx_type = 'CM' AND UPPER (piv_source_name) = 'NAFSC'
              THEN
              l_err_code := 'ETN_AR_TRX_TYPE_ERROR';
              l_err_msg := 'Error : For NAFSC, Credit memos are not allowed';
                 pov_valid_flag := 'E';
                 log_errors (
                             --   pin_transaction_id           =>  pin_trx_id
                             piv_source_keyname1       => 'R12 Transaction type',
                             piv_source_keyvalue1      => l_trx_type,
                             piv_error_type               => 'ERR_VAL',
                             piv_source_column_name          => 'LEGACY_CUSTOMER_TRX_ID',
                             piv_source_column_value         => pin_trx_id,
                             piv_error_code               => l_err_code,
                             piv_error_message            => l_err_msg,
                             pov_return_status            => l_log_ret_status,
                             pov_error_msg                => l_log_err_msg
                            );
              END IF;
           END IF;
        EXCEPTION
           WHEN NO_DATA_FOUND
           THEN
              l_err_code := 'ETN_AR_TRX_TYPE_ERROR';
              l_err_msg := 'Error : Transaction type not setup in R12';
              pov_valid_flag := 'E';
              log_errors (
                          --       pin_transaction_id           =>  pin_trx_id
                             piv_source_keyname1       => 'R12 Transaction type',
                             piv_source_keyvalue1      => l_trx_type,
                             piv_error_type               => 'ERR_VAL',
                             piv_source_column_name          => 'LEGACY_CUSTOMER_TRX_ID',
                             piv_source_column_value         => pin_trx_id,
                          piv_error_code               => l_err_code,
                          piv_error_message            => l_err_msg,
                          pov_return_status            => l_log_ret_status,
                          pov_error_msg                => l_log_err_msg
                         );
           WHEN OTHERS
           THEN
              l_err_code := 'ETN_AR_PROCEDURE_EXCEPTION';
              l_err_msg := 'Error : Error fetching R12 Transaction type' || SUBSTR (SQLERRM, 1, 150);
              pov_valid_flag := 'E';
              log_errors (
                          --       pin_transaction_id           =>  pin_trx_id
                             piv_source_keyname1       => 'R12 Transaction type',
                             piv_source_keyvalue1      => l_trx_type,
                             piv_error_type               => 'ERR_VAL',
                             piv_source_column_name          => 'LEGACY_CUSTOMER_TRX_ID',
                             piv_source_column_value         => pin_trx_id,
                          piv_error_code               => l_err_code,
                          piv_error_message            => l_err_msg,
                          pov_return_status            => l_log_ret_status,
                          pov_error_msg                => l_log_err_msg
                         );
        END;
     EXCEPTION
        WHEN OTHERS
        THEN
           l_err_code := 'ETN_AR_PROCEDURE_EXCEPTION';
           l_err_msg := 'Error : Error fetching R12 transaction type' || SUBSTR (SQLERRM, 1, 150);
           pov_valid_flag := 'E';
           log_errors (
  --           pin_transaction_id           =>  pin_trx_id
                       piv_source_keyname1       => 'Legacy transaction type',
                       piv_source_keyvalue1      => piv_trx_type,
                       piv_error_type               => 'ERR_VAL',
                       piv_error_code               => l_err_code,
                       piv_source_column_name          => 'LEGACY_CUSTOMER_TRX_ID',
                       piv_source_column_value         => pin_trx_id,
                       piv_error_message            => l_err_msg,
                       pov_return_status            => l_log_ret_status,
                       pov_error_msg                => l_log_err_msg
                      );
     END validate_trx_type;
  */ --perf
  /*
  --
  -- ========================
  -- Procedure: VALIDATE_GL_PERIOD
  -- =============================================================================
  --   This procedure will validate gl period
  -- =============================================================================
     PROCEDURE validate_gl_period (pin_trx_id IN NUMBER, piv_gl_date IN DATE, pin_ledger_id IN NUMBER, piv_period_name IN VARCHAR2, pov_valid_flag OUT VARCHAR2)
     IS
        l_gl_status        gl_period_statuses.closing_status%TYPE;
        l_err_code         VARCHAR2 (40);
        l_err_msg          VARCHAR2 (2000);
        l_log_ret_status   VARCHAR2 (50);
        l_log_err_msg      VARCHAR2 (2000);
     BEGIN
        -- Check is gl period is open for gl date
        IF piv_gl_date IS NOT NULL
        THEN
           print_log_message ('VALIDATE_GL_PERIOD procedure');
           SELECT NVL (gps.closing_status, 'X')
             INTO l_gl_status
             FROM gl_periods glp, gl_period_statuses gps
            WHERE UPPER (glp.period_name) = UPPER (gps.period_name)
              AND glp.period_set_name = piv_period_name--'ETN Corp Calend'
              AND piv_gl_date BETWEEN glp.start_date AND glp.end_date
              AND gps.application_id = (SELECT fap.application_id
                                          FROM fnd_application_vl fap
                                         WHERE fap.application_short_name = 'SQLGL')
              AND ledger_id = pin_ledger_id;
           IF l_gl_status <> 'O'
           THEN
              l_err_code := 'ETN_AR_GL_PERIOD_ERROR';
              l_err_msg := 'Error : GL Period is not open for GL date ' || piv_gl_date;
              pov_valid_flag := 'E';
              log_errors (
                          --   pin_transaction_id           =>  pin_trx_id
                          piv_source_keyname1       => 'GL Period date',
                          piv_source_keyvalue1      => piv_gl_date,
                          piv_source_column_name          => 'LEGACY_CUSTOMER_TRX_ID',
                          piv_source_column_value         => pin_trx_id,
                          piv_error_type               => 'ERR_VAL',
                          piv_error_code               => l_err_code,
                          piv_error_message            => l_err_msg,
                          pov_return_status            => l_log_ret_status,
                          pov_error_msg                => l_log_err_msg
                         );
           END IF;
        END IF;
     EXCEPTION
        WHEN NO_DATA_FOUND
        THEN
           l_err_code := 'ETN_AR_GL_PERIOD_ERROR';
           l_err_msg := 'GL Period is not defined for GL date ' || piv_gl_date;
           pov_valid_flag := 'E';
           log_errors (
                       --   pin_transaction_id           =>  pin_trx_id
                          piv_source_keyname1       => 'GL Period date',
                          piv_source_keyvalue1      => piv_gl_date,
                          piv_source_column_name          => 'LEGACY_CUSTOMER_TRX_ID',
                          piv_source_column_value         => pin_trx_id,
                       piv_error_type               => 'ERR_VAL',
                       piv_error_code               => l_err_code,
                       piv_error_message            => l_err_msg,
                       pov_return_status            => l_log_ret_status,
                       pov_error_msg                => l_log_err_msg
                      );
        WHEN OTHERS
        THEN
           l_err_code := 'ETN_AR_PROCEDURE_EXCEPTION';
           l_err_msg := 'Error : Error validating gl period ' || piv_gl_date || SUBSTR (SQLERRM, 1, 150);
           pov_valid_flag := 'E';
           log_errors (
  --           pin_transaction_id           =>  pin_trx_id
                          piv_source_keyname1       => 'GL Period date',
                          piv_source_keyvalue1      => piv_gl_date,
                          piv_source_column_name          => 'LEGACY_CUSTOMER_TRX_ID',
                          piv_source_column_value         => pin_trx_id,
                       piv_error_type               => 'ERR_VAL',
                       piv_error_code               => l_err_code,
                       piv_error_message            => l_err_msg,
                       pov_return_status            => l_log_ret_status,
                       pov_error_msg                => l_log_err_msg
                      );
     END validate_gl_period;
  */ --perf
  --
  -- ========================
  -- Procedure: VALIDATE_AMOUNT
  -- =============================================================================
  --   This procedure will validate amount
  -- =============================================================================
  PROCEDURE validate_amount(pin_amount           IN NUMBER,
                            piv_trx_type         IN VARCHAR2,
                            pin_interface_txn_id IN NUMBER,
                            pov_valid_flag       OUT VARCHAR2) IS
    l_err_code       VARCHAR2(40);
    l_err_msg        VARCHAR2(2000);
    l_log_ret_status VARCHAR2(50);
    l_log_err_msg    VARCHAR2(2000);
  BEGIN
    IF pin_amount IS NOT NULL THEN
      print_log_message('validate_amount procedure');
      -- Check whether amount is not negative for Invoices and debit memos
      IF piv_trx_type IN ('INV', 'DM') AND pin_amount < 0 THEN
        g_retcode      := 1;
        l_err_code     := 'ETN_AR_TRX_AMOUNT_ERROR';
        l_err_msg      := 'Transaction is of Invoice or Debit Memo but amount is less than 0';
        pov_valid_flag := 'E';
        log_errors(pin_transaction_id      => pin_interface_txn_id,
                   piv_source_keyname1     => 'Transaction Amount',
                   piv_source_keyvalue1    => pin_amount,
                   piv_source_column_name  => 'INTERFACE_TXN_ID',
                   piv_source_column_value => pin_interface_txn_id,
                   piv_error_type          => 'ERR_VAL',
                   piv_error_code          => l_err_code,
                   piv_error_message       => l_err_msg,
                   pov_return_status       => l_log_ret_status,
                   pov_error_msg           => l_log_err_msg);
        -- Check whether amount is not positive for credit memos
      ELSIF piv_trx_type IN ('CM') AND pin_amount > 0 THEN
        l_err_code     := 'ETN_AR_TRX_AMOUNT_ERROR';
        l_err_msg      := 'Transaction is Credit Memo but amount is more than 0';
        pov_valid_flag := 'E';
        g_retcode      := 1;
        log_errors(pin_transaction_id      => pin_interface_txn_id,
                   piv_source_keyname1     => 'Transaction Amount',
                   piv_source_keyvalue1    => pin_amount,
                   piv_source_column_name  => 'INTERFACE_TXN_ID',
                   piv_source_column_value => pin_interface_txn_id,
                   piv_error_type          => 'ERR_VAL',
                   piv_error_code          => l_err_code,
                   piv_error_message       => l_err_msg,
                   pov_return_status       => l_log_ret_status,
                   pov_error_msg           => l_log_err_msg);
      END IF;
    END IF;
  EXCEPTION
    WHEN OTHERS THEN
      l_err_code     := 'ETN_AR_PROCEDURE_EXCEPTION';
      l_err_msg      := 'Error : Error validating tranasction amount for interface transaction id ' ||
                        pin_interface_txn_id || SUBSTR(SQLERRM, 1, 150);
      g_retcode      := 2;
      pov_valid_flag := 'E';
      log_errors(pin_transaction_id      => pin_interface_txn_id,
                 piv_source_keyname1     => 'Transaction Amount',
                 piv_source_keyvalue1    => pin_amount,
                 piv_error_type          => 'ERR_VAL',
                 piv_source_column_name  => 'INTERFACE_TXN_ID',
                 piv_source_column_value => pin_interface_txn_id,
                 piv_error_code          => l_err_code,
                 piv_error_message       => l_err_msg,
                 pov_return_status       => l_log_ret_status,
                 pov_error_msg           => l_log_err_msg);
  END validate_amount;

  /*
  --
  -- ========================
  -- Procedure: VALIDATE_TAX
  -- =============================================================================
  --   This procedure will validate tax
  -- =============================================================================
     PROCEDURE validate_tax (
        pin_trx_id                  IN       NUMBER,
        piv_tax_code                IN       VARCHAR2,
        pin_org_id                  IN       NUMBER,
        pov_valid_flag              OUT      VARCHAR2,
        pov_tax_code_r12            OUT      VARCHAR2,
        pov_tax_regime_code         OUT      VARCHAR2,
        pov_tax_rate_code           OUT      VARCHAR2,
        pov_tax                     OUT      VARCHAR2,
        pov_tax_status_code         OUT      VARCHAR2,
        pov_tax_jurisdiction_code   OUT      VARCHAR2
     )
     IS
        l_err_code         VARCHAR2 (40);
        l_err_msg          VARCHAR2 (2000);
        l_log_ret_status   VARCHAR2 (50);
        l_log_err_msg      VARCHAR2 (2000);
        l_tax_code_r12     fnd_lookup_values.description%TYPE;
     BEGIN
        BEGIN
           SELECT flv.description
             INTO l_tax_code_r12
             FROM apps.fnd_lookup_values flv
            WHERE TRIM (UPPER (flv.meaning)) = TRIM (UPPER (piv_tax_code))
              AND flv.enabled_flag = 'Y'
              AND UPPER (flv.lookup_type) = g_tax_code_lookup
              AND TRUNC (SYSDATE) BETWEEN TRUNC (NVL (flv.start_date_active, SYSDATE))
                                      AND TRUNC (NVL (flv.end_date_active, SYSDATE))
              AND flv.LANGUAGE = USERENV ('LANG');
        EXCEPTION
           WHEN NO_DATA_FOUND
           THEN
              l_err_code := 'ETN_AR_TAX_ERROR';
              l_err_msg := 'Error : Cross reference not defined for tax code';
              pov_valid_flag := 'E';
              log_errors (
                          --      pin_transaction_id           =>  pin_trx_id
                          piv_source_column_name       => 'LEGACY_CUSTOMER_TRX_ID',
                          piv_source_column_value      => pin_trx_id,
                          piv_source_keyname1          => 'Legacy Tax code',
                          piv_source_keyvalue1         => piv_tax_code,
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
                        'Error : Error deriving R12 tax code in mapping ' || SUBSTR (SQLERRM, 1, 150);
              pov_valid_flag := 'E';
              log_errors (
                          --      pin_transaction_id           =>  pin_trx_id
                          piv_source_column_name       => 'LEGACY_CUSTOMER_TRX_ID',
                          piv_source_column_value      => pin_trx_id,
                          piv_source_keyname1          => 'Legacy Tax code',
                          piv_source_keyvalue1         => piv_tax_code,
                          piv_error_type               => 'ERR_VAL',
                          piv_error_code               => l_err_code,
                          piv_error_message            => l_err_msg,
                          pov_return_status            => l_log_ret_status,
                          pov_error_msg                => l_log_err_msg
                         );
        END;
        BEGIN
  /*         SELECT zrb.tax, zrb.tax_regime_code, zrb.tax_rate_code, zrb.tax, zrb.tax_status_code, zrb.tax_jurisdiction_code
             INTO pov_tax_code_r12, pov_tax_regime_code, pov_tax_rate_code, pov_tax, pov_tax_status_code, pov_tax_jurisdiction_code
             FROM zx_rates_b zrb, zx_regimes_b zb
            WHERE zb.tax_regime_code = zrb.tax_regime_code
              AND zrb.tax = l_tax_code_r12
          AND tax_rate_code like '%AR%'
              AND TRUNC (SYSDATE) BETWEEN TRUNC (NVL (zb.effective_from, SYSDATE)) AND TRUNC (NVL (zb.effective_to, SYSDATE));
           SELECT DISTINCT zrb.tax, zrb.tax_regime_code, zrb.tax_rate_code, zrb.tax,
                           zrb.tax_status_code, zrb.tax_jurisdiction_code
                      INTO pov_tax_code_r12, pov_tax_regime_code, pov_tax_rate_code, pov_tax,
                           pov_tax_status_code, pov_tax_jurisdiction_code
                      FROM zx_accounts za,
                           hr_operating_units hrou,
                           gl_ledgers gl,
                           fnd_id_flex_structures fifs,
                           zx_rates_b zrb,
                           zx_regimes_b zb
                     WHERE za.internal_organization_id = hrou.organization_id
                       AND gl.ledger_id = za.ledger_id
                       AND fifs.application_id = (SELECT fap.application_id
                                                    FROM fnd_application_vl fap
                                                   WHERE fap.application_short_name = 'SQLGL')
                       AND fifs.id_flex_code = 'GL#'
                       AND fifs.id_flex_num = gl.chart_of_accounts_id
                       AND zrb.tax_rate_id = za.tax_account_entity_id
                       AND za.tax_account_entity_code = 'RATES'
                       AND zrb.tax_rate_code = l_tax_code_r12
                       AND hrou.organization_id = pin_org_id
                       AND TRUNC (SYSDATE) BETWEEN TRUNC (NVL (zb.effective_from, SYSDATE))
                                               AND TRUNC (NVL (zb.effective_to, SYSDATE));
        EXCEPTION
           WHEN NO_DATA_FOUND
           THEN
              l_err_code := 'ETN_AR_TAX_ERROR';
              l_err_msg := 'Error : R12 set up not done for tax code';
              pov_valid_flag := 'E';
              log_errors (
                          --      pin_transaction_id           =>  pin_trx_id
                          piv_source_keyname1          => 'R12 Tax code',
                          piv_source_keyvalue1         => l_tax_code_r12,
                          piv_source_column_name       => 'LEGACY_CUSTOMER_TRX_ID',
                          piv_source_column_value      => pin_trx_id,
                          piv_error_type               => 'ERR_VAL',
                          piv_error_code               => l_err_code,
                          piv_error_message            => l_err_msg,
                          pov_return_status            => l_log_ret_status,
                          pov_error_msg                => l_log_err_msg
                         );
           WHEN OTHERS
           THEN
              l_err_code := 'ETN_AR_PROCEDURE_EXCEPTION';
              l_err_msg := 'Error : Error validating tax ' || SUBSTR (SQLERRM, 1, 150);
              pov_valid_flag := 'E';
              log_errors (
                          --pin_transaction_id           =>  pin_trx_id
                          piv_source_keyname1          => 'R12 Tax code',
                          piv_source_keyvalue1         => l_tax_code_r12,
                          piv_source_column_name       => 'LEGACY_CUSTOMER_TRX_ID',
                          piv_source_column_value      => pin_trx_id,
                          piv_error_type               => 'ERR_VAL',
                          piv_error_code               => l_err_code,
                          piv_error_message            => l_err_msg,
                          pov_return_status            => l_log_ret_status,
                          pov_error_msg                => l_log_err_msg
                         );
        END;
     END validate_tax;
  */ -- perf
  /*
  --
  -- ========================
  -- Procedure: VALIDATE_CURRENCY
  -- =============================================================================
  --   This procedure will validate currency
  -- =============================================================================
     PROCEDURE validate_currency (piv_in_currency IN VARCHAR2, pin_trx_id IN NUMBER, pov_valid_flag OUT VARCHAR2)
     IS
        l_curr_code        NUMBER;
        l_err_code         VARCHAR2 (40);
        l_err_msg          VARCHAR2 (2000);
        l_log_ret_status   VARCHAR2 (50);
        l_log_err_msg      VARCHAR2 (2000);
  -- Check whether currency is setup
     BEGIN
        print_log1_message ('VALIDATE_CURRENCY procedure');
        SELECT 1
          INTO l_curr_code
          FROM fnd_currencies fc
         WHERE fc.currency_code = piv_in_currency
           AND fc.enabled_flag = 'Y'
           AND fc.currency_flag = 'Y'
           AND TRUNC (SYSDATE) BETWEEN TRUNC (NVL (fc.start_date_active, SYSDATE)) AND TRUNC (NVL (fc.end_date_active, SYSDATE));
     EXCEPTION
        WHEN NO_DATA_FOUND
        THEN
           l_err_code := 'ETN_AR_CURRENCY_ERROR';
           l_err_msg := 'Error : Currency not found in R12 ';
           print_log1_message (l_err_msg || piv_in_currency);
           pov_valid_flag := 'E';
           log_errors (
  --          pin_transaction_id           =>  pin_trx_id
                       piv_source_column_name       => 'LEGACY_CUSTOMER_TRX_ID',
                       piv_source_column_value      => pin_trx_id,
                       piv_source_keyname1          => 'Legacy currency code',
                       piv_source_keyvalue1         => piv_in_currency,
                       piv_error_type               => 'ERR_VAL',
                       piv_error_code               => l_err_code,
                       piv_error_message            => l_err_msg,
                       pov_return_status            => l_log_ret_status,
                       pov_error_msg                => l_log_err_msg
                      );
        WHEN OTHERS
        THEN
           l_err_code := 'ETN_AR_PROCEDURE_EXCEPTION';
           l_err_msg := 'Error : Error validating currency ' || SUBSTR (SQLERRM, 1, 150);
           print_log1_message (l_err_msg || piv_in_currency);
           pov_valid_flag := 'E';
           log_errors (
  --          pin_transaction_id           =>  pin_trx_id
                       piv_source_column_name       => 'LEGACY_CUSTOMER_TRX_ID',
                       piv_source_column_value      => pin_trx_id,
                       piv_source_keyname1          => 'Legacy currency code',
                       piv_source_keyvalue1         => piv_in_currency,
                       piv_error_type               => 'ERR_VAL',
                       piv_error_code               => l_err_code,
                       piv_error_message            => l_err_msg,
                       pov_return_status            => l_log_ret_status,
                       pov_error_msg                => l_log_err_msg
                      );
     END validate_currency;
  */ -- Perf
  --
  -- ========================
  -- Procedure: VALIDATE_BATCH_SOURCE
  -- =============================================================================
  --   This procedure will validate batch source
  -- =============================================================================
  PROCEDURE validate_batch_source(pin_org_id     IN NUMBER,
                                  pov_valid_flag OUT VARCHAR2) IS
    l_batch_source   NUMBER;
    l_err_code       VARCHAR2(40);
    l_err_msg        VARCHAR2(2000);
    l_log_ret_status VARCHAR2(50);
    l_log_err_msg    VARCHAR2(2000);
    -- Check whether batch source 'CONVERSION' is setup
  BEGIN
    SELECT 1
      INTO l_batch_source
      FROM ra_batch_sources_all
     WHERE NAME = g_batch_source
       AND org_id = pin_org_id;
  EXCEPTION
    WHEN NO_DATA_FOUND THEN
      l_err_code     := 'ETN_AR_BATCH_SOURCE_ERROR';
      l_err_msg      := 'Error : Batch source CONVERSION not found in R12 ';
      pov_valid_flag := 'E';
      g_retcode      := 1;
      log_errors(piv_source_column_name  => 'Org Id',
                 piv_source_column_value => pin_org_id,
                 piv_error_type          => 'ERR_VAL',
                 piv_error_code          => l_err_code,
                 piv_error_message       => l_err_msg,
                 pov_return_status       => l_log_ret_status,
                 pov_error_msg           => l_log_err_msg);
    WHEN OTHERS THEN
      l_err_code     := 'ETN_AR_PROCEDURE_EXCEPTION';
      l_err_msg      := 'Error : Error validating batch source CONVERSION' ||
                        SUBSTR(SQLERRM, 1, 150);
      g_retcode      := 2;
      pov_valid_flag := 'E';
      log_errors(piv_source_column_name  => 'Org Id',
                 piv_source_column_value => pin_org_id,
                 piv_error_type          => 'ERR_VAL',
                 piv_error_code          => l_err_code,
                 piv_error_message       => l_err_msg,
                 pov_return_status       => l_log_ret_status,
                 pov_error_msg           => l_log_err_msg);
  END validate_batch_source;

  --
  -- ========================
  -- Procedure: VALIDATE_ACCOUNTS
  -- =============================================================================
  --   This procedure validates all
  --   the account related information
  -- =============================================================================
  PROCEDURE validate_accounts(p_in_txn_id IN NUMBER,
                              p_in_seg1   IN VARCHAR2,
                              p_in_seg2   IN VARCHAR2,
                              p_in_seg3   IN VARCHAR2,
                              p_in_seg4   IN VARCHAR2,
                              p_in_seg5   IN VARCHAR2,
                              p_in_seg6   IN VARCHAR2,
                              p_in_seg7   IN VARCHAR2
                              --ver1.26 changes start
                             ,
                              p_leg_operating_unit     IN VARCHAR2,
                              p_leg_int_hdr_attribute1 IN VARCHAR2,
                              p_customer_type          IN VARCHAR2,
                              p_cust_id                IN NUMBER -- v1.42
                              --ver1.26 changes end
                             ,
                              x_out_acc  OUT xxetn_common_pkg.g_rec_type,
                              x_out_ccid OUT NUMBER) IS
    l_in_rec         xxetn_coa_mapping_pkg.g_coa_rec_type := NULL;
    x_ccid           NUMBER := NULL;
    x_out_rec        xxetn_coa_mapping_pkg.g_coa_rec_type := NULL;
    x_msg            VARCHAR2(4000) := NULL;
    x_status         VARCHAR2(50) := NULL;
    l_in_seg_rec     xxetn_common_pkg.g_rec_type := NULL;
    x_err            VARCHAR2(4000) := NULL;
    l_err_code       VARCHAR2(40) := NULL;
    l_err_msg        VARCHAR2(2000) := NULL;
    l_log_ret_status VARCHAR2(50);
    l_log_err_msg    VARCHAR2(2000);
    --ver1.26 changes start
    l_in_seg1 VARCHAR2(240);
    l_in_seg3 VARCHAR2(240);
    --ver1.26 changes end

    l_in_seg6 VARCHAR2(240); --v1.42
    l_in_seg7 VARCHAR2(240); --v1.42

  BEGIN
    x_out_acc  := NULL;
    x_out_ccid := NULL;
    l_in_seg6  := NULL; -- v1.42
    l_in_seg7  := NULL; -- v1.42

    xxetn_debug_pkg.add_debug(piv_debug_msg => 'Validate accounts procedure called ');
    --ver1.26 changes start
    l_in_seg1 := p_in_seg1;
    l_in_seg3 := p_in_seg3;
    IF NVL(p_leg_operating_unit, '-XXX') IN ('OU US AR', 'OU CA AR') THEN
      IF p_leg_int_hdr_attribute1 IS NOT NULL THEN
        --that means its REC line
        l_in_seg1 := SUBSTR(p_leg_int_hdr_attribute1, -4);
      END IF;
    --  IF l_in_seg3 = '11410' THEN --v1.60 commented as part of Defect#12840
       IF l_in_seg3 in ('11410', '11411') THEN --v1.60 Changes made for Defect#12840
        IF (NVL(p_customer_type, 'R') = 'I') THEN
          --Intercompany customer
          l_in_seg3 := '15310';
        ELSIF NVL(p_customer_type, 'R') = 'R' THEN
          -- Trade customer
          l_in_seg3 := '11411';
        END IF;
      END IF;

      /** Added below for v1.42 **/
      IF NVL(p_customer_type, 'R') = 'I' AND p_cust_id IS NOT NULL THEN

        -- deriving segment7 from first 4 digit of account name --
        SELECT SUBSTR(account_name, 1, 4)
          INTO l_in_seg7
          FROM apps.hz_cust_accounts_all
         WHERE 1 = 1
           AND cust_account_id = p_cust_id;

        -- deriving segment6 from ETN Map unit --
        BEGIN
          SELECT le_number
            INTO l_in_seg6
            FROM apps.xxetn_map_unit_v
           WHERE 1 = 1
             AND site = l_in_seg7;
        EXCEPTION
          WHEN OTHERS THEN
            g_retcode := 1;
            print_log1_message('Error while deriving LE_NUMBER for IC Customer for SITE :' ||
                               l_in_seg6 || '. Error: ' || SQLERRM);
        END;

      END IF;
      /** Added above for v1.42 **/

    END IF;

    --ver1.26 changes end
    --ver1.26 changes start
    --l_in_rec.segment1 := p_in_seg1;
    l_in_rec.segment1 := l_in_seg1;
    --ver1.26 changes end
    l_in_rec.segment2 := p_in_seg2;
    --ver1.26 changes start
    --l_in_rec.segment3 := p_in_seg3;
    l_in_rec.segment3 := l_in_seg3;
    --ver1.26 changes end
    l_in_rec.segment4 := p_in_seg4;
    l_in_rec.segment5 := p_in_seg5;
    l_in_rec.segment6 := p_in_seg6;
    l_in_rec.segment7 := p_in_seg7;
    xxetn_coa_mapping_pkg.get_code_combination(g_direction,
                                               NULL,
                                               g_sysdate,
                                               l_in_rec,
                                               x_out_rec,
                                               x_status,
                                               x_msg);
    IF x_status = g_coa_processed THEN
      l_in_seg_rec.segment1 := x_out_rec.segment1;
      l_in_seg_rec.segment2 := x_out_rec.segment2;
      l_in_seg_rec.segment3 := x_out_rec.segment3;
      l_in_seg_rec.segment4 := x_out_rec.segment4;
      l_in_seg_rec.segment5 := x_out_rec.segment5;
      --l_in_seg_rec.segment6  := x_out_rec.segment6;
      --l_in_seg_rec.segment7  := x_out_rec.segment7;
      -- v1.42
      IF NVL(p_customer_type, 'R') = 'I' AND
         NVL(p_leg_operating_unit, '-XXX') IN ('OU US AR', 'OU CA AR') THEN
        l_in_seg_rec.segment6 := l_in_seg6;
        l_in_seg_rec.segment7 := l_in_seg7;
      ELSE
        l_in_seg_rec.segment6 := x_out_rec.segment6;
        l_in_seg_rec.segment7 := x_out_rec.segment7;
      END IF;

      l_in_seg_rec.segment8  := x_out_rec.segment8;
      l_in_seg_rec.segment9  := x_out_rec.segment9;
      l_in_seg_rec.segment10 := x_out_rec.segment10;
      xxetn_common_pkg.get_ccid(l_in_seg_rec, x_ccid, x_err);
      IF x_err IS NULL THEN
        x_out_acc.segment1 := x_out_rec.segment1;
        x_out_acc.segment2 := x_out_rec.segment2;
        x_out_acc.segment3 := x_out_rec.segment3;
        x_out_acc.segment4 := x_out_rec.segment4;
        x_out_acc.segment5 := x_out_rec.segment5;
        --x_out_acc.segment6  := x_out_rec.segment6;
        --x_out_acc.segment7  := x_out_rec.segment7;

        -- v1.42
        IF NVL(p_customer_type, 'R') = 'I' AND
           NVL(p_leg_operating_unit, '-XXX') IN ('OU US AR', 'OU CA AR') THEN
          x_out_acc.segment6 := l_in_seg6;
          x_out_acc.segment7 := l_in_seg7;
        ELSE
          x_out_acc.segment6 := x_out_rec.segment6;
          x_out_acc.segment7 := x_out_rec.segment7;
        END IF;

        x_out_acc.segment8  := x_out_rec.segment8;
        x_out_acc.segment9  := x_out_rec.segment9;
        x_out_acc.segment10 := x_out_rec.segment10;
        x_out_ccid          := x_ccid;
        print_log1_message('Account information successfully derived ');
      ELSE
        l_err_code := 'ETN_AR_INCORRECT_ACCOUNT_INFORMATION';
        l_err_msg  := 'Error : Following error in COA transformation : ' ||
                      x_err;
        print_log1_message(l_err_msg || 'leg_dist_segment1');
        g_retcode := 1;
        log_errors(pin_transaction_id      => p_in_txn_id,
                   piv_source_column_name  => 'SEGMENT1',
                   piv_source_column_value => p_in_seg1,
                   piv_error_type          => 'ERR_VAL',
                   piv_error_code          => l_err_code,
                   piv_error_message       => l_err_msg,
                   pov_return_status       => l_log_ret_status,
                   piv_source_table        => 'XXAR_INVOICES_DIST_STG',
                   pov_error_msg           => l_log_err_msg);
      END IF;
    ELSIF x_status = g_coa_error THEN
      l_err_code := 'ETN_AR_INCORRECT_ACCOUNT_INFORMATION';
      l_err_msg  := 'Error : Following error in COA transformation : ' ||
                    x_msg;
      print_log1_message(l_err_msg || 'leg_dist_segment1');
      g_retcode := 1;
      log_errors(pin_transaction_id      => p_in_txn_id,
                 piv_source_column_name  => 'SEGMENT1',
                 piv_source_column_value => p_in_seg1,
                 piv_error_type          => 'ERR_VAL',
                 piv_error_code          => l_err_code,
                 piv_error_message       => l_err_msg,
                 pov_return_status       => l_log_ret_status,
                 piv_source_table        => 'XXAR_INVOICES_DIST_STG',
                 pov_error_msg           => l_log_err_msg);
    END IF;
    xxetn_debug_pkg.add_debug(piv_debug_msg => 'Validate accounts procedure ends ');
  EXCEPTION
    WHEN OTHERS THEN
      g_retcode  := 2;
      l_err_code := 'ETN_AR_PROCEDURE_EXCEPTION';
      l_err_msg  := 'Error while deriving accounting information ';
      log_errors(pin_transaction_id      => p_in_txn_id,
                 piv_source_column_name  => 'SEGMENT1',
                 piv_source_column_value => p_in_seg1,
                 piv_error_type          => 'ERR_VAL',
                 piv_error_code          => l_err_code,
                 piv_error_message       => l_err_msg,
                 piv_source_table        => 'XXAR_INVOICES_DIST_STG',
                 pov_return_status       => l_log_ret_status,
                 pov_error_msg           => l_log_err_msg);
  END validate_accounts;

  --
  -- ========================
  -- Procedure: VALIDATE_INVOICE
  -- =============================================================================
  --   This procedure is used to run generic validations for invoice lines
  --   and distribution lines
  -- =============================================================================
  --
  PROCEDURE validate_invoice(piv_period_name IN VARCHAR2) IS
    l_err_code            VARCHAR2(40);
    l_err_msg             VARCHAR2(2000);
    l_upd_ret_status      VARCHAR2(50) := NULL;
    l_log_ret_status      VARCHAR2(50) := NULL;
    l_log_err_msg         VARCHAR2(2000);
    l_customer_id         xxar_invoices_stg.system_bill_customer_id%TYPE;
    l_trx_number          xxar_invoices_stg.trx_number%TYPE := -1;
    l_leg_customer_trx_id xxar_invoices_stg.leg_customer_trx_id%TYPE := -1; -- added for v1.48
    l_trx_type_id         xxar_invoices_stg.transaction_type_id%TYPE;
    l_org_id              xxar_invoices_stg.org_id%TYPE;
    l_line_number         xxar_invoices_stg.leg_line_number%TYPE;
    l_valid_flag          VARCHAR2(1) := 'Y';
    l_ledger_id           gl_ledgers.ledger_id%TYPE;
    l_validate_flag       VARCHAR2(1) := 'S';
    l_validate_line_flag  VARCHAR2(1) := 'S';
    --      l_dist_flag                   VARCHAR2 (1);
    l_leg_int_line_attribute1   xxar_invoices_stg.leg_interface_line_attribute1%TYPE;
    l_leg_int_line_attribute2   xxar_invoices_stg.leg_interface_line_attribute2%TYPE;
    l_leg_int_line_attribute3   xxar_invoices_stg.leg_interface_line_attribute3%TYPE;
    l_leg_int_line_attribute4   xxar_invoices_stg.leg_interface_line_attribute4%TYPE;
    l_leg_int_line_attribute5   xxar_invoices_stg.leg_interface_line_attribute5%TYPE;
    l_leg_int_line_attribute6   xxar_invoices_stg.leg_interface_line_attribute6%TYPE;
    l_leg_int_line_attribute7   xxar_invoices_stg.leg_interface_line_attribute7%TYPE;
    l_leg_int_line_attribute8   xxar_invoices_stg.leg_interface_line_attribute8%TYPE;
    l_leg_int_line_attribute9   xxar_invoices_stg.leg_interface_line_attribute9%TYPE;
    l_leg_int_line_attribute10  xxar_invoices_stg.leg_interface_line_attribute10%TYPE;
    l_leg_int_line_attribute11  xxar_invoices_stg.leg_interface_line_attribute11%TYPE;
    l_leg_int_line_attribute12  xxar_invoices_stg.leg_interface_line_attribute12%TYPE;
    l_leg_int_line_attribute13  xxar_invoices_stg.leg_interface_line_attribute13%TYPE;
    l_leg_int_line_attribute14  VARCHAR2(240);
    l_leg_int_line_attribute15  VARCHAR2(240);
    l_src_concatenated_segments VARCHAR2(1000);
    l_tgt_concatenated_segments VARCHAR2(1000);
    l_inv_flag                  VARCHAR2(1);
    l_cm_status_flag            VARCHAR2(1);
    l_gl_date                   DATE;
    x_out_acc_rec               xxetn_common_pkg.g_rec_type;
    x_ccid                      NUMBER;
    l_rec_flag                  VARCHAR2(1) := 'N';
    l_rev_flag                  VARCHAR2(1) := 'N';
    l_assign_flag               VARCHAR2(1) := 'N';
    l_rec_dist_idx              NUMBER := NULL;
    l_rec_int_line_attribute1   xxar_invoices_stg.leg_interface_line_attribute1%TYPE;
    l_rec_int_line_attribute2   xxar_invoices_stg.leg_interface_line_attribute2%TYPE;
    l_rec_int_line_attribute3   xxar_invoices_stg.leg_interface_line_attribute3%TYPE;
    l_rec_int_line_attribute4   xxar_invoices_stg.leg_interface_line_attribute4%TYPE;
    l_rec_int_line_attribute5   xxar_invoices_stg.leg_interface_line_attribute5%TYPE;
    l_rec_int_line_attribute6   xxar_invoices_stg.leg_interface_line_attribute6%TYPE;
    l_rec_int_line_attribute7   xxar_invoices_stg.leg_interface_line_attribute7%TYPE;
    l_rec_int_line_attribute8   xxar_invoices_stg.leg_interface_line_attribute8%TYPE;
    l_rec_int_line_attribute9   xxar_invoices_stg.leg_interface_line_attribute9%TYPE;
    l_rec_int_line_attribute10  xxar_invoices_stg.leg_interface_line_attribute10%TYPE;
    l_rec_int_line_attribute11  xxar_invoices_stg.leg_interface_line_attribute11%TYPE;
    l_rec_int_line_attribute12  xxar_invoices_stg.leg_interface_line_attribute12%TYPE;
    l_rec_int_line_attribute13  xxar_invoices_stg.leg_interface_line_attribute13%TYPE;
    l_rec_int_line_attribute14  VARCHAR2(240);
    l_rec_int_line_attribute15  VARCHAR2(240);
    l_dist_cust_trx_id          NUMBER;
    l_dist_org_id               NUMBER;
    l_limit                     NUMBER := 1000;
    l_line_limit                NUMBER := 1000;
    l_dist_limit                NUMBER := 1000;
    l_leg_line_amount           NUMBER := 0;
    l_r12_org_id                NUMBER;
    l_ou_name                   hr_operating_units.NAME%TYPE;
    l_sob_id                    NUMBER;
    l_func_curr                 gl_ledgers.currency_code%TYPE;
    l_r12_cust_id               NUMBER;
    l_bill_to_addr              NUMBER;
    l_ship_to_addr              NUMBER;
    l_trx_type_name             VARCHAR2(100);
    l_gl_error                  VARCHAR2(1);
    l_valid_cust_flag           VARCHAR2(1);
    l_curr_code                 VARCHAR2(30);
    l_trx_type                  ra_cust_trx_types_all.NAME%TYPE;
    --l_cm_term_error             VARCHAR2(1);   -- commented for v1.58
    l_term_name                 ra_terms_tl.NAME%TYPE;
    l_term_id                   NUMBER;
    l_org_name                  hr_operating_units.NAME%TYPE;
    l_gl_status                 gl_period_statuses.closing_status%TYPE;
    l_tax_code_r12              zx_rates_b.tax_rate_code%TYPE;
    l_tax_r12                   zx_rates_b.tax%TYPE;
    l_tax_regime_code           zx_rates_b.tax_regime_code%TYPE;
    l_tax_rate_code             zx_rates_b.tax_rate_code%TYPE;
    l_tax                       zx_rates_b.tax%TYPE;
    l_tax_status_code           zx_rates_b.tax_status_code%TYPE;
    l_tax_jurisdiction_code     zx_rates_b.tax_jurisdiction_code%TYPE;
    l_header_attr4              VARCHAR2(240);
    l_header_attr8              VARCHAR2(240);
    --ver1.23 changes start
    l_header_attr13 VARCHAR2(240);
    --ver1.23 changes end
    --ver1.25 changes start
    l_header_attr14 VARCHAR2(240);
    --ver1.25 changes end
    l_fsc_int_hdr_attr1 NUMBER(15); --v1.12  -- C9988598  --Quick fix for Batch Source CONVERSION 00030 issue
    l_fsc_header_attr1  NUMBER(15); --v1.13 Freight Line Error
    l_len_fsc_attr      NUMBER;
    l_fsc_attr          NUMBER(15);
    l_frght_org_id      NUMBER;
    l_inter_hdr_attr    NUMBER(15);
    l_fsc_attrbute      NUMBER;
    l_len_hdr_attr      NUMBER;
    --Ver 1.6 changes start
    l_oper_unit xxetn_map_unit.operating_unit%TYPE;
    l_rec       xxetn_map_util.g_input_rec;
    --Ver 1.6 changes end
    --Ver 1.7 changes start
    l_inv_rule_id      NUMBER;
    l_acc_rule_id      NUMBER;
    l_acc_rule_exists  NUMBER := 0;
    l_inv_rule_err     NUMBER := 0;
    l_inv_rule_err_msg VARCHAR2(500);
    l_valerr_cnt       NUMBER := 1;
    --Ver 1.7 changes end
    --Ver 1.10 changes start
    l_batch_error         VARCHAR2(1);
    l_batch_source        VARCHAR2(240);
    l_source_status       NUMBER;
    l_plant_credit_office VARCHAR2(240);
    --Ver 1.10 changes end
    --ver.1.20.2 changes start
    l_credit_office VARCHAR2(240);
    --ver.1.20.2 changes end
    --ver1.25 changes start
    l_inv_country     xxetn_map_unit.country%TYPE;
    l_inv_site_status xxetn_map_unit.site_status%TYPE;
    --ver1.25 changes end
    --ver1.26 changes start
    l_customer_type hz_cust_accounts_all.customer_type%TYPE;
    --ver1.26 changes start
    --ver1.27 changes start
    l_leg_bill_to_addr VARCHAR2(240);
    l_leg_orig_sys_ref VARCHAR2(240);
    --ver1.27 changes end
    --v1.24 Changes Start
    l_attr1 VARCHAR2(240);
    --v1.29
    l_br_ou_name              VARCHAR2(240) := NULL;
    l_br_default_warehouse_id NUMBER := NULL;
    --end of v1.29
    --v1.36 change start
    l_receipt_method_id   NUMBER := NULL;
    l_receipt_method_name VARCHAR2(30) := NULL;
    l_cust_receipt_method NUMBER := NULL; --v1.37
    --v1.49 Change Starts--
    l_pay_method_rec         hz_payment_method_pub.payment_method_rec_type;
    l_return_status          VARCHAR2(2000);
    l_msg_count              NUMBER;
    l_msg_data               VARCHAR2(2000);
    l_cust_receipt_method_id NUMBER;
    l_pymt_site_use_id       NUMBER;
    l_msg                    VARCHAR2(2000);
    l_cust_receipt_mtd       NUMBER;
    --v1.49 Change Ends--

    --  l_pay_meth_count      NUMBER := NULL;
    --v1.36 change ends
    TYPE l_inv_rec IS RECORD(
      leg_trx_number                xxar_invoices_stg.leg_trx_number%TYPE,
      leg_currency_code             xxar_invoices_stg.leg_currency_code%TYPE,
      leg_customer_number           xxar_invoices_stg.leg_customer_number%TYPE,
      leg_bill_to_address           xxar_invoices_stg.leg_bill_to_address%TYPE,
      leg_ship_to_address           xxar_invoices_stg.leg_ship_to_address%TYPE,
      leg_term_name                 xxar_invoices_stg.leg_term_name%TYPE,
      leg_operating_unit            xxar_invoices_stg.leg_operating_unit%TYPE,
      leg_trx_date                  xxar_invoices_stg.leg_trx_date%TYPE,
      leg_gl_date                   xxar_invoices_stg.leg_gl_date%TYPE,
      leg_batch_source_name         xxar_invoices_stg.leg_batch_source_name%TYPE,
      leg_customer_trx_id           xxar_invoices_stg.leg_customer_trx_id%TYPE,
      leg_cust_trx_type_name        xxar_invoices_stg.leg_cust_trx_type_name%TYPE,
      leg_source_system             xxar_invoices_stg.leg_source_system%TYPE,
      leg_purchase_order            xxar_invoices_stg.leg_purchase_order%TYPE,
      leg_header_attribute_category xxar_invoices_stg.leg_header_attribute_category%TYPE,
      leg_header_attribute1         xxar_invoices_stg.leg_header_attribute1%TYPE,
      leg_header_attribute2         xxar_invoices_stg.leg_header_attribute2%TYPE,
      leg_header_attribute3         xxar_invoices_stg.leg_header_attribute3%TYPE,
      leg_header_attribute4         xxar_invoices_stg.leg_header_attribute4%TYPE,
      leg_header_attribute5         xxar_invoices_stg.leg_header_attribute5%TYPE,
      leg_header_attribute6         xxar_invoices_stg.leg_header_attribute6%TYPE,
      leg_header_attribute7         xxar_invoices_stg.leg_header_attribute7%TYPE,
      leg_header_attribute8         xxar_invoices_stg.leg_header_attribute8%TYPE,
      leg_header_attribute9         xxar_invoices_stg.leg_header_attribute9%TYPE,
      leg_header_attribute10        xxar_invoices_stg.leg_header_attribute10%TYPE,
      leg_header_attribute11        xxar_invoices_stg.leg_header_attribute11%TYPE,
      leg_header_attribute12        xxar_invoices_stg.leg_header_attribute12%TYPE,
      leg_header_attribute13        xxar_invoices_stg.leg_header_attribute13%TYPE,
      leg_header_attribute14        xxar_invoices_stg.leg_header_attribute14%TYPE,
      leg_header_attribute15        xxar_invoices_stg.leg_header_attribute15%TYPE);
    TYPE l_inv_tab IS TABLE OF l_inv_rec;
    val_inv_rec l_inv_tab;
    TYPE l_inv_det_tab IS TABLE OF xxar_invoices_stg%ROWTYPE;
    val_inv_det_rec l_inv_det_tab;
    TYPE l_dist_rec IS RECORD(
      interface_txn_id             xxar_invoices_dist_stg.interface_txn_id%TYPE,
      leg_percent                  xxar_invoices_dist_stg.leg_percent%TYPE,
      leg_account_class            xxar_invoices_dist_stg.leg_account_class%TYPE,
      leg_dist_segment1            xxar_invoices_dist_stg.leg_dist_segment1%TYPE,
      leg_dist_segment2            xxar_invoices_dist_stg.leg_dist_segment2%TYPE,
      leg_dist_segment3            xxar_invoices_dist_stg.leg_dist_segment3%TYPE,
      leg_dist_segment4            xxar_invoices_dist_stg.leg_dist_segment4%TYPE,
      leg_dist_segment5            xxar_invoices_dist_stg.leg_dist_segment5%TYPE,
      leg_dist_segment6            xxar_invoices_dist_stg.leg_dist_segment6%TYPE,
      leg_dist_segment7            xxar_invoices_dist_stg.leg_dist_segment7%TYPE,
      leg_org_name                 xxar_invoices_dist_stg.leg_org_name%TYPE,
      leg_operating_unit           xxar_invoices_stg.leg_operating_unit%TYPE,
      org_id                       xxar_invoices_stg.org_id%TYPE,
      leg_customer_trx_id          xxar_invoices_dist_stg.leg_customer_trx_id%TYPE,
      leg_cust_trx_line_id         xxar_invoices_dist_stg.leg_cust_trx_line_id%TYPE,
      leg_cust_trx_line_gl_dist_id xxar_invoices_dist_stg.leg_cust_trx_line_gl_dist_id%TYPE,
      leg_accounted_amount         xxar_invoices_dist_stg.leg_accounted_amount%TYPE,
      interface_line_context       xxar_invoices_stg.interface_line_context%TYPE,
      interface_line_attribute1    xxar_invoices_stg.interface_line_attribute1%TYPE,
      interface_line_attribute2    xxar_invoices_stg.interface_line_attribute2%TYPE,
      interface_line_attribute3    xxar_invoices_stg.interface_line_attribute3%TYPE,
      interface_line_attribute4    xxar_invoices_stg.interface_line_attribute4%TYPE,
      interface_line_attribute5    xxar_invoices_stg.interface_line_attribute5%TYPE,
      interface_line_attribute6    xxar_invoices_stg.interface_line_attribute6%TYPE,
      interface_line_attribute7    xxar_invoices_stg.interface_line_attribute7%TYPE,
      interface_line_attribute8    xxar_invoices_stg.interface_line_attribute8%TYPE,
      interface_line_attribute9    xxar_invoices_stg.interface_line_attribute9%TYPE,
      interface_line_attribute10   xxar_invoices_stg.interface_line_attribute10%TYPE,
      interface_line_attribute11   xxar_invoices_stg.interface_line_attribute11%TYPE,
      interface_line_attribute12   xxar_invoices_stg.interface_line_attribute12%TYPE,
      interface_line_attribute13   xxar_invoices_stg.interface_line_attribute13%TYPE,
      interface_line_attribute14   xxar_invoices_stg.interface_line_attribute14%TYPE,
      interface_line_attribute15   xxar_invoices_stg.interface_line_attribute15%TYPE,
      leg_cust_trx_type_name       xxar_invoices_stg.leg_cust_trx_type_name%TYPE,
      leg_trx_number               xxar_invoices_stg.leg_trx_number%TYPE,
      leg_line_type                xxar_invoices_stg.leg_line_type%TYPE,
      --ver1.26 changes start
      --         leg_operating_unit              xxar_invoices_stg.leg_operating_unit%TYPE,
      leg_interface_hdr_attribute1 xxar_invoices_stg.leg_interface_hdr_attribute1%TYPE,
      customer_type                xxar_invoices_stg.customer_type%TYPE,
      system_bill_customer_id      xxar_invoices_stg.system_bill_customer_id%TYPE -- v1.42
      --ver1.26 changes end
      );
    TYPE l_dist_tab IS TABLE OF l_dist_rec;
    val_dist_rec l_dist_tab;
    TYPE l_line_rec IS RECORD(
      leg_line_amount            NUMBER,
      interface_line_attribute1  VARCHAR2(240),
      interface_line_attribute15 VARCHAR2(240));
    TYPE l_line_dff_tab IS TABLE OF l_line_rec INDEX BY VARCHAR2(100);
    l_line_dff_rec l_line_dff_tab;
    CURSOR val_inv_cur IS
      SELECT leg_trx_number,
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
             leg_header_attribute15,
             leg_source_system
        FROM xxar_invoices_stg
       WHERE 1 = 1
            /*         AND UPPER (leg_operating_unit) =

                        UPPER (NVL (g_leg_operating_unit,

                            leg_operating_unit

                               )

                          )

                     AND UPPER (leg_cust_trx_type_name) =

                        UPPER (NVL (g_leg_trasaction_type,

                            leg_cust_trx_type_name

                               )

                          )

            */ --performance
         AND process_flag = 'N'
         AND NVL(ERROR_TYPE, 'NO_ERR_TYPE') <> 'ERR_IMP'
         AND batch_id = g_new_batch_id
      -- C9988598 : Batch_Source_name
      -- and leg_customer_trx_id = 177705
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
    CURSOR val_inv_det_cur IS
      SELECT *
        FROM xxar_invoices_stg
       WHERE process_flag IN ('N', 'E')
         AND NVL(ERROR_TYPE, 'NO_ERR_TYPE') <> 'ERR_IMP'
         AND batch_id = g_new_batch_id
            --  and leg_customer_trx_id = 177705
            /*              AND UPPER (leg_operating_unit) = UPPER (NVL (g_leg_operating_unit, leg_operating_unit))

                          AND UPPER (leg_cust_trx_type_name) = UPPER (NVL (g_leg_trasaction_type,leg_cust_trx_type_name))

            */ --performance
         AND run_sequence_id = g_new_run_seq_id
      --ORDER BY leg_customer_trx_id, leg_line_number;
       ORDER BY leg_trx_number, leg_line_type, leg_line_number;
    CURSOR val_dist_cur
    --(p_cust_trx_id IN NUMBER, p_cust_trx_line_id IN NUMBER, p_org_name IN VARCHAR2)
    IS
    --ver1.26 changes start
    /*
                                   SELECT xds.interface_txn_id
                                         ,xds.leg_percent
                                         ,xds.leg_account_class
                                         ,xds.leg_dist_segment1
                                         ,xds.leg_dist_segment2
                                         ,xds.leg_dist_segment3
                                         ,xds.leg_dist_segment4
                                         ,xds.leg_dist_segment5
                                         ,xds.leg_dist_segment6
                                         ,xds.leg_dist_segment7
                                         ,xds.leg_org_name
                                         ,xis.leg_operating_unit
                                         ,xis.org_id
                                         ,xds.leg_customer_trx_id
                                         ,xds.leg_cust_trx_line_id
                                         ,xds.leg_cust_trx_line_gl_dist_id
                                         ,xds.leg_accounted_amount
                                         ,xis.interface_line_context
                                         ,xis.interface_line_attribute1
                                         ,xis.interface_line_attribute2
                                         ,xis.interface_line_attribute3
                                         ,xis.interface_line_attribute4
                                         ,xis.interface_line_attribute5
                                         ,xis.interface_line_attribute6
                                         ,xis.interface_line_attribute7
                                         ,xis.interface_line_attribute8
                                         ,xis.interface_line_attribute9
                                         ,xis.interface_line_attribute10
                                         ,xis.interface_line_attribute11
                                         ,xis.interface_line_attribute12
                                         ,xis.interface_line_attribute13
                                         ,xis.interface_line_attribute14
                                         ,xis.interface_line_attribute15
                                         ,xis.leg_cust_trx_type_name
                                         ,xis.leg_trx_number
                                         ,xis.leg_line_type
                                     FROM xxar_invoices_dist_stg xds
                                         ,xxar_invoices_stg      xis
                                    WHERE xds.process_flag = 'N'
                                  --  and xds.leg_customer_trx_id = 177705
                                      AND NVL(xds.ERROR_TYPE, 'NO_ERR_TYPE') <> 'ERR_IMP'
                                      AND xds.batch_id = g_new_batch_id
                                      AND xds.run_sequence_id = g_new_run_seq_id --performance
                                      AND xds.leg_customer_trx_id = xis.leg_customer_trx_id
                                      AND xds.leg_cust_trx_line_id = xis.leg_cust_trx_line_id
                                      AND xds.leg_account_class NOT IN ('REC', 'ROUND', 'UNEARN')
                                         --   AND    xds.leg_org_name = xis.leg_operating_unit
                                      AND xis.process_flag = 'V'
                                   UNION
                                   SELECT xds.interface_txn_id
                                         ,xds.leg_percent
                                         ,xds.leg_account_class
                                         ,xds.leg_dist_segment1
                                         ,xds.leg_dist_segment2
                                         ,xds.leg_dist_segment3
                                         ,xds.leg_dist_segment4
                                         ,xds.leg_dist_segment5
                                         ,xds.leg_dist_segment6
                                         ,xds.leg_dist_segment7
                                         ,xds.leg_org_name
                                         ,NULL
                                         ,NULL
                                         ,xds.leg_customer_trx_id
                                         ,xds.leg_cust_trx_line_id
                                         ,xds.leg_cust_trx_line_gl_dist_id
                                         ,xds.leg_accounted_amount
                                         ,NULL
                                         ,NULL
                                         ,NULL
                                         ,NULL
                                         ,NULL
                                         ,NULL
                                         ,NULL
                                         ,NULL
                                         ,NULL
                                         ,NULL
                                         ,NULL
                                         ,NULL
                                         ,NULL
                                         ,NULL
                                         ,NULL
                                         ,NULL
                                         ,NULL
                                         ,NULL
                                         ,NULL
                                     FROM xxar_invoices_dist_stg xds
                                    WHERE xds.process_flag = 'N'
                                  --  AND xds.leg_customer_trx_id = 177705
                                      AND NVL(xds.ERROR_TYPE, 'NO_ERR_TYPE') <> 'ERR_IMP'
                                      AND xds.batch_id = g_new_batch_id
                                      AND xds.run_sequence_id = g_new_run_seq_id --performance
                                      AND xds.leg_account_class = 'REC'
                                         --   AND    xds.leg_org_name = xis.leg_operating_unit
                                      AND EXISTS
                                    (SELECT 1
                                             FROM xxar_invoices_stg xis
                                            WHERE xds.leg_customer_trx_id = xis.leg_customer_trx_id
                                              AND xis.process_flag = 'V'
                                              )-- C9988598 : Batch Source Name
                                    ORDER BY leg_customer_trx_id;
                                    */
    --v1.32, added index hints below
      SELECT /*+ INDEX (xds XXAR_INVOICES_DIST_STG_N6) */
       xds.interface_txn_id,
       xds.leg_percent,
       xds.leg_account_class,
       xds.leg_dist_segment1,
       xds.leg_dist_segment2,
       xds.leg_dist_segment3,
       xds.leg_dist_segment4,
       xds.leg_dist_segment5,
       xds.leg_dist_segment6,
       xds.leg_dist_segment7,
       xds.leg_org_name,
       xis.leg_operating_unit,
       xis.org_id,
       xds.leg_customer_trx_id,
       xds.leg_cust_trx_line_id,
       xds.leg_cust_trx_line_gl_dist_id,
       xds.leg_accounted_amount,
       xis.interface_line_context,
       xis.interface_line_attribute1,
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
       xis.interface_line_attribute15,
       xis.leg_cust_trx_type_name,
       xis.leg_trx_number,
       xis.leg_line_type
       --               ,NULL
      ,
       NULL
       --,NULL
       --,NULL
      ,
       xis.customer_type -- v1.42
      ,
       system_bill_customer_id -- v1.42
        FROM xxar_invoices_dist_stg xds, xxar_invoices_stg xis
       WHERE xds.leg_customer_trx_id = xis.leg_customer_trx_id
            --ver1.41 changes start
         AND xds.leg_source_system = xis.leg_source_system
            --ver1.41 changes end
         AND xds.batch_id = g_new_batch_id
         AND xds.run_sequence_id = g_new_run_seq_id --performance
         AND xis.leg_cust_trx_line_id = xds.leg_cust_trx_line_id
         AND xds.process_flag = 'N'
         AND NVL(xds.ERROR_TYPE, 'NO_ERR_TYPE') <> 'ERR_IMP'
         AND xds.leg_account_class NOT IN ('REC', 'ROUND', 'UNEARN')
         AND xis.process_flag = 'V'
      UNION
      SELECT /*+ INDEX (xds XXAR_INVOICES_DIST_STG_N6) */
      DISTINCT xds.interface_txn_id,
               xds.leg_percent,
               xds.leg_account_class,
               xds.leg_dist_segment1,
               xds.leg_dist_segment2,
               xds.leg_dist_segment3,
               xds.leg_dist_segment4,
               xds.leg_dist_segment5,
               xds.leg_dist_segment6,
               xds.leg_dist_segment7,
               xds.leg_org_name,
               xis.leg_operating_unit,
               NULL,
               xds.leg_customer_trx_id,
               xds.leg_cust_trx_line_id,
               xds.leg_cust_trx_line_gl_dist_id,
               xds.leg_accounted_amount,
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
               NULL,
               NULL
               --,xis.leg_operating_unit
              ,
               xis.leg_interface_hdr_attribute1,
               xis.customer_type,
               system_bill_customer_id -- v1.42
        FROM xxar_invoices_dist_stg xds, xxar_invoices_stg xis
       WHERE xds.leg_customer_trx_id = xis.leg_customer_trx_id
            --ver1.41 changes start
         AND xds.leg_source_system = xis.leg_source_system
            --ver1.41 changes end
         AND xds.batch_id = g_new_batch_id
         AND xds.run_sequence_id = g_new_run_seq_id --performance
         AND xds.process_flag = 'N'
         AND NVL(xds.ERROR_TYPE, 'NO_ERR_TYPE') <> 'ERR_IMP'
         AND xds.leg_account_class = 'REC'
         AND xis.process_flag = 'V'
       ORDER BY leg_customer_trx_id;
    --ver1.26 changes end
    --Ver1.6 Changes start
    /*      CURSOR org_cur

          IS

             SELECT DISTINCT leg_operating_unit

                        FROM xxar_invoices_stg

                       WHERE batch_id = g_new_batch_id

             AND run_sequence_id = g_new_run_seq_id;

    */
    --Ver 1.10 Changes start
    /*      CURSOR org_cur

          IS

             SELECT DISTINCT leg_dist_segment1

                        FROM xxar_invoices_dist_stg

                       WHERE batch_id = g_new_batch_id

             AND run_sequence_id = g_new_run_seq_id;

    */
    CURSOR org_cur_issc IS
      SELECT DISTINCT leg_dist_segment1 /*decode(leg_org_name, 'OU ELECTRICAL BR', leg_dist_segment1, leg_dist_segment1)*/ /*leg_dist_segment1*/
        FROM xxar_invoices_dist_stg
       WHERE batch_id = g_new_batch_id
         AND run_sequence_id = g_new_run_seq_id
            --ver1.20.1 changes start
            --AND leg_source_system  != 'NAFSC'
         AND leg_org_name NOT IN ('OU US AR', 'OU CA AR')
            --ver1.20.1 changes end
         AND leg_account_class = 'REC';
    --Ver 1.10 Changes end
    --Ver1.6 Changes end
    --ver1.20.1 changes start
    /*    CURSOR org_cur_nafsc IS
           SELECT DISTINCT interface_header_attribute1
             FROM xxar_invoices_stg hdr
             ,    xxar_interf_hdr_attr_stg temp_tbl
            WHERE temp_tbl.customer_trx_id = hdr.leg_customer_trx_id
              AND hdr.batch_id = g_new_batch_id
              AND hdr.run_sequence_id = g_new_run_seq_id
              AND hdr.leg_source_system = 'NAFSC'; -- C9988598
              --and hdr.leg_customer_trx_id = 177705;
    */
    CURSOR org_cur_nafsc IS
      SELECT DISTINCT leg_interface_hdr_attribute1
        FROM xxar_invoices_stg hdr
       WHERE hdr.batch_id = g_new_batch_id
         AND hdr.run_sequence_id = g_new_run_seq_id
         AND leg_operating_unit IN ('OU US AR', 'OU CA AR');
    --ver1.20.1 changes end
    CURSOR customer_cur IS
      SELECT DISTINCT leg_customer_number,
                      leg_bill_to_address,
                      leg_ship_to_address,
                      org_id
                      --ver1.27 changes start
                     ,
                      org_name,
                      leg_operating_unit,
                      leg_source_system
      --ver1.27 changes end
        FROM xxar_invoices_stg
       WHERE batch_id = g_new_batch_id
         AND run_sequence_id = g_new_run_seq_id;
    CURSOR currency_cur IS
      SELECT DISTINCT leg_currency_code
        FROM xxar_invoices_stg
       WHERE batch_id = g_new_batch_id
         AND run_sequence_id = g_new_run_seq_id;
    CURSOR trx_type_cur IS
      SELECT DISTINCT leg_cust_trx_type_name, org_id
        FROM xxar_invoices_stg
       WHERE batch_id = g_new_batch_id
         AND run_sequence_id = g_new_run_seq_id;
    CURSOR term_cur IS
      SELECT DISTINCT leg_term_name
        FROM xxar_invoices_stg
       WHERE batch_id = g_new_batch_id
         AND run_sequence_id = g_new_run_seq_id;
    --          process_flag in ( 'N','E')

    -- Commented this cursor for v1.48, removal of Service Contracts validation, Defect# 9239
    /**CURSOR gl_date_cur IS
    SELECT DISTINCT leg_gl_date
                   ,ledger_id
      FROM xxar_invoices_stg
     WHERE batch_id = g_new_batch_id
       AND run_sequence_id = g_new_run_seq_id
       AND leg_cust_trx_type_name LIKE '%OKS%'; **/

    CURSOR tax_cur IS
      SELECT DISTINCT leg_tax_code, org_id
      --                        ,leg_customer_trx_id
      --                        ,leg_source_system
        FROM xxar_invoices_stg
       WHERE batch_id = g_new_batch_id
         AND run_sequence_id = g_new_run_seq_id
            --ver1.22 changes start
         AND leg_line_type = 'TAX';
    --ver1.22 changes end
    --Ver1.7 changes start

    -- Commented this cursor for v1.48, removal of Service Contracts validation, Defect# 9239
    /**CURSOR accounting_cur IS
    SELECT DISTINCT leg_agreement_name
      FROM xxar_invoices_stg
     WHERE batch_id = g_new_batch_id
       AND run_sequence_id = g_new_run_seq_id; **/

    --Ver1.7 changes end
    ---Added by Shailesh Chaudhari CR 305000 Start--
    /* CURSOR warehouse_id_cur(cp_warehouse_id IN VARCHAR2) IS
    SELECT tag
      FROM fnd_lookup_values
     WHERE meaning = cp_warehouse_id
       AND lookup_type =  'BR_AR_WAREHOUSE_IDS'
       AND ((attribute12 in ('501','502'))or (attribute13 in ('501','502')))
       AND attribute_category = 'BR_AR_WAREHOUSE_IDS'
       AND language ='US';*/ --commented for v1.28
    ---Added by Shailesh Chaudhari CR 305000 End--
    -- SS
    /**PROCEDURE get_org( p_leg_source_system IN VARCHAR2, p_leg_customer_trx_id IN NUMBER, p_org_id OUT NUMBER, p_ou_name OUT VARCHAR2 ) IS
      l_fsc_int_hdr_attr1 xxar_interf_hdr_attr_stg.interface_header_attribute1%TYPE;
    BEGIN
          l_fsc_int_hdr_attr1 := NULL;
          IF p_leg_source_system = 'NAFSC' THEN
              BEGIN
                  FOR i IN (  SELECT interface_header_attribute1
                              FROM xxar_interf_hdr_attr_stg
                              WHERE customer_trx_id = p_leg_customer_trx_id)
                  LOOP
                      l_fsc_int_hdr_attr1 := i.interface_header_attribute1;
                      EXIT;
                  END LOOP;
              EXCEPTION
                  WHEN OTHERS THEN
                      print_log_message('Error while deriving Header Attr1 for customer_trx_id: ' || p_leg_customer_trx_id || SQLERRM);
              END;
              l_rec.site := SUBSTR(l_fsc_int_hdr_attr1, -4);
          ELSE
              FOR i IN (  SELECT leg_dist_segment1 FROM xxar_invoices_dist_stg WHERE leg_customer_trx_id = p_leg_customer_trx_id)
              LOOP
                  l_rec.site := i.leg_dist_segment1;
                  EXIT;
              END LOOP;
          END IF;
          l_org_name := xxetn_map_util.get_value(l_rec).operating_unit;
          FOR i IN (SELECT DISTINCT hou.organization_id
                                      ,hou.NAME
                      FROM apps.hr_operating_units hou
                      WHERE UPPER(hou.NAME) = UPPER(l_org_name)
                      )
          LOOP
              p_org_id := i.organization_id;
              p_ou_name := i.name;
              EXIT;
          END LOOP;
    EXCEPTION
      WHEN OTHERS THEN
          print_log_message('get_org : Error while deriving OU : ' || p_leg_customer_trx_id || SQLERRM);
          p_org_id := NULL;
          p_ou_name := '** Error OU **';
    END get_org;**/
    l_plant VARCHAR2(100); -- v1.34

    /** Added below for v1.43 **/

    /** Modified for v1.48, Defect# 9249 **/

    CURSOR multiple_le_trx_cur IS
    /**SELECT leg_customer_trx_id,dist_segment1
              FROM xxar_invoices_dist_stg
              WHERE 1 = 1
              --AND process_flag = 'V'
              AND batch_id = g_new_batch_id
              AND run_sequence_id = g_new_run_seq_id
              AND dist_segment1 IS NOT NULL
              GROUP BY leg_customer_trx_id,dist_segment1
              HAVING COUNT(1) = 1; **/
        --v1.56 added brazil Operating units
      SELECT leg_customer_trx_id, COUNT(1)
        FROM (SELECT leg_customer_trx_id, dist_segment1
                FROM xxar_invoices_dist_stg
               WHERE 1 = 1
                    --AND process_flag = 'V'
                 AND batch_id = g_new_batch_id
                 AND run_sequence_id = g_new_run_seq_id
                 AND dist_segment1 IS NOT NULL
         AND leg_org_name not in ('OU ELECTRICAL BR','OU TRUCK COMPONENTS BR','OU FLUID POWER BR') -- v1.57 modified condition to exclude BR OU's.
               GROUP BY leg_customer_trx_id, dist_segment1)
       GROUP BY leg_customer_trx_id
      HAVING COUNT(1) > 1;


    -----v1.51 Change Starts --------------------

/**    CURSOR c_upd_dist_cur IS
      SELECT xids.rowid,
             xids.leg_customer_trx_id,
             xids.leg_cust_trx_line_id,
             xids.leg_account_class,
             xids.leg_org_name
        FROM xxconv.xxar_invoices_dist_stg xids
       WHERE xids.leg_source_system = 'SASC'
         AND xids.leg_account_class <> 'TAX'
         AND xids.batch_id = g_new_batch_id
         AND xids.run_sequence_id = g_new_run_seq_id
         AND (xids.leg_customer_trx_id, xids.leg_cust_trx_line_id) NOT IN
             (SELECT xis.leg_customer_trx_id, xis.leg_cust_trx_line_id
                FROM xxconv.xxar_invoices_stg xis
               WHERE xis.leg_line_type <> 'TAX'
                 AND xis.leg_source_system = 'SASC'
                 AND xis.leg_customer_trx_id = xids.leg_customer_trx_id
                 AND xis.batch_id = g_new_batch_id
                 AND xis.run_sequence_id = g_new_run_seq_id); **/


   -- Modified for v1.55, added REC in main query and used NOT EXISTS instead of NOT IN

    CURSOR c_upd_dist_cur IS
      SELECT xids.rowid,
             xids.leg_customer_trx_id,
             xids.leg_cust_trx_line_id,
             xids.leg_account_class,
             xids.leg_org_name
        FROM xxconv.xxar_invoices_dist_stg xids
       WHERE xids.leg_source_system = 'SASC'
         AND xids.leg_account_class NOT IN ('TAX','REC')
         AND xids.batch_id = g_new_batch_id
         AND xids.run_sequence_id = g_new_run_seq_id
         AND  NOT EXISTS
             (SELECT 1
                FROM xxconv.xxar_invoices_stg xis
               WHERE xis.leg_line_type <> 'TAX'
                 AND xis.leg_source_system = 'SASC'
                 AND xis.leg_customer_trx_id = xids.leg_customer_trx_id
                 AND xis.leg_cust_trx_line_id = xids.leg_cust_trx_line_id
                 AND xis.batch_id = g_new_batch_id
                 AND xis.run_sequence_id = g_new_run_seq_id);



    -----v1.51 Change Ends ----------------------

  BEGIN
    --   insert into test_error values('VALIDATE invoice started');
    COMMIT;
    -- Start of ISSC ORG LOOP ---------------------------------------------------------------------------------------------------------------------------------------------------
    FOR org_rec_i IN org_cur_issc LOOP
      l_r12_org_id          := NULL;
      l_ou_name             := NULL;
      l_sob_id              := NULL;
      l_func_curr           := NULL;
      l_ledger_id           := NULL;
      l_org_name            := NULL;
      l_gl_error            := 'Y';
      l_batch_error         := 'Y';
      l_batch_source        := NULL;
      l_plant_credit_office := NULL;
      --ver.1.20.2 changes start
      l_credit_office := NULL;
      --ver.1.20.2 changes end
      --ver1.25 changes start
      l_inv_country     := NULL;
      l_inv_site_status := NULL;
      --ver1.25 changes end
      --insert into test_error values('inside org_rec_i');
      BEGIN
        l_rec.site := org_rec_i.leg_dist_segment1;
        l_org_name := xxetn_map_util.get_value(l_rec).operating_unit;
        print_log_message('MOCK2 ISSC Org Name ' || l_org_name); --v 1.12
        IF l_org_name IS NULL THEN
          print_log_message('Couldnt find Org Name for ' || l_rec.site); --v 1.12
          FOR r_org_ref_err_rec IN (SELECT /*+ INDEX (xis XXAR_INVOICES_DIST_STG_N13) */
                                     interface_txn_id
                                      FROM xxar_invoices_dist_stg xis
                                     WHERE leg_dist_segment1 =
                                           org_rec_i.leg_dist_segment1
                                          --ver1.20.1 changes start
                                          --AND leg_source_system  != 'NAFSC'
                                       AND leg_org_name NOT IN
                                           ('OU US AR', 'OU CA AR')
                                          --ver1.20.1 changes end
                                       AND leg_account_class = 'REC'
                                       AND batch_id = g_new_batch_id
                                       AND run_sequence_id =
                                           g_new_run_seq_id)
          --Ver 1.10 Changes end
           LOOP
            l_err_code := 'ETN_AR_OPERATING UNIT_ERROR';
            l_err_msg  := 'Error : Cross reference not defined for operating unit in XXETN_MAP_UNIT table';
            g_retcode  := 1;
            log_errors(pin_transaction_id      => r_org_ref_err_rec.interface_txn_id,
                       piv_source_column_name  => 'Legacy Segment 1',
                       piv_source_column_value => org_rec_i.leg_dist_segment1,
                       piv_error_type          => 'ERR_VAL',
                       piv_error_code          => l_err_code,
                       piv_error_message       => l_err_msg,
                       pov_return_status       => l_log_ret_status,
                       pov_error_msg           => l_log_err_msg);
          END LOOP;
          UPDATE /*+ INDEX (xids XXAR_INVOICES_DIST_STG_N13) */ xxar_invoices_dist_stg xids
             SET xids.process_flag      = 'E',
                 xids.error_type        = 'ERR_VAL',
                 xids.last_update_date  = g_sysdate,
                 xids.last_updated_by   = g_last_updated_by,
                 xids.last_update_login = g_login_id
           WHERE xids.leg_dist_segment1 = org_rec_i.leg_dist_segment1
             AND xids.batch_id = g_new_batch_id
             AND xids.run_sequence_id = g_new_run_seq_id
             AND xids.leg_account_class = 'REC'
                --ver1.20.1 changes start
                --AND leg_source_system  != 'NAFSC'
             AND xids.leg_org_name NOT IN ('OU US AR', 'OU CA AR');
          --ver1.20.1 changes end
          UPDATE /*+ INDEX (xis XXAR_INVOICES_STG_N4) */ xxar_invoices_stg xis
             SET xis.process_flag      = 'E',
                 xis.error_type        = 'ERR_VAL',
                 xis.last_update_date  = g_sysdate,
                 xis.last_updated_by   = g_last_updated_by,
                 xis.last_update_login = g_login_id
           WHERE EXISTS
           (SELECT /*+ INDEX (xds1 XXAR_INVOICES_DIST_STG_N13) */
                   1
                    FROM xxar_invoices_dist_stg xds1
                   WHERE xds1.leg_dist_segment1 =
                         org_rec_i.leg_dist_segment1
                     AND xds1.leg_customer_trx_id = xis.leg_customer_trx_id
                     AND xds1.batch_id = g_new_batch_id
                     AND xds1.run_sequence_id = g_new_run_seq_id
                     AND xds1.leg_account_class = 'REC'
                        --ver1.20.1 changes start
                        --AND leg_source_system  != 'NAFSC'
                     AND leg_org_name NOT IN ('OU US AR', 'OU CA AR')
                  --ver1.20.1 changes end
                  )
             AND xis.batch_id = g_new_batch_id
             AND xis.run_sequence_id = g_new_run_seq_id;
          -- ver1.10 changes end
          COMMIT;
        END IF;
      EXCEPTION
        WHEN OTHERS THEN
          l_err_code := 'ETN_AR_PROCEDURE_EXCEPTION';
          l_err_msg  := 'Error : Error updating staging table for operating unit' ||
                        SUBSTR(SQLERRM, 1, 150);
          g_retcode  := 2;
          FOR r_org_ref_err_rec1 IN (SELECT /*+ INDEX (xis XXAR_INVOICES_DIST_STG_N13) */
                                      interface_txn_id
                                       FROM xxar_invoices_dist_stg xis
                                      WHERE leg_dist_segment1 =
                                            org_rec_i.leg_dist_segment1
                                        AND leg_account_class = 'REC'
                                           --ver1.20.1 changes start
                                           --AND leg_source_system  != 'NAFSC'
                                        AND leg_org_name NOT IN
                                            ('OU US AR', 'OU CA AR')
                                           --ver1.20.1 changes end
                                        AND batch_id = g_new_batch_id
                                        AND run_sequence_id =
                                            g_new_run_seq_id) LOOP
            --Ver 1.10 Changes end
            log_errors(pin_transaction_id      => r_org_ref_err_rec1.interface_txn_id,
                       piv_source_column_name  => 'Legacy Segment 1',
                       piv_source_column_value => org_rec_i.leg_dist_segment1,
                       piv_error_type          => 'ERR_VAL',
                       piv_error_code          => l_err_code,
                       piv_error_message       => l_err_msg,
                       pov_return_status       => l_log_ret_status,
                       pov_error_msg           => l_log_err_msg);
          END LOOP;
          UPDATE /*+ INDEX (xids XXAR_INVOICES_DIST_STG_N13) */ xxar_invoices_dist_stg xids
             SET xids.process_flag      = 'E',
                 xids.error_type        = 'ERR_VAL',
                 xids.last_update_date  = g_sysdate,
                 xids.last_updated_by   = g_last_updated_by,
                 xids.last_update_login = g_login_id
           WHERE xids.leg_dist_segment1 = org_rec_i.leg_dist_segment1
             AND xids.leg_account_class = 'REC'
                --ver1.20.1 changes start
                --AND leg_source_system  != 'NAFSC'
             AND xids.leg_org_name NOT IN ('OU US AR', 'OU CA AR')
                --ver1.20.1 changes end
             AND xids.batch_id = g_new_batch_id
             AND xids.run_sequence_id = g_new_run_seq_id;
          UPDATE /*+ INDEX (xis XXAR_INVOICES_STG_N4) */ xxar_invoices_stg xis
             SET xis.process_flag      = 'E',
                 xis.error_type        = 'ERR_VAL',
                 xis.last_update_date  = g_sysdate,
                 xis.last_updated_by   = g_last_updated_by,
                 xis.last_update_login = g_login_id
           WHERE EXISTS
           (SELECT /*+ INDEX (xds1 XXAR_INVOICES_DIST_STG_N13) */
                   1
                    FROM xxar_invoices_dist_stg xds1
                   WHERE xds1.leg_dist_segment1 =
                         org_rec_i.leg_dist_segment1
                     AND xds1.leg_customer_trx_id = xis.leg_customer_trx_id
                     AND xds1.batch_id = g_new_batch_id
                     AND xds1.run_sequence_id = g_new_run_seq_id
                     AND xds1.leg_account_class = 'REC'
                        --ver1.20.1 changes start
                        --AND leg_source_system  != 'NAFSC'
                     AND xds1.leg_org_name NOT IN ('OU US AR', 'OU CA AR')
                  --ver1.20.1 changes end
                  )
             AND xis.batch_id = g_new_batch_id
             AND xis.run_sequence_id = g_new_run_seq_id;
          -- ver1.10 changes end
          COMMIT;
      END;
      -- Check whether R12 operating unit in mapping table is already setup
      BEGIN
        IF l_org_name IS NOT NULL THEN
          print_log_message('Validating R12 operating unit ' || l_org_name);
          SELECT hou.organization_id,
                 hou.NAME,
                 hou.set_of_books_id,
                 gll.currency_code,
                 gll.ledger_id
            INTO l_r12_org_id,
                 l_ou_name,
                 l_sob_id,
                 l_func_curr,
                 l_ledger_id
            FROM apps.hr_operating_units hou, gl_ledgers gll
           WHERE UPPER(hou.NAME) = UPPER(l_org_name)
             AND hou.set_of_books_id = gll.ledger_id(+)
             AND TRUNC(NVL(hou.date_to, g_sysdate)) >= TRUNC(g_sysdate);
          print_log_message('MOCK2:R12 operating id ' || l_r12_org_id); --v1.12
        END IF;
        IF l_ledger_id IS NOT NULL THEN
          l_gl_error := 'N';
          BEGIN
            SELECT 1
              INTO l_gl_status
              FROM gl_periods glp, gl_period_statuses gps
             WHERE UPPER(glp.period_name) = UPPER(gps.period_name)
               AND glp.period_set_name = g_period_set_name --'ETN Corp Calend'
               AND g_gl_date BETWEEN glp.start_date AND glp.end_date
               AND gps.application_id =
                   (SELECT fap.application_id
                      FROM fnd_application_vl fap
                     WHERE fap.application_short_name = 'AR') -- V1.31 :- Refer AR Period
               AND gps.closing_status = 'O'
               AND ledger_id = l_ledger_id;
            /*    (SELECT fap.application_id

                                        FROM fnd_application_vl fap

                                       WHERE fap.application_short_name = 'SQLGL')

                                 AND gps.closing_status = 'O'

                                 AND ledger_id = l_ledger_id;
            */
            print_log_message('MOCK2:R12 AR Period ' || l_r12_org_id); --v1.31
            --            print_log_message('MOCK2:R12 GL Period ' ||l_r12_org_id);  --v1.12
          EXCEPTION
            WHEN NO_DATA_FOUND THEN
              l_gl_error := 'Y';
              l_err_code := 'ETN_AR_GL_PERIOD_ERROR';
              g_retcode  := 1;
              l_err_msg  := 'AR Period not open/defined for GL date ' ||
                            g_gl_date; --V1.31
              /*  l_err_msg  := 'GL Period not open/defined for GL date ' ||

              g_gl_date;*/
              FOR r_gl_per_err_rec IN (SELECT /*+ INDEX (xis XXAR_INVOICES_DIST_STG_N13) */
                                        interface_txn_id
                                         FROM xxar_invoices_dist_stg xis
                                        WHERE leg_dist_segment1 =
                                              org_rec_i.leg_dist_segment1
                                          AND leg_account_class = 'REC'
                                             --ver1.20.1 changes start
                                             --AND leg_source_system  != 'NAFSC'
                                          AND leg_org_name NOT IN
                                              ('OU US AR', 'OU CA AR')
                                             --ver1.20.1 changes end
                                          AND batch_id = g_new_batch_id
                                          AND run_sequence_id =
                                              g_new_run_seq_id) LOOP
                -- ver1.10 changes end
                log_errors(pin_transaction_id      => r_gl_per_err_rec.interface_txn_id,
                           piv_source_keyname1     => 'GL Period date',
                           piv_source_keyvalue1    => g_gl_date,
                           piv_source_column_name  => 'R12 Operating Unit',
                           piv_source_column_value => l_org_name,
                           piv_error_type          => 'ERR_VAL',
                           piv_error_code          => l_err_code,
                           piv_error_message       => l_err_msg,
                           pov_return_status       => l_log_ret_status,
                           pov_error_msg           => l_log_err_msg);
              END LOOP;
              UPDATE /*+ INDEX (xids XXAR_INVOICES_DIST_STG_N13) */ xxar_invoices_dist_stg xids
                 SET xids.process_flag      = 'E',
                     xids.error_type        = 'ERR_VAL',
                     xids.last_update_date  = g_sysdate,
                     xids.last_updated_by   = g_last_updated_by,
                     xids.last_update_login = g_login_id
               WHERE xids.leg_dist_segment1 = org_rec_i.leg_dist_segment1
                 AND xids.leg_account_class = 'REC'
                    --ver1.20.1 changes start
                    --AND leg_source_system  != 'NAFSC'
                 AND xids.leg_org_name NOT IN ('OU US AR', 'OU CA AR')
                    --ver1.20.1 changes end
                 AND xids.batch_id = g_new_batch_id
                 AND xids.run_sequence_id = g_new_run_seq_id;
              UPDATE /*+ INDEX (xis XXAR_INVOICES_STG_N4) */ xxar_invoices_stg xis
                 SET xis.process_flag      = 'E',
                     xis.error_type        = 'ERR_VAL',
                     xis.last_update_date  = g_sysdate,
                     xis.last_updated_by   = g_last_updated_by,
                     xis.last_update_login = g_login_id
               WHERE EXISTS
               (SELECT /*+ INDEX (xds1 XXAR_INVOICES_DIST_STG_N13) */
                       1
                        FROM xxar_invoices_dist_stg xds1
                       WHERE xds1.leg_dist_segment1 =
                             org_rec_i.leg_dist_segment1
                         AND xds1.leg_customer_trx_id =
                             xis.leg_customer_trx_id
                         AND xds1.batch_id = g_new_batch_id
                         AND xds1.run_sequence_id = g_new_run_seq_id
                         AND xds1.leg_account_class = 'REC'
                            --ver1.20.1 changes start
                            --AND leg_source_system  != 'NAFSC'
                         AND leg_org_name NOT IN ('OU US AR', 'OU CA AR')
                      --ver1.20.1 changes end
                      )
                 AND xis.batch_id = g_new_batch_id
                 AND xis.run_sequence_id = g_new_run_seq_id;
              -- ver1.10 changes end
            WHEN OTHERS THEN
              l_gl_error := 'Y';
              g_retcode  := 2;
              l_err_code := 'ETN_AR_PROCEDURE_EXCEPTION';
              l_err_msg  := 'Error : Error validating gl period ' ||
                            g_gl_date || SUBSTR(SQLERRM, 1, 150);
              FOR r_gl_per_err1_rec IN (SELECT /*+ INDEX (xis XXAR_INVOICES_DIST_STG_N13) */
                                         interface_txn_id
                                          FROM xxar_invoices_dist_stg xis
                                         WHERE leg_dist_segment1 =
                                               org_rec_i.leg_dist_segment1
                                           AND leg_account_class = 'REC'
                                              --ver1.20.1 changes start
                                              --AND leg_source_system  != 'NAFSC'
                                           AND leg_org_name NOT IN
                                               ('OU US AR', 'OU CA AR')
                                              --ver1.20.1 changes end
                                           AND batch_id = g_new_batch_id
                                           AND run_sequence_id =
                                               g_new_run_seq_id) LOOP
                -- ver1.10 changes end
                log_errors(pin_transaction_id      => r_gl_per_err1_rec.interface_txn_id,
                           piv_source_keyname1     => 'GL Period date',
                           piv_source_keyvalue1    => g_gl_date,
                           piv_source_column_name  => 'R12 Operating Unit',
                           piv_source_column_value => l_org_name,
                           piv_error_type          => 'ERR_VAL',
                           piv_error_code          => l_err_code,
                           piv_error_message       => l_err_msg,
                           pov_return_status       => l_log_ret_status,
                           pov_error_msg           => l_log_err_msg);
              END LOOP;
              UPDATE /*+ INDEX (xids XXAR_INVOICES_DIST_STG_N13) */ xxar_invoices_dist_stg xids
                 SET xids.process_flag      = 'E',
                     xids.error_type        = 'ERR_VAL',
                     xids.last_update_date  = g_sysdate,
                     xids.last_updated_by   = g_last_updated_by,
                     xids.last_update_login = g_login_id
               WHERE xids.leg_dist_segment1 = org_rec_i.leg_dist_segment1
                 AND xids.leg_account_class = 'REC'
                    --ver1.20.1 changes start
                    --AND leg_source_system  != 'NAFSC'
                 AND xids.leg_org_name NOT IN ('OU US AR', 'OU CA AR')
                    --ver1.20.1 changes end
                 AND xids.batch_id = g_new_batch_id
                 AND xids.run_sequence_id = g_new_run_seq_id;
              UPDATE /*+ INDEX (xis XXAR_INVOICES_STG_N4) */ xxar_invoices_stg xis
                 SET xis.process_flag      = 'E',
                     xis.error_type        = 'ERR_VAL',
                     xis.last_update_date  = g_sysdate,
                     xis.last_updated_by   = g_last_updated_by,
                     xis.last_update_login = g_login_id
               WHERE EXISTS
               (SELECT /*+ INDEX (xds1 XXAR_INVOICES_DIST_STG_N13) */
                       1
                        FROM xxar_invoices_dist_stg xds1
                       WHERE xds1.leg_dist_segment1 =
                             org_rec_i.leg_dist_segment1
                         AND xds1.leg_customer_trx_id =
                             xis.leg_customer_trx_id
                         AND xds1.batch_id = g_new_batch_id
                         AND xds1.run_sequence_id = g_new_run_seq_id
                         AND xds1.leg_account_class = 'REC'
                            --ver1.20.1 changes start
                            --AND leg_source_system  != 'NAFSC'
                         AND leg_org_name NOT IN ('OU US AR', 'OU CA AR')
                      --ver1.20.1 changes end
                      )
                 AND xis.batch_id = g_new_batch_id
                 AND xis.run_sequence_id = g_new_run_seq_id;
              -- ver1.10 changes end
          END;
        END IF;
        -- Check batch source
        --      BEGIN
        l_batch_source        := NULL;
        l_plant_credit_office := NULL;
        --ver.1.20.2 changes start
        l_credit_office := NULL;
        --ver.1.20.2 changes end
        IF (l_org_name IS NOT NULL AND l_r12_org_id IS NOT NULL) THEN
          print_log_message('Deriving and validating batch source ' ||
                            l_org_name);
          --SELECT DECODE(xmu.ar_credit_office, NULL, g_batch_source||'-'||org_rec_i.leg_dist_segment1, g_batch_source||'-'||xmu.ar_credit_office)
          --INTO l_batch_source
          --ver.1.20.2 changes start
          /*         SELECT DECODE(xmu.ar_credit_office, NULL, org_rec_i.leg_dist_segment1, xmu.ar_credit_office)

                   INTO l_plant_credit_office

                           FROM xxetn_map_unit xmu

                          WHERE operating_unit = l_org_name

                  AND xmu.site = org_rec_i.leg_dist_segment1;

          */
          --ver1.25 changes start
          --commenting below query
          /*         SELECT DECODE(xmu.ar_credit_office, NULL, org_rec_i.leg_dist_segment1, xmu.ar_credit_office),xmu.ar_credit_office

                   INTO l_plant_credit_office, l_credit_office

                           FROM xxetn_map_unit_v xmu

                          WHERE operating_unit = l_org_name

                  AND xmu.site = org_rec_i.leg_dist_segment1;

          */
          --Adding below query
          SELECT DECODE(xmu.ar_credit_office,
                        NULL,
                        org_rec_i.leg_dist_segment1,
                        xmu.ar_credit_office),
                 xmu.ar_credit_office,
                 xmu.country,
                 xmu.site_status
            INTO l_plant_credit_office,
                 l_credit_office,
                 l_inv_country,
                 l_inv_site_status
            FROM xxetn_map_unit_v xmu
           WHERE operating_unit = l_org_name
             AND xmu.site = org_rec_i.leg_dist_segment1;
          --ver1.25 changes end
          --ver.1.20.2 changes end
          l_batch_source := g_batch_source || ' ' || l_plant_credit_office;
          print_log_message('MOCK2:R12 l_batch_source: ' || l_batch_source); --v1.12
        END IF;
        IF l_batch_source IS NOT NULL THEN
          l_batch_error := 'N';
          BEGIN
            SELECT 1
              INTO l_source_status
              FROM ra_batch_sources_all rbs
             WHERE UPPER(NAME) = l_batch_source
               AND org_id = l_r12_org_id;
            print_log_message('MOCK2:R12 l_batch_source1: ' ||
                              l_batch_source); --v1.12
          EXCEPTION
            WHEN NO_DATA_FOUND THEN
              l_batch_error := 'Y';
              l_err_code    := 'ETN_AR_BATCH_SOURCE_ERROR';
              g_retcode     := 1;
              l_err_msg     := 'Batch Source ' || l_batch_source ||
                               ' not defined for R12 operating unit ' ||
                               l_org_name;
              FOR r_bs_per_err_rec IN (SELECT /*+ INDEX (xis XXAR_INVOICES_DIST_STG_N13) */
                                        interface_txn_id
                                         FROM xxar_invoices_dist_stg xis
                                        WHERE leg_dist_segment1 =
                                              org_rec_i.leg_dist_segment1
                                          AND leg_account_class = 'REC'
                                             --ver1.20.1 changes start
                                             --AND leg_source_system  != 'NAFSC'
                                          AND leg_org_name NOT IN
                                              ('OU US AR', 'OU CA AR')
                                             --ver1.20.1 changes end
                                          AND batch_id = g_new_batch_id
                                          AND run_sequence_id =
                                              g_new_run_seq_id) LOOP
                log_errors(pin_transaction_id      => r_bs_per_err_rec.interface_txn_id,
                           piv_source_keyname1     => 'R12 Operating Unit',
                           piv_source_keyvalue1    => l_org_name,
                           piv_source_column_name  => 'Batch Source',
                           piv_source_column_value => l_batch_source,
                           piv_error_type          => 'ERR_VAL',
                           piv_error_code          => l_err_code,
                           piv_error_message       => l_err_msg,
                           pov_return_status       => l_log_ret_status,
                           pov_error_msg           => l_log_err_msg);
              END LOOP;
              UPDATE /*+ INDEX (xids XXAR_INVOICES_DIST_STG_N13) */ xxar_invoices_dist_stg xids
                 SET xids.process_flag      = 'E',
                     xids.error_type        = 'ERR_VAL',
                     xids.last_update_date  = g_sysdate,
                     xids.last_updated_by   = g_last_updated_by,
                     xids.last_update_login = g_login_id
               WHERE xids.leg_dist_segment1 = org_rec_i.leg_dist_segment1
                 AND xids.leg_account_class = 'REC'
                    --ver1.20.1 changes start
                    --AND leg_source_system  != 'NAFSC'
                 AND xids.leg_org_name NOT IN ('OU US AR', 'OU CA AR')
                    --ver1.20.1 changes end
                 AND xids.batch_id = g_new_batch_id
                 AND xids.run_sequence_id = g_new_run_seq_id;
              UPDATE /*+ INDEX (xis XXAR_INVOICES_STG_N4) */ xxar_invoices_stg xis
                 SET xis.process_flag      = 'E',
                     xis.error_type        = 'ERR_VAL',
                     xis.last_update_date  = g_sysdate,
                     xis.last_updated_by   = g_last_updated_by,
                     xis.last_update_login = g_login_id
               WHERE EXISTS (SELECT /*+ INDEX (xds1 XXAR_INVOICES_DIST_STG_N13) */
                       1
                        FROM xxar_invoices_dist_stg xds1
                       WHERE xds1.leg_dist_segment1 =
                             org_rec_i.leg_dist_segment1
                         AND xds1.leg_customer_trx_id =
                             xis.leg_customer_trx_id
                         AND xds1.batch_id = g_new_batch_id
                         AND xds1.run_sequence_id = g_new_run_seq_id
                         AND xds1.leg_account_class = 'REC'
                            --ver1.20.1 changes start
                            --AND leg_source_system  != 'NAFSC'
                         AND xds1.leg_org_name NOT IN
                             ('OU US AR', 'OU CA AR')
                      --ver1.20.1 changes end
                      )
                 AND xis.batch_id = g_new_batch_id
                 AND xis.run_sequence_id = g_new_run_seq_id;
            WHEN OTHERS THEN
              l_batch_error := 'Y';
              g_retcode     := 2;
              l_err_code    := 'ETN_AR_PROCEDURE_EXCEPTION';
              l_err_msg     := 'Error : Error validating batch source ' ||
                               l_batch_source || SUBSTR(SQLERRM, 1, 150);
              FOR r_bs_per_err1_rec IN (SELECT /*+ INDEX (xis XXAR_INVOICES_DIST_STG_N13) */
                                         interface_txn_id
                                          FROM xxar_invoices_dist_stg xis
                                         WHERE leg_dist_segment1 =
                                               org_rec_i.leg_dist_segment1
                                           AND leg_account_class = 'REC'
                                              --ver1.20.1 changes start
                                              --AND leg_source_system  != 'NAFSC'
                                           AND leg_org_name NOT IN
                                               ('OU US AR', 'OU CA AR')
                                              --ver1.20.1 changes end
                                           AND batch_id = g_new_batch_id
                                           AND run_sequence_id =
                                               g_new_run_seq_id) LOOP
                log_errors(pin_transaction_id      => r_bs_per_err1_rec.interface_txn_id,
                           piv_source_keyname1     => 'R12 Operating Unit',
                           piv_source_keyvalue1    => l_org_name,
                           piv_source_column_name  => 'Batch Source',
                           piv_source_column_value => l_batch_source,
                           piv_error_type          => 'ERR_VAL',
                           piv_error_code          => l_err_code,
                           piv_error_message       => l_err_msg,
                           pov_return_status       => l_log_ret_status,
                           pov_error_msg           => l_log_err_msg);
              END LOOP;
              UPDATE /*+ INDEX (xids XXAR_INVOICES_DIST_STG_N13) */ xxar_invoices_dist_stg xids
                 SET xids.process_flag      = 'E',
                     xids.error_type        = 'ERR_VAL',
                     xids.last_update_date  = g_sysdate,
                     xids.last_updated_by   = g_last_updated_by,
                     xids.last_update_login = g_login_id
               WHERE xids.leg_dist_segment1 = org_rec_i.leg_dist_segment1
                 AND xids.leg_account_class = 'REC'
                    --ver1.20.1 changes start
                    --AND leg_source_system  != 'NAFSC'
                 AND xids.leg_org_name NOT IN ('OU US AR', 'OU CA AR')
                    --ver1.20.1 changes end
                 AND xids.batch_id = g_new_batch_id
                 AND xids.run_sequence_id = g_new_run_seq_id;
              UPDATE /*+ INDEX (xis XXAR_INVOICES_STG_N4) */ xxar_invoices_stg xis
                 SET xis.process_flag      = 'E',
                     xis.error_type        = 'ERR_VAL',
                     xis.last_update_date  = g_sysdate,
                     xis.last_updated_by   = g_last_updated_by,
                     xis.last_update_login = g_login_id
               WHERE EXISTS
               (SELECT /*+ INDEX (xds1 XXAR_INVOICES_DIST_STG_N13) */
                       1
                        FROM xxar_invoices_dist_stg xds1
                       WHERE xds1.leg_dist_segment1 =
                             org_rec_i.leg_dist_segment1
                         AND xds1.leg_customer_trx_id =
                             xis.leg_customer_trx_id
                         AND xds1.batch_id = g_new_batch_id
                         AND xds1.run_sequence_id = g_new_run_seq_id
                         AND xds1.leg_account_class = 'REC'
                            --ver1.20.1 changes start
                            --AND leg_source_system  != 'NAFSC'
                         AND leg_org_name NOT IN ('OU US AR', 'OU CA AR')
                      --ver1.20.1 changes end
                      )
                 AND xis.batch_id = g_new_batch_id
                 AND xis.run_sequence_id = g_new_run_seq_id;
          END;
        END IF;
        print_log_message('OrgID Update : ' || org_rec_i.leg_dist_segment1 || '; ' ||
                          g_new_batch_id || '; ' || g_new_run_seq_id);
        IF l_r12_org_id IS NOT NULL -- SS
        -- IF NVL(l_gl_error, 'N') = 'N' AND NVL(l_batch_error, 'N') = 'N'   -- SS
         THEN
          UPDATE /*+ INDEX (xiss XXAR_INVOICES_STG_N4) */ xxar_invoices_stg xiss
             SET org_id = l_r12_org_id
                 --ver1.27 changes start
                ,
                 org_name = l_ou_name
                 --ver1.27 changes end
                ,
                 set_of_books_id   = l_sob_id,
                 func_curr         = l_func_curr,
                 ledger_id         = l_ledger_id,
                 batch_source_name = l_batch_source
                 --ver1.20.3 changes start
                 --,header_attribute9 = l_plant_credit_office
                ,
                 header_attribute9 = org_rec_i.leg_dist_segment1
                 --ver1.20.3 changes end
                 --ver1.20.2 changes start
                ,
                 credit_office = l_credit_office
                 --ver1.20.2 changes end
                 --ver1.25 changes start
                ,
                 country     = l_inv_country,
                 site_status = l_inv_site_status
          --ver1.25 changes end
           WHERE leg_customer_trx_id IN
                 (SELECT /*+ INDEX (xis XXAR_INVOICES_DIST_STG_N13) */
                   leg_customer_trx_id
                    FROM xxar_invoices_dist_stg xis
                   WHERE leg_dist_segment1 = org_rec_i.leg_dist_segment1
                     AND batch_id = g_new_batch_id
                     AND run_sequence_id = g_new_run_seq_id
                     AND leg_account_class = 'REC'
                        --ver1.20.1 changes start
                        --AND leg_source_system  != 'NAFSC'
                     AND leg_org_name NOT IN ('OU US AR', 'OU CA AR')
                  --ver1.20.1 changes end
                  )
             AND batch_id = g_new_batch_id
             AND run_sequence_id = g_new_run_seq_id;
          IF SQL%FOUND THEN
            print_log_message('ISSC OrgID update successful ' ||
                              SQL%ROWCOUNT);
          ELSE
            print_log_message('ISSC OrgID update UNsuccessful');
          END IF;
          print_log_message('MOCK2:R12 UPDATE query: ' || l_batch_source); --v1.12
        END IF;
        COMMIT;
      EXCEPTION
        WHEN NO_DATA_FOUND THEN
          g_retcode := 1;
          FOR r_r12_org_err_rec IN (SELECT /*+ INDEX (xis XXAR_INVOICES_DIST_STG_N13) */
                                     interface_txn_id
                                      FROM xxar_invoices_dist_stg xis
                                     WHERE leg_dist_segment1 =
                                           org_rec_i.leg_dist_segment1
                                       AND leg_account_class = 'REC'
                                          --ver1.20.1 changes start
                                          --AND leg_source_system  != 'NAFSC'
                                       AND leg_org_name NOT IN
                                           ('OU US AR', 'OU CA AR')
                                          --ver1.20.1 changes end
                                       AND batch_id = g_new_batch_id
                                       AND run_sequence_id =
                                           g_new_run_seq_id) LOOP
            -- ver1.10 changes end
            l_err_code := 'ETN_AR_OPERATING UNIT_ERROR';
            l_err_msg  := 'Error : Operating unit not setup';
            log_errors(pin_transaction_id      => r_r12_org_err_rec.interface_txn_id,
                       piv_source_column_name  => 'R12 Operating Unit',
                       piv_source_column_value => l_org_name,
                       piv_error_type          => 'ERR_VAL',
                       piv_error_code          => l_err_code,
                       piv_error_message       => l_err_msg,
                       pov_return_status       => l_log_ret_status,
                       pov_error_msg           => l_log_err_msg);
          END LOOP;
          UPDATE /*+ INDEX (xids XXAR_INVOICES_DIST_STG_N13) */ xxar_invoices_dist_stg xids
             SET xids.process_flag      = 'E',
                 xids.error_type        = 'ERR_VAL',
                 xids.last_update_date  = g_sysdate,
                 xids.last_updated_by   = g_last_updated_by,
                 xids.last_update_login = g_login_id
           WHERE xids.leg_dist_segment1 = org_rec_i.leg_dist_segment1
             AND xids.leg_account_class = 'REC'
                --ver1.20.1 changes start
                --AND leg_source_system  != 'NAFSC'
             AND xids.leg_org_name NOT IN ('OU US AR', 'OU CA AR')
                --ver1.20.1 changes end
             AND xids.batch_id = g_new_batch_id
             AND xids.run_sequence_id = g_new_run_seq_id;
          UPDATE /*+ INDEX (xis XXAR_INVOICES_STG_N4) */ xxar_invoices_stg xis
             SET xis.process_flag      = 'E',
                 xis.error_type        = 'ERR_VAL',
                 xis.last_update_date  = g_sysdate,
                 xis.last_updated_by   = g_last_updated_by,
                 xis.last_update_login = g_login_id
           WHERE EXISTS
           (SELECT /*+ INDEX (xds1 XXAR_INVOICES_DIST_STG_N13) */
                   1
                    FROM xxar_invoices_dist_stg xds1
                   WHERE xds1.leg_dist_segment1 =
                         org_rec_i.leg_dist_segment1
                     AND xds1.leg_customer_trx_id = xis.leg_customer_trx_id
                     AND xds1.batch_id = g_new_batch_id
                     AND xds1.run_sequence_id = g_new_run_seq_id
                     AND xds1.leg_account_class = 'REC'
                        --ver1.20.1 changes start
                        --AND leg_source_system  != 'NAFSC'
                     AND xds1.leg_org_name NOT IN ('OU US AR', 'OU CA AR')
                  --ver1.20.1 changes end
                  )
             AND xis.batch_id = g_new_batch_id
             AND xis.run_sequence_id = g_new_run_seq_id;
          -- ver1.10 changes end
        WHEN OTHERS THEN
          l_err_code := 'ETN_AR_PROCEDURE_EXCEPTION';
          g_retcode  := 2;
          l_err_msg  := 'Error : Error fetching R12 operating unit' ||
                        SUBSTR(SQLERRM, 1, 150);
          FOR r_r12_org_err1_rec IN (SELECT /*+ INDEX (xis XXAR_INVOICES_DIST_STG_N13) */
                                      interface_txn_id
                                       FROM xxar_invoices_dist_stg xis
                                      WHERE leg_dist_segment1 =
                                            org_rec_i.leg_dist_segment1
                                        AND leg_account_class = 'REC'
                                           --ver1.20.1 changes start
                                           --AND leg_source_system  != 'NAFSC'
                                        AND leg_org_name NOT IN
                                            ('OU US AR', 'OU CA AR')
                                           --ver1.20.1 changes end
                                        AND batch_id = g_new_batch_id
                                        AND run_sequence_id =
                                            g_new_run_seq_id) LOOP
            -- ver1.10 changes end
            log_errors(pin_transaction_id      => r_r12_org_err1_rec.interface_txn_id,
                       piv_source_column_name  => 'R12 Operating Unit',
                       piv_source_column_value => l_org_name,
                       piv_error_type          => 'ERR_VAL',
                       piv_error_code          => l_err_code,
                       piv_error_message       => l_err_msg,
                       pov_return_status       => l_log_ret_status,
                       pov_error_msg           => l_log_err_msg);
          END LOOP;
          UPDATE /*+ INDEX (xids XXAR_INVOICES_DIST_STG_N13) */ xxar_invoices_dist_stg xids
             SET xids.process_flag      = 'E',
                 xids.error_type        = 'ERR_VAL',
                 xids.last_update_date  = g_sysdate,
                 xids.last_updated_by   = g_last_updated_by,
                 xids.last_update_login = g_login_id
           WHERE xids.leg_dist_segment1 = org_rec_i.leg_dist_segment1
             AND xids.leg_account_class = 'REC'
                --ver1.20.1 changes start
                --AND leg_source_system  != 'NAFSC'
             AND xids.leg_org_name NOT IN ('OU US AR', 'OU CA AR')
                --ver1.20.1 changes end
             AND xids.batch_id = g_new_batch_id
             AND xids.run_sequence_id = g_new_run_seq_id;
          UPDATE /*+ INDEX (xis XXAR_INVOICES_STG_N4) */ xxar_invoices_stg xis
             SET xis.process_flag      = 'E',
                 xis.error_type        = 'ERR_VAL',
                 xis.last_update_date  = g_sysdate,
                 xis.last_updated_by   = g_last_updated_by,
                 xis.last_update_login = g_login_id
           WHERE EXISTS
           (SELECT /*+ INDEX (xds1 XXAR_INVOICES_DIST_STG_N13) */
                   1
                    FROM xxar_invoices_dist_stg xds1
                   WHERE xds1.leg_dist_segment1 =
                         org_rec_i.leg_dist_segment1
                     AND xds1.leg_customer_trx_id = xis.leg_customer_trx_id
                     AND xds1.batch_id = g_new_batch_id
                     AND xds1.run_sequence_id = g_new_run_seq_id
                     AND xds1.leg_account_class = 'REC'
                        --ver1.20.1 changes start
                        --AND leg_source_system  != 'NAFSC'
                     AND leg_org_name NOT IN ('OU US AR', 'OU CA AR')
                  --ver1.20.1 changes end
                  )
             AND xis.batch_id = g_new_batch_id
             AND xis.run_sequence_id = g_new_run_seq_id;
      END;
    END LOOP;
    -- End of ISSC ORG LOOP ---------------------------------------------------------------------------------------------------------------------------------------------------
    --insert into test_error values('Entering into NAFSC loop');
    --ver 1.20.1 changes start
    /*
    -- Start of NAFSC ORG LOOP ---------------------------------------------------------------------------------------------------------------------------------------------------
          FOR org_rec_n IN org_cur_nafsc
          LOOP
             l_r12_org_id    := NULL;
             l_ou_name       := NULL;
             l_sob_id        := NULL;
             l_func_curr     := NULL;
             l_ledger_id     := NULL;
             l_org_name      := NULL;
             l_gl_error      := 'Y';
         l_batch_error   := 'Y';
         l_batch_source  := NULL;
         l_fsc_int_hdr_attr1    := NULL;
         l_plant_credit_office := NULL;
             BEGIN
                l_rec.site := SUBSTR(org_rec_n.interface_header_attribute1, -4);
                l_org_name := xxetn_map_util.get_value(l_rec).operating_unit;
          print_log_message('MOCK2 NAFSC Org Name ' ||l_org_name);    --v 1.12
                IF l_org_name IS NULL
                THEN
                    print_log_message('Couldnt find Org Name for ' || l_rec.site);  --v 1.12
                    FOR r_org_ref_err_rec IN (SELECT interface_txn_id
                                               FROM xxar_invoices_stg xis
                                              WHERE leg_customer_trx_id IN
                                                    (SELECT customer_trx_id
                                                       FROM xxar_interf_hdr_attr_stg xis
                                                      WHERE interface_header_attribute1 = org_rec_n.interface_header_attribute1
                            )
                                                AND leg_source_system = 'NAFSC'
                                                AND batch_id = g_new_batch_id
                                                AND run_sequence_id = g_new_run_seq_id)
                    --Ver 1.10 Changes end
                    LOOP
                      l_err_code := 'ETN_AR_OPERATING UNIT_ERROR';
                      l_err_msg  := 'Error : Cross reference not defined for operating unit in XXETN_MAP_UNIT table';
                      g_retcode  := 1;
                      log_errors(pin_transaction_id => r_org_ref_err_rec.interface_txn_id,
                                 piv_source_column_name => 'interface_header_attribute1',
                                 piv_source_column_value => org_rec_n.interface_header_attribute1,
                                 piv_error_type => 'ERR_VAL',
                                 piv_error_code => l_err_code,
                                 piv_error_message => l_err_msg,
                                 pov_return_status => l_log_ret_status,
                                 pov_error_msg => l_log_err_msg);
                   END LOOP;
                   UPDATE xxar_invoices_dist_stg xids
                      SET xids.process_flag      = 'E',
                          xids.error_type        = 'ERR_VAL',
                          xids.last_update_date  = SYSDATE,
                          xids.last_updated_by   = g_last_updated_by,
                          xids.last_update_login = g_login_id
                    WHERE EXISTS (SELECT 1
                             FROM xxar_interf_hdr_attr_stg xis
                            WHERE xis.interface_header_attribute1 =
                                  org_rec_n.interface_header_attribute1
                              AND xis.customer_trx_id =
                                  xids.leg_customer_trx_id)
                      AND xids.leg_source_system = 'NAFSC'
                      AND xids.batch_id = g_new_batch_id
                      AND xids.run_sequence_id = g_new_run_seq_id;
                   UPDATE xxar_invoices_stg xisa
                      SET xisa.process_flag      = 'E',
                          xisa.error_type        = 'ERR_VAL',
                          xisa.last_update_date  = SYSDATE,
                          xisa.last_updated_by   = g_last_updated_by,
                          xisa.last_update_login = g_login_id
                    WHERE EXISTS (SELECT 1
                             FROM xxar_interf_hdr_attr_stg xis
                            WHERE xis.interface_header_attribute1 =
                                  org_rec_n.interface_header_attribute1
                              AND xis.customer_trx_id =
                                  xisa.leg_customer_trx_id)
                      AND xisa.batch_id = g_new_batch_id
                      AND xisa.run_sequence_id = g_new_run_seq_id
                      AND xisa.leg_source_system = 'NAFSC';
                   -- ver1.10 changes end
                   COMMIT;
                END IF;
             EXCEPTION
                WHEN OTHERS THEN
                   l_err_code := 'ETN_AR_PROCEDURE_EXCEPTION';
                   l_err_msg  := 'Error : Error updating staging table for operating unit' ||
                                 SUBSTR(SQLERRM, 1, 150);
                   g_retcode  := 2;
                   FOR r_org_ref_err_rec1 IN (SELECT interface_txn_id
                                                FROM xxar_invoices_stg xis
                                               WHERE leg_customer_trx_id IN
                                                     (SELECT customer_trx_id
                                                        FROM xxar_interf_hdr_attr_stg xis
                                                       WHERE interface_header_attribute1 = org_rec_n.interface_header_attribute1
                                                      )
                                                 AND leg_source_system = 'NAFSC'
                                                 AND batch_id = g_new_batch_id
                                                 AND run_sequence_id = g_new_run_seq_id)
                   LOOP
                      --Ver 1.10 Changes end
                      log_errors(pin_transaction_id => r_org_ref_err_rec1.interface_txn_id,
                                 piv_source_column_name => 'interface_header_attribute1',
                                 piv_source_column_value => org_rec_n.interface_header_attribute1,
                                 piv_error_type => 'ERR_VAL',
                                 piv_error_code => l_err_code,
                                 piv_error_message => l_err_msg,
                                 pov_return_status => l_log_ret_status,
                                 pov_error_msg => l_log_err_msg);
                   END LOOP;
                   UPDATE xxar_invoices_dist_stg xids
                      SET xids.process_flag      = 'E',
                          xids.error_type        = 'ERR_VAL',
                          xids.last_update_date  = SYSDATE,
                          xids.last_updated_by   = g_last_updated_by,
                          xids.last_update_login = g_login_id
                    WHERE EXISTS (SELECT 1
                             FROM xxar_interf_hdr_attr_stg xis
                            WHERE xis.interface_header_attribute1 =
                                  org_rec_n.interface_header_attribute1
                              AND xis.customer_trx_id =
                                  xids.leg_customer_trx_id)
                      AND xids.leg_source_system = 'NAFSC'
                      AND xids.batch_id = g_new_batch_id
                      AND xids.run_sequence_id = g_new_run_seq_id;
                   UPDATE xxar_invoices_stg xisa
                      SET xisa.process_flag      = 'E',
                          xisa.error_type        = 'ERR_VAL',
                          xisa.last_update_date  = SYSDATE,
                          xisa.last_updated_by   = g_last_updated_by,
                          xisa.last_update_login = g_login_id
                    WHERE EXISTS (SELECT 1
                             FROM xxar_interf_hdr_attr_stg xis
                            WHERE xis.interface_header_attribute1 =
                                  org_rec_n.interface_header_attribute1
                              AND xis.customer_trx_id =
                                  xisa.leg_customer_trx_id)
                      AND xisa.leg_source_system = 'NAFSC'
                      AND xisa.batch_id = g_new_batch_id
                      AND xisa.run_sequence_id = g_new_run_seq_id;
                   -- ver1.10 changes end
                   COMMIT;
             END;
             -- Check whether R12 operating unit in mapping table is already setup
             BEGIN
                IF l_org_name IS NOT NULL
                THEN
                   print_log_message('Validating R12 operating unit ' ||
                                     l_org_name);
                   SELECT hou.organization_id
                         ,hou.NAME
                         ,hou.set_of_books_id
                         ,gll.currency_code
                         ,gll.ledger_id
                     INTO l_r12_org_id
                         ,l_ou_name
                         ,l_sob_id
                         ,l_func_curr
                         ,l_ledger_id
                     FROM apps.hr_operating_units hou
                         ,gl_ledgers              gll
                    WHERE UPPER(hou.NAME) = UPPER(l_org_name)
                      AND hou.set_of_books_id = gll.ledger_id(+)
                      AND TRUNC(NVL(hou.date_to, SYSDATE)) >= TRUNC(SYSDATE);
                    print_log_message('MOCK2:R12 operating id ' ||l_r12_org_id);  --v1.12
                END IF;
                IF l_ledger_id IS NOT NULL
                THEN
                   l_gl_error := 'N';
                   BEGIN
                      SELECT 1
                        INTO l_gl_status
                        FROM gl_periods         glp
                            ,gl_period_statuses gps
                       WHERE UPPER(glp.period_name) = UPPER(gps.period_name)
                         AND glp.period_set_name = g_period_set_name --'ETN Corp Calend'
                         AND g_gl_date BETWEEN glp.start_date AND glp.end_date
                         AND gps.application_id =
                             (SELECT fap.application_id
                                FROM fnd_application_vl fap
                               WHERE fap.application_short_name = 'SQLGL')
                         AND gps.closing_status = 'O'
                         AND ledger_id = l_ledger_id;
                print_log_message('MOCK2:R12 GL Period ' ||l_r12_org_id);  --v1.12
                   EXCEPTION
                      WHEN NO_DATA_FOUND THEN
                         l_gl_error := 'Y';
                         l_err_code := 'ETN_AR_GL_PERIOD_ERROR';
                         g_retcode  := 1;
                         l_err_msg  := 'GL Period not open/defined for GL date ' ||
                                       g_gl_date;
                         FOR r_gl_per_err_rec IN (SELECT interface_txn_id
                                                    FROM xxar_invoices_stg xis
                                                   WHERE leg_customer_trx_id IN
                                                          (SELECT customer_trx_id
                                                            FROM xxar_interf_hdr_attr_stg xis
                                                           WHERE interface_header_attribute1 = org_rec_n.interface_header_attribute1
                                                          )
                                                     AND leg_source_system = 'NAFSC'
                                                     AND batch_id = g_new_batch_id
                                                     AND run_sequence_id = g_new_run_seq_id)
                         LOOP
                            -- ver1.10 changes end
                            log_errors(pin_transaction_id => r_gl_per_err_rec.interface_txn_id,
                                       piv_source_keyname1 => 'GL Period date',
                                       piv_source_keyvalue1 => g_gl_date,
                                       piv_source_column_name => 'R12 Operating Unit',
                                       piv_source_column_value => l_org_name,
                                       piv_error_type => 'ERR_VAL',
                                       piv_error_code => l_err_code,
                                       piv_error_message => l_err_msg,
                                       pov_return_status => l_log_ret_status,
                                       pov_error_msg => l_log_err_msg);
                         END LOOP;
                         UPDATE xxar_invoices_dist_stg xids
                            SET xids.process_flag      = 'E',
                                xids.error_type        = 'ERR_VAL',
                                xids.last_update_date  = SYSDATE,
                                xids.last_updated_by   = g_last_updated_by,
                                xids.last_update_login = g_login_id
                          WHERE EXISTS
                          (SELECT 1
                                   FROM xxar_interf_hdr_attr_stg xis
                                  WHERE xis.interface_header_attribute1 =
                                        org_rec_n.interface_header_attribute1
                                    AND xids.leg_customer_trx_id =
                                        xis.customer_trx_id)
                            AND xids.leg_source_system = 'NAFSC'
                            AND xids.batch_id = g_new_batch_id
                            AND xids.run_sequence_id = g_new_run_seq_id;
                         UPDATE xxar_invoices_stg xisa
                            SET xisa.process_flag      = 'E',
                                xisa.error_type        = 'ERR_VAL',
                                xisa.last_update_date  = SYSDATE,
                                xisa.last_updated_by   = g_last_updated_by,
                                xisa.last_update_login = g_login_id
                          WHERE EXISTS
                          (SELECT 1
                                   FROM xxar_interf_hdr_attr_stg xis
                                  WHERE xis.interface_header_attribute1 =
                                        org_rec_n.interface_header_attribute1
                                    AND xis.customer_trx_id =
                                        xisa.leg_customer_trx_id)
                            AND xisa.leg_source_system = 'NAFSC'
                            AND xisa.batch_id = g_new_batch_id
                            AND xisa.run_sequence_id = g_new_run_seq_id;
                      -- ver1.10 changes end
                      WHEN OTHERS THEN
                         l_gl_error := 'Y';
                         g_retcode  := 2;
                         l_err_code := 'ETN_AR_PROCEDURE_EXCEPTION';
                         l_err_msg  := 'Error : Error validating gl period ' ||
                                       g_gl_date || SUBSTR(SQLERRM, 1, 150);
                         FOR r_gl_per_err1_rec IN (SELECT interface_txn_id
                                                     FROM xxar_invoices_stg xis
                                                    WHERE leg_customer_trx_id IN
                                                          (SELECT customer_trx_id
                                                            FROM xxar_interf_hdr_attr_stg xis
                                                           WHERE interface_header_attribute1 = org_rec_n.interface_header_attribute1
                                                          )
                                                      AND leg_source_system = 'NAFSC'
                                                      AND batch_id = g_new_batch_id
                                                      AND run_sequence_id = g_new_run_seq_id)
                         LOOP
                            -- ver1.10 changes end
                            log_errors(pin_transaction_id => r_gl_per_err1_rec.interface_txn_id,
                                       piv_source_keyname1 => 'GL Period date',
                                       piv_source_keyvalue1 => g_gl_date,
                                       piv_source_column_name => 'R12 Operating Unit',
                                       piv_source_column_value => l_org_name,
                                       piv_error_type => 'ERR_VAL',
                                       piv_error_code => l_err_code,
                                       piv_error_message => l_err_msg,
                                       pov_return_status => l_log_ret_status,
                                       pov_error_msg => l_log_err_msg);
                         END LOOP;
                         UPDATE xxar_invoices_dist_stg xids
                            SET xids.process_flag      = 'E',
                                xids.error_type        = 'ERR_VAL',
                                xids.last_update_date  = SYSDATE,
                                xids.last_updated_by   = g_last_updated_by,
                                xids.last_update_login = g_login_id
                          WHERE EXISTS
                          (SELECT 1
                                   FROM xxar_interf_hdr_attr_stg xis
                                  WHERE xis.interface_header_attribute1 =
                                        org_rec_n.interface_header_attribute1
                                    AND xis.customer_trx_id =
                                        xids.leg_customer_trx_id)
                            AND xids.leg_source_system = 'NAFSC'
                            AND xids.batch_id = g_new_batch_id
                            AND xids.run_sequence_id = g_new_run_seq_id;
                         UPDATE xxar_invoices_stg xisa
                            SET xisa.process_flag      = 'E',
                                xisa.error_type        = 'ERR_VAL',
                                xisa.last_update_date  = SYSDATE,
                                xisa.last_updated_by   = g_last_updated_by,
                                xisa.last_update_login = g_login_id
                          WHERE EXISTS
                          (SELECT 1
                                   FROM xxar_interf_hdr_attr_stg xis
                                  WHERE xis.interface_header_attribute1 =
                                        org_rec_n.interface_header_attribute1
                                    AND xis.customer_trx_id =
                                        xisa.leg_customer_trx_id)
                            AND xisa.leg_source_system = 'NAFSC'
                            AND xisa.batch_id = g_new_batch_id
                            AND xisa.run_sequence_id = g_new_run_seq_id;
                   END;
                END IF;
             -- Check batch source
       --      BEGIN
          l_batch_source := NULL;
          l_plant_credit_office := NULL;
                IF l_org_name IS NOT NULL AND l_r12_org_id IS NOT NULL
                THEN
                   print_log_message('Deriving and validating batch source ' || l_org_name);
                   --SELECT DECODE(xmu.ar_credit_office, NULL, g_batch_source||'-'||org_rec.leg_dist_segment1, g_batch_source||'-'||xmu.ar_credit_office)
                     --INTO l_batch_source
             SELECT DECODE(xmu.ar_credit_office, NULL, SUBSTR(org_rec_n.interface_header_attribute1, -4), xmu.ar_credit_office)
             INTO l_plant_credit_office
                     FROM xxetn_map_unit xmu
                    WHERE operating_unit = l_org_name
            AND xmu.site = SUBSTR(org_rec_n.interface_header_attribute1, -4);
            l_batch_source := g_batch_source||' '||l_plant_credit_office;
            print_log_message('MOCK2:R12 l_batch_source: ' ||l_batch_source);  --v1.12
                END IF;
                IF l_batch_source IS NOT NULL
                THEN
                   l_batch_error := 'N';
                   BEGIN
                      SELECT 1
                        INTO l_source_status
                        FROM ra_batch_sources_all rbs
                       WHERE UPPER (name) = l_batch_source
                 AND org_id = l_r12_org_id;
              print_log_message('MOCK2:R12 l_batch_source1: ' ||l_batch_source);  --v1.12
                   EXCEPTION
                      WHEN NO_DATA_FOUND THEN
                         l_batch_error := 'Y';
                         l_err_code := 'ETN_AR_BATCH_SOURCE_ERROR';
                         g_retcode  := 1;
                         l_err_msg  := 'Batch Source ' ||l_batch_source||' not defined for R12 operating unit '||l_org_name;
                         FOR r_bs_per_err_rec IN (SELECT interface_txn_id
                                                    FROM xxar_invoices_stg xis
                                                   WHERE leg_customer_trx_id IN
                                                          (SELECT customer_trx_id
                                                            FROM xxar_interf_hdr_attr_stg xis
                                                           WHERE interface_header_attribute1 = org_rec_n.interface_header_attribute1
                                                          )
                                                     AND leg_source_system = 'NAFSC'
                                                     AND batch_id = g_new_batch_id
                                                     AND run_sequence_id = g_new_run_seq_id)
                         LOOP
                            log_errors(pin_transaction_id => r_bs_per_err_rec.interface_txn_id,
                                       piv_source_keyname1 => 'R12 Operating Unit',
                                       piv_source_keyvalue1 => l_org_name,
                                       piv_source_column_name => 'Batch Source',
                                       piv_source_column_value => l_batch_source,
                                       piv_error_type => 'ERR_VAL',
                                       piv_error_code => l_err_code,
                                       piv_error_message => l_err_msg,
                                       pov_return_status => l_log_ret_status,
                                       pov_error_msg => l_log_err_msg);
                         END LOOP;
                         UPDATE xxar_invoices_dist_stg xids
                            SET xids.process_flag      = 'E',
                                xids.error_type        = 'ERR_VAL',
                                xids.last_update_date  = SYSDATE,
                                xids.last_updated_by   = g_last_updated_by,
                                xids.last_update_login = g_login_id
                          WHERE EXISTS
                          (SELECT 1
                                   FROM xxar_interf_hdr_attr_stg xis
                                  WHERE xis.interface_header_attribute1 =
                                        org_rec_n.interface_header_attribute1
                                    AND xis.customer_trx_id =
                                        xids.leg_customer_trx_id)
                            AND xids.leg_source_system = 'NAFSC'
                            AND xids.batch_id = g_new_batch_id
                            AND xids.run_sequence_id = g_new_run_seq_id;
                         UPDATE xxar_invoices_stg xisa
                            SET xisa.process_flag      = 'E',
                                xisa.error_type        = 'ERR_VAL',
                                xisa.last_update_date  = SYSDATE,
                                xisa.last_updated_by   = g_last_updated_by,
                                xisa.last_update_login = g_login_id
                          WHERE EXISTS
                          (SELECT 1
                                   FROM xxar_interf_hdr_attr_stg xis
                                  WHERE xis.interface_header_attribute1 =
                                        org_rec_n.interface_header_attribute1
                                    AND xis.customer_trx_id =
                                        xisa.leg_customer_trx_id)
                            AND xisa.leg_source_system = 'NAFSC'
                            AND xisa.batch_id = g_new_batch_id
                            AND xisa.run_sequence_id = g_new_run_seq_id;
                      WHEN OTHERS THEN
                         l_batch_error := 'Y';
                         g_retcode  := 2;
                         l_err_code := 'ETN_AR_PROCEDURE_EXCEPTION';
                         l_err_msg  := 'Error : Error validating batch source ' ||
                                       l_batch_source || SUBSTR(SQLERRM, 1, 150);
                         FOR r_bs_per_err1_rec IN (SELECT interface_txn_id
                                                     FROM xxar_invoices_dist_stg xis
                                                    WHERE leg_customer_trx_id IN
                                                          (SELECT customer_trx_id
                                                            FROM xxar_interf_hdr_attr_stg xis
                                                           WHERE interface_header_attribute1 = org_rec_n.interface_header_attribute1
                                                          )
                                                      AND leg_source_system = 'NAFSC'
                                                      AND batch_id = g_new_batch_id
                                                      AND run_sequence_id = g_new_run_seq_id)
                         LOOP
                            log_errors(pin_transaction_id => r_bs_per_err1_rec.interface_txn_id,
                                       piv_source_keyname1 => 'R12 Operating Unit',
                                       piv_source_keyvalue1 => l_org_name,
                                       piv_source_column_name => 'Batch Source',
                                       piv_source_column_value => l_batch_source,
                                       piv_error_type => 'ERR_VAL',
                                       piv_error_code => l_err_code,
                                       piv_error_message => l_err_msg,
                                       pov_return_status => l_log_ret_status,
                                       pov_error_msg => l_log_err_msg);
                         END LOOP;
                         UPDATE xxar_invoices_dist_stg xids
                            SET xids.process_flag      = 'E'
                               ,xids.error_type        = 'ERR_VAL'
                               ,xids.last_update_date  = SYSDATE
                               ,xids.last_updated_by   = g_last_updated_by
                               ,xids.last_update_login = g_login_id
                          WHERE EXISTS
                                  (SELECT 1
                                    FROM xxar_interf_hdr_attr_stg xis
                                   WHERE xis.interface_header_attribute1 = org_rec_n.interface_header_attribute1
                                   AND xis.customer_trx_id = xids.leg_customer_trx_id
                                  )
                            AND xids.leg_source_system = 'NAFSC'
                            AND xids.batch_id = g_new_batch_id
                            AND xids.run_sequence_id = g_new_run_seq_id;
                         UPDATE xxar_invoices_stg xisa
                            SET xisa.process_flag      = 'E',
                                xisa.error_type        = 'ERR_VAL',
                                xisa.last_update_date  = SYSDATE,
                                xisa.last_updated_by   = g_last_updated_by,
                                xisa.last_update_login = g_login_id
                          WHERE EXISTS
                          (SELECT 1
                                   FROM xxar_interf_hdr_attr_stg xis
                                  WHERE xis.interface_header_attribute1 =
                                        org_rec_n.interface_header_attribute1
                                    AND xis.customer_trx_id =
                                        xisa.leg_customer_trx_id)
                            AND xisa.leg_source_system = 'NAFSC'
                            AND xisa.batch_id = g_new_batch_id
                            AND xisa.run_sequence_id = g_new_run_seq_id;
                   END;
                END IF;
                print_log_message('NAFSC OrgID Update : '|| org_rec_n.interface_header_attribute1 || '; ' || g_new_batch_id|| '; ' || g_new_run_seq_id );
                IF l_r12_org_id IS NOT NULL     -- SS
                -- IF NVL(l_gl_error, 'N') = 'N' AND NVL(l_batch_error, 'N') = 'N'   -- SS
                THEN
                   UPDATE xxar_invoices_stg xisa
                      SET xisa.org_id          = l_r12_org_id
                         ,xisa.set_of_books_id = l_sob_id
                         ,xisa.func_curr       = l_func_curr
                         ,xisa.ledger_id       = l_ledger_id
               ,xisa.batch_source_name = l_batch_source
               ,xisa.header_attribute9 = l_plant_credit_office
                    WHERE EXISTS
                          (SELECT 1
                            FROM xxar_interf_hdr_attr_stg xis
                           WHERE xis.interface_header_attribute1 = org_rec_n.interface_header_attribute1
                           AND xis.customer_trx_id = xisa.leg_customer_trx_id
                          )
                      AND xisa.leg_source_system = 'NAFSC'
                      AND xisa.batch_id = g_new_batch_id
                      AND xisa.run_sequence_id = g_new_run_seq_id;
              IF SQL%FOUND THEN
                        print_log_message('FSC OrgID update successful ' || SQL%ROWCOUNT);
                      ELSE
                        print_log_message('FSC OrgID update UNsuccessful');
                      END IF;
              print_log_message('MOCK2:R12 UPDATE query: ' ||l_batch_source);  --v1.12
                END IF;
                COMMIT;
             EXCEPTION
                WHEN NO_DATA_FOUND THEN
                   g_retcode := 1;
                   FOR r_r12_org_err_rec IN (SELECT interface_txn_id
                                               FROM xxar_invoices_stg xis
                                              WHERE leg_customer_trx_id IN
                                                      (SELECT customer_trx_id
                                                        FROM xxar_interf_hdr_attr_stg xis
                                                       WHERE interface_header_attribute1 = org_rec_n.interface_header_attribute1
                                                      )
                                                AND batch_id = g_new_batch_id
                                                AND run_sequence_id = g_new_run_seq_id)
                   LOOP
                      -- ver1.10 changes end
                      l_err_code := 'ETN_AR_OPERATING UNIT_ERROR';
                      l_err_msg  := 'Error : Operating unit not setup';
                      log_errors(pin_transaction_id => r_r12_org_err_rec.interface_txn_id,
                                 piv_source_column_name => 'R12 Operating Unit',
                                 piv_source_column_value => l_org_name,
                                 piv_error_type => 'ERR_VAL',
                                 piv_error_code => l_err_code,
                                 piv_error_message => l_err_msg,
                                 pov_return_status => l_log_ret_status,
                                 pov_error_msg => l_log_err_msg);
                   END LOOP;
                   UPDATE xxar_invoices_dist_stg xids
                      SET xids.process_flag      = 'E',
                          xids.error_type        = 'ERR_VAL',
                          xids.last_update_date  = SYSDATE,
                          xids.last_updated_by   = g_last_updated_by,
                          xids.last_update_login = g_login_id
                    WHERE EXISTS
                    (SELECT 1
                             FROM xxar_interf_hdr_attr_stg xis
                            WHERE xis.interface_header_attribute1 =
                                  org_rec_n.interface_header_attribute1
                              AND xis.customer_trx_id = xids.leg_customer_trx_id)
                      AND xids.leg_source_system = 'NAFSC'
                      AND xids.batch_id = g_new_batch_id
                      AND xids.run_sequence_id = g_new_run_seq_id;
                   UPDATE xxar_invoices_stg xisa
                      SET xisa.process_flag      = 'E',
                          xisa.error_type        = 'ERR_VAL',
                          xisa.last_update_date  = SYSDATE,
                          xisa.last_updated_by   = g_last_updated_by,
                          xisa.last_update_login = g_login_id
                    WHERE EXISTS
                    (SELECT 1
                             FROM xxar_interf_hdr_attr_stg xis
                            WHERE xis.interface_header_attribute1 =
                                  org_rec_n.interface_header_attribute1
                              AND xis.customer_trx_id = xisa.leg_customer_trx_id)
                      AND xisa.leg_source_system = 'NAFSC'
                      AND xisa.batch_id = g_new_batch_id
                      AND xisa.run_sequence_id = g_new_run_seq_id;
                   -- ver1.10 changes end
                WHEN OTHERS THEN
                   l_err_code := 'ETN_AR_PROCEDURE_EXCEPTION';
                   g_retcode  := 2;
                   l_err_msg  := 'Error : Error fetching R12 operating unit' ||
                                 SUBSTR(SQLERRM, 1, 150);
                   FOR r_r12_org_err1_rec IN (SELECT interface_txn_id
                                                FROM xxar_invoices_stg xis
                                               WHERE leg_customer_trx_id IN
                                                      (SELECT customer_trx_id
                                                        FROM xxar_interf_hdr_attr_stg xis
                                                       WHERE interface_header_attribute1 = org_rec_n.interface_header_attribute1
                                                      )
                                                 AND leg_source_system = 'NAFSC'
                                                 AND batch_id = g_new_batch_id
                                                 AND run_sequence_id = g_new_run_seq_id)
                   LOOP
                      -- ver1.10 changes end
                      log_errors(pin_transaction_id => r_r12_org_err1_rec.interface_txn_id,
                                 piv_source_column_name => 'R12 Operating Unit',
                                 piv_source_column_value => l_org_name,
                                 piv_error_type => 'ERR_VAL',
                                 piv_error_code => l_err_code,
                                 piv_error_message => l_err_msg,
                                 pov_return_status => l_log_ret_status,
                                 pov_error_msg => l_log_err_msg);
                   END LOOP;
                   UPDATE xxar_invoices_dist_stg xids
                      SET xids.process_flag      = 'E',
                          xids.error_type        = 'ERR_VAL',
                          xids.last_update_date  = SYSDATE,
                          xids.last_updated_by   = g_last_updated_by,
                          xids.last_update_login = g_login_id
                    WHERE EXISTS
                          (SELECT 1
                             FROM xxar_interf_hdr_attr_stg xis
                            WHERE interface_header_attribute1 =
                                  org_rec_n.interface_header_attribute1
                              AND xis.customer_trx_id = xids.leg_customer_trx_id)
                      AND xids.leg_source_system = 'NAFSC'
                      AND xids.batch_id = g_new_batch_id
                      AND xids.run_sequence_id = g_new_run_seq_id;
                   UPDATE xxar_invoices_stg xisa
                      SET xisa.process_flag      = 'E',
                          xisa.error_type        = 'ERR_VAL',
                          xisa.last_update_date  = SYSDATE,
                          xisa.last_updated_by   = g_last_updated_by,
                          xisa.last_update_login = g_login_id
                    WHERE EXISTS
                    (SELECT 1
                             FROM xxar_interf_hdr_attr_stg xis
                            WHERE xis.interface_header_attribute1 =
                                  org_rec_n.interface_header_attribute1
                              AND xis.customer_trx_id = xisa.leg_customer_trx_id)
                      AND xisa.leg_source_system = 'NAFSC'
                      AND xisa.batch_id = g_new_batch_id
                      AND xisa.run_sequence_id = g_new_run_seq_id;
             END;
          END LOOP;
    -- End of NAFSC ORG LOOP ---------------------------------------------------------------------------------------------------------------------------------------------------
          COMMIT;
    */
    -- Start of NAFSC ORG LOOP ---------------------------------------------------------------------------------------------------------------------------------------------------
    FOR org_rec_n IN org_cur_nafsc LOOP
      l_r12_org_id          := NULL;
      l_ou_name             := NULL;
      l_sob_id              := NULL;
      l_func_curr           := NULL;
      l_ledger_id           := NULL;
      l_org_name            := NULL;
      l_gl_error            := 'Y';
      l_batch_error         := 'Y';
      l_batch_source        := NULL;
      l_fsc_int_hdr_attr1   := NULL;
      l_plant_credit_office := NULL;
      --ver.1.20.2 changes start
      l_credit_office := NULL;
      --ver.1.20.2 changes end
      --ver1.25 changes start
      l_inv_country     := NULL;
      l_inv_site_status := NULL;
      --ver1.25 changes end
      BEGIN
        l_rec.site := SUBSTR(org_rec_n.leg_interface_hdr_attribute1, -4);
        l_org_name := xxetn_map_util.get_value(l_rec).operating_unit;
        print_log_message('MOCK2 NAFSC Org Name ' || l_org_name);
        IF l_org_name IS NULL THEN
          print_log_message('Couldnt find Org Name for ' || l_rec.site);
          FOR r_org_ref_err_rec IN (SELECT interface_txn_id
                                      FROM xxar_invoices_stg xis
                                     WHERE leg_interface_hdr_attribute1 =
                                           org_rec_n.leg_interface_hdr_attribute1
                                       AND leg_operating_unit IN
                                           ('OU US AR', 'OU CA AR')
                                       AND batch_id = g_new_batch_id
                                       AND run_sequence_id =
                                           g_new_run_seq_id) LOOP
            l_err_code := 'ETN_AR_OPERATING UNIT_ERROR';
            l_err_msg  := 'Error : Cross reference not defined for operating unit in XXETN_MAP_UNIT table';
            g_retcode  := 1;
            log_errors(pin_transaction_id      => r_org_ref_err_rec.interface_txn_id,
                       piv_source_column_name  => 'leg_interface_hdr_attribute1',
                       piv_source_column_value => org_rec_n.leg_interface_hdr_attribute1,
                       piv_error_type          => 'ERR_VAL',
                       piv_error_code          => l_err_code,
                       piv_error_message       => l_err_msg,
                       pov_return_status       => l_log_ret_status,
                       pov_error_msg           => l_log_err_msg);
          END LOOP;
          UPDATE /*+ INDEX (xids XXAR_INVOICES_DIST_STG_N6) */ xxar_invoices_dist_stg xids
             SET xids.process_flag      = 'E',
                 xids.error_type        = 'ERR_VAL',
                 xids.last_update_date  = g_sysdate,
                 xids.last_updated_by   = g_last_updated_by,
                 xids.last_update_login = g_login_id
           WHERE EXISTS
           (SELECT /*+ INDEX (xis XXAR_INVOICES_STG_N4) */
                   1
                    FROM xxar_invoices_stg xis
                   WHERE -- xis.interface_header_attribute1 = modified for v1.61
                         xis.leg_interface_hdr_attribute1 = org_rec_n.leg_interface_hdr_attribute1
                     AND xis.leg_customer_trx_id = xids.leg_customer_trx_id)
             AND xids.leg_org_name IN ('OU US AR', 'OU CA AR')
             AND xids.batch_id = g_new_batch_id
             AND xids.run_sequence_id = g_new_run_seq_id;
          UPDATE xxar_invoices_stg xisa
             SET xisa.process_flag      = 'E',
                 xisa.error_type        = 'ERR_VAL',
                 xisa.last_update_date  = g_sysdate,
                 xisa.last_updated_by   = g_last_updated_by,
                 xisa.last_update_login = g_login_id
           WHERE -- xisa.interface_header_attribute1 = commented for v1.61
		  xisa.leg_interface_hdr_attribute1 =
                 org_rec_n.leg_interface_hdr_attribute1
             AND xisa.batch_id = g_new_batch_id
             AND xisa.run_sequence_id = g_new_run_seq_id
             AND xisa.leg_operating_unit IN ('OU US AR', 'OU CA AR');
          COMMIT;
        END IF;
      EXCEPTION
        WHEN OTHERS THEN
          l_err_code := 'ETN_AR_PROCEDURE_EXCEPTION';
          l_err_msg  := 'Error : Error updating staging table for operating unit' ||
                        SUBSTR(SQLERRM, 1, 150);
          g_retcode  := 2;
          FOR r_org_ref_err_rec1 IN (SELECT interface_txn_id
                                       FROM xxar_invoices_stg xis
                                      WHERE leg_interface_hdr_attribute1 =
                                            org_rec_n.leg_interface_hdr_attribute1
                                        AND leg_operating_unit IN
                                            ('OU US AR', 'OU CA AR')
                                        AND batch_id = g_new_batch_id
                                        AND run_sequence_id =
                                            g_new_run_seq_id) LOOP
            --Ver 1.10 Changes end
            log_errors(pin_transaction_id      => r_org_ref_err_rec1.interface_txn_id,
                       piv_source_column_name  => 'interface_header_attribute1',
                       piv_source_column_value => org_rec_n.leg_interface_hdr_attribute1,
                       piv_error_type          => 'ERR_VAL',
                       piv_error_code          => l_err_code,
                       piv_error_message       => l_err_msg,
                       pov_return_status       => l_log_ret_status,
                       pov_error_msg           => l_log_err_msg);
          END LOOP;
          UPDATE /*+ INDEX (xids XXAR_INVOICES_DIST_STG_N6) */ xxar_invoices_dist_stg xids
             SET xids.process_flag      = 'E',
                 xids.error_type        = 'ERR_VAL',
                 xids.last_update_date  = g_sysdate,
                 xids.last_updated_by   = g_last_updated_by,
                 xids.last_update_login = g_login_id
           WHERE EXISTS
           (SELECT /*+ INDEX (xis XXAR_INVOICES_STG_N4) */
                   1
                    FROM xxar_invoices_stg xis
                   WHERE xis.leg_interface_hdr_attribute1 =
                         org_rec_n.leg_interface_hdr_attribute1
                     AND xis.leg_customer_trx_id = xids.leg_customer_trx_id)
             AND xids.leg_org_name IN ('OU US AR', 'OU CA AR')
             AND xids.batch_id = g_new_batch_id
             AND xids.run_sequence_id = g_new_run_seq_id;
          UPDATE xxar_invoices_stg xisa
             SET xisa.process_flag      = 'E',
                 xisa.error_type        = 'ERR_VAL',
                 xisa.last_update_date  = g_sysdate,
                 xisa.last_updated_by   = g_last_updated_by,
                 xisa.last_update_login = g_login_id
           WHERE xisa.leg_interface_hdr_attribute1 =
                 org_rec_n.leg_interface_hdr_attribute1
             AND xisa.leg_operating_unit IN ('OU US AR', 'OU CA AR')
             AND xisa.batch_id = g_new_batch_id
             AND xisa.run_sequence_id = g_new_run_seq_id;
          -- ver1.10 changes end
          COMMIT;
      END;
      -- Check whether R12 operating unit in mapping table is already setup
      BEGIN
        IF l_org_name IS NOT NULL THEN
          /*     insert into test_error

          values('l_org_name is null');

          commit;*/
          print_log_message('Validating R12 operating unit ' || l_org_name);
          SELECT hou.organization_id,
                 hou.NAME,
                 hou.set_of_books_id,
                 gll.currency_code,
                 gll.ledger_id
            INTO l_r12_org_id,
                 l_ou_name,
                 l_sob_id,
                 l_func_curr,
                 l_ledger_id
            FROM apps.hr_operating_units hou, gl_ledgers gll
           WHERE UPPER(hou.NAME) = UPPER(l_org_name)
             AND hou.set_of_books_id = gll.ledger_id(+)
             AND TRUNC(NVL(hou.date_to, g_sysdate)) >= TRUNC(g_sysdate);
          print_log_message('MOCK2:R12 operating id ' || l_r12_org_id); --v1.12
          /*  insert into test_error

          values(l_org_name);

          commit;*/
        END IF;
        IF l_ledger_id IS NOT NULL THEN
          l_gl_error := 'N';
          BEGIN
            SELECT 1
              INTO l_gl_status
              FROM gl_periods glp, gl_period_statuses gps
             WHERE UPPER(glp.period_name) = UPPER(gps.period_name)
               AND glp.period_set_name = g_period_set_name --'ETN Corp Calend'
               AND g_gl_date BETWEEN glp.start_date AND glp.end_date
               AND gps.application_id =
                   (SELECT fap.application_id
                      FROM fnd_application_vl fap
                     WHERE fap.application_short_name = 'SQLGL')
               AND gps.closing_status = 'O'
               AND ledger_id = l_ledger_id;
            print_log_message('MOCK2:R12 GL Period ' || l_r12_org_id); --v1.12
          EXCEPTION
            WHEN NO_DATA_FOUND THEN
              l_gl_error := 'Y';
              l_err_code := 'ETN_AR_GL_PERIOD_ERROR';
              g_retcode  := 1;
              l_err_msg  := 'GL Period not open/defined for GL date ' ||
                            g_gl_date;
              FOR r_gl_per_err_rec IN (SELECT interface_txn_id
                                         FROM xxar_invoices_stg xis
                                        WHERE leg_interface_hdr_attribute1 =
                                              org_rec_n.leg_interface_hdr_attribute1
                                          AND leg_operating_unit IN
                                              ('OU US AR', 'OU CA AR')
                                          AND batch_id = g_new_batch_id
                                          AND run_sequence_id =
                                              g_new_run_seq_id) LOOP
                -- ver1.10 changes end
                log_errors(pin_transaction_id      => r_gl_per_err_rec.interface_txn_id,
                           piv_source_keyname1     => 'GL Period date',
                           piv_source_keyvalue1    => g_gl_date,
                           piv_source_column_name  => 'R12 Operating Unit',
                           piv_source_column_value => l_org_name,
                           piv_error_type          => 'ERR_VAL',
                           piv_error_code          => l_err_code,
                           piv_error_message       => l_err_msg,
                           pov_return_status       => l_log_ret_status,
                           pov_error_msg           => l_log_err_msg);
              END LOOP;
              UPDATE /*+ INDEX (xids XXAR_INVOICES_DIST_STG_N6) */ xxar_invoices_dist_stg xids
                 SET xids.process_flag      = 'E',
                     xids.error_type        = 'ERR_VAL',
                     xids.last_update_date  = g_sysdate,
                     xids.last_updated_by   = g_last_updated_by,
                     xids.last_update_login = g_login_id
               WHERE EXISTS (SELECT /*+ INDEX (xis XXAR_INVOICES_STG_N4) */
                       1
                        FROM xxar_invoices_stg xis
                       WHERE xis.leg_interface_hdr_attribute1 =
                             org_rec_n.leg_interface_hdr_attribute1
                         AND xids.leg_customer_trx_id =
                             xis.leg_customer_trx_id)
                 AND xids.leg_org_name IN ('OU US AR', 'OU CA AR')
                 AND xids.batch_id = g_new_batch_id
                 AND xids.run_sequence_id = g_new_run_seq_id;
              UPDATE xxar_invoices_stg xisa
                 SET xisa.process_flag      = 'E',
                     xisa.error_type        = 'ERR_VAL',
                     xisa.last_update_date  = g_sysdate,
                     xisa.last_updated_by   = g_last_updated_by,
                     xisa.last_update_login = g_login_id
               WHERE xisa.leg_interface_hdr_attribute1 =
                     org_rec_n.leg_interface_hdr_attribute1
                 AND xisa.leg_operating_unit IN ('OU US AR', 'OU CA AR')
                 AND xisa.batch_id = g_new_batch_id
                 AND xisa.run_sequence_id = g_new_run_seq_id;
              -- ver1.10 changes end
            WHEN OTHERS THEN
              l_gl_error := 'Y';
              g_retcode  := 2;
              l_err_code := 'ETN_AR_PROCEDURE_EXCEPTION';
              l_err_msg  := 'Error : Error validating gl period ' ||
                            g_gl_date || SUBSTR(SQLERRM, 1, 150);
              FOR r_gl_per_err1_rec IN (SELECT interface_txn_id
                                          FROM xxar_invoices_stg xis
                                         WHERE leg_interface_hdr_attribute1 =
                                               org_rec_n.leg_interface_hdr_attribute1
                                           AND leg_operating_unit IN
                                               ('OU US AR', 'OU CA AR')
                                           AND batch_id = g_new_batch_id
                                           AND run_sequence_id =
                                               g_new_run_seq_id) LOOP
                -- ver1.10 changes end
                log_errors(pin_transaction_id      => r_gl_per_err1_rec.interface_txn_id,
                           piv_source_keyname1     => 'GL Period date',
                           piv_source_keyvalue1    => g_gl_date,
                           piv_source_column_name  => 'R12 Operating Unit',
                           piv_source_column_value => l_org_name,
                           piv_error_type          => 'ERR_VAL',
                           piv_error_code          => l_err_code,
                           piv_error_message       => l_err_msg,
                           pov_return_status       => l_log_ret_status,
                           pov_error_msg           => l_log_err_msg);
              END LOOP;
              UPDATE /*+ INDEX (xids XXAR_INVOICES_DIST_STG_N6) */ xxar_invoices_dist_stg xids
                 SET xids.process_flag      = 'E',
                     xids.error_type        = 'ERR_VAL',
                     xids.last_update_date  = g_sysdate,
                     xids.last_updated_by   = g_last_updated_by,
                     xids.last_update_login = g_login_id
               WHERE EXISTS (SELECT /*+ INDEX (xis XXAR_INVOICES_STG_N4) */
                       1
                        FROM xxar_invoices_stg xis
                       WHERE xis.leg_interface_hdr_attribute1 =
                             org_rec_n.leg_interface_hdr_attribute1
                         AND xis.leg_customer_trx_id =
                             xids.leg_customer_trx_id)
                 AND xids.leg_org_name IN ('OU US AR', 'OU CA AR')
                 AND xids.batch_id = g_new_batch_id
                 AND xids.run_sequence_id = g_new_run_seq_id;
              UPDATE xxar_invoices_stg xisa
                 SET xisa.process_flag      = 'E',
                     xisa.error_type        = 'ERR_VAL',
                     xisa.last_update_date  = g_sysdate,
                     xisa.last_updated_by   = g_last_updated_by,
                     xisa.last_update_login = g_login_id
               WHERE xisa.leg_interface_hdr_attribute1 =
                     org_rec_n.leg_interface_hdr_attribute1
                 AND xisa.leg_operating_unit IN ('OU US AR', 'OU CA AR')
                 AND xisa.batch_id = g_new_batch_id
                 AND xisa.run_sequence_id = g_new_run_seq_id;
          END;
        END IF;
        -- Check batch source
        --      BEGIN
        l_batch_source        := NULL;
        l_plant_credit_office := NULL;
        --ver.1.20.2 changes start
        l_credit_office := NULL;
        --ver.1.20.2 changes end
        IF (l_org_name IS NOT NULL AND l_r12_org_id IS NOT NULL) THEN
          /*

             insert into test_error

          values(l_org_name);

          commit;*/
          print_log_message('Deriving and validating batch source ' ||
                            l_org_name);
          --SELECT DECODE(xmu.ar_credit_office, NULL, g_batch_source||'-'||org_rec.leg_dist_segment1, g_batch_source||'-'||xmu.ar_credit_office)
          --INTO l_batch_source
          --ver.1.20.2 changes start
          /*         SELECT DECODE(xmu.ar_credit_office, NULL, SUBSTR(org_rec_n.interface_header_attribute1, -4), xmu.ar_credit_office)

               INTO l_plant_credit_office

                       FROM xxetn_map_unit xmu

                      WHERE operating_unit = l_org_name

              AND xmu.site = SUBSTR(org_rec_n.interface_header_attribute1, -4);

          */
          --ver1.25 changes start
          --commenting below query
          /*      SELECT DECODE(xmu.ar_credit_office, NULL, SUBSTR(org_rec_n.leg_interface_hdr_attribute1, -4), xmu.ar_credit_office), xmu.ar_credit_office

                    INTO l_plant_credit_office, l_credit_office

                    FROM xxetn_map_unit_v xmu

                   WHERE operating_unit = l_org_name

                     AND xmu.site = SUBSTR(org_rec_n.leg_interface_hdr_attribute1, -4);

          */
          --Adding below query
          SELECT DECODE(xmu.ar_credit_office,
                        NULL,
                        SUBSTR(org_rec_n.leg_interface_hdr_attribute1, -4),
                        xmu.ar_credit_office),
                 xmu.ar_credit_office,
                 xmu.country,
                 xmu.site_status
            INTO l_plant_credit_office,
                 l_credit_office,
                 l_inv_country,
                 l_inv_site_status
            FROM xxetn_map_unit_v xmu
           WHERE operating_unit = l_org_name
             AND xmu.site =
                 SUBSTR(org_rec_n.leg_interface_hdr_attribute1, -4);
          --ver1.25 changes end
          --ver.1.20.2 changes end
          l_batch_source := g_batch_source || ' ' || l_plant_credit_office;
          print_log_message('MOCK2:R12 l_batch_source: ' || l_batch_source); --v1.12
        END IF;
        IF l_batch_source IS NOT NULL THEN
          l_batch_error := 'N';
          BEGIN
            SELECT 1
              INTO l_source_status
              FROM ra_batch_sources_all rbs
             WHERE UPPER(NAME) = l_batch_source
               AND org_id = l_r12_org_id;
            print_log_message('MOCK2:R12 l_batch_source1: ' ||
                              l_batch_source); --v1.12
          EXCEPTION
            WHEN NO_DATA_FOUND THEN
              l_batch_error := 'Y';
              l_err_code    := 'ETN_AR_BATCH_SOURCE_ERROR';
              g_retcode     := 1;
              l_err_msg     := 'Batch Source ' || l_batch_source ||
                               ' not defined for R12 operating unit ' ||
                               l_org_name;
              FOR r_bs_per_err_rec IN (SELECT interface_txn_id
                                         FROM xxar_invoices_stg xis
                                        WHERE leg_interface_hdr_attribute1 =
                                              org_rec_n.leg_interface_hdr_attribute1
                                          AND leg_operating_unit IN
                                              ('OU US AR', 'OU CA AR')
                                          AND batch_id = g_new_batch_id
                                          AND run_sequence_id =
                                              g_new_run_seq_id) LOOP
                log_errors(pin_transaction_id      => r_bs_per_err_rec.interface_txn_id,
                           piv_source_keyname1     => 'R12 Operating Unit',
                           piv_source_keyvalue1    => l_org_name,
                           piv_source_column_name  => 'Batch Source',
                           piv_source_column_value => l_batch_source,
                           piv_error_type          => 'ERR_VAL',
                           piv_error_code          => l_err_code,
                           piv_error_message       => l_err_msg,
                           pov_return_status       => l_log_ret_status,
                           pov_error_msg           => l_log_err_msg);
              END LOOP;
              UPDATE /*+ INDEX (xids XXAR_INVOICES_DIST_STG_N6) */ xxar_invoices_dist_stg xids
                 SET xids.process_flag      = 'E',
                     xids.error_type        = 'ERR_VAL',
                     xids.last_update_date  = g_sysdate,
                     xids.last_updated_by   = g_last_updated_by,
                     xids.last_update_login = g_login_id
               WHERE EXISTS (SELECT /*+ INDEX (xis XXAR_INVOICES_STG_N4) */
                       1
                        FROM xxar_invoices_stg xis
                       WHERE xis.leg_interface_hdr_attribute1 =
                             org_rec_n.leg_interface_hdr_attribute1
                         AND xis.leg_customer_trx_id =
                             xids.leg_customer_trx_id)
                 AND xids.leg_org_name IN ('OU US AR', 'OU CA AR')
                 AND xids.batch_id = g_new_batch_id
                 AND xids.run_sequence_id = g_new_run_seq_id;
              UPDATE xxar_invoices_stg xisa
                 SET xisa.process_flag      = 'E',
                     xisa.error_type        = 'ERR_VAL',
                     xisa.last_update_date  = g_sysdate,
                     xisa.last_updated_by   = g_last_updated_by,
                     xisa.last_update_login = g_login_id
               WHERE xisa.leg_interface_hdr_attribute1 =
                     org_rec_n.leg_interface_hdr_attribute1
                 AND xisa.leg_operating_unit IN ('OU US AR', 'OU CA AR')
                 AND xisa.batch_id = g_new_batch_id
                 AND xisa.run_sequence_id = g_new_run_seq_id;
            WHEN OTHERS THEN
              l_batch_error := 'Y';
              g_retcode     := 2;
              l_err_code    := 'ETN_AR_PROCEDURE_EXCEPTION';
              l_err_msg     := 'Error : Error validating batch source ' ||
                               l_batch_source || SUBSTR(SQLERRM, 1, 150);
              FOR r_bs_per_err1_rec IN (SELECT interface_txn_id
                                          FROM xxar_invoices_stg xis
                                         WHERE leg_interface_hdr_attribute1 =
                                               org_rec_n.leg_interface_hdr_attribute1
                                           AND leg_operating_unit IN
                                               ('OU US AR', 'OU CA AR')
                                           AND batch_id = g_new_batch_id
                                           AND run_sequence_id =
                                               g_new_run_seq_id) LOOP
                log_errors(pin_transaction_id      => r_bs_per_err1_rec.interface_txn_id,
                           piv_source_keyname1     => 'R12 Operating Unit',
                           piv_source_keyvalue1    => l_org_name,
                           piv_source_column_name  => 'Batch Source',
                           piv_source_column_value => l_batch_source,
                           piv_error_type          => 'ERR_VAL',
                           piv_error_code          => l_err_code,
                           piv_error_message       => l_err_msg,
                           pov_return_status       => l_log_ret_status,
                           pov_error_msg           => l_log_err_msg);
              END LOOP;
              UPDATE /*+ INDEX (xids XXAR_INVOICES_DIST_STG_N6) */ xxar_invoices_dist_stg xids
                 SET xids.process_flag      = 'E',
                     xids.error_type        = 'ERR_VAL',
                     xids.last_update_date  = g_sysdate,
                     xids.last_updated_by   = g_last_updated_by,
                     xids.last_update_login = g_login_id
               WHERE EXISTS (SELECT /*+ INDEX (xis XXAR_INVOICES_STG_N4) */
                       1
                        FROM xxar_invoices_stg xis
                       WHERE xis.leg_interface_hdr_attribute1 =
                             org_rec_n.leg_interface_hdr_attribute1
                         AND xis.leg_customer_trx_id =
                             xids.leg_customer_trx_id)
                 AND xids.leg_org_name IN ('OU US AR', 'OU CA AR')
                 AND xids.batch_id = g_new_batch_id
                 AND xids.run_sequence_id = g_new_run_seq_id;
              UPDATE xxar_invoices_stg xisa
                 SET xisa.process_flag      = 'E',
                     xisa.error_type        = 'ERR_VAL',
                     xisa.last_update_date  = g_sysdate,
                     xisa.last_updated_by   = g_last_updated_by,
                     xisa.last_update_login = g_login_id
               WHERE xisa.leg_interface_hdr_attribute1 =
                     org_rec_n.leg_interface_hdr_attribute1
                 AND xisa.leg_operating_unit IN ('OU US AR', 'OU CA AR')
                 AND xisa.batch_id = g_new_batch_id
                 AND xisa.run_sequence_id = g_new_run_seq_id;
          END;
        END IF;
        print_log_message('NAFSC OrgID Update : ' ||
                          org_rec_n.leg_interface_hdr_attribute1 || '; ' ||
                          g_new_batch_id || '; ' || g_new_run_seq_id);
        IF l_r12_org_id IS NOT NULL -- SS
        -- IF NVL(l_gl_error, 'N') = 'N' AND NVL(l_batch_error, 'N') = 'N'   -- SS
         THEN
          UPDATE xxar_invoices_stg xisa
             SET xisa.org_id = l_r12_org_id
                 --ver1.27 changes start
                ,
                 xisa.org_name = l_ou_name
                 --ver1.27 changes end
                ,
                 xisa.set_of_books_id   = l_sob_id,
                 xisa.func_curr         = l_func_curr,
                 xisa.ledger_id         = l_ledger_id,
                 xisa.batch_source_name = l_batch_source
                 --ver1.20.3 changes start
                 --,xisa.header_attribute9 = l_plant_credit_office
                ,
                 xisa.header_attribute9 = SUBSTR(org_rec_n.leg_interface_hdr_attribute1,
                                                 -4)
                 --ver1.20.3 changes end
                 --ver.1.20.2 changes start
                ,
                 xisa.credit_office = l_credit_office
          --ver.1.20.2 changes end
           WHERE xisa.leg_interface_hdr_attribute1 =
                 org_rec_n.leg_interface_hdr_attribute1
             AND xisa.leg_operating_unit IN ('OU US AR', 'OU CA AR')
             AND xisa.batch_id = g_new_batch_id
             AND xisa.run_sequence_id = g_new_run_seq_id;
          IF SQL%FOUND THEN
            print_log_message('FSC OrgID update successful ' ||
                              SQL%ROWCOUNT);
          ELSE
            print_log_message('FSC OrgID update UNsuccessful');
          END IF;
          print_log_message('MOCK2:R12 UPDATE query: ' || l_batch_source); --v1.12
        END IF;
        COMMIT;
      EXCEPTION
        WHEN NO_DATA_FOUND THEN
          g_retcode := 1;
          FOR r_r12_org_err_rec IN (SELECT interface_txn_id
                                      FROM xxar_invoices_stg xis
                                     WHERE leg_interface_hdr_attribute1 =
                                           org_rec_n.leg_interface_hdr_attribute1
                                       AND batch_id = g_new_batch_id
                                       AND run_sequence_id =
                                           g_new_run_seq_id) LOOP
            -- ver1.10 changes end
            l_err_code := 'ETN_AR_OPERATING UNIT_ERROR';
            l_err_msg  := 'Error : Operating unit not setup';
            log_errors(pin_transaction_id      => r_r12_org_err_rec.interface_txn_id,
                       piv_source_column_name  => 'R12 Operating Unit',
                       piv_source_column_value => l_org_name,
                       piv_error_type          => 'ERR_VAL',
                       piv_error_code          => l_err_code,
                       piv_error_message       => l_err_msg,
                       pov_return_status       => l_log_ret_status,
                       pov_error_msg           => l_log_err_msg);
          END LOOP;
          UPDATE /*+ INDEX (xids XXAR_INVOICES_DIST_STG_N6) */ xxar_invoices_dist_stg xids
             SET xids.process_flag      = 'E',
                 xids.error_type        = 'ERR_VAL',
                 xids.last_update_date  = g_sysdate,
                 xids.last_updated_by   = g_last_updated_by,
                 xids.last_update_login = g_login_id
           WHERE EXISTS
           (SELECT /*+ INDEX (xis XXAR_INVOICES_STG_N4) */
                   1
                    FROM xxar_invoices_stg xis
                   WHERE xis.leg_interface_hdr_attribute1 =
                         org_rec_n.leg_interface_hdr_attribute1
                     AND xis.leg_customer_trx_id = xids.leg_customer_trx_id)
             AND xids.leg_org_name IN ('OU US AR', 'OU CA AR')
             AND xids.batch_id = g_new_batch_id
             AND xids.run_sequence_id = g_new_run_seq_id;
          UPDATE xxar_invoices_stg xisa
             SET xisa.process_flag      = 'E',
                 xisa.error_type        = 'ERR_VAL',
                 xisa.last_update_date  = g_sysdate,
                 xisa.last_updated_by   = g_last_updated_by,
                 xisa.last_update_login = g_login_id
           WHERE xisa.leg_interface_hdr_attribute1 =
                 org_rec_n.leg_interface_hdr_attribute1
             AND xisa.leg_operating_unit IN ('OU US AR', 'OU CA AR')
             AND xisa.batch_id = g_new_batch_id
             AND xisa.run_sequence_id = g_new_run_seq_id;
          -- ver1.10 changes end
        WHEN OTHERS THEN
          l_err_code := 'ETN_AR_PROCEDURE_EXCEPTION';
          g_retcode  := 2;
          l_err_msg  := 'Error : Error fetching R12 operating unit' ||
                        SUBSTR(SQLERRM, 1, 150);
          FOR r_r12_org_err1_rec IN (SELECT interface_txn_id
                                       FROM xxar_invoices_stg xis
                                      WHERE leg_interface_hdr_attribute1 =
                                            org_rec_n.leg_interface_hdr_attribute1
                                        AND leg_operating_unit IN
                                            ('OU US AR', 'OU CA AR')
                                        AND batch_id = g_new_batch_id
                                        AND run_sequence_id =
                                            g_new_run_seq_id) LOOP
            -- ver1.10 changes end
            log_errors(pin_transaction_id      => r_r12_org_err1_rec.interface_txn_id,
                       piv_source_column_name  => 'R12 Operating Unit',
                       piv_source_column_value => l_org_name,
                       piv_error_type          => 'ERR_VAL',
                       piv_error_code          => l_err_code,
                       piv_error_message       => l_err_msg,
                       pov_return_status       => l_log_ret_status,
                       pov_error_msg           => l_log_err_msg);
          END LOOP;
          UPDATE /*+ INDEX (xids XXAR_INVOICES_DIST_STG_N6) */ xxar_invoices_dist_stg xids
             SET xids.process_flag      = 'E',
                 xids.error_type        = 'ERR_VAL',
                 xids.last_update_date  = g_sysdate,
                 xids.last_updated_by   = g_last_updated_by,
                 xids.last_update_login = g_login_id
           WHERE EXISTS
           (SELECT /*+ INDEX (xis XXAR_INVOICES_STG_N4) */
                   1
                    FROM xxar_invoices_stg xis
                   WHERE leg_interface_hdr_attribute1 =
                         org_rec_n.leg_interface_hdr_attribute1
                     AND xis.leg_customer_trx_id = xids.leg_customer_trx_id)
             AND xids.leg_org_name IN ('OU US AR', 'OU CA AR')
             AND xids.batch_id = g_new_batch_id
             AND xids.run_sequence_id = g_new_run_seq_id;
          UPDATE xxar_invoices_stg xisa
             SET xisa.process_flag      = 'E',
                 xisa.error_type        = 'ERR_VAL',
                 xisa.last_update_date  = g_sysdate,
                 xisa.last_updated_by   = g_last_updated_by,
                 xisa.last_update_login = g_login_id
           WHERE xisa.leg_interface_hdr_attribute1 =
                 org_rec_n.leg_interface_hdr_attribute1
             AND xisa.leg_operating_unit IN ('OU US AR', 'OU CA AR')
             AND xisa.batch_id = g_new_batch_id
             AND xisa.run_sequence_id = g_new_run_seq_id;
      END;
    END LOOP;
    -- End of NAFSC ORG LOOP ---------------------------------------------------------------------------------------------------------------------------------------------------
    COMMIT;
    --ver 1.20.1 changes end
    FOR customer_rec IN customer_cur LOOP
      -- BEGIN
      IF customer_rec.org_id IS NULL THEN
        print_log1_message('Customer validation not done for ' ||
                           customer_rec.leg_customer_number ||
                           ' as R12 Org Id not present');
      ELSE
        l_r12_cust_id     := NULL;
        l_bill_to_addr    := NULL;
        l_ship_to_addr    := NULL;
        l_valid_cust_flag := 'N';
        --ver1.26 changes start
        l_customer_type := NULL;
        --ver1.26 changes end
        --ver1.27 changes start
        l_leg_bill_to_addr := NULL;
        l_leg_orig_sys_ref := NULL;
        --ver1.27 changes end
        BEGIN
          --ver1.27 changes start
          l_leg_bill_to_addr := TRIM(SUBSTR(customer_rec.leg_bill_to_address,
                                            1,
                                            (INSTR(customer_rec.leg_bill_to_address,
                                                   '|') - 1)));
          l_leg_orig_sys_ref := TRIM(SUBSTR(customer_rec.leg_bill_to_address,
                                            (INSTR(customer_rec.leg_bill_to_address,
                                                   '|') + 1)));
          --ver1.8 changes start
          /*              SELECT hca.cust_account_id

                           INTO l_r12_cust_id

                           FROM apps.hz_cust_accounts_all hca

                          WHERE hca.orig_system_reference = TRIM (customer_rec.leg_customer_number)

                            AND hca.status = 'A';

          */
          /* SELECT DISTINCT hca.cust_account_id

             --ver1.26 changes start

             , hca.customer_type

             --ver1.26 changes start

               INTO l_r12_cust_id

             --ver1.26 changes start

                  , l_customer_type

             --ver1.26 changes start

               FROM apps.hz_cust_accounts_all   hca

                   ,apps.hz_cust_acct_sites_all hcas

                   ,apps.hz_cust_site_uses_all  hcsu

              WHERE hcsu.cust_acct_site_id = hcas.cust_acct_site_id

                AND hcas.cust_account_id = hca.cust_account_id

                AND hcsu.location =

                    TRIM(customer_rec.leg_bill_to_address)

                AND NVL(hcas.org_id, 1) = NVL(customer_rec.org_id, 1)

                AND hca.status = 'A'

                AND hcsu.status = 'A'

                AND hcas.status = 'A'

                AND hcsu.site_use_code = 'BILL_TO'

                AND hca.orig_system_reference LIKE

                    '%' || (TRIM(customer_rec.leg_customer_number)) || '%'

             -- v1.14 SS : The above line was as below

             -- '%' || (TRIM(customer_rec.leg_customer_number));-- || '%';

             -- uncommented last '%'

             --ver1.8 changes end

          */
          /** Added for v1.34 **/
          l_plant := NULL;
          BEGIN
            SELECT meaning
              INTO l_plant
              FROM fnd_lookup_values
             WHERE lookup_type = 'XXAR_CUST_CNV_BR_OU_MAP'
               AND enabled_flag = 'Y'
               AND UPPER(description) = UPPER(customer_rec.org_name)
               AND TRUNC(g_sysdate) BETWEEN
                   NVL(start_date_active, g_sysdate - 1) AND
                   NVL(end_date_active, g_sysdate + 1)
               AND LANGUAGE = USERENV('LANG');
          EXCEPTION
            WHEN OTHERS THEN
              print_log_message('When Others of verifying plant in lookup XXAR_CUST_CNV_BR_OU_MAP. Msg: ' ||
                                SQLERRM);
          END;
          --IF UPPER(customer_rec.org_name) = UPPER('Eaton EPS OU') THEN    -- commented for v1.34
          IF NVL(l_plant, 'XXX') = '4470' THEN
            -- added for v1.34
            l_leg_orig_sys_ref := 'EPS.' || REGEXP_SUBSTR(l_leg_orig_sys_ref,
                                                          '[^."]+',
                                                          6);
            SELECT DISTINCT hca.cust_account_id
                            --ver1.26 changes start
                           ,
                            hca.customer_type
            --ver1.26 changes start
              INTO l_r12_cust_id
                   --ver1.26 changes start
                  ,
                   l_customer_type
            --ver1.26 changes start
              FROM apps.hz_cust_accounts_all   hca,
                   apps.hz_cust_acct_sites_all hcas,
                   apps.hz_cust_site_uses_all  hcsu
             WHERE hcsu.cust_acct_site_id = hcas.cust_acct_site_id
               AND hcas.cust_account_id = hca.cust_account_id
               AND hcsu.location = l_leg_bill_to_addr --ver1.27
               AND NVL(hcas.org_id, 1) = NVL(customer_rec.org_id, 1)
               AND hca.status = 'A'
               AND hcsu.status = 'A'
               AND hcas.status = 'A'
               AND hcsu.site_use_code = 'BILL_TO'
                  --AND hca.orig_system_reference LIKE
                  --  '%' || (TRIM(customer_rec.leg_customer_number)) || '%'   -- commented for v1.45
                  -- v1.14 SS : The above line was as below
                  -- '%' || (TRIM(customer_rec.leg_customer_number));-- || '%';
                  -- uncommented last '%'
                  --ver1.8 changes end
               AND hcas.orig_system_reference = l_leg_orig_sys_ref; --ver1.27
          ELSIF ((customer_rec.leg_source_system <> 'NAFSC') OR
                (UPPER(customer_rec.leg_operating_unit) IN
                ('OU USD 1775 TCO', 'OU MXN CORP'))) THEN
            SELECT DISTINCT hca.cust_account_id
                            --ver1.26 changes start
                           ,
                            hca.customer_type
            --ver1.26 changes start
              INTO l_r12_cust_id
                   --ver1.26 changes start
                  ,
                   l_customer_type
            --ver1.26 changes start
              FROM apps.hz_cust_accounts_all   hca,
                   apps.hz_cust_acct_sites_all hcas,
                   apps.hz_cust_site_uses_all  hcsu
             WHERE hcsu.cust_acct_site_id = hcas.cust_acct_site_id
               AND hcas.cust_account_id = hca.cust_account_id
               AND hcsu.location = l_leg_bill_to_addr --ver1.27
               AND NVL(hcas.org_id, 1) = NVL(customer_rec.org_id, 1)
               AND hca.status = 'A'
               AND hcsu.status = 'A'
               AND hcas.status = 'A'
               AND hcsu.site_use_code = 'BILL_TO'
                  -- AND hca.orig_system_reference LIKE
                  --   '%' || (TRIM(customer_rec.leg_customer_number)) || '%'   -- commented for v1.45
                  -- v1.14 SS : The above line was as below
                  -- '%' || (TRIM(customer_rec.leg_customer_number));-- || '%';
                  -- uncommented last '%'
                  --ver1.8 changes end
               AND hcas.orig_system_reference = l_leg_orig_sys_ref; --ver1.27
          ELSE
            SELECT DISTINCT hca.cust_account_id
                            --ver1.26 changes start
                           ,
                            hca.customer_type
            --ver1.26 changes start
              INTO l_r12_cust_id
                   --ver1.26 changes start
                  ,
                   l_customer_type
            --ver1.26 changes start
              FROM apps.hz_cust_accounts_all   hca,
                   apps.hz_cust_acct_sites_all hcas
            --,apps.hz_cust_site_uses_all  hcsu  -- commented for v1.33
            --WHERE hcsu.cust_acct_site_id = hcas.cust_acct_site_id   -- commented for v1.33
             WHERE hcas.cust_account_id = hca.cust_account_id
                  --AND hcsu.location =  l_leg_bill_to_addr  --ver1.27, -- commented for v1.33
               AND hcas.orig_system_reference = l_leg_bill_to_addr -- added for v1.33
               AND NVL(hcas.org_id, 1) = NVL(customer_rec.org_id, 1)
               AND hca.status = 'A'
                  --AND hcsu.status = 'A'              -- commented for v1.33
               AND hcas.status = 'A';
            --AND hcsu.site_use_code = 'BILL_TO' -- commented for v1.33
            --AND hca.orig_system_reference LIKE
            --    '%' || (TRIM(customer_rec.leg_customer_number)) || '%';    -- commented for v1.33
            -- v1.14 SS : The above line was as below
            -- '%' || (TRIM(customer_rec.leg_customer_number));-- || '%';
            -- uncommented last '%'
            --ver1.8 changes end
          END IF;
          --ver1.27 changes end
          l_valid_cust_flag := 'Y';
        EXCEPTION
          WHEN NO_DATA_FOUND THEN
            l_valid_cust_flag := 'N';
            g_retcode         := 1;
            FOR r_r12_cust_err_rec IN (SELECT /*+ INDEX (xis XXAR_INVOICES_STG_N11) */
                                        interface_txn_id
                                         FROM xxar_invoices_stg xis
                                        WHERE leg_customer_number =
                                              customer_rec.leg_customer_number
                                             --1.18 changes start
                                          AND leg_bill_to_address =
                                              customer_rec.leg_bill_to_address
                                             --1.18 changes end
                                             --1.27 changes start
                                          AND org_id = customer_rec.org_id
                                          AND leg_operating_unit =
                                              customer_rec.leg_operating_unit
                                             --1.27 changes end
                                          AND batch_id = g_new_batch_id
                                          AND run_sequence_id =
                                              g_new_run_seq_id) LOOP
              l_err_code := 'ETN_AR_BILL_CUSTOMER_ERROR';
              l_err_msg  := 'Error : Cross reference not defined for customer';
              log_errors(pin_transaction_id      => r_r12_cust_err_rec.interface_txn_id,
                         piv_source_column_name  => 'Legacy Customer Number',
                         piv_source_column_value => customer_rec.leg_customer_number,
                         piv_error_type          => 'ERR_VAL',
                         piv_error_code          => l_err_code,
                         piv_error_message       => l_err_msg,
                         pov_return_status       => l_log_ret_status,
                         pov_error_msg           => l_log_err_msg);
            END LOOP;
            UPDATE /*+ INDEX (xis XXAR_INVOICES_STG_N11) */ xxar_invoices_stg xis
               SET process_flag      = 'E',
                   ERROR_TYPE        = 'ERR_VAL',
                   last_update_date  = g_sysdate,
                   last_updated_by   = g_last_updated_by,
                   last_update_login = g_login_id
             WHERE leg_customer_number = customer_rec.leg_customer_number
                  --1.18 changes start
               AND leg_bill_to_address = customer_rec.leg_bill_to_address
                  --1.18 changes end
                  --1.27 changes start
               AND org_id = customer_rec.org_id
               AND leg_operating_unit = customer_rec.leg_operating_unit
                  --1.27 changes end
               AND batch_id = g_new_batch_id
               AND run_sequence_id = g_new_run_seq_id;
          WHEN OTHERS THEN
            l_valid_cust_flag := 'N';
            g_retcode         := 2;
            l_err_code        := 'ETN_AR_PROCEDURE_EXCEPTION';
            l_err_msg         := 'Error : Error while fetching customer cross reference' ||
                                 SUBSTR(SQLERRM, 1, 150);
            print_log1_message(l_err_msg);
            FOR r_r12_cust_err1_rec IN (SELECT /*+ INDEX (xis XXAR_INVOICES_STG_N11) */
                                         interface_txn_id
                                          FROM xxar_invoices_stg xis
                                         WHERE leg_customer_number =
                                               customer_rec.leg_customer_number
                                              --1.18 changes start
                                           AND leg_bill_to_address =
                                               customer_rec.leg_bill_to_address
                                              --1.18 changes end
                                              --1.27 changes start
                                           AND org_id = customer_rec.org_id
                                           AND leg_operating_unit =
                                               customer_rec.leg_operating_unit
                                              --1.27 changes end
                                           AND batch_id = g_new_batch_id
                                           AND run_sequence_id =
                                               g_new_run_seq_id) LOOP
              log_errors(pin_transaction_id      => r_r12_cust_err1_rec.interface_txn_id,
                         piv_source_column_name  => 'Legacy Customer Number',
                         piv_source_column_value => customer_rec.leg_customer_number,
                         piv_error_type          => 'ERR_VAL',
                         piv_error_code          => l_err_code,
                         piv_error_message       => l_err_msg,
                         pov_return_status       => l_log_ret_status,
                         pov_error_msg           => l_log_err_msg);
            END LOOP;
            UPDATE /*+ INDEX (xis XXAR_INVOICES_STG_N11) */ xxar_invoices_stg xis
               SET process_flag      = 'E',
                   ERROR_TYPE        = 'ERR_VAL',
                   last_update_date  = g_sysdate,
                   last_updated_by   = g_last_updated_by,
                   last_update_login = g_login_id
             WHERE leg_customer_number = customer_rec.leg_customer_number
                  --1.18 changes start
               AND leg_bill_to_address = customer_rec.leg_bill_to_address
                  --1.18 changes end
                  --1.27 changes start
               AND org_id = customer_rec.org_id
               AND leg_operating_unit = customer_rec.leg_operating_unit
                  --1.27 changes end
               AND batch_id = g_new_batch_id
               AND run_sequence_id = g_new_run_seq_id;
        END;
        -- bill to
        BEGIN
          --ver 1.8 changes start
          /*

                         SELECT hcas.cust_acct_site_id

                           INTO l_bill_to_addr

                           FROM apps.hz_cust_acct_sites_all hcas, apps.hz_cust_site_uses_all hcsu

                          WHERE hcsu.cust_acct_site_id = hcas.cust_acct_site_id

                            AND hcas.orig_system_reference = TRIM (customer_rec.leg_bill_to_address)

                            AND NVL (hcas.org_id, 1) = NVL (customer_rec.org_id, 1)

                            AND hcsu.status = 'A'

                            AND hcas.status = 'A'

                            AND hcsu.site_use_code = 'BILL_TO'

                            AND hcas.cust_account_id = l_r12_cust_id;

          */
          --ver1.27 changes start
          /* SELECT hcas.cust_acct_site_id

            INTO l_bill_to_addr

            FROM apps.hz_cust_acct_sites_all hcas

                ,apps.hz_cust_site_uses_all  hcsu

           WHERE hcsu.cust_acct_site_id = hcas.cust_acct_site_id

             AND hcsu.location =

                 TRIM(customer_rec.leg_bill_to_address)

             AND NVL(hcas.org_id, 1) = NVL(customer_rec.org_id, 1)

             AND hcsu.status = 'A'

             AND hcas.status = 'A'

             AND hcsu.site_use_code = 'BILL_TO'

             AND hcas.cust_account_id = l_r12_cust_id;

          --ver 1.8 changes end

          */
          --IF UPPER(customer_rec.org_name) = UPPER('Eaton EPS OU') THEN    -- commented for v1.34
          IF NVL(l_plant, 'XXX') = '4470' THEN
            -- added for v1.34
            SELECT hcas.cust_acct_site_id
              INTO l_bill_to_addr
              FROM apps.hz_cust_acct_sites_all hcas,
                   apps.hz_cust_site_uses_all  hcsu
             WHERE hcsu.cust_acct_site_id = hcas.cust_acct_site_id
               AND hcsu.location = l_leg_bill_to_addr --ver1.27
               AND NVL(hcas.org_id, 1) = NVL(customer_rec.org_id, 1)
               AND hcsu.status = 'A'
               AND hcas.status = 'A'
               AND hcsu.site_use_code = 'BILL_TO'
               AND hcas.cust_account_id = l_r12_cust_id
               AND hcas.orig_system_reference = l_leg_orig_sys_ref; -- 'EPS.'||REGEXP_SUBSTR (l_leg_orig_sys_ref ,'[^."]+',6); --ver1.27;
            --ELSIF UPPER(customer_rec.leg_operating_unit)  IN ('OU USD 1775 TCO', 'OU MXN CORP') THEN
          ELSIF ((customer_rec.leg_source_system <> 'NAFSC') OR
                (UPPER(customer_rec.leg_operating_unit) IN
                ('OU USD 1775 TCO', 'OU MXN CORP'))) THEN
            SELECT hcas.cust_acct_site_id
              INTO l_bill_to_addr
              FROM apps.hz_cust_acct_sites_all hcas,
                   apps.hz_cust_site_uses_all  hcsu
             WHERE hcsu.cust_acct_site_id = hcas.cust_acct_site_id
               AND hcsu.location = l_leg_bill_to_addr --ver1.27
               AND NVL(hcas.org_id, 1) = NVL(customer_rec.org_id, 1)
               AND hcsu.status = 'A'
               AND hcas.status = 'A'
               AND hcsu.site_use_code = 'BILL_TO'
               AND hcas.cust_account_id = l_r12_cust_id
               AND hcas.orig_system_reference = l_leg_orig_sys_ref; --ver1.27
          ELSE
            SELECT hcas.cust_acct_site_id
              INTO l_bill_to_addr
              FROM apps.hz_cust_acct_sites_all hcas
            --,apps.hz_cust_site_uses_all  hcsu           -- commented for v1.34
            --WHERE hcsu.cust_acct_site_id = hcas.cust_acct_site_id -- commented for v1.34
            --AND hcsu.location =  l_leg_bill_to_addr  --ver1.27   -- commented for v1.34
             WHERE hcas.orig_system_reference = l_leg_bill_to_addr -- added for v1.34
               AND NVL(hcas.org_id, 1) = NVL(customer_rec.org_id, 1)
                  --AND hcsu.status = 'A'
               AND hcas.status = 'A'
                  --AND hcsu.site_use_code = 'BILL_TO'
               AND hcas.cust_account_id = l_r12_cust_id;
            --ver 1.8 changes end
          END IF;
          --ver1.27 changes end
          l_valid_cust_flag := 'Y';
        EXCEPTION
          WHEN NO_DATA_FOUND THEN
            g_retcode         := 1;
            l_valid_cust_flag := 'N';
            FOR r_r12_bill_err_rec IN (SELECT /*+ INDEX (xis XXAR_INVOICES_STG_N11) */
                                        interface_txn_id
                                         FROM xxar_invoices_stg xis
                                        WHERE leg_customer_number =
                                              customer_rec.leg_customer_number
                                          AND leg_bill_to_address =
                                              customer_rec.leg_bill_to_address
                                             --1.27 changes start
                                          AND org_id = customer_rec.org_id
                                          AND leg_operating_unit =
                                              customer_rec.leg_operating_unit
                                             --1.27 changes end
                                          AND batch_id = g_new_batch_id
                                          AND run_sequence_id =
                                              g_new_run_seq_id) LOOP
              l_err_code := 'ETN_AR_BILL_CUSTOMER_ERROR';
              l_err_msg  := 'Error : Cross reference not defined for bill to customer';
              log_errors(pin_transaction_id      => r_r12_bill_err_rec.interface_txn_id,
                         piv_source_column_name  => 'Legacy Customer number/ Legacy Bill to address',
                         piv_source_column_value => customer_rec.leg_customer_number ||
                                                    ' / ' ||
                                                    customer_rec.leg_bill_to_address,
                         piv_error_type          => 'ERR_VAL',
                         piv_error_code          => l_err_code,
                         piv_error_message       => l_err_msg,
                         pov_return_status       => l_log_ret_status,
                         pov_error_msg           => l_log_err_msg);
            END LOOP;
            UPDATE /*+ INDEX (xis XXAR_INVOICES_STG_N11) */ xxar_invoices_stg xis
               SET process_flag      = 'E',
                   ERROR_TYPE        = 'ERR_VAL',
                   last_update_date  = g_sysdate,
                   last_updated_by   = g_last_updated_by,
                   last_update_login = g_login_id
             WHERE leg_customer_number = customer_rec.leg_customer_number
               AND leg_bill_to_address = customer_rec.leg_bill_to_address
                  --1.27 changes start
               AND org_id = customer_rec.org_id
               AND leg_operating_unit = customer_rec.leg_operating_unit
                  --1.27 changes end
               AND batch_id = g_new_batch_id
               AND run_sequence_id = g_new_run_seq_id;
          WHEN OTHERS THEN
            l_valid_cust_flag := 'N';
            g_retcode         := 2;
            l_err_code        := 'ETN_AR_PROCEDURE_EXCEPTION';
            l_err_msg         := 'Error : Error while fetching bill to customer cross reference' ||
                                 SUBSTR(SQLERRM, 1, 150);
            print_log1_message(l_err_msg);
            FOR r_r12_bill_err1_rec IN (SELECT /*+ INDEX (xis XXAR_INVOICES_STG_N11) */
                                         interface_txn_id
                                          FROM xxar_invoices_stg xis
                                         WHERE leg_customer_number =
                                               customer_rec.leg_customer_number
                                           AND leg_bill_to_address =
                                               customer_rec.leg_bill_to_address
                                              --1.27 changes start
                                           AND org_id = customer_rec.org_id
                                           AND leg_operating_unit =
                                               customer_rec.leg_operating_unit
                                              --1.27 changes end
                                           AND batch_id = g_new_batch_id
                                           AND run_sequence_id =
                                               g_new_run_seq_id) LOOP
              log_errors(pin_transaction_id      => r_r12_bill_err1_rec.interface_txn_id,
                         piv_source_column_name  => 'Legacy Customer number/ Legacy Bill to address',
                         piv_source_column_value => customer_rec.leg_customer_number ||
                                                    ' / ' ||
                                                    customer_rec.leg_bill_to_address,
                         piv_error_type          => 'ERR_VAL',
                         piv_error_code          => l_err_code,
                         piv_error_message       => l_err_msg,
                         pov_return_status       => l_log_ret_status,
                         pov_error_msg           => l_log_err_msg);
            END LOOP;
            UPDATE /*+ INDEX (xis XXAR_INVOICES_STG_N11) */ xxar_invoices_stg xis
               SET process_flag      = 'E',
                   ERROR_TYPE        = 'ERR_VAL',
                   last_update_date  = g_sysdate,
                   last_updated_by   = g_last_updated_by,
                   last_update_login = g_login_id
             WHERE leg_customer_number = customer_rec.leg_customer_number
               AND leg_bill_to_address = customer_rec.leg_bill_to_address
                  --1.27 changes start
               AND org_id = customer_rec.org_id
               AND leg_operating_unit = customer_rec.leg_operating_unit
                  --1.27 changes end
               AND batch_id = g_new_batch_id
               AND run_sequence_id = g_new_run_seq_id;
        END;
        -- ship to
        IF customer_rec.leg_ship_to_address IS NOT NULL THEN
          BEGIN
            --ver1.8 changes start
            /*                  SELECT hcas.cust_acct_site_id

                                INTO l_ship_to_addr

                                FROM apps.hz_cust_acct_sites_all hcas, apps.hz_cust_site_uses_all hcsu

                               WHERE hcsu.cust_acct_site_id = hcas.cust_acct_site_id

                                 AND hcas.orig_system_reference = TRIM (customer_rec.leg_ship_to_address)

                                 AND NVL (hcas.org_id, 1) = NVL (customer_rec.org_id, 1)

                                 AND hcsu.status = 'A'

                                 AND hcas.status = 'A'

                                 AND hcsu.site_use_code = 'SHIP_TO'

                                 AND hcas.cust_account_id = l_r12_cust_id;

            */
            SELECT hcas.cust_acct_site_id
              INTO l_ship_to_addr
              FROM apps.hz_cust_acct_sites_all hcas,
                   apps.hz_cust_site_uses_all  hcsu
             WHERE hcsu.cust_acct_site_id = hcas.cust_acct_site_id
               AND hcsu.location = TRIM(customer_rec.leg_ship_to_address)
               AND NVL(hcas.org_id, 1) = NVL(customer_rec.org_id, 1)
               AND hcsu.status = 'A'
               AND hcas.status = 'A'
               AND hcsu.site_use_code = 'SHIP_TO'
               AND hcas.cust_account_id = l_r12_cust_id;
            --ver1.8 changes end
            l_valid_cust_flag := 'Y';
          EXCEPTION
            WHEN NO_DATA_FOUND THEN
              l_valid_cust_flag := 'N';
              g_retcode         := 1;
              FOR r_r12_ship_err_rec IN (SELECT interface_txn_id
                                           FROM xxar_invoices_stg xis
                                          WHERE leg_customer_number =
                                                customer_rec.leg_customer_number
                                            AND leg_ship_to_address =
                                                customer_rec.leg_ship_to_address
                                               --1.27 changes start
                                            AND org_id = customer_rec.org_id
                                            AND leg_operating_unit =
                                                customer_rec.leg_operating_unit
                                               --1.27 changes end
                                            AND batch_id = g_new_batch_id
                                            AND run_sequence_id =
                                                g_new_run_seq_id) LOOP
                l_err_code := 'ETN_AR_SHIP_CUSTOMER_ERROR';
                l_err_msg  := 'Error : Cross reference not defined for ship to customer';
                log_errors(pin_transaction_id      => r_r12_ship_err_rec.interface_txn_id,
                           piv_source_column_name  => 'Legacy Customer number/ Legacy ship to address',
                           piv_source_column_value => customer_rec.leg_customer_number ||
                                                      ' / ' ||
                                                      customer_rec.leg_ship_to_address,
                           piv_error_type          => 'ERR_VAL',
                           piv_error_code          => l_err_code,
                           piv_error_message       => l_err_msg,
                           pov_return_status       => l_log_ret_status,
                           pov_error_msg           => l_log_err_msg);
              END LOOP;
              UPDATE xxar_invoices_stg
                 SET process_flag      = 'E',
                     ERROR_TYPE        = 'ERR_VAL',
                     last_update_date  = g_sysdate,
                     last_updated_by   = g_last_updated_by,
                     last_update_login = g_login_id
               WHERE leg_customer_number = customer_rec.leg_customer_number
                 AND leg_ship_to_address = customer_rec.leg_ship_to_address
                    --1.27 changes start
                 AND org_id = customer_rec.org_id
                 AND leg_operating_unit = customer_rec.leg_operating_unit
                    --1.27 changes end
                 AND batch_id = g_new_batch_id
                 AND run_sequence_id = g_new_run_seq_id;
            WHEN OTHERS THEN
              l_valid_cust_flag := 'N';
              l_err_code        := 'ETN_AR_PROCEDURE_EXCEPTION';
              g_retcode         := 2;
              l_err_msg         := 'Error : Error while fetching bill to customer cross reference' ||
                                   SUBSTR(SQLERRM, 1, 150);
              print_log1_message(l_err_msg);
              FOR r_r12_ship_err1_rec IN (SELECT interface_txn_id
                                            FROM xxar_invoices_stg xis
                                           WHERE leg_customer_number =
                                                 customer_rec.leg_customer_number
                                             AND leg_ship_to_address =
                                                 customer_rec.leg_ship_to_address
                                                --1.27 changes start
                                             AND org_id =
                                                 customer_rec.org_id
                                             AND leg_operating_unit =
                                                 customer_rec.leg_operating_unit
                                                --1.27 changes end
                                             AND batch_id = g_new_batch_id
                                             AND run_sequence_id =
                                                 g_new_run_seq_id) LOOP
                log_errors(pin_transaction_id      => r_r12_ship_err1_rec.interface_txn_id,
                           piv_source_column_name  => 'Legacy Customer number/ Legacy ship to address',
                           piv_source_column_value => customer_rec.leg_customer_number ||
                                                      ' / ' ||
                                                      customer_rec.leg_ship_to_address,
                           piv_error_type          => 'ERR_VAL',
                           piv_error_code          => l_err_code,
                           piv_error_message       => l_err_msg,
                           pov_return_status       => l_log_ret_status,
                           pov_error_msg           => l_log_err_msg);
              END LOOP;
              UPDATE xxar_invoices_stg
                 SET process_flag      = 'E',
                     ERROR_TYPE        = 'ERR_VAL',
                     last_update_date  = g_sysdate,
                     last_updated_by   = g_last_updated_by,
                     last_update_login = g_login_id
               WHERE leg_customer_number = customer_rec.leg_customer_number
                 AND leg_ship_to_address = customer_rec.leg_ship_to_address
                    --1.27 changes start
                 AND org_id = customer_rec.org_id
                 AND leg_operating_unit = customer_rec.leg_operating_unit
                    --1.27 changes end
                 AND batch_id = g_new_batch_id
                 AND run_sequence_id = g_new_run_seq_id;
          END;
        ELSE
          l_valid_cust_flag := 'Y';
        END IF;
        IF l_valid_cust_flag = 'Y' THEN
          UPDATE /*+ INDEX (xis XXAR_INVOICES_STG_N11) */ xxar_invoices_stg xis
             SET system_ship_address_id  = l_ship_to_addr,
                 system_bill_customer_id = l_r12_cust_id,
                 system_bill_address_id  = l_bill_to_addr
                 --ver1.26 changes start
                ,
                 customer_type = l_customer_type
          --ver1.26 changes end
           WHERE leg_customer_number = customer_rec.leg_customer_number
             AND leg_bill_to_address = customer_rec.leg_bill_to_address
             AND NVL(leg_ship_to_address, 'NO SHIP') =
                 NVL(customer_rec.leg_ship_to_address,
                     NVL(leg_ship_to_address, 'NO SHIP'))
                --1.27 changes start
             AND org_id = customer_rec.org_id
             AND leg_operating_unit = customer_rec.leg_operating_unit
                --1.27 changes end
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
    FOR currency_rec IN currency_cur LOOP
      l_curr_code := NULL;
      BEGIN
        print_log_message('Validating legacy currency code ' ||
                          currency_rec.leg_currency_code);
        SELECT 1
          INTO l_curr_code
          FROM fnd_currencies fc
         WHERE fc.currency_code = currency_rec.leg_currency_code
           AND fc.enabled_flag = 'Y'
           AND fc.currency_flag = 'Y'
           AND TRUNC(g_sysdate) BETWEEN
               TRUNC(NVL(fc.start_date_active, g_sysdate)) AND
               TRUNC(NVL(fc.end_date_active, g_sysdate));
        UPDATE xxar_invoices_stg
           SET currency_code = currency_rec.leg_currency_code
         WHERE leg_currency_code = currency_rec.leg_currency_code
           AND batch_id = g_new_batch_id
           AND run_sequence_id = g_new_run_seq_id;
        COMMIT;
      EXCEPTION
        WHEN NO_DATA_FOUND THEN
          FOR r_curr_err_rec IN (SELECT interface_txn_id
                                   FROM xxar_invoices_stg xis
                                  WHERE leg_currency_code =
                                        currency_rec.leg_currency_code
                                    AND batch_id = g_new_batch_id
                                    AND run_sequence_id = g_new_run_seq_id) LOOP
            l_err_code := 'ETN_AR_CURRENCY_ERROR';
            g_retcode  := 1;
            l_err_msg  := 'Error : Currency not found in R12 ';
            print_log1_message(l_err_msg || currency_rec.leg_currency_code);
            log_errors(pin_transaction_id      => r_curr_err_rec.interface_txn_id,
                       piv_source_column_name  => 'Legacy currency code',
                       piv_source_column_value => currency_rec.leg_currency_code,
                       piv_error_type          => 'ERR_VAL',
                       piv_error_code          => l_err_code,
                       piv_error_message       => l_err_msg,
                       pov_return_status       => l_log_ret_status,
                       pov_error_msg           => l_log_err_msg);
          END LOOP;
          UPDATE xxar_invoices_stg
             SET process_flag      = 'E',
                 ERROR_TYPE        = 'ERR_VAL',
                 last_update_date  = g_sysdate,
                 last_updated_by   = g_last_updated_by,
                 last_update_login = g_login_id
           WHERE leg_currency_code = currency_rec.leg_currency_code
             AND batch_id = g_new_batch_id
             AND run_sequence_id = g_new_run_seq_id;
          COMMIT;
        WHEN OTHERS THEN
          l_err_code := 'ETN_AR_PROCEDURE_EXCEPTION';
          g_retcode  := 2;
          l_err_msg  := 'Error : Error validating currency ' ||
                        SUBSTR(SQLERRM, 1, 150);
          print_log1_message(l_err_msg || currency_rec.leg_currency_code);
          FOR r_curr_err1_rec IN (SELECT interface_txn_id
                                    FROM xxar_invoices_stg xis
                                   WHERE leg_currency_code =
                                         currency_rec.leg_currency_code
                                     AND batch_id = g_new_batch_id
                                     AND run_sequence_id = g_new_run_seq_id) LOOP
            log_errors(pin_transaction_id      => r_curr_err1_rec.interface_txn_id,
                       piv_source_column_name  => 'Legacy currency code',
                       piv_source_column_value => currency_rec.leg_currency_code,
                       piv_error_type          => 'ERR_VAL',
                       piv_error_code          => l_err_code,
                       piv_error_message       => l_err_msg,
                       pov_return_status       => l_log_ret_status,
                       pov_error_msg           => l_log_err_msg);
          END LOOP;
          UPDATE xxar_invoices_stg
             SET process_flag      = 'E',
                 ERROR_TYPE        = 'ERR_VAL',
                 last_update_date  = g_sysdate,
                 last_updated_by   = g_last_updated_by,
                 last_update_login = g_login_id
           WHERE leg_currency_code = currency_rec.leg_currency_code
             AND batch_id = g_new_batch_id
             AND run_sequence_id = g_new_run_seq_id;
      END;
    END LOOP;
    COMMIT;
    FOR trx_type_rec IN trx_type_cur LOOP
      l_trx_type      := NULL;
      l_trx_type_id   := NULL;
      l_trx_type_name := NULL;
      -- l_cm_term_error := 'N';  -- commented for v1.58
      BEGIN
        print_log_message('Validating legacy transaction type ' ||
                          trx_type_rec.leg_cust_trx_type_name);
        SELECT TRIM(flv.description)
          INTO l_trx_type
          FROM fnd_lookup_values flv
         WHERE TRIM(UPPER(flv.meaning)) =
               TRIM(UPPER(trx_type_rec.leg_cust_trx_type_name))
           AND flv.LANGUAGE = USERENV('LANG')
           AND flv.enabled_flag = 'Y'
              -- AND UPPER(flv.lookup_type) = g_trx_type_lookup        -- Removed UPPER so that index can be used. Table already has value in ALL CAPS
           AND flv.lookup_type = g_trx_type_lookup
           AND TRUNC(g_sysdate) BETWEEN
               TRUNC(NVL(flv.start_date_active, g_sysdate)) AND
               TRUNC(NVL(flv.end_date_active, g_sysdate));
      EXCEPTION
        WHEN NO_DATA_FOUND THEN
          g_retcode := 1;
          FOR r_trx_type_ref_err_rec IN (SELECT interface_txn_id
                                           FROM xxar_invoices_stg xis
                                          WHERE leg_cust_trx_type_name =
                                                trx_type_rec.leg_cust_trx_type_name
                                            AND batch_id = g_new_batch_id
                                            AND run_sequence_id =
                                                g_new_run_seq_id) LOOP
            l_err_code := 'ETN_AR_TRX_TYPE_ERROR';
            l_err_msg  := 'Error : Cross reference not defined for transaction type';
            log_errors(pin_transaction_id      => r_trx_type_ref_err_rec.interface_txn_id,
                       piv_source_column_name  => 'Legacy Transaction type',
                       piv_source_column_value => trx_type_rec.leg_cust_trx_type_name,
                       piv_error_type          => 'ERR_VAL',
                       piv_error_code          => l_err_code,
                       piv_error_message       => l_err_msg,
                       pov_return_status       => l_log_ret_status,
                       pov_error_msg           => l_log_err_msg);
          END LOOP;
          UPDATE xxar_invoices_stg
             SET process_flag      = 'E',
                 ERROR_TYPE        = 'ERR_VAL',
                 last_update_date  = g_sysdate,
                 last_updated_by   = g_last_updated_by,
                 last_update_login = g_login_id
           WHERE leg_cust_trx_type_name =
                 trx_type_rec.leg_cust_trx_type_name
             AND org_id = trx_type_rec.org_id
             AND batch_id = g_new_batch_id
             AND run_sequence_id = g_new_run_seq_id;
          COMMIT;
        WHEN OTHERS THEN
          l_err_code := 'ETN_AR_PROCEDURE_EXCEPTION';
          g_retcode  := 2;
          l_err_msg  := 'Error : Error deriving transaction type from cross reference' ||
                        SUBSTR(SQLERRM, 1, 150);
          FOR r_trx_type_ref_err1_rec IN (SELECT interface_txn_id
                                            FROM xxar_invoices_stg xis
                                           WHERE leg_cust_trx_type_name =
                                                 trx_type_rec.leg_cust_trx_type_name
                                             AND batch_id = g_new_batch_id
                                             AND run_sequence_id =
                                                 g_new_run_seq_id) LOOP
            log_errors(pin_transaction_id      => r_trx_type_ref_err1_rec.interface_txn_id,
                       piv_source_column_name  => 'Legacy Transaction type',
                       piv_source_column_value => trx_type_rec.leg_cust_trx_type_name,
                       piv_error_type          => 'ERR_VAL',
                       piv_error_code          => l_err_code,
                       piv_error_message       => l_err_msg,
                       pov_return_status       => l_log_ret_status,
                       pov_error_msg           => l_log_err_msg);
          END LOOP;
          UPDATE xxar_invoices_stg
             SET process_flag      = 'E',
                 ERROR_TYPE        = 'ERR_VAL',
                 last_update_date  = g_sysdate,
                 last_updated_by   = g_last_updated_by,
                 last_update_login = g_login_id
           WHERE leg_cust_trx_type_name =
                 trx_type_rec.leg_cust_trx_type_name
             AND org_id = trx_type_rec.org_id
             AND batch_id = g_new_batch_id
             AND run_sequence_id = g_new_run_seq_id;
      END;
      BEGIN
        IF l_trx_type IS NOT NULL THEN
          print_log_message('Validating R12 transaction type ' ||
                            l_trx_type);
          SELECT rct.cust_trx_type_id, rct.TYPE
            INTO l_trx_type_id, l_trx_type_name
            FROM ra_cust_trx_types_all rct
           WHERE UPPER(rct.NAME) = UPPER(l_trx_type)
             AND org_id = trx_type_rec.org_id
             AND TRUNC(g_sysdate) BETWEEN
                 TRUNC(NVL(rct.start_date, g_sysdate)) AND
                 TRUNC(NVL(rct.end_date, g_sysdate));
          IF l_trx_type_name IN ('CB') THEN
            g_retcode := 1;
            FOR r_chgbck_err_rec IN (SELECT interface_txn_id
                                       FROM xxar_invoices_stg xis
                                      WHERE leg_cust_trx_type_name =
                                            trx_type_rec.leg_cust_trx_type_name
                                        AND batch_id = g_new_batch_id
                                        AND run_sequence_id =
                                            g_new_run_seq_id) LOOP
              l_err_code := 'ETN_AR_TRX_TYPE_ERROR';
              l_err_msg  := 'Error : Invalid transaction type: Chargeback ';
              log_errors(pin_transaction_id      => r_chgbck_err_rec.interface_txn_id,
                         piv_source_column_name  => 'R12 Transaction type',
                         piv_source_column_value => l_trx_type,
                         piv_error_type          => 'ERR_VAL',
                         piv_error_code          => l_err_code,
                         piv_error_message       => l_err_msg,
                         pov_return_status       => l_log_ret_status,
                         pov_error_msg           => l_log_err_msg);
            END LOOP;
            UPDATE xxar_invoices_stg
               SET process_flag      = 'E',
                   ERROR_TYPE        = 'ERR_VAL',
                   last_update_date  = g_sysdate,
                   last_updated_by   = g_last_updated_by,
                   last_update_login = g_login_id
             WHERE leg_cust_trx_type_name =
                   trx_type_rec.leg_cust_trx_type_name
               AND org_id = trx_type_rec.org_id
               AND batch_id = g_new_batch_id
               AND run_sequence_id = g_new_run_seq_id;
            /*            ELSIF



                    l_trx_type_name = 'CM' AND UPPER (piv_source_name) = 'NAFSC'

                    THEN

                    l_err_code := 'ETN_AR_TRX_TYPE_ERROR';

                    l_err_msg := 'Error : For NAFSC, Credit memos are not allowed';



                       log_errors (

                                   --   pin_transaction_id           =>  pin_trx_id

                                   piv_source_keyname1       => 'R12 Transaction type',

                                   piv_source_keyvalue1      => l_trx_type,

                                   piv_error_type               => 'ERR_VAL',

                                   piv_source_column_name          => 'LEGACY_CUSTOMER_TRX_ID',

                                   piv_source_column_value         => pin_trx_id,

                                   piv_error_code               => l_err_code,

                                   piv_error_message            => l_err_msg,

                                   pov_return_status            => l_log_ret_status,

                                   pov_error_msg                => l_log_err_msg

                                  );

            */
          ELSE

            IF l_trx_type_name IN ('CM') THEN
              --l_cm_term_error := 'N';  -- commented for v1.58
              FOR r_cm_err_rec IN (SELECT interface_txn_id, leg_term_name
                                     FROM xxar_invoices_stg xis
                                    WHERE leg_cust_trx_type_name =
                                          trx_type_rec.leg_cust_trx_type_name
                                      AND org_id = trx_type_rec.org_id
                                      AND leg_term_name IS NOT NULL
                                      AND batch_id = g_new_batch_id
                                      AND run_sequence_id = g_new_run_seq_id) LOOP
                --l_cm_term_error := 'Y';   -- commented for v1.58
                l_err_code      := 'ETN_AR_PMT_TERM_ERROR';
                g_retcode       := 1;
                l_err_msg       := 'Error : Payment term is NOT NULL for Credit memo transaction';
                log_errors(pin_transaction_id      => r_cm_err_rec.interface_txn_id,
                           piv_source_column_name  => 'Legacy Payment term',
                           piv_source_column_value => r_cm_err_rec.leg_term_name,
                           piv_error_type          => 'ERR_VAL',
                           piv_error_code          => l_err_code,
                           piv_error_message       => l_err_msg,
                           pov_return_status       => l_log_ret_status,
                           pov_error_msg           => l_log_err_msg);
              END LOOP;
              UPDATE xxar_invoices_stg
                 SET process_flag      = 'E',
                     ERROR_TYPE        = 'ERR_VAL',
                     last_update_date  = g_sysdate,
                     last_updated_by   = g_last_updated_by,
                     last_update_login = g_login_id
               WHERE leg_cust_trx_type_name =
                     trx_type_rec.leg_cust_trx_type_name
                 AND org_id = trx_type_rec.org_id
                 AND leg_term_name IS NOT NULL
                 AND batch_id = g_new_batch_id
                 AND run_sequence_id = g_new_run_seq_id;
            END IF;

            --IF NVL(l_cm_term_error, 'N') = 'N' THEN    -- commented for v1.58
              UPDATE xxar_invoices_stg
                 SET transaction_type_id = l_trx_type_id,
                     trx_type            = l_trx_type_name,
                     cust_trx_type_name  = l_trx_type
               WHERE leg_cust_trx_type_name =
                     trx_type_rec.leg_cust_trx_type_name
                 AND org_id = trx_type_rec.org_id
                 AND batch_id = g_new_batch_id
                 AND run_sequence_id = g_new_run_seq_id;
              COMMIT;
            --END IF;    -- commented for v1.58

          END IF;
        END IF;
      EXCEPTION
        WHEN NO_DATA_FOUND THEN
          FOR r_r12_trx_type_err_rec IN (SELECT interface_txn_id
                                           FROM xxar_invoices_stg xis
                                          WHERE leg_cust_trx_type_name =
                                                trx_type_rec.leg_cust_trx_type_name
                                            AND org_id = trx_type_rec.org_id
                                            AND batch_id = g_new_batch_id
                                            AND run_sequence_id =
                                                g_new_run_seq_id) LOOP
            l_err_code := 'ETN_AR_TRX_TYPE_ERROR';
            l_err_msg  := 'Error : Transaction type not setup in R12 for organization id ' ||
                          trx_type_rec.org_id;
            g_retcode  := 1;
            log_errors(pin_transaction_id      => r_r12_trx_type_err_rec.interface_txn_id,
                       piv_source_column_name  => 'R12 Transaction type',
                       piv_source_column_value => l_trx_type,
                       piv_error_type          => 'ERR_VAL',
                       piv_error_code          => l_err_code,
                       piv_error_message       => l_err_msg,
                       pov_return_status       => l_log_ret_status,
                       pov_error_msg           => l_log_err_msg);
          END LOOP;
          UPDATE xxar_invoices_stg
             SET process_flag      = 'E',
                 ERROR_TYPE        = 'ERR_VAL',
                 last_update_date  = g_sysdate,
                 last_updated_by   = g_last_updated_by,
                 last_update_login = g_login_id
           WHERE leg_cust_trx_type_name =
                 trx_type_rec.leg_cust_trx_type_name
             AND org_id = trx_type_rec.org_id
             AND batch_id = g_new_batch_id
             AND run_sequence_id = g_new_run_seq_id;
        WHEN OTHERS THEN
          l_err_code := 'ETN_AR_PROCEDURE_EXCEPTION';
          l_err_msg  := 'Error : Error fetching/ updating R12 Transaction type' ||
                        SUBSTR(SQLERRM, 1, 150);
          g_retcode  := 2;
          FOR r_r12_trx_type_err1_rec IN (SELECT interface_txn_id
                                            FROM xxar_invoices_stg xis
                                           WHERE leg_cust_trx_type_name =
                                                 trx_type_rec.leg_cust_trx_type_name
                                             AND org_id =
                                                 trx_type_rec.org_id
                                             AND batch_id = g_new_batch_id
                                             AND run_sequence_id =
                                                 g_new_run_seq_id) LOOP
            log_errors(pin_transaction_id      => r_r12_trx_type_err1_rec.interface_txn_id,
                       piv_source_column_name  => 'R12 Transaction type',
                       piv_source_column_value => l_trx_type,
                       piv_error_type          => 'ERR_VAL',
                       piv_error_code          => l_err_code,
                       piv_error_message       => l_err_msg,
                       pov_return_status       => l_log_ret_status,
                       pov_error_msg           => l_log_err_msg);
          END LOOP;
          UPDATE xxar_invoices_stg
             SET process_flag      = 'E',
                 ERROR_TYPE        = 'ERR_VAL',
                 last_update_date  = g_sysdate,
                 last_updated_by   = g_last_updated_by,
                 last_update_login = g_login_id
           WHERE leg_cust_trx_type_name =
                 trx_type_rec.leg_cust_trx_type_name
             AND org_id = trx_type_rec.org_id
             AND batch_id = g_new_batch_id
             AND run_sequence_id = g_new_run_seq_id;
      END;
    END LOOP;
    COMMIT;
    FOR term_rec IN term_cur LOOP
      l_term_name := NULL;
      l_term_id   := NULL;
      BEGIN
        print_log_message('Validating legacy payment term ' ||
                          term_rec.leg_term_name);
        SELECT TRIM(flv.description)
          INTO l_term_name
          FROM fnd_lookup_values flv
         WHERE TRIM(UPPER(flv.meaning)) =
               TRIM(UPPER(term_rec.leg_term_name))
           AND flv.LANGUAGE = USERENV('LANG')
           AND flv.enabled_flag = 'Y'
              -- AND UPPER(flv.lookup_type) = g_pmt_term_lookup        -- Removed UPPER so that index can be used. Table already has value in ALL CAPS
           AND flv.lookup_type = g_pmt_term_lookup
           AND TRUNC(g_sysdate) BETWEEN
               TRUNC(NVL(flv.start_date_active, g_sysdate)) AND
               TRUNC(NVL(flv.end_date_active, g_sysdate));
      EXCEPTION
        WHEN NO_DATA_FOUND THEN
          g_retcode := 1;
          FOR r_term_ref_err_rec IN (SELECT interface_txn_id
                                       FROM xxar_invoices_stg xis
                                      WHERE leg_term_name =
                                            term_rec.leg_term_name
                                        AND batch_id = g_new_batch_id
                                        AND run_sequence_id =
                                            g_new_run_seq_id) LOOP
            l_err_code := 'ETN_AR_PMT_TERM_ERROR';
            l_err_msg  := 'Error : Cross reference not defined for payment term';
            log_errors(pin_transaction_id      => r_term_ref_err_rec.interface_txn_id,
                       piv_source_column_name  => 'Legacy Payment Term',
                       piv_source_column_value => term_rec.leg_term_name,
                       piv_error_type          => 'ERR_VAL',
                       piv_error_code          => l_err_code,
                       piv_error_message       => l_err_msg,
                       pov_return_status       => l_log_ret_status,
                       pov_error_msg           => l_log_err_msg);
          END LOOP;
          UPDATE xxar_invoices_stg
             SET process_flag      = 'E',
                 ERROR_TYPE        = 'ERR_VAL',
                 last_update_date  = g_sysdate,
                 last_updated_by   = g_last_updated_by,
                 last_update_login = g_login_id
           WHERE leg_term_name = term_rec.leg_term_name
             AND batch_id = g_new_batch_id
             AND run_sequence_id = g_new_run_seq_id;
          COMMIT;
        WHEN OTHERS THEN
          l_err_code := 'ETN_AR_PROCEDURE_EXCEPTION';
          g_retcode  := 2;
          l_err_msg  := 'Error : Error updating staging table for payment term' ||
                        SUBSTR(SQLERRM, 1, 150);
          log_errors(
                     --   pin_transaction_id           =>  pin_trx_id
                     piv_source_column_name  => 'Legacy Payment Term',
                     piv_source_column_value => term_rec.leg_term_name,
                     piv_error_type          => 'ERR_VAL',
                     piv_error_code          => l_err_code,
                     piv_error_message       => l_err_msg,
                     pov_return_status       => l_log_ret_status,
                     pov_error_msg           => l_log_err_msg);
          UPDATE xxar_invoices_stg
             SET process_flag      = 'E',
                 ERROR_TYPE        = 'ERR_VAL',
                 last_update_date  = g_sysdate,
                 last_updated_by   = g_last_updated_by,
                 last_update_login = g_login_id
           WHERE leg_term_name = term_rec.leg_term_name
             AND batch_id = g_new_batch_id
             AND run_sequence_id = g_new_run_seq_id;
      END;
      BEGIN
        IF l_term_name IS NOT NULL THEN
          print_log_message('Validating R12 term name ' || l_term_name);
          SELECT rtm.term_id
            INTO l_term_id
            FROM ra_terms_tl rtm
           WHERE UPPER(rtm.NAME) = UPPER(l_term_name)
             AND rtm.LANGUAGE = USERENV('LANG');
        END IF;
        UPDATE xxar_invoices_stg
           SET term_id = l_term_id, term_name = l_term_name
         WHERE leg_term_name = term_rec.leg_term_name
           AND batch_id = g_new_batch_id
           AND run_sequence_id = g_new_run_seq_id;
        COMMIT;
      EXCEPTION
        WHEN NO_DATA_FOUND THEN
          g_retcode := 1;
          FOR r_r12_term_err_rec IN (SELECT interface_txn_id
                                       FROM xxar_invoices_stg xis
                                      WHERE leg_term_name =
                                            term_rec.leg_term_name
                                        AND batch_id = g_new_batch_id
                                        AND run_sequence_id =
                                            g_new_run_seq_id) LOOP
            l_err_code := 'ETN_AR_PMT_TERM_ERROR';
            l_err_msg  := 'Error : Payment term not setup in R12';
            log_errors(pin_transaction_id      => r_r12_term_err_rec.interface_txn_id,
                       piv_source_column_name  => 'R12 payment term',
                       piv_source_column_value => l_term_name,
                       piv_error_type          => 'ERR_VAL',
                       piv_error_code          => l_err_code,
                       piv_error_message       => l_err_msg,
                       pov_return_status       => l_log_ret_status,
                       pov_error_msg           => l_log_err_msg);
          END LOOP;
          UPDATE xxar_invoices_stg
             SET process_flag      = 'E',
                 ERROR_TYPE        = 'ERR_VAL',
                 last_update_date  = g_sysdate,
                 last_updated_by   = g_last_updated_by,
                 last_update_login = g_login_id
           WHERE leg_term_name = term_rec.leg_term_name
             AND batch_id = g_new_batch_id
             AND run_sequence_id = g_new_run_seq_id;
        WHEN OTHERS THEN
          l_err_code := 'ETN_AR_PROCEDURE_EXCEPTION';
          g_retcode  := 2;
          l_err_msg  := 'Error : Error fetching R12 Payment term' ||
                        SUBSTR(SQLERRM, 1, 150);
          FOR r_r12_term_err1_rec IN (SELECT interface_txn_id
                                        FROM xxar_invoices_stg xis
                                       WHERE leg_term_name =
                                             term_rec.leg_term_name
                                         AND batch_id = g_new_batch_id
                                         AND run_sequence_id =
                                             g_new_run_seq_id) LOOP
            log_errors(pin_transaction_id      => r_r12_term_err1_rec.interface_txn_id,
                       piv_source_column_name  => 'R12 payment term',
                       piv_source_column_value => l_term_name,
                       piv_error_type          => 'ERR_VAL',
                       piv_error_code          => l_err_code,
                       piv_error_message       => l_err_msg,
                       pov_return_status       => l_log_ret_status,
                       pov_error_msg           => l_log_err_msg);
          END LOOP;
          UPDATE xxar_invoices_stg
             SET process_flag      = 'E',
                 ERROR_TYPE        = 'ERR_VAL',
                 last_update_date  = g_sysdate,
                 last_updated_by   = g_last_updated_by,
                 last_update_login = g_login_id
           WHERE leg_term_name = term_rec.leg_term_name
             AND batch_id = g_new_batch_id
             AND run_sequence_id = g_new_run_seq_id;
      END;
    END LOOP;
    COMMIT;

    -- Commented below section for v1.48, removal of Service Contracts validation, Defect# 9239

    /**   FOR gl_date_rec IN gl_date_cur
          LOOP
             BEGIN
                print_log_message('Validating gl date for Service contracts ' ||
                                  gl_date_rec.leg_gl_date);
                SELECT 1
                  INTO l_gl_status
                  FROM gl_periods         glp
                      ,gl_period_statuses gps
                 WHERE UPPER(glp.period_name) = UPPER(gps.period_name)
                   AND glp.period_set_name = g_period_set_name --'ETN Corp Calend'
                   AND gl_date_rec.leg_gl_date BETWEEN glp.start_date AND
                       glp.end_date
                   AND gps.application_id =
                       (SELECT fap.application_id
                          FROM fnd_application_vl fap
                         WHERE fap.application_short_name = 'SQLGL')
                   AND gps.closing_status = 'O'
                   AND gps.adjustment_period_flag = 'N' --v1.12  Added to discard Adjustment periods for Defect 1895
                   AND ledger_id = gl_date_rec.ledger_id;
             EXCEPTION
                WHEN NO_DATA_FOUND THEN
                   l_gl_error := 'Y';
                   l_err_code := 'ETN_AR_GL_PERIOD_ERROR';
                   g_retcode  := 1;
                   l_err_msg  := 'GL Period is not open/defined for SERVICE CONTRACTS for GL date ' ||
                                 gl_date_rec.leg_gl_date;
                   FOR r_gl_oks_err_rec IN (SELECT interface_txn_id
                                              FROM xxar_invoices_stg xis
                                             WHERE leg_gl_date =
                                                   gl_date_rec.leg_gl_date
                                               AND batch_id = g_new_batch_id
                                               AND run_sequence_id =
                                                   g_new_run_seq_id)
                   LOOP
                      log_errors(pin_transaction_id => r_gl_oks_err_rec.interface_txn_id,
                                 piv_source_column_name => 'GL Period date',
                                 piv_source_column_value => gl_date_rec.leg_gl_date,
                                 piv_error_type => 'ERR_VAL',
                                 piv_error_code => l_err_code,
                                 piv_error_message => l_err_msg,
                                 pov_return_status => l_log_ret_status,
                                 pov_error_msg => l_log_err_msg);
                   END LOOP;
                   UPDATE xxar_invoices_stg
                      SET process_flag      = 'E'
                         ,ERROR_TYPE        = 'ERR_VAL'
                         ,last_update_date  = g_sysdate
                         ,last_updated_by   = g_last_updated_by
                         ,last_update_login = g_login_id
                    WHERE leg_gl_date = gl_date_rec.leg_gl_date
                      AND batch_id = g_new_batch_id
                      AND run_sequence_id = g_new_run_seq_id;
                WHEN OTHERS THEN
                   l_gl_error := 'Y';
                   g_retcode  := 2;
                   l_err_code := 'ETN_AR_PROCEDURE_EXCEPTION';
                   l_err_msg  := 'Error : Error validating gl period for SERVICE CONTRACTS ' ||
                                 gl_date_rec.leg_gl_date ||
                                 SUBSTR(SQLERRM, 1, 150);
                   FOR r_gl_oks_err1_rec IN (SELECT interface_txn_id
                                               FROM xxar_invoices_stg xis
                                              WHERE leg_gl_date =
                                                    gl_date_rec.leg_gl_date
                                                AND batch_id = g_new_batch_id
                                                AND run_sequence_id =
                                                    g_new_run_seq_id)
                   LOOP
                      log_errors(pin_transaction_id => r_gl_oks_err1_rec.interface_txn_id,
                                 piv_source_column_name => 'GL Period date',
                                 piv_source_column_value => gl_date_rec.leg_gl_date,
                                 piv_error_type => 'ERR_VAL',
                                 piv_error_code => l_err_code,
                                 piv_error_message => l_err_msg,
                                 pov_return_status => l_log_ret_status,
                                 pov_error_msg => l_log_err_msg);
                   END LOOP;
                   UPDATE xxar_invoices_stg
                      SET process_flag      = 'E'
                         ,ERROR_TYPE        = 'ERR_VAL'
                         ,last_update_date  = g_sysdate
                         ,last_updated_by   = g_last_updated_by
                         ,last_update_login = g_login_id
                    WHERE leg_gl_date = gl_date_rec.leg_gl_date
                      AND batch_id = g_new_batch_id
                      AND run_sequence_id = g_new_run_seq_id;
             END;
          END LOOP;

          BEGIN
             SELECT rul.rule_id
               INTO l_inv_rule_id
               FROM ra_rules rul
              WHERE upper(rul.name) = 'ADVANCE INVOICE'
                AND rul.status = 'A'
                AND rul.type = 'I';
          EXCEPTION
             WHEN OTHERS THEN
                l_inv_rule_err     := 1;
                g_retcode          := 2;
                l_inv_rule_err_msg := 'Error while deriving invoicing rule ' ||
                                      SUBSTR(SQLERRM, 1, 150);
          END;

          FOR accounting_rec IN accounting_cur
          LOOP
             IF accounting_rec.leg_agreement_name IS NOT NULL
             THEN
                l_acc_rule_id     := NULL;
                l_acc_rule_exists := 1;
                BEGIN
                   --check if the currency code exists in the system
                   SELECT rul.rule_id
                     INTO l_acc_rule_id
                     FROM ra_rules rul
                    WHERE rul.name = accounting_rec.leg_agreement_name
                      AND rul.status = 'A'
                      AND rul.type = 'A';
                   UPDATE xxar_invoices_stg
                      SET agreement_id      = l_acc_rule_id
                         ,invoicing_rule_id = l_inv_rule_id
                    WHERE leg_agreement_name =
                          accounting_rec.leg_agreement_name
                      AND batch_id = g_new_batch_id
                      AND run_sequence_id = g_new_run_seq_id;
                EXCEPTION
                   WHEN NO_DATA_FOUND THEN
                      l_valerr_cnt := 2;
                      g_retcode    := 1;
                      print_log_message('Accounting rule not found');
                      l_err_code := 'ETN_AP_INVALID_ACC_RULE';
                      l_err_msg  := 'Accounting rule is not Valid';
                   WHEN OTHERS THEN
                      l_valerr_cnt := 2;
                      g_retcode    := 2;
                      print_log_message('In When others of accounting rule check' ||
                                        SQLERRM);
                      l_err_code := 'ETN_AP_INVALID_ACC_RULE';
                      l_err_msg  := 'Error while deriving accounting rule ' ||
                                    SUBSTR(SQLERRM, 1, 150);
                END;
                IF l_valerr_cnt = 2
                THEN
                   UPDATE xxar_invoices_stg
                      SET process_flag      = 'E'
                         ,error_type        = 'ERR_VAL'
                         ,last_update_date  = g_sysdate
                         ,last_updated_by   = g_last_updated_by
                         ,last_update_login = g_login_id
                    WHERE leg_agreement_name =
                          accounting_rec.leg_agreement_name
                      AND batch_id = g_new_batch_id
                      AND run_sequence_id = g_new_run_seq_id;
                   FOR r_acc_err_rec IN (SELECT interface_txn_id
                                           FROM xxar_invoices_stg xis
                                          WHERE leg_agreement_name =
                                                accounting_rec.leg_agreement_name
                                            AND batch_id = g_new_batch_id
                                            AND run_sequence_id =
                                                g_new_run_seq_id)
                   LOOP
                      log_errors(pin_transaction_id => r_acc_err_rec.interface_txn_id,
                                 piv_source_column_name => 'Accounting rule',
                                 piv_source_column_value => accounting_rec.leg_agreement_name,
                                 piv_error_type => 'ERR_VAL',
                                 piv_error_code => l_err_code,
                                 piv_error_message => l_err_msg,
                                 pov_return_status => l_log_ret_status,
                                 pov_error_msg => l_log_err_msg);
                   END LOOP;
                END IF;
             END IF;
          END LOOP;
          COMMIT;

          IF (l_acc_rule_exists = 1 AND l_inv_rule_err = 1)
          THEN
             print_log_message(l_inv_rule_err_msg);
             l_err_code := 'ETN_AP_INVALID_INV_RULE';
             l_err_msg  := l_inv_rule_err_msg;
             g_retcode  := 1;
             log_errors(piv_source_column_name => 'Invoicing rule',
                        piv_source_column_value => 'Advance Invoice',
                        piv_error_type => 'ERR_VAL',
                        piv_error_code => l_err_code,
                        piv_error_message => l_err_msg,
                        pov_return_status => l_log_ret_status,
                        pov_error_msg => l_log_err_msg);
          END IF;
          --ver 1.7 changes end
    **/

    -- Commented above section for v1.48, removal of Service Contracts validation, Defect# 9239

    FOR tax_rec IN tax_cur LOOP
      l_tax_code_r12          := NULL;
      l_tax_r12               := NULL;
      l_tax_regime_code       := NULL;
      l_tax_rate_code         := NULL;
      l_tax                   := NULL;
      l_tax_status_code       := NULL;
      l_tax_jurisdiction_code := NULL;
      --ver1.17 changes start
      -- IF tax_rec.leg_tax_code IS NOT NULL
      IF (tax_rec.leg_tax_code IS NOT NULL AND tax_rec.org_id IS NOT NULL)
      --ver1.17 changes end
       THEN
        BEGIN
          print_log_message('Validating legacy tax code ' ||
                            tax_rec.leg_tax_code);
          SELECT flv.description
            INTO l_tax_code_r12
            FROM apps.fnd_lookup_values flv
           WHERE TRIM(UPPER(flv.meaning)) =
                 TRIM(UPPER(tax_rec.leg_tax_code))
             AND flv.enabled_flag = 'Y'
             AND flv.lookup_type = g_tax_code_lookup
             AND TRUNC(g_sysdate) BETWEEN
                 TRUNC(NVL(flv.start_date_active, g_sysdate)) AND
                 TRUNC(NVL(flv.end_date_active, g_sysdate))
             AND flv.LANGUAGE = USERENV('LANG');
          --C9988598
          print_log_message('Tax Code:' || l_tax_code_r12);
        EXCEPTION
          WHEN NO_DATA_FOUND THEN
            FOR r_tax_ref_err_rec IN (SELECT interface_txn_id
                                        FROM xxar_invoices_stg xis
                                       WHERE leg_tax_code =
                                             tax_rec.leg_tax_code
                                            --ver1.17 changes start
                                         AND org_id = tax_rec.org_id
                                            --ver1.17 changes end
                                            --ver1.22 changes start
                                         AND leg_line_type = 'TAX'
                                            --ver1.22 changes end
                                         AND batch_id = g_new_batch_id
                                         AND run_sequence_id =
                                             g_new_run_seq_id) LOOP
              l_err_code := 'ETN_AR_TAX_ERROR';
              g_retcode  := 1;
              l_err_msg  := 'Error : Cross reference not defined for tax code';
              log_errors(pin_transaction_id      => r_tax_ref_err_rec.interface_txn_id,
                         piv_source_column_name  => 'Legacy Tax code',
                         piv_source_column_value => tax_rec.leg_tax_code,
                         piv_error_type          => 'ERR_VAL',
                         piv_error_code          => l_err_code,
                         piv_error_message       => l_err_msg,
                         pov_return_status       => l_log_ret_status,
                         pov_error_msg           => l_log_err_msg);
            END LOOP;
            UPDATE xxar_invoices_stg
               SET process_flag      = 'E',
                   ERROR_TYPE        = 'ERR_VAL',
                   last_update_date  = g_sysdate,
                   last_updated_by   = g_last_updated_by,
                   last_update_login = g_login_id
             WHERE leg_tax_code = tax_rec.leg_tax_code
                  --ver1.17 changes start
               AND org_id = tax_rec.org_id
                  --ver1.17 changes end
                  --ver1.22 changes start
               AND leg_line_type = 'TAX'
                  --ver1.22 changes end
               AND batch_id = g_new_batch_id
               AND run_sequence_id = g_new_run_seq_id;
            COMMIT;
          WHEN OTHERS THEN
            l_err_code := 'ETN_AR_PROCEDURE_EXCEPTION';
            g_retcode  := 2;
            l_err_msg  := 'Error : Error validating tax ' ||
                          SUBSTR(SQLERRM, 1, 150);
            FOR r_tax_ref_err1_rec IN (SELECT interface_txn_id
                                         FROM xxar_invoices_stg xis
                                        WHERE leg_tax_code =
                                              tax_rec.leg_tax_code
                                             --ver1.17 changes start
                                          AND org_id = tax_rec.org_id
                                             --ver1.17 changes end
                                             --ver1.22 changes start
                                          AND leg_line_type = 'TAX'
                                             --ver1.22 changes end
                                          AND batch_id = g_new_batch_id
                                          AND run_sequence_id =
                                              g_new_run_seq_id) LOOP
              log_errors(pin_transaction_id      => r_tax_ref_err1_rec.interface_txn_id,
                         piv_source_column_name  => 'Legacy Tax code',
                         piv_source_column_value => tax_rec.leg_tax_code,
                         piv_error_type          => 'ERR_VAL',
                         piv_error_code          => l_err_code,
                         piv_error_message       => l_err_msg,
                         pov_return_status       => l_log_ret_status,
                         pov_error_msg           => l_log_err_msg);
            END LOOP;
            UPDATE xxar_invoices_stg
               SET process_flag      = 'E',
                   ERROR_TYPE        = 'ERR_VAL',
                   last_update_date  = g_sysdate,
                   last_updated_by   = g_last_updated_by,
                   last_update_login = g_login_id
             WHERE leg_tax_code = tax_rec.leg_tax_code
                  --ver1.17 changes start
               AND org_id = tax_rec.org_id
                  --ver1.17 changes end
                  --ver1.22 changes start
               AND leg_line_type = 'TAX'
                  --ver1.22 changes end
               AND batch_id = g_new_batch_id
               AND run_sequence_id = g_new_run_seq_id;
        END;
      END IF;
      BEGIN
        -- SS
        l_org_id  := NULL;
        l_ou_name := NULL;
        IF (l_tax_code_r12 IS NOT NULL AND tax_rec.org_id IS NOT NULL) THEN
          print_log_message('Validating R12 tax code ' || l_tax_code_r12);
          --SS
          /*

          get_org(  p_leg_source_system => tax_rec.leg_source_system,

                       p_leg_customer_trx_id => tax_rec.leg_customer_trx_id,

                       p_org_id => l_org_id,

                        p_ou_name => l_ou_name);

           */
          SELECT DISTINCT zrb.tax,
                          zrb.tax_regime_code,
                          zrb.tax_rate_code,
                          zrb.tax,
                          zrb.tax_status_code,
                          zrb.tax_jurisdiction_code
            INTO l_tax_r12,
                 l_tax_regime_code,
                 l_tax_rate_code,
                 l_tax,
                 l_tax_status_code,
                 l_tax_jurisdiction_code
            FROM zx_accounts            za,
                 hr_operating_units     hrou,
                 gl_ledgers             gl,
                 fnd_id_flex_structures fifs,
                 zx_rates_b             zrb
          --,zx_regimes_b           zb                --v1.12  Commented as zb is not being used anywhere
           WHERE za.internal_organization_id = hrou.organization_id
             AND gl.ledger_id = za.ledger_id
             AND fifs.application_id =
                 (SELECT fap.application_id
                    FROM fnd_application_vl fap
                   WHERE fap.application_short_name = 'SQLGL')
             AND fifs.id_flex_code = 'GL#'
             AND fifs.id_flex_num = gl.chart_of_accounts_id
             AND zrb.tax_rate_id = za.tax_account_entity_id
             AND za.tax_account_entity_code = 'RATES'
             AND zrb.tax_rate_code = l_tax_code_r12 -- SS
             AND hrou.organization_id = tax_rec.org_id
                /*AND TRUNC(SYSDATE) BETWEEN                    --v1.12  Modified as this was returning multiple rows

                TRUNC(NVL(zb.effective_from, SYSDATE)) AND

                TRUNC(NVL(zb.effective_to, SYSDATE));*/
             AND TRUNC(g_sysdate) BETWEEN
                 TRUNC(NVL(zrb.effective_from, g_sysdate)) AND
                 TRUNC(NVL(zrb.effective_to, g_sysdate))
                --ver1.16 changes start
             AND NVL(zrb.active_flag, 'N') = 'Y';
          --ver1.16 changes end
        END IF;
        UPDATE xxar_invoices_stg
           SET tax_code              = l_tax_r12,
               tax_regime_code       = l_tax_regime_code,
               tax_rate_code         = l_tax_rate_code,
               tax                   = l_tax,
               tax_status_code       = l_tax_status_code,
               tax_jurisdiction_code = l_tax_jurisdiction_code
         WHERE leg_tax_code = tax_rec.leg_tax_code
              --ver1.17 changes start
           AND org_id = tax_rec.org_id
              --ver1.17 changes end
              --ver1.22 changes start
           AND leg_line_type = 'TAX'
              --ver1.22 changes end
           AND batch_id = g_new_batch_id
           AND run_sequence_id = g_new_run_seq_id;
        COMMIT;
      EXCEPTION
        WHEN NO_DATA_FOUND THEN
          FOR r_r12_tax_err_rec IN (SELECT interface_txn_id
                                      FROM xxar_invoices_stg xis
                                     WHERE leg_tax_code =
                                           tax_rec.leg_tax_code
                                          --ver1.17 changes start
                                       AND org_id = tax_rec.org_id
                                          --ver1.17 changes end
                                          --ver1.22 changes start
                                       AND leg_line_type = 'TAX'
                                          --ver1.22 changes end
                                       AND batch_id = g_new_batch_id
                                       AND run_sequence_id =
                                           g_new_run_seq_id) LOOP
            l_err_code := 'ETN_AR_TAX_ERROR';
            l_err_msg  := 'Error : R12 set up not done for tax code';
            g_retcode  := 1;
            log_errors(pin_transaction_id      => r_r12_tax_err_rec.interface_txn_id,
                       piv_source_column_name  => 'R12 tax code',
                       piv_source_column_value => l_tax_code_r12,
                       piv_error_type          => 'ERR_VAL',
                       piv_error_code          => l_err_code,
                       piv_error_message       => l_err_msg,
                       pov_return_status       => l_log_ret_status,
                       pov_error_msg           => l_log_err_msg);
          END LOOP;
          UPDATE xxar_invoices_stg
             SET process_flag      = 'E',
                 ERROR_TYPE        = 'ERR_VAL',
                 last_update_date  = g_sysdate,
                 last_updated_by   = g_last_updated_by,
                 last_update_login = g_login_id
           WHERE leg_tax_code = tax_rec.leg_tax_code
                --ver1.17 changes start
             AND org_id = tax_rec.org_id
                --ver1.17 changes end
                --ver1.22 changes start
             AND leg_line_type = 'TAX'
                --ver1.22 changes end
             AND batch_id = g_new_batch_id
             AND run_sequence_id = g_new_run_seq_id;
        WHEN OTHERS THEN
          l_err_code := 'ETN_AR_PROCEDURE_EXCEPTION';
          l_err_msg  := 'Error : Error validating tax ' ||
                        SUBSTR(SQLERRM, 1, 150);
          g_retcode  := 2;
          FOR r_r12_tax_err1_rec IN (SELECT interface_txn_id
                                       FROM xxar_invoices_stg xis
                                      WHERE leg_tax_code =
                                            tax_rec.leg_tax_code
                                           --ver1.17 changes start
                                        AND org_id = tax_rec.org_id
                                           --ver1.17 changes end
                                           --ver1.22 changes start
                                        AND leg_line_type = 'TAX'
                                           --ver1.22 changes end
                                        AND batch_id = g_new_batch_id
                                        AND run_sequence_id =
                                            g_new_run_seq_id) LOOP
            log_errors(pin_transaction_id      => r_r12_tax_err1_rec.interface_txn_id,
                       piv_source_column_name  => 'R12 tax code',
                       piv_source_column_value => l_tax_code_r12,
                       piv_error_type          => 'ERR_VAL',
                       piv_error_code          => l_err_code,
                       piv_error_message       => l_err_msg,
                       pov_return_status       => l_log_ret_status,
                       pov_error_msg           => l_log_err_msg);
          END LOOP;
          UPDATE xxar_invoices_stg
             SET process_flag      = 'E',
                 ERROR_TYPE        = 'ERR_VAL',
                 last_update_date  = g_sysdate,
                 last_updated_by   = g_last_updated_by,
                 last_update_login = g_login_id
           WHERE leg_tax_code = tax_rec.leg_tax_code
                --ver1.17 changes start
             AND org_id = tax_rec.org_id
                --ver1.17 changes end
                --ver1.22 changes start
             AND leg_line_type = 'TAX'
                --ver1.22 changes end
             AND batch_id = g_new_batch_id
             AND run_sequence_id = g_new_run_seq_id;
      END;
    END LOOP;
    COMMIT;

    UPDATE xxar_invoices_stg
       SET trx_number   = leg_trx_number,
           process_flag = DECODE(process_flag, 'E', 'E', 'N')
           -- ,gl_date      = DECODE(INSTR(leg_cust_trx_type_name, 'OKS'), 0,   -- commented for v1.48, Service Contracts,Defect# 9239
           --                      g_gl_date, leg_gl_date)
          ,
           gl_date = g_gl_date -- modified for v1.48, Service Contracts,Defect# 9239
          ,
           --Ver1.5 Changes for DFF rationalization starts
           /*             header_attribute_category = leg_header_attribute_category,

                 header_attribute1 = leg_header_attribute1,
           header_attribute2 = leg_header_attribute2,
                 header_attribute3 = leg_header_attribute3,
                 header_attribute4 = leg_header_attribute4,
                 header_attribute5 = leg_header_attribute5,
                 header_attribute6 = leg_header_attribute6,
                 header_attribute7 = leg_header_attribute7,
                 header_attribute8 = leg_header_attribute8,
                 header_attribute9 = leg_header_attribute9,
                 header_attribute10 = leg_header_attribute10,
                 header_attribute11 = leg_header_attribute11,
                 header_attribute12 = leg_header_attribute12,
                 header_attribute13 = leg_header_attribute13,
                 header_attribute14 = leg_header_attribute14,
                 header_attribute15 = leg_header_attribute15,
           */
           --Ver1.5 Changes for DFF rationalization end
           trx_date = leg_trx_date
           --,batch_source_name = 'CONVERSION'
          ,
           purchase_order    = leg_purchase_order,
           last_update_date  = g_sysdate,
           last_updated_by   = g_last_updated_by,
           last_update_login = g_login_id
     WHERE batch_id = g_new_batch_id
       AND run_sequence_id = g_new_run_seq_id;
    COMMIT;
    ------------------------------------------------------------------   --perf
    -- Open cursor for invoice header level validations
    /*      OPEN val_inv_cur;
          LOOP
             FETCH val_inv_cur BULK COLLECT INTO val_inv_rec LIMIT l_limit;
             EXIT WHEN val_inv_rec.COUNT = 0;
              FOR l_inv_cnt IN 1 .. val_inv_rec.COUNT
              LOOP
             l_validate_flag := 'S';
             l_valid_flag := 'Y';
             l_ledger_id := NULL;
             l_gl_date := NULL;
             print_log_message (CHR (10));
             print_log_message ('For legacy transaction number: ' || val_inv_rec(l_inv_cnt).leg_trx_number);
             print_log_message ('------------------------------------------------------------------');
             -- Verify Customer number is NOT NULL
             IF check_mandatory (pin_cust_trx_id            => val_inv_rec(l_inv_cnt).leg_customer_trx_id,
                         piv_column_value      => val_inv_rec(l_inv_cnt).leg_customer_number,
                         piv_column_name       => 'Customer Number'
                        )
             THEN
                l_valid_flag := 'E';
             END IF;
             -- Verify Bill to address is NOT NULL
             IF check_mandatory (pin_cust_trx_id            => val_inv_rec(l_inv_cnt).leg_customer_trx_id,
                         piv_column_value      => val_inv_rec(l_inv_cnt).leg_bill_to_address,
                         piv_column_name       => 'Bill to address'
                        )
             THEN
                l_valid_flag := 'E';
             END IF;
             -- Verify transaction number is NOT NULL
             IF check_mandatory (pin_cust_trx_id            => val_inv_rec(l_inv_cnt).leg_customer_trx_id,
                         piv_column_value      => val_inv_rec(l_inv_cnt).leg_trx_number,
                         piv_column_name       => 'Transaction number'
                        )
             THEN
                l_valid_flag := 'E';
             END IF;
             -- Verify transaction date is NOT NULL
             IF check_mandatory (pin_cust_trx_id            => val_inv_rec(l_inv_cnt).leg_customer_trx_id,
                         piv_column_value      => val_inv_rec(l_inv_cnt).leg_trx_date,
                         piv_column_name       => 'Transaction date'
                        )
             THEN
                l_valid_flag := 'E';
             END IF;
             -- Verify currency is NOT NULL
             IF check_mandatory (pin_cust_trx_id            => val_inv_rec(l_inv_cnt).leg_customer_trx_id,
                         piv_column_value      => val_inv_rec(l_inv_cnt).leg_currency_code,
                         piv_column_name       => 'Currency code'
                        )
             THEN
                l_valid_flag := 'E';
             END IF;
             -- Verify Transaction type is NOT NULL
             IF check_mandatory (pin_cust_trx_id            => val_inv_rec(l_inv_cnt).leg_customer_trx_id,
                         piv_column_value      => val_inv_rec(l_inv_cnt).leg_cust_trx_type_name,
                         piv_column_name       => 'Transaction type'
                        )
             THEN
                l_valid_flag := 'E';
             END IF;
             -- Verify Operating Unit is NOT NULL
             IF check_mandatory (pin_cust_trx_id            => val_inv_rec(l_inv_cnt).leg_customer_trx_id,
                         piv_column_value      => val_inv_rec(l_inv_cnt).leg_operating_unit,
                         piv_column_name       => 'Operating Unit'
                        )
             THEN
                l_valid_flag := 'E';
             END IF;
    /*         validate_operating_unit (val_inv_rec(l_inv_cnt).leg_customer_trx_id, val_inv_rec(l_inv_cnt).leg_operating_unit, l_ledger_id, l_validate_flag); --perf
             IF l_validate_flag = 'E'
             THEN
                l_valid_flag := 'E';
             END IF;
              IF g_invoice(g_index).org_id  IS NOT NULL THEN
                  validate_customer_details ( val_inv_rec(l_inv_cnt).leg_customer_trx_id
                    , val_inv_rec(l_inv_cnt).leg_customer_number
                    , val_inv_rec(l_inv_cnt).leg_bill_to_address
                    , val_inv_rec(l_inv_cnt).leg_ship_to_address
                    , g_invoice(g_index).org_id
                    , l_validate_flag
                    );
                  IF l_validate_flag ='E' THEN
                l_valid_flag := 'E';
                  END IF;
              END IF;
        --       g_invoice (g_index).system_bill_customer_id := 3040;--2055;                                                                                   --test
        --       g_invoice(g_index).system_bill_customer_id  := 205;--test
        --       g_invoice (g_index).system_bill_address_id := 1120;--1095;                                                                                    --test
             validate_currency (val_inv_rec(l_inv_cnt).leg_currency_code, val_inv_rec(l_inv_cnt).leg_customer_trx_id, l_validate_flag);
             IF l_validate_flag = 'E'
             THEN
                l_valid_flag := 'E';
             END IF;
             validate_trx_type (val_inv_rec(l_inv_cnt).leg_customer_trx_id, val_inv_rec(l_inv_cnt).leg_cust_trx_type_name, val_inv_rec(l_inv_cnt).leg_source_system, l_validate_flag);
             IF l_validate_flag = 'E'
             THEN
                l_valid_flag := 'E';
             END IF;
             IF val_inv_rec(l_inv_cnt).leg_cust_trx_type_name LIKE '%OKS%'
             THEN
                l_gl_date := val_inv_rec(l_inv_cnt).leg_gl_date;
             ELSE
                l_gl_date := g_gl_date;
             END IF;
             validate_payment_terms (val_inv_rec(l_inv_cnt).leg_customer_trx_id, val_inv_rec(l_inv_cnt).leg_term_name, l_validate_flag);
             IF l_validate_flag = 'E'
             THEN
                l_valid_flag := 'E';
             END IF;
             IF l_ledger_id IS NOT NULL
             THEN
                validate_gl_period (val_inv_rec(l_inv_cnt).leg_customer_trx_id, l_gl_date, l_ledger_id, piv_period_name, l_validate_flag);
                IF l_validate_flag = 'E'
                THEN
                   l_valid_flag := 'E';
                END IF;
             END IF;
             IF NVL (l_valid_flag, 'Y') = 'E'
             THEN
                g_invoice (g_index).process_flag := 'E';
                g_invoice (g_index).ERROR_TYPE := 'ERR_VAL';
                g_retcode := 1;
             ELSE
                g_invoice (g_index).process_flag := 'N';
             END IF;
             -- Assign value to table type g_invoice
             g_invoice (g_index).leg_customer_trx_id := val_inv_rec(l_inv_cnt).leg_customer_trx_id;
             g_invoice (g_index).trx_number := val_inv_rec(l_inv_cnt).leg_trx_number;
             g_invoice (g_index).gl_date := l_gl_date;
             g_invoice (g_index).header_attribute_category := val_inv_rec(l_inv_cnt).leg_header_attribute_category;
             g_invoice (g_index).header_attribute1 := val_inv_rec(l_inv_cnt).leg_header_attribute1;
             g_invoice (g_index).header_attribute2 := val_inv_rec(l_inv_cnt).leg_header_attribute2;
             g_invoice (g_index).header_attribute3 := val_inv_rec(l_inv_cnt).leg_header_attribute3;
             g_invoice (g_index).header_attribute4 := val_inv_rec(l_inv_cnt).leg_header_attribute4;
             g_invoice (g_index).header_attribute5 := val_inv_rec(l_inv_cnt).leg_header_attribute5;
             g_invoice (g_index).header_attribute6 := val_inv_rec(l_inv_cnt).leg_header_attribute6;
             g_invoice (g_index).header_attribute7 := val_inv_rec(l_inv_cnt).leg_header_attribute7;
             g_invoice (g_index).header_attribute8 := val_inv_rec(l_inv_cnt).leg_header_attribute8;
             g_invoice (g_index).header_attribute9 := val_inv_rec(l_inv_cnt).leg_header_attribute9;
             g_invoice (g_index).header_attribute10 := val_inv_rec(l_inv_cnt).leg_header_attribute10;
             g_invoice (g_index).header_attribute11 := val_inv_rec(l_inv_cnt).leg_header_attribute11;
             g_invoice (g_index).header_attribute12 := val_inv_rec(l_inv_cnt).leg_header_attribute12;
             g_invoice (g_index).header_attribute13 := val_inv_rec(l_inv_cnt).leg_header_attribute13;
             g_invoice (g_index).header_attribute14 := val_inv_rec(l_inv_cnt).leg_header_attribute14;
             g_invoice (g_index).header_attribute15 := val_inv_rec(l_inv_cnt).leg_header_attribute15;
             g_invoice (g_index).trx_date := val_inv_rec(l_inv_cnt).leg_trx_date;
             g_invoice (g_index).batch_source_name := 'CONVERSION';
             g_invoice (g_index).purchase_order := val_inv_rec(l_inv_cnt).leg_purchase_order;
             g_invoice (g_index).leg_operating_unit := val_inv_rec(l_inv_cnt).leg_operating_unit;
             g_invoice (g_index).leg_currency_code := val_inv_rec(l_inv_cnt).leg_currency_code;
             g_index := g_index + 1;
    --         l_inv_cnt := l_inv_cnt + 1;
              END LOOP;
          END LOOP;
          --
          CLOSE val_inv_cur;
          -- Update invoice line staging table with derived values
          IF g_invoice.EXISTS (1)
          THEN
             FORALL l_indx IN 1 .. g_invoice.COUNT
                UPDATE xxar_invoices_stg
                   SET org_id = g_invoice (l_indx).org_id,
                       set_of_books_id = g_invoice (l_indx).set_of_books_id,
                       currency_code = g_invoice (l_indx).leg_currency_code,
                       trx_number = g_invoice (l_indx).trx_number,
                       system_bill_customer_id = g_invoice (l_indx).system_bill_customer_id,
                       system_bill_address_id = g_invoice (l_indx).system_bill_address_id,
                       system_ship_address_id = g_invoice (l_indx).system_ship_address_id,
                       term_id = g_invoice (l_indx).term_id,
                       transaction_type_id = g_invoice (l_indx).transaction_type_id,
                       process_flag = g_invoice (l_indx).process_flag,
                       ERROR_TYPE = g_invoice (l_indx).ERROR_TYPE,
                       gl_date = g_invoice (l_indx).gl_date,
                       header_attribute_category = g_invoice (l_indx).header_attribute_category,
                       header_attribute1 = g_invoice (l_indx).header_attribute1,
                       header_attribute2 = g_invoice (l_indx).header_attribute2,
                       header_attribute3 = g_invoice (l_indx).header_attribute3,
                       header_attribute4 = g_invoice (l_indx).header_attribute4,
                       header_attribute5 = g_invoice (l_indx).header_attribute5,
                       header_attribute6 = g_invoice (l_indx).header_attribute6,
                       header_attribute7 = g_invoice (l_indx).header_attribute7,
                       header_attribute8 = g_invoice (l_indx).header_attribute8,
                       header_attribute9 = g_invoice (l_indx).header_attribute9,
                       header_attribute10 = g_invoice (l_indx).header_attribute10,
                       header_attribute11 = g_invoice (l_indx).header_attribute11,
                       header_attribute12 = g_invoice (l_indx).header_attribute12,
                       header_attribute13 = g_invoice (l_indx).header_attribute13,
                       header_attribute14 = g_invoice (l_indx).header_attribute14,
                       header_attribute15 = g_invoice (l_indx).header_attribute15,
                       trx_type = g_invoice (l_indx).trx_type,
                       trx_date = g_invoice (l_indx).trx_date,
                       batch_source_name = g_invoice (l_indx).batch_source_name,
                       purchase_order = g_invoice (l_indx).purchase_order,
               last_update_date = sysdate,
               last_updated_by  = g_last_updated_by,
               last_update_login = g_login_id
                 WHERE leg_customer_trx_id = g_invoice (l_indx).leg_customer_trx_id
                   AND leg_operating_unit = g_invoice (l_indx).leg_operating_unit
                   AND batch_id = g_new_batch_id;
          END IF;
    */
    -- Perf
    /*
          -- If invoice line is invalid, mark corresponding distributions in error
          UPDATE xxar_invoices_dist_stg xds
             SET process_flag = 'E'
           WHERE EXISTS (
                    SELECT 1
                      FROM xxar_invoices_stg xis
                     WHERE xis.leg_customer_trx_id = xds.leg_customer_trx_id
                       AND xis.leg_cust_trx_line_id = xds.leg_cust_trx_line_id
                       --  AND xis.leg_operating_unit   = xds.leg_org_name    --commenting since operting unit name in lines/dist staging can be invalid
                       AND xis.process_flag = 'E'
                       AND batch_id = g_new_batch_id);
    */
    COMMIT;
    OPEN val_inv_det_cur;
    LOOP
      FETCH val_inv_det_cur BULK COLLECT
        INTO val_inv_det_rec LIMIT l_line_limit;
      EXIT WHEN val_inv_det_rec.COUNT = 0;
      FOR l_line_cnt IN 1 .. val_inv_det_rec.COUNT LOOP
        l_valid_flag  := 'Y';
        l_customer_id := val_inv_det_rec(l_line_cnt).system_bill_customer_id;
        --l_trx_type_id := val_inv_det_rec(l_line_cnt).transaction_type_id;
        l_org_id      := val_inv_det_rec(l_line_cnt).org_id;
        l_line_number := val_inv_det_rec(l_line_cnt).leg_line_number;
        --          g_int_line_att := xxar_intattribute_s.NEXTVAL;
        l_ledger_id := NULL;
        -- Initialize Loop Variables
        l_err_code                 := NULL;
        l_err_msg                  := NULL;
        l_upd_ret_status           := NULL;
        l_validate_line_flag       := NULL;
        l_inv_flag                 := NULL;
        l_cm_status_flag           := NULL;
        l_leg_int_line_attribute1  := NULL;
        l_leg_int_line_attribute2  := NULL;
        l_leg_int_line_attribute3  := NULL;
        l_leg_int_line_attribute4  := NULL;
        l_leg_int_line_attribute5  := NULL;
        l_leg_int_line_attribute6  := NULL;
        l_leg_int_line_attribute7  := NULL;
        l_leg_int_line_attribute8  := NULL;
        l_leg_int_line_attribute9  := NULL;
        l_leg_int_line_attribute10 := NULL;
        l_leg_int_line_attribute11 := NULL;
        l_leg_int_line_attribute12 := NULL;
        l_leg_int_line_attribute13 := NULL;
        l_leg_int_line_attribute14 := NULL;
        l_leg_int_line_attribute15 := NULL;
        l_leg_line_amount          := NULL;
        --Ver1.5 start

        --IF l_trx_number <> val_inv_det_rec(l_line_cnt).trx_number   -- commented for v1.48, Defect# 9542

        -- modified for v1.48, Defect# 9542
        IF l_trx_number <> val_inv_det_rec(l_line_cnt).trx_number OR
           l_leg_customer_trx_id <> val_inv_det_rec(l_line_cnt)
          .leg_customer_trx_id THEN
          l_header_attr4 := NULL;
          l_header_attr8 := NULL;
          --ver1.23 changes start
          l_header_attr13 := NULL;
          --ver1.23 changes end
          --ver1.25 changes start
          l_header_attr14 := NULL;
          --ver1.25 changes end
          --        l_header_attr1 :=
          --ver1.20.2 starts
          /*  IF val_inv_det_rec(l_line_cnt)

             .leg_interface_line_context = 'PLANT SHIPMENTS (EUROPE)'

            THEN

               l_header_attr8 := val_inv_det_rec(l_line_cnt)

                                 .leg_interface_line_attribute15;

          */
          IF val_inv_det_rec(l_line_cnt)
           .leg_header_attribute_category = 'PLANT SHIPMENTS (EUROPE)' THEN
            l_header_attr8 := val_inv_det_rec(l_line_cnt)
                              .leg_header_attribute15;
            g_invoice_details(g_line_idx).header_attribute_category := g_interface_line_context;
            g_invoice_details(g_line_idx).header_attribute8 := l_header_attr8;
            --ver1.20.2 ends
            --ver1.20.2 starts
            /*   ELSIF val_inv_det_rec(l_line_cnt)

                      .leg_interface_line_context IN ('328696', '328697')

                     THEN

                        l_header_attr4 := val_inv_det_rec(l_line_cnt)

                                          .leg_interface_line_attribute14;



            */
            --ver1.20.2 ends
            --ver1.23 starts
          ELSIF val_inv_det_rec(l_line_cnt)
           .leg_interface_line_context IN ('328696', '328697') THEN
            l_header_attr13 := val_inv_det_rec(l_line_cnt)
                               .leg_interface_line_attribute8;
            g_invoice_details(g_line_idx).header_attribute_category := g_interface_line_context;
            g_invoice_details(g_line_idx).header_attribute13 := l_header_attr13;
            --ver1.23 ends
          END IF;
          --ver1.25 starts
          IF UPPER(NVL(val_inv_det_rec(l_line_cnt).site_status, 'XXX')) =
             'SASC LIVE' THEN
            l_header_attr14 := val_inv_det_rec(l_line_cnt)
                               .leg_batch_source_name;
            g_invoice_details(g_line_idx).header_attribute_category := g_interface_line_context;
            g_invoice_details(g_line_idx).header_attribute14 := l_header_attr14;
          END IF;
          --ver1.25 ends
          l_trx_number          := val_inv_det_rec(l_line_cnt).trx_number;
          l_leg_customer_trx_id := val_inv_det_rec(l_line_cnt)
                                   .leg_customer_trx_id; -- added for v1.48, Defect# 9542
        END IF;
        --Ver1.5 end
        --ver1.20.2 starts
        g_invoice_details(g_line_idx).header_attribute_category := g_interface_line_context;
        g_invoice_details(g_line_idx).header_attribute8 := l_header_attr8;
        --ver1.20.2 end
        --ver1.23 starts
        g_invoice_details(g_line_idx).header_attribute_category := g_interface_line_context;
        g_invoice_details(g_line_idx).header_attribute13 := l_header_attr13;
        --ver1.23 ends
        --ver1.25 starts
        g_invoice_details(g_line_idx).header_attribute_category := g_interface_line_context;
        g_invoice_details(g_line_idx).header_attribute14 := l_header_attr14;
        --ver1.25 ends
        --v1.12  Added to do currency check mandatory only when R12 org id is available
        IF l_org_id IS NOT NULL THEN
          -- If currency on transaction and functional currency do not match the conversion rate is required
          IF NVL(val_inv_det_rec(l_line_cnt).leg_currency_code, 'A') <> --1.4   Added by Rohit D for FOT
             NVL(val_inv_det_rec(l_line_cnt).func_curr, 'A') AND val_inv_det_rec(l_line_cnt)
            .leg_currency_code IS NOT NULL AND val_inv_det_rec(l_line_cnt)
            .func_curr IS NOT NULL THEN
            IF check_mandatory(pin_trx_id       => val_inv_det_rec(l_line_cnt)
                                                   .interface_txn_id,
                               piv_column_value => val_inv_det_rec(l_line_cnt)
                                                   .leg_conversion_date,
                               piv_column_name  => 'Conversion date') THEN
              l_valid_flag := 'E';
            END IF;
            IF check_mandatory(pin_trx_id       => val_inv_det_rec(l_line_cnt)
                                                   .interface_txn_id,
                               piv_column_value => val_inv_det_rec(l_line_cnt)
                                                   .leg_conversion_rate,
                               piv_column_name  => 'Conversion rate') THEN
              l_valid_flag := 'E';
            END IF;
          ELSE
            IF val_inv_det_rec(l_line_cnt).leg_conversion_date IS NOT NULL THEN
              l_valid_flag := 'E';
              l_err_code   := 'ETN_AR_CONVERSION_DATE_ERROR';
              g_retcode    := 1;
              l_err_msg    := 'Conversion date must be null since legacy currency is same as R12 currency';
              log_errors(pin_transaction_id      => val_inv_det_rec(l_line_cnt)
                                                    .interface_txn_id,
                         piv_source_column_name  => 'Legacy currency code',
                         piv_source_column_value => val_inv_det_rec(l_line_cnt)
                                                    .leg_currency_code,
                         piv_source_keyname1     => 'R12 currency code',
                         piv_source_keyvalue1    => val_inv_det_rec(l_line_cnt)
                                                    .currency_code,
                         piv_error_type          => 'ERR_VAL',
                         piv_error_code          => l_err_code,
                         piv_error_message       => l_err_msg,
                         pov_return_status       => l_log_ret_status,
                         pov_error_msg           => l_log_err_msg);
            END IF;
            IF NVL(val_inv_det_rec(l_line_cnt).leg_conversion_rate, 1) <> 1 THEN
              l_valid_flag := 'E';
              l_err_code   := 'ETN_AR_CONVERSION_RATE_ERROR';
              g_retcode    := 1;
              l_err_msg    := 'Conversion Rate must be null since legacy currency is same as R12 currency';
              log_errors(pin_transaction_id      => val_inv_det_rec(l_line_cnt)
                                                    .interface_txn_id,
                         piv_source_column_name  => 'Legacy currency code',
                         piv_source_column_value => val_inv_det_rec(l_line_cnt)
                                                    .leg_currency_code,
                         piv_source_keyname1     => 'R12 currency code',
                         piv_source_keyvalue1    => val_inv_det_rec(l_line_cnt)
                                                    .currency_code,
                         piv_error_type          => 'ERR_VAL',
                         piv_error_code          => l_err_code,
                         piv_error_message       => l_err_msg,
                         pov_return_status       => l_log_ret_status,
                         pov_error_msg           => l_log_err_msg);
            END IF;
          END IF;
        END IF;
        /*validate_amount(val_inv_det_rec(l_line_cnt).leg_line_amount,                --v1.12 Commented for Defect 1872 and 1874

        val_inv_det_rec(l_line_cnt).trx_type,

        val_inv_det_rec(l_line_cnt).interface_txn_id,

        l_validate_line_flag); */
        validate_amount(val_inv_det_rec(l_line_cnt)
                        .leg_inv_amount_due_remaining,
                        --v1.12 Added for Defect 1872 and 1874
                        val_inv_det_rec     (l_line_cnt).trx_type,
                        val_inv_det_rec     (l_line_cnt).interface_txn_id,
                        l_validate_line_flag);
        IF l_validate_line_flag = 'E' THEN
          l_valid_flag := 'E';
        END IF;
        g_invoice_details(g_line_idx).interface_line_context := g_interface_line_context;
        g_invoice_details(g_line_idx).header_attribute_category := g_interface_line_context;
        /*  --Ver1.5 Commented after DFF rationalization start
                    g_invoice_details (g_line_idx).interface_line_attribute1 :=
                                                  val_inv_det_rec (l_line_cnt).leg_interface_line_attribute1;
              g_invoice_details (g_line_idx).interface_line_attribute2 :=
                                                  val_inv_det_rec (l_line_cnt).leg_interface_line_attribute2;
                    g_invoice_details (g_line_idx).interface_line_attribute3 :=
                                                  val_inv_det_rec (l_line_cnt).leg_interface_line_attribute3;
                    g_invoice_details (g_line_idx).interface_line_attribute4 :=
                                                  val_inv_det_rec (l_line_cnt).leg_interface_line_attribute4;
                    g_invoice_details (g_line_idx).interface_line_attribute5 :=
                                                  val_inv_det_rec (l_line_cnt).leg_interface_line_attribute5;
                    g_invoice_details (g_line_idx).interface_line_attribute6 :=
                                                  val_inv_det_rec (l_line_cnt).leg_interface_line_attribute6;
                    g_invoice_details (g_line_idx).interface_line_attribute7 :=
                                                  val_inv_det_rec (l_line_cnt).leg_interface_line_attribute7;
                    g_invoice_details (g_line_idx).interface_line_attribute8 :=
                                                  val_inv_det_rec (l_line_cnt).leg_interface_line_attribute8;
                    g_invoice_details (g_line_idx).interface_line_attribute9 :=
                                                  val_inv_det_rec (l_line_cnt).leg_interface_line_attribute9;
                    g_invoice_details (g_line_idx).interface_line_attribute10 :=
                                                 val_inv_det_rec (l_line_cnt).leg_interface_line_attribute10;
                    g_invoice_details (g_line_idx).interface_line_attribute11 :=
                                                 val_inv_det_rec (l_line_cnt).leg_interface_line_attribute11;
                    g_invoice_details (g_line_idx).interface_line_attribute12 :=
                                                 val_inv_det_rec (l_line_cnt).leg_interface_line_attribute12;
                    g_invoice_details (g_line_idx).interface_line_attribute13 :=
                                                 val_inv_det_rec (l_line_cnt).leg_interface_line_attribute13;
                    --        g_invoice_details(g_line_idx).interface_line_attribute14    := val_inv_det_rec(l_line_cnt).leg_interface_line_attribute14;
                    --        g_invoice_details(g_line_idx).interface_line_attribute15    := NVL(val_inv_det_rec(l_line_cnt).link_to_line_attribute15, g_int_line_att);
                    g_invoice_details (g_line_idx).interface_line_attribute14 :=
                                                            val_inv_det_rec (l_line_cnt).leg_customer_trx_id;
        */
        --Ver1.5 Changes after DFF rationalization start
        IF val_inv_det_rec(l_line_cnt)
         .leg_interface_line_context = 'PLANT SHIPMENTS (EUROPE)' THEN
          IF val_inv_det_rec(l_line_cnt)
           .leg_interface_line_attribute1 IS NOT NULL THEN
            g_invoice_details(g_line_idx).interface_line_attribute1 := val_inv_det_rec(l_line_cnt)
                                                                       .leg_interface_line_attribute1;
          ELSE
            g_invoice_details(g_line_idx).interface_line_attribute1 := val_inv_det_rec(l_line_cnt)
                                                                       .leg_trx_number;
          END IF;
          --ver1.20.2 changes start
          --g_invoice_details(g_line_idx).header_attribute_category := g_interface_line_context;
          --g_invoice_details(g_line_idx).header_attribute8 := l_header_attr8;
          --ver1.20.2 changes end
        ELSIF val_inv_det_rec(l_line_cnt)
         .leg_interface_line_context IN ('328696', '328697') THEN
          IF val_inv_det_rec(l_line_cnt)
           .leg_interface_line_attribute2 IS NOT NULL THEN
            g_invoice_details(g_line_idx).interface_line_attribute1 := val_inv_det_rec(l_line_cnt)
                                                                       .leg_interface_line_attribute2;
          ELSE
            g_invoice_details(g_line_idx).interface_line_attribute1 := val_inv_det_rec(l_line_cnt)
                                                                       .leg_trx_number;
          END IF;
          g_invoice_details(g_line_idx).header_attribute_category := g_interface_line_context;
          g_invoice_details(g_line_idx).header_attribute4 := l_header_attr4;
        ELSE
          g_invoice_details(g_line_idx).interface_line_attribute1 := val_inv_det_rec(l_line_cnt)
                                                                     .leg_trx_number;
        END IF;
        g_invoice_details(g_line_idx).interface_line_attribute15 := xxar_intattribute_s.NEXTVAL;
        --      g_invoice_details (g_line_idx).header_attribute_category := g_interface_line_context;
        -- DFF : Factoring Changes
        --      g_invoice_details (g_line_idx).header_attribute1 := leg_header_attribute1;
        --ver1.19 changes start
        /*  IF g_invoice_details (g_line_idx).leg_source_system = 'ISSC' THEN
                   IF  g_invoice_details (g_line_idx).leg_header_attribute14 IS NOT NULL THEN
                       IF g_invoice_details (g_line_idx).leg_header_attribute14 IN ('A','D','S') THEN
                          g_invoice_details (g_line_idx).header_attribute1 := g_invoice_details (g_line_idx).leg_header_attribute14;
                       ELSE
                          g_invoice_details (g_line_idx).header_attribute1 := null;
                       END IF;
                   END IF;
                ELSE -- FSC
                   IF g_invoice_details (g_line_idx).leg_header_attribute6 IS NOT NULL THEN
                      IF g_invoice_details (g_line_idx).leg_header_attribute6 IN ('A','D') THEN
                         g_invoice_details (g_line_idx).header_attribute1 := g_invoice_details (g_line_idx).leg_header_attribute6;
                      END IF;
                   ELSE
                      IF g_invoice_details (g_line_idx).leg_header_attribute4 = 'F' THEN
                         g_invoice_details (g_line_idx).header_attribute1 := 'S';
                      ELSE
                         g_invoice_details (g_line_idx).header_attribute1 := 'I';
                      END IF;
                   END IF;
                END IF;
        */
        IF val_inv_det_rec(l_line_cnt).leg_source_system = 'ISSC' THEN
          IF val_inv_det_rec(l_line_cnt).leg_header_attribute14 IS NOT NULL THEN
            IF val_inv_det_rec(l_line_cnt)
             .leg_header_attribute14 IN ('A', 'D', 'S') THEN
              g_invoice_details(g_line_idx).header_attribute1 := val_inv_det_rec(l_line_cnt)
                                                                 .leg_header_attribute14;
            ELSE
              g_invoice_details(g_line_idx).header_attribute1 := NULL;
            END IF;
          END IF;
        ELSE
          -- FSC
          IF val_inv_det_rec(l_line_cnt).leg_header_attribute6 IS NOT NULL THEN
            IF val_inv_det_rec(l_line_cnt)
             .leg_header_attribute6 IN ('A', 'D') THEN
              g_invoice_details(g_line_idx).header_attribute1 := val_inv_det_rec(l_line_cnt)
                                                                 .leg_header_attribute6;
            END IF;
          ELSE
            IF val_inv_det_rec(l_line_cnt).leg_header_attribute4 = 'F' THEN
              g_invoice_details(g_line_idx).header_attribute1 := 'S';
            ELSE
              g_invoice_details(g_line_idx).header_attribute1 := 'I';
            END IF;
          END IF;
        END IF;
        --ver1.19 changes end
        --Ver 1.9 changes start
        l_line_dff_rec(val_inv_det_rec(l_line_cnt).leg_cust_trx_line_id).leg_line_amount := g_invoice_details(g_line_idx)
                                                                                            .leg_line_amount;
        l_line_dff_rec(val_inv_det_rec(l_line_cnt).leg_cust_trx_line_id).interface_line_attribute1 := g_invoice_details(g_line_idx)
                                                                                                      .interface_line_attribute1;
        l_line_dff_rec(val_inv_det_rec(l_line_cnt).leg_cust_trx_line_id).interface_line_attribute15 := g_invoice_details(g_line_idx)
                                                                                                       .interface_line_attribute15;
        --Ver 1.9 changes end
        --Ver1.5 Changes after DFF rationalization end
        --ver1.21.3 changes start
        --g_invoice_details(g_line_idx).amount_includes_tax_flag := 'N';
        --ver1.21.3 changes end
        IF val_inv_det_rec(l_line_cnt).leg_line_type = 'TAX' THEN
          l_validate_line_flag := NULL;
          /*               validate_tax (val_inv_det_rec (l_line_cnt).interface_txn_id,
                                       val_inv_det_rec (l_line_cnt).leg_tax_code,
                                       l_org_id,
                                       l_validate_line_flag,
                                       g_invoice_details (g_line_idx).tax_code,
                                       g_invoice_details (g_line_idx).tax_regime_code,
                                       g_invoice_details (g_line_idx).tax_rate_code,
                                       g_invoice_details (g_line_idx).tax,
                                       g_invoice_details (g_line_idx).tax_status_code,
                                       g_invoice_details (g_line_idx).tax_jurisdiction_code
                                      );


                         IF NVL (l_validate_line_flag, 'S') = 'E'
                         THEN
                            l_valid_flag := 'E';
                         END IF;
          */ --perf
          --ver1.21.3 changes start
          --    g_invoice_details(g_line_idx).amount_includes_tax_flag := val_inv_det_rec(l_line_cnt)  --ttax
          --                                                               .amount_includes_tax_flag;
          g_invoice_details(g_line_idx).amount_includes_tax_flag := val_inv_det_rec(l_line_cnt) --ttax
                                                                    .leg_amount_includes_tax_flag;
          --ver1.21.3 changes end
        END IF;
        -- Line type must either be line or tax or freight
        IF val_inv_det_rec(l_line_cnt).leg_line_type <> 'LINE' AND val_inv_det_rec(l_line_cnt)
           .leg_line_type <> 'TAX' AND val_inv_det_rec(l_line_cnt)
           .leg_line_type <> 'FREIGHT' THEN
          l_err_code   := 'ETN_AR_LINE_TYPE_EXCEPTION';
          g_retcode    := 1;
          l_err_msg    := 'Error : Invalid line type. Line type must either be LINE, TAX or FREIGHT ';
          l_valid_flag := 'E';
          log_errors(pin_transaction_id      => val_inv_det_rec(l_line_cnt)
                                                .interface_txn_id,
                     piv_source_column_name  => 'Legacy line type',
                     piv_source_column_value => val_inv_det_rec(l_line_cnt)
                                                .leg_line_type,
                     piv_error_type          => 'ERR_VAL',
                     piv_error_code          => l_err_code,
                     piv_error_message       => l_err_msg,
                     pov_return_status       => l_log_ret_status,
                     pov_error_msg           => l_log_err_msg);
        END IF;
        -- If line type is Tax the tax code must be provided
        IF val_inv_det_rec(l_line_cnt).leg_line_type = 'TAX'
        --            AND val_inv_det_rec(l_line_cnt).tax_code IS NULL
         THEN
          IF check_mandatory(pin_trx_id       => val_inv_det_rec(l_line_cnt)
                                                 .interface_txn_id,
                             piv_column_value => val_inv_det_rec(l_line_cnt)
                                                 .leg_tax_code,
                             piv_column_name  => 'Tax Code') THEN
            l_valid_flag := 'E';
          END IF;
          -- Set the link_to_line_attributes of the tax line equal to interface_line_attributes of the invoice line linked to it
          -- link_to_line_context will be hardcoded to Conversion
          IF val_inv_det_rec(l_line_cnt)
           .leg_link_to_cust_trx_line_id IS NOT NULL THEN
            BEGIN
              --ver 1.9 changes start
              /*
                                   SELECT                                            --leg_interface_line_context
                                          leg_interface_line_attribute1, leg_interface_line_attribute2,
                                          leg_interface_line_attribute3, leg_interface_line_attribute4,
                                          leg_interface_line_attribute5, leg_interface_line_attribute6,
                                          leg_interface_line_attribute7, leg_interface_line_attribute8,
                                          leg_interface_line_attribute9, leg_interface_line_attribute10,
                                          leg_interface_line_attribute11, leg_interface_line_attribute12,
                                          leg_interface_line_attribute13, leg_customer_trx_id,
                                          leg_cust_trx_line_id,
                                                               --v1.3 FOT issue fix start
                                                               leg_line_amount
                                     --v1.3 FOT issue fix end
                                   INTO   l_leg_int_line_attribute1, l_leg_int_line_attribute2,
                                          l_leg_int_line_attribute3, l_leg_int_line_attribute4,
                                          l_leg_int_line_attribute5, l_leg_int_line_attribute6,
                                          l_leg_int_line_attribute7, l_leg_int_line_attribute8,
                                          l_leg_int_line_attribute9, l_leg_int_line_attribute10,
                                          l_leg_int_line_attribute11, l_leg_int_line_attribute12,
                                          l_leg_int_line_attribute13, l_leg_int_line_attribute14,
                                          l_leg_int_line_attribute15,
                                                                     --v1.3 FOT issue fix start
                                                                     l_leg_line_amount
                                     --v1.3 FOT issue fix end
                                   FROM   xxar_invoices_stg
                                    WHERE leg_cust_trx_line_id =
                                                         val_inv_det_rec (l_line_cnt).leg_link_to_cust_trx_line_id
                                      AND org_id = val_inv_det_rec (l_line_cnt).org_id;

              */
              print_log_message('249-1');
              print_log_message('val_inv_det_rec(l_line_cnt).leg_link_to_cust_trx_line_id ' || val_inv_det_rec(l_line_cnt)
                                .leg_link_to_cust_trx_line_id);
              l_leg_line_amount := l_line_dff_rec(val_inv_det_rec(l_line_cnt).leg_link_to_cust_trx_line_id)
                                   .leg_line_amount;
              print_log_message('249-2');
              l_leg_int_line_attribute1 := l_line_dff_rec(val_inv_det_rec(l_line_cnt).leg_link_to_cust_trx_line_id)
                                           .interface_line_attribute1;
              print_log_message('249-3');
              l_leg_int_line_attribute15 := l_line_dff_rec(val_inv_det_rec(l_line_cnt).leg_link_to_cust_trx_line_id)
                                            .interface_line_attribute15;
            EXCEPTION
              WHEN NO_DATA_FOUND THEN
                l_err_code   := 'ETN_TAX_LINK_ERROR';
                l_err_msg    := 'Error : Cannot find invoice line corresponding to tax line ';
                l_valid_flag := 'E';
                g_retcode    := 1;
                log_errors(pin_transaction_id      => val_inv_det_rec(l_line_cnt)
                                                      .interface_txn_id,
                           piv_source_column_name  => 'Legacy link_to_customer_trx_line_id',
                           piv_source_column_value => val_inv_det_rec(l_line_cnt)
                                                      .leg_link_to_cust_trx_line_id,
                           piv_error_type          => 'ERR_VAL',
                           piv_error_code          => l_err_code,
                           piv_error_message       => l_err_msg,
                           pov_return_status       => l_log_ret_status,
                           pov_error_msg           => l_log_err_msg);
              WHEN OTHERS THEN
                l_err_code   := 'ETN_AR_PROCEDURE_EXCEPTION';
                l_err_msg    := 'Error : Error linking tax with invoice line ' ||
                                SUBSTR(SQLERRM, 1, 150);
                l_valid_flag := 'E';
                g_retcode    := 2;
                log_errors(pin_transaction_id      => val_inv_det_rec(l_line_cnt)
                                                      .interface_txn_id,
                           piv_source_column_name  => 'Legacy link_to_customer_trx_line_id',
                           piv_source_column_value => val_inv_det_rec(l_line_cnt)
                                                      .leg_link_to_cust_trx_line_id,
                           piv_error_type          => 'ERR_VAL',
                           piv_error_code          => l_err_code,
                           piv_error_message       => l_err_msg,
                           pov_return_status       => l_log_ret_status,
                           pov_error_msg           => l_log_err_msg);
            END;
            --ver 1.9 changes end
            --v1.3 FOT issue fix start
            --g_invoice_details (g_line_idx).tax_rate :=
            --    (l_leg_line_amount * 100) / val_inv_det_rec (l_line_cnt).leg_line_amount;
            --v1.3 FOT issue fix end
            --      IF val_inv_det_rec (l_line_cnt).leg_line_amount = 0 THEN
            IF l_leg_line_amount = 0 THEN
              g_invoice_details(g_line_idx).tax_rate := 1;
            ELSE
              g_invoice_details(g_line_idx).tax_rate := (val_inv_det_rec(l_line_cnt)
                                                        .leg_line_amount * 100) /
                                                        l_leg_line_amount; --- 1.4 Changed for FOT issue Rohit D
            END IF;
            g_invoice_details(g_line_idx).link_to_line_context := g_interface_line_context;
            g_invoice_details(g_line_idx).link_to_line_attribute1 := l_leg_int_line_attribute1;
            --Ver1.5 start
            /*                  g_invoice_details (g_line_idx).link_to_line_attribute2 :=
                                                                                       l_leg_int_line_attribute2;
                              g_invoice_details (g_line_idx).link_to_line_attribute3 :=
                                                                                       l_leg_int_line_attribute3;
                              g_invoice_details (g_line_idx).link_to_line_attribute4 :=
                                                                                       l_leg_int_line_attribute4;
                              g_invoice_details (g_line_idx).link_to_line_attribute5 :=
                                                                                       l_leg_int_line_attribute5;
                              g_invoice_details (g_line_idx).link_to_line_attribute6 :=
                                                                                       l_leg_int_line_attribute6;
                              g_invoice_details (g_line_idx).link_to_line_attribute7 :=
                                                                                       l_leg_int_line_attribute7;
                              g_invoice_details (g_line_idx).link_to_line_attribute8 :=
                                                                                       l_leg_int_line_attribute8;
                              g_invoice_details (g_line_idx).link_to_line_attribute9 :=
                                                                                       l_leg_int_line_attribute9;
                              g_invoice_details (g_line_idx).link_to_line_attribute10 :=
                                                                                      l_leg_int_line_attribute10;
                              g_invoice_details (g_line_idx).link_to_line_attribute11 :=
                                                                                      l_leg_int_line_attribute11;
                              g_invoice_details (g_line_idx).link_to_line_attribute12 :=
                                                                                      l_leg_int_line_attribute12;
                              g_invoice_details (g_line_idx).link_to_line_attribute13 :=
                                                                                      l_leg_int_line_attribute13;
                              g_invoice_details (g_line_idx).link_to_line_attribute14 :=
                                                                                      l_leg_int_line_attribute14;
            */ --Ver1.5 end
            g_invoice_details(g_line_idx).link_to_line_attribute15 := l_leg_int_line_attribute15;
            /*--Ver 1.5 Commented for DFF rationalization start
                              IF val_inv_det_rec (l_line_cnt).leg_interface_line_attribute1 IS NULL
                              THEN
                                 g_invoice_details (g_line_idx).interface_line_context :=
                                                                                       g_interface_line_context;
                                 g_invoice_details (g_line_idx).interface_line_attribute1 :=
                                                                                      l_leg_int_line_attribute1;
                     g_invoice_details (g_line_idx).interface_line_attribute2 :=
                                                                                      l_leg_int_line_attribute2;
                                 g_invoice_details (g_line_idx).interface_line_attribute3 :=
                                                                                      l_leg_int_line_attribute3;
                                 g_invoice_details (g_line_idx).interface_line_attribute4 :=
                                                                                      l_leg_int_line_attribute4;
                                 g_invoice_details (g_line_idx).interface_line_attribute5 :=
                                                                                      l_leg_int_line_attribute5;
                                 g_invoice_details (g_line_idx).interface_line_attribute6 :=
                                                                                      l_leg_int_line_attribute6;
                                 g_invoice_details (g_line_idx).interface_line_attribute7 :=
                                                                                      l_leg_int_line_attribute7;
                                 g_invoice_details (g_line_idx).interface_line_attribute8 :=
                                                                                      l_leg_int_line_attribute8;
                                 g_invoice_details (g_line_idx).interface_line_attribute9 :=
                                                                                      l_leg_int_line_attribute9;
                                 g_invoice_details (g_line_idx).interface_line_attribute10 :=
                                                                                     l_leg_int_line_attribute10;
                                 g_invoice_details (g_line_idx).interface_line_attribute11 :=
                                                                                     l_leg_int_line_attribute11;
                                 g_invoice_details (g_line_idx).interface_line_attribute12 :=
                                                                                     l_leg_int_line_attribute12;
                                 g_invoice_details (g_line_idx).interface_line_attribute13 :=
                                                                                     l_leg_int_line_attribute13;
                                 g_invoice_details (g_line_idx).interface_line_attribute14 :=
                                                               val_inv_det_rec (l_line_cnt).leg_customer_trx_id;
                                 g_invoice_details (g_line_idx).interface_line_attribute15 :=
                                                              val_inv_det_rec (l_line_cnt).leg_cust_trx_line_id;
                              END IF;
            */ --Ver 1.5 Commented for DFF rationalization end
          END IF;
        END IF;
        -- Set the link_to_line_attributes of the tax line equal to interface_line_attributes of the invoice line linked to it
        -- link_to_line_context will be hardcoded to Conversion
        IF val_inv_det_rec(l_line_cnt).trx_type = 'CM' AND val_inv_det_rec(l_line_cnt)
           .leg_reference_line_id IS NOT NULL THEN
          BEGIN
            SELECT 'Y'
              INTO l_inv_flag
              FROM xxar_invoices_stg
             WHERE leg_cust_trx_line_id = val_inv_det_rec(l_line_cnt)
                  .leg_reference_line_id
               AND org_id = val_inv_det_rec(l_line_cnt).org_id
               AND ROWNUM = 1;
          EXCEPTION
            WHEN OTHERS THEN
              l_inv_flag := 'N';
          END;
          BEGIN
            SELECT 'S'
              INTO l_cm_status_flag
              FROM ra_customer_trx_all   rct,
                   xxar_invoices_stg     xis,
                   ra_cust_trx_types_all rctt
             WHERE rct.org_id = xis.org_id
               AND rct.trx_number = xis.leg_trx_number
               AND rct.org_id = rctt.org_id
               AND rct.cust_trx_type_id = rctt.cust_trx_type_id
               AND xis.trx_type = rctt.TYPE
               AND xis.org_id = val_inv_det_rec(l_line_cnt).org_id
               AND xis.leg_cust_trx_line_id = val_inv_det_rec(l_line_cnt)
                  .leg_reference_line_id
               AND ROWNUM = 1;
          EXCEPTION
            WHEN OTHERS THEN
              l_cm_status_flag := 'E';
              --Error: yet to be converted
          END;
          IF (l_inv_flag = 'Y' AND l_cm_status_flag = 'E') THEN
            NULL;
            l_valid_flag := 'E';
          ELSIF l_cm_status_flag = 'S' THEN
            g_invoice_details(g_line_idx).reference_line_id := val_inv_det_rec(l_line_cnt)
                                                               .leg_reference_line_id;
          ELSE
            NULL;
            l_valid_flag := 'E';
          END IF;
        END IF;
        IF val_inv_det_rec(l_line_cnt).leg_line_type = 'FREIGHT' THEN
          g_invoice_details(g_line_idx).line_type := 'LINE';
          --------------------------------End of Frieght Line Error------------------------------------------
          --v1.3 FOT Issue comment starts
          --g_invoice_details (g_line_idx).description := 'Conversion Freight1';
          --v1.3 FOT Issue comment end
          l_org_id  := NULL;
          l_ou_name := NULL;
          IF val_inv_det_rec(l_line_cnt).org_id IS NOT NULL THEN
            BEGIN
              /*

              get_org(  p_leg_source_system => val_inv_det_rec(l_line_cnt).leg_source_system,

                        p_leg_customer_trx_id => val_inv_det_rec(l_line_cnt).leg_customer_trx_id,

                        p_org_id => l_org_id,

                         p_ou_name => l_ou_name);

               */
              SELECT memo_line_id,
                     --v1.3 FOT Issue fix starts
                     description
              --v1.3 FOT Issue fix ends
                INTO g_invoice_details(g_line_idx).memo_line_id,
                     --v1.3 FOT Issue fix starts
                     g_invoice_details(g_line_idx).description
              --v1.3 FOT Issue fix ends
                FROM ar_memo_lines_all_tl
               WHERE LANGUAGE = USERENV('LANG')
                 AND UPPER(NAME) LIKE '%CONVERSION%FREIGHT%'
                    -- AND org_id = l_org_id;         -- SS
                    -- org_id = l_frght_org_id;
                 AND org_id = val_inv_det_rec(l_line_cnt).org_id;
            EXCEPTION
              WHEN NO_DATA_FOUND THEN
                l_err_code   := 'ETN_MEMO_LINE_ERROR';
                l_err_msg    := 'Error : Cannot find memo line to create freight lines ';
                l_valid_flag := 'E';
                g_retcode    := 1;
                log_errors(pin_transaction_id      => val_inv_det_rec(l_line_cnt)
                                                      .interface_txn_id,
                           piv_source_column_name  => 'MEMO LINE NAME',
                           piv_source_column_value => 'Conversion Freight',
                           piv_error_type          => 'ERR_VAL',
                           piv_error_code          => l_err_code,
                           piv_error_message       => l_err_msg,
                           pov_return_status       => l_log_ret_status,
                           pov_error_msg           => l_log_err_msg);
              WHEN OTHERS THEN
                l_err_code   := 'ETN_AR_PROCEDURE_EXCEPTION';
                l_err_msg    := 'Error : Error linking tax with invoice line ' ||
                                SUBSTR(SQLERRM, 1, 150);
                l_valid_flag := 'E';
                g_retcode    := 2;
                log_errors(pin_transaction_id      => val_inv_det_rec(l_line_cnt)
                                                      .interface_txn_id,
                           piv_source_column_name  => 'Legacy link_to_customer_trx_line_id',
                           piv_source_column_value => val_inv_det_rec(l_line_cnt)
                                                      .leg_link_to_cust_trx_line_id,
                           piv_error_type          => 'ERR_VAL',
                           piv_error_code          => l_err_code,
                           piv_error_message       => l_err_msg,
                           pov_return_status       => l_log_ret_status,
                           pov_error_msg           => l_log_err_msg);
            END;
          END IF;
        ELSE
          g_invoice_details(g_line_idx).line_type := val_inv_det_rec(l_line_cnt)
                                                     .leg_line_type;
          g_invoice_details(g_line_idx).description := 'Converted Net Amount';
        END IF;
        --        IF NVL(l_valid_flag, 'Y') = 'Y' AND l_dist_flag = 'Y' THEN
        /*IF NVL(l_valid_flag, 'Y') = 'Y'

        THEN

           g_invoice_details(g_line_idx).process_flag := 'V';

        ELSE

           g_invoice_details(g_line_idx).process_flag := 'E';

           g_invoice_details(g_line_idx).ERROR_TYPE := 'ERR_VAL';

           g_retcode := 1;

        END IF;*/
        g_invoice_details(g_line_idx).interface_txn_id := val_inv_det_rec(l_line_cnt)
                                                          .interface_txn_id;
        g_invoice_details(g_line_idx).line_number := val_inv_det_rec(l_line_cnt)
                                                     .line_number;
        g_invoice_details(g_line_idx).conversion_type := 'User';
        g_invoice_details(g_line_idx).conversion_date := val_inv_det_rec(l_line_cnt)
                                                         .leg_conversion_date;
        g_invoice_details(g_line_idx).conversion_rate := NVL(val_inv_det_rec(l_line_cnt)
                                                             .leg_conversion_rate,
                                                             1);
        g_invoice_details(g_line_idx).line_amount := val_inv_det_rec(l_line_cnt)
                                                     .leg_line_amount;
        g_invoice_details(g_line_idx).reason_code := val_inv_det_rec(l_line_cnt)
                                                     .leg_reason_code;
        g_invoice_details(g_line_idx).taxable_flag := 'N';
        g_invoice_details(g_line_idx).comments := val_inv_det_rec(l_line_cnt)
                                                  .leg_comments;
        --ver1.20.2 changes start
        --g_invoice_details(g_line_idx).attribute_category := val_inv_det_rec(l_line_cnt)
        --                                                    .attribute_category;
        g_invoice_details(g_line_idx).attribute_category := 'Eaton';
        --ver1.20.2 changes end
        g_invoice_details(g_line_idx).attribute1 := val_inv_det_rec(l_line_cnt)
                                                    .attribute1;
        g_invoice_details(g_line_idx).attribute2 := val_inv_det_rec(l_line_cnt)
                                                    .attribute2;
        g_invoice_details(g_line_idx).attribute3 := val_inv_det_rec(l_line_cnt)
                                                    .attribute3;
        --ver1.20.2 changes start
        --g_invoice_details(g_line_idx).attribute4 := val_inv_det_rec(l_line_cnt).attribute4;
        IF (val_inv_det_rec(l_line_cnt)
           .leg_interface_line_context IN ('328696', '328697')) AND
           (val_inv_det_rec(l_line_cnt)
           .credit_office IN ('01105', '01169', '04172', '04176')) --Check for ELECTRICAL ledger
         THEN
          g_invoice_details(g_line_idx).attribute4 := val_inv_det_rec(l_line_cnt)
                                                      .leg_interface_line_attribute14; --assign DIVUSE4 of 11i
        ELSE
          g_invoice_details(g_line_idx).attribute4 := val_inv_det_rec(l_line_cnt)
                                                      .attribute4;
        END IF;
        --ver1.20.2 changes end

        -- v1.43 Populate attribute3 for US/CA OUs based on credit office, starts

        IF val_inv_det_rec(l_line_cnt).credit_office = '00098' THEN
          g_invoice_details(g_line_idx).header_attribute3 := val_inv_det_rec(l_line_cnt)
                                                             .LEG_INTERFACE_HDR_ATTRIBUTE13;
        ELSIF val_inv_det_rec(l_line_cnt).credit_office IS NOT NULL AND val_inv_det_rec(l_line_cnt)
              .credit_office <> '00098' THEN
          g_invoice_details(g_line_idx).header_attribute3 := val_inv_det_rec(l_line_cnt)
                                                             .LEG_INTERFACE_HDR_ATTRIBUTE6;
        END IF;
        -- v1.43 Populate attribute3 for US/CA OUs based on credit office, ends

        g_invoice_details(g_line_idx).attribute5 := val_inv_det_rec(l_line_cnt)
                                                    .attribute5;
        g_invoice_details(g_line_idx).attribute6 := val_inv_det_rec(l_line_cnt)
                                                    .attribute6;
        g_invoice_details(g_line_idx).attribute7 := val_inv_det_rec(l_line_cnt)
                                                    .attribute7;
        g_invoice_details(g_line_idx).attribute8 := val_inv_det_rec(l_line_cnt)
                                                    .attribute8;
        g_invoice_details(g_line_idx).attribute9 := val_inv_det_rec(l_line_cnt)
                                                    .attribute9;
        g_invoice_details(g_line_idx).attribute10 := val_inv_det_rec(l_line_cnt)
                                                     .attribute10;
        g_invoice_details(g_line_idx).attribute11 := val_inv_det_rec(l_line_cnt)
                                                     .attribute11;
        g_invoice_details(g_line_idx).attribute12 := val_inv_det_rec(l_line_cnt)
                                                     .attribute12;
        g_invoice_details(g_line_idx).attribute13 := val_inv_det_rec(l_line_cnt)
                                                     .attribute13;
        g_invoice_details(g_line_idx).attribute14 := val_inv_det_rec(l_line_cnt)
                                                     .attribute14;
        g_invoice_details(g_line_idx).attribute15 := val_inv_det_rec(l_line_cnt)
                                                     .attribute15;
        g_invoice_details(g_line_idx).header_gdf_attr_category := val_inv_det_rec(l_line_cnt)
                                                                  .leg_header_gdf_attr_category;
        g_invoice_details(g_line_idx).header_gdf_attribute1 := val_inv_det_rec(l_line_cnt)
                                                               .leg_header_gdf_attribute1;
        g_invoice_details(g_line_idx).header_gdf_attribute2 := val_inv_det_rec(l_line_cnt)
                                                               .leg_header_gdf_attribute2;
        g_invoice_details(g_line_idx).header_gdf_attribute3 := val_inv_det_rec(l_line_cnt)
                                                               .leg_header_gdf_attribute3;
        g_invoice_details(g_line_idx).header_gdf_attribute4 := val_inv_det_rec(l_line_cnt)
                                                               .leg_header_gdf_attribute4;
        g_invoice_details(g_line_idx).header_gdf_attribute5 := val_inv_det_rec(l_line_cnt)
                                                               .leg_header_gdf_attribute5;
        g_invoice_details(g_line_idx).header_gdf_attribute6 := val_inv_det_rec(l_line_cnt)
                                                               .leg_header_gdf_attribute6;
        g_invoice_details(g_line_idx).header_gdf_attribute7 := val_inv_det_rec(l_line_cnt)
                                                               .leg_header_gdf_attribute7;
        g_invoice_details(g_line_idx).header_gdf_attribute8 := val_inv_det_rec(l_line_cnt)
                                                               .leg_header_gdf_attribute8;
        g_invoice_details(g_line_idx).header_gdf_attribute9 := val_inv_det_rec(l_line_cnt)
                                                               .leg_header_gdf_attribute9;
        g_invoice_details(g_line_idx).header_gdf_attribute10 := val_inv_det_rec(l_line_cnt)
                                                                .leg_header_gdf_attribute10;
        g_invoice_details(g_line_idx).header_gdf_attribute11 := val_inv_det_rec(l_line_cnt)
                                                                .leg_header_gdf_attribute11;
        g_invoice_details(g_line_idx).header_gdf_attribute12 := val_inv_det_rec(l_line_cnt)
                                                                .leg_header_gdf_attribute12;
        g_invoice_details(g_line_idx).header_gdf_attribute13 := val_inv_det_rec(l_line_cnt)
                                                                .leg_header_gdf_attribute13;
        g_invoice_details(g_line_idx).header_gdf_attribute14 := val_inv_det_rec(l_line_cnt)
                                                                .leg_header_gdf_attribute14;
        g_invoice_details(g_line_idx).header_gdf_attribute15 := val_inv_det_rec(l_line_cnt)
                                                                .leg_header_gdf_attribute15;
        g_invoice_details(g_line_idx).header_gdf_attribute16 := val_inv_det_rec(l_line_cnt)
                                                                .leg_header_gdf_attribute16;
        g_invoice_details(g_line_idx).header_gdf_attribute17 := val_inv_det_rec(l_line_cnt)
                                                                .leg_header_gdf_attribute17;
        g_invoice_details(g_line_idx).header_gdf_attribute18 := val_inv_det_rec(l_line_cnt)
                                                                .leg_header_gdf_attribute18;
        g_invoice_details(g_line_idx).header_gdf_attribute19 := val_inv_det_rec(l_line_cnt)
                                                                .leg_header_gdf_attribute19;
        g_invoice_details(g_line_idx).header_gdf_attribute20 := val_inv_det_rec(l_line_cnt)
                                                                .leg_header_gdf_attribute20;
        g_invoice_details(g_line_idx).header_gdf_attribute21 := val_inv_det_rec(l_line_cnt)
                                                                .leg_header_gdf_attribute21;
        g_invoice_details(g_line_idx).header_gdf_attribute22 := val_inv_det_rec(l_line_cnt)
                                                                .leg_header_gdf_attribute22;
        g_invoice_details(g_line_idx).header_gdf_attribute23 := val_inv_det_rec(l_line_cnt)
                                                                .leg_header_gdf_attribute23;
        g_invoice_details(g_line_idx).header_gdf_attribute24 := val_inv_det_rec(l_line_cnt)
                                                                .leg_header_gdf_attribute24;
        g_invoice_details(g_line_idx).header_gdf_attribute25 := val_inv_det_rec(l_line_cnt)
                                                                .leg_header_gdf_attribute25;
        g_invoice_details(g_line_idx).header_gdf_attribute26 := val_inv_det_rec(l_line_cnt)
                                                                .leg_header_gdf_attribute26;
        g_invoice_details(g_line_idx).header_gdf_attribute27 := val_inv_det_rec(l_line_cnt)
                                                                .leg_header_gdf_attribute27;
        g_invoice_details(g_line_idx).header_gdf_attribute28 := val_inv_det_rec(l_line_cnt)
                                                                .leg_header_gdf_attribute28;
        g_invoice_details(g_line_idx).header_gdf_attribute29 := val_inv_det_rec(l_line_cnt)
                                                                .leg_header_gdf_attribute29;
        g_invoice_details(g_line_idx).header_gdf_attribute30 := val_inv_det_rec(l_line_cnt)
                                                                .leg_header_gdf_attribute30;
        g_invoice_details(g_line_idx).line_gdf_attr_category := val_inv_det_rec(l_line_cnt)
                                                                .leg_line_gdf_attr_category;
        g_invoice_details(g_line_idx).line_gdf_attribute1 := val_inv_det_rec(l_line_cnt)
                                                             .leg_line_gdf_attribute1;
        g_invoice_details(g_line_idx).line_gdf_attribute2 := val_inv_det_rec(l_line_cnt)
                                                             .leg_line_gdf_attribute2;
        g_invoice_details(g_line_idx).line_gdf_attribute3 := val_inv_det_rec(l_line_cnt)
                                                             .leg_line_gdf_attribute3;
        g_invoice_details(g_line_idx).line_gdf_attribute4 := val_inv_det_rec(l_line_cnt)
                                                             .leg_line_gdf_attribute4;
        g_invoice_details(g_line_idx).line_gdf_attribute5 := val_inv_det_rec(l_line_cnt)
                                                             .leg_line_gdf_attribute5;
        g_invoice_details(g_line_idx).line_gdf_attribute6 := val_inv_det_rec(l_line_cnt)
                                                             .leg_line_gdf_attribute6;
        g_invoice_details(g_line_idx).line_gdf_attribute7 := val_inv_det_rec(l_line_cnt)
                                                             .leg_line_gdf_attribute7;
        g_invoice_details(g_line_idx).line_gdf_attribute8 := val_inv_det_rec(l_line_cnt)
                                                             .leg_line_gdf_attribute8;
        g_invoice_details(g_line_idx).line_gdf_attribute9 := val_inv_det_rec(l_line_cnt)
                                                             .leg_line_gdf_attribute9;
        g_invoice_details(g_line_idx).line_gdf_attribute10 := val_inv_det_rec(l_line_cnt)
                                                              .leg_line_gdf_attribute10;
        g_invoice_details(g_line_idx).line_gdf_attribute11 := val_inv_det_rec(l_line_cnt)
                                                              .leg_line_gdf_attribute11;
        g_invoice_details(g_line_idx).line_gdf_attribute12 := val_inv_det_rec(l_line_cnt)
                                                              .leg_line_gdf_attribute12;
        g_invoice_details(g_line_idx).line_gdf_attribute13 := val_inv_det_rec(l_line_cnt)
                                                              .leg_line_gdf_attribute13;
        g_invoice_details(g_line_idx).line_gdf_attribute14 := val_inv_det_rec(l_line_cnt)
                                                              .leg_line_gdf_attribute14;
        g_invoice_details(g_line_idx).line_gdf_attribute15 := val_inv_det_rec(l_line_cnt)
                                                              .leg_line_gdf_attribute15;
        g_invoice_details(g_line_idx).line_gdf_attribute16 := val_inv_det_rec(l_line_cnt)
                                                              .leg_line_gdf_attribute16;
        g_invoice_details(g_line_idx).line_gdf_attribute17 := val_inv_det_rec(l_line_cnt)
                                                              .leg_line_gdf_attribute17;
        g_invoice_details(g_line_idx).line_gdf_attribute18 := val_inv_det_rec(l_line_cnt)
                                                              .leg_line_gdf_attribute18;
        g_invoice_details(g_line_idx).line_gdf_attribute19 := val_inv_det_rec(l_line_cnt)
                                                              .leg_line_gdf_attribute19;
        g_invoice_details(g_line_idx).line_gdf_attribute20 := val_inv_det_rec(l_line_cnt)
                                                              .leg_line_gdf_attribute20;
        g_invoice_details(g_line_idx).sales_order_date := val_inv_det_rec(l_line_cnt)
                                                          .leg_sales_order_date;
        g_invoice_details(g_line_idx).sales_order := val_inv_det_rec(l_line_cnt)
                                                     .leg_sales_order;
        g_invoice_details(g_line_idx).uom_name := val_inv_det_rec(l_line_cnt)
                                                  .leg_uom_name;
        g_invoice_details(g_line_idx).ussgl_transaction_code_context := val_inv_det_rec(l_line_cnt)
                                                                        .leg_ussgl_trx_code_context;
        g_invoice_details(g_line_idx).internal_notes := val_inv_det_rec(l_line_cnt)
                                                        .leg_internal_notes;
        g_invoice_details(g_line_idx).ship_date_actual := val_inv_det_rec(l_line_cnt)
                                                          .leg_ship_date_actual;
        g_invoice_details(g_line_idx).fob_point := val_inv_det_rec(l_line_cnt)
                                                   .leg_fob_point;
        g_invoice_details(g_line_idx).ship_via := val_inv_det_rec(l_line_cnt)
                                                  .leg_ship_via;
        g_invoice_details(g_line_idx).waybill_number := val_inv_det_rec(l_line_cnt)
                                                        .leg_waybill_number;
        g_invoice_details(g_line_idx).sales_order_line := val_inv_det_rec(l_line_cnt)
                                                          .leg_sales_order_line;
        g_invoice_details(g_line_idx).sales_order_source := val_inv_det_rec(l_line_cnt)
                                                            .leg_sales_order_source;
        g_invoice_details(g_line_idx).sales_order_revision := val_inv_det_rec(l_line_cnt)
                                                              .leg_sales_order_revision;
        g_invoice_details(g_line_idx).purchase_order := val_inv_det_rec(l_line_cnt)
                                                        .leg_purchase_order;
        g_invoice_details(g_line_idx).purchase_order_revision := val_inv_det_rec(l_line_cnt)
                                                                 .leg_purchase_order_revision;
        g_invoice_details(g_line_idx).purchase_order_date := val_inv_det_rec(l_line_cnt)
                                                             .leg_purchase_order_date;
        --ver 1.7  changes start
        /*

              g_invoice_details (g_line_idx).agreement_name :=

                                                             val_inv_det_rec (l_line_cnt).leg_agreement_name;

                    g_invoice_details (g_line_idx).agreement_id :=

                                                               val_inv_det_rec (l_line_cnt).leg_agreement_id;

        */
        --ver 1.7  changes end
        g_invoice_details(g_line_idx).quantity := val_inv_det_rec(l_line_cnt)
                                                  .leg_quantity;
        g_invoice_details(g_line_idx).quantity_ordered := val_inv_det_rec(l_line_cnt)
                                                          .leg_quantity_ordered;
        g_invoice_details(g_line_idx).unit_selling_price := val_inv_det_rec(l_line_cnt)
                                                            .leg_unit_selling_price;
        g_invoice_details(g_line_idx).unit_standard_price := val_inv_det_rec(l_line_cnt)
                                                             .leg_unit_standard_price;
        --v1.24 Added by Shailesh Chaudhari for CR 305000 START-------------------------
        BEGIN
          --print_log_message('Warehouse for Brazil OrgB1: ' ||val_inv_det_rec(l_line_cnt).warehouse_id);
          print_log_message('11i Warehouse for Brazil OrgB1: ' || val_inv_det_rec(l_line_cnt)
                            .leg_warehouse_id); -- added v1.28
          print_log_message('Warehouse for Brazil OrgB1: ' || val_inv_det_rec(l_line_cnt)
                            .org_id);
          ---for V1.29  Changes done by Piyush
          l_br_ou_name              := NULL;
          l_br_default_warehouse_id := NULL;
          BEGIN
            SELECT meaning
              INTO l_br_ou_name
              FROM fnd_lookup_values
             WHERE lookup_type = 'XXAR_BR_WAREHOUSE_OUS_LKP'
               AND enabled_flag = 'Y'
               AND LANGUAGE = 'US'
               AND g_sysdate BETWEEN start_date_active AND
                   NVL(end_date_active, g_sysdate)
               AND description = val_inv_det_rec(l_line_cnt).org_id;
          EXCEPTION
            WHEN OTHERS THEN
              l_br_ou_name := NULL;
          END;
          --IF   val_inv_det_rec(l_line_cnt).org_id in (501,502) THEN commented for V1.29
          IF l_br_ou_name IS NOT NULL THEN
            --IF val_inv_det_rec(l_line_cnt).warehouse_id IS NULL  THEN  commented for V1.29
            IF val_inv_det_rec(l_line_cnt).leg_warehouse_id IS NULL AND val_inv_det_rec(l_line_cnt)
               .leg_line_type = 'LINE' THEN
              print_log_message('Warehouse for Brazil OrgC1: ' || val_inv_det_rec(l_line_cnt)
                                .warehouse_id);
              IF l_br_ou_name = 'OU ETN LTDA 0185 BRL' THEN
                BEGIN
                  SELECT ORGANIZATION_ID
                    INTO l_br_default_warehouse_id
                    FROM hr_all_organization_units
                   WHERE --NAME = 'IO SAHQ BR DEFAULT' commented for 1.52
           NAME = 'IO SOUTH AMERICA SERVICE CENTER BR'; --added for 1.52
                EXCEPTION
                  WHEN OTHERS THEN
                    l_br_default_warehouse_id := NULL;
                END;
              ELSIF l_br_ou_name = 'OU ETN POWER SOL LTDA 4470 BRL' THEN
                BEGIN
                  SELECT ORGANIZATION_ID
                    INTO l_br_default_warehouse_id
                    FROM hr_all_organization_units
                   WHERE NAME = 'IO EPS SAO PAULO BR DEFAULT';
                EXCEPTION
                  WHEN OTHERS THEN
                    l_br_default_warehouse_id := NULL;
                END;
              ELSE
                BEGIN
                  SELECT ORGANIZATION_ID
                    INTO l_br_default_warehouse_id
                    FROM hr_all_organization_units
                   WHERE NAME = 'IO VG VALINHOS BR DEFAULT';
                EXCEPTION
                  WHEN OTHERS THEN
                    l_br_default_warehouse_id := NULL;
                END;
              END IF; -- end of  IF l_br_ou_name = 'OU ETN LTDA 0185 BRL'
              --val_inv_det_rec(l_line_cnt).warehouse_id := 506;  commented for V1.29
              val_inv_det_rec(l_line_cnt).warehouse_id := l_br_default_warehouse_id; --added for v1.29
              -- ELSIF   val_inv_det_rec(l_line_cnt).warehouse_id IS NOT NULL THEN commented for v1.29
            ELSIF val_inv_det_rec(l_line_cnt).leg_warehouse_id IS NOT NULL THEN
              --print_log_message('Warehouse for Brazil OrgD1: ' ||val_inv_det_rec(l_line_cnt).leg_warehouse_id);
              SELECT tag
                INTO l_attr1
                FROM fnd_lookup_values
               WHERE --description = to_char(val_inv_det_rec(l_line_cnt).warehouse_id) v1.29
               description =
               to_char(val_inv_det_rec(l_line_cnt).leg_warehouse_id)
               AND lookup_type = 'BR_AR_WAREHOUSE_IDS'
               AND ((attribute12 IN
               ('OU ETN LTDA 0185 BRL', 'OU ETN POWER SOL LTDA 4470 BRL')) OR
               (attribute13 IN
               ('OU ETN LTDA 0185 BRL', 'OU ETN POWER SOL LTDA 4470 BRL')) OR
               (attribute14 = 'Eaton Ltda OU'))
               AND attribute_category = 'BR_AR_WAREHOUSE_IDS'
               AND LANGUAGE = 'US';
              val_inv_det_rec(l_line_cnt).warehouse_id := to_number(l_attr1);
            END IF;
            print_log_message('R12 Warehouse for Brazil OrgB1: ' || val_inv_det_rec(l_line_cnt)
                              .warehouse_id); -- added v1.29
            --ver1.28 changes start

          ELSE   -- If warehouse lookup meaning value is not derived

             /**  Added below validation for v1.54 **/
             IF val_inv_det_rec(l_line_cnt).leg_operating_unit IN ('OU ELECTRICAL BR','OU FLUID POWER BR','OU TRUCK COMPONENTS BR') THEN
                l_err_code   := 'ETN_WAREHOUSE_MEANING_ERROR';
                l_err_msg    := 'Error : Meaning value not found for Brazil Org in lookup XXAR_BR_WAREHOUSE_OUS_LKP ';
                l_valid_flag := 'E';
                g_retcode    := 1;
                log_errors ( pin_transaction_id => val_inv_det_rec(l_line_cnt).interface_txn_id,
                             piv_source_column_name  => 'WAREHOUSE_ORG_ID',
                             piv_source_column_value => val_inv_det_rec(l_line_cnt).org_id,
                             piv_error_type          => 'ERR_VAL',
                             piv_error_code          => l_err_code,
                             piv_error_message       => l_err_msg,
                             pov_return_status       => l_log_ret_status,
                             pov_error_msg           => l_log_err_msg
                           );
             END IF;
             /**  Added above validation for v1.54 **/

            --only for Brazil OU's Ware house change should pass for remaining it should be null
            val_inv_det_rec(l_line_cnt).warehouse_id := NULL;
            --ver1.28 changes end

          END IF; --IF l_br_ou_name IS NOT NULL THEN

          g_invoice_details(g_line_idx).warehouse_id := val_inv_det_rec(l_line_cnt)
                                                        .warehouse_id;
        EXCEPTION
          WHEN NO_DATA_FOUND THEN
            print_log_message(l_err_msg || val_inv_det_rec(l_line_cnt)
                              .warehouse_id);
            l_err_code   := 'ETN_WAREHOUSE_ID_ERROR';
            l_err_msg    := 'Error : Warehouse not found for Brazil Org ';
            l_valid_flag := 'E';
            g_retcode    := 1;
            log_errors(pin_transaction_id      => val_inv_det_rec(l_line_cnt)
                                                  .interface_txn_id,
                       piv_source_column_name  => 'WAREHOUSE_ID',
                       piv_source_column_value => val_inv_det_rec(l_line_cnt)
                                                  .warehouse_id,
                       piv_error_type          => 'ERR_VAL',
                       piv_error_code          => l_err_code,
                       piv_error_message       => l_err_msg,
                       pov_return_status       => l_log_ret_status,
                       pov_error_msg           => l_log_err_msg);
        END;
        --v1.24 Added by Shailesh Chaudhari for CR 305000 END-------------------------
        -- v1.36 Change Start for CR #371665
        IF (val_inv_det_rec(l_line_cnt).leg_receipt_method_name) IS NOT NULL THEN
          BEGIN
            print_log_message('11i Receipt Method Name: ' || val_inv_det_rec(l_line_cnt)
                              .leg_receipt_method_name); -- added v1.36
            BEGIN
              l_receipt_method_name := NULL;
              SELECT description
                INTO l_receipt_method_name
                FROM fnd_lookup_values
               WHERE lookup_type = 'XXAR_CUST_CNV_PAY_METH_MAP'
                 AND upper(Meaning) =
                     upper(val_inv_det_rec(l_line_cnt)
                           .leg_receipt_method_name)
                 AND enabled_flag = 'Y'
                 AND TRUNC(g_sysdate) BETWEEN
                     NVL(start_date_active, g_sysdate - 1) AND
                     NVL(end_date_active, g_sysdate + 1)
                 AND LANGUAGE = USERENV('LANG')
                 AND ROWNUM = 1;
              val_inv_det_rec(l_line_cnt).receipt_method_name := l_receipt_method_name;
              print_log_message('R12 Receipt Method Name from lookup: ' || val_inv_det_rec(l_line_cnt)
                                .receipt_method_name); -- added v1.36
            EXCEPTION
              WHEN NO_DATA_FOUND THEN
                val_inv_det_rec(l_line_cnt).receipt_method_name := NULL;
                l_err_code := 'ETN_RECEIPT_METHOD_ERROR';
                l_err_msg := 'Error : R12 Receipt Method not found in Lookup XXAR_CUST_CNV_PAY_METH_MAP ';
                l_valid_flag := 'E';
                g_retcode := 1;
                log_errors(pin_transaction_id      => val_inv_det_rec(l_line_cnt)
                                                      .interface_txn_id,
                           piv_source_column_name  => 'RECEIPT_METHOD_NAME',
                           piv_source_column_value => val_inv_det_rec(l_line_cnt)
                                                      .leg_receipt_method_name,
                           piv_error_type          => 'ERR_VAL',
                           piv_error_code          => l_err_code,
                           piv_error_message       => l_err_msg,
                           pov_return_status       => l_log_ret_status,
                           pov_error_msg           => l_log_err_msg);
              WHEN OTHERS THEN
                val_inv_det_rec(l_line_cnt).receipt_method_name := NULL;
                l_err_code := 'ETN_R12_RECEIPT_METHOD_EXCEPTION';
                g_retcode := 2;
                l_err_msg := 'Error : Error validating R12 Receipt Method ' ||
                             SUBSTR(SQLERRM, 1, 150);
                log_errors(pin_transaction_id      => val_inv_det_rec(l_line_cnt)
                                                      .interface_txn_id,
                           piv_source_column_name  => 'Legacy Receipt Method Name',
                           piv_source_column_value => val_inv_det_rec(l_line_cnt)
                                                      .leg_receipt_method_name,
                           piv_error_type          => 'ERR_VAL',
                           piv_error_code          => l_err_code,
                           piv_error_message       => l_err_msg,
                           pov_return_status       => l_log_ret_status,
                           pov_error_msg           => l_log_err_msg);
                --val_inv_det_rec(l_line_cnt).receipt_method_name := l_receipt_method_name;
                print_log_message('R12 Derived Receipt Method Name from lookup: ' || val_inv_det_rec(l_line_cnt)
                                  .receipt_method_name); -- added v1.36
            END;
            BEGIN
            -- v1.59 Added condition to check whether receipt method is attached with correct org_id
             SELECT arm.receipt_method_id
                INTO l_receipt_method_id
                FROM ar_receipt_methods arm,
                 apps.ar_receipt_method_accounts_all arma --v1.59
               WHERE upper(arm.NAME) =
                     upper(val_inv_det_rec(l_line_cnt).receipt_method_name)
                     AND arm.receipt_method_id = arma.receipt_method_id --v1.59
                     AND arma.org_id = upper(val_inv_det_rec(l_line_cnt).org_id) --v1.59
                     AND NVL(arma.end_date, SYSDATE) >= SYSDATE --v1.59
                 AND g_sysdate BETWEEN (NVL(arm.start_date, g_sysdate - 1)) AND
                     (NVL(arma.end_date, g_sysdate + 1));
              val_inv_det_rec(l_line_cnt).receipt_method_id := l_receipt_method_id;
            EXCEPTION
              WHEN NO_DATA_FOUND THEN
                val_inv_det_rec(l_line_cnt).receipt_method_id := NULL;
                l_err_code := 'ETN_RECEIPT_METHOD_ID_ERROR';
                l_err_msg := 'Error : R12 Receipt Method ID not found in ar_receipt_methods table';
                l_valid_flag := 'E';
                g_retcode := 1;
                log_errors(pin_transaction_id      => val_inv_det_rec(l_line_cnt)
                                                      .interface_txn_id,
                           piv_source_column_name  => 'RECEIPT_METHOD_ID',
                           piv_source_column_value => val_inv_det_rec(l_line_cnt)
                                                      .receipt_method_name,
                           piv_error_type          => 'ERR_VAL',
                           piv_error_code          => l_err_code,
                           piv_error_message       => l_err_msg,
                           pov_return_status       => l_log_ret_status,
                           pov_error_msg           => l_log_err_msg);
              WHEN OTHERS THEN
                val_inv_det_rec(l_line_cnt).receipt_method_id := NULL;
                l_err_code := 'ETN_RECEIPT_METHOD_ID_EXCEPTION';
                g_retcode := 2;
                l_err_msg := 'Error : Error validating Receipt Method ID' ||
                             SUBSTR(SQLERRM, 1, 150);
                log_errors(pin_transaction_id      => val_inv_det_rec(l_line_cnt)
                                                      .interface_txn_id,
                           piv_source_column_name  => 'Receipt Method ID',
                           piv_source_column_value => val_inv_det_rec(l_line_cnt)
                                                      .receipt_method_id,
                           piv_error_type          => 'ERR_VAL',
                           piv_error_code          => l_err_code,
                           piv_error_message       => l_err_msg,
                           pov_return_status       => l_log_ret_status,
                           pov_error_msg           => l_log_err_msg);
            END;
            --v1.37 Checking whether Receipt Method is linked with R12 Customer or not
            l_cust_receipt_method := NULL;
            BEGIN
              SELECT acrm.receipt_method_id
                INTO l_cust_receipt_method
                FROM ar_cust_receipt_methods_v acrm,
                     ar_receipt_methods        arm,
                     hz_cust_acct_sites_all    hcas,
                     hz_cust_site_uses_all     hcsu
               WHERE acrm.customer_id = val_inv_det_rec(l_line_cnt)
                    .system_bill_customer_id
                 AND acrm.customer_id = hcas.cust_account_id
                 AND hcsu.cust_acct_site_id = hcas.cust_acct_site_id
                 AND hcas.status = 'A'
                 AND hcsu.status = 'A'
                 AND hcsu.cust_acct_site_id = val_inv_det_rec(l_line_cnt)
                    .system_bill_address_id
                 AND hcsu.site_use_code = 'BILL_TO'
                 AND acrm.site_use_id = hcsu.site_use_id
                 AND arm.receipt_method_id = acrm.receipt_method_id
                 AND arm.receipt_method_id = val_inv_det_rec(l_line_cnt)
                    .receipt_method_id
                 AND NVL(acrm.end_date, g_sysdate) >= g_sysdate
                 AND ROWNUM = 1;

              -- val_inv_det_rec(l_line_cnt).receipt_method_id := l_cust_receipt_method;
            EXCEPTION
              WHEN NO_DATA_FOUND THEN
                -- val_inv_det_rec(l_line_cnt).receipt_method_id := NULL;
                l_cust_receipt_method := NULL;
                l_err_code            := 'ETN_CUST_RECEIPT_METHOD_ID_ERROR';
                l_err_msg             := 'Error : Receipt Method is not attached to the Customer BILL_TO Site';
                -- v1.49 Commented as a part of v1.49 ---------------------------------
              -- l_valid_flag := 'E';
              --                g_retcode := 1;
              /*              log_errors(pin_transaction_id      => val_inv_det_rec(l_line_cnt)
                                                                  .interface_txn_id,
                                       piv_source_column_name  => 'CUST_RECEIPT_METHOD_ID',
                                       piv_source_column_value => val_inv_det_rec(l_line_cnt)
                                                                  .receipt_method_name,
                                       piv_error_type          => 'ERR_VAL',
                                       piv_error_code          => l_err_code,
                                       piv_error_message       => l_err_msg,
                                       pov_return_status       => l_log_ret_status,
                                       pov_error_msg           => l_log_err_msg);
              */
              -- v1.49 Change Ends ----------------------------------------------------------
              WHEN OTHERS THEN
                -- val_inv_det_rec(l_line_cnt).receipt_method_id := NULL;
                l_cust_receipt_method := NULL;
                l_err_code            := 'ETN_CUST_RECEIPT_METHOD_ID_EXCEPTION';
                -- g_retcode := 2;
                l_err_msg := 'Error : Error validating Customer Receipt Method ID' ||
                             SUBSTR(SQLERRM, 1, 150);
                log_errors(pin_transaction_id      => val_inv_det_rec(l_line_cnt)
                                                      .interface_txn_id,
                           piv_source_column_name  => 'Receipt Method ID',
                           piv_source_column_value => val_inv_det_rec(l_line_cnt)
                                                      .receipt_method_id,
                           piv_error_type          => 'ERR_VAL',
                           piv_error_code          => l_err_code,
                           piv_error_message       => l_err_msg,
                           pov_return_status       => l_log_ret_status,
                           pov_error_msg           => l_log_err_msg);
            END;

            --------------------v1.49 Change Starts ------------------------------------
            --            FND_FILE.PUT_LINE(FND_FILE.log,'l_receipt_method_id: '||l_receipt_method_id ||' :l_cust_receipt_method : '||l_cust_receipt_method );

            IF (l_receipt_method_id IS NOT NULL AND
               l_cust_receipt_method IS NULL) THEN

              BEGIN
                SELECT hcsu.site_use_id
                  INTO l_pymt_site_use_id
                  FROM apps.hz_cust_acct_sites_all hcas,
                       apps.hz_cust_site_uses_all  hcsu
                 WHERE hcsu.cust_acct_site_id = hcas.cust_acct_site_id
                   AND hcsu.cust_acct_site_id = val_inv_det_rec(l_line_cnt)
                      .system_bill_address_id
                   AND NVL(hcas.org_id, 1) = val_inv_det_rec(l_line_cnt)
                      .org_id
                   AND hcsu.status = 'A'
                   AND hcas.status = 'A'
                   AND hcsu.site_use_code = 'BILL_TO'
                     --AND hcas.cust_account_id = l_r12_cust_id -- v1.53 : l_r12_cust_id variable is bug
                   AND hcas.cust_account_id = val_inv_det_rec(l_line_cnt).system_bill_customer_id --v1.53
                   ;

                FND_FILE.PUT_LINE(FND_FILE.log,
                                  'Taking site use id to attach receipt method:' ||
                                  l_pymt_site_use_id);

              EXCEPTION
                WHEN NO_DATA_FOUND THEN
                  -- val_inv_det_rec(l_line_cnt).receipt_method_id := NULL;
                  l_pymt_site_use_id := NULL;
                  l_err_code         := 'ETN_AR_PROCEDURE_EXCEPTION';
                  l_err_msg          := 'Error : Customer Site Id is not defined in R12'||
                                        SUBSTR(SQLERRM, 1, 150); --v1.53 changed error message
                  l_valid_flag       := 'E'; --
                  g_retcode          := 1;
                  log_errors(pin_transaction_id      => val_inv_det_rec(l_line_cnt)
                                                        .interface_txn_id,
                             piv_source_column_name  => 'CUST_RECEIPT_METHOD_ID',
                             piv_source_column_value => val_inv_det_rec(l_line_cnt)
                                                        .receipt_method_name,
                             piv_error_type          => 'ERR_VAL',
                             piv_error_code          => l_err_code,
                             piv_error_message       => l_err_msg,
                             pov_return_status       => l_log_ret_status,
                             pov_error_msg           => l_log_err_msg);
                WHEN OTHERS THEN
                  -- val_inv_det_rec(l_line_cnt).receipt_method_id := NULL;
                  l_pymt_site_use_id := NULL;
                  l_err_code         := 'ETN_AR_PROCEDURE_EXCEPTION';
                  g_retcode          := 2;
                  l_valid_flag       := 'E';
                  l_err_msg          := 'Error : Customer Site ID is not defined in R12' ||
                                        SUBSTR(SQLERRM, 1, 150); --v1.53 changed error message
                  log_errors(pin_transaction_id      => val_inv_det_rec(l_line_cnt)
                                                        .interface_txn_id,
                             piv_source_column_name  => 'Receipt Method ID',
                             piv_source_column_value => val_inv_det_rec(l_line_cnt)
                                                        .receipt_method_id,
                             piv_error_type          => 'ERR_VAL',
                             piv_error_code          => l_err_code,
                             piv_error_message       => l_err_msg,
                             pov_return_status       => l_log_ret_status,
                             pov_error_msg           => l_log_err_msg);

              END;

              BEGIN
                l_pay_method_rec.cust_account_id   := val_inv_det_rec(l_line_cnt)
                                                      .system_bill_customer_id;
                l_pay_method_rec.receipt_method_id := l_receipt_method_id;
                l_pay_method_rec.primary_flag      := 'Y';
                l_pay_method_rec.site_use_id       := l_pymt_site_use_id;
                l_pay_method_rec.start_date        := '01-JAN-2001';
                l_pay_method_rec.end_date          := NULL;

                hz_payment_method_pub.create_payment_method(p_init_msg_list          => fnd_api.g_true,
                                                            p_payment_method_rec     => l_pay_method_rec,
                                                            x_cust_receipt_method_id => l_cust_receipt_method_id,
                                                            x_return_status          => l_return_status,
                                                            x_msg_count              => l_msg_count,
                                                            x_msg_data               => l_msg_data);

                IF l_return_status = 'S' THEN
                  print_log1_message('Attaching the receipt mthod at site level successfully');
                ELSE
                  print_log1_message('ERROR : Attaching the receipt mthod at site level not successfully' ||
                                     l_msg_data);

                  IF l_msg_count > 0 THEN
                    FOR i IN 1 .. l_msg_count LOOP
                      l_msg := fnd_msg_pub.get(p_msg_index => i,
                                               p_encoded   => fnd_api.g_false);
                      print_log1_message('ERROR : Attaching the receipt method at site level not successfully' ||
                                         l_msg_count || l_msg_data);
                    END LOOP;
                  END IF;
                END IF;

              EXCEPTION
                WHEN NO_DATA_FOUND THEN
                  val_inv_det_rec(l_line_cnt).receipt_method_id := NULL;
                  l_err_code := 'ETN_AR_PROCEDURE_EXECPTION';
                  l_err_msg := 'Error : Receipt Method is not setup in R12 and Receipt Method is not attached to the Customer BILL_TO Site';
                  l_valid_flag := 'E';
                  g_retcode := 1;
                  log_errors(pin_transaction_id      => val_inv_det_rec(l_line_cnt)
                                                        .interface_txn_id,
                             piv_source_column_name  => 'Receipt Method ID',
                             piv_source_column_value => val_inv_det_rec(l_line_cnt)
                                                        .receipt_method_name,
                             piv_error_type          => 'ERR_VAL',
                             piv_error_code          => l_err_code,
                             piv_error_message       => l_err_msg,
                             pov_return_status       => l_log_ret_status,
                             pov_error_msg           => l_log_err_msg);
                WHEN OTHERS THEN
                  val_inv_det_rec(l_line_cnt).receipt_method_id := NULL;
                  l_err_code := 'ETN_AR_PROCEDURE_EXECPTION';
                  g_retcode := 2;
                  l_valid_flag := 'E';
                  l_err_msg := 'Error : Receipt Method is not setup in R12 and Receipt Method is not attached to the Customer BILL_TO Site' ||
                               SUBSTR(SQLERRM, 1, 150);
                  log_errors(pin_transaction_id      => val_inv_det_rec(l_line_cnt)
                                                        .interface_txn_id,
                             piv_source_column_name  => 'Receipt Method ID',
                             piv_source_column_value => val_inv_det_rec(l_line_cnt)
                                                        .receipt_method_id,
                             piv_error_type          => 'ERR_VAL',
                             piv_error_code          => l_err_code,
                             piv_error_message       => l_err_msg,
                             pov_return_status       => l_log_ret_status,
                             pov_error_msg           => l_log_err_msg);

              END;
            ELSE
              print_log1_message('Receipt method at site level already defined');
            END IF; -- end of IF l_receipt_method_id IS NOT NULL AND l_cust_receipt_method IS NULL
            --v1.49 Change Ends --------------------------------------

            g_invoice_details(g_line_idx).receipt_method_id := val_inv_det_rec(l_line_cnt)
                                                               .receipt_method_id;
            g_invoice_details(g_line_idx).receipt_method_name := val_inv_det_rec(l_line_cnt)
                                                                 .receipt_method_name;
          EXCEPTION
            WHEN NO_DATA_FOUND THEN
              print_log_message(l_err_msg || val_inv_det_rec(l_line_cnt)
                                .receipt_method_id);
              l_err_code   := 'ETN_RECEIPT_METHOD_ERROR';
              l_err_msg    := 'Error : R12 Receipt Method not found ';
              l_valid_flag := 'E';
              g_retcode    := 1;
              log_errors(pin_transaction_id      => val_inv_det_rec(l_line_cnt)
                                                    .interface_txn_id,
                         piv_source_column_name  => 'RECEIPT_METHOD',
                         piv_source_column_value => val_inv_det_rec(l_line_cnt)
                                                    .receipt_method_id,
                         piv_error_type          => 'ERR_VAL',
                         piv_error_code          => l_err_code,
                         piv_error_message       => l_err_msg,
                         pov_return_status       => l_log_ret_status,
                         pov_error_msg           => l_log_err_msg);
          END;
        ELSE
          g_invoice_details(g_line_idx).receipt_method_id := NULL;
          g_invoice_details(g_line_idx).receipt_method_name := NULL;
        END IF;
        --v1.38 change start
        --   FND_FILE.PUT_LINE(FND_FILE.log,'l_valid_flag: '||l_valid_flag );
        IF NVL(l_valid_flag, 'Y') = 'Y' THEN
          g_invoice_details(g_line_idx).process_flag := 'V';
        ELSE
          g_invoice_details(g_line_idx).process_flag := 'E';
          g_invoice_details(g_line_idx).ERROR_TYPE := 'ERR_VAL';
          g_retcode := 1;
        END IF;
        --v1.38 change ends
        -- v1.36 Change end for CR#371665
        g_line_idx := g_line_idx + 1;
      END LOOP;
    END LOOP;
    CLOSE val_inv_det_cur;
    val_inv_det_rec.delete; -- v1.30
    IF g_invoice_details.EXISTS(1) THEN
      FORALL l_indx IN 1 .. g_invoice_details.COUNT
      -- LOOP
        UPDATE xxar_invoices_stg
           SET line_number     = g_invoice_details(l_indx).line_number,
               line_type       = g_invoice_details(l_indx).line_type,
               description     = g_invoice_details(l_indx).description,
               conversion_type = g_invoice_details(l_indx).conversion_type,
               conversion_date = g_invoice_details(l_indx).conversion_date,
               conversion_rate = g_invoice_details(l_indx).conversion_rate,
               line_amount     = g_invoice_details(l_indx).line_amount,
               --                   process_flag = g_invoice_details (l_indx).process_flag,
               process_flag              = DECODE(process_flag,
                                                  'E',
                                                  'E',
                                                  g_invoice_details(l_indx)
                                                  .process_flag),
               ERROR_TYPE                = g_invoice_details(l_indx)
                                           .ERROR_TYPE,
               reason_code               = g_invoice_details(l_indx)
                                           .reason_code,
               comments                  = g_invoice_details(l_indx).comments,
               reference_line_id         = g_invoice_details(l_indx)
                                           .reference_line_id,
               memo_line_id              = g_invoice_details(l_indx)
                                           .memo_line_id,
               attribute_category        = g_invoice_details(l_indx)
                                           .attribute_category,
               attribute1                = g_invoice_details(l_indx)
                                           .attribute1,
               attribute2                = g_invoice_details(l_indx)
                                           .attribute2,
               attribute3                = g_invoice_details(l_indx)
                                           .attribute3,
               attribute4                = g_invoice_details(l_indx)
                                           .attribute4,
               attribute5                = g_invoice_details(l_indx)
                                           .attribute5,
               attribute6                = g_invoice_details(l_indx)
                                           .attribute6,
               attribute7                = g_invoice_details(l_indx)
                                           .attribute7,
               attribute8                = g_invoice_details(l_indx)
                                           .attribute8,
               attribute9                = g_invoice_details(l_indx)
                                           .attribute9,
               attribute10               = g_invoice_details(l_indx)
                                           .attribute10,
               attribute11               = g_invoice_details(l_indx)
                                           .attribute11,
               attribute12               = g_invoice_details(l_indx)
                                           .attribute12,
               attribute13               = g_invoice_details(l_indx)
                                           .attribute13,
               attribute14               = g_invoice_details(l_indx)
                                           .attribute14,
               attribute15               = g_invoice_details(l_indx)
                                           .attribute15,
               interface_line_context    = g_invoice_details(l_indx)
                                           .interface_line_context,
               interface_line_attribute1 = g_invoice_details(l_indx)
                                           .interface_line_attribute1,
               /*--Ver 1.5 Commented for DFF rationalization start
               interface_line_attribute2 = g_invoice_details (l_indx).interface_line_attribute2,
                           interface_line_attribute3 = g_invoice_details (l_indx).interface_line_attribute3,
                           interface_line_attribute4 = g_invoice_details (l_indx).interface_line_attribute4,
                           interface_line_attribute5 = g_invoice_details (l_indx).interface_line_attribute5,
                           interface_line_attribute6 = g_invoice_details (l_indx).interface_line_attribute6,
                           interface_line_attribute7 = g_invoice_details (l_indx).interface_line_attribute7,
                           interface_line_attribute8 = g_invoice_details (l_indx).interface_line_attribute8,
                           interface_line_attribute9 = g_invoice_details (l_indx).interface_line_attribute9,
                           interface_line_attribute10 =
                                                       g_invoice_details (l_indx).interface_line_attribute10,
                           interface_line_attribute11 =
                                                       g_invoice_details (l_indx).interface_line_attribute11,
                           interface_line_attribute12 =
                                                       g_invoice_details (l_indx).interface_line_attribute12,
                           interface_line_attribute13 =
                                                       g_invoice_details (l_indx).interface_line_attribute13,
                           interface_line_attribute14 =
                                                       g_invoice_details (l_indx).interface_line_attribute14,
               */ --Ver 1.5 Commented for DFF rationalization end
               interface_line_attribute15 = g_invoice_details(l_indx)
                                            .interface_line_attribute15,
               link_to_line_context       = g_invoice_details(l_indx)
                                            .link_to_line_context,
               link_to_line_attribute1    = g_invoice_details(l_indx)
                                            .link_to_line_attribute1,
               --Ver1.5 starts
               /*
               link_to_line_attribute2 = g_invoice_details (l_indx).link_to_line_attribute2,
                           link_to_line_attribute3 = g_invoice_details (l_indx).link_to_line_attribute3,
                           link_to_line_attribute4 = g_invoice_details (l_indx).link_to_line_attribute4,
                           link_to_line_attribute5 = g_invoice_details (l_indx).link_to_line_attribute5,
                           link_to_line_attribute6 = g_invoice_details (l_indx).link_to_line_attribute6,
                           link_to_line_attribute7 = g_invoice_details (l_indx).link_to_line_attribute7,
                           link_to_line_attribute8 = g_invoice_details (l_indx).link_to_line_attribute8,
                           link_to_line_attribute9 = g_invoice_details (l_indx).link_to_line_attribute9,
                           link_to_line_attribute10 = g_invoice_details (l_indx).link_to_line_attribute10,
                           link_to_line_attribute11 = g_invoice_details (l_indx).link_to_line_attribute11,
                           link_to_line_attribute12 = g_invoice_details (l_indx).link_to_line_attribute12,
                           link_to_line_attribute13 = g_invoice_details (l_indx).link_to_line_attribute13,
                           link_to_line_attribute14 = g_invoice_details (l_indx).link_to_line_attribute14,

               */
               header_attribute_category = g_invoice_details(l_indx)
                                           .header_attribute_category,
               header_attribute1         = g_invoice_details(l_indx)
                                           .header_attribute1,
               header_attribute3         = g_invoice_details(l_indx)
                                           .header_attribute3 -- added for v1.43
              ,
               header_attribute8         = g_invoice_details(l_indx)
                                           .header_attribute8,
               header_attribute4         = g_invoice_details(l_indx)
                                           .header_attribute4
               --Ver1.5 ends
               --ver1.23 changes start
              ,
               header_attribute13 = g_invoice_details(l_indx)
                                    .header_attribute13
               --ver1.23 changes end
               --ver1.25 changes starts
              ,
               header_attribute14 = g_invoice_details(l_indx)
                                    .header_attribute14
               --ver1.25 changes ends
              ,
               link_to_line_attribute15 = g_invoice_details(l_indx)
                                          .link_to_line_attribute15,
               header_gdf_attr_category = g_invoice_details(l_indx)
                                          .header_gdf_attr_category,
               header_gdf_attribute1    = g_invoice_details(l_indx)
                                          .header_gdf_attribute1,
               header_gdf_attribute2    = g_invoice_details(l_indx)
                                          .header_gdf_attribute2,
               header_gdf_attribute3    = g_invoice_details(l_indx)
                                          .header_gdf_attribute3,
               header_gdf_attribute4    = g_invoice_details(l_indx)
                                          .header_gdf_attribute4,
               header_gdf_attribute5    = g_invoice_details(l_indx)
                                          .header_gdf_attribute5,
               header_gdf_attribute6    = g_invoice_details(l_indx)
                                          .header_gdf_attribute6,
               header_gdf_attribute7    = g_invoice_details(l_indx)
                                          .header_gdf_attribute7,
               header_gdf_attribute8    = g_invoice_details(l_indx)
                                          .header_gdf_attribute8,
               header_gdf_attribute9    = g_invoice_details(l_indx)
                                          .header_gdf_attribute9,
               header_gdf_attribute10   = g_invoice_details(l_indx)
                                          .header_gdf_attribute10,
               header_gdf_attribute11   = g_invoice_details(l_indx)
                                          .header_gdf_attribute11,
               header_gdf_attribute12   = g_invoice_details(l_indx)
                                          .header_gdf_attribute12,
               header_gdf_attribute13   = g_invoice_details(l_indx)
                                          .header_gdf_attribute13,
               header_gdf_attribute14   = g_invoice_details(l_indx)
                                          .header_gdf_attribute14,
               header_gdf_attribute15   = g_invoice_details(l_indx)
                                          .header_gdf_attribute15,
               header_gdf_attribute16   = g_invoice_details(l_indx)
                                          .header_gdf_attribute16,
               header_gdf_attribute17   = g_invoice_details(l_indx)
                                          .header_gdf_attribute17,
               header_gdf_attribute18   = g_invoice_details(l_indx)
                                          .header_gdf_attribute18,
               header_gdf_attribute19   = g_invoice_details(l_indx)
                                          .header_gdf_attribute19,
               header_gdf_attribute20   = g_invoice_details(l_indx)
                                          .header_gdf_attribute20,
               header_gdf_attribute21   = g_invoice_details(l_indx)
                                          .header_gdf_attribute21,
               header_gdf_attribute22   = g_invoice_details(l_indx)
                                          .header_gdf_attribute22,
               header_gdf_attribute23   = g_invoice_details(l_indx)
                                          .header_gdf_attribute23,
               header_gdf_attribute24   = g_invoice_details(l_indx)
                                          .header_gdf_attribute24,
               header_gdf_attribute25   = g_invoice_details(l_indx)
                                          .header_gdf_attribute25,
               header_gdf_attribute26   = g_invoice_details(l_indx)
                                          .header_gdf_attribute26,
               header_gdf_attribute27   = g_invoice_details(l_indx)
                                          .header_gdf_attribute27,
               header_gdf_attribute28   = g_invoice_details(l_indx)
                                          .header_gdf_attribute28,
               header_gdf_attribute29   = g_invoice_details(l_indx)
                                          .header_gdf_attribute29,
               header_gdf_attribute30   = g_invoice_details(l_indx)
                                          .header_gdf_attribute30,
               line_gdf_attr_category   = g_invoice_details(l_indx)
                                          .line_gdf_attr_category,
               line_gdf_attribute1      = g_invoice_details(l_indx)
                                          .line_gdf_attribute1,
               line_gdf_attribute2      = g_invoice_details(l_indx)
                                          .line_gdf_attribute2,
               line_gdf_attribute3      = g_invoice_details(l_indx)
                                          .line_gdf_attribute3,
               line_gdf_attribute4      = g_invoice_details(l_indx)
                                          .line_gdf_attribute4,
               line_gdf_attribute5      = g_invoice_details(l_indx)
                                          .line_gdf_attribute5,
               line_gdf_attribute6      = g_invoice_details(l_indx)
                                          .line_gdf_attribute6,
               line_gdf_attribute7      = g_invoice_details(l_indx)
                                          .line_gdf_attribute7,
               line_gdf_attribute8      = g_invoice_details(l_indx)
                                          .line_gdf_attribute8,
               line_gdf_attribute9      = g_invoice_details(l_indx)
                                          .line_gdf_attribute9,
               line_gdf_attribute10     = g_invoice_details(l_indx)
                                          .line_gdf_attribute10,
               line_gdf_attribute11     = g_invoice_details(l_indx)
                                          .line_gdf_attribute11,
               line_gdf_attribute12     = g_invoice_details(l_indx)
                                          .line_gdf_attribute12,
               line_gdf_attribute13     = g_invoice_details(l_indx)
                                          .line_gdf_attribute13,
               line_gdf_attribute14     = g_invoice_details(l_indx)
                                          .line_gdf_attribute14,
               line_gdf_attribute15     = g_invoice_details(l_indx)
                                          .line_gdf_attribute15,
               line_gdf_attribute16     = g_invoice_details(l_indx)
                                          .line_gdf_attribute16,
               line_gdf_attribute17     = g_invoice_details(l_indx)
                                          .line_gdf_attribute17,
               line_gdf_attribute18     = g_invoice_details(l_indx)
                                          .line_gdf_attribute18,
               line_gdf_attribute19     = g_invoice_details(l_indx)
                                          .line_gdf_attribute19,
               line_gdf_attribute20     = g_invoice_details(l_indx)
                                          .line_gdf_attribute20,
               --tax_code = g_invoice_details (l_indx).tax_code,
               --tax_regime_code = g_invoice_details (l_indx).tax_regime_code,
               --tax_rate_code = g_invoice_details (l_indx).tax_rate_code,
               --tax = g_invoice_details (l_indx).tax,
               --tax_status_code = g_invoice_details (l_indx).tax_status_code,
               --tax_jurisdiction_code = g_invoice_details (l_indx).tax_jurisdiction_code,
               --v1.3 FOT issue fix start
               tax_rate = g_invoice_details(l_indx).tax_rate,
               --v1.3 FOT issue fix end
               amount_includes_tax_flag       = g_invoice_details(l_indx)
                                                .amount_includes_tax_flag,
               taxable_flag                   = g_invoice_details(l_indx)
                                                .taxable_flag,
               sales_order_date               = g_invoice_details(l_indx)
                                                .sales_order_date,
               sales_order                    = g_invoice_details(l_indx)
                                                .sales_order,
               uom_name                       = g_invoice_details(l_indx)
                                                .uom_name,
               ussgl_transaction_code_context = g_invoice_details(l_indx)
                                                .ussgl_transaction_code_context,
               internal_notes                 = g_invoice_details(l_indx)
                                                .internal_notes,
               ship_date_actual               = g_invoice_details(l_indx)
                                                .ship_date_actual,
               fob_point                      = g_invoice_details(l_indx)
                                                .fob_point,
               ship_via                       = g_invoice_details(l_indx)
                                                .ship_via,
               waybill_number                 = g_invoice_details(l_indx)
                                                .waybill_number,
               sales_order_line               = g_invoice_details(l_indx)
                                                .sales_order_line,
               sales_order_source             = g_invoice_details(l_indx)
                                                .sales_order_source,
               sales_order_revision           = g_invoice_details(l_indx)
                                                .sales_order_revision,
               purchase_order_revision        = g_invoice_details(l_indx)
                                                .purchase_order_revision,
               purchase_order                 = g_invoice_details(l_indx)
                                                .purchase_order,
               --Ver1.7 changes start
               /*

               agreement_name = g_invoice_details (l_indx).agreement_name,

               agreement_id = g_invoice_details (l_indx).agreement_id,

               */
               purchase_order_date = g_invoice_details(l_indx)
                                     .purchase_order_date,
               --accounting_rule_id  = g_invoice_details (l_indx).agreement_id,
               --rule_start_date = g_invoice_details (l_indx).purchase_order_date,
               --invoicing_rule_id = g_invoice_details (l_indx).invoicing_rule_id,
               --Ver1.7 changes end
               quantity            = g_invoice_details(l_indx).quantity,
               quantity_ordered    = g_invoice_details(l_indx)
                                     .quantity_ordered,
               unit_selling_price  = g_invoice_details(l_indx)
                                     .unit_selling_price,
               unit_standard_price = g_invoice_details(l_indx)
                                     .unit_standard_price,
               last_update_date    = g_sysdate,
               last_updated_by     = g_last_updated_by,
               last_update_login   = g_login_id
               --v1.24Added by Shailesh Chaudhari for CR 305000-Warehouse ID START-------------------------
              ,
               warehouse_id = g_invoice_details(l_indx).warehouse_id
               --v1.24Added by Shailesh Chaudhari for CR 305000-Warehouse ID END-------------------------
               --v1.36 For CR #371665 ----
              ,
               receipt_method_id   = g_invoice_details(l_indx)
                                     .receipt_method_id,
               receipt_method_name = g_invoice_details(l_indx)
                                     .receipt_method_name
        -- v1.36 Change ends--
         WHERE interface_txn_id = g_invoice_details(l_indx)
              .interface_txn_id;
      --END LOOP;
    END IF;
    g_invoice_details.delete; -- v1.30
    FOR r_invline_err_rec IN (SELECT DISTINCT xis.leg_customer_trx_id,
                                              xis.leg_trx_number
                                FROM xxar_invoices_stg xis
                               WHERE xis.process_flag = 'E'
                                 AND xis.batch_id = g_new_batch_id
                                 AND xis.run_sequence_id = g_new_run_seq_id) LOOP
      UPDATE /*+ INDEX (xis XXAR_INVOICES_STG_N4) */ xxar_invoices_stg xis
         SET process_flag      = 'E',
             ERROR_TYPE        = 'ERR_VAL',
             last_update_date  = g_sysdate,
             last_updated_by   = g_last_updated_by,
             last_update_login = g_login_id
       WHERE leg_customer_trx_id = r_invline_err_rec.leg_customer_trx_id
         AND batch_id = g_new_batch_id
         AND run_sequence_id = g_new_run_seq_id;
      l_err_code := 'ETN_INVOICE_ERROR';
      l_err_msg  := 'Error : Erroring out remaining lines since one of the lines is in error';
      print_log_message('For legacy transaction number: ' ||
                        r_invline_err_rec.leg_trx_number);
      print_log_message(l_err_msg);
      log_errors(
                 --   pin_transaction_id           =>  r_dist_err_rec.interface_txn_id
                 piv_error_type          => 'ERR_VAL',
                 piv_source_column_name  => 'LEGACY_CUSTOMER_TRX_ID',
                 piv_source_column_value => r_invline_err_rec.leg_customer_trx_id,
                 piv_source_keyname1     => 'LEGACY_TRX_NUMBER',
                 piv_source_keyvalue1    => r_invline_err_rec.leg_trx_number,
                 piv_error_code          => l_err_code,
                 piv_error_message       => l_err_msg,
                 pov_return_status       => l_log_ret_status,
                 pov_error_msg           => l_log_err_msg);
    END LOOP;
    COMMIT;
    l_dist_cust_trx_id := -1;
    OPEN val_dist_cur;
    LOOP
      FETCH val_dist_cur BULK COLLECT
        INTO val_dist_rec LIMIT l_dist_limit;
      EXIT WHEN val_dist_rec.COUNT = 0;
      FOR l_dist_cnt IN 1 .. val_dist_rec.COUNT LOOP
        l_valid_flag := 'Y'; -- added for v1.34
        --    l_dist_flag := 'N';
        --(val_inv_det_rec(l_line_cnt).leg_customer_trx_id, val_inv_det_rec(l_line_cnt).leg_cust_trx_line_id, val_inv_det_rec(l_line_cnt).leg_operating_unit)
        --              l_dist_flag := 'Y';
        x_out_acc_rec := NULL;
        x_ccid        := NULL;
        IF l_dist_cust_trx_id <> val_dist_rec(l_dist_cnt)
          .leg_customer_trx_id THEN
          l_rec_flag                 := 'N';
          l_rev_flag                 := 'N';
          l_assign_flag              := 'N';
          l_rec_dist_idx             := NULL;
          l_rec_int_line_attribute1  := NULL;
          l_rec_int_line_attribute2  := NULL;
          l_rec_int_line_attribute3  := NULL;
          l_rec_int_line_attribute4  := NULL;
          l_rec_int_line_attribute5  := NULL;
          l_rec_int_line_attribute6  := NULL;
          l_rec_int_line_attribute7  := NULL;
          l_rec_int_line_attribute8  := NULL;
          l_rec_int_line_attribute9  := NULL;
          l_rec_int_line_attribute10 := NULL;
          l_rec_int_line_attribute11 := NULL;
          l_rec_int_line_attribute12 := NULL;
          l_rec_int_line_attribute13 := NULL;
          l_rec_int_line_attribute14 := NULL;
          l_rec_int_line_attribute15 := NULL;
        END IF;

        /** Commented below for v1.48, Defect# 9239 **/

        /** IF val_dist_rec(l_dist_cnt)
         .leg_cust_trx_type_name LIKE '%OKS%'
            AND val_dist_rec(l_dist_cnt)
           .leg_account_class NOT IN ('REV', 'REC', 'TAX')
        THEN
           print_log_message('For legacy transaction number: ' || val_dist_rec(l_dist_cnt)
                             .leg_trx_number ||
                             ' distribution entry ignored since it is of class ' || val_dist_rec(l_dist_cnt)
                             .leg_account_class ||
                             ' and not REC or REV');
        ELSE **/

        /** Commented above for v1.48, Defect# 9239 **/

        IF check_mandatory(pin_trx_id       => val_dist_rec(l_dist_cnt)
                                               .interface_txn_id,
                           piv_column_value => val_dist_rec(l_dist_cnt)
                                               .leg_dist_segment1,
                           piv_column_name  => 'Distribution segment1',
                           piv_table_name   => 'XXAR_INVOICES_DIST_STG') THEN
          l_valid_flag := 'E';
        END IF;
        -- Verify Distribution segment2 is NOT NULL
        IF check_mandatory(pin_trx_id       => val_dist_rec(l_dist_cnt)
                                               .interface_txn_id,
                           piv_column_value => val_dist_rec(l_dist_cnt)
                                               .leg_dist_segment2,
                           piv_column_name  => 'Distribution segment2',
                           piv_table_name   => 'XXAR_INVOICES_DIST_STG') THEN
          l_valid_flag := 'E';
        END IF;
        -- Verify Distribution segment3 is NOT NULL
        IF check_mandatory(pin_trx_id       => val_dist_rec(l_dist_cnt)
                                               .interface_txn_id,
                           piv_column_value => val_dist_rec(l_dist_cnt)
                                               .leg_dist_segment3,
                           piv_column_name  => 'Distribution segment3',
                           piv_table_name   => 'XXAR_INVOICES_DIST_STG') THEN
          l_valid_flag := 'E';
        END IF;
        -- Verify Distribution segment4 is NOT NULL
        IF check_mandatory(pin_trx_id       => val_dist_rec(l_dist_cnt)
                                               .interface_txn_id,
                           piv_column_value => val_dist_rec(l_dist_cnt)
                                               .leg_dist_segment4,
                           piv_column_name  => 'Distribution segment4',
                           piv_table_name   => 'XXAR_INVOICES_DIST_STG') THEN
          l_valid_flag := 'E';
        END IF;
        -- Verify Distribution segment5 is NOT NULL
        IF check_mandatory(pin_trx_id       => val_dist_rec(l_dist_cnt)
                                               .interface_txn_id,
                           piv_column_value => val_dist_rec(l_dist_cnt)
                                               .leg_dist_segment5,
                           piv_column_name  => 'Distribution segment5',
                           piv_table_name   => 'XXAR_INVOICES_DIST_STG') THEN
          l_valid_flag := 'E';
        END IF;
        -- Verify Distribution segment6 is NOT NULL
        IF check_mandatory(pin_trx_id       => val_dist_rec(l_dist_cnt)
                                               .interface_txn_id,
                           piv_column_value => val_dist_rec(l_dist_cnt)
                                               .leg_dist_segment6,
                           piv_column_name  => 'Distribution segment6',
                           piv_table_name   => 'XXAR_INVOICES_DIST_STG') THEN
          l_valid_flag := 'E';
        END IF;
        -- Verify Distribution segment7 is NOT NULL
        IF check_mandatory(pin_trx_id       => val_dist_rec(l_dist_cnt)
                                               .interface_txn_id,
                           piv_column_value => val_dist_rec(l_dist_cnt)
                                               .leg_dist_segment7,
                           piv_column_name  => 'Distribution segment7',
                           piv_table_name   => 'XXAR_INVOICES_DIST_STG') THEN
          l_valid_flag := 'E';
        END IF;
        IF val_dist_rec(l_dist_cnt).leg_account_class NOT IN ('REC') THEN
          IF UPPER(val_dist_rec(l_dist_cnt).leg_org_name) <>
             UPPER(val_dist_rec(l_dist_cnt).leg_operating_unit) THEN
            l_valid_flag := 'E';
          ELSE
            g_invoice_dist(g_dist_idx).org_id := val_dist_rec(l_dist_cnt)
                                                 .org_id;
          END IF;
        END IF;
        validate_accounts(val_dist_rec(l_dist_cnt).interface_txn_id,
                          val_dist_rec(l_dist_cnt).leg_dist_segment1,
                          val_dist_rec(l_dist_cnt).leg_dist_segment2,
                          val_dist_rec(l_dist_cnt).leg_dist_segment3,
                          val_dist_rec(l_dist_cnt).leg_dist_segment4,
                          val_dist_rec(l_dist_cnt).leg_dist_segment5,
                          val_dist_rec(l_dist_cnt).leg_dist_segment6,
                          val_dist_rec(l_dist_cnt).leg_dist_segment7,
                          --ver1.26 changes start
                          val_dist_rec(l_dist_cnt).leg_operating_unit,
                          val_dist_rec(l_dist_cnt)
                          .leg_interface_hdr_attribute1,
                          val_dist_rec(l_dist_cnt).customer_type,
                          val_dist_rec(l_dist_cnt).system_bill_customer_id, -- v1.42
                          --ver1.26 changes end
                          x_out_acc_rec,
                          x_ccid);
        IF x_ccid IS NULL THEN
          l_valid_flag := 'E';
        END IF;
        g_invoice_dist(g_dist_idx).dist_segment1 := x_out_acc_rec.segment1;
        g_invoice_dist(g_dist_idx).dist_segment2 := x_out_acc_rec.segment2;
        g_invoice_dist(g_dist_idx).dist_segment3 := x_out_acc_rec.segment3;
        g_invoice_dist(g_dist_idx).dist_segment4 := x_out_acc_rec.segment4;
        g_invoice_dist(g_dist_idx).dist_segment5 := x_out_acc_rec.segment5;
        g_invoice_dist(g_dist_idx).dist_segment6 := x_out_acc_rec.segment6;
        g_invoice_dist(g_dist_idx).dist_segment7 := x_out_acc_rec.segment7;
        g_invoice_dist(g_dist_idx).dist_segment8 := x_out_acc_rec.segment8;
        g_invoice_dist(g_dist_idx).dist_segment9 := x_out_acc_rec.segment9;
        g_invoice_dist(g_dist_idx).dist_segment10 := x_out_acc_rec.segment10;
        g_invoice_dist(g_dist_idx).code_combination_id := x_ccid;
        g_invoice_dist(g_dist_idx).interface_line_context := g_interface_line_context;
        g_invoice_dist(g_dist_idx).interface_line_attribute1 := val_dist_rec(l_dist_cnt)
                                                                .interface_line_attribute1;
        /*--Ver 1.5 Commented for DFF rationalization start
                 g_invoice_dist (g_dist_idx).interface_line_attribute2 :=
                                                         val_dist_rec (l_dist_cnt).interface_line_attribute2;
                       g_invoice_dist (g_dist_idx).interface_line_attribute3 :=
                                                         val_dist_rec (l_dist_cnt).interface_line_attribute3;
                       g_invoice_dist (g_dist_idx).interface_line_attribute4 :=
                                                         val_dist_rec (l_dist_cnt).interface_line_attribute4;
                       g_invoice_dist (g_dist_idx).interface_line_attribute5 :=
                                                         val_dist_rec (l_dist_cnt).interface_line_attribute5;
                       g_invoice_dist (g_dist_idx).interface_line_attribute6 :=
                                                         val_dist_rec (l_dist_cnt).interface_line_attribute6;
                       g_invoice_dist (g_dist_idx).interface_line_attribute7 :=
                                                         val_dist_rec (l_dist_cnt).interface_line_attribute7;
                       g_invoice_dist (g_dist_idx).interface_line_attribute8 :=
                                                         val_dist_rec (l_dist_cnt).interface_line_attribute8;
                       g_invoice_dist (g_dist_idx).interface_line_attribute9 :=
                                                         val_dist_rec (l_dist_cnt).interface_line_attribute9;
                       g_invoice_dist (g_dist_idx).interface_line_attribute10 :=
                                                        val_dist_rec (l_dist_cnt).interface_line_attribute10;
                       g_invoice_dist (g_dist_idx).interface_line_attribute11 :=
                                                        val_dist_rec (l_dist_cnt).interface_line_attribute11;
                       g_invoice_dist (g_dist_idx).interface_line_attribute12 :=
                                                        val_dist_rec (l_dist_cnt).interface_line_attribute12;
                       g_invoice_dist (g_dist_idx).interface_line_attribute13 :=
                                                        val_dist_rec (l_dist_cnt).interface_line_attribute13;
                       g_invoice_dist (g_dist_idx).interface_line_attribute14 :=
                                                        val_dist_rec (l_dist_cnt).interface_line_attribute14;
        */ --Ver 1.5 Commented for DFF rationalization end
        g_invoice_dist(g_dist_idx).interface_line_attribute15 := val_dist_rec(l_dist_cnt)
                                                                 .interface_line_attribute15;
        --            g_invoice_dist(g_dist_idx).interface_line_attribute14 := val_dist_rec(l_dist_cnt).leg_interface_line_attribute14;
        --            g_invoice_dist(g_dist_idx).interface_line_attribute15 := NVL(val_dist_rec(l_dist_cnt).leg_interface_line_attribute15, g_int_line_att);
        --g_invoice_dist (g_dist_idx).interface_line_attribute14 := val_dist_rec(l_dist_cnt).leg_customer_trx_id;
        --g_invoice_dist (g_dist_idx).interface_line_attribute15 := val_dist_rec(l_dist_cnt).leg_cust_trx_line_id;
        g_invoice_dist(g_dist_idx).accounted_amount := val_dist_rec(l_dist_cnt)
                                                       .leg_accounted_amount;
        g_invoice_dist(g_dist_idx).interface_txn_id := val_dist_rec(l_dist_cnt)
                                                       .interface_txn_id;
        g_invoice_dist(g_dist_idx).PERCENT := val_dist_rec(l_dist_cnt)
                                              .leg_percent;
        IF val_dist_rec(l_dist_cnt).leg_account_class = 'FREIGHT' THEN
          g_invoice_dist(g_dist_idx).account_class := 'REV';
        ELSE
          g_invoice_dist(g_dist_idx).account_class := val_dist_rec(l_dist_cnt)
                                                      .leg_account_class;
        END IF;
        IF l_valid_flag = 'E' THEN
          g_invoice_dist(g_dist_idx).process_flag := 'E';
          g_invoice_dist(g_dist_idx).ERROR_TYPE := 'ERR_VAL';
          g_retcode := 1;
        ELSE
          g_invoice_dist(g_dist_idx).process_flag := 'V';
        END IF;
        l_dist_cust_trx_id := val_dist_rec(l_dist_cnt).leg_customer_trx_id;
        IF val_dist_rec(l_dist_cnt).leg_account_class = 'REC' THEN
          l_rec_dist_idx := g_dist_idx;
          l_rec_flag     := 'Y';
        END IF;
        IF (val_dist_rec(l_dist_cnt)
           .leg_account_class = 'REV' AND l_assign_flag = 'N' AND val_dist_rec(l_dist_cnt)
           .leg_line_type = 'LINE') THEN
          l_rec_int_line_attribute1 := val_dist_rec(l_dist_cnt)
                                       .interface_line_attribute1;
          /*--Ver 1.5 Commented for DFF rationalization start
                   l_rec_int_line_attribute2 := val_dist_rec (l_dist_cnt).interface_line_attribute2;
                         l_rec_int_line_attribute3 := val_dist_rec (l_dist_cnt).interface_line_attribute3;
                         l_rec_int_line_attribute4 := val_dist_rec (l_dist_cnt).interface_line_attribute4;
                         l_rec_int_line_attribute5 := val_dist_rec (l_dist_cnt).interface_line_attribute5;
                         l_rec_int_line_attribute6 := val_dist_rec (l_dist_cnt).interface_line_attribute6;
                         l_rec_int_line_attribute7 := val_dist_rec (l_dist_cnt).interface_line_attribute7;
                         l_rec_int_line_attribute8 := val_dist_rec (l_dist_cnt).interface_line_attribute8;
                         l_rec_int_line_attribute9 := val_dist_rec (l_dist_cnt).interface_line_attribute9;
                         l_rec_int_line_attribute10 := val_dist_rec (l_dist_cnt).interface_line_attribute10;
                         l_rec_int_line_attribute11 := val_dist_rec (l_dist_cnt).interface_line_attribute11;
                         l_rec_int_line_attribute12 := val_dist_rec (l_dist_cnt).interface_line_attribute12;
                         l_rec_int_line_attribute13 := val_dist_rec (l_dist_cnt).interface_line_attribute13;
                         l_rec_int_line_attribute14 := val_dist_rec (l_dist_cnt).interface_line_attribute14;

          */ --Ver 1.5 Commented for DFF rationalization end
          l_rec_int_line_attribute15 := val_dist_rec(l_dist_cnt)
                                        .interface_line_attribute15;
          l_dist_org_id              := val_dist_rec(l_dist_cnt).org_id;
          l_rev_flag                 := 'Y';
        END IF;
        IF (l_rec_flag = 'Y' AND l_rev_flag = 'Y' AND
           l_rec_dist_idx IS NOT NULL AND l_assign_flag = 'N') THEN
          g_invoice_dist(l_rec_dist_idx).interface_line_context := g_interface_line_context;
          g_invoice_dist(l_rec_dist_idx).interface_line_attribute1 := l_rec_int_line_attribute1;
          /*--Ver 1.5 Commented for DFF rationalization start
                   g_invoice_dist (l_rec_dist_idx).interface_line_attribute2 :=
                                                                                    l_rec_int_line_attribute2;
                         g_invoice_dist (l_rec_dist_idx).interface_line_attribute3 :=
                                                                                    l_rec_int_line_attribute3;
                         g_invoice_dist (l_rec_dist_idx).interface_line_attribute4 :=
                                                                                    l_rec_int_line_attribute4;
                         g_invoice_dist (l_rec_dist_idx).interface_line_attribute5 :=
                                                                                    l_rec_int_line_attribute5;
                         g_invoice_dist (l_rec_dist_idx).interface_line_attribute6 :=
                                                                                    l_rec_int_line_attribute6;
                         g_invoice_dist (l_rec_dist_idx).interface_line_attribute7 :=
                                                                                    l_rec_int_line_attribute7;
                         g_invoice_dist (l_rec_dist_idx).interface_line_attribute8 :=
                                                                                    l_rec_int_line_attribute8;
                         g_invoice_dist (l_rec_dist_idx).interface_line_attribute9 :=
                                                                                    l_rec_int_line_attribute9;
                         g_invoice_dist (l_rec_dist_idx).interface_line_attribute10 :=
                                                                                   l_rec_int_line_attribute10;
                         g_invoice_dist (l_rec_dist_idx).interface_line_attribute11 :=
                                                                                   l_rec_int_line_attribute11;
                         g_invoice_dist (l_rec_dist_idx).interface_line_attribute12 :=
                                                                                   l_rec_int_line_attribute12;
                         g_invoice_dist (l_rec_dist_idx).interface_line_attribute13 :=
                                                                                   l_rec_int_line_attribute13;
                         g_invoice_dist (l_rec_dist_idx).interface_line_attribute14 :=
                                                                                   l_rec_int_line_attribute14;
          */ --Ver 1.5 Commented for DFF rationalization end
          g_invoice_dist(l_rec_dist_idx).interface_line_attribute15 := l_rec_int_line_attribute15;
          g_invoice_dist(l_rec_dist_idx).org_id := l_dist_org_id;
          l_dist_org_id := NULL;
          l_assign_flag := 'Y';
        END IF;
        g_dist_idx := g_dist_idx + 1;

      --END IF;      -- commented for v1.48, Defect# 9239

      END LOOP;
    END LOOP;
    CLOSE val_dist_cur; -- v1.30
    val_dist_rec.delete; -- v1.30

    IF g_invoice_dist.EXISTS(1) THEN
      FORALL l_indx IN 1 .. g_invoice_dist.COUNT
      -- LOOP
        UPDATE xxar_invoices_dist_stg
           SET dist_segment1             = g_invoice_dist(l_indx)
                                           .dist_segment1,
               dist_segment2             = g_invoice_dist(l_indx)
                                           .dist_segment2,
               dist_segment3             = g_invoice_dist(l_indx)
                                           .dist_segment3,
               dist_segment4             = g_invoice_dist(l_indx)
                                           .dist_segment4,
               dist_segment5             = g_invoice_dist(l_indx)
                                           .dist_segment5,
               dist_segment6             = g_invoice_dist(l_indx)
                                           .dist_segment6,
               dist_segment7             = g_invoice_dist(l_indx)
                                           .dist_segment7,
               dist_segment8             = g_invoice_dist(l_indx)
                                           .dist_segment8,
               dist_segment9             = g_invoice_dist(l_indx)
                                           .dist_segment9,
               dist_segment10            = g_invoice_dist(l_indx)
                                           .dist_segment10,
               code_combination_id       = g_invoice_dist(l_indx)
                                           .code_combination_id,
               interface_line_context    = g_invoice_dist(l_indx)
                                           .interface_line_context,
               interface_line_attribute1 = g_invoice_dist(l_indx)
                                           .interface_line_attribute1,
               /*--Ver 1.5 Commented for DFF rationalization start
               interface_line_attribute2 = g_invoice_dist (l_indx).interface_line_attribute2,
                           interface_line_attribute3 = g_invoice_dist (l_indx).interface_line_attribute3,
                           interface_line_attribute4 = g_invoice_dist (l_indx).interface_line_attribute4,
                           interface_line_attribute5 = g_invoice_dist (l_indx).interface_line_attribute5,
                           interface_line_attribute6 = g_invoice_dist (l_indx).interface_line_attribute6,
                           interface_line_attribute7 = g_invoice_dist (l_indx).interface_line_attribute7,
                           interface_line_attribute8 = g_invoice_dist (l_indx).interface_line_attribute8,
                           interface_line_attribute9 = g_invoice_dist (l_indx).interface_line_attribute9,
                           interface_line_attribute10 = g_invoice_dist (l_indx).interface_line_attribute10,
                           interface_line_attribute11 = g_invoice_dist (l_indx).interface_line_attribute11,
                           interface_line_attribute12 = g_invoice_dist (l_indx).interface_line_attribute12,
                           interface_line_attribute13 = g_invoice_dist (l_indx).interface_line_attribute13,
                           interface_line_attribute14 = g_invoice_dist (l_indx).interface_line_attribute14,
               */ --Ver 1.5 Commented for DFF rationalization end
               interface_line_attribute15 = g_invoice_dist(l_indx)
                                            .interface_line_attribute15,
               accounted_amount           = g_invoice_dist(l_indx)
                                            .accounted_amount
               --ver1.21.1 changes start
              ,
               dist_amount = g_invoice_dist(l_indx).accounted_amount
               --ver1.21.1 changes end
              ,
               account_class     = g_invoice_dist(l_indx).account_class,
               org_id            = g_invoice_dist(l_indx).org_id,
               PERCENT           = g_invoice_dist(l_indx).PERCENT,
               process_flag      = g_invoice_dist(l_indx).process_flag,
               ERROR_TYPE        = g_invoice_dist(l_indx).ERROR_TYPE,
               last_update_date  = g_sysdate,
               last_updated_by   = g_last_updated_by,
               last_update_login = g_login_id
         WHERE interface_txn_id = g_invoice_dist(l_indx).interface_txn_id;
      --END LOOP;
    END IF;
    g_invoice_dist.DELETE; -- v1.30
    /*     FOR r_dist_err_rec IN
             (SELECT xds.interface_txn_id
                FROM xxar_invoices_dist_stg xds
               WHERE xds.process_flag = 'N' AND xds.batch_id = g_new_batch_id
              UNION
              SELECT xds1.interface_txn_id
                FROM xxar_invoices_dist_stg xds1
               WHERE xds1.batch_id = g_new_batch_id
                 AND xds1.leg_customer_trx_id IN (
                        SELECT xds2.leg_customer_trx_id
                          FROM xxar_invoices_dist_stg xds2
                         WHERE xds2.process_flag IN ('N', 'E')
                           AND xds2.batch_id = g_new_batch_id))
          LOOP
    */

    /** Added below for v1.43 **/

    FOR multiple_le_trx_rec IN multiple_le_trx_cur LOOP
      g_retcode := 1;

      UPDATE /*+ INDEX (xis XXAR_INVOICES_DIST_STG_N6) */ xxar_invoices_dist_stg xis
         SET process_flag      = 'E',
             error_type        = 'ERR_VAL',
             last_update_date  = g_sysdate,
             last_updated_by   = g_last_updated_by,
             last_update_login = g_login_id
       WHERE batch_id = g_new_batch_id
         AND run_sequence_id = g_new_run_seq_id
         AND leg_customer_trx_id = multiple_le_trx_rec.leg_customer_trx_id
         AND dist_segment1 IS NOT NULL; -- modified for v1.48, Defect# 9249

      FOR log_errors_rec IN (SELECT /*+ INDEX (xids XXAR_INVOICES_DIST_STG_N6) */
                              xids.interface_txn_id, dist_segment1
                               FROM xxar_invoices_dist_stg xids
                              WHERE xids.batch_id = g_new_batch_id
                                AND xids.run_sequence_id = g_new_run_seq_id
                                AND xids.leg_customer_trx_id =
                                    multiple_le_trx_rec.leg_customer_trx_id
                                AND xids.dist_segment1 IS NOT NULL) -- modified for v1.48, Defect# 9249
       LOOP
        l_err_code := 'ETN_MULTIPLE_LE_INVOICE';
        l_err_msg  := 'Error : Multiple Legal Entity (segment1) at distributions are not allowed for an Invoice';
        log_errors(pin_transaction_id      => log_errors_rec.interface_txn_id,
                   piv_source_column_name  => 'DIST_SEGMENT1',
                   piv_source_column_value => log_errors_rec.dist_segment1,
                   piv_error_type          => 'ERR_VAL',
                   piv_error_code          => l_err_code,
                   piv_error_message       => l_err_msg,
                   piv_source_table        => 'XXAR_INVOICES_DIST_STG',
                   pov_return_status       => l_log_ret_status,
                   pov_error_msg           => l_log_err_msg);
      END LOOP;

    END LOOP;

    COMMIT;  -- added commit for v1.55 to avoid table lock

    /** v1.51 Change starts  *********************/

    FOR c_upd_dist_rec IN c_upd_dist_cur LOOP
      g_retcode := 1;

      UPDATE  xxar_invoices_dist_stg xis
         SET process_flag      = 'E',
             error_type        = 'ERR_VAL',
             last_update_date  = g_sysdate,
             last_updated_by   = g_last_updated_by,
             last_update_login = g_login_id
       WHERE batch_id = g_new_batch_id
         AND run_sequence_id = g_new_run_seq_id
         AND leg_customer_trx_id = c_upd_dist_rec.leg_customer_trx_id
         AND leg_cust_trx_line_id = c_upd_dist_rec.leg_cust_trx_line_id
         AND leg_account_class = c_upd_dist_rec.leg_account_class
         AND leg_org_name      = c_upd_dist_rec.leg_org_name
         AND leg_source_system = 'SASC'
         AND rowid = c_upd_dist_rec.rowid;

         FOR log_errors_rec IN (SELECT
                              xids.interface_txn_id, leg_customer_trx_id
                               FROM xxar_invoices_dist_stg xids
                              WHERE xids.batch_id = g_new_batch_id
                                AND xids.run_sequence_id = g_new_run_seq_id
                                AND xids.leg_customer_trx_id =
                                    c_upd_dist_rec.leg_customer_trx_id
                                AND xids.leg_cust_trx_line_id = c_upd_dist_rec.leg_cust_trx_line_id
                                AND xids.leg_source_system = 'SASC'
                                AND xids.leg_account_class = c_upd_dist_rec.leg_account_class
                                AND xids.leg_org_name = c_upd_dist_rec.leg_org_name)
       LOOP
        l_err_code := 'ETN_DIST_CUST_TRX_LINE_ID';
        l_err_msg  := 'Error : One or more transaction distributions does not have correct line number/11i customer_trx_line_id';
        log_errors(pin_transaction_id      => log_errors_rec.interface_txn_id,
                   piv_source_column_name  => 'leg_customer_trx_id',
                   piv_source_column_value => log_errors_rec.leg_customer_trx_id,
                   piv_error_type          => 'ERR_VAL',
                   piv_error_code          => l_err_code,
                   piv_error_message       => l_err_msg,
                   piv_source_table        => 'XXAR_INVOICES_DIST_STG',
                   pov_return_status       => l_log_ret_status,
                   pov_error_msg           => l_log_err_msg);
      END LOOP;

      END LOOP;

      COMMIT;  -- added commit for v1.55 to avoid table lock

    /** v1.51 Change ends    **********************/


    /** Added above for v1.43 **/

    FOR r_dist_err_rec IN (SELECT DISTINCT xds.leg_customer_trx_id,
                                           xds.process_flag
                             FROM xxar_invoices_dist_stg xds
                            WHERE xds.process_flag IN ('N', 'E')
                              AND xds.batch_id = g_new_batch_id
                              AND xds.run_sequence_id = g_new_run_seq_id
                              AND DECODE(xds.process_flag,
                                         'E',
                                         NVL(xds.ERROR_TYPE, 'A'),
                                         'ERR_VAL') = 'ERR_VAL')
    --AND NVL(xds.leg_account_class,'A') <> 'ROUND')  --performance
     LOOP
      g_retcode := 1;
      UPDATE /*+ INDEX (xis XXAR_INVOICES_DIST_STG_N6) */ xxar_invoices_dist_stg xis
         SET process_flag      = 'E',
             ERROR_TYPE        = 'ERR_VAL',
             last_update_date  = g_sysdate,
             last_updated_by   = g_last_updated_by,
             last_update_login = g_login_id
       WHERE leg_customer_trx_id = r_dist_err_rec.leg_customer_trx_id
         AND batch_id = g_new_batch_id
         AND run_sequence_id = g_new_run_seq_id;
      IF r_dist_err_rec.process_flag <> 'N' THEN
        UPDATE /*+ INDEX (xis XXAR_INVOICES_STG_N4) */ xxar_invoices_stg xis
           SET process_flag      = 'E',
               ERROR_TYPE        = 'ERR_VAL',
               last_update_date  = g_sysdate,
               last_updated_by   = g_last_updated_by,
               last_update_login = g_login_id
         WHERE leg_customer_trx_id = r_dist_err_rec.leg_customer_trx_id
           AND batch_id = g_new_batch_id
           AND run_sequence_id = g_new_run_seq_id;
        l_err_code := 'ETN_INVOICE_ERROR';
        l_err_msg  := 'Error : Erroring out lines since corresponding distribution is in error';
        log_errors(
                   --   pin_transaction_id           =>  r_dist_err_rec.interface_txn_id
                   piv_error_type          => 'ERR_VAL',
                   piv_source_column_name  => 'LEGACY_CUSTOMER_TRX_ID',
                   piv_source_column_value => r_dist_err_rec.leg_customer_trx_id,
                   piv_error_code          => l_err_code,
                   piv_error_message       => l_err_msg,
                   pov_return_status       => l_log_ret_status,
                   pov_error_msg           => l_log_err_msg);
      END IF;
      l_err_code := 'ETN_DISTRIBUTION_ERROR';
      IF r_dist_err_rec.process_flag = 'N' THEN
        l_err_msg := 'Error : Erroring distribution since corresponding invoice line in error ';
      ELSE
        l_err_msg := 'Error : Erroring distribution since another related distribution in error ';
      END IF;
      log_errors( --pin_transaction_id      => r_dist_err_rec.interface_txn_id,
                 piv_error_type          => 'ERR_VAL',
                 piv_source_column_name  => 'LEGACY_CUSTOMER_TRX_ID',
                 piv_source_column_value => r_dist_err_rec.leg_customer_trx_id,
                 piv_error_code          => l_err_code,
                 piv_error_message       => l_err_msg,
                 pov_return_status       => l_log_ret_status,
                 piv_source_table        => 'XXAR_INVOICES_DIST_STG',
                 pov_error_msg           => l_log_err_msg);
    END LOOP;
    /*    FOR r_invoice_err_rec IN (SELECT DISTINCT xds.leg_customer_trx_id
                                               FROM xxar_invoices_dist_stg xds
                                              WHERE xds.process_flag = 'E'
                                                AND xds.batch_id = g_new_batch_id)
          LOOP
             UPDATE xxar_invoices_stg
                SET process_flag = 'E'
              WHERE leg_customer_trx_id = r_invoice_err_rec.leg_customer_trx_id
                AND batch_id = g_new_batch_id;
             l_err_code := 'ETN_INVOICE_ERROR';
             l_err_msg :=
                'Error : Erroring out lines since corresponding distribution is in error';
             log_errors
                (
                 --   pin_transaction_id           =>  r_dist_err_rec.interface_txn_id
                 piv_error_type               => 'ERR_VAL',
                 piv_source_column_name       => 'LEGACY_CUSTOMER_TRX_ID',
                 piv_source_column_value      => r_invoice_err_rec.leg_customer_trx_id,
                 piv_error_code               => l_err_code,
                 piv_error_message            => l_err_msg,
                 pov_return_status            => l_log_ret_status,
                 pov_error_msg                => l_log_err_msg
                );
          END LOOP;
    */
    COMMIT;
  EXCEPTION
    WHEN OTHERS THEN
      print_log_message('Error : Backtace : ' ||
                        DBMS_UTILITY.format_error_backtrace);
      g_retcode  := 2;
      l_err_code := 'ETN_AR_PROCEDURE_EXCEPTION';
      l_err_msg  := 'Error : Exception in validate_invoice Procedure. ' ||
                    SUBSTR(SQLERRM, 1, 150);
      log_errors(
                 -- pin_transaction_id           =>  pin_trx_id
                 -- , piv_source_column_name     =>  'Legacy link_to_customer_trx_line_id'
                 --  , piv_source_column_value    =>  val_inv_det_rec(l_line_cnt).leg_link_to_cust_trx_line_id
                 piv_error_type    => 'ERR_VAL',
                 piv_error_code    => l_err_code,
                 piv_error_message => l_err_msg,
                 pov_return_status => l_log_ret_status,
                 pov_error_msg     => l_log_err_msg);
  END validate_invoice;

  --ver1.11
  --ver1.21.2 changes start
  PROCEDURE recalc_dist_amount IS
    /*CURSOR cur_inclusive_trx
        SELECT DISTINCT leg_custumer_trx_id
          ,link_to_line_attribute1
          ,link_to_line_attribute15
          FROM xxar_invoices_stg
         WHERE process_flag = 'V'
             AND batch_id = g_new_batch_id
             AND run_sequence_id = g_new_run_seq_id
         AND amount_includes_tax_flag = 'Y';
    */
    CURSOR cur_inclusive_trx IS
      SELECT incl.leg_cust_trx_line_id leg_cust_trx_line_id,
             SUM(incl.leg_line_amount) leg_line_amount
        FROM (SELECT xis2.leg_cust_trx_line_id,  -- removed DISTINCT for v1.54
                              xis1.leg_line_amount
              --  ,link_to_line_attribute1
              --  ,link_to_line_attribute15
                FROM xxar_invoices_stg xis1, xxar_invoices_stg xis2
               WHERE xis1.process_flag = 'V'
                 AND xis1.batch_id = g_new_batch_id
                 AND xis1.run_sequence_id = g_new_run_seq_id
                 AND xis1.amount_includes_tax_flag = 'Y'
                 AND xis1.leg_operating_unit IN
                     ('OU ELECTRICAL BR',
                      'OU FLUID POWER BR',
                      'OU TRUCK COMPONENTS BR') -- added for v1.48, Defect# 9404
                 AND xis1.leg_link_to_cust_trx_line_id =
                     xis2.leg_cust_trx_line_id) incl
       GROUP BY incl.leg_cust_trx_line_id;

    TYPE l_inclusive_trx_rec IS RECORD(
      leg_cust_trx_line_id NUMBER,
      leg_line_amount      NUMBER);
    TYPE l_inclusive_trx_tbl IS TABLE OF l_inclusive_trx_rec INDEX BY BINARY_INTEGER;
    l_dist_idx      NUMBER := 1;
    l_inclusive_trx l_inclusive_trx_tbl;
  BEGIN
    OPEN cur_inclusive_trx;
    LOOP
      l_inclusive_trx.DELETE;
      FETCH cur_inclusive_trx BULK COLLECT
        INTO l_inclusive_trx LIMIT 5000;
      EXIT WHEN l_inclusive_trx.COUNT = 0;
      BEGIN
        IF l_inclusive_trx.EXISTS(1) THEN
          FORALL l_indx IN 1 .. l_inclusive_trx.COUNT
            UPDATE xxar_invoices_dist_stg
               SET dist_amount       = dist_amount + l_inclusive_trx(l_indx)
                                      .leg_line_amount,
                   last_update_date  = g_sysdate,
                   last_updated_by   = g_last_updated_by,
                   last_update_login = g_login_id
             WHERE leg_cust_trx_line_id = l_inclusive_trx(l_indx)
                  .leg_cust_trx_line_id;
        END IF;
      END;
    END LOOP;
    CLOSE cur_inclusive_trx;

    -- Added exception for v1.44
  EXCEPTION
    WHEN OTHERS THEN
      g_retcode := 1;
      print_log_message('SQL Error in proc: recalc_dist_amount: ' ||
                        SQLERRM);
  END recalc_dist_amount;

  --ver1.21.2 changes end
  PROCEDURE group_tax_lines IS
    CURSOR get_tax_lines_cur(p_leg_customer_trx_id NUMBER) IS --, p_int_line_attr_15 IN VARCHAR2)  IS  --ttax
      SELECT batch_id,
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
             header_attribute8
             --ver1.23 changes start
            ,
             header_attribute13
             --ver1.23 changes end
             --ver1.25 changes start
            ,
             header_attribute14
             --ver1.25 changes end
             --ver1.15 changes start
             /*,attribute_category
                     ,attribute1
                     ,attribute2
                     ,attribute3
                     ,attribute4
                     ,attribute5
                     ,attribute6
                     ,attribute7
                     ,attribute8
                     ,attribute9
                     ,attribute10
                     ,attribute11
                     ,attribute12
                     ,attribute13
                     ,attribute14
                     ,attribute15
                     ,header_gdf_attr_category
                     ,header_gdf_attribute1
                     ,header_gdf_attribute2
                     ,header_gdf_attribute3
                     ,header_gdf_attribute4
                     ,header_gdf_attribute5
                     ,header_gdf_attribute6
                     ,header_gdf_attribute7
                     ,header_gdf_attribute8
                     ,header_gdf_attribute9
                     ,header_gdf_attribute10
                     ,header_gdf_attribute11
                     ,header_gdf_attribute12
                     ,header_gdf_attribute13
                     ,header_gdf_attribute14
                     ,header_gdf_attribute15
                     ,header_gdf_attribute16
                     ,header_gdf_attribute17
                     ,header_gdf_attribute18
                     ,header_gdf_attribute19
                     ,header_gdf_attribute20
                     ,header_gdf_attribute21
                     ,header_gdf_attribute22
                     ,header_gdf_attribute23
                     ,header_gdf_attribute24
                     ,header_gdf_attribute25
                     ,header_gdf_attribute26
                     ,header_gdf_attribute27
                     ,header_gdf_attribute28
                     ,header_gdf_attribute29
                     ,header_gdf_attribute30
                     ,line_gdf_attr_category
                     ,line_gdf_attribute1
                     ,line_gdf_attribute2
                     ,line_gdf_attribute3
                     ,line_gdf_attribute4
                     ,line_gdf_attribute5
                     ,line_gdf_attribute6
                     ,line_gdf_attribute7
                     ,line_gdf_attribute8
                     ,line_gdf_attribute9
                     ,line_gdf_attribute10
                     ,line_gdf_attribute11
                     ,line_gdf_attribute12
                     ,line_gdf_attribute13
                     ,line_gdf_attribute14
                     ,line_gdf_attribute15
                     ,line_gdf_attribute16
                     ,line_gdf_attribute17
                     ,line_gdf_attribute18
                     ,line_gdf_attribute19
                     ,line_gdf_attribute20
             --    */
             --ver1.15 changes end
            ,
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
             --ver1.39.2 changes start
            ,
             leg_source_system
             --ver1.39.2 changes end
            ,
             SUM(quantity) quantity,
             SUM(quantity_ordered) quantity_ordered,
             SUM(unit_selling_price) unit_selling_price,
             SUM(unit_standard_price) unit_standard_price,
             SUM(tax_rate) tax_rate,
             MAX(interface_line_attribute1) interface_line_attribute1,
             MAX(interface_line_attribute15) interface_line_attribute15,
             SUM(line_amount) line_amount,
             MAX(comments) comments
      --MAX(interface_txn_id) interface_txn_id
      --,xxar_invoices_ext_r12_s.nextval interface_txn_id
        FROM xxar_invoices_stg
       WHERE process_flag = 'V'
         AND batch_id = g_new_batch_id
         AND run_sequence_id = g_new_run_seq_id
         AND leg_customer_trx_id = p_leg_customer_trx_id --ttax
            --AND interface_line_attribute15  = p_int_line_attr_15 --ttax
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
                header_attribute8
                --ver1.23 changes start
               ,
                header_attribute13
                --ver1.23 changes end
                --ver1.25 changes start
               ,
                header_attribute14
                --ver1.25 changes end
                --ver1.15 changes start
                /*
                ,attribute_category
                ,attribute1
                ,attribute2
                ,attribute3
                ,attribute4
                ,attribute5
                ,attribute6
                ,attribute7
                ,attribute8
                ,attribute9
                ,attribute10
                ,attribute11
                ,attribute12
                ,attribute13
                ,attribute14
                ,attribute15
                ,header_gdf_attr_category
                ,header_gdf_attribute1
                ,header_gdf_attribute2
                ,header_gdf_attribute3
                ,header_gdf_attribute4
                ,header_gdf_attribute5
                ,header_gdf_attribute6
                ,header_gdf_attribute7
                ,header_gdf_attribute8
                ,header_gdf_attribute9
                ,header_gdf_attribute10
                ,header_gdf_attribute11
                ,header_gdf_attribute12
                ,header_gdf_attribute13
                ,header_gdf_attribute14
                ,header_gdf_attribute15
                ,header_gdf_attribute16
                ,header_gdf_attribute17
                ,header_gdf_attribute18
                ,header_gdf_attribute19
                ,header_gdf_attribute20
                ,header_gdf_attribute21
                ,header_gdf_attribute22
                ,header_gdf_attribute23
                ,header_gdf_attribute24
                ,header_gdf_attribute25
                ,header_gdf_attribute26
                ,header_gdf_attribute27
                ,header_gdf_attribute28
                ,header_gdf_attribute29
                ,header_gdf_attribute30
                ,line_gdf_attr_category
                ,line_gdf_attribute1
                ,line_gdf_attribute2
                ,line_gdf_attribute3
                ,line_gdf_attribute4
                ,line_gdf_attribute5
                ,line_gdf_attribute6
                ,line_gdf_attribute7
                ,line_gdf_attribute8
                ,line_gdf_attribute9
                ,line_gdf_attribute10
                ,line_gdf_attribute11
                ,line_gdf_attribute12
                ,line_gdf_attribute13
                ,line_gdf_attribute14
                ,line_gdf_attribute15
                ,line_gdf_attribute16
                ,line_gdf_attribute17
                ,line_gdf_attribute18
                ,line_gdf_attribute19
                ,line_gdf_attribute20
                */
                --ver1.15 changes end
               ,
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
                --ver1.39.2 changes start
               ,
                leg_source_system;
    --ver1.39.2 changes end
    --   HAVING COUNT(1) > 1;
    --ttax
    CURSOR get_tax_lines_wrapper_cur IS
      SELECT DISTINCT leg_customer_trx_id
        FROM (SELECT batch_id,
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
                     system_ship_address_id,
                     purchase_order,
                     reason_code,
                     header_attribute_category,
                     header_attribute1,
                     header_attribute4,
                     header_attribute8
                     --ver1.23 changes start
                    ,
                     header_attribute13
                     --ver1.23 changes end
                     --ver1.25 changes start
                    ,
                     header_attribute14
                     --ver1.25 changes end
                    ,
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
                     amount_includes_tax_flag,-- uncommented for v1.54
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
                     --ver1.39.2 changes start
                    ,
                     leg_source_system
                     --ver1.39.2 changes end
                    ,
                     SUM(quantity) quantity,
                     SUM(quantity_ordered) quantity_ordered,
                     SUM(unit_selling_price) unit_selling_price,
                     SUM(unit_standard_price) unit_standard_price,
                     SUM(tax_rate) tax_rate,
                     MAX(interface_line_attribute1) interface_line_attribute1,
                     MAX(interface_line_attribute15) interface_line_attribute15,
                     SUM(line_amount) line_amount,
                     MAX(comments) comments
                FROM xxar_invoices_stg
               WHERE process_flag = 'V'
                 AND batch_id = g_new_batch_id
                 AND run_sequence_id = g_new_run_seq_id
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
                        system_ship_address_id,
                        purchase_order,
                        reason_code,
                        header_attribute_category,
                        header_attribute1,
                        header_attribute4,
                        header_attribute8
                        --ver1.23 changes start
                       ,
                        header_attribute13
                        --ver1.23 changes end
                        --ver1.25 changes start
                       ,
                        header_attribute14
                        --ver1.25 changes end
                       ,
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
                        amount_includes_tax_flag,  -- uncommented for v1.54
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
                        --ver1.39.2 changes start
                       ,
                        leg_source_system
              --ver1.39.2 changes end
              HAVING COUNT(1) > 1);
    --ttax
    CURSOR get_tax_dist_cur(p_link_to_line_attribute1    IN VARCHAR2,
                            p_link_to_line_attribute15   IN VARCHAR2,
                            p_tax_regime_code            IN VARCHAR2,
                            p_tax_rate_code              IN VARCHAR2,
                            p_tax                        IN VARCHAR2,
                            p_tax_status_code            IN VARCHAR2,
                            p_tax_jurisdiction_code      IN VARCHAR2,
                            p_interface_line_attribute1  IN VARCHAR2,
                            p_interface_line_attribute15 IN VARCHAR2,
                            p_tax_line_amount            IN NUMBER,
                            p_amount_includes_tax_flag   IN VARCHAR2 --ttax
                            ) IS
      SELECT xds.batch_id,
             xds.load_id,
             xds.leg_customer_trx_id
             --ver1.39.2 changes start
            ,
             xds.leg_source_system
             --ver1.39.2 changes end
            ,
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
             p_interface_line_attribute1  interface_line_attribute1,
             p_interface_line_attribute15 interface_line_attribute15,
             xds.account_class
             --,ROUND((SUM(xds.accounted_amount) *100)/p_tax_line_amount, 4) tax_dist_per
             --ver1.21.1 changes start
             --,SUM(xds.accounted_amount) accounted_amount
            ,
             SUM(xds.dist_amount) dist_amount
      --ver1.21.1 changes end
      --MAX(xds.interface_txn_id) interface_txn_id
      --,xxar_invoices_dist_ext_r12_s.nextval interface_txn_id
        FROM xxar_invoices_dist_stg xds, xxar_invoices_stg xis
       WHERE xds.leg_customer_trx_id = xis.leg_customer_trx_id
         AND xds.leg_cust_trx_line_id = xis.leg_cust_trx_line_id
         AND NVL(xds.leg_account_class, 'A') = 'TAX'
         AND xds.process_flag = 'V'
         AND xds.batch_id = g_new_batch_id
         AND xds.run_sequence_id = g_new_run_seq_id
         AND xis.process_flag = 'X'
         AND NVL(xis.link_to_line_attribute1, 'NO VALUE') =
             NVL(p_link_to_line_attribute1, 'NO VALUE')
         AND NVL(xis.link_to_line_attribute15, 'NO VALUE') =
             NVL(p_link_to_line_attribute15, 'NO VALUE')
         AND NVL(xis.tax_regime_code, 'NO VALUE') =
             NVL(p_tax_regime_code, 'NO VALUE')
         AND NVL(xis.tax_rate_code, 'NO VALUE') =
             NVL(p_tax_rate_code, 'NO VALUE')
         AND NVL(xis.tax, 'NO VALUE') = NVL(p_tax, 'NO VALUE')
         AND NVL(xis.tax_status_code, 'NO VALUE') =
             NVL(p_tax_status_code, 'NO VALUE')
         AND NVL(xis.tax_jurisdiction_code, 'NO VALUE') =
             NVL(p_tax_jurisdiction_code, 'NO VALUE')
         AND NVL(xis.amount_includes_tax_flag, 'N') =
             NVL(p_amount_includes_tax_flag, 'N') --ttax
       GROUP BY xds.batch_id,
                xds.load_id,
                xds.leg_customer_trx_id
                --ver1.39.2 changes start
               ,
                xds.leg_source_system
                --ver1.39.2 changes end
               ,
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
    l_tax_insert_flag VARCHAR2(1);
    l_err             VARCHAR2(1000);

    -- v1.46 changes
    /**   CURSOR fetch_x_invoices_cur
    IS
    SELECT xis.interface_line_attribute1
          ,xis.interface_line_attribute15
    FROM xxar_invoices_stg xis
    WHERE xis.batch_id = g_new_batch_id
    AND xis.run_sequence_id = g_new_run_seq_id
    AND xis.process_flag = 'X'
    AND xis.leg_line_type = 'TAX';  **/

  BEGIN
    FOR get_tax_lines_wrapper_rec IN get_tax_lines_wrapper_cur LOOP
      --ttax
      FOR get_tax_lines_rec IN get_tax_lines_cur(get_tax_lines_wrapper_rec.leg_customer_trx_id) --, get_tax_lines_wrapper_rec.interface_line_attribute15) --ttax
       LOOP
        BEGIN
          SAVEPOINT tax;
          l_tax_insert_flag := NULL;
          INSERT INTO xxar_invoices_stg
            (interface_txn_id,
             batch_id,
             load_id,
             leg_customer_trx_id,
             run_sequence_id,
             creation_date,
             created_by,
             last_update_date,
             last_updated_by,
             last_update_login,
             program_application_id,
             program_id,
             program_update_date,
             request_id,
             process_flag,
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
             header_attribute8
             --ver1.23 changes start
            ,
             header_attribute13
             --ver1.23 changes end
             --ver1.25 changes start
            ,
             header_attribute14
             --ver1.25 changes end
             --ver1.15 changes start
             /*
             ,attribute_category
             ,attribute1
             ,attribute2
             ,attribute3
             ,attribute4
             ,attribute5
             ,attribute6
             ,attribute7
             ,attribute8
             ,attribute9
             ,attribute10
             ,attribute11
             ,attribute12
             ,attribute13
             ,attribute14
             ,attribute15
             ,header_gdf_attr_category
             ,header_gdf_attribute1
             ,header_gdf_attribute2
             ,header_gdf_attribute3
             ,header_gdf_attribute4
             ,header_gdf_attribute5
             ,header_gdf_attribute6
             ,header_gdf_attribute7
             ,header_gdf_attribute8
             ,header_gdf_attribute9
             ,header_gdf_attribute10
             ,header_gdf_attribute11
             ,header_gdf_attribute12
             ,header_gdf_attribute13
             ,header_gdf_attribute14
             ,header_gdf_attribute15
             ,header_gdf_attribute16
             ,header_gdf_attribute17
             ,header_gdf_attribute18
             ,header_gdf_attribute19
             ,header_gdf_attribute20
             ,header_gdf_attribute21
             ,header_gdf_attribute22
             ,header_gdf_attribute23
             ,header_gdf_attribute24
             ,header_gdf_attribute25
             ,header_gdf_attribute26
             ,header_gdf_attribute27
             ,header_gdf_attribute28
             ,header_gdf_attribute29
             ,header_gdf_attribute30
             ,line_gdf_attr_category
             ,line_gdf_attribute1
             ,line_gdf_attribute2
             ,line_gdf_attribute3
             ,line_gdf_attribute4
             ,line_gdf_attribute5
             ,line_gdf_attribute6
             ,line_gdf_attribute7
             ,line_gdf_attribute8
             ,line_gdf_attribute9
             ,line_gdf_attribute10
             ,line_gdf_attribute11
             ,line_gdf_attribute12
             ,line_gdf_attribute13
             ,line_gdf_attribute14
             ,line_gdf_attribute15
             ,line_gdf_attribute16
             ,line_gdf_attribute17
             ,line_gdf_attribute18
             ,line_gdf_attribute19
             ,line_gdf_attribute20
             */
             --ver1.15 changes end
            ,
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
             --ver1.39.2 changes start
            ,
             leg_source_system
             --ver1.39.2 changes end
            ,
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
             leg_line_amount)
          VALUES
            (xxar_invoices_ext_r12_s.nextval --get_tax_lines_rec.interface_txn_id
            ,
             get_tax_lines_rec.batch_id,
             get_tax_lines_rec.load_id,
             get_tax_lines_rec.leg_customer_trx_id,
             get_tax_lines_rec.run_sequence_id,
             g_sysdate,
             g_user_id,
             g_sysdate,
             g_user_id,
             g_login_id,
             get_tax_lines_rec.program_application_id,
             get_tax_lines_rec.program_id,
             get_tax_lines_rec.program_update_date,
             get_tax_lines_rec.request_id,
             'V',
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
             get_tax_lines_rec.header_attribute8
             --ver1.23 changes start
            ,
             get_tax_lines_rec.header_attribute13
             --ver1.23 changes end
             --ver1.25 changes start
            ,
             get_tax_lines_rec.header_attribute14
             --ver1.25 changes end
             --ver1.15 changes start
             /*
             ,get_tax_lines_rec.attribute_category
             ,get_tax_lines_rec.attribute1
             ,get_tax_lines_rec.attribute2
             ,get_tax_lines_rec.attribute3
             ,get_tax_lines_rec.attribute5
             ,get_tax_lines_rec.attribute6
             ,get_tax_lines_rec.attribute7
             ,get_tax_lines_rec.attribute8
             ,get_tax_lines_rec.attribute9
             ,get_tax_lines_rec.attribute10
             ,get_tax_lines_rec.attribute11
             ,get_tax_lines_rec.attribute12
             ,get_tax_lines_rec.attribute13
             ,get_tax_lines_rec.attribute14
             ,get_tax_lines_rec.attribute15
             ,get_tax_lines_rec.header_gdf_attr_category
             ,get_tax_lines_rec.header_gdf_attribute1
             ,get_tax_lines_rec.header_gdf_attribute2
             ,get_tax_lines_rec.header_gdf_attribute3
             ,get_tax_lines_rec.header_gdf_attribute4
             ,get_tax_lines_rec.header_gdf_attribute5
             ,get_tax_lines_rec.header_gdf_attribute6
             ,get_tax_lines_rec.header_gdf_attribute7
             ,get_tax_lines_rec.header_gdf_attribute8
             ,get_tax_lines_rec.header_gdf_attribute9
             ,get_tax_lines_rec.header_gdf_attribute10
             ,get_tax_lines_rec.header_gdf_attribute11
             ,get_tax_lines_rec.header_gdf_attribute12
             ,get_tax_lines_rec.header_gdf_attribute13
             ,get_tax_lines_rec.header_gdf_attribute14
             ,get_tax_lines_rec.header_gdf_attribute15
             ,get_tax_lines_rec.header_gdf_attribute16
             ,get_tax_lines_rec.header_gdf_attribute17
             ,get_tax_lines_rec.header_gdf_attribute18
             ,get_tax_lines_rec.header_gdf_attribute19
             ,get_tax_lines_rec.header_gdf_attribute20
             ,get_tax_lines_rec.header_gdf_attribute21
             ,get_tax_lines_rec.header_gdf_attribute22
             ,get_tax_lines_rec.header_gdf_attribute23
             ,get_tax_lines_rec.header_gdf_attribute24
             ,get_tax_lines_rec.header_gdf_attribute25
             ,get_tax_lines_rec.header_gdf_attribute26
             ,get_tax_lines_rec.header_gdf_attribute27
             ,get_tax_lines_rec.header_gdf_attribute28
             ,get_tax_lines_rec.header_gdf_attribute29
             ,get_tax_lines_rec.header_gdf_attribute30
             ,get_tax_lines_rec.line_gdf_attr_category
             ,get_tax_lines_rec.line_gdf_attribute1
             ,get_tax_lines_rec.line_gdf_attribute2
             ,get_tax_lines_rec.line_gdf_attribute3
             ,get_tax_lines_rec.line_gdf_attribute4
             ,get_tax_lines_rec.line_gdf_attribute5
             ,get_tax_lines_rec.line_gdf_attribute6
             ,get_tax_lines_rec.line_gdf_attribute7
             ,get_tax_lines_rec.line_gdf_attribute8
             ,get_tax_lines_rec.line_gdf_attribute9
             ,get_tax_lines_rec.line_gdf_attribute10
             ,get_tax_lines_rec.line_gdf_attribute11
             ,get_tax_lines_rec.line_gdf_attribute12
             ,get_tax_lines_rec.line_gdf_attribute13
             ,get_tax_lines_rec.line_gdf_attribute14
             ,get_tax_lines_rec.line_gdf_attribute15
             ,get_tax_lines_rec.line_gdf_attribute16
             ,get_tax_lines_rec.line_gdf_attribute17
             ,get_tax_lines_rec.line_gdf_attribute18
             ,get_tax_lines_rec.line_gdf_attribute19
             ,get_tax_lines_rec.line_gdf_attribute20
             */
             --ver1.15 changes end
            ,
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
             get_tax_lines_rec.conversion_date
             --ver1.39.2 changes start
            ,
             get_tax_lines_rec.leg_source_system
             --ver1.39.2 changes end
            ,
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
             get_tax_lines_rec.line_amount);
          UPDATE xxar_invoices_stg
             SET process_flag      = 'X',
                 last_update_date  = g_sysdate,
                 last_updated_by   = g_last_updated_by,
                 last_update_login = g_login_id
           WHERE batch_id = g_new_batch_id
             AND run_sequence_id = g_new_run_seq_id
             AND leg_line_type = 'TAX'
             AND NVL(amount_includes_tax_flag, 'N') =
                 NVL(get_tax_lines_rec.amount_includes_tax_flag, 'N') --ttax
             AND leg_tax_code IS NOT NULL
             AND NVL(link_to_line_attribute1, 'NO VALUE') =
                 NVL(get_tax_lines_rec.link_to_line_attribute1, 'NO VALUE')
             AND NVL(link_to_line_attribute15, 'NO VALUE') =
                 NVL(get_tax_lines_rec.link_to_line_attribute15, 'NO VALUE')
             AND NVL(tax_regime_code, 'NO VALUE') =
                 NVL(get_tax_lines_rec.tax_regime_code, 'NO VALUE')
             AND NVL(tax_rate_code, 'NO VALUE') =
                 NVL(get_tax_lines_rec.tax_rate_code, 'NO VALUE')
             AND NVL(tax, 'NO VALUE') =
                 NVL(get_tax_lines_rec.tax, 'NO VALUE')
             AND NVL(tax_status_code, 'NO VALUE') =
                 NVL(get_tax_lines_rec.tax_status_code, 'NO VALUE')
             AND NVL(tax_jurisdiction_code, 'NO VALUE') =
                 NVL(get_tax_lines_rec.tax_jurisdiction_code, 'NO VALUE');
          COMMIT;
          l_tax_insert_flag := 'S';
        EXCEPTION
          WHEN OTHERS THEN
            l_err := SQLERRM;
            ROLLBACK TO tax;
            l_tax_insert_flag := 'E';
        END;
        print_log_message('get_tax_lines_rec.interface_line_attribute1:' ||
                          get_tax_lines_rec.interface_line_attribute1);
        print_log_message('get_tax_lines_rec.interface_line_attribute15:' ||
                          get_tax_lines_rec.interface_line_attribute15);
        print_log_message('get_tax_lines_rec.amount_includes_tax_flag:' ||
                          get_tax_lines_rec.amount_includes_tax_flag);
        print_log_message('get_tax_lines_rec.line_amount:' ||
                          get_tax_lines_rec.line_amount);
        IF l_tax_insert_flag = 'S' THEN
          FOR get_tax_dist_rec IN get_tax_dist_cur(get_tax_lines_rec.link_to_line_attribute1,
                                                   get_tax_lines_rec.link_to_line_attribute15,
                                                   get_tax_lines_rec.tax_regime_code,
                                                   get_tax_lines_rec.tax_rate_code,
                                                   get_tax_lines_rec.tax,
                                                   get_tax_lines_rec.tax_status_code,
                                                   get_tax_lines_rec.tax_jurisdiction_code,
                                                   get_tax_lines_rec.interface_line_attribute1,
                                                   get_tax_lines_rec.interface_line_attribute15,
                                                   get_tax_lines_rec.line_amount,
                                                   get_tax_lines_rec.amount_includes_tax_flag
                                                   --ttax
                                                   ) LOOP
            BEGIN
              print_log_message('get_tax_dist_rec.interface_line_attribute1' ||
                                get_tax_dist_rec.interface_line_attribute1);
              print_log_message('get_tax_dist_rec.interface_line_attribute15' ||
                                get_tax_dist_rec.interface_line_attribute15);
              print_log_message('get_tax_dist_rec.dist_amount' ||
                                get_tax_dist_rec.dist_amount);
              INSERT INTO xxar_invoices_dist_stg
                (
                 --ver1.21.1 changes start
                 --accounted_amount
                 dist_amount
                 --ver1.21.1 changes end
                ,
                 code_combination_id,
                 org_id,
                 leg_customer_trx_id
                 --ver1.39.2 changes start
                ,
                 leg_source_system
                 --ver1.39.2 changes end
                ,
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
                 run_sequence_id,
                 creation_date,
                 created_by,
                 last_update_date,
                 last_updated_by,
                 last_update_login,
                 program_application_id,
                 program_id,
                 program_update_date,
                 request_id,
                 process_flag)
              VALUES
                (
                 --ver1.21.1 changes start
                 --get_tax_dist_rec.accounted_amount
                 get_tax_dist_rec.dist_amount
                 --ver1.21.1 changes end
                ,
                 get_tax_dist_rec.code_combination_id,
                 get_tax_dist_rec.org_id,
                 get_tax_dist_rec.leg_customer_trx_id
                 --ver1.39.2 changes start
                ,
                 get_tax_dist_rec.leg_source_system
                 --ver1.39.2 changes end
                ,
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
                 xxar_invoices_dist_ext_r12_s.nextval --get_tax_dist_rec.interface_txn_id
                ,
                 get_tax_dist_rec.batch_id,
                 get_tax_dist_rec.load_id,
                 get_tax_dist_rec.run_sequence_id,
                 g_sysdate,
                 g_user_id,
                 g_sysdate,
                 g_user_id,
                 g_login_id,
                 get_tax_dist_rec.program_application_id,
                 get_tax_dist_rec.program_id,
                 get_tax_dist_rec.program_update_date,
                 get_tax_dist_rec.request_id,
                 'V');
              /** Moving below section to end of procedure, v1.32 **/
              /**           UPDATE xxar_invoices_dist_stg xds
                 SET xds.process_flag ='X'
                  ,xds.last_update_date  = SYSDATE
                   ,xds.last_updated_by   = g_last_updated_by
                   ,xds.last_update_login = g_login_id
               WHERE xds.batch_id = g_new_batch_id
                 AND xds.run_sequence_id = g_new_run_seq_id
                 AND xds.leg_account_class = 'TAX'
                 AND (xds.interface_line_attribute1, xds.interface_line_attribute15)
                  IN(SELECT xis.interface_line_attribute1, xis.interface_line_attribute15
                    FROM xxar_invoices_stg xis
                    WHERE xis.process_flag = 'X'
              AND NVL(xis.amount_includes_tax_flag, 'N') = NVL(get_tax_lines_rec.amount_includes_tax_flag, 'N')  --ttax
                    AND NVL(xis.link_to_line_attribute1,'NO VALUE') = NVL(get_tax_lines_rec.link_to_line_attribute1 ,'NO VALUE')
                    AND NVL(xis.link_to_line_attribute15,'NO VALUE') = NVL(get_tax_lines_rec.link_to_line_attribute15,'NO VALUE')
                    AND NVL(xis.tax_regime_code ,'NO VALUE')= NVL(get_tax_lines_rec.tax_regime_code,'NO VALUE')
                    AND NVL(xis.tax_rate_code ,'NO VALUE')= NVL(get_tax_lines_rec.tax_rate_code,'NO VALUE')
                    AND NVL(xis.tax ,'NO VALUE')= NVL(get_tax_lines_rec.tax,'NO VALUE')
                    AND NVL(xis.tax_status_code ,'NO VALUE')= NVL(get_tax_lines_rec.tax_status_code,'NO VALUE')
                    AND NVL(xis.tax_jurisdiction_code ,'NO VALUE')= NVL(get_tax_lines_rec.tax_jurisdiction_code,'NO VALUE'));**/
            EXCEPTION
              WHEN OTHERS THEN
                l_err := SQLERRM;
                ROLLBACK TO tax;
            END;
          END LOOP;
          COMMIT;
        END IF;
      END LOOP;
    END LOOP; --ttax

    COMMIT; -- added for v1.47

    /** Moved from Loop section of Distributions, v1.32, added again for v1.46 **/
    UPDATE xxar_invoices_dist_stg xds
       SET xds.process_flag      = 'X',
           xds.last_update_date  = SYSDATE,
           xds.last_updated_by   = g_last_updated_by,
           xds.last_update_login = g_login_id
     WHERE xds.batch_id = g_new_batch_id
       AND xds.run_sequence_id = g_new_run_seq_id
       AND xds.leg_account_class = 'TAX'
          --AND (xds.interface_line_attribute1, xds.interface_line_attribute15) IN
          --    (SELECT xis.interface_line_attribute1          -- modified for v1.47
       AND xds.leg_cust_trx_line_id IN
           (SELECT xis.leg_cust_trx_line_id
              FROM xxar_invoices_stg xis
             WHERE xis.process_flag = 'X'
               AND xis.batch_id = g_new_batch_id
               AND xis.run_sequence_id = g_new_run_seq_id
               AND xis.leg_line_type = 'TAX');
    --AND NVL(xis.amount_includes_tax_flag, 'N') = NVL(get_tax_lines_rec.amount_includes_tax_flag, 'N')  --ttax
    --AND NVL(xis.link_to_line_attribute1,'NO VALUE') = NVL(get_tax_lines_rec.link_to_line_attribute1 ,'NO VALUE')
    --AND NVL(xis.link_to_line_attribute15,'NO VALUE') = NVL(get_tax_lines_rec.link_to_line_attribute15,'NO VALUE')
    --AND NVL(xis.tax_regime_code ,'NO VALUE')= NVL(get_tax_lines_rec.tax_regime_code,'NO VALUE')
    --AND NVL(xis.tax_rate_code ,'NO VALUE')= NVL(get_tax_lines_rec.tax_rate_code,'NO VALUE')
    --AND NVL(xis.tax ,'NO VALUE')= NVL(get_tax_lines_rec.tax,'NO VALUE')
    --AND NVL(xis.tax_status_code ,'NO VALUE')= NVL(get_tax_lines_rec.tax_status_code,'NO VALUE')
    --AND NVL(xis.tax_jurisdiction_code ,'NO VALUE')= NVL(get_tax_lines_rec.tax_jurisdiction_code,'NO VALUE'));
    COMMIT;

    /**-- Commented for v1.46 changes start
    FOR fetch_x_invoices_rec IN fetch_x_invoices_cur LOOP
    UPDATE xxar_invoices_dist_stg xds
       SET xds.process_flag      = 'X'
          ,xds.last_update_date  = g_sysdate
          ,xds.last_updated_by   = g_last_updated_by
          ,xds.last_update_login = g_login_id
     WHERE xds.batch_id = g_new_batch_id
       AND xds.run_sequence_id = g_new_run_seq_id
       AND xds.leg_account_class = 'TAX'
       AND xds.interface_line_attribute1  = fetch_x_invoices_rec.interface_line_attribute1
       AND xds.interface_line_attribute15 = fetch_x_invoices_rec.interface_line_attribute15;
     END LOOP;
    -- Commented for v1.46 changes end **/

  EXCEPTION
    WHEN OTHERS THEN
      g_retcode := 1;
      print_log_message('SQL Error in proc: group_tax_lines: ' || SQLERRM);
  END group_tax_lines;

  --ver1.11 end
  --
  -- ========================
  -- Procedure: CREATE_INVOICE
  -- =============================================================================
  --   This procedure insert records in interface table
  -- =============================================================================
  --
  PROCEDURE create_invoice IS
    l_err_code       VARCHAR2(40);
    l_err_msg        VARCHAR2(2000);
    l_upd_ret_status VARCHAR2(50) := NULL;
    l_log_ret_status VARCHAR2(50) := NULL;
    l_log_err_msg    VARCHAR2(2000);
    l_br_ou          VARCHAR2(100);
    l_country        VARCHAR2(50);
    CURSOR create_inv_cur IS
      SELECT *
        FROM xxar_invoices_stg
       WHERE process_flag = 'V'
         AND batch_id = g_new_batch_id;
    --       AND    leg_operating_unit = NVL( g_leg_operating_unit, leg_operating_unit);
    --   AND    run_sequence_id = g_new_run_seq_id
    CURSOR create_dist_cur IS
      SELECT /*+ INDEX (xds XXAR_INVOICES_DIST_STG_N4) */
       xds.*
        FROM xxar_invoices_dist_stg xds, xxar_invoices_stg xis
       WHERE xds.leg_customer_trx_id = xis.leg_customer_trx_id
         AND xds.leg_cust_trx_line_id = xis.leg_cust_trx_line_id
         AND NVL(xds.leg_account_class, 'A') <> 'REC'
            --        AND NVL(xds.leg_account_class,'A') <> 'ROUND' --performance
         AND xds.process_flag = 'V'
         AND xds.batch_id = g_new_batch_id
         AND xis.process_flag = 'P'
      UNION
      SELECT /*+ INDEX (xds XXAR_INVOICES_DIST_STG_N6) */
       xds.*
        FROM xxar_invoices_dist_stg xds, xxar_invoices_stg xis
       WHERE xds.leg_customer_trx_id = xis.leg_customer_trx_id
         AND xds.leg_account_class = 'REC'
         AND xds.process_flag = 'V'
         AND xds.batch_id = g_new_batch_id
         AND xis.process_flag = 'P'
      UNION
      --1.11
      SELECT /*+ INDEX (xds XXAR_INVOICES_DIST_STG_N6) */
       xds.*
        FROM xxar_invoices_dist_stg xds, xxar_invoices_stg xis
       WHERE xds.leg_customer_trx_id = xis.leg_customer_trx_id
         AND xds.account_class = 'TAX'
         AND xis.line_type = 'TAX'
         AND xds.leg_account_class IS NULL
         AND xds.process_flag = 'V'
         AND xds.batch_id = g_new_batch_id
         AND xis.process_flag = 'P';
  BEGIN
    FOR create_inv_rec IN create_inv_cur LOOP
      BEGIN
        /*     ---v1.13 C99885988  --Added for ALM Defect 1828: Quick fix for Inserting Warehouse ID for Brazil OU  --START

        BEGIN
           SELECT DISTINCT (xmu.operating_unit), xmu.country
           INTO l_br_ou , l_country
           FROM xxetn_map_unit xmu, hr_operating_units hou
           WHERE --xmu.country = 'Brazil'
                xmu.operating_unit = hou.name
           AND hou.organization_id = create_inv_rec.org_id;

           IF UPPER(l_country) != 'BRAZIL' THEN
             l_br_ou := NULL;
           END IF;

        EXCEPTION
           WHEN OTHERS THEN
            l_br_ou := NULL;
            print_log_message('Error while deriving Warehouse Id for BR Operating Unit: ' ||
                               create_inv_rec.leg_operating_unit);
        END;

        ---v1.13 C99885988  --Added for ALM Defect 1828: Quick fix for Inserting Warehouse ID for Brazil OU  --START */
        INSERT INTO apps.ra_interface_lines_all
          (interface_line_context,
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
           term_name,
           term_id,
           orig_system_bill_customer_ref,
           orig_system_bill_customer_id,
           orig_system_bill_address_ref,
           orig_system_bill_address_id,
           --orig_system_ship_customer_ref,
           orig_system_ship_address_id,
           orig_system_ship_customer_id,
           receipt_method_name --v1.36
          ,
           receipt_method_id --v1.36
          ,
           conversion_type,
           conversion_rate,
           conversion_date,
           purchase_order,
           reason_code,
           comments,
           header_attribute_category,
           header_attribute1
           --, header_attribute2
          ,
           header_attribute3 -- uncommented for v1.43
          ,
           header_attribute4
           --, header_attribute5
           --, header_attribute6
           --, header_attribute7
          ,
           header_attribute8,
           header_attribute9
           --, header_attribute10
           --, header_attribute11
           --, header_attribute12
           --, header_attribute13
           --, header_attribute14
           --, header_attribute15
           --ver1.23 changes start
          ,
           header_attribute13
           --ver1.23 changes end
           --ver1.25 changes start
          ,
           header_attribute14
           --ver1.25 changes end
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
           warehouse_id ---v1.13 C99885988  --Added for ALM Defect 1828 --Inserting Warehouse ID for Brazil OU
          ,
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
           org_id,
           creation_date,
           created_by,
           last_update_date,
           last_updated_by,
           last_update_login,
           tax_rate ---1.4 Added by Rohit D for FOT
           )
        VALUES
          (create_inv_rec.interface_line_context,
           create_inv_rec.interface_line_attribute1,
           /*--Ver 1.5 Commented for DFF rationalization start

           create_inv_rec.interface_line_attribute2,
                             create_inv_rec.interface_line_attribute3,
                             create_inv_rec.interface_line_attribute4,
                             create_inv_rec.interface_line_attribute5,
                             create_inv_rec.interface_line_attribute6,
                             create_inv_rec.interface_line_attribute7,
                             create_inv_rec.interface_line_attribute8,
                             create_inv_rec.interface_line_attribute9,
                             create_inv_rec.interface_line_attribute10,
                             create_inv_rec.interface_line_attribute11,
                             create_inv_rec.interface_line_attribute12,
                             create_inv_rec.interface_line_attribute13,
                             create_inv_rec.interface_line_attribute14,
           */
           --Ver 1.5 Commented for DFF rationalization end
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
           create_inv_rec.term_name,
           create_inv_rec.term_id,
           create_inv_rec.system_bill_customer_ref,
           create_inv_rec.system_bill_customer_id,
           create_inv_rec.system_bill_address_ref,
           create_inv_rec.system_bill_address_id,
           create_inv_rec.system_ship_address_id,
           DECODE(create_inv_rec.system_ship_address_id,
                  NULL,
                  NULL,
                  create_inv_rec.system_bill_customer_id),
           create_inv_rec.receipt_method_name --v1.36
          ,
           create_inv_rec.receipt_method_id --v1.36
          ,
           create_inv_rec.conversion_type,
           create_inv_rec.conversion_rate,
           create_inv_rec.conversion_date,
           create_inv_rec.purchase_order,
           create_inv_rec.reason_code,
           create_inv_rec.comments,
           create_inv_rec.header_attribute_category,
           create_inv_rec.header_attribute1,
           --ver 1.5 changes start
           --create_inv_rec.header_attribute2,
           create_inv_rec.header_attribute3, -- uncommented for v1.43
           create_inv_rec.header_attribute4,
           --create_inv_rec.header_attribute5,
           --create_inv_rec.header_attribute6,
           --create_inv_rec.header_attribute7,
           create_inv_rec.header_attribute8,
           create_inv_rec.header_attribute9
           --create_inv_rec.header_attribute10,
           --create_inv_rec.header_attribute11,
           --create_inv_rec.header_attribute12,
           --create_inv_rec.header_attribute13,
           --create_inv_rec.header_attribute14,
           --create_inv_rec.header_attribute15,
           --ver 1.5 changes end
           --ver1.23 changes start
          ,
           create_inv_rec.header_attribute13
           --ver1.23 changes end
           --ver1.25 changes start
          ,
           create_inv_rec.header_attribute14
           --ver1.25 changes end
          ,
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
           create_inv_rec.line_gdf_attribute20
           --,DECODE (create_inv_rec.leg_operating_unit, l_br_ou, 505, NULL) ---v1.13 C99885988  --Added for ALM Defect 1828
           ----  ,NVL2(l_br_ou,506,NULL) ---v1.13 C99885988  --Added for ALM Defect 1828
          ,
           create_inv_rec.warehouse_id ---------------------------Modified for CR 305000 by Sherine V
          ,
           create_inv_rec.trx_date,
           create_inv_rec.gl_date,
           create_inv_rec.trx_number,
           create_inv_rec.line_number,
           create_inv_rec.tax_code,
           create_inv_rec.tax_regime_code,
           create_inv_rec.tax_rate_code,
           create_inv_rec.tax,
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
           create_inv_rec.fob_point,
           create_inv_rec.ship_via,
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
           create_inv_rec.org_id,
           g_sysdate,
           apps.fnd_global.user_id,
           g_sysdate,
           apps.fnd_global.user_id,
           apps.fnd_global.login_id,
           create_inv_rec.tax_rate ---1.4 Added by Rohit D for FOT
           );
        update_status(pin_interface_txn_id => create_inv_rec.interface_txn_id,
                      piv_process_flag     => 'P',
                      piv_err_type         => NULL,
                      pov_return_status    => l_upd_ret_status
                      -- OUT
                     ,
                      pov_error_code => l_err_code
                      -- OUT
                     ,
                      pov_error_message => l_err_msg
                      -- OUT
                      );
      EXCEPTION
        WHEN OTHERS THEN
          g_retcode  := 1;
          l_err_code := 'ETN_AR_CREATE_EXCEPTION';
          l_err_msg  := 'Error : Exception in create_invoice Procedure for invoice lines. ' ||
                        SUBSTR(SQLERRM, 1, 150);
          log_errors(pin_transaction_id      => create_inv_rec.interface_txn_id,
                     piv_source_column_name  => 'LEGACY_CUSTOMER_TRX_ID',
                     piv_source_column_value => create_inv_rec.leg_customer_trx_id,
                     piv_error_type          => 'ERR_INT',
                     piv_error_code          => l_err_code,
                     piv_error_message       => l_err_msg,
                     pov_return_status       => l_log_ret_status,
                     pov_error_msg           => l_log_err_msg);
          update_status(pin_interface_txn_id => create_inv_rec.interface_txn_id,
                        piv_process_flag     => 'E',
                        piv_err_type         => 'ERR_INT',
                        pov_return_status    => l_upd_ret_status
                        -- OUT
                       ,
                        pov_error_code => l_err_code
                        -- OUT
                       ,
                        pov_error_message => l_err_msg
                        -- OUT
                        );
      END;
    END LOOP;
    FOR r_createline_err_rec IN (SELECT DISTINCT xis.leg_customer_trx_id,
                                                 xis.leg_trx_number
                                   FROM xxar_invoices_stg xis
                                  WHERE xis.process_flag IN ('E')
                                    AND xis.batch_id = g_new_batch_id
                                    AND NVL(xis.ERROR_TYPE, 'A') = 'ERR_INT') LOOP
      g_retcode := 1;
      UPDATE xxar_invoices_stg
         SET process_flag      = 'E',
             ERROR_TYPE        = 'ERR_INT',
             run_sequence_id   = g_new_run_seq_id,
             last_update_date  = g_sysdate,
             last_updated_by   = g_last_updated_by,
             last_update_login = g_login_id,
             request_id        = g_request_id -- added for v1.48, Defect# 9376
       WHERE leg_customer_trx_id = r_createline_err_rec.leg_customer_trx_id
         AND batch_id = g_new_batch_id;
      --Ver 1.5 Changes for DFF rationalization start
      /*       DELETE FROM ra_interface_lines_all
           --Ver 1.5 Changes for DFF rationalization start
      --               WHERE interface_line_attribute14 = r_createline_err_rec.leg_customer_trx_id;
          WHERE cust_trx_type_id = r_createline_err_rec.transaction_type_id
            AND trx_number = r_createline_err_rec.trx_number
            AND org_id = r_createline_err_rec.org_id;

      */
      --Ver 1.5 Changes for DFF rationalization end
      l_err_code := 'ETN_INVOICE_ERROR';
      l_err_msg  := 'Error : Erroring out remaining lines since one of the lines is in error while inserting in ra_interface_lines_all';
      print_log_message('For legacy transaction number: ' ||
                        r_createline_err_rec.leg_trx_number);
      print_log_message(l_err_msg);
      log_errors(
                 --   pin_transaction_id           =>  r_dist_err_rec.interface_txn_id
                 piv_error_type          => 'ERR_INT',
                 piv_source_column_name  => 'TRX_NUMBER',
                 piv_source_column_value => r_createline_err_rec.leg_trx_number,
                 piv_error_code          => l_err_code,
                 piv_error_message       => l_err_msg,
                 pov_return_status       => l_log_ret_status,
                 pov_error_msg           => l_log_err_msg);
    END LOOP;
    COMMIT;
    --Ver 1.5 Changes for DFF rationalization start
    FOR r_rila_err_rec IN (SELECT DISTINCT xis.interface_line_attribute15
                             FROM xxar_invoices_stg xis
                            WHERE xis.process_flag IN ('E')
                              AND xis.batch_id = g_new_batch_id
                              AND NVL(xis.ERROR_TYPE, 'A') = 'ERR_INT') LOOP
      DELETE FROM ra_interface_lines_all
       WHERE interface_line_attribute15 =
             r_rila_err_rec.interface_line_attribute15;
    END LOOP;
    COMMIT;
    --Ver 1.5 Changes for DFF rationalization end
    FOR create_dist_rec IN create_dist_cur LOOP
      BEGIN
        INSERT INTO apps.ra_interface_distributions_all
          (amount,
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
          ,
           creation_date,
           created_by,
           last_update_date,
           last_updated_by,
           last_update_login)
        VALUES
          (
           --ver1.21.1 changes start
           --create_dist_rec.accounted_amount
           create_dist_rec.dist_amount
           --ver1.21.1 changes end
          ,
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

           */
           --Ver 1.5 Commented for DFF rationalization end
           create_dist_rec.interface_line_attribute15,
           create_dist_rec.account_class,
           create_dist_rec.PERCENT
           --,xxar_interface_line_s.currval
          ,
           g_sysdate,
           apps.fnd_global.user_id,
           g_sysdate,
           apps.fnd_global.user_id,
           apps.fnd_global.login_id);
        update_dist_status(pin_interface_txn_id => create_dist_rec.interface_txn_id,
                           piv_process_flag     => 'P',
                           piv_err_type         => NULL,
                           pov_return_status    => l_upd_ret_status,
                           pov_error_code       => l_err_code,
                           pov_error_message    => l_err_msg);
      EXCEPTION
        WHEN OTHERS THEN
          g_retcode  := 1;
          l_err_code := 'ETN_AR_CREATE_EXCEPTION';
          l_err_msg  := 'Error : Exception in create_invoice Procedure for distributions. ' ||
                        SUBSTR(SQLERRM, 1, 150);
          log_errors(
                     -- pin_interface_txn_id      => create_dist_rec.interface_txn_id,
                     -- , piv_source_column_name     =>  'Legacy link_to_customer_trx_line_id'
                     --  , piv_source_column_value    =>  val_inv_det_rec(l_line_cnt).leg_link_to_cust_trx_line_id
                     piv_error_type    => 'ERR_INT',
                     piv_error_code    => l_err_code,
                     piv_error_message => l_err_msg,
                     piv_source_table  => 'XXAR_INVOICES_DIST_STG',
                     pov_return_status => l_log_ret_status,
                     pov_error_msg     => l_log_err_msg);
          update_dist_status(pin_interface_txn_id => create_dist_rec.interface_txn_id,
                             piv_process_flag     => 'E',
                             piv_err_type         => 'ERR_INT',
                             pov_return_status    => l_upd_ret_status,
                             pov_error_code       => l_err_code,
                             pov_error_message    => l_err_msg);
      END;
    END LOOP;
    FOR r_createdist_err_rec IN (SELECT DISTINCT xds.leg_customer_trx_id,
                                                 xds.process_flag
                                   FROM xxar_invoices_dist_stg xds
                                  WHERE xds.process_flag IN ('V', 'E')
                                    AND xds.batch_id = g_new_batch_id
                                    AND DECODE(xds.process_flag,
                                               'E',
                                               NVL(xds.ERROR_TYPE, 'A'),
                                               'ERR_INT') = 'ERR_INT')
    --AND NVL(xds.leg_account_class, 'A') <> 'ROUND') --performance
     LOOP
      UPDATE xxar_invoices_dist_stg
         SET process_flag      = 'E',
             ERROR_TYPE        = 'ERR_INT',
             run_sequence_id   = g_new_run_seq_id,
             last_update_date  = g_sysdate,
             last_updated_by   = g_last_updated_by,
             last_update_login = g_login_id,
             request_id        = g_request_id -- Added for v1.48, Defect# 9376
       WHERE leg_customer_trx_id = r_createdist_err_rec.leg_customer_trx_id
         AND batch_id = g_new_batch_id;
      g_retcode := 1;

      -- v1.45 changes started
      /** DELETE FROM ra_interface_distributions_all
       WHERE interface_line_attribute14 =
             r_createdist_err_rec.leg_customer_trx_id;
      DELETE FROM ra_interface_lines_all
       WHERE interface_line_attribute14 =
             r_createdist_err_rec.leg_customer_trx_id; **/
      -- v1.45 changes ended

      IF r_createdist_err_rec.process_flag <> 'V' THEN
        UPDATE xxar_invoices_stg
           SET process_flag      = 'E',
               ERROR_TYPE        = 'ERR_INT',
               run_sequence_id   = g_new_run_seq_id,
               last_update_date  = g_sysdate,
               last_updated_by   = g_last_updated_by,
               last_update_login = g_login_id,
               request_id        = g_request_id -- Added for v1.48, Defect# 9376
         WHERE leg_customer_trx_id =
               r_createdist_err_rec.leg_customer_trx_id
           AND batch_id = g_new_batch_id;
        l_err_code := 'ETN_DISTRIBUTION_ERROR';
        l_err_msg  := 'Error : Erroring out invoice lines since distribution is in error while inserting in ra_interface_distributions_all';
        -- print_log_message ('For legacy transaction number: '||r_createdist_err_rec.leg_trx_number);
        print_log_message(l_err_msg);
        log_errors(
                   --   pin_transaction_id           =>  r_dist_err_rec.interface_txn_id
                   piv_error_type          => 'ERR_INT',
                   piv_source_column_name  => 'LEGACY_CUSTOMER_TRX_ID',
                   piv_source_column_value => r_createdist_err_rec.leg_customer_trx_id,
                   piv_error_code          => l_err_code,
                   piv_error_message       => l_err_msg,
                   pov_return_status       => l_log_ret_status,
                   pov_error_msg           => l_log_err_msg);
      END IF;
      l_err_code := 'ETN_INVOICE_ERROR';
      IF r_createdist_err_rec.process_flag = 'V' THEN
        l_err_msg := 'Error : Erroring distribution since corresponding invoice line in error while inserting in ra_interface_lines_all ';
      ELSE
        l_err_msg := 'Error : Erroring distribution since another related distribution in error while inserting in ra_interface_distributions_all';
      END IF;
      -- print_log_message ('For legacy transaction number: '||r_createdist_err_rec.leg_trx_number);
      print_log_message(l_err_msg);
      log_errors(
                 --   pin_transaction_id           =>  r_dist_err_rec.interface_txn_id
                 piv_error_type          => 'ERR_INT',
                 piv_source_column_name  => 'LEGACY_CUSTOMER_TRX_ID',
                 piv_source_column_value => r_createdist_err_rec.leg_customer_trx_id,
                 piv_source_table        => 'XXAR_INVOICES_DIST_STG',
                 piv_error_code          => l_err_code,
                 piv_error_message       => l_err_msg,
                 pov_return_status       => l_log_ret_status,
                 pov_error_msg           => l_log_err_msg);
    END LOOP;
    COMMIT;
    FOR r_rida_err_rec IN (SELECT DISTINCT xds.interface_line_attribute15
                             FROM xxar_invoices_dist_stg xds
                            WHERE xds.process_flag = 'E'
                              AND xds.batch_id = g_new_batch_id
                              AND NVL(xds.ERROR_TYPE, 'A') = 'ERR_INT')
    --AND NVL(xds.leg_account_class, 'A') <> 'ROUND') --performance
     LOOP

      DELETE FROM ra_interface_distributions_all
       WHERE interface_line_attribute15 =
             r_rida_err_rec.interface_line_attribute15
         AND interface_line_context = 'Eaton'; -- added for v1.45
      DELETE FROM ra_interface_lines_all
       WHERE interface_line_attribute15 =
             r_rida_err_rec.interface_line_attribute15
         AND interface_line_context = 'Eaton'; -- added for v1.45

    END LOOP;
    COMMIT;
  EXCEPTION
    WHEN OTHERS THEN
      g_retcode  := 2;
      l_err_code := 'ETN_AR_PROCEDURE_EXCEPTION';
      l_err_msg  := 'Error : Exception in create_invoice Procedure. ' ||
                    SUBSTR(SQLERRM, 1, 150);
      log_errors(
                 -- pin_transaction_id           =>  pin_trx_id
                 -- , piv_source_column_name     =>  'Legacy link_to_customer_trx_line_id'
                 --  , piv_source_column_value    =>  val_inv_det_rec(l_line_cnt).leg_link_to_cust_trx_line_id
                 piv_error_type    => 'ERR_INT',
                 piv_error_code    => l_err_code,
                 piv_error_message => l_err_msg,
                 pov_return_status => l_log_ret_status,
                 pov_error_msg     => l_log_err_msg);
  END create_invoice;

  /*
   --
   -- ========================
   -- Procedure: CREATE_INVOICE
   -- =============================================================================
   --   This procedure insert records in interface table
   -- =============================================================================
   --
   PROCEDURE create_invoice IS
     l_err_code       VARCHAR2(40);
      l_err_msg        VARCHAR2(2000);
      l_upd_ret_status VARCHAR2(50) := NULL;
      l_log_ret_status VARCHAR2(50) := NULL;
      l_log_err_msg    VARCHAR2(2000);
      CURSOR create_inv_cur IS
         SELECT  interface_line_context
        ,link_to_line_context
        ,link_to_line_attribute1
        ,link_to_line_attribute2
        ,link_to_line_attribute3
        ,link_to_line_attribute4
        ,link_to_line_attribute5
        ,link_to_line_attribute6
        ,link_to_line_attribute7
        ,link_to_line_attribute8
        ,link_to_line_attribute9
        ,link_to_line_attribute10
        ,link_to_line_attribute11
        ,link_to_line_attribute12
        ,link_to_line_attribute13
        ,link_to_line_attribute14
        ,link_to_line_attribute15
        ,batch_source_name
        ,set_of_books_id
        ,memo_line_id
        ,line_type
        ,description
        ,currency_code
        ,cust_trx_type_name
        ,transaction_type_id
        ,term_name
        ,term_id
        ,system_bill_customer_ref
        ,system_bill_customer_id
        ,system_bill_address_ref
        ,system_bill_address_id
        ,system_ship_address_id
        ,system_ship_address_id
        ,system_bill_customer_id

        ,purchase_order
        ,reason_code
        ,header_attribute_category
        ,header_attribute1
        ,header_attribute4
        ,header_attribute8
        ,attribute_category
        ,attribute1
        ,attribute2
        ,attribute3
        ,attribute4
        ,attribute5
        ,attribute6
        ,attribute7
        ,attribute8
        ,attribute9
        ,attribute10
        ,attribute11
        ,attribute12
        ,attribute13
        ,attribute14
        ,attribute15
        ,header_gdf_attr_category
        ,header_gdf_attribute1
        ,header_gdf_attribute2
        ,header_gdf_attribute3
        ,header_gdf_attribute4
        ,header_gdf_attribute5
        ,header_gdf_attribute6
        ,header_gdf_attribute7
        ,header_gdf_attribute8
        ,header_gdf_attribute9
        ,header_gdf_attribute10
        ,header_gdf_attribute11
        ,header_gdf_attribute12
        ,header_gdf_attribute13
        ,header_gdf_attribute14
        ,header_gdf_attribute15
        ,header_gdf_attribute16
        ,header_gdf_attribute17
        ,header_gdf_attribute18
        ,header_gdf_attribute19
        ,header_gdf_attribute20
        ,header_gdf_attribute21
        ,header_gdf_attribute22
        ,header_gdf_attribute23
        ,header_gdf_attribute24
        ,header_gdf_attribute25
        ,header_gdf_attribute26
        ,header_gdf_attribute27
        ,header_gdf_attribute28
        ,header_gdf_attribute29
        ,header_gdf_attribute30
        ,line_gdf_attr_category
        ,line_gdf_attribute1
        ,line_gdf_attribute2
        ,line_gdf_attribute3
        ,line_gdf_attribute4
        ,line_gdf_attribute5
        ,line_gdf_attribute6
        ,line_gdf_attribute7
        ,line_gdf_attribute8
        ,line_gdf_attribute9
        ,line_gdf_attribute10
        ,line_gdf_attribute11
        ,line_gdf_attribute12
        ,line_gdf_attribute13
        ,line_gdf_attribute14
        ,line_gdf_attribute15
        ,line_gdf_attribute16
        ,line_gdf_attribute17
        ,line_gdf_attribute18
        ,line_gdf_attribute19
        ,line_gdf_attribute20
        ,trx_date
        ,gl_date
        ,trx_number
        ,line_number
        ,tax_code
        ,tax_regime_code
        ,tax_rate_code
        ,tax
        ,tax_status_code
        ,tax_jurisdiction_code
        ,amount_includes_tax_flag
        ,taxable_flag
        ,sales_order_date
        ,sales_order
        ,uom_name
        ,ussgl_transaction_code_context
        ,internal_notes
        ,ship_date_actual
        ,fob_point
        ,ship_via
        ,waybill_number
        ,sales_order_line
        ,sales_order_source
        ,sales_order_revision
        ,purchase_order_revision
        ,agreement_id
        ,purchase_order_date
        ,invoicing_rule_id
        ,org_id
        ,quantity
        ,quantity_ordered
        ,unit_selling_price
        ,unit_standard_price
        ,tax_rate
        ,interface_line_attribute1
        ,interface_line_attribute15
        ,line_amount
        ,conversion_type
        ,conversion_rate
        ,conversion_date
        ,comments
           FROM xxar_invoices_stg
          WHERE process_flag = 'V'
            AND batch_id = g_new_batch_id
      AND leg_line_type <> 'TAX';
     UNION
         SELECT  interface_line_context
        ,link_to_line_context
        ,link_to_line_attribute1
        ,link_to_line_attribute2
        ,link_to_line_attribute3
        ,link_to_line_attribute4
        ,link_to_line_attribute5
        ,link_to_line_attribute6
        ,link_to_line_attribute7
        ,link_to_line_attribute8
        ,link_to_line_attribute9
        ,link_to_line_attribute10
        ,link_to_line_attribute11
        ,link_to_line_attribute12
        ,link_to_line_attribute13
        ,link_to_line_attribute14
        ,link_to_line_attribute15
        ,batch_source_name
        ,set_of_books_id
        ,memo_line_id
        ,line_type
        ,description
        ,currency_code
        ,cust_trx_type_name
        ,transaction_type_id
        ,term_name
        ,term_id
        ,system_bill_customer_ref
        ,system_bill_customer_id
        ,system_bill_address_ref
        ,system_bill_address_id
        ,system_ship_address_id
        ,system_ship_address_id
        ,system_bill_customer_id
        ,purchase_order
        ,reason_code
        ,header_attribute_category
        ,header_attribute1
        ,header_attribute4
        ,header_attribute8
        ,attribute_category
        ,attribute1
        ,attribute2
        ,attribute3
        ,attribute4
        ,attribute5
        ,attribute6
        ,attribute7
        ,attribute8
        ,attribute9
        ,attribute10
        ,attribute11
        ,attribute12
        ,attribute13
        ,attribute14
        ,attribute15
        ,header_gdf_attr_category
        ,header_gdf_attribute1
        ,header_gdf_attribute2
        ,header_gdf_attribute3
        ,header_gdf_attribute4
        ,header_gdf_attribute5
        ,header_gdf_attribute6
        ,header_gdf_attribute7
        ,header_gdf_attribute8
        ,header_gdf_attribute9
        ,header_gdf_attribute10
        ,header_gdf_attribute11
        ,header_gdf_attribute12
        ,header_gdf_attribute13
        ,header_gdf_attribute14
        ,header_gdf_attribute15
        ,header_gdf_attribute16
        ,header_gdf_attribute17
        ,header_gdf_attribute18
        ,header_gdf_attribute19
        ,header_gdf_attribute20
        ,header_gdf_attribute21
        ,header_gdf_attribute22
        ,header_gdf_attribute23
        ,header_gdf_attribute24
        ,header_gdf_attribute25
        ,header_gdf_attribute26
        ,header_gdf_attribute27
        ,header_gdf_attribute28
        ,header_gdf_attribute29
        ,header_gdf_attribute30
        ,line_gdf_attr_category
        ,line_gdf_attribute1
        ,line_gdf_attribute2
        ,line_gdf_attribute3
        ,line_gdf_attribute4
        ,line_gdf_attribute5
        ,line_gdf_attribute6
        ,line_gdf_attribute7
        ,line_gdf_attribute8
        ,line_gdf_attribute9
        ,line_gdf_attribute10
        ,line_gdf_attribute11
        ,line_gdf_attribute12
        ,line_gdf_attribute13
        ,line_gdf_attribute14
        ,line_gdf_attribute15
        ,line_gdf_attribute16
        ,line_gdf_attribute17
        ,line_gdf_attribute18
        ,line_gdf_attribute19
        ,line_gdf_attribute20
        ,trx_date
        ,gl_date
        ,trx_number
        ,line_number
        ,tax_code
        ,tax_regime_code
        ,tax_rate_code
        ,tax
        ,tax_status_code
        ,tax_jurisdiction_code
        ,amount_includes_tax_flag
        ,taxable_flag
        ,sales_order_date
        ,sales_order
        ,uom_name
        ,ussgl_transaction_code_context
        ,internal_notes
        ,ship_date_actual
        ,fob_point
        ,ship_via
        ,waybill_number
        ,sales_order_line
        ,sales_order_source
        ,sales_order_revision
        ,purchase_order_revision
        ,agreement_id
        ,purchase_order_date
        ,invoicing_rule_id
        ,org_id
        ,conversion_type
        ,conversion_rate
        ,conversion_date
        ,quantity
        ,quantity_ordered
        ,unit_selling_price
        ,unit_standard_price
        ,tax_rate
        ,interface_line_attribute1
        ,interface_line_attribute15
        ,line_amount
        ,DECODE(cnt,1,comments,'Multiple Comments') comments
      FROM
    (
     SELECT  interface_line_context
        ,link_to_line_context
        ,link_to_line_attribute1
        ,link_to_line_attribute2
        ,link_to_line_attribute3
        ,link_to_line_attribute4
        ,link_to_line_attribute5
        ,link_to_line_attribute6
        ,link_to_line_attribute7
        ,link_to_line_attribute8
        ,link_to_line_attribute9
        ,link_to_line_attribute10
        ,link_to_line_attribute11
        ,link_to_line_attribute12
        ,link_to_line_attribute13
        ,link_to_line_attribute14
        ,link_to_line_attribute15
        ,batch_source_name
        ,set_of_books_id
        ,memo_line_id
        ,line_type
        ,description
        ,currency_code
        ,cust_trx_type_name
        ,transaction_type_id
        ,term_name
        ,term_id
        ,system_bill_customer_ref
        ,system_bill_customer_id
        ,system_bill_address_ref
        ,system_bill_address_id
        ,system_ship_address_id
        ,system_ship_address_id
        ,system_bill_customer_id
        ,purchase_order
        ,reason_code
        ,header_attribute_category
        ,header_attribute1
        ,header_attribute4
        ,header_attribute8
        ,attribute_category
        ,attribute1
        ,attribute2
        ,attribute3
        ,attribute4
        ,attribute5
        ,attribute6
        ,attribute7
        ,attribute8
        ,attribute9
        ,attribute10
        ,attribute11
        ,attribute12
        ,attribute13
        ,attribute14
        ,attribute15
        ,header_gdf_attr_category
        ,header_gdf_attribute1
        ,header_gdf_attribute2
        ,header_gdf_attribute3
        ,header_gdf_attribute4
        ,header_gdf_attribute5
        ,header_gdf_attribute6
        ,header_gdf_attribute7
        ,header_gdf_attribute8
        ,header_gdf_attribute9
        ,header_gdf_attribute10
        ,header_gdf_attribute11
        ,header_gdf_attribute12
        ,header_gdf_attribute13
        ,header_gdf_attribute14
        ,header_gdf_attribute15
        ,header_gdf_attribute16
        ,header_gdf_attribute17
        ,header_gdf_attribute18
        ,header_gdf_attribute19
        ,header_gdf_attribute20
        ,header_gdf_attribute21
        ,header_gdf_attribute22
        ,header_gdf_attribute23
        ,header_gdf_attribute24
        ,header_gdf_attribute25
        ,header_gdf_attribute26
        ,header_gdf_attribute27
        ,header_gdf_attribute28
        ,header_gdf_attribute29
        ,header_gdf_attribute30
        ,line_gdf_attr_category
        ,line_gdf_attribute1
        ,line_gdf_attribute2
        ,line_gdf_attribute3
        ,line_gdf_attribute4
        ,line_gdf_attribute5
        ,line_gdf_attribute6
        ,line_gdf_attribute7
        ,line_gdf_attribute8
        ,line_gdf_attribute9
        ,line_gdf_attribute10
        ,line_gdf_attribute11
        ,line_gdf_attribute12
        ,line_gdf_attribute13
        ,line_gdf_attribute14
        ,line_gdf_attribute15
        ,line_gdf_attribute16
        ,line_gdf_attribute17
        ,line_gdf_attribute18
        ,line_gdf_attribute19
        ,line_gdf_attribute20
        ,trx_date
        ,gl_date
        ,trx_number
        ,line_number
        ,tax_code
        ,tax_regime_code
        ,tax_rate_code
        ,tax
        ,tax_status_code
        ,tax_jurisdiction_code
        ,amount_includes_tax_flag
        ,taxable_flag
        ,sales_order_date
        ,sales_order
        ,uom_name
        ,ussgl_transaction_code_context
        ,internal_notes
        ,ship_date_actual
        ,fob_point
        ,ship_via
        ,waybill_number
        ,sales_order_line
        ,sales_order_source
        ,sales_order_revision
        ,purchase_order_revision
        ,agreement_id
        ,purchase_order_date
        ,invoicing_rule_id
        ,org_id
        ,conversion_type
        ,conversion_rate
        ,conversion_date
        ,SUM(quantity) quantity
        ,SUM(quantity_ordered)        quantity_ordered
        ,SUM(unit_selling_price)          unit_selling_price
        ,SUM(unit_standard_price)         unit_standard_price
        ,SUM(tax_rate )                   tax_rate
        ,MAX(interface_line_attribute1)   interface_line_attribute1
        ,MAX(interface_line_attribute15)  interface_line_attribute15
        ,SUM(line_amount)                 line_amount
        ,MAX(comments)                    comments
           FROM xxar_invoices_stg
          WHERE process_flag = 'V'
            AND batch_id = g_new_batch_id
      AND leg_line_type = 'TAX');
      --       AND    leg_operating_unit = NVL( g_leg_operating_unit, leg_operating_unit);
      --   AND    run_sequence_id = g_new_run_seq_id
      CURSOR create_dist_cur IS
         SELECT xds.*
           FROM xxar_invoices_dist_stg xds
               ,xxar_invoices_stg      xis
          WHERE xds.leg_customer_trx_id = xis.leg_customer_trx_id
            AND xds.leg_cust_trx_line_id = xis.leg_cust_trx_line_id
            AND NVL(xds.leg_account_class, 'A') <> 'REC'
               --        AND NVL(xds.leg_account_class,'A') <> 'ROUND' --performance
            AND xds.process_flag = 'V'
            AND xds.batch_id = g_new_batch_id
            AND xis.process_flag = 'P'
      AND NVL(xds.leg_account_class, 'A') <> 'TAX'
         UNION
         SELECT xds.*
           FROM xxar_invoices_dist_stg xds
               ,xxar_invoices_stg      xis
          WHERE xds.leg_customer_trx_id = xis.leg_customer_trx_id
            AND xds.leg_account_class = 'REC'
            AND xds.process_flag = 'V'
            AND xds.batch_id = g_new_batch_id
            AND xis.process_flag = 'P';
     CURSOR create_dist_cur_tax IS
         SELECT xds.*
           FROM xxar_invoices_dist_stg xds
               ,xxar_invoices_stg      xis
          WHERE xds.leg_customer_trx_id = xis.leg_customer_trx_id
            AND xds.leg_cust_trx_line_id = xis.leg_cust_trx_line_id
            AND NVL(xds.leg_account_class, 'A') <> 'REC'
               --        AND NVL(xds.leg_account_class,'A') <> 'ROUND' --performance
            AND xds.process_flag = 'V'
            AND xds.batch_id = g_new_batch_id
            AND xis.process_flag = 'P'
      AND NVL(xds.leg_account_class, 'A') = 'TAX';
  BEGIN
      FOR create_inv_rec IN create_inv_cur
      LOOP
         BEGIN
            INSERT INTO apps.ra_interface_lines_all
               (interface_line_context
               ,interface_line_attribute1
               ,
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
                 --Ver 1.5 Commented for DFF rationalization end
                interface_line_attribute15
               ,link_to_line_context
               ,link_to_line_attribute1
               ,link_to_line_attribute2
               ,link_to_line_attribute3
               ,link_to_line_attribute4
               ,link_to_line_attribute5
               ,link_to_line_attribute6
               ,link_to_line_attribute7
               ,link_to_line_attribute8
               ,link_to_line_attribute9
               ,link_to_line_attribute10
               ,link_to_line_attribute11
               ,link_to_line_attribute12
               ,link_to_line_attribute13
               ,link_to_line_attribute14
               ,link_to_line_attribute15
               ,batch_source_name
               ,set_of_books_id
               ,memo_line_id
               ,line_type
               ,description
               ,currency_code
               ,amount
               ,cust_trx_type_name
               ,cust_trx_type_id
               ,term_name
               ,term_id
               ,orig_system_bill_customer_ref
               ,orig_system_bill_customer_id
               ,orig_system_bill_address_ref
               ,orig_system_bill_address_id
               ,
                --orig_system_ship_customer_ref,
                orig_system_ship_address_id
               ,orig_system_ship_customer_id
               ,conversion_type
               ,conversion_rate
               ,conversion_date
               ,purchase_order
               ,reason_code
               ,comments
               ,header_attribute_category
               ,header_attribute1
                --, header_attribute2
                --, header_attribute3
               ,header_attribute4
                --, header_attribute5
                --, header_attribute6
                --, header_attribute7
               ,header_attribute8
                --, header_attribute9
                --, header_attribute10
                --, header_attribute11
                --, header_attribute12
                --, header_attribute13
                --, header_attribute14
                --, header_attribute15
               ,attribute_category
               ,attribute1
               ,attribute2
               ,attribute3
               ,attribute4
               ,attribute5
               ,attribute6
               ,attribute7
               ,attribute8
               ,attribute9
               ,attribute10
               ,attribute11
               ,attribute12
               ,attribute13
               ,attribute14
               ,attribute15
               ,header_gdf_attr_category
               ,header_gdf_attribute1
               ,header_gdf_attribute2
               ,header_gdf_attribute3
               ,header_gdf_attribute4
               ,header_gdf_attribute5
               ,header_gdf_attribute6
               ,header_gdf_attribute7
               ,header_gdf_attribute8
               ,header_gdf_attribute9
               ,header_gdf_attribute10
               ,header_gdf_attribute11
               ,header_gdf_attribute12
               ,header_gdf_attribute13
               ,header_gdf_attribute14
               ,header_gdf_attribute15
               ,header_gdf_attribute16
               ,header_gdf_attribute17
               ,header_gdf_attribute18
               ,header_gdf_attribute19
               ,header_gdf_attribute20
               ,header_gdf_attribute21
               ,header_gdf_attribute22
               ,header_gdf_attribute23
               ,header_gdf_attribute24
               ,header_gdf_attribute25
               ,header_gdf_attribute26
               ,header_gdf_attribute27
               ,header_gdf_attribute28
               ,header_gdf_attribute29
               ,header_gdf_attribute30
               ,line_gdf_attr_category
               ,line_gdf_attribute1
               ,line_gdf_attribute2
               ,line_gdf_attribute3
               ,line_gdf_attribute4
               ,line_gdf_attribute5
               ,line_gdf_attribute6
               ,line_gdf_attribute7
               ,line_gdf_attribute8
               ,line_gdf_attribute9
               ,line_gdf_attribute10
               ,line_gdf_attribute11
               ,line_gdf_attribute12
               ,line_gdf_attribute13
               ,line_gdf_attribute14
               ,line_gdf_attribute15
               ,line_gdf_attribute16
               ,line_gdf_attribute17
               ,line_gdf_attribute18
               ,line_gdf_attribute19
               ,line_gdf_attribute20
               ,trx_date
               ,gl_date
               ,trx_number
               ,line_number
               ,tax_code
               ,tax_regime_code
               ,tax_rate_code
               ,tax
               ,tax_status_code
               ,tax_jurisdiction_code
               ,amount_includes_tax_flag
               ,taxable_flag
               ,sales_order_date
               ,sales_order
               ,uom_name
               ,ussgl_transaction_code_context
               ,internal_notes
               ,ship_date_actual
               ,fob_point
               ,ship_via
               ,waybill_number
               ,sales_order_line
               ,sales_order_source
               ,sales_order_revision
               ,purchase_order_revision
               ,
                --Ver1.7 changes start
                /*
                            purchase_order_date,
                agreement_name,
                            agreement_id,
                accounting_rule_id
               ,rule_start_date
               ,invoicing_rule_id
               ,
                --Ver1.7 changes end
                quantity
               ,quantity_ordered
               ,unit_selling_price
               ,unit_standard_price
               ,org_id
               ,creation_date
               ,created_by
               ,last_update_date
               ,last_updated_by
               ,last_update_login
               ,tax_rate ---1.4 Added by Rohit D for FOT
                )
            VALUES
               (create_inv_rec.interface_line_context
               ,create_inv_rec.interface_line_attribute1
               ,
                /*--Ver 1.5 Commented for DFF rationalization start
                create_inv_rec.interface_line_attribute2,
                                  create_inv_rec.interface_line_attribute3,
                                  create_inv_rec.interface_line_attribute4,
                                  create_inv_rec.interface_line_attribute5,
                                  create_inv_rec.interface_line_attribute6,
                                  create_inv_rec.interface_line_attribute7,
                                  create_inv_rec.interface_line_attribute8,
                                  create_inv_rec.interface_line_attribute9,
                                  create_inv_rec.interface_line_attribute10,
                                  create_inv_rec.interface_line_attribute11,
                                  create_inv_rec.interface_line_attribute12,
                                  create_inv_rec.interface_line_attribute13,
                                  create_inv_rec.interface_line_attribute14,
                 --Ver 1.5 Commented for DFF rationalization end
                create_inv_rec.interface_line_attribute15
               ,create_inv_rec.link_to_line_context
               ,create_inv_rec.link_to_line_attribute1
               ,create_inv_rec.link_to_line_attribute2
               ,create_inv_rec.link_to_line_attribute3
               ,create_inv_rec.link_to_line_attribute4
               ,create_inv_rec.link_to_line_attribute5
               ,create_inv_rec.link_to_line_attribute6
               ,create_inv_rec.link_to_line_attribute7
               ,create_inv_rec.link_to_line_attribute8
               ,create_inv_rec.link_to_line_attribute9
               ,create_inv_rec.link_to_line_attribute10
               ,create_inv_rec.link_to_line_attribute11
               ,create_inv_rec.link_to_line_attribute12
               ,create_inv_rec.link_to_line_attribute13
               ,create_inv_rec.link_to_line_attribute14
               ,create_inv_rec.link_to_line_attribute15
               ,create_inv_rec.batch_source_name
               ,create_inv_rec.set_of_books_id
               ,create_inv_rec.memo_line_id
               ,create_inv_rec.line_type
               ,create_inv_rec.description
               ,create_inv_rec.currency_code
               ,create_inv_rec.line_amount
               ,create_inv_rec.cust_trx_type_name
               ,create_inv_rec.transaction_type_id
               ,create_inv_rec.term_name
               ,create_inv_rec.term_id
               ,create_inv_rec.system_bill_customer_ref
               ,create_inv_rec.system_bill_customer_id
               ,create_inv_rec.system_bill_address_ref
               ,create_inv_rec.system_bill_address_id
               ,create_inv_rec.system_ship_address_id
               ,DECODE(create_inv_rec.system_ship_address_id, NULL, NULL,
                       create_inv_rec.system_bill_customer_id)
               ,create_inv_rec.conversion_type
               ,create_inv_rec.conversion_rate
               ,create_inv_rec.conversion_date
               ,create_inv_rec.purchase_order
               ,create_inv_rec.reason_code
               ,create_inv_rec.comments
               ,create_inv_rec.header_attribute_category
               ,create_inv_rec.header_attribute1
               ,
                --ver 1.5 changes start
                --create_inv_rec.header_attribute2,
                --create_inv_rec.header_attribute3,
                create_inv_rec.header_attribute4
               ,
                --create_inv_rec.header_attribute5,
                --create_inv_rec.header_attribute6,
                --create_inv_rec.header_attribute7,
                create_inv_rec.header_attribute8
               ,
                --create_inv_rec.header_attribute9,
                --create_inv_rec.header_attribute10,
                --create_inv_rec.header_attribute11,
                --create_inv_rec.header_attribute12,
                --create_inv_rec.header_attribute13,
                --create_inv_rec.header_attribute14,
                --create_inv_rec.header_attribute15,
                --ver 1.5 changes end
                create_inv_rec.attribute_category
               ,create_inv_rec.attribute1
               ,create_inv_rec.attribute2
               ,create_inv_rec.attribute3
               ,create_inv_rec.attribute4
               ,create_inv_rec.attribute5
               ,create_inv_rec.attribute6
               ,create_inv_rec.attribute7
               ,create_inv_rec.attribute8
               ,create_inv_rec.attribute9
               ,create_inv_rec.attribute10
               ,create_inv_rec.attribute11
               ,create_inv_rec.attribute12
               ,create_inv_rec.attribute13
               ,create_inv_rec.attribute14
               ,create_inv_rec.attribute15
               ,create_inv_rec.header_gdf_attr_category
               ,create_inv_rec.header_gdf_attribute1
               ,create_inv_rec.header_gdf_attribute2
               ,create_inv_rec.header_gdf_attribute3
               ,create_inv_rec.header_gdf_attribute4
               ,create_inv_rec.header_gdf_attribute5
               ,create_inv_rec.header_gdf_attribute6
               ,create_inv_rec.header_gdf_attribute7
               ,create_inv_rec.header_gdf_attribute8
               ,create_inv_rec.header_gdf_attribute9
               ,create_inv_rec.header_gdf_attribute10
               ,create_inv_rec.header_gdf_attribute11
               ,create_inv_rec.header_gdf_attribute12
               ,create_inv_rec.header_gdf_attribute13
               ,create_inv_rec.header_gdf_attribute14
               ,create_inv_rec.header_gdf_attribute15
               ,create_inv_rec.header_gdf_attribute16
               ,create_inv_rec.header_gdf_attribute17
               ,create_inv_rec.header_gdf_attribute18
               ,create_inv_rec.header_gdf_attribute19
               ,create_inv_rec.header_gdf_attribute20
               ,create_inv_rec.header_gdf_attribute21
               ,create_inv_rec.header_gdf_attribute22
               ,create_inv_rec.header_gdf_attribute23
               ,create_inv_rec.header_gdf_attribute24
               ,create_inv_rec.header_gdf_attribute25
               ,create_inv_rec.header_gdf_attribute26
               ,create_inv_rec.header_gdf_attribute27
               ,create_inv_rec.header_gdf_attribute28
               ,create_inv_rec.header_gdf_attribute29
               ,create_inv_rec.header_gdf_attribute30
               ,create_inv_rec.line_gdf_attr_category
               ,create_inv_rec.line_gdf_attribute1
               ,create_inv_rec.line_gdf_attribute2
               ,create_inv_rec.line_gdf_attribute3
               ,create_inv_rec.line_gdf_attribute4
               ,create_inv_rec.line_gdf_attribute5
               ,create_inv_rec.line_gdf_attribute6
               ,create_inv_rec.line_gdf_attribute7
               ,create_inv_rec.line_gdf_attribute8
               ,create_inv_rec.line_gdf_attribute9
               ,create_inv_rec.line_gdf_attribute10
               ,create_inv_rec.line_gdf_attribute11
               ,create_inv_rec.line_gdf_attribute12
               ,create_inv_rec.line_gdf_attribute13
               ,create_inv_rec.line_gdf_attribute14
               ,create_inv_rec.line_gdf_attribute15
               ,create_inv_rec.line_gdf_attribute16
               ,create_inv_rec.line_gdf_attribute17
               ,create_inv_rec.line_gdf_attribute18
               ,create_inv_rec.line_gdf_attribute19
               ,create_inv_rec.line_gdf_attribute20
               ,create_inv_rec.trx_date
               ,create_inv_rec.gl_date
               ,create_inv_rec.trx_number
               ,create_inv_rec.line_number
               ,create_inv_rec.tax_code
               ,create_inv_rec.tax_regime_code
               ,create_inv_rec.tax_rate_code
               ,create_inv_rec.tax
               ,create_inv_rec.tax_status_code
               ,create_inv_rec.tax_jurisdiction_code
               ,create_inv_rec.amount_includes_tax_flag
               ,create_inv_rec.taxable_flag
               ,create_inv_rec.sales_order_date
               ,create_inv_rec.sales_order
               ,create_inv_rec.uom_name
               ,create_inv_rec.ussgl_transaction_code_context
               ,create_inv_rec.internal_notes
               ,create_inv_rec.ship_date_actual
               ,create_inv_rec.fob_point
               ,create_inv_rec.ship_via
               ,create_inv_rec.waybill_number
               ,create_inv_rec.sales_order_line
               ,create_inv_rec.sales_order_source
               ,create_inv_rec.sales_order_revision
               ,create_inv_rec.purchase_order_revision
               ,
                --ver1.7 changes start
                /*                         create_inv_rec.purchase_order_date,
                create_inv_rec.agreement_name,
                            create_inv_rec.agreement_id,
                create_inv_rec.agreement_id
               ,create_inv_rec.purchase_order_date
               ,create_inv_rec.invoicing_rule_id
               ,
                --ver1.7 changes end
                create_inv_rec.quantity
               ,create_inv_rec.quantity_ordered
               ,create_inv_rec.unit_selling_price
               ,create_inv_rec.unit_standard_price
               ,create_inv_rec.org_id
               ,SYSDATE
               ,apps.fnd_global.user_id
               ,SYSDATE
               ,apps.fnd_global.user_id
               ,apps.fnd_global.login_id
               ,create_inv_rec.tax_rate ---1.4 Added by Rohit D for FOT
                );
            update_status(pin_interface_txn_id => create_inv_rec.interface_txn_id,
                          piv_process_flag => 'P', piv_err_type => NULL,
                          pov_return_status => l_upd_ret_status
                           -- OUT
                         , pov_error_code => l_err_code
                           -- OUT
                         , pov_error_message => l_err_msg
                           -- OUT
                          );
         EXCEPTION
            WHEN OTHERS THEN
               g_retcode  := 1;
               l_err_code := 'ETN_AR_CREATE_EXCEPTION';
               l_err_msg  := 'Error : Exception in create_invoice Procedure for invoice lines. ' ||
                             SUBSTR(SQLERRM, 1, 150);
               log_errors(pin_transaction_id => create_inv_rec.interface_txn_id,
                          piv_source_column_name => 'LEGACY_CUSTOMER_TRX_ID',
                          piv_source_column_value => create_inv_rec.leg_customer_trx_id,
                          piv_error_type => 'ERR_INT',
                          piv_error_code => l_err_code,
                          piv_error_message => l_err_msg,
                          pov_return_status => l_log_ret_status,
                          pov_error_msg => l_log_err_msg);
               update_status(pin_interface_txn_id => create_inv_rec.interface_txn_id,
                             piv_process_flag => 'E',
                             piv_err_type => 'ERR_INT',
                             pov_return_status => l_upd_ret_status
                              -- OUT
                            , pov_error_code => l_err_code
                              -- OUT
                            , pov_error_message => l_err_msg
                              -- OUT
                             );
         END;
      END LOOP;
      FOR r_createline_err_rec IN (SELECT DISTINCT xis.leg_customer_trx_id
                                                  ,xis.leg_trx_number
                                     FROM xxar_invoices_stg xis
                                    WHERE xis.process_flag IN ('E')
                                      AND xis.batch_id = g_new_batch_id
                                      AND NVL(xis.ERROR_TYPE, 'A') =
                                          'ERR_INT')
      LOOP
         g_retcode := 1;
         UPDATE xxar_invoices_stg
            SET process_flag      = 'E'
               ,ERROR_TYPE        = 'ERR_INT'
               ,run_sequence_id   = g_new_run_seq_id
               ,last_update_date  = SYSDATE
               ,last_updated_by   = g_last_updated_by
               ,last_update_login = g_login_id
          WHERE leg_customer_trx_id =
                r_createline_err_rec.leg_customer_trx_id
            AND batch_id = g_new_batch_id;
         --Ver 1.5 Changes for DFF rationalization start
         /*       DELETE FROM ra_interface_lines_all
              --Ver 1.5 Changes for DFF rationalization start
         --               WHERE interface_line_attribute14 = r_createline_err_rec.leg_customer_trx_id;
             WHERE cust_trx_type_id = r_createline_err_rec.transaction_type_id
               AND trx_number = r_createline_err_rec.trx_number
               AND org_id = r_createline_err_rec.org_id;
         --Ver 1.5 Changes for DFF rationalization end
         l_err_code := 'ETN_INVOICE_ERROR';
         l_err_msg  := 'Error : Erroring out remaining lines since one of the lines is in error while inserting in ra_interface_lines_all';
         print_log_message('For legacy transaction number: ' ||
                           r_createline_err_rec.leg_trx_number);
         print_log_message(l_err_msg);
         log_errors(
                    --   pin_transaction_id           =>  r_dist_err_rec.interface_txn_id
                    piv_error_type => 'ERR_INT',
                    piv_source_column_name => 'TRX_NUMBER',
                    piv_source_column_value => r_createline_err_rec.leg_trx_number,
                    piv_error_code => l_err_code,
                    piv_error_message => l_err_msg,
                    pov_return_status => l_log_ret_status,
                    pov_error_msg => l_log_err_msg);
      END LOOP;
      COMMIT;
      --Ver 1.5 Changes for DFF rationalization start
      FOR r_rila_err_rec IN (SELECT DISTINCT xis.interface_line_attribute15
                               FROM xxar_invoices_stg xis
                              WHERE xis.process_flag IN ('E')
                                AND xis.batch_id = g_new_batch_id
                                AND NVL(xis.ERROR_TYPE, 'A') = 'ERR_INT')
      LOOP
         DELETE FROM ra_interface_lines_all
          WHERE interface_line_attribute15 =
                r_rila_err_rec.interface_line_attribute15;
      END LOOP;
      COMMIT;
      --Ver 1.5 Changes for DFF rationalization end
      FOR create_dist_rec IN create_dist_cur
      LOOP
         BEGIN
            INSERT INTO apps.ra_interface_distributions_all
               (amount
               ,code_combination_id
               ,org_id
               ,segment1
               ,segment2
               ,segment3
               ,segment4
               ,segment5
               ,segment6
               ,segment7
               ,segment8
               ,segment9
               ,segment10
               ,interface_line_context
               ,interface_line_attribute1
               ,
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
                 --Ver 1.5 Commented for DFF rationalization end
                interface_line_attribute15
               ,account_class
               ,PERCENT
                --,interface_line_id
               ,creation_date
               ,created_by
               ,last_update_date
               ,last_updated_by
               ,last_update_login)
            VALUES
               (create_dist_rec.accounted_amount
               ,create_dist_rec.code_combination_id
               ,create_dist_rec.org_id
               ,create_dist_rec.dist_segment1
               ,create_dist_rec.dist_segment2
               ,create_dist_rec.dist_segment3
               ,create_dist_rec.dist_segment4
               ,create_dist_rec.dist_segment5
               ,create_dist_rec.dist_segment6
               ,create_dist_rec.dist_segment7
               ,create_dist_rec.dist_segment8
               ,create_dist_rec.dist_segment9
               ,create_dist_rec.dist_segment10
               ,create_dist_rec.interface_line_context
               ,create_dist_rec.interface_line_attribute1
               ,
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
                 --Ver 1.5 Commented for DFF rationalization end
                create_dist_rec.interface_line_attribute15
               ,create_dist_rec.account_class
               ,create_dist_rec.PERCENT
                --,xxar_interface_line_s.currval
               ,SYSDATE
               ,apps.fnd_global.user_id
               ,SYSDATE
               ,apps.fnd_global.user_id
               ,apps.fnd_global.login_id);
            update_dist_status(pin_interface_txn_id => create_dist_rec.interface_txn_id,
                               piv_process_flag => 'P', piv_err_type => NULL,
                               pov_return_status => l_upd_ret_status,
                               pov_error_code => l_err_code,
                               pov_error_message => l_err_msg);
         EXCEPTION
            WHEN OTHERS THEN
               g_retcode  := 1;
               l_err_code := 'ETN_AR_CREATE_EXCEPTION';
               l_err_msg  := 'Error : Exception in create_invoice Procedure for distributions. ' ||
                             SUBSTR(SQLERRM, 1, 150);
               log_errors(
                          -- pin_interface_txn_id      => create_dist_rec.interface_txn_id,
                          -- , piv_source_column_name     =>  'Legacy link_to_customer_trx_line_id'
                          --  , piv_source_column_value    =>  val_inv_det_rec(l_line_cnt).leg_link_to_cust_trx_line_id
                          piv_error_type => 'ERR_INT',
                          piv_error_code => l_err_code,
                          piv_error_message => l_err_msg,
                          piv_source_table => 'XXAR_INVOICES_DIST_STG',
                          pov_return_status => l_log_ret_status,
                          pov_error_msg => l_log_err_msg);
               update_dist_status(pin_interface_txn_id => create_dist_rec.interface_txn_id,
                                  piv_process_flag => 'E',
                                  piv_err_type => 'ERR_INT',
                                  pov_return_status => l_upd_ret_status,
                                  pov_error_code => l_err_code,
                                  pov_error_message => l_err_msg);
         END;
      END LOOP;
      FOR r_createdist_err_rec IN (SELECT DISTINCT xds.leg_customer_trx_id
                                                  ,xds.process_flag
                                     FROM xxar_invoices_dist_stg xds
                                    WHERE xds.process_flag IN ('V', 'E')
                                      AND xds.batch_id = g_new_batch_id
                                      AND DECODE(xds.process_flag, 'E',
                                                 NVL(xds.ERROR_TYPE, 'A'),
                                                 'ERR_INT') = 'ERR_INT')
      --AND NVL(xds.leg_account_class, 'A') <> 'ROUND') --performance
      LOOP
         UPDATE xxar_invoices_dist_stg
            SET process_flag      = 'E'
               ,ERROR_TYPE        = 'ERR_INT'
               ,run_sequence_id   = g_new_run_seq_id
               ,last_update_date  = SYSDATE
               ,last_updated_by   = g_last_updated_by
               ,last_update_login = g_login_id
          WHERE leg_customer_trx_id =
                r_createdist_err_rec.leg_customer_trx_id
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
            UPDATE xxar_invoices_stg
               SET process_flag      = 'E'
                  ,ERROR_TYPE        = 'ERR_INT'
                  ,run_sequence_id   = g_new_run_seq_id
                  ,last_update_date  = SYSDATE
                  ,last_updated_by   = g_last_updated_by
                  ,last_update_login = g_login_id
             WHERE leg_customer_trx_id =
                   r_createdist_err_rec.leg_customer_trx_id
               AND batch_id = g_new_batch_id;
            l_err_code := 'ETN_DISTRIBUTION_ERROR';
            l_err_msg  := 'Error : Erroring out invoice lines since distribution is in error while inserting in ra_interface_distributions_all';
            -- print_log_message ('For legacy transaction number: '||r_createdist_err_rec.leg_trx_number);
            print_log_message(l_err_msg);
            log_errors(
                       --   pin_transaction_id           =>  r_dist_err_rec.interface_txn_id
                       piv_error_type => 'ERR_INT',
                       piv_source_column_name => 'LEGACY_CUSTOMER_TRX_ID',
                       piv_source_column_value => r_createdist_err_rec.leg_customer_trx_id,
                       piv_error_code => l_err_code,
                       piv_error_message => l_err_msg,
                       pov_return_status => l_log_ret_status,
                       pov_error_msg => l_log_err_msg);
         END IF;
         l_err_code := 'ETN_INVOICE_ERROR';
         IF r_createdist_err_rec.process_flag = 'V'
         THEN
            l_err_msg := 'Error : Erroring distribution since corresponding invoice line in error while inserting in ra_interface_lines_all ';
         ELSE
            l_err_msg := 'Error : Erroring distribution since another related distribution in error while inserting in ra_interface_distributions_all';
         END IF;
         -- print_log_message ('For legacy transaction number: '||r_createdist_err_rec.leg_trx_number);
         print_log_message(l_err_msg);
         log_errors(
                    --   pin_transaction_id           =>  r_dist_err_rec.interface_txn_id
                    piv_error_type => 'ERR_INT',
                    piv_source_column_name => 'LEGACY_CUSTOMER_TRX_ID',
                    piv_source_column_value => r_createdist_err_rec.leg_customer_trx_id,
                    piv_source_table => 'XXAR_INVOICES_DIST_STG',
                    piv_error_code => l_err_code,
                    piv_error_message => l_err_msg,
                    pov_return_status => l_log_ret_status,
                    pov_error_msg => l_log_err_msg);
      END LOOP;
      COMMIT;
      FOR r_rida_err_rec IN (SELECT DISTINCT xds.interface_line_attribute15
                               FROM xxar_invoices_dist_stg xds
                              WHERE xds.process_flag = 'E'
                                AND xds.batch_id = g_new_batch_id
                                AND NVL(xds.ERROR_TYPE, 'A') = 'ERR_INT')
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
      WHEN OTHERS THEN
         g_retcode  := 2;
         l_err_code := 'ETN_AR_PROCEDURE_EXCEPTION';
         l_err_msg  := 'Error : Exception in create_invoice Procedure. ' ||
                       SUBSTR(SQLERRM, 1, 150);
         log_errors(
                    -- pin_transaction_id           =>  pin_trx_id
                    -- , piv_source_column_name     =>  'Legacy link_to_customer_trx_line_id'
                    --  , piv_source_column_value    =>  val_inv_det_rec(l_line_cnt).leg_link_to_cust_trx_line_id
                    piv_error_type => 'ERR_INT',
                    piv_error_code => l_err_code,
                    piv_error_message => l_err_msg,
                    pov_return_status => l_log_ret_status,
                    pov_error_msg => l_log_err_msg);
   END create_invoice;
   */
  --
  -- ========================
  -- Procedure: ASSIGN_BATCH_ID
  -- =============================================================================
  --   This procedure assigns batch id
  -- =============================================================================
  --
  PROCEDURE assign_batch_id(p_return_status OUT VARCHAR2,
                            p_error_code    OUT VARCHAR2,
                            p_error_message OUT VARCHAR2) IS
    PRAGMA AUTONOMOUS_TRANSACTION;

    -- v1.45 changes
    CURSOR fetch_cust_trx_cur IS
      SELECT xis.leg_customer_trx_id
        FROM xxar_invoices_stg xis
       WHERE xis.batch_id = g_new_batch_id
         AND xis.run_sequence_id = g_new_run_seq_id;

  BEGIN
    -- g_batch_id NULL is considered a fresh run
    IF g_batch_id IS NULL THEN
      print_log_message('assign_batch_id g_batch_id IS NULL');
      UPDATE xxar_invoices_stg
         SET process_flag           = 'N',
             batch_id               = g_new_batch_id,
             run_sequence_id        = g_new_run_seq_id,
             last_update_date       = g_sysdate,
             last_updated_by        = g_user_id,
             last_update_login      = g_login_id,
             program_application_id = g_prog_appl_id,
             program_id             = g_conc_program_id,
             program_update_date    = g_sysdate,
             request_id             = g_request_id
       WHERE batch_id IS NULL
         AND UPPER(leg_operating_unit) =
             UPPER(NVL(g_leg_operating_unit, leg_operating_unit))
         AND UPPER(leg_cust_trx_type_name) =
             UPPER(NVL(g_leg_trasaction_type, leg_cust_trx_type_name));

      -- v1.45 changes start
      FOR fetch_cust_trx_rec IN fetch_cust_trx_cur LOOP
        UPDATE xxar_invoices_dist_stg xids
           SET xids.process_flag           = 'N',
               xids.batch_id               = g_new_batch_id,
               xids.run_sequence_id        = g_new_run_seq_id,
               xids.last_update_date       = g_sysdate,
               xids.last_updated_by        = g_user_id,
               xids.last_update_login      = g_login_id,
               xids.program_application_id = g_prog_appl_id,
               xids.program_id             = g_conc_program_id,
               xids.program_update_date    = g_sysdate,
               xids.request_id             = g_request_id
         WHERE xids.leg_customer_trx_id =
               fetch_cust_trx_rec.leg_customer_trx_id
           AND xids.batch_id IS NULL
           AND xids.leg_org_name =
               NVL(g_leg_operating_unit, xids.leg_org_name);
      END LOOP;
      -- v1.45 changes end

      --ver.1.39.1 chaanges start, v1.45 changes start
      FOR fetch_cust_trx_rec IN fetch_cust_trx_cur LOOP
        UPDATE xxar_inv_pmt_schedule_stg xips
           SET xips.process_flag           = 'N',
               xips.batch_id               = g_new_batch_id,
               xips.run_sequence_id        = g_new_run_seq_id,
               xips.last_update_date       = g_sysdate,
               xips.last_updated_by        = g_user_id,
               xips.last_update_login      = g_login_id,
               xips.program_application_id = g_prog_appl_id,
               xips.program_id             = g_conc_program_id,
               xips.program_update_date    = g_sysdate,
               xips.request_id             = g_request_id
         WHERE xips.leg_customer_trx_id =
               fetch_cust_trx_rec.leg_customer_trx_id
           AND xips.batch_id IS NULL
           AND xips.leg_operating_unit =
               NVL(g_leg_operating_unit, xips.leg_operating_unit);
      END LOOP;

      --ver.1.39.1 chaanges end, v1.45 changes end
      p_error_message := 'Updated staging table, Update Count : ' ||
                         SQL%ROWCOUNT;
      print_log_message('p_error_message:' || p_error_message);
    ELSE
      UPDATE xxar_invoices_stg
         SET process_flag           = 'N',
             run_sequence_id        = g_new_run_seq_id,
             last_update_date       = g_sysdate,
             last_updated_by        = g_user_id,
             last_update_login      = g_login_id,
             program_application_id = g_prog_appl_id,
             program_id             = g_conc_program_id,
             program_update_date    = g_sysdate,
             request_id             = g_request_id
       WHERE batch_id = g_new_batch_id
         AND (g_process_records = 'ALL' AND (process_flag IN ('N', 'E')) OR
             g_process_records = 'ERROR' AND (process_flag = 'E') OR
             g_process_records = 'UNPROCESSED' AND (process_flag = 'N'))
         AND NVL(ERROR_TYPE, 'NO_ERR_TYPE') <> 'ERR_IMP'
         AND UPPER(leg_cust_trx_type_name) =
             UPPER(NVL(g_leg_trasaction_type, leg_cust_trx_type_name))
         AND UPPER(leg_operating_unit) =
             UPPER(NVL(g_leg_operating_unit, leg_operating_unit));
      UPDATE xxar_invoices_dist_stg xids
         SET xids.process_flag           = 'N',
             xids.run_sequence_id        = g_new_run_seq_id,
             xids.last_update_date       = g_sysdate,
             xids.last_updated_by        = g_user_id,
             xids.last_update_login      = g_login_id,
             xids.program_application_id = g_prog_appl_id,
             xids.program_id             = g_conc_program_id,
             xids.program_update_date    = g_sysdate,
             xids.request_id             = g_request_id
       WHERE xids.batch_id = g_new_batch_id
         AND (g_process_records = 'ALL' AND
             (xids.process_flag IN ('N', 'E')) OR
             g_process_records = 'ERROR' AND (xids.process_flag = 'E') OR
             g_process_records = 'UNPROCESSED' AND
             (xids.process_flag = 'N'))
         AND UPPER(xids.leg_org_name) =
             UPPER(NVL(g_leg_operating_unit, xids.leg_org_name))
         AND NVL(xids.ERROR_TYPE, 'NO_ERR_TYPE') <> 'ERR_IMP'
         AND EXISTS
       (SELECT /*+ INDEX (xis XXAR_INVOICES_STG_N4) */
               1
                FROM xxar_invoices_stg xis
               WHERE xis.leg_customer_trx_id = xids.leg_customer_trx_id
                 AND xis.batch_id = g_new_batch_id
                 AND xis.run_sequence_id = g_new_run_seq_id);
      --ver.1.39.1 chaanges start
      UPDATE xxar_inv_pmt_schedule_stg xips
         SET xips.process_flag           = 'N',
             xips.run_sequence_id        = g_new_run_seq_id,
             xips.last_update_date       = g_sysdate,
             xips.last_updated_by        = g_user_id,
             xips.last_update_login      = g_login_id,
             xips.program_application_id = g_prog_appl_id,
             xips.program_id             = g_conc_program_id,
             xips.program_update_date    = g_sysdate,
             xips.request_id             = g_request_id
       WHERE xips.batch_id = g_new_batch_id
         AND EXISTS
       (SELECT /*+ INDEX (xis XXAR_INVOICES_STG_N4) */
               1
                FROM xxar_invoices_stg xis
               WHERE xis.leg_customer_trx_id = xips.leg_customer_trx_id
                 AND xis.batch_id = g_new_batch_id
                 AND xis.run_sequence_id = g_new_run_seq_id);
      --ver.1.39.1 chaanges end
      p_error_message := 'Updated staging table, Update Count : ' ||
                         SQL%ROWCOUNT;
    END IF; -- g_batch_id
    COMMIT;
    p_return_status := fnd_api.g_ret_sts_success;
  EXCEPTION
    WHEN OTHERS THEN
      p_return_status := fnd_api.g_ret_sts_error;
      p_error_code    := 'ETN_AR_ASSIGN_BATCH_ERROR';
      p_error_message := 'Error : Exception in assign_batch_id Procedure. ' ||
                         SUBSTR(SQLERRM, 1, 150);
      print_log_message('Error in assign_batch_id ' || p_error_message ||
                        'p_error_code' || p_error_code);
  END assign_batch_id;

  --
  -- ========================
  -- Procedure: UPDATE_DUEDATE
  -- =============================================================================
  --   This procedure update due date for invoices where due date in 11i is
  --   is different from R12
  -- =============================================================================
  --
  --ver1.39.3 changes start
  /*
    PROCEDURE update_duedate(pov_errbuf   OUT NOCOPY VARCHAR2
                             ,pon_retcode  OUT NOCOPY NUMBER
                             ,piv_dummy1   IN VARCHAR2
                             ,pin_batch_id IN NUMBER -- NULL / <BATCH_ID>
                              ) IS
        l_err_msg VARCHAR2(2000);
      l_count   NUMBER:= 0; --added for v1.35 Defect #5232
     BEGIN
        UPDATE ar_payment_schedules_all apsa
           SET due_date         =
               (SELECT MAX(xis.leg_due_date)
                  FROM xxar_invoices_stg        xis
                     ,ar_payment_schedules_all apsa1
                      ,ra_customer_trx_all      rcta
                 WHERE apsa1.payment_schedule_id = apsa.payment_schedule_id
                   AND apsa1.customer_trx_id = rcta.customer_trx_id
                   AND rcta.trx_number = xis.leg_trx_number
                   AND rcta.org_id = xis.org_id
                   AND rcta.cust_trx_type_id = xis.transaction_type_id
                   AND xis.process_flag = 'C'
                   AND xis.batch_id = pin_batch_id
                   AND xis.leg_cust_trx_type_name NOT LIKE '%OKS%')
              ,last_update_date  = SYSDATE
              ,last_updated_by   = g_last_updated_by
              ,last_update_login = g_login_id
         WHERE payment_schedule_id IN
               (SELECT apsa2.payment_schedule_id
                  FROM xxar_invoices_stg        xis
                      ,ar_payment_schedules_all apsa2
                      ,ra_customer_trx_all      rcta
                 WHERE apsa2.payment_schedule_id = apsa.payment_schedule_id
                   AND apsa2.customer_trx_id = rcta.customer_trx_id
                   AND rcta.trx_number = xis.leg_trx_number
                   AND rcta.org_id = xis.org_id
                   AND rcta.cust_trx_type_id = xis.transaction_type_id
                   AND xis.process_flag = 'C'
                   AND xis.batch_id = pin_batch_id
                   AND xis.leg_cust_trx_type_name NOT LIKE '%OKS%');
    print_log_message('+ Updated Due Date on Recs = ' || SQL%ROWCOUNT);
  ----added for v1.35 Defect #5232
          BEGIN
            SELECT COUNT (*)
            INTO l_count
            FROM xxconv.xxar_invoices_stg
           WHERE process_flag = 'C'
             AND batch_id = pin_batch_id
             AND leg_cust_trx_type_name LIKE '%OKS%';
         EXCEPTION
            WHEN OTHERS
            THEN
             l_count := 0;
         END;
  --end of additional code for v1.35 Defect #5232
  IF l_count > 0 THEN --v1.35 calling statement only if any service contracts are processed
        UPDATE ar_payment_schedules_all apsa
           SET due_date         =
               (SELECT MAX(xis.leg_due_date)
                  FROM xxar_invoices_stg        xis
                      ,ar_payment_schedules_all apsa1
                      ,ra_customer_trx_all      rcta
                 WHERE apsa1.payment_schedule_id = apsa.payment_schedule_id
                   AND apsa1.customer_trx_id = rcta.customer_trx_id
                   AND rcta.trx_number = xis.leg_trx_number
                   AND rcta.org_id = xis.org_id
                   AND rcta.cust_trx_type_id = xis.transaction_type_id
                   AND xis.process_flag = 'C'
                   AND xis.batch_id = pin_batch_id
                   AND xis.leg_cust_trx_type_name LIKE '%OKS%'
                   AND apsa1.due_date =
                       (SELECT MAX(apsa.due_date)
                          FROM ar_payment_schedules_all apsa
                         WHERE apsa.customer_trx_id = apsa1.customer_trx_id))
              ,last_update_date  = SYSDATE
              ,last_updated_by   = g_last_updated_by
              ,last_update_login = g_login_id
         WHERE payment_schedule_id IN
               (SELECT apsa2.payment_schedule_id
                  FROM xxar_invoices_stg        xis
                      ,ar_payment_schedules_all apsa2
                      ,ra_customer_trx_all      rcta
                 WHERE apsa2.payment_schedule_id = apsa.payment_schedule_id
                   AND apsa2.customer_trx_id = rcta.customer_trx_id
                   AND rcta.trx_number = xis.leg_trx_number
                   AND rcta.org_id = xis.org_id
                   AND rcta.cust_trx_type_id = xis.transaction_type_id
                   AND xis.process_flag = 'C'
                   AND xis.batch_id = pin_batch_id
                   AND xis.leg_cust_trx_type_name LIKE '%OKS%'
                   AND apsa2.due_date =
                       (SELECT MAX(apsa3.due_date)
                          FROM ar_payment_schedules_all apsa3
                         WHERE apsa2.customer_trx_id = apsa3.customer_trx_id));
    END IF; --v1.35 end of IF l_count >0

        pov_errbuf  := g_errbuff;
        pon_retcode := g_retcode;
        print_log_message('-   PROCEDURE : Update Due Date Program for batch id: ' ||
                          pin_batch_id);
        print_log_message('Update Due Date Ends at: ' ||
                          TO_CHAR(g_sysdate, 'DD-MON-YYYY HH24:MI:SS'));
        print_log_message('---------------------------------------------');
     EXCEPTION
        WHEN OTHERS THEN
           pov_errbuf  := 'Error : Main program procedure encounter error. ' ||
                          SUBSTR(SQLERRM, 1, 150);
           pon_retcode := 2;
           print_log_message('In Due Date update when others' ||
                             SUBSTR(SQLERRM, 1, 150));
     END update_duedate;
  */

  PROCEDURE update_duedate(pov_errbuf   OUT NOCOPY VARCHAR2,
                           pon_retcode  OUT NOCOPY NUMBER,
                           piv_dummy1   IN VARCHAR2,
                           pin_batch_id IN NUMBER -- NULL / <BATCH_ID>
                           ) IS
    l_err_msg VARCHAR2(2000);

    -- v1.45 changes
    CURSOR fetch_pay_schdle_cur IS
      SELECT apsa2.payment_schedule_id, xps.leg_due_date
        FROM xxar_invoices_stg        xis,
             ar_payment_schedules_all apsa2,
             ra_customer_trx_all      rcta
             --,ra_customer_trx_lines_all rctl
             --,ra_interface_lines_all rctl
            ,
             xxar_inv_pmt_schedule_stg xps
       WHERE 1 = 1
            --apsa2.payment_schedule_id = apsa.payment_schedule_id
         AND rcta.customer_trx_id = apsa2.customer_trx_id
         AND xps.leg_customer_trx_id = xis.leg_customer_trx_id
         AND apsa2.terms_sequence_number = xps.leg_terms_sequence_number
         AND xis.batch_id = xps.batch_id
         AND xis.leg_source_system = xps.leg_source_system
         AND rcta.trx_number = xis.leg_trx_number
         AND rcta.org_id = xis.org_id
         AND rcta.cust_trx_type_id = xis.transaction_type_id
         AND xis.process_flag = 'C'
         AND xis.batch_id = pin_batch_id
            --AND rctl.interface_status = 'P'
            --AND rctl.interface_line_attribute15 = xis.interface_line_attribute15
            --AND xis.interface_line_attribute15 = --  rctl.interface_line_attribute15)
            --AND rctl.interface_line_context = 'Eaton'
         AND rcta.interface_header_attribute15 =
             xis.interface_line_attribute15
         AND apsa2.due_date <> xps.leg_due_date;

  BEGIN

    /*
     UPDATE ar_payment_schedules_all apsa
        SET due_date         =
            (SELECT MAX(xps.leg_due_date)
               FROM xxar_invoices_stg         xis
                   ,ar_payment_schedules_all  apsa1
                   ,ra_customer_trx_all       rcta
                   ,ra_customer_trx_lines_all rctl
                   ,xxar_inv_pmt_schedule_stg xps
              WHERE apsa1.payment_schedule_id = apsa.payment_schedule_id
                AND apsa1.customer_trx_id = rcta.customer_trx_id
                AND xps.leg_customer_trx_id = xis.leg_customer_trx_id
                AND apsa1.terms_sequence_number =
                    xps.leg_terms_sequence_number
                AND xis.batch_id = xps.batch_id
                AND xis.leg_source_system = xps.leg_source_system
                AND rcta.trx_number = xis.leg_trx_number
                AND rcta.org_id = xis.org_id
                AND rcta.cust_trx_type_id = xis.transaction_type_id
                AND xis.process_flag = 'C'
                AND xis.batch_id = pin_batch_id
                AND xis.interface_line_attribute15 =
                    rctl.interface_line_attribute15)
           ,last_update_date  = SYSDATE
           ,last_updated_by   = g_last_updated_by
           ,last_update_login = g_login_id
      WHERE payment_schedule_id IN
            (SELECT apsa2.payment_schedule_id
               FROM xxar_invoices_stg         xis
                   ,ar_payment_schedules_all  apsa2
                   ,ra_customer_trx_all       rcta
                   ,ra_customer_trx_lines_all rctl
                   ,xxar_inv_pmt_schedule_stg xps
              WHERE apsa2.payment_schedule_id = apsa.payment_schedule_id
                AND apsa2.customer_trx_id = rcta.customer_trx_id
                AND xps.leg_customer_trx_id = xis.leg_customer_trx_id
                AND apsa2.terms_sequence_number =
                    xps.leg_terms_sequence_number
                AND xis.batch_id = xps.batch_id
                AND xis.leg_source_system = xps.leg_source_system
                AND rcta.trx_number = xis.leg_trx_number
                AND rcta.org_id = xis.org_id
                AND rcta.cust_trx_type_id = xis.transaction_type_id
                AND xis.process_flag = 'C'
                AND xis.batch_id = pin_batch_id
                AND xis.interface_line_attribute15 =
                    rctl.interface_line_attribute15
                AND xps.leg_due_date <> apsa2.due_date);
    */
    /**
    UPDATE ar_payment_schedules_all apsa
      SET due_date         =
          (SELECT MAX(xps.leg_due_date)
             FROM xxar_invoices_stg         xis
                 ,ar_payment_schedules_all  apsa1
                 ,ra_customer_trx_all       rcta
                 --,ra_customer_trx_lines_all rctl
                 ,ra_interface_lines_all rctl
                 ,xxar_inv_pmt_schedule_stg xps
            WHERE apsa1.payment_schedule_id = apsa.payment_schedule_id
              AND rcta.customer_trx_id = apsa1.customer_trx_id
              AND xps.leg_customer_trx_id = xis.leg_customer_trx_id
              AND apsa1.terms_sequence_number =
                  xps.leg_terms_sequence_number
              AND xis.batch_id = xps.batch_id
              AND xis.leg_source_system = xps.leg_source_system
              AND rcta.trx_number = xis.leg_trx_number
              AND rcta.org_id = xis.org_id
              AND rcta.cust_trx_type_id = xis.transaction_type_id
              AND xis.process_flag = 'C'
              AND xis.batch_id = pin_batch_id
              AND rctl.interface_status = 'P'
              AND rctl.interface_line_attribute15 = xis.interface_line_attribute15
              --AND xis.interface_line_attribute15 =
                --  rctl.interface_line_attribute15)
              AND rctl.interface_line_context = 'Eaton'
              AND rcta.interface_header_attribute15 = xis.interface_line_attribute15)
         ,last_update_date  = SYSDATE
         ,last_updated_by   = g_last_updated_by
         ,last_update_login = g_login_id
    WHERE payment_schedule_id IN
          (SELECT apsa2.payment_schedule_id
             FROM xxar_invoices_stg         xis
                 ,ar_payment_schedules_all  apsa2
                 ,ra_customer_trx_all       rcta
                 --,ra_customer_trx_lines_all rctl
                 ,ra_interface_lines_all rctl
                 ,xxar_inv_pmt_schedule_stg xps
            WHERE apsa2.payment_schedule_id = apsa.payment_schedule_id
              AND rcta.customer_trx_id = apsa2.customer_trx_id
              AND xps.leg_customer_trx_id = xis.leg_customer_trx_id
              AND apsa2.terms_sequence_number =
                  xps.leg_terms_sequence_number
              AND xis.batch_id = xps.batch_id
              AND xis.leg_source_system = xps.leg_source_system
              AND rcta.trx_number = xis.leg_trx_number
              AND rcta.org_id = xis.org_id
              AND rcta.cust_trx_type_id = xis.transaction_type_id
              AND xis.process_flag = 'C'
              AND xis.batch_id = pin_batch_id
              AND rctl.interface_status = 'P'
              AND rctl.interface_line_attribute15 = xis.interface_line_attribute15
              --AND xis.interface_line_attribute15 =
                --  rctl.interface_line_attribute15)
              AND rctl.interface_line_context = 'Eaton'
              AND rcta.interface_header_attribute15 = xis.interface_line_attribute15
              AND apsa2.due_date <> xps.leg_due_date); **/

    -- v1.45 changes start
    FOR fetch_pay_schdle_rec IN fetch_pay_schdle_cur LOOP

      UPDATE ar_payment_schedules_all
         SET due_date          = fetch_pay_schdle_rec.leg_due_date,
             last_update_date  = g_sysdate,
             last_updated_by   = g_last_updated_by,
             last_update_login = g_login_id
       WHERE payment_schedule_id = fetch_pay_schdle_rec.payment_schedule_id;

    END LOOP;
    -- v1.45 changes end

    print_log_message('+ Updated Due Date on Recs = ' || SQL%ROWCOUNT);
    pov_errbuf  := g_errbuff;
    pon_retcode := g_retcode;
    print_log_message('-   PROCEDURE : Update Due Date Program for batch id: ' ||
                      pin_batch_id);
    print_log_message('Update Due Date Ends at: ' ||
                      TO_CHAR(g_sysdate, 'DD-MON-YYYY HH24:MI:SS'));
    print_log_message('---------------------------------------------');
  EXCEPTION
    WHEN OTHERS THEN
      pov_errbuf  := 'Error : Main program procedure encounter error. ' ||
                     SUBSTR(SQLERRM, 1, 150);
      pon_retcode := 2;
      print_log_message('In Due Date update when others' ||
                        SUBSTR(SQLERRM, 1, 150));
  END update_duedate;

  --ver1.39.3 changes end
  --
  -- ========================
  -- Procedure: TIE_BACK
  -- =============================================================================
  --   This procedure to tie back the process status after Autoinvoice program is complete
  -- =============================================================================
  --
  PROCEDURE tie_back(pov_errbuf   OUT NOCOPY VARCHAR2,
                     pon_retcode  OUT NOCOPY NUMBER,
                     piv_dummy1   IN VARCHAR2,
                     pin_batch_id IN NUMBER -- NULL / <BATCH_ID>
                     ) IS
    l_err_msg          VARCHAR2(4000);
    l_error_flag       VARCHAR2(10);
    l_return_status    VARCHAR2(200) := NULL;
    l_log_ret_status   VARCHAR2(50);
    l_log_err_msg      VARCHAR2(2000);
    l_interface_status ra_interface_lines_all.interface_status%TYPE; -- v1.40
    CURSOR tie_back_cur IS
      SELECT *
        FROM xxar_invoices_stg
       WHERE (process_flag = 'P' OR
             (process_flag = 'E' AND Error_Type = 'ERR_IMP'))
            -- C9988598 : process_flag = 'P'
         AND batch_id = pin_batch_id
       ORDER BY leg_trx_number; --, account_class;
    --Ver 1.5 Changes due to DFF rationalization start
    /*      CURSOR interface_error_cur (p_cust_trx_id IN VARCHAR2, p_cust_trx_line_id IN VARCHAR2)

          IS

             SELECT ril.interface_line_id, rie.MESSAGE_TEXT

               FROM ra_interface_errors_all rie, ra_interface_lines_all ril

              WHERE ril.interface_line_id = rie.interface_line_id

                AND ril.interface_line_attribute14 = p_cust_trx_id

                AND ril.interface_line_attribute15 = p_cust_trx_line_id

         AND ril.interface_line_context = 'Conversion';

    */
    CURSOR interface_error_cur(p_interface_line_attribute15 IN VARCHAR2) IS
      SELECT /*+ INDEX (ril XX_RA_INTERFACE_LINES_N11) */
       ril.interface_line_id, rie.MESSAGE_TEXT
        FROM ra_interface_errors_all rie, ra_interface_lines_all ril
       WHERE ril.interface_line_id = rie.interface_line_id
         AND ril.interface_line_attribute15 = p_interface_line_attribute15
         AND ril.interface_line_context = 'Eaton';
  BEGIN
    print_log_message('Tie Back Starts at: ' ||
                      TO_CHAR(g_sysdate, 'DD-MON-YYYY HH24:MI:SS'));
    print_log_message('+ Start of Tie Back + ' || pin_batch_id);
    g_new_run_seq_id                    := xxetn_run_sequences_s.NEXTVAL;
    xxetn_common_error_pkg.g_run_seq_id := g_new_run_seq_id;
    FOR tie_back_rec IN tie_back_cur LOOP
      print_log_message('Interface Transaction Id = ' ||
                        tie_back_rec.interface_txn_id);
      --IF tie_back_rec.account_class = 'REC' THEN
      l_error_flag := NULL;
      FOR interface_error_rec IN interface_error_cur(tie_back_rec.interface_line_attribute15) LOOP
        print_log_message('In error loop: Interface line Id = ' ||
                          interface_error_rec.interface_line_id);
        print_log_message('In error loop: Message Text - ' ||
                          interface_error_rec.MESSAGE_TEXT);
        l_error_flag := 'E';
        log_errors(pin_transaction_id      => tie_back_rec.interface_txn_id,
                   piv_source_column_name  => 'Interface Error',
                   piv_source_column_value => NULL,
                   piv_error_type          => 'ERR_IMP',
                   piv_error_code          => 'ETN_AR_INVOICE_CREATION_FAILED',
                   piv_error_message       => interface_error_rec.MESSAGE_TEXT,
                   pov_return_status       => l_log_ret_status,
                   pov_error_msg           => l_log_err_msg);
      END LOOP;
      --END IF;
      IF l_error_flag = 'E' THEN
        UPDATE xxar_invoices_stg
           SET process_flag      = 'E',
               ERROR_TYPE        = 'ERR_IMP',
               run_sequence_id   = g_new_run_seq_id,
               last_update_date  = g_sysdate,
               last_updated_by   = g_last_updated_by,
               last_update_login = g_login_id,
               request_id        = g_request_id -- added for v1.40
         WHERE interface_txn_id = tie_back_rec.interface_txn_id;
        UPDATE xxar_invoices_dist_stg
           SET process_flag      = 'E',
               ERROR_TYPE        = 'ERR_IMP',
               run_sequence_id   = g_new_run_seq_id,
               last_update_date  = g_sysdate,
               last_updated_by   = g_last_updated_by,
               last_update_login = g_login_id,
               request_id        = g_request_id -- added for v1.40
         WHERE leg_customer_trx_id = tie_back_rec.leg_customer_trx_id
           AND process_flag <> 'X'
              --     AND leg_cust_trx_line_id = tie_back_rec.leg_cust_trx_line_id
           AND batch_id = pin_batch_id;
      ELSE
        /** Added below for v1.40 **/
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
          /** Added above for v1.40 **/
          UPDATE xxar_invoices_stg
             SET process_flag      = 'C',
                 error_type        = NULL,
                 run_sequence_id   = g_new_run_seq_id,
                 last_update_date  = g_sysdate,
                 last_updated_by   = g_last_updated_by,
                 last_update_login = g_login_id,
                 request_id        = g_request_id -- added for v1.40
           WHERE interface_txn_id = tie_back_rec.interface_txn_id;
          UPDATE xxar_invoices_dist_stg
             SET process_flag      = 'C',
                 error_type        = NULL,
                 run_sequence_id   = g_new_run_seq_id,
                 last_update_date  = g_sysdate,
                 last_updated_by   = g_last_updated_by,
                 last_update_login = g_login_id,
                 request_id        = g_request_id -- added for v1.40
           WHERE leg_customer_trx_id = tie_back_rec.leg_customer_trx_id
                --AND leg_cust_trx_line_id = tie_back_rec.leg_cust_trx_line_id   commented for v1.40
             AND process_flag <> 'X'
             AND batch_id = pin_batch_id;
          /** Added below for v1.40 **/
        ELSE
          -- consider this as error record because of one of parent line failing
          UPDATE xxar_invoices_stg
             SET process_flag      = 'E',
                 ERROR_TYPE        = 'ERR_IMP',
                 run_sequence_id   = g_new_run_seq_id,
                 last_update_date  = g_sysdate,
                 last_updated_by   = g_last_updated_by,
                 last_update_login = g_login_id,
                 request_id        = g_request_id -- added for v1.40
           WHERE interface_txn_id = tie_back_rec.interface_txn_id;
          UPDATE xxar_invoices_dist_stg
             SET process_flag      = 'E',
                 ERROR_TYPE        = 'ERR_IMP',
                 run_sequence_id   = g_new_run_seq_id,
                 last_update_date  = g_sysdate,
                 last_updated_by   = g_last_updated_by,
                 last_update_login = g_login_id,
                 request_id        = g_request_id -- added for v1.40
           WHERE leg_customer_trx_id = tie_back_rec.leg_customer_trx_id
             AND process_flag <> 'X'
                --     AND leg_cust_trx_line_id = tie_back_rec.leg_cust_trx_line_id
             AND batch_id = pin_batch_id;
        END IF;
        /** Added above for v1.40 **/
      END IF;
    END LOOP;
    IF g_error_tab.COUNT > 0 THEN
      xxetn_common_error_pkg.add_error(pov_return_status => l_return_status
                                       -- OUT
                                      ,
                                       pov_error_msg => l_err_msg
                                       -- OUT
                                      ,
                                       pi_source_tab => g_error_tab
                                       -- IN  G_SOURCE_TAB_TYPE
                                      ,
                                       pin_batch_id        => pin_batch_id,
                                       pin_run_sequence_id => g_new_run_seq_id);
      g_error_tab.DELETE;
    END IF;
    pon_retcode := g_retcode;
    pov_errbuf  := g_errbuff;
    COMMIT;
  EXCEPTION
    WHEN OTHERS THEN
      g_retcode := 2;
      g_errbuff := 'Failed During Tie Back';
      print_log_message('In Tie Back when others' ||
                        SUBSTR(SQLERRM, 1, 150));
  END tie_back;

  --
  -- ========================
  -- Procedure: MAIN
  -- =============================================================================
  --   This is a main public procedure, which will be invoked through concurrent
  --   program.
  --
  -- =============================================================================
  --
  --
  --  Input Parameters :
  --    piv_run_mode        : Control the program excution for VALIDATE CONVERSION
  --
  --    pin_batch_id        : List all unique batches from staging table , this will
  --                        be NULL for first Conversion Run.
  --    piv_process_records : Conditionally available only when pin_batch_id is popul-
  --                        -ated. Otherwise this will be disabled and defaulted
  --                        to ALL
  --    piv_operating_unit  : 11i Operating Unit. If provided then program will
  --               run only for specified Operating unit
  --    piv_gl_date      : GL date for which the conversion is done. This
  --                date will be considered only during VALIDATE mode
  --                for transaction other than service contracts
  --
  --  Output Parameters :
  --    pov_errbuf          : Standard output parameter for concurrent program
  --    pon_retcode         : Standard output parameter for concurrent program
  --
  --  Return     : Not applicable
  -- -----------------------------------------------------------------------------
  PROCEDURE main(pov_errbuf           OUT NOCOPY VARCHAR2,
                 pon_retcode          OUT NOCOPY NUMBER,
                 piv_run_mode         IN VARCHAR2,
                 piv_dummy1           IN VARCHAR2,
                 piv_operating_unit   IN VARCHAR2,
                 piv_transaction_type IN VARCHAR2,
                 pin_batch_id         IN NUMBER,
                 piv_dummy            IN VARCHAR2,
                 piv_process_records  IN VARCHAR2,
                 piv_gl_date          IN VARCHAR2,
                 piv_period_set_name  IN VARCHAR2) IS
    l_debug_on      BOOLEAN;
    l_return_status VARCHAR2(200) := NULL;
    l_err_code      VARCHAR2(40) := NULL;
    l_err_msg       VARCHAR2(2000) := NULL;
    l_err_excep  EXCEPTION;
    l_warn_excep EXCEPTION;
    l_log_ret_status      VARCHAR2(50) := NULL;
    l_log_err_msg         VARCHAR2(2000);
    l_debug_err           VARCHAR2(2000);
    l_load_ret_stats      VARCHAR2(1);
    l_dist_load_ret_stats VARCHAR2(1);
    --ver1.39.1 changes start
    l_pmt_sch_load_ret_stats VARCHAR2(1);
    l_pmt_sch_load_err_msg   VARCHAR2(1000);
    --ver1.39.1 changes end
    l_print_ret_stats   VARCHAR2(1);
    l_print_err_msg     VARCHAR2(1000);
    l_load_err_msg      VARCHAR2(1000);
    l_dist_load_err_msg VARCHAR2(1000);
    l_conv_batch_ou     xxar_conv_batches.leg_operating_unit%TYPE;
    l_conv_batch_txn    xxar_conv_batches.leg_transaction_type%TYPE;
    l_err_ret_status    VARCHAR2(1);
    l_error_message     VARCHAR2(2000);
  BEGIN
    xxetn_debug_pkg.initialize_debug(pov_err_msg      => g_debug_err,
                                     piv_program_name => 'ETN_AR_INVOICE_CONVERSION');
    xxetn_debug_pkg.add_debug('Program Parameters');
    xxetn_debug_pkg.add_debug('---------------------------------------------');
    xxetn_debug_pkg.add_debug('Run Mode        : ' || piv_run_mode);
    xxetn_debug_pkg.add_debug('Batch ID        : ' || pin_batch_id);
    xxetn_debug_pkg.add_debug('Reprocess records     : ' ||
                              piv_process_records);
    xxetn_debug_pkg.add_debug('Legacy Operating Unit : ' ||
                              piv_operating_unit);
    xxetn_debug_pkg.add_debug('Legacy Transaction Type : ' ||
                              piv_transaction_type);
    xxetn_debug_pkg.add_debug('GL Date        : ' || piv_gl_date);
    print_log_message('Program Parameters');
    print_log_message('---------------------------------------------');
    print_log_message('Run Mode        : ' || piv_run_mode);
    print_log_message('Batch ID        : ' || pin_batch_id);
    print_log_message('Reprocess records    : ' || piv_process_records);
    print_log_message('Legacy Operating Unit : ' || piv_operating_unit);
    print_log_message('Legacy Transaction Type : ' || piv_transaction_type);
    print_log_message('GL Date        : ' || piv_gl_date);
    print_log_message('');
    g_run_mode            := piv_run_mode;
    g_batch_id            := pin_batch_id;
    g_process_records     := piv_process_records;
    g_leg_operating_unit  := piv_operating_unit;
    g_leg_trasaction_type := piv_transaction_type;
    IF piv_gl_date IS NOT NULL THEN
      g_gl_date := apps.fnd_date.canonical_to_date(piv_gl_date);
    END IF;
    g_period_set_name := piv_period_set_name;
    ----------------------------------------------------------------------------------------------------------
    -- Program run in run mode = 'LOAD'
    -- Data will be loaded from extraction table which was populated by Eaton into the R12 staging tables for conversion in R12
    -- Data will be loaded in xxar_invoices_stg and xxar_invoices_dist_stg
    ----------------------------------------------------------------------------------------------------------
    IF g_run_mode = 'LOAD-DATA' THEN
      print_log_message('Calling procedure load_invoice');
      print_log_message('');
      load_invoice(pov_ret_stats => l_load_ret_stats,
                   pov_err_msg   => l_load_err_msg);
      pon_retcode := g_retcode;
      --        print_stats_p;
      print_log_message('Calling procedure load_distribution');
      print_log_message('');
      load_distribution(pov_ret_stats => l_dist_load_ret_stats,
                        pov_err_msg   => l_dist_load_err_msg);
      pon_retcode := g_retcode;
      --ver1.39.1 changes start
      print_log_message('Calling procedure load_pmt_schedules');
      print_log_message('');
      load_pmt_schedules(pov_ret_stats => l_pmt_sch_load_ret_stats,
                         pov_err_msg   => l_pmt_sch_load_err_msg);
      pon_retcode := g_retcode;
      --ver1.39.1 changes end
      print_stats_p;
      IF l_load_ret_stats <> 'S' THEN
        print_log_message('Error in procedure load_invoice' ||
                          l_load_err_msg);
        print_log_message('');
        RAISE l_warn_excep;
      END IF;
      IF l_dist_load_ret_stats <> 'S' THEN
        print_log_message('Error in procedure load_distribution' ||
                          l_dist_load_err_msg);
        print_log_message('');
        RAISE l_warn_excep;
      END IF;
      --ver1.39.1 changes start
      IF l_pmt_sch_load_ret_stats <> 'S' THEN
        print_log_message('Error in procedure load_pmt_schedules' ||
                          l_pmt_sch_load_err_msg);
        print_log_message('');
        RAISE l_warn_excep;
      END IF;
      --ver1.39.1 changes end
    ELSIF UPPER(piv_run_mode) = 'PRE-VALIDATE' THEN
      print_log_message('Calling procedure pre_validate_invoice');
      print_log_message('');
      pre_validate_invoice;
      pon_retcode := g_retcode;
    ELSIF UPPER(piv_run_mode) = 'VALIDATE' THEN
      IF (g_batch_id IS NOT NULL AND piv_process_records IS NULL) THEN
        l_err_code := 'ETN_AR_CHECK_PARAMETER';
        l_err_msg  := 'Parameter batch ID provided but Parameter Reprocess records is NULL';
        log_errors(piv_error_type    => 'ERR_VAL',
                   piv_error_code    => l_err_code,
                   piv_error_message => l_err_msg,
                   pov_return_status => l_log_ret_status,
                   pov_error_msg     => l_log_err_msg);
        RAISE l_err_excep;
      END IF;
      IF (g_batch_id IS NULL AND piv_process_records IS NOT NULL) THEN
        l_err_code := 'ETN_AR_CHECK_PARAMETER';
        l_err_msg  := 'Parameter Reprocess records is provided but Parameter batch id is null';
        log_errors(piv_error_type    => 'ERR_VAL',
                   piv_error_code    => l_err_code,
                   piv_error_message => l_err_msg,
                   pov_return_status => l_log_ret_status,
                   pov_error_msg     => l_log_err_msg);
        RAISE l_err_excep;
      END IF;
      IF (g_batch_id IS NULL AND g_gl_date IS NULL) THEN
        l_err_code := 'ETN_AR_CHECK_PARAMETER';
        l_err_msg  := 'GL date cannot be NULL';
        log_errors(piv_error_type    => 'ERR_VAL',
                   piv_error_code    => l_err_code,
                   piv_error_message => l_err_msg,
                   pov_return_status => l_log_ret_status,
                   pov_error_msg     => l_log_err_msg);
        RAISE l_err_excep;
      END IF;
      IF (g_leg_operating_unit IS NOT NULL AND g_batch_id IS NOT NULL) THEN
        BEGIN
          SELECT leg_operating_unit
            INTO l_conv_batch_ou
            FROM xxar_conv_batches
           WHERE batch_id = g_batch_id;
          IF NVL(l_conv_batch_ou, piv_operating_unit) <> piv_operating_unit THEN
            l_err_code := 'ETN_AR_CHECK_PARAMETER';
            l_err_msg  := 'Operating unit used while processing batch is different from operating unit provided while reprocessing batch';
            log_errors(piv_error_type    => 'ERR_VAL',
                       piv_error_code    => l_err_code,
                       piv_error_message => l_err_msg,
                       pov_return_status => l_log_ret_status,
                       pov_error_msg     => l_log_err_msg);
            RAISE l_err_excep;
          END IF;
        EXCEPTION
          WHEN NO_DATA_FOUND THEN
            l_err_code := 'ETN_AR_CHECK_PARAMETER';
            l_err_msg  := 'Batch number provided is not a valid batch ';
            log_errors(piv_error_type    => 'ERR_VAL',
                       piv_error_code    => l_err_code,
                       piv_error_message => l_err_msg,
                       pov_return_status => l_log_ret_status,
                       pov_error_msg     => l_log_err_msg);
            RAISE l_err_excep;
        END;
      END IF;
      IF (g_leg_trasaction_type IS NOT NULL AND g_batch_id IS NOT NULL) THEN
        BEGIN
          SELECT leg_transaction_type
            INTO l_conv_batch_txn
            FROM xxar_conv_batches
           WHERE batch_id = g_batch_id;
          IF NVL(l_conv_batch_txn, piv_transaction_type) <>
             piv_transaction_type THEN
            l_err_code := 'ETN_AR_CHECK_PARAMETER';
            l_err_msg  := 'Transaction Type used while processing batch is different from Transaction Type provided while reprocessing batch';
            log_errors(piv_error_type    => 'ERR_VAL',
                       piv_error_code    => l_err_code,
                       piv_error_message => l_err_msg,
                       pov_return_status => l_log_ret_status,
                       pov_error_msg     => l_log_err_msg);
            RAISE l_err_excep;
          END IF;
        EXCEPTION
          WHEN NO_DATA_FOUND THEN
            l_err_code := 'ETN_AR_CHECK_PARAMETER';
            l_err_msg  := 'Batch number provided is not a valid batch ';
            log_errors(piv_error_type    => 'ERR_VAL',
                       piv_error_code    => l_err_code,
                       piv_error_message => l_err_msg,
                       pov_return_status => l_log_ret_status,
                       pov_error_msg     => l_log_err_msg);
            RAISE l_err_excep;
        END;
      END IF;
      IF g_batch_id IS NULL THEN
        g_new_batch_id := xxetn_batches_s.NEXTVAL;
        BEGIN
          INSERT INTO xxar_conv_batches
          VALUES
            (g_new_batch_id,
             NULL,
             --g_new_run_seq_id,
             'AR_INVOICE_CONVERSION',
             g_leg_operating_unit,
             g_sysdate,
             g_sysdate,
             g_user_id,
             g_sysdate,
             g_user_id,
             g_login_id,
             g_leg_trasaction_type);
          COMMIT;
        EXCEPTION
          WHEN OTHERS THEN
            l_err_code := 'ETN_AR_BATCH_MONITOR';
            l_err_msg  := 'Error inserting record in XXAR_CONV_BATCHES  ';
            log_errors(piv_error_type    => 'ERR_VAL',
                       piv_error_code    => l_err_code,
                       piv_error_message => l_err_msg,
                       pov_return_status => l_log_ret_status,
                       pov_error_msg     => l_log_err_msg);
            RAISE l_err_excep;
        END;
      ELSE
        g_new_batch_id := g_batch_id;
      END IF;
      g_new_run_seq_id := xxetn_run_sequences_s.NEXTVAL;
      xxetn_debug_pkg.add_debug('New Batch ID        : ' || g_new_batch_id);
      xxetn_debug_pkg.add_debug('New Run Sequence ID : ' ||
                                g_new_run_seq_id);
      print_log_message('New Batch ID        : ' || g_new_batch_id);
      print_log_message('New Run Sequence ID : ' || g_new_run_seq_id);
      xxetn_debug_pkg.add_debug('---------------------------------------------');
      xxetn_debug_pkg.add_debug('PROCEDURE: assign_batch_id' || CHR(10));
      -- Call procedure to assign batch IDs
      l_err_code := NULL;
      l_err_msg  := NULL;
      assign_batch_id(l_return_status, l_err_code, l_err_msg);
      IF l_return_status = fnd_api.g_ret_sts_error THEN
        log_errors(piv_error_type    => 'ERR_VAL',
                   piv_error_code    => l_err_code,
                   piv_error_message => l_err_msg,
                   pov_return_status => l_log_ret_status,
                   pov_error_msg     => l_log_err_msg);
        print_log_message('Exiting Program');
        xxetn_debug_pkg.add_debug('Exiting Program..');
        RETURN;
      END IF;
      xxetn_debug_pkg.add_debug('---------------------------------------------');
      xxetn_debug_pkg.add_debug('PROCEDURE: Validate Invoice' || CHR(10));
      --duplicate_check;
      --    populate_list;
      validate_invoice(piv_period_set_name);
      --ver1.21.2 start
      recalc_dist_amount;
      --ver1.21.2 end
      --ver1.11 start
      group_tax_lines;
      --ver1.11 end
      IF g_error_tab.COUNT > 0 THEN
        xxetn_common_error_pkg.add_error(pov_return_status   => l_err_ret_status,
                                         pov_error_msg       => l_error_message,
                                         pi_source_tab       => g_error_tab,
                                         pin_batch_id        => g_new_batch_id,
                                         pin_run_sequence_id => g_new_run_seq_id);
      END IF;
      print_stats_p;
      pon_retcode := g_retcode;
    ELSIF UPPER(piv_run_mode) = 'CONVERSION' THEN
      IF g_leg_operating_unit IS NOT NULL THEN
        print_log_message('Parameter Legacy Operating Unit will not be considered for this mode ');
        g_leg_operating_unit := NULL;
      END IF;
      IF g_leg_trasaction_type IS NOT NULL THEN
        print_log_message('Parameter Legacy Transaction Type will not be considered for this mode ');
        g_leg_trasaction_type := NULL;
      END IF;
      IF g_process_records IS NOT NULL THEN
        print_log_message('Parameter Reprocess Records will not be considered for this mode ');
        g_process_records := NULL;
      END IF;
      IF g_gl_date IS NOT NULL THEN
        print_log_message('Parameter GL Date will not be considered for this mode ');
        g_gl_date := NULL;
      END IF;
      IF g_batch_id IS NULL THEN
        l_err_code := 'ETN_AR_CHECK_PARAMETER';
        l_err_msg  := 'Parameter Batch Id is mandatory for run mode "CONVERSION"';
        print_log_message(l_err_msg);
        log_errors(piv_error_type    => 'ERR_VAL',
                   piv_error_code    => l_err_code,
                   piv_error_message => l_err_msg,
                   pov_return_status => l_log_ret_status,
                   pov_error_msg     => l_log_err_msg);
        RAISE l_err_excep;
      END IF;
      g_new_batch_id   := g_batch_id;
      g_new_run_seq_id := xxetn_run_sequences_s.NEXTVAL;
      xxetn_debug_pkg.add_debug('---------------------------------------------');
      xxetn_debug_pkg.add_debug('PROCEDURE: create_invoice' || CHR(10));
      create_invoice;
      IF g_error_tab.COUNT > 0 THEN
        xxetn_common_error_pkg.add_error(pov_return_status   => l_err_ret_status,
                                         pov_error_msg       => l_error_message,
                                         pi_source_tab       => g_error_tab,
                                         pin_batch_id        => g_new_batch_id,
                                         pin_run_sequence_id => g_new_run_seq_id);
      END IF;
      print_stats_p;
      pon_retcode := g_retcode;
    ELSIF UPPER(piv_run_mode) = 'RECONCILE' THEN
      print_log_message('In Reconciliation Mode');
      IF g_leg_operating_unit IS NOT NULL THEN
        print_log_message('Parameter Legacy Operating Unit will not be considered for this mode ');
        g_leg_operating_unit := NULL;
      END IF;
      IF g_leg_trasaction_type IS NOT NULL THEN
        print_log_message('Parameter Legacy Transaction Type will not be considered for this mode ');
        g_leg_trasaction_type := NULL;
      END IF;
      IF g_process_records IS NOT NULL THEN
        print_log_message('Parameter Reprocess Records will not be considered for this mode ');
        g_process_records := NULL;
      END IF;
      IF g_gl_date IS NOT NULL THEN
        print_log_message('Parameter GL Date will not be considered for this mode ');
        g_gl_date := NULL;
      END IF;
      g_new_batch_id   := g_batch_id;
      g_new_run_seq_id := NULL;
      print_stats_p;
    END IF; -- IF piv_run_mode
  EXCEPTION
    WHEN l_warn_excep THEN
      print_log_message('Main program procedure encounter user exception ' ||
                        SUBSTR(SQLERRM, 1, 150));
      pov_errbuf  := 'Error : Main program procedure encounter user exception. ' ||
                     SUBSTR(SQLERRM, 1, 150);
      pon_retcode := 1;
    WHEN l_err_excep THEN
      print_log_message('Main program procedure encounter user exception ' ||
                        SUBSTR(SQLERRM, 1, 150));
      pov_errbuf  := 'Error : Main program procedure encounter user exception. ' ||
                     SUBSTR(SQLERRM, 1, 150);
      pon_retcode := 2;
    WHEN OTHERS THEN
      print_log_message('Main program procedure encounter error ' ||
                        SUBSTR(SQLERRM, 1, 150));
      pov_errbuf  := 'Error : Main program procedure encounter error. ' ||
                     SUBSTR(SQLERRM, 1, 150);
      pon_retcode := 2;
  END main;

END xxar_invoices_pkg;
/
CREATE OR REPLACE PACKAGE BODY xxap_invoices_pkg
------------------------------------------------------------------------------------------
--    Owner        : EATON CORPORATION.
--    Application  : Account Payables
--    Schema       : APPS
--    Compile AS   : APPS
--    File Name    : XXAP_INVOICES_PKG.pkb
--    Date         : 24-Apr-2014
--    Author       : Archita Monga
--    Description  : Package Body for Supplier conversion
--
--    Version      : $ETNHeader: /CCSTORE/ccweb/C9909691/C9909691_AP_TOP_Development_View_2/vobs/AP_TOP/xxap/12.0.0/install/xxap_invoices_pkg.pkb /main/11 05-Aug-2015 03:20:20 C9909691  $
--
--    Parameters  :
--
--    Change History
--    Version     Created By       Date            Comments
--  ======================================================================================
--    v1.0      Archita Monga    24-Apr-2014     Creation
--    v1.1      Seema Machado    29-Jul-2014     Changes for CR34
--    v1.2      Seema Machado    18-Aug-2014     Changes for ETN_MAP_UNIT
--    v1.3      Seema Machado    21-Aug-2014     Changes for performance improvement
--    v1.4      Seema Machado    8-Sep-2014      Changes due to FOT
--    v1.5      Manpreet Singh   17-Mar-2015     GRNI changes
--    v1.6      Ankur Sharma     01-Jul-2015     Country Specific Changes for Poland, Italy,
--                                               Spain, Thailand. Updated as per TUT feedback from Mohit
--    v1.7      Aditya Bhagat    25-Aug-2015     Multiple chanes for CRs and Defects
--    v1.8      Ankur Sharma     14-Oct-2015     Defaulting value for CALC_TAX_DURING_IMPORT_FLAG
--                                               and ADD_TAX_TO_INV_AMT_FLAG
--    v1.9      Aditya Bhagat    16-Oct-2015     CR for Tax cross Reference, fix for process flag update
--                                               fix for including vendur number in create invoice procedure
--    v1.10     Aditya Bhagat    27Nov-2015      CR 336616
--    v1.11     Aditya Bhagat    11-Mar-2016      CR 339759
--    v1.12     Aditya Bhagat    16-Mar-2016     Change to update the prepay line type to MISC,
--                                               Commented the logic of using %like in project_id SQL
--                                               Change the Exchange rate type to User
--    v2.0      Aditya Bhagat    01-Apr-2016     CR#373588 change to Invoice holds additional lookup
--                                               Vertex tax code change lookup, mapping posted flag and control amt
--    v2.1      Aditya Bhagat    11-Apr-2016     Change to fix issue with duplicate voucher number
--    v3.0      Aditya Bhagat    14-Sep-2016     Changes for PMC412994
--    v4.0      Aditya Bhagat    14-Jan-2016     Changes to revert the CR for Tax
--    v5.0      Aditya Bhagat    23-May-2017     Changes for CR463429 
--   ====================================================================================
------------------------------------------------------------------------------------------
 AS
  -- global variables
  g_request_id      NUMBER DEFAULT fnd_global.conc_request_id;
  g_resp_id         NUMBER DEFAULT fnd_global.resp_id;
  g_prog_appl_id    NUMBER DEFAULT fnd_global.prog_appl_id;
  g_resp_appl_id    NUMBER DEFAULT fnd_global.resp_appl_id;
  g_conc_program_id NUMBER DEFAULT fnd_global.conc_program_id;
  g_user_id         NUMBER DEFAULT fnd_global.user_id;
  g_login_id        NUMBER DEFAULT fnd_global.login_id;
  g_org_id          NUMBER DEFAULT fnd_global.org_id;
  g_set_of_books_id NUMBER DEFAULT fnd_profile.VALUE('GL_SET_OF_BKS_ID');
  g_retcode         NUMBER;
  g_errbuff         VARCHAR2(1000);
  g_operating_unit  VARCHAR2(100);
  g_ou_lkp       CONSTANT VARCHAR2(50) := 'ETN_COMMON_OU_MAP';
  g_sysdate      CONSTANT DATE := SYSDATE;
  g_batch_source CONSTANT VARCHAR2(30) := 'CONVERSION';
  g_source_table      VARCHAR2(30);
  g_loaded_count      NUMBER;
  g_loaded_header_chk NUMBER := 0;
  g_loaded_line_chk   NUMBER := 0;
  g_err_cnt           NUMBER DEFAULT 1;
  g_tot_header_count  NUMBER;
  g_tot_lines_count   NUMBER;
  g_suc_count_head    NUMBER;
  g_suc_count_line    NUMBER;
  g_fail_header_count NUMBER;
  g_fail_lines_count  NUMBER;
  g_fail_count_head   NUMBER;
  g_fail_count_line   NUMBER;
  g_run_sequence_id   NUMBER;
  g_new               CONSTANT VARCHAR2(1) := 'N';
  g_error             CONSTANT VARCHAR2(1) := 'E';
  g_validated         CONSTANT VARCHAR2(1) := 'V';
  g_obsolete          CONSTANT VARCHAR2(1) := 'X';
  --v1.11
  --O- for records which are offset tax lines
  --S for records which will be summarized and should not
  --be carried forward
  g_offsets           CONSTANT VARCHAR2(1) := 'O';
  g_summarized        CONSTANT VARCHAR2(1) := 'S';
  --v1.11 ends here
  g_processed         CONSTANT VARCHAR2(1) := 'P';
  g_converted         CONSTANT VARCHAR2(1) := 'C';
  --v1.11 commented the below as it is not used anywhere
  --g_success           CONSTANT VARCHAR2(1) := 'S';
  g_yes               CONSTANT VARCHAR2(1) := 'Y';
  g_ricew_id          CONSTANT VARCHAR2(10) := 'CNV-0002';
  g_created_by_module CONSTANT VARCHAR2(10) := 'TCA_V1_API';
  g_init_msg_list     CONSTANT VARCHAR2(20) := fnd_api.g_true;
  g_run_mode        VARCHAR2(100);
  g_process_records VARCHAR2(100);
  g_gl_date         DATE;
  g_err_code        VARCHAR2(100);
  g_err_message     VARCHAR2(2000);
  g_failed_count    NUMBER;
  g_total_count     NUMBER;
  g_load_id         NUMBER;
  g_indx            NUMBER := 0;
  g_limit          CONSTANT NUMBER := fnd_profile.VALUE('ETN_FND_ERROR_TAB_LIMIT');
  g_err_imp        CONSTANT VARCHAR2(10) := 'ERR_IMP';
  g_err_val        CONSTANT VARCHAR2(10) := 'ERR_VAL';
  g_invoice_t      CONSTANT VARCHAR2(30) := 'XXAP_INVOICES_STG';
  g_invoice_line_t CONSTANT VARCHAR2(30) := 'XXAP_INVOICE_LINES_STG';
  g_invoice_hold_t CONSTANT VARCHAR2(30) := 'XXAP_INVOICE_HOLDS_STG';
  g_batch_id        NUMBER;
  g_new_batch_id    NUMBER;
  g_run_seq_id      NUMBER;
  g_source_tab      xxetn_common_error_pkg.g_source_tab_type;
  g_intf_staging_id xxetn_common_error.interface_staging_id%TYPE;
  g_src_keyname1    xxetn_common_error.source_keyname1%TYPE;
  g_src_keyvalue1   xxetn_common_error.source_keyvalue1%TYPE;
  g_src_keyname2    xxetn_common_error.source_keyname2%TYPE;
  g_src_keyvalue2   xxetn_common_error.source_keyvalue2%TYPE;
  g_src_keyname3    xxetn_common_error.source_keyname3%TYPE;
  g_src_keyvalue3   xxetn_common_error.source_keyvalue3%TYPE;
  g_src_keyname4    xxetn_common_error.source_keyname4%TYPE;
  g_src_keyvalue4   xxetn_common_error.source_keyvalue4%TYPE;
  g_src_keyname5    xxetn_common_error.source_keyname5%TYPE;
  g_src_keyvalue5   xxetn_common_error.source_keyvalue5%TYPE;
  --
  g_coa_error     CONSTANT VARCHAR2(30) := 'Error';
  g_coa_processed CONSTANT VARCHAR2(30) := 'Processed';

  -- ========================
  -- Procedure: print_log_message
  -- =============================================================================
  --   This procedure is used to write message to log file.
  -- =============================================================================
  --  Input Parameters :
  --    piv_message         : Message which needs to  be written in log file
  --  Output Parameters :
  --  Return     : Not applicable
  -- -----------------------------------------------------------------------------
  PROCEDURE print_log_message(piv_message IN VARCHAR2) IS
  BEGIN
    fnd_file.put_line(fnd_file.LOG, piv_message);
  END;

  --
  -- ========================
  -- Procedure: log_errors
  -- =============================================================================
  --   This procedure will log the errors in the error report using error
  --   framework
  -- =============================================================================
  --  Input Parameters :
  --    piv_source_column_name  : Column Name - column which has errored
  --    piv_source_column_value : Column Value - the value in the errored column
  --    piv_error_type          : Error Type - validation or import error
  --    piv_error_code          : Error Code - error at which point in execution
  --    piv_error_message       : Error Message - description of error
  --    piv_severity            : Severity
  --    piv_proposed_solution   : Proposed Solution
  --
  --  Output Parameters :
  --    pov_return_status  : Return Status - Success / Error
  --    pov_error_msg      : Error message in case of any failure
  -- -----------------------------------------------------------------------------
  PROCEDURE log_errors(pov_return_status       OUT NOCOPY VARCHAR2,
                       pov_error_msg           OUT NOCOPY VARCHAR2,
                       piv_source_column_name  IN xxetn_common_error.source_column_name%TYPE DEFAULT NULL,
                       piv_source_column_value IN xxetn_common_error.source_column_value%TYPE DEFAULT NULL,
                       piv_error_type          IN xxetn_common_error.error_type%TYPE,
                       piv_error_code          IN xxetn_common_error.ERROR_CODE%TYPE,
                       piv_error_message       IN xxetn_common_error.error_message%TYPE,
                       piv_severity            IN xxetn_common_error.severity%TYPE DEFAULT NULL,
                       piv_proposed_solution   IN xxetn_common_error.proposed_solution%TYPE DEFAULT NULL) IS
    l_return_status VARCHAR2(50);
    l_error_msg     VARCHAR2(2000);
  BEGIN
    pov_return_status := NULL;
    pov_error_msg     := NULL;
    xxetn_debug_pkg.add_debug('p_err_msg: ' || piv_source_column_name);
    xxetn_debug_pkg.add_debug('g_limit: ' || g_limit);
    xxetn_debug_pkg.add_debug('g_indx: ' || g_indx);
    --increment index for every new insertion in the error table
    g_indx := g_indx + 1;
    --assignment of the error record details into the table type
    g_source_tab(g_indx).source_table := g_source_table;
    g_source_tab(g_indx).interface_staging_id := g_intf_staging_id;
    g_source_tab(g_indx).source_keyname1 := g_src_keyname1;
    g_source_tab(g_indx).source_keyvalue1 := g_src_keyvalue1;
    g_source_tab(g_indx).source_keyname2 := g_src_keyname2;
    g_source_tab(g_indx).source_keyvalue2 := g_src_keyvalue2;
    g_source_tab(g_indx).source_keyname3 := g_src_keyname3;
    g_source_tab(g_indx).source_keyvalue3 := g_src_keyvalue3;
    g_source_tab(g_indx).source_keyname4 := g_src_keyname4;
    g_source_tab(g_indx).source_keyvalue4 := g_src_keyvalue4;
    g_source_tab(g_indx).source_keyname5 := g_src_keyname5;
    g_source_tab(g_indx).source_keyvalue5 := g_src_keyvalue5;
    g_source_tab(g_indx).source_column_name := piv_source_column_name;
    g_source_tab(g_indx).source_column_value := piv_source_column_value;
    g_source_tab(g_indx).error_type := piv_error_type;
    g_source_tab(g_indx).ERROR_CODE := piv_error_code;
    g_source_tab(g_indx).error_message := piv_error_message;
    g_source_tab(g_indx).severity := piv_severity;
    g_source_tab(g_indx).proposed_solution := piv_proposed_solution;

    IF MOD(g_indx, g_limit) = 0 THEN
      xxetn_common_error_pkg.add_error(pov_return_status => l_return_status -- OUT
                                      ,
                                       pov_error_msg     => l_error_msg -- OUT
                                      ,
                                       pi_source_tab     => g_source_tab
                                       -- IN  G_SOURCE_TAB_TYPE
                                      ,
                                       pin_batch_id => g_new_batch_id);
      g_source_tab.DELETE;
      pov_return_status := l_return_status;
      pov_error_msg     := l_error_msg;
      xxetn_debug_pkg.add_debug('Calling xxetn_common_error_pkg.add_error ' ||
                                l_return_status || ', ' || l_error_msg);
      g_indx := 0;
    END IF;
  EXCEPTION
    WHEN OTHERS THEN
      xxetn_debug_pkg.add_debug('Error: Exception occured in log_errors procedure ' ||
                                SUBSTR(SQLERRM, 1, 240));
  END log_errors;

  --
  -- ========================
  -- Procedure: print_stat
  -- =============================================================================
  --   This procedure print_stat
  -- =============================================================================
  --  Input Parameters :
  --    None
  --  Output Parameters :
  --    None
  -- -----------------------------------------------------------------------------
  --
  PROCEDURE print_stat AS
    l_err_message    VARCHAR2(2000);
    l_log_err_msg    VARCHAR2(2000);
    l_log_ret_status VARCHAR2(50);
    l_pass_val1      NUMBER := 0;
    l_err_val1       NUMBER := 0;
    l_tot_val1       NUMBER := 0;
    l_pass_val2      NUMBER := 0;
    l_err_val2       NUMBER := 0;
    l_tot_val2       NUMBER := 0;
    l_pass_imp1      NUMBER := 0;
    l_err_imp1       NUMBER := 0;
    l_tot_imp1       NUMBER := 0;
    l_pass_imp2      NUMBER := 0;
    l_err_imp2       NUMBER := 0;
    l_tot_imp2       NUMBER := 0;
  BEGIN
    xxetn_debug_pkg.add_debug(' + Print_stat + ');
    xxetn_debug_pkg.add_debug('Program Name : Eaton AP Invoices Conversion Program');
    fnd_file.put_line(fnd_file.output,
                      'Program Name : Eaton AP Invoices Conversion Program');
    fnd_file.put_line(fnd_file.output,
                      'Request ID   : ' || TO_CHAR(g_request_id));
    fnd_file.put_line(fnd_file.output,
                      'Report Date  : ' ||
                      TO_CHAR(SYSDATE, 'DD-MON-RRRR HH:MI:SS AM'));
    fnd_file.put_line(fnd_file.output,
                      '-------------------------------------------------------------------------------------------------');
    fnd_file.put_line(fnd_file.output, 'Parameters  : ');
    fnd_file.put_line(fnd_file.output,
                      '---------------------------------------------');
    fnd_file.put_line(fnd_file.output,
                      'Run Mode            : ' || g_run_mode);
    fnd_file.put_line(fnd_file.output,
                      'Operating Unit      : ' || g_operating_unit);
    fnd_file.put_line(fnd_file.output,
                      'Batch ID            : ' || g_batch_id);
    fnd_file.put_line(fnd_file.output,
                      'Process records     : ' || g_process_records);
    fnd_file.put_line(fnd_file.output,
                      'GL Date             : ' || g_gl_date);
    fnd_file.put_line(fnd_file.output,
                      '===================================================================================================');
    fnd_file.put_line(fnd_file.output,
                      'Statistics (' || g_run_mode || '):');

    --count for all the records processed for header
    SELECT COUNT(1)
      INTO l_tot_val1
      FROM xxap_invoices_stg stg
     WHERE stg.batch_id = NVL(g_new_batch_id, stg.batch_id)
       AND stg.run_sequence_id = NVL(g_run_seq_id, stg.run_sequence_id);

    --count for all the records processed for lines
    SELECT COUNT(1)
      INTO l_tot_val2
      FROM xxap_invoice_lines_stg stg
     WHERE stg.batch_id = NVL(g_new_batch_id, stg.batch_id)
       AND stg.run_sequence_id = NVL(g_run_seq_id, stg.run_sequence_id);

    --count for all the header records which errored out while validating
    SELECT COUNT(1)
      INTO l_err_val1
      FROM xxap_invoices_stg stg
     WHERE stg.batch_id = NVL(g_new_batch_id, stg.batch_id)
       AND stg.run_sequence_id = NVL(g_run_seq_id, stg.run_sequence_id)
       AND stg.process_flag = g_error
       AND stg.error_type = g_err_val;

    --count for all the line records which errored out while validating
    SELECT COUNT(1)
      INTO l_err_val2
      FROM xxap_invoice_lines_stg stg
     WHERE stg.batch_id = NVL(g_new_batch_id, stg.batch_id)
       AND stg.run_sequence_id = NVL(g_run_seq_id, stg.run_sequence_id)
       AND stg.process_flag = g_error
       AND stg.error_type = g_err_val;

    --count for all the records which errored out while importing
    SELECT COUNT(1)
      INTO l_err_imp1
      FROM xxap_invoices_stg stg
     WHERE stg.batch_id = NVL(g_new_batch_id, stg.batch_id)
       AND stg.run_sequence_id = NVL(g_run_seq_id, stg.run_sequence_id)
       AND stg.process_flag = g_error
       AND stg.error_type = g_err_imp;

    --count for all the records which errored out while importing
    SELECT COUNT(1)
      INTO l_err_imp2
      FROM xxap_invoice_lines_stg stg
     WHERE stg.batch_id = NVL(g_new_batch_id, stg.batch_id)
       AND stg.run_sequence_id = NVL(g_run_seq_id, stg.run_sequence_id)
       AND stg.process_flag = g_error
       AND stg.error_type = g_err_imp;

    --count for all the records which successfully got validated
    SELECT COUNT(1)
      INTO l_pass_val1
      FROM xxap_invoices_stg stg
     WHERE stg.batch_id = NVL(g_new_batch_id, stg.batch_id)
       AND stg.run_sequence_id = NVL(g_run_seq_id, stg.run_sequence_id)
       AND stg.process_flag = g_validated;

    --count for all the line records which successfully got validated
    SELECT COUNT(1)
      INTO l_pass_val2
      FROM xxap_invoice_lines_stg stg
     WHERE stg.batch_id = NVL(g_new_batch_id, stg.batch_id)
       AND stg.run_sequence_id = NVL(g_run_seq_id, stg.run_sequence_id)
       AND stg.process_flag = g_validated;

    --count for all the header records which successfully got converted
    SELECT COUNT(1)
      INTO l_pass_imp1
      FROM xxap_invoices_stg stg
     WHERE stg.batch_id = NVL(g_new_batch_id, stg.batch_id)
       AND stg.run_sequence_id = NVL(g_run_seq_id, stg.run_sequence_id)
       AND stg.process_flag = g_converted;

    --count for all the line records which successfully got converted
    SELECT COUNT(1)
      INTO l_pass_imp2
      FROM xxap_invoice_lines_stg stg
     WHERE stg.batch_id = NVL(g_new_batch_id, stg.batch_id)
       AND stg.run_sequence_id = NVL(g_run_seq_id, stg.run_sequence_id)
       AND stg.process_flag = g_converted;

    IF g_run_mode = 'LOAD_DATA' THEN
      fnd_file.put_line(fnd_file.output,
                        'Records Submitted       : ' || g_total_count);
      fnd_file.put_line(fnd_file.output,
                        'Records Extracted       : ' ||
                        (g_total_count - g_failed_count));
      fnd_file.put_line(fnd_file.output,
                        'Records Errored         : ' || g_failed_count);
    ELSIF g_run_mode = 'VALIDATE' THEN
      fnd_file.put_line(fnd_file.output,
                        'Header Records Submitted  : ' || l_tot_val1);
      fnd_file.put_line(fnd_file.output,
                        'Header Records Validated  : ' || l_pass_val1);
      fnd_file.put_line(fnd_file.output,
                        'Header Records Errored    : ' || l_err_val1);
      fnd_file.put_line(fnd_file.output,
                        'Line Records Submitted  : ' || l_tot_val2);
      fnd_file.put_line(fnd_file.output,
                        'Line Records Validated  : ' || l_pass_val2);
      fnd_file.put_line(fnd_file.output,
                        'Line Records Errored    : ' || l_err_val2);
    ELSIF g_run_mode = 'CONVERSION' THEN
      fnd_file.put_line(fnd_file.output,
                        'Header Records Submitted  : ' || l_tot_val1);
      fnd_file.put_line(fnd_file.output,
                        'Header Records Imported   : ' || l_pass_imp1);
      fnd_file.put_line(fnd_file.output,
                        'Header Records Errored    : ' || l_err_imp1);
      fnd_file.put_line(fnd_file.output,
                        'Line Records Submitted  : ' || l_tot_val2);
      fnd_file.put_line(fnd_file.output,
                        'Line Records Imported   : ' || l_pass_imp2);
      fnd_file.put_line(fnd_file.output,
                        'Line Records Errored    : ' || l_err_imp2);
    ELSIF g_run_mode = 'RECONCILE' THEN
      fnd_file.put_line(fnd_file.output,
                        'Header Records Submitted              : ' ||
                        l_tot_val1);
      fnd_file.put_line(fnd_file.output,
                        'Header Records Imported               : ' ||
                        l_pass_imp1);
      fnd_file.put_line(fnd_file.output,
                        'Header Records Errored in Validation  : ' ||
                        l_err_val1);
      fnd_file.put_line(fnd_file.output,
                        'Header Records Errored in Import      : ' ||
                        l_err_imp1);
      fnd_file.put_line(fnd_file.output,
                        'Line Records Submitted              : ' ||
                        l_tot_val1);
      fnd_file.put_line(fnd_file.output,
                        'Line Records Imported               : ' ||
                        l_pass_imp1);
      fnd_file.put_line(fnd_file.output,
                        'Line Records Errored in Validation  : ' ||
                        l_err_val1);
      fnd_file.put_line(fnd_file.output,
                        'Line Records Errored in Import      : ' ||
                        l_err_imp1);
    END IF;

    fnd_file.put_line(fnd_file.output, CHR(10));
    fnd_file.put_line(fnd_file.output,
                      '===================================================================================================');
    xxetn_debug_pkg.add_debug(' - Print_stat - ');
  EXCEPTION
    WHEN OTHERS THEN
      l_err_message := 'Error : print stat procedure encounter error. ' ||
                       SUBSTR(SQLERRM, 1, 150);
      xxetn_debug_pkg.add_debug(' - Print_stat - ');
      print_log_message(l_err_message);
      g_retcode := 2;
  END print_stat;

  --
  -- ========================
  -- Procedure: load_data
  -- =============================================================================
  --   This procedure will load data from 11i staging table to r12 staging table
  --   basing
  -- =============================================================================
  --  Input Parameters :
  --
  --
  --  Output Parameters :
  --    pov_return_status  : Return Status - Success / Error
  --    pov_error_msg      : Error message in case of any failure
  -- -----------------------------------------------------------------------------
  PROCEDURE load_data IS
    TYPE invoice_hdr_ext_rec IS RECORD(
      interface_txn_id               xxap_invoices_ext_r12.interface_txn_id%TYPE,
      batch_id                       xxap_invoices_ext_r12.batch_id%TYPE,
      load_id                        xxap_invoices_ext_r12.load_id%TYPE,
      run_sequence_id                xxap_invoices_ext_r12.run_sequence_id%TYPE,
      leg_accts_pay_code_concat      xxap_invoices_ext_r12.leg_accts_pay_code_concat%TYPE,
      leg_apply_advances_flag        xxap_invoices_ext_r12.leg_apply_advances_flag%TYPE,
      leg_attribute_category         xxap_invoices_ext_r12.leg_attribute_category%TYPE,
      leg_attribute1                 xxap_invoices_ext_r12.leg_attribute1%TYPE,
      leg_attribute10                xxap_invoices_ext_r12.leg_attribute10%TYPE,
      leg_attribute11                xxap_invoices_ext_r12.leg_attribute11%TYPE,
      leg_attribute12                xxap_invoices_ext_r12.leg_attribute12%TYPE,
      leg_attribute13                xxap_invoices_ext_r12.leg_attribute13%TYPE,
      leg_attribute14                xxap_invoices_ext_r12.leg_attribute14%TYPE,
      leg_attribute15                xxap_invoices_ext_r12.leg_attribute15%TYPE,
      leg_attribute2                 xxap_invoices_ext_r12.leg_attribute2%TYPE,
      leg_attribute3                 xxap_invoices_ext_r12.leg_attribute3%TYPE,
      leg_attribute4                 xxap_invoices_ext_r12.leg_attribute4%TYPE,
      leg_attribute5                 xxap_invoices_ext_r12.leg_attribute5%TYPE,
      leg_attribute6                 xxap_invoices_ext_r12.leg_attribute6%TYPE,
      leg_attribute7                 xxap_invoices_ext_r12.leg_attribute7%TYPE,
      leg_attribute8                 xxap_invoices_ext_r12.leg_attribute8%TYPE,
      leg_attribute9                 xxap_invoices_ext_r12.leg_attribute9%TYPE,
      leg_cust_registration_code     xxap_invoices_ext_r12.leg_cust_registration_code%TYPE,
      leg_cust_registration_number   xxap_invoices_ext_r12.leg_cust_registration_number%TYPE,
      leg_delivery_channel_code      xxap_invoices_ext_r12.leg_delivery_channel_code%TYPE,
      leg_description                xxap_invoices_ext_r12.leg_description%TYPE,
      leg_document_sub_type          xxap_invoices_ext_r12.leg_document_sub_type%TYPE,
      leg_exchange_date              xxap_invoices_ext_r12.leg_exchange_date%TYPE,
      leg_exchange_rate              xxap_invoices_ext_r12.leg_exchange_rate%TYPE,
      leg_exchange_rate_type         xxap_invoices_ext_r12.leg_exchange_rate_type%TYPE,
      leg_exclusive_payment_flag     xxap_invoices_ext_r12.leg_exclusive_payment_flag%TYPE,
      leg_gl_date                    xxap_invoices_ext_r12.leg_gl_date%TYPE,
      leg_global_attribute_category  xxap_invoices_ext_r12.leg_global_attribute_category%TYPE,
      leg_global_attribute1          xxap_invoices_ext_r12.leg_global_attribute1%TYPE,
      leg_global_attribute10         xxap_invoices_ext_r12.leg_global_attribute10%TYPE,
      leg_global_attribute11         xxap_invoices_ext_r12.leg_global_attribute11%TYPE,
      leg_global_attribute12         xxap_invoices_ext_r12.leg_global_attribute12%TYPE,
      leg_global_attribute13         xxap_invoices_ext_r12.leg_global_attribute13%TYPE,
      leg_global_attribute14         xxap_invoices_ext_r12.leg_global_attribute14%TYPE,
      leg_global_attribute15         xxap_invoices_ext_r12.leg_global_attribute15%TYPE,
      leg_global_attribute16         xxap_invoices_ext_r12.leg_global_attribute16%TYPE,
      leg_global_attribute17         xxap_invoices_ext_r12.leg_global_attribute17%TYPE,
      leg_global_attribute18         xxap_invoices_ext_r12.leg_global_attribute18%TYPE,
      leg_global_attribute19         xxap_invoices_ext_r12.leg_global_attribute19%TYPE,
      leg_global_attribute2          xxap_invoices_ext_r12.leg_global_attribute2%TYPE,
      leg_global_attribute20         xxap_invoices_ext_r12.leg_global_attribute20%TYPE,
      leg_global_attribute3          xxap_invoices_ext_r12.leg_global_attribute3%TYPE,
      leg_global_attribute4          xxap_invoices_ext_r12.leg_global_attribute4%TYPE,
      leg_global_attribute5          xxap_invoices_ext_r12.leg_global_attribute5%TYPE,
      leg_global_attribute6          xxap_invoices_ext_r12.leg_global_attribute6%TYPE,
      leg_global_attribute7          xxap_invoices_ext_r12.leg_global_attribute7%TYPE,
      leg_global_attribute8          xxap_invoices_ext_r12.leg_global_attribute8%TYPE,
      leg_global_attribute9          xxap_invoices_ext_r12.leg_global_attribute9%TYPE,
      leg_inv_currency_code          xxap_invoices_ext_r12.leg_inv_currency_code%TYPE,
      leg_inv_includes_prepay_flag   xxap_invoices_ext_r12.leg_inv_includes_prepay_flag%TYPE,
      leg_inv_type_lookup_code       xxap_invoices_ext_r12.leg_inv_type_lookup_code%TYPE,
      leg_invoice_amount             xxap_invoices_ext_r12.leg_invoice_amount%TYPE,
      leg_invoice_date               xxap_invoices_ext_r12.leg_invoice_date%TYPE,
      leg_invoice_num                xxap_invoices_ext_r12.leg_invoice_num%TYPE,
      leg_invoice_received_date      xxap_invoices_ext_r12.leg_invoice_received_date%TYPE,
      leg_legal_entity_name          xxap_invoices_ext_r12.leg_legal_entity_name%TYPE,
      leg_net_of_retainage_flag      xxap_invoices_ext_r12.leg_net_of_retainage_flag%TYPE,
      leg_operating_unit             xxap_invoices_ext_r12.leg_operating_unit%TYPE,
      leg_pay_awt_group_name         xxap_invoices_ext_r12.leg_pay_awt_group_name%TYPE,
      leg_pay_group_lookup_cod       xxap_invoices_ext_r12.leg_pay_group_lookup_cod%TYPE,
      leg_pay_proc_trxn_type_code    xxap_invoices_ext_r12.leg_pay_proc_trxn_type_code%TYPE,
      leg_payment_cross_rate         xxap_invoices_ext_r12.leg_payment_cross_rate%TYPE,
      leg_payment_cross_rate_date    xxap_invoices_ext_r12.leg_payment_cross_rate_date%TYPE,
      leg_payment_cross_rate_type    xxap_invoices_ext_r12.leg_payment_cross_rate_type%TYPE,
      leg_payment_currency_code      xxap_invoices_ext_r12.leg_payment_currency_code%TYPE,
      leg_payment_method_code        xxap_invoices_ext_r12.leg_payment_method_code%TYPE,
      leg_payment_method_lookup_code xxap_invoices_ext_r12.leg_payment_method_lookup_code%TYPE,
      leg_payment_reason_code        xxap_invoices_ext_r12.leg_payment_reason_code%TYPE,
      leg_po_number                  xxap_invoices_ext_r12.leg_po_number%TYPE,
      leg_port_of_entry_code         xxap_invoices_ext_r12.leg_port_of_entry_code%TYPE,
      leg_prepay_apply_amount        xxap_invoices_ext_r12.leg_prepay_apply_amount%TYPE,
      leg_prepay_line_num            xxap_invoices_ext_r12.leg_prepay_line_num%TYPE,
      leg_product_table              xxap_invoices_ext_r12.leg_product_table%TYPE,
      leg_reference_1                xxap_invoices_ext_r12.leg_reference_1%TYPE,
      leg_reference_2                xxap_invoices_ext_r12.leg_reference_2%TYPE,
      leg_reference_key1             xxap_invoices_ext_r12.leg_reference_key1%TYPE,
      leg_reference_key2             xxap_invoices_ext_r12.leg_reference_key2%TYPE,
      leg_reference_key3             xxap_invoices_ext_r12.leg_reference_key3%TYPE,
      leg_reference_key4             xxap_invoices_ext_r12.leg_reference_key4%TYPE,
      leg_reference_key5             xxap_invoices_ext_r12.leg_reference_key5%TYPE,
      leg_remit_to_supplier_num      xxap_invoices_ext_r12.leg_remit_to_supplier_num%TYPE,
      leg_remit_to_supplier_site     xxap_invoices_ext_r12.leg_remit_to_supplier_site%TYPE,
      leg_remittance_message1        xxap_invoices_ext_r12.leg_remittance_message1%TYPE,
      leg_remittance_message2        xxap_invoices_ext_r12.leg_remittance_message2%TYPE,
      leg_remittance_message3        xxap_invoices_ext_r12.leg_remittance_message3%TYPE,
      leg_supplier_tax_exchange_rate xxap_invoices_ext_r12.leg_supplier_tax_exchange_rate%TYPE,
      leg_supplier_tax_invoice_date  xxap_invoices_ext_r12.leg_supplier_tax_invoice_date%TYPE,
      leg_supplier_tax_invoice_num   xxap_invoices_ext_r12.leg_supplier_tax_invoice_num%TYPE,
      leg_tax_invoice_internal_seq   xxap_invoices_ext_r12.leg_tax_invoice_internal_seq%TYPE,
      leg_tax_invoice_recording_date xxap_invoices_ext_r12.leg_tax_invoice_recording_date%TYPE,
      leg_taxation_country           xxap_invoices_ext_r12.leg_taxation_country%TYPE,
      leg_terms_date                 xxap_invoices_ext_r12.leg_terms_date%TYPE,
      leg_terms_name                 xxap_invoices_ext_r12.leg_terms_name%TYPE,
      leg_unique_remit_identifier    xxap_invoices_ext_r12.leg_unique_remit_identifier%TYPE,
      leg_uri_check_digit            xxap_invoices_ext_r12.leg_uri_check_digit%TYPE,
      leg_ussgl_transaction_code     xxap_invoices_ext_r12.leg_ussgl_transaction_code%TYPE,
      leg_vat_code                   xxap_invoices_ext_r12.leg_vat_code%TYPE,
      leg_vendor_num                 xxap_invoices_ext_r12.leg_vendor_num%TYPE,
      leg_vendor_site_code           xxap_invoices_ext_r12.leg_vendor_site_code%TYPE,
      leg_voucher_num                xxap_invoices_ext_r12.leg_voucher_num%TYPE,
      leg_payment_function           xxap_invoices_ext_r12.leg_payment_function%TYPE,
      leg_payment_priority           xxap_invoices_ext_r12.leg_payment_priority%TYPE,
      leg_payment_reason_comments    xxap_invoices_ext_r12.leg_payment_reason_comments%TYPE,
      leg_request_id                 xxap_invoices_ext_r12.leg_request_id%TYPE,
      leg_seq_num                    xxap_invoices_ext_r12.leg_seq_num%TYPE,
      leg_source_system              xxap_invoices_ext_r12.leg_source_system%TYPE,
      leg_process_flag               xxap_invoices_ext_r12.leg_process_flag%TYPE,
      leg_awt_group_name             xxap_invoices_ext_r12.leg_awt_group_name%TYPE,
      leg_prepay_num                 xxap_invoices_ext_r12.leg_prepay_num%TYPE,
      leg_prepay_dist_num            xxap_invoices_ext_r12.leg_prepay_dist_num%TYPE,
      leg_prepay_gl_date             xxap_invoices_ext_r12.leg_prepay_gl_date%TYPE,
      leg_invoice_incl_prepay_flag   xxap_invoices_ext_r12.leg_invoice_incl_prepay_flag%TYPE,
      prepay_line_num                xxap_invoices_ext_r12.prepay_line_num%TYPE,
      product_table                  xxap_invoices_ext_r12.product_table%TYPE,
      leg_bank_charge_bearer         xxap_invoices_ext_r12.leg_bank_charge_bearer%TYPE,
      leg_unique_remittance_id       xxap_invoices_ext_r12.leg_unique_remittance_id%TYPE,
      leg_settlement_priority        xxap_invoices_ext_r12.leg_settlement_priority%TYPE,
      leg_requester_employee_num     xxap_invoices_ext_r12.leg_requester_employee_num%TYPE,
      leg_remit_to_supplier_name     xxap_invoices_ext_r12.leg_remit_to_supplier_name%TYPE,
      leg_due_date                   xxap_invoices_ext_r12.leg_due_date%TYPE,
      accts_pay_code_combination_id  xxap_invoices_ext_r12.accts_pay_code_combination_id%TYPE,
      accts_pay_code_concatenated    xxap_invoices_ext_r12.accts_pay_code_concatenated%TYPE,
      amount_applicable_to_discount  xxap_invoices_ext_r12.amount_applicable_to_discount%TYPE,
      attribute_category             xxap_invoices_ext_r12.attribute_category%TYPE,
      attribute1                     xxap_invoices_ext_r12.attribute1%TYPE,
      attribute10                    xxap_invoices_ext_r12.attribute10%TYPE,
      attribute11                    xxap_invoices_ext_r12.attribute11%TYPE,
      attribute12                    xxap_invoices_ext_r12.attribute12%TYPE,
      attribute13                    xxap_invoices_ext_r12.attribute13%TYPE,
      attribute14                    xxap_invoices_ext_r12.attribute14%TYPE,
      attribute15                    xxap_invoices_ext_r12.attribute15%TYPE,
      attribute2                     xxap_invoices_ext_r12.attribute2%TYPE,
      attribute3                     xxap_invoices_ext_r12.attribute3%TYPE,
      attribute4                     xxap_invoices_ext_r12.attribute4%TYPE,
      attribute5                     xxap_invoices_ext_r12.attribute5%TYPE,
      attribute6                     xxap_invoices_ext_r12.attribute6%TYPE,
      attribute7                     xxap_invoices_ext_r12.attribute7%TYPE,
      attribute8                     xxap_invoices_ext_r12.attribute8%TYPE,
      attribute9                     xxap_invoices_ext_r12.attribute9%TYPE,
      global_attribute_category      xxap_invoices_ext_r12.global_attribute_category%TYPE,
      global_attribute1              xxap_invoices_ext_r12.global_attribute1%TYPE,
      global_attribute10             xxap_invoices_ext_r12.global_attribute10%TYPE,
      global_attribute11             xxap_invoices_ext_r12.global_attribute11%TYPE,
      global_attribute12             xxap_invoices_ext_r12.global_attribute12%TYPE,
      global_attribute13             xxap_invoices_ext_r12.global_attribute13%TYPE,
      global_attribute14             xxap_invoices_ext_r12.global_attribute14%TYPE,
      global_attribute15             xxap_invoices_ext_r12.global_attribute15%TYPE,
      global_attribute16             xxap_invoices_ext_r12.global_attribute16%TYPE,
      global_attribute17             xxap_invoices_ext_r12.global_attribute17%TYPE,
      global_attribute18             xxap_invoices_ext_r12.global_attribute18%TYPE,
      global_attribute19             xxap_invoices_ext_r12.global_attribute19%TYPE,
      global_attribute2              xxap_invoices_ext_r12.global_attribute2%TYPE,
      global_attribute20             xxap_invoices_ext_r12.global_attribute20%TYPE,
      global_attribute3              xxap_invoices_ext_r12.global_attribute3%TYPE,
      global_attribute4              xxap_invoices_ext_r12.global_attribute4%TYPE,
      global_attribute5              xxap_invoices_ext_r12.global_attribute5%TYPE,
      global_attribute6              xxap_invoices_ext_r12.global_attribute6%TYPE,
      global_attribute7              xxap_invoices_ext_r12.global_attribute7%TYPE,
      global_attribute8              xxap_invoices_ext_r12.global_attribute8%TYPE,
      global_attribute9              xxap_invoices_ext_r12.global_attribute9%TYPE,
      calc_tax_during_import_flag    xxap_invoices_ext_r12.calc_tax_during_import_flag%TYPE,
      gl_date                        xxap_invoices_ext_r12.gl_date%TYPE,
      goods_received_date            xxap_invoices_ext_r12.goods_received_date%TYPE,
      invoice_date                   xxap_invoices_ext_r12.invoice_date%TYPE,
      invoice_num                    xxap_invoices_ext_r12.invoice_num%TYPE,
      legal_entity_id                xxap_invoices_ext_r12.legal_entity_id%TYPE,
      legal_entity_name              xxap_invoices_ext_r12.legal_entity_name%TYPE,
      operating_unit                 xxap_invoices_ext_r12.operating_unit%TYPE,
      org_id                         xxap_invoices_ext_r12.org_id%TYPE,
      pay_awt_group_name             xxap_invoices_ext_r12.pay_awt_group_name%TYPE,
      pay_group_lookup_code          xxap_invoices_ext_r12.pay_group_lookup_code%TYPE,
      pay_proc_trxn_type_code        xxap_invoices_ext_r12.pay_proc_trxn_type_code%TYPE,
      payment_method_code            xxap_invoices_ext_r12.payment_method_code%TYPE,
      payment_method_lookup_code     xxap_invoices_ext_r12.payment_method_lookup_code%TYPE,
      payment_reason_code            xxap_invoices_ext_r12.payment_reason_code%TYPE,
      remit_to_supplier_id           xxap_invoices_ext_r12.remit_to_supplier_id%TYPE,
      remit_to_supplier_name         xxap_invoices_ext_r12.remit_to_supplier_name%TYPE,
      remit_to_supplier_site_id      xxap_invoices_ext_r12.remit_to_supplier_site_id%TYPE,
      SOURCE                         xxap_invoices_ext_r12.SOURCE%TYPE,
      terms_date                     xxap_invoices_ext_r12.terms_date%TYPE,
      terms_name                     xxap_invoices_ext_r12.terms_name%TYPE,
      workflow_flag                  xxap_invoices_ext_r12.workflow_flag%TYPE,
      GROUP_ID                       xxap_invoices_ext_r12.GROUP_ID%TYPE,
      last_updated_by                xxap_invoices_ext_r12.last_updated_by%TYPE,
      last_updated_date              xxap_invoices_ext_r12.last_updated_date%TYPE,
      last_update_login              xxap_invoices_ext_r12.last_update_login%TYPE,
      program_application_id         xxap_invoices_ext_r12.program_application_id%TYPE,
      program_id                     xxap_invoices_ext_r12.program_id%TYPE,
      program_update_date            xxap_invoices_ext_r12.program_update_date%TYPE,
      request_id                     xxap_invoices_ext_r12.request_id%TYPE,
      process_flag                   xxap_invoices_ext_r12.process_flag%TYPE,
      error_type                     xxap_invoices_ext_r12.error_type%TYPE,
      plant_segment                  VARCHAR2(240),
      leg_accts_pay_code_comb_id     xxap_invoices_ext_r12.leg_accts_pay_code_comb_id%TYPE,
      leg_invoice_id                 xxap_invoices_ext_r12.leg_invoice_id%TYPE,
      leg_org_id                     xxap_invoices_ext_r12.leg_org_id%TYPE,
      leg_po_header_id               xxap_invoices_ext_r12.leg_po_header_id%TYPE,
      leg_term_id                    xxap_invoices_ext_r12.leg_term_id%TYPE,
      leg_vat_id                     xxap_invoices_ext_r12.leg_vat_id%TYPE,
      leg_vendor_id                  xxap_invoices_ext_r12.leg_vendor_id%TYPE,
      leg_vendor_site_id             xxap_invoices_ext_r12.leg_vendor_site_id%TYPE,
      leg_awt_group_id               xxap_invoices_ext_r12.leg_awt_group_id%TYPE,
      leg_requester_id               xxap_invoices_ext_r12.leg_requester_id%TYPE,
      leg_doc_category_code          xxap_invoices_ext_r12.leg_doc_category_code%TYPE,
      leg_doc_sequence_id            xxap_invoices_ext_r12.leg_doc_sequence_id%TYPE,
      leg_doc_sequence_value         xxap_invoices_ext_r12.leg_doc_sequence_value%TYPE,
      leg_set_of_books_name          xxap_invoices_ext_r12.leg_set_of_books_name%TYPE,
      leg_set_of_books_id            xxap_invoices_ext_r12.leg_set_of_books_id%TYPE,
      leg_wfapproval_status          xxap_invoices_ext_r12.leg_wfapproval_status%TYPE,
      leg_future_date_inv            xxap_invoices_ext_r12.leg_future_date_inv%TYPE);

    TYPE invoice_line_ext_rec IS RECORD(
      interface_txn_id               xxap_invoice_lines_ext_r12.interface_txn_id%TYPE,
      batch_id                       xxap_invoice_lines_ext_r12.batch_id%TYPE,
      load_id                        xxap_invoice_lines_ext_r12.load_id%TYPE,
      run_sequence_id                xxap_invoice_lines_ext_r12.run_sequence_id%TYPE,
      leg_invoice_num                xxap_invoice_lines_ext_r12.leg_invoice_num%TYPE,
      leg_vendor_num                 xxap_invoice_lines_ext_r12.leg_vendor_num%TYPE,
      leg_operating_unit             xxap_invoice_lines_ext_r12.leg_operating_unit%TYPE,
      leg_line_number                xxap_invoice_lines_ext_r12.leg_line_number%TYPE,
      line_number                    xxap_invoice_lines_ext_r12.line_number%TYPE,
      leg_line_type_lookup_code      xxap_invoice_lines_ext_r12.leg_line_type_lookup_code%TYPE,
      line_type_lookup_code          xxap_invoice_lines_ext_r12.line_type_lookup_code%TYPE,
      leg_amount                     xxap_invoice_lines_ext_r12.leg_amount%TYPE,
      amount                         xxap_invoice_lines_ext_r12.amount%TYPE,
      leg_accounting_date            xxap_invoice_lines_ext_r12.leg_accounting_date%TYPE,
      accounting_date                xxap_invoice_lines_ext_r12.accounting_date%TYPE,
      leg_description                xxap_invoice_lines_ext_r12.leg_description%TYPE,
      amount_includes_tax_flag       xxap_invoice_lines_ext_r12.amount_includes_tax_flag%TYPE,
      leg_tax_code                   xxap_invoice_lines_ext_r12.leg_tax_code%TYPE,
      tax_code                       xxap_invoice_lines_ext_r12.tax_code%TYPE,
      leg_quantity_invoiced          xxap_invoice_lines_ext_r12.leg_quantity_invoiced%TYPE,
      leg_ship_to_location_code      xxap_invoice_lines_ext_r12.leg_ship_to_location_code%TYPE,
      dist_code_concatenated         xxap_invoice_lines_ext_r12.dist_code_concatenated%TYPE,
      dist_code_concatenated_id      xxap_invoice_lines_ext_r12.dist_code_concatenated_id%TYPE,
      awt_group_name                 xxap_invoice_lines_ext_r12.awt_group_name%TYPE,
      attribute_category             xxap_invoice_lines_ext_r12.attribute_category%TYPE,
      attribute1                     xxap_invoice_lines_ext_r12.attribute1%TYPE,
      attribute2                     xxap_invoice_lines_ext_r12.attribute2%TYPE,
      attribute3                     xxap_invoice_lines_ext_r12.attribute3%TYPE,
      attribute4                     xxap_invoice_lines_ext_r12.attribute4%TYPE,
      attribute5                     xxap_invoice_lines_ext_r12.attribute5%TYPE,
      attribute6                     xxap_invoice_lines_ext_r12.attribute6%TYPE,
      attribute7                     xxap_invoice_lines_ext_r12.attribute7%TYPE,
      attribute8                     xxap_invoice_lines_ext_r12.attribute8%TYPE,
      attribute9                     xxap_invoice_lines_ext_r12.attribute9%TYPE,
      attribute10                    xxap_invoice_lines_ext_r12.attribute10%TYPE,
      attribute11                    xxap_invoice_lines_ext_r12.attribute11%TYPE,
      attribute12                    xxap_invoice_lines_ext_r12.attribute12%TYPE,
      attribute13                    xxap_invoice_lines_ext_r12.attribute13%TYPE,
      attribute14                    xxap_invoice_lines_ext_r12.attribute14%TYPE,
      attribute15                    xxap_invoice_lines_ext_r12.attribute15%TYPE,
      global_attribute_category      xxap_invoice_lines_ext_r12.global_attribute_category%TYPE,
      global_attribute1              xxap_invoice_lines_ext_r12.global_attribute1%TYPE,
      global_attribute2              xxap_invoice_lines_ext_r12.global_attribute2%TYPE,
      global_attribute3              xxap_invoice_lines_ext_r12.global_attribute3%TYPE,
      global_attribute4              xxap_invoice_lines_ext_r12.global_attribute4%TYPE,
      global_attribute5              xxap_invoice_lines_ext_r12.global_attribute5%TYPE,
      global_attribute6              xxap_invoice_lines_ext_r12.global_attribute6%TYPE,
      global_attribute7              xxap_invoice_lines_ext_r12.global_attribute7%TYPE,
      global_attribute8              xxap_invoice_lines_ext_r12.global_attribute8%TYPE,
      global_attribute9              xxap_invoice_lines_ext_r12.global_attribute9%TYPE,
      global_attribute10             xxap_invoice_lines_ext_r12.global_attribute10%TYPE,
      global_attribute11             xxap_invoice_lines_ext_r12.global_attribute11%TYPE,
      global_attribute12             xxap_invoice_lines_ext_r12.global_attribute12%TYPE,
      global_attribute13             xxap_invoice_lines_ext_r12.global_attribute13%TYPE,
      global_attribute14             xxap_invoice_lines_ext_r12.global_attribute14%TYPE,
      global_attribute15             xxap_invoice_lines_ext_r12.global_attribute15%TYPE,
      global_attribute16             xxap_invoice_lines_ext_r12.global_attribute16%TYPE,
      global_attribute17             xxap_invoice_lines_ext_r12.global_attribute17%TYPE,
      global_attribute18             xxap_invoice_lines_ext_r12.global_attribute18%TYPE,
      global_attribute19             xxap_invoice_lines_ext_r12.global_attribute19%TYPE,
      global_attribute20             xxap_invoice_lines_ext_r12.global_attribute20%TYPE,
      expenditure_type               xxap_invoice_lines_ext_r12.expenditure_type%TYPE,
      expenditure_item_date          xxap_invoice_lines_ext_r12.expenditure_item_date%TYPE,
      project_id                     xxap_invoice_lines_ext_r12.project_id%TYPE,
      task_id                        xxap_invoice_lines_ext_r12.task_id%TYPE,
      organization_name              xxap_invoice_lines_ext_r12.organization_name%TYPE,
      org_id                         xxap_invoice_lines_ext_r12.org_id%TYPE,
      expenditure_organization_id    xxap_invoice_lines_ext_r12.expenditure_organization_id%TYPE,
      tax_code_id                    xxap_invoice_lines_ext_r12.tax_code_id%TYPE,
      tax_regime_code                xxap_invoice_lines_ext_r12.tax_regime_code%TYPE,
      tax                            xxap_invoice_lines_ext_r12.tax%TYPE,
      tax_jurisdiction_code          xxap_invoice_lines_ext_r12.tax_jurisdiction_code%TYPE,
      tax_status_code                xxap_invoice_lines_ext_r12.tax_status_code%TYPE,
      tax_rate_id                    xxap_invoice_lines_ext_r12.tax_rate_id%TYPE,
      tax_rate_code                  xxap_invoice_lines_ext_r12.tax_rate_code%TYPE,
      tax_rate                       xxap_invoice_lines_ext_r12.tax_rate%TYPE,
      incl_in_taxable_line_flag      xxap_invoice_lines_ext_r12.incl_in_taxable_line_flag%TYPE,
      tax_classification_code        xxap_invoice_lines_ext_r12.tax_classification_code%TYPE,
      project_name                   xxap_invoice_lines_ext_r12.project_name%TYPE,
      task_name                      xxap_invoice_lines_ext_r12.task_name%TYPE,
      leg_unit_price                 xxap_invoice_lines_ext_r12.leg_unit_price%TYPE,
      leg_dist_code_concatenated     xxap_invoice_lines_ext_r12.leg_dist_code_concatenated%TYPE,
      leg_awt_group_name             xxap_invoice_lines_ext_r12.leg_awt_group_name%TYPE,
      leg_attribute_category         xxap_invoice_lines_ext_r12.leg_attribute_category%TYPE,
      leg_attribute1                 xxap_invoice_lines_ext_r12.leg_attribute1%TYPE,
      leg_attribute2                 xxap_invoice_lines_ext_r12.leg_attribute2%TYPE,
      leg_attribute3                 xxap_invoice_lines_ext_r12.leg_attribute3%TYPE,
      leg_attribute4                 xxap_invoice_lines_ext_r12.leg_attribute4%TYPE,
      leg_attribute5                 xxap_invoice_lines_ext_r12.leg_attribute5%TYPE,
      leg_attribute6                 xxap_invoice_lines_ext_r12.leg_attribute6%TYPE,
      leg_attribute7                 xxap_invoice_lines_ext_r12.leg_attribute7%TYPE,
      leg_attribute8                 xxap_invoice_lines_ext_r12.leg_attribute8%TYPE,
      leg_attribute9                 xxap_invoice_lines_ext_r12.leg_attribute9%TYPE,
      leg_attribute10                xxap_invoice_lines_ext_r12.leg_attribute10%TYPE,
      leg_attribute11                xxap_invoice_lines_ext_r12.leg_attribute11%TYPE,
      leg_attribute12                xxap_invoice_lines_ext_r12.leg_attribute12%TYPE,
      leg_attribute13                xxap_invoice_lines_ext_r12.leg_attribute13%TYPE,
      leg_attribute14                xxap_invoice_lines_ext_r12.leg_attribute14%TYPE,
      leg_attribute15                xxap_invoice_lines_ext_r12.leg_attribute15%TYPE,
      leg_global_attribute_category  xxap_invoice_lines_ext_r12.leg_global_attribute_category%TYPE,
      leg_global_attribute1          xxap_invoice_lines_ext_r12.leg_global_attribute1%TYPE,
      leg_global_attribute2          xxap_invoice_lines_ext_r12.leg_global_attribute2%TYPE,
      leg_global_attribute3          xxap_invoice_lines_ext_r12.leg_global_attribute3%TYPE,
      leg_global_attribute4          xxap_invoice_lines_ext_r12.leg_global_attribute4%TYPE,
      leg_global_attribute5          xxap_invoice_lines_ext_r12.leg_global_attribute5%TYPE,
      leg_global_attribute6          xxap_invoice_lines_ext_r12.leg_global_attribute6%TYPE,
      leg_global_attribute7          xxap_invoice_lines_ext_r12.leg_global_attribute7%TYPE,
      leg_global_attribute8          xxap_invoice_lines_ext_r12.leg_global_attribute8%TYPE,
      leg_global_attribute9          xxap_invoice_lines_ext_r12.leg_global_attribute9%TYPE,
      leg_global_attribute10         xxap_invoice_lines_ext_r12.leg_global_attribute10%TYPE,
      leg_global_attribute11         xxap_invoice_lines_ext_r12.leg_global_attribute11%TYPE,
      leg_global_attribute12         xxap_invoice_lines_ext_r12.leg_global_attribute12%TYPE,
      leg_global_attribute13         xxap_invoice_lines_ext_r12.leg_global_attribute13%TYPE,
      leg_global_attribute14         xxap_invoice_lines_ext_r12.leg_global_attribute14%TYPE,
      leg_global_attribute15         xxap_invoice_lines_ext_r12.leg_global_attribute15%TYPE,
      leg_global_attribute16         xxap_invoice_lines_ext_r12.leg_global_attribute16%TYPE,
      leg_global_attribute17         xxap_invoice_lines_ext_r12.leg_global_attribute17%TYPE,
      leg_global_attribute18         xxap_invoice_lines_ext_r12.leg_global_attribute18%TYPE,
      leg_global_attribute19         xxap_invoice_lines_ext_r12.leg_global_attribute19%TYPE,
      leg_global_attribute20         xxap_invoice_lines_ext_r12.leg_global_attribute20%TYPE,
      leg_project_accounting_context xxap_invoice_lines_ext_r12.leg_project_accounting_context%TYPE,
      leg_pa_addition_flag           xxap_invoice_lines_ext_r12.leg_pa_addition_flag%TYPE,
      leg_pa_quantity                xxap_invoice_lines_ext_r12.leg_pa_quantity%TYPE,
      leg_ussgl_transaction_code     xxap_invoice_lines_ext_r12.leg_ussgl_transaction_code%TYPE,
      leg_stat_amount                xxap_invoice_lines_ext_r12.leg_stat_amount%TYPE,
      leg_type_1099                  xxap_invoice_lines_ext_r12.leg_type_1099%TYPE,
      leg_income_tax_region          xxap_invoice_lines_ext_r12.leg_income_tax_region%TYPE,
      leg_assets_tracking_flag       xxap_invoice_lines_ext_r12.leg_assets_tracking_flag%TYPE,
      leg_price_correction_flag      xxap_invoice_lines_ext_r12.leg_price_correction_flag%TYPE,
      leg_project_name               xxap_invoice_lines_ext_r12.leg_project_name%TYPE,
      leg_task_name                  xxap_invoice_lines_ext_r12.leg_task_name%TYPE,
      leg_packing_slip               xxap_invoice_lines_ext_r12.leg_packing_slip%TYPE,
      leg_pa_cc_ar_invoice_line_num  xxap_invoice_lines_ext_r12.leg_pa_cc_ar_invoice_line_num%TYPE,
      leg_reference_1                xxap_invoice_lines_ext_r12.leg_reference_1%TYPE,
      leg_reference_2                xxap_invoice_lines_ext_r12.leg_reference_2%TYPE,
      leg_pa_cc_processed_code       xxap_invoice_lines_ext_r12.leg_pa_cc_processed_code%TYPE,
      leg_tax_recovery_rate          xxap_invoice_lines_ext_r12.leg_tax_recovery_rate%TYPE,
      leg_tax_recovery_override_flag xxap_invoice_lines_ext_r12.leg_tax_recovery_override_flag%TYPE,
      leg_tax_recoverable_flag       xxap_invoice_lines_ext_r12.leg_tax_recoverable_flag%TYPE,
      leg_tax_code_override_flag     xxap_invoice_lines_ext_r12.leg_tax_code_override_flag%TYPE,
      leg_serial_number              xxap_invoice_lines_ext_r12.leg_serial_number%TYPE,
      leg_manufacturer               xxap_invoice_lines_ext_r12.leg_manufacturer%TYPE,
      leg_model_number               xxap_invoice_lines_ext_r12.leg_model_number%TYPE,
      leg_warranty_number            xxap_invoice_lines_ext_r12.leg_warranty_number%TYPE,
      leg_deferred_acctg_flag        xxap_invoice_lines_ext_r12.leg_deferred_acctg_flag%TYPE,
      leg_def_acctg_start_date       xxap_invoice_lines_ext_r12.leg_def_acctg_start_date%TYPE,
      leg_def_acctg_end_date         xxap_invoice_lines_ext_r12.leg_def_acctg_end_date%TYPE,
      leg_def_acctg_num_of_periods   xxap_invoice_lines_ext_r12.leg_def_acctg_num_of_periods%TYPE,
      leg_def_acctg_period_type      xxap_invoice_lines_ext_r12.leg_def_acctg_period_type%TYPE,
      leg_unit_of_meas_lookup_code   xxap_invoice_lines_ext_r12.leg_unit_of_meas_lookup_code%TYPE,
      leg_asset_book_type_code       xxap_invoice_lines_ext_r12.leg_asset_book_type_code%TYPE,
      leg_reference_key1             xxap_invoice_lines_ext_r12.leg_reference_key1%TYPE,
      leg_reference_key2             xxap_invoice_lines_ext_r12.leg_reference_key2%TYPE,
      leg_reference_key3             xxap_invoice_lines_ext_r12.leg_reference_key3%TYPE,
      leg_reference_key4             xxap_invoice_lines_ext_r12.leg_reference_key4%TYPE,
      leg_reference_key5             xxap_invoice_lines_ext_r12.leg_reference_key5%TYPE,
      leg_cost_factor_name           xxap_invoice_lines_ext_r12.leg_cost_factor_name%TYPE,
      leg_source_entity_code         xxap_invoice_lines_ext_r12.leg_source_entity_code%TYPE,
      leg_source_event_class_code    xxap_invoice_lines_ext_r12.leg_source_event_class_code%TYPE,
      leg_cc_reversal_flag           xxap_invoice_lines_ext_r12.leg_cc_reversal_flag%TYPE,
      leg_expense_group              xxap_invoice_lines_ext_r12.leg_expense_group%TYPE,
      leg_justification              xxap_invoice_lines_ext_r12.leg_justification%TYPE,
      leg_merchant_document_number   xxap_invoice_lines_ext_r12.leg_merchant_document_number%TYPE,
      leg_merchant_reference         xxap_invoice_lines_ext_r12.leg_merchant_reference%TYPE,
      leg_merchant_tax_reg_number    xxap_invoice_lines_ext_r12.leg_merchant_tax_reg_number%TYPE,
      leg_receipt_currency_code      xxap_invoice_lines_ext_r12.leg_receipt_currency_code%TYPE,
      leg_receipt_conversion_rate    xxap_invoice_lines_ext_r12.leg_receipt_conversion_rate%TYPE,
      leg_receipt_currency_amount    xxap_invoice_lines_ext_r12.leg_receipt_currency_amount%TYPE,
      leg_country_of_supply          xxap_invoice_lines_ext_r12.leg_country_of_supply%TYPE,
      leg_pay_awt_group_name         xxap_invoice_lines_ext_r12.leg_pay_awt_group_name%TYPE,
      leg_expense_start_date         xxap_invoice_lines_ext_r12.leg_expense_start_date%TYPE,
      leg_expense_end_date           xxap_invoice_lines_ext_r12.leg_expense_end_date%TYPE,
      leg_po_number                  xxap_invoice_lines_ext_r12.leg_po_number%TYPE,
      leg_po_line_number             xxap_invoice_lines_ext_r12.leg_po_line_number%TYPE,
      leg_po_shipment_num            xxap_invoice_lines_ext_r12.leg_po_shipment_num%TYPE,
      leg_po_distribution_num        xxap_invoice_lines_ext_r12.leg_po_distribution_num%TYPE,
      leg_release_num                xxap_invoice_lines_ext_r12.leg_release_num%TYPE,
      leg_expenditure_type           xxap_invoice_lines_ext_r12.leg_expenditure_type%TYPE,
      leg_expenditure_item_date      xxap_invoice_lines_ext_r12.leg_expenditure_item_date%TYPE,
      leg_expenditure_org_name       xxap_invoice_lines_ext_r12.leg_expenditure_org_name%TYPE,
      leg_receipt_number             xxap_invoice_lines_ext_r12.leg_receipt_number%TYPE,
      leg_receipt_line_number        xxap_invoice_lines_ext_r12.leg_receipt_line_number%TYPE,
      leg_requester_first_name       xxap_invoice_lines_ext_r12.leg_requester_first_name%TYPE,
      leg_requester_last_name        xxap_invoice_lines_ext_r12.leg_requester_last_name%TYPE,
      leg_requester_employee_num     xxap_invoice_lines_ext_r12.leg_requester_employee_num%TYPE,
      leg_source_trx_level_type      xxap_invoice_lines_ext_r12.leg_source_trx_level_type%TYPE,
      leg_merchant_name#1            xxap_invoice_lines_ext_r12.leg_merchant_name#1%TYPE,
      leg_source_system              xxap_invoice_lines_ext_r12.leg_source_system%TYPE,
      leg_request_id                 xxap_invoice_lines_ext_r12.leg_request_id%TYPE,
      leg_seq_num                    xxap_invoice_lines_ext_r12.leg_seq_num%TYPE,
      leg_process_flag               xxap_invoice_lines_ext_r12.leg_process_flag%TYPE,
      process_flag                   xxap_invoice_lines_ext_r12.process_flag%TYPE,
      po_header_id                   xxap_invoice_lines_ext_r12.po_header_id%TYPE,
      po_line_id                     xxap_invoice_lines_ext_r12.po_line_id%TYPE,
      po_line_location_id            xxap_invoice_lines_ext_r12.po_line_location_id%TYPE,
      po_distribution_id             xxap_invoice_lines_ext_r12.po_distribution_id%TYPE,
      receipt_id                     xxap_invoice_lines_ext_r12.receipt_id%TYPE,
      GROUP_ID                       xxap_invoice_lines_ext_r12.GROUP_ID%TYPE,
      last_updated_by                xxap_invoice_lines_ext_r12.last_updated_by%TYPE,
      last_updated_date              xxap_invoice_lines_ext_r12.last_updated_date%TYPE,
      last_update_login              xxap_invoice_lines_ext_r12.last_update_login%TYPE,
      program_application_id         xxap_invoice_lines_ext_r12.program_application_id%TYPE,
      program_id                     xxap_invoice_lines_ext_r12.program_id%TYPE,
      program_update_date            xxap_invoice_lines_ext_r12.program_update_date%TYPE,
      request_id                     xxap_invoice_lines_ext_r12.request_id%TYPE,
      error_type                     xxap_invoice_lines_ext_r12.error_type%TYPE,
      plant_segment                  VARCHAR2(240),
      leg_invoice_id                 xxap_invoice_lines_ext_r12.leg_invoice_id%TYPE,
      leg_org_id                     xxap_invoice_lines_ext_r12.leg_org_id%TYPE,
      leg_distribution_line_id       xxap_invoice_lines_ext_r12.leg_distribution_line_id%TYPE,
      leg_tax_id                     xxap_invoice_lines_ext_r12.leg_tax_id%TYPE,
      leg_dist_code_combination_id   xxap_invoice_lines_ext_r12.leg_dist_code_combination_id%TYPE,
      leg_awt_flag                   xxap_invoice_lines_ext_r12.leg_awt_flag%TYPE,
      leg_awt_group_id               xxap_invoice_lines_ext_r12.leg_awt_group_id%TYPE,
      leg_project_id                 xxap_invoice_lines_ext_r12.leg_project_id%TYPE,
      leg_task_id                    xxap_invoice_lines_ext_r12.leg_task_id%TYPE,
      leg_po_header_id               xxap_invoice_lines_ext_r12.leg_po_header_id%TYPE,
      leg_po_line_id                 xxap_invoice_lines_ext_r12.leg_po_line_id%TYPE,
      leg_po_line_location_id        xxap_invoice_lines_ext_r12.leg_po_line_location_id%TYPE,
      leg_po_distibution_id          xxap_invoice_lines_ext_r12.leg_po_distibution_id%TYPE,
      leg_expenditure_inv_org_id     xxap_invoice_lines_ext_r12.leg_expenditure_inv_org_id%TYPE,
      leg_receipt_header_id          xxap_invoice_lines_ext_r12.leg_receipt_header_id%TYPE,
      leg_receipt_line_id            xxap_invoice_lines_ext_r12.leg_receipt_line_id%TYPE,
      leg_rcv_transaction_id         xxap_invoice_lines_ext_r12.leg_rcv_transaction_id%TYPE,
      leg_requester_id               xxap_invoice_lines_ext_r12.leg_requester_id%TYPE,
      leg_set_of_books_name          xxap_invoice_lines_ext_r12.leg_set_of_books_name%TYPE,
      leg_set_of_books_id            xxap_invoice_lines_ext_r12.leg_set_of_books_id%TYPE,
      taxable_flag                   VARCHAR2(1));

    --Ver 1.2 changes start for hold staging
    TYPE invoice_hold_ext_rec IS RECORD(
      interface_txn_id     xxap_invoice_holds_ext_r12.interface_txn_id%TYPE,
      batch_id             xxap_invoice_holds_ext_r12.batch_id%TYPE,
      load_id              xxap_invoice_holds_ext_r12.load_id%TYPE,
      run_sequence_id      xxap_invoice_holds_ext_r12.run_sequence_id%TYPE,
      leg_operating_unit   xxap_invoice_holds_ext_r12.leg_operating_unit%TYPE,
      leg_supplier_number  xxap_invoice_holds_ext_r12.leg_supplier_number%TYPE,
      leg_supplier_site    xxap_invoice_holds_ext_r12.leg_supplier_site%TYPE,
      leg_invoice_number   xxap_invoice_holds_ext_r12.leg_invoice_number%TYPE,
      invoice_id           xxap_invoice_holds_ext_r12.invoice_id%TYPE,
      line_location_id     xxap_invoice_holds_ext_r12.line_location_id%TYPE,
      leg_hold_lookup_code xxap_invoice_holds_ext_r12.leg_hold_lookup_code%TYPE,
      leg_hold_type        xxap_invoice_holds_ext_r12.leg_hold_type%TYPE,
      hold_lookup_code     xxap_invoice_holds_ext_r12.hold_lookup_code%TYPE,
      hold_type            xxap_invoice_holds_ext_r12.hold_type%TYPE,
      leg_hold_date        xxap_invoice_holds_ext_r12.leg_hold_date%TYPE,
      leg_hold_reason      xxap_invoice_holds_ext_r12.leg_hold_reason%TYPE,
      org_id               xxap_invoice_holds_ext_r12.org_id%TYPE,
      operating_unit       xxap_invoice_holds_ext_r12.operating_unit%TYPE,
      request_id           xxap_invoice_holds_ext_r12.request_id%TYPE,
      process_flag         xxap_invoice_holds_ext_r12.process_flag%TYPE,
      leg_source_system    xxap_invoice_holds_ext_r12.leg_source_system%TYPE,
      leg_request_id       xxap_invoice_holds_ext_r12.leg_request_id%TYPE,
      leg_seq_num          xxap_invoice_holds_ext_r12.leg_seq_num%TYPE,
      leg_process_flag     xxap_invoice_holds_ext_r12.leg_process_flag%TYPE,
      leg_invoice_id       xxap_invoice_holds_ext_r12.leg_invice_id%TYPE);

    --Ver 1.2 changes end for hold staging
    TYPE invoice_hdr_ext_tbl IS TABLE OF invoice_hdr_ext_rec INDEX BY BINARY_INTEGER;

    TYPE invoice_line_ext_tbl IS TABLE OF invoice_line_ext_rec INDEX BY BINARY_INTEGER;

    --Ver 1.2 changes start for hold staging
    TYPE invoice_hold_ext_tbl IS TABLE OF invoice_hold_ext_rec INDEX BY BINARY_INTEGER;

    --Ver 1.2 changes end for hold staging
    l_invoice_hdr_ext_tbl  invoice_hdr_ext_tbl;
    l_invoice_line_ext_tbl invoice_line_ext_tbl;
    l_invoice_hold_ext_tbl invoice_hold_ext_tbl;
    l_err_record           NUMBER;
    lv_ret_code            VARCHAR2(10);

    -- cursor to fetch the data from extraction header table
    CURSOR cur_data_header_stg IS
      SELECT xier.interface_txn_id,
             xier.batch_id,
             xier.load_id,
             xier.run_sequence_id,
             xier.leg_accts_pay_code_concat,
             xier.leg_apply_advances_flag,
             xier.leg_attribute_category,
             xier.leg_attribute1,
             xier.leg_attribute10,
             xier.leg_attribute11,
             xier.leg_attribute12,
             xier.leg_attribute13,
             xier.leg_attribute14,
             xier.leg_attribute15,
             xier.leg_attribute2,
             xier.leg_attribute3,
             xier.leg_attribute4,
             xier.leg_attribute5,
             xier.leg_attribute6,
             xier.leg_attribute7,
             xier.leg_attribute8,
             xier.leg_attribute9,
             xier.leg_cust_registration_code,
             xier.leg_cust_registration_number,
             xier.leg_delivery_channel_code,
             xier.leg_description,
             xier.leg_document_sub_type,
             xier.leg_exchange_date,
             xier.leg_exchange_rate,
             DECODE(xier.leg_exchange_rate, NULL, NULL, 'User'), --v1.12
             xier.leg_exclusive_payment_flag,
             xier.leg_gl_date,
             xier.leg_global_attribute_category,
             xier.leg_global_attribute1,
             xier.leg_global_attribute10,
             xier.leg_global_attribute11,
             xier.leg_global_attribute12,
             xier.leg_global_attribute13,
             xier.leg_global_attribute14,
             xier.leg_global_attribute15,
             xier.leg_global_attribute16,
             xier.leg_global_attribute17,
             xier.leg_global_attribute18,
             xier.leg_global_attribute19,
             xier.leg_global_attribute2,
             xier.leg_global_attribute20,
             xier.leg_global_attribute3,
             xier.leg_global_attribute4,
             xier.leg_global_attribute5,
             xier.leg_global_attribute6,
             xier.leg_global_attribute7,
             xier.leg_global_attribute8,
             xier.leg_global_attribute9,
             xier.leg_inv_currency_code,
             xier.leg_inv_includes_prepay_flag,
             xier.leg_inv_type_lookup_code,
             xier.leg_invoice_amount,
             xier.leg_invoice_date,
             xier.leg_invoice_num,
             xier.leg_invoice_received_date,
             xier.leg_legal_entity_name,
             xier.leg_net_of_retainage_flag,
             xier.leg_operating_unit,
             xier.leg_pay_awt_group_name,
             xier.leg_pay_group_lookup_cod,
             xier.leg_pay_proc_trxn_type_code,
             xier.leg_payment_cross_rate,
             xier.leg_payment_cross_rate_date,
             xier.leg_payment_cross_rate_type,
             xier.leg_payment_currency_code,
             xier.leg_payment_method_code,
             xier.leg_payment_method_lookup_code,
             xier.leg_payment_reason_code,
             xier.leg_po_number,
             xier.leg_port_of_entry_code,
             xier.leg_prepay_apply_amount,
             xier.leg_prepay_line_num,
             xier.leg_product_table,
             xier.leg_reference_1,
             xier.leg_reference_2,
             xier.leg_reference_key1,
             xier.leg_reference_key2,
             xier.leg_reference_key3,
             xier.leg_reference_key4,
             xier.leg_reference_key5,
             xier.leg_remit_to_supplier_num,
             xier.leg_remit_to_supplier_site,
             xier.leg_remittance_message1,
             xier.leg_remittance_message2,
             xier.leg_remittance_message3,
             xier.leg_supplier_tax_exchange_rate,
             xier.leg_supplier_tax_invoice_date,
             xier.leg_supplier_tax_invoice_num,
             xier.leg_tax_invoice_internal_seq,
             xier.leg_tax_invoice_recording_date,
             xier.leg_taxation_country,
             xier.leg_terms_date,
             xier.leg_terms_name,
             xier.leg_unique_remit_identifier,
             xier.leg_uri_check_digit,
             xier.leg_ussgl_transaction_code,
             xier.leg_vat_code,
             xier.leg_vendor_num,
             xier.leg_vendor_site_code,
             --xier.leg_voucher_num,
             --v2.1 commented the above
             xier.leg_doc_sequence_value,
             --v2.1 ends
             xier.leg_payment_function,
             xier.leg_payment_priority,
             xier.leg_payment_reason_comments,
             xier.leg_request_id,
             xier.leg_seq_num,
             xier.leg_source_system,
             xier.leg_process_flag,
             xier.leg_awt_group_name,
             xier.leg_prepay_num,
             xier.leg_prepay_dist_num,
             xier.leg_prepay_gl_date,
             xier.leg_invoice_incl_prepay_flag,
             xier.prepay_line_num,
             xier.product_table,
             xier.leg_bank_charge_bearer,
             xier.leg_unique_remittance_id,
             xier.leg_settlement_priority,
             xier.leg_requester_employee_num,
             xier.leg_remit_to_supplier_name,
             xier.leg_due_date,
             xier.accts_pay_code_combination_id,
             xier.accts_pay_code_concatenated,
             xier.amount_applicable_to_discount,
             xier.attribute_category,
             xier.attribute1,
             xier.attribute10,
             xier.attribute11,
             xier.attribute12,
             xier.attribute13,
             xier.attribute14,
             xier.attribute15,
             xier.attribute2,
             xier.attribute3,
             xier.attribute4,
             xier.attribute5,
             xier.attribute6,
             xier.attribute7,
             xier.attribute8,
             xier.attribute9,
             xier.global_attribute_category,
             xier.global_attribute1,
             xier.global_attribute10,
             xier.global_attribute11,
             xier.global_attribute12,
             xier.global_attribute13,
             xier.global_attribute14,
             xier.global_attribute15,
             xier.global_attribute16,
             xier.global_attribute17,
             xier.global_attribute18,
             xier.global_attribute19,
             xier.global_attribute2,
             xier.global_attribute20,
             xier.global_attribute3,
             xier.global_attribute4,
             xier.global_attribute5,
             xier.global_attribute6,
             xier.global_attribute7,
             xier.global_attribute8,
             xier.global_attribute9,
             xier.calc_tax_during_import_flag,
             xier.gl_date,
             xier.goods_received_date,
             xier.invoice_date,
             xier.invoice_num,
             xier.legal_entity_id,
             xier.legal_entity_name,
             xier.operating_unit,
             xier.org_id,
             xier.pay_awt_group_name,
             --v2.0
             DECODE (xier.leg_inv_type_lookup_code,'PREPAYMENT',xier.leg_pay_group_lookup_cod, xier.pay_group_lookup_code),
             --v2.0
             xier.pay_proc_trxn_type_code,
             xier.payment_method_code,
             xier.payment_method_lookup_code,
             xier.payment_reason_code,
             xier.remit_to_supplier_id,
             xier.remit_to_supplier_name,
             xier.remit_to_supplier_site_id,
             xier.SOURCE,
             xier.terms_date,
             xier.terms_name,
             xier.workflow_flag,
             xier.GROUP_ID,
             xier.last_updated_by,
             xier.last_updated_date,
             xier.last_update_login,
             xier.program_application_id,
             xier.program_id,
             xier.program_update_date,
             xier.request_id,
             xier.process_flag,
             xier.error_type,
             NULL plant_segment,
             xier.leg_accts_pay_code_comb_id,
             xier.leg_invoice_id,
             xier.leg_org_id,
             xier.leg_po_header_id,
             xier.leg_term_id,
             xier.leg_vat_id,
             xier.leg_vendor_id,
             xier.leg_vendor_site_id,
             xier.leg_awt_group_id,
             xier.leg_requester_id,
             xier.leg_doc_category_code,
             xier.leg_doc_sequence_id,
             xier.leg_doc_sequence_value,
             xier.leg_set_of_books_name,
             xier.leg_set_of_books_id,
             xier.leg_wfapproval_status,
             xier.leg_future_date_inv
        FROM xxap_invoices_ext_r12 xier
       WHERE xier.leg_process_flag = 'V';

    -- cursor to fetch the data from extraction lines table
    CURSOR cur_data_lines_stg IS
      SELECT xiler.interface_txn_id,
             xiler.batch_id,
             xiler.load_id,
             xiler.run_sequence_id,
             xiler.leg_invoice_num,
             xiler.leg_vendor_num,
             xiler.leg_operating_unit,
             xiler.leg_line_number,
             xiler.line_number,
             xiler.leg_line_type_lookup_code,
             xiler.line_type_lookup_code,
             xiler.leg_amount,
             xiler.amount,
             xiler.leg_accounting_date,
             xiler.accounting_date,
             xiler.leg_description,
             xiler.amount_includes_tax_flag,
             xiler.leg_tax_code,
             xiler.tax_code,
             xiler.leg_quantity_invoiced,
             xiler.leg_ship_to_location_code,
             xiler.dist_code_concatenated,
             xiler.dist_code_concatenated_id,
             xiler.awt_group_name,
             xiler.attribute_category,
             xiler.attribute1,
             xiler.attribute2,
             xiler.attribute3,
             xiler.attribute4,
             xiler.attribute5,
             xiler.attribute6,
             xiler.attribute7,
             xiler.attribute8,
             xiler.attribute9,
             xiler.attribute10,
             xiler.attribute11,
             xiler.attribute12,
             xiler.attribute13,
             xiler.attribute14,
             xiler.attribute15,
             xiler.global_attribute_category,
             xiler.global_attribute1,
             xiler.global_attribute2,
             xiler.global_attribute3,
             xiler.global_attribute4,
             xiler.global_attribute5,
             xiler.global_attribute6,
             xiler.global_attribute7,
             xiler.global_attribute8,
             xiler.global_attribute9,
             xiler.global_attribute10,
             xiler.global_attribute11,
             xiler.global_attribute12,
             xiler.global_attribute13,
             xiler.global_attribute14,
             xiler.global_attribute15,
             xiler.global_attribute16,
             xiler.global_attribute17,
             xiler.global_attribute18,
             xiler.global_attribute19,
             xiler.global_attribute20,
             xiler.expenditure_type,
             xiler.expenditure_item_date,
             xiler.project_id,
             xiler.task_id,
             xiler.organization_name,
             xiler.org_id,
             xiler.expenditure_organization_id,
             xiler.tax_code_id,
             xiler.tax_regime_code,
             xiler.tax,
             xiler.tax_jurisdiction_code,
             xiler.tax_status_code,
             xiler.tax_rate_id,
             xiler.tax_rate_code,
             xiler.tax_rate,
             xiler.incl_in_taxable_line_flag,
             xiler.tax_classification_code,
             xiler.project_name,
             xiler.task_name,
             xiler.leg_unit_price,
             xiler.leg_dist_code_concatenated,
             xiler.leg_awt_group_name,
             xiler.leg_attribute_category,
             xiler.leg_attribute1,
             xiler.leg_attribute2,
             xiler.leg_attribute3,
             xiler.leg_attribute4,
             xiler.leg_attribute5,
             xiler.leg_attribute6,
             xiler.leg_attribute7,
             xiler.leg_attribute8,
             xiler.leg_attribute9,
             xiler.leg_attribute10,
             xiler.leg_attribute11,
             xiler.leg_attribute12,
             xiler.leg_attribute13,
             xiler.leg_attribute14,
             xiler.leg_attribute15,
             xiler.leg_global_attribute_category,
             xiler.leg_global_attribute1,
             xiler.leg_global_attribute2,
             xiler.leg_global_attribute3,
             xiler.leg_global_attribute4,
             xiler.leg_global_attribute5,
             xiler.leg_global_attribute6,
             xiler.leg_global_attribute7,
             xiler.leg_global_attribute8,
             xiler.leg_global_attribute9,
             xiler.leg_global_attribute10,
             xiler.leg_global_attribute11,
             xiler.leg_global_attribute12,
             xiler.leg_global_attribute13,
             xiler.leg_global_attribute14,
             xiler.leg_global_attribute15,
             xiler.leg_global_attribute16,
             xiler.leg_global_attribute17,
             xiler.leg_global_attribute18,
             xiler.leg_global_attribute19,
             xiler.leg_global_attribute20,
             xiler.leg_project_accounting_context,
             DECODE(xiler.leg_pa_addition_flag, 'Y','E',xiler.leg_pa_addition_flag) leg_pa_addition_flag, --CR256293
             xiler.leg_pa_quantity,
             xiler.leg_ussgl_transaction_code,
             xiler.leg_stat_amount,
             xiler.leg_type_1099,
             xiler.leg_income_tax_region,
             xiler.leg_assets_tracking_flag,
             xiler.leg_price_correction_flag,
             DECODE(xiler.leg_pa_addition_flag,'Y', NULL ,xiler.leg_project_name) leg_project_name,  --CR256293
             DECODE(xiler.leg_pa_addition_flag,'Y', NULL ,xiler.leg_task_name) leg_task_name,  --CR256293
             xiler.leg_packing_slip,
             xiler.leg_pa_cc_ar_invoice_line_num,
             xiler.leg_reference_1,
             xiler.leg_reference_2,
             xiler.leg_pa_cc_processed_code,
             xiler.leg_tax_recovery_rate,
             xiler.leg_tax_recovery_override_flag,
             xiler.leg_tax_recoverable_flag,
             xiler.leg_tax_code_override_flag,
             xiler.leg_serial_number,
             xiler.leg_manufacturer,
             xiler.leg_model_number,
             xiler.leg_warranty_number,
             xiler.leg_deferred_acctg_flag,
             xiler.leg_def_acctg_start_date,
             xiler.leg_def_acctg_end_date,
             xiler.leg_def_acctg_num_of_periods,
             xiler.leg_def_acctg_period_type,
             xiler.leg_unit_of_meas_lookup_code,
             xiler.leg_asset_book_type_code,
             xiler.leg_reference_key1,
             xiler.leg_reference_key2,
             xiler.leg_reference_key3,
             xiler.leg_reference_key4,
             xiler.leg_reference_key5,
             xiler.leg_cost_factor_name,
             xiler.leg_source_entity_code,
             xiler.leg_source_event_class_code,
             xiler.leg_cc_reversal_flag,
             xiler.leg_expense_group,
             xiler.leg_justification,
             xiler.leg_merchant_document_number,
             xiler.leg_merchant_reference,
             xiler.leg_merchant_tax_reg_number,
             xiler.leg_receipt_currency_code,
             xiler.leg_receipt_conversion_rate,
             xiler.leg_receipt_currency_amount,
             xiler.leg_country_of_supply,
             xiler.leg_pay_awt_group_name,
             xiler.leg_expense_start_date,
             xiler.leg_expense_end_date,
             xiler.leg_po_number,
             xiler.leg_po_line_number,
             xiler.leg_po_shipment_num,
             xiler.leg_po_distribution_num,
             xiler.leg_release_num,
             DECODE(xiler.leg_pa_addition_flag,'Y', NULL ,
                                                  DECODE(xiler.leg_expenditure_type, NULL,
                                                                                     NULL, 'SUPPLIER INVOICED MATERIAL') ) leg_expenditure_type, --CR256293
             DECODE(xiler.leg_pa_addition_flag,'Y', NULL ,xiler.leg_expenditure_item_date) leg_expenditure_item_date, --CR 256293
             DECODE(xiler.leg_pa_addition_flag,'Y', NULL ,xiler.leg_expenditure_org_name) leg_expenditure_org_name, --CR256293

             xiler.leg_receipt_number,
             xiler.leg_receipt_line_number,
             xiler.leg_requester_first_name,
             xiler.leg_requester_last_name,
             xiler.leg_requester_employee_num,
             xiler.leg_source_trx_level_type,
             xiler.leg_merchant_name#1,
             xiler.leg_source_system,
             xiler.leg_request_id,
             xiler.leg_seq_num,
             xiler.leg_process_flag,
             xiler.process_flag,
             xiler.po_header_id,
             xiler.po_line_id,
             xiler.po_line_location_id,
             xiler.po_distribution_id,
             xiler.receipt_id,
             xiler.GROUP_ID,
             xiler.last_updated_by,
             xiler.last_updated_date,
             xiler.last_update_login,
             xiler.program_application_id,
             xiler.program_id,
             xiler.program_update_date,
             xiler.request_id,
             xiler.error_type,
             NULL plant_segment,
             xiler.leg_invoice_id,
             xiler.leg_org_id,
             xiler.leg_distribution_line_id,
             xiler.leg_tax_id,
             xiler.leg_dist_code_combination_id,
             xiler.leg_awt_flag,
             xiler.leg_awt_group_id,
             xiler.leg_project_id,
             xiler.leg_task_id,
             xiler.leg_po_header_id,
             xiler.leg_po_line_id,
             xiler.leg_po_line_location_id,
             xiler.leg_po_distibution_id,
             xiler.leg_expenditure_inv_org_id,
             xiler.leg_receipt_header_id,
             xiler.leg_receipt_line_id,
             xiler.leg_rcv_transaction_id,
             xiler.leg_requester_id,
             xiler.leg_set_of_books_name,
             xiler.leg_set_of_books_id,
             NULL
        FROM xxap_invoice_lines_ext_r12 xiler
       WHERE xiler.leg_process_flag = 'V';

    -- cursor to fetch the data from extraction header table
    CURSOR cur_data_hold_stg IS
      SELECT xier.interface_txn_id,
             xier.batch_id,
             xier.load_id,
             xier.run_sequence_id,
             xier.leg_operating_unit,
             xier.leg_supplier_number,
             xier.leg_supplier_site,
             xier.leg_invoice_number,
             xier.invoice_id,
             xier.line_location_id,
             xier.leg_hold_lookup_code,
             xier.leg_hold_type,
             xier.hold_lookup_code,
             xier.hold_type,
             xier.leg_hold_date,
             xier.leg_hold_reason,
             xier.org_id,
             xier.operating_unit,
             xier.request_id,
             xier.process_flag,
             xier.leg_source_system,
             xier.leg_request_id,
             xier.leg_seq_num,
             xier.leg_process_flag,
             xier.leg_invice_id
        FROM xxap_invoice_holds_ext_r12 xier
       WHERE xier.leg_process_flag = 'V';
       --v1.10
       Cursor cur_inv_lin_acct IS
    SELECT xils.leg_invoice_id, xils.leg_dist_code_concatenated, xils.leg_Source_System
      FROM xxap_invoice_lines_Stg xils,
           (SELECT MIN(leg_line_number) min_lin_num, leg_invoice_id, leg_source_system
              FROM xxap_invoice_lines_Stg
             WHERE --leg_request_id = '&P_LEG_REQUEST_ID'
               --AND leg_org_id = NVL('&P_LEG_OPERATING_UNIT', leg_org_id)
               --AND leg_process_flag = 'V'
               process_flag IS NULL
             GROUP BY leg_invoice_id, leg_source_system) xils2
     WHERE xils.leg_invoice_id = xils2.leg_invoice_id
      -- AND xils.leg_request_id = '&P_LEG_REQUEST_ID'
       --AND xils.leg_org_id = NVL('&P_LEG_OPERATING_UNIT', xils.leg_org_id)
       --AND xils.leg_process_flag = 'V'
       AND  xils.leg_line_number = xils2.min_lin_num
       AND xils.leg_source_system = xils2.leg_source_system
       AND process_flag IS NULL
       /*and rownum < 100*/;

  TYPE stg_line_det_typ IS TABLE OF cur_inv_lin_acct%ROWTYPE;
  stg_line_det stg_line_det_typ;
  l_error_cnt  NUMBER;
  l_var        varchar2(200);

  --fix for AWT line
  CURSOR upd_amt_Awt
  IS SELECT sum(leg_amount) awt_total, leg_invoice_id, leg_source_System
     FROM xxap_invoice_lines_stg xil
     WHERE leg_line_type_lookup_code = 'AWT'
     AND NOT EXISTS (SELECT 1 FROM xxap_invoice_lines_stg xil2
                     WHERE xil2.leg_line_type_lookup_code != 'AWT'
                     AND xil2.leg_invoice_id = xil.leg_invoice_id
                     AND xil2.leg_source_system = xil.leg_source_system
                     AND leg_awt_group_id IS  NOT NULL)
     AND process_flag IS NULL
     GROUP BY leg_invoice_id, leg_source_System;
       --v1.10
  BEGIN
    g_total_count  := 0;
    g_failed_count := 0;
    fnd_file.put_line(fnd_file.LOG,
                      'BULK UPDATE OF STAGING TABLE1: STARTS ');

    OPEN cur_data_header_stg;

    LOOP
      l_invoice_hdr_ext_tbl.DELETE;

      FETCH cur_data_header_stg BULK COLLECT
        INTO l_invoice_hdr_ext_tbl LIMIT 1000;

      ---limit size of bulk fetch

      -- Get Total Count
      g_total_count := g_total_count + l_invoice_hdr_ext_tbl.COUNT;
      EXIT WHEN l_invoice_hdr_ext_tbl.COUNT = 0;

      BEGIN
        FORALL indx IN 1 .. l_invoice_hdr_ext_tbl.COUNT SAVE EXCEPTIONS
        ---insert into custom staging table
          INSERT INTO xxap_invoices_stg
          VALUES l_invoice_hdr_ext_tbl
            (indx);
      EXCEPTION
        WHEN OTHERS THEN
          fnd_file.put_line(fnd_file.LOG,
                            'BULK UPDATE OF STAGING TABLE: FAILED');
          fnd_file.put_line(fnd_file.LOG, 'BULK EXCEPTION FOR LOOP STARTS');
          lv_ret_code := 'E';

          FOR j IN 1 .. SQL%BULK_EXCEPTIONS.COUNT LOOP
            ---to capture each error record
            l_err_record := l_invoice_hdr_ext_tbl(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX)
                            .interface_txn_id;
            fnd_file.put_line(fnd_file.LOG,
                              'Record sequence(interface_txn_id) : ' || l_invoice_hdr_ext_tbl(SQL%BULK_EXCEPTIONS(j).ERROR_INDEX)
                              .interface_txn_id);
            fnd_file.put_line(fnd_file.LOG,
                              'Error Message : ' ||
                              SQLERRM(-SQL%BULK_EXCEPTIONS(j).ERROR_CODE));

            -- Updating Leg_process_flag to 'E' for failed records
            UPDATE xxap_invoices_ext_r12 xier
               SET xier.leg_process_flag       = 'E',
                   xier.last_updated_date      = SYSDATE,
                   xier.last_updated_by        = g_user_id,
                   xier.last_update_login      = g_login_id,
                   xier.program_id             = g_conc_program_id,
                   xier.program_application_id = g_prog_appl_id,
                   xier.program_update_date    = SYSDATE
             WHERE xier.interface_txn_id = l_err_record
               AND xier.leg_process_flag = 'V';

            g_failed_count := g_failed_count + SQL%ROWCOUNT;
          END LOOP;

          fnd_file.put_line(fnd_file.LOG, 'BULK EXCEPTION FOR LOOP ENDS');
      END;
    END LOOP;

    CLOSE cur_data_header_stg;

    COMMIT;
    g_loaded_count := g_total_count - g_failed_count;
    fnd_file.put_line(fnd_file.output, ' Stats for Header table load ');
    fnd_file.put_line(fnd_file.output, '================================');
    fnd_file.put_line(fnd_file.output, 'Total Count : ' || g_total_count);
    fnd_file.put_line(fnd_file.output, 'Loaded Count: ' || g_loaded_count);
    fnd_file.put_line(fnd_file.output, 'Failed Count: ' || g_failed_count);
    fnd_file.put_line(fnd_file.output, '================================');
    g_loaded_header_chk := g_loaded_count; -- 1.1

    -- update process flag to 'P' for sucessful records loaded in conversion table
    IF g_total_count > 0 THEN
      fnd_file.put_line(fnd_file.LOG,
                        'Updating process flag (leg_process_flag) in extraction table for processed records ');

      UPDATE xxap_invoices_ext_r12 xier
         SET xier.leg_process_flag       = 'P',
             xier.last_updated_date      = SYSDATE,
             xier.last_updated_by        = g_user_id,
             xier.last_update_login      = g_login_id,
             xier.program_id             = g_conc_program_id,
             xier.program_application_id = g_prog_appl_id,
             xier.program_update_date    = SYSDATE
       WHERE xier.leg_process_flag = 'V'
         AND EXISTS
       (SELECT 1
                FROM xxap_invoices_stg xis
               WHERE xier.interface_txn_id = xis.interface_txn_id);

      COMMIT;
      -- Either no data to load from extraction table or records already exist in R12 staging table and hence not loaded
    ELSE
      fnd_file.put_line(fnd_file.LOG,
                        'Either no data found for loading from extraction table or records already exist in R12 staging table and hence not loaded ');

      UPDATE xxap_invoices_ext_r12 xier
         SET xier.leg_process_flag       = 'E',
             xier.last_updated_date      = SYSDATE,
             xier.last_updated_by        = g_user_id,
             xier.last_update_login      = g_login_id,
             xier.program_id             = g_conc_program_id,
             xier.program_application_id = g_prog_appl_id,
             xier.program_update_date    = SYSDATE
       WHERE xier.leg_process_flag = 'V'
         AND EXISTS
       (SELECT 1
                FROM xxap_invoices_stg xis
               WHERE xier.interface_txn_id = xis.interface_txn_id);

      lv_ret_code := 'E';
      COMMIT;
    END IF;

    -- place holder for output file
    fnd_file.put_line(fnd_file.LOG,
                      'BULK UPLOAD PROGRAM ENDS FOR FIRST TABLE');
    fnd_file.put_line(fnd_file.LOG,
                      'BULK UPDATE OF STAGING TABLE2: STARTS ');
    g_total_count  := 0;
    g_failed_count := 0;
    g_loaded_count := 0;

    IF g_loaded_header_chk <> 0 THEN
      --1.1
      OPEN cur_data_lines_stg;

      LOOP
        l_invoice_line_ext_tbl.DELETE;

        FETCH cur_data_lines_stg BULK COLLECT
          INTO l_invoice_line_ext_tbl LIMIT 1000;

        ---limit size of bulk fetch

        -- Get Total Count
        g_total_count := g_total_count + l_invoice_line_ext_tbl.COUNT;
        EXIT WHEN l_invoice_line_ext_tbl.COUNT = 0;

        BEGIN
          FORALL indx IN 1 .. l_invoice_line_ext_tbl.COUNT SAVE EXCEPTIONS
          ---insert into custom staging table
            INSERT INTO xxap_invoice_lines_stg
            VALUES l_invoice_line_ext_tbl
              (indx);
        EXCEPTION
          WHEN OTHERS THEN
            FOR l_indx_exp IN 1 .. SQL%BULK_EXCEPTIONS.COUNT LOOP
              l_err_record := l_invoice_line_ext_tbl(SQL%BULK_EXCEPTIONS(l_indx_exp).ERROR_INDEX)
                              .interface_txn_id;
              -- pov_return_status := 'E';
              fnd_file.put_line(fnd_file.LOG,
                                'Record sequence : ' || l_invoice_line_ext_tbl(SQL%BULK_EXCEPTIONS(l_indx_exp).ERROR_INDEX)
                                .interface_txn_id);
              fnd_file.put_line(fnd_file.LOG,
                                'Error Message : ' ||
                                SQLERRM(-SQL%BULK_EXCEPTIONS(l_indx_exp)
                                        .ERROR_CODE));

              -- Updating Leg_process_flag to 'E' for failed records
              UPDATE xxap_invoice_lines_ext_r12 xiler
                 SET xiler.leg_process_flag       = 'E',
                     xiler.last_updated_date      = SYSDATE,
                     xiler.last_updated_by        = g_user_id,
                     xiler.last_update_login      = g_login_id,
                     xiler.program_id             = g_conc_program_id,
                     xiler.program_application_id = g_prog_appl_id,
                     xiler.program_update_date    = SYSDATE
               WHERE xiler.interface_txn_id = l_err_record
                 AND xiler.leg_process_flag = 'V';

              g_failed_count := g_failed_count + SQL%ROWCOUNT;
            END LOOP;

            fnd_file.put_line(fnd_file.LOG,
                              'BULK EXCEPTION FOR LOOP ENDS FOR SECOND TABLE');
        END;
      END LOOP;

      CLOSE cur_data_lines_stg;
    END IF;

    COMMIT;
    fnd_file.put_line(fnd_file.LOG, 'BULK UPLOAD PROGRAM ENDS');
    g_loaded_count := g_total_count - g_failed_count;
    fnd_file.put_line(fnd_file.output, ' Stats for Lines table load ');
    fnd_file.put_line(fnd_file.output, '================================');
    fnd_file.put_line(fnd_file.output, 'Total Count : ' || g_total_count);
    fnd_file.put_line(fnd_file.output, 'Loaded Count: ' || g_loaded_count);
    fnd_file.put_line(fnd_file.output, 'Failed Count: ' || g_failed_count);
    fnd_file.put_line(fnd_file.output, '================================');
    g_loaded_line_chk := g_loaded_count;

    -- updating successful record process_flag to 'P'
    -- If records successfully posted to conversion staging table
    IF g_total_count > 0 THEN
      fnd_file.put_line(fnd_file.LOG,
                        'Updating process flag (leg_process_flag) in extraction table for processed records ');

      UPDATE xxap_invoice_lines_ext_r12 xiler
         SET xiler.leg_process_flag       = 'P',
             xiler.last_updated_date      = SYSDATE,
             xiler.last_updated_by        = g_user_id,
             xiler.last_update_login      = g_login_id,
             xiler.program_id             = g_conc_program_id,
             xiler.program_application_id = g_prog_appl_id,
             xiler.program_update_date    = SYSDATE
       WHERE xiler.leg_process_flag = 'V'
         AND EXISTS
       (SELECT 1
                FROM xxap_invoice_lines_stg xils
               WHERE xils.interface_txn_id = xiler.interface_txn_id);

      COMMIT;
      -- Either no data to load from extraction table or records already exist in R12 staging table and hence not loaded
    ELSE
      fnd_file.put_line(fnd_file.LOG,
                        'Either no data found for loading from extraction table or records already exist in R12 staging table and hence not loaded or no corresponding header records loaded');

      UPDATE xxap_invoice_lines_ext_r12 xiler
         SET xiler.leg_process_flag       = 'E',
             xiler.last_updated_date      = SYSDATE,
             xiler.last_updated_by        = g_user_id,
             xiler.last_update_login      = g_login_id,
             xiler.program_id             = g_conc_program_id,
             xiler.program_application_id = g_prog_appl_id,
             xiler.program_update_date    = SYSDATE
       WHERE xiler.leg_process_flag = 'V'
         AND EXISTS
       (SELECT 1
                FROM xxap_invoice_lines_stg xils
               WHERE xils.interface_txn_id = xiler.interface_txn_id);

      lv_ret_code := 'E';
      COMMIT;
    END IF;

    --Ver1.3 start
    fnd_file.put_line(fnd_file.LOG,
                      'BULK UPLOAD PROGRAM ENDS FOR THIRD TABLE');
    fnd_file.put_line(fnd_file.LOG,
                      'BULK UPDATE OF STAGING TABLE3: STARTS ');
    g_total_count  := 0;
    g_failed_count := 0;
    g_loaded_count := 0;

    --      IF g_loaded_line_chk <> 0
    --      THEN                                                                                               --1.1
    OPEN cur_data_hold_stg;

    LOOP
      l_invoice_hold_ext_tbl.DELETE;

      FETCH cur_data_hold_stg BULK COLLECT
        INTO l_invoice_hold_ext_tbl LIMIT 1000;

      ---limit size of bulk fetch

      -- Get Total Count
      g_total_count := g_total_count + l_invoice_hold_ext_tbl.COUNT;
      EXIT WHEN l_invoice_hold_ext_tbl.COUNT = 0;

      BEGIN
        FORALL indx IN 1 .. l_invoice_hold_ext_tbl.COUNT SAVE EXCEPTIONS
        ---insert into custom staging table
          INSERT INTO xxap_invoice_holds_stg
          VALUES l_invoice_hold_ext_tbl
            (indx);
      EXCEPTION
        WHEN OTHERS THEN
          FOR l_indx_exp IN 1 .. SQL%BULK_EXCEPTIONS.COUNT LOOP
            l_err_record := l_invoice_hold_ext_tbl(SQL%BULK_EXCEPTIONS(l_indx_exp).ERROR_INDEX)
                            .interface_txn_id;
            -- pov_return_status := 'E';
            fnd_file.put_line(fnd_file.LOG,
                              'Record sequence : ' || l_invoice_hold_ext_tbl(SQL%BULK_EXCEPTIONS(l_indx_exp).ERROR_INDEX)
                              .interface_txn_id);
            fnd_file.put_line(fnd_file.LOG,
                              'Error Message : ' ||
                              SQLERRM(-SQL%BULK_EXCEPTIONS(l_indx_exp)
                                      .ERROR_CODE));

            -- Updating Leg_process_flag to 'E' for failed records
            UPDATE xxap_invoice_holds_ext_r12 xiler
               SET xiler.leg_process_flag = 'E'
             WHERE xiler.interface_txn_id = l_err_record
               AND xiler.leg_process_flag = 'V';

            g_failed_count := g_failed_count + SQL%ROWCOUNT;
          END LOOP;

          fnd_file.put_line(fnd_file.LOG,
                            'BULK EXCEPTION FOR LOOP ENDS FOR SECOND TABLE');
      END;
    END LOOP;

    CLOSE cur_data_hold_stg;

    --    END IF;
    COMMIT;
    fnd_file.put_line(fnd_file.LOG, 'BULK UPLOAD PROGRAM ENDS');
    g_loaded_count := g_total_count - g_failed_count;
    fnd_file.put_line(fnd_file.output, ' Stats for hold table load ');
    fnd_file.put_line(fnd_file.output, '================================');
    fnd_file.put_line(fnd_file.output, 'Total Count : ' || g_total_count);
    fnd_file.put_line(fnd_file.output, 'Loaded Count: ' || g_loaded_count);
    fnd_file.put_line(fnd_file.output, 'Failed Count: ' || g_failed_count);
    fnd_file.put_line(fnd_file.output, '================================');

    -- updating successful record process_flag to 'P'
    -- If records successfully posted to conversion staging table
    IF g_total_count > 0 THEN
      fnd_file.put_line(fnd_file.LOG,
                        'Updating process flag (leg_process_flag) in extraction table for processed records ');

      UPDATE xxap_invoice_holds_ext_r12 xiler
         SET xiler.leg_process_flag = 'P'
       WHERE xiler.leg_process_flag = 'V'
         AND EXISTS
       (SELECT 1
                FROM xxap_invoice_lines_stg xils
               WHERE xils.interface_txn_id = xiler.interface_txn_id);

      COMMIT;
      -- Either no data to load from extraction table or records already exist in R12 staging table and hence not loaded
    ELSE
      fnd_file.put_line(fnd_file.LOG,
                        'Either no data found for loading from extraction table or records already exist in R12 staging table and hence not loaded or no corresponding header records loaded');

      UPDATE xxap_invoice_holds_ext_r12 xiler
         SET xiler.leg_process_flag = 'E'
       WHERE xiler.leg_process_flag = 'V'
         AND EXISTS
       (SELECT 1
                FROM xxap_invoice_lines_stg xils
               WHERE xils.interface_txn_id = xiler.interface_txn_id);

      lv_ret_code := 'E';
      COMMIT;
    END IF;

    --Ver1.3 end
    IF lv_ret_code = 'E' THEN
      g_retcode := 1;
    END IF;

    --v1.10
    BEGIN

       OPEN cur_inv_lin_acct;
       LOOP
          FETCH cur_inv_lin_acct BULK COLLECT
          INTO stg_line_det;-- LIMIT 10000;
          EXIT WHEN cur_inv_lin_acct%NOTFOUND;
       END LOOP;
       CLOSE cur_inv_lin_acct;

       IF stg_line_det.COUNT > 0 THEN

          FORALL i IN stg_line_det.FIRST .. stg_line_det.LAST
          UPDATE xxap_invoices_Stg
          SET leg_accts_pay_code_concat = SUBSTR(stg_line_det(i)
                                                .leg_dist_code_concatenated,
                                                1,
                                                instr(stg_line_det(i)
                                                      .leg_dist_code_concatenated,
                                                      '.',
                                                      1,
                                                      2) - 1) ||
                                         SUBSTR(leg_accts_pay_code_concat,
                                                instr(leg_accts_pay_code_concat,
                                                      '.',
                                                      1,
                                                      2))
          WHERE leg_invoice_id = stg_line_det(i).leg_invoice_id
          --AND leg_request_id = '&P_LEG_REQUEST_ID'
          AND process_flag IS NULL
          AND leg_Source_System = stg_line_det(i).leg_source_system
          --AND leg_org_id = NVL('&P_LEG_OPERATING_UNIT', leg_org_id)
          /*AND process_Flag IS NULL*/;

          COMMIT;



        END IF;
    EXCEPTION

    WHEN OTHERS THEN
       IF cur_inv_lin_acct%ISOPEN THEN
         CLOSE cur_inv_lin_acct;
       END IF;
       g_retcode := 1;
       l_error_cnt := SQL%BULK_EXCEPTIONS.count;
       fnd_file.put_line(fnd_file.log, 'Number of failures while updating liability account : ' || l_error_cnt);
       FOR i IN 1 .. l_error_cnt LOOP
         fnd_file.put_line(fnd_file.log,
                           'Error: ' || i || ' Array Index: ' || SQL%BULK_EXCEPTIONS(i)
                           .error_index || ' Message: ' ||
                            SQLERRM(-SQL%BULK_EXCEPTIONS(i).ERROR_CODE));
       END LOOP;
    END;

    --AWT Line changes below
    FOR upd_amt_rec IN upd_amt_Awt LOOP

         UPDATE xxap_invoices_stg
         SET leg_invoice_amount = leg_invoice_amount + upd_amt_rec.awt_total
         WHERE leg_invoice_id = upd_amt_rec.leg_invoice_id
         AND   leg_source_System = upd_amt_rec.leg_source_System
         AND process_flag IS NULL;

         COMMIT;--v3.0
         UPDATE Xxap_Invoice_Lines_Stg
         SET leg_line_type_lookup_code = 'MISCELLANEOUS'
         WHERE leg_invoice_id = upd_amt_rec.leg_invoice_id
         AND leg_source_System = upd_amt_rec.leg_source_System
         AND leg_line_type_lookup_code = 'AWT'
         AND process_flag IS NULL;

      END LOOP;
    --v1.10 ends
    --v3.0
          UPDATE xxap_invoices_Stg
          SET leg_inv_type_lookup_code = DECODE(SIGN(leg_invoice_amount),
                                                              '1', 'STANDARD',
                                                              '-1', 'DEBIT'
                                                              ,leg_inv_type_lookup_code)
          WHERE leg_inv_type_lookup_code IN ( 'MIXED','STANDARD');
       COMMIT;
    --v3.0 ends

  EXCEPTION
    WHEN OTHERS THEN
      fnd_file.put_line(fnd_file.LOG,
                        'BULK INSERT INTO STAGING TABLE FAILED DUE TO ERROR : ' ||
                        SQLERRM);
      g_retcode := 2;
  END load_data;

  -------------------------------------------------------------------------------------------------------------------------------
  -- TYPE                         :         PROCEDURE
  -- NAME                         :         ASSIGN_BATCH_ID
  -- INPUT OUTPUT PARAMETERS      :         NA
  -- INPUT PARAMETERS             :         NA
  -- OUTPUT PARAMETERS            :         NA
  -- PURPOSE                      :         This procedure is used to
  --                                        assign batch id and run seq id
  --
  -------------------------------------------------------------------------------------------------------------------------------
  PROCEDURE assign_batch_id IS
  BEGIN
    -- g_batch_id NULL is considered a fresh run
    IF g_batch_id IS NULL THEN
      BEGIN
        xxetn_debug_pkg.add_debug(piv_debug_msg => 'Generating new batch id..header');

        UPDATE xxap_invoices_stg
           SET batch_id = g_new_batch_id,
               --TUT changes start
               process_flag = 'N',
               --TUT changes end
               run_sequence_id        = g_run_seq_id,
               last_updated_date      = SYSDATE,
               last_updated_by        = g_user_id,
               last_update_login      = g_login_id,
               request_id             = g_request_id,
               program_application_id = g_prog_appl_id,
               program_id             = g_conc_program_id
         WHERE 1 = 1
           AND batch_id IS NULL
           AND leg_operating_unit =
               NVL(g_operating_unit, leg_operating_unit);
      EXCEPTION
        WHEN OTHERS THEN
          fnd_file.put_line(fnd_file.LOG,
                            'Error : Exception occured while updating new batch id in staging ' ||
                            SUBSTR(SQLERRM, 1, 150));
      END;

      BEGIN
        xxetn_debug_pkg.add_debug(piv_debug_msg => 'Generating new batch id..lines');

        UPDATE xxap_invoice_lines_stg
           SET batch_id = g_new_batch_id,
               --TUT changes start
               process_flag = 'N',
               --TUT changes end
               run_sequence_id        = g_run_seq_id,
               last_updated_date      = SYSDATE,
               last_updated_by        = g_user_id,
               last_update_login      = g_login_id,
               request_id             = g_request_id,
               program_application_id = g_prog_appl_id,
               program_id             = g_conc_program_id
         WHERE 1 = 1
           AND batch_id IS NULL
           AND leg_operating_unit =
               NVL(g_operating_unit, leg_operating_unit);
      EXCEPTION
        WHEN OTHERS THEN
          fnd_file.put_line(fnd_file.LOG,
                            'Error : Exception occured while updating new batch id in lines staging ' ||
                            SUBSTR(SQLERRM, 1, 150));
      END;

      BEGIN
        xxetn_debug_pkg.add_debug(piv_debug_msg => 'Generating new batch id..hold');

        UPDATE xxap_invoice_holds_stg
           SET batch_id = g_new_batch_id,
               --TUT changes start
               process_flag = 'N',
               --TUT changes end
               run_sequence_id = g_run_seq_id,
               request_id      = g_request_id
         WHERE 1 = 1
           AND batch_id IS NULL
           AND leg_operating_unit =
               NVL(g_operating_unit, leg_operating_unit);
      EXCEPTION
        WHEN OTHERS THEN
          fnd_file.put_line(fnd_file.LOG,
                            'Error : Exception occured while updating new batch id in staging hold ' ||
                            SUBSTR(SQLERRM, 1, 150));
      END;

      COMMIT;
    ELSE
      BEGIN
        xxetn_debug_pkg.add_debug(piv_debug_msg => 'Reprocess updating run sequence id: header');

        UPDATE xxap_invoices_stg
           SET process_flag = 'N'
               --, batch_id               = g_new_batch_id
              ,
               run_sequence_id        = g_run_seq_id,
               last_updated_date      = SYSDATE,
               last_updated_by        = g_user_id,
               last_update_login      = g_login_id,
               request_id             = g_request_id,
               program_application_id = g_prog_appl_id,
               program_id             = g_conc_program_id
         WHERE 1 = 1
           AND batch_id = g_new_batch_id
           AND ((g_process_records = 'ALL' AND
               process_flag NOT IN ('C', 'X', 'V')) OR
               (g_process_records = 'ERROR' AND (process_flag = 'E')) OR
               (g_process_records = 'UNPROCESSED' AND (process_flag = 'N')));
      EXCEPTION
        WHEN OTHERS THEN
          fnd_file.put_line(fnd_file.LOG,
                            'Error : Exception occured while updating run seq id for reprocess: ' ||
                            SUBSTR(SQLERRM, 1, 150));
      END;

      BEGIN
        xxetn_debug_pkg.add_debug(piv_debug_msg => 'Reprocess updating run sequence id: lines');

        UPDATE xxap_invoice_lines_stg
           SET process_flag = 'N'
               --, batch_id               = g_new_batch_id
              ,
               run_sequence_id        = g_run_seq_id,
               last_updated_date      = SYSDATE,
               last_updated_by        = g_user_id,
               last_update_login      = g_login_id,
               request_id             = g_request_id,
               program_application_id = g_prog_appl_id,
               program_id             = g_conc_program_id
         WHERE 1 = 1
           AND batch_id = g_new_batch_id
           AND ((g_process_records = 'ALL' AND
               process_flag NOT IN ('C', 'X', 'V','S','O')) OR
               (g_process_records = 'ERROR' AND (process_flag = 'E')
                ) OR
               (g_process_records = 'UNPROCESSED' AND (process_flag = 'N')));
      EXCEPTION
        WHEN OTHERS THEN
          fnd_file.put_line(fnd_file.LOG,
                            'Error : Exception occured while updating run seq id for reprocess for lines: ' ||
                            SUBSTR(SQLERRM, 1, 150));
      END;

      COMMIT;
    END IF;
  EXCEPTION
    WHEN OTHERS THEN
      fnd_file.put_line(fnd_file.LOG,
                        'Error : Exception occured in assign batch id procedure: ' ||
                        SUBSTR(SQLERRM, 1, 150));
      fnd_file.put_line(fnd_file.LOG,'Error at line: '||DBMS_UTILITY.format_error_backtrace());
  END;

  --changes for v1.3 starts here
  PROCEDURE validate_supplier(piv_vendor_num IN VARCHAR2,
                              pou_suppl_err_code OUT VARCHAR2,
                              pou_suppl_err_msg OUT VARCHAR2) IS
     l_supp_id NUMBER;
     l_supp_name ap_suppliers.vendor_name%TYPE;
  BEGIN

     l_supp_id       := NULL;
     l_supp_name     := NULL;

          --validate vendor and derive vendor ID
     SELECT DISTINCT aps.vendor_id, aps.vendor_name
            INTO l_supp_id, l_supp_name
            FROM  apps.ap_suppliers aps
            WHERE (aps.segment1) = (piv_vendor_num)
            AND NVL(aps.end_date_active, SYSDATE) >= SYSDATE;
     fnd_file.put_line(fnd_file.log, 'Vendor ID is '||l_supp_id ||'l_supp_name '||l_supp_name);
  EXCEPTION
     WHEN NO_DATA_FOUND THEN
        print_log_message('In No Data found of validate supplier check');
        pou_suppl_err_code := 'ETN_AP_INVALID_SUPPLIER';
        pou_suppl_err_msg  := 'Supplier is not set up in R12';
     WHEN OTHERS THEN
        print_log_message('In When others of validate supplier check' ||
                          SUBSTR(SQLERRM, 1, 150));
        pou_suppl_err_code   := 'ETN_AP_INVALID_SUPPLIER';
        pou_suppl_err_msg    := 'Error while validating supplier ' ||
                            SUBSTR(SQLERRM, 1, 150);
  END;

  PROCEDURE Create_new_site(piv_vendor_id IN NUMBER,
                            piv_vendor_site_id IN NUMBER,
                            piv_liab_ccid IN NUMBER,
                            piv_org IN NUMBER,
                            piv_vend_Site_code VARCHAR2,
                            pou_vendor_site_id OUT NUMBER,
                            /*pou_location_id OUT NUMBER,
                            pou_party_site_id OUT NUMBER*/
                            pou_error_code OUT VARCHAR2,
                            pou_error_mssg OUT VARCHAR2)
  IS

     CURSOR source_ven_site_cur IS
     SELECT * FROM ap_supplier_sites_all
     WHERE vendor_id = piv_vendor_id
     AND   vendor_site_id = piv_vendor_site_id ;

     CURSOR source_bank_acct_cur IS
     SELECT  ieba.*, aps.party_id party_id_supp, assa.party_site_id party_Site_id_sup, ipiua.order_of_preference
     FROM          ap_supplier_sites_all              assa
                   ,hz_parties                         hp
                   ,iby_ext_bank_accounts              ieba
                   ,iby_external_payees_all            iepa
                   ,iby_pmt_instr_uses_all             ipiua
                   ,ap_suppliers                       aps
                  ,hz_parties                         hp1
     WHERE        assa.vendor_site_id         =      iepa.supplier_site_id
     AND hp.party_id                 =      ieba.bank_id
     AND ipiua.instrument_id         =      ieba.ext_bank_account_id
     AND ipiua.ext_pmt_party_id      =      iepa.ext_payee_id
     AND assa.vendor_id              =      aps.vendor_id
     AND ieba.branch_id              =      hp1.party_id
     AND ipiua.instrument_type       =      'BANKACCOUNT'
     AND ipiua.payment_flow          =      'DISBURSEMENTS'
     --AND ipiua.order_of_preference   =      1
     AND assa.vendor_site_id = piv_vendor_site_id
     AND TRUNC(SYSDATE) BETWEEN
             TRUNC(NVL(ieba.start_date, SYSDATE)) AND
             TRUNC(NVL(ieba.end_date, SYSDATE));

     CURSOR ext_pymt_cur IS
     SELECT   /*eppm.payment_method_code, assa.vendor_site_id,
            assa.org_id,*/ iepa.*
     FROM ap_supplier_sites_all assa,
          ap_suppliers sup,
          iby_external_payees_all iepa,
          iby_ext_party_pmt_mthds ieppm
     WHERE sup.vendor_id = assa.vendor_id
     AND assa.pay_site_flag = 'Y'
     AND assa.vendor_site_id = iepa.supplier_site_id
     AND iepa.ext_payee_id = ieppm.ext_pmt_party_id(+)
     AND assa.vendor_site_id =piv_vendor_site_id
     AND sup.vendor_id = piv_vendor_id;

     l_vendor_site_rec AP_VENDOR_PUB_PKG.r_vendor_site_rec_type;

     x_vendor_site_id NUMBER;
     x_party_site_id NUMBER;
     x_location_id NUMBER;
     x_return_status VARCHAR2(2000);
     x_msg_count NUMBER;
     x_msg_data VARCHAR2(2000);
     l_error_mssg VARCHAR2(2000);

     l_ext_bank_acct_rec        iby_ext_bankacct_pub.extbankacct_rec_type;
     l_instrument_rec           iby_fndcpt_setup_pub.pmtinstrument_rec_type;
     l_payee_rec                iby_disbursement_setup_pub.payeecontext_rec_type;
     l_assignment_attribs_rec   iby_fndcpt_setup_pub.pmtinstrassignment_rec_type;
     l_bank_acct_id             iby_ext_bank_accounts.ext_bank_account_id%TYPE;
     l_payee_err_msg   VARCHAR2(2000);
     l_payee_msg_count NUMBER;
     l_payee_ret_status         VARCHAR2(50);
     l_assign_id                NUMBER;
     l_payee_result_rec         iby_fndcpt_common_pub.result_rec_type;

     --
     t_output                    VARCHAR2(200)                         := NULL;
     t_msg_dummy                 VARCHAR2(200)                         := NULL;
     l_payee_upd_status iby_disbursement_setup_pub.ext_payee_update_tab_type;
     l_external_payee_tab_type iby_disbursement_setup_pub.external_payee_tab_type ;
     l_ext_payee_id_tab_type iby_disbursement_setup_pub.ext_payee_id_tab_type;
     i                           NUMBER                                    := 0;
     --

     FUNCTION get_new_ccid(piv_liab_acct_ccid IN NUMBER, piv_supp_acct_ccid IN NUMBER)
     RETURN NUMBER
     IS
        l_segment1 gl_Code_combinations.segment1%TYPE;
        l_segment2 gl_Code_combinations.segment1%TYPE;
        l_segment3 gl_Code_combinations.segment1%TYPE;
        l_segment4 gl_Code_combinations.segment1%TYPE;
        l_segment5 gl_Code_combinations.segment1%TYPE;
        l_segment6 gl_Code_combinations.segment1%TYPE;
        l_segment7 gl_Code_combinations.segment1%TYPE;
        l_segment8 gl_Code_combinations.segment1%TYPE;
        l_segment9 gl_Code_combinations.segment1%TYPE;
        l_segment10 gl_Code_combinations.segment1%TYPE;
        l_acct_ccid NUMBER := 0;
     BEGIN
        SELECT SEGMENT1
               ,SEGMENT2
        INTO l_segment1, l_segment2
        FROM gl_code_combinations
        WHERE code_combination_id = NVL(piv_liab_acct_ccid,0);
        --today
        /*fnd_file.put_line(fnd_file.log,'ccid for liab ' ||piv_liab_acct_ccid);
        fnd_file.put_line(fnd_file.log,'segment1 from liability ' ||l_segment1);
        fnd_file.put_line(fnd_file.log,'segment2 from liability ' ||l_segment2);*/
        --today

        SELECT SEGMENT3
               ,SEGMENT4
               ,SEGMENT5
               ,SEGMENT6
               ,SEGMENT7
               ,SEGMENT8
               ,SEGMENT9
               ,SEGMENT10
         INTO  l_segment3, l_segment4,l_segment5, l_segment6,l_segment7, l_segment8
               ,l_segment9, l_segment10
         FROM gl_code_combinations
         WHERE code_combination_id = NVL(piv_supp_acct_ccid,0);

         SELECT code_combination_id
         INTO   l_acct_ccid
         FROM gl_code_combinations
         WHERE segment1 = l_segment1
         AND   segment2 = l_segment2
         AND SEGMENT3 = l_segment3
         AND SEGMENT4 = l_segment4
         AND SEGMENT5 = l_segment5
         AND SEGMENT6 = l_segment6
         AND SEGMENT7 = l_segment7
         AND SEGMENT8 = l_segment8
         AND SEGMENT9 = l_segment9
         AND SEGMENT10 = l_segment10;
         --today
        /*fnd_file.put_line(fnd_file.log,'segment3 from suppl acct' ||l_segment3);
        fnd_file.put_line(fnd_file.log,'segment4 from suppl acct' ||l_segment4);
        fnd_file.put_line(fnd_file.log,'segment5 from suppl acct' ||l_segment5);
        fnd_file.put_line(fnd_file.log,'segment6 from suppl acct' ||l_segment6);
        fnd_file.put_line(fnd_file.log,'segment7 from suppl acct' ||l_segment7);
        fnd_file.put_line(fnd_file.log,'segment8 from suppl acct' ||l_segment8);
        fnd_file.put_line(fnd_file.log,'segment9 from suppl acct' ||l_segment9);
        fnd_file.put_line(fnd_file.log,'segment10 from suppl acct' ||l_segment10);*/
        --today
         RETURN l_acct_ccid;
     EXCEPTION
        WHEN OTHERS
           THEN
             --today
        /*fnd_file.put_line(fnd_file.log,'in exception for liab create site');*/
        l_Acct_ccid := null;
             RETURN l_acct_ccid;
     END;

  BEGIN


     FOR source_site_rec in source_ven_site_cur

     LOOP

        l_vendor_site_rec.area_code             := source_site_rec.area_code;
        l_vendor_site_rec.phone                 := source_site_rec.phone;
        l_vendor_site_rec.customer_num          := source_site_rec.customer_num;
        l_vendor_site_rec.SHIP_TO_LOCATION_ID   := source_site_rec.SHIP_TO_LOCATION_ID;
        l_vendor_site_rec.BILL_TO_LOCATION_ID   := source_site_rec.BILL_TO_LOCATION_ID;
        l_vendor_site_rec.SHIP_VIA_LOOKUP_CODE  := source_site_rec.SHIP_VIA_LOOKUP_CODE;
        l_vendor_site_rec.FREIGHT_TERMS_LOOKUP_CODE  := source_site_rec.FREIGHT_TERMS_LOOKUP_CODE;
        l_vendor_site_rec.FOB_LOOKUP_CODE            := source_site_rec.FOB_LOOKUP_CODE;
        l_vendor_site_rec.INACTIVE_DATE              := source_site_rec.INACTIVE_DATE;
        l_vendor_site_rec.FAX                        := source_site_rec.fax;
        l_vendor_site_rec.FAX_AREA_CODE              := source_site_rec.FAX_AREA_CODE;
        l_vendor_site_rec.TELEX                      := source_site_rec.TELEX;
        l_vendor_site_rec.TERMS_DATE_BASIS           := source_site_rec.TERMS_DATE_BASIS;
        l_vendor_site_rec.DISTRIBUTION_SET_ID             := source_site_rec.DISTRIBUTION_SET_ID;
        l_vendor_site_rec.ACCTS_PAY_CODE_COMBINATION_ID   := get_new_ccid(piv_liab_ccid,
                                                             source_site_rec.accts_pay_code_combination_id);
        l_vendor_site_rec.PREPAY_CODE_COMBINATION_ID      := get_new_ccid(piv_liab_ccid,
                                                             source_site_rec.prepay_code_combination_id);
        l_vendor_site_rec.PAY_GROUP_LOOKUP_CODE           := source_site_rec.PAY_GROUP_LOOKUP_CODE;
        l_vendor_site_rec.PAYMENT_PRIORITY                := source_site_rec.PAYMENT_PRIORITY;
        l_vendor_site_rec.TERMS_ID                        := source_site_rec.TERMS_ID;
        l_vendor_site_rec.INVOICE_AMOUNT_LIMIT            := source_site_rec.INVOICE_AMOUNT_LIMIT;
        l_vendor_site_rec.PAY_DATE_BASIS_LOOKUP_CODE      := source_site_rec.PAY_DATE_BASIS_LOOKUP_CODE;
        l_vendor_site_rec.ALWAYS_TAKE_DISC_FLAG           := source_site_rec.ALWAYS_TAKE_DISC_FLAG;
        l_vendor_site_rec.INVOICE_CURRENCY_CODE           := source_site_rec.INVOICE_CURRENCY_CODE;
        l_vendor_site_rec.PAYMENT_CURRENCY_CODE           := source_site_rec.PAYMENT_CURRENCY_CODE;
        --l_vendor_site_rec.VENDOR_SITE_ID                  := source_site_rec.
        l_vendor_site_rec.LAST_UPDATE_DATE                := SYSDATE;
        l_vendor_site_rec.LAST_UPDATED_BY                 := fnd_profile.value('USER_ID');
        l_vendor_site_rec.VENDOR_ID                       := piv_vendor_id;
        l_vendor_site_rec.VENDOR_SITE_CODE                := piv_vend_Site_code;
        l_vendor_site_rec.VENDOR_SITE_CODE_ALT            := source_site_rec.VENDOR_SITE_CODE_ALT;
        l_vendor_site_rec.PURCHASING_SITE_FLAG            := source_site_rec.PURCHASING_SITE_FLAG;
        l_vendor_site_rec.RFQ_ONLY_SITE_FLAG              := source_site_rec.RFQ_ONLY_SITE_FLAG;
        l_vendor_site_rec.PAY_SITE_FLAG                   := source_site_rec.PAY_SITE_FLAG;
        l_vendor_site_rec.ATTENTION_AR_FLAG               := source_site_rec.ATTENTION_AR_FLAG;
        l_vendor_site_rec.HOLD_ALL_PAYMENTS_FLAG          := source_site_rec.HOLD_ALL_PAYMENTS_FLAG;
        l_vendor_site_rec.HOLD_FUTURE_PAYMENTS_FLAG       := source_site_rec.HOLD_FUTURE_PAYMENTS_FLAG;
        l_vendor_site_rec.HOLD_REASON                     := source_site_rec.HOLD_REASON;
        l_vendor_site_rec.HOLD_UNMATCHED_INVOICES_FLAG    := source_site_rec.HOLD_UNMATCHED_INVOICES_FLAG;
        l_vendor_site_rec.TAX_REPORTING_SITE_FLAG    := source_site_rec.TAX_REPORTING_SITE_FLAG;
        l_vendor_site_rec.ATTRIBUTE_CATEGORY    := source_site_rec.ATTRIBUTE_CATEGORY;
        l_vendor_site_rec.ATTRIBUTE1      := source_site_rec.ATTRIBUTE1;
        l_vendor_site_rec.ATTRIBUTE2      := source_site_rec.ATTRIBUTE2;
        l_vendor_site_rec.ATTRIBUTE3      := source_site_rec.ATTRIBUTE3;
        l_vendor_site_rec.ATTRIBUTE4      := source_site_rec.ATTRIBUTE4;
        l_vendor_site_rec.ATTRIBUTE5      := source_site_rec.ATTRIBUTE5;
        l_vendor_site_rec.ATTRIBUTE6      := source_site_rec.ATTRIBUTE6;
        l_vendor_site_rec.ATTRIBUTE7      := source_site_rec.ATTRIBUTE7;
        l_vendor_site_rec.ATTRIBUTE8      := source_site_rec.ATTRIBUTE8;
        l_vendor_site_rec.ATTRIBUTE9      := source_site_rec.ATTRIBUTE9;
        l_vendor_site_rec.ATTRIBUTE10     := source_site_rec.ATTRIBUTE10;
        l_vendor_site_rec.ATTRIBUTE11      := source_site_rec.ATTRIBUTE11;
        l_vendor_site_rec.ATTRIBUTE12      := source_site_rec.ATTRIBUTE12;
        l_vendor_site_rec.ATTRIBUTE13      := source_site_rec.ATTRIBUTE13;
        l_vendor_site_rec.ATTRIBUTE14      := source_site_rec.ATTRIBUTE14;
        l_vendor_site_rec.ATTRIBUTE15      := source_site_rec.ATTRIBUTE15;
        l_vendor_site_rec.VALIDATION_NUMBER               := source_site_rec.VALIDATION_NUMBER;
        l_vendor_site_rec.EXCLUDE_FREIGHT_FROM_DISCOUNT  := source_site_rec.EXCLUDE_FREIGHT_FROM_DISCOUNT;
        l_vendor_site_rec.BANK_CHARGE_BEARER             := source_site_rec.BANK_CHARGE_BEARER;
        l_vendor_site_rec.ORG_ID                         := piv_org;
        l_vendor_site_rec.CHECK_DIGITS                   := source_site_rec.CHECK_DIGITS;
        l_vendor_site_rec.ALLOW_AWT_FLAG                 := source_site_rec.ALLOW_AWT_FLAG;
        l_vendor_site_rec.AWT_GROUP_ID                   := source_site_rec.AWT_GROUP_ID;
        l_vendor_site_rec.PAY_AWT_GROUP_ID               := source_site_rec.PAY_AWT_GROUP_ID;
        l_vendor_site_rec.DEFAULT_PAY_SITE_ID            := source_site_rec.DEFAULT_PAY_SITE_ID;
        l_vendor_site_rec.PAY_ON_CODE                    := source_site_rec.PAY_ON_CODE;
        l_vendor_site_rec.PAY_ON_RECEIPT_SUMMARY_CODE    := source_site_rec.PAY_ON_RECEIPT_SUMMARY_CODE;
        l_vendor_site_rec.GLOBAL_ATTRIBUTE_CATEGORY      := source_site_rec.GLOBAL_ATTRIBUTE_CATEGORY;
        l_vendor_site_rec.GLOBAL_ATTRIBUTE1              := source_site_rec.GLOBAL_ATTRIBUTE1;
        l_vendor_site_rec.GLOBAL_ATTRIBUTE2              := source_site_rec.GLOBAL_ATTRIBUTE2;
        l_vendor_site_rec.GLOBAL_ATTRIBUTE3              := source_site_rec.GLOBAL_ATTRIBUTE3;
        l_vendor_site_rec.GLOBAL_ATTRIBUTE4              := source_site_rec.GLOBAL_ATTRIBUTE4;
        l_vendor_site_rec.GLOBAL_ATTRIBUTE5              := source_site_rec.GLOBAL_ATTRIBUTE5;
        l_vendor_site_rec.GLOBAL_ATTRIBUTE6              := source_site_rec.GLOBAL_ATTRIBUTE6;
        l_vendor_site_rec.GLOBAL_ATTRIBUTE7              := source_site_rec.GLOBAL_ATTRIBUTE7;
        l_vendor_site_rec.GLOBAL_ATTRIBUTE8              := source_site_rec.GLOBAL_ATTRIBUTE8;
        l_vendor_site_rec.GLOBAL_ATTRIBUTE9              := source_site_rec.GLOBAL_ATTRIBUTE9;
        l_vendor_site_rec.GLOBAL_ATTRIBUTE10             := source_site_rec.GLOBAL_ATTRIBUTE10;
        l_vendor_site_rec.GLOBAL_ATTRIBUTE11             := source_site_rec.GLOBAL_ATTRIBUTE11;
        l_vendor_site_rec.GLOBAL_ATTRIBUTE12             := source_site_rec.GLOBAL_ATTRIBUTE12;
        l_vendor_site_rec.GLOBAL_ATTRIBUTE13             := source_site_rec.GLOBAL_ATTRIBUTE13;
        l_vendor_site_rec.GLOBAL_ATTRIBUTE14             := source_site_rec.GLOBAL_ATTRIBUTE14;
        l_vendor_site_rec.GLOBAL_ATTRIBUTE15             := source_site_rec.GLOBAL_ATTRIBUTE15;
        l_vendor_site_rec.GLOBAL_ATTRIBUTE16             := source_site_rec.GLOBAL_ATTRIBUTE16;
        l_vendor_site_rec.GLOBAL_ATTRIBUTE17             := source_site_rec.GLOBAL_ATTRIBUTE17;
        l_vendor_site_rec.GLOBAL_ATTRIBUTE18             := source_site_rec.GLOBAL_ATTRIBUTE18;
        l_vendor_site_rec.GLOBAL_ATTRIBUTE19             := source_site_rec.GLOBAL_ATTRIBUTE19;
        l_vendor_site_rec.GLOBAL_ATTRIBUTE20             := source_site_rec.GLOBAL_ATTRIBUTE20;
        l_vendor_site_rec.TP_HEADER_ID                   := source_site_rec.TP_HEADER_ID;
        l_vendor_site_rec.ECE_TP_LOCATION_CODE           := source_site_rec.ECE_TP_LOCATION_CODE;
        l_vendor_site_rec.PCARD_SITE_FLAG                := source_site_rec.PCARD_SITE_FLAG;
        l_vendor_site_rec.MATCH_OPTION                   := source_site_rec.MATCH_OPTION;
        l_vendor_site_rec.COUNTRY_OF_ORIGIN_CODE         := source_site_rec.COUNTRY_OF_ORIGIN_CODE;
        IF source_site_rec.FUTURE_DATED_PAYMENT_CCID IS NULL
        THEN
           l_vendor_site_rec.FUTURE_DATED_PAYMENT_CCID      := source_site_rec.FUTURE_DATED_PAYMENT_CCID;
        ELSE
           l_vendor_site_rec.FUTURE_DATED_PAYMENT_CCID      := get_new_ccid(piv_liab_ccid,
                                                             source_site_rec.FUTURE_DATED_PAYMENT_CCID);
        END IF;
        l_vendor_site_rec.CREATE_DEBIT_MEMO_FLAG         := source_site_rec.CREATE_DEBIT_MEMO_FLAG;
        l_vendor_site_rec.SUPPLIER_NOTIF_METHOD          := source_site_rec.SUPPLIER_NOTIF_METHOD;
        l_vendor_site_rec.EMAIL_ADDRESS                  := source_site_rec.EMAIL_ADDRESS;
        l_vendor_site_rec.PRIMARY_PAY_SITE_FLAG          := source_site_rec.PRIMARY_PAY_SITE_FLAG;
        l_vendor_site_rec.SHIPPING_CONTROL               := source_site_rec.SHIPPING_CONTROL;
        l_vendor_site_rec.SELLING_COMPANY_IDENTIFIER     := source_site_rec.SELLING_COMPANY_IDENTIFIER;
        l_vendor_site_rec.GAPLESS_INV_NUM_FLAG           := source_site_rec.GAPLESS_INV_NUM_FLAG;
        l_vendor_site_rec.LOCATION_ID                    := source_site_rec.LOCATION_ID;
        l_vendor_site_rec.PARTY_SITE_ID                  := source_site_rec.party_site_id;
        --l_vendor_site_rec.ORG_NAME                       := source_site_rec.
        l_vendor_site_rec.DUNS_NUMBER                    := source_site_rec.DUNS_NUMBER;
        l_vendor_site_rec.ADDRESS_STYLE                  := source_site_rec.ADDRESS_STYLE;
        l_vendor_site_rec.LANGUAGE                       := source_site_rec.LANGUAGE;
        l_vendor_site_rec.PROVINCE                       := source_site_rec.PROVINCE;
        l_vendor_site_rec.COUNTRY                        := source_site_rec.COUNTRY;
        l_vendor_site_rec.ADDRESS_LINE1                  := source_site_rec.ADDRESS_LINE1;
        l_vendor_site_rec.ADDRESS_LINE2                  := source_site_rec.ADDRESS_LINE2;
        l_vendor_site_rec.ADDRESS_LINE3                  := source_site_rec.ADDRESS_LINE3;
        l_vendor_site_rec.ADDRESS_LINE4                  := source_site_rec.ADDRESS_LINE4;
        l_vendor_site_rec.ADDRESS_LINES_ALT              := source_site_rec.ADDRESS_LINES_ALT;
        l_vendor_site_rec.COUNTY                         := source_site_rec.COUNTY;
        l_vendor_site_rec.CITY                := source_site_rec.CITY;
        l_vendor_site_rec.STATE               := source_site_rec.STATE;
        l_vendor_site_rec.ZIP                 := source_site_rec.ZIP;
        --l_vendor_site_rec.TERMS_NAME      AP_TERMS_TL.NAME%TYPE,
        --DEFAULT_TERMS_ID    NUMBER,
        --AWT_GROUP_NAME      AP_AWT_GROUPS.NAME%TYPE,
        --PAY_AWT_GROUP_NAME              AP_AWT_GROUPS.NAME%TYPE,--bug6664407
        --DISTRIBUTION_SET_NAME    AP_DISTRIBUTION_SETS_ALL.DISTRIBUTION_SET_NAME%TYPE,
        --SHIP_TO_LOCATION_CODE           HR_LOCATIONS_ALL_TL.LOCATION_CODE%TYPE,
        --BILL_TO_LOCATION_CODE           HR_LOCATIONS_ALL_TL.LOCATION_CODE%TYPE,
        --DEFAULT_DIST_SET_ID             NUMBER,
        --DEFAULT_SHIP_TO_LOC_ID          NUMBER,
        --DEFAULT_BILL_TO_LOC_ID          NUMBER,
        --l_vendor_site_rec.TOLERANCE_ID      := source_site_rec.TOLERANCE_ID;
        --TOLERANCE_NAME      AP_TOLERANCE_TEMPLATES.TOLERANCE_NAME%TYPE,
        --VENDOR_INTERFACE_ID    NUMBER,
        --VENDOR_SITE_INTERFACE_ID  NUMBER,
        --EXT_PAYEE_REC      IBY_DISBURSEMENT_SETUP_PUB.EXTERNAL_PAYEE_REC_TYPE,
        l_vendor_site_rec.RETAINAGE_RATE     := source_site_rec.RETAINAGE_RATE;
        --l_vendor_site_rec.SERVICES_TOLERANCE_ID    := source_site_rec.SERVICES_TOLERANCE_ID  ;
        --SERVICES_TOLERANCE_NAME         AP_TOLERANCE_TEMPLATES.TOLERANCE_NAME%TYPE,
        --SHIPPING_LOCATION_ID            NUMBER,
        l_vendor_site_rec.VAT_CODE              := source_site_rec.VAT_CODE;
        l_vendor_site_rec.VAT_REGISTRATION_NUM  := source_site_rec.VAT_REGISTRATION_NUM;
        l_vendor_site_rec.REMITTANCE_EMAIL      := source_site_rec.REMITTANCE_EMAIL;
        l_vendor_site_rec.EDI_ID_NUMBER         := source_site_rec.EDI_ID_NUMBER;
        l_vendor_site_rec.EDI_PAYMENT_FORMAT    := source_site_rec.EDI_PAYMENT_FORMAT;
        l_vendor_site_rec.EDI_TRANSACTION_HANDLING  := source_site_rec.EDI_TRANSACTION_HANDLING;
        l_vendor_site_rec.EDI_PAYMENT_METHOD        := source_site_rec.EDI_PAYMENT_METHOD;
        l_vendor_site_rec.EDI_REMITTANCE_METHOD     := source_site_rec.EDI_REMITTANCE_METHOD;
        l_vendor_site_rec.EDI_REMITTANCE_INSTRUCTION  := source_site_rec.EDI_REMITTANCE_INSTRUCTION;
        -- PARTY_SITE_NAME
        l_vendor_site_rec.OFFSET_TAX_FLAG            := source_site_rec.OFFSET_TAX_FLAG;
        l_vendor_site_rec.AUTO_TAX_CALC_FLAG         := source_site_rec.AUTO_TAX_CALC_FLAG;
        --,REMIT_ADVICE_DELIVERY_METHOD
        --,REMIT_ADVICE_FAX
        l_vendor_site_rec.CAGE_CODE                  := source_site_rec.CAGE_CODE;
        l_vendor_site_rec.LEGAL_BUSINESS_NAME        := source_site_rec.LEGAL_BUSINESS_NAME;
        l_vendor_site_rec.DOING_BUS_AS_NAME          := source_site_rec.DOING_BUS_AS_NAME;
        l_vendor_site_rec.DIVISION_NAME              := source_site_rec.DIVISION_NAME;
        l_vendor_site_rec.SMALL_BUSINESS_CODE        := source_site_rec.SMALL_BUSINESS_CODE;
        l_vendor_site_rec.CCR_COMMENTS               := source_site_rec.CCR_COMMENTS;
        l_vendor_site_rec.DEBARMENT_START_DATE       := source_site_rec.DEBARMENT_START_DATE;
        l_vendor_site_rec.DEBARMENT_END_DATE         := source_site_rec.DEBARMENT_END_DATE ;
        l_vendor_site_rec.AP_TAX_ROUNDING_RULE       := source_site_rec.AP_TAX_ROUNDING_RULE;
        l_vendor_site_rec.AMOUNT_INCLUDES_TAX_FLAG   := source_site_rec.AMOUNT_INCLUDES_TAX_FLAG;
       FND_FILE.PUT_LINE(FND_FILE.LOG,'api called');
       FND_FILE.PUT_LINE(FND_FILE.LOG,piv_vendor_id ||'-'||
                            piv_vendor_site_id ||'-'||
                            piv_liab_ccid ||'-'||
                            piv_org ||'-'||
                            piv_vend_Site_code);
        mo_global.set_policy_context('S',piv_org);
        AP_VENDOR_PUB_PKG.Create_Vendor_Site
               (p_api_version    => 1,
                x_return_status    => x_return_status,
                x_msg_count        => x_msg_count,
                x_msg_data         => x_msg_data,
                p_vendor_site_rec  => l_vendor_site_rec,
                x_vendor_site_id   => x_vendor_site_id,
                x_party_site_id    => x_party_site_id,
                x_location_id      => x_location_id
               );

               IF (x_return_status <> 'S') THEN
                  fnd_file.put_line(fnd_file.log,'Error Creating Supplier site'||x_msg_data);
                  pou_error_code := 'ETN_AP_SUPP_SITE_CREATE_ERROR';
                  --today
                  fnd_file.put_line(fnd_file.log,'ERR: source site :'||piv_vendor_site_id
                                                 ||' Input Liability :'||piv_liab_ccid
                                                 ||' Generated liabilty CCID :'||
                                                 l_vendor_site_rec.ACCTS_PAY_CODE_COMBINATION_ID
                                                 ||' Prepay CCID :'
                                                 ||l_vendor_site_rec.PREPAY_CODE_COMBINATION_ID
                                                 ||' future pay CCID :'||
                                                 l_vendor_site_rec.FUTURE_DATED_PAYMENT_CCID  );
                  --today
                  IF x_msg_count > 1 THEN
                     FOR i IN 1..x_msg_count LOOP
                         pou_error_mssg := Substr(pou_error_mssg||'-'||substr(FND_MSG_PUB.Get( p_encoded => FND_API.G_FALSE ),1,155),1,155);

                     END LOOP;
                  /*ELSIF  x_msg_count = 1
                     THEN
                     pou_error_code := 'ETN_AP_SUPP_SITE_CREATE_ERROR';
                     pou_error_mssg := x_msg_data;  */
                  END IF;
               END IF;
        END LOOP;

         pou_vendor_site_id := x_vendor_site_id;

         IF x_vendor_site_id IS NOT NULL
         THEN
            FOR source_bank_acct_rec IN source_bank_acct_cur LOOP
               l_assign_id           := NULL;
               l_payee_ret_status               := 'S';
               l_payee_rec.payment_function     := 'PAYABLES_DISB';
               l_payee_rec.party_id             := source_bank_acct_rec.party_id_supp;
               l_payee_rec.party_site_id        := source_bank_acct_rec.party_Site_id_sup;
               l_payee_rec.supplier_site_id     := x_vendor_site_id;
               l_payee_rec.org_id               := piv_org;
               l_payee_rec.org_type             := 'OPERATING_UNIT';
               l_instrument_rec.instrument_id   := source_bank_acct_rec.ext_bank_account_id;
               l_instrument_rec.instrument_type := 'BANKACCOUNT';
               l_assignment_attribs_rec.start_date         := SYSDATE;
              -- l_assignment_attribs_rec.start_date := source_bank_acct_rec.start_date;
               l_assignment_attribs_rec.instrument := l_instrument_rec;


               print_log_message('Before Payee Assignment API');

               iby_disbursement_setup_pub.set_payee_instr_assignment(1.0,
                                                                fnd_api.g_false,
                                                                fnd_api.g_false,
                                                                l_payee_ret_status,
                                                                l_payee_msg_count,
                                                                l_payee_err_msg,
                                                                l_payee_rec,
                                                                l_assignment_attribs_rec,
                                                                l_assign_id,
                                                                l_payee_result_rec);

          /*print_log_message('After Payee Assignment API');
          xxetn_debug_pkg.add_debug('Supplier Bank Payee API status: ' ||
                                    l_payee_ret_status);
          xxetn_debug_pkg.add_debug('l_payee_err_msg: ' || l_payee_err_msg);
          xxetn_debug_pkg.add_debug('Create Bank Payee Assignment: ' ||
                                    l_assign_id);*/

          -- If payee assignment record was successfully created
          /*IF l_payee_ret_status = fnd_api.g_ret_sts_success THEN
            -- Entire data set for account record created successfully
            l_retcode := g_success;
          ELSE
            -- if payee instrument assignment api error

            l_retcode  := g_error;
            l_err_code := 'ETN_AP_ACCOUNT_IMPORT_ERROR';
            l_err_msg  := 'Error : Payee instrument assignment for branch address failed. ';
            print_log_message(l_err_msg);*/
           IF (x_return_status <> 'S') THEN
                    FND_FILE.PUT_LINE(FND_FILE.LOG,'Encountered ERROR in supplier site bank creation!!!');
                    FND_FILE.PUT_LINE(FND_FILE.LOG,SQLCODE||' '||SQLERRM);
                    FND_FILE.PUT_LINE(FND_FILE.LOG,'--------------------------------------');
                    FND_FILE.PUT_LINE(FND_FILE.LOG,x_msg_data);

                    IF x_msg_count > 1 THEN
                        FOR i IN 1..x_msg_count LOOP
                             FND_FILE.PUT_LINE(FND_FILE.LOG,substr(FND_MSG_PUB.Get( p_encoded => FND_API.G_FALSE ),1,255));
                             pou_error_code := 'ETN_AP_SUPSITE_BACT_ASGN_ERR';
                             pou_error_mssg := Substr(pou_error_mssg||substr(FND_MSG_PUB.Get( p_encoded => FND_API.G_FALSE ),1,155),1,155);

                        END LOOP;
                    END IF;
            ELSE
             FND_FILE.PUT_LINE(FND_FILE.LOG,'SUPPLIER SITE BANK UPDATED SUCCESSFULLY AND BANK ID IS '||source_bank_acct_rec.EXT_BANK_ACCOUNT_ID);
            END IF;
             COMMIT;
             END LOOP;

                   FOR crt_pay_rec  IN ext_pymt_cur
                   LOOP
                      i := 0;
                      i := i + 1;
                      l_external_payee_tab_type(i).Payee_Party_Id      := crt_pay_rec.Payee_Party_Id;
                      l_external_payee_tab_type(i).Payment_Function    := crt_pay_rec.Payment_Function;
                      l_external_payee_tab_type(i).Exclusive_Pay_Flag  := crt_pay_rec.Exclusive_Payment_Flag;
                      l_external_payee_tab_type(i).Payee_Party_Site_Id := crt_pay_rec.Party_Site_Id;
                      l_external_payee_tab_type(i).Supplier_Site_Id    := x_vendor_site_id;
                      l_external_payee_tab_type(i).Payer_Org_Id        := piv_org;
                      l_external_payee_tab_type(i).Payer_Org_Type      := crt_pay_rec.Org_Type;
                      l_external_payee_tab_type(i).Default_Pmt_method  := crt_pay_rec.Default_Payment_method_code;
                      l_external_payee_tab_type(i).ECE_TP_Loc_Code     := crt_pay_rec.ECE_TP_Location_Code;
                      l_external_payee_tab_type(i).Bank_Charge_Bearer  := crt_pay_rec.Bank_Charge_Bearer;
                      l_external_payee_tab_type(i).Bank_Instr1_Code    := crt_pay_rec.Bank_Instruction1_Code;
                      l_external_payee_tab_type(i).Bank_Instr2_Code    := crt_pay_rec.Bank_Instruction2_Code;
                      l_external_payee_tab_type(i).Bank_Instr_Detail   := crt_pay_rec.Bank_Instruction_Details;
                      l_external_payee_tab_type(i).Pay_Reason_Code     := crt_pay_rec.Payment_Reason_Code;
                      l_external_payee_tab_type(i).Pay_Reason_Com      := crt_pay_rec.Payment_Reason_Comments;
                      l_external_payee_tab_type(i).Inactive_Date       := crt_pay_rec.Inactive_Date;
                      l_external_payee_tab_type(i).Pay_Message1        := crt_pay_rec.Payment_text_Message1;
                      l_external_payee_tab_type(i).Pay_Message2        := crt_pay_rec.Payment_text_Message2;
                      l_external_payee_tab_type(i).Pay_Message3        := crt_pay_rec.Payment_text_Message3;
                      l_external_payee_tab_type(i).Delivery_Channel    := crt_pay_rec.Delivery_Channel_Code;
                      --Pmt_Format            IBY_FORMATS_B.format_code%TYPE,
                      l_external_payee_tab_type(i).Settlement_Priority          := crt_pay_rec.Settlement_Priority;
                      l_external_payee_tab_type(i).Remit_advice_delivery_method := crt_pay_rec.Remit_advice_delivery_method;
                      l_external_payee_tab_type(i).Remit_advice_email           := crt_pay_rec.Remit_advice_email;
                      /*l_external_payee_tab_type(crt_pay_rec).edi_payment_format;
                        l_external_payee_tab_type(crt_pay_rec).edi_transaction_handling;
                        l_external_payee_tab_type(crt_pay_rec).edi_payment_method;
                        l_external_payee_tab_type(crt_pay_rec).edi_remittance_method;
                        l_external_payee_tab_type(crt_pay_rec).edi_remittance_instruction;
                        */
                      l_external_payee_tab_type(i).remit_advice_fax   :=crt_pay_rec.remit_advice_fax;

     fnd_file.put_line (fnd_file.log,crt_pay_rec.PAYEE_PARTY_ID );
     fnd_file.put_line (fnd_file.log,crt_pay_rec.PAYMENT_FUNCTION );
     fnd_file.put_line (fnd_file.log,crt_pay_rec.PARTY_SITE_ID );
     fnd_file.put_line (fnd_file.log,crt_pay_rec.SUPPLIER_SITE_ID );
     fnd_file.put_line (fnd_file.log,crt_pay_rec.ORG_ID);
     fnd_file.put_line (fnd_file.log,crt_pay_rec.ORG_TYPE );
      --
                    BEGIN

                    SELECT ext_payee_id
                    INTO l_ext_payee_id_tab_type(i).ext_payee_id
                    FROM iby_external_payees_all where supplier_site_id = x_vendor_site_id ;

                    mo_global.set_policy_context ('S', piv_org);

                    iby_disbursement_setup_pub.update_external_payee
                                 (p_api_version => 1.0,
                                  p_init_msg_list => 'T',
                                  p_ext_payee_tab => l_external_payee_tab_type,
                                  p_ext_payee_id_tab => l_ext_payee_id_tab_type,
                                  x_return_status => x_return_status,
                                  x_msg_count => x_msg_count,
                                  x_msg_data => x_msg_data,
                                  x_ext_payee_status_tab => l_payee_upd_status
                                  );

                    IF x_return_status <> 'S'
                    THEN
                       IF x_msg_count > 0
                       THEN
                          FOR i IN 1 .. x_msg_count
                          LOOP
                             pou_error_code := 'ETN_AP_SUPSITE_PAYMTHD_ERR';
                             fnd_msg_pub.get (i, fnd_api.g_false, x_msg_data, t_msg_dummy);
                             --fnd_msg_pub.get (i, fnd_api.g_false, x_msg_data, t_msg_dummy);

                             t_output := (TO_CHAR (i) || ': ' || x_msg_data);
                          END LOOP;
                              pou_error_mssg := t_output;
                       END IF;
                       COMMIT;
                    END IF;

                     EXCEPTION
                        WHEN no_data_found
                        THEN
                           pou_error_code := 'ETN_AP_SUPSITE_PAYMTHD_ERR';
                           pou_error_mssg := 'Error Not able to Search Pay Method for new site';
                     END;
             END LOOP;

         END IF;


  EXCEPTION
    WHEN OTHERS
      THEN
        pou_error_code := 'ETN_AP_SUPP_SITE_CREATE_ERROR' ;
        pou_error_mssg := 'Error'||SUBSTR (SQLERRM, 1, 155);
  END;
  --changes for v1.3 end here
  --
  -- ========================
  -- Procedure: pre_validate
  -- =============================================================================
  --   This procedure pre_validate
  -- =============================================================================
  --  Input Parameters :
  --    None
  --  Output Parameters :
  --    None
  -- -----------------------------------------------------------------------------
  --
  PROCEDURE pre_validate IS
    l_lookup_exists NUMBER;
    l_payment_lkp  CONSTANT VARCHAR2(50) := 'XXAP_PAYTERM_MAPPING';
    l_tax_code_lkp CONSTANT VARCHAR2(50) := 'XXEBTAX_TAX_CODE_MAPPING';
  BEGIN
    l_lookup_exists := 0;
    xxetn_debug_pkg.add_debug('+   PROCEDURE : pre_validate +');
    xxetn_debug_pkg.add_debug('+ Checking Payment Term Lookup +');
    -- check whether the lookup ETN_PTP_PAYTERM_MAPPING exists
    l_lookup_exists := 0;

    BEGIN
      SELECT 1
        INTO l_lookup_exists
        FROM fnd_lookup_types flv
       WHERE flv.lookup_type = l_payment_lkp;
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        l_lookup_exists := 0;
        print_log_message(' In No Data found of Payment Terms lookup check' ||
                          SQLERRM);
      WHEN OTHERS THEN
        l_lookup_exists := 0;
        print_log_message(' In when others of Payment Terms lookup check' ||
                          SQLERRM);
    END;

    IF l_lookup_exists = 0 THEN
      g_retcode := 1;
      fnd_file.put_line(fnd_file.output,
                        'PAYMENT TERMS LOOKUP IS NOT SETUP');
    END IF;

    fnd_file.put_line(fnd_file.output, 'PAYMENT TERMS LOOKUP IS SETUP');
    xxetn_debug_pkg.add_debug('- Checking Payment Term Lookup -');
    l_lookup_exists := 0;
    xxetn_debug_pkg.add_debug('+ Checking Tax Code Lookup +');

    BEGIN
      SELECT 1
        INTO l_lookup_exists
        FROM fnd_lookup_types flv
       WHERE flv.lookup_type = l_tax_code_lkp;
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        l_lookup_exists := 0;
        print_log_message(' In No Data found of EBTAX TAX CODE CONV lookup check' ||
                          SQLERRM);
      WHEN OTHERS THEN
        l_lookup_exists := 0;
        print_log_message(' In when others of EBTAX TAX CODE CONV lookup check' ||
                          SQLERRM);
    END;

    IF l_lookup_exists = 0 THEN
      g_retcode := 1;
      fnd_file.put_line(fnd_file.output,
                        'EBTAX TAX CODE CONV  LOOKUP IS NOT SETUP');
    END IF;

    fnd_file.put_line(fnd_file.output,
                      'EBTAX TAX CODE CONV  LOOKUP IS SETUP');
    xxetn_debug_pkg.add_debug('- Checking Tax Code Lookup -');
    xxetn_debug_pkg.add_debug('+ Checking Operating Unit Lookup +');
    l_lookup_exists := 0;

    -- check whether the lookup ETN_COMMON_OU_MAP exists
    BEGIN
      SELECT 1
        INTO l_lookup_exists
        FROM fnd_lookup_types flv
       WHERE flv.lookup_type = g_ou_lkp;
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        l_lookup_exists := 0;
        print_log_message(' In No Data found of Common OU Lookup check' ||
                          SQLERRM);
      WHEN OTHERS THEN
        l_lookup_exists := 0;
        print_log_message(' In when others of Common OU lookup check' ||
                          SQLERRM);
    END;

    IF l_lookup_exists = 0 THEN
      g_retcode := 1;
      fnd_file.put_line(fnd_file.output, 'COMMON OU LOOKUP IS NOT SETUP');
    END IF;

    xxetn_debug_pkg.add_debug('- Checking Operating Unit Lookup -');
    fnd_file.put_line(fnd_file.output, 'COMMON OU LOOKUP IS SETUP');
    -- check whether Batch Source ?CONVERSION? is setup
    xxetn_debug_pkg.add_debug('+ Batch Source CONVERSION setup check +');

    BEGIN
      SELECT 1
        INTO l_lookup_exists
        FROM apps.ap_lookup_codes
       WHERE lookup_type = 'SOURCE'
         AND enabled_flag = 'Y'
         AND NVL(TRUNC(inactive_date), SYSDATE) >= TRUNC(SYSDATE)
         AND UPPER(TRIM(lookup_code)) = UPPER('CONVERSION');
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        l_lookup_exists := 0;
        print_log_message(' In No Data found of Batch Source check' ||
                          SQLERRM);
      WHEN OTHERS THEN
        l_lookup_exists := 0;
        print_log_message(' In when others of Batch Source check' ||
                          SQLERRM);
    END;

    IF l_lookup_exists = 0 THEN
      g_retcode := 1;
      fnd_file.put_line(fnd_file.output,
                        'Batch Source CONVERSION IS NOT SETUP in AP Lookup Codes');
    ELSE
      fnd_file.put_line(fnd_file.output,
                        'Batch Source CONVERSION IS SETUP in AP Lookup Codes');
    END IF;
    xxetn_debug_pkg.add_debug('- Batch Source CONVERSION setup check-');
    xxetn_debug_pkg.add_debug('-   PROCEDURE : pre_validate -');
  EXCEPTION
    WHEN OTHERS THEN
      g_retcode := 2;
      print_log_message('In Pre Validate when others' || SQLERRM);
  END pre_validate;

  --
  -- ========================
  -- Procedure: validate_mandatory_value
  -- =============================================================================
  --   This procedure to do mandatory value check
  -- =============================================================================
  --  Input Parameters :
  --   piv_invoice_num
  --   pin_invoice_amount
  --   piv_invoice_type
  --   piv_terms_name
  --   pid_invoice_Date
  --   piv_operating_unit
  --   piv_vendor_num
  --   piv_payment_method
  --   piv_venndor_site_code
  --   piv_currency_code
  --   piv_pay_acc_code_concat
  --
  --  Output Parameters :
  --   pon_error_cnt    : Return Error Count
  -- -----------------------------------------------------------------------------
  --
  PROCEDURE validate_mandatory_value(/*piv_intf_txn_id         IN NUMBER,  --v1.10
                                     piv_org_id              IN NUMBER, --v1.10
                                     piv_liab_ccid           IN NUMBER,*/  --v1.10
                                     piv_invoice_num         IN VARCHAR2,
                                     pin_invoice_amount      IN NUMBER,
                                     piv_invoice_type        IN VARCHAR2,
                                     piv_terms_name          IN VARCHAR2,
                                     pid_invoice_date        IN DATE,
                                     piv_operating_unit      IN VARCHAR2,
                                     piv_vendor_num          IN VARCHAR2,
                                     piv_payment_method      IN VARCHAR2,
                                     piv_venndor_site_code   IN VARCHAR2,
                                     piv_currency_code       IN VARCHAR2,
                                     piv_pay_acc_code_concat IN VARCHAR2,
                                     pon_error_cnt           OUT NUMBER) IS
    l_record_cnt     NUMBER;
    l_err_msg        VARCHAR2(2000);
    l_log_ret_status VARCHAR2(50);
    l_log_err_msg    VARCHAR2(2000);
    l_err_code       VARCHAR2(40) := NULL;
    --changes for v1.10

    /*l_suppl_err_msg  VARCHAR2(2000);
    l_suppl_err_code VARCHAR2(40) := NULL;
    l_site_count     NUMBER;
    l_ven_site_id    NUMBER;
    l_vendor_id      NUMBER;
    l_new_vensite_id NUMBER;*/
    --changes for v1.10 End Here
  BEGIN
    print_log_message('   PROCEDURE : validate_mandatory_value');
    l_record_cnt     := 0;
    l_err_msg        := NULL;
    l_log_ret_status := NULL;
    l_log_err_msg    := NULL;
    l_err_code       := NULL;

    IF piv_invoice_num IS NULL THEN
      l_record_cnt := 2;
      xxetn_debug_pkg.add_debug('Mandatory Value missing on record.');
      l_err_code := 'ETN_AP_MANDATORY_NOT_ENTERED';
      l_err_msg  := 'Error: Mandatory Value missing on record.';
      log_errors(pov_return_status       => l_log_ret_status -- OUT
                ,
                 pov_error_msg           => l_log_err_msg -- OUT
                ,
                 piv_source_column_name  => 'LEG_INVOICE_NUM',
                 piv_source_column_value => piv_invoice_num,
                 piv_error_type          => g_err_val,
                 piv_error_code          => l_err_code,
                 piv_error_message       => l_err_msg);
    END IF;

    IF pin_invoice_amount IS NULL THEN
      l_record_cnt := 2;
      xxetn_debug_pkg.add_debug('Mandatory Value missing on record.');
      l_err_code := 'ETN_AP_MANDATORY_NOT_ENTERED';
      l_err_msg  := 'Error: Mandatory Value missing on record.';
      log_errors(pov_return_status       => l_log_ret_status -- OUT
                ,
                 pov_error_msg           => l_log_err_msg -- OUT
                ,
                 piv_source_column_name  => 'LEG_INVOICE_AMOUNT',
                 piv_source_column_value => pin_invoice_amount,
                 piv_error_type          => g_err_val,
                 piv_error_code          => l_err_code,
                 piv_error_message       => l_err_msg);
    END IF;

    --Mandatory Column check
    IF piv_invoice_type IS NULL THEN
      l_record_cnt := 2;
      xxetn_debug_pkg.add_debug('Mandatory Value missing on record.');
      l_err_code := 'ETN_AP_MANDATORY_NOT_ENTERED';
      l_err_msg  := 'Error: Mandatory Value missing on record.';
      log_errors(pov_return_status       => l_log_ret_status -- OUT
                ,
                 pov_error_msg           => l_log_err_msg -- OUT
                ,
                 piv_source_column_name  => 'LEG_INV_TYPE_LOOKUP_CODE',
                 piv_source_column_value => piv_invoice_type,
                 piv_error_type          => g_err_val,
                 piv_error_code          => l_err_code,
                 piv_error_message       => l_err_msg);
    END IF;

    --Mandatory Column check
    IF piv_terms_name IS NULL THEN
      l_record_cnt := 2;
      xxetn_debug_pkg.add_debug('Mandatory Value missing on record.');
      l_err_code := 'ETN_AP_MANDATORY_NOT_ENTERED';
      l_err_msg  := 'Error: Mandatory Value missing on record.';
      log_errors(pov_return_status       => l_log_ret_status -- OUT
                ,
                 pov_error_msg           => l_log_err_msg -- OUT
                ,
                 piv_source_column_name  => 'LEG_TERMS_NAME',
                 piv_source_column_value => piv_terms_name,
                 piv_error_type          => g_err_val,
                 piv_error_code          => l_err_code,
                 piv_error_message       => l_err_msg);
    END IF;

    --Mandatory Column check
    IF pid_invoice_date IS NULL THEN
      l_record_cnt := 2;
      xxetn_debug_pkg.add_debug('Mandatory Value missing on record.');
      l_err_code := 'ETN_AP_MANDATORY_NOT_ENTERED';
      l_err_msg  := 'Error: Mandatory Value missing on record.';
      log_errors(pov_return_status       => l_log_ret_status -- OUT
                ,
                 pov_error_msg           => l_log_err_msg -- OUT
                ,
                 piv_source_column_name  => 'LEG_INVOICE_DATE',
                 piv_source_column_value => pid_invoice_date,
                 piv_error_type          => g_err_val,
                 piv_error_code          => l_err_code,
                 piv_error_message       => l_err_msg);
    END IF;

    --Mandatory Column check
    IF piv_operating_unit IS NULL THEN
      l_record_cnt := 2;
      xxetn_debug_pkg.add_debug('Mandatory Value missing on record.');
      l_err_code := 'ETN_AP_MANDATORY_NOT_ENTERED';
      l_err_msg  := 'Error: Mandatory Value missing on record.';
      log_errors(pov_return_status       => l_log_ret_status -- OUT
                ,
                 pov_error_msg           => l_log_err_msg -- OUT
                ,
                 piv_source_column_name  => 'LEG_OPERATING_UNIT',
                 piv_source_column_value => piv_operating_unit,
                 piv_error_type          => g_err_val,
                 piv_error_code          => l_err_code,
                 piv_error_message       => l_err_msg);
    END IF;

    --Mandatory Column check
    IF piv_vendor_num IS NULL THEN
      l_record_cnt := 2;
      xxetn_debug_pkg.add_debug('Mandatory Value missing on record.');
      l_err_code := 'ETN_AP_MANDATORY_NOT_ENTERED';
      l_err_msg  := 'Error: Mandatory Value missing on record.';
      log_errors(pov_return_status       => l_log_ret_status -- OUT
                ,
                 pov_error_msg           => l_log_err_msg -- OUT
                ,
                 piv_source_column_name  => 'LEG_VENDOR_NUM',
                 piv_source_column_value => piv_vendor_num,
                 piv_error_type          => g_err_val,
                 piv_error_code          => l_err_code,
                 piv_error_message       => l_err_msg);
    --v1.10 starts here
    /*ELSE

          validate_supplier(piv_vendor_num,l_suppl_err_code,l_suppl_err_msg);
           FND_FILE.PUT_LINE(FND_FILE.LOG,l_suppl_err_code||' '||l_suppl_err_msg);  --remove@@
          IF l_suppl_err_msg IS NOT NULL
          THEN
             l_record_cnt := 2;
             FND_FILE.PUT_LINE(FND_FILE.LOG,'In supplier error'||l_suppl_err_code||' '||l_suppl_err_msg);  --remove@@
             log_errors(pov_return_status       => l_log_ret_status -- OUT
                ,
                 pov_error_msg           => l_log_err_msg -- OUT
                ,
                 piv_source_column_name  => 'LEG_VENDOR_NUM',
                 piv_source_column_value => piv_vendor_num,
                 piv_error_type          => g_err_val,
                 piv_error_code          => l_suppl_err_code,
                 piv_error_message       => l_suppl_err_msg);
          ELSE
          IF piv_org_id IS NOT NULL
          THEN
             SELECT COUNT(1)
             INTO l_site_count
             FROM ap_supplier_sites_all apsa, ap_suppliers aps
             WHERE apsa.vendor_site_code = piv_venndor_site_code
             AND  org_id = piv_org_id
             AND apsa.vendor_id = aps.VENDOR_ID
             AND aps.vendor_name = piv_vendor_num;
           FND_FILE.PUT_LINE(FND_FILE.LOG,'after l_Site_count'||l_site_count);  --remove@@

             IF l_site_count = 0
             THEN
                BEGIN
                  FND_FILE.PUT_LINE(FND_FILE.LOG,'Checking for source site');
                   SELECT apsa.vendor_site_id, aps.vendor_id
                   INTO l_ven_site_id , l_vendor_id
                   FROM ap_supplier_sites_all apsa, ap_suppliers aps
                   WHERE apsa.vendor_site_code = piv_venndor_site_code
                   AND apsa.vendor_id = aps.VENDOR_ID
                   AND apsa.pay_site_flag = 'Y'
                   AND aps.segment1 = piv_vendor_num
                   AND ROWNUM<2;
                   FND_FILE.PUT_LINE(FND_FILE.LOG,'Calling site create process');
                   Create_new_site(l_vendor_id,
                                   l_ven_site_id ,
                                   piv_liab_ccid,
                                   piv_org_id,
                                   piv_venndor_site_code,
                                   l_new_vensite_id,
                                   l_suppl_err_code,
                                   l_suppl_err_msg);

                   --create_new_acct()
                   --create_new_contact()
                   --create_relation_Site_bank

                EXCEPTION
                   WHEN no_Data_found
                   THEN
                      l_record_cnt := 2;
                      l_suppl_err_code := 'ETN_AP_NO_SOURCE_SUPP_SITE_AVLBL';
                      l_suppl_err_msg  := 'Cannot Create New Site in the OU No Source Site Available';
                      log_errors(pov_return_status       => l_log_ret_status -- OUT
                      ,
                      pov_error_msg           => l_log_err_msg -- OUT
                      ,
                      piv_source_column_name  => 'VENDOR_NUM/VENDOR_SITE',
                      piv_source_column_value => piv_vendor_num||' / ' ||piv_venndor_site_code,
                      piv_error_type          => g_err_val,
                      piv_error_code          => l_suppl_err_code,
                      piv_error_message       => l_suppl_err_msg);
                END;

             END IF;
          ELSE
             l_record_cnt := 2;
             l_suppl_err_code := 'ETN_AP_INVALID_SUPPLIER_SITE_NO_ORG';
             l_suppl_err_msg  := 'Supplier site not fetched, Missing OU Info';
             log_errors(pov_return_status       => l_log_ret_status -- OUT
                ,
                 pov_error_msg           => l_log_err_msg -- OUT
                ,
                 piv_source_column_name  => 'VENDOR_NUM/VENDOR_SITE',
                 piv_source_column_value => piv_vendor_num||' / ' ||piv_venndor_site_code,
                 piv_error_type          => g_err_val,
                 piv_error_code          => l_suppl_err_code,
                 piv_error_message       => l_suppl_err_msg);
          END IF;
          END IF;*/
    --v1.10 change ends here
    END IF;

    --Mandatory Column check
    IF piv_payment_method IS NULL THEN
      l_record_cnt := 2;
      xxetn_debug_pkg.add_debug('Mandatory Value missing on record.');
      l_err_code := 'ETN_AP_MANDATORY_NOT_ENTERED';
      l_err_msg  := 'Error: Mandatory Value missing on record.';
      log_errors(pov_return_status       => l_log_ret_status -- OUT
                ,
                 pov_error_msg           => l_log_err_msg -- OUT
                ,
                 piv_source_column_name  => 'LEG_PAYMENT_METHOD_CODE', --TUT changes
                 piv_source_column_value => piv_payment_method,
                 piv_error_type          => g_err_val,
                 piv_error_code          => l_err_code,
                 piv_error_message       => l_err_msg);
    END IF;

    --Mandatory Column check
    IF piv_venndor_site_code IS NULL THEN
      l_record_cnt := 2;
      xxetn_debug_pkg.add_debug('Mandatory Value missing on record.');
      l_err_code := 'ETN_AP_MANDATORY_NOT_ENTERED';
      l_err_msg  := 'Error: Mandatory Value missing on record.';
      log_errors(pov_return_status       => l_log_ret_status -- OUT
                ,
                 pov_error_msg           => l_log_err_msg -- OUT
                ,
                 piv_source_column_name  => 'LEG_VENDOR_SITE_CODE',
                 piv_source_column_value => piv_venndor_site_code,
                 piv_error_type          => g_err_val,
                 piv_error_code          => l_err_code,
                 piv_error_message       => l_err_msg);
    END IF;

    --Mandatory Column check
    IF piv_currency_code IS NULL THEN
      l_record_cnt := 2;
      xxetn_debug_pkg.add_debug('Mandatory Value missing on record.');
      l_err_code := 'ETN_AP_MANDATORY_NOT_ENTERED';
      l_err_msg  := 'Error: Mandatory Value missing on record.';
      log_errors(pov_return_status       => l_log_ret_status -- OUT
                ,
                 pov_error_msg           => l_log_err_msg -- OUT
                ,
                 piv_source_column_name  => 'LEG_INV_CURRENCY_CODE',
                 piv_source_column_value => piv_currency_code,
                 piv_error_type          => g_err_val,
                 piv_error_code          => l_err_code,
                 piv_error_message       => l_err_msg);
    END IF;

    /*--Mandatory Column check
    IF  piv_pay_acc_code_concat IS NULL
    THEN
       l_record_cnt := 2;
       xxetn_debug_pkg.add_debug ( 'Mandatory Value missing on record.');
       l_err_code        := 'ETN_AP_MANDATORY_NOT_ENTERED';
       l_err_msg         := 'Error: Mandatory Value missing on record.';

       log_errors ( pov_return_status          =>   l_log_ret_status          -- OUT
                  , pov_error_msg              =>   l_log_err_msg             -- OUT
                  , piv_source_column_name     =>   'LEG_ACCTS_PAY_CODE_CONCAT'
                  , piv_source_column_value    =>   piv_pay_acc_code_concat
                  , piv_error_type             =>   g_err_val
                  , piv_error_code             =>   l_err_code
                  , piv_error_message          =>   l_err_msg
                  );
    END IF;*/
    IF l_record_cnt > 1 THEN
      pon_error_cnt := 2;
    END IF;
  EXCEPTION
    WHEN OTHERS THEN
      g_retcode     := 2;
      pon_error_cnt := 2;
      print_log_message('In Exception validate_mandatory_value check' ||
                        SQLERRM);
  END validate_mandatory_value;

  --
  -- ========================
  -- Procedure: validate_line_mandatory_value
  -- =============================================================================
  --   This procedure to do mandatory value check for invoice lines
  -- =============================================================================
  --  Input Parameters :
  --   piv_line_type
  --   pin_line_number
  --   pin_amount
  --   piv_dist_code_concatenated
  --   pid_accounting_Date
  --   piv_project_name
  --   piv_task_name
  --   pid_expenditure_item_date
  --   piv_expenditure_type
  --   piv_expenditure_org_name
  --   piv_pa_addition_flag
  --   piv_tax_code
  --  Output Parameters :
  --   pon_error_cnt    : Return Error Count
  -- -----------------------------------------------------------------------------
  --
  PROCEDURE validate_line_mandatory_value(piv_line_type              IN VARCHAR2,
                                          pin_line_number            IN NUMBER,
                                          pin_amount                 IN NUMBER,
                                          piv_dist_code_concatenated IN VARCHAR2,
                                          pid_accounting_date        IN DATE,
                                          piv_project_name           IN VARCHAR2,
                                          piv_task_name              IN VARCHAR2,
                                          pid_expenditure_item_date  IN DATE,
                                          piv_expenditure_type       IN VARCHAR2,
                                          piv_expenditure_org_name   IN VARCHAR2,
                                          piv_pa_addition_flag       IN VARCHAR2,
                                          piv_tax_code               IN VARCHAR2,
                                          piv_vendor_num             IN VARCHAR2,
                                          pon_error_cnt              OUT NUMBER) IS
    l_record_cnt     NUMBER;
    l_err_msg        VARCHAR2(2000);
    l_log_ret_status VARCHAR2(50);
    l_log_err_msg    VARCHAR2(2000);
    l_err_code       VARCHAR2(40) := NULL;
  BEGIN
    print_log_message('   PROCEDURE : validate_mandatory_value');
    l_record_cnt     := 0;
    l_err_msg        := NULL;
    l_log_ret_status := NULL;
    l_log_err_msg    := NULL;
    l_err_code       := NULL;

    IF piv_line_type IS NULL THEN
      l_record_cnt := 2;
      xxetn_debug_pkg.add_debug('Mandatory Value missing on record.');
      l_err_code := 'ETN_AP_MANDATORY_NOT_ENTERED';
      l_err_msg  := 'Error: Mandatory Value missing on record.';
      log_errors(pov_return_status       => l_log_ret_status -- OUT
                ,
                 pov_error_msg           => l_log_err_msg -- OUT
                ,
                 piv_source_column_name  => 'LEG_LINE_TYPE_LOOKUP_CODE',
                 piv_source_column_value => piv_line_type,
                 piv_error_type          => g_err_val,
                 piv_error_code          => l_err_code,
                 piv_error_message       => l_err_msg);
    END IF;

    IF pin_line_number IS NULL THEN
      l_record_cnt := 2;
      xxetn_debug_pkg.add_debug('Mandatory Value missing on record.');
      l_err_code := 'ETN_AP_MANDATORY_NOT_ENTERED';
      l_err_msg  := 'Error: Mandatory Value missing on record.';
      log_errors(pov_return_status       => l_log_ret_status -- OUT
                ,
                 pov_error_msg           => l_log_err_msg -- OUT
                ,
                 piv_source_column_name  => 'LEG_LINE_NUMBER',
                 piv_source_column_value => pin_line_number,
                 piv_error_type          => g_err_val,
                 piv_error_code          => l_err_code,
                 piv_error_message       => l_err_msg);
    END IF;

    --Mandatory Column check
    IF pin_amount IS NULL THEN
      l_record_cnt := 2;
      xxetn_debug_pkg.add_debug('Mandatory Value missing on record.');
      l_err_code := 'ETN_AP_MANDATORY_NOT_ENTERED';
      l_err_msg  := 'Error: Mandatory Value missing on record.';
      log_errors(pov_return_status       => l_log_ret_status -- OUT
                ,
                 pov_error_msg           => l_log_err_msg -- OUT
                ,
                 piv_source_column_name  => 'LEG_AMOUNT',
                 piv_source_column_value => pin_amount,
                 piv_error_type          => g_err_val,
                 piv_error_code          => l_err_code,
                 piv_error_message       => l_err_msg);
    END IF;

    /*--Mandatory Column check
    IF  piv_dist_code_concatenated IS NULL
    THEN
       l_record_cnt := 2;
       xxetn_debug_pkg.add_debug ( 'Mandatory Value missing on record.');
       l_err_code        := 'ETN_AP_MANDATORY_NOT_ENTERED';
       l_err_msg         := 'Error: Mandatory Value missing on record.';

       log_errors ( pov_return_status          =>   l_log_ret_status          -- OUT
                  , pov_error_msg              =>   l_log_err_msg             -- OUT
                  , piv_source_column_name     =>   'DIST_CODE_CONCATENATED'
                  , piv_source_column_value    =>   piv_dist_code_concatenated
                  , piv_error_type             =>   g_err_val
                  , piv_error_code             =>   l_err_code
                  , piv_error_message          =>   l_err_msg
                  );
    END IF;*/

    --Mandatory Column check
    IF pid_accounting_date IS NULL THEN
      l_record_cnt := 2;
      xxetn_debug_pkg.add_debug('Mandatory Value missing on record.');
      l_err_code := 'ETN_AP_MANDATORY_NOT_ENTERED';
      l_err_msg  := 'Error: Mandatory Value missing on record.';
      log_errors(pov_return_status       => l_log_ret_status -- OUT
                ,
                 pov_error_msg           => l_log_err_msg -- OUT
                ,
                 piv_source_column_name  => 'LEG_ACCOUNTING_DATE',
                 piv_source_column_value => pid_accounting_date,
                 piv_error_type          => g_err_val,
                 piv_error_code          => l_err_code,
                 piv_error_message       => l_err_msg);
    END IF;

    --Mandatory Column check for project related invoices
    IF piv_project_name IS NOT NULL THEN
      --Mandatory Column check
      IF piv_task_name IS NULL THEN
        l_record_cnt := 2;
        xxetn_debug_pkg.add_debug('Mandatory Value missing on record.');
        l_err_code := 'ETN_AP_MANDATORY_NOT_ENTERED';
        l_err_msg  := 'Error: Mandatory Value missing on record.';
        log_errors(pov_return_status       => l_log_ret_status -- OUT
                  ,
                   pov_error_msg           => l_log_err_msg -- OUT
                  ,
                   piv_source_column_name  => 'LEG_TASK_NAME',
                   piv_source_column_value => piv_task_name,
                   piv_error_type          => g_err_val,
                   piv_error_code          => l_err_code,
                   piv_error_message       => l_err_msg);
      END IF;

      --Mandatory Column check
      IF pid_expenditure_item_date IS NULL THEN
        l_record_cnt := 2;
        xxetn_debug_pkg.add_debug('Mandatory Value missing on record.');
        l_err_code := 'ETN_AP_MANDATORY_NOT_ENTERED';
        l_err_msg  := 'Error: Mandatory Value missing on record.';
        log_errors(pov_return_status       => l_log_ret_status -- OUT
                  ,
                   pov_error_msg           => l_log_err_msg -- OUT
                  ,
                   piv_source_column_name  => 'EXPENDITURE_ITEM_DATE',
                   piv_source_column_value => pid_expenditure_item_date,
                   piv_error_type          => g_err_val,
                   piv_error_code          => l_err_code,
                   piv_error_message       => l_err_msg);
      END IF;

      --Mandatory Column check
      IF piv_expenditure_type IS NULL THEN
        l_record_cnt := 2;
        xxetn_debug_pkg.add_debug('Mandatory Value missing on record.');
        l_err_code := 'ETN_AP_MANDATORY_NOT_ENTERED';
        l_err_msg  := 'Error: Mandatory Value missing on record.';
        log_errors(pov_return_status       => l_log_ret_status -- OUT
                  ,
                   pov_error_msg           => l_log_err_msg -- OUT
                  ,
                   piv_source_column_name  => 'EXPENDITURE_TYPE',
                   piv_source_column_value => piv_expenditure_type,
                   piv_error_type          => g_err_val,
                   piv_error_code          => l_err_code,
                   piv_error_message       => l_err_msg);
      END IF;

      --Mandatory Column check
      IF piv_expenditure_org_name IS NULL THEN
        l_record_cnt := 2;
        xxetn_debug_pkg.add_debug('Mandatory Value missing on record.');
        l_err_code := 'ETN_AP_MANDATORY_NOT_ENTERED';
        l_err_msg  := 'Error: Mandatory Value missing on record.';
        log_errors(pov_return_status       => l_log_ret_status -- OUT
                  ,
                   pov_error_msg           => l_log_err_msg -- OUT
                  ,
                   piv_source_column_name  => 'LEG_EXPENDITURE_ORG_NAME',
                   piv_source_column_value => piv_expenditure_org_name,
                   piv_error_type          => g_err_val,
                   piv_error_code          => l_err_code,
                   piv_error_message       => l_err_msg);
      END IF;

      --Mandatory Column check
      IF piv_pa_addition_flag IS NULL THEN
        l_record_cnt := 2;
        xxetn_debug_pkg.add_debug('Mandatory Value missing on record.');
        l_err_code := 'ETN_AP_MANDATORY_NOT_ENTERED';
        l_err_msg  := 'Error: Mandatory Value missing on record.';
        log_errors(pov_return_status       => l_log_ret_status -- OUT
                  ,
                   pov_error_msg           => l_log_err_msg -- OUT
                  ,
                   piv_source_column_name  => 'LEG_PA_ADDITION_FLAG',
                   piv_source_column_value => piv_pa_addition_flag,
                   piv_error_type          => g_err_val,
                   piv_error_code          => l_err_code,
                   piv_error_message       => l_err_msg);
      END IF;
    END IF;

    -- if line type is TAX
    IF (UPPER(piv_line_type) = 'TAX') THEN
      --Mandatory Column check
      IF piv_tax_code IS NULL THEN
        l_record_cnt := 2;
        xxetn_debug_pkg.add_debug('Mandatory Value missing on record.');
        l_err_code := 'ETN_AP_MANDATORY_NOT_ENTERED';
        l_err_msg  := 'Error: Mandatory Value missing on record.';
        log_errors(pov_return_status       => l_log_ret_status -- OUT
                  ,
                   pov_error_msg           => l_log_err_msg -- OUT
                  ,
                   piv_source_column_name  => 'LEG_TAX_CODE',
                   piv_source_column_value => piv_tax_code,
                   piv_error_type          => g_err_val,
                   piv_error_code          => l_err_code,
                   piv_error_message       => l_err_msg);
      END IF;
    END IF;

    --ver1.4 changes start
    IF piv_vendor_num IS NULL THEN
      l_record_cnt := 2;
      xxetn_debug_pkg.add_debug('Mandatory Value missing on record.');
      l_err_code := 'ETN_AP_MANDATORY_NOT_ENTERED';
      l_err_msg  := 'Error: Mandatory Value missing on record at line.';
      log_errors(pov_return_status       => l_log_ret_status -- OUT
                ,
                 pov_error_msg           => l_log_err_msg -- OUT
                ,
                 piv_source_column_name  => 'LEG_VENDOR_NUM',
                 piv_source_column_value => piv_vendor_num,
                 piv_error_type          => g_err_val,
                 piv_error_code          => l_err_code,
                 piv_error_message       => l_err_msg);
    END IF;
    --ver1.4 changes end

    IF l_record_cnt > 1 THEN
      pon_error_cnt := 2;
    END IF;
  EXCEPTION
    WHEN OTHERS THEN
      g_retcode     := 2;
      pon_error_cnt := 2;
      print_log_message('In Exception validate_line_mandatory_value check' ||
                        SQLERRM);
  END validate_line_mandatory_value;

  --
  -- ========================
  -- Procedure: duplicate_check
  -- =============================================================================
  --   This procedure to do duplicate invoice record check
  -- =============================================================================
  --  Input Parameters :
  --   piv_bank_name
  --   piv_bank_number
  --   piv_country

  --  Output Parameters :
  --   pon_error_cnt    : Return Error Count
  -- -----------------------------------------------------------------------------
  --
  PROCEDURE duplicate_check(piv_invoice_num    IN VARCHAR2,
                            piv_operating_unit IN VARCHAR2,
                            piv_vendor_num     IN VARCHAR2,
                            pon_error_cnt      OUT NUMBER) IS
    l_record_cnt     NUMBER;
    l_err_msg        VARCHAR2(2000);
    l_log_ret_status VARCHAR2(50);
    l_log_err_msg    VARCHAR2(2000);
    l_err_code       VARCHAR2(40) := NULL;
  BEGIN
    print_log_message(' + PROCEDURE : duplicate_check +');
    l_record_cnt     := 0;
    l_err_msg        := NULL;
    l_log_ret_status := NULL;
    l_log_err_msg    := NULL;
    l_err_code       := NULL;

    --check if the duplicate invoice already exists
    BEGIN
      SELECT COUNT(1)
        INTO l_record_cnt
        FROM xxap_invoices_stg xis
       WHERE xis.leg_invoice_num = piv_invoice_num
         AND xis.leg_operating_unit = piv_operating_unit
         AND xis.leg_vendor_num = piv_vendor_num
         AND xis.batch_id = g_new_batch_id
         AND xis.run_sequence_id = g_run_seq_id;
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        print_log_message('In No Data found of duplicate_check for invoice' ||
                          SQLERRM);
      WHEN OTHERS THEN
        l_record_cnt := 2;
        print_log_message('In When others of duplicate_check for invoice' ||
                          SQLERRM);
    END;

    IF (l_record_cnt > 1) THEN
      l_record_cnt := 2;
    END IF;

    IF l_record_cnt = 2 THEN
      pon_error_cnt := 2;
    END IF;

    print_log_message(' - PROCEDURE : duplicate_check -');
  EXCEPTION
    WHEN OTHERS THEN
      g_retcode     := 2;
      pon_error_cnt := 2;
      print_log_message('In Exception duplicate_check' || SQLERRM);
  END duplicate_check;

  --
  -- ========================
  -- Procedure: duplicate_line_check
  -- =============================================================================
  --   This procedure to do duplicate invoice line record check
  -- =============================================================================
  --  Input Parameters :
  --   pin_invoice_id
  --   pin_line_number
  --   piv_operating_unit
  --   piv_vendor_num

  --  Output Parameters :
  --   pon_error_cnt    : Return Error Count
  -- -----------------------------------------------------------------------------
  --
  PROCEDURE duplicate_line_check(piv_invoice_num    IN VARCHAR2,
                                 pin_line_number    IN NUMBER,
                                 piv_operating_unit IN VARCHAR2,
                                 piv_vendor_num     IN VARCHAR2,
                                 pon_error_cnt      OUT NUMBER) IS
    l_record_cnt     NUMBER;
    l_err_msg        VARCHAR2(2000);
    l_log_ret_status VARCHAR2(50);
    l_log_err_msg    VARCHAR2(2000);
    l_err_code       VARCHAR2(40) := NULL;
  BEGIN
    print_log_message(' + PROCEDURE : duplicate_line_check +');
    l_record_cnt     := 0;
    l_err_msg        := NULL;
    l_log_ret_status := NULL;
    l_log_err_msg    := NULL;
    l_err_code       := NULL;

    --check if the duplicate invoice line already exists
    BEGIN
      SELECT COUNT(1)
        INTO l_record_cnt
        FROM xxap_invoice_lines_stg xils
       WHERE xils.leg_invoice_num = piv_invoice_num
         AND xils.leg_line_number = pin_line_number
         AND xils.leg_operating_unit = piv_operating_unit
         AND xils.leg_vendor_num = piv_vendor_num
         AND xils.batch_id = g_new_batch_id
         AND xils.run_sequence_id = g_run_seq_id;
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        print_log_message('In No Data found of duplicate_line_check for invoice line' ||
                          SQLERRM);
      WHEN OTHERS THEN
        l_record_cnt := 2;
        print_log_message('In When others of duplicate_line_check for invoice line' ||
                          SQLERRM);
    END;

    IF (l_record_cnt > 1) THEN
      pon_error_cnt := 2;
    END IF;

    print_log_message(' - PROCEDURE : duplicate_line_check -');
  EXCEPTION
    WHEN OTHERS THEN
      g_retcode     := 2;
      pon_error_cnt := 2;
      print_log_message('In Exception duplicate_line_check' || SQLERRM);
  END duplicate_line_check;

  --
  -- ========================
  -- Procedure: validate_operating_unit
  -- =============================================================================
  --   This procedure validate_operating_unit
  -- =============================================================================
  --  Input Parameters :
  --   piv_operating_unit - 11i operating unit name

  --  Output Parameters :
  --  pov_operating_unit - R12 operating unit name
  --  pon_org_id - R12 organization id
  --  pon_error_cnt    : Return Status
  -- -----------------------------------------------------------------------------
  --
  PROCEDURE validate_operating_unit(piv_operating_unit IN VARCHAR2,
                                    pov_operating_unit OUT VARCHAR2,
                                    pon_org_id         OUT NUMBER,
                                    pon_sob_id         OUT NUMBER,
                                    pon_error_cnt      OUT NUMBER) IS
    l_record_cnt     NUMBER;
    l_operating_unit hr_operating_units.NAME%TYPE;
    l_org_id         hr_operating_units.organization_id%TYPE;
    l_sob_id         hr_operating_units.set_of_books_id%TYPE;
    g_ou_lkp CONSTANT VARCHAR2(50) := 'ETN_COMMON_OU_MAP';
    --Ver 1.2 changes start
    l_rec xxetn_map_util.g_input_rec;
    --Ver 1.2 changes end
  BEGIN
    xxetn_debug_pkg.add_debug(' + PROCEDURE : validate_operating_unit = ' ||
                              piv_operating_unit || ' + ');
    l_record_cnt     := 0;
    l_operating_unit := NULL;
    l_org_id         := NULL;

    --Ver1.2 changes start
    --/*
    BEGIN
      --Derive R12 value for the given operating unit
      SELECT description
        INTO l_operating_unit
        FROM fnd_lookup_values flv
       WHERE flv.lookup_type = g_ou_lkp
         AND flv.meaning = piv_operating_unit
         AND flv.LANGUAGE = USERENV('LANG')
         AND TRUNC(SYSDATE) BETWEEN
             NVL(flv.start_date_active, TRUNC(SYSDATE)) AND
             NVL(flv.end_date_active, TRUNC(SYSDATE));
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        l_record_cnt     := 2;
        l_operating_unit := NULL;
        print_log_message('In No Data found of operating unit lookup check' ||
                          SQLERRM);
      WHEN OTHERS THEN
        l_record_cnt     := 2;
        l_operating_unit := NULL;
        print_log_message('In When others of operating unit lookup check' ||
                          SQLERRM);
    END;

    --

    --      l_rec.site    := piv_operating_unit;
    --      l_operating_unit  := xxetn_map_util.get_value (l_rec).operating_unit;
    --Ver 1.2 changes end
    --if operating_unit is not null
    IF l_operating_unit IS NOT NULL THEN
      BEGIN
        xxetn_debug_pkg.add_debug(' + PROCEDURE : validate_operating_unit...derivation of org_id + ');

        --Fetch org_id  for the R12 value of the operating unit derived
        SELECT hou.organization_id, hou.set_of_books_id
          INTO l_org_id, l_sob_id
          FROM hr_operating_units hou
         WHERE hou.NAME = l_operating_unit
           AND TRUNC(SYSDATE) BETWEEN NVL(hou.date_from, TRUNC(SYSDATE)) AND
               NVL(hou.date_to, TRUNC(SYSDATE));

        xxetn_debug_pkg.add_debug(' + PROCEDURE : validate_operating_unit...derivation of org_id + ');
      EXCEPTION
        WHEN NO_DATA_FOUND THEN
          l_record_cnt := 2;
          l_org_id     := NULL;
          print_log_message('In No Data found of operating unit check' ||
                            SQLERRM);
        WHEN OTHERS THEN
          l_record_cnt := 2;
          l_org_id     := NULL;
          print_log_message('In When others of operating unit check' ||
                            SQLERRM);
      END;
    END IF;

    xxetn_debug_pkg.add_debug('Operating Unit = ' || l_operating_unit);
    xxetn_debug_pkg.add_debug('Org Id = ' || l_org_id);
    xxetn_debug_pkg.add_debug('SOB Id = ' || l_sob_id);
    pon_org_id         := l_org_id;
    pov_operating_unit := l_operating_unit;
    pon_sob_id         := l_sob_id;

    IF l_record_cnt > 1 THEN
      pon_error_cnt := 2;
    END IF;

    xxetn_debug_pkg.add_debug(' - PROCEDURE : validate_operating_unit = ' ||
                              piv_operating_unit || ' - ');
  EXCEPTION
    WHEN OTHERS THEN
      g_retcode     := 2;
      pon_error_cnt := 2;
      g_errbuff     := 'Failed while validating operating unit';
      print_log_message('In Exception Opertaing Unit Validation' ||
                        SQLERRM);
  END validate_operating_unit;

  --Ver1.3 changes start
  /*

     --
  -- ========================
  -- Procedure: validate_payment_terms
  -- =============================================================================
  --   This procedure validate_payment_terms
  -- =============================================================================
  --  Input Parameters :
  --   piv_terms_name - 11i payment term name

     --  Output Parameters :
  --  pov_terms_name - R12 payment term name
  --  pon_error_cnt    : Return Status
  -- -----------------------------------------------------------------------------
  --
     PROCEDURE validate_payment_terms (
        piv_payment_term   IN       VARCHAR2,
        pov_term_name      OUT      VARCHAR2,
        pon_error_cnt      OUT      NUMBER
     )
     IS
        l_record_cnt                  NUMBER;
        l_count                       NUMBER;
        l_payment_terms               VARCHAR2 (50);
        l_payment_term_lkp   CONSTANT VARCHAR2 (50) := 'XXAP_PAYTERM_MAPPING';     --'ETN_PTP_PAYTERM_MAPPING';
     BEGIN
        xxetn_debug_pkg.add_debug (' + PROCEDURE : validate_payment_terms = ' || piv_payment_term || ' + ');
        l_record_cnt               := 0;
        l_payment_terms            := NULL;
        l_count                    := NULL;

        BEGIN
           --Derive R12 value for the given payment term
           SELECT TRIM (flv.description)
             INTO l_payment_terms
             FROM fnd_lookup_values flv
            WHERE TRIM (UPPER (flv.meaning)) = TRIM (UPPER (piv_payment_term))
              AND flv.LANGUAGE = USERENV ('LANG')
              AND flv.enabled_flag = 'Y'
              AND UPPER (flv.lookup_type) = l_payment_term_lkp
              AND TRUNC (SYSDATE) BETWEEN TRUNC (NVL (flv.start_date_active, SYSDATE))
                                      AND TRUNC (NVL (flv.end_date_active, SYSDATE));
        EXCEPTION
           WHEN NO_DATA_FOUND
           THEN
              l_record_cnt               := 2;
              l_payment_terms            := NULL;
              print_log_message ('In No Data found of payment terms lookup check' || SQLERRM);
           WHEN OTHERS
           THEN
              l_record_cnt               := 2;
              l_payment_terms            := NULL;
              print_log_message ('In When others of payment terms lookup check' || SQLERRM);
        END;

        --if terms_name is not null
        IF l_payment_terms IS NOT NULL
        THEN
           BEGIN
              xxetn_debug_pkg.add_debug (' + PROCEDURE : validate_payment_terms...checking R12 term name + ');

              --Check whether the derived terms_name exist
              SELECT COUNT (1)
                INTO l_count
                FROM ap_terms apt
               WHERE NAME = l_payment_terms
                 AND enabled_flag = 'Y'
                 AND TRUNC (SYSDATE) BETWEEN TRUNC (NVL (apt.start_date_active, SYSDATE))
                                         AND TRUNC (NVL (apt.end_date_active, SYSDATE));

              xxetn_debug_pkg.add_debug (' - PROCEDURE : validate_payment_terms..checking R12 term name- ');
           EXCEPTION
              WHEN NO_DATA_FOUND
              THEN
                 l_record_cnt               := 2;
                 print_log_message ('In No Data found of R12 payment term check' || SQLERRM);
              WHEN OTHERS
              THEN
                 l_record_cnt               := 2;
                 print_log_message ('In When others of R12 payment term check' || SQLERRM);
           END;

           IF (l_count = 0)
           THEN
              l_record_cnt               := 2;
           END IF;
        END IF;

        xxetn_debug_pkg.add_debug ('Payment Term = ' || l_payment_terms);
        pov_term_name              := l_payment_terms;

        IF l_record_cnt > 1
        THEN
           pon_error_cnt              := 2;
        END IF;

        xxetn_debug_pkg.add_debug (' - PROCEDURE : validate_payment_terms = ' || piv_payment_term || ' - ');
     EXCEPTION
        WHEN OTHERS
        THEN
           g_retcode                  := 2;
           pov_term_name              := NULL;
           pon_error_cnt              := 2;
           g_errbuff                  := 'Failed while validating Payment Terms';
           print_log_message ('In Exception Payment Terms Validation' || SQLERRM);
     END validate_payment_terms;

  --
  -- ========================
  -- Procedure: validate_vendor
  -- =============================================================================
  --   This procedure validate_vendor
  -- =============================================================================
  --  Input Parameters :
  --  piv_bank_name: Leg bank name
  --  piv_country  : leg country

     --  Output Parameters :
  --  pon_error_cnt    : Return Error Count
  --  pon_vendor _id
  -- -----------------------------------------------------------------------------
  --
     PROCEDURE validate_supplier_details (
        piv_vendor_num         IN       VARCHAR2,
        piv_vendor_site_code   IN       VARCHAR2,
        pin_org_id             IN       NUMBER,
        pon_vendor_id          OUT      NUMBER,
        pov_vendor_name        OUT      VARCHAR2,
        pon_vendor_site_id     OUT      NUMBER,
        pon_error_cnt          OUT      NUMBER
     )
     IS
        l_record_cnt                  NUMBER;
        l_vendor_id                   NUMBER;
     BEGIN
        xxetn_debug_pkg.add_debug (' +  PROCEDURE : validate_supplier_details  ' || piv_vendor_num || ' + ');
        l_record_cnt               := 0;

        BEGIN
           --validate vendor and derive vendor ID
           SELECT DISTINCT apsa.vendor_id,
                           aps.vendor_name
                      INTO pon_vendor_id,
                           pov_vendor_name
                      FROM apps.ap_supplier_sites_all apsa, apps.ap_suppliers aps
                     WHERE UPPER (aps.segment1) = UPPER (piv_vendor_num)
                       AND apsa.vendor_id = aps.vendor_id
                       AND apsa.org_id = pin_org_id
                       AND NVL (apsa.inactive_date, SYSDATE) >= SYSDATE
                       AND NVL (aps.end_date_active, SYSDATE) >= SYSDATE;
        EXCEPTION
           WHEN NO_DATA_FOUND
           THEN
              l_record_cnt               := 2;
              print_log_message ('In No Data found of validate supplier check' || SQLERRM);
           WHEN OTHERS
           THEN
              l_record_cnt               := 2;
              print_log_message ('In When others of validate supplier check' || SQLERRM);
        END;

        IF pon_vendor_id IS NOT NULL
        THEN
           -- Deriving vendor_Site_code for the vendor id derived above
           BEGIN
              SELECT apsa.vendor_site_id
                INTO pon_vendor_site_id
                FROM apps.ap_supplier_sites_all apsa, apps.ap_suppliers aps
               WHERE aps.vendor_id = pon_vendor_id
                 AND apsa.vendor_id = aps.vendor_id
                 AND apsa.vendor_site_code = piv_vendor_site_code
                 AND NVL (apsa.inactive_date, SYSDATE) >= SYSDATE
                 AND NVL (aps.end_date_active, SYSDATE) >= SYSDATE
                 AND apsa.org_id = pin_org_id;
           EXCEPTION
              WHEN NO_DATA_FOUND
              THEN
                 l_record_cnt               := 2;
                 print_log_message ('In No Data found of validate vendor site check' || SQLERRM);
              WHEN OTHERS
              THEN
                 l_record_cnt               := 2;
                 print_log_message ('In When others of validate vendor site check' || SQLERRM);
           END;

           IF l_record_cnt = 2
           THEN
              pon_error_cnt              := 2;
           END IF;
        END IF;

        print_log_message (' -  PROCEDURE : validate_supplier_details   - ');
     EXCEPTION
        WHEN OTHERS
        THEN
           g_retcode                  := 2;
           pon_error_cnt              := 2;
           g_errbuff                  := 'Failed while validate_supplier_details.';
           print_log_message ('In Exception validate_supplier_details' || SQLERRM);
     END validate_supplier_details;

     --
  -- ========================
  -- Procedure: validate_gl_period
  -- =============================================================================
  --   This procedure validate_gl_period
  -- =============================================================================
  --  Input Parameters :
  --  pin_sob_id  : Set of books ID

     --  Output Parameters :
  --  pon_error_cnt    : Return Status
  -- -----------------------------------------------------------------------------
  --
     PROCEDURE validate_gl_period (pin_sob_id IN NUMBER, pon_error_cnt OUT NUMBER)
     IS
        l_record_cnt                  NUMBER;
     BEGIN
        xxetn_debug_pkg.add_debug (' +  PROCEDURE : validate_gl_period  ' || g_gl_date || ' + ');
        l_record_cnt               := 0;

        BEGIN
           --check if the GL period is open for SQLGL
           SELECT 1
             INTO l_record_cnt
             FROM gl_period_statuses gps, fnd_application fa, gl_ledgers gl
            WHERE gl.accounted_period_type = gps.period_type
              AND gl.ledger_id = gps.ledger_id
              AND fa.application_short_name = 'SQLGL'
              AND fa.application_id = gps.application_id
              AND gps.set_of_books_id = pin_sob_id
              AND gps.closing_status = 'O'
              AND g_gl_date BETWEEN gps.start_date AND gps.end_date;
        EXCEPTION
           WHEN NO_DATA_FOUND
           THEN
              l_record_cnt               := 2;
              print_log_message ('In No Data found of gl period check' || SQLERRM);
           WHEN OTHERS
           THEN
              l_record_cnt               := 2;
              print_log_message ('In When others of gl period check' || SQLERRM);
        END;

        IF l_record_cnt = 2
        THEN
           pon_error_cnt              := 2;
        END IF;

        BEGIN
            --Check if the GL period is open for AP
            SELECT 1
              INTO l_record_cnt
              FROM gl_period_statuses gps
                 , fnd_application fa
                 , gl_ledgers gl
             WHERE gl.accounted_period_type  = gps.period_type
               AND gl.ledger_id              = gps.ledger_id
               AND fa.application_short_name = 'SQLAP'
               AND fa.application_id         = gps.application_id
               AND gps.set_of_books_id       = pin_sob_id
               AND gps.closing_status        = 'O'
               AND g_gl_date          BETWEEN gps.start_date AND gps.end_date;

          EXCEPTION
             WHEN NO_DATA_FOUND THEN
               l_record_cnt := 3;
               print_log_message ( 'In No Data found of AP period check'||SQLERRM);
             WHEN OTHERS THEN
               l_record_cnt := 3;
               print_log_message ( 'In When others of AP period check'||SQLERRM);
          END;
        IF l_record_cnt = 3
        THEN
           pon_error_cnt              := 3;
        END IF;

        print_log_message (' -  PROCEDURE : validate_gl_period  ' || g_gl_date || ' - ');
     EXCEPTION
        WHEN OTHERS
        THEN
           g_retcode                  := 2;
           pon_error_cnt              := 2;
           g_errbuff                  := 'Failed while validating GL Period.';
           print_log_message ('In Exception validate gl period' || SQLERRM);
     END validate_gl_period;

  --
  -- ========================
  -- Procedure: validate_payment_mtd
  -- =============================================================================
  --   This procedure validate_payment_mtd
  -- =============================================================================
  --  Input Parameters :
  --   piv_payment_method -11i payment method name

     --  Output Parameters :
  --  pov_payment_method - R12 payment method name
  --  pon_error_cnt    : Return Status
  -- -----------------------------------------------------------------------------
  --
     PROCEDURE validate_payment_mtd (
        piv_payment_method   IN       VARCHAR2,
        pov_payment_method   OUT      VARCHAR2,
        pon_error_cnt        OUT      NUMBER
     )
     IS
        l_record_cnt                  NUMBER;
        l_payment_method              VARCHAR2 (50);
        l_payment_lkp        CONSTANT VARCHAR2 (50) := 'PAYMENT METHOD';
     BEGIN
        xxetn_debug_pkg.add_debug (' +  PROCEDURE : validate_payment_mtd = ' || piv_payment_method || ' + ');
        l_record_cnt               := 0;
        l_payment_method           := NULL;

        --Fetch R12 value of the payment method
        SELECT flv.lookup_code
          INTO l_payment_method
          FROM apps.fnd_lookup_values flv
         WHERE flv.lookup_type = 'PAYMENT METHOD'
           AND flv.lookup_code = UPPER (piv_payment_method)
           AND LANGUAGE = USERENV ('LANG');

        pov_payment_method         := l_payment_method;
        xxetn_debug_pkg.add_debug (' -  PROCEDURE : validate_payment_mtd = ' || piv_payment_method || ' -');
     EXCEPTION
        WHEN NO_DATA_FOUND
        THEN
           pon_error_cnt              := 2;
           l_payment_method           := NULL;
           print_log_message ('In No Data found of payment method lookup check' || SQLERRM);
        WHEN OTHERS
        THEN
           pon_error_cnt              := 2;
           l_payment_method           := NULL;
           print_log_message ('In When others of payment method lookup check' || SQLERRM);
     END validate_payment_mtd;

     --
  -- ========================
  -- Procedure: validate_currency_code
  -- =============================================================================
  --   This procedure validate_currency_code
  -- =============================================================================
  --  Input Parameters :
  --   piv_currency_code : currency code

     --  Output Parameters :
  --  pon_error_cnt    : Return Status
  -- -----------------------------------------------------------------------------
  --
     PROCEDURE validate_currency_code (piv_currency_code IN VARCHAR2, pon_error_cnt OUT NUMBER)
     IS
        l_record_cnt                  NUMBER;
     BEGIN
        xxetn_debug_pkg.add_debug (' +  PROCEDURE : validate_currency_code = ' || piv_currency_code || ' + ');
        l_record_cnt               := 0;

        BEGIN
           --check if the currency code exists in the system
           SELECT 1
             INTO l_record_cnt
             FROM fnd_currencies fc
            WHERE fc.currency_code = piv_currency_code
              AND fc.enabled_flag = g_yes
              AND fc.currency_flag = g_yes
              AND TRUNC (SYSDATE) BETWEEN TRUNC (NVL (fc.start_date_active, SYSDATE))
                                      AND TRUNC (NVL (fc.end_date_active, SYSDATE));
        EXCEPTION
           WHEN NO_DATA_FOUND
           THEN
              l_record_cnt               := 2;
              print_log_message ('In No Data found of currency code check' || SQLERRM);
           WHEN OTHERS
           THEN
              l_record_cnt               := 2;
              print_log_message ('In When others of currency code check' || SQLERRM);
        END;

        IF l_record_cnt > 1
        THEN
           pon_error_cnt              := 2;
        END IF;

        print_log_message (' -  PROCEDURE : validate_currency_code = ' || piv_currency_code || ' - ');
     EXCEPTION
        WHEN OTHERS
        THEN
           g_retcode                  := 2;
           pon_error_cnt              := 2;
           g_errbuff                  := 'Failed while validating currency code.';
           print_log_message ('In Exception validate currency code' || SQLERRM);
     END validate_currency_code;


  */
  --Ver1.3 changes end

  --
  -- ========================
  -- Procedure: validate_tax
  -- =============================================================================
  --   This procedure validate_tax
  -- =============================================================================
  --  Input Parameters :
  --   piv_tax_code : 11i tax code

  --  Output Parameters :
  --  pov_tax_code     : R12 tax code
  --  pon_error_cnt    : Return Status
  -- -----------------------------------------------------------------------------
  --
  PROCEDURE validate_tax(piv_tax_code              IN VARCHAR2,
                         pin_org_id                IN NUMBER,
                         pov_tax_code              OUT VARCHAR2,
                         pov_tax                   OUT VARCHAR2,
                         pov_tax_regime_code       OUT VARCHAR2,
                         pov_tax_rate_code         OUT VARCHAR2,
                         pov_tax_status_code       OUT VARCHAR2,
                         pov_tax_jurisdiction_code OUT VARCHAR2,
                         pon_error_cnt             OUT NUMBER) IS
    l_record_cnt NUMBER;
    l_tax_code   VARCHAR2(50);
    l_tax_lkp CONSTANT VARCHAR2(50) := 'XXEBTAX_TAX_CODE_MAPPING';
  BEGIN
    xxetn_debug_pkg.add_debug(' +  PROCEDURE : validate_tax = ' ||
                              piv_tax_code || ' + ');
    l_record_cnt := 0;
    l_tax_code   := NULL;

    --Fetch R12 value of the tax code
    SELECT flv.description --Ver1.4 changes
      INTO l_tax_code
      FROM apps.fnd_lookup_values flv
     WHERE TRIM((flv.lookup_code)) = TRIM((piv_tax_code))
       AND flv.enabled_flag = 'Y'
       AND UPPER(flv.lookup_type) = l_tax_lkp
       AND TRUNC(SYSDATE) BETWEEN
           TRUNC(NVL(flv.start_date_active, SYSDATE)) AND
           TRUNC(NVL(flv.end_date_active, SYSDATE))
       AND flv.LANGUAGE = USERENV('LANG');

    pov_tax_code := l_tax_code;

    -- fetch values corresponding to the R12 tax code
    IF (l_tax_code IS NOT NULL) THEN
      BEGIN
        /*     SELECT DISTINCT zrb.tax,
              zrb.tax_regime_code,
              zrb.tax_rate_code,
              zrb.tax_status_code,
              zrb.tax_jurisdiction_code
         INTO pov_tax,
              pov_tax_regime_code,
              pov_tax_rate_code,
              pov_tax_status_code,
              pov_tax_jurisdiction_code
         FROM zx_rates_b zrb, zx_regimes_b zb
        WHERE zb.tax_regime_code = zrb.tax_regime_code
          AND zrb.tax_rate_code = l_tax_code
          AND TRUNC (SYSDATE) BETWEEN TRUNC (NVL (zb.effective_from, SYSDATE))
                                  AND TRUNC (NVL (zb.effective_to, SYSDATE));*/

        SELECT DISTINCT zrb.tax,
                        zrb.tax_regime_code,
                        zrb.tax_rate_code,
                        zrb.tax_status_code,
                        zrb.tax_jurisdiction_code
          INTO pov_tax,
               pov_tax_regime_code,
               pov_tax_rate_code,
               pov_tax_status_code,
               pov_tax_jurisdiction_code
          FROM zx_accounts            za,
               hr_operating_units     hrou,
               gl_ledgers             gl,
               fnd_id_flex_structures fifs,
               zx_rates_b             zrb,
               zx_regimes_b           zb
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
           AND zrb.tax_rate_code = l_tax_code
           AND hrou.organization_id = pin_org_id
           AND TRUNC(SYSDATE) BETWEEN
               TRUNC(NVL(zb.effective_from, SYSDATE)) AND
               TRUNC(NVL(zb.effective_to, SYSDATE));
      EXCEPTION
        WHEN NO_DATA_FOUND THEN
          pov_tax                   := NULL;
          pov_tax_regime_code       := NULL;
          pov_tax_rate_code         := NULL;
          pov_tax_status_code       := NULL;
          pov_tax_jurisdiction_code := NULL;
          pon_error_cnt             := 2;
          print_log_message('In No Data found of fetching tax code values' ||
                            SQLERRM);
        WHEN OTHERS THEN
          pov_tax                   := NULL;
          pov_tax_regime_code       := NULL;
          pov_tax_rate_code         := NULL;
          pov_tax_status_code       := NULL;
          pov_tax_jurisdiction_code := NULL;
          pon_error_cnt             := 2;
          print_log_message('In When others of fetching tax code values' ||
                            SQLERRM);
      END;
    END IF;

    xxetn_debug_pkg.add_debug(' -  PROCEDURE : validate_tax = ' ||
                              piv_tax_code || ' - ');
  EXCEPTION
    WHEN NO_DATA_FOUND THEN
      pon_error_cnt := 2;
      pov_tax_code  := NULL;
      print_log_message('In No Data found of validate tax check' ||
                        SQLERRM);
    WHEN OTHERS THEN
      pon_error_cnt := 2;
      pov_tax_code  := NULL;
      print_log_message('In When others of validate tax check' || SQLERRM);
  END validate_tax;

  --
  -- ========================
  -- Procedure: validate_awt_tax
  -- =============================================================================
  --   This procedure validate_awt_tax
  -- =============================================================================
  --  Input Parameters :
  --   piv_awt_group -11i awt group name

  --  Output Parameters :
  --  pov_awt_group - R12 awt group name
  --  pon_error_cnt    : Return Status
  -- -----------------------------------------------------------------------------
  --
  PROCEDURE validate_awt_tax(piv_awt_group IN VARCHAR2,
                             pov_awt_group OUT VARCHAR2,
                             pon_error_cnt OUT NUMBER) IS
    l_record_cnt NUMBER;
    l_awt_group  VARCHAR2(50);
    l_awt_lkp CONSTANT VARCHAR2(50) := '';
  BEGIN
    xxetn_debug_pkg.add_debug(' +  PROCEDURE : validate_awt_tax = ' ||
                              piv_awt_group || ' + ');
    l_record_cnt := 0;
    l_awt_group  := NULL;

    --Fetch R12 value of the payment method
    SELECT flv.lookup_code
      INTO l_awt_group
      FROM apps.fnd_lookup_values flv
     WHERE flv.lookup_type = l_awt_lkp
       AND flv.lookup_code = UPPER(piv_awt_group)
       AND LANGUAGE = USERENV('LANG');

    pov_awt_group := l_awt_group;
    xxetn_debug_pkg.add_debug(' -  PROCEDURE : validate_awt_tax = ' ||
                              piv_awt_group || ' -');
  EXCEPTION
    WHEN NO_DATA_FOUND THEN
      pon_error_cnt := 2;
      pov_awt_group := NULL;
      print_log_message('In No Data found of awt tax code group lookup check' ||
                        SQLERRM);
    WHEN OTHERS THEN
      pon_error_cnt := 2;
      pov_awt_group := NULL;
      print_log_message('In When others of awt tax code group lookup check' ||
                        SQLERRM);
  END validate_awt_tax;

  --
  -- ========================
  -- Procedure: validate_dist_code_concatenatd
  -- =============================================================================
  --   This procedure validate_dist_code_concatenatd
  -- =============================================================================
  --  Input Parameters :
  --   piv_tax_code : 11i tax code

  --  Output Parameters :
  --  pov_tax_code     : R12 tax code
  --  pon_error_cnt    : Return Status
  -- -----------------------------------------------------------------------------
  --
  PROCEDURE validate_dist_code_concatenatd(piv_operating_unit         IN VARCHAR2,
                                           pov_dist_code_concatenated OUT VARCHAR2,
                                           pon_dist_code_concat_id    OUT NUMBER,
                                           pon_error_cnt              OUT NUMBER) IS
    l_record_cnt NUMBER;
    l_dist_ccid_lkp CONSTANT VARCHAR2(50) := 'XXAP_PART_PAY_MGR_ACT';
    l_rec        xxetn_coa_mapping_pkg.g_coa_rec_type;
    x_rec        xxetn_coa_mapping_pkg.g_coa_rec_type;
    x_msg        VARCHAR2(3000);
    x_status     VARCHAR2(50);
    l_direction  VARCHAR2(30);
    l_ext_system VARCHAR2(240);
    l_txn_date   DATE;
  BEGIN
    xxetn_debug_pkg.add_debug(' +  PROCEDURE : validate_dist_code_concatenatd  + ');
    l_record_cnt := 0;

    --Fetch R12 value of the distribution code combination
    SELECT flv.description
      INTO pov_dist_code_concatenated
      FROM apps.fnd_lookup_values flv
     WHERE TRIM((flv.meaning)) = TRIM((piv_operating_unit))
       AND flv.enabled_flag = 'Y'
       AND TRIM(UPPER(flv.lookup_type)) = l_dist_ccid_lkp
       AND TRUNC(SYSDATE) BETWEEN
           TRUNC(NVL(flv.start_date_active, SYSDATE)) AND
           TRUNC(NVL(flv.end_date_active, SYSDATE))
       AND flv.LANGUAGE = USERENV('LANG');

    -- fetch values corresponding to the R12 tax code
    IF (pov_dist_code_concatenated IS NOT NULL) THEN
      BEGIN
        l_rec.concatenated_segments := pov_dist_code_concatenated;
        l_txn_date                  := SYSDATE;

        SELECT code_combination_id
          INTO pon_dist_code_concat_id
          FROM gl_code_combinations_kfv gcck
         WHERE concatenated_segments = pov_dist_code_concatenated;
      EXCEPTION
        WHEN NO_DATA_FOUND THEN
          pon_error_cnt           := 2;
          pon_dist_code_concat_id := NULL;
          print_log_message('In No Data found of deriving ccid from concatenated segments' ||
                            SQLERRM);
        WHEN OTHERS THEN
          pon_error_cnt           := 2;
          pon_dist_code_concat_id := NULL;
          print_log_message('In When others of deriving ccid from concatenated segments' ||
                            SQLERRM);
      END;
    END IF;

    xxetn_debug_pkg.add_debug(' CCID for account:' ||
                              pov_dist_code_concatenated || 'is :' ||
                              pon_dist_code_concat_id);
    xxetn_debug_pkg.add_debug(' -  PROCEDURE : validate_dist_code_concatenatd  - ');
  EXCEPTION
    WHEN NO_DATA_FOUND THEN
      pon_error_cnt           := 2;
      pon_dist_code_concat_id := NULL;
      print_log_message('In No Data found of validate_dist_code_concatenatd check' ||
                        SQLERRM);
    WHEN OTHERS THEN
      pon_error_cnt           := 2;
      pon_dist_code_concat_id := NULL;
      print_log_message('In When others of validate_dist_code_concatenatd check' ||
                        SQLERRM);
  END validate_dist_code_concatenatd;

  --ver 1.3 changes start

  --
  -- ========================
  -- Procedure: validate_code_combinations
  -- =============================================================================
  --   This procedure validate_code_combinations
  -- =============================================================================
  --  Input Parameters :
  --   piv_dist_code_concatenated : 11i account segments

  --  Output Parameters :
  --  pon_dist_code_concat_id     : R12 CCID
  --  pon_error_cnt    : Return Status
  -- -----------------------------------------------------------------------------
  --
  PROCEDURE validate_code_combinations(piv_dist_code_concatenated IN VARCHAR2,
                                       pon_dist_code_concat_id    OUT NUMBER,
                                       pon_error_cnt              OUT NUMBER) IS
    l_record_cnt NUMBER;
    l_rec        xxetn_coa_mapping_pkg.g_coa_rec_type;
    x_rec        xxetn_coa_mapping_pkg.g_coa_rec_type;
    l_coa_rec    xxetn_common_pkg.g_rec_type;
    x_msg        VARCHAR2(3000);
    l_msg        VARCHAR2(3000);
    x_status     VARCHAR2(50);
    l_direction  VARCHAR2(30);
    l_ext_system VARCHAR2(240) := NULL;
    l_txn_date   DATE;
  BEGIN
    xxetn_debug_pkg.add_debug(' +  PROCEDURE : validate_code_combinations  + ');
    l_record_cnt := 0;

    BEGIN
      l_rec.concatenated_segments := piv_dist_code_concatenated;
      l_txn_date                  := SYSDATE;
      l_direction                 := 'LEGACY-TO-R12';
      xxetn_coa_mapping_pkg.get_code_combination(l_direction,
                                                 l_ext_system,
                                                 l_txn_date,
                                                 l_rec,
                                                 x_rec,
                                                 x_status,
                                                 x_msg);

      IF (x_status = g_coa_error) --Ver.1.4 changes
       THEN
        pon_error_cnt := 2;
        xxetn_debug_pkg.add_debug(' + API Status:' || x_status ||
                                  'Error is: ' || x_msg);
      ELSE
        xxetn_debug_pkg.add_debug('R12 concatenated segments :' ||
                                  x_rec.concatenated_segments);
        l_coa_rec.segment1  := x_rec.segment1;
        l_coa_rec.segment2  := x_rec.segment2;
        l_coa_rec.segment3  := x_rec.segment3;
        l_coa_rec.segment4  := x_rec.segment4;
        l_coa_rec.segment5  := x_rec.segment5;
        l_coa_rec.segment6  := x_rec.segment6;
        l_coa_rec.segment7  := x_rec.segment7;
        l_coa_rec.segment8  := x_rec.segment8;
        l_coa_rec.segment9  := x_rec.segment9;
        l_coa_rec.segment10 := x_rec.segment10;
        xxetn_common_pkg.get_ccid(p_in_segments => l_coa_rec,
                                  p_ccid        => pon_dist_code_concat_id,
                                  p_err_msg     => l_msg);
        xxetn_debug_pkg.add_debug('ccid ' || pon_dist_code_concat_id ||
                                  ' / l_msg ' || l_msg);

        IF (pon_dist_code_concat_id IS NULL) THEN
          pon_error_cnt := 2;
          xxetn_debug_pkg.add_debug(' Error in deriving CCID. Error is :' ||
                                    l_msg);
        END IF;
      END IF;
    EXCEPTION
      WHEN OTHERS THEN
        xxetn_debug_pkg.add_debug('ERROR - ' || SQLERRM);
        xxetn_debug_pkg.add_debug('Error : Backtace : ' ||
                                  DBMS_UTILITY.format_error_backtrace);
        pon_error_cnt := 2;
    END;

    xxetn_debug_pkg.add_debug(' CCID for account is :' ||
                              pon_dist_code_concat_id);
    xxetn_debug_pkg.add_debug(' -  PROCEDURE : validate_code_combinations  - ');
  EXCEPTION
    WHEN NO_DATA_FOUND THEN
      pon_error_cnt           := 2;
      pon_dist_code_concat_id := NULL;
      print_log_message('In No Data found of validate_code_combinations check' ||
                        SQLERRM);
    WHEN OTHERS THEN
      pon_error_cnt           := 2;
      pon_dist_code_concat_id := NULL;
      print_log_message('In When others of validate_code_combinations check' ||
                        SQLERRM);
  END validate_code_combinations;

  --ver 1.3 changes end

  --
  -- ========================
  -- Procedure: validate_project
  -- =============================================================================
  --   This procedure validate_project
  -- =============================================================================
  --  Input Parameters :
  --   piv_project_name
  --   piv_task_name
  --   piv_exp_org_name
  --  Output Parameters :
  --  pon_project_id
  --  pon_tax_id
  --  pov_exp_org_name
  --  pon_exp_org_id
  --  pon_error_cnt    : Return Status
  -- -----------------------------------------------------------------------------
  --
  PROCEDURE validate_project(piv_project_name  IN VARCHAR2,
                             piv_task_name     IN VARCHAR2,
                             piv_exp_org_name  IN VARCHAR2,
                             pid_exp_item_date IN DATE,
                             pon_project_id    OUT NUMBER,
                             pon_task_id       OUT NUMBER,
                             pov_exp_org_name  OUT VARCHAR2,
                             pon_exp_org_id    OUT NUMBER,
                             pon_error_cnt     OUT NUMBER,
                             pov_err_msg       OUT VARCHAR2,
                             pov_source_column OUT VARCHAR2,
                             pov_source_value  OUT VARCHAR2) IS
    l_record_cnt    NUMBER;
    l_pa_project_id NUMBER;
    l_pa_task_id    NUMBER;
    l_project_type  VARCHAR2(20);
    l_task_lookup   fnd_lookup_types_tl.lookup_type%TYPE := 'XXETN_PA_TASK_MAPPING';
    l_task_name     VARCHAR2(20) := NULL;
  BEGIN
    xxetn_debug_pkg.add_debug(' +  PROCEDURE : validate_project = ' ||
                              piv_project_name || ' + ');
    l_record_cnt    := 0;
    l_pa_project_id := NULL;
    l_pa_task_id    := NULL;
    l_project_type  := NULL;
    --l_task_lookup         := NULL;
    l_task_name := NULL;

    --Fetch R12 project id for the project name
    BEGIN
      SELECT project_id
      --project_type
        INTO l_pa_project_id
      --                l_project_type   --ver1.4 FOT changes
        FROM pa_projects_all
       WHERE NAME like piv_project_name || '%';

    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        pon_error_cnt     := 2;
        l_pa_project_id   := NULL;
        pov_err_msg       := 'Project not found';
        pov_source_column := 'PROJECT NUMBER';
        pov_source_value  := piv_project_name;
        print_log_message('In No Data found for validating project name' ||
                          SQLERRM);
      WHEN OTHERS THEN
        pon_error_cnt     := 2;
        l_pa_project_id   := NULL;
        pov_err_msg       := 'Error while validating Project ' ||
                             SUBSTR(SQLERRM, 1, 150);
        pov_source_column := 'PROJECT NUMBER';
        pov_source_value  := piv_project_name;
        print_log_message('In When others for validating project name' ||
                          SQLERRM);
    END;

    pon_project_id := l_pa_project_id;

    --ver 1.4 changes start
    IF l_pa_project_id IS NOT NULL THEN
      BEGIN
        SELECT class_code
          INTO l_project_type
          FROM pa_project_classes
         WHERE project_id = l_pa_project_id
           AND class_category = 'Project Type';

      EXCEPTION
        WHEN NO_DATA_FOUND THEN
          pov_err_msg       := 'Project class not found';
          pov_source_column := 'PROJECT NUMBER';
          pov_source_value  := piv_project_name;
          pon_error_cnt     := 2;
          l_project_type    := NULL;
          print_log_message('Project classification not defined' ||
                            SQLERRM);
        WHEN OTHERS THEN
          pon_error_cnt     := 2;
          pov_err_msg       := 'Error while fetching project class ' ||
                               SUBSTR(SQLERRM, 1, 150);
          pov_source_column := 'PROJECT NUMBER';
          pov_source_value  := piv_project_name;
          l_project_type    := NULL;
          print_log_message('In When others for validating project classification' ||
                            SQLERRM);
      END;
    END IF;

    -- ver1.4 changes end

    /* --Ver1.1 Changes start
    --Ver1.1 Commented for CR34
    -- fetch task values corresponding to the R12 project
            IF ( l_pa_project_id IS NOT NULL)
            THEN
               BEGIN
                  SELECT task_id
                    INTO l_pa_task_id
                    FROM pa_tasks
                   WHERE task_name  = piv_task_name
                     AND project_id = l_pa_project_id;

               EXCEPTION
                  WHEN NO_DATA_FOUND
                  THEN

                     l_pa_task_id := NULL;
                     print_log_message ( 'In No Data found of fetching task values'||SQLERRM);
                  WHEN OTHERS
                  THEN
                     l_pa_task_id := NULL;
                     print_log_message ( 'In When others of fetching task values'||SQLERRM);
               END;
               pon_task_id := l_pa_task_id;
            END IF;
    */ -- Ver1.1 Commented for CR34
    IF l_project_type IS NOT NULL THEN
      BEGIN
        SELECT tag
          INTO l_task_name
          FROM fnd_lookup_values flv
         WHERE lookup_type = l_task_lookup
           AND flv.LANGUAGE = USERENV('LANG')
           AND TRUNC(SYSDATE) BETWEEN
               TRUNC(NVL(flv.start_date_active, SYSDATE)) AND
               TRUNC(NVL(flv.end_date_active, SYSDATE))
           AND enabled_flag = 'Y'
           AND (meaning) = (l_project_type);
      EXCEPTION
        WHEN NO_DATA_FOUND THEN
          l_pa_task_id      := NULL;
          pov_err_msg       := 'Task not found for project class';
          pov_source_column := 'PROJECT CLASS';
          pov_source_value  := l_project_type;
          pon_error_cnt     := 2;
          print_log_message('In No Data found of fetching task values' ||
                            SQLERRM);
        WHEN OTHERS THEN
          l_pa_task_id      := NULL;
          pon_error_cnt     := 2;
          pov_err_msg       := 'Error while fetching task from project class ' ||
                               SUBSTR(SQLERRM, 1, 150);
          pov_source_column := 'PROJECT CLASS';
          pov_source_value  := l_project_type;
          print_log_message('In When others of fetching task values' ||
                            SQLERRM);
      END;
    END IF;

    --Ver1.1 Changes end
    IF l_task_name IS NOT NULL THEN
      BEGIN
        SELECT task_id
          INTO pon_task_id
          FROM pa_tasks pt, pa_projects_all pp
         WHERE (pt.task_number) = (l_task_name)
           AND pp.project_id = l_pa_project_id
           AND pp.project_id = pt.project_id
           AND pid_exp_item_date BETWEEN NVL(pp.start_date, SYSDATE) AND
               NVL(pp.closed_date, SYSDATE)
           AND pp.enabled_flag = 'Y'
           AND pid_exp_item_date BETWEEN NVL(pt.start_date, SYSDATE) AND
               NVL(pt.completion_date, SYSDATE);
      EXCEPTION
        WHEN OTHERS THEN
          pon_task_id       := NULL;
          pon_error_cnt     := 2;
          pov_err_msg       := 'Error while validating task  ' ||
                               SUBSTR(SQLERRM, 1, 150);
          pov_source_column := 'PROJECT TASK';
          pov_source_value  := l_task_name;
          print_log_message('Error: Task Number could not be derived from the system. ' ||
                            SQLERRM);
      END;
    END IF;
    /*
          --Fetch R12 expenditure org name from the 11i value
          BEGIN
             SELECT TRIM (flv.description)
               INTO pov_exp_org_name
               FROM fnd_lookup_values flv
              WHERE  (flv.meaning) =  (piv_exp_org_name)
                AND flv.LANGUAGE = USERENV ('LANG')
                AND flv.enabled_flag = 'Y'
                AND UPPER (flv.lookup_type) = g_ou_lkp
                AND TRUNC (SYSDATE) BETWEEN TRUNC (NVL (flv.start_date_active, SYSDATE))
                                        AND TRUNC (NVL (flv.end_date_active, SYSDATE));
          EXCEPTION
             WHEN NO_DATA_FOUND
             THEN
                pov_exp_org_name           := NULL;
                print_log_message ('In No Data found of fetching exp org name' || SQLERRM);
             WHEN OTHERS
             THEN
                pov_exp_org_name           := NULL;
                print_log_message ('In When others of fetching exp org name' || SQLERRM);
          END;
    */
    --Fetch R12 expenditure org id from the exp org name
    IF (piv_exp_org_name IS NOT NULL) THEN
      BEGIN
        SELECT hou.organization_id
          INTO pon_exp_org_id
          FROM apps.hr_all_organization_units hou
         WHERE (hou.NAME) = (piv_exp_org_name);
      EXCEPTION
        WHEN NO_DATA_FOUND THEN
          pon_exp_org_id    := NULL;
          pov_err_msg       := 'Expenditure_organization not set ';
          pov_source_column := 'Expenditure Organization';
          pov_source_value  := pov_exp_org_name;
          pon_error_cnt     := 2;
          print_log_message('In No Data found of fetching exp org id' ||
                            SQLERRM);
        WHEN OTHERS THEN
          pon_exp_org_id    := NULL;
          pon_error_cnt     := 2;
          pov_err_msg       := 'Error while validating Expenditure_organization  ' ||
                               SUBSTR(SQLERRM, 1, 150);
          pov_source_column := 'Expenditure Organization';
          pov_source_value  := pov_exp_org_name;
          print_log_message('In When others of fetching exp org id' ||
                            SQLERRM);
      END;
    END IF;

    xxetn_debug_pkg.add_debug(' -  PROCEDURE : validate_project = ' ||
                              piv_project_name || ' - ');
  EXCEPTION
    WHEN NO_DATA_FOUND THEN
      pon_error_cnt     := 2;
      pov_err_msg       := 'Error while validaitng project details ' ||
                           SUBSTR(SQLERRM, 1, 150);
      pov_source_column := 'PROJECT NUMBER';
      pov_source_value  := piv_project_name;
      pov_exp_org_name  := NULL;
      print_log_message('In No Data found of validate project check' ||
                        SQLERRM);
    WHEN OTHERS THEN
      pon_error_cnt     := 2;
      pov_err_msg       := 'Error while validaitng project details  ' ||
                           SUBSTR(SQLERRM, 1, 150);
      pov_source_column := 'PROJECT NUMBER';
      pov_source_value  := piv_project_name;
      pov_exp_org_name  := NULL;
      print_log_message('In When others of validate project check' ||
                        SQLERRM);
  END validate_project;

  --
  -- ========================
  -- Procedure: derive_po_info
  -- =============================================================================
  --   This procedure derive_po_info
  -- =============================================================================
  --  Input Parameters :
  --   piv_project_name
  --   piv_task_name
  --   piv_exp_org_name
  --  Output Parameters :
  --  pon_project_id
  --  pon_tax_id
  --  pov_exp_org_name
  --  pon_exp_org_id
  --  pon_error_cnt    : Return Status
  -- -----------------------------------------------------------------------------
  --
  PROCEDURE derive_po_info(piv_po_number           IN VARCHAR2,
                           piv_po_line_number      IN VARCHAR2,
                           piv_po_shipment_num     IN VARCHAR2,
                           piv_po_distribution_num IN VARCHAR2,
                           piv_receipt_number      IN VARCHAR2,
                           piv_receipt_line_number OUT VARCHAR2, --v1.7 picked this from the working code
                           pon_po_header_id        OUT NUMBER,
                           pon_po_line_id          OUT NUMBER,
                           pon_po_shipment_id      OUT NUMBER,
                           pon_po_distribution_id  OUT NUMBER,
                           pon_receipt_id          OUT NUMBER,
                           pon_error_cnt           OUT NUMBER) IS
    l_record_cnt          NUMBER;
    l_po_header_id        NUMBER;
    l_po_line_id          NUMBER;
    l_po_line_location_id NUMBER;
  BEGIN
    xxetn_debug_pkg.add_debug(' +  PROCEDURE : derive_po_info = ' ||
                              piv_po_number || ' + ');
    l_record_cnt := 0;

    --Fetch R12 po_header id for the po number
    BEGIN
      SELECT po_header_id
        INTO l_po_header_id
        FROM po_headers_all
       WHERE segment1 = piv_po_number;
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        pon_error_cnt  := 2;
        l_po_header_id := NULL;
        print_log_message('In No Data found for validating po number' ||
                          SQLERRM);
      WHEN OTHERS THEN
        pon_error_cnt  := 2;
        l_po_header_id := NULL;
        print_log_message('In When others for validating po number' ||
                          SQLERRM);
    END;

    pon_po_header_id := l_po_header_id;

    --- V1.5 changes (GRNI Changes)

    -- fetch po_line values corresponding to the R12 po_header id
    IF (l_po_header_id IS NOT NULL) THEN
      BEGIN
        SELECT pla.po_line_id, plla.line_location_id
          INTO l_po_line_id, l_po_line_location_id
          FROM po_lines_all pla, po_line_locations_all plla
         WHERE plla.attribute6 = piv_po_line_number
           AND plla.attribute7 = piv_po_shipment_num
           AND pla.po_header_id = l_po_header_id
           AND pla.po_line_id = plla.po_line_id
           AND plla.po_header_id = l_po_header_id;
      EXCEPTION
        WHEN NO_DATA_FOUND THEN
          l_po_line_id  := NULL;
          pon_error_cnt := 2;
          print_log_message('In No Data found of fetching po line and location values' ||
                            SQLERRM);
        WHEN OTHERS THEN
          l_po_line_id  := NULL;
          pon_error_cnt := 2;
          print_log_message('In When others of fetching po line and location values' ||
                            SQLERRM);
      END;

      pon_po_line_id     := l_po_line_id;
      pon_po_shipment_id := l_po_line_location_id;
    END IF;

    /*  IF (l_po_line_id IS NOT NULL)
    THEN
       --Fetch shipment ID from shipment number
       BEGIN
          SELECT line_location_id
            INTO l_po_line_location_id
            FROM po_line_locations_all
           WHERE shipment_num = piv_po_shipment_num AND po_line_id = l_po_line_id;
       EXCEPTION
          WHEN NO_DATA_FOUND
          THEN
             l_po_line_location_id      := NULL;
       pon_error_cnt               := 2;
             print_log_message ('In No Data found of fetching shipment id' || SQLERRM);
          WHEN OTHERS
          THEN
             l_po_line_location_id      := NULL;
       pon_error_cnt               := 2;
             print_log_message ('In When others of fetching shipment id' || SQLERRM);
       END;

       pon_po_shipment_id         := l_po_line_location_id;
    END IF; */

    --Fetch po distribution id from distribution number
    IF (l_po_line_location_id IS NOT NULL) THEN
      BEGIN
        SELECT po_distribution_id
          INTO pon_po_distribution_id
          FROM po_distributions_all
         WHERE distribution_num = piv_po_distribution_num
           AND line_location_id = l_po_line_location_id;
      EXCEPTION
        WHEN NO_DATA_FOUND THEN
          pon_po_distribution_id := NULL;
          pon_error_cnt          := 2;
          print_log_message('In No Data found of fetching po distribution id' ||
                            SQLERRM);
        WHEN OTHERS THEN
          pon_po_distribution_id := NULL;
          pon_error_cnt          := 2;
          print_log_message('In When others of fetching po_distribution id' ||
                            SQLERRM);
      END;
    END IF;

    --fetch receipt id from receipt number and receipt line number
    IF piv_receipt_number IS NOT NULL THEN
      --check
      BEGIN
        SELECT rsl.line_num , transaction_id
          INTO piv_receipt_line_number , pon_receipt_id    --v1.7 picked this from the working code
          FROM rcv_shipment_headers rsh,
               rcv_shipment_lines   rsl,
               rcv_transactions     rct
         WHERE rsh.shipment_header_id = rsl.shipment_header_id
           AND rsl.shipment_line_id = rct.shipment_line_id
           AND rsh.shipment_header_id = rct.shipment_header_id
           AND rsh.receipt_num = piv_receipt_number
           --AND rsl.line_num = piv_receipt_line_number
           and rct.po_distribution_id = pon_po_distribution_id   --v1.7 picked this from the working code
           AND rct.transaction_type = 'RECEIVE';
      EXCEPTION
        WHEN NO_DATA_FOUND THEN
          pon_receipt_id := NULL;
          pon_error_cnt  := 2;
          print_log_message('In No Data found of fetching receipt id' ||
                            SQLERRM);
        WHEN OTHERS THEN
          pon_receipt_id := NULL;
          pon_error_cnt  := 2;
          print_log_message('In When others of fetching receipt id' ||
                            SQLERRM);
      END;
    END IF;

    xxetn_debug_pkg.add_debug(' -  PROCEDURE : derive_po_info = ' ||
                              piv_po_number || ' - ');
  EXCEPTION
    WHEN NO_DATA_FOUND THEN
      pon_error_cnt := 2;
      print_log_message('In No Data found of derive_po_info check' ||
                        SQLERRM);
    WHEN OTHERS THEN
      pon_error_cnt := 2;
      print_log_message('In When others of derive_po_infocheck' || SQLERRM);
  END derive_po_info;

  --added for defect #2568
  FUNCTION get_upd_line_error_info(p_leg_inv_id IN NUMBER
                               , p_inv_hdr_batch_id IN NUMBER
                               , p_inv_inv_num IN VARCHAR2
                               , p_inv_ven_num In VARCHAR2)
  RETURN CHAR IS
     v_line_error VARCHAR2(1);
     v_line_error_cnt NUMBER;
  BEGIN
     SELECT count(1)
     INTO  v_line_error_cnt
     FROM xxap_invoice_lines_stg xil
     WHERE xil.leg_invoice_id= p_leg_inv_id
     AND   xil.process_flag='E'
     AND   xil.batch_id = p_inv_hdr_batch_id
     --v2.xx performance change
     AND  xil.leg_invoice_num = p_inv_inv_num
     AND  xil.leg_vendor_num  = p_inv_ven_num;

     IF v_line_error_cnt >0
     THEN
        v_line_error :='Y';

        UPDATE xxap_invoice_lines_stg
        SET process_flag = 'E',
            error_type = 'Error In another Line'
        WHERE process_flag = 'V'
        AND leg_invoice_id= p_leg_inv_id
        AND batch_id = p_inv_hdr_batch_id
        --v2.xx performance change
        AND leg_invoice_num = p_inv_inv_num
        AND leg_vendor_num  = p_inv_ven_num;

     ELSE
        v_line_error :='N';
     END IF;

     RETURN v_line_error;


  EXCEPTION
     WHEN OTHERS THEN
        v_line_error := 'Y';
        fnd_file.put_line(fnd_file.log,'get_upd_line_error_info' );
        RETURN v_line_error;
  END get_upd_line_error_info;

  --v2.0 start add function to fetch
  FUNCTION get_tax_classification(p_org_id IN NUMBER)
  RETURN CHAR IS
     l_tcc VARCHAR2(30);
     v_line_error_cnt NUMBER;
  BEGIN

     SELECT flv.attribute1  --v3.0
     INTO l_tcc
     FROM    fnd_lookup_Values_vl flv, hr_operating_units hou
     WHERE flv.lookup_type= 'XXETN_PTP_INV_FRM_PERS'
     AND flv.enabled_flag = 'Y'
     AND flv.start_date_Active <= trunc(sysdate)
     AND hou.name = flv.description
     AND hou.organization_id = p_org_id;

     RETURN l_tcc;

  EXCEPTION
     WHEN OTHERS
     THEN

        RETURN l_tcc;
  END;
  -- ========================
  -- Procedure: validate_invoice
  -- =============================================================================
  --   This procedure is used to run generic validations for all mandatory columns
  --   checks
  -- =============================================================================
  --  Input Parameters :
  --    None
  --  Output Parameters :
  --    pov_retcode          :
  -- -----------------------------------------------------------------------------
  --
  PROCEDURE validate_invoice IS
    l_vendor_id NUMBER;
    --      l_org_id                      NUMBER;
    --      l_sob_id                      NUMBER;
    l_vendor_site_id NUMBER;
    l_vendor_name    VARCHAR2(50);
    l_terms_name     VARCHAR2(50);
    --      l_operating_unit              VARCHAR2 (100);
    --      l_payment_method              VARCHAR2 (100);
    l_project_id            NUMBER := NULL;
    l_task_id               NUMBER := NULL;
    l_project_number        VARCHAR2(240) := NULL;
    l_task_number           VARCHAR2(100) := NULL;
    l_exp_org_name          VARCHAR2(100);
    l_exp_org_id            NUMBER;
    l_value_out             VARCHAR2(50);
    l_log_ret_status        VARCHAR2(50);
    l_log_err_msg           VARCHAR2(2000);
    l_dist_code_concat      VARCHAR2(1000);
    l_dist_code_concat_id   NUMBER;
    l_lin_operating_unit    VARCHAR2(500);
    l_lin_org_id            NUMBER;
    l_lin_sob_id            NUMBER;
    l_awt_group             VARCHAR2(500);
    l_accts_pay_code_concat VARCHAR2(1000);
    l_tax                   VARCHAR2(30);
    l_tax_regime_code       VARCHAR2(30);
    l_tax_rate_code         VARCHAR2(150);
    l_tax_status_code       VARCHAR2(30);
    l_tax_jurisdiction_code VARCHAR2(30);
    l_tax_code              VARCHAR2(150);
    l_amount                apps.ap_invoice_distributions_all.amount%TYPE;
    l_invoice_count         NUMBER := 0;
    l_inv_header_count      NUMBER;
    l_inv_line_count        NUMBER;
    l_line_err_cnt          NUMBER;
    l_seg_count             NUMBER;
    l_pay_acc_segment       VARCHAR2(240);
    l_dist_acc_segment      VARCHAR2(240);
    l_po_header_id          NUMBER;
    l_receipt_line_number   NUMBER; --v1.7 picked this from the working code
    l_po_line_id            NUMBER;
    l_po_shipment_id        NUMBER;
    l_po_distribution_id    NUMBER;
    l_receipt_id            NUMBER;
    l_msg_count             NUMBER;
    l_error_cnt             NUMBER;
    l_error_flag            VARCHAR2(10);
    l_header_error_flag     VARCHAR2(10);
    l_msg_data              VARCHAR2(2000);
    l_return_status         VARCHAR2(200);
    l_party_exists          VARCHAR2(1);
    l_process_flag          xxap_invoices_stg.process_flag%TYPE;
    l_err_code              VARCHAR2(40);
    l_err_msg               VARCHAR2(2000);
    l_ret_status            VARCHAR2(50);
    l_leg_attribute1        VARCHAR2(240); --TUT changes
    l_attr_category         VARCHAR2(240); --ver1.4 changes
    --Ver1.3 start
    l_rec           xxetn_coa_mapping_pkg.g_coa_rec_type;
    x_rec           xxetn_coa_mapping_pkg.g_coa_rec_type;
    l_coa_rec       xxetn_common_pkg.g_rec_type;
    x_msg           VARCHAR2(3000);
    l_msg           VARCHAR2(3000);
    x_status        VARCHAR2(50);
    l_direction     VARCHAR2(30);
    l_ext_system    VARCHAR2(240) := NULL;
    l_txn_date      DATE;
    l_record_cnt    NUMBER;
    l_vendor_id     NUMBER;
    l_payment_terms VARCHAR2(50);
    l_payment_term_lkp CONSTANT VARCHAR2(50) := 'XXAP_PAYTERM_MAPPING';
    --'ETN_PTP_PAYTERM_MAPPING';
    l_count          NUMBER;
    l_payment_method fnd_lookup_values.lookup_code%TYPE;
    l_payment_lkp CONSTANT VARCHAR2(50) := 'PAYMENT METHOD';
    l_operating_unit hr_operating_units.NAME%TYPE;
    l_org_id         hr_operating_units.organization_id%TYPE;
    l_sob_id         hr_operating_units.set_of_books_id%TYPE;
    g_ou_lkp CONSTANT VARCHAR2(50) := 'ETN_COMMON_OU_MAP';
    l_valerr_cnt    NUMBER := 1;
    l_dist_ccid     NUMBER;
    l_supp_id       ap_supplier_sites_all.vendor_id%TYPE;
    l_supp_name     ap_suppliers.vendor_name%TYPE;
    l_suppl_site_id NUMBER;
    l_dist_ccid_lkp CONSTANT VARCHAR2(50) := 'XXAP_PART_PAY_MGR_ACT';
    l_pay_dist_ccid              NUMBER;
    l_pay_dist_code_concatenated fnd_lookup_values.description%TYPE;
    l_amount_includes_tax_flag   VARCHAR2(1);
    l_taxable_flag               VARCHAR2(1);
    --Ver 1.2 changes start
    l_rec_org       xxetn_map_util.g_input_rec;
    l_column_name   VARCHAR2(100);
    l_column_value  VARCHAR2(240);
    l_gl_valerr_cnt NUMBER := 1;
    l_ap_valerr_cnt NUMBER := 1;
    l_ap_err_code   VARCHAR2(40);
    l_ap_err_msg    VARCHAR2(2000);
    l_gl_err_code   VARCHAR2(40);
    l_gl_err_msg    VARCHAR2(2000);
    l_source_name   VARCHAR2(2000);
    l_source_value  VARCHAR2(2000);
    l_tax_lkp CONSTANT VARCHAR2(50) := 'XXEBTAX_TAX_CODE_MAPPING';

    l_pa_project_id NUMBER;
    l_pa_task_id    NUMBER;
    l_project_type  VARCHAR2(20);
    l_task_lookup   fnd_lookup_types_tl.lookup_type%TYPE := 'XXETN_PA_TASK_MAPPING';
    l_task_name     VARCHAR2(20) := NULL;

    --Ver 1.2 changes end

    l_expenditure_type xxap_invoice_lines_stg.leg_expenditure_type%TYPE; --ver1.4 FUT changes

    --Ver1.3 end

    l_tax_count    number := NULL;     --v1.7 picked this from the working code
    l_ref_key5     xxap_invoice_lines_stg.leg_reference_key5%type;  --v1.7 picked this from the working code
    l_dist_line_id number;  --v1.7 picked this from the working code
    l_key3         xxap_invoice_lines_stg.leg_reference_key3%type;  --v1.7 picked this from the working code
    l_key4         xxap_invoice_lines_stg.leg_reference_key4%type;  --v1.7 picked this from the working code
    l_line_error_flag VARCHAR2(1);  --defect 2568
    --changes for CR 308318
    l_10_seg_rec          xxetn_common_pkg.g_rec_type;
    l_ret_msg             VARCHAR2(5000);
    --change for CR 308318 end here
    --changes for cross reference v1.9
    lv_out_val1 VARCHAR2(200);
    lv_out_val2 VARCHAR2(200);
    lv_out_val3 VARCHAR2(200);
    lv_err_msg1 VARCHAR2(2000);
    --changes for cross reference ends v1.9
    --v1.10 --change2nd option
    l_src_ven_site_id NUMBER;
    l_src_ven_id      NUMBER;
    l_suppl_err_msg  VARCHAR2(2000);
    l_suppl_err_code VARCHAR2(40) := NULL;
    l_new_vensite_id NUMBER;
    --v1.10 --change 2nd option variable declaration end here
    --v2.0 --change to add tax control amount
    l_tax_cont_amt NUMBER;
    --v2.0 ends here
    --v3.0
    l_tax_rate_percent NUMBER;
    --v3.0 ends
    -- --------------------------------------------------------------------------
    -- Cursor to select the new invoice data from invoice header staging table
    -- --------------------------------------------------------------------------
    CURSOR validate_invoice_hdr_cur IS
      SELECT xis.*
        FROM xxap_invoices_stg xis
       WHERE xis.process_flag IN (g_new, g_error) --TUT changes
         AND xis.batch_id = g_new_batch_id;

    -- -----------------------------------------------------------------
    -- Cursor to select new lines data from staging table
    -- -----------------------------------------------------------------
   /* CURSOR validate_invoice_line_cur(piv_invoice_num IN VARCHAR2,
                                     piv_vendor_num  IN VARCHAR2) IS
    --     SELECT   xils.*
      SELECT DISTINCT xils.*, xihs.leg_hold_lookup_code --TUT changes
        FROM xxap_invoice_lines_stg xils, xxap_invoice_holds_stg xihs
       WHERE 1 = 1
         AND xihs.leg_invoice_number(+) = xils.leg_invoice_num
         AND xihs.leg_operating_unit(+) = xils.leg_operating_unit
         AND xils.process_flag IN (g_new, g_error) --TUT changes
         AND xils.batch_id = g_new_batch_id
         AND xils.leg_invoice_num(+) = piv_invoice_num
         AND xils.leg_vendor_num(+) = piv_vendor_num
         AND xihs.process_flag(+) != g_converted
         AND xihs.process_flag(+) != g_error
      --          AND    line_type_lookup_code NOT IN ('PREPAY') --- Added this comment after  DRY run of INT2  To exclude PREPAY line Type
       ORDER BY xils.line_number;*/
       --v2.xx performance change
    CURSOR validate_invoice_line_cur(piv_invoice_num IN VARCHAR2,
                                     piv_vendor_num  IN VARCHAR2) IS
    --     SELECT   xils.*
      SELECT DISTINCT xils.*--, xis.leg_hold_lookup_code --TUT changes
        FROM xxap_invoice_lines_stg xils --, xxap_invoice_holds_stg xihs
       WHERE 1 = 1
         --AND xihs.leg_invoice_number(+) = xils.leg_invoice_num
         --AND xihs.leg_operating_unit(+) = xils.leg_operating_unit
         AND xils.process_flag IN (g_new, g_error) --TUT changes
         AND xils.batch_id = g_new_batch_id
         AND xils.leg_invoice_num(+) = piv_invoice_num
         AND xils.leg_vendor_num(+) = piv_vendor_num
         --AND xihs.process_flag(+) != g_converted
         --AND xihs.process_flag(+) != g_error
      --          AND    line_type_lookup_code NOT IN ('PREPAY') --- Added this comment after  DRY run of INT2  To exclude PREPAY line Type
       ORDER BY xils.line_number;

    --Ver1.3 changes start
    CURSOR coa_cur IS
      SELECT DISTINCT 'L' l_level,
                      leg_dist_code_concatenated code_concatenated
        FROM xxap_invoice_lines_stg
       WHERE batch_id = g_new_batch_id
         AND run_sequence_id = g_run_seq_id
         AND leg_reference_key1 = 'INVH'    -- CR 308318
      UNION
      SELECT DISTINCT 'H' l_level,
                      leg_accts_pay_code_concat code_concatenated
        FROM xxap_invoices_stg
       WHERE batch_id = g_new_batch_id
         AND run_sequence_id = g_run_seq_id;
    -- Ver1.2 changes start
    CURSOR org_cur IS
      SELECT DISTINCT plant_segment
        FROM xxap_invoice_lines_stg
       WHERE batch_id = g_new_batch_id
         AND run_sequence_id = g_run_seq_id;
    /*
          CURSOR org_cur
          IS
             SELECT DISTINCT leg_operating_unit
                        FROM xxap_invoices_stg
                       WHERE batch_id = g_new_batch_id AND run_sequence_id = g_run_seq_id;
    */
    -- Ver1.2 changes end
    CURSOR vendor_cur IS
      SELECT DISTINCT leg_vendor_num, leg_vendor_site_code, org_id
             ,accts_pay_code_combination_id  --v1.10 new column added
        FROM xxap_invoices_stg
       WHERE batch_id = g_new_batch_id
         AND run_sequence_id = g_run_seq_id;

    CURSOR term_cur IS
      SELECT DISTINCT leg_terms_name
        FROM xxap_invoices_stg
       WHERE batch_id = g_new_batch_id
         AND run_sequence_id = g_run_seq_id;

    CURSOR pmtmtd_cur IS
      SELECT DISTINCT leg_payment_method_code
        FROM xxap_invoices_stg
       WHERE batch_id = g_new_batch_id
         AND run_sequence_id = g_run_seq_id;

    CURSOR currency_cur IS
      SELECT DISTINCT leg_inv_currency_code
        FROM xxap_invoices_stg
       WHERE batch_id = g_new_batch_id
         AND run_sequence_id = g_run_seq_id;
--v5.0 starts
    /*CURSOR pay_mgr_cur IS
      SELECT DISTINCT organization_name, 
        FROM xxap_invoice_lines_stg
       WHERE batch_id = g_new_batch_id
         AND run_sequence_id = g_run_seq_id
         AND NVL(leg_deferred_acctg_flag, 'N') = 'P';*/
         CURSOR pay_mgr_cur IS
         SELECT DISTINCT organization_name, 
                substr(xil.leg_dist_code_concatenated,1,4) plant
        FROM xxap_invoice_lines_stg xil
       WHERE  NVL(leg_deferred_acctg_flag, 'N') = 'P';
--v5.0 ends
    CURSOR tax_cur IS
       SELECT DISTINCT leg_tax_code, org_id, plant_segment --v1.9
       FROM xxap_invoice_lines_stg
       WHERE batch_id = g_new_batch_id
       AND run_sequence_id = g_run_seq_id
       --v1.11 comment the line type change as it should be for all now
       -- add not null condition also as below
       AND leg_tax_code IS NOT NULL;
        -- AND leg_line_type_lookup_code ='TAX'   --NO TAX Related changes for 2568

    CURSOR project_cur IS
      SELECT DISTINCT leg_project_name,
                      leg_task_name,
                      leg_expenditure_item_date
        FROM xxap_invoice_lines_stg
       WHERE batch_id = g_new_batch_id
         AND run_sequence_id = g_run_seq_id;

    CURSOR exp_org_cur IS
      SELECT DISTINCT leg_expenditure_org_name
        FROM xxap_invoice_lines_stg
       WHERE batch_id = g_new_batch_id
         AND run_sequence_id = g_run_seq_id;

    --Ver 1.3 changes end
    --CR 308318 changes start here
    CURSOR coa_cur_ninvh IS
      SELECT  DISTINCT leg_dist_code_concatenated leg_code_concatenated
              /*,SUBSTR(leg_dist_code_concatenated,
                 instr(leg_dist_code_concatenated,'.',1,2)+1,
                    instr(leg_dist_code_concatenated,'.',1,3)-
                       instr(leg_dist_code_concatenated,'.',1,2)-1) leg_segment3*/
        FROM xxap_invoice_lines_stg
       WHERE batch_id = g_new_batch_id
         AND run_sequence_id = g_run_seq_id
         AND leg_reference_key1 <> 'INVH';
     l_derived_segment3   VARCHAR2(30);
     l_derived_Segment7   VARCHAR2(30);
     l_rec_ninvh           xxetn_coa_mapping_pkg.g_coa_rec_type;
     x_rec_ninvh           xxetn_coa_mapping_pkg.g_coa_rec_type;
     l_coa_rec_ninvh       xxetn_common_pkg.g_rec_type;
     x_msg_ninvh           VARCHAR2(3000);
     x_status_ninvh        VARCHAR2(3000);
     l_dist_ccid_ninvh     NUMBER;
    --CR 308318 changes end here

    --v1.11
    --cursor to assign unique number to tax code
    CURSOR unq_tax_cur IS
    SELECT  xout.tax_code,row_number() over (order by xout.tax_code) as "SNO"
    FROM (SELECT DISTINCT tax_code
          FROM xxap_invoice_lines_stg
          WHERE tax_code is not null
          AND batch_id =  g_new_batch_id
          AND run_sequence_id = g_run_seq_id
          AND process_flag = g_validated
         ) xout;

    --cursor to summarize tax lines
    CURSOR Summ_inv_tax_cur IS
    SELECT  leg_invoice_id , min(xil.leg_line_number) min_line_id, sum(leg_amount) summarized, tax_Code
    FROM xxap_invoice_lines_stg xil
    WHERE leg_line_type_lookup_code = 'TAX'
    AND batch_id =  g_new_batch_id
    AND run_sequence_id = g_run_seq_id
    AND process_flag = g_validated
    GROUP BY leg_invoice_id , tax_Code;

    --v1.11 ends

  BEGIN
    -- Initialize global variables for log_errors
    g_source_table    := g_invoice_t;
    g_intf_staging_id := NULL;
    g_src_keyname1    := NULL;
    g_src_keyvalue1   := NULL;
    g_src_keyname2    := NULL;
    g_src_keyvalue2   := NULL;
    g_src_keyname3    := NULL;
    g_src_keyvalue3   := NULL;
    g_src_keyname4    := NULL;
    g_src_keyvalue4   := NULL;
    g_src_keyname5    := NULL;
    g_src_keyvalue5   := NULL;

    -- Ver 1.3 start

    --ver 1.3 changes start
    FOR coa_rec IN coa_cur LOOP
      l_valerr_cnt    := 1;
      l_err_code      := NULL;
      l_err_msg       := NULL;
      l_rec           := NULL;
      x_rec           := NULL;
      x_status        := NULL;
      x_msg           := NULL;
      l_coa_rec       := NULL;
      l_dist_ccid     := NULL;
      l_msg           := NULL;
      l_gl_valerr_cnt := NULL;
      l_ap_valerr_cnt := NULL;
      l_ap_err_code   := NULL;
      l_ap_err_msg    := NULL;
      l_gl_err_code   := NULL;
      l_gl_err_msg    := NULL;

      BEGIN
        l_rec.concatenated_segments := coa_rec.code_concatenated;
        l_txn_date                  := SYSDATE;
        l_direction                 := 'LEGACY-TO-R12';
        xxetn_coa_mapping_pkg.get_code_combination(l_direction,
                                                   l_ext_system,
                                                   l_txn_date,
                                                   l_rec,
                                                   x_rec,
                                                   x_status,
                                                   x_msg);

        --IF (x_status = fnd_api.g_ret_sts_error)
        IF (x_status = g_coa_error) --Ver.1.4 changes
         THEN
          l_valerr_cnt := 2;
          l_err_code   := 'ETN_AP_INVALID_ACCOUNT_CCID';
          l_err_msg    := 'API Status:' || x_status || 'Error is: ' ||
                          x_msg;
        ELSE
          xxetn_debug_pkg.add_debug('R12 concatenated segments :' ||
                                    x_rec.concatenated_segments);
          l_coa_rec.segment1  := x_rec.segment1;
          l_coa_rec.segment2  := x_rec.segment2;
          l_coa_rec.segment3  := x_rec.segment3;
          l_coa_rec.segment4  := x_rec.segment4;
          l_coa_rec.segment5  := x_rec.segment5;
          l_coa_rec.segment6  := x_rec.segment6;
          l_coa_rec.segment7  := x_rec.segment7;
          l_coa_rec.segment8  := x_rec.segment8;
          l_coa_rec.segment9  := x_rec.segment9;
          l_coa_rec.segment10 := x_rec.segment10;
          xxetn_common_pkg.get_ccid(p_in_segments => l_coa_rec,
                                    p_ccid        => l_dist_ccid,
                                    p_err_msg     => l_msg);
          xxetn_debug_pkg.add_debug('ccid ' || l_dist_ccid || ' / l_msg ' ||
                                    l_msg);

          IF (l_dist_ccid IS NULL) THEN
            l_valerr_cnt := 2;
            xxetn_debug_pkg.add_debug(' Error in deriving CCID. Error is :' ||
                                      l_msg);
            l_err_code := 'ETN_AP_INVALID_ACCOUNT_CCID';
            l_err_msg  := ' Error in deriving CCID. Error is : ' || l_msg;
          ELSE
            IF coa_rec.l_level = 'L' THEN
              UPDATE xxap_invoice_lines_stg
                 SET dist_code_concatenated_id = l_dist_ccid,
                     plant_segment             = l_coa_rec.segment2
               WHERE leg_dist_code_concatenated = coa_rec.code_concatenated
                 AND batch_id = g_new_batch_id
                 AND run_sequence_id = g_run_seq_id
                 AND leg_reference_key1 ='INVH';  --CR 308318
            ELSE
              UPDATE xxap_invoices_stg
                 SET accts_pay_code_combination_id = l_dist_ccid,
                     plant_segment                 = l_coa_rec.segment2
               WHERE leg_accts_pay_code_concat = coa_rec.code_concatenated
                 AND batch_id = g_new_batch_id
                 AND run_sequence_id = g_run_seq_id;
            END IF;
          END IF;
        END IF;
      EXCEPTION
        WHEN OTHERS THEN
          l_err_code := 'ETN_AP_INVALID_ACCOUNT_CCID';
          l_err_msg  := 'ERROR - ' || SUBSTR(SQLERRM, 1, 150);
          xxetn_debug_pkg.add_debug('ERROR - ' || SQLERRM);
          xxetn_debug_pkg.add_debug('Error : Backtace : ' ||
                                    DBMS_UTILITY.format_error_backtrace);
          l_valerr_cnt := 2;
      END;

      IF l_valerr_cnt = 2 THEN
        IF coa_rec.l_level = 'L' THEN
          UPDATE xxap_invoice_lines_stg
             SET process_flag      = 'E',
                 error_type        = 'ERR_VAL',
                 last_updated_date = SYSDATE,
                 last_updated_by   = g_user_id,
                 last_update_login = g_login_id
           WHERE leg_dist_code_concatenated = coa_rec.code_concatenated
             AND batch_id = g_new_batch_id
             AND run_sequence_id = g_run_seq_id
             AND leg_reference_key1 = 'INVH';  --CR 308318;

          FOR r_coa_err_rec IN (SELECT interface_txn_id
                                  FROM xxap_invoice_lines_stg xis
                                 WHERE leg_dist_code_concatenated =
                                       coa_rec.code_concatenated
                                   AND batch_id = g_new_batch_id
                                   AND leg_reference_key1 ='INVH'  --CR 308318
                                   AND run_sequence_id = g_run_seq_id) LOOP
            g_intf_staging_id := r_coa_err_rec.interface_txn_id;
            g_source_table    := g_invoice_line_t;
            log_errors(pov_return_status       => l_log_ret_status -- OUT
                      ,
                       pov_error_msg           => l_log_err_msg -- OUT
                      ,
                       piv_source_column_name  => 'LEG_DIST_CODE_CONCAT',
                       piv_source_column_value => coa_rec.code_concatenated,
                       piv_error_type          => g_err_val,
                       piv_error_code          => l_err_code,
                       piv_error_message       => l_err_msg);
          END LOOP;
        ELSE
          UPDATE xxap_invoices_stg
             SET process_flag      = 'E',
                 error_type        = 'ERR_VAL',
                 last_updated_date = SYSDATE,
                 last_updated_by   = g_user_id,
                 last_update_login = g_login_id
           WHERE leg_accts_pay_code_concat = coa_rec.code_concatenated
             AND batch_id = g_new_batch_id
             AND run_sequence_id = g_run_seq_id;

          FOR r_coa_err_rec IN (SELECT interface_txn_id
                                  FROM xxap_invoices_stg xis
                                 WHERE leg_accts_pay_code_concat =
                                       coa_rec.code_concatenated
                                   AND batch_id = g_new_batch_id
                                   AND run_sequence_id = g_run_seq_id) LOOP
            g_intf_staging_id := r_coa_err_rec.interface_txn_id;
            g_source_table    := g_invoice_t;
            log_errors(pov_return_status       => l_log_ret_status -- OUT
                      ,
                       pov_error_msg           => l_log_err_msg -- OUT
                      ,
                       piv_source_column_name  => 'LEG_ACCTS_PAY_CODE_CONCAT',
                       piv_source_column_value => coa_rec.code_concatenated,
                       piv_error_type          => g_err_val,
                       piv_error_code          => l_err_code,
                       piv_error_message       => l_err_msg);
          END LOOP;
        END IF;
      END IF;
    END LOOP;

    COMMIT;

    --Ver1.2 changes start
    /*
          l_valerr_cnt               := 1;

          FOR org_rec IN org_cur
          LOOP
             l_valerr_cnt               := 1;
             l_err_code                 := NULL;
             l_err_msg                  := NULL;
             l_operating_unit           := NULL;
             l_org_id                   := NULL;
             l_sob_id                   := NULL;
             l_rec_org                  := NULL;
          l_column_name := NULL;
          l_column_value := NULL;

             BEGIN
                --Ver1.2 changes start
                --/*
                BEGIN
                   --Derive R12 value for the given operating unit
                   SELECT description
                     INTO l_operating_unit
                     FROM fnd_lookup_values flv
                    WHERE flv.lookup_type = g_ou_lkp
                      AND flv.meaning = org_rec.leg_operating_unit
                      AND flv.LANGUAGE = USERENV ('LANG')
                      AND TRUNC (SYSDATE) BETWEEN NVL (flv.start_date_active, TRUNC (SYSDATE))
                                              AND NVL (flv.end_date_active, TRUNC (SYSDATE));
                EXCEPTION
                   WHEN NO_DATA_FOUND
                   THEN
                      l_valerr_cnt               := 2;
                      l_operating_unit           := NULL;
            l_column_name := 'PLANT/SITE';
            l_column_value := org_rec.leg_operating_unit;

                      print_log_message ('In No Data found of operating unit lookup check' || SQLERRM);
                      l_err_code                 := 'ETN_AP_INVALID_OPERATING_UNIT';
                      l_err_msg                  := 'Cross reference not found for Operating unit';
                   WHEN OTHERS
                   THEN
                      l_valerr_cnt               := 2;
                      l_operating_unit           := NULL;
            l_column_name := 'PLANT/SITE';
            l_column_value := org_rec.leg_operating_unit;
                      print_log_message ('In When others of operating unit lookup check' || SQLERRM);
                      l_err_code                 := 'ETN_AP_INVALID_OPERATING_UNIT';
                      l_err_msg                  :=
                            'Error while deriving cross reference for Operating unit'
                         || SUBSTR (SQLERRM,
                                    1,
                                    150
                                   );
                END;

                --

                --      l_rec_org.site      := org_rec.plant_segment;
                --      l_operating_unit  := xxetn_map_util.get_value (l_rec_org).operating_unit;
                --Ver 1.2 changes end
                IF l_operating_unit IS NOT NULL
                THEN
                   BEGIN
                      xxetn_debug_pkg.add_debug
                                    (' + PROCEDURE : validate_operating_unit...derivation of org_id + ');

                      --Fetch org_id  for the R12 value of the operating unit derived
                      SELECT hou.organization_id,
                             hou.set_of_books_id
                        INTO l_org_id,
                             l_sob_id
                        FROM hr_operating_units hou
                       WHERE hou.NAME = l_operating_unit
                         AND TRUNC (SYSDATE) BETWEEN NVL (hou.date_from, TRUNC (SYSDATE))
                                                 AND NVL (hou.date_to, TRUNC (SYSDATE));
                   EXCEPTION
                      WHEN NO_DATA_FOUND
                      THEN
              l_column_name := 'R12 Operating unit';
              l_column_value := l_operating_unit;
                         l_valerr_cnt               := 2;
                         l_org_id                   := NULL;
                         print_log_message ('In No Data found of operating unit check' || SQLERRM);
                         l_err_code                 := 'ETN_AP_INVALID_OPERATING_UNIT';
                         l_err_msg                  := 'R12 Operating unit not setup';
                      WHEN OTHERS
                      THEN
                         l_valerr_cnt               := 2;
                         l_org_id                   := NULL;
                         print_log_message ('In When others of operating unit check' || SQLERRM);
              l_column_name := 'R12 Operating unit';
              l_column_value := l_operating_unit;
                         l_err_code                 := 'ETN_AP_INVALID_OPERATING_UNIT';
                         l_err_msg                  :=
                               'Error while deriving/ updating R12 Operating unit '
                            || SUBSTR (SQLERRM,
                                       1,
                                       150
                                      );
                   END;

                   BEGIN
                      --check if the GL period is open for SQLGL
                      SELECT 1
                        INTO l_record_cnt
                        FROM gl_period_statuses gps, fnd_application fa, gl_ledgers gl
                       WHERE gl.accounted_period_type = gps.period_type
                         AND gl.ledger_id = gps.ledger_id
                         AND fa.application_short_name = 'SQLGL'
                         AND fa.application_id = gps.application_id
                         AND gps.set_of_books_id = l_sob_id
                         AND gps.closing_status = 'O'
                         AND g_gl_date BETWEEN gps.start_date AND gps.end_date;
                   EXCEPTION
                      WHEN NO_DATA_FOUND
                      THEN
                         l_gl_valerr_cnt               := 2;
                         print_log_message ('In No Data found of gl period check' || SQLERRM);
                         l_gl_err_code                 := 'ETN_AP_INVALID_GL_PERIOD';
                         l_gl_err_msg                  := 'GL Period is not open.';
                      WHEN OTHERS
                      THEN
                         l_gl_valerr_cnt               := 2;
                         print_log_message ('In When others of gl period check' || SQLERRM);
                         l_gl_err_code                 := 'ETN_AP_INVALID_GL_PERIOD';
                         l_gl_err_msg                  :=
                               'Error while checking if GL Period is open.' || SUBSTR (SQLERRM,
                                                                                       1,
                                                                                       150
                                                                                      );
                   END;

                   BEGIN
                      --Check if the GL period is open for AP
                      SELECT 1
                        INTO l_record_cnt
                        FROM gl_period_statuses gps, fnd_application fa, gl_ledgers gl
                       WHERE gl.accounted_period_type = gps.period_type
                         AND gl.ledger_id = gps.ledger_id
                         AND fa.application_short_name = 'SQLAP'
                         AND fa.application_id = gps.application_id
                         AND gps.set_of_books_id = l_sob_id
                         AND gps.closing_status = 'O'
                         AND g_gl_date BETWEEN gps.start_date AND gps.end_date;
                   EXCEPTION
                      WHEN NO_DATA_FOUND
                      THEN
                         l_ap_valerr_cnt               := 2;
                         print_log_message ('In No Data found of AP period check' || SQLERRM);
                         l_ap_err_code                 := 'ETN_AP_INVALID_AP_PERIOD';
                         l_ap_err_msg                  := 'AP Period is not open.';
                      WHEN OTHERS
                      THEN
                         l_ap_valerr_cnt               := 2;
                         print_log_message ('In When others of AP period check' || SQLERRM);
                         l_ap_err_code                 := 'ETN_AP_INVALID_AP_PERIOD';
                         l_ap_err_msg                  :=
                               'Error while checking if AP Period is open.' || SUBSTR (SQLERRM,
                                                                                       1,
                                                                                       150
                                                                                      );
                   END;
                ELSE
                   l_valerr_cnt               := 2;
                   l_err_code                 := 'ETN_AP_INVALID_OPERATING_UNIT';
                   l_err_msg                  := 'Cross reference not found for Operating unit';
                END IF;

                xxetn_debug_pkg.add_debug ('Operating Unit = ' || l_operating_unit);
                xxetn_debug_pkg.add_debug ('Org Id = ' || l_org_id);
                xxetn_debug_pkg.add_debug ('SOB Id = ' || l_sob_id);

     --           IF l_valerr_cnt <> 2
    --            THEN
                   UPDATE xxap_invoices_stg
                      SET org_id = l_org_id
    --                   , organization_name = l_operating_unit
                   WHERE  leg_operating_unit  = org_rec.leg_operating_unit
    --               AND leg_vendor_num = piv_vendor_num
                      AND batch_id = g_new_batch_id
                      AND run_sequence_id = g_run_seq_id;

                   UPDATE xxap_invoice_lines_stg
                      SET org_id = l_org_id,
                          organization_name = l_operating_unit
                    WHERE leg_operating_unit  = org_rec.leg_operating_unit
                      AND batch_id = g_new_batch_id
                      AND run_sequence_id = g_run_seq_id;
    --            END IF;
             EXCEPTION
                WHEN OTHERS
                THEN
                   l_err_code                 := 'ETN_AP_INVALID_OPERATING_UNIT';
                   l_err_msg                  := 'ERROR - ' || SUBSTR (SQLERRM,
                                                                       1,
                                                                       150
                                                                      );
                   l_valerr_cnt               := 2;
             END;

             IF l_valerr_cnt = 2 OR l_gl_valerr_cnt = 2 OR l_ap_valerr_cnt = 2
             THEN
                UPDATE xxap_invoices_stg
                   SET process_flag = 'E',
                       error_type = 'ERR_VAL',
                       last_updated_date = SYSDATE,
                       last_updated_by = g_user_id,
                       last_update_login = g_login_id
                 WHERE leg_operating_unit  = org_rec.leg_operating_unit
    --               AND leg_vendor_num = piv_vendor_num
                   AND batch_id = g_new_batch_id
                   AND run_sequence_id = g_run_seq_id;

                UPDATE xxap_invoice_lines_stg
                   SET process_flag = 'E',
                       error_type = 'ERR_VAL',
                       last_updated_date = SYSDATE,
                       last_updated_by = g_user_id,
                       last_update_login = g_login_id
                 WHERE leg_operating_unit  = org_rec.leg_operating_unit
                   AND batch_id = g_new_batch_id
                   AND run_sequence_id = g_run_seq_id;

                FOR r_org_err_rec IN (SELECT interface_txn_id
                                        FROM xxap_invoices_stg xis
                                       WHERE leg_operating_unit  = org_rec.leg_operating_unit
                                         AND batch_id = g_new_batch_id
                                         AND run_sequence_id = g_run_seq_id)
                LOOP
                   g_intf_staging_id          := r_org_err_rec.interface_txn_id;
                   g_source_table             := g_invoice_t;
             IF l_valerr_cnt = 2 THEN
               log_errors (pov_return_status            => l_log_ret_status                   -- OUT
                                             ,
                     pov_error_msg                => l_log_err_msg                      -- OUT
                                          ,
                     piv_source_column_name       => l_column_name,
                     piv_source_column_value      => l_column_value,
                     piv_error_type               => g_err_val,
                     piv_error_code               => l_err_code,
                     piv_error_message            => l_err_msg
                    );
            END IF;
            IF l_gl_valerr_cnt = 2 THEN
                    log_errors (pov_return_status            => l_log_ret_status                   -- OUT
                                             ,
                     pov_error_msg                => l_log_err_msg                      -- OUT
                                          ,
                     piv_source_column_name       => 'GL Date',
                     piv_source_column_value      => g_gl_date,
                     piv_error_type               => g_err_val,
                     piv_error_code               => l_gl_err_code,
                     piv_error_message            => l_gl_err_msg
                    );

            END IF;

            IF l_ap_valerr_cnt = 2 THEN
                    log_errors (pov_return_status            => l_log_ret_status                   -- OUT
                                             ,
                     pov_error_msg                => l_log_err_msg                      -- OUT
                                          ,
                     piv_source_column_name       => 'GL Date',
                     piv_source_column_value      => g_gl_date,
                     piv_error_type               => g_err_val,
                     piv_error_code               => l_ap_err_code,
                     piv_error_message            => l_ap_err_msg
                    );
            END IF;
                END LOOP;
             END IF;
          END LOOP;


          COMMIT;
    */
    --ver1.2 changes
    --/*
    l_valerr_cnt := 1;

    --Changes for CR 308318 start here
    FOR coa_rec IN coa_cur_ninvh LOOP
       l_valerr_cnt    := 1;
       l_err_code      := NULL;
       l_err_msg       := NULL;
       l_rec_ninvh     := NULL;
       x_rec_ninvh     := NULL;
       x_status_ninvh  := NULL;
       x_msg_ninvh     := NULL;
       l_derived_segment3   :=NULL;
       l_derived_Segment7   :=NULL;
       l_coa_rec_ninvh      :=NULL;

       l_rec_ninvh.segment1 := SUBSTR(coa_rec.leg_code_concatenated,1,
                            instr(coa_rec.leg_code_concatenated,'.',1,1)-1
                                  );
       l_rec_ninvh.segment2 := SUBSTR(coa_rec.leg_code_concatenated,
                           instr(coa_rec.leg_code_concatenated,'.',1,1)+1,
                              instr(coa_rec.leg_code_concatenated,'.',1,2)-
                                 instr(coa_rec.leg_code_concatenated,'.',1,1)-1);
       l_rec_ninvh.segment3 := SUBSTR(coa_rec.leg_code_concatenated,
                                       instr(coa_rec.leg_code_concatenated,'.',1,2)+1,
                                          instr(coa_rec.leg_code_concatenated,'.',1,3)-
                                             instr(coa_rec.leg_code_concatenated,'.',1,2)-1);
       l_rec_ninvh.segment4 := SUBSTR(coa_rec.leg_code_concatenated,
                                       instr(coa_rec.leg_code_concatenated,'.',1,3)+1,
                                          instr(coa_rec.leg_code_concatenated,'.',1,4)-
                                             instr(coa_rec.leg_code_concatenated,'.',1,3)-1);
       l_rec_ninvh.segment5 := SUBSTR(coa_rec.leg_code_concatenated,
                                       instr(coa_rec.leg_code_concatenated,'.',1,4)+1,
                                          instr(coa_rec.leg_code_concatenated,'.',1,5)-
                                             instr(coa_rec.leg_code_concatenated,'.',1,4)-1);
       l_rec_ninvh.segment6 := SUBSTR(coa_rec.leg_code_concatenated,
                                       instr(coa_rec.leg_code_concatenated,'.',1,5)+1,
                                          instr(coa_rec.leg_code_concatenated,'.',1,6)-
                                             instr(coa_rec.leg_code_concatenated,'.',1,5)-1);
       l_rec_ninvh.segment7 := /*SUBSTR(coa_rec.leg_code_concatenated,
                                        instr(coa_rec.leg_code_concatenated,'.',-1)+1)*/
                               SUBSTR(coa_rec.leg_code_concatenated,
                                       instr(coa_rec.leg_code_concatenated,'.',1,6)+1
                                         ) ;
--today
       fnd_file.put_line(fnd_file.log,'Broken leg values');
       fnd_file.put_line(fnd_file.log,l_rec_ninvh.segment1||'-'||
          l_rec_ninvh.segment2||'-'||
          l_rec_ninvh.segment3||'-'||
          l_rec_ninvh.segment4||'-'||
          l_rec_ninvh.segment5||'-'||
          l_rec_ninvh.segment6||'-'||
          l_rec_ninvh.segment7);
 --today
       IF l_rec_ninvh.segment1 IS NULL OR
          l_rec_ninvh.segment2 IS NULL OR
          l_rec_ninvh.segment3 IS NULL OR
          l_rec_ninvh.segment4 IS NULL OR
          l_rec_ninvh.segment5 IS NULL OR
          l_rec_ninvh.segment6 IS NULL OR
          l_rec_ninvh.segment7 IS NULL
       THEN
          l_err_code := 'ETN_AP_INVNH_SEGMENT_DERIVE';
          l_err_msg  := 'ERROR - ' ||'Could Not Derive Segment1 To Segment7 From LEG DIST CODE Value';
          xxetn_debug_pkg.add_debug('ERROR - ' || 'Could Not Derive Segment1 To Segment7 From LEG DIST CODE Value');
          xxetn_debug_pkg.add_debug('Error : Backtace : ' ||
                                    DBMS_UTILITY.format_error_backtrace);
          l_valerr_cnt := 2;
       END IF;
      IF l_valerr_cnt !=2

      THEN

         BEGIN
            SELECT SUBSTR(flv.attribute3,
                      instr(flv.attribute3,'.',1,7)+1,
                         instr(flv.attribute3,'.',1,8)-
                            instr(flv.attribute3,'.',1,7)-1),
                   SUBSTR(flv.attribute3,
                      instr(flv.attribute3,'.',1,2)+1,
                         instr(flv.attribute3,'.',1,3)-
                            instr(flv.attribute3,'.',1,2)-1)    --its segment8 which is updated in 11i segments as segment7
            INTO l_derived_segment7, l_derived_Segment3
            FROM    fnd_lookup_Values_vl flv
            WHERE lookup_type= 'ETN_PTP_INVOICE_NO_HLD_MAPPING'
            AND   meaning = 'INV NOT ON HLD'
            AND   attribute_category = 'ETN_PTP_INVOICE_NO_HLD_MAPPING'
            AND attribute2 = l_rec_ninvh.segment3
            AND flv.enabled_flag ='Y'
            AND TRUNC(SYSDATE) BETWEEN
                 TRUNC(NVL(flv.start_date_active, SYSDATE)) AND
                 TRUNC(NVL(flv.end_date_active, SYSDATE));
         --AND flv.LANGUAGE = USERENV('LANG');

            IF  l_derived_segment3 IS NULL OR l_derived_Segment7 IS NULL
            THEN
               l_err_code := 'ETN_AP_INVNH_ACCOUNT_DERIVE';
               l_err_msg  := 'ERROR - ' ||'Could Not Derive Segment3/Segment7 From ETN_PTP_INVOICE_NO_HLD_MAPPING';
               xxetn_debug_pkg.add_debug('ERROR - ' || 'Could Not Derive Segment3/Segment7 From ETN_PTP_INVOICE_NO_HLD_MAPPING');
               xxetn_debug_pkg.add_debug('Error : Backtace : ' ||
                                    DBMS_UTILITY.format_error_backtrace);
               l_valerr_cnt := 2;
            END IF;

         EXCEPTION
            WHEN no_data_found
            THEN
               l_derived_segment3 := l_rec_ninvh.segment3;
               l_derived_Segment7 := l_rec_ninvh.segment7;


            WHEN OTHERS
               THEN
                  l_err_code := 'ETN_AP_INVNH_ACCOUNT_DERIVE';
                  l_err_msg  := 'ERROR - ' || SUBSTR(SQLERRM, 1, 150);
                  xxetn_debug_pkg.add_debug('ERROR - ' || SQLERRM);
                  xxetn_debug_pkg.add_debug('Error : Backtace : ' ||
                                    DBMS_UTILITY.format_error_backtrace);
                  l_valerr_cnt := 2;

         END;

         BEGIN
            l_txn_date                  := SYSDATE;
            l_direction                 := 'LEGACY-TO-R12';
            l_rec_ninvh.segment3 := l_derived_segment3;
            l_rec_ninvh.segment7 := l_derived_Segment7;
            xxetn_coa_mapping_pkg.get_code_combination(l_direction,
                                                       l_ext_system,
                                                       l_txn_date,
                                                       l_rec_ninvh,
                                                       x_rec_ninvh,
                                                       x_status_ninvh,
                                                       x_msg_ninvh);

        --IF (x_status = fnd_api.g_ret_sts_error)

            IF (x_status_ninvh = g_coa_error) --Ver.1.4 changes
               THEN
               l_valerr_cnt := 2;
               l_err_code   := 'ETN_AP_INVALID_ACCOUNT_CCID_INVNH';
               l_err_msg    := 'API Status:' || x_status || 'Error is: ' ||
                          x_msg_ninvh;
            ELSE

               xxetn_debug_pkg.add_debug('R12 concatenated segments :' ||
                                    x_rec_ninvh.concatenated_segments);

               l_coa_rec_ninvh.segment1  := x_rec_ninvh.segment1;
               l_coa_rec_ninvh.segment2  := x_rec_ninvh.segment2;
               l_coa_rec_ninvh.segment3  := x_rec_ninvh.segment3;
               l_coa_rec_ninvh.segment4  := x_rec_ninvh.segment4;
               l_coa_rec_ninvh.segment5  := x_rec_ninvh.segment5;
               l_coa_rec_ninvh.segment6  := x_rec_ninvh.segment6;
               l_coa_rec_ninvh.segment7  := x_rec_ninvh.segment7;
               l_coa_rec_ninvh.segment8  := x_rec_ninvh.segment8;
               l_coa_rec_ninvh.segment9  := x_rec_ninvh.segment9;
               l_coa_rec_ninvh.segment10 := x_rec_ninvh.segment10;
               --today
               fnd_file.put_line(fnd_file.log,'passed into getccid');
               fnd_file.put_line(fnd_file.log,l_coa_rec_ninvh.segment1||'-'||
               l_coa_rec_ninvh.segment2||'-'||
               l_coa_rec_ninvh.segment3||'-'||
               l_coa_rec_ninvh.segment4||'-'||
               l_coa_rec_ninvh.segment5||'-'||
               l_coa_rec_ninvh.segment6||'-'||
               l_coa_rec_ninvh.segment7||'-'||
               l_coa_rec_ninvh.segment8||'-'||
               l_coa_rec_ninvh.segment9||'-'||
               l_coa_rec_ninvh.segment10 );
               --today
               xxetn_common_pkg.get_ccid(p_in_segments => l_coa_rec_ninvh,
                                         p_ccid        => l_dist_ccid_ninvh,
                                         p_err_msg     => l_msg);
               xxetn_debug_pkg.add_debug('ccid ' || l_dist_ccid_ninvh || ' / l_msg ' ||
                                    l_msg);
               --today
               fnd_file.put_line(fnd_file.log,l_dist_ccid_ninvh);
               --today
               IF (l_dist_ccid_ninvh IS NULL) THEN
                  l_valerr_cnt := 2;
                  xxetn_debug_pkg.add_debug(' Error in deriving CCID. Error is :' ||
                                      l_msg);
                  l_err_code := 'ETN_AP_INVALID_ACCOUNT_CCID_INVNH';
                  l_err_msg  := ' Error in deriving CCID. Error is : ' || l_msg;
               ELSE
                  UPDATE xxap_invoice_lines_stg
                  SET dist_code_concatenated_id = l_dist_ccid_ninvh,
                      plant_segment             = l_coa_rec_ninvh.segment2
                  WHERE leg_dist_code_concatenated = coa_rec.leg_code_concatenated
                  AND batch_id = g_new_batch_id
                  AND run_sequence_id = g_run_seq_id
                  AND leg_reference_key1 <> 'INVH';  --CR 308318
                  COMMIT;  -- Added by Kulraj
               END IF;
            END IF;
         EXCEPTION
            WHEN OTHERS THEN
               l_err_code := 'ETN_AP_INVALID_ACCOUNT_CCID_INVNH';
               l_err_msg  := 'ERROR - ' || SUBSTR(SQLERRM, 1, 150);
               xxetn_debug_pkg.add_debug('ERROR - ' || SQLERRM);
               xxetn_debug_pkg.add_debug('Error : Backtace : ' ||
                                    DBMS_UTILITY.format_error_backtrace);
               l_valerr_cnt := 2;
         END;
      END IF;
         IF l_valerr_cnt = 2 THEN
            UPDATE xxap_invoice_lines_stg
            SET process_flag      = 'E',
                 error_type        = 'ERR_VAL',
                 last_updated_date = SYSDATE,
                 last_updated_by   = g_user_id,
                 last_update_login = g_login_id
            WHERE leg_dist_code_concatenated = coa_rec.leg_code_concatenated
            AND batch_id = g_new_batch_id
            AND run_sequence_id = g_run_seq_id
            AND leg_reference_key1 <> 'INVH';
            COMMIT;  -- Added by Kulraj

          FOR r_coa_err_rec IN (SELECT interface_txn_id
                                  FROM xxap_invoice_lines_stg xils
                                 WHERE leg_dist_code_concatenated =
                                       coa_rec.leg_code_concatenated
                                   AND batch_id = g_new_batch_id
                                   AND run_sequence_id = g_run_seq_id
                                   AND leg_reference_key1 <> 'INVH') LOOP
             g_intf_staging_id := r_coa_err_rec.interface_txn_id;
             g_source_table    := g_invoice_line_t;
             log_errors(pov_return_status       => l_log_ret_status -- OUT
                        ,
                        pov_error_msg           => l_log_err_msg -- OUT
                        ,
                        piv_source_column_name  => 'LEG_DIST_CODE_CONCAT_INVNH',
                        piv_source_column_value => coa_rec.leg_code_concatenated,
                        piv_error_type          => g_err_val,
                        piv_error_code          => l_err_code,
                        piv_error_message       => l_err_msg);
      END LOOP;
      END IF;
      END LOOP;

      COMMIT;
      l_valerr_cnt := 1;
    --Changes for CR 308318 end here

    FOR org_rec IN org_cur LOOP
      l_valerr_cnt     := 1;
      l_err_code       := NULL;
      l_err_msg        := NULL;
      l_operating_unit := NULL;
      l_org_id         := NULL;
      l_sob_id         := NULL;
      l_rec_org        := NULL;

      BEGIN
        l_rec_org.site   := org_rec.plant_segment;
        l_operating_unit := xxetn_map_util.get_value(l_rec_org)
                            .operating_unit;
            IF l_operating_unit IS NOT NULL THEN
          BEGIN
            xxetn_debug_pkg.add_debug(' + PROCEDURE : validate_operating_unit...derivation of org_id + ');

            --Fetch org_id  for the R12 value of the operating unit derived
            SELECT hou.organization_id, hou.set_of_books_id
              INTO l_org_id, l_sob_id
              FROM hr_operating_units hou
             WHERE hou.NAME = l_operating_unit
               AND TRUNC(SYSDATE) BETWEEN
                   NVL(hou.date_from, TRUNC(SYSDATE)) AND
                   NVL(hou.date_to, TRUNC(SYSDATE));

          EXCEPTION
            WHEN NO_DATA_FOUND THEN
              l_valerr_cnt := 2;
              l_org_id     := NULL;
              print_log_message('In No Data found of operating unit check' ||
                                SQLERRM);
              l_err_code := 'ETN_AP_INVALID_OPERATING_UNIT';
              l_err_msg  := 'R12 Operating unit not setup';
            WHEN OTHERS THEN
              l_valerr_cnt := 2;
              l_org_id     := NULL;
              print_log_message('In When others of operating unit check' ||
                                SQLERRM);
              l_err_code := 'ETN_AP_INVALID_OPERATING_UNIT';
              l_err_msg  := 'Error while deriving/ updating R12 Operating unit ' ||
                            SUBSTR(SQLERRM, 1, 150);
          END;


          BEGIN
            --check if the GL period is open for SQLGL
            SELECT 1
              INTO l_record_cnt
              FROM gl_period_statuses gps,
                   fnd_application    fa,
                   gl_ledgers         gl
             WHERE gl.accounted_period_type = gps.period_type
               AND gl.ledger_id = gps.ledger_id
               AND fa.application_short_name = 'SQLGL'
               AND fa.application_id = gps.application_id
               AND gps.set_of_books_id = l_sob_id
               AND gps.closing_status = 'O'
               AND g_gl_date BETWEEN gps.start_date AND gps.end_date
               AND ADJUSTMENT_PERIOD_FLAG= 'N';  --defect 2154
          EXCEPTION
            WHEN NO_DATA_FOUND THEN
              l_valerr_cnt := 2;
              print_log_message('In No Data found of gl period check' ||
                                SQLERRM);
              l_err_code := 'ETN_AP_INVALID_GL_PERIOD';
              l_err_msg  := 'GL Period is not open.';
            WHEN OTHERS THEN
              l_valerr_cnt := 2;
              print_log_message('In When others of gl period check' ||
                                SQLERRM);
              l_err_code := 'ETN_AP_INVALID_GL_PERIOD';
              l_err_msg  := 'Error while checking if GL Period is open.' ||
                            SUBSTR(SQLERRM, 1, 150);
          END;

          BEGIN
            --Check if the GL period is open for AP
            SELECT 1
              INTO l_record_cnt
              FROM gl_period_statuses gps,
                   fnd_application    fa,
                   gl_ledgers         gl
             WHERE gl.accounted_period_type = gps.period_type
               AND gl.ledger_id = gps.ledger_id
               AND fa.application_short_name = 'SQLAP'
               AND fa.application_id = gps.application_id
               AND gps.set_of_books_id = l_sob_id
               AND gps.closing_status = 'O'
               AND g_gl_date BETWEEN gps.start_date AND gps.end_date
               AND ADJUSTMENT_PERIOD_FLAG= 'N';  --defect 2154
          EXCEPTION
            WHEN NO_DATA_FOUND THEN
              l_valerr_cnt := 2;
              print_log_message('In No Data found of AP period check' ||
                                SQLERRM);
              l_err_code := 'ETN_AP_INVALID_AP_PERIOD';
              l_err_msg  := 'AP Period is not open.';
            WHEN OTHERS THEN
              l_valerr_cnt := 2;
              print_log_message('In When others of AP period check' ||
                                SQLERRM);
              l_err_code := 'ETN_AP_INVALID_AP_PERIOD';
              l_err_msg  := 'Error while checking if AP Period is open.' ||
                            SUBSTR(SQLERRM, 1, 150);
          END;
        ELSE
          l_valerr_cnt := 2;
          l_err_code   := 'ETN_AP_INVALID_OPERATING_UNIT';
          l_err_msg    := 'Cross reference not found for Operating unit';
        END IF;

        xxetn_debug_pkg.add_debug('Operating Unit = ' || l_operating_unit);
        xxetn_debug_pkg.add_debug('Org Id = ' || l_org_id);
        xxetn_debug_pkg.add_debug('SOB Id = ' || l_sob_id);

        IF l_valerr_cnt <> 2 THEN
          UPDATE xxap_invoices_stg
             SET org_id = l_org_id
          --                   , organization_name = l_operating_unit
           WHERE plant_segment = org_rec.plant_segment
                --               AND leg_vendor_num = piv_vendor_num
             AND batch_id = g_new_batch_id
             AND run_sequence_id = g_run_seq_id;

          UPDATE xxap_invoice_lines_stg
             SET org_id = l_org_id, organization_name = l_operating_unit
           WHERE plant_segment = org_rec.plant_segment
             AND batch_id = g_new_batch_id
             AND run_sequence_id = g_run_seq_id;
        END IF;
      EXCEPTION
        WHEN OTHERS THEN
          l_err_code   := 'ETN_AP_INVALID_OPERATING_UNIT';
          l_err_msg    := 'ERROR - ' || SUBSTR(SQLERRM, 1, 150);
          l_valerr_cnt := 2;
      END;

      IF l_valerr_cnt = 2 THEN
        UPDATE xxap_invoices_stg
           SET process_flag      = 'E',
               error_type        = 'ERR_VAL',
               last_updated_date = SYSDATE,
               last_updated_by   = g_user_id,
               last_update_login = g_login_id
         WHERE plant_segment = org_rec.plant_segment
              --               AND leg_vendor_num = piv_vendor_num
           AND batch_id = g_new_batch_id
           AND run_sequence_id = g_run_seq_id;

        UPDATE xxap_invoice_lines_stg
           SET process_flag      = 'E',
               error_type        = 'ERR_VAL',
               last_updated_date = SYSDATE,
               last_updated_by   = g_user_id,
               last_update_login = g_login_id
         WHERE plant_segment = org_rec.plant_segment
           AND batch_id = g_new_batch_id
           AND run_sequence_id = g_run_seq_id;

        FOR r_org_err_rec IN (SELECT interface_txn_id
                                FROM xxap_invoice_lines_stg xis
                               WHERE plant_segment = org_rec.plant_segment
                                 AND batch_id = g_new_batch_id
                                 AND run_sequence_id = g_run_seq_id) LOOP
          g_intf_staging_id := r_org_err_rec.interface_txn_id;
          g_source_table    := g_invoice_t;
          log_errors(pov_return_status       => l_log_ret_status -- OUT
                    ,
                     pov_error_msg           => l_log_err_msg -- OUT
                    ,
                     piv_source_column_name  => 'PLANT/SITE',
                     piv_source_column_value => org_rec.plant_segment,
                     piv_error_type          => g_err_val,
                     piv_error_code          => l_err_code,
                     piv_error_message       => l_err_msg);
        END LOOP;
      END IF;
    END LOOP;
    COMMIT;
    --Ver1.2 changes end
    l_valerr_cnt := 1;
  --v1.10 Changes start
    /*FOR vendor_rec IN vendor_cur LOOP
      IF vendor_rec.leg_vendor_num IS NOT NULL THEN
        BEGIN
          l_valerr_cnt    := 1;
          l_suppl_site_id := NULL;
          l_supp_id       := NULL;
          l_supp_name     := NULL;

          --validate vendor and derive vendor ID
          SELECT DISTINCT apsa.vendor_id, aps.vendor_name
            INTO l_supp_id, l_supp_name
            FROM apps.ap_supplier_sites_all apsa, apps.ap_suppliers aps
           WHERE (aps.segment1) = (vendor_rec.leg_vendor_num)
             AND apsa.vendor_id = aps.vendor_id
             AND apsa.org_id = vendor_rec.org_id
             AND NVL(apsa.inactive_date, SYSDATE) >= SYSDATE
             AND NVL(aps.end_date_active, SYSDATE) >= SYSDATE;
        EXCEPTION
          WHEN NO_DATA_FOUND THEN
            print_log_message('In No Data found of validate supplier check');
            l_valerr_cnt := 2;
            l_err_code   := 'ETN_AP_INVALID_SUPPLIER';
            l_err_msg    := 'Supplier is not set up in R12';
          WHEN OTHERS THEN
            print_log_message('In When others of validate supplier check' ||
                              SUBSTR(SQLERRM, 1, 150));
            l_valerr_cnt := 2;
            l_err_code   := 'ETN_AP_INVALID_SUPPLIER';
            l_err_msg    := 'Error while validating supplier ' ||
                            SUBSTR(SQLERRM, 1, 150);
        END;
      END IF;
      IF l_supp_id IS NOT NULL THEN
        -- Deriving vendor_Site_code for the vendor id derived above
        BEGIN
          SELECT apsa.vendor_site_id
            INTO l_suppl_site_id
            FROM apps.ap_supplier_sites_all apsa, apps.ap_suppliers aps
           WHERE aps.vendor_id = l_supp_id
             AND apsa.vendor_id = aps.vendor_id
             AND apsa.vendor_site_code = vendor_rec.leg_vendor_site_code
             AND NVL(apsa.inactive_date, SYSDATE) >= SYSDATE
             AND NVL(aps.end_date_active, SYSDATE) >= SYSDATE
             AND apsa.org_id = vendor_rec.org_id;
        EXCEPTION
          WHEN NO_DATA_FOUND THEN
            l_valerr_cnt := 2;
            print_log_message('In No Data found of validate vendor site check' ||
                              SQLERRM);
            l_err_code := 'ETN_AP_INVALID_SUPPLIER_SITE';
            l_err_msg  := 'Supplier site is not set up in R12';
          WHEN OTHERS THEN
            l_valerr_cnt := 2;
            print_log_message('In When others of validate vendor site check' ||
                              SQLERRM);
            l_err_code := 'ETN_AP_INVALID_SUPPLIER_SITE';
            l_err_msg  := 'Supplier site is not set up in R12';
        END;
      END IF;

      print_log_message(' -  PROCEDURE : validate_supplier_details   - ');

      IF l_valerr_cnt = 2 THEN
        UPDATE xxap_invoices_stg
           SET process_flag      = 'E',
               error_type        = 'ERR_VAL',
               last_updated_date = SYSDATE,
               last_updated_by   = g_user_id,
               last_update_login = g_login_id
         WHERE leg_vendor_num = vendor_rec.leg_vendor_num
           AND leg_vendor_site_code = vendor_rec.leg_vendor_site_code
           AND batch_id = g_new_batch_id
           AND run_sequence_id = g_run_seq_id;

        FOR r_vendor_err_rec IN (SELECT interface_txn_id
                                   FROM xxap_invoices_stg xis
                                  WHERE leg_vendor_num =
                                        vendor_rec.leg_vendor_num
                                    AND leg_vendor_site_code =
                                        vendor_rec.leg_vendor_site_code
                                    AND batch_id = g_new_batch_id
                                    AND run_sequence_id = g_run_seq_id) LOOP
          g_intf_staging_id := r_vendor_err_rec.interface_txn_id;
          g_source_table    := g_invoice_t;
          log_errors(pov_return_status       => l_log_ret_status -- OUT
                    ,
                     pov_error_msg           => l_log_err_msg -- OUT
                    ,
                     piv_source_column_name  => 'VENDOR_NUM/VENDOR_SITE',
                     piv_source_column_value => vendor_rec.leg_vendor_num ||
                                                ' / ' ||
                                                vendor_rec.leg_vendor_site_code,
                     piv_error_type          => g_err_val,
                     piv_error_code          => l_err_code,
                     piv_error_message       => l_err_msg);
        END LOOP;
      END IF;
    END LOOP;
    COMMIT;
    l_valerr_cnt := 1;*/

    FOR vendor_rec IN vendor_cur LOOP
      IF vendor_rec.leg_vendor_num IS NOT NULL THEN
         BEGIN
            l_valerr_cnt    := 1;
            l_suppl_site_id := NULL;
            l_supp_id       := NULL;
            l_supp_name     := NULL;

               --validate vendor and derive vendor ID
            SELECT  aps.vendor_id, aps.vendor_name
            INTO l_supp_id, l_supp_name
            FROM  apps.ap_suppliers aps
            WHERE (aps.segment1) = (vendor_rec.leg_vendor_num)
            AND NVL(aps.end_date_active, SYSDATE) >= SYSDATE;

         EXCEPTION
          WHEN NO_DATA_FOUND THEN
             print_log_message('In No Data found of validate supplier check');
             l_valerr_cnt := 2;
             l_err_code   := 'ETN_AP_INVALID_SUPPLIER';
             l_err_msg    := 'Supplier is not set up in R12';
          WHEN OTHERS THEN
             print_log_message('In When others of validate supplier check' ||
                              SUBSTR(SQLERRM, 1, 150));
             l_valerr_cnt := 2;
             l_err_code   := 'ETN_AP_INVALID_SUPPLIER';
             l_err_msg    := 'Error while validating supplier ' ||
                            SUBSTR(SQLERRM, 1, 150);
         END;
      END IF;
      IF l_supp_id IS NOT NULL THEN
        -- Deriving vendor_Site_code for the vendor id derived above
       IF vendor_rec.org_id IS NOT NULL
            THEN
        BEGIN
          SELECT apsa.vendor_site_id
            INTO l_suppl_site_id
            FROM apps.ap_supplier_sites_all apsa, apps.ap_suppliers aps
           WHERE aps.vendor_id = l_supp_id
             AND apsa.vendor_id = aps.vendor_id
             AND apsa.vendor_site_code = vendor_rec.leg_vendor_site_code
             AND NVL(apsa.inactive_date, SYSDATE) >= SYSDATE
             AND NVL(aps.end_date_active, SYSDATE) >= SYSDATE
             AND apsa.org_id = vendor_rec.org_id;

        EXCEPTION
          WHEN NO_DATA_FOUND THEN
             BEGIN
                SELECT apsa.vendor_site_id, aps.vendor_id
                INTO l_src_ven_site_id , l_src_ven_id
                FROM ap_supplier_sites_all apsa, ap_suppliers aps
                WHERE apsa.vendor_site_code = vendor_rec.leg_vendor_site_code
                AND apsa.vendor_id = aps.vendor_id
                AND apsa.pay_site_flag = 'Y'
                AND NVL(apsa.inactive_date, SYSDATE) >= SYSDATE
                AND NVL(aps.end_date_active, SYSDATE) >= SYSDATE
                AND aps.vendor_id = l_supp_id
                AND ROWNUM<2;

                IF vendor_rec.accts_pay_code_combination_id IS NOT NULL
                THEN
                   Create_new_site(l_src_ven_id,
                                   l_src_ven_site_id ,
                                   vendor_rec.accts_pay_code_combination_id,
                                   vendor_rec.org_id,
                                   vendor_rec.leg_vendor_site_code,
                                   l_new_vensite_id,
                                   l_suppl_err_code,
                                   l_suppl_err_msg);

                    l_err_code := l_suppl_err_code;
                    l_err_msg  := l_suppl_err_msg;

                    IF l_err_code IS NOT NULL
                    THEN
                    --fnd_file.put_line(fnd_file.log, 'l_err_code for crt_new_site'||l_err_code);

                    UPDATE xxap_invoices_stg
                    SET process_flag      = 'E',
                        error_type        = 'ERR_VAL',
                        last_updated_date = SYSDATE,
                        last_updated_by   = g_user_id,
                        last_update_login = g_login_id
                    WHERE leg_vendor_num = vendor_rec.leg_vendor_num
                    AND leg_vendor_site_code = vendor_rec.leg_vendor_site_code
                    AND accts_pay_code_combination_id = vendor_rec.accts_pay_code_combination_id
                    AND batch_id = g_new_batch_id
                    AND run_sequence_id = g_run_seq_id
                    AND org_id = vendor_rec.org_id;

                    FOR r_vendor_err_rec IN (SELECT interface_txn_id
                                           FROM xxap_invoices_stg xis
                                           WHERE leg_vendor_num =
                                           vendor_rec.leg_vendor_num
                                           AND leg_vendor_site_code =
                                           vendor_rec.leg_vendor_site_code
                                           AND batch_id = g_new_batch_id
                                           AND run_sequence_id = g_run_seq_id
                                           AND org_id = vendor_rec.org_id
                                           AND accts_pay_code_combination_id = vendor_rec.accts_pay_code_combination_id
                                           ) LOOP
                       g_intf_staging_id := r_vendor_err_rec.interface_txn_id;
                       g_source_table    := g_invoice_t;
                       log_errors(pov_return_status       => l_log_ret_status -- OUT
                                  ,
                                  pov_error_msg           => l_log_err_msg -- OUT
                                  ,
                                  piv_source_column_name  => 'VENDOR_NUM/VENDOR_SITE',
                                  piv_source_column_value => vendor_rec.leg_vendor_num ||
                                                         ' / ' ||
                                                    vendor_rec.leg_vendor_site_code,
                                  piv_error_type          => g_err_val,
                                  piv_error_code          => l_err_code,
                                  piv_error_message       => l_err_msg);
                    END LOOP;
                    l_valerr_cnt := 1;
                    END IF;
                ELSE
                   --when liability account R12 is not derived
                   l_valerr_cnt := 2;
                   print_log_message('vendor_rec.accts_pay_code_combination_id IS  NULL' ||
                              SQLERRM);
                   l_err_code := 'ETN_AP_CREATE_INV_SUPP_SITE';
                   l_err_msg  := 'New Supplier site Cannot Be Created accts_pay_code_combination_id Is Null';

                   UPDATE xxap_invoices_stg
                   SET process_flag      = 'E',
                       error_type        = 'ERR_VAL',
                       last_updated_date = SYSDATE,
                       last_updated_by   = g_user_id,
                       last_update_login = g_login_id
                   WHERE leg_vendor_num = vendor_rec.leg_vendor_num
                   AND leg_vendor_site_code = vendor_rec.leg_vendor_site_code
                   AND vendor_rec.accts_pay_code_combination_id IS NULL
                   AND batch_id = g_new_batch_id
                   AND run_sequence_id = g_run_seq_id
                   AND org_id = vendor_rec.org_id;

                    FOR r_vendor_err_rec IN (SELECT interface_txn_id
                                           FROM xxap_invoices_stg xis
                                           WHERE leg_vendor_num =
                                           vendor_rec.leg_vendor_num
                                           AND leg_vendor_site_code =
                                           vendor_rec.leg_vendor_site_code
                                           AND batch_id = g_new_batch_id
                                           AND run_sequence_id = g_run_seq_id
                                           AND org_id = vendor_rec.org_id
                                           AND vendor_rec.accts_pay_code_combination_id IS NULL
                                           ) LOOP
                       g_intf_staging_id := r_vendor_err_rec.interface_txn_id;
                       g_source_table    := g_invoice_t;
                       log_errors(pov_return_status       => l_log_ret_status -- OUT
                                  ,
                                  pov_error_msg           => l_log_err_msg -- OUT
                                  ,
                                  piv_source_column_name  => 'VENDOR_NUM/VENDOR_SITE',
                                  piv_source_column_value => vendor_rec.leg_vendor_num ||
                                                         ' / ' ||
                                                    vendor_rec.leg_vendor_site_code,
                                  piv_error_type          => g_err_val,
                                  piv_error_code          => l_err_code,
                                  piv_error_message       => l_err_msg);
                    END LOOP;
                    l_valerr_cnt := 1;
                END IF;
             EXCEPTION
             WHEN no_data_found THEN
                l_valerr_cnt := 2;
                print_log_message('In No Data found of validate vendor site check' ||
                              SQLERRM);
                l_err_code := 'ETN_AP_INVALID_SUPPLIER_SITE';
                l_err_msg  := 'Supplier site not setup in R12, All OUs Verified ';

                UPDATE xxap_invoices_stg
                SET process_flag      = 'E',
                   error_type        = 'ERR_VAL',
                   last_updated_date = SYSDATE,
                   last_updated_by   = g_user_id,
                   last_update_login = g_login_id
               WHERE leg_vendor_num = vendor_rec.leg_vendor_num
               AND leg_vendor_site_code = vendor_rec.leg_vendor_site_code
               AND accts_pay_code_combination_id = vendor_rec.accts_pay_code_combination_id
               AND batch_id = g_new_batch_id
               AND run_sequence_id = g_run_seq_id
               AND org_id = vendor_rec.org_id;

               FOR r_vendor_err_rec IN (SELECT interface_txn_id
                                        FROM xxap_invoices_stg xis
                                        WHERE leg_vendor_num =
                                        vendor_rec.leg_vendor_num
                                        AND leg_vendor_site_code =
                                         vendor_rec.leg_vendor_site_code
                                        AND batch_id = g_new_batch_id
                                        AND run_sequence_id = g_run_seq_id
                                        AND org_id = vendor_rec.org_id
                                        AND accts_pay_code_combination_id = vendor_rec.accts_pay_code_combination_id
               ) LOOP
               g_intf_staging_id := r_vendor_err_rec.interface_txn_id;
               g_source_table    := g_invoice_t;
               log_errors(pov_return_status       => l_log_ret_status -- OUT
                          ,
                          pov_error_msg           => l_log_err_msg -- OUT
                          ,
                          piv_source_column_name  => 'VENDOR_NUM/VENDOR_SITE',
                          piv_source_column_value => vendor_rec.leg_vendor_num ||
                                                     ' / ' ||
                                                vendor_rec.leg_vendor_site_code,
                          piv_error_type          => g_err_val,
                          piv_error_code          => l_err_code,
                          piv_error_message       => l_err_msg);
               END LOOP;
               l_valerr_cnt := 1;

             WHEN OTHERS THEN
                l_valerr_cnt := 2;
                print_log_message('In When others of validate vendor site check' ||
                              SQLERRM);
                l_err_code := 'ETN_AP_INVALID_SUPPLIER_SITE_SRC';
                l_err_msg  := 'Error Finding Source Site'||SUBSTR(SQLERRM, 1, 150);

                UPDATE xxap_invoices_stg
                SET process_flag      = 'E',
                   error_type        = 'ERR_VAL',
                   last_updated_date = SYSDATE,
                   last_updated_by   = g_user_id,
                   last_update_login = g_login_id
               WHERE leg_vendor_num = vendor_rec.leg_vendor_num
               AND leg_vendor_site_code = vendor_rec.leg_vendor_site_code
               AND batch_id = g_new_batch_id
               AND run_sequence_id = g_run_seq_id
               AND org_id = vendor_rec.org_id;

               FOR r_vendor_err_rec IN (SELECT interface_txn_id
                                        FROM xxap_invoices_stg xis
                                        WHERE leg_vendor_num =
                                        vendor_rec.leg_vendor_num
                                        AND leg_vendor_site_code =
                                         vendor_rec.leg_vendor_site_code
                                        AND batch_id = g_new_batch_id
                                        AND run_sequence_id = g_run_seq_id
                                        AND org_id = vendor_rec.org_id) LOOP
               g_intf_staging_id := r_vendor_err_rec.interface_txn_id;
               g_source_table    := g_invoice_t;
               log_errors(pov_return_status       => l_log_ret_status -- OUT
                          ,
                          pov_error_msg           => l_log_err_msg -- OUT
                          ,
                          piv_source_column_name  => 'VENDOR_NUM/VENDOR_SITE',
                          piv_source_column_value => vendor_rec.leg_vendor_num ||
                                                     ' / ' ||
                                                vendor_rec.leg_vendor_site_code,
                          piv_error_type          => g_err_val,
                          piv_error_code          => l_err_code,
                          piv_error_message       => l_err_msg);
               END LOOP;
               l_valerr_cnt := 1;
             END;

          WHEN OTHERS THEN
            l_valerr_cnt := 2;
            print_log_message('In When others of validate vendor site check' ||
                              SQLERRM);
            l_err_code := 'ETN_AP_INVALID_SUPPLIER_SITE';
            l_err_msg  := 'Error Finding Source Site'||SUBSTR(SQLERRM, 1, 150);
        END;
      ELSE
               --When R12 OU is Not Derived
               l_supp_id       := NULL;
               l_supp_name     := NULL;
               l_valerr_cnt := 2;
               l_err_code   := 'ETN_AP_INVALID_SUPPLIER_SITE';
               l_err_msg    := 'Cannot Derive Supplier site, OU Missing';


               UPDATE xxap_invoices_stg
               SET process_flag      = 'E',
                   error_type        = 'ERR_VAL',
                   last_updated_date = SYSDATE,
                   last_updated_by   = g_user_id,
                   last_update_login = g_login_id
               WHERE leg_vendor_num = vendor_rec.leg_vendor_num
               AND leg_vendor_site_code = vendor_rec.leg_vendor_site_code
               AND batch_id = g_new_batch_id
               AND run_sequence_id = g_run_seq_id
               AND org_id IS NULL;

               FOR r_vendor_err_rec IN (SELECT interface_txn_id
                                        FROM xxap_invoices_stg xis
                                        WHERE leg_vendor_num =
                                        vendor_rec.leg_vendor_num
                                        AND leg_vendor_site_code =
                                         vendor_rec.leg_vendor_site_code
                                        AND batch_id = g_new_batch_id
                                        AND run_sequence_id = g_run_seq_id
                                        AND org_id IS NULL) LOOP
               g_intf_staging_id := r_vendor_err_rec.interface_txn_id;
               g_source_table    := g_invoice_t;
               log_errors(pov_return_status       => l_log_ret_status -- OUT
                          ,
                          pov_error_msg           => l_log_err_msg -- OUT
                          ,
                          piv_source_column_name  => 'VENDOR_NUM/VENDOR_SITE',
                          piv_source_column_value => vendor_rec.leg_vendor_num ||
                                                     ' / ' ||
                                                vendor_rec.leg_vendor_site_code,
                          piv_error_type          => g_err_val,
                          piv_error_code          => l_err_code,
                          piv_error_message       => l_err_msg);
               END LOOP;
               l_valerr_cnt := 1;
            END IF;
         END IF;
     -- END IF;

      print_log_message(' -  PROCEDURE : validate_supplier_details   - ');

      IF l_valerr_cnt = 2 THEN
        UPDATE xxap_invoices_stg
           SET process_flag      = 'E',
               error_type        = 'ERR_VAL',
               last_updated_date = SYSDATE,
               last_updated_by   = g_user_id,
               last_update_login = g_login_id
         WHERE leg_vendor_num = vendor_rec.leg_vendor_num
           AND leg_vendor_site_code = vendor_rec.leg_vendor_site_code
           AND batch_id = g_new_batch_id
           AND run_sequence_id = g_run_seq_id;

        FOR r_vendor_err_rec IN (SELECT interface_txn_id
                                   FROM xxap_invoices_stg xis
                                  WHERE leg_vendor_num =
                                        vendor_rec.leg_vendor_num
                                    AND leg_vendor_site_code =
                                        vendor_rec.leg_vendor_site_code
                                    AND batch_id = g_new_batch_id
                                    AND run_sequence_id = g_run_seq_id) LOOP
          g_intf_staging_id := r_vendor_err_rec.interface_txn_id;
          g_source_table    := g_invoice_t;
          log_errors(pov_return_status       => l_log_ret_status -- OUT
                    ,
                     pov_error_msg           => l_log_err_msg -- OUT
                    ,
                     piv_source_column_name  => 'VENDOR_NUM/VENDOR_SITE',
                     piv_source_column_value => vendor_rec.leg_vendor_num ||
                                                ' / ' ||
                                                vendor_rec.leg_vendor_site_code,
                     piv_error_type          => g_err_val,
                     piv_error_code          => l_err_code,
                     piv_error_message       => l_err_msg);
        END LOOP;
      END IF;
    END LOOP;
    COMMIT;
    l_valerr_cnt := 1;

  --v1.10 Changes end
    FOR term_rec IN term_cur LOOP
      l_valerr_cnt    := 1;
      l_payment_terms := NULL;
      l_count         := NULL;
      l_column_name   := NULL;
      l_column_value  := NULL;
      IF term_rec.leg_terms_name IS NOT NULL THEN
        BEGIN
          --Derive R12 value for the given payment term
          SELECT TRIM(flv.description)
            INTO l_payment_terms
            FROM fnd_lookup_values flv
           WHERE TRIM((flv.meaning)) = TRIM((term_rec.leg_terms_name))
             AND flv.LANGUAGE = USERENV('LANG')
             AND flv.enabled_flag = 'Y'
             AND UPPER(flv.lookup_type) = l_payment_term_lkp
             AND TRUNC(SYSDATE) BETWEEN
                 TRUNC(NVL(flv.start_date_active, SYSDATE)) AND
                 TRUNC(NVL(flv.end_date_active, SYSDATE));
        EXCEPTION
          WHEN NO_DATA_FOUND THEN
            l_column_name  := 'LEG_TERMS_NAME';
            l_column_value := term_rec.leg_terms_name;

            l_valerr_cnt    := 2;
            l_payment_terms := NULL;
            print_log_message('In No Data found of payment terms lookup check' ||
                              SQLERRM);
            l_err_code := 'ETN_AP_INVALID_PAYMENT_TERMS';
            l_err_msg  := 'Cross reference for payment Terms not present';
          WHEN OTHERS THEN
            l_column_name   := 'LEG_TERMS_NAME';
            l_column_value  := term_rec.leg_terms_name;
            l_valerr_cnt    := 2;
            l_payment_terms := NULL;
            print_log_message('In When others of payment terms lookup check' ||
                              SQLERRM);
            l_err_code := 'ETN_AP_INVALID_PAYMENT_TERMS';
            l_err_msg  := 'Error while deriving cross reference for payment Terms not present ' ||
                          SUBSTR(SQLERRM, 1, 150);
        END;
      END IF;

      --if terms_name is not null
      IF l_payment_terms IS NOT NULL THEN
        BEGIN
          --Check whether the derived terms_name exist
          SELECT 1
            INTO l_count
            FROM ap_terms apt
           WHERE NAME = l_payment_terms
             AND enabled_flag = 'Y'
             AND TRUNC(SYSDATE) BETWEEN
                 TRUNC(NVL(apt.start_date_active, SYSDATE)) AND
                 TRUNC(NVL(apt.end_date_active, SYSDATE));

          UPDATE xxap_invoices_stg
             SET terms_name = l_payment_terms
           WHERE leg_terms_name = term_rec.leg_terms_name
             AND batch_id = g_new_batch_id
             AND run_sequence_id = g_run_seq_id;
        EXCEPTION
          WHEN NO_DATA_FOUND THEN
            l_column_name  := 'R12_TERMS_NAME';
            l_column_value := l_payment_terms;
            l_valerr_cnt   := 2;
            print_log_message('In No Data found of R12 payment term check' ||
                              SQLERRM);
            l_err_code := 'ETN_AP_INVALID_PAYMENT_TERMS';
            l_err_msg  := 'Payment Terms not defined ' ||
                          SUBSTR(SQLERRM, 1, 150);
          WHEN OTHERS THEN
            l_column_name  := 'R12_TERMS_NAME';
            l_column_value := l_payment_terms;
            l_valerr_cnt   := 2;
            print_log_message('In When others of R12 payment term check' ||
                              SQLERRM);
            l_err_code := 'ETN_AP_INVALID_PAYMENT_TERMS';
            l_err_msg  := 'Error while deriving payment Terms not present ' ||
                          SUBSTR(SQLERRM, 1, 150);
        END;
      END IF;

      IF l_valerr_cnt = 2 THEN
        UPDATE xxap_invoices_stg
           SET process_flag      = 'E',
               error_type        = 'ERR_VAL',
               last_updated_date = SYSDATE,
               last_updated_by   = g_user_id,
               last_update_login = g_login_id
         WHERE leg_terms_name = term_rec.leg_terms_name
           AND batch_id = g_new_batch_id
           AND run_sequence_id = g_run_seq_id;

        FOR r_terms_err_rec IN (SELECT interface_txn_id
                                  FROM xxap_invoices_stg xis
                                 WHERE leg_terms_name =
                                       term_rec.leg_terms_name
                                   AND batch_id = g_new_batch_id
                                   AND run_sequence_id = g_run_seq_id) LOOP
          g_intf_staging_id := r_terms_err_rec.interface_txn_id;
          g_source_table    := g_invoice_t;
          log_errors(pov_return_status       => l_log_ret_status -- OUT
                    ,
                     pov_error_msg           => l_log_err_msg -- OUT
                    ,
                     piv_source_column_name  => l_column_name,
                     piv_source_column_value => l_column_value,
                     piv_error_type          => g_err_val,
                     piv_error_code          => l_err_code,
                     piv_error_message       => l_err_msg);
        END LOOP;
      END IF;
    END LOOP;
    COMMIT;
    l_valerr_cnt := 1;

    FOR pmtmtd_rec IN pmtmtd_cur LOOP
      l_valerr_cnt     := 1;
      l_payment_method := NULL;
      IF pmtmtd_rec.leg_payment_method_code IS NOT NULL THEN
        BEGIN
          /*  Changes for defect 2260 start here
         --Fetch R12 value of the payment method
          SELECT flv.lookup_code
            INTO l_payment_method
            FROM apps.fnd_lookup_values flv
           WHERE flv.lookup_type = 'PAYMENT METHOD'
             AND flv.lookup_code =
                 UPPER(pmtmtd_rec.leg_payment_method_code)
             AND LANGUAGE = USERENV('LANG'); */


           SELECT payment_method_code
           INTO l_payment_method
           FROM apps.IBY_PAYMENT_METHODS_TL ipm
           WHERE ipm.payment_method_code = UPPER(pmtmtd_rec.leg_payment_method_code)
           AND ipm.language = USERENV('LANG');

           --Changes for 2260 end here

          UPDATE xxap_invoices_stg
             SET payment_method_code = l_payment_method
           WHERE leg_payment_method_code =
                 pmtmtd_rec.leg_payment_method_code
             AND batch_id = g_new_batch_id
             AND run_sequence_id = g_run_seq_id;
        EXCEPTION
          WHEN NO_DATA_FOUND THEN
            l_payment_method := NULL;
            print_log_message('In No Data found of payment method lookup check' ||
                              SQLERRM);
            l_valerr_cnt := 2;
            --changes for 2260
            --updated the error code
            l_err_code   := 'ETN_AP_INVALID_PAYMENT_METHOD';
            l_err_msg    := 'Payment method not set up in R12';
            --changes for 2260 end here
          WHEN OTHERS THEN
            l_valerr_cnt     := 2;
            l_payment_method := NULL;
            print_log_message('In When others of payment method lookup check' ||
                              SQLERRM);
            --changes for 2260
            --updated the error code
            l_err_code := 'ETN_AP_INVALID_PAYMENT_METHOD';
            l_err_msg  := 'Error while validating payment method ' ||
                          SUBSTR(SQLERRM, 1, 150);
            --changes for 2260 end here
        END;
      END IF;

      IF l_valerr_cnt = 2 THEN
        UPDATE xxap_invoices_stg
           SET process_flag      = 'E',
               error_type        = 'ERR_VAL',
               last_updated_date = SYSDATE,
               last_updated_by   = g_user_id,
               last_update_login = g_login_id
         WHERE leg_payment_method_code = pmtmtd_rec.leg_payment_method_code
           AND batch_id = g_new_batch_id
           AND run_sequence_id = g_run_seq_id;

        FOR r_mtd_err_rec IN (SELECT interface_txn_id
                                FROM xxap_invoices_stg xis
                               WHERE leg_payment_method_code =
                                     pmtmtd_rec.leg_payment_method_code
                                 AND batch_id = g_new_batch_id
                                 AND run_sequence_id = g_run_seq_id) LOOP
          g_intf_staging_id := r_mtd_err_rec.interface_txn_id;
          g_source_table    := g_invoice_t;
          log_errors(pov_return_status       => l_log_ret_status -- OUT
                    ,
                     pov_error_msg           => l_log_err_msg -- OUT
                    ,
                     piv_source_column_name  => 'LEG_PAYMENT_METHOD_CODE',
                     piv_source_column_value => pmtmtd_rec.leg_payment_method_code
                     --TUT changes
                    ,
                     piv_error_type    => g_err_val,
                     piv_error_code    => l_err_code,
                     piv_error_message => l_err_msg);
        END LOOP;
      END IF;
    END LOOP;
    COMMIT;
    l_valerr_cnt := 1;

    FOR curr_rec IN currency_cur LOOP
      l_record_cnt := NULL;
      l_valerr_cnt := 1;
      IF curr_rec.leg_inv_currency_code IS NOT NULL THEN
        BEGIN
          --check if the currency code exists in the system
          SELECT 1
            INTO l_record_cnt
            FROM fnd_currencies fc
           WHERE fc.currency_code = curr_rec.leg_inv_currency_code
             AND fc.enabled_flag = g_yes
             AND fc.currency_flag = g_yes
             AND TRUNC(SYSDATE) BETWEEN
                 TRUNC(NVL(fc.start_date_active, SYSDATE)) AND
                 TRUNC(NVL(fc.end_date_active, SYSDATE));
        EXCEPTION
          WHEN NO_DATA_FOUND THEN
            l_valerr_cnt := 2;
            print_log_message('In No Data found of currency code check' ||
                              SQLERRM);
            l_err_code := 'ETN_AP_INVALID_CURRENCY_CODE';
            l_err_msg  := 'Currency Code is not Valid';
          WHEN OTHERS THEN
            l_valerr_cnt := 2;
            print_log_message('In When others of currency code check' ||
                              SQLERRM);
            l_err_code := 'ETN_AP_INVALID_CURRENCY_CODE';
            l_err_msg  := 'Error while deriving currency Code ' ||
                          SUBSTR(SQLERRM, 1, 150);
        END;
      END IF;

      IF l_valerr_cnt = 2 THEN
        UPDATE xxap_invoices_stg
           SET process_flag      = 'E',
               error_type        = 'ERR_VAL',
               last_updated_date = SYSDATE,
               last_updated_by   = g_user_id,
               last_update_login = g_login_id
         WHERE leg_inv_currency_code = curr_rec.leg_inv_currency_code
           AND batch_id = g_new_batch_id
           AND run_sequence_id = g_run_seq_id;

        FOR r_curr_err_rec IN (SELECT interface_txn_id
                                 FROM xxap_invoices_stg xis
                                WHERE leg_inv_currency_code =
                                      curr_rec.leg_inv_currency_code
                                  AND batch_id = g_new_batch_id
                                  AND run_sequence_id = g_run_seq_id) LOOP
          g_intf_staging_id := r_curr_err_rec.interface_txn_id;
          g_source_table    := g_invoice_t;
          log_errors(pov_return_status       => l_log_ret_status -- OUT
                    ,
                     pov_error_msg           => l_log_err_msg -- OUT
                    ,
                     piv_source_column_name  => 'CURRENCY_CODE',
                     piv_source_column_value => curr_rec.leg_inv_currency_code
                     --TUT changes
                    ,
                     piv_error_type    => g_err_val,
                     piv_error_code    => l_err_code,
                     piv_error_message => l_err_msg);
        END LOOP;
      END IF;
    END LOOP;
    COMMIT;
    l_valerr_cnt := 1;
    FOR pay_mgr_rec IN pay_mgr_cur LOOP
      l_valerr_cnt    := 1;
      l_err_code      := NULL;
      l_err_msg       := NULL;
      l_ret_msg       := NULL;  --CR 308318
      l_pay_dist_ccid := NULL;
      l_column_name   := NULL;
      l_column_value  := NULL;
      IF pay_mgr_rec.organization_name IS NOT NULL
        --v5.0 starts
      AND pay_mgr_rec.plant IS NOT NULL   
         --v5.0 ends
      THEN
        --Fetch R12 value of the distribution code combination
        BEGIN
          --v5.0 starts
          /*SELECT flv.description
            INTO l_pay_dist_code_concatenated
            FROM apps.fnd_lookup_values flv
           WHERE TRIM((flv.meaning)) =
                 TRIM((pay_mgr_rec.organization_name))
             AND flv.enabled_flag = 'Y'
             AND TRIM(UPPER(flv.lookup_type)) = l_dist_ccid_lkp
             AND TRUNC(SYSDATE) BETWEEN
                 TRUNC(NVL(flv.start_date_active, SYSDATE)) AND
                 TRUNC(NVL(flv.end_date_active, SYSDATE))
             AND flv.LANGUAGE = USERENV('LANG');
             
             AND flv.tag = pay_mgr_rec.plant;*/
             
             SELECT flv.description
            INTO l_pay_dist_code_concatenated
            FROM apps.fnd_lookup_values flv
           WHERE TRIM((flv.tag)) =
                 TRIM((pay_mgr_rec.organization_name))
             AND flv.enabled_flag = 'Y'
             AND TRIM(UPPER(flv.lookup_type)) = l_dist_ccid_lkp
             AND TRUNC(SYSDATE) BETWEEN
                 TRUNC(NVL(flv.start_date_active, SYSDATE)) AND
                 TRUNC(NVL(flv.end_date_active, SYSDATE))
             AND flv.LANGUAGE = USERENV('LANG')
             AND flv.lookup_code = pay_mgr_rec.plant;
             --v5.0 ends
        EXCEPTION
          WHEN NO_DATA_FOUND THEN
            l_pay_dist_ccid := NULL;
            print_log_message('Mapping not present for Operting unit in lookup XXAP_PART_PAY_MGR_ACT. Code combination cannot be derived' ||
                              SQLERRM);
            l_valerr_cnt := 2;
            l_err_code   := 'ETN_AP_INVALID_ACCOUNT_CCID';
            l_err_msg    := ' Cross reference not found for operating unit  ' ||
                            pay_mgr_rec.organization_name ||' Plant '||pay_mgr_rec.plant||
                            ' in XXAP_PART_PAY_MGR_ACT lookup ';

            l_column_name  := 'R12_OPERATING_UNIT';
            l_column_value := pay_mgr_rec.organization_name;
            l_pay_dist_code_concatenated := NULL;  --CR308318
          WHEN OTHERS THEN
            l_valerr_cnt    := 2;
            l_pay_dist_ccid := NULL;
            l_err_code      := 'ETN_AP_INVALID_ACCOUNT_CCID';
            l_err_msg       := 'Error while deriving cross reference for operating unit  ' ||
                               pay_mgr_rec.organization_name ||' Plant '||pay_mgr_rec.plant||
                               ' in XXAP_PART_PAY_MGR_ACT lookup ' ||
                               SUBSTR(SQLERRM, 1, 150);
            l_column_name   := 'R12_OPERATING_UNIT';
            l_column_value  := pay_mgr_rec.organization_name;

            print_log_message('Error in dfetching mapping for Operting unit in lookup XXAP_PART_PAY_MGR_ACT.' ||
                              SQLERRM);
            l_pay_dist_code_concatenated := NULL;  --CR308318
        END;
      END IF;

      -- fetch values corresponding to the R12 tax code
      IF (l_pay_dist_code_concatenated IS NOT NULL) THEN
        BEGIN
          l_rec.concatenated_segments := l_pay_dist_code_concatenated;
          l_txn_date                  := SYSDATE;
          --changes for CR 308318
          /*SELECT code_combination_id
            INTO l_pay_dist_ccid
            FROM gl_code_combinations_kfv gcck
           WHERE concatenated_segments = l_pay_dist_code_concatenated;*/
           l_10_seg_rec.segment1 := SUBSTR(l_pay_dist_code_concatenated,1,
                                          instr(l_pay_dist_code_concatenated,'.',1,1)-1
                                         );
           l_10_seg_rec.segment2 := SUBSTR(l_pay_dist_code_concatenated,
                                       instr(l_pay_dist_code_concatenated,'.',1,1)+1,
                                          instr(l_pay_dist_code_concatenated,'.',1,2)-
                                             instr(l_pay_dist_code_concatenated,'.',1,1)-1);
           l_10_seg_rec.segment3 := SUBSTR(l_pay_dist_code_concatenated,
                                       instr(l_pay_dist_code_concatenated,'.',1,2)+1,
                                          instr(l_pay_dist_code_concatenated,'.',1,3)-
                                             instr(l_pay_dist_code_concatenated,'.',1,2)-1);
           l_10_seg_rec.segment4 := SUBSTR(l_pay_dist_code_concatenated,
                                       instr(l_pay_dist_code_concatenated,'.',1,3)+1,
                                          instr(l_pay_dist_code_concatenated,'.',1,4)-
                                             instr(l_pay_dist_code_concatenated,'.',1,3)-1);
           l_10_seg_rec.segment5 := SUBSTR(l_pay_dist_code_concatenated,
                                       instr(l_pay_dist_code_concatenated,'.',1,4)+1,
                                          instr(l_pay_dist_code_concatenated,'.',1,5)-
                                             instr(l_pay_dist_code_concatenated,'.',1,4)-1);
           l_10_seg_rec.segment6 := SUBSTR(l_pay_dist_code_concatenated,
                                       instr(l_pay_dist_code_concatenated,'.',1,5)+1,
                                          instr(l_pay_dist_code_concatenated,'.',1,6)-
                                             instr(l_pay_dist_code_concatenated,'.',1,5)-1);
           l_10_seg_rec.segment7 := SUBSTR(l_pay_dist_code_concatenated,
                                       instr(l_pay_dist_code_concatenated,'.',1,6)+1,
                                          instr(l_pay_dist_code_concatenated,'.',1,7)-
                                             instr(l_pay_dist_code_concatenated,'.',1,6)-1);
           l_10_seg_rec.segment8 := SUBSTR(l_pay_dist_code_concatenated,
                                       instr(l_pay_dist_code_concatenated,'.',1,7)+1,
                                          instr(l_pay_dist_code_concatenated,'.',1,8)-
                                             instr(l_pay_dist_code_concatenated,'.',1,7)-1);

           l_10_seg_rec.segment9 := SUBSTR(l_pay_dist_code_concatenated,
                                       instr(l_pay_dist_code_concatenated,'.',1,8)+1,
                                          instr(l_pay_dist_code_concatenated,'.',1,9)-
                                             instr(l_pay_dist_code_concatenated,'.',1,8)-1);

           l_10_seg_rec.segment10 := SUBSTR(l_pay_dist_code_concatenated,
                                        instr(l_pay_dist_code_concatenated,'.',-1)+1);

           xxetn_common_pkg.get_ccid(l_10_seg_rec,l_pay_dist_ccid,l_ret_msg);

           IF l_ret_msg IS NOT NULL
           THEN
              l_valerr_cnt    := 2;
              l_err_msg       := SUBSTR(l_ret_msg,2000);
              l_err_code      := 'ETN_AP_INVALID_ACCOUNT_CCID';
              l_pay_dist_ccid := NULL;
              l_column_name   := 'pay_dist_code_concatenated';
              l_column_value  := l_pay_dist_code_concatenated;

              print_log_message('API Did not return any value validate_dist_code_concatenatd check' ||
                              SQLERRM);
           END IF;
           --changes for CR 308318 end here
        EXCEPTION
          WHEN NO_DATA_FOUND THEN
            l_valerr_cnt    := 2;
            l_err_code      := 'ETN_AP_INVALID_ACCOUNT_CCID';
            l_err_msg       := ' Code combination not defined for concatenated segment';
            l_pay_dist_ccid := NULL;
            l_column_name   := 'pay_dist_code_concatenated';
            l_column_value  := l_pay_dist_code_concatenated;

            print_log_message('In No Data found of validate_dist_code_concatenatd check' ||
                              SQLERRM);
          WHEN OTHERS THEN
            l_valerr_cnt   := 2;
            l_err_code     := 'ETN_AP_INVALID_ACCOUNT_CCID';
            l_err_msg      := 'Error while deriving CCID  ' ||
                              substr(SQLERRM, 1, 150);
            l_column_name  := 'pay_dist_code_concatenated';
            l_column_value := l_pay_dist_code_concatenated;

            l_pay_dist_ccid := NULL;
            print_log_message('In When others of validate_dist_code_concatenatd check' ||
                              SQLERRM);
        END;

        UPDATE xxap_invoice_lines_stg
           SET dist_code_concatenated_id = l_pay_dist_ccid
         WHERE organization_name = pay_mgr_rec.organization_name
         --v5.0
         AND substr(leg_dist_code_concatenated,1,4) =pay_mgr_rec.plant
         --v5.0 ends    
           AND batch_id = g_new_batch_id
           --today
           AND NVL(leg_deferred_acctg_flag, 'N') = 'P'
           --today
           AND run_sequence_id = g_run_seq_id;
      END IF;

      IF l_valerr_cnt = 2 THEN
        UPDATE xxap_invoice_lines_stg
           SET process_flag      = 'E',
               error_type        = 'ERR_VAL',
               last_updated_date = SYSDATE,
               last_updated_by   = g_user_id,
               last_update_login = g_login_id
         WHERE organization_name = pay_mgr_rec.organization_name
         --v5.0
         AND substr(leg_dist_code_concatenated,1,4) =pay_mgr_rec.plant
         --v5.0 ends
           AND NVL(leg_deferred_acctg_flag, 'N') = 'P'
           AND batch_id = g_new_batch_id
           AND run_sequence_id = g_run_seq_id;

        FOR r_pay_err_rec IN (SELECT interface_txn_id
                                FROM xxap_invoice_lines_stg xis
                               WHERE organization_name =
                                     pay_mgr_rec.organization_name
                                 --v5.0
                                AND substr(leg_dist_code_concatenated,1,4) =pay_mgr_rec.plant
                                --v5.0 ends    
                                 AND batch_id = g_new_batch_id
                                 AND run_sequence_id = g_run_seq_id
                                 AND NVL(leg_deferred_acctg_flag, 'N') = 'P') LOOP
          g_intf_staging_id := r_pay_err_rec.interface_txn_id;
          g_source_table    := g_invoice_line_t;
          log_errors(pov_return_status       => l_log_ret_status -- OUT
                    ,
                     pov_error_msg           => l_log_err_msg -- OUT
                    ,
                     piv_source_column_name  => l_column_name,
                     piv_source_column_value => l_column_value,
                     piv_error_type          => g_err_val,
                     piv_error_code          => l_err_code,
                     piv_error_message       => l_err_msg);
        END LOOP;
      END IF;
    END LOOP;
    COMMIT;

    l_valerr_cnt := 1;

    FOR tax_rec IN tax_cur LOOP
      l_valerr_cnt            := 1;
      l_err_code              := NULL;
      l_err_msg               := NULL;
      l_tax_code              := NULL;
      l_column_name           := NULL;
      l_column_value          := NULL;
      l_tax                   := NULL;
      l_tax_regime_code       := NULL;
      l_tax_rate_code         := NULL;
      l_tax_status_code       := NULL;
      l_tax_jurisdiction_code := NULL;
      --v2.0
      l_ref_key5              := NULL;
      --v2.0 ends
      --v3.0
      l_tax_rate_percent      := NULL;
      --v3.0 ends


      xxetn_debug_pkg.add_debug(' +  PROCEDURE : validate_tax = ' ||
                                tax_rec.leg_tax_code || ' + ');
      l_record_cnt := 0;
      l_tax_code   := NULL;
      IF tax_rec.leg_tax_code IS NOT NULL THEN
        BEGIN
          --Fetch R12 value of the tax code
          --v1.9
          /*SELECT flv.description --Ver1.4 changes
            INTO l_tax_code
            FROM apps.fnd_lookup_values flv
          --WHERE TRIM ( (flv.lookup_code)) = TRIM ( (tax_rec.leg_tax_code))
           WHERE TRIM((flv.meaning)) = TRIM((tax_rec.leg_tax_code))
             AND flv.enabled_flag = 'Y'
             AND UPPER(flv.lookup_type) = l_tax_lkp
             AND TRUNC(SYSDATE) BETWEEN
                 TRUNC(NVL(flv.start_date_active, SYSDATE)) AND
                 TRUNC(NVL(flv.end_date_active, SYSDATE))
             AND flv.LANGUAGE = USERENV('LANG');*/

          -- pov_tax_code               := l_tax_code;

          xxetn_cross_ref_pkg.get_value(piv_eaton_ledger   => tax_rec.plant_segment, -- Plant Number passed from cursor cur_plant
                                          piv_type           => 'XXEBTAX_TAX_CODE_MAPPING',
                                          piv_direction      => 'E', --value Required
                                          piv_application    => 'XXAP', --value Required
                                          piv_input_value1   => tax_rec.leg_tax_Code,
                                          piv_input_value2   => null, --value Required
                                          piv_input_value3   => null, --value Required
                                          pid_effective_date => SYSDATE, --value Required  -- PASS DEFAULT
                                          pov_output_value1  => lv_out_val1,
                                          pov_output_value2  => lv_out_val2,
                                          pov_output_value3  => lv_out_val3,
                                          pov_err_msg        => lv_err_msg1);
           IF lv_err_msg1 IS NULL
           THEN
              l_tax_code :=  lv_out_val1;
              fnd_file.put_line(fnd_file.log,' in if else plant '||tax_rec.plant_segment
              ||' is passed'|| ' with leg tax code '||tax_rec.leg_tax_Code
              ||' got the value lv_out_val1 as '||lv_out_val1);
           ELSE
              lv_err_msg1 := NULL;
              lv_out_val1 := NULL;
              xxetn_cross_ref_pkg.get_value(piv_eaton_ledger   => NULL, -- Plant Number passed from cursor cur_plant
                                          piv_type           => 'XXEBTAX_TAX_CODE_MAPPING',
                                          piv_direction      => 'E', --value Required
                                          piv_application    => 'XXAP', --value Required
                                          piv_input_value1   => tax_rec.leg_tax_Code,
                                          piv_input_value2   => null, --value Required
                                          piv_input_value3   => null, --value Required
                                          pid_effective_date => SYSDATE, --value Required  -- PASS DEFAULT
                                          pov_output_value1  => lv_out_val1,
                                          pov_output_value2  => lv_out_val2,
                                          pov_output_value3  => lv_out_val3,
                                          pov_err_msg        => lv_err_msg1);
              IF lv_err_msg1 IS  NULL
              THEN
                 l_tax_code :=  lv_out_val1;

                 fnd_file.put_line(fnd_file.log,' in if else NO plant '||tax_rec.plant_segment
              ||' is NOT passed'|| ' with leg tax code '||tax_rec.leg_tax_Code
              ||' got the value lv_out_val1 as '||lv_out_val1);

              ELSE
                 l_tax_code :=  tax_rec.leg_tax_code;

                 fnd_file.put_line(fnd_file.log,' in if else NO plant tax code is assigned as is '||
                 tax_rec.plant_segment||' tax code for r12 will be '||l_tax_code||'-'||lv_err_msg1);
              END IF;
           END IF;
           --v1.9 end here
        EXCEPTION
          --v1.9
            /*
           WHEN NO_DATA_FOUND THEN
            l_column_name  := 'LEG_TAX_CODE';
            l_column_value := tax_rec.leg_tax_code;

            l_valerr_cnt := 2;
            l_tax_code   := NULL;
            print_log_message('In No Data found of tax code lookup check' ||
                              SQLERRM);
            l_err_code := 'ETN_AP_INVALID_TAX_CODE';
            l_err_msg  := 'Cross reference for tax code not present';*/
            --v1.9 end here
          WHEN OTHERS THEN
            l_column_name  := 'LEG_TAX_CODE';
            l_column_value := tax_rec.leg_tax_code;
            l_valerr_cnt   := 2;
            l_tax_code     := NULL;
            print_log_message('In When others of tax Cross ref check' ||
                              SQLERRM);
            l_err_code := 'ETN_AP_INVALID_TAX_CODE';
            l_err_msg  := 'Error while deriving cross reference for tax code ' ||
                          SUBSTR(SQLERRM, 1, 150);
        END;
      END IF;
      -- fetch values corresponding to the R12 tax code
      IF (l_tax_code IS NOT NULL) THEN
        BEGIN

          SELECT DISTINCT zrb.tax,
                          zrb.tax_regime_code,
                          zrb.tax_rate_code,
                          zrb.tax_status_code,
                          zrb.tax_jurisdiction_code,
                          zrb.percentage_rate
            INTO l_tax,
                 l_tax_regime_code,
                 l_tax_rate_code,
                 l_tax_status_code,
                 l_tax_jurisdiction_code,
                 l_tax_rate_percent
            FROM zx_accounts            za,
                 hr_operating_units     hrou,
                 gl_ledgers             gl,
                 fnd_id_flex_structures fifs,
                 zx_rates_b             zrb,
                 zx_regimes_b           zb
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
             AND zrb.tax_rate_code = l_tax_code
             AND hrou.organization_id = tax_rec.org_id
             AND TRUNC(SYSDATE) BETWEEN
                 TRUNC(NVL(zb.effective_from, SYSDATE)) AND
                 TRUNC(NVL(zb.effective_to, SYSDATE))
                 --v3.0
             AND TRUNC(SYSDATE) BETWEEN
                 TRUNC(NVL(zrb.effective_from, SYSDATE)) AND
                 TRUNC(NVL(zrb.effective_to, SYSDATE))
                 --v3.0 ends
             AND ZRB.ACTIVE_FLAG = 'Y';  --change for 2260
                                         --added the condition to pick only one record

          UPDATE xxap_invoice_lines_stg
             SET tax                   = l_tax,
                 tax_code              = l_tax_code,
                 tax_regime_code       = l_tax_regime_code,
                 tax_rate_code         = l_tax_rate_code,
                 tax_status_code       = l_tax_status_code,
                 tax_jurisdiction_code = l_tax_jurisdiction_code,
                 --v1.11
                 leg_reference_key4    = 'Y',
                 amount                = leg_amount
                 --v3.0
                 ,tax_Rate              = DECODE(l_tax_rate_percent, 0
                                                                  , DECODE(l_tax_rate_percent
                                                                           ,leg_amount, null,
                                                                                        99  )
                                                                   ,null)
                --v3.0 ends
                 --v1.11 ends
           WHERE leg_tax_code = tax_rec.leg_tax_code
             AND batch_id = g_new_batch_id
             AND run_sequence_id = g_run_seq_id
             AND plant_Segment = tax_rec.plant_segment  --v1.9
             AND leg_line_type_lookup_code ='TAX'; --change for 2260
                                                   --condition included to update only tax lines
                                                   --with tax details
           --v1.11 update the tax code on item line as well
            --v2.0
            l_ref_key5 := get_tax_classification(tax_rec.org_id);
            --v2.0 ends
            UPDATE xxap_invoice_lines_stg
            SET tax_code              = l_tax_code,
            leg_reference_key5 = l_ref_key5  --v1.11(Change specific to VERTEX)
            WHERE leg_tax_code = tax_rec.leg_tax_code
            AND batch_id = g_new_batch_id
            AND run_sequence_id = g_run_seq_id
            AND plant_Segment = tax_rec.plant_segment  --v1.9
            AND leg_line_type_lookup_code <>'TAX'
            AND leg_cost_factor_name IS NULL;  --v2.0

            --v1.11 ends
        EXCEPTION
          WHEN NO_DATA_FOUND THEN
            l_column_name  := 'R12_TAX_CODE';
            l_column_value := l_tax_code;
            l_valerr_cnt   := 2;
            print_log_message('In No Data found of R12 tax code check' ||
                              SQLERRM);
            l_err_code := 'ETN_AP_INVALID_TAX_CODE';
            l_err_msg  := 'Cross Ref And R12 Tax Code Not Defined for Plant'||tax_rec.plant_segment;
          WHEN OTHERS THEN
            l_column_name  := 'R12_TAX_CODE';
            l_column_value := l_tax_code;
            l_valerr_cnt   := 2;
            print_log_message('In When others of R12 tax code check' ||
                              SQLERRM);
            l_err_code := 'ETN_AP_INVALID_TAX_CODE';
            l_err_msg  := 'Error while deriving Cross Ref And R12 tax code  for Plant'||tax_rec.plant_segment||'-'||
                          SUBSTR(SQLERRM, 1, 150);
        END;
      END IF;

      IF l_valerr_cnt = 2 THEN
        UPDATE xxap_invoice_lines_stg
           SET process_flag      = 'E',
               error_type        = 'ERR_VAL',
               last_updated_date = SYSDATE,
               last_updated_by   = g_user_id,
               last_update_login = g_login_id
         WHERE leg_tax_code = tax_rec.leg_tax_code
           AND batch_id = g_new_batch_id
           AND plant_Segment = tax_rec.plant_segment --v1.9
           AND run_sequence_id = g_run_seq_id;
           --v1.11 comment the below line as we need to track errors for all
           --AND leg_line_type_lookup_Code ='TAX'
           --v1.11 ends

        FOR r_tax_err_rec IN (SELECT interface_txn_id
                                FROM xxap_invoice_lines_stg xis
                               WHERE leg_tax_code = tax_rec.leg_tax_code
                                 AND batch_id = g_new_batch_id
                                 AND run_sequence_id = g_run_seq_id
                                 AND plant_Segment = tax_rec.plant_segment --v1.9
                                 --AND leg_line_type_lookup_Code ='TAX' --v1.11
                                 ) LOOP
          g_intf_staging_id := r_tax_err_rec.interface_txn_id;
          g_source_table    := g_invoice_line_t;
          log_errors(pov_return_status       => l_log_ret_status -- OUT
                    ,
                     pov_error_msg           => l_log_err_msg -- OUT
                    ,
                     piv_source_column_name  => l_column_name,
                     piv_source_column_value => l_column_value,
                     piv_error_type          => g_err_val,
                     piv_error_code          => l_err_code,
                     piv_error_message       => l_err_msg);
        END LOOP;
      END IF;
    END LOOP;
    COMMIT;

    l_valerr_cnt := 1;
    FOR project_rec IN project_cur LOOP
      l_valerr_cnt   := 1;
      l_err_code     := NULL;
      l_err_msg      := NULL;
      l_column_name  := NULL;
      l_column_value := NULL;
      l_project_type := NULL;
      l_task_name    := NULL;
      l_task_id      := NULL;

      xxetn_debug_pkg.add_debug(' +  PROCEDURE : validate_project = ' ||
                                project_rec.leg_project_name || ' + ');
      l_record_cnt    := 0;
      l_pa_project_id := NULL;
      IF project_rec.leg_project_name IS NOT NULL THEN
        BEGIN
          --Fetch R12 value of the tax code
          SELECT project_id
          --project_type
            INTO l_pa_project_id
          --                l_project_type   --ver1.4 FOT changes
            FROM pa_projects_all
           WHERE NAME like project_rec.leg_project_name/* || '%'*/;-- v1.12

          -- pov_tax_code               := l_tax_code;
        EXCEPTION
          WHEN NO_DATA_FOUND THEN
            l_column_name  := 'LEG_PROJECT_NAME';
            l_column_value := project_rec.leg_project_name;

            l_valerr_cnt    := 2;
            l_pa_project_id := NULL;
            print_log_message('In No Data found for legacy project check' ||
                              SQLERRM);
            l_err_code := 'ETN_AP_INVALID_LEG_PROJECT';
            l_err_msg  := 'Project not found';
          WHEN OTHERS THEN
            l_column_name   := 'LEG_PROJECT_NAME';
            l_column_value  := project_rec.leg_project_name;
            l_valerr_cnt    := 2;
            l_pa_project_id := NULL;
            print_log_message('In When others of legacy project check' ||
                              SQLERRM);
            l_err_code := 'ETN_AP_INVALID_LEG_PROJECT';
            l_err_msg  := 'Error while validating Project ' ||
                          SUBSTR(SQLERRM, 1, 150);
        END;
      END IF;
      -- fetch values corresponding to the project class
      IF l_pa_project_id IS NOT NULL THEN
        BEGIN
          SELECT class_code
            INTO l_project_type
            FROM pa_project_classes
           WHERE project_id = l_pa_project_id;

        EXCEPTION
          WHEN NO_DATA_FOUND THEN
            l_column_name  := 'PROJECT_NAME';
            l_column_value := project_rec.leg_project_name;
            l_valerr_cnt   := 2;
            print_log_message('In No Data found of project class check' ||
                              SQLERRM);
            l_err_code := 'ETN_AP_PROJECT_CLASS';
            l_err_msg  := 'Project class not found';
          WHEN OTHERS THEN
            l_column_name  := 'PROJECT_NAME';
            l_column_value := project_rec.leg_project_name;
            l_valerr_cnt   := 2;
            print_log_message('In When others of project class check' ||
                              SQLERRM);
            l_err_code := 'ETN_AP_PROJECT_CLASS';
            l_err_msg  := 'Error while fetching project class ' ||
                          SUBSTR(SQLERRM, 1, 150);
        END;
      END IF;

      IF l_project_type IS NOT NULL THEN
        BEGIN
          SELECT tag
            INTO l_task_name
            FROM fnd_lookup_values flv
           WHERE lookup_type = l_task_lookup
             AND flv.LANGUAGE = USERENV('LANG')
             AND TRUNC(SYSDATE) BETWEEN
                 TRUNC(NVL(flv.start_date_active, SYSDATE)) AND
                 TRUNC(NVL(flv.end_date_active, SYSDATE))
             AND enabled_flag = 'Y'
             AND (meaning) = (l_project_type);
        EXCEPTION
          WHEN NO_DATA_FOUND THEN
            l_task_name    := NULL;
            l_column_name  := 'PROJECT_CLASS';
            l_column_value := l_project_type;
            l_valerr_cnt   := 2;
            print_log_message('In No Data found of fetching task values' ||
                              SQLERRM);
            l_err_code := 'ETN_AP_PROJECT_TASK';
            l_err_msg  := 'Task not found for project class in lookup';
          WHEN OTHERS THEN
            l_task_name    := NULL;
            l_column_name  := 'PROJECT_CLASS';
            l_column_value := l_project_type;
            l_valerr_cnt   := 2;
            print_log_message('In When others of project task check' ||
                              SQLERRM);
            l_err_code := 'ETN_AP_PROJECT_TASK';
            l_err_msg  := 'Error while fetching project task ' ||
                          SUBSTR(SQLERRM, 1, 150);
        END;
      END IF;

      IF l_task_name IS NOT NULL THEN
        BEGIN
          SELECT task_id
            INTO l_task_id
            FROM pa_tasks pt, pa_projects_all pp
           WHERE (pt.task_number) = (l_task_name)
             AND pp.project_id = l_pa_project_id
             AND pp.project_id = pt.project_id
             AND project_rec.leg_expenditure_item_date BETWEEN
                 NVL(pp.start_date, SYSDATE) AND
                 NVL(pp.closed_date, SYSDATE)
             AND pp.enabled_flag = 'Y'
             AND project_rec.leg_expenditure_item_date BETWEEN
                 NVL(pt.start_date, SYSDATE) AND
                 NVL(pt.completion_date, SYSDATE);

          UPDATE xxap_invoice_lines_stg
             SET project_id = l_pa_project_id, task_id = l_task_id
             , expenditure_item_date = leg_expenditure_item_date    --defect 2568
           WHERE leg_project_name = project_rec.leg_project_name
             AND batch_id = g_new_batch_id
             AND run_sequence_id = g_run_seq_id;

        EXCEPTION
          WHEN NO_DATA_FOUND THEN
            l_task_id      := NULL;
            l_column_name  := 'PROJECT_TASK_NAME';
            l_column_value := l_task_name;
            l_valerr_cnt   := 2;
            print_log_message('In No Data found of fetching task id' ||
                              SQLERRM);
            l_err_code := 'ETN_AP_INVALID_TASK';
            l_err_msg  := 'Task not found ';
          WHEN OTHERS THEN
            l_task_name    := NULL;
            l_column_name  := 'PROJECT_TASK_NAME';
            l_column_value := l_task_name;
            l_valerr_cnt   := 2;
            print_log_message('In When others of project task check' ||
                              SQLERRM);
            l_err_code := 'ETN_AP_INVALID_TASK';
            l_err_msg  := 'Error while fetching project task id' ||
                          SUBSTR(SQLERRM, 1, 150);
        END;
      END IF;

      IF l_valerr_cnt = 2 THEN
        UPDATE xxap_invoice_lines_stg
           SET process_flag      = 'E',
               error_type        = 'ERR_VAL',
               last_updated_date = SYSDATE,
               last_updated_by   = g_user_id,
               last_update_login = g_login_id
         WHERE leg_project_name = project_rec.leg_project_name
           AND batch_id = g_new_batch_id
           AND run_sequence_id = g_run_seq_id;

        FOR r_proj_err_rec IN (SELECT interface_txn_id
                                 FROM xxap_invoice_lines_stg xis
                                WHERE leg_project_name =
                                      project_rec.leg_project_name
                                  AND batch_id = g_new_batch_id
                                  AND run_sequence_id = g_run_seq_id) LOOP
          g_intf_staging_id := r_proj_err_rec.interface_txn_id;
          g_source_table    := g_invoice_line_t;
          log_errors(pov_return_status       => l_log_ret_status -- OUT
                    ,
                     pov_error_msg           => l_log_err_msg -- OUT
                    ,
                     piv_source_column_name  => l_column_name,
                     piv_source_column_value => l_column_value,
                     piv_error_type          => g_err_val,
                     piv_error_code          => l_err_code,
                     piv_error_message       => l_err_msg);
        END LOOP;
      END IF;
    END LOOP;
    COMMIT;

    l_valerr_cnt := 1;
    FOR exp_org_rec IN exp_org_cur LOOP
      l_valerr_cnt   := 1;
      l_err_code     := NULL;
      l_err_msg      := NULL;
      l_exp_org_id   := NULL;
      l_column_name  := NULL;
      l_column_value := NULL;

      xxetn_debug_pkg.add_debug(' +  PROCEDURE : validate_expenditure org = ' ||
                                exp_org_rec.leg_expenditure_org_name ||
                                ' + ');
      l_record_cnt := 0;
      l_exp_org_id := NULL;
      IF exp_org_rec.leg_expenditure_org_name IS NOT NULL THEN
        BEGIN

          SELECT hou.organization_id
            INTO l_exp_org_id
            FROM apps.hr_all_organization_units hou
           WHERE /*(hou.NAME) = (exp_org_rec.leg_expenditure_org_name)
           AND */  hou.date_to IS NULL              -- Change for Expenditure Org and Expenditure type Logic in Invoices
           AND (SUBSTR(hou.name,1,7)) =(SUBSTR(exp_org_rec.leg_expenditure_org_name,1,7));  -- Change for Expenditure Org and Expenditure type Logic in Invoices v1.7

          UPDATE xxap_invoice_lines_stg
             SET expenditure_organization_id = l_exp_org_id
           WHERE leg_expenditure_org_name =
                 exp_org_rec.leg_expenditure_org_name
             AND batch_id = g_new_batch_id
             AND run_sequence_id = g_run_seq_id;

        EXCEPTION
          WHEN NO_DATA_FOUND THEN
            l_column_name  := 'EXPENDITURE_ORGANIZATION';
            l_column_value := exp_org_rec.leg_expenditure_org_name;

            l_valerr_cnt    := 2;
            l_pa_project_id := NULL;
            print_log_message('In No Data found for legacy expenditure org' ||
                              SQLERRM);
            l_err_code := 'ETN_AP_INVALID_LEG_EXP_ORG';
            l_err_msg  := 'Expenditure_organization not set up';
          WHEN OTHERS THEN
            l_column_name   := 'EXPENDITURE_ORGANIZATION';
            l_column_value  := exp_org_rec.leg_expenditure_org_name;
            l_valerr_cnt    := 2;
            l_pa_project_id := NULL;
            print_log_message('In When others of legacy expenditure org check' ||
                              SQLERRM);
            l_err_code := 'ETN_AP_INVALID_LEG_EXP_ORG';
            l_err_msg  := 'Error while validating expenditure org ' ||
                          SUBSTR(SQLERRM, 1, 150);
        END;
      END IF;

      IF l_valerr_cnt = 2 THEN
        UPDATE xxap_invoice_lines_stg
           SET process_flag      = 'E',
               error_type        = 'ERR_VAL',
               last_updated_date = SYSDATE,
               last_updated_by   = g_user_id,
               last_update_login = g_login_id
         WHERE leg_expenditure_org_name =
               exp_org_rec.leg_expenditure_org_name
           AND batch_id = g_new_batch_id
           AND run_sequence_id = g_run_seq_id;

        FOR r_exporg_err_rec IN (SELECT interface_txn_id
                                   FROM xxap_invoice_lines_stg xis
                                  WHERE leg_expenditure_org_name =
                                        exp_org_rec.leg_expenditure_org_name
                                    AND batch_id = g_new_batch_id
                                    AND run_sequence_id = g_run_seq_id) LOOP
          g_intf_staging_id := r_exporg_err_rec.interface_txn_id;
          g_source_table    := g_invoice_line_t;
          log_errors(pov_return_status       => l_log_ret_status -- OUT
                    ,
                     pov_error_msg           => l_log_err_msg -- OUT
                    ,
                     piv_source_column_name  => l_column_name,
                     piv_source_column_value => l_column_value,
                     piv_error_type          => g_err_val,
                     piv_error_code          => l_err_code,
                     piv_error_message       => l_err_msg);
        END LOOP;
      END IF;
    END LOOP;
    COMMIT;

    l_line_error_flag := 'N'; --Defect 2568
    -- Ver 1.3 end
    FOR validate_invoice_hdr_rec IN validate_invoice_hdr_cur LOOP
      BEGIN
        -- Initialize loop variables
        l_error_cnt             := 0;
        l_err_code              := NULL;
        l_err_msg               := NULL;
        l_header_error_flag     := NULL;
        l_org_id                := NULL;
        l_operating_unit        := NULL;
        l_accts_pay_code_concat := NULL;
        l_terms_name            := NULL;
        l_payment_method        := NULL;
        -- l_upd_ret_status  :=  NULL;
        l_log_ret_status := NULL;
        l_log_err_msg    := NULL;
        --v2.0 initialize tax control amount
        l_tax_cont_amt   := 0;
        --v2.0 ends here
        xxetn_debug_pkg.add_debug('validate Invoice Header Record : ' ||
                                  validate_invoice_hdr_rec.leg_invoice_num);
        g_intf_staging_id := validate_invoice_hdr_rec.interface_txn_id;
        g_src_keyname1    := 'LEG_INVOICE_NUM';
        g_src_keyvalue1   := validate_invoice_hdr_rec.leg_invoice_num;
        --procedure to check mandatory values are not missing
        validate_mandatory_value(/*validate_invoice_hdr_rec.interface_txn_id, --v1.10
                                 validate_invoice_hdr_rec.org_id,           --v1.10
                                 validate_invoice_hdr_rec.accts_pay_code_combination_id,*/ --v1.10
                                 validate_invoice_hdr_rec.leg_invoice_num,
                                 validate_invoice_hdr_rec.leg_invoice_amount,
                                 validate_invoice_hdr_rec.leg_inv_type_lookup_code,
                                 validate_invoice_hdr_rec.leg_terms_name,
                                 validate_invoice_hdr_rec.leg_invoice_date,
                                 validate_invoice_hdr_rec.leg_operating_unit,
                                 validate_invoice_hdr_rec.leg_vendor_num,
                                 --TUT changes start
                                 --                                      validate_invoice_hdr_rec.leg_payment_method_code,
                                 validate_invoice_hdr_rec.leg_payment_method_code,
                                 --TUT changes end
                                 validate_invoice_hdr_rec.leg_vendor_site_code,
                                 validate_invoice_hdr_rec.leg_inv_currency_code,
                                 validate_invoice_hdr_rec.leg_accts_pay_code_concat,
                                 l_error_cnt);

        IF l_error_cnt > 0 THEN
          l_header_error_flag := g_yes;
        END IF;

        duplicate_check(validate_invoice_hdr_rec.leg_invoice_num,
                        validate_invoice_hdr_rec.leg_operating_unit,
                        validate_invoice_hdr_rec.leg_vendor_num,
                        l_error_cnt);

        IF l_error_cnt > 0 THEN
          l_header_error_flag := g_yes;
          l_err_code          := 'ETN_AP_DUPLICATE_INVOICE';
          l_err_msg           := 'Error: duplicate invoice header record';
          log_errors(pov_return_status       => l_log_ret_status -- OUT
                    ,
                     pov_error_msg           => l_log_err_msg -- OUT
                    ,
                     piv_source_column_name  => 'LEG_INVOICE_NUM',
                     piv_source_column_value => validate_invoice_hdr_rec.leg_invoice_num,
                     piv_error_type          => g_err_val,
                     piv_error_code          => l_err_code,
                     piv_error_message       => l_err_msg);
        END IF;

        l_error_cnt := 0;

        --If Conversion type is 'User' ,then exchange rate and exchange_Rate_date should not be NULL
        --IF NVL (UPPER (validate_invoice_hdr_rec.leg_exchange_rate_type), 'XX') = 'USER'
        IF NVL(UPPER(validate_invoice_hdr_rec.leg_exchange_rate_type), 'XX') in
           ('USER', 'CORPORATE') --Ver 1.4  FUT changes
         THEN
          IF validate_invoice_hdr_rec.leg_exchange_rate IS NULL OR
             validate_invoice_hdr_rec.leg_exchange_date IS NULL THEN
            l_header_error_flag := g_yes;
            xxetn_debug_pkg.add_debug(' Exchange Date or Rate IS NULL');
            l_err_code := 'ETN_AP_INVALID_EXCHANGE_RATE';
            l_err_msg  := 'Error: Exchange Date or Rate IS NULL';
            log_errors(pov_return_status       => l_log_ret_status -- OUT
                      ,
                       pov_error_msg           => l_log_err_msg -- OUT
                      ,
                       piv_source_column_name  => 'LEG_EXCHANGE_DATE/RATE',
                       piv_source_column_value => validate_invoice_hdr_rec.leg_exchange_date ||
                                                  validate_invoice_hdr_rec.leg_exchange_rate,
                       piv_error_type          => g_err_val,
                       piv_error_code          => l_err_code,
                       piv_error_message       => l_err_msg);
          END IF;
        END IF;

        l_error_cnt := 0;
        --Ver1.3 changes start
        /*
                --Derive org_id from Operating Unit
                    validate_operating_unit (validate_invoice_hdr_rec.leg_operating_unit,
                                             l_operating_unit,
                                             l_org_id,
                                             l_sob_id,
                                             l_error_cnt
                                            );

                    IF l_error_cnt > 0
                    THEN
                       l_header_error_flag        := g_yes;
                       l_err_code                 := 'ETN_AP_INVALID_OPERATING_UNIT';
                       l_err_msg                  := 'Error: Operating Unit name is not valid.';
                       log_errors (pov_return_status            => l_log_ret_status                             -- OUT
                                                                                   ,
                                   pov_error_msg                => l_log_err_msg                                -- OUT
                                                                                ,
                                   piv_source_column_name       => 'LEG_OPERATING_UNIT',
                                   piv_source_column_value      => validate_invoice_hdr_rec.leg_operating_unit,
                                   piv_error_type               => g_err_val,
                                   piv_error_code               => l_err_code,
                                   piv_error_message            => l_err_msg
                                  );
                    END IF;
                    --validate supplier details and fetch vendor_id and vendor_site_id
                    validate_supplier_details (validate_invoice_hdr_rec.leg_vendor_num,
                                               validate_invoice_hdr_rec.leg_vendor_site_code,
                                               l_org_id,
                                               l_vendor_id,
                                               l_vendor_name,
                                               l_vendor_site_id,
                                               l_error_cnt
                                              );

                    IF l_error_cnt > 0
                    THEN
                       l_header_error_flag        := g_yes;
                       l_err_code                 := 'ETN_AP_INVALID_SUPPLIER';
                       l_err_msg                  := 'Error: Supplier is not Valid';
                       log_errors (pov_return_status            => l_log_ret_status                             -- OUT
                                                                                   ,
                                   pov_error_msg                => l_log_err_msg                                -- OUT
                                                                                ,
                                   piv_source_column_name       => 'LEG_VENDOR_NUM',
                                   piv_source_column_value      => validate_invoice_hdr_rec.leg_vendor_num,
                                   piv_error_type               => g_err_val,
                                   piv_error_code               => l_err_code,
                                   piv_error_message            => l_err_msg
                                  );
                    END IF;
                     validate_payment_terms( validate_invoice_hdr_rec.leg_terms_name
                                             ,l_terms_name
                                            , l_error_cnt);

                     IF l_error_cnt > 0 THEN
                        l_header_error_flag := g_yes;
                        l_err_code   := 'ETN_AP_INVALID_PAYMENT_TERMS';
                        l_err_msg    := 'Error:Payment Terms is not Valid';


                        log_errors ( pov_return_status          =>   l_log_ret_status          -- OUT
                                   , pov_error_msg              =>   l_log_err_msg             -- OUT
                                   , piv_source_column_name     =>   'LEG_TERMS_NAME'
                                   , piv_source_column_value    =>   validate_invoice_hdr_rec.leg_terms_name
                                   , piv_error_type             =>   g_err_val
                                   , piv_error_code             =>   l_err_code
                                   , piv_error_message          =>   l_err_msg
                                   );

                     END IF;


                     validate_code_combinations( validate_invoice_hdr_rec.leg_accts_pay_code_concat
                                                ,l_accts_pay_code_concat
                                                ,l_error_cnt);
                               IF l_error_cnt > 0 THEN
                                  l_header_error_flag := g_yes;
                                  l_err_code   := 'ETN_AP_INVALID_ACCOUNT_CCID';
                                  l_err_msg    := 'Error: Invalid Distribution Code Combination';


                                  log_errors ( pov_return_status          =>   l_log_ret_status          -- OUT
                                             , pov_error_msg              =>   l_log_err_msg             -- OUT
                                             , piv_source_column_name     =>   'LEG_ACCTS_PAY_CODE_CONCAT'
                                             , piv_source_column_value    =>   validate_invoice_hdr_rec.leg_accts_pay_code_concat
                                             , piv_error_type             =>   g_err_val
                                             , piv_error_code             =>   l_err_code
                                             , piv_error_message          =>   l_err_msg
                                             );

                               END IF;

                    --Validate GL Period
                    validate_gl_period (l_sob_id, l_error_cnt);

                    IF l_error_cnt > 0
                    THEN
                       l_header_error_flag        := g_yes;

                       IF l_error_cnt = 2
                       THEN
                          xxetn_debug_pkg.add_debug ('GL Period is close. Input = ' || g_gl_date);
                          l_err_code                 := 'ETN_AR_INVALID_GL_PERIOD';
                          l_err_msg                  := 'Error:GL Period is not open.';
                       ELSIF l_error_cnt = 3
                       THEN
                          xxetn_debug_pkg.add_debug ('AR Period is close. Input = ' || g_gl_date);
                          l_err_code                 := 'ETN_AR_INVALID_AR_PERIOD';
                          l_err_msg                  := 'Error:AR Period is not open.';
                       END IF;

                       log_errors (pov_return_status            => l_log_ret_status                             -- OUT
                                                                                   ,
                                   pov_error_msg                => l_log_err_msg                                -- OUT
                                                                                ,
                                   piv_source_column_name       => 'GL_date',
                                   piv_source_column_value      => g_gl_date,
                                   piv_error_type               => g_err_val,
                                   piv_error_code               => l_err_code,
                                   piv_error_message            => l_err_msg
                                  );
                    END IF;


                    l_error_cnt                := 0;
                    --Check whether payment method exists in the system
        --TUT changes start
              /*      validate_payment_mtd (validate_invoice_hdr_rec.leg_payment_method_code,
                                          l_payment_method,
                                          l_error_cnt
                                         );
                    validate_payment_mtd (validate_invoice_hdr_rec.leg_payment_method_code,
                                          l_payment_method,
                                          l_error_cnt
                                         );

        --TUT changes end
                    IF l_error_cnt > 0
                    THEN
                       l_header_error_flag        := g_yes;
                       l_err_code                 := 'ETN_AP_INVALID_PAYMENT_METHOD';
                       l_err_msg                  := 'Error: Payment Method is not Valid';
                       log_errors (pov_return_status            => l_log_ret_status                             -- OUT
                                                                                   ,
                                   pov_error_msg                => l_log_err_msg                                -- OUT
                                                                                ,
                                   piv_source_column_name       => 'LEG_PAYMENT_METHOD_CODE',
                                   piv_source_column_value      => validate_invoice_hdr_rec.leg_payment_method_code,  --TUT changes
                                   piv_error_type               => g_err_val,
                                   piv_error_code               => l_err_code,
                                   piv_error_message            => l_err_msg
                                  );
                    END IF;
                    l_error_cnt                := 0;
                    --Validate currency code
                    validate_currency_code (validate_invoice_hdr_rec.leg_inv_currency_code, l_error_cnt);

                    IF l_error_cnt > 0
                    THEN
                       l_header_error_flag        := g_yes;
                       l_err_code                 := 'ETN_AP_INVALID_CURRENCY_CODE';
                       l_err_msg                  := 'Error: Currency Code is not Valid';
                       log_errors (pov_return_status            => l_log_ret_status                             -- OUT
                                                                                   ,
                                   pov_error_msg                => l_log_err_msg                                -- OUT
                                                                                ,
                                   piv_source_column_name       => 'LEG_INV_CURRENCY_CODE',
                                   piv_source_column_value      => validate_invoice_hdr_rec.leg_inv_currency_code,
                                   piv_error_type               => g_err_val,
                                   piv_error_code               => l_err_code,
                                   piv_error_message            => l_err_msg
                                  );
                    END IF;
        */
        l_error_cnt := 0;

        print_log_message(validate_invoice_hdr_rec.leg_invoice_num);
        print_log_message(validate_invoice_hdr_rec.leg_vendor_num);

        -- ------------------------------------------
        --  Invoice Line Validation
        -- ------------------------------------------
        FOR validate_invoice_line_rec IN validate_invoice_line_cur(validate_invoice_hdr_rec.leg_invoice_num,
                                                                   validate_invoice_hdr_rec.leg_vendor_num) LOOP
          BEGIN
            -- Initialize loop variables
            l_amount              := 0;
            l_invoice_count       := NULL;
            l_leg_attribute1      := NULL;
            l_attr_category       := NULL;
            l_seg_count           := NULL;
            l_inv_header_count    := NULL;
            l_line_err_cnt        := NULL;
            l_project_id          := NULL;
            l_task_id             := NULL;
            l_project_number      := NULL;
            l_task_number         := NULL;
            l_dist_code_concat_id := NULL;
            l_po_header_id        := NULL;
            l_receipt_line_number     := NULL; --v1.7
            l_po_line_id          := NULL;
            l_po_shipment_id      := NULL;
            l_po_distribution_id  := NULL;
            l_receipt_id          := NULL;
            l_awt_group           := NULL;
            l_exp_org_id          := NULL;
            l_exp_org_name        := NULL;
            l_error_cnt           := 0;
            l_err_code            := NULL;
            l_error_flag          := NULL;
            l_expenditure_type    := NULL;
            l_err_msg             := NULL;
            --l_upd_ret_status  :=  NULL;
            l_log_ret_status        := NULL;
            l_log_err_msg           := NULL;
            l_source_name           := NULL;
            l_source_value          := NULL;
            l_tax_code              := NULL;
            l_tax                   := NULL;
            l_tax_regime_code       := NULL;
            l_tax_rate_code         := NULL;
            l_tax_status_code       := NULL;
            l_tax_jurisdiction_code := NULL;
            l_tax_count             := NULL;  --v1.7 change start
            l_ref_key5              := NULL;
            l_dist_line_id          := NULL;
            l_key3                  := NULL;
            l_key4                  := NULL;  --v1.7 change end

            xxetn_debug_pkg.add_debug('validate Invoice Line Record : ' ||
                                      validate_invoice_line_rec.leg_line_number);
            g_intf_staging_id := validate_invoice_line_rec.interface_txn_id;
            g_src_keyname1    := 'LEG_LINE_NUMBER';
            g_src_keyvalue1   := validate_invoice_line_rec.leg_line_number;
            --procedure to check mandatory values are not missing
            validate_line_mandatory_value(validate_invoice_line_rec.leg_line_type_lookup_code,
                                          validate_invoice_line_rec.leg_line_number,
                                          validate_invoice_line_rec.leg_amount,
                                          validate_invoice_line_rec.leg_dist_code_concatenated,
                                          validate_invoice_line_rec.leg_accounting_date,
                                          validate_invoice_line_rec.leg_project_name,
                                          validate_invoice_line_rec.leg_task_name,
                                          validate_invoice_line_rec.leg_expenditure_item_date,
                                          validate_invoice_line_rec.leg_expenditure_type,
                                          validate_invoice_line_rec.leg_expenditure_org_name,
                                          validate_invoice_line_rec.leg_pa_addition_flag,
                                          validate_invoice_line_rec.leg_tax_code,
                                          validate_invoice_line_rec.leg_vendor_num, --ver1.4 FUT changes
                                          l_error_cnt);

            IF l_error_cnt > 0 THEN
              l_error_flag := g_yes;
            END IF;

            duplicate_line_check(validate_invoice_line_rec.leg_invoice_num,
                                 validate_invoice_line_rec.leg_line_number,
                                 validate_invoice_line_rec.leg_operating_unit,
                                 validate_invoice_line_rec.leg_vendor_num,
                                 l_error_cnt);

            IF l_error_cnt > 0 THEN
              l_error_flag := g_yes;
              l_err_code   := 'ETN_AP_DUPLICATE_INVOICE_LINE';
              l_err_msg    := 'Error: duplicate invoice line record';
              log_errors(pov_return_status       => l_log_ret_status -- OUT
                        ,
                         pov_error_msg           => l_log_err_msg -- OUT
                        ,
                         piv_source_column_name  => 'LEG_INVOICE_NUM',
                         piv_source_column_value => validate_invoice_line_rec.leg_invoice_num,
                         piv_error_type          => g_err_val,
                         piv_error_code          => l_err_code,
                         piv_error_message       => l_err_msg);
            END IF;

            --Ver 1.3 start
            /*

                              l_error_cnt                := 0;
                              --Derive org_id from Operating Unit
                              validate_operating_unit (validate_invoice_line_rec.leg_operating_unit,
                                                       l_lin_operating_unit,
                                                       l_lin_org_id,
                                                       l_lin_sob_id,
                                                       l_error_cnt
                                                      );

                              IF l_error_cnt > 0
                              THEN
                                 l_error_flag               := g_yes;
                                 l_err_code                 := 'ETN_AP_INVALID_OPERATING_UNIT';
                                 l_err_msg                  := 'Error: Operating Unit name is not valid.';
                                 log_errors (pov_return_status            => l_log_ret_status                       -- OUT
                                                                                             ,
                                             pov_error_msg                => l_log_err_msg                          -- OUT
                                                                                          ,
                                             piv_source_column_name       => 'LEG_OPERATING_UNIT',
                                             piv_source_column_value      => validate_invoice_line_rec.leg_operating_unit,
                                             piv_error_type               => g_err_val,
                                             piv_error_code               => l_err_code,
                                             piv_error_message            => l_err_msg
                                            );
                              END IF;
            */
            --Ver 1.3 end
            --if line type is TAX then validate TAX information
            l_amount_includes_tax_flag := NULL;
            l_taxable_flag             := NULL;
            IF (validate_invoice_line_rec.leg_line_type_lookup_code = 'TAX') THEN
              --Ver 1.3 start
              /*                     validate_tax (validate_invoice_line_rec.leg_tax_code,
                                 validate_invoice_line_rec.org_id,
                                                 l_tax_code,
                                                 l_tax,
                                                 l_tax_regime_code,
                                                 l_tax_rate_code,
                                                 l_tax_status_code,
                                                 l_tax_jurisdiction_code,
                                                 l_error_cnt
                                                );
                                   IF l_error_cnt > 0
                                   THEN
                                      l_error_flag               := g_yes;
                                      l_err_code                 := 'ETN_AP_INVALID_TAX_CODE';
                                      l_err_msg                  := 'Error: Invalid Tax Code';
                                      log_errors
                                              (pov_return_status            => l_log_ret_status             -- OUT
                                                                                               ,
                                               pov_error_msg                => l_log_err_msg                -- OUT
                                                                                            ,
                                               piv_source_column_name       => 'LEG_TAX_CODE',
                                               piv_source_column_value      => validate_invoice_line_rec.leg_tax_code,
                                               piv_error_type               => g_err_val,
                                               piv_error_code               => l_err_code,
                                               piv_error_message            => l_err_msg
                                              );
                                   END IF;
              */
              --Ver 1.3 end
              l_amount_includes_tax_flag := '';
              l_taxable_flag             := '';

            ELSE
              l_amount_includes_tax_flag := 'N';
              l_taxable_flag             := 'N';
            END IF;

            --if invoice is of project type then validate project information
            IF (validate_invoice_line_rec.leg_project_name IS NOT NULL) THEN
              --ver 1.3 start
              /*

                                   validate_project (validate_invoice_line_rec.leg_project_name,
                                                     validate_invoice_line_rec.leg_task_name,
                                                     validate_invoice_line_rec.leg_expenditure_org_name,
                                   validate_invoice_line_rec.leg_expenditure_item_date,
                                                     l_project_id,
                                                     l_task_id,
                                                     l_exp_org_name,
                                                     l_exp_org_id,
                                                     l_error_cnt,
                                   l_err_msg,
                                   l_source_name,
                                   l_source_value

                                                    );
              */
              l_expenditure_type := validate_invoice_line_rec.leg_expenditure_type;
              /*l_expenditure_type := SUBSTR(validate_invoice_line_rec.leg_expenditure_type,
                                           5);*/ /*SUBSTR(validate_invoice_line_rec.leg_expenditure_type,
                                           5);*/ --2568 change
              /*
                        IF l_error_cnt > 0
                                   THEN
                                      l_error_flag               := g_yes;
                                      l_err_code                 := 'ETN_AP_INVALID_PROJECT';
                                    --  l_err_msg                  := 'Error: Invalid Project';
                                      log_errors
                                          (pov_return_status            => l_log_ret_status                 -- OUT
                                                                                           ,
                                           pov_error_msg                => l_log_err_msg                    -- OUT
                                                                                        ,
                                           piv_source_column_name       => l_source_name,
                                           piv_source_column_value      => l_source_value,
                                           piv_error_type               => g_err_val,
                                           piv_error_code               => l_err_code,
                                           piv_error_message            => l_err_msg
                                          );
                                   END IF;
              */
            END IF;

            --Ver 1.3 start

            --if invoice is partially paid then fetch distribution code combination from lookup
            -- IF NVL(validate_invoice_line_rec.leg_assets_tracking_flag, 'N') IN ('N', 'P')
            --              IF NVL(validate_invoice_line_rec.leg_deferred_acctg_flag , 'N') IN ('P')    --TUT Changes
            --             THEN
            --ver 1.3 start
            /*                  validate_dist_code_concatenatd(  validate_invoice_line_rec.organization_name
                                                                  ,l_dist_code_concat
                                                                  ,l_dist_code_concat_id
                                                                  ,l_error_cnt);
                                  IF l_error_cnt > 0 THEN
                                     l_error_flag := g_yes;
                                     l_err_code   := 'ETN_AP_INVALID_DIST_CCID';
                                     l_err_msg    := 'Error: Invalid Distribution Code COmbinnation';


                                     log_errors ( pov_return_status          =>   l_log_ret_status          -- OUT
                                                , pov_error_msg              =>   l_log_err_msg             -- OUT
                                                , piv_source_column_name     =>   'DIST_CODE_CONCATENATED'
                                                , piv_source_column_value    =>   validate_invoice_line_rec.dist_code_concatenated
                                                , piv_error_type             =>   g_err_val
                                                , piv_error_code             =>   l_err_code
                                                , piv_error_message          =>   l_err_msg
                                                );

                                  END IF;
            */
            --ver 1.3 end
            --        NULL;
            --else derive code combination id from distributed account segments
            --        ELSE
            --               l_dist_code_concat_id := validate_invoice_line_rec.dist_code_concatenated_id;
            --      END IF;
            /*                   ELSE

                                  validate_code_combinations( validate_invoice_line_rec.leg_dist_code_concatenated
                                                             ,l_dist_code_concat_id
                                                             ,l_error_cnt);
                                  IF l_error_cnt > 0 THEN
                                     l_error_flag := g_yes;
                                     l_err_code   := 'ETN_AP_INVALID_DIST_CCID';
                                     l_err_msg    := 'Error: Invalid Distribution Code Combination';


                                     log_errors ( pov_return_status          =>   l_log_ret_status          -- OUT
                                                , pov_error_msg              =>   l_log_err_msg             -- OUT
                                                , piv_source_column_name     =>   'DIST_CODE_CONCATENATED'
                                                , piv_source_column_value    =>   validate_invoice_line_rec.dist_code_concatenated
                                                , piv_error_type             =>   g_err_val
                                                , piv_error_code             =>   l_err_code
                                                , piv_error_message          =>   l_err_msg
                                                );
                                  END IF;
                               END IF;
            */
            --Ver 1.3 end

            --if awt group name is given then fetch R12 cross reference value for awt group name
            IF (validate_invoice_line_rec.leg_awt_group_name IS NOT NULL) THEN
              --TUT changes start
              /*                     validate_awt_tax (validate_invoice_line_rec.leg_awt_group_name,
                                                     l_awt_group,
                                                     l_error_cnt
                                                    );

                                   IF l_error_cnt > 0
                                   THEN
                                      l_error_flag               := g_yes;
                                      l_err_code                 := 'ETN_AP_INVALID_AWT_GROUP';
                                      l_err_msg                  := 'Error:PAWT Group Name is not Valid';
                                      log_errors (pov_return_status            => l_log_ret_status                    -- OUT
                                                                                                  ,
                                                  pov_error_msg                => l_log_err_msg                       -- OUT
                                                                                               ,
                                                  piv_source_column_name       => 'LEG_AWT_GROUP_NAME',
                                                  piv_source_column_value      => validate_invoice_line_rec.leg_awt_group_name,
                                                  piv_error_type               => g_err_val,
                                                  piv_error_code               => l_err_code,
                                                  piv_error_message            => l_err_msg
                                                 );

                                   END IF;
              */
              l_awt_group := validate_invoice_line_rec.leg_awt_group_name;
              --TUT changes end
            END IF;

            --if po number is given then validate po information
            IF (validate_invoice_line_rec.leg_po_number IS NOT NULL OR
               validate_invoice_line_rec.leg_receipt_number IS NOT NULL)
               ----v2.xx performance change
                /*AND
               (validate_invoice_line_rec.leg_hold_lookup_code IS NOT NULL)*/ --TUT changes
             THEN
              derive_po_info(validate_invoice_line_rec.leg_po_number,
                             validate_invoice_line_rec.leg_po_line_number,
                             validate_invoice_line_rec.leg_po_shipment_num,
                             validate_invoice_line_rec.leg_po_distribution_num,
                             validate_invoice_line_rec.leg_receipt_number,
                             l_receipt_line_number ,--validate_invoice_line_rec.leg_receipt_line_number, v1.7
                             l_po_header_id,
                             l_po_line_id,
                             l_po_shipment_id,
                             l_po_distribution_id,
                             l_receipt_id,
                             l_error_cnt);
              l_leg_attribute1 := validate_invoice_line_rec.leg_attribute1;

              IF l_error_cnt > 0 THEN
               --change v1.7
                g_intf_staging_id := validate_invoice_line_rec.interface_txn_id;
                g_src_keyname1    := 'LEG_INVOICE_NUM';
                g_src_keyvalue1   := validate_invoice_line_rec.leg_invoice_num;
                g_source_table    := g_invoice_line_t;
                --change v1.7 change ends

                l_error_flag := g_yes;
                l_err_code   := 'ETN_AP_PO_INFO';
                l_err_msg    := 'Error: Invalid PO/Receipt Information';
                log_errors(pov_return_status       => l_log_ret_status -- OUT
                          ,
                           pov_error_msg           => l_log_err_msg -- OUT
                          ,
                           piv_source_column_name  => 'LEG_INVOICE_NUM',
                           piv_source_column_value => validate_invoice_line_rec.leg_invoice_num,
                           piv_error_type          => g_err_val,
                           piv_error_code          => l_err_code,
                           piv_error_message       => l_err_msg);
              END IF;
              --TUT changes start
            ELSE
              l_leg_attribute1     := validate_invoice_line_rec.leg_po_number ||
                                      validate_invoice_line_rec.leg_po_line_number ||
                                      validate_invoice_line_rec.leg_po_shipment_num ||
                                      validate_invoice_line_rec.leg_po_distribution_num ||
                                      validate_invoice_line_rec.leg_receipt_number;
              l_attr_category      := 'Conversion';
              l_po_header_id       := NULL;
              l_po_line_id         := NULL;
              l_po_shipment_id     := NULL;
              l_po_distribution_id := NULL;
              l_receipt_id         := NULL;
              l_receipt_line_number := NULL; --v1.7
              --TUT changes end
            END IF;
            --v1.11 commenting this below as the soln is provided in the end
            ------------- Tax changes
           /* begin

              select count(1)
                into l_tax_count
                from xxap_invoice_lines_stg
               where leg_line_type_lookup_code = 'TAX'
                 and leg_invoice_num =
                     validate_invoice_hdr_rec.leg_invoice_num
                 and leg_invoice_num =
                     validate_invoice_hdr_rec.leg_invoice_num;

            exception
              when others then
                l_error_flag := g_yes;
                l_err_code   := 'ETN_AP_TAX_INFO';
                l_err_msg    := 'Error: Invalid Tax lines';
                log_errors(pov_return_status       => l_log_ret_status -- OUT
                          ,
                           pov_error_msg           => l_log_err_msg -- OUT
                          ,
                           piv_source_column_name  => 'LEG_REFERENCE_KEY5',
                           piv_source_column_value => validate_invoice_line_rec.leg_invoice_num,
                           piv_error_type          => g_err_imp,
                           piv_error_code          => l_err_code,
                           piv_error_message       => l_err_msg);

            end;

            if l_tax_count = 0 then
              l_ref_key5 := 'NO TAX';
            else
              l_ref_key5 := validate_invoice_line_rec.leg_reference_key5;

            end if;*/
            --v1.11 change ends
            --- fetching line group num and prorate

            /* Cursor Selecting Minimum distribution */

            /*begin
              select min(LEG_DISTRIBUTION_LINE_ID) LEG_DISTRIBUTION_LINE_ID,
                     Row_Number() over(Partition by Leg_Invoice_id Order By min(LEG_DISTRIBUTION_LINE_ID), Leg_Invoice_id desc) RowN
                into l_dist_line_id, l_key3
                from xxap_invoice_lines_stg
               where 1 = 1
                 and LEG_INVOICE_ID =
                     validate_invoice_line_rec.LEG_INVOICE_ID
                 and LEG_LINE_TYPE_LOOKUP_CODE = 'ITEM'
                 and LEG_REFERENCE_KEY2 is not null
                 and org_id = validate_invoice_line_rec.org_ID
               Group by LEG_REFERENCE_KEY2, LEG_INVOICE_ID, LEG_ORG_ID
               Order by 1;
            exception
              when no_data_found then
                null;
              when others then
                fnd_file.put_line(fnd_file.log, 'validate_invoice_line_rec.LEG_INVOICE_ID '||validate_invoice_line_rec.LEG_INVOICE_ID );
                fnd_file.put_line(fnd_file.log, 'validate_invoice_line_rec.leg_org_ID '||validate_invoice_line_rec.leg_org_ID );
                l_error_flag := g_yes;
                l_err_code   := 'ETN_AP_GRP_LINE_INFO';
                l_err_msg    := 'Error: Invalid Group line info';
                log_errors(pov_return_status       => l_log_ret_status -- OUT
                          ,
                           pov_error_msg           => l_log_err_msg -- OUT
                          ,
                           piv_source_column_name  => 'LEG_REFERENCE_KEY3',
                           piv_source_column_value => validate_invoice_line_rec.leg_invoice_num,
                           piv_error_type          => g_err_imp,
                           piv_error_code          => l_err_code,
                           piv_error_message       => l_err_msg);
            end;*/

            --v1.9
            /*if validate_invoice_line_rec.LEG_REFERENCE_KEY4 is not null then
              l_key4 := 'N';
            else
              l_key4 := validate_invoice_line_rec.LEG_REFERENCE_KEY4;
            end if;*/ --v1.9 end here
            -- If any of the line is errored out then invoice header should be errored out
            IF (l_error_flag = g_yes OR l_valerr_cnt = 2)
            THEN l_line_error_flag := 'Y';
            END IF;

            --Update invoice line staging table with the values derived
            UPDATE xxap_invoice_lines_stg
               SET process_flag = DECODE(l_error_flag,
                                         g_yes,
                                         g_error,
                                         DECODE(process_flag,
                                                g_error,
                                                g_error,
                                                g_validated)),
                   error_type   = DECODE(l_error_flag,
                                         g_yes,
                                         DECODE(error_type,
                                                g_err_val,
                                                g_err_val,
                                                NULL)),
                   --                         tax = l_tax,
                   --tax_code = l_tax_code,
                   --tax_regime_code = l_tax_regime_code,
                   --tax_rate_code = l_tax_rate_code,
                   --tax_status_code = l_tax_status_code,
                   --tax_jurisdiction_code = l_tax_jurisdiction_code,
                   amount_includes_tax_flag = l_amount_includes_tax_flag, --TUT changes
                   taxable_flag             = l_taxable_flag, --TUT changes
                   --project_id = l_project_id,
                   --task_id = l_task_id,
                   --dist_code_concatenated_id = l_dist_code_concat_id,
                   po_header_id        = l_po_header_id,
                   po_line_id          = l_po_line_id,
                   po_line_location_id = l_po_shipment_id,
                   po_distribution_id  = l_po_distribution_id,
                   receipt_id          = l_receipt_id,
                   leg_receipt_line_number     = l_receipt_line_number, --v1.7
                   attribute1          = l_leg_attribute1, --TUT changes
                   attribute_category  = DECODE(leg_attribute_category,
                                                NULL,
                                                l_attr_category,
                                                leg_attribute_category),
                   --org_id = l_lin_org_id,
                   awt_group_name = l_awt_group,
                   --expenditure_organization_id = l_exp_org_id,
                   expenditure_type   = l_expenditure_type,
                   --leg_reference_key5 = l_ref_key5, --v1.11 commented this line
                   --leg_reference_key4 = l_key4,  --v1.11 commented this line
                   --leg_reference_key3 = l_key3,
                   --                         organization_name = l_exp_org_name,
                   last_updated_date      = SYSDATE,
                   last_updated_by        = g_user_id,
                   program_application_id = g_prog_appl_id,
                   program_id             = g_conc_program_id,
                   program_update_date    = SYSDATE,
                   request_id             = g_request_id
            --                   WHERE interface_txn_id = validate_invoice_hdr_rec.interface_txn_id;
             WHERE interface_txn_id =
                   validate_invoice_line_rec.interface_txn_id;
            --TUT changes

            g_intf_staging_id := NULL;
            g_src_keyname1    := NULL;
            g_src_keyvalue1   := NULL;
            g_src_keyname2    := NULL;
            g_src_keyvalue2   := NULL;
            g_src_keyname3    := NULL;
            g_src_keyvalue3   := NULL;
          END;

          COMMIT;
          --v2.0 starts here , change for tax control amount
          IF validate_invoice_line_rec.leg_reference_key2 = 'Y'
            AND validate_invoice_line_rec.leg_line_type_lookup_code = 'TAX'
          THEN
             l_tax_cont_amt := validate_invoice_line_rec.leg_amount + l_tax_cont_amt;

          END IF;

          --v2.0 change ends
        END LOOP;

        -- In case line status 'E' then mark invoice header 'E'
        --added 06/24/2015 defect #2568
        l_line_error_flag := get_upd_line_error_info(validate_invoice_hdr_rec.leg_invoice_id
                               , g_new_batch_id
                               --v2.xx performance change
                               ,validate_invoice_hdr_rec.leg_invoice_num
                               ,validate_invoice_hdr_rec.leg_vendor_num
                               );
        IF l_line_error_flag = 'Y'
        THEN
          g_intf_staging_id := validate_invoice_hdr_rec.interface_txn_id;
          g_src_keyname1    := 'LEG_INVOICE_NUM';
          g_src_keyvalue1   := validate_invoice_hdr_rec.leg_invoice_num;
          g_source_table    := g_invoice_t;
          l_header_error_flag := g_yes;
          l_err_code   := 'ERR_VAL';
                l_err_msg    := 'All lines are not valid so Invoice Header process_flag set to Error for Invoice# '  || validate_invoice_hdr_rec.leg_invoice_num;
                log_errors(pov_return_status       => l_log_ret_status -- OUT
                              ,
                               pov_error_msg           => l_log_err_msg -- OUT
                              ,
                               piv_source_column_name  => 'LEG_INVOICE_NUM',
                               piv_source_column_value => validate_invoice_hdr_rec.leg_invoice_num,
                               piv_error_type          => g_err_val,
                               piv_error_code          => l_err_code,
                                  piv_error_message       => l_err_msg);

           g_intf_staging_id := NULL;
           g_src_keyname1    := NULL;
           g_src_keyvalue1   := NULL;
           g_source_table    := NULL;

        END IF;
         --changes for defect #2568 end here

        --Update invoice header staging table with the values derived
        UPDATE xxap_invoices_stg
           SET process_flag = DECODE(l_header_error_flag,
                                     g_yes,
                                     g_error,
                                     DECODE(process_flag,
                                            g_error,
                                            g_error,
                                            g_validated)),
               error_type   = DECODE(l_header_error_flag,
                                     g_yes,
                                     DECODE(error_type,
                                            g_err_val,
                                            g_err_val,
                                            NULL)),
               --operating_unit = l_operating_unit,
               --accts_pay_code_combination_id = l_accts_pay_code_concat,
               --org_id = l_org_id,
               --terms_name = l_terms_name,
               --payment_method_lookup_code = l_payment_method, --TUT changes
               last_updated_date      = SYSDATE,
               last_updated_by        = g_user_id,
               program_application_id = g_prog_appl_id,
               program_id             = g_conc_program_id,
               program_update_date    = SYSDATE,
               request_id             = g_request_id,
               -- TUT changes start
               SOURCE = g_batch_source
               --v2.0
               ,leg_reference_key2 = l_tax_cont_amt
               --v2.0 ends here
        -- TUT changes end
         WHERE interface_txn_id = validate_invoice_hdr_rec.interface_txn_id;

         l_line_error_flag := 'N';  --defect 2568
        /*IF ( l_error_flag = g_yes)
        THEN
          g_retcode := 1;
        END IF;*/
        COMMIT;
        g_intf_staging_id := NULL;
        g_src_keyname1    := NULL;
        g_src_keyvalue1   := NULL;
        g_src_keyname2    := NULL;
        g_src_keyvalue2   := NULL;
        g_src_keyname3    := NULL;
        g_src_keyvalue3   := NULL;
      END;
    END LOOP;

    --v1.9
       UPDATE xxap_invoice_lines_stg xils
       SET process_flag = g_error
       WHERE process_flag= g_validated
       AND EXISTS (SELECT 1 FROM xxap_invoices_stg xis
                   WHERE xis.process_flag IN ( g_error) --TUT changes
                   AND xis.batch_id = xils.batch_id
                   AND xis.batch_id = g_new_batch_id
                   AND xis.leg_invoice_id = xils.leg_invoice_id
                   AND xis.leg_operating_unit = xils.leg_operating_unit
                  );

       COMMIT;
    --v1.9 ends here

    --v1.11tax Related CR 339759 change starts

    --Below statement to exclude the offset records
    UPDATE xxap_invoice_lines_stg xil
    SET process_flag = g_offsets
    WHERE batch_id      = g_new_batch_id
    AND run_sequence_id =  g_run_seq_id
    AND process_flag = g_validated
    AND leg_line_type_lookup_code = 'TAX'
    AND EXISTS (SELECT  1
                FROM zx_taxes_b ztb
                WHERE ztb.tax_regime_code = xil.tax_regime_code
                AND ztb.tax = xil.tax
                AND ztb.offset_tax_flag = 'Y'
                AND TRUNC (SYSDATE)
                      BETWEEN TRUNC (NVL (ztb.effective_from,SYSDATE))
                      AND TRUNC (NVL (ztb.effective_to, SYSDATE)));
    COMMIT;
    BEGIN
       --assign unique line group number to lines

       FOR lgn_rec IN  unq_tax_cur LOOP

          UPDATE Xxap_Invoice_Lines_Stg
          SET leg_reference_key3 =  lgn_rec.sno
          WHERE NVL(tax_Code, 'x') = lgn_rec.tax_code
          AND batch_id =  g_new_batch_id
          AND run_sequence_id = g_run_seq_id
          AND process_flag = g_validated;

       END LOOP;

       COMMIT;
       --Summarize tax lines
       FOR summ_tax_rec IN Summ_inv_tax_cur LOOP

          UPDATE xxap_invoice_lines_stg
          SET amount = summ_tax_rec.summarized
          WHERE leg_invoice_id = summ_tax_rec.leg_invoice_id
          AND  leg_line_number = summ_tax_rec.min_line_id
          AND tax_code         = summ_tax_rec.tax_code
          AND batch_id         = g_new_batch_id
          AND run_Sequence_id  = g_run_seq_id;

       --Mark the tax lines that have been summarized to S
       --these lines will not be imported into the interface
          UPDATE xxap_invoice_lines_stg
          SET process_flag = 'S'
          WHERE leg_invoice_id = summ_tax_rec.leg_invoice_id
          AND  leg_line_number != summ_tax_rec.min_line_id
          AND tax_code         = summ_tax_rec.tax_code
          AND batch_id         = g_new_batch_id
          AND run_Sequence_id  = g_run_seq_id
          AND leg_line_type_lookup_code = 'TAX'
          AND process_flag     = g_validated;

       END LOOP;
       COMMIT;
       --update the prorate flag and lgn for tax lines
       --with no item line against them
       UPDATE xxap_invoice_lines_stg xil
       SET leg_reference_key4 = 'N',
           leg_reference_key3 = NULL
       WHERE NOT EXISTS (SELECT 1 from xxap_invoice_lines_Stg xil1
                         WHERE xil1.batch_id = xil.batch_id
                         AND xil1.leg_invoice_id = xil.leg_invoice_id
                         AND xil1.tax_Code  = NVL(xil.tax_Code, 'X')
                         AND xil1.leg_line_type_lookup_Code = 'ITEM')
       AND leg_line_type_lookup_Code = 'TAX'
       AND batch_id        = g_new_batch_id
       AND run_sequence_id = g_run_seq_id
       AND process_flag    = g_validated;

       COMMIT;
       --update NO TAX on all lines where
       --there is no linkage with tax lines clear lgn
       UPDATE xxap_invoice_lines_stg xil
       SET leg_reference_key3 = NULL,
           leg_reference_key5 = 'NO TAX'
       WHERE NOT EXISTS (SELECT 1 from xxap_invoice_lines_Stg xil1
                         WHERE xil1.batch_id = xil.batch_id
                         AND xil1.leg_invoice_id = xil.leg_invoice_id
                         AND xil1.tax_Code  = NVL(xil.tax_Code, 'X')
                         AND xil1.leg_line_type_lookup_Code = 'TAX')
       AND leg_line_type_lookup_Code <> 'TAX'
       AND batch_id        = g_new_batch_id
       AND run_sequence_id = g_run_seq_id
       AND process_flag    = g_validated;

       COMMIT;
       --Clear the accounting sources for
       --tax lines where prorate is Y
       UPDATE xxap_Invoice_Lines_Stg
       SET project_id = NULL,
           task_id = NULL,
           expenditure_type= NULL,
           expenditure_item_Date = NULL,
           expenditure_organization_id = NULL,
           dist_code_concatenated_id  = NULL,
           po_header_id        = NULL,
           po_line_id          = NULL,
           po_line_location_id = NULL,
           po_distribution_id  = NULL,
           receipt_id          = NULL,
           leg_pa_addition_flag = NULL  --v3.0 added
       WHERE --v3.0 --v4.0 to un-comment the below line
        leg_reference_key4      = 'Y'
       AND --v3.0 ends
           leg_line_type_lookup_Code = 'TAX'
       AND batch_id        = g_new_batch_id
       AND run_sequence_id = g_run_seq_id
       AND process_flag    = g_validated;

       COMMIT;
       --v3.0 to clear the item project info if the po details are present
       UPDATE xxap_invoice_lines_stg
       Set project_id = NULL,
           task_id = NULL,
           expenditure_type= NULL,
           expenditure_item_Date = NULL,
           expenditure_organization_id = NULL,
           leg_pa_addition_flag = NULL,
           leg_expenditure_item_Date = NULL  --v4.0 as this is mapped to invoice line interface
       WHERE leg_line_type_lookup_Code = 'ITEM'
       AND batch_id        = g_new_batch_id
       AND run_sequence_id = g_run_seq_id
       AND process_flag    = g_validated
       AND po_header_id IS NOT NULL;

       COMMIT;
       --v3.0 ends

    EXCEPTION
       WHEN OTHERS THEN
          print_log_message('In Validate invoice - lgn update when others' || SQLERRM);
    END;

     --v1.11 tax Related CR 339759 change ends
     --v1.12

       UPDATE xxap_invoice_lines_stg
       SET leg_line_type_lookup_Code = 'MISCELLANEOUS'
       WHERE batch_id        = g_new_batch_id
       AND run_sequence_id = g_run_seq_id
       AND process_flag    = g_validated
       AND leg_line_type_lookup_Code = 'PREPAY';
     --v1.12 ends here

     COMMIT;
     --v2.1
     /*UPDATE xxap_invoices_stg xas
     SET leg_voucher_num = leg_voucher_num||'_'||xas.plant_segment
     WHERE  xas.rowid >
                  ANY (SELECT xas1.rowid
                       FROM xxap_invoices_stg xas1
                       WHERE xas1.leg_voucher_num = xas.leg_voucher_num
                       AND xas1.org_id = xas.org_id
                      )
     AND batch_id        = g_new_batch_id
     AND run_sequence_id = g_run_seq_id
     AND process_flag    = g_validated;*/

     UPDATE xxap_invoices_stg xas
     SET leg_voucher_num = leg_voucher_num||'_'||xas.plant_segment
      WHERE leg_voucher_num is not null
      AND xas.rowid > ANY
                        (SELECT xas1.rowid
                         FROM xxap_invoices_stg xas1
                         WHERE xas1.leg_voucher_num = xas.leg_voucher_num
                         AND xas1.leg_voucher_num IS NOT NULL
                         AND xas1.org_id = xas.org_id)

     AND process_flag    = g_validated
     AND batch_id        = g_new_batch_id
     AND run_sequence_id = g_run_seq_id;
     --v2.1 end here
    xxetn_debug_pkg.add_debug('-   PROCEDURE : validate_invoice for batch id = ' ||
                              g_new_batch_id || ' - ');
     COMMIT;
  EXCEPTION
    WHEN OTHERS THEN
      g_retcode := 2;
      g_errbuff := 'Failed while vaildating invoice';
      print_log_message('In Validate invoice when others' || SQLERRM);
      fnd_file.put_line(fnd_file.LOG,'Error at line of validate_invoice: '||DBMS_UTILITY.format_error_backtrace());
  END validate_invoice;

  --
  -- ========================
  -- Procedure: create_invoice
  -- =============================================================================
  --   This procedure create_invoice
  -- =============================================================================
  --  Input Parameters :
  --    None
  --  Output Parameters :
  --
  -- -----------------------------------------------------------------------------
  --
  PROCEDURE create_invoice IS
    l_status_flag        VARCHAR2(1);
    l_log_ret_status     VARCHAR2(50);
    l_log_err_msg        VARCHAR2(2000);
    l_err_msg            VARCHAR2(2000);
    l_err_code           VARCHAR2(500);
    l_error_message      VARCHAR2(500);
    l_error_flag         VARCHAR2(10) := 'N';
    l_invoice_id_out     NUMBER;
    l_return_status_out  VARCHAR2(1);
    l_msg_count_out      NUMBER;
    l_msg_data_out       VARCHAR2(1000);
    l_msg_index_out      NUMBER;
    l_po_header_id       NUMBER;
    l_po_line_id         NUMBER;
    l_po_shipment_id     NUMBER;
    l_po_distribution_id NUMBER;
    l_receipt_id         NUMBER;
    l_error_count        NUMBER;
    l_error_cnt          NUMBER;
 --  Added by Ankur for Country Specific Changes V 1.6
 -- Start
    l_invoice_stg_id     NUMBER;
    l_country            VARCHAR2(240);
    l_ap_invoices_int_attribute2 VARCHAR2(240);
    l_ap_invoices_int_att_cat  VARCHAR2(240);
    l_awt_group_id       NUMBER;
    l_intra_eu           NUMBER := 0;
    l_business_category  VARCHAR2(240):= NULL;
    l_fisc_classification VARCHAR2(240) := NULL;
    l_doc_subtype        VARCHAR2(240)  := NULL;
    l_taxation_country   VARCHAR2(240)  := NULL;
    l_supp_country       VARCHAR2(240)  := NULL;
    l_supp_type          VARCHAR2(240)  := NULL;
    l_invoice_stg_line_id NUMBER;

 --  Added by Ankur for Country Specific Changes V 1.6
 --  END
    -- cursor to fetch all the valid header data
    CURSOR invoice_header_import_cur IS
      SELECT xis.*
        FROM xxap_invoices_stg xis
       WHERE 1 = 1
         AND xis.process_flag = g_validated
         AND xis.batch_id = g_new_batch_id;

    -- cursor to fetch all the valid lines data
    CURSOR invoice_lines_import_cur(piv_invoice_num VARCHAR2,
                                    p_org_id        NUMBER,
                                    piv_vendor_num VARCHAR2 --v1.9
                                    ) IS
      SELECT xils.*
      --  xihs.leg_hold_type --TUT changes
        FROM xxap_invoice_lines_stg xils
      --, xxap_invoice_holds_stg xihs --TUT changes
       WHERE 1 = 1
            -- AND xihs.leg_invoice_number(+) = xils.leg_invoice_num   --TUT changes
            --AND xihs.leg_operating_unit(+) = xils.leg_operating_unit  --TUT changes
         AND xils.process_flag = g_validated
         AND xils.leg_invoice_num = piv_invoice_num
         AND xils.leg_vendor_num = piv_vendor_num  --v1.9
         AND xils.batch_id = g_new_batch_id;
  BEGIN
    FOR invoice_header_import_rec IN invoice_header_import_cur LOOP
      l_error_flag := NULL;

      BEGIN
    
    
        xxetn_debug_pkg.add_debug(piv_debug_msg => 'Insert into invoice headers base table');

        INSERT INTO apps.ap_invoices_interface
          (invoice_id,
           invoice_num,
           invoice_type_lookup_code,
           invoice_date,
           --vendor_id,
           --vendor_name,
           vendor_num,
           -- vendor_site_id,
           vendor_site_code,
           invoice_amount,
           amount_applicable_to_discount,
           invoice_currency_code,
           goods_received_date,
           terms_name,
           description,
           last_update_date,
           last_updated_by,
           last_update_login,
           creation_date,
           created_by,
           --status,
           SOURCE,
           payment_currency_code,
           gl_date,
           org_id,
           terms_date,
           accts_pay_code_concatenated,
           accts_pay_code_combination_id,
           payment_method_code,
           payment_method_lookup_code,
           exchange_rate_type,
           exchange_rate,
           exchange_date,
           --doc_category_code,
           voucher_num,
           pay_group_lookup_code,
           exclusive_payment_flag,
           taxation_country,
           document_sub_type,
           calc_tax_during_import_flag,
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
           global_attribute_category,
           global_attribute1,
           global_attribute2,
           global_attribute3,
           global_attribute4,
           global_attribute5,
           global_attribute6,
           global_attribute7,
           global_attribute8,
           global_attribute9,
           global_attribute10,
           global_attribute11,
           global_attribute12,
           global_attribute13,
           global_attribute14,
           global_attribute15,
           global_attribute16,
           global_attribute17,
           global_attribute18,
           global_attribute19,
           global_attribute20,
           ADD_TAX_TO_INV_AMT_FLAG,     -- Added by Ankur for Version 1.8
           control_amount              --v2.0
          )
        VALUES
          (ap_invoices_interface_s.NEXTVAL,
           invoice_header_import_rec.leg_invoice_num,
           invoice_header_import_rec.leg_inv_type_lookup_code,
           invoice_header_import_rec.leg_invoice_date,
           invoice_header_import_rec.leg_vendor_num,
           invoice_header_import_rec.leg_vendor_site_code,
           invoice_header_import_rec.leg_invoice_amount,
           invoice_header_import_rec.amount_applicable_to_discount,
           invoice_header_import_rec.leg_inv_currency_code,
           invoice_header_import_rec.goods_received_date,
           invoice_header_import_rec.terms_name,   --v1.7
           invoice_header_import_rec.leg_description,
           SYSDATE,
           g_user_id,
           g_login_id,
           SYSDATE,
           g_user_id,
           invoice_header_import_rec.SOURCE,
           invoice_header_import_rec.leg_payment_currency_code,
           g_gl_date,
           invoice_header_import_rec.org_id,
           invoice_header_import_rec.leg_terms_date,
           invoice_header_import_rec.accts_pay_code_concatenated,
           invoice_header_import_rec.accts_pay_code_combination_id,
           invoice_header_import_rec.payment_method_code,
           invoice_header_import_rec.leg_payment_method_lookup_code,
           DECODE(invoice_header_import_rec.leg_exchange_rate_type,
                  'Corporate',
                  'User',
                  invoice_header_import_rec.leg_exchange_rate_type), --ver1.4 FUT changes
           invoice_header_import_rec.leg_exchange_rate,
           invoice_header_import_rec.leg_exchange_date,
           --invoice_header_import_rec.leg_doc_category_code,
           --invoice_header_import_rec.leg_doc_sequence_value,  --v1.10
           --v2.1 commented above line, mapped to voucher num
           invoice_header_import_rec.leg_voucher_num,
           --v2.1 ends
           invoice_header_import_rec.pay_group_lookup_code,
           invoice_header_import_rec.leg_exclusive_payment_flag,
           invoice_header_import_rec.leg_taxation_country,
           invoice_header_import_rec.leg_document_sub_type,
           'N', -- calc_tax_during_import_flag -- Added by Ankur for Version 1.8
           invoice_header_import_rec.leg_attribute_category,
           DECODE(invoice_header_import_rec.leg_attribute_category,
                  'Belgium',
                  invoice_header_import_rec.leg_attribute1,
                  'Project Code',
                  invoice_header_import_rec.leg_attribute1,
                  'Turkey',
                  invoice_header_import_rec.leg_attribute1,
                  NULL),
           DECODE(invoice_header_import_rec.leg_attribute_category,
                  'Turkey',
                  invoice_header_import_rec.leg_attribute2,
                  NULL),
           DECODE(invoice_header_import_rec.leg_attribute_category,
                  'Turkey',
                  invoice_header_import_rec.leg_attribute3,
                  NULL),
           NULL,
           NULL,
           DECODE(invoice_header_import_rec.leg_attribute_category,
                  'Global Data Elements',
                  invoice_header_import_rec.leg_attribute6,
                  'South Africa',
                  invoice_header_import_rec.leg_attribute6,
                  NULL),
           DECODE(invoice_header_import_rec.leg_attribute_category,
                  'South Africa',
                  invoice_header_import_rec.leg_attribute7,
                  NULL),
           DECODE(invoice_header_import_rec.leg_attribute_category,
                  'South Africa',
                  invoice_header_import_rec.leg_attribute8,
                  NULL),
           DECODE(invoice_header_import_rec.leg_attribute_category,
                  'South Africa',
                  invoice_header_import_rec.leg_attribute9,
                  NULL),
           /*DECODE(invoice_header_import_rec.leg_attribute_category,  --v3.0
                  'South Africa',
                  invoice_header_import_rec.leg_attribute9,
                  NULL),
           */DECODE(invoice_header_import_rec.leg_attribute_category,
                  'South Africa',
                  invoice_header_import_rec.leg_attribute10,
                  NULL),
           DECODE(invoice_header_import_rec.leg_attribute_category,
                  'TAIWAN GUI Number/FX Payment Instruction',
                  invoice_header_import_rec.leg_attribute11,
                  NULL),
           DECODE(invoice_header_import_rec.leg_attribute_category,
                  'South Africa',
                  invoice_header_import_rec.leg_attribute11,
                  'TAIWAN GUI Number/FX Payment Instruction',
                  invoice_header_import_rec.leg_attribute11,
                  NULL),
           DECODE(invoice_header_import_rec.leg_attribute_category,
                  'TAIWAN GUI Number/FX Payment Instruction',
                  invoice_header_import_rec.leg_attribute13,
                  NULL),
           DECODE(invoice_header_import_rec.leg_attribute_category,
                  'TAIWAN GUI Number/FX Payment Instruction',
                  invoice_header_import_rec.leg_attribute14,
                  NULL),
           DECODE(invoice_header_import_rec.leg_attribute_category,
                  'Actual payment',
                  invoice_header_import_rec.leg_attribute15,
                  NULL),
                  --v3.0
           DECODE(invoice_header_import_rec.leg_global_attribute_category
                                            ,'JA.TW.APXINWKB.INVOICES'
                                               ,'JA.TW.APXIISIM.INVOICES_FOLDER'
                                            ,'JL.BR.APXINWKB.AP_INVOICES'
                                               ,'JL.BR.APXIISIM.INVOICES_FOLDER'
                                            ,'JL.CL.APXINWKB.AP_INVOICES'
                                               ,'JL.CL.APXIISIM.INVOICES_FOLDER'
                                            ,'JE.ES.APXINWKB.MODELO340'
                                               ,'JE.ES.APXIISIM.MODELO349'
                                            ,'JE.PL.APXINWKB.INVOICE_INFO'
                                               ,'JE.PL.APXIISIM.INVOICE_INFO'
                                            ,'JE.PL.APXINWKB.EDI_INFO'
                                               ,'JE.PL.APXIISIM.EDI_INFO'
                                            ,'JE.DK.APXINWKB.EDI_INFO'
                                               ,'JE.DK.APXIISIM.EDI_INFO'

                                               ,invoice_header_import_rec.leg_global_attribute_category   ),
           --v3.0 ends
           invoice_header_import_rec.leg_global_attribute1,
           invoice_header_import_rec.leg_global_attribute2,
           invoice_header_import_rec.leg_global_attribute3,
           invoice_header_import_rec.leg_global_attribute4,
           invoice_header_import_rec.leg_global_attribute5,
           invoice_header_import_rec.leg_global_attribute6,
           invoice_header_import_rec.leg_global_attribute7,
           invoice_header_import_rec.leg_global_attribute8,
           invoice_header_import_rec.leg_global_attribute9,
           invoice_header_import_rec.leg_global_attribute10,
           invoice_header_import_rec.leg_global_attribute11,
           invoice_header_import_rec.leg_global_attribute12,
           invoice_header_import_rec.leg_global_attribute13,
           invoice_header_import_rec.leg_global_attribute14,
           invoice_header_import_rec.leg_global_attribute15,
           invoice_header_import_rec.leg_global_attribute16,
           invoice_header_import_rec.leg_global_attribute17,
           invoice_header_import_rec.leg_global_attribute18,
           invoice_header_import_rec.leg_global_attribute19,
           invoice_header_import_rec.leg_global_attribute20,
           'N',   -- Added by Ankur for Version 1.8 --
           invoice_header_import_rec.leg_reference_key2);  --v2.0 added control amt



        l_error_flag := 'N';
    
    
    
    
    
      EXCEPTION
        WHEN OTHERS THEN
    
    
    
  /*  raise_application_error(-20001,
                            'An error was encountered - ' || sqlcode ||
                            ' -ERROR- ' || sqlerrm);*/
    
          l_error_flag := 'Y';
          l_err_msg    := 'Error while importing invoice header';

          UPDATE xxap_invoices_stg
             SET process_flag           = g_error,
                 run_sequence_id        = g_run_seq_id,
                 error_type             = g_err_imp,
                 last_updated_date      = SYSDATE,
                 last_updated_by        = g_user_id,
                 last_update_login      = g_login_id,
                 program_application_id = g_prog_appl_id,
                 program_id             = g_conc_program_id,
                 program_update_date    = SYSDATE,
                 request_id             = g_request_id
           WHERE interface_txn_id =
                 invoice_header_import_rec.interface_txn_id;

          log_errors(pov_return_status       => l_log_ret_status -- OUT
                    ,
                     pov_error_msg           => l_log_err_msg -- OUT
                    ,
                     piv_source_column_name  => NULL,
                     piv_source_column_value => NULL,
                     piv_error_type          => g_err_imp,
                     piv_error_code          => SQLCODE,
                     piv_error_message       => l_err_msg);
      END;

      BEGIN
        IF l_error_flag = 'N' THEN
          xxetn_debug_pkg.add_debug(piv_debug_msg => 'update headers staging table for converted data');

          UPDATE xxap_invoices_stg
             SET process_flag           = g_processed,
                 run_sequence_id        = g_run_seq_id,
                 error_type             = g_err_imp,
                 last_updated_date      = SYSDATE,
                 last_updated_by        = g_user_id,
                 last_update_login      = g_login_id,
                 program_application_id = g_prog_appl_id,
                 program_id             = g_conc_program_id,
                 program_update_date    = SYSDATE,
                 request_id             = g_request_id
           WHERE interface_txn_id =
                 invoice_header_import_rec.interface_txn_id;
        END IF;
      EXCEPTION
        WHEN OTHERS THEN
          l_err_msg := 'Error while updating staging table with status P while importing invoice header' ||
                       SUBSTR(SQLERRM, 1, 150);

          UPDATE xxap_invoices_stg
             SET process_flag           = g_error,
                 run_sequence_id        = g_run_seq_id,
                 error_type             = g_err_imp,
                 last_updated_date      = SYSDATE,
                 last_updated_by        = g_user_id,
                 last_update_login      = g_login_id,
                 program_application_id = g_prog_appl_id,
                 program_id             = g_conc_program_id,
                 program_update_date    = SYSDATE,
                 request_id             = g_request_id
           WHERE interface_txn_id =
                 invoice_header_import_rec.interface_txn_id;

          log_errors(pov_return_status       => l_log_ret_status -- OUT
                    ,
                     pov_error_msg           => l_log_err_msg -- OUT
                    ,
                     piv_source_column_name  => NULL,
                     piv_source_column_value => NULL,
                     piv_error_type          => g_err_imp,
                     piv_error_code          => SQLCODE,
                     piv_error_message       => l_err_msg);
      END;

      FOR invoice_lines_import_rec IN invoice_lines_import_cur(invoice_header_import_rec.leg_invoice_num,
                                                               invoice_header_import_rec.org_id,
                                                               invoice_header_import_rec.leg_vendor_num) LOOP --v1.9
        IF l_error_flag = 'N' THEN
    
    
    
    
          /*--if the invoice is not on hold, insert all the PO/Receipt information in attribute1
          IF( invoice_lines_import_rec.leg_hold_type IS NULL)
          THEN
             invoice_lines_import_rec.leg_attribute1 := invoice_lines_import_rec.LEG_PO_NUMBER
                                                        ||invoice_lines_import_rec.LEG_PO_LINE_NUMBER
                                                        ||invoice_lines_import_rec.LEG_PO_SHIPMENT_NUM
                                                        ||invoice_lines_import_rec.LEG_PO_DISTRIBUTION_NUM
                                                        ||invoice_lines_import_rec.LEG_RECEIPT_NUMBER;

             invoice_lines_import_rec.leg_po_number           := NULL;
             invoice_lines_import_rec.leg_po_line_number      := NULL;
             invoice_lines_import_rec.leg_po_shipment_num     := NULL;
             invoice_lines_import_rec.LEG_PO_DISTRIBUTION_NUM := NULL;
             invoice_lines_import_rec.leg_receipt_number      := NULL;
             l_po_header_id                                   := NULL;
             l_po_line_id                                     := NULL;
             l_po_shipment_id                                 := NULL;
             l_po_distribution_id                             := NULL;
             l_receipt_id                                     := NULL;
          ELSE
                    derive_po_info( invoice_lines_import_rec.leg_po_number,
                                    invoice_lines_import_rec.leg_po_line_number,
                                    invoice_lines_import_rec.leg_po_shipment_num,
                                    invoice_lines_import_rec.leg_po_distribution_num,
                                    invoice_lines_import_rec.leg_receipt_number,
                                    invoice_lines_import_rec.leg_receipt_line_number,
                                    l_po_header_id,
                                    l_po_line_id,
                                    l_po_shipment_id,
                                    l_po_distribution_id,
                                    l_receipt_id,
                                    l_error_cnt
                                   );

             IF l_error_cnt > 0 THEN
                 l_error_flag := g_yes;
                 l_err_code   := 'ETN_AP_PO_INFO';
                 l_err_msg    := 'Error: Invalid PO/Receipt Information';


                 log_errors ( pov_return_status          =>   l_log_ret_status          -- OUT
                            , pov_error_msg              =>   l_log_err_msg             -- OUT
                            , piv_source_column_name     =>   'LEG_INVOICE_NUM'
                            , piv_source_column_value    =>   invoice_lines_import_rec.leg_invoice_num
                            , piv_error_type             =>   g_err_imp
                            , piv_error_code             =>   l_err_code
                            , piv_error_message          =>   l_err_msg
                            );

              END IF;

          END IF; */
          BEGIN
            xxetn_debug_pkg.add_debug(piv_debug_msg => 'Insert into lines base table');

            INSERT INTO apps.ap_invoice_lines_interface
              (invoice_id,
               invoice_line_id,
               line_number,
               line_type_lookup_code,
               amount,
               accounting_date,
               --TUT changes start
               /*                             po_number,
               po_line_number,
               po_shipment_num,
               po_distribution_num,
               receipt_number,
               */
               --TUT changes end
               po_header_id,
               po_line_id,
               po_line_location_id,
               po_distribution_id,
               rcv_transaction_id,
               awt_group_name,
               description,
               dist_code_combination_id,
               last_updated_by,
               last_update_date,
               last_update_login,
               created_by,
               creation_date,
               org_id,
               income_tax_region,
               unit_price,
               quantity_invoiced,
               tax_code_id,
               stat_amount,
               type_1099,
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
               global_attribute_category,
               global_attribute1,
               global_attribute2,
               global_attribute3,
               global_attribute4,
               global_attribute5,
               global_attribute6,
               global_attribute7,
               global_attribute8,
               global_attribute9,
               global_attribute10,
               global_attribute11,
               global_attribute12,
               global_attribute13,
               global_attribute14,
               global_attribute15,
               global_attribute16,
               global_attribute17,
               global_attribute18,
               global_attribute19,
               global_attribute20,
               --TUT changes start
               pa_addition_flag,
               assets_tracking_flag,
               tax_recovery_rate,
               tax_recovery_override_flag,
               tax_recoverable_flag,
               tax_code_override_flag,
               receipt_currency_code,
               receipt_conversion_rate,
               receipt_currency_amount,
               receipt_number,
               receipt_line_number,
               amount_includes_tax_flag,
               tax_code,
               tax_classification_code,
               LINE_GROUP_NUMber,
               PRORATE_across_FLAG,
               tax_jurisdiction_code,
               tax_status_code,
               tax_rate_code,
               tax_regime_code,
               tax, --TUT changes
               project_id,
               task_id,
               expenditure_type,
               expenditure_organization_id,
               expenditure_item_date,
               taxable_flag,
               project_accounting_context --Ver1.4 changes
               ,tax_rate  --v3.0
               )
            VALUES
              (ap_invoices_interface_s.CURRVAL,
               ap_invoice_lines_interface_s.NEXTVAL,
               invoice_lines_import_rec.leg_line_number,
               invoice_lines_import_rec.leg_line_type_lookup_code,
               DECODE (invoice_lines_import_rec.leg_line_type_lookup_code,
                       'TAX',
                       invoice_lines_import_rec.amount,
                       invoice_lines_import_rec.leg_amount), --v1.11 updated
               g_gl_date, --TUT changes
               --TUT changes start
               /*
               invoice_lines_import_rec.leg_accounting_date,
               invoice_lines_import_rec.leg_po_number,
               invoice_lines_import_rec.leg_po_line_number,
               invoice_lines_import_rec.leg_po_shipment_num,
               invoice_lines_import_rec.leg_po_distribution_num,
               invoice_lines_import_rec.leg_receipt_number,
               */
               --TUT changes end
               invoice_lines_import_rec.po_header_id,
               invoice_lines_import_rec.po_line_id,
               invoice_lines_import_rec.po_line_location_id,
               invoice_lines_import_rec.po_distribution_id,
               invoice_lines_import_rec.receipt_id,
               invoice_lines_import_rec.awt_group_name,
               invoice_lines_import_rec.leg_description,
               DECODE(invoice_lines_import_rec.po_header_id,
                      NULL, invoice_lines_import_rec.dist_code_concatenated_id
                      , NULL),  --2568 change for po details avalbl then null
               g_user_id,
               SYSDATE,
               g_login_id,
               g_user_id,
               SYSDATE,
               invoice_lines_import_rec.org_id,
               invoice_lines_import_rec.leg_income_tax_region,
               invoice_lines_import_rec.leg_unit_price,
               invoice_lines_import_rec.leg_quantity_invoiced,
               invoice_lines_import_rec.tax_code_id,
               invoice_lines_import_rec.leg_stat_amount,
               invoice_lines_import_rec.leg_type_1099,
               invoice_lines_import_rec.attribute_category,
               DECODE(invoice_lines_import_rec.leg_attribute_category,
                      'India Distributions',
                      invoice_lines_import_rec.leg_attribute1,
                      'PQNA',
                      invoice_lines_import_rec.leg_attribute1,
                      'Turkey',
                      invoice_lines_import_rec.leg_attribute1,
                      DECODE(invoice_lines_import_rec.attribute_category,
                             'Conversion',
                             invoice_lines_import_rec.attribute1,
                             NULL)),
               DECODE(invoice_lines_import_rec.leg_attribute_category,
                      'India Distributions',
                      invoice_lines_import_rec.leg_attribute2,
                      'PQNA',
                      invoice_lines_import_rec.leg_attribute2,
                      'Turkey',
                      invoice_lines_import_rec.leg_attribute2,
                      NULL),
               DECODE(invoice_lines_import_rec.leg_attribute_category,
                      'India Distributions',
                      invoice_lines_import_rec.leg_attribute3,
                      'PQNA',
                      invoice_lines_import_rec.leg_attribute3,
                      NULL),
               DECODE(invoice_lines_import_rec.leg_attribute_category,
                      'Global Data Elements',
                      invoice_lines_import_rec.leg_attribute4,
                      NULL),
              --v2.0 NULL,
              invoice_lines_import_rec.leg_reference_key2,
               DECODE(invoice_lines_import_rec.leg_attribute_category,
                      'Global Data Elements',
                      invoice_lines_import_rec.leg_attribute6,
                      'PQNA',
                      invoice_lines_import_rec.leg_attribute6,
                      'Turkey',
                      invoice_lines_import_rec.leg_attribute6,
                      NULL),
               DECODE(invoice_lines_import_rec.leg_attribute_category,
                      'Global Data Elements',
                      invoice_lines_import_rec.leg_attribute7,
                      'PQNA',
                      invoice_lines_import_rec.leg_attribute7,
                      'Turkey',
                      invoice_lines_import_rec.leg_attribute7,
                      NULL),
               DECODE(invoice_lines_import_rec.leg_attribute_category,
                      'Global Data Elements',
                      invoice_lines_import_rec.leg_attribute8,
                      'PQNA',
                      invoice_lines_import_rec.leg_attribute8,
                      'Turkey',
                      invoice_lines_import_rec.leg_attribute8,
                      NULL),
               DECODE(invoice_lines_import_rec.leg_attribute_category,
                      'Global Data Elements',
                      invoice_lines_import_rec.leg_attribute9,
                      'PQNA',
                      invoice_lines_import_rec.leg_attribute9,
                      NULL),
               DECODE(invoice_lines_import_rec.leg_attribute_category,
                      'PQNA',
                      invoice_lines_import_rec.leg_attribute10,
                      'Turkey',
                      invoice_lines_import_rec.leg_attribute10,
                      NULL),
               DECODE(invoice_lines_import_rec.leg_attribute_category,
                      'Turkey',
                      invoice_lines_import_rec.leg_attribute11,
                      NULL),
               DECODE(invoice_lines_import_rec.leg_attribute_category,
                      'Global Data Elements',
                      invoice_lines_import_rec.leg_attribute12,
                      NULL),
               NULL,
               DECODE(invoice_lines_import_rec.leg_attribute_category,
                      'Global Data Elements',
                      invoice_lines_import_rec.leg_attribute14,
                      'Eaton',
                      invoice_lines_import_rec.leg_attribute14,
                      'PQNA',
                      invoice_lines_import_rec.leg_attribute14,
                      'Turkey',
                      invoice_lines_import_rec.leg_attribute14,
                      NULL),
               DECODE(invoice_lines_import_rec.leg_attribute_category,
                      'Global Data Elements',
                      invoice_lines_import_rec.leg_attribute15,   --v3.0 changed from attrb14
                      'Eaton',
                      invoice_lines_import_rec.leg_attribute15,
                      'PQNA',
                      invoice_lines_import_rec.leg_attribute15,
                      'Turkey',
                      invoice_lines_import_rec.leg_attribute15,
                      NULL),
               DECODE(invoice_lines_import_rec.leg_global_attribute_category
                                              ,'JE.PL.APXINWKB.FINAL'
                                                 ,'JE.PL.APXIISIM.FINAL'
                                              ,'JE.BR.APXIISIM.LINES_FOLDER'
                                              ,'JL.BR.APXIISIM.LINES_FOLDER'),
               invoice_lines_import_rec.leg_global_attribute1,
               invoice_lines_import_rec.leg_global_attribute2,
               invoice_lines_import_rec.leg_global_attribute3,
               invoice_lines_import_rec.leg_global_attribute4,
               invoice_lines_import_rec.leg_global_attribute5,
               invoice_lines_import_rec.leg_global_attribute6,
               invoice_lines_import_rec.leg_global_attribute7,
               invoice_lines_import_rec.leg_global_attribute8,
               invoice_lines_import_rec.leg_global_attribute9,
               invoice_lines_import_rec.leg_global_attribute10,
               invoice_lines_import_rec.leg_global_attribute11,
               invoice_lines_import_rec.leg_global_attribute12,
               invoice_lines_import_rec.leg_global_attribute13,
               invoice_lines_import_rec.leg_global_attribute14,
               invoice_lines_import_rec.leg_global_attribute15,
               invoice_lines_import_rec.leg_global_attribute16,
               invoice_lines_import_rec.leg_global_attribute17,
               invoice_lines_import_rec.leg_global_attribute18,
               invoice_lines_import_rec.leg_global_attribute19,
               invoice_lines_import_rec.leg_global_attribute20,
               invoice_lines_import_rec.leg_pa_addition_flag,
               invoice_lines_import_rec.leg_assets_tracking_flag,
               invoice_lines_import_rec.leg_tax_recovery_rate,
               invoice_lines_import_rec.leg_tax_recovery_override_flag,
               invoice_lines_import_rec.leg_tax_recoverable_flag,
               invoice_lines_import_rec.leg_tax_code_override_flag,
               invoice_lines_import_rec.leg_receipt_currency_code,
               invoice_lines_import_rec.leg_receipt_conversion_rate,
               invoice_lines_import_rec.leg_receipt_currency_amount,
               invoice_lines_import_rec.leg_receipt_number,
               invoice_lines_import_rec.leg_receipt_line_number,
               invoice_lines_import_rec.amount_includes_tax_flag,
               DECODE(invoice_lines_import_rec.leg_line_type_lookup_code,'TAX',
                      invoice_lines_import_rec.tax_code,
                      NULL), --v1.11
               invoice_lines_import_rec.leg_reference_key5,  --ver 1.7
               invoice_lines_import_rec.leg_reference_key3,  --ver 1.7
               invoice_lines_import_rec.leg_reference_key4,  --ver 1.7
               --invoice_lines_import_rec.tax_classification_code,
               invoice_lines_import_rec.tax_jurisdiction_code,
               invoice_lines_import_rec.tax_status_code,
               invoice_lines_import_rec.tax_rate_code,
               invoice_lines_import_rec.tax_regime_code,
               invoice_lines_import_rec.tax,
               invoice_lines_import_rec.project_id,
               invoice_lines_import_rec.task_id,
               invoice_lines_import_rec.expenditure_type,
               invoice_lines_import_rec.expenditure_organization_id,
               invoice_lines_import_rec.leg_expenditure_item_date,
               invoice_lines_import_rec.taxable_flag,
               DECODE(invoice_lines_import_rec.project_id, NULL, NULL, 'Y')
               ,invoice_lines_import_rec.Tax_Rate --v3.0
               );

           --  Added by Ankur for Poland Country Specific Changes V 1.6
           -- Start
            l_invoice_stg_line_id := ap_invoice_lines_interface_s.CURRVAL;
        -- Added by Ankur for Poland Country Specific Changes V 1.6
        -- Start
          l_invoice_stg_id := ap_invoices_interface_s.CURRVAL;

            BEGIN
              l_country := NULL;

            --              SELECT DISTINCT UPPER (country)
            --                         INTO l_country
            --                         FROM xxetn_map_unit xmu, hr_operating_units hou
            --                        WHERE UPPER (xmu.operating_unit) = UPPER (hou.NAME)
            --                          AND hou.organization_id = invoice_header_import_rec.org_id
            --                          AND ROWNUM = 1;

                SELECT DISTINCT UPPER (flv.attribute4)
                  INTO l_country
                  FROM fnd_lookup_values_vl flv, hr_operating_units hou
                 WHERE UPPER (flv.attribute3) = UPPER (hou.NAME)
                   AND hou.organization_id = invoice_header_import_rec.org_id
                   AND flv.lookup_type = 'XXETN_PLANT_COUNTRY_LOC_MAP'
                   AND lookup_code = invoice_lines_import_rec.Plant_segment;

                   l_country := SUBSTR(l_country,1,2); -- Get territory Code --

            EXCEPTION
              WHEN OTHERS
              THEN
                 l_country := NULL;
            END;

            IF l_country = 'PL'
            THEN
              IF     (TRIM (TO_CHAR (invoice_header_import_rec.leg_gl_date, 'YYYY')) <>
                                                      TRIM (TO_CHAR (SYSDATE, 'YYYY'))
                     )
                 AND (NVL (invoice_header_import_rec.leg_attribute1, 'X') <> 'NKUP')
              THEN
                 l_ap_invoices_int_attribute2 := 'NKUP';
              ELSE
                 l_ap_invoices_int_attribute2 := NULL;
              END IF;

              IF l_ap_invoices_int_attribute2 IS NOT NULL OR
                 invoice_header_import_rec.leg_attribute1 IS NOT NULL THEN

               l_ap_invoices_int_att_cat := 'Poland';

              ELSE

               l_ap_invoices_int_att_cat := NULL;

              END IF;



              UPDATE apps.ap_invoices_interface
                 SET global_attribute_category = 'JE.PL.APXIISIM.INVOICE_INFO',--'JE.PL.APXIISIM.INSURANCE_INFO', --'JE.PL.APXINWKB.INSURANCE_INFO',
                     attribute_category = l_ap_invoices_int_att_cat,
                     tax_invoice_recording_date = TO_DATE (invoice_header_import_rec.leg_global_attribute1,'yyyy/mm/dd hh24:mi:ss') ,
                     /*doc_category_code =  invoice_header_import_rec.leg_doc_category_code,*/ --decode(invoice_header_import_rec.leg_doc_category_code, 'Dokument SAD', 'Dokument SAD', 'SAD Reverse Charge','SAD Reverse Charge', NULL),
                     attribute8 = invoice_header_import_rec.leg_attribute1,
                     attribute2 = l_ap_invoices_int_attribute2
               WHERE invoice_id = l_invoice_stg_id;
            END IF;


            IF l_country = 'IT'
            THEN
              IF (invoice_header_import_rec.LEG_AWT_GROUP_NAME IS NOT NULL)
              THEN
                 BEGIN
                    l_awt_group_id := NULL;

                    SELECT GROUP_ID
                      INTO l_awt_group_id
                      FROM ap_awt_groups
                     WHERE UPPER (NAME) =
                                  UPPER (invoice_header_import_rec.LEG_AWT_GROUP_NAME);

                    UPDATE apps.ap_invoices_interface
                       SET pay_awt_group_id = l_awt_group_id,
                           pay_awt_group_name = invoice_header_import_rec.LEG_AWT_GROUP_NAME
                     WHERE invoice_id = l_invoice_stg_id;
                 EXCEPTION
                    WHEN OTHERS
                    THEN
                       l_awt_group_id := NULL;
                 END;


              END IF;

              IF (invoice_lines_import_rec.LEG_AWT_GROUP_NAME IS NOT NULL)
              THEN
                 BEGIN
                    l_awt_group_id := NULL;

                    SELECT GROUP_ID
                      INTO l_awt_group_id
                      FROM ap_awt_groups
                     WHERE UPPER (NAME) =
                                  UPPER (invoice_lines_import_rec.LEG_AWT_GROUP_NAME);

                    UPDATE apps.ap_invoice_lines_interface
                       SET pay_awt_group_id = l_awt_group_id,
                           pay_awt_group_name = invoice_lines_import_rec.LEG_AWT_GROUP_NAME,
                           awt_group_name = NULL
                     WHERE invoice_line_id = l_invoice_stg_line_id;
                 EXCEPTION
                    WHEN OTHERS
                    THEN
                       l_awt_group_id := NULL;
                 END;
              END IF;

            END IF;

            IF l_country = 'TH'
            THEN
              IF (invoice_header_import_rec.LEG_AWT_GROUP_NAME IS NOT NULL)
              THEN
                 BEGIN
                    l_awt_group_id := NULL;

                    SELECT GROUP_ID
                      INTO l_awt_group_id
                      FROM ap_awt_groups
                     WHERE UPPER (NAME) =
                                  UPPER (invoice_header_import_rec.LEG_AWT_GROUP_NAME);

                    UPDATE apps.ap_invoices_interface
                       SET pay_awt_group_id = l_awt_group_id,
                           pay_awt_group_name = invoice_header_import_rec.LEG_AWT_GROUP_NAME
                     WHERE invoice_id = l_invoice_stg_id;
                 EXCEPTION
                    WHEN OTHERS
                    THEN
                       l_awt_group_id := NULL;
                 END;
              END IF;

              IF (invoice_lines_import_rec.LEG_AWT_GROUP_NAME IS NOT NULL)
              THEN
                 BEGIN
                    l_awt_group_id := NULL;

                    SELECT GROUP_ID
                      INTO l_awt_group_id
                      FROM ap_awt_groups
                     WHERE UPPER (NAME) =
                                  UPPER (invoice_lines_import_rec.LEG_AWT_GROUP_NAME);

                    UPDATE apps.ap_invoice_lines_interface
                       SET pay_awt_group_id = l_awt_group_id,
                           pay_awt_group_name = invoice_lines_import_rec.LEG_AWT_GROUP_NAME,
                           awt_group_name = NULL
                     WHERE invoice_line_id = l_invoice_stg_line_id;
                 EXCEPTION
                    WHEN OTHERS
                    THEN
                       l_awt_group_id := NULL;
                 END;
              END IF;

            END IF;

            IF l_country = 'ES'
            THEN
              l_intra_eu := 0;
              l_business_category := NULL;
              l_fisc_classification := NULL;
              l_doc_subtype := NULL;
              l_taxation_country := NULL;

              BEGIN
                 SELECT apsa.country, aps.vendor_type_lookup_code
                   INTO l_supp_country, l_supp_type
                   FROM apps.ap_supplier_sites_all apsa, apps.ap_suppliers aps
                  WHERE aps.segment1 = invoice_header_import_rec.leg_vendor_num
                    AND apsa.vendor_id = aps.vendor_id
                    AND apsa.vendor_site_code =
                                        invoice_header_import_rec.leg_vendor_site_code
                    AND NVL (apsa.inactive_date, SYSDATE) >= SYSDATE
                    AND NVL (aps.start_date_active, SYSDATE) <= SYSDATE
                    AND NVL (aps.end_date_active, SYSDATE) >= SYSDATE
                    AND apsa.org_id = invoice_header_import_rec.org_id;
              EXCEPTION
                 WHEN OTHERS
                 THEN
                    l_supp_country := NULL;
              END;

              l_business_category := 'PURCHASE_TRANSACTION';
              l_fisc_classification := NULL;
              l_doc_subtype := 'MOD340/R';
              l_taxation_country := 'ES';

              IF NVL (l_supp_country, '-XX') <> 'ES'
              THEN
                 BEGIN
                    SELECT 1
                      INTO l_intra_eu
                      FROM mtl_country_assignments_v
                     WHERE territory_code = l_supp_country
                       AND zone_code = 'EC'
                       AND ROWNUM = 1;
                 EXCEPTION
                    WHEN OTHERS
                    THEN
                       l_intra_eu := 0;
                 END;
              END IF;

              IF NVL (l_supp_country, '-XX') <> 'ES' AND l_intra_eu = 1
              THEN
                 --Third party Intra-EU transactions
                 IF UPPER (l_supp_type) <> 'EATON CORPORATION'
                 THEN
                    l_fisc_classification := 'MOD340P';
                    l_doc_subtype := 'MOD340/R';
                 ELSE
                    --Intercompany Intra EU Transactions
                    l_fisc_classification := NULL;
                    l_doc_subtype := 'MOD340/UB';
                 END IF;
              END IF;

              --END IF;
              UPDATE apps.ap_invoices_interface
                 SET document_sub_type = l_doc_subtype,
                     taxation_country = l_taxation_country,
                     add_tax_to_inv_amt_flag = 'N'
               WHERE invoice_id = l_invoice_stg_id;


              UPDATE apps.ap_invoice_lines_interface
                 SET trx_business_category = l_business_category
                     ,user_defined_fisc_class = l_fisc_classification
               WHERE invoice_line_id = l_invoice_stg_line_id;

            END IF;
          -- Added by Ankur for Poland Country Specific Changes V 1.6
          -- End

            l_error_flag := 'N';
          EXCEPTION
            WHEN OTHERS THEN
              l_error_flag := 'Y';
              l_err_msg    := 'Error while importing invoice lines';

              UPDATE xxap_invoice_lines_stg
                 SET process_flag           = g_error,
                     run_sequence_id        = g_run_seq_id,
                     error_type             = g_err_imp,
                     last_updated_date      = SYSDATE,
                     last_updated_by        = g_user_id,
                     last_update_login      = g_login_id,
                     program_application_id = g_prog_appl_id,
                     program_id             = g_conc_program_id,
                     program_update_date    = SYSDATE,
                     request_id             = g_request_id
               WHERE interface_txn_id =
                     invoice_lines_import_rec.interface_txn_id;

              log_errors(pov_return_status       => l_log_ret_status -- OUT
                        ,
                         pov_error_msg           => l_log_err_msg -- OUT
                        ,
                         piv_source_column_name  => NULL,
                         piv_source_column_value => NULL,
                         piv_error_type          => g_err_imp,
                         piv_error_code          => SQLCODE,
                         piv_error_message       => l_err_msg);
          END;

          BEGIN
            IF l_error_flag = 'N' THEN
              xxetn_debug_pkg.add_debug(piv_debug_msg => 'update lines staging table for converted data');

              UPDATE xxap_invoice_lines_stg
                 SET process_flag           = g_processed,
                     run_sequence_id        = g_run_seq_id,
                     error_type             = g_err_imp,
                     last_updated_date      = SYSDATE,
                     last_updated_by        = g_user_id,
                     last_update_login      = g_login_id,
                     program_application_id = g_prog_appl_id,
                     program_id             = g_conc_program_id,
                     program_update_date    = SYSDATE,
                     request_id             = g_request_id
               WHERE interface_txn_id =
                     invoice_lines_import_rec.interface_txn_id;
            END IF;
          EXCEPTION
            WHEN OTHERS THEN
              l_err_msg := 'Error while updating staging table with status P while importing invoice lines' ||
                           SUBSTR(SQLERRM, 1, 150);

              UPDATE xxap_invoices_stg
                 SET process_flag           = g_error,
                     run_sequence_id        = g_run_seq_id,
                     error_type             = g_err_imp,
                     last_updated_date      = SYSDATE,
                     last_updated_by        = g_user_id,
                     last_update_login      = g_login_id,
                     program_application_id = g_prog_appl_id,
                     program_id             = g_conc_program_id,
                     program_update_date    = SYSDATE,
                     request_id             = g_request_id
               WHERE interface_txn_id =
                     invoice_header_import_rec.interface_txn_id;

              log_errors(pov_return_status       => l_log_ret_status -- OUT
                        ,
                         pov_error_msg           => l_log_err_msg -- OUT
                        ,
                         piv_source_column_name  => NULL,
                         piv_source_column_value => NULL,
                         piv_error_type          => g_err_imp,
                         piv_error_code          => SQLCODE,
                         piv_error_message       => l_err_msg);
          END;
        END IF;
      END LOOP;
    END LOOP;

    COMMIT;
  EXCEPTION
    WHEN OTHERS THEN
      fnd_file.put_line(fnd_file.LOG,
                        'Error in Insert create_invoice main loop ');
  END create_invoice;

  --
  -- ========================
  -- Procedure: validate_hold_invoice
  -- =============================================================================
  --   This procedure will first check if the transaction is already on hold due the
  --   same hold reason as received from legacy in the hold staging table.
  --   If yes, it will not process it further and report it the log file. In addition
  --   it will perform further validations on the hold invoices
  -- =============================================================================
  --  Input Parameters :

  --  Output Parameters :

  -- -----------------------------------------------------------------------------
  --
  PROCEDURE validate_hold_invoice IS
    l_err_msg          VARCHAR2(4000);
    l_error_flag       VARCHAR2(10);
    l_return_status    VARCHAR2(200) := NULL;
    l_log_ret_status   VARCHAR2(50);
    l_log_err_msg      VARCHAR2(2000);
    l_error_message    VARCHAR2(4000);
    l_hold_code        VARCHAR2(100) := NULL;
    l_hold_type        VARCHAR2(100) := NULL;
    l_hold_count       NUMBER := NULL;
    l_calling_sequence VARCHAR2(1000);
    l_success          VARCHAR2(1);
    l_org_id           NUMBER;
    l_sob_id           NUMBER;
    l_operating_unit   VARCHAR2(100);
    l_error_cnt        NUMBER;
    l_err_code         VARCHAR2(100);
    l_invoice_id       NUMBER;

    -- --------------------------------------------------------------------------
    -- Cursor to select the new invoice data from staging table
    -- --------------------------------------------------------------------------
    CURSOR invoice_hold_cur IS
    /*        SELECT xihs.*,
                        aia.invoice_id invoice_id_r12,
                        aia.org_id org_idr12
                   FROM xxap_invoice_holds_stg xihs,
                                                    --  xxap_invoices_stg xis,
                                                    ap_invoices_all aia, ap_suppliers asp
                  WHERE xihs.leg_invoice_number = aia.invoice_num
                    --   AND xis.leg_invoice_num = xihs.leg_invoice_number
                     --  AND xis.leg_operating_unit = xihs.leg_operating_unit
                    AND aia.vendor_id = asp.vendor_id
                    AND xihs.leg_supplier_number = asp.segment1
                    AND xihs.batch_id = g_new_batch_id
                    AND aia.SOURCE = 'CONVERSION'
                    AND xihs.process_flag = g_new;
        */
      SELECT xihs.interface_txn_id,
             xihs.leg_operating_unit,
             xihs.leg_supplier_number,
             xihs.leg_hold_lookup_code,
             xihs.hold_lookup_code,
             xihs.leg_invoice_number,
             xis.org_id
        FROM xxap_invoice_holds_stg xihs, xxap_invoices_stg xis
       WHERE xihs.leg_invoice_id = xis.leg_invoice_id
         AND xihs.batch_id = g_new_batch_id
         AND xihs.process_flag = g_new;


  BEGIN
    -- ----------------------------
    -- For each unprocessed Record
    -- ----------------------------
    FOR invoice_hold_rec IN invoice_hold_cur LOOP
      print_log_message('Interface Transaction Id = ' ||
                        invoice_hold_rec.interface_txn_id);
      print_log_message('Invoice Num = ' ||
                        invoice_hold_rec.leg_invoice_number);
      g_intf_staging_id := invoice_hold_rec.interface_txn_id;
      l_error_flag      := NULL;
      l_error_cnt  := 0;
      l_err_code   := NULL;
      l_err_msg    := NULL;
      -------------  Hold validations
      /*
               --Derive org_id from Operating Unit
               validate_operating_unit (invoice_hold_rec.leg_operating_unit,
                                        l_operating_unit,
                                        l_org_id,
                                        l_sob_id,
                                        l_error_cnt
                                       );

               IF l_error_cnt > 0
               THEN
                  l_error_flag               := g_yes;
                  l_err_code                 := 'ETN_AP_INVALID_OPERATING_UNIT';
                  l_err_msg                  :=
                                              'Error: Operating Unit name is not valid for hold invoice.';
                  log_errors (pov_return_status            => l_log_ret_status                      -- OUT
                                                                              ,
                              pov_error_msg                => l_log_err_msg                         -- OUT
                                                                           ,
                              piv_source_column_name       => 'LEG_OPERATING_UNIT',
                              piv_source_column_value      => invoice_hold_rec.leg_operating_unit,
                              piv_error_type               => g_err_val,
                              piv_error_code               => l_err_code,
                              piv_error_message            => l_err_msg
                             );
               END IF;

               IF (l_org_id IS NOT NULL)
               THEN
      */
      BEGIN
        /*SELECT invoice_id
                     INTO l_invoice_id
                     FROM ap_invoices_all
                    WHERE invoice_num = invoice_hold_rec.leg_invoice_number AND org_id = l_org_id;
        */
        SELECT aia.invoice_id
          INTO l_invoice_id
          FROM ap_invoices_all aia, ap_suppliers asp
         WHERE aia.invoice_num = invoice_hold_rec.leg_invoice_number
           AND aia.vendor_id = asp.vendor_id
           AND asp.segment1 = invoice_hold_rec.leg_supplier_number
           AND aia.org_id = invoice_hold_rec.org_id
           AND aia.SOURCE = 'CONVERSION';

        print_log_message('Invoice Id = ' || l_invoice_id);
      EXCEPTION
        WHEN NO_DATA_FOUND THEN
          l_error_cnt  := 2;
          l_invoice_id := NULL;
          print_log_message('In No Data found of invoice_exists check' ||
                            SQLERRM);
        WHEN OTHERS THEN
          l_error_cnt  := 2;
          l_invoice_id := NULL;
          print_log_message('In When others of invoice_exists check' ||
                            SQLERRM);
      END;

      IF l_error_cnt > 0 THEN
        l_error_flag := g_yes;
        l_err_code   := 'ETN_AP_INVALID_INVOICE';
        l_err_msg    := 'Error: Invoice to be placed on hold doesnt exist.';
        log_errors(pov_return_status       => l_log_ret_status -- OUT
                  ,
                   pov_error_msg           => l_log_err_msg -- OUT
                  ,
                   piv_source_column_name  => 'LEG_INVOICE_NUMBER',
                   piv_source_column_value => invoice_hold_rec.leg_invoice_number,
                   piv_error_type          => g_err_val,
                   piv_error_code          => l_err_code,
                   piv_error_message       => l_err_msg);
      END IF;
      --     END IF;

      BEGIN
        --v2.00
        /*SELECT DISTINCT hold_lookup_code, hold_type
          INTO l_hold_code, l_hold_type
          FROM ap_hold_codes
         WHERE (hold_lookup_code) = (invoice_hold_rec.leg_hold_lookup_code);*/

         SELECT DISTINCT apc.hold_lookup_code, apc.hold_type
         INTO l_hold_code, l_hold_type
         FROM ap_hold_codes apc, fnd_lookup_Values_vl flv
         WHERE (apc.hold_lookup_code) = flv.tag
         AND   flv.lookup_type = 'XXETN_INV_HLD_MAP'
         AND flv.description = invoice_hold_rec.leg_hold_lookup_code
         AND flv.enabled_flag = 'Y';
        --v2.00 ends here
        print_log_message('Hold Lookup Code = ' || l_hold_code);
        print_log_message('Hold type = ' || l_hold_type);

        SELECT COUNT(*)
          INTO l_hold_count
          FROM ap_holds_all
         WHERE invoice_id = l_invoice_id
           AND hold_lookup_code = l_hold_code;
         print_log_message('l_hold_count: ' || l_hold_count||'for Invoice ID '||l_invoice_id ||' - '||
                  l_hold_code);
        IF l_hold_count >= 1 THEN
          l_error_flag := g_yes;
          l_err_msg    := l_err_msg || 'Cannot Apply Hold as Hold Code :' ||
                          l_hold_code ||
                          'already exists';
          print_log_message('l_err_msg: ' || l_err_msg);
          log_errors(pov_return_status       => l_log_ret_status -- OUT
                    ,
                     pov_error_msg           => l_log_err_msg -- OUT
                    ,
                     piv_source_column_name  => 'LEG_INVOICE_NUMBER',
                     piv_source_column_value => invoice_hold_rec.leg_invoice_number,
                     piv_error_type          => g_err_val,
                     piv_error_code          => 'ETN_AP_INVOICE_HOLD_EXISTS',
                     piv_error_message       => l_err_msg);
        END IF;

        print_log_message('Invoice Hold validated. No Errors');
      EXCEPTION
         WHEN no_data_found
         THEN
            l_error_flag := g_yes;
            l_err_code   := 'ETN_AP_INVALID_HOLD_INVOICE';
            l_err_msg    := 'Error: Hold Mapping Does Not Exists OR Hold Not Present in R12';
            print_log_message('Error: Hold Mapping Does Not Exists OR Hold Not Present in R12 ' ||
                            SQLERRM);
            log_errors(pov_return_status       => l_log_ret_status -- OUT
                  ,
                   pov_error_msg           => l_log_err_msg -- OUT
                  ,
                   piv_source_column_name  => 'LEG_HOLD_LOOKUP_CODE',
                   piv_source_column_value => invoice_hold_rec.leg_hold_lookup_code,
                   piv_error_type          => g_err_val,
                   piv_error_code          => l_err_code,
                   piv_error_message       => l_err_msg);
      END;

      /*******************************************************/
      /* updating Invoice hold staging table with the result      */
      /*******************************************************/
      IF l_error_flag = g_yes THEN
        print_log_message('updating staging table with process flag as error');

        UPDATE xxap_invoice_holds_stg
           SET process_flag = g_error,
               --vendor_id = rec_invoice.vendor_id,
               --                   leg_supplier_number = invoice_hold_rec.leg_supplier_number,
               --                 leg_supplier_site = invoice_hold_rec.leg_supplier_site,
               --               invoice_id = invoice_hold_rec.invoice_id,
               --               org_id = invoice_hold_rec.org_id,
               request_id       = g_request_id,
               hold_lookup_code = NULL,
               hold_type        = NULL
         WHERE interface_txn_id = invoice_hold_rec.interface_txn_id;

        COMMIT;
      ELSE
        print_log_message('updating staging table with process flag as validated');

        UPDATE xxap_invoice_holds_stg
           SET process_flag = g_validated,
               --vendor_id = rec_invoice.vendor_id,
               --leg_supplier_number = invoice_hold_rec.leg_supplier_number,
               --leg_supplier_site   = invoice_hold_rec.leg_supplier_site,
               --    operating_unit = l_operating_unit,
               org_id           = invoice_hold_rec.org_id,
               invoice_id       = l_invoice_id,
               request_id       = g_request_id,
               hold_lookup_code = l_hold_code,
               hold_type        = l_hold_type
         WHERE interface_txn_id = invoice_hold_rec.interface_txn_id;

        COMMIT;
        l_error_flag := NULL;
      END IF;
    END LOOP;
  EXCEPTION
    WHEN OTHERS THEN
      log_errors(pov_return_status       => l_log_ret_status -- OUT
                ,
                 pov_error_msg           => l_log_err_msg -- OUT
                ,
                 piv_source_column_name  => NULL,
                 piv_source_column_value => NULL,
                 piv_error_type          => g_err_imp,
                 piv_error_code          => 'ETN_AP_INVOICE_HOLD_VAL_ERROR',
                 piv_error_message       => 'AP Invoice Holds Validation Error' ||
                                            SUBSTR(SQLERRM, 1, 2000));
      fnd_file.put_line(fnd_file.output,
                        'Error In Invoice Hold Validations ');
  END validate_hold_invoice;

  --
  -- ========================
  -- Procedure: HOLD_INVOICE
  -- =============================================================================
  --   This procedure will first check if the transaction is already on hold due the
  --   same hold reason as received from legacy in the hold staging table.
  --   If yes, it will not process it further and report it the log file.
  --   If no then it will apply hold on the transaction using API.
  -- =============================================================================
  --  Input Parameters :
  --  pin_batch_id      : Batch id
  --  Output Parameters :
  --  pov_errbuf        : Return error
  --  pon_retcode       : Return status
  -- -----------------------------------------------------------------------------
  --
  PROCEDURE hold_invoice(pov_errbuf   OUT NOCOPY VARCHAR2,
                         pon_retcode  OUT NOCOPY NUMBER,
                         pin_dummy    IN VARCHAR2,
                         pin_batch_id IN NUMBER -- NULL / <BATCH_ID>
                         ) IS
    l_err_msg          VARCHAR2(4000);
    l_error_flag       VARCHAR2(10);
    l_return_status    VARCHAR2(200) := NULL;
    l_log_ret_status   VARCHAR2(50);
    l_log_err_msg      VARCHAR2(2000);
    l_error_message    VARCHAR2(4000);
    l_hold_code        VARCHAR2(100) := NULL;
    l_hold_type        VARCHAR2(100) := NULL;
    l_hold_count       NUMBER := NULL;
    l_calling_sequence VARCHAR2(1000);
    l_success          VARCHAR2(1);

    ---Cursor for fetching Invoice holds
    CURSOR val_invoices_hold_cur(pin_org_id IN NUMBER) IS
      SELECT xihs.*
        FROM xxap_invoice_holds_stg xihs
       WHERE xihs.process_flag = g_validated
         AND xihs.org_id = pin_org_id
         AND xihs.batch_id = pin_batch_id
       ORDER BY interface_txn_id;

    CURSOR ins_ap_invoices_hold_org_cur IS
      SELECT DISTINCT org_id
        FROM xxap_invoice_holds_stg xihs
       WHERE process_flag = g_validated
         AND xihs.batch_id = pin_batch_id
         AND xihs.request_id = g_request_id;
  BEGIN
    xxetn_debug_pkg.initialize_debug(pov_err_msg      => l_err_msg,
                                     piv_program_name => 'ETN_AP_INVOICES_CONV_HOLD');
    print_log_message('Invoice Hold Program Starts at: ' ||
                      TO_CHAR(g_sysdate, 'DD-MON-YYYY HH24:MI:SS'));
    print_log_message('+ Start of Invoice Hold Program + ' || pin_batch_id);
    g_new_batch_id                      := pin_batch_id;
    g_run_seq_id                        := xxetn_run_sequences_s.NEXTVAL;
    xxetn_common_error_pkg.g_run_seq_id := g_run_seq_id;

    IF (pin_batch_id IS NULL) THEN
      print_log_message('Batch ID cannot be null');
      pon_retcode := 2;
    ELSE
      --Initialize global variables for error framework
      g_source_table    := g_invoice_hold_t;
      g_intf_staging_id := NULL;
      g_src_keyname1    := NULL;
      g_src_keyvalue1   := NULL;
      g_src_keyname2    := NULL;
      g_src_keyvalue2   := NULL;
      g_src_keyname3    := NULL;
      g_src_keyvalue3   := NULL;
      g_src_keyname4    := NULL;
      g_src_keyvalue4   := NULL;
      g_src_keyname5    := NULL;
      g_src_keyvalue5   := NULL;

      validate_hold_invoice();

      --apply hold to all the validated invoices
      FOR ins_ap_invoices_hold_org_rec IN ins_ap_invoices_hold_org_cur LOOP
        mo_global.set_policy_context('S',
                                     ins_ap_invoices_hold_org_rec.org_id);

        --end;
        FOR val_invoices_hold_rec IN val_invoices_hold_cur(ins_ap_invoices_hold_org_rec.org_id) LOOP
          l_error_message := NULL;

          BEGIN
            fnd_global.apps_initialize(user_id      => g_user_id,
                                       resp_id      => g_resp_id,
                                       resp_appl_id => g_resp_appl_id);
          END;

          BEGIN
            l_success := '';
            -- DBMS_OUTPUT.put_line (' Insert header LOOP Upload Procedure ');
            fnd_file.put_line(fnd_file.output,
                              'Invoice Hold Upload Started');

            BEGIN
              mo_global.init('SQLAP');
              mo_global.set_org_context(val_invoices_hold_rec.org_id,
                                        ' ',
                                        'S');
              l_calling_sequence := NULL;
              ap_holds_pkg.insert_single_hold(x_invoice_id       => val_invoices_hold_rec.invoice_id,
                                              x_hold_lookup_code => val_invoices_hold_rec.hold_lookup_code,
                                              x_hold_type        => val_invoices_hold_rec.hold_type,
                                              x_hold_reason      => val_invoices_hold_rec.leg_hold_reason,
                                              x_held_by          => g_user_id,
                                              x_calling_sequence => l_calling_sequence);
            END;

            l_success := 'Y';
          EXCEPTION
            WHEN OTHERS THEN
              l_error_flag    := g_error;
              l_success       := 'N';
              l_error_message := l_error_message ||
                                 'Failed to insert the record into AP_INVOICE_HOLDS table' ||
                                 CHR(10);
              log_errors(pov_return_status       => l_log_ret_status -- OUT
                        ,
                         pov_error_msg           => l_log_err_msg -- OUT
                        ,
                         piv_source_column_name  => 'LEG_INVOICE_NUMBER',
                         piv_source_column_value => val_invoices_hold_rec.leg_invoice_number,
                         piv_error_type          => g_err_imp,
                         piv_error_code          => 'ETN_AP_INVOICE_HOLD_API',
                         piv_error_message       => l_error_message);
          END;

          IF l_success = 'Y' THEN
            UPDATE xxap_invoice_holds_stg
               SET process_flag = g_converted
             WHERE interface_txn_id =
                   val_invoices_hold_rec.interface_txn_id;
          ELSE
            UPDATE xxap_invoice_holds_stg
               SET process_flag = g_error
             WHERE interface_txn_id =
                   val_invoices_hold_rec.interface_txn_id;
          END IF;
        END LOOP; /* Header Level */
      END LOOP;
    END IF;
  EXCEPTION
    WHEN OTHERS THEN
      log_errors(pov_return_status       => l_log_ret_status -- OUT
                ,
                 pov_error_msg           => l_log_err_msg -- OUT
                ,
                 piv_source_column_name  => NULL,
                 piv_source_column_value => NULL,
                 piv_error_type          => g_err_imp,
                 piv_error_code          => 'ETN_AP_INVOICE_HOLD_API_MAIN',
                 piv_error_message       => 'AP Invoice Holds API Main Error' ||
                                            SUBSTR(SQLERRM, 1, 2000));
      fnd_file.put_line(fnd_file.output, 'Error In Invoice Hold Main ');
      pon_retcode := 1;
  END hold_invoice;

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
  PROCEDURE tie_back(pov_errbuf   OUT NOCOPY VARCHAR2,
                     pon_retcode  OUT NOCOPY NUMBER,
                     pin_dummy    IN VARCHAR2,
                     pin_batch_id IN NUMBER -- NULL / <BATCH_ID>
                     ) IS
    l_err_msg        VARCHAR2(4000);
    l_error_flag     VARCHAR2(10);
    l_return_status  VARCHAR2(200) := NULL;
    l_log_ret_status VARCHAR2(50);
    l_log_err_msg    VARCHAR2(2000);

    --cursor to fetch all successfully inserted header records
    CURSOR tie_back_hdr_cur IS
      SELECT *
        FROM xxap_invoices_stg xis
       WHERE xis.process_flag = g_processed
         AND xis.batch_id = pin_batch_id
       ORDER BY xis.leg_invoice_num;

    --cursor to fetch all successfully inserted lines records
    CURSOR tie_back_line_cur IS
      SELECT *
        FROM xxap_invoice_lines_stg xis
       WHERE xis.process_flag = g_processed
         AND xis.batch_id = pin_batch_id
       ORDER BY xis.leg_invoice_num;

    --cursor to fetch header error details
    CURSOR interface_error_hdr_cur(piv_invoice_num IN VARCHAR2,
                                   pin_org_id      IN NUMBER,
                                   piv_vendor_num  IN VARCHAR2) IS
      SELECT api.invoice_num, apr.reject_lookup_code reject_lookup_code
        FROM ap_interface_rejections apr, ap_invoices_interface api
       WHERE api.invoice_id = apr.parent_id
         AND api.invoice_num = piv_invoice_num
         AND api.vendor_num = piv_vendor_num
         AND api.org_id = pin_org_id
         AND apr.parent_table = 'AP_INVOICES_INTERFACE';

    --cursor to fetch line error details
    CURSOR interface_error_line_cur(piv_invoice_num IN VARCHAR2,
                                    pin_org_id      IN NUMBER,
                                    piv_vendor_num  IN VARCHAR2,
                                    pin_line_number IN NUMBER) IS
      SELECT api.invoice_num        invoice_num,
             apr.reject_lookup_code reject_lookup_code
        FROM ap_interface_rejections    apr,
             ap_invoice_lines_interface apli,
             ap_invoices_interface      api
       WHERE apli.invoice_line_id = apr.parent_id
         AND api.invoice_id = apli.invoice_id
         AND api.invoice_num = piv_invoice_num
         AND apli.org_id = pin_org_id
         AND apli.line_number = pin_line_number
         AND apr.parent_table = 'AP_INVOICE_LINES_INTERFACE';
  BEGIN
    xxetn_debug_pkg.initialize_debug(pov_err_msg      => l_err_msg,
                                     piv_program_name => 'ETN_AP_INV_CONV_TIEBACK');
    print_log_message('Tie Back Starts at: ' ||
                      TO_CHAR(g_sysdate, 'DD-MON-YYYY HH24:MI:SS'));
    print_log_message('+ Start of Tie Back + ' || pin_batch_id);
    g_run_seq_id                        := xxetn_run_sequences_s.NEXTVAL;
    xxetn_common_error_pkg.g_run_seq_id := g_run_seq_id;

    --tie back for invoice header
    FOR tie_back_hdr_rec IN tie_back_hdr_cur LOOP
      print_log_message('Interface Transaction Id = ' ||
                        tie_back_hdr_rec.interface_txn_id);
      print_log_message('Invoice Num = ' ||
                        tie_back_hdr_rec.leg_invoice_num);
      g_intf_staging_id := tie_back_hdr_rec.interface_txn_id;
      l_error_flag      := NULL;

      FOR interface_error_hdr_rec IN interface_error_hdr_cur(tie_back_hdr_rec.leg_invoice_num,
                                                             tie_back_hdr_rec.org_id,
                                                             tie_back_hdr_rec.leg_vendor_num) LOOP
        print_log_message('In error loop: Invoice Number= ' ||
                          interface_error_hdr_rec.invoice_num);
        print_log_message('In error loop: reject_lookup_code - ' ||
                          interface_error_hdr_rec.reject_lookup_code);
        l_error_flag   := g_error;
        g_source_table := g_invoice_t;
        log_errors(pov_return_status       => l_log_ret_status -- OUT
                  ,
                   pov_error_msg           => l_log_err_msg -- OUT
                  ,
                   piv_source_column_name  => 'Interface Error',
                   piv_source_column_value => NULL,
                   piv_error_type          => g_err_imp,
                   piv_error_code          => 'ETN_AP_INVOICE_CREATION_FAILED',
                   piv_error_message       => interface_error_hdr_rec.reject_lookup_code);
      END LOOP;

      IF l_error_flag = g_error THEN
        UPDATE xxap_invoices_stg xis
           SET process_flag      = g_error,
               error_type        = g_err_imp,
               run_sequence_id   = g_run_seq_id,
               last_updated_date = g_sysdate,
               last_updated_by   = g_user_id,
               last_update_login = g_login_id
         WHERE xis.interface_txn_id = tie_back_hdr_rec.interface_txn_id;

        COMMIT;
      ELSE
        UPDATE xxap_invoices_stg xis
           SET process_flag      = g_converted,
               run_sequence_id   = g_run_seq_id,
               last_updated_date = g_sysdate,
               last_updated_by   = g_user_id,
               last_update_login = g_login_id
         WHERE xis.interface_txn_id = tie_back_hdr_rec.interface_txn_id;

        COMMIT;
      END IF;

      g_intf_staging_id := NULL;
    END LOOP;

    --tie back for invoice line
    FOR tie_back_line_rec IN tie_back_line_cur LOOP
      print_log_message('Interface Transaction Id = ' ||
                        tie_back_line_rec.interface_txn_id);
      print_log_message('Invoice Num = ' ||
                        tie_back_line_rec.leg_invoice_num);
      g_intf_staging_id := tie_back_line_rec.interface_txn_id;
      l_error_flag      := NULL;

      FOR interface_error_line_rec IN interface_error_line_cur(tie_back_line_rec.leg_invoice_num,
                                                               tie_back_line_rec.org_id,
                                                               tie_back_line_rec.leg_vendor_num,
                                                               tie_back_line_rec.leg_line_number) LOOP
        print_log_message('In error loop: Invoice Num for line= ' ||
                          interface_error_line_rec.invoice_num);
        print_log_message('In error loop: reject_lookup_code - ' ||
                          interface_error_line_rec.reject_lookup_code);
        l_error_flag   := g_error;
        g_source_table := g_invoice_line_t;
        log_errors(pov_return_status       => l_log_ret_status -- OUT
                  ,
                   pov_error_msg           => l_log_err_msg -- OUT
                  ,
                   piv_source_column_name  => 'Interface Error',
                   piv_source_column_value => NULL,
                   piv_error_type          => g_err_imp,
                   piv_error_code          => 'ETN_AP_INVOICE_LINE_IMP_FAILED',
                   piv_error_message       => interface_error_line_rec.reject_lookup_code);
      END LOOP;

      IF l_error_flag = g_error THEN
        UPDATE xxap_invoice_lines_stg xis
           SET process_flag      = g_error,
               error_type        = g_err_imp,
               run_sequence_id   = g_run_seq_id,
               last_updated_date = g_sysdate,
               last_updated_by   = g_user_id,
               last_update_login = g_login_id
         WHERE xis.interface_txn_id = tie_back_line_rec.interface_txn_id;

        COMMIT;
      ELSE
        UPDATE xxap_invoice_lines_stg xis
           SET process_flag      = g_converted,
               run_sequence_id   = g_run_seq_id,
               last_updated_date = g_sysdate,
               last_updated_by   = g_user_id,
               last_update_login = g_login_id
         WHERE xis.interface_txn_id = tie_back_line_rec.interface_txn_id;

        COMMIT;
      END IF;

      g_intf_staging_id := NULL;
    END LOOP;
    --v2.0
    UPDATE xxap_invoices_stg xas
    SET process_flag       = g_error
    WHERE batch_id         = g_new_batch_id
    AND   run_sequence_id  = g_run_seq_id
    AND   process_flag     = g_converted
    AND EXISTS (SELECT 1 FROM xxap_invoice_lines_stg xil
                WHERE xil.batch_id         = g_new_batch_id
                AND   xil.run_sequence_id  = g_run_seq_id
                AND   xil.process_flag = g_error
                AND xil.leg_invoice_id = xas.leg_invoice_id);
    COMMIT;

    UPDATE xxap_invoice_lines_stg xil
    SET process_flag       = g_error
    WHERE batch_id         = g_new_batch_id
    AND   run_sequence_id  = g_run_seq_id
    AND   process_flag     = g_converted
    AND EXISTS (SELECT 1 FROM xxap_invoices_stg xas
                WHERE xas.batch_id         = g_new_batch_id
                AND   xas.run_sequence_id  = g_run_seq_id
                AND   xas.process_flag = g_error
                AND xas.leg_invoice_id = xil.leg_invoice_id);

    COMMIT;
    --v2.0 Ends here
    IF g_source_tab.COUNT > 0 THEN
      xxetn_common_error_pkg.add_error(pov_return_status => l_return_status -- OUT
                                      ,
                                       pov_error_msg     => l_err_msg -- OUT
                                      ,
                                       pi_source_tab     => g_source_tab
                                       -- IN  G_SOURCE_TAB_TYPE
                                      ,
                                       pin_batch_id => pin_batch_id);
      g_source_tab.DELETE;
    END IF;

    pon_retcode := g_retcode;
    pov_errbuf  := g_errbuff;
    print_log_message('- Start of Tie Back - ' || pin_batch_id);
    print_log_message('+---------------------------------------------------------------------------+');
    print_log_message('Tie Back Ends at: ' ||
                      TO_CHAR(g_sysdate, 'DD-MON-YYYY HH24:MI:SS'));
    print_log_message('---------------------------------------------');
  EXCEPTION
    WHEN OTHERS THEN
      g_retcode := 2;
      g_errbuff := 'Failed During Tie Back';
      print_log_message('In Tie Back when others' || SQLERRM);
  END tie_back;

  --
  -- ========================
  -- Procedure: main
  -- =============================================================================
  --   This is a main public procedure, which will be invoked through concurrent
  --   program.
  --
  --   This conversion program is used to validate AP open invoice data
  --   from legacy system.0$ AP open invoices are created and put on hold
  --   if necessary
  -- =============================================================================
  --
  -- -----------------------------------------------------------------------------
  --  Called By Concurrent Program: Eaton AP Invoice Conversion Program
  -- -----------------------------------------------------------------------------
  -- -----------------------------------------------------------------------------
  --
  --  Input Parameters :
  --    piv_run_mode        : Control the program execution for VALIDATE and CONVERSION
  --    piv_hidden          : Dummy variable
  --    pin_batch_id        : List all unique batches from staging table , this will
  --                        be NULL for first Conversion Run.
  --    piv_dummy           : Dummy variable
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
  PROCEDURE main(pov_errbuf          OUT NOCOPY VARCHAR2,
                 pon_retcode         OUT NOCOPY NUMBER,
                 piv_run_mode        IN VARCHAR2 -- pre validate/validate/conversion/reconcile
                ,
                 piv_hidden          IN VARCHAR2 -- dummy variable
                ,
                 piv_operating_unit  IN VARCHAR2,
                 pin_batch_id        IN NUMBER -- null / <batch_id>
                ,
                 piv_dummy           IN VARCHAR2 -- dummy variable
                ,
                 piv_process_records IN VARCHAR2 -- (a) all / (e) error only / (n) unprocessed
                ,
                 piv_gl_date         IN VARCHAR2) IS
    l_debug_on       BOOLEAN;
    l_retcode        VARCHAR2(1) := 'S';
    l_return_status  VARCHAR2(200) := NULL;
    l_err_code       VARCHAR2(40) := NULL;
    l_err_msg        VARCHAR2(2000) := NULL;
    l_log_ret_status VARCHAR2(50) := NULL;
    l_log_err_msg    VARCHAR2(2000);
  BEGIN
    --Initialize global variables for error framework
    g_source_table    := g_invoice_t;
    g_intf_staging_id := NULL;
    g_src_keyname1    := NULL;
    g_src_keyvalue1   := NULL;
    g_src_keyname2    := NULL;
    g_src_keyvalue2   := NULL;
    g_src_keyname3    := NULL;
    g_src_keyvalue3   := NULL;
    g_src_keyname4    := NULL;
    g_src_keyvalue4   := NULL;
    g_src_keyname5    := NULL;
    g_src_keyvalue5   := NULL;
    xxetn_debug_pkg.initialize_debug(pov_err_msg      => l_err_msg,
                                     piv_program_name => 'ETN_AP_OPEN_INV_CONV');

    -- If error initializing debug messages
    IF l_err_msg IS NOT NULL THEN
      pon_retcode := 2;
      pov_errbuf  := l_err_msg;
      print_log_message('Debug Initialization failed');
      RETURN;
    END IF;

    print_log_message('Started AP Invoice Conversion at ' ||
                      TO_CHAR(g_sysdate, 'DD-MON-YYYY HH24:MI:SS'));
    print_log_message('+---------------------------------------------------------------------------+');
    -- Check if debug logging is enabled/disabled
    g_run_mode        := piv_run_mode;
    g_batch_id        := pin_batch_id;
    g_process_records := piv_process_records;
    g_operating_unit  := piv_operating_unit;
    g_gl_date         := TO_DATE(piv_gl_date, 'YYYY/MM/DD:HH24:MI:SS');

    -- Call Common Debug and Error Framework initialization
    print_log_message('Program Parameters  : ');
    print_log_message('---------------------------------------------');
    print_log_message('Run Mode            : ' || g_run_mode);
    print_log_message('Batch ID            : ' || pin_batch_id);
    print_log_message('Process records     : ' || g_process_records);

    IF piv_run_mode = 'LOAD-DATA' THEN
      xxetn_debug_pkg.add_debug('In Load Data Mode');
      --call the procedure to load data from extraction tables into staging table
      --get_data();
      load_data;
      print_stat();
    END IF;

    IF piv_run_mode = 'PRE-VALIDATE' THEN
      xxetn_debug_pkg.add_debug('In Pre-Validate Mode');
      --call the procedure to check if the custom setups are done
      pre_validate();
      pon_retcode := g_retcode;
    END IF;

    IF piv_run_mode = 'VALIDATE' THEN
      IF g_gl_date IS NULL THEN
        print_log_message('Exiting Program as GL Date is not passed');
        g_retcode := 2;
        RETURN;
      END IF;

      IF g_batch_id IS NULL THEN
        g_new_batch_id := xxetn_batches_s.NEXTVAL;
        xxetn_debug_pkg.add_debug('New Batch Id' || g_new_batch_id);
        g_run_seq_id := xxetn_run_sequences_s.NEXTVAL;
        xxetn_debug_pkg.add_debug('New Run Sequence ID : ' || g_run_seq_id);
      ELSE
        g_new_batch_id := g_batch_id;
        g_run_seq_id   := xxetn_run_sequences_s.NEXTVAL;
        xxetn_debug_pkg.add_debug('New Run Sequence ID : ' || g_run_seq_id);
      END IF;

      assign_batch_id(); --API for assigning batch id
      print_log_message('New Batch ID            : ' || g_new_batch_id);
      --if assign_batch_id fails, exit the program
      IF (g_retcode != 0) THEN
        xxetn_debug_pkg.add_debug('Assign Batch ID failed.Program ended. ');
        RETURN;
      END IF;

      --Setting the run sequence id for error framework
      xxetn_common_error_pkg.g_run_seq_id := g_run_seq_id;
      xxetn_debug_pkg.add_debug('In Validate Mode');
      --call the procedure for validating data for invoices
      validate_invoice();
      print_stat();
    END IF;

    IF piv_run_mode = 'CONVERSION' THEN
      xxetn_debug_pkg.add_debug('In Conversion Mode');

      IF (pin_batch_id IS NULL) THEN
        xxetn_debug_pkg.add_debug('Batch ID is mandatory for CONVERSION mode ');
        pon_retcode := 2;
        RETURN;
      ELSE
        g_run_seq_id := xxetn_run_sequences_s.NEXTVAL;
        xxetn_debug_pkg.add_debug('New Run Sequence ID : ' || g_run_seq_id);
        g_new_batch_id := pin_batch_id;
        --Setting the run sequence id for error framework
        xxetn_common_error_pkg.g_run_seq_id := g_run_seq_id;
        xxetn_debug_pkg.add_debug('---------------------------------------------');
        xxetn_debug_pkg.add_debug('PROCEDURE: create_invoice' || CHR(10));
        --procedure to import AP invoices
        create_invoice;
      END IF; -- IF pin_batch_id IS NULL

      print_stat();
    END IF;

    IF piv_run_mode = 'RECONCILE' THEN
      xxetn_debug_pkg.add_debug('In Reconcile Mode');
      g_new_batch_id := pin_batch_id;
      -- get stats for the program
      xxetn_debug_pkg.add_debug(piv_debug_msg => 'Calling print stat');
      --Call the procedure to print the statistics of the records processed by the conversion
      print_stat();
    END IF;

    print_log_message('+---------------------------------------------------------------------------+');
    print_log_message('AP Invoice Conversion Ends at: ' ||
                      TO_CHAR(g_sysdate, 'DD-MON-YYYY HH24:MI:SS'));
    pon_retcode := g_retcode;
    pov_errbuf  := g_errbuff;

    IF g_source_tab.COUNT > 0 THEN
      xxetn_common_error_pkg.add_error(pov_return_status => l_return_status -- OUT
                                      ,
                                       pov_error_msg     => l_err_msg -- OUT
                                      ,
                                       pi_source_tab     => g_source_tab
                                       -- IN  G_SOURCE_TAB_TYPE
                                      ,
                                       pin_batch_id => g_new_batch_id);
    END IF;
  EXCEPTION
    WHEN OTHERS THEN
      pov_errbuf  := 'Error : Main program procedure encounter error. ' ||
                     SUBSTR(SQLERRM, 1, 150);
      pon_retcode := 2;
      print_log_message(pov_errbuf);
  END main;
END xxap_invoices_pkg;
/
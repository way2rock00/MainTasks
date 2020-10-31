CREATE OR REPLACE PACKAGE BODY XXAP_SUPPLIER_CNV_PKG

------------------------------------------------------------------------------------------
--    Owner        : EATON CORPORATION.
--    Application  : Accounts Payables
--    Schema       : APPS
--    Compile AS   : APPS
--    File Name    : XXAP_SUPPLIER_PKG.pkb
--    Date         : 23-Jan-2014
--    Author       : Deepak Dakhore/Archita Monga
--    Description  : Package Body for Supplier conversion
--
--    Version      : $ETNHeader: /CCSTORE/ccweb/C9915567/C9915567_/vobs/AP_TOP/xxap/12.0.0/install/XXAP_SUPPLIER_CNV_PKG.pkb /main/8 09-Jul-2015 08:52:13 C9915567  $
--
--    Parameters  :
--
--    Change History
--    Version     Created By       Date            Comments
--  ======================================================================================
--    v1.0        Archita Monga    25-Mar-2014     Initial Creation
--    v1.1        Kulraj Singh     18-Jul-2014     Added another Entity to load IBAN and
--                                                 Intermediary Bank details. CR# 228493
--    v1.1        Kulraj Singh     20-Aug-2014     Removed references of OU Lookup.
--                                                 Updated OU derivation logic using ETN Map Unit Utility
--    v1.2        Kulraj Singh     23-Dec-2014     Updated Supplier Site Email Address mapping for Defect# 219,447
--                                                 Defect# 294 (CR#259992) change on DUNS#
--    v1.3        Biswajit Sen     25-Jun-2015     updated the leg columns that are present in 11i
--    v1.4        Abhijit Pande    04-Jul-2015     Added logic for localization requirements for Brazil, Spain, Thiland, Chile, China etc.
--    v1.5        Deepak Dakhore   25-Sep-2015     Added logic for Last_name, Attribute 2, 3, and 12 as per PMC#317614/322734
--    v1.6        Deepak Dakhore   30-Sep-2015     Duplicate Site code logic
--                                                 - If vendor name, Vendor site and Address  line 1 is same , take the uniqe one and suppress the other one
--                                                 - If vendor name, Vendor site and Address  line 1 is different , take the site code concatenated by vendor_ID.
--    v1.7        Deepak Dakhore   12-Apr-2015     MOCK2 CR# 372676 changes - Vat code TAX: Update to Supplier Conversion
--                              -All countries, China, EMEA (PTP-CNV-0001) : Enable VAT fields
--    v1.8        Deepak Dakhore   04-May-2016     MOCK3 CR# 339760 Changes
--    v1.9        Deepak Dakhore   04-May-2016     Defect : 1028 ,4894,4358,4700,4971,5521
--                                                        Alternate Pay site/ Pay on code
--    v2.0        Deepak Dakhore   04-May-2016     Tieback procedure change exclude interface
--                                                 txn id from error message field.R12.
--    v2.1        Deepak Dakhore   04-June-2016    Mock3 CR#385966 - BIC Number logic to derive new branches for ISSC ONLY
--    v2.2        Deepak Dakhore   13-June-2016    Mock3 CR#393268 :- Allow International payment flag set as default for suppliers banks
--    v2.3        Deepak Dakhore   14-June-2016    Supplier Header - leg_num_1099 is less than 20 characters.
--    v3.0        Aditya           02-Oct-2016     CR changes for MOCK5
--    v4.0        Aditya Bhagat    07-Jan-2016     bug fixes and improvements after MOCK5
--    ====================================================================================
------------------------------------------------------------------------------------------
 AS

  -- global variables
  g_request_id      NUMBER DEFAULT fnd_global.conc_request_id;
  g_prog_appl_id    NUMBER DEFAULT fnd_global.prog_appl_id;
  g_conc_program_id NUMBER DEFAULT fnd_global.conc_program_id;
  g_user_id         NUMBER DEFAULT fnd_global.user_id;
  g_login_id        NUMBER DEFAULT fnd_global.login_id;
  g_org_id          NUMBER DEFAULT fnd_global.org_id;
  g_set_of_books_id NUMBER DEFAULT fnd_profile.value('GL_SET_OF_BKS_ID');
  g_retcode         NUMBER := 0;
  g_errbuff         VARCHAR2(1);
   --changes for cross reference v1.9
    lv_out_val1 VARCHAR2(200);
    lv_out_val2 VARCHAR2(200);
    lv_out_val3 VARCHAR2(200);
    lv_err_msg1 VARCHAR2(2000);
    --changes for cross reference ends v1.9
  g_normal    CONSTANT NUMBER := 0;
  g_warning   CONSTANT NUMBER := 1;
  g_ret_error CONSTANT NUMBER := 2;

  g_sysdate      CONSTANT DATE := SYSDATE;
  g_batch_source CONSTANT VARCHAR2(30) := 'Conversion';
  g_source_fsc   CONSTANT VARCHAR2(30) := 'FSC';
  g_source_issc  CONSTANT VARCHAR2(30) := 'ISSC';
  g_source_table VARCHAR2(30);

  g_supplier            CONSTANT VARCHAR2(50) := 'SUPPLIERS';
  g_supplier_sites      CONSTANT VARCHAR2(50) := 'SUPPLIER_SITES';
  g_supplier_contacts   CONSTANT VARCHAR2(50) := 'SUPPLIER_CONTACTS';
  g_bank                CONSTANT VARCHAR2(50) := 'SUPPLIER_BANKS';
  g_branch              CONSTANT VARCHAR2(50) := 'SUPPLIER_BRANCHES';
  g_account             CONSTANT VARCHAR2(50) := 'SUPPLIER_BANK_ACCOUNTS';
  g_int_accts           CONSTANT VARCHAR2(50) := 'SUPPLIER_INTERMEDIARY_ACCOUNTS'; -- v1.1
  g_supplier_t          CONSTANT VARCHAR2(30) := 'XXAP_SUPPLIERS_STG';
  g_supplier_sites_t    CONSTANT VARCHAR2(30) := 'XXAP_SUPPLIER_SITES_STG';
  g_supplier_contacts_t CONSTANT VARCHAR2(30) := 'XXAP_SUPPLIER_CONTACTS_STG';
  g_bank_t              CONSTANT VARCHAR2(30) := 'XXAP_SUPPLIER_BANKS_STG';
  g_branch_t            CONSTANT VARCHAR2(30) := 'XXAP_SUPPLIER_BRANCHES_STG';
  g_account_t           CONSTANT VARCHAR2(30) := 'XXAP_SUPPLIER_BANKACCNTS_STG';
  g_int_accts_t         CONSTANT VARCHAR2(30) := 'XXAP_SUPPLIER_INT_ACCTS_STG';

  -- SQL Loader Program Variables (v1.1)
  g_ctl_file_name  CONSTANT VARCHAR2(50) := 'XXAP_SUPPLIER_INT_ACCTS_CTL.ctl';
  g_load_prog_appl CONSTANT fnd_application.application_short_name%TYPE := 'XXAP';
  g_load_prog_name CONSTANT fnd_concurrent_programs.concurrent_program_name%TYPE := 'XXAP_SUPP_INTACCTS_LOAD';

  g_run_sequence_id NUMBER;
  g_new               CONSTANT VARCHAR2(1) := 'N';
  g_error             CONSTANT VARCHAR2(1) := 'E';
  g_validated         CONSTANT VARCHAR2(1) := 'V';
  g_obsolete          CONSTANT VARCHAR2(1) := 'X';
  g_processed         CONSTANT VARCHAR2(1) := 'P';
  g_converted         CONSTANT VARCHAR2(1) := 'C';
  g_success           CONSTANT VARCHAR2(1) := 'S';
  g_yes               CONSTANT VARCHAR2(1) := 'Y';
  g_ricew_id          CONSTANT VARCHAR2(10) := 'CNV-0001';
  g_created_by_module CONSTANT VARCHAR2(10) := 'TCA_V1_API';
  g_init_msg_list     CONSTANT VARCHAR2(20) := fnd_api.g_true;

  g_run_mode        VARCHAR2(100);
  g_entity          VARCHAR2(100);
  g_process_records VARCHAR2(100);
  g_data_file       VARCHAR2(240); -- v1.1
  g_gl_date         DATE;
  g_err_code        VARCHAR2(100);
  g_err_message     VARCHAR2(2000);
  g_failed_count    NUMBER;
  g_total_count     NUMBER;

  g_load_id NUMBER;
  g_indx    NUMBER := 0;
  g_limit   CONSTANT NUMBER := fnd_profile.value('ETN_FND_ERROR_TAB_LIMIT');
  g_err_imp CONSTANT VARCHAR2(10) := 'ERR_IMP';
  g_err_val CONSTANT VARCHAR2(10) := 'ERR_VAL';
  g_err_int CONSTANT VARCHAR2(10) := 'ERR_INT';

  g_batch_id     NUMBER;
  g_new_batch_id NUMBER;
  g_run_seq_id   NUMBER;

  g_source_Tab xxetn_common_error_pkg.g_source_tab_type;

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

  g_bulk_exception EXCEPTION;
  PRAGMA EXCEPTION_INIT(g_bulk_exception, -24381);

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
                       piv_error_code          IN xxetn_common_error.error_code%TYPE,
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
    g_source_Tab(g_indx).source_table := g_source_table;
    g_source_Tab(g_indx).interface_staging_id := g_intf_staging_id;
    g_source_Tab(g_indx).source_keyname1 := g_src_keyname1;
    g_source_Tab(g_indx).source_keyvalue1 := g_src_keyvalue1;
    g_source_Tab(g_indx).source_keyname2 := g_src_keyname2;
    g_source_Tab(g_indx).source_keyvalue2 := g_src_keyvalue2;
    g_source_Tab(g_indx).source_keyname3 := g_src_keyname3;
    g_source_Tab(g_indx).source_keyvalue3 := g_src_keyvalue3;
    g_source_Tab(g_indx).source_keyname4 := g_src_keyname4;
    g_source_Tab(g_indx).source_keyvalue4 := g_src_keyvalue4;
    g_source_Tab(g_indx).source_keyname5 := g_src_keyname5;
    g_source_Tab(g_indx).source_keyvalue5 := g_src_keyvalue5;
    g_source_Tab(g_indx).source_column_name := piv_source_column_name;
    g_source_Tab(g_indx).source_column_value := piv_source_column_value;
    g_source_Tab(g_indx).error_type := piv_error_type;
    g_source_Tab(g_indx).error_code := piv_error_code;
    g_source_Tab(g_indx).error_message := piv_error_message;
    g_source_Tab(g_indx).severity := piv_severity;
    g_source_Tab(g_indx).proposed_solution := piv_proposed_solution;

    IF MOD(g_indx, g_limit) = 0 THEN

      xxetn_common_error_pkg.add_error(pov_return_status => l_return_status -- OUT
                                      ,
                                       pov_error_msg     => l_error_msg -- OUT
                                      ,
                                       pi_source_tab     => g_source_Tab -- IN  G_SOURCE_TAB_TYPE
                                      ,
                                       pin_batch_id      => g_new_batch_id);

      g_source_Tab.DELETE;

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

    fnd_file.put_line(fnd_file.log, piv_message);

  END;


----------------------------------------------------------------------------------------

  /********************************************************************************************
         NAME:       Update_Bank_Num
         PURPOSE:    This procedure will update supplier bank number

  *********************************************************************************************/
PROCEDURE Update_Bank_Num IS
  l_status_flag       VARCHAR2(1);
  l_error_message     VARCHAR2(500);
  l_return_status_out VARCHAR2(1);
  l_msg_count_out     NUMBER;
  l_msg_data_out      VARCHAR2(1000);
  l_msg_index_out     NUMBER;

  l_bank_id             NUMBER;
  l_location_id         NUMBER;
  l_party_site_id       NUMBER;
  l_party_site_number   NUMBER;
  l_org_contact_id      NUMBER;
  l_org_party_id        NUMBER;
  l_email_cont_point_id NUMBER;
  l_phone_cont_point_id NUMBER;

  l_bank_msg_data       VARCHAR2(2000);
  l_loc_msg_data        VARCHAR2(2000);
  l_party_site_msg_data VARCHAR2(2000);
  l_org_cont_msg_data   VARCHAR2(2000);
  l_phone_cont_msg_data VARCHAR2(2000);
  l_email_cont_msg_data VARCHAR2(2000);

  l_bank_ret_status       VARCHAR2(50);
  l_loc_ret_status        VARCHAR2(50);
  l_site_ret_status       VARCHAR2(50);
  l_state_ret_status      VARCHAR2(50);
  l_upd_ret_status        VARCHAR2(50);
  l_org_cont_ret_status   VARCHAR2(50);
  l_phone_cont_ret_status VARCHAR2(50);
  l_email_cont_ret_status VARCHAR2(50);

  l_log_ret_status VARCHAR2(50);
  l_log_err_msg    VARCHAR2(2000);

  l_retcode   VARCHAR2(1);
  l_err_code  VARCHAR2(40);
  l_err_msg   VARCHAR2(2000);
  l_msg_count NUMBER;
  g_retcode   NUMBER := 0;
  g_error   CONSTANT VARCHAR2(1) := 'E';
  g_success CONSTANT VARCHAR2(1) := 'S';

  l_extbank_rec_type iby_ext_bankacct_pub.extbank_rec_type;
  l_result_rec       iby_fndcpt_common_pub.result_rec_type;

Cursor C is
  Select leg_country, leg_bank_name, leg_bank_number, bank_party_id,leg_bank_institution_type
    from xxconv.XXAP_SUPPLIER_BANKS_STG;

Begin

  l_retcode   := NULL;
  l_msg_count := NULL;
  l_err_code  := NULL;
  l_err_msg   := NULL;

for i in c loop
  --Assign staging table values to the bank record type to be passed in the API
  l_extbank_rec_type.object_version_number := 2.0;
  l_extbank_rec_type.bank_name             := i.leg_bank_name;
  l_extbank_rec_type.bank_id               := i.bank_party_id;
  l_extbank_rec_type.bank_number           := i.leg_bank_number;
  l_extbank_rec_type.institution_type      := i.leg_bank_institution_type;
  l_extbank_rec_type.country_code          := i.leg_country;
  l_extbank_rec_type.bank_alt_name := NULL;

  iby_ext_bankacct_pub.update_ext_bank(p_api_version   => 1.0,
                                       p_init_msg_list => fnd_api.g_true,
                                       p_ext_bank_rec  => l_extbank_rec_type,
                                       x_return_status => l_bank_ret_status,
                                       x_msg_count     => l_msg_count,
                                       x_msg_data      => l_bank_msg_data,
                                       x_response      => l_result_rec);

  print_log_message('Status :- ' || l_bank_ret_status);

  IF l_bank_ret_status <> fnd_api.g_ret_sts_success THEN
    g_retcode  := 1;
    l_retcode  := g_error;
    l_err_code := 'ETN_AP_BANK_IMPORT_ERROR';
    l_err_msg  := 'Error : Supplier Bank creation failed.';

    IF l_msg_count > 0 THEN
      FOR i IN 1 .. l_msg_count LOOP
        l_bank_msg_data := fnd_msg_pub.get(p_msg_index => i,
                                           p_encoded   => fnd_api.g_false);
        print_log_message('Message :- ' || l_bank_msg_data);
  END LOOP;
    END IF;
     ROLLBACK;
  ELSE

    COMMIT;
    print_log_message('Bank account number updated Successful ' || i.leg_bank_name || '-' || i.leg_bank_number);
    l_retcode := g_success;
  END IF;
 END LOOP;
 COMMIT;
 END Update_Bank_Num;
-----------------------------------------------------------------------------------------



  /********************************************************************************************
         NAME:       submit_request
         PURPOSE:    This procedure will submit request and wait for request to complete
         Input Parameters:
                   piv_application  : Application Short Name of program which is to be submitted
                   piv_program_name : Program Short Name to be submitted
                   piv_argument1    : Program Paramter 1
         Output Parameters:
                   pon_request_id:   : Returns Request Id which is submitted
                   pov_return_status : Return Status as 'S' or 'E' or 'W'
                   pov_return_msg    : Return Error Message
  *********************************************************************************************/

  PROCEDURE submit_request(pon_request_id    OUT NOCOPY NUMBER,
                           pov_return_status OUT NOCOPY VARCHAR2,
                           pov_return_msg    OUT NOCOPY VARCHAR2,
                           piv_argument1     IN VARCHAR2 DEFAULT NULL)

   IS

    PRAGMA AUTONOMOUS_TRANSACTION;

    l_submit_failed EXCEPTION;
    l_wait_failed EXCEPTION;
    l_error_message VARCHAR2(500);
    l_phase         VARCHAR2(80) DEFAULT NULL;
    l_status        VARCHAR2(80) DEFAULT NULL;
    l_dev_phase     VARCHAR2(80) DEFAULT NULL;
    l_dev_status    VARCHAR2(80) DEFAULT NULL;
    l_message       VARCHAR2(240) DEFAULT NULL;
    l_req_st        BOOLEAN;

  BEGIN

    xxetn_debug_pkg.add_debug('In Begin of Proc: SUBMIT_REQUEST');

    pon_request_id    := 0;
    pov_return_status := 'S';
    pov_return_msg    := NULL;

    /** Submit Request **/
    pon_request_id := fnd_request.submit_request(application => g_load_prog_appl,
                                                 program     => g_load_prog_name,
                                                 argument1   => piv_argument1,
                                                 argument2   => NULL,
                                                 argument3   => NULL,
                                                 argument4   => NULL);

    IF NVL(pon_request_id, 0) = 0 THEN
      RAISE l_submit_failed;
    ELSE
      xxetn_debug_pkg.add_debug('Request submitted with Id: ' ||
                                pon_request_id);
      COMMIT;

      -----------------------------------------
      /* Waiting for Request to Complete */
      -----------------------------------------
      LOOP
        l_req_st := fnd_concurrent.wait_for_request(request_id => pon_request_id,
                                                    interval   => 0,
                                                    max_wait   => 0,
                                                    phase      => l_phase,
                                                    status     => l_status,
                                                    dev_phase  => l_dev_phase,
                                                    dev_status => l_dev_status,
                                                    message    => l_message);
        EXIT WHEN l_dev_phase = 'COMPLETE';
      END LOOP;
      IF l_dev_status = 'WARNING' THEN
        pov_return_status := 'W';
        l_error_message   := ' Request Wait Completed in Warning. Reason: ' ||
                             l_message;
        RAISE l_wait_failed;
      ELSIF l_dev_status = 'ERROR' THEN
        pov_return_status := 'E';
        l_error_message   := ' Request Wait Completed in Error. Reason: ' ||
                             l_message;
        RAISE l_wait_failed;
      END IF;

    END IF;

  EXCEPTION
    WHEN l_submit_failed THEN
      pon_request_id  := 0;
      l_error_message := 'Unable to Submit Request. Reason: ' ||
                         SUBSTR(SQLERRM, 1, 250);
      xxetn_debug_pkg.add_debug('Unable to Submit Request. Reason: ' ||
                                SUBSTR(SQLERRM, 1, 250));

      pov_return_status := 'E';
      pov_return_msg    := l_error_message;

    WHEN l_wait_failed THEN
      pon_request_id := 0;
      xxetn_debug_pkg.add_debug(l_error_message);
      pov_return_msg := l_error_message;

    WHEN OTHERS THEN
      pon_request_id    := 0;
      pov_return_status := 'E';
      pov_return_msg    := 'SQL Error. SUBMIT_REQUEST. Error: ' ||
                           SUBSTR(SQLERRM, 1, 150);
      xxetn_debug_pkg.add_debug('SQL Error. SUBMIT_REQUEST. Error: ' ||
                                SUBSTR(SQLERRM, 1, 150));
  END submit_request;

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

    l_pass_val1 NUMBER := 0;
    l_err_val1  NUMBER := 0;
    l_tot_val1  NUMBER := 0;

    l_pass_val2 NUMBER := 0;
    l_err_val2  NUMBER := 0;
    l_tot_val2  NUMBER := 0;

    l_pass_imp1 NUMBER := 0;
    l_err_imp1  NUMBER := 0;
    l_tot_imp1  NUMBER := 0;

    l_pass_imp2 NUMBER := 0;
    l_err_imp2  NUMBER := 0;
    l_tot_imp2  NUMBER := 0;

  BEGIN

    xxetn_debug_pkg.add_debug(' + Print_stat + ');
    xxetn_debug_pkg.add_debug('Program Name : Eaton AP Supplier Conversion Program ');
    fnd_file.put_line(fnd_file.OUTPUT,
                      'Program Name : Eaton AP Supplier Conversion Program ');
    fnd_file.put_line(fnd_file.OUTPUT,
                      'Request ID   : ' || TO_CHAR(g_request_id));
    fnd_file.put_line(fnd_file.OUTPUT,
                      'Report Date  : ' ||
                      TO_CHAR(SYSDATE, 'DD-MON-RRRR HH:MI:SS AM'));
    fnd_file.put_line(fnd_file.OUTPUT,
                      '-------------------------------------------------------------------------------------------------');
    fnd_file.put_line(fnd_file.OUTPUT, 'Parameters  : ');
    fnd_file.put_line(fnd_file.OUTPUT,
                      '---------------------------------------------');
    fnd_file.put_line(fnd_file.OUTPUT,
                      'Run Mode            : ' || g_run_mode);
    fnd_file.put_line(fnd_file.OUTPUT,
                      'Entity              : ' || g_entity);
    fnd_file.put_line(fnd_file.OUTPUT,
                      'Batch ID            : ' || g_batch_id);
    fnd_file.put_line(fnd_file.OUTPUT,
                      'Process records     : ' || g_process_records);
    fnd_file.put_line(fnd_file.OUTPUT,
                      'Data File           : ' || g_data_file);
    fnd_file.put_line(fnd_file.OUTPUT,
                      '===================================================================================================');
    fnd_file.put_line(fnd_file.OUTPUT,
                      'Statistics (' || g_run_mode || '):');

    IF UPPER(g_run_mode) = 'LOAD-DATA' THEN
      IF g_entity = g_int_accts THEN
        fnd_file.PUT_LINE(fnd_file.OUTPUT,
                          'Records loaded to Staging Table   : ' ||
                          g_total_count);
      ELSE
        fnd_file.PUT_LINE(fnd_file.OUTPUT,
                          'Records Eligible            : ' || g_total_count);
        fnd_file.PUT_LINE(fnd_file.OUTPUT,
                          'Records Pulled              : ' ||
                          (g_total_count - g_failed_Count));
        fnd_file.PUT_LINE(fnd_file.OUTPUT,
                          'Records Errored             : ' ||
                          g_failed_Count);
      END IF;
    END IF;

    -- Get COUNTS for BANK
    IF g_entity = g_bank THEN
      --count for all the records processed
      SELECT COUNT(1)
        INTO l_tot_val1
        FROM xxap_supplier_banks_stg stg
       WHERE stg.batch_id = NVL(g_new_batch_id, stg.batch_id)
         AND stg.run_sequence_id = NVL(g_run_seq_id, stg.run_sequence_id);

      --count for all the records which errored out while validating
      SELECT COUNT(1)
        INTO l_err_val1
        FROM xxap_supplier_banks_stg stg
       WHERE stg.batch_id = NVL(g_new_batch_id, stg.batch_id)
         AND stg.run_sequence_id = NVL(g_run_seq_id, stg.run_sequence_id)
         AND stg.process_flag = g_error
         AND stg.error_type = g_err_val;

      --count for all the records which errored out while importing
      SELECT COUNT(1)
        INTO l_err_imp1
        FROM xxap_supplier_banks_stg stg
       WHERE stg.batch_id = NVL(g_new_batch_id, stg.batch_id)
         AND stg.run_sequence_id = NVL(g_run_seq_id, stg.run_sequence_id)
         AND stg.process_flag = g_error
         AND stg.error_type = g_err_imp;

      --count for all the records which successfully got validated
      SELECT COUNT(1)
        INTO l_pass_val1
        FROM xxap_supplier_banks_stg stg
       WHERE stg.batch_id = NVL(g_new_batch_id, stg.batch_id)
         AND stg.run_sequence_id = NVL(g_run_seq_id, stg.run_sequence_id)
         AND stg.process_flag = g_validated;

      --count for all the records which successfully got converted
      SELECT COUNT(1)
        INTO l_pass_imp1
        FROM xxap_supplier_banks_stg stg
       WHERE stg.batch_id = NVL(g_new_batch_id, stg.batch_id)
         AND stg.run_sequence_id = NVL(g_run_seq_id, stg.run_sequence_id)
         AND stg.process_flag = g_converted;
    END IF;

    -- Get COUNTS for BRANCH
    IF g_entity = g_branch THEN
      --count for all the records processed
      SELECT COUNT(1)
        INTO l_tot_val1
        FROM xxap_supplier_branches_stg stg
       WHERE stg.batch_id = NVL(g_new_batch_id, stg.batch_id)
         AND stg.run_sequence_id = NVL(g_run_seq_id, stg.run_sequence_id);

      --count for all the records which errored out while validating
      SELECT COUNT(1)
        INTO l_err_val1
        FROM xxap_supplier_branches_stg stg
       WHERE stg.batch_id = NVL(g_new_batch_id, stg.batch_id)
         AND stg.run_sequence_id = NVL(g_run_seq_id, stg.run_sequence_id)
         AND stg.process_flag = g_error
         AND stg.error_type = g_err_val;

      --count for all the records which errored out while importing
      SELECT COUNT(1)
        INTO l_err_imp1
        FROM xxap_supplier_branches_stg stg
       WHERE stg.batch_id = NVL(g_new_batch_id, stg.batch_id)
         AND stg.run_sequence_id = NVL(g_run_seq_id, stg.run_sequence_id)
         AND stg.process_flag = g_error
         AND stg.error_type = g_err_imp;

      --count for all the records which successfully got validated
      SELECT COUNT(1)
        INTO l_pass_val1
        FROM xxap_supplier_branches_stg stg
       WHERE stg.batch_id = NVL(g_new_batch_id, stg.batch_id)
         AND stg.run_sequence_id = NVL(g_run_seq_id, stg.run_sequence_id)
         AND stg.process_flag = g_validated;

      --count for all the records which successfully got converted
      SELECT COUNT(1)
        INTO l_pass_imp1
        FROM xxap_supplier_branches_stg stg
       WHERE stg.batch_id = NVL(g_new_batch_id, stg.batch_id)
         AND stg.run_sequence_id = NVL(g_run_seq_id, stg.run_sequence_id)
         AND stg.process_flag = g_converted;
    END IF;

    -- Get COUNTS for ACCOUNTS
    IF g_entity = g_account THEN
      --count for all the records processed
      SELECT COUNT(1)
        INTO l_tot_val1
        FROM xxap_supplier_bankaccnts_stg stg
       WHERE stg.batch_id = NVL(g_new_batch_id, stg.batch_id)
         AND stg.run_sequence_id = NVL(g_run_seq_id, stg.run_sequence_id);

      --count for all the records which errored out while validating
      SELECT COUNT(1)
        INTO l_err_val1
        FROM xxap_supplier_bankaccnts_stg stg
       WHERE stg.batch_id = NVL(g_new_batch_id, stg.batch_id)
         AND stg.run_sequence_id = NVL(g_run_seq_id, stg.run_sequence_id)
         AND stg.process_flag = g_error
         AND stg.error_type = g_err_val;

      --count for all the records which errored out while importing
      SELECT COUNT(1)
        INTO l_err_imp1
        FROM xxap_supplier_bankaccnts_stg stg
       WHERE stg.batch_id = NVL(g_new_batch_id, stg.batch_id)
         AND stg.run_sequence_id = NVL(g_run_seq_id, stg.run_sequence_id)
         AND stg.process_flag = g_error
         AND stg.error_type = g_err_imp;

      --count for all the records which successfully got validated
      SELECT COUNT(1)
        INTO l_pass_val1
        FROM xxap_supplier_bankaccnts_stg stg
       WHERE stg.batch_id = NVL(g_new_batch_id, stg.batch_id)
         AND stg.run_sequence_id = NVL(g_run_seq_id, stg.run_sequence_id)
         AND stg.process_flag = g_validated;

      --count for all the records which successfully got converted
      SELECT COUNT(1)
        INTO l_pass_imp1
        FROM xxap_supplier_bankaccnts_stg stg
       WHERE stg.batch_id = NVL(g_new_batch_id, stg.batch_id)
         AND stg.run_sequence_id = NVL(g_run_seq_id, stg.run_sequence_id)
         AND stg.process_flag = g_converted;
    END IF;

    -- Get COUNTS for Suppliers
    IF g_entity = g_supplier THEN
      --count for all the records processed
      SELECT COUNT(1)
        INTO l_tot_val1
        FROM xxap_suppliers_stg xss
       WHERE xss.batch_id = NVL(g_new_batch_id, xss.batch_id)
         AND xss.run_sequence_id = NVL(g_run_seq_id, xss.run_sequence_id);

      --count for all the records which errored out while validating
      SELECT COUNT(1)
        INTO l_err_val1
        FROM xxap_suppliers_stg xss
       WHERE xss.batch_id = NVL(g_new_batch_id, xss.batch_id)
         AND xss.run_sequence_id = NVL(g_run_seq_id, xss.run_sequence_id)
         AND xss.process_flag = g_error
         AND xss.error_type = g_err_val;

      --count for all the records which errored out while importing
      SELECT COUNT(1)
        INTO l_err_imp1
        FROM xxap_suppliers_stg xss
       WHERE xss.batch_id = NVL(g_new_batch_id, xss.batch_id)
         AND xss.run_sequence_id = NVL(g_run_seq_id, xss.run_sequence_id)
         AND xss.process_flag = g_error
         AND xss.error_type = g_err_imp;

      --count for all the records which successfully got validated
      SELECT COUNT(1)
        INTO l_pass_val1
        FROM xxap_suppliers_stg xss
       WHERE xss.batch_id = NVL(g_new_batch_id, xss.batch_id)
         AND xss.run_sequence_id = NVL(g_run_seq_id, xss.run_sequence_id)
         AND xss.process_flag = g_validated;

      --count for all the records which successfully got interfaced
      SELECT COUNT(1)
        INTO l_pass_imp1
        FROM xxap_suppliers_stg xss
       WHERE xss.batch_id = NVL(g_new_batch_id, xss.batch_id)
         AND xss.run_sequence_id = NVL(g_run_seq_id, xss.run_sequence_id)
         AND xss.process_flag = g_processed;
    END IF;

    -- Get COUNTS for Supplier Sites
    IF g_entity = g_supplier_sites THEN
      --count for all the records processed
      SELECT COUNT(1)
        INTO l_tot_val1
        FROM xxap_supplier_sites_stg xsss
       WHERE xsss.batch_id = NVL(g_new_batch_id, xsss.batch_id)
         AND xsss.run_sequence_id = NVL(g_run_seq_id, xsss.run_sequence_id);

      --count for all the records which errored out while validating
      SELECT COUNT(1)
        INTO l_err_val1
        FROM xxap_supplier_sites_stg xsss
       WHERE xsss.batch_id = NVL(g_new_batch_id, xsss.batch_id)
         AND xsss.run_sequence_id = NVL(g_run_seq_id, xsss.run_sequence_id)
         AND xsss.process_flag = g_error
         AND xsss.error_type = g_err_val;

      --count for all the records which errored out while importing
      SELECT COUNT(1)
        INTO l_err_imp1
        FROM xxap_supplier_sites_stg xsss
       WHERE xsss.batch_id = NVL(g_new_batch_id, xsss.batch_id)
         AND xsss.run_sequence_id = NVL(g_run_seq_id, xsss.run_sequence_id)
         AND xsss.process_flag = g_error
         AND xsss.error_type = g_err_imp;

      --count for all the records which successfully got validated
      SELECT COUNT(1)
        INTO l_pass_val1
        FROM xxap_supplier_sites_stg xsss
       WHERE xsss.batch_id = NVL(g_new_batch_id, xsss.batch_id)
         AND xsss.run_sequence_id = NVL(g_run_seq_id, xsss.run_sequence_id)
         AND xsss.process_flag = g_validated;

      --count for all the records which successfully got interfaced
      SELECT COUNT(1)
        INTO l_pass_imp1
        FROM xxap_supplier_sites_stg xsss
       WHERE xsss.batch_id = NVL(g_new_batch_id, xsss.batch_id)
         AND xsss.run_sequence_id = NVL(g_run_seq_id, xsss.run_sequence_id)
         AND xsss.process_flag = g_processed;
    END IF;

    -- Get COUNTS for Supplier Contacts
    IF g_entity = g_supplier_contacts THEN
      --count for all the records processed
      SELECT COUNT(1)
        INTO l_tot_val1
        FROM xxap_supplier_contacts_stg xscs
       WHERE xscs.batch_id = NVL(g_new_batch_id, xscs.batch_id)
         AND xscs.run_sequence_id = NVL(g_run_seq_id, xscs.run_sequence_id);

      --count for all the records which errored out while validating
      SELECT COUNT(1)
        INTO l_err_val1
        FROM xxap_supplier_contacts_stg xscs
       WHERE xscs.batch_id = NVL(g_new_batch_id, xscs.batch_id)
         AND xscs.run_sequence_id = NVL(g_run_seq_id, xscs.run_sequence_id)
         AND xscs.process_flag = g_error
         AND xscs.error_type = g_err_val;

      --count for all the records which errored out while importing
      SELECT COUNT(1)
        INTO l_err_imp1
        FROM xxap_supplier_contacts_stg xscs
       WHERE xscs.batch_id = NVL(g_new_batch_id, xscs.batch_id)
         AND xscs.run_sequence_id = NVL(g_run_seq_id, xscs.run_sequence_id)
         AND xscs.process_flag = g_error
         AND xscs.error_type = g_err_imp;

      --count for all the records which successfully got validated
      SELECT COUNT(1)
        INTO l_pass_val1
        FROM xxap_supplier_contacts_stg xscs
       WHERE xscs.batch_id = NVL(g_new_batch_id, xscs.batch_id)
         AND xscs.run_sequence_id = NVL(g_run_seq_id, xscs.run_sequence_id)
         AND xscs.process_flag = g_validated;

      --count for all the records which successfully got interfaced
      SELECT COUNT(1)
        INTO l_pass_imp1
        FROM xxap_supplier_contacts_stg xscs
       WHERE xscs.batch_id = NVL(g_new_batch_id, xscs.batch_id)
         AND xscs.run_sequence_id = NVL(g_run_seq_id, xscs.run_sequence_id)
         AND xscs.process_flag = g_processed;
    END IF;

    /** Added for v1.1 **/

    -- Get COUNTS for Intermediary Accounts
    IF g_entity = g_int_accts THEN
      --count for all the records processed
      SELECT COUNT(1)
        INTO l_tot_val1
        FROM xxap_supplier_int_accts_stg xsias
       WHERE xsias.batch_id = NVL(g_new_batch_id, xsias.batch_id)
         AND xsias.run_sequence_id =
             NVL(g_run_seq_id, xsias.run_sequence_id);

      --count for all the records which errored out while validating
      SELECT COUNT(1)
        INTO l_err_val1
        FROM xxap_supplier_int_accts_stg xsias
       WHERE xsias.batch_id = NVL(g_new_batch_id, xsias.batch_id)
         AND xsias.run_sequence_id =
             NVL(g_run_seq_id, xsias.run_sequence_id)
         AND xsias.process_flag = g_error
         AND xsias.error_type = g_err_val;

      --count for all the records which errored out while importing
      SELECT COUNT(1)
        INTO l_err_imp1
        FROM xxap_supplier_int_accts_stg xsias
       WHERE xsias.batch_id = NVL(g_new_batch_id, xsias.batch_id)
         AND xsias.run_sequence_id =
             NVL(g_run_seq_id, xsias.run_sequence_id)
         AND xsias.process_flag = g_error
         AND xsias.error_type = g_err_imp;

      --count for all the records which successfully got validated
      SELECT COUNT(1)
        INTO l_pass_val1
        FROM xxap_supplier_int_accts_stg xsias
       WHERE xsias.batch_id = NVL(g_new_batch_id, xsias.batch_id)
         AND xsias.run_sequence_id =
             NVL(g_run_seq_id, xsias.run_sequence_id)
         AND xsias.process_flag = g_validated;

      --count for all the records which successfully got converted
      SELECT COUNT(1)
        INTO l_pass_imp1
        FROM xxap_supplier_int_accts_stg xsias
       WHERE xsias.batch_id = NVL(g_new_batch_id, xsias.batch_id)
         AND xsias.run_sequence_id =
             NVL(g_run_seq_id, xsias.run_sequence_id)
         AND xsias.process_flag = g_converted;
    END IF;

    IF g_run_mode = 'VALIDATE' THEN
      fnd_file.PUT_LINE(fnd_file.OUTPUT,
                        'Records Submitted              : ' || l_tot_val1);
      fnd_file.PUT_LINE(fnd_file.OUTPUT,
                        'Records Validated              : ' || l_pass_val1);
      fnd_file.PUT_LINE(fnd_file.OUTPUT,
                        'Records Errored                : ' || l_err_val1);

    ELSIF g_run_mode = 'CONVERSION' THEN
      fnd_file.PUT_LINE(fnd_file.OUTPUT,
                        'Records Submitted              : ' || l_tot_val1);
      fnd_file.PUT_LINE(fnd_file.OUTPUT,
                        'Records Interfaced/Imported    : ' || l_pass_imp1);
      fnd_file.PUT_LINE(fnd_file.OUTPUT,
                        'Records Errored                : ' || l_err_imp1);

    ELSIF g_run_mode = 'RECONCILE' THEN

      fnd_file.PUT_LINE(fnd_file.OUTPUT,
                        'Records Submitted              : ' || l_tot_val1);
      fnd_file.PUT_LINE(fnd_file.OUTPUT,
                        'Records Imported               : ' || l_pass_imp1);
      fnd_file.PUT_LINE(fnd_file.OUTPUT,
                        'Records Errored in Validation  : ' || l_err_val1);
      fnd_file.PUT_LINE(fnd_file.OUTPUT,
                        'Records Errored in Import      : ' || l_err_imp1);

    END IF;

    fnd_file.put_line(fnd_file.OUTPUT, CHR(10));
    fnd_file.put_line(fnd_file.OUTPUT,
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

  --Manoj:START
  --
  -- ========================
  -- Procedure: validate_fax_suppcont
  -- =============================================================================
  --   This procedure is used to
  -- =============================================================================
  --  Input Parameters :
  --    None
  --  Output Parameters :
  --    None
  -- -----------------------------------------------------------------------------
  --
  PROCEDURE validate_fax_suppcont(piv_fax     IN VARCHAR2,
                                  pov_fax     OUT VARCHAR2,
                                  pon_retcode OUT NUMBER) IS
    l_status VARCHAR2(1);

    l_log_ret_status VARCHAR2(50);
    l_log_err_msg    VARCHAR2(2000);
  BEGIN
    pon_retcode := g_normal;

    IF LENGTH(piv_fax) < 7 THEN
      pon_retcode := g_warning;
      log_errors(pov_return_status       => l_log_ret_status -- OUT
                ,
                 pov_error_msg           => l_log_err_msg -- OUT
                ,
                 piv_source_column_name  => 'LEG_FAX',
                 piv_source_column_value => piv_fax,
                 piv_error_type          => g_err_val,
                 piv_error_code          => 'ETN_AP_INVALID_FAX',
                 piv_error_message       => 'Error : Fax number length is less than 7');
    ELSIF LENGTH(piv_fax) > 7 THEN
      pov_fax := SUBSTR(piv_fax, 1, 2) || '-' || SUBSTR(piv_fax, 3, 2) || '-' ||
                 SUBSTR(piv_fax, 5);
    END IF;

  EXCEPTION
    WHEN OTHERS THEN
      pon_retcode := g_ret_error;
      print_log_message(SUBSTR('Exception in Procedure validate_fax_suppcont. ' ||
                               SQLERRM,
                               1,
                               2000));
  END validate_fax_suppcont;

  --
  -- ========================
  -- Procedure: validate_phone_suppcont
  -- =============================================================================
  --   This procedure is used to
  -- =============================================================================
  --  Input Parameters :
  --    None
  --  Output Parameters :
  --    None
  -- -----------------------------------------------------------------------------
  --
  PROCEDURE validate_phone_suppcont(piv_phone   IN VARCHAR2,
                                    pov_phone   OUT VARCHAR2,
                                    pon_retcode OUT NUMBER) IS
    l_status VARCHAR2(1);

    l_log_ret_status VARCHAR2(50);
    l_log_err_msg    VARCHAR2(2000);
  BEGIN
    pon_retcode := g_normal;

    IF LENGTH(piv_phone) < 7 THEN
      pon_retcode := g_warning;
      log_errors(pov_return_status       => l_log_ret_status -- OUT
                ,
                 pov_error_msg           => l_log_err_msg -- OUT
                ,
                 piv_source_column_name  => 'LEG_PHONE',
                 piv_source_column_value => piv_phone,
                 piv_error_type          => g_err_val,
                 piv_error_code          => 'ETN_AP_INVALID_PHONE',
                 piv_error_message       => 'Error : Phone number length is less than 7');
    ELSIF LENGTH(piv_phone) > 7 THEN
      pov_phone := SUBSTR(piv_phone, 1, 2) || '-' ||
                   SUBSTR(piv_phone, 3, 2) || '-' || SUBSTR(piv_phone, 5);
    END IF;

  EXCEPTION
    WHEN OTHERS THEN
      pon_retcode := g_ret_error;
      print_log_message(SUBSTR('Exception in Procedure validate_phone_suppcont. ' ||
                               SQLERRM,
                               1,
                               2000));
  END validate_phone_suppcont;

  --
  -- ========================
  -- Procedure: validate_continfo_suppcont
  -- =============================================================================
  --   This procedure is used to
  -- =============================================================================
  --  Input Parameters :
  --    None
  --  Output Parameters :
  --    None
  -- -----------------------------------------------------------------------------
  --
  PROCEDURE validate_continfo_suppcont(piv_first_name  IN VARCHAR2,
                                       piv_middle_name IN VARCHAR2,
                                       piv_last_name   IN VARCHAR2,
                                       pon_retcode     OUT NUMBER) IS
    l_status VARCHAR2(1);

    l_log_ret_status VARCHAR2(50);
    l_log_err_msg    VARCHAR2(2000);

    l_err_count     NUMBER := 0;
    l_error_message VARCHAR2(2000);
  BEGIN
    pon_retcode := g_normal;

    /**
          IF piv_first_name IS NULL THEN
             l_err_count := l_err_count + 1;
             l_error_message := 'Error : Vendor Legacy First Name is NULL';
          END IF;

          IF piv_middle_name IS NULL THEN
             l_err_count := l_err_count + 1;
             l_error_message := l_error_message||' ; '||'Error : Vendor Legacy Middle Name is NULL';
          END IF;
    **/

    IF piv_last_name IS NULL THEN
      l_err_count     := l_err_count + 1;
      l_error_message := l_error_message || ' ; ' ||
                         'Error : Vendor Legacy Last Name is NULL';
    END IF;

    IF l_err_count > 0 THEN
      pon_retcode := g_warning;
      log_errors(pov_return_status       => l_log_ret_status -- OUT
                ,
                 pov_error_msg           => l_log_err_msg -- OUT
                ,
                 piv_source_column_name  => 'LEG_LAST_NAME',
                 piv_source_column_value => piv_first_name || '-' ||
                                            piv_middle_name || '-' ||
                                            piv_last_name,
                 piv_error_type          => g_err_val,
                 piv_error_code          => 'ETN_AP_INVALID_CONTACT_INFO',
                 piv_error_message       => l_error_message);
    END IF;

  EXCEPTION
    WHEN OTHERS THEN
      pon_retcode := g_ret_error;
      print_log_message(SUBSTR('Exception in Procedure validate_continfo_suppcont. ' ||
                               SQLERRM,
                               1,
                               2000));
  END validate_continfo_suppcont;

  --
  -- ========================
  -- Procedure: validate_vendor_site_suppcont
  -- =============================================================================
  --   This procedure is used to validate Vendor Site for Supplier Contact
  -- =============================================================================
  --  Input Parameters :
  --   piv_vendor_name      : Vendor Name
  --   piv_vendor_site_code : Vendor Site
  --   pin_org_id           : Org id
  --  Output Parameters :
  --    pon_vendor_site_id  : Returns Vendor Id
  --    pov_vendor_site_code: Returns Vendor Site Id
  --    pon_retcode         : Returns Code
  -- -----------------------------------------------------------------------------
  --
  PROCEDURE validate_vendor_site_suppcont(piv_vendor_name      IN VARCHAR2,
                                          piv_vendor_site_code IN VARCHAR2,
                                          pin_org_id           IN NUMBER,
                                          pin_leg_vendor_site_id IN NUMBER,  -- to avoid error ETN_AP_INVALID_VENDOR_SITE of too may rows
                                          pon_vendor_site_id   OUT NUMBER,
                                          pov_vendor_site_code OUT VARCHAR2,
                                          pon_retcode          OUT NUMBER) IS
    l_status VARCHAR2(1);

    l_log_ret_status VARCHAR2(50);
    l_log_err_msg    VARCHAR2(2000);
  BEGIN
    pon_retcode := g_normal;

    BEGIN
    --Added supplier site stg table in query to get right vendor site code as per change done in vendor site code
      SELECT assa.vendor_site_id, assa.vendor_site_code
        INTO pon_vendor_site_id, pov_vendor_site_code
        FROM ap_supplier_sites_all assa,
             ap_suppliers          asp,
             hr_operating_units    hou,
             xxap_supplier_sites_stg xss
       WHERE asp.vendor_name = piv_vendor_name
         AND assa.vendor_id = asp.vendor_id
         AND assa.vendor_site_code = xss.vendor_site_code
         AND assa.org_id = hou.organization_id
         AND assa.org_id = pin_org_id
         AND asp.enabled_flag = g_yes
         AND SYSDATE BETWEEN NVL(asp.start_date_active, SYSDATE) AND
             NVL(asp.end_date_active, SYSDATE)
         AND SYSDATE <= NVL(assa.inactive_date, SYSDATE)
         AND xss.leg_vendor_name = piv_vendor_name
         AND xss.org_id = pin_org_id
         AND xss.leg_vendor_site_code = piv_vendor_site_code
         AND xss.process_flag in ('C','P')
         AND xss.leg_vendor_site_id = pin_leg_vendor_site_id;  -- to avoid error ETN_AP_INVALID_VENDOR_SITE of too may rows
    --Added supplier site stg table in query to get right vendor site code as per change done in vendor site code
    EXCEPTION
      WHEN OTHERS THEN
        pon_retcode := g_warning;
        log_errors(pov_return_status       => l_log_ret_status -- OUT
                  ,
                   pov_error_msg           => l_log_err_msg -- OUT
                  ,
                   piv_source_column_name  => 'LEG_VENDOR_SITE_CODE',
                   piv_source_column_value => piv_vendor_site_code,
                   piv_error_type          => g_err_val,
                   piv_error_code          => 'ETN_AP_INVALID_VENDOR_SITE',
                   piv_error_message       => 'Error : Vendor Site Code is not valid,(Dependent Site) Oracle Error is ' ||
                                              SQLERRM);
    END;

  EXCEPTION
    WHEN OTHERS THEN
      pon_retcode := g_ret_error;
      print_log_message(SUBSTR('Exception in Procedure validate_vendor_site_suppcont. ' ||
                               SQLERRM,
                               1,
                               2000));
  END validate_vendor_site_suppcont;

  --
  -- ========================
  -- Procedure: validate_vendor_suppcont
  -- =============================================================================
  --   This procedure is used to
  -- =============================================================================
  --  Input Parameters :
  --    None
  --  Output Parameters :
  --    None
  -- -----------------------------------------------------------------------------
  --
  PROCEDURE validate_vendor_suppcont(piv_vendor_name IN VARCHAR2,
                                     pon_vendor_id   OUT NUMBER,
                                     pon_retcode     OUT NUMBER) IS
    l_status VARCHAR2(1);

    l_log_ret_status VARCHAR2(50);
    l_log_err_msg    VARCHAR2(2000);
  BEGIN
    pon_retcode := g_normal;

    BEGIN
      SELECT vendor_id
        INTO pon_vendor_id
        FROM ap_suppliers
       WHERE vendor_name = piv_vendor_name
         AND enabled_flag = g_yes;

    EXCEPTION
      WHEN OTHERS THEN
        pon_retcode := g_warning;
        log_errors(pov_return_status       => l_log_ret_status -- OUT
                  ,
                   pov_error_msg           => l_log_err_msg -- OUT
                  ,
                   piv_source_column_name  => 'LEG_VENDOR_NAME',
                   piv_source_column_value => piv_vendor_name,
                   piv_error_type          => g_err_val,
                   piv_error_code          => 'ETN_AP_INVALID_VENDOR',
                   piv_error_message       => 'Error : Vendor Name is not valid, Oracle Error is ' ||
                                              SQLERRM);
    END;

  EXCEPTION
    WHEN OTHERS THEN
      pon_retcode := g_ret_error;
      print_log_message(SUBSTR('Exception in Procedure validate_vendor_suppcont. ' ||
                               SQLERRM,
                               1,
                               2000));
  END validate_vendor_suppcont;

  --
  -- ========================
  -- Procedure: dup_val_chk_suppcont
  -- =============================================================================
  --   This procedure to do mandatory value check for Supplier Contacts entity
  -- =============================================================================
  --  Input Parameters :
  --    None
  --  Output Parameters :
  --    None
  -- -----------------------------------------------------------------------------
  --
  PROCEDURE dup_val_chk_suppcont(pon_retcode OUT NUMBER) IS
    l_log_ret_status VARCHAR2(50);
    l_log_err_msg    VARCHAR2(2000);

    --cursor to select new records from supplier contacts staging table
    CURSOR supplier_cont_cur IS
      SELECT xscs.leg_vendor_name,
             xscs.leg_vendor_site_code,
             xscs.leg_operating_unit_name,
             xscs.leg_first_name,
             xscs.leg_last_name,
             xscs.leg_email_address,
             xscs.leg_phone,
             xscs.leg_vendor_contact_id,
             COUNT(1)
        FROM xxap_supplier_contacts_stg xscs
       WHERE xscs.process_flag = g_new
         AND xscs.batch_id = g_new_batch_id
         AND xscs.run_sequence_id = g_run_seq_id
       GROUP BY xscs.leg_vendor_name,
                xscs.leg_vendor_site_code,
                xscs.leg_operating_unit_name,
                xscs.leg_first_name,
                xscs.leg_last_name,
                xscs.leg_email_address,
                xscs.leg_phone,
                xscs.leg_vendor_contact_id
      HAVING COUNT(1) > 1;

    TYPE supplier_cont_t IS TABLE OF supplier_cont_cur%ROWTYPE INDEX BY BINARY_INTEGER;
    l_supplier_cont_tbl supplier_cont_t;
  BEGIN
    pon_retcode := g_normal;

    OPEN supplier_cont_cur;
    LOOP
      FETCH supplier_cont_cur BULK COLLECT
        INTO l_supplier_cont_tbl LIMIT 1000;
      EXIT WHEN l_supplier_cont_tbl.COUNT = 0;
      IF l_supplier_cont_tbl.COUNT > 0 THEN
        FOR indx IN 1 .. l_supplier_cont_tbl.COUNT LOOP
          --g_intf_staging_id := l_supplier_cont_tbl(indx).interface_txn_id;

          g_src_keyname1  := 'LEG_VENDOR_SITE_CODE';
          g_src_keyvalue1 := l_supplier_cont_tbl(indx).leg_vendor_site_code;
          g_src_keyname2  := 'LEG_FIRST_NAME';
          g_src_keyvalue2 := l_supplier_cont_tbl(indx).leg_first_name;
          g_src_keyname3  := 'LEG_LAST_NAME';
          g_src_keyvalue3 := l_supplier_cont_tbl(indx).leg_last_name;
          g_src_keyname4  := 'LEG_EMAIL_ADDRESS';
          g_src_keyvalue4 := l_supplier_cont_tbl(indx).leg_email_address;
          g_src_keyname5  := 'LEG_PHONE';
          g_src_keyvalue5 := l_supplier_cont_tbl(indx).leg_phone;

          log_errors(pov_return_status       => l_log_ret_status,
                     pov_error_msg           => l_log_err_msg,
                     piv_source_column_name  => 'LEG_VENDOR_SITE_CODE',
                     piv_source_column_value => l_supplier_cont_tbl(indx)
                                               .leg_vendor_site_code,
                     piv_error_type          => g_err_val,
                     piv_error_code          => 'ETN_AP_DUPLICATE_ENTITY',
                     piv_error_message       => 'Error : Duplicate information entered for Supplier Contact Record');
        END LOOP;
      END IF;
    END LOOP;
    CLOSE supplier_cont_cur;

  EXCEPTION
    WHEN OTHERS THEN
      pon_retcode := g_ret_error;
      print_log_message(SUBSTR('Exception in Procedure dup_val_chk_suppcont. ' ||
                               SQLERRM,
                               1,
                               2000));
  END dup_val_chk_suppcont;

  --
  -- ========================
  -- Procedure: req_val_chk_suppcont
  -- =============================================================================
  --   This procedure to do mandatory value check for Supplier Contacts entity
  -- =============================================================================
  --  Input Parameters :
  --    None
  --  Output Parameters :
  --    None
  -- -----------------------------------------------------------------------------
  --
  PROCEDURE req_val_chk_suppcont(pon_retcode OUT NUMBER) IS
    l_err_code CONSTANT VARCHAR2(28) := 'ETN_AP_MANDATORY_NOT_ENTERED';

    l_log_ret_status VARCHAR2(50);
    l_log_err_msg    VARCHAR2(2000);

    --cursor to select new records from supplier bank staging table
    CURSOR supplier_cont_cur IS
      SELECT *
        FROM xxap_supplier_contacts_stg xscs
       WHERE xscs.process_flag = g_new
         AND xscs.batch_id = g_new_batch_id
         AND xscs.run_sequence_id = g_run_seq_id
         AND (xscs.leg_vendor_name IS NULL
             --OR xscs.leg_vendor_site_code        IS NULL
             --OR xscs.leg_operating_unit_name     IS NULL
             OR xscs.leg_last_name IS NULL);

    TYPE supplier_cont_t IS TABLE OF supplier_cont_cur%ROWTYPE INDEX BY BINARY_INTEGER;
    l_supplier_cont_tbl supplier_cont_t;
  BEGIN
    pon_retcode := g_normal;

    OPEN supplier_cont_cur;
    LOOP
      FETCH supplier_cont_cur BULK COLLECT
        INTO l_supplier_cont_tbl LIMIT 1000;
      EXIT WHEN l_supplier_cont_tbl.COUNT = 0;
      IF l_supplier_cont_tbl.COUNT > 0 THEN
        FOR indx IN 1 .. l_supplier_cont_tbl.COUNT LOOP
          g_intf_staging_id := l_supplier_cont_tbl(indx).interface_txn_id;

          IF l_supplier_cont_tbl(indx).leg_vendor_name IS NULL THEN

          l_supplier_cont_tbl(indx).process_flag := g_error;

            log_errors(pov_return_status       => l_log_ret_status -- OUT
                      ,
                       pov_error_msg           => l_log_err_msg -- OUT
                      ,
                       piv_source_column_name  => 'LEG_VENDOR_NAME',
                       piv_source_column_value => l_supplier_cont_tbl(indx)
                                                 .leg_vendor_name,
                       piv_error_type          => g_err_val,
                       piv_error_code          => l_err_code,
                       piv_error_message       => 'Error : LEG_VENDOR_NAME should not be NULL');
          END IF;
          /**
                         IF l_supplier_cont_tbl(indx).leg_vendor_site_code IS NULL
                         THEN
                            log_errors  ( pov_return_status          =>   l_log_ret_status          -- OUT
                                        , pov_error_msg              =>   l_log_err_msg             -- OUT
                                        , piv_source_column_name     =>   'LEG_VENDOR_SITE_CODE'
                                        , piv_source_column_value    =>   l_supplier_cont_tbl(indx).leg_vendor_site_code
                                        , piv_error_type             =>   g_err_val
                                        , piv_error_code             =>   l_err_code
                                        , piv_error_message          =>   'Error : LEG_VENDOR_SITE_CODE should not be NULL'
                                        );
                         END IF;

                         IF l_supplier_cont_tbl(indx).leg_operating_unit_name IS NULL
                         THEN
                            log_errors  ( pov_return_status          =>   l_log_ret_status          -- OUT
                                        , pov_error_msg              =>   l_log_err_msg             -- OUT
                                        , piv_source_column_name     =>   'LEG_OPERATING_UNIT_NAME'
                                        , piv_source_column_value    =>   l_supplier_cont_tbl(indx).leg_operating_unit_name
                                        , piv_error_type             =>   g_err_val
                                        , piv_error_code             =>   l_err_code
                                        , piv_error_message          =>   'Error : LEG_OPERATING_UNIT_NAME should not be NULL'
                                        );
                         END IF;
          **/

          IF l_supplier_cont_tbl(indx).leg_last_name IS NULL THEN
            l_supplier_cont_tbl(indx).process_flag := g_error;

            log_errors(pov_return_status       => l_log_ret_status -- OUT
                      ,
                       pov_error_msg           => l_log_err_msg -- OUT
                      ,
                       piv_source_column_name  => 'LEG_LAST_NAME',
                       piv_source_column_value => l_supplier_cont_tbl(indx)
                                                 .leg_last_name,
                       piv_error_type          => g_err_val,
                       piv_error_code          => l_err_code,
                       piv_error_message       => 'Error : LEG_LAST_NAME should not be NULL');
          END IF;

        END LOOP;

        BEGIN
          FORALL indx IN 1 .. l_supplier_cont_tbl.COUNT SAVE EXCEPTIONS
            UPDATE xxap_supplier_contacts_stg xscs
               SET xscs.last_updated_date           = SYSDATE,
                   xscs.error_type                  = DECODE(l_supplier_cont_tbl(indx)
                                                             .process_flag,
                                                             g_validated,
                                                             NULL,
                                                             g_error,
                                                             g_err_val),
                   xscs.process_flag                = l_supplier_cont_tbl(indx)
                                                     .process_flag
             WHERE xscs.interface_txn_id = l_supplier_cont_tbl(indx).interface_txn_id;

        EXCEPTION
          WHEN OTHERS THEN
            print_log_message(SUBSTR('Exception in Procedure req_val_chk_suppcont while doing Bulk Insert. ' ||
                                     SQLERRM,
                                     1,
                                     2000));
            print_log_message('No. of records in Bulk Exception : ' ||
                              SQL%BULK_EXCEPTIONS.COUNT);
        END;

      END IF;


    END LOOP;
    CLOSE supplier_cont_cur;

  EXCEPTION
    WHEN OTHERS THEN
      pon_retcode := g_ret_error;
      print_log_message(SUBSTR('Exception in Procedure req_val_chk_suppcont. ' ||
                               SQLERRM,
                               1,
                               2000));
  END req_val_chk_suppcont;

  --
  -- ========================
  -- Procedure: validate_att_suppsite
  -- =============================================================================
  --   This procedure is used to
  -- =============================================================================
  --  Input Parameters :
  --    None
  --  Output Parameters :
  --    None
  -- -----------------------------------------------------------------------------
  --
  PROCEDURE validate_att_suppsite(piv_attribute IN VARCHAR2,
                                  piv_value_set IN VARCHAR2,
                                  piv_value     IN VARCHAR2,
                                  pov_value     OUT VARCHAR2,
                                  pon_retcode   OUT NUMBER) IS
    l_status VARCHAR2(1);

    l_log_ret_status VARCHAR2(50);
    l_log_err_msg    VARCHAR2(2000);
  BEGIN
    pon_retcode := g_normal;

    IF piv_value_set IN ('XXETN_EAP_NAICS', 'XXETN_ACH_FORMAT_CODES'
       --,  'XXETN_PUR_TYPE'
       --,  'XXETN_SUP_ADD_RSN'
       --,  '13 Characters'
       --,  'XXETN_PLANT_LEDGER_NUMBER'
       --,  'XXETN_PAY_IS_FOR'
       --,  'XXETN_EAP_ADD_CITIBANK_INFO'
       --,  'XXETN_LEGACY_VENDOR_NUMBER'
       --,  'XXETN_EATON_PLANT_ID'
       --,  'XXETN_MVT_AREA'
       --,  'XXETN_MVT_PORT'
       --,  'XXETN_MVT_TRANSACTION_NATURE'
       --,  'XXETN_MVT_DELIVERY_TERMS'
       --,  'XXETN_MVT_TRANSACTION_MODE'
       --,  'XXETN_MVT_COMMODITY_CODES'
       ) THEN
      BEGIN
        SELECT ffv.flex_value
          INTO pov_value
          FROM fnd_flex_value_sets ffvs, fnd_flex_values ffv
         WHERE ffvs.flex_value_set_name = piv_value_set
           AND ffvs.flex_value_set_id = ffv.flex_value_set_id
           AND ffv.enabled_flag = g_yes
           AND TRUNC(SYSDATE) BETWEEN
               NVL(ffv.start_date_active, TRUNC(SYSDATE)) AND
               NVL(ffv.end_date_active, TRUNC(SYSDATE + 1))
           AND ffv.flex_value = piv_value;

        pov_value := piv_value;
      EXCEPTION
        /*CV40 Page#24 -- 7/1: Additional requirements added : PMC <317614> */
        WHEN NO_DATA_FOUND THEN
          pov_value := NULL;

        WHEN OTHERS THEN
          pon_retcode := g_warning;
          log_errors(pov_return_status       => l_log_ret_status -- OUT
                    ,
                     pov_error_msg           => l_log_err_msg -- OUT
                    ,
                     piv_source_column_name  => UPPER(piv_attribute),
                     piv_source_column_value => piv_value,
                     piv_error_type          => g_err_val,
                     piv_error_code          => 'ETN_AP_INVALID_' ||
                                                UPPER(piv_attribute),
                     piv_error_message       => 'Error : Legacy ' ||
                                                piv_attribute || ' = ' ||
                                                piv_value ||
                                                ' not present in Value set: ' ||
                                                piv_value_set ||
                                                '. Oracle Error is ' ||
                                                SQLERRM);
      END;
    ELSIF piv_value_set IN ('AP_SRS_YES_NO_MAND', 'AP_SRS_YES_NO_OPT') THEN
      IF UPPER(piv_value) IN ('Y', 'N') THEN
        pov_value := piv_value;
      ELSE
        pon_retcode := g_warning;
        log_errors(pov_return_status       => l_log_ret_status -- OUT
                  ,
                   pov_error_msg           => l_log_err_msg -- OUT
                  ,
                   piv_source_column_name  => UPPER(piv_attribute),
                   piv_source_column_value => piv_value,
                   piv_error_type          => g_err_val,
                   piv_error_code          => 'ETN_AP_INVALID_' ||
                                              UPPER(piv_attribute),
                   piv_error_message       => 'Error : Legacy ' ||
                                              piv_attribute || ' = ' ||
                                              piv_value ||
                                              ' not present in Value set: ' ||
                                              piv_value_set);
      END IF;

    ELSIF piv_value_set = 'XXETN_PUR_TYPE' THEN

      BEGIN
        SELECT flvv.description
          INTO pov_value
          FROM fnd_lookup_values_vl flvv
         WHERE flvv.lookup_type = 'ETN_PURCHASE_TYPE'
           AND SYSDATE BETWEEN NVL(flvv.start_date_active, SYSDATE) AND
               NVL(flvv.end_date_active, SYSDATE)
           /*AND flvv.description = piv_value  Change 6 May 2016  DD - Meaning keeps 11i value  keep R12 value*/
            AND flvv.meaning = upper(piv_value);


      EXCEPTION

        /*CV40 Page#24 -- 7/1: Additional requirements added : PMC <317614> */
        WHEN NO_DATA_FOUND THEN
          pov_value := NULL;

        WHEN OTHERS THEN
          pon_retcode := g_warning;
          log_errors(pov_return_status       => l_log_ret_status -- OUT
                    ,
                     pov_error_msg           => l_log_err_msg -- OUT
                    ,
                     piv_source_column_name  => UPPER(piv_attribute),
                     piv_source_column_value => piv_value,
                     piv_error_type          => g_err_val,
                     piv_error_code          => 'ETN_AP_INVALID_' ||
                                                UPPER(piv_attribute),
                     piv_error_message       => 'Error : Legacy ' ||
                                                piv_attribute || ' = ' ||
                                                piv_value ||
                                                ' not present in Value set: ' ||
                                                piv_value_set ||
                                                '. Oracle Error is ' ||
                                                SQLERRM);
      END;

    ELSIF piv_value_set = 'XXETN_PAY_IS_FOR' THEN

      BEGIN
        SELECT flvv.description
          INTO pov_value
          FROM fnd_lookup_values_vl flvv
         WHERE flvv.lookup_type = 'ETN_PAY_IS_FOR'
           AND SYSDATE BETWEEN NVL(flvv.start_date_active, SYSDATE) AND
               NVL(flvv.end_date_active, SYSDATE)
           AND flvv.description = piv_value;

        pov_value := piv_value;
      EXCEPTION
        /*CV40 Page#24 -- 7/1: Additional requirements added : PMC <317614> */
        WHEN NO_DATA_FOUND THEN
          pov_value := NULL;

        WHEN OTHERS THEN
          pon_retcode := g_warning;
          log_errors(pov_return_status       => l_log_ret_status -- OUT
                    ,
                     pov_error_msg           => l_log_err_msg -- OUT
                    ,
                     piv_source_column_name  => UPPER(piv_attribute),
                     piv_source_column_value => piv_value,
                     piv_error_type          => g_err_val,
                     piv_error_code          => 'ETN_AP_INVALID_' ||
                                                UPPER(piv_attribute),
                     piv_error_message       => 'Error : Legacy ' ||
                                                piv_attribute || ' = ' ||
                                                piv_value ||
                                                ' not present in Value set: ' ||
                                                piv_value_set ||
                                                '. Oracle Error is ' ||
                                                SQLERRM);
      END;

    ELSIF piv_value_set = 'XXETN_SUP_ADD_RSN' THEN

      BEGIN
        SELECT flvv.description
          INTO pov_value
          FROM fnd_lookup_values_vl flvv
         WHERE flvv.lookup_type = 'ETN_SUP_ADD_REASON'
           AND SYSDATE BETWEEN NVL(flvv.start_date_active, SYSDATE) AND
               NVL(flvv.end_date_active, SYSDATE)
           AND flvv.description = piv_value;

        pov_value := piv_value;
      EXCEPTION

        WHEN OTHERS THEN
          pon_retcode := g_warning;
          log_errors(pov_return_status       => l_log_ret_status -- OUT
                    ,
                     pov_error_msg           => l_log_err_msg -- OUT
                    ,
                     piv_source_column_name  => UPPER(piv_attribute),
                     piv_source_column_value => piv_value,
                     piv_error_type          => g_err_val,
                     piv_error_code          => 'ETN_AP_INVALID_' ||
                                                UPPER(piv_attribute),
                     piv_error_message       => 'Error : Legacy ' ||
                                                piv_attribute || ' = ' ||
                                                piv_value ||
                                                ' not present in Value set: ' ||
                                                piv_value_set ||
                                                '. Oracle Error is ' ||
                                                SQLERRM);
      END;

    ELSIF piv_value_set IN
          ('XXETN_PLANT_LEDGER_NUMBER', 'XXETN_EAP_ADD_CITIBANK_INFO',
           'XXETN_EATON_PLANT_ID', 'XXETN_MVT_AREA', 'XXETN_MVT_PORT',
           'XXETN_MVT_TRANSACTION_NATURE', 'XXETN_MVT_DELIVERY_TERMS',
           'XXETN_MVT_TRANSACTION_MODE', 'XXETN_MVT_COMMODITY_CODES') THEN
      pov_value := piv_value;
    --ADB change starts 09/27/2016
    ELSIF piv_value_set = 'XXAP_PO_NONPO_MAPPING' THEN

      BEGIN
        SELECT flvv.description
          INTO pov_value
          FROM fnd_lookup_values_vl flvv
         WHERE flvv.lookup_type = 'XXAP_PO_NONPO_MAPPING'
           AND SYSDATE BETWEEN NVL(flvv.start_date_active, SYSDATE) AND
               NVL(flvv.end_date_active, SYSDATE)
           AND flvv.meaning = piv_value;


      EXCEPTION

        WHEN OTHERS THEN
           pov_value :=NULL;
      END;
    --ADB change ends
    END IF;

  EXCEPTION
    WHEN OTHERS THEN
      pon_retcode := g_ret_error;
      print_log_message(SUBSTR('Exception in Procedure validate_att_suppsite. ' ||
                               SQLERRM,
                               1,
                               2000));
  END validate_att_suppsite;

  --
  -- ========================
  -- Procedure: validate_vendor_suppsite
  -- =============================================================================
  --   This procedure is used to
  -- =============================================================================
  --  Input Parameters :
  --    None
  --  Output Parameters :
  --    None
  -- -----------------------------------------------------------------------------
  --
  PROCEDURE validate_vendor_suppsite(piv_vendor_number IN VARCHAR2,
                                     pon_vendor_id     OUT NUMBER,
                                     pon_retcode       OUT NUMBER) IS
    l_status VARCHAR2(1);

    l_log_ret_status VARCHAR2(50);
    l_log_err_msg    VARCHAR2(2000);
  BEGIN
    pon_retcode := g_normal;

    BEGIN
      SELECT vendor_id
        INTO pon_vendor_id
        FROM ap_suppliers
       WHERE segment1 = piv_vendor_number
         AND enabled_flag = g_yes;

    EXCEPTION
      WHEN OTHERS THEN
        pon_retcode := g_warning;
        log_errors(pov_return_status       => l_log_ret_status -- OUT
                  ,
                   pov_error_msg           => l_log_err_msg -- OUT
                  ,
                   piv_source_column_name  => 'LEG_VENDOR_NUMBER',
                   piv_source_column_value => piv_vendor_number,
                   piv_error_type          => g_err_val,
                   piv_error_code          => 'ETN_AP_INVALID_VENDOR',
                   piv_error_message       => 'Error : Vendor Number is not valid, Oracle Error is ' ||
                                              SQLERRM);
    END;

  EXCEPTION
    WHEN OTHERS THEN
      pon_retcode := g_ret_error;
      print_log_message(SUBSTR('Exception in Procedure validate_vendor_suppsite. ' ||
                               SQLERRM,
                               1,
                               2000));
  END validate_vendor_suppsite;

  --
  -- ========================
  -- Procedure: validate_autotaxc_flg_suppsite
  -- =============================================================================
  --   This procedure is used to
  -- =============================================================================
  --  Input Parameters :
  --    None
  --  Output Parameters :
  --    None
  -- -----------------------------------------------------------------------------
  --
  PROCEDURE validate_autotaxc_flg_suppsite(piv_lookup_code IN VARCHAR2,
                                           pon_retcode     OUT NUMBER) IS
    l_status VARCHAR2(1);

    l_log_ret_status VARCHAR2(50);
    l_log_err_msg    VARCHAR2(2000);
  BEGIN
    pon_retcode := g_normal;

    BEGIN
      SELECT 'N'
        INTO l_status
        FROM fnd_lookup_values flv
       WHERE flv.lookup_type = 'AP_TAX_CALCULATION_METHOD'
         AND UPPER(flv.lookup_code) = UPPER(piv_lookup_code)
         AND SYSDATE BETWEEN NVL(flv.start_date_active, SYSDATE) AND
             NVL(flv.end_date_active, SYSDATE)
         AND flv.enabled_flag = g_yes
         AND flv.language = USERENV('LANG');

    EXCEPTION
      WHEN OTHERS THEN
        pon_retcode := g_warning;
        log_errors(pov_return_status       => l_log_ret_status -- OUT
                  ,
                   pov_error_msg           => l_log_err_msg -- OUT
                  ,
                   piv_source_column_name  => 'LEG_AUTO_TAX_CALC_FLAG',
                   piv_source_column_value => piv_lookup_code,
                   piv_error_type          => g_err_val,
                   piv_error_code          => 'ETN_AP_INVALID_AUTOTAX_CALC_FLAG',
                   piv_error_message       => 'Error : Auto Tax Calculation Flag is not valid, Oracle Error is ' ||
                                              SQLERRM);
    END;

  EXCEPTION
    WHEN OTHERS THEN
      pon_retcode := g_ret_error;
      print_log_message(SUBSTR('Exception in Procedure validate_autotaxc_flg_suppsite. ' ||
                               SQLERRM,
                               1,
                               2000));
  END validate_autotaxc_flg_suppsite;

  --
  -- ========================
  -- Procedure: validate_tolrnce_tmp_suppsite
  -- =============================================================================
  --   This procedure is used to
  -- =============================================================================
  --  Input Parameters :
  --    None
  --  Output Parameters :
  --    None
  -- -----------------------------------------------------------------------------
  --
  PROCEDURE validate_tolrnce_tmp_suppsite(piv_tolerance_name IN VARCHAR2,
                                          pon_tolerance_id   OUT NUMBER,
                                          pon_retcode        OUT NUMBER) IS
    l_log_ret_status VARCHAR2(50);
    l_log_err_msg    VARCHAR2(2000);
  BEGIN
    pon_retcode := g_normal;

    BEGIN
      SELECT att.tolerance_id
        INTO pon_tolerance_id
        FROM ap_tolerance_templates att
       WHERE att.tolerance_name = piv_tolerance_name;
    EXCEPTION
      WHEN OTHERS THEN
        pon_retcode := g_warning;
        log_errors(pov_return_status       => l_log_ret_status -- OUT
                  ,
                   pov_error_msg           => l_log_err_msg -- OUT
                  ,
                   piv_source_column_name  => 'LEG_TOLERANCE_NAME',
                   piv_source_column_value => piv_tolerance_name,
                   piv_error_type          => g_err_val,
                   piv_error_code          => 'ETN_AP_INVALID_TOLERANCE_TEMP',
                   piv_error_message       => 'Error : Tolerance Temp is not valid, Oracle Error is ' ||
                                              SQLERRM);
    END;

  EXCEPTION
    WHEN OTHERS THEN
      pon_retcode := g_ret_error;
      print_log_message(SUBSTR('Exception in Procedure validate_tolrnce_tmp_suppsite. ' ||
                               SQLERRM,
                               1,
                               2000));
  END validate_tolrnce_tmp_suppsite;

  --
  -- ========================
  -- Procedure: validate_dist_set_suppsite
  -- =============================================================================
  --   This procedure is used to
  -- =============================================================================
  --  Input Parameters :
  --    None
  --  Output Parameters :
  --    None
  -- -----------------------------------------------------------------------------
  --
  PROCEDURE validate_dist_set_suppsite(piv_dist_set_name IN VARCHAR2,
                                       pin_org_id        IN NUMBER,
                                       pon_dist_set_id   OUT NUMBER,
                                       pov_dist_set_name OUT VARCHAR2,
                                       pon_retcode       OUT NUMBER) IS
    l_log_ret_status VARCHAR2(50);
    l_log_err_msg    VARCHAR2(2000);
  BEGIN
    pon_retcode := g_normal;

    BEGIN
      SELECT distribution_set_id
        INTO pon_dist_set_id
        FROM ap_distribution_sets_all
       WHERE distribution_set_name = piv_dist_set_name
         AND org_id = pin_org_id
         AND (SYSDATE) <= NVL(inactive_date, (SYSDATE));

      pov_dist_set_name := piv_dist_set_name;
    EXCEPTION
      WHEN OTHERS THEN
        /* v3.0 ADB change start 09/27/2016
        pon_retcode := g_warning;
        log_errors(pov_return_status       => l_log_ret_status -- OUT
                  ,
                   pov_error_msg           => l_log_err_msg -- OUT
                  ,
                   piv_source_column_name  => 'LEG_DISTRIBUTION_SET_NAME',
                   piv_source_column_value => piv_dist_set_name,
                   piv_error_type          => g_err_val,
                   piv_error_code          => 'ETN_AP_INVALID_DISTIRBUTION_SET',
                   piv_error_message       => 'Error : Distribution set is not valid, Oracle Error is ' ||
                                              SQLERRM);*/
        pon_dist_set_id := NULL;
        pov_dist_set_name := NULL;
        --v3.0 ADB change end 09/27/2016
    END;

  EXCEPTION
    WHEN OTHERS THEN
      pon_retcode := g_ret_error;
      print_log_message(SUBSTR('Exception in Procedure validate_dist_set_suppsite. ' ||
                               SQLERRM,
                               1,
                               2000));
  END validate_dist_set_suppsite;

  --
  -- ========================
  -- Procedure: validate_invmtchoptn_suppsite
  -- =============================================================================
  --   This procedure is used to
  -- =============================================================================
  --  Input Parameters :
  --    None
  --  Output Parameters :
  --    None
  -- -----------------------------------------------------------------------------
  --
  PROCEDURE validate_invmtchoptn_suppsite(piv_lookup_code IN VARCHAR2,
                                          pon_retcode     OUT VARCHAR2) IS
    l_status VARCHAR2(1);

    l_log_ret_status VARCHAR2(50);
    l_log_err_msg    VARCHAR2(2000);
  BEGIN
    pon_retcode := g_normal;

    BEGIN
      SELECT 'N'
        INTO l_status
        FROM fnd_lookup_values flv
       WHERE flv.lookup_type = 'PO INVOICE MATCH OPTION'
         AND flv.enabled_flag = g_yes
         AND SYSDATE BETWEEN NVL(flv.start_date_active, SYSDATE) AND
             NVL(flv.end_date_active, SYSDATE)
         AND UPPER(flv.lookup_code) = UPPER(piv_lookup_code)
         AND language = USERENV('LANG');

    EXCEPTION
      WHEN OTHERS THEN
        pon_retcode := g_warning;
        log_errors(pov_return_status       => l_log_ret_status -- OUT
                  ,
                   pov_error_msg           => l_log_err_msg -- OUT
                  ,
                   piv_source_column_name  => 'LEG_MATCH_OPTION',
                   piv_source_column_value => piv_lookup_code,
                   piv_error_type          => g_err_val,
                   piv_error_code          => 'ETN_AP_INVALID_INVOICE_MATCH_OPTION',
                   piv_error_message       => 'Error : Invoice Match Option is not valid, Oracle Error is ' ||
                                              SQLERRM);
    END;

  EXCEPTION
    WHEN OTHERS THEN
      pon_retcode := g_ret_error;
      print_log_message(SUBSTR('Exception in Procedure validate_invmtchoptn_suppsite. ' ||
                               SQLERRM,
                               1,
                               2000));
  END validate_invmtchoptn_suppsite;

  --
  -- ========================
  -- Procedure: validate_supp_notif_suppsite
  -- =============================================================================
  --   This procedure is used to
  -- =============================================================================
  --  Input Parameters :
  --    None
  --  Output Parameters :
  --    None
  -- -----------------------------------------------------------------------------
  --
  PROCEDURE validate_supp_notif_suppsite(pin_supp_notif_code IN VARCHAR2,
                                         pon_retcode         OUT VARCHAR2) IS
    l_log_ret_status VARCHAR2(50);
    l_log_err_msg    VARCHAR2(2000);
  BEGIN
    pon_retcode := g_normal;

    IF (pin_supp_notif_code IN ('EMAIL', 'PRINT', 'FAX')) THEN
      pon_retcode := g_normal;
    ELSE
      pon_retcode := g_warning;
      log_errors(pov_return_status       => l_log_ret_status -- OUT
                ,
                 pov_error_msg           => l_log_err_msg -- OUT
                ,
                 piv_source_column_name  => 'LEG_SUPPLIER_NOTIF_METHOD',
                 piv_source_column_value => pin_supp_notif_code,
                 piv_error_type          => g_err_val,
                 piv_error_code          => 'ETN_AP_INVALID_SUPP_NOTIF_METHOD',
                 piv_error_message       => 'Error : Supplier Notification Code is not in EMAIL, PRINT and FAX');
    END IF;

  EXCEPTION
    WHEN OTHERS THEN
      pon_retcode := g_ret_error;
      print_log_message(SUBSTR('Exception in Procedure validate_supp_notif_suppsite. ' ||
                               SQLERRM,
                               1,
                               2000));
  END validate_supp_notif_suppsite;

  --
  -- ========================
  -- Procedure: validate_pay_on_recpt_suppsite
  -- =============================================================================
  --   This procedure is used to
  -- =============================================================================
  --  Input Parameters :
  --    None
  --  Output Parameters :
  --    None
  -- -----------------------------------------------------------------------------
  --
  PROCEDURE validate_pay_on_recpt_suppsite(pin_pay_on_receipt_code IN VARCHAR2,
                                           pon_pay_on_receipt_code OUT VARCHAR2,
                                           pon_retcode             OUT NUMBER) IS
    l_log_ret_status VARCHAR2(50);
    l_log_err_msg    VARCHAR2(2000);
  BEGIN
    pon_retcode := g_normal;

    IF (pin_pay_on_receipt_code IN ('RECEIPT', 'PAY_SITE', 'PACKING_SLIP')) THEN
      pon_pay_on_receipt_code := pin_pay_on_receipt_code;
    ELSE
      pon_retcode := g_warning;
      log_errors(pov_return_status       => l_log_ret_status -- OUT
                ,
                 pov_error_msg           => l_log_err_msg -- OUT
                ,
                 piv_source_column_name  => 'LEG_PAY_ON_RECEIPT_SUMMARY_CODE',
                 piv_source_column_value => pin_pay_on_receipt_code,
                 piv_error_type          => g_err_val,
                 piv_error_code          => 'ETN_AP_INVALID_PAYON_RECEIPT_CODE',
                 piv_error_message       => 'Error : Pay on Receipt Code is not in RECEIPT and DELIVERY');
    END IF;

  EXCEPTION
    WHEN OTHERS THEN
      pon_retcode := g_ret_error;
      print_log_message(SUBSTR('Exception in Procedure validate_pay_on_recpt_suppsite. ' ||
                               SQLERRM,
                               1,
                               2000));
  END validate_pay_on_recpt_suppsite;

  --
  -- ========================
  -- Procedure: validate_pay_on_code_suppsite
  -- =============================================================================
  --   This procedure is used to
  -- =============================================================================
  --  Input Parameters :
  --    None
  --  Output Parameters :
  --    None
  -- -----------------------------------------------------------------------------
  --
  PROCEDURE validate_pay_on_code_suppsite(piv_lookup_code IN VARCHAR2,
                                          pov_lookup_code OUT VARCHAR2,
                                          pon_retcode     OUT NUMBER) IS
    l_status VARCHAR2(1);

    l_log_ret_status VARCHAR2(50);
    l_log_err_msg    VARCHAR2(2000);
  BEGIN
    pon_retcode := g_normal;

    BEGIN
      SELECT 'N'
        INTO l_status
        FROM fnd_lookup_values flv
       WHERE flv.lookup_type = 'PAY ON CODE'
         AND SYSDATE BETWEEN NVL(flv.start_date_active, SYSDATE) AND
             NVL(flv.end_date_active, SYSDATE)
         AND flv.enabled_flag = g_yes
         AND UPPER(flv.lookup_code) = UPPER(piv_lookup_code)
         AND language = USERENV('LANG');

      pov_lookup_code := UPPER(piv_lookup_code);
    EXCEPTION
      WHEN OTHERS THEN
        pon_retcode := g_warning;
        log_errors(pov_return_status       => l_log_ret_status -- OUT
                  ,
                   pov_error_msg           => l_log_err_msg -- OUT
                  ,
                   piv_source_column_name  => 'LEG_PAY_ON_CODE',
                   piv_source_column_value => piv_lookup_code,
                   piv_error_type          => g_err_val,
                   piv_error_code          => 'ETN_AP_INVALID_PAYON_CODE',
                   piv_error_message       => 'Error : Pay On Code is not valid, Oracle Error is ' ||
                                              SQLERRM);
    END;

  EXCEPTION
    WHEN OTHERS THEN
      pon_retcode := g_ret_error;
      print_log_message(SUBSTR('Exception in Procedure validate_pay_on_code_suppsite. ' ||
                               SQLERRM,
                               1,
                               2000));
  END validate_pay_on_code_suppsite;

  --
  -- ========================
  -- Procedure: validate_site_lang_suppsite
  -- =============================================================================
  --   This procedure is used to
  -- =============================================================================
  --  Input Parameters :
  --    None
  --  Output Parameters :
  --    None
  -- -----------------------------------------------------------------------------
  --
  PROCEDURE validate_site_lang_suppsite(piv_language IN VARCHAR2,
                                        pon_retcode  OUT NUMBER) IS
    l_status VARCHAR2(1);

    l_log_ret_status VARCHAR2(50);
    l_log_err_msg    VARCHAR2(2000);
  BEGIN
    pon_retcode := g_normal;

    BEGIN
      SELECT 'N'
        INTO l_status
        FROM fnd_languages
       WHERE nls_language = piv_language;

    EXCEPTION
      WHEN OTHERS THEN
        pon_retcode := g_warning;
        log_errors(pov_return_status       => l_log_ret_status -- OUT
                  ,
                   pov_error_msg           => l_log_err_msg -- OUT
                  ,
                   piv_source_column_name  => 'LEG_LANGUAGE',
                   piv_source_column_value => piv_language,
                   piv_error_type          => g_err_val,
                   piv_error_code          => 'ETN_AP_INVALID_SITE_LANGUAGE',
                   piv_error_message       => 'Error : Site Language is not valid, Oracle Error is ' ||
                                              SQLERRM);
    END;

  EXCEPTION
    WHEN OTHERS THEN
      pon_retcode := g_ret_error;
      print_log_message(SUBSTR('Exception in Procedure validate_site_lang_suppsite. ' ||
                               SQLERRM,
                               1,
                               2000));
  END validate_site_lang_suppsite;

  --
  -- ========================
  -- Procedure: validate_addrs_style_suppsite
  -- =============================================================================
  --   This procedure is used to
  -- =============================================================================
  --  Input Parameters :
  --    None
  --  Output Parameters :
  --    None
  -- -----------------------------------------------------------------------------
  --
  PROCEDURE validate_addrs_style_suppsite(piv_lookup_code IN VARCHAR2,
                                          pon_retcode     OUT NUMBER) IS
    l_status VARCHAR2(1);

    l_log_ret_status VARCHAR2(50);
    l_log_err_msg    VARCHAR2(2000);
  BEGIN
    pon_retcode := g_normal;

    BEGIN
      SELECT 'N'
        INTO l_status
        FROM fnd_lookup_values flv
       WHERE flv.lookup_type = 'ADDRESS_STYLE'
         AND SYSDATE BETWEEN NVL(flv.start_date_active, SYSDATE) AND
             NVL(flv.end_date_active, SYSDATE)
         AND flv.enabled_flag = g_yes
         AND UPPER(flv.lookup_code) =
             UPPER(DECODE(piv_lookup_code, 'GB', 'UAA', piv_lookup_code))
         AND language = USERENV('LANG');

    EXCEPTION
      WHEN OTHERS THEN
        pon_retcode := g_warning;
        log_errors(pov_return_status       => l_log_ret_status -- OUT
                  ,
                   pov_error_msg           => l_log_err_msg -- OUT
                  ,
                   piv_source_column_name  => 'LEG_ADDRESS_STYLE',
                   piv_source_column_value => piv_lookup_code,
                   piv_error_type          => g_err_val,
                   piv_error_code          => 'ETN_AP_INVALID_ADDRESS_STYLE',
                   piv_error_message       => 'Error : Address Style is not valid, Oracle Error is ' ||
                                              SQLERRM);
    END;

  EXCEPTION
    WHEN OTHERS THEN
      pon_retcode := g_ret_error;
      print_log_message(SUBSTR('Exception in Procedure validate_addrs_style_suppsite. ' ||
                               SQLERRM,
                               1,
                               2000));
  END validate_addrs_style_suppsite;

  --
  -- ========================
  -- Procedure: validate_ship_via_suppsite
  -- =============================================================================
  --   This procedure is used to
  -- =============================================================================
  --  Input Parameters :
  --    None
  --  Output Parameters :
  --    None
  -- -----------------------------------------------------------------------------
  --
  PROCEDURE validate_ship_via_suppsite(piv_lookup_code IN VARCHAR2,
                                       pin_org_id      IN NUMBER,
                                       pov_lookup_code OUT VARCHAR2,
                                       pon_retcode     OUT NUMBER) IS
    l_ship_lkp CONSTANT VARCHAR2(30) := 'XXAP_SHIP_VIA_LOOKUP_CODE';

    l_status      VARCHAR2(1);
    l_lookup_code fnd_lookup_values.lookup_code%TYPE;

    l_log_ret_status VARCHAR2(50);
    l_log_err_msg    VARCHAR2(2000);
    l_err_count      NUMBER := 0;
  BEGIN
    pon_retcode := g_normal;

    BEGIN
      SELECT flv.description
        INTO l_lookup_code
        FROM fnd_lookup_values flv
       WHERE flv.lookup_type = l_ship_lkp
         AND flv.meaning = piv_lookup_code
         AND flv.enabled_flag = g_yes
         AND SYSDATE BETWEEN NVL(flv.start_date_active, SYSDATE) AND
             NVL(flv.end_date_active, SYSDATE + 1)
         AND flv.language = USERENV('LANG');
    EXCEPTION
      WHEN OTHERS THEN
        l_err_count := l_err_count + 1;
        log_errors(pov_return_status       => l_log_ret_status -- OUT
                  ,
                   pov_error_msg           => l_log_err_msg -- OUT
                  ,
                   piv_source_column_name  => 'LEG_SHIP_VIA_LOOKUP_CODE',
                   piv_source_column_value => piv_lookup_code,
                   piv_error_type          => g_err_val,
                   piv_error_code          => 'ETN_AP_INVALID_SHIP_VIA_CODE',
                   piv_error_message       => 'Error : Lookup Code ' ||
                                              piv_lookup_code ||
                                              ' for Ship Via Lookup (' ||
                                              l_ship_lkp ||
                                              ') setup issue. Oracle Error is ' ||
                                              SQLERRM);
    END;

    IF l_err_count = 0 THEN
      BEGIN
        SELECT 'N'
          INTO l_status
          FROM org_freight
         WHERE organization_id = (SELECT inventory_organization_id
                                       FROM  financials_system_params_all
                                       WHERE org_id= pin_org_id)
           AND SYSDATE <= NVL(disable_date, SYSDATE)
           AND UPPER(freight_code) = UPPER(l_lookup_code);

        pov_lookup_code := UPPER(l_lookup_code);
      EXCEPTION
        WHEN OTHERS THEN
          l_err_count := l_err_count + 1;
          log_errors(pov_return_status       => l_log_ret_status -- OUT
                    ,
                     pov_error_msg           => l_log_err_msg -- OUT
                    ,
                     piv_source_column_name  => 'LEG_SHIP_VIA_LOOKUP_CODE',
                     piv_source_column_value => piv_lookup_code,
                     piv_error_type          => g_err_val,
                     piv_error_code          => 'ETN_AP_INVALID_SHIP_VIA_CODE',
                     piv_error_message       => 'Error : Record not found for Ship Via Code ' ||
                                                l_lookup_code ||
                                                ', Oracle Error is ' ||
                                                SQLERRM);
      END;
    END IF;

    IF l_err_count > 0 THEN
      pon_retcode := g_warning;
    END IF;
  EXCEPTION
    WHEN OTHERS THEN
      pon_retcode := g_ret_error;
      print_log_message(SUBSTR('Exception in Procedure validate_ship_via_suppsite. ' ||
                               SQLERRM,
                               1,
                               2000));
  END validate_ship_via_suppsite;

  --
  -- ========================
  -- Procedure: validate_pymtpriority_suppsite
  -- =============================================================================
  --   This procedure is used to
  -- =============================================================================
  --  Input Parameters :
  --    None
  --  Output Parameters :
  --    None
  -- -----------------------------------------------------------------------------
  --
  PROCEDURE validate_pymtpriority_suppsite(pin_payment_priority IN NUMBER,
                                           pon_payment_priority OUT NUMBER,
                                           pon_retcode          OUT NUMBER) IS
    l_log_ret_status VARCHAR2(50);
    l_log_err_msg    VARCHAR2(2000);
  BEGIN
    pon_retcode := g_normal;

    IF (pin_payment_priority BETWEEN 0 AND 100) THEN
      pon_payment_priority := pin_payment_priority;
    ELSE
      pon_retcode := g_warning;
      log_errors(pov_return_status       => l_log_ret_status -- OUT
                ,
                 pov_error_msg           => l_log_err_msg -- OUT
                ,
                 piv_source_column_name  => 'LEG_PAYMENT_PRIORITY',
                 piv_source_column_value => pin_payment_priority,
                 piv_error_type          => g_err_val,
                 piv_error_code          => 'ETN_AP_INVALID_PAYMENT_PRIORITY',
                 piv_error_message       => 'Error : Payment Priority is not between 0 and 100.');
    END IF;

  EXCEPTION
    WHEN OTHERS THEN
      pon_retcode := g_ret_error;
      print_log_message(SUBSTR('Exception in Procedure validate_pymtpriority_suppsite. ' ||
                               SQLERRM,
                               1,
                               2000));
  END validate_pymtpriority_suppsite;

  --
  -- ========================
  -- Procedure: validate_date_basis_suppsite
  -- =============================================================================
  --   This procedure is used to
  -- =============================================================================
  --  Input Parameters :
  --    None
  --  Output Parameters :
  --    None
  -- -----------------------------------------------------------------------------
  --
  PROCEDURE validate_date_basis_suppsite(piv_lookup_code IN VARCHAR2,
                                         pov_lookup_code OUT VARCHAR2,
                                         pon_retcode     OUT NUMBER) IS
    l_status VARCHAR2(1);

    l_log_ret_status VARCHAR2(50);
    l_log_err_msg    VARCHAR2(2000);
  BEGIN
    pon_retcode := g_normal;

    BEGIN
      SELECT flv.lookup_code
        INTO pov_lookup_code
        FROM fnd_lookup_values flv
       WHERE flv.lookup_type = 'TERMS DATE BASIS'
         AND SYSDATE BETWEEN NVL(flv.start_date_active, SYSDATE) AND
             NVL(flv.end_date_active, SYSDATE)
         AND flv.enabled_flag = g_yes
         AND UPPER(flv.lookup_code) = UPPER(piv_lookup_code)
         AND language = USERENV('LANG');

    EXCEPTION
      WHEN OTHERS THEN
        pon_retcode := g_warning;
        log_errors(pov_return_status       => l_log_ret_status -- OUT
                  ,
                   pov_error_msg           => l_log_err_msg -- OUT
                  ,
                   piv_source_column_name  => 'LEG_PAY_DATE_BASIS_CODE',
                   piv_source_column_value => piv_lookup_code,
                   piv_error_type          => g_err_val,
                   piv_error_code          => 'ETN_AP_INVALID_DATE_BASIS',
                   piv_error_message       => 'Error : Date Basis Code is not valid, Oracle Error is ' ||
                                              SQLERRM);
    END;

  EXCEPTION
    WHEN OTHERS THEN
      pon_retcode := g_ret_error;
      print_log_message(SUBSTR('Exception in Procedure validate_date_basis_suppsite. ' ||
                               SQLERRM,
                               1,
                               2000));
  END validate_date_basis_suppsite;

  --
  -- ========================
  -- Procedure: validate_shpng_cntrl_suppsite
  -- =============================================================================
  --   This procedure is used to
  -- =============================================================================
  --  Input Parameters :
  --    None
  --  Output Parameters :
  --    None
  -- -----------------------------------------------------------------------------
  --
  PROCEDURE validate_shpng_cntrl_suppsite(piv_lookup_code IN VARCHAR2,
                                          pon_retcode     OUT NUMBER) IS
    l_status VARCHAR2(1);

    l_log_ret_status VARCHAR2(50);
    l_log_err_msg    VARCHAR2(2000);
  BEGIN
    pon_retcode := g_normal;

    BEGIN
      SELECT 'N'
        INTO l_status
        FROM fnd_lookup_values
       WHERE lookup_type = 'SHIPPING CONTROL'
         AND SYSDATE BETWEEN NVL(start_date_active, SYSDATE) AND
             NVL(end_date_active, SYSDATE)
         AND enabled_flag = g_yes
         AND UPPER(lookup_code) = UPPER(piv_lookup_code)
         AND language = USERENV('LANG');

    EXCEPTION
      WHEN OTHERS THEN
        pon_retcode := g_warning;
        log_errors(pov_return_status       => l_log_ret_status -- OUT
                  ,
                   pov_error_msg           => l_log_err_msg -- OUT
                  ,
                   piv_source_column_name  => 'LEG_SHIPPING_CONTROL',
                   piv_source_column_value => piv_lookup_code,
                   piv_error_type          => g_err_val,
                   piv_error_code          => 'ETN_AP_INVALID_SHIPPING_CONTROL',
                   piv_error_message       => 'Error : Shipping Control Code is not valid, Oracle Error is ' ||
                                              SQLERRM);
    END;

  EXCEPTION
    WHEN OTHERS THEN
      pon_retcode := g_ret_error;
      print_log_message(SUBSTR('Exception in Procedure validate_shpng_cntrl_suppsite. ' ||
                               SQLERRM,
                               1,
                               2000));
  END validate_shpng_cntrl_suppsite;

  --
  -- ========================
  -- Procedure: validate_country_suppsite
  -- =============================================================================
  --   This procedure is used to
  -- =============================================================================
  --  Input Parameters :
  --    None
  --  Output Parameters :
  --    None
  -- -----------------------------------------------------------------------------
  --
  PROCEDURE validate_country_suppsite(piv_lookup_code IN VARCHAR2,
                                      pon_retcode     OUT NUMBER) IS
    l_status VARCHAR2(1);

    l_log_ret_status VARCHAR2(50);
    l_log_err_msg    VARCHAR2(2000);
  BEGIN
    pon_retcode := g_normal;

    BEGIN
      SELECT 'N'
        INTO l_status
        FROM fnd_territories
       WHERE UPPER(territory_code) = UPPER(piv_lookup_code)
         AND obsolete_flag = 'N';

    EXCEPTION
      WHEN OTHERS THEN
        pon_retcode := g_warning;
        log_errors(pov_return_status       => l_log_ret_status -- OUT
                  ,
                   pov_error_msg           => l_log_err_msg -- OUT
                  ,
                   piv_source_column_name  => 'LEG_COUNTRY',
                   piv_source_column_value => piv_lookup_code,
                   piv_error_type          => g_err_val,
                   piv_error_code          => 'ETN_AP_INVALID_COUNTRY',
                   piv_error_message       => 'Error : Country Code is not valid, Oracle Error is ' ||
                                              SQLERRM);
    END;

  EXCEPTION
    WHEN OTHERS THEN
      pon_retcode := g_ret_error;
      print_log_message(SUBSTR('Exception in Procedure validate_country_suppsite. ' ||
                               SQLERRM,
                               1,
                               2000));
  END validate_country_suppsite;

  --
  -- =============================================================================
  -- Procedure: validate_oper_unit_suppsite
  -- =============================================================================
  --   This procedure is used to derive R12 OU based on Plant# or Site#
  -- =============================================================================
  --  Input Parameters :
  --    piv_site : 11i Plant# or Site#
  --  Output Parameters :
  --    pov_operating_unit : R12 Operating Unit
  --    pon_org_id         : R12 Org Id
  --    pon_retcode        : Return Code
  -- -----------------------------------------------------------------------------
  --
  PROCEDURE validate_oper_unit_suppsite(piv_site           IN VARCHAR2,
                                        pov_operating_unit OUT VARCHAR2,
                                        pon_org_id         OUT NUMBER,
                                        pon_retcode        OUT NUMBER)

   IS
    l_log_ret_status VARCHAR2(50);
    l_log_err_msg    VARCHAR2(2000);
    l_err_count      NUMBER := 0;

    l_oper_unit xxetn_map_unit.operating_unit%TYPE := NULL;
    l_rec       xxetn_map_util.g_input_rec;

  BEGIN
    pon_retcode        := g_normal;
    pov_operating_unit := NULL;
    pon_org_id         := NULL;

    -- Assigning 11i Site/Plant to API variable
    l_rec.site := piv_site;

    --R12 OU
    l_oper_unit := xxetn_map_util.get_value(l_rec).operating_unit;

    -- If R12 OU derivation failed
    IF l_oper_unit IS NULL THEN
      l_err_count := l_err_count + 1;
      log_errors(pov_return_status       => l_log_ret_status -- OUT
                ,
                 pov_error_msg           => l_log_err_msg -- OUT
                ,
                 piv_source_column_name  => 'LEG_ACCTS_PAY_CODE_SEGMENT1',
                 piv_source_column_value => piv_site,
                 piv_error_type          => g_err_val,
                 piv_error_code          => 'ETN_AP_INVALID_PLANT_SITE',
                 piv_error_message       => 'Error : R12 OU does not exist in ETN Map Unit Table for Site: ' ||
                                            piv_site);
    ELSE
      pov_operating_unit := l_oper_unit;
    END IF;

    -- If R12 Operating Unit is not NULL
    IF l_err_count = 0 THEN
      BEGIN
        SELECT hou.organization_id
          INTO pon_org_id
          FROM hr_operating_units hou
         WHERE hou.name = pov_operating_unit
           AND TRUNC(SYSDATE) BETWEEN NVL(hou.date_from, TRUNC(SYSDATE)) AND
               NVL(hou.date_to, TRUNC(SYSDATE));
      EXCEPTION
        WHEN OTHERS THEN
          l_err_count := l_err_count + 1;
          log_errors(pov_return_status       => l_log_ret_status -- OUT
                    ,
                     pov_error_msg           => l_log_err_msg -- OUT
                    ,
                     piv_source_column_name  => 'R12_OPERATING_UNIT',
                     piv_source_column_value => pov_operating_unit,
                     piv_error_type          => g_err_val,
                     piv_error_code          => 'ETN_AP_INVALID_OPERATING_UNIT',
                     piv_error_message       => 'Error : Organization Id not found for Operating Unit' ||
                                                pov_operating_unit ||
                                                ', Oracle Error is ' ||
                                                SQLERRM);
      END;
    END IF;

    IF l_err_count > 0 THEN
      pon_retcode := g_warning;
    END IF;
  EXCEPTION
    WHEN OTHERS THEN
      pon_retcode := g_ret_error;
      print_log_message(SUBSTR('Exception in Procedure validate_oper_unit_suppsite. ' ||
                               SQLERRM,
                               1,
                               2000));
  END validate_oper_unit_suppsite;

  --
  -- ========================
  -- Procedure: validate_pymt_term_suppsite
  -- =============================================================================
  --   This procedure is used to
  -- =============================================================================
  --  Input Parameters :
  --    None
  --  Output Parameters :
  --    None
  -- -----------------------------------------------------------------------------
  --
  PROCEDURE validate_pymt_term_suppsite(piv_lookup_code IN VARCHAR2,
                                        pov_lookup_code OUT VARCHAR2,
                                        pon_terms_id    OUT NUMBER,
                                        pon_retcode     OUT NUMBER) IS
    l_pay_terms_lkp CONSTANT VARCHAR2(30) := 'XXAP_PAYTERM_MAPPING';

    l_lookup_code fnd_lookup_values.lookup_code%TYPE;

    l_log_ret_status VARCHAR2(50);
    l_log_err_msg    VARCHAR2(2000);
    l_err_count      NUMBER := 0;
  BEGIN
    pon_retcode := g_normal;

    BEGIN
      SELECT DISTINCT UPPER(flv.description)
        INTO l_lookup_code
        FROM fnd_lookup_values flv
       WHERE flv.lookup_type = l_pay_terms_lkp
         AND flv.meaning = piv_lookup_code
         AND flv.enabled_flag = g_yes
         AND SYSDATE BETWEEN NVL(flv.start_date_active, SYSDATE) AND
             NVL(flv.end_date_active, SYSDATE + 1)
         AND flv.language = USERENV('LANG');
    EXCEPTION
      WHEN OTHERS THEN
        l_err_count := l_err_count + 1;
        log_errors(pov_return_status       => l_log_ret_status -- OUT
                  ,
                   pov_error_msg           => l_log_err_msg -- OUT
                  ,
                   piv_source_column_name  => 'LEG_TERMS_NAME',
                   piv_source_column_value => piv_lookup_code,
                   piv_error_type          => g_err_val,
                   piv_error_code          => 'ETN_AP_INVALID_PAYMENT_TERM',
                   piv_error_message       => 'Error : Lookup Code ' ||
                                              piv_lookup_code ||
                                              ' for Payment Term Lookup (' ||
                                              l_pay_terms_lkp ||
                                              ') setup issue. Oracle Error is ' ||
                                              SQLERRM);
    END;

    IF l_err_count = 0 THEN
      BEGIN
        SELECT term_id
          INTO pon_terms_id
          FROM ap_terms
         WHERE name = l_lookup_code
           AND SYSDATE BETWEEN NVL(start_date_active, SYSDATE) AND
               NVL(end_date_active, SYSDATE)
           AND enabled_flag = g_yes;

        pov_lookup_code := l_lookup_code;
      EXCEPTION
        WHEN OTHERS THEN
          l_err_count := l_err_count + 1;
          log_errors(pov_return_status       => l_log_ret_status -- OUT
                    ,
                     pov_error_msg           => l_log_err_msg -- OUT
                    ,
                     piv_source_column_name  => 'LEG_TERMS_NAME',
                     piv_source_column_value => piv_lookup_code,
                     piv_error_type          => g_err_val,
                     piv_error_code          => 'ETN_AP_INVALID_PAYMENT_TERM',
                     piv_error_message       => 'Error : Payment Term Id not found for Term Name: ' ||
                                                l_lookup_code ||
                                                ', Oracle Error is ' ||
                                                SQLERRM);
      END;
    END IF;

    IF l_err_count > 0 THEN
      pon_retcode := g_warning;
    END IF;
  EXCEPTION
    WHEN OTHERS THEN
      pon_retcode := g_ret_error;
      print_log_message(SUBSTR('Exception in Procedure validate_pymt_term_suppsite. ' ||
                               SQLERRM,
                               1,
                               2000));
  END validate_pymt_term_suppsite;

  --
  -- ========================
  -- Procedure: dup_val_chk_suppsite
  -- =============================================================================
  --   This procedure to do mandatory value check for Supplier entity
  -- =============================================================================
  --  Input Parameters :
  --    None
  --  Output Parameters :
  --    None
  -- -----------------------------------------------------------------------------
  --
  PROCEDURE dup_val_chk_suppsite(pon_retcode OUT NUMBER) IS
    l_log_ret_status VARCHAR2(50);
    l_log_err_msg    VARCHAR2(2000);

    --cursor to select new records from supplier bank staging table
    CURSOR supplier_site_cur IS
      SELECT xsss.leg_vendor_name,
             xsss.leg_vendor_site_code,
             xsss.leg_operating_unit_name,
             COUNT(1)
        FROM xxap_supplier_sites_stg xsss
       WHERE xsss.process_flag = g_new
         AND xsss.batch_id = g_new_batch_id
         AND xsss.run_sequence_id = g_run_seq_id
       GROUP BY xsss.leg_vendor_name,
                xsss.leg_vendor_site_code,
                xsss.leg_operating_unit_name
      HAVING COUNT(1) > 1;

    TYPE supplier_site_t IS TABLE OF supplier_site_cur%ROWTYPE INDEX BY BINARY_INTEGER;
    l_supplier_site_tbl supplier_site_t;
  BEGIN
    pon_retcode := g_normal;

    OPEN supplier_site_cur;
    LOOP
      FETCH supplier_site_cur BULK COLLECT
        INTO l_supplier_site_tbl LIMIT 1000;
      EXIT WHEN l_supplier_site_tbl.COUNT = 0;
      IF l_supplier_site_tbl.COUNT > 0 THEN
        FOR indx IN 1 .. l_supplier_site_tbl.COUNT LOOP
          --g_intf_staging_id := l_supplier_site_tbl(indx).interface_txn_id;

          g_src_keyname1  := 'LEG_VENDOR_NAME';
          g_src_keyvalue1 := l_supplier_site_tbl(indx).leg_vendor_name;
          g_src_keyname2  := 'LEG_VENDOR_SITE_CODE';
          g_src_keyvalue2 := l_supplier_site_tbl(indx).leg_vendor_site_code;
          g_src_keyname3  := 'LEG_OPERATING_UNIT_NAME';
          g_src_keyvalue3 := l_supplier_site_tbl(indx)
                            .leg_operating_unit_name;

          log_errors(pov_return_status       => l_log_ret_status,
                     pov_error_msg           => l_log_err_msg,
                     piv_source_column_name  => 'LEG_VENDOR_SITE_CODE',
                     piv_source_column_value => l_supplier_site_tbl(indx)
                                               .leg_vendor_site_code,
                     piv_error_type          => g_err_val,
                     piv_error_code          => 'ETN_AP_DUPLICATE_ENTITY',
                     piv_error_message       => 'Error : Duplicate information entered for Supplier Site Record');
        END LOOP;

      END IF;
    END LOOP;
    CLOSE supplier_site_cur;

  EXCEPTION
    WHEN OTHERS THEN
      pon_retcode := g_ret_error;
      print_log_message(SUBSTR('Exception in Procedure dup_val_chk_suppsite. ' ||
                               SQLERRM,
                               1,
                               2000));
  END dup_val_chk_suppsite;

  --
  -- =============================================================================
  -- Procedure: req_val_chk_suppsite
  -- =============================================================================
  --   This procedure to do mandatory value check for Supplier Site entity
  -- =============================================================================
  --  Input Parameters :
  --    None
  --  Output Parameters :
  --    None
  -- -----------------------------------------------------------------------------
  --
  PROCEDURE req_val_chk_suppsite(pon_retcode OUT NUMBER) IS
    l_err_code CONSTANT VARCHAR2(28) := 'ETN_AP_MANDATORY_NOT_ENTERED';

    l_log_ret_status VARCHAR2(50);
    l_log_err_msg    VARCHAR2(2000);

    --cursor to select new records from supplier sites staging table
    CURSOR supplier_site_cur IS
      SELECT *
        FROM xxap_supplier_sites_stg xsss
       WHERE xsss.process_flag = g_new
         AND xsss.batch_id = g_new_batch_id
         AND xsss.run_sequence_id = g_run_seq_id
         AND (leg_vendor_name IS NULL OR leg_vendor_site_code IS NULL OR
             leg_terms_name IS NULL
             --OR leg_operating_unit_name   IS NULL
             OR leg_accts_pay_code_segment1 IS NULL OR leg_country IS NULL);

    TYPE supplier_site_t IS TABLE OF supplier_site_cur%ROWTYPE INDEX BY BINARY_INTEGER;
    l_supplier_site_tbl supplier_site_t;
  BEGIN
    pon_retcode := g_normal;

    OPEN supplier_site_cur;
    LOOP
      FETCH supplier_site_cur BULK COLLECT
        INTO l_supplier_site_tbl LIMIT 1000;
      EXIT WHEN l_supplier_site_tbl.COUNT = 0;
      IF l_supplier_site_tbl.COUNT > 0 THEN
        FOR indx IN 1 .. l_supplier_site_tbl.COUNT LOOP
          g_intf_staging_id := l_supplier_site_tbl(indx).interface_txn_id;

          IF l_supplier_site_tbl(indx).leg_vendor_name IS NULL THEN

            l_supplier_site_tbl(indx).process_flag := g_error;
            log_errors(pov_return_status       => l_log_ret_status -- OUT
                      ,pov_error_msg           => l_log_err_msg -- OUT
                      ,piv_source_column_name  => 'LEG_VENDOR_NAME',
                       piv_source_column_value => l_supplier_site_tbl(indx)
                                                 .leg_vendor_name,
                       piv_error_type          => g_err_val,
                       piv_error_code          => l_err_code,
                       piv_error_message       => 'Error : LEG_VENDOR_NAME should not be NULL');
          END IF;

          IF l_supplier_site_tbl(indx).leg_vendor_site_code IS NULL THEN
              l_supplier_site_tbl(indx).process_flag := g_error;
            log_errors(pov_return_status       => l_log_ret_status -- OUT
                      ,pov_error_msg           => l_log_err_msg -- OUT
                      ,piv_source_column_name  => 'LEG_VENDOR_SITE_CODE',
                       piv_source_column_value => l_supplier_site_tbl(indx)
                                                 .leg_vendor_site_code,
                       piv_error_type          => g_err_val,
                       piv_error_code          => l_err_code,
                       piv_error_message       => 'Error : LEG_VENDOR_SITE_CODE should not be NULL');
          END IF;

          IF l_supplier_site_tbl(indx).leg_terms_name IS NULL THEN
               l_supplier_site_tbl(indx).process_flag := g_error;
            log_errors(pov_return_status       => l_log_ret_status -- OUT
                      ,pov_error_msg           => l_log_err_msg -- OUT
                      ,piv_source_column_name  => 'LEG_TERMS_NAME',
                       piv_source_column_value => l_supplier_site_tbl(indx)
                                                 .leg_terms_name,
                       piv_error_type          => g_err_val,
                       piv_error_code          => l_err_code,
                       piv_error_message       => 'Error : LEG_TERMS_NAME should not be NULL');
          END IF;

          IF l_supplier_site_tbl(indx).leg_accts_pay_code_segment1 IS NULL THEN
             l_supplier_site_tbl(indx).process_flag := g_error;
            log_errors(pov_return_status       => l_log_ret_status -- OUT
                      ,pov_error_msg           => l_log_err_msg -- OUT
                      ,piv_source_column_name  => 'LEG_ACCTS_PAY_CODE_SEGMENT1',
                       piv_source_column_value => l_supplier_site_tbl(indx)
                                                 .leg_accts_pay_code_segment1,
                       piv_error_type          => g_err_val,
                       piv_error_code          => l_err_code,
                       piv_error_message       => 'Error : LEG_ACCTS_PAY_CODE_SEGMENT1 should not be NULL');
          END IF;
          --added ADB 09/27/2016
           IF l_supplier_site_tbl(indx).leg_country IS NULL THEN

            l_supplier_site_tbl(indx).process_flag := g_error;
            log_errors(pov_return_status       => l_log_ret_status -- OUT
                      ,pov_error_msg           => l_log_err_msg -- OUT
                      ,piv_source_column_name  => 'LEG_COUNTRY',
                       piv_source_column_value => l_supplier_site_tbl(indx)
                                                 .leg_country,
                       piv_error_type          => g_err_val,
                       piv_error_code          => l_err_code,
                       piv_error_message       => 'Error : LEG_COUNTRY should not be NULL');
          END IF;
          --ADB changes end
        END LOOP;

/*         print_log_message('l_supplier_site_tbl.COUNT ' ||
                          l_supplier_site_tbl.COUNT);

        print_log_message('Updating Supplier sites staging table');*/

        BEGIN
          FORALL indx IN 1 .. l_supplier_site_tbl.COUNT SAVE EXCEPTIONS
            UPDATE xxap_supplier_sites_stg xsss
               SET xsss.last_update_date              = SYSDATE,
                   xsss.error_type                    = DECODE(l_supplier_site_tbl(indx)
                                                               .process_flag,
                                                               g_validated,
                                                               NULL,
                                                               g_error,
                                                               g_err_val),
                   xsss.process_flag                  = l_supplier_site_tbl(indx)
                                                       .process_flag
             WHERE xsss.interface_txn_id = l_supplier_site_tbl(indx).interface_txn_id;
        EXCEPTION
          WHEN OTHERS THEN
            print_log_message(SUBSTR('Exception in Procedure validate_supplier_sites while doing Bulk Insert. ' ||
                                     SQLERRM,
                                     1,
                                     2000));
            print_log_message('No. of records in Bulk Exception : ' ||
                              SQL%BULK_EXCEPTIONS.COUNT);
        END;


      END IF;
    END LOOP;
    CLOSE supplier_site_cur;

  EXCEPTION
    WHEN OTHERS THEN
      pon_retcode := g_ret_error;
      print_log_message(SUBSTR('Exception in Procedure req_val_chk_suppsite. ' ||
                               SQLERRM,
                               1,
                               2000));
  END req_val_chk_suppsite;

  --
  -- ========================
  -- Procedure: validate_att15_supp
  -- =============================================================================
  --   This procedure is used to validate attribute15
  -- =============================================================================
  --  Input Parameters :
  --    None
  --  Output Parameters :
  --    None
  -- -----------------------------------------------------------------------------
  --
  PROCEDURE validate_att15_supp(piv_attribute15 IN VARCHAR2,
                                pov_attribute15 OUT VARCHAR2,
                                pon_retcode     OUT NUMBER) IS
    l_status VARCHAR2(1);

    l_log_ret_status VARCHAR2(50);
    l_log_err_msg    VARCHAR2(2000);
  BEGIN
    pon_retcode := g_normal;

    BEGIN
      SELECT ffv.flex_value
        INTO pov_attribute15
        FROM fnd_flex_value_sets ffvs, fnd_flex_values ffv
       WHERE ffvs.flex_value_set_name = 'XXETN_Y/N'
         AND ffvs.flex_value_set_id = ffv.flex_value_set_id
         AND ffv.enabled_flag = 'Y'
         AND TRUNC(SYSDATE) BETWEEN
             NVL(ffv.start_date_active, TRUNC(SYSDATE)) AND
             NVL(ffv.end_date_active, TRUNC(SYSDATE + 1))
         AND ffv.flex_value IN NVL(piv_attribute15, 'N');

      pov_attribute15 := piv_attribute15;
    EXCEPTION
      WHEN OTHERS THEN
        pon_retcode := g_warning;
        log_errors(pov_return_status       => l_log_ret_status -- OUT
                  ,
                   pov_error_msg           => l_log_err_msg -- OUT
                  ,
                   piv_source_column_name  => 'LEG_ATTRIBUTE15',
                   piv_source_column_value => piv_attribute15,
                   piv_error_type          => g_err_val,
                   piv_error_code          => 'ETN_AP_INVALID_ATTRIBUTE15',
                   piv_error_message       => 'Error : Legacy Attribute15 = ' ||
                                              piv_attribute15 ||
                                              ' not present in Value set XXETN_Y/N. Oracle Error is ' ||
                                              SQLERRM);
    END;
  EXCEPTION
    WHEN OTHERS THEN
      pon_retcode := g_ret_error;
      print_log_message(SUBSTR('Exception in Procedure validate_att15_supp. ' ||
                               SQLERRM,
                               1,
                               2000));
  END validate_att15_supp;

  --
  -- ========================
  -- Procedure: validate_att14_supp
  -- =============================================================================
  --   This procedure is used to validate attribute14
  -- =============================================================================
  --  Input Parameters :
  --    None
  --  Output Parameters :
  --    None
  -- -----------------------------------------------------------------------------
  --
  PROCEDURE validate_att14_supp(piv_attribute14 IN VARCHAR2,
                                pov_attribute14 OUT VARCHAR2,
                                pon_retcode     OUT NUMBER) IS
    l_status VARCHAR2(1);

    l_log_ret_status VARCHAR2(50);
    l_log_err_msg    VARCHAR2(2000);
  BEGIN
    pon_retcode := g_normal;

    BEGIN
      SELECT '1'
        INTO l_status
        FROM fnd_flex_value_sets ffvs, fnd_flex_values ffv
       WHERE ffvs.flex_value_set_name = 'XXETN_Y/N'
         AND ffvs.flex_value_set_id = ffv.flex_value_set_id
         AND ffv.enabled_flag = 'Y'
         AND TRUNC(SYSDATE) BETWEEN
             NVL(ffv.start_date_active, TRUNC(SYSDATE)) AND
             NVL(ffv.end_date_active, TRUNC(SYSDATE + 1))
         AND ffv.flex_value IN (piv_attribute14);

      pov_attribute14 := piv_attribute14;
    EXCEPTION
      WHEN OTHERS THEN
        pon_retcode := g_warning;
        log_errors(pov_return_status       => l_log_ret_status -- OUT
                  ,
                   pov_error_msg           => l_log_err_msg -- OUT
                  ,
                   piv_source_column_name  => 'LEG_ATTRIBUTE14',
                   piv_source_column_value => piv_attribute14,
                   piv_error_type          => g_err_val,
                   piv_error_code          => 'ETN_AP_INVALID_ATTRIBUTE14',
                   piv_error_message       => 'Error : Legacy Attribute14 = ' ||
                                              piv_attribute14 ||
                                              ' not present in Value set XXETN_Y/N. Oracle Error is ' ||
                                              SQLERRM);
    END;
  EXCEPTION
    WHEN OTHERS THEN
      pon_retcode := g_ret_error;
      print_log_message(SUBSTR('Exception in Procedure validate_att14_supp. ' ||
                               SQLERRM,
                               1,
                               2000));
  END validate_att14_supp;

  --
  -- ========================
  -- Procedure: validate_att7_supp
  -- =============================================================================
  --   This procedure is used to validate attribute7
  -- =============================================================================
  --  Input Parameters :
  --    None
  --  Output Parameters :
  --    None
  -- -----------------------------------------------------------------------------
  --
  PROCEDURE validate_att7_supp(piv_attribute7 IN VARCHAR2,
                               pov_attribute7 OUT VARCHAR2,
                               pon_retcode    OUT NUMBER) IS
    l_log_ret_status VARCHAR2(50);
    l_log_err_msg    VARCHAR2(2000);
  BEGIN
    pon_retcode := g_normal;

    IF UPPER(piv_attribute7) IN ('Y', 'N') THEN
      pov_attribute7 := UPPER(piv_attribute7);
    ELSE
      pov_attribute7 := NULL;
    END IF;

  EXCEPTION
    WHEN OTHERS THEN
      pon_retcode := g_ret_error;
      print_log_message(SUBSTR('Exception in Procedure validate_att7_supp. ' ||
                               SQLERRM,
                               1,
                               2000));
  END validate_att7_supp;

  --
  -- ========================
  -- Procedure: validate_att5_supp
  -- =============================================================================
  --   This procedure is used to validate attribute5
  -- =============================================================================
  --  Input Parameters :
  --    None
  --  Output Parameters :
  --    None
  -- -----------------------------------------------------------------------------
  --
  PROCEDURE validate_att5_supp(piv_attribute5 IN VARCHAR2,
                               pov_attribute5 OUT VARCHAR2,
                               pon_retcode    OUT NUMBER) IS
    l_status VARCHAR2(1);

    l_log_ret_status VARCHAR2(50);
    l_log_err_msg    VARCHAR2(2000);
  BEGIN
    pon_retcode := g_normal;

    BEGIN
      SELECT to_char(to_date(piv_attribute5,'YYYY/MM/DD HH24:MI:SS'),'DD-MON-YYYY')
        INTO pov_attribute5
        FROM dual;
    EXCEPTION
      WHEN OTHERS THEN
        pov_attribute5 := NULL;
    END;

  EXCEPTION
    WHEN OTHERS THEN
      pon_retcode := g_ret_error;
      print_log_message(SUBSTR('Exception in Procedure validate_att5_supp. ' ||
                               SQLERRM,
                               1,
                               2000));
  END validate_att5_supp;

  --
  -- ========================
  -- Procedure: validate_att2_supp
  -- =============================================================================
  --   This procedure is used to validate attribute2
  -- =============================================================================
  --  Input Parameters :
  --    None
  --  Output Parameters :
  --    None
  -- -----------------------------------------------------------------------------
  --
  PROCEDURE validate_att2_supp(piv_attribute2 IN VARCHAR2,
                               pov_attribute2 OUT VARCHAR2,
                               pon_retcode    OUT NUMBER) IS
    l_status VARCHAR2(1);

    l_log_ret_status VARCHAR2(50);
    l_log_err_msg    VARCHAR2(2000);
  BEGIN
    pon_retcode := g_normal;

    pov_attribute2 := NULL;

    IF UPPER(piv_attribute2) LIKE ('%SANCTIONED%') THEN
      pov_attribute2 := UPPER(piv_attribute2);
    END IF;

    IF UPPER(piv_attribute2) LIKE ('%UNSANCTIONED%') THEN
      pov_attribute2 := UPPER(piv_attribute2);
    END IF;

    IF UPPER(piv_attribute2) LIKE ('%VERIFIED%') THEN
      pov_attribute2 := UPPER(piv_attribute2);
    END IF;

  EXCEPTION
    WHEN OTHERS THEN
      pon_retcode := g_ret_error;
      print_log_message(SUBSTR('Exception in Procedure validate_att2_supp. ' ||
                               SQLERRM,
                               1,
                               2000));
  END validate_att2_supp;

  --
  -- ========================
  -- Procedure: validate_vat_code_supp
  -- =============================================================================
  --   This procedure is used to
  -- =============================================================================
  --  Input Parameters :
  --    None
  --  Output Parameters :
  --    None
  -- -----------------------------------------------------------------------------
  --
PROCEDURE validate_vat_code_supp (pov_errbuf  OUT NOCOPY VARCHAR2,
                                    pon_retcode OUT NOCOPY NUMBER) IS


 Cursor tax_cur is
Select distinct leg_vat_code,
                org_id,operating_unit_name,
                leg_accts_pay_code_segment1,
                interface_txn_id,
                leg_vendor_site_id,
                leg_vendor_id,
                leg_vendor_site_code
  from xxconv.xxap_supplier_Sites_stg
 WHERE batch_id = g_new_batch_id
   AND run_sequence_id = g_run_seq_id
   AND leg_vat_code IS NOT NULL;

l_default_vat_code         Varchar2(2000);
l_valerr_cnt               Varchar2(2000);
l_err_code                 Varchar2(2000);
l_err_msg                  Varchar2(2000);
l_tax_code                 Varchar2(2000);
l_column_name              Varchar2(2000);
l_column_value             Varchar2(2000);
l_tax                      Varchar2(2000);
l_tax_regime_code          Varchar2(2000);
l_tax_rate_code            Varchar2(2000);
l_tax_status_code          Varchar2(2000);
l_tax_jurisdiction_code    Varchar2(2000);
l_log_ret_status     Varchar2(2000);
l_log_err_msg  Varchar2(2000);
BEGIN

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
      l_default_vat_code        := NULL;
      --v4.0 below line added to handle errors correctly
      g_intf_staging_id := tax_rec.interface_txn_id;
      --

      xxetn_debug_pkg.add_debug(' +  PROCEDURE : validate_tax = ' || tax_rec.leg_vat_code || ' + ');
     -- l_record_cnt := 0;
      l_tax_code   := NULL;
      IF tax_rec.leg_vat_code IS NOT NULL THEN
        BEGIN

          xxetn_cross_ref_pkg.get_value(piv_eaton_ledger   => tax_rec.leg_accts_pay_code_segment1, -- Plant Number passed from cursor cur_plant
                                          piv_type           => 'XXEBTAX_TAX_CODE_MAPPING',
                                          piv_direction      => 'E',                               --value Required
                                          piv_application    => 'XXAP',                            --value Required
                                          piv_input_value1   => tax_rec.leg_vat_code,
                                          piv_input_value2   => null,                              --value Required
                                          piv_input_value3   => null,                              --value Required
                                          pid_effective_date => SYSDATE,                           --value Required  -- PASS DEFAULT
                                          pov_output_value1  => lv_out_val1,
                                          pov_output_value2  => lv_out_val2,
                                          pov_output_value3  => lv_out_val3,
                                          pov_err_msg        => lv_err_msg1);
           IF lv_err_msg1 IS NULL
           THEN
              l_tax_code :=  lv_out_val1;
              fnd_file.put_line(fnd_file.log,' in if else plant '||tax_rec.leg_accts_pay_code_segment1
              ||' is passed'|| ' with leg tax code '||tax_rec.leg_vat_code
              ||' got the value lv_out_val1 as '||lv_out_val1);
           ELSE
              lv_err_msg1 := NULL;
              lv_out_val1 := NULL;
              xxetn_cross_ref_pkg.get_value(piv_eaton_ledger   => NULL,                 -- Plant Number passed from cursor cur_plant
                                          piv_type           => 'XXEBTAX_TAX_CODE_MAPPING',
                                          piv_direction      => 'E',                    --value Required
                                          piv_application    => 'XXAP',                 --value Required
                                          piv_input_value1   => tax_rec.leg_vat_code,
                                          piv_input_value2   => null,                   --value Required
                                          piv_input_value3   => null,                   --value Required
                                          pid_effective_date => SYSDATE,                --value Required  -- PASS DEFAULT
                                          pov_output_value1  => lv_out_val1,
                                          pov_output_value2  => lv_out_val2,
                                          pov_output_value3  => lv_out_val3,
                                          pov_err_msg        => lv_err_msg1);
              IF lv_err_msg1 IS  NULL
              THEN
                 l_tax_code :=  lv_out_val1;

                 fnd_file.put_line(fnd_file.log,' in if else NO plant '||tax_rec.leg_accts_pay_code_segment1
              ||' is NOT passed'|| ' with leg tax code '||tax_rec.leg_vat_code
              ||' got the value lv_out_val1 as '||lv_out_val1);

              ELSE
                 l_tax_code :=  tax_rec.leg_vat_code;

                 fnd_file.put_line(fnd_file.log,' in if else NO plant tax code is assigned as is '||
                 tax_rec.leg_accts_pay_code_segment1||' Vat tax code for r12 will be '||l_tax_code||'-'||lv_err_msg1);
              END IF;
           END IF;

        EXCEPTION

          WHEN OTHERS THEN
            l_column_name  := 'LEG_VAT_CODE';
            l_column_value := tax_rec.leg_vat_code;
            l_valerr_cnt   := 2;
            l_tax_code     := NULL;
            print_log_message('In When others of tax Cross ref check' || SQLERRM);
            l_err_code := 'ETN_AP_INVALID_VAT_CODE';
            l_err_msg  := 'Error while deriving cross reference for tax code ' || SUBSTR(SQLERRM, 1, 150);
       log_errors(pov_return_status       => l_log_ret_status -- OUT
                  ,pov_error_msg           => l_log_err_msg -- OUT
                  ,piv_source_column_name  => l_column_name,
                   piv_source_column_value => l_column_value,
                   piv_error_type          => g_err_val,
                   piv_error_code          => l_err_code,
                   piv_error_message       => l_err_msg);

        END;
      END IF;

    IF (l_tax_code IS NOT NULL) THEN
        BEGIN
      ------Default Vat Tax verification in look up-------------------------------
       SELECT Count(1)
         INTO l_default_vat_code
         FROM fnd_lookup_values flv
        WHERE flv.lookup_type = 'XXETN_DEF_TAX_RATES'
          AND LTRIM(RTRIM(flv.tag)) = LTRIM(RTRIM(l_tax_code))
          AND LTRIM(RTRIM(flv.attribute1)) = LTRIM(RTRIM(l_tax_code))
          AND LTRIM(RTRIM(flv.description)) = LTRIM(RTRIM(tax_rec.operating_unit_name))
          AND flv.enabled_flag = 'Y'
          AND SYSDATE BETWEEN NVL(flv.start_date_active, SYSDATE) AND
              NVL(flv.end_date_active, SYSDATE + 1)
          AND flv.language = USERENV('LANG');

        /*d.  If (b) exists in lookup referred in Pt(c) against R12 OU then donot populate any value in Tax Classification code (field details in Pt 4)*/
     IF l_default_vat_code <> 0 THEN
       UPDATE xxap_supplier_Sites_stg
             SET vat_code              = NULL
           WHERE leg_vat_code = tax_rec.leg_vat_code
             AND batch_id = g_new_batch_id
             AND run_sequence_id = g_run_seq_id
             AND interface_txn_id = tax_rec.interface_txn_id
             AND leg_vendor_site_id =tax_rec.leg_vendor_site_id
             AND leg_vendor_id  =tax_rec.leg_vendor_id
             AND leg_vendor_site_code  =tax_rec.leg_vendor_site_code
             AND leg_accts_pay_code_segment1 = tax_rec.leg_accts_pay_code_segment1;

           COMMIT;

      ELSE
         /*e.  If (b) does not exists in lookup referred in Pt(c) against R12 OU then populate any value in Tax Classification code. (field details in Pt 4)*/
         /* fetch values corresponding to the R12 tax code if value in not configure in cross -referrence tables*/

          SELECT DISTINCT zrb.tax,
                          zrb.tax_regime_code,
                          zrb.tax_rate_code,
                          zrb.tax_status_code,
                          zrb.tax_jurisdiction_code
            INTO l_tax,
                 l_tax_regime_code,
                 l_tax_rate_code,
                 l_tax_status_code,
                 l_tax_jurisdiction_code
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
             AND ZRB.ACTIVE_FLAG = 'Y';

          UPDATE xxap_supplier_Sites_stg
             SET vat_code              = l_tax_rate_code  --16336
           WHERE leg_vat_code = tax_rec.leg_vat_code
             AND batch_id = g_new_batch_id
             AND run_sequence_id = g_run_seq_id
              AND interface_txn_id = tax_rec.interface_txn_id
             AND leg_vendor_site_id =tax_rec.leg_vendor_site_id
             AND leg_vendor_id  =tax_rec.leg_vendor_id
             AND leg_vendor_site_code  =tax_rec.leg_vendor_site_code
             AND leg_accts_pay_code_segment1 = tax_rec.leg_accts_pay_code_segment1;

           COMMIT;
        END IF;
        EXCEPTION
          WHEN NO_DATA_FOUND THEN
            l_column_name  := 'R12_VAT_CODE';
            l_column_value := l_tax_code;
            l_valerr_cnt   := 2;
            print_log_message('In No Data found of R12 tax code check' ||
                              SQLERRM);
            l_err_code := 'ETN_AP_INVALID_VAT_CODE';
            l_err_msg  := 'Cross Ref And R12 Tax Code Not Defined for Plant'||tax_rec.leg_accts_pay_code_segment1;
            log_errors(pov_return_status       => l_log_ret_status -- OUT
                  ,pov_error_msg           => l_log_err_msg -- OUT
                  ,piv_source_column_name  => l_column_name,
                   piv_source_column_value => l_column_value,
                   piv_error_type          => g_err_val,
                   piv_error_code          => l_err_code,
                   piv_error_message       => l_err_msg);
          WHEN OTHERS THEN
            l_column_name  := 'R12_VAT_CODE';
            l_column_value := l_tax_code;
            l_valerr_cnt   := 2;
            print_log_message('In When others of R12 tax code check' ||
                              SQLERRM);
            l_err_code := 'ETN_AP_INVALID_VAT_CODE';
            l_err_msg  := 'Error while deriving Cross Ref And R12 tax code  for Plant'||tax_rec.leg_accts_pay_code_segment1||'-'||SUBSTR(SQLERRM, 1, 150);
            log_errors(pov_return_status       => l_log_ret_status -- OUT
                  ,pov_error_msg           => l_log_err_msg -- OUT
                  ,piv_source_column_name  => l_column_name,
                   piv_source_column_value => l_column_value,
                   piv_error_type          => g_err_val,
                   piv_error_code          => l_err_code,
                   piv_error_message       => l_err_msg);
        END;

    END IF;

      IF l_valerr_cnt = 2 THEN
        UPDATE xxap_supplier_Sites_stg
           SET process_flag      = 'E',
               error_type        = 'ERR_VAL',
               last_update_date = SYSDATE,
               last_updated_by   = g_user_id,
               last_update_login = g_login_id
         WHERE leg_Vat_code = tax_rec.leg_vat_code
           AND batch_id = g_new_batch_id
           AND leg_accts_pay_code_segment1 = tax_rec.leg_accts_pay_code_segment1
           AND run_sequence_id = g_run_seq_id
           AND interface_txn_id = tax_rec.interface_txn_id
           AND leg_vendor_site_id =tax_rec.leg_vendor_site_id
           AND leg_vendor_id  =tax_rec.leg_vendor_id
           AND leg_vendor_site_code  =tax_rec.leg_vendor_site_code;
       END IF;
      END LOOP;
  EXCEPTION
    WHEN OTHERS THEN
      pon_retcode := g_ret_error;
      print_log_message(SUBSTR('Exception in Procedure validate_vat_code_supp. ' ||
                               SQLERRM,
                               1,
                               2000));
  END validate_vat_code_supp;

  --
  -- ========================
  -- Procedure: validate_employee_supp
  -- =============================================================================
  --   This procedure is used to validate employee number for Vendor Type as Employee
  -- =============================================================================
  --  Input Parameters :
  --    None
  --  Output Parameters :
  --    None
  -- -----------------------------------------------------------------------------
  --
  PROCEDURE validate_employee_supp(piv_employee_num IN VARCHAR2,
                                   pov_employee_id  OUT NUMBER,
                                   pon_retcode      OUT NUMBER) IS
    l_log_ret_status VARCHAR2(50);
    l_log_err_msg    VARCHAR2(2000);
  BEGIN
    pon_retcode := g_normal;

    BEGIN
      SELECT person_id
        INTO pov_employee_id
        FROM per_all_people_f
       WHERE employee_number = piv_employee_num
         AND NVL(current_employee_flag, 'X') = g_yes
         AND TRUNC(SYSDATE) BETWEEN
             TRUNC(NVL(effective_start_date, SYSDATE)) AND
             TRUNC(NVL(effective_end_date, SYSDATE));
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
           pon_retcode := '100';
        --   l_supplier_tbl(indx).leg_vendor_type_lookup_code := 'VENDOR';

      WHEN OTHERS THEN
        pon_retcode := g_warning;
        log_errors(pov_return_status       => l_log_ret_status -- OUT
                  ,
                   pov_error_msg           => l_log_err_msg -- OUT
                  ,
                   piv_source_column_name  => 'LEG_EMPLOYEE_NUMBER',
                   piv_source_column_value => piv_employee_num,
                   piv_error_type          => g_err_val,
                   piv_error_code          => 'ETN_AP_INVALID_EMPLOYEE',
                   piv_error_message       => 'Error : Employee Number is not valid, Oracle Error is ' ||
                                              SQLERRM);
    END;

  EXCEPTION
    WHEN OTHERS THEN
      pon_retcode := g_ret_error;
      print_log_message(SUBSTR('Exception in Procedure validate_employee_supp. ' ||
                               SQLERRM,
                               1,
                               2000));
  END validate_employee_supp;

  --
  -- ========================
  -- Procedure: validate_pymt_method_supp
  -- =============================================================================
  --   This procedure is used to
  -- =============================================================================
  --  Input Parameters :
  --    None
  --  Output Parameters :
  --    None
  -- -----------------------------------------------------------------------------
  --
  PROCEDURE validate_pymt_method_supp(piv_lookup_code IN VARCHAR2,
                                      pov_lookup_code OUT VARCHAR2,
                                      pon_retcode     OUT NUMBER) IS
    l_pay_method_lkp CONSTANT VARCHAR2(50) := 'XXAP_PAYMENT_METHOD_LOOKUP';

    l_lookup_code fnd_lookup_values.lookup_code%TYPE;
    l_status      VARCHAR2(1);

    l_log_ret_status VARCHAR2(50);
    l_log_err_msg    VARCHAR2(2000);
    l_err_count      NUMBER := 0;
  BEGIN
    pon_retcode := g_normal;

    BEGIN
      SELECT flv.description
        INTO l_lookup_code
        FROM fnd_lookup_values flv
       WHERE flv.lookup_type = l_pay_method_lkp
         AND flv.meaning = piv_lookup_code
         AND flv.enabled_flag = g_yes
         AND SYSDATE BETWEEN NVL(flv.start_date_active, SYSDATE) AND
             NVL(flv.end_date_active, SYSDATE + 1)
         AND flv.language = USERENV('LANG');
    EXCEPTION
      WHEN OTHERS THEN
        l_err_count := l_err_count + 1;
        log_errors(pov_return_status       => l_log_ret_status -- OUT
                  ,
                   pov_error_msg           => l_log_err_msg -- OUT
                  ,
                   piv_source_column_name  => 'LEG_PAYMENT_METHOD_CODE',
                   piv_source_column_value => piv_lookup_code,
                   piv_error_type          => g_err_val,
                   piv_error_code          => 'ETN_AP_INVALID_PAYMENT_METHOD',
                   piv_error_message       => 'Error : Lookup Code ' ||
                                              piv_lookup_code ||
                                              ' for Payment Method Lookup (' ||
                                              l_pay_method_lkp ||
                                              ') setup issue. Oracle Error is ' ||
                                              SQLERRM);
    END;

    IF l_err_count = 0 THEN
      BEGIN
        SELECT 'N'
          INTO l_status
          FROM iby_payment_methods_b ipmb
         WHERE SYSDATE <= NVL(ipmb.inactive_date, SYSDATE)
           AND UPPER(ipmb.payment_method_code) = UPPER(l_lookup_code)
        --AND ipmb.enabled_flag = g_yes
        ;

        pov_lookup_code := l_lookup_code;
      EXCEPTION
        WHEN OTHERS THEN
          log_errors(pov_return_status       => l_log_ret_status -- OUT
                    ,
                     pov_error_msg           => l_log_err_msg -- OUT
                    ,
                     piv_source_column_name  => 'LEG_PAYMENT_METHOD_CODE',
                     piv_source_column_value => piv_lookup_code,
                     piv_error_type          => g_err_val,
                     piv_error_code          => 'ETN_AP_INVALID_PAYMENT_METHOD',
                     piv_error_message       => 'Error : Payment Method (' ||
                                                l_lookup_code ||
                                                ') does not exist in R12, Oracle Error is ' ||
                                                SQLERRM);
      END;
    END IF;

    IF l_err_count > 0 THEN
      pon_retcode := g_warning;
    END IF;
  EXCEPTION
    WHEN OTHERS THEN
      pon_retcode := g_ret_error;
      print_log_message(SUBSTR('Exception in Procedure validate_pymt_method_supp. ' ||
                               SQLERRM,
                               1,
                               2000));
  END validate_pymt_method_supp;

  --
  -- ========================
  -- Procedure: validate_rcv_code_supp
  -- =============================================================================
  --   This procedure is used to
  -- =============================================================================
  --  Input Parameters :
  --    None
  --  Output Parameters :
  --    None
  -- -----------------------------------------------------------------------------
  --
  PROCEDURE validate_rcv_code_supp(piv_lookup_code IN VARCHAR2,
                                   pov_lookup_code OUT VARCHAR2,
                                   pon_retcode     OUT NUMBER) IS
    l_status VARCHAR2(1);

    l_log_ret_status VARCHAR2(50);
    l_log_err_msg    VARCHAR2(2000);
  BEGIN
    pon_retcode := g_normal;

    BEGIN
      SELECT 'N'
        INTO l_status
        FROM fnd_lookup_values flv
       WHERE lookup_type = 'RCV OPTION'
         AND UPPER(lookup_code) = UPPER(piv_lookup_code)
         AND SYSDATE BETWEEN NVL(start_date_active, SYSDATE) AND
             NVL(end_date_active, SYSDATE)
         AND enabled_flag = g_yes
         AND language = USERENV('LANG');

      pov_lookup_code := UPPER(piv_lookup_code);
    EXCEPTION
      WHEN OTHERS THEN
        pon_retcode := g_warning;
        log_errors(pov_return_status       => l_log_ret_status -- OUT
                  ,
                   pov_error_msg           => l_log_err_msg -- OUT
                  ,
                   piv_source_column_name  => 'LEG_QTY_RCV_EXCEPTION_CODE',
                   piv_source_column_value => piv_lookup_code,
                   piv_error_type          => g_err_val,
                   piv_error_code          => 'ETN_AP_INVALID_RCV_CODE',
                   piv_error_message       => 'Error : RCV Exception Code is not valid, Oracle Error is ' ||
                                              SQLERRM);
    END;

  EXCEPTION
    WHEN OTHERS THEN
      pon_retcode := g_ret_error;
      print_log_message(SUBSTR('Exception in Procedure validate_rcv_code_supp. ' ||
                               SQLERRM,
                               1,
                               2000));
  END validate_rcv_code_supp;

  --
  -- ========================
  -- Procedure: validate_type_1099_supp
  -- =============================================================================
  --   This procedure is used to
  -- =============================================================================
  --  Input Parameters :
  --    None
  --  Output Parameters :
  --    None
  -- -----------------------------------------------------------------------------
  --
  PROCEDURE validate_type_1099_supp(piv_type1099 IN VARCHAR2,
                                    pon_retcode  OUT NUMBER) IS
    l_status VARCHAR2(1);

    l_log_ret_status VARCHAR2(50);
    l_log_err_msg    VARCHAR2(2000);
  BEGIN
    pon_retcode := g_normal;

    BEGIN
      SELECT 'N'
        INTO l_status
        FROM ap_income_tax_types aitt
       WHERE SYSDATE <= NVL(aitt.inactive_date, SYSDATE)
         AND income_tax_type = piv_type1099;
    EXCEPTION
      WHEN OTHERS THEN
        pon_retcode := g_warning;
        log_errors(pov_return_status       => l_log_ret_status -- OUT
                  ,
                   pov_error_msg           => l_log_err_msg -- OUT
                  ,
                   piv_source_column_name  => 'LEG_TYPE_1099',
                   piv_source_column_value => piv_type1099,
                   piv_error_type          => g_err_val,
                   piv_error_code          => 'ETN_AP_INVALID_TYPE_1099',
                   piv_error_message       => 'Error : Type 1099 value is not valid, Oracle Error is ' ||
                                              SQLERRM);
    END;

  EXCEPTION
    WHEN OTHERS THEN
      pon_retcode := g_ret_error;
      print_log_message(SUBSTR('Exception in Procedure validate_type_1099_supp. ' ||
                               SQLERRM,
                               1,
                               2000));
  END validate_type_1099_supp;

  --
  -- ========================
  -- Procedure: validate_pay_date_basis_supp
  -- =============================================================================
  --   This procedure is used to
  -- =============================================================================
  --  Input Parameters :
  --    None
  --  Output Parameters :
  --    None
  -- -----------------------------------------------------------------------------
  --
  PROCEDURE validate_pay_date_basis_supp(piv_lookup_code IN VARCHAR2,
                                         pov_lookup_code OUT VARCHAR2,
                                         pon_retcode     OUT NUMBER) IS
    l_status VARCHAR2(1);

    l_log_ret_status VARCHAR2(50);
    l_log_err_msg    VARCHAR2(2000);
  BEGIN
    pon_retcode := g_normal;

    BEGIN
      SELECT flv.lookup_code
        INTO pov_lookup_code
        FROM fnd_lookup_values flv
       WHERE flv.lookup_type = 'PAY DATE BASIS'
         AND UPPER(flv.lookup_code) = UPPER(piv_lookup_code)
         AND SYSDATE BETWEEN NVL(flv.start_date_active, SYSDATE) AND
             NVL(flv.end_date_active, SYSDATE)
         AND flv.language = USERENV('LANG')
         AND ROWNUM = 1;

    EXCEPTION
      WHEN OTHERS THEN
        pon_retcode := g_warning;
        log_errors(pov_return_status       => l_log_ret_status -- OUT
                  ,
                   pov_error_msg           => l_log_err_msg -- OUT
                  ,
                   piv_source_column_name  => 'LEG_PAY_DATE_BASIS_CODE',
                   piv_source_column_value => piv_lookup_code,
                   piv_error_type          => g_err_val,
                   piv_error_code          => 'ETN_AP_INVALID_PAY_DATE_BASIS',
                   piv_error_message       => 'Error : Pay Date Basis code is not valid, Oracle Error is ' ||
                                              SQLERRM);
    END;

  EXCEPTION
    WHEN OTHERS THEN
      pon_retcode := g_ret_error;
      print_log_message(SUBSTR('Exception in Procedure validate_pay_date_basis_supp. ' ||
                               SQLERRM,
                               1,
                               2000));
  END validate_pay_date_basis_supp;

  --
  -- ========================
  -- Procedure: validate_match_options_supp
  -- =============================================================================
  --   This procedure is used to
  -- =============================================================================
  --  Input Parameters :
  --    None
  --  Output Parameters :
  --    None
  -- -----------------------------------------------------------------------------
  --
  PROCEDURE validate_match_options_supp(piv_lookup_code IN VARCHAR2,
                                        pov_lookup_code OUT VARCHAR2,
                                        pon_retcode     OUT NUMBER) IS
    l_status VARCHAR2(1);

    l_log_ret_status VARCHAR2(50);
    l_log_err_msg    VARCHAR2(2000);
  BEGIN
    pon_retcode := g_normal;

    BEGIN
      SELECT 'N'
        INTO l_status
        FROM fnd_lookup_values flv
       WHERE flv.lookup_type = 'PO INVOICE MATCH OPTION'
         AND UPPER(flv.lookup_code) = UPPER(piv_lookup_code)
         AND SYSDATE BETWEEN NVL(flv.start_date_active, SYSDATE) AND
             NVL(flv.end_date_active, SYSDATE)
         AND flv.language = USERENV('LANG');

      pov_lookup_code := UPPER(piv_lookup_code);
    EXCEPTION
      WHEN OTHERS THEN
        pon_retcode := g_warning;
        log_errors(pov_return_status       => l_log_ret_status -- OUT
                  ,
                   pov_error_msg           => l_log_err_msg -- OUT
                  ,
                   piv_source_column_name  => 'LEG_MATCH_OPTION',
                   piv_source_column_value => piv_lookup_code,
                   piv_error_type          => g_err_val,
                   piv_error_code          => 'ETN_AP_INVALID_MATCH_OPTIONS',
                   piv_error_message       => 'Error : Match Option is not valid, Oracle Error is ' ||
                                              SQLERRM);
    END;

  EXCEPTION
    WHEN OTHERS THEN
      pon_retcode := g_ret_error;
      print_log_message(SUBSTR('Exception in Procedure validate_match_options_supp. ' ||
                               SQLERRM,
                               1,
                               2000));
  END validate_match_options_supp;

  --
  -- ========================
  --
  -- ========================
  -- Procedure: validate_minority_group_supp
  -- =============================================================================
  --   This procedure is used to
  -- =============================================================================
  --  Input Parameters :
  --    None
  --  Output Parameters :
  --    None
  -- -----------------------------------------------------------------------------
  --
  PROCEDURE validate_minority_group_supp(piv_lookup_code IN VARCHAR2,
                                         pov_lookup_code OUT VARCHAR2,
                                         pon_retcode     OUT NUMBER) IS
    l_minority_lkp CONSTANT VARCHAR2(30) := 'XXAP_MINORITY_GRP_LOOKUP';

    l_lookup_code fnd_lookup_values.lookup_code%TYPE;
    l_status      VARCHAR2(1);

    l_log_ret_status VARCHAR2(50);
    l_log_err_msg    VARCHAR2(2000);
    l_err_count      NUMBER := 0;
  BEGIN
    pon_retcode := g_normal;

    BEGIN
      SELECT flv.description
        INTO l_lookup_code
        FROM fnd_lookup_values flv
       WHERE flv.lookup_type = l_minority_lkp
         AND flv.meaning = piv_lookup_code
         AND flv.enabled_flag = g_yes
         AND SYSDATE BETWEEN NVL(flv.start_date_active, SYSDATE) AND
             NVL(flv.end_date_active, SYSDATE + 1)
         AND flv.language = USERENV('LANG');
    EXCEPTION
      WHEN OTHERS THEN
        l_err_count := l_err_count + 1;
        log_errors(pov_return_status       => l_log_ret_status -- OUT
                  ,
                   pov_error_msg           => l_log_err_msg -- OUT
                  ,
                   piv_source_column_name  => 'LEG_MINORITY_GROUP_CODE',
                   piv_source_column_value => piv_lookup_code,
                   piv_error_type          => g_err_val,
                   piv_error_code          => 'ETN_AP_INVALID_MINORITY_GROUP',
                   piv_error_message       => 'Error : Lookup Code ' ||
                                              piv_lookup_code ||
                                              ' for Minority Type Lookup (' ||
                                              l_minority_lkp ||
                                              ') setup issue. Oracle Error is ' ||
                                              SQLERRM);
    END;

    IF l_err_count = 0 THEN
      BEGIN
        SELECT 'N'
          INTO l_status
          FROM po_lookup_codes plc
         WHERE plc.lookup_type = 'MINORITY GROUP'
           AND UPPER(plc.lookup_code) = UPPER(l_lookup_code)
           AND plc.enabled_flag = g_yes
           AND SYSDATE <= NVL(plc.inactive_date, SYSDATE);

        pov_lookup_code := l_lookup_code;
      EXCEPTION
        WHEN OTHERS THEN
          log_errors(pov_return_status       => l_log_ret_status -- OUT
                    ,
                     pov_error_msg           => l_log_err_msg -- OUT
                    ,
                     piv_source_column_name  => 'LEG_MINORITY_GROUP_CODE',
                     piv_source_column_value => piv_lookup_code,
                     piv_error_type          => g_err_val,
                     piv_error_code          => 'ETN_AP_INVALID_MINORITY_GROUP',
                     piv_error_message       => 'Error : Lookup Code (' ||
                                                l_lookup_code ||
                                                ') setup issue for MINORITY GROUP Lookup in R12, Oracle Error is ' ||
                                                SQLERRM);
      END;
    END IF;

    IF l_err_count > 0 THEN
      pon_retcode := g_warning;
    END IF;
  EXCEPTION
    WHEN OTHERS THEN
      pon_retcode := g_ret_error;
      print_log_message(SUBSTR('Exception in Procedure validate_minority_group_supp. ' ||
                               SQLERRM,
                               1,
                               2000));
  END validate_minority_group_supp;

  -- Procedure: validate_org_type_supp
  -- =============================================================================
  --   This procedure is used to
  -- =============================================================================
  --  Input Parameters :
  --    None
  --  Output Parameters :
  --    None
  -- -----------------------------------------------------------------------------
  --
  PROCEDURE validate_org_type_supp(piv_lookup_code IN VARCHAR2,
                                   pov_lookup_code OUT VARCHAR2,
                                   pon_retcode     OUT NUMBER) IS
    l_org_type_lkp CONSTANT VARCHAR2(30) := 'XXAP_ORG_TYPE_LOOKUP';

    l_lookup_code fnd_lookup_values.lookup_code%TYPE;
    l_status      VARCHAR2(1);

    l_log_ret_status VARCHAR2(50);
    l_log_err_msg    VARCHAR2(2000);
    l_err_count      NUMBER := 0;
  BEGIN
    pon_retcode := g_normal;

    BEGIN
      SELECT flv.description
        INTO l_lookup_code
        FROM fnd_lookup_values flv
       WHERE flv.lookup_type = l_org_type_lkp
         AND flv.meaning = piv_lookup_code
         AND flv.enabled_flag = g_yes
         AND SYSDATE BETWEEN NVL(flv.start_date_active, SYSDATE) AND
             NVL(flv.end_date_active, SYSDATE + 1)
         AND flv.language = USERENV('LANG');
    EXCEPTION
      WHEN OTHERS THEN
        l_err_count := l_err_count + 1;
        log_errors(pov_return_status       => l_log_ret_status -- OUT
                  ,
                   pov_error_msg           => l_log_err_msg -- OUT
                  ,
                   piv_source_column_name  => 'LEG_ORGANIZATION_TYPE_CODE',
                   piv_source_column_value => piv_lookup_code,
                   piv_error_type          => g_err_val,
                   piv_error_code          => 'ETN_AP_INVALID_ORGANIZATION_TYPE',
                   piv_error_message       => 'Error : Lookup Code ' ||
                                              piv_lookup_code ||
                                              ' for Organization Type Lookup (' ||
                                              l_org_type_lkp ||
                                              ') setup issue. Oracle Error is ' ||
                                              SQLERRM);
    END;

    IF l_err_count = 0 THEN
      BEGIN
        SELECT 'N'
          INTO l_status
          FROM po_lookup_codes plc
         WHERE plc.lookup_type = 'ORGANIZATION TYPE'
           AND UPPER(plc.lookup_code) = UPPER(l_lookup_code)
           AND plc.enabled_flag = g_yes
           AND SYSDATE <= NVL(plc.inactive_date, SYSDATE)
        --AND plc.language = USERENV ('LANG')
        ;

        pov_lookup_code := UPPER(l_lookup_code);
      EXCEPTION
        WHEN OTHERS THEN
          l_err_count := l_err_count + 1;
          log_errors(pov_return_status       => l_log_ret_status -- OUT
                    ,
                     pov_error_msg           => l_log_err_msg -- OUT
                    ,
                     piv_source_column_name  => 'LEG_ORGANIZATION_TYPE_CODE',
                     piv_source_column_value => piv_lookup_code,
                     piv_error_type          => g_err_val,
                     piv_error_code          => 'ETN_AP_INVALID_ORGANIZATION_TYPE',
                     piv_error_message       => 'Error : Lookup Code (' ||
                                                l_lookup_code ||
                                                ') setup issue for PAY GROUP Lookup in R12, Oracle Error is ' ||
                                                SQLERRM);
      END;
    END IF;

    IF l_err_count > 0 THEN
      pon_retcode := g_warning;
    END IF;
  EXCEPTION
    WHEN OTHERS THEN
      pon_retcode := g_ret_error;
      print_log_message(SUBSTR('Exception in Procedure validate_org_type_supp. ' ||
                               SQLERRM,
                               1,
                               2000));
  END validate_org_type_supp;

  --
  -- ========================
  -- Procedure: validate_currency_code_supp
  -- =============================================================================
  --   This procedure is used to
  -- =============================================================================
  --  Input Parameters :
  --    None
  --  Output Parameters :
  --    None
  -- -----------------------------------------------------------------------------
  --
  PROCEDURE validate_currency_code_supp(piv_currency_code IN VARCHAR2,
                                        piv_code_type     IN VARCHAR2,
                                        pon_retcode       OUT NUMBER) IS
    l_status VARCHAR2(1);

    l_log_ret_status VARCHAR2(50);
    l_log_err_msg    VARCHAR2(2000);
  BEGIN
    pon_retcode := g_normal;

    BEGIN
      SELECT 'N'
        INTO l_status
        FROM fnd_currencies fc
       WHERE UPPER(fc.currency_code) = UPPER(piv_currency_code)
         AND SYSDATE BETWEEN NVL(fc.start_date_active, SYSDATE) AND
             NVL(fc.end_date_active, SYSDATE)
         AND fc.enabled_flag = g_yes;
    EXCEPTION
      WHEN OTHERS THEN
        pon_retcode := g_warning;
        log_errors(pov_return_status       => l_log_ret_status -- OUT
                  ,
                   pov_error_msg           => l_log_err_msg -- OUT
                  ,
                   piv_source_column_name  => piv_code_type,
                   piv_source_column_value => piv_currency_code,
                   piv_error_type          => g_err_val,
                   piv_error_code          => 'ETN_AP_INVALID_CURRENCY_CODE',
                   piv_error_message       => 'Error : Currency code is not valid, Oracle Error is ' ||
                                              SQLERRM);
    END;

  EXCEPTION
    WHEN OTHERS THEN
      pon_retcode := g_ret_error;
      print_log_message(SUBSTR('Exception in Procedure validate_currency_code_supp. ' ||
                               SQLERRM,
                               1,
                               2000));
  END validate_currency_code_supp;

  --
  -- ========================
  -- Procedure: validate_bank_bearer_supp
  -- =============================================================================
  --   This procedure is used to
  -- =============================================================================
  --  Input Parameters :
  --    None
  --  Output Parameters :
  --    None
  -- -----------------------------------------------------------------------------
  --
  PROCEDURE validate_bank_bearer_supp(piv_lookup_code IN VARCHAR2,
                                      pov_lookup_code OUT VARCHAR2,
                                      pon_retcode     OUT NUMBER) IS
    l_status VARCHAR2(1);

    l_log_ret_status VARCHAR2(50);
    l_log_err_msg    VARCHAR2(2000);
  BEGIN
    pon_retcode := g_normal;

    BEGIN
      SELECT 'N'
        INTO l_status
        FROM ap_lookup_codes alc
       WHERE alc.lookup_type = 'BANK CHARGE BEARER'
         AND UPPER(alc.lookup_code) = UPPER(piv_lookup_code)
         AND SYSDATE <= NVL(alc.inactive_date, SYSDATE)
         AND alc.enabled_flag = g_yes
      --AND alc.language = USERENV ('LANG')
      ;

      pov_lookup_code := UPPER(piv_lookup_code);
    EXCEPTION
      WHEN OTHERS THEN
        pon_retcode := g_warning;
        log_errors(pov_return_status       => l_log_ret_status -- OUT
                  ,
                   pov_error_msg           => l_log_err_msg -- OUT
                  ,
                   piv_source_column_name  => 'LEG_BANK_CHARGE_BEARER',
                   piv_source_column_value => piv_lookup_code,
                   piv_error_type          => g_err_val,
                   piv_error_code          => 'ETN_AP_INVALID_BANK_CHARGE_BEARER_CODE',
                   piv_error_message       => 'Error : Lookup Code (' ||
                                              piv_lookup_code ||
                                              ') setup issue for BANK CHARGE BEARER Lookup in R12, Oracle Error is ' ||
                                              SQLERRM);
    END;

  EXCEPTION
    WHEN OTHERS THEN
      pon_retcode := g_ret_error;
      print_log_message(SUBSTR('Exception in Procedure validate_bank_bearer_supp. ' ||
                               SQLERRM,
                               1,
                               2000));
  END validate_bank_bearer_supp;

  --
  -- ========================
  -- Procedure: validate_pay_group_supp
  -- =============================================================================
  --   This procedure is used to
  -- =============================================================================
  --  Input Parameters :
  --    None
  --  Output Parameters :
  --    None
  -- -----------------------------------------------------------------------------
  --
  PROCEDURE validate_pay_group_supp(piv_lookup_code IN VARCHAR2,
                                    pov_lookup_code OUT VARCHAR2,
                                    pon_retcode     OUT NUMBER) IS
    l_pay_grp_lkp CONSTANT VARCHAR2(30) := 'XXAP_PAY_GROUP_LOOKUP_CODE';

    l_lookup_code fnd_lookup_values.lookup_code%TYPE;
    l_status      VARCHAR2(1);

    l_log_ret_status VARCHAR2(50);
    l_log_err_msg    VARCHAR2(2000);
    l_err_count      NUMBER := 0;
  BEGIN
    pon_retcode := g_normal;

    BEGIN
      SELECT flv.description
        INTO l_lookup_code
        FROM fnd_lookup_values flv
       WHERE flv.lookup_type = l_pay_grp_lkp
         AND flv.meaning = piv_lookup_code
         AND flv.enabled_flag = g_yes
         AND SYSDATE BETWEEN NVL(flv.start_date_active, SYSDATE) AND
             NVL(flv.end_date_active, SYSDATE + 1)
         AND flv.language = USERENV('LANG');
    EXCEPTION
      WHEN OTHERS THEN
        l_err_count := l_err_count + 1;
        log_errors(pov_return_status       => l_log_ret_status -- OUT
                  ,
                   pov_error_msg           => l_log_err_msg -- OUT
                  ,
                   piv_source_column_name  => 'LEG_PAY_GROUP_LOOKUP_CODE',
                   piv_source_column_value => piv_lookup_code,
                   piv_error_type          => g_err_val,
                   piv_error_code          => 'ETN_AP_INVALID_PAY_GROUP_CODE',
                   piv_error_message       => 'Error : Lookup Code ' ||
                                              piv_lookup_code ||
                                              ' for Pay Group Lookup (' ||
                                              l_pay_grp_lkp ||
                                              ') setup issue. Oracle Error is ' ||
                                              SQLERRM);
    END;

    IF l_err_count = 0 THEN
      BEGIN
        SELECT 'N'
          INTO l_status
          FROM fnd_lookup_values flv
         WHERE flv.lookup_type = 'PAY GROUP'
           AND UPPER(lookup_code) = UPPER(l_lookup_code)
           AND SYSDATE BETWEEN NVL(flv.start_date_active, SYSDATE) AND
               NVL(flv.end_date_active, SYSDATE)
           AND flv.enabled_flag = g_yes
           AND flv.language = USERENV('LANG')
           AND view_application_id =
               (SELECT application_id
                  FROM fnd_application
                 WHERE
                --application_short_name = 'SQLAP'
                 application_short_name = 'PO');

        pov_lookup_code := l_lookup_code;
      EXCEPTION
        WHEN OTHERS THEN
          l_err_count := l_err_count + 1;
          log_errors(pov_return_status       => l_log_ret_status -- OUT
                    ,
                     pov_error_msg           => l_log_err_msg -- OUT
                    ,
                     piv_source_column_name  => 'LEG_PAY_GROUP_LOOKUP_CODE',
                     piv_source_column_value => piv_lookup_code,
                     piv_error_type          => g_err_val,
                     piv_error_code          => 'ETN_AP_INVALID_PAY_GROUP_CODE',
                     piv_error_message       => 'Error : Lookup Code (' ||
                                                l_lookup_code ||
                                                ')derived from Custom Lookup does not exist in standard lookup PAY GROUP or is invalid, Oracle Error is ' ||
                                                SQLERRM);
      END;
    END IF;

    IF l_err_count > 0 THEN
      pon_retcode := g_warning;
    END IF;
  EXCEPTION
    WHEN OTHERS THEN
      pon_retcode := g_ret_error;
      print_log_message(SUBSTR('Exception in Procedure validate_pay_group_supp. ' ||
                               SQLERRM,
                               1,
                               2000));
  END validate_pay_group_supp;

  --
  -- ========================
  -- Procedure: validate_fob_code_supp
  -- =============================================================================
  --   This procedure is used to
  -- =============================================================================
  --  Input Parameters :
  --    None
  --  Output Parameters :
  --    None
  -- -----------------------------------------------------------------------------
  --
  PROCEDURE validate_fob_code_supp(piv_lookup_code IN VARCHAR2,
                                   pov_lookup_code OUT VARCHAR2,
                                   pon_retcode     OUT NUMBER) IS
    l_fob_lkp CONSTANT VARCHAR2(30) := 'XXAP_FOB_LOOKUP_CODE';

    l_lookup_code fnd_lookup_values.lookup_code%TYPE;
    l_status      VARCHAR2(1);

    l_log_ret_status VARCHAR2(50);
    l_log_err_msg    VARCHAR2(2000);
    l_err_count      NUMBER := 0;
  BEGIN
    pon_retcode := g_normal;

    BEGIN
      SELECT flv.description
        INTO l_lookup_code
        FROM fnd_lookup_values flv
       WHERE flv.lookup_type = l_fob_lkp
         AND flv.meaning = piv_lookup_code
         AND flv.enabled_flag = g_yes
         AND SYSDATE BETWEEN NVL(flv.start_date_active, SYSDATE) AND
             NVL(flv.end_date_active, SYSDATE + 1)
         AND language = USERENV('LANG');
    EXCEPTION
      WHEN OTHERS THEN
        l_err_count := l_err_count + 1;
        log_errors(pov_return_status       => l_log_ret_status -- OUT
                  ,
                   pov_error_msg           => l_log_err_msg -- OUT
                  ,
                   piv_source_column_name  => 'LEG_FOB_LOOKUP_CODE',
                   piv_source_column_value => piv_lookup_code,
                   piv_error_type          => g_err_val,
                   piv_error_code          => 'ETN_AP_INVALID_FOB_CODE',
                   piv_error_message       => 'Error : Lookup Code ' ||
                                              piv_lookup_code ||
                                              ' for FOB Lookup (' ||
                                              l_fob_lkp ||
                                              ') setup issue. Oracle Error is ' ||
                                              SQLERRM);
    END;

    IF l_err_count = 0 THEN
      BEGIN
        SELECT 'N'
          INTO l_status
          FROM fnd_lookup_values flv
         WHERE flv.lookup_type = 'FOB'
           AND UPPER(flv.lookup_code) = UPPER(l_lookup_code)
           AND TRUNC(SYSDATE) BETWEEN
               NVL(start_date_active, TRUNC(SYSDATE)) AND
               NVL(end_date_active, TRUNC(SYSDATE + 1))
           AND flv.enabled_flag = g_yes
           AND language = USERENV('LANG')
           AND flv.view_application_id =
               (SELECT application_id
                  FROM fnd_application
                 WHERE application_short_name = 'PO');

        pov_lookup_code := l_lookup_code;
      EXCEPTION
        WHEN OTHERS THEN
          l_err_count := l_err_count + 1;
          log_errors(pov_return_status       => l_log_ret_status -- OUT
                    ,
                     pov_error_msg           => l_log_err_msg -- OUT
                    ,
                     piv_source_column_name  => 'LEG_FOB_LOOKUP_CODE',
                     piv_source_column_value => piv_lookup_code,
                     piv_error_type          => g_err_val,
                     piv_error_code          => 'ETN_AP_INVALID_FOB_CODE',
                     piv_error_message       => 'Error : Lookup Code (' ||
                                                l_lookup_code ||
                                                ') setup issue for FOB Lookup in R12, Oracle Error is ' ||
                                                SQLERRM);
      END;
    END IF;

    IF l_err_count > 0 THEN
      pon_retcode := g_warning;
    END IF;
  EXCEPTION
    WHEN OTHERS THEN
      pon_retcode := g_ret_error;
      print_log_message(SUBSTR('Exception in Procedure validate_fob_code_supp. ' ||
                               SQLERRM,
                               1,
                               2000));
  END validate_fob_code_supp;

  --
  -- ========================
  -- Procedure: validate_freight_term_supp
  -- =============================================================================
  --   This procedure is used to
  -- =============================================================================
  --  Input Parameters :
  --    None
  --  Output Parameters :
  --    None
  -- -----------------------------------------------------------------------------
  --
  PROCEDURE validate_freight_term_supp(piv_lookup_code IN VARCHAR2,
                                       pov_lookup_code OUT VARCHAR2,
                                       pon_retcode     OUT NUMBER) IS
    l_freight_lkp CONSTANT VARCHAR2(30) := 'XXAP_FREIGHT_TERMS_LOOKUP';

    l_lookup_code fnd_lookup_values.lookup_code%TYPE;
    l_status      VARCHAR2(1);

    l_log_ret_status VARCHAR2(50);
    l_log_err_msg    VARCHAR2(2000);
    l_err_count      NUMBER := 0;
  BEGIN
    pon_retcode := g_normal;

    BEGIN
      SELECT upper( flv.description) -- 23 March 2016 DD
        INTO l_lookup_code
        FROM fnd_lookup_values flv
       WHERE flv.lookup_type = l_freight_lkp
         AND flv.meaning = piv_lookup_code
         AND flv.enabled_flag = g_yes
         AND SYSDATE BETWEEN NVL(flv.start_date_active, SYSDATE) AND
             NVL(flv.end_date_active, SYSDATE + 1)
         AND flv.language = USERENV('LANG');
    EXCEPTION
      WHEN OTHERS THEN
        l_err_count := l_err_count + 1;
        log_errors(pov_return_status       => l_log_ret_status -- OUT
                  ,
                   pov_error_msg           => l_log_err_msg -- OUT
                  ,
                   piv_source_column_name  => 'LEG_FREIGHT_TERMS_CODE',
                   piv_source_column_value => piv_lookup_code,
                   piv_error_type          => g_err_val,
                   piv_error_code          => 'ETN_AP_INVALID_FREIGHT_TERMS_CODE',
                   piv_error_message       => 'Error : Lookup Code ' ||
                                              piv_lookup_code ||
                                              ' for Freight Terms Lookup (' ||
                                              l_freight_lkp ||
                                              ') setup issue. Oracle Error is ' ||
                                              SQLERRM);
    END;

    IF l_err_count = 0 THEN
      BEGIN
        SELECT 'N'
          INTO l_status
          FROM po_lookup_codes plc
         WHERE plc.lookup_type = 'FREIGHT TERMS'
           AND UPPER(plc.lookup_code) = UPPER(l_lookup_code)
           AND SYSDATE <= NVL(plc.inactive_date, SYSDATE)
           AND plc.enabled_flag = g_yes
        --AND language = USERENV ('LANG')
        ;

        pov_lookup_code := l_lookup_code;
      EXCEPTION
        WHEN OTHERS THEN
          l_err_count := l_err_count + 1;
          log_errors(pov_return_status       => l_log_ret_status -- OUT
                    ,
                     pov_error_msg           => l_log_err_msg -- OUT
                    ,
                     piv_source_column_name  => 'LEG_FREIGHT_TERMS_CODE',
                     piv_source_column_value => piv_lookup_code,
                     piv_error_type          => g_err_val,
                     piv_error_code          => 'ETN_AP_INVALID_FREIGHT_TERMS_CODE',
                     piv_error_message       => 'Error : Lookup Code (' ||
                                                l_lookup_code ||
                                                ') setup issue for FREIGHT TERMS Lookup in R12, Oracle Error is ' ||
                                                SQLERRM);
      END;
    END IF;

    IF l_err_count > 0 THEN
      pon_retcode := g_warning;
    END IF;
  EXCEPTION
    WHEN OTHERS THEN
      pon_retcode := g_ret_error;
      print_log_message(SUBSTR('Exception in Procedure validate_freight_term_supp. ' ||
                               SQLERRM,
                               1,
                               2000));
  END validate_freight_term_supp;

  --
  -- ========================
  -- Procedure: validate_vendor_type_supp
  -- =============================================================================
  --   This procedure is used to
  -- =============================================================================
  --  Input Parameters :
  --    None
  --  Output Parameters :
  --    None
  -- -----------------------------------------------------------------------------
  --
  PROCEDURE validate_vendor_type_supp(piv_lookup_code IN VARCHAR2,
                                      pov_lookup_code OUT VARCHAR2,
                                      pon_retcode     OUT NUMBER) IS
    l_vendor_lkp CONSTANT VARCHAR2(30) := 'XXAP_VENDOR_TYPE_LOOKUP_CODE';

    l_lookup_code fnd_lookup_values.lookup_code%TYPE;
    l_status      VARCHAR2(1);

    l_log_ret_status VARCHAR2(50);
    l_log_err_msg    VARCHAR2(2000);
    l_err_count      NUMBER := 0;
  BEGIN
    pon_retcode := g_normal;

    BEGIN
      SELECT flv.description
        INTO l_lookup_code
        FROM fnd_lookup_values flv
       WHERE flv.lookup_type = l_vendor_lkp
         AND flv.meaning = piv_lookup_code
         AND flv.enabled_flag = g_yes
         AND SYSDATE BETWEEN NVL(flv.start_date_active, SYSDATE) AND
             NVL(flv.end_date_active, SYSDATE + 1)
         AND flv.language = USERENV('LANG');
    EXCEPTION
      WHEN OTHERS THEN
        l_err_count := l_err_count + 1;
        log_errors(pov_return_status       => l_log_ret_status -- OUT
                  ,
                   pov_error_msg           => l_log_err_msg -- OUT
                  ,
                   piv_source_column_name  => 'LEG_VENDOR_TYPE_LOOKUP_CODE',
                   piv_source_column_value => piv_lookup_code,
                   piv_error_type          => g_err_val,
                   piv_error_code          => 'ETN_AP_INVALID_VENDOR_TYPE',
                   piv_error_message       => 'Error : Lookup Code ' ||
                                              piv_lookup_code ||
                                              ' for Vendor Type Lookup (' ||
                                              l_vendor_lkp ||
                                              ') setup issue. Oracle Error is ' ||
                                              SQLERRM);
    END;

    IF l_err_count = 0 THEN
      BEGIN
        SELECT 'N'
          INTO l_status
          FROM fnd_lookup_values flv
         WHERE flv.lookup_type = 'VENDOR TYPE'
           AND flv.lookup_code = upper(l_lookup_code)
           AND flv.enabled_flag = g_yes
           AND SYSDATE BETWEEN NVL(flv.start_date_active, SYSDATE) AND
               NVL(flv.end_date_active, SYSDATE + 1)
           AND flv.language = USERENV('LANG');

        pov_lookup_code := l_lookup_code;
      EXCEPTION
        WHEN OTHERS THEN
          l_err_count := l_err_count + 1;
          log_errors(pov_return_status       => l_log_ret_status -- OUT
                    ,
                     pov_error_msg           => l_log_err_msg -- OUT
                    ,
                     piv_source_column_name  => 'VENDOR_TYPE_LOOKUP_CODE',
                     piv_source_column_value => l_lookup_code,
                     piv_error_type          => g_err_val,
                     piv_error_code          => 'ETN_AP_INVALID_VENDOR_TYPE',
                     piv_error_message       => 'Error : Derived value (' ||
                                                l_lookup_code ||
                                                ') from Custom Lookup do not exist in Lookup VENDOR TYPE or invalid in R12, Oracle Error is ' ||
                                                SQLERRM);
      END;
    END IF;

    IF l_err_count > 0 THEN
      pon_retcode := g_warning;
    END IF;
  EXCEPTION
    WHEN OTHERS THEN
      pon_retcode := g_ret_error;
      print_log_message(SUBSTR('Exception in Procedure validate_vendor_type_supp. ' ||
                               SQLERRM,
                               1,
                               2000));
  END validate_vendor_type_supp;

  --
  -- ========================
  -- Procedure: validate_awt_grp_supp
  -- =============================================================================
  --   This procedure is used to
  -- =============================================================================
  --  Input Parameters :
  --    None
  --  Output Parameters :
  --    None
  -- -----------------------------------------------------------------------------
  --
  PROCEDURE validate_awt_grp_supp(piv_group_name IN VARCHAR2,
                                  pon_group_id   OUT NUMBER,
                                  pov_group_name OUT VARCHAR2,
                                  pon_retcode    OUT NUMBER) IS
    l_log_ret_status VARCHAR2(50);
    l_log_err_msg    VARCHAR2(2000);
    l_group_name     VARCHAR2(2000);
  BEGIN
    pon_retcode := g_normal;
    -- v1.4  Added for localization
    BEGIN
      SELECT flvv.description
        INTO l_group_name
        FROM fnd_lookup_values_vl flvv
       WHERE flvv.lookup_type = 'ETN_WTHTAX_MAPPING'
         AND SYSDATE BETWEEN NVL(flvv.start_date_active, SYSDATE) AND
             NVL(flvv.end_date_active, SYSDATE)
         AND flvv.meaning = piv_group_name;
    EXCEPTION
      WHEN OTHERS THEN
        pon_retcode := g_warning;
        log_errors(pov_return_status       => l_log_ret_status -- OUT
                  ,
                   pov_error_msg           => l_log_err_msg -- OUT
                  ,
                   piv_source_column_name  => 'LEG_AWT_GROUP_NAME',
                   piv_source_column_value => piv_group_name,
                   piv_error_type          => g_err_val,
                   piv_error_code          => 'ETN_AP_INVALID_AWT_GROUP',
                   piv_error_message       => 'Error : AWT group is not defined in lookup ETN_WTHTAX_MAPPING, Oracle Error is ' ||
                                              SQLERRM);
    END;

    BEGIN
      SELECT group_id
        INTO pon_group_id
        FROM ap_awt_groups
       WHERE name = l_group_name
         AND (SYSDATE) <= NVL(inactive_date, SYSDATE);

      pov_group_name := UPPER(l_group_name);

    EXCEPTION
      WHEN OTHERS THEN
        pon_retcode := g_warning;
        log_errors(pov_return_status       => l_log_ret_status -- OUT
                  ,
                   pov_error_msg           => l_log_err_msg -- OUT
                  ,
                   piv_source_column_name  => 'LEG_AWT_GROUP_NAME',
                   piv_source_column_value => piv_group_name,
                   piv_error_type          => g_err_val,
                   piv_error_code          => 'ETN_AP_INVALID_AWT_GROUP',
                   piv_error_message       => 'Error : AWT group is not valid, Oracle Error is ' ||
                                              SQLERRM);
    END;

  EXCEPTION
    WHEN OTHERS THEN
      pon_retcode := g_ret_error;
      print_log_message(SUBSTR('Exception in Procedure validate_awt_grp_supp. ' ||
                               SQLERRM,
                               1,
                               2000));
  END validate_awt_grp_supp;

  --
  -- ========================
  -- Procedure: validate_coa_supp
  -- =============================================================================
  --   This procedure is used to
  -- =============================================================================
  --  Input Parameters :
  --    None
  --  Output Parameters :
  --    None
  -- -----------------------------------------------------------------------------
  --
  PROCEDURE validate_coa_supp(piv_segment1    IN VARCHAR2,
                              piv_segment2    IN VARCHAR2,
                              piv_segment3    IN VARCHAR2,
                              piv_segment4    IN VARCHAR2,
                              piv_segment5    IN VARCHAR2,
                              piv_segment6    IN VARCHAR2,
                              piv_segment7    IN VARCHAR2,
                              piv_column_name IN VARCHAR2,
                              pon_cc_id       OUT NUMBER,
                              pon_retcode     OUT NUMBER) IS
    l_log_ret_status VARCHAR2(50);
    l_log_err_msg    VARCHAR2(2000);

    l_11i_coa_rec xxetn_coa_mapping_pkg.g_coa_rec_type;
    l_r12_coa_rec xxetn_coa_mapping_pkg.g_coa_rec_type;
    l_coa_rec     xxetn_common_pkg.g_rec_type;
    l_msg         VARCHAR2(3000);
    l_status      VARCHAR2(50);

  BEGIN
    pon_retcode := g_normal;

    l_11i_coa_rec.segment1 := piv_segment1;
    l_11i_coa_rec.segment2 := piv_segment2;
    l_11i_coa_rec.segment3 := piv_segment3;
    l_11i_coa_rec.segment4 := piv_segment4;
    l_11i_coa_rec.segment5 := piv_segment5;
    l_11i_coa_rec.segment6 := piv_segment6;
    l_11i_coa_rec.segment7 := piv_segment7;

    xxetn_coa_mapping_pkg.get_code_combination(p_direction           => 'LEGACY-TO-R12',
                                               p_external_system     => NULL,
                                               p_transformation_date => TRUNC(SYSDATE),
                                               p_coa_input           => l_11i_coa_rec,
                                               p_coa_output          => l_r12_coa_rec,
                                               p_out_message         => l_msg,
                                               p_out_status          => l_status);

  /*  print_log_message('10 segments ' || l_r12_coa_rec.segment1 || '.' ||
                      l_r12_coa_rec.segment2 || '.' ||
                      l_r12_coa_rec.segment3 || '.' ||
                      l_r12_coa_rec.segment4 || '.' ||
                      l_r12_coa_rec.segment5 || '.' ||
                      l_r12_coa_rec.segment6 || '.' ||
                      l_r12_coa_rec.segment7 || '.' ||
                      l_r12_coa_rec.segment8 || '.' ||
                      l_r12_coa_rec.segment9 || '.' ||
                      l_r12_coa_rec.segment10 || ' / l_status ' ||
                      l_status);*/

    IF UPPER(l_status) = UPPER('Processed') THEN
      l_msg := NULL;

      l_coa_rec.segment1  := l_r12_coa_rec.segment1;
      l_coa_rec.segment2  := l_r12_coa_rec.segment2;
      l_coa_rec.segment3  := l_r12_coa_rec.segment3;
      l_coa_rec.segment4  := l_r12_coa_rec.segment4;
      l_coa_rec.segment5  := l_r12_coa_rec.segment5;
      l_coa_rec.segment6  := l_r12_coa_rec.segment6;
      l_coa_rec.segment7  := l_r12_coa_rec.segment7;
      l_coa_rec.segment8  := l_r12_coa_rec.segment8;
      l_coa_rec.segment9  := l_r12_coa_rec.segment9;
      l_coa_rec.segment10 := l_r12_coa_rec.segment10;

      xxetn_common_pkg.get_ccid(p_in_segments => l_coa_rec,
                                p_ccid        => pon_cc_id,
                                p_err_msg     => l_msg);

     /* print_log_message('ccid ' || pon_cc_id || ' / l_msg ' || l_msg);*/

      IF l_msg IS NOT NULL THEN
        pon_cc_id := NULL;
        log_errors(pov_return_status       => l_log_ret_status -- OUT
                  ,
                   pov_error_msg           => l_log_err_msg -- OUT
                  ,
                   piv_source_column_name  => UPPER(piv_column_name),
                   piv_source_column_value => (piv_segment1 || '.' ||
                                              piv_segment2 || '.' ||
                                              piv_segment3 || '.' ||
                                              piv_segment4 || '.' ||
                                              piv_segment5 || '.' ||
                                              piv_segment6 || '.' ||
                                              piv_segment7),
                   piv_error_type          => g_err_val,
                   piv_error_code          => 'ETN_CCID_DERIVATION_ERR',
                   piv_error_message       => 'Error : ' || l_msg);
        pon_retcode := g_warning;
      END IF;

    ELSIF UPPER(l_status) = UPPER('Error') THEN
      log_errors(pov_return_status       => l_log_ret_status -- OUT
                ,
                 pov_error_msg           => l_log_err_msg -- OUT
                ,
                 piv_source_column_name  => UPPER(piv_column_name),
                 piv_source_column_value => (piv_segment1 || '.' ||
                                            piv_segment2 || '.' ||
                                            piv_segment3 || '.' ||
                                            piv_segment4 || '.' ||
                                            piv_segment5 || '.' ||
                                            piv_segment6 || '.' ||
                                            piv_segment7),
                 piv_error_type          => g_err_val,
                 piv_error_code          => 'ETN_CCID_DERIVATION_ERR',
                 piv_error_message       => 'Error : ' || l_msg);
      pon_retcode := g_warning;
    END IF;

  EXCEPTION
    WHEN OTHERS THEN
      pon_retcode := g_ret_error;
      print_log_message(SUBSTR('Exception in Procedure validate_coa_supp. ' ||
                               SQLERRM,
                               1,
                               2000));
  END validate_coa_supp;

  --
  -- ========================
  -- Procedure: validate_loc_dtls_supp
  -- =============================================================================
  --   This procedure is used to
  -- =============================================================================
  --  Input Parameters :
  --    None
  --  Output Parameters :
  --    None
  -- -----------------------------------------------------------------------------
  --
  PROCEDURE validate_loc_dtls_supp(piv_ship_to_loc_code IN VARCHAR2,
                                   piv_bill_to_loc_code IN VARCHAR2,
                                   pon_ship_to_loc_id   OUT NUMBER,
                                   pon_bill_to_loc_id   OUT NUMBER,
                                   pon_retcode          OUT NUMBER) IS
    l_log_ret_status VARCHAR2(50);
    l_log_err_msg    VARCHAR2(2000);
    l_err_count      NUMBER := 0;
  BEGIN
    pon_retcode := g_normal;

    IF piv_ship_to_loc_code IS NOT NULL THEN

      BEGIN
        SELECT ship_to_location_id
          INTO pon_ship_to_loc_id
          FROM hr_locations
         WHERE ship_to_site_flag = g_yes
           AND location_code = piv_ship_to_loc_code
           AND SYSDATE <= NVL(inactive_date, SYSDATE);
      EXCEPTION
        WHEN OTHERS THEN
          l_err_count := l_err_count + 1;
          log_errors(pov_return_status       => l_log_ret_status -- OUT
                    ,
                     pov_error_msg           => l_log_err_msg -- OUT
                    ,
                     piv_source_column_name  => 'LEG_SHIP_TO_LOCATION_CODE',
                     piv_source_column_value => piv_ship_to_loc_code,
                     piv_error_type          => g_err_val,
                     piv_error_code          => 'ETN_AP_INVALID_SHIP_LOCATION_DETAILS',
                     piv_error_message       => 'Error while validating Vendor Ship-To Location Code in R12. Oracle Error is ' ||
                                                SQLERRM);
      END;

    END IF;

    IF piv_bill_to_loc_code IS NOT NULL THEN

      BEGIN
        SELECT location_id
          INTO pon_bill_to_loc_id
          FROM hr_locations
         WHERE bill_to_site_flag = g_yes
           AND location_code = piv_bill_to_loc_code
           AND SYSDATE <= NVL(inactive_date, SYSDATE);
      EXCEPTION
        WHEN OTHERS THEN
          l_err_count := l_err_count + 1;
          log_errors(pov_return_status       => l_log_ret_status -- OUT
                    ,
                     pov_error_msg           => l_log_err_msg -- OUT
                    ,
                     piv_source_column_name  => 'LEG_BILL_TO_LOCATION_CODE',
                     piv_source_column_value => piv_bill_to_loc_code,
                     piv_error_type          => g_err_val,
                     piv_error_code          => 'ETN_AP_INVALID_BILL_LOCATION_DETAILS',
                     piv_error_message       => 'Error while validating Vendor Bill-To Location Code in R12. Oracle Error is ' ||
                                                SQLERRM);
      END;

    END IF;

    IF l_err_count > 0 THEN
      pon_retcode := g_warning;
    END IF;
  EXCEPTION
    WHEN OTHERS THEN
      pon_retcode := g_ret_error;
      print_log_message(SUBSTR('Exception in Procedure validate_loc_dtls_supp. ' ||
                               SQLERRM,
                               1,
                               2000));
  END validate_loc_dtls_supp;

  --
  -- ========================
  -- Procedure: dup_val_chk_supp
  -- =============================================================================
  --   This procedure to do mandatory value check for Supplier entity
  -- =============================================================================
  --  Input Parameters :
  --    None
  --  Output Parameters :
  --    None
  -- -----------------------------------------------------------------------------
  --
  PROCEDURE dup_val_chk_supp(pon_retcode OUT NUMBER) IS
    l_log_ret_status VARCHAR2(50);
    l_log_err_msg    VARCHAR2(2000);

    --cursor to select new records from supplier staging table
    CURSOR supplier_cur IS
      SELECT xss.leg_segment1, COUNT(1)
        FROM xxap_suppliers_stg xss
       WHERE xss.process_flag = g_new
         AND xss.batch_id = g_new_batch_id
         AND xss.run_sequence_id = g_run_seq_id
       GROUP BY xss.leg_segment1
      HAVING COUNT(1) > 1;

    TYPE supplier_t IS TABLE OF supplier_cur%ROWTYPE INDEX BY BINARY_INTEGER;
    l_supplier_tbl supplier_t;
  BEGIN
    pon_retcode := g_normal;

    OPEN supplier_cur;
    LOOP
      FETCH supplier_cur BULK COLLECT
        INTO l_supplier_tbl LIMIT 1000;
      EXIT WHEN l_supplier_tbl.COUNT = 0;
      IF l_supplier_tbl.COUNT > 0 THEN
        FOR indx IN 1 .. l_supplier_tbl.COUNT LOOP
          --g_intf_staging_id := l_supplier_tbl(indx).interface_txn_id;
          log_errors(pov_return_status       => l_log_ret_status -- OUT
                    ,
                     pov_error_msg           => l_log_err_msg -- OUT
                    ,
                     piv_source_column_name  => 'LEG_SEGMENT1',
                     piv_source_column_value => l_supplier_tbl(indx)
                                               .leg_segment1,
                     piv_error_type          => g_err_val,
                     piv_error_code          => 'ETN_AP_DUPLICATE_SUPPLIER',
                     piv_error_message       => 'Error : LEG_SEGMENT1 should not be duplicate');
        END LOOP;
      END IF;
    END LOOP;
    CLOSE supplier_cur;

  EXCEPTION
    WHEN OTHERS THEN
      pon_retcode := g_ret_error;
      print_log_message(SUBSTR('Exception in Procedure dup_val_chk_supp. ' ||
                               SQLERRM,
                               1,
                               2000));
  END dup_val_chk_supp;

  /*
  -- ========================
  -- Procedure: mul_flag_upd_supp_site
  -- =============================================================================
  --   This procedure to do apply logic on primary pay site flag  reporting flag so
       Multiple tax reporting flag :only one site can be enabled for tax reporting within single OU.
       defect # 4701 -- •  Multiple tax reporting site within same OU
  -- =============================================================================
  --  Input Parameters :
  --    None
  --  Output Parameters :
  --    None
  -- -----------------------------------------------------------------------------
  */

PROCEDURE mul_flag_upd_supp_site(pon_retcode OUT NUMBER) IS
    l_err_code CONSTANT VARCHAR2(28) := 'ETN_AP_DUPLICATE_TAX_RS';
    l_log_ret_status VARCHAR2(50);
    l_log_err_msg    VARCHAR2(2000);
  BEGIN


 Update xxconv.XXAP_SUPPLIER_SITES_STG S
   Set leg_primary_pay_site_flag = 'Y'
 Where rowid =
       (select min(rowid)
          from xxconv.XXAP_SUPPLIER_SITES_STG X
         where X.leg_primary_pay_site_flag = 'Y'
           and S.leg_vendor_id = X.leg_vendor_id
           and S.leg_vendor_site_id = X.leg_vendor_site_id
           and S.org_id = X.org_id
           and S.leg_vendor_site_code = X.leg_vendor_site_code);

/* Old 30 March 2016
Update xxconv.XXAP_SUPPLIER_SITES_STG S
    Set leg_tax_reporting_site_flag = 'Y'
  Where rowid = (select min(rowid)
                   from xxconv.XXAP_SUPPLIER_SITES_STG X
                  where X.leg_primary_pay_site_flag = 'Y'
                    and S.leg_vendor_id = X.leg_vendor_id
                    and S.org_id = X.org_id)
    and S.leg_primary_pay_site_flag = 'Y';*/

 Update xxconv.XXAP_SUPPLIER_SITES_STG S
    Set leg_tax_reporting_site_flag = 'Y'
  Where rowid =
        (select min(rowid)
           from xxconv.XXAP_SUPPLIER_SITES_STG X
          where 1 = 1
            and X.leg_primary_pay_site_flag = 'Y'
            and S.leg_vendor_id = X.leg_vendor_id
            and S.org_id = X.org_id
            and exists (Select 1
                   from xxconv.XXAP_SUPPLIER_SITES_STG Y
                  where 1 = 1
                    and Y.leg_tax_reporting_site_flag = 'Y'
                    and Y.leg_vendor_id = X.leg_vendor_id
                    and Y.org_id = X.org_id))
    and S.leg_primary_pay_site_flag = 'Y';

Update xxconv.XXAP_SUPPLIER_SITES_STG S
   Set leg_primary_pay_site_flag = NULL, leg_tax_reporting_site_flag = NULL
 Where rowid not in (select min(rowid)
                       from xxconv.XXAP_SUPPLIER_SITES_STG X
                      where X.leg_primary_pay_site_flag = 'Y'
                        and S.org_id = X.org_id
                        and S.leg_vendor_id = X.leg_vendor_id);

Update xxconv.XXAP_SUPPLIER_SITES_STG S
   Set leg_tax_reporting_site_flag = NUll
 Where rowid not in (select min(rowid)
                       from xxconv.XXAP_SUPPLIER_SITES_STG X
                      where X.leg_tax_reporting_site_flag = 'Y'
                        and S.org_id = X.org_id
                        and S.leg_vendor_id = X.leg_vendor_id);

Commit;

 pon_retcode := g_normal;
  EXCEPTION
    WHEN OTHERS THEN
      pon_retcode := g_ret_error;
      print_log_message(SUBSTR('Exception in Procedure mul_flag_upd_supp_site. ' ||
                               SQLERRM,
                               1,
                               2000));
  END mul_flag_upd_supp_site;



  /*
  -- ========================
  -- Procedure: ECE_Code_upd_supp_site
  -- =============================================================================
  --   This procedure to do apply logic on ECE CODE  so
       ECE codes cannot be shared across sites within one OU .
       This is happening as multiple 11i OU are converging into single R12 OU.
       defect # 4701 -- •  Change :Process first record and clear code on remaining sites within single OU
  -- =============================================================================
  --  Input Parameters :
  --    None
  --  Output Parameters :
  --    None
  -- -----------------------------------------------------------------------------
  */

PROCEDURE ECE_Code_upd_supp_site(pon_retcode OUT NUMBER) IS
    l_log_ret_status VARCHAR2(50);
    l_log_err_msg    VARCHAR2(2000);
    l_column_name  VARCHAR2(2000);
    l_column_value VARCHAR2(2000);
    l_err_code VARCHAR2(2000);
    l_err_msg  VARCHAR2(2000);
  BEGIN

  Update xxconv.XXAP_SUPPLIER_SITES_STG
    Set leg_ece_tp_location_code = NULL
  Where leg_ece_tp_location_code is not null
    and rowid in (Select rowid
                    from xxconv.XXAP_SUPPLIER_SITES_STG S
                   where 1 = 1
                     and S.org_id = org_id
                     and S.leg_vendor_id = leg_vendor_id
                     and S.leg_vendor_site_code = leg_vendor_site_code
                     and S.vendor_site_code = vendor_site_code
                     and S.leg_ece_tp_location_code is not null
                     and rowid not in
                         (Select min(rowid)
                            from xxconv.XXAP_SUPPLIER_SITES_STG X
                           Where X.leg_ece_tp_location_code is not null
                             and S.org_id = X.org_id
                             and x.process_flag <> 'D'  --v4.0 without this condition D record might get saved and actual record will have the code cleared
                             and S.leg_vendor_id = X.leg_vendor_id)
                   group by rowid,
                            S.leg_vendor_id,
                            S.leg_ece_tp_location_code,
                            S.leg_vendor_site_code,
                            S.org_id,
                            S.operating_unit_name);
  Commit;
   pon_retcode := g_normal;
  EXCEPTION
    WHEN OTHERS THEN
            l_column_name  := 'LEG_ECE_TP_LOCATION_CODE';
            l_column_value := 'LEG_ECE_TP_LOCATION_CODE';
            l_err_code := 'ETN_AP_INVALID_VAT_CODE';
            l_err_msg  := 'Error while update ECE TP location code ' || SUBSTR(SQLERRM, 1, 150);
      pon_retcode := g_ret_error;
      print_log_message(SUBSTR('Exception in Procedure ECE_Code_upd_supp_site. ' ||SQLERRM,1,2000));
      log_errors(pov_return_status       => l_log_ret_status -- OUT
                  ,pov_error_msg           => l_log_err_msg -- OUT
                  ,piv_source_column_name  => l_column_name,
                   piv_source_column_value => l_column_value,
                   piv_error_type          => g_err_val,
                   piv_error_code          => l_err_code,
                   piv_error_message       => l_err_msg);
  END ECE_Code_upd_supp_site;




/*
  -- ========================
  -- Procedure: Check_Valid_Location
  -- =============================================================================
  --   This procedure check address ( location id) already exists in R12,If exists the return TRUE else FALSE.
  -- =============================================================================
  --  Input Parameters :
  --    None
  --  Output Parameters :
  --    None
  -- -----------------------------------------------------------------------------
  */

 PROCEDURE Check_Valid_Location(
                                p_address_line1     IN    VARCHAR2,
                                p_address_line2     IN    VARCHAR2,
                                p_address_line3     IN    VARCHAR2,
                                p_address_line4     IN    VARCHAR2,
                                p_city              IN    VARCHAR2,
                                p_state             IN    VARCHAR2,
                                p_zip               IN    VARCHAR2,
                                p_province          IN    VARCHAR2,
                                p_country           IN    VARCHAR2,
                                p_county            IN    VARCHAR2,
                                p_language          IN    VARCHAR2,
                                p_address_style     IN    VARCHAR2,
                                p_vendor_id         IN    NUMBER,
                                x_location_id       OUT NOCOPY NUMBER,
                                x_valid             OUT NOCOPY BOOLEAN,
                                x_loc_count         OUT NOCOPY NUMBER
                                      ) IS

    p_party_site_id NUMBER;
 BEGIN
    x_valid    := TRUE;
    x_loc_count := 0;

  SELECT MAX(hl.location_id)
        INTO x_location_id
        FROM HZ_Locations hl,
             HZ_Party_Sites hps,
             po_vendors pv,
             fnd_languages fl
        WHERE nvl(upper(hl.country), 'dummy')       = nvl(upper(p_country), 'dummy')         AND
              nvl(upper(hl.address1), 'dummy')      = nvl(upper(p_address_line1), 'dummy')   AND
              nvl(upper(hl.address2), 'dummy')      = nvl(upper(p_address_line2), 'dummy')   AND
              nvl(upper(hl.address3), 'dummy')      = nvl(upper(p_address_line3), 'dummy')   AND
              nvl(upper(hl.address4), 'dummy')      = nvl(upper(p_address_line4), 'dummy')   AND
              nvl(upper(hl.city), 'dummy')          = nvl(upper(p_city), 'dummy')            AND
              nvl(upper(hl.state), 'dummy')         = nvl(upper(p_state), 'dummy')           AND
              nvl(upper(hl.postal_code), 'dummy')   = nvl(upper(p_zip), 'dummy')             AND
              nvl(upper(hl.province), 'dummy')      = nvl(upper(p_province), 'dummy')        AND
              nvl(upper(hl.county), 'dummy')        = nvl(upper(p_county), 'dummy')          AND
              nvl(upper(fl.nls_language), 'dummy')  = nvl(upper(p_language), 'dummy')        AND
              nvl(upper(hl.address_style), 'dummy') = nvl(upper(p_address_style), 'dummy')   AND
              hl.location_id                        = hps.location_id                        AND
              hps.party_id                          = pv.party_id                            AND
              pv.vendor_id                          = p_vendor_id                            AND
              hl.language                           = fl.language_code(+)                    AND
              hps.status                            = 'A'                                    AND
              hps.end_date_active is NULL;

        SELECT hps.party_site_id
        INTO p_party_site_id
        FROM HZ_Party_Sites hps
        WHERE hps.location_id = x_location_id
        AND ROWNUM = 1;

     IF x_location_id IS NULL THEN
        x_valid := FALSE;
     END IF;

    EXCEPTION
    -- Trap validation error
      WHEN NO_DATA_FOUND THEN
         x_valid    := FALSE;
      -- Bug 7429668 Trap validation error when more than 1 row is found
      WHEN OTHERS THEN
            x_valid    := FALSE;
            x_loc_count := 2;
            print_log_message('In Exception Check_Valid_Location' || SQLERRM);
  END Check_Valid_Location;




/*
  -- ========================
  -- Procedure: Supp_Party_Site_API
  -- =============================================================================
  --   This procedure create address ( location id) and party site id for all supplier sites.
       same vendor site code cannot exists across two different address
  -- =============================================================================
  --  Input Parameters :
  --    None
  --  Output Parameters :
  --    None
  -- -----------------------------------------------------------------------------
  */


PROCEDURE Supp_Party_Site_API(pov_errbuf  OUT NOCOPY VARCHAR2,
                              pon_retcode OUT NOCOPY NUMBER) IS


 p_location_rec   HZ_LOCATION_V2PUB.LOCATION_REC_TYPE;
 x_location_id    NUMBER;
 x_return_status  VARCHAR2(2000);
 x_msg_count      NUMBER;
 x_msg_data       VARCHAR2(2000);
 l_error_message      VARCHAR2(2000);
 l_location_id      NUMBER;
 x_valid   boolean;
 x_loc_count   NUMBER;

 CURSOR cur_site_address IS
 SELECT * from xxap_supplier_sites_stg api
 where process_flag = 'V'
 and vendor_id is not null
 AND NOT EXISTS (SELECT 1 FROM ap_suppliers ap
                 where ap.vendor_id = api.vendor_id
                 and ap.vendor_type_lookup_code = 'EMPLOYEE');


--For party site
  p_party_site_rec    HZ_PARTY_SITE_V2PUB.PARTY_SITE_REC_TYPE;
  x_party_site_id     NUMBER;
  x_party_site_number VARCHAR2(2000);
  x_return_status_ps     VARCHAR2(2000);
  x_msg_count_ps         NUMBER;
  x_msg_data_ps          VARCHAR2(2000);
  l_party_id NUMBER;
  l_party_Site_count NUMBER;
  l_lng    fnd_languages.language_Code%TYPE;
  l_Address_style hz_locations.ADDRESS_STYLE%TYPE;
BEGIN

FOR rec_add_rec IN cur_site_address
  LOOP
  --v3.0 to fix the language issue where code was not populated and language was going
   l_lng :=NULL;

    IF rec_add_rec.leg_language IS NOT NULL
      THEN
         BEGIN
            SELECT language_code
            INTO l_lng
            FROM fnd_languages
            WHERE nls_language = rec_add_rec.leg_language;

         EXCEPTION
            when others then
              l_lng := NULL;
              UPDATE xxap_supplier_Sites_stg
              SET leg_language = null
              WHERE interface_txn_id = rec_add_rec.interface_txn_id;
              COMMIT;

         END;
    END IF;

    --v3.0 change to fix address style issue
    l_Address_style := NULL;
    IF rec_add_rec.leg_address_style IS NOT NULL
      AND rec_add_rec.leg_address_style = 'GB'
    THEN
       l_Address_style := 'UAA';
    ELSE
       l_Address_style := rec_add_rec.leg_address_style;
    END IF;
    --v3.0 ends
   print_log_message('Check_Valid_Location : ' );
    Check_Valid_Location(
                p_address_line1  => trim(rec_add_rec.leg_address_line1),
                p_address_line2  => rec_add_rec.leg_address_line2,
                p_address_line3  => rec_add_rec.leg_address_line3,
                p_address_line4  => rec_add_rec.leg_address_line4,
                p_city           => trim(rec_add_rec.leg_city)       ,
                p_state          => trim(rec_add_rec.leg_state)    ,
                p_zip            => rec_add_rec.leg_zip ,
                p_province       => rec_add_rec.leg_province  ,
                p_country        => rec_add_rec.leg_country,
                p_county         => rec_add_rec.leg_county,
                p_language       => l_lng,
                p_address_style  => l_Address_style  ,
                p_vendor_id      => rec_add_rec.vendor_id,
                x_location_id    => l_location_id,
                x_valid          => x_valid,
                x_loc_count      => x_loc_count);

/*  If address is not exists in R12 then only create new address and party site id */
   IF NOT x_valid THEN
     p_location_rec.country           := trim(rec_add_rec.leg_country);
     p_location_rec.county            := trim(rec_add_rec.leg_county);
     p_location_rec.address1          := trim(rec_add_rec.leg_address_line1);
     p_location_rec.address2          := trim(rec_add_rec.leg_address_line2);
     p_location_rec.address3          := trim(rec_add_rec.leg_address_line3);
     p_location_rec.address4          := trim(rec_add_rec.leg_address_line4);
     p_location_rec.city              := trim(rec_add_rec.leg_city);
     p_location_rec.postal_code       := trim(rec_add_rec.leg_zip);
     p_location_rec.state             := trim(rec_add_rec.leg_state);
     p_location_rec.created_by_module := 'AP_SUPPLIERS_API';
     p_location_rec.province          := trim(rec_add_rec.leg_province);
     p_location_rec.address_style     := l_Address_style;
     p_location_rec.language          := l_lng;




     print_log_message('Calling create_location for '|| 'for vendor id ' ||rec_add_rec.vendor_id
       || ' vendor_site_code ' ||rec_add_rec.vendor_site_code);

     HZ_LOCATION_V2PUB.CREATE_LOCATION
               (
                 p_init_msg_list => FND_API.G_TRUE,
                 p_location_rec  => p_location_rec,
                 x_location_id   => x_location_id,
                 x_return_status => x_return_status,
                 x_msg_count     => x_msg_count,
                 x_msg_data      => x_msg_data);

    IF x_return_status = fnd_api.g_ret_sts_success THEN
       COMMIT;
       print_log_message('Creation of Location is Successful ');
       print_log_message('Output information ....');
       print_log_message('x_location_id: '||x_location_id );
       print_log_message('x_return_status: '||x_return_status);
       print_log_message('x_msg_count: '||x_msg_count);
       print_log_message('x_msg_data: '||x_msg_data);
    ELSE
       print_log_message ('Creation of Location failed:'||x_msg_data);
       ROLLBACK;
       FOR i IN 1 .. x_msg_count
       LOOP
          x_msg_data := oe_msg_pub.get( p_msg_index => i, p_encoded => 'F');
          print_log_message( i|| ') '|| x_msg_data);
       END LOOP;
    END IF;

     --nullify all values
     p_location_rec.country           := null;
     p_location_rec.county            := null;
     p_location_rec.address1          := null;
     p_location_rec.address2          := null;
     p_location_rec.address3          := null;
     p_location_rec.address4          := null;
     p_location_rec.city              := null;
     p_location_rec.postal_code       := null;
     p_location_rec.state             := null;
     p_location_rec.province          := null;
     p_location_rec.address_style     := null;
    p_location_rec.language            :=null;

    BEGIN
    --get party id for the supplier
       select party_id
       INTO l_party_id
       from ap_suppliers
       where vendor_id = rec_add_rec.vendor_id;

    END;

    p_party_site_rec.party_id                   := l_party_id;
    p_party_site_rec.location_id                 := x_location_id;
    p_party_site_rec.identifying_address_flag   := 'Y';
    p_party_site_rec.created_by_module          := 'AP_SUPPLIERS_API';
    p_party_site_rec.party_site_name             := rec_add_rec.vendor_site_code;

   --added on july20-to fix issue with duplicate party site
   BEGIN
      SELECT count(1)
      INTO l_party_Site_count
      FROM hz_party_sites
      where party_site_name = rec_add_rec.vendor_site_code
      AND party_id = l_party_id;

      IF l_party_Site_count >0
      THEN
         p_party_site_rec.party_site_name := rec_add_rec.vendor_site_code||'-'||rec_add_rec.leg_vendor_site_id;
      END IF;

   END;
   --change ends here

    print_log_message('Calling the API hz_party_site_v2pub.create_party_site');

    HZ_PARTY_SITE_V2PUB.CREATE_PARTY_SITE
                      (
                       p_init_msg_list     => FND_API.G_TRUE,
                       p_party_site_rec    => p_party_site_rec,
                       x_party_site_id     => x_party_site_id,
                       x_party_site_number => x_party_site_number,
                       x_return_status     => x_return_status_ps,
                       x_msg_count         => x_msg_count_ps,
                       x_msg_data          => x_msg_data_ps
                           );

     IF x_return_status_ps = fnd_api.g_ret_sts_success THEN
        COMMIT;
        print_log_message('Creation of Party Site is Successful Party Site Id = '||x_party_site_id);
     ELSE
      print_log_message ('Creation of Party Site failed:'||x_msg_data_ps);
        ROLLBACK;
        FOR i IN 1 .. x_msg_count_ps
        LOOP
           x_msg_data_ps := fnd_msg_pub.get( p_msg_index => i, p_encoded => 'F');
           print_log_message( i|| ') '|| x_msg_data_ps);
        END LOOP;
     END IF;

     --nullify all variables
      p_party_site_rec.party_id                 := null;
      p_party_site_rec.location_id              := null;
     END IF;
    END LOOP;

 EXCEPTION
    WHEN OTHERS THEN
      l_error_message := SUBSTR('Exception in Procedure Dup_SupplierSite_Block. ' ||
                                SQLERRM,
                                1,
                                1999);
      pon_retcode     := 2;
      pov_errbuf      := l_error_message;
      fnd_file.put_line(fnd_file.LOG, l_error_message);
End Supp_Party_Site_API;


  --
  -- ========================
  -- Procedure: req_val_chk_supp
  -- =============================================================================
  --   This procedure to do mandatory value check for Supplier entity
  -- =============================================================================
  --  Input Parameters :
  --    None
  --  Output Parameters :
  --    None
  -- -----------------------------------------------------------------------------
  --
  PROCEDURE req_val_chk_supp(pon_retcode OUT NUMBER) IS
    l_err_code CONSTANT VARCHAR2(28) := 'ETN_AP_MANDATORY_NOT_ENTERED';

    l_log_ret_status VARCHAR2(50);
    l_log_err_msg    VARCHAR2(2000);

    --cursor to select new records from supplier bank staging table
    CURSOR supplier_cur IS
      SELECT *
        FROM xxap_suppliers_stg xss
       WHERE xss.process_flag = g_new
         AND xss.batch_id = g_new_batch_id
         AND xss.run_sequence_id = g_run_seq_id
         AND (leg_vendor_name IS NULL OR
             leg_vendor_type_lookup_code IS NULL
             --OR leg_bill_to_location_code   IS NULL
             --OR leg_ship_to_location_code   IS NULL
             --OR leg_terms_name              IS NULL
             --OR leg_start_date_active       IS NULL
             );

    TYPE supplier_t IS TABLE OF supplier_cur%ROWTYPE INDEX BY BINARY_INTEGER;
    l_supplier_tbl supplier_t;
  BEGIN
    pon_retcode := g_normal;
    -- Initialize global variables for log_errors
    g_source_table    := g_supplier_t;
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

    OPEN supplier_cur;
    LOOP
      FETCH supplier_cur BULK COLLECT
        INTO l_supplier_tbl LIMIT 1000;
      EXIT WHEN l_supplier_tbl.COUNT = 0;
      IF l_supplier_tbl.COUNT > 0 THEN
        FOR indx IN 1 .. l_supplier_tbl.COUNT LOOP
          g_intf_staging_id := l_supplier_tbl(indx).interface_txn_id;

          IF l_supplier_tbl(indx).leg_vendor_name IS NULL THEN
              l_supplier_tbl(indx).process_flag := g_error;
            log_errors(pov_return_status       => l_log_ret_status -- OUT
                      ,
                       pov_error_msg           => l_log_err_msg -- OUT
                      ,
                       piv_source_column_name  => 'LEG_VENDOR_NAME',
                       piv_source_column_value => l_supplier_tbl(indx)
                                                 .leg_vendor_name,
                       piv_error_type          => g_err_val,
                       piv_error_code          => l_err_code,
                       piv_error_message       => 'Error : LEG_VENDOR_NAME should not be NULL');
          END IF;

          IF l_supplier_tbl(indx).leg_vendor_type_lookup_code IS NULL THEN
              l_supplier_tbl(indx).process_flag := g_error;
            log_errors(pov_return_status       => l_log_ret_status -- OUT
                      ,
                       pov_error_msg           => l_log_err_msg -- OUT
                      ,
                       piv_source_column_name  => 'LEG_VENDOR_TYPE_LOOKUP_CODE',
                       piv_source_column_value => l_supplier_tbl(indx)
                                                 .leg_vendor_type_lookup_code,
                       piv_error_type          => g_err_val,
                       piv_error_code          => l_err_code,
                       piv_error_message       => 'Error : LEG_VENDOR_TYPE_LOOKUP_CODE should not be NULL');
          END IF;
        END LOOP;


        BEGIN
          FORALL indx IN 1 .. l_supplier_tbl.COUNT SAVE EXCEPTIONS
            UPDATE xxap_suppliers_stg xss
               SET xss.last_updated_date             = SYSDATE,
                   xss.error_type                    = DECODE(l_supplier_tbl(indx)
                                                              .process_flag,
                                                              g_validated,
                                                              NULL,
                                                              g_error,
                                                              g_err_val),
                   xss.process_flag                  = l_supplier_tbl(indx)
                                                      .process_flag
             WHERE xss.interface_txn_id = l_supplier_tbl(indx).interface_txn_id;
        EXCEPTION
          WHEN OTHERS THEN
            print_log_message(SUBSTR('Exception in Procedure req_val_chk_supp while doing Bulk Insert. ' ||
                                     SQLERRM,
                                     1,
                                     2000));
            print_log_message('No. of records in Bulk Exception : ' ||
                              SQL%BULK_EXCEPTIONS.COUNT);
        END;
      END IF;
    END LOOP;
    CLOSE supplier_cur;

  EXCEPTION
    WHEN OTHERS THEN
      pon_retcode := g_ret_error;
      print_log_message(SUBSTR('Exception in Procedure req_val_chk_supp. ' ||
                               SQLERRM,
                               1,
                               2000));
  END req_val_chk_supp;

  --
  -- ========================
  -- Procedure: create_supplier_contacts
  -- =============================================================================
  --   This procedure is used to insert validated supplier contacts entity records
  --   into supplier site contacts interface table
  -- =============================================================================
  --  Input Parameters :
  --    None
  --  Output Parameters :
  --    None
  -- -----------------------------------------------------------------------------
  --
  PROCEDURE create_supplier_contacts(pon_retcode OUT NUMBER) IS
    l_retcode NUMBER;

    l_log_ret_status VARCHAR2(50);
    l_log_err_msg    VARCHAR2(2000);

    CURSOR supplier_contact_cur IS
      SELECT xscs.*
        FROM xxap_supplier_contacts_stg xscs
       WHERE xscs.process_flag = g_validated
         AND xscs.batch_id = g_new_batch_id;

    TYPE supplier_contact_t IS TABLE OF supplier_contact_cur%ROWTYPE INDEX BY BINARY_INTEGER;
    l_supplier_contact_tbl supplier_contact_t;
  BEGIN
    pon_retcode := g_normal;

    -- Initialize global variables for log_errors
    g_source_table    := g_supplier_contacts_t;
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

    OPEN supplier_contact_cur;
    LOOP
      FETCH supplier_contact_cur BULK COLLECT
        INTO l_supplier_contact_tbl LIMIT 1000;
      EXIT WHEN l_supplier_contact_tbl.COUNT = 0;
      IF l_supplier_contact_tbl.COUNT > 0 THEN
        BEGIN
          FORALL indx IN 1 .. l_supplier_contact_tbl.COUNT SAVE EXCEPTIONS
            INSERT INTO ap_sup_site_contact_int
              (last_update_date,
               last_updated_by,
               vendor_site_id,
               vendor_site_code,
               org_id,
               operating_unit_name,
               last_update_login,
               creation_date,
               created_by,
               first_name,
               middle_name,
               last_name,
               prefix,
               title,
               mail_stop,
               area_code,
               phone,
               contact_name_alt,
               first_name_alt,
               last_name_alt,
               department,
               url,
               alt_area_code,
               alt_phone,
               fax_area_code,
               status,
               email_address,
               fax,
               vendor_interface_id,
               vendor_id,
               vendor_contact_interface_id)
            VALUES
              (SYSDATE --last_update_date
              ,
               g_user_id --last_updated_by
              ,
               l_supplier_contact_tbl(indx).vendor_site_id --vendor_site_id
              ,
               l_supplier_contact_tbl(indx).vendor_site_code --vendor_site_code
              ,
               l_supplier_contact_tbl(indx).org_id --org_id
              ,
               l_supplier_contact_tbl(indx).operating_unit_name --operating_unit_name
              ,
               g_login_id --last_update_login
              ,
               SYSDATE --creation_date
              ,
               g_user_id --created_by
              ,
               l_supplier_contact_tbl(indx).leg_first_name --first_name
              ,
               l_supplier_contact_tbl(indx).leg_middle_name --middle_name
              ,
               l_supplier_contact_tbl(indx).leg_last_name --last_name
              ,
               l_supplier_contact_tbl(indx).leg_prefix --prefix
              ,
               l_supplier_contact_tbl(indx).leg_title --title
              ,
               l_supplier_contact_tbl(indx).leg_mail_stop --mail_stop
              ,
               l_supplier_contact_tbl(indx).leg_area_code --area_code
              ,
               l_supplier_contact_tbl(indx).phone --phone
              ,
               substr(l_supplier_contact_tbl(indx).leg_contact_name_alt,1,50) --contact_name_alt
              ,
               substr(l_supplier_contact_tbl(indx).leg_first_name_alt,1,50) --first_name_alt
              ,
               substr(l_supplier_contact_tbl(indx).leg_last_name_alt,1,50) --last_name_alt
              ,
               l_supplier_contact_tbl(indx).leg_department --department
              ,
               l_supplier_contact_tbl(indx).leg_url --url
              ,
               l_supplier_contact_tbl(indx).leg_alt_area_code --alt_area_code
              ,
               l_supplier_contact_tbl(indx).leg_alt_phone --alt_phone
              ,
               l_supplier_contact_tbl(indx).leg_fax_area_code --fax_area_code
              ,
               'NEW' --status
              ,
               l_supplier_contact_tbl(indx).leg_email_address --email_address
              ,
               l_supplier_contact_tbl(indx).fax --fax
              ,
               l_supplier_contact_tbl(indx).vendor_interface_id --vendor_interface_id
              ,
               l_supplier_contact_tbl(indx).vendor_id --vendor_id
              ,
               l_supplier_contact_tbl(indx).vendor_contact_interface_id --vendor_contact_interface_id
               );

        EXCEPTION
          WHEN OTHERS THEN
            print_log_message(SUBSTR('Exception in Procedure create_supplier_contacts while doing Bulk Insert. ' ||
                                     SQLERRM,
                                     1,
                                     2000));
            print_log_message('No. of records in Bulk Exception : ' ||
                              SQL%BULK_EXCEPTIONS.COUNT);
        END;
      END IF;
      COMMIT;
    END LOOP;
    CLOSE supplier_contact_cur;

    COMMIT;

    -- Update Successfully Interfaced Contacts
    BEGIN
      UPDATE xxap_supplier_contacts_stg xscs
         SET xscs.process_flag           = g_processed,
             xscs.last_updated_date      = SYSDATE,
             xscs.last_updated_by        = g_user_id,
             xscs.last_update_login      = g_login_id,
             xscs.program_application_id = g_prog_appl_id,
             xscs.program_id             = g_conc_program_id,
             xscs.program_update_date    = SYSDATE,
             xscs.request_id             = g_request_id,
             xscs.batch_id               = g_new_batch_id,
             xscs.run_sequence_id        = g_run_seq_id
       WHERE xscs.process_flag = g_validated
         AND xscs.batch_id = g_new_batch_id
         AND EXISTS (SELECT 1
                FROM ap_sup_site_contact_int assci
               WHERE assci.vendor_contact_interface_id =
                     xscs.vendor_contact_interface_id
                 AND assci.status = 'NEW');
      print_log_message('No. of records interfaced to ap_sup_site_contact_int table ' ||
                        SQL%ROWCOUNT);
    EXCEPTION
      WHEN OTHERS THEN
        print_log_message('Exception occured while updating staging table for records interfaced to ap_sup_site_contact_int table. Oracle error is ' ||
                          SQLERRM);
    END;

    COMMIT;

    -- Update Unsuccessful Contacts
    BEGIN
      UPDATE xxap_supplier_contacts_stg xscs
         SET xscs.process_flag           = g_error,
             xscs.error_type             = g_err_int,
             xscs.last_updated_date      = SYSDATE,
             xscs.last_updated_by        = g_user_id,
             xscs.last_update_login      = g_login_id,
             xscs.program_application_id = g_prog_appl_id,
             xscs.program_id             = g_conc_program_id,
             xscs.program_update_date    = SYSDATE,
             xscs.request_id             = g_request_id,
             xscs.batch_id               = g_new_batch_id,
             xscs.run_sequence_id        = g_run_seq_id
       WHERE xscs.process_flag = g_validated
         AND xscs.batch_id = g_new_batch_id
         AND NOT EXISTS (SELECT 1
                FROM ap_sup_site_contact_int assci
               WHERE assci.vendor_contact_interface_id =
                     xscs.vendor_contact_interface_id
                 AND assci.status = 'NEW');
      print_log_message('No. of records interfaced to ap_sup_site_contact_int table ' ||
                        SQL%ROWCOUNT);
      COMMIT;

      FOR indx IN (SELECT xscs.*
                     FROM xxap_supplier_contacts_stg xscs
                    WHERE xscs.process_flag = g_error
                      AND xscs.request_id = g_request_id
                      AND NOT EXISTS
                    (SELECT 1
                             FROM ap_sup_site_contact_int assci
                            WHERE assci.vendor_contact_interface_id =
                                  xscs.vendor_contact_interface_id
                              AND assci.status = 'NEW')) LOOP
        g_intf_staging_id := indx.interface_txn_id;
        log_errors(pov_return_status       => l_log_ret_status -- OUT
                  ,
                   pov_error_msg           => l_log_err_msg -- OUT
                  ,
                   piv_source_column_name  => 'LEG_VENDOR_SITE_CODE',
                   piv_source_column_value => indx.leg_vendor_site_code,
                   piv_error_type          => g_err_int,
                   piv_error_code          => 'ETN_SUPP_CONT_INTF_ERR',
                   piv_error_message       => 'Error : Record could not be interfaced to ap_sup_site_contact_int table. interface_txn_id=' ||
                                              indx.interface_txn_id);
      END LOOP;

    EXCEPTION
      WHEN OTHERS THEN
        print_log_message('Exception occured while updating staging table for records interfaced to ap_sup_site_contact_int table. Oracle error is ' ||
                          SQLERRM);
    END;

  EXCEPTION
    WHEN OTHERS THEN
      pon_retcode := g_ret_error;
      print_log_message(SUBSTR('Exception in Procedure create_supplier_contacts. ' ||
                               SQLERRM,
                               1,
                               2000));
  END create_supplier_contacts;

  --
  -- ========================
  -- Procedure: create_supplier_sites
  -- =============================================================================
  --   This procedure is used to insert validated supplier Site entity records
  --   into supplier site interface table
  -- =============================================================================
  --  Input Parameters :
  --    None
  --  Output Parameters :
  --    None
  -- -----------------------------------------------------------------------------
  --
  PROCEDURE create_supplier_sites(pon_retcode OUT NUMBER) IS
    l_retcode NUMBER;
    l_log_ret_status VARCHAR2(50);
    l_log_err_msg    VARCHAR2(2000);
    l_site_count     NUMBER ;
    l_new_count      NUMBER ;
    CURSOR supplier_site_cur IS
      SELECT xsss.*
        FROM xxap_supplier_sites_stg xsss
       WHERE xsss.process_flag = g_validated
         AND xsss.batch_id = g_new_batch_id;

    CURSOR supplier_site_spain IS
      SELECT DISTINCT xsss.leg_vendor_id, operating_unit_name
        FROM xxap_supplier_sites_stg xsss
       WHERE xsss.process_flag = g_validated
         AND leg_global_attribute18 IN ('ES_LOC','BOTH_LOC')
         AND xsss.batch_id = g_new_batch_id;

    TYPE supplier_site_t IS TABLE OF supplier_site_cur%ROWTYPE INDEX BY BINARY_INTEGER;
    l_supplier_site_tbl supplier_site_t;
  BEGIN
    pon_retcode := g_normal;

    -- Initialize global variables for log_errors
    g_source_table    := g_supplier_sites_t;
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

/*This procedure create address for all supplier sites in R12
  dt - 03/30 CR#372676
  Technical code  :AP_DUP_PARTY_SITE_NAME: same vendor site code cannot exists across two different address*/
   print_log_message('Supp_Party_Site_API Starts : '  );
    Supp_Party_Site_API(l_log_err_msg, l_retcode);
   print_log_message('Supp_Party_Site_API Ends : ' );

/*  This code is comments because this logic execute for all and it capture in procedure - mul_flag_upd_spp_site - 03/29
    -- Apply spain localization logic

    FOR es_cur IN supplier_site_spain
    LOOP
        SELECT COUNT (leg_vendor_site_code)
          INTO l_site_count
          FROM xxap_supplier_sites_stg
         WHERE leg_vendor_id = es_cur.leg_vendor_id
           AND batch_id = g_new_batch_id
           AND leg_tax_reporting_site_flag = 'Y'
           AND operating_unit_name = es_cur.operating_unit_name;

         IF l_site_count > 1 THEN

            UPDATE xxap_supplier_sites_stg
               SET leg_tax_reporting_site_flag = 'N'
             WHERE leg_vendor_id = es_cur.leg_vendor_id
               AND operating_unit_name = es_cur.operating_unit_name
               AND batch_id = g_new_batch_id
               AND leg_tax_reporting_site_flag = 'Y'
               AND rowid > (SELECT min(rowid)
                              FROM xxap_supplier_sites_stg
                             WHERE leg_vendor_id = es_cur.leg_vendor_id
                               AND batch_id = g_new_batch_id
                               AND operating_unit_name = es_cur.operating_unit_name
                               AND leg_tax_reporting_site_flag = 'Y');

         ELSIF l_site_count = 1 THEN

             SELECT COUNT (leg_vendor_site_code)
               INTO l_new_count
               FROM xxap_supplier_sites_stg
              WHERE leg_vendor_id = es_cur.leg_vendor_id
                AND batch_id = g_new_batch_id
                AND leg_tax_reporting_site_flag = 'Y';

             IF l_new_count > 1 THEN

                    UPDATE xxap_supplier_sites_stg
                       SET leg_tax_reporting_site_flag = 'N'
                     WHERE leg_vendor_id = es_cur.leg_vendor_id
                       AND batch_id = g_new_batch_id;
--                       AND operating_unit_name = es_cur.operating_unit_name;
             END if;
         END IF ;

    END LOOP ;

    COMMIT ;*/

    OPEN supplier_site_cur;
    LOOP
      FETCH supplier_site_cur BULK COLLECT
        INTO l_supplier_site_tbl LIMIT 1000;
      EXIT WHEN l_supplier_site_tbl.COUNT = 0;
      IF l_supplier_site_tbl.COUNT > 0 THEN
        BEGIN
          FORALL indx IN 1 .. l_supplier_site_tbl.COUNT SAVE EXCEPTIONS

            INSERT INTO ap_supplier_sites_int
              (accts_pay_code_combination_id,
               address_line1,
               address_line2,
               address_line3,
               address_line4,
               address_lines_alt,
               address_style,
               allow_awt_flag,
               always_take_disc_flag,
               amount_includes_tax_flag,
               ap_tax_rounding_rule,
               area_code,
               attention_ar_flag,
               attribute1,
               attribute10,
               attribute11,
               attribute12,
               attribute13,
               attribute14,
               attribute15,
               attribute2,
               attribute3,
               attribute4,
               attribute5,
               attribute6,
               attribute7,
               attribute8,
               attribute9,
               attribute_category,
               auto_tax_calc_flag,
               auto_tax_calc_override,
               awt_group_id,
               awt_group_name,
               bank_charge_bearer,
               bank_instruction1_code,
               bank_instruction2_code,
               bank_instruction_details,
               bill_to_location_code,
               bill_to_location_id,
               cage_code,
               ccr_comments,
               city,
               country,
               country_of_origin_code,
               county,
               created_by,
               create_debit_memo_flag,
               creation_date,
               customer_num,
               debarment_end_date,
               debarment_start_date,
               default_pay_site_id,
               delivery_channel_code,
               distribution_set_id,
               distribution_set_name,
               division_name,
               doing_bus_as_name,
               duns_number,
               ece_tp_location_code,
               edi_id_number,
               edi_payment_format,
               edi_payment_method,
               edi_remittance_instruction,
               edi_remittance_method,
               edi_transaction_handling,
               email_address,
               exclude_freight_from_discount,
               exclusive_payment_flag,
               fax,
               fax_area_code,
               fob_lookup_code,
               freight_terms_lookup_code,
               future_dated_payment_ccid,
               gapless_inv_num_flag,
               global_attribute1,
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
               global_attribute2,
               global_attribute20,
               global_attribute3,
               global_attribute4,
               global_attribute5,
               global_attribute6,
               global_attribute7,
               global_attribute8,
               global_attribute9,
               global_attribute_category,
               hold_all_payments_flag,
               hold_future_payments_flag,
               hold_reason,
               hold_unmatched_invoices_flag,
               iby_bank_charge_bearer,
               import_request_id,
               inactive_date,
               invoice_amount_limit,
               invoice_currency_code,
               language,
               last_updated_by,
               last_update_date,
               last_update_login,
               legal_business_name,
               location_id,
               match_option,
               offset_tax_flag,
               operating_unit_name,
               org_id,
               party_id,
               party_orig_system,
               party_orig_system_reference,
               party_site_id,
               party_site_name,
               party_site_orig_system,
               party_site_orig_sys_reference,
               payment_currency_code,
               payment_format_code,
               payment_method_code,
               payment_method_lookup_code,
               payment_priority,
               payment_reason_code,
               payment_reason_comments,
               payment_text_message1,
               payment_text_message2,
               payment_text_message3,
               pay_awt_group_id,
               pay_awt_group_name,
               pay_date_basis_lookup_code,
               pay_group_lookup_code,
               pay_on_code,
               pay_on_receipt_summary_code,
               pay_site_flag,
               pcard_site_flag,
               phone,
               prepay_code_combination_id,
               primary_pay_site_flag,
               program_application_id,
               program_id,
               program_update_date,
               province,
               purchasing_site_flag,
               reject_code,
               remittance_email,
               remit_advice_delivery_method,
               remit_advice_fax,
               request_id,
               retainage_rate,
               rfq_only_site_flag,
               sdh_batch_id,
               selling_company_identifier
               --,  services_tolerance_id
               --,  services_tolerance_name
              ,
               settlement_priority,
               shipping_control,
               ship_to_location_code,
               ship_to_location_id,
               ship_via_lookup_code,
               small_business_code,
               state,
               status,
               supplier_notif_method,
               supplier_site_orig_system,
               sup_site_orig_system_reference,
               tax_reporting_site_flag,
               telex,
               terms_date_basis,
               terms_id
               --,  terms_name
              ,
               tolerance_id,
               tolerance_name,
               tp_header_id,
               vat_code,
               vat_registration_num,
               vendor_id,
               vendor_interface_id,
               vendor_site_code,
               vendor_site_code_alt,
               vendor_site_interface_id,
               zip)

            VALUES
              (l_supplier_site_tbl(indx).accts_pay_code_combination_id --accts_pay_code_combination_id
              ,
               trim(l_supplier_site_tbl(indx).leg_address_line1) --address_line1
              ,
               trim(l_supplier_site_tbl(indx).leg_address_line2) --address_line2
              ,
               trim(l_supplier_site_tbl(indx).leg_address_line3) --address_line3
              ,
               trim(l_supplier_site_tbl(indx).leg_address_line4) --address_line4
              ,
               NULL --address_lines_alt
              ,
               UPPER(DECODE(l_supplier_site_tbl(indx).leg_address_style,
                            'GB',
                            'UAA',
                            l_supplier_site_tbl(indx).leg_address_style)) --address_style
              ,
               l_supplier_site_tbl(indx).leg_allow_awt_flag --allow_awt_flag
              ,
               l_supplier_site_tbl(indx).leg_always_take_disc_flag --always_take_disc_flag
              ,
               l_supplier_site_tbl(indx).leg_amount_incl_tax_flag --amount_includes_tax_flag
              ,
               l_supplier_site_tbl(indx).leg_ap_tax_rounding_rule --ap_tax_rounding_rule
              ,
               l_supplier_site_tbl(indx).leg_area_code --area_code
              ,
               l_supplier_site_tbl(indx).leg_attention_ar_flag --attention_ar_flag
              ,
               DECODE(l_supplier_site_tbl(indx).leg_source_system,
                      g_source_issc,
                      l_supplier_site_tbl(indx).attribute1,
                      NULL) --attribute1

              ,
               l_supplier_site_tbl(indx).attribute10 --attribute10
              ,
               l_supplier_site_tbl(indx).leg_attribute1 --attribute11   -- v1.2 change
              ,
               l_supplier_site_tbl(indx).attribute12 --attribute12
              ,
               l_supplier_site_tbl(indx).attribute13 --attribute13
              ,
               l_supplier_site_tbl(indx).attribute14 --attribute14
              ,
               l_supplier_site_tbl(indx).attribute15 --attribute15
              ,
               l_supplier_site_tbl(indx).attribute2 --attribute2
              ,
               l_supplier_site_tbl(indx).attribute3 --attribute3
              ,
               l_supplier_site_tbl(indx).attribute4 --attribute4
              ,
               l_supplier_site_tbl(indx).attribute5 --attribute5
              ,
               l_supplier_site_tbl(indx).attribute6 --attribute6
              ,
               l_supplier_site_tbl(indx).attribute7 --attribute7
              ,
               l_supplier_site_tbl(indx).attribute8 --attribute8
              ,
          /*     DECODE(l_supplier_site_tbl(indx).leg_source_system,
                      g_source_fsc,
                      l_supplier_site_tbl(indx).leg_attribute9,
                      NULL) --attribute9*/
               l_supplier_site_tbl(indx).leg_vendor_site_id  --ADB 09/27/2016 removed leg_Attribute9
              ,
               l_supplier_site_tbl(indx).attribute_category --attribute_category
              ,
               l_supplier_site_tbl(indx).leg_auto_tax_calc_flag --auto_tax_calc_flag
              ,
               l_supplier_site_tbl(indx).leg_auto_tax_calc_override --auto_tax_calc_override
              ,
               DECODE(l_supplier_site_tbl(indx).leg_global_attribute18,
                      'IT_LOC',
                      null,
                      'TH_LOC',
                      null,
                      l_supplier_site_tbl(indx).awt_group_id) --awt_group_id
              ,
               DECODE(l_supplier_site_tbl(indx).leg_global_attribute18,
                      'IT_LOC',
                      null,
                      'TH_LOC',
                      null,
                      l_supplier_site_tbl(indx).awt_group_name) --awt_group_name
              ,
               NULL --Bank_charge_bearer
              ,
               NULL --bank_instruction1_code
              ,
               NULL --bank_instruction2_code
              ,
               NULL --bank_instruction_details
              ,
               l_supplier_site_tbl(indx).bill_to_location_code --bill_to_location_code
              ,
               l_supplier_site_tbl(indx).bill_to_location_id --bill_to_location_id
              ,
               NULL --cage_code
              ,
               NULL --ccr_comments
              ,
               trim(l_supplier_site_tbl(indx).leg_city) --city
              ,
               trim(l_supplier_site_tbl(indx).leg_country) --country
              ,
               NULL --country_of_origin_code
              ,
               trim(l_supplier_site_tbl(indx).leg_county) --county
              ,
               g_user_id --created_by
              ,
               l_supplier_site_tbl(indx).leg_create_debit_memo_flag --create_debit_memo_flag
              ,
               SYSDATE --creation_date
              ,
               l_supplier_site_tbl(indx).leg_customer_num --customer_num
              ,
               NULL --debarment_end_date
              ,
               NULL --debarment_start_date
              ,
               l_supplier_site_tbl(indx).default_pay_site_id --default_pay_site_id
              ,
               NULL --delivery_channel_code
              ,
               l_supplier_site_tbl(indx).distribution_set_id --distribution_set_id
              ,
               l_supplier_site_tbl(indx).distribution_set_name --distribution_set_name
              ,
               NULL --division_name
              ,
               NULL --doing_bus_as_name
              ,
               NULL --duns_number        -- v1.2
              ,
               l_supplier_site_tbl(indx).leg_ece_tp_location_code --ece_tp_location_code
              ,
               l_supplier_site_tbl(indx).leg_edi_id_number --edi_id_number
              ,
               NULL --edi_payment_format
              ,
               NULL --edi_payment_method
              ,
               NULL --edi_remittance_instruction
              ,
               NULL --edi_remittance_method
              ,
               NULL --edi_transaction_handling
              ,
               l_supplier_site_tbl(indx).leg_email_address --email_address
              ,
               l_supplier_site_tbl(indx).leg_excl_freight_from_dist --exclude_freight_from_discount
              ,
               l_supplier_site_tbl(indx).leg_exclusive_payment_flag --exclusive_payment_flag
              ,
               l_supplier_site_tbl(indx).leg_fax --fax
              ,
               l_supplier_site_tbl(indx).leg_fax_area_code --fax_area_code
              ,
               l_supplier_site_tbl(indx).fob_lookup_code --fob_lookup_code
              ,
               l_supplier_site_tbl(indx).freight_terms_lookup_code --freight_terms_lookup_code
              ,
               l_supplier_site_tbl(indx).future_dated_payment_ccid --future_dated_payment_ccid
              ,
               NULL --gapless_inv_num_flag
              ,
               DECODE(l_supplier_site_tbl(indx).leg_global_attribute18,
                      'BR_LOC',
                      l_supplier_site_tbl(indx).leg_global_attribute1,
                      NULL) --global_attribute1
              ,
               DECODE(l_supplier_site_tbl(indx).leg_global_attribute18,
                      'BR_LOC',
                      l_supplier_site_tbl(indx).leg_global_attribute10,
                      NULL) --global_attribute10
              ,
               DECODE(l_supplier_site_tbl(indx).leg_global_attribute18,
                      'BR_LOC',
                      l_supplier_site_tbl(indx).leg_global_attribute11,
                      NULL) --global_attribute11
              ,
               DECODE(l_supplier_site_tbl(indx).leg_global_attribute18,
                      'BR_LOC',
                      l_supplier_site_tbl(indx).leg_global_attribute12,
                      NULL) --global_attribute12
              ,
               DECODE(l_supplier_site_tbl(indx).leg_global_attribute18,
                      'BR_LOC',
                      l_supplier_site_tbl(indx).leg_global_attribute13,
                      NULL) --global_attribute13
              ,
               DECODE(l_supplier_site_tbl(indx).leg_global_attribute18,
                      'BR_LOC',
                      l_supplier_site_tbl(indx).leg_global_attribute14,
                      NULL) --global_attribute14
              ,
               DECODE(l_supplier_site_tbl(indx).leg_global_attribute18,
                      'BR_LOC',
                      l_supplier_site_tbl(indx).leg_global_attribute15,
                      NULL) --global_attribute15
              ,
               NULL --l_supplier_site_tbl(indx).leg_global_attribute16           --global_attribute16
              ,
               NULL --l_supplier_site_tbl(indx).leg_global_attribute17           --global_attribute17
              ,
               NULL --l_supplier_site_tbl(indx).leg_global_attribute18           --global_attribute18
              ,
               NULL --l_supplier_site_tbl(indx).leg_global_attribute19           --global_attribute19
              ,
               NULL --l_supplier_site_tbl(indx).leg_global_attribute2            --global_attribute2
              ,
               NULL --l_supplier_site_tbl(indx).leg_global_attribute20           --global_attribute20
              ,
               NULL --l_supplier_site_tbl(indx).leg_global_attribute3            --global_attribute3
              ,
               NULL --l_supplier_site_tbl(indx).leg_global_attribute4            --global_attribute4
              ,
               NULL --l_supplier_site_tbl(indx).leg_global_attribute5            --global_attribute5
              ,
               NULL --l_supplier_site_tbl(indx).leg_global_attribute6            --global_attribute6
              ,
               NULL --l_supplier_site_tbl(indx).leg_global_attribute7            --global_attribute7
              ,
               NULL --l_supplier_site_tbl(indx).leg_global_attribute8            --global_attribute8
              ,
               DECODE(l_supplier_site_tbl(indx).leg_global_attribute18,
                      'BR_LOC',
                      l_supplier_site_tbl(indx).leg_global_attribute9,
                      NULL) --global_attribute9
              ,
               l_supplier_site_tbl(indx).leg_global_attribute_category --global_attribute_category
              ,
               l_supplier_site_tbl(indx).leg_hold_all_payments_flag --hold_all_payments_flag
              ,
               l_supplier_site_tbl(indx).leg_hold_future_pay_flag --hold_future_payments_flag
              ,
               l_supplier_site_tbl(indx).leg_hold_reason --hold_reason
              ,
               l_supplier_site_tbl(indx).leg_hold_unmatched_inv_flag --hold_unmatched_invoices_flag
              ,
               DECODE(l_supplier_site_tbl(indx).leg_bank_charge_bearer,
                      'I',
                      'OUR',
                      'S',
                      'BEN',
                      'N',
                      'BEN',
                      NULL,
                      NULL,
                      l_supplier_site_tbl(indx).leg_bank_charge_bearer) --iby_bank_charge_bearer
              ,
               NULL --import_request_id
              ,
               l_supplier_site_tbl(indx).leg_inactive_date --inactive_date
              ,
               l_supplier_site_tbl(indx).leg_invoice_amount_limit --invoice_amount_limit
              ,
               l_supplier_site_tbl(indx).leg_invoice_currency_code --invoice_currency_code
              ,
               l_supplier_site_tbl(indx).leg_language --language
              ,
               g_user_id --last_updated_by
              ,
               SYSDATE --last_update_date
              ,
               g_login_id --last_update_login
              ,
               NULL --legal_business_name
              ,
               NULL --location_id
              ,
               l_supplier_site_tbl(indx).leg_match_option --match_option
              ,
               l_supplier_site_tbl(indx).leg_offset_tax_flag --offset_tax_flag Dt 17 March 2016
              ,
               l_supplier_site_tbl(indx).operating_unit_name --operating_unit_name
              ,
               l_supplier_site_tbl(indx).org_id --org_id
              ,
               NULL --party_id
              ,
               NULL --party_orig_system
              ,
               NULL --party_orig_system_reference
              ,
               NULL --party_site_id
              ,
               NULL --party_site_name
              ,
               NULL --party_site_orig_system
              ,
               NULL --party_site_orig_sys_reference
              ,
               l_supplier_site_tbl(indx).leg_payment_currency_code --payment_currency_code
              ,
               NULL --payment_format_code
              ,
               l_supplier_site_tbl(indx).payment_method_code --payment_method_code
              ,
               l_supplier_site_tbl(indx).payment_method_lookup_code --payment_method_lookup_code
              ,
               l_supplier_site_tbl(indx).payment_priority --payment_priority
              ,
               NULL --payment_reason_code
              ,
               NULL --payment_reason_comments
              ,
               NULL --payment_text_message1
              ,
               NULL --payment_text_message2
              ,
               NULL --payment_text_message3
              ,
               DECODE(l_supplier_site_tbl(indx).leg_global_attribute18,
                      'IT_LOC',
                      l_supplier_site_tbl(indx).awt_group_id,
                      'TH_LOC',
                      l_supplier_site_tbl(indx).awt_group_id,
                      null) --pay_awt_group_id
              ,
               DECODE(l_supplier_site_tbl(indx).leg_global_attribute18,
                      'IT_LOC',
                      l_supplier_site_tbl(indx).awt_group_name,
                      'TH_LOC',
                      l_supplier_site_tbl(indx).awt_group_name,
                      null) --pay_awt_group_name
              ,
               l_supplier_site_tbl(indx).pay_date_basis_lookup_code --pay_date_basis_lookup_code
              ,
               l_supplier_site_tbl(indx).pay_group_lookup_code --pay_group_lookup_code
              ,
               l_supplier_site_tbl(indx).pay_on_code --pay_on_code
              ,
               l_supplier_site_tbl(indx).pay_on_receipt_summary_code --pay_on_receipt_summary_code
              ,
               l_supplier_site_tbl(indx).leg_pay_site_flag --pay_site_flag
              ,
               l_supplier_site_tbl(indx).leg_pcard_site_flag --pcard_site_flag
              ,
               l_supplier_site_tbl(indx).leg_phone --phone
              ,
               l_supplier_site_tbl(indx).prepay_code_combination_id --prepay_code_combination_id
              ,
               l_supplier_site_tbl(indx).leg_primary_pay_site_flag --primary_pay_site_flag
              ,
               NULL --program_application_id
              ,
               NULL --program_id
              ,
               NULL --program_update_date
              ,
               trim(l_supplier_site_tbl(indx).leg_province) --province
              ,
               l_supplier_site_tbl(indx).leg_purchasing_site_flag --purchasing_site_flag
              ,
               NULL --reject_code
              ,
               l_supplier_site_tbl(indx).leg_remittance_email --remittance_email
              ,
               DECODE(l_supplier_site_tbl(indx).leg_remittance_email,null
                                                                    ,null
                                                                    ,'EMAIL') --NULL --remit_advice_delivery_method  --ADB change 09/27/2016 change
              ,
               NULL --remit_advice_fax
              ,
               NULL --request_id
              ,
               NULL --retainage_rate
              ,
               l_supplier_site_tbl(indx).leg_rfq_only_site_flag --rfq_only_site_flag
              ,
               NULL --sdh_batch_id
              ,
               NULL --selling_company_identifier
               --, NULL                                                       --services_tolerance_id
               --, NULL                                                       --services_tolerance_name
              ,
               NULL --settlement_priority
              ,
               NULL --shipping_control
              ,
               l_supplier_site_tbl(indx).ship_to_location_code --ship_to_location_code
              ,
               l_supplier_site_tbl(indx).ship_to_location_id --ship_to_location_id
              ,
               l_supplier_site_tbl(indx).ship_via_lookup_code --ship_via_lookup_code
              ,
               NULL --small_business_code
              ,
               trim(l_supplier_site_tbl(indx).leg_state) --state
              ,
               'NEW' --status
              ,
               l_supplier_site_tbl(indx).leg_supplier_notif_method --supplier_notif_method
              ,
               NULL --supplier_site_orig_system
              ,
               NULL --sup_site_orig_system_reference
              ,
               l_supplier_site_tbl(indx).leg_tax_reporting_site_flag --tax_reporting_site_flag
              ,
               l_supplier_site_tbl(indx).leg_telex --telex
              ,
               l_supplier_site_tbl(indx).terms_date_basis --terms_date_basis
              ,
               l_supplier_site_tbl(indx).terms_id --terms_id
               --, l_supplier_site_tbl(indx).terms_name                       --terms_name
              ,
               l_supplier_site_tbl(indx).tolerance_id --tolerance_id
              ,
               l_supplier_site_tbl(indx).leg_tolerance_name --tolerance_name
              ,
               l_supplier_site_tbl(indx).leg_tp_header_id --tp_header_id
              ,
               l_supplier_site_tbl(indx).vat_Code --vat_code  --change for defect16336
              ,
               l_supplier_site_tbl(indx).leg_vat_registration_num --vat_registration_num
              ,
               l_supplier_site_tbl(indx).vendor_id --vendor_id
              ,
               l_supplier_site_tbl(indx).vendor_interface_id --vendor_interface_id
              ,
               l_supplier_site_tbl(indx).vendor_site_code --vendor_site_code
              ,
               l_supplier_site_tbl(indx).leg_vendor_site_code_alt --vendor_site_code_alt
              ,
               l_supplier_site_tbl(indx).vendor_site_interface_id --vendor_site_interface_id
              ,
               trim(l_supplier_site_tbl(indx).leg_zip) --zip
               );
        EXCEPTION
          WHEN OTHERS THEN
            print_log_message(SUBSTR('Exception in Procedure create_supplier_sites while doing Bulk Insert. ' ||
                                     SQLERRM,
                                     1,
                                     2000));
            print_log_message('No. of records in Bulk Exception : ' ||
                              SQL%BULK_EXCEPTIONS.COUNT);
        END;
      END IF;
      COMMIT;
    END LOOP;
    CLOSE supplier_site_cur;

    COMMIT;

    -- Update Successfully Interfaced Supplier Sites
    BEGIN
      UPDATE xxap_supplier_sites_stg xsss
         SET xsss.process_flag           = g_processed,
             xsss.last_update_date       = SYSDATE,
             xsss.last_updated_by        = g_user_id,
             xsss.last_update_login      = g_login_id,
             xsss.program_application_id = g_prog_appl_id,
             xsss.program_id             = g_conc_program_id,
             xsss.program_update_date    = SYSDATE,
             xsss.request_id             = g_request_id,
             xsss.batch_id               = g_new_batch_id,
             xsss.run_sequence_id        = g_run_seq_id
       WHERE xsss.process_flag = g_validated
         AND xsss.batch_id = g_new_batch_id
         AND EXISTS (SELECT 1
                FROM ap_supplier_sites_int assi
               WHERE assi.vendor_site_interface_id =
                     xsss.vendor_site_interface_id
                 AND assi.status = 'NEW');
      print_log_message('No. of records interfaced to ap_supplier_sites_int table ' ||
                        SQL%ROWCOUNT);
    EXCEPTION
      WHEN OTHERS THEN
        print_log_message('Exception occured while updating staging table for records interfaced to ap_supplier_sites_int table. Oracle error is ' ||
                          SQLERRM);
    END;

    COMMIT;

    -- Update Unsuccessful Supplier Sites
    BEGIN
      UPDATE xxap_supplier_sites_stg xsss
         SET xsss.process_flag           = g_error,
             xsss.error_type             = g_err_int,
             xsss.last_update_date       = SYSDATE,
             xsss.last_updated_by        = g_user_id,
             xsss.last_update_login      = g_login_id,
             xsss.program_application_id = g_prog_appl_id,
             xsss.program_id             = g_conc_program_id,
             xsss.program_update_date    = SYSDATE,
             xsss.request_id             = g_request_id,
             xsss.batch_id               = g_new_batch_id,
             xsss.run_sequence_id        = g_run_seq_id
       WHERE xsss.process_flag = g_validated
         AND xsss.batch_id = g_new_batch_id
         AND NOT EXISTS (SELECT 1
                FROM ap_supplier_sites_int assi
               WHERE assi.vendor_site_interface_id =
                     xsss.vendor_site_interface_id
                 AND assi.status = 'NEW');
      print_log_message('No. of records interfaced to ap_supplier_sites_int table ' ||
                        SQL%ROWCOUNT);

      COMMIT;

      FOR indx IN (SELECT xsss.*
                     FROM xxap_supplier_sites_stg xsss
                    WHERE xsss.process_flag = g_error
                      AND xsss.request_id = g_request_id
                      AND NOT EXISTS
                    (SELECT 1
                             FROM ap_supplier_sites_int assi
                            WHERE assi.vendor_site_interface_id =
                                  xsss.vendor_site_interface_id
                              AND assi.status = 'NEW')) LOOP
        g_intf_staging_id := indx.interface_txn_id;
        log_errors(pov_return_status       => l_log_ret_status -- OUT
                  ,
                   pov_error_msg           => l_log_err_msg -- OUT
                  ,
                   piv_source_column_name  => 'LEG_VENDOR_SITE_CODE',
                   piv_source_column_value => indx.leg_vendor_site_code,
                   piv_error_type          => g_err_int,
                   piv_error_code          => 'ETN_SUPP_SITE_INTF_ERR',
                   piv_error_message       => 'Error : Record could not be interfaced to ap_supplier_sites_int table. interface_txn_id=' ||
                                              indx.interface_txn_id);
      END LOOP;

    EXCEPTION
      WHEN OTHERS THEN
        print_log_message('Exception occured while updating staging table for records interfaced to ap_supplier_sites_int table. Oracle error is ' ||
                          SQLERRM);
    END;



/*This is Data fix, where pay on code = Receipt then pay site flag should be Y otherwise it fails in open
  interface after conversion will execute one program so pay site flag with update as per 11i data*/
BEGIN

  Update AP_SUPPLIER_SITES_INT
     Set pay_site_flag = 'Y'
   where pay_on_code = 'RECEIPT';

   COMMIT;
EXCEPTION
  WHEN OTHERS THEN
    print_log_message('Error :- In update Site code (HOME) for employee as supplier where site is TE,HOME. ' || SQLERRM);
END;


  EXCEPTION
    WHEN OTHERS THEN
      pon_retcode := g_ret_error;
      print_log_message(SUBSTR('Exception in Procedure create_supplier_sites. ' ||
                               SQLERRM,
                               1,
                               2000));
  END create_supplier_sites;

  --
  -- ========================
  -- Procedure: create_suppliers
  -- =============================================================================
  --   This procedure is used to insert validated supplier entity records
  --   into supplier interface table
  -- =============================================================================
  --  Input Parameters :
  --    None
  --  Output Parameters :
  --    None
  -- -----------------------------------------------------------------------------
  --
  PROCEDURE create_suppliers(pon_retcode OUT NUMBER) IS
    l_retcode NUMBER;
    l_supp_num_mtd NUMBER;
    l_log_ret_status VARCHAR2(50);
    l_log_err_msg    VARCHAR2(2000);

    CURSOR supplier_cur IS
      SELECT *
        FROM xxap_suppliers_stg xss
       WHERE xss.process_flag = g_validated
         AND xss.batch_id = g_new_batch_id;

    TYPE supplier_t IS TABLE OF supplier_cur%ROWTYPE INDEX BY BINARY_INTEGER;
    l_supplier_tbl supplier_t;
  BEGIN
    pon_retcode := g_normal;

/* Dt 03/30
  This code checks whether supplier numbering method is set to MANUAL
  if not set up then ignore insertion in supplier interface table   */
   select count(1)
     into l_supp_num_mtd
     from ap.ap_product_setup
    where supplier_numbering_method = 'MANUAL';

   /* l_supp_num_mtd := 1;*/

  If l_supp_num_mtd > 0 then
    -- Initialize global variables for log_errors
    g_source_table    := g_supplier_t;
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

    OPEN supplier_cur;
    LOOP
      FETCH supplier_cur BULK COLLECT
        INTO l_supplier_tbl LIMIT 1000;
      EXIT WHEN l_supplier_tbl.COUNT = 0;
      IF l_supplier_tbl.COUNT > 0 THEN
        BEGIN
          FORALL indx IN 1 .. l_supplier_tbl.COUNT SAVE EXCEPTIONS
            INSERT INTO ap_suppliers_int
              (vendor_interface_id,
               last_update_date,
               last_updated_by,
               vendor_name,
               vendor_name_alt,
               segment1,
               summary_flag,
               enabled_flag,
               last_update_login,
               creation_date,
               created_by,
               employee_id,
               vendor_type_lookup_code,
               customer_num,
               one_time_flag,
               min_order_amount,
               ship_to_location_id,
               ship_to_location_code,
               bill_to_location_id,
               bill_to_location_code,
               ship_via_lookup_code,
               freight_terms_lookup_code,
               fob_lookup_code,
               terms_id
               --,  terms_name
              ,
               set_of_books_id,
               always_take_disc_flag,
               pay_date_basis_lookup_code,
               pay_group_lookup_code,
               payment_priority,
               invoice_currency_code,
               payment_currency_code,
               invoice_amount_limit,
               hold_all_payments_flag,
               hold_future_payments_flag,
               hold_reason,
               distribution_set_id,
               distribution_set_name,
               accts_pay_code_combination_id,
               prepay_code_combination_id,
               num_1099,
               type_1099,
               organization_type_lookup_code,
               vat_code,
               start_date_active,
               end_date_active,
               minority_group_lookup_code,
               payment_method_lookup_code,
               women_owned_flag,
               small_business_flag,
               standard_industry_class,
               hold_flag,
               purchasing_hold_reason,
               hold_by,
               hold_date,
               terms_date_basis,
               inspection_required_flag,
               receipt_required_flag,
               qty_rcv_tolerance,
               qty_rcv_exception_code,
               enforce_ship_to_location_code,
               days_early_receipt_allowed,
               days_late_receipt_allowed,
               receipt_days_exception_code,
               receiving_routing_id,
               allow_substitute_receipts_flag,
               allow_unordered_receipts_flag,
               hold_unmatched_invoices_flag,
               exclusive_payment_flag,
               ap_tax_rounding_rule,
               auto_tax_calc_flag,
               auto_tax_calc_override,
               amount_includes_tax_flag,
               tax_verification_date,
               name_control,
               state_reportable_flag,
               federal_reportable_flag,
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
               request_id,
               program_application_id,
               program_id,
               program_update_date,
               vat_registration_num,
               auto_calculate_interest_flag,
               exclude_freight_from_discount,
               tax_reporting_name,
               allow_awt_flag,
               awt_group_id,
               awt_group_name,
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
               global_attribute_category,
               edi_transaction_handling,
               edi_payment_method,
               edi_payment_format,
               edi_remittance_method,
               edi_remittance_instruction,
               bank_charge_bearer,
               match_option,
               future_dated_payment_ccid,
               create_debit_memo_flag,
               offset_tax_flag,
               import_request_id,
               status,
               reject_code,
               ece_tp_location_code,
               iby_bank_charge_bearer,
               bank_instruction1_code,
               bank_instruction2_code,
               bank_instruction_details,
               payment_reason_code,
               payment_reason_comments,
               payment_text_message1,
               payment_text_message2,
               payment_text_message3,
               delivery_channel_code,
               payment_format_code,
               settlement_priority,
               payment_method_code,
               pay_awt_group_id,
               pay_awt_group_name,
               url,
               supplier_notif_method,
               remittance_email,
               email_address,
               remit_advice_fax,
               party_orig_system,
               party_orig_system_reference,
               sdh_batch_id,
               party_id,
               ceo_name,
               ceo_title)
            VALUES
              (l_supplier_tbl(indx).vendor_interface_id --vendor_interface_id
              ,
               SYSDATE --last_update_date
              ,
               g_user_id --last_updated_by
              ,
               l_supplier_tbl(indx).leg_vendor_name --vendor_name
              ,
               l_supplier_tbl(indx).leg_vendor_name_alt --vendor_name_alt
              ,
               l_supplier_tbl(indx).leg_segment1 --segment1
              ,
               l_supplier_tbl(indx).leg_summary_flag --summary_flag
              ,
               l_supplier_tbl(indx).leg_enabled_flag --enabled_flag
              ,
               g_login_id --last_update_login
              ,
               SYSDATE --creation_date
              ,
               g_user_id --created_by
              ,
               l_supplier_tbl(indx).employee_id --employee_id
              ,
               l_supplier_tbl(indx).vendor_type_lookup_code --vendor_type_lookup_code
              ,
               l_supplier_tbl(indx).leg_customer_num --customer_num
              ,
               l_supplier_tbl(indx).leg_one_time_flag --one_time_flag
              ,
               l_supplier_tbl(indx).leg_min_order_amount --min_order_amount
              ,
               l_supplier_tbl(indx).ship_to_location_id --ship_to_location_id
              ,
               l_supplier_tbl(indx).ship_to_location_code --ship_to_location_code
              ,
               l_supplier_tbl(indx).bill_to_location_id --bill_to_location_id
              ,
               l_supplier_tbl(indx).bill_to_location_code --bill_to_location_code
              ,
               l_supplier_tbl(indx).ship_via_lookup_code --ship_via_lookup_code
              ,
               l_supplier_tbl(indx).freight_terms_lookup_code --freight_terms_lookup_code
              ,
               l_supplier_tbl(indx).fob_lookup_code --fob_lookup_code
              ,
               l_supplier_tbl(indx).terms_id --terms_id
               --,  l_supplier_tbl(indx).terms_name                          --terms_name
              ,
               l_supplier_tbl(indx).set_of_books_id --set_of_books_id
              ,
               l_supplier_tbl(indx).leg_always_take_disc_flag --always_take_disc_flag
              ,
               l_supplier_tbl(indx).pay_date_basis_lookup_code --pay_date_basis_lookup_code
              ,
               l_supplier_tbl(indx).pay_group_lookup_code --pay_group_lookup_code
              ,
               l_supplier_tbl(indx).payment_priority --payment_priority
              ,
               l_supplier_tbl(indx).leg_invoice_currency_code --invoice_currency_code
              ,
               l_supplier_tbl(indx).leg_payment_currency_code --payment_currency_code
              ,
               NULL --invoice_amount_limit
              ,
               l_supplier_tbl(indx).Leg_hold_all_payments_flag --hold_all_payments_flag
              ,
               NULL --hold_future_payments_flag
              ,
               NULL --hold_reason
              ,
               l_supplier_tbl(indx).distribution_set_id --distribution_set_id
              ,
               l_supplier_tbl(indx).distribution_set_name --distribution_set_name
              ,
               l_supplier_tbl(indx).accts_pay_code_combination_id --accts_pay_code_combination_id
              ,
               l_supplier_tbl(indx).prepay_code_combination_id --prepay_code_combination_id
              ,
               l_supplier_tbl(indx).leg_num_1099 --num_1099
              ,
               l_supplier_tbl(indx).leg_type_1099 --type_1099
              ,
               l_supplier_tbl(indx).organization_type_lookup_code --organization_type_lookup_code
              ,
               l_supplier_tbl(indx).leg_vat_code --vat_code
              ,
               l_supplier_tbl(indx).leg_start_date_active --start_date_active
              ,
               l_supplier_tbl(indx).leg_end_date_active --end_date_active
              ,
               l_supplier_tbl(indx).minority_group_lookup_code --minority_group_lookup_code
              ,
               l_supplier_tbl(indx).payment_method_lookup_code --payment_method_lookup_code
              ,
               l_supplier_tbl(indx).leg_women_owned_flag --women_owned_flag
              ,
               l_supplier_tbl(indx).leg_small_business_flag --small_business_flag
              ,
               l_supplier_tbl(indx).leg_standard_industry_class --standard_industry_class
              ,
               l_supplier_tbl(indx).leg_hold_flag --hold_flag
              ,
               l_supplier_tbl(indx).leg_purchasing_hold_reason --purchasing_hold_reason
              ,
               l_supplier_tbl(indx).leg_hold_by --hold_by
              ,
               l_supplier_tbl(indx).leg_hold_date --hold_date
              ,
               l_supplier_tbl(indx).leg_terms_date_basis --terms_date_basis
              ,
               l_supplier_tbl(indx).leg_inspection_required_flag --inspection_required_flag
              ,
               l_supplier_tbl(indx).leg_receipt_required_flag --receipt_required_flag
              ,
               l_supplier_tbl(indx).LEG_QTY_RCV_TOLERANCE --qty_rcv_tolerance (No Translations, so passing as-is)
              ,
               --v4.0
               DECODE(l_supplier_tbl(indx).vendor_type_lookup_code,'EMPLOYEE',NULL, 'NONE')
               --l_supplier_tbl(indx).qty_rcv_exception_code --qty_rcv_exception_code
              ,
               l_supplier_tbl(indx).LEG_ENFORCE_SHIP_TO_CODE --  (No Translations, so passing as-is)
              ,
               l_supplier_tbl(indx).leg_days_early_recpt_allowed --days_early_receipt_allowed
              ,
               l_supplier_tbl(indx).leg_days_late_recpt_allowed --days_late_receipt_allowed
              ,
               l_supplier_tbl(indx).leg_receipt_days_except_code --receipt_days_exception_code
              ,
               l_supplier_tbl(indx).leg_receiving_routing_id --receiving_routing_id
              ,
               l_supplier_tbl(indx).leg_allow_subs_recpt_flag --allow_substitute_receipts_flag
              ,
               l_supplier_tbl(indx).leg_unordered_receipts_flag --allow_unordered_receipts_flag
              ,
               l_supplier_tbl(indx).leg_hold_unmatched_inv_flag --hold_unmatched_invoices_flag
              ,
               l_supplier_tbl(indx).leg_exclusive_payment_flag --exclusive_payment_flag
              ,
               l_supplier_tbl(indx).leg_ap_tax_rounding_rule --ap_tax_rounding_rule
              ,
               l_supplier_tbl(indx).leg_auto_tax_calc_flag --auto_tax_calc_flag
              ,
               l_supplier_tbl(indx).leg_auto_tax_calc_override --auto_tax_calc_override
              ,
               l_supplier_tbl(indx).leg_amount_includes_tax_flag --amount_includes_tax_flag
              ,
               l_supplier_tbl(indx).leg_tax_verification_date --tax_verification_date
              ,
               l_supplier_tbl(indx).leg_name_control --name_control
              ,
               l_supplier_tbl(indx).leg_state_reportable_flag --state_reportable_flag
              ,
               DECODE(l_supplier_tbl(indx).leg_global_attribute17,
                      'ES_LOC',
                      'Y',
                      'BOTH_LOC',
                      'Y',
                      l_supplier_tbl(indx).leg_federal_reportable_flag) --federal_reportable_flag
              ,
               l_supplier_tbl(indx).attribute_category --attribute_category
              ,
               l_supplier_tbl(indx).leg_attribute1 --attribute1
              ,
               l_supplier_tbl(indx).attribute2 --attribute2
              ,
               l_supplier_tbl(indx).leg_attribute3 --attribute3
              ,
               l_supplier_tbl(indx).leg_attribute4 --attribute4
              ,
               l_supplier_tbl(indx).attribute5 --attribute5
              ,
               l_supplier_tbl(indx).leg_attribute6 --attribute6
              ,
               l_supplier_tbl(indx).attribute7 --attribute7
              ,
               l_supplier_tbl(indx).leg_attribute8 --attribute8
              ,
               l_supplier_tbl(indx).leg_attribute9 --attribute9
              ,
               l_supplier_tbl(indx).leg_attribute10 --attribute10
              ,
               l_supplier_tbl(indx).leg_attribute11 --attribute11
              ,
               l_supplier_tbl(indx).leg_attribute12 --attribute12
              ,
               l_supplier_tbl(indx).leg_attribute13 --attribute13
              ,
               l_supplier_tbl(indx).attribute14 --attribute14
              ,
               l_supplier_tbl(indx).attribute15 --attribute15
              ,
               NULL --request_id
              ,
               NULL --program_application_id
              ,
               NULL --program_id
              ,
               NULL --program_update_date
              ,
               l_supplier_tbl(indx).leg_vat_registration_num --vat_registration_num
              ,
               l_supplier_tbl(indx).leg_auto_calculate_int_flag --auto_calculate_interest_flag
              ,
               l_supplier_tbl(indx).leg_exclude_freight_from_disc --exclude_freight_from_discount
              ,
               l_supplier_tbl(indx).leg_tax_reporting_name --tax_reporting_name
              ,
               l_supplier_tbl(indx).leg_allow_awt_flag --allow_awt_flag
              ,
               DECODE(l_supplier_tbl(indx).leg_global_attribute17,
                      'IT_LOC',
                      NULL,
                      'TH_LOC',
                      NULL,
                      'BOTH_LOC',
                      NULL,
                      l_supplier_tbl(indx).awt_group_id) --awt_group_id
              ,
               DECODE(l_supplier_tbl(indx).leg_global_attribute17,
                      'IT_LOC',
                      NULL,
                      'TH_LOC',
                      NULL,
                      'BOTH_LOC',
                      NULL,
                      l_supplier_tbl(indx).awt_group_name) --awt_group_name
              ,
               NULL --l_supplier_tbl(indx).leg_global_attribute1               --global_attribute1
              ,
               DECODE(l_supplier_tbl(indx).leg_global_attribute17,
                      'IT_LOC',
                      l_supplier_tbl(indx).leg_global_attribute2,
                      'BOTH_LOC',
                      l_supplier_tbl(indx).leg_global_attribute2,
                      NULL) --l_supplier_tbl(indx).leg_global_attribute2               --global_attribute2
              ,
               DECODE(l_supplier_tbl(indx).leg_global_attribute17,
                      'IT_LOC',
                      l_supplier_tbl(indx).leg_global_attribute3,
                      'BOTH_LOC',
                      l_supplier_tbl(indx).leg_global_attribute3,
                      NULL) --l_supplier_tbl(indx).leg_global_attribute3               --global_attribute3
              ,
               DECODE(l_supplier_tbl(indx).leg_global_attribute17,
                      'IT_LOC',
                      l_supplier_tbl(indx).leg_global_attribute4,
                      'BOTH_LOC',
                      l_supplier_tbl(indx).leg_global_attribute4,
                      NULL) --l_supplier_tbl(indx).leg_global_attribute4               --global_attribute4
              ,
               NULL --l_supplier_tbl(indx).leg_global_attribute5               --global_attribute5
              ,
               NULL --l_supplier_tbl(indx).leg_global_attribute6               --global_attribute6
              ,
               NULL --l_supplier_tbl(indx).leg_global_attribute7               --global_attribute7
              ,
               NULL --l_supplier_tbl(indx).leg_global_attribute8               --global_attribute8
              ,
               NULL --l_supplier_tbl(indx).leg_global_attribute9               --global_attribute9
              ,
               DECODE(l_supplier_tbl(indx).leg_global_attribute17,
                      'CL_LOC',
                      l_supplier_tbl(indx).leg_global_attribute10,
                      'BOTH_LOC',
                      l_supplier_tbl(indx).leg_global_attribute10,
                      NULL) --l_supplier_tbl(indx).leg_global_attribute10              --global_attribute10
              ,
               NULL --l_supplier_tbl(indx).leg_global_attribute11              --global_attribute11

              ,DECODE ( l_supplier_tbl(indx).leg_global_attribute17,
                       'CL_LOC',
                       DECODE (l_supplier_tbl(indx).leg_global_attribute10,'DOMESTIC_ORIGIN',l_supplier_tbl(indx).leg_global_attribute12, NULL ),
                       'BOTH_LOC',
                       DECODE (l_supplier_tbl(indx).leg_global_attribute10,'DOMESTIC_ORIGIN',l_supplier_tbl(indx).leg_global_attribute12,NULL ),
                       NULL ) --l_supplier_tbl(indx).leg_global_attribute12              --global_attribute12
              ,
               NULL --l_supplier_tbl(indx).leg_global_attribute13              --global_attribute13
              ,
               NULL --l_supplier_tbl(indx).leg_global_attribute14              --global_attribute14
              ,
               NULL --l_supplier_tbl(indx).leg_global_attribute15              --global_attribute15
              ,
               NULL --l_supplier_tbl(indx).leg_global_attribute16              --global_attribute16
              ,
               NULL --l_supplier_tbl(indx).leg_global_attribute17              --global_attribute17
              ,
               NULL --l_supplier_tbl(indx).leg_global_attribute18              --global_attribute18
              ,
               NULL --l_supplier_tbl(indx).leg_global_attribute19              --global_attribute19
              ,
               NULL --l_supplier_tbl(indx).leg_global_attribute20              --global_attribute20
              ,
               l_supplier_tbl(indx).leg_global_attribute_category --global_attribute_category
              ,
               NULL --edi_transaction_handling
              ,
               NULL --edi_payment_method
              ,
               NULL --edi_payment_format
              ,
               NULL --edi_remittance_method
              ,
               NULL --edi_remittance_instruction
              ,
               NULL --bank_charge_bearer
              ,
               l_supplier_tbl(indx).match_option --match_option
              ,
               l_supplier_tbl(indx).future_dated_payment_ccid --future_dated_payment_ccid
              ,
               l_supplier_tbl(indx).leg_create_debit_memo_flag --create_debit_memo_flag
              ,
               l_supplier_tbl(indx).leg_offset_tax_flag --offset_tax_flag
              ,
               NULL --import_request_id
              ,
               'NEW' --status
              ,
               NULL --reject_code
              ,
               NULL --ece_tp_location_code
              ,
               DECODE(l_supplier_tbl(indx).bank_charge_bearer,
                      'I',
                      'OUR',
                      'S',
                      'BEN',
                      'N',
                      'BEN',
                      NULL,
                      NULL,
                      l_supplier_tbl(indx).bank_charge_bearer) --iby_bank_charge_bearer
              ,
               NULL --bank_instruction1_code
              ,
               NULL --bank_instruction2_code
              ,
               NULL --bank_instruction_details
              ,
               NULL --payment_reason_code
              ,
               NULL --payment_reason_comments
              ,
               NULL --payment_text_message1
              ,
               NULL --payment_text_message2
              ,
               NULL --payment_text_message3
              ,
               NULL --delivery_channel_code
              ,
               NULL --payment_format_code
              ,
               NULL --settlement_priority
              ,
               NULL --payment_method_code
               --               ,  NULL                                                     --pay_awt_group_id
              ,
               DECODE(l_supplier_tbl(indx).leg_global_attribute17,
                      'IT_LOC',
                      l_supplier_tbl(indx).awt_group_id,
                      'TH_LOC',
                      l_supplier_tbl(indx).awt_group_id,
                      'BOTH_LOC',
                      l_supplier_tbl(indx).awt_group_id,
                      NULL) --pay_awt_group_id
               --               ,  NULL                                                     --pay_awt_group_name
              ,
               DECODE(l_supplier_tbl(indx).leg_global_attribute17,
                      'IT_LOC',
                      l_supplier_tbl(indx).awt_group_name,
                      'TH_LOC',
                      l_supplier_tbl(indx).awt_group_name,
                      'BOTH_LOC',
                      l_supplier_tbl(indx).awt_group_id,
                      NULL) --pay_awt_group_id
              ,
               NULL --url
              ,
               NULL --supplier_notif_method
              ,
               NULL --remittance_email
              ,
               NULL --email_address
              ,
               NULL --remit_advice_fax
              ,
               NULL --party_orig_system
              ,
               NULL --party_orig_system_reference
              ,
               NULL --sdh_batch_id
              ,
               NULL --party_id
              ,
               NULL --ceo_name
              ,
               NULL --ceo_title
               );
        EXCEPTION
          WHEN OTHERS THEN
            print_log_message(SUBSTR('Exception in Procedure create_suppliers while doing Bulk Insert. ' ||
                                     SQLERRM,
                                     1,
                                     2000));
            print_log_message('No. of records in Bulk Exception : ' ||
                              SQL%BULK_EXCEPTIONS.COUNT);
        END;
      END IF;
      COMMIT;
    END LOOP;
    CLOSE supplier_cur;

    -- Updated Successfully interfaced Suppliers
    BEGIN
      UPDATE xxap_suppliers_stg xss
         SET xss.process_flag       = g_processed,
             last_updated_date      = SYSDATE,
             last_updated_by        = g_user_id,
             last_update_login      = g_login_id,
             program_application_id = g_prog_appl_id,
             program_id             = g_conc_program_id,
             program_update_date    = SYSDATE,
             request_id             = g_request_id,
             batch_id               = g_new_batch_id,
             run_sequence_id        = g_run_seq_id
       WHERE xss.process_flag = g_validated
         AND xss.batch_id = g_new_batch_id
         AND EXISTS
       (SELECT 1
                FROM ap_suppliers_int asi
               WHERE asi.vendor_interface_id = xss.vendor_interface_id
                 AND asi.status = 'NEW');
      print_log_message('No. of records interfaced to ap_suppliers_int table ' ||
                        SQL%ROWCOUNT);
    EXCEPTION
      WHEN OTHERS THEN
        print_log_message('Exception occured while updating staging table for records interfaced to ap_suppliers_int table. Oracle error is ' ||
                          SQLERRM);
    END;

    COMMIT;

    -- Updated Unsuccessful Suppliers
    BEGIN
      UPDATE xxap_suppliers_stg xss
         SET xss.process_flag       = g_error,
             xss.error_type         = g_err_int,
             last_updated_date      = SYSDATE,
             last_updated_by        = g_user_id,
             last_update_login      = g_login_id,
             program_application_id = g_prog_appl_id,
             program_id             = g_conc_program_id,
             program_update_date    = SYSDATE,
             request_id             = g_request_id,
             batch_id               = g_new_batch_id,
             run_sequence_id        = g_run_seq_id
       WHERE xss.process_flag = g_validated
         AND xss.batch_id = g_new_batch_id
         AND NOT EXISTS
       (SELECT 1
                FROM ap_suppliers_int asi
               WHERE asi.vendor_interface_id = xss.vendor_interface_id
                 AND asi.status = 'NEW');

      print_log_message('No. of records not interfaced to ap_suppliers_int table ' ||
                        SQL%ROWCOUNT);
      COMMIT;

      FOR indx IN (SELECT xss.*
                     FROM xxap_suppliers_stg xss
                    WHERE xss.process_flag = g_error
                      AND xss.request_id = g_request_id
                      AND NOT EXISTS (SELECT 1
                             FROM ap_suppliers_int asi
                            WHERE asi.vendor_interface_id =
                                  xss.vendor_interface_id
                              AND asi.status = 'NEW')) LOOP
        g_intf_staging_id := indx.interface_txn_id;
        log_errors(pov_return_status       => l_log_ret_status -- OUT
                  ,
                   pov_error_msg           => l_log_err_msg -- OUT
                  ,
                   piv_source_column_name  => 'LEG_SEGMENT1',
                   piv_source_column_value => indx.leg_segment1,
                   piv_error_type          => g_err_int,
                   piv_error_code          => 'ETN_SUPP_INTF_ERR',
                   piv_error_message       => 'Error : Record could not be interfaced to ap_suppliers_int table. interface_txn_id=' ||
                                              indx.interface_txn_id);
      END LOOP;

    EXCEPTION
      WHEN OTHERS THEN
        print_log_message('Exception occured while updating staging table for records interfaced to ap_suppliers_int table. Oracle error is ' ||
                          SQLERRM);
    END;

ELSE
    pon_retcode := g_ret_error;
    print_log_message('Supplier numbering method is not set up MANUAL');

END IF;
  EXCEPTION
    WHEN OTHERS THEN
      pon_retcode := g_ret_error;
      print_log_message(SUBSTR('Exception in Procedure create_suppliers. ' ||
                               SQLERRM,
                               1,
                               2000));
  END create_suppliers;

  --
  -- =============================================================================
  -- Procedure: validate_vendor_site
  -- =============================================================================
  --   This procedure validate_vendor_site
  -- =============================================================================
  --  Input Parameters :
  -- piv_vendor_num      : Vendor Num
  -- piv_vendor_site_Code: Vendor Site Code
  -- pin_org_id          : Org id
  --

  --  Output Parameters :
  --  pon_error_cnt       : Return Error Count
  --  pon_vendor_site_id  : Vendor site ID
  -- -----------------------------------------------------------------------------
  --
  PROCEDURE validate_vendor_site(piv_vendor_num       IN VARCHAR2,
                                 piv_vendor_site_Code IN VARCHAR2,
                                 pin_org_id           IN NUMBER,
                                 pon_vendor_site_id   OUT NUMBER,
                                 pon_error_cnt        OUT NUMBER) IS
    l_record_cnt     NUMBER;
    l_vendor_site_id NUMBER;

  BEGIN

    xxetn_debug_pkg.add_debug(' +  PROCEDURE : validate_vendor_site  ' ||
                              piv_vendor_site_Code || ' + ');

    l_record_cnt := 0;

    BEGIN
      --validate vendor site and derive vendor site ID
      SELECT vendor_site_id
        INTO l_vendor_site_id
        FROM ap_supplier_sites_all a, ap_suppliers b
       WHERE --b.vendor_name = p_in_vendor_name
       b.segment1 = piv_vendor_num
       AND a.vendor_id = b.vendor_id
       AND a.vendor_site_code = piv_vendor_site_Code
       AND b.enabled_flag = 'Y'
       AND a.org_id = pin_org_id;

      pon_vendor_site_id := l_vendor_site_id;

    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        l_record_cnt       := 2;
        pon_vendor_site_id := NULL;
        print_log_message('In No Data found of validate vendor site check' ||
                          SQLERRM);
      WHEN OTHERS THEN
        l_record_cnt       := 2;
        pon_vendor_site_id := NULL;
        print_log_message('In When others of validate vendor site check' ||
                          SQLERRM);
    END;

    IF l_record_cnt = 2 THEN
      pon_error_cnt := 2;
    ELSE
      pon_error_cnt := 0;
    END IF;

    print_log_message(' -  PROCEDURE : validate_vendor_site  - ');
  EXCEPTION
    WHEN OTHERS THEN
      g_retcode          := 2;
      pon_error_cnt      := 2;
      pon_vendor_site_id := NULL;
      g_errbuff          := 'Failed while validating vendor site.';
      print_log_message('In Exception validate vendor site' || SQLERRM);
  END validate_vendor_site;



/*
  -- ========================
  -- Procedure: Sup_Pay_site_flag_UPD
  -- =============================================================================
  --   This procedure update pay site flag as per 11i data this because as per
       R12 expert data fix solution we have to update pay site flag = 'Y' if pay on code is receipt
       below procedure reset pay site flag as per 11i
  -- =============================================================================
  --  Input Parameters :
  --    None
  --  Output Parameters :
  --    None
  -- -----------------------------------------------------------------------------
  */

PROCEDURE Sup_Pay_site_flag_UPD(pov_errbuf  OUT NOCOPY VARCHAR2,
                                 pon_retcode OUT NOCOPY NUMBER) IS
  l_error_message      VARCHAR2(2000);
  ln_update_vendor_site_id NUMBER;
  lc_leg_vsc           VARCHAR2(1000);
  lc_return_status     VARCHAR2(2000);
  ln_msg_count         NUMBER;
  ll_msg_data          LONG;
  Ln_Vendor_Id         NUMBER;
  Ln_Vendor_site_Id    NUMBER;
  ln_message_int       NUMBER;
  Ln_Party_Id          NUMBER;
  lrec_vendor_site_rec ap_vendor_pub_pkg.r_vendor_site_rec_type;

  Cursor cur_pay_site is
   Select distinct vendor_id,vendor_site_id,org_id,leg_pay_site_flag
   from xxconv.xxap_supplier_sites_stg a
  where /*LEG_DEFAULT_PAY_SITE is not null  --v3.0 ADB 09/27/2016 this is to bring the pay site update in sync
    and */process_flag = 'C'
    and leg_pay_site_flag = 'N'
    and pay_on_code = 'RECEIPT'
    and exists (Select 1 from ap_supplier_sites_all
                where vendor_site_code =  a.vendor_site_code --v3.0 ADB instead of leg_vendor_Site_Code
                    and vendor_id = a.vendor_id
                    and vendor_site_id = a.vendor_site_id
                    and pay_site_flag = 'Y');

--v3.0
v_resp_appl_id NUMBER := fnd_global.resp_appl_id;
v_resp_id  NUMBER     := fnd_global.resp_id;
v_user_id  NUMBER     := fnd_global.user_id;
--v3.0
BEGIN

FOR r_update_pay_site in cur_pay_site LOOP
  --v3.0
  FND_GLOBAL.APPS_INITIALIZE(v_user_id,v_resp_id, v_resp_appl_id);
                MO_GLOBAL.INIT ('SQLAP');
                mo_global.set_policy_context ('S' ,r_update_pay_site.org_id);
  --v3.0
  print_log_message('-----------------------------------------------------------------------------');
  print_log_message('--------------- Values to be passed to API ----------------------------------');

  Ln_Vendor_site_Id :=r_update_pay_site.vendor_site_id;

  Lrec_Vendor_site_Rec.vendor_id := r_update_pay_site.vendor_id;
  print_log_message('--------------- Lrec_Vendor_site_Rec.vendor_id:'||r_update_pay_site.vendor_id);

  Lrec_Vendor_site_Rec.vendor_site_id := r_update_pay_site.vendor_site_id;
  print_log_message('--------------- Lrec_Vendor_site_Rec.vendor_site_id:'|| r_update_pay_site.vendor_site_id);

  Lrec_Vendor_site_Rec.org_id := r_update_pay_site.org_id;
  print_log_message('--------------- Lrec_Vendor_site_Rec.org_id:'||r_update_pay_site.org_id);

  Lrec_Vendor_site_Rec.pay_site_flag := nvl(r_update_pay_site.leg_pay_site_flag,'N');
  print_log_message('--------------- Lrec_Vendor_site_Rec.pay_site_flag: '|| nvl(r_update_pay_site.leg_pay_site_flag,'N'));

  print_log_message('-----------------------------------------------------------------------------');
  print_log_message('--------------- Call API ** AP_VENDOR_PUB_PKG.Update_Vendor_Site_public **');

  AP_VENDOR_PUB_PKG.Update_Vendor_Site_public(p_api_version     => 1, --
                                              x_return_status   => lc_return_status, --
                                              x_msg_count       => ln_msg_count, --
                                              x_msg_data        => ll_msg_data, --
                                              p_vendor_site_rec => Lrec_Vendor_site_Rec, --
                                              p_Vendor_site_Id  => Ln_Vendor_site_Id);

  print_log_message('API Return Status:: ' || lc_return_status);
  print_log_message('API msg count:: ' || ln_msg_count);

  IF (lc_return_status <> 'S') THEN
    IF ln_msg_count >= 1 THEN
      FOR v_index IN 1 .. ln_msg_count LOOP
        fnd_msg_pub.get(p_msg_index     => v_index,
                        p_encoded       => 'F',
                        p_data          => ll_msg_data,
                        p_msg_index_out => ln_message_int);
        Ll_Msg_Data := 'UPDATE_VENDOR_SITE ' || SUBSTR(Ll_Msg_Data, 1, 3900);
        print_log_message('Ll_Msg_Data - ' || Ll_Msg_Data);
      END LOOP;
    End If; -- msg count
  END IF; -- API return status

--END IF; -- IF default vendor site exists and converted
 END LOOP;
COMMIT;
  EXCEPTION
    WHEN OTHERS THEN
      l_error_message := SUBSTR('Exception in Procedure Dup_SupplierSite_Block. ' ||
                                SQLERRM,
                                1,
                                1999);
      pon_retcode     := 2;
      pov_errbuf      := l_error_message;
      fnd_file.put_line(fnd_file.LOG, l_error_message);
End Sup_Pay_site_flag_UPD;


/*
  -- ========================
  -- Procedure: Sup_Default_Pay_site
  -- =============================================================================
  --   This procedure create link bewteen pay site and alternamte as per 11i
  -- =============================================================================
  --  Input Parameters :
  --    None
  --  Output Parameters :
  --    None
  -- -----------------------------------------------------------------------------
  */

PROCEDURE Sup_Default_Pay_site(pov_errbuf  OUT NOCOPY VARCHAR2,
                                 pon_retcode OUT NOCOPY NUMBER) IS
  l_error_message      VARCHAR2(2000);
  ln_update_vendor_site_id     NUMBER;
  lc_leg_vsc                   VARCHAR2(1000);

  lc_return_status             VARCHAR2(2000);
  ln_msg_count                 NUMBER;
  ll_msg_data                  LONG;
  Ln_Vendor_Id                 NUMBER;
  Ln_Vendor_site_Id            NUMBER;
  ln_message_int               NUMBER;
  Ln_Party_Id                  NUMBER;
  lrec_vendor_site_rec ap_vendor_pub_pkg.r_vendor_site_rec_type;

-- Cursor to select list of Vendors whose sites are to be updated
Cursor cur_update_vendors IS
Select distinct leg_vendor_id,leg_vendor_name,vendor_id,org_id--, leg_vendor_site_id
  from xxconv.xxap_supplier_sites_stg
 where process_flag = 'C'
   and leg_pay_site_flag = 'N'
   --and vendor_id=12241 -- hardcoded vendor_id for testing...remove thereafter...
   and LEG_DEFAULT_PAY_SITE is not null;

-- Cursor to select the vendor sites to be updated for each Vendor
Cursor cur_update_vendor_sites (v_leg_vendor_id NUMBER,v_org_id NUMBER )IS
Select interface_txn_id,leg_vendor_site_id,LEG_DEFAULT_PAY_SITE,org_id, vendor_site_id
  from xxconv.xxap_supplier_sites_stg
 where process_flag = 'C'
   and leg_pay_site_flag = 'N'
   and LEG_DEFAULT_PAY_SITE is not null
   and leg_vendor_id= v_leg_vendor_id --(from above Query)
   and org_id= v_org_id; --(from above Query)

-- Cursor to get leg_default_pay_site_id based on interface_tx_id from xxextn schema table
Cursor cur_get_DPS_id (v_interface_txn_id NUMBER) IS
Select leg_default_pay_site_id, leg_default_pay_site, leg_org_id
  from xxextn.xxap_supplier_sites_stg
 where interface_txn_id = v_interface_txn_id; --(from above Query)

v_resp_appl_id NUMBER := fnd_global.resp_appl_id;
v_resp_id  NUMBER     := fnd_global.resp_id;
v_user_id  NUMBER     := fnd_global.user_id;
BEGIN


-- Select list of Vendors whose sites are to be updated
FOR r_update_vendors in cur_update_vendors LOOP
   print_log_message('------------ Vendor to be updated : '||r_update_vendors.leg_vendor_id||' : '||r_update_vendors.leg_vendor_name||ln_update_vendor_site_id  );
    -- Select the vendor sites to be updated for each Vendor
    FOR r_update_vendor_sites in cur_update_vendor_sites (r_update_vendors.leg_vendor_id,r_update_vendors.org_id) LOOP
        -- Get leg_default_pay_site_id based on interface_tx_id from xxextn schema table
        FOR r_get_DPS_id in cur_get_DPS_id (r_update_vendor_sites.interface_txn_id) LOOP
            ln_update_vendor_site_id := 0;
            lc_leg_vsc := '';
            -- Find out if the vendor_site_id exists in xxconv table and its already converted

          BEGIN
              Select vendor_site_id, leg_vendor_site_code
                INTO ln_update_vendor_site_id, lc_leg_vsc
                from xxconv.xxap_supplier_sites_stg
               where leg_vendor_site_id = r_get_DPS_id.leg_default_pay_site_id  -- (leg_default_pay_site_id from above query)
                 and leg_vendor_id = r_update_vendors.leg_vendor_id -- (leg_vendor_id from 1st query)
                 and process_flag = 'C';

                  print_log_message('------------ Is pay site converted? - Yes: Default Pay Site '||ln_update_vendor_site_id ||' : '||lc_leg_vsc );
          EXCEPTION
            WHEN NO_DATA_FOUND THEN
              print_log_message('------------ Is actual pay site converted? - No: ' );
              --v3.0
                BEGIN
                   SELECT vendor_site_id,vendor_site_code
                   INTO ln_update_vendor_site_id, lc_leg_vsc
                   from xxconv.xxap_supplier_sites_stg
                   where  leg_vendor_id = r_update_vendors.leg_vendor_id
                   AND   org_id = r_update_vendor_sites.org_id
                   AND   process_flag = 'C'
                   AND   vendor_site_Code =r_update_vendor_sites.LEG_DEFAULT_PAY_SITE
                   AND rownum =1;

                  print_log_message('Could Not find leg_default_pay_site_id '||r_update_vendor_sites.leg_default_pay_site||
                  'Updating the record with vendor site id as default '||ln_update_vendor_site_id );
              EXCEPTION
                when others then
                 print_log_message('No Default pay site found that was Successfully Converted ' );
              END;
              --v3.0 ends
            WHEN OTHERS THEN
              print_log_message('In Exception Others when finding if Default Pay Site is converted ');
          END;

          -- If Default_vendor_site exists then call API and update base table with vendor_site_id in above query
             If (ln_update_vendor_site_id <> 0) Then

                FND_GLOBAL.APPS_INITIALIZE(v_user_id,v_resp_id, v_resp_appl_id);
                MO_GLOBAL.INIT ('SQLAP');
                mo_global.set_policy_context ('S' ,r_update_vendor_sites.org_id);
                -- Call API
                -- Define values to be passed to API
                print_log_message('-----------------------------------------------------------------------------');
                print_log_message('--------------- Values to be passed to API ----------------------------------');

                Ln_Vendor_Id := r_update_vendors.vendor_id;
                print_log_message('--------------- Ln_Vendor_Id :'||r_update_vendors.vendor_id);

                Ln_Vendor_site_Id := r_update_vendor_sites.vendor_site_id;
                print_log_message('--------------- Ln_Vendor_site_Id :'||r_update_vendor_sites.vendor_site_id);

                Lrec_Vendor_site_Rec.vendor_id := r_update_vendors.vendor_id;
                print_log_message('--------------- Lrec_Vendor_site_Rec.vendor_id:'||r_update_vendors.vendor_id);

                Lrec_Vendor_site_Rec.vendor_site_id := r_update_vendor_sites.vendor_site_id;
                print_log_message('--------------- Lrec_Vendor_site_Rec.vendor_site_id:'||r_update_vendor_sites.vendor_site_id);

                Lrec_Vendor_site_Rec.org_id := r_update_vendor_sites.org_id;
                print_log_message('--------------- Lrec_Vendor_site_Rec.org_id:'||r_update_vendor_sites.org_id);

                Lrec_Vendor_site_Rec.default_pay_site_id := ln_update_vendor_site_id;
                Lrec_Vendor_site_Rec.pay_site_flag := 'N';
                print_log_message('--------------- Lrec_Vendor_site_Rec.default_pay_site_id:'||ln_update_vendor_site_id);

                print_log_message('-----------------------------------------------------------------------------');
                print_log_message('--------------- Call API ** AP_VENDOR_PUB_PKG.Update_Vendor_Site_public **');

                AP_VENDOR_PUB_PKG.Update_Vendor_Site_public ( p_api_version => 1,--
                                                              x_return_status => lc_return_status,
                                                              x_msg_count => ln_msg_count,
                                                              x_msg_data => ll_msg_data,
                                                              p_vendor_site_rec => Lrec_Vendor_site_Rec,
                                                              p_Vendor_site_Id => Ln_Vendor_site_Id);

                print_log_message('API Return Status:: '||lc_return_status);
                print_log_message('API msg count:: '||ln_msg_count);

                IF (lc_return_status <> 'S') THEN
                  IF ln_msg_count    >= 1 THEN
                    FOR v_index IN 1..ln_msg_count
                    LOOP
                      fnd_msg_pub.get (p_msg_index => v_index, p_encoded => 'F', p_data => ll_msg_data, p_msg_index_out => ln_message_int );
                      Ll_Msg_Data := 'UPDATE_VENDOR_SITE '||SUBSTR(Ll_Msg_Data,1,3900);
                      dbms_output.put_line('Ll_Msg_Data - '||Ll_Msg_Data );
                    END LOOP;
                  End If; -- msg count
                END IF; -- API return status

             END IF; -- IF default vendor site exists and converted

             COMMIT;
        EXIT WHEN cur_get_DPS_id%NOTFOUND;
        END LOOP;

    EXIT WHEN cur_update_vendor_sites%NOTFOUND;
    END LOOP;
EXIT WHEN cur_update_vendors%NOTFOUND;
END LOOP;

  EXCEPTION
    WHEN OTHERS THEN
      l_error_message := SUBSTR('Exception in Procedure Sup_Default_Pay_site. ' ||
                                SQLERRM,
                                1,
                                1999);
      pon_retcode     := 2;
      pov_errbuf      := l_error_message;
      fnd_file.put_line(fnd_file.LOG, l_error_message);
End Sup_Default_Pay_site;


 /*  =============================================================================
   Procedure: Dup_SupplierSite_merge
   =============================================================================
   To check supplier site  line is same then consider that
   record as dupplicate and merge it with first records
   As per technical - First record is active and rest of all blocked.
   */

PROCEDURE Dup_SupplierSite_merge(pov_errbuf  OUT NOCOPY VARCHAR2,
                                 pon_retcode OUT NOCOPY NUMBER,
                                 P_Batch_id IN NUMBER,
                                 P_Request_ID  NUMBER) IS
--v4.0 old cursor commented & added new one
/*Cursor Dup_Site_Blk Is
    Select batch_id,run_sequence_id,request_id,
           leg_vendor_site_code,
           leg_vendor_name,
           leg_vendor_number,
           leg_address_line1,
           org_id
      from xxap_supplier_sites_stg
      where 1=1
        and batch_id = P_Batch_id
        and request_id = P_Request_ID
        and process_flag = g_validated
     Group by batch_id, run_sequence_id,request_id,
              leg_vendor_site_code,
              leg_vendor_name,
              leg_vendor_number,
              leg_address_line1,
              org_id
    Having count(1) > 1
     order by 4;*/
  Cursor Dup_Site_Blk Is
    Select batch_id,run_sequence_id,request_id,
           leg_vendor_site_code,
           leg_vendor_name,
           leg_vendor_number,
           leg_address_line1,
           org_id
      from xxap_supplier_sites_stg
      where org_id is not null
        and process_flag <> 'D'
     Group by batch_id, run_sequence_id,request_id,
              leg_vendor_site_code,
              leg_vendor_name,
              leg_vendor_number,
              leg_address_line1,
              org_id
    Having count(1) > 1;
    --v4.0 change ends

   --v3.0 ADB 09/27/2016 changes start here
   CURSOR upd_flag_cur IS
   SELECT  leg_vendor_number,
           leg_Vendor_Site_code,
           leg_address_line1,
           org_id,
           batch_id,
           sum(DECODE(leg_pay_site_flag,'Y',1,0)) pay_site_count,
           sum(DECODE(leg_purchasing_Site_flag,'Y',1,0)) pur_site_count,
           MAX(leg_default_pay_Site_id) new_default_pay_site
   FROM xxap_supplier_sites_stg
   WHERE batch_id = P_Batch_id
   AND request_id = P_Request_ID
   AND NVL(process_Flag,'#')= 'D'
   GROUP BY   leg_vendor_number,
              leg_Vendor_Site_code,
              leg_address_line1,
              org_id,
              batch_id;
   --v3.0 ADB changes end here

  l_error_message varchar2(1000);

   TYPE Dup_supplier_t IS TABLE OF Dup_Site_Blk%ROWTYPE INDEX BY BINARY_INTEGER;
   l_Dup_supplier_tbl Dup_supplier_t;

Begin
    print_log_message('|---------------------------------------');
    print_log_message('|Program Dup_SupplierSite_merge Starts');
    print_log_message('|---------------------------------------');
--FOR Dup_site_rec IN Dup_Site_Blk LOOP
--v4.0 changes start here
   Declare
       l_stmt  VARCHAR2(2000);
    BEGIN
       l_stmt := 'CREATE INDEX xxconv.xxap_supplier_sites_stg_rnt ON xxconv.xxap_supplier_sites_stg(leg_vendor_number,leg_vendor_site_code,org_id,leg_address_line1)' ;
       EXECUTE IMMEDIATE l_stmt;
       print_log_message ( 'Indexes created On XXAP_SUPPLIER_SITES_STG Table');

       dbms_stats.gather_table_stats(ownname          => 'XXCONV',
                                    tabname          => 'XXAP_SUPPLIER_SITES_STG',
                                    cascade          => true,
                                    estimate_percent => dbms_stats.auto_sample_size,
                                    degree           => dbms_stats.default_degree);
    EXCEPTION
       WHEN OTHERS
          THEN
            print_log_message('Exception creating index/running gather stats');
            print_log_message(SUBSTR(SQLERRM,
                                1,
                                1999));
    END;
--v4.0 changes end here
 BEGIN

  OPEN Dup_Site_Blk;
    LOOP
      FETCH Dup_Site_Blk BULK COLLECT
        INTO l_Dup_supplier_tbl LIMIT 3000;
      EXIT WHEN l_Dup_supplier_tbl.COUNT = 0;
      IF l_Dup_supplier_tbl.COUNT > 0 THEN
        BEGIN
          FORALL indx IN 1 .. l_Dup_supplier_tbl.COUNT SAVE EXCEPTIONS
          --v4.0 changes start commenting the update and adding 2 new updates
            /*Update xxconv.xxap_supplier_sites_stg
                   Set process_flag = 'D'
                   where 1=1
                    and request_id = l_Dup_supplier_tbl(indx).request_id
                    and run_sequence_id = l_Dup_supplier_tbl(indx).run_sequence_id
                    and batch_id =l_Dup_supplier_tbl(indx).batch_id
                    and rowid in (
                    Select rowid from xxap_supplier_sites_stg
                    Where nvl(upper(batch_id), 'dummy') = nvl(upper(l_Dup_supplier_tbl(indx).batch_id), 'dummy')
                                     and nvl(upper(leg_vendor_number), 'dummy') = nvl(upper(l_Dup_supplier_tbl(indx).leg_vendor_number), 'dummy')
                                     and nvl(upper(leg_vendor_site_code), 'dummy') = nvl(upper( l_Dup_supplier_tbl(indx).leg_vendor_site_code), 'dummy')
                                     and nvl(upper(leg_address_line1), 'dummy') = nvl(upper( l_Dup_supplier_tbl(indx).leg_address_line1), 'dummy')
                                     and nvl(upper(org_id), 'dummy') = nvl(upper( l_Dup_supplier_tbl(indx).org_id), 'dummy')--);
                   and rowid not in (
                                  Select min(rowid)
                                    from xxap_supplier_sites_stg
                                     where  nvl(upper(batch_id), 'dummy') = nvl(upper(l_Dup_supplier_tbl(indx).batch_id), 'dummy')
                                     and nvl(upper(leg_vendor_number), 'dummy') = nvl(upper(l_Dup_supplier_tbl(indx).leg_vendor_number), 'dummy')
                                     and nvl(upper(leg_vendor_site_code), 'dummy') = nvl(upper( l_Dup_supplier_tbl(indx).leg_vendor_site_code), 'dummy')
                                     and nvl(upper(leg_address_line1), 'dummy') = nvl(upper( l_Dup_supplier_tbl(indx).leg_address_line1), 'dummy')
                                     and nvl(upper(org_id), 'dummy') = nvl(upper( l_Dup_supplier_tbl(indx).org_id), 'dummy')));*/
             --below update is for all records except the ones where
             --scenario is that there 3 records in total and 2 new records that have org_id populated in current batch
             --one record was already converted in previous batch. union will achieve it

             Update xxconv.xxap_supplier_sites_stg
             Set process_flag = 'D'
             WHERE/* process_flag not in ('C', 'P')
             and*/ batch_id =l_Dup_supplier_tbl(indx).batch_id
             and rowid in (Select rowid from xxap_supplier_sites_stg xss
                           Where nvl(upper(xss.batch_id), 'dummy') = nvl(upper(l_Dup_supplier_tbl(indx).batch_id), 'dummy')
                           and nvl(upper(xss.leg_vendor_number), 'dummy') = nvl(upper(l_Dup_supplier_tbl(indx).leg_vendor_number), 'dummy')
                           and nvl(upper(xss.leg_vendor_site_code), 'dummy') = nvl(upper( l_Dup_supplier_tbl(indx).leg_vendor_site_code), 'dummy')
                           and nvl(upper(xss.leg_address_line1), 'dummy') = nvl(upper( l_Dup_supplier_tbl(indx).leg_address_line1), 'dummy')
                           and nvl(upper(xss.org_id), 'dummy') =  nvl(upper(l_Dup_supplier_tbl(indx).org_id), 'dummy')
                           and xss.process_flag not in ('D','C', 'P')
                           and rowid not in (Select min(rowid)
                                             from xxap_supplier_sites_stg xss1
                                             where  nvl(upper(xss1.batch_id), 'dummy') = nvl(upper(l_Dup_supplier_tbl(indx).batch_id), 'dummy')
                                             and nvl(upper(xss1.leg_vendor_number), 'dummy') = nvl(upper(l_Dup_supplier_tbl(indx).leg_vendor_number), 'dummy')
                                             and nvl(upper(xss1.leg_vendor_site_code), 'dummy') = nvl(upper( l_Dup_supplier_tbl(indx).leg_vendor_site_code), 'dummy')
                                             and nvl(upper(xss1.leg_address_line1), 'dummy') = nvl(upper( l_Dup_supplier_tbl(indx).leg_address_line1), 'dummy')
                                             and nvl(upper(xss1.org_id), 'dummy') = nvl(upper(l_Dup_supplier_tbl(indx).org_id), 'dummy')
                                             and xss1.process_flag not in ('D','C', 'P'))
                            UNION
                             select rowid from xxconv.xxap_supplier_sites_stg xss2
                             where process_flag not in ('C', 'P','D')
                             and nvl(upper(xss2.batch_id), 'dummy') = nvl(upper(l_Dup_supplier_tbl(indx).batch_id), 'dummy')
                             and nvl(upper(xss2.leg_vendor_number), 'dummy') = nvl(upper(l_Dup_supplier_tbl(indx).leg_vendor_number), 'dummy')
                             and nvl(upper(xss2.leg_vendor_site_code), 'dummy') = nvl(upper( l_Dup_supplier_tbl(indx).leg_vendor_site_code), 'dummy')
                             and nvl(upper(xss2.leg_address_line1), 'dummy') = nvl(upper( l_Dup_supplier_tbl(indx).leg_address_line1), 'dummy')
                             and nvl(upper(xss2.org_id), 'dummy') =  nvl(upper(l_Dup_supplier_tbl(indx).org_id), 'dummy')
                             and exists (SELECT 1 from ap_supplier_Sites_int aps
                                         where aps.org_id = xss2.org_id
                                         and aps.vendor_site_Code = xss2.leg_vendor_site_code
                                         and aps.vendor_id = xss2.vendor_id
                                         and aps.address_line1 = xss2.leg_Address_line1
                                         and status IN ('NEW','PROCESSED')));


            --v4.0 ends
            print_log_message('Number of Supplier Merge :- ' || TO_CHAR(SQL%ROWCOUNT));




        EXCEPTION
          WHEN OTHERS THEN
            print_log_message(SUBSTR('Exception in Procedure validate_supplier_contacts while doing Bulk Insert. ' ||
                                     SQLERRM,
                                     1,
                                     2000));
            print_log_message('No. of records in Bulk Exception : ' ||
                              SQL%BULK_EXCEPTIONS.COUNT);
        END;
      END IF;

    END LOOP;
    CLOSE Dup_Site_Blk;


   COMMIT;
   END;

   --v3.0 ADB 09/27/2016 change start here
   FOR upd_site_rec IN upd_flag_cur
   LOOP
      UPDATE xxap_supplier_sites_Stg
      SET leg_pay_Site_flag = DECODE(upd_site_rec.pay_site_count,0,leg_pay_Site_flag, 'Y')
         ,leg_purchasing_site_flag = DECODE(upd_site_rec.pur_site_count,0,leg_purchasing_site_flag, 'Y')
         ,leg_default_pay_site_id  = NVL(leg_default_pay_site_id,upd_site_rec.new_default_pay_site )
      WHERE  nvl(upper(leg_vendor_number), '#') = nvl(upper(upd_site_rec.leg_vendor_number), '#')
      AND nvl(upper(leg_vendor_site_code), '#') = nvl(upper( upd_site_rec.leg_vendor_site_code), '#')
      AND nvl(upper(leg_address_line1), '#') = nvl(upper( upd_site_rec.leg_address_line1), '#')
      AND nvl(upper(org_id), '#') = nvl(upper( upd_site_rec.org_id), '#')
      AND nvl(upper(batch_id), '#')   = p_batch_Id
      --AND request_id = P_Request_ID          --v4.0 it is possible that the record was validated in previous run but another record was marked D in currnt validation
      AND nvl(process_flag,'#') NOT IN ('D','C','X');

   END LOOP;

   COMMIT;

   --v3.0 ADB 09/27/2016 change end here
--v4.0 changes start here
   Declare
       l_stmt1  VARCHAR2(2000);
    BEGIN
       l_stmt1 := 'DROP INDEX xxconv.xxap_supplier_sites_stg_rnt' ;
       EXECUTE IMMEDIATE l_stmt1;
       print_log_message ( 'Indexes Dropped On XXAP_SUPPLIER_SITES_STG Table');

       dbms_stats.gather_table_stats(ownname          => 'XXCONV',
                                    tabname          => 'XXAP_SUPPLIER_SITES_STG',
                                    cascade          => true,
                                    estimate_percent => dbms_stats.auto_sample_size,
                                    degree           => dbms_stats.default_degree);
    EXCEPTION
       WHEN OTHERS
          THEN
            print_log_message('Exception Dropping index/running gather stats');
            print_log_message(SUBSTR(SQLERRM,
                                1,
                                1999));
    END;
--v4.0 changes end here

  EXCEPTION
    WHEN OTHERS THEN
      l_error_message := SUBSTR('Exception in Procedure Dup_SupplierSite_merge. ' ||
                                SQLERRM,
                                1,
                                1999);
      pon_retcode     := 2;
      pov_errbuf      := l_error_message;
      fnd_file.put_line(fnd_file.LOG, l_error_message);
End Dup_SupplierSite_merge;



/* =============================================================================
   Procedure: Dup_Sup_Contact_Merge
   =============================================================================
   To check supplier contacts attach with duplicate site which marked as duplicate sites
   All those contacts attach to site which is active.
   As per technical - All contacts attach to active site.
  -- =============================================================================
  --  Input Parameters :
  --  NA
  --  Output Parameters :
  --    pov_errbuf    : Error message in case of any failure
  --    pon_retcode   : Return Status - Normal/Warning/Error
  -- -----------------------------------------------------------------------------
   */

PROCEDURE Dup_Sup_Contact_Merge(pov_errbuf  OUT NOCOPY VARCHAR2,
                                 pon_retcode OUT NOCOPY NUMBER) IS

Cursor Dup_Cont_cur Is
Select x.leg_vendor_site_id, /*x.leg_vendor_contact_id,*/
       x.interface_txn_id, x.leg_vendor_name,
       x.leg_vendor_site_code
from xxconv./*xxap_supplier_contacts_stg*/ xxap_supplier_sites_stg x
where exists ( Select 1
              from xxconv.xxap_supplier_contacts_stg a
                   where a.process_flag = 'D'
                     and a.leg_vendor_name = x.leg_vendor_name
                     and a.leg_vendor_site_code = x.leg_vendor_site_code
                     and a.leg_source_system = x.leg_source_system)
   and x.process_flag not in ('E','D') ;

 l_error_message varchar2(1000);
 BEGIN

   /* Updating contact to 'D' for all duplicate sites but all this contact attach to active site
     in procedure - Dup_Sup_Contact_Merge */
  BEGIN
    Update xxconv.xxap_supplier_contacts_stg o
     Set process_flag = 'D'
    where leg_vendor_site_id in (Select leg_vendor_site_id
                                        from xxconv.xxap_supplier_sites_stg i
                                   where process_flag= 'D'
                                   AND o.leg_source_system = i.leg_source_system);

      print_log_message('Number of Supplier contact Merge ' || TO_CHAR(SQL%ROWCOUNT));
   COMMIT;
  END;


 Update xxconv.xxap_supplier_contacts_stg
    set leg_old_vendor_site_id = leg_vendor_site_id
   where process_flag = 'D';
  print_log_message('Number of duplicate Supplier contact :- ' || TO_CHAR(SQL%ROWCOUNT));
 COMMIT;


For Dup_Cont_rec in Dup_Cont_cur loop

  update xxconv.xxap_supplier_contacts_stg
    set process_flag = 'N'
       ,leg_vendor_site_id = Dup_Cont_rec.leg_vendor_site_id
   where process_flag = 'D'
     and leg_vendor_name = Dup_Cont_rec.leg_vendor_name
     and leg_vendor_site_code = Dup_Cont_rec.leg_vendor_site_code;

end loop;
 print_log_message('Number of Supplier Contact Merge with sites ' || TO_CHAR(SQL%ROWCOUNT));


COMMIT;

  EXCEPTION
    WHEN OTHERS THEN
      l_error_message := SUBSTR('Exception in Procedure Dup_Sup_Contact_Merge. ' ||
                                SQLERRM,1,1999);
      pon_retcode     := 2;
      pov_errbuf      := l_error_message;
      fnd_file.put_line(fnd_file.LOG, l_error_message);
End Dup_Sup_Contact_Merge;





  PROCEDURE Dup_SupplierSite_Upd(pov_errbuf  OUT NOCOPY VARCHAR2,
                                 pon_retcode OUT NOCOPY NUMBER,
                                 P_Batch_id IN NUMBER,
                                 P_request_id IN NUMBER) IS
    --v4.0 starts here
    --used a new cursor instead of the below old one
    /*cursor Dup_suppliersite_cur is
      Select batch_id,
             leg_vendor_site_code,
             leg_vendor_name,
             leg_vendor_number,
             org_id
        from xxap_supplier_sites_stg
        where process_flag != 'D'
        and batch_id = P_Batch_id
        and request_id = P_request_id
        Group by batch_id, leg_vendor_site_code, leg_vendor_name,leg_vendor_number,org_id
      Having count(1) > 1;*/

      cursor Dup_suppliersite_cur is
      Select batch_id,
             leg_vendor_site_code,
             leg_vendor_name,
             leg_vendor_number,
             org_id, count(distinct leg_address_line1)
        from xxap_supplier_sites_stg
        where process_flag != 'D'
        and org_id is not null
       -- and batch_id = P_Batch_id
        Group by batch_id, leg_vendor_site_code, leg_vendor_name,leg_vendor_number,org_id
      Having count(distinct leg_address_line1) > 1;
   --v4.0 change ends
 Cursor New_Ven_Code is
                Select leg_vendor_id,
                       leg_vendor_site_id,
                       leg_vendor_site_code,
                       Vendor_site_code,
                       leg_org_id,
                       leg_request_id
                  From xxap_supplier_sites_stg
                 Where Vendor_site_code = leg_org_id || '-' ||leg_vendor_site_id
                 /*and process_flag not in ('C','P')*/;
CURSOR upd_flag_cur is
SELECT batch_id,
             leg_vendor_site_code,
             leg_vendor_name,
             leg_vendor_number,
             org_id,
           sum(DECODE(leg_pay_site_flag,'Y',1,0)) pay_site_count,
           sum(DECODE(leg_purchasing_Site_flag,'Y',1,0)) pur_site_count,
           MAX(leg_default_pay_Site_id) new_default_pay_site
   FROM xxap_supplier_sites_stg
   WHERE batch_id = P_Batch_id
   AND request_id = P_request_id
   AND vendor_site_code = leg_org_id || '-' ||leg_vendor_site_id
   GROUP BY   batch_id,
             leg_vendor_site_code,
             leg_vendor_name,
             leg_vendor_number,
             org_id;

    l_error_cnt      number;
    l_vendor_site_id xxap_supplier_sites_stg.Vendor_Site_Id%type;

    l_error_message varchar2(1000);
   l_New_vendor_site_code varchar2(1000);
  BEGIN
    print_log_message('|---------------------------------------');
    print_log_message('|Program Dup_SupplierSite_Create Starts');
    print_log_message('|---------------------------------------');
    print_log_message('P_request_id :-'||P_request_id);
    print_log_message('P_Batch_id :-'||P_Batch_id);
    FOR Dup_suppliersite_rec IN Dup_suppliersite_cur LOOP
      BEGIN

        l_error_cnt := 0;
        IF Dup_suppliersite_rec.leg_vendor_site_code IS NOT NULL AND
           Dup_suppliersite_rec.org_id IS NOT NULL THEN

          validate_vendor_site(Dup_suppliersite_rec.leg_vendor_number,
                               Dup_suppliersite_rec.leg_vendor_site_code,
                               Dup_suppliersite_rec.org_id,
                               l_vendor_site_id,
                               l_error_cnt);

          -- If not null(Site Exists) then l_error_cnt = 0 ,
          -- If null (Site not exists) then l_error_cnt =2

          IF l_error_cnt = 0 THEN
            /* Vendor Site is exists in R12 based table
            Then Skip that Vendor site code to modify
            i.e. l_error_cnt = 0  = NOT NULL  */

            Update xxap_supplier_sites_stg
               Set Vendor_site_code = leg_org_id || '-' ||leg_vendor_site_id
             where rowid in
                   (select "Row_id"
                      from (Select Rowid "Row_id",
                                   batch_id,
                                   leg_vendor_site_code,
                                   leg_vendor_name,
                                   org_id,
                                   leg_org_id,
                                   leg_operating_unit_name,
                                   leg_vendor_site_id,
                                   rank() over(partition by batch_id, leg_vendor_site_code, leg_vendor_name, org_id order by rowid) rank_n
                              from xxap_supplier_sites_stg a
                            /* where exists
                             (select 1
                                      from xxap_supplier_sites_stg xsbs*/
                                     where batch_id =
                                           Dup_suppliersite_rec.batch_id
                                       and leg_vendor_site_code =
                                           Dup_suppliersite_rec.leg_vendor_site_code
                                       and vendor_site_id <> l_vendor_site_id
                                       and org_id =
                                           Dup_suppliersite_rec.org_id
                                       and leg_vendor_name =
                                           Dup_suppliersite_rec.leg_vendor_name
                                       and process_flag <>'D'    ) a);

        print_log_message('New Number of Supplier Site Updated :- ' || TO_CHAR(SQL%ROWCOUNT));

       --v4.0 this is not correct place for the below code
       /* FOR i in New_Ven_Code loop
                -- Update new vendor site code in supplier contact.

         Update xxap_supplier_contacts_stg
            Set vendor_site_code = i.vendor_site_code
          where leg_vendor_site_id = i.leg_vendor_site_id
            and leg_vendor_site_code = i.leg_vendor_site_code
            and leg_request_id = i.leg_request_id;

        -- Update new vendor site code in bank account.
        Update xxap_supplier_bankaccnts_stg
           Set leg_vendor_site_code = i.vendor_site_code
         where leg_vendor_id = i.leg_vendor_id
           and leg_vendor_site_id = i.leg_vendor_site_id
           and leg_vendor_site_code = i.leg_vendor_site_code
           and leg_org_id = i.leg_org_id
           and leg_request_id = i.leg_request_id;

        end loop;*/
        --v4.0 ends

          ELSE
            /* Vendor Site is not exists in R12 based table
            Then Skip only first Vendor Site code and rest of all Vendor site code to be modify
             i.e. l_error_cnt = 2  = NULL  */

            Update xxap_supplier_sites_stg
               Set Vendor_site_code = leg_org_id || '-' ||
                                      leg_vendor_site_id
             where rowid in (select "Row_id"
                               from (Select Rowid "Row_id",
                                            batch_id,
                                            leg_vendor_site_code,
                                            leg_vendor_name,
                                            org_id,
                                            leg_org_id,
                                            leg_operating_unit_name,
                                            leg_vendor_site_id,
                                            rank() over(partition by batch_id, leg_vendor_site_code, leg_vendor_name, org_id order by rowid) rank_n
                                       from xxap_supplier_sites_stg a
                                      /*where exists
                                      (select 1
                                               from xxap_supplier_sites_stg xsbs*/
                                              where batch_id =
                                                    Dup_suppliersite_rec.batch_id
                                                and leg_vendor_site_code =
                                                    Dup_suppliersite_rec.leg_vendor_site_code
                                                and org_id =
                                                    Dup_suppliersite_rec.org_id
                                                and leg_vendor_name =
                                                    Dup_suppliersite_rec.leg_vendor_name
                                                and process_flag <> 'D'    ) a
                              Where rank_n > 1);
      print_log_message('Existing Number of Supplier Site Updated :- ' || TO_CHAR(SQL%ROWCOUNT));
          END IF;

        END IF;
      END;
    END LOOP;

        COMMIT;
        --v4.0 change starts
        FOR i in New_Ven_Code loop
                -- Update new vendor site code in supplier contact.

         Update xxap_supplier_contacts_stg
            Set vendor_site_code = i.vendor_site_code
          where leg_vendor_site_id = i.leg_vendor_site_id
            and leg_vendor_site_code = i.leg_vendor_site_code
            and leg_request_id = i.leg_request_id;

        -- Update new vendor site code in bank account.
        Update xxap_supplier_bankaccnts_stg
           Set leg_vendor_site_code = i.vendor_site_code
         where leg_vendor_id = i.leg_vendor_id
           and leg_vendor_site_id = i.leg_vendor_site_id
           and leg_vendor_site_code = i.leg_vendor_site_code
           and leg_org_id = i.leg_org_id
           and leg_request_id = i.leg_request_id;

        end loop;
        --v4.0 change ends
        --v3.0 ADB 09/27/2016 change start here
   FOR upd_site_rec IN upd_flag_cur
   LOOP
      UPDATE xxap_supplier_sites_Stg
      SET leg_pay_Site_flag = DECODE(upd_site_rec.pay_site_count,0,leg_pay_Site_flag, 'Y')
         ,leg_purchasing_site_flag = DECODE(upd_site_rec.pur_site_count,0,leg_purchasing_site_flag, 'Y')
         ,leg_default_pay_site_id  = NVL(leg_default_pay_site_id,upd_site_rec.new_default_pay_site )
      WHERE  nvl(upper(leg_vendor_number), '#') = nvl(upper(upd_site_rec.leg_vendor_number), '#')
      AND nvl(upper(leg_vendor_site_code), '#') = nvl(upper( upd_site_rec.leg_vendor_site_code), '#')
      AND nvl(upper(org_id), '#') = nvl(upper( upd_site_rec.org_id), '#')
      AND nvl(upper(batch_id), '#')   = p_batch_Id
      AND request_id = P_Request_ID
      AND nvl(process_flag,'#') NOT IN ( 'D','C','X')
      AND leg_vendor_site_Code = vendor_site_code;

   END LOOP;

   COMMIT;

   --v3.0 ADB 09/27/2016 change end here

  EXCEPTION
    WHEN OTHERS THEN
      l_error_message := SUBSTR('Exception in Procedure Dup_SupplierSite_Create. ' ||
                                SQLERRM,
                                1,
                                1999);
      pon_retcode     := 2;
      pov_errbuf      := l_error_message;
      fnd_file.put_line(fnd_file.LOG, l_error_message);
  END Dup_SupplierSite_Upd;

  --
  -- ========================
  -- Procedure: Update_supp_cont_last_name
  -- =============================================================================
  -- This procedure is used to update In case the last name is not populated
  -- in 11i for any contact, then during conversion these contacts must be
  -- converted in R12 with last name as ‘UNKNOWN’, In case there are multiple contacts
  -- under same supplier then suffix with the sequential number 1,2,3. E.g. 3 Contacts
  -- unders same supplier X without last name, must be converted in R12 as 3 contacts:
  -- UNKNOWN,UNKNOWN1,UNKNOWN2
  -- =============================================================================
  --  Input Parameters :
  --    None
  --  Output Parameters :
  --    None
  -- -----------------------------------------------------------------------------

  PROCEDURE Update_supp_cont_last_name(pon_retcode OUT NUMBER) IS
    Cursor Cur_last_name is
     select v.leg_vendor_id,
             vc.leg_vendor_site_id,
             vc.leg_vendor_contact_id,
             vc.leg_vendor_site_code,
             ROW_NUMBER() OVER (PARTITION BY v.leg_vendor_id ORDER BY v.leg_vendor_id ) rn,
             leg_last_name
        from xxap_suppliers_stg         v,
             xxap_supplier_sites_stg    vs,
             xxap_supplier_contacts_stg vc
       where v.leg_vendor_id = vs.leg_vendor_id
         and vs.leg_vendor_site_id = vc.leg_vendor_site_id
         and v.leg_request_id = vs.leg_request_id
         and vs.leg_request_id = vc.leg_request_id
         and vc.leg_last_name is null
      Group by v.leg_vendor_id, vc.leg_vendor_site_id,vc.leg_vendor_contact_id,vc.leg_vendor_site_code,leg_last_name
       order by v.leg_vendor_id, vc.leg_vendor_site_id;

    V_Seq Number;
  BEGIN
    V_Seq := 0;

    FOR i IN Cur_last_name LOOP
      UPDATE xxap_supplier_contacts_stg
         SET leg_last_name = 'UNKNOWN' || decode (to_char(i.rn-1),'0',NULL,to_char(i.rn-1))
       Where leg_vendor_site_id = i.leg_vendor_site_id
         AND leg_vendor_contact_id = i.leg_vendor_contact_id;
      V_Seq := V_Seq + 1;
    END LOOP;

    print_log_message('Update total record . ' || SQl%ROWCOUNT);

    COMMIT;
  EXCEPTION
    WHEN OTHERS THEN
      pon_retcode := g_ret_error;
      print_log_message(SUBSTR('Exception in Procedure Update_supp_cont_last_name. ' ||
                               SQLERRM,
                               1,
                               2000));
  END Update_supp_cont_last_name;
  --
  -- ========================
  -- Procedure: validate_supplier_contacts
  -- =============================================================================
  --   This procedure is used to run validations for Supplier Contacts entity records
  -- =============================================================================
  --  Input Parameters :
  --    None
  --  Output Parameters :
  --    None
  -- -----------------------------------------------------------------------------
  --
  PROCEDURE validate_supplier_contacts(pon_retcode OUT NUMBER) IS
    l_retcode NUMBER;

    l_return_status VARCHAR2(50);
    l_error_msg     VARCHAR2(2000);
    l_site          xxetn_map_unit.site%TYPE;

    l_log_ret_status VARCHAR2(50);
    l_log_err_msg    VARCHAR2(2000);

    CURSOR supplier_cont_cur IS
      SELECT xscs.*,
             (SELECT assi.vendor_interface_id
                FROM xxap_supplier_sites_stg xsss,
                     ap_supplier_sites_int   assi
               WHERE xscs.leg_vendor_name = xsss.leg_vendor_name
                 AND xscs.leg_vendor_site_code = xsss.leg_vendor_site_code
                 AND xscs.leg_operating_unit_name =
                     xsss.leg_operating_unit_name
                 AND xsss.process_flag = g_converted
                 AND xsss.vendor_interface_id = assi.vendor_interface_id
                 AND xsss.vendor_site_interface_id =
                     assi.vendor_site_interface_id
                 AND assi.status = 'PROCESSED') AS vendor_intf_id
        FROM xxap_supplier_contacts_stg xscs
       WHERE xscs.request_id = g_request_id
       ORDER BY xscs.interface_txn_id;

    TYPE supplier_cont_t IS TABLE OF supplier_cont_cur%ROWTYPE INDEX BY BINARY_INTEGER;
    l_supplier_cont_tbl supplier_cont_t;
  BEGIN
    pon_retcode := g_normal;

   /* Active supplier contacts logic date on 05 April 2016
      This logic merge contacts if site is blocked*/
     Dup_Sup_Contact_Merge(l_error_msg, l_retcode);


    -- Initialize global variables for log_errors
    g_source_table    := g_supplier_contacts_t;
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

    --Procedure call to perform mandatory value checks for Supplier Contact entity
    req_val_chk_suppcont(l_retcode);

    --Procedure call to perform duplicate value checks for Supplier Contact entity
    dup_val_chk_suppcont(l_retcode);
    g_src_keyname1  := NULL;
    g_src_keyvalue1 := NULL;
    g_src_keyname2  := NULL;
    g_src_keyvalue2 := NULL;
    g_src_keyname3  := NULL;
    g_src_keyvalue3 := NULL;
    g_src_keyname4  := NULL;
    g_src_keyvalue4 := NULL;
    g_src_keyname5  := NULL;
    g_src_keyvalue5 := NULL;

    OPEN supplier_cont_cur;
    LOOP
      FETCH supplier_cont_cur BULK COLLECT
        INTO l_supplier_cont_tbl LIMIT 1000;
      EXIT WHEN l_supplier_cont_tbl.COUNT = 0;
      IF l_supplier_cont_tbl.COUNT > 0 THEN
        FOR indx IN 1 .. l_supplier_cont_tbl.COUNT LOOP
          g_intf_staging_id := l_supplier_cont_tbl(indx).interface_txn_id;
          l_supplier_cont_tbl(indx).process_flag := g_validated;

          validate_vendor_suppcont(piv_vendor_name => l_supplier_cont_tbl(indx)
                                                     .leg_vendor_name,
                                   pon_vendor_id   => l_supplier_cont_tbl(indx)
                                                     .vendor_id,
                                   pon_retcode     => l_retcode);

          IF l_retcode <> g_normal THEN
            l_supplier_cont_tbl(indx).process_flag := g_error;
          END IF;

          /**
                         validate_continfo_suppcont
                         (  piv_first_name  => l_supplier_cont_tbl(indx).leg_first_name
                         ,  piv_middle_name => l_supplier_cont_tbl(indx).leg_middle_name
                         ,  piv_last_name   => l_supplier_cont_tbl(indx).leg_last_name
                         ,  pon_retcode     => l_retcode
                         );

                         IF l_retcode <> g_normal THEN
                          l_supplier_cont_tbl(indx).process_flag := g_error;
                         END IF;
          **/

          -- Phone
          l_supplier_cont_tbl(indx).phone := l_supplier_cont_tbl(indx)
                                            .leg_phone;

          -- Fax
          l_supplier_cont_tbl(indx).fax := l_supplier_cont_tbl(indx)
                                          .leg_fax;

          l_site := NULL;
          IF l_supplier_cont_tbl(indx)
          .leg_vendor_site_id IS NOT NULL AND l_supplier_cont_tbl(indx)
          .leg_vendor_site_code IS NOT NULL THEN

            -- Fetching Plant# or Site# from Corresponding Site record
            BEGIN
              SELECT DISTINCT xsss.leg_accts_pay_code_segment1    --- To avoid error code ETN_AP_ACCT_PAY_CODE_SEGMENT1_ERROR of too may rows
                INTO l_site
                FROM xxap_supplier_sites_stg xsss
               WHERE xsss.leg_vendor_site_id = l_supplier_cont_tbl(indx).leg_vendor_site_id
                 AND xsss.leg_source_system = l_supplier_cont_tbl(indx).leg_source_system;

            EXCEPTION
              WHEN OTHERS THEN
                l_supplier_cont_tbl(indx).process_flag := g_error;
                log_errors(pov_return_status       => l_log_ret_status -- OUT
                          ,
                           pov_error_msg           => l_log_err_msg -- OUT
                          ,
                           piv_source_column_name  => 'LEG_VENDOR_SITE_CODE',
                           piv_source_column_value => l_supplier_cont_tbl(indx)
                                                     .leg_vendor_site_code,
                           piv_error_type          => g_err_val,
                           piv_error_code          => 'ETN_AP_ACCT_PAY_CODE_SEGMENT1_ERROR',
                           piv_error_message       => 'Error : Acct Pay Code Segment1 is invalid for given Vendor Site Code.Error: ' ||
                                                      SQLERRM);
            END;

          END IF;

          -- If Site value is found
          IF l_site IS NOT NULL THEN
            validate_oper_unit_suppsite(piv_site           => l_site,
                                        pov_operating_unit => l_supplier_cont_tbl(indx)
                                                             .operating_unit_name,
                                        pon_org_id         => l_supplier_cont_tbl(indx)
                                                             .org_id,
                                        pon_retcode        => l_retcode);

            IF l_retcode <> g_normal THEN
              l_supplier_cont_tbl(indx).process_flag := g_error;
            END IF;
          END IF;

          -- Validate Vendor and Vendor Site
          IF l_supplier_cont_tbl(indx).leg_vendor_name IS NOT NULL
         AND l_supplier_cont_tbl(indx).leg_vendor_site_code IS NOT NULL
         AND l_supplier_cont_tbl(indx).org_id IS NOT NULL THEN

            validate_vendor_site_suppcont(piv_vendor_name      => l_supplier_cont_tbl(indx)
                                                                 .leg_vendor_name,
                                          piv_vendor_site_code => l_supplier_cont_tbl(indx)
                                                                 .leg_vendor_site_code,
                                          pin_org_id           => l_supplier_cont_tbl(indx)
                                                                 .org_id,
                                          pin_leg_vendor_site_id =>l_supplier_cont_tbl(indx)
                                                                 .leg_vendor_site_id,        --- To avoid the ETN_AP_INVALID_VENDOR_SITE error of too many rows
                                          pon_vendor_site_id   => l_supplier_cont_tbl(indx)
                                                                 .vendor_site_id,
                                          pov_vendor_site_code => l_supplier_cont_tbl(indx)
                                                                 .vendor_site_code,
                                          pon_retcode          => l_retcode);

            IF l_retcode <> g_normal THEN
              l_supplier_cont_tbl(indx).process_flag := g_error;
            END IF;
          END IF;

          IF l_supplier_cont_tbl(indx).leg_attribute1 IS NOT NULL THEN
            validate_att_suppsite(piv_attribute => 'LEG_ATTRIBUTE1',
                                  piv_value_set => 'XXETN_EATON_PLANT_ID',
                                  piv_value     => l_supplier_cont_tbl(indx)
                                                  .leg_attribute1,
                                  pov_value     => l_supplier_cont_tbl(indx)
                                                  .attribute1,
                                  pon_retcode   => l_retcode);

            IF l_retcode <> g_normal THEN
              l_supplier_cont_tbl(indx).process_flag := g_error;
            END IF;
          END IF;

          IF l_supplier_cont_tbl(indx).leg_attribute2 IS NOT NULL THEN
            validate_att_suppsite(piv_attribute => 'LEG_ATTRIBUTE2',
                                  piv_value_set => 'XXETN_MVT_AREA',
                                  piv_value     => l_supplier_cont_tbl(indx)
                                                  .leg_attribute2,
                                  pov_value     => l_supplier_cont_tbl(indx)
                                                  .attribute2,
                                  pon_retcode   => l_retcode);

            IF l_retcode <> g_normal THEN
              l_supplier_cont_tbl(indx).process_flag := g_error;
            END IF;
          END IF;

          IF l_supplier_cont_tbl(indx).leg_attribute3 IS NOT NULL THEN
            validate_att_suppsite(piv_attribute => 'LEG_ATTRIBUTE3',
                                  piv_value_set => 'XXETN_MVT_PORT',
                                  piv_value     => l_supplier_cont_tbl(indx)
                                                  .leg_attribute3,
                                  pov_value     => l_supplier_cont_tbl(indx)
                                                  .attribute3,
                                  pon_retcode   => l_retcode);

            IF l_retcode <> g_normal THEN
              l_supplier_cont_tbl(indx).process_flag := g_error;
            END IF;
          END IF;

          IF l_supplier_cont_tbl(indx).leg_attribute4 IS NOT NULL THEN
            validate_att_suppsite(piv_attribute => 'LEG_ATTRIBUTE4',
                                  piv_value_set => 'XXETN_MVT_TRANSACTION_NATURE',
                                  piv_value     => l_supplier_cont_tbl(indx)
                                                  .leg_attribute4,
                                  pov_value     => l_supplier_cont_tbl(indx)
                                                  .attribute4,
                                  pon_retcode   => l_retcode);

            IF l_retcode <> g_normal THEN
              l_supplier_cont_tbl(indx).process_flag := g_error;
            END IF;
          END IF;

          IF l_supplier_cont_tbl(indx).leg_attribute5 IS NOT NULL THEN
            validate_att_suppsite(piv_attribute => 'LEG_ATTRIBUTE5',
                                  piv_value_set => 'XXETN_MVT_DELIVERY_TERMS',
                                  piv_value     => l_supplier_cont_tbl(indx)
                                                  .leg_attribute5,
                                  pov_value     => l_supplier_cont_tbl(indx)
                                                  .attribute5,
                                  pon_retcode   => l_retcode);

            IF l_retcode <> g_normal THEN
              l_supplier_cont_tbl(indx).process_flag := g_error;
            END IF;
          END IF;

          IF l_supplier_cont_tbl(indx).leg_attribute6 IS NOT NULL THEN
            validate_att_suppsite(piv_attribute => 'LEG_ATTRIBUTE6',
                                  piv_value_set => 'XXETN_MVT_TRANSACTION_MODE',
                                  piv_value     => l_supplier_cont_tbl(indx)
                                                  .leg_attribute6,
                                  pov_value     => l_supplier_cont_tbl(indx)
                                                  .attribute6,
                                  pon_retcode   => l_retcode);

            IF l_retcode <> g_normal THEN
              l_supplier_cont_tbl(indx).process_flag := g_error;
            END IF;
          END IF;

          IF l_supplier_cont_tbl(indx).leg_attribute7 IS NOT NULL THEN
            validate_att_suppsite(piv_attribute => 'LEG_ATTRIBUTE7',
                                  piv_value_set => 'XXETN_MVT_COMMODITY_CODES',
                                  piv_value     => l_supplier_cont_tbl(indx)
                                                  .leg_attribute7,
                                  pov_value     => l_supplier_cont_tbl(indx)
                                                  .attribute7,
                                  pon_retcode   => l_retcode);

            IF l_retcode <> g_normal THEN
              l_supplier_cont_tbl(indx).process_flag := g_error;
            END IF;
          END IF;

          IF (l_supplier_cont_tbl(indx).process_flag = g_error) THEN
            g_retcode := 1;
          END IF;

        END LOOP;

        BEGIN
          FORALL indx IN 1 .. l_supplier_cont_tbl.COUNT SAVE EXCEPTIONS
            UPDATE xxap_supplier_contacts_stg xscs
               SET xscs.last_updated_date           = SYSDATE,
                   xscs.error_type                  = DECODE(l_supplier_cont_tbl(indx)
                                                             .process_flag,
                                                             g_validated,
                                                             NULL,
                                                             g_error,
                                                             g_err_val),
                   xscs.process_flag                = l_supplier_cont_tbl(indx)
                                                     .process_flag,
                   xscs.vendor_id                   = l_supplier_cont_tbl(indx)
                                                     .vendor_id,
                   xscs.vendor_site_id              = l_supplier_cont_tbl(indx)
                                                     .vendor_site_id,
                   xscs.vendor_site_code            = l_supplier_cont_tbl(indx)
                                                     .vendor_site_code,
                   xscs.phone                       = l_supplier_cont_tbl(indx)
                                                     .phone,
                   xscs.fax                         = l_supplier_cont_tbl(indx).fax,
                   xscs.org_id                      = l_supplier_cont_tbl(indx)
                                                     .org_id,
                   xscs.operating_unit_name         = l_supplier_cont_tbl(indx)
                                                     .operating_unit_name,
                   xscs.vendor_name                 = l_supplier_cont_tbl(indx)
                                                     .leg_vendor_name,
                   xscs.vendor_interface_id         = l_supplier_cont_tbl(indx)
                                                     .vendor_intf_id,
                   xscs.vendor_contact_interface_id = DECODE(l_supplier_cont_tbl(indx)
                                                             .process_flag,
                                                             g_validated,
                                                             ap_sup_site_contact_int_s.NEXTVAL,
                                                             g_error,
                                                             NULL)
             WHERE xscs.interface_txn_id = l_supplier_cont_tbl(indx)
            .interface_txn_id;
        EXCEPTION
          WHEN OTHERS THEN
            print_log_message(SUBSTR('Exception in Procedure validate_supplier_contacts while doing Bulk Insert. ' ||
                                     SQLERRM,
                                     1,
                                     2000));
            print_log_message('No. of records in Bulk Exception : ' ||
                              SQL%BULK_EXCEPTIONS.COUNT);
        END;
      END IF;

    END LOOP;
    CLOSE supplier_cont_cur;

    -- Insert remaining records in Error Table
    IF g_source_tab.COUNT > 0 THEN
      xxetn_common_error_pkg.add_error(pov_return_status => l_return_status,
                                       pov_error_msg     => l_error_msg,
                                       pi_source_tab     => g_source_tab,
                                       pin_batch_id      => g_new_batch_id);
      g_source_tab.DELETE;
      g_indx := 0;
    END IF;

  EXCEPTION
    WHEN OTHERS THEN
      pon_retcode := g_ret_error;
      print_log_message(SUBSTR('Exception in Procedure validate_supplier_contacts. ' ||
                               SQLERRM,
                               1,
                               2000));
  END validate_supplier_contacts;

  --
  -- ========================
  -- Procedure: validate_supplier_sites
  -- =============================================================================
  --   This procedure is used to run validations for Supplier entity records
  -- =============================================================================
  --  Input Parameters :
  --    None
  --  Output Parameters :
  --    None
  -- -----------------------------------------------------------------------------
  --
  PROCEDURE validate_supplier_sites(pon_retcode OUT NUMBER) IS
    l_retcode NUMBER;

    l_return_status  VARCHAR2(50);
    l_error_msg      VARCHAR2(2000);
    l_localization   VARCHAR2(30);
    l_tax_flag_count number;
    CURSOR supplier_site_cur IS
      SELECT xsss.*,
             (SELECT xss.vendor_interface_id
                FROM xxap_suppliers_stg xss, ap_suppliers_int asi
               WHERE xsss.leg_vendor_name = xss.leg_vendor_name
                 AND xss.process_flag = g_converted
                 AND xss.leg_vendor_name = asi.vendor_name
                 AND xss.leg_segment1 = xsss.leg_vendor_number
                 AND xss.leg_segment1 = asi.segment1
                 AND xss.vendor_interface_id = asi.vendor_interface_id
                 AND asi.status = 'PROCESSED') AS vendor_intf_id
        FROM xxap_supplier_sites_stg xsss
       WHERE xsss.request_id = g_request_id
       and xsss.process_flag <> g_obsolete
       and xsss.process_flag = g_new      -- 07 Jan 2016 data fix
       ORDER BY xsss.interface_txn_id;

    TYPE supplier_site_t IS TABLE OF supplier_site_cur%ROWTYPE INDEX BY BINARY_INTEGER;
    l_supplier_site_tbl supplier_site_t;
  BEGIN
    pon_retcode := g_normal;

    -- Initialize global variables for log_errors
    print_log_message('Initializing common error utility global variables for entity ' ||
                      g_supplier_sites);
    g_source_table    := g_supplier_sites_t;
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





    --Procedure call to perform mandatory value checks for Supplier entity
    print_log_message('Performing Supplier site required value checks');
    req_val_chk_suppsite(l_retcode);

    --Procedure call to perform duplicate value checks for Supplier entity
    print_log_message('Performing Supplier site duplicate records check');
    dup_val_chk_suppsite(l_retcode);

    OPEN supplier_site_cur;
    LOOP
      FETCH supplier_site_cur BULK COLLECT
        INTO l_supplier_site_tbl LIMIT 1000;
      print_log_message('l_supplier_site_tbl.COUNT ' ||
                        l_supplier_site_tbl.COUNT);
      EXIT WHEN l_supplier_site_tbl.COUNT = 0;
      IF l_supplier_site_tbl.COUNT > 0 THEN
        FOR indx IN 1 .. l_supplier_site_tbl.COUNT LOOP
          g_intf_staging_id := l_supplier_site_tbl(indx).interface_txn_id;
          l_supplier_site_tbl(indx).process_flag := g_validated;

          validate_loc_dtls_supp(piv_ship_to_loc_code => l_supplier_site_tbl(indx)
                                                        .leg_ship_to_location_code,
                                 piv_bill_to_loc_code => l_supplier_site_tbl(indx)
                                                        .leg_bill_to_location_code,
                                 pon_ship_to_loc_id   => l_supplier_site_tbl(indx)
                                                        .ship_to_location_id,
                                 pon_bill_to_loc_id   => l_supplier_site_tbl(indx)
                                                        .bill_to_location_id,
                                 pon_retcode          => l_retcode);

          IF l_retcode <> g_normal THEN
            l_supplier_site_tbl(indx).process_flag := g_error;
          END IF;

          IF l_supplier_site_tbl(indx).leg_terms_name IS NOT NULL THEN
            validate_pymt_term_suppsite(piv_lookup_code => l_supplier_site_tbl(indx)
                                                          .leg_terms_name,
                                        pov_lookup_code => l_supplier_site_tbl(indx)
                                                          .terms_name,
                                        pon_terms_id    => l_supplier_site_tbl(indx)
                                                          .terms_id,
                                        pon_retcode     => l_retcode);

            IF l_retcode <> g_normal THEN
              l_supplier_site_tbl(indx).process_flag := g_error;
            END IF;
          END IF;

          -- If Site value is Not NULL
          IF l_supplier_site_tbl(indx).leg_accts_pay_code_segment1 IS NOT NULL THEN
            validate_oper_unit_suppsite(piv_site           => l_supplier_site_tbl(indx)
                                                             .leg_accts_pay_code_segment1,
                                        pov_operating_unit => l_supplier_site_tbl(indx)
                                                             .operating_unit_name,
                                        pon_org_id         => l_supplier_site_tbl(indx)
                                                             .org_id,
                                        pon_retcode        => l_retcode);

            l_localization := NULL;

            IF l_retcode <> g_normal THEN
              l_supplier_site_tbl(indx).process_flag := g_error;
            ELSE
              -- Added for supplier localization  v1.4
              BEGIN
                SELECT attribute4 -- territory_code
                  INTO l_localization
                  FROM fnd_lookup_values_vl flvv
                 WHERE lookup_type = 'XXETN_PLANT_COUNTRY_LOC_MAP'
                   AND attribute5 = 'Y'
                   AND attribute1 = l_supplier_site_tbl(indx)
                .leg_accts_pay_code_segment1
                   AND enabled_flag = 'Y'
                   AND SYSDATE BETWEEN NVL(flvv.start_date_active, SYSDATE) AND
                       NVL(flvv.end_date_active, SYSDATE);
              EXCEPTION
                WHEN OTHERS THEN
                  l_localization := NULL;
              END;
              l_supplier_site_tbl(indx).leg_global_attribute18 := l_localization;
            END IF;

            IF l_supplier_site_tbl(indx).leg_country IS NOT NULL THEN
              validate_country_suppsite(piv_lookup_code => l_supplier_site_tbl(indx)
                                                          .leg_country,
                                        pon_retcode     => l_retcode);

              IF l_retcode <> g_normal THEN
                l_supplier_site_tbl(indx).process_flag := g_error;
              END IF;
        --ADB Change 09/27/2016
         /* ELSE
               \* If country code is null then error our *\
                l_supplier_site_tbl(indx).process_flag := g_error;*/
         --ADB changes ends
            END IF;

            IF l_supplier_site_tbl(indx).leg_shipping_control IS NOT NULL THEN
              validate_shpng_cntrl_suppsite(piv_lookup_code => l_supplier_site_tbl(indx)
                                                              .leg_shipping_control,
                                            pon_retcode     => l_retcode);

              IF l_retcode <> g_normal THEN
                l_supplier_site_tbl(indx).process_flag := g_error;
              END IF;
            END IF;

            IF l_supplier_site_tbl(indx).leg_terms_date_basis IS NOT NULL THEN
              validate_date_basis_suppsite(piv_lookup_code => l_supplier_site_tbl(indx)
                                                             .leg_terms_date_basis,
                                           pov_lookup_code => l_supplier_site_tbl(indx)
                                                             .terms_date_basis,
                                           pon_retcode     => l_retcode);

              IF l_retcode <> g_normal THEN
                l_supplier_site_tbl(indx).process_flag := g_error;
              END IF;
            END IF;

            IF l_supplier_site_tbl(indx).leg_payment_priority IS NOT NULL THEN
              validate_pymtpriority_suppsite(pin_payment_priority => l_supplier_site_tbl(indx)
                                                                    .leg_payment_priority,
                                             pon_payment_priority => l_supplier_site_tbl(indx)
                                                                    .payment_priority,
                                             pon_retcode          => l_retcode);

              IF l_retcode <> g_normal THEN
                l_supplier_site_tbl(indx).process_flag := g_error;
              END IF;
            END IF;

            IF l_supplier_site_tbl(indx)
            .org_id IS NOT NULL AND l_supplier_site_tbl(indx)
            .leg_ship_via_lookup_code IS NOT NULL THEN
              validate_ship_via_suppsite(piv_lookup_code => l_supplier_site_tbl(indx)
                                                           .leg_ship_via_lookup_code,
                                         pin_org_id      => l_supplier_site_tbl(indx)
                                                           .org_id,
                                         pov_lookup_code => l_supplier_site_tbl(indx)
                                                           .ship_via_lookup_code,
                                         pon_retcode     => l_retcode);

              IF l_retcode <> g_normal THEN
                l_supplier_site_tbl(indx).process_flag := g_error;
              END IF;
            END IF;

            IF l_supplier_site_tbl(indx).leg_address_style IS NOT NULL THEN
              validate_addrs_style_suppsite(piv_lookup_code => l_supplier_site_tbl(indx)
                                                              .leg_address_style,
                                            pon_retcode     => l_retcode);

              IF l_retcode <> g_normal THEN
                l_supplier_site_tbl(indx).process_flag := g_error;
              END IF;
            END IF;

            IF l_supplier_site_tbl(indx)
            .leg_payment_method_code IS NOT NULL THEN
              validate_pymt_method_supp(piv_lookup_code => l_supplier_site_tbl(indx)
                                                          .leg_payment_method_code,
                                        pov_lookup_code => l_supplier_site_tbl(indx)
                                                          .payment_method_lookup_code,
                                        pon_retcode     => l_retcode);

              IF l_retcode <> g_normal THEN
                l_supplier_site_tbl(indx).process_flag := g_error;
              END IF;
            END IF;

            IF l_supplier_site_tbl(indx).leg_language IS NOT NULL THEN
              validate_site_lang_suppsite(piv_language => l_supplier_site_tbl(indx)
                                                         .leg_language,
                                          pon_retcode  => l_retcode);

              IF l_retcode <> g_normal THEN
                l_supplier_site_tbl(indx).process_flag := g_error;
              END IF;
            END IF;

            IF l_supplier_site_tbl(indx).leg_pay_on_code IS NOT NULL THEN
              validate_pay_on_code_suppsite(piv_lookup_code => l_supplier_site_tbl(indx)
                                                              .leg_pay_on_code,
                                            pov_lookup_code => l_supplier_site_tbl(indx)
                                                              .pay_on_code,
                                            pon_retcode     => l_retcode);

              IF l_retcode <> g_normal THEN
                l_supplier_site_tbl(indx).process_flag := g_error;
              END IF;
            END IF;

            IF l_supplier_site_tbl(indx)
            .leg_pay_on_receipt_sum_code IS NOT NULL THEN
              validate_pay_on_recpt_suppsite(pin_pay_on_receipt_code => l_supplier_site_tbl(indx)
                                                                       .leg_pay_on_receipt_sum_code,
                                             pon_pay_on_receipt_code => l_supplier_site_tbl(indx)
                                                                       .pay_on_receipt_summary_code,
                                             pon_retcode             => l_retcode);

              IF l_retcode <> g_normal THEN
                l_supplier_site_tbl(indx).process_flag := g_error;
              END IF;
            END IF;

            IF l_supplier_site_tbl(indx)
            .leg_supplier_notif_method IS NOT NULL THEN
              validate_supp_notif_suppsite(pin_supp_notif_code => l_supplier_site_tbl(indx)
                                                                 .leg_supplier_notif_method,
                                           pon_retcode         => l_retcode);

              IF l_retcode <> g_normal THEN
                l_supplier_site_tbl(indx).process_flag := g_error;
              END IF;
            END IF;

            IF l_supplier_site_tbl(indx).leg_match_option IS NOT NULL THEN
              validate_invmtchoptn_suppsite(piv_lookup_code => l_supplier_site_tbl(indx)
                                                              .leg_match_option,
                                            pon_retcode     => l_retcode);

              IF l_retcode <> g_normal THEN
                l_supplier_site_tbl(indx).process_flag := g_error;
              END IF;
            END IF;

            IF l_supplier_site_tbl(indx)
            .leg_pay_date_basis_code IS NOT NULL THEN
              validate_pay_date_basis_supp(piv_lookup_code => l_supplier_site_tbl(indx)
                                                             .leg_pay_date_basis_code,
                                           pov_lookup_code => l_supplier_site_tbl(indx)
                                                             .pay_date_basis_lookup_code,
                                           pon_retcode     => l_retcode);

              IF l_retcode <> g_normal THEN
                l_supplier_site_tbl(indx).process_flag := g_error;
              END IF;
            END IF;

            --Validation for Bank Charge Bearer
            IF l_supplier_site_tbl(indx).leg_bank_charge_bearer IS NOT NULL THEN
              validate_bank_bearer_supp(piv_lookup_code => l_supplier_site_tbl(indx)
                                                          .leg_bank_charge_bearer,
                                        pov_lookup_code => l_supplier_site_tbl(indx)
                                                          .leg_bank_charge_bearer,
                                        pon_retcode     => l_retcode);

              IF l_retcode <> g_normal THEN
                l_supplier_site_tbl(indx).process_flag := g_error;
              END IF;
            END IF;

            IF l_supplier_site_tbl(indx)
            .leg_accts_pay_code_segment1 || l_supplier_site_tbl(indx)
            .leg_accts_pay_code_segment2 || l_supplier_site_tbl(indx)
            .leg_accts_pay_code_segment3 || l_supplier_site_tbl(indx)
            .leg_accts_pay_code_segment4 || l_supplier_site_tbl(indx)
            .leg_accts_pay_code_segment5 || l_supplier_site_tbl(indx)
            .leg_accts_pay_code_segment6 || l_supplier_site_tbl(indx)
            .leg_accts_pay_code_segment7 IS NOT NULL THEN
              validate_coa_supp(piv_segment1    => l_supplier_site_tbl(indx)
                                                  .leg_accts_pay_code_segment1,
                                piv_segment2    => l_supplier_site_tbl(indx)
                                                  .leg_accts_pay_code_segment2,
                                piv_segment3    => l_supplier_site_tbl(indx)
                                                  .leg_accts_pay_code_segment3,
                                piv_segment4    => l_supplier_site_tbl(indx)
                                                  .leg_accts_pay_code_segment4,
                                piv_segment5    => l_supplier_site_tbl(indx)
                                                  .leg_accts_pay_code_segment5,
                                piv_segment6    => l_supplier_site_tbl(indx)
                                                  .leg_accts_pay_code_segment6,
                                piv_segment7    => l_supplier_site_tbl(indx)
                                                  .leg_accts_pay_code_segment7,
                                piv_column_name => 'LEG_ACCTS_PAY_CODE_SEGMENTS',
                                pon_cc_id       => l_supplier_site_tbl(indx)
                                                  .accts_pay_code_combination_id,
                                pon_retcode     => l_retcode);

              IF l_retcode <> g_normal THEN
                l_supplier_site_tbl(indx).process_flag := g_error;
              END IF;
            END IF;

            IF l_supplier_site_tbl(indx)
            .leg_prepay_code_segment1 || l_supplier_site_tbl(indx)
            .leg_prepay_code_segment2 || l_supplier_site_tbl(indx)
            .leg_prepay_code_segment3 || l_supplier_site_tbl(indx)
            .leg_prepay_code_segment4 || l_supplier_site_tbl(indx)
            .leg_prepay_code_segment5 || l_supplier_site_tbl(indx)
            .leg_prepay_code_segment6 || l_supplier_site_tbl(indx)
            .leg_prepay_code_segment7 IS NOT NULL THEN
              validate_coa_supp(piv_segment1    => l_supplier_site_tbl(indx)
                                                  .leg_prepay_code_segment1,
                                piv_segment2    => l_supplier_site_tbl(indx)
                                                  .leg_prepay_code_segment2,
                                piv_segment3    => l_supplier_site_tbl(indx)
                                                  .leg_prepay_code_segment3,
                                piv_segment4    => l_supplier_site_tbl(indx)
                                                  .leg_prepay_code_segment4,
                                piv_segment5    => l_supplier_site_tbl(indx)
                                                  .leg_prepay_code_segment5,
                                piv_segment6    => l_supplier_site_tbl(indx)
                                                  .leg_prepay_code_segment6,
                                piv_segment7    => l_supplier_site_tbl(indx)
                                                  .leg_prepay_code_segment7,
                                piv_column_name => 'LEG_PREPAY_CODE_SEGMENTS',
                                pon_cc_id       => l_supplier_site_tbl(indx)
                                                  .prepay_code_combination_id,
                                pon_retcode     => l_retcode);

              IF l_retcode <> g_normal THEN
                l_supplier_site_tbl(indx).process_flag := g_error;
              END IF;
            END IF;

            IF l_supplier_site_tbl(indx)
            .leg_future_dated_segment1 || l_supplier_site_tbl(indx)
            .leg_future_dated_segment2 || l_supplier_site_tbl(indx)
            .leg_future_dated_segment3 || l_supplier_site_tbl(indx)
            .leg_future_dated_segment4 || l_supplier_site_tbl(indx)
            .leg_future_dated_segment5 || l_supplier_site_tbl(indx)
            .leg_future_dated_segment6 || l_supplier_site_tbl(indx)
            .leg_future_dated_segment7 IS NOT NULL THEN
              validate_coa_supp(piv_segment1    => l_supplier_site_tbl(indx)
                                                  .leg_future_dated_segment1,
                                piv_segment2    => l_supplier_site_tbl(indx)
                                                  .leg_future_dated_segment2,
                                piv_segment3    => l_supplier_site_tbl(indx)
                                                  .leg_future_dated_segment3,
                                piv_segment4    => l_supplier_site_tbl(indx)
                                                  .leg_future_dated_segment4,
                                piv_segment5    => l_supplier_site_tbl(indx)
                                                  .leg_future_dated_segment5,
                                piv_segment6    => l_supplier_site_tbl(indx)
                                                  .leg_future_dated_segment6,
                                piv_segment7    => l_supplier_site_tbl(indx)
                                                  .leg_future_dated_segment7,
                                piv_column_name => 'LEG_FUTURE_DATED_SEGMENTS',
                                pon_cc_id       => l_supplier_site_tbl(indx)
                                                  .future_dated_payment_ccid,
                                pon_retcode     => l_retcode);

              IF l_retcode <> g_normal THEN
                l_supplier_site_tbl(indx).process_flag := g_error;
              END IF;
            END IF;

            IF l_supplier_site_tbl(indx).leg_awt_group_name IS NOT NULL THEN
              validate_awt_grp_supp(piv_group_name => l_supplier_site_tbl(indx)
                                                     .leg_awt_group_name,
                                    pon_group_id   => l_supplier_site_tbl(indx)
                                                     .awt_group_id,
                                    pov_group_name => l_supplier_site_tbl(indx)
                                                     .awt_group_name,
                                    pon_retcode    => l_retcode);

              IF l_retcode <> g_normal THEN
                l_supplier_site_tbl(indx).process_flag := g_error;
              END IF;
            END IF;

            IF l_supplier_site_tbl(indx)
            .leg_invoice_currency_code IS NOT NULL THEN
              validate_currency_code_supp(piv_currency_code => l_supplier_site_tbl(indx)
                                                              .leg_invoice_currency_code,
                                          piv_code_type     => 'LEG_INVOICE_CURRENCY_CODE',
                                          pon_retcode       => l_retcode);

              IF l_retcode <> g_normal THEN
                l_supplier_site_tbl(indx).process_flag := g_error;
              END IF;
            END IF;

            IF l_supplier_site_tbl(indx)
            .leg_payment_currency_code IS NOT NULL THEN
              validate_currency_code_supp(piv_currency_code => l_supplier_site_tbl(indx)
                                                              .leg_payment_currency_code,
                                          piv_code_type     => 'LEG_PAYMENT_CURRENCY_CODE',
                                          pon_retcode       => l_retcode);

              IF l_retcode <> g_normal THEN
                l_supplier_site_tbl(indx).process_flag := g_error;
              END IF;
            END IF;

            IF l_supplier_site_tbl(indx)
            .org_id IS NOT NULL AND l_supplier_site_tbl(indx)
            .leg_distribution_set_name IS NOT NULL THEN
              validate_dist_set_suppsite(piv_dist_set_name => l_supplier_site_tbl(indx)
                                                             .leg_distribution_set_name,
                                         pin_org_id        => l_supplier_site_tbl(indx)
                                                             .org_id,
                                         pon_dist_set_id   => l_supplier_site_tbl(indx)
                                                             .distribution_set_id,
                                         pov_dist_set_name => l_supplier_site_tbl(indx)
                                                             .distribution_set_name,
                                         pon_retcode       => l_retcode);

              --v3.0 ADB 09/27/2016 change start
              /*IF l_retcode <> g_normal THEN
                l_supplier_site_tbl(indx).process_flag := g_error;
              END IF;*/
               --v3.0 ADB 09/27/2016 change start
            END IF;

            IF l_supplier_site_tbl(indx).leg_tolerance_name IS NOT NULL THEN
              validate_tolrnce_tmp_suppsite(piv_tolerance_name => l_supplier_site_tbl(indx)
                                                                 .leg_tolerance_name,
                                            pon_tolerance_id   => l_supplier_site_tbl(indx)
                                                                 .tolerance_id,
                                            pon_retcode        => l_retcode);

              IF l_retcode <> g_normal THEN
                l_supplier_site_tbl(indx).process_flag := g_error;
              END IF;
            END IF;

         IF l_supplier_site_tbl(indx).leg_freight_terms_code IS NOT NULL THEN
            validate_freight_term_supp(piv_lookup_code => l_supplier_site_tbl(indx)
                                                         .leg_freight_terms_code,
                                       pov_lookup_code => l_supplier_site_tbl(indx)
                                                         .freight_terms_lookup_code,
                                       pon_retcode     => l_retcode);

            IF l_retcode <> g_normal THEN
              l_supplier_site_tbl(indx).process_flag := g_error;
            END IF;
          END IF;

            IF l_supplier_site_tbl(indx).leg_fob_lookup_code IS NOT NULL THEN
              validate_fob_code_supp(piv_lookup_code => l_supplier_site_tbl(indx)
                                                       .leg_fob_lookup_code,
                                     pov_lookup_code => l_supplier_site_tbl(indx)
                                                       .fob_lookup_code,
                                     pon_retcode     => l_retcode);

              IF l_retcode <> g_normal THEN
                l_supplier_site_tbl(indx).process_flag := g_error;
              END IF;
            END IF;

            IF l_supplier_site_tbl(indx)
            .leg_pay_group_lookup_code IS NOT NULL THEN
              validate_pay_group_supp(piv_lookup_code => l_supplier_site_tbl(indx)
                                                        .leg_pay_group_lookup_code,
                                      pov_lookup_code => l_supplier_site_tbl(indx)
                                                        .pay_group_lookup_code,
                                      pon_retcode     => l_retcode);

              IF l_retcode <> g_normal THEN
                l_supplier_site_tbl(indx).process_flag := g_error;
              END IF;
            END IF;

            IF l_supplier_site_tbl(indx).leg_auto_tax_calc_flag IS NOT NULL THEN
              validate_autotaxc_flg_suppsite(piv_lookup_code => l_supplier_site_tbl(indx)
                                                               .leg_auto_tax_calc_flag,
                                             pon_retcode     => l_retcode);

              IF l_retcode <> g_normal THEN
                l_supplier_site_tbl(indx).process_flag := g_error;
              END IF;

                --change 07/22/2015 --- CV 40 page # 25 - Change by Goutam on 7/16
              IF l_supplier_site_tbl(indx)
              .leg_auto_tax_calc_flag = 'T' OR l_supplier_site_tbl(indx)
              .leg_auto_tax_calc_flag = 'L' THEN
              -- print_log_message (l_supplier_site_tbl(indx).leg_vendor_name|| '-' ||l_supplier_site_tbl(indx).leg_vendor_id);
                l_supplier_site_tbl(indx).leg_auto_tax_calc_flag := 'N';
              END IF;

               --change -7/22/2015 ends

            END IF;

            IF l_supplier_site_tbl(indx).leg_vendor_number IS NOT NULL THEN
              validate_vendor_suppsite(piv_vendor_number => l_supplier_site_tbl(indx)
                                                           .leg_vendor_number,
                                       pon_vendor_id     => l_supplier_site_tbl(indx)
                                                           .vendor_id,
                                       pon_retcode       => l_retcode);

              IF l_retcode <> g_normal THEN
                l_supplier_site_tbl(indx).process_flag := g_error;
              END IF;


            END IF;

            IF l_supplier_site_tbl(indx).leg_attribute2 IS NOT NULL THEN
              validate_att_suppsite(piv_attribute => 'LEG_ATTRIBUTE2',
                                    piv_value_set => 'XXETN_EAP_NAICS',
                                    piv_value     => l_supplier_site_tbl(indx)
                                                    .leg_attribute2,
                                    pov_value     => l_supplier_site_tbl(indx)
                                                    .attribute2,
                                    pon_retcode   => l_retcode);

              IF l_retcode <> g_normal THEN
                l_supplier_site_tbl(indx).process_flag := g_error;
              END IF;
            END IF;

            IF l_supplier_site_tbl(indx)
            .leg_attribute3 IS NOT NULL AND l_supplier_site_tbl(indx)
            .leg_source_system = g_source_fsc THEN
              validate_att_suppsite(piv_attribute => 'LEG_ATTRIBUTE3',
                                    piv_value_set => 'XXETN_ACH_FORMAT_CODES',
                                    piv_value     => l_supplier_site_tbl(indx)
                                                    .leg_attribute3,
                                    pov_value     => l_supplier_site_tbl(indx)
                                                    .attribute3,
                                    pon_retcode   => l_retcode);

              IF l_retcode <> g_normal THEN
                l_supplier_site_tbl(indx).process_flag := g_error;
              END IF;
            END IF;
            --defect 12887 blocking the mapping to attribute4 of R12 from attr4 of fsc 11i
            /*IF l_supplier_site_tbl(indx).leg_attribute4 IS NOT NULL
              AND l_supplier_site_tbl(indx).leg_source_system = g_source_fsc THEN
              validate_att_suppsite(piv_attribute => 'LEG_ATTRIBUTE4',
                                    piv_value_set => 'AP_SRS_YES_NO_MAND',
                                    piv_value     => l_supplier_site_tbl(indx)
                                                    .leg_attribute4,
                                    pov_value     => l_supplier_site_tbl(indx)
                                                    .attribute4,
                                    pon_retcode   => l_retcode);

              IF l_retcode <> g_normal THEN
                l_supplier_site_tbl(indx).process_flag := g_error;
              END IF;
            END IF;*/

            IF l_supplier_site_tbl(indx).leg_attribute6 IS NOT NULL THEN
              validate_att_suppsite(piv_attribute => 'LEG_ATTRIBUTE6',
                                    piv_value_set => 'XXETN_SUP_ADD_RSN',
                                    piv_value     => l_supplier_site_tbl(indx)
                                                    .leg_attribute6,
                                    pov_value     => l_supplier_site_tbl(indx)
                                                    .attribute6,
                                    pon_retcode   => l_retcode);

              IF l_retcode <> g_normal THEN
                l_supplier_site_tbl(indx).process_flag := g_error;
              END IF;
            END IF;

            /**
                           IF l_supplier_site_tbl(indx).leg_attribute7 IS NOT NULL THEN
                              validate_att_suppsite
                              (  piv_attribute => 'LEG_ATTRIBUTE7'
                              ,  piv_value_set => 'AP_SRS_YES_NO_MAND'
                              ,  piv_value     => l_supplier_site_tbl(indx).leg_attribute7
                              ,  pov_value     => l_supplier_site_tbl(indx).attribute7
                              ,  pon_retcode   => l_retcode
                              );

                              IF l_retcode <> g_normal THEN
                               l_supplier_site_tbl(indx).process_flag := g_error;
                              END IF;
                           END IF;
            **/

            /**
                           IF l_supplier_site_tbl(indx).leg_attribute8 IS NOT NULL AND l_supplier_site_tbl(indx).leg_source_system = g_source_issc THEN
                              validate_att_suppsite
                              (  piv_attribute => 'LEG_ATTRIBUTE8'
                              ,  piv_value_set => '13 Characters'
                              ,  piv_value     => l_supplier_site_tbl(indx).leg_attribute8
                              ,  pov_value     => l_supplier_site_tbl(indx).attribute8
                              ,  pon_retcode   => l_retcode
                              );

                              IF l_retcode <> g_normal THEN
                               l_supplier_site_tbl(indx).process_flag := g_error;
                              END IF;
                           END IF;
            **/
            --ISSC ADB 09/27/2016
            IF l_supplier_site_tbl(indx).leg_attribute9 IS NOT NULL
                  AND l_supplier_site_tbl(indx).leg_source_system = g_source_issc THEN
              validate_att_suppsite(piv_attribute => 'LEG_ATTRIBUTE9',
                                    piv_value_set => 'XXAP_PO_NONPO_MAPPING',
                                    piv_value     => l_supplier_site_tbl(indx).leg_attribute9,
                                    pov_value     => l_supplier_site_tbl(indx).attribute1,
                                    pon_retcode   => l_retcode);

              IF l_retcode <> g_normal THEN
                l_supplier_site_tbl(indx).process_flag := g_error;
              END IF;
            END IF;
            --ADB Changes ends
            -- FSC
            IF l_supplier_site_tbl(indx)
            .leg_attribute10 IS NOT NULL AND l_supplier_site_tbl(indx)
            .leg_source_system = g_source_fsc THEN
              validate_att_suppsite(piv_attribute => 'LEG_ATTRIBUTE10',
                                    piv_value_set => 'XXETN_PLANT_LEDGER_NUMBER',
                                    piv_value     => l_supplier_site_tbl(indx)
                                                    .leg_attribute10,
                                    pov_value     => l_supplier_site_tbl(indx)
                                                    .attribute10,
                                    pon_retcode   => l_retcode);

              IF l_retcode <> g_normal THEN
                l_supplier_site_tbl(indx).process_flag := g_error;
              END IF;

              --ISSC
            ELSIF l_supplier_site_tbl(indx).leg_attribute10 IS NOT NULL
                  AND l_supplier_site_tbl(indx).leg_source_system = g_source_issc THEN
              validate_att_suppsite(piv_attribute => 'LEG_ATTRIBUTE10',
                                    piv_value_set => 'XXETN_PUR_TYPE',
                                    piv_value     => l_supplier_site_tbl(indx).leg_attribute10,
                                    pov_value     => l_supplier_site_tbl(indx).attribute4,
                                    pon_retcode   => l_retcode);

              IF l_retcode <> g_normal THEN
                l_supplier_site_tbl(indx).process_flag := g_error;
              END IF;
            END IF;

            IF l_supplier_site_tbl(indx)
            .leg_attribute12 IS NOT NULL AND l_supplier_site_tbl(indx)
            .leg_source_system = g_source_issc THEN
              validate_att_suppsite(piv_attribute => 'LEG_ATTRIBUTE12',
                                    piv_value_set => 'XXETN_PAY_IS_FOR',
                                    piv_value     => l_supplier_site_tbl(indx)
                                                    .leg_attribute12,
                                    pov_value     => l_supplier_site_tbl(indx)
                                                    .attribute12,
                                    pon_retcode   => l_retcode);

              IF l_retcode <> g_normal THEN
                l_supplier_site_tbl(indx).process_flag := g_error;
              END IF;
            END IF;

            IF l_supplier_site_tbl(indx)
            .leg_attribute13 IS NOT NULL AND l_supplier_site_tbl(indx)
            .leg_source_system = g_source_issc THEN
              validate_att_suppsite(piv_attribute => 'LEG_ATTRIBUTE13',
                                    piv_value_set => 'XXETN_EAP_ADD_CITIBANK_INFO',
                                    piv_value     => l_supplier_site_tbl(indx)
                                                    .leg_attribute13,
                                    pov_value     => l_supplier_site_tbl(indx)
                                                    .attribute13,
                                    pon_retcode   => l_retcode);

              IF l_retcode <> g_normal THEN
                l_supplier_site_tbl(indx).process_flag := g_error;
              END IF;
            END IF;

            IF l_supplier_site_tbl(indx).leg_attribute13 IS NOT NULL
            AND l_supplier_site_tbl(indx).leg_source_system = g_source_fsc
            THEN
               l_supplier_site_tbl(indx).attribute13 := l_supplier_site_tbl(indx).leg_attribute13;
            END IF;


            IF l_supplier_site_tbl(indx).leg_attribute14 IS NOT NULL THEN
              validate_att_suppsite(piv_attribute => 'LEG_ATTRIBUTE14',
                                    piv_value_set => 'AP_SRS_YES_NO_OPT',
                                    piv_value     => l_supplier_site_tbl(indx)
                                                    .leg_attribute14,
                                    pov_value     => l_supplier_site_tbl(indx)
                                                    .attribute14,
                                    pon_retcode   => l_retcode);

              IF l_retcode <> g_normal THEN
                l_supplier_site_tbl(indx).process_flag := g_error;
              END IF;
            END IF;
            -- V1.4
            l_tax_flag_count := 0;
/*
            IF l_localization = 'ES_LOC' THEN

              SELECT COUNT(1)
                INTO l_tax_flag_count
                FROM xxap_supplier_sites_stg
               WHERE leg_vendor_id = l_supplier_site_tbl(indx)
              .leg_vendor_id
                 AND leg_tax_reporting_site_flag = 'Y';
              IF l_tax_flag_count in (0, 1) THEN
                l_supplier_site_tbl(indx).leg_tax_reporting_site_flag := 'Y';
              ELSIF l_tax_flag_count > 1 THEN
                l_supplier_site_tbl(indx).leg_tax_reporting_site_flag := 'N';

              END IF;

            END IF;*/
          END IF;

          -- V1.4 end
          /**
                         IF l_supplier_site_tbl(indx).leg_attribute15 IS NOT NULL THEN
                            validate_att_suppsite
                            (  piv_attribute => 'LEG_ATTRIBUTE15'
                            ,  piv_value_set => 'XXETN_LEGACY_VENDOR_NUMBER'
                            ,  piv_value     => l_supplier_site_tbl(indx).leg_attribute15
                            ,  pov_value     => l_supplier_site_tbl(indx).attribute15
                            ,  pon_retcode   => l_retcode
                            );

                            IF l_retcode <> g_normal THEN
                             l_supplier_site_tbl(indx).process_flag := g_error;
                            END IF;
                         END IF;
          **/

          IF (l_supplier_site_tbl(indx).process_flag = g_error) THEN
            g_retcode := 1;
          END IF;

        END LOOP;

    /*    print_log_message('l_supplier_site_tbl.COUNT ' ||
                          l_supplier_site_tbl.COUNT);

        print_log_message('Updating Supplier sites staging table');
*/
        BEGIN
          FORALL indx IN 1 .. l_supplier_site_tbl.COUNT SAVE EXCEPTIONS
            UPDATE xxap_supplier_sites_stg xsss
               SET xsss.last_update_date              = SYSDATE,
                   xsss.error_type                    = DECODE(l_supplier_site_tbl(indx)
                                                               .process_flag,
                                                               g_validated,
                                                               NULL,
                                                               g_error,
                                                               g_err_val),
                   xsss.process_flag                  = l_supplier_site_tbl(indx)
                                                       .process_flag,
                   xsss.ship_to_location_id           = l_supplier_site_tbl(indx)
                                                       .ship_to_location_id,
                   xsss.bill_to_location_id           = l_supplier_site_tbl(indx)
                                                       .bill_to_location_id,
                   xsss.ship_to_location_code         = DECODE(NVL(l_supplier_site_tbl(indx)
                                                                   .ship_to_location_id,
                                                                   -1),
                                                               -1,
                                                               NULL,
                                                               l_supplier_site_tbl(indx)
                                                               .leg_ship_to_location_code),
                   xsss.bill_to_location_code         = DECODE(NVL(l_supplier_site_tbl(indx)
                                                                   .bill_to_location_id,
                                                                   -1),
                                                               -1,
                                                               NULL,
                                                               l_supplier_site_tbl(indx)
                                                               .leg_bill_to_location_code),
                   xsss.terms_name                    = l_supplier_site_tbl(indx)
                                                       .terms_name,
                   xsss.terms_id                      = l_supplier_site_tbl(indx)
                                                       .terms_id,
                   xsss.operating_unit_name           = l_supplier_site_tbl(indx)
                                                       .operating_unit_name,
                   xsss.org_id                        = l_supplier_site_tbl(indx)
                                                       .org_id,
                   xsss.terms_date_basis              = l_supplier_site_tbl(indx)
                                                       .terms_date_basis,
                   xsss.payment_priority              = l_supplier_site_tbl(indx)
                                                       .payment_priority,
                   xsss.payment_method_lookup_code    = l_supplier_site_tbl(indx)
                                                       .payment_method_lookup_code,
                   xsss.ship_via_lookup_code          = l_supplier_site_tbl(indx)
                                                       .ship_via_lookup_code,
                   xsss.pay_on_receipt_summary_code   = l_supplier_site_tbl(indx)
                                                       .pay_on_receipt_summary_code,
                   xsss.pay_date_basis_lookup_code    = l_supplier_site_tbl(indx)
                                                       .leg_pay_date_basis_code,
                   xsss.accts_pay_code_combination_id = l_supplier_site_tbl(indx)
                                                       .accts_pay_code_combination_id,
                   xsss.prepay_code_combination_id    = l_supplier_site_tbl(indx)
                                                       .prepay_code_combination_id,
                   xsss.future_dated_payment_ccid     = l_supplier_site_tbl(indx)
                                                       .future_dated_payment_ccid,
                   xsss.awt_group_id                  = l_supplier_site_tbl(indx)
                                                       .awt_group_id,
                   xsss.awt_group_name                = l_supplier_site_tbl(indx)
                                                       .awt_group_name,
                   xsss.distribution_set_id           = l_supplier_site_tbl(indx)
                                                       .distribution_set_id,
                   xsss.distribution_set_name         = l_supplier_site_tbl(indx)
                                                       .distribution_set_name,
                   xsss.tolerance_id                  = l_supplier_site_tbl(indx)
                                                       .tolerance_id,
                   xsss.freight_terms_lookup_code     = l_supplier_site_tbl(indx)
                                                       .freight_terms_lookup_code,
                   xsss.fob_lookup_code               = l_supplier_site_tbl(indx)
                                                       .fob_lookup_code,
                   xsss.pay_group_lookup_code         = l_supplier_site_tbl(indx)
                                                       .pay_group_lookup_code,
                   xsss.vendor_id                     = l_supplier_site_tbl(indx)
                                                       .vendor_id,
                   xsss.leg_bank_charge_bearer        = l_supplier_site_tbl(indx)
                                                       .leg_bank_charge_bearer,
                   xsss.attribute2                    = l_supplier_site_tbl(indx)
                                                       .attribute2, --.leg_attribute2,
                   xsss.attribute3                    = l_supplier_site_tbl(indx).attribute3,
                                                              /* DECODE(l_supplier_site_tbl(indx)
                                                               .leg_source_system,
                                                               g_source_fsc,
                                                               l_supplier_site_tbl(indx)
                                                               .leg_attribute11,
                                                               NULL)*/
                   xsss.attribute4                    = l_supplier_site_tbl(indx).attribute4,
                                                               /*DECODE(l_supplier_site_tbl(indx).leg_source_system,
                                                               g_source_issc,
                                                               l_supplier_site_tbl(indx).attribute4,                                                               ,
                                                               NULL),*/
                   xsss.attribute6                    = l_supplier_site_tbl(indx).leg_attribute6,
                   xsss.attribute7                    = DECODE(l_supplier_site_tbl(indx)
                                                               .leg_source_system,
                                                               g_source_fsc,
                                                               l_supplier_site_tbl(indx)
                                                               .leg_attribute4,
                                                               NULL),
                   xsss.attribute8                    = DECODE(l_supplier_site_tbl(indx)
                                                               .leg_source_system,
                                                               g_source_fsc,
                                                               l_supplier_site_tbl(indx)
                                                               .leg_attribute12,
                                                               l_supplier_site_tbl(indx)
                                                               .attribute8),
                   xsss.attribute10                   = DECODE(l_supplier_site_tbl(indx)
                                                               .leg_source_system,
                                                               g_source_fsc,
                                                               l_supplier_site_tbl(indx).attribute10,
                                                               NULL),
                   xsss.attribute12                   = DECODE(l_supplier_site_tbl(indx)
                                                               .leg_source_system,
                                                               g_source_fsc,
                                                               NULL,
                                                               l_supplier_site_tbl(indx)
                                                               .attribute12),
                   xsss.attribute13                   =  l_supplier_site_tbl(indx).attribute13, --v4.0
                   xsss.attribute14                   = l_supplier_site_tbl(indx)
                                                       .leg_attribute14,
                   xsss.attribute15                   = l_supplier_site_tbl(indx)
                                                       .leg_attribute15,
                   xsss.vendor_interface_id           = l_supplier_site_tbl(indx)
                                                       .vendor_intf_id,
                   xsss.vendor_site_interface_id      = DECODE(l_supplier_site_tbl(indx)
                                                               .process_flag,
                                                               g_validated,
                                                               ap_supplier_sites_int_s.NEXTVAL,
                                                               g_error,
                                                               NULL),
                   xsss.vendor_site_code              = l_supplier_site_tbl(indx)
                                                       .leg_vendor_site_code,
                   xsss.leg_tax_reporting_site_flag   = l_supplier_site_tbl(indx)
                                                       .leg_tax_reporting_site_flag,
                   xsss.leg_global_attribute18        = l_supplier_site_tbl(indx)
                                                       .leg_global_attribute18,
                  xsss.pay_on_code                    = l_supplier_site_tbl(indx).pay_on_code,      /*18 July 2016  */
                  --ADB 09/27/2017 change starts
                  xsss.attribute1                     = DECODE(l_supplier_site_tbl(indx)
                                                               .leg_source_system,
                                                               g_source_issc,
                                                               l_supplier_site_tbl(indx).attribute1,
                                                               NULL)  --ADB change ends
             WHERE xsss.interface_txn_id = l_supplier_site_tbl(indx)
            .interface_txn_id;
        EXCEPTION
          WHEN OTHERS THEN
            print_log_message(SUBSTR('Exception in Procedure validate_supplier_sites while doing Bulk Insert. ' ||
                                     SQLERRM,
                                     1,
                                     2000));
            print_log_message('No. of records in Bulk Exception : ' ||
                              SQL%BULK_EXCEPTIONS.COUNT);
        END;
      END IF;

    END LOOP;

    CLOSE supplier_site_cur;

    -- Insert remaining records in Error Table
    IF g_source_tab.COUNT > 0 THEN
      xxetn_common_error_pkg.add_error(pov_return_status => l_return_status,
                                       pov_error_msg     => l_error_msg,
                                       pi_source_tab     => g_source_tab,
                                       pin_batch_id      => g_new_batch_id);
      g_source_tab.DELETE;
      g_indx := 0;
    END IF;

/*This is for CR#372676, Site code replace with HOME for employee as supplier (Dated - 15 March 2016)*/
/* This comment because we donot make all TE, HOME vendor site code to HOME due to some Vendor containing TE,HOME
BEGIN
Update AP_SUPPLIER_SITES_INT
   Set vendor_site_code = 'HOME'
 Where vendor_site_code like '%TE%HOME%';

   COMMIT;
EXCEPTION
  WHEN OTHERS THEN
    print_log_message('Error :- In update Site code (HOME) for employee as supplier where site is TE,HOME. ' || SQLERRM);
END;*/

BEGIN

/*Update AP_SUPPLIER_SITES_INT
   Set vendor_site_code = 'HOME'
 where vendor_site_interface_id in
       (Select vendor_site_interface_id
          from AP_SUPPLIER_SITES_INT a
         where vendor_interface_id in
               (Select vendor_interface_id
                  from AP_SUPPLIERS_INT
                 where vendor_type_lookup_code = 'EMPLOYEE')
           and vendor_site_code not in ('HOME', 'OFFICE', 'PROVISIONAL'));*/

--ADB Changes 09/27/2016
   Update xxap_supplier_sites_stg xas
   Set vendor_site_code = 'HOME'
   WHERE EXISTS (SELECT 1
                 FROM xxap_suppliers_stg x
                 WHERE x.leg_vendor_id = xas.leg_vendor_id
                 AND x.leg_source_system = xas.leg_source_System
                 AND x.leg_vendor_type_lookup_code = 'EMPLOYEE')
   AND   leg_vendor_site_code not in ('HOME', 'OFFICE', 'PROVISIONAL');
   COMMIT;

/*Update xxap_supplier_sites_stg xas
   Set vendor_site_code = 'HOME'
 where leg_vendor_site_id in
       (Select leg_vendor_site_id
          from xxap_supplier_sites_stg a
         where vendor_id in
               (Select vendor_id
                  from xxap_suppliers_stg
                 where leg_vendor_type_lookup_code = 'EMPLOYEE')
           and leg_vendor_site_code not in ('HOME', 'OFFICE', 'PROVISIONAL'));*/
--ADB Changes ends
    Update xxap_supplier_contacts_stg
    Set vendor_site_code = 'HOME'
    where (leg_vendor_site_id, leg_vendor_site_code, leg_request_id) in
          (Select leg_vendor_site_id, leg_vendor_site_code, leg_request_id
           from xxap_supplier_sites_stg
           Where vendor_site_code = 'HOME');

        -- Update new vendor site code in bank account.
        Update xxap_supplier_bankaccnts_stg
           Set leg_vendor_site_code = 'HOME'
         where (leg_vendor_id, leg_vendor_site_id, leg_vendor_site_code,
                leg_org_id, leg_request_id) in
         (Select leg_vendor_id,
                       leg_vendor_site_id,
                       leg_vendor_site_code,
                       leg_org_id,
                       leg_request_id
                  from xxap_supplier_sites_stg
                 Where vendor_site_code = 'HOME');
   COMMIT;


EXCEPTION
  WHEN OTHERS THEN
    print_log_message('Error :- In update Site code other then HOME for employee as supplier where site is TE,HOME. ' || SQLERRM);
END;



   /*BEGIN PMC : 339760*/
  /* 4.  Tax classification code :  <Only for exceptions i.e. where equivalent code is not equal to default code> : Migrate 11i tax code to R12 (leveraging cross reference utility). (FYI:  Field Name - AP_SUPPLIER_SITES_INT. VAT_CODE)
        a.  Extract code from 11i Supplier site
        b.  Leverage Tax mapping cross mapping utility to derive equivalent code for R12
        c.  Identify if this equal to default tax code for that Operating Unit (custom lookup : XXETN_DEF_TAX_RATES. Column :  “Code” – serial number ; “Meaning “ - serial number ; “Description “ – R12 Operating Unit ; “Tag” : R12 Default Code , “Attribute1” : R12 Default Code).
        d.  If (b) exists in lookup referred in Pt(c) against R12 OU then donot populate any value in Tax Classification code (field details in Pt 4)
        e.  If (b) does not exists in lookup referred in Pt(c) against R12 OU then populate any value in Tax Classification code. (field details in Pt 4)
  */
   validate_vat_code_supp(l_error_msg, l_retcode);
   /* END PMC : 339760*/

  /* For Duplicate site we have to perform 2 steps
    1- Merge  - if same site code with same address line1 then merge that site in single site.
    2- Update - if same site code with different address line 1 then update the site code value with 11i orgid  vendor site id.*/

   /* Duplicate Supplier site merge/block logic date on 31 March 2016
      This logic merge site if site code is same and address line1 is also same*/
   print_log_message('Dup_SupplierSite_merge procedure Starts '||Sysdate);
   Dup_SupplierSite_merge(l_error_msg, l_retcode,g_new_batch_id,g_request_id);
   print_log_message('Dup_SupplierSite_merge procedure End '||Sysdate);


  /*Procedure call to perform Multiple tax reporting flag :only one site can be enabled for tax reporting within single OU.*/
    print_log_message('Performing Supplier site :- Multiple tax reporting flag :only one site can be enabled for tax reporting within single OU. ');
    mul_flag_upd_supp_site(l_retcode);
    print_log_message('mul_flag_upd_supp_site Ends '||sysdate);

   /*Procedure call to perform ECE codes cannot be shared across sites within one OU*/
    print_log_message('Performing Supplier site :- ECE codes cannot be shared across sites within one OU');
    ECE_Code_upd_supp_site(l_retcode);


    if l_error_msg <> g_normal Then
      print_log_message('Error in procedure :- Dup_SupplierSite_Upd ' ||
                        l_error_msg || ' - ' || l_retcode);
    else
      print_log_message('Successfully executed in procedure :- Dup_SupplierSite_Upd ');
    end if;



  EXCEPTION
    WHEN OTHERS THEN
      pon_retcode := g_ret_error;
      print_log_message(SUBSTR('Exception in Procedure validate_supplier_sites. ' ||
                               dbms_utility.format_error_stack || ' / ' ||
                               dbms_utility.format_error_backtrace,
                               1,
                               2000));
  END validate_supplier_sites;

  --
  -- ========================
  -- Procedure: validate_supplier
  -- =============================================================================
  --   This procedure is used to run validations for Supplier entity records
  -- =============================================================================
  --  Input Parameters :
  --    None
  --  Output Parameters :
  --    None
  -- -----------------------------------------------------------------------------
  --
  PROCEDURE validate_supplier(pon_retcode OUT NUMBER) IS
    l_retcode NUMBER;
    l_active_date date;
    l_return_status  VARCHAR2(50);
    l_error_msg      VARCHAR2(2000);
    l_log_ret_status VARCHAR2(50);
    l_log_err_msg    VARCHAR2(2000);
    l_err_code CONSTANT VARCHAR2(28) := 'ETN_AP_MANDATORY_NOT_ENTERED';
    l_localization VARCHAR2(30);
    CURSOR supplier_cur IS
      SELECT *
        FROM xxap_suppliers_stg xss
       WHERE xss.request_id = g_request_id
       and   xss.process_flag = g_new      -- 07 Jan 2016 data fix
       ORDER BY xss.interface_txn_id;

    TYPE supplier_t IS TABLE OF supplier_cur%ROWTYPE INDEX BY BINARY_INTEGER;
    l_supplier_tbl supplier_t;

    CURSOR cu_sup_local(p_vendor_id NUMBER, p_source_system VARCHAR2, p_vendor_name VARCHAR2 ) IS
      SELECT *
        FROM xxap_supplier_sites_stg
       WHERE leg_vendor_id = p_vendor_id
         AND leg_source_system = p_source_system
         AND leg_vendor_name = p_vendor_name ;
  BEGIN
    pon_retcode := g_normal;

    -- Initialize global variables for log_errors
    print_log_message('Initializing common error utility global variables');
    g_source_table    := g_supplier_t;
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

    --Procedure call to perform mandatory value checks for Supplier entity
    print_log_message('Checking mandatory values');
    req_val_chk_supp(l_retcode);

    --Procedure call to perform duplicate value checks for Supplier entity
    print_log_message('Checking duplicate values');
    dup_val_chk_supp(l_retcode);

    print_log_message('Start validating supplier records');
    OPEN supplier_cur;
    LOOP
      FETCH supplier_cur BULK COLLECT
        INTO l_supplier_tbl LIMIT 1000;
      print_log_message('l_supplier_tbl.COUNT ' || l_supplier_tbl.COUNT);
      EXIT WHEN l_supplier_tbl.COUNT = 0;
      IF l_supplier_tbl.COUNT > 0 THEN
        FOR indx IN 1 .. l_supplier_tbl.COUNT LOOP
          l_supplier_tbl(indx).process_flag := g_validated;
          g_intf_staging_id := l_supplier_tbl(indx).interface_txn_id;

          validate_loc_dtls_supp(piv_ship_to_loc_code => l_supplier_tbl(indx)
                                                        .leg_ship_to_location_code,
                                 piv_bill_to_loc_code => l_supplier_tbl(indx)
                                                        .leg_bill_to_location_code,
                                 pon_ship_to_loc_id   => l_supplier_tbl(indx)
                                                        .ship_to_location_id,
                                 pon_bill_to_loc_id   => l_supplier_tbl(indx)
                                                        .bill_to_location_id,
                                 pon_retcode          => l_retcode);

          IF l_retcode <> g_normal THEN
            l_supplier_tbl(indx).process_flag := g_error;
          END IF;

          IF l_supplier_tbl(indx)
          .leg_accts_pay_code_segment1 || l_supplier_tbl(indx)
          .leg_accts_pay_code_segment2 || l_supplier_tbl(indx)
          .leg_accts_pay_code_segment3 || l_supplier_tbl(indx)
          .leg_accts_pay_code_segment4 || l_supplier_tbl(indx)
          .leg_accts_pay_code_segment5 || l_supplier_tbl(indx)
          .leg_accts_pay_code_segment6 || l_supplier_tbl(indx)
          .leg_accts_pay_code_segment7 IS NOT NULL THEN
            validate_coa_supp(piv_segment1    => l_supplier_tbl(indx)
                                                .leg_accts_pay_code_segment1,
                              piv_segment2    => l_supplier_tbl(indx)
                                                .leg_accts_pay_code_segment2,
                              piv_segment3    => l_supplier_tbl(indx)
                                                .leg_accts_pay_code_segment3,
                              piv_segment4    => l_supplier_tbl(indx)
                                                .leg_accts_pay_code_segment4,
                              piv_segment5    => l_supplier_tbl(indx)
                                                .leg_accts_pay_code_segment5,
                              piv_segment6    => l_supplier_tbl(indx)
                                                .leg_accts_pay_code_segment6,
                              piv_segment7    => l_supplier_tbl(indx)
                                                .leg_accts_pay_code_segment7,
                              piv_column_name => 'LEG_ACCTS_PAY_CODE_SEGMENTS',
                              pon_cc_id       => l_supplier_tbl(indx)
                                                .accts_pay_code_combination_id,
                              pon_retcode     => l_retcode);

            IF l_retcode <> g_normal THEN
              l_supplier_tbl(indx).process_flag := g_error;
            END IF;
          END IF;

          IF l_supplier_tbl(indx)
          .leg_prepay_code_segment1 || l_supplier_tbl(indx)
          .leg_prepay_code_segment2 || l_supplier_tbl(indx)
          .leg_prepay_code_segment3 || l_supplier_tbl(indx)
          .leg_prepay_code_segment4 || l_supplier_tbl(indx)
          .leg_prepay_code_segment5 || l_supplier_tbl(indx)
          .leg_prepay_code_segment6 || l_supplier_tbl(indx)
          .leg_prepay_code_segment7 IS NOT NULL THEN
            validate_coa_supp(piv_segment1    => l_supplier_tbl(indx)
                                                .leg_prepay_code_segment1,
                              piv_segment2    => l_supplier_tbl(indx)
                                                .leg_prepay_code_segment2,
                              piv_segment3    => l_supplier_tbl(indx)
                                                .leg_prepay_code_segment3,
                              piv_segment4    => l_supplier_tbl(indx)
                                                .leg_prepay_code_segment4,
                              piv_segment5    => l_supplier_tbl(indx)
                                                .leg_prepay_code_segment5,
                              piv_segment6    => l_supplier_tbl(indx)
                                                .leg_prepay_code_segment6,
                              piv_segment7    => l_supplier_tbl(indx)
                                                .leg_prepay_code_segment7,
                              piv_column_name => 'LEG_PREPAY_CODE_SEGMENTS',
                              pon_cc_id       => l_supplier_tbl(indx)
                                                .prepay_code_combination_id,
                              pon_retcode     => l_retcode);

            IF l_retcode <> g_normal THEN
              l_supplier_tbl(indx).process_flag := g_error;
            END IF;
          END IF;

          IF l_supplier_tbl(indx)
          .leg_future_dated_segment1 || l_supplier_tbl(indx)
          .leg_future_dated_segment2 || l_supplier_tbl(indx)
          .leg_future_dated_segment3 || l_supplier_tbl(indx)
          .leg_future_dated_segment4 || l_supplier_tbl(indx)
          .leg_future_dated_segment5 || l_supplier_tbl(indx)
          .leg_future_dated_segment6 || l_supplier_tbl(indx)
          .leg_future_dated_segment7 IS NOT NULL THEN
            validate_coa_supp(piv_segment1    => l_supplier_tbl(indx)
                                                .leg_future_dated_segment1,
                              piv_segment2    => l_supplier_tbl(indx)
                                                .leg_future_dated_segment2,
                              piv_segment3    => l_supplier_tbl(indx)
                                                .leg_future_dated_segment3,
                              piv_segment4    => l_supplier_tbl(indx)
                                                .leg_future_dated_segment4,
                              piv_segment5    => l_supplier_tbl(indx)
                                                .leg_future_dated_segment5,
                              piv_segment6    => l_supplier_tbl(indx)
                                                .leg_future_dated_segment6,
                              piv_segment7    => l_supplier_tbl(indx)
                                                .leg_future_dated_segment7,
                              piv_column_name => 'LEG_FUTURE_DATED_SEGMENTS',
                              pon_cc_id       => l_supplier_tbl(indx)
                                                .future_dated_payment_ccid,
                              pon_retcode     => l_retcode);

            IF l_retcode <> g_normal THEN
              l_supplier_tbl(indx).process_flag := g_error;
            END IF;
          END IF;

          IF l_supplier_tbl(indx).leg_awt_group_name IS NOT NULL THEN
            validate_awt_grp_supp(piv_group_name => l_supplier_tbl(indx)
                                                   .leg_awt_group_name,
                                  pon_group_id   => l_supplier_tbl(indx)
                                                   .awt_group_id,
                                  pov_group_name => l_supplier_tbl(indx)
                                                   .awt_group_name,
                                  pon_retcode    => l_retcode);

            IF l_retcode <> g_normal THEN
              l_supplier_tbl(indx).process_flag := g_error;
            END IF;
          END IF;

          IF l_supplier_tbl(indx).leg_vendor_type_lookup_code IS NOT NULL THEN
            validate_vendor_type_supp(piv_lookup_code => l_supplier_tbl(indx)
                                                        .leg_vendor_type_lookup_code,
                                      pov_lookup_code => l_supplier_tbl(indx)
                                                        .vendor_type_lookup_code,
                                      pon_retcode     => l_retcode);

            IF l_retcode <> g_normal THEN
              l_supplier_tbl(indx).process_flag := g_error;
            END IF;
          END IF;

          IF l_supplier_tbl(indx).leg_freight_terms_code IS NOT NULL THEN
            validate_freight_term_supp(piv_lookup_code => l_supplier_tbl(indx)
                                                         .leg_freight_terms_code,
                                       pov_lookup_code => l_supplier_tbl(indx)
                                                         .freight_terms_lookup_code,
                                       pon_retcode     => l_retcode);

            IF l_retcode <> g_normal THEN
              l_supplier_tbl(indx).process_flag := g_error;
            END IF;
          END IF;

          IF l_supplier_tbl(indx).leg_fob_lookup_code IS NOT NULL THEN
            validate_fob_code_supp(piv_lookup_code => l_supplier_tbl(indx)
                                                     .leg_fob_lookup_code,
                                   pov_lookup_code => l_supplier_tbl(indx)
                                                     .fob_lookup_code,
                                   pon_retcode     => l_retcode);

            IF l_retcode <> g_normal THEN
              l_supplier_tbl(indx).process_flag := g_error;
            END IF;
          END IF;

          IF l_supplier_tbl(indx).leg_pay_group_lookup_code IS NOT NULL THEN
            validate_pay_group_supp(piv_lookup_code => l_supplier_tbl(indx)
                                                      .leg_pay_group_lookup_code,
                                    pov_lookup_code => l_supplier_tbl(indx)
                                                      .pay_group_lookup_code,
                                    pon_retcode     => l_retcode);

            IF l_retcode <> g_normal THEN
              l_supplier_tbl(indx).process_flag := g_error;
            END IF;
          END IF;

          IF l_supplier_tbl(indx).leg_bank_charge_bearer IS NOT NULL THEN
            validate_bank_bearer_supp(piv_lookup_code => l_supplier_tbl(indx)
                                                        .leg_bank_charge_bearer,
                                      pov_lookup_code => l_supplier_tbl(indx)
                                                        .bank_charge_bearer,
                                      pon_retcode     => l_retcode);

            IF l_retcode <> g_normal THEN
              l_supplier_tbl(indx).process_flag := g_error;
            END IF;
          END IF;

          IF l_supplier_tbl(indx).leg_invoice_currency_code IS NOT NULL THEN
            validate_currency_code_supp(piv_currency_code => l_supplier_tbl(indx)
                                                            .leg_invoice_currency_code,
                                        piv_code_type     => 'LEG_INVOICE_CURRENCY_CODE',
                                        pon_retcode       => l_retcode);

            IF l_retcode <> g_normal THEN
              l_supplier_tbl(indx).process_flag := g_error;
            END IF;
          END IF;

          IF l_supplier_tbl(indx).leg_payment_currency_code IS NOT NULL THEN
            validate_currency_code_supp(piv_currency_code => l_supplier_tbl(indx)
                                                            .leg_payment_currency_code,
                                        piv_code_type     => 'LEG_PAYMENT_CURRENCY_CODE',
                                        pon_retcode       => l_retcode);

            IF l_retcode <> g_normal THEN
              l_supplier_tbl(indx).process_flag := g_error;
            END IF;
          END IF;

          IF l_supplier_tbl(indx).leg_organization_type_code IS NOT NULL THEN
            validate_org_type_supp(piv_lookup_code => l_supplier_tbl(indx)
                                                     .leg_organization_type_code,
                                   pov_lookup_code => l_supplier_tbl(indx)
                                                     .organization_type_lookup_code,
                                   pon_retcode     => l_retcode);

            IF l_retcode <> g_normal THEN
              l_supplier_tbl(indx).process_flag := g_error;
            END IF;
          END IF;

          IF l_supplier_tbl(indx).leg_minority_group_code IS NOT NULL THEN
            validate_minority_group_supp(piv_lookup_code => l_supplier_tbl(indx)
                                                           .leg_minority_group_code,
                                         pov_lookup_code => l_supplier_tbl(indx)
                                                           .minority_group_lookup_code,
                                         pon_retcode     => l_retcode);

            IF l_retcode <> g_normal THEN
              l_supplier_tbl(indx).process_flag := g_error;
            END IF;
          END IF;

          IF l_supplier_tbl(indx).leg_match_option IS NOT NULL THEN
            validate_match_options_supp(piv_lookup_code => l_supplier_tbl(indx)
                                                          .leg_match_option,
                                        pov_lookup_code => l_supplier_tbl(indx)
                                                          .match_option,
                                        pon_retcode     => l_retcode);

            IF l_retcode <> g_normal THEN
              l_supplier_tbl(indx).process_flag := g_error;
            END IF;
          END IF;

          IF l_supplier_tbl(indx).leg_pay_date_basis_code IS NOT NULL THEN
            validate_pay_date_basis_supp(piv_lookup_code => l_supplier_tbl(indx)
                                                           .leg_pay_date_basis_code,
                                         pov_lookup_code => l_supplier_tbl(indx)
                                                           .pay_date_basis_lookup_code,
                                         pon_retcode     => l_retcode);

            IF l_retcode <> g_normal THEN
              l_supplier_tbl(indx).process_flag := g_error;
            END IF;
          END IF;

          IF l_supplier_tbl(indx).leg_type_1099 IS NOT NULL THEN
            validate_type_1099_supp(piv_type1099 => l_supplier_tbl(indx)
                                                   .leg_type_1099,
                                    pon_retcode  => l_retcode);

            IF l_retcode <> g_normal THEN
              l_supplier_tbl(indx).process_flag := g_error;
            END IF;
          END IF;

          IF l_supplier_tbl(indx).leg_qty_rcv_exception_code IS NOT NULL THEN
            validate_rcv_code_supp(piv_lookup_code => l_supplier_tbl(indx)
                                                     .leg_qty_rcv_exception_code,
                                   pov_lookup_code => l_supplier_tbl(indx)
                                                     .qty_rcv_exception_code,
                                   pon_retcode     => l_retcode);

            IF l_retcode <> g_normal THEN
              l_supplier_tbl(indx).process_flag := g_error;
            END IF;
          END IF;

          IF l_supplier_tbl(indx).leg_payment_method_code IS NOT NULL THEN
            validate_pymt_method_supp(piv_lookup_code => l_supplier_tbl(indx)
                                                        .leg_payment_method_code,
                                      pov_lookup_code => l_supplier_tbl(indx)
                                                        .payment_method_lookup_code,
                                      pon_retcode     => l_retcode);

            IF l_retcode <> g_normal THEN
              l_supplier_tbl(indx).process_flag := g_error;
            END IF;
          END IF;

         /* Vat code on supplier header is not use in R12
            IF l_supplier_tbl(indx).leg_vat_code IS NOT NULL THEN
            validate_vat_code_supp(piv_lookup_code => l_supplier_tbl(indx)
                                                     .leg_vat_code,
                                   pon_retcode     => l_retcode);

            IF l_retcode <> g_normal THEN
              l_supplier_tbl(indx).process_flag := g_error;
            END IF;
          END IF;*/

          IF l_supplier_tbl(indx).leg_terms_name IS NOT NULL THEN
            validate_pymt_term_suppsite(piv_lookup_code => l_supplier_tbl(indx)
                                                          .leg_terms_name,
                                        pov_lookup_code => l_supplier_tbl(indx)
                                                          .terms_name,
                                        pon_terms_id    => l_supplier_tbl(indx)
                                                          .terms_id,
                                        pon_retcode     => l_retcode);

            IF l_retcode <> g_normal THEN
              l_supplier_tbl(indx).process_flag := g_error;
            END IF;
          END IF;

          -- If Vendor Type is Employee, then Employee Number is mandatory
          IF NVL(l_supplier_tbl(indx).leg_vendor_type_lookup_code, 'XXXXX') =
             'EMPLOYEE' THEN

            IF l_supplier_tbl(indx).leg_employee_number IS NULL THEN

              log_errors(pov_return_status       => l_log_ret_status -- OUT
                        ,
                         pov_error_msg           => l_log_err_msg -- OUT
                        ,
                         piv_source_column_name  => 'LEG_EMPLOYEE_NUMBER',
                         piv_source_column_value => l_supplier_tbl(indx)
                                                   .leg_employee_number,
                         piv_error_type          => g_err_val,
                         piv_error_code          => 'NULL_LEG_EMPLOYEE_NUMBER',
                         piv_error_message       => 'Error : LEG_EMPLOYEE_NUMBER should not be NULL for Vendor Type as Employee');

              l_supplier_tbl(indx).process_flag := g_error;

            ELSE
              validate_employee_supp(piv_employee_num => l_supplier_tbl(indx)
                                                        .leg_employee_number,
                                     pov_employee_id  => l_supplier_tbl(indx)
                                                        .employee_id,
                                     pon_retcode      => l_retcode);

          IF l_retcode = '100' THEN  /*If employee is does not exist in employee master or employee is deactive */
              l_supplier_tbl(indx).vendor_type_lookup_code :='VENDOR'; --v3.0 removed leg type and updated only vendor type
              l_supplier_tbl(indx).Leg_hold_all_payments_flag :='Y';

           BEGIN
             --v4.0 change needed to synch with BR10 lookup setup
              SELECT  distinct to_Date(lookup_code) --to_date(flv.description)
              INTO l_active_date
              FROM fnd_lookup_values flv
             WHERE flv.lookup_type = 'XXETN_VENDOR_INACTIVE_DATE'
               --AND flv.meaning = 'VENDOR'
               AND flv.enabled_flag = g_yes
               AND SYSDATE BETWEEN NVL(flv.start_date_active, SYSDATE) AND
                   NVL(flv.end_date_active, SYSDATE + 1)
               AND flv.language = USERENV('LANG');

            l_supplier_tbl(indx).Leg_END_Date_active := l_active_date;
            l_retcode := g_normal;
            EXCEPTION
                 WHEN OTHERS THEN
                     pon_retcode := g_warning;
                    log_errors(pov_return_status       => l_log_ret_status -- OUT
                              ,
                               pov_error_msg           => l_log_err_msg -- OUT
                              ,
                               piv_source_column_name  => 'LEG_EMPLOYEE_NUMBER',
                               piv_source_column_value => l_supplier_tbl(indx).leg_employee_number,
                               piv_error_type          => g_err_val,
                               piv_error_code          => 'XXETN_VENDOR_INACTIVE_DATE',
                               piv_error_message       => 'Error : Employee inactive date is not configure in lookup - XXETN_VENDOR_INACTIVE_DATE, Oracle Error is ' ||
                                                          SQLERRM);

            END;

              END IF;

              IF l_retcode <> g_normal THEN
                l_supplier_tbl(indx).process_flag := g_error;
              END IF;

            END IF;

          END IF;

          -- Attribute2
          validate_att2_supp(piv_attribute2 => l_supplier_tbl(indx)
                                              .leg_attribute2,
                             pov_attribute2 => l_supplier_tbl(indx)
                                              .attribute2,
                             pon_retcode    => l_retcode);

          IF l_retcode <> g_normal THEN
            l_supplier_tbl(indx).process_flag := g_error;
          END IF;

          IF l_supplier_tbl(indx).leg_attribute5 IS NOT NULL THEN

            validate_att5_supp(piv_attribute5 => l_supplier_tbl(indx)
                                                .leg_attribute5,
                               pov_attribute5 => l_supplier_tbl(indx)
                                                .attribute5,
                               pon_retcode    => l_retcode);

            IF l_retcode <> g_normal THEN
              l_supplier_tbl(indx).process_flag := g_error;
            END IF;

          END IF;

          IF l_supplier_tbl(indx)
          .leg_attribute7 IS NOT NULL AND l_supplier_tbl(indx)
          .leg_source_system = g_source_fsc THEN

            validate_att7_supp(piv_attribute7 => l_supplier_tbl(indx)
                                                .leg_attribute7,
                               pov_attribute7 => l_supplier_tbl(indx)
                                                .attribute7,
                               pon_retcode    => l_retcode);

            IF l_retcode <> g_normal THEN
              l_supplier_tbl(indx).process_flag := g_error;
            END IF;

          END IF;

          IF l_supplier_tbl(indx)
          .leg_attribute14 IS NOT NULL AND l_supplier_tbl(indx)
          .leg_source_system = g_source_fsc THEN

            validate_att14_supp(piv_attribute14 => l_supplier_tbl(indx)
                                                  .leg_attribute14,
                                pov_attribute14 => l_supplier_tbl(indx)
                                                  .attribute14,
                                pon_retcode     => l_retcode);

            IF l_retcode <> g_normal THEN
              l_supplier_tbl(indx).process_flag := g_error;
            END IF;

          END IF;

          IF l_supplier_tbl(indx)
          .leg_attribute15 IS NOT NULL AND l_supplier_tbl(indx)
          .leg_source_system = g_source_fsc THEN

            validate_att15_supp(piv_attribute15 => l_supplier_tbl(indx)
                                                  .leg_attribute15,
                                pov_attribute15 => l_supplier_tbl(indx)
                                                  .attribute15,
                                pon_retcode     => l_retcode);

            IF l_retcode <> g_normal THEN
              l_supplier_tbl(indx).process_flag := g_error;
            END IF;

          END IF;
          --
          -- Start supplier localization check v1.4
          --

          FOR s_rec IN cu_sup_local(l_supplier_tbl(indx).leg_vendor_id,
                                    l_supplier_tbl(indx).leg_source_system,
                                    l_supplier_tbl(indx).leg_vendor_name) LOOP

            l_localization := NULL;

            BEGIN
              SELECT attribute4 -- territory_code
                INTO l_localization
                FROM fnd_lookup_values_vl flvv
               WHERE lookup_type = 'XXETN_PLANT_COUNTRY_LOC_MAP'
                 AND attribute5 = 'Y'
                 AND attribute1 = s_rec.leg_accts_pay_code_segment1
                 AND enabled_flag = 'Y'
                 AND SYSDATE BETWEEN NVL(flvv.start_date_active, SYSDATE) AND
                     NVL(flvv.end_date_active, SYSDATE);
            EXCEPTION
              WHEN OTHERS THEN
                l_localization := NULL;
            END;

            IF l_localization IN ('IT_LOC', 'TH_LOC') THEN
              l_supplier_tbl(indx).leg_global_attribute17 := l_localization;
              IF s_rec.leg_allow_awt_flag = 'Y' THEN
                l_supplier_tbl(indx).leg_allow_awt_flag := 'Y';
              END IF;
            ELSIF l_localization = 'CL_LOC' THEN
              IF l_supplier_tbl(indx).leg_global_attribute17 IS NULL THEN
                l_supplier_tbl(indx).leg_global_attribute17 := l_localization;
              ELSE
                l_supplier_tbl(indx).leg_global_attribute17 := 'BOTH_LOC';
              END IF;
                  IF   l_supplier_tbl(indx).leg_global_attribute10 = 'DOMESTIC_ORIGIN' THEN
                       IF (l_supplier_tbl(indx).leg_global_attribute12 NOT BETWEEN 0 AND 9) OR  (l_supplier_tbl(indx).leg_global_attribute12 <> 'K') THEN

                                  g_intf_staging_id := l_supplier_tbl(indx).interface_txn_id;

                                  log_errors  ( pov_return_status          =>   l_log_ret_status          -- OUT
                                              , pov_error_msg              =>   l_log_err_msg             -- OUT
                                              , piv_source_column_name     =>   'LEG_VENDOR_NAME'
                                              , piv_source_column_value    =>   l_supplier_tbl(indx).leg_vendor_name
                                              , piv_error_type             =>   g_err_val
                                              , piv_error_code             =>   l_err_code
                                              , piv_error_message          =>   'Error : Invalid Taxpayer ID Validation Digit for Chile Suppliers'
                                              );
                        END IF ;
                   END IF ;
            ELSIF l_localization = 'ES_LOC' THEN
              IF l_supplier_tbl(indx).leg_global_attribute17 IS NOT NULL THEN
                l_supplier_tbl(indx).leg_global_attribute17 := 'BOTH_LOC';
              ELSE
                g_intf_staging_id := l_supplier_tbl(indx).interface_txn_id;
                l_supplier_tbl(indx).leg_global_attribute17 := l_localization;
              END IF; -- IF l_supplier_tbl(indx).leg_global_attribute17 IS NOT NULL THEN   -- DD 2 March 2016

               IF l_supplier_tbl(indx).leg_num_1099 IS NULL THEN
                  log_errors(pov_return_status       => l_log_ret_status -- OUT
                            ,
                             pov_error_msg           => l_log_err_msg -- OUT
                            ,
                             piv_source_column_name  => 'LEG_VENDOR_NAME',
                             piv_source_column_value => l_supplier_tbl(indx)
                                                       .leg_vendor_name,
                             piv_error_type          => g_err_val,
                             piv_error_code          => l_err_code,
                             piv_error_message       => 'Error : LEG_NUM_1099 should not be NULL for Spain Suppliers');

                  l_supplier_tbl(indx).process_flag := g_error;

                END IF;

                IF l_supplier_tbl(indx).leg_vat_registration_num IS NULL THEN

                  log_errors(pov_return_status       => l_log_ret_status -- OUT
                            ,
                             pov_error_msg           => l_log_err_msg -- OUT
                            ,
                             piv_source_column_name  => 'LEG_VENDOR_NAME',
                             piv_source_column_value => l_supplier_tbl(indx)
                                                       .leg_vendor_name,
                             piv_error_type          => g_err_val,
                             piv_error_code          => l_err_code,
                             piv_error_message       => 'Error : LEG_VAT_REGISTRATION_NUM should not be NULL for Spain Suppliers');

                  l_supplier_tbl(indx).process_flag := g_error;

                END IF;
             -- END IF; -- IF l_supplier_tbl(indx).leg_global_attribute17 IS NOT NULL THEN
            END IF; -- Localization check

          END LOOP;

          IF (l_supplier_tbl(indx).process_flag = g_error) THEN
            g_retcode := 1;
          END IF;

        END LOOP;

        BEGIN
          FORALL indx IN 1 .. l_supplier_tbl.COUNT SAVE EXCEPTIONS
            UPDATE xxap_suppliers_stg xss
               SET xss.last_updated_date             = SYSDATE,
                   xss.error_type                    = DECODE(l_supplier_tbl(indx)
                                                              .process_flag,
                                                              g_validated,
                                                              NULL,
                                                              g_error,
                                                              g_err_val),
                   xss.process_flag                  = l_supplier_tbl(indx)
                                                      .process_flag,
                   xss.ship_to_location_id           = l_supplier_tbl(indx)
                                                      .ship_to_location_id,
                   xss.bill_to_location_id           = l_supplier_tbl(indx)
                                                      .bill_to_location_id,
                   xss.ship_to_location_code         = DECODE(NVL(l_supplier_tbl(indx)
                                                                  .ship_to_location_id,
                                                                  -1),
                                                              -1,
                                                              NULL,
                                                              l_supplier_tbl(indx)
                                                              .leg_ship_to_location_code),
                   xss.bill_to_location_code         = DECODE(NVL(l_supplier_tbl(indx)
                                                                  .bill_to_location_id,
                                                                  -1),
                                                              -1,
                                                              NULL,
                                                              l_supplier_tbl(indx)
                                                              .leg_bill_to_location_code),
                   xss.accts_pay_code_combination_id = l_supplier_tbl(indx)
                                                      .accts_pay_code_combination_id,
                   xss.prepay_code_combination_id    = l_supplier_tbl(indx)
                                                      .prepay_code_combination_id,
                   xss.future_dated_payment_ccid     = l_supplier_tbl(indx)
                                                      .future_dated_payment_ccid,
                   xss.awt_group_id                  = l_supplier_tbl(indx)
                                                      .awt_group_id,
                   xss.awt_group_name                = l_supplier_tbl(indx)
                                                      .awt_group_name,
                   xss.vendor_type_lookup_code       = l_supplier_tbl(indx)
                                                      .vendor_type_lookup_code,
                   xss.freight_terms_lookup_code     = l_supplier_tbl(indx)
                                                      .freight_terms_lookup_code,
                   xss.fob_lookup_code               = l_supplier_tbl(indx)
                                                      .fob_lookup_code,
                   xss.pay_group_lookup_code         = l_supplier_tbl(indx)
                                                      .pay_group_lookup_code,
                   xss.bank_charge_bearer            = l_supplier_tbl(indx)
                                                      .bank_charge_bearer,
                   xss.organization_type_lookup_code = l_supplier_tbl(indx)
                                                      .organization_type_lookup_code,
                   xss.minority_group_lookup_code    = l_supplier_tbl(indx)
                                                      .minority_group_lookup_code,
                   xss.match_option                  = l_supplier_tbl(indx)
                                                      .match_option,
                   xss.pay_date_basis_lookup_code    = l_supplier_tbl(indx)
                                                      .pay_date_basis_lookup_code,
                   xss.qty_rcv_exception_code        = l_supplier_tbl(indx)
                                                      .qty_rcv_exception_code,
                   xss.payment_method_lookup_code    = l_supplier_tbl(indx)
                                                      .payment_method_lookup_code,
                   xss.employee_id                   = l_supplier_tbl(indx)
                                                      .employee_id,
                   xss.attribute2                    = l_supplier_tbl(indx)
                                                      .attribute2,
                   xss.attribute5                    = l_supplier_tbl(indx)
                                                      .attribute5,
                   xss.attribute7                    = DECODE(l_supplier_tbl(indx)
                                                              .leg_source_system,
                                                              g_source_fsc,
                                                              l_supplier_tbl(indx)
                                                              .attribute7,
                                                              NULL),
                   xss.attribute14                   = DECODE(l_supplier_tbl(indx)
                                                              .leg_source_system,
                                                              g_source_fsc,
                                                              l_supplier_tbl(indx)
                                                              .attribute14,
                                                              NULL),
                   xss.attribute15                   = DECODE(l_supplier_tbl(indx)
                                                              .leg_source_system,
                                                              g_source_fsc,
                                                              l_supplier_tbl(indx)
                                                              .attribute15,
                                                              NULL),
                   xss.terms_name                    = l_supplier_tbl(indx)
                                                      .terms_name,
                   xss.terms_id                      = l_supplier_tbl(indx)
                                                      .terms_id,
                   xss.vendor_interface_id           = DECODE(l_supplier_tbl(indx)
                                                              .process_flag,
                                                              g_validated,
                                                              ap_suppliers_int_s.NEXTVAL,
                                                              g_error,
                                                              NULL),
                   xss.leg_allow_awt_flag            = l_supplier_tbl(indx)
                                                      .leg_allow_awt_flag,
                   xss.leg_global_attribute17        = l_supplier_tbl(indx)
                                                      .leg_global_attribute17
             WHERE xss.interface_txn_id = l_supplier_tbl(indx)
            .interface_txn_id;
        EXCEPTION
          WHEN OTHERS THEN
            print_log_message(SUBSTR('Exception in Procedure validate_supplier while doing Bulk Insert. ' ||
                                     SQLERRM,
                                     1,
                                     2000));
            print_log_message('No. of records in Bulk Exception : ' ||
                              SQL%BULK_EXCEPTIONS.COUNT);
        END;
      END IF;

    END LOOP;
    CLOSE supplier_cur;

    -- Insert remaining records in Error Table
    IF g_source_tab.COUNT > 0 THEN
      xxetn_common_error_pkg.add_error(pov_return_status => l_return_status,
                                       pov_error_msg     => l_error_msg,
                                       pi_source_tab     => g_source_tab,
                                       pin_batch_id      => g_new_batch_id);
      g_source_tab.DELETE;
      g_indx := 0;
    END IF;

  EXCEPTION
    WHEN OTHERS THEN
      pon_retcode := g_ret_error;
      print_log_message(SUBSTR('Exception in Procedure validate_supplier. ' ||
                               SQLERRM,
                               1,
                               2000));
  END validate_supplier;
  --Manoj:END

  --
  -- ========================
  -- Procedure: get_data
  -- =============================================================================
  --   This procedure get_data
  -- =============================================================================
  --  Input Parameters :
  --   None

  --  Output Parameters :
  --   None
  -- -----------------------------------------------------------------------------
  --

  PROCEDURE get_data IS
    l_return_status  VARCHAR2(1);
    l_error_message  VARCHAR2(2000);
    l_last_stmt      NUMBER;
    l_log_err_msg    VARCHAR2(2000);
    l_log_ret_status VARCHAR2(50);

    l_err_record  NUMBER;
    l_request_id  NUMBER; -- v1.1
    l_return_stat VARCHAR2(1); -- v1.1
    l_return_msg  VARCHAR2(2000); -- v1.1

    -- Supplier Bank Record Type
    TYPE supp_bank_ext_rec IS RECORD(
      interface_txn_id    xxap_supplier_banks_stg_R12.interface_txn_id%TYPE,
      batch_id            xxap_supplier_banks_stg_R12.batch_id%TYPE,
      run_sequence_id     xxap_supplier_banks_stg_R12.run_sequence_id%TYPE,
      leg_bank_account_id xxap_supplier_banks_stg_R12.leg_bank_account_id%TYPE,
      leg_country         xxap_supplier_banks_stg_R12.leg_country%TYPE,
      leg_bank_name       xxap_supplier_banks_stg_R12.leg_bank_name%TYPE,
      leg_bank_number     xxap_supplier_banks_stg_R12.leg_bank_number%TYPE
      -- ,leg_bank_branch_id                    xxap_supplier_banks_stg_R12.leg_bank_branch_id%TYPE
      ,
      leg_bank_institution_type xxap_supplier_banks_stg_R12.leg_bank_institution_type%TYPE,
      leg_bank_name_alt         xxap_supplier_banks_stg_R12.leg_bank_name_alt%TYPE,
      leg_description           xxap_supplier_banks_stg_R12.leg_description%TYPE,
      leg_inactive_date         xxap_supplier_banks_stg_R12.leg_inactive_date%TYPE,
      leg_address1              xxap_supplier_banks_stg_R12.leg_address1%TYPE,
      leg_address2              xxap_supplier_banks_stg_R12.leg_address2%TYPE,
      leg_address3              xxap_supplier_banks_stg_R12.leg_address3%TYPE,
      leg_city                  xxap_supplier_banks_stg_R12.leg_city%TYPE,
      leg_state                 xxap_supplier_banks_stg_R12.leg_state%TYPE,
      leg_postal_code           xxap_supplier_banks_stg_R12.leg_postal_code%TYPE,
      bank_party_id             xxap_supplier_banks_stg_R12.bank_party_id%TYPE,
      site_location_id          xxap_supplier_banks_stg_R12.site_location_id%TYPE,
      tax_payer_id              xxap_supplier_banks_stg_R12.tax_payer_id%TYPE,
      vendor_id                 xxap_supplier_banks_stg_R12.vendor_id%TYPE,
      vendor_site_id            xxap_supplier_banks_stg_R12.vendor_site_id%TYPE,
      creation_date             xxap_supplier_banks_stg_R12.creation_date%TYPE,
      created_by                xxap_supplier_banks_stg_R12.created_by%TYPE,
      last_updated_date         xxap_supplier_banks_stg_R12.last_updated_date%TYPE,
      last_updated_by           xxap_supplier_banks_stg_R12.last_updated_by%TYPE,
      last_update_login         xxap_supplier_banks_stg_R12.last_update_login%TYPE,
      program_application_id    xxap_supplier_banks_stg_R12.program_application_id%TYPE,
      program_id                xxap_supplier_banks_stg_R12.program_id%TYPE,
      program_update_date       xxap_supplier_banks_stg_R12.program_update_date%TYPE,
      request_id                xxap_supplier_banks_stg_R12.request_id%TYPE,
      process_flag              xxap_supplier_banks_stg_R12.process_flag%TYPE,
      error_type                xxap_supplier_banks_stg_R12.error_type%TYPE,
      attribute_category        xxap_supplier_banks_stg_R12.attribute_category%TYPE,
      attribute1                xxap_supplier_banks_stg_R12.attribute1%TYPE,
      attribute2                xxap_supplier_banks_stg_R12.attribute2%TYPE,
      attribute3                xxap_supplier_banks_stg_R12.attribute3%TYPE,
      attribute4                xxap_supplier_banks_stg_R12.attribute4%TYPE,
      attribute5                xxap_supplier_banks_stg_R12.attribute5%TYPE,
      attribute6                xxap_supplier_banks_stg_R12.attribute6%TYPE,
      attribute7                xxap_supplier_banks_stg_R12.attribute7%TYPE,
      attribute8                xxap_supplier_banks_stg_R12.attribute8%TYPE,
      attribute9                xxap_supplier_banks_stg_R12.attribute9%TYPE,
      attribute10               xxap_supplier_banks_stg_R12.attribute10%TYPE,
      attribute11               xxap_supplier_banks_stg_R12.attribute11%TYPE,
      attribute12               xxap_supplier_banks_stg_R12.attribute12%TYPE,
      attribute13               xxap_supplier_banks_stg_R12.attribute13%TYPE,
      attribute14               xxap_supplier_banks_stg_R12.attribute14%TYPE,
      attribute15               xxap_supplier_banks_stg_R12.attribute15%TYPE,
      leg_source_system         xxap_supplier_banks_stg_R12.leg_source_system%TYPE,
      leg_request_id            xxap_supplier_banks_stg_R12.leg_request_id%TYPE,
      leg_seq_num               xxap_supplier_banks_stg_R12.leg_seq_num%TYPE,
      leg_process_flag          xxap_supplier_banks_stg_R12.leg_process_flag%TYPE);

    -- Supplier Bank Branch Record Type
    TYPE supp_branch_ext_rec IS RECORD(
      interface_txn_id              xxap_supplier_branches_stg_R12.interface_txn_id%TYPE,
      batch_id                      xxap_supplier_branches_stg_R12.batch_id%TYPE,
      run_sequence_id               xxap_supplier_branches_stg_R12.run_sequence_id%TYPE,
      leg_bank_branch_id            xxap_supplier_branches_stg_R12.leg_bank_branch_id%TYPE ,
      leg_bank_name                 xxap_supplier_branches_stg_R12.leg_bank_name%TYPE,
      leg_bank_number               xxap_supplier_branches_stg_R12.leg_bank_number%TYPE,
      leg_bank_branch_name          xxap_supplier_branches_stg_R12.leg_bank_branch_name%TYPE,
      leg_branch_number             xxap_supplier_branches_stg_R12.leg_branch_number%TYPE,
      leg_bank_branch_name_alt      xxap_supplier_branches_stg_R12.leg_bank_branch_name_alt%TYPE,
      leg_attribute5                xxap_supplier_branches_stg_R12.leg_attribute5%TYPE,
      leg_bank_branch_type          xxap_supplier_branches_stg_R12.leg_bank_branch_type%TYPE,
      leg_global_attribute_category xxap_supplier_branches_stg_R12.leg_global_attribute_category%TYPE,
      leg_global_attribute1         xxap_supplier_branches_stg_R12.leg_global_attribute1%TYPE,
      leg_global_attribute2         xxap_supplier_branches_stg_R12.leg_global_attribute2%TYPE,
      leg_global_attribute3         xxap_supplier_branches_stg_R12.leg_global_attribute3%TYPE,
      leg_global_attribute4         xxap_supplier_branches_stg_R12.leg_global_attribute4%TYPE,
      leg_global_attribute5         xxap_supplier_branches_stg_R12.leg_global_attribute5%TYPE,
      leg_global_attribute6         xxap_supplier_branches_stg_R12.leg_global_attribute6%TYPE,
      leg_global_attribute7         xxap_supplier_branches_stg_R12.leg_global_attribute7%TYPE,
      leg_global_attribute8         xxap_supplier_branches_stg_R12.leg_global_attribute8%TYPE,
      leg_global_attribute9         xxap_supplier_branches_stg_R12.leg_global_attribute9%TYPE,
      leg_global_attribute10        xxap_supplier_branches_stg_R12.leg_global_attribute10%TYPE,
      leg_global_attribute11        xxap_supplier_branches_stg_R12.leg_global_attribute11%TYPE,
      leg_global_attribute12        xxap_supplier_branches_stg_R12.leg_global_attribute12%TYPE,
      leg_global_attribute13        xxap_supplier_branches_stg_R12.leg_global_attribute13%TYPE,
      leg_global_attribute14        xxap_supplier_branches_stg_R12.leg_global_attribute14%TYPE,
      leg_global_attribute15        xxap_supplier_branches_stg_R12.leg_global_attribute15%TYPE,
      leg_global_attribute16        xxap_supplier_branches_stg_R12.leg_global_attribute16%TYPE,
      leg_global_attribute17        xxap_supplier_branches_stg_R12.leg_global_attribute17%TYPE,
      leg_global_attribute18        xxap_supplier_branches_stg_R12.leg_global_attribute18%TYPE,
      leg_global_attribute19        xxap_supplier_branches_stg_R12.leg_global_attribute19%TYPE,
      leg_global_attribute20        xxap_supplier_branches_stg_R12.leg_global_attribute20%TYPE,
      leg_address_lines_alt         xxap_supplier_branches_stg_R12.leg_address_lines_alt%TYPE,
      leg_description               xxap_supplier_branches_stg_R12.leg_description%TYPE,
      leg_rfc_identifier            xxap_supplier_branches_stg_R12.leg_rfc_identifier%TYPE,
      leg_start_date                xxap_supplier_branches_stg_R12.leg_start_date%TYPE,
      leg_end_date                  xxap_supplier_branches_stg_R12.leg_end_date%TYPE,
      leg_address_line1             xxap_supplier_branches_stg_R12.leg_address_line1%TYPE,
      leg_address_line2             xxap_supplier_branches_stg_R12.leg_address_line2%TYPE,
      leg_address_line3             xxap_supplier_branches_stg_R12.leg_address_line3%TYPE,
      leg_address_line4             xxap_supplier_branches_stg_R12.leg_address_line4%TYPE,
      leg_city                      xxap_supplier_branches_stg_R12.leg_city%TYPE,
      leg_state                     xxap_supplier_branches_stg_R12.leg_state%TYPE,
      leg_province                  xxap_supplier_branches_stg_R12.leg_province%TYPE,
      leg_zip                       xxap_supplier_branches_stg_R12.leg_zip%TYPE,
      leg_address_style             xxap_supplier_branches_stg_R12.leg_address_style%TYPE,
      leg_county                    xxap_supplier_branches_stg_R12.leg_county%TYPE,
      leg_country                   xxap_supplier_branches_stg_R12.leg_country%TYPE,
      branch_party_id               xxap_supplier_branches_stg_R12.branch_party_id%TYPE,
      bank_party_id                 xxap_supplier_branches_stg_R12.bank_party_id%TYPE,
      branch_location_id            xxap_supplier_branches_stg_R12.branch_location_id%TYPE,
      branch_site_use_id            xxap_supplier_branches_stg_R12.branch_site_use_id%TYPE,
      creation_date                 xxap_supplier_branches_stg_R12.creation_date%TYPE,
      created_by                    xxap_supplier_branches_stg_R12.created_by%TYPE,
      last_update_date              xxap_supplier_branches_stg_R12.last_update_date%TYPE,
      last_updated_by               xxap_supplier_branches_stg_R12.last_updated_by%TYPE,
      last_update_login             xxap_supplier_branches_stg_R12.last_update_login%TYPE,
      program_application_id        xxap_supplier_branches_stg_R12.program_application_id%TYPE,
      program_id                    xxap_supplier_branches_stg_R12.program_id%TYPE,
      program_update_date           xxap_supplier_branches_stg_R12.program_update_date%TYPE,
      request_id                    xxap_supplier_branches_stg_R12.request_id%TYPE,
      process_flag                  xxap_supplier_branches_stg_R12.process_flag%TYPE,
      error_type                    xxap_supplier_branches_stg_R12.error_type%TYPE,
      attribute_category            xxap_supplier_branches_stg_R12.attribute_category%TYPE,
      attribute1                    xxap_supplier_branches_stg_R12.attribute1%TYPE,
      attribute2                    xxap_supplier_branches_stg_R12.attribute2%TYPE,
      attribute3                    xxap_supplier_branches_stg_R12.attribute3%TYPE,
      attribute4                    xxap_supplier_branches_stg_R12.attribute4%TYPE,
      attribute5                    xxap_supplier_branches_stg_R12.attribute5%TYPE,
      attribute6                    xxap_supplier_branches_stg_R12.attribute6%TYPE,
      attribute7                    xxap_supplier_branches_stg_R12.attribute7%TYPE,
      attribute8                    xxap_supplier_branches_stg_R12.attribute8%TYPE,
      attribute9                    xxap_supplier_branches_stg_R12.attribute9%TYPE,
      attribute10                   xxap_supplier_branches_stg_R12.attribute10%TYPE,
      attribute11                   xxap_supplier_branches_stg_R12.attribute11%TYPE,
      attribute12                   xxap_supplier_branches_stg_R12.attribute12%TYPE,
      attribute13                   xxap_supplier_branches_stg_R12.attribute13%TYPE,
      attribute14                   xxap_supplier_branches_stg_R12.attribute14%TYPE,
      attribute15                   xxap_supplier_branches_stg_R12.attribute15%TYPE,
      leg_source_system             xxap_supplier_branches_stg_R12.leg_source_system%TYPE,
      leg_request_id                xxap_supplier_branches_stg_R12.leg_request_id%TYPE,
      leg_seq_num                   xxap_supplier_branches_stg_R12.leg_seq_num%TYPE,
      leg_process_flag              xxap_supplier_branches_stg_R12.leg_process_flag%TYPE,
      leg_institution_type          xxap_supplier_branches_stg_R12.leg_institution_type%TYPE ,
      bank_branch_name_alt          xxap_supplier_branches_stg_R12.bank_branch_name_alt%TYPE );

    -- Supplier Bank Account Record Type
    TYPE supp_account_ext_rec IS RECORD(
      interface_txn_id          xxap_supplier_bankaccnts_R12.interface_txn_id%TYPE,
      batch_id                  xxap_supplier_bankaccnts_R12.batch_id%TYPE,
      run_sequence_id           xxap_supplier_bankaccnts_R12.run_sequence_id%TYPE,
      leg_vendor_name           xxap_supplier_bankaccnts_R12.leg_vendor_name%TYPE,
      leg_vendor_num            xxap_supplier_bankaccnts_R12.leg_vendor_num%TYPE,
      leg_vendor_site_code      xxap_supplier_bankaccnts_R12.leg_vendor_site_code%TYPE,
      leg_operating_unit_name   xxap_supplier_bankaccnts_R12.leg_operating_unit_name%TYPE,
      leg_bank_name             xxap_supplier_bankaccnts_R12.leg_bank_name%TYPE,
      leg_branch_name           xxap_supplier_bankaccnts_R12.leg_branch_name%TYPE,
      leg_account_name          xxap_supplier_bankaccnts_R12.leg_account_name%TYPE,
      leg_account_num           xxap_supplier_bankaccnts_R12.leg_account_num%TYPE,
      leg_check_digits          xxap_supplier_bankaccnts_R12.leg_check_digits%TYPE,
      leg_iban                  xxap_supplier_bankaccnts_R12.leg_iban%TYPE,
      leg_currency              xxap_supplier_bankaccnts_R12.leg_currency%TYPE,
      leg_country               xxap_supplier_bankaccnts_R12.leg_country%TYPE,
      leg_primary_flag          xxap_supplier_bankaccnts_R12.leg_primary_flag%TYPE,
      leg_account_name_alt      xxap_supplier_bankaccnts_R12.leg_account_name_alt%TYPE,
      leg_agency_loc_code       xxap_supplier_bankaccnts_R12.leg_agency_loc_code%TYPE,
      leg_account_type          xxap_supplier_bankaccnts_R12.leg_account_type%TYPE,
      leg_bank_account_type     xxap_supplier_bankaccnts_R12.leg_bank_account_type%TYPE,
      leg_multi_currency_flag   xxap_supplier_bankaccnts_R12.leg_multi_currency_flag%TYPE,
      leg_secondary_account_ref xxap_supplier_bankaccnts_R12.leg_secondary_account_ref%TYPE,
      leg_description           xxap_supplier_bankaccnts_R12.leg_description%TYPE,
      leg_end_Date              xxap_supplier_bankaccnts_R12.leg_end_Date%TYPE,
      ext_bank_account_id       xxap_supplier_bankaccnts_R12.ext_bank_account_id%TYPE,
      branch_id                 xxap_supplier_bankaccnts_R12.branch_id%TYPE,
      bank_id                   xxap_supplier_bankaccnts_R12.bank_id%TYPE,
      iban                      xxap_supplier_bankaccnts_R12.iban%TYPE,
      bank_account_type         xxap_supplier_bankaccnts_R12.bank_account_type%TYPE,
      object_version_number     xxap_supplier_bankaccnts_R12.object_version_number%TYPE,
      org_id                    xxap_supplier_bankaccnts_R12.org_id%TYPE,
      vendor_id                 xxap_supplier_bankaccnts_R12.vendor_id%TYPE,
      vendor_site_id            xxap_supplier_bankaccnts_R12.vendor_site_id%TYPE,
      party_id                  xxap_supplier_bankaccnts_R12.party_id%TYPE,
      party_site_id             xxap_supplier_bankaccnts_R12.party_site_id%TYPE,
      bank_branch_type          xxap_supplier_bankaccnts_R12.bank_branch_type%TYPE,
      creation_date             xxap_supplier_bankaccnts_R12.creation_date%TYPE,
      created_by                xxap_supplier_bankaccnts_R12.created_by%TYPE,
      last_update_date          xxap_supplier_bankaccnts_R12.last_update_date%TYPE,
      last_updated_by           xxap_supplier_bankaccnts_R12.last_updated_by%TYPE,
      last_update_login         xxap_supplier_bankaccnts_R12.last_update_login%TYPE,
      program_application_id    xxap_supplier_bankaccnts_R12.program_application_id%TYPE,
      program_id                xxap_supplier_bankaccnts_R12.program_id%TYPE,
      program_update_date       xxap_supplier_bankaccnts_R12.program_update_date%TYPE,
      request_id                xxap_supplier_bankaccnts_R12.request_id%TYPE,
      process_flag              xxap_supplier_bankaccnts_R12.process_flag%TYPE,
      error_type                xxap_supplier_bankaccnts_R12.error_type%TYPE,
      attribute_category        xxap_supplier_bankaccnts_R12.attribute_category%TYPE,
      attribute1                xxap_supplier_bankaccnts_R12.attribute1%TYPE,
      attribute2                xxap_supplier_bankaccnts_R12.attribute2%TYPE,
      attribute3                xxap_supplier_bankaccnts_R12.attribute3%TYPE,
      attribute4                xxap_supplier_bankaccnts_R12.attribute4%TYPE,
      attribute5                xxap_supplier_bankaccnts_R12.attribute5%TYPE,
      attribute6                xxap_supplier_bankaccnts_R12.attribute6%TYPE,
      attribute7                xxap_supplier_bankaccnts_R12.attribute7%TYPE,
      attribute8                xxap_supplier_bankaccnts_R12.attribute8%TYPE,
      attribute9                xxap_supplier_bankaccnts_R12.attribute9%TYPE,
      attribute10               xxap_supplier_bankaccnts_R12.attribute10%TYPE,
      attribute11               xxap_supplier_bankaccnts_R12.attribute11%TYPE,
      attribute12               xxap_supplier_bankaccnts_R12.attribute12%TYPE,
      attribute13               xxap_supplier_bankaccnts_R12.attribute13%TYPE,
      attribute14               xxap_supplier_bankaccnts_R12.attribute14%TYPE,
      attribute15               xxap_supplier_bankaccnts_R12.attribute15%TYPE,
      leg_source_system         xxap_supplier_bankaccnts_R12.leg_source_system%TYPE,
      leg_request_id            xxap_supplier_bankaccnts_R12.leg_request_id%TYPE,
      leg_seq_num               xxap_supplier_bankaccnts_R12.leg_seq_num%TYPE,
      leg_process_flag          xxap_supplier_bankaccnts_R12.leg_process_flag%TYPE
      --,leg_operating_unit_name       xxap_supplier_bankaccnts_R12.leg_operating_unit_name%TYPE ---added--
      ,
      LEG_BANK_BRANCH_ID          xxap_supplier_bankaccnts_R12.LEG_BANK_BRANCH_ID%TYPE -- added--
      ,
      LEG_ALLOW_MULTI_ASSIGN_FLAG xxap_supplier_bankaccnts_R12.LEG_ALLOW_MULTI_ASSIGN_FLAG%TYPE -- added--
      --,last_update_date              xxap_supplier_bankaccnts_R12.last_update_date%TYPE -- added--
      ,
      leg_bank_account_uses_id      xxap_supplier_bankaccnts_R12.leg_bank_account_uses_id%TYPE,
      leg_vendor_id                 xxap_supplier_bankaccnts_R12.leg_vendor_id%TYPE,
      leg_vendor_site_id            xxap_supplier_bankaccnts_R12.leg_vendor_site_id%TYPE,
      leg_external_bank_account_id  xxap_supplier_bankaccnts_R12.leg_external_bank_account_id%TYPE,
      leg_global_attribute_category xxap_supplier_bankaccnts_R12.leg_global_attribute_category%TYPE,
      leg_global_attribute1         xxap_supplier_bankaccnts_R12.leg_global_attribute1%TYPE,
      leg_global_attribute2         xxap_supplier_bankaccnts_R12.leg_global_attribute2%TYPE,
      leg_global_attribute3         xxap_supplier_bankaccnts_R12.leg_global_attribute3%TYPE,
      leg_global_attribute4         xxap_supplier_bankaccnts_R12.leg_global_attribute4%TYPE,
      leg_global_attribute5         xxap_supplier_bankaccnts_R12.leg_global_attribute5%TYPE,
      leg_global_attribute6         xxap_supplier_bankaccnts_R12.leg_global_attribute6%TYPE,
      leg_global_attribute7         xxap_supplier_bankaccnts_R12.leg_global_attribute7%TYPE,
      leg_global_attribute8         xxap_supplier_bankaccnts_R12.leg_global_attribute8%TYPE,
      leg_global_attribute9         xxap_supplier_bankaccnts_R12.leg_global_attribute9%TYPE,
      leg_global_attribute10        xxap_supplier_bankaccnts_R12.leg_global_attribute10%TYPE,
      leg_global_attribute11        xxap_supplier_bankaccnts_R12.leg_global_attribute11%TYPE,
      leg_global_attribute12        xxap_supplier_bankaccnts_R12.leg_global_attribute12%TYPE,
      leg_global_attribute13        xxap_supplier_bankaccnts_R12.leg_global_attribute13%TYPE,
      leg_global_attribute14        xxap_supplier_bankaccnts_R12.leg_global_attribute14%TYPE,
      leg_global_attribute15        xxap_supplier_bankaccnts_R12.leg_global_attribute15%TYPE,
      leg_global_attribute16        xxap_supplier_bankaccnts_R12.leg_global_attribute16%TYPE,
      leg_global_attribute17        xxap_supplier_bankaccnts_R12.leg_global_attribute17%TYPE,
      leg_global_attribute18        xxap_supplier_bankaccnts_R12.leg_global_attribute18%TYPE,
      leg_global_attribute19        xxap_supplier_bankaccnts_R12.leg_global_attribute19%TYPE,
      leg_global_attribute20        xxap_supplier_bankaccnts_R12.leg_global_attribute20%TYPE,
      leg_attribute2                xxap_supplier_bankaccnts_R12.leg_attribute2%TYPE,
      leg_org_id                    xxap_supplier_bankaccnts_R12.leg_org_id%TYPE,
      leg_bank_account_id           xxap_supplier_bankaccnts_R12.leg_bank_account_id%TYPE,
      leg_set_of_books_id           xxap_supplier_bankaccnts_R12.leg_set_of_books_id%TYPE
      --,LEG_PREPAY_CODE_COMB_ID       xxap_supplier_bankaccnts_R12.LEG_PREPAY_CODE_COMB_ID%TYPE   ---- added
      --,LEG_SERVICES_TOLERANCES       xxap_supplier_bankaccnts_R12.LEG_SERVICES_TOLERANCES%TYPE   ---- added
      );

    -- PLSQL Table based on Record Type for Supplier Bank
    TYPE supp_bank_ext_tbl IS TABLE OF supp_bank_ext_rec INDEX BY BINARY_INTEGER;

    l_supp_bank_ext_tbl supp_bank_ext_tbl;

    -- PLSQL Table based on Record Type for Supplier Bank Branch
    TYPE supp_branch_ext_tbl IS TABLE OF supp_branch_ext_rec INDEX BY BINARY_INTEGER;

    l_supp_branch_ext_tbl supp_branch_ext_tbl;

    -- PLSQL Table based on Record Type for Supplier Account
    TYPE supp_account_ext_tbl IS TABLE OF supp_account_ext_rec INDEX BY BINARY_INTEGER;

    l_supp_account_ext_tbl supp_account_ext_tbl;

    -- Supplier Bank Extraction Table cursor
    CURSOR supp_bank_ext_cur IS
      SELECT xsber.interface_txn_id,
             xsber.batch_id,
             xsber.run_sequence_id,
             xsber.leg_bank_account_id,
             xsber.leg_country,
             xsber.leg_bank_name,
             xsber.leg_bank_number
             --  ,xsber.leg_bank_branch_id
            ,
             xsber.leg_bank_institution_type,
             xsber.leg_bank_name_alt,
             xsber.leg_description,
             xsber.leg_inactive_date,
             xsber.leg_address1,
             xsber.leg_address2,
             xsber.leg_address3,
             xsber.leg_city,
             xsber.leg_state,
             xsber.leg_postal_code,
             xsber.bank_party_id,
             xsber.site_location_id,
             xsber.tax_payer_id,
             xsber.vendor_id,
             xsber.vendor_site_id,
             xsber.creation_date,
             xsber.created_by,
             xsber.last_updated_date,
             xsber.last_updated_by,
             xsber.last_update_login,
             xsber.program_application_id,
             xsber.program_id,
             xsber.program_update_date,
             xsber.request_id,
             xsber.process_flag,
             xsber.error_type,
             xsber.attribute_category,
             xsber.attribute1,
             xsber.attribute2,
             xsber.attribute3,
             xsber.attribute4,
             xsber.attribute5,
             xsber.attribute6,
             xsber.attribute7,
             xsber.attribute8,
             xsber.attribute9,
             xsber.attribute10,
             xsber.attribute11,
             xsber.attribute12,
             xsber.attribute13,
             xsber.attribute14,
             xsber.attribute15,
             xsber.leg_source_system,
             xsber.leg_request_id,
             xsber.leg_seq_num,
             xsber.leg_process_flag
        FROM xxap_supplier_banks_stg_R12 xsber
       WHERE xsber.leg_process_flag = g_validated
         AND NOT EXISTS
       (SELECT 1
                FROM xxap_supplier_banks_stg xsbs
               WHERE xsbs.interface_txn_id = xsber.interface_txn_id);

    -- Supplier Bank Branch Extraction Table cursor
    CURSOR supp_branch_ext_cur IS
      SELECT xsber.interface_txn_id,
             xsber.batch_id,
             xsber.run_sequence_id,
             xsber.leg_bank_branch_id,
             xsber.leg_bank_name,
             xsber.leg_bank_number,
             xsber.leg_bank_branch_name,
             xsber.leg_branch_number,
             xsber.leg_bank_branch_name_alt,
             xsber.leg_attribute5,
             xsber.leg_bank_branch_type,
             xsber.leg_global_attribute_category,
             xsber.leg_global_attribute1,
             xsber.leg_global_attribute2,
             xsber.leg_global_attribute3,
             xsber.leg_global_attribute4,
             xsber.leg_global_attribute5,
             xsber.leg_global_attribute6,
             xsber.leg_global_attribute7,
             xsber.leg_global_attribute8,
             xsber.leg_global_attribute9,
             xsber.leg_global_attribute10,
             xsber.leg_global_attribute11,
             xsber.leg_global_attribute12,
             xsber.leg_global_attribute13,
             xsber.leg_global_attribute14,
             xsber.leg_global_attribute15,
             xsber.leg_global_attribute16,
             xsber.leg_global_attribute17,
             xsber.leg_global_attribute18,
             xsber.leg_global_attribute19,
             xsber.leg_global_attribute20,
             xsber.leg_address_lines_alt,
             xsber.leg_description,
             xsber.leg_rfc_identifier,
             xsber.leg_start_date,
             xsber.leg_end_date,
             xsber.leg_address_line1,
             xsber.leg_address_line2,
             xsber.leg_address_line3,
             xsber.leg_address_line4,
             xsber.leg_city,
             xsber.leg_state,
             xsber.leg_province,
             xsber.leg_zip,
             xsber.leg_address_style,
             xsber.leg_county,
             xsber.leg_country,
             xsber.branch_party_id,
             xsber.bank_party_id,
             xsber.branch_location_id,
             xsber.branch_site_use_id,
             xsber.creation_date,
             xsber.created_by,
             xsber.last_update_date,
             xsber.last_updated_by,
             xsber.last_update_login,
             xsber.program_application_id,
             xsber.program_id,
             xsber.program_update_date,
             xsber.request_id,
             xsber.process_flag,
             xsber.error_type,
             xsber.attribute_category,
             xsber.attribute1,
             xsber.attribute2,
             xsber.attribute3,
             xsber.attribute4,
             xsber.attribute5,
             xsber.attribute6,
             xsber.attribute7,
             xsber.attribute8,
             xsber.attribute9,
             xsber.attribute10,
             xsber.attribute11,
             xsber.attribute12,
             xsber.attribute13,
             xsber.attribute14,
             xsber.attribute15,
             xsber.leg_source_system,
             xsber.leg_request_id,
             xsber.leg_seq_num,
             xsber.leg_process_flag,
             xsber.leg_institution_type,
             xsber.bank_branch_name_alt
        FROM xxap_supplier_branches_stg_R12 xsber
       WHERE xsber.leg_process_flag = g_validated
         AND NOT EXISTS
       (SELECT 1
                FROM xxap_supplier_branches_stg xsbs
               WHERE xsbs.interface_txn_id = xsber.interface_txn_id);

    -- Supplier Bank Account Extraction Table cursor
    CURSOR supp_account_ext_cur IS
      SELECT interface_txn_id,
             batch_id,
             run_sequence_id,
             leg_vendor_name,
             leg_vendor_num,
             leg_vendor_site_code,
             leg_operating_unit_name,
             leg_bank_name,
             leg_branch_name,
             leg_account_name,
             leg_account_num,
             leg_check_digits,
             leg_iban,
             leg_currency,
             leg_country,
             leg_primary_flag,
             leg_account_name_alt,
             leg_agency_loc_code,
             leg_account_type,
             leg_bank_account_type,
             leg_multi_currency_flag,
             leg_secondary_account_ref,
             leg_description,
             leg_end_date,
             ext_bank_account_id,
             branch_id,
             bank_id,
             iban,
             bank_account_type,
             object_version_number,
             org_id,
             vendor_id,
             vendor_site_id,
             party_id,
             party_site_id,
             bank_branch_type,
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
             error_type,
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
             --,leg_operating_unit_name ---added
            ,
             leg_bank_branch_id ---added
            ,
             leg_allow_multi_assign_flag -- added
             --,last_update_date     ---added
            ,
             leg_bank_account_uses_id,
             leg_vendor_id,
             leg_vendor_site_id,
             leg_external_bank_account_id,
             leg_global_attribute_category,
             leg_global_attribute1,
             leg_global_attribute2,
             leg_global_attribute3,
             leg_global_attribute4,
             leg_global_attribute5,
             leg_global_attribute6,
             leg_global_attribute7,
             leg_global_attribute8,
             leg_global_attribute9,
             leg_global_attribute10,
             leg_global_attribute11,
             leg_global_attribute12,
             leg_global_attribute13,
             leg_global_attribute14,
             leg_global_attribute15,
             leg_global_attribute16,
             leg_global_attribute17,
             leg_global_attribute18,
             leg_global_attribute19,
             leg_global_attribute20,
             leg_attribute2,
             leg_org_id,
             leg_bank_account_id,
             leg_set_of_books_id
      /* ,leg_institution_type*/
        FROM xxap_supplier_bankaccnts_R12 xsber
       WHERE xsber.leg_process_flag = g_validated
         AND NOT EXISTS
       (SELECT 1
                FROM xxap_supplier_bankaccnts_stg xsbs
               WHERE xsbs.interface_txn_id = xsber.interface_txn_id);

    -- Supplier Extraction Table cursor
    CURSOR supp_ext_cur IS
      SELECT xser.*
        FROM xxap_supplier_stg_R12 xser
       WHERE xser.leg_process_flag = g_validated
         AND NOT EXISTS
       (SELECT 1
                FROM xxap_suppliers_stg xss
               WHERE xss.interface_txn_id = xser.interface_txn_id);

    TYPE supp_ext_t IS TABLE OF supp_ext_cur%ROWTYPE INDEX BY BINARY_INTEGER;
    l_supp_ext_tbl supp_ext_t;

    -- Supplier Sites Extraction Table cursor
    CURSOR suppsite_ext_cur IS
      SELECT xsser.*
        FROM xxap_supplier_sites_stg_R12 xsser
       WHERE xsser.leg_process_flag = g_validated
         AND NOT EXISTS
       (SELECT 1
                FROM xxap_supplier_sites_stg xsss
               WHERE xsss.interface_txn_id = xsser.interface_txn_id);

    TYPE suppsite_ext_t IS TABLE OF suppsite_ext_cur%ROWTYPE INDEX BY BINARY_INTEGER;
    l_suppsite_ext_tbl suppsite_ext_t;

    -- Supplier Contacts Extraction Table cursor
    CURSOR suppcont_ext_cur IS
      SELECT xscer.*
        FROM xxap_supplier_contacts_stg_R12 xscer
       WHERE xscer.leg_process_flag = g_validated
         AND NOT EXISTS
       (SELECT 1
                FROM xxap_supplier_contacts_stg xscs
               WHERE xscs.interface_txn_id = xscer.interface_txn_id);

    TYPE suppcont_ext_t IS TABLE OF suppcont_ext_cur%ROWTYPE INDEX BY BINARY_INTEGER;
    l_suppcont_ext_tbl suppcont_ext_t;

  BEGIN

    xxetn_debug_pkg.add_debug(' + get_data +');
    l_return_status := fnd_api.g_ret_sts_success;
    l_error_message := NULL;
    g_total_count   := 0;
    g_failed_count  := 0;


    -- BANK
    -- Insert bank data from extract staging area to R12 staging table

    IF g_entity = g_bank THEN
      -- Open Cursor for all bank records
      OPEN supp_bank_ext_cur;
      LOOP

        l_supp_bank_ext_tbl.DELETE;

        FETCH supp_bank_ext_cur BULK COLLECT
          INTO l_supp_bank_ext_tbl LIMIT 1000; --limit size of Bulk Collect

        -- Get Total Count
        g_total_count := g_total_count + l_supp_bank_ext_tbl.COUNT;

        EXIT WHEN l_supp_bank_ext_tbl.COUNT = 0;

        BEGIN

          -- Bulk Insert into Conversion table
          FORALL indx IN 1 .. l_supp_bank_ext_tbl.COUNT SAVE EXCEPTIONS
            INSERT INTO xxap_supplier_banks_stg
              (interface_txn_id,
               batch_id,
               run_sequence_id,
               leg_bank_account_id,
               leg_country,
               leg_bank_name,
               leg_bank_number
               --,leg_bank_branch_id
              ,
               leg_bank_institution_type,
               leg_bank_name_alt,
               leg_description,
               leg_inactive_date,
               leg_address1,
               leg_address2,
               leg_address3,
               leg_city,
               leg_state,
               leg_postal_code,
               bank_party_id,
               site_location_id,
               tax_payer_id,
               vendor_id,
               vendor_site_id,
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
               error_type,
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
            VALUES
              (l_supp_bank_ext_tbl(indx).interface_txn_id,
               l_supp_bank_ext_tbl(indx).batch_id,
               l_supp_bank_ext_tbl(indx).run_sequence_id,
               l_supp_bank_ext_tbl(indx).leg_bank_account_id,
               l_supp_bank_ext_tbl(indx).leg_country,
               l_supp_bank_ext_tbl(indx).leg_bank_name,
               l_supp_bank_ext_tbl(indx).leg_bank_number
               --       ,l_supp_bank_ext_tbl (indx).leg_bank_branch_id
              ,
               l_supp_bank_ext_tbl(indx).leg_bank_institution_type,
               l_supp_bank_ext_tbl(indx).leg_bank_name_alt,
               l_supp_bank_ext_tbl(indx).leg_description,
               l_supp_bank_ext_tbl(indx).leg_inactive_date,
               l_supp_bank_ext_tbl(indx).leg_address1,
               l_supp_bank_ext_tbl(indx).leg_address2,
               l_supp_bank_ext_tbl(indx).leg_address3,
               l_supp_bank_ext_tbl(indx).leg_city,
               l_supp_bank_ext_tbl(indx).leg_state,
               l_supp_bank_ext_tbl(indx).leg_postal_code,
               l_supp_bank_ext_tbl(indx).bank_party_id,
               l_supp_bank_ext_tbl(indx).site_location_id,
               l_supp_bank_ext_tbl(indx).tax_payer_id,
               l_supp_bank_ext_tbl(indx).vendor_id,
               l_supp_bank_ext_tbl(indx).vendor_site_id,
               SYSDATE,
               g_user_id,
               SYSDATE,
               g_user_id,
               g_login_id,
               g_prog_appl_id,
               g_conc_program_id,
               SYSDATE,
               g_request_id,
               g_new,
               l_supp_bank_ext_tbl(indx).error_type,
               l_supp_bank_ext_tbl(indx).attribute_category,
               l_supp_bank_ext_tbl(indx).attribute1,
               l_supp_bank_ext_tbl(indx).attribute2,
               l_supp_bank_ext_tbl(indx).attribute3,
               l_supp_bank_ext_tbl(indx).attribute4,
               l_supp_bank_ext_tbl(indx).attribute5,
               l_supp_bank_ext_tbl(indx).attribute6,
               l_supp_bank_ext_tbl(indx).attribute7,
               l_supp_bank_ext_tbl(indx).attribute8,
               l_supp_bank_ext_tbl(indx).attribute9,
               l_supp_bank_ext_tbl(indx).attribute10,
               l_supp_bank_ext_tbl(indx).attribute11,
               l_supp_bank_ext_tbl(indx).attribute12,
               l_supp_bank_ext_tbl(indx).attribute13,
               l_supp_bank_ext_tbl(indx).attribute14,
               l_supp_bank_ext_tbl(indx).attribute15,
               l_supp_bank_ext_tbl(indx).leg_source_system,
               l_supp_bank_ext_tbl(indx).leg_request_id,
               l_supp_bank_ext_tbl(indx).leg_seq_num,
               l_supp_bank_ext_tbl(indx).leg_process_flag
               --   ,l_supp_bank_ext_tbl (indx).leg_institution_type
               );

        EXCEPTION
          WHEN OTHERS THEN
            FOR l_indx_exp IN 1 .. SQL%BULK_EXCEPTIONS.COUNT LOOP
              l_err_record := l_supp_bank_ext_tbl(SQL%BULK_EXCEPTIONS(l_indx_exp)
                                                  .ERROR_INDEX)
                             .interface_txn_id;
              g_retcode    := '1';
              fnd_file.PUT_LINE(fnd_file.LOG,
                                'Record sequence : ' ||
                                 l_supp_bank_ext_tbl(SQL%BULK_EXCEPTIONS(l_indx_exp)
                                                     .error_index)
                                .interface_txn_id);
              fnd_file.PUT_LINE(fnd_file.LOG,
                                'Error Message : ' ||
                                SQLERRM(-SQL%BULK_EXCEPTIONS(l_indx_exp)
                                        .error_code));

              -- Updating Leg_process_flag to 'E' for failed records
              UPDATE xxap_supplier_banks_stg_R12 xsber
                 SET leg_process_flag  = g_error,
                     last_updated_by   = g_user_id,
                     last_update_login = g_login_id,
                     last_updated_date = SYSDATE
               WHERE xsber.interface_txn_id = l_err_record
                 AND xsber.leg_process_flag = g_validated;

              g_failed_count := g_failed_count + SQL%ROWCOUNT;

              l_error_message := l_error_message || ' ~~ ' ||
                                 SQLERRM(-SQL%BULK_EXCEPTIONS(l_indx_exp)
                                         .ERROR_CODE);
            END LOOP;
        END;

      END LOOP;
      CLOSE supp_bank_ext_cur; -- Close Cursor

      -- Update Successful records in Extraction Table
      UPDATE xxap_supplier_banks_stg_R12 stg2
         SET leg_process_flag  = g_processed,
             last_updated_by   = g_user_id,
             last_update_login = g_login_id,
             last_updated_date = SYSDATE
       WHERE leg_process_flag = g_validated
         AND EXISTS
       (SELECT 1
                FROM xxap_supplier_banks_stg stg1
               WHERE stg1.interface_txn_id = stg2.interface_txn_id);
    END IF;
    -- Insert branch data from extract staging area to R12 staging table


    IF g_entity = g_branch THEN

      -- Open Cursor for all branch records
      OPEN supp_branch_ext_cur;
      LOOP

        l_supp_branch_ext_tbl.DELETE;

        FETCH supp_branch_ext_cur BULK COLLECT
          INTO l_supp_branch_ext_tbl LIMIT 1000; --limit size of Bulk Collect

        -- Get Total Count
        g_total_count := g_total_count + l_supp_branch_ext_tbl.COUNT;

        EXIT WHEN l_supp_branch_ext_tbl.COUNT = 0;

        BEGIN

          -- Bulk Insert into Conversion table
          FORALL indx IN 1 .. l_supp_branch_ext_tbl.COUNT SAVE EXCEPTIONS
            INSERT INTO xxap_supplier_branches_stg
              (interface_txn_id,
               batch_id,
               run_sequence_id,
               leg_bank_branch_id ,
               leg_bank_name,
               leg_bank_number,
               leg_bank_branch_name,
               leg_branch_number,
               leg_bank_branch_name_alt,
               leg_attribute5,
               leg_bank_branch_type,
               leg_global_attribute_category,
               leg_global_attribute1,
               leg_global_attribute2,
               leg_global_attribute3,
               leg_global_attribute4,
               leg_global_attribute5,
               leg_global_attribute6,
               leg_global_attribute7,
               leg_global_attribute8,
               leg_global_attribute9,
               leg_global_attribute10,
               leg_global_attribute11,
               leg_global_attribute12,
               leg_global_attribute13,
               leg_global_attribute14,
               leg_global_attribute15,
               leg_global_attribute16,
               leg_global_attribute17,
               leg_global_attribute18,
               leg_global_attribute19,
               leg_global_attribute20,
               leg_address_lines_alt,
               leg_description,
               leg_rfc_identifier,
               leg_start_date,
               leg_end_date,
               leg_address_line1,
               leg_address_line2,
               leg_address_line3,
               leg_address_line4,
               leg_city,
               leg_state,
               leg_province,
               leg_zip,
               leg_address_style,
               leg_county,
               leg_country,
               branch_party_id,
               bank_party_id,
               branch_location_id,
               branch_site_use_id,
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
               error_type,
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
               leg_institution_type,
               bank_branch_name_alt)
            VALUES
              (l_supp_branch_ext_tbl(indx).interface_txn_id,
               l_supp_branch_ext_tbl(indx).batch_id,
               l_supp_branch_ext_tbl(indx).run_sequence_id,
               l_supp_branch_ext_tbl(indx).leg_bank_branch_id,
               l_supp_branch_ext_tbl(indx).leg_bank_name,
               l_supp_branch_ext_tbl(indx).leg_bank_number,
               l_supp_branch_ext_tbl(indx).leg_bank_branch_name,
               l_supp_branch_ext_tbl(indx).leg_branch_number,
               l_supp_branch_ext_tbl(indx).leg_bank_branch_name_alt,
               l_supp_branch_ext_tbl(indx).leg_attribute5,
               l_supp_branch_ext_tbl(indx).leg_bank_branch_type,
               l_supp_branch_ext_tbl(indx).leg_global_attribute_category,
               l_supp_branch_ext_tbl(indx).leg_global_attribute1,
               l_supp_branch_ext_tbl(indx).leg_global_attribute2,
               l_supp_branch_ext_tbl(indx).leg_global_attribute3,
               l_supp_branch_ext_tbl(indx).leg_global_attribute4,
               l_supp_branch_ext_tbl(indx).leg_global_attribute5,
               l_supp_branch_ext_tbl(indx).leg_global_attribute6,
               l_supp_branch_ext_tbl(indx).leg_global_attribute7,
               l_supp_branch_ext_tbl(indx).leg_global_attribute8,
               l_supp_branch_ext_tbl(indx).leg_global_attribute9,
               l_supp_branch_ext_tbl(indx).leg_global_attribute10,
               l_supp_branch_ext_tbl(indx).leg_global_attribute11,
               l_supp_branch_ext_tbl(indx).leg_global_attribute12,
               l_supp_branch_ext_tbl(indx).leg_global_attribute13,
               l_supp_branch_ext_tbl(indx).leg_global_attribute14,
               l_supp_branch_ext_tbl(indx).leg_global_attribute15,
               l_supp_branch_ext_tbl(indx).leg_global_attribute16,
               l_supp_branch_ext_tbl(indx).leg_global_attribute17,
               l_supp_branch_ext_tbl(indx).leg_global_attribute18,
               l_supp_branch_ext_tbl(indx).leg_global_attribute19,
               l_supp_branch_ext_tbl(indx).leg_global_attribute20,
               l_supp_branch_ext_tbl(indx).leg_address_lines_alt,
               l_supp_branch_ext_tbl(indx).leg_description,
               l_supp_branch_ext_tbl(indx).leg_rfc_identifier,
               l_supp_branch_ext_tbl(indx).leg_start_date,
               l_supp_branch_ext_tbl(indx).leg_end_date,
               l_supp_branch_ext_tbl(indx).leg_address_line1,
               l_supp_branch_ext_tbl(indx).leg_address_line2,
               l_supp_branch_ext_tbl(indx).leg_address_line3,
               l_supp_branch_ext_tbl(indx).leg_address_line4,
               l_supp_branch_ext_tbl(indx).leg_city,
               l_supp_branch_ext_tbl(indx).leg_state,
               l_supp_branch_ext_tbl(indx).leg_province,
               l_supp_branch_ext_tbl(indx).leg_zip,
               l_supp_branch_ext_tbl(indx).leg_address_style,
               l_supp_branch_ext_tbl(indx).leg_county,
               l_supp_branch_ext_tbl(indx).leg_country,
               l_supp_branch_ext_tbl(indx).branch_party_id,
               l_supp_branch_ext_tbl(indx).bank_party_id,
               l_supp_branch_ext_tbl(indx).branch_location_id,
               l_supp_branch_ext_tbl(indx).branch_site_use_id,
               SYSDATE,
               g_user_id,
               SYSDATE,
               g_user_id,
               g_login_id,
               g_prog_appl_id,
               g_conc_program_id,
               SYSDATE,
               g_request_id,
               g_new,
               l_supp_branch_ext_tbl(indx).error_type,
               l_supp_branch_ext_tbl(indx).attribute_category,
               l_supp_branch_ext_tbl(indx).attribute1,
               l_supp_branch_ext_tbl(indx).attribute2,
               l_supp_branch_ext_tbl(indx).attribute3,
               l_supp_branch_ext_tbl(indx).attribute4,
               l_supp_branch_ext_tbl(indx).attribute5,
               l_supp_branch_ext_tbl(indx).attribute6,
               l_supp_branch_ext_tbl(indx).attribute7,
               l_supp_branch_ext_tbl(indx).attribute8,
               l_supp_branch_ext_tbl(indx).attribute9,
               l_supp_branch_ext_tbl(indx).attribute10,
               l_supp_branch_ext_tbl(indx).attribute11,
               l_supp_branch_ext_tbl(indx).attribute12,
               l_supp_branch_ext_tbl(indx).attribute13,
               l_supp_branch_ext_tbl(indx).attribute14,
               l_supp_branch_ext_tbl(indx).attribute15,
               l_supp_branch_ext_tbl(indx).leg_source_system,
               l_supp_branch_ext_tbl(indx).leg_request_id,
               l_supp_branch_ext_tbl(indx).leg_seq_num,
               l_supp_branch_ext_tbl(indx).leg_process_flag,
               l_supp_branch_ext_tbl(indx).leg_institution_type,
               l_supp_branch_ext_tbl(indx).bank_branch_name_alt);

        EXCEPTION
          WHEN OTHERS THEN
            FOR l_indx_exp IN 1 .. SQL%BULK_EXCEPTIONS.COUNT LOOP
              l_err_record := l_supp_branch_ext_tbl(SQL%BULK_EXCEPTIONS(l_indx_exp)
                                                    .ERROR_INDEX)
                             .interface_txn_id;
              g_retcode    := '1';
              fnd_file.PUT_LINE(fnd_file.LOG,
                                'Record sequence : ' ||
                                 l_supp_branch_ext_tbl(SQL%BULK_EXCEPTIONS(l_indx_exp)
                                                       .error_index)
                                .interface_txn_id);
              fnd_file.PUT_LINE(fnd_file.LOG,
                                'Error Message : ' ||
                                SQLERRM(-SQL%BULK_EXCEPTIONS(l_indx_exp)
                                        .error_code));

              -- Updating Leg_process_flag to 'E' for failed records
              UPDATE xxap_supplier_branches_stg_R12 xsber
                 SET leg_process_flag  = g_error,
                     last_updated_by   = g_user_id,
                     last_update_login = g_login_id,
                     last_update_date  = SYSDATE
               WHERE xsber.interface_txn_id = l_err_record
                 AND xsber.leg_process_flag = g_validated;

              g_failed_count := g_failed_count + SQL%ROWCOUNT;

              l_error_message := l_error_message || ' ~~ ' ||
                                 SQLERRM(-SQL%BULK_EXCEPTIONS(l_indx_exp)
                                         .ERROR_CODE);
            END LOOP;
        END;

      END LOOP;
      CLOSE supp_branch_ext_cur; -- Close Cursor

      COMMIT;


      -- Update Successful records in Extraction Table
      UPDATE xxap_supplier_branches_stg_R12 stg2
         SET leg_process_flag  = g_processed,
             last_updated_by   = g_user_id,
             last_update_login = g_login_id,
             last_update_date  = SYSDATE
       WHERE leg_process_flag = g_validated
         AND EXISTS
       (SELECT 1
                FROM xxap_supplier_branches_stg stg1
               WHERE stg1.interface_txn_id = stg2.interface_txn_id);
    END IF;
    COMMIT;
    xxetn_debug_pkg.add_debug(' - get_data -');

    /** Added for v1.1 **/
    IF g_entity = g_account THEN
      -- Open Cursor for all account records
      OPEN supp_account_ext_cur;
      LOOP

        l_supp_account_ext_tbl.DELETE;

        FETCH supp_account_ext_cur BULK COLLECT
          INTO l_supp_account_ext_tbl LIMIT 1000; --limit size of Bulk Collect

        -- Get Total Count
        g_total_count := g_total_count + l_supp_account_ext_tbl.COUNT;

        EXIT WHEN l_supp_account_ext_tbl.COUNT = 0;

        BEGIN

          -- Bulk Insert into Conversion table
          FORALL indx IN 1 .. l_supp_account_ext_tbl.COUNT SAVE EXCEPTIONS
            INSERT INTO xxap_supplier_bankaccnts_stg
              (interface_txn_id,
               batch_id,
               run_sequence_id,
               leg_vendor_name,
               leg_vendor_num,
               leg_vendor_site_code,
               leg_bank_name,
               leg_branch_name,
               leg_account_name,
               leg_account_num,
               leg_check_digits,
               leg_iban,
               leg_currency,
               leg_country,
               leg_primary_flag,
               leg_account_name_alt,
               leg_agency_loc_code,
               leg_account_type,
               leg_bank_account_type,
               leg_multi_currency_flag,
               leg_secondary_account_ref,
               leg_description,
               leg_end_Date,
               ext_bank_account_id,
               branch_id,
               bank_id,
               iban,
               bank_account_type,
               object_version_number,
               org_id,
               vendor_id,
               vendor_site_id,
               party_id,
               party_site_id,
               bank_branch_type,
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
               error_type,
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
               leg_operating_unit_name ---added
              ,
               leg_bank_branch_id,
               leg_allow_multi_assign_flag
               --,last_update_date
              ,
               leg_bank_account_uses_id,
               leg_vendor_id,
               leg_vendor_site_id,
               leg_external_bank_account_id,
               leg_global_attribute_category,
               leg_global_attribute1,
               leg_global_attribute2,
               leg_global_attribute3,
               leg_global_attribute4,
               leg_global_attribute5,
               leg_global_attribute6,
               leg_global_attribute7,
               leg_global_attribute8,
               leg_global_attribute9,
               leg_global_attribute10,
               leg_global_attribute11,
               leg_global_attribute12,
               leg_global_attribute13,
               leg_global_attribute14,
               leg_global_attribute15,
               leg_global_attribute16,
               leg_global_attribute17,
               leg_global_attribute18,
               leg_global_attribute19,
               leg_global_attribute20,
               leg_attribute2,
               leg_org_id,
               leg_bank_account_id,
               leg_set_of_books_id)
            VALUES
              (l_supp_account_ext_tbl(indx).interface_txn_id,
               l_supp_account_ext_tbl(indx).batch_id,
               l_supp_account_ext_tbl(indx).run_sequence_id,
               l_supp_account_ext_tbl(indx).leg_vendor_name,
               l_supp_account_ext_tbl(indx).leg_vendor_num,
               l_supp_account_ext_tbl(indx).leg_vendor_site_code,
               l_supp_account_ext_tbl(indx).leg_bank_name,
               l_supp_account_ext_tbl(indx).leg_branch_name,
               l_supp_account_ext_tbl(indx).leg_account_name,
               l_supp_account_ext_tbl(indx).leg_account_num,
               l_supp_account_ext_tbl(indx).leg_check_digits,
               l_supp_account_ext_tbl(indx).leg_iban,
               l_supp_account_ext_tbl(indx).leg_currency,
               l_supp_account_ext_tbl(indx).leg_country,
               l_supp_account_ext_tbl(indx).leg_primary_flag,
               l_supp_account_ext_tbl(indx).leg_account_name_alt,
               l_supp_account_ext_tbl(indx).leg_agency_loc_code,
               l_supp_account_ext_tbl(indx).leg_account_type,
               l_supp_account_ext_tbl(indx).leg_bank_account_type,
               'Y', /*l_supp_account_ext_tbl(indx).leg_multi_currency_flag,  CR# 393268 :- Allow International payment flag set as default for suppliers banks  */
               l_supp_account_ext_tbl(indx).leg_secondary_account_ref,
               l_supp_account_ext_tbl(indx).leg_description,
               l_supp_account_ext_tbl(indx).leg_end_Date,
               l_supp_account_ext_tbl(indx).ext_bank_account_id,
               l_supp_account_ext_tbl(indx).branch_id,
               l_supp_account_ext_tbl(indx).bank_id,
               l_supp_account_ext_tbl(indx).iban,
               l_supp_account_ext_tbl(indx).bank_account_type,
               l_supp_account_ext_tbl(indx).object_version_number,
               l_supp_account_ext_tbl(indx).org_id,
               l_supp_account_ext_tbl(indx).vendor_id,
               l_supp_account_ext_tbl(indx).vendor_site_id,
               l_supp_account_ext_tbl(indx).party_id,
               l_supp_account_ext_tbl(indx).party_site_id,
               l_supp_account_ext_tbl(indx).bank_branch_type,
               SYSDATE,
               g_user_id,
               SYSDATE,
               g_user_id,
               g_login_id,
               g_prog_appl_id,
               g_conc_program_id,
               SYSDATE,
               g_request_id,
               g_new,
               l_supp_account_ext_tbl(indx).error_type,
               l_supp_account_ext_tbl(indx).attribute_category,
               l_supp_account_ext_tbl(indx).attribute1,
               l_supp_account_ext_tbl(indx).attribute2,
               l_supp_account_ext_tbl(indx).attribute3,
               l_supp_account_ext_tbl(indx).attribute4,
               l_supp_account_ext_tbl(indx).attribute5,
               l_supp_account_ext_tbl(indx).attribute6,
               l_supp_account_ext_tbl(indx).attribute7,
               l_supp_account_ext_tbl(indx).attribute8,
               l_supp_account_ext_tbl(indx).attribute9,
               l_supp_account_ext_tbl(indx).attribute10,
               l_supp_account_ext_tbl(indx).attribute11,
               l_supp_account_ext_tbl(indx).attribute12,
               l_supp_account_ext_tbl(indx).attribute13,
               l_supp_account_ext_tbl(indx).attribute14,
               l_supp_account_ext_tbl(indx).attribute15,
               l_supp_account_ext_tbl(indx).leg_source_system,
               l_supp_account_ext_tbl(indx).leg_request_id,
               l_supp_account_ext_tbl(indx).leg_seq_num,
               l_supp_account_ext_tbl(indx).leg_process_flag,
               l_supp_account_ext_tbl(indx).leg_operating_unit_name --added
              ,
               l_supp_account_ext_tbl(indx).leg_bank_branch_id,
               l_supp_account_ext_tbl(indx).leg_allow_multi_assign_flag
               --,l_supp_account_ext_tbl (indx).last_update_date
              ,
               l_supp_account_ext_tbl(indx).leg_bank_account_uses_id,
               l_supp_account_ext_tbl(indx).leg_vendor_id,
               l_supp_account_ext_tbl(indx).leg_vendor_site_id,
               l_supp_account_ext_tbl(indx).leg_external_bank_account_id,
               l_supp_account_ext_tbl(indx).leg_global_attribute_category,
               l_supp_account_ext_tbl(indx).leg_global_attribute1,
               l_supp_account_ext_tbl(indx).leg_global_attribute2,
               l_supp_account_ext_tbl(indx).leg_global_attribute3,
               l_supp_account_ext_tbl(indx).leg_global_attribute4,
               l_supp_account_ext_tbl(indx).leg_global_attribute5,
               l_supp_account_ext_tbl(indx).leg_global_attribute6,
               l_supp_account_ext_tbl(indx).leg_global_attribute7,
               l_supp_account_ext_tbl(indx).leg_global_attribute8,
               l_supp_account_ext_tbl(indx).leg_global_attribute9,
               l_supp_account_ext_tbl(indx).leg_global_attribute10,
               l_supp_account_ext_tbl(indx).leg_global_attribute11,
               l_supp_account_ext_tbl(indx).leg_global_attribute12,
               l_supp_account_ext_tbl(indx).leg_global_attribute13,
               l_supp_account_ext_tbl(indx).leg_global_attribute14,
               l_supp_account_ext_tbl(indx).leg_global_attribute15,
               l_supp_account_ext_tbl(indx).leg_global_attribute16,
               l_supp_account_ext_tbl(indx).leg_global_attribute17,
               l_supp_account_ext_tbl(indx).leg_global_attribute18,
               l_supp_account_ext_tbl(indx).leg_global_attribute19,
               l_supp_account_ext_tbl(indx).leg_global_attribute20,
               l_supp_account_ext_tbl(indx).leg_attribute2,
               l_supp_account_ext_tbl(indx).leg_org_id,
               l_supp_account_ext_tbl(indx).leg_bank_account_id,
               l_supp_account_ext_tbl(indx).leg_set_of_books_id);

        EXCEPTION
          WHEN OTHERS THEN
            FOR l_indx_exp IN 1 .. SQL%BULK_EXCEPTIONS.COUNT LOOP
              l_err_record := l_supp_account_ext_tbl(SQL%BULK_EXCEPTIONS(l_indx_exp)
                                                     .ERROR_INDEX)
                             .interface_txn_id;
              g_retcode    := '1';
              fnd_file.PUT_LINE(fnd_file.LOG,
                                'Record sequence : ' ||
                                 l_supp_account_ext_tbl(SQL%BULK_EXCEPTIONS(l_indx_exp)
                                                        .error_index)
                                .interface_txn_id);
              fnd_file.PUT_LINE(fnd_file.LOG,
                                'Error Message : ' ||
                                SQLERRM(-SQL%BULK_EXCEPTIONS(l_indx_exp)
                                        .error_code));

              -- Updating Leg_process_flag to 'E' for failed records
              UPDATE xxap_supplier_bankaccnts_R12 xsber
                 SET leg_process_flag  = g_error,
                     last_updated_by   = g_user_id,
                     last_update_login = g_login_id,
                     last_update_date  = SYSDATE
               WHERE xsber.interface_txn_id = l_err_record
                 AND xsber.leg_process_flag = g_validated;

              g_failed_count := g_failed_count + SQL%ROWCOUNT;

              l_error_message := l_error_message || ' ~~ ' ||
                                 SQLERRM(-SQL%BULK_EXCEPTIONS(l_indx_exp)
                                         .ERROR_CODE);
            END LOOP;
        END;

      END LOOP;
      CLOSE supp_account_ext_cur; -- Close Cursor

      COMMIT;

      -- Update Successful records in Extraction Table
      UPDATE xxap_supplier_bankaccnts_R12 stg2
         SET leg_process_flag  = g_processed,
             last_updated_by   = g_user_id,
             last_update_login = g_login_id,
             last_update_date  = SYSDATE
       WHERE leg_process_flag = g_validated
         AND EXISTS
       (SELECT 1
                FROM xxap_supplier_bankaccnts_stg stg1
               WHERE stg1.interface_txn_id = stg2.interface_txn_id);
    END IF;
    COMMIT;

    xxetn_debug_pkg.add_debug(' - get_data -');

    --Manoj:START
    IF g_entity = g_supplier THEN
      -- Open Cursor for all branch records
      OPEN supp_ext_cur;
      LOOP

        l_supp_ext_tbl.DELETE;

        FETCH supp_ext_cur BULK COLLECT
          INTO l_supp_ext_tbl LIMIT 1000;

        -- Get Total Count
        g_total_count := g_total_count + l_supp_ext_tbl.COUNT;

        BEGIN
          FORALL indx IN 1 .. l_supp_ext_tbl.COUNT SAVE EXCEPTIONS
            INSERT INTO xxap_suppliers_stg
              (interface_txn_id,
               batch_id,
               run_sequence_id,
               leg_vendor_name,
               leg_vendor_name_alt,
               leg_segment1,
               leg_segment2,
               leg_segment3,
               leg_segment4,
               leg_segment5,
               leg_summary_flag,
               leg_enabled_flag,
               leg_employee_name,
               leg_employee_number,
               leg_vendor_type_lookup_code,
               leg_customer_num,
               leg_parent_vendor_name,
               leg_one_time_flag,
               leg_min_order_amount,
               leg_ship_to_location_code,
               leg_bill_to_location_code,
               leg_ship_via_lookup_code,
               leg_freight_terms_code,
               leg_fob_lookup_code,
               leg_terms_name,
               leg_set_of_books,
               leg_always_take_disc_flag,
               leg_pay_date_basis_code,
               leg_pay_group_lookup_code,
               leg_payment_priority,
               leg_invoice_currency_code,
               leg_payment_currency_code,
               Leg_hold_all_payments_flag,
               leg_hold_future_pay_flag,
               leg_hold_reason,
               leg_distribution_set_name,
               leg_accts_pay_code_segment1,
               leg_accts_pay_code_segment2,
               leg_accts_pay_code_segment3,
               leg_accts_pay_code_segment4,
               leg_accts_pay_code_segment5,
               leg_accts_pay_code_segment6,
               leg_accts_pay_code_segment7,
               leg_prepay_code_segment1,
               leg_prepay_code_segment2,
               leg_prepay_code_segment3,
               leg_prepay_code_segment4,
               leg_prepay_code_segment5,
               leg_prepay_code_segment6,
               leg_prepay_code_segment7,
               leg_future_dated_segment1,
               leg_future_dated_segment2,
               leg_future_dated_segment3,
               leg_future_dated_segment4,
               leg_future_dated_segment5,
               leg_future_dated_segment6,
               leg_future_dated_segment7,
               leg_num_1099,
               leg_type_1099,
               leg_organization_type_code,
               leg_vat_code,
               leg_start_date_active,
               leg_end_date_active,
               leg_minority_group_code,
               leg_payment_method_code,
               leg_women_owned_flag,
               leg_small_business_flag,
               leg_standard_industry_class,
               leg_hold_flag,
               leg_purchasing_hold_reason,
               leg_hold_by,
               leg_hold_date,
               leg_terms_date_basis,
               leg_inspection_required_flag,
               leg_receipt_required_flag,
               leg_qty_rcv_tolerance,
               leg_qty_rcv_exception_code,
               leg_enforce_ship_to_code,
               leg_days_early_recpt_allowed,
               leg_days_late_recpt_allowed,
               leg_receipt_days_except_code,
               leg_receiving_routing_id,
               leg_allow_subs_recpt_flag,
               leg_unordered_receipts_flag,
               leg_hold_unmatched_inv_flag,
               leg_exclusive_payment_flag,
               leg_ap_tax_rounding_rule,
               leg_auto_tax_calc_flag,
               leg_auto_tax_calc_override,
               leg_amount_includes_tax_flag,
               leg_tax_verification_date,
               leg_name_control,
               leg_state_reportable_flag,
               leg_federal_reportable_flag,
               leg_attribute_category,
               leg_attribute1,
               leg_attribute2,
               leg_attribute3,
               leg_attribute4,
               leg_attribute5,
               leg_attribute6,
               leg_attribute7,
               leg_attribute8,
               leg_attribute9,
               leg_attribute10,
               leg_attribute11,
               leg_attribute12,
               leg_attribute13,
               leg_attribute14,
               leg_attribute15,
               leg_vat_registration_num,
               leg_auto_calculate_int_flag,
               leg_exclude_freight_from_disc,
               leg_tax_reporting_name,
               leg_allow_awt_flag,
               leg_awt_group_name,
               leg_global_attribute1,
               leg_global_attribute2,
               leg_global_attribute3,
               leg_global_attribute4,
               leg_global_attribute5,
               leg_global_attribute6,
               leg_global_attribute7,
               leg_global_attribute8,
               leg_global_attribute9,
               leg_global_attribute10,
               leg_global_attribute11,
               leg_global_attribute12,
               leg_global_attribute13,
               leg_global_attribute14,
               leg_global_attribute15,
               leg_global_attribute16,
               leg_global_attribute17,
               leg_global_attribute18,
               leg_global_attribute19,
               leg_global_attribute20,
               leg_global_attribute_category,
               leg_edi_payment_method,
               leg_bank_charge_bearer,
               leg_match_option,
               leg_create_debit_memo_flag,
               leg_offset_tax_flag,
               leg_individual_1099,
               vendor_interface_id,
               vendor_id,
               employee_id,
               vendor_type_lookup_code,
               ship_to_location_id,
               ship_to_location_code,
               bill_to_location_id,
               bill_to_location_code,
               ship_via_lookup_code,
               freight_terms_lookup_code,
               fob_lookup_code,
               terms_id,
               terms_name,
               set_of_books_id,
               set_of_books,
               pay_date_basis_lookup_code,
               pay_group_lookup_code,
               payment_priority,
               distribution_set_id,
               distribution_set_name,
               accts_pay_code_combination_id,
               prepay_code_combination_id,
               organization_type_lookup_code,
               minority_group_lookup_code,
               payment_method_lookup_code,
               qty_rcv_tolerance,
               qty_rcv_exception_code,
               enforce_ship_to_location_code,
               allow_awt_flag,
               awt_group_id,
               awt_group_name,
               bank_charge_bearer,
               match_option,
               future_dated_payment_ccid,
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
               error_type,
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
               leg_vendor_id,
               leg_employee_id --added the id
              ,
               leg_parent_vendor_id,
               leg_ship_to_location_id,
               leg_bill_to_location_id,
               leg_terms_id,
               leg_set_of_books_id,
               leg_distribution_set_id,
               leg_accts_pay_code_comb_id,
               leg_prepay_code_comb_id,
               leg_allow_unorder_recpt_flag,
               leg_awt_group_id,
               leg_future_dated_paym_ccid)
            VALUES
              (l_supp_ext_tbl(indx).interface_txn_id,
               NULL,
               NULL,
               l_supp_ext_tbl(indx).leg_vendor_name,
               l_supp_ext_tbl(indx).leg_vendor_name_alt,
               l_supp_ext_tbl(indx).leg_segment1,
               l_supp_ext_tbl(indx).leg_segment2,
               l_supp_ext_tbl(indx).leg_segment3,
               l_supp_ext_tbl(indx).leg_segment4,
               l_supp_ext_tbl(indx).leg_segment5,
               l_supp_ext_tbl(indx).leg_summary_flag,
               l_supp_ext_tbl(indx).leg_enabled_flag,
               l_supp_ext_tbl(indx).leg_employee_name,
               l_supp_ext_tbl(indx).leg_employee_number,
               NVL(l_supp_ext_tbl(indx).leg_vendor_type_lookup_code,'VENDOR'), -- CV 40 page 17 is NULL then VENDOR
               l_supp_ext_tbl(indx).leg_customer_num,
               l_supp_ext_tbl(indx).leg_parent_vendor_name,
               l_supp_ext_tbl(indx).leg_one_time_flag,
               l_supp_ext_tbl(indx).leg_min_order_amount,
               l_supp_ext_tbl(indx).leg_ship_to_location_code,
               l_supp_ext_tbl(indx).leg_bill_to_location_code,
               l_supp_ext_tbl(indx).leg_ship_via_lookup_code,
               l_supp_ext_tbl(indx).leg_freight_terms_code,
               l_supp_ext_tbl(indx).leg_fob_lookup_code,
               l_supp_ext_tbl(indx).leg_terms_name,
               l_supp_ext_tbl(indx).leg_set_of_books,
               l_supp_ext_tbl(indx).leg_always_take_disc_flag,
               l_supp_ext_tbl(indx).leg_pay_date_basis_code,
               l_supp_ext_tbl(indx).leg_pay_group_lookup_code,
               l_supp_ext_tbl(indx).leg_payment_priority,
               l_supp_ext_tbl(indx).leg_invoice_currency_code,
               l_supp_ext_tbl(indx).leg_payment_currency_code,
               l_supp_ext_tbl(indx).leg_hold_all_payments_flag,
               l_supp_ext_tbl(indx).leg_hold_future_pay_flag,
               l_supp_ext_tbl(indx).leg_hold_reason,
               l_supp_ext_tbl(indx).leg_distribution_set_name,
               NULL, --l_supp_ext_tbl(indx).leg_accts_pay_code_segment1,   -- 03/28 - CR#372676
               NULL, --l_supp_ext_tbl(indx).leg_accts_pay_code_segment2,   -- 03/28 - CR#372676
               NULL, --l_supp_ext_tbl(indx).leg_accts_pay_code_segment3,   -- 03/28 - CR#372676
               NULL, --l_supp_ext_tbl(indx).leg_accts_pay_code_segment4,   -- 03/28 - CR#372676
               NULL, --l_supp_ext_tbl(indx).leg_accts_pay_code_segment5,   -- 03/28 - CR#372676
               NULL, --l_supp_ext_tbl(indx).leg_accts_pay_code_segment6,   -- 03/28 - CR#372676
               NULL, --l_supp_ext_tbl(indx).leg_accts_pay_code_segment7,   -- 03/28 - CR#372676
               NULL, --l_supp_ext_tbl(indx).leg_prepay_code_segment1,
               NULL, --l_supp_ext_tbl(indx).leg_prepay_code_segment2,
               NULL, --l_supp_ext_tbl(indx).leg_prepay_code_segment3,
               NULL, --l_supp_ext_tbl(indx).leg_prepay_code_segment4,
               NULL, --l_supp_ext_tbl(indx).leg_prepay_code_segment5,
               NULL, --l_supp_ext_tbl(indx).leg_prepay_code_segment6,
               NULL, --l_supp_ext_tbl(indx).leg_prepay_code_segment7,
               NULL, --l_supp_ext_tbl(indx).leg_future_dated_segment1,
               NULL, --l_supp_ext_tbl(indx).leg_future_dated_segment2,
               NULL, --l_supp_ext_tbl(indx).leg_future_dated_segment3,
               NULL, --l_supp_ext_tbl(indx).leg_future_dated_segment4,
               NULL, --l_supp_ext_tbl(indx).leg_future_dated_segment5,
               NULL, --l_supp_ext_tbl(indx).leg_future_dated_segment6,
               NULL, --l_supp_ext_tbl(indx).leg_future_dated_segment7,
               substr(l_supp_ext_tbl(indx).leg_num_1099,1,19),
               l_supp_ext_tbl(indx).leg_type_1099,
               l_supp_ext_tbl(indx).leg_organization_type_code,
               l_supp_ext_tbl(indx).leg_vat_code,
               l_supp_ext_tbl(indx).leg_start_date_active,
               l_supp_ext_tbl(indx).leg_end_date_active,
               l_supp_ext_tbl(indx).leg_minority_group_code,
               l_supp_ext_tbl(indx).leg_payment_method_code,
               l_supp_ext_tbl(indx).leg_women_owned_flag,
               l_supp_ext_tbl(indx).leg_small_business_flag,
               l_supp_ext_tbl(indx).leg_standard_industry_class,
               l_supp_ext_tbl(indx).leg_hold_flag,
               l_supp_ext_tbl(indx).leg_purchasing_hold_reason,
               l_supp_ext_tbl(indx).leg_hold_by,
               l_supp_ext_tbl(indx).leg_hold_date,
               l_supp_ext_tbl(indx).leg_terms_date_basis,
               l_supp_ext_tbl(indx).leg_inspection_required_flag,
               l_supp_ext_tbl(indx).leg_receipt_required_flag,
               l_supp_ext_tbl(indx).leg_qty_rcv_tolerance,
               l_supp_ext_tbl(indx).leg_qty_rcv_exception_code,
               l_supp_ext_tbl(indx).leg_enforce_ship_to_code,
               l_supp_ext_tbl(indx).leg_days_early_recpt_allowed,
               l_supp_ext_tbl(indx).leg_days_late_recpt_allowed,
               l_supp_ext_tbl(indx).leg_receipt_days_except_code,
               l_supp_ext_tbl(indx).leg_receiving_routing_id,
               l_supp_ext_tbl(indx).leg_allow_subs_recpt_flag,
               l_supp_ext_tbl(indx).leg_allow_unorder_recpt_flag,
               l_supp_ext_tbl(indx).leg_hold_unmatched_inv_flag,
               l_supp_ext_tbl(indx).leg_exclusive_payment_flag,
               l_supp_ext_tbl(indx).leg_ap_tax_rounding_rule,
               /*l_supp_ext_tbl(indx).leg_auto_tax_calc_flag,*/
               'Y' , /*PMC : 339760 : Update  - Added by DD on 3/18/2016
                             1.  Supplier header : "Allow Tax Applicability " flag :  need to be
                             enabled (header) (FYI:  Field Name - AP_SUPPLIERS_INT.AUTO_TAX_CALC_FLAG) */
               l_supp_ext_tbl(indx).leg_auto_tax_calc_override,
               l_supp_ext_tbl(indx).leg_amount_includes_tax_flag,
               l_supp_ext_tbl(indx).leg_tax_verification_date,
               l_supp_ext_tbl(indx).leg_name_control,
               l_supp_ext_tbl(indx).leg_state_reportable_flag,
               l_supp_ext_tbl(indx).leg_federal_reportable_flag,
               l_supp_ext_tbl(indx).leg_attribute_category,
               l_supp_ext_tbl(indx).LEG_VENDOR_ID, /*l_supp_ext_tbl(indx).leg_attribute1, To Capture leg vendor id in R12*/--SDP CHANGE SEGMENT1 TO VENDOR_ID
               l_supp_ext_tbl(indx).leg_attribute2,
               l_supp_ext_tbl(indx).leg_attribute3,
               l_supp_ext_tbl(indx).leg_attribute4,
               l_supp_ext_tbl(indx).leg_attribute5,
               l_supp_ext_tbl(indx).leg_attribute6,
               l_supp_ext_tbl(indx).leg_attribute7,
               l_supp_ext_tbl(indx).leg_attribute8,
               l_supp_ext_tbl(indx).leg_attribute9,
               l_supp_ext_tbl(indx).leg_attribute10,
               l_supp_ext_tbl(indx).leg_attribute11,
               l_supp_ext_tbl(indx).leg_attribute12,
               l_supp_ext_tbl(indx).leg_attribute13,
               l_supp_ext_tbl(indx).leg_attribute14,
               l_supp_ext_tbl(indx).leg_attribute15,
               l_supp_ext_tbl(indx).leg_vat_registration_num,
               l_supp_ext_tbl(indx).leg_auto_calculate_int_flag,
               l_supp_ext_tbl(indx).leg_exclude_freight_from_disc,
               l_supp_ext_tbl(indx).leg_tax_reporting_name,
               l_supp_ext_tbl(indx).leg_allow_awt_flag,
               l_supp_ext_tbl(indx).leg_awt_group_name,
               l_supp_ext_tbl(indx).leg_global_attribute1,
               l_supp_ext_tbl(indx).leg_global_attribute2,
               l_supp_ext_tbl(indx).leg_global_attribute3,
               l_supp_ext_tbl(indx).leg_global_attribute4,
               l_supp_ext_tbl(indx).leg_global_attribute5,
               l_supp_ext_tbl(indx).leg_global_attribute6,
               l_supp_ext_tbl(indx).leg_global_attribute7,
               l_supp_ext_tbl(indx).leg_global_attribute8,
               l_supp_ext_tbl(indx).leg_global_attribute9,
               l_supp_ext_tbl(indx).leg_global_attribute10,
               l_supp_ext_tbl(indx).leg_global_attribute11,
               l_supp_ext_tbl(indx).leg_global_attribute12,
               l_supp_ext_tbl(indx).leg_global_attribute13,
               l_supp_ext_tbl(indx).leg_global_attribute14,
               l_supp_ext_tbl(indx).leg_global_attribute15,
               l_supp_ext_tbl(indx).leg_global_attribute16,
               l_supp_ext_tbl(indx).leg_global_attribute17,
               l_supp_ext_tbl(indx).leg_global_attribute18,
               l_supp_ext_tbl(indx).leg_global_attribute19,
               l_supp_ext_tbl(indx).leg_global_attribute20,
               l_supp_ext_tbl(indx).leg_global_attribute_category,
               l_supp_ext_tbl(indx).leg_edi_payment_method,
               l_supp_ext_tbl(indx).leg_bank_charge_bearer,
               l_supp_ext_tbl(indx).leg_match_option,
               l_supp_ext_tbl(indx).leg_create_debit_memo_flag,
               /*l_supp_ext_tbl(indx).leg_offset_tax_flag,*/
               'Y',/* PMC : 339760 : Update  - Added by DD on 3/18/2016
                       2.  Allow "Offset flag" needs to be enabled  - All suppliers
                       (FYI:  Field Name -AP_SUPPLIERS_INT.OFFSET_TAX_FLAG  .OFFSET_TAX_FLAG)*/
               l_supp_ext_tbl(indx).leg_individual_1099,
               l_supp_ext_tbl(indx).vendor_interface_id,
               l_supp_ext_tbl(indx).vendor_id,
               l_supp_ext_tbl(indx).employee_id,
               l_supp_ext_tbl(indx).vendor_type_lookup_code,
               l_supp_ext_tbl(indx).ship_to_location_id,
               l_supp_ext_tbl(indx).ship_to_location_code,
               l_supp_ext_tbl(indx).bill_to_location_id,
               l_supp_ext_tbl(indx).bill_to_location_code,
               l_supp_ext_tbl(indx).ship_via_lookup_code,
               l_supp_ext_tbl(indx).freight_terms_lookup_code,
               l_supp_ext_tbl(indx).fob_lookup_code,
               l_supp_ext_tbl(indx).terms_id,
               l_supp_ext_tbl(indx).terms_name,
               l_supp_ext_tbl(indx).set_of_books_id,
               l_supp_ext_tbl(indx).set_of_books,
               l_supp_ext_tbl(indx).pay_date_basis_lookup_code,
               l_supp_ext_tbl(indx).pay_group_lookup_code,
               l_supp_ext_tbl(indx).payment_priority,
               l_supp_ext_tbl(indx).distribution_set_id,
               l_supp_ext_tbl(indx).distribution_set_name,
               l_supp_ext_tbl(indx).accts_pay_code_combination_id,
               l_supp_ext_tbl(indx).prepay_code_combination_id,
               l_supp_ext_tbl(indx).organization_type_lookup_code,
               l_supp_ext_tbl(indx).minority_group_lookup_code,
               l_supp_ext_tbl(indx).payment_method_lookup_code,
               l_supp_ext_tbl(indx).qty_rcv_tolerance,
               l_supp_ext_tbl(indx).qty_rcv_exception_code,
               l_supp_ext_tbl(indx).enforce_ship_to_location_code,
               l_supp_ext_tbl(indx).allow_awt_flag,
               l_supp_ext_tbl(indx).awt_group_id,
               l_supp_ext_tbl(indx).awt_group_name,
               l_supp_ext_tbl(indx).bank_charge_bearer,
               l_supp_ext_tbl(indx).match_option,
               l_supp_ext_tbl(indx).future_dated_payment_ccid,
               SYSDATE,
               g_user_id,
               SYSDATE,
               g_user_id,
               g_login_id,
               g_prog_appl_id,
               g_conc_program_id,
               SYSDATE,
               g_request_id,
               g_new,
               NULL,
               l_supp_ext_tbl(indx).attribute_category,
               l_supp_ext_tbl(indx).LEG_VENDOR_ID, /*l_supp_ext_tbl(indx).attribute1, To Capture leg vendor id in R12*/--SDP CHANGE SEGMENT1 TO VENDOR_ID
               l_supp_ext_tbl(indx).attribute2,
               l_supp_ext_tbl(indx).attribute3,
               l_supp_ext_tbl(indx).attribute4,
               l_supp_ext_tbl(indx).attribute5,
               l_supp_ext_tbl(indx).attribute6,
               l_supp_ext_tbl(indx).attribute7,
               l_supp_ext_tbl(indx).attribute8,
               l_supp_ext_tbl(indx).attribute9,
               l_supp_ext_tbl(indx).attribute10,
               l_supp_ext_tbl(indx).attribute11,
               l_supp_ext_tbl(indx).attribute12,
               l_supp_ext_tbl(indx).attribute13,
               l_supp_ext_tbl(indx).attribute14,
               l_supp_ext_tbl(indx).attribute15,
               l_supp_ext_tbl(indx).leg_source_system,
               l_supp_ext_tbl(indx).leg_request_id,
               l_supp_ext_tbl(indx).leg_seq_num,
               l_supp_ext_tbl(indx).leg_process_flag,
               l_supp_ext_tbl(indx).leg_vendor_id,
               l_supp_ext_tbl(indx).leg_employee_id,
               l_supp_ext_tbl(indx).leg_parent_vendor_id,
               l_supp_ext_tbl(indx).leg_ship_to_location_id,
               l_supp_ext_tbl(indx).leg_bill_to_location_id,
               l_supp_ext_tbl(indx).leg_terms_id,
               l_supp_ext_tbl(indx).leg_set_of_books_id,
               l_supp_ext_tbl(indx).leg_distribution_set_id,
               l_supp_ext_tbl(indx).leg_accts_pay_code_comb_id,
               l_supp_ext_tbl(indx).leg_prepay_code_comb_id,
               l_supp_ext_tbl(indx).leg_allow_unorder_recpt_flag,
               l_supp_ext_tbl(indx).leg_awt_group_id,
               l_supp_ext_tbl(indx).leg_future_dated_paym_ccid);
          COMMIT;
        EXCEPTION
          WHEN g_bulk_exception THEN
            g_failed_count := g_failed_count + SQL%BULK_EXCEPTIONS.COUNT;
            FOR l_indx_exp IN 1 .. SQL%BULK_EXCEPTIONS.COUNT LOOP

              l_err_record := l_supp_ext_tbl(SQL%BULK_EXCEPTIONS(l_indx_exp)
                                             .ERROR_INDEX).interface_txn_id;
              g_retcode    := '1';
              fnd_file.put_line(fnd_file.LOG,
                                'Record sequence : ' ||
                                 l_supp_ext_tbl(SQL%BULK_EXCEPTIONS(l_indx_exp)
                                                .error_index)
                                .interface_txn_id);
              fnd_file.put_line(fnd_file.LOG,
                                'Error Message : ' ||
                                SQLERRM(-SQL%BULK_EXCEPTIONS(l_indx_exp)
                                        .ERROR_CODE));

              -- Updating Leg_process_flag to 'E' for failed records
              UPDATE xxap_supplier_stg_R12
                 SET leg_process_flag  = g_error,
                     last_updated_by   = g_user_id,
                     last_update_login = g_login_id,
                     last_updated_date = SYSDATE
               WHERE interface_txn_id = l_err_record
                 AND leg_process_flag = g_validated;

            END LOOP;
        END;
        EXIT WHEN supp_ext_cur%NOTFOUND;
      END LOOP;
      CLOSE supp_ext_cur;

      -- Update Successful records in Extraction Table
      UPDATE xxap_supplier_stg_R12 xsser
         SET xsser.leg_process_flag = g_processed,
             last_updated_by        = g_user_id,
             last_update_login      = g_login_id,
             last_updated_date      = SYSDATE
       WHERE xsser.leg_process_flag = g_validated
         AND EXISTS
       (SELECT 1
                FROM xxap_suppliers_stg xss
               WHERE xss.interface_txn_id = xsser.interface_txn_id);
    END IF;

    IF g_entity = g_supplier_sites THEN
      -- Open Cursor for all branch records

      print_log_message ('Starting Supplier Site Staging Table');

      OPEN suppsite_ext_cur;
      LOOP

        l_suppsite_ext_tbl.DELETE;

        FETCH suppsite_ext_cur BULK COLLECT
          INTO l_suppsite_ext_tbl LIMIT 10000;

        -- Get Total Count
        g_total_count := g_total_count + l_suppsite_ext_tbl.COUNT;
        print_log_message ('Supplier Site bulk count ' ||g_total_count);

        BEGIN
          FORALL indx IN 1 .. l_suppsite_ext_tbl.COUNT SAVE EXCEPTIONS
            INSERT INTO xxap_supplier_sites_stg
              (interface_txn_id,
               batch_id,
               run_sequence_id,
               leg_vendor_site_code,
               leg_vendor_name,
               leg_vendor_site_code_alt,
               leg_purchasing_site_flag,
               leg_rfq_only_site_flag,
               leg_pay_site_flag,
               leg_attention_ar_flag,
               leg_address_line1,
               leg_address_line2,
               leg_address_line3,
               leg_city,
               leg_state,
               leg_zip,
               leg_province,
               leg_country,
               leg_area_code,
               leg_phone,
               leg_customer_num,
               leg_ship_to_location_code,
               leg_bill_to_location_code,
               leg_ship_via_lookup_code,
               leg_freight_terms_code,
               leg_fob_lookup_code,
               leg_inactive_date,
               leg_fax,
               leg_fax_area_code,
               leg_telex,
               leg_payment_method_code,
               leg_terms_date_basis,
               leg_distribution_set_name,
               leg_accts_pay_code_segment1,
               leg_accts_pay_code_segment2,
               leg_accts_pay_code_segment3,
               leg_accts_pay_code_segment4,
               leg_accts_pay_code_segment5,
               leg_accts_pay_code_segment6,
               leg_accts_pay_code_segment7,
               leg_prepay_code_segment1,
               leg_prepay_code_segment2,
               leg_prepay_code_segment3,
               leg_prepay_code_segment4,
               leg_prepay_code_segment5,
               leg_prepay_code_segment6,
               leg_prepay_code_segment7,
               leg_future_dated_segment1,
               leg_future_dated_segment2,
               leg_future_dated_segment3,
               leg_future_dated_segment4,
               leg_future_dated_segment5,
               leg_future_dated_segment6,
               leg_future_dated_segment7,
               leg_pay_group_lookup_code,
               leg_payment_priority,
               leg_terms_name,
               leg_invoice_amount_limit,
               leg_pay_date_basis_code,
               leg_always_take_disc_flag,
               leg_invoice_currency_code,
               leg_payment_currency_code,
               leg_hold_all_payments_flag,
               leg_hold_future_pay_flag,
               leg_hold_reason,
               leg_hold_unmatched_inv_flag,
               leg_ap_tax_rounding_rule,
               leg_auto_tax_calc_flag,
               leg_auto_tax_calc_override,
               leg_amount_incl_tax_flag,
               leg_exclusive_payment_flag,
               leg_tax_reporting_site_flag,
               leg_attribute_category,
               leg_attribute1,
               leg_attribute2,
               leg_attribute3,
               leg_attribute4,
               leg_attribute5,
               leg_attribute6,
               leg_attribute7,
               leg_attribute8,
               leg_attribute9,
               leg_attribute10,
               leg_attribute11,
               leg_attribute12,
               leg_attribute13,
               leg_attribute14,
               leg_attribute15,
               leg_validation_number,
               leg_excl_freight_from_dist,
               leg_vat_registration_num,
               leg_operating_unit_name,
               leg_check_digits,
               leg_address_line4,
               leg_county,
               leg_address_style,
               leg_language,
               leg_allow_awt_flag,
               leg_awt_group_name,
               leg_global_attribute1,
               leg_global_attribute2,
               leg_global_attribute3,
               leg_global_attribute4,
               leg_global_attribute5,
               leg_global_attribute6,
               leg_global_attribute7,
               leg_global_attribute8,
               leg_global_attribute9,
               leg_global_attribute10,
               leg_global_attribute11,
               leg_global_attribute12,
               leg_global_attribute13,
               leg_global_attribute14,
               leg_global_attribute15,
               leg_global_attribute16,
               leg_global_attribute17,
               leg_global_attribute18,
               leg_global_attribute19,
               leg_global_attribute20,
               leg_global_attribute_category,
               leg_edi_id_number,
               leg_bank_charge_bearer,
               leg_pay_on_code,
               leg_default_pay_site,
               leg_pay_on_receipt_sum_code,
               leg_tp_header_id,
               leg_ece_tp_location_code,
               leg_pcard_site_flag,
               leg_match_option,
               leg_country_of_origin_code,
               leg_create_debit_memo_flag,
               leg_offset_tax_flag,
               leg_supplier_notif_method,
               leg_email_address,
               leg_remittance_email,
               leg_primary_pay_site_flag,
               leg_shipping_control,
               leg_tolerance_name,
               leg_service_tolerance_name,
               vendor_interface_id,
               vendor_id,
               vendor_site_id,
               vendor_site_code,
               ship_to_location_id,
               ship_to_location_code,
               bill_to_location_id,
               bill_to_location_code,
               ship_via_lookup_code,
               freight_terms_lookup_code,
               fob_lookup_code,
               payment_method_lookup_code,
               terms_date_basis,
               distribution_set_id,
               distribution_set_name,
               accts_pay_code_combination_id,
               prepay_code_combination_id,
               future_dated_payment_ccid,
               pay_group_lookup_code,
               payment_priority,
               terms_id,
               terms_name,
               pay_date_basis_lookup_code,
               org_id,
               operating_unit_name,
               awt_group_id,
               awt_group_name,
               pay_on_code,
               default_pay_site_id,
               pay_on_receipt_summary_code,
               tp_header_id,
               tolerance_id,
               vendor_site_interface_id,
               payment_method_code,
               vendor_name,
               vendor_number,
               creation_date,
               created_by,
               last_updated_by,
               last_update_login,
               program_application_id,
               program_id,
               program_update_date,
               request_id,
               process_flag,
               error_type,
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
               leg_vendor_number,
               leg_vendor_id,
               leg_vendor_site_id,
               leg_ship_to_location_id,
               leg_bill_to_location_id,
               leg_distribution_set_id,
               leg_accts_pay_code_comb_id,
               leg_prepay_code_comb_id,
               leg_terms_id,
               leg_org_id,
               leg_default_pay_site_id,
               leg_future_dated_paym_ccid,
               leg_selling_co_identifier,
               leg_tolerance_id,
               leg_services_tolerance_id,
               leg_old_vendor_site_code_alt,
               leg_old_vendor_id,
               last_update_date,
               Leg_vat_code)
            VALUES
              (l_suppsite_ext_tbl(indx).interface_txn_id,
               NULL,
               NULL,
               l_suppsite_ext_tbl(indx).leg_vendor_site_code,
               l_suppsite_ext_tbl(indx).leg_vendor_name,
               l_suppsite_ext_tbl(indx).leg_vendor_site_code_alt,
               l_suppsite_ext_tbl(indx).leg_purchasing_site_flag,
               l_suppsite_ext_tbl(indx).leg_rfq_only_site_flag,
               l_suppsite_ext_tbl(indx).leg_pay_site_flag,
               l_suppsite_ext_tbl(indx).leg_attention_ar_flag,
               nvl(l_suppsite_ext_tbl(indx).leg_address_line1,'.'),
               l_suppsite_ext_tbl(indx).leg_address_line2,
               l_suppsite_ext_tbl(indx).leg_address_line3,
               l_suppsite_ext_tbl(indx).leg_city,
               l_suppsite_ext_tbl(indx).leg_state,
               l_suppsite_ext_tbl(indx).leg_zip,
               l_suppsite_ext_tbl(indx).leg_province,
               l_suppsite_ext_tbl(indx).leg_country,
               l_suppsite_ext_tbl(indx).leg_area_code,
               l_suppsite_ext_tbl(indx).leg_phone,
               l_suppsite_ext_tbl(indx).leg_customer_num,
               l_suppsite_ext_tbl(indx).leg_ship_to_location_code,
               l_suppsite_ext_tbl(indx).leg_bill_to_location_code,
               l_suppsite_ext_tbl(indx).leg_ship_via_lookup_code,
               l_suppsite_ext_tbl(indx).leg_freight_terms_code,
               l_suppsite_ext_tbl(indx).leg_fob_lookup_code,
               l_suppsite_ext_tbl(indx).leg_inactive_date,
               l_suppsite_ext_tbl(indx).leg_fax,
               l_suppsite_ext_tbl(indx).leg_fax_area_code,
               l_suppsite_ext_tbl(indx).leg_telex,
               l_suppsite_ext_tbl(indx).leg_payment_method_code,
               l_suppsite_ext_tbl(indx).leg_terms_date_basis,
               l_suppsite_ext_tbl(indx).leg_distribution_set_name,
               l_suppsite_ext_tbl(indx).leg_accts_pay_code_segment1,
               l_suppsite_ext_tbl(indx).leg_accts_pay_code_segment2,
               l_suppsite_ext_tbl(indx).leg_accts_pay_code_segment3,
               l_suppsite_ext_tbl(indx).leg_accts_pay_code_segment4,
               l_suppsite_ext_tbl(indx).leg_accts_pay_code_segment5,
               l_suppsite_ext_tbl(indx).leg_accts_pay_code_segment6,
               l_suppsite_ext_tbl(indx).leg_accts_pay_code_segment7,
               l_suppsite_ext_tbl(indx).leg_prepay_code_segment1,
               l_suppsite_ext_tbl(indx).leg_prepay_code_segment2,
               l_suppsite_ext_tbl(indx).leg_prepay_code_segment3,
               l_suppsite_ext_tbl(indx).leg_prepay_code_segment4,
               l_suppsite_ext_tbl(indx).leg_prepay_code_segment5,
               l_suppsite_ext_tbl(indx).leg_prepay_code_segment6,
               l_suppsite_ext_tbl(indx).leg_prepay_code_segment7,
               l_suppsite_ext_tbl(indx).leg_future_dated_segment1,
               l_suppsite_ext_tbl(indx).leg_future_dated_segment2,
               l_suppsite_ext_tbl(indx).leg_future_dated_segment3,
               l_suppsite_ext_tbl(indx).leg_future_dated_segment4,
               l_suppsite_ext_tbl(indx).leg_future_dated_segment5,
               l_suppsite_ext_tbl(indx).leg_future_dated_segment6,
               l_suppsite_ext_tbl(indx).leg_future_dated_segment7,
               l_suppsite_ext_tbl(indx).leg_pay_group_lookup_code,
               l_suppsite_ext_tbl(indx).leg_payment_priority,
               l_suppsite_ext_tbl(indx).leg_terms_name,
               l_suppsite_ext_tbl(indx).leg_invoice_amount_limit,
               l_suppsite_ext_tbl(indx).leg_pay_date_basis_code,
               l_suppsite_ext_tbl(indx).leg_always_take_disc_flag,
               l_suppsite_ext_tbl(indx).leg_invoice_currency_code,
               l_suppsite_ext_tbl(indx).leg_payment_currency_code,
               l_suppsite_ext_tbl(indx).leg_hold_all_payments_flag,
               l_suppsite_ext_tbl(indx).leg_hold_future_pay_flag,
               l_suppsite_ext_tbl(indx).leg_hold_reason,
               l_suppsite_ext_tbl(indx).leg_hold_unmatched_inv_flag,
               l_suppsite_ext_tbl(indx).leg_ap_tax_rounding_rule,
               /*Decode (l_suppsite_ext_tbl(indx).leg_auto_tax_calc_flag,'L','N','T','N',l_suppsite_ext_tbl(indx).leg_auto_tax_calc_flag),*/
               'Y', /*Begin : PMC : 339760 : Update  - Added by DD on 03/18/2016
                       1.  Supplier header : "Allow Tax Applicability " flag :  need to be
                        enabled (header) (FYI:  Field Name - AP_SUPPLIERS_INT.AUTO_TAX_CALC_FLAG) */
               l_suppsite_ext_tbl(indx).leg_auto_tax_calc_override,
               l_suppsite_ext_tbl(indx).leg_amount_incl_tax_flag,
               l_suppsite_ext_tbl(indx).leg_exclusive_payment_flag,
               l_suppsite_ext_tbl(indx).leg_tax_reporting_site_flag,
               l_suppsite_ext_tbl(indx).leg_attribute_category,
               l_suppsite_ext_tbl(indx).leg_attribute1,
               l_suppsite_ext_tbl(indx).leg_attribute2,
               l_suppsite_ext_tbl(indx).leg_attribute3,
               l_suppsite_ext_tbl(indx).leg_attribute4,
               l_suppsite_ext_tbl(indx).leg_attribute5,
               l_suppsite_ext_tbl(indx).leg_attribute6,
               l_suppsite_ext_tbl(indx).leg_attribute7,
               l_suppsite_ext_tbl(indx).leg_attribute8,
               l_suppsite_ext_tbl(indx).leg_attribute9, --ADB 09/27/2016 reverted changes made by Deepak
               l_suppsite_ext_tbl(indx).leg_attribute10,
               l_suppsite_ext_tbl(indx).leg_attribute11,
               l_suppsite_ext_tbl(indx).leg_attribute12,
               l_suppsite_ext_tbl(indx).leg_attribute13,
               l_suppsite_ext_tbl(indx).leg_attribute14,
               l_suppsite_ext_tbl(indx).leg_attribute15,
               l_suppsite_ext_tbl(indx).leg_validation_number,
               l_suppsite_ext_tbl(indx).leg_excl_freight_from_dist,
               l_suppsite_ext_tbl(indx).leg_vat_registration_num,
               l_suppsite_ext_tbl(indx).leg_operating_unit_name,
               l_suppsite_ext_tbl(indx).leg_check_digits,
               l_suppsite_ext_tbl(indx).leg_address_line4,
               l_suppsite_ext_tbl(indx).leg_county,
               l_suppsite_ext_tbl(indx).leg_address_style,
               l_suppsite_ext_tbl(indx).leg_language,
               l_suppsite_ext_tbl(indx).leg_allow_awt_flag,
               l_suppsite_ext_tbl(indx).leg_awt_group_name,
               l_suppsite_ext_tbl(indx).leg_global_attribute1,
               l_suppsite_ext_tbl(indx).leg_global_attribute2,
               l_suppsite_ext_tbl(indx).leg_global_attribute3,
               l_suppsite_ext_tbl(indx).leg_global_attribute4,
               l_suppsite_ext_tbl(indx).leg_global_attribute5,
               l_suppsite_ext_tbl(indx).leg_global_attribute6,
               l_suppsite_ext_tbl(indx).leg_global_attribute7,
               l_suppsite_ext_tbl(indx).leg_global_attribute8,
               l_suppsite_ext_tbl(indx).leg_global_attribute9,
               l_suppsite_ext_tbl(indx).leg_global_attribute10,
               l_suppsite_ext_tbl(indx).leg_global_attribute11,
               l_suppsite_ext_tbl(indx).leg_global_attribute12,
               l_suppsite_ext_tbl(indx).leg_global_attribute13,
               l_suppsite_ext_tbl(indx).leg_global_attribute14,
               l_suppsite_ext_tbl(indx).leg_global_attribute15,
               l_suppsite_ext_tbl(indx).leg_global_attribute16,
               l_suppsite_ext_tbl(indx).leg_global_attribute17,
               l_suppsite_ext_tbl(indx).leg_global_attribute18,
               l_suppsite_ext_tbl(indx).leg_global_attribute19,
               l_suppsite_ext_tbl(indx).leg_global_attribute20,
               l_suppsite_ext_tbl(indx).leg_global_attribute_category,
               l_suppsite_ext_tbl(indx).leg_edi_id_number,
               l_suppsite_ext_tbl(indx).leg_bank_charge_bearer,
               l_suppsite_ext_tbl(indx).leg_pay_on_code,
               l_suppsite_ext_tbl(indx).leg_default_pay_site,
               l_suppsite_ext_tbl(indx).leg_pay_on_receipt_sum_code,
               l_suppsite_ext_tbl(indx).leg_tp_header_id,
               l_suppsite_ext_tbl(indx).leg_ece_tp_location_code,
               l_suppsite_ext_tbl(indx).leg_pcard_site_flag,
               l_suppsite_ext_tbl(indx).leg_match_option,
               l_suppsite_ext_tbl(indx).leg_country_of_origin_code,
               l_suppsite_ext_tbl(indx).leg_create_debit_memo_flag,
               /*l_suppsite_ext_tbl(indx).leg_offset_tax_flag,*/
               'Y', /* PMC : 339760 : Update  - Added by DD on 03/18/2016
                    2.  Allow "Offset flag" needs to be enabled  - All suppliers
                  (FYI:  Field Name -AP_SUPPLIERS_INT.OFFSET_TAX_FLAG  .OFFSET_TAX_FLAG)*/
               l_suppsite_ext_tbl(indx).leg_supplier_notif_method,
               l_suppsite_ext_tbl(indx).leg_email_address,
               l_suppsite_ext_tbl(indx).leg_remittance_email,
               l_suppsite_ext_tbl(indx).leg_primary_pay_site_flag,
               l_suppsite_ext_tbl(indx).leg_shipping_control,
               l_suppsite_ext_tbl(indx).leg_tolerance_name,
               l_suppsite_ext_tbl(indx).leg_service_tolerance_name,
               l_suppsite_ext_tbl(indx).vendor_interface_id,
               l_suppsite_ext_tbl(indx).vendor_id,
               l_suppsite_ext_tbl(indx).vendor_site_id,
               l_suppsite_ext_tbl(indx).vendor_site_code,
               l_suppsite_ext_tbl(indx).ship_to_location_id,
               l_suppsite_ext_tbl(indx).ship_to_location_code,
               l_suppsite_ext_tbl(indx).bill_to_location_id,
               l_suppsite_ext_tbl(indx).bill_to_location_code,
               l_suppsite_ext_tbl(indx).ship_via_lookup_code,
               l_suppsite_ext_tbl(indx).freight_terms_lookup_code,
               l_suppsite_ext_tbl(indx).fob_lookup_code,
               l_suppsite_ext_tbl(indx).payment_method_lookup_code,
               l_suppsite_ext_tbl(indx).terms_date_basis,
               l_suppsite_ext_tbl(indx).distribution_set_id,
               l_suppsite_ext_tbl(indx).distribution_set_name,
               l_suppsite_ext_tbl(indx).accts_pay_code_combination_id,
               l_suppsite_ext_tbl(indx).prepay_code_combination_id,
               l_suppsite_ext_tbl(indx).future_dated_payment_ccid,
               l_suppsite_ext_tbl(indx).pay_group_lookup_code,
               l_suppsite_ext_tbl(indx).payment_priority,
               l_suppsite_ext_tbl(indx).terms_id,
               l_suppsite_ext_tbl(indx).terms_name,
               l_suppsite_ext_tbl(indx).pay_date_basis_lookup_code,
               l_suppsite_ext_tbl(indx).org_id,
               l_suppsite_ext_tbl(indx).operating_unit_name,
               l_suppsite_ext_tbl(indx).awt_group_id,
               l_suppsite_ext_tbl(indx).awt_group_name,
               l_suppsite_ext_tbl(indx).pay_on_code,
               l_suppsite_ext_tbl(indx).default_pay_site_id,
               l_suppsite_ext_tbl(indx).pay_on_receipt_summary_code,
               l_suppsite_ext_tbl(indx).tp_header_id,
               l_suppsite_ext_tbl(indx).tolerance_id,
               l_suppsite_ext_tbl(indx).vendor_site_interface_id,
               l_suppsite_ext_tbl(indx).payment_method_code,
               l_suppsite_ext_tbl(indx).vendor_name,
               l_suppsite_ext_tbl(indx).vendor_number,
               SYSDATE,
               g_user_id,
               g_user_id,
               g_login_id,
               g_prog_appl_id,
               g_conc_program_id,
               SYSDATE,
               g_request_id,
               g_new,
               NULL,
               l_suppsite_ext_tbl(indx).attribute_category,
               l_suppsite_ext_tbl(indx).attribute1,
               l_suppsite_ext_tbl(indx).attribute2,
               l_suppsite_ext_tbl(indx).attribute3,
               l_suppsite_ext_tbl(indx).attribute4,
               l_suppsite_ext_tbl(indx).attribute5,
               l_suppsite_ext_tbl(indx).attribute6,
               l_suppsite_ext_tbl(indx).attribute7,
               l_suppsite_ext_tbl(indx).attribute8,
               l_suppsite_ext_tbl(indx).leg_vendor_site_id, /*l_suppsite_ext_tbl(indx).attribute9, -- Caputure 11i vendor site id in R12 */
               l_suppsite_ext_tbl(indx).attribute10,
               l_suppsite_ext_tbl(indx).attribute11,
               l_suppsite_ext_tbl(indx).attribute12,
               l_suppsite_ext_tbl(indx).attribute13,
               l_suppsite_ext_tbl(indx).attribute14,
               l_suppsite_ext_tbl(indx).attribute15,
               l_suppsite_ext_tbl(indx).leg_source_system,
               l_suppsite_ext_tbl(indx).leg_request_id,
               l_suppsite_ext_tbl(indx).leg_seq_num,
               l_suppsite_ext_tbl(indx).leg_process_flag,
               l_suppsite_ext_tbl(indx).leg_vendor_number,
               l_suppsite_ext_tbl(indx).leg_vendor_id,
               l_suppsite_ext_tbl(indx).leg_vendor_site_id,
               l_suppsite_ext_tbl(indx).leg_ship_to_location_id,
               l_suppsite_ext_tbl(indx).leg_bill_to_location_id,
               l_suppsite_ext_tbl(indx).leg_distribution_set_id,
               l_suppsite_ext_tbl(indx).leg_accts_pay_code_comb_id,
               l_suppsite_ext_tbl(indx).leg_prepay_code_comb_id,
               l_suppsite_ext_tbl(indx).leg_terms_id,
               l_suppsite_ext_tbl(indx).leg_org_id,
               l_suppsite_ext_tbl(indx).leg_default_pay_site_id,
               l_suppsite_ext_tbl(indx).leg_future_dated_paym_ccid,
               l_suppsite_ext_tbl(indx).leg_selling_co_identifier,
               l_suppsite_ext_tbl(indx).leg_tolerance_id,
               l_suppsite_ext_tbl(indx).leg_services_tolerance_id,
               l_suppsite_ext_tbl(indx).leg_old_vendor_site_code_alt,
               l_suppsite_ext_tbl(indx).leg_old_vendor_id,
               l_suppsite_ext_tbl(indx).last_update_date,
               l_suppsite_ext_tbl(indx).leg_vat_code
               );

          COMMIT;
        EXCEPTION
          WHEN g_bulk_exception THEN
            g_failed_count := g_failed_count + SQL%BULK_EXCEPTIONS.COUNT;
            FOR l_indx_exp IN 1 .. SQL%BULK_EXCEPTIONS.COUNT LOOP

              l_err_record := l_suppsite_ext_tbl(SQL%BULK_EXCEPTIONS(l_indx_exp)
                                                 .ERROR_INDEX)
                             .interface_txn_id;
              g_retcode    := '1';
              fnd_file.put_line(fnd_file.LOG,
                                'Record sequence : ' ||
                                 l_suppsite_ext_tbl(SQL%BULK_EXCEPTIONS(l_indx_exp)
                                                    .error_index)
                                .interface_txn_id);
              fnd_file.put_line(fnd_file.LOG,
                                'Error Message : ' ||
                                SQLERRM(-SQL%BULK_EXCEPTIONS(l_indx_exp)
                                        .ERROR_CODE));

              -- Updating Leg_process_flag to 'E' for failed records
              UPDATE xxap_supplier_sites_stg_R12
                 SET leg_process_flag  = g_error,
                     last_updated_by   = g_user_id,
                     last_update_login = g_login_id,
                     last_update_date  = SYSDATE
               WHERE interface_txn_id = l_err_record
                 AND leg_process_flag = g_validated;

            END LOOP;
        END;

        EXIT WHEN suppsite_ext_cur%NOTFOUND;
      END LOOP;
      CLOSE suppsite_ext_cur;


      -- Update Successful records in Extraction Table
      UPDATE xxap_supplier_sites_stg_R12 xssser
         SET xssser.leg_process_flag = g_processed,
             last_updated_by         = g_user_id,
             last_update_login       = g_login_id,
             last_update_date        = SYSDATE
       WHERE xssser.leg_process_flag = g_validated
         AND EXISTS
       (SELECT 1
                FROM xxap_supplier_sites_stg xsss
               WHERE xsss.interface_txn_id = xssser.interface_txn_id);
    END IF;

    IF g_entity = g_supplier_contacts THEN
      -- Open Cursor for all branch records
      OPEN suppcont_ext_cur;
      LOOP

        l_suppcont_ext_tbl.DELETE;

        FETCH suppcont_ext_cur BULK COLLECT
          INTO l_suppcont_ext_tbl LIMIT 1000;

        -- Get Total Count
        g_total_count := g_total_count + l_suppcont_ext_tbl.COUNT;

        BEGIN
          FORALL indx IN 1 .. l_suppcont_ext_tbl.COUNT SAVE EXCEPTIONS
            INSERT INTO xxap_supplier_contacts_stg
              (interface_txn_id,
               batch_id,
               run_sequence_id,
               leg_vendor_name,
               leg_vendor_site_code,
               leg_operating_unit_name,
               leg_inactive_date,
               leg_first_name,
               leg_middle_name,
               leg_last_name,
               leg_prefix,
               leg_title,
               leg_mail_stop,
               leg_area_code,
               leg_phone,
               leg_contact_name_alt,
               leg_first_name_alt,
               leg_last_name_alt,
               leg_department,
               leg_email_address,
               leg_url,
               leg_alt_area_code,
               leg_alt_phone,
               leg_fax_area_code,
               leg_fax,
               leg_attribute_category,
               leg_attribute1,
               leg_attribute2,
               leg_attribute3,
               leg_attribute4,
               leg_attribute5,
               leg_attribute6,
               leg_attribute7,
               leg_attribute8,
               leg_attribute9,
               leg_attribute10,
               leg_attribute11,
               leg_attribute12,
               leg_attribute13,
               leg_attribute14,
               leg_attribute15,
               vendor_site_id,
               vendor_site_code,
               org_id,
               operating_unit_name,
               vendor_interface_id,
               vendor_id,
               vendor_contact_interface_id,
               vendor_name,
               vendor_number,
               vendor_contact_id,
               phone,
               fax,
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
               error_type,
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
               leg_vendor_contact_id,
               last_update_date,
               leg_vendor_site_id)
            VALUES
              (l_suppcont_ext_tbl(indx).interface_txn_id,
               NULL,
               NULL,
               l_suppcont_ext_tbl(indx).leg_vendor_name,
               l_suppcont_ext_tbl(indx).leg_vendor_site_code,
               l_suppcont_ext_tbl(indx).leg_operating_unit_name,
               l_suppcont_ext_tbl(indx).leg_inactive_date,
               l_suppcont_ext_tbl(indx).leg_first_name,
               l_suppcont_ext_tbl(indx).leg_middle_name,
               l_suppcont_ext_tbl(indx).leg_last_name,
               l_suppcont_ext_tbl(indx).leg_prefix,
               l_suppcont_ext_tbl(indx).leg_title,
               l_suppcont_ext_tbl(indx).leg_mail_stop,
               l_suppcont_ext_tbl(indx).leg_area_code,
               l_suppcont_ext_tbl(indx).leg_phone,
               l_suppcont_ext_tbl(indx).leg_contact_name_alt,
               l_suppcont_ext_tbl(indx).leg_first_name_alt,
               l_suppcont_ext_tbl(indx).leg_last_name_alt,
               l_suppcont_ext_tbl(indx).leg_department,
               l_suppcont_ext_tbl(indx).leg_email_address,
               l_suppcont_ext_tbl(indx).leg_url,
               l_suppcont_ext_tbl(indx).leg_alt_area_code,
               l_suppcont_ext_tbl(indx).leg_alt_phone,
               l_suppcont_ext_tbl(indx).leg_fax_area_code,
               l_suppcont_ext_tbl(indx).leg_fax,
               l_suppcont_ext_tbl(indx).leg_attribute_category,
                l_suppcont_ext_tbl(indx).leg_vendor_contact_id, /*l_suppcont_ext_tbl(indx).leg_attribute1, To Capture Contact id in R12*/
               l_suppcont_ext_tbl(indx).leg_attribute2,
               l_suppcont_ext_tbl(indx).leg_attribute3,
               l_suppcont_ext_tbl(indx).leg_attribute4,
               l_suppcont_ext_tbl(indx).leg_attribute5,
               l_suppcont_ext_tbl(indx).leg_attribute6,
               l_suppcont_ext_tbl(indx).leg_attribute7,
               l_suppcont_ext_tbl(indx).leg_attribute8,
               l_suppcont_ext_tbl(indx).leg_attribute9,
               l_suppcont_ext_tbl(indx).leg_attribute10,
               l_suppcont_ext_tbl(indx).leg_attribute11,
               l_suppcont_ext_tbl(indx).leg_attribute12,
               l_suppcont_ext_tbl(indx).leg_attribute13,
               l_suppcont_ext_tbl(indx).leg_attribute14,
               l_suppcont_ext_tbl(indx).leg_attribute15,
               l_suppcont_ext_tbl(indx).vendor_site_id,
               l_suppcont_ext_tbl(indx).vendor_site_code,
               l_suppcont_ext_tbl(indx).org_id,
               l_suppcont_ext_tbl(indx).operating_unit_name,
               l_suppcont_ext_tbl(indx).vendor_interface_id,
               l_suppcont_ext_tbl(indx).vendor_id,
               l_suppcont_ext_tbl(indx).vendor_contact_interface_id,
               l_suppcont_ext_tbl(indx).vendor_name,
               l_suppcont_ext_tbl(indx).vendor_number,
               l_suppcont_ext_tbl(indx).vendor_contact_id
               --,  l_suppcont_ext_tbl(indx).last_update_date
              ,
               l_suppcont_ext_tbl(indx).phone,
               l_suppcont_ext_tbl(indx).fax,
               SYSDATE,
               g_user_id,
               SYSDATE,
               g_user_id,
               g_login_id,
               g_prog_appl_id,
               g_conc_program_id,
               SYSDATE,
               g_request_id,
               g_new,
               NULL,
               l_suppcont_ext_tbl(indx).attribute_category,
               l_suppcont_ext_tbl(indx).leg_vendor_contact_id, /*l_suppcont_ext_tbl(indx).attribute1, To Capture contact id in R12*/
               l_suppcont_ext_tbl(indx).attribute2,
               l_suppcont_ext_tbl(indx).attribute3,
               l_suppcont_ext_tbl(indx).attribute4,
               l_suppcont_ext_tbl(indx).attribute5,
               l_suppcont_ext_tbl(indx).attribute6,
               l_suppcont_ext_tbl(indx).attribute7,
               l_suppcont_ext_tbl(indx).attribute8,
               l_suppcont_ext_tbl(indx).attribute9,
               l_suppcont_ext_tbl(indx).attribute10,
               l_suppcont_ext_tbl(indx).attribute11,
               l_suppcont_ext_tbl(indx).attribute12,
               l_suppcont_ext_tbl(indx).attribute13,
               l_suppcont_ext_tbl(indx).attribute14,
               l_suppcont_ext_tbl(indx).attribute15,
               l_suppcont_ext_tbl(indx).leg_source_system,
               l_suppcont_ext_tbl(indx).leg_request_id,
               l_suppcont_ext_tbl(indx).leg_seq_num,
               l_suppcont_ext_tbl(indx).leg_process_flag,
               l_suppcont_ext_tbl(indx).leg_vendor_contact_id,
               l_suppcont_ext_tbl(indx).last_update_date,
               l_suppcont_ext_tbl(indx).leg_vendor_site_id);

          COMMIT;
        EXCEPTION
          WHEN g_bulk_exception THEN
            g_failed_count := g_failed_count + SQL%BULK_EXCEPTIONS.COUNT;
            FOR l_indx_exp IN 1 .. SQL%BULK_EXCEPTIONS.COUNT LOOP

              l_err_record := l_suppcont_ext_tbl(SQL%BULK_EXCEPTIONS(l_indx_exp)
                                                 .ERROR_INDEX)
                             .interface_txn_id;
              g_retcode    := '1';
              fnd_file.put_line(fnd_file.LOG,
                                'Record sequence : ' ||
                                 l_suppcont_ext_tbl(SQL%BULK_EXCEPTIONS(l_indx_exp)
                                                    .error_index)
                                .interface_txn_id);
              fnd_file.put_line(fnd_file.LOG,
                                'Error Message : ' ||
                                SQLERRM(-SQL%BULK_EXCEPTIONS(l_indx_exp)
                                        .ERROR_CODE));

              -- Updating Leg_process_flag to 'E' for failed records
              UPDATE xxap_supplier_contacts_stg_R12
                 SET leg_process_flag  = g_error,
                     last_updated_by   = g_user_id,
                     last_update_login = g_login_id,
                     last_update_date  = SYSDATE
               WHERE interface_txn_id = l_err_record
                 AND leg_process_flag = g_validated;

            END LOOP;
        END;

        EXIT WHEN suppcont_ext_cur%NOTFOUND;
      END LOOP;
      CLOSE suppcont_ext_cur;

      -- Update Successful records in Extraction Table
      UPDATE xxap_supplier_contacts_stg_R12 xscer
         SET xscer.leg_process_flag = g_processed,
             last_updated_by        = g_user_id,
             last_update_login      = g_login_id,
             last_update_date       = SYSDATE
       WHERE xscer.leg_process_flag = g_validated
         AND EXISTS
       (SELECT 1
                FROM xxap_supplier_contacts_stg xscs
               WHERE xscs.interface_txn_id = xscer.interface_txn_id);
    END IF;
    --Manoj:END

    -- /** Added for v1.1 **/
    IF g_entity = g_int_accts THEN
      -- If Entity is Intermediary Accounts

      --Calling SQL Loader Program to load IBAN and Intermediary Accounts
      submit_request(pon_request_id    => l_request_id,
                     pov_return_status => l_return_stat,
                     pov_return_msg    => l_return_msg,
                     piv_argument1     => g_data_file);

      IF l_return_stat = 'W' THEN
        g_retcode := 1;
        print_log_message(l_return_msg);
      ELSIF l_return_stat = 'E' THEN
        g_retcode := 2;
        print_log_message(l_return_msg);
      END IF;

      -- Get Count of Total Records loaded
      SELECT COUNT(1)
        INTO g_total_count
        FROM xxap_supplier_int_accts_stg xsias
       WHERE xsias.batch_id IS NULL
         AND xsias.run_sequence_id IS NULL;

    END IF;

  EXCEPTION
    WHEN OTHERS THEN
      g_retcode       := 2;
      l_error_message := SUBSTR('In Exception while loading data' ||
                                SQLERRM,
                                1,
                                1999);
      print_log_message(l_error_message);
      print_log_message(' - get_data -');
      ROLLBACK;
  END get_data;

  --
  -- ========================
  -- Procedure: pre_validate
  -- =============================================================================
  --   This procedure performs pre validations for Suppliers
  -- =============================================================================
  --  Input Parameters :
  --    None
  --  Output Parameters :
  --    None
  -- -----------------------------------------------------------------------------
  --
  PROCEDURE pre_validate IS

    l_lookup_exists NUMBER;
    --l_receipt_lkp    CONSTANT VARCHAR2(50) := 'ETN_AR_RECEIPT_METHOD';
    --l_ou_lkp         CONSTANT VARCHAR2(50) := 'ETN_COMMON_OU_MAP';
    l_vendor_lkp CONSTANT VARCHAR2(50) := 'XXAP_VENDOR_TYPE_LOOKUP_CODE';
    --l_loc_lkp        CONSTANT VARCHAR2(50) := 'ETN_AP_LOCATION_CODE';
    l_ship_lkp       CONSTANT VARCHAR2(50) := 'XXAP_SHIP_VIA_LOOKUP_CODE';
    l_freight_lkp    CONSTANT VARCHAR2(50) := 'XXAP_FREIGHT_TERMS_LOOKUP';
    l_fob_lkp        CONSTANT VARCHAR2(50) := 'XXAP_FOB_LOOKUP_CODE';
    l_pay_grp_lkp    CONSTANT VARCHAR2(50) := 'XXAP_PAY_GROUP_LOOKUP_CODE';
    l_org_type_lkp   CONSTANT VARCHAR2(50) := 'XXAP_ORG_TYPE_LOOKUP';
    l_minority_lkp   CONSTANT VARCHAR2(50) := 'XXAP_MINORITY_GRP_LOOKUP';
    l_pay_method_lkp CONSTANT VARCHAR2(50) := 'XXAP_PAYMENT_METHOD_LOOKUP';
    l_pay_terms_lkp  CONSTANT VARCHAR2(50) := 'XXAP_PAYTERM_MAPPING';
  BEGIN

    l_lookup_exists := 0;

    xxetn_debug_pkg.add_debug('+   PROCEDURE : pre_validate +');

    /**
          print_log_message ( '+ Checking Location Cross Reference +' );
          -- check whether the lookup ETN_AP_LOCATION_CODE exists

          l_lookup_exists := 0;

          BEGIN
             SELECT 1
               INTO l_lookup_exists
               FROM fnd_lookup_types flv
              WHERE flv.lookup_type =  l_loc_lkp;
          EXCEPTION
             WHEN NO_DATA_FOUND
             THEN
                l_lookup_exists := 0;
                print_log_message ( ' In No Data found of Location Cross Reference lookup check'||SQLERRM);
             WHEN OTHERS
             THEN
                l_lookup_exists := 0;
                print_log_message ( ' In when others of Location Cross Reference lookup check'||SQLERRM);
          END;

          IF l_lookup_exists = 0
          THEN
             g_retcode := 1;
             FND_FILE.PUT_LINE(FND_FILE.OUTPUT, 'Location Cross Reference lookup is not setup');
          END IF;

          print_log_message ( '- Checking Location Cross Reference -' );
    **/

    print_log_message('+ Checking Payment Term Cross Reference +');

    -- check whether the lookup XXAP_PAYTERM_MAPPING exists

    l_lookup_exists := 0;

    BEGIN
      SELECT 1
        INTO l_lookup_exists
        FROM fnd_lookup_types flv
       WHERE flv.lookup_type = l_pay_terms_lkp;
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        l_lookup_exists := 0;
        print_log_message(' In No Data found of Payment Terms Cross Reference lookup check' ||
                          SQLERRM);
      WHEN OTHERS THEN
        l_lookup_exists := 0;
        print_log_message(' In when others of Payment Terms Cross Reference lookup check' ||
                          SQLERRM);
    END;

    IF l_lookup_exists = 0 THEN
      g_retcode := 1;
      FND_FILE.PUT_LINE(FND_FILE.OUTPUT,
                        'Payment Terms Cross Reference lookup is not setup');
    END IF;

    print_log_message('- Checking Payment Term Cross Reference -');

    print_log_message('+ Checking Reason Code Cross Reference +');

    print_log_message('- Checking Reason Code Cross Reference -');

    print_log_message('+ Checking COA Cross Reference +');

    print_log_message('- Checking COA Cross Reference -');

    print_log_message('+ Checking Pay Group Cross Reference +');

    -- check whether the lookup XXAP_PAY_GROUP_LOOKUP_CODE exists

    l_lookup_exists := 0;

    BEGIN
      SELECT 1
        INTO l_lookup_exists
        FROM fnd_lookup_types flv
       WHERE flv.lookup_type = l_pay_grp_lkp;
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        l_lookup_exists := 0;
        print_log_message(' In No Data found of Pay Group Cross Reference lookup check' ||
                          SQLERRM);
      WHEN OTHERS THEN
        l_lookup_exists := 0;
        print_log_message(' In when others of Pay Group Cross Reference lookup check' ||
                          SQLERRM);
    END;

    IF l_lookup_exists = 0 THEN
      g_retcode := 1;
      FND_FILE.PUT_LINE(FND_FILE.OUTPUT,
                        'Pay Group Cross Reference lookup is not setup');
    END IF;

    print_log_message('- Checking Pay Group Cross Reference -');

    print_log_message('+ Checking Pay Methods Cross Reference +');

    -- check whether the lookup XXAP_PAYMENT_METHOD_LOOKUP exists

    l_lookup_exists := 0;

    BEGIN
      SELECT 1
        INTO l_lookup_exists
        FROM fnd_lookup_types flv
       WHERE flv.lookup_type = l_pay_method_lkp;
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        l_lookup_exists := 0;
        print_log_message(' In No Data found of Pay Methods Cross Reference lookup check' ||
                          SQLERRM);
      WHEN OTHERS THEN
        l_lookup_exists := 0;
        print_log_message(' In when others of Pay Methods Cross Reference lookup check' ||
                          SQLERRM);
    END;

    IF l_lookup_exists = 0 THEN
      g_retcode := 1;
      FND_FILE.PUT_LINE(FND_FILE.OUTPUT,
                        'Pay Methods Cross Reference lookup is not setup');
    END IF;

    print_log_message('- Checking Pay Methods Cross Reference -');

    print_log_message('+ Checking Vendor Type Cross Reference +');

    -- check whether the lookup XXAP_VENDOR_TYPE_LOOKUP_CODE exists

    l_lookup_exists := 0;

    BEGIN
      SELECT 1
        INTO l_lookup_exists
        FROM fnd_lookup_types flv
       WHERE flv.lookup_type = l_vendor_lkp;
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        l_lookup_exists := 0;
        print_log_message(' In No Data found of Vendor Type Cross Reference lookup check' ||
                          SQLERRM);
      WHEN OTHERS THEN
        l_lookup_exists := 0;
        print_log_message(' In when others of Vendor Type Cross Reference lookup check' ||
                          SQLERRM);
    END;

    IF l_lookup_exists = 0 THEN
      g_retcode := 1;
      FND_FILE.PUT_LINE(FND_FILE.OUTPUT,
                        'Vendor Type Cross Reference lookup is not setup');
    END IF;

    print_log_message('- Checking Vendor Type Cross Reference -');

    print_log_message('+ Checking Organization Type Cross Reference +');

    -- check whether the lookup XXAP_ORG_TYPE_LOOKUP exists

    l_lookup_exists := 0;

    BEGIN
      SELECT 1
        INTO l_lookup_exists
        FROM fnd_lookup_types flv
       WHERE flv.lookup_type = l_org_type_lkp;
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        l_lookup_exists := 0;
        print_log_message(' In No Data found of Org Type Cross Reference lookup check' ||
                          SQLERRM);
      WHEN OTHERS THEN
        l_lookup_exists := 0;
        print_log_message(' In when others of Org Type Cross Reference lookup check' ||
                          SQLERRM);
    END;

    IF l_lookup_exists = 0 THEN
      g_retcode := 1;
      FND_FILE.PUT_LINE(FND_FILE.OUTPUT,
                        'Org Type Cross Reference lookup is not setup');
    END IF;

    print_log_message('- Checking Organization Type Cross Reference -');

    print_log_message('+ Checking ship via lookup code Cross Reference +');

    -- check whether the lookup XXAP_SHIP_VIA_LOOKUP_CODE exists

    l_lookup_exists := 0;

    BEGIN
      SELECT 1
        INTO l_lookup_exists
        FROM fnd_lookup_types flv
       WHERE flv.lookup_type = l_ship_lkp;
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        l_lookup_exists := 0;
        print_log_message(' In No Data found of Ship Via Cross Reference lookup check' ||
                          SQLERRM);
      WHEN OTHERS THEN
        l_lookup_exists := 0;
        print_log_message(' In when others of Ship via Reference lookup check' ||
                          SQLERRM);
    END;

    IF l_lookup_exists = 0 THEN
      g_retcode := 1;
      FND_FILE.PUT_LINE(FND_FILE.OUTPUT,
                        'Ship via Cross Reference lookup is not setup');
    END IF;

    print_log_message('- Checking ship via lookup code Cross Reference -');

    print_log_message('+ Checking freight term lookup code Cross Reference +');

    -- check whether the lookup XXAP_FREIGHT_TERMS_LOOKUP exists

    l_lookup_exists := 0;

    BEGIN
      SELECT 1
        INTO l_lookup_exists
        FROM fnd_lookup_types flv
       WHERE flv.lookup_type = l_freight_lkp;
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        l_lookup_exists := 0;
        print_log_message(' In No Data found of Freight Term Cross Reference lookup check' ||
                          SQLERRM);
      WHEN OTHERS THEN
        l_lookup_exists := 0;
        print_log_message(' In when others of Freight Term Cross Reference lookup check' ||
                          SQLERRM);
    END;

    IF l_lookup_exists = 0 THEN
      g_retcode := 1;
      FND_FILE.PUT_LINE(FND_FILE.OUTPUT,
                        'Freight Term Cross Reference lookup is not setup');
    END IF;

    print_log_message('- Checking freight term lookup code Cross Reference -');

    print_log_message('+ Checking FOB lookup code Cross Reference +');

    -- check whether the lookup XXAP_FOB_LOOKUP_CODE exists

    l_lookup_exists := 0;

    BEGIN
      SELECT 1
        INTO l_lookup_exists
        FROM fnd_lookup_types flv
       WHERE flv.lookup_type = l_fob_lkp;
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        l_lookup_exists := 0;
        print_log_message(' In No Data found of FOB Cross Reference lookup check' ||
                          SQLERRM);
      WHEN OTHERS THEN
        l_lookup_exists := 0;
        print_log_message(' In when others of FOB Cross Reference lookup check' ||
                          SQLERRM);
    END;

    IF l_lookup_exists = 0 THEN
      g_retcode := 1;
      FND_FILE.PUT_LINE(FND_FILE.OUTPUT,
                        'FOB Cross Reference lookup is not setup');
    END IF;

    print_log_message('- Checking FOB lookup code Cross Reference -');

    print_log_message('+ Checking Minority group  lookup code Cross Reference +');

    -- check whether the lookup XXAP_MINORITY_GRP_LOOKUP exists

    l_lookup_exists := 0;

    BEGIN
      SELECT 1
        INTO l_lookup_exists
        FROM fnd_lookup_types flv
       WHERE flv.lookup_type = l_minority_lkp;
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        l_lookup_exists := 0;
        print_log_message(' In No Data found of Minority group Cross Reference lookup check' ||
                          SQLERRM);
      WHEN OTHERS THEN
        l_lookup_exists := 0;
        print_log_message(' In when others of Minority group Cross Reference lookup check' ||
                          SQLERRM);
    END;

    IF l_lookup_exists = 0 THEN
      g_retcode := 1;
      FND_FILE.PUT_LINE(FND_FILE.OUTPUT,
                        'Minority group Cross Reference lookup is not setup');
    END IF;

    print_log_message('- Checking Minority group lookup code Cross Reference -');

    /**
          print_log_message ( '+ Checking Operating Unit Lookup +' );
          l_lookup_exists := 0;

          -- check whether the lookup ETN_COMMON_OU_MAP exists
          BEGIN
             SELECT 1
               INTO l_lookup_exists
               FROM fnd_lookup_types flv
              WHERE flv.lookup_type =  l_ou_lkp;
          EXCEPTION
             WHEN NO_DATA_FOUND
             THEN
                l_lookup_exists := 0;
                print_log_message ( ' In No Data found of Common OU Lookup check'||SQLERRM);
             WHEN OTHERS
             THEN
                l_lookup_exists := 0;
                print_log_message ( ' In when others of Common OU lookup check'||SQLERRM);
          END;

          IF l_lookup_exists = 0
          THEN
             g_retcode := 1;
             FND_FILE.PUT_LINE(FND_FILE.OUTPUT, 'COMMON OU LOOKUP IS NOT SETUP');
          END IF;

          print_log_message ( '- Checking Operating Unit Lookup -' );
    **/

    -- If Error Table Type Limit is not set
    IF g_limit IS NULL THEN
      g_retcode := 1;
      FND_FILE.PUT_LINE(FND_FILE.OUTPUT,
                        'Value for Profile== Eaton:Error Table Type Limit is not set. ');
    END IF;

    xxetn_debug_pkg.add_debug('-   PROCEDURE : pre_validate -');

  EXCEPTION
    WHEN OTHERS THEN
      g_retcode := 2;
      print_log_message('In Pre Validate when others' || SQLERRM);

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
  PROCEDURE assign_batch_id IS
    l_err_msg        VARCHAR2(2000);
    l_ret_status     VARCHAR2(50);
    l_log_ret_status VARCHAR2(50);
    l_log_err_msg    VARCHAR2(2000);
  BEGIN

    xxetn_debug_pkg.add_debug('+   PROCEDURE : assign_batch_id - g_batch_id :' ||
                              g_batch_id);
    l_log_err_msg    := NULL;
    l_log_ret_status := NULL;
    l_err_msg        := NULL;

    -- g_batch_id NULL is considered a fresh run
    IF g_batch_id IS NULL THEN
      --if entity is BANK update bank staging table
      IF g_entity = g_bank THEN
        UPDATE xxap_supplier_banks_stg
           SET batch_id               = g_new_batch_id,
               process_flag           = g_new,
               run_sequence_id        = g_run_seq_id,
               last_updated_date      = SYSDATE,
               last_updated_by        = g_user_id,
               last_update_login      = g_login_id,
               program_application_id = g_prog_appl_id,
               program_id             = g_conc_program_id,
               program_update_date    = SYSDATE,
               request_id             = g_request_id
         WHERE batch_id IS NULL;

        xxetn_debug_pkg.add_debug('Updated bank staging table where batch id is null' ||
                                  SQL%ROWCOUNT);
        --if entity is BRANCH update bank staging table
      ELSIF g_entity = g_branch THEN
        UPDATE xxap_supplier_branches_stg
           SET batch_id               = g_new_batch_id,
               process_flag           = g_new,
               run_sequence_id        = g_run_seq_id,
               last_update_date       = SYSDATE,
               last_updated_by        = g_user_id,
               last_update_login      = g_login_id,
               program_application_id = g_prog_appl_id,
               program_id             = g_conc_program_id,
               program_update_date    = SYSDATE,
               request_id             = g_request_id
         WHERE batch_id IS NULL;

        xxetn_debug_pkg.add_debug('Updated branch staging table where batch id is null' ||
                                  SQL%ROWCOUNT);
        --if entity is ACCOUNT update account staging table
      ELSIF g_entity = g_account THEN
        UPDATE xxap_supplier_bankaccnts_stg
           SET batch_id               = g_new_batch_id,
               process_flag           = g_new,
               run_sequence_id        = g_run_seq_id,
               last_updated_date      = SYSDATE,
               last_updated_by        = g_user_id,
               last_update_login      = g_login_id,
               program_application_id = g_prog_appl_id,
               program_id             = g_conc_program_id,
               program_update_date    = SYSDATE,
               request_id             = g_request_id
         WHERE batch_id IS NULL;

        xxetn_debug_pkg.add_debug('Updated account staging table where batch id is null' ||
                                  SQL%ROWCOUNT);
        --Manoj:START
        --if entity is SUPPLIER update supplier staging table
      ELSIF g_entity = g_supplier THEN
        UPDATE xxap_suppliers_stg
           SET batch_id               = g_new_batch_id,
               process_flag           = g_new,
               run_sequence_id        = g_run_seq_id,
               last_updated_date      = SYSDATE,
               last_updated_by        = g_user_id,
               last_update_login      = g_login_id,
               program_application_id = g_prog_appl_id,
               program_id             = g_conc_program_id,
               program_update_date    = SYSDATE,
               request_id             = g_request_id
         WHERE batch_id IS NULL;

        xxetn_debug_pkg.add_debug('Updated supplier staging table where batch id is null' ||
                                  SQL%ROWCOUNT);
        --if entity is SUPPLIER_SITES update supplier sites staging table
      ELSIF g_entity = g_supplier_sites THEN
        UPDATE xxap_supplier_sites_stg
           SET batch_id               = g_new_batch_id,
               process_flag           = g_new,
               run_sequence_id        = g_run_seq_id,
               last_update_date       = SYSDATE,
               last_updated_by        = g_user_id,
               last_update_login      = g_login_id,
               program_application_id = g_prog_appl_id,
               program_id             = g_conc_program_id,
               program_update_date    = SYSDATE,
               request_id             = g_request_id
         WHERE batch_id IS NULL;

        xxetn_debug_pkg.add_debug('Updated supplier sites staging table where batch id is null' ||
                                  SQL%ROWCOUNT);
        --if entity is SUPPLIER_CONTACTS update supplier contacts staging table
      ELSIF g_entity = g_supplier_contacts THEN
        UPDATE xxap_supplier_contacts_stg
           SET batch_id               = g_new_batch_id,
               process_flag           = g_new,
               run_sequence_id        = g_run_seq_id,
               last_updated_date      = SYSDATE,
               last_updated_by        = g_user_id,
               last_update_login      = g_login_id,
               program_application_id = g_prog_appl_id,
               program_id             = g_conc_program_id,
               program_update_date    = SYSDATE,
               request_id             = g_request_id
         WHERE batch_id IS NULL;

        xxetn_debug_pkg.add_debug('Updated supplier contacts staging table where batch id is null' ||
                                  SQL%ROWCOUNT);
        --Manoj:END

        /** Added for v1.1 **/
        --if entity is Supplier Intermediary Accounts update Supplier Intermediary accounts staging table
      ELSIF g_entity = g_int_accts THEN
        UPDATE xxap_supplier_int_accts_stg xsias
           SET xsias.batch_id               = g_new_batch_id,
               xsias.process_flag           = g_new,
               xsias.run_sequence_id        = g_run_seq_id,
               xsias.created_by             = g_user_id,
               xsias.last_update_date       = SYSDATE,
               xsias.last_updated_by        = g_user_id,
               xsias.last_update_login      = g_login_id,
               xsias.program_application_id = g_prog_appl_id,
               xsias.program_id             = g_conc_program_id,
               xsias.program_update_date    = SYSDATE,
               xsias.request_id             = g_request_id
         WHERE xsias.batch_id IS NULL;

        xxetn_debug_pkg.add_debug('Updated supplier intermediary accounts staging table where batch id is null: ' ||
                                  SQL%ROWCOUNT);
      END IF;

    ELSE

      --------------------------------------------
      -- All : All the records in batch other than obsolete
      --------------------------------------------
      -- Error : Records where validated flag are 'E'
      --------------------------------------------
      -- Unprocessed : Records which are assigned
      -- batch ID but are in new status
      --------------------------------------------
      IF g_process_records = 'ALL' THEN
        --if entity is BANK update bank staging table
        IF g_entity = g_bank THEN
          UPDATE xxap_supplier_banks_stg
             SET process_flag           = g_new,
                 run_sequence_id        = g_run_seq_id,
                 last_updated_date      = SYSDATE,
                 last_updated_by        = g_user_id,
                 last_update_login      = g_login_id,
                 program_application_id = g_prog_appl_id,
                 program_id             = g_conc_program_id,
                 program_update_date    = SYSDATE,
                 request_id             = g_request_id
           WHERE batch_id = g_new_batch_id
             AND process_flag NOT IN (g_obsolete, g_converted);
          --if entity is BRANCH update bank staging table
        ELSIF g_entity = g_branch THEN
          UPDATE xxap_supplier_branches_stg
             SET process_flag           = g_new,
                 run_sequence_id        = g_run_seq_id,
                 last_update_date       = SYSDATE,
                 last_updated_by        = g_user_id,
                 last_update_login      = g_login_id,
                 program_application_id = g_prog_appl_id,
                 program_id             = g_conc_program_id,
                 program_update_date    = SYSDATE,
                 request_id             = g_request_id
           WHERE batch_id = g_new_batch_id
             AND process_flag NOT IN (g_obsolete, g_converted);
          xxetn_debug_pkg.add_debug('Updated staging table where process record All' ||
                                    SQL%ROWCOUNT);
          --if entity is ACCOUNT update account staging table
        ELSIF g_entity = g_account THEN
          UPDATE xxap_supplier_bankaccnts_stg
             SET process_flag           = g_new,
                 run_sequence_id        = g_run_seq_id,
                 last_updated_date      = SYSDATE,
                 last_updated_by        = g_user_id,
                 last_update_login      = g_login_id,
                 program_application_id = g_prog_appl_id,
                 program_id             = g_conc_program_id,
                 program_update_date    = SYSDATE,
                 request_id             = g_request_id
           WHERE batch_id = g_new_batch_id
             AND process_flag NOT IN (g_obsolete, g_converted);
          xxetn_debug_pkg.add_debug('Updated staging table where process record All' ||
                                    SQL%ROWCOUNT);
          --Manoj:START
          --if entity is SUPPLIER update supplier staging table
        ELSIF g_entity = g_supplier THEN
          UPDATE xxap_suppliers_stg
             SET process_flag           = g_new,
                 run_sequence_id        = g_run_seq_id,
                 last_updated_date      = SYSDATE,
                 last_updated_by        = g_user_id,
                 last_update_login      = g_login_id,
                 program_application_id = g_prog_appl_id,
                 program_id             = g_conc_program_id,
                 program_update_date    = SYSDATE,
                 request_id             = g_request_id
           WHERE batch_id = g_new_batch_id
             AND process_flag NOT IN (g_obsolete, g_converted);
          xxetn_debug_pkg.add_debug('Updated supplier staging table where process record All ' ||
                                    SQL%ROWCOUNT);
          --if entity is SUPPLIER_SITES update supplier sites staging table
        ELSIF g_entity = g_supplier_sites THEN
          UPDATE xxap_supplier_sites_stg
             SET process_flag           = g_new,
                 run_sequence_id        = g_run_seq_id,
                 last_update_date       = SYSDATE,
                 last_updated_by        = g_user_id,
                 last_update_login      = g_login_id,
                 program_application_id = g_prog_appl_id,
                 program_id             = g_conc_program_id,
                 program_update_date    = SYSDATE,
                 request_id             = g_request_id
           WHERE batch_id = g_new_batch_id
             AND process_flag NOT IN (g_obsolete, g_converted,'D');
          xxetn_debug_pkg.add_debug('Updated supplier sites staging table process record All ' ||
                                    SQL%ROWCOUNT);
          --if entity is SUPPLIER_CONTACTS update supplier contacts staging table
        ELSIF g_entity = g_supplier_contacts THEN
          UPDATE xxap_supplier_contacts_stg
             SET process_flag           = g_new,
                 run_sequence_id        = g_run_seq_id,
                 last_updated_date      = SYSDATE,
                 last_updated_by        = g_user_id,
                 last_update_login      = g_login_id,
                 program_application_id = g_prog_appl_id,
                 program_id             = g_conc_program_id,
                 program_update_date    = SYSDATE,
                 request_id             = g_request_id
           WHERE batch_id = g_new_batch_id
             AND process_flag NOT IN (g_obsolete, g_converted);
          xxetn_debug_pkg.add_debug('Updated supplier sites contacts table process record All ' ||
                                    SQL%ROWCOUNT);

          /** Added for v1.1 **/
          --if entity is Supplier Intermediary Accounts update Supplier Intermediary accounts staging table
        ELSIF g_entity = g_int_accts THEN
          UPDATE xxap_supplier_int_accts_stg
             SET process_flag           = g_new,
                 run_sequence_id        = g_run_seq_id,
                 last_update_date       = SYSDATE,
                 last_updated_by        = g_user_id,
                 last_update_login      = g_login_id,
                 program_application_id = g_prog_appl_id,
                 program_id             = g_conc_program_id,
                 program_update_date    = SYSDATE,
                 request_id             = g_request_id
           WHERE batch_id = g_new_batch_id
             AND process_flag NOT IN (g_obsolete, g_converted);
          xxetn_debug_pkg.add_debug('Updated supplier intermediary accounts table process record All ' ||
                                    SQL%ROWCOUNT);
        END IF;

      ELSIF g_process_records = 'ERROR' THEN
        --if entity is BANK update bank staging table
        IF g_entity = g_bank THEN
          UPDATE xxap_supplier_banks_stg
             SET process_flag           = g_new,
                 run_sequence_id        = g_run_seq_id,
                 last_updated_date      = SYSDATE,
                 last_updated_by        = g_user_id,
                 last_update_login      = g_login_id,
                 program_application_id = g_prog_appl_id,
                 program_id             = g_conc_program_id,
                 program_update_date    = SYSDATE,
                 request_id             = g_request_id
           WHERE batch_id = g_new_batch_id
             AND process_flag = g_error;

          xxetn_debug_pkg.add_debug('Updated staging table where process record Error' ||
                                    SQL%ROWCOUNT);
        ELSIF g_entity = g_branch THEN
          UPDATE xxap_supplier_branches_stg
             SET process_flag           = g_new,
                 run_sequence_id        = g_run_seq_id,
                 last_update_date       = SYSDATE,
                 last_updated_by        = g_user_id,
                 last_update_login      = g_login_id,
                 program_application_id = g_prog_appl_id,
                 program_id             = g_conc_program_id,
                 program_update_date    = SYSDATE,
                 request_id             = g_request_id
           WHERE batch_id = g_new_batch_id
             AND process_flag = g_error;
        ELSIF g_entity = g_account THEN
          UPDATE xxap_supplier_bankaccnts_stg
             SET process_flag           = g_new,
                 run_sequence_id        = g_run_seq_id,
                 last_updated_date      = SYSDATE,
                 last_updated_by        = g_user_id,
                 last_update_login      = g_login_id,
                 program_application_id = g_prog_appl_id,
                 program_id             = g_conc_program_id,
                 program_update_date    = SYSDATE,
                 request_id             = g_request_id
           WHERE batch_id = g_new_batch_id
             AND process_flag = g_error;
          --Manoj:START
          --if entity is SUPPLIER update supplier staging table
        ELSIF g_entity = g_supplier THEN
          UPDATE xxap_suppliers_stg
             SET process_flag           = g_new,
                 run_sequence_id        = g_run_seq_id,
                 last_updated_date      = SYSDATE,
                 last_updated_by        = g_user_id,
                 last_update_login      = g_login_id,
                 program_application_id = g_prog_appl_id,
                 program_id             = g_conc_program_id,
                 program_update_date    = SYSDATE,
                 request_id             = g_request_id
           WHERE batch_id = g_new_batch_id
             AND process_flag = g_error;
          xxetn_debug_pkg.add_debug('Updated supplier staging table where process record Error ' ||
                                    SQL%ROWCOUNT);
          --if entity is SUPPLIER_SITES update supplier sites staging table
        ELSIF g_entity = g_supplier_sites THEN
          UPDATE xxap_supplier_sites_stg
             SET process_flag           = g_new,
                 run_sequence_id        = g_run_seq_id,
                 last_update_date       = SYSDATE,
                 last_updated_by        = g_user_id,
                 last_update_login      = g_login_id,
                 program_application_id = g_prog_appl_id,
                 program_id             = g_conc_program_id,
                 program_update_date    = SYSDATE,
                 request_id             = g_request_id
           WHERE batch_id = g_new_batch_id
             AND process_flag = g_error;

          xxetn_debug_pkg.add_debug('Updated supplier sites staging table process record Error ' ||
                                    SQL%ROWCOUNT);
          --if entity is SUPPLIER_CONTACTS update supplier contacts staging table
        ELSIF g_entity = g_supplier_contacts THEN
          UPDATE xxap_supplier_contacts_stg
             SET process_flag           = g_new,
                 run_sequence_id        = g_run_seq_id,
                 last_updated_date      = SYSDATE,
                 last_updated_by        = g_user_id,
                 last_update_login      = g_login_id,
                 program_application_id = g_prog_appl_id,
                 program_id             = g_conc_program_id,
                 program_update_date    = SYSDATE,
                 request_id             = g_request_id
           WHERE batch_id = g_new_batch_id
             AND process_flag = g_error;
          xxetn_debug_pkg.add_debug('Updated supplier sites contacts table process record Error ' ||
                                    SQL%ROWCOUNT);
          --Manoj:END

          /** Added for v1.1 **/
          --if entity is Supplier Intermediary Accounts update Supplier Intermediary accounts staging table
        ELSIF g_entity = g_int_accts THEN
          UPDATE xxap_supplier_int_accts_stg
             SET process_flag           = g_new,
                 run_sequence_id        = g_run_seq_id,
                 last_update_date       = SYSDATE,
                 last_updated_by        = g_user_id,
                 last_update_login      = g_login_id,
                 program_application_id = g_prog_appl_id,
                 program_id             = g_conc_program_id,
                 program_update_date    = SYSDATE,
                 request_id             = g_request_id
           WHERE batch_id = g_new_batch_id
             AND process_flag = g_error;
          xxetn_debug_pkg.add_debug('Updated supplier intermediary accounts table process record Error ' ||
                                    SQL%ROWCOUNT);
        END IF;

      ELSIF g_process_records = 'UNPROCESSED' THEN
        --if entity is BANK update bank staging table
        IF g_entity = g_bank THEN
          UPDATE xxap_supplier_banks_stg
             SET run_sequence_id        = g_run_seq_id,
                 last_updated_date      = SYSDATE,
                 last_updated_by        = g_user_id,
                 last_update_login      = g_login_id,
                 program_application_id = g_prog_appl_id,
                 program_id             = g_conc_program_id,
                 program_update_date    = SYSDATE,
                 request_id             = g_request_id
           WHERE batch_id = g_new_batch_id
             AND process_flag = g_new;

          xxetn_debug_pkg.add_debug('Updated staging table where process record Unprocessed' ||
                                    SQL%ROWCOUNT);
        ELSIF g_entity = g_branch THEN
          UPDATE xxap_supplier_branches_stg
             SET run_sequence_id        = g_run_seq_id,
                 last_update_date       = SYSDATE,
                 last_updated_by        = g_user_id,
                 last_update_login      = g_login_id,
                 program_application_id = g_prog_appl_id,
                 program_id             = g_conc_program_id,
                 program_update_date    = SYSDATE,
                 request_id             = g_request_id
           WHERE batch_id = g_new_batch_id
             AND process_flag = g_new;
        ELSIF g_entity = g_account THEN
          UPDATE xxap_supplier_bankaccnts_stg
             SET run_sequence_id        = g_run_seq_id,
                 last_updated_date      = SYSDATE,
                 last_updated_by        = g_user_id,
                 last_update_login      = g_login_id,
                 program_application_id = g_prog_appl_id,
                 program_id             = g_conc_program_id,
                 program_update_date    = SYSDATE,
                 request_id             = g_request_id
           WHERE batch_id = g_new_batch_id
             AND process_flag = g_new;
          --Manoj:START
          --if entity is SUPPLIER update supplier staging table
        ELSIF g_entity = g_supplier THEN
          UPDATE xxap_suppliers_stg
             SET run_sequence_id        = g_run_seq_id,
                 last_updated_date      = SYSDATE,
                 last_updated_by        = g_user_id,
                 last_update_login      = g_login_id,
                 program_application_id = g_prog_appl_id,
                 program_id             = g_conc_program_id,
                 program_update_date    = SYSDATE,
                 request_id             = g_request_id
           WHERE batch_id = g_new_batch_id
             AND process_flag = g_new;
          xxetn_debug_pkg.add_debug('Updated supplier staging table where process record Unprocessed ' ||
                                    SQL%ROWCOUNT);
          --if entity is SUPPLIER_SITES update supplier sites staging table
        ELSIF g_entity = g_supplier_sites THEN
          UPDATE xxap_supplier_sites_stg
             SET run_sequence_id        = g_run_seq_id,
                 last_update_date       = SYSDATE,
                 last_updated_by        = g_user_id,
                 last_update_login      = g_login_id,
                 program_application_id = g_prog_appl_id,
                 program_id             = g_conc_program_id,
                 program_update_date    = SYSDATE,
                 request_id             = g_request_id
           WHERE batch_id = g_new_batch_id
             AND process_flag = g_new;
          xxetn_debug_pkg.add_debug('Updated supplier sites staging table process record Unprocessed ' ||
                                    SQL%ROWCOUNT);
          --if entity is SUPPLIER_CONTACTS update supplier contacts staging table
        ELSIF g_entity = g_supplier_contacts THEN
          UPDATE xxap_supplier_contacts_stg
             SET run_sequence_id        = g_run_seq_id,
                 last_updated_date      = SYSDATE,
                 last_updated_by        = g_user_id,
                 last_update_login      = g_login_id,
                 program_application_id = g_prog_appl_id,
                 program_id             = g_conc_program_id,
                 program_update_date    = SYSDATE,
                 request_id             = g_request_id
           WHERE batch_id = g_new_batch_id
             AND process_flag = g_new;
          xxetn_debug_pkg.add_debug('Updated supplier sites contacts table process record Unprocessed ' ||
                                    SQL%ROWCOUNT);
          --Manoj:END

          /** Added for v1.1 **/
          --if entity is Supplier Intermediary Accounts update Supplier Intermediary accounts staging table
        ELSIF g_entity = g_int_accts THEN
          UPDATE xxap_supplier_int_accts_stg
             SET run_sequence_id        = g_run_seq_id,
                 last_update_date       = SYSDATE,
                 last_updated_by        = g_user_id,
                 last_update_login      = g_login_id,
                 program_application_id = g_prog_appl_id,
                 program_id             = g_conc_program_id,
                 program_update_date    = SYSDATE,
                 request_id             = g_request_id
           WHERE batch_id = g_new_batch_id
             AND process_flag = g_new;
          xxetn_debug_pkg.add_debug('Updated supplier intermediary accounts table process record Unprocessed ' ||
                                    SQL%ROWCOUNT);
        END IF;

      END IF;

    END IF; -- g_batch_id

    COMMIT;

  EXCEPTION
    WHEN OTHERS THEN
      g_retcode := 2;
      l_err_msg := SUBSTR('Error While Assigning batch ID' || SQLERRM,
                          1,
                          2000);
      print_log_message('In When Other of Assign Batch Id procedure' ||
                        SQL%ROWCOUNT);

  END assign_batch_id;

  --
  -- ========================
  -- Procedure: mandatory_value_check_bank
  -- =============================================================================
  --   This procedure to do mandatory value check
  -- =============================================================================
  --  Input Parameters :
  --   piv_bank_name
  --   --piv_bank_number
  --   piv_country
  --   piv_bank_institution_type
  --   --piv_address1
  --   pon_error_cnt

  --  Output Parameters :
  --   pon_error_cnt    : Return Error Count
  -- -----------------------------------------------------------------------------
  --
  PROCEDURE mandatory_value_check_bank(piv_bank_name IN VARCHAR2
                                       --, piv_bank_number             IN      VARCHAR2
                                      ,
                                       piv_country               IN VARCHAR2,
                                       piv_bank_institution_type IN VARCHAR2
                                       --, piv_address1                IN      VARCHAR2
                                      ,
                                       pon_error_cnt OUT NUMBER) IS
    l_record_cnt     NUMBER;
    l_err_msg        VARCHAR2(2000);
    l_log_ret_status VARCHAR2(50);
    l_log_err_msg    VARCHAR2(2000);
    l_err_code       VARCHAR2(40) := NULL;
  BEGIN
    print_log_message('   PROCEDURE : mandatory_value_check_bank');
    l_record_cnt     := 0;
    l_err_msg        := NULL;
    l_log_ret_status := NULL;
    l_log_err_msg    := NULL;
    l_err_code       := NULL;

    IF piv_bank_name IS NULL THEN
      xxetn_debug_pkg.add_debug('Mandatory Value missing on record.');
      l_err_code := 'ETN_AP_MANDATORY_NOT_ENTERED';
      l_err_msg  := 'Error: Mandatory Value missing on record.';

      log_errors(pov_return_status       => l_log_ret_status -- OUT
                ,
                 pov_error_msg           => l_log_err_msg -- OUT
                ,
                 piv_source_column_name  => 'LEG_BANK_NAME',
                 piv_source_column_value => piv_bank_name,
                 piv_error_type          => g_err_val,
                 piv_error_code          => l_err_code,
                 piv_error_message       => l_err_msg);
    END IF;

    --Mandatory Column check
    /**      IF  piv_bank_number IS NULL
          THEN
             l_record_cnt := 2;
             xxetn_debug_pkg.add_debug ( 'Mandatory Value missing on record.');
             l_err_code        := 'ETN_AP_MANDATORY_NOT_ENTERED';
             l_err_msg         := 'Error: Mandatory Value missing on record.';

             log_errors ( pov_return_status          =>   l_log_ret_status          -- OUT
                        , pov_error_msg              =>   l_log_err_msg             -- OUT
                        , piv_source_column_name     =>   'LEG_BANK_NUMBER'
                        , piv_source_column_value    =>   piv_bank_number
                        , piv_error_type             =>   g_err_val
                        , piv_error_code             =>   l_err_code
                        , piv_error_message          =>   l_err_msg
                        );
          END IF;
    **/

    --Mandatory Column check
    IF piv_country IS NULL THEN
      l_record_cnt := 2;
      xxetn_debug_pkg.add_debug('Mandatory Value missing on record.');
      l_err_code := 'ETN_AP_MANDATORY_NOT_ENTERED';
      l_err_msg  := 'Error: Mandatory Value missing on record.';

      log_errors(pov_return_status       => l_log_ret_status -- OUT
                ,
                 pov_error_msg           => l_log_err_msg -- OUT
                ,
                 piv_source_column_name  => 'LEG_COUNTRY',
                 piv_source_column_value => piv_country,
                 piv_error_type          => g_err_val,
                 piv_error_code          => l_err_code,
                 piv_error_message       => l_err_msg);
    END IF;

    --Mandatory Column check
    IF piv_bank_institution_type IS NULL THEN
      l_record_cnt := 2;
      xxetn_debug_pkg.add_debug('Mandatory Value missing on record.');
      l_err_code := 'ETN_AP_MANDATORY_NOT_ENTERED';
      l_err_msg  := 'Error: Mandatory Value missing on record.';

      log_errors(pov_return_status       => l_log_ret_status -- OUT
                ,
                 pov_error_msg           => l_log_err_msg -- OUT
                ,
                 piv_source_column_name  => 'LEG_BANK_INSTITUTION_TYPE',
                 piv_source_column_value => piv_bank_institution_type,
                 piv_error_type          => g_err_val,
                 piv_error_code          => l_err_code,
                 piv_error_message       => l_err_msg);
    END IF;

    --Mandatory Column check
    /**      IF  piv_address1 IS NULL
          THEN
             l_record_cnt := 2;
             xxetn_debug_pkg.add_debug ( 'Mandatory Value missing on record.');
             l_err_code        := 'ETN_AP_MANDATORY_NOT_ENTERED';
             l_err_msg         := 'Error: Mandatory Value missing on record.';

             log_errors ( pov_return_status          =>   l_log_ret_status          -- OUT
                        , pov_error_msg              =>   l_log_err_msg             -- OUT
                        , piv_source_column_name     =>   'LEG_ADDRESS1'
                        , piv_source_column_value    =>   piv_address1
                        , piv_error_type             =>   g_err_val
                        , piv_error_code             =>   l_err_code
                        , piv_error_message          =>   l_err_msg
                        );
          END IF;
    **/

    IF l_record_cnt > 1 THEN
      pon_error_cnt := 2;
    END IF;
  EXCEPTION
    WHEN OTHERS THEN
      g_retcode     := 2;
      pon_error_cnt := 2;
      print_log_message('In Exception mandatory_value_check_bank check' ||
                        SQLERRM);
  END mandatory_value_check_bank;

  --
  -- ========================
  -- Procedure: mandatory_value_check_branch
  -- =============================================================================
  --   This procedure to do mandatory value check
  -- =============================================================================
  --  Input Parameters :
  --   piv_branch_name
  --   piv_bank_name
  --   piv_branch_number
  --   piv_country
  --   piv_branch_type
  --   pon_error_cnt

  --  Output Parameters :
  --   pon_error_cnt    : Return Error Count
  -- -----------------------------------------------------------------------------
  --
  PROCEDURE mandatory_value_check_branch(piv_branch_name IN VARCHAR2,
                                         piv_bank_name   IN VARCHAR2
                                         --, piv_branch_number           IN      VARCHAR2
                                        ,
                                         piv_country IN VARCHAR2
                                         --, piv_branch_type             IN      VARCHAR2
                                        ,
                                         pon_error_cnt OUT NUMBER) IS
    l_record_cnt     NUMBER;
    l_err_msg        VARCHAR2(2000);
    l_log_ret_status VARCHAR2(50);
    l_log_err_msg    VARCHAR2(2000);
    l_err_code       VARCHAR2(40) := NULL;
  BEGIN
    print_log_message('   PROCEDURE : mandatory_value_check_branch');
    l_record_cnt     := 0;
    l_err_msg        := NULL;
    l_log_ret_status := NULL;
    l_log_err_msg    := NULL;
    l_err_code       := NULL;

    IF piv_branch_name IS NULL THEN
      xxetn_debug_pkg.add_debug('Mandatory Value missing on record.');
      l_err_code := 'ETN_AP_MANDATORY_NOT_ENTERED';
      l_err_msg  := 'Error: Mandatory Value missing on record.';

      log_errors(pov_return_status       => l_log_ret_status -- OUT
                ,
                 pov_error_msg           => l_log_err_msg -- OUT
                ,
                 piv_source_column_name  => 'LEG_BANK_BRANCH_NAME',
                 piv_source_column_value => piv_branch_name,
                 piv_error_type          => g_err_val,
                 piv_error_code          => l_err_code,
                 piv_error_message       => l_err_msg);
    END IF;

    IF piv_bank_name IS NULL THEN
      xxetn_debug_pkg.add_debug('Mandatory Value missing on record.');
      l_err_code := 'ETN_AP_MANDATORY_NOT_ENTERED';
      l_err_msg  := 'Error: Mandatory Value missing on record.';

      log_errors(pov_return_status       => l_log_ret_status -- OUT
                ,
                 pov_error_msg           => l_log_err_msg -- OUT
                ,
                 piv_source_column_name  => 'LEG_BANK_NAME',
                 piv_source_column_value => piv_bank_name,
                 piv_error_type          => g_err_val,
                 piv_error_code          => l_err_code,
                 piv_error_message       => l_err_msg);
    END IF;

    --Mandatory Column check
    /**      IF  piv_branch_number IS NULL
          THEN
             l_record_cnt := 2;
             xxetn_debug_pkg.add_debug ( 'Mandatory Value missing on record.');
             l_err_code        := 'ETN_AP_MANDATORY_NOT_ENTERED';
             l_err_msg         := 'Error: Mandatory Value missing on record.';

             log_errors ( pov_return_status          =>   l_log_ret_status          -- OUT
                        , pov_error_msg              =>   l_log_err_msg             -- OUT
                        , piv_source_column_name     =>   'BRANCH_NUMBER'
                        , piv_source_column_value    =>   piv_branch_number
                        , piv_error_type             =>   g_err_val
                        , piv_error_code             =>   l_err_code
                        , piv_error_message          =>   l_err_msg
                        );
          END IF;
    **/

    --Mandatory Column check
    IF piv_country IS NULL THEN
      l_record_cnt := 2;
      xxetn_debug_pkg.add_debug('Mandatory Value missing on record.');
      l_err_code := 'ETN_AP_MANDATORY_NOT_ENTERED';
      l_err_msg  := 'Error: Mandatory Value missing on record.';

      log_errors(pov_return_status       => l_log_ret_status -- OUT
                ,
                 pov_error_msg           => l_log_err_msg -- OUT
                ,
                 piv_source_column_name  => 'LEG_COUNTRY',
                 piv_source_column_value => piv_country,
                 piv_error_type          => g_err_val,
                 piv_error_code          => l_err_code,
                 piv_error_message       => l_err_msg);
    END IF;

    --Mandatory Column check
    /**      IF  piv_branch_type IS NULL
          THEN
             l_record_cnt := 2;
             xxetn_debug_pkg.add_debug ( 'Mandatory Value missing on record.');
             l_err_code        := 'ETN_AP_MANDATORY_NOT_ENTERED';
             l_err_msg         := 'Error: Mandatory Value missing on record.';

             log_errors ( pov_return_status          =>   l_log_ret_status          -- OUT
                        , pov_error_msg              =>   l_log_err_msg             -- OUT
                        , piv_source_column_name     =>   'LEG_BANK_BRANCH_TYPE'
                        , piv_source_column_value    =>   piv_branch_type
                        , piv_error_type             =>   g_err_val
                        , piv_error_code             =>   l_err_code
                        , piv_error_message          =>   l_err_msg
                        );
          END IF;
    **/

    IF l_record_cnt > 1 THEN
      pon_error_cnt := 2;
    END IF;
  EXCEPTION
    WHEN OTHERS THEN
      g_retcode     := 2;
      pon_error_cnt := 2;
      print_log_message('In Exception mandatory_value_check_branch check' ||
                        SQLERRM);
  END mandatory_value_check_branch;

  --
  -- ========================
  -- Procedure: mandatory_value_check_account
  -- =============================================================================
  --   This procedure to do mandatory value check
  -- =============================================================================
  --  Input Parameters :
  --   leg_bank_name
  --   leg_branch_name
  --   leg_vendor_name
  --   leg_country
  --   leg_account_name
  --   leg_account_num
  --   leg_operating_unit
  --   leg_account_type
  --   Output Parameters :
  --   pon_error_cnt    : Return Error Count
  -- -----------------------------------------------------------------------------
  --
  PROCEDURE mandatory_value_check_account(piv_bank_name   IN VARCHAR2,
                                          piv_branch_name IN VARCHAR2,
                                          piv_vendor_name IN VARCHAR2
                                          --, piv_country               IN      VARCHAR2
                                         ,
                                          piv_account_name IN VARCHAR2,
                                          piv_account_num  IN VARCHAR2
                                          --, piv_operating_unit        IN      VARCHAR2
                                         ,
                                          piv_account_type IN VARCHAR2,
                                          pon_error_cnt    OUT NUMBER) IS
    l_record_cnt     NUMBER;
    l_err_msg        VARCHAR2(2000);
    l_log_ret_status VARCHAR2(50);
    l_log_err_msg    VARCHAR2(2000);
    l_err_code       VARCHAR2(40) := NULL;
  BEGIN
    print_log_message('   PROCEDURE : mandatory_value_check_account');
    l_record_cnt     := 0;
    l_err_msg        := NULL;
    l_log_ret_status := NULL;
    l_log_err_msg    := NULL;
    l_err_code       := NULL;

    IF piv_bank_name IS NULL THEN
      xxetn_debug_pkg.add_debug('Mandatory Value missing on record.');
      l_err_code := 'ETN_AP_MANDATORY_NOT_ENTERED';
      l_err_msg  := 'Error: Mandatory Value missing on record.';

      log_errors(pov_return_status       => l_log_ret_status -- OUT
                ,
                 pov_error_msg           => l_log_err_msg -- OUT
                ,
                 piv_source_column_name  => 'LEG_BANK_NAME',
                 piv_source_column_value => piv_bank_name,
                 piv_error_type          => g_err_val,
                 piv_error_code          => l_err_code,
                 piv_error_message       => l_err_msg);
    END IF;

    IF piv_branch_name IS NULL THEN
      xxetn_debug_pkg.add_debug('Mandatory Value missing on record.');
      l_err_code := 'ETN_AP_MANDATORY_NOT_ENTERED';
      l_err_msg  := 'Error: Mandatory Value missing on record.';

      log_errors(pov_return_status       => l_log_ret_status -- OUT
                ,
                 pov_error_msg           => l_log_err_msg -- OUT
                ,
                 piv_source_column_name  => 'LEG_BRANCH_NAME',
                 piv_source_column_value => piv_branch_name,
                 piv_error_type          => g_err_val,
                 piv_error_code          => l_err_code,
                 piv_error_message       => l_err_msg);
    END IF;

    --Mandatory Column check
    IF piv_vendor_name IS NULL THEN
      l_record_cnt := 2;
      xxetn_debug_pkg.add_debug('Mandatory Value missing on record.');
      l_err_code := 'ETN_AP_MANDATORY_NOT_ENTERED';
      l_err_msg  := 'Error: Mandatory Value missing on record.';

      log_errors(pov_return_status       => l_log_ret_status -- OUT
                ,
                 pov_error_msg           => l_log_err_msg -- OUT
                ,
                 piv_source_column_name  => 'LEG_VENDOR_NAME',
                 piv_source_column_value => piv_vendor_name,
                 piv_error_type          => g_err_val,
                 piv_error_code          => l_err_code,
                 piv_error_message       => l_err_msg);
    END IF;

    /**
          --Mandatory Column check
          IF  piv_country IS NULL
          THEN
             l_record_cnt := 2;
             xxetn_debug_pkg.add_debug ( 'Mandatory Value missing on record.');
             l_err_code        := 'ETN_AP_MANDATORY_NOT_ENTERED';
             l_err_msg         := 'Error: Mandatory Value missing on record.';

             log_errors ( pov_return_status          =>   l_log_ret_status          -- OUT
                        , pov_error_msg              =>   l_log_err_msg             -- OUT
                        , piv_source_column_name     =>   'LEG_COUNTRY'
                        , piv_source_column_value    =>   piv_country
                        , piv_error_type             =>   g_err_val
                        , piv_error_code             =>   l_err_code
                        , piv_error_message          =>   l_err_msg
                        );
          END IF;
    **/

    --Mandatory Column check
    IF piv_account_name IS NULL THEN
      l_record_cnt := 2;
      xxetn_debug_pkg.add_debug('Mandatory Value missing on record.');
      l_err_code := 'ETN_AP_MANDATORY_NOT_ENTERED';
      l_err_msg  := 'Error: Mandatory Value missing on record.';

      log_errors(pov_return_status       => l_log_ret_status -- OUT
                ,
                 pov_error_msg           => l_log_err_msg -- OUT
                ,
                 piv_source_column_name  => 'LEG_ACCOUNT_NAME',
                 piv_source_column_value => piv_account_name,
                 piv_error_type          => g_err_val,
                 piv_error_code          => l_err_code,
                 piv_error_message       => l_err_msg);
    END IF;

    --Mandatory Column check
    IF piv_account_name IS NULL THEN
      l_record_cnt := 2;
      xxetn_debug_pkg.add_debug('Mandatory Value missing on record.');
      l_err_code := 'ETN_AP_MANDATORY_NOT_ENTERED';
      l_err_msg  := 'Error: Mandatory Value missing on record.';

      log_errors(pov_return_status       => l_log_ret_status -- OUT
                ,
                 pov_error_msg           => l_log_err_msg -- OUT
                ,
                 piv_source_column_name  => 'LEG_ACCOUNT_NAME',
                 piv_source_column_value => piv_account_name,
                 piv_error_type          => g_err_val,
                 piv_error_code          => l_err_code,
                 piv_error_message       => l_err_msg);
    END IF;

    --Mandatory Column check
    IF piv_account_num IS NULL THEN
      l_record_cnt := 2;
      xxetn_debug_pkg.add_debug('Mandatory Value missing on record.');
      l_err_code := 'ETN_AP_MANDATORY_NOT_ENTERED';
      l_err_msg  := 'Error: Mandatory Value missing on record.';

      log_errors(pov_return_status       => l_log_ret_status -- OUT
                ,
                 pov_error_msg           => l_log_err_msg -- OUT
                ,
                 piv_source_column_name  => 'LEG_ACCOUNT_NUM',
                 piv_source_column_value => piv_account_num,
                 piv_error_type          => g_err_val,
                 piv_error_code          => l_err_code,
                 piv_error_message       => l_err_msg);
    END IF;

    /**
          --Mandatory Column check
          IF  piv_operating_unit IS NULL
          THEN
             l_record_cnt := 2;
             xxetn_debug_pkg.add_debug ( 'Mandatory Value missing on record.');
             l_err_code        := 'ETN_AP_MANDATORY_NOT_ENTERED';
             l_err_msg         := 'Error: Mandatory Value missing on record.';

             log_errors ( pov_return_status          =>   l_log_ret_status          -- OUT
                        , pov_error_msg              =>   l_log_err_msg             -- OUT
                        , piv_source_column_name     =>   'LEG_OPERATING_UNIT'
                        , piv_source_column_value    =>   piv_operating_unit
                        , piv_error_type             =>   g_err_val
                        , piv_error_code             =>   l_err_code
                        , piv_error_message          =>   l_err_msg
                        );
          END IF;
    **/

    --Mandatory Column check
    IF piv_account_type IS NULL THEN
      l_record_cnt := 2;
      xxetn_debug_pkg.add_debug('Mandatory Value missing on record.');
      l_err_code := 'ETN_AP_MANDATORY_NOT_ENTERED';
      l_err_msg  := 'Error: Mandatory Value missing on record.';

      log_errors(pov_return_status       => l_log_ret_status -- OUT
                ,
                 pov_error_msg           => l_log_err_msg -- OUT
                ,
                 piv_source_column_name  => 'LEG_ACCOUNT_TYPE',
                 piv_source_column_value => piv_account_type,
                 piv_error_type          => g_err_val,
                 piv_error_code          => l_err_code,
                 piv_error_message       => l_err_msg);
    END IF;

    IF l_record_cnt > 1 THEN
      pon_error_cnt := 2;
    END IF;
  EXCEPTION
    WHEN OTHERS THEN
      g_retcode     := 2;
      pon_error_cnt := 2;
      print_log_message('In Exception mandatory_value_check_account check' ||
                        SQLERRM);
  END mandatory_value_check_account;

  --
  -- ========================
  -- Procedure: duplicate_bank
  -- =============================================================================
  --   This procedure to do duplicate bank record check
  -- =============================================================================
  --  Input Parameters :
  --   piv_bank_name
  --   piv_bank_number
  --   piv_country

  --  Output Parameters :
  --   pon_error_cnt    : Return Error Count
  -- -----------------------------------------------------------------------------
  --
  PROCEDURE duplicate_bank(piv_bank_name   IN VARCHAR2,
                           piv_bank_number IN VARCHAR2,
                           piv_country     IN VARCHAR2,
                           pon_error_cnt   OUT NUMBER) IS
    l_record_cnt     NUMBER;
    l_err_msg        VARCHAR2(2000);
    l_log_ret_status VARCHAR2(50);
    l_log_err_msg    VARCHAR2(2000);
    l_err_code       VARCHAR2(40) := NULL;
  BEGIN
    print_log_message(' + PROCEDURE : duplicate_bank +');
    l_record_cnt     := 0;
    l_err_msg        := NULL;
    l_log_ret_status := NULL;
    l_log_err_msg    := NULL;
    l_err_code       := NULL;

    --check if the duplicate bank already exists
    BEGIN
      SELECT COUNT(1)
        INTO l_record_cnt
        FROM xxap_supplier_banks_stg xsbs
       WHERE UPPER(xsbs.leg_bank_name) = UPPER(piv_bank_name)  --v3.0
         AND xsbs.leg_country = piv_country
         AND NVL(xsbs.leg_bank_number, 'KKKKKK') =
             NVL(piv_bank_number, 'KKKKKK')
         AND xsbs.request_id = g_request_id
         AND xsbs.process_flag <> 'D';

    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        print_log_message('In No Data found of duplicate bank check' ||
                          SQLERRM);
      WHEN OTHERS THEN
        l_record_cnt := 2;
        print_log_message('In When others of duplicate bank check' ||
                          SQLERRM);
    END;
    IF (l_record_cnt > 1) THEN

      l_record_cnt := 2;
    END IF;

    IF l_record_cnt = 2 THEN
      pon_error_cnt := 2;
    END IF;
    print_log_message(' - PROCEDURE : duplicate_bank -');
  EXCEPTION
    WHEN OTHERS THEN
      g_retcode     := 2;
      pon_error_cnt := 2;
      print_log_message('In Exception duplicate_bank check' || SQLERRM);
  END duplicate_bank;

  --
  -- ========================
  -- Procedure: duplicate_branch
  -- =============================================================================
  --   This procedure to do duplicate branch record check
  -- =============================================================================
  --  Input Parameters :
  --   piv_bank_name
  --   piv_bank_number
  --   piv_country

  --  Output Parameters :
  --   pon_error_cnt    : Return Error Count
  -- -----------------------------------------------------------------------------
  --
  PROCEDURE duplicate_branch(piv_bank_name   IN VARCHAR2,
                             piv_branch_name IN VARCHAR2,
                             piv_country     IN VARCHAR2,
                             pon_error_cnt   OUT NUMBER) IS
    l_record_cnt     NUMBER;
    l_err_msg        VARCHAR2(2000);
    l_log_ret_status VARCHAR2(50);
    l_log_err_msg    VARCHAR2(2000);
    l_err_code       VARCHAR2(40) := NULL;
  BEGIN
    print_log_message(' + PROCEDURE : duplicate_branch +');
    l_record_cnt     := 0;
    l_err_msg        := NULL;
    l_log_ret_status := NULL;
    l_log_err_msg    := NULL;
    l_err_code       := NULL;

    --check if the duplicate bank already exists
    BEGIN
      SELECT COUNT(1)
        INTO l_record_cnt
        FROM xxap_supplier_branches_stg xsbs
       WHERE xsbs.leg_bank_name = piv_bank_name
         AND xsbs.leg_country = piv_country
         AND xsbs.leg_bank_branch_name = piv_branch_name
         AND xsbs.request_id = g_request_id
         AND xsbs.process_flag <> 'D';
      --AND xsbs.batch_id        = g_new_batch_id
      --AND xsbs.run_sequence_id = g_run_seq_id;

    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        print_log_message('In No Data found of duplicate branch check' ||
                          SQLERRM);
      WHEN OTHERS THEN
        l_record_cnt := 2;
        print_log_message('In When others of duplicate branch check' ||
                          SQLERRM);
    END;

    IF (l_record_cnt > 1) THEN

      l_record_cnt := 2;
    END IF;

    IF l_record_cnt = 2 THEN
      pon_error_cnt := 2;
    END IF;
    print_log_message(' - PROCEDURE : duplicate_branch -');
  EXCEPTION
    WHEN OTHERS THEN
      g_retcode     := 2;
      pon_error_cnt := 2;
      print_log_message('In Exception duplicate_branch check' || SQLERRM);
  END duplicate_branch;

  --
  -- ========================
  -- Procedure: duplicate_account
  -- =============================================================================
  --   This procedure to do duplicate account record check
  -- =============================================================================
  --  Input Parameters :
  --   piv_bank_name
  --   piv_branch_name
  --   piv_account_name
  --   piv_operating_unit
  --   piv_country
  --   piv_vendor_num
  --   piv_vendor_site_code
  --   piv_account_num

  --  Output Parameters :
  --   pon_error_cnt    : Return Error Count
  -- -----------------------------------------------------------------------------
  --
  PROCEDURE duplicate_account(piv_bank_name        IN VARCHAR2,
                              piv_branch_name      IN VARCHAR2,
                              piv_account_name     IN VARCHAR2,
                              piv_operating_unit   IN VARCHAR2,
                              piv_country          IN VARCHAR2,
                              piv_vendor_num       IN VARCHAR2,
                              piv_vendor_site_code IN VARCHAR2,
                              piv_account_num      IN VARCHAR2,
                              pon_error_cnt        OUT NUMBER) IS
    l_record_cnt     NUMBER;
    l_err_msg        VARCHAR2(2000);
    l_log_ret_status VARCHAR2(50);
    l_log_err_msg    VARCHAR2(2000);
    l_err_code       VARCHAR2(40) := NULL;
  BEGIN
    print_log_message(' + PROCEDURE : duplicate_account +');
    l_record_cnt     := 0;
    l_err_msg        := NULL;
    l_log_ret_status := NULL;
    l_log_err_msg    := NULL;
    l_err_code       := NULL;

    --check if the duplicate account already exists
    BEGIN
      SELECT COUNT(1)
        INTO l_record_cnt
        FROM xxap_supplier_bankaccnts_stg xsbs
       WHERE xsbs.leg_bank_name = piv_bank_name
         AND NVL(xsbs.leg_country, '99') = NVL(piv_country, '99')
         AND xsbs.leg_branch_name = piv_branch_name
         AND xsbs.leg_account_name = piv_account_name
         AND NVL(xsbs.leg_operating_unit_name, '99999') =
             NVL(piv_operating_unit, '99999')
         AND xsbs.leg_vendor_num = piv_vendor_num
         AND xsbs.leg_vendor_site_code = piv_vendor_site_code
         and xsbs.leg_account_num = piv_account_num
         AND xsbs.request_id = g_request_id
         AND xsbs.process_flag <> 'D';

    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        print_log_message('In No Data found of duplicate account check' ||
                          SQLERRM);
      WHEN OTHERS THEN
        l_record_cnt := 2;
        print_log_message('In When others of duplicate account check' ||
                          SQLERRM);
    END;
    IF (l_record_cnt > 1) THEN
      l_record_cnt := 2;
    END IF;

    IF l_record_cnt = 2 THEN
      pon_error_cnt := 2;
    END IF;
    print_log_message(' - PROCEDURE : duplicate_account -');
  EXCEPTION
    WHEN OTHERS THEN
      g_retcode     := 2;
      pon_error_cnt := 2;
      print_log_message('In Exception duplicate_account check' || SQLERRM);
  END duplicate_account;

  --
  -- ========================
  -- Procedure: validate_country
  -- =============================================================================
  --   This procedure validate_country
  -- =============================================================================
  --  Input Parameters :
  --  piv_country  : Legacy Country Name

  --  Output Parameters :
  --  pon_error_cnt    : Return Error Count
  -- -----------------------------------------------------------------------------
  --
  PROCEDURE validate_country(piv_country   IN VARCHAR2,
                             pon_error_cnt OUT NUMBER) IS
    l_record_cnt NUMBER;
    l_status     VARCHAR2(1);

  BEGIN

    xxetn_debug_pkg.add_debug(' +  PROCEDURE : validate_country  ' ||
                              piv_country || ' + ');

    l_record_cnt := 0;

    BEGIN
      --check if the Country is valid
      SELECT 1
        INTO l_status
        FROM fnd_territories
       WHERE UPPER(territory_code) = UPPER(piv_country)
         AND obsolete_flag = 'N';

    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        l_record_cnt := 2;
        print_log_message('In No Data found of country check' || SQLERRM);
      WHEN OTHERS THEN
        l_record_cnt := 2;
        print_log_message('In When others of country check' || SQLERRM);
    END;
    IF (l_record_cnt < 0) THEN
      l_record_cnt := 2;
    END IF;

    IF l_record_cnt = 2 THEN
      pon_error_cnt := 2;
    END IF;

    print_log_message(' -  PROCEDURE : validate_country  ' || piv_country ||
                      ' - ');
  EXCEPTION
    WHEN OTHERS THEN
      g_retcode    := 2;
      l_record_cnt := 2;
      g_errbuff    := 'Failed while validating Country.';
      print_log_message('In Exception validate country' || SQLERRM);
  END validate_country;

  --
  -- ========================
  -- Procedure: validate_institution_type
  -- =============================================================================
  --   This procedure validate_institution_type
  -- =============================================================================
  --  Input Parameters :
  --  piv_country  : Legacy Institution Type

  --  Output Parameters :
  --  pon_error_cnt    : Return Error Count
  -- -----------------------------------------------------------------------------
  --
  PROCEDURE validate_institution_type(piv_institution_type IN VARCHAR2,
                                      pon_error_cnt        OUT NUMBER) IS
    l_record_cnt NUMBER;

  BEGIN

    xxetn_debug_pkg.add_debug(' +  PROCEDURE : validate_institution_type  ' ||
                              piv_institution_type || ' + ');

    l_record_cnt := 0;

    BEGIN
      --check if the institution type is valid
      SELECT COUNT(1)
        INTO l_record_cnt
        FROM ap_lookup_codes
       WHERE lookup_type = 'INSTITUTION TYPE'
         AND lookup_code = piv_institution_type;

    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        l_record_cnt := 2;
        print_log_message('In No Data found of institution type check' ||
                          SQLERRM);
      WHEN OTHERS THEN
        l_record_cnt := 2;
        print_log_message('In When others of institution type check' ||
                          SQLERRM);
    END;
    IF (l_record_cnt < 0) THEN
      l_record_cnt := 2;
    END IF;

    IF l_record_cnt = 2 THEN
      pon_error_cnt := 2;
    END IF;

    print_log_message(' -  PROCEDURE : validate_institution_type  ' ||
                      ' - ');
  EXCEPTION
    WHEN OTHERS THEN
      g_retcode    := 2;
      l_record_cnt := 2;
      g_errbuff    := 'Failed while validating bank institution type.';
      print_log_message('In Exception validate bank institution type' ||
                        SQLERRM);
  END validate_institution_type;

  --
  -- ========================
  -- Procedure: validate_branch_type
  -- =============================================================================
  --   This procedure validate_branch_type
  -- =============================================================================
  --  Input Parameters :
  --  piv_branch_type  : Legacy bank branch Type

  --  Output Parameters :
  --  pon_error_cnt    : Return Error Count
  -- -----------------------------------------------------------------------------
  --
  PROCEDURE validate_branch_type(piv_branch_type IN VARCHAR2,
                                 pon_error_cnt   OUT NUMBER) IS
    l_record_cnt NUMBER;

  BEGIN

    xxetn_debug_pkg.add_debug(' +  PROCEDURE : validate_branch_type  ' ||
                              piv_branch_type || ' + ');

    l_record_cnt := 0;

    BEGIN
      --check if the branch type is valid
      SELECT COUNT(1)
        INTO l_record_cnt
        FROM ap_lookup_codes
       WHERE lookup_type = 'BANK BRANCH TYPE'
         AND lookup_code = piv_branch_type;

    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        l_record_cnt := 2;
        print_log_message('In No Data found of branch type check' ||
                          SQLERRM);
      WHEN OTHERS THEN
        l_record_cnt := 2;
        print_log_message('In When others of branch type check' || SQLERRM);
    END;
    IF (l_record_cnt < 0) THEN
      l_record_cnt := 2;
    END IF;

    IF l_record_cnt = 2 THEN
      pon_error_cnt := 2;
    END IF;

    print_log_message(' -  PROCEDURE : validate_branch_type  - ');
  EXCEPTION
    WHEN OTHERS THEN
      g_retcode    := 2;
      l_record_cnt := 2;
      g_errbuff    := 'Failed while validating bank branch type.';
      print_log_message('In Exception validate bank branch type' ||
                        SQLERRM);
  END validate_branch_type;

  --
  -- ========================
  -- Procedure: validate_bank_exists
  -- =============================================================================
  --   This procedure validate_bank_exists
  -- =============================================================================
  --  Input Parameters :
  --  piv_bank_name: Leg bank name
  --  piv_country  : leg country

  --  Output Parameters :
  --  pon_error_cnt    : Return Error Count
  -- -----------------------------------------------------------------------------
  --
  PROCEDURE validate_bank_exists(piv_bank_name IN VARCHAR2,
                                 piv_bank_num  IN VARCHAR2,
                                 piv_country   IN VARCHAR2,
                                 pon_error_cnt OUT NUMBER) IS
    l_record_cnt NUMBER;

  BEGIN

    xxetn_debug_pkg.add_debug(' +  PROCEDURE : validate_bank_exists  ' ||
                              piv_bank_name || 'in country ' ||
                              piv_country || ' + ');

    l_record_cnt := 0;

    BEGIN
      --check if the bank already exists
      SELECT COUNT(1)
        INTO l_record_cnt
        FROM ce_banks_v
       WHERE UPPER(bank_name) = UPPER(piv_bank_name)
         AND UPPER(NVL(bank_number, 'KKKKKK')) =
             UPPER(NVL(piv_bank_num, 'KKKKKK'))
         AND home_country = piv_country;

    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        print_log_message('In No Data found of bank already exists check' ||
                          SQLERRM);
      WHEN OTHERS THEN
        l_record_cnt := 2;
        print_log_message('In When others of bank already exists check' ||
                          SQLERRM);
    END;
    IF (l_record_cnt > 0) THEN
      l_record_cnt := 2;
    END IF;

    IF l_record_cnt = 2 THEN
      pon_error_cnt := 2;
    END IF;

    print_log_message(' -  PROCEDURE : validate_bank_exists  ' ||
                      piv_country || ' - ');
  EXCEPTION
    WHEN OTHERS THEN
      g_retcode    := 2;
      l_record_cnt := 2;
      g_errbuff    := 'Failed while validating whether bank already exists.';
      print_log_message('In Exception validate bank exists' || SQLERRM);
  END validate_bank_exists;

  --
  -- ========================
  -- Procedure: validate_br_bank_exists
  -- =============================================================================
  --   This procedure validate_br_bank_exists
  -- =============================================================================
  --  Input Parameters :
  --  piv_bank_name: Leg bank name
  --  piv_country  : leg country

  --  Output Parameters :
  --  pon_error_cnt    : Return Error Count
  --  pon_bank_id      : Return Bank ID
  -- -----------------------------------------------------------------------------
  --
  PROCEDURE validate_br_bank_exists(piv_bank_name IN VARCHAR2,
                                    piv_bank_num  IN VARCHAR2,
                                    piv_country   IN VARCHAR2,
                                    pon_bank_id   OUT NUMBER,
                                    pon_error_cnt OUT NUMBER) IS
    l_record_cnt NUMBER;
    l_bank_id    NUMBER;

  BEGIN

    xxetn_debug_pkg.add_debug(' +  PROCEDURE : validate_br_bank_exists  ' ||
                              piv_bank_name || 'in country ' ||
                              piv_country || ' + ');

    l_record_cnt := 0;
    l_bank_id    := NULL;

    BEGIN
      --check if the bank for a branch exists and derive party ID
      SELECT bank_party_id
        INTO l_bank_id
        FROM iby_ext_banks_v
       WHERE UPPER(bank_name) = UPPER(piv_bank_name)
         AND UPPER(NVL(bank_number, 'KKKKKKK')) =
             UPPER(NVL(piv_bank_num, 'KKKKKKK'))
         AND home_country = piv_country;

    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        l_record_cnt := 2;
        print_log_message('In No Data found of branch bank exists check' ||
                          SQLERRM);
      WHEN OTHERS THEN
        l_record_cnt := 2;
        print_log_message('In When others of branch bank exists check' ||
                          SQLERRM);
    END;
    IF (l_bank_id IS NULL) THEN
      l_record_cnt := 2;
    ELSE
      pon_bank_id := l_bank_id;
    END IF;

    IF l_record_cnt = 2 THEN
      pon_error_cnt := 2;
    END IF;

    print_log_message(' -  PROCEDURE : validate_br_bank_exists  ' ||
                      piv_country || ' - ');
  EXCEPTION
    WHEN OTHERS THEN
      g_retcode    := 2;
      l_record_cnt := 2;
      g_errbuff    := 'Failed while validating whether bank already exists.';
      print_log_message('In Exception validate branch bank exists' ||
                        SQLERRM);
  END validate_br_bank_exists;

  --
  -- ========================
  -- Procedure: validate_branch_exists
  -- =============================================================================
  --   This procedure validate_branch_exists
  -- =============================================================================
  --  Input Parameters :
  --  piv_bank_name: Leg bank name
  --  piv_country  : leg country

  --  Output Parameters :
  --  pon_error_cnt    : Return Error Count
  -- -----------------------------------------------------------------------------
  --
  PROCEDURE validate_branch_exists(piv_bank_name   IN VARCHAR2,
                                   piv_branch_name IN VARCHAR2,
                                   piv_country     IN VARCHAR2,
                                   --ABD changes 09/27/2016 start here
                                   piv_bank_num    IN VARCHAR2,
                                   piv_branch_num  IN VARCHAR2,
                                   --ABD changes 09/27/2016 end here
                                   pon_error_cnt   OUT NUMBER) IS
    l_record_cnt NUMBER;

  BEGIN

    xxetn_debug_pkg.add_debug(' +  PROCEDURE : validate_branch_exists  ' ||
                              piv_bank_name || 'in country ' ||
                              piv_country || ' + ');

    l_record_cnt := 0;

    BEGIN
      --check if the bank branch already exists
      SELECT count(1)
        INTO l_record_cnt
        FROM iby_ext_bank_branches_v iebb, ce_banks_v cbv
       WHERE iebb.bank_party_id = cbv.bank_party_id
         AND UPPER(cbv.bank_name) = UPPER(piv_bank_name)
         AND cbv.home_country = piv_country
         AND iebb.bank_branch_name = piv_branch_name
         --ABD changes 09/27/2016 start here
         AND nvl(cbv.bank_number,'XXXXX') = nvl(piv_bank_num,'XXXXX')
         AND nvl(iebb.Branch_Number,'XXXXX')= nvl(piv_branch_num,'XXXXX')
         --ABD changes 09/27/2016 end here
         ;

    EXCEPTION
      WHEN NO_DATA_FOUND THEN

        print_log_message('In No Data found of branch already exists check' ||
                          SQLERRM);
      WHEN OTHERS THEN
        pon_error_cnt := 2;
        print_log_message('In When others of branch already exists check' ||
                          SQLERRM);
    END;
    IF (l_record_cnt > 0) THEN
      pon_error_cnt := 2;
    END IF;

    print_log_message(' -  PROCEDURE : validate_branch_exists  ' ||
                      piv_country || ' - ');
  EXCEPTION
    WHEN OTHERS THEN
      g_retcode    := 2;
      l_record_cnt := 2;
      g_errbuff    := 'Failed while validating whether bank branch already exists.';
      print_log_message('In Exception validate bank branch exists' ||
                        SQLERRM);
  END validate_branch_exists;

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
  PROCEDURE validate_vendor(piv_vendor_num IN VARCHAR2,
                            pon_vendor_id  OUT NUMBER,
                            pon_error_cnt  OUT NUMBER) IS
    l_record_cnt NUMBER;
    l_vendor_id  NUMBER;

  BEGIN

    xxetn_debug_pkg.add_debug(' +  PROCEDURE : validate_vendor: ' ||
                              piv_vendor_num || ' + ');

    l_record_cnt := 0;

    BEGIN
      --validate vendor and derive vendor ID
      SELECT vendor_id
        INTO pon_vendor_id
        FROM ap_suppliers
       WHERE --vendor_name = p_in_vendor_name
       segment1 = piv_vendor_num
       AND enabled_flag = 'Y';

    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        l_record_cnt := 2;
        print_log_message('In No Data found of validate vendor check: ' ||
                          SQLERRM);
      WHEN OTHERS THEN
        l_record_cnt := 2;
        print_log_message('In When others of validate vendor check: ' ||
                          SQLERRM);
    END;

    IF l_record_cnt = 2 THEN
      pon_error_cnt := 2;
    END IF;

    print_log_message(' -  PROCEDURE : validate_vendor   - ');
  EXCEPTION
    WHEN OTHERS THEN
      g_retcode     := 2;
      pon_error_cnt := 2;
      g_errbuff     := 'Failed while validating vendor.';
      print_log_message('In Exception validate vendor: ' || SQLERRM);
  END validate_vendor;

  --
  -- ========================
  -- Procedure: validate_party_exists
  -- =============================================================================
  --   This procedure validate_party_exists
  -- =============================================================================
  --  Input Parameters :
  --  piv_vendor_num: Leg Vendor Number

  --  Output Parameters :
  --  pon_error_cnt    : Return Error Count
  --  pon_vendor _id
  -- -----------------------------------------------------------------------------
  --
  PROCEDURE validate_party_exists(piv_vendor_num IN VARCHAR2,
                                  pon_party_id   OUT NUMBER,
                                  pon_error_cnt  OUT NUMBER) IS
    l_record_cnt NUMBER;
    l_party_id   NUMBER;

  BEGIN

    xxetn_debug_pkg.add_debug(' +  PROCEDURE : validate_party_exists  ' ||
                              piv_vendor_num || ' + ');

    l_record_cnt := 0;

    BEGIN
      --validate vendor and derive party ID
      SELECT party_id
        INTO pon_party_id
        FROM ap_suppliers
       WHERE --vendor_name = p_in_vendor_name
       segment1 = piv_vendor_num
       AND enabled_flag = 'Y';

    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        l_record_cnt := 2;
        print_log_message('In No Data found of check party exists ' ||
                          SQLERRM);
      WHEN OTHERS THEN
        l_record_cnt := 2;
        print_log_message('In When others of check vendor party exists' ||
                          SQLERRM);
    END;

    IF l_record_cnt = 2 THEN
      pon_error_cnt := 2;
    END IF;

    print_log_message(' -  PROCEDURE : validate_party_exists   - ');
  EXCEPTION
    WHEN OTHERS THEN
      g_retcode     := 2;
      pon_error_cnt := 2;
      g_errbuff     := 'Failed while validate_party_exists.';
      print_log_message('In Exception vvalidate_party_exists' || SQLERRM);
  END validate_party_exists;

  --
  -- =============================================================================
  -- Procedure: validate_operating_unit
  -- =============================================================================
  --   This procedure validates operating_unit
  -- =============================================================================
  --  Input Parameters :
  --   piv_site - 11i Plant# or Site#

  --  Output Parameters :
  --  pov_operating_unit - R12 operating unit name
  --  pon_org_id         - R12 organization id
  --  pon_error_cnt      - Return Status
  -- -----------------------------------------------------------------------------
  --
  PROCEDURE validate_operating_unit(piv_site           IN VARCHAR2,
                                    pov_operating_unit OUT VARCHAR2,
                                    pon_org_id         OUT NUMBER,
                                    pon_error_cnt      OUT NUMBER) IS
    l_record_cnt NUMBER;
    l_oper_unit  xxetn_map_unit.operating_unit%TYPE := NULL;
    l_rec        xxetn_map_util.g_input_rec;

  BEGIN

    xxetn_debug_pkg.add_debug(' + PROCEDURE : validate_operating_unit = ' ||
                              piv_site || ' + ');

    l_record_cnt       := 0;
    pov_operating_unit := NULL;
    pon_org_id         := NULL;
    pon_error_cnt      := 0;

    -- Assigning 11i Site/Plant to API variable
    l_rec.site := piv_site;

    --R12 OU
    l_oper_unit := xxetn_map_util.get_value(l_rec).operating_unit;

    -- If R12 OU derivation failed
    IF l_oper_unit IS NULL THEN
      l_record_cnt := 2;
    ELSE
      pov_operating_unit := l_oper_unit;
    END IF;

    --if operating_unit is not null
    IF pov_operating_unit IS NOT NULL THEN

      BEGIN
        xxetn_debug_pkg.add_debug(' + PROCEDURE : validate_operating_unit...derivation of org_id + ');

        --Fetch org_id for the R12 value of the operating unit derived
        SELECT hou.organization_id
          INTO pon_org_id
          FROM hr_operating_units hou
         WHERE hou.name = pov_operating_unit
           AND TRUNC(SYSDATE) BETWEEN NVL(hou.DATE_FROM, TRUNC(SYSDATE)) AND
               NVL(hou.DATE_TO, TRUNC(SYSDATE));

        xxetn_debug_pkg.add_debug(' + PROCEDURE : validate_operating_unit...derivation of org_id + ');
      EXCEPTION
        WHEN NO_DATA_FOUND THEN
          l_record_cnt := 2;
          pon_org_id   := NULL;
          print_log_message('In No Data found of operating unit check');
        WHEN OTHERS THEN
          l_record_cnt := 2;
          pon_org_id   := NULL;
          print_log_message('In When others of operating unit check' ||
                            SQLERRM);
      END;
    END IF;

    xxetn_debug_pkg.add_debug('Operating Unit = ' || pov_operating_unit);

    xxetn_debug_pkg.add_debug('Org Id = ' || pon_org_id);

    IF l_record_cnt > 1 THEN
      pon_error_cnt := 2;
    END IF;
    xxetn_debug_pkg.add_debug(' - PROCEDURE : validate_operating_unit = ' ||
                              piv_site || ' - ');
  EXCEPTION
    WHEN OTHERS THEN
      g_retcode    := 2;
      l_record_cnt := 2;
      g_errbuff    := 'Failed while validating operating unit';
      print_log_message('In Exception Opertaing Unit Validation' ||
                        SQLERRM);
  END validate_operating_unit;

  --
  -- ========================
  -- Procedure: validate_party_site_exists
  -- =============================================================================
  --   This procedure validate_party_site_exists
  -- =============================================================================
  --  Input Parameters :
  -- piv_vendor_num
  -- piv_vendor_site_Code
  -- pin_org_id
  --

  --  Output Parameters :
  --  pon_error_cnt       : Return Error Count
  --  pon_vendor_site_id  : Vendor site ID
  -- -----------------------------------------------------------------------------
  --
  PROCEDURE validate_party_site_exists(piv_vendor_num       IN VARCHAR2,
                                       piv_vendor_site_Code IN VARCHAR2,
                                       pin_org_id           IN NUMBER,
                                       pon_party_site_id    OUT NUMBER,
                                       pon_error_cnt        OUT NUMBER) IS
    l_record_cnt    NUMBER;
    l_party_site_id NUMBER;

  BEGIN

    xxetn_debug_pkg.add_debug(' +  PROCEDURE : validate_party_site_exists  ' ||
                              piv_vendor_site_Code || ' + ');

    l_record_cnt := 0;

    BEGIN
      --validate party site and derive party site ID
      SELECT party_site_id
        INTO l_party_site_id
        FROM ap_supplier_sites_all a, ap_suppliers b
       WHERE b.segment1 = piv_vendor_num
         AND a.vendor_id = b.vendor_id
         AND a.vendor_site_code = piv_vendor_site_code
         AND a.org_id = pin_org_id
         AND b.enabled_flag = 'Y';

      pon_party_site_id := l_party_site_id;

    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        l_record_cnt      := 2;
        pon_party_site_id := NULL;
        print_log_message('In No Data found of validate party site check' ||
                          SQLERRM);
      WHEN OTHERS THEN
        l_record_cnt      := 2;
        pon_party_site_id := NULL;
        print_log_message('In When others of validate party site check' ||
                          SQLERRM);
    END;

    IF l_record_cnt = 2 THEN
      pon_error_cnt := 2;
    ELSE
      pon_error_cnt := 0;
    END IF;

    print_log_message(' -  PROCEDURE : validate_party_site_exists  - ');
  EXCEPTION
    WHEN OTHERS THEN
      g_retcode         := 2;
      pon_error_cnt     := 2;
      pon_party_site_id := NULL;
      g_errbuff         := 'Failed while validating party site.';
      print_log_message('In Exception validate party site' || SQLERRM);
  END validate_party_site_exists;

  --
  -- ========================
  -- Procedure: validate_acc_br_exists
  -- =============================================================================
  --   This procedure validate_acc_br__exists
  -- =============================================================================
  --  Input Parameters :
  --  piv_bank_name: Leg bank name
  --  piv_branch_number :leg_branch_number
  --  piv_country  : leg country

  --  Output Parameters :
  --  pon_branch_id    : Returns Branch id
  --  pon_bank_id      : Returns Bank Id
  --  pov_country      : Returns Bank/Branch Country
  --  pon_error_cnt    : Return Error Count
  -- -----------------------------------------------------------------------------
  --
  PROCEDURE validate_acc_br_exists(piv_bank_name   IN VARCHAR2,
                                   piv_branch_name IN VARCHAR2,
                                   piv_country     IN VARCHAR2,
                                   piv_bank_number IN VARCHAR2,     --- v3.0 Mock 4 country code is null
                                   piv_branch_number IN VARCHAR2,   --- v3.0 Mock 4 country code is null
                                   pon_branch_id   OUT NUMBER,
                                   pon_bank_id     OUT NUMBER,
                                   pov_country     OUT VARCHAR2,
                                   pon_error_cnt   OUT NUMBER) IS
    l_record_cnt NUMBER;
    l_branch_id  NUMBER;
    l_bank_id    NUMBER;
    l_country    VARCHAR2(60);

  BEGIN

    xxetn_debug_pkg.add_debug(' +  PROCEDURE : validate_acc_br_exists  ' ||
                              piv_branch_name || 'in country ' ||
                              piv_country || ' + ');

    l_record_cnt := 0;

    BEGIN
      --check if the bank branch already exists
      SELECT iebb.branch_party_id,
             cbv.bank_party_id,
             NVL(iebb.country, cbv.home_country)
        INTO l_branch_id, l_bank_id, l_country
        FROM iby_ext_bank_branches_v iebb, ce_banks_v cbv
       WHERE iebb.bank_party_id = cbv.bank_party_id
         AND UPPER(cbv.bank_name) = UPPER(piv_bank_name)
         AND UPPER(NVL(cbv.bank_number,'X')) = UPPER(NVL(piv_bank_number,'X'))          --- v3.0 Mock 4 country code is null
         AND (cbv.home_country = piv_country OR
             NVL(iebb.country, '99') = NVL(piv_country, '99'))
         AND iebb.bank_branch_name = piv_branch_name
         AND UPPER(NVL(iebb.branch_number,'X')) = UPPER(NVL(piv_branch_number,'X'));    --- v3.0 Mock 4 country code is null

    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        pon_error_cnt := 2;
        print_log_message('In No Data found of account branch already exists check' ||
                          SQLERRM);
      WHEN OTHERS THEN
        pon_error_cnt := 2;
        print_log_message('In When others of account branch already exists check' ||
                          SQLERRM);
    END;

    IF (l_branch_id IS NULL) THEN
      pon_error_cnt := 2;
    ELSE
      pon_branch_id := l_branch_id;
      pon_bank_id   := l_bank_id;
      pov_country   := l_country;
      pon_error_cnt := 0;
    END IF;

    print_log_message(' -  PROCEDURE : validate_acc_br_exists  - ');
  EXCEPTION
    WHEN OTHERS THEN
      g_retcode    := 2;
      l_record_cnt := 2;
      g_errbuff    := 'Failed while validating whether account branch already exists.';
      print_log_message('In Exception validate account branch exists' ||
                        SQLERRM);
  END validate_acc_br_exists;

  --
  -- ========================
  -- Procedure: validate_account_exists
  -- =============================================================================
  --   This procedure validate_account_exists
  -- =============================================================================
  --  Input Parameters :
  --  piv_bank_acct_num : Leg
  --  piv_bank_name: Leg bank name
  --  piv_country  : leg country
  --  piv_branch_name : Leg branch name
  --  piv_vendor_num  : leg vanedor number
  --  piv_vendor_site_code : Leg vendor site code
  --  pin_org_id : Leg org id
  --  Output Parameters :
  --  pon_error_cnt    : Return Error Count
  --  pon_bank_id      : Return Bank ID
  -- -----------------------------------------------------------------------------
  --
  PROCEDURE validate_account_exists(piv_bank_acct_num    IN VARCHAR2,
                                    piv_bank_name        IN VARCHAR2,
                                    piv_country          IN VARCHAR2,
                                    piv_branch_name      IN VARCHAR2,
                                    piv_vendor_num       IN VARCHAR2,
                                    piv_vendor_site_code IN VARCHAR2,
                                    pin_org_id           IN NUMBER,
                                    pon_error_cnt        OUT NUMBER) IS
    l_record_cnt NUMBER;
    l_account_id NUMBER;

  BEGIN

    xxetn_debug_pkg.add_debug(' +  PROCEDURE : validate_account_exists  ' ||
                              piv_bank_name || 'in country ' ||
                              piv_country || ' + ');

    l_record_cnt := 0;
    l_account_id := NULL;

    BEGIN
      SELECT COUNT(bbr.bank_account_num)
        INTO l_record_cnt
        FROM iby_ext_bank_accounts   bbr,
             iby_pmt_instr_uses_all  piu,
             iby_ext_banks_v         cbv,
             iby_ext_bank_branches_v iebb,
             iby_external_payees_all iepa,
             po_vendors              pv,
             po_vendor_sites_all     pvs,
             hr_operating_units      hou
       WHERE piu.payment_flow = 'DISBURSEMENTS'
         AND piu.instrument_type = 'BANKACCOUNT'
         AND piu.instrument_id = bbr.ext_bank_account_id
         AND bbr.bank_id = cbv.bank_party_id
         AND bbr.branch_id = iebb.branch_party_id
         AND piu.ext_pmt_party_id = iepa.ext_payee_id
         AND iepa.payee_party_id = pv.party_id
         AND iepa.supplier_site_id = pvs.vendor_site_id
         AND pvs.org_id = hou.organization_id
         AND bbr.bank_account_num = piv_bank_acct_num
         AND pv.segment1 = piv_vendor_num
         AND pvs.vendor_site_code = piv_vendor_site_code
         AND hou.organization_id = pin_org_id -- Added to ensure Supplier Bank Account exists is being checked properly
         AND UPPER(cbv.bank_name) = UPPER(piv_bank_name)
         AND NVL(bbr.country_code, '99') = NVL(piv_country, '99')
         AND UPPER(iebb.bank_branch_name) = UPPER(piv_branch_name);

    EXCEPTION
      WHEN NO_DATA_FOUND THEN

        print_log_message('In No Data found of account exists check' ||
                          SQLERRM);
      WHEN OTHERS THEN
        l_record_cnt := 2;
        print_log_message('In When others of account exists check' ||
                          SQLERRM);
    END;

    IF l_record_cnt = 2 THEN
      pon_error_cnt := 2;
    END IF;

    print_log_message(' -  PROCEDURE : validate_account_exists  - ');
  EXCEPTION
    WHEN OTHERS THEN
      g_retcode    := 2;
      l_record_cnt := 2;
      g_errbuff    := 'Failed while validating whether account already exists.';
      print_log_message('In Exception validate account exists' || SQLERRM);
  END validate_account_exists;

  --
  -- ========================
  -- Procedure: validate_banks
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
PROCEDURE Update_BIC_ISSC IS
Cursor C_bank is
Select Distinct BA.leg_attribute2,
                BB.leg_attribute5,
                BB.leg_bank_branch_id,
                BB.leg_bank_name,
                BB.Leg_Bank_Branch_name,
                BA.Leg_Branch_name,
                BA.Org_id, bb.leg_source_System
  from XXAP_SUPPLIER_BRANCHES_STG BB, XXAP_SUPPLIER_BANKACCNTS_STG BA
 where BA.leg_Source_system = 'ISSC'
   and BA.leg_Source_system = BB.leg_Source_system
   and BA.leg_bank_name = BB.leg_bank_name
   and BA.leg_branch_name = BB.leg_Bank_branch_name
   and BA.leg_bank_branch_id = BB.leg_bank_branch_id
   and BB.leg_attribute5 is NULL
   and  BA.leg_attribute2 IS NOT NULL;

Begin
For i in C_bank loop
Update  XXAP_SUPPLIER_BRANCHES_STG
 Set  leg_attribute5 = i.leg_attribute2
 Where leg_bank_branch_id = i.leg_bank_branch_id
 and leg_bank_name =i.leg_bank_name
 and Leg_Bank_Branch_name =i.Leg_Bank_Branch_name
 --ADB change 09/27/2016 starts here
 and leg_source_system = i.leg_source_system;
 --ADB change ends here
End loop;

  EXCEPTION
    WHEN OTHERS THEN
      g_retcode := 2;
      g_errbuff := 'Failed while Update_BIC_ISSC';
      print_log_message('In Update_BIC_ISSC when others' || SQLERRM);
  END Update_BIC_ISSC;


  --
  -- ========================
  -- Procedure: validate_banks
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
  PROCEDURE validate_banks

   IS
    l_bank_id         NUMBER;
    l_bank_end_date   DATE;
    l_bank_ret_status VARCHAR2(50);
    l_upd_ret_status  VARCHAR2(50);
    l_value_out       VARCHAR2(50);
    l_log_ret_status  VARCHAR2(50);
    l_log_err_msg     VARCHAR2(2000);

    l_msg_count     NUMBER;
    l_error_cnt     NUMBER;
    l_error_flag    VARCHAR2(10);
    l_msg_data      VARCHAR2(2000);
    l_return_status VARCHAR2(200);

    l_party_exists VARCHAR2(1);
    l_process_flag xxap_supplier_banks_stg.process_flag%TYPE;
    l_err_code     VARCHAR2(40);
    l_err_msg      VARCHAR2(2000);
    l_ret_status   VARCHAR2(50);

    -- Cursor to select Duplicate record for supplier bank staging table
    CURSOR Dup_supplierbank_cur IS
      select upper(xsbs.leg_bank_name) leg_bank_name, --v3.0
             xsbs.leg_bank_number,
             xsbs.leg_country,
             xsbs.request_id
        from xxap_supplier_banks_stg xsbs
       where xsbs.request_id = g_request_id
         and xsbs.process_flag = g_new
       group by upper(xsbs.leg_bank_name),  --v3.0
                xsbs.leg_bank_number,
                xsbs.leg_country,
                xsbs.request_id
      having count(1) > 1;

    /* SELECT *
    FROM   xxap_supplier_banks_stg xsbs
    WHERE  xsbs.request_id = g_request_id;*/

    --cursor to select new records from supplier bank staging table
    CURSOR validate_supplierbank_cur IS
      SELECT *
        FROM xxap_supplier_banks_stg xsbs
       WHERE xsbs.request_id = g_request_id
         AND xsbs.process_flag = g_new;

  BEGIN
    -- Initialize global variables for log_errors
    g_source_table    := g_bank_t;
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

    FOR Dup_supplierbank_rec IN Dup_supplierbank_cur LOOP
      BEGIN
        /*  If all duplicate bank is error out then make first bank is Valid and rest of all Error*/

        update xxap_supplier_banks_stg
           set process_flag = 'D'
         where rowid in
               (select "row_id"
                  from (select rowid "row_id",
                               upper(leg_bank_name), --v3.0
                               leg_bank_number,
                               leg_country,
                               request_id,
                               rank() over(partition by upper(leg_bank_name), leg_bank_number, leg_country, request_id order by rowid) rank_n
                          from xxap_supplier_banks_stg xs
                         where exists
                         (select 1
                                  from xxap_supplier_banks_stg xsbs
                                 where xsbs.request_id = g_request_id
                                   and xsbs.process_flag = g_new
                                   and upper(nvl(xsbs.leg_bank_name, '#')) =          --v3.0
                                       upper(nvl(dup_supplierbank_rec.leg_bank_name,  --v3.0
                                           '#'))
                                   and nvl(xsbs.leg_bank_number, '#') =
                                       nvl(dup_supplierbank_rec.leg_bank_number,
                                           '#')
                                   and nvl(xsbs.leg_country, '#') =
                                       nvl(dup_supplierbank_rec.leg_country,
                                           '#')
                                   and upper(nvl(xsbs.leg_bank_name, '#')) =         --v3.0
                                       upper(nvl(xs.leg_bank_name, '#'))             --v3.0
                                   and nvl(xsbs.leg_bank_number, '#') =
                                       nvl(xs.leg_bank_number, '#')
                                   and nvl(xsbs.leg_country, '#') =
                                       nvl(xs.leg_country, '#')
                                   and xsbs.request_id = xs.request_id))
                 where rank_n > 1);

        COMMIT;

        /*  -- Update End */
      END;
    END LOOP;

    FOR validate_supplierbank_rec IN validate_supplierbank_cur LOOP
      BEGIN

        -- Initialize loop variables
        l_error_flag     := 'N';
        l_error_cnt      := 0;
        l_err_code       := NULL;
        l_err_msg        := NULL;
        l_upd_ret_status := NULL;
        l_log_ret_status := NULL;
        l_log_err_msg    := NULL;
        xxetn_debug_pkg.add_debug('validate Bank Record : ' ||
                                  validate_supplierbank_rec.leg_bank_name || ', ' ||
                                  validate_supplierbank_rec.leg_country || ', ' ||
                                  validate_supplierbank_rec.interface_txn_id);

        g_intf_staging_id := validate_supplierbank_rec.interface_txn_id;
        g_src_keyname1    := 'LEG_BANK_NAME';
        g_src_keyvalue1   := validate_supplierbank_rec.leg_bank_name;
        g_src_keyname2    := 'LEG_COUNTRY';
        g_src_keyvalue2   := validate_supplierbank_rec.leg_country;

        --procedure to check mandatory values are not missing
        mandatory_value_check_bank(validate_supplierbank_rec.leg_bank_name
                                   --, validate_supplierbank_rec.leg_bank_number
                                  ,
                                   validate_supplierbank_rec.leg_country,
                                   validate_supplierbank_rec.leg_bank_institution_type
                                   --, validate_supplierbank_rec.leg_address1
                                  ,
                                   l_error_cnt);

        IF l_error_cnt > 0 THEN
          l_error_flag := g_yes;
        END IF;

        duplicate_bank(validate_supplierbank_rec.leg_bank_name,
                       validate_supplierbank_rec.leg_bank_number
                       --  , validate_supplierbank_rec.leg_bank_branch_id
                      ,
                       validate_supplierbank_rec.leg_country,
                       l_error_cnt);
        IF l_error_cnt > 0 THEN
          l_error_flag := g_yes;
          l_err_code   := 'ETN_AP_DUPLICATE_BANK';
          l_err_msg    := 'Error: duplicate bank record';

          log_errors(pov_return_status       => l_log_ret_status -- OUT
                    ,
                     pov_error_msg           => l_log_err_msg -- OUT
                    ,
                     piv_source_column_name  => 'LEG_BANK_NAME',
                     piv_source_column_value => validate_supplierbank_rec.leg_bank_name,
                     piv_error_type          => g_err_val,
                     piv_error_code          => l_err_code,
                     piv_error_message       => l_err_msg);

        END IF;

        l_error_cnt := 0;

        validate_country(validate_supplierbank_rec.leg_country,
                         l_error_cnt);

        IF l_error_cnt > 0 THEN
          l_error_flag := g_yes;
          l_err_code   := 'ETN_AP_INVALID_COUNTRY';
          l_err_msg    := 'Error: Country Code is not Valid';

          log_errors(pov_return_status       => l_log_ret_status -- OUT
                    ,
                     pov_error_msg           => l_log_err_msg -- OUT
                    ,
                     piv_source_column_name  => 'LEG_COUNTRY',
                     piv_source_column_value => validate_supplierbank_rec.leg_country,
                     piv_error_type          => g_err_val,
                     piv_error_code          => l_err_code,
                     piv_error_message       => l_err_msg);

        END IF;

        validate_institution_type(validate_supplierbank_rec.leg_bank_institution_type,
                                  l_error_cnt);

        IF l_error_cnt > 0 THEN
          l_error_flag := g_yes;
          l_err_code   := 'ETN_AP_INVALID_INSTITUION_TYPE';
          l_err_msg    := 'Error: Bank Institution Type is not Valid';

          log_errors(pov_return_status       => l_log_ret_status -- OUT
                    ,
                     pov_error_msg           => l_log_err_msg -- OUT
                    ,
                     piv_source_column_name  => 'LEG_BANK_INSTITUTION_TYPE',
                     piv_source_column_value => validate_supplierbank_rec.leg_bank_institution_type,
                     piv_error_type          => g_err_val,
                     piv_error_code          => l_err_code,
                     piv_error_message       => l_err_msg);

        END IF;

        validate_bank_exists(validate_supplierbank_rec.leg_bank_name,
                             validate_supplierbank_rec.leg_bank_number
                             -- , validate_supplierbank_rec.leg_bank_branch_id
                            ,
                             validate_supplierbank_rec.leg_country,
                             l_error_cnt);

        IF l_error_cnt > 0 THEN
          l_error_flag := g_yes;
          l_err_code   := 'ETN_AP_BANK_EXISTS';
          l_err_msg    := 'Error: Invalid bank. Bank already exists.';

          log_errors(pov_return_status       => l_log_ret_status -- OUT
                    ,
                     pov_error_msg           => l_log_err_msg -- OUT
                    ,
                     piv_source_column_name  => 'LEG_BANK_NAME',
                     piv_source_column_value => validate_supplierbank_rec.leg_bank_name,
                     piv_error_type          => g_err_val,
                     piv_error_code          => l_err_code,
                     piv_error_message       => l_err_msg);

        END IF;

        --Update staging table with the validation status as 'V' or 'E'
        UPDATE xxap_supplier_banks_stg
           SET process_flag           = DECODE(l_error_flag,
                                               g_yes,
                                               g_error,
                                               g_validated),
               error_type             = DECODE(l_error_flag,
                                               g_yes,
                                               g_err_val,
                                               NULL),
               last_updated_date      = SYSDATE,
               last_updated_by        = g_user_id,
               last_update_login      = g_login_id,
               program_application_id = g_prog_appl_id,
               program_id             = g_conc_program_id,
               program_update_date    = SYSDATE,
               request_id             = g_request_id
         WHERE interface_txn_id =
               validate_supplierbank_rec.interface_txn_id;

        IF (l_error_flag = g_yes) THEN
          g_retcode := 1;
        END IF;

        g_intf_staging_id := NULL;
        g_src_keyname1    := NULL;
        g_src_keyvalue1   := NULL;
        g_src_keyname2    := NULL;
        g_src_keyvalue2   := NULL;
        g_src_keyname3    := NULL;
        g_src_keyvalue3   := NULL;

        COMMIT;
      END;
    END LOOP;

    xxetn_debug_pkg.add_debug('-   PROCEDURE : validate_banks for batch id = ' ||
                              g_new_batch_id || ' - ');
  EXCEPTION
    WHEN OTHERS THEN
      g_retcode := 2;
      g_errbuff := 'Failed while vaildating bank';
      print_log_message('In Validate bank when others' || SQLERRM);
  END validate_banks;

  --
  -- ========================
  -- Procedure: validate_branches
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
  PROCEDURE validate_branches

   IS
    l_log_ret_status VARCHAR2(50);
    l_log_err_msg    VARCHAR2(2000);

    l_msg_count     NUMBER;
    l_error_cnt     NUMBER;
    l_error_flag    VARCHAR2(10);
    l_msg_data      VARCHAR2(2000);
    l_return_status VARCHAR2(200);

    l_party_exists VARCHAR2(1);
    l_process_flag xxap_supplier_branches_stg.process_flag%TYPE;
    l_err_code     VARCHAR2(40);
    l_bank_id      NUMBER;
    l_err_msg      VARCHAR2(2000);
    l_ret_status   VARCHAR2(50);

    -- Cursor to select Duplicate record for supplier branch staging table
    CURSOR Dup_supplierbranch_cur IS
      SELECT UPPER(xsbs.leg_bank_name)  leg_bank_name, --v3.0
             xsbs.leg_bank_branch_name,
             xsbs.leg_country,
             xsbs.request_id, --ADB 09/27/2016 changes start
             xsbs.leg_bank_number,
             xsbs.leg_branch_number --ADB 09/27/2016 changes end
        FROM xxap_supplier_branches_stg xsbs
       WHERE xsbs.request_id = g_request_id
         AND xsbs.process_flag = g_new
       Group by UPPER(xsbs.leg_bank_name),  --v3.0
                xsbs.leg_bank_branch_name,
                xsbs.leg_country,
                xsbs.leg_bank_number,  --ADB 09/27/2016 changes start
                xsbs.leg_branch_number,--ADB 09/27/2016 changes end
                xsbs.request_id
      having COUNT(1) > 1;

    --cursor to select new records from supplier branch staging table
    CURSOR validate_supplierbranch_cur IS
      SELECT *
        FROM xxap_supplier_branches_stg xsbs
       WHERE xsbs.request_id = g_request_id
       --ADB 09/27/2016 starts
       AND NVL(process_flag ,'#') <>'D';

  BEGIN
    -- Initialize global variables for log_errors
    g_source_table    := g_branch_t;
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

    Update_BIC_ISSC;

    FOR Dup_supplierbranch_rec IN Dup_supplierbranch_cur LOOP
      BEGIN
        /*  If all duplicate bank is error out then make first branchs is Valid and rest of all Error*/
        update xxap_supplier_branches_stg
           set process_flag = 'D'
         where rowid in
               (select "row_id"
                  from (select rowid "row_id",
                               UPPER(leg_bank_name),  --v3.0
                               leg_bank_branch_name,
                               leg_country,
                               request_id,
                               rank() over(partition by UPPER(leg_bank_name), leg_bank_branch_name, leg_country, request_id order by rowid) rank_n
                          from xxap_supplier_branches_stg xs
                         where exists
                         (select 1
                                  from xxap_supplier_branches_stg xsbs
                                 where xsbs.request_id = g_request_id
                                   and xsbs.process_flag = g_new
                                   and UPPER(nvl(xsbs.leg_bank_name, '#')) =   --v3.0
                                       UPPER(nvl(Dup_supplierbranch_rec.leg_bank_name,
                                           '#'))                               --v3.0
                                   and nvl(xsbs.leg_bank_branch_name, '#') =
                                       nvl(Dup_supplierbranch_rec.leg_bank_branch_name,
                                           '#')
                                   and nvl(xsbs.leg_country, '#') =
                                       nvl(Dup_supplierbranch_rec.leg_country,
                                           '#')
                                   and UPPER(nvl(xsbs.leg_bank_name, '#')) =  --v3.0
                                       UPPER(nvl(xs.leg_bank_name, '#'))      --v3.0
                                   and nvl(xsbs.leg_bank_branch_name, '#') =
                                       nvl(xs.leg_bank_branch_name, '#')
                                   and nvl(xsbs.leg_country, '#') =
                                       nvl(xs.leg_country, '#')
                                   and xsbs.request_id = xs.request_id
                                   --ADB changes 09/27/2016
                                   and nvl(xsbs.leg_bank_number, '#') =
                                       nvl(xs.leg_bank_number, '#')
                                   and nvl(xsbs.leg_bank_number, '#') =
                                       nvl(Dup_supplierbranch_rec.leg_bank_number, '#')
                                   and nvl(xsbs.leg_branch_number, '#') =
                                       nvl(xs.leg_branch_number, '#')
                                   and nvl(xsbs.leg_branch_number, '#') =
                                       nvl(Dup_supplierbranch_rec.leg_branch_number, '#')
                                   --ADB changes ends

                                       )
                                       )
                 where rank_n > 1);

        COMMIT;
      END;
    END LOOP;

    FOR validate_supplierbranch_rec IN validate_supplierbranch_cur LOOP
      BEGIN

        -- Initialize loop variables
        l_error_flag     := 'N';
        l_error_cnt      := 0;
        l_err_code       := NULL;
        l_err_msg        := NULL;
        l_log_ret_status := NULL;
        l_log_err_msg    := NULL;
        xxetn_debug_pkg.add_debug('Validate branch Record : ' ||
                                  validate_supplierbranch_rec.leg_bank_branch_name || ', ' ||
                                  validate_supplierbranch_rec.leg_country || ', ' ||
                                  validate_supplierbranch_rec.interface_txn_id);

        g_intf_staging_id := validate_supplierbranch_rec.interface_txn_id;
        g_src_keyname1    := 'LEG_BANK_BRANCH_NAME';
        g_src_keyvalue1   := validate_supplierbranch_rec.leg_bank_branch_name;
        g_src_keyname2    := 'LEG_COUNTRY';
        g_src_keyvalue2   := validate_supplierbranch_rec.leg_country;

        --procedure to check mandatory values are not missing
        mandatory_value_check_branch(validate_supplierbranch_rec.leg_bank_branch_name,
                                     validate_supplierbranch_rec.leg_bank_name
                                     --, validate_supplierbranch_rec.leg_branch_number
                                    ,
                                     validate_supplierbranch_rec.leg_country
                                     --, validate_supplierbranch_rec.leg_bank_branch_type
                                    ,
                                     l_error_cnt);

        IF l_error_cnt > 0 THEN
          l_error_flag := g_yes;
        END IF;
        --ADB 09/27/2016 changes start
        /*duplicate_branch(validate_supplierbranch_rec.leg_bank_name,
                         validate_supplierbranch_rec.leg_bank_branch_name,
                         validate_supplierbranch_rec.leg_country,
                         l_error_cnt);
        IF l_error_cnt > 0 THEN
          l_error_flag := g_yes;
          l_err_code   := 'ETN_AP_DUPLICATE_branch';
          l_err_msg    := 'Error: duplicate branch record';

          log_errors(pov_return_status       => l_log_ret_status -- OUT
                    ,
                     pov_error_msg           => l_log_err_msg -- OUT
                    ,
                     piv_source_column_name  => 'leg_bank_branch_name',
                     piv_source_column_value => validate_supplierbranch_rec.leg_bank_branch_name,
                     piv_error_type          => g_err_val,
                     piv_error_code          => l_err_code,
                     piv_error_message       => l_err_msg);

        END IF;*/
        --ADB 09/27/2016 changes ends
        l_error_cnt := 0;

        validate_country(validate_supplierbranch_rec.leg_country,
                         l_error_cnt);

        IF l_error_cnt > 0 THEN
          l_error_flag := g_yes;
          l_err_code   := 'ETN_AP_INVALID_COUNTRY';
          l_err_msg    := 'Error: Branch Country Code is not Valid';

          log_errors(pov_return_status       => l_log_ret_status -- OUT
                    ,
                     pov_error_msg           => l_log_err_msg -- OUT
                    ,
                     piv_source_column_name  => 'LEG_COUNTRY',
                     piv_source_column_value => validate_supplierbranch_rec.leg_country,
                     piv_error_type          => g_err_val,
                     piv_error_code          => l_err_code,
                     piv_error_message       => l_err_msg);

        END IF;

        IF validate_supplierbranch_rec.leg_bank_branch_type IS NOT NULL THEN

          validate_branch_type(validate_supplierbranch_rec.leg_bank_branch_type,
                               l_error_cnt);

          IF l_error_cnt > 0 THEN
            l_error_flag := g_yes;
            l_err_code   := 'ETN_AP_INVALID_BRANCH_TYPE';
            l_err_msg    := 'Error: branch branch Type is not Valid';

            log_errors(pov_return_status       => l_log_ret_status -- OUT
                      ,
                       pov_error_msg           => l_log_err_msg -- OUT
                      ,
                       piv_source_column_name  => 'LEG_BANK_BRANCH_TYPE',
                       piv_source_column_value => validate_supplierbranch_rec.leg_bank_branch_type,
                       piv_error_type          => g_err_val,
                       piv_error_code          => l_err_code,
                       piv_error_message       => l_err_msg);

          END IF;

        END IF;

        validate_br_bank_exists(validate_supplierbranch_rec.leg_bank_name,
                                validate_supplierbranch_rec.leg_bank_number
                                --  , validate_supplierbranch_rec.leg_bank_brach_id
                               ,
                                validate_supplierbranch_rec.leg_country,
                                l_bank_id,
                                l_error_cnt);

        IF l_error_cnt > 0 THEN
          l_error_flag := g_yes;
          l_err_code   := 'ETN_AP_BANK_DOESNOT_EXISTS';
          l_err_msg    := 'Error: Invalid bank. Bank does not exist for branch.';

          log_errors(pov_return_status       => l_log_ret_status -- OUT
                    ,
                     pov_error_msg           => l_log_err_msg -- OUT
                    ,
                     piv_source_column_name  => 'LEG_BANK_NAME',
                     piv_source_column_value => validate_supplierbranch_rec.leg_bank_name,
                     piv_error_type          => g_err_val,
                     piv_error_code          => l_err_code,
                     piv_error_message       => l_err_msg);

        END IF;

        validate_branch_exists(validate_supplierbranch_rec.leg_bank_name,
                               validate_supplierbranch_rec.leg_bank_branch_name,
                               validate_supplierbranch_rec.leg_country,
                               validate_supplierbranch_rec.leg_bank_number,
                               validate_supplierbranch_rec.leg_branch_number,
                               l_error_cnt);

        IF l_error_cnt > 0 THEN
          l_error_flag := g_yes;
          l_err_code   := 'ETN_AP_INVALID_branch_EXISTS';
          l_err_msg    := 'Error: Invalid branch. branch already exists.';

          log_errors(pov_return_status       => l_log_ret_status -- OUT
                    ,
                     pov_error_msg           => l_log_err_msg -- OUT
                    ,
                     piv_source_column_name  => 'leg_bank_branch_name',
                     piv_source_column_value => validate_supplierbranch_rec.leg_bank_branch_name,
                     piv_error_type          => g_err_val,
                     piv_error_code          => l_err_code,
                     piv_error_message       => l_err_msg);

        END IF;
        --Update staging table with the validation status as 'V' or 'E'
        UPDATE xxap_supplier_branches_stg
           SET bank_party_id          = l_bank_id,
               process_flag           = DECODE(l_error_flag,
                                               g_yes,
                                               g_error,
                                               g_validated),
               error_type             = DECODE(l_error_flag,
                                               g_yes,
                                               g_err_val,
                                               NULL),
               last_update_date       = SYSDATE,
               last_updated_by        = g_user_id,
               last_update_login      = g_login_id,
               program_application_id = g_prog_appl_id,
               program_id             = g_conc_program_id,
               program_update_date    = SYSDATE,
               request_id             = g_request_id
         WHERE interface_txn_id =
               validate_supplierbranch_rec.interface_txn_id;

        IF (l_error_flag = g_yes) THEN
          g_retcode := 1;
        END IF;

        g_intf_staging_id := NULL;
        g_src_keyname1    := NULL;
        g_src_keyvalue1   := NULL;
        g_src_keyname2    := NULL;
        g_src_keyvalue2   := NULL;
        g_src_keyname3    := NULL;
        g_src_keyvalue3   := NULL;

        COMMIT;
      END;
    END LOOP;

    xxetn_debug_pkg.add_debug('-   PROCEDURE : validate_branches for batch id = ' ||
                              g_new_batch_id || ' - ');
  EXCEPTION
    WHEN OTHERS THEN
      g_retcode := 2;
      g_errbuff := 'Failed while vaildating branches';
      print_log_message('In Validate branches when others' || SQLERRM);

  END validate_branches;

  --
  -- ========================
  -- Procedure: validate_bank_accounts
  -- =============================================================================
  --   This procedure is used to run generic validations for all mandatory columns
  --   checks for bank accounts
  -- =============================================================================
  --  Input Parameters :
  --    None
  --  Output Parameters :
  --    pov_retcode          :
  -- -----------------------------------------------------------------------------
  --
  PROCEDURE validate_bank_accounts

   IS
    l_count          NUMBER := 0;
    l_log_ret_status VARCHAR2(500);
    l_log_err_msg    VARCHAR2(4000);
    l_branch_id      NUMBER;
    l_vendor_id      NUMBER;
    l_vendor_site_id NUMBER;
    l_party_id       NUMBER;
    l_party_site_id  NUMBER;
    l_org_id         NUMBER;
    l_operating_unit VARCHAR2(4000);

    l_msg_count     NUMBER;
    l_error_cnt     NUMBER;
    l_error_flag    VARCHAR2(100);
    l_msg_data      VARCHAR2(4000);
    l_return_status VARCHAR2(4000);

    l_party_exists VARCHAR2(10);
    l_process_flag xxap_supplier_bankaccnts_stg.process_flag%TYPE;
    l_err_code     VARCHAR2(4000);
    l_bank_id      NUMBER;
    l_err_msg      VARCHAR2(4000);
    l_ret_status   VARCHAR2(4000);
    l_country      VARCHAR2(600);

    l_site xxetn_map_unit.site%TYPE;

    v_bank_number   VARCHAR2(240);
    v_branch_number VARCHAR2(240);


    -- Cursor to select Duplicate record for supplier bank accounts staging table
    CURSOR Dup_supplieraccounts_cur IS
      SELECT UPPER(xsbs.leg_bank_name) leg_bank_name, --v3.0
             xsbs.leg_branch_name,
             xsbs.leg_account_name,
             xsbs.leg_country,
             xsbs.leg_operating_unit_name,  -- v4.0
             xsbs.leg_vendor_num,
             xsbs.leg_vendor_site_code,
             xsbs.leg_account_num,
             xsbs.request_id,
             xsbr.leg_branch_number, --v3.0
             xsbr.leg_bank_number   --v3.0
        FROM xxap_supplier_bankaccnts_stg xsbs, xxap_supplier_branches_stg xsbr
       WHERE xsbs.request_id = g_request_id
         AND xsbs.process_flag = g_new
         --V3.0 ADB 09/27/2016 change start here
         AND xsbr.leg_source_system = xsbs.leg_source_system
         AND xsbr.leg_bank_branch_id = xsbs.leg_bank_branch_id
         AND UPPER(xsbr.leg_bank_name) = UPPER(xsbs.leg_bank_name) --v3.0
         AND xsbr.leg_bank_branch_name = xsbs.leg_branch_name
         --V3.0 ADB 09/27/2016 change end here
       Group by UPPER(xsbs.leg_bank_name),  --v3.0
                xsbs.leg_branch_name,
                xsbs.leg_account_name,
                xsbs.leg_country,
                xsbs.leg_operating_unit_name, --v4.0
                xsbs.leg_vendor_num,
                xsbs.leg_vendor_site_code,
                xsbs.leg_account_num,
                xsbs.request_id,
                xsbr.leg_branch_number, --v3.0
                xsbr.leg_bank_number   --v3.0
      having COUNT(1) > 1;

    --cursor to select new records from supplier bank accounts staging table
    CURSOR validate_supplieraccounts_cur IS
      SELECT *
        FROM xxap_supplier_bankaccnts_stg xsbs
       WHERE xsbs.request_id = g_request_id
       AND   NVL(xsbs.process_Flag,'#') <> 'D';

  BEGIN
    xxetn_debug_pkg.add_debug('+ PROCEDURE:  Validate_bank_accounts +');
    -- Initialize global variables for log_errors
    g_source_table    := g_account_t;
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

    FOR Dup_supplieraccounts_rec IN Dup_supplieraccounts_cur LOOP
      BEGIN
        --v3.0 ADB 09/27/2016 change start
        /*  If all duplicate bank account is error out then make first bank is Valid and rest of all Error*/
       /* Update xxap_supplier_bankaccnts_stg
           Set process_flag = 'D'
         Where rowid in
               (Select "row_id"
                  From (select rowid "row_id",
                               leg_bank_name,
                               leg_branch_name,
                               leg_country,
                               leg_account_name,
                               leg_operating_unit,
                               leg_vendor_num,
                               leg_vendor_site_code,
                               leg_account_num,
                               request_id,
                               rank() over(partition by leg_bank_name, leg_branch_name, leg_country, leg_account_name, leg_operating_unit, leg_vendor_num, leg_vendor_site_code, leg_account_num, request_id order by rowid) rank_n
                          From xxap_supplier_bankaccnts_stg xs
                         Where exists
                         (Select 1
                                  From xxap_supplier_bankaccnts_stg xsbs
                                 Where xsbs.request_id = g_request_id
                                   and xsbs.process_flag = g_new
                                   and nvl(xsbs.leg_bank_name, '#') =
                                       nvl(Dup_supplieraccounts_rec.leg_bank_name,
                                           '#')
                                   and nvl(xsbs.leg_branch_name, '#') =
                                       nvl(Dup_supplieraccounts_rec.leg_branch_name,
                                           '#')
                                   and nvl(xsbs.leg_country, '#') =
                                       nvl(Dup_supplieraccounts_rec.leg_country,
                                           '#')
                                   and nvl(xsbs.leg_account_name, '#') =
                                       nvl(Dup_supplieraccounts_rec.leg_account_name,
                                           '#')
                                   and nvl(xsbs.leg_operating_unit, '#') =
                                       nvl(Dup_supplieraccounts_rec.leg_operating_unit,
                                           '#')
                                   and nvl(xsbs.leg_vendor_num, '#') =
                                       nvl(Dup_supplieraccounts_rec.leg_vendor_num,
                                           '#')
                                   and nvl(xsbs.leg_vendor_site_code, '#') =
                                       nvl(Dup_supplieraccounts_rec.leg_vendor_site_code,
                                           '#')
                                   and nvl(xsbs.leg_account_num, '#') =
                                       nvl(Dup_supplieraccounts_rec.leg_account_num,
                                           '#')
                                   and nvl(xsbs.leg_bank_name, '#') =
                                       nvl(xs.leg_bank_name, '#')
                                   and nvl(xsbs.leg_branch_name, '#') =
                                       nvl(xs.leg_branch_name, '#')
                                   and nvl(xsbs.leg_country, '#') =
                                       nvl(xs.leg_country, '#')
                                   and nvl(xsbs.leg_account_name, '#') =
                                       nvl(xs.leg_account_name, '#')
                                   and nvl(xsbs.leg_operating_unit, '#') =
                                       nvl(xs.leg_operating_unit, '#')
                                   and nvl(xsbs.leg_vendor_num, '#') =
                                       nvl(xs.leg_vendor_num, '#')
                                   and nvl(xsbs.leg_vendor_site_code, '#') =
                                       nvl(xs.leg_vendor_site_code, '#')
                                   and nvl(xsbs.leg_account_num, '#') =
                                       nvl(xs.leg_account_num, '#')
                                   and xsbs.request_id = xs.request_id))
                 Where rank_n > 1);*/


           Update xxap_supplier_bankaccnts_stg
           Set process_flag = 'D'
           Where rowid in
               (Select "row_id"
                  From (select xs.rowid "row_id",
                               UPPER(xs.leg_bank_name), --v3.0
                               xs.leg_branch_name,
                               xs.leg_country,
                               xs.leg_account_name,
                               xs.leg_operating_unit_name,
                               xs.leg_vendor_num,
                               xs.leg_vendor_site_code,
                               xs.leg_account_num,
                               xs.request_id,
                               rank() over(partition by UPPER(xs.leg_bank_name),xsr.leg_bank_number, xs.leg_branch_name,  xsr.leg_branch_number, xs.leg_country, xs.leg_account_name, xs.leg_operating_unit_name,--v4.0
                                                        xs.leg_vendor_num, xs.leg_vendor_site_code, xs.leg_account_num, xs.request_id
                                                         order by xs.rowid) rank_n
                          From xxap_supplier_bankaccnts_stg xs, xxap_supplier_branches_stg xsr
                         Where --
                                   xs.leg_bank_branch_id = xsr.leg_bank_branch_id
                                   and xs.leg_source_system = xsr.leg_source_System
                                   and UPPER( xs.leg_bank_name) = UPPER(xsr.leg_bank_name)   --v3.0
                                   and xs.leg_branch_name = xsr.leg_bank_branch_name
                                   --
                          and exists
                         (Select 1
                                  From xxap_supplier_bankaccnts_stg xsbs, xxap_supplier_branches_stg xsbr
                                 Where xsbs.request_id = g_request_id
                                   and xsbs.process_flag = g_new
                                   --
                                   and xsbs.leg_bank_branch_id = xsbr.leg_bank_branch_id
                                   and xsbs.leg_source_system = xsbr.leg_source_System
                                   and UPPER(xsbs.leg_bank_name) = UPPER(xsbr.leg_bank_name)  --v3.0
                                   and xsbs.leg_branch_name = xsbr.leg_bank_branch_name
                                   and nvl(xsbr.leg_bank_number, '#') =
                                       nvl(Dup_supplieraccounts_rec.leg_bank_number,
                                           '#')
                                   and nvl(xsbr.leg_branch_number, '#') =
                                       nvl(Dup_supplieraccounts_rec.leg_branch_number,
                                           '#')
                                    and nvl(xsbr.leg_branch_number, '#') =
                                       nvl(xsr.leg_branch_number, '#')
                                   and nvl(xsbr.leg_bank_number, '#') =
                                       nvl(xsr.leg_bank_number, '#')
                                   --
                                   and UPPER(nvl(xsbs.leg_bank_name, '#')) =
                                       UPPER(nvl(Dup_supplieraccounts_rec.leg_bank_name,
                                           '#'))  --v3.0
                                   and nvl(xsbs.leg_branch_name, '#') =
                                       nvl(Dup_supplieraccounts_rec.leg_branch_name,
                                           '#')
                                   and nvl(xsbs.leg_country, '#') =
                                       nvl(Dup_supplieraccounts_rec.leg_country,
                                           '#')
                                   and nvl(xsbs.leg_account_name, '#') =
                                       nvl(Dup_supplieraccounts_rec.leg_account_name,
                                           '#')
                                   and nvl(xsbs.leg_operating_unit_name, '#') =
                                       nvl(Dup_supplieraccounts_rec.leg_operating_unit_name,
                                           '#')
                                   and nvl(xsbs.leg_vendor_num, '#') =
                                       nvl(Dup_supplieraccounts_rec.leg_vendor_num,
                                           '#')
                                   and nvl(xsbs.leg_vendor_site_code, '#') =
                                       nvl(Dup_supplieraccounts_rec.leg_vendor_site_code,
                                           '#')
                                   and nvl(xsbs.leg_account_num, '#') =
                                       nvl(Dup_supplieraccounts_rec.leg_account_num,
                                           '#')
                                   and UPPER(nvl(xsbs.leg_bank_name, '#')) =
                                       UPPER(nvl(xs.leg_bank_name, '#'))        --v3.0
                                   and nvl(xsbs.leg_branch_name, '#') =
                                       nvl(xs.leg_branch_name, '#')
                                   and nvl(xsbs.leg_country, '#') =
                                       nvl(xs.leg_country, '#')
                                   and nvl(xsbs.leg_account_name, '#') =
                                       nvl(xs.leg_account_name, '#')
                                   and nvl(xsbs.leg_operating_unit_name, '#') =
                                       nvl(xs.leg_operating_unit_name, '#')
                                   and nvl(xsbs.leg_vendor_num, '#') =
                                       nvl(xs.leg_vendor_num, '#')
                                   and nvl(xsbs.leg_vendor_site_code, '#') =
                                       nvl(xs.leg_vendor_site_code, '#')
                                   and nvl(xsbs.leg_account_num, '#') =
                                       nvl(xs.leg_account_num, '#')
                                   and xsbs.request_id = xs.request_id))
                 Where rank_n > 1);
--v3.0 ADB 09/27/2016 change end
        COMMIT;
      END;
    END LOOP;

    FOR validate_supplieraccounts_rec IN validate_supplieraccounts_cur LOOP
      BEGIN

        -- Initialize loop variables
        l_error_flag     := 'N';
        l_error_cnt      := 0;
        l_err_code       := NULL;
        l_err_msg        := NULL;
        l_log_ret_status := NULL;
        l_log_err_msg    := NULL;
        xxetn_debug_pkg.add_debug('Validate account Record : ' ||
                                  validate_supplieraccounts_rec.leg_account_name || ', ' ||
                                  validate_supplieraccounts_rec.leg_country || ', ' ||
                                  validate_supplieraccounts_rec.interface_txn_id);

        g_intf_staging_id := validate_supplieraccounts_rec.interface_txn_id;
        g_src_keyname1    := 'LEG_ACCOUNT_NAME';
        g_src_keyvalue1   := validate_supplieraccounts_rec.leg_account_name;
        g_src_keyname2    := 'LEG_COUNTRY';
        g_src_keyvalue2   := validate_supplieraccounts_rec.leg_country;
        g_src_keyname3    := 'LEG_BANK_NAME';
        g_src_keyvalue3   := validate_supplieraccounts_rec.leg_bank_name;

        --procedure to check mandatory values are not missing
        mandatory_value_check_account(validate_supplieraccounts_rec.leg_bank_name,
                                      validate_supplieraccounts_rec.leg_branch_name,
                                      validate_supplieraccounts_rec.leg_vendor_name
                                      --, validate_supplieraccounts_rec.leg_country
                                     ,
                                      validate_supplieraccounts_rec.leg_account_name,
                                      validate_supplieraccounts_rec.leg_account_num
                                      --, validate_supplieraccounts_rec.leg_operating_unit
                                     ,
                                      validate_supplieraccounts_rec.leg_account_type,
                                      l_error_cnt);

        IF l_error_cnt > 0 THEN
          l_error_flag := g_yes;
        END IF;
       --V3.0 ADB  09/27/2016 start here
       /* duplicate_account(validate_supplieraccounts_rec.leg_bank_name,
                          validate_supplieraccounts_rec.leg_branch_name,
                          validate_supplieraccounts_rec.leg_account_name,
                          validate_supplieraccounts_rec.leg_operating_unit,
                          validate_supplieraccounts_rec.leg_country,
                          validate_supplieraccounts_rec.leg_vendor_num,
                          validate_supplieraccounts_rec.leg_vendor_site_code,
                          validate_supplieraccounts_rec.leg_account_num,
                          l_error_cnt);
        IF l_error_cnt > 0 THEN
          l_error_flag := g_yes;
          l_err_code   := 'ETN_AP_DUPLICATE_ACCOUNT';
          l_err_msg    := 'Error: duplicate account record';

          log_errors(pov_return_status       => l_log_ret_status -- OUT
                    ,
                     pov_error_msg           => l_log_err_msg -- OUT
                    ,
                     piv_source_column_name  => 'lEG_ACCOUNT_NAME',
                     piv_source_column_value => validate_supplieraccounts_rec.leg_account_name,
                     piv_error_type          => g_err_val,
                     piv_error_code          => l_err_code,
                     piv_error_message       => l_err_msg);

        END IF;*/
        --V3.0 ADB  09/27/2016 start here
        l_error_cnt := 0;

        print_log_message('validate_supplieraccounts_rec.leg_vendor_num' ||
                          validate_supplieraccounts_rec.leg_vendor_num);
        print_log_message('validate_supplieraccounts_rec.leg_vendor_site_code' ||
                          validate_supplieraccounts_rec.leg_vendor_site_code);

        validate_vendor(validate_supplieraccounts_rec.leg_vendor_num,
                        l_vendor_id,
                        l_error_cnt);

        IF l_error_cnt > 0 THEN
          l_error_flag := g_yes;
          l_err_code   := 'ETN_AP_INVALID_VENDOR';
          l_err_msg    := 'Error: Vendor is not Valid';

          log_errors(pov_return_status       => l_log_ret_status -- OUT
                    ,
                     pov_error_msg           => l_log_err_msg -- OUT
                    ,
                     piv_source_column_name  => 'LEG_VENDOR_NAME',
                     piv_source_column_value => validate_supplieraccounts_rec.leg_vendor_name,
                     piv_error_type          => g_err_val,
                     piv_error_code          => l_err_code,
                     piv_error_message       => l_err_msg);

        END IF;

        l_site := NULL;
        IF validate_supplieraccounts_rec.leg_vendor_site_id IS NOT NULL AND
           validate_supplieraccounts_rec.leg_vendor_site_code IS NOT NULL THEN

          -- Fetching Plant# or Site# from Corresponding Site record
          BEGIN
            SELECT xsss.leg_accts_pay_code_segment1
              INTO l_site
              FROM xxap_supplier_sites_stg xsss
             WHERE xsss.leg_vendor_site_id =
                   validate_supplieraccounts_rec.leg_vendor_site_id
               AND xsss.leg_source_system =
                   validate_supplieraccounts_rec.leg_source_system;
          EXCEPTION
            WHEN OTHERS THEN
              l_error_flag := g_yes;
              log_errors(pov_return_status       => l_log_ret_status -- OUT
                        ,
                         pov_error_msg           => l_log_err_msg -- OUT
                        ,
                         piv_source_column_name  => 'LEG_VENDOR_SITE_CODE',
                         piv_source_column_value => validate_supplieraccounts_rec.leg_vendor_site_code,
                         piv_error_type          => g_err_val,
                         piv_error_code          => 'ETN_AP_ACCT_PAY_CODE_SEGMENT1_ERROR',
                         piv_error_message       => 'Error : Acct Pay Code Segment1 is invalid for given Vendor Site Code.Error: ' ||
                                                    SQLERRM);
          END;

        END IF;

        -- If Site value is found
        l_error_cnt := 0;
        IF l_site IS NOT NULL THEN
          -- validate operating unit
          validate_operating_unit(piv_site           => l_site,
                                  pov_operating_unit => l_operating_unit,
                                  pon_org_id         => l_org_id,
                                  pon_error_cnt      => l_error_cnt);

          IF l_error_cnt > 0 THEN
            l_error_flag := g_yes;
            l_err_code   := 'ETN_AP_INVALID_PLANT_SITE';
            l_err_msg    := 'Error: R12 OU does not exist in ETN Map Unit table for given site or Operating unit does not exist in R12.';

            log_errors(pov_return_status       => l_log_ret_status -- OUT
                      ,
                       pov_error_msg           => l_log_err_msg -- OUT
                      ,
                       piv_source_column_name  => 'PLANT_SITE',
                       piv_source_column_value => l_site,
                       piv_error_type          => g_err_val,
                       piv_error_code          => l_err_code,
                       piv_error_message       => l_err_msg);
          END IF;
        END IF;

        l_error_cnt := 0;
        IF validate_supplieraccounts_rec.leg_vendor_site_code IS NOT NULL AND
           l_org_id IS NOT NULL THEN

          validate_vendor_site(validate_supplieraccounts_rec.leg_vendor_num,
                               validate_supplieraccounts_rec.leg_vendor_site_code,
                               l_org_id,
                               l_vendor_site_id,
                               l_error_cnt);

          IF l_error_cnt > 0 THEN
            l_error_flag := g_yes;
            l_err_code   := 'ETN_AP_INVALID_VENDOR_SITE';
            l_err_msg    := 'Error: Vendor Site is not valid.';

            log_errors(pov_return_status       => l_log_ret_status -- OUT
                      ,
                       pov_error_msg           => l_log_err_msg -- OUT
                      ,
                       piv_source_column_name  => 'LEG_VENDOR_SITE_CODE',
                       piv_source_column_value => validate_supplieraccounts_rec.leg_vendor_site_code,
                       piv_error_type          => g_err_val,
                       piv_error_code          => l_err_code,
                       piv_error_message       => l_err_msg);

          END IF;

        END IF;

        /**
                 IF validate_supplieraccounts_rec.leg_bank_name IS NOT NULL AND validate_supplieraccounts_rec.leg_country IS NOT NULL THEN
                    l_error_cnt := 0;
                    --passing NULL for bank_number since the field is not present in Account stg table
                    validate_br_bank_exists( validate_supplieraccounts_rec.leg_bank_name
                                           , NULL
                                           , validate_supplieraccounts_rec.leg_country
                                           , l_bank_id
                                           , l_error_cnt);

                    IF l_error_cnt > 0 THEN
                       l_error_flag := g_yes;
                       l_err_code   := 'ETN_AP_BANK_DOESNOT_EXISTS';
                       l_err_msg    := 'Error: Invalid bank. Bank does not exist for account.';

                       log_errors ( pov_return_status          =>   l_log_ret_status          -- OUT
                                  , pov_error_msg              =>   l_log_err_msg             -- OUT
                                  , piv_source_column_name     =>   'LEG_BANK_NAME'
                                  , piv_source_column_value    =>   validate_supplieraccounts_rec.leg_bank_name
                                  , piv_error_type             =>   g_err_val
                                  , piv_error_code             =>   l_err_code
                                  , piv_error_message          =>   l_err_msg
                                  );
                    END IF;
                 END IF;
        **/

        IF validate_supplieraccounts_rec.leg_bank_name IS NOT NULL AND
           validate_supplieraccounts_rec.leg_branch_name IS NOT NULL THEN

        --- v3.0 Mock 4 country code is null



           BEGIN

               SELECT leg_bank_number , leg_branch_number  INTO v_bank_number , v_branch_number
               FROM   xxconv.xxap_supplier_branches_stg
               WHERE  leg_bank_branch_id = validate_supplieraccounts_rec.leg_bank_branch_id
               AND    leg_source_system  = validate_supplieraccounts_rec.leg_source_system
               AND    upper(leg_bank_name)      = upper(validate_supplieraccounts_rec.leg_bank_name) --v3.0
               AND    leg_bank_branch_name = validate_supplieraccounts_rec.leg_branch_name;

           EXCEPTION
              WHEN OTHERS THEN

                 print_log_message('BANK ACCOUNT VALIDATION FOR BRANCH-ID -' || validate_supplieraccounts_rec.LEG_BANK_BRANCH_ID);

           END;

     --- v3.0 Mock 4 country code is null


          l_error_cnt := 0;
          validate_acc_br_exists(validate_supplieraccounts_rec.leg_bank_name,
                                 validate_supplieraccounts_rec.leg_branch_name,
                                 validate_supplieraccounts_rec.leg_country,
                                 v_bank_number,    --- v3.0 Mock 4 country code is null
                                 v_branch_number,  --- v3.0 Mock 4 country code is null
                                 l_branch_id,
                                 l_bank_id,
                                 l_country,
                                 l_error_cnt);

          IF l_error_cnt > 0 THEN
            l_error_flag := g_yes;
            l_err_code   := 'ETN_AP_BANK_BRANCH_DOESNOT_EXISTS';
            l_err_msg    := 'Error: Invalid Bank and branch. Bank and Branch does not exist for account.';

            log_errors(pov_return_status       => l_log_ret_status -- OUT
                      ,
                       pov_error_msg           => l_log_err_msg -- OUT
                      ,
                       piv_source_column_name  => 'LEG_BANK_BRANCH_NAME',
                       piv_source_column_value => validate_supplieraccounts_rec.leg_bank_name || '~~' ||
                                                  validate_supplieraccounts_rec.leg_branch_name,
                       piv_error_type          => g_err_val,
                       piv_error_code          => l_err_code,
                       piv_error_message       => l_err_msg);
          END IF;
        END IF;

        l_error_cnt := 0;
        IF (validate_supplieraccounts_rec.leg_vendor_num IS NOT NULL) THEN

          validate_party_exists(validate_supplieraccounts_rec.leg_vendor_num,
                                l_party_id,
                                l_error_cnt);

          IF l_error_cnt > 0 THEN
            l_error_flag := g_yes;
            l_err_code   := 'ETN_AP_PARTY_DOESNOT_EXISTS';
            l_err_msg    := 'Error: Invalid PArty. PArty does not exist for vendor.';

            log_errors(pov_return_status       => l_log_ret_status -- OUT
                      ,
                       pov_error_msg           => l_log_err_msg -- OUT
                      ,
                       piv_source_column_name  => 'LEG_VENDOR_NUM',
                       piv_source_column_value => validate_supplieraccounts_rec.leg_vendor_num,
                       piv_error_type          => g_err_val,
                       piv_error_code          => l_err_code,
                       piv_error_message       => l_err_msg);

          END IF;
        END IF;

        l_error_cnt := 0;
        IF (validate_supplieraccounts_rec.leg_vendor_num IS NOT NULL AND
           validate_supplieraccounts_rec.leg_vendor_site_code IS NOT NULL) THEN

          validate_party_site_exists(validate_supplieraccounts_rec.leg_vendor_num,
                                     validate_supplieraccounts_rec.leg_vendor_site_code,
                                     l_org_id,
                                     l_party_site_id,
                                     l_error_cnt);

          IF l_error_cnt > 0 THEN
            l_error_flag := g_yes;
            l_err_code   := 'ETN_AP_PARTY_SITE_NOT_EXISTS';
            l_err_msg    := 'Error: Invalid Party Site. Party Site does not exist for vendor.';

            log_errors(pov_return_status       => l_log_ret_status -- OUT
                      ,
                       pov_error_msg           => l_log_err_msg -- OUT
                      ,
                       piv_source_column_name  => 'LEG_VENDOR_NUM' || '~' ||
                                                  'LEG_VENDOR_SITE_CODE',
                       piv_source_column_value => validate_supplieraccounts_rec.leg_vendor_num || '~' ||
                                                  validate_supplieraccounts_rec.leg_vendor_site_code,
                       piv_error_type          => g_err_val,
                       piv_error_code          => l_err_code,
                       piv_error_message       => l_err_msg);

          END IF;
        END IF;

        l_error_cnt := 0;

        validate_account_exists(validate_supplieraccounts_rec.leg_account_num,
                                validate_supplieraccounts_rec.leg_bank_name,
                                l_country,
                                validate_supplieraccounts_rec.leg_branch_name,
                                validate_supplieraccounts_rec.leg_vendor_num,
                                validate_supplieraccounts_rec.leg_vendor_site_code,
                                l_org_id,
                                l_error_cnt);

        IF l_error_cnt > 0 THEN
          l_error_flag := g_yes;
          l_err_code   := 'ETN_AP_INVALID_BANK_ACCOUNT';
          l_err_msg    := 'Error: Invalid Account.BAnk Account already exists.';

          log_errors(pov_return_status       => l_log_ret_status -- OUT
                    ,
                     pov_error_msg           => l_log_err_msg -- OUT
                    ,
                     piv_source_column_name  => 'LEG_ACCOUNT_NUM',
                     piv_source_column_value => validate_supplieraccounts_rec.leg_account_num,
                     piv_error_type          => g_err_val,
                     piv_error_code          => l_err_code,
                     piv_error_message       => l_err_msg);

        END IF;
        --Update staging table with the validation status as 'V' or 'E'
        UPDATE xxap_supplier_bankaccnts_stg
           SET bank_id                = l_bank_id,
               branch_id              = l_branch_id,
               vendor_id              = l_vendor_id,
               vendor_site_id         = l_vendor_site_id,
               party_id               = l_party_id,
               party_site_id          = l_party_site_id,
               org_id                 = l_org_id,
               country                = l_country,
               process_flag           = DECODE(l_error_flag,
                                               g_yes,
                                               g_error,
                                               g_validated),
               error_type             = DECODE(l_error_flag,
                                               g_yes,
                                               g_err_val,
                                               NULL),
               last_updated_date      = SYSDATE,
               last_updated_by        = g_user_id,
               last_update_login      = g_login_id,
               program_application_id = g_prog_appl_id,
               program_id             = g_conc_program_id,
               program_update_date    = SYSDATE,
               request_id             = g_request_id
         WHERE interface_txn_id =
               validate_supplieraccounts_rec.interface_txn_id;

        IF (l_error_flag = g_yes) THEN
          g_retcode := 1;
        END IF;

        g_intf_staging_id := NULL;
        g_src_keyname1    := NULL;
        g_src_keyvalue1   := NULL;
        g_src_keyname2    := NULL;
        g_src_keyvalue2   := NULL;
        g_src_keyname3    := NULL;
        g_src_keyvalue3   := NULL;

        -- If Batch Commit Limit is reached
        IF l_count >= 1000 THEN
          l_count := 0;
          COMMIT;
        ELSE
          l_count := l_count + 1;
        END IF;

      END;
    END LOOP;

    xxetn_debug_pkg.add_debug('-   PROCEDURE : validate_bank_accounts for batch id = ' ||
                              g_new_batch_id || ' - ');
  EXCEPTION
    WHEN OTHERS THEN
      g_retcode := 2;
      g_errbuff := 'Failed while vaildating accounts';
      print_log_message('In Validate accounts when others' || SQLERRM);

  END validate_bank_accounts;

  /*** Added for v1.1 ***/
  --
  -- =============================================================================
  -- Procedure: mandatory_check_int_accts
  -- =============================================================================
  --   This procedure to do mandatory value check
  -- =============================================================================
  --  Input Parameters :
  --   piv_supplier_num
  --   piv_operating_unit
  --   piv_site_code
  --   piv_suplier_account

  --  Output Parameters :
  --   pon_error_cnt    : Return Error Count
  -- -----------------------------------------------------------------------------
  --
  PROCEDURE mandatory_check_int_accts(piv_supplier_num    IN VARCHAR2,
                                      piv_operating_unit  IN VARCHAR2,
                                      piv_site_code       IN VARCHAR2,
                                      piv_suplier_account IN VARCHAR2,
                                      pon_error_cnt       OUT NUMBER) IS
    l_record_cnt     NUMBER;
    l_err_msg        VARCHAR2(2000);
    l_log_ret_status VARCHAR2(50);
    l_log_err_msg    VARCHAR2(2000);
    l_err_code       VARCHAR2(40) := NULL;
  BEGIN

    print_log_message('   PROCEDURE : mandatory_check_int_accts');
    l_record_cnt     := 0;
    l_err_msg        := NULL;
    l_log_ret_status := NULL;
    l_log_err_msg    := NULL;
    l_err_code       := NULL;

    --Mandatory Column check
    IF piv_supplier_num IS NULL THEN
      l_record_cnt := 1;
      xxetn_debug_pkg.add_debug('Mandatory Value missing on record.');
      l_err_code := 'ETN_AP_MANDATORY_NOT_ENTERED';
      l_err_msg  := 'Error: Mandatory Value missing on record.';

      log_errors(pov_return_status       => l_log_ret_status -- OUT
                ,
                 pov_error_msg           => l_log_err_msg -- OUT
                ,
                 piv_source_column_name  => 'SUPPLIER_NUM',
                 piv_source_column_value => piv_supplier_num,
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
                 piv_source_column_name  => 'OPERATING_UNIT',
                 piv_source_column_value => piv_operating_unit,
                 piv_error_type          => g_err_val,
                 piv_error_code          => l_err_code,
                 piv_error_message       => l_err_msg);
    END IF;

    --Mandatory Column check
    IF piv_site_code IS NULL THEN
      l_record_cnt := 3;
      xxetn_debug_pkg.add_debug('Mandatory Value missing on record.');
      l_err_code := 'ETN_AP_MANDATORY_NOT_ENTERED';
      l_err_msg  := 'Error: Mandatory Value missing on record.';

      log_errors(pov_return_status       => l_log_ret_status -- OUT
                ,
                 pov_error_msg           => l_log_err_msg -- OUT
                ,
                 piv_source_column_name  => 'SUPPLIER_SITE_CODE',
                 piv_source_column_value => piv_site_code,
                 piv_error_type          => g_err_val,
                 piv_error_code          => l_err_code,
                 piv_error_message       => l_err_msg);
    END IF;

    --Mandatory Column check
    IF piv_suplier_account IS NULL THEN
      l_record_cnt := 4;
      xxetn_debug_pkg.add_debug('Mandatory Value missing on record.');
      l_err_code := 'ETN_AP_MANDATORY_NOT_ENTERED';
      l_err_msg  := 'Error: Mandatory Value missing on record.';

      log_errors(pov_return_status       => l_log_ret_status -- OUT
                ,
                 pov_error_msg           => l_log_err_msg -- OUT
                ,
                 piv_source_column_name  => 'SUPPLIER_ACCOUNT',
                 piv_source_column_value => piv_suplier_account,
                 piv_error_type          => g_err_val,
                 piv_error_code          => l_err_code,
                 piv_error_message       => l_err_msg);
    END IF;

    IF l_record_cnt > 1 THEN
      pon_error_cnt := 2;
    END IF;

  EXCEPTION
    WHEN OTHERS THEN
      g_retcode     := 2;
      pon_error_cnt := 2;
      print_log_message('In Exception mandatory_check_int_accts check' ||
                        SQLERRM);
  END mandatory_check_int_accts;

  /** Added for v1.1 **/
  --
  -- =============================================================================
  -- Procedure: validate_account_int
  -- =============================================================================
  --   This procedure validate if Supplier Account exists
  -- =============================================================================
  --  Input Parameters :
  --  piv_bank_acct_num    : Bank Account Number
  --  piv_vendor_num       : Vendor Number
  --  pin_vendor_site_id   : Vendor Site Id
  --  pin_org_id           : Org Id

  --  Output Parameters    :
  --  pon_bank_acct_id     : External Bank Account Id
  --  pov_account_name     : External Bank Account Name
  --  pod_start_date       : External Bank Account Start Date
  --  pon_branch_id        : External Bank Branch Id
  --  pon_bank_id          : External Bank Id
  --  pov_currency         : External Bank Account Currency
  --  pon_error_cnt        : Return Error Count
  -- -----------------------------------------------------------------------------
  --

  PROCEDURE validate_account_int(piv_bank_acct_num  IN VARCHAR2,
                                 piv_vendor_num     IN VARCHAR2,
                                 pin_vendor_site_id IN NUMBER,
                                 pin_org_id         IN NUMBER,
                                 pon_bank_acct_id   OUT NUMBER,
                                 pov_account_name   OUT VARCHAR2,
                                 pod_start_date     OUT DATE,
                                 pon_branch_id      OUT NUMBER,
                                 pon_bank_id        OUT NUMBER,
                                 pov_currency       OUT VARCHAR2,
                                 pon_error_cnt      OUT NUMBER) IS

    l_record_cnt NUMBER;

  BEGIN

    xxetn_debug_pkg.add_debug(' +  PROCEDURE : validate_account_int  ');

    pon_bank_acct_id := NULL;
    pov_account_name := NULL;
    pod_start_date   := NULL;
    pon_branch_id    := NULL;
    pon_bank_id      := NULL;
    pov_currency     := NULL;

    BEGIN
      SELECT bbr.ext_bank_account_id,
             bbr.bank_account_name,
             bbr.start_date,
             bbr.branch_id,
             bbr.bank_id,
             bbr.currency_code
        INTO pon_bank_acct_id,
             pov_account_name,
             pod_start_date,
             pon_branch_id,
             pon_bank_id,
             pov_currency
        FROM iby_ext_bank_accounts   bbr,
             iby_pmt_instr_uses_all  piu,
             iby_external_payees_all iepa,
             ap_suppliers            asa
       WHERE piu.payment_flow = 'DISBURSEMENTS'
         AND piu.instrument_type = 'BANKACCOUNT'
         AND piu.instrument_id = bbr.ext_bank_account_id
         AND piu.ext_pmt_party_id = iepa.ext_payee_id
         AND iepa.payee_party_id = asa.party_id
         AND iepa.supplier_site_id = pin_vendor_site_id
         AND iepa.org_id = pin_org_id
         AND bbr.bank_account_num = piv_bank_acct_num
         AND asa.segment1 = piv_vendor_num;

    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        l_record_cnt := 2;
        print_log_message('In No Data found of account exists check' ||
                          SQLERRM);
      WHEN OTHERS THEN
        l_record_cnt := 2;
        print_log_message('In When others of account exists check' ||
                          SQLERRM);
    END;

    IF l_record_cnt = 2 THEN
      pon_error_cnt := 2;
    END IF;

    print_log_message(' -  PROCEDURE : validate_account_int  - ');
  EXCEPTION
    WHEN OTHERS THEN
      g_retcode     := 2;
      pon_error_cnt := 2;
      g_errbuff     := 'Failed while validating whether account already exists.';
      print_log_message('In Exception of validate_account_int. Error: ' ||
                        SQLERRM);
  END validate_account_int;

  --
  -- =============================================================================
  -- Procedure: validate_int_country
  -- =============================================================================
  --   This procedure validates if Country exists
  -- =============================================================================
  --  Input Parameters :
  --  piv_country          : Country

  --  Output Parameters    :
  --  pon_error_cnt        : Return Error Count
  --  pov_country_code     : Return Country Code
  -- -----------------------------------------------------------------------------
  --

  PROCEDURE validate_int_country(piv_country      IN VARCHAR2,
                                 pov_country_code OUT VARCHAR2,
                                 pon_error_cnt    OUT NUMBER) IS

    l_record_cnt NUMBER := 0;

  BEGIN

    xxetn_debug_pkg.add_debug(' +  PROCEDURE : validate_int_country  ');

    pov_country_code := NULL;

    BEGIN
      SELECT ftv.territory_code
        INTO pov_country_code
        FROM fnd_territories_vl ftv
       WHERE UPPER(ftv.territory_short_name) = UPPER(piv_country)
         AND ftv.obsolete_flag = 'N';

    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        l_record_cnt := 2;
        print_log_message('In No Data found of Country exists check');
      WHEN OTHERS THEN
        l_record_cnt := 2;
        print_log_message('In When others of Country exists check' ||
                          SQLERRM);
    END;

    IF l_record_cnt = 2 THEN
      pon_error_cnt := 2;
    ELSE
      pon_error_cnt := 0;
    END IF;

    print_log_message(' -  PROCEDURE : validate_int_country  - ');
  EXCEPTION
    WHEN OTHERS THEN
      g_retcode     := 2;
      pon_error_cnt := 2;
      g_errbuff     := 'Failed while validating whether Country exists.';
      print_log_message('In Exception of validate_int_country. Error: ' ||
                        SQLERRM);
  END validate_int_country;

  --
  -- =============================================================================
  -- Procedure: validate_int_accts
  -- =============================================================================
  --   This procedure is used to validate Supplier Intermediary Accounts Details
  -- =============================================================================
  --  Input Parameters :
  --    None
  --  Output Parameters :
  --    None
  -- -----------------------------------------------------------------------------
  --
  PROCEDURE validate_int_accts

   IS

    l_value_out      VARCHAR2(50);
    l_log_ret_status VARCHAR2(50);
    l_log_err_msg    VARCHAR2(2000);

    l_msg_count     NUMBER;
    l_error_cnt     NUMBER;
    l_error_flag    VARCHAR2(10);
    l_msg_data      VARCHAR2(2000);
    l_return_status VARCHAR2(200);

    l_err_code   VARCHAR2(40);
    l_err_msg    VARCHAR2(2000);
    l_ret_status VARCHAR2(50);
    l_site       xxetn_map_unit.site%TYPE;

    --Cursor to select new records from supplier intermediary staging table
    CURSOR validate_int_accts_cur IS
      SELECT xsias.*
        FROM xxap_supplier_int_accts_stg xsias
       WHERE xsias.request_id = g_request_id
       ORDER BY xsias.interface_txn_id;

  BEGIN
    -- Initialize global variables for log_errors
    g_source_table    := g_int_accts_t;
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

    FOR validate_int_accts_rec IN validate_int_accts_cur LOOP

      BEGIN

        -- Initialize loop variables
        l_error_cnt      := 0;
        l_err_code       := NULL;
        l_err_msg        := NULL;
        l_log_ret_status := NULL;
        l_log_err_msg    := NULL;
        l_error_flag     := 'N';
        xxetn_debug_pkg.add_debug('validate Supplier Intermediary Acct Record: ' ||
                                  validate_int_accts_rec.interface_txn_id);

        g_intf_staging_id := validate_int_accts_rec.interface_txn_id;

        --procedure to check mandatory values are not missing
        mandatory_check_int_accts(validate_int_accts_rec.supplier_num,
                                  validate_int_accts_rec.operating_unit,
                                  validate_int_accts_rec.supplier_site_code,
                                  validate_int_accts_rec.supplier_account,
                                  l_error_cnt);

        IF l_error_cnt > 0 THEN
          l_error_flag := g_yes;
        END IF;

        /** Validate Intermediary Account Country **/
        IF validate_int_accts_rec.int_country IS NOT NULL THEN

          -- procedure to validate Intermediary Account Country
          validate_int_country(piv_country      => validate_int_accts_rec.int_country,
                               pov_country_code => validate_int_accts_rec.int_country_code,
                               pon_error_cnt    => l_error_cnt);

          IF l_error_cnt > 0 THEN
            l_error_flag := g_yes;
            l_err_code   := 'ETN_AP_INVALID_COUNTRY';
            l_err_msg    := 'Error: Country is not Valid';

            log_errors(pov_return_status       => l_log_ret_status -- OUT
                      ,
                       pov_error_msg           => l_log_err_msg -- OUT
                      ,
                       piv_source_column_name  => 'INT_COUNTRY',
                       piv_source_column_value => validate_int_accts_rec.int_country,
                       piv_error_type          => g_err_val,
                       piv_error_code          => l_err_code,
                       piv_error_message       => l_err_msg);
          END IF;

        END IF;

        /** Validate Vendor/Supplier Number **/
        IF validate_int_accts_rec.supplier_num IS NOT NULL THEN

          -- procedure to validate Vendor/Supplier Number
          validate_vendor(piv_vendor_num => validate_int_accts_rec.supplier_num,
                          pon_vendor_id  => validate_int_accts_rec.supplier_id,
                          pon_error_cnt  => l_error_cnt);

          IF l_error_cnt > 0 THEN
            l_error_flag := g_yes;
            l_err_code   := 'ETN_AP_INVALID_VENDOR';
            l_err_msg    := 'Error: Vendor is not Valid';

            log_errors(pov_return_status       => l_log_ret_status -- OUT
                      ,
                       pov_error_msg           => l_log_err_msg -- OUT
                      ,
                       piv_source_column_name  => 'SUPPLIER_NUM',
                       piv_source_column_value => validate_int_accts_rec.supplier_num,
                       piv_error_type          => g_err_val,
                       piv_error_code          => l_err_code,
                       piv_error_message       => l_err_msg);
          END IF;

        END IF;

        /** Validate Operating Unit **/
        l_site := NULL;
        IF validate_int_accts_rec.supplier_num IS NOT NULL AND
           validate_int_accts_rec.supplier_site_code IS NOT NULL AND
           validate_int_accts_rec.operating_unit IS NOT NULL THEN

          -- Fetching Plant# or Site# from Corresponding Site record
          BEGIN
            SELECT xsss.leg_accts_pay_code_segment1
              INTO l_site
              FROM xxap_supplier_sites_stg xsss
             WHERE xsss.leg_vendor_number =
                   validate_int_accts_rec.supplier_num
               AND xsss.leg_vendor_site_code =
                   validate_int_accts_rec.supplier_site_code
               AND xsss.leg_operating_unit_name =
                   validate_int_accts_rec.operating_unit;
          EXCEPTION
            WHEN OTHERS THEN
              l_error_flag := g_yes;
              log_errors(pov_return_status       => l_log_ret_status -- OUT
                        ,
                         pov_error_msg           => l_log_err_msg -- OUT
                        ,
                         piv_source_column_name  => 'SUPPLIER_NUM' || '~' ||
                                                    'SUPPLIER_SITE_CODE' || '~' ||
                                                    'OPERATING_UNIT',
                         piv_source_column_value => validate_int_accts_rec.supplier_num || '~' ||
                                                    validate_int_accts_rec.supplier_site_code || '~' ||
                                                    validate_int_accts_rec.operating_unit,
                         piv_error_type          => g_err_val,
                         piv_error_code          => 'ETN_AP_ACCT_PAY_CODE_SEGMENT1_ERROR',
                         piv_error_message       => 'Error : Acct Pay Code Segment1 is invalid for given Vendor Site Code.Error: ' ||
                                                    SQLERRM);
          END;

        END IF;

        -- If Site value is found
        IF l_site IS NOT NULL THEN
          validate_operating_unit(piv_site           => l_site,
                                  pov_operating_unit => validate_int_accts_rec.operating_unit_r12,
                                  pon_org_id         => validate_int_accts_rec.org_id,
                                  pon_error_cnt      => l_error_cnt);

          IF l_error_cnt > 0 THEN
            l_error_flag := g_yes;
            l_err_code   := 'ETN_AP_INVALID_PLANT_SITE';
            l_err_msg    := 'Error: R12 OU does not exist in ETN Map Unit table for given site or Operating unit does not exist in R12.';

            log_errors(pov_return_status       => l_log_ret_status -- OUT
                      ,
                       pov_error_msg           => l_log_err_msg -- OUT
                      ,
                       piv_source_column_name  => 'PLANT_SITE',
                       piv_source_column_value => l_site,
                       piv_error_type          => g_err_val,
                       piv_error_code          => l_err_code,
                       piv_error_message       => l_err_msg);
          END IF;
        END IF;

        /** Validate Vendor Site Code **/
        IF validate_int_accts_rec.supplier_site_code IS NOT NULL THEN

          -- procedure to validate Vendor Site Code
          validate_vendor_site(piv_vendor_num       => validate_int_accts_rec.supplier_num,
                               piv_vendor_site_Code => validate_int_accts_rec.supplier_site_code,
                               pin_org_id           => validate_int_accts_rec.org_id,
                               pon_vendor_site_id   => validate_int_accts_rec.supplier_site_id,
                               pon_error_cnt        => l_error_cnt);

          IF l_error_cnt > 0 THEN
            l_error_flag := g_yes;
            l_err_code   := 'ETN_AP_INVALID_VENDOR_SITE';
            l_err_msg    := 'Error: Vendor Site Code is not Valid';

            log_errors(pov_return_status       => l_log_ret_status -- OUT
                      ,
                       pov_error_msg           => l_log_err_msg -- OUT
                      ,
                       piv_source_column_name  => 'SUPPLIER_SITE_CODE',
                       piv_source_column_value => validate_int_accts_rec.supplier_site_code,
                       piv_error_type          => g_err_val,
                       piv_error_code          => l_err_code,
                       piv_error_message       => l_err_msg);
          END IF;

        END IF;

        /** Validate Supplier Bank Account **/
        IF validate_int_accts_rec.supplier_account IS NOT NULL AND
           validate_int_accts_rec.supplier_site_id IS NOT NULL THEN

          validate_account_int(piv_bank_acct_num  => validate_int_accts_rec.supplier_account,
                               piv_vendor_num     => validate_int_accts_rec.supplier_num,
                               pin_vendor_site_id => validate_int_accts_rec.supplier_site_id,
                               pin_org_id         => validate_int_accts_rec.org_id,
                               pon_bank_acct_id   => validate_int_accts_rec.supplier_account_id,
                               pov_account_name   => validate_int_accts_rec.supplier_account_name,
                               pod_start_date     => validate_int_accts_rec.sup_acct_start_date,
                               pon_branch_id      => validate_int_accts_rec.sup_acct_branch_id,
                               pon_bank_id        => validate_int_accts_rec.sup_acct_bank_id,
                               pov_currency       => validate_int_accts_rec.sup_acct_currency,
                               pon_error_cnt      => l_error_cnt);

          IF l_error_cnt > 0 THEN
            l_error_flag := g_yes;
            l_err_code   := 'ETN_AP_INVALID_SUPPLIER_ACCOUNT';
            l_err_msg    := 'Error: Supplier Account is not Valid';

            log_errors(pov_return_status       => l_log_ret_status -- OUT
                      ,
                       pov_error_msg           => l_log_err_msg -- OUT
                      ,
                       piv_source_column_name  => 'SUPPLIER_ACCOUNT',
                       piv_source_column_value => validate_int_accts_rec.supplier_account,
                       piv_error_type          => g_err_val,
                       piv_error_code          => l_err_code,
                       piv_error_message       => l_err_msg);
          END IF;

        END IF;

        --Update staging table with the validation status as 'V' or 'E'
        UPDATE xxap_supplier_int_accts_stg xsias
           SET xsias.process_flag           = DECODE(l_error_flag,
                                                     g_yes,
                                                     g_error,
                                                     g_validated),
               xsias.error_type             = DECODE(l_error_flag,
                                                     g_yes,
                                                     g_err_val,
                                                     NULL),
               xsias.last_update_date       = SYSDATE,
               xsias.last_updated_by        = g_user_id,
               xsias.last_update_login      = g_login_id,
               xsias.program_application_id = g_prog_appl_id,
               xsias.program_id             = g_conc_program_id,
               xsias.program_update_date    = SYSDATE,
               xsias.request_id             = g_request_id,
               xsias.supplier_id            = validate_int_accts_rec.supplier_id,
               xsias.operating_unit_r12     = validate_int_accts_rec.operating_unit_r12,
               xsias.org_id                 = validate_int_accts_rec.org_id,
               xsias.supplier_site_id       = validate_int_accts_rec.supplier_site_id,
               xsias.supplier_account_id    = validate_int_accts_rec.supplier_account_id,
               xsias.supplier_account_name  = validate_int_accts_rec.supplier_account_name,
               xsias.sup_acct_start_date    = validate_int_accts_rec.sup_acct_start_date,
               xsias.sup_acct_branch_id     = validate_int_accts_rec.sup_acct_branch_id,
               xsias.sup_acct_bank_id       = validate_int_accts_rec.sup_acct_bank_id,
               xsias.sup_acct_currency      = validate_int_accts_rec.sup_acct_currency,
               xsias.int_country_code       = validate_int_accts_rec.int_country_code
         WHERE xsias.interface_txn_id =
               validate_int_accts_rec.interface_txn_id;

        IF (l_error_flag = g_yes) THEN
          g_retcode := 1;
        END IF;

        g_intf_staging_id := NULL;
        g_src_keyname1    := NULL;
        g_src_keyvalue1   := NULL;
        g_src_keyname2    := NULL;
        g_src_keyvalue2   := NULL;
        g_src_keyname3    := NULL;
        g_src_keyvalue3   := NULL;

      END;

    END LOOP;

    COMMIT;

    xxetn_debug_pkg.add_debug('-   PROCEDURE : validate_int_accts for batch id = ' ||
                              g_new_batch_id || ' - ');
  EXCEPTION
    WHEN OTHERS THEN
      g_retcode := 2;
      g_errbuff := 'Failed while vaildating IBAN and Intermediary Accounts';
      print_log_message('In validate_int_accts when others' || SQLERRM);
  END validate_int_accts;

  --
  -- ========================
  -- Procedure: create_banks
  -- =============================================================================
  --   This procedure create_banks
  -- =============================================================================
  --  Input Parameters :
  --    None
  --  Output Parameters :
  --    None
  -- -----------------------------------------------------------------------------
  --
  PROCEDURE create_banks IS

    l_status_flag       VARCHAR2(1);
    l_error_message     VARCHAR2(500);
    l_return_status_out VARCHAR2(1);
    l_msg_count_out     NUMBER;
    l_msg_data_out      VARCHAR2(1000);
    l_msg_index_out     NUMBER;

    l_bank_id             NUMBER;
    l_location_id         NUMBER;
    l_party_site_id       NUMBER;
    l_party_site_number   NUMBER;
    l_org_contact_id      NUMBER;
    l_org_party_id        NUMBER;
    l_email_cont_point_id NUMBER;
    l_phone_cont_point_id NUMBER;

    l_bank_msg_data       VARCHAR2(2000);
    l_loc_msg_data        VARCHAR2(2000);
    l_party_site_msg_data VARCHAR2(2000);
    l_org_cont_msg_data   VARCHAR2(2000);
    l_phone_cont_msg_data VARCHAR2(2000);
    l_email_cont_msg_data VARCHAR2(2000);

    l_bank_ret_status       VARCHAR2(50);
    l_loc_ret_status        VARCHAR2(50);
    l_site_ret_status       VARCHAR2(50);
    l_state_ret_status      VARCHAR2(50);
    l_upd_ret_status        VARCHAR2(50);
    l_org_cont_ret_status   VARCHAR2(50);
    l_phone_cont_ret_status VARCHAR2(50);
    l_email_cont_ret_status VARCHAR2(50);

    l_log_ret_status VARCHAR2(50);
    l_log_err_msg    VARCHAR2(2000);

    l_retcode   VARCHAR2(1);
    l_err_code  VARCHAR2(40);
    l_err_msg   VARCHAR2(2000);
    l_msg_count NUMBER;

    l_extbank_rec_type iby_ext_bankacct_pub.extbank_rec_type;
    l_result_rec       iby_fndcpt_common_pub.result_rec_type;

    CURSOR create_banks_cur IS
      SELECT *
        FROM xxap_supplier_banks_stg xsbs
       WHERE xsbs.process_flag = g_validated
         AND xsbs.batch_id = g_new_batch_id;

  BEGIN
    -- Initialize global variables for log_errors
    g_source_table    := g_bank_t;
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

    FOR create_banks_rec IN create_banks_cur LOOP
      BEGIN

        SAVEPOINT start_bank;

        l_bank_id           := NULL;
        l_location_id       := NULL;
        l_party_site_id     := NULL;
        l_party_site_number := NULL;
        l_org_contact_id    := NULL;
        l_org_party_id      := NULL;

        l_bank_msg_data       := NULL;
        l_loc_msg_data        := NULL;
        l_party_site_msg_data := NULL;
        l_org_cont_msg_data   := NULL;

        l_bank_ret_status     := NULL;
        l_loc_ret_status      := NULL;
        l_site_ret_status     := NULL;
        l_state_ret_status    := NULL;
        l_upd_ret_status      := NULL;
        l_org_cont_ret_status := NULL;
        l_log_ret_status      := NULL;

        l_retcode   := NULL;
        l_msg_count := NULL;
        l_err_code  := NULL;
        l_err_msg   := NULL;
        xxetn_debug_pkg.add_debug('Import bank record : ' ||
                                  create_banks_rec.leg_bank_name || ', ' ||
                                  create_banks_rec.leg_country || ', ' ||
                                  create_banks_rec.interface_txn_id);

        -- Assign global variables for log_errors
        g_intf_staging_id := create_banks_rec.interface_txn_id;
        g_src_keyname1    := 'LEG_BANK_NAME';
        g_src_keyvalue1   := create_banks_rec.leg_bank_name;
        g_src_keyname2    := 'LEG_COUNTRY';
        g_src_keyvalue2   := create_banks_rec.leg_country;

        --Assign staging table values to the bank record type to be passed in the API
        l_extbank_rec_type.object_version_number := 1.0;
        l_extbank_rec_type.bank_name             := create_banks_rec.leg_bank_name;
        l_extbank_rec_type.bank_number           := create_banks_rec.leg_bank_number;
        --   l_extbank_rec_type.bank_branch_id          := create_banks_rec.leg_bank_branch_id; --added
        l_extbank_rec_type.institution_type := create_banks_rec.leg_bank_institution_type;
        l_extbank_rec_type.country_code     := create_banks_rec.leg_country;
        --l_extbank_rec_type.description             := create_banks_rec.leg_description;
        l_extbank_rec_type.bank_alt_name := create_banks_rec.leg_bank_name_alt;

        --Call API to create external banks
        iby_ext_bankacct_pub.create_ext_bank(p_api_version   => 1.0,
                                             p_init_msg_list => fnd_api.g_true,
                                             p_ext_bank_rec  => l_extbank_rec_type,
                                             x_bank_id       => l_bank_id,
                                             x_return_status => l_bank_ret_status,
                                             x_msg_count     => l_msg_count,
                                             x_msg_data      => l_bank_msg_data,
                                             x_response      => l_result_rec);

        --if API return status is not 'S'
        IF l_bank_ret_status <> fnd_api.g_ret_sts_success THEN
          g_retcode  := 1;
          l_retcode  := g_error;
          l_err_code := 'ETN_AP_BANK_IMPORT_ERROR';
          l_err_msg  := 'Error : Supplier Bank creation failed.';

          IF l_msg_count > 0 THEN
            FOR i IN 1 .. l_msg_count LOOP
              l_bank_msg_data := fnd_msg_pub.get(p_msg_index => i,
                                                 p_encoded   => fnd_api.g_false);

              log_errors(pov_return_status       => l_log_ret_status -- OUT
                        ,
                         pov_error_msg           => l_log_err_msg -- OUT
                        ,
                         piv_source_column_name  => NULL,
                         piv_source_column_value => NULL,
                         piv_error_type          => g_err_imp,
                         piv_error_code          => l_err_code,
                         piv_error_message       => l_err_msg ||
                                                    l_bank_msg_data);

            END LOOP;
          END IF;
        ELSE
          COMMIT;
          l_retcode := g_success;
        END IF; -- IF l_bank_ret_status = fnd_api.g_ret_sts_success

        IF l_retcode = g_success THEN
          --Update process_flag to 'C' in case of API Success
          UPDATE xxap_supplier_banks_stg
             SET bank_party_id          = l_bank_id,
                 process_flag           = g_converted,
                 run_sequence_id        = g_run_seq_id,
                 last_updated_date      = SYSDATE,
                 last_updated_by        = g_user_id,
                 last_update_login      = g_login_id,
                 program_application_id = g_prog_appl_id,
                 program_id             = g_conc_program_id,
                 program_update_date    = SYSDATE,
                 request_id             = g_request_id
           WHERE interface_txn_id = create_banks_rec.interface_txn_id;
        ELSE
          --Update process_flag to 'E' in case of API Failure
          UPDATE xxap_supplier_banks_stg
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
           WHERE interface_txn_id = create_banks_rec.interface_txn_id;
        END IF;
        COMMIT;
      EXCEPTION
        WHEN OTHERS THEN
          ROLLBACK TO start_bank;
          g_retcode  := 1;
          l_err_code := 'ETN_AP_BANK_IMPORT_ERROR';
          l_err_msg  := 'Error : Exception in Supplier Bank Import Loop. ' ||
                        SUBSTR(SQLERRM, 1, 240);
          print_log_message(l_err_msg);

          log_errors(pov_return_status       => l_log_ret_status -- OUT
                    ,
                     pov_error_msg           => l_err_msg -- OUT
                    ,
                     piv_source_column_name  => NULL,
                     piv_source_column_value => NULL,
                     piv_error_type          => g_err_imp,
                     piv_error_code          => l_err_code,
                     piv_error_message       => l_err_msg);

      END;
    END LOOP;
  EXCEPTION
    WHEN OTHERS THEN
      g_retcode  := 1;
      l_err_code := 'ETN_AP_BANK_IMPORT_ERROR';
      l_err_msg  := 'Error : Exception in Supplier Bank Import Procedure. ' ||
                    SUBSTR(SQLERRM, 1, 240);
      print_log_message(l_err_msg);
  END create_banks;

  --
  -- ========================
  -- Procedure: create_branches
  -- =============================================================================
  --   This procedure create_branches
  -- =============================================================================
  --  Input Parameters :
  --    None
  --  Output Parameters :
  --    None
  -- -----------------------------------------------------------------------------
  --
  PROCEDURE create_branches IS

    l_status_flag       VARCHAR2(1);
    l_error_message     VARCHAR2(500);
    l_return_status_out VARCHAR2(1);
    l_msg_count_out     NUMBER;
    l_msg_data_out      VARCHAR2(1000);
    l_msg_index_out     NUMBER;

    l_bank_id             NUMBER;
    l_branch_id           NUMBER;
    l_location_id         NUMBER;
    l_party_site_id       NUMBER;
    l_party_site_number   NUMBER;
    l_org_contact_id      NUMBER;
    l_org_party_id        NUMBER;
    l_email_cont_point_id NUMBER;
    l_phone_cont_point_id NUMBER;

    l_branch_msg_data     VARCHAR2(2000);
    l_loc_msg_data        VARCHAR2(2000);
    l_party_site_msg_data VARCHAR2(2000);
    l_org_cont_msg_data   VARCHAR2(2000);
    l_phone_cont_msg_data VARCHAR2(2000);
    l_email_cont_msg_data VARCHAR2(2000);

    l_branch_ret_status     VARCHAR2(50);
    l_loc_ret_status        VARCHAR2(50);
    l_site_ret_status       VARCHAR2(50);
    l_state_ret_status      VARCHAR2(50);
    l_upd_ret_status        VARCHAR2(50);
    l_org_cont_ret_status   VARCHAR2(50);
    l_phone_cont_ret_status VARCHAR2(50);
    l_email_cont_ret_status VARCHAR2(50);

    l_log_ret_status VARCHAR2(50);
    l_log_err_msg    VARCHAR2(2000);
    l_loc_msg_count  NUMBER;

    l_retcode   VARCHAR2(1);
    l_err_code  VARCHAR2(40);
    l_err_msg   VARCHAR2(2000);
    l_msg_count NUMBER;

    l_extbranch_rec     iby_ext_bankacct_pub.extbankbranch_rec_type;
    l_result_rec        iby_fndcpt_common_pub.result_rec_type;
    l_br_location_rec   hz_location_v2pub.location_rec_type;
    l_br_party_site_rec hz_party_site_v2pub.party_site_rec_type;

    CURSOR create_branches_cur IS
      SELECT *
        FROM xxap_supplier_branches_stg xsbs
       WHERE xsbs.process_flag = g_validated
         AND xsbs.batch_id = g_new_batch_id;

  BEGIN
    -- Initialize global variables for log_errors
    g_source_table    := g_branch_t;
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

    FOR create_branches_rec IN create_branches_cur LOOP
      BEGIN

        SAVEPOINT start_branch;

        l_bank_id           := NULL;
        l_location_id       := NULL;
        l_party_site_id     := NULL;
        l_party_site_number := NULL;
        l_org_contact_id    := NULL;
        l_org_party_id      := NULL;

        l_branch_msg_data     := NULL;
        l_loc_msg_data        := NULL;
        l_party_site_msg_data := NULL;
        l_org_cont_msg_data   := NULL;

        l_branch_ret_status   := NULL;
        l_loc_ret_status      := NULL;
        l_site_ret_status     := NULL;
        l_state_ret_status    := NULL;
        l_upd_ret_status      := NULL;
        l_org_cont_ret_status := NULL;
        l_log_ret_status      := NULL;

        l_retcode   := g_success;
        l_msg_count := NULL;
        l_err_code  := NULL;
        l_err_msg   := NULL;
        xxetn_debug_pkg.add_debug('Import branch record : ' ||
                                         create_branches_rec.leg_bank_name || ', ' ||
                                  create_branches_rec.leg_bank_branch_name || ', ' ||
                                  create_branches_rec.leg_country || ', ' ||
                                  create_branches_rec.interface_txn_id);
        print_log_message('Import branch record : ' ||
                                  create_branches_rec.leg_bank_name || ', ' ||
                          create_branches_rec.leg_bank_branch_name || ', ' ||
                          create_branches_rec.leg_country || ', ' ||
                          create_branches_rec.interface_txn_id);
        --derive bank ID from the bank name
        BEGIN

            /* This code only for MOCK3
            SELECT (bank_party_id)
              INTO l_bank_id
              FROM iby_ext_banks_v x
             WHERE UPPER(bank_name) = UPPER(create_branches_rec.leg_bank_name)
               AND home_country = create_branches_rec.leg_country
               AND bank_party_id in (
                 Select h1.party_id
                FROM HZ_PARTIES h1
               Where UPPER(h1.party_name) = UPPER(x.bank_name)
                 and object_version_number =
                     (Select max(object_version_number)
                        FROM HZ_PARTIES h2
                       WHERE UPPER(h1.party_name) = UPPER(h2.party_name)));*/

            SELECT bank_party_id
            INTO l_bank_id
            FROM iby_ext_banks_v
           WHERE UPPER(bank_name) =
                 UPPER(create_branches_rec.leg_bank_name)
             AND nvl(UPPER(Bank_number),'#') = nvl(UPPER(create_branches_rec.leg_bank_number),'#')
             AND home_country = create_branches_rec.leg_country;

        EXCEPTION
          WHEN NO_DATA_FOUND THEN
            l_branch_ret_status := 'E';
            xxetn_debug_pkg.add_debug('Given bank_name does not exist ' ||
                                      'in the system : ' ||
                                      create_branches_rec.leg_bank_name ||
                                      ' and country ' ||
                                      create_branches_rec.leg_country);
            l_err_msg := l_err_msg || 'Given bank does not exist ' ||
                         'in the system : ' ||
                         create_branches_rec.leg_bank_name ||
                         ' and country ' || create_branches_rec.leg_country;
            print_log_message(l_err_msg);

            log_errors(pov_return_status       => l_log_ret_status -- OUT
                      ,
                       pov_error_msg           => l_log_err_msg -- OUT
                      ,
                       piv_source_column_name  => 'LEG_BANK_NAME',
                       piv_source_column_value => create_branches_rec.leg_bank_name,
                       piv_error_type          => g_err_imp,
                       piv_error_code          => l_err_code,
                       piv_error_message       => l_err_msg,
                       piv_severity            => NULL,
                       piv_proposed_solution   => NULL);

          WHEN OTHERS THEN
            l_branch_ret_status := 'E';
            xxetn_debug_pkg.add_debug('Error Occurred while deriving Bank_party_id : ' ||
                                      create_branches_rec.bank_party_id ||
                                      SQLCODE || ',' ||
                                      ' and error message is : ' ||
                                      SQLERRM);
            l_err_msg := l_err_msg || 'Oracle Error  ' || SQLERRM;
            print_log_message(l_err_msg);

            log_errors(pov_return_status       => l_log_ret_status -- OUT
                      ,
                       pov_error_msg           => l_log_err_msg -- OUT
                      ,
                       piv_source_column_name  => 'LEG_BANK_NAME',
                       piv_source_column_value => create_branches_rec.leg_bank_name,
                       piv_error_type          => g_err_imp,
                       piv_error_code          => l_err_code,
                       piv_error_message       => l_err_msg,
                       piv_severity            => NULL,
                       piv_proposed_solution   => NULL);
        END;

        print_log_message('After bank party ID derivation Bank ID:- ' || l_bank_id);
        -- Assign global variables for log_errors
        g_intf_staging_id := create_branches_rec.interface_txn_id;
        g_src_keyname1    := 'LEG_BANK_BRANCH_NAME';
        g_src_keyvalue1   := create_branches_rec.leg_bank_branch_name;
        g_src_keyname2    := 'LEG_COUNTRY';
        g_src_keyvalue2   := create_branches_rec.leg_country;

        --Assign staging table values to the branch record type to be passed in the API

        l_extbranch_rec.bch_object_version_number := 1.0;
        l_extbranch_rec.branch_name               := create_branches_rec.leg_bank_branch_name;
        l_extbranch_rec.bank_party_id             := l_bank_id;-- create_branches_rec.bank_party_id;
        l_extbranch_rec.branch_number             := create_branches_rec.leg_branch_number;
        l_extbranch_rec.branch_type               := create_branches_rec.leg_bank_branch_type;
        l_extbranch_rec.bic                       := create_branches_rec.leg_attribute5;
        l_extbranch_rec.description               := create_branches_rec.leg_description;
        l_extbranch_rec.rfc_identifier            := create_branches_rec.leg_rfc_identifier;
        l_extbranch_rec.alternate_branch_name     := create_branches_rec.leg_bank_branch_name_alt;

        print_log_message('l_branch_ret_status:' || l_branch_ret_status);
        IF l_bank_id IS NOT NULL THEN
          print_log_message('Inside branch creation API');
          --Call API to create external branches
          iby_ext_bankacct_pub.create_ext_bank_branch(p_api_version         => 1.0,
                                                      p_init_msg_list       => fnd_api.g_true,
                                                      p_ext_bank_branch_rec => l_extbranch_rec,
                                                      x_branch_id           => l_branch_id,
                                                      x_return_status       => l_branch_ret_status,
                                                      x_msg_count           => l_msg_count,
                                                      x_msg_data            => l_branch_msg_data,
                                                      x_response            => l_result_rec);
          xxetn_debug_pkg.add_debug('l_branch_ret_status:' ||
                                    l_branch_ret_status);
          print_log_message('l_branch_ret_status:' || l_branch_ret_status);

          -- If branch record was successfully created
          IF l_branch_ret_status = fnd_api.g_ret_sts_success THEN
            IF create_branches_rec.leg_country IS NOT NULL AND
               create_branches_rec.leg_address_line1 IS NOT NULL THEN

              l_br_location_rec.orig_system_reference := l_branch_id;
              l_br_location_rec.country               := create_branches_rec.leg_country;
              l_br_location_rec.address1              := create_branches_rec.leg_address_line1;
              l_br_location_rec.address2              := create_branches_rec.leg_address_line2;
              l_br_location_rec.address3              := create_branches_rec.leg_address_line3;
              l_br_location_rec.address4              := create_branches_rec.leg_address_line4;
              l_br_location_rec.city                  := create_branches_rec.leg_city;
              l_br_location_rec.postal_code           := create_branches_rec.leg_zip;
              l_br_location_rec.state                 := create_branches_rec.leg_state;
              l_br_location_rec.province              := create_branches_rec.leg_province;
              l_br_location_rec.created_by_module     := 'CE';
              -- l_br_location_rec.county              :=
              --  create_branches_rec.county;
              hz_location_v2pub.create_location(p_init_msg_list => fnd_api.g_false,
                                                p_location_rec  => l_br_location_rec,
                                                x_location_id   => l_location_id,
                                                x_return_status => l_loc_ret_status,
                                                x_msg_count     => l_msg_count,
                                                x_msg_data      => l_loc_msg_data);

              xxetn_debug_pkg.add_debug('   API create_location : ' ||
                                        l_loc_ret_status || ',' ||
                                        l_location_id || ',' ||
                                        l_loc_msg_data);
              print_log_message('   API create_location : ' ||
                                l_loc_ret_status || ',' || l_location_id || ',' ||
                                l_loc_msg_data);
              -- If location record was successfully created
              IF l_loc_ret_status = fnd_api.g_ret_sts_success THEN
                -- Assign location to bank record in form of a party site
                l_br_party_site_rec.location_id              := l_location_id;
                l_br_party_site_rec.party_id                 := l_branch_id;
                l_br_party_site_rec.identifying_address_flag := g_yes;
                l_br_party_site_rec.orig_system_reference    := l_branch_id;
                l_br_party_site_rec.identifying_address_flag := g_yes;
                l_br_party_site_rec.status                   := 'A';
                l_br_party_site_rec.party_site_name          := create_branches_rec.leg_bank_branch_name ||
                                                                '_Site';
                l_br_party_site_rec.created_by_module        := 'CE';
                hz_bank_pub.create_bank_site(p_init_msg_list     => fnd_api.g_false,
                                             p_party_site_rec    => l_br_party_site_rec,
                                             x_party_site_id     => l_party_site_id,
                                             x_party_site_number => l_party_site_number,
                                             x_return_status     => l_site_ret_status,
                                             x_msg_count         => l_msg_count,
                                             x_msg_data          => l_party_site_msg_data);

                xxetn_debug_pkg.add_debug('   API create_party_site : ' ||
                                          l_site_ret_status || ',' ||
                                          l_party_site_id || ',' ||
                                          l_party_site_msg_data);
                print_log_message('   API create_party_site : ' ||
                                  l_site_ret_status || ',' ||
                                  l_party_site_id || ',' ||
                                  l_party_site_msg_data);
                -- if api error
                IF l_site_ret_status <> fnd_api.g_ret_sts_success THEN

                  l_retcode  := g_error;
                  l_err_code := 'ETN_AP_BRANCH_IMPORT_ERROR';
                  l_err_msg  := 'Error : Party site creation for branch address failed. ';
                  print_log_message(l_err_msg);

                  IF l_msg_count = 1 THEN
                    log_errors(pov_return_status       => l_log_ret_status -- OUT
                              ,
                               pov_error_msg           => l_log_err_msg -- OUT
                              ,
                               piv_source_column_name  => NULL,
                               piv_source_column_value => NULL,
                               piv_error_type          => g_err_imp,
                               piv_error_code          => l_err_code,
                               piv_error_message       => l_err_msg ||
                                                          l_party_site_msg_data,
                               piv_severity            => NULL,
                               piv_proposed_solution   => NULL);
                    --extract API errors
                  ELSIF l_msg_count > 1 THEN
                    FOR i IN 1 .. l_msg_count LOOP
                      l_party_site_msg_data := fnd_msg_pub.get(p_msg_index => i,
                                                               p_encoded   => fnd_api.g_false);

                      log_errors(pov_return_status       => l_log_ret_status -- OUT
                                ,
                                 pov_error_msg           => l_log_err_msg -- OUT
                                ,
                                 piv_source_column_name  => NULL,
                                 piv_source_column_value => NULL,
                                 piv_error_type          => g_err_imp,
                                 piv_error_code          => l_err_code,
                                 piv_error_message       => l_err_msg ||
                                                            l_party_site_msg_data,
                                 piv_severity            => NULL,
                                 piv_proposed_solution   => NULL);

                    END LOOP;
                  END IF;
                END IF;

                -- if api error
              ELSIF l_loc_ret_status <> fnd_api.g_ret_sts_success THEN

                l_retcode  := g_error;
                l_err_code := 'ETN_AP_BRANCH_IMPORT_ERROR';
                l_err_msg  := 'Error : Location creation for branch address failed.';
                print_log_message(l_err_msg);

                IF l_msg_count = 1 THEN

                  log_errors(pov_return_status       => l_log_ret_status -- OUT
                            ,
                             pov_error_msg           => l_log_err_msg -- OUT
                            ,
                             piv_source_column_name  => NULL,
                             piv_source_column_value => NULL,
                             piv_error_type          => g_err_imp,
                             piv_error_code          => l_err_code,
                             piv_error_message       => l_err_msg ||
                                                        l_loc_msg_data,
                             piv_severity            => NULL,
                             piv_proposed_solution   => NULL);
                  --extract API errors
                ELSIF l_msg_count > 1 THEN
                  FOR i IN 1 .. l_msg_count LOOP
                    l_loc_msg_data := fnd_msg_pub.get(p_msg_index => i,
                                                      p_encoded   => fnd_api.g_false);

                    log_errors(pov_return_status       => l_log_ret_status -- OUT
                              ,
                               pov_error_msg           => l_log_err_msg -- OUT
                              ,
                               piv_source_column_name  => NULL,
                               piv_source_column_value => NULL,
                               piv_error_type          => g_err_imp,
                               piv_error_code          => l_err_code,
                               piv_error_message       => l_err_msg ||
                                                          l_loc_msg_data,
                               piv_severity            => NULL,
                               piv_proposed_solution   => NULL);

                  END LOOP;
                END IF;

              END IF; -- IF l_loc_ret_status = fnd_api.g_ret_sts_success
            END IF; -- IF home country is not NULL

            --if branch creation API return status is not 'S'
          ELSIF l_branch_ret_status <> fnd_api.g_ret_sts_success THEN
            l_retcode  := g_error;
            l_err_code := 'ETN_AP_BRANCH_IMPORT_ERROR';
            l_err_msg  := 'Error : Supplier Branch creation failed.';
            print_log_message(l_err_msg);

            IF l_msg_count > 0 THEN
              FOR i IN 1 .. l_msg_count LOOP
                l_branch_msg_data := fnd_msg_pub.get(p_msg_index => i,
                                                     p_encoded   => fnd_api.g_false);

                log_errors(pov_return_status       => l_log_ret_status -- OUT
                          ,
                           pov_error_msg           => l_log_err_msg -- OUT
                          ,
                           piv_source_column_name  => NULL,
                           piv_source_column_value => NULL,
                           piv_error_type          => g_err_imp,
                           piv_error_code          => l_err_code,
                           piv_error_message       => l_err_msg ||
                                                      l_branch_msg_data);

              END LOOP;
            END IF;
          ELSE
            COMMIT;
            l_retcode := g_success;
          END IF; -- IF l_bank_ret_status = fnd_api.g_ret_sts_success

          IF l_retcode = g_success THEN
            --Update process_flag to 'C' in case of API Success
            UPDATE xxap_supplier_branches_stg
               SET bank_party_id          = l_bank_id,
                   branch_party_id        = l_branch_id,
                   process_flag           = g_converted,
                   run_sequence_id        = g_run_seq_id,
                   last_update_date       = SYSDATE,
                   last_updated_by        = g_user_id,
                   last_update_login      = g_login_id,
                   program_application_id = g_prog_appl_id,
                   program_id             = g_conc_program_id,
                   program_update_date    = SYSDATE,
                   request_id             = g_request_id
             WHERE interface_txn_id = create_branches_rec.interface_txn_id;
          ELSE
            print_log_message('Inside Else 1');
            g_retcode := 1;
            --Update process_flag to 'E' in case of API Failure
            UPDATE xxap_supplier_branches_stg
               SET process_flag           = g_error,
                   run_sequence_id        = g_run_seq_id,
                   error_type             = g_err_imp,
                   last_update_date       = SYSDATE,
                   last_updated_by        = g_user_id,
                   last_update_login      = g_login_id,
                   program_application_id = g_prog_appl_id,
                   program_id             = g_conc_program_id,
                   program_update_date    = SYSDATE,
                   request_id             = g_request_id
             WHERE interface_txn_id = create_branches_rec.interface_txn_id;
          END IF;
          COMMIT;
        ELSE
          print_log_message('Enside Else 2');
          g_retcode := 1;
          --Update process_flag to 'E' in case of API Failure
          UPDATE xxap_supplier_branches_stg
             SET process_flag           = g_error,
                 run_sequence_id        = g_run_seq_id,
                 error_type             = g_err_imp,
                 last_update_date       = SYSDATE,
                 last_updated_by        = g_user_id,
                 last_update_login      = g_login_id,
                 program_application_id = g_prog_appl_id,
                 program_id             = g_conc_program_id,
                 program_update_date    = SYSDATE,
                 request_id             = g_request_id
           WHERE interface_txn_id = create_branches_rec.interface_txn_id;
        END IF; -- if bank id does not exist
        COMMIT;
      EXCEPTION
        WHEN OTHERS THEN

          g_retcode  := 1;
          l_err_code := 'ETN_AP_BRANCH_IMPORT_ERROR';
          l_err_msg  := 'Error : Exception in Supplier Branch Import Loop. ' ||
                        SUBSTR(SQLERRM, 1, 240);
          print_log_message(l_err_msg);

          log_errors(pov_return_status       => l_log_ret_status -- OUT
                    ,
                     pov_error_msg           => l_err_msg -- OUT
                    ,
                     piv_source_column_name  => NULL,
                     piv_source_column_value => NULL,
                     piv_error_type          => g_err_imp,
                     piv_error_code          => l_err_code,
                     piv_error_message       => l_err_msg);
          ROLLBACK TO start_bank;
      END;
    END LOOP;
  EXCEPTION
    WHEN OTHERS THEN
      g_retcode  := 1;
      l_err_code := 'ETN_AP_BRANCH_IMPORT_ERROR';
      l_err_msg  := 'Error : Exception in Supplier Branch Import Procedure. ' ||
                    SUBSTR(SQLERRM, 1, 240);
      print_log_message(l_err_msg);
  END create_branches;

  --
  -- ========================
  -- Procedure: create_bank_accounts
  -- =============================================================================
  --   This procedure create_bank_accounts
  -- =============================================================================
  --  Input Parameters :
  --    None
  --  Output Parameters :
  --    None
  -- -----------------------------------------------------------------------------
  --
  PROCEDURE create_bank_accounts IS

    l_status_flag       VARCHAR2(1);
    l_error_message     VARCHAR2(500);
    l_return_status_out VARCHAR2(1);
    l_msg_count_out     NUMBER;
    l_msg_data_out      VARCHAR2(1000);
    l_msg_index_out     NUMBER;

    l_bank_id    NUMBER;
    l_branch_id  NUMBER;
    l_account_id NUMBER;

    l_account_msg_data    VARCHAR2(2000);
    l_loc_msg_data        VARCHAR2(2000);
    l_party_site_msg_data VARCHAR2(2000);

    l_account_ret_status VARCHAR2(50);
    l_loc_ret_status     VARCHAR2(50);
    l_site_ret_status    VARCHAR2(50);
    l_state_ret_status   VARCHAR2(50);
    l_upd_ret_status     VARCHAR2(50);

    l_log_ret_status VARCHAR2(50);
    l_log_err_msg    VARCHAR2(2000);
    l_loc_msg_count  NUMBER;

    l_retcode         VARCHAR2(1);
    l_err_code        VARCHAR2(40);
    l_err_msg         VARCHAR2(2000);
    l_payee_err_msg   VARCHAR2(2000);
    l_msg_count       NUMBER;
    l_payee_msg_count NUMBER;

    l_ext_bank_acct_rec        iby_ext_bankacct_pub.extbankacct_rec_type;
    l_instrument_rec           iby_fndcpt_setup_pub.pmtinstrument_rec_type;
    l_payee_rec                iby_disbursement_setup_pub.payeecontext_rec_type;
    l_assignment_attribs_rec   iby_fndcpt_setup_pub.pmtinstrassignment_rec_type;
    l_bank_acct_id             iby_ext_bank_accounts.ext_bank_account_id%TYPE;
    l_temp_ext_bank_acct_id    iby_ext_bank_accounts.ext_bank_account_id%TYPE;
    l_temp_ext_bank_acct_count NUMBER;
    l_bank_acct_count          NUMBER;
    l_ext_payee_id             NUMBER;
    l_assign_id                NUMBER;
    l_result_rec               iby_fndcpt_common_pub.result_rec_type;
    l_payee_result_rec         iby_fndcpt_common_pub.result_rec_type;
    l_priority                 NUMBER;
    l_bank_acct_priority       NUMBER;
    l_bank_acct_priority_count NUMBER;
    l_joint_acct_owner_id      NUMBER;
    l_payee_ret_status         VARCHAR2(50);

    -- variables for checking existing account
    l_acct_start_date         DATE;
    l_acct_end_date           DATE;
    l_checkacct_return_status VARCHAR2(10);
    l_checkacct_msg_count     NUMBER;
    l_checkacct_msg_data      VARCHAR2(2000);
    l_checkacct_response      IBY_FNDCPT_COMMON_PUB.Result_rec_type;

    -- variables for checking existing Account Owner
    l_acct_owner_ret_status VARCHAR2(10);
    l_acct_owner_msg_count  NUMBER;
    l_acct_owner_msg_data   VARCHAR2(2000);
    l_acct_owner_response   IBY_FNDCPT_COMMON_PUB.Result_rec_type;

    -- variables for creating Joint Account Owner
    l_jointacct_owner_ret_status VARCHAR2(10);
    l_jointacct_owner_msg_count  NUMBER;
    l_jointacct_owner_msg_data   VARCHAR2(2000);
    l_jointacct_owner_response   IBY_FNDCPT_COMMON_PUB.Result_rec_type;
    l_joint_owner_id             NUMBER;

    CURSOR create_accounts_cur IS
      SELECT *
        FROM xxap_supplier_bankaccnts_stg xsbs
       WHERE xsbs.process_flag = g_validated
         AND xsbs.batch_id = g_new_batch_id
       ORDER BY NVL(leg_primary_flag, 'A') desc;

  BEGIN
    -- Initialize global variables for log_errors
    g_source_table    := g_account_t;
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

    FOR create_accounts_rec IN create_accounts_cur LOOP
      BEGIN

        SAVEPOINT start_account;

        l_bank_id             := NULL;
        l_assign_id           := NULL;
        l_account_msg_data    := NULL;
        l_loc_msg_data        := NULL;
        l_party_site_msg_data := NULL;

        l_ext_payee_id             := NULL;
        l_temp_ext_bank_acct_id    := NULL;
        l_bank_acct_id             := NULL;
        l_temp_ext_bank_acct_count := NULL;
        l_bank_acct_count          := NULL;
        l_result_rec               := NULL;
        l_instrument_rec           := NULL;
        l_priority                 := NULL;
        l_bank_acct_priority       := NULL;
        l_bank_acct_priority_count := NULL;
        l_joint_acct_owner_id      := NULL;

        l_account_ret_status := 'S';
        l_payee_ret_status   := 'S';
        l_site_ret_status    := NULL;
        l_state_ret_status   := NULL;
        l_upd_ret_status     := NULL;
        l_log_ret_status     := NULL;

        l_retcode         := NULL;
        l_msg_count       := NULL;
        l_payee_msg_count := NULL;
        l_err_code        := NULL;
        l_err_msg         := NULL;
        l_payee_err_msg   := NULL;

        -- Check if Bank Account exist already
        l_acct_start_date         := NULL;
        l_acct_end_date           := NULL;
        l_checkacct_return_status := NULL;
        l_checkacct_msg_count     := NULL;
        l_checkacct_msg_data      := NULL;
        l_checkacct_response      := NULL;

        -- variables for checking existing Account Owner
        l_acct_owner_ret_status := 'S';
        l_acct_owner_msg_count  := NULL;
        l_acct_owner_msg_data   := NULL;

        -- variables for creating Joint Account Owner
        l_jointacct_owner_ret_status := 'S';
        l_jointacct_owner_msg_count  := NULL;
        l_jointacct_owner_msg_data   := NULL;
        l_joint_owner_id             := NULL;

        xxetn_debug_pkg.add_debug('Checking Existance of Bank Account : ' ||
                                  create_accounts_rec.leg_account_num || ', ' ||
                                  create_accounts_rec.leg_bank_name || ', ' ||
                                  create_accounts_rec.interface_txn_id);

        -- API to check Bank Account exist or not
        IBY_EXT_BANKACCT_PUB.check_ext_acct_exist(p_api_version   => 1.0,
                                                  p_init_msg_list => fnd_api.g_true,
                                                  p_bank_id       => create_accounts_rec.bank_id,
                                                  p_branch_id     => create_accounts_rec.branch_id,
                                                  p_acct_number   => create_accounts_rec.leg_account_num,
                                                  p_acct_name     => create_accounts_rec.leg_account_name,
                                                  p_currency      => create_accounts_rec.leg_currency,
                                                  p_country_code  => create_accounts_rec.country,
                                                  x_acct_id       => l_bank_acct_id,
                                                  x_start_date    => l_acct_start_date,
                                                  x_end_date      => l_acct_end_date,
                                                  x_return_status => l_checkacct_return_status,
                                                  x_msg_count     => l_checkacct_msg_count,
                                                  x_msg_data      => l_checkacct_msg_data,
                                                  x_response      => l_checkacct_response);

        xxetn_debug_pkg.add_debug('Existing Bank Account Id: ' ||
                                  l_bank_acct_id);

        -- If Bank Account does not exist
        IF l_bank_acct_id IS NULL THEN

          xxetn_debug_pkg.add_debug('Bank Account does not exist. Creating new Bank Account');
          xxetn_debug_pkg.add_debug('Import Account record : ' ||
                                    create_accounts_rec.leg_account_num || ', ' ||
                                    create_accounts_rec.leg_bank_name || ', ' ||
                                    create_accounts_rec.interface_txn_id);

          -- Assign global variables for log_errors
          g_intf_staging_id := create_accounts_rec.interface_txn_id;
          g_src_keyname1    := 'LEG_ACCOUNT_NUM';
          g_src_keyvalue1   := create_accounts_rec.leg_account_num;
          g_src_keyname2    := 'LEG_BANK_NAME';
          g_src_keyvalue2   := create_accounts_rec.leg_bank_name;

          --Assign staging table values to the account record type to be passed in the API
          l_ext_bank_acct_rec.object_version_number := 1.0;
          l_ext_bank_acct_rec.acct_owner_party_id   := create_accounts_rec.party_id;
          l_ext_bank_acct_rec.bank_account_num      := create_accounts_rec.leg_account_num;
          l_ext_bank_acct_rec.bank_account_name     := create_accounts_rec.leg_account_name;
          l_ext_bank_acct_rec.bank_id               := create_accounts_rec.bank_id;
          l_ext_bank_acct_rec.branch_id             := create_accounts_rec.branch_id;
          l_ext_bank_acct_rec.iban                  := create_accounts_rec.leg_iban;
          --l_ext_bank_acct_rec.start_date              := SYSDATE;
          l_ext_bank_acct_rec.start_date               := create_accounts_rec.creation_date;
          l_ext_bank_acct_rec.currency                 := create_accounts_rec.leg_currency;
          l_ext_bank_acct_rec.country_code             := create_accounts_rec.country;
          l_ext_bank_acct_rec.alternate_acct_name      := create_accounts_rec.leg_account_name_alt;
          l_ext_bank_acct_rec.acct_type                := UPPER(create_accounts_rec.leg_bank_account_type);
          l_ext_bank_acct_rec.check_digits             := create_accounts_rec.leg_check_digits;
          l_ext_bank_acct_rec.agency_location_code     := create_accounts_rec.leg_agency_loc_code;
          l_ext_bank_acct_rec.foreign_payment_use_flag := 'Y'; /*create_accounts_rec.leg_multi_currency_flag;*/

          --Call API to create external bank accounts
          xxetn_debug_pkg.add_debug('Calling Bank Account Creation API ');

          iby_ext_bankacct_pub.create_ext_bank_acct(p_api_version       => 1.0,
                                                    p_init_msg_list     => fnd_api.g_true,
                                                    p_ext_bank_acct_rec => l_ext_bank_acct_rec,
                                                    x_acct_id           => l_bank_acct_id,
                                                    x_return_status     => l_account_ret_status,
                                                    x_msg_count         => l_msg_count,
                                                    x_msg_data          => l_account_msg_data,
                                                    x_response          => l_result_rec);
          xxetn_debug_pkg.add_debug('l_account_ret_status:' ||
                                    l_account_ret_status);
          print_log_message('l_account_ret_status:' ||
                            l_account_ret_status);

        END IF; -- If Bank Account does not exist

        -- If account record was successfully created or already existed
        IF l_account_ret_status = fnd_api.g_ret_sts_success THEN

          -- Check if the new supplier/party is already an owner on the account.
          IBY_EXT_BANKACCT_PUB.check_bank_acct_owner(p_api_version         => 1.0,
                                                     p_init_msg_list       => fnd_api.g_true,
                                                     p_bank_acct_id        => l_bank_acct_id,
                                                     p_acct_owner_party_id => create_accounts_rec.party_id,
                                                     x_return_status       => l_acct_owner_ret_status,
                                                     x_msg_count           => l_acct_owner_msg_count,
                                                     x_msg_data            => l_acct_owner_msg_data,
                                                     x_response            => l_acct_owner_response);

          IF l_acct_owner_ret_status <> 'S' THEN

            xxetn_debug_pkg.add_debug('Creating Joint Account Owner for Party Id: ' ||
                                      create_accounts_rec.party_id);
            -- Create Joint Account Owner if not already existing
            IBY_EXT_BANKACCT_PUB.add_joint_account_owner(p_api_version         => 1.0,
                                                         p_init_msg_list       => fnd_api.g_true,
                                                         p_bank_account_id     => l_bank_acct_id,
                                                         p_acct_owner_party_id => create_accounts_rec.party_id,
                                                         x_joint_acct_owner_id => l_joint_owner_id,
                                                         x_return_status       => l_jointacct_owner_ret_status,
                                                         x_msg_count           => l_jointacct_owner_msg_count,
                                                         x_msg_data            => l_jointacct_owner_msg_data,
                                                         x_response            => l_jointacct_owner_response);

            xxetn_debug_pkg.add_debug('l_joint_owner_id: ' ||
                                      l_joint_owner_id);

            IF l_jointacct_owner_ret_status <> 'S' THEN

              l_err_code := 'ETN_AP_JOINT_ACCT_ERROR';
              l_err_msg  := 'Error : Error occured while trying to create Joint Account Owner. ';

              IF l_jointacct_owner_msg_count = 1 THEN
                log_errors(pov_return_status       => l_log_ret_status -- OUT
                          ,
                           pov_error_msg           => l_log_err_msg -- OUT
                          ,
                           piv_source_column_name  => NULL,
                           piv_source_column_value => NULL,
                           piv_error_type          => g_err_imp,
                           piv_error_code          => l_err_code,
                           piv_error_message       => l_err_msg ||
                                                      l_jointacct_owner_msg_data,
                           piv_severity            => NULL,
                           piv_proposed_solution   => NULL);

                --extract API errors
              ELSIF l_jointacct_owner_msg_count > 1 THEN
                FOR i IN 1 .. l_jointacct_owner_msg_count LOOP
                  l_jointacct_owner_msg_data := fnd_msg_pub.get(p_msg_index => i,
                                                                p_encoded   => fnd_api.g_false);

                  log_errors(pov_return_status       => l_log_ret_status -- OUT
                            ,
                             pov_error_msg           => l_log_err_msg -- OUT
                            ,
                             piv_source_column_name  => NULL,
                             piv_source_column_value => NULL,
                             piv_error_type          => g_err_imp,
                             piv_error_code          => l_err_code,
                             piv_error_message       => l_err_msg ||
                                                        l_jointacct_owner_msg_data,
                             piv_severity            => NULL,
                             piv_proposed_solution   => NULL);

                END LOOP;
              END IF;

            END IF;

          END IF;

          xxetn_debug_pkg.add_debug('Creating Supplier Bank Payee Assignment using API');
          xxetn_debug_pkg.add_debug('Setting Bank Account priority ');

          --fetch the count of the bank account number to set the priority
          BEGIN
            SELECT COUNT(bbr.bank_account_num)
              INTO l_bank_acct_priority_count
              FROM iby_ext_bank_accounts   bbr,
                   iby_pmt_instr_uses_all  piu,
                   iby_ext_banks_v         cbv,
                   iby_ext_bank_branches_v iebb,
                   iby_external_payees_all iepa,
                   po_vendors              pv,
                   po_vendor_sites_all     pvs,
                   hr_operating_units      hou
             WHERE piu.payment_flow = 'DISBURSEMENTS'
               AND piu.instrument_type = 'BANKACCOUNT'
               AND piu.instrument_id = bbr.ext_bank_account_id
               AND bbr.bank_id = cbv.bank_party_id
               AND bbr.branch_id = iebb.branch_party_id
               AND piu.ext_pmt_party_id = iepa.ext_payee_id
               AND iepa.payee_party_id = pv.party_id
               AND iepa.supplier_site_id = pvs.vendor_site_id
               AND pvs.org_id = hou.organization_id
               AND pv.segment1 = create_accounts_rec.leg_vendor_num
               AND pvs.vendor_site_code =
                   create_accounts_rec.leg_vendor_site_code
               AND hou.organization_id = create_accounts_rec.org_id;

            xxetn_debug_pkg.add_debug('l_bank_acct_priority_count ' ||
                                      l_bank_acct_priority_count);
          EXCEPTION
            WHEN OTHERS THEN
              xxetn_debug_pkg.add_debug('Error in deriving bank acct priority count. Error is: ' ||
                                        SUBSTR(SQLERRM, 1, 2000));
          END;

          IF l_bank_acct_priority_count = 0 THEN
            l_assignment_attribs_rec.priority := 1;

            xxetn_debug_pkg.add_debug('priority ' ||
                                      l_assignment_attribs_rec.priority);
          ELSE

            BEGIN
              SELECT MAX(order_of_preference)
                INTO l_bank_acct_priority
                FROM iby_ext_bank_accounts   bbr,
                     iby_pmt_instr_uses_all  piu,
                     iby_ext_banks_v         cbv,
                     iby_ext_bank_branches_v iebb,
                     iby_external_payees_all iepa,
                     po_vendors              pv,
                     po_vendor_sites_all     pvs,
                     hr_operating_units      hou
               WHERE piu.payment_flow = 'DISBURSEMENTS'
                 AND piu.instrument_type = 'BANKACCOUNT'
                 AND piu.instrument_id = bbr.ext_bank_account_id
                 AND bbr.bank_id = cbv.bank_party_id
                 AND bbr.branch_id = iebb.branch_party_id
                 AND piu.ext_pmt_party_id = iepa.ext_payee_id
                 AND iepa.payee_party_id = pv.party_id
                 AND iepa.supplier_site_id = pvs.vendor_site_id
                 AND pvs.org_id = hou.organization_id
                 AND pv.segment1 = create_accounts_rec.leg_vendor_num
                 AND pvs.vendor_site_code =
                     create_accounts_rec.leg_vendor_site_code
                 AND hou.organization_id = create_accounts_rec.org_id;

            EXCEPTION
              WHEN OTHERS THEN
                print_log_message('Error while derivation of priority for bank account NUM ' ||
                                  create_accounts_rec.leg_account_num ||
                                  'vendor number ' ||
                                  create_accounts_rec.leg_vendor_num ||
                                  'vendor site' ||
                                  create_accounts_rec.leg_vendor_site_code || 'OU' ||
                                  create_accounts_rec.leg_operating_unit ||
                                  '. Error is:' ||
                                  SUBSTR(SQLERRM, 1, 2000));
            END;

            xxetn_debug_pkg.add_debug('l_bank_acct_priority ' ||
                                      l_bank_acct_priority);

            l_assignment_attribs_rec.priority := l_bank_acct_priority + 1;
          END IF;

          l_payee_rec.payment_function     := 'PAYABLES_DISB';
          l_payee_rec.party_id             := create_accounts_rec.party_id;
          l_payee_rec.party_site_id        := create_accounts_rec.party_site_id;
          l_payee_rec.supplier_site_id     := create_accounts_rec.vendor_site_id;
          l_payee_rec.org_id               := create_accounts_rec.org_id;
          l_payee_rec.org_type             := 'OPERATING_UNIT';
          l_instrument_rec.instrument_id   := l_bank_acct_id;
          l_instrument_rec.instrument_type := 'BANKACCOUNT';
          --v_assignment_attribs_rec.start_date         := SYSDATE;
          l_assignment_attribs_rec.start_date := create_accounts_rec.creation_date;
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

          print_log_message('After Payee Assignment API');
          xxetn_debug_pkg.add_debug('Supplier Bank Payee API status: ' ||
                                    l_payee_ret_status);
          xxetn_debug_pkg.add_debug('l_payee_err_msg: ' || l_payee_err_msg);
          xxetn_debug_pkg.add_debug('Create Bank Payee Assignment: ' ||
                                    l_assign_id);

          -- If payee assignment record was successfully created
          IF l_payee_ret_status = fnd_api.g_ret_sts_success THEN
            -- Entire data set for account record created successfully
            l_retcode := g_success;
          ELSE
            -- if payee instrument assignment api error

            l_retcode  := g_error;
            l_err_code := 'ETN_AP_ACCOUNT_IMPORT_ERROR';
            l_err_msg  := 'Error : Payee instrument assignment for branch address failed. ';
            print_log_message(l_err_msg);

            print_log_message('l_payee_msg_count: ' || l_payee_msg_count);

            IF l_payee_msg_count = 1 THEN
              log_errors(pov_return_status       => l_log_ret_status -- OUT
                        ,
                         pov_error_msg           => l_log_err_msg -- OUT
                        ,
                         piv_source_column_name  => NULL,
                         piv_source_column_value => NULL,
                         piv_error_type          => g_err_imp,
                         piv_error_code          => l_err_code,
                         piv_error_message       => l_err_msg ||
                                                    l_payee_err_msg,
                         piv_severity            => NULL,
                         piv_proposed_solution   => NULL);
              --extract API errors
            ELSIF l_payee_msg_count > 1 THEN
              FOR i IN 1 .. l_payee_msg_count LOOP
                l_payee_err_msg := fnd_msg_pub.get(p_msg_index => i,
                                                   p_encoded   => fnd_api.g_false);

                log_errors(pov_return_status       => l_log_ret_status -- OUT
                          ,
                           pov_error_msg           => l_log_err_msg -- OUT
                          ,
                           piv_source_column_name  => NULL,
                           piv_source_column_value => NULL,
                           piv_error_type          => g_err_imp,
                           piv_error_code          => l_err_code,
                           piv_error_message       => l_err_msg ||
                                                      l_payee_err_msg,
                           piv_severity            => NULL,
                           piv_proposed_solution   => NULL);

              END LOOP;
            END IF;
            print_log_message(l_payee_err_msg);

          END IF;

          -- if api error
        ELSIF l_account_ret_status <> fnd_api.g_ret_sts_success THEN

          l_retcode  := g_error;
          l_err_code := 'ETN_AP_ACCOUNT_IMPORT_ERROR';
          l_err_msg  := 'Error :Account creation for the supplier bank failed.';
          print_log_message(l_err_msg);

          IF l_msg_count = 1 THEN

            log_errors(pov_return_status       => l_log_ret_status -- OUT
                      ,
                       pov_error_msg           => l_log_err_msg -- OUT
                      ,
                       piv_source_column_name  => NULL,
                       piv_source_column_value => NULL,
                       piv_error_type          => g_err_imp,
                       piv_error_code          => l_err_code,
                       piv_error_message       => l_err_msg ||
                                                  l_account_msg_data,
                       piv_severity            => NULL,
                       piv_proposed_solution   => NULL);
            --extract API errors
          ELSIF l_msg_count > 1 THEN
            FOR i IN 1 .. l_msg_count LOOP
              l_account_msg_data := fnd_msg_pub.get(p_msg_index => i,
                                                    p_encoded   => fnd_api.g_false);

              log_errors(pov_return_status       => l_log_ret_status -- OUT
                        ,
                         pov_error_msg           => l_log_err_msg -- OUT
                        ,
                         piv_source_column_name  => NULL,
                         piv_source_column_value => NULL,
                         piv_error_type          => g_err_imp,
                         piv_error_code          => l_err_code,
                         piv_error_message       => l_err_msg ||
                                                    l_account_msg_data,
                         piv_severity            => NULL,
                         piv_proposed_solution   => NULL);

            END LOOP;
          END IF;
        ELSE
          COMMIT;
          l_retcode := g_success;
        END IF;

        IF l_retcode = g_success THEN
          --Update process_flag to 'C' in case of API Success
          UPDATE xxap_supplier_bankaccnts_stg
             SET ext_bank_account_id    = l_bank_acct_id,
                 process_flag           = g_converted,
                 run_sequence_id        = g_run_seq_id,
                 last_updated_date      = SYSDATE,
                 last_updated_by        = g_user_id,
                 last_update_login      = g_login_id,
                 program_application_id = g_prog_appl_id,
                 program_id             = g_conc_program_id,
                 program_update_date    = SYSDATE,
                 request_id             = g_request_id
           WHERE interface_txn_id = create_accounts_rec.interface_txn_id;
        ELSE
          print_log_message('Inside Else 1');
          --Update process_flag to 'E' in case of API Failure
          UPDATE xxap_supplier_bankaccnts_stg
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
           WHERE interface_txn_id = create_accounts_rec.interface_txn_id;
        END IF;
        COMMIT;

      EXCEPTION
        WHEN OTHERS THEN

          g_retcode  := 1;
          l_err_code := 'ETN_AP_ACCOUNT_IMPORT_ERROR';
          l_err_msg  := 'Error : Exception in Supplier Account Import Loop. ' ||
                        SUBSTR(SQLERRM, 1, 240);
          print_log_message(l_err_msg);

          log_errors(pov_return_status       => l_log_ret_status -- OUT
                    ,
                     pov_error_msg           => l_err_msg -- OUT
                    ,
                     piv_source_column_name  => NULL,
                     piv_source_column_value => NULL,
                     piv_error_type          => g_err_imp,
                     piv_error_code          => l_err_code,
                     piv_error_message       => l_err_msg);
          ROLLBACK TO start_account;
      END;
    END LOOP;
  EXCEPTION
    WHEN OTHERS THEN
      g_retcode  := 1;
      l_err_code := 'ETN_AP_ACCOUNT_IMPORT_ERROR';
      l_err_msg  := 'Error : Exception in Supplier Account Import Procedure. ' ||
                    SUBSTR(SQLERRM, 1, 240);
      print_log_message(l_err_msg);
  END create_bank_accounts;

  --
  -- =============================================================================
  -- Procedure: create_int_accts
  -- =============================================================================
  --   This procedure create Intermediary Accounts
  -- =============================================================================
  --  Input Parameters :
  --    None
  --  Output Parameters :
  --    None
  -- -----------------------------------------------------------------------------
  --
  PROCEDURE create_int_accts IS

    -- For Intermediary Accounts
    l_int_acct_rec  iby_ext_bankacct_pub.IntermediaryAcct_rec_type;
    l_int_acct_id   NUMBER;
    l_return_status VARCHAR2(10);
    l_msg_count     NUMBER;
    l_msg_data      VARCHAR2(2000);
    l_msg           VARCHAR2(2000);
    l_response_out  IBY_FNDCPT_COMMON_PUB.Result_rec_type;
    l_retcode       VARCHAR2(1);

    -- For IBAN# Update
    l_iban_rec           iby_ext_bankacct_pub.ExtBankAcct_rec_type;
    l_return_status_iban VARCHAR2(10);
    l_msg_count_iban     NUMBER;
    l_msg_data_iban      VARCHAR2(2000);
    l_msg_iban           VARCHAR2(2000);
    l_response_out_iban  IBY_FNDCPT_COMMON_PUB.Result_rec_type;

    l_log_ret_status VARCHAR2(100);
    l_log_err_msg    VARCHAR2(2000);
    l_err_code       VARCHAR2(100);
    l_err_msg        VARCHAR2(2000);
    l_object_ver     NUMBER;
    l_country_code   iby_ext_bank_accounts.country_code%TYPE;
    l_check_digits   iby_ext_bank_accounts.check_digits%TYPE;

    CURSOR create_int_accts_cur IS
      SELECT *
        FROM xxap_supplier_int_accts_stg xsias
       WHERE xsias.process_flag = g_validated
         AND xsias.batch_id = g_new_batch_id
       ORDER BY xsias.interface_txn_id;

  BEGIN

    -- Initialize global variables for log_errors
    g_source_table    := g_int_accts_t;
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

    -- Start of Cursor Loop
    FOR create_int_accts_rec IN create_int_accts_cur LOOP

      -- Assign global variables for log_errors
      g_intf_staging_id := create_int_accts_rec.interface_txn_id;

      l_retcode            := 'S';
      l_return_status      := NULL;
      l_msg_count          := NULL;
      l_msg_data           := NULL;
      l_msg                := NULL;
      l_int_acct_id        := NULL;
      l_return_status_iban := NULL;
      l_msg_count_iban     := NULL;
      l_msg_data_iban      := NULL;
      l_msg_iban           := NULL;

      /** If Intermediary Account attributes are not NULL **/
      IF ((create_int_accts_rec.int_bank_name IS NOT NULL OR
         create_int_accts_rec.int_bank_branch_num IS NOT NULL OR
         create_int_accts_rec.int_account_number IS NOT NULL) AND
         create_int_accts_rec.intermediary_acct_id IS NULL) THEN

        BEGIN
          xxetn_debug_pkg.add_debug('Import Account record : ' ||
                                    create_int_accts_rec.interface_txn_id);

          --Assign staging table values to the account record type to be passed in the API
          l_int_acct_rec.bank_account_id       := create_int_accts_rec.supplier_account_id;
          l_int_acct_rec.bank_name             := create_int_accts_rec.int_bank_name;
          l_int_acct_rec.branch_number         := create_int_accts_rec.int_bank_branch_num;
          l_int_acct_rec.iban                  := create_int_accts_rec.int_iban;
          l_int_acct_rec.account_number        := create_int_accts_rec.int_account_number;
          l_int_acct_rec.bic                   := create_int_accts_rec.int_bic;
          l_int_acct_rec.bank_code             := create_int_accts_rec.int_bank_code;
          l_int_acct_rec.country_code          := create_int_accts_rec.int_country_code;
          l_int_acct_rec.city                  := create_int_accts_rec.int_city;
          l_int_acct_rec.object_version_number := 1.0;

          --Call API to create Intermediary Accounts
          xxetn_debug_pkg.add_debug('Calling Intermediary Account Creation API ');

          -- Calling Intermediary Accounts creation API
          iby_ext_bankacct_pub.create_intermediary_acct(p_api_version          => 1.0,
                                                        p_init_msg_list        => fnd_api.g_true,
                                                        p_intermed_acct_rec    => l_int_acct_rec,
                                                        x_intermediary_acct_id => l_int_acct_id,
                                                        x_return_status        => l_return_status,
                                                        x_msg_count            => l_msg_count,
                                                        x_msg_data             => l_msg_data,
                                                        x_response             => l_response_out);

          xxetn_debug_pkg.add_debug('l_return_status:' || l_return_status);
          print_log_message('l_return_status:' || l_return_status);

          IF l_return_status <> 'S' THEN
            l_retcode := 'E';

            xxetn_debug_pkg.add_debug('Intermediary Acct API Error');

            l_msg := NULL;
            FOR i IN 1 .. l_msg_count LOOP
              l_msg := l_msg || '~' ||
                       fnd_msg_pub.get(p_msg_index => i,
                                       p_encoded   => fnd_api.g_false);
            END LOOP;
            print_log_message('l_msg:' || l_msg);

            log_errors(pov_return_status       => l_log_ret_status -- OUT
                      ,
                       pov_error_msg           => l_log_err_msg -- OUT
                      ,
                       piv_source_column_name  => NULL,
                       piv_source_column_value => NULL,
                       piv_error_type          => g_err_imp,
                       piv_error_code          => 'ETN_AP_IMPORT_INTERMEDIARY_ACCTS_ERROR',
                       piv_error_message       => l_msg,
                       piv_severity            => NULL,
                       piv_proposed_solution   => NULL);
          END IF;

        EXCEPTION
          WHEN OTHERS THEN
            g_retcode  := 1;
            l_err_code := 'ETN_AP_IMPORT_INT_ACCTS_ERROR';
            l_err_msg  := 'Error : Exception in Supplier Intermediary Account Import Loop. ' ||
                          SUBSTR(SQLERRM, 1, 240);
            print_log_message(l_err_msg);

            log_errors(pov_return_status       => l_log_ret_status -- OUT
                      ,
                       pov_error_msg           => l_err_msg -- OUT
                      ,
                       piv_source_column_name  => NULL,
                       piv_source_column_value => NULL,
                       piv_error_type          => g_err_imp,
                       piv_error_code          => l_err_code,
                       piv_error_message       => l_err_msg);
        END;

      END IF; -- If Intermediary Account attributes are not NULL
      COMMIT;

      /** If IBAN detail is provided **/
      IF create_int_accts_rec.supplier_iban IS NOT NULL THEN

        BEGIN
          xxetn_debug_pkg.add_debug('IBAN# update record : ' ||
                                    create_int_accts_rec.interface_txn_id);

          l_object_ver := NULL;

          SELECT ieba.object_version_number,
                 ieba.country_code,
                 ieba.check_digits
            INTO l_object_ver, l_country_code, l_check_digits
            FROM iby_ext_bank_accounts ieba
           WHERE ieba.ext_bank_account_id =
                 create_int_accts_rec.supplier_account_id
             AND ieba.bank_account_num =
                 create_int_accts_rec.supplier_account;

          --Assign staging table values to the account record type to be passed in the API
          l_iban_rec.bank_account_id       := create_int_accts_rec.supplier_account_id;
          l_iban_rec.bank_account_num      := create_int_accts_rec.supplier_account;
          l_iban_rec.bank_account_name     := create_int_accts_rec.supplier_account_name;
          l_iban_rec.start_date            := create_int_accts_rec.sup_acct_start_date;
          l_iban_rec.branch_id             := create_int_accts_rec.sup_acct_branch_id;
          l_iban_rec.bank_id               := create_int_accts_rec.sup_acct_bank_id;
          l_iban_rec.currency              := create_int_accts_rec.sup_acct_currency;
          l_iban_rec.iban                  := create_int_accts_rec.supplier_iban;
          l_iban_rec.country_code          := l_country_code;
          l_iban_rec.check_digits          := l_check_digits;
          l_iban_rec.object_version_number := l_object_ver;

          --Call API to update IBAN# for Supplier Account
          xxetn_debug_pkg.add_debug('Calling IBAN# Update API ');

          -- Calling IBAN# Update API
          iby_ext_bankacct_pub.update_ext_bank_acct(p_api_version       => 1.0,
                                                    p_init_msg_list     => fnd_api.g_true,
                                                    p_ext_bank_acct_rec => l_iban_rec,
                                                    x_return_status     => l_return_status_iban,
                                                    x_msg_count         => l_msg_count_iban,
                                                    x_msg_data          => l_msg_data_iban,
                                                    x_response          => l_response_out_iban);

          xxetn_debug_pkg.add_debug('l_return_status_iban:' ||
                                    l_return_status_iban);
          print_log_message('l_return_status_iban:' ||
                            l_return_status_iban);

          IF l_return_status_iban <> 'S' THEN
            l_retcode := 'E';

            xxetn_debug_pkg.add_debug('IBAN# Update API Error');

            l_msg_iban := NULL;
            FOR i IN 1 .. l_msg_count_iban LOOP
              l_msg_iban := l_msg_iban || '~' ||
                            fnd_msg_pub.get(p_msg_index => i,
                                            p_encoded   => fnd_api.g_false);
            END LOOP;
            print_log_message('l_msg_iban:' || l_msg_iban);

            log_errors(pov_return_status       => l_log_ret_status -- OUT
                      ,
                       pov_error_msg           => l_log_err_msg -- OUT
                      ,
                       piv_source_column_name  => 'SUPPLIER_IBAN',
                       piv_source_column_value => create_int_accts_rec.supplier_iban,
                       piv_error_type          => g_err_imp,
                       piv_error_code          => 'ETN_AP_UPDATE_IBAN_ERROR',
                       piv_error_message       => l_msg_iban,
                       piv_severity            => NULL,
                       piv_proposed_solution   => NULL);
          END IF;

        EXCEPTION
          WHEN OTHERS THEN
            g_retcode  := 1;
            l_err_code := 'ETN_AP_UPDATE_IBAN_ERROR';
            l_err_msg  := 'Error : Exception in Updating IBAN# Loop. ' ||
                          SUBSTR(SQLERRM, 1, 240);
            print_log_message(l_err_msg);

            log_errors(pov_return_status       => l_log_ret_status -- OUT
                      ,
                       pov_error_msg           => l_err_msg -- OUT
                      ,
                       piv_source_column_name  => NULL,
                       piv_source_column_value => NULL,
                       piv_error_type          => g_err_imp,
                       piv_error_code          => l_err_code,
                       piv_error_message       => l_err_msg);
        END;

      END IF; -- If IBAN detail is provided

      IF l_retcode = 'S' THEN
        --Update process_flag to 'C' in case of API Success
        UPDATE xxap_supplier_int_accts_stg xsias
           SET xsias.intermediary_acct_id   = NVL(create_int_accts_rec.intermediary_acct_id,
                                                  l_int_acct_id),
               xsias.process_flag           = g_converted,
               xsias.run_sequence_id        = g_run_seq_id,
               xsias.last_update_date       = SYSDATE,
               xsias.last_updated_by        = g_user_id,
               xsias.last_update_login      = g_login_id,
               xsias.program_application_id = g_prog_appl_id,
               xsias.program_id             = g_conc_program_id,
               xsias.program_update_date    = SYSDATE,
               xsias.request_id             = g_request_id
         WHERE xsias.interface_txn_id =
               create_int_accts_rec.interface_txn_id;
      ELSE
        g_retcode := 1; -- Program set to Warning even if one record Fails
        print_log_message('Inside Else 1');
        --Update process_flag to 'E' in case of API Failure
        UPDATE xxap_supplier_int_accts_stg xsias
           SET xsias.intermediary_acct_id   = NVL(create_int_accts_rec.intermediary_acct_id,
                                                  l_int_acct_id),
               xsias.process_flag           = g_error,
               xsias.run_sequence_id        = g_run_seq_id,
               xsias.error_type             = g_err_imp,
               xsias.last_update_date       = SYSDATE,
               xsias.last_updated_by        = g_user_id,
               xsias.last_update_login      = g_login_id,
               xsias.program_application_id = g_prog_appl_id,
               xsias.program_id             = g_conc_program_id,
               xsias.program_update_date    = SYSDATE,
               xsias.request_id             = g_request_id
         WHERE xsias.interface_txn_id =
               create_int_accts_rec.interface_txn_id;
      END IF;
      COMMIT;

    END LOOP; -- End of Loop

  EXCEPTION
    WHEN OTHERS THEN
      g_retcode := 1;
      l_err_msg := 'Error : Exception in Supplier Intermediary Account Import and IBAN# Update Procedure. ' ||
                   SUBSTR(SQLERRM, 1, 240);
      print_log_message(l_err_msg);
  END create_int_accts;



-- =============================================================================
  -- Procedure: Supplier_contact_API
  -- =============================================================================
  -- To update supplier contact DFF attribute1 in AP_SUPPLIER_CONTACTS.
  -- =============================================================================
  --  Input Parameters :
    --    pin_batch_id  : Batch Id
  --  Output Parameters :
  --    pov_errbuf    : Error message in case of any failure
  --    pon_retcode   : Return Status - Normal/Warning/Error
  -- -----------------------------------------------------------------------------
  PROCEDURE Supplier_contact_API(pov_errbuf   OUT NOCOPY VARCHAR2,
                         pon_retcode  OUT NOCOPY NUMBER,
                        pin_batch_id IN NUMBER) IS
 lv_return_status        VARCHAR2 (1);
 lv_msg_count            NUMBER;
 lv_msg_data             VARCHAR2 (2000);
 lv_message_int          NUMBER;
v_resp_appl_id NUMBER := fnd_global.resp_appl_id;
v_resp_id  NUMBER     := fnd_global.resp_id;
v_user_id  NUMBER     := fnd_global.user_id;

cursor ven_cont is
select vendor_id, vendor_site_id, vendor_contact_id, attribute1,org_id
  from xxap_supplier_contacts_stg
 where vendor_contact_id is not null
 and vendor_site_id is not null
 and batch_id = pin_batch_id ;
   /*and vendor_contact_id = 32536*/

lv_vendor_contact_rec   ap_vendor_pub_pkg.r_vendor_contact_rec_type;

  BEGIN

/*    mo_global.init('SQLAP');
    mo_global.set_policy_context('S',500141);  --please use org id
    fnd_global.set_nls_context('AMERICAN');
*/




FOR i IN VEN_CONT LOOP
print_log_message('Vendor id :- '||i.vendor_id);
print_log_message('Vendor Site id :- '||i.vendor_site_id);
print_log_message('Vendor Contact Id :- '|| i.vendor_contact_id);

  FND_GLOBAL.APPS_INITIALIZE(v_user_id,v_resp_id, v_resp_appl_id);
  MO_GLOBAL.INIT ('SQLAP');
  mo_global.set_policy_context ('S' ,i.org_id);
  fnd_global.set_nls_context('AMERICAN');

 SELECT Distinct VENDOR_CONTACT_ID,
        VENDOR_SITE_ID,
        VENDOR_ID,
        PER_PARTY_ID,
        RELATIONSHIP_ID,
        REL_PARTY_ID,
        PARTY_SITE_ID,
        ORG_CONTACT_ID,
        ORG_PARTY_SITE_ID
   INTO lv_vendor_contact_rec.vendor_contact_id,
        lv_vendor_contact_rec.VENDOR_SITE_ID,
        lv_vendor_contact_rec.VENDOR_ID,
        lv_vendor_contact_rec.PER_PARTY_ID,
        lv_vendor_contact_rec.RELATIONSHIP_ID,
        lv_vendor_contact_rec.REL_PARTY_ID,
        lv_vendor_contact_rec.PARTY_SITE_ID,
        lv_vendor_contact_rec.ORG_CONTACT_ID,
        lv_vendor_contact_rec.ORG_PARTY_SITE_ID
   FROM po_vendor_contacts
  where vendor_contact_id = i.vendor_contact_id
  and vendor_site_id=i.vendor_site_id
  and vendor_id=i.vendor_id;

/*Hard Code value for attribute category */
lv_vendor_contact_rec.ATTRIBUTE_CATEGORY   := 'Conversion';

lv_vendor_contact_rec.ATTRIBUTE1           := i.attribute1;

/*Call API */

ap_vendor_pub_pkg.Update_Vendor_Contact_Public
                           (p_api_version           => 1.0,
                            p_init_msg_list         => fnd_api.g_FALSE,
                            p_commit                => fnd_api.g_false,
                            p_validation_level      => fnd_api.g_valid_level_full,
                            p_vendor_contact_rec    => lv_vendor_contact_rec,
                            x_return_status         => lv_return_status,
                            x_msg_count             => lv_msg_count,
                            x_msg_data              => lv_msg_data
                           );
COMMIT;

    /*dbms_output.put_line('return_status: '||lV_return_status);
    dbms_output.put_line('msg_data: '||lV_msg_data);
    dbms_output.put_line('msg_count: '||lV_msg_count);*/

print_log_message ('lV_return_status :- '|| lV_return_status || 'Vendor contact id' || i.vendor_contact_id);

     IF (lV_return_status <> 'S') THEN
                  IF lv_msg_count    >= 1 THEN
                    FOR v_index IN 1..lv_msg_count
                    LOOP
                      fnd_msg_pub.get (p_msg_index => v_index, p_encoded => 'F', p_data => lV_msg_data, p_msg_index_out => lv_message_int );
                      lV_msg_data := 'update_vendor_contact '||SUBSTR(lV_msg_data,1,3900);
                      print_log_message('lV_msg_data - '||lV_msg_data );
                     /* dbms_output.put_line('lV_msg_data - '||lV_msg_data );*/
                    END LOOP;
                  End If; -- msg count
     END IF; -- API return status

END LOOP;


  EXCEPTION
    WHEN OTHERS THEN

      pov_errbuf  := 'Error : Supplier_contact_API program procedure encounter error. ' ||
                     SUBSTR(SQLERRM, 1, 150);
      pon_retcode := 2;
      print_log_message(pov_errbuf);
END Supplier_contact_API;


  -- =============================================================================
  -- Procedure: tieback_main
  -- =============================================================================
  -- To check records errored/imported successfully by Supplier import program
  -- and update the corresponding records with process_flag = 'E'/'P' in staging table
  -- =============================================================================
  --  Input Parameters :
  --    piv_entity    : Entity Name
  --    pin_batch_id  : Batch Id
  --  Output Parameters :
  --    pov_errbuf    : Error message in case of any failure
  --    pon_retcode   : Return Status - Normal/Warning/Error
  -- -----------------------------------------------------------------------------
  PROCEDURE tieback_main(pov_errbuf   OUT NOCOPY VARCHAR2,
                         pon_retcode  OUT NOCOPY NUMBER,
                         piv_entity   IN VARCHAR2,
                         p_dummy      IN VARCHAR2,
                         pin_batch_id IN NUMBER) IS
    process_exception EXCEPTION;
    l_error_code_sup  CONSTANT VARCHAR2(100) := 'ETN_AP_SUPPLIER_INTERFACE_ERROR';
    l_error_code_site CONSTANT VARCHAR2(100) := 'ETN_AP_SUPPLIER_SITE_INTERFACE_ERROR';
    l_error_code_cont CONSTANT VARCHAR2(100) := 'ETN_AP_SUPPLIER_CONTACTS_INTERFACE_ERROR';
    l_log_ret_status VARCHAR2(50);
    l_log_err_msg    VARCHAR2(2000);
    l_error_message  VARCHAR2(2000) := NULL;
    l_success_cnt    NUMBER := 0;
    l_failed_cnt     NUMBER := 0;
    l_total_cnt      NUMBER := 0;

    l_return_status VARCHAR2(100);
    l_err_msg       VARCHAR2(2000);
    l_retcode         NUMBER := 0;
  BEGIN
    pon_retcode := g_normal;
    pov_errbuf  := NULL;

    xxetn_debug_pkg.initialize_debug(pov_err_msg      => l_error_message,
                                     piv_program_name => 'XXAP_SUPPLIER_TIEBK_CONVERSION');

    --Printing program parameters to program log
    print_log_message('|---------------------------------------');
    print_log_message('|Program Parameters');
    print_log_message('|---------------------------------------');
    print_log_message('|Entity       : ' || piv_entity);
    print_log_message('|Batch Id     : ' || pin_batch_id);
    print_log_message('|---------------------------------------');

    IF l_error_message IS NOT NULL THEN
      l_error_message := 'Error while initializing debug !!';
      RAISE process_exception;
    END IF;

    -- Initialize global variables for log_errors
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
    g_run_seq_id      := xxetn_run_sequences_s.NEXTVAL; --change 22/07/2015
    --Setting the run sequence id for error framework
    xxetn_common_error_pkg.g_run_seq_id := g_run_seq_id;
    -- If Entity is Supplier
    IF piv_entity = g_supplier THEN

      -- Successful Records
      BEGIN
        UPDATE xxap_suppliers_stg xss
           SET xss.process_flag           = g_converted,
               xss.last_updated_date      = SYSDATE,
               xss.last_updated_by        = g_user_id,
               xss.last_update_login      = g_login_id,
               xss.program_application_id = g_prog_appl_id,
               xss.program_id             = g_conc_program_id,
               xss.program_update_date    = SYSDATE,
               xss.request_id             = g_request_id,
               xss.vendor_id              = (SELECT asa.vendor_id
                                               FROM ap_suppliers asa
                                              WHERE xss.leg_segment1 =
                                                    asa.segment1),
               xss.run_sequence_id        = g_run_seq_id
         WHERE xss.batch_id = pin_batch_id
           AND xss.process_flag = g_processed
           AND EXISTS
         (SELECT 1
                  FROM ap_suppliers_int asi
                 WHERE xss.vendor_interface_id = asi.vendor_interface_id
                   AND asi.status = 'PROCESSED');

        l_success_cnt := SQL%ROWCOUNT;

      EXCEPTION
        WHEN OTHERS THEN
          l_error_message := SUBSTR('Exception occurred while updating process_flag for converted suppliers. Oracle error is ' ||
                                    SQLERRM,
                                    1,
                                    1999);
          pon_retcode     := g_error;
          pov_errbuf      := l_error_message;
          fnd_file.put_line(fnd_file.LOG, l_error_message);
          ROLLBACK;
      END;

      -- Rejected Records
      BEGIN
        g_source_table := g_supplier_t;

        UPDATE xxap_suppliers_stg xss
           SET xss.process_flag           = g_error,
               xss.error_type             = g_err_imp,
               xss.last_updated_date      = SYSDATE,
               xss.last_updated_by        = g_user_id,
               xss.last_update_login      = g_login_id,
               xss.program_application_id = g_prog_appl_id,
               xss.program_id             = g_conc_program_id,
               xss.program_update_date    = SYSDATE,
               xss.request_id             = g_request_id,
               xss.run_sequence_id        = g_run_seq_id
         WHERE xss.batch_id = pin_batch_id
           AND xss.process_flag = g_processed
           AND EXISTS
         (SELECT 1
                  FROM ap_suppliers_int asi
                 WHERE xss.vendor_interface_id = asi.vendor_interface_id
                   AND asi.status = 'REJECTED');

        l_failed_cnt := SQL%ROWCOUNT;

        COMMIT;

        FOR indx IN (SELECT xss.*,
                            (SELECT RTRIM(XMLAGG(XMLELEMENT(e,
                                                            reject_lookup_code ||
                                                            ' | '))
                                          .EXTRACT('//text()'),
                                          ' | ')
                               FROM ap_supplier_int_rejections
                              WHERE parent_table = 'AP_SUPPLIERS_INT'
                                AND parent_id = xss.vendor_interface_id) AS reject_lookup_code
                       FROM xxap_suppliers_stg xss
                      WHERE xss.batch_id = pin_batch_id
                        AND xss.process_flag = g_error
                        AND EXISTS
                      (SELECT 1
                               FROM ap_supplier_int_rejections asir
                              WHERE asir.parent_table = 'AP_SUPPLIERS_INT'
                                AND xss.vendor_interface_id = asir.parent_id)) LOOP
          g_intf_staging_id := indx.interface_txn_id;
          log_errors(pov_return_status       => l_log_ret_status -- OUT
                    ,
                     pov_error_msg           => l_log_err_msg -- OUT
                    ,
                     piv_source_column_name  => 'LEG_SEGMENT1',
                     piv_source_column_value => indx.leg_segment1,
                     piv_error_type          => g_err_imp,
                     piv_error_code          => l_error_code_sup,
                     piv_error_message       => 'Error : ' ||
                                                indx.reject_lookup_code /*||
                                                ' interface_txn_id-' ||   -- 4 May 2016 DD - it will difficult to cosolidate error message
                                                indx.interface_txn_id*/);
        END LOOP;
      EXCEPTION
        WHEN OTHERS THEN
          l_error_message := SUBSTR('Exception occurred while updating process_flag for converted suppliers. Oracle error is ' ||
                                    SQLERRM,
                                    1,
                                    1999);
          pon_retcode     := g_error;
          pov_errbuf      := l_error_message;
          fnd_file.put_line(fnd_file.LOG, l_error_message);
          ROLLBACK;
      END;

      -- If Entity is Supplier Sites
    ELSIF piv_entity = g_supplier_sites THEN

      -- Successful Records
      BEGIN
        UPDATE xxap_supplier_sites_stg xsss
           SET xsss.process_flag           = g_converted,
               xsss.last_update_date       = SYSDATE,
               xsss.last_updated_by        = g_user_id,
               xsss.last_update_login      = g_login_id,
               xsss.program_application_id = g_prog_appl_id,
               xsss.program_id             = g_conc_program_id,
               xsss.program_update_date    = SYSDATE,
               xsss.request_id             = g_request_id,
               xsss.vendor_site_id         = (SELECT assa.vendor_site_id
                                                FROM ap_supplier_sites_all assa
                                               WHERE assa.vendor_id =
                                                     xsss.vendor_id
                                                 AND assa.vendor_site_code =
                                                     xsss.leg_vendor_site_code
                                                 AND assa.org_id =
                                                     xsss.org_id),
               xsss.run_sequence_id        = g_run_seq_id
         WHERE xsss.batch_id = pin_batch_id
           AND xsss.process_flag = g_processed
           AND EXISTS (SELECT 1
                  FROM ap_supplier_sites_int assi
                 WHERE xsss.vendor_site_interface_id =
                       assi.vendor_site_interface_id
                   AND assi.status = 'PROCESSED');

        l_success_cnt := SQL%ROWCOUNT;

      EXCEPTION
        WHEN OTHERS THEN
          l_error_message := SUBSTR('Exception occurred while updating process_flag for converted supplier sites. Oracle error is ' ||
                                    SQLERRM,
                                    1,
                                    1999);
          pon_retcode     := g_error;
          pov_errbuf      := l_error_message;
          fnd_file.put_line(fnd_file.LOG, l_error_message);
          ROLLBACK;
      END;

      -- Rejected Records
      BEGIN
        g_source_table := g_supplier_sites_t;

        UPDATE xxap_supplier_sites_stg xsss
           SET xsss.process_flag           = g_error,
               xsss.error_type             = g_err_imp,
               xsss.last_update_date       = SYSDATE,
               xsss.last_updated_by        = g_user_id,
               xsss.last_update_login      = g_login_id,
               xsss.program_application_id = g_prog_appl_id,
               xsss.program_id             = g_conc_program_id,
               xsss.program_update_date    = SYSDATE,
               xsss.request_id             = g_request_id,
               xsss.run_sequence_id        = g_run_seq_id
         WHERE xsss.batch_id = pin_batch_id
           AND xsss.process_flag = g_processed
           AND EXISTS (SELECT 1
                  FROM ap_supplier_sites_int assi
                 WHERE xsss.vendor_site_interface_id =
                       assi.vendor_site_interface_id
                   AND assi.status = 'REJECTED');

        l_failed_cnt := SQL%ROWCOUNT;

        COMMIT;

        FOR indx IN (SELECT xsss.*,
                            (SELECT RTRIM(XMLAGG(XMLELEMENT(e,
                                                            reject_lookup_code ||
                                                            ' | '))
                                          .EXTRACT('//text()'),
                                          ' | ')
                               FROM ap_supplier_int_rejections
                              WHERE parent_table = 'AP_SUPPLIER_SITES_INT'
                                AND parent_id = xsss.vendor_site_interface_id) AS reject_lookup_code
                       FROM xxap_supplier_sites_stg xsss
                      WHERE xsss.batch_id = pin_batch_id
                        AND xsss.process_flag = g_error
                        AND EXISTS (SELECT 1
                               FROM ap_supplier_int_rejections asir
                              WHERE asir.parent_table =
                                    'AP_SUPPLIER_SITES_INT'
                                AND xsss.vendor_site_interface_id =
                                    asir.parent_id)) LOOP
          g_intf_staging_id := indx.interface_txn_id;
          log_errors(pov_return_status       => l_log_ret_status -- OUT
                    ,
                     pov_error_msg           => l_log_err_msg -- OUT
                    ,
                     piv_source_column_name  => 'LEG_VENDOR_SITE_CODE',
                     piv_source_column_value => indx.leg_vendor_site_code,
                     piv_error_type          => g_err_imp,
                     piv_error_code          => l_error_code_site,
                     piv_error_message       => 'Error : ' ||
                                                indx.reject_lookup_code /*||
                                                ' interface_txn_id-' ||  -- 4 May 2016 DD - it will difficult to cosolidate error message
                                                indx.interface_txn_id*/);
        END LOOP;
      EXCEPTION
        WHEN OTHERS THEN
          l_error_message := SUBSTR('Exception occurred while updating process_flag for converted suppliersites. Oracle error is ' ||
                                    SQLERRM,
                                    1,
                                    1999);
          pon_retcode     := g_error;
          pov_errbuf      := l_error_message;
          fnd_file.put_line(fnd_file.LOG, l_error_message);
          ROLLBACK;
      END;

      -- If Entity is Supplier Contacts
    ELSIF piv_entity = g_supplier_contacts THEN

      -- Successful Records
      print_log_message('Supplier contact Tieback - PROCESSED');

     BEGIN

       UPDATE xxap_supplier_contacts_stg xscs
           SET xscs.process_flag           = g_converted,
               xscs.last_updated_date      = SYSDATE,
               xscs.last_updated_by        = g_user_id,
               xscs.last_update_login      = g_login_id,
               xscs.program_application_id = g_prog_appl_id,
               xscs.program_id             = g_conc_program_id,
               xscs.program_update_date    = SYSDATE,
               xscs.request_id             = g_request_id,
               xscs.vendor_contact_id      = (SELECT pvc.vendor_contact_id
                                                FROM po_vendor_contacts pvc
                                               WHERE pvc.vendor_id =
                                                     xscs.vendor_id
                                                 AND NVL(pvc.vendor_site_id,
                                                         999999999) =
                                                     NVL(xscs.vendor_site_id,
                                                         999999999)
                                                 AND NVL(pvc.last_name,
                                                         999999999) =
                                                     NVL(xscs.leg_last_name,
                                                         999999999)
                                                 AND NVL(pvc.email_address,
                                                         999999999) =
                                                     NVL(xscs.leg_email_address,
                                                         999999999)
                                                 AND NVL(pvc.first_name,
                                                         999999999) =
                                                     NVL(xscs.leg_first_name,
                                                         999999999)
                                                 AND NVL(pvc.title, 999999999) =
                                                     NVL(xscs.leg_title,
                                                         999999999)
                                                 AND NVL(pvc.Area_code, 999999999) =
                                                     NVL(xscs.Leg_area_code,
                                                         999999999)
                                                 AND NVL(pvc.phone, 999999999) =
                                                     NVL(xscs.phone,
                                                         999999999))
         WHERE xscs.batch_id = pin_batch_id
           AND xscs.process_flag = g_processed
           AND EXISTS (SELECT 1
                  FROM ap_sup_site_contact_int assci
                 WHERE xscs.vendor_contact_interface_id =
                       assci.vendor_contact_interface_id
                   AND assci.status = 'PROCESSED');

        l_success_cnt := SQL%ROWCOUNT;
           print_log_message('Completed - Supplier contact Tieback - PROCESSED');
       COMMIT;
      EXCEPTION
        WHEN OTHERS THEN
          l_error_message := SUBSTR('Exception occurred while updating process_flag for converted supplier contacts. Oracle error is ' ||
                                    SQLERRM,
                                    1,
                                    1999);
          pon_retcode     := g_error;
          pov_errbuf      := l_error_message;
          fnd_file.put_line(fnd_file.LOG, l_error_message);
          ROLLBACK;
      END;

      -- Rejected Records
      BEGIN
        g_source_table := g_supplier_contacts_t;
  print_log_message('Supplier contact Tieback - REJECTED');
        UPDATE xxap_supplier_contacts_stg xscs
           SET xscs.process_flag           = g_error,
               xscs.error_type             = g_err_imp,
               xscs.last_updated_date      = SYSDATE,
               xscs.last_updated_by        = g_user_id,
               xscs.last_update_login      = g_login_id,
               xscs.program_application_id = g_prog_appl_id,
               xscs.program_id             = g_conc_program_id,
               xscs.program_update_date    = SYSDATE,
               xscs.request_id             = g_request_id
         WHERE xscs.batch_id = pin_batch_id
           AND xscs.process_flag = g_processed
           AND EXISTS (SELECT 1
                  FROM ap_sup_site_contact_int assci
                 WHERE xscs.vendor_contact_interface_id =
                       assci.vendor_contact_interface_id
                   AND assci.status = 'REJECTED');

        l_failed_cnt := SQL%ROWCOUNT;
  print_log_message('Completed - Supplier contact Tieback - REJECTED');
        COMMIT;

        FOR indx IN (SELECT xscs.*,
                            (SELECT RTRIM(XMLAGG(XMLELEMENT(e,
                                                            reject_lookup_code ||
                                                            ' | '))
                                          .EXTRACT('//text()'),
                                          ' | ')
                               FROM ap_supplier_int_rejections
                              WHERE parent_table = 'AP_SUP_SITE_CONTACT_INT'
                                AND parent_id =
                                    xscs.vendor_contact_interface_id) AS reject_lookup_code
                       FROM xxap_supplier_contacts_stg xscs
                      WHERE xscs.batch_id = pin_batch_id
                        AND xscs.process_flag = g_error
                        AND EXISTS (SELECT 1
                               FROM ap_supplier_int_rejections asir
                              WHERE asir.parent_table =
                                    'AP_SUP_SITE_CONTACT_INT'
                                AND xscs.vendor_contact_interface_id =
                                    asir.parent_id)) LOOP
          g_intf_staging_id := indx.interface_txn_id;
          log_errors(pov_return_status       => l_log_ret_status -- OUT
                    ,
                     pov_error_msg           => l_log_err_msg -- OUT
                    ,
                     piv_source_column_name  => 'LEG_VENDOR_SITE_CODE',
                     piv_source_column_value => indx.leg_vendor_site_code,
                     piv_error_type          => g_err_imp,
                     piv_error_code          => l_error_code_cont,
                     piv_error_message       => 'Error : ' ||
                                                indx.reject_lookup_code /*||
                                                ' interface_txn_id-' ||  -- 4 May 2016 DD - it will difficult to cosolidate error message
                                                indx.interface_txn_id*/);
        END LOOP;
      EXCEPTION
        WHEN OTHERS THEN
          l_error_message := SUBSTR('Exception occurred while updating process_flag for converted supplier contacts. Oracle error is ' ||
                                    SQLERRM,
                                    1,
                                    1999);
          pon_retcode     := g_error;
          pov_errbuf      := l_error_message;
          fnd_file.put_line(fnd_file.LOG, l_error_message);
          ROLLBACK;
      END;

  /* Update Vendor Contact API to update attribute1 for leg vendor contact id
  ap_vendor_pub_pkg.Update_Vendor_Contact_Public*/
  print_log_message (' Update contact API Starts');
      Supplier_contact_API(l_err_msg, l_retcode, pin_batch_id);

    END IF;

    -- Insert remaining records in Error Table
    IF g_source_tab.COUNT > 0 THEN
      xxetn_common_error_pkg.add_error(pov_return_status => l_return_status,
                                       pov_error_msg     => l_err_msg,
                                       pi_source_tab     => g_source_tab,
                                       pin_batch_id      => pin_batch_id);
      g_source_tab.DELETE;
      g_indx := 0;
    END IF;

    -- Print Stats for Tieback
    xxetn_debug_pkg.add_debug('Program Name : Eaton AP Supplier Conversion Tieback Program ');
    fnd_file.put_line(fnd_file.OUTPUT,
                      'Program Name : Eaton AP Supplier Conversion Tieback Program ');
    fnd_file.put_line(fnd_file.OUTPUT,
                      'Request ID   : ' || TO_CHAR(g_request_id));
    fnd_file.put_line(fnd_file.OUTPUT,
                      'Report Date  : ' ||
                      TO_CHAR(SYSDATE, 'DD-MON-RRRR HH:MI:SS AM'));
    fnd_file.put_line(fnd_file.OUTPUT,
                      '-------------------------------------------------------------------------------------------------');
    fnd_file.put_line(fnd_file.OUTPUT, 'Parameters   : ');
    fnd_file.put_line(fnd_file.OUTPUT,
                      '---------------------------------------------');
    fnd_file.put_line(fnd_file.OUTPUT, 'Entity       : ' || piv_entity);
    fnd_file.put_line(fnd_file.OUTPUT, 'Batch ID     : ' || pin_batch_id);

    fnd_file.put_line(fnd_file.output, CHR(10));
    fnd_file.put_line(fnd_file.output,
                      '=============================================================================================');
    fnd_file.put_line(fnd_file.output, 'Statistics :');
    fnd_file.put_line(fnd_file.output,
                      '=============================================================================================');

    l_total_cnt := l_success_cnt + l_failed_cnt;
    fnd_file.PUT_LINE(fnd_file.OUTPUT,
                      'Total Records Interfaced       : ' || l_total_cnt);
    fnd_file.PUT_LINE(fnd_file.OUTPUT,
                      'Records Successfully Imported  : ' || l_success_cnt);
    fnd_file.PUT_LINE(fnd_file.OUTPUT,
                      'Records Errored in Interface   : ' || l_failed_cnt);

  EXCEPTION
    WHEN process_exception THEN
      pon_retcode := g_warning;
      pov_errbuf  := l_error_message;
      fnd_file.put_line(fnd_file.LOG, l_error_message);
    WHEN OTHERS THEN
      l_error_message := SUBSTR('Exception in Procedure tieback_main. Outer  ' ||
                                SQLERRM,
                                1,
                                1999);
      pon_retcode     := 2;
      pov_errbuf      := l_error_message;
      fnd_file.put_line(fnd_file.LOG, l_error_message);
  END tieback_main;

  --
  -- ========================
  -- Procedure: main
  -- =============================================================================
  --   This is a main public procedure, which will be invoked through concurrent
  --   program.
  --
  --   This conversion program is used to validate and convert following Entities
  --
  --   Supplier
  --   Supplier Sites
  --   Supplier Contacts
  --   Supplier Bank
  --   Supplier Bank Branches
  --   Supplier Bank Accounts
  --   Supplier Intermediary Bank Accounts
  --
  -- =============================================================================
  --
  -- -----------------------------------------------------------------------------
  --  Called By Concurrent Program: Eaton AP Supplier Conversion Program
  -- -----------------------------------------------------------------------------
  -- -----------------------------------------------------------------------------
  --
  --  Input Parameters :
  --    piv_run_mode        : Control the program execution for VALIDATE and CONVERSION
  --    piv_hidden1         : Hidden variable
  --    piv_entity          : bank / branch
  --    pin_batch_id        : List all unique batches from staging table , this will
  --                          be NULL for first Conversion Run.
  --    piv_hidden          : Hidden variable
  --    piv_process_records : Conditionally available only when P_BATCH_ID is popul-
  --                          -ated. Otherwise this will be disabled and defaulted
  --                          to ALL
  --    piv_hidden_int      : Hidden Parameter to Control Data File Parameter
  --    piv_data_file       : Data File for IBAN and Intermediary Accounts Load
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
                 piv_hidden1         IN VARCHAR2 -- dummy variable
                ,
                 piv_entity          IN VARCHAR2 -- bank / branch
                ,
                 pin_batch_id        IN NUMBER -- null / <batch_id>
                ,
                 piv_hidden          IN VARCHAR2 -- dummy variable
                ,
                 piv_process_records IN VARCHAR2 -- (a) all / (e) error only / (n) unprocessed
                ,
                 piv_hidden_int      IN VARCHAR2 -- Hidden Parameter to Control Data File Parameter
                ,
                 piv_data_file       IN VARCHAR2 -- Data File for IBAN and Intermediary Accounts Load
                 ) IS

    l_debug_on       BOOLEAN;
    l_retcode        VARCHAR2(1) := 'S';
    l_return_status  VARCHAR2(200) := NULL;
    l_err_code       VARCHAR2(40) := NULL;
    l_err_msg        VARCHAR2(2000) := NULL;
    l_log_ret_status VARCHAR2(50) := NULL;
    l_log_err_msg    VARCHAR2(2000);
    l_active_date    Date;
    l_ret_code NUMBER;
    l_lookup_code VARCHAR2(2000) := NULL;
  BEGIN
    --Initialize global variables for error framework
    g_source_table    := g_bank_t;
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
                                     piv_program_name => 'XXAP_SUPPLIER_CONVERSION');

    -- If error initializing debug messages
    IF l_err_msg IS NOT NULL THEN
      pon_retcode := 2;
      pov_errbuf  := l_err_msg;
      print_log_message('Debug Initialization failed');
      RETURN;
    END IF;
    print_log_message('Started Supplier Conversion at ' ||
                      TO_CHAR(g_sysdate, 'DD-MON-YYYY HH24:MI:SS'));

    print_log_message('+---------------------------------------------------------------------------+');

    -- Check if debug logging is enabled/disabled

    g_run_mode        := piv_run_mode;
    g_entity          := piv_entity;
    g_batch_id        := pin_batch_id;
    g_process_records := piv_process_records;
    g_data_file       := piv_data_file;

    -- Call Common Debug and Error Framework initialization
    print_log_message('Program Parameters  : ');
    print_log_message('---------------------------------------------');
    print_log_message('Run Mode            : ' || g_run_mode);
    print_log_message('Entity              : ' || piv_entity);
    print_log_message('Batch ID            : ' || pin_batch_id);
    print_log_message('Process records     : ' || g_process_records);
    print_log_message('Data File           : ' || g_data_file);
    print_log_message('Request ID          : ' || g_request_id);


    IF piv_run_mode = 'LOAD-DATA' THEN
      xxetn_debug_pkg.add_debug('In Load Data Mode');
      print_log_message('In Load Data Mode');
      --call the procedure to load data from extraction tables into staging table
      get_data();

/*CR # 372676 - Employee type supplier
           Employee number however corresponding details does not exists in employee master
           Employee type supplier(VENDOR) without employee number*/
 IF piv_entity = g_supplier THEN
   Begin
       --v4.0 commented the description to match the values with BR100 setup
       SELECT distinct to_date(flv.lookup_Code)--to_date(flv.description)
        INTO l_active_date
        FROM fnd_lookup_values flv
       WHERE flv.lookup_type = 'XXETN_VENDOR_INACTIVE_DATE'
         --AND flv.meaning = 'VENDOR'
         AND flv.enabled_flag = g_yes
         AND SYSDATE BETWEEN NVL(flv.start_date_active, SYSDATE) AND
             NVL(flv.end_date_active, SYSDATE + 1)
         AND flv.language = USERENV('LANG');

     If  l_active_date is NOT NULL THEN
    /*If Employee Number is Blank then Vendor Type lookup code = 'VENDOR'*/
         Update xxconv.xxap_suppliers_stg
              Set leg_vendor_type_lookup_code = 'VENDOR'
                 ,Leg_END_Date_active = l_active_date
                 ,Leg_hold_all_payments_flag = 'Y'
            Where leg_vendor_type_lookup_code = 'EMPLOYEE'
              and leg_employee_number IS NULL;
        print_log_message ('Total Employee converted as Vendor due to employee number is blank :- '||SQL%ROWCOUNT);
          COMMIT;
      END IF;
      EXCEPTION
      WHEN OTHERS THEN
                     pon_retcode := g_warning;
                    log_errors(pov_return_status       => l_log_ret_status -- OUT
                              ,pov_error_msg           => l_log_err_msg -- OUT
                              ,piv_source_column_name  => 'LEG_EMPLOYEE_NUMBER',
                               piv_source_column_value => '',
                               piv_error_type          => 'E',
                               piv_error_code          => 'XXETN_VENDOR_INACTIVE_DATE',
                               piv_error_message       => 'Error : Employee inactive date is not configure in lookup - XXETN_VENDOR_INACTIVE_DATE, Oracle Error is ' ||
                                                          SQLERRM);
   End;
END IF;

      /*Change by Goutam on 7/8:
      if payment method = EFT, then set branch type to ABA,
      if payment method = WIRE then set branch type to SWIFT,
      for all others select branch type OTHER.
      This below patch run only at the last for all load activity*/
 IF piv_entity = g_account THEN

    Declare
       l_stmt  VARCHAR2(2000);
    BEGIN
       l_stmt := 'CREATE INDEX XXCONV.XXAP_SUPPLIER_SITES_STG_PC ON XXCONV.XXAP_SUPPLIER_siteS_STG (leg_request_id, leg_org_id, leg_vendor_Site_id, leg_vendor_id)';
       EXECUTE IMMEDIATE l_stmt;
       print_log_message ( 'Indexes created On XXAP_SUPPLIER_SITES_STG Table');

       dbms_stats.gather_table_stats(ownname          => 'XXCONV',
                                    tabname          => 'XXAP_SUPPLIER_SITES_STG',
                                    cascade          => true,
                                    estimate_percent => dbms_stats.auto_sample_size,
                                    degree           => dbms_stats.default_degree);
       dbms_stats.gather_table_stats(ownname          => 'XXCONV',
                                    tabname          => 'XXAP_SUPPLIER_BANKACCNTS_STG',
                                    cascade          => true,
                                    estimate_percent => dbms_stats.auto_sample_size,
                                    degree           => dbms_stats.default_degree);

    EXCEPTION
       WHEN OTHERS
          THEN
            print_log_message('Exception creating index/running gather stats');
    END;

   Begin
     print_log_message('Updating XXAP_SUPPLIER_BRANCHES_STG Staging Table');

        Update XXAP_SUPPLIER_BRANCHES_STG t
        Set LEG_BANK_BRANCH_TYPE =
            NVL((Select DECODE(xss.LEG_PAYMENT_METHOD_CODE,
                           'EFT',
                           'ABA',
                           'WIRE',
                           'SWIFT',
                           'OTHER') New_Branch_Type
               from XXAP_SUPPLIER_SITES_STG      xss,
                    XXAP_SUPPLIER_BANKACCNTS_STG xsba
              Where t.LEG_BANK_NAME = xsba.LEG_BANK_NAME
                AND t.LEG_BANK_BRANCH_NAME = xsba.LEG_BRANCH_NAME
                AND xss.LEG_VENDOR_ID = xsba.LEG_VENDOR_ID
                AND xss.LEG_VENDOR_SITE_ID = xsba.LEG_VENDOR_SITE_ID
                AND xss.LEG_ORG_ID =  xsba.LEG_ORG_ID
                AND xss.LEG_REQUEST_ID =  xsba.LEG_REQUEST_ID
                AND t.LEG_REQUEST_ID =  xsba.LEG_REQUEST_ID
                AND Rownum = 1),'OTHER');

     COMMIT;
     print_log_message('Succesfully complete updatation on XXAP_SUPPLIER_BRANCHES_STG Staging Table');

     Declare
        l_stmt1  VARCHAR2(2000);
     BEGIN
        l_stmt1 := 'DROP INDEX XXCONV.XXAP_SUPPLIER_SITES_STG_PC';
        EXECUTE IMMEDIATE l_stmt1;
        print_log_message ( 'Indexes dropped On XXAP_SUPPLIER_SITES_STG');

        dbms_stats.gather_table_stats(ownname          => 'XXCONV',
                                    tabname          => 'XXAP_SUPPLIER_SITES_STG',
                                    cascade          => true,
                                    estimate_percent => dbms_stats.auto_sample_size,
                                    degree           => dbms_stats.default_degree);


      EXCEPTION
       WHEN OTHERS
          THEN
            print_log_message('Exception Dropping index/running gather stats');
     END;

   Exception
     When Others Then
       print_log_message('Error :- Upating Supplier Site Staging Table');
   End;
  END IF;
  /* End CR Changes */

      print_stat();
    END IF;

    IF piv_run_mode = 'PRE-VALIDATE' THEN
      xxetn_debug_pkg.add_debug('In Pre-Validate Mode');

      --call the procedure to check if the custom setups are done
      pre_validate();
      pon_retcode := g_retcode;
    END IF;

    IF piv_run_mode = 'VALIDATE' THEN
      IF (piv_entity IS NULL) THEN
        xxetn_debug_pkg.add_debug('Entity is mandatory for VALIDATE mode ');
        pon_retcode := 2;
        RETURN;
      ELSE
        IF g_batch_id IS NULL THEN
          g_new_batch_id := xxetn_batches_s.NEXTVAL;
          xxetn_debug_pkg.add_debug('New Batch Id: ' || g_new_batch_id);
          g_run_seq_id := xxetn_run_sequences_s.NEXTVAL;
          xxetn_debug_pkg.add_debug('New Run Sequence ID: ' ||
                                    g_run_seq_id);
        ELSE
          g_new_batch_id := g_batch_id;
          g_run_seq_id   := xxetn_run_sequences_s.NEXTVAL;
          xxetn_debug_pkg.add_debug('New Run Sequence ID : ' ||
                                    g_run_seq_id);
        END IF;

        assign_batch_id(); --API for assigning batch id
        --if assign_batch_id fails, exit the program
        IF (g_retcode != 0) THEN
          xxetn_debug_pkg.add_debug('Assign Batch ID failed.Program ended. ');
          RETURN;
        END IF;

        --Setting the run sequence id for error framework
        xxetn_common_error_pkg.g_run_seq_id := g_run_seq_id;
        xxetn_debug_pkg.add_debug('In Validate Mode');

        --call the procedure for validating data based on entity
        IF piv_entity = g_bank THEN
          validate_banks();
        END IF; -- IF piv_entity = g_bank

        IF piv_entity = g_branch THEN
          validate_branches();
        END IF;

        IF piv_entity = g_account THEN
          validate_bank_accounts();
        END IF;

        --Manoj:START
        IF piv_entity = g_supplier THEN
          validate_supplier(l_ret_code);
        END IF;

        IF piv_entity = g_supplier_sites THEN
          validate_supplier_sites(l_ret_code);

       /* Duplicate Supplier site codes update logic date on 05 Oct 2015
          This logic update site code if site code is same but address line1 is different*/
        print_log_message('Dup_SupplierSite_Upd procedure Starts '||to_char(Sysdate,'DD-MON-YYYY HH24:MI:SS'));
        Dup_SupplierSite_Upd(l_err_msg, l_ret_code,g_new_batch_id ,g_request_id);
        print_log_message('Dup_SupplierSite_Upd procedure Ends '||to_char(Sysdate,'DD-MON-YYYY HH24:MI:SS'));

         END IF;

        IF piv_entity = g_supplier_contacts THEN

          /* CV 40 page # 22  Update last with Seq UNKNOWN */
          Update_supp_cont_last_name(l_ret_code);

          validate_supplier_contacts(l_ret_code);
        END IF;
        --Manoj:END

        /** Added for v1.1 **/
        IF piv_entity = g_int_accts -- If Entity is Supplier Intermediary Accounts
         THEN
          validate_int_accts();
        END IF;

      END IF;

      print_stat();
    END IF;

    IF piv_run_mode = 'CONVERSION' THEN
      xxetn_debug_pkg.add_debug('In Conversion Mode');
      IF (piv_entity IS NULL) THEN
        xxetn_debug_pkg.add_debug('Entity is mandatory for CONVERSION mode ');
        pon_retcode := 2;
        RETURN;
      ELSE
        IF (pin_batch_id IS NULL) THEN
          xxetn_debug_pkg.add_debug('Batch ID is mandatory for CONVERSION mode ');
          pon_retcode := 2;
          RETURN;
        ELSE
          g_run_seq_id := xxetn_run_sequences_s.NEXTVAL;
          xxetn_debug_pkg.add_debug('New Run Sequence ID : ' ||
                                    g_run_seq_id);
          g_new_batch_id := pin_batch_id;
          --Setting the run sequence id for error framework
          xxetn_common_error_pkg.g_run_seq_id := g_run_seq_id;

          -- Call import procedure for BANK
          IF piv_entity = g_bank THEN

            xxetn_debug_pkg.add_debug('---------------------------------------------');
            xxetn_debug_pkg.add_debug('PROCEDURE: create_bank' || CHR(10));
            create_banks;

          END IF; -- IF piv_entity = g_bank

          -- Call import procedure for BRANCH
          IF piv_entity = g_branch THEN

            xxetn_debug_pkg.add_debug('---------------------------------------------');
            xxetn_debug_pkg.add_debug('PROCEDURE: create_branch' ||
                                      CHR(10));
            create_branches;

          END IF; -- IF piv_entity = g_branch

          -- Call import procedure for BANK ACCOUNTS
          IF piv_entity = g_account THEN

            xxetn_debug_pkg.add_debug('---------------------------------------------');
            xxetn_debug_pkg.add_debug('PROCEDURE: create_bank_accounts' ||
                                      CHR(10));
            create_bank_accounts;

          END IF; -- IF piv_entity = g_account

          --Manoj:START
          IF piv_entity = g_supplier THEN
            xxetn_debug_pkg.add_debug('---------------------------------------------');
            xxetn_debug_pkg.add_debug('PROCEDURE: create_suppliers' ||
                                      CHR(10));
            create_suppliers(l_ret_code);
          END IF;

          IF piv_entity = g_supplier_sites THEN
            xxetn_debug_pkg.add_debug('---------------------------------------------');
            xxetn_debug_pkg.add_debug('PROCEDURE: create_supplier_sites' ||
                                      CHR(10));
            create_supplier_sites(l_ret_code);
          END IF;

          IF piv_entity = g_supplier_contacts THEN
            xxetn_debug_pkg.add_debug('---------------------------------------------');
            xxetn_debug_pkg.add_debug('PROCEDURE: create_supplier_contacts' ||
                                      CHR(10));
            create_supplier_contacts(l_ret_code);
          END IF;
          --Manoj:END

          -- /** Added for v1.1 **/
          IF piv_entity = g_int_accts THEN
            xxetn_debug_pkg.add_debug('---------------------------------------------');
            xxetn_debug_pkg.add_debug('PROCEDURE: create_int_accts' ||
                                      CHR(10));
            create_int_accts;
          END IF;

        END IF; -- IF pin_batch_id IS NULL
      END IF; -- IF piv_entity IS NULL IS NULL
      print_stat();
    END IF;

    IF piv_run_mode = 'RECONCILE' THEN
      xxetn_debug_pkg.add_debug('In Reconcile Mode');
      g_new_batch_id := pin_batch_id;

      --Call the procedure to print the statistics of the records processed by the conversion
      print_stat();
    END IF;

    /* This program mode is use after all conversion process complete to establish link between
       default and alternate pay site */
    IF piv_run_mode = 'PAY-LINK' THEN
      xxetn_debug_pkg.add_debug('In PAY Link Mode');
      g_new_batch_id := pin_batch_id;
      IF piv_entity = g_supplier_sites THEN
      print_log_message('Supplier Pay Site update program starts');
      Sup_Pay_site_flag_UPD(l_err_msg, l_ret_code);
      print_log_message('Supplier Pay Site update program End');

      print_log_message('Supplier Pay Site link program starts');
      Sup_Default_Pay_site(l_err_msg, l_ret_code);
      print_log_message('Supplier Pay Site link program starts');
      ELSE
       xxetn_debug_pkg.add_debug('Entity is Supplier Site mode Only ');
      END IF;
    END IF;

    print_log_message('+---------------------------------------------------------------------------+');

    print_log_message('Supplier Conversion Ends at: ' ||
                      TO_CHAR(g_sysdate, 'DD-MON-YYYY HH24:MI:SS'));

    pon_retcode := g_retcode;
    pov_errbuf  := g_errbuff;

    -- Insert remaining records in Error Table
    IF g_source_tab.COUNT > 0 THEN
      print_log_message('Inserting remaining records in Error Table');
      xxetn_common_error_pkg.add_error(pov_return_status => l_return_status,
                                       pov_error_msg     => l_err_msg,
                                       pi_source_tab     => g_source_tab,
                                       pin_batch_id      => g_new_batch_id);
      g_source_tab.DELETE;
      g_indx := 0;
    END IF;

  EXCEPTION
    WHEN OTHERS THEN

      pov_errbuf  := 'Error : Main program procedure encounter error. ' ||
                     SUBSTR(SQLERRM, 1, 150);
      pon_retcode := 2;
      print_log_message(pov_errbuf);
  END main;

END xxap_supplier_cnv_pkg;
/
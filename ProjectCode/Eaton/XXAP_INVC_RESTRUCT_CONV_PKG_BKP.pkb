CREATE OR REPLACE PACKAGE BODY xxap_invc_restruct_conv_pkg
------------------------------------------------------------------------------------------
--    Owner        : EATON CORPORATION.
--    Application  : Account Payables
--    Schema       : APPS
--    Compile AS   : APPS
--    File Name    : XXAP_INVC_RESTRUCT_CONV_PKG.pkb
--    Date         : 13-Aug-2019
--    Author       : Vinay Gharge
--    Description  : Package Body for AP Invoice Conversion
--
--    Version      : $ETNHeader:  $
--
--    Parameters  :
--
--    Change History
--    Version     Created By       Date            Comments
--  ======================================================================================
--    v1.0        Vinay Gharge    13-Aug-2019      Initial Version
--    v1.1        Vinay Gharge    01-Oct-2019      Conversion Account fix
--    v1.2        Vinay Gharge    15-Oct-2019      Freeze Changes and PO Matched Invoices
--    v1.3        Vinay Gharge    21-Oct-2019      Zero Tax Line Changes
--   ====================================================================================
------------------------------------------------------------------------------------------
 AS
  -- -----------------------------------------------------------------
  -- Package level Global Placeholders
  -- -----------------------------------------------------------------
  /* Program completion codes */
  g_normal  CONSTANT NUMBER := 0;
  g_warning CONSTANT NUMBER := 1;
  g_error   CONSTANT NUMBER := 2;
  /* Run Mode constants*/
  g_run_mode_loadata        CONSTANT VARCHAR2(20) := 'LOAD-DATA';
  g_run_mode_validate       CONSTANT VARCHAR2(20) := 'VALIDATE';
  g_run_mode_cancel_on_hold CONSTANT VARCHAR2(20) := 'CANCEL-ON-HOLD';
  g_run_mode_conversion     CONSTANT VARCHAR2(20) := 'CONVERSION';
  g_run_mode_reconcilition  CONSTANT VARCHAR2(20) := 'RECONCILE';
  g_run_mode_tie_back       CONSTANT VARCHAR2(20) := 'TIE-BACK';
  g_run_mode_close          CONSTANT VARCHAR2(20) := 'CLOSE';
  /* Flag constants */
  g_flag_ntprocessed CONSTANT VARCHAR2(1) := 'N';
  g_flag_validated   CONSTANT VARCHAR2(1) := 'V';
  g_flag_processed   CONSTANT VARCHAR2(1) := 'P';
  g_flag_completed   CONSTANT VARCHAR2(1) := 'C';
  g_flag_success     CONSTANT VARCHAR2(1) := 'S';
  g_flag_error       CONSTANT VARCHAR2(1) := 'E';
  g_flag_yes         CONSTANT VARCHAR2(1) := 'Y';
  g_flag_no          CONSTANT VARCHAR2(1) := 'N';
  /* Other constants */
  g_source_table       VARCHAR2(30) := 'XXAP_INVC_INTFC_STG';
  g_source_lines_table VARCHAR2(30) := 'XXAP_INVC_LINES_INTFC_STG';
  g_source_hold_table  VARCHAR2(30) := 'XXAP_INVC_HOLDS_CONV_STG';
  g_err_val CONSTANT VARCHAR2(7) := 'ERR_VAL';
  g_err_imp CONSTANT VARCHAR2(7) := 'ERR_IMP';
  /* Global variables */
  g_run_mode        VARCHAR2(14) := NULL;
  g_batch_id        NUMBER := NULL;
  g_po_param        VARCHAR2(10) := NULL;
  g_new_batch_id    NUMBER := NULL;
  g_run_sequence_id NUMBER := NULL;
  g_limit           NUMBER := 100;
  g_bulk_exception EXCEPTION;
  g_invoice_separator VARCHAR2(10) := '-';
  PRAGMA EXCEPTION_INIT(g_bulk_exception, -24381);
  /* WHO Columns */
  g_request_id        NUMBER := apps.fnd_global.conc_request_id;
  g_prog_appl_id      NUMBER := apps.fnd_global.prog_appl_id;
  g_program_id        NUMBER := apps.fnd_global.conc_program_id;
  g_user_id           NUMBER := apps.fnd_global.user_id;
  g_login_id          NUMBER := apps.fnd_global.login_id;
  g_org_id            NUMBER := apps.fnd_global.org_id;
  g_resp_id           NUMBER := apps.fnd_global.resp_id;
  g_resp_appl_id      NUMBER := apps.fnd_global.resp_appl_id;
  g_go_live_date      VARCHAR2(100) := apps.fnd_profile.value('XXPA_BUDGETS_CUTOFF_DATE');
  g_loader_request_id NUMBER;
  g_accounting_date   DATE;

  /* Record Types and Table Types */
  TYPE g_status_invc_rec IS RECORD(
    interface_txn_id xxap_invc_intfc_stg.record_id%TYPE,
    status_flag      xxap_invc_intfc_stg.status_flag%TYPE,
    error_type       xxap_invc_intfc_stg.error_type%TYPE,
    message          VARCHAR2(2000));

  TYPE status_invc_ttype IS TABLE OF g_status_invc_rec INDEX BY BINARY_INTEGER;

  g_status_invc_ttype status_invc_ttype;

  /* Record Types and Table Types */
  TYPE g_status_invc_lines_rec IS RECORD(
    record_line_id xxap_invc_lines_intfc_stg.record_line_id%TYPE,
    status_flag    xxap_invc_lines_intfc_stg.status_flag%TYPE,
    error_type     xxap_invc_lines_intfc_stg.error_type%TYPE,
    message        VARCHAR2(2000));

  TYPE status_invc_lines_ttype IS TABLE OF g_status_invc_lines_rec INDEX BY BINARY_INTEGER;

  g_status_invc_lines_ttype status_invc_lines_ttype;

  -- =============================================================================
  -- Procedure: debug
  -- =============================================================================
  --   Common procedure to pring message to concurrent program log
  -- =============================================================================
  --  Input Parameters :
  --    p_message  : Message Text
  --  Output Parameters :
  --    No Output parameters
  -- -----------------------------------------------------------------------------
  PROCEDURE debug(p_message IN VARCHAR2) IS
    l_error_message VARCHAR2(2000);
  BEGIN
    xxetn_debug_pkg.add_debug(piv_debug_msg => p_message);
  EXCEPTION
    WHEN OTHERS THEN
      l_error_message := substr('Exception in Procedure XXPA_PRJTSK_CNV_PKG.debug. SQLERRM ' ||
                                SQLERRM,
                                1,
                                2000);
      fnd_file.put_line(fnd_file.log, l_error_message);
  END debug;

  -- =============================================================================
  -- Procedure: log_error
  -- =============================================================================
  --   Common procedure to insert record into common error table
  -- =============================================================================
  --  Input Parameters :
  --    pin_batch_id        : Batch Id
  --    pin_run_sequence_id : Run Requence Id
  --    p_source_tab_type   : Error Table Type (xxetn_common_error_pkg.g_source_tab_type)
  --  Output Parameters :
  --    pov_return_status    :
  --    pov_error_message    : Error message in case of any failure
  -- -----------------------------------------------------------------------------
  PROCEDURE log_error(p_source_tab_type IN xxetn_common_error_pkg.g_source_tab_type,
                      pov_return_status OUT NOCOPY VARCHAR2,
                      pov_error_message OUT NOCOPY VARCHAR2) IS
    process_exception EXCEPTION;
    l_return_status VARCHAR2(1) := NULL;
    l_error_message VARCHAR2(2000) := NULL;
  BEGIN
    xxetn_common_error_pkg.add_error(pov_return_status   => l_return_status,
                                     pov_error_msg       => l_error_message,
                                     pin_batch_id        => g_new_batch_id,
                                     pin_iface_load_id   => NULL,
                                     pin_run_sequence_id => g_run_sequence_id,
                                     pi_source_tab       => p_source_tab_type,
                                     piv_active_flag     => g_flag_yes,
                                     pin_program_id      => g_program_id,
                                     pin_request_id      => g_request_id);
  
    IF l_error_message IS NOT NULL
    THEN
      RAISE process_exception;
    END IF;
  EXCEPTION
    WHEN process_exception THEN
      pov_return_status := g_error;
      pov_error_message := l_error_message;
      debug(l_error_message);
    WHEN OTHERS THEN
      l_error_message   := substr('Exception in Procedure log_error. ' ||
                                  SQLERRM,
                                  1,
                                  1999);
      pov_return_status := g_error;
      pov_error_message := l_error_message;
      debug(l_error_message);
  END log_error;

  -- =============================================================================
  -- Procedure: print_report
  -- =============================================================================
  --   To print program stats at the end of each run and also executed when program
  --   is submitted in RECONCILE mode
  -- =============================================================================
  --  Input Parameters :
  --    pin_batch_id        : Batch Id
  --  Output Parameters :
  --    p_retcode   : Program Return Code = 0/1/2
  --    p_errbuf    : Error message in case of any failure
  -- -----------------------------------------------------------------------------
  PROCEDURE print_report(p_errbuf   OUT VARCHAR2,
                         p_retcode  OUT NUMBER,
                         p_batch_id IN NUMBER) IS
    l_error_message   VARCHAR2(2000);
    l_total           NUMBER := 0;
    l_validated       NUMBER := 0;
    l_complete        NUMBER := 0;
    l_val_error       NUMBER := 0;
    l_imp_error       NUMBER := 0;
    l_total_lines     NUMBER := 0;
    l_validated_lines NUMBER := 0;
    l_complete_lines  NUMBER := 0;
    l_val_error_lines NUMBER := 0;
    l_imp_error_lines NUMBER := 0;
    l_total_holds     NUMBER := 0;
    l_validated_holds NUMBER := 0;
    l_complete_holds  NUMBER := 0;
    l_val_error_holds NUMBER := 0;
    l_imp_error_holds NUMBER := 0;
  
    CURSOR invoices_cur IS
      SELECT new_invoice_id,
             old_invoice_id
        FROM xxap_invc_intfc_stg
       WHERE status_flag IN ('P', 'C')
         AND batch_id = g_loader_request_id;
  
    CURSOR get_total_cur IS
      SELECT COUNT(*) cnt
        FROM xxap_invc_intfc_stg xops
       WHERE xops.batch_id = nvl(p_batch_id, batch_id);
  
    CURSOR get_total_lines_cur IS
      SELECT COUNT(*) cnt
        FROM xxap_invc_lines_intfc_stg
       WHERE batch_id = nvl(p_batch_id, batch_id);
  
    CURSOR get_total_holds_cur IS
      SELECT COUNT(*) cnt
        FROM xxap_invc_holds_conv_stg
       WHERE batch_id = nvl(p_batch_id, batch_id);
  
    CURSOR get_validated_cur IS
      SELECT COUNT(*) cnt
        FROM xxap_invc_intfc_stg xops
       WHERE xops.batch_id = nvl(p_batch_id, batch_id)
         AND xops.status_flag = 'V';
  
    CURSOR get_validated_lines_cur IS
      SELECT COUNT(*) cnt
        FROM xxap_invc_lines_intfc_stg xops
       WHERE xops.batch_id = nvl(p_batch_id, batch_id)
         AND xops.status_flag = 'V';
  
    CURSOR get_validated_holds_cur IS
      SELECT COUNT(*) cnt
        FROM xxap_invc_holds_conv_stg xops
       WHERE xops.batch_id = nvl(p_batch_id, batch_id)
         AND xops.status_flag = 'V';
  
    CURSOR get_val_error_cur IS
      SELECT COUNT(*) cnt
        FROM xxap_invc_intfc_stg xops
       WHERE xops.batch_id = nvl(p_batch_id, batch_id)
         AND xops.error_type = g_err_val
         AND xops.status_flag = 'E';
  
    CURSOR get_val_error_lines_cur IS
      SELECT COUNT(*) cnt
        FROM xxap_invc_lines_intfc_stg xops
       WHERE xops.batch_id = nvl(p_batch_id, batch_id)
         AND xops.error_type = g_err_val
         AND xops.status_flag = 'E';
  
    CURSOR get_val_error_holds_cur IS
      SELECT COUNT(*) cnt
        FROM xxap_invc_holds_conv_stg xops
       WHERE xops.batch_id = nvl(p_batch_id, batch_id)
         AND xops.error_type = g_err_val
         AND xops.status_flag = 'E';
  
    CURSOR get_complete_cur IS
      SELECT COUNT(*) cnt
        FROM xxap_invc_intfc_stg xops
       WHERE xops.batch_id = nvl(p_batch_id, batch_id)
         AND xops.status_flag IN ('P', 'C');
  
    CURSOR get_complete_lines_cur IS
      SELECT COUNT(*) cnt
        FROM xxap_invc_lines_intfc_stg xops
       WHERE xops.batch_id = nvl(p_batch_id, batch_id)
         AND xops.status_flag IN ('P', 'C');
  
    CURSOR get_complete_holds_cur IS
      SELECT COUNT(*) cnt
        FROM xxap_invc_holds_conv_stg xops
       WHERE xops.batch_id = nvl(p_batch_id, batch_id)
         AND xops.status_flag IN ('P', 'C');
  
    CURSOR get_imp_error_cur IS
      SELECT COUNT(*) cnt
        FROM xxap_invc_intfc_stg xops
       WHERE xops.batch_id = nvl(p_batch_id, batch_id)
         AND xops.error_type = g_err_imp
         AND xops.status_flag = 'E';
  
    CURSOR get_imp_error_lines_cur IS
      SELECT COUNT(*) cnt
        FROM xxap_invc_lines_intfc_stg xops
       WHERE xops.batch_id = nvl(p_batch_id, batch_id)
         AND xops.error_type = g_err_imp
         AND xops.status_flag = 'E';
  
    CURSOR get_imp_error_holds_cur IS
      SELECT COUNT(*) cnt
        FROM xxap_invc_holds_conv_stg xops
       WHERE xops.batch_id = nvl(p_batch_id, batch_id)
         AND xops.error_type = g_err_imp
         AND xops.status_flag = 'E';
  BEGIN
    p_retcode := g_normal;
    p_errbuf  := NULL;
    debug('p_batch_id : ' || p_batch_id || chr(10));
    BEGIN
      l_total       := 0;
      l_total_lines := 0;
      l_total_holds := 0;
    
      FOR get_total_rec IN get_total_cur
      LOOP
        l_total := get_total_rec.cnt;
      END LOOP;
    
      FOR get_total_lines_rec IN get_total_lines_cur
      LOOP
        l_total_lines := get_total_lines_rec.cnt;
      END LOOP;
    
      FOR get_total_holds_rec IN get_total_holds_cur
      LOOP
        l_total_holds := get_total_holds_rec.cnt;
      END LOOP;
    EXCEPTION
      WHEN OTHERS THEN
        debug('Exception occured while fetching total no. of records');
    END;
    BEGIN
      l_validated       := 0;
      l_validated_lines := 0;
      l_validated_holds := 0;
    
      FOR get_validated_rec IN get_validated_cur
      LOOP
        l_validated := get_validated_rec.cnt;
      END LOOP;
    
      FOR get_validated_lines_rec IN get_validated_lines_cur
      LOOP
        l_validated_lines := get_validated_lines_rec.cnt;
      END LOOP;
    
      FOR get_validated_holds_rec IN get_validated_holds_cur
      LOOP
        l_validated_holds := get_validated_holds_rec.cnt;
      END LOOP;
    EXCEPTION
      WHEN OTHERS THEN
        debug('Exception occured while fetching validated records');
    END;
    BEGIN
      l_val_error       := 0;
      l_val_error_lines := 0;
      l_val_error_holds := 0;
    
      FOR get_val_error_rec IN get_val_error_cur
      LOOP
        l_val_error := get_val_error_rec.cnt;
      END LOOP;
    
      FOR get_val_error_lines_rec IN get_val_error_lines_cur
      LOOP
        l_val_error_lines := get_val_error_lines_rec.cnt;
      END LOOP;
    
      FOR get_val_error_holds_rec IN get_val_error_holds_cur
      LOOP
        l_val_error_holds := get_val_error_holds_rec.cnt;
      END LOOP;
    EXCEPTION
      WHEN OTHERS THEN
        debug('Exception occured while fetching validated error records');
    END;
    BEGIN
      l_complete       := 0;
      l_complete_lines := 0;
      l_complete_holds := 0;
    
      FOR get_complete_rec IN get_complete_cur
      LOOP
        l_complete := get_complete_rec.cnt;
      END LOOP;
    
      FOR get_complete_lines_rec IN get_complete_lines_cur
      LOOP
        l_complete_lines := get_complete_lines_rec.cnt;
      END LOOP;
    
      FOR get_complete_holds_rec IN get_complete_holds_cur
      LOOP
        l_complete_holds := get_complete_holds_rec.cnt;
      END LOOP;
    EXCEPTION
      WHEN OTHERS THEN
        debug('Exception occured while fetching tieback records');
    END;
    BEGIN
      l_imp_error       := 0;
      l_imp_error_lines := 0;
      l_imp_error_holds := 0;
    
      FOR get_imp_error_rec IN get_imp_error_cur
      LOOP
        l_imp_error := get_imp_error_rec.cnt;
      END LOOP;
    
      FOR get_imp_error_lines_rec IN get_imp_error_lines_cur
      LOOP
        l_imp_error_lines := get_imp_error_lines_rec.cnt;
      END LOOP;
    
      FOR get_imp_error_holds_rec IN get_imp_error_holds_cur
      LOOP
        l_imp_error_holds := get_imp_error_holds_rec.cnt;
      END LOOP;
    EXCEPTION
      WHEN OTHERS THEN
        debug('Exception occured while fetching tieback error records');
    END;
    fnd_file.put_line(fnd_file.output,
                      'Program Name: Eaton AP Invoices Conversion Program - Site Restructure');
    fnd_file.put_line(fnd_file.output, '  Request Id: ' || g_request_id);
    fnd_file.put_line(fnd_file.output,
                      ' Report Date: ' ||
                      to_char(SYSDATE, 'DD-MON-RRRR HH:MI:SS AM'));
    fnd_file.put_line(fnd_file.output, ' ');
    fnd_file.put_line(fnd_file.output,
                      '.......................................');
    fnd_file.put_line(fnd_file.output, 'Program Parameters');
    fnd_file.put_line(fnd_file.output,
                      '.......................................');
    fnd_file.put_line(fnd_file.output, '       Run Mode : ' || g_run_mode);
    fnd_file.put_line(fnd_file.output, '       Batch Id : ' || g_batch_id);
    fnd_file.put_line(fnd_file.output,
                      '.......................................');
    fnd_file.put_line(fnd_file.output, ' ');
    fnd_file.put_line(fnd_file.output, ' ');
    fnd_file.put_line(fnd_file.output,
                      '---------------------------------------');
    fnd_file.put_line(fnd_file.output,
                      'Records Status Stats - Invoice Header');
    fnd_file.put_line(fnd_file.output,
                      '---------------------------------------');
    fnd_file.put_line(fnd_file.output,
                      '  Total Count of Records : ' || l_total);
    fnd_file.put_line(fnd_file.output,
                      '       Validated Records : ' || l_validated);
    fnd_file.put_line(fnd_file.output,
                      'Validation Error Records : ' || l_val_error);
    fnd_file.put_line(fnd_file.output,
                      '    Import Error Records : ' || l_imp_error);
    fnd_file.put_line(fnd_file.output,
                      '       Converted Records : ' || l_complete);
    fnd_file.put_line(fnd_file.output,
                      '---------------------------------------');
    fnd_file.put_line(fnd_file.output, ' ');
    fnd_file.put_line(fnd_file.output, ' ');
    fnd_file.put_line(fnd_file.output,
                      '---------------------------------------');
    fnd_file.put_line(fnd_file.output,
                      'Records Status Stats - Invoice Lines');
    fnd_file.put_line(fnd_file.output,
                      '---------------------------------------');
    fnd_file.put_line(fnd_file.output,
                      '  Total Count of Records : ' || l_total_lines);
    fnd_file.put_line(fnd_file.output,
                      '       Validated Records : ' || l_validated_lines);
    fnd_file.put_line(fnd_file.output,
                      'Validation Error Records : ' || l_val_error_lines);
    fnd_file.put_line(fnd_file.output,
                      '    Import Error Records : ' || l_imp_error_lines);
    fnd_file.put_line(fnd_file.output,
                      '       Converted Records : ' || l_complete_lines);
    fnd_file.put_line(fnd_file.output,
                      '---------------------------------------');
    fnd_file.put_line(fnd_file.output, ' ');
    fnd_file.put_line(fnd_file.output, ' ');
    fnd_file.put_line(fnd_file.output,
                      '---------------------------------------');
    fnd_file.put_line(fnd_file.output,
                      'Records Status Stats - Invoice Holds');
    fnd_file.put_line(fnd_file.output,
                      '---------------------------------------');
    fnd_file.put_line(fnd_file.output,
                      '  Total Count of Records : ' || l_total_holds);
    fnd_file.put_line(fnd_file.output,
                      '       Validated Records : ' || l_validated_holds);
    fnd_file.put_line(fnd_file.output,
                      'Validation Error Records : ' || l_val_error_holds);
    fnd_file.put_line(fnd_file.output,
                      '    Import Error Records : ' || l_imp_error_holds);
    fnd_file.put_line(fnd_file.output,
                      '       Converted Records : ' || l_complete_holds);
    fnd_file.put_line(fnd_file.output,
                      '---------------------------------------');
  
    IF l_complete > 0
    THEN
      fnd_file.put_line(fnd_file.output,
                        'Legacy Invoice ID        New Invoice ID');
    
      FOR invoices_rec IN invoices_cur
      LOOP
        fnd_file.put_line(fnd_file.output,
                          invoices_rec.old_invoice_id ||
                          '                     ' ||
                          invoices_rec.new_invoice_id);
      END LOOP;
    
    END IF;
  EXCEPTION
    WHEN OTHERS THEN
      l_error_message := substr('Exception in Procedure XXAP_INVC_RESTRUCT_CONV_PKG.print_report. SQLERRM ' ||
                                SQLERRM,
                                1,
                                2000);
      p_retcode       := g_error;
      p_errbuf        := l_error_message;
      debug(p_errbuf);
  END print_report;

  -- =============================================================================
  -- Procedure: validate_conv_vendor_sites
  -- =============================================================================
  --   To Validate whether the Vendor Sites are converted or not
  -- =============================================================================
  --  Input Parameters :
  --    p_vendor_id           : Old Vendor ID
  --    p_old_vendor_site_id  : Old Vendor Site ID
  --    p_target_org_id       : New Org ID
  --  Output Parameters :
  --    p_new_vendor_site_id  : New Vendor Site ID
  --    p_vendor_name         : New Vendor Name
  --    p_vendor_number       : New Vendor Number
  --    p_vendor_site_code    : New Vendor Site Code
  --    p_retcode             : Program Return Code = 0/1/2
  --    p_errbuf              : Error message in case of any failure
  -- -----------------------------------------------------------------------------
  PROCEDURE validate_conv_vendor_sites(p_vendor_id          IN ap_suppliers.vendor_id%TYPE,
                                       p_old_vendor_site_id IN ap_supplier_sites_all.vendor_site_id%TYPE,
                                       p_source_org_id      IN ap_supplier_sites_all.org_id%TYPE,
                                       p_target_org_id      IN xxetn.xxap_supplier_site_res_stg.new_org_id%TYPE,
                                       p_new_vendor_site_id OUT xxetn.xxap_supplier_site_res_stg.new_vendor_site_id%TYPE,
                                       p_vendor_name        OUT ap_suppliers.vendor_name%TYPE,
                                       p_vendor_number      OUT ap_suppliers.segment1%TYPE,
                                       p_vendor_site_code   OUT ap_supplier_sites_all.vendor_site_code%TYPE,
                                       p_errbuf             OUT VARCHAR2,
                                       p_retcode            OUT NUMBER) IS
    l_new_vendor_site_id xxetn.xxap_supplier_site_res_stg.new_vendor_site_id%TYPE;
    l_vendor_name        ap_suppliers.vendor_name%TYPE;
    l_vendor_number      ap_suppliers.segment1%TYPE;
    l_vendor_site_code   ap_supplier_sites_all.vendor_site_code%TYPE;
    l_pay_site_flag      ap_supplier_sites_all.pay_site_flag%TYPE;
    l_inactive_date      ap_supplier_sites_all.inactive_date%TYPE;
  
    CURSOR get_conv_vendors_cur IS
      SELECT asp.vendor_name,
             asp.segment1 vendor_number,
             assa.vendor_site_code,
             aspnew.new_vendor_site_id,
             nvl(assa.pay_site_flag, 'N') pay_site_flag,
             nvl(assa.inactive_date, SYSDATE + 1) inactive_date
        FROM ap_suppliers                     asp,
             ap_supplier_sites_all            assa,
             xxetn.xxap_supplier_site_res_stg aspnew
       WHERE asp.vendor_id = assa.vendor_id
         AND aspnew.vendor_id = asp.vendor_id
         AND aspnew.new_vendor_site_id = assa.vendor_site_id
         AND asp.vendor_id = p_vendor_id
         AND aspnew.vendor_site_id = p_old_vendor_site_id
         AND aspnew.new_org_id = p_target_org_id
         AND aspnew.status_flag IN ('P', 'C')
         AND asp.enabled_flag = 'Y'
         AND trunc(SYSDATE) BETWEEN
             trunc(nvl(asp.start_date_active, SYSDATE - 1)) AND
             trunc(nvl(asp.end_date_active, SYSDATE + 1));
  
    CURSOR get_target_vendor_cur IS
      SELECT asp.vendor_name,
             asp.segment1 vendor_number,
             assa.vendor_site_code,
             assa.vendor_site_id new_vendor_site_id,
             nvl(assa.pay_site_flag, 'N') pay_site_flag,
             nvl(assa.inactive_date, SYSDATE + 1) inactive_date
        FROM ap_suppliers          asp,
             ap_supplier_sites_all assa
       WHERE asp.vendor_id = assa.vendor_id
         AND asp.vendor_id = p_vendor_id
         AND assa.org_id = p_target_org_id
         AND asp.enabled_flag = 'Y'
         AND EXISTS
       (SELECT 1
                FROM ap_supplier_sites_all assa1
               WHERE assa1.vendor_site_id = p_old_vendor_site_id
                 AND assa1.vendor_site_code = assa.vendor_site_code
                 AND assa1.org_id = p_source_org_id
                 AND assa1.vendor_id = assa.vendor_id)
         AND trunc(SYSDATE) BETWEEN
             trunc(nvl(asp.start_date_active, SYSDATE - 1)) AND
             trunc(nvl(asp.end_date_active, SYSDATE + 1));
  BEGIN
    l_new_vendor_site_id := NULL;
    l_vendor_name        := NULL;
    l_vendor_number      := NULL;
    l_vendor_site_code   := NULL;
    l_pay_site_flag      := NULL;
    l_inactive_date      := NULL;
  
    FOR get_conv_vendors_rec IN get_conv_vendors_cur
    LOOP
      l_new_vendor_site_id := get_conv_vendors_rec.new_vendor_site_id;
      l_vendor_name        := get_conv_vendors_rec.vendor_name;
      l_vendor_number      := get_conv_vendors_rec.vendor_number;
      l_vendor_site_code   := get_conv_vendors_rec.vendor_site_code;
      l_pay_site_flag      := get_conv_vendors_rec.pay_site_flag;
      l_inactive_date      := get_conv_vendors_rec.inactive_date;
    END LOOP;
  
    IF l_new_vendor_site_id IS NULL
    THEN
    
      FOR get_target_vendor_rec IN get_target_vendor_cur
      LOOP
        l_new_vendor_site_id := get_target_vendor_rec.new_vendor_site_id;
        l_vendor_name        := get_target_vendor_rec.vendor_name;
        l_vendor_number      := get_target_vendor_rec.vendor_number;
        l_vendor_site_code   := get_target_vendor_rec.vendor_site_code;
        l_pay_site_flag      := get_target_vendor_rec.pay_site_flag;
        l_inactive_date      := get_target_vendor_rec.inactive_date;
      END LOOP;
    
      IF l_new_vendor_site_id IS NULL
      THEN
        p_new_vendor_site_id := NULL;
        p_errbuf             := 'Vendor Site for Vendor ID ' || p_vendor_id ||
                                ' and Target Org ID: ' || p_target_org_id ||
                                ' not defined in the Target System';
        p_retcode            := g_warning;
      ELSE
      
        IF l_pay_site_flag = 'Y' AND l_inactive_date > SYSDATE
        THEN
          p_new_vendor_site_id := l_new_vendor_site_id;
          p_vendor_name        := l_vendor_name;
          p_vendor_number      := l_vendor_number;
          p_vendor_site_code   := l_vendor_site_code;
          p_errbuf             := NULL;
          p_retcode            := g_normal;
        ELSIF l_pay_site_flag <> 'Y'
        THEN
          p_new_vendor_site_id := NULL;
          p_vendor_name        := NULL;
          p_vendor_number      := NULL;
          p_vendor_site_code   := NULL;
          p_errbuf             := 'Vendor Site is Not Pay Site for Vendor ID ' ||
                                  p_vendor_id || ' and Target Org ID: ' ||
                                  p_target_org_id;
          p_retcode            := g_warning;
        ELSE
          p_new_vendor_site_id := NULL;
          p_vendor_name        := NULL;
          p_vendor_number      := NULL;
          p_vendor_site_code   := NULL;
          p_errbuf             := 'Vendor Site is In-Active for Vendor ID ' ||
                                  p_vendor_id || ' and Target Org ID: ' ||
                                  p_target_org_id;
          p_retcode            := g_warning;
        END IF;
      
      END IF;
    
    ELSE
    
      IF l_pay_site_flag = 'Y' AND l_inactive_date > SYSDATE
      THEN
        p_new_vendor_site_id := l_new_vendor_site_id;
        p_vendor_name        := l_vendor_name;
        p_vendor_number      := l_vendor_number;
        p_vendor_site_code   := l_vendor_site_code;
        p_errbuf             := NULL;
        p_retcode            := g_normal;
      ELSIF l_pay_site_flag <> 'Y'
      THEN
        p_new_vendor_site_id := NULL;
        p_vendor_name        := NULL;
        p_vendor_number      := NULL;
        p_vendor_site_code   := NULL;
        p_errbuf             := 'Vendor Site is Not Pay Site for Vendor ID ' ||
                                p_vendor_id || ' and Target Org ID: ' ||
                                p_target_org_id;
        p_retcode            := g_warning;
      ELSE
        p_new_vendor_site_id := NULL;
        p_vendor_name        := NULL;
        p_vendor_number      := NULL;
        p_vendor_site_code   := NULL;
        p_errbuf             := 'Vendor Site is In-Active for Vendor ID ' ||
                                p_vendor_id || ' and Target Org ID: ' ||
                                p_target_org_id;
        p_retcode            := g_warning;
      END IF;
    
    END IF;
  EXCEPTION
    WHEN OTHERS THEN
      p_errbuf             := 'Error: Exception occured in validate_conv_project_id function ' ||
                              substr(SQLERRM, 1, 240);
      p_retcode            := g_error;
      p_new_vendor_site_id := NULL;
  END validate_conv_vendor_sites;

  -- =============================================================================
  -- Procedure: validate_conv_project_id
  -- =============================================================================
  --   To Validate whether the project has been converted or not.
  -- =============================================================================
  --  Input Parameters :
  --    p_old_project_id  : Old Project ID
  --    p_org_id          : Old Org ID
  --  Output Parameters :
  --    p_new_project_id  : New Project ID
  --    p_retcode         : Program Return Code = 0/1/2
  --    p_errbuf          : Error message in case of any failure
  -- -----------------------------------------------------------------------------
  PROCEDURE validate_conv_project_id(p_old_project_id IN pa_projects_all.project_id%TYPE,
                                     p_source_org_id  IN pa_projects_all.org_id%TYPE,
                                     p_target_org_id  IN pa_projects_all.org_id%TYPE,
                                     p_new_project_id OUT pa_projects_all.project_id%TYPE,
                                     p_errbuf         OUT VARCHAR2,
                                     p_retcode        OUT NUMBER) IS
    l_new_project_id pa_projects_all.project_id%TYPE;
  
    CURSOR get_new_proj_id IS
      SELECT ppa1.project_id new_project_id
        FROM pa_projects_all ppa1,
             pa_projects_all ppa2
       WHERE ppa1.attribute1 = ppa2.segment1
         AND ppa1.attribute_category = 'Eaton'
         AND ppa2.project_id = p_old_project_id
         AND ppa2.org_id = p_source_org_id
         AND ppa1.org_id = p_target_org_id
         AND ppa1.project_status_code != 'CLOSED'
         AND ppa1.enabled_flag = 'Y'
         AND trunc(SYSDATE) BETWEEN ppa1.start_date AND
             nvl(ppa1.completion_date, SYSDATE + 1);
  BEGIN
    l_new_project_id := NULL;
  
    FOR rec_new_proj_id IN get_new_proj_id
    LOOP
      l_new_project_id := rec_new_proj_id.new_project_id;
    END LOOP;
  
    IF l_new_project_id IS NULL
    THEN
      p_new_project_id := NULL;
      p_errbuf         := 'Active Project ID not found against Old Project ID: ' ||
                          p_old_project_id || ' and Source Org ID: ' ||
                          p_source_org_id;
      p_retcode        := g_warning;
    ELSE
      p_new_project_id := l_new_project_id;
      p_errbuf         := NULL;
      p_retcode        := g_normal;
    END IF;
  EXCEPTION
    WHEN OTHERS THEN
      p_errbuf         := 'Error: Exception occured in validate_conv_project_id function ' ||
                          substr(SQLERRM, 1, 240);
      p_retcode        := g_error;
      p_new_project_id := NULL;
  END validate_conv_project_id;

  -- =============================================================================
  -- Procedure: validate_pay_terms
  -- =============================================================================
  --   To Validate Payment Terms
  -- =============================================================================
  --  Input and Output Parameters :
  --    p_terms_id                : Payment Terms ID
  --  Output Parameters :
  --    p_terms_name              : Payment Terms Name
  --    p_retcode                 : Program Return Code = 0/1/2
  --    p_errbuf                  : Error message in case of any failure
  -- -----------------------------------------------------------------------------
  PROCEDURE validate_pay_terms(p_terms_id   IN ap_terms.term_id%TYPE,
                               p_terms_name OUT ap_terms.name%TYPE,
                               p_errbuf     OUT VARCHAR2,
                               p_retcode    OUT NUMBER) IS
    l_terms_name ap_terms.name%TYPE;
  
    CURSOR check_pay_terms_cur IS
      SELECT NAME
        FROM ap_terms apt
       WHERE apt.term_id = p_terms_id
         AND apt.enabled_flag = 'Y'
         AND trunc(SYSDATE) BETWEEN
             trunc(nvl(apt.start_date_active, SYSDATE)) AND
             trunc(nvl(apt.end_date_active, SYSDATE));
  BEGIN
    l_terms_name := NULL;
  
    FOR check_pay_terms_rec IN check_pay_terms_cur
    LOOP
      l_terms_name := check_pay_terms_rec.name;
    END LOOP;
  
    IF l_terms_name IS NULL
    THEN
      p_terms_name := NULL;
      p_errbuf     := 'Payment Terms not found or is disabled';
      p_retcode    := g_warning;
    ELSE
      p_terms_name := l_terms_name;
      p_errbuf     := NULL;
      p_retcode    := g_normal;
    END IF;
  EXCEPTION
    WHEN OTHERS THEN
      p_errbuf     := 'Error: Exception occured in validate_pay_terms function ' ||
                      substr(SQLERRM, 1, 240);
      p_retcode    := g_error;
      p_terms_name := NULL;
  END validate_pay_terms;

  -- =============================================================================
  -- Procedure: validate_pay_method
  -- =============================================================================
  --   To Validate Payment Method
  -- =============================================================================
  --  Input and Output Parameters :
  --    p_payment_method_code     : Payment Method
  --  Output Parameters :
  --    p_retcode                 : Program Return Code = 0/1/2
  --    p_errbuf                  : Error message in case of any failure
  -- -----------------------------------------------------------------------------
  PROCEDURE validate_pay_method(p_payment_method_code IN OUT iby_payment_methods_tl.payment_method_code%TYPE,
                                p_errbuf              OUT VARCHAR2,
                                p_retcode             OUT NUMBER) IS
    l_payment_method_code iby_payment_methods_tl.payment_method_code%TYPE;
  
    CURSOR check_pay_methods_cur IS
      SELECT ipm.payment_method_code
        FROM iby_payment_methods_tl ipm
       WHERE ipm.payment_method_code = p_payment_method_code
         AND ipm.language = userenv('LANG');
  BEGIN
    l_payment_method_code := NULL;
  
    FOR check_pay_methods_rec IN check_pay_methods_cur
    LOOP
      l_payment_method_code := check_pay_methods_rec.payment_method_code;
    END LOOP;
  
    IF l_payment_method_code IS NULL
    THEN
      p_payment_method_code := NULL;
      p_errbuf              := 'Payment Methods not found or is disabled';
      p_retcode             := g_warning;
    ELSE
      p_payment_method_code := l_payment_method_code;
      p_errbuf              := NULL;
      p_retcode             := g_normal;
    END IF;
  EXCEPTION
    WHEN OTHERS THEN
      p_errbuf              := 'Error: Exception occured in validate_pay_method function ' ||
                               substr(SQLERRM, 1, 240);
      p_retcode             := g_error;
      p_payment_method_code := NULL;
  END validate_pay_method;

  -- =============================================================================
  -- Procedure: validate_currency_code
  -- =============================================================================
  --   To Validate Currency Code
  -- =============================================================================
  --  Input and Output Parameters :
  --    p_currency_code       : Currency Code
  --  Output Parameters :
  --    p_retcode             : Program Return Code = 0/1/2
  --    p_errbuf              : Error message in case of any failure
  -- -----------------------------------------------------------------------------
  PROCEDURE validate_currency_code(p_currency_code IN OUT fnd_currencies.currency_code%TYPE,
                                   p_errbuf        OUT VARCHAR2,
                                   p_retcode       OUT NUMBER) IS
    l_currency_code fnd_currencies.currency_code%TYPE;
  
    CURSOR check_currency_code_cur IS
      SELECT fc.currency_code
        FROM fnd_currencies fc
       WHERE fc.currency_code = p_currency_code
         AND fc.enabled_flag = g_flag_yes
         AND fc.currency_flag = g_flag_yes
         AND trunc(SYSDATE) BETWEEN
             trunc(nvl(fc.start_date_active, SYSDATE)) AND
             trunc(nvl(fc.end_date_active, SYSDATE));
  BEGIN
    l_currency_code := NULL;
  
    FOR check_currency_code_rec IN check_currency_code_cur
    LOOP
      l_currency_code := check_currency_code_rec.currency_code;
    END LOOP;
  
    IF l_currency_code IS NULL
    THEN
      p_currency_code := NULL;
      p_errbuf        := 'Currency Code not found or is disabled';
      p_retcode       := g_warning;
    ELSE
      p_currency_code := l_currency_code;
      p_errbuf        := NULL;
      p_retcode       := g_normal;
    END IF;
  EXCEPTION
    WHEN OTHERS THEN
      p_errbuf        := 'Error: Exception occured in validate_currency_code function ' ||
                         substr(SQLERRM, 1, 240);
      p_retcode       := g_error;
      p_currency_code := NULL;
  END validate_currency_code;

  -- =============================================================================
  -- Procedure: validate_gl_open_period
  -- =============================================================================
  --   To Validate if the GL period is Open or not
  -- =============================================================================
  --  Input Parameters :
  --    No Input Parameters
  --  Output Parameters :
  --    p_retcode   : Program Return Code = 0/1/2
  --    p_errbuf    : Error message in case of any failure
  -- -----------------------------------------------------------------------------
  /*PROCEDURE validate_gl_open_period(p_set_of_books_id IN gl_period_statuses.set_of_books_id%TYPE,
                                    p_gl_date         IN DATE,
                                    p_errbuf          OUT VARCHAR2,
                                    p_retcode         OUT NUMBER) IS
    l_check_gl_open_period NUMBER := 0;
  
    CURSOR get_gl_period_status_cur IS
      SELECT 1
        FROM gl_period_statuses gps,
             fnd_application    fa,
             gl_ledgers         gl
       WHERE gl.accounted_period_type = gps.period_type
         AND gl.ledger_id = gps.ledger_id
         AND fa.application_short_name = 'SQLGL'
         AND fa.application_id = gps.application_id
         AND gps.set_of_books_id = p_set_of_books_id
         AND gps.closing_status = 'O'
         AND p_gl_date BETWEEN gps.start_date AND gps.end_date
         AND adjustment_period_flag = 'N';
  BEGIN
    l_check_gl_open_period := 0;
  
    FOR get_gl_period_status_rec IN get_gl_period_status_cur
    LOOP
      l_check_gl_open_period := 1;
    END LOOP;
  
    IF l_check_gl_open_period = 0
    THEN
      p_errbuf  := 'GL Period is Not Open for Set of Books ID: ' ||
                   p_set_of_books_id || ' and GL Date: ' ||
                   to_char(p_gl_date, 'DD/MM/YYYY');
      p_retcode := g_error;
    ELSE
      p_errbuf  := NULL;
      p_retcode := g_normal;
    END IF;
  EXCEPTION
    WHEN OTHERS THEN
      p_errbuf  := 'Error: Exception occured in validate_gl_open_period function ' ||
                   substr(SQLERRM, 1, 240);
      p_retcode := g_error;
  END validate_gl_open_period;*/

  -- =============================================================================
  -- Procedure: validate_ap_open_period
  -- =============================================================================
  --   To Validate if the AP period is Open or not
  -- =============================================================================
  --  Input Parameters :
  --    No Input Parameters
  --  Output Parameters :
  --    p_retcode   : Program Return Code = 0/1/2
  --    p_errbuf    : Error message in case of any failure
  -- -----------------------------------------------------------------------------
  PROCEDURE validate_ap_open_period(p_set_of_books_id IN gl_period_statuses.set_of_books_id%TYPE,
                                    p_gl_date         IN DATE,
                                    p_errbuf          OUT VARCHAR2,
                                    p_retcode         OUT NUMBER) IS
    l_check_ap_open_period NUMBER := 0;
  
    CURSOR get_ap_period_status_cur IS
      SELECT 1
        FROM gl_period_statuses gps,
             fnd_application    fa,
             gl_ledgers         gl
       WHERE gl.accounted_period_type = gps.period_type
         AND gl.ledger_id = gps.ledger_id
         AND fa.application_short_name = 'SQLAP'
         AND fa.application_id = gps.application_id
         AND gps.set_of_books_id = p_set_of_books_id
         AND gps.closing_status = 'O'
         AND p_gl_date BETWEEN gps.start_date AND gps.end_date
         AND adjustment_period_flag = 'N';
  BEGIN
    l_check_ap_open_period := 0;
  
    FOR get_ap_period_status_rec IN get_ap_period_status_cur
    LOOP
      l_check_ap_open_period := 1;
    END LOOP;
  
    IF l_check_ap_open_period = 0
    THEN
      p_errbuf  := 'AP Period is Not Open for Set of Books ID: ' ||
                   p_set_of_books_id || ' and GL Date: ' ||
                   to_char(p_gl_date, 'DD/MM/YYYY');
      p_retcode := g_error;
    ELSE
      p_errbuf  := NULL;
      p_retcode := g_normal;
    END IF;
  EXCEPTION
    WHEN OTHERS THEN
      p_errbuf  := 'Error: Exception occured in validate_ap_open_period function ' ||
                   substr(SQLERRM, 1, 240);
      p_retcode := g_error;
  END validate_ap_open_period;

  -- =============================================================================
  -- Procedure: validate_conv_task_id
  -- =============================================================================
  --   To Validate whether the task has been converted or not.
  -- =============================================================================
  --  Input Parameters :
  --    p_new_project_id  : New Project ID
  --    p_old_task_id     : Old Task ID
  --  Output Parameters :
  --    p_new_task_id     : New Task ID
  --    p_retcode         : Program Return Code = 0/1/2
  --    p_errbuf          : Error message in case of any failure
  -- -----------------------------------------------------------------------------
  PROCEDURE validate_conv_task_id(p_new_project_id IN pa_projects_all.project_id%TYPE,
                                  p_old_task_id    IN pa_tasks.task_id%TYPE,
                                  p_new_task_id    OUT pa_tasks.task_id%TYPE,
                                  p_errbuf         OUT VARCHAR2,
                                  p_retcode        OUT NUMBER) IS
    l_new_task_id pa_tasks.task_id%TYPE;
  
    CURSOR get_new_task_id IS
      SELECT pt1.task_id new_task_id
        FROM pa_tasks pt1,
             pa_tasks pt2
       WHERE pt1.task_number = pt2.task_number
         AND pt1.project_id = p_new_project_id
         AND pt2.task_id = p_old_task_id;
  BEGIN
    l_new_task_id := NULL;
  
    FOR rec_new_task_id IN get_new_task_id
    LOOP
      l_new_task_id := rec_new_task_id.new_task_id;
    END LOOP;
  
    IF l_new_task_id IS NULL
    THEN
      p_new_task_id := NULL;
      p_errbuf      := 'Active Task ID not found against Old Task ID: ' ||
                       p_old_task_id;
      p_retcode     := g_warning;
    ELSE
      p_new_task_id := l_new_task_id;
      p_errbuf      := NULL;
      p_retcode     := g_normal;
    END IF;
  EXCEPTION
    WHEN OTHERS THEN
      p_new_task_id := NULL;
      p_errbuf      := 'Error: Exception occured in validate_conv_task_id function ' ||
                       substr(SQLERRM, 1, 240);
      p_retcode     := g_error;
  END validate_conv_task_id;

  -- =============================================================================
  -- Procedure: validate_expenditure_types
  -- =============================================================================
  --   To Validate Expenditure Org, Item Date, and type against the converted projects
  --   and task
  -- =============================================================================
  --  Input Parameters :
  --    p_new_project_id          : New Project ID
  --    p_new_task_id             : New Task ID
  --  Output Parameters :
  --    p_expenditure_org_id      : Expenditure Org ID
  --    p_retcode                 : Program Return Code = 0/1/2
  --    p_errbuf                  : Error message in case of any failure
  -- -----------------------------------------------------------------------------
  PROCEDURE validate_expenditure_types(p_new_project_id     IN pa_projects_all.project_id%TYPE,
                                       p_new_task_id        IN pa_tasks.task_id%TYPE,
                                       p_expenditure_org_id OUT pa_expenditures_all.incurred_by_organization_id%TYPE,
                                       p_errbuf             OUT VARCHAR2,
                                       p_retcode            OUT NUMBER) IS
    l_expenditure_org_id pa_expenditures_all.incurred_by_organization_id%TYPE;
  
    CURSOR get_new_expediture_cur IS
      SELECT carrying_out_organization_id expenditure_organization_id
        FROM pa_projects_all
       WHERE project_id = p_new_project_id;
  BEGIN
    l_expenditure_org_id := NULL;
  
    FOR get_new_expediture_rec IN get_new_expediture_cur
    LOOP
      l_expenditure_org_id := get_new_expediture_rec.expenditure_organization_id;
    END LOOP;
  
    IF l_expenditure_org_id IS NULL
    THEN
      p_expenditure_org_id := NULL;
      p_errbuf             := 'Expenditure Org ID Not found against New Project ID: ' ||
                              p_new_project_id || ' and New Task ID: ' ||
                              p_new_task_id;
      p_retcode            := g_warning;
    ELSE
      p_expenditure_org_id := l_expenditure_org_id;
      p_errbuf             := NULL;
      p_retcode            := g_normal;
    END IF;
  EXCEPTION
    WHEN OTHERS THEN
      p_expenditure_org_id := NULL;
      p_errbuf             := 'Error: Exception occured in validate_expenditure_types function ' ||
                              substr(SQLERRM, 1, 240);
      p_retcode            := g_error;
  END validate_expenditure_types;

  -- =============================================================================
  -- Procedure: validate_pay_sch
  -- =============================================================================
  --   To Validate Payment Schedules
  -- =============================================================================
  --  Input Parameters :
  --    p_old_invoice_id          : Old Invoice ID
  --    p_old_org_id              : Old Org ID
  --  Output Parameters :
  --    p_payment_priority        : Payment Priority
  --    p_due_date                : Due Date
  --    p_retcode                 : Program Return Code = 0/1/2
  --    p_errbuf                  : Error message in case of any failure
  -- -----------------------------------------------------------------------------
  PROCEDURE validate_pay_sch(p_old_invoice_id   IN ap_invoices_all.invoice_id%TYPE,
                             p_old_org_id       IN ap_invoices_all.org_id%TYPE,
                             p_payment_priority OUT ap_payment_schedules_all.payment_priority%TYPE,
                             p_due_date         OUT ap_payment_schedules_all.due_date%TYPE,
                             p_errbuf           OUT VARCHAR2,
                             p_retcode          OUT NUMBER) IS
    l_payment_priority ap_payment_schedules_all.payment_priority%TYPE;
    l_due_date         ap_payment_schedules_all.due_date%TYPE;
  
    CURSOR get_pay_sch_cur IS
      SELECT apsa.payment_priority,
             apsa.due_date
        FROM ap_payment_schedules_all apsa
       WHERE apsa.invoice_id = p_old_invoice_id
         AND apsa.payment_num =
             (SELECT MAX(a.payment_num)
                FROM ap_payment_schedules_all a
               WHERE a.invoice_id = apsa.invoice_id)
         AND apsa.amount_remaining <> 0
         AND apsa.org_id = p_old_org_id;
  BEGIN
    l_payment_priority := NULL;
    l_due_date         := NULL;
  
    FOR get_pay_sch_rec IN get_pay_sch_cur
    LOOP
      l_payment_priority := get_pay_sch_rec.payment_priority;
      l_due_date         := get_pay_sch_rec.due_date;
    END LOOP;
  
    IF l_due_date IS NULL
    THEN
      p_payment_priority := NULL;
      p_due_date         := NULL;
      p_errbuf           := 'Payment Schedules not found';
      p_retcode          := g_warning;
    ELSE
      p_payment_priority := l_payment_priority;
      p_due_date         := l_due_date;
      p_errbuf           := NULL;
      p_retcode          := g_normal;
    END IF;
  EXCEPTION
    WHEN OTHERS THEN
      p_payment_priority := NULL;
      p_due_date         := NULL;
      p_errbuf           := 'Error: Exception occured in validate_pay_sch function ' ||
                            substr(SQLERRM, 1, 240);
      p_retcode          := g_error;
  END validate_pay_sch;

  -- =============================================================================
  -- Procedure: validate_party_sites
  -- =============================================================================
  --   To Validate Party and Party Site ID are present for the new vendor site
  -- =============================================================================
  --  Input Parameters :
  --    p_vendor_id               : Old Vendor ID
  --    p_new_vendor_site_id      : New Vendor Site ID
  --  Output Parameters :
  --    p_party_id                : Party ID
  --    p_party_site_id           : Party Site ID
  --    p_retcode                 : Program Return Code = 0/1/2
  --    p_errbuf                  : Error message in case of any failure
  -- -----------------------------------------------------------------------------
  PROCEDURE validate_party_sites(p_vendor_id          IN ap_suppliers.vendor_id%TYPE,
                                 p_new_vendor_site_id IN ap_supplier_sites_all.vendor_site_id%TYPE,
                                 p_party_id           OUT hz_parties.party_id%TYPE,
                                 p_party_site_id      OUT hz_party_sites.party_site_id%TYPE,
                                 p_errbuf             OUT VARCHAR2,
                                 p_retcode            OUT NUMBER) IS
    l_party_id      hz_parties.party_id%TYPE;
    l_party_site_id hz_party_sites.party_site_id%TYPE;
  
    CURSOR get_party_sites_cur IS
      SELECT aps.party_id,
             apss.party_site_id
        FROM ap_suppliers          aps,
             ap_supplier_sites_all apss
       WHERE aps.vendor_id = apss.vendor_id
         AND aps.vendor_id = p_vendor_id
         AND apss.vendor_site_id = p_new_vendor_site_id;
  BEGIN
    l_party_id      := NULL;
    l_party_site_id := NULL;
  
    FOR get_party_sites_rec IN get_party_sites_cur
    LOOP
      l_party_id      := get_party_sites_rec.party_id;
      l_party_site_id := get_party_sites_rec.party_site_id;
    END LOOP;
  
    IF l_party_id IS NULL
    THEN
      p_party_id      := NULL;
      p_party_site_id := NULL;
      p_errbuf        := 'Party ID and Party Site ID Not found against New Supplier Site ID: ' ||
                         p_new_vendor_site_id;
      p_retcode       := g_warning;
    ELSE
      p_party_id      := l_party_id;
      p_party_site_id := l_party_site_id;
      p_errbuf        := NULL;
      p_retcode       := g_normal;
    END IF;
  EXCEPTION
    WHEN OTHERS THEN
      p_party_id      := NULL;
      p_party_site_id := NULL;
      p_errbuf        := 'Error: Exception occured in validate_new_expenditure_types function ' ||
                         substr(SQLERRM, 1, 240);
      p_retcode       := g_error;
  END validate_party_sites;

  --
  -- ========================
  -- Procedure: validate_duplicate_invoices
  -- =============================================================================
  --   This procedure is used to check invoices containing 2 plant numbers
  -- =============================================================================
  --  Input Parameters :
  --  p_old_invoice_id  : Source Invoice ID
  --  p_batch_id        : Batch ID
  --  p_record_id       : Unique Key in the Staging Table
  --  Output Parameters :
  --  p_errbuf          : Return error
  --  p_retcode         : Return status
  -- -----------------------------------------------------------------------------
  --
  PROCEDURE validate_duplicate_invoices(p_errbuf         OUT VARCHAR2,
                                        p_retcode        OUT NUMBER,
                                        p_old_invoice_id IN xxap_invc_intfc_stg.old_invoice_id%TYPE,
                                        p_org_id         IN xxap_invc_intfc_stg.org_id%TYPE,
                                        p_source_le      IN xxap_invc_intfc_stg.source_le%TYPE,
                                        p_source_plant   IN xxap_invc_intfc_stg.source_plant%TYPE,
                                        p_batch_id       IN xxap_invc_intfc_stg.batch_id%TYPE,
                                        p_record_id      IN xxap_invc_intfc_stg.record_id%TYPE) IS
    l_invoice_count NUMBER := 0;
  
    CURSOR get_dupl_invoices_cur IS
      SELECT COUNT(1) cnt
        FROM xxap_invc_intfc_stg
       WHERE old_invoice_id = p_old_invoice_id
         AND record_id = p_record_id
         AND batch_id = p_batch_id
         AND org_id = p_org_id
         AND operation = 'COPY'
         AND destination_le LIKE '%,%'
       GROUP BY old_invoice_id;
  
    CURSOR check_other_plant_cur IS
      SELECT COUNT(1) cnt
        FROM ap_invoices_all aia
       WHERE aia.invoice_id = p_old_invoice_id
         AND aia.org_id = p_org_id
         AND EXISTS
       (SELECT 1
                FROM ap_invoice_lines_all         aila,
                     ap_invoice_distributions_all aida
               WHERE aila.invoice_id = aia.invoice_id
                 AND aida.invoice_id = aia.invoice_id
                 AND aida.org_id = aia.org_id
                 AND aida.org_id = aila.org_id
                 AND aida.amount <> 0
                 AND aila.amount <> 0
                 AND aila.line_type_lookup_code <> 'TAX'
                 AND aida.invoice_line_number = aila.line_number
                 AND EXISTS
               (SELECT 1
                        FROM gl_code_combinations gcc
                       WHERE aida.dist_code_combination_id =
                             gcc.code_combination_id
                         AND gcc.segment1 = p_source_le /*Source LE*/
                         AND gcc.segment2 NOT IN
                             (SELECT CAST(TRIM(regexp_substr(p_source_plant,
                                                             '[^,]+',
                                                             1,
                                                             LEVEL)) AS
                                          VARCHAR2(30))
                                FROM dual
                              CONNECT BY LEVEL <=
                                         regexp_count(p_source_plant, ',') + 1)));
  BEGIN
    debug('Function: validate_duplicate_invoices');
    p_errbuf  := NULL;
    p_retcode := g_normal;
  
    FOR get_dupl_invoices_rec IN get_dupl_invoices_cur
    LOOP
      l_invoice_count := get_dupl_invoices_rec.cnt;
    END LOOP;
  
    IF l_invoice_count > 0
    THEN
      p_errbuf  := 'Invoice has more than one LE in the distribution at Target';
      p_retcode := g_warning;
      debug(p_errbuf);
    ELSE
    
      FOR check_other_plant_rec IN check_other_plant_cur
      LOOP
        l_invoice_count := check_other_plant_rec.cnt;
      END LOOP;
    
      IF l_invoice_count > 0
      THEN
        p_errbuf  := 'Invoice has more than one plant in the distribution at source';
        p_retcode := g_warning;
        debug(p_errbuf);
      ELSE
        p_errbuf  := NULL;
        p_retcode := g_normal;
      END IF;
    
    END IF;
  EXCEPTION
    WHEN OTHERS THEN
      p_errbuf  := substr('Exception in Function validate_duplicate_invoices. ' ||
                          SQLERRM,
                          1,
                          2000);
      p_retcode := g_error;
      debug(p_errbuf);
  END validate_duplicate_invoices;

  --
  -- ========================
  -- Procedure: validate_tax
  -- =============================================================================
  --   This procedure is used to validate Tax Codes
  -- =============================================================================
  --  Input Parameters :
  --  p_org_id                : Source Invoice ID
  --  p_tax_rate_code         : Tax Rate Code
  --  p_tax_regime_code       : Tax Regime Code
  --  p_tax_status_code       : Tax Status Code
  --  p_tax_jurisdiction_code : Tax Jurisdiction Code
  --  p_tax_rate_id           : Tax Rate ID
  --  p_percentage_rate       : Tax Percentage Rate
  --  Output Parameters :
  --  pov_errbuf              : Return error
  --  pon_retcode             : Return status
  --  Input-Output Parameters :
  --  p_tax                   : Tax
  -- -----------------------------------------------------------------------------
  PROCEDURE validate_tax(p_org_id                IN NUMBER,
                         p_tax                   OUT zx_rates_b.tax%TYPE,
                         p_tax_rate_code         IN OUT zx_rates_b.tax_rate_code%TYPE,
                         p_tax_regime_code       OUT zx_rates_b.tax_regime_code%TYPE,
                         p_tax_status_code       OUT zx_rates_b.tax_status_code%TYPE,
                         p_tax_jurisdiction_code OUT zx_rates_b.tax_jurisdiction_code%TYPE,
                         p_tax_rate_id           IN OUT zx_rates_b.tax_rate_id%TYPE,
                         p_percentage_rate       OUT zx_rates_b.percentage_rate%TYPE,
                         p_errbuf                OUT VARCHAR2,
                         p_retcode               OUT NUMBER) IS
    l_record_cnt NUMBER := 0;
  
    CURSOR get_tax_rates_cur IS
      SELECT zrb.tax,
             zrb.tax_regime_code,
             zrb.tax_rate_code,
             zrb.tax_status_code,
             zrb.tax_jurisdiction_code,
             zrb.tax_rate_id,
             zrb.percentage_rate
        FROM zx_rates_b zrb
       WHERE EXISTS
       (SELECT 1
                FROM zx_accounts            za,
                     hr_operating_units     hrou,
                     gl_ledgers             gl,
                     fnd_id_flex_structures fifs,
                     zx_regimes_b           zb
               WHERE za.internal_organization_id = hrou.organization_id
                 AND gl.ledger_id = za.ledger_id
                 AND EXISTS
               (SELECT 1
                        FROM fnd_application_vl fap
                       WHERE fifs.application_id = fap.application_id
                         AND fap.application_short_name = 'SQLGL')
                 AND fifs.id_flex_code = 'GL#'
                 AND fifs.id_flex_num = gl.chart_of_accounts_id
                 AND zrb.tax_rate_id = za.tax_account_entity_id
                 AND za.tax_account_entity_code = 'RATES'
                 AND hrou.organization_id = p_org_id
                 AND trunc(SYSDATE) BETWEEN
                     trunc(nvl(zb.effective_from, SYSDATE)) AND
                     trunc(nvl(zb.effective_to, SYSDATE)))
         AND nvl(zrb.tax_rate_code, 1) =
             nvl(p_tax_rate_code, nvl(zrb.tax_rate_code, 1))
         AND zrb.tax_rate_id = p_tax_rate_id
         AND trunc(SYSDATE) BETWEEN trunc(nvl(zrb.effective_from, SYSDATE)) AND
             trunc(nvl(zrb.effective_to, SYSDATE))
         AND zrb.active_flag = 'Y';
  BEGIN
    debug(' START  PROCEDURE : validate_tax = ' || p_tax_rate_code);
    l_record_cnt := 0;
    BEGIN
    
      FOR get_tax_rates_rec IN get_tax_rates_cur
      LOOP
        l_record_cnt            := l_record_cnt + 1;
        p_tax                   := get_tax_rates_rec.tax;
        p_tax_regime_code       := get_tax_rates_rec.tax_regime_code;
        p_tax_rate_code         := get_tax_rates_rec.tax_rate_code;
        p_tax_status_code       := get_tax_rates_rec.tax_status_code;
        p_tax_jurisdiction_code := get_tax_rates_rec.tax_jurisdiction_code;
        p_tax_rate_id           := get_tax_rates_rec.tax_rate_id;
        p_percentage_rate       := get_tax_rates_rec.percentage_rate;
      END LOOP;
    
      IF l_record_cnt = 0
      THEN
        p_tax                   := NULL;
        p_tax_regime_code       := NULL;
        p_tax_rate_code         := NULL;
        p_tax_status_code       := NULL;
        p_tax_jurisdiction_code := NULL;
        p_tax_rate_id           := NULL;
        p_percentage_rate       := NULL;
        p_errbuf                := 'No Data found while fetching tax rate code: ' ||
                                   p_tax_rate_code;
        p_retcode               := g_error;
      ELSIF l_record_cnt > 1
      THEN
        p_tax                   := NULL;
        p_tax_regime_code       := NULL;
        p_tax_rate_code         := NULL;
        p_tax_status_code       := NULL;
        p_tax_jurisdiction_code := NULL;
        p_tax_rate_id           := NULL;
        p_percentage_rate       := NULL;
        p_errbuf                := 'More than one record found while fetching tax rate code: ' ||
                                   p_tax_rate_code;
        p_retcode               := g_error;
      END IF;
    EXCEPTION
      WHEN OTHERS THEN
        p_tax                   := NULL;
        p_tax_regime_code       := NULL;
        p_tax_rate_code         := NULL;
        p_tax_status_code       := NULL;
        p_tax_jurisdiction_code := NULL;
        p_tax_rate_id           := NULL;
        p_percentage_rate       := NULL;
        p_errbuf                := substr('Error in Procedure-validate_tax: ' ||
                                          SQLERRM,
                                          1,
                                          2000);
        p_retcode               := g_error;
        debug(p_errbuf);
    END;
    debug(' END  PROCEDURE : validate_tax = ' || p_tax_rate_code);
  EXCEPTION
    WHEN OTHERS THEN
      p_tax                   := NULL;
      p_tax_regime_code       := NULL;
      p_tax_rate_code         := NULL;
      p_tax_status_code       := NULL;
      p_tax_jurisdiction_code := NULL;
      p_tax_rate_id           := NULL;
      p_percentage_rate       := NULL;
      p_errbuf                := substr('Error in Procedure-validate_tax: ' ||
                                        SQLERRM,
                                        1,
                                        2000);
      p_retcode               := g_error;
      debug(p_errbuf);
  END validate_tax;

  -- =============================================================================
  -- Procedure: validate_ext_bank_acc_id
  -- =============================================================================
  --   To Validate External Bank Account ID
  -- =============================================================================
  --  Input/Output Parameters :
  --    p_ext_bank_account_id     : External Bank Account ID
  --  Output Parameters :
  --    p_retcode                 : Program Return Code = 0/1/2
  --    p_errbuf                  : Error message in case of any failure
  -- -----------------------------------------------------------------------------
  PROCEDURE validate_ext_bank_acc_id(p_ext_bank_account_id IN OUT iby_ext_bank_accounts.ext_bank_account_id%TYPE,
                                     p_errbuf              OUT VARCHAR2,
                                     p_retcode             OUT NUMBER) IS
  
    l_ext_bank_account_id iby_ext_bank_accounts.ext_bank_account_id%TYPE;
    CURSOR get_ext_bank_acc_id_cur IS
      SELECT ext_bank_account_id
        FROM iby_ext_bank_accounts
       WHERE ext_bank_account_id = p_ext_bank_account_id
         AND end_date IS NULL;
  BEGIN
    l_ext_bank_account_id := NULL;
  
    FOR get_ext_bank_acc_id_rec IN get_ext_bank_acc_id_cur
    LOOP
      l_ext_bank_account_id := get_ext_bank_acc_id_rec.ext_bank_account_id;
    END LOOP;
  
    /*No Validation Error to be thrown if Bank Acount is End-dated*/
    p_ext_bank_account_id := l_ext_bank_account_id;
    p_errbuf              := NULL;
    p_retcode             := g_normal;
  
  EXCEPTION
    WHEN OTHERS THEN
      p_ext_bank_account_id := NULL;
      p_errbuf              := 'Error: Exception occured in validate_ext_bank_acc_id function ' ||
                               substr(SQLERRM, 1, 240);
      p_retcode             := g_error;
  END validate_ext_bank_acc_id;
  -- =============================================================================
  -- Procedure: validate_glb_attr_cat
  -- =============================================================================
  --   To Validate Global Attribute Category
  -- =============================================================================
  --  Input/Output Parameters :
  --    p_global_attribute_category     : Global Attribute Category
  --  Output Parameters :
  --    p_retcode                 : Program Return Code = 0/1/2
  --    p_errbuf                  : Error message in case of any failure
  -- -----------------------------------------------------------------------------
  PROCEDURE validate_glb_attr_cat(p_global_attribute_category IN OUT fnd_lookup_values.lookup_code%TYPE,
                                  p_errbuf                    OUT VARCHAR2,
                                  p_retcode                   OUT NUMBER) IS
  
    l_global_attribute_category fnd_lookup_values.meaning%TYPE;
    CURSOR get_glb_attr_cat_cur IS
      SELECT flv.meaning
        FROM fnd_lookup_values flv
       WHERE flv.lookup_type = 'XXETN_RESTRUCT_REG_CONTEXT_VAL'
         AND flv.language = userenv('LANG')
         AND flv.enabled_flag = 'Y'
         AND SYSDATE BETWEEN nvl(flv.start_date_active, SYSDATE - 1) AND
             nvl(flv.end_date_active, SYSDATE + 1)
         AND (flv.lookup_code = upper(p_global_attribute_category) OR
             flv.meaning = p_global_attribute_category);
  BEGIN
    l_global_attribute_category := NULL;
  
    FOR get_glb_attr_cat_rec IN get_glb_attr_cat_cur
    LOOP
      l_global_attribute_category := get_glb_attr_cat_rec.meaning;
    END LOOP;
    IF l_global_attribute_category IS NULL
    THEN
      p_global_attribute_category := p_global_attribute_category;
      p_errbuf                    := 'Global Attribute Category not defined in the lookup XXETN_RESTRUCT_REG_CONTEXT_VAL';
      p_retcode                   := g_warning;
    ELSE
      p_global_attribute_category := l_global_attribute_category;
      p_errbuf                    := NULL;
      p_retcode                   := g_normal;
    END IF;
  
  EXCEPTION
    WHEN OTHERS THEN
      p_global_attribute_category := NULL;
      p_errbuf                    := 'Error: Exception occured in validate_ext_bank_acc_id function ' ||
                                     substr(SQLERRM, 1, 240);
      p_retcode                   := g_error;
  END validate_glb_attr_cat;

  --
  -- ========================
  -- Procedure: get_po_info
  -- =============================================================================
  --   This procedure is to get the PO Number and Receipt Required Flag
  -- =============================================================================
  --  Input Parameters :
  --  p_old_po_header_id          : Old PO Header ID
  --  p_old_po_line_id            : Old PO Line ID
  --  p_old_line_location_id      : Old PO Line Location ID
  --  p_old_po_distribution_id    : Old PO Distribution ID
  --  p_old_org_id                : Old Org ID
  --  p_source_le                 : Source LE
  --  p_source_plant              : Source Plant
  --  p_destination_le            : Target LE
  --  p_destination_plant         : Target Plant
  --  Output Parameters :
  --  p_receipt_required_flag     : Receipt Required Flag
  --  p_po_info                   : PO Info to be stored in attribute1
  --  p_po_number                 : PO Number
  --  p_po_line_number            : PO Line Number
  --  p_po_shipment_number        : PO Shipment Number
  --  p_po_distribution_number    : PO Distribution Number
  -- -----------------------------------------------------------------------------
  --
  PROCEDURE get_po_info(p_old_po_header_id       IN po_headers_all.po_header_id%TYPE,
                        p_old_po_line_id         IN po_lines_all.po_line_id%TYPE,
                        p_old_line_location_id   IN po_line_locations_all.line_location_id%TYPE,
                        p_old_po_distribution_id IN po_distributions_all.po_distribution_id%TYPE,
                        p_old_org_id             IN ap_invoice_distributions_all.org_id%TYPE,
                        p_source_le              IN xxap_invc_intfc_stg.source_le%TYPE,
                        p_source_plant           IN xxap_invc_intfc_stg.source_plant%TYPE,
                        p_destination_le         IN xxap_invc_intfc_stg.destination_le%TYPE,
                        p_destination_plant      IN xxap_invc_intfc_stg.destination_plant%TYPE,
                        p_receipt_required_flag  OUT po_line_locations_all.receipt_required_flag%TYPE,
                        p_po_info                OUT VARCHAR2,
                        p_po_number              OUT po_headers_all.segment1%TYPE,
                        p_po_line_number         OUT po_lines_all.line_num%TYPE,
                        p_po_shipment_number     OUT po_line_locations_all.shipment_num%TYPE,
                        p_po_distribution_number OUT po_distributions_all.distribution_num%TYPE) IS
    l_receipt_required_flag  po_line_locations_all.receipt_required_flag%TYPE;
    l_po_info                VARCHAR2(200);
    l_po_number              po_headers_all.segment1%TYPE;
    l_po_line_number         po_lines_all.line_num%TYPE;
    l_po_shipment_number     po_line_locations_all.shipment_num%TYPE;
    l_po_distribution_number po_distributions_all.distribution_num%TYPE;
  
    CURSOR po_details_cur IS
      SELECT plla.receipt_required_flag,
             pha.segment1 po_number,
             pla.line_num,
             plla.shipment_num,
             pda.distribution_num
        FROM po_distributions_all  pda,
             po_line_locations_all plla,
             po_lines_all          pla,
             po_headers_all        pha
       WHERE pha.po_header_id = pla.po_header_id
         AND pha.po_header_id = plla.po_header_id
         AND pha.po_header_id = pda.po_header_id
         AND pla.po_line_id = plla.po_line_id
         AND pla.po_line_id = pda.po_line_id
         AND pda.line_location_id = plla.line_location_id
         AND pha.org_id = pla.org_id
         AND pha.org_id = plla.org_id
         AND pha.org_id = pda.org_id
         AND pha.org_id = p_old_org_id
         AND pha.po_header_id = p_old_po_header_id
         AND pla.po_line_id = p_old_po_line_id
         AND plla.line_location_id = p_old_line_location_id
         AND pda.po_distribution_id = p_old_po_distribution_id;
  
    CURSOR get_po_header_cur IS
      SELECT segment1 po_number
        FROM po_headers_all
       WHERE org_id = p_old_org_id
         AND po_header_id = p_old_po_header_id;
  
    CURSOR get_po_lines_cur IS
      SELECT pha.segment1 po_number,
             pla.line_num
        FROM po_headers_all pha,
             po_lines_all   pla
       WHERE pha.po_header_id = pla.po_header_id
         AND pha.org_id = pla.org_id
         AND pha.org_id = p_old_org_id
         AND pha.po_header_id = p_old_po_header_id
         AND pla.po_line_id = p_old_po_line_id;
  
    CURSOR get_po_shipment_cur IS
      SELECT plla.receipt_required_flag,
             pha.segment1 po_number,
             pla.line_num,
             plla.shipment_num
        FROM po_headers_all        pha,
             po_lines_all          pla,
             po_line_locations_all plla
       WHERE pha.po_header_id = pla.po_header_id
         AND pha.po_header_id = plla.po_header_id
         AND pla.po_line_id = plla.po_line_id
         AND pha.org_id = pla.org_id
         AND pha.org_id = plla.org_id
         AND pha.org_id = p_old_org_id
         AND pha.po_header_id = p_old_po_header_id
         AND pla.po_line_id = p_old_po_line_id
         AND plla.line_location_id = p_old_line_location_id;
  
  BEGIN
    l_receipt_required_flag  := 'N';
    l_po_info                := NULL;
    l_po_number              := NULL;
    l_po_line_number         := NULL;
    l_po_shipment_number     := NULL;
    l_po_distribution_number := NULL;
  
    IF p_old_po_distribution_id IS NOT NULL
    THEN
      FOR po_details_rec IN po_details_cur
      LOOP
        l_receipt_required_flag := nvl(po_details_rec.receipt_required_flag,
                                       'N');
        IF p_source_le = p_destination_le
        THEN
          l_po_number := REPLACE(to_char(po_details_rec.po_number),
                                 p_source_plant || '-',
                                 p_destination_plant || '-');
        ELSE
          l_po_number := to_char(po_details_rec.po_number);
        END IF;
      
        l_po_line_number         := po_details_rec.line_num;
        l_po_shipment_number     := po_details_rec.shipment_num;
        l_po_distribution_number := po_details_rec.distribution_num;
        l_po_info                := l_po_number || '|' ||
                                    to_char(l_po_line_number) || '|' ||
                                    to_char(l_po_shipment_number);
      
        EXIT;
      END LOOP;
    
    ELSIF p_old_po_distribution_id IS NULL AND
          p_old_line_location_id IS NOT NULL
    THEN
      FOR get_po_shipment_rec IN get_po_shipment_cur
      LOOP
      
        l_receipt_required_flag := nvl(get_po_shipment_rec.receipt_required_flag,
                                       'N');
        IF p_source_le = p_destination_le
        THEN
          l_po_number := REPLACE(to_char(get_po_shipment_rec.po_number),
                                 p_source_plant || '-',
                                 p_destination_plant || '-');
        ELSE
          l_po_number := to_char(get_po_shipment_rec.po_number);
        END IF;
      
        l_po_line_number         := get_po_shipment_rec.line_num;
        l_po_shipment_number     := get_po_shipment_rec.shipment_num;
        l_po_distribution_number := NULL;
        l_po_info                := l_po_number || '|' ||
                                    to_char(l_po_line_number) || '|' ||
                                    to_char(l_po_shipment_number);
      END LOOP;
    ELSIF p_old_line_location_id IS NULL AND p_old_po_line_id IS NOT NULL
    THEN
    
      FOR get_po_lines_rec IN get_po_lines_cur
      LOOP
      
        l_receipt_required_flag := 'N';
        IF p_source_le = p_destination_le
        THEN
          l_po_number := REPLACE(to_char(get_po_lines_rec.po_number),
                                 p_source_plant || '-',
                                 p_destination_plant || '-');
        ELSE
          l_po_number := to_char(get_po_lines_rec.po_number);
        END IF;
      
        l_po_line_number         := get_po_lines_rec.line_num;
        l_po_shipment_number     := NULL;
        l_po_distribution_number := NULL;
        l_po_info                := l_po_number || '|' ||
                                    to_char(l_po_line_number) || '|' ||
                                    to_char(l_po_shipment_number);
      END LOOP;
    ELSIF p_old_po_line_id IS NULL AND p_old_po_header_id IS NOT NULL
    THEN
    
      FOR get_po_header_rec IN get_po_header_cur
      LOOP
      
        l_receipt_required_flag := 'N';
        IF p_source_le = p_destination_le
        THEN
          l_po_number := REPLACE(to_char(get_po_header_rec.po_number),
                                 p_source_plant || '-',
                                 p_destination_plant || '-');
        ELSE
          l_po_number := to_char(get_po_header_rec.po_number);
        END IF;
      
        l_po_line_number     := NULL;
        l_po_shipment_number := NULL;
        l_po_info            := l_po_number || '|' ||
                                to_char(l_po_line_number) || '|' ||
                                to_char(l_po_shipment_number);
      END LOOP;
    END IF;
    p_receipt_required_flag  := l_receipt_required_flag;
    p_po_info                := l_po_info;
    p_po_number              := l_po_number;
    p_po_line_number         := l_po_line_number;
    p_po_shipment_number     := l_po_shipment_number;
    p_po_distribution_number := l_po_distribution_number;
  EXCEPTION
    WHEN OTHERS THEN
      p_receipt_required_flag  := NULL;
      p_po_info                := NULL;
      p_po_number              := NULL;
      p_po_line_number         := NULL;
      p_po_shipment_number     := NULL;
      p_po_distribution_number := NULL;
      debug('Error: Exception occured in get_po_info procedure ' ||
            substr(SQLERRM, 1, 240));
  END get_po_info;

  -- =============================================================================
  -- Function: get_legal_entity_id
  -- =============================================================================
  --   To get legal entity ID
  -- =============================================================================
  --  Input Parameters :
  --    p_legal_entity  : Legal Entity
  --  Output Parameters :
  --    No Output Parameters
  -- -----------------------------------------------------------------------------
  FUNCTION get_legal_entity_id(p_legal_entity IN gl_legal_entities_bsvs.flex_segment_value%TYPE)
    RETURN NUMBER IS
    l_legal_entity_id gl_legal_entities_bsvs.legal_entity_id%TYPE;
  
    CURSOR get_le_id_cur IS
      SELECT legal_entity_id
        FROM gl_legal_entities_bsvs
       WHERE flex_segment_value = p_legal_entity;
  BEGIN
    l_legal_entity_id := NULL;
  
    FOR rec_get_le_id IN get_le_id_cur
    LOOP
      l_legal_entity_id := rec_get_le_id.legal_entity_id;
    END LOOP;
  
    RETURN l_legal_entity_id;
  END get_legal_entity_id;

  -- ========================
  -- Procedure: get_code_combination_segments
  -- =============================================================================
  --   This procedure will get the code combination segments.
  -- =============================================================================
  PROCEDURE get_code_combination_segments(pin_code_combination_id  IN NUMBER,
                                          pov_concatenated_segment OUT VARCHAR,
                                          pon_chart_of_account_id  OUT NUMBER) IS
    CURSOR get_concat_segments_coa_cur IS
      SELECT concatenated_segments,
             chart_of_accounts_id
        FROM gl_code_combinations_kfv
       WHERE code_combination_id = pin_code_combination_id;
  BEGIN
  
    FOR concat_segments_coa_rec IN get_concat_segments_coa_cur
    LOOP
      pov_concatenated_segment := concat_segments_coa_rec.concatenated_segments;
      pon_chart_of_account_id  := concat_segments_coa_rec.chart_of_accounts_id;
    END LOOP;
  EXCEPTION
    WHEN OTHERS THEN
      debug('Exception in procedure get_code_combination_segments. Error Message:' ||
            SQLERRM);
      pov_concatenated_segment := NULL;
      pon_chart_of_account_id  := NULL;
  END get_code_combination_segments;

  -- ========================
  -- Procedure: get_code_combination_id
  -- =============================================================================
  --   This procedure will return code combination id
  -- =============================================================================
  PROCEDURE get_code_combination_id(piv_concatenated_segment IN VARCHAR,
                                    pin_chart_of_account_id  IN NUMBER,
                                    pon_code_combination_id  OUT NUMBER,
                                    pov_error_message        OUT VARCHAR2) IS
  BEGIN
    pon_code_combination_id := fnd_flex_ext.get_ccid(application_short_name => 'SQLGL',
                                                     key_flex_code          => 'GL#',
                                                     structure_number       => pin_chart_of_account_id,
                                                     validation_date        => to_char(SYSDATE,
                                                                                       fnd_flex_ext.date_format),
                                                     concatenated_segments  => piv_concatenated_segment);
    pov_error_message       := fnd_flex_ext.get_message;
  EXCEPTION
    WHEN OTHERS THEN
      debug('Exception in procedure get_code_combination_id. Error Message:' ||
            SQLERRM);
      pov_error_message       := 'Exception in procedure get_code_combination_id. Error Message:' ||
                                 SQLERRM;
      pon_code_combination_id := NULL;
  END get_code_combination_id;

  -- =============================================================================
  -- Function: is_invc_cancelled
  -- =============================================================================
  --   To check if the Invoice is Cancelled at Source
  -- =============================================================================
  --  Input Parameters :
  --    p_invoice_id  : Source Invoice ID
  --    p_org_id      : Source Org ID
  --  Output Parameters :
  --    No Output Parameters
  -- -----------------------------------------------------------------------------
  FUNCTION is_invc_cancelled(p_invoice_id IN ap_invoices_all.invoice_id%TYPE,
                             p_org_id     IN ap_invoices_all.org_id%TYPE)
    RETURN VARCHAR2 IS
    l_cancel_count NUMBER := 0;
  
    CURSOR check_cancel_cur IS
      SELECT COUNT(1) cnt
        FROM ap_invoices_all
       WHERE invoice_id = p_invoice_id
         AND cancelled_date IS NOT NULL
         AND org_id = p_org_id;
  BEGIN
    l_cancel_count := 0;
  
    FOR check_cancel_rec IN check_cancel_cur
    LOOP
      l_cancel_count := check_cancel_rec.cnt;
    END LOOP;
  
    IF l_cancel_count > 0
    THEN
      RETURN 'Y';
    ELSE
      RETURN 'N';
    END IF;
  EXCEPTION
    WHEN OTHERS THEN
      debug('Error: Exception occured in is_invc_cancelled procedure ' ||
            substr(SQLERRM, 1, 240));
      RETURN 'N';
  END is_invc_cancelled;

  -- =============================================================================
  -- Procedure: is_invc_cancellable
  -- =============================================================================
  --   To check if the Invoice can be Cancelled at Source
  -- =============================================================================
  --  Input Parameters :
  --    p_invoice_id  : Source Invoice ID
  --    p_org_id      : Source Org ID
  --  Output Parameters :
  --    p_retcode     : Program Return Code = 0/1/2
  --    p_errbuf      : Error message in case of any failure
  -- -----------------------------------------------------------------------------
  PROCEDURE is_invc_cancellable(p_invoice_id IN ap_invoices_all.invoice_id%TYPE,
                                p_org_id     IN ap_invoices_all.org_id%TYPE,
                                p_errbuf     OUT VARCHAR2,
                                p_retcode    OUT NUMBER) IS
    l_err_details      VARCHAR2(4000);
    l_boolean          BOOLEAN;
    l_error_code       VARCHAR2(4000) := NULL;
    l_debug_info       VARCHAR2(4000) := NULL;
    l_calling_sequence VARCHAR2(4000) := NULL;
  
    CURSOR get_err_details_cur(p_reject_code IN VARCHAR2) IS
      SELECT m.message_text err_details
        FROM fnd_new_messages m
       WHERE m.message_name = p_reject_code
         AND m.language_code = userenv('LANG')
      UNION ALL
      SELECT meaning err_details
        FROM fnd_lookup_values
       WHERE lookup_code = p_reject_code
         AND LANGUAGE = userenv('LANG')
         AND lookup_type LIKE 'REJECT CODE'
         AND enabled_flag = 'Y'
         AND SYSDATE BETWEEN nvl(start_date_active, SYSDATE - 1) AND
             nvl(end_date_active, SYSDATE + 1);
  BEGIN
    l_err_details := NULL;
    l_boolean     := TRUE;
    l_error_code  := NULL;
    mo_global.set_policy_context('S', p_org_id);
    l_boolean := ap_cancel_pkg.is_invoice_cancellable(p_invoice_id       => p_invoice_id,
                                                      p_error_code       => l_error_code,
                                                      p_debug_info       => l_debug_info,
                                                      p_calling_sequence => l_calling_sequence);
    COMMIT;
    debug(l_debug_info);
  
    IF l_boolean
    THEN
      debug('Invoice can be cancelled');
      p_errbuf  := NULL;
      p_retcode := g_normal;
    ELSE
      debug('Invoice cannot be cancelled');
    
      IF l_error_code IS NULL
      THEN
        p_errbuf := 'Invoice cannot be cancelled. Error Code = NULL indicates data corruption issue. Please review and handle the invoice manually.';
      ELSE
      
        FOR get_err_details_rec IN get_err_details_cur(l_error_code)
        LOOP
          l_err_details := get_err_details_rec.err_details;
        END LOOP;
      
        p_errbuf := l_err_details;
      END IF;
    
      p_retcode := g_warning;
    END IF;
  
    COMMIT;
  EXCEPTION
    WHEN OTHERS THEN
      p_errbuf  := 'Error: Exception occured in is_invc_cancellable procedure ' ||
                   substr(SQLERRM, 1, 240);
      p_retcode := g_error;
      debug(p_errbuf);
  END is_invc_cancellable;

  -- =============================================================================
  -- Procedure: load_invc
  -- =============================================================================
  --   To Load Invoice rom Base to Staging table
  -- =============================================================================
  --  Input Parameters :
  --    No Input Parameters
  --  Output Parameters :
  --    p_retcode   : Program Return Code = 0/1/2
  --    p_errbuf    : Error message in case of any failure
  -- -----------------------------------------------------------------------------
  PROCEDURE load_invc(p_errbuf  OUT VARCHAR2,
                      p_retcode OUT NUMBER) IS
    /*Cursor to fetch all the scenario details*/
    CURSOR get_lookup_scenarios_cur IS
      SELECT hou.organization_id source_org_id,
             hou.name source_org_name,
             substr(flv.lookup_code, instr(flv.lookup_code, '.') + 1) source_plant_name,
             substr(flv.lookup_code, 1, instr(flv.lookup_code, '.') - 1) source_le,
             hou1.organization_id destination_org_id,
             hou1.name destination_org_name,
             substr(flv.meaning, instr(flv.meaning, '.') + 1) destination_plant_name,
             substr(flv.meaning, 1, instr(flv.meaning, '.') - 1) destination_le,
             flv.description scenario,
             flv.tag operation_name
        FROM fnd_lookup_values  flv,
             hr_operating_units hou,
             hr_operating_units hou1
       WHERE flv.lookup_type = 'XXETN_RESTRUCTURE_MAPPING'
         AND flv.language = userenv('LANG')
         AND flv.enabled_flag = 'Y'
         AND SYSDATE BETWEEN nvl(start_date_active, SYSDATE - 1) AND
             nvl(end_date_active, SYSDATE + 1)
         AND hou.short_code =
             substr(lookup_code, 1, instr(lookup_code, '.') - 1) || '_OU'
         AND hou1.short_code =
             substr(meaning, 1, instr(meaning, '.') - 1) || '_OU';
  
    l_conv_account fnd_profile_option_values.profile_option_value%TYPE;
  
    CURSOR get_conv_account_cur IS
      SELECT fpov.profile_option_value
        FROM fnd_profile_options       fpo,
             fnd_profile_option_values fpov
       WHERE fpo.profile_option_id = fpov.profile_option_id
         AND fpo.profile_option_name = 'XXETN_CONV_ACC_COMBINATION';
  
    lv_new_dist_concat_flip VARCHAR2(750);
    CURSOR flip_segment6_cur(p_code_combination_id IN NUMBER,
                             p_source_le           IN VARCHAR2) IS
      SELECT 1
        FROM gl_code_combinations
       WHERE code_combination_id = p_code_combination_id
         AND segment6 = p_source_le;
  
    l_destination_les VARCHAR2(1000);
    CURSOR get_dup_inv_head_cur IS
      SELECT batch_id,
             old_invoice_id,
             COUNT(1),
             MIN(record_id) min_record_id,
             listagg(source_plant, ',') within GROUP(ORDER BY source_plant) source_plants,
             listagg(destination_plant, ',') within GROUP(ORDER BY destination_plant) destination_plants,
             listagg(destination_le, ',') within GROUP(ORDER BY destination_le) destination_les
        FROM xxap_invc_intfc_stg
       WHERE batch_id = g_loader_request_id
       GROUP BY old_invoice_id,
                batch_id
      HAVING COUNT(1) > 1;
  
    CURSOR get_dup_inv_line_cur(p_old_invoice_id IN xxap_invc_lines_intfc_stg.old_invoice_id%TYPE,
                                p_min_record_id  IN xxap_invc_lines_intfc_stg.record_id%TYPE) IS
      SELECT MAX(line_number) max_line_number
        FROM xxap_invc_lines_intfc_stg
       WHERE batch_id = g_loader_request_id
         AND old_invoice_id = p_old_invoice_id
         AND record_id = p_min_record_id;
  
    CURSOR get_invoice_id_cur(p_record_id IN xxap_invc_intfc_stg.record_id%TYPE) IS
      SELECT invoice_id
        FROM xxap_invc_intfc_stg
       WHERE batch_id = g_loader_request_id
         AND record_id = p_record_id;
  
    CURSOR cur_get_ap_inv_head(p_source_org_id          IN hr_operating_units.organization_id%TYPE,
                               p_source_plant_name      IN fnd_lookup_values.lookup_code%TYPE,
                               p_source_le              IN fnd_lookup_values.lookup_code%TYPE,
                               p_destination_org_id     IN hr_operating_units.organization_id%TYPE,
                               p_destination_plant_name IN fnd_lookup_values.meaning%TYPE,
                               p_destination_le         IN fnd_lookup_values.meaning%TYPE,
                               p_destination_org_name   IN hr_operating_units.name%TYPE,
                               p_operation_name         IN fnd_lookup_values.tag%TYPE) IS
      SELECT NULL record_id,
             g_request_id request_id,
             g_flag_ntprocessed status_flag,
             'NEW' record_status,
             NULL error_type,
             NULL error_message,
             p_operation_name operation,
             NULL invoice_id,
             invoice_id old_invoice_id,
             NULL new_invoice_id,
             xxap_invc_restruct_conv_pkg.is_invc_cancelled(invoice_id,
                                                           org_id) is_invc_cancelled,
             NULL is_invc_on_hold,
             xxap_invc_restruct_conv_pkg.get_hold_count(invoice_id,
                                                        p_source_org_id) num_of_holds,
             NULL is_non_po,
             NULL is_po_matched,
             CASE
               WHEN aia.payment_status_flag = 'P' THEN
                'Y'
               ELSE
                'N'
             END is_partially_paid,
             nvl(amount_paid, 0) amt_partially_paid,
             CASE
               WHEN invoice_type_lookup_code = 'PREPAYMENT' THEN
                nvl(ap_invoices_utility_pkg.get_prepay_amount_applied(aia.invoice_id),
                    0)
               ELSE
                0
             END prepay_amt_applied,
             (nvl(abs(aia.invoice_amount), 0) -
             nvl(abs(aia.amount_paid), 0)) * sign(invoice_amount) amt_unpaid,
             CASE
               WHEN invoice_type_lookup_code = 'PREPAYMENT' THEN
                nvl(ap_invoices_utility_pkg.get_prepay_amount_remaining(aia.invoice_id),
                    0)
               ELSE
                0
             END prepay_amt_unapplied,
             invoice_num,
             invoice_num || g_invoice_separator || p_source_le new_invoice_num,
             CASE
               WHEN invoice_type_lookup_code IN ('MIXED') AND
                    invoice_amount > 0 THEN
                'STANDARD'
               WHEN invoice_type_lookup_code IN ('MIXED') AND
                    invoice_amount < 0 THEN
                'DEBIT'
               WHEN invoice_type_lookup_code = 'AWT' THEN
                'STANDARD'
               ELSE
                invoice_type_lookup_code
             END invoice_type_lookup_code,
             invoice_date,
             NULL po_number,
             po_header_id,
             quick_po_header_id,
             vendor_id,
             NULL vendor_num,
             NULL vendor_name,
             vendor_site_id old_vendor_site_id,
             NULL new_vendor_site_id,
             NULL vendor_site_code,
             invoice_amount,
             invoice_currency_code,
             exchange_rate,
             exchange_rate_type,
             exchange_date,
             terms_id,
             NULL terms_name,
             description,
             awt_group_id,
             NULL awt_group_name,
             SYSDATE last_update_date,
             g_user_id last_updated_by,
             g_login_id last_update_login,
             SYSDATE creation_date,
             g_user_id created_by,
             CASE
               WHEN attribute_category IS NULL THEN
                'Global Data Elementss'
               ELSE
                attribute_category
             END attribute_category,
             NULL attribute1,
             attribute2,
             attribute3,
             CASE
               WHEN attribute4 IS NULL THEN
                p_source_le
               ELSE
                attribute4
             END attribute4,
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
             NULL status,
             'CONVERSION' SOURCE,
             NULL group_id,
             payment_cross_rate_type,
             payment_cross_rate_date,
             payment_cross_rate,
             payment_currency_code,
             NULL workflow_flag,
             doc_category_code,
             voucher_num,
             payment_method_lookup_code,
             CASE
               WHEN invoice_type_lookup_code = 'PREPAYMENT' THEN
                'CNVADVANCE'
               ELSE
                pay_group_lookup_code
             END pay_group_lookup_code,
             goods_received_date,
             invoice_received_date,
             g_accounting_date gl_date,
             NULL accts_pay_code_combination_id,
             ussgl_transaction_code,
             exclusive_payment_flag,
             org_id,
             p_destination_org_id new_org_id,
             amount_applicable_to_discount,
             NULL prepay_num,
             NULL prepay_dist_num,
             NULL prepay_apply_amount,
             NULL prepay_gl_date,
             NULL invoice_includes_prepay_flag,
             NULL no_xrate_base_amount,
             NULL vendor_email_address,
             terms_date,
             requester_id,
             NULL ship_to_location,
             NULL external_doc_ref,
             NULL prepay_line_num,
             NULL requester_first_name,
             NULL requester_last_name,
             application_id,
             product_table,
             reference_key1,
             reference_key2,
             reference_key3,
             reference_key4,
             reference_key5,
             NULL apply_advances_flag,
             'N' calc_tax_during_import_flag,
             control_amount,
             total_tax_amount,
             'N' add_tax_to_inv_amt_flag,
             tax_related_invoice_id,
             taxation_country,
             document_sub_type,
             supplier_tax_invoice_number,
             supplier_tax_invoice_date,
             supplier_tax_exchange_rate,
             tax_invoice_recording_date,
             tax_invoice_internal_seq,
             xxap_invc_restruct_conv_pkg.get_legal_entity_id(p_source_le) legal_entity_id,
             xxap_invc_restruct_conv_pkg.get_legal_entity_id(p_destination_le) new_legal_entity_id,
             NULL legal_entity_name,
             reference_1,
             reference_2,
             p_destination_org_name operating_unit,
             bank_charge_bearer,
             remittance_message1,
             remittance_message2,
             remittance_message3,
             unique_remittance_identifier,
             uri_check_digit,
             settlement_priority,
             payment_reason_code,
             payment_reason_comments,
             payment_method_code,
             delivery_channel_code,
             paid_on_behalf_employee_id,
             net_of_retainage_flag,
             NULL requester_employee_num,
             cust_registration_code,
             cust_registration_number,
             party_id old_party_id,
             NULL new_party_id,
             party_site_id old_party_site_id,
             NULL new_party_site_id,
             pay_proc_trxn_type_code,
             payment_function,
             NULL payment_priority,
             NULL old_due_date,
             NULL new_due_date,
             port_of_entry_code,
             external_bank_account_id,
             NULL accts_pay_code_concatenated,
             pay_awt_group_id,
             NULL pay_awt_group_name,
             original_invoice_amount,
             dispute_reason,
             remit_to_supplier_name,
             remit_to_supplier_id,
             remit_to_supplier_site,
             remit_to_supplier_site_id,
             relationship_id,
             NULL remit_to_supplier_num,
             g_batch_id batch_id,
             p_source_le source_le,
             p_source_plant_name source_plant,
             p_destination_le destination_le,
             p_destination_plant_name destination_plant,
             ap_invoices_pkg.get_posting_status(invoice_id) posting_status
        FROM ap_invoices_all aia
       WHERE EXISTS
       (SELECT 1
                FROM ap_invoice_lines_all         aila,
                     ap_invoice_distributions_all aida
               WHERE aila.invoice_id = aia.invoice_id
                 AND aida.invoice_id = aia.invoice_id
                 AND aida.org_id = aia.org_id
                 AND aida.org_id = aila.org_id
                 AND aida.invoice_line_number = aila.line_number
                 AND aila.line_type_lookup_code <> 'TAX'
                 AND EXISTS
               (SELECT 1
                        FROM gl_code_combinations gcc
                       WHERE aida.dist_code_combination_id =
                             gcc.code_combination_id
                         AND gcc.segment1 = p_source_le /*Source LE*/
                         AND gcc.segment2 = p_source_plant_name)) /*Source Plant Number*/
         AND aia.org_id = p_source_org_id /*Source Org ID*/
         AND (aia.payment_status_flag IN ('N', 'P') OR /*Unpaid and Partially Paid*/
             (aia.payment_status_flag = 'Y' AND
             aia.invoice_type_lookup_code = 'PREPAYMENT' AND
             nvl(ap_invoices_utility_pkg.get_prepay_amount_remaining(aia.invoice_id),
                   0) > 0)) /*Fully Paid but Unapplied Prepayments*/
         AND aia.cancelled_date IS NULL /*Not Cancelled*/
         AND aia.invoice_amount <> 0
         AND aia.wfapproval_status IN
             ('INITIATED', 'REQUIRED', 'NOT REQUIRED', 'WFAPPROVED');
  
    CURSOR cur_get_invc_lines(p_source_org_id          IN hr_operating_units.organization_id%TYPE,
                              p_source_plant_name      IN fnd_lookup_values.lookup_code%TYPE,
                              p_source_le              IN fnd_lookup_values.lookup_code%TYPE,
                              p_destination_org_id     IN hr_operating_units.organization_id%TYPE,
                              p_destination_plant_name IN fnd_lookup_values.meaning%TYPE,
                              p_destination_le         IN fnd_lookup_values.meaning%TYPE,
                              p_invoice_id             IN ap_invoice_lines_all.invoice_id%TYPE,
                              p_org_id                 IN ap_invoice_lines_all.org_id%TYPE) IS
      SELECT *
        FROM (SELECT NULL record_id,
                     NULL record_line_id,
                     g_request_id request_id,
                     g_flag_ntprocessed status_flag,
                     NULL error_type,
                     NULL error_message,
                     NULL invoice_id,
                     aila.invoice_id old_invoice_id,
                     NULL new_invoice_id,
                     NULL invoice_line_id,
                     NULL old_invoice_line_id,
                     NULL new_invoice_line_id,
                     aila.line_number line_number,
                     CASE
                       WHEN (aida.line_type_lookup_code IN ('PREPAY', 'AWT')) OR
                            (aila.line_type_lookup_code = 'ITEM' AND
                            upper(aida.line_type_lookup_code) NOT IN
                            ('ACCRUAL', 'EXPENSE', 'ITEM')) THEN
                        'MISCELLANEOUS'
                       WHEN (aila.line_type_lookup_code <> 'ITEM') OR
                            (aila.line_type_lookup_code = 'ITEM' AND
                            aida.line_type_lookup_code IN
                            ('ACCRUAL', 'EXPENSE', 'ITEM')) THEN
                        aila.line_type_lookup_code
                       ELSE
                        aila.line_type_lookup_code
                     END line_type_lookup_code,
                     aida.line_type_lookup_code dist_line_type_lookup_code,
                     CASE
                       WHEN aida.charge_applicable_to_dist_id IS NULL THEN
                        NULL
                       ELSE
                        ap_invoice_distributions_pkg.get_inv_line_num(aida.charge_applicable_to_dist_id)
                     END line_group_number,
                     SUM(nvl(aida.amount, 0)) amount,
                     g_accounting_date accounting_date,
                     aila.description,
                     NULL amount_includes_tax_flag,
                     CASE
                       WHEN aida.charge_applicable_to_dist_id IS NULL OR
                            ap_invoice_distributions_pkg.get_inv_line_num(aida.charge_applicable_to_dist_id) IS NULL THEN
                        'N'
                       ELSE
                        'Y'
                     END prorate_across_flag,
                     NULL tax_code,
                     aida.final_match_flag,
                     aila.po_header_id po_header_id,
                     NULL po_number,
                     aila.po_line_id po_line_id,
                     NULL po_line_number,
                     aila.po_line_location_id,
                     NULL po_shipment_num,
                     aila.po_distribution_id,
                     NULL po_distribution_num,
                     NULL po_unit_of_measure,
                     aila.inventory_item_id,
                     aila.item_description,
                     SUM(nvl(aida.quantity_invoiced, 0)) quantity_invoiced,
                     NULL ship_to_location_code,
                     SUM(nvl(aida.unit_price, 0)) unit_price,
                     NULL distribution_set_id,
                     NULL distribution_set_name,
                     NULL dist_code_concatenated,
                     0 dist_code_combination_id,
                     aida.dist_code_combination_id old_dist_code_combination_id,
                     aida.awt_group_id,
                     NULL awt_group_name,
                     g_user_id last_updated_by,
                     SYSDATE last_update_date,
                     g_login_id last_update_login,
                     g_user_id created_by,
                     SYSDATE creation_date,
                     CASE
                       WHEN aila.po_header_id IS NOT NULL THEN
                        'Conversion'
                       ELSE
                        aila.attribute_category
                     END attribute_category,
                     aila.attribute1 attribute1,
                     aila.attribute2,
                     aila.attribute3,
                     aila.attribute4,
                     aila.attribute5,
                     aila.attribute6,
                     aila.attribute7,
                     aila.attribute8,
                     aila.attribute9,
                     aila.attribute10,
                     aila.attribute11,
                     aila.attribute12,
                     aila.attribute13,
                     aila.attribute14,
                     aila.attribute15,
                     aila.global_attribute_category,
                     aila.global_attribute1,
                     aila.global_attribute2,
                     aila.global_attribute3,
                     aila.global_attribute4,
                     aila.global_attribute5,
                     aila.global_attribute6,
                     aila.global_attribute7,
                     aila.global_attribute8,
                     aila.global_attribute9,
                     aila.global_attribute10,
                     aila.global_attribute11,
                     aila.global_attribute12,
                     aila.global_attribute13,
                     aila.global_attribute14,
                     aila.global_attribute15,
                     aila.global_attribute16,
                     aila.global_attribute17,
                     aila.global_attribute18,
                     aila.global_attribute19,
                     aila.global_attribute20,
                     aila.po_release_id,
                     NULL release_num,
                     aila.account_segment,
                     aila.balancing_segment,
                     aila.cost_center_segment,
                     aida.project_id old_project_id,
                     NULL new_project_id,
                     aida.task_id old_task_id,
                     NULL new_task_id,
                     CASE
                       WHEN aida.project_id IS NULL THEN
                        NULL
                       ELSE
                        aida.expenditure_type
                     END expenditure_type,
                     CASE
                       WHEN aida.project_id IS NULL THEN
                        NULL
                       ELSE
                        aida.expenditure_item_date
                     END expenditure_item_date,
                     CASE
                       WHEN aida.project_id IS NULL THEN
                        NULL
                       ELSE
                        aida.expenditure_organization_id
                     END expenditure_organization_id,
                     CASE
                       WHEN aida.project_id IS NULL THEN
                        NULL
                       ELSE
                        aida.project_accounting_context
                     END project_accounting_context,
                     CASE
                       WHEN aida.project_id IS NULL THEN
                        NULL
                       ELSE
                        aida.pa_addition_flag
                     END pa_addition_flag,
                     CASE
                       WHEN aida.project_id IS NULL THEN
                        NULL
                       ELSE
                        aila.pa_quantity
                     END pa_quantity,
                     aila.ussgl_transaction_code,
                     aila.stat_amount,
                     aila.type_1099,
                     aila.income_tax_region,
                     aila.assets_tracking_flag,
                     NULL price_correction_flag,
                     aida.org_id,
                     p_destination_org_id new_org_id,
                     NULL receipt_number,
                     NULL receipt_line_number,
                     NULL match_option,
                     NULL packing_slip,
                     aida.rcv_transaction_id,
                     aida.pa_cc_ar_invoice_id,
                     aida.pa_cc_ar_invoice_line_num,
                     aida.reference_1,
                     aida.reference_2,
                     aida.pa_cc_processed_code,
                     aida.tax_recovery_rate,
                     aida.tax_recovery_override_flag,
                     aida.tax_recoverable_flag,
                     aida.tax_code_override_flag,
                     aida.tax_code_id,
                     aida.credit_card_trx_id,
                     aida.award_id,
                     NULL vendor_item_num,
                     NULL taxable_flag,
                     NULL price_correct_inv_num,
                     NULL external_doc_line_ref,
                     aila.serial_number,
                     aila.manufacturer,
                     aila.model_number,
                     aila.warranty_number,
                     aila.deferred_acctg_flag,
                     aila.def_acctg_start_date,
                     aila.def_acctg_end_date,
                     aila.def_acctg_number_of_periods,
                     aila.def_acctg_period_type,
                     NULL unit_of_meas_lookup_code,
                     NULL price_correct_inv_line_num,
                     aida.asset_book_type_code,
                     aida.asset_category_id,
                     aila.requester_id,
                     NULL requester_first_name,
                     NULL requester_last_name,
                     NULL requester_employee_num,
                     aila.application_id,
                     aila.product_table,
                     aila.reference_key1,
                     aila.reference_key2,
                     aila.reference_key3,
                     aila.reference_key4,
                     aila.reference_key5,
                     NULL purchasing_category,
                     aila.purchasing_category_id,
                     aila.cost_factor_id,
                     NULL cost_factor_name,
                     aila.control_amount,
                     aila.assessable_value,
                     aila.default_dist_ccid,
                     aila.primary_intended_use,
                     aila.ship_to_location_id,
                     aila.product_type,
                     aila.product_category,
                     aila.product_fisc_classification,
                     aila.user_defined_fisc_class,
                     aila.trx_business_category,
                     aila.tax_regime_code,
                     aila.tax,
                     aila.tax_jurisdiction_code,
                     aila.tax_status_code,
                     aila.tax_rate_id,
                     aila.tax_rate_code,
                     aila.tax_rate,
                     NULL incl_in_taxable_line_flag,
                     aila.source_application_id,
                     aila.source_entity_code,
                     aila.source_event_class_code,
                     aila.source_trx_id,
                     aila.source_line_id,
                     aila.source_trx_level_type,
                     aila.tax_classification_code,
                     aida.cc_reversal_flag,
                     aida.company_prepaid_invoice_id,
                     aida.expense_group,
                     aida.justification,
                     aida.merchant_document_number,
                     aida.merchant_reference,
                     aida.merchant_tax_reg_number,
                     aida.merchant_taxpayer_id,
                     aida.receipt_currency_code,
                     aida.receipt_conversion_rate,
                     aida.receipt_currency_amount,
                     aida.country_of_supply,
                     aida.pay_awt_group_id,
                     NULL pay_awt_group_name,
                     NULL expense_start_date,
                     NULL expense_end_date,
                     NULL merchant_name#1
                FROM ap_invoice_lines_all         aila,
                     ap_invoice_distributions_all aida
               WHERE aila.invoice_id = p_invoice_id
                 AND aila.org_id = p_org_id
                 AND aila.invoice_id = aida.invoice_id
                 AND aida.org_id = aila.org_id
                 AND aida.invoice_line_number = aila.line_number
                    /*AND nvl(aida.amount, 0) <> 0
                    AND nvl(aila.amount, 0) <> 0*/
                 AND aila.line_type_lookup_code = 'TAX'
                 AND NOT EXISTS
               (SELECT 1
                        FROM zx_taxes_b ztb
                       WHERE ztb.tax_regime_code = aila.tax_regime_code
                         AND ztb.tax = aila.tax
                         AND ztb.offset_tax_flag = 'Y'
                         AND trunc(SYSDATE) BETWEEN
                             trunc(nvl(ztb.effective_from, SYSDATE)) AND
                             trunc(nvl(ztb.effective_to, SYSDATE)))
               GROUP BY aila.invoice_id,
                        aila.line_number,
                        aila.line_type_lookup_code,
                        aida.line_type_lookup_code,
                        aida.charge_applicable_to_dist_id,
                        aila.description,
                        aida.final_match_flag,
                        aila.po_header_id,
                        aila.po_line_id,
                        aila.po_line_location_id,
                        aila.po_distribution_id,
                        aila.inventory_item_id,
                        aila.item_description,
                        aida.dist_code_combination_id,
                        aida.awt_group_id,
                        aila.po_distribution_id,
                        aila.attribute_category,
                        aila.attribute1,
                        aila.attribute2,
                        aila.attribute3,
                        aila.attribute4,
                        aila.attribute5,
                        aila.attribute6,
                        aila.attribute7,
                        aila.attribute8,
                        aila.attribute9,
                        aila.attribute10,
                        aila.attribute11,
                        aila.attribute12,
                        aila.attribute13,
                        aila.attribute14,
                        aila.attribute15,
                        aila.global_attribute_category,
                        aila.global_attribute1,
                        aila.global_attribute2,
                        aila.global_attribute3,
                        aila.global_attribute4,
                        aila.global_attribute5,
                        aila.global_attribute6,
                        aila.global_attribute7,
                        aila.global_attribute8,
                        aila.global_attribute9,
                        aila.global_attribute10,
                        aila.global_attribute11,
                        aila.global_attribute12,
                        aila.global_attribute13,
                        aila.global_attribute14,
                        aila.global_attribute15,
                        aila.global_attribute16,
                        aila.global_attribute17,
                        aila.global_attribute18,
                        aila.global_attribute19,
                        aila.global_attribute20,
                        aila.po_release_id,
                        aila.account_segment,
                        aila.balancing_segment,
                        aila.cost_center_segment,
                        aida.project_id,
                        aida.task_id,
                        aida.expenditure_type,
                        aida.expenditure_item_date,
                        aida.expenditure_organization_id,
                        aida.project_accounting_context,
                        aida.pa_addition_flag,
                        aila.pa_quantity,
                        aila.ussgl_transaction_code,
                        aila.stat_amount,
                        aila.type_1099,
                        aila.income_tax_region,
                        aila.assets_tracking_flag,
                        aida.org_id,
                        aida.rcv_transaction_id,
                        aida.pa_cc_ar_invoice_id,
                        aida.pa_cc_ar_invoice_line_num,
                        aida.reference_1,
                        aida.reference_2,
                        aida.pa_cc_processed_code,
                        aida.tax_recovery_rate,
                        aida.tax_recovery_override_flag,
                        aida.tax_recoverable_flag,
                        aida.tax_code_override_flag,
                        aida.tax_code_id,
                        aida.credit_card_trx_id,
                        aida.award_id,
                        aila.serial_number,
                        aila.manufacturer,
                        aila.model_number,
                        aila.warranty_number,
                        aila.deferred_acctg_flag,
                        aila.def_acctg_start_date,
                        aila.def_acctg_end_date,
                        aila.def_acctg_number_of_periods,
                        aila.def_acctg_period_type,
                        aida.asset_book_type_code,
                        aida.asset_category_id,
                        aila.requester_id,
                        aila.application_id,
                        aila.product_table,
                        aila.reference_key1,
                        aila.reference_key2,
                        aila.reference_key3,
                        aila.reference_key4,
                        aila.reference_key5,
                        aila.purchasing_category_id,
                        aila.cost_factor_id,
                        aila.control_amount,
                        aila.assessable_value,
                        aila.default_dist_ccid,
                        aila.primary_intended_use,
                        aila.ship_to_location_id,
                        aila.product_type,
                        aila.product_category,
                        aila.product_fisc_classification,
                        aila.user_defined_fisc_class,
                        aila.trx_business_category,
                        aila.tax_regime_code,
                        aila.tax,
                        aila.tax_jurisdiction_code,
                        aila.tax_status_code,
                        aila.tax_rate_id,
                        aila.tax_rate_code,
                        aila.tax_rate,
                        aila.source_application_id,
                        aila.source_entity_code,
                        aila.source_event_class_code,
                        aila.source_trx_id,
                        aila.source_line_id,
                        aila.source_trx_level_type,
                        aila.tax_classification_code,
                        aida.cc_reversal_flag,
                        aida.company_prepaid_invoice_id,
                        aida.expense_group,
                        aida.justification,
                        aida.merchant_document_number,
                        aida.merchant_reference,
                        aida.merchant_tax_reg_number,
                        aida.merchant_taxpayer_id,
                        aida.receipt_currency_code,
                        aida.receipt_conversion_rate,
                        aida.receipt_currency_amount,
                        aida.country_of_supply,
                        aida.pay_awt_group_id
              UNION ALL
              SELECT NULL record_id,
                     NULL record_line_id,
                     g_request_id request_id,
                     g_flag_ntprocessed status_flag,
                     NULL error_type,
                     NULL error_message,
                     NULL invoice_id,
                     aila.invoice_id old_invoice_id,
                     NULL new_invoice_id,
                     NULL invoice_line_id,
                     NULL old_invoice_line_id,
                     NULL new_invoice_line_id,
                     aila.line_number line_number,
                     CASE
                       WHEN (aida.line_type_lookup_code IN ('PREPAY', 'AWT')) OR
                            (aila.line_type_lookup_code = 'ITEM' AND
                            upper(aida.line_type_lookup_code) NOT IN
                            ('ACCRUAL', 'EXPENSE', 'ITEM')) THEN
                        'MISCELLANEOUS'
                       WHEN (aila.line_type_lookup_code <> 'ITEM') OR
                            (aila.line_type_lookup_code = 'ITEM' AND
                            aida.line_type_lookup_code IN
                            ('ACCRUAL', 'EXPENSE', 'ITEM')) THEN
                        aila.line_type_lookup_code
                       ELSE
                        aila.line_type_lookup_code
                     END line_type_lookup_code,
                     aida.line_type_lookup_code dist_line_type_lookup_code,
                     aida.invoice_line_number line_group_number,
                     aida.amount amount,
                     g_accounting_date accounting_date,
                     aida.description,
                     NULL amount_includes_tax_flag,
                     NULL prorate_across_flag,
                     NULL tax_code,
                     aida.final_match_flag,
                     aila.po_header_id po_header_id,
                     NULL po_number,
                     aila.po_line_id po_line_id,
                     NULL po_line_number,
                     aila.po_line_location_id,
                     NULL po_shipment_num,
                     aila.po_distribution_id,
                     NULL po_distribution_num,
                     NULL po_unit_of_measure,
                     aila.inventory_item_id,
                     aila.item_description,
                     aida.quantity_invoiced,
                     NULL ship_to_location_code,
                     aida.unit_price,
                     NULL distribution_set_id,
                     NULL distribution_set_name,
                     NULL dist_code_concatenated,
                     0 dist_code_combination_id,
                     aida.dist_code_combination_id old_dist_code_combination_id,
                     aida.awt_group_id,
                     NULL awt_group_name,
                     g_user_id last_updated_by,
                     SYSDATE last_update_date,
                     g_login_id last_update_login,
                     g_user_id created_by,
                     SYSDATE creation_date,
                     CASE
                       WHEN aila.po_distribution_id IS NOT NULL THEN
                        'Conversion'
                       ELSE
                        aila.attribute_category
                     END attribute_category,
                     aila.attribute1 attribute1,
                     aila.attribute2,
                     aila.attribute3,
                     aila.attribute4,
                     aila.attribute5,
                     aila.attribute6,
                     aila.attribute7,
                     aila.attribute8,
                     aila.attribute9,
                     aila.attribute10,
                     aila.attribute11,
                     aila.attribute12,
                     aila.attribute13,
                     aila.attribute14,
                     aila.attribute15,
                     aila.global_attribute_category,
                     aila.global_attribute1,
                     aila.global_attribute2,
                     aila.global_attribute3,
                     aila.global_attribute4,
                     aila.global_attribute5,
                     aila.global_attribute6,
                     aila.global_attribute7,
                     aila.global_attribute8,
                     aila.global_attribute9,
                     aila.global_attribute10,
                     aila.global_attribute11,
                     aila.global_attribute12,
                     aila.global_attribute13,
                     aila.global_attribute14,
                     aila.global_attribute15,
                     aila.global_attribute16,
                     aila.global_attribute17,
                     aila.global_attribute18,
                     aila.global_attribute19,
                     aila.global_attribute20,
                     aila.po_release_id,
                     NULL release_num,
                     aila.account_segment,
                     aila.balancing_segment,
                     aila.cost_center_segment,
                     aida.project_id old_project_id,
                     NULL new_project_id,
                     aida.task_id old_task_id,
                     NULL new_task_id,
                     CASE
                       WHEN aida.project_id IS NULL THEN
                        NULL
                       ELSE
                        aida.expenditure_type
                     END expenditure_type,
                     CASE
                       WHEN aida.project_id IS NULL THEN
                        NULL
                       ELSE
                        aida.expenditure_item_date
                     END expenditure_item_date,
                     CASE
                       WHEN aida.project_id IS NULL THEN
                        NULL
                       ELSE
                        aida.expenditure_organization_id
                     END expenditure_organization_id,
                     CASE
                       WHEN aida.project_id IS NULL THEN
                        NULL
                       ELSE
                        aida.project_accounting_context
                     END project_accounting_context,
                     CASE
                       WHEN aida.project_id IS NULL THEN
                        NULL
                       ELSE
                        aida.pa_addition_flag
                     END pa_addition_flag,
                     CASE
                       WHEN aida.project_id IS NULL THEN
                        NULL
                       ELSE
                        aila.pa_quantity
                     END pa_quantity,
                     aila.ussgl_transaction_code,
                     aila.stat_amount,
                     aila.type_1099,
                     aila.income_tax_region,
                     aila.assets_tracking_flag,
                     NULL price_correction_flag,
                     aida.org_id,
                     p_destination_org_id new_org_id,
                     NULL receipt_number,
                     NULL receipt_line_number,
                     NULL match_option,
                     NULL packing_slip,
                     aida.rcv_transaction_id,
                     aida.pa_cc_ar_invoice_id,
                     aida.pa_cc_ar_invoice_line_num,
                     aida.reference_1,
                     aida.reference_2,
                     aida.pa_cc_processed_code,
                     aida.tax_recovery_rate,
                     aida.tax_recovery_override_flag,
                     aida.tax_recoverable_flag,
                     aida.tax_code_override_flag,
                     aida.tax_code_id,
                     aida.credit_card_trx_id,
                     aida.award_id,
                     NULL vendor_item_num,
                     NULL taxable_flag,
                     NULL price_correct_inv_num,
                     NULL external_doc_line_ref,
                     aila.serial_number,
                     aila.manufacturer,
                     aila.model_number,
                     aila.warranty_number,
                     aila.deferred_acctg_flag,
                     aila.def_acctg_start_date,
                     aila.def_acctg_end_date,
                     aila.def_acctg_number_of_periods,
                     aila.def_acctg_period_type,
                     NULL unit_of_meas_lookup_code,
                     NULL price_correct_inv_line_num,
                     aida.asset_book_type_code,
                     aida.asset_category_id,
                     aila.requester_id,
                     NULL requester_first_name,
                     NULL requester_last_name,
                     NULL requester_employee_num,
                     aila.application_id,
                     aila.product_table,
                     aila.reference_key1,
                     aila.reference_key2,
                     aila.reference_key3,
                     aila.reference_key4,
                     aila.reference_key5,
                     NULL purchasing_category,
                     aila.purchasing_category_id,
                     aila.cost_factor_id,
                     NULL cost_factor_name,
                     aila.control_amount,
                     aila.assessable_value,
                     aila.default_dist_ccid,
                     aila.primary_intended_use,
                     aila.ship_to_location_id,
                     aila.product_type,
                     aila.product_category,
                     aila.product_fisc_classification,
                     aila.user_defined_fisc_class,
                     aila.trx_business_category,
                     aila.tax_regime_code,
                     aila.tax,
                     aila.tax_jurisdiction_code,
                     aila.tax_status_code,
                     aila.tax_rate_id,
                     aila.tax_rate_code,
                     aila.tax_rate,
                     NULL incl_in_taxable_line_flag,
                     aila.source_application_id,
                     aila.source_entity_code,
                     aila.source_event_class_code,
                     aila.source_trx_id,
                     aila.source_line_id,
                     aila.source_trx_level_type,
                     'NO TAX' tax_classification_code,
                     aida.cc_reversal_flag,
                     aida.company_prepaid_invoice_id,
                     aida.expense_group,
                     aida.justification,
                     aida.merchant_document_number,
                     aida.merchant_reference,
                     aida.merchant_tax_reg_number,
                     aida.merchant_taxpayer_id,
                     aida.receipt_currency_code,
                     aida.receipt_conversion_rate,
                     aida.receipt_currency_amount,
                     aida.country_of_supply,
                     aida.pay_awt_group_id,
                     NULL pay_awt_group_name,
                     NULL expense_start_date,
                     NULL expense_end_date,
                     NULL merchant_name#1
                FROM ap_invoice_lines_all         aila,
                     ap_invoice_distributions_all aida
               WHERE aila.invoice_id = p_invoice_id
                 AND aila.org_id = p_org_id
                 AND aila.invoice_id = aida.invoice_id
                 AND aida.org_id = aila.org_id
                 AND aida.invoice_line_number = aila.line_number
                 AND nvl(aida.amount, 0) <> 0
                 AND nvl(aila.amount, 0) <> 0
                 AND aila.line_type_lookup_code <> 'TAX'
                 AND EXISTS
               (SELECT 1
                        FROM gl_code_combinations gcc
                       WHERE aida.dist_code_combination_id =
                             gcc.code_combination_id
                         AND gcc.segment1 = p_source_le --Source LE
                         AND gcc.segment2 = p_source_plant_name))
       ORDER BY line_number;
  
    CURSOR get_invc_holds_cur(p_invoice_id IN NUMBER,
                              p_org_id     IN NUMBER) IS
      SELECT NULL                  record_id,
             g_request_id          request_id,
             g_flag_ntprocessed    status_flag,
             NULL                  error_type,
             NULL                  error_message,
             p_invoice_id          old_invoice_id,
             NULL                  new_invoice_id,
             line_location_id,
             hold_lookup_code,
             SYSDATE               last_update_date,
             g_user_id             last_updated_by,
             held_by,
             hold_date,
             hold_reason,
             release_lookup_code,
             release_reason,
             g_login_id            last_update_login,
             SYSDATE               creation_date,
             g_user_id             created_by,
             org_id,
             g_resp_id             responsibility_id,
             rcv_transaction_id,
             hold_details,
             line_number,
             hold_id,
             wf_status,
             validation_request_id
        FROM ap_holds_all
       WHERE invoice_id = p_invoice_id
         AND org_id = p_org_id
         AND release_lookup_code IS NULL
         AND (status_flag IS NULL OR status_flag = 'S');
  
    l_source_org_id          hr_operating_units.organization_id%TYPE;
    l_source_org_name        hr_operating_units.name%TYPE;
    l_source_plant_name      fnd_lookup_values.lookup_code%TYPE;
    l_source_le              fnd_lookup_values.lookup_code%TYPE;
    l_destination_org_id     hr_operating_units.organization_id%TYPE;
    l_destination_org_name   hr_operating_units.name%TYPE;
    l_destination_plant_name fnd_lookup_values.meaning%TYPE;
    l_destination_le         fnd_lookup_values.meaning%TYPE;
    l_operation_name         fnd_lookup_values.tag%TYPE;
    l_receipt_required_flag  po_line_locations_all.receipt_required_flag%TYPE;
  
    l_po_info                VARCHAR2(200);
    l_po_number              po_headers_all.segment1%TYPE;
    l_po_line_number         po_lines_all.line_num%TYPE;
    l_po_shipment_number     po_line_locations_all.shipment_num%TYPE;
    l_po_distribution_number po_distributions_all.distribution_num%TYPE;
  
    TYPE l_invc_infc_stg_tbl IS TABLE OF xxetn.xxap_invc_intfc_stg%ROWTYPE;
  
    l_invc_infc_stg l_invc_infc_stg_tbl;
  
    TYPE l_invc_infc_lines_stg_tbl IS TABLE OF xxetn.xxap_invc_lines_intfc_stg%ROWTYPE;
  
    l_invc_infc_lines_stg l_invc_infc_lines_stg_tbl;
  
    TYPE l_invc_holds_stg_tbl IS TABLE OF xxetn.xxap_invc_holds_conv_stg%ROWTYPE;
  
    l_payment_priority          ap_payment_schedules_all.payment_priority%TYPE;
    l_due_date                  ap_payment_schedules_all.due_date%TYPE;
    l_invc_holds_stg            l_invc_holds_stg_tbl;
    i                           NUMBER := 1;
    j                           NUMBER := 1;
    p                           NUMBER := 1;
    l_line_number               NUMBER := 1;
    lv_dist_concat_segment      VARCHAR2(750);
    ln_dist_code_comb_id        NUMBER;
    lv_new_dist_concat_segment  VARCHAR2(750);
    ln_dist_chart_of_account_id NUMBER;
    lv_dist_error_message       VARCHAR2(1000);
    l_errbuf                    VARCHAR2(2000) := NULL;
    l_retcode                   NUMBER := NULL;
  BEGIN
    p_errbuf            := NULL;
    p_retcode           := g_normal;
    g_loader_request_id := g_request_id;
    debug('Go Live Date - ' || g_go_live_date);
    debug('Start of loop for AP Invoices');
    i                     := 1;
    j                     := 1;
    p                     := 1;
    l_invc_infc_stg       := l_invc_infc_stg_tbl();
    l_invc_infc_lines_stg := l_invc_infc_lines_stg_tbl();
    l_invc_holds_stg      := l_invc_holds_stg_tbl();
  
    FOR rec_get_lookup_scenarios IN get_lookup_scenarios_cur
    LOOP
      l_source_org_id          := rec_get_lookup_scenarios.source_org_id;
      l_source_org_name        := rec_get_lookup_scenarios.source_org_name;
      l_source_plant_name      := rec_get_lookup_scenarios.source_plant_name;
      l_source_le              := rec_get_lookup_scenarios.source_le;
      l_destination_org_id     := rec_get_lookup_scenarios.destination_org_id;
      l_destination_org_name   := rec_get_lookup_scenarios.destination_org_name;
      l_destination_plant_name := rec_get_lookup_scenarios.destination_plant_name;
      l_destination_le         := rec_get_lookup_scenarios.destination_le;
      l_operation_name         := rec_get_lookup_scenarios.operation_name;
      debug('Source Plant Name: ' || l_source_plant_name);
      debug('Source Legal Entity: ' || l_source_le);
      debug('Source Org ID: ' || l_source_org_id);
      debug('Target Plant Name: ' || l_destination_plant_name);
      debug('Target Legal Entity: ' || l_destination_le);
      debug('Target Org ID: ' || l_destination_org_id);
      debug('Operation: ' || l_operation_name);
    
      FOR rec_get_ap_inv_head IN cur_get_ap_inv_head(l_source_org_id,
                                                     l_source_plant_name,
                                                     l_source_le,
                                                     l_destination_org_id,
                                                     l_destination_plant_name,
                                                     l_destination_le,
                                                     l_destination_org_name,
                                                     l_operation_name)
      LOOP
        l_payment_priority := NULL;
        l_due_date         := NULL;
        IF rec_get_ap_inv_head.invoice_type_lookup_code <> 'PREPAYMENT'
        THEN
          validate_pay_sch(p_old_invoice_id   => rec_get_ap_inv_head.old_invoice_id,
                           p_old_org_id       => rec_get_ap_inv_head.org_id,
                           p_payment_priority => l_payment_priority,
                           p_due_date         => l_due_date,
                           p_errbuf           => l_errbuf,
                           p_retcode          => l_retcode);
        END IF;
        l_invc_infc_stg.extend;
        l_invc_infc_stg(i).record_id := xxetn.xxap_invc_intfc_stg_rec_s.nextval;
        l_invc_infc_stg(i).request_id := rec_get_ap_inv_head.request_id;
        l_invc_infc_stg(i).status_flag := rec_get_ap_inv_head.status_flag;
        l_invc_infc_stg(i).process_flag := g_flag_yes;
        l_invc_infc_stg(i).record_status := rec_get_ap_inv_head.record_status;
        l_invc_infc_stg(i).error_type := rec_get_ap_inv_head.error_type;
        l_invc_infc_stg(i).error_message := rec_get_ap_inv_head.error_message;
        l_invc_infc_stg(i).operation := rec_get_ap_inv_head.operation;
        l_invc_infc_stg(i).invoice_id := ap_invoices_interface_s.nextval; --ap_invoices_interface_s.nextval
        l_invc_infc_stg(i).old_invoice_id := rec_get_ap_inv_head.old_invoice_id;
        l_invc_infc_stg(i).new_invoice_id := rec_get_ap_inv_head.new_invoice_id;
        l_invc_infc_stg(i).is_invc_cancelled := rec_get_ap_inv_head.is_invc_cancelled;
        l_invc_infc_stg(i).num_of_holds := rec_get_ap_inv_head.num_of_holds;
        l_invc_infc_stg(i).is_invc_on_hold := CASE
                                                WHEN rec_get_ap_inv_head.num_of_holds > 0 THEN
                                                 'Y'
                                                ELSE
                                                 'N'
                                              END;
        l_invc_infc_stg(i).is_partially_paid := rec_get_ap_inv_head.is_partially_paid;
        l_invc_infc_stg(i).amt_partially_paid := rec_get_ap_inv_head.amt_partially_paid;
        l_invc_infc_stg(i).prepay_amt_applied := rec_get_ap_inv_head.prepay_amt_applied;
        l_invc_infc_stg(i).amt_unpaid := rec_get_ap_inv_head.amt_unpaid;
        l_invc_infc_stg(i).prepay_amt_unapplied := rec_get_ap_inv_head.prepay_amt_unapplied;
        l_invc_infc_stg(i).invoice_num := rec_get_ap_inv_head.invoice_num;
        l_invc_infc_stg(i).new_invoice_num := rec_get_ap_inv_head.new_invoice_num;
        l_invc_infc_stg(i).invoice_type_lookup_code := rec_get_ap_inv_head.invoice_type_lookup_code;
        l_invc_infc_stg(i).invoice_date := rec_get_ap_inv_head.invoice_date;
        l_invc_infc_stg(i).po_number := rec_get_ap_inv_head.po_number;
        l_invc_infc_stg(i).vendor_id := rec_get_ap_inv_head.vendor_id;
        l_invc_infc_stg(i).vendor_num := rec_get_ap_inv_head.vendor_num;
        l_invc_infc_stg(i).vendor_name := rec_get_ap_inv_head.vendor_name;
        l_invc_infc_stg(i).old_vendor_site_id := rec_get_ap_inv_head.old_vendor_site_id;
        l_invc_infc_stg(i).new_vendor_site_id := rec_get_ap_inv_head.new_vendor_site_id;
        l_invc_infc_stg(i).vendor_site_code := rec_get_ap_inv_head.vendor_site_code;
        l_invc_infc_stg(i).invoice_amount := rec_get_ap_inv_head.invoice_amount;
        l_invc_infc_stg(i).invoice_currency_code := rec_get_ap_inv_head.invoice_currency_code;
        l_invc_infc_stg(i).exchange_rate := rec_get_ap_inv_head.exchange_rate;
        l_invc_infc_stg(i).exchange_rate_type := CASE
                                                   WHEN rec_get_ap_inv_head.exchange_rate IS NOT NULL THEN
                                                    'User'
                                                   ELSE
                                                    rec_get_ap_inv_head.exchange_rate_type
                                                 END;
        l_invc_infc_stg(i).exchange_date := rec_get_ap_inv_head.exchange_date;
        l_invc_infc_stg(i).terms_id := rec_get_ap_inv_head.terms_id;
        l_invc_infc_stg(i).terms_name := rec_get_ap_inv_head.terms_name;
        l_invc_infc_stg(i).description := rec_get_ap_inv_head.description;
        l_invc_infc_stg(i).awt_group_id := rec_get_ap_inv_head.awt_group_id;
        l_invc_infc_stg(i).awt_group_name := rec_get_ap_inv_head.awt_group_name;
        l_invc_infc_stg(i).last_update_date := rec_get_ap_inv_head.last_update_date;
        l_invc_infc_stg(i).last_updated_by := rec_get_ap_inv_head.last_updated_by;
        l_invc_infc_stg(i).last_update_login := rec_get_ap_inv_head.last_update_login;
        l_invc_infc_stg(i).creation_date := rec_get_ap_inv_head.creation_date;
        l_invc_infc_stg(i).created_by := rec_get_ap_inv_head.created_by;
        l_invc_infc_stg(i).attribute_category := rec_get_ap_inv_head.attribute_category;
        l_invc_infc_stg(i).attribute1 := rec_get_ap_inv_head.attribute1;
        l_invc_infc_stg(i).attribute2 := rec_get_ap_inv_head.attribute2;
        l_invc_infc_stg(i).attribute3 := rec_get_ap_inv_head.attribute3;
        l_invc_infc_stg(i).attribute4 := rec_get_ap_inv_head.attribute4;
        l_invc_infc_stg(i).attribute5 := rec_get_ap_inv_head.attribute5;
        l_invc_infc_stg(i).attribute6 := rec_get_ap_inv_head.attribute6;
        l_invc_infc_stg(i).attribute7 := rec_get_ap_inv_head.attribute7;
        l_invc_infc_stg(i).attribute8 := rec_get_ap_inv_head.attribute8;
        l_invc_infc_stg(i).attribute9 := rec_get_ap_inv_head.attribute9;
        l_invc_infc_stg(i).attribute10 := rec_get_ap_inv_head.attribute10;
        l_invc_infc_stg(i).attribute11 := rec_get_ap_inv_head.attribute11;
        l_invc_infc_stg(i).attribute12 := rec_get_ap_inv_head.attribute12;
        l_invc_infc_stg(i).attribute13 := rec_get_ap_inv_head.attribute13;
        l_invc_infc_stg(i).attribute14 := rec_get_ap_inv_head.attribute14;
        l_invc_infc_stg(i).attribute15 := rec_get_ap_inv_head.attribute15;
        l_invc_infc_stg(i).global_attribute_category := rec_get_ap_inv_head.global_attribute_category;
        l_invc_infc_stg(i).global_attribute1 := rec_get_ap_inv_head.global_attribute1;
        l_invc_infc_stg(i).global_attribute2 := rec_get_ap_inv_head.global_attribute2;
        l_invc_infc_stg(i).global_attribute3 := rec_get_ap_inv_head.global_attribute3;
        l_invc_infc_stg(i).global_attribute4 := rec_get_ap_inv_head.global_attribute4;
        l_invc_infc_stg(i).global_attribute5 := rec_get_ap_inv_head.global_attribute5;
        l_invc_infc_stg(i).global_attribute6 := rec_get_ap_inv_head.global_attribute6;
        l_invc_infc_stg(i).global_attribute7 := rec_get_ap_inv_head.global_attribute7;
        l_invc_infc_stg(i).global_attribute8 := rec_get_ap_inv_head.global_attribute8;
        l_invc_infc_stg(i).global_attribute9 := rec_get_ap_inv_head.global_attribute9;
        l_invc_infc_stg(i).global_attribute10 := rec_get_ap_inv_head.global_attribute10;
        l_invc_infc_stg(i).global_attribute11 := rec_get_ap_inv_head.global_attribute11;
        l_invc_infc_stg(i).global_attribute12 := rec_get_ap_inv_head.global_attribute12;
        l_invc_infc_stg(i).global_attribute13 := rec_get_ap_inv_head.global_attribute13;
        l_invc_infc_stg(i).global_attribute14 := rec_get_ap_inv_head.global_attribute14;
        l_invc_infc_stg(i).global_attribute15 := rec_get_ap_inv_head.global_attribute15;
        l_invc_infc_stg(i).global_attribute16 := rec_get_ap_inv_head.global_attribute16;
        l_invc_infc_stg(i).global_attribute17 := rec_get_ap_inv_head.global_attribute17;
        l_invc_infc_stg(i).global_attribute18 := rec_get_ap_inv_head.global_attribute18;
        l_invc_infc_stg(i).global_attribute19 := rec_get_ap_inv_head.global_attribute19;
        l_invc_infc_stg(i).global_attribute20 := rec_get_ap_inv_head.global_attribute20;
        l_invc_infc_stg(i).status := rec_get_ap_inv_head.status;
        l_invc_infc_stg(i).source := rec_get_ap_inv_head.source;
        l_invc_infc_stg(i).group_id := rec_get_ap_inv_head.group_id;
        l_invc_infc_stg(i).payment_cross_rate_type := rec_get_ap_inv_head.payment_cross_rate_type;
        l_invc_infc_stg(i).payment_cross_rate_date := rec_get_ap_inv_head.payment_cross_rate_date;
        l_invc_infc_stg(i).payment_cross_rate := rec_get_ap_inv_head.payment_cross_rate;
        l_invc_infc_stg(i).payment_currency_code := rec_get_ap_inv_head.payment_currency_code;
        l_invc_infc_stg(i).workflow_flag := rec_get_ap_inv_head.workflow_flag;
        l_invc_infc_stg(i).doc_category_code := rec_get_ap_inv_head.doc_category_code;
        l_invc_infc_stg(i).voucher_num := rec_get_ap_inv_head.voucher_num;
        l_invc_infc_stg(i).payment_method_lookup_code := rec_get_ap_inv_head.payment_method_lookup_code;
        l_invc_infc_stg(i).pay_group_lookup_code := rec_get_ap_inv_head.pay_group_lookup_code;
        l_invc_infc_stg(i).goods_received_date := rec_get_ap_inv_head.goods_received_date;
        l_invc_infc_stg(i).invoice_received_date := rec_get_ap_inv_head.invoice_received_date;
        l_invc_infc_stg(i).gl_date := rec_get_ap_inv_head.gl_date;
        l_invc_infc_stg(i).accts_pay_code_combination_id := rec_get_ap_inv_head.accts_pay_code_combination_id;
        l_invc_infc_stg(i).ussgl_transaction_code := rec_get_ap_inv_head.ussgl_transaction_code;
        l_invc_infc_stg(i).exclusive_payment_flag := rec_get_ap_inv_head.exclusive_payment_flag;
        l_invc_infc_stg(i).org_id := rec_get_ap_inv_head.org_id;
        l_invc_infc_stg(i).new_org_id := rec_get_ap_inv_head.new_org_id;
        l_invc_infc_stg(i).amount_applicable_to_discount := rec_get_ap_inv_head.amount_applicable_to_discount;
        l_invc_infc_stg(i).prepay_num := rec_get_ap_inv_head.prepay_num;
        l_invc_infc_stg(i).prepay_dist_num := rec_get_ap_inv_head.prepay_dist_num;
        l_invc_infc_stg(i).prepay_apply_amount := rec_get_ap_inv_head.prepay_apply_amount;
        l_invc_infc_stg(i).prepay_gl_date := rec_get_ap_inv_head.prepay_gl_date;
        l_invc_infc_stg(i).invoice_includes_prepay_flag := rec_get_ap_inv_head.invoice_includes_prepay_flag;
        l_invc_infc_stg(i).no_xrate_base_amount := rec_get_ap_inv_head.no_xrate_base_amount;
        l_invc_infc_stg(i).vendor_email_address := rec_get_ap_inv_head.vendor_email_address;
        l_invc_infc_stg(i).terms_date := rec_get_ap_inv_head.terms_date;
        l_invc_infc_stg(i).requester_id := rec_get_ap_inv_head.requester_id;
        l_invc_infc_stg(i).ship_to_location := rec_get_ap_inv_head.ship_to_location;
        l_invc_infc_stg(i).external_doc_ref := rec_get_ap_inv_head.external_doc_ref;
        l_invc_infc_stg(i).prepay_line_num := rec_get_ap_inv_head.prepay_line_num;
        l_invc_infc_stg(i).requester_first_name := rec_get_ap_inv_head.requester_first_name;
        l_invc_infc_stg(i).requester_last_name := rec_get_ap_inv_head.requester_last_name;
        l_invc_infc_stg(i).application_id := rec_get_ap_inv_head.application_id;
        l_invc_infc_stg(i).product_table := rec_get_ap_inv_head.product_table;
        l_invc_infc_stg(i).reference_key1 := rec_get_ap_inv_head.reference_key1;
        l_invc_infc_stg(i).reference_key2 := rec_get_ap_inv_head.reference_key2;
        l_invc_infc_stg(i).reference_key3 := rec_get_ap_inv_head.reference_key3;
        l_invc_infc_stg(i).reference_key4 := rec_get_ap_inv_head.reference_key4;
        l_invc_infc_stg(i).reference_key5 := rec_get_ap_inv_head.reference_key5;
        l_invc_infc_stg(i).apply_advances_flag := rec_get_ap_inv_head.apply_advances_flag;
        l_invc_infc_stg(i).calc_tax_during_import_flag := rec_get_ap_inv_head.calc_tax_during_import_flag;
        l_invc_infc_stg(i).control_amount := CASE
                                               WHEN ap_invoices_pkg.get_posting_status(rec_get_ap_inv_head.old_invoice_id) = 'Y' AND
                                                    rec_get_ap_inv_head.num_of_holds = 0 THEN
                                                rec_get_ap_inv_head.total_tax_amount
                                               ELSE
                                                rec_get_ap_inv_head.control_amount
                                             END;
        l_invc_infc_stg(i).add_tax_to_inv_amt_flag := rec_get_ap_inv_head.add_tax_to_inv_amt_flag;
        l_invc_infc_stg(i).tax_related_invoice_id := rec_get_ap_inv_head.tax_related_invoice_id;
        l_invc_infc_stg(i).taxation_country := rec_get_ap_inv_head.taxation_country;
        l_invc_infc_stg(i).document_sub_type := rec_get_ap_inv_head.document_sub_type;
        l_invc_infc_stg(i).supplier_tax_invoice_number := rec_get_ap_inv_head.supplier_tax_invoice_number;
        l_invc_infc_stg(i).supplier_tax_invoice_date := rec_get_ap_inv_head.supplier_tax_invoice_date;
        l_invc_infc_stg(i).supplier_tax_exchange_rate := rec_get_ap_inv_head.supplier_tax_exchange_rate;
        l_invc_infc_stg(i).tax_invoice_recording_date := rec_get_ap_inv_head.tax_invoice_recording_date;
        l_invc_infc_stg(i).tax_invoice_internal_seq := rec_get_ap_inv_head.tax_invoice_internal_seq;
        l_invc_infc_stg(i).legal_entity_id := rec_get_ap_inv_head.legal_entity_id;
        l_invc_infc_stg(i).new_legal_entity_id := rec_get_ap_inv_head.new_legal_entity_id;
        l_invc_infc_stg(i).legal_entity_name := rec_get_ap_inv_head.legal_entity_name;
        l_invc_infc_stg(i).reference_1 := rec_get_ap_inv_head.reference_1;
        l_invc_infc_stg(i).reference_2 := rec_get_ap_inv_head.reference_2;
        l_invc_infc_stg(i).operating_unit := rec_get_ap_inv_head.operating_unit;
        l_invc_infc_stg(i).bank_charge_bearer := rec_get_ap_inv_head.bank_charge_bearer;
        l_invc_infc_stg(i).remittance_message1 := rec_get_ap_inv_head.remittance_message1;
        l_invc_infc_stg(i).remittance_message2 := rec_get_ap_inv_head.remittance_message2;
        l_invc_infc_stg(i).remittance_message3 := rec_get_ap_inv_head.remittance_message3;
        l_invc_infc_stg(i).unique_remittance_identifier := rec_get_ap_inv_head.unique_remittance_identifier;
        l_invc_infc_stg(i).uri_check_digit := rec_get_ap_inv_head.uri_check_digit;
        l_invc_infc_stg(i).settlement_priority := rec_get_ap_inv_head.settlement_priority;
        l_invc_infc_stg(i).payment_reason_code := rec_get_ap_inv_head.payment_reason_code;
        l_invc_infc_stg(i).payment_reason_comments := rec_get_ap_inv_head.payment_reason_comments;
        l_invc_infc_stg(i).payment_method_code := rec_get_ap_inv_head.payment_method_code;
        l_invc_infc_stg(i).delivery_channel_code := rec_get_ap_inv_head.delivery_channel_code;
        l_invc_infc_stg(i).paid_on_behalf_employee_id := rec_get_ap_inv_head.paid_on_behalf_employee_id;
        l_invc_infc_stg(i).net_of_retainage_flag := rec_get_ap_inv_head.net_of_retainage_flag;
        l_invc_infc_stg(i).requester_employee_num := rec_get_ap_inv_head.requester_employee_num;
        l_invc_infc_stg(i).cust_registration_code := rec_get_ap_inv_head.cust_registration_code;
        l_invc_infc_stg(i).cust_registration_number := rec_get_ap_inv_head.cust_registration_number;
        l_invc_infc_stg(i).old_party_id := rec_get_ap_inv_head.old_party_id;
        l_invc_infc_stg(i).new_party_id := rec_get_ap_inv_head.new_party_id;
        l_invc_infc_stg(i).old_party_site_id := rec_get_ap_inv_head.old_party_site_id;
        l_invc_infc_stg(i).new_party_site_id := rec_get_ap_inv_head.new_party_site_id;
        l_invc_infc_stg(i).pay_proc_trxn_type_code := rec_get_ap_inv_head.pay_proc_trxn_type_code;
        l_invc_infc_stg(i).payment_function := rec_get_ap_inv_head.payment_function;
        l_invc_infc_stg(i).payment_priority := l_payment_priority;
        l_invc_infc_stg(i).old_due_date := l_due_date;
        l_invc_infc_stg(i).new_due_date := rec_get_ap_inv_head.new_due_date;
        l_invc_infc_stg(i).port_of_entry_code := rec_get_ap_inv_head.port_of_entry_code;
        l_invc_infc_stg(i).external_bank_account_id := rec_get_ap_inv_head.external_bank_account_id;
        l_invc_infc_stg(i).accts_pay_code_concatenated := rec_get_ap_inv_head.accts_pay_code_concatenated;
        l_invc_infc_stg(i).pay_awt_group_id := rec_get_ap_inv_head.pay_awt_group_id;
        l_invc_infc_stg(i).pay_awt_group_name := rec_get_ap_inv_head.pay_awt_group_name;
        l_invc_infc_stg(i).original_invoice_amount := rec_get_ap_inv_head.original_invoice_amount;
        l_invc_infc_stg(i).dispute_reason := rec_get_ap_inv_head.dispute_reason;
        l_invc_infc_stg(i).remit_to_supplier_name := rec_get_ap_inv_head.remit_to_supplier_name;
        l_invc_infc_stg(i).remit_to_supplier_id := rec_get_ap_inv_head.remit_to_supplier_id;
        l_invc_infc_stg(i).remit_to_supplier_site := rec_get_ap_inv_head.remit_to_supplier_site;
        l_invc_infc_stg(i).remit_to_supplier_site_id := rec_get_ap_inv_head.remit_to_supplier_site_id;
        l_invc_infc_stg(i).relationship_id := rec_get_ap_inv_head.relationship_id;
        l_invc_infc_stg(i).remit_to_supplier_num := rec_get_ap_inv_head.remit_to_supplier_num;
        l_invc_infc_stg(i).batch_id := g_loader_request_id;
        l_invc_infc_stg(i).is_non_po := NULL;
        l_invc_infc_stg(i).receipt_req_flag := NULL;
        l_invc_infc_stg(i).source_le := rec_get_ap_inv_head.source_le;
        l_invc_infc_stg(i).source_plant := rec_get_ap_inv_head.source_plant;
        l_invc_infc_stg(i).destination_le := rec_get_ap_inv_head.destination_le;
        l_invc_infc_stg(i).destination_plant := rec_get_ap_inv_head.destination_plant;
        l_invc_infc_stg(i).posting_status := rec_get_ap_inv_head.posting_status;
        l_line_number := 1;
      
        FOR rec_get_invc_lines IN cur_get_invc_lines(l_source_org_id,
                                                     l_source_plant_name,
                                                     l_source_le,
                                                     l_destination_org_id,
                                                     l_destination_plant_name,
                                                     l_destination_le,
                                                     rec_get_ap_inv_head.old_invoice_id,
                                                     rec_get_ap_inv_head.org_id)
        LOOP
          l_invc_infc_lines_stg.extend;
          l_invc_infc_lines_stg(j).record_id := xxetn.xxap_invc_intfc_stg_rec_s.currval;
          l_invc_infc_lines_stg(j).record_line_id := xxetn.xxap_invc_intfc_stg_rline_s.nextval;
          l_invc_infc_lines_stg(j).request_id := rec_get_invc_lines.request_id;
          l_invc_infc_lines_stg(j).batch_id := g_loader_request_id;
          l_invc_infc_lines_stg(j).status_flag := rec_get_invc_lines.status_flag;
          l_invc_infc_lines_stg(j).error_type := rec_get_invc_lines.error_type;
          l_invc_infc_lines_stg(j).error_message := rec_get_invc_lines.error_message;
          l_invc_infc_lines_stg(j).invoice_id := ap_invoices_interface_s.currval; --ap_invoices_interface_s.currval
          l_invc_infc_lines_stg(j).old_invoice_id := rec_get_invc_lines.old_invoice_id;
          l_invc_infc_lines_stg(j).new_invoice_id := rec_get_invc_lines.new_invoice_id;
          l_invc_infc_lines_stg(j).invoice_line_id := ap_invoice_lines_interface_s.nextval; --ap_invoice_lines_interface_s.nextval
          l_invc_infc_lines_stg(j).old_invoice_line_id := rec_get_invc_lines.old_invoice_line_id;
          l_invc_infc_lines_stg(j).new_invoice_line_id := rec_get_invc_lines.new_invoice_line_id;
          l_invc_infc_lines_stg(j).line_number := l_line_number;
          l_line_number := l_line_number + 1;
          l_invc_infc_lines_stg(j).line_type_lookup_code := rec_get_invc_lines.line_type_lookup_code;
          l_invc_infc_lines_stg(j).line_group_number := rec_get_invc_lines.line_group_number;
          l_invc_infc_lines_stg(j).amount := rec_get_invc_lines.amount;
          l_invc_infc_lines_stg(j).accounting_date := rec_get_invc_lines.accounting_date;
          l_invc_infc_lines_stg(j).description := rec_get_invc_lines.description;
          l_invc_infc_lines_stg(j).amount_includes_tax_flag := rec_get_invc_lines.amount_includes_tax_flag;
          l_invc_infc_lines_stg(j).prorate_across_flag := rec_get_invc_lines.prorate_across_flag;
          l_invc_infc_lines_stg(j).tax_code := rec_get_invc_lines.tax_code;
          l_invc_infc_lines_stg(j).final_match_flag := rec_get_invc_lines.final_match_flag;
        
          l_invc_infc_lines_stg(j).po_unit_of_measure := rec_get_invc_lines.po_unit_of_measure;
          l_invc_infc_lines_stg(j).inventory_item_id := rec_get_invc_lines.inventory_item_id;
          l_invc_infc_lines_stg(j).item_description := rec_get_invc_lines.item_description;
          l_invc_infc_lines_stg(j).quantity_invoiced := CASE
                                                          WHEN rec_get_invc_lines.line_type_lookup_code <>
                                                               'ITEM' AND
                                                               rec_get_invc_lines.po_header_id IS NULL THEN
                                                           NULL
                                                          ELSE
                                                           rec_get_invc_lines.quantity_invoiced
                                                        END;
          l_invc_infc_lines_stg(j).ship_to_location_code := rec_get_invc_lines.ship_to_location_code;
          l_invc_infc_lines_stg(j).unit_price := CASE
                                                   WHEN rec_get_invc_lines.line_type_lookup_code <>
                                                        'ITEM' AND
                                                        rec_get_invc_lines.po_header_id IS NULL THEN
                                                    NULL
                                                   ELSE
                                                    rec_get_invc_lines.unit_price
                                                 END;
          l_invc_infc_lines_stg(j).distribution_set_id := rec_get_invc_lines.distribution_set_id;
          l_invc_infc_lines_stg(j).distribution_set_name := rec_get_invc_lines.distribution_set_name;
          /*Fetching PO Details*/
          l_receipt_required_flag  := 'N';
          l_po_info                := NULL;
          l_po_number              := NULL;
          l_po_line_number         := NULL;
          l_po_shipment_number     := NULL;
          l_po_distribution_number := NULL;
        
          get_po_info(rec_get_invc_lines.po_header_id,
                      rec_get_invc_lines.po_line_id,
                      rec_get_invc_lines.po_line_location_id,
                      rec_get_invc_lines.po_distribution_id,
                      rec_get_invc_lines.org_id,
                      rec_get_ap_inv_head.source_le,
                      rec_get_ap_inv_head.source_plant,
                      rec_get_ap_inv_head.destination_le,
                      rec_get_ap_inv_head.destination_plant,
                      l_receipt_required_flag,
                      l_po_info,
                      l_po_number,
                      l_po_line_number,
                      l_po_shipment_number,
                      l_po_distribution_number);
          l_invc_infc_lines_stg(j).po_header_id := rec_get_invc_lines.po_header_id;
          l_invc_infc_lines_stg(j).po_line_id := rec_get_invc_lines.po_line_id;
          l_invc_infc_lines_stg(j).po_line_location_id := rec_get_invc_lines.po_line_location_id;
          l_invc_infc_lines_stg(j).po_distribution_id := rec_get_invc_lines.po_distribution_id;
        
          IF g_po_param = 'Y' AND
             nvl(rec_get_ap_inv_head.num_of_holds, 0) > 0
          THEN
            IF rec_get_ap_inv_head.po_header_id IS NOT NULL OR
               rec_get_ap_inv_head.quick_po_header_id IS NOT NULL
            THEN
              l_invc_infc_stg(i).po_number := l_po_number;
            ELSE
              l_invc_infc_stg(i).po_number := NULL;
            END IF;
            l_invc_infc_lines_stg(j).po_number := l_po_number;
            l_invc_infc_lines_stg(j).po_line_number := l_po_line_number;
            l_invc_infc_lines_stg(j).po_shipment_num := l_po_shipment_number;
            l_invc_infc_lines_stg(j).po_distribution_num := l_po_distribution_number;
          ELSE
            l_invc_infc_stg(i).po_number := NULL;
            l_invc_infc_lines_stg(j).po_number := NULL;
            l_invc_infc_lines_stg(j).po_line_number := NULL;
            l_invc_infc_lines_stg(j).po_shipment_num := NULL;
            l_invc_infc_lines_stg(j).po_distribution_num := NULL;
          END IF;
        
          l_invc_infc_stg(i).is_non_po := CASE
                                            WHEN l_po_info IS NULL AND
                                                 nvl(l_invc_infc_stg(i).is_non_po,
                                                     'Non-PO') <> 'PO' THEN
                                             'Non-PO'
                                            ELSE
                                             'PO'
                                          END;
          l_invc_infc_stg(i).receipt_req_flag := CASE
                                                   WHEN nvl(l_invc_infc_stg(i)
                                                            .receipt_req_flag,
                                                            'N') <> 'Y' THEN
                                                    l_receipt_required_flag
                                                   ELSE
                                                    'Y'
                                                 END;
          lv_new_dist_concat_segment := NULL;
          --Deriving new dist_code_combination_id
          get_code_combination_segments(rec_get_invc_lines.old_dist_code_combination_id,
                                        lv_dist_concat_segment,
                                        ln_dist_chart_of_account_id);
        
          IF rec_get_invc_lines.dist_line_type_lookup_code IN
             ('ACCRUAL', 'EXPENSE', 'ITEM') AND
             nvl(rec_get_ap_inv_head.num_of_holds, 0) = 0
          THEN
          
            FOR get_conv_account_rec IN get_conv_account_cur
            LOOP
              l_conv_account := get_conv_account_rec.profile_option_value;
            END LOOP;
          
            lv_new_dist_concat_segment := l_destination_le || '.' ||
                                          l_destination_plant_name || '.' ||
                                          l_conv_account;
          ELSE
            lv_new_dist_concat_flip := l_destination_le || '.' ||
                                       l_destination_plant_name ||
                                       substr(lv_dist_concat_segment,
                                              instr(lv_dist_concat_segment,
                                                    '.',
                                                    1,
                                                    2));
          
            FOR flip_segment6_rec IN flip_segment6_cur(rec_get_invc_lines.old_dist_code_combination_id,
                                                       l_source_le)
            LOOP
              lv_new_dist_concat_segment := substr(lv_new_dist_concat_flip,
                                                   1,
                                                   instr(lv_new_dist_concat_flip,
                                                         '.',
                                                         1,
                                                         5) - 1) || '.' ||
                                            l_destination_le ||
                                            substr(lv_new_dist_concat_flip,
                                                   instr(lv_new_dist_concat_flip,
                                                         '.',
                                                         1,
                                                         6));
            END LOOP;
            lv_new_dist_concat_segment := nvl(lv_new_dist_concat_segment,
                                              lv_new_dist_concat_flip);
          END IF;
          ln_dist_code_comb_id := NULL;
          get_code_combination_id(lv_new_dist_concat_segment,
                                  ln_dist_chart_of_account_id,
                                  ln_dist_code_comb_id,
                                  lv_dist_error_message);
          l_invc_infc_lines_stg(j).dist_code_concatenated := lv_new_dist_concat_segment;
          l_invc_infc_lines_stg(j).dist_code_combination_id := ln_dist_code_comb_id;
        
          IF g_po_param = 'Y' AND
             nvl(rec_get_ap_inv_head.num_of_holds, 0) > 0
          THEN
            l_invc_infc_lines_stg(j).dist_code_concatenated := NULL;
            l_invc_infc_lines_stg(j).dist_code_combination_id := NULL;
          END IF;
          l_invc_infc_lines_stg(j).old_dist_code_combination_id := rec_get_invc_lines.old_dist_code_combination_id;
          l_invc_infc_lines_stg(j).chart_of_accounts_id := ln_dist_chart_of_account_id;
          l_invc_infc_lines_stg(j).awt_group_id := rec_get_invc_lines.awt_group_id;
          l_invc_infc_lines_stg(j).awt_group_name := rec_get_invc_lines.awt_group_name;
          l_invc_infc_lines_stg(j).last_updated_by := rec_get_invc_lines.last_updated_by;
          l_invc_infc_lines_stg(j).last_update_date := rec_get_invc_lines.last_update_date;
          l_invc_infc_lines_stg(j).last_update_login := rec_get_invc_lines.last_update_login;
          l_invc_infc_lines_stg(j).created_by := rec_get_invc_lines.created_by;
          l_invc_infc_lines_stg(j).creation_date := rec_get_invc_lines.creation_date;
          l_invc_infc_lines_stg(j).attribute_category := CASE
                                                           WHEN l_po_info IS NOT NULL THEN
                                                            'Conversion'
                                                           ELSE
                                                            rec_get_invc_lines.attribute_category
                                                         END;
          l_invc_infc_lines_stg(j).attribute1 := CASE
                                                   WHEN l_po_info IS NOT NULL THEN
                                                    l_po_info
                                                   ELSE
                                                    rec_get_invc_lines.attribute1
                                                 END;
          l_invc_infc_lines_stg(j).attribute2 := rec_get_invc_lines.attribute2;
          l_invc_infc_lines_stg(j).attribute3 := rec_get_invc_lines.attribute3;
          l_invc_infc_lines_stg(j).attribute4 := rec_get_invc_lines.attribute4;
          l_invc_infc_lines_stg(j).attribute5 := rec_get_invc_lines.attribute5;
          l_invc_infc_lines_stg(j).attribute6 := rec_get_invc_lines.attribute6;
          l_invc_infc_lines_stg(j).attribute7 := rec_get_invc_lines.attribute7;
          l_invc_infc_lines_stg(j).attribute8 := rec_get_invc_lines.attribute8;
          l_invc_infc_lines_stg(j).attribute9 := rec_get_invc_lines.attribute9;
          l_invc_infc_lines_stg(j).attribute10 := rec_get_invc_lines.attribute10;
          l_invc_infc_lines_stg(j).attribute11 := rec_get_invc_lines.attribute11;
          l_invc_infc_lines_stg(j).attribute12 := rec_get_invc_lines.attribute12;
          l_invc_infc_lines_stg(j).attribute13 := rec_get_invc_lines.attribute13;
          l_invc_infc_lines_stg(j).attribute14 := rec_get_invc_lines.attribute14;
          l_invc_infc_lines_stg(j).attribute15 := rec_get_invc_lines.attribute15;
          l_invc_infc_lines_stg(j).global_attribute_category := rec_get_invc_lines.global_attribute_category;
          l_invc_infc_lines_stg(j).global_attribute1 := rec_get_invc_lines.global_attribute1;
          l_invc_infc_lines_stg(j).global_attribute2 := rec_get_invc_lines.global_attribute2;
          l_invc_infc_lines_stg(j).global_attribute3 := rec_get_invc_lines.global_attribute3;
          l_invc_infc_lines_stg(j).global_attribute4 := rec_get_invc_lines.global_attribute4;
          l_invc_infc_lines_stg(j).global_attribute5 := rec_get_invc_lines.global_attribute5;
          l_invc_infc_lines_stg(j).global_attribute6 := rec_get_invc_lines.global_attribute6;
          l_invc_infc_lines_stg(j).global_attribute7 := rec_get_invc_lines.global_attribute7;
          l_invc_infc_lines_stg(j).global_attribute8 := rec_get_invc_lines.global_attribute8;
          l_invc_infc_lines_stg(j).global_attribute9 := rec_get_invc_lines.global_attribute9;
          l_invc_infc_lines_stg(j).global_attribute10 := rec_get_invc_lines.global_attribute10;
          l_invc_infc_lines_stg(j).global_attribute11 := rec_get_invc_lines.global_attribute11;
          l_invc_infc_lines_stg(j).global_attribute12 := rec_get_invc_lines.global_attribute12;
          l_invc_infc_lines_stg(j).global_attribute13 := rec_get_invc_lines.global_attribute13;
          l_invc_infc_lines_stg(j).global_attribute14 := rec_get_invc_lines.global_attribute14;
          l_invc_infc_lines_stg(j).global_attribute15 := rec_get_invc_lines.global_attribute15;
          l_invc_infc_lines_stg(j).global_attribute16 := rec_get_invc_lines.global_attribute16;
          l_invc_infc_lines_stg(j).global_attribute17 := rec_get_invc_lines.global_attribute17;
          l_invc_infc_lines_stg(j).global_attribute18 := rec_get_invc_lines.global_attribute18;
          l_invc_infc_lines_stg(j).global_attribute19 := rec_get_invc_lines.global_attribute19;
          l_invc_infc_lines_stg(j).global_attribute20 := rec_get_invc_lines.global_attribute20;
          l_invc_infc_lines_stg(j).po_release_id := rec_get_invc_lines.po_release_id;
          l_invc_infc_lines_stg(j).release_num := rec_get_invc_lines.release_num;
          l_invc_infc_lines_stg(j).account_segment := rec_get_invc_lines.account_segment;
          l_invc_infc_lines_stg(j).balancing_segment := rec_get_invc_lines.balancing_segment;
          l_invc_infc_lines_stg(j).cost_center_segment := rec_get_invc_lines.cost_center_segment;
          l_invc_infc_lines_stg(j).old_project_id := rec_get_invc_lines.old_project_id;
          l_invc_infc_lines_stg(j).new_project_id := rec_get_invc_lines.new_project_id;
          l_invc_infc_lines_stg(j).old_task_id := rec_get_invc_lines.old_task_id;
          l_invc_infc_lines_stg(j).new_task_id := rec_get_invc_lines.new_task_id;
          l_invc_infc_lines_stg(j).expenditure_type := rec_get_invc_lines.expenditure_type;
          l_invc_infc_lines_stg(j).expenditure_item_date := rec_get_invc_lines.expenditure_item_date;
          l_invc_infc_lines_stg(j).expenditure_organization_id := rec_get_invc_lines.expenditure_organization_id;
          l_invc_infc_lines_stg(j).project_accounting_context := rec_get_invc_lines.project_accounting_context;
          l_invc_infc_lines_stg(j).pa_addition_flag := rec_get_invc_lines.pa_addition_flag;
          l_invc_infc_lines_stg(j).pa_quantity := rec_get_invc_lines.pa_quantity;
          l_invc_infc_lines_stg(j).ussgl_transaction_code := rec_get_invc_lines.ussgl_transaction_code;
          l_invc_infc_lines_stg(j).stat_amount := rec_get_invc_lines.stat_amount;
          l_invc_infc_lines_stg(j).type_1099 := rec_get_invc_lines.type_1099;
          l_invc_infc_lines_stg(j).income_tax_region := rec_get_invc_lines.income_tax_region;
          l_invc_infc_lines_stg(j).assets_tracking_flag := rec_get_invc_lines.assets_tracking_flag;
          l_invc_infc_lines_stg(j).price_correction_flag := rec_get_invc_lines.price_correction_flag;
          l_invc_infc_lines_stg(j).org_id := rec_get_invc_lines.org_id;
          l_invc_infc_lines_stg(j).new_org_id := rec_get_invc_lines.new_org_id;
          l_invc_infc_lines_stg(j).receipt_number := rec_get_invc_lines.receipt_number;
          l_invc_infc_lines_stg(j).receipt_line_number := rec_get_invc_lines.receipt_line_number;
          l_invc_infc_lines_stg(j).match_option := rec_get_invc_lines.match_option;
          l_invc_infc_lines_stg(j).packing_slip := rec_get_invc_lines.packing_slip;
          l_invc_infc_lines_stg(j).rcv_transaction_id := rec_get_invc_lines.rcv_transaction_id;
          l_invc_infc_lines_stg(j).pa_cc_ar_invoice_id := rec_get_invc_lines.pa_cc_ar_invoice_id;
          l_invc_infc_lines_stg(j).pa_cc_ar_invoice_line_num := rec_get_invc_lines.pa_cc_ar_invoice_line_num;
          l_invc_infc_lines_stg(j).reference_1 := rec_get_invc_lines.reference_1;
          l_invc_infc_lines_stg(j).reference_2 := rec_get_invc_lines.reference_2;
          l_invc_infc_lines_stg(j).pa_cc_processed_code := rec_get_invc_lines.pa_cc_processed_code;
          l_invc_infc_lines_stg(j).tax_recovery_rate := rec_get_invc_lines.tax_recovery_rate;
          l_invc_infc_lines_stg(j).tax_recovery_override_flag := rec_get_invc_lines.tax_recovery_override_flag;
          l_invc_infc_lines_stg(j).tax_recoverable_flag := rec_get_invc_lines.tax_recoverable_flag;
          l_invc_infc_lines_stg(j).tax_code_override_flag := rec_get_invc_lines.tax_code_override_flag;
          l_invc_infc_lines_stg(j).tax_code_id := rec_get_invc_lines.tax_code_id;
          l_invc_infc_lines_stg(j).credit_card_trx_id := rec_get_invc_lines.credit_card_trx_id;
          l_invc_infc_lines_stg(j).award_id := rec_get_invc_lines.award_id;
          l_invc_infc_lines_stg(j).vendor_item_num := rec_get_invc_lines.vendor_item_num;
          l_invc_infc_lines_stg(j).taxable_flag := rec_get_invc_lines.taxable_flag;
          l_invc_infc_lines_stg(j).price_correct_inv_num := rec_get_invc_lines.price_correct_inv_num;
          l_invc_infc_lines_stg(j).external_doc_line_ref := rec_get_invc_lines.external_doc_line_ref;
          l_invc_infc_lines_stg(j).serial_number := rec_get_invc_lines.serial_number;
          l_invc_infc_lines_stg(j).manufacturer := rec_get_invc_lines.manufacturer;
          l_invc_infc_lines_stg(j).model_number := rec_get_invc_lines.model_number;
          l_invc_infc_lines_stg(j).warranty_number := rec_get_invc_lines.warranty_number;
          l_invc_infc_lines_stg(j).deferred_acctg_flag := rec_get_invc_lines.deferred_acctg_flag;
          l_invc_infc_lines_stg(j).def_acctg_start_date := rec_get_invc_lines.def_acctg_start_date;
          l_invc_infc_lines_stg(j).def_acctg_end_date := rec_get_invc_lines.def_acctg_end_date;
          l_invc_infc_lines_stg(j).def_acctg_number_of_periods := rec_get_invc_lines.def_acctg_number_of_periods;
          l_invc_infc_lines_stg(j).def_acctg_period_type := rec_get_invc_lines.def_acctg_period_type;
          l_invc_infc_lines_stg(j).unit_of_meas_lookup_code := rec_get_invc_lines.unit_of_meas_lookup_code;
          l_invc_infc_lines_stg(j).price_correct_inv_line_num := rec_get_invc_lines.price_correct_inv_line_num;
          l_invc_infc_lines_stg(j).asset_book_type_code := rec_get_invc_lines.asset_book_type_code;
          l_invc_infc_lines_stg(j).asset_category_id := rec_get_invc_lines.asset_category_id;
          l_invc_infc_lines_stg(j).requester_id := rec_get_invc_lines.requester_id;
          l_invc_infc_lines_stg(j).requester_first_name := rec_get_invc_lines.requester_first_name;
          l_invc_infc_lines_stg(j).requester_last_name := rec_get_invc_lines.requester_last_name;
          l_invc_infc_lines_stg(j).requester_employee_num := rec_get_invc_lines.requester_employee_num;
          l_invc_infc_lines_stg(j).application_id := rec_get_invc_lines.application_id;
          l_invc_infc_lines_stg(j).product_table := rec_get_invc_lines.product_table;
          l_invc_infc_lines_stg(j).reference_key1 := rec_get_invc_lines.reference_key1;
          l_invc_infc_lines_stg(j).reference_key2 := rec_get_invc_lines.reference_key2;
          l_invc_infc_lines_stg(j).reference_key3 := rec_get_invc_lines.reference_key3;
          l_invc_infc_lines_stg(j).reference_key4 := rec_get_invc_lines.reference_key4;
          l_invc_infc_lines_stg(j).reference_key5 := rec_get_invc_lines.reference_key5;
          l_invc_infc_lines_stg(j).purchasing_category := rec_get_invc_lines.purchasing_category;
          l_invc_infc_lines_stg(j).purchasing_category_id := rec_get_invc_lines.purchasing_category_id;
          l_invc_infc_lines_stg(j).cost_factor_id := rec_get_invc_lines.cost_factor_id;
          l_invc_infc_lines_stg(j).cost_factor_name := rec_get_invc_lines.cost_factor_name;
          l_invc_infc_lines_stg(j).control_amount := rec_get_invc_lines.control_amount;
          l_invc_infc_lines_stg(j).assessable_value := rec_get_invc_lines.assessable_value;
          l_invc_infc_lines_stg(j).default_dist_ccid := rec_get_invc_lines.default_dist_ccid;
          l_invc_infc_lines_stg(j).primary_intended_use := rec_get_invc_lines.primary_intended_use;
          l_invc_infc_lines_stg(j).ship_to_location_id := rec_get_invc_lines.ship_to_location_id;
          l_invc_infc_lines_stg(j).product_type := rec_get_invc_lines.product_type;
          l_invc_infc_lines_stg(j).product_category := rec_get_invc_lines.product_category;
          l_invc_infc_lines_stg(j).product_fisc_classification := rec_get_invc_lines.product_fisc_classification;
          l_invc_infc_lines_stg(j).user_defined_fisc_class := rec_get_invc_lines.user_defined_fisc_class;
          l_invc_infc_lines_stg(j).trx_business_category := rec_get_invc_lines.trx_business_category;
          l_invc_infc_lines_stg(j).tax_regime_code := rec_get_invc_lines.tax_regime_code;
          l_invc_infc_lines_stg(j).tax := rec_get_invc_lines.tax;
          l_invc_infc_lines_stg(j).tax_jurisdiction_code := rec_get_invc_lines.tax_jurisdiction_code;
          l_invc_infc_lines_stg(j).tax_status_code := rec_get_invc_lines.tax_status_code;
          l_invc_infc_lines_stg(j).tax_rate_id := rec_get_invc_lines.tax_rate_id;
          l_invc_infc_lines_stg(j).tax_rate_code := rec_get_invc_lines.tax_rate_code;
          l_invc_infc_lines_stg(j).tax_rate := rec_get_invc_lines.tax_rate;
          l_invc_infc_lines_stg(j).incl_in_taxable_line_flag := rec_get_invc_lines.incl_in_taxable_line_flag;
          l_invc_infc_lines_stg(j).source_application_id := rec_get_invc_lines.source_application_id;
          l_invc_infc_lines_stg(j).source_entity_code := rec_get_invc_lines.source_entity_code;
          l_invc_infc_lines_stg(j).source_event_class_code := rec_get_invc_lines.source_event_class_code;
          l_invc_infc_lines_stg(j).source_trx_id := rec_get_invc_lines.source_trx_id;
          l_invc_infc_lines_stg(j).source_line_id := rec_get_invc_lines.source_line_id;
          l_invc_infc_lines_stg(j).source_trx_level_type := rec_get_invc_lines.source_trx_level_type;
          l_invc_infc_lines_stg(j).tax_classification_code := rec_get_invc_lines.tax_classification_code;
          l_invc_infc_lines_stg(j).cc_reversal_flag := rec_get_invc_lines.cc_reversal_flag;
          l_invc_infc_lines_stg(j).company_prepaid_invoice_id := rec_get_invc_lines.company_prepaid_invoice_id;
          l_invc_infc_lines_stg(j).expense_group := rec_get_invc_lines.expense_group;
          l_invc_infc_lines_stg(j).justification := rec_get_invc_lines.justification;
          l_invc_infc_lines_stg(j).merchant_document_number := rec_get_invc_lines.merchant_document_number;
          l_invc_infc_lines_stg(j).merchant_reference := rec_get_invc_lines.merchant_reference;
          l_invc_infc_lines_stg(j).merchant_tax_reg_number := rec_get_invc_lines.merchant_tax_reg_number;
          l_invc_infc_lines_stg(j).merchant_taxpayer_id := rec_get_invc_lines.merchant_taxpayer_id;
          l_invc_infc_lines_stg(j).receipt_currency_code := rec_get_invc_lines.receipt_currency_code;
          l_invc_infc_lines_stg(j).receipt_conversion_rate := rec_get_invc_lines.receipt_conversion_rate;
          l_invc_infc_lines_stg(j).receipt_currency_amount := rec_get_invc_lines.receipt_currency_amount;
          l_invc_infc_lines_stg(j).country_of_supply := rec_get_invc_lines.country_of_supply;
          l_invc_infc_lines_stg(j).pay_awt_group_id := rec_get_invc_lines.pay_awt_group_id;
          l_invc_infc_lines_stg(j).pay_awt_group_name := rec_get_invc_lines.pay_awt_group_name;
          l_invc_infc_lines_stg(j).expense_start_date := rec_get_invc_lines.expense_start_date;
          l_invc_infc_lines_stg(j).expense_end_date := rec_get_invc_lines.expense_end_date;
          l_invc_infc_lines_stg(j).merchant_name#1 := rec_get_invc_lines.merchant_name#1;
          j := j + 1;
        END LOOP;
      
        FOR rec_invc_holds_cur IN get_invc_holds_cur(rec_get_ap_inv_head.old_invoice_id,
                                                     l_source_org_id)
        LOOP
          l_invc_holds_stg.extend;
          l_invc_holds_stg(p).record_id := xxetn.xxap_invc_intfc_stg_rec_s.currval;
          l_invc_holds_stg(p).request_id := rec_invc_holds_cur.request_id;
          l_invc_holds_stg(p).status_flag := rec_invc_holds_cur.status_flag;
          l_invc_holds_stg(p).error_type := rec_invc_holds_cur.error_type;
          l_invc_holds_stg(p).error_message := rec_invc_holds_cur.error_message;
          l_invc_holds_stg(p).old_invoice_id := rec_invc_holds_cur.old_invoice_id;
          l_invc_holds_stg(p).new_invoice_id := rec_invc_holds_cur.new_invoice_id;
          l_invc_holds_stg(p).line_location_id := rec_invc_holds_cur.line_location_id;
          l_invc_holds_stg(p).hold_lookup_code := rec_invc_holds_cur.hold_lookup_code;
          l_invc_holds_stg(p).last_update_date := rec_invc_holds_cur.last_update_date;
          l_invc_holds_stg(p).last_updated_by := rec_invc_holds_cur.last_updated_by;
          l_invc_holds_stg(p).held_by := rec_invc_holds_cur.held_by;
          l_invc_holds_stg(p).hold_date := rec_invc_holds_cur.hold_date;
          l_invc_holds_stg(p).hold_reason := rec_invc_holds_cur.hold_reason;
          l_invc_holds_stg(p).release_lookup_code := rec_invc_holds_cur.release_lookup_code;
          l_invc_holds_stg(p).release_reason := rec_invc_holds_cur.release_reason;
          l_invc_holds_stg(p).last_update_login := rec_invc_holds_cur.last_update_login;
          l_invc_holds_stg(p).creation_date := rec_invc_holds_cur.creation_date;
          l_invc_holds_stg(p).created_by := rec_invc_holds_cur.created_by;
          l_invc_holds_stg(p).org_id := rec_invc_holds_cur.org_id;
          l_invc_holds_stg(p).responsibility_id := rec_invc_holds_cur.responsibility_id;
          l_invc_holds_stg(p).rcv_transaction_id := rec_invc_holds_cur.rcv_transaction_id;
          l_invc_holds_stg(p).hold_details := rec_invc_holds_cur.hold_details;
          l_invc_holds_stg(p).line_number := rec_invc_holds_cur.line_number;
          l_invc_holds_stg(p).hold_id := rec_invc_holds_cur.hold_id;
          l_invc_holds_stg(p).wf_status := rec_invc_holds_cur.wf_status;
          l_invc_holds_stg(p).validation_request_id := rec_invc_holds_cur.validation_request_id;
          l_invc_holds_stg(p).batch_id := g_loader_request_id;
          p := p + 1;
        END LOOP;
      
        i := i + 1;
      END LOOP;
    
    END LOOP;
  
    debug('Records loaded to all table types');
    BEGIN
    
      FORALL i1 IN l_invc_infc_stg.first .. l_invc_infc_stg.last SAVE
                                            EXCEPTIONS
        INSERT INTO xxetn.xxap_invc_intfc_stg VALUES l_invc_infc_stg (i1);
    EXCEPTION
      WHEN OTHERS THEN
      
        FOR k IN 1 .. SQL%bulk_exceptions.count
        LOOP
          debug('Error Message : ' ||
                SQLERRM(-sql%BULK_EXCEPTIONS(k).error_code));
        END LOOP;
      
        ROLLBACK;
    END;
    debug('Records loaded to all Invoice Header Staging Table');
    BEGIN
    
      FORALL j1 IN l_invc_infc_lines_stg.first .. l_invc_infc_lines_stg.last SAVE
                                                  EXCEPTIONS
        INSERT INTO xxetn.xxap_invc_lines_intfc_stg
        VALUES l_invc_infc_lines_stg
          (j1);
    EXCEPTION
      WHEN OTHERS THEN
      
        FOR k IN 1 .. SQL%bulk_exceptions.count
        LOOP
          debug('Error Message : ' ||
                SQLERRM(-sql%BULK_EXCEPTIONS(k).error_code));
        END LOOP;
      
        ROLLBACK;
    END;
    debug('Records loaded to all Invoice Lines Staging Table');
    BEGIN
    
      FORALL k1 IN l_invc_holds_stg.first .. l_invc_holds_stg.last SAVE
                                             EXCEPTIONS
        INSERT INTO xxetn.xxap_invc_holds_conv_stg
        VALUES l_invc_holds_stg
          (k1);
    EXCEPTION
      WHEN OTHERS THEN
      
        FOR k IN 1 .. SQL%bulk_exceptions.count
        LOOP
          debug('Error Message : ' ||
                SQLERRM(-sql%BULK_EXCEPTIONS(k).error_code));
        END LOOP;
      
        ROLLBACK;
    END;
    COMMIT;
    debug('Records loaded to all Invoice Holds Staging Table');
    fnd_file.put_line(fnd_file.output,
                      '===================================================================================================');
  
    DELETE FROM xxap_invc_lines_intfc_stg xils
     WHERE EXISTS (SELECT 1
              FROM (SELECT SUM(amount) over(PARTITION BY line_group_number, line_type_lookup_code, old_invoice_id, batch_id) amount,
                           line_group_number,
                           line_type_lookup_code,
                           invoice_line_id
                      FROM xxap_invc_lines_intfc_stg
                     WHERE amount <> 0) xils1
             WHERE xils1.amount = 0
               AND xils.invoice_line_id = xils1.invoice_line_id)
       AND batch_id = g_loader_request_id;
    COMMIT;
    debug('Deleted Lines which are reversed');
  
    DELETE FROM xxap_invc_lines_intfc_stg xils
     WHERE xils.line_type_lookup_code = 'TAX'
       AND xils.amount = 0
       AND xils.batch_id = g_loader_request_id
       AND NOT EXISTS
     (SELECT 1
              FROM xxap_invc_lines_intfc_stg xls
             WHERE xils.record_id = xls.record_id
               AND xils.line_group_number = xls.line_group_number
               AND xils.invoice_line_id <> xls.invoice_line_id
               AND xils.batch_id = xls.batch_id
               AND xls.line_type_lookup_code <> 'TAX'
               AND xls.amount <> 0);
    COMMIT;
    debug('Deleted Tax Lines with zero amount and No Lines associated to the tax line');
  
    FOR get_dup_inv_head_rec IN get_dup_inv_head_cur
    LOOP
      IF substr(get_dup_inv_head_rec.destination_les,
                1,
                instr(get_dup_inv_head_rec.destination_les, ',') - 1) =
         substr(get_dup_inv_head_rec.destination_les,
                instr(get_dup_inv_head_rec.destination_les, ',') + 1)
      THEN
        l_destination_les := substr(get_dup_inv_head_rec.destination_les,
                                    1,
                                    instr(get_dup_inv_head_rec.destination_les,
                                          ',') - 1);
      ELSE
        l_destination_les := get_dup_inv_head_rec.destination_les;
      END IF;
    
      UPDATE xxap_invc_intfc_stg
         SET source_plant      = get_dup_inv_head_rec.source_plants,
             destination_le    = l_destination_les,
             destination_plant = get_dup_inv_head_rec.destination_plants
       WHERE batch_id = get_dup_inv_head_rec.batch_id
         AND old_invoice_id = get_dup_inv_head_rec.old_invoice_id
         AND record_id = get_dup_inv_head_rec.min_record_id;
    
      DELETE FROM xxap_invc_holds_conv_stg
       WHERE batch_id = get_dup_inv_head_rec.batch_id
         AND old_invoice_id = get_dup_inv_head_rec.old_invoice_id
         AND record_id <> get_dup_inv_head_rec.min_record_id;
    
      DELETE FROM xxap_invc_intfc_stg
       WHERE batch_id = get_dup_inv_head_rec.batch_id
         AND old_invoice_id = get_dup_inv_head_rec.old_invoice_id
         AND record_id <> get_dup_inv_head_rec.min_record_id;
    
      FOR get_dup_inv_line_rec IN get_dup_inv_line_cur(get_dup_inv_head_rec.old_invoice_id,
                                                       get_dup_inv_head_rec.min_record_id)
      LOOP
        FOR get_invoice_id_rec IN get_invoice_id_cur(get_dup_inv_head_rec.min_record_id)
        LOOP
          UPDATE xxap_invc_lines_intfc_stg
             SET line_number = line_number +
                               get_dup_inv_line_rec.max_line_number,
                 record_id   = get_dup_inv_head_rec.min_record_id,
                 invoice_id  = get_invoice_id_rec.invoice_id
           WHERE batch_id = get_dup_inv_head_rec.batch_id
             AND old_invoice_id = get_dup_inv_head_rec.old_invoice_id
             AND record_id <> get_dup_inv_head_rec.min_record_id;
        END LOOP;
      END LOOP;
    END LOOP;
    COMMIT;
    debug('Combined multiple plants into a single record');
  
    DELETE FROM xxap_invc_lines_intfc_stg xils
     WHERE xils.line_type_lookup_code = 'TAX'
       AND xils.amount = 0
       AND xils.batch_id = g_loader_request_id
       AND xils.tax_recoverable_flag = 'N'
       AND EXISTS
     (SELECT 1
              FROM xxap_invc_lines_intfc_stg xls
             WHERE xils.record_id = xls.record_id
               AND xils.line_group_number = xls.line_group_number
               AND xils.invoice_line_id <> xls.invoice_line_id
               AND xils.batch_id = xls.batch_id
               AND xils.line_type_lookup_code = xls.line_type_lookup_code
               AND xils.tax_regime_code = xls.tax_regime_code
               AND xils.tax = xls.tax
               AND xls.tax_recoverable_flag = 'Y');
    debug('Deleted Non-Recoverable Tax Lines with 0 amount for which Recoverable Tax Line Exists');
  
    DELETE FROM xxap_invc_lines_intfc_stg xils
     WHERE xils.line_type_lookup_code = 'TAX'
       AND xils.amount = 0
       AND xils.batch_id = g_loader_request_id
       AND EXISTS
     (SELECT 1
              FROM xxap_invc_lines_intfc_stg xls
             WHERE xils.record_id = xls.record_id
               AND xils.line_group_number = xls.line_group_number
               AND xils.invoice_line_id <> xls.invoice_line_id
               AND xils.batch_id = xls.batch_id
               AND xils.line_type_lookup_code = xls.line_type_lookup_code
               AND xils.tax_regime_code = xls.tax_regime_code
               AND xils.tax = xls.tax
               AND xls.amount <> 0);
    debug('Deleted Tax Lines with 0 amount for which Non-zero Tax Line Exists');
 
    DELETE FROM xxap_invc_lines_intfc_stg
     WHERE ROWID NOT IN (SELECT MIN(ROWID)
                           FROM xxap_invc_lines_intfc_stg
                          WHERE batch_id = g_loader_request_id
                          GROUP BY line_group_number,
                                   amount,
                                   tax_regime_code,
                                   tax,
                                   tax_recoverable_flag,
                                   batch_id,
                                   record_id)
       AND batch_id = g_loader_request_id;
    debug('Deleted Duplicate Tax Records for Multiple Plants at Distribution');
  COMMIT;
  EXCEPTION
    WHEN OTHERS THEN
      debug('Error: Exception occured in load_invc procedure ' ||
            substr(SQLERRM, 1, 240));
      ROLLBACK;
  END load_invc;

  --
  -- ========================
  -- Procedure: cancel_invc
  -- =============================================================================
  --   This procedure will cancel/retire the invoice
  -- =============================================================================
  --  Input Parameters :
  --  No Input Parameters
  --  Output Parameters :
  --  p_errbuf          : Return error
  --  p_retcode         : Return status
  -- -----------------------------------------------------------------------------
  --
  PROCEDURE cancel_invc(p_errbuf  OUT VARCHAR2,
                        p_retcode OUT NUMBER) IS
    l_boolean               BOOLEAN;
    l_message_name          VARCHAR2(1000);
    l_invoice_amount        NUMBER;
    l_base_amount           NUMBER;
    l_temp_cancelled_amount NUMBER;
    l_cancelled_by          VARCHAR2(1000);
    l_cancelled_amount      NUMBER;
    l_cancelled_date        DATE;
    l_last_update_date      DATE;
    l_orig_prepay_amt       NUMBER;
    l_pay_cur_inv_amt       NUMBER;
    l_token                 VARCHAR2(100);
    l_org_id                xxap_invc_holds_conv_stg.org_id%TYPE;
  
    CURSOR invoice_cancel_cur IS
      SELECT xiis.old_invoice_id,
             xiis.record_id,
             xiis.org_id
        FROM xxap_invc_intfc_stg xiis
       WHERE (xiis.batch_id = g_loader_request_id AND
             xiis.status_flag = g_flag_processed AND
             xiis.operation = 'COPY' AND xiis.new_invoice_id IS NOT NULL AND
             xiis.process_flag = g_flag_yes)
          OR (xiis.operation = 'RETIRE' AND xiis.process_flag = g_flag_yes AND
             xiis.batch_id = g_loader_request_id);
  BEGIN
    l_org_id := NULL;
  
    FOR invoice_cancel_rec IN invoice_cancel_cur
    LOOP
    
      IF l_org_id IS NULL OR l_org_id <> invoice_cancel_rec.org_id
      THEN
        mo_global.set_policy_context('S', invoice_cancel_rec.org_id);
        /*BEGIN
          fnd_global.apps_initialize(user_id      => g_user_id,
                                     resp_id      => g_resp_id,
                                     resp_appl_id => g_resp_appl_id);
        END;*/
        l_org_id := invoice_cancel_rec.org_id;
      END IF;
    
      l_boolean := TRUE;
      l_boolean := ap_cancel_pkg.ap_cancel_single_invoice(p_invoice_id                 => invoice_cancel_rec.old_invoice_id,
                                                          p_last_updated_by            => g_user_id,
                                                          p_last_update_login          => g_login_id,
                                                          p_accounting_date            => g_accounting_date,
                                                          p_message_name               => l_message_name,
                                                          p_invoice_amount             => l_invoice_amount,
                                                          p_base_amount                => l_base_amount,
                                                          p_temp_cancelled_amount      => l_temp_cancelled_amount,
                                                          p_cancelled_by               => l_cancelled_by,
                                                          p_cancelled_amount           => l_cancelled_amount,
                                                          p_cancelled_date             => l_cancelled_date,
                                                          p_last_update_date           => l_last_update_date,
                                                          p_original_prepayment_amount => l_orig_prepay_amt,
                                                          p_pay_curr_invoice_amount    => l_pay_cur_inv_amt,
                                                          p_token                      => l_token,
                                                          p_calling_sequence           => NULL);
    
      IF l_boolean
      THEN
        UPDATE xxap_invc_holds_conv_stg
           SET status_flag = g_flag_completed
         WHERE record_id = invoice_cancel_rec.record_id;
        UPDATE xxap_invc_intfc_stg
           SET record_status     = 'CANCEL-INVC',
               status_flag       = g_flag_completed,
               last_update_date  = SYSDATE,
               last_updated_by   = g_user_id,
               last_update_login = g_login_id,
               request_id        = g_request_id,
               is_invc_cancelled = g_flag_yes
         WHERE record_id = invoice_cancel_rec.record_id;
        COMMIT;
        debug('Successfully Cancelled the Invoice: ' ||
              invoice_cancel_rec.old_invoice_id);
      ELSE
        ROLLBACK;
        UPDATE xxap_invc_holds_conv_stg
           SET error_type    = g_err_imp,
               error_message = 'Failed to Cancel the Invoice: ' ||
                               l_message_name,
               status_flag   = g_flag_error
         WHERE record_id = invoice_cancel_rec.record_id;
        UPDATE xxap_invc_intfc_stg
           SET record_status     = 'CANCEL-INVC-ERROR',
               last_update_date  = SYSDATE,
               last_updated_by   = g_user_id,
               last_update_login = g_login_id,
               request_id        = g_request_id,
               error_message     = 'Failed to Cancel the Invoice: ' ||
                                   l_message_name,
               status_flag       = g_flag_error,
               is_invc_cancelled = g_flag_no
         WHERE record_id = invoice_cancel_rec.record_id;
        debug('Failed to Cancel the Invoice: ' ||
              invoice_cancel_rec.old_invoice_id);
        debug(l_message_name);
        COMMIT;
      END IF;
    
    END LOOP;
  EXCEPTION
    WHEN OTHERS THEN
      p_errbuf  := substr('Exception in Procedure cancel_invc. ' || SQLERRM,
                          1,
                          240);
      p_retcode := g_error;
      debug(p_errbuf);
  END cancel_invc;

  --
  -- ========================
  -- Procedure: cancel_invc_on_hold
  -- =============================================================================
  --   This procedure will cancel the invoices on hold
  -- =============================================================================
  --  Input Parameters :
  --  No Input Parameters
  --  Output Parameters :
  --  p_errbuf          : Return error
  --  p_retcode         : Return status
  -- -----------------------------------------------------------------------------
  --
  PROCEDURE cancel_invc_on_hold(p_errbuf  OUT VARCHAR2,
                                p_retcode OUT NUMBER) IS
    l_boolean               BOOLEAN;
    l_message_name          VARCHAR2(1000);
    l_invoice_amount        NUMBER;
    l_base_amount           NUMBER;
    l_temp_cancelled_amount NUMBER;
    l_cancelled_by          VARCHAR2(1000);
    l_cancelled_amount      NUMBER;
    l_cancelled_date        DATE;
    l_last_update_date      DATE;
    l_orig_prepay_amt       NUMBER;
    l_pay_cur_inv_amt       NUMBER;
    l_token                 VARCHAR2(100);
    l_org_id                xxap_invc_holds_conv_stg.org_id%TYPE;
  
    CURSOR invoice_cancel_cur IS
      SELECT xiis.old_invoice_id,
             xiis.record_id,
             xiis.batch_id,
             xiis.org_id
        FROM xxap_invc_intfc_stg xiis
       WHERE xiis.batch_id = g_loader_request_id
         AND xiis.status_flag IN (g_flag_validated, g_flag_processed)
         AND xiis.process_flag = g_flag_yes
         AND EXISTS (SELECT 1
                FROM xxap_invc_holds_conv_stg xihc
               WHERE xiis.batch_id = xihc.batch_id
                 AND xiis.record_id = xihc.record_id
                 AND xiis.old_invoice_id = xihc.old_invoice_id)
         AND xiis.is_non_po = 'PO'
         AND xiis.operation = 'COPY'
         AND xiis.is_invc_cancelled <> 'Y';
  BEGIN
    l_org_id := NULL;
  
    FOR invoice_cancel_rec IN invoice_cancel_cur
    LOOP
    
      IF l_org_id IS NULL OR l_org_id <> invoice_cancel_rec.org_id
      THEN
        mo_global.set_policy_context('S', invoice_cancel_rec.org_id);
        /*BEGIN
          fnd_global.apps_initialize(user_id      => g_user_id,
                                     resp_id      => g_resp_id,
                                     resp_appl_id => g_resp_appl_id);
        END;*/
        l_org_id := invoice_cancel_rec.org_id;
      END IF;
    
      l_boolean := TRUE;
      l_boolean := ap_cancel_pkg.ap_cancel_single_invoice(p_invoice_id                 => invoice_cancel_rec.old_invoice_id,
                                                          p_last_updated_by            => g_user_id,
                                                          p_last_update_login          => g_login_id,
                                                          p_accounting_date            => g_accounting_date,
                                                          p_message_name               => l_message_name,
                                                          p_invoice_amount             => l_invoice_amount,
                                                          p_base_amount                => l_base_amount,
                                                          p_temp_cancelled_amount      => l_temp_cancelled_amount,
                                                          p_cancelled_by               => l_cancelled_by,
                                                          p_cancelled_amount           => l_cancelled_amount,
                                                          p_cancelled_date             => l_cancelled_date,
                                                          p_last_update_date           => l_last_update_date,
                                                          p_original_prepayment_amount => l_orig_prepay_amt,
                                                          p_pay_curr_invoice_amount    => l_pay_cur_inv_amt,
                                                          p_token                      => l_token,
                                                          p_calling_sequence           => NULL);
    
      IF l_boolean
      THEN
        UPDATE xxap_invc_holds_conv_stg
           SET status_flag = g_flag_validated
         WHERE old_invoice_id = invoice_cancel_rec.old_invoice_id
           AND batch_id = invoice_cancel_rec.batch_id
           AND record_id = invoice_cancel_rec.record_id;
        UPDATE xxap_invc_intfc_stg
           SET record_status     = 'CANCEL-INVC-ON-HOLD',
               last_update_date  = SYSDATE,
               last_updated_by   = g_user_id,
               last_update_login = g_login_id,
               request_id        = g_request_id,
               status_flag       = g_flag_validated,
               is_invc_cancelled = g_flag_yes
         WHERE old_invoice_id = invoice_cancel_rec.old_invoice_id
           AND batch_id = invoice_cancel_rec.batch_id
           AND record_id = invoice_cancel_rec.record_id;
        COMMIT;
        debug('Successfully Cancelled the Invoice: ' ||
              invoice_cancel_rec.old_invoice_id);
      ELSE
        ROLLBACK;
        UPDATE xxap_invc_holds_conv_stg
           SET error_type    = g_err_imp,
               error_message = 'Failed to Cancel the Invoice with PO on Hold: ' ||
                               l_message_name,
               status_flag   = g_flag_error
         WHERE old_invoice_id = invoice_cancel_rec.old_invoice_id
           AND batch_id = invoice_cancel_rec.batch_id
           AND record_id = invoice_cancel_rec.record_id;
        UPDATE xxap_invc_intfc_stg
           SET record_status     = 'CANCEL-INVC-ON-HOLD-ERROR',
               error_type        = g_err_imp,
               last_update_date  = SYSDATE,
               last_updated_by   = g_user_id,
               last_update_login = g_login_id,
               request_id        = g_request_id,
               error_message     = 'Failed to Cancel the Invoice with PO on Hold: ' ||
                                   l_message_name,
               status_flag       = g_flag_error,
               is_invc_cancelled = g_flag_no
         WHERE record_id = invoice_cancel_rec.record_id;
        debug('Failed to Cancel the Invoice: ' ||
              invoice_cancel_rec.old_invoice_id);
        debug(l_message_name);
        COMMIT;
      END IF;
    
    END LOOP;
  EXCEPTION
    WHEN OTHERS THEN
      p_errbuf  := substr('Exception in Procedure cancel_invc_on_hold. ' ||
                          SQLERRM,
                          1,
                          240);
      p_retcode := g_error;
      debug(p_errbuf);
  END cancel_invc_on_hold;

  -- =============================================================================
  -- Function: get_hold_count
  -- =============================================================================
  --   To get count of the number of holds
  -- =============================================================================
  --  Input Parameters :
  --    p_invoice_id  : Source Invoice ID
  --    p_org_id      : Source Org ID
  --  Output Parameters :
  --    p_retcode   : Program Return Code = 0/1/2
  --    p_errbuf    : Error message in case of any failure
  -- -----------------------------------------------------------------------------
  FUNCTION get_hold_count(p_invoice_id IN ap_holds_all.invoice_id%TYPE,
                          p_org_id     IN ap_holds_all.org_id%TYPE)
    RETURN NUMBER IS
    l_holds_cnt NUMBER := 0;
  
    CURSOR cur_get_hold_count IS
      SELECT COUNT(1)
        FROM ap_holds_all
       WHERE invoice_id = p_invoice_id
         AND org_id = p_org_id
         AND release_lookup_code IS NULL
         AND (status_flag IS NULL OR status_flag = 'S');
  BEGIN
    l_holds_cnt := 0;
    OPEN cur_get_hold_count;
    FETCH cur_get_hold_count
      INTO l_holds_cnt;
    CLOSE cur_get_hold_count;
    RETURN l_holds_cnt;
  EXCEPTION
    WHEN OTHERS THEN
      debug('Failed to get the count of holds in the Invoice');
      RETURN 0;
  END get_hold_count;

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
  --  p_batch_id        : Batch id
  --  p_new_invoice_id  : New Invoice ID
  --  p_new_org_id      : Source Org ID
  --  Output Parameters :
  --  p_errbuf          : Return error
  --  p_retcode         : Return status
  -- -----------------------------------------------------------------------------
  --
  PROCEDURE hold_invoice(p_errbuf            OUT NOCOPY VARCHAR2,
                         p_retcode           OUT NOCOPY NUMBER,
                         p_new_invoice_id    IN xxap_invc_intfc_stg.new_invoice_id%TYPE,
                         p_is_non_po         IN xxap_invc_intfc_stg.is_non_po%TYPE,
                         p_is_invc_cancelled IN xxap_invc_intfc_stg.is_invc_cancelled%TYPE,
                         p_receipt_req_flag  IN xxap_invc_intfc_stg.receipt_req_flag%TYPE,
                         p_record_id         IN xxap_invc_intfc_stg.record_id%TYPE,
                         p_new_org_id        IN xxap_invc_intfc_stg.new_org_id%TYPE,
                         p_batch_id          IN xxap_invc_intfc_stg.batch_id%TYPE) IS
    l_err_msg          VARCHAR2(4000);
    l_error_flag       VARCHAR2(10);
    l_error_message    VARCHAR2(4000);
    l_calling_sequence VARCHAR2(1000);
    l_success          VARCHAR2(1);
  
    ---Cursor for fetching Invoice holds
    CURSOR val_invoices_hold_cur IS
      SELECT xihs.hold_lookup_code,
             xihs.hold_reason,
             ahc.hold_type,
             xiis.new_org_id,
             xiis.new_invoice_id,
             xihs.record_id,
             p_is_invc_cancelled is_invc_cancelled
        FROM xxap_invc_holds_conv_stg xihs,
             ap_hold_codes            ahc,
             xxap_invc_intfc_stg      xiis
       WHERE ahc.hold_lookup_code = xihs.hold_lookup_code
         AND xiis.record_id = xihs.record_id
         AND xiis.operation = 'COPY'
         AND xiis.process_flag = g_flag_yes
         AND xihs.batch_id = p_batch_id
         AND xiis.new_org_id = p_new_org_id
         AND xiis.new_invoice_id = p_new_invoice_id
         AND xiis.record_id = p_record_id
         AND EXISTS
       (SELECT 1
                FROM fnd_lookup_values flv
               WHERE flv.lookup_type = 'XXETN_RESTRUCTURE_HOLD_MAP'
                 AND flv.language = userenv('LANG')
                 AND flv.enabled_flag = 'Y'
                 AND flv.lookup_code = ahc.hold_lookup_code
                 AND SYSDATE BETWEEN nvl(start_date_active, SYSDATE - 1) AND
                     nvl(end_date_active, SYSDATE + 1))
         AND NOT EXISTS
       (SELECT 1
                FROM ap_holds_all aha
               WHERE aha.invoice_id = xiis.new_invoice_id
                 AND release_lookup_code IS NULL
                 AND (status_flag IS NULL OR status_flag = 'S'))
       ORDER BY xihs.record_id;
  
    CURSOR get_custom_holds_cur IS
      SELECT hold_lookup_code,
             description      hold_reason,
             hold_type,
             p_new_org_id     new_org_id,
             p_new_invoice_id new_invoice_id,
             p_record_id      record_id
        FROM ap_hold_codes
       WHERE hold_lookup_code = CASE
               WHEN p_receipt_req_flag = 'Y' AND p_is_non_po = 'PO' THEN
                'REMATCH'
               ELSE
                'REVIEW'
             END;
  
    l_status_flag VARCHAR2(1);
  BEGIN
    xxetn_debug_pkg.initialize_debug(pov_err_msg      => l_err_msg,
                                     piv_program_name => 'ETN_AP_INVOICES_CONV_HOLD');
    debug('Invoice Hold Program Starts at: ' ||
          to_char(SYSDATE, 'DD-MON-YYYY HH24:MI:SS'));
    debug('+ Start of Invoice Hold Program + ' || p_batch_id);
    g_new_batch_id := p_batch_id;
  
    IF (p_batch_id IS NULL)
    THEN
      debug('Batch ID cannot be null');
      p_retcode := 2;
    ELSE
    
      FOR val_invoices_hold_rec IN val_invoices_hold_cur
      LOOP
        mo_global.set_policy_context('S', val_invoices_hold_rec.new_org_id);
        l_error_message := NULL;
        /*BEGIN
          fnd_global.apps_initialize(user_id      => g_user_id,
                                     resp_id      => g_resp_id,
                                     resp_appl_id => g_resp_appl_id);
        END;*/
        BEGIN
          l_success := '';
          debug('Invoice Hold Upload Started');
          BEGIN
            mo_global.init('SQLAP');
            mo_global.set_org_context(val_invoices_hold_rec.new_org_id,
                                      ' ',
                                      'S');
            l_calling_sequence := NULL;
            ap_holds_pkg.insert_single_hold(x_invoice_id       => val_invoices_hold_rec.new_invoice_id,
                                            x_hold_lookup_code => val_invoices_hold_rec.hold_lookup_code,
                                            x_hold_type        => val_invoices_hold_rec.hold_type,
                                            x_hold_reason      => val_invoices_hold_rec.hold_reason,
                                            x_held_by          => g_user_id,
                                            x_calling_sequence => l_calling_sequence);
          END;
          l_success := 'Y';
        EXCEPTION
          WHEN OTHERS THEN
            l_error_flag    := g_error;
            l_success       := 'N';
            l_error_message := l_error_message ||
                               'Failed to insert the record into AP_HOLDS_ALL table' ||
                               chr(10);
        END;
      
        IF l_success = 'Y'
        THEN
          UPDATE xxap_invc_holds_conv_stg
             SET status_flag = g_flag_processed
           WHERE record_id = val_invoices_hold_rec.record_id;
          UPDATE xxap_invc_intfc_stg
             SET record_status     = 'SOURCE-HOLD-APPLIED',
                 last_update_date  = SYSDATE,
                 last_updated_by   = g_user_id,
                 last_update_login = g_login_id,
                 request_id        = g_request_id,
                 status_flag       = g_flag_processed
           WHERE record_id = val_invoices_hold_rec.record_id;
        ELSE
          UPDATE xxap_invc_holds_conv_stg
             SET status_flag   = g_flag_error,
                 error_type    = g_err_imp,
                 error_message = 'Error in applying source hold: ' ||
                                 val_invoices_hold_rec.hold_lookup_code
           WHERE record_id = val_invoices_hold_rec.record_id;
          UPDATE xxap_invc_intfc_stg
             SET record_status     = 'SOURCE-HOLD-ERROR',
                 last_update_date  = SYSDATE,
                 last_updated_by   = g_user_id,
                 last_update_login = g_login_id,
                 request_id        = g_request_id,
                 status_flag       = g_flag_error,
                 error_type        = g_err_imp,
                 error_message     = error_message ||
                                     ';Error in applying source hold: ' ||
                                     val_invoices_hold_rec.hold_lookup_code
           WHERE record_id = val_invoices_hold_rec.record_id;
        END IF;
      
      END LOOP;
    
      FOR get_custom_holds_rec IN get_custom_holds_cur
      LOOP
        mo_global.set_policy_context('S', get_custom_holds_rec.new_org_id);
        l_error_message := NULL;
        /*BEGIN
          fnd_global.apps_initialize(user_id      => g_user_id,
                                     resp_id      => g_resp_id,
                                     resp_appl_id => g_resp_appl_id);
        END;*/
        BEGIN
          l_success := '';
          debug('Invoice Hold Upload Started');
          BEGIN
            mo_global.init('SQLAP');
            mo_global.set_org_context(get_custom_holds_rec.new_org_id,
                                      ' ',
                                      'S');
            l_calling_sequence := NULL;
            ap_holds_pkg.insert_single_hold(x_invoice_id       => get_custom_holds_rec.new_invoice_id,
                                            x_hold_lookup_code => get_custom_holds_rec.hold_lookup_code,
                                            x_hold_type        => get_custom_holds_rec.hold_type,
                                            x_hold_reason      => get_custom_holds_rec.hold_reason,
                                            x_held_by          => g_user_id,
                                            x_calling_sequence => l_calling_sequence);
          END;
          l_success := 'Y';
        EXCEPTION
          WHEN OTHERS THEN
            l_error_flag    := g_error;
            l_success       := 'N';
            l_error_message := l_error_message ||
                               'Failed to insert the record into AP_HOLDS_ALL table' ||
                               chr(10);
        END;
      
        IF l_success = 'Y'
        THEN
        
          IF p_is_invc_cancelled = 'Y'
          THEN
            l_status_flag := g_flag_completed;
          ELSE
            l_status_flag := g_flag_processed;
          END IF;
        
          UPDATE xxap_invc_holds_conv_stg
             SET status_flag = l_status_flag
           WHERE record_id = get_custom_holds_rec.record_id;
          UPDATE xxap_invc_lines_intfc_stg
             SET status_flag = l_status_flag
           WHERE record_id = get_custom_holds_rec.record_id;
          UPDATE xxap_invc_intfc_stg
             SET record_status     = 'CUSTOM-HOLD-APPLIED',
                 last_update_date  = SYSDATE,
                 last_updated_by   = g_user_id,
                 last_update_login = g_login_id,
                 request_id        = g_request_id,
                 status_flag       = l_status_flag
           WHERE record_id = get_custom_holds_rec.record_id;
        ELSE
          UPDATE xxap_invc_holds_conv_stg
             SET status_flag       = g_flag_error,
                 last_update_date  = SYSDATE,
                 last_updated_by   = g_user_id,
                 last_update_login = g_login_id,
                 request_id        = g_request_id,
                 error_type        = g_err_imp,
                 error_message     = 'Error in applying custom hold: ' ||
                                     get_custom_holds_rec.hold_lookup_code
           WHERE record_id = get_custom_holds_rec.record_id;
          UPDATE xxap_invc_intfc_stg
             SET record_status     = 'CUSTOM-HOLD-ERROR',
                 last_update_date  = SYSDATE,
                 last_updated_by   = g_user_id,
                 last_update_login = g_login_id,
                 request_id        = g_request_id,
                 status_flag       = g_flag_error,
                 error_type        = g_err_imp,
                 error_message     = error_message ||
                                     ';Error in applying custom hold: ' ||
                                     get_custom_holds_rec.hold_lookup_code
           WHERE record_id = get_custom_holds_rec.record_id;
        END IF;
      
      END LOOP;
    
    END IF;
  EXCEPTION
    WHEN OTHERS THEN
      p_errbuf  := substr('Exception in Procedure hold_invoice. ' ||
                          SQLERRM,
                          1,
                          240);
      p_retcode := g_error;
      debug(p_errbuf);
  END hold_invoice;

  -- =============================================================================
  -- Procedure: validate_data
  -- =============================================================================
  --   To Validate Project Data
  -- =============================================================================
  --  Input Parameters :
  --    No Input Parameters
  --  Output Parameters :
  --    p_retcode   : Program Return Code = 0/1/2
  --    p_errbuf    : Error message in case of any failure
  -- -----------------------------------------------------------------------------
  PROCEDURE validate_data(p_errbuf  OUT VARCHAR2,
                          p_retcode OUT NUMBER) IS
    l_error_tab_type xxetn_common_error_pkg.g_source_tab_type;
    l_error_code CONSTANT VARCHAR2(25) := 'ETN_AP_INVOICE_VALIDATION';
    l_error_count            NUMBER := 0;
    l_error_lines_count      NUMBER := 0;
    l_error_tab_count        NUMBER := 0;
    l_error_message          VARCHAR2(2000);
    l_log_ret_sts            VARCHAR2(2000);
    l_log_err_msg            VARCHAR2(2000);
    l_errbuf                 VARCHAR2(2000) := NULL;
    l_retcode                NUMBER := NULL;
    l_loop_count             NUMBER := 0;
    l_line_loop_count        NUMBER := 0;
    l_org_template_null_flag VARCHAR2(1);
    l_total_err_msg          VARCHAR2(3000) := NULL;
    l_total_lines_err_msg    VARCHAR2(3000) := NULL;
    /*Supplier Variables*/
    l_new_vendor_site_id xxetn.xxap_supplier_site_res_stg.new_vendor_site_id%TYPE;
    l_vendor_name        ap_suppliers.vendor_name%TYPE;
    l_vendor_number      ap_suppliers.segment1%TYPE;
    l_vendor_site_code   ap_supplier_sites_all.vendor_site_code%TYPE;
    /*Payment Terms*/
    l_terms_name ap_terms.name%TYPE;
    /*Payment Method*/
    l_payment_method_code iby_payment_methods_tl.payment_method_code%TYPE;
    /*Currency Code*/
    l_currency_code fnd_currencies.currency_code%TYPE;
    /*Party ID and Party Site ID*/
    l_party_id      hz_parties.party_id%TYPE;
    l_party_site_id hz_party_sites.party_site_id%TYPE;
    /*Tax Variables*/
    l_tax                   zx_rates_b.tax%TYPE;
    l_tax_regime_code       zx_rates_b.tax_regime_code%TYPE;
    l_tax_rate_code         zx_rates_b.tax_rate_code%TYPE;
    l_tax_status_code       zx_rates_b.tax_status_code%TYPE;
    l_tax_jurisdiction_code zx_rates_b.tax_jurisdiction_code%TYPE;
    l_tax_rate_id           zx_rates_b.tax_rate_id%TYPE;
    l_percentage_rate       zx_rates_b.percentage_rate%TYPE;
    /*Project Variables*/
    l_new_project_id pa_projects_all.project_id%TYPE;
    /*Task Variables*/
    l_new_task_id pa_tasks.task_id%TYPE;
    /*Expenditure Variables*/
    l_expenditure_org_id    pa_expenditures_all.incurred_by_organization_id%TYPE;
    l_expenditure_item_date pa_expenditure_items_all.expenditure_item_date%TYPE;
    l_expenditure_type      pa_expenditure_items_all.expenditure_type%TYPE;
    /*AP Period*/
    l_set_of_books_id gl_period_statuses.set_of_books_id%TYPE;
    /*Payment Schedules Variables*/
    l_payment_priority ap_payment_schedules_all.payment_priority%TYPE;
    l_due_date         ap_payment_schedules_all.due_date%TYPE;
    /*External Bank Account ID*/
    l_ext_bank_account_id iby_ext_bank_accounts.ext_bank_account_id%TYPE;
    /*Global Attribute Category at Invoice Header*/
    l_glb_attr_cat ap_invoices_all.global_attribute_category%TYPE;
    /*Distribution Code Combination Segments and ID Variables*/
    lv_dist_concat_segment      VARCHAR2(750);
    ln_dist_code_comb_id        NUMBER;
    lv_new_dist_concat_segment  VARCHAR2(750);
    lv_new_dist_concat_flip     VARCHAR2(750);
    ln_dist_chart_of_account_id NUMBER;
    lv_dist_error_message       VARCHAR2(1000);
  
    /* Cursor to fetch records from Staging table eligible for validation */
    CURSOR invoices_cur IS
      SELECT xis.*
        FROM xxap_invc_intfc_stg xis
       WHERE xis.batch_id = g_loader_request_id
         AND xis.status_flag IN ('N', 'E')
         AND xis.operation = 'COPY'
         AND xis.process_flag = g_flag_yes
       ORDER BY record_id;
  
    CURSOR get_invc_sob_cur(p_org_id IN hr_operating_units.organization_id%TYPE) IS
      SELECT hou.set_of_books_id
        FROM hr_operating_units hou
       WHERE hou.organization_id = p_org_id;
  
    CURSOR invoice_lines_cur(p_record_id IN NUMBER) IS
      SELECT xils.*
        FROM xxap_invc_lines_intfc_stg xils
       WHERE xils.record_id = p_record_id;
  
    l_conv_account fnd_profile_option_values.profile_option_value%TYPE;
  
    CURSOR get_conv_account_cur IS
      SELECT fpov.profile_option_value
        FROM fnd_profile_options       fpo,
             fnd_profile_option_values fpov
       WHERE fpo.profile_option_id = fpov.profile_option_id
         AND fpo.profile_option_name = 'XXETN_CONV_ACC_COMBINATION';
  
    l_sum_prepay_amt NUMBER := 0;
  
    CURSOR get_dist_prepay_amt_cur(p_invoice_id IN ap_invoice_distributions_all.invoice_id%TYPE) IS
      SELECT nvl(abs(SUM(amount)), 0) sum_amount
        FROM ap_invoice_distributions_all aida
       WHERE aida.invoice_id = p_invoice_id
         AND aida.line_type_lookup_code = 'PREPAY';
  
    CURSOR flip_segment6_cur(p_code_combination_id IN NUMBER,
                             p_source_le           IN VARCHAR2) IS
      SELECT 1
        FROM gl_code_combinations
       WHERE code_combination_id = p_code_combination_id
         AND segment6 = p_source_le;
  
    TYPE invoices_cur_t IS TABLE OF invoices_cur%ROWTYPE INDEX BY BINARY_INTEGER;
  
    l_invoices_cur_t invoices_cur_t;
  
    TYPE invoice_lines_cur_t IS TABLE OF invoice_lines_cur%ROWTYPE INDEX BY BINARY_INTEGER;
  
    l_invoice_lines_cur_t invoice_lines_cur_t;
    l_mandatory_err_msg   VARCHAR2(400) := 'Error: Mandatory Value missing on record.';
  BEGIN
    l_invoices_cur_t.delete;
    l_invoice_lines_cur_t.delete;
    debug('Call Procedure derive_project_template - Start');
  
    FOR invoices_stg_rec IN invoices_cur
    LOOP
      debug('Invoice Number: ' || invoices_stg_rec.invoice_num);
      l_errbuf                 := NULL;
      l_retcode                := g_normal;
      l_total_err_msg          := NULL;
      l_org_template_null_flag := 'N';
      l_error_count            := 0;
      l_loop_count             := l_loop_count + 1;
      /* Marking record as validated */
      l_invoices_cur_t(l_loop_count).record_id := invoices_stg_rec.record_id;
      l_invoices_cur_t(l_loop_count).status_flag := g_flag_validated;
      l_invoices_cur_t(l_loop_count).record_status := 'VALIDATED';
      l_invoices_cur_t(l_loop_count).error_type := NULL;
      l_invoices_cur_t(l_loop_count).error_message := NULL;
      l_invoices_cur_t(l_loop_count).new_vendor_site_id := NULL;
      l_invoices_cur_t(l_loop_count).vendor_name := NULL;
      l_invoices_cur_t(l_loop_count).vendor_num := NULL;
      l_invoices_cur_t(l_loop_count).vendor_site_code := NULL;
      l_invoices_cur_t(l_loop_count).terms_name := NULL;
      l_invoices_cur_t(l_loop_count).new_party_id := NULL;
      l_invoices_cur_t(l_loop_count).new_party_site_id := NULL;
      l_invoices_cur_t(l_loop_count).external_bank_account_id := NULL;
      l_invoices_cur_t(l_loop_count).global_attribute_category := NULL;
      l_errbuf := NULL;
      l_retcode := g_normal;
      validate_duplicate_invoices(l_errbuf,
                                  l_retcode,
                                  invoices_stg_rec.old_invoice_id,
                                  invoices_stg_rec.org_id,
                                  invoices_stg_rec.source_le,
                                  invoices_stg_rec.source_plant,
                                  invoices_stg_rec.batch_id,
                                  invoices_stg_rec.record_id);
    
      IF l_retcode <> g_normal
      THEN
        l_error_count := l_error_count + 1;
        l_error_tab_count := l_error_tab_count + 1;
        l_error_message := substr(l_errbuf, 1, 2000);
        l_total_err_msg := l_error_message;
        l_error_tab_type(l_error_tab_count).source_table := g_source_table;
        l_error_tab_type(l_error_tab_count).interface_staging_id := invoices_stg_rec.record_id;
        l_error_tab_type(l_error_tab_count).error_type := g_err_val;
        l_error_tab_type(l_error_tab_count).error_code := l_error_code;
        l_error_tab_type(l_error_tab_count).error_message := l_error_message;
        l_error_tab_type(l_error_tab_count).source_column_name := 'OLD_INVOICE_ID';
        l_error_tab_type(l_error_tab_count).source_column_value := invoices_stg_rec.old_invoice_id;
      END IF;
    
      debug('validate_duplicate_invoices. l_error_count ' || l_error_count ||
            ' l_retcode ' || l_retcode || ' l_errbuf ' || l_errbuf);
      --------------------------------------------
      l_errbuf  := NULL;
      l_retcode := g_normal;
    
      IF invoices_stg_rec.is_partially_paid = 'Y'
      THEN
        l_sum_prepay_amt := 0;
      
        FOR get_dist_prepay_amt_rec IN get_dist_prepay_amt_cur(invoices_stg_rec.old_invoice_id)
        LOOP
          l_sum_prepay_amt := get_dist_prepay_amt_rec.sum_amount;
        END LOOP;
      
        IF l_sum_prepay_amt <> invoices_stg_rec.amt_partially_paid
        THEN
          l_errbuf  := 'Partially Paid Invoices to be handled manually';
          l_retcode := g_error;
        END IF;
      
      END IF;
    
      IF l_retcode <> g_normal
      THEN
        l_error_count := l_error_count + 1;
        l_error_tab_count := l_error_tab_count + 1;
        l_error_message := substr(l_errbuf, 1, 2000);
        l_total_err_msg := l_total_err_msg || ';' || l_error_message;
        l_error_tab_type(l_error_tab_count).source_table := g_source_table;
        l_error_tab_type(l_error_tab_count).interface_staging_id := invoices_stg_rec.record_id;
        l_error_tab_type(l_error_tab_count).error_type := g_err_val;
        --
        l_error_tab_type(l_error_tab_count).error_code := l_error_code;
        l_error_tab_type(l_error_tab_count).error_message := l_error_message;
        l_error_tab_type(l_error_tab_count).source_column_name := 'INVOICE_NUM';
        l_error_tab_type(l_error_tab_count).source_column_value := invoices_stg_rec.invoice_num;
      END IF;
      --------------------------------------------
      l_errbuf  := NULL;
      l_retcode := g_normal;
    
      IF invoices_stg_rec.prepay_amt_applied > 0 AND
         invoices_stg_rec.invoice_type_lookup_code = 'PREPAYMENT'
      THEN
        l_errbuf  := 'Partially Applied Prepayment Invoices to be handled manually';
        l_retcode := g_error;
      END IF;
    
      IF l_retcode <> g_normal
      THEN
        l_error_count := l_error_count + 1;
        l_error_tab_count := l_error_tab_count + 1;
        l_error_message := substr(l_errbuf, 1, 2000);
        l_total_err_msg := l_total_err_msg || ';' || l_error_message;
        l_error_tab_type(l_error_tab_count).source_table := g_source_table;
        l_error_tab_type(l_error_tab_count).interface_staging_id := invoices_stg_rec.record_id;
        l_error_tab_type(l_error_tab_count).error_type := g_err_val;
        --
        l_error_tab_type(l_error_tab_count).error_code := l_error_code;
        l_error_tab_type(l_error_tab_count).error_message := l_error_message;
        l_error_tab_type(l_error_tab_count).source_column_name := 'INVOICE_NUM';
        l_error_tab_type(l_error_tab_count).source_column_value := invoices_stg_rec.invoice_num;
      END IF;
    
      --------------------------------------------
      /*l_errbuf  := NULL;
        l_retcode := g_normal;
      
        IF invoices_stg_rec.invoice_type_lookup_code = 'PREPAYMENT'
        THEN
          l_errbuf  := 'Prepayment Invoices to be handled manually';
          l_retcode := g_error;
        END IF;
      
        IF l_retcode <> g_normal
        THEN
          l_error_count := l_error_count + 1;
          l_error_tab_count := l_error_tab_count + 1;
          l_error_message := substr(l_errbuf, 1, 2000);
          l_total_err_msg := l_total_err_msg || ';' || l_error_message;
          l_error_tab_type(l_error_tab_count).source_table := g_source_table;
          l_error_tab_type(l_error_tab_count).interface_staging_id := invoices_stg_rec.record_id;
          l_error_tab_type(l_error_tab_count).error_type := g_err_val;
          --
          l_error_tab_type(l_error_tab_count).error_code := l_error_code;
          l_error_tab_type(l_error_tab_count).error_message := l_error_message;
          l_error_tab_type(l_error_tab_count).source_column_name := 'INVOICE_TYPE_LOOKUP_CODE';
          l_error_tab_type(l_error_tab_count).source_column_value := invoices_stg_rec.invoice_type_lookup_code;
        END IF;
      */
      --------------------------------------------
      l_set_of_books_id := NULL;
    
      FOR get_invc_sob_rec IN get_invc_sob_cur(invoices_stg_rec.new_org_id)
      LOOP
        l_set_of_books_id := get_invc_sob_rec.set_of_books_id;
      END LOOP;
    
      l_errbuf  := NULL;
      l_retcode := g_normal;
      /*validate_gl_open_period(p_set_of_books_id => l_set_of_books_id,
      p_gl_date         => nvl(g_accounting_date,
                               invoices_stg_rec.gl_date),
      p_errbuf          => l_errbuf,
      p_retcode         => l_retcode);*/
    
      IF l_retcode <> g_normal
      THEN
        l_error_count := l_error_count + 1;
        l_error_tab_count := l_error_tab_count + 1;
        l_error_message := substr(l_errbuf, 1, 2000);
        l_total_err_msg := l_total_err_msg || ';' || l_error_message;
        l_error_tab_type(l_error_tab_count).source_table := g_source_table;
        l_error_tab_type(l_error_tab_count).interface_staging_id := invoices_stg_rec.record_id;
        l_error_tab_type(l_error_tab_count).error_type := g_err_val;
        --
        l_error_tab_type(l_error_tab_count).error_code := l_error_code;
        l_error_tab_type(l_error_tab_count).error_message := l_error_message;
        l_error_tab_type(l_error_tab_count).source_column_name := 'GL_DATE';
        l_error_tab_type(l_error_tab_count).source_column_value := nvl(g_accounting_date,
                                                                       invoices_stg_rec.gl_date);
      END IF;
    
      --------------------------------------------
      l_errbuf  := NULL;
      l_retcode := g_normal;
      validate_ap_open_period(p_set_of_books_id => l_set_of_books_id,
                              p_gl_date         => nvl(g_accounting_date,
                                                       invoices_stg_rec.gl_date),
                              p_errbuf          => l_errbuf,
                              p_retcode         => l_retcode);
    
      IF l_retcode <> g_normal
      THEN
        l_error_count := l_error_count + 1;
        l_error_tab_count := l_error_tab_count + 1;
        l_error_message := substr(l_errbuf, 1, 2000);
        l_total_err_msg := l_total_err_msg || ';' || l_error_message;
        l_error_tab_type(l_error_tab_count).source_table := g_source_table;
        l_error_tab_type(l_error_tab_count).interface_staging_id := invoices_stg_rec.record_id;
        l_error_tab_type(l_error_tab_count).error_type := g_err_val;
        --
        l_error_tab_type(l_error_tab_count).error_code := l_error_code;
        l_error_tab_type(l_error_tab_count).error_message := l_error_message;
        l_error_tab_type(l_error_tab_count).source_column_name := 'GL_DATE';
        l_error_tab_type(l_error_tab_count).source_column_value := nvl(g_accounting_date,
                                                                       invoices_stg_rec.gl_date);
      END IF;
    
      --------------------------------------------
      IF invoices_stg_rec.invoice_num IS NULL OR
         invoices_stg_rec.new_invoice_num IS NULL
      THEN
        l_error_message := l_mandatory_err_msg;
        l_error_count := l_error_count + 1;
        l_error_tab_count := l_error_tab_count + 1;
        l_total_err_msg := l_total_err_msg || ';' || l_error_message;
        l_error_tab_type(l_error_tab_count).source_table := g_source_table;
        l_error_tab_type(l_error_tab_count).interface_staging_id := invoices_stg_rec.record_id;
        l_error_tab_type(l_error_tab_count).error_type := g_err_val;
        --
        l_error_tab_type(l_error_tab_count).error_code := l_error_code;
        l_error_tab_type(l_error_tab_count).error_message := l_error_message;
        l_error_tab_type(l_error_tab_count).source_column_name := 'INVOICE_NUM';
        l_error_tab_type(l_error_tab_count).source_column_value := invoices_stg_rec.invoice_num;
      END IF;
    
      debug('Mandatory Check for Invoice Number. l_error_count ' ||
            l_error_count || ' l_retcode ' || l_retcode || ' l_errbuf ' ||
            l_errbuf);
    
      --------------------------------------------
      IF invoices_stg_rec.invoice_amount IS NULL
      THEN
        l_error_message := l_mandatory_err_msg;
        l_error_count := l_error_count + 1;
        l_error_tab_count := l_error_tab_count + 1;
        l_total_err_msg := l_total_err_msg || ';' || l_error_message;
        l_error_tab_type(l_error_tab_count).source_table := g_source_table;
        l_error_tab_type(l_error_tab_count).interface_staging_id := invoices_stg_rec.record_id;
        l_error_tab_type(l_error_tab_count).error_type := g_err_val;
        --
        l_error_tab_type(l_error_tab_count).error_code := l_error_code;
        l_error_tab_type(l_error_tab_count).error_message := l_error_message;
        l_error_tab_type(l_error_tab_count).source_column_name := 'INVOICE_AMOUNT';
        l_error_tab_type(l_error_tab_count).source_column_value := invoices_stg_rec.invoice_amount;
      END IF;
    
      debug('Mandatory Check for Invoice Amount. l_error_count ' ||
            l_error_count || ' l_retcode ' || l_retcode || ' l_errbuf ' ||
            l_errbuf);
    
      --------------------------------------------
      IF invoices_stg_rec.invoice_type_lookup_code IS NULL
      THEN
        l_error_message := l_mandatory_err_msg;
        l_error_count := l_error_count + 1;
        l_error_tab_count := l_error_tab_count + 1;
        l_total_err_msg := l_total_err_msg || ';' || l_error_message;
        l_error_tab_type(l_error_tab_count).source_table := g_source_table;
        l_error_tab_type(l_error_tab_count).interface_staging_id := invoices_stg_rec.record_id;
        l_error_tab_type(l_error_tab_count).error_type := g_err_val;
        --
        l_error_tab_type(l_error_tab_count).error_code := l_error_code;
        l_error_tab_type(l_error_tab_count).error_message := l_error_message;
        l_error_tab_type(l_error_tab_count).source_column_name := 'INVOICE_TYPE_LOOKUP_CODE';
        l_error_tab_type(l_error_tab_count).source_column_value := invoices_stg_rec.invoice_type_lookup_code;
      END IF;
    
      debug('Mandatory Check for Invoice Amount. l_error_count ' ||
            l_error_count || ' l_retcode ' || l_retcode || ' l_errbuf ' ||
            l_errbuf);
    
      --------------------------------------------
      IF invoices_stg_rec.invoice_date IS NULL
      THEN
        l_error_message := l_mandatory_err_msg;
        l_error_count := l_error_count + 1;
        l_error_tab_count := l_error_tab_count + 1;
        l_total_err_msg := l_total_err_msg || ';' || l_error_message;
        l_error_tab_type(l_error_tab_count).source_table := g_source_table;
        l_error_tab_type(l_error_tab_count).interface_staging_id := invoices_stg_rec.record_id;
        l_error_tab_type(l_error_tab_count).error_type := g_err_val;
        --
        l_error_tab_type(l_error_tab_count).error_code := l_error_code;
        l_error_tab_type(l_error_tab_count).error_message := l_error_message;
        l_error_tab_type(l_error_tab_count).source_column_name := 'INVOICE_DATE';
        l_error_tab_type(l_error_tab_count).source_column_value := invoices_stg_rec.invoice_date;
      END IF;
    
      debug('Mandatory Check for Invoice Date. l_error_count ' ||
            l_error_count || ' l_retcode ' || l_retcode || ' l_errbuf ' ||
            l_errbuf);
    
      --------------------------------------------
      IF invoices_stg_rec.operating_unit IS NULL
      THEN
        l_error_message := l_mandatory_err_msg;
        l_error_count := l_error_count + 1;
        l_error_tab_count := l_error_tab_count + 1;
        l_total_err_msg := l_total_err_msg || ';' || l_error_message;
        l_error_tab_type(l_error_tab_count).source_table := g_source_table;
        l_error_tab_type(l_error_tab_count).interface_staging_id := invoices_stg_rec.record_id;
        l_error_tab_type(l_error_tab_count).error_type := g_err_val;
        --
        l_error_tab_type(l_error_tab_count).error_code := l_error_code;
        l_error_tab_type(l_error_tab_count).error_message := l_error_message;
        l_error_tab_type(l_error_tab_count).source_column_name := 'OPERATING_UNIT';
        l_error_tab_type(l_error_tab_count).source_column_value := invoices_stg_rec.operating_unit;
      END IF;
    
      debug('Mandatory Check for Operating Unit. l_error_count ' ||
            l_error_count || ' l_retcode ' || l_retcode || ' l_errbuf ' ||
            l_errbuf);
      --------------------------------------------
      l_errbuf  := NULL;
      l_retcode := g_normal;
      is_invc_cancellable(p_invoice_id => invoices_stg_rec.old_invoice_id,
                          p_org_id     => invoices_stg_rec.org_id,
                          p_errbuf     => l_errbuf,
                          p_retcode    => l_retcode);
    
      IF l_retcode <> g_normal AND
         l_errbuf != 'This invoice has an effective payment.' AND
         invoices_stg_rec.is_invc_cancelled <> 'Y'
      THEN
        l_error_count := l_error_count + 1;
        l_error_tab_count := l_error_tab_count + 1;
        l_error_message := substr(l_errbuf, 1, 2000);
        l_total_err_msg := l_total_err_msg || ';' || l_error_message;
        l_error_tab_type(l_error_tab_count).source_table := g_source_table;
        l_error_tab_type(l_error_tab_count).interface_staging_id := invoices_stg_rec.record_id;
        l_error_tab_type(l_error_tab_count).error_type := g_err_val;
        l_error_tab_type(l_error_tab_count).error_code := l_error_code;
        l_error_tab_type(l_error_tab_count).error_message := l_error_message;
        l_error_tab_type(l_error_tab_count).source_column_name := 'IS_INVC_CANCELLED';
        l_error_tab_type(l_error_tab_count).source_column_value := invoices_stg_rec.is_invc_cancelled;
      END IF;
    
      debug('Check if Invoice can be cancelled at Source. l_error_count ' ||
            l_error_count || ' l_retcode ' || l_retcode || ' l_errbuf ' ||
            l_errbuf);
      --------------------------------------------
      l_payment_priority := NULL;
      l_due_date         := NULL;
      l_errbuf           := NULL;
      l_retcode          := g_normal;
    
      IF invoices_stg_rec.invoice_type_lookup_code <> 'PREPAYMENT' AND
         invoices_stg_rec.is_invc_cancelled <> 'Y'
      THEN
        validate_pay_sch(p_old_invoice_id   => invoices_stg_rec.old_invoice_id,
                         p_old_org_id       => invoices_stg_rec.org_id,
                         p_payment_priority => l_payment_priority,
                         p_due_date         => l_due_date,
                         p_errbuf           => l_errbuf,
                         p_retcode          => l_retcode);
      
        IF l_retcode <> g_normal
        THEN
          l_error_count := l_error_count + 1;
          l_error_tab_count := l_error_tab_count + 1;
          l_error_message := substr(l_errbuf, 1, 2000);
          l_total_err_msg := l_total_err_msg || ';' || l_error_message;
          l_error_tab_type(l_error_tab_count).source_table := g_source_table;
          l_error_tab_type(l_error_tab_count).interface_staging_id := invoices_stg_rec.record_id;
          l_error_tab_type(l_error_tab_count).error_type := g_err_val;
          l_error_tab_type(l_error_tab_count).error_code := l_error_code;
          l_error_tab_type(l_error_tab_count).error_message := l_error_message;
          l_error_tab_type(l_error_tab_count).source_column_name := 'PAYMENT_PRIORITY';
          l_error_tab_type(l_error_tab_count).source_column_value := invoices_stg_rec.payment_priority;
        END IF;
      END IF;
      debug('Check Payment Schedules. l_error_count ' || l_error_count ||
            ' l_retcode ' || l_retcode || ' l_errbuf ' || l_errbuf);
      --------------------------------------------
      l_new_vendor_site_id := NULL;
      l_vendor_name        := NULL;
      l_vendor_number      := NULL;
      l_vendor_site_code   := NULL;
      l_errbuf             := NULL;
      l_retcode            := g_normal;
      validate_conv_vendor_sites(p_vendor_id          => invoices_stg_rec.vendor_id,
                                 p_old_vendor_site_id => invoices_stg_rec.old_vendor_site_id,
                                 p_source_org_id      => invoices_stg_rec.org_id,
                                 p_target_org_id      => invoices_stg_rec.new_org_id,
                                 p_new_vendor_site_id => l_new_vendor_site_id,
                                 p_vendor_name        => l_vendor_name,
                                 p_vendor_number      => l_vendor_number,
                                 p_vendor_site_code   => l_vendor_site_code,
                                 p_errbuf             => l_errbuf,
                                 p_retcode            => l_retcode);
    
      IF l_retcode <> g_normal
      THEN
        l_error_count := l_error_count + 1;
        l_error_tab_count := l_error_tab_count + 1;
        l_error_message := substr(l_errbuf, 1, 2000);
        l_total_err_msg := l_total_err_msg || ';' || l_error_message;
        l_error_tab_type(l_error_tab_count).source_table := g_source_table;
        l_error_tab_type(l_error_tab_count).interface_staging_id := invoices_stg_rec.record_id;
        l_error_tab_type(l_error_tab_count).error_type := g_err_val;
        l_error_tab_type(l_error_tab_count).error_code := l_error_code;
        l_error_tab_type(l_error_tab_count).error_message := l_error_message;
        l_error_tab_type(l_error_tab_count).source_column_name := 'NEW_VENDOR_SITE_ID';
        l_error_tab_type(l_error_tab_count).source_column_value := invoices_stg_rec.new_vendor_site_id;
      ELSE
        l_invoices_cur_t(l_loop_count).new_vendor_site_id := l_new_vendor_site_id;
        l_invoices_cur_t(l_loop_count).vendor_name := l_vendor_name;
        l_invoices_cur_t(l_loop_count).vendor_num := l_vendor_number;
        l_invoices_cur_t(l_loop_count).vendor_site_code := l_vendor_site_code;
      END IF;
    
      debug('Check Vendor Site ID. l_error_count ' || l_error_count ||
            ' l_retcode ' || l_retcode || ' l_errbuf ' || l_errbuf);
      --------------------------------------------
      l_errbuf     := NULL;
      l_retcode    := g_normal;
      l_terms_name := NULL;
      validate_pay_terms(p_terms_id   => invoices_stg_rec.terms_id,
                         p_terms_name => l_terms_name,
                         p_errbuf     => l_errbuf,
                         p_retcode    => l_retcode);
    
      IF l_retcode <> g_normal
      THEN
        l_error_count := l_error_count + 1;
        l_error_tab_count := l_error_tab_count + 1;
        l_error_message := substr(l_errbuf, 1, 2000);
        l_total_err_msg := l_total_err_msg || ';' || l_error_message;
        l_error_tab_type(l_error_tab_count).source_table := g_source_table;
        l_error_tab_type(l_error_tab_count).interface_staging_id := invoices_stg_rec.record_id;
        l_error_tab_type(l_error_tab_count).error_type := g_err_val;
        l_error_tab_type(l_error_tab_count).error_code := l_error_code;
        l_error_tab_type(l_error_tab_count).error_message := l_error_message;
        l_error_tab_type(l_error_tab_count).source_column_name := 'TERMS_NAME';
        l_error_tab_type(l_error_tab_count).source_column_value := invoices_stg_rec.terms_name;
      ELSE
        l_invoices_cur_t(l_loop_count).terms_name := l_terms_name;
      END IF;
    
      debug('Check Payment Terms. l_error_count ' || l_error_count ||
            ' l_retcode ' || l_retcode || ' l_errbuf ' || l_errbuf);
      --------------------------------------------
      l_errbuf              := NULL;
      l_retcode             := g_normal;
      l_payment_method_code := NULL;
      l_payment_method_code := invoices_stg_rec.payment_method_code;
      validate_pay_method(p_payment_method_code => l_payment_method_code,
                          p_errbuf              => l_errbuf,
                          p_retcode             => l_retcode);
    
      IF l_retcode <> g_normal
      THEN
        l_error_count := l_error_count + 1;
        l_error_tab_count := l_error_tab_count + 1;
        l_error_message := substr(l_errbuf, 1, 2000);
        l_total_err_msg := l_total_err_msg || ';' || l_error_message;
        l_error_tab_type(l_error_tab_count).source_table := g_source_table;
        l_error_tab_type(l_error_tab_count).interface_staging_id := invoices_stg_rec.record_id;
        l_error_tab_type(l_error_tab_count).error_type := g_err_val;
        l_error_tab_type(l_error_tab_count).error_code := l_error_code;
        l_error_tab_type(l_error_tab_count).error_message := l_error_message;
        l_error_tab_type(l_error_tab_count).source_column_name := 'PAYMENT_METHOD_CODE';
        l_error_tab_type(l_error_tab_count).source_column_value := invoices_stg_rec.payment_method_code;
      END IF;
    
      debug('Check Payment Method Code. l_error_count ' || l_error_count ||
            ' l_retcode ' || l_retcode || ' l_errbuf ' || l_errbuf);
      --------------------------------------------
      l_errbuf        := NULL;
      l_retcode       := g_normal;
      l_currency_code := NULL;
      l_currency_code := invoices_stg_rec.invoice_currency_code;
      validate_currency_code(p_currency_code => l_currency_code,
                             p_errbuf        => l_errbuf,
                             p_retcode       => l_retcode);
    
      IF l_retcode <> g_normal
      THEN
        l_error_count := l_error_count + 1;
        l_error_tab_count := l_error_tab_count + 1;
        l_error_message := substr(l_errbuf, 1, 2000);
        l_total_err_msg := l_total_err_msg || ';' || l_error_message;
        l_error_tab_type(l_error_tab_count).source_table := g_source_table;
        l_error_tab_type(l_error_tab_count).interface_staging_id := invoices_stg_rec.record_id;
        l_error_tab_type(l_error_tab_count).error_type := g_err_val;
        l_error_tab_type(l_error_tab_count).error_code := l_error_code;
        l_error_tab_type(l_error_tab_count).error_message := l_error_message;
        l_error_tab_type(l_error_tab_count).source_column_name := 'CURRENCY_CODE';
        l_error_tab_type(l_error_tab_count).source_column_value := invoices_stg_rec.invoice_currency_code;
      END IF;
    
      debug('Check Currency Code. l_error_count ' || l_error_count ||
            ' l_retcode ' || l_retcode || ' l_errbuf ' || l_errbuf);
      --------------------------------------------
      l_party_id      := NULL;
      l_party_site_id := NULL;
    
      IF l_new_vendor_site_id IS NOT NULL
      THEN
        l_errbuf  := NULL;
        l_retcode := g_normal;
        validate_party_sites(p_vendor_id          => invoices_stg_rec.vendor_id,
                             p_new_vendor_site_id => l_new_vendor_site_id,
                             p_party_id           => l_party_id,
                             p_party_site_id      => l_party_site_id,
                             p_errbuf             => l_errbuf,
                             p_retcode            => l_retcode);
      
        IF l_retcode <> g_normal
        THEN
          l_error_count := l_error_count + 1;
          l_error_tab_count := l_error_tab_count + 1;
          l_error_message := substr(l_errbuf, 1, 2000);
          l_total_err_msg := l_total_err_msg || ';' || l_error_message;
          l_error_tab_type(l_error_tab_count).source_table := g_source_table;
          l_error_tab_type(l_error_tab_count).interface_staging_id := invoices_stg_rec.record_id;
          l_error_tab_type(l_error_tab_count).error_type := g_err_val;
          l_error_tab_type(l_error_tab_count).error_code := l_error_code;
          l_error_tab_type(l_error_tab_count).error_message := l_error_message;
          l_error_tab_type(l_error_tab_count).source_column_name := 'NEW_PARTY_ID';
          l_error_tab_type(l_error_tab_count).source_column_value := invoices_stg_rec.new_party_id;
        ELSE
          l_invoices_cur_t(l_loop_count).new_party_id := l_party_id;
          l_invoices_cur_t(l_loop_count).new_party_site_id := l_party_site_id;
        END IF;
      
        debug('Check Party ID. l_error_count ' || l_error_count ||
              ' l_retcode ' || l_retcode || ' l_errbuf ' || l_errbuf);
      END IF;
    
      --------------------------------------------
      l_ext_bank_account_id := NULL;
    
      IF invoices_stg_rec.external_bank_account_id IS NOT NULL
      THEN
        l_errbuf              := NULL;
        l_retcode             := g_normal;
        l_ext_bank_account_id := invoices_stg_rec.external_bank_account_id;
        validate_ext_bank_acc_id(p_ext_bank_account_id => l_ext_bank_account_id,
                                 p_errbuf              => l_errbuf,
                                 p_retcode             => l_retcode);
      
        IF l_retcode <> g_normal
        THEN
          l_error_count := l_error_count + 1;
          l_error_tab_count := l_error_tab_count + 1;
          l_error_message := substr(l_errbuf, 1, 2000);
          l_total_err_msg := l_total_err_msg || ';' || l_error_message;
          l_error_tab_type(l_error_tab_count).source_table := g_source_table;
          l_error_tab_type(l_error_tab_count).interface_staging_id := invoices_stg_rec.record_id;
          l_error_tab_type(l_error_tab_count).error_type := g_err_val;
          l_error_tab_type(l_error_tab_count).error_code := l_error_code;
          l_error_tab_type(l_error_tab_count).error_message := l_error_message;
          l_error_tab_type(l_error_tab_count).source_column_name := 'EXTERNAL_BANK_ACCOUNT_ID';
          l_error_tab_type(l_error_tab_count).source_column_value := invoices_stg_rec.external_bank_account_id;
        ELSE
          l_invoices_cur_t(l_loop_count).external_bank_account_id := l_ext_bank_account_id;
        END IF;
      
        debug('Check External Bank Account ID. l_error_count ' ||
              l_error_count || ' l_retcode ' || l_retcode || ' l_errbuf ' ||
              l_errbuf);
      END IF;
    
      --------------------------------------------
      l_glb_attr_cat := NULL;
    
      l_invoices_cur_t(l_loop_count).global_attribute_category := invoices_stg_rec.global_attribute_category;
    
      IF invoices_stg_rec.global_attribute_category IS NOT NULL
      THEN
        l_errbuf       := NULL;
        l_retcode      := g_normal;
        l_glb_attr_cat := invoices_stg_rec.global_attribute_category;
        validate_glb_attr_cat(p_global_attribute_category => l_glb_attr_cat,
                              p_errbuf                    => l_errbuf,
                              p_retcode                   => l_retcode);
      
        IF l_retcode <> g_normal
        THEN
          l_error_count := l_error_count + 1;
          l_error_tab_count := l_error_tab_count + 1;
          l_error_message := substr(l_errbuf, 1, 2000);
          l_total_err_msg := l_total_err_msg || ';' || l_error_message;
          l_error_tab_type(l_error_tab_count).source_table := g_source_table;
          l_error_tab_type(l_error_tab_count).interface_staging_id := invoices_stg_rec.record_id;
          l_error_tab_type(l_error_tab_count).error_type := g_err_val;
          l_error_tab_type(l_error_tab_count).error_code := l_error_code;
          l_error_tab_type(l_error_tab_count).error_message := l_error_message;
          l_error_tab_type(l_error_tab_count).source_column_name := 'GLOBAL_ATTRIBUTE_CATEGORY';
          l_error_tab_type(l_error_tab_count).source_column_value := invoices_stg_rec.global_attribute_category;
        ELSE
          l_invoices_cur_t(l_loop_count).global_attribute_category := l_glb_attr_cat;
        END IF;
      
        debug('Check Global Attribute Category. l_error_count ' ||
              l_error_count || ' l_retcode ' || l_retcode || ' l_errbuf ' ||
              l_errbuf);
      END IF;
    
      --------------------------------------------
      FOR invoice_lines_rec IN invoice_lines_cur(invoices_stg_rec.record_id)
      LOOP
        /* Marking record as validated */
        l_line_loop_count := l_line_loop_count + 1;
        l_invoice_lines_cur_t(l_line_loop_count).record_id := invoice_lines_rec.record_id;
        l_invoice_lines_cur_t(l_line_loop_count).record_line_id := invoice_lines_rec.record_line_id;
        l_invoice_lines_cur_t(l_line_loop_count).status_flag := g_flag_validated;
        l_invoice_lines_cur_t(l_line_loop_count).error_type := NULL;
        l_invoice_lines_cur_t(l_line_loop_count).error_message := NULL;
        l_invoice_lines_cur_t(l_line_loop_count).new_project_id := NULL;
        l_invoice_lines_cur_t(l_line_loop_count).new_task_id := NULL;
        l_invoice_lines_cur_t(l_line_loop_count).expenditure_organization_id := NULL;
        l_invoice_lines_cur_t(l_line_loop_count).tax := NULL;
        l_invoice_lines_cur_t(l_line_loop_count).tax_rate_code := NULL;
        l_invoice_lines_cur_t(l_line_loop_count).tax_regime_code := NULL;
        l_invoice_lines_cur_t(l_line_loop_count).tax_status_code := NULL;
        l_invoice_lines_cur_t(l_line_loop_count).tax_jurisdiction_code := NULL;
        l_invoice_lines_cur_t(l_line_loop_count).tax_rate := NULL;
        l_invoice_lines_cur_t(l_line_loop_count).dist_code_combination_id := NULL;
        l_invoice_lines_cur_t(l_line_loop_count).dist_code_concatenated := NULL;
        l_errbuf := NULL;
        l_retcode := g_normal;
        l_total_lines_err_msg := NULL;
        l_error_lines_count := l_error_count;
      
        --------------------------------------------
        IF invoice_lines_rec.line_type_lookup_code IS NULL
        THEN
          l_error_message := l_mandatory_err_msg;
          l_error_count := l_error_count + 1;
          l_error_tab_count := l_error_tab_count + 1;
          l_total_lines_err_msg := l_total_lines_err_msg || ';' ||
                                   l_error_message;
          l_error_tab_type(l_error_tab_count).source_table := g_source_lines_table;
          l_error_tab_type(l_error_tab_count).interface_staging_id := invoice_lines_rec.record_line_id;
          l_error_tab_type(l_error_tab_count).error_type := g_err_val;
          --
          l_error_tab_type(l_error_tab_count).error_code := l_error_code;
          l_error_tab_type(l_error_tab_count).error_message := l_error_message;
          l_error_tab_type(l_error_tab_count).source_column_name := 'LINE_TYPE_LOOKUP_CODE';
          l_error_tab_type(l_error_tab_count).source_column_value := invoice_lines_rec.line_type_lookup_code;
          debug('Mandatory Check for Line Type Lookup Code. l_error_count ' ||
                l_error_count || ' l_retcode ' || l_retcode ||
                ' l_errbuf ' || l_errbuf);
        END IF;
      
        --------------------------------------------
        IF invoice_lines_rec.line_number IS NULL
        THEN
          l_error_message := l_mandatory_err_msg;
          l_error_count := l_error_count + 1;
          l_error_tab_count := l_error_tab_count + 1;
          l_total_lines_err_msg := l_total_lines_err_msg || ';' ||
                                   l_error_message;
          l_error_tab_type(l_error_tab_count).source_table := g_source_lines_table;
          l_error_tab_type(l_error_tab_count).interface_staging_id := invoice_lines_rec.record_line_id;
          l_error_tab_type(l_error_tab_count).error_type := g_err_val;
          --
          l_error_tab_type(l_error_tab_count).error_code := l_error_code;
          l_error_tab_type(l_error_tab_count).error_message := l_error_message;
          l_error_tab_type(l_error_tab_count).source_column_name := 'LINE_NUMBER';
          l_error_tab_type(l_error_tab_count).source_column_value := invoice_lines_rec.line_number;
          debug('Mandatory Check for Line Number. l_error_count ' ||
                l_error_count || ' l_retcode ' || l_retcode ||
                ' l_errbuf ' || l_errbuf);
        END IF;
      
        --------------------------------------------
        IF invoice_lines_rec.amount IS NULL
        THEN
          l_error_message := l_mandatory_err_msg;
          l_error_count := l_error_count + 1;
          l_error_tab_count := l_error_tab_count + 1;
          l_total_lines_err_msg := l_total_lines_err_msg || ';' ||
                                   l_error_message;
          l_error_tab_type(l_error_tab_count).source_table := g_source_lines_table;
          l_error_tab_type(l_error_tab_count).interface_staging_id := invoice_lines_rec.record_line_id;
          l_error_tab_type(l_error_tab_count).error_type := g_err_val;
          --
          l_error_tab_type(l_error_tab_count).error_code := l_error_code;
          l_error_tab_type(l_error_tab_count).error_message := l_error_message;
          l_error_tab_type(l_error_tab_count).source_column_name := 'AMOUNT';
          l_error_tab_type(l_error_tab_count).source_column_value := invoice_lines_rec.amount;
          debug('Mandatory Check for Line Amount. l_error_count ' ||
                l_error_count || ' l_retcode ' || l_retcode ||
                ' l_errbuf ' || l_errbuf);
        END IF;
      
        --------------------------------------------
        IF invoice_lines_rec.accounting_date IS NULL
        THEN
          l_error_message := l_mandatory_err_msg;
          l_error_count := l_error_count + 1;
          l_error_tab_count := l_error_tab_count + 1;
          l_total_lines_err_msg := l_total_lines_err_msg || ';' ||
                                   l_error_message;
          l_error_tab_type(l_error_tab_count).source_table := g_source_lines_table;
          l_error_tab_type(l_error_tab_count).interface_staging_id := invoice_lines_rec.record_line_id;
          l_error_tab_type(l_error_tab_count).error_type := g_err_val;
          --
          l_error_tab_type(l_error_tab_count).error_code := l_error_code;
          l_error_tab_type(l_error_tab_count).error_message := l_error_message;
          l_error_tab_type(l_error_tab_count).source_column_name := 'ACCOUNTING_DATE';
          l_error_tab_type(l_error_tab_count).source_column_value := invoice_lines_rec.accounting_date;
          debug('Mandatory Check for Line Accounting Date. l_error_count ' ||
                l_error_count || ' l_retcode ' || l_retcode ||
                ' l_errbuf ' || l_errbuf);
        END IF;
      
        --------------------------------------------
        IF (invoice_lines_rec.dist_code_combination_id = 0 OR
           invoice_lines_rec.dist_code_combination_id IS NULL) AND
           invoice_lines_rec.po_number IS NULL
        THEN
          ln_dist_code_comb_id        := 0;
          lv_dist_error_message       := NULL;
          lv_new_dist_concat_segment  := NULL;
          lv_dist_concat_segment      := NULL;
          lv_new_dist_concat_flip     := NULL;
          ln_dist_chart_of_account_id := invoice_lines_rec.chart_of_accounts_id;
          --Deriving new dist_code_combination_id
          get_code_combination_segments(invoice_lines_rec.old_dist_code_combination_id,
                                        lv_dist_concat_segment,
                                        ln_dist_chart_of_account_id);
        
          IF invoice_lines_rec.line_type_lookup_code IN
             ('ACCRUAL', 'EXPENSE', 'ITEM') AND
             invoices_stg_rec.num_of_holds = 0
          THEN
          
            FOR get_conv_account_rec IN get_conv_account_cur
            LOOP
              l_conv_account := get_conv_account_rec.profile_option_value;
            END LOOP;
          
            lv_new_dist_concat_segment := nvl(substr(invoice_lines_rec.dist_code_concatenated,
                                                     1,
                                                     instr(invoice_lines_rec.dist_code_concatenated,
                                                           '.',
                                                           1,
                                                           2) - 1),
                                              invoice_lines_rec.dist_code_concatenated) || '.' ||
                                          l_conv_account;
          
          ELSE
            lv_new_dist_concat_flip := nvl(substr(invoice_lines_rec.dist_code_concatenated,
                                                  1,
                                                  instr(invoice_lines_rec.dist_code_concatenated,
                                                        '.',
                                                        1,
                                                        2) - 1),
                                           invoice_lines_rec.dist_code_concatenated) ||
                                       substr(lv_dist_concat_segment,
                                              instr(lv_dist_concat_segment,
                                                    '.',
                                                    1,
                                                    2));
          
            FOR flip_segment6_rec IN flip_segment6_cur(invoice_lines_rec.old_dist_code_combination_id,
                                                       invoices_stg_rec.source_le)
            LOOP
              lv_new_dist_concat_segment := substr(lv_new_dist_concat_flip,
                                                   1,
                                                   instr(lv_new_dist_concat_flip,
                                                         '.',
                                                         1,
                                                         5) - 1) || '.' ||
                                            invoices_stg_rec.destination_le ||
                                            substr(lv_new_dist_concat_flip,
                                                   instr(lv_new_dist_concat_flip,
                                                         '.',
                                                         1,
                                                         6));
            END LOOP;
            lv_new_dist_concat_segment := nvl(lv_new_dist_concat_segment,
                                              lv_new_dist_concat_flip);
          END IF;
        
          get_code_combination_id(lv_new_dist_concat_segment,
                                  ln_dist_chart_of_account_id,
                                  ln_dist_code_comb_id,
                                  lv_dist_error_message);
        
          IF ln_dist_code_comb_id = 0 OR ln_dist_code_comb_id IS NULL
          THEN
            l_error_message := lv_dist_error_message;
            l_error_count := l_error_count + 1;
            l_error_tab_count := l_error_tab_count + 1;
            l_total_lines_err_msg := l_total_lines_err_msg || ';' ||
                                     l_error_message;
            l_error_tab_type(l_error_tab_count).source_table := g_source_lines_table;
            l_error_tab_type(l_error_tab_count).interface_staging_id := invoice_lines_rec.record_line_id;
            l_error_tab_type(l_error_tab_count).error_type := g_err_val;
            --
            l_error_tab_type(l_error_tab_count).error_code := l_error_code;
            l_error_tab_type(l_error_tab_count).error_message := l_error_message;
            l_error_tab_type(l_error_tab_count).source_column_name := 'DIST_CODE_COMBINATION_ID';
            l_error_tab_type(l_error_tab_count).source_column_value := invoice_lines_rec.dist_code_combination_id;
            debug('Issue in getting CCID for: ' ||
                  invoice_lines_rec.dist_code_concatenated || ' Error: ' ||
                  lv_dist_error_message);
          ELSE
            l_invoice_lines_cur_t(l_line_loop_count).dist_code_combination_id := ln_dist_code_comb_id;
            l_invoice_lines_cur_t(l_line_loop_count).dist_code_concatenated := lv_new_dist_concat_segment;
          END IF;
        ELSE
          l_invoice_lines_cur_t(l_line_loop_count).dist_code_combination_id := invoice_lines_rec.dist_code_combination_id;
          l_invoice_lines_cur_t(l_line_loop_count).dist_code_concatenated := invoice_lines_rec.dist_code_concatenated;
        END IF;
      
        --------------------------------------------
        l_new_project_id := NULL;
      
        IF invoice_lines_rec.old_project_id IS NOT NULL
        THEN
          l_errbuf  := NULL;
          l_retcode := g_normal;
          validate_conv_project_id(p_old_project_id => invoice_lines_rec.old_project_id,
                                   p_source_org_id  => invoice_lines_rec.org_id,
                                   p_target_org_id  => invoice_lines_rec.new_org_id,
                                   p_new_project_id => l_new_project_id,
                                   p_errbuf         => l_errbuf,
                                   p_retcode        => l_retcode);
        
          IF l_retcode <> g_normal
          THEN
            l_error_count := l_error_count + 1;
            l_error_tab_count := l_error_tab_count + 1;
            l_error_message := substr(l_errbuf, 1, 2000);
            l_total_lines_err_msg := l_total_lines_err_msg || ';' ||
                                     l_error_message;
            l_error_tab_type(l_error_tab_count).source_table := g_source_lines_table;
            l_error_tab_type(l_error_tab_count).interface_staging_id := invoice_lines_rec.record_line_id;
            l_error_tab_type(l_error_tab_count).error_type := g_err_val;
            l_error_tab_type(l_error_tab_count).error_code := l_error_code;
            l_error_tab_type(l_error_tab_count).error_message := l_error_message;
            l_error_tab_type(l_error_tab_count).source_column_name := 'NEW_PROJECT_ID';
            l_error_tab_type(l_error_tab_count).source_column_value := invoice_lines_rec.new_project_id;
          ELSE
            l_invoice_lines_cur_t(l_line_loop_count).new_project_id := l_new_project_id;
          END IF;
        
          debug('Check Converted Project ID. l_error_count ' ||
                l_error_count || ' l_retcode ' || l_retcode ||
                ' l_errbuf ' || l_errbuf);
        
          IF invoice_lines_rec.pa_addition_flag IS NULL
          THEN
            l_error_message := l_mandatory_err_msg;
            l_error_count := l_error_count + 1;
            l_error_tab_count := l_error_tab_count + 1;
            l_total_lines_err_msg := l_total_lines_err_msg || ';' ||
                                     l_error_message;
            l_error_tab_type(l_error_tab_count).source_table := g_source_lines_table;
            l_error_tab_type(l_error_tab_count).interface_staging_id := invoice_lines_rec.record_line_id;
            l_error_tab_type(l_error_tab_count).error_type := g_err_val;
            --
            l_error_tab_type(l_error_tab_count).error_code := l_error_code;
            l_error_tab_type(l_error_tab_count).error_message := l_error_message;
            l_error_tab_type(l_error_tab_count).source_column_name := 'PA_ADDITION_FLAG';
            l_error_tab_type(l_error_tab_count).source_column_value := invoice_lines_rec.pa_addition_flag;
          END IF;
        
          debug('Mandatory check for PA Addition Flag. l_error_count ' ||
                l_error_count || ' l_retcode ' || l_retcode ||
                ' l_errbuf ' || l_errbuf);
        END IF;
      
        --------------------------------------------
        l_new_task_id := NULL;
      
        IF invoice_lines_rec.old_task_id IS NOT NULL AND
           l_new_project_id IS NOT NULL
        THEN
          l_errbuf  := NULL;
          l_retcode := g_normal;
          validate_conv_task_id(p_new_project_id => l_new_project_id,
                                p_old_task_id    => invoice_lines_rec.old_task_id,
                                p_new_task_id    => l_new_task_id,
                                p_errbuf         => l_errbuf,
                                p_retcode        => l_retcode);
        
          IF l_retcode <> g_normal
          THEN
            l_error_count := l_error_count + 1;
            l_error_tab_count := l_error_tab_count + 1;
            l_error_message := substr(l_errbuf, 1, 2000);
            l_total_lines_err_msg := l_total_lines_err_msg || ';' ||
                                     l_error_message;
            l_error_tab_type(l_error_tab_count).source_table := g_source_lines_table;
            l_error_tab_type(l_error_tab_count).interface_staging_id := invoice_lines_rec.record_line_id;
            l_error_tab_type(l_error_tab_count).error_type := g_err_val;
            l_error_tab_type(l_error_tab_count).error_code := l_error_code;
            l_error_tab_type(l_error_tab_count).error_message := l_error_message;
            l_error_tab_type(l_error_tab_count).source_column_name := 'NEW_TASK_ID';
            l_error_tab_type(l_error_tab_count).source_column_value := invoice_lines_rec.new_task_id;
          ELSE
            l_invoice_lines_cur_t(l_line_loop_count).new_task_id := l_new_task_id;
          END IF;
        
          debug('Check Converted Task ID. l_error_count ' || l_error_count ||
                ' l_retcode ' || l_retcode || ' l_errbuf ' || l_errbuf);
        END IF;
      
        ------------------------------------
        IF l_new_project_id IS NOT NULL AND l_new_task_id IS NOT NULL
        THEN
          l_errbuf  := NULL;
          l_retcode := g_normal;
          validate_expenditure_types(p_new_project_id     => l_new_project_id,
                                     p_new_task_id        => l_new_task_id,
                                     p_expenditure_org_id => l_expenditure_org_id,
                                     p_errbuf             => l_errbuf,
                                     p_retcode            => l_retcode);
        
          IF l_retcode <> g_normal
          THEN
            l_error_count := l_error_count + 1;
            l_error_tab_count := l_error_tab_count + 1;
            l_error_message := substr(l_errbuf, 1, 2000);
            l_total_lines_err_msg := l_total_lines_err_msg || ';' ||
                                     l_error_message;
            l_error_tab_type(l_error_tab_count).source_table := g_source_lines_table;
            l_error_tab_type(l_error_tab_count).interface_staging_id := invoice_lines_rec.record_line_id;
            l_error_tab_type(l_error_tab_count).error_type := g_err_val;
            l_error_tab_type(l_error_tab_count).error_code := l_error_code;
            l_error_tab_type(l_error_tab_count).error_message := l_error_message;
            l_error_tab_type(l_error_tab_count).source_column_name := 'EXPENDITURE_ORGANIZATION_ID';
            l_error_tab_type(l_error_tab_count).source_column_value := invoice_lines_rec.expenditure_organization_id;
          ELSE
            l_invoice_lines_cur_t(l_line_loop_count).expenditure_organization_id := l_expenditure_org_id;
          END IF;
        
          debug('Check Expenditures from Converted Projects and Tasks. l_error_count ' ||
                l_error_count || ' l_retcode ' || l_retcode ||
                ' l_errbuf ' || l_errbuf);
        END IF;
      
        ------------------------------------
        IF invoice_lines_rec.line_type_lookup_code = 'TAX' AND
           invoice_lines_rec.tax_regime_code IS NULL
        THEN
          l_errbuf                := NULL;
          l_retcode               := g_normal;
          l_tax_rate_code         := invoice_lines_rec.tax_rate_code;
          l_tax_rate_id           := invoice_lines_rec.tax_rate_id;
          l_tax                   := NULL;
          l_tax_regime_code       := NULL;
          l_tax_status_code       := NULL;
          l_tax_jurisdiction_code := NULL;
          l_percentage_rate       := NULL;
          validate_tax(p_org_id                => invoice_lines_rec.org_id,
                       p_tax                   => l_tax,
                       p_tax_rate_code         => l_tax_rate_code,
                       p_tax_regime_code       => l_tax_regime_code,
                       p_tax_status_code       => l_tax_status_code,
                       p_tax_jurisdiction_code => l_tax_jurisdiction_code,
                       p_tax_rate_id           => l_tax_rate_id,
                       p_percentage_rate       => l_percentage_rate,
                       p_errbuf                => l_errbuf,
                       p_retcode               => l_retcode);
        
          IF l_retcode <> g_normal
          THEN
            l_error_count := l_error_count + 1;
            l_error_tab_count := l_error_tab_count + 1;
            l_error_message := substr(l_errbuf, 1, 2000);
            l_total_lines_err_msg := l_total_lines_err_msg || ';' ||
                                     l_error_message;
            l_error_tab_type(l_error_tab_count).source_table := g_source_lines_table;
            l_error_tab_type(l_error_tab_count).interface_staging_id := invoice_lines_rec.record_line_id;
            l_error_tab_type(l_error_tab_count).error_type := g_err_val;
            l_error_tab_type(l_error_tab_count).error_code := l_error_code;
            l_error_tab_type(l_error_tab_count).error_message := l_error_message;
            l_error_tab_type(l_error_tab_count).source_column_name := 'TAX_RATE_CODE';
            l_error_tab_type(l_error_tab_count).source_column_value := invoice_lines_rec.tax_rate_code;
          ELSIF invoice_lines_rec.tax_regime_code IS NULL
          THEN
            l_invoice_lines_cur_t(l_line_loop_count).tax := l_tax;
            l_invoice_lines_cur_t(l_line_loop_count).tax_rate_code := l_tax_rate_code;
            l_invoice_lines_cur_t(l_line_loop_count).tax_regime_code := l_tax_regime_code;
            l_invoice_lines_cur_t(l_line_loop_count).tax_status_code := l_tax_status_code;
            l_invoice_lines_cur_t(l_line_loop_count).tax_jurisdiction_code := l_tax_jurisdiction_code;
            l_invoice_lines_cur_t(l_line_loop_count).tax_rate := l_percentage_rate;
          END IF;
        
        ELSIF invoice_lines_rec.line_type_lookup_code = 'TAX' AND
              invoice_lines_rec.tax_regime_code IS NOT NULL
        THEN
          l_invoice_lines_cur_t(l_line_loop_count).tax := invoice_lines_rec.tax;
          l_invoice_lines_cur_t(l_line_loop_count).tax_rate_code := invoice_lines_rec.tax_rate_code;
          l_invoice_lines_cur_t(l_line_loop_count).tax_regime_code := invoice_lines_rec.tax_regime_code;
          l_invoice_lines_cur_t(l_line_loop_count).tax_status_code := invoice_lines_rec.tax_status_code;
          l_invoice_lines_cur_t(l_line_loop_count).tax_jurisdiction_code := invoice_lines_rec.tax_jurisdiction_code;
          l_invoice_lines_cur_t(l_line_loop_count).tax_rate := invoice_lines_rec.tax_rate;
        END IF;
      
        debug('Check Tax Rate Code. l_error_count ' || l_error_count ||
              ' l_retcode ' || l_retcode || ' l_errbuf ' || l_errbuf);
      
        IF l_error_count > 0
        THEN
          l_invoice_lines_cur_t(l_line_loop_count).status_flag := g_flag_error;
          l_invoice_lines_cur_t(l_line_loop_count).error_type := g_err_val;
          l_invoice_lines_cur_t(l_line_loop_count).error_message := TRIM(leading ';' FROM
                                                                         l_total_lines_err_msg);
        END IF;
      
        /* Logging error for records failed validation  */
        IF l_error_tab_type.count > 0
        THEN
          log_error(p_source_tab_type => l_error_tab_type,
                    pov_return_status => l_log_ret_sts,
                    pov_error_message => l_log_err_msg);
        END IF;
      
      END LOOP;
    
      IF l_error_count > 0
      THEN
      
        IF l_error_count > l_error_lines_count
        THEN
          l_total_err_msg := l_total_err_msg || ';' ||
                             'Invoice Line Level Errors. Please refer line error messages for more details.';
        END IF;
      
        l_invoices_cur_t(l_loop_count).status_flag := g_flag_error;
        l_invoices_cur_t(l_loop_count).error_type := g_err_val;
        l_invoices_cur_t(l_loop_count).record_status := 'VALIDATION-ERROR';
        l_invoices_cur_t(l_loop_count).error_message := TRIM(leading ';' FROM
                                                             l_total_err_msg);
      END IF;
    
      /* Logging error for records failed validation  */
      IF l_error_tab_type.count > 0
      THEN
        log_error(p_source_tab_type => l_error_tab_type,
                  pov_return_status => l_log_ret_sts,
                  pov_error_message => l_log_err_msg);
        l_error_tab_type.delete;
        l_error_tab_count := 0;
      END IF;
    
    END LOOP;
  
    debug('Records loaded to all table types');
    BEGIN
    
      FORALL idx IN l_invoices_cur_t.first .. l_invoices_cur_t.last SAVE
                                              EXCEPTIONS
        UPDATE xxap_invc_intfc_stg
           SET status_flag               = l_invoices_cur_t(idx).status_flag,
               error_type                = l_invoices_cur_t(idx).error_type,
               error_message             = l_invoices_cur_t(idx)
                                           .error_message,
               new_vendor_site_id        = l_invoices_cur_t(idx)
                                           .new_vendor_site_id,
               vendor_name               = l_invoices_cur_t(idx).vendor_name,
               vendor_num                = l_invoices_cur_t(idx).vendor_num,
               vendor_site_code          = l_invoices_cur_t(idx)
                                           .vendor_site_code,
               terms_name                = l_invoices_cur_t(idx).terms_name,
               new_party_id              = l_invoices_cur_t(idx).new_party_id,
               new_party_site_id         = l_invoices_cur_t(idx)
                                           .new_party_site_id,
               external_bank_account_id  = l_invoices_cur_t(idx)
                                           .external_bank_account_id,
               global_attribute_category = l_invoices_cur_t(idx)
                                           .global_attribute_category,
               gl_date                   = nvl(g_accounting_date, gl_date),
               request_id                = g_request_id,
               record_status             = l_invoices_cur_t(idx)
                                           .record_status,
               last_update_date          = SYSDATE,
               last_updated_by           = g_user_id,
               last_update_login         = g_login_id
         WHERE batch_id = g_loader_request_id
           AND record_id = l_invoices_cur_t(idx).record_id;
    EXCEPTION
      WHEN OTHERS THEN
      
        FOR k IN 1 .. SQL%bulk_exceptions.count
        LOOP
          debug('Error Message : ' ||
                SQLERRM(-sql%BULK_EXCEPTIONS(k).error_code));
        END LOOP;
      
        ROLLBACK;
    END;
    debug('Records updated at Invoice Header Staging Table');
    BEGIN
    
      FORALL idx IN l_invoice_lines_cur_t.first .. l_invoice_lines_cur_t.last SAVE
                                                   EXCEPTIONS
        UPDATE xxap_invc_lines_intfc_stg
           SET status_flag                 = l_invoice_lines_cur_t(idx)
                                             .status_flag,
               error_type                  = l_invoice_lines_cur_t(idx)
                                             .error_type,
               error_message               = l_invoice_lines_cur_t(idx)
                                             .error_message,
               new_project_id              = l_invoice_lines_cur_t(idx)
                                             .new_project_id,
               new_task_id                 = l_invoice_lines_cur_t(idx)
                                             .new_task_id,
               expenditure_organization_id = l_invoice_lines_cur_t(idx)
                                             .expenditure_organization_id,
               tax                         = l_invoice_lines_cur_t(idx).tax,
               tax_rate_code               = l_invoice_lines_cur_t(idx)
                                             .tax_rate_code,
               tax_regime_code             = l_invoice_lines_cur_t(idx)
                                             .tax_regime_code,
               tax_status_code             = l_invoice_lines_cur_t(idx)
                                             .tax_status_code,
               tax_jurisdiction_code       = l_invoice_lines_cur_t(idx)
                                             .tax_jurisdiction_code,
               tax_rate                    = l_invoice_lines_cur_t(idx)
                                             .tax_rate,
               dist_code_combination_id    = l_invoice_lines_cur_t(idx)
                                             .dist_code_combination_id,
               dist_code_concatenated      = l_invoice_lines_cur_t(idx)
                                             .dist_code_concatenated,
               request_id                  = g_request_id,
               accounting_date             = nvl(g_accounting_date,
                                                 accounting_date),
               last_update_date            = SYSDATE,
               last_updated_by             = g_user_id,
               last_update_login           = g_login_id
         WHERE batch_id = g_loader_request_id
           AND record_id = l_invoice_lines_cur_t(idx).record_id
           AND record_line_id = l_invoice_lines_cur_t(idx).record_line_id;
    
      debug('Records updated at Invoice Lines Staging Table');
    EXCEPTION
      WHEN OTHERS THEN
      
        FOR k IN 1 .. SQL%bulk_exceptions.count
        LOOP
          debug('Error Message : ' ||
                SQLERRM(-sql%BULK_EXCEPTIONS(k).error_code));
        END LOOP;
      
        ROLLBACK;
    END;
  EXCEPTION
    WHEN OTHERS THEN
      p_errbuf  := substr('Exception in Procedure validate_data. ' ||
                          SQLERRM,
                          1,
                          2000);
      p_retcode := g_error;
      debug(p_errbuf);
  END validate_data;

  -- =============================================================================
  -- Procedure: update_stg_flags
  -- =============================================================================
  --   To update staging table flags for error records
  -- =============================================================================
  --  Input Parameters :
  --    No Input Parameters
  --  Output Parameters :
  --    p_retcode   : Program Return Code = 0/1/2
  --    p_errbuf    : Error message in case of any failure
  -- -----------------------------------------------------------------------------
  PROCEDURE update_stg_flags(p_errbuf  OUT VARCHAR2,
                             p_retcode OUT NUMBER) IS
    PRAGMA AUTONOMOUS_TRANSACTION;
    l_error_tab_type     xxetn_common_error_pkg.g_source_tab_type;
    l_blk_error_tab_type xxetn_common_error_pkg.g_source_tab_type;
    l_log_ret_sts        VARCHAR2(2000);
    l_log_err_msg        VARCHAR2(2000);
    l_error_message      VARCHAR2(2000);
    l_blk_err_var1       VARCHAR2(2000);
  BEGIN
    p_errbuf  := NULL;
    p_retcode := g_normal;
    /* Updating Process Flag to E/C for error/converted records during conversion run */
    BEGIN
    
      FORALL indx IN g_status_invc_ttype.first .. g_status_invc_ttype.last SAVE
                                                  EXCEPTIONS
        UPDATE xxap_invc_intfc_stg xops
           SET xops.status_flag       = g_status_invc_ttype(indx).status_flag,
               xops.error_type        = g_status_invc_ttype(indx).error_type,
               xops.error_message     = g_status_invc_ttype(indx).message,
               xops.last_update_date  = SYSDATE,
               record_status          = 'INTERFACE-REJECTIONS-ERROR',
               xops.last_updated_by   = g_user_id,
               xops.last_update_login = g_login_id,
               xops.request_id        = g_request_id
         WHERE xops.record_id = g_status_invc_ttype(indx).interface_txn_id;
    EXCEPTION
      WHEN g_bulk_exception THEN
        debug('Bulk Exception Count ' || SQL%bulk_exceptions.count);
      
        FOR exep_indx IN 1 .. SQL%bulk_exceptions.count
        LOOP
          l_blk_err_var1  := g_status_invc_ttype(SQL%BULK_EXCEPTIONS(exep_indx).error_index)
                             .interface_txn_id;
          l_error_message := substr('Bulk Exception occured while updating project status_flag. ' ||
                                    SQLERRM(-1 * (SQL%BULK_EXCEPTIONS(exep_indx)
                                            .error_code)),
                                    1,
                                    2000);
          debug(l_error_message);
          l_blk_error_tab_type(exep_indx).source_table := g_source_table;
          l_blk_error_tab_type(exep_indx).interface_staging_id := to_number(l_blk_err_var1);
          l_blk_error_tab_type(exep_indx).source_keyname1 := NULL;
          l_blk_error_tab_type(exep_indx).source_keyvalue1 := NULL;
          l_blk_error_tab_type(exep_indx).source_keyname2 := NULL;
          l_blk_error_tab_type(exep_indx).source_keyvalue2 := NULL;
          l_blk_error_tab_type(exep_indx).source_keyname3 := NULL;
          l_blk_error_tab_type(exep_indx).source_keyvalue3 := NULL;
          l_blk_error_tab_type(exep_indx).source_keyname4 := NULL;
          l_blk_error_tab_type(exep_indx).source_keyvalue4 := NULL;
          l_blk_error_tab_type(exep_indx).source_keyname5 := NULL;
          l_blk_error_tab_type(exep_indx).source_keyvalue5 := NULL;
          l_blk_error_tab_type(exep_indx).source_column_name := 'RECORD_ID';
          l_blk_error_tab_type(exep_indx).source_column_value := l_blk_err_var1;
          l_blk_error_tab_type(exep_indx).error_type := g_err_imp;
          l_blk_error_tab_type(exep_indx).error_code := 'ETN_AP_INVC_BLKUPD_EXCEP';
          l_blk_error_tab_type(exep_indx).error_message := l_error_message;
          l_blk_error_tab_type(exep_indx).severity := NULL;
          l_blk_error_tab_type(exep_indx).proposed_solution := NULL;
          UPDATE xxap_invc_intfc_stg
             SET status_flag       = g_flag_error,
                 error_type        = g_err_val,
                 last_update_date  = SYSDATE,
                 last_updated_by   = g_user_id,
                 last_update_login = g_login_id,
                 request_id        = g_request_id,
                 record_status     = 'INTERFACE-REJECTIONS-EXCEPTION'
           WHERE record_id = to_number(l_blk_err_var1);
          log_error(p_source_tab_type => l_blk_error_tab_type,
                    pov_return_status => l_log_ret_sts,
                    pov_error_message => l_log_err_msg);
        END LOOP;
      
    END;
  
    --
    FOR indx IN 1 .. g_status_invc_ttype.count
    LOOP
      l_error_tab_type(indx).source_table := g_source_table;
      l_error_tab_type(indx).interface_staging_id := g_status_invc_ttype(indx)
                                                     .interface_txn_id;
      l_error_tab_type(indx).source_keyname1 := NULL;
      l_error_tab_type(indx).source_keyvalue1 := NULL;
      l_error_tab_type(indx).source_keyname2 := NULL;
      l_error_tab_type(indx).source_keyvalue2 := NULL;
      l_error_tab_type(indx).source_keyname3 := NULL;
      l_error_tab_type(indx).source_keyvalue3 := NULL;
      l_error_tab_type(indx).source_keyname4 := NULL;
      l_error_tab_type(indx).source_keyvalue4 := NULL;
      l_error_tab_type(indx).source_keyname5 := NULL;
      l_error_tab_type(indx).source_keyvalue5 := NULL;
      l_error_tab_type(indx).source_column_name := 'RECORD_ID';
      l_error_tab_type(indx).source_column_value := g_status_invc_ttype(indx)
                                                    .interface_txn_id;
      l_error_tab_type(indx).error_type := g_err_imp;
      l_error_tab_type(indx).error_code := 'ETN_AP_INVC_IMPORT_ERR';
      l_error_tab_type(indx).error_message := g_status_invc_ttype(indx)
                                              .message;
      l_error_tab_type(indx).proposed_solution := NULL;
      l_error_tab_type(indx).severity := NULL;
    END LOOP;
  
    debug('l_error_tab_type.COUNT ' || l_error_tab_type.count);
  
    IF l_error_tab_type.count > 0
    THEN
      log_error(p_source_tab_type => l_error_tab_type,
                pov_return_status => l_log_ret_sts,
                pov_error_message => l_log_err_msg);
      l_error_tab_type.delete;
    END IF;
  
    --
    g_status_invc_ttype.delete;
    COMMIT;
  EXCEPTION
    WHEN OTHERS THEN
      p_errbuf  := substr('Exception in Procedure update_stg_flags. ' ||
                          SQLERRM,
                          1,
                          2000);
      p_retcode := g_warning;
      debug(p_errbuf);
      ROLLBACK;
  END update_stg_flags;

  -- =============================================================================
  -- Procedure: update_lines_stg_flags
  -- =============================================================================
  --   To update staging lines table flags for error records
  -- =============================================================================
  --  Input Parameters :
  --    No Input Parameters
  --  Output Parameters :
  --    p_retcode   : Program Return Code = 0/1/2
  --    p_errbuf    : Error message in case of any failure
  -- -----------------------------------------------------------------------------
  PROCEDURE update_lines_stg_flags(p_errbuf  OUT VARCHAR2,
                                   p_retcode OUT NUMBER) IS
    PRAGMA AUTONOMOUS_TRANSACTION;
    l_error_tab_type     xxetn_common_error_pkg.g_source_tab_type;
    l_blk_error_tab_type xxetn_common_error_pkg.g_source_tab_type;
    l_log_ret_sts        VARCHAR2(2000);
    l_log_err_msg        VARCHAR2(2000);
    l_error_message      VARCHAR2(2000);
    l_blk_err_var1       VARCHAR2(2000);
  BEGIN
    p_errbuf  := NULL;
    p_retcode := g_normal;
    /* Updating Process Flag to E/C for error/converted records during conversion run */
    BEGIN
    
      FORALL indx IN g_status_invc_lines_ttype.first .. g_status_invc_lines_ttype.last SAVE
                                                        EXCEPTIONS
        UPDATE xxap_invc_lines_intfc_stg xops
           SET xops.status_flag       = g_status_invc_lines_ttype(indx)
                                        .status_flag,
               xops.error_type        = g_status_invc_lines_ttype(indx)
                                        .error_type,
               xops.error_message     = g_status_invc_lines_ttype(indx)
                                        .message,
               xops.last_update_date  = SYSDATE,
               xops.last_updated_by   = g_user_id,
               xops.last_update_login = g_login_id,
               xops.request_id        = g_request_id
         WHERE xops.record_line_id = g_status_invc_lines_ttype(indx)
              .record_line_id;
    EXCEPTION
      WHEN g_bulk_exception THEN
        debug('Bulk Exception Count ' || SQL%bulk_exceptions.count);
      
        FOR exep_indx IN 1 .. SQL%bulk_exceptions.count
        LOOP
          l_blk_err_var1  := g_status_invc_lines_ttype(SQL%BULK_EXCEPTIONS(exep_indx).error_index)
                             .record_line_id;
          l_error_message := substr('Bulk Exception occured while updating project status_flag. ' ||
                                    SQLERRM(-1 * (SQL%BULK_EXCEPTIONS(exep_indx)
                                            .error_code)),
                                    1,
                                    2000);
          debug(l_error_message);
          l_blk_error_tab_type(exep_indx).source_table := g_source_lines_table;
          l_blk_error_tab_type(exep_indx).interface_staging_id := to_number(l_blk_err_var1);
          l_blk_error_tab_type(exep_indx).source_keyname1 := NULL;
          l_blk_error_tab_type(exep_indx).source_keyvalue1 := NULL;
          l_blk_error_tab_type(exep_indx).source_keyname2 := NULL;
          l_blk_error_tab_type(exep_indx).source_keyvalue2 := NULL;
          l_blk_error_tab_type(exep_indx).source_keyname3 := NULL;
          l_blk_error_tab_type(exep_indx).source_keyvalue3 := NULL;
          l_blk_error_tab_type(exep_indx).source_keyname4 := NULL;
          l_blk_error_tab_type(exep_indx).source_keyvalue4 := NULL;
          l_blk_error_tab_type(exep_indx).source_keyname5 := NULL;
          l_blk_error_tab_type(exep_indx).source_keyvalue5 := NULL;
          l_blk_error_tab_type(exep_indx).source_column_name := 'RECORD_LINE_ID';
          l_blk_error_tab_type(exep_indx).source_column_value := l_blk_err_var1;
          l_blk_error_tab_type(exep_indx).error_type := g_err_imp;
          l_blk_error_tab_type(exep_indx).error_code := 'ETN_AP_INVC_LINES_BLKUPD_EXCEP';
          l_blk_error_tab_type(exep_indx).error_message := l_error_message;
          l_blk_error_tab_type(exep_indx).severity := NULL;
          l_blk_error_tab_type(exep_indx).proposed_solution := NULL;
          UPDATE xxap_invc_lines_intfc_stg
             SET status_flag = g_flag_error,
                 error_type  = g_err_val
           WHERE record_line_id = to_number(l_blk_err_var1);
          log_error(p_source_tab_type => l_blk_error_tab_type,
                    pov_return_status => l_log_ret_sts,
                    pov_error_message => l_log_err_msg);
        END LOOP;
      
    END;
  
    --
    FOR indx IN 1 .. g_status_invc_lines_ttype.count
    LOOP
      l_error_tab_type(indx).source_table := g_source_lines_table;
      l_error_tab_type(indx).interface_staging_id := g_status_invc_lines_ttype(indx)
                                                     .record_line_id;
      l_error_tab_type(indx).source_keyname1 := NULL;
      l_error_tab_type(indx).source_keyvalue1 := NULL;
      l_error_tab_type(indx).source_keyname2 := NULL;
      l_error_tab_type(indx).source_keyvalue2 := NULL;
      l_error_tab_type(indx).source_keyname3 := NULL;
      l_error_tab_type(indx).source_keyvalue3 := NULL;
      l_error_tab_type(indx).source_keyname4 := NULL;
      l_error_tab_type(indx).source_keyvalue4 := NULL;
      l_error_tab_type(indx).source_keyname5 := NULL;
      l_error_tab_type(indx).source_keyvalue5 := NULL;
      l_error_tab_type(indx).source_column_name := 'RECORD_LINE_ID';
      l_error_tab_type(indx).source_column_value := g_status_invc_lines_ttype(indx)
                                                    .record_line_id;
      l_error_tab_type(indx).error_type := g_err_imp;
      l_error_tab_type(indx).error_code := 'ETN_AP_INVC_LINES_IMPORT_ERR';
      l_error_tab_type(indx).error_message := g_status_invc_lines_ttype(indx)
                                              .message;
      l_error_tab_type(indx).proposed_solution := NULL;
      l_error_tab_type(indx).severity := NULL;
    END LOOP;
  
    debug('l_error_tab_type.COUNT ' || l_error_tab_type.count);
  
    IF l_error_tab_type.count > 0
    THEN
      log_error(p_source_tab_type => l_error_tab_type,
                pov_return_status => l_log_ret_sts,
                pov_error_message => l_log_err_msg);
      l_error_tab_type.delete;
    END IF;
  
    --
    g_status_invc_lines_ttype.delete;
    COMMIT;
  EXCEPTION
    WHEN OTHERS THEN
      p_errbuf  := substr('Exception in Procedure update_lines_stg_flags. ' ||
                          SQLERRM,
                          1,
                          2000);
      p_retcode := g_warning;
      debug(p_errbuf);
      ROLLBACK;
  END update_lines_stg_flags;

  -- =============================================================================
  -- Procedure: tie_back
  -- =============================================================================
  --   To Map Old and New Invoices and to fetch errors
  -- =============================================================================
  --  Input Parameters :
  --    No Input Parameters
  --  Output Parameters :
  --    p_retcode   : Program Return Code = 0/1/2
  --    p_errbuf    : Error message in case of any failure
  -- -----------------------------------------------------------------------------
  PROCEDURE tie_back(p_errbuf  OUT VARCHAR2,
                     p_retcode OUT NUMBER) IS
    l_invoice_count    NUMBER := 0;
    l_errbuf           VARCHAR2(2000) := NULL;
    l_status           VARCHAR2(2000) := NULL;
    l_retcode          NUMBER := NULL;
    l_payment_priority ap_payment_schedules_all.payment_priority%TYPE;
    l_due_date         ap_payment_schedules_all.due_date%TYPE;
  
    /*Cursor to fetch successfully inserted data to AP Interface table*/
    CURSOR tie_back_hdr_cur IS
      SELECT *
        FROM xxap_invc_intfc_stg xis
       WHERE xis.status_flag = g_flag_processed
         AND xis.batch_id = g_loader_request_id
         AND xis.process_flag = g_flag_yes
         AND xis.record_status <> 'ATTACHMENT-ADDED'
         AND xis.operation = 'COPY'
       ORDER BY xis.record_id;
  
    /*Cursor to fetch successfully inserted lines data to AP Interface lines table*/
    CURSOR tie_back_line_cur IS
      SELECT xils.*
        FROM xxap_invc_lines_intfc_stg xils,
             xxap_invc_intfc_stg       xis
       WHERE xils.status_flag = g_flag_processed
         AND xils.batch_id = g_loader_request_id
         AND xils.record_id = xis.record_id
         AND xis.process_flag = g_flag_yes
         AND xis.operation = 'COPY'
       ORDER BY xils.record_id,
                xils.record_line_id;
  
    CURSOR interface_error_hdr_cur(p_invoice_id IN ap_invoices_interface.invoice_id%TYPE,
                                   p_org_id     IN ap_invoices_interface.org_id%TYPE) IS
      SELECT api.invoice_num,
             apr.reject_lookup_code,
             api.invoice_id
        FROM ap_interface_rejections apr,
             ap_invoices_interface   api
       WHERE api.invoice_id = apr.parent_id
         AND api.invoice_id = p_invoice_id
         AND api.org_id = p_org_id
         AND apr.parent_table = 'AP_INVOICES_INTERFACE';
  
    --cursor to fetch line error details
    CURSOR interface_error_line_cur(p_org_id          IN ap_invoice_lines_interface.org_id%TYPE,
                                    p_invoice_id      IN ap_invoice_lines_interface.invoice_id%TYPE,
                                    p_invoice_line_id IN ap_invoice_lines_interface.org_id%TYPE) IS
      SELECT api.invoice_num        invoice_num,
             apr.reject_lookup_code reject_lookup_code
        FROM ap_interface_rejections    apr,
             ap_invoice_lines_interface apli,
             ap_invoices_interface      api
       WHERE apli.invoice_line_id = apr.parent_id
         AND apli.invoice_id = api.invoice_id
         AND apli.invoice_id = p_invoice_id
         AND apli.org_id = p_org_id
         AND apli.invoice_line_id = p_invoice_line_id
         AND apr.parent_table = 'AP_INVOICE_LINES_INTERFACE';
  
    l_err_details VARCHAR2(4000);
  
    CURSOR get_err_details_cur(p_reject_code IN VARCHAR2) IS
      SELECT m.message_text err_details
        FROM fnd_new_messages m
       WHERE m.message_name = p_reject_code
         AND m.language_code = userenv('LANG')
      UNION ALL
      SELECT meaning || ' - ' || description err_details
        FROM fnd_lookup_values
       WHERE lookup_code = p_reject_code
         AND LANGUAGE = userenv('LANG')
         AND lookup_type LIKE 'REJECT CODE'
         AND enabled_flag = 'Y'
         AND SYSDATE BETWEEN nvl(start_date_active, SYSDATE - 1) AND
             nvl(end_date_active, SYSDATE + 1);
  
    l_invc_cnt   NUMBER := 0;
    l_invoice_id ap_invoices_all.invoice_id%TYPE;
  
    CURSOR get_processed_invc_cur(p_invoice_num    IN ap_invoices_all.invoice_num%TYPE,
                                  p_org_id         IN ap_invoices_all.org_id%TYPE,
                                  p_vendor_id      IN ap_invoices_all.vendor_id%TYPE,
                                  p_vendor_site_id IN ap_invoices_all.vendor_site_id%TYPE) IS
      SELECT invoice_id
        FROM ap_invoices_all
       WHERE invoice_num = p_invoice_num
         AND org_id = p_org_id
         AND vendor_id = p_vendor_id
         AND vendor_site_id = p_vendor_site_id;
  BEGIN
    debug('Tie Back Starts at: ' ||
          to_char(SYSDATE, 'DD-MON-YYYY HH24:MI:SS'));
  
    /*Fetching all Processed Invoice Header records from Staging Table*/
    FOR tie_back_hdr_rec IN tie_back_hdr_cur
    LOOP
    
      debug('Interface Transaction Id = ' || tie_back_hdr_rec.record_id);
      debug('Invoice Num = ' || tie_back_hdr_rec.new_invoice_num);
    
      FOR interface_error_hdr_rec IN interface_error_hdr_cur(tie_back_hdr_rec.invoice_id,
                                                             tie_back_hdr_rec.new_org_id)
      LOOP
        l_invoice_count := l_invoice_count + 1;
        g_status_invc_ttype(l_invoice_count).interface_txn_id := tie_back_hdr_rec.record_id;
        g_status_invc_ttype(l_invoice_count).status_flag := g_flag_processed;
        g_status_invc_ttype(l_invoice_count).error_type := NULL;
        g_status_invc_ttype(l_invoice_count).message := NULL;
        debug('Invoice Number= ' || interface_error_hdr_rec.invoice_num);
        debug('Reject Code - ' ||
              interface_error_hdr_rec.reject_lookup_code);
        l_err_details := NULL;
      
        FOR get_err_details_rec IN get_err_details_cur(interface_error_hdr_rec.reject_lookup_code)
        LOOP
          l_err_details := get_err_details_rec.err_details;
        END LOOP;
      
        g_status_invc_ttype(l_invoice_count).status_flag := g_flag_error;
        g_status_invc_ttype(l_invoice_count).error_type := g_err_imp;
        g_status_invc_ttype(l_invoice_count).message := CASE
                                                          WHEN l_err_details IS NULL THEN
                                                           interface_error_hdr_rec.reject_lookup_code
                                                          ELSE
                                                           l_err_details
                                                        END;
      END LOOP;
    
      l_invc_cnt := 0;
    
      FOR get_processed_invc_rec IN get_processed_invc_cur(tie_back_hdr_rec.new_invoice_num,
                                                           tie_back_hdr_rec.new_org_id,
                                                           tie_back_hdr_rec.vendor_id,
                                                           tie_back_hdr_rec.new_vendor_site_id)
      LOOP
        l_invc_cnt   := l_invc_cnt + 1;
        l_invoice_id := get_processed_invc_rec.invoice_id;
        validate_pay_sch(p_old_invoice_id   => l_invoice_id,
                         p_old_org_id       => tie_back_hdr_rec.new_org_id,
                         p_payment_priority => l_payment_priority,
                         p_due_date         => l_due_date,
                         p_errbuf           => l_errbuf,
                         p_retcode          => l_retcode);
      END LOOP;
    
      IF l_invc_cnt = 1
      THEN
        UPDATE xxap_invc_intfc_stg
           SET new_invoice_id    = l_invoice_id,
               new_due_date      = l_due_date,
               last_update_date  = SYSDATE,
               last_updated_by   = g_user_id,
               last_update_login = g_login_id,
               request_id        = g_request_id,
               record_status     = 'TIE-BACK'
         WHERE record_id = tie_back_hdr_rec.record_id;
        UPDATE xxap_invc_lines_intfc_stg
           SET last_update_date  = SYSDATE,
               last_updated_by   = g_user_id,
               last_update_login = g_login_id,
               request_id        = g_request_id,
               new_invoice_id    = l_invoice_id
         WHERE record_id = tie_back_hdr_rec.record_id;
        UPDATE xxap_invc_holds_conv_stg
           SET new_invoice_id = l_invoice_id
         WHERE record_id = tie_back_hdr_rec.record_id;
        COMMIT;
      
        IF tie_back_hdr_rec.num_of_holds > 0
        THEN
          debug('Call Procedure hold_invoice - Start');
          hold_invoice(l_errbuf,
                       l_retcode,
                       l_invoice_id,
                       tie_back_hdr_rec.is_non_po,
                       tie_back_hdr_rec.is_invc_cancelled,
                       tie_back_hdr_rec.receipt_req_flag,
                       tie_back_hdr_rec.record_id,
                       tie_back_hdr_rec.new_org_id,
                       tie_back_hdr_rec.batch_id);
          debug('Call Procedure hold_invoice - End; l_retcode ' ||
                l_retcode || ' l_errbuf ' || l_errbuf);
        ELSE
          debug('No Holds on Source Invoice');
        END IF;
      
        /*Migrate Attachments From Source to Target Invoice*/
        BEGIN
          xxfnd_cmn_res_attach_pkg.migrate_attachments(p_entity_name      => 'AP_INVOICES',
                                                       p_source_pk1_value => tie_back_hdr_rec.old_invoice_id, --Source Invoice ID
                                                       p_source_pk2_value => NULL,
                                                       p_target_pk1_value => l_invoice_id, --Target Invoice ID
                                                       p_target_pk2_value => NULL,
                                                       p_status           => l_status,
                                                       p_error_message    => l_errbuf);
          debug('Attachment Status for Invoice ID: ' || l_invoice_id ||
                ' is: ' || l_status);
          debug('Error Message Attachment Status for Invoice ID: ' ||
                l_invoice_id || ' is: ' || l_errbuf);
        
          IF l_status != 'S'
          THEN
            l_invoice_count := l_invoice_count + 1;
            g_status_invc_ttype(l_invoice_count).interface_txn_id := tie_back_hdr_rec.record_id;
            g_status_invc_ttype(l_invoice_count).status_flag := g_flag_error;
            g_status_invc_ttype(l_invoice_count).error_type := g_err_imp;
            g_status_invc_ttype(l_invoice_count).message := l_errbuf;
            UPDATE xxap_invc_intfc_stg
               SET last_update_date  = SYSDATE,
                   last_updated_by   = g_user_id,
                   last_update_login = g_login_id,
                   request_id        = g_request_id,
                   record_status     = 'ATTACHMENT-ERROR'
             WHERE record_id = tie_back_hdr_rec.record_id;
            COMMIT;
          ELSE
            UPDATE xxap_invc_intfc_stg
               SET last_update_date  = SYSDATE,
                   last_updated_by   = g_user_id,
                   last_update_login = g_login_id,
                   request_id        = g_request_id,
                   record_status     = 'ATTACHMENT-ADDED'
             WHERE record_id = tie_back_hdr_rec.record_id;
            COMMIT;
          END IF;
        
        END;
      ELSIF l_invc_cnt > 1
      THEN
        l_invoice_count := l_invoice_count + 1;
        g_status_invc_ttype(l_invoice_count).interface_txn_id := tie_back_hdr_rec.record_id;
        g_status_invc_ttype(l_invoice_count).status_flag := g_flag_error;
        g_status_invc_ttype(l_invoice_count).error_type := g_err_imp;
        g_status_invc_ttype(l_invoice_count).message := 'More than one invoice found for Invoice Num: ' ||
                                                        tie_back_hdr_rec.new_invoice_num ||
                                                        ', Org ID: ' ||
                                                        tie_back_hdr_rec.new_org_id ||
                                                        ', Vendor ID: ' ||
                                                        tie_back_hdr_rec.vendor_id ||
                                                        ', Vendor Site ID: ' ||
                                                        tie_back_hdr_rec.new_vendor_site_id;
      END IF;
    
    END LOOP;
  
    update_stg_flags(l_errbuf, l_retcode);
  
    l_invoice_count := 0;
    --tie back for invoice line
    FOR tie_back_line_rec IN tie_back_line_cur
    LOOP
      debug('Interface Transaction Id = ' ||
            tie_back_line_rec.record_line_id);
    
      FOR interface_error_line_rec IN interface_error_line_cur(tie_back_line_rec.new_org_id,
                                                               tie_back_line_rec.invoice_id,
                                                               tie_back_line_rec.invoice_line_id)
      LOOP
        l_invoice_count := l_invoice_count + 1;
        g_status_invc_lines_ttype(l_invoice_count).record_line_id := tie_back_line_rec.record_line_id;
        g_status_invc_lines_ttype(l_invoice_count).status_flag := g_flag_processed;
        g_status_invc_lines_ttype(l_invoice_count).error_type := NULL;
        g_status_invc_lines_ttype(l_invoice_count).message := NULL;
        debug('Invoice Num for line= ' ||
              interface_error_line_rec.invoice_num);
        debug('Reject Code - ' ||
              interface_error_line_rec.reject_lookup_code);
        l_err_details := NULL;
      
        FOR get_err_details_rec IN get_err_details_cur(interface_error_line_rec.reject_lookup_code)
        LOOP
          l_err_details := get_err_details_rec.err_details;
        END LOOP;
      
        g_status_invc_lines_ttype(l_invoice_count).status_flag := g_flag_error;
        g_status_invc_lines_ttype(l_invoice_count).error_type := g_err_imp;
        g_status_invc_lines_ttype(l_invoice_count).message := CASE
                                                                WHEN l_err_details IS NULL THEN
                                                                 interface_error_line_rec.reject_lookup_code
                                                                ELSE
                                                                 l_err_details
                                                              END;
      END LOOP;
    
    END LOOP;
  
    update_lines_stg_flags(l_errbuf, l_retcode);
    UPDATE xxap_invc_lines_intfc_stg xils
       SET status_flag       = g_flag_error,
           error_type        = g_err_imp,
           last_update_date  = SYSDATE,
           last_updated_by   = g_user_id,
           last_update_login = g_login_id,
           request_id        = g_request_id,
           error_message     = 'Error at Invoice Header.'
     WHERE EXISTS (SELECT 1
              FROM xxap_invc_intfc_stg xiis
             WHERE xiis.record_id = xils.record_id
               AND xiis.status_flag = g_flag_error
               AND xiis.error_type = g_err_imp)
       AND xils.status_flag = g_flag_processed;
  
    UPDATE xxap_invc_intfc_stg xiis
       SET status_flag       = g_flag_error,
           error_type        = g_err_imp,
           record_status     = 'INTERFACE-REJECTIONS-ERROR',
           last_update_date  = SYSDATE,
           last_updated_by   = g_user_id,
           last_update_login = g_login_id,
           request_id        = g_request_id,
           error_message     = TRIM(leading ';' FROM 'Invoice Line Level Errors. Please refer line error messages for more details.' || ';' ||
                                    error_message)
     WHERE EXISTS (SELECT 1
              FROM xxap_invc_lines_intfc_stg xils
             WHERE xils.record_id = xiis.record_id
               AND xils.status_flag = g_flag_error
               AND xils.error_type = g_err_imp)
       AND xiis.status_flag = g_flag_processed;
    COMMIT;
  
    UPDATE xxap_invc_lines_intfc_stg xils
       SET status_flag       = g_flag_error,
           error_type        = g_err_imp,
           last_update_date  = SYSDATE,
           last_updated_by   = g_user_id,
           last_update_login = g_login_id,
           request_id        = g_request_id,
           error_message     = TRIM(leading ';' FROM
                                    error_message || ';' ||
                                    'Error at other invoice line.')
     WHERE EXISTS (SELECT 1
              FROM xxap_invc_lines_intfc_stg xils1
             WHERE xils1.record_id = xils.record_id
               AND xils1.status_flag = g_flag_error
               AND xils1.error_type = g_err_imp)
       AND xils.status_flag = g_flag_processed;
    COMMIT;
  EXCEPTION
    WHEN OTHERS THEN
      p_retcode := g_error;
      p_errbuf  := 'Failed During Tie Back';
      debug('In Tie Back when others' || SQLERRM);
  END tie_back;

  -- =============================================================================
  -- Procedure: import_data
  -- =============================================================================
  --   To COnvert Validated records
  -- =============================================================================
  --  Input Parameters :
  --    No Input Parameters
  --  Output Parameters :
  --    p_retcode   : Program Return Code = 0/1/2
  --    p_errbuf    : Error message in case of any failure
  -- -----------------------------------------------------------------------------
  PROCEDURE import_data(p_errbuf  OUT VARCHAR2,
                        p_retcode OUT NUMBER) IS
    l_errbuf            VARCHAR2(2000) := NULL;
    l_retcode           NUMBER := NULL;
    l_phase             VARCHAR2(50);
    l_status            VARCHAR2(50);
    l_dev_phase         VARCHAR2(50);
    l_dev_status        VARCHAR2(50);
    l_message           VARCHAR2(50);
    l_req_return_status BOOLEAN;
  
    /*Cursor to fetch all the scenario details*/
    CURSOR get_lookup_scenarios_cur IS
      SELECT hou1.organization_id destination_org_id
        FROM hr_operating_units hou1
       WHERE EXISTS
       (SELECT 1
                FROM fnd_lookup_types  flt,
                     fnd_lookup_values flv
               WHERE flv.lookup_type = 'XXETN_RESTRUCTURE_MAPPING'
                 AND flv.language = userenv('LANG')
                 AND flv.enabled_flag = 'Y'
                 AND flv.tag = 'COPY' --Operation
                 AND SYSDATE BETWEEN nvl(start_date_active, SYSDATE - 1) AND
                     nvl(end_date_active, SYSDATE + 1)
                 AND hou1.short_code =
                     substr(meaning, 1, instr(meaning, '.') - 1) || '_OU');
  
    /* Cursor to fetch validated data from staging table */
    CURSOR invc_stg_cur IS
      SELECT invoice_id,
             new_invoice_num invoice_num,
             invoice_type_lookup_code,
             invoice_date,
             po_number,
             vendor_id,
             vendor_num,
             vendor_name,
             new_vendor_site_id,
             vendor_site_code,
             CASE
               WHEN invoice_type_lookup_code = 'PREPAYMENT' THEN
                prepay_amt_unapplied
               ELSE
                amt_unpaid
             END invoice_amount,
             invoice_currency_code,
             exchange_rate,
             exchange_rate_type,
             exchange_date,
             terms_id,
             terms_name,
             description,
             awt_group_id,
             awt_group_name,
             last_update_date,
             last_updated_by,
             last_update_login,
             creation_date,
             created_by,
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
             NULL status,
             SOURCE,
             group_id,
             NULL request_id,
             payment_cross_rate_type,
             payment_cross_rate_date,
             payment_cross_rate,
             payment_currency_code,
             workflow_flag,
             doc_category_code,
             voucher_num,
             payment_method_lookup_code,
             pay_group_lookup_code,
             goods_received_date,
             invoice_received_date,
             g_accounting_date gl_date,
             accts_pay_code_combination_id,
             ussgl_transaction_code,
             exclusive_payment_flag,
             new_org_id,
             amount_applicable_to_discount,
             prepay_num,
             prepay_dist_num,
             prepay_apply_amount,
             prepay_gl_date,
             invoice_includes_prepay_flag,
             no_xrate_base_amount,
             vendor_email_address,
             terms_date,
             requester_id,
             ship_to_location,
             external_doc_ref,
             prepay_line_num,
             requester_first_name,
             requester_last_name,
             application_id,
             product_table,
             reference_key1,
             reference_key2,
             reference_key3,
             reference_key4,
             reference_key5,
             apply_advances_flag,
             calc_tax_during_import_flag,
             control_amount,
             add_tax_to_inv_amt_flag,
             tax_related_invoice_id,
             taxation_country,
             document_sub_type,
             supplier_tax_invoice_number,
             supplier_tax_invoice_date,
             supplier_tax_exchange_rate,
             tax_invoice_recording_date,
             tax_invoice_internal_seq,
             new_legal_entity_id,
             legal_entity_name,
             reference_1,
             reference_2,
             operating_unit,
             bank_charge_bearer,
             remittance_message1,
             remittance_message2,
             remittance_message3,
             unique_remittance_identifier,
             uri_check_digit,
             settlement_priority,
             payment_reason_code,
             payment_reason_comments,
             payment_method_code,
             delivery_channel_code,
             paid_on_behalf_employee_id,
             net_of_retainage_flag,
             requester_employee_num,
             cust_registration_code,
             cust_registration_number,
             new_party_id,
             new_party_site_id,
             pay_proc_trxn_type_code,
             payment_function,
             payment_priority,
             port_of_entry_code,
             external_bank_account_id,
             accts_pay_code_concatenated,
             pay_awt_group_id,
             pay_awt_group_name,
             original_invoice_amount,
             dispute_reason,
             remit_to_supplier_name,
             remit_to_supplier_id,
             remit_to_supplier_site,
             remit_to_supplier_site_id,
             relationship_id,
             remit_to_supplier_num
        FROM xxap_invc_intfc_stg xiis
       WHERE xiis.status_flag = g_flag_validated
         AND xiis.batch_id = g_loader_request_id
         AND xiis.process_flag = g_flag_yes
         AND xiis.operation = 'COPY'
       ORDER BY xiis.invoice_id,
                xiis.org_id;
  
    CURSOR invc_lines_stg_cur IS
      SELECT invoice_id,
             invoice_line_id,
             line_number,
             line_type_lookup_code,
             line_group_number,
             amount,
             g_accounting_date           accounting_date,
             description,
             amount_includes_tax_flag,
             prorate_across_flag,
             tax_code,
             final_match_flag,
             NULL                        po_header_id,
             po_number,
             NULL                        po_line_id,
             po_line_number,
             NULL                        po_line_location_id,
             po_shipment_num,
             NULL                        po_distribution_id,
             po_distribution_num,
             po_unit_of_measure,
             inventory_item_id,
             item_description,
             quantity_invoiced,
             ship_to_location_code,
             unit_price,
             distribution_set_id,
             distribution_set_name,
             dist_code_concatenated,
             dist_code_combination_id,
             awt_group_id,
             awt_group_name,
             last_updated_by,
             last_update_date,
             last_update_login,
             created_by,
             creation_date,
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
             po_release_id,
             release_num,
             account_segment,
             balancing_segment,
             cost_center_segment,
             new_project_id,
             new_task_id,
             expenditure_type,
             expenditure_item_date,
             expenditure_organization_id,
             project_accounting_context,
             pa_addition_flag,
             pa_quantity,
             ussgl_transaction_code,
             stat_amount,
             type_1099,
             income_tax_region,
             assets_tracking_flag,
             price_correction_flag,
             new_org_id,
             receipt_number,
             receipt_line_number,
             match_option,
             packing_slip,
             rcv_transaction_id,
             pa_cc_ar_invoice_id,
             pa_cc_ar_invoice_line_num,
             reference_1,
             reference_2,
             pa_cc_processed_code,
             tax_recovery_rate,
             tax_recovery_override_flag,
             tax_recoverable_flag,
             tax_code_override_flag,
             tax_code_id,
             credit_card_trx_id,
             award_id,
             vendor_item_num,
             taxable_flag,
             price_correct_inv_num,
             external_doc_line_ref,
             serial_number,
             manufacturer,
             model_number,
             warranty_number,
             deferred_acctg_flag,
             def_acctg_start_date,
             def_acctg_end_date,
             def_acctg_number_of_periods,
             def_acctg_period_type,
             unit_of_meas_lookup_code,
             price_correct_inv_line_num,
             asset_book_type_code,
             asset_category_id,
             requester_id,
             requester_first_name,
             requester_last_name,
             requester_employee_num,
             application_id,
             product_table,
             reference_key1,
             reference_key2,
             reference_key3,
             reference_key4,
             reference_key5,
             purchasing_category,
             purchasing_category_id,
             cost_factor_id,
             cost_factor_name,
             control_amount,
             assessable_value,
             default_dist_ccid,
             primary_intended_use,
             ship_to_location_id,
             product_type,
             product_category,
             product_fisc_classification,
             user_defined_fisc_class,
             trx_business_category,
             tax_regime_code,
             tax,
             tax_jurisdiction_code,
             tax_status_code,
             tax_rate_id,
             tax_rate_code,
             tax_rate,
             incl_in_taxable_line_flag,
             source_application_id,
             source_entity_code,
             source_event_class_code,
             source_trx_id,
             source_line_id,
             source_trx_level_type,
             tax_classification_code,
             cc_reversal_flag,
             company_prepaid_invoice_id,
             expense_group,
             justification,
             merchant_document_number,
             merchant_reference,
             merchant_tax_reg_number,
             merchant_taxpayer_id,
             receipt_currency_code,
             receipt_conversion_rate,
             receipt_currency_amount,
             country_of_supply,
             pay_awt_group_id,
             pay_awt_group_name,
             expense_start_date,
             expense_end_date,
             merchant_name#1
        FROM xxap_invc_lines_intfc_stg xils
       WHERE xils.status_flag = g_flag_validated
         AND EXISTS
       (SELECT 1
                FROM xxap_invc_intfc_stg xiis
               WHERE xiis.record_id = xils.record_id
                 AND xiis.status_flag = g_flag_validated
                 AND xiis.process_flag = g_flag_yes
                 AND xiis.operation = 'COPY'
                 AND xiis.batch_id = g_loader_request_id);
  
    TYPE l_invc_infc_tbl IS TABLE OF ap_invoices_interface%ROWTYPE;
  
    l_invc_infc l_invc_infc_tbl;
  
    TYPE l_invc_infc_lines_tbl IS TABLE OF ap_invoice_lines_interface%ROWTYPE;
  
    l_invc_infc_lines l_invc_infc_lines_tbl;
    l_request_id      NUMBER;
    l_error_count     NUMBER := 0;
    l_error_message   VARCHAR2(4000);
  BEGIN
    debug('Invoice Import Start');
    DELETE FROM ap_invoice_lines_interface apli
     WHERE EXISTS (SELECT 1
              FROM xxap_invc_intfc_stg       xiis,
                   xxap_invc_lines_intfc_stg xils
             WHERE xiis.invoice_id = apli.invoice_id
               AND xiis.batch_id = g_loader_request_id
               AND xils.record_id = xiis.record_id
               AND xiis.process_flag = g_flag_yes
               AND xiis.status_flag = g_flag_validated);
    DELETE FROM ap_invoices_interface api
     WHERE EXISTS (SELECT 1
              FROM xxap_invc_intfc_stg xiis
             WHERE xiis.invoice_id = api.invoice_id
               AND xiis.batch_id = g_loader_request_id
               AND xiis.process_flag = g_flag_yes
               AND xiis.status_flag = g_flag_validated);
    COMMIT;
    OPEN invc_stg_cur;
  
    LOOP
      FETCH invc_stg_cur BULK COLLECT
        INTO l_invc_infc LIMIT g_limit;
      BEGIN
      
        FORALL i IN l_invc_infc.first .. l_invc_infc.last SAVE EXCEPTIONS
          INSERT INTO ap_invoices_interface VALUES l_invc_infc (i);
      EXCEPTION
        WHEN OTHERS THEN
          ROLLBACK;
          l_error_count := l_error_count + 1;
        
          FOR k IN 1 .. SQL%bulk_exceptions.count
          LOOP
            l_error_message := l_error_message || ';' ||
                               SQLERRM(-sql%BULK_EXCEPTIONS(k).error_code);
            debug('Error Message : ' ||
                  SQLERRM(-sql%BULK_EXCEPTIONS(k).error_code));
          END LOOP;
        
      END;
      EXIT WHEN l_invc_infc.count = 0;
    END LOOP;
  
    CLOSE invc_stg_cur;
    OPEN invc_lines_stg_cur;
  
    LOOP
      FETCH invc_lines_stg_cur BULK COLLECT
        INTO l_invc_infc_lines LIMIT g_limit;
      BEGIN
      
        FORALL i IN l_invc_infc_lines.first .. l_invc_infc_lines.last SAVE
                                               EXCEPTIONS
          INSERT INTO ap_invoice_lines_interface
          VALUES l_invc_infc_lines
            (i);
      EXCEPTION
        WHEN OTHERS THEN
          ROLLBACK;
          l_error_count := l_error_count + 1;
        
          FOR k IN 1 .. SQL%bulk_exceptions.count
          LOOP
            l_error_message := l_error_message || ';' ||
                               SQLERRM(-sql%BULK_EXCEPTIONS(k).error_code);
            debug('Error Message : ' ||
                  SQLERRM(-sql%BULK_EXCEPTIONS(k).error_code));
          END LOOP;
        
      END;
      EXIT WHEN l_invc_infc_lines.count = 0;
    END LOOP;
  
    CLOSE invc_lines_stg_cur;
  
    IF l_error_count > 0
    THEN
      UPDATE xxap_invc_intfc_stg
         SET error_type        = g_err_imp,
             last_update_date  = SYSDATE,
             last_updated_by   = g_user_id,
             last_update_login = g_login_id,
             request_id        = g_request_id,
             gl_date           = g_accounting_date,
             error_message     = 'Error in Inserting records in Interface Tables' ||
                                 l_error_message,
             record_status     = 'RECORDS-INTERFACED-ERROR'
       WHERE status_flag = g_flag_validated
         AND process_flag = g_flag_yes
         AND batch_id = g_loader_request_id;
      UPDATE xxap_invc_lines_intfc_stg xils
         SET error_type        = g_err_imp,
             last_update_date  = SYSDATE,
             last_updated_by   = g_user_id,
             last_update_login = g_login_id,
             request_id        = g_request_id,
             accounting_date   = g_accounting_date,
             error_message     = 'Error in Inserting records in Interface Tables' ||
                                 l_error_message
       WHERE status_flag = g_flag_validated
         AND xils.batch_id = g_loader_request_id
         AND EXISTS
       (SELECT 1
                FROM xxap_invc_intfc_stg xiis
               WHERE xiis.record_id = xils.record_id
                 AND process_flag = g_flag_yes
                 AND xiis.batch_id = g_loader_request_id);
      COMMIT;
    ELSE
      UPDATE xxap_invc_intfc_stg
         SET status_flag       = g_flag_processed,
             last_update_date  = SYSDATE,
             last_updated_by   = g_user_id,
             last_update_login = g_login_id,
             request_id        = g_request_id,
             gl_date           = g_accounting_date,
             record_status     = 'RECORDS-INTERFACED'
       WHERE status_flag = g_flag_validated
         AND process_flag = g_flag_yes
         AND batch_id = g_loader_request_id;
      UPDATE xxap_invc_lines_intfc_stg xils
         SET last_update_date  = SYSDATE,
             last_updated_by   = g_user_id,
             last_update_login = g_login_id,
             request_id        = g_request_id,
             accounting_date   = g_accounting_date,
             status_flag       = g_flag_processed
       WHERE status_flag = g_flag_validated
         AND batch_id = g_loader_request_id
         AND EXISTS
       (SELECT 1
                FROM xxap_invc_intfc_stg xiis
               WHERE xiis.record_id = xils.record_id
                 AND process_flag = g_flag_yes
                 AND xiis.batch_id = g_loader_request_id);
      COMMIT;
    
      FOR get_lookup_scenarios_rec IN get_lookup_scenarios_cur
      LOOP
        mo_global.set_policy_context('S',
                                     get_lookup_scenarios_rec.destination_org_id);
        /*BEGIN
          fnd_global.apps_initialize(user_id      => g_user_id,
                                     resp_id      => g_resp_id,
                                     resp_appl_id => g_resp_appl_id);
        END;*/
        l_request_id := fnd_request.submit_request(application => 'SQLAP',
                                                   program     => 'APXIIMPT',
                                                   description => 'Eaton AP Invoices Conversion Program - Site Restructure',
                                                   start_time  => SYSDATE,
                                                   sub_request => FALSE,
                                                   argument1   => get_lookup_scenarios_rec.destination_org_id,
                                                   argument2   => 'CONVERSION',
                                                   argument3   => NULL,
                                                   argument4   => NULL,
                                                   argument5   => NULL,
                                                   argument6   => NULL,
                                                   argument7   => NULL,
                                                   argument8   => 'N',
                                                   argument9   => 'N',
                                                   argument10  => 'Y', -- Debug Flag
                                                   argument11  => 'N',
                                                   argument12  => '1000',
                                                   argument13  => g_user_id, --g_user_id
                                                   argument14  => g_login_id); --g_login_id
        COMMIT;
      
        IF l_request_id = 0
        THEN
          debug('Request Not Submitted due to "' || fnd_message.get || '".');
        ELSE
          debug('The Program PROGRAM_1 submitted successfully ?Request id :' ||
                l_request_id);
        END IF;
      
        IF l_request_id > 0
        THEN
        
          LOOP
            --
            --To make process execution to wait for 1st program to complete
            --
            l_req_return_status := fnd_concurrent.wait_for_request(request_id => l_request_id,
                                                                   INTERVAL   => 5 --interval Number of seconds to wait between checks
                                                                  ,
                                                                   max_wait   => 60 --Maximum number of seconds to wait for the request completion
                                                                   -- out arguments
                                                                  ,
                                                                   phase      => l_phase,
                                                                   status     => l_status,
                                                                   dev_phase  => l_dev_phase,
                                                                   dev_status => l_dev_status,
                                                                   message    => l_message);
            EXIT WHEN upper(l_phase) = 'COMPLETED' OR upper(l_status) IN('CANCELLED',
                                                                         'ERROR',
                                                                         'TERMINATED');
          END LOOP;
        
          --
          --
          IF upper(l_phase) = 'COMPLETED' AND upper(l_status) = 'ERROR'
          THEN
            debug('The APXIIMPT completed in error. Request id: ' ||
                  l_request_id || ' ' || SQLERRM);
          ELSIF upper(l_phase) = 'COMPLETED' AND upper(l_status) = 'NORMAL'
          THEN
            debug('The APXIIMPT request successful for request id: ' ||
                  l_request_id);
            COMMIT;
          END IF;
        
        END IF;
      
      END LOOP;
    
      debug('Call Procedure tie_back - Start');
      tie_back(l_errbuf, l_retcode);
      debug('Call Procedure tie_back - End; l_retcode ' || l_retcode ||
            ' l_errbuf ' || l_errbuf);
    END IF;
  EXCEPTION
    WHEN OTHERS THEN
      p_errbuf := substr('Exception in Procedure import_data. ' || SQLERRM,
                         1,
                         2000);
      debug('Error : Backtace : ' || dbms_utility.format_error_backtrace);
      p_retcode := g_error;
      debug(p_errbuf);
  END import_data;

  -- =============================================================================
  -- Procedure: main
  -- =============================================================================
  --   Main Procedure - Called from Concurrent Program
  -- =============================================================================
  --  Input Parameters :
  --    p_run_mode         : Run Mode ('LOAD-DATA', 'VALIDATE', 'CANCEL-ON-HOLD',
  --                                    'CONVERSION', 'TIE-BACK', 'RECONCILE', 
  --                                      'CLOSE')
  --    p_batch_id         : Batch Id
  --    p_accounting_date  : Accounting Date
  --    p_po_matched       : PO Matched
  --  Output Parameters :
  --    p_retcode   : Program Return Code = 0/1/2
  --    p_errbuf    : Error message in case of any failure
  -- -----------------------------------------------------------------------------
  PROCEDURE main(p_errbuf          OUT VARCHAR2,
                 p_retcode         OUT NUMBER,
                 p_run_mode        IN VARCHAR2 DEFAULT NULL,
                 p_batch_id        IN VARCHAR2 DEFAULT NULL,
                 p_accounting_date IN VARCHAR2 DEFAULT NULL,
                 p_po_matched      IN VARCHAR2 DEFAULT NULL) IS
    l_errbuf    VARCHAR2(2000) := NULL;
    l_retcode   NUMBER := NULL;
    l_init_err  VARCHAR2(200) := NULL;
    l_step_flag VARCHAR2(1) := g_flag_success;
  BEGIN
    /* Printing program parameters */
    debug('p_run_mode         ' || p_run_mode);
    debug('p_batch_id         ' || p_batch_id);
    /* Printing global profile driven placeholder's value */
    debug('g_request_id       ' || g_request_id);
    debug('g_prog_appl_id     ' || g_prog_appl_id);
    debug('g_program_id       ' || g_program_id);
    debug('g_user_id          ' || g_user_id);
    debug('g_login_id         ' || g_login_id);
    debug('g_org_id           ' || g_org_id);
    debug('g_resp_id          ' || g_resp_id);
    /* Assigning program parameter values to global placeholders */
    g_run_mode          := p_run_mode;
    g_batch_id          := p_batch_id;
    g_loader_request_id := p_batch_id;
    g_po_param          := p_po_matched;
    g_accounting_date   := trunc(nvl(fnd_date.canonical_to_date(p_accounting_date),
                                     SYSDATE));
    /* Initialization of Debug Framework */
    xxetn_debug_pkg.initialize_debug(pov_err_msg      => l_init_err,
                                     piv_program_name => 'XXETN_AP_INVC_RESTRUCT_CNV');
  
    /* Checking for Debug Framework initialization result */
    IF l_init_err IS NULL
    THEN
    
      IF p_run_mode <> g_run_mode_loadata AND p_batch_id IS NULL
      THEN
        p_errbuf    := 'Parameter BatchId should not be null when Run Mode is ' ||
                       p_run_mode;
        p_retcode   := g_error;
        l_step_flag := g_flag_error;
        debug(p_errbuf);
      ELSIF p_run_mode <> g_run_mode_loadata AND p_batch_id IS NOT NULL
      THEN
        l_step_flag := g_flag_success;
      END IF;
    
    ELSIF l_init_err IS NOT NULL
    THEN
      p_errbuf    := substr('Error Framework Initialization failed. ' ||
                            l_init_err,
                            1,
                            2000);
      p_retcode   := g_error;
      l_step_flag := g_flag_error;
      debug(p_errbuf);
    END IF;
  
    debug('l_step_flag ' || l_step_flag);
  
    /* Calling appropriate procedures as per the program Run mode */
    IF l_step_flag = g_flag_success
    THEN
    
      /* Run Mode = LOAD-DATA */
      IF p_run_mode = g_run_mode_loadata
      THEN
        debug('Call Procedure load_invc - Start');
        load_invc(l_errbuf, l_retcode);
        p_errbuf  := l_errbuf;
        p_retcode := l_retcode;
        debug('Call Procedure load_invc - End; p_retcode ' || p_retcode ||
              ' p_errbuf ' || p_errbuf);
        /* Calling print_report to print program stats after data load */
        print_report(l_errbuf, l_retcode, g_loader_request_id);
        p_errbuf  := l_errbuf;
        p_retcode := l_retcode;
        /* Run Mode = VALIDATE */
      ELSIF p_run_mode = g_run_mode_validate
      THEN
        debug('Call Procedure validate - Start');
        validate_data(l_errbuf, l_retcode);
        debug('Call Procedure validate - End; p_retcode ' || l_retcode ||
              ' p_errbuf ' || l_errbuf);
        /* Calling print_report to print program stats after validation */
        print_report(l_errbuf, l_retcode, g_loader_request_id);
        p_errbuf  := l_errbuf;
        p_retcode := l_retcode;
        /* Run Mode = CANCEL-ON-HOLD */
      ELSIF p_run_mode = g_run_mode_cancel_on_hold
      THEN
        debug('Call Procedure cancel_invc_on_hold - Start');
        cancel_invc_on_hold(l_errbuf, l_retcode);
        debug('Call Procedure cancel_invc_on_hold - End; p_retcode ' ||
              l_retcode || ' p_errbuf ' || l_errbuf);
        /* Calling print_report to print program stats after validation */
        print_report(l_errbuf, l_retcode, g_loader_request_id);
        p_errbuf  := l_errbuf;
        p_retcode := l_retcode;
        /* Run Mode = CONVERSION */
      ELSIF p_run_mode = g_run_mode_conversion
      THEN
        debug('Call Procedure import_data - Start');
        import_data(l_errbuf, l_retcode);
        debug('Call Procedure import_data - End; l_retcode ' || l_retcode ||
              ' l_errbuf ' || l_errbuf);
        /* Calling print_report to print program stats after conversion */
        print_report(l_errbuf, l_retcode, g_loader_request_id);
        p_errbuf  := l_errbuf;
        p_retcode := l_retcode;
        /* Run Mode = TIE-BACK */
      ELSIF p_run_mode = g_run_mode_tie_back
      THEN
        debug('Call Procedure tie_back - Start');
        tie_back(l_errbuf, l_retcode);
        debug('Call Procedure tie_back - End; l_retcode ' || l_retcode ||
              ' l_errbuf ' || l_errbuf);
        /* Calling print_report to print program stats after conversion */
        print_report(l_errbuf, l_retcode, g_loader_request_id);
        p_errbuf  := l_errbuf;
        p_retcode := l_retcode;
        /* Run Mode = RECONCILE */
      ELSIF p_run_mode = g_run_mode_reconcilition
      THEN
        /* Calling print_report to print program stats */
        print_report(l_errbuf, l_retcode, g_loader_request_id);
        p_errbuf  := l_errbuf;
        p_retcode := l_retcode;
        /* Run Mode = CLOSE */
      ELSIF p_run_mode = g_run_mode_close
      THEN
        debug('Call Procedure cancel_invc - Start');
        cancel_invc(l_errbuf, l_retcode);
        debug('Call Procedure cancel_invc - End; l_retcode ' || l_retcode ||
              ' l_errbuf ' || l_errbuf);
        /* Calling print_report to print program stats after conversion */
        print_report(l_errbuf, l_retcode, g_loader_request_id);
        p_errbuf  := l_errbuf;
        p_retcode := l_retcode;
      END IF;
    
    END IF;
  EXCEPTION
    WHEN OTHERS THEN
      p_errbuf  := substr('Error in Procedure-main. SQLERRM ' || SQLERRM,
                          1,
                          2000);
      p_retcode := g_error;
      debug(p_errbuf);
  END main;

END xxap_invc_restruct_conv_pkg;
/

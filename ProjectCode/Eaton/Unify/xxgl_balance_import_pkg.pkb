--Begin Revision History
--<<<
-- 09-Mar-2017 09:48:21 C9987884 /main/19
-- 
--<<<
--End Revision History  
CREATE OR REPLACE PACKAGE BODY xxgl_balance_import_pkg AS
  ------------------------------------------------------------------------------------------
  --    Owner        : EATON CORPORATION.
  --    Application  : General Ledger
  --    Schema       : APPS
  --    Compile AS   : APPS
  --    File Name    : XXGL_BALANCE_IMPORT_PKG.pkb
  --    Date         : 22-Jan-2014
  --    Author       : Satendra Bhati
  --    Description  : Package Body for GL Balance conversion
  --
  --    Version      : $ETNHeader: /CCSTORE/ccweb/C9987884/C9987884_GL_TOP_3/vobs/GL_TOP/xxgl/12.0.0/install/xxgl_balance_import_pkg.pkb /main/19 09-Mar-2017 09:48:21 C9987884  $
  --
  --    Parameters  :
  --
  --    Change History
  --  ======================================================================================
  --    v1.0        Satendra Bhati      22-Jan-2014     Initial Creation
  --    v1.1        Manoj Sharma        10-Mar-2014     Code changes as per Review Checklist
  --    v1.2        Manoj Sharma        12-Mar-2014     Added Tie-Back Logic
  --    v1.3        Manoj Sharma        20-Mar-2014     Code changes as per Review Comments
  --    v1.4        Manoj Sharma        06-May-2014     Updated pre_validate procedure
  --    v1.5        Kulraj Singh        19-Jun-2014     Fixed Performance Issues.
  --                                                 Modified proc update_ids_reference, removed unncessary TRIMS
  --    v2.0        Kulraj Singh        26-Aug-2014     Modifications post DFF rationalization. ETN_MAP_UNIT change
  --    v2.1        Kulraj Singh        28-Oct-2014     Defect# 243.Used Accounting Date for COA Mapping Transformation
  --    v2.2        Kulraj Singh        09-Dec-2014     CR# 229933. Logic to offset Functional Currency Amount for
  --                                                 Foreign Currency Open Balances
  --    v2.3        Kulraj Singh        07-Apr-2015     Changes related to Common Error Package. Added 'stage'
  --    v2.4        Harjinder Singh     20-May-2015    Changed as per defect 1803 User Category Key is Changes to user_je_category_name
  --    v2.5        Harjinder Singh     6-jul-2015     Changed for the defect  - 1436  to capture the error messagein the error table
  --    v2.6        Harjinder Singh     15-Jul-2015    Changed for the defect -  1649  -  Ledger derivation using accounting date instead of current calendar date
  --    v2.7        Shailesh Chaudhari  03-May-2016    Changes impleamented for CR 374261- Updating the Refrence column values to Foreign Balance
  --    v2.8        Shailesh Chaudhari  10-Jan-2017    Added Gather Stat
  --    v2.9        Shailesh Chaudhari  10-Jan-2017    Changes implemented to get the atribute values in Journal lines tested in MOCK4.5    
  --  ======================================================================================
  ------------------------------------------------------------------------------------------

  g_balance_category VARCHAR2(27) := fnd_profile.VALUE('ETN_GL_JOURNAL_CON_CATEGORY');
  g_balance_source   VARCHAR2(25) := fnd_profile.VALUE('ETN_GL_JOURNAL_CON_SOURCE');
  g_user_id          NUMBER := fnd_global.user_id;
  g_login_id         NUMBER := fnd_global.login_id;
  g_program_id       NUMBER := fnd_global.conc_program_id;
  g_prog_appl_id     NUMBER := fnd_global.prog_appl_id;
  g_request_id       NUMBER := fnd_global.conc_request_id;
  g_resp_id          NUMBER := fnd_global.resp_id;
  g_resp_appl_id     NUMBER := fnd_global.resp_appl_id;
  g_org_id           NUMBER := mo_global.get_current_org_id;

  g_run_sequence_id  NUMBER;
  g_records_loaded   NUMBER;
  g_records_error    NUMBER;
  g_validation_error VARCHAR2(1);

  d_debug_profile VARCHAR2(100) := Fnd_Profile.VALUE('ETN_FND_DEBUG_PROFILE');
  g_bulk_limit    NUMBER := Fnd_Profile.VALUE('ETN_FND_ERROR_TAB_LIMIT');
  g_bulk_exception EXCEPTION;
  PRAGMA EXCEPTION_INIT(g_bulk_exception, -24381);

  /* Placeholders fo Program Parameters */
  g_run_mode       VARCHAR2(50);
  g_entity         VARCHAR2(50);
  g_source_system  VARCHAR2(50);
  g_set_of_books   VARCHAR2(50);
  g_period_start   DATE;
  g_period_end     DATE;
  g_batch_id       NUMBER;
  g_reprocess_mode VARCHAR2(50);

  /* Program completion codes */
  g_normal  CONSTANT NUMBER := 0;
  g_warning CONSTANT NUMBER := 1;
  g_error   CONSTANT NUMBER := 2;

  /* Run Mode constants*/
  g_run_mode_loadata       CONSTANT VARCHAR2(9) := 'LOAD-DATA';
  g_run_mode_prevalidate   CONSTANT VARCHAR2(12) := 'PRE-VALIDATE';
  g_run_mode_validate      CONSTANT VARCHAR2(8) := 'VALIDATE';
  g_run_mode_conversion    CONSTANT VARCHAR2(10) := 'CONVERSION';
  g_run_mode_reconcilition CONSTANT VARCHAR2(9) := 'RECONCILE';

  /* Entity Name constants*/
  g_entity_openbalance    CONSTANT VARCHAR2(20) := 'OPEN-BALANCE';
  g_entity_monthbalance   CONSTANT VARCHAR2(20) := 'MONTH-BALANCE';
  g_entity_journal        CONSTANT VARCHAR2(20) := 'JOURNAL';
  g_entity_foreignbalance CONSTANT VARCHAR2(20) := 'FOREIGN-BALANCE';

  /* Process records constants */
  g_process_recs_all         CONSTANT VARCHAR2(3) := 'ALL';
  g_process_recs_error       CONSTANT VARCHAR2(5) := 'ERROR';
  g_process_recs_unprocessed CONSTANT VARCHAR2(11) := 'UNPROCESSED';

  /* Flag constants */
  g_flag_v CONSTANT VARCHAR2(1) := 'V';
  g_flag_y CONSTANT VARCHAR2(1) := 'Y';
  g_flag_n CONSTANT VARCHAR2(1) := 'N';
  g_flag_s CONSTANT VARCHAR2(1) := 'S';
  g_flag_e CONSTANT VARCHAR2(1) := 'E';
  g_flag_p CONSTANT VARCHAR2(1) := 'P';
  g_flag_c CONSTANT VARCHAR2(1) := 'C';
  g_flag_x CONSTANT VARCHAR2(1) := 'X';
  g_flag_t CONSTANT VARCHAR2(1) := 'T'; -- v2.2. Intemediate status for Foreign Balance

  /* Lookup constants */
  g_xr_journal_category CONSTANT VARCHAR2(19) := 'ETN_GL_CATEGORY_MAP';
  g_xr_journal_source   CONSTANT VARCHAR2(17) := 'ETN_GL_SOURCE_MAP';
  g_xr_books_mapping    CONSTANT VARCHAR2(21) := 'ETN_GL_SOB_LEDGER_MAP';

  /* Other constants */
  g_source_table   CONSTANT VARCHAR2(16) := 'XXGL_BALANCE_STG';
  g_err_validation CONSTANT VARCHAR2(7) := 'ERR_VAL';
  g_err_import     CONSTANT VARCHAR2(7) := 'ERR_IMP';
  g_err_int        CONSTANT VARCHAR2(7) := 'ERR_INT';
  g_char_period    CONSTANT VARCHAR2(1) := '.';
  g_char_hyphen    CONSTANT VARCHAR2(1) := '-';
  g_status_new     CONSTANT VARCHAR2(3) := 'NEW';
  g_leg_source_fsc  xxgl_balance_stg.leg_source_system%TYPE DEFAULT 'FSC';
  g_leg_source_issc xxgl_balance_stg.leg_source_system%TYPE DEFAULT 'ISSC';
  g_flex_context    xxgl_balance_stg.leg_context%TYPE DEFAULT 'PQNA';
  g_flex_context1    xxgl_balance_stg.leg_context%TYPE DEFAULT 'Conversion'; --Addedd for v2.9

  -- =============================================================================
  -- Procedure: debug
  -- =============================================================================
  --   Common procedure for printing statements to Log
  -- =============================================================================
  --  Input Parameters :
  --    pov_error_me        : Debug message text
  --    pin_run_sequence_id : Run Requence Id
  --    p_source_tab_type   : Error Table Type (xxetn_common_error_pkg.g_source_tab_type)
  --  Output Parameters :
  --    pov_return_status    :
  --    pov_error_message    : Error message in case of any failure
  -- -----------------------------------------------------------------------------
  PROCEDURE debug(piv_message IN VARCHAR2) IS
    l_error_message VARCHAR2(2000) := NULL;
  BEGIN
    xxetn_debug_pkg.add_debug(piv_debug_msg => piv_message);
  EXCEPTION
    WHEN OTHERS THEN
      l_error_message := SUBSTR('Exception in Procedure debug. ' || SQLERRM,
                                1,
                                1999);
      fnd_file.put_line(fnd_file.LOG, l_error_message);
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
  PROCEDURE log_error(pin_batch_id        IN NUMBER DEFAULT NULL,
                      pin_run_sequence_id IN NUMBER DEFAULT NULL,
                      p_source_tab_type   IN xxetn_common_error_pkg.g_source_tab_type,
                      pov_return_status   OUT NOCOPY VARCHAR2,
                      pov_error_message   OUT NOCOPY VARCHAR2) IS
    process_exception EXCEPTION;

    l_return_status VARCHAR2(1) := NULL;
    l_error_message VARCHAR2(2000) := NULL;
  BEGIN

    xxetn_common_error_pkg.add_error(pov_return_status   => l_return_status,
                                     pov_error_msg       => l_error_message,
                                     pin_batch_id        => pin_batch_id,
                                     pin_iface_load_id   => NULL,
                                     pin_run_sequence_id => pin_run_sequence_id,
                                     pi_source_tab       => p_source_tab_type,
                                     piv_active_flag     => g_flag_y,
                                     pin_program_id      => g_program_id,
                                     pin_request_id      => g_request_id);

    IF l_error_message IS NOT NULL THEN
      RAISE process_exception;
    END IF;

  EXCEPTION
    WHEN process_exception THEN
      pov_return_status := g_error;
      pov_error_message := l_error_message;
      fnd_file.put_line(fnd_file.LOG, l_error_message);
    WHEN OTHERS THEN
      l_error_message   := SUBSTR('Exception in Procedure log_error. ' ||
                                  SQLERRM,
                                  1,
                                  1999);
      pov_return_status := g_error;
      pov_error_message := l_error_message;
      fnd_file.put_line(fnd_file.LOG, l_error_message);
  END log_error;

  -- =============================================================================
  -- Procedure: print_report
  -- =============================================================================
  -- Print Conversion program run statistics to program output at the end of each
  -- program run. Also called when program run in RECONCILE mode
  -- =============================================================================
  --  Input Parameters :
  --    piv_entity           : Entity Name
  --    pin_batch_id         : Batch Id
  --    pin_run_sequence_id  : Run Sequence Id
  --  Output Parameters :
  --    pov_errbuf    : Error message in case of any failure
  --    pon_retcode   : Return Status - Normal/Warning/Error
  -- -----------------------------------------------------------------------------
  PROCEDURE print_report(piv_entity          IN VARCHAR2,
                         pin_batch_id        IN NUMBER,
                         pin_run_sequence_id IN NUMBER,
                         pov_return_status   OUT NOCOPY VARCHAR2,
                         pov_error_message   OUT NOCOPY VARCHAR2) IS
    l_return_status VARCHAR2(1);
    l_error_message VARCHAR2(2000);

    l_total           NUMBER := 0;
    l_validated       NUMBER := 0;
    l_interfaced      NUMBER := 0;
    l_process_success NUMBER := 0;
    l_process_error   NUMBER := 0;

    l_reprocess_mode VARCHAR2(20);
  BEGIN
    l_return_status := g_normal;
    l_error_message := NULL;

    SELECT COUNT(1) record_count,
           SUM(DECODE(xbs.process_flag, g_flag_v, 1, 0)) validation_success_count,
           SUM(DECODE(xbs.process_flag, g_flag_p, 1, 0)) interface_count,
           SUM(DECODE(xbs.process_flag, g_flag_c, 1, 0)) process_success_count,
           SUM(DECODE(xbs.process_flag, g_flag_e, 1, 0)) process_error_count
      INTO l_total,
           l_validated,
           l_interfaced,
           l_process_success,
           l_process_error
      FROM xxgl_balance_stg xbs
     WHERE xbs.leg_entity = piv_entity
       AND xbs.batch_id = NVL(pin_batch_id, batch_id)
       AND xbs.run_sequence_id = NVL(pin_run_sequence_id, run_sequence_id);

    SELECT DECODE(g_reprocess_mode,
                  'ALL',
                  'All',
                  'ERROR',
                  'Error',
                  'UNPROCESSED',
                  'Unprocessed',
                  'NA')
      INTO l_reprocess_mode
      FROM dual;

    fnd_file.put_line(fnd_file.OUTPUT,
                      'Parameters :                                                             Date : ' ||
                      TO_CHAR(SYSDATE, 'DD-Mon-RRRR HH24:MI:SS'));
    fnd_file.put_line(fnd_file.OUTPUT, '');
    fnd_file.put_line(fnd_file.OUTPUT, '');
    fnd_file.put_line(fnd_file.OUTPUT,
                      '          Request Id : ' || g_request_id);
    fnd_file.put_line(fnd_file.OUTPUT,
                      '            Run Mode : ' || g_run_mode);
    fnd_file.put_line(fnd_file.OUTPUT,
                      '   Conversion Entity : ' || piv_entity);

    IF g_run_mode = g_run_mode_loadata THEN
      fnd_file.put_line(fnd_file.OUTPUT,
                        '            Batch Id : ' || g_batch_id);
    ELSE
      fnd_file.put_line(fnd_file.OUTPUT,
                        '            Batch Id : ' || pin_batch_id);
    END IF;

    fnd_file.put_line(fnd_file.OUTPUT,
                      '      Reprocess Mode : ' || l_reprocess_mode);
    fnd_file.put_line(fnd_file.OUTPUT, '');
    fnd_file.put_line(fnd_file.OUTPUT, '');
    fnd_file.put_line(fnd_file.OUTPUT,
                      '===================================================================================================');
    fnd_file.put_line(fnd_file.OUTPUT, 'Summary');
    fnd_file.put_line(fnd_file.OUTPUT,
                      '===================================================================================================');
    fnd_file.put_line(fnd_file.OUTPUT, '');

    IF g_run_mode = g_run_mode_loadata THEN
      fnd_file.put_line(fnd_file.OUTPUT,
                        'Records Loaded            :  ' || g_records_loaded);
      fnd_file.put_line(fnd_file.OUTPUT,
                        'Records Not Loaded        :  ' || g_records_error);
    ELSE
      fnd_file.put_line(fnd_file.OUTPUT,
                        'Records Submitted         :  ' || l_total);
      fnd_file.put_line(fnd_file.OUTPUT,
                        'Records Validated         :  ' || l_validated);
      fnd_file.put_line(fnd_file.OUTPUT,
                        'Records Interfaced        :  ' || l_interfaced);
      fnd_file.put_line(fnd_file.OUTPUT,
                        'Records Processed         :  ' ||
                        l_process_success);
      fnd_file.put_line(fnd_file.OUTPUT,
                        'Records Errored           :  ' || l_process_error);
    END IF;

    fnd_file.put_line(fnd_file.OUTPUT, '');
    fnd_file.put_line(fnd_file.OUTPUT,
                      '===================================================================================================');
    fnd_file.put_line(fnd_file.OUTPUT,
                      '                                   *** End of report ***');
  EXCEPTION
    WHEN OTHERS THEN
      l_error_message   := SUBSTR('Exception in Procedure print_report. ' ||
                                  SQLERRM,
                                  1,
                                  1999);
      pov_return_status := g_error;
      pov_error_message := l_error_message;
      fnd_file.put_line(fnd_file.LOG, l_error_message);
  END print_report;

  -- =============================================================================
  -- Procedure: tieback_main
  -- =============================================================================
  -- To check records errored/imported successfully by Journal Import program
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
                         pin_batch_id IN NUMBER) IS
    process_exception EXCEPTION;
    l_error_tab_type xxetn_common_error_pkg.g_source_tab_type;
    l_error_code CONSTANT VARCHAR2(100) := 'ETN_GL_INTERFACE_ERROR';

    l_return_status VARCHAR2(1) := NULL;
    l_error_message VARCHAR2(2000) := NULL;

    l_run_sequence_id NUMBER;
  BEGIN
    pon_retcode := g_normal;
    pov_errbuf  := NULL;

    xxetn_debug_pkg.initialize_debug(pov_err_msg      => l_error_message,
                                     piv_program_name => 'GL_BALANCE_IMPORT_TIEBACK');

    --Printing program parameters to program log
    debug('|---------------------------------------');
    debug('|Program Parameters');
    debug('|---------------------------------------');
    debug('|pin_batch_id : ' || pin_batch_id);
    debug('|---------------------------------------');

    IF l_error_message IS NOT NULL THEN
      l_error_message := 'Error while initializing debug !!';
      RAISE process_exception;
    END IF;

    UPDATE xxgl_balance_stg xbs
       SET xbs.process_flag           = g_flag_c,
           xbs.error_type             = NULL,
           xbs.last_updated_date      = SYSDATE,
           xbs.last_updated_by        = g_user_id,
           xbs.last_update_login      = g_login_id,
           xbs.program_application_id = g_prog_appl_id,
           xbs.program_id             = g_program_id,
           xbs.program_update_date    = SYSDATE,
           xbs.request_id             = g_request_id
     WHERE xbs.leg_entity = piv_entity
       AND xbs.batch_id = pin_batch_id
       AND xbs.process_flag = g_flag_p
       AND NOT EXISTS (SELECT 1
              FROM gl_interface gi
             WHERE gi.attribute1 = xbs.interface_txn_id
               AND gi.group_id = xbs.group_id);

    debug('01. Records Updated for Imported     : ' || SQL%ROWCOUNT);

    UPDATE xxgl_balance_stg xbs
       SET xbs.process_flag           = g_flag_e,
           xbs.error_type             = g_err_import,
           xbs.last_updated_date      = SYSDATE,
           xbs.last_updated_by        = g_user_id,
           xbs.last_update_login      = g_login_id,
           xbs.program_application_id = g_prog_appl_id,
           xbs.program_id             = g_program_id,
           xbs.program_update_date    = SYSDATE,
           xbs.request_id             = g_request_id
     WHERE xbs.leg_entity = piv_entity
       AND xbs.batch_id = pin_batch_id
       AND xbs.process_flag = g_flag_p
       AND EXISTS (SELECT 1
              FROM gl_interface gi
             WHERE gi.status <> g_status_new
               AND gi.attribute1 = xbs.interface_txn_id
               AND gi.group_id = xbs.group_id);

    debug('02. Records Updated for Import Error : ' || SQL%ROWCOUNT);

    --bring Journal Import Errors Into Error Framework Table
    debug('Updating the Journal Import Errors to Common Error Table');

    SELECT err_tbl.source_table,
           err_tbl.interface_staging_id,
           err_tbl.source_keyname1,
           err_tbl.source_keyvalue1,
           err_tbl.source_keyname2,
           err_tbl.source_keyvalue2,
           err_tbl.source_keyname3,
           err_tbl.source_keyvalue3,
           err_tbl.source_keyname4,
           err_tbl.source_keyvalue4,
           err_tbl.source_keyname5,
           err_tbl.source_keyvalue5,
           err_tbl.source_column_name,
           err_tbl.source_column_value,
           err_tbl.error_type,
           err_tbl.error_code,
           err_tbl.error_message,
           err_tbl.severity,
           err_tbl.proposed_solution,
           err_tbl.stage, -- v2.3
           NULL ---Add as per changes done by Sagar in the common error package
           BULK COLLECT
      INTO l_error_tab_type
      FROM (SELECT g_source_table AS source_table,
                   xbs.interface_txn_id AS interface_staging_id,
                   'INTERFACE_TXN_ID' AS source_keyname1,
                   xbs.interface_txn_id AS source_keyvalue1,
                   'BATCH_ID' AS source_keyname2,
                   xbs.batch_id AS source_keyvalue2,
                   'GROUP_ID' AS source_keyname3,
                   xbs.group_id AS source_keyvalue3,
                   NULL AS source_keyname4,
                   NULL AS source_keyvalue4,
                   NULL AS source_keyname5,
                   NULL AS source_keyvalue5,
                   'GL_INTERFACE' AS source_column_name,
                   je_header_id AS source_column_value,
                   g_err_import AS error_type,
                   l_error_code AS error_code,
                   'Error In Interface : ' ||
                   NVL(flv.description, flv.meaning) AS error_message,
                   NULL AS severity,
                   NULL AS proposed_solution,
                   NULL AS stage -- v2.3
              FROM xxgl_balance_stg  xbs,
                   gl_interface      gli,
                   fnd_lookup_values flv
             WHERE flv.lookup_type = 'PSP_SUSP_AC_ERRORS'
               AND flv.language = USERENV('LANG')
               AND flv.enabled_flag = g_flag_y
               AND TRUNC(SYSDATE) BETWEEN
                   NVL(flv.start_date_active, TRUNC(SYSDATE)) AND
                   NVL(flv.end_date_active, TRUNC(SYSDATE + 1))
               AND flv.lookup_code = gli.status
               AND xbs.process_flag = g_flag_e
               AND xbs.leg_entity = piv_entity
               AND xbs.batch_id = pin_batch_id
               AND gli.status <> g_status_new
               AND gli.group_id = xbs.group_id
               AND gli.attribute1 = xbs.interface_txn_id
            UNION ALL
            SELECT g_source_table AS source_table,
                   xbs.interface_txn_id AS interface_staging_id,
                   'INTERFACE_TXN_ID' AS source_keyname1,
                   xbs.interface_txn_id AS source_keyvalue1,
                   'BATCH_ID' AS source_keyname2,
                   xbs.batch_id AS source_keyvalue2,
                   'GROUP_ID' AS source_keyname3,
                   xbs.group_id AS source_keyvalue3,
                   NULL AS source_keyname4,
                   NULL AS source_keyvalue4,
                   NULL AS source_keyname5,
                   NULL AS source_keyvalue5,
                   'GL_INTERFACE' AS source_column_name,
                   JE_HEADER_ID AS source_column_value,
                   g_err_import AS error_type,
                   l_error_code AS error_code,
                   'Error In Interface : Records not imported because other records errored in same Group Id' AS error_message,
                   NULL AS severity,
                   NULL AS proposed_solution,
                   NULL AS stage -- v2.3
              FROM xxgl_balance_stg xbs, gl_interface gli
             WHERE xbs.process_flag = g_flag_e
               AND xbs.leg_entity = piv_entity
               AND xbs.batch_id = pin_batch_id
               AND gli.status = g_flag_p
               AND gli.group_id = xbs.group_id
               AND gli.attribute1 = xbs.interface_txn_id) err_tbl;

    IF l_error_tab_type.COUNT > 0 THEN
      log_error(pin_batch_id        => pin_batch_id,
                pin_run_sequence_id => NULL,
                p_source_tab_type   => l_error_tab_type,
                pov_return_status   => l_return_status,
                pov_error_message   => l_error_message);
    END IF;

    --Print Report Output
    print_report(piv_entity          => piv_entity,
                 pin_batch_id        => pin_batch_id,
                 pin_run_sequence_id => l_run_sequence_id,
                 pov_return_status   => l_return_status,
                 pov_error_message   => l_error_message);

    IF l_return_status <> g_normal THEN
      RAISE process_exception;
    END IF;

  EXCEPTION
    WHEN process_exception THEN
      pon_retcode := g_warning;
      pov_errbuf  := l_error_message;
      fnd_file.put_line(fnd_file.LOG, l_error_message);
    WHEN OTHERS THEN
      l_error_message := SUBSTR('Exception in Procedure tieback_main. ' ||
                                SQLERRM,
                                1,
                                1999);
      pon_retcode     := 2;
      pov_errbuf      := l_error_message;
      fnd_file.put_line(fnd_file.LOG, l_error_message);
  END tieback_main;

  -- =============================================================================
  -- Procedure: interface_data
  -- =============================================================================
  -- Interface validated staging table records to GL_INTERFACE table
  -- =============================================================================
  --  Input Parameters :
  --    piv_entity           : Entity Name
  --    pin_batch_id         : Batch Id
  --    pin_run_sequence_id  : Run Sequence Id
  --  Output Parameters :
  --    pov_return_status    : Return Status - Normal/Warning/Error
  --    pov_error_message    : Error message in case of any failure
  -- -----------------------------------------------------------------------------
  PROCEDURE interface_data(piv_entity          IN VARCHAR2,
                           pin_batch_id        IN NUMBER,
                           pin_run_sequence_id IN NUMBER,
                           pov_return_status   OUT NOCOPY VARCHAR2,
                           pov_error_message   OUT NOCOPY VARCHAR2) IS
    PRAGMA AUTONOMOUS_TRANSACTION;

    l_error_message    VARCHAR2(2000) := NULL;
    l_excep_col1       VARCHAR2(100) := NULL;
    l_intf_count       NUMBER := 0;
    l_bluk_error_count NUMBER := 0;

    l_log_err_sts VARCHAR2(2000) := NULL;
    l_log_err_msg VARCHAR2(2000) := NULL;

    /* Cursor to get validated data from Staging table */
    CURSOR stg_recs_cur IS
      SELECT xbs.leg_source_system     AS leg_source_system,
             xbs.status                AS status,
             xbs.accounting_date       AS accounting_date,
             xbs.leg_currency_code     AS currency_code,
             xbs.creation_date         AS date_created,
             xbs.created_by            AS created_by,
             xbs.leg_actual_flag       AS actual_flag,
             xbs.user_je_category_name AS user_je_category_name,
             xbs.user_je_source_name   AS user_je_source_name
             --,  xbs.leg_curr_conv_date            AS currency_conversion_date
             --,  xbs.leg_user_curr_conv_type       AS user_currency_conversion_type
             --,  xbs.leg_curr_con_rate             AS currency_conversion_rate
            ,
             NULL AS currency_conversion_date,
             NULL AS user_currency_conversion_type,
             NULL AS currency_conversion_rate,
             xbs.segment1 AS segment1,
             xbs.segment2 AS segment2,
             xbs.segment3 AS segment3,
             xbs.segment4 AS segment4,
             xbs.segment5 AS segment5,
             xbs.segment6 AS segment6,
             xbs.segment7 AS segment7,
             xbs.segment8 AS segment8,
             xbs.segment9 AS segment9,
             xbs.segment10 AS segment10,
             DECODE(piv_entity,
                    g_entity_openbalance,
                    xbs.leg_begin_balance_dr,
                    g_entity_monthbalance,
                    xbs.leg_period_net_dr,
                    g_entity_journal,
                    xbs.leg_entered_dr,
                    g_entity_foreignbalance,
                    DECODE(SIGN((xbs.leg_begin_balance_dr -
                                xbs.leg_begin_balance_cr) +
                                (xbs.leg_period_net_dr -
                                xbs.leg_period_net_cr)),
                           1,
                           (xbs.leg_begin_balance_dr -
                           xbs.leg_begin_balance_cr) +
                           (xbs.leg_period_net_dr - xbs.leg_period_net_cr),
                           0) -- v2.2
                   ,
                    0) AS entered_dr --entered_dr
            ,
             DECODE(piv_entity,
                    g_entity_openbalance,
                    xbs.leg_begin_balance_cr,
                    g_entity_monthbalance,
                    xbs.leg_period_net_cr,
                    g_entity_journal,
                    xbs.leg_entered_cr,
                    g_entity_foreignbalance,
                    DECODE(SIGN((xbs.leg_begin_balance_dr -
                                xbs.leg_begin_balance_cr) +
                                (xbs.leg_period_net_dr -
                                xbs.leg_period_net_cr)),
                           -1,
                           (xbs.leg_begin_balance_cr -
                           xbs.leg_begin_balance_dr) +
                           (xbs.leg_period_net_cr - xbs.leg_period_net_dr),
                           0) -- v2.2
                   ,
                    0) AS entered_cr --entered_cr
            ,
             DECODE(piv_entity,
                    g_entity_openbalance,
                    xbs.leg_begin_balance_dr,
                    g_entity_monthbalance,
                    xbs.leg_period_net_dr,
                    g_entity_journal,
                    xbs.leg_accounted_dr,
                    g_entity_foreignbalance,
                    DECODE(SIGN((xbs.leg_begin_balance_dr_beq -
                                xbs.leg_begin_balance_cr_beq) +
                                (xbs.leg_period_net_dr_beq -
                                xbs.leg_period_net_cr_beq)),
                           1,
                           (xbs.leg_begin_balance_dr_beq -
                           xbs.leg_begin_balance_cr_beq) +
                           (xbs.leg_period_net_dr_beq -
                           xbs.leg_period_net_cr_beq),
                           0) -- v2.2
                   ,
                    0) AS accounted_dr --accounted_dr
            ,
             DECODE(piv_entity,
                    g_entity_openbalance,
                    xbs.leg_begin_balance_cr,
                    g_entity_monthbalance,
                    xbs.leg_period_net_cr,
                    g_entity_journal,
                    xbs.leg_accounted_cr,
                    g_entity_foreignbalance,
                    DECODE(SIGN((xbs.leg_begin_balance_dr_beq -
                                xbs.leg_begin_balance_cr_beq) +
                                (xbs.leg_period_net_dr_beq -
                                xbs.leg_period_net_cr_beq)),
                           -1,
                           (xbs.leg_begin_balance_cr_beq -
                           xbs.leg_begin_balance_dr_beq) +
                           (xbs.leg_period_net_cr_beq -
                           xbs.leg_period_net_dr_beq),
                           0) -- v2.2
                   ,
                    0) AS accounted_cr --accounted_cr
            ,
             xbs.leg_transaction_date AS transaction_date,
             xbs.leg_reference1 AS reference1,
             xbs.leg_reference2 AS reference2,
             xbs.leg_reference3 AS reference3,
             xbs.leg_reference4 AS reference4,
             xbs.leg_reference5 AS reference5,
             xbs.leg_reference6 AS reference6,
             xbs.leg_reference7 AS reference7,
             xbs.leg_reference8 AS reference8,
             xbs.leg_reference9 AS reference9,
             xbs.leg_reference10 AS reference10,
             xbs.leg_reference11 AS reference11,
             xbs.leg_reference12 AS reference12,
             xbs.leg_reference13 AS reference13,
             xbs.leg_reference14 AS reference14,
             xbs.leg_reference15 AS reference15,
             xbs.leg_reference16 AS reference16,
             xbs.leg_reference17 AS reference17,
             xbs.leg_reference18 AS reference18,
             xbs.leg_reference19 AS reference19,
             xbs.leg_reference20 AS reference20,
             xbs.leg_reference21 AS reference21,
             xbs.leg_reference22 AS reference22,
             xbs.leg_reference23 AS reference23,
             xbs.leg_reference24 AS reference24,
             xbs.leg_reference25 AS reference25,
             xbs.leg_reference26 AS reference26,
             xbs.leg_reference27 AS reference27,
             xbs.leg_reference28 AS reference28,
             xbs.leg_reference29 AS reference29,
             xbs.leg_reference30 AS reference30,
             xbs.period_name AS period_name,
             xbs.leg_je_line_num AS je_line_num,
             xbs.chart_of_accounts_id AS chart_of_accounts_id,
             xbs.functional_currency_code AS functional_currency_code,
             xbs.code_combination_id AS code_combination_id,
             xbs.leg_date_created_in_gl AS date_created_in_gl,
             xbs.status_description AS status_description,
             xbs.stat_amount AS stat_amount,
             xbs.group_id AS group_id,
             xbs.subledger_doc_sequence_id AS subledger_doc_sequence_id,
             xbs.subledger_doc_sequence_value AS subledger_doc_sequence_value,
             xbs.interface_txn_id AS interface_txn_id,
             xbs.leg_attribute1 AS attribute1,
             xbs.leg_attribute2 AS attribute2,
             xbs.leg_attribute3 AS attribute3,
             xbs.leg_attribute4 AS attribute4,
             xbs.leg_attribute5 AS attribute5,
             xbs.leg_attribute6 AS attribute6,
             xbs.leg_attribute7 AS attribute7,
             xbs.leg_attribute8 AS attribute8,
             xbs.leg_attribute9 AS attribute9,
             xbs.leg_attribute10 AS attribute10,
             xbs.leg_attribute11 AS attribute11,
             xbs.leg_attribute12 AS attribute12,
             xbs.leg_attribute13 AS attribute13,
             xbs.leg_attribute14 AS attribute14,
             xbs.leg_attribute15 AS attribute15,
             xbs.leg_attribute16 AS attribute16,
             xbs.leg_attribute17 AS attribute17,
             xbs.leg_attribute18 AS attribute18,
             xbs.leg_attribute19 AS attribute19,
             xbs.leg_attribute20 AS attribute20,
             xbs.leg_context AS context,
             xbs.leg_context2 AS context2,
             xbs.leg_invoice_date AS invoice_date,
             xbs.leg_tax_code AS tax_code,
             xbs.leg_invoice_identifier AS invoice_identifier,
             xbs.leg_invoice_amount AS invoice_amount,
             xbs.leg_context3 AS context3,
             xbs.ussgl_transaction_code AS ussgl_transaction_code,
             xbs.descr_flex_error_message AS descr_flex_error_message,
             xbs.jgzz_recon_ref AS jgzz_recon_ref,
             xbs.average_journal_flag AS average_journal_flag,
             xbs.originating_bal_seg_value AS originating_bal_seg_value,
             xbs.reference_date AS reference_date,
             xbs.set_of_books_id AS set_of_books_id,
             xbs.balancing_segment_value AS balancing_segment_value,
             xbs.management_segment_value AS management_segment_value,
             xbs.funds_reserved_flag AS funds_reserved_flag
        FROM xxgl_balance_stg xbs
       WHERE xbs.process_flag = g_flag_v
         AND xbs.leg_entity = piv_entity
         AND xbs.batch_id = pin_batch_id
         AND xbs.run_sequence_id = pin_run_sequence_id;

    TYPE stg_recs_cur_type IS TABLE OF stg_recs_cur%ROWTYPE INDEX BY BINARY_INTEGER;
    l_stg_recs_cur_type stg_recs_cur_type;

    l_error_tab_type xxetn_common_error_pkg.g_source_tab_type;

  BEGIN
    debug('interface_data >>');
    pov_return_status := g_normal;
    l_error_message   := NULL;

--Added for v2.8 START
    BEGIN

      dbms_stats.gather_table_stats(ownname          => 'XXCONV',
                                    tabname          => 'XXGL_BALANCE_STG',
                                    cascade          => true,
                                    estimate_percent => 20);--dbms_stats.auto_sample_size,
                                    --degree           => dbms_stats.default_degree);

    END;
--Added for v2.8 END
    OPEN stg_recs_cur;
    LOOP
      FETCH stg_recs_cur BULK COLLECT
        INTO l_stg_recs_cur_type LIMIT g_bulk_limit;

      debug('l_stg_recs_cur_type.COUNT ' || l_stg_recs_cur_type.COUNT);

      IF l_stg_recs_cur_type.COUNT > 0 THEN
        BEGIN
          FORALL indx IN 1 .. l_stg_recs_cur_type.COUNT SAVE EXCEPTIONS
            INSERT INTO gl_interface
              (status,
               ledger_id,
               accounting_date,
               currency_code,
               date_created,
               created_by,
               actual_flag,
               user_je_category_name,
               user_je_source_name,
               currency_conversion_date,
               encumbrance_type_id,
               budget_version_id,
               user_currency_conversion_type,
               currency_conversion_rate,
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
               segment11,
               segment12,
               segment13,
               segment14,
               segment15,
               segment16,
               segment17,
               segment18,
               segment19,
               segment20,
               segment21,
               segment22,
               segment23,
               segment24,
               segment25,
               segment26,
               segment27,
               segment28,
               segment29,
               segment30,
               entered_dr,
               entered_cr,
               accounted_dr,
               accounted_cr,
               transaction_date,
               reference1,
               reference2,
               reference3,
               reference4,
               reference5,
               reference6,
               reference7,
               reference8,
               reference9,
               reference10,
               reference11,
               reference12,
               reference13,
               reference14,
               reference15,
               reference16,
               reference17,
               reference18,
               reference19,
               reference20,
               reference21,
               reference22,
               reference23,
               reference24,
               reference25,
               reference26,
               reference27,
               reference28,
               reference29,
               reference30,
               je_batch_id,
               period_name,
               je_header_id,
               je_line_num,
               chart_of_accounts_id,
               functional_currency_code,
               code_combination_id,
               date_created_in_gl,
               warning_code,
               status_description,
               stat_amount,
               group_id,
               request_id,
               subledger_doc_sequence_id,
               subledger_doc_sequence_value,
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
               attribute16,
               attribute17,
               attribute18,
               attribute19,
               attribute20,
               context,
               context2,
               invoice_date,
               tax_code,
               invoice_identifier,
               invoice_amount,
               context3,
               ussgl_transaction_code,
               descr_flex_error_message,
               jgzz_recon_ref,
               average_journal_flag,
               originating_bal_seg_value,
               gl_sl_link_id,
               gl_sl_link_table,
               reference_date,
               set_of_books_id,
               balancing_segment_value,
               management_segment_value,
               funds_reserved_flag)
            VALUES
              (l_stg_recs_cur_type(indx).status,
               l_stg_recs_cur_type(indx).set_of_books_id,
               l_stg_recs_cur_type(indx).accounting_date,
               l_stg_recs_cur_type(indx).currency_code,
               l_stg_recs_cur_type(indx).date_created,
               l_stg_recs_cur_type(indx).created_by,
               l_stg_recs_cur_type(indx).actual_flag,
               l_stg_recs_cur_type(indx).user_je_category_name,
               l_stg_recs_cur_type(indx).user_je_source_name,
               l_stg_recs_cur_type(indx).currency_conversion_date,
               NULL --encumbrance_type_id
              ,
               NULL --budget_version_id
              ,
               l_stg_recs_cur_type(indx).user_currency_conversion_type,
               l_stg_recs_cur_type(indx).currency_conversion_rate,
               l_stg_recs_cur_type(indx).segment1,
               l_stg_recs_cur_type(indx).segment2,
               l_stg_recs_cur_type(indx).segment3,
               l_stg_recs_cur_type(indx).segment4,
               l_stg_recs_cur_type(indx).segment5,
               l_stg_recs_cur_type(indx).segment6,
               l_stg_recs_cur_type(indx).segment7,
               l_stg_recs_cur_type(indx).segment8,
               l_stg_recs_cur_type(indx).segment9,
               l_stg_recs_cur_type(indx).segment10,
               NULL --segment11
              ,
               NULL --segment12
              ,
               NULL --segment13
              ,
               NULL --segment14
              ,
               NULL --segment15
              ,
               NULL --segment16
              ,
               NULL --segment17
              ,
               NULL --segment18
              ,
               NULL --segment19
              ,
               NULL --segment20
              ,
               NULL --segment21
              ,
               NULL --segment22
              ,
               NULL --segment23
              ,
               NULL --segment24
              ,
               NULL --segment25
              ,
               NULL --segment26
              ,
               NULL --segment27
              ,
               NULL --segment28
              ,
               NULL --segment29
              ,
               NULL --segment30
              ,
               l_stg_recs_cur_type(indx).entered_dr,
               l_stg_recs_cur_type(indx).entered_cr,
               l_stg_recs_cur_type(indx).accounted_dr,
               l_stg_recs_cur_type(indx).accounted_cr,
               l_stg_recs_cur_type(indx).transaction_date,
               l_stg_recs_cur_type(indx).reference1,
               l_stg_recs_cur_type(indx).reference2,
               l_stg_recs_cur_type(indx).reference3,
               l_stg_recs_cur_type(indx).reference4,
               l_stg_recs_cur_type(indx).reference5,
               l_stg_recs_cur_type(indx).reference6,
               l_stg_recs_cur_type(indx).reference7,
               l_stg_recs_cur_type(indx).reference8,
               l_stg_recs_cur_type(indx).reference9,
               l_stg_recs_cur_type(indx).reference10,
               l_stg_recs_cur_type(indx).reference11,
               l_stg_recs_cur_type(indx).reference12,
               l_stg_recs_cur_type(indx).reference13,
               l_stg_recs_cur_type(indx).reference14,
               l_stg_recs_cur_type(indx).reference15,
               l_stg_recs_cur_type(indx).reference16,
               l_stg_recs_cur_type(indx).reference17,
               l_stg_recs_cur_type(indx).reference18,
               NULL,--l_stg_recs_cur_type(indx).reference19,
               l_stg_recs_cur_type(indx).reference20,
               l_stg_recs_cur_type(indx).reference21,
               l_stg_recs_cur_type(indx).reference22,
               l_stg_recs_cur_type(indx).reference23,
               l_stg_recs_cur_type(indx).reference24,
               l_stg_recs_cur_type(indx).reference25,
               l_stg_recs_cur_type(indx).reference26,
               l_stg_recs_cur_type(indx).reference27,
               l_stg_recs_cur_type(indx).reference28,
               l_stg_recs_cur_type(indx).reference29,
               l_stg_recs_cur_type(indx).reference30,
               NULL --je_batch_id
              ,
               l_stg_recs_cur_type(indx).period_name,
               NULL --je_header_id
              ,
               l_stg_recs_cur_type(indx).je_line_num,
               l_stg_recs_cur_type(indx).chart_of_accounts_id,
               l_stg_recs_cur_type(indx).functional_currency_code,
               l_stg_recs_cur_type(indx).code_combination_id,
               l_stg_recs_cur_type(indx).date_created_in_gl,
               NULL --.warning_code
              ,
               l_stg_recs_cur_type(indx).status_description,
               l_stg_recs_cur_type(indx).stat_amount,
               l_stg_recs_cur_type(indx).group_id,
               NULL --.request_id
              ,
               l_stg_recs_cur_type(indx).subledger_doc_sequence_id,
               l_stg_recs_cur_type(indx).subledger_doc_sequence_value,
               DECODE(l_stg_recs_cur_type(indx).context,
                      g_flex_context,
                      DECODE(l_stg_recs_cur_type(indx).leg_source_system,
                             g_leg_source_issc,
                             l_stg_recs_cur_type(indx).attribute1,
                             NULL),
                      
                      NULL),
               DECODE(l_stg_recs_cur_type(indx).context,
                      g_flex_context,
                      DECODE(l_stg_recs_cur_type(indx).leg_source_system,
                             g_leg_source_issc,
                             l_stg_recs_cur_type(indx).attribute2,
                             NULL),
                      NULL),
               DECODE(l_stg_recs_cur_type(indx).context,
                      g_flex_context,
                      DECODE(l_stg_recs_cur_type(indx).leg_source_system,
                             g_leg_source_issc,
                             l_stg_recs_cur_type(indx).attribute3,
                             NULL),
                      NULL),
               DECODE(l_stg_recs_cur_type(indx).leg_source_system,
                      g_leg_source_fsc,
                      l_stg_recs_cur_type(indx).attribute4,
                      NULL),
               DECODE(l_stg_recs_cur_type(indx).leg_source_system,
                      g_leg_source_fsc,
                      l_stg_recs_cur_type(indx).attribute5,
                      NULL),
               DECODE(l_stg_recs_cur_type(indx).context,
                      g_flex_context1,                            --Added for v2.9
                           l_stg_recs_cur_type(indx).attribute6,  --Added for v2.9 batch_id
                      g_flex_context,
                      DECODE(l_stg_recs_cur_type(indx).leg_source_system,
                             g_leg_source_issc,
                             l_stg_recs_cur_type(indx).attribute6,
                             NULL),
                      NULL),
               DECODE(l_stg_recs_cur_type(indx).context,
                      g_flex_context1,                            -- Added for v2.9
                           l_stg_recs_cur_type(indx).attribute7,  -- Added for v2.9 header_id
                      g_flex_context,
                      DECODE(l_stg_recs_cur_type(indx).leg_source_system,
                             g_leg_source_issc,
                             l_stg_recs_cur_type(indx).attribute7,
                             NULL),
                      NULL),
               DECODE(l_stg_recs_cur_type(indx).context,
                      g_flex_context1,                            -- Added for v2.9
                           l_stg_recs_cur_type(indx).attribute8,  -- Added for v2.9 line num
                      g_flex_context,
                      DECODE(l_stg_recs_cur_type(indx).leg_source_system,
                             g_leg_source_issc,
                             l_stg_recs_cur_type(indx).attribute8,
                             NULL),
                      NULL),
               DECODE(l_stg_recs_cur_type(indx).context,
                      g_flex_context,
                      DECODE(l_stg_recs_cur_type(indx).leg_source_system,
                             g_leg_source_issc,
                             l_stg_recs_cur_type(indx).attribute9,
                             NULL),
                      NULL),
               DECODE(l_stg_recs_cur_type(indx).leg_source_system,
                      g_leg_source_fsc,
                      l_stg_recs_cur_type(indx).attribute10,
                      NULL),
               NULL --l_stg_recs_cur_type(indx).attribute11
              ,
               NULL --l_stg_recs_cur_type(indx).attribute12
              ,
               DECODE(l_stg_recs_cur_type(indx).leg_source_system,
                      g_leg_source_fsc,
                      l_stg_recs_cur_type(indx).attribute1,
                      NULL),
               DECODE(l_stg_recs_cur_type(indx).leg_source_system,
                      g_leg_source_fsc,
                      l_stg_recs_cur_type(indx).attribute9,
                      NULL),
               l_stg_recs_cur_type(indx).attribute15,
               l_stg_recs_cur_type(indx).attribute16,
               l_stg_recs_cur_type(indx).attribute17,
               l_stg_recs_cur_type(indx).attribute18,
               l_stg_recs_cur_type(indx).attribute19,
               l_stg_recs_cur_type(indx).attribute20,
               l_stg_recs_cur_type(indx).context,
               l_stg_recs_cur_type(indx).context2,
               l_stg_recs_cur_type(indx).invoice_date,
               l_stg_recs_cur_type(indx).tax_code,
               l_stg_recs_cur_type(indx).invoice_identifier,
               l_stg_recs_cur_type(indx).invoice_amount,
               l_stg_recs_cur_type(indx).context3,
               l_stg_recs_cur_type(indx).ussgl_transaction_code,
               l_stg_recs_cur_type(indx).descr_flex_error_message,
               l_stg_recs_cur_type(indx).jgzz_recon_ref,
               l_stg_recs_cur_type(indx).average_journal_flag,
               l_stg_recs_cur_type(indx).originating_bal_seg_value,
               NULL --gl_sl_link_id
              ,
               NULL --gl_sl_link_table
              ,
               l_stg_recs_cur_type(indx).reference_date,
               l_stg_recs_cur_type(indx).set_of_books_id,
               l_stg_recs_cur_type(indx).balancing_segment_value,
               l_stg_recs_cur_type(indx).management_segment_value,
               l_stg_recs_cur_type(indx).funds_reserved_flag);

          l_intf_count := l_intf_count + l_stg_recs_cur_type.COUNT;
        EXCEPTION
          WHEN g_bulk_exception THEN
            l_bluk_error_count := l_bluk_error_count +
                                  SQL%BULK_EXCEPTIONS.COUNT;
            FOR exep_indx IN 1 .. SQL%BULK_EXCEPTIONS.COUNT LOOP
              l_excep_col1    := l_stg_recs_cur_type(SQL%BULK_EXCEPTIONS(exep_indx).ERROR_INDEX)
                                 .interface_txn_id;
              l_error_message := SQLERRM(-1 * (SQL%BULK_EXCEPTIONS(exep_indx)
                                         .ERROR_CODE));

              debug('Record sequence : ' || l_excep_col1);
              debug('Error Message   : ' || l_error_message);

              --update process_flag to 'E' for error records
              UPDATE xxgl_balance_stg xber
                 SET xber.process_flag      = g_flag_e,
                     xber.error_type        = g_err_int,
                     xber.last_updated_date = SYSDATE,
                     xber.last_updated_by   = g_user_id,
                     xber.last_update_login = g_login_id,
                     xber.request_id        = g_request_id
               WHERE xber.interface_txn_id = TO_NUMBER(l_excep_col1);

              l_error_tab_type(exep_indx).source_table := g_source_table;
              l_error_tab_type(exep_indx).interface_staging_id := l_excep_col1;
              l_error_tab_type(exep_indx).source_keyname1 := NULL;
              l_error_tab_type(exep_indx).source_keyvalue1 := NULL;
              l_error_tab_type(exep_indx).source_keyname2 := NULL;
              l_error_tab_type(exep_indx).source_keyvalue2 := NULL;
              l_error_tab_type(exep_indx).source_keyname3 := NULL;
              l_error_tab_type(exep_indx).source_keyvalue3 := NULL;
              l_error_tab_type(exep_indx).source_keyname4 := NULL;
              l_error_tab_type(exep_indx).source_keyvalue4 := NULL;
              l_error_tab_type(exep_indx).source_keyname5 := NULL;
              l_error_tab_type(exep_indx).source_keyvalue5 := NULL;
              l_error_tab_type(exep_indx).source_column_name := NULL;
              l_error_tab_type(exep_indx).source_column_value := NULL;
              l_error_tab_type(exep_indx).error_type := g_err_validation;
              l_error_tab_type(exep_indx).error_code := 'ETN_BLKEXCEP_INTERFACE_DATA';
              l_error_tab_type(exep_indx).error_message := l_error_message;
              l_error_tab_type(exep_indx).severity := NULL;
              l_error_tab_type(exep_indx).proposed_solution := NULL;
              l_error_tab_type(exep_indx).stage := NULL; -- v2.3
              l_error_tab_type(exep_indx).interface_load_id := NULL; --- as per changes done by Sagar

              log_error(pin_batch_id        => NULL,
                        pin_run_sequence_id => NULL,
                        p_source_tab_type   => l_error_tab_type,
                        pov_return_status   => l_log_err_sts,
                        pov_error_message   => l_log_err_msg);
            END LOOP;
        END;
      END IF;

      COMMIT;
      EXIT WHEN stg_recs_cur%NOTFOUND;
    END LOOP;
    CLOSE stg_recs_cur;

    debug('No of Records Interfaced : ' || l_intf_count);
    debug('No of Records Errored    : ' || l_bluk_error_count);

    IF l_bluk_error_count > 0 THEN
      pov_return_status := g_warning;
      debug(l_bluk_error_count || ' records errored during bulk insert');
    END IF;

    --update successful records to 'P' in Staging Table
    UPDATE xxgl_balance_stg xbs
       SET xbs.process_flag           = DECODE(piv_entity,
                                               g_entity_foreignbalance,
                                               g_flag_t,
                                               g_flag_p),
           xbs.last_updated_date      = SYSDATE,
           xbs.last_updated_by        = g_user_id,
           xbs.last_update_login      = g_login_id,
           xbs.program_application_id = g_prog_appl_id,
           xbs.program_id             = g_program_id,
           xbs.program_update_date    = SYSDATE,
           xbs.request_id             = g_request_id
     WHERE xbs.process_flag = g_flag_v
       AND xbs.leg_entity = piv_entity
       AND xbs.batch_id = pin_batch_id
       AND xbs.run_sequence_id = pin_run_sequence_id;

    debug('Success records count ' || SQL%ROWCOUNT);

    COMMIT;
    debug('interface_data <<E');
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      l_error_message := SUBSTR('Exception in Procedure interface_data. ' ||
                                SQLERRM,
                                1,
                                1999);
      fnd_file.put_line(fnd_file.LOG, l_error_message);
      fnd_file.put_line(fnd_file.LOG, 'interface_data <<E');
      pov_return_status := g_error;
      pov_error_message := l_error_message;
  END interface_data;

  -- =============================================================================
  -- Procedure: interface_offset_balance
  -- =============================================================================
  -- Insert new records in Interface Table to offset Foreign Balance Functional Amounts
  -- =============================================================================
  --  Input Parameters :
  --    piv_entity           : Entity Name
  --    pin_batch_id         : Batch Id
  --    pin_run_sequence_id  : Run Sequence Id
  --  Output Parameters :
  --    pov_return_status    : Return Status - Normal/Warning/Error
  --    pov_error_message    : Error message in case of any failure
  -- -----------------------------------------------------------------------------
  PROCEDURE interface_offset_balance(piv_entity          IN VARCHAR2,
                                     pin_batch_id        IN NUMBER,
                                     pin_run_sequence_id IN NUMBER,
                                     pov_return_status   OUT NOCOPY VARCHAR2,
                                     pov_error_message   OUT NOCOPY VARCHAR2) IS
    PRAGMA AUTONOMOUS_TRANSACTION;

    l_error_message    VARCHAR2(2000) := NULL;
    l_excep_col1       VARCHAR2(100) := NULL;
    l_intf_count       NUMBER := 0;
    l_bulk_error_count NUMBER := 0;

    l_log_err_sts VARCHAR2(2000) := NULL;
    l_log_err_msg VARCHAR2(2000) := NULL;

    /* Cursor to get interfaced data from Staging table */
    CURSOR stg_recs_cur IS
      SELECT xbs.leg_source_system     AS leg_source_system,
             xbs.status                AS status,
             xbs.accounting_date       AS accounting_date,
             xbs.leg_currency_code     AS currency_code,
             xbs.creation_date         AS date_created,
             xbs.created_by            AS created_by,
             xbs.leg_actual_flag       AS actual_flag,
             xbs.user_je_category_name AS user_je_category_name,
             xbs.user_je_source_name   AS user_je_source_name
             --,  xbs.leg_curr_conv_date            AS currency_conversion_date
             --,  xbs.leg_user_curr_conv_type       AS user_currency_conversion_type
             --,  xbs.leg_curr_con_rate             AS currency_conversion_rate
            ,
             NULL AS currency_conversion_date,
             NULL AS user_currency_conversion_type,
             NULL AS currency_conversion_rate,
             xbs.segment1 AS segment1,
             xbs.segment2 AS segment2,
             xbs.segment3 AS segment3,
             xbs.segment4 AS segment4,
             xbs.segment5 AS segment5,
             xbs.segment6 AS segment6,
             xbs.segment7 AS segment7,
             xbs.segment8 AS segment8,
             xbs.segment9 AS segment9,
             xbs.segment10 AS segment10,
             DECODE(SIGN((xbs.leg_begin_balance_dr_beq -
                         xbs.leg_begin_balance_cr_beq) +
                         (xbs.leg_period_net_dr_beq -
                         xbs.leg_period_net_cr_beq)),
                    -1,
                    (xbs.leg_begin_balance_cr_beq -
                    xbs.leg_begin_balance_dr_beq) +
                    (xbs.leg_period_net_cr_beq - xbs.leg_period_net_dr_beq),
                    0) AS entered_dr -- v2.2
            ,
             DECODE(SIGN((xbs.leg_begin_balance_dr_beq -
                         xbs.leg_begin_balance_cr_beq) +
                         (xbs.leg_period_net_dr_beq -
                         xbs.leg_period_net_cr_beq)),
                    +1,
                    (xbs.leg_begin_balance_dr_beq -
                    xbs.leg_begin_balance_cr_beq) +
                    (xbs.leg_period_net_dr_beq - xbs.leg_period_net_cr_beq),
                    0) AS entered_cr -- v2.2
            ,
             DECODE(SIGN((xbs.leg_begin_balance_dr_beq -
                         xbs.leg_begin_balance_cr_beq) +
                         (xbs.leg_period_net_dr_beq -
                         xbs.leg_period_net_cr_beq)),
                    -1,
                    (xbs.leg_begin_balance_cr_beq -
                    xbs.leg_begin_balance_dr_beq) +
                    (xbs.leg_period_net_cr_beq - xbs.leg_period_net_dr_beq),
                    0) AS accounted_dr -- v2.2
            ,
             DECODE(SIGN((xbs.leg_begin_balance_dr_beq -
                         xbs.leg_begin_balance_cr_beq) +
                         (xbs.leg_period_net_dr_beq -
                         xbs.leg_period_net_cr_beq)),
                    +1,
                    (xbs.leg_begin_balance_dr_beq -
                    xbs.leg_begin_balance_cr_beq) +
                    (xbs.leg_period_net_dr_beq - xbs.leg_period_net_cr_beq),
                    0) AS accounted_cr -- v2.2
            ,
             xbs.leg_transaction_date AS transaction_date,
             xbs.leg_source_system || g_char_hyphen ||
             'FX Offset Conversion Balances' || g_char_hyphen ||
             xbs.period_name || g_char_hyphen || xbs.leg_currency_code ||
             g_char_hyphen || xbs.leg_sob_short_name AS reference1,
             'FX Offset Conversion Balances' || g_char_hyphen ||
             xbs.period_name AS reference2,
             xbs.leg_reference3 AS reference3,
             xbs.leg_source_system || g_char_hyphen ||
             'FX Offset Conversion Balances' || g_char_hyphen ||
             xbs.Period_name || g_char_hyphen || xbs.leg_currency_code ||
             g_char_hyphen || xbs.leg_sob_short_name AS reference4,
             'FX Offset Conversion Balances' || g_char_hyphen ||
             xbs.period_name AS reference5,
             xbs.leg_reference6 AS reference6,
             xbs.leg_reference7 AS reference7,
             xbs.leg_reference8 AS reference8,
             xbs.leg_reference9 AS reference9,
             xbs.leg_reference10 AS reference10,
             xbs.leg_reference11 AS reference11,
             xbs.leg_reference12 AS reference12,
             xbs.leg_reference13 AS reference13,
             xbs.leg_reference14 AS reference14,
             xbs.leg_reference15 AS reference15,
             xbs.leg_reference16 AS reference16,
             xbs.leg_reference17 AS reference17,
             xbs.leg_reference18 AS reference18,
             xbs.leg_reference19 AS reference19,
             xbs.leg_reference20 AS reference20,
             xbs.leg_reference21 AS reference21,
             xbs.leg_reference22 AS reference22,
             xbs.leg_reference23 AS reference23,
             xbs.leg_reference24 AS reference24,
             xbs.leg_reference25 AS reference25,
             xbs.leg_reference26 AS reference26,
             xbs.leg_reference27 AS reference27,
             xbs.leg_reference28 AS reference28,
             xbs.leg_reference29 AS reference29,
             xbs.leg_reference30 AS reference30,
             xbs.period_name AS period_name,
             xbs.leg_je_line_num AS je_line_num,
             xbs.chart_of_accounts_id AS chart_of_accounts_id,
             xbs.functional_currency_code AS functional_currency_code,
             xbs.code_combination_id AS code_combination_id,
             xbs.leg_date_created_in_gl AS date_created_in_gl,
             xbs.status_description AS status_description,
             xbs.stat_amount AS stat_amount,
             xbs.group_id AS group_id,
             xbs.subledger_doc_sequence_id AS subledger_doc_sequence_id,
             xbs.subledger_doc_sequence_value AS subledger_doc_sequence_value,
             xbs.interface_txn_id AS interface_txn_id,
             xbs.leg_attribute1 AS attribute1,
             xbs.leg_attribute2 AS attribute2,
             xbs.leg_attribute3 AS attribute3,
             xbs.leg_attribute4 AS attribute4,
             xbs.leg_attribute5 AS attribute5,
             xbs.leg_attribute6 AS attribute6,
             xbs.leg_attribute7 AS attribute7,
             xbs.leg_attribute8 AS attribute8,
             xbs.leg_attribute9 AS attribute9,
             xbs.leg_attribute10 AS attribute10,
             xbs.leg_attribute11 AS attribute11,
             xbs.leg_attribute12 AS attribute12,
             xbs.leg_attribute13 AS attribute13,
             xbs.leg_attribute14 AS attribute14,
             xbs.leg_attribute15 AS attribute15,
             xbs.leg_attribute16 AS attribute16,
             xbs.leg_attribute17 AS attribute17,
             xbs.leg_attribute18 AS attribute18,
             xbs.leg_attribute19 AS attribute19,
             xbs.leg_attribute20 AS attribute20,
             xbs.leg_context AS context,
             xbs.leg_context2 AS context2,
             xbs.leg_invoice_date AS invoice_date,
             xbs.leg_tax_code AS tax_code,
             xbs.leg_invoice_identifier AS invoice_identifier,
             xbs.leg_invoice_amount AS invoice_amount,
             xbs.leg_context3 AS context3,
             xbs.ussgl_transaction_code AS ussgl_transaction_code,
             xbs.descr_flex_error_message AS descr_flex_error_message,
             xbs.jgzz_recon_ref AS jgzz_recon_ref,
             xbs.average_journal_flag AS average_journal_flag,
             xbs.originating_bal_seg_value AS originating_bal_seg_value,
             xbs.reference_date AS reference_date,
             xbs.set_of_books_id AS set_of_books_id,
             xbs.balancing_segment_value AS balancing_segment_value,
             xbs.management_segment_value AS management_segment_value,
             xbs.funds_reserved_flag AS funds_reserved_flag
        FROM xxgl_balance_stg xbs
       WHERE xbs.process_flag = g_flag_t
         AND xbs.leg_entity = piv_entity
         AND xbs.batch_id = pin_batch_id
         AND xbs.run_sequence_id = pin_run_sequence_id
         AND xbs.request_id = g_request_id;

    TYPE stg_recs_cur_type IS TABLE OF stg_recs_cur%ROWTYPE INDEX BY BINARY_INTEGER;
    l_stg_recs_cur_type stg_recs_cur_type;

    l_error_tab_type xxetn_common_error_pkg.g_source_tab_type;

  BEGIN
    debug('interface_offset_balance >>');
    pov_return_status := g_normal;
    l_error_message   := NULL;

    OPEN stg_recs_cur;
    LOOP
      FETCH stg_recs_cur BULK COLLECT
        INTO l_stg_recs_cur_type LIMIT g_bulk_limit;

      debug('l_stg_recs_cur_type.COUNT ' || l_stg_recs_cur_type.COUNT);

      IF l_stg_recs_cur_type.COUNT > 0 THEN
        BEGIN
          FORALL indx IN 1 .. l_stg_recs_cur_type.COUNT SAVE EXCEPTIONS
            INSERT INTO gl_interface
              (status,
               ledger_id,
               accounting_date,
               currency_code,
               date_created,
               created_by,
               actual_flag,
               user_je_category_name,
               user_je_source_name,
               currency_conversion_date,
               encumbrance_type_id,
               budget_version_id,
               user_currency_conversion_type,
               currency_conversion_rate,
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
               segment11,
               segment12,
               segment13,
               segment14,
               segment15,
               segment16,
               segment17,
               segment18,
               segment19,
               segment20,
               segment21,
               segment22,
               segment23,
               segment24,
               segment25,
               segment26,
               segment27,
               segment28,
               segment29,
               segment30,
               entered_dr,
               entered_cr,
               accounted_dr,
               accounted_cr,
               transaction_date,
               reference1,
               reference2,
               reference3,
               reference4,
               reference5,
               reference6,
               reference7,
               reference8,
               reference9,
               reference10,
               reference11,
               reference12,
               reference13,
               reference14,
               reference15,
               reference16,
               reference17,
               reference18,
               reference19,
               reference20,
               reference21,
               reference22,
               reference23,
               reference24,
               reference25,
               reference26,
               reference27,
               reference28,
               reference29,
               reference30,
               je_batch_id,
               period_name,
               je_header_id,
               je_line_num,
               chart_of_accounts_id,
               functional_currency_code,
               code_combination_id,
               date_created_in_gl,
               warning_code,
               status_description,
               stat_amount,
               group_id,
               request_id,
               subledger_doc_sequence_id,
               subledger_doc_sequence_value,
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
               attribute16,
               attribute17,
               attribute18,
               attribute19,
               attribute20,
               context,
               context2,
               invoice_date,
               tax_code,
               invoice_identifier,
               invoice_amount,
               context3,
               ussgl_transaction_code,
               descr_flex_error_message,
               jgzz_recon_ref,
               average_journal_flag,
               originating_bal_seg_value,
               gl_sl_link_id,
               gl_sl_link_table,
               reference_date,
               set_of_books_id,
               balancing_segment_value,
               management_segment_value,
               funds_reserved_flag)
            VALUES
              (l_stg_recs_cur_type(indx).status,
               l_stg_recs_cur_type(indx).set_of_books_id,
               l_stg_recs_cur_type(indx).accounting_date,
               l_stg_recs_cur_type(indx).functional_currency_code -- currency code same as functional currency code
              ,
               l_stg_recs_cur_type(indx).date_created,
               l_stg_recs_cur_type(indx).created_by,
               l_stg_recs_cur_type(indx).actual_flag,
               l_stg_recs_cur_type(indx).user_je_category_name,
               l_stg_recs_cur_type(indx).user_je_source_name,
               l_stg_recs_cur_type(indx).currency_conversion_date,
               NULL --encumbrance_type_id
              ,
               NULL --budget_version_id
              ,
               l_stg_recs_cur_type(indx).user_currency_conversion_type,
               l_stg_recs_cur_type(indx).currency_conversion_rate,
               l_stg_recs_cur_type(indx).segment1,
               l_stg_recs_cur_type(indx).segment2,
               l_stg_recs_cur_type(indx).segment3,
               l_stg_recs_cur_type(indx).segment4,
               l_stg_recs_cur_type(indx).segment5,
               l_stg_recs_cur_type(indx).segment6,
               l_stg_recs_cur_type(indx).segment7,
               l_stg_recs_cur_type(indx).segment8,
               l_stg_recs_cur_type(indx).segment9,
               l_stg_recs_cur_type(indx).segment10,
               NULL --segment11
              ,
               NULL --segment12
              ,
               NULL --segment13
              ,
               NULL --segment14
              ,
               NULL --segment15
              ,
               NULL --segment16
              ,
               NULL --segment17
              ,
               NULL --segment18
              ,
               NULL --segment19
              ,
               NULL --segment20
              ,
               NULL --segment21
              ,
               NULL --segment22
              ,
               NULL --segment23
              ,
               NULL --segment24
              ,
               NULL --segment25
              ,
               NULL --segment26
              ,
               NULL --segment27
              ,
               NULL --segment28
              ,
               NULL --segment29
              ,
               NULL --segment30
              ,
               l_stg_recs_cur_type(indx).entered_dr,
               l_stg_recs_cur_type(indx).entered_cr,
               l_stg_recs_cur_type(indx).accounted_dr,
               l_stg_recs_cur_type(indx).accounted_cr,
               l_stg_recs_cur_type(indx).transaction_date,
               l_stg_recs_cur_type(indx).reference1,
               l_stg_recs_cur_type(indx).reference2,
               l_stg_recs_cur_type(indx).reference3,
               l_stg_recs_cur_type(indx).reference4,
               l_stg_recs_cur_type(indx).reference5,
               l_stg_recs_cur_type(indx).reference6,
               l_stg_recs_cur_type(indx).reference7,
               l_stg_recs_cur_type(indx).reference8,
               l_stg_recs_cur_type(indx).reference9,
               l_stg_recs_cur_type(indx).reference10,
               l_stg_recs_cur_type(indx).reference11,
               l_stg_recs_cur_type(indx).reference12,
               l_stg_recs_cur_type(indx).reference13,
               l_stg_recs_cur_type(indx).reference14,
               l_stg_recs_cur_type(indx).reference15,
               l_stg_recs_cur_type(indx).reference16,
               l_stg_recs_cur_type(indx).reference17,
               l_stg_recs_cur_type(indx).reference18,
               NULL,--l_stg_recs_cur_type(indx).reference19,
               l_stg_recs_cur_type(indx).reference20,
               l_stg_recs_cur_type(indx).reference21,
               l_stg_recs_cur_type(indx).reference22,
               l_stg_recs_cur_type(indx).reference23,
               l_stg_recs_cur_type(indx).reference24,
               l_stg_recs_cur_type(indx).reference25,
               l_stg_recs_cur_type(indx).reference26,
               l_stg_recs_cur_type(indx).reference27,
               l_stg_recs_cur_type(indx).reference28,
               l_stg_recs_cur_type(indx).reference29,
               l_stg_recs_cur_type(indx).reference30,
               NULL --je_batch_id
              ,
               l_stg_recs_cur_type(indx).period_name,
               NULL --je_header_id
              ,
               l_stg_recs_cur_type(indx).je_line_num,
               l_stg_recs_cur_type(indx).chart_of_accounts_id,
               l_stg_recs_cur_type(indx).functional_currency_code,
               l_stg_recs_cur_type(indx).code_combination_id,
               l_stg_recs_cur_type(indx).date_created_in_gl,
               NULL --.warning_code
              ,
               l_stg_recs_cur_type(indx).status_description,
               l_stg_recs_cur_type(indx).stat_amount,
               l_stg_recs_cur_type(indx).group_id,
               NULL --.request_id
              ,
               l_stg_recs_cur_type(indx).subledger_doc_sequence_id,
               l_stg_recs_cur_type(indx).subledger_doc_sequence_value,
               DECODE(l_stg_recs_cur_type(indx).context,
                      g_flex_context,
                      DECODE(l_stg_recs_cur_type(indx).leg_source_system,
                             g_leg_source_issc,
                             l_stg_recs_cur_type(indx).attribute1,
                             NULL),
                      NULL),
               DECODE(l_stg_recs_cur_type(indx).context,
                      g_flex_context,
                      DECODE(l_stg_recs_cur_type(indx).leg_source_system,
                             g_leg_source_issc,
                             l_stg_recs_cur_type(indx).attribute2,
                             NULL),
                      NULL),
               DECODE(l_stg_recs_cur_type(indx).context,
                      g_flex_context,
                      DECODE(l_stg_recs_cur_type(indx).leg_source_system,
                             g_leg_source_issc,
                             l_stg_recs_cur_type(indx).attribute3,
                             NULL),
                      NULL),
               DECODE(l_stg_recs_cur_type(indx).leg_source_system,
                      g_leg_source_fsc,
                      l_stg_recs_cur_type(indx).attribute4,
                      NULL),
               DECODE(l_stg_recs_cur_type(indx).leg_source_system,
                      g_leg_source_fsc,
                      l_stg_recs_cur_type(indx).attribute5,
                      NULL),
               DECODE(l_stg_recs_cur_type(indx).context,
                      g_flex_context,
                      DECODE(l_stg_recs_cur_type(indx).leg_source_system,
                             g_leg_source_issc,
                             l_stg_recs_cur_type(indx).attribute6,
                             NULL),
                      NULL),
               DECODE(l_stg_recs_cur_type(indx).context,
                      g_flex_context,
                      DECODE(l_stg_recs_cur_type(indx).leg_source_system,
                             g_leg_source_issc,
                             l_stg_recs_cur_type(indx).attribute7,
                             NULL),
                      NULL),
               DECODE(l_stg_recs_cur_type(indx).context,
                      g_flex_context,
                      DECODE(l_stg_recs_cur_type(indx).leg_source_system,
                             g_leg_source_issc,
                             l_stg_recs_cur_type(indx).attribute8,
                             NULL),
                      NULL),
               DECODE(l_stg_recs_cur_type(indx).context,
                      g_flex_context,
                      DECODE(l_stg_recs_cur_type(indx).leg_source_system,
                             g_leg_source_issc,
                             l_stg_recs_cur_type(indx).attribute9,
                             NULL),
                      NULL),
               DECODE(l_stg_recs_cur_type(indx).leg_source_system,
                      g_leg_source_fsc,
                      l_stg_recs_cur_type(indx).attribute10,
                      NULL),
               NULL --l_stg_recs_cur_type(indx).attribute11
              ,
               NULL --l_stg_recs_cur_type(indx).attribute12
              ,
               DECODE(l_stg_recs_cur_type(indx).leg_source_system,
                      g_leg_source_fsc,
                      l_stg_recs_cur_type(indx).attribute1,
                      NULL),
               DECODE(l_stg_recs_cur_type(indx).leg_source_system,
                      g_leg_source_fsc,
                      l_stg_recs_cur_type(indx).attribute9,
                      NULL),
               l_stg_recs_cur_type(indx).attribute15,
               l_stg_recs_cur_type(indx).attribute16,
               l_stg_recs_cur_type(indx).attribute17,
               l_stg_recs_cur_type(indx).attribute18,
               l_stg_recs_cur_type(indx).attribute19,
               l_stg_recs_cur_type(indx).attribute20,
               l_stg_recs_cur_type(indx).context,
               l_stg_recs_cur_type(indx).context2,
               l_stg_recs_cur_type(indx).invoice_date,
               l_stg_recs_cur_type(indx).tax_code,
               l_stg_recs_cur_type(indx).invoice_identifier,
               l_stg_recs_cur_type(indx).invoice_amount,
               l_stg_recs_cur_type(indx).context3,
               l_stg_recs_cur_type(indx).ussgl_transaction_code,
               l_stg_recs_cur_type(indx).descr_flex_error_message,
               l_stg_recs_cur_type(indx).jgzz_recon_ref,
               l_stg_recs_cur_type(indx).average_journal_flag,
               l_stg_recs_cur_type(indx).originating_bal_seg_value,
               NULL --gl_sl_link_id
              ,
               NULL --gl_sl_link_table
              ,
               l_stg_recs_cur_type(indx).reference_date,
               l_stg_recs_cur_type(indx).set_of_books_id,
               l_stg_recs_cur_type(indx).balancing_segment_value,
               l_stg_recs_cur_type(indx).management_segment_value,
               l_stg_recs_cur_type(indx).funds_reserved_flag);

          l_intf_count := l_intf_count + l_stg_recs_cur_type.COUNT;
        EXCEPTION
          WHEN g_bulk_exception THEN
            l_bulk_error_count := l_bulk_error_count +
                                  SQL%BULK_EXCEPTIONS.COUNT;
            FOR exep_indx IN 1 .. SQL%BULK_EXCEPTIONS.COUNT LOOP
              l_excep_col1    := l_stg_recs_cur_type(SQL%BULK_EXCEPTIONS(exep_indx).ERROR_INDEX)
                                 .interface_txn_id;
              l_error_message := SQLERRM(-1 * (SQL%BULK_EXCEPTIONS(exep_indx)
                                         .ERROR_CODE));

              debug('Record sequence : ' || l_excep_col1);
              debug('Error Message   : ' || l_error_message);

              --update process_flag to 'E' for error records
              UPDATE xxgl_balance_stg xber
                 SET xber.process_flag      = g_flag_e,
                     xber.error_type        = g_err_int,
                     xber.last_updated_date = SYSDATE,
                     xber.last_updated_by   = g_user_id,
                     xber.last_update_login = g_login_id,
                     xber.request_id        = g_request_id
               WHERE xber.interface_txn_id = TO_NUMBER(l_excep_col1);

              l_error_tab_type(exep_indx).source_table := g_source_table;
              l_error_tab_type(exep_indx).interface_staging_id := l_excep_col1;
              l_error_tab_type(exep_indx).source_keyname1 := NULL;
              l_error_tab_type(exep_indx).source_keyvalue1 := NULL;
              l_error_tab_type(exep_indx).source_keyname2 := NULL;
              l_error_tab_type(exep_indx).source_keyvalue2 := NULL;
              l_error_tab_type(exep_indx).source_keyname3 := NULL;
              l_error_tab_type(exep_indx).source_keyvalue3 := NULL;
              l_error_tab_type(exep_indx).source_keyname4 := NULL;
              l_error_tab_type(exep_indx).source_keyvalue4 := NULL;
              l_error_tab_type(exep_indx).source_keyname5 := NULL;
              l_error_tab_type(exep_indx).source_keyvalue5 := NULL;
              l_error_tab_type(exep_indx).source_column_name := NULL;
              l_error_tab_type(exep_indx).source_column_value := NULL;
              l_error_tab_type(exep_indx).error_type := g_err_validation;
              l_error_tab_type(exep_indx).error_code := 'ETN_OFFSET_INTERFACE_DATA';
              l_error_tab_type(exep_indx).error_message := l_error_message;
              l_error_tab_type(exep_indx).severity := NULL;
              l_error_tab_type(exep_indx).proposed_solution := NULL;
              l_error_tab_type(exep_indx).stage := NULL; -- v2.3
              l_error_tab_type(exep_indx).stage := NULL; ---as per changes done by Sagar

              log_error(pin_batch_id        => NULL,
                        pin_run_sequence_id => NULL,
                        p_source_tab_type   => l_error_tab_type,
                        pov_return_status   => l_log_err_sts,
                        pov_error_message   => l_log_err_msg);
            END LOOP;
        END;
      END IF;

      COMMIT;
      EXIT WHEN stg_recs_cur%NOTFOUND;
    END LOOP;
    CLOSE stg_recs_cur;

    debug('No of Records Interfaced : ' || l_intf_count);
    debug('No of Records Errored    : ' || l_bulk_error_count);

    IF l_bulk_error_count > 0 THEN
      pov_return_status := g_warning;
      debug(l_bulk_error_count || ' records errored during bulk insert');
    END IF;

    --Update successful records to 'P' in Staging Table
    UPDATE xxgl_balance_stg xbs
       SET xbs.process_flag           = g_flag_p,
           xbs.last_updated_date      = SYSDATE,
           xbs.last_updated_by        = g_user_id,
           xbs.last_update_login      = g_login_id,
           xbs.program_application_id = g_prog_appl_id,
           xbs.program_id             = g_program_id,
           xbs.program_update_date    = SYSDATE,
           xbs.request_id             = g_request_id
     WHERE xbs.process_flag = g_flag_t
       AND xbs.leg_entity = piv_entity
       AND xbs.batch_id = pin_batch_id
       AND xbs.run_sequence_id = pin_run_sequence_id;

    debug('Success records count ' || SQL%ROWCOUNT);

    COMMIT;
    debug('interface_offset_balance <<E');
  EXCEPTION
    WHEN OTHERS THEN
      l_error_message := SUBSTR('Exception in Procedure interface_offset_balance. ' ||
                                SQLERRM,
                                1,
                                1999);
      fnd_file.put_line(fnd_file.LOG, l_error_message);
      fnd_file.put_line(fnd_file.LOG, 'interface_offset_balance <<E');
      pov_return_status := g_error;
      pov_error_message := l_error_message;
  END interface_offset_balance;

  -- =============================================================================
  -- Procedure: get_code_combination_id
  -- =============================================================================
  -- To get code_combination_id based on segment values/concetenated segment
  -- =============================================================================
  --  Input Parameters :
  --    piv_segment1         : R12 Segment1 value
  --    piv_segment2         : R12 Segment2 value
  --    piv_segment3         : R12 Segment3 value
  --    piv_segment4         : R12 Segment4 value
  --    piv_segment5         : R12 Segment5 value
  --    piv_segment6         : R12 Segment6 value
  --    piv_segment7         : R12 Segment7 value
  --    piv_segment8         : R12 Segment8 value
  --    piv_segment9         : R12 Segment9 value
  --    piv_segment10        : R12 Segment10 value
  --    piv_concat_segment   : Concatenated segment value
  -- -----------------------------------------------------------------------------
  PROCEDURE get_code_combination_id(piv_entity          IN VARCHAR2,
                                    pin_batch_id        IN NUMBER,
                                    pin_run_sequence_id IN NUMBER,
                                    pov_return_status   OUT NOCOPY VARCHAR2,
                                    pov_error_message   OUT NOCOPY VARCHAR2) IS
    l_bluk_error_count NUMBER := 0;
    l_excep_col1       VARCHAR2(2000);
    l_error_message    VARCHAR2(2000);

    l_11i_coa_rec xxetn_coa_mapping_pkg.g_coa_rec_type;
    l_r12_coa_rec xxetn_coa_mapping_pkg.g_coa_rec_type;
    l_coa_rec     xxetn_common_pkg.g_rec_type; ---changed
    l_msg         VARCHAR2(3000);
    l_status      VARCHAR2(50);
    l_acct_date   DATE;

    CURSOR coa_recs_cur IS
      SELECT xbs.leg_segment1,
             xbs.leg_segment2,
             xbs.leg_segment3,
             xbs.leg_segment4,
             xbs.leg_segment5,
             xbs.leg_segment6,
             xbs.leg_segment7,
             xbs.accounting_date -- v2.1
            ,
             NULL                AS segment1,
             NULL                AS segment2,
             NULL                AS segment3,
             NULL                AS segment4,
             NULL                AS segment5,
             NULL                AS segment6,
             NULL                AS segment7,
             NULL                AS segment8,
             NULL                AS segment9,
             NULL                AS segment10,
             NULL                AS ccid
        FROM xxgl_balance_stg xbs
       WHERE xbs.leg_entity = piv_entity
         AND xbs.batch_id = pin_batch_id
         AND xbs.run_sequence_id = pin_run_sequence_id
       GROUP BY xbs.leg_segment1,
                xbs.leg_segment2,
                xbs.leg_segment3,
                xbs.leg_segment4,
                xbs.leg_segment5,
                xbs.leg_segment6,
                xbs.leg_segment7,
                xbs.accounting_date;

    TYPE coa_recs_cur_type IS TABLE OF coa_recs_cur%ROWTYPE INDEX BY BINARY_INTEGER;
    l_coa_recs_cur_type coa_recs_cur_type;

  BEGIN
    pov_return_status := g_normal;
    pov_error_message := NULL;

--Added for v2.8 START
    BEGIN

      dbms_stats.gather_table_stats(ownname          => 'XXCONV',
                                    tabname          => 'XXGL_BALANCE_STG',
                                    cascade          => true,
                                    estimate_percent => 20);--dbms_stats.auto_sample_size,
                                    --degree           => dbms_stats.default_degree);

    END;
--Added for v2.8 END
    OPEN coa_recs_cur;
    LOOP
      FETCH coa_recs_cur BULK COLLECT
        INTO l_coa_recs_cur_type LIMIT g_bulk_limit;
      EXIT WHEN l_coa_recs_cur_type.COUNT = 0;
      IF l_coa_recs_cur_type.COUNT > 0 THEN
        FOR indx IN 1 .. l_coa_recs_cur_type.COUNT LOOP
          l_11i_coa_rec.segment1  := l_coa_recs_cur_type(indx).leg_segment1;
          l_11i_coa_rec.segment2  := l_coa_recs_cur_type(indx).leg_segment2;
          l_11i_coa_rec.segment3  := l_coa_recs_cur_type(indx).leg_segment3;
          l_11i_coa_rec.segment4  := l_coa_recs_cur_type(indx).leg_segment4;
          l_11i_coa_rec.segment5  := l_coa_recs_cur_type(indx).leg_segment5;
          l_11i_coa_rec.segment6  := l_coa_recs_cur_type(indx).leg_segment6;
          l_11i_coa_rec.segment7  := l_coa_recs_cur_type(indx).leg_segment7;
          l_11i_coa_rec.segment8  := NULL;
          l_11i_coa_rec.segment9  := NULL;
          l_11i_coa_rec.segment10 := NULL;
/*         fnd_file.put_line(fnd_file.LOG, 'before calling the get_code_combinatin');
*/          xxetn_coa_mapping_pkg.get_code_combination(p_direction           => 'LEGACY-TO-R12',
                                                     p_external_system     => NULL,
                                                     p_transformation_date => l_coa_recs_cur_type(indx)
                                                                              .accounting_date -- v2.1
                                                    ,
                                                     p_coa_input           => l_11i_coa_rec,
                                                     p_coa_output          => l_r12_coa_rec,
                                                     p_out_message         => l_msg,
                                                     p_out_status          => l_status);

          debug('10 segments ' || l_r12_coa_rec.segment1 || g_char_period ||
                l_r12_coa_rec.segment2 || g_char_period ||
                l_r12_coa_rec.segment3 || g_char_period ||
                l_r12_coa_rec.segment4 || g_char_period ||
                l_r12_coa_rec.segment5 || g_char_period ||
                l_r12_coa_rec.segment6 || g_char_period ||
                l_r12_coa_rec.segment7 || g_char_period ||
                l_r12_coa_rec.segment8 || g_char_period ||
                l_r12_coa_rec.segment9 || g_char_period ||
                l_r12_coa_rec.segment10 || ' / l_status ' || l_status);

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
                                      p_ccid        => l_coa_recs_cur_type(indx).ccid,
                                      p_err_msg     => l_msg);

            debug('ccid ' || l_coa_recs_cur_type(indx).ccid || ' / l_msg ' ||
                  l_msg);

            IF l_coa_recs_cur_type(indx).ccid IS NOT NULL THEN
              l_coa_recs_cur_type(indx).segment1 := l_r12_coa_rec.segment1;
              l_coa_recs_cur_type(indx).segment2 := l_r12_coa_rec.segment2;
              l_coa_recs_cur_type(indx).segment3 := l_r12_coa_rec.segment3;
              l_coa_recs_cur_type(indx).segment4 := l_r12_coa_rec.segment4;
              l_coa_recs_cur_type(indx).segment5 := l_r12_coa_rec.segment5;
              l_coa_recs_cur_type(indx).segment6 := l_r12_coa_rec.segment6;
              l_coa_recs_cur_type(indx).segment7 := l_r12_coa_rec.segment7;
              l_coa_recs_cur_type(indx).segment8 := l_r12_coa_rec.segment8;
              l_coa_recs_cur_type(indx).segment9 := l_r12_coa_rec.segment9;
              l_coa_recs_cur_type(indx).segment10 := l_r12_coa_rec.segment10;
            ELSE

/*             fnd_file.put_line(fnd_file.LOG, 'first else');
*/              ---update the staging table for the error records where ccid was not derived

              UPDATE xxgl_balance_stg  --    v2.5
                 SET leg_reference19 = SUBSTR(l_msg, 1, 100)
               WHERE  NVL(accounting_date,TO_DATE('03-12-4172','MM/DD/YYYY')) = nvl(l_coa_recs_cur_type(indx).accounting_date,TO_DATE('03-12-4172','MM/DD/YYYY'))
                    /*nvl(accounting_date,'$') = nvl(l_coa_recs_cur_type(indx)
                  .accounting_date,NVL(accounting_date,'$'))*/
                 AND leg_segment1 = l_11i_coa_rec.segment1
                 AND leg_segment2 = l_11i_coa_rec.segment2
                 AND leg_segment3 = l_11i_coa_rec.segment3
                 AND leg_segment4 = l_11i_coa_rec.segment4
                 AND leg_segment5 = l_11i_coa_rec.segment5
                 AND leg_segment6 = l_11i_coa_rec.segment6
                 AND leg_segment7 = l_11i_coa_rec.segment7
                 AND leg_entity = piv_entity
                 AND batch_id = pin_batch_id
                 AND run_sequence_id = pin_run_sequence_id
                 AND code_combination_id IS NULL;
            END IF;

          ELSE




            --UPDATE THE STAGIBG TABLE WHERE THE R12 SEGMENTS ARE NOT DERIVED
            UPDATE xxgl_balance_stg   --    v2.5
               SET leg_reference19 = SUBSTR(l_msg, 1, 100)
             WHERE NVL(accounting_date,TO_DATE('03-12-4172','MM/DD/YYYY')) = nvl(l_coa_recs_cur_type(indx).accounting_date,TO_DATE('03-12-4172','MM/DD/YYYY'))

            /* nvl(accounting_date,'$') = nvl(l_coa_recs_cur_type(indx)
                  .accounting_date,NVL(accounting_date,'$'))*/
               AND leg_segment1 = l_11i_coa_rec.segment1
               AND leg_segment2 = l_11i_coa_rec.segment2
               AND leg_segment3 = l_11i_coa_rec.segment3
               AND leg_segment4 = l_11i_coa_rec.segment4
               AND leg_segment5 = l_11i_coa_rec.segment5
               AND leg_segment6 = l_11i_coa_rec.segment6
               AND leg_segment7 = l_11i_coa_rec.segment7
               AND leg_entity = piv_entity
               AND batch_id = pin_batch_id
               AND run_sequence_id = pin_run_sequence_id
               AND code_combination_id IS NULL;


          END IF;

        END LOOP;

        BEGIN
          FORALL indx IN 1 .. l_coa_recs_cur_type.COUNT SAVE EXCEPTIONS
            UPDATE /*+ INDEX(xxgl_balance_stg xxgl_balance_stg_N8) */ 
            xxgl_balance_stg
               SET code_combination_id = l_coa_recs_cur_type(indx).ccid,
                   segment1            = l_coa_recs_cur_type(indx).segment1,
                   segment2            = l_coa_recs_cur_type(indx).segment2,
                   segment3            = l_coa_recs_cur_type(indx).segment3,
                   segment4            = l_coa_recs_cur_type(indx).segment4,
                   segment5            = l_coa_recs_cur_type(indx).segment5,
                   segment6            = l_coa_recs_cur_type(indx).segment6,
                   segment7            = l_coa_recs_cur_type(indx).segment7,
                   segment8            = l_coa_recs_cur_type(indx).segment8,
                   segment9            = l_coa_recs_cur_type(indx).segment9,
                   segment10           = l_coa_recs_cur_type(indx).segment10
             WHERE leg_entity = piv_entity
               AND batch_id = pin_batch_id
               AND run_sequence_id = pin_run_sequence_id
               AND accounting_date = l_coa_recs_cur_type(indx)
                  .accounting_date -- v2.1
               AND (leg_segment1 = l_coa_recs_cur_type(indx).leg_segment1 AND
                   leg_segment2 = l_coa_recs_cur_type(indx).leg_segment2 AND
                   leg_segment3 = l_coa_recs_cur_type(indx).leg_segment3 AND
                   leg_segment4 = l_coa_recs_cur_type(indx).leg_segment4 AND
                   leg_segment5 = l_coa_recs_cur_type(indx).leg_segment5 AND
                   leg_segment6 = l_coa_recs_cur_type(indx).leg_segment6 AND
                   leg_segment7 = l_coa_recs_cur_type(indx).leg_segment7)
               AND l_coa_recs_cur_type(indx).ccid IS NOT NULL;

        EXCEPTION
          WHEN g_bulk_exception THEN
            l_bluk_error_count := l_bluk_error_count +
                                  SQL%BULK_EXCEPTIONS.COUNT;
            FOR exep_indx IN 1 .. SQL%BULK_EXCEPTIONS.COUNT LOOP
              l_excep_col1    := l_coa_recs_cur_type(SQL%BULK_EXCEPTIONS(exep_indx).ERROR_INDEX)
                                 .leg_segment1 || l_coa_recs_cur_type(SQL%BULK_EXCEPTIONS(exep_indx).ERROR_INDEX)
                                 .leg_segment2 || l_coa_recs_cur_type(SQL%BULK_EXCEPTIONS(exep_indx).ERROR_INDEX)
                                 .leg_segment3 || l_coa_recs_cur_type(SQL%BULK_EXCEPTIONS(exep_indx).ERROR_INDEX)
                                 .leg_segment4 || l_coa_recs_cur_type(SQL%BULK_EXCEPTIONS(exep_indx).ERROR_INDEX)
                                 .leg_segment5 || l_coa_recs_cur_type(SQL%BULK_EXCEPTIONS(exep_indx).ERROR_INDEX)
                                 .leg_segment6 || l_coa_recs_cur_type(SQL%BULK_EXCEPTIONS(exep_indx).ERROR_INDEX)
                                 .leg_segment7;
              l_error_message := SQLERRM(-1 * (SQL%BULK_EXCEPTIONS(exep_indx)
                                         .ERROR_CODE));

              -- v2.1
              l_acct_date := l_coa_recs_cur_type(SQL%BULK_EXCEPTIONS(exep_indx).ERROR_INDEX)
                             .accounting_date;

              debug('get_code_combination_id; Legacy Segments : ' ||
                    l_excep_col1);
              debug('get_code_combination_id; Error Message   : ' ||
                    l_error_message);

              --update leg_process_flag to 'E' for error records
              UPDATE xxgl_balance_ext_r12
                 SET leg_process_flag  = g_flag_e,
                     last_updated_date = SYSDATE,
                     last_updated_by   = g_user_id,
                     last_update_login = g_login_id
               WHERE leg_entity = piv_entity
                 AND batch_id = pin_batch_id
                 AND run_sequence_id = pin_run_sequence_id
                 AND accounting_date = l_acct_date -- v2.1
                 AND (leg_segment1 || leg_segment2 || leg_segment3 ||
                     leg_segment4 || leg_segment5 || leg_segment6 ||
                     leg_segment7) = l_excep_col1;
            END LOOP;
        END;

        COMMIT;
      END IF;
    END LOOP;
    CLOSE coa_recs_cur;
  EXCEPTION
    WHEN OTHERS THEN
      l_error_message := SUBSTR('Exception in Procedure get_code_combination_id. ' ||
                                SQLERRM,
                                1,
                                1999);
      fnd_file.put_line(fnd_file.LOG, l_error_message);
      fnd_file.put_line(fnd_file.LOG, 'get_code_combination_id<<E');
      pov_return_status := g_error;
      pov_error_message := l_error_message;
  END get_code_combination_id;

  -- =============================================================================
  -- Procedure: update_flags
  -- =============================================================================
  -- To update records as Validated/Error/Processed/Converted in staging table
  -- =============================================================================
  --  Input Parameters :
  --    piv_entity           : Entity Name
  --    pin_batch_id         : Batch Id
  --    piv_run_mode         : Run Mode
  --    pin_run_sequence_id  : Run Sequence Id
  --  Output Parameters :
  --    pov_return_status    : Return Status - Normal/Warning/Error
  --    pov_error_message    : Error message in case of any failure
  -- -----------------------------------------------------------------------------
  PROCEDURE update_flags(piv_entity          IN VARCHAR2,
                         pin_batch_id        IN NUMBER,
                         piv_run_mode        IN VARCHAR2,
                         pin_run_sequence_id IN NUMBER,
                         pov_return_status   OUT NOCOPY VARCHAR2,
                         pov_error_message   OUT NOCOPY VARCHAR2) IS
    PRAGMA AUTONOMOUS_TRANSACTION;

    l_error_tab_type xxetn_common_error_pkg.g_source_tab_type;
    l_error_message  VARCHAR2(2000);
    l_record_count   NUMBER;

  BEGIN
    debug('update_flags>>');

    pov_return_status := g_normal;
    pov_error_message := NULL;

--Added for v2.8 START
    BEGIN

      dbms_stats.gather_table_stats(ownname          => 'XXCONV',
                                    tabname          => 'XXGL_BALANCE_STG',
                                    cascade          => true,
                                    estimate_percent => 20);--dbms_stats.auto_sample_size,
                                    --degree           => dbms_stats.default_degree);

    END;
--Added for v2.8 END
    -- Updating Error Records
    IF (piv_run_mode = g_run_mode_validate) THEN
      UPDATE xxgl_balance_stg xbs
         SET xbs.process_flag           = g_flag_e,
             xbs.error_type             = g_err_validation,
             xbs.last_updated_date      = SYSDATE,
             xbs.last_updated_by        = g_user_id,
             xbs.last_update_login      = g_login_id,
             xbs.program_application_id = g_prog_appl_id,
             xbs.program_id             = g_program_id,
             xbs.program_update_date    = SYSDATE,
             xbs.request_id             = g_request_id
       WHERE xbs.leg_entity = piv_entity
         AND xbs.batch_id = pin_batch_id
         AND xbs.run_sequence_id = pin_run_sequence_id
         AND EXISTS
       (SELECT 1
                FROM xxetn_common_error xce
               WHERE xce.source_table = g_source_table
                 AND xce.interface_staging_id = xbs.interface_txn_id
                 AND xce.batch_id = pin_batch_id
                 AND xce.run_sequence_id = pin_run_sequence_id);

      debug('Records Updated for Error : ' || SQL%ROWCOUNT);

--Added for v2.8 START
    BEGIN

      dbms_stats.gather_table_stats(ownname          => 'XXCONV',
                                    tabname          => 'XXGL_BALANCE_STG',
                                    cascade          => true,
                                    estimate_percent => 20);--dbms_stats.auto_sample_size,
                                    --degree           => dbms_stats.default_degree);

    END;
--Added for v2.8 END
      -- Updating Journal Batch if a Journal Line Fails
      UPDATE xxgl_balance_stg xbs
         SET xbs.process_flag           = g_flag_e,
             xbs.error_type             = g_err_validation,
             xbs.last_updated_date      = SYSDATE,
             xbs.last_updated_by        = g_user_id,
             xbs.last_update_login      = g_login_id,
             xbs.program_application_id = g_prog_appl_id,
             xbs.program_id             = g_program_id,
             xbs.program_update_date    = SYSDATE,
             xbs.request_id             = g_request_id
       WHERE xbs.leg_entity = piv_entity
         AND xbs.batch_id = pin_batch_id
         AND xbs.run_sequence_id = pin_run_sequence_id
         AND xbs.leg_reference1 IN
             (SELECT DISTINCT xbsg.leg_reference1
                FROM xxgl_balance_stg xbsg
               WHERE xbsg.batch_id = pin_batch_id
                 AND xbsg.run_sequence_id = pin_run_sequence_id
                 AND xbsg.process_flag = g_flag_e
                 AND xbsg.leg_entity = piv_entity);

      debug('Records Updated for GL Batch Error : ' || SQL%ROWCOUNT);

--Added for v2.8 START
    BEGIN

      dbms_stats.gather_table_stats(ownname          => 'XXCONV',
                                    tabname          => 'XXGL_BALANCE_STG',
                                    cascade          => true,
                                    estimate_percent => 20);--dbms_stats.auto_sample_size,
                                    --degree           => dbms_stats.default_degree);

    END;
--Added for v2.8 END
      -- Updating Validated Records
      UPDATE xxgl_balance_stg xbs
         SET xbs.process_flag           = g_flag_v,
             xbs.error_type             = NULL,
             xbs.last_updated_date      = SYSDATE,
             xbs.last_updated_by        = g_user_id,
             xbs.last_update_login      = g_login_id,
             xbs.program_application_id = g_prog_appl_id,
             xbs.program_id             = g_program_id,
             xbs.program_update_date    = SYSDATE,
             xbs.request_id             = g_request_id
       WHERE xbs.leg_entity = piv_entity
         AND xbs.batch_id = pin_batch_id
         AND xbs.run_sequence_id = pin_run_sequence_id
         AND process_flag = g_flag_n;

      debug('Records Updated for Validation Success : ' || SQL%ROWCOUNT);
    END IF;

    COMMIT;
    debug('update_flags<<');
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      l_error_message := SUBSTR('Exception in Procedure update_flags. ' ||
                                SQLERRM,
                                1,
                                1999);
      fnd_file.put_line(fnd_file.LOG, l_error_message);
      fnd_file.put_line(fnd_file.LOG, 'update_flags<<E');
      pov_return_status := g_error;
      pov_error_message := l_error_message;
  END update_flags;

  -- =============================================================================
  -- Procedure: validate_cnv_rate_type
  -- =============================================================================
  -- Accounted amount must be supplied when entering
  -- foreign currency journal lines
  -- =============================================================================
  --  Input Parameters :
  --    piv_entity           : Entity Name
  --    pin_batch_id         : Batch Id
  --    pin_run_sequence_id  : Run Sequence Id
  --  Output Parameters :
  --    pov_return_status    : Return Status - Normal/Warning/Error
  --    pov_error_message    : Error message in case of any failure
  -- -----------------------------------------------------------------------------
  PROCEDURE validate_cnv_rate_type(piv_entity          IN VARCHAR2,
                                   pin_batch_id        IN NUMBER,
                                   pin_run_sequence_id IN NUMBER,
                                   pov_return_status   OUT NOCOPY VARCHAR2,
                                   pov_error_message   OUT NOCOPY VARCHAR2) IS
    l_error_tab_type xxetn_common_error_pkg.g_source_tab_type;
    l_log_err_sts    VARCHAR2(2000) := NULL;
    l_log_err_msg    VARCHAR2(2000) := NULL;
    l_error_message  VARCHAR2(2000) := NULL;

    CURSOR cnv_rate_type_cur IS
      SELECT g_source_table       AS source_table,
             xbs.interface_txn_id AS interface_staging_id,
             NULL                 AS source_keyname1,
             NULL                 AS source_keyvalue1,
             NULL                 AS source_keyname2,
             NULL                 AS source_keyvalue2,
             NULL                 AS source_keyname3,
             NULL                 AS source_keyvalue3,
             NULL                 AS source_keyname4,
             NULL                 AS source_keyvalue4,
             NULL                 AS source_keyname5,
             NULL                 AS source_keyvalue5
             --, 'LEG_USER_CURR_CONV_TYPE'   AS source_column_name
            ,
             'LEG_ACCOUNTED_DR_CR' AS source_column_name
             --, xbs.leg_user_curr_conv_type AS source_column_value
            ,
             NULL             AS source_column_value,
             g_err_validation AS error_type
             --, 'ETN_INVALID_CNV_RATE_TYPE' AS error_code
             --, 'A conversion rate type or an accounted amount must be supplied when entering foreign currency journal lines'
            ,
             'ETN_NULL_ACCT_DR_CR' AS error_code,
             'Accounted amount must be supplied when entering foreign currency journal lines' AS error_message,
             NULL AS severity,
             NULL AS proposed_solution,
             NULL AS stage, -- v2.3
             NULL AS interface_load_id -- as per changes done by Sagar
        FROM xxgl_balance_stg xbs
       WHERE xbs.process_flag = g_flag_n
         AND xbs.leg_entity = piv_entity
         AND xbs.batch_id = pin_batch_id
         AND xbs.run_sequence_id = pin_run_sequence_id
         AND DECODE(xbs.leg_currency_code,
                    'STAT',
                    xbs.functional_currency_code,
                    xbs.leg_currency_code) <> xbs.functional_currency_code
         AND xbs.leg_accounted_dr IS NULL
         AND xbs.leg_accounted_cr IS NULL; -- addded this. Commented below lines

    --AND    ( xbs.leg_user_curr_conv_type   IS NULL
    --        OR ( NVL(xbs.leg_accounted_dr,0) = 0 AND NVL(xbs.leg_accounted_cr,0) = 0));
  BEGIN
    debug('validate_cnv_rate_type>>');

    pov_return_status := g_normal;
    pov_error_message := NULL;

    BEGIN
      OPEN cnv_rate_type_cur;
      LOOP
        FETCH cnv_rate_type_cur BULK COLLECT
          INTO l_error_tab_type LIMIT g_bulk_limit;
        EXIT WHEN l_error_tab_type.COUNT = 0;
        IF l_error_tab_type.COUNT > 0 THEN
          log_error(pin_batch_id        => pin_batch_id,
                    pin_run_sequence_id => pin_run_sequence_id,
                    p_source_tab_type   => l_error_tab_type,
                    pov_return_status   => l_log_err_sts,
                    pov_error_message   => l_log_err_msg);

          IF l_log_err_msg IS NOT NULL THEN
            debug('validate_cnv_rate_type. l_log_err_msg ' ||
                  l_log_err_msg);
          END IF;
          l_error_tab_type.DELETE;
        END IF;
      END LOOP;
      CLOSE cnv_rate_type_cur;
    EXCEPTION
      WHEN OTHERS THEN
        debug(SUBSTR('Exception while validating currency conversion type. ' ||
                     SQLERRM,
                     1,
                     1999));
    END;

    debug('validate_cnv_rate_type<<');
  EXCEPTION
    WHEN OTHERS THEN
      l_error_message := SUBSTR('Exception in Procedure validate_cnv_rate_type. ' ||
                                SQLERRM,
                                1,
                                1999);
      fnd_file.put_line(fnd_file.LOG, l_error_message);
      fnd_file.put_line(fnd_file.LOG, 'validate_cnv_rate_type<<E');
      pov_return_status := g_error;
      pov_error_message := l_error_message;
  END validate_cnv_rate_type;

  -- =============================================================================
  -- Procedure: validate_cnv_rate
  -- =============================================================================
  -- conversion rate must be entered when using the User conversion rate type
  -- =============================================================================
  --  Input Parameters :
  --    piv_entity           : Entity Name
  --    pin_batch_id         : Batch Id
  --    pin_run_sequence_id  : Run Sequence Id
  --  Output Parameters :
  --    pov_return_status    : Return Status - Normal/Warning/Error
  --    pov_error_message    : Error message in case of any failure
  -- -----------------------------------------------------------------------------
  PROCEDURE validate_cnv_rate(piv_entity          IN VARCHAR2,
                              pin_batch_id        IN NUMBER,
                              pin_run_sequence_id IN NUMBER,
                              pov_return_status   OUT NOCOPY VARCHAR2,
                              pov_error_message   OUT NOCOPY VARCHAR2) IS
    l_error_tab_type xxetn_common_error_pkg.g_source_tab_type;
    l_log_err_sts    VARCHAR2(2000) := NULL;
    l_log_err_msg    VARCHAR2(2000) := NULL;
    l_error_message  VARCHAR2(2000) := NULL;

    CURSOR cnv_rate_cur IS
      SELECT g_source_table AS source_table,
             xbs.interface_txn_id AS interface_staging_id,
             NULL AS source_keyname1,
             NULL AS source_keyvalue1,
             NULL AS source_keyname2,
             NULL AS source_keyvalue2,
             NULL AS source_keyname3,
             NULL AS source_keyvalue3,
             NULL AS source_keyname4,
             NULL AS source_keyvalue4,
             NULL AS source_keyname5,
             NULL AS source_keyvalue5,
             'LEG_CURR_CON_RATE' AS source_column_name,
             xbs.leg_curr_con_rate AS source_column_value,
             g_err_validation AS error_type,
             'ETN_INVALID_CNV_RATE' AS error_code,
             'A conversion rate must be entered when using the User conversion rate type' AS error_message,
             NULL AS severity,
             NULL AS proposed_solution,
             NULL AS stage, -- v2.3
             NULL AS interface_load_id -- as per changes done by Sagar
        FROM xxgl_balance_stg xbs
       WHERE xbs.process_flag = g_flag_n
         AND xbs.leg_entity = piv_entity
         AND xbs.batch_id = pin_batch_id
         AND xbs.run_sequence_id = pin_run_sequence_id
         AND DECODE(xbs.leg_currency_code,
                    'STAT',
                    xbs.functional_currency_code,
                    xbs.leg_currency_code) <> xbs.functional_currency_code
         AND xbs.leg_user_curr_conv_type = 'User'
         AND NVL(xbs.leg_curr_con_rate, 0) = 0;
  BEGIN
    debug('validate_cnv_rate>>');

    pov_return_status := g_normal;
    pov_error_message := NULL;

    BEGIN
      OPEN cnv_rate_cur;
      LOOP
        FETCH cnv_rate_cur BULK COLLECT
          INTO l_error_tab_type LIMIT g_bulk_limit;
        EXIT WHEN l_error_tab_type.COUNT = 0;
        IF l_error_tab_type.COUNT > 0 THEN
          log_error(pin_batch_id        => pin_batch_id,
                    pin_run_sequence_id => pin_run_sequence_id,
                    p_source_tab_type   => l_error_tab_type,
                    pov_return_status   => l_log_err_sts,
                    pov_error_message   => l_log_err_msg);

          IF l_log_err_msg IS NOT NULL THEN
            debug('validate_cnv_rate. l_log_err_msg ' || l_log_err_msg);
          END IF;
          l_error_tab_type.DELETE;
        END IF;
      END LOOP;
      CLOSE cnv_rate_cur;
    EXCEPTION
      WHEN OTHERS THEN
        debug(SUBSTR('Exception while validating currency conversion rate. ' ||
                     SQLERRM,
                     1,
                     1999));
    END;

    debug('validate_cnv_rate<<');
  EXCEPTION
    WHEN OTHERS THEN
      l_error_message := SUBSTR('Exception in Procedure validate_cnv_rate. ' ||
                                SQLERRM,
                                1,
                                1999);
      fnd_file.put_line(fnd_file.LOG, l_error_message);
      fnd_file.put_line(fnd_file.LOG, 'validate_cnv_rate<<E');
      pov_return_status := g_error;
      pov_error_message := l_error_message;
  END validate_cnv_rate;

  -- =============================================================================
  -- Procedure: validate_ent_cr_amt
  -- =============================================================================
  --        should be equal to Accounted amount for Functional Currency Journals
  -- =============================================================================
  --  Input Parameters :
  --    piv_entity           : Entity Name
  --    pin_batch_id         : Batch Id
  --    pin_run_sequence_id  : Run Sequence Id
  --  Output Parameters :
  --    pov_return_status    : Return Status - Normal/Warning/Error
  --    pov_error_message    : Error message in case of any failure
  -- -----------------------------------------------------------------------------
  PROCEDURE validate_ent_cr_amt(piv_entity          IN VARCHAR2,
                                pin_batch_id        IN NUMBER,
                                pin_run_sequence_id IN NUMBER,
                                pov_return_status   OUT NOCOPY VARCHAR2,
                                pov_error_message   OUT NOCOPY VARCHAR2) IS
    l_error_tab_type xxetn_common_error_pkg.g_source_tab_type;
    l_log_err_sts    VARCHAR2(2000) := NULL;
    l_log_err_msg    VARCHAR2(2000) := NULL;
    l_error_message  VARCHAR2(2000) := NULL;

    CURSOR ent_cr_cur IS
      SELECT g_source_table AS source_table,
             xbs.interface_txn_id AS interface_staging_id,
             NULL AS source_keyname1,
             NULL AS source_keyvalue1,
             NULL AS source_keyname2,
             NULL AS source_keyvalue2,
             NULL AS source_keyname3,
             NULL AS source_keyvalue3,
             NULL AS source_keyname4,
             NULL AS source_keyvalue4,
             NULL AS source_keyname5,
             NULL AS source_keyvalue5,
             'LEG_ENTERED_CR' AS source_column_name,
             xbs.leg_entered_cr AS source_column_value,
             g_err_validation AS error_type,
             'ETN_INVALID_ENT_DR_AMT' AS error_code,
             'Accounted value not equal entered value for Functional/STAT Currency Journal' AS error_message,
             NULL AS severity,
             NULL AS proposed_solution,
             NULL AS stage, -- v2.3
             NULL AS interface_load_id --as per changes done by Sagar
        FROM xxgl_balance_stg xbs
       WHERE xbs.process_flag = g_flag_n
         AND xbs.leg_entity = piv_entity
         AND xbs.batch_id = pin_batch_id
         AND xbs.run_sequence_id = pin_run_sequence_id
         AND NVL(xbs.leg_entered_cr, 0) <> NVL(xbs.leg_accounted_cr, 0)
         AND (xbs.leg_currency_code = xbs.functional_currency_code OR
             xbs.leg_currency_code = 'STAT');
  BEGIN
    debug('validate_ent_cr_amt>>');

    pov_return_status := g_normal;
    pov_error_message := NULL;

    BEGIN
      OPEN ent_cr_cur;
      LOOP
        FETCH ent_cr_cur BULK COLLECT
          INTO l_error_tab_type LIMIT g_bulk_limit;
        EXIT WHEN l_error_tab_type.COUNT = 0;
        IF l_error_tab_type.COUNT > 0 THEN
          log_error(pin_batch_id        => pin_batch_id,
                    pin_run_sequence_id => pin_run_sequence_id,
                    p_source_tab_type   => l_error_tab_type,
                    pov_return_status   => l_log_err_sts,
                    pov_error_message   => l_log_err_msg);

          IF l_log_err_msg IS NOT NULL THEN
            debug('validate_ent_cr_amt. l_log_err_msg ' || l_log_err_msg);
          END IF;
          l_error_tab_type.DELETE;
        END IF;
      END LOOP;
      CLOSE ent_cr_cur;
    EXCEPTION
      WHEN OTHERS THEN
        debug(SUBSTR('Exception while Entered Cr Amount. ' || SQLERRM,
                     1,
                     1999));
    END;

    debug('validate_ent_cr_amt<<');
  EXCEPTION
    WHEN OTHERS THEN
      l_error_message := SUBSTR('Exception in Procedure validate_ent_cr_amt. ' ||
                                SQLERRM,
                                1,
                                1999);
      fnd_file.put_line(fnd_file.LOG, l_error_message);
      fnd_file.put_line(fnd_file.LOG, 'validate_ent_cr_amt<<E');
      pov_return_status := g_error;
      pov_error_message := l_error_message;
  END validate_ent_cr_amt;

  -- =============================================================================
  -- Procedure: validate_ent_dr_amt
  -- =============================================================================
  -- Entered Dr Amount should be equal to Accounted amount for Functional Currency Journals
  -- =============================================================================
  --  Input Parameters :
  --    piv_entity           : Entity Name
  --    pin_batch_id         : Batch Id
  --    pin_run_sequence_id  : Run Sequence Id
  --  Output Parameters :
  --    pov_return_status    : Return Status - Normal/Warning/Error
  --    pov_error_message    : Error message in case of any failure
  -- -----------------------------------------------------------------------------
  PROCEDURE validate_ent_dr_amt(piv_entity          IN VARCHAR2,
                                pin_batch_id        IN NUMBER,
                                pin_run_sequence_id IN NUMBER,
                                pov_return_status   OUT NOCOPY VARCHAR2,
                                pov_error_message   OUT NOCOPY VARCHAR2) IS
    l_error_tab_type xxetn_common_error_pkg.g_source_tab_type;
    l_log_err_sts    VARCHAR2(2000) := NULL;
    l_log_err_msg    VARCHAR2(2000) := NULL;
    l_error_message  VARCHAR2(2000) := NULL;

    CURSOR ent_dr_cur IS
      SELECT g_source_table AS source_table,
             xbs.interface_txn_id AS interface_staging_id,
             NULL AS source_keyname1,
             NULL AS source_keyvalue1,
             NULL AS source_keyname2,
             NULL AS source_keyvalue2,
             NULL AS source_keyname3,
             NULL AS source_keyvalue3,
             NULL AS source_keyname4,
             NULL AS source_keyvalue4,
             NULL AS source_keyname5,
             NULL AS source_keyvalue5,
             'LEG_ENTERED_DR' AS source_column_name,
             xbs.leg_entered_dr AS source_column_value,
             g_err_validation AS error_type,
             'ETN_INVALID_ENT_DR_AMT' AS error_code,
             'Accounted value not equal entered value for Functional/STAT Currency Journal' AS error_message,
             NULL AS severity,
             NULL AS proposed_solution,
             NULL AS stage, -- v2.3
             NULL AS interface_load_id --as per changes done by sagar
        FROM xxgl_balance_stg xbs
       WHERE xbs.process_flag = g_flag_n
         AND xbs.leg_entity = piv_entity
         AND xbs.batch_id = pin_batch_id
         AND xbs.run_sequence_id = pin_run_sequence_id
         AND NVL(xbs.leg_entered_dr, 0) <> NVL(xbs.leg_accounted_dr, 0)
         AND (xbs.leg_currency_code = xbs.functional_currency_code OR
             xbs.leg_currency_code = 'STAT');
  BEGIN
    debug('validate_ent_dr_amt>>');

    pov_return_status := g_normal;
    pov_error_message := NULL;

    BEGIN
      OPEN ent_dr_cur;
      LOOP
        FETCH ent_dr_cur BULK COLLECT
          INTO l_error_tab_type LIMIT g_bulk_limit;
        EXIT WHEN l_error_tab_type.COUNT = 0;
        IF l_error_tab_type.COUNT > 0 THEN
          log_error(pin_batch_id        => pin_batch_id,
                    pin_run_sequence_id => pin_run_sequence_id,
                    p_source_tab_type   => l_error_tab_type,
                    pov_return_status   => l_log_err_sts,
                    pov_error_message   => l_log_err_msg);

          IF l_log_err_msg IS NOT NULL THEN
            debug('validate_ent_dr_amt. l_log_err_msg ' || l_log_err_msg);
          END IF;
          l_error_tab_type.DELETE;
        END IF;
      END LOOP;
      CLOSE ent_dr_cur;
    EXCEPTION
      WHEN OTHERS THEN
        debug(SUBSTR('Exception while Entered Dr Amount. ' || SQLERRM,
                     1,
                     1999));
    END;

    debug('validate_ent_dr_amt<<');
  EXCEPTION
    WHEN OTHERS THEN
      l_error_message := SUBSTR('Exception in Procedure validate_ent_dr_amt. ' ||
                                SQLERRM,
                                1,
                                1999);
      fnd_file.put_line(fnd_file.LOG, l_error_message);
      fnd_file.put_line(fnd_file.LOG, 'validate_ent_dr_amt<<E');
      pov_return_status := g_error;
      pov_error_message := l_error_message;
  END validate_ent_dr_amt;

  -- =============================================================================
  -- Procedure: validate_avg_journal_flag
  -- =============================================================================
  -- Validate Balance Conversion Source; should be valid
  -- =============================================================================
  --  Input Parameters :
  --    piv_entity           : Entity Name
  --    pin_batch_id         : Batch Id
  --    pin_run_sequence_id  : Run Sequence Id
  --  Output Parameters :
  --    pov_return_status    : Return Status - Normal/Warning/Error
  --    pov_error_message    : Error message in case of any failure
  -- -----------------------------------------------------------------------------
  PROCEDURE validate_avg_journal_flag(piv_entity          IN VARCHAR2,
                                      pin_batch_id        IN NUMBER,
                                      pin_run_sequence_id IN NUMBER,
                                      pov_return_status   OUT NOCOPY VARCHAR2,
                                      pov_error_message   OUT NOCOPY VARCHAR2) IS
    l_error_tab_type xxetn_common_error_pkg.g_source_tab_type;
    l_log_err_sts    VARCHAR2(2000) := NULL;
    l_log_err_msg    VARCHAR2(2000) := NULL;
    l_error_message  VARCHAR2(2000) := NULL;

    CURSOR avg_journal_flag_cur IS
      SELECT g_source_table AS source_table,
             xbs.interface_txn_id AS interface_staging_id,
             NULL AS source_keyname1,
             NULL AS source_keyvalue1,
             NULL AS source_keyname2,
             NULL AS source_keyvalue2,
             NULL AS source_keyname3,
             NULL AS source_keyvalue3,
             NULL AS source_keyname4,
             NULL AS source_keyvalue4,
             NULL AS source_keyname5,
             NULL AS source_keyvalue5,
             'AVERAGE_JOURNAL_FLAG' AS source_column_name,
             xbs.average_journal_flag AS source_column_value,
             g_err_validation AS error_type,
             'ETN_INVALID_AVG_JOUR_FLAG' AS error_code,
             'Average Journal flag not valid' AS error_message,
             NULL AS severity,
             NULL AS proposed_solution,
             NULL AS stage, -- v2.3
             NULL AS interface_load_id --as per changes done by sagar
        FROM xxgl_balance_stg xbs
       WHERE xbs.process_flag = g_flag_n
         AND xbs.leg_entity = piv_entity
         AND xbs.batch_id = pin_batch_id
         AND xbs.run_sequence_id = pin_run_sequence_id
         AND NVL(xbs.average_journal_flag, g_flag_n) NOT IN
             (g_flag_y, g_flag_n);
  BEGIN
    debug('validate_avg_journal_flag>>');

    pov_return_status := g_normal;
    pov_error_message := NULL;

    BEGIN
      OPEN avg_journal_flag_cur;
      LOOP
        FETCH avg_journal_flag_cur BULK COLLECT
          INTO l_error_tab_type LIMIT g_bulk_limit;
        EXIT WHEN l_error_tab_type.COUNT = 0;
        IF l_error_tab_type.COUNT > 0 THEN
          log_error(pin_batch_id        => pin_batch_id,
                    pin_run_sequence_id => pin_run_sequence_id,
                    p_source_tab_type   => l_error_tab_type,
                    pov_return_status   => l_log_err_sts,
                    pov_error_message   => l_log_err_msg);

          IF l_log_err_msg IS NOT NULL THEN
            debug('validate_avg_journal_flag. l_log_err_msg ' ||
                  l_log_err_msg);
          END IF;
          l_error_tab_type.DELETE;
        END IF;
      END LOOP;
      CLOSE avg_journal_flag_cur;
    EXCEPTION
      WHEN OTHERS THEN
        debug(SUBSTR('Exception while validating average  journal flag. ' ||
                     SQLERRM,
                     1,
                     1999));
    END;

    debug('validate_avg_journal_flag<<');
  EXCEPTION
    WHEN OTHERS THEN
      l_error_message := SUBSTR('Exception in Procedure validate_avg_journal_flag. ' ||
                                SQLERRM,
                                1,
                                1999);
      fnd_file.put_line(fnd_file.LOG, l_error_message);
      fnd_file.put_line(fnd_file.LOG, 'validate_avg_journal_flag<<E');
      pov_return_status := g_error;
      pov_error_message := l_error_message;
  END validate_avg_journal_flag;

  -- =============================================================================
  -- Procedure: validate_user_je_source
  -- =============================================================================
  -- Validate Balance Conversion Source; should be valid
  -- =============================================================================
  --  Input Parameters :
  --    piv_entity           : Entity Name
  --    pin_batch_id         : Batch Id
  --    pin_run_sequence_id  : Run Sequence Id
  --  Output Parameters :
  --    pov_return_status    : Return Status - Normal/Warning/Error
  --    pov_error_message    : Error message in case of any failure
  -- -----------------------------------------------------------------------------
  PROCEDURE validate_user_je_source(piv_entity          IN VARCHAR2,
                                    pin_batch_id        IN NUMBER,
                                    pin_run_sequence_id IN NUMBER,
                                    pov_return_status   OUT NOCOPY VARCHAR2,
                                    pov_error_message   OUT NOCOPY VARCHAR2) IS
    l_error_tab_type xxetn_common_error_pkg.g_source_tab_type;
    l_log_err_sts    VARCHAR2(2000) := NULL;
    l_log_err_msg    VARCHAR2(2000) := NULL;
    l_error_message  VARCHAR2(2000) := NULL;

    CURSOR je_src_bal_cur IS
      SELECT g_source_table AS source_table,
             xbs.interface_txn_id AS interface_staging_id,
             NULL AS source_keyname1,
             NULL AS source_keyvalue1,
             NULL AS source_keyname2,
             NULL AS source_keyvalue2,
             NULL AS source_keyname3,
             NULL AS source_keyvalue3,
             NULL AS source_keyname4,
             NULL AS source_keyvalue4,
             NULL AS source_keyname5,
             NULL AS source_keyvalue5,
             'LEG_USER_JE_SRC_NAME' AS source_column_name,
             xbs.leg_user_je_src_name AS source_column_value,
             g_err_validation AS error_type,
             'ETN_INVALID_USER_JE_SRC' AS error_code,
             'Balance Journal Source not valid' AS error_message,
             NULL AS severity,
             NULL AS proposed_solution,
             NULL AS stage, -- v2.3
             NULL AS interface_load_id --as per changes done by sagar
        FROM xxgl_balance_stg xbs
       WHERE xbs.process_flag = g_flag_n
         AND xbs.leg_entity = piv_entity
         AND xbs.batch_id = pin_batch_id
         AND xbs.run_sequence_id = pin_run_sequence_id
         AND xbs.user_je_source_name <> g_balance_source;

    CURSOR je_src_jour_cur IS
      SELECT g_source_table AS source_table,
             xbs.interface_txn_id AS interface_staging_id,
             NULL AS source_keyname1,
             NULL AS source_keyvalue1,
             NULL AS source_keyname2,
             NULL AS source_keyvalue2,
             NULL AS source_keyname3,
             NULL AS source_keyvalue3,
             NULL AS source_keyname4,
             NULL AS source_keyvalue4,
             NULL AS source_keyname5,
             NULL AS source_keyvalue5,
             'LEG_USER_JE_SRC_NAME' AS source_column_name,
             xbs.leg_user_je_src_name AS source_column_value,
             g_err_validation AS error_type,
             'ETN_INVALID_USER_JE_SRC' AS error_code,
             'Balance Journal Source should not be null' AS error_message,
             NULL AS severity,
             NULL AS proposed_solution,
             NULL AS stage -- v2.3
        FROM xxgl_balance_stg xbs
       WHERE xbs.process_flag = g_flag_n
         AND xbs.leg_entity = piv_entity
         AND xbs.batch_id = pin_batch_id
         AND xbs.run_sequence_id = pin_run_sequence_id
         AND xbs.user_je_source_name IS NULL;
  BEGIN
    debug('validate_user_je_source>>');

    pov_return_status := g_normal;
    pov_error_message := NULL;

    --IF (piv_entity IN (g_entity_openbalance, g_entity_monthbalance)) THEN                      -- commented for v2.2
    IF piv_entity IN (g_entity_openbalance,
                      g_entity_monthbalance,
                      g_entity_foreignbalance) THEN
      -- added for v2.2

      BEGIN
        OPEN je_src_bal_cur;
        LOOP
          FETCH je_src_bal_cur BULK COLLECT
            INTO l_error_tab_type LIMIT g_bulk_limit;
          EXIT WHEN l_error_tab_type.COUNT = 0;
          IF l_error_tab_type.COUNT > 0 THEN
            log_error(pin_batch_id        => pin_batch_id,
                      pin_run_sequence_id => pin_run_sequence_id,
                      p_source_tab_type   => l_error_tab_type,
                      pov_return_status   => l_log_err_sts,
                      pov_error_message   => l_log_err_msg);

            IF l_log_err_msg IS NOT NULL THEN
              debug('validate_user_je_source (balance). l_log_err_msg ' ||
                    l_log_err_msg);
            END IF;
            l_error_tab_type.DELETE;
          END IF;
        END LOOP;
        CLOSE je_src_bal_cur;
      EXCEPTION
        WHEN OTHERS THEN
          debug(SUBSTR('Exception while validating je source (balance). ' ||
                       SQLERRM,
                       1,
                       1999));
      END;

      /** Commented on 19-May. Conversion gives 2 messages when User JE Source is not derived from Leg value
            ELSIF (piv_entity = g_entity_journal) THEN

               BEGIN
                  OPEN   je_src_jour_cur;
                  LOOP
                     FETCH je_src_jour_cur BULK COLLECT INTO l_error_tab_type LIMIT g_bulk_limit;
                     EXIT WHEN l_error_tab_type.COUNT = 0;
                     IF l_error_tab_type.COUNT > 0 THEN
                        log_error
                        ( pin_batch_id        => pin_batch_id
                        , pin_run_sequence_id => pin_run_sequence_id
                        , p_source_tab_type   => l_error_tab_type
                        , pov_return_status   => l_log_err_sts
                        , pov_error_message   => l_log_err_msg );

                        IF l_log_err_msg IS NOT NULL THEN
                           debug('validate_user_je_source (journal). l_log_err_msg '||l_log_err_msg);
                        END IF;
                        l_error_tab_type.DELETE;
                     END IF;
                  END LOOP;
                  CLOSE  je_src_jour_cur;
               EXCEPTION
                  WHEN OTHERS THEN
                     debug(SUBSTR('Exception while validating je source (journal). '||SQLERRM,1,1999));
               END;
      **/

    END IF;

    debug('validate_user_je_source<<');
  EXCEPTION
    WHEN OTHERS THEN
      l_error_message := SUBSTR('Exception in Procedure validate_user_je_source. ' ||
                                SQLERRM,
                                1,
                                1999);
      fnd_file.put_line(fnd_file.LOG, l_error_message);
      fnd_file.put_line(fnd_file.LOG, 'validate_user_je_source<<E');
      pov_return_status := g_error;
      pov_error_message := l_error_message;
  END validate_user_je_source;

  -- =============================================================================
  -- Procedure: validate_user_je_cat
  -- =============================================================================
  -- Validate Balance Conversion Category; should be valid
  -- =============================================================================
  --  Input Parameters :
  --    piv_entity           : Entity Name
  --    pin_batch_id         : Batch Id
  --    pin_run_sequence_id  : Run Sequence Id
  --  Output Parameters :
  --    pov_return_status    : Return Status - Normal/Warning/Error
  --    pov_error_message    : Error message in case of any failure
  -- -----------------------------------------------------------------------------
  PROCEDURE validate_user_je_cat(piv_entity          IN VARCHAR2,
                                 pin_batch_id        IN NUMBER,
                                 pin_run_sequence_id IN NUMBER,
                                 pov_return_status   OUT NOCOPY VARCHAR2,
                                 pov_error_message   OUT NOCOPY VARCHAR2) IS
    l_error_tab_type xxetn_common_error_pkg.g_source_tab_type;
    l_log_err_sts    VARCHAR2(2000) := NULL;
    l_log_err_msg    VARCHAR2(2000) := NULL;
    l_error_message  VARCHAR2(2000) := NULL;

    CURSOR je_cat_bal_cur IS
      SELECT g_source_table AS source_table,
             xbs.interface_txn_id AS interface_staging_id,
             NULL AS source_keyname1,
             NULL AS source_keyvalue1,
             NULL AS source_keyname2,
             NULL AS source_keyvalue2,
             NULL AS source_keyname3,
             NULL AS source_keyvalue3,
             NULL AS source_keyname4,
             NULL AS source_keyvalue4,
             NULL AS source_keyname5,
             NULL AS source_keyvalue5,
             'LEG_USER_JE_CAT_NAME' AS source_column_name,
             xbs.leg_user_je_cat_name AS source_column_value,
             g_err_validation AS error_type,
             'ETN_INVALID_USER_JE_CAT' AS error_code,
             'Balance Journal Category not valid' AS error_message,
             NULL AS severity,
             NULL AS proposed_solution,
             NULL AS stage, -- v2.3
             NULL AS interface_load_id -- as per changes done by sagar
        FROM xxgl_balance_stg xbs
       WHERE xbs.process_flag = g_flag_n
         AND xbs.leg_entity = piv_entity
         AND xbs.batch_id = pin_batch_id
         AND xbs.run_sequence_id = pin_run_sequence_id
         AND xbs.leg_user_je_cat_name <> g_balance_category;

    CURSOR je_cat_jour_cur IS
      SELECT g_source_table AS source_table,
             xbs.interface_txn_id AS interface_staging_id,
             NULL AS source_keyname1,
             NULL AS source_keyvalue1,
             NULL AS source_keyname2,
             NULL AS source_keyvalue2,
             NULL AS source_keyname3,
             NULL AS source_keyvalue3,
             NULL AS source_keyname4,
             NULL AS source_keyvalue4,
             NULL AS source_keyname5,
             NULL AS source_keyvalue5,
             'LEG_USER_JE_CAT_NAME' AS source_column_name,
             xbs.leg_user_je_cat_name AS source_column_value,
             g_err_validation AS error_type,
             'ETN_INVALID_USER_JE_CAT' AS error_code,
             'Balance Journal Category should not be null' AS error_message,
             NULL AS severity,
             NULL AS proposed_solution,
             NULL AS stage, -- v2.3
             NULL AS interface_load_id -- as per changes done by sagar
        FROM xxgl_balance_stg xbs
       WHERE xbs.process_flag = g_flag_n
         AND xbs.leg_entity = piv_entity
         AND xbs.batch_id = pin_batch_id
         AND xbs.run_sequence_id = pin_run_sequence_id
         AND xbs.leg_user_je_cat_name IS NULL;
  BEGIN
    debug('validate_user_je_cat>>');

    pov_return_status := g_normal;
    pov_error_message := NULL;

    --IF (piv_entity IN (g_entity_openbalance, g_entity_monthbalance)) THEN                      -- commented for v2.2
    IF piv_entity IN (g_entity_openbalance,
                      g_entity_monthbalance,
                      g_entity_foreignbalance) THEN
      -- added for v2.2

      BEGIN
        OPEN je_cat_bal_cur;
        LOOP
          FETCH je_cat_bal_cur BULK COLLECT
            INTO l_error_tab_type LIMIT g_bulk_limit;
          EXIT WHEN l_error_tab_type.COUNT = 0;
          IF l_error_tab_type.COUNT > 0 THEN
            log_error(pin_batch_id        => pin_batch_id,
                      pin_run_sequence_id => pin_run_sequence_id,
                      p_source_tab_type   => l_error_tab_type,
                      pov_return_status   => l_log_err_sts,
                      pov_error_message   => l_log_err_msg);

            IF l_log_err_msg IS NOT NULL THEN
              debug('validate_user_je_cat (balance). l_log_err_msg ' ||
                    l_log_err_msg);
            END IF;
            l_error_tab_type.DELETE;
          END IF;
        END LOOP;
        CLOSE je_cat_bal_cur;
      EXCEPTION
        WHEN OTHERS THEN
          debug(SUBSTR('Exception while validating je category (balance). ' ||
                       SQLERRM,
                       1,
                       1999));
      END;

    ELSIF (piv_entity = g_entity_journal) THEN

      BEGIN
        OPEN je_cat_jour_cur;
        LOOP
          FETCH je_cat_jour_cur BULK COLLECT
            INTO l_error_tab_type LIMIT g_bulk_limit;
          EXIT WHEN l_error_tab_type.COUNT = 0;
          IF l_error_tab_type.COUNT > 0 THEN
            log_error(pin_batch_id        => pin_batch_id,
                      pin_run_sequence_id => pin_run_sequence_id,
                      p_source_tab_type   => l_error_tab_type,
                      pov_return_status   => l_log_err_sts,
                      pov_error_message   => l_log_err_msg);

            IF l_log_err_msg IS NOT NULL THEN
              debug('validate_user_je_cat (journal). l_log_err_msg ' ||
                    l_log_err_msg);
            END IF;
            l_error_tab_type.DELETE;
          END IF;
        END LOOP;
        CLOSE je_cat_jour_cur;
      EXCEPTION
        WHEN OTHERS THEN
          debug(SUBSTR('Exception while validating je category (journal). ' ||
                       SQLERRM,
                       1,
                       1999));
      END;

    END IF;

    debug('validate_user_je_cat<<');
  EXCEPTION
    WHEN OTHERS THEN
      l_error_message := SUBSTR('Exception in Procedure validate_user_je_cat. ' ||
                                SQLERRM,
                                1,
                                1999);
      fnd_file.put_line(fnd_file.LOG, l_error_message);
      fnd_file.put_line(fnd_file.LOG, 'validate_user_je_cat<<E');
      pov_return_status := g_error;
      pov_error_message := l_error_message;
  END validate_user_je_cat;

  -- =============================================================================
  -- Procedure: validate_currency_code
  -- =============================================================================
  -- Validate Currency Code; must be a Valid Currency code
  -- =============================================================================
  --  Input Parameters :
  --    piv_entity           : Entity Name
  --    pin_batch_id         : Batch Id
  --    pin_run_sequence_id  : Run Sequence Id
  --  Output Parameters :
  --    pov_return_status    : Return Status - Normal/Warning/Error
  --    pov_error_message    : Error message in case of any failure
  -- -----------------------------------------------------------------------------
  PROCEDURE validate_currency_code(piv_entity          IN VARCHAR2,
                                   pin_batch_id        IN NUMBER,
                                   pin_run_sequence_id IN NUMBER,
                                   pov_return_status   OUT NOCOPY VARCHAR2,
                                   pov_error_message   OUT NOCOPY VARCHAR2) IS
    l_error_tab_type xxetn_common_error_pkg.g_source_tab_type;
    l_log_err_sts    VARCHAR2(2000) := NULL;
    l_log_err_msg    VARCHAR2(2000) := NULL;
    l_error_message  VARCHAR2(2000) := NULL;

    CURSOR curr_code_cur IS
      SELECT g_source_table AS source_table,
             xbs.interface_txn_id AS interface_staging_id,
             NULL AS source_keyname1,
             NULL AS source_keyvalue1,
             NULL AS source_keyname2,
             NULL AS source_keyvalue2,
             NULL AS source_keyname3,
             NULL AS source_keyvalue3,
             NULL AS source_keyname4,
             NULL AS source_keyvalue4,
             NULL AS source_keyname5,
             NULL AS source_keyvalue5,
             'LEG_CURRENCY_CODE' AS source_column_name,
             xbs.leg_currency_code AS source_column_value,
             g_err_validation AS error_type,
             'ETN_INVALID_CURRENCY' AS error_code,
             'Legacy Currency Code not defined/enabled' AS error_message,
             NULL AS severity,
             NULL AS proposed_solution,
             NULL AS stage, -- v2.3
             NULL AS interface_load_id -- as per changes done by sagar
        FROM xxgl_balance_stg xbs
       WHERE xbs.process_flag = g_flag_n
         AND xbs.leg_entity = piv_entity
         AND xbs.batch_id = pin_batch_id
         AND xbs.run_sequence_id = pin_run_sequence_id
         AND NOT EXISTS
       (SELECT fc.currency_code
                FROM fnd_currencies fc
               WHERE TRUNC(SYSDATE) BETWEEN
                     NVL(fc.start_date_active, TRUNC(SYSDATE)) AND
                     NVL(fc.end_date_active, TRUNC(SYSDATE + 1))
                 AND fc.currency_code = xbs.leg_currency_code
                 AND fc.enabled_flag = g_flag_y);
  BEGIN
    debug('validate_currency_code>>');

    pov_return_status := g_normal;
    pov_error_message := NULL;

    BEGIN
      OPEN curr_code_cur;
      LOOP
        FETCH curr_code_cur BULK COLLECT
          INTO l_error_tab_type LIMIT g_bulk_limit;
        EXIT WHEN l_error_tab_type.COUNT = 0;
        IF l_error_tab_type.COUNT > 0 THEN
          log_error(pin_batch_id        => pin_batch_id,
                    pin_run_sequence_id => pin_run_sequence_id,
                    p_source_tab_type   => l_error_tab_type,
                    pov_return_status   => l_log_err_sts,
                    pov_error_message   => l_log_err_msg);

          IF l_log_err_msg IS NOT NULL THEN
            debug('validate_currency_code. l_log_err_msg ' ||
                  l_log_err_msg);
          END IF;
          l_error_tab_type.DELETE;
        END IF;
      END LOOP;
      CLOSE curr_code_cur;
    EXCEPTION
      WHEN OTHERS THEN
        debug(SUBSTR('Exception while validating currency code. ' ||
                     SQLERRM,
                     1,
                     1999));
    END;

    debug('validate_currency_code<<');
  EXCEPTION
    WHEN OTHERS THEN
      l_error_message := SUBSTR('Exception in Procedure validate_currency_code. ' ||
                                SQLERRM,
                                1,
                                1999);
      fnd_file.put_line(fnd_file.LOG, l_error_message);
      fnd_file.put_line(fnd_file.LOG, 'validate_currency_code<<E');
      pov_return_status := g_error;
      pov_error_message := l_error_message;
  END validate_currency_code;

  -- =============================================================================
  -- Procedure: validate_actual_flag
  -- =============================================================================
  -- Validate Actual Flag; value must be 'A'
  -- =============================================================================
  --  Input Parameters :
  --    piv_entity           : Entity Name
  --    pin_batch_id         : Batch Id
  --    pin_run_sequence_id  : Run Sequence Id
  --  Output Parameters :
  --    pov_return_status    : Return Status - Normal/Warning/Error
  --    pov_error_message    : Error message in case of any failure
  -- -----------------------------------------------------------------------------
  PROCEDURE validate_actual_flag(piv_entity          IN VARCHAR2,
                                 pin_batch_id        IN NUMBER,
                                 pin_run_sequence_id IN NUMBER,
                                 pov_return_status   OUT NOCOPY VARCHAR2,
                                 pov_error_message   OUT NOCOPY VARCHAR2) IS
    l_error_tab_type xxetn_common_error_pkg.g_source_tab_type;
    l_log_err_sts    VARCHAR2(2000) := NULL;
    l_log_err_msg    VARCHAR2(2000) := NULL;
    l_error_message  VARCHAR2(2000) := NULL;

    CURSOR actual_flag_cur IS
      SELECT g_source_table AS source_table,
             xbs.interface_txn_id AS interface_staging_id,
             NULL AS source_keyname1,
             NULL AS source_keyvalue1,
             NULL AS source_keyname2,
             NULL AS source_keyvalue2,
             NULL AS source_keyname3,
             NULL AS source_keyvalue3,
             NULL AS source_keyname4,
             NULL AS source_keyvalue4,
             NULL AS source_keyname5,
             NULL AS source_keyvalue5,
             'LEG_ACTUAL_FLAG' AS source_column_name,
             xbs.leg_actual_flag AS source_column_value,
             g_err_validation AS error_type,
             'ETN_INVALID_ACTUAL_FLAG' AS error_code,
             'Actual flag not valid, Only Actual journals can be converted' AS error_message,
             NULL AS severity,
             NULL AS proposed_solution,
             NULL AS stage, -- v2.3
             NULL AS interface_load_id -- as per changes done by sagar
        FROM xxgl_balance_stg xbs
       WHERE xbs.process_flag = g_flag_n
         AND xbs.leg_entity = piv_entity
         AND xbs.batch_id = pin_batch_id
         AND xbs.run_sequence_id = pin_run_sequence_id
         AND xbs.leg_actual_flag <> 'A';
  BEGIN
    debug('validate_actual_flag>>');

    pov_return_status := g_normal;
    pov_error_message := NULL;

    BEGIN
      OPEN actual_flag_cur;
      LOOP
        FETCH actual_flag_cur BULK COLLECT
          INTO l_error_tab_type LIMIT g_bulk_limit;
        EXIT WHEN l_error_tab_type.COUNT = 0;
        IF l_error_tab_type.COUNT > 0 THEN
          log_error(pin_batch_id        => pin_batch_id,
                    pin_run_sequence_id => pin_run_sequence_id,
                    p_source_tab_type   => l_error_tab_type,
                    pov_return_status   => l_log_err_sts,
                    pov_error_message   => l_log_err_msg);

          IF l_log_err_msg IS NOT NULL THEN
            debug('validate_actual_flag. l_log_err_msg ' || l_log_err_msg);
          END IF;
          l_error_tab_type.DELETE;
        END IF;
      END LOOP;
      CLOSE actual_flag_cur;
    EXCEPTION
      WHEN OTHERS THEN
        debug(SUBSTR('Exception while validating actual flag. ' || SQLERRM,
                     1,
                     1999));
    END;

    debug('validate_actual_flag<<');
  EXCEPTION
    WHEN OTHERS THEN
      l_error_message := SUBSTR('Exception in Procedure validate_actual_flag. ' ||
                                SQLERRM,
                                1,
                                1999);
      fnd_file.put_line(fnd_file.LOG, l_error_message);
      fnd_file.put_line(fnd_file.LOG, 'validate_actual_flag<<E');
      pov_return_status := g_error;
      pov_error_message := l_error_message;
  END validate_actual_flag;

  -- =============================================================================
  -- Procedure: validate_dr_cr
  -- =============================================================================
  -- Either of the fields Entered_Dr,Entered_Cr,Accounted_Dr,Accounted_Cr must be populated
  -- =============================================================================
  --  Input Parameters :
  --    piv_entity           : Entity Name
  --    pin_batch_id         : Batch Id
  --    pin_run_sequence_id  : Run Sequence Id
  --  Output Parameters :
  --    pov_return_status    : Return Status - Normal/Warning/Error
  --    pov_error_message    : Error message in case of any failure
  -- -----------------------------------------------------------------------------
  PROCEDURE validate_dr_cr(piv_entity          IN VARCHAR2,
                           pin_batch_id        IN NUMBER,
                           pin_run_sequence_id IN NUMBER,
                           pov_return_status   OUT NOCOPY VARCHAR2,
                           pov_error_message   OUT NOCOPY VARCHAR2) IS
    l_error_tab_type xxetn_common_error_pkg.g_source_tab_type;
    l_log_err_sts    VARCHAR2(2000) := NULL;
    l_log_err_msg    VARCHAR2(2000) := NULL;
    l_error_message  VARCHAR2(2000) := NULL;

    CURSOR dr_cr_cur IS
      SELECT g_source_table AS source_table,
             xbs.interface_txn_id AS interface_staging_id,
             NULL AS source_keyname1,
             NULL AS source_keyvalue1,
             NULL AS source_keyname2,
             NULL AS source_keyvalue2,
             NULL AS source_keyname3,
             NULL AS source_keyvalue3,
             NULL AS source_keyname4,
             NULL AS source_keyvalue4,
             NULL AS source_keyname5,
             NULL AS source_keyvalue5,
             'LEG_ACCOUNTED_DR' AS source_column_name,
             xbs.leg_accounted_dr AS source_column_value,
             g_err_validation AS error_type,
             'ETN_INVALID_DR_CR_VALUE' AS error_code,
             'Both Accounted Debit and Credit amounts cannot be zero' AS error_message,
             NULL AS severity,
             NULL AS proposed_solution,
             NULL AS stage, -- v2.3
             NULL AS interface_load_id -- as per changes done by sagar
        FROM xxgl_balance_stg xbs
       WHERE xbs.process_flag = g_flag_n
         AND xbs.leg_entity = piv_entity
         AND xbs.batch_id = pin_batch_id
         AND xbs.run_sequence_id = pin_run_sequence_id
         AND NVL(xbs.leg_accounted_dr, 0) + NVL(xbs.leg_accounted_cr, 0) = 0;
  BEGIN
    debug('validate_dr_cr>>');

    pov_return_status := g_normal;
    pov_error_message := NULL;

    BEGIN
      OPEN dr_cr_cur;
      LOOP
        FETCH dr_cr_cur BULK COLLECT
          INTO l_error_tab_type LIMIT g_bulk_limit;
        EXIT WHEN l_error_tab_type.COUNT = 0;
        IF l_error_tab_type.COUNT > 0 THEN
          log_error(pin_batch_id        => pin_batch_id,
                    pin_run_sequence_id => pin_run_sequence_id,
                    p_source_tab_type   => l_error_tab_type,
                    pov_return_status   => l_log_err_sts,
                    pov_error_message   => l_log_err_msg);

          IF l_log_err_msg IS NOT NULL THEN
            debug('validate_dr_cr. l_log_err_msg ' || l_log_err_msg);
          END IF;
          l_error_tab_type.DELETE;
        END IF;
      END LOOP;
      CLOSE dr_cr_cur;
    EXCEPTION
      WHEN OTHERS THEN
        debug(SUBSTR('Exception while validating Entered_Dr,Entered_Cr,Accounted_Dr,Accounted_Cr. ' ||
                     SQLERRM,
                     1,
                     1999));
    END;

    debug('validate_dr_cr<<');
  EXCEPTION
    WHEN OTHERS THEN
      l_error_message := SUBSTR('Exception in Procedure validate_dr_cr. ' ||
                                SQLERRM,
                                1,
                                1999);
      fnd_file.put_line(fnd_file.LOG, l_error_message);
      fnd_file.put_line(fnd_file.LOG, 'validate_dr_cr<<E');
      pov_return_status := g_error;
      pov_error_message := l_error_message;
  END validate_dr_cr;

  -- =============================================================================
  -- Procedure: validate_accounting_date
  -- =============================================================================
  -- Check Accounting date is a valid date and falls in ?Open? accounting period
  -- =============================================================================
  --  Input Parameters :
  --    piv_entity           : Entity Name
  --    pin_batch_id         : Batch Id
  --    pin_run_sequence_id  : Run Sequence Id
  --  Output Parameters :
  --    pov_return_status    : Return Status - Normal/Warning/Error
  --    pov_error_message    : Error message in case of any failure
  -- -----------------------------------------------------------------------------
  PROCEDURE validate_accounting_date(piv_entity          IN VARCHAR2,
                                     pin_batch_id        IN NUMBER,
                                     pin_run_sequence_id IN NUMBER,
                                     pov_return_status   OUT NOCOPY VARCHAR2,
                                     pov_error_message   OUT NOCOPY VARCHAR2) IS
    l_error_tab_type xxetn_common_error_pkg.g_source_tab_type;
    l_log_err_sts    VARCHAR2(2000) := NULL;
    l_log_err_msg    VARCHAR2(2000) := NULL;
    l_error_message  VARCHAR2(2000) := NULL;

    CURSOR acc_date_cur IS
      SELECT g_source_table AS source_table,
             xbs.interface_txn_id AS interface_staging_id,
             NULL AS source_keyname1,
             NULL AS source_keyvalue1,
             NULL AS source_keyname2,
             NULL AS source_keyvalue2,
             NULL AS source_keyname3,
             NULL AS source_keyvalue3,
             NULL AS source_keyname4,
             NULL AS source_keyvalue4,
             NULL AS source_keyname5,
             NULL AS source_keyvalue5,
             'LEG_ACCOUNTING_DATE' AS source_column_name,
             xbs.leg_accounting_date AS source_column_value,
             g_err_validation AS error_type,
             'ETN_GL_INVALID_PERIOD' AS error_code,
             'Accounting date invalid; lies in a closed period' AS error_message,
             NULL AS severity,
             NULL AS proposed_solution,
             NULL AS stage, -- v2.3
             NULL AS interface_load_id -- as per changes done by sagar
        FROM xxgl_balance_stg xbs
       WHERE xbs.process_flag = g_flag_n
         AND xbs.leg_entity = piv_entity
         AND xbs.batch_id = pin_batch_id
         AND xbs.run_sequence_id = pin_run_sequence_id
         AND NOT EXISTS
       (SELECT 1
                FROM gl_periods gp, gl_period_statuses gps, gl_ledgers gl
               WHERE gp.period_name = gps.period_name
                 AND TRUNC(xbs.accounting_date) BETWEEN TRUNC(gp.start_date) AND
                     TRUNC(gp.end_date)
                 AND gp.period_set_name = gl.period_set_name
                 AND gl.ledger_id = xbs.set_of_books_id
                 AND gl.ledger_id = gps.set_of_books_id
                 AND gps.closing_status IN ('O', 'F')
                 AND gps.application_id =
                     (SELECT fa.application_id
                        FROM fnd_application fa
                       WHERE fa.application_short_name = 'SQLGL'));
  BEGIN
    debug('validate_accounting_date>>');

    pov_return_status := g_normal;
    pov_error_message := NULL;

    BEGIN
      OPEN acc_date_cur;
      LOOP
        FETCH acc_date_cur BULK COLLECT
          INTO l_error_tab_type LIMIT g_bulk_limit;
        EXIT WHEN l_error_tab_type.COUNT = 0;
        IF l_error_tab_type.COUNT > 0 THEN
          log_error(pin_batch_id        => pin_batch_id,
                    pin_run_sequence_id => pin_run_sequence_id,
                    p_source_tab_type   => l_error_tab_type,
                    pov_return_status   => l_log_err_sts,
                    pov_error_message   => l_log_err_msg);

          IF l_log_err_msg IS NOT NULL THEN
            debug('validate_accounting_date. l_log_err_msg ' ||
                  l_log_err_msg);
          END IF;
          l_error_tab_type.DELETE;
        END IF;
      END LOOP;
      CLOSE acc_date_cur;
    EXCEPTION
      WHEN OTHERS THEN
        debug(SUBSTR('Exception while validating accounting date. ' ||
                     SQLERRM,
                     1,
                     1999));
    END;

    debug('validate_accounting_date<<');
  EXCEPTION
    WHEN OTHERS THEN
      l_error_message := SUBSTR('Exception in Procedure validate_accounting_date. ' ||
                                SQLERRM,
                                1,
                                1999);
      fnd_file.put_line(fnd_file.LOG, l_error_message);
      fnd_file.put_line(fnd_file.LOG, 'validate_accounting_date<<E');
      pov_return_status := g_error;
      pov_error_message := l_error_message;
  END validate_accounting_date;

  -- =============================================================================
  -- Procedure: validate_ids_reference
  -- =============================================================================
  -- To verify the values in derivation fields (updated with values derived in R12
  -- by update_ids_reference procedure) in staging table
  -- =============================================================================
  --  Input Parameters :
  --    piv_entity           : Entity Name
  --    pin_batch_id         : Batch Id
  --    pin_run_sequence_id  : Run Sequence Id
  --  Output Parameters :
  --    pov_return_status    : Return Status - Normal/Warning/Error
  --    pov_error_message    : Error message in case of any failure
  -- -----------------------------------------------------------------------------
  PROCEDURE validate_ids_reference(piv_entity          IN VARCHAR2,
                                   pin_batch_id        IN NUMBER,
                                   pin_run_sequence_id IN NUMBER,
                                   pov_return_status   OUT NOCOPY VARCHAR2,
                                   pov_error_message   OUT NOCOPY VARCHAR2) IS
    l_error_tab_type xxetn_common_error_pkg.g_source_tab_type;
    l_error_code CONSTANT VARCHAR2(100) := 'ETN_GL_INVALID_VALUE';

    l_log_err_sts VARCHAR2(2000);
    l_log_err_msg VARCHAR2(2000);

    l_return_status VARCHAR2(1);
    l_error_message VARCHAR2(2000);

    CURSOR sob_id_cur IS
      SELECT g_source_table AS source_table,
             xbs.interface_txn_id AS interface_staging_id,
             NULL AS source_keyname1,
             NULL AS source_keyvalue1,
             NULL AS source_keyname2,
             NULL AS source_keyvalue2,
             NULL AS source_keyname3,
             NULL AS source_keyvalue3,
             NULL AS source_keyname4,
             NULL AS source_keyvalue4,
             NULL AS source_keyname5,
             NULL AS source_keyvalue5,
             'LEG_SEGMENT1' AS source_column_name,
             xbs.leg_segment1 AS source_column_value,
             g_err_validation AS error_type,
             l_error_code AS error_code,
             'R12 Set of Books Id could not be derived, verify Site/Ledger mapping is available in ETN Map Utility and R12 Ledger is setup' AS error_message,
             NULL AS severity,
             NULL AS proposed_solution,
             NULL AS stage, -- v2.3
             NULL AS interface_load_id -- as per changes done by sagar
        FROM xxgl_balance_stg xbs
       WHERE xbs.leg_entity = piv_entity
         AND xbs.batch_id = pin_batch_id
         AND xbs.run_sequence_id = pin_run_sequence_id
         AND xbs.set_of_books_id IS NULL;

    CURSOR coa_id_cur IS
      SELECT g_source_table AS source_table,
             xbs.interface_txn_id AS interface_staging_id,
             NULL AS source_keyname1,
             NULL AS source_keyvalue1,
             NULL AS source_keyname2,
             NULL AS source_keyvalue2,
             NULL AS source_keyname3,
             NULL AS source_keyvalue3,
             NULL AS source_keyname4,
             NULL AS source_keyvalue4,
             NULL AS source_keyname5,
             NULL AS source_keyvalue5,
             'LEG_SEGMENT1' AS source_column_name,
             xbs.leg_segment1 AS source_column_value,
             g_err_validation AS error_type,
             l_error_code AS error_code,
             'R12 Chart of Accounts Id could not be derived, verify Site/Ledger mapping is available in ETN Map Utility and R12 Ledger is setup' AS error_message,
             NULL AS severity,
             NULL AS proposed_solution,
             NULL AS stage, -- v2.3
             NULL AS interface_load_id -- as per changes done by sagar
        FROM xxgl_balance_stg xbs
       WHERE xbs.leg_entity = piv_entity
         AND xbs.batch_id = pin_batch_id
         AND xbs.run_sequence_id = pin_run_sequence_id
         AND xbs.chart_of_accounts_id IS NULL;

    CURSOR func_currency_cur IS
      SELECT g_source_table AS source_table,
             xbs.interface_txn_id AS interface_staging_id,
             NULL AS source_keyname1,
             NULL AS source_keyvalue1,
             NULL AS source_keyname2,
             NULL AS source_keyvalue2,
             NULL AS source_keyname3,
             NULL AS source_keyvalue3,
             NULL AS source_keyname4,
             NULL AS source_keyvalue4,
             NULL AS source_keyname5,
             NULL AS source_keyvalue5,
             'LEG_SEGMENT1' AS source_column_name,
             xbs.leg_segment1 AS source_column_value,
             g_err_validation AS error_type,
             l_error_code AS error_code,
             'R12 Functional Currency Code could not be derived, verify Site/Ledger mapping is available in ETN Map Utility and R12 Ledger is setup' AS error_message,
             NULL AS severity,
             NULL AS proposed_solution,
             NULL AS stage, -- v2.3
             NULL AS interface_load_id -- as per changes done by sagar
        FROM xxgl_balance_stg xbs
       WHERE xbs.leg_entity = piv_entity
         AND xbs.batch_id = pin_batch_id
         AND xbs.run_sequence_id = pin_run_sequence_id
         AND xbs.functional_currency_code IS NULL;

    CURSOR je_cat_name_cur IS
      SELECT g_source_table AS source_table,
             xbs.interface_txn_id AS interface_staging_id,
             NULL AS source_keyname1,
             NULL AS source_keyvalue1,
             NULL AS source_keyname2,
             NULL AS source_keyvalue2,
             NULL AS source_keyname3,
             NULL AS source_keyvalue3,
             NULL AS source_keyname4,
             NULL AS source_keyvalue4,
             NULL AS source_keyname5,
             NULL AS source_keyvalue5,
             'LEG_USER_JE_CAT_NAME' AS source_column_name,
             xbs.leg_user_je_cat_name AS source_column_value,
             g_err_validation AS error_type,
             l_error_code AS error_code,
             'USER_JE_CATEGORY_NAME could not be derived, verify LEG_USER_JE_CAT_NAME and cross reference mapping' AS error_message,
             NULL AS severity,
             NULL AS proposed_solution,
             NULL AS stage, -- v2.3
             NULL AS interface_load_id -- as per changes done by sagar
        FROM xxgl_balance_stg xbs
       WHERE xbs.leg_entity = piv_entity
         AND xbs.batch_id = pin_batch_id
         AND xbs.run_sequence_id = pin_run_sequence_id
         AND xbs.user_je_category_name IS NULL;

    CURSOR je_src_name_cur IS
      SELECT g_source_table AS source_table,
             xbs.interface_txn_id AS interface_staging_id,
             NULL AS source_keyname1,
             NULL AS source_keyvalue1,
             NULL AS source_keyname2,
             NULL AS source_keyvalue2,
             NULL AS source_keyname3,
             NULL AS source_keyvalue3,
             NULL AS source_keyname4,
             NULL AS source_keyvalue4,
             NULL AS source_keyname5,
             NULL AS source_keyvalue5,
             'LEG_USER_JE_SRC_NAME' AS source_column_name,
             xbs.leg_user_je_src_name AS source_column_value,
             g_err_validation AS error_type,
             l_error_code AS error_code,
             'USER_JE_SOURCE_NAME could not be derived, verify LEG_USER_JE_SRC_NAME and cross reference mapping' AS error_message,
             NULL AS severity,
             NULL AS proposed_solution,
             NULL AS stage, -- v2.3
             NULL AS interface_load_id -- as per changes done by sagar
        FROM xxgl_balance_stg xbs
       WHERE xbs.leg_entity = piv_entity
         AND xbs.batch_id = pin_batch_id
         AND xbs.run_sequence_id = pin_run_sequence_id
         AND xbs.user_je_source_name IS NULL;

    CURSOR period_dtls_cur IS
      SELECT g_source_table AS source_table,
             xbs.interface_txn_id AS interface_staging_id,
             NULL AS source_keyname1,
             NULL AS source_keyvalue1,
             NULL AS source_keyname2,
             NULL AS source_keyvalue2,
             NULL AS source_keyname3,
             NULL AS source_keyvalue3,
             NULL AS source_keyname4,
             NULL AS source_keyvalue4,
             NULL AS source_keyname5,
             NULL AS source_keyvalue5,
             'LEG_ACCOUNTING_DATE' AS source_column_name,
             xbs.leg_accounting_date AS source_column_value,
             g_err_validation AS error_type,
             l_error_code AS error_code,
             'Accounting Period details (Name, Start/End Date, Year and Number) could not be derived, because no Open GL period found for corresponding Accounting Date' AS error_message,
             NULL AS severity,
             NULL AS proposed_solution,
             NULL AS stage, -- v2.3
             NULL AS interface_load_id -- as per changes done by sagar
        FROM xxgl_balance_stg xbs
       WHERE xbs.leg_entity = piv_entity
         AND xbs.batch_id = pin_batch_id
         AND xbs.run_sequence_id = pin_run_sequence_id
         AND (xbs.period_name IS NULL OR xbs.start_date IS NULL OR
             xbs.end_date IS NULL OR xbs.period_year IS NULL OR
             xbs.period_num IS NULL);

    CURSOR ccid_cur IS
      SELECT g_source_table AS source_table,
             xbs.interface_txn_id AS interface_staging_id,
             NULL AS source_keyname1,
             NULL AS source_keyvalue1,
             NULL AS source_keyname2,
             NULL AS source_keyvalue2,
             NULL AS source_keyname3,
             NULL AS source_keyvalue3,
             NULL AS source_keyname4,
             NULL AS source_keyvalue4,
             NULL AS source_keyname5,
             NULL AS source_keyvalue5,
             'CODE_COMBINATION_ID' AS source_column_name,
             xbs.leg_segment1 || g_char_period || xbs.leg_segment2 ||
             g_char_period || xbs.leg_segment3 || g_char_period ||
             xbs.leg_segment4 || g_char_period || xbs.leg_segment5 ||
             g_char_period || xbs.leg_segment6 || g_char_period ||
             xbs.leg_segment7 AS source_column_value,
             g_err_validation AS error_type,
             l_error_code AS error_code,
             leg_reference19 AS error_message,
             NULL AS severity,
             NULL AS proposed_solution,
             NULL AS stage, -- v2.3
             NULL AS interface_load_id -- as per changes done by sagar
        FROM xxgl_balance_stg xbs
       WHERE xbs.leg_entity = piv_entity
         AND xbs.batch_id = pin_batch_id
         AND xbs.run_sequence_id = pin_run_sequence_id
         AND xbs.code_combination_id IS NULL;
  BEGIN
    debug('validate_ids_reference >>');

    pov_return_status := g_normal;
    pov_error_message := NULL;

    SAVEPOINT update_ids_reference_sp;

    --Checking records for which R12 Set of Books not updated in staging table
    BEGIN
      debug('Checking R12 Set of Books');
      OPEN sob_id_cur;
      LOOP
        FETCH sob_id_cur BULK COLLECT
          INTO l_error_tab_type LIMIT g_bulk_limit;
        EXIT WHEN l_error_tab_type.COUNT = 0;
        IF l_error_tab_type.COUNT > 0 THEN
          log_error(pin_batch_id        => pin_batch_id,
                    pin_run_sequence_id => pin_run_sequence_id,
                    p_source_tab_type   => l_error_tab_type,
                    pov_return_status   => l_log_err_sts,
                    pov_error_message   => l_log_err_msg);

          IF l_log_err_msg IS NOT NULL THEN
            debug('Checking R12 Set of Books. l_log_err_msg ' ||
                  l_log_err_msg);
          END IF;
          l_error_tab_type.DELETE;
        END IF;
      END LOOP;
      CLOSE sob_id_cur;
    EXCEPTION
      WHEN OTHERS THEN
        debug(SUBSTR('Exception while Checking R12 Set of Books. ' ||
                     SQLERRM,
                     1,
                     1999));
    END;

    --Checking records for which R12 Chart of Accounts Id not updated in staging table
    BEGIN
      debug('Checking R12 Chart of Accounts Id');
      OPEN coa_id_cur;
      LOOP
        FETCH coa_id_cur BULK COLLECT
          INTO l_error_tab_type LIMIT g_bulk_limit;
        EXIT WHEN l_error_tab_type.COUNT = 0;
        IF l_error_tab_type.COUNT > 0 THEN
          log_error(pin_batch_id        => pin_batch_id,
                    pin_run_sequence_id => pin_run_sequence_id,
                    p_source_tab_type   => l_error_tab_type,
                    pov_return_status   => l_log_err_sts,
                    pov_error_message   => l_log_err_msg);

          IF l_log_err_msg IS NOT NULL THEN
            debug('Checking R12 Chart of Accounts Id. l_log_err_msg' ||
                  l_log_err_msg);
          END IF;
          l_error_tab_type.DELETE;
        END IF;
      END LOOP;
      CLOSE coa_id_cur;
    EXCEPTION
      WHEN OTHERS THEN
        debug(SUBSTR('Exception while Checking R12 Chart of Accounts Id. ' ||
                     SQLERRM,
                     1,
                     1999));
    END;

    --Checking records for which R12 Functional Currency Code not updated in staging table
    BEGIN
      debug('Checking R12 Functional Currency Code');
      OPEN func_currency_cur;
      LOOP
        FETCH func_currency_cur BULK COLLECT
          INTO l_error_tab_type LIMIT g_bulk_limit;
        EXIT WHEN l_error_tab_type.COUNT = 0;
        IF l_error_tab_type.COUNT > 0 THEN
          log_error(pin_batch_id        => pin_batch_id,
                    pin_run_sequence_id => pin_run_sequence_id,
                    p_source_tab_type   => l_error_tab_type,
                    pov_return_status   => l_log_err_sts,
                    pov_error_message   => l_log_err_msg);

          IF l_log_err_msg IS NOT NULL THEN
            debug('Checking R12 Functional Currency Code. l_log_err_msg' ||
                  l_log_err_msg);
          END IF;
          l_error_tab_type.DELETE;
        END IF;
      END LOOP;
      CLOSE func_currency_cur;
    EXCEPTION
      WHEN OTHERS THEN
        debug(SUBSTR('Exception while Checking R12 Functional Currency Code. ' ||
                     SQLERRM,
                     1,
                     1999));
    END;

    --Checking records for which user_je_category_name not updated in staging table
    BEGIN
      debug('Checking user_je_category_name');
      OPEN je_cat_name_cur;
      LOOP
        FETCH je_cat_name_cur BULK COLLECT
          INTO l_error_tab_type LIMIT g_bulk_limit;
        EXIT WHEN l_error_tab_type.COUNT = 0;
        IF l_error_tab_type.COUNT > 0 THEN
          log_error(pin_batch_id        => pin_batch_id,
                    pin_run_sequence_id => pin_run_sequence_id,
                    p_source_tab_type   => l_error_tab_type,
                    pov_return_status   => l_log_err_sts,
                    pov_error_message   => l_log_err_msg);

          IF l_log_err_msg IS NOT NULL THEN
            debug('Checking user_je_category_name. l_log_err_msg' ||
                  l_log_err_msg);
          END IF;
          l_error_tab_type.DELETE;
        END IF;
      END LOOP;
      CLOSE je_cat_name_cur;
    EXCEPTION
      WHEN OTHERS THEN
        debug(SUBSTR('Exception while Updating SOB Id, Currency COA Id, Journal Source and Category. ' ||
                     SQLERRM,
                     1,
                     1999));
    END;

    --Checking records for which user_je_source_name not updated in staging table
    BEGIN
      debug('Checking user_je_source_name');
      OPEN je_src_name_cur;
      LOOP
        FETCH je_src_name_cur BULK COLLECT
          INTO l_error_tab_type LIMIT g_bulk_limit;
        EXIT WHEN l_error_tab_type.COUNT = 0;
        IF l_error_tab_type.COUNT > 0 THEN
          log_error(pin_batch_id        => pin_batch_id,
                    pin_run_sequence_id => pin_run_sequence_id,
                    p_source_tab_type   => l_error_tab_type,
                    pov_return_status   => l_log_err_sts,
                    pov_error_message   => l_log_err_msg);

          IF l_log_err_msg IS NOT NULL THEN
            debug('Checking user_je_source_name. l_log_err_msg' ||
                  l_log_err_msg);
          END IF;
          l_error_tab_type.DELETE;
        END IF;
      END LOOP;
      CLOSE je_src_name_cur;
    EXCEPTION
      WHEN OTHERS THEN
        debug(SUBSTR('Exception while Checking user_je_source_name. ' ||
                     SQLERRM,
                     1,
                     1999));
    END;

    --Checking records for which Accounting Period details not updated in staging table
    BEGIN
      debug('Checking Accounting Period details');
      OPEN period_dtls_cur;
      LOOP
        FETCH period_dtls_cur BULK COLLECT
          INTO l_error_tab_type LIMIT g_bulk_limit;
        EXIT WHEN l_error_tab_type.COUNT = 0;
        IF l_error_tab_type.COUNT > 0 THEN
          log_error(pin_batch_id        => pin_batch_id,
                    pin_run_sequence_id => pin_run_sequence_id,
                    p_source_tab_type   => l_error_tab_type,
                    pov_return_status   => l_log_err_sts,
                    pov_error_message   => l_log_err_msg);

          IF l_log_err_msg IS NOT NULL THEN
            debug('Checking Accounting Period details. l_log_err_msg' ||
                  l_log_err_msg);
          END IF;
          l_error_tab_type.DELETE;
        END IF;
      END LOOP;
      CLOSE period_dtls_cur;
    EXCEPTION
      WHEN OTHERS THEN
        debug(SUBSTR('Exception while Checking Accounting Period details. ' ||
                     SQLERRM,
                     1,
                     1999));
    END;

    --Checking records for which code_combination_id not updated in staging table
    BEGIN
      debug('Checking derived code_combination_id');
      OPEN ccid_cur;
      LOOP
        FETCH ccid_cur BULK COLLECT
          INTO l_error_tab_type LIMIT g_bulk_limit;
        EXIT WHEN l_error_tab_type.COUNT = 0;
        IF l_error_tab_type.COUNT > 0 THEN
          log_error(pin_batch_id        => pin_batch_id,
                    pin_run_sequence_id => pin_run_sequence_id,
                    p_source_tab_type   => l_error_tab_type,
                    pov_return_status   => l_log_err_sts,
                    pov_error_message   => l_log_err_msg);

          IF l_log_err_msg IS NOT NULL THEN
            debug('Checking derived code_combination_id. l_log_err_msg' ||
                  l_log_err_msg);
          END IF;
          l_error_tab_type.DELETE;
        END IF;
      END LOOP;
      CLOSE ccid_cur;
    EXCEPTION
      WHEN OTHERS THEN
        debug(SUBSTR('Exception while Checking derived code_combination_id. ' ||
                     SQLERRM,
                     1,
                     1999));
    END;

    COMMIT;
    debug('validate_ids_reference <<');
  EXCEPTION
    WHEN OTHERS THEN
      l_error_message := SUBSTR('Exception in Procedure validate_ids_reference. ' ||
                                SQLERRM,
                                1,
                                1999);
      fnd_file.put_line(fnd_file.LOG, l_error_message);
      fnd_file.put_line(fnd_file.LOG, 'validate_ids_reference <<E');

      pov_return_status := g_error;
      pov_error_message := l_error_message;
  END validate_ids_reference;

  -- =============================================================================
  -- Procedure: update_ids_reference
  -- =============================================================================
  -- To updated derivation fields in staging table with the values derived in R12
  -- =============================================================================
  --  Input Parameters :
  --    piv_entity           : Entity Name
  --    pin_batch_id         : Batch Id
  --    pin_run_sequence_id  : Run Sequence Id
  --  Output Parameters :
  --    pov_return_status    : Return Status - Normal/Warning/Error
  --    pov_error_message    : Error message in case of any failure
  -- -----------------------------------------------------------------------------
  PROCEDURE update_ids_reference(piv_entity          IN VARCHAR2,
                                 pin_batch_id        IN NUMBER,
                                 pin_run_sequence_id IN NUMBER,
                                 pov_return_status   OUT NOCOPY VARCHAR2,
                                 pov_error_message   OUT NOCOPY VARCHAR2) IS
    PRAGMA AUTONOMOUS_TRANSACTION;

    l_return_status VARCHAR2(1);
    l_error_message VARCHAR2(2000);

    l_batch_id NUMBER;
    process_exception EXCEPTION;
    l_cc_id NUMBER;

    CURSOR update_ids_cur IS
      SELECT xbs.interface_txn_id,
             xbs.leg_ledger_name,
             xbs.leg_source_system,
             xbs.leg_user_je_cat_name,
             xbs.leg_user_je_src_name,
             xbs.leg_segment1 ,-- added for v2.0,
             xbs.leg_accounting_date
        FROM xxgl_balance_stg xbs
       WHERE xbs.leg_entity = piv_entity
         AND xbs.batch_id = pin_batch_id
         AND xbs.run_sequence_id = pin_run_sequence_id;

    TYPE update_ids_tbl IS TABLE OF update_ids_cur%ROWTYPE;

    l_update_ids_tbl update_ids_tbl;

  BEGIN
    debug('update_ids_reference >>');

    pov_return_status := g_normal;
    pov_error_message := NULL;

    SAVEPOINT update_ids_reference_sp;

    --Updating SOB Id, Currency COA Id, Journal Source and Category in Staging table
    debug('Updating SOB Id, Currency COA Id, Journal Source and Category');

    -- Open Cursor
    OPEN update_ids_cur;
    LOOP
      FETCH update_ids_cur BULK COLLECT
        INTO l_update_ids_tbl LIMIT g_bulk_limit;

      BEGIN
        FORALL idx IN 1 .. l_update_ids_tbl.COUNT SAVE EXCEPTIONS

          UPDATE xxgl_balance_stg xbs
             SET (set_of_books_id,
                  functional_currency_code,
                  chart_of_accounts_id)
                 /**=  (  SELECT gl.ledger_id
                       ,  gl.currency_code
                       ,  gl.chart_of_accounts_id
                   FROM   gl_ledgers        gl
                       ,  fnd_lookup_values flv
                   WHERE TRIM(flv.attribute3)   = gl.name
                   AND   flv.lookup_type        = g_xr_books_mapping
                   AND   flv.enabled_flag       = g_flag_y
                   AND   TRUNC(SYSDATE) BETWEEN NVL(flv.start_date_active, TRUNC(SYSDATE)) AND NVL(flv.end_date_active, TRUNC(SYSDATE+1))
                   AND   flv.language           = USERENV('LANG')
                   AND   flv.attribute_category = g_xr_books_mapping
                   AND   flv.attribute2         = l_update_ids_tbl (idx).leg_ledger_name
                   AND   flv.attribute1         = l_update_ids_tbl (idx).leg_source_system
                   AND   ROWNUM = 1
                 )**/

                  =
                 (SELECT gl.ledger_id,
                         gl.currency_code,
                         gl.chart_of_accounts_id
                    FROM gl_ledgers gl, xxetn_map_unit xmu
                   WHERE NVL(xmu.ledger, 'XXXXXX') = gl.name
                     AND xmu.enabled_flag = 'Y'
                     /*AND TRUNC(SYSDATE) BETWEEN
                         NVL(xmu.start_date_active, TRUNC(SYSDATE)) AND
                         NVL(xmu.end_date_active, TRUNC(SYSDATE + 1))*/
                     AND TRUNC(l_update_ids_tbl(idx).leg_accounting_date) BETWEEN   -----Changed By Harjinder Singh
                         NVL(xmu.start_date_active, TRUNC(SYSDATE)) AND
                         NVL(xmu.end_date_active, TRUNC(SYSDATE + 1))
                          AND xmu.site = l_update_ids_tbl(idx).leg_segment1
                     AND ROWNUM = 1)

                ,
                 user_je_category_name =
                 (SELECT /*gl.je_category_key -- Changed as per defect 1803 */
                   gl.user_je_category_name
                    FROM gl_je_categories gl, fnd_lookup_values flv
                   WHERE TRIM(flv.attribute3) = gl.user_je_category_name
                     AND flv.lookup_type = g_xr_journal_category
                     AND flv.enabled_flag = g_flag_y
                     AND TRUNC(SYSDATE) BETWEEN
                         NVL(flv.start_date_active, TRUNC(SYSDATE)) AND
                         NVL(flv.end_date_active, TRUNC(SYSDATE + 1))
                     AND flv.language = USERENV('LANG')
                     AND flv.attribute_category = g_xr_journal_category
                     AND flv.attribute2 = l_update_ids_tbl(idx)
                        .leg_user_je_cat_name
                     AND flv.attribute1 = l_update_ids_tbl(idx)
                        .leg_source_system
                     AND gl.language = USERENV('LANG')
                     AND ROWNUM = 1),
                 user_je_source_name   =
                 (SELECT gl.je_source_key
                    FROM gl_je_sources gl, fnd_lookup_values flv
                   WHERE TRIM(flv.attribute3) = gl.user_je_source_name
                     AND flv.lookup_type = g_xr_journal_source
                     AND flv.enabled_flag = g_flag_y
                     AND TRUNC(SYSDATE) BETWEEN
                         NVL(flv.start_date_active, TRUNC(SYSDATE)) AND
                         NVL(flv.end_date_active, TRUNC(SYSDATE + 1))
                     AND flv.language = USERENV('LANG')
                     AND flv.attribute_category = g_xr_journal_source
                     AND flv.attribute2 = l_update_ids_tbl(idx)
                        .leg_user_je_src_name
                     AND flv.attribute1 = l_update_ids_tbl(idx)
                        .leg_source_system
                     AND gl.language = USERENV('LANG')
                     AND ROWNUM = 1),
                 last_updated_date      = SYSDATE,
                 last_updated_by        = g_user_id,
                 last_update_login      = g_login_id,
                 program_application_id = g_prog_appl_id,
                 program_id             = g_program_id,
                 program_update_date    = SYSDATE,
                 request_id             = g_request_id
           WHERE xbs.interface_txn_id = l_update_ids_tbl(idx)
                .interface_txn_id;

      EXCEPTION
        WHEN OTHERS THEN
          FOR l_indx IN 1 .. SQL%BULK_EXCEPTIONS.COUNT LOOP
            debug('Error Interface Txn Id: ' || l_update_ids_tbl(SQL%BULK_EXCEPTIONS(l_indx).error_index)
                  .interface_txn_id);
            debug('Error Message: ' ||
                  SQLERRM(-SQL%BULK_EXCEPTIONS(l_indx).error_code));
          END LOOP;
      END;

      EXIT WHEN l_update_ids_tbl.COUNT = 0;

    END LOOP;
    CLOSE update_ids_cur;

    --Updating Period Name, Start/End Date, Period Name and Year in staging table
    BEGIN
      debug('Updating Period Name, Start/End Date, Period Name and Year');
      UPDATE xxgl_balance_stg xbs
         SET (period_name, start_date, end_date, period_year, period_num) =
             (SELECT gp.period_name,
                     gp.start_date,
                     gp.end_date,
                     gp.period_year,
                     gp.period_num
                FROM gl_periods gp, gl_ledgers gl, gl_period_statuses gps
               WHERE TRUNC(leg_accounting_date) BETWEEN TRUNC(gp.start_date) AND
                     TRUNC(gp.end_date)
                 AND gp.period_set_name = gl.period_set_name
                 AND gl.ledger_id = xbs.set_of_books_id
                 AND gp.adjustment_period_flag <> g_flag_y
                 AND gl.ledger_id = gps.set_of_books_id
                 AND gps.application_id =
                     (SELECT fa.application_id
                        FROM fnd_application fa
                       WHERE fa.application_short_name = 'SQLGL')
                 AND gp.period_name = gps.period_name)
       WHERE leg_entity = piv_entity
         AND batch_id = pin_batch_id
         AND run_sequence_id = pin_run_sequence_id;

      debug('02. No. of records updated : ' || SQL%ROWCOUNT);
    EXCEPTION
      WHEN OTHERS THEN
        debug(SUBSTR('Exception while Updating Period Name, Start/End Date, Period Name and Year. ' ||
                     SQLERRM,
                     1,
                     1999));
    END;

    --Updating Reference1, Reference2, Reference4, Reference5 and Reference10 in staging table based on Entity
    --IF (piv_entity = g_entity_openbalance) THEN                             -- commented for v2.2
    IF piv_entity IN (g_entity_openbalance) THEN
      -- added for v2.2
      BEGIN
        debug('Updating Reference1, Reference2, Reference4, Reference5 and Reference10 for entity OPEN-BALANCE');

        UPDATE xxgl_balance_stg xbs
           SET accounting_date = DECODE(piv_entity,
                                        g_entity_openbalance,
                                        start_date,
                                        end_date) -- added for v2.2
              ,
               leg_reference1  = leg_source_system || g_char_hyphen ||
                                 'Conversion Opening Balances' ||
                                 g_char_hyphen || period_name ||
                                 g_char_hyphen || leg_currency_code ||
                                 g_char_hyphen || leg_sob_short_name,
               leg_reference2  = 'Conversion Opening Journal' ||
                                 g_char_hyphen || period_name,
               leg_reference4  = leg_source_system || g_char_hyphen ||
                                 'Conversion Opening Balances' ||
                                 g_char_hyphen || Period_name ||
                                 g_char_hyphen || leg_currency_code ||
                                 g_char_hyphen || leg_sob_short_name,
               leg_reference5  = 'Conversion Opening Journal' ||
                                 g_char_hyphen || period_name,
               leg_reference10 = SUBSTRB('Legacy account: ' || g_char_hyphen ||
                                         leg_segment1 || g_char_period ||
                                         leg_segment2 || g_char_period ||
                                         leg_segment3 || g_char_period ||
                                         leg_segment4 || g_char_period ||
                                         leg_segment5 || g_char_period ||
                                         leg_segment6 || g_char_period ||
                                         leg_segment7,
                                         1,
                                         240),
               group_id        = set_of_books_id || period_year ||
                                 period_num ||
                                 DECODE(leg_source_system, 'ISSC', 1, 2) || '1'
         WHERE leg_entity = piv_entity
           AND batch_id = pin_batch_id
           AND run_sequence_id = pin_run_sequence_id;

        debug('03.a. No. of records updated : ' || SQL%ROWCOUNT);
      EXCEPTION
        WHEN OTHERS THEN
          debug(SUBSTR('Exception while Updating Reference1, Reference2, Reference4, Reference5 and Reference10 for entity OPEN-BALANCE. ' ||
                       SQLERRM,
                       1,
                       1999));
      END;
       ---Added Changes for CR 374261 v2.7 START
        ELSIF piv_entity IN (g_entity_foreignbalance) THEN
        BEGIN
          debug('Updating Reference1, Reference2, Reference4, Reference5 and Reference10 for entity FOREIGN-BALANCE');

          UPDATE xxgl_balance_stg xbs
             SET accounting_date = DECODE(piv_entity,
                                          g_entity_openbalance,
                                          start_date,
                                          end_date)

                 ,leg_reference1  = leg_source_system || g_char_hyphen ||
                                    'Conversion Foreign Balances' ||
                                    g_char_hyphen || period_name ||
                                    g_char_hyphen || leg_currency_code ||
                                    g_char_hyphen || leg_sob_short_name
                 ,leg_reference2  = 'Conversion Foreign Journal' ||
                                    g_char_hyphen || period_name
                 ,leg_reference4  = leg_source_system || g_char_hyphen ||
                                   'Conversion Foreign Balances' ||
                                    g_char_hyphen || Period_name ||
                                    g_char_hyphen || leg_currency_code ||
                                    g_char_hyphen || leg_sob_short_name
                 ,leg_reference5  = 'Conversion Foreign Journal' ||
                                    g_char_hyphen || period_name
                 ,leg_reference10 = SUBSTRB('Legacy account: ' || g_char_hyphen ||
                                           leg_segment1 || g_char_period ||
                                           leg_segment2 || g_char_period ||
                                           leg_segment3 || g_char_period ||
                                           leg_segment4 || g_char_period ||
                                           leg_segment5 || g_char_period ||
                                           leg_segment6 || g_char_period ||
                                           leg_segment7,
                                           1,
                                           240),
                 group_id        = set_of_books_id || period_year ||
                                   period_num ||
                                   DECODE(leg_source_system, 'ISSC', 1, 2) || '1'
           WHERE leg_entity = piv_entity
             AND batch_id = pin_batch_id
             AND run_sequence_id = pin_run_sequence_id;

          debug('03.b. No. of records updated : ' || SQL%ROWCOUNT);
        EXCEPTION
          WHEN OTHERS THEN
            debug(SUBSTR('Exception while Updating Reference1, Reference2, Reference4, Reference5 and Reference10 for entity FOREIGN-BALANCE. ' ||
                         SQLERRM,
                         1,
                         1999));
        END;
        ---Added Changes for CR 374261 v2.7 END

    ELSIF (piv_entity = g_entity_monthbalance) THEN
      BEGIN
        debug('Updating Reference1, Reference2, Reference4, Reference5 and Reference10 for entity MONTH-BALANCE');

        UPDATE xxgl_balance_stg xbs
           SET accounting_date = end_date,
               leg_reference1  = leg_source_system || g_char_hyphen ||
                                 'Conversion Balances' || g_char_hyphen ||
                                 period_name || g_char_hyphen ||
                                 leg_currency_code || g_char_hyphen ||
                                 leg_sob_short_name,
               leg_reference2  = 'Conversion Journal' || g_char_hyphen ||
                                 period_name,
               leg_reference4  = leg_source_system || g_char_hyphen ||
                                 'Conversion Balances' || g_char_hyphen ||
                                 Period_name || g_char_hyphen ||
                                 leg_currency_code || g_char_hyphen ||
                                 leg_sob_short_name,
               leg_reference5  = 'Conversion Journal ' || g_char_hyphen ||
                                 period_name,
               leg_reference10 = SUBSTRB('Legacy account: ' || g_char_hyphen ||
                                         leg_segment1 || g_char_period ||
                                         leg_segment2 || g_char_period ||
                                         leg_segment3 || g_char_period ||
                                         leg_segment4 || g_char_period ||
                                         leg_segment5 || g_char_period ||
                                         leg_segment6 || g_char_period ||
                                         leg_segment7,
                                         1,
                                         240),
               group_id        = set_of_books_id || period_year ||
                                 period_num ||
                                 DECODE(leg_source_system, 'ISSC', 1, 2) || '2'
         WHERE leg_entity = piv_entity
           AND batch_id = pin_batch_id
           AND run_sequence_id = pin_run_sequence_id;

        debug('03.b. No. of records updated : ' || SQL%ROWCOUNT);
      EXCEPTION
        WHEN OTHERS THEN
          debug(SUBSTR('Exception while Updating Reference1, Reference2, Reference4, Reference5 and Reference10 for entity MONTH-BALANCE. ' ||
                       SQLERRM,
                       1,
                       1999));
      END;
    ELSIF (piv_entity = g_entity_journal) THEN
      BEGIN
        debug('Updating Reference1, Reference2, Reference4, Reference5 and Reference10 for entity JOURNAL');

        UPDATE xxgl_balance_stg xbs
           SET accounting_date = leg_accounting_date,
               leg_reference1  = SUBSTRB(leg_source_system || g_char_hyphen ||
                                         leg_batch_name,
                                         1,
                                         100),
               leg_reference2  = leg_batch_description,
               leg_reference4  = leg_journal_name,
               leg_reference5  = leg_journal_description,
               leg_reference10 = SUBSTRB(leg_segment1 || g_char_period ||
                                         leg_segment2 || g_char_period ||
                                         leg_segment3 || g_char_period ||
                                         leg_segment4 || g_char_period ||
                                         leg_segment5 || g_char_period ||
                                         leg_segment6 || g_char_period ||
                                         leg_segment7 || ' : ' ||
                                         leg_je_line_num || ' : ' ||
                                         leg_je_line_description,
                                         1,
                                         240),
               group_id        = set_of_books_id || period_year ||
                                 period_num ||
                                 DECODE(leg_source_system, 'ISSC', 1, 2) || '3'
         WHERE leg_entity = piv_entity
           AND batch_id = pin_batch_id
           AND run_sequence_id = pin_run_sequence_id;

        debug('03.c. No. of records updated : ' || SQL%ROWCOUNT);
      EXCEPTION
        WHEN OTHERS THEN
          debug(SUBSTR('Exception while Updating Reference1, Reference2, Reference4, Reference5 and Reference10 for entity JOURNAL. ' ||
                       SQLERRM,
                       1,
                       1999));
      END;
    END IF;

    --Updating Code Combination Id in staging table

    BEGIN
      get_code_combination_id(piv_entity          => piv_entity,
                              pin_batch_id        => pin_batch_id,
                              pin_run_sequence_id => pin_run_sequence_id,
                              pov_return_status   => l_return_status,
                              pov_error_message   => l_error_message);

      debug('get_code_combination_id->l_return_status ' || l_return_status);
      debug('get_code_combination_id->l_error_message ' || l_error_message);
    EXCEPTION
      WHEN OTHERS THEN
        debug(SUBSTR('Exception while Updating Code Combination Id. ' ||
                     SQLERRM,
                     1,
                     1999));
    END;

    COMMIT;
    debug('update_ids_reference <<');
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK TO update_ids_reference_sp;
      l_error_message := SUBSTR('Exception in Procedure update_ids_reference. ' ||
                                SQLERRM,
                                1,
                                1999);
      fnd_file.put_line(fnd_file.LOG, l_error_message);
      fnd_file.put_line(fnd_file.LOG, 'update_ids_reference <<E');
      pov_return_status := g_error;
      pov_error_message := l_error_message;
  END update_ids_reference;

  -- =============================================================================
  -- Procedure: check_required
  -- =============================================================================
  -- Check required/mandatory columns as following:
  --    > LEG_LEDGER_NAME       > LEG_ACCOUNTING_DATE
  --    > LEG_CURRENCY_CODE     > LEG_ACTUAL_FLAG
  --    > LEG_USER_JE_CAT_NAME  > LEG_USER_JE_SRC_NAME
  --    > LEG_DATE_CREATED_IN_GL
  -- =============================================================================
  --  Input Parameters :
  --    piv_entity           : Entity Name
  --    pin_batch_id         : Batch Id
  --    pin_run_sequence_id  : Run Sequence Id
  --  Output Parameters :
  --    pov_return_status    : Return Status - Normal/Warning/Error
  --    pov_error_message    : Error message in case of any failure
  -- -----------------------------------------------------------------------------
  PROCEDURE check_required(piv_entity          IN VARCHAR2,
                           pin_batch_id        IN NUMBER,
                           pin_run_sequence_id IN NUMBER,
                           pov_return_status   OUT NOCOPY VARCHAR2,
                           pov_error_message   OUT NOCOPY VARCHAR2) IS
    l_error_tab_type xxetn_common_error_pkg.g_source_tab_type;
    l_error_code CONSTANT VARCHAR2(100) := 'ETN_GL_MANDATORY_NOT_ENTERED';

    l_log_err_sts VARCHAR2(2000);
    l_log_err_msg VARCHAR2(2000);

    l_error_message VARCHAR2(2000);
  BEGIN
    debug('check_required >>');

    --Validate SOB Name for Required value
    BEGIN
      debug('Validate SOB Name for Required value');
      SELECT g_source_table --source_table
            ,
             xbs.interface_txn_id --interface_staging_id
            ,
             NULL --source_keyname1
            ,
             NULL --source_keyvalue1
            ,
             NULL --source_keyname2
            ,
             NULL --source_keyvalue2
            ,
             NULL --source_keyname3
            ,
             NULL --source_keyvalue3
            ,
             NULL --source_keyname4
            ,
             NULL --source_keyvalue4
            ,
             NULL --source_keyname5
            ,
             NULL --source_keyvalue5
            ,
             'LEG_LEDGER_NAME' --source_column_name
            ,
             xbs.leg_ledger_name --source_column_value
            ,
             g_err_validation --error_type
            ,
             l_error_code --error_code
            ,
             'Legacy Set of Books not entered' --error_message
            ,
             NULL --severity
            ,
             NULL --proposed_solution
            ,
             NULL, --stage    -- v2.3
             NULL -- ASP PER CHANGES DONE BY SAGAR
             BULK COLLECT
        INTO l_error_tab_type
        FROM xxgl_balance_stg xbs
       WHERE xbs.process_flag IN (g_flag_v, g_flag_n, g_flag_e)
         AND xbs.leg_entity = piv_entity
         AND xbs.batch_id = pin_batch_id
         AND xbs.run_sequence_id = pin_run_sequence_id
         AND xbs.leg_ledger_name IS NULL;

      IF l_error_tab_type.COUNT > 0 THEN
        log_error(pin_batch_id        => pin_batch_id,
                  pin_run_sequence_id => pin_run_sequence_id,
                  p_source_tab_type   => l_error_tab_type,
                  pov_return_status   => l_log_err_sts,
                  pov_error_message   => l_log_err_msg);

        l_error_tab_type.DELETE;
      END IF;
    EXCEPTION
      WHEN OTHERS THEN
        debug('Exception occured while fetching records for LEG_LEDGER_NAME IS NULL.' ||
              SQLERRM);
        l_error_tab_type.DELETE;
    END;

    --Validate LEG_ACCOUNTING_DATE for Required value
    BEGIN
      debug('Validate LEG_ACCOUNTING_DATE for Required value');
      SELECT g_source_table --source_table
            ,
             xbs.interface_txn_id --interface_staging_id
            ,
             NULL --source_keyname1
            ,
             NULL --source_keyvalue1
            ,
             NULL --source_keyname2
            ,
             NULL --source_keyvalue2
            ,
             NULL --source_keyname3
            ,
             NULL --source_keyvalue3
            ,
             NULL --source_keyname4
            ,
             NULL --source_keyvalue4
            ,
             NULL --source_keyname5
            ,
             NULL --source_keyvalue5
            ,
             'LEG_ACCOUNTING_DATE' --source_column_name
            ,
             xbs.leg_accounting_date --source_column_value
            ,
             g_err_validation --error_type
            ,
             l_error_code --error_code
            ,
             'Legacy Accounting Date not entered' --error_message
            ,
             NULL --severity
            ,
             NULL --proposed_solution
            ,
             NULL, --stage   -- v2.3
             NULL -- as per changes done by sagar
             BULK COLLECT
        INTO l_error_tab_type
        FROM xxgl_balance_stg xbs
       WHERE xbs.process_flag IN (g_flag_v, g_flag_n, g_flag_e)
         AND xbs.leg_entity = piv_entity
         AND xbs.batch_id = pin_batch_id
         AND xbs.run_sequence_id = pin_run_sequence_id
         AND xbs.leg_accounting_date IS NULL;

      IF l_error_tab_type.COUNT > 0 THEN
        log_error(pin_batch_id        => pin_batch_id,
                  pin_run_sequence_id => pin_run_sequence_id,
                  p_source_tab_type   => l_error_tab_type,
                  pov_return_status   => l_log_err_sts,
                  pov_error_message   => l_log_err_msg);

        l_error_tab_type.DELETE;
      END IF;
    EXCEPTION
      WHEN OTHERS THEN
        debug('Exception occured while fetching records for LEG_ACCOUNTING_DATE IS NULL.' ||
              SQLERRM);
    END;

    --Validate LEG_CURRENCY_CODE for Required value
    BEGIN
      debug('Validate LEG_CURRENCY_CODE for Required value');
      SELECT g_source_table --source_table
            ,
             xbs.interface_txn_id --interface_staging_id
            ,
             NULL --source_keyname1
            ,
             NULL --source_keyvalue1
            ,
             NULL --source_keyname2
            ,
             NULL --source_keyvalue2
            ,
             NULL --source_keyname3
            ,
             NULL --source_keyvalue3
            ,
             NULL --source_keyname4
            ,
             NULL --source_keyvalue4
            ,
             NULL --source_keyname5
            ,
             NULL --source_keyvalue5
            ,
             'LEG_CURRENCY_CODE' --source_column_name
            ,
             xbs.leg_currency_code --source_column_value
            ,
             g_err_validation --error_type
            ,
             l_error_code --error_code
            ,
             'Legacy Currency Code not entered' --error_message
            ,
             NULL --severity
            ,
             NULL --proposed_solution
            ,
             NULL, --stage   -- 2.3
             NULL -- as per changes done by sagar
             BULK COLLECT
        INTO l_error_tab_type
        FROM xxgl_balance_stg xbs
       WHERE process_flag IN (g_flag_v, g_flag_n, g_flag_e)
         AND xbs.leg_entity = piv_entity
         AND xbs.batch_id = pin_batch_id
         AND xbs.run_sequence_id = pin_run_sequence_id
         AND xbs.leg_currency_code IS NULL;

      IF l_error_tab_type.COUNT > 0 THEN
        log_error(pin_batch_id        => pin_batch_id,
                  pin_run_sequence_id => pin_run_sequence_id,
                  p_source_tab_type   => l_error_tab_type,
                  pov_return_status   => l_log_err_sts,
                  pov_error_message   => l_log_err_msg);

        l_error_tab_type.DELETE;
      END IF;
    EXCEPTION
      WHEN OTHERS THEN
        debug('Exception occured while fetching records for LEG_CURRENCY_CODE IS NULL.' ||
              SQLERRM);
    END;

    --Validate LEG_ACTUAL_FLAG for Required value
    BEGIN
      debug('Validate LEG_ACTUAL_FLAG for Required value');
      SELECT g_source_table --source_table
            ,
             xbs.interface_txn_id --interface_staging_id
            ,
             NULL --source_keyname1
            ,
             NULL --source_keyvalue1
            ,
             NULL --source_keyname2
            ,
             NULL --source_keyvalue2
            ,
             NULL --source_keyname3
            ,
             NULL --source_keyvalue3
            ,
             NULL --source_keyname4
            ,
             NULL --source_keyvalue4
            ,
             NULL --source_keyname5
            ,
             NULL --source_keyvalue5
            ,
             'LEG_ACTUAL_FLAG' --source_column_name
            ,
             xbs.leg_actual_flag --source_column_value
            ,
             g_err_validation --error_type
            ,
             l_error_code --error_code
            ,
             'Legacy Actual Flag not entered' --error_message
            ,
             NULL --severity
            ,
             NULL --proposed_solution
            ,
             NULL, --stage   -- 2.3
             NULL -- as per changes done by sagar
             BULK COLLECT
        INTO l_error_tab_type
        FROM xxgl_balance_stg xbs
       WHERE xbs.process_flag IN (g_flag_v, g_flag_n, g_flag_e)
         AND xbs.leg_entity = piv_entity
         AND xbs.batch_id = pin_batch_id
         AND xbs.run_sequence_id = pin_run_sequence_id
         AND xbs.leg_actual_flag IS NULL;

      IF l_error_tab_type.COUNT > 0 THEN
        log_error(pin_batch_id        => pin_batch_id,
                  pin_run_sequence_id => pin_run_sequence_id,
                  p_source_tab_type   => l_error_tab_type,
                  pov_return_status   => l_log_err_sts,
                  pov_error_message   => l_log_err_msg);

        l_error_tab_type.DELETE;
      END IF;
    EXCEPTION
      WHEN OTHERS THEN
        debug('Exception occured while fetching records for LEG_ACTUAL_FLAG IS NULL.' ||
              SQLERRM);
    END;

    --Validate LEG_USER_JE_CAT_NAME for Required value
    BEGIN
      debug('Validate LEG_USER_JE_CAT_NAME for Required value');
      SELECT g_source_table --source_table
            ,
             xbs.interface_txn_id --interface_staging_id
            ,
             NULL --source_keyname1
            ,
             NULL --source_keyvalue1
            ,
             NULL --source_keyname2
            ,
             NULL --source_keyvalue2
            ,
             NULL --source_keyname3
            ,
             NULL --source_keyvalue3
            ,
             NULL --source_keyname4
            ,
             NULL --source_keyvalue4
            ,
             NULL --source_keyname5
            ,
             NULL --source_keyvalue5
            ,
             'LEG_USER_JE_CAT_NAME' --source_column_name
            ,
             xbs.leg_user_je_cat_name --source_column_value
            ,
             g_err_validation --error_type
            ,
             l_error_code --error_code
            ,
             'Legacy User Journal Category not entered'
             --error_message
            ,
             NULL --severity
            ,
             NULL --proposed_solution
            ,
             NULL, --stage   -- 2.3
             NULL -- as per changes done by sagar
             BULK COLLECT
        INTO l_error_tab_type
        FROM xxgl_balance_stg xbs
       WHERE xbs.process_flag IN (g_flag_v, g_flag_n, g_flag_e)
         AND xbs.leg_entity = piv_entity
         AND xbs.batch_id = pin_batch_id
         AND xbs.run_sequence_id = pin_run_sequence_id
         AND xbs.leg_user_je_cat_name IS NULL;

      IF l_error_tab_type.COUNT > 0 THEN
        log_error(pin_batch_id        => pin_batch_id,
                  pin_run_sequence_id => pin_run_sequence_id,
                  p_source_tab_type   => l_error_tab_type,
                  pov_return_status   => l_log_err_sts,
                  pov_error_message   => l_log_err_msg);

        l_error_tab_type.DELETE;
      END IF;
    EXCEPTION
      WHEN OTHERS THEN
        debug('Exception occured while fetching records for LEG_USER_JE_CAT_NAME IS NULL.' ||
              SQLERRM);
    END;

    --Validate LEG_USER_JE_SRC_NAME for Required value
    BEGIN
      debug('Validate LEG_USER_JE_SRC_NAME for Required value');
      SELECT g_source_table --source_table
            ,
             xbs.interface_txn_id --interface_staging_id
            ,
             NULL --source_keyname1
            ,
             NULL --source_keyvalue1
            ,
             NULL --source_keyname2
            ,
             NULL --source_keyvalue2
            ,
             NULL --source_keyname3
            ,
             NULL --source_keyvalue3
            ,
             NULL --source_keyname4
            ,
             NULL --source_keyvalue4
            ,
             NULL --source_keyname5
            ,
             NULL --source_keyvalue5
            ,
             'LEG_USER_JE_SRC_NAME' --source_column_name
            ,
             xbs.leg_user_je_src_name --source_column_value
            ,
             g_err_validation --error_type
            ,
             l_error_code --error_code
            ,
             'Legacy User Journal Source Name not entered'
             --error_message
            ,
             NULL --severity
            ,
             NULL --proposed_solution
            ,
             NULL, --stage   -- 2.3
             NULL -- as per changes done by sagar
             BULK COLLECT
        INTO l_error_tab_type
        FROM xxgl_balance_stg xbs
       WHERE xbs.process_flag IN (g_flag_v, g_flag_n, g_flag_e)
         AND xbs.leg_entity = piv_entity
         AND xbs.batch_id = pin_batch_id
         AND xbs.run_sequence_id = pin_run_sequence_id
         AND xbs.leg_user_je_src_name IS NULL;

      IF l_error_tab_type.COUNT > 0 THEN
        log_error(pin_batch_id        => pin_batch_id,
                  pin_run_sequence_id => pin_run_sequence_id,
                  p_source_tab_type   => l_error_tab_type,
                  pov_return_status   => l_log_err_sts,
                  pov_error_message   => l_log_err_msg);

        l_error_tab_type.DELETE;
      END IF;
    EXCEPTION
      WHEN OTHERS THEN
        debug('Exception occured while fetching records for LEG_USER_JE_SRC_NAME IS NULL.' ||
              SQLERRM);
    END;

    --Validate LEG_DATE_CREATED_IN_GL for Required value
    BEGIN
      IF piv_entity = g_entity_journal THEN
        debug('Validate LEG_DATE_CREATED_IN_GL for Required value');

        SELECT g_source_table --source_table
              ,
               xbs.interface_txn_id --interface_staging_id
              ,
               NULL --source_keyname1
              ,
               NULL --source_keyvalue1
              ,
               NULL --source_keyname2
              ,
               NULL --source_keyvalue2
              ,
               NULL --source_keyname3
              ,
               NULL --source_keyvalue3
              ,
               NULL --source_keyname4
              ,
               NULL --source_keyvalue4
              ,
               NULL --source_keyname5
              ,
               NULL --source_keyvalue5
              ,
               'LEG_DATE_CREATED_IN_GL' --source_column_name
              ,
               xbs.leg_date_created_in_gl --source_column_value
              ,
               g_err_validation --error_type
              ,
               l_error_code --error_code
              ,
               'Legacy Date created in GL not entered' --error_message
              ,
               NULL --severity
              ,
               NULL --proposed_solution
              ,
               NULL, --stage   -- 2.3
               NULL -- as per changes done by sagar
               BULK COLLECT
          INTO l_error_tab_type
          FROM xxgl_balance_stg xbs
         WHERE xbs.process_flag IN (g_flag_v, g_flag_n, g_flag_e)
           AND xbs.leg_entity = piv_entity
           AND xbs.batch_id = pin_batch_id
           AND xbs.run_sequence_id = pin_run_sequence_id
           AND xbs.leg_date_created_in_gl IS NULL;

        IF l_error_tab_type.COUNT > 0 THEN
          log_error(pin_batch_id        => pin_batch_id,
                    pin_run_sequence_id => pin_run_sequence_id,
                    p_source_tab_type   => l_error_tab_type,
                    pov_return_status   => l_log_err_sts,
                    pov_error_message   => l_log_err_msg);

          l_error_tab_type.DELETE;
        END IF;

      END IF;
    EXCEPTION
      WHEN OTHERS THEN
        DEBUG('Exception occured while fetching records for LEG_DATE_CREATED_IN_GL IS NULL.' ||
              SQLERRM);
    END;

    debug('check_required <<');
  EXCEPTION
    WHEN OTHERS THEN
      l_error_message := SUBSTR('Exception in Procedure check_required. ' ||
                                SQLERRM,
                                1,
                                1999);
      fnd_file.put_line(fnd_file.LOG, l_error_message);
      fnd_file.put_line(fnd_file.LOG, 'check_required <<E');

      pov_return_status := g_error;
      pov_error_message := l_error_message;
  END check_required;

  -- =============================================================================
  -- Procedure: validate_data
  -- =============================================================================
  -- Validate staging table records
  -- =============================================================================
  --  Input Parameters :
  --    piv_entity           : Entity Name
  --    pin_batch_id         : Batch Id
  --    pin_run_sequence_id  : Run Sequence Id
  --  Output Parameters :
  --    pov_return_status    : Return Status - Normal/Warning/Error
  --    pov_error_message    : Error message in case of any failure
  -- -----------------------------------------------------------------------------
  PROCEDURE validate_data(piv_entity          IN VARCHAR2,
                          pin_batch_id        IN NUMBER,
                          pin_run_sequence_id IN NUMBER,
                          pov_return_status   OUT NOCOPY VARCHAR2,
                          pov_error_message   OUT NOCOPY VARCHAR2) IS
    l_error_tab_type     xxetn_common_error_pkg.g_source_tab_type;
    l_validation_failure VARCHAR2(1) := 'N';
    l_count              NUMBER := 0;
    l_return_status      VARCHAR2(1);
    l_error_message      VARCHAR2(2000);
    l_last_stmt          NUMBER := 10;
    process_exception EXCEPTION;
  BEGIN
    debug('validate_data >>');
    l_return_status := g_normal;
    l_error_message := NULL;
--Added for v2.8 START
    BEGIN

      dbms_stats.gather_table_stats(ownname          => 'XXCONV',
                                    tabname          => 'XXGL_BALANCE_STG',
                                    cascade          => true,
                                    estimate_percent => 20);--dbms_stats.auto_sample_size,
                                    --degree           => dbms_stats.default_degree);

    END;
--Added for v2.8 END
    debug('01# Check all mandatory columns in staging table');
    --Calling check_required procedure to Check all mandatory columns in staging table
    check_required(piv_entity          => piv_entity,
                   pin_batch_id        => pin_batch_id,
                   pin_run_sequence_id => pin_run_sequence_id,
                   pov_return_status   => l_return_status,
                   pov_error_message   => l_error_message);

    IF l_return_status <> g_normal THEN
      RAISE process_exception;
    END IF;

    --Update the ID Values for important columns
    debug('02# Update required ID values of important columns');
    l_last_stmt := 20;
    update_ids_reference(piv_entity          => piv_entity,
                         pin_batch_id        => pin_batch_id,
                         pin_run_sequence_id => pin_run_sequence_id,
                         pov_return_status   => l_return_status,
                         pov_error_message   => l_error_message);

    IF l_return_status <> g_normal THEN
      RAISE process_exception;
    END IF;

    --Validate the updated ID Values
    debug('03# alidate the updated ID Values');
    validate_ids_reference(piv_entity          => piv_entity,
                           pin_batch_id        => pin_batch_id,
                           pin_run_sequence_id => pin_run_sequence_id,
                           pov_return_status   => l_return_status,
                           pov_error_message   => l_error_message);

    IF l_return_status <> g_normal THEN
      RAISE process_exception;
    END IF;

    debug('04# Check Accounting date is a valid date and falls in ?Open? accounting period');
    --calling procedure validate_accounting_date to validate accounting date
    validate_accounting_date(piv_entity          => piv_entity,
                             pin_batch_id        => pin_batch_id,
                             pin_run_sequence_id => pin_run_sequence_id,
                             pov_return_status   => l_return_status,
                             pov_error_message   => l_error_message);

    /**
    IF piv_entity = g_entity_journal THEN
       debug('05# Either of the fields Entered_Dr,Entered_Cr,Accounted_Dr,Accounted_Cr must be populated');
       --calling procedure validate_dr_cr to validate Entered and Accounted Dr/Cr amounts
       validate_dr_cr
       ( piv_entity          => piv_entity
       , pin_batch_id        => pin_batch_id
       , pin_run_sequence_id => pin_run_sequence_id
       , pov_return_status   => l_return_status
       , pov_error_message   => l_error_message
       );
    END IF;
    **/

    debug('06# Actual Flag must be A');
    --calling procedure actual_flag to validate actual flag
    validate_actual_flag(piv_entity          => piv_entity,
                         pin_batch_id        => pin_batch_id,
                         pin_run_sequence_id => pin_run_sequence_id,
                         pov_return_status   => l_return_status,
                         pov_error_message   => l_error_message);

    debug('07# Currency Code must be a Valid Currency');
    --calling procedure validate_currency_code to validate currency code
    validate_currency_code(piv_entity          => piv_entity,
                           pin_batch_id        => pin_batch_id,
                           pin_run_sequence_id => pin_run_sequence_id,
                           pov_return_status   => l_return_status,
                           pov_error_message   => l_error_message);

    debug('08# Balance Conversion Category should be valid');
    --calling procedure validate_user_je_cat to validate Conversion category
    validate_user_je_cat(piv_entity          => piv_entity,
                         pin_batch_id        => pin_batch_id,
                         pin_run_sequence_id => pin_run_sequence_id,
                         pov_return_status   => l_return_status,
                         pov_error_message   => l_error_message);

    debug('09# Balance Conversion Source should be valid');
    --calling procedure validate_user_je_source to validate Conversion source
    validate_user_je_source(piv_entity          => piv_entity,
                            pin_batch_id        => pin_batch_id,
                            pin_run_sequence_id => pin_run_sequence_id,
                            pov_return_status   => l_return_status,
                            pov_error_message   => l_error_message);

    debug('10# Average Journal flag should be valid');
    --calling procedure validate_avg_journal_flag to validate Average Journal flag
    validate_avg_journal_flag(piv_entity          => piv_entity,
                              pin_batch_id        => pin_batch_id,
                              pin_run_sequence_id => pin_run_sequence_id,
                              pov_return_status   => l_return_status,
                              pov_error_message   => l_error_message);

    debug('11# Entered Debit Amount should be equal to Accounted amount for Functional Currency Journals');
    --calling procedure validate_ent_dr_amt to validate Entered Dr Amount
    validate_ent_dr_amt(piv_entity          => piv_entity,
                        pin_batch_id        => pin_batch_id,
                        pin_run_sequence_id => pin_run_sequence_id,
                        pov_return_status   => l_return_status,
                        pov_error_message   => l_error_message);

    debug('12# Entered Credit Amount should be equal to Accounted amount for Functional Currency Journals');
    --calling procedure validate_ent_cr_amt to validate Entered Cr Amount
    validate_ent_cr_amt(piv_entity          => piv_entity,
                        pin_batch_id        => pin_batch_id,
                        pin_run_sequence_id => pin_run_sequence_id,
                        pov_return_status   => l_return_status,
                        pov_error_message   => l_error_message);

    /**
          debug('13# A conversion rate must be entered when using the User conversion rate type');
          --calling procedure validate_cnv_rate to validate User conversion rate
          validate_cnv_rate
          ( piv_entity          => piv_entity
          , pin_batch_id        => pin_batch_id
          , pin_run_sequence_id => pin_run_sequence_id
          , pov_return_status   => l_return_status
          , pov_error_message   => l_error_message
          );
    **/

    debug('14# Accounted amount must be supplied when entering foreign currency journal lines');
    --calling procedure validate_cnv_rate_type to validate Accounted Amount
    /**      validate_cnv_rate_type
          ( piv_entity          => piv_entity
          , pin_batch_id        => pin_batch_id
          , pin_run_sequence_id => pin_run_sequence_id
          , pov_return_status   => l_return_status
          , pov_error_message   => l_error_message
          );
    **/

    /* Update flags */
    update_flags(piv_entity          => piv_entity,
                 pin_batch_id        => pin_batch_id,
                 piv_run_mode        => g_run_mode_validate,
                 pin_run_sequence_id => pin_run_sequence_id,
                 pov_return_status   => l_return_status,
                 pov_error_message   => l_error_message);

    debug('validate_data <<E');
  EXCEPTION
    WHEN process_exception THEN
      pov_return_status := l_return_status;
      pov_error_message := l_error_message;
      fnd_file.put_line(fnd_file.LOG, 'validate_data <<E1');
    WHEN OTHERS THEN
      l_error_message   := SUBSTR('Exception in Procedure validate_data. ' ||
                                  SQLERRM,
                                  1,
                                  1999);
      pov_return_status := g_error;
      pov_error_message := l_error_message;
      fnd_file.put_line(fnd_file.LOG, l_error_message);
      fnd_file.put_line(fnd_file.LOG, 'validate_data <<E2');
  END validate_data;

  -- =============================================================================
  -- Procedure: assign_batch
  -- =============================================================================
  -- To assign batch_id, based on program parameters, for records eligible for
  -- validation, re-validation and conversion
  -- =============================================================================
  --  Input Parameters :
  --    piv_entity           : Entity Name
  --    piv_source_system    : Legacy Source System Name
  --    piv_set_of_books     : Legacy Set of Books Name
  --    pid_period_start     : GL Period Start Date
  --    pid_period_end       : GL Period End Date
  --    pin_batch_id         : Batch Id
  --    piv_run_mode         : Run Mode
  --    piv_process_records  : Process Records ('ALL', 'ERROR', 'UNPROCESSED')
  --  Output Parameters :
  --    pov_return_status    : Return Status - Normal/Warning/Error
  --    pov_error_message    : Error message in case of any failure
  -- -----------------------------------------------------------------------------
  PROCEDURE assign_batch(piv_entity           IN VARCHAR2,
                         piv_source_system    IN VARCHAR2,
                         piv_set_of_books     IN VARCHAR2,
                         pid_period_start     IN DATE,
                         pid_period_end       IN DATE,
                         pin_batch_id         IN OUT NUMBER,
                         piv_run_mode         IN VARCHAR2,
                         piv_process_records  IN VARCHAR2,
                         pion_run_sequence_id IN OUT NUMBER,
                         pov_return_status    OUT NOCOPY VARCHAR2,
                         pov_error_message    OUT NOCOPY VARCHAR2) IS
    PRAGMA AUTONOMOUS_TRANSACTION;

    l_error_tab_type xxetn_common_error_pkg.g_source_tab_type;
    l_error_message  VARCHAR2(2000) := NULL;
    l_log_err_sts    VARCHAR2(2000) := NULL;
    l_log_err_msg    VARCHAR2(2000) := NULL;

  BEGIN
    debug('assign_batch>> ');

    pov_return_status := g_normal;
    pov_error_message := NULL;

    --generating new batch id and run seq id
    g_batch_id        := xxetn_batches_s.NEXTVAL;
    g_run_sequence_id := xxetn_run_sequences_s.NEXTVAL;

    debug('Generated Batch Id   : ' || g_batch_id);
    debug('Generated Run Seq Id : ' || g_run_sequence_id);

    SAVEPOINT assign_batch_sp;

    /* Validation Mode: Fresh run */
    IF (pin_batch_id IS NULL AND piv_run_mode = g_run_mode_validate) THEN
      --assigning batch id and run seq. id to fresh records (first time validation)
      UPDATE xxgl_balance_stg
         SET process_flag           = g_flag_n,
             batch_id               = g_batch_id,
             run_sequence_id        = g_run_sequence_id,
             last_updated_date      = SYSDATE,
             last_updated_by        = g_user_id,
             last_update_login      = g_login_id,
             program_application_id = g_prog_appl_id,
             program_id             = g_program_id,
             program_update_date    = SYSDATE,
             request_id             = g_request_id
       WHERE leg_entity = piv_entity
         AND batch_id IS NULL
         AND process_flag <> g_flag_x
         AND leg_source_system = NVL(piv_source_system, leg_source_system)
         AND leg_ledger_name = NVL(piv_set_of_books, leg_ledger_name)
         AND TRUNC(leg_accounting_date) BETWEEN
             NVL(pid_period_start, TRUNC(leg_accounting_date)) AND
             NVL(pid_period_end, TRUNC(leg_accounting_date + 1));

      debug('01. no. of records updated ' || SQL%ROWCOUNT);

      /* Validation Mode: Re-validation run */
    ELSIF (pin_batch_id IS NOT NULL AND piv_run_mode = g_run_mode_validate) THEN
      --assigning batch id and run seq. id to fresh records (re-validation)
      IF (NVL(piv_process_records, g_process_recs_all) = g_process_recs_all) THEN
        --updating records with process_flag in ('N','E','V')
        UPDATE xxgl_balance_stg
           SET process_flag           = g_flag_n,
               batch_id               = g_batch_id,
               run_sequence_id        = g_run_sequence_id,
               last_updated_date      = SYSDATE,
               last_updated_by        = g_user_id,
               last_update_login      = g_login_id,
               program_application_id = g_prog_appl_id,
               program_id             = g_program_id,
               program_update_date    = SYSDATE,
               request_id             = g_request_id
         WHERE leg_entity = piv_entity
           AND batch_id = pin_batch_id
           AND process_flag IN (g_flag_n, g_flag_e, g_flag_v)
           AND leg_source_system =
               NVL(piv_source_system, leg_source_system)
           AND leg_ledger_name = NVL(piv_set_of_books, leg_ledger_name)
           AND TRUNC(leg_accounting_date) BETWEEN
               NVL(pid_period_start, TRUNC(leg_accounting_date)) AND
               NVL(pid_period_end, TRUNC(leg_accounting_date + 1));

        debug('02. no. of records updated ' || SQL%ROWCOUNT);

      ELSIF (piv_process_records = g_process_recs_error) THEN
        --updating records with process_flag = 'E'
        UPDATE xxgl_balance_stg
           SET process_flag           = g_flag_n,
               batch_id               = g_batch_id,
               run_sequence_id        = g_run_sequence_id,
               last_updated_date      = SYSDATE,
               last_updated_by        = g_user_id,
               last_update_login      = g_login_id,
               program_application_id = g_prog_appl_id,
               program_id             = g_program_id,
               program_update_date    = SYSDATE,
               request_id             = g_request_id
         WHERE leg_entity = piv_entity
           AND batch_id = pin_batch_id
           AND process_flag = g_flag_e
           AND leg_source_system =
               NVL(piv_source_system, leg_source_system)
           AND leg_ledger_name = NVL(piv_set_of_books, leg_ledger_name)
           AND TRUNC(leg_accounting_date) BETWEEN
               NVL(pid_period_start, TRUNC(leg_accounting_date)) AND
               NVL(pid_period_end, TRUNC(leg_accounting_date + 1));

        debug('03. no. of records updated ' || SQL%ROWCOUNT);

      ELSIF (piv_process_records = g_process_recs_unprocessed) THEN
        --updating records with process_flag = 'N'
        UPDATE xxgl_balance_stg
           SET batch_id               = g_batch_id,
               run_sequence_id        = g_run_sequence_id,
               last_updated_date      = SYSDATE,
               last_updated_by        = g_user_id,
               last_update_login      = g_login_id,
               program_application_id = g_prog_appl_id,
               program_id             = g_program_id,
               program_update_date    = SYSDATE,
               request_id             = g_request_id
         WHERE leg_entity = piv_entity
           AND batch_id = pin_batch_id
           AND process_flag = g_flag_n
           AND leg_source_system =
               NVL(piv_source_system, leg_source_system)
           AND leg_ledger_name = NVL(piv_set_of_books, leg_ledger_name)
           AND TRUNC(leg_accounting_date) BETWEEN
               NVL(pid_period_start, TRUNC(leg_accounting_date)) AND
               NVL(pid_period_end, TRUNC(leg_accounting_date + 1));

        debug('04. no. of records updated ' || SQL%ROWCOUNT);

      END IF;

      /* Conversion Mode */
    ELSIF (pin_batch_id IS NOT NULL AND
          piv_run_mode = g_run_mode_conversion) THEN
      --re-assigning batch id and run seq. id based to records selected based on program parameters
      g_run_sequence_id := xxetn_run_sequences_s.NEXTVAL;
      debug('re-assigining batch id in conversion mode');
      --updating records with process_flag in ('N','E','V')
      UPDATE xxgl_balance_stg
         SET batch_id               = g_batch_id,
             run_sequence_id        = g_run_sequence_id,
             last_updated_date      = SYSDATE,
             last_updated_by        = g_user_id,
             last_update_login      = g_login_id,
             program_application_id = g_prog_appl_id,
             program_id             = g_program_id,
             program_update_date    = SYSDATE,
             request_id             = g_request_id
       WHERE leg_entity = piv_entity
         AND batch_id = NVL(pin_batch_id, batch_id)
            -- AND process_flag      IN (g_flag_n, g_flag_e, g_flag_v)
         AND process_flag = g_flag_v -- only validated records will be updated
         AND leg_source_system = NVL(piv_source_system, leg_source_system)
         AND leg_ledger_name = NVL(piv_set_of_books, leg_ledger_name)
         AND TRUNC(leg_accounting_date) BETWEEN
             NVL(pid_period_start, TRUNC(leg_accounting_date)) AND
             NVL(pid_period_end, TRUNC(leg_accounting_date + 1));

      debug('05. no. of records updated ' || SQL%ROWCOUNT);
    END IF;

    COMMIT;
    pion_run_sequence_id := g_run_sequence_id;
    debug('assign_batch<<');
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK to assign_batch_sp;
      l_error_message := SUBSTR('Exception in Procedure assign_batch. ' ||
                                SQLERRM,
                                1,
                                1999);

      l_error_tab_type.DELETE;

      l_error_tab_type(1).source_table := g_source_table;
      l_error_tab_type(1).interface_staging_id := NULL;
      l_error_tab_type(1).source_keyname1 := NULL;
      l_error_tab_type(1).source_keyvalue1 := NULL;
      l_error_tab_type(1).source_keyname2 := NULL;
      l_error_tab_type(1).source_keyvalue2 := NULL;
      l_error_tab_type(1).source_keyname3 := NULL;
      l_error_tab_type(1).source_keyvalue3 := NULL;
      l_error_tab_type(1).source_keyname4 := NULL;
      l_error_tab_type(1).source_keyvalue4 := NULL;
      l_error_tab_type(1).source_keyname5 := NULL;
      l_error_tab_type(1).source_keyvalue5 := NULL;
      l_error_tab_type(1).source_column_name := NULL;
      l_error_tab_type(1).source_column_value := NULL;
      l_error_tab_type(1).error_type := g_err_validation;
      l_error_tab_type(1).error_code := 'ETN_OTREXCEP_ASSIGN_BATCH';
      l_error_tab_type(1).error_message := l_error_message;
      l_error_tab_type(1).severity := NULL;
      l_error_tab_type(1).proposed_solution := NULL;
      l_error_tab_type(1).stage := NULL; -- v2.3
      l_error_tab_type(1).interface_load_id := NULL; -- AS PER CHANGES DONE BY SAGAR

      log_error(pin_batch_id        => NULL,
                pin_run_sequence_id => NULL,
                p_source_tab_type   => l_error_tab_type,
                pov_return_status   => l_log_err_sts,
                pov_error_message   => l_log_err_msg);

      fnd_file.put_line(fnd_file.LOG, l_error_message);
      fnd_file.put_line(fnd_file.LOG, 'assign_batch<<E');
      pov_return_status := g_error;
      pov_error_message := l_error_message;
  END assign_batch;

  -- =============================================================================
  -- Procedure: pre_validate
  -- =============================================================================
  --   Verify custom Lookup Setups for GL Balance Conversion
  -- =============================================================================
  --  Input Parameters :
  --    No input parameters
  --  Output Parameters :
  --    pov_return_status    : Return Status - Normal/Warning/Error
  --    pov_error_message    : Error message in case of any failure
  -- -----------------------------------------------------------------------------
  PROCEDURE pre_validate(pov_return_status OUT NOCOPY VARCHAR2,
                         pov_error_message OUT NOCOPY VARCHAR2) IS
    l_count         NUMBER := 0;
    l_err_count     NUMBER := 0;
    l_error_message VARCHAR2(2000) := NULL;

  BEGIN
    debug('pre_validate >>');

    pov_return_status := g_normal;
    pov_error_message := NULL;

    fnd_file.put_line(fnd_file.OUTPUT, LPAD('-', 102, '-'));
    fnd_file.put_line(fnd_file.OUTPUT, 'Custom Profile issues');
    fnd_file.put_line(fnd_file.OUTPUT, LPAD('-', 102, '-'));
    fnd_file.put_line(fnd_file.OUTPUT,
                      RPAD('Profile Option', 32, ' ') ||
                      RPAD('Error Message', 70, ' '));
    fnd_file.put_line(fnd_file.OUTPUT,
                      LPAD('-', 30, '-') || '  ' || LPAD('-', 70, '-'));

    IF fnd_profile.VALUE('ETN_GL_JOURNAL_CON_CATEGORY') IS NULL THEN
      l_error_message := 'Profile Value is NULL';
      fnd_file.put_line(fnd_file.OUTPUT,
                        RPAD('ETN_GL_JOURNAL_CON_CATEGORY', 32, ' ') ||
                        l_error_message);
    ELSE
      l_error_message := 'Success : Value - ' ||
                         NVL(fnd_profile.VALUE('ETN_GL_JOURNAL_CON_CATEGORY'),
                             'NULL');
      fnd_file.put_line(fnd_file.OUTPUT,
                        RPAD('ETN_GL_JOURNAL_CON_CATEGORY', 32, ' ') ||
                        l_error_message);
    END IF;

    IF fnd_profile.VALUE('ETN_GL_JOURNAL_CON_SOURCE') IS NULL THEN
      l_error_message := 'Profile Value is NULL';
      fnd_file.put_line(fnd_file.OUTPUT,
                        RPAD('ETN_GL_JOURNAL_CON_SOURCE', 32, ' ') ||
                        l_error_message);
    ELSE
      l_error_message := 'Success : Value - ' ||
                         NVL(fnd_profile.VALUE('ETN_GL_JOURNAL_CON_SOURCE'),
                             'NULL');
      fnd_file.put_line(fnd_file.OUTPUT,
                        RPAD('ETN_GL_JOURNAL_CON_SOURCE', 32, ' ') ||
                        l_error_message);
    END IF;

    IF fnd_profile.VALUE('ETN_FND_DEBUG_PROFILE') IS NULL THEN
      l_error_message := 'Profile Value is NULL';
      fnd_file.put_line(fnd_file.OUTPUT,
                        RPAD('ETN_FND_DEBUG_PROFILE', 32, ' ') ||
                        l_error_message);
    ELSE
      l_error_message := 'Success : Value - ' ||
                         NVL(fnd_profile.VALUE('ETN_FND_DEBUG_PROFILE'),
                             'NULL');
      fnd_file.put_line(fnd_file.OUTPUT,
                        RPAD('ETN_FND_DEBUG_PROFILE', 32, ' ') ||
                        l_error_message);
    END IF;

    IF fnd_profile.VALUE('ETN_FND_ERROR_TAB_LIMIT') IS NULL THEN
      l_error_message := 'Profile Value is NULL';
      fnd_file.put_line(fnd_file.OUTPUT,
                        RPAD('ETN_FND_ERROR_TAB_LIMIT', 32, ' ') ||
                        l_error_message);
    ELSE
      l_error_message := 'Success : Value - ' ||
                         NVL(fnd_profile.VALUE('ETN_FND_ERROR_TAB_LIMIT'),
                             'NULL');
      fnd_file.put_line(fnd_file.OUTPUT,
                        RPAD('ETN_FND_ERROR_TAB_LIMIT', 32, ' ') ||
                        l_error_message);
    END IF;

    fnd_file.put_line(fnd_file.OUTPUT, LPAD('-', 102, '-'));
    fnd_file.put_line(fnd_file.OUTPUT, ' ');

    SELECT COUNT(*)
      INTO l_count
      FROM fnd_lookup_types flt
     WHERE flt.lookup_type = g_xr_journal_category --ETN_GL_CATEGORY_MAP
    ;

    debug(g_xr_journal_category || ' Count : ' || l_count);

    fnd_file.put_line(fnd_file.OUTPUT, LPAD('-', 102, '-'));
    fnd_file.put_line(fnd_file.OUTPUT, 'Custom Lookup Setup issues');
    fnd_file.put_line(fnd_file.OUTPUT, LPAD('-', 102, '-'));
    fnd_file.put_line(fnd_file.OUTPUT,
                      RPAD('Lookup Type', 32, ' ') ||
                      RPAD('Error Message', 70, ' '));
    fnd_file.put_line(fnd_file.OUTPUT,
                      LPAD('-', 30, '-') || '  ' || LPAD('-', 70, '-'));

    IF l_count = 0 THEN
      l_err_count     := l_err_count + 1;
      l_error_message := 'Missing Cross Reference : Journal Category Lookup Missing.';
      fnd_file.put_line(fnd_file.OUTPUT,
                        RPAD(g_xr_journal_category, 32, ' ') ||
                        l_error_message);
    ELSE
      fnd_file.put_line(fnd_file.OUTPUT,
                        RPAD(g_xr_journal_category, 32, ' ') ||
                        'Success : LookupType Count-' || l_count);
    END IF;

    SELECT COUNT(*)
      INTO l_count
      FROM fnd_lookup_types flt
     WHERE flt.lookup_type = g_xr_journal_source --ETN_GL_SOURCE_MAP
    ;

    debug(g_xr_journal_source || ' Count : ' || l_count);

    IF l_count = 0 THEN
      l_err_count     := l_err_count + 1;
      l_error_message := 'Missing Cross Reference : Journal Source Lookup Missing.';
      fnd_file.put_line(fnd_file.OUTPUT,
                        RPAD(g_xr_journal_source, 32, ' ') ||
                        l_error_message);
    ELSE
      fnd_file.put_line(fnd_file.OUTPUT,
                        RPAD(g_xr_journal_source, 32, ' ') ||
                        'Success : LookupType Count-' || l_count);
    END IF;

    SELECT COUNT(*)
      INTO l_count
      FROM fnd_lookup_types flt
     WHERE flt.lookup_type = g_xr_books_mapping --ETN_GL_SOB_LEDGER_MAP
    ;

    debug(g_xr_books_mapping || ' Count : ' || l_count);

    IF l_count = 0 THEN
      l_err_count     := l_err_count + 1;
      l_error_message := 'Missing Cross Reference : Ledger/Set of books Lookup Missing.';
      fnd_file.put_line(fnd_file.OUTPUT,
                        RPAD(g_xr_books_mapping, 32, ' ') ||
                        l_error_message);
    ELSE
      fnd_file.put_line(fnd_file.OUTPUT,
                        RPAD(g_xr_books_mapping, 32, ' ') ||
                        'Success : LookupType Count-' || l_count);
    END IF;

    fnd_file.put_line(fnd_file.OUTPUT, LPAD('-', 102, '-'));

    l_error_message := 'Custom Lookup Setup issues Count ' || l_err_count;
    fnd_file.put_line(fnd_file.OUTPUT, l_error_message);

    fnd_file.put_line(fnd_file.OUTPUT, LPAD('-', 102, '-'));
    fnd_file.put_line(fnd_file.OUTPUT, ' ');
    fnd_file.put_line(fnd_file.OUTPUT, ' ');
    fnd_file.put_line(fnd_file.OUTPUT, ' ');

    BEGIN
      fnd_file.put_line(fnd_file.OUTPUT, LPAD('-', 102, '-'));
      fnd_file.put_line(fnd_file.OUTPUT,
                        'List of Journal Categories (Lookup Type : ' ||
                        g_xr_journal_category || ' ) not Setup');
      fnd_file.put_line(fnd_file.OUTPUT, LPAD('-', 102, '-'));
      fnd_file.put_line(fnd_file.OUTPUT,
                        RPAD('Legacy Source System', 21, ' ') ||
                        RPAD('Category Name', 30, ' ') || '  Staging Count');
      fnd_file.put_line(fnd_file.OUTPUT,
                        RPAD('--------------------', 21, ' ') ||
                        RPAD('-------------', 30, ' ') || '  -------------');
      FOR indx IN (WITH je_category AS
                      (SELECT xbs.leg_source_system,
                             xbs.leg_user_je_cat_name,
                             COUNT(1) cat_count
                        FROM xxgl_balance_stg xbs
                       GROUP BY xbs.leg_source_system,
                                xbs.leg_user_je_cat_name)
                     SELECT jc.*
                       FROM je_category jc
                      WHERE NOT EXISTS
                      (SELECT 1
                               FROM gl_je_categories  gl,
                                    fnd_lookup_values flv
                              WHERE gl.language = USERENV('LANG')
                                AND gl.user_je_category_name =
                                    TRIM(flv.attribute3)
                                AND flv.lookup_type = g_xr_journal_category
                                AND flv.enabled_flag = g_flag_y
                                AND TRUNC(SYSDATE) BETWEEN
                                    NVL(flv.start_date_active, TRUNC(SYSDATE)) AND
                                    NVL(flv.end_date_active,
                                        TRUNC(SYSDATE + 1))
                                AND flv.language = USERENV('LANG')
                                AND TRIM(flv.attribute_category) =
                                    g_xr_journal_category
                                AND TRIM(flv.attribute1) =
                                    jc.leg_source_system
                                AND TRIM(flv.attribute2) =
                                    jc.leg_user_je_cat_name)
                      ORDER BY 1, 2) LOOP
        fnd_file.put_line(fnd_file.OUTPUT,
                          RPAD(indx.leg_source_system, 21, ' ') ||
                          RPAD(indx.leg_user_je_cat_name, 30, ' ') || '  ' ||
                          indx.cat_count);
      END LOOP;
      fnd_file.put_line(fnd_file.OUTPUT, LPAD('-', 102, '-'));
    END;

    fnd_file.put_line(fnd_file.OUTPUT, ' ');
    fnd_file.put_line(fnd_file.OUTPUT, ' ');
    fnd_file.put_line(fnd_file.OUTPUT, ' ');

    BEGIN
      fnd_file.put_line(fnd_file.OUTPUT, LPAD('-', 102, '-'));
      fnd_file.put_line(fnd_file.OUTPUT,
                        'List of Journal Sources (Lookup Type : ' ||
                        g_xr_journal_source || ' ) not Setup');
      fnd_file.put_line(fnd_file.OUTPUT, LPAD('-', 102, '-'));
      fnd_file.put_line(fnd_file.OUTPUT,
                        RPAD('Legacy Source System', 21, ' ') ||
                        RPAD('Source Name', 30, ' ') || '  Staging Count');
      fnd_file.put_line(fnd_file.OUTPUT,
                        RPAD('--------------------', 21, ' ') ||
                        RPAD('-------------', 30, ' ') || '  -------------');
      FOR indx IN (WITH je_category AS
                      (SELECT xbs.leg_source_system,
                             xbs.leg_user_je_src_name,
                             COUNT(1) cat_count
                        FROM xxgl_balance_stg xbs
                       GROUP BY xbs.leg_source_system,
                                xbs.leg_user_je_src_name)
                     SELECT jc.*
                       FROM je_category jc
                      WHERE NOT EXISTS
                      (SELECT 1
                               FROM gl_je_sources gl, fnd_lookup_values flv
                              WHERE gl.language = USERENV('LANG')
                                AND gl.user_je_source_name =
                                    TRIM(flv.attribute3)
                                AND flv.lookup_type = g_xr_journal_source
                                AND flv.enabled_flag = g_flag_y
                                AND TRUNC(SYSDATE) BETWEEN
                                    NVL(flv.start_date_active, TRUNC(SYSDATE)) AND
                                    NVL(flv.end_date_active,
                                        TRUNC(SYSDATE + 1))
                                AND flv.language = USERENV('LANG')
                                AND TRIM(flv.attribute_category) =
                                    g_xr_journal_source
                                AND TRIM(flv.attribute1) =
                                    jc.leg_source_system
                                AND TRIM(flv.attribute2) =
                                    jc.leg_user_je_src_name)
                      ORDER BY 1, 2) LOOP
        fnd_file.put_line(fnd_file.OUTPUT,
                          RPAD(indx.leg_source_system, 21, ' ') ||
                          RPAD(indx.leg_user_je_src_name, 30, ' ') || '  ' ||
                          indx.cat_count);
      END LOOP;
      fnd_file.put_line(fnd_file.OUTPUT, LPAD('-', 102, '-'));
    END;

    debug('pre_validate <<');
  EXCEPTION
    WHEN OTHERS THEN
      l_error_message := SUBSTR('Exception in Procedure pre_validate. ' ||
                                SQLERRM,
                                1,
                                1999);
      fnd_file.put_line(fnd_file.LOG, l_error_message);
      fnd_file.put_line(fnd_file.LOG, 'pre_validate <<E');
      pov_return_status := g_error;
      pov_error_message := l_error_message;
  END pre_validate;

  -- =============================================================================
  -- Procedure: get_data
  -- =============================================================================
  --   To load validated data from extraction table to staging table
  -- =============================================================================
  --  Input Parameters :
  --    No input parameters
  --  Output Parameters :
  --    pov_return_status    : Return Status - Normal/Warning/Error
  --    pov_error_message    : Error message in case of any failure
  -- -----------------------------------------------------------------------------
  PROCEDURE get_data(pov_return_status OUT NOCOPY VARCHAR2,
                     pov_error_message OUT NOCOPY VARCHAR2) IS
    l_error_message    VARCHAR2(2000) := NULL;
    l_excep_col1       VARCHAR2(100) := NULL;
    l_bluk_error_count NUMBER := 0;

    l_log_err_sts VARCHAR2(2000) := NULL;
    l_log_err_msg VARCHAR2(2000) := NULL;

    l_load_count NUMBER := 0;

    /* Cursor to get validated data from Extraction table */
    CURSOR ext_recs_cur IS
      SELECT xber.interface_txn_id,
             xber.leg_source_system,
             xber.leg_entity,
             xber.leg_seq_num,
             xber.leg_status,
             g_status_new AS status,
             xber.leg_ledger_name,
             xber.leg_sob_short_name,
             xber.leg_accounting_date,
             xber.leg_start_date,
             xber.leg_end_date,
             xber.leg_period_year,
             xber.leg_period_num,
             xber.leg_currency_code,
             xber.leg_actual_flag,
             DECODE(xber.leg_entity,
                    g_entity_journal,
                    xber.leg_user_je_cat_name,
                    'Conversion') AS leg_user_je_cat_name,
             DECODE(xber.leg_entity,
                    g_entity_journal,
                    xber.leg_user_je_src_name,
                    'Conversion') AS leg_user_je_src_name,
             xber.leg_curr_conv_date,
             xber.leg_encumbrance_type,
             xber.leg_budget_name,
             xber.leg_version_num,
             xber.leg_user_curr_conv_type,
             xber.leg_curr_con_rate,
             xber.leg_segment1,
             xber.leg_segment2,
             xber.leg_segment3,
             xber.leg_segment4,
             xber.leg_segment5,
             xber.leg_segment6,
             xber.leg_segment7,
             xber.leg_segment8,
             xber.leg_segment9,
             xber.leg_segment10,
             xber.leg_segment11,
             xber.leg_segment12,
             xber.leg_segment13,
             xber.leg_segment14,
             xber.leg_segment15,
             xber.leg_segment16,
             xber.leg_segment17,
             xber.leg_segment18,
             xber.leg_segment19,
             xber.leg_segment20,
             xber.leg_segment21,
             xber.leg_segment22,
             xber.leg_segment23,
             xber.leg_segment24,
             xber.leg_segment25,
             xber.leg_segment26,
             xber.leg_segment27,
             xber.leg_segment28,
             xber.leg_segment29,
             xber.leg_segment30,
             xber.leg_begin_balance_cr,
             xber.leg_begin_balance_dr,
             xber.leg_period_net_cr,
             xber.leg_period_net_dr,
             xber.leg_begin_balance_cr_beq -- v2.2
            ,
             xber.leg_begin_balance_dr_beq -- v2.2
            ,
             xber.leg_period_net_cr_beq -- v2.2
            ,
             xber.leg_period_net_dr_beq -- v2.2
            ,
             xber.leg_entered_dr,
             xber.leg_entered_cr,
             xber.leg_accounted_dr,
             xber.leg_accounted_cr,
             xber.leg_transaction_date,
             xber.leg_reference1,
             xber.leg_reference2,
             xber.leg_reference3,
             xber.leg_reference4,
             xber.leg_reference5,
             xber.leg_reference6,
             xber.leg_reference7,
             xber.leg_reference8,
             xber.leg_reference9,
             xber.leg_reference10,
             xber.leg_reference11,
             xber.leg_reference12,
             xber.leg_reference13,
             xber.leg_reference14,
             xber.leg_reference15,
             xber.leg_reference16,
             xber.leg_reference17,
             xber.leg_reference18,
             xber.leg_reference19,
             xber.leg_reference20,
             xber.leg_reference21,
             xber.leg_reference22,
             xber.leg_reference23,
             xber.leg_reference24,
             xber.leg_reference25,
             xber.leg_reference26,
             xber.leg_reference27,
             xber.leg_reference28,
             xber.leg_reference29,
             xber.leg_reference30,
             xber.leg_je_batch_id,
             xber.leg_batch_name,
             xber.leg_batch_description,
             xber.leg_period_name,
             xber.leg_je_header_id,
             xber.leg_journal_name,
             xber.leg_journal_description,
             xber.leg_je_line_num,
             xber.leg_je_line_description,
             NULL AS chart_of_accounts_id,
             NULL AS functional_currency_code,
             NULL AS code_combination_id,
             NULL AS segment1,
             NULL AS segment2,
             NULL AS segment3,
             NULL AS segment4,
             NULL AS segment5,
             NULL AS segment6,
             NULL AS segment7,
             NULL AS segment8,
             NULL AS segment9,
             NULL AS segment10,
             xber.leg_date_created_in_gl,
             NULL AS warning_code,
             xber.status_description,
             xber.stat_amount,
             NULL AS group_id,
             NULL AS subledger_doc_sequence_id,
             NULL AS subledger_doc_sequence_value,
             xber.leg_attribute1,
             xber.leg_attribute2,
             xber.leg_attribute3,
             xber.leg_attribute4,
             xber.leg_attribute5,
             xber.leg_attribute6,
             xber.leg_attribute7,
             xber.leg_attribute8,
             xber.leg_attribute9,
             xber.leg_attribute10,
             xber.leg_attribute11,
             xber.leg_attribute12,
             xber.leg_attribute13,
             xber.leg_attribute14,
             xber.leg_attribute15,
             xber.leg_attribute16,
             xber.leg_attribute17,
             xber.leg_attribute18,
             xber.leg_attribute19,
             xber.leg_attribute20,
             xber.leg_context,
             xber.leg_context2,
             xber.leg_invoice_date,
             xber.leg_tax_code,
             xber.leg_invoice_identifier,
             xber.leg_invoice_amount,
             xber.leg_context3,
             xber.ussgl_transaction_code,
             xber.descr_flex_error_message,
             xber.jgzz_recon_ref,
             xber.average_journal_flag,
             xber.originating_bal_seg_value,
             xber.gl_sl_link_id,
             xber.gl_sl_link_table,
             xber.reference_date,
             NULL AS set_of_books_id,
             xber.balancing_segment_value,
             xber.management_segment_value,
             xber.code_combination_id_interim,
             xber.funds_reserved_flag,
             NULL AS error_type,
             xber.leg_request_id,
             xber.leg_process_flag
        FROM xxgl_balance_ext_r12 xber
       WHERE xber.leg_process_flag = g_flag_v
         AND xber.leg_entity = g_entity
         AND xber.leg_source_system =
             NVL(g_source_system, leg_source_system)
         AND xber.leg_ledger_name = NVL(g_set_of_books, leg_ledger_name)
         AND TRUNC(xber.leg_accounting_date) BETWEEN
             NVL(g_period_start, TRUNC(xber.leg_accounting_date)) AND
             NVL(g_period_end, TRUNC(xber.leg_accounting_date + 1)) ;

    TYPE ext_recs_cur_type IS TABLE OF ext_recs_cur%ROWTYPE INDEX BY BINARY_INTEGER;
    l_ext_recs_cur_type ext_recs_cur_type;

    l_error_tab_type xxetn_common_error_pkg.g_source_tab_type;

  BEGIN

    debug('get_data >>');
    pov_return_status := g_normal;
    l_error_message   := NULL;

    g_batch_id        := xxetn_batches_s.NEXTVAL;
    g_run_sequence_id := xxetn_run_sequences_s.NEXTVAL;

    FND_FILE. PUT_LINE(FND_FILE.log, 'Step1.5' || g_batch_id);

    debug('g_batch_id        : ' || g_batch_id);
    debug('g_run_sequence_id : ' || g_run_sequence_id);
--Added for v2.8 START    
    BEGIN

      dbms_stats.gather_table_stats(ownname          => 'XXCONV',
                                    tabname          => 'XXGL_BALANCE_STG',
                                    cascade          => true,
                                    estimate_percent => 20);--dbms_stats.auto_sample_size,
                                    --degree           => dbms_stats.default_degree);

    END;
--Added for v2.8 END
    
--Added for v2.8 START    
    BEGIN

      dbms_stats.gather_table_stats(ownname          => 'XXEXTN',
                                    tabname          => 'XXGL_BALANCE_STG',
                                    cascade          => true,
                                    estimate_percent => 20);--dbms_stats.auto_sample_size,
                                    --degree           => dbms_stats.default_degree);

    END;
--Added for v2.8 END
    
    OPEN ext_recs_cur;
    LOOP
      FETCH ext_recs_cur BULK COLLECT
        INTO l_ext_recs_cur_type LIMIT g_bulk_limit;
          FND_FILE. PUT_LINE(FND_FILE.log, 'Step1.6' || g_batch_id);

      debug('l_ext_recs_cur_type.COUNT ' || l_ext_recs_cur_type.COUNT);

      IF l_ext_recs_cur_type.COUNT > 0 THEN
        FND_FILE. PUT_LINE(FND_FILE.log, 'Step1.7' || g_batch_id);
        BEGIN
          FORALL indx IN 1 .. l_ext_recs_cur_type.COUNT SAVE EXCEPTIONS

            INSERT INTO xxgl_balance_stg
              (interface_txn_id,
               leg_source_system,
               leg_entity,
               leg_seq_num,
               leg_status,
               status,
               leg_ledger_name,
               leg_sob_short_name,
               leg_accounting_date,
               leg_start_date,
               leg_end_date,
               leg_period_year,
               leg_period_num,
               leg_currency_code,
               leg_actual_flag,
               leg_user_je_cat_name,
               leg_user_je_src_name,
               leg_curr_conv_date,
               leg_encumbrance_type,
               leg_budget_name,
               leg_version_num,
               leg_user_curr_conv_type,
               leg_curr_con_rate,
               leg_segment1,
               leg_segment2,
               leg_segment3,
               leg_segment4,
               leg_segment5,
               leg_segment6,
               leg_segment7,
               leg_segment8,
               leg_segment9,
               leg_segment10,
               leg_segment11,
               leg_segment12,
               leg_segment13,
               leg_segment14,
               leg_segment15,
               leg_segment16,
               leg_segment17,
               leg_segment18,
               leg_segment19,
               leg_segment20,
               leg_segment21,
               leg_segment22,
               leg_segment23,
               leg_segment24,
               leg_segment25,
               leg_segment26,
               leg_segment27,
               leg_segment28,
               leg_segment29,
               leg_segment30,
               leg_begin_balance_cr,
               leg_begin_balance_dr,
               leg_period_net_cr,
               leg_period_net_dr,
               leg_begin_balance_cr_beq -- v2.2
              ,
               leg_begin_balance_dr_beq -- v2.2
              ,
               leg_period_net_cr_beq -- v2.2
              ,
               leg_period_net_dr_beq -- v2.2
              ,
               leg_entered_dr,
               leg_entered_cr,
               leg_accounted_dr,
               leg_accounted_cr,
               leg_transaction_date,
               leg_reference1,
               leg_reference2,
               leg_reference3,
               leg_reference4,
               leg_reference5,
               leg_reference6,
               leg_reference7,
               leg_reference8,
               leg_reference9,
               leg_reference10,
               leg_reference11,
               leg_reference12,
               leg_reference13,
               leg_reference14,
               leg_reference15,
               leg_reference16,
               leg_reference17,
               leg_reference18,
               leg_reference19,
               leg_reference20,
               leg_reference21,
               leg_reference22,
               leg_reference23,
               leg_reference24,
               leg_reference25,
               leg_reference26,
               leg_reference27,
               leg_reference28,
               leg_reference29,
               leg_reference30,
               leg_je_batch_id,
               leg_batch_name,
               leg_batch_description,
               leg_period_name,
               leg_je_header_id,
               leg_journal_name,
               leg_journal_description,
               leg_je_line_num,
               leg_je_line_description,
               chart_of_accounts_id,
               functional_currency_code,
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
               leg_date_created_in_gl,
               warning_code,
               status_description,
               stat_amount,
               group_id,
               subledger_doc_sequence_id,
               subledger_doc_sequence_value,
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
               leg_attribute16,
               leg_attribute17,
               leg_attribute18,
               leg_attribute19,
               leg_attribute20,
               leg_context,
               leg_context2,
               leg_invoice_date,
               leg_tax_code,
               leg_invoice_identifier,
               leg_invoice_amount,
               leg_context3,
               ussgl_transaction_code,
               descr_flex_error_message,
               jgzz_recon_ref,
               average_journal_flag,
               originating_bal_seg_value,
               gl_sl_link_id,
               gl_sl_link_table,
               reference_date,
               set_of_books_id,
               balancing_segment_value,
               management_segment_value,
               code_combination_id_interim,
               funds_reserved_flag,
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
               leg_request_id,
               leg_process_flag)
            VALUES
              (l_ext_recs_cur_type(indx).interface_txn_id,
               l_ext_recs_cur_type(indx).leg_source_system,
               l_ext_recs_cur_type(indx).leg_entity,
               l_ext_recs_cur_type(indx).leg_seq_num,
               l_ext_recs_cur_type(indx).leg_status,
               l_ext_recs_cur_type(indx).status,
               l_ext_recs_cur_type(indx).leg_ledger_name,
               l_ext_recs_cur_type(indx).leg_sob_short_name,
               l_ext_recs_cur_type(indx).leg_accounting_date,
               l_ext_recs_cur_type(indx).leg_start_date,
               l_ext_recs_cur_type(indx).leg_end_date,
               l_ext_recs_cur_type(indx).leg_period_year,
               l_ext_recs_cur_type(indx).leg_period_num,
               l_ext_recs_cur_type(indx).leg_currency_code,
               l_ext_recs_cur_type(indx).leg_actual_flag,
               l_ext_recs_cur_type(indx).leg_user_je_cat_name,
               l_ext_recs_cur_type(indx).leg_user_je_src_name,
               l_ext_recs_cur_type(indx).leg_curr_conv_date,
               l_ext_recs_cur_type(indx).leg_encumbrance_type,
               l_ext_recs_cur_type(indx).leg_budget_name,
               l_ext_recs_cur_type(indx).leg_version_num,
               l_ext_recs_cur_type(indx).leg_user_curr_conv_type,
               l_ext_recs_cur_type(indx).leg_curr_con_rate,
               l_ext_recs_cur_type(indx).leg_segment1,
               l_ext_recs_cur_type(indx).leg_segment2,
               l_ext_recs_cur_type(indx).leg_segment3,
               l_ext_recs_cur_type(indx).leg_segment4,
               l_ext_recs_cur_type(indx).leg_segment5,
               l_ext_recs_cur_type(indx).leg_segment6,
               l_ext_recs_cur_type(indx).leg_segment7,
               l_ext_recs_cur_type(indx).leg_segment8,
               l_ext_recs_cur_type(indx).leg_segment9,
               l_ext_recs_cur_type(indx).leg_segment10,
               l_ext_recs_cur_type(indx).leg_segment11,
               l_ext_recs_cur_type(indx).leg_segment12,
               l_ext_recs_cur_type(indx).leg_segment13,
               l_ext_recs_cur_type(indx).leg_segment14,
               l_ext_recs_cur_type(indx).leg_segment15,
               l_ext_recs_cur_type(indx).leg_segment16,
               l_ext_recs_cur_type(indx).leg_segment17,
               l_ext_recs_cur_type(indx).leg_segment18,
               l_ext_recs_cur_type(indx).leg_segment19,
               l_ext_recs_cur_type(indx).leg_segment20,
               l_ext_recs_cur_type(indx).leg_segment21,
               l_ext_recs_cur_type(indx).leg_segment22,
               l_ext_recs_cur_type(indx).leg_segment23,
               l_ext_recs_cur_type(indx).leg_segment24,
               l_ext_recs_cur_type(indx).leg_segment25,
               l_ext_recs_cur_type(indx).leg_segment26,
               l_ext_recs_cur_type(indx).leg_segment27,
               l_ext_recs_cur_type(indx).leg_segment28,
               l_ext_recs_cur_type(indx).leg_segment29,
               l_ext_recs_cur_type(indx).leg_segment30,
               l_ext_recs_cur_type(indx).leg_begin_balance_cr,
               l_ext_recs_cur_type(indx).leg_begin_balance_dr,
               l_ext_recs_cur_type(indx).leg_period_net_cr,
               l_ext_recs_cur_type(indx).leg_period_net_dr,
               l_ext_recs_cur_type(indx).leg_begin_balance_cr_beq -- v2.2
              ,
               l_ext_recs_cur_type(indx).leg_begin_balance_dr_beq -- v2.2
              ,
               l_ext_recs_cur_type(indx).leg_period_net_cr_beq -- v2.2
              ,
               l_ext_recs_cur_type(indx).leg_period_net_dr_beq -- v2.2
              ,
               l_ext_recs_cur_type(indx).leg_entered_dr,
               l_ext_recs_cur_type(indx).leg_entered_cr,
               l_ext_recs_cur_type(indx).leg_accounted_dr,
               l_ext_recs_cur_type(indx).leg_accounted_cr,
               l_ext_recs_cur_type(indx).leg_transaction_date,
               l_ext_recs_cur_type(indx).leg_reference1,
               l_ext_recs_cur_type(indx).leg_reference2,
               l_ext_recs_cur_type(indx).leg_reference3,
               l_ext_recs_cur_type(indx).leg_reference4,
               l_ext_recs_cur_type(indx).leg_reference5,
               l_ext_recs_cur_type(indx).leg_reference6,
               l_ext_recs_cur_type(indx).leg_reference7,
               l_ext_recs_cur_type(indx).leg_reference8,
               l_ext_recs_cur_type(indx).leg_reference9,
               l_ext_recs_cur_type(indx).leg_reference10,
               l_ext_recs_cur_type(indx).leg_reference11,
               l_ext_recs_cur_type(indx).leg_reference12,
               l_ext_recs_cur_type(indx).leg_reference13,
               l_ext_recs_cur_type(indx).leg_reference14,
               l_ext_recs_cur_type(indx).leg_reference15,
               l_ext_recs_cur_type(indx).leg_reference16,
               l_ext_recs_cur_type(indx).leg_reference17,
               l_ext_recs_cur_type(indx).leg_reference18,
               l_ext_recs_cur_type(indx).leg_reference19,
               l_ext_recs_cur_type(indx).leg_reference20,
               l_ext_recs_cur_type(indx).leg_reference21,
               l_ext_recs_cur_type(indx).leg_reference22,
               l_ext_recs_cur_type(indx).leg_reference23,
               l_ext_recs_cur_type(indx).leg_reference24,
               l_ext_recs_cur_type(indx).leg_reference25,
               l_ext_recs_cur_type(indx).leg_reference26,
               l_ext_recs_cur_type(indx).leg_reference27,
               l_ext_recs_cur_type(indx).leg_reference28,
               l_ext_recs_cur_type(indx).leg_reference29,
               l_ext_recs_cur_type(indx).leg_reference30,
               l_ext_recs_cur_type(indx).leg_je_batch_id,
               l_ext_recs_cur_type(indx).leg_batch_name,
               l_ext_recs_cur_type(indx).leg_batch_description,
               l_ext_recs_cur_type(indx).leg_period_name,
               l_ext_recs_cur_type(indx).leg_je_header_id,
               l_ext_recs_cur_type(indx).leg_journal_name,
               l_ext_recs_cur_type(indx).leg_journal_description,
               l_ext_recs_cur_type(indx).leg_je_line_num,
               l_ext_recs_cur_type(indx).leg_je_line_description,
               l_ext_recs_cur_type(indx).chart_of_accounts_id,
               l_ext_recs_cur_type(indx).functional_currency_code,
               l_ext_recs_cur_type(indx).code_combination_id,
               l_ext_recs_cur_type(indx).segment1,
               l_ext_recs_cur_type(indx).segment2,
               l_ext_recs_cur_type(indx).segment3,
               l_ext_recs_cur_type(indx).segment4,
               l_ext_recs_cur_type(indx).segment5,
               l_ext_recs_cur_type(indx).segment6,
               l_ext_recs_cur_type(indx).segment7,
               l_ext_recs_cur_type(indx).segment8,
               l_ext_recs_cur_type(indx).segment9,
               l_ext_recs_cur_type(indx).segment10,
               l_ext_recs_cur_type(indx).leg_date_created_in_gl,
               l_ext_recs_cur_type(indx).warning_code,
               l_ext_recs_cur_type(indx).status_description,
               l_ext_recs_cur_type(indx).stat_amount,
               l_ext_recs_cur_type(indx).group_id,
               l_ext_recs_cur_type(indx).subledger_doc_sequence_id,
               l_ext_recs_cur_type(indx).subledger_doc_sequence_value,
               l_ext_recs_cur_type(indx).leg_attribute1,
               l_ext_recs_cur_type(indx).leg_attribute2,
               l_ext_recs_cur_type(indx).leg_attribute3,
               l_ext_recs_cur_type(indx).leg_attribute4,
               l_ext_recs_cur_type(indx).leg_attribute5,
               l_ext_recs_cur_type(indx).leg_attribute6,
               l_ext_recs_cur_type(indx).leg_attribute7,
               l_ext_recs_cur_type(indx).leg_attribute8,
               l_ext_recs_cur_type(indx).leg_attribute9,
               l_ext_recs_cur_type(indx).leg_attribute10,
               l_ext_recs_cur_type(indx).leg_attribute11,
               l_ext_recs_cur_type(indx).leg_attribute12,
               l_ext_recs_cur_type(indx).leg_attribute13,
               l_ext_recs_cur_type(indx).leg_attribute14,
               l_ext_recs_cur_type(indx).leg_attribute15,
               l_ext_recs_cur_type(indx).leg_attribute16,
               l_ext_recs_cur_type(indx).leg_attribute17,
               l_ext_recs_cur_type(indx).leg_attribute18,
               l_ext_recs_cur_type(indx).leg_attribute19,
               l_ext_recs_cur_type(indx).leg_attribute20,
               l_ext_recs_cur_type(indx).leg_context,
               l_ext_recs_cur_type(indx).leg_context2,
               l_ext_recs_cur_type(indx).leg_invoice_date,
               l_ext_recs_cur_type(indx).leg_tax_code,
               l_ext_recs_cur_type(indx).leg_invoice_identifier,
               l_ext_recs_cur_type(indx).leg_invoice_amount,
               l_ext_recs_cur_type(indx).leg_context3,
               l_ext_recs_cur_type(indx).ussgl_transaction_code,
               l_ext_recs_cur_type(indx).descr_flex_error_message,
               l_ext_recs_cur_type(indx).jgzz_recon_ref,
               l_ext_recs_cur_type(indx).average_journal_flag,
               l_ext_recs_cur_type(indx).originating_bal_seg_value,
               l_ext_recs_cur_type(indx).gl_sl_link_id,
               l_ext_recs_cur_type(indx).gl_sl_link_table,
               l_ext_recs_cur_type(indx).reference_date,
               l_ext_recs_cur_type(indx).set_of_books_id,
               l_ext_recs_cur_type(indx).balancing_segment_value,
               l_ext_recs_cur_type(indx).management_segment_value,
               l_ext_recs_cur_type(indx).code_combination_id_interim,
               l_ext_recs_cur_type(indx).funds_reserved_flag,
               SYSDATE --creation_date
              ,
               g_user_id --created_by
              ,
               SYSDATE --last_updated_date
              ,
               g_user_id --last_updated_by
              ,
               g_login_id --last_update_login
              ,
               g_prog_appl_id --program_application_id
              ,
               g_program_id --program_id
              ,
               SYSDATE --program_update_date
              ,
               g_request_id --request_id
              ,
               g_flag_n --process_flag
              ,
               l_ext_recs_cur_type(indx).error_type,
               l_ext_recs_cur_type(indx).leg_request_id,
               l_ext_recs_cur_type(indx).leg_process_flag);

               FND_FILE. PUT_LINE(FND_FILE.log, 'Step1.8' || g_batch_id);
        EXCEPTION
          WHEN g_bulk_exception THEN
            l_bluk_error_count := l_bluk_error_count +
                                  SQL%BULK_EXCEPTIONS.COUNT;
            FOR exep_indx IN 1 .. SQL%BULK_EXCEPTIONS.COUNT LOOP
              l_excep_col1    := l_ext_recs_cur_type(SQL%BULK_EXCEPTIONS(exep_indx).ERROR_INDEX)
                                 .interface_txn_id;
              l_error_message := SQLERRM(-1 * (SQL%BULK_EXCEPTIONS(exep_indx)
                                         .ERROR_CODE));

              debug('Record sequence : ' || l_excep_col1);
              debug('Error Message   : ' || l_error_message);

              --update leg_process_flag to 'E' for error records
              UPDATE xxgl_balance_ext_r12
                 SET leg_process_flag  = g_flag_e,
                     last_updated_date = SYSDATE,
                     last_updated_by   = g_user_id,
                     last_update_login = g_login_id
               WHERE interface_txn_id = TO_NUMBER(l_excep_col1)
                 AND leg_process_flag = g_flag_v;

              l_error_tab_type(exep_indx).source_table := g_source_table;
              l_error_tab_type(exep_indx).interface_staging_id := l_excep_col1;
              l_error_tab_type(exep_indx).source_keyname1 := NULL;
              l_error_tab_type(exep_indx).source_keyvalue1 := NULL;
              l_error_tab_type(exep_indx).source_keyname2 := NULL;
              l_error_tab_type(exep_indx).source_keyvalue2 := NULL;
              l_error_tab_type(exep_indx).source_keyname3 := NULL;
              l_error_tab_type(exep_indx).source_keyvalue3 := NULL;
              l_error_tab_type(exep_indx).source_keyname4 := NULL;
              l_error_tab_type(exep_indx).source_keyvalue4 := NULL;
              l_error_tab_type(exep_indx).source_keyname5 := NULL;
              l_error_tab_type(exep_indx).source_keyvalue5 := NULL;
              l_error_tab_type(exep_indx).source_column_name := NULL;
              l_error_tab_type(exep_indx).source_column_value := NULL;
              l_error_tab_type(exep_indx).error_type := g_err_validation;
              l_error_tab_type(exep_indx).error_code := 'ETN_BLKEXCEP_GET_DATA';
              l_error_tab_type(exep_indx).error_message := l_error_message;
              l_error_tab_type(exep_indx).severity := NULL;
              l_error_tab_type(exep_indx).proposed_solution := NULL;
           ----   l_error_tab_type(exep_indx).stage := NULL; -- v2.3
            --  l_error_tab_type(exep_indx).stage := NULL; --AS PER CHANGES DONE BY SAGAR

              FND_FILE. PUT_LINE(FND_FILE.log, 'Step1.9' || g_batch_id);

              log_error(pin_batch_id        => NULL,
                        pin_run_sequence_id => NULL,
                        p_source_tab_type   => l_error_tab_type,
                        pov_return_status   => l_log_err_sts,
                        pov_error_message   => l_log_err_msg);
            END LOOP;
        END;

        FOR i IN 1 .. l_ext_recs_cur_type.COUNT LOOP
          l_load_count := l_load_count + SQL%BULK_ROWCOUNT(i);
        END LOOP;
      END IF;

      COMMIT;
      EXIT WHEN ext_recs_cur%NOTFOUND;
    END LOOP;
    CLOSE ext_recs_cur;

    IF l_bluk_error_count > 0 THEN
      pov_return_status := g_warning;
      debug(l_bluk_error_count || ' records errored during bulk insert');
    END IF;

    g_records_loaded := l_load_count;
    g_records_error  := l_bluk_error_count;
--Commented for v2.8 START
-- Added by Loats on 17-Nov-2016 for INT2 testing.
--fnd_stats.gather_table_stats('XXCONV','XXGL_BALANCE_STG');
--Commented for v2.8 END

--Added for v2.8 START
    BEGIN

      dbms_stats.gather_table_stats(ownname          => 'XXCONV',
                                    tabname          => 'XXGL_BALANCE_STG',
                                    cascade          => true,
                                    estimate_percent => 20);--dbms_stats.auto_sample_size,
                                    --degree           => dbms_stats.default_degree);

    END;
--Added for v2.8 END
    --Update successful records to 'P' in extraction Table
    UPDATE xxgl_balance_ext_r12 xber
       SET xber.leg_process_flag  = g_flag_p,
           xber.last_updated_date = SYSDATE,
           xber.last_updated_by   = g_user_id,
           xber.last_update_login = g_login_id
     WHERE leg_process_flag = g_flag_v
       AND leg_entity = g_entity
       AND leg_source_system = NVL(g_source_system, leg_source_system)
       AND leg_ledger_name = NVL(g_set_of_books, leg_ledger_name)
       AND TRUNC(leg_accounting_date) BETWEEN
           NVL(g_period_start, TRUNC(leg_accounting_date)) AND
           NVL(g_period_end, TRUNC(leg_accounting_date + 1))
       AND EXISTS (SELECT 1
              FROM xxgl_balance_stg xbs
             WHERE xbs.interface_txn_id = xber.interface_txn_id
               AND leg_entity = g_entity
               AND batch_id IS NULL);

    debug('Success records count ' || SQL%ROWCOUNT);
    COMMIT;
    debug('get_data <<');
  EXCEPTION
    WHEN OTHERS THEN
      l_error_message := SUBSTR('Exception in Procedure get_data. ' ||
                                SQLERRM,
                                1,
                                1999);
      fnd_file.put_line(fnd_file.LOG, l_error_message);
      fnd_file.put_line(fnd_file.LOG, 'get_data <<E');
      pov_return_status := g_error;
      pov_error_message := l_error_message;
  END get_data;

  -- =============================================================================
  -- Procedure: main
  -- =============================================================================
  -- Main Procedure; called from GL Balance Conversion Concurrent Program
  -- =============================================================================
  --  Input Parameters :
  --    piv_run_mode         : Run Mode
  --    piv_entity           : Entity Name
  --    piv_dummy1           : Dummy Parameter 1
  --    piv_source_system    : Legacy Source System Name
  --    piv_set_of_books     : Legacy Set of Books Name
  --    pid_period_start     : GL Period Start Date
  --    pid_period_end       : GL Period End Date
  --    pin_batch_id         : Batch Id
  --    piv_dummy2           : Dummy Parameter 2
  --    piv_process_records  : Process Records ('ALL', 'ERROR', 'UNPROCESSED')
  --  Output Parameters :
  --    pov_errbuf           : Error message in case of any failure
  --    pon_retcode          : Return Status - Normal/Warning/Error
  -- -----------------------------------------------------------------------------
  PROCEDURE main(pov_errbuf          OUT NOCOPY VARCHAR2,
                 pon_retcode         OUT NOCOPY NUMBER,
                 piv_run_mode        IN VARCHAR2,
                 piv_entity          IN VARCHAR2,
                 piv_dummy1          IN VARCHAR2,
                 piv_source_system   IN VARCHAR2,
                 piv_set_of_books    IN VARCHAR2,
                 pid_period_start    IN VARCHAR2,
                 pid_period_end      IN VARCHAR2,
                 pin_batch_id        IN NUMBER,
                 piv_dummy2          IN VARCHAR2,
                 piv_process_records IN VARCHAR2) IS
    process_exception    EXCEPTION;
    validation_exception EXCEPTION;

    l_return_status VARCHAR2(1);
    l_error_message VARCHAR2(4000);

    l_batch_id        NUMBER;
    l_run_sequence_id NUMBER;

    l_last_stmt NUMBER := 10;

    l_period_start_date DATE;
    l_period_end_date   DATE;
  BEGIN
    pov_errbuf  := NULL;
    pon_retcode := g_normal;

    l_return_status := g_normal;
    l_error_message := '';

    g_run_mode       := piv_run_mode;
    g_entity         := piv_entity;
    g_source_system  := piv_source_system;
    g_set_of_books   := piv_set_of_books;
    g_period_start   := FND_DATE.CANONICAL_TO_DATE(pid_period_start);
    g_period_end     := FND_DATE.CANONICAL_TO_DATE(pid_period_end);
    g_batch_id       := pin_batch_id;
    g_reprocess_mode := piv_process_records;

    --Printing program parameters to program log
    debug('|---------------------------------------');
    debug('|Program Parameters');
    debug('|---------------------------------------');
    debug('|g_run_mode        : ' || g_run_mode);
    debug('|g_entity          : ' || g_entity);
    debug('|g_source_system   : ' || g_source_system);
    debug('|g_set_of_books    : ' || g_set_of_books);
    debug('|g_period_start    : ' || g_period_start);
    debug('|g_period_end      : ' || g_period_end);
    debug('|g_batch_id        : ' || g_batch_id);
    debug('|g_reprocess_mode  : ' || g_reprocess_mode);
    debug('|---------------------------------------');

    l_period_start_date := g_period_start;
    l_period_end_date   := g_period_end;

    --Initializing Common Error Framework API
    xxetn_debug_pkg.initialize_debug(pov_err_msg      => l_error_message,
                                     piv_program_name => 'ETN_GL_JOURNAL_CONVERSION');

    IF l_error_message IS NOT NULL THEN
      l_error_message := 'Error while initializing common debug utility';
      RAISE process_exception;
    END IF;

    IF g_bulk_limit IS NULL OR g_bulk_limit < 1 THEN

      l_error_message := 'Profile ETN_FND_ERROR_TAB_LIMIT value is ' ||
                         NVL(g_bulk_limit, 'NULL');

      RAISE process_exception;
    END IF;

    IF (UPPER(g_run_mode) = g_run_mode_loadata) THEN

      --calling procedure to load data from extraction table to staging table
      get_data(pov_return_status => l_return_status,
               pov_error_message => l_error_message);

      pon_retcode := l_return_status;
      pov_errbuf  := l_error_message;

    ELSIF (UPPER(g_run_mode) = g_run_mode_prevalidate) THEN

      --calling procedure to perform pre-validation
      pre_validate(pov_return_status => l_return_status,
                   pov_error_message => l_error_message);

      pon_retcode := l_return_status;
      pov_errbuf  := l_error_message;

    ELSIF (UPPER(g_run_mode) = g_run_mode_validate) THEN

      IF (pin_batch_id IS NOT NULL) THEN
        l_batch_id := pin_batch_id;
      END IF;

      --calling procedure to assign batch_id
      assign_batch(piv_entity           => piv_entity --IN
                  ,
                   piv_source_system    => piv_source_system --IN
                  ,
                   piv_set_of_books     => piv_set_of_books --IN
                  ,
                   pid_period_start     => l_period_start_date --IN
                  ,
                   pid_period_end       => l_period_end_date --IN
                  ,
                   pin_batch_id         => l_batch_id --IN
                  ,
                   piv_run_mode         => piv_run_mode --IN
                  ,
                   piv_process_records  => piv_process_records --IN
                  ,
                   pion_run_sequence_id => l_run_sequence_id --IN OUT
                  ,
                   pov_return_status    => l_return_status --OUT
                  ,
                   pov_error_message    => l_error_message); --OUT

      IF l_return_status <> g_normal THEN
        l_error_message := SUBSTR('Assigning Batch' || l_error_message,
                                  1,
                                  1999);
        RAISE process_exception;
      END IF;

      debug(' batch id           : ' || l_batch_id);
      debug(' Run Sequence id    : ' || l_run_sequence_id);
      debug(' g_batch_id         : ' || g_batch_id);
      debug(' g_run_sequence_id  : ' || g_run_sequence_id);

      --calling procedure to validate_data data (for specified batch_id) in staging table
      validate_data(piv_entity          => piv_entity,
                    pin_batch_id        => g_batch_id,
                    pin_run_sequence_id => g_run_sequence_id,
                    pov_return_status   => l_return_status,
                    pov_error_message   => l_error_message);

      IF l_return_status <> g_normal THEN
        RAISE validation_exception;
      END IF;

    ELSIF (UPPER(g_run_mode) = g_run_mode_conversion) THEN

      IF (pin_batch_id IS NULL) THEN
        l_error_message := ' BATCH_ID parameter cannot be NULL, Please enter a BATCH_ID';
        RAISE process_exception;
      ELSE
        l_batch_id := pin_batch_id;
      END IF;

      --calling procedure to re-assign batch_id based on program parameters
      assign_batch(piv_entity           => piv_entity --IN
                  ,
                   piv_source_system    => piv_source_system --IN
                  ,
                   piv_set_of_books     => piv_set_of_books --IN
                  ,
                   pid_period_start     => l_period_start_date --IN
                  ,
                   pid_period_end       => l_period_end_date --IN
                  ,
                   pin_batch_id         => l_batch_id --IN
                  ,
                   piv_run_mode         => piv_run_mode --IN
                  ,
                   piv_process_records  => piv_process_records --IN
                  ,
                   pion_run_sequence_id => l_run_sequence_id --IN OUT
                  ,
                   pov_return_status    => l_return_status --OUT
                  ,
                   pov_error_message    => l_error_message); --OUT

      debug('l_run_sequence_id ' || l_run_sequence_id || ' g_batch_id ' ||
            g_batch_id || ' g_run_sequence_id ' || g_run_sequence_id);

      IF l_return_status <> g_normal THEN
        l_error_message := SUBSTR(l_error_message, 1, 1999);
        RAISE process_exception;
      END IF;

      --calling procedure to insert records into GL_INTERFACE table
      interface_data(piv_entity          => piv_entity,
                     pin_batch_id        => g_batch_id,
                     pin_run_sequence_id => g_run_sequence_id,
                     pov_return_status   => l_return_status,
                     pov_error_message   => l_error_message);

      /*** Added for v2.2 ***/
      IF piv_entity = g_entity_foreignbalance THEN
        --calling procedure to insert records into GL_INTERFACE table to offset Foreign Balance Functional amounts
        interface_offset_balance(piv_entity          => piv_entity,
                                 pin_batch_id        => g_batch_id,
                                 pin_run_sequence_id => g_run_sequence_id,
                                 pov_return_status   => l_return_status,
                                 pov_error_message   => l_error_message);
      END IF;

    ELSIF (UPPER(g_run_mode) = g_run_mode_reconcilition) THEN
      g_batch_id        := pin_batch_id;
      g_run_sequence_id := NULL;
    END IF;

    IF (UPPER(g_run_mode) <> g_run_mode_prevalidate) THEN
      --Print Report Output
      print_report(piv_entity          => piv_entity,
                   pin_batch_id        => g_batch_id,
                   pin_run_sequence_id => g_run_sequence_id,
                   pov_return_status   => l_return_status,
                   pov_error_message   => l_error_message);

      IF l_return_status <> g_normal THEN
        l_error_message := SUBSTR(l_error_message, 1, 1999);
        RAISE process_exception;
      END IF;

      pon_retcode := g_normal;
      pov_errbuf  := NULL;
    END IF;
  EXCEPTION
    WHEN validation_exception THEN
      pon_retcode := g_warning;
      pov_errbuf  := l_error_message;
      fnd_file.put_line(fnd_file.LOG, pon_retcode);
      fnd_file.put_line(fnd_file.LOG, l_error_message);
      --Print Report Output
      print_report(piv_entity          => piv_entity,
                   pin_batch_id        => NVL(pin_batch_id, l_batch_id),
                   pin_run_sequence_id => l_run_sequence_id,
                   pov_return_status   => l_return_status,
                   pov_error_message   => l_error_message);
    WHEN process_exception THEN
      pon_retcode := g_warning;
      pov_errbuf  := l_error_message;
      fnd_file.put_line(fnd_file.LOG, pon_retcode);
      fnd_file.put_line(fnd_file.LOG, l_error_message);
      --Print Report Output
      print_report(piv_entity          => piv_entity,
                   pin_batch_id        => NVL(pin_batch_id, l_batch_id),
                   pin_run_sequence_id => l_run_sequence_id,
                   pov_return_status   => l_return_status,
                   pov_error_message   => l_error_message);
    WHEN OTHERS THEN
      l_error_message := SUBSTR('Exception in Procedure main. ' || SQLERRM,
                                1,
                                1999);
      pon_retcode     := g_error;
      pov_errbuf      := l_error_message;
      fnd_file.put_line(fnd_file.LOG, pon_retcode);
      fnd_file.put_line(fnd_file.LOG, l_error_message);
  END main;

END xxgl_balance_import_pkg;
/

CREATE OR REPLACE PACKAGE BODY xxpa_proj_expend_cnv_pkg AS
  ------------------------------------------------------------------------------------------
  --    Owner        : EATON CORPORATION.
  --    Application  : Eaton Projects
  --    Schema       : APPS
  --    Compile AS   : APPS
  --    File Name    : XXPA_PROJ_EXPEND_CNV_PKG.pkb
  --    Date         : 08-May-2014
  --    Author       : Kulraj Singh
  --    Description  : Package body for Project Expenditures Conversion (RTR-CNV-0008)
  --
  --    Version      : $ETNHeader: /CCSTORE/ccweb/C9916816/C9916816_view_PA_TOP_r12/vobs/PA_TOP/xxpa/12.0.0/install/XXPA_PROJ_EXPEND_CNV_PKG.pkb /main/8 11-May-2016 09:55:16 C9916816  $
  --
  --    Parameters  :
  --
  --    Change History
  --  ======================================================================================
  --    v1.0        Kulraj Singh    08-May-2014     Initial Creation
  --    v2.0        Harjinder Singh 8-5-2015         --changes as per defect 1740
  --    v3.0        Harjinder Singh 11-5-2015         --changes as per defect 1821
  --    V4.0        Harjinder Singh 08-10-2015        --- -----CHAGES DONE FOR THE PMC 332169
  --    v5.0        Harjinder Singh 07-Jan-2015     ---Removed upper clause from procedure validate_proj_details_p
  --    v6.0        Tushar Sharma   24-Aug-2016     Changes as per CR# 404561 to comment the logic of expenditure item date > project end date
  --  ======================================================================================
  ------------------------------------------------------------------------------------------

  -- ---------------------------------------------------------
  -- P R I V A T E - G L O B A L - V A R I A B L E S
  -- ---------------------------------------------------------

  -- WHO columns global variables
  g_last_updated_by   NUMBER DEFAULT fnd_global.user_id;
  g_last_update_login NUMBER DEFAULT fnd_global.login_id;
  g_request_id        NUMBER DEFAULT fnd_global.conc_request_id;
  g_prog_appl_id      NUMBER DEFAULT fnd_global.prog_appl_id;
  g_conc_program_id   NUMBER DEFAULT fnd_global.conc_program_id;

  -- Stats global variables
  g_total_count      NUMBER DEFAULT 0;
  g_success_count    NUMBER DEFAULT 0;
  g_failed_count     NUMBER DEFAULT 0;
  g_failed_count_imp NUMBER DEFAULT 0;
  g_failed_count_int NUMBER DEFAULT 0;

  -- Batch Id and Run Sequence Id variables
  g_new_batch_id   NUMBER DEFAULT NULL;
  g_new_run_seq_id NUMBER DEFAULT NULL;
  g_limit          NUMBER DEFAULT 500;

  -- Error Types variables
  g_val_err_type xxpa_proj_expend_stg.error_type%TYPE DEFAULT 'ERR_VAL';
  g_int_err_type xxpa_proj_expend_stg.error_type%TYPE DEFAULT 'ERR_INT';
  g_imp_err_type xxpa_proj_expend_stg.error_type%TYPE DEFAULT 'ERR_IMP';
  g_val_rec      VARCHAR2(10) DEFAULT NULL;
  g_err_rec      VARCHAR2(10) DEFAULT NULL;
  g_new_rec      VARCHAR2(10) DEFAULT NULL;

  -- Custom Profiles variables
  g_err_tab_limit NUMBER DEFAULT fnd_profile.value('ETN_FND_ERROR_TAB_LIMIT');
  g_err_cnt       NUMBER DEFAULT 1;

  -- Table Type for Error table
  g_source_tab xxetn_common_error_pkg.g_source_tab_type;

  -- Program Parameter Global variables
  g_run_mode        VARCHAR2(50);
  g_batch_id        NUMBER;
  g_process_records VARCHAR2(50);

  -- Record status Global variables
  g_new        xxpa_proj_expend_stg.process_flag%TYPE DEFAULT 'N';
  g_validated  xxpa_proj_expend_stg.process_flag%TYPE DEFAULT 'V';
  g_error      xxpa_proj_expend_stg.process_flag%TYPE DEFAULT 'E';
  g_interfaced xxpa_proj_expend_stg.process_flag%TYPE DEFAULT 'P';
  g_converted  xxpa_proj_expend_stg.process_flag%TYPE DEFAULT 'C';

  -- Custom Lookups variables
  g_ou_lookup    fnd_lookup_types.lookup_type%TYPE DEFAULT 'ETN_COMMON_OU_MAP';
  g_task_lookup  fnd_lookup_types.lookup_type%TYPE DEFAULT 'XXETN_PA_TASK_MAPPING';
  g_expend_type  pa_expenditure_types.expenditure_type%TYPE DEFAULT 'CAPITAL MATERIAL';
  g_trx_source   pa_transaction_sources.transaction_source%TYPE DEFAULT 'CONVERT';
  g_source_nafsc xxpa_proj_expend_stg.leg_source_system%TYPE DEFAULT 'NAFSC';
  g_source_issc  xxpa_proj_expend_stg.leg_source_system%TYPE DEFAULT 'ISSC';

  --
  -- =============================================================================
  -- Procedure: debug_msg_p
  -- =============================================================================
  -- This private Procedure write Debug messages.Calls xxetn_debug_pkg.add_debug internally
  --
  -- =============================================================================
  --
  -- -----------------------------------------------------------------------------
  --  Called By: Various Procedures
  -- -----------------------------------------------------------------------------
  --
  --  Input Parameters :
  --  piv_type          : 'STEP' number.
  --  piv_name          : Procedure Name in which debug message is present
  --  piv_string        : Debug Message String
  --
  --  Output Parameters : NONE
  --
  --  Return            : Not applicable
  -- -----------------------------------------------------------------------------

  PROCEDURE debug_msg_p(piv_type   IN VARCHAR2,
                        piv_name   IN VARCHAR2,
                        piv_string IN VARCHAR2) IS
    l_error_message VARCHAR2(2000);

  BEGIN
    IF xxetn_debug_pkg.isdebugon THEN
      -- If Debug Profile is set to 'Y'
      xxetn_debug_pkg.add_debug(piv_type || ',XXPA_PROJ_EXPEND_CNV_PKG,' ||
                                piv_name || ',' || piv_string);
    END IF;
  EXCEPTION
    WHEN OTHERS THEN
      l_error_message := 'Reason :' || SUBSTR(SQLERRM, 1, 250);
      fnd_file.put_line(fnd_file.LOG,
                        'ERROR : XXPA_PROJ_EXPEND_CNV_PKG.DEBUG_MSG_P->WHEN_OTHERS->' ||
                        l_error_message);
  END debug_msg_p;

  --
  -- =============================================================================
  -- Procedure: print_log_message_p
  -- =============================================================================
  -- This private procedure writes messages in Log file
  --
  -- =============================================================================
  --
  -- -----------------------------------------------------------------------------
  --  Called By: Various Procedures
  -- -----------------------------------------------------------------------------
  --
  --  Input Parameters :
  --  piv_message       : Message to be printed in Log File
  --
  --  Output Parameters : NONE
  --
  --  Return            : Not applicable
  -- -----------------------------------------------------------------------------

  PROCEDURE print_log_message_p(piv_message IN VARCHAR2) IS
  BEGIN
    IF NVL(g_request_id, 0) > 0 THEN
      fnd_file.put_line(fnd_file.LOG, piv_message);
    END IF;
  END print_log_message_p;

  --
  -- =============================================================================
  -- Procedure: load_expend_data_p
  -- =============================================================================
  -- This procedure pulls Expenditure data from Extraction Table and insert into Conversion Table
  --
  -- =============================================================================
  --
  -- -----------------------------------------------------------------------------
  --  Called By: PROCESS_EXPENDITURES_P
  -- -----------------------------------------------------------------------------
  --
  --  Input Parameters : NONE
  --
  --  Output Parameters :
  --  pov_ret_stats     : Return Status of Procedure as 'S' or 'E'
  --  pov_err_msg       : Returns Error Message
  --
  --  Return            : Not applicable
  -- -----------------------------------------------------------------------------

  PROCEDURE load_expend_data_p(pov_ret_stats OUT NOCOPY VARCHAR2,
                               pov_err_msg   OUT NOCOPY VARCHAR2) IS

    /**** PL/SQL table for xxpa_proj_expend_ext_r12 table ****/

    -- PLSQL Table based on Extraction Table
    TYPE expend_ext_tbl IS TABLE OF xxpa_proj_expend_ext_r12%ROWTYPE INDEX BY BINARY_INTEGER;

    l_expend_ext_tbl expend_ext_tbl;
    l_err_record     NUMBER;

    -- Extraction Table cursor
    CURSOR ext_expend_cur IS
      SELECT xpee.*
        FROM xxpa_proj_expend_ext_r12 xpee
       WHERE xpee.leg_process_flag = g_validated
         AND NOT EXISTS
       (SELECT 1
                FROM xxpa_proj_expend_stg xpes
               WHERE xpes.interface_txn_id = xpee.interface_txn_id);

  BEGIN

    pov_ret_stats  := 'S';
    pov_err_msg    := NULL;
    g_total_count  := 0;
    g_failed_count := 0;

    debug_msg_p('STEP:2.1',
                'LOAD_EXPEND_DATA_P',
                'In Begin of Prc: LOAD_EXPEND_DATA_P');

    debug_msg_p('STEP:2.2',
                'LOAD_EXPEND_DATA_P',
                'Perform Bulk Insert of Extraction table into Conversion Table');

    -- Open Cursor
    OPEN ext_expend_cur;
    LOOP

      l_expend_ext_tbl.DELETE;

      FETCH ext_expend_cur BULK COLLECT
        INTO l_expend_ext_tbl LIMIT 1000; --limit size of Bulk Collect

      -- Get Total Count
      g_total_count := g_total_count + l_expend_ext_tbl.COUNT;

      EXIT WHEN l_expend_ext_tbl.COUNT = 0;

      BEGIN

        -- Bulk Insert into Conversion table
        FORALL indx IN 1 .. l_expend_ext_tbl.COUNT SAVE EXCEPTIONS
          INSERT INTO xxpa_proj_expend_stg
            (interface_txn_id,
             leg_source_system,
             leg_project_number,
             leg_acct_raw_cost,
             leg_gl_period_name,
             expenditure_item_date,
             expenditure_type,
             expenditure_ending_date,
             leg_operating_unit,
             operating_unit,
             leg_count_of_rows,
             quantity,
             uom,
             raw_cost_rate,
             leg_acct_currency_code,
             attribute_category,
             attribute1,
             attribute2,
             attribute3,
             attribute4,
             attribute5,
             transaction_source,
             orig_transaction_reference,
             batch_name,
             leg_exp_organization_name,
             exp_organization_name,
             gl_date,
             leg_billable_flag,
             batch_id,
             run_sequence_id,
             request_id,
             process_flag,
             error_type,
             creation_date,
             created_by,
             last_update_date,
             last_updated_by,
             last_update_login,
             program_application_id,
             program_id,
             program_update_date,
             leg_request_id,
             leg_seq_num,
             leg_process_flag)
          VALUES
            (l_expend_ext_tbl   (indx).interface_txn_id,
             l_expend_ext_tbl   (indx).leg_source_system,
             l_expend_ext_tbl   (indx).leg_project_number,
             l_expend_ext_tbl   (indx).leg_acct_raw_cost,
             l_expend_ext_tbl   (indx).leg_gl_period_name,
             l_expend_ext_tbl   (indx).expenditure_item_date,
             l_expend_ext_tbl   (indx).expenditure_type,
             l_expend_ext_tbl   (indx).expenditure_ending_date,
             l_expend_ext_tbl   (indx).leg_operating_unit,
             l_expend_ext_tbl   (indx).operating_unit,
             l_expend_ext_tbl   (indx).leg_count_of_rows,
             l_expend_ext_tbl   (indx).quantity,
             l_expend_ext_tbl   (indx).uom,
             l_expend_ext_tbl   (indx).raw_cost_rate,
             l_expend_ext_tbl   (indx).leg_acct_currency_code,
             l_expend_ext_tbl   (indx).attribute_category,
             l_expend_ext_tbl   (indx).attribute1,
             l_expend_ext_tbl   (indx).attribute2,
             l_expend_ext_tbl   (indx).attribute3,
             l_expend_ext_tbl   (indx).attribute4,
             l_expend_ext_tbl   (indx).attribute5,
             l_expend_ext_tbl   (indx).transaction_source,
             l_expend_ext_tbl   (indx).interface_txn_id -- orig trx reference
            ,
             l_expend_ext_tbl   (indx).batch_name,
             l_expend_ext_tbl   (indx).leg_exp_organization_name,
             l_expend_ext_tbl   (indx).exp_organization_name,
             l_expend_ext_tbl   (indx).gl_date,
             l_expend_ext_tbl   (indx).leg_billable_flag,
             l_expend_ext_tbl   (indx).batch_id,
             l_expend_ext_tbl   (indx).run_sequence_id,
             g_request_id -- Current Request Id
            ,
             g_new,
             l_expend_ext_tbl   (indx).error_type,
             SYSDATE -- Creation Date
            ,
             g_last_updated_by -- Created By
            ,
             SYSDATE -- Last Update Date
            ,
             g_last_updated_by -- Last Updated By
            ,
             g_last_update_login -- Last Update Login
            ,
             g_prog_appl_id -- Program Application Id
            ,
             g_conc_program_id -- Program Id
            ,
             SYSDATE -- Program Update Date
            ,
             l_expend_ext_tbl   (indx).leg_request_id,
             l_expend_ext_tbl   (indx).leg_seq_num,
             l_expend_ext_tbl   (indx).leg_process_flag);

      EXCEPTION
        WHEN OTHERS THEN
          FOR l_indx_exp IN 1 .. SQL%BULK_EXCEPTIONS.COUNT LOOP
            l_err_record := l_expend_ext_tbl(SQL%BULK_EXCEPTIONS(l_indx_exp).error_index)
                            .interface_txn_id;
            fnd_file.put_line(fnd_file.LOG,
                              'Interface Txn Id : ' || l_expend_ext_tbl(SQL%BULK_EXCEPTIONS(l_indx_exp).error_index)
                              .interface_txn_id);
            fnd_file.put_line(fnd_file.LOG,
                              'Error Message : ' ||
                              SQLERRM(-SQL%BULK_EXCEPTIONS(l_indx_exp)
                                      .error_code));

            -- Updating Leg_process_flag to 'E' for failed records
            UPDATE xxpa_proj_expend_ext_r12 xpee
               SET xpee.leg_process_flag  = g_error,
                   xpee.last_update_date  = SYSDATE,
                   xpee.last_updated_by   = g_last_updated_by,
                   xpee.last_update_login = g_last_update_login
             WHERE xpee.interface_txn_id = l_err_record
               AND xpee.leg_process_flag = g_validated;

            g_failed_count := g_failed_count + SQL%ROWCOUNT;

          END LOOP;
          pov_ret_stats := 'E';
          pov_err_msg   := 'Few/All Records failed to load from Extraction to Conversion Staging Table. Please check Log File for details.';
      END;

    END LOOP;
    CLOSE ext_expend_cur; -- Close Cursor

    COMMIT;

    -- Update Successful records in Extraction Table
    UPDATE xxpa_proj_expend_ext_r12 xpee
       SET xpee.leg_process_flag  = 'P',
           xpee.last_update_date  = SYSDATE,
           xpee.last_updated_by   = g_last_updated_by,
           xpee.last_update_login = g_last_update_login
     WHERE xpee.leg_process_flag = g_validated
       AND EXISTS
     (SELECT 1
              FROM xxpa_proj_expend_stg xpes
             WHERE xpes.interface_txn_id = xpee.interface_txn_id
               AND NVL(xpes.leg_seq_num, 1) = NVL(xpee.leg_seq_num, 1));

    COMMIT;

  EXCEPTION
    WHEN OTHERS THEN
      pov_ret_stats := 'E';
      pov_err_msg   := 'ERROR: XXPA_PROJ_EXPEND_CNV_PKG.LOAD_EXPEND_DATA_P->WHEN_OTHERS->' ||
                       SQLERRM;
      ROLLBACK;

  END load_expend_data_p;

  --
  -- =============================================================================
  -- Procedure: assign_batch_id_p
  -- =============================================================================
  -- This private procedure will assign Batch Id and Run Sequence Id to New records
  --
  -- =============================================================================
  --
  -- -----------------------------------------------------------------------------
  --  Called By: PROCESS_EXPENDITURES_P
  -- -----------------------------------------------------------------------------
  --
  --  Input Parameters : NONE
  --
  --  Output Parameters :
  --  pov_ret_stats     : Return Status of Procedure as 'S' or 'E'
  --  pov_err_msg       : Returns Error Message
  --
  --  Return            : Not applicable
  -- -----------------------------------------------------------------------------

  PROCEDURE assign_batch_id_p(pov_ret_stats OUT NOCOPY VARCHAR2,
                              pov_err_msg   OUT NOCOPY VARCHAR2) IS
    PRAGMA AUTONOMOUS_TRANSACTION;

  BEGIN
    pov_ret_stats := 'S';
    pov_err_msg   := NULL;

    debug_msg_p('STEP:4.1',
                'ASSIGN_BATCH_ID_P',
                'In Begin of Prc: ASSIGN_BATCH_ID_P');

    debug_msg_p('STEP:4.2',
                'ASSIGN_BATCH_ID_P',
                'Batch Id to assign: ' || g_new_batch_id);
    debug_msg_p('STEP:4.3',
                'ASSIGN_BATCH_ID_P',
                'Run Sequence Id to assign: ' || g_new_run_seq_id);

    -- Assigning Batch Id and Run Sequence Id to Conversion Staging Table
    UPDATE xxpa_proj_expend_stg xpes
       SET xpes.batch_id               = g_new_batch_id,
           xpes.run_sequence_id        = g_new_run_seq_id,
           xpes.process_flag           = g_new,
           xpes.request_id             = g_request_id,
           xpes.last_update_date       = SYSDATE,
           xpes.last_update_login      = g_last_update_login,
           xpes.last_updated_by        = g_last_updated_by,
           xpes.program_id             = g_conc_program_id,
           xpes.program_application_id = g_prog_appl_id,
           xpes.program_update_date    = SYSDATE
     WHERE xpes.batch_id IS NULL;

    COMMIT;

  EXCEPTION
    WHEN OTHERS THEN
      pov_ret_stats := 'E';
      pov_err_msg   := 'ERROR : XXPA_PROJ_EXPEND_CNV_PKG.ASSIGN_BATCH_ID_P->WHEN_OTHERS->' ||
                       SQLERRM;
      ROLLBACK;

  END assign_batch_id_p;

  --
  -- =============================================================================
  -- Procedure: log_errors_p
  -- =============================================================================
  -- This private procedure will add Validation/Conversion errors in Table Type
  -- and call add_error proc in Error Framework if Limit exceeds
  --
  -- =============================================================================
  --
  -- -----------------------------------------------------------------------------
  --  Called By: Multiple Procs
  -- -----------------------------------------------------------------------------
  --
  --  Input Parameters  :
  --  pi_err_rec        : Record Type Variable with Error Details
  --
  --  Output Parameters :
  --  pov_ret_stats     : Return Status of Procedure as 'S' or 'E'
  --  pov_err_msg       : Returns Error Message
  --
  --  Return            : Not applicable
  -- -----------------------------------------------------------------------------

  PROCEDURE log_errors_p(pov_ret_stats OUT NOCOPY VARCHAR2,
                         pov_err_msg   OUT NOCOPY VARCHAR2,
                         pi_err_rec    IN xxetn_common_error_pkg.g_source_rec_type) IS

  BEGIN
    pov_ret_stats := 'S';
    pov_err_msg   := NULL;

    debug_msg_p('STEP:50.1',
                'LOG_ERRORS_P',
                'In Begin of Prc: LOG_ERRORS_P');

    debug_msg_p('STEP:50.2',
                'LOG_ERRORS_P',
                'PLSQL Table Error Record Count: ' || g_err_cnt);

    debug_msg_p('STEP:50.3',
                'LOG_ERRORS_P',
                'Adding error to PLSQL table for Record id: ' ||
                pi_err_rec.interface_staging_id);

    -- Assigning record type values to current table record
    g_source_tab(g_err_cnt).interface_staging_id := pi_err_rec.interface_staging_id;
    g_source_tab(g_err_cnt).source_table := g_table_name;
    g_source_tab(g_err_cnt).source_column_name := pi_err_rec.source_column_name;
    g_source_tab(g_err_cnt).source_column_value := pi_err_rec.source_column_value;
    g_source_tab(g_err_cnt).error_type := pi_err_rec.error_type;
    g_source_tab(g_err_cnt).error_code := pi_err_rec.error_code;
    g_source_tab(g_err_cnt).error_message := pi_err_rec.error_message;

    IF g_err_cnt >= g_err_tab_limit THEN
      -- if Table Type Error Count exceeds limit
      g_err_cnt := 1;

      xxetn_common_error_pkg.add_error(pov_return_status => pov_ret_stats,
                                       pov_error_msg     => pov_err_msg,
                                       pi_source_tab     => g_source_tab);

      -- Flushing PLSQL Table
      g_source_tab.DELETE;

    ELSE
      g_err_cnt := g_err_cnt + 1; -- else increment Table Type Error Count
    END IF;

  EXCEPTION
    WHEN OTHERS THEN
      pov_ret_stats := 'E';
      pov_err_msg   := pov_err_msg || ' ~~ ' ||
                       'ERROR : XXPA_PROJ_EXPEND_CNV_PKG.LOG_ERRORS_P->WHEN_OTHERS->' ||
                       SQLERRM;
  END log_errors_p;

  --
  -- =============================================================================
  -- Procedure: pre_validate_expend_p
  -- =============================================================================
  -- This private procedure will perform pre-validations for Project Expenditures
  --
  -- =============================================================================
  --
  -- -----------------------------------------------------------------------------
  --  Called By: PROCESS_EXPENDITURES_P
  -- -----------------------------------------------------------------------------
  --
  --  Input Parameters  : NONE
  --
  --  Output Parameters :
  --  pov_ret_stats     : Return Status of Procedure as 'S' or 'E'
  --  pov_err_msg       : Returns Error Message
  --
  --  Return            : Not applicable
  -- -----------------------------------------------------------------------------

  PROCEDURE pre_validate_expend_p(pov_ret_stats OUT NOCOPY VARCHAR2,
                                  pov_err_msg   OUT NOCOPY VARCHAR2) IS
    l_ou_check     VARCHAR2(1);
    l_task_check   VARCHAR2(1);
    l_expend_check VARCHAR2(1);
    l_source_check VARCHAR2(1);

  BEGIN
    pov_ret_stats := 'S';
    pov_err_msg   := NULL;

    debug_msg_p('STEP:3.1',
                'PRE_VALIDATE_EXPEND_P',
                'In Begin of Prc: PRE_VALIDATE_EXPEND_P');

    /**
       debug_msg_p ('STEP:3.2',
                    'PRE_VALIDATE_EXPEND_P',
                    'Checking 11i-R12 OU Mapping Lookup'
                   );

       -- Check if 11i-R12 OU Mapping Lookup is defined
       BEGIN
          SELECT 1
          INTO l_ou_check
          FROM fnd_lookup_types fl
          WHERE fl.lookup_type = g_ou_lookup;
       EXCEPTION
          WHEN NO_DATA_FOUND THEN
             pov_ret_stats := 'E';
             pov_err_msg := '11i-R12 Operating Unit Mapping Lookup ETN_COMMON_OU_MAP is not setup';
          WHEN OTHERS THEN
             pov_ret_stats := 'E';
             pov_err_msg := 'SQL Error while checking OU Lookup. ' || SQLERRM;
             debug_msg_p ('STEP:3.3',
                          'PRE_VALIDATE_EXPEND_P',
                          'SQL Error while checking OU Lookup. ' || SQLERRM
                         );
       END;
    **/

    debug_msg_p('STEP:3.4',
                'PRE_VALIDATE_EXPEND_P',
                'Checking 11i Project Type and R12 Task Mapping Lookup');

    -- Check if 11i Project Type - R12 Task Mapping Lookup is defined
    BEGIN
      SELECT 1
        INTO l_task_check
        FROM fnd_lookup_types fl
       WHERE fl.lookup_type = g_task_lookup;
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        pov_ret_stats := 'E';
        pov_err_msg   := pov_err_msg || ' ~~ ' ||
                         '11i Project Type - R12 Task Mapping Lookup XXETN_PA_TASK_MAPPING is not setup';
      WHEN OTHERS THEN
        pov_ret_stats := 'E';
        pov_err_msg   := pov_err_msg || ' ~~ ' ||
                         'SQL Error while checking Task Mapping Lookup. ' ||
                         SQLERRM;

        debug_msg_p('STEP:3.5',
                    'PRE_VALIDATE_EXPEND_P',
                    'SQL Error while checking Task Mapping Lookup. ' ||
                    SQLERRM);
    END;

    debug_msg_p('STEP:3.6',
                'PRE_VALIDATE_EXPEND_P',
                'Checking Expenditure Type "CAPITAL MATERIAL" setup');

    -- Check if "CAPITAL MATERIAL" is setup as Expenditure Type
    BEGIN
      SELECT 1
        INTO l_expend_check
        FROM pa_expenditure_types pet
       WHERE pet.expenditure_type = g_expend_type
         AND NVL(pet.end_date_active, TRUNC(SYSDATE + 1)) > TRUNC(SYSDATE);
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        pov_ret_stats := 'E';
        pov_err_msg   := pov_err_msg || ' ~~ ' ||
                         'Expenditure Type CAPITAL MATERIAL is not setup';
      WHEN OTHERS THEN
        pov_ret_stats := 'E';
        pov_err_msg   := pov_err_msg || ' ~~ ' ||
                         'SQL Error while checking Expenditure Type ' ||
                         SQLERRM;

        debug_msg_p('STEP:3.7',
                    'PRE_VALIDATE_EXPEND_P',
                    'SQL Error while checking Expenditure Type ' || SQLERRM);
    END;

    debug_msg_p('STEP:3.8',
                'PRE_VALIDATE_EXPEND_P',
                'Checking Transaction Source "CONVERT" setup');

    -- Check if CONVERT is setup as Transaction Source
    BEGIN
      SELECT 1
        INTO l_source_check
        FROM pa_transaction_sources pts
       WHERE pts.transaction_source = g_trx_source
         AND NVL(pts.end_date_active, TRUNC(SYSDATE + 1)) > TRUNC(SYSDATE);
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        pov_ret_stats := 'E';
        pov_err_msg   := pov_err_msg || ' ~~ ' ||
                         'Transaction Source CONVERT is not setup';
      WHEN OTHERS THEN
        pov_ret_stats := 'E';
        pov_err_msg   := pov_err_msg || ' ~~ ' ||
                         'SQL Error while checking Transaction Source' ||
                         SQLERRM;

        debug_msg_p('STEP:3.9',
                    'PRE_VALIDATE_EXPEND_P',
                    'SQL Error while checking Transaction Source' ||
                    SQLERRM);
    END;

  EXCEPTION
    WHEN OTHERS THEN
      pov_ret_stats := 'E';
      pov_err_msg   := pov_err_msg || ' ~~ ' ||
                       'ERROR : XXPA_PROJ_EXPEND_CNV_PKG.PRE_VALIDATE_EXPEND_P->WHEN_OTHERS->' ||
                       SQLERRM;
  END pre_validate_expend_p;

  --
  -- =============================================================================
  -- Function: mandatory_check_f
  -- =============================================================================
  -- This private procedure will perform mandatory check for Not NULL fields
  --
  -- =============================================================================
  --
  -- -----------------------------------------------------------------------------
  --  Called By: VALIDATE_EXPEND_P
  -- -----------------------------------------------------------------------------
  --
  --  Input Parameters  : piv_field     Field to be checked for Mandatory value
  --                      pi_source_rec Error Table Record Type
  --
  --  Output Parameters : NONE
  --
  --  Return            : BOOLEAN
  -- -----------------------------------------------------------------------------

  FUNCTION mandatory_check_f(piv_field     IN VARCHAR2,
                             pi_source_rec IN xxetn_common_error_pkg.g_source_rec_type)
    RETURN BOOLEAN

   IS
    l_log_ret_stats VARCHAR2(10);
    l_log_err_msg   VARCHAR2(2000);

  BEGIN

    IF TRIM(piv_field) IS NULL THEN
      log_errors_p(pov_ret_stats => l_log_ret_stats,
                   pov_err_msg   => l_log_err_msg,
                   pi_err_rec    => pi_source_rec);

      IF l_log_ret_stats <> 'S' THEN
        print_log_message_p('Not able to insert error details in Common Error Table. ' ||
                            'Error: ' || l_log_err_msg);
      END IF;

      RETURN TRUE;

    ELSE
      RETURN FALSE;
    END IF;

  EXCEPTION
    WHEN OTHERS THEN
      print_log_message_p('ERROR : XXPA_PROJ_EXPEND_CNV_PKG.MANDATORY_CHECK_F->WHEN_OTHERS->' ||
                          SQLERRM);
      RETURN TRUE;
  END mandatory_check_f;

  --
  -- =============================================================================
  -- Procedure: derive_r12_ou_p
  -- =============================================================================
  -- This private procedure will derive R12 OU based on R12 Plant derived from Project
  --
  -- =============================================================================
  --
  -- -----------------------------------------------------------------------------
  --  Called By: VALIDATE_EXPEND_P
  -- -----------------------------------------------------------------------------
  --
  --  Input Parameters  :
  --  piv_r12_plant     : R12 Plant# derived from Project Number
  --
  --  Output Parameters :
  --  pov_ou_name       : Returns R12 Operating Unit Name
  --  pov_status        : Returns Status of Validation as 'S' or 'E'
  --  pov_error_code    : Returns Error Code
  --  pov_error_message : Returns Error Message
  --
  --  Return            : Not applicable
  -- -----------------------------------------------------------------------------

  PROCEDURE derive_r12_ou_p(pov_ou_name       OUT NOCOPY VARCHAR2,
                            pov_status        OUT NOCOPY VARCHAR2,
                            pov_error_code    OUT NOCOPY VARCHAR2,
                            pov_error_message OUT NOCOPY VARCHAR2,
                            piv_r12_plant     IN VARCHAR2) IS
    l_oper_unit xxetn_map_unit.operating_unit%TYPE := NULL;
    l_rec       xxetn_map_util.g_input_rec;

  BEGIN

    pov_ou_name       := NULL;
    pov_status        := 'S';
    pov_error_code    := NULL;
    pov_error_message := NULL;

    -- Assigning R12 Site/Plant to API variable
    l_rec.site := piv_r12_plant;

    --R12 OU
    l_oper_unit := xxetn_map_util.get_value(l_rec).operating_unit;

    debug_msg_p('STEP:6.10.1',
                'DERIVE_R12_OU_P',
                'Derived R12 Operating Unit: ' || l_oper_unit);

    IF l_oper_unit IS NULL THEN
      pov_ou_name       := NULL;
      pov_status        := g_error;
      pov_error_code    := 'R12_OU_DERIVATION_FAILED';
      pov_error_message := 'R12 OU does not exist in ETN Common Map Utility table for given Plant/Site: ' ||
                           piv_r12_plant;
    ELSE
      pov_ou_name := l_oper_unit;
    END IF;

  EXCEPTION
    WHEN OTHERS THEN
      print_log_message_p('ERROR : XXPA_PROJ_EXPEND_CNV_PKG.DERIVE_R12_OU_P->WHEN_OTHERS->' ||
                          SQLERRM);
  END derive_r12_ou_p;

  --
  -- =============================================================================
  -- Procedure: derive_org_id_p
  -- =============================================================================
  -- This private procedure will derive Org ID based on R12 Operating Unit
  --
  -- =============================================================================
  --
  -- -----------------------------------------------------------------------------
  --  Called By: VALIDATE_EXPEND_P
  -- -----------------------------------------------------------------------------
  --
  --  Input Parameters  :
  --  piv_ou_name       : R12 Operating Unit Name
  --
  --  Output Parameters :
  --  pon_org_id        : Returns R12 Organization Id
  --  pov_status        : Returns Status of Validation as 'S' or 'E'
  --  pov_error_code    : Returns Error Code
  --  pov_error_message : Returns Error Message
  --
  --  Return            : Not applicable
  -- -----------------------------------------------------------------------------

  PROCEDURE derive_org_id_p(pon_org_id        OUT NOCOPY NUMBER,
                            pov_status        OUT NOCOPY VARCHAR2,
                            pov_error_code    OUT NOCOPY VARCHAR2,
                            pov_error_message OUT NOCOPY VARCHAR2,
                            piv_ou_name       IN VARCHAR2) IS

  BEGIN

    pon_org_id        := NULL;
    pov_status        := 'S';
    pov_error_code    := NULL;
    pov_error_message := NULL;

    -- Deriving Org Id based on R12 OU
    BEGIN
      SELECT hou.organization_id
        INTO pon_org_id
        FROM hr_operating_units hou
       WHERE UPPER(hou.NAME) = UPPER(piv_ou_name)
         AND TRUNC(NVL(hou.date_to, SYSDATE)) >= TRUNC(SYSDATE);
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        pov_status        := g_error;
        pon_org_id        := -1;
        pov_error_code    := 'INVALID_R12_OU';
        pov_error_message := 'No Org_id exist for derived R12 Operating Unit';
      WHEN TOO_MANY_ROWS THEN
        pov_status        := g_error;
        pon_org_id        := -1;
        pov_error_code    := 'TOO_MANY_R12_OU';
        pov_error_message := 'Too Many Org_id exist for derived R12 Operating Unit';
      WHEN OTHERS THEN
        pov_status        := g_error;
        pon_org_id        := -1;
        pov_error_code    := 'SQL_ERR_R12_OU';
        pov_error_message := 'SQL Error occured while deriving Org_id for R12 Operating Unit. ERROR: ' ||
                             SQLERRM;
    END;

  EXCEPTION
    WHEN OTHERS THEN
      print_log_message_p('ERROR : XXPA_PROJ_EXPEND_CNV_PKG.DERIVE_ORG_ID_P->WHEN_OTHERS->' ||
                          SQLERRM);
  END derive_org_id_p;

  --
  -- =============================================================================
  -- Procedure: validate_proj_details_p
  -- =============================================================================
  -- This private procedure will validate Project Details
  --
  -- =============================================================================
  --
  -- -----------------------------------------------------------------------------
  --  Called By: VALIDATE_EXPEND_P
  -- -----------------------------------------------------------------------------
  --
  --  Input Parameters  :
  --  piv_leg_proj_num  : Legacy Project Number
  --
  --  Output Parameters :
  --  pon_project_id    : Returns R12 Project Id
  --  pod_start_date    : Returns Project Start Date
  --  pod_end_date      : Returns Project End Date
  --  pov_class_code    : Returns Project Classification (11i Project Type)
  --  pov_allow_charges : Returns Allow Cross Charges Value
  --  pon_org_id        : Returns Project Org Id
  --  pov_status        : Returns Status of Validation as 'S' or 'E'
  --  pov_error_code    : Returns Error Code
  --  pov_error_message : Returns Error Message
  --
  --  Return            : Not applicable
  -- -----------------------------------------------------------------------------

  PROCEDURE validate_proj_details_p(pon_project_id    OUT NOCOPY NUMBER,
                                    pod_start_date    OUT NOCOPY DATE,
                                    pod_end_date      OUT NOCOPY DATE,
                                    pov_class_code    OUT NOCOPY VARCHAR2,
                                    pov_allow_charges OUT NOCOPY VARCHAR2,
                                    pon_org_id        OUT NOCOPY NUMBER,
                                    pov_status        OUT NOCOPY VARCHAR2,
                                    pov_error_code    OUT NOCOPY VARCHAR2,
                                    pov_error_message OUT NOCOPY VARCHAR2,
                                    piv_leg_proj_num  IN VARCHAR2) IS

  BEGIN

    pov_status        := 'S';
    pov_error_code    := NULL;
    pov_error_message := NULL;

    -- Validate Project Details
    BEGIN
      SELECT ppa.project_id,
             ppa.start_date,
             ppa.completion_date,
             ppc.class_code,
             ppa.allow_cross_charge_flag,
             ppa.org_id
        INTO pon_project_id,
             pod_start_date,
             pod_end_date,
             pov_class_code,
             pov_allow_charges,
             pon_org_id
        FROM pa_projects_all ppa, pa_project_classes ppc
       WHERE ppa.enabled_flag = 'Y'
         AND ppa.template_flag = 'N'
       --  AND UPPER(ppa.segment1) = UPPER(piv_leg_proj_num)  v5.0
          AND (ppa.segment1) = (piv_leg_proj_num)         ----removed upper clause as per defect#4691
          AND ppa.project_id = ppc.project_id
         AND ppc.object_type = 'PA_PROJECTS'
         AND ppc.object_id = ppa.project_id;



    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        pov_status        := g_error;
        pon_project_id    := -1;
        pod_start_date    := NULL;
        pod_end_date      := NULL;
        pov_class_code    := NULL;
        pov_allow_charges := NULL;
        pon_org_id        := -1;
        pov_error_code    := 'INVALID_LEG_PROJECT_NUMBER';
        pov_error_message := 'Project Number is invalid or does not have a Project Classification';
      WHEN TOO_MANY_ROWS THEN
        pov_status        := g_error;
        pon_project_id    := -1;
        pod_start_date    := NULL;
        pod_end_date      := NULL;
        pov_class_code    := NULL;
        pov_allow_charges := NULL;
        pon_org_id        := -1;
        pov_error_code    := 'TOO_MANY_LEG_PROJECT_NUMBER';
        pov_error_message := 'Too Many Project Numbers exist';
      WHEN OTHERS THEN
        pov_status        := g_error;
        pon_project_id    := -1;
        pod_start_date    := NULL;
        pod_end_date      := NULL;
        pov_class_code    := NULL;
        pov_allow_charges := NULL;
        pon_org_id        := -1;
        pov_error_code    := 'SQL_ERR_LEG_PROJECT_NUMBER';
        pov_error_message := 'SQL Error occured while validating Project Details. ERROR: ' ||
                             SQLERRM;
    END;

  EXCEPTION
    WHEN OTHERS THEN
      print_log_message_p('ERROR : XXPA_PROJ_EXPEND_CNV_PKG.VALIDATE_PROJ_DETAILS_P->WHEN_OTHERS->' ||
                          SQLERRM);
  END validate_proj_details_p;

  --
  -- =============================================================================
  -- Procedure: derive_r12_task_p
  -- =============================================================================
  -- This private procedure will derive R12 Task Details from Project Classification
  --
  -- =============================================================================
  --
  -- -----------------------------------------------------------------------------
  --  Called By: VALIDATE_EXPEND_P
  -- -----------------------------------------------------------------------------
  --
  --  Input Parameters  :
  --  piv_class_code    : R12 Project Classification
  --
  --  Output Parameters :
  --  pov_task_number   : Returns R12 Task Number
  --  pov_status        : Returns Status of Validation as 'S' or 'E'
  --  pov_error_code    : Returns Error Code
  --  pov_error_message : Returns Error Message
  --
  --  Return            : Not applicable
  -- -----------------------------------------------------------------------------

  PROCEDURE derive_r12_task_p(pov_task_number   OUT NOCOPY VARCHAR2,
                              pov_status        OUT NOCOPY VARCHAR2,
                              pov_error_code    OUT NOCOPY VARCHAR2,
                              pov_error_message OUT NOCOPY VARCHAR2,
                              piv_class_code    IN VARCHAR2) IS

  BEGIN

    pov_status        := 'S';
    pov_error_code    := NULL;
    pov_error_message := NULL;

    -- Derive R12 Task Number from Project Classification
    BEGIN
      SELECT flv.tag
        INTO pov_task_number
        FROM fnd_lookup_values_vl flv
       WHERE flv.lookup_type = g_task_lookup
         AND TRIM(flv.meaning) = TRIM(piv_class_code)
         AND TRUNC(NVL(flv.end_date_active, SYSDATE)) >= TRUNC(SYSDATE)
         AND flv.enabled_flag = 'Y';
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        pov_status        := g_error;
        pov_task_number   := NULL;
        pov_error_code    := 'NO_MAPPING_R12_TASK';
        pov_error_message := 'R12 Task does not exist for Project Classification: ' ||
                             piv_class_code ||
                             ' in XXETN_PA_TASK_MAPPING Lookup';
      WHEN TOO_MANY_ROWS THEN
        pov_status        := g_error;
        pov_task_number   := NULL;
        pov_error_code    := 'TOO_MANY_R12_TASK';
        pov_error_message := 'Too Many R12 Task exist for Project Classification: ' ||
                             piv_class_code ||
                             ' in XXETN_PA_TASK_MAPPING Lookup';
      WHEN OTHERS THEN
        pov_status        := g_error;
        pov_task_number   := NULL;
        pov_error_code    := 'SQL_ERR_R12_TASK';
        pov_error_message := 'SQL Error occured while mapping Project Classification: ' ||
                             piv_class_code || ' to R12 Task. Error:' ||
                             SQLERRM;
    END;

  EXCEPTION
    WHEN OTHERS THEN
      print_log_message_p('ERROR: XXPA_PROJ_EXPEND_CNV_PKG.DERIVE_R12_TASK_P->WHEN_OTHERS->' ||
                          SQLERRM);
  END derive_r12_task_p;

  --
  -- =============================================================================
  -- Procedure: derive_task_id_p
  -- =============================================================================
  -- This private procedure will derive Task id for R12 Task Number
  --
  -- =============================================================================
  --
  -- -----------------------------------------------------------------------------
  --  Called By: VALIDATE_EXPEND_P
  -- -----------------------------------------------------------------------------
  --
  --  Input Parameters  :
  --  pin_project_id    : Project Id
  --  piv_task_number   : R12 Task Number
  --
  --  Output Parameters :
  --  pon_task_id       : Returns R12 Task Id
  --  pod_task_start_date : Returns Task Start Date
  --  pod_task_end_date   : Returns Task End Date
  --  pov_status        : Return Status of Validation as 'S' or 'E'
  --  pov_error_code    : Returns Error Code
  --  pov_error_message : Returns Error Message
  --
  --  Return            : Not applicable
  -- -----------------------------------------------------------------------------

  PROCEDURE derive_task_id_p(pon_task_id         OUT NOCOPY NUMBER,
                             pod_task_start_date OUT NOCOPY DATE,
                             pod_task_end_date   OUT NOCOPY DATE,
                             pov_status          OUT NOCOPY VARCHAR2,
                             pov_error_code      OUT NOCOPY VARCHAR2,
                             pov_error_message   OUT NOCOPY VARCHAR2,
                             pin_project_id      IN NUMBER,
                             piv_task_number     IN VARCHAR2) IS

  BEGIN

    pon_task_id       := NULL;
    pov_status        := 'S';
    pov_error_code    := NULL;
    pov_error_message := NULL;

    -- Deriving Task Id based on R12 Task Number
    BEGIN
      SELECT pt.task_id, pt.start_date, pt.completion_date
        INTO pon_task_id, pod_task_start_date, pod_task_end_date
        FROM pa_tasks pt
       WHERE UPPER(TRIM(pt.task_number)) = UPPER(TRIM(piv_task_number))
         AND pt.project_id = pin_project_id
         AND pt.chargeable_flag = 'Y';

    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        pov_status          := g_error;
        pon_task_id         := -1;
        pod_task_start_date := NULL;
        pod_task_end_date   := NULL;
        pov_error_code      := 'INVALID_R12_TASK_NUMBER';
        pov_error_message   := 'Task Id does not exist or not chargeable for derived R12 Task Number';
      WHEN TOO_MANY_ROWS THEN
        pov_status          := g_error;
        pon_task_id         := -1;
        pod_task_start_date := NULL;
        pod_task_end_date   := NULL;
        pov_error_code      := 'TOO_MANY_R12_TASK_NUMBER';
        pov_error_message   := 'Too Many Task Ids exist for derived R12 Task Number';
      WHEN OTHERS THEN
        pov_status          := g_error;
        pon_task_id         := -1;
        pod_task_start_date := NULL;
        pod_task_end_date   := NULL;
        pov_error_code      := 'SQL_ERR_TASK_ID';
        pov_error_message   := 'SQL Error occured while deriving Task Id for R12 Task Number. ERROR: ' ||
                               SQLERRM;
    END;

  EXCEPTION
    WHEN OTHERS THEN
      print_log_message_p('ERROR: XXPA_PROJ_EXPEND_CNV_PKG.DERIVE_TASK_ID_P->WHEN_OTHERS->' ||
                          SQLERRM);
  END derive_task_id_p;

  --
  -- =============================================================================
  -- Procedure: derive_item_date_p
  -- =============================================================================
  -- This private procedure will derive Expenditure item Date based on GL Period
  --
  -- =============================================================================
  --
  -- -----------------------------------------------------------------------------
  --  Called By: VALIDATE_EXPEND_P
  -- -----------------------------------------------------------------------------
  --
  --  Input Parameters  :
  --  pin_org_id        : Org Id
  --  piv_period_name   : GL Period Name
  --
  --  Output Parameters :
  --  pod_exp_item_date : Returns Expenditure Item Date
  --  pov_status        : Return Status of Validation as 'S' or 'E'
  --  pov_error_code    : Returns Error Code
  --  pov_error_message : Returns Error Message
  --
  --  Return            : Not applicable
  -- -----------------------------------------------------------------------------

  PROCEDURE derive_item_date_p(pod_exp_item_date    OUT NOCOPY DATE,
                               pov_status           OUT NOCOPY VARCHAR2,
                               pov_error_code       OUT NOCOPY VARCHAR2,
                               pov_error_message    OUT NOCOPY VARCHAR2,
                               pin_org_id           IN NUMBER,
                               piv_period_name      IN VARCHAR2,
                               piv_project_end_date IN VARCHAR2) IS

    l_last_day         VARCHAR2(20);
    l_exp_item_date    DATE;
    l_project_end_date DATE;
    l_pass_date        DATE;

  BEGIN

    l_last_day         := NULL;
    l_pass_date        := NULL;
    l_exp_item_date    := NULL;
    pod_exp_item_date  := NULL;
    pov_status         := 'S';
    pov_error_code     := NULL;
    pov_error_message  := NULL;
    l_project_end_date := TO_DATE(piv_project_end_date, 'DD-MON-RRRR');

    -- Fetch last day of Expenditure Week based on Operating Unit
    BEGIN
      SELECT DECODE(pia.exp_cycle_start_day_code - 1,
                    0,
                    'SATURDAY',
                    1,
                    'SUNDAY',
                    2,
                    'MONDAY',
                    3,
                    'TUESDAY',
                    4,
                    'WEDNESDAY',
                    5,
                    'THURSDAY',
                    6,
                    'FRIDAY')
        INTO l_last_day
        FROM pa_implementations_all pia
       WHERE pia.org_id = pin_org_id;

    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        pov_status        := g_error;
        pod_exp_item_date := NULL;
        pov_error_code    := 'EXPEND_WEEK_CYCLE_NOT_SET';
        pov_error_message := 'Task Id does not exist or not chargeable for derived R12 Task Number';
      WHEN OTHERS THEN
        pov_status        := g_error;
        pod_exp_item_date := NULL;
        pov_error_code    := 'SQL_ERR_EXPEND_WEEK_CYCLE';
        pov_error_message := 'SQL Error occured while fetching last day of Expenditure Week. ERROR: ' ||
                             SQLERRM;
    END;

    -- Derive Expenditure Item Date based on GL Period
    IF pov_status = 'S' AND l_last_day IS NOT NULL THEN

      BEGIN
        SELECT NEXT_DAY(LAST_DAY(TO_DATE('01-' || piv_period_name,
                                         'DD-MON-RRRR')) - 7,
                        l_last_day)
          INTO l_exp_item_date
          FROM dual;
      EXCEPTION
        WHEN OTHERS THEN
          pov_status        := g_error;
          pod_exp_item_date := NULL;
          pov_error_code    := 'SQL_ERR_EXPEND_ITEM_DATE';
          pov_error_message := 'SQL Error occured while deriving Expenditure Item Date from GL Period. ERROR: ' ||
                               SQLERRM;
      END;
    END IF;
        -----CHAGES DONE FOR THE PMC 332169
    IF pov_status = 'S' AND l_exp_item_date IS NOT NULL THEN
      IF l_exp_item_date > l_project_end_date THEN

       /* print_log_message_p('Exp date is greater than project end date ');
         print_log_message_p('exp_item_date - '||l_exp_item_date);
         print_log_message_p('project_end_date - '||l_project_end_date);*/
        BEGIN
          -------------------------------------------
         
----------------Commented as per version v6.0---------------------
         -- pod_exp_item_date := l_exp_item_date; -- added by Tushar as per version V6.0
          pov_status        := g_error;
          pod_exp_item_date := NULL;
          pov_error_code    := 'EXPEND_ITEM_DATE_ERR';
          pov_error_message := 'Error occured while deriving Expenditure Item Date ,Expenditure_Item_Date is greater than Project_End_Date  ' ||
                               SQLERRM;
        /*  SELECT ADD_MONTHS(l_project_end_date, -1)
            INTO l_pass_date
            FROM DUAL;


          SELECT NEXT_DAY(LAST_DAY(TO_DATE(l_pass_date, 'DD/MM/RRRR')) -
                          7 * LEVEL,
                          'SATURDAY')
            INTO pod_exp_item_date
            FROM DUAL
          CONNECT BY LEVEL <= 1;*/

----------------Commented as per version v6.0---------------------
        EXCEPTION
          WHEN OTHERS THEN
            pov_status        := g_error;
            pod_exp_item_date := NULL;
            pov_error_code    := 'SQL_ERR_EXPEND_ITEM_DATE';
            pov_error_message := 'SQL Error occured while deriving Expenditure Item Date from GL Period. ERROR: ' ||
                                 SQLERRM;

        END;
      ELSE
        pod_exp_item_date := l_exp_item_date;
      END IF;
    END IF;
  EXCEPTION
    WHEN OTHERS THEN
      print_log_message_p('ERROR: XXPA_PROJ_EXPEND_CNV_PKG.DERIVE_ITEM_DATE_P->WHEN_OTHERS->' ||
                          SQLERRM);
  END derive_item_date_p;

  --
  -- =============================================================================
  -- Procedure: validate_currency_p
  -- =============================================================================
  -- This private procedure will validate Currency Code
  --
  -- =============================================================================
  --
  -- -----------------------------------------------------------------------------
  --  Called By: VALIDATE_EXPEND_P
  -- -----------------------------------------------------------------------------
  --
  --  Input Parameters  :
  --  piv_currency_code : Acct Currency Code
  --
  --  Output Parameters :
  --  pov_status        : Return Status of Validation as 'S' or 'E'
  --  pov_error_code    : Returns Error Code
  --  pov_error_message : Returns Error Message
  --
  --  Return            : Not applicable
  -- -----------------------------------------------------------------------------

  PROCEDURE validate_currency_p(pov_status        OUT NOCOPY VARCHAR2,
                                pov_error_code    OUT NOCOPY VARCHAR2,
                                pov_error_message OUT NOCOPY VARCHAR2,
                                piv_currency_code IN VARCHAR2) IS

    l_curr VARCHAR2(1);

  BEGIN

    pov_status        := 'S';
    pov_error_code    := NULL;
    pov_error_message := NULL;

    -- Validate Acct Currency Code
    BEGIN
      SELECT 1
        INTO l_curr
        FROM fnd_currencies fc
       WHERE fc.currency_code = piv_currency_code
         AND fc.enabled_flag = 'Y'
         AND fc.currency_flag = 'Y';

    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        pov_status        := g_error;
        pov_error_code    := 'INVALID_LEG_ACCT_CURRENCY_CODE';
        pov_error_message := 'Acct Currency Code is Invalid';
      WHEN TOO_MANY_ROWS THEN
        pov_status        := g_error;
        pov_error_code    := 'TOO_MANY_LEG_ACCT_CURRENCY_CODE';
        pov_error_message := 'Too Many currencies exist for given Acct Currency Code';
      WHEN OTHERS THEN
        pov_status        := g_error;
        pov_error_code    := 'SQL_ERR_LEG_ACCT_CURRENCY_CODE';
        pov_error_message := 'SQL Error occured while validating Acct Currency Code. ERROR: ' ||
                             SQLERRM;
    END;

  EXCEPTION
    WHEN OTHERS THEN
      print_log_message_p('ERROR: XXPA_PROJ_EXPEND_CNV_PKG.VALIDATE_CURRENCY_P->WHEN_OTHERS->' ||
                          SQLERRM);
  END validate_currency_p;

  --
  -- =============================================================================
  -- Procedure: validate_exp_org_p
  -- =============================================================================
  -- This private procedure will validate Expenditure Organization
  --
  -- =============================================================================
  --
  -- -----------------------------------------------------------------------------
  --  Called By: VALIDATE_EXPEND_P
  -- -----------------------------------------------------------------------------
  --
  --  Input Parameters  :
  --  piv_exp_org       : Expenditure Organization Name
  --
  --  Output Parameters :
  --  pon_exp_org_id    : Expenditure Organization Id
  --  pov_status        : Return Status of Validation as 'S' or 'E'
  --  pov_error_code    : Returns Error Code
  --  pov_error_message : Returns Error Message
  --
  --  Return            : Not applicable
  -- -----------------------------------------------------------------------------

  PROCEDURE validate_exp_org_p(pon_exp_org_id    OUT NOCOPY NUMBER,
                               pon_exp_name      OUT NOCOPY VARCHAR2,
                               pov_status        OUT NOCOPY VARCHAR2,
                               pov_error_code    OUT NOCOPY VARCHAR2,
                               pov_error_message OUT NOCOPY VARCHAR2,
                               piv_exp_org       IN VARCHAR2) IS

  BEGIN

    pon_exp_org_id    := NULL;
    pon_exp_name      := NULL;
    pov_status        := 'S';
    pov_error_code    := NULL;
    pov_error_message := NULL;

    -- Validate Expenditure Organization
    BEGIN
      SELECT haou.organization_id, haou.name --changes as per defect 1821
        INTO pon_exp_org_id, pon_exp_name
        FROM hr_all_organization_units haou
      --  WHERE haou.name = piv_exp_org  --changed as per defect 1740
       WHERE SUBSTR(haou.NAME, 1, 7) = SUBSTR(piv_exp_org, 1, 7)
         AND TRUNC(SYSDATE) BETWEEN TRUNC(DATE_FROM) AND
             TRUNC(NVL(DATE_TO, SYSDATE + 1));

    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        pon_exp_org_id    := -1;
        pov_status        := g_error;
        pov_error_code    := 'INVALID_LEG_EXP_ORGANIZATION_NAME';
        pov_error_message := 'Expenditure Organization Name is Invalid';
      WHEN TOO_MANY_ROWS THEN
        pon_exp_org_id    := -1;
        pov_status        := g_error;
        pov_error_code    := 'TOO_MANY_LEG_EXP_ORGANIZATION_NAME';
        pov_error_message := 'Too Many orgs exist for given Expenditure Organization';
      WHEN OTHERS THEN
        pon_exp_org_id    := -1;
        pov_status        := g_error;
        pov_error_code    := 'SQL_ERR_LEG_EXP_ORGANIZATION_NAME';
        pov_error_message := 'SQL Error occured while validating Expenditure Organization. ERROR: ' ||
                             SQLERRM;
    END;

  EXCEPTION
    WHEN OTHERS THEN
      print_log_message_p('ERROR: XXPA_PROJ_EXPEND_CNV_PKG.VALIDATE_EXP_ORG_P->WHEN_OTHERS->' ||
                          SQLERRM);
  END validate_exp_org_p;

  --
  -- =============================================================================
  -- Procedure: validate_expend_p
  -- =============================================================================
  -- This private procedure will perform custom validations on Project Expenditures
  --
  -- =============================================================================
  --
  -- -----------------------------------------------------------------------------
  --  Called By: PROCESS_EXPENDITURES_P
  -- -----------------------------------------------------------------------------
  --
  --  Input Parameters  : NONE
  --
  --  Output Parameters :
  --  pov_ret_stats     : Returns Status of Procedure as 'S' or 'E'
  --  pov_err_msg       : Returns Error Message
  --
  --  Return            : Not applicable
  -- -----------------------------------------------------------------------------

  PROCEDURE validate_expend_p(pov_ret_stats OUT NOCOPY VARCHAR2,
                              pov_err_msg   OUT NOCOPY VARCHAR2) IS
    l_status    xxpa_proj_expend_stg.process_flag%TYPE;
    l_count     NUMBER := 0;
    l_error_msg VARCHAR2(2000);

    l_log_ret_stats VARCHAR2(10);
    l_log_err_msg   VARCHAR2(2000);
    l_api_stats     VARCHAR2(10);
    l_last_day      VARCHAR2(20);
    l_site          VARCHAR2(10);

    -- Error Table Record Type
    source_rec xxetn_common_error_pkg.g_source_rec_type;

    -- Staging Table Cursor to Process Records
    CURSOR validate_expend_cur IS
      SELECT xpes.interface_txn_id,
             xpes.leg_source_system,
             xpes.leg_operating_unit,
             xpes.operating_unit,
             xpes.org_id,
             xpes.leg_project_number,
             xpes.project_id,
             xpes.leg_acct_currency_code,
             xpes.leg_gl_period_name,
             xpes.leg_acct_raw_cost,
             xpes.leg_exp_organization_name,
             xpes.exp_organization_name,
             xpes.leg_billable_flag,
             xpes.project_start_date,
             xpes.project_end_date,
             xpes.allow_cross_charges,
             xpes.task_number,
             xpes.task_id,
             xpes.task_start_date,
             xpes.task_end_date,
             xpes.exp_organization_id,
             xpes.expenditure_item_date,
             xpes.expenditure_ending_date,
             xpes.project_class_code,
             xpes.project_org_id,
             xpes.batch_id,
             xpes.run_sequence_id,
             xpes.request_id,
             xpes.process_flag,
             xpes.error_type
        FROM xxpa_proj_expend_stg xpes
       WHERE xpes.process_flag IN (g_new_rec, g_err_rec, g_val_rec)
         AND xpes.batch_id = NVL(g_new_batch_id, g_batch_id)
       ORDER BY xpes.interface_txn_id;

  BEGIN

    pov_ret_stats  := 'S';
    pov_err_msg    := NULL;
    g_total_count  := 0;
    g_failed_count := 0;

    debug_msg_p('STEP:6.1',
                'VALIDATE_EXPEND_P',
                'In Begin of Prc: VALIDATE_EXPEND_P');

    -- Assign Batch Id and Run Sequence Id for Current Run
    g_new_run_seq_id := NVL(g_new_run_seq_id, xxetn_run_sequences_s.NEXTVAL);
    g_new_batch_id   := NVL(g_new_batch_id, g_batch_id);

    -- Intialize Batch Id and Run Sequence Id for Error Framework
    xxetn_common_error_pkg.g_batch_id   := g_new_batch_id; -- batch id
    xxetn_common_error_pkg.g_run_seq_id := g_new_run_seq_id; -- run sequence id

    debug_msg_p('STEP:6.2',
                'VALIDATE_EXPEND_P',
                'Batch Id to Update: ' || g_new_batch_id ||
                ' and Run Sequence Id: ' || g_new_run_seq_id);

    /** Open Cursor for records to process **/
    FOR validate_expend_rec IN validate_expend_cur LOOP
      l_status    := g_validated;
      l_error_msg := NULL;

      -- total record count
      g_total_count := g_total_count + 1;

      debug_msg_p('STEP:6.3',
                  'VALIDATE_EXPEND_P',
                  'Inside Cursor Loop for Record Id: ' ||
                  validate_expend_rec.interface_txn_id);

      -- Intialize Common variables for Record Type
      source_rec.interface_staging_id := validate_expend_rec.interface_txn_id;
      source_rec.error_type           := g_val_err_type;

      /** Mandatory Check for Project Number **/

      source_rec.source_column_name  := 'LEG_PROJECT_NUMBER';
      source_rec.source_column_value := NULL;
      source_rec.error_code          := 'NULL_LEG_PROJECT_NUMBER';
      source_rec.error_message       := 'Legacy Project Number cannot be NULL';

      IF mandatory_check_f(validate_expend_rec.leg_project_number,
                           source_rec) THEN
        debug_msg_p('STEP:6.5',
                    'VALIDATE_EXPEND_P',
                    'Legacy Project Number cannot be NULL');
        l_status                       := g_error;
        validate_expend_rec.project_id := NULL;
      END IF;

      /** Mandatory Check for Acct Raw Cost **/

      source_rec.source_column_name  := 'LEG_ACCT_RAW_COST';
      source_rec.source_column_value := NULL;
      source_rec.error_code          := 'NULL_LEG_ACCT_RAW_COST';
      source_rec.error_message       := 'Legacy Acct Raw Cost cannot be NULL';

      IF mandatory_check_f(validate_expend_rec.leg_acct_raw_cost,
                           source_rec) THEN
        debug_msg_p('STEP:6.6',
                    'VALIDATE_EXPEND_P',
                    'Legacy Acct Raw Cost cannot be NULL');
        l_status := g_error;
      END IF;

      /** Mandatory Check for Acct Currency Code **/

      source_rec.source_column_name  := 'LEG_ACCT_CURRENCY_CODE';
      source_rec.source_column_value := NULL;
      source_rec.error_code          := 'NULL_LEG_ACCT_CURRENCY_CODE';
      source_rec.error_message       := 'Legacy Acct Currency Code cannot be NULL';

      IF mandatory_check_f(validate_expend_rec.leg_acct_currency_code,
                           source_rec) THEN
        debug_msg_p('STEP:6.7',
                    'VALIDATE_EXPEND_P',
                    'Legacy Acct Currency Code cannot be NULL');
        l_status := g_error;
      END IF;

      /** Mandatory Check for GL Period Name **/

      source_rec.source_column_name  := 'LEG_GL_PERIOD_NAME';
      source_rec.source_column_value := NULL;
      source_rec.error_code          := 'NULL_LEG_GL_PERIOD_NAME';
      source_rec.error_message       := 'Legacy GL Period Name cannot be NULL';

      IF mandatory_check_f(validate_expend_rec.leg_gl_period_name,
                           source_rec) THEN
        debug_msg_p('STEP:6.8',
                    'VALIDATE_EXPEND_P',
                    'Legacy GL Period Name cannot be NULL');
        l_status := g_error;
      END IF;

      /** Mandatory Check for Expenditure Organization Name **/

      source_rec.source_column_name  := 'LEG_EXP_ORGANIZATION_NAME';
      source_rec.source_column_value := NULL;
      source_rec.error_code          := 'NULL_LEG_EXP_ORGANIZATION_NAME';
      source_rec.error_message       := 'Legacy Expenditure Organization Name cannot be NULL';

      IF mandatory_check_f(validate_expend_rec.leg_exp_organization_name,
                           source_rec) THEN
        debug_msg_p('STEP:6.9',
                    'VALIDATE_EXPEND_P',
                    'Legacy Expendture Organization Name cannot be NULL');
        l_status := g_error;
      END IF;

      /** Mandatory Check for Billable/Capitalizable Flag **/

      source_rec.source_column_name  := 'LEG_BILLABLE_FLAG';
      source_rec.source_column_value := NULL;
      source_rec.error_code          := 'NULL_LEG_BILLABLE_FLAG';
      source_rec.error_message       := 'Legacy Billable/Capitalizable Flag cannot be NULL';

      IF mandatory_check_f(validate_expend_rec.leg_billable_flag,
                           source_rec) THEN
        debug_msg_p('STEP:6.10',
                    'VALIDATE_EXPEND_P',
                    'Legacy Billable/Capitalizable Flag cannot be NULL');
        l_status := g_error;
      END IF;

      /** Derive R12 Operating Unit from Site/Plant Number **/
      l_site := NULL;
      IF validate_expend_rec.leg_project_number IS NOT NULL THEN
        source_rec.source_column_name  := 'LEG_PROJECT_NUMBER';
        source_rec.source_column_value := validate_expend_rec.leg_project_number;

        -- Deriving R12 Site/Plant#
        l_site := SUBSTR(validate_expend_rec.leg_project_number, 1, 4);

        -- Calling Procedure to Derive R12 OU from Site/Plant Number
        derive_r12_ou_p(pov_ou_name       => validate_expend_rec.operating_unit,
                        pov_status        => l_api_stats,
                        pov_error_code    => source_rec.error_code,
                        pov_error_message => source_rec.error_message,
                        piv_r12_plant     => l_site);

        IF l_api_stats <> 'S' THEN
          l_status := g_error;
          log_errors_p(pov_ret_stats => l_log_ret_stats,
                       pov_err_msg   => l_log_err_msg,
                       pi_err_rec    => source_rec);

          IF l_log_ret_stats <> 'S' THEN
            print_log_message_p('Not able to insert error details in Common Error Table for record: ' ||
                                validate_expend_rec.interface_txn_id ||
                                'Error: ' || l_log_err_msg);
          END IF;

        END IF;

      END IF;
      debug_msg_p('STEP:6.11',
                  'VALIDATE_EXPEND_P',
                  'R12 Operating Unit: ' ||
                  validate_expend_rec.operating_unit);

      /** Derive Org_id based on R12 OU **/
      IF validate_expend_rec.operating_unit IS NOT NULL THEN
        source_rec.source_column_name  := 'OPERATING_UNIT';
        source_rec.source_column_value := validate_expend_rec.operating_unit;

        -- Calling Procedure to Derive Org ID from R12 OU
        derive_org_id_p(pon_org_id        => validate_expend_rec.org_id,
                        pov_status        => l_api_stats,
                        pov_error_code    => source_rec.error_code,
                        pov_error_message => source_rec.error_message,
                        piv_ou_name       => validate_expend_rec.operating_unit);

        IF l_api_stats <> 'S' THEN
          l_status := g_error;
          log_errors_p(pov_ret_stats => l_log_ret_stats,
                       pov_err_msg   => l_log_err_msg,
                       pi_err_rec    => source_rec);

          IF l_log_ret_stats <> 'S' THEN
            print_log_message_p('Not able to insert error details in Common Error Table for record: ' ||
                                validate_expend_rec.interface_txn_id ||
                                'Error: ' || l_log_err_msg);
          END IF;

        END IF;

      END IF;
      debug_msg_p('STEP:6.12',
                  'VALIDATE_EXPEND_P',
                  'Org_id: ' || validate_expend_rec.org_id);

      /** Validate Project Details **/
      IF validate_expend_rec.leg_project_number IS NOT NULL THEN

        source_rec.source_column_name  := 'LEG_PROJECT_NUMBER';
        source_rec.source_column_value := validate_expend_rec.leg_project_number;

        -- Calling Procedure to Validate Project Details
        validate_proj_details_p(pon_project_id    => validate_expend_rec.project_id,
                                pod_start_date    => validate_expend_rec.project_start_date,
                                pod_end_date      => validate_expend_rec.project_end_date,
                                pov_class_code    => validate_expend_rec.project_class_code,
                                pov_allow_charges => validate_expend_rec.allow_cross_charges,
                                pon_org_id        => validate_expend_rec.project_org_id,
                                pov_status        => l_api_stats,
                                pov_error_code    => source_rec.error_code,
                                pov_error_message => source_rec.error_message,
                                piv_leg_proj_num  => validate_expend_rec.leg_project_number);

        IF l_api_stats <> 'S' THEN
          l_status := g_error;
          log_errors_p(pov_ret_stats => l_log_ret_stats,
                       pov_err_msg   => l_log_err_msg,
                       pi_err_rec    => source_rec);

          IF l_log_ret_stats <> 'S' THEN
            print_log_message_p('Not able to insert error details in Common Error Table for record: ' ||
                                validate_expend_rec.interface_txn_id ||
                                'Error: ' || l_log_err_msg);
          END IF;

        END IF;

      END IF;
      debug_msg_p('STEP:6.13',
                  'VALIDATE_EXPEND_P',
                  'Project Id: ' || validate_expend_rec.project_id ||
                  ' and Class Code: ' ||
                  validate_expend_rec.project_class_code);

      /** Check Allow Cross Charges Flag for Cross Operating Unit Expenditures **/
      IF validate_expend_rec.project_org_id <> -1 AND
         validate_expend_rec.org_id <> -1 THEN
        IF validate_expend_rec.project_org_id <> validate_expend_rec.org_id AND
           NVL(validate_expend_rec.allow_cross_charges, 'N') = 'N' THEN
          source_rec.source_column_name  := 'ALLOW_CROSS_CHARGES';
          source_rec.source_column_value := validate_expend_rec.allow_cross_charges;
          source_rec.error_code          := 'ALLOW_CROSS_CHARGES_DISABLED';
          source_rec.error_message       := 'Allow Cross Charges must be enabled for Project: ' ||
                                            validate_expend_rec.leg_project_number ||
                                            ' to allow Cross OU Expenditures';

          l_status := g_error;
          log_errors_p(pov_ret_stats => l_log_ret_stats,
                       pov_err_msg   => l_log_err_msg,
                       pi_err_rec    => source_rec);

          IF l_log_ret_stats <> 'S' THEN
            print_log_message_p('Not able to insert error details in Common Error Table for record: ' ||
                                validate_expend_rec.interface_txn_id ||
                                'Error: ' || l_log_err_msg);
          END IF;
        END IF;
      END IF;

      /** Derive R12 Task Details **/
      IF validate_expend_rec.project_class_code IS NOT NULL THEN

        source_rec.source_column_name  := 'PROJECT_CLASS_CODE';
        source_rec.source_column_value := validate_expend_rec.project_class_code;

        -- Calling Procedure to derive R12 Task Number
        derive_r12_task_p(pov_task_number   => validate_expend_rec.task_number,
                          pov_status        => l_api_stats,
                          pov_error_code    => source_rec.error_code,
                          pov_error_message => source_rec.error_message,
                          piv_class_code    => validate_expend_rec.project_class_code);

        IF l_api_stats <> 'S' THEN
          l_status := g_error;
          log_errors_p(pov_ret_stats => l_log_ret_stats,
                       pov_err_msg   => l_log_err_msg,
                       pi_err_rec    => source_rec);

          IF l_log_ret_stats <> 'S' THEN
            print_log_message_p('Not able to insert error details in Common Error Table for record: ' ||
                                validate_expend_rec.interface_txn_id ||
                                'Error: ' || l_log_err_msg);
          END IF;

        END IF;

      END IF;

      debug_msg_p('STEP:6.14',
                  'VALIDATE_EXPEND_P',
                  'R12 Task Number: ' || validate_expend_rec.task_number);

      /** Derive Task Id based on R12 Task Number **/
      IF validate_expend_rec.task_number IS NOT NULL AND
         validate_expend_rec.project_id <> -1 THEN

        source_rec.source_column_name  := 'TASK_NUMBER';
        source_rec.source_column_value := validate_expend_rec.task_number;

        -- Calling Procedure to derive Task Id for R12 Task
        derive_task_id_p(pon_task_id         => validate_expend_rec.task_id,
                         pod_task_start_date => validate_expend_rec.task_start_date,
                         pod_task_end_date   => validate_expend_rec.task_end_date,
                         pov_status          => l_api_stats,
                         pov_error_code      => source_rec.error_code,
                         pov_error_message   => source_rec.error_message,
                         pin_project_id      => validate_expend_rec.project_id,
                         piv_task_number     => validate_expend_rec.task_number);

        IF l_api_stats <> 'S' THEN
          l_status := g_error;
          log_errors_p(pov_ret_stats => l_log_ret_stats,
                       pov_err_msg   => l_log_err_msg,
                       pi_err_rec    => source_rec);

          IF l_log_ret_stats <> 'S' THEN
            print_log_message_p('Not able to insert error details in Common Error Table for record: ' ||
                                validate_expend_rec.interface_txn_id ||
                                'Error: ' || l_log_err_msg);
          END IF;

        END IF;

      END IF;

      debug_msg_p('STEP:6.15',
                  'VALIDATE_EXPEND_P',
                  'Task Id: ' || validate_expend_rec.task_id);

      /** Derive Expenditure Item Date based on GL Period Name **/
      IF validate_expend_rec.leg_gl_period_name IS NOT NULL AND
         validate_expend_rec.org_id <> -1 THEN

        source_rec.source_column_name  := 'LEG_GL_PERIOD_NAME';
        source_rec.source_column_value := validate_expend_rec.leg_gl_period_name;

        -- Calling Procedure to derive Expenditure Item based on GL Period Name
        derive_item_date_p(pod_exp_item_date    => validate_expend_rec.expenditure_item_date,
                           pov_status           => l_api_stats,
                           pov_error_code       => source_rec.error_code,
                           pov_error_message    => source_rec.error_message,
                           pin_org_id           => validate_expend_rec.org_id,
                           piv_period_name      => validate_expend_rec.leg_gl_period_name,
                           piv_project_end_date => validate_expend_rec.project_end_date --- added for the PMC 331563
                           );



        IF l_api_stats <> 'S' THEN
          l_status := g_error;
          log_errors_p(pov_ret_stats => l_log_ret_stats,
                       pov_err_msg   => l_log_err_msg,
                       pi_err_rec    => source_rec);

          IF l_log_ret_stats <> 'S' THEN
            print_log_message_p('Not able to insert error details in Common Error Table for record: ' ||
                                validate_expend_rec.interface_txn_id ||
                                'Error: ' || l_log_err_msg);
          END IF;
        END IF;

      END IF;
      debug_msg_p('STEP:6.16',
                  'VALIDATE_EXPEND_P',
                  'Expenditure Item Date: ' ||
                  validate_expend_rec.expenditure_item_date);

      -- Assigning Expenditure Item Date to Expenditure Ending Date
      validate_expend_rec.expenditure_ending_date := validate_expend_rec.expenditure_item_date;

      /** Validate if Expenditure Item Date lies within Project/Task Start and End Date **/
      IF validate_expend_rec.expenditure_item_date IS NOT NULL AND
         validate_expend_rec.task_id <> -1 THEN

        IF validate_expend_rec.expenditure_item_date <
           NVL(validate_expend_rec.project_start_date,
               validate_expend_rec.project_start_date - 1) OR
           validate_expend_rec.expenditure_item_date >
           NVL(validate_expend_rec.project_end_date,
               validate_expend_rec.expenditure_item_date + 1) THEN

          source_rec.source_column_name  := 'EXPENDITURE_ITEM_DATE';
          source_rec.source_column_value := validate_expend_rec.expenditure_item_date;
          source_rec.error_code          := 'EXPEND_ITEM_DATE_OUT_OF_RANGE';
          source_rec.error_message       := 'Expenditure Item Date must lie between Project Start and End Date';

          l_status := g_error;
          log_errors_p(pov_ret_stats => l_log_ret_stats,
                       pov_err_msg   => l_log_err_msg,
                       pi_err_rec    => source_rec);

          IF l_log_ret_stats <> 'S' THEN
            print_log_message_p('Not able to insert error details in Common Error Table for record: ' ||
                                validate_expend_rec.interface_txn_id ||
                                'Error: ' || l_log_err_msg);
          END IF;

        ELSIF validate_expend_rec.expenditure_item_date <
              NVL(validate_expend_rec.task_start_date,
                  validate_expend_rec.task_start_date - 1) OR
              validate_expend_rec.expenditure_item_date >
              NVL(validate_expend_rec.task_end_date,
                  validate_expend_rec.task_end_date + 1) THEN

          source_rec.source_column_name  := 'EXPENDITURE_ITEM_DATE';
          source_rec.source_column_value := validate_expend_rec.expenditure_item_date;
          source_rec.error_code          := 'EXPEND_ITEM_DATE_OUT_OF_RANGE';
          source_rec.error_message       := 'Expenditure Item Date must lie between Task Start and End Date';

          l_status := g_error;
          log_errors_p(pov_ret_stats => l_log_ret_stats,
                       pov_err_msg   => l_log_err_msg,
                       pi_err_rec    => source_rec);

          IF l_log_ret_stats <> 'S' THEN
            print_log_message_p('Not able to insert error details in Common Error Table for record: ' ||
                                validate_expend_rec.interface_txn_id ||
                                'Error: ' || l_log_err_msg);
          END IF;

        END IF;

      END IF;

      /** Validate Acct Currency Code **/
      IF validate_expend_rec.leg_acct_currency_code IS NOT NULL THEN

        source_rec.source_column_name  := 'LEG_ACCT_CURRENCY_CODE';
        source_rec.source_column_value := validate_expend_rec.leg_acct_currency_code;

        -- Calling Procedure to validate Acct Currency Code
        validate_currency_p(pov_status        => l_api_stats,
                            pov_error_code    => source_rec.error_code,
                            pov_error_message => source_rec.error_message,
                            piv_currency_code => validate_expend_rec.leg_acct_currency_code);

        IF l_api_stats <> 'S' THEN
          l_status := g_error;
          log_errors_p(pov_ret_stats => l_log_ret_stats,
                       pov_err_msg   => l_log_err_msg,
                       pi_err_rec    => source_rec);

          IF l_log_ret_stats <> 'S' THEN
            print_log_message_p('Not able to insert error details in Common Error Table for record: ' ||
                                validate_expend_rec.interface_txn_id ||
                                'Error: ' || l_log_err_msg);
          END IF;
        END IF;

      END IF;

      /** Validate Expenditure Organization **/
      IF validate_expend_rec.leg_exp_organization_name IS NOT NULL THEN

        source_rec.source_column_name  := 'LEG_EXP_ORGANIZATION_NAME';
        source_rec.source_column_value := validate_expend_rec.leg_exp_organization_name;

        -- Calling Procedure to Validate Expenditure Organization Name
        validate_exp_org_p(pon_exp_org_id    => validate_expend_rec.exp_organization_id,
                           pon_exp_name      => validate_expend_rec.exp_organization_name,
                           pov_status        => l_api_stats,
                           pov_error_code    => source_rec.error_code,
                           pov_error_message => source_rec.error_message,
                           piv_exp_org       => validate_expend_rec.leg_exp_organization_name);

        IF l_api_stats <> 'S' THEN
          l_status := g_error;
          log_errors_p(pov_ret_stats => l_log_ret_stats,
                       pov_err_msg   => l_log_err_msg,
                       pi_err_rec    => source_rec);

          IF l_log_ret_stats <> 'S' THEN
            print_log_message_p('Not able to insert error details in Common Error Table for record: ' ||
                                validate_expend_rec.interface_txn_id ||
                                'Error: ' || l_log_err_msg);
          END IF;
        END IF;

      END IF;
      debug_msg_p('STEP:6.17',
                  'VALIDATE_EXPEND_P',
                  'Expenditure Org Id: ' ||
                  validate_expend_rec.exp_organization_id);

      /** If any Validation Fails, increment failed record count **/
      IF l_status = g_error THEN
        g_failed_count := g_failed_count + 1;
      END IF;

      debug_msg_p('STEP:6.18',
                  'VALIDATE_EXPEND_P',
                  validate_expend_rec.interface_txn_id || ' status is: ' ||
                  l_status);

      /** Update Staging table with Record Status **/
      debug_msg_p('STEP:6.19',
                  'VALIDATE_EXPEND_P',
                  'Update Staging Table with record status');

      BEGIN
        UPDATE xxpa_proj_expend_stg xpes
           SET xpes.process_flag            = l_status,
               xpes.run_sequence_id         = g_new_run_seq_id,
               xpes.error_type              = DECODE(l_status,
                                                     g_error,
                                                     g_val_err_type,
                                                     NULL),
               xpes.operating_unit          = validate_expend_rec.operating_unit,
               xpes.org_id                  = validate_expend_rec.org_id,
               xpes.project_id              = validate_expend_rec.project_id,
               xpes.project_start_date      = validate_expend_rec.project_start_date,
               xpes.project_end_date        = validate_expend_rec.project_end_date,
               xpes.project_class_code      = validate_expend_rec.project_class_code,
               xpes.allow_cross_charges     = validate_expend_rec.allow_cross_charges,
               xpes.project_org_id          = validate_expend_rec.project_org_id,
               xpes.task_number             = validate_expend_rec.task_number,
               xpes.task_id                 = validate_expend_rec.task_id,
               xpes.task_start_date         = validate_expend_rec.task_start_date,
               xpes.task_end_date           = validate_expend_rec.task_end_date,
               xpes.expenditure_item_date   = validate_expend_rec.expenditure_item_date,
               xpes.expenditure_ending_date = validate_expend_rec.expenditure_ending_date,
               xpes.exp_organization_id     = validate_expend_rec.exp_organization_id,
               xpes.exp_organization_name   = validate_expend_rec.exp_organization_name --changes as per defect 1821
              ,
               xpes.last_update_date        = SYSDATE,
               xpes.last_update_login       = g_last_update_login,
               xpes.last_updated_by         = g_last_updated_by,
               xpes.request_id              = g_request_id
         WHERE xpes.interface_txn_id = validate_expend_rec.interface_txn_id;
      EXCEPTION
        WHEN OTHERS THEN
          debug_msg_p('STEP:6.20',
                      'VALIDATE_EXPEND_P',
                      'SQL Error in updating Staging table: ' || SQLERRM);
      END;

      -- If Batch Commit Limit is reached
      IF l_count >= g_limit THEN
        l_count := 0;
        debug_msg_p('STEP:6.21',
                    'VALIDATE_EXPEND_P',
                    'Performing Batch Commit');
        COMMIT;
      ELSE
        l_count := l_count + 1;
      END IF;

    END LOOP; -- End Cursor Loop

    COMMIT;
    debug_msg_p('STEP:6.22', 'VALIDATE_EXPEND_P', 'Outside Cursor Loop');

    -- Insert remaining errors into Error Table
    IF g_source_tab.COUNT > 0 THEN

      debug_msg_p('STEP:6.23',
                  'VALIDATE_EXPEND_P',
                  'Error Table Type Count: ' || g_source_tab.COUNT);
      debug_msg_p('STEP:6.24',
                  'VALIDATE_EXPEND_P',
                  'Logging Remaining Errors in Error table');

      g_err_cnt := 1;
      xxetn_common_error_pkg.add_error(pov_return_status => l_log_ret_stats,
                                       pov_error_msg     => l_log_err_msg,
                                       pi_source_tab     => g_source_tab);

      IF l_log_ret_stats <> 'S' THEN
        pov_ret_stats := 'E';
        print_log_message_p('Not able to insert error details in Common Error Table: ' ||
                            'Error: ' || l_log_err_msg);
      END IF;

      -- Flushing PLSQL Table
      g_source_tab.DELETE;

    END IF;

    debug_msg_p('STEP:6.25',
                'VALIDATE_EXPEND_P',
                'Processed Records during Validation: ' || g_total_count);
    debug_msg_p('STEP:6.26',
                'VALIDATE_EXPEND_P',
                'Failed Records during Validation: ' || g_failed_count);

    IF g_failed_count > 0 THEN
      pov_ret_stats := 'E'; -- Program must complete in Warning if any of Validation fails
    END IF;

  EXCEPTION
    WHEN OTHERS THEN
      pov_ret_stats := 'E';
      pov_err_msg   := 'ERROR : XXPA_PROJ_EXPEND_CNV_PKG.VALIDATE_EXPEND_P->WHEN_OTHERS->' ||
                       SQLERRM;
  END validate_expend_p;

  --
  -- =============================================================================
  -- Procedure: update_staging_p
  -- =============================================================================
  -- This private Procedure will update Conversion Staging table process flag while Interfacing
  --
  -- =============================================================================
  --
  -- -----------------------------------------------------------------------------
  --  Called By: IMPORT_EXPEND_P
  -- -----------------------------------------------------------------------------
  --
  --  Input Parameters     :
  --  piv_status           : Process Flag to be updated for record
  --  pin_interface_txn_id : Interface Txn Id for which record to be updated
  --
  --  Output Parameters    :
  --  pov_err_msg          : Return Error Message
  --
  --  Return               : Not applicable
  -- -----------------------------------------------------------------------------

  PROCEDURE update_staging_p(pov_err_msg          OUT NOCOPY VARCHAR2,
                             piv_status           IN VARCHAR2,
                             pin_interface_txn_id IN NUMBER) IS

    PRAGMA AUTONOMOUS_TRANSACTION;

  BEGIN
    pov_err_msg := NULL;

    debug_msg_p('STEP:7.5.1',
                'UPDATE_STAGING_P',
                'In Begin of Proc: UPDATE_STAGING_P');

    debug_msg_p('STEP:7.5.2',
                'UPDATE_STAGING_P',
                'Updating Staging Table for Record id: ' ||
                pin_interface_txn_id || ' and status: ' || piv_status);

    BEGIN
      UPDATE xxpa_proj_expend_stg xpes
         SET xpes.process_flag      = piv_status,
             xpes.run_sequence_id   = g_new_run_seq_id,
             xpes.error_type        = DECODE(piv_status,
                                             g_error,
                                             g_int_err_type,
                                             NULL),
             xpes.last_update_date  = SYSDATE,
             xpes.last_update_login = g_last_update_login,
             xpes.last_updated_by   = g_last_updated_by,
             xpes.request_id        = g_request_id
       WHERE xpes.interface_txn_id = pin_interface_txn_id;
    EXCEPTION
      WHEN OTHERS THEN
        pov_err_msg := 'SQL Error in updating Staging table: ' || SQLERRM;
        debug_msg_p('STEP:7.5.3',
                    'UPDATE_STAGING_P',
                    'SQL Error in updating Staging table: ' || SQLERRM);
    END;

    COMMIT;

  EXCEPTION
    WHEN OTHERS THEN
      pov_err_msg := 'SQL Error in Proc UPDATE_STAGING_P: ' || SQLERRM;
      debug_msg_p('STEP:7.5.4',
                  'UPDATE_STAGING_P',
                  'Unexpected SQL Error in Proc UPDATE_STAGING_P: ' ||
                  SQLERRM);
  END update_staging_p;

  --
  -- =============================================================================
  -- Procedure: import_expend_p
  -- =============================================================================
  -- This private procedure will import/interface Project Expenditures using Interface Table
  --
  -- =============================================================================
  --
  -- -----------------------------------------------------------------------------
  --  Called By: PROCESS_EXPENDITURES_P
  -- -----------------------------------------------------------------------------
  --
  --  Input Parameters     : NONE
  --
  --  Output Parameters    :
  --  pov_ret_stats        : Returns Status
  --  pov_err_msg          : Return Error Message
  --
  --  Return               : Not applicable
  -- -----------------------------------------------------------------------------

  PROCEDURE import_expend_p(pov_ret_stats OUT NOCOPY VARCHAR2,
                            pov_err_msg   OUT NOCOPY VARCHAR2) IS

    l_err_msg VARCHAR2(1000);
    l_count   NUMBER DEFAULT 0;
    l_status  xxpa_proj_expend_stg.process_flag%TYPE;

    l_log_ret_stats               VARCHAR2(10);
    l_log_err_msg                 VARCHAR2(2000);
    l_unmatched_negative_txn_flag VARCHAR2(1);

    -- Error Table Record Type
    source_rec xxetn_common_error_pkg.g_source_rec_type;

    -- Staging table cursor with process_flag 'V'
    CURSOR valid_expend_cur IS
      SELECT xpes.interface_txn_id,
             xpes.batch_id,
             xpes.operating_unit,
             xpes.expenditure_ending_date
             -- ,xpes.leg_exp_organization_name
            ,
             xpes.exp_organization_name --changes as per defect 1821
            ,
             xpes.expenditure_item_date,
             xpes.leg_project_number,
             xpes.task_number,
             xpes.leg_acct_raw_cost,
             xpes.org_id,
             xpes.leg_acct_currency_code,
             xpes.leg_billable_flag,
             xpes.project_id,
             xpes.task_id,
             xpes.exp_organization_id,
             xpes.leg_count_of_rows,
             xpes.orig_transaction_reference,
             xpes.attribute_category,
             xpes.attribute1,
             xpes.attribute2,
             xpes.attribute3,
             xpes.attribute4,
             xpes.attribute5
        FROM xxpa_proj_expend_stg xpes
       WHERE xpes.process_flag = g_validated
         AND xpes.batch_id = NVL(g_batch_id, xpes.batch_id)
       ORDER BY xpes.expenditure_item_date asc;  -- Reshu commneted added on 4-Feb
      -- ORDER BY xpes.interface_txn_id;  -- Reshu commneted on 4-Feb

  BEGIN

    pov_ret_stats  := 'S';
    pov_err_msg    := NULL;
    g_total_count  := 0;
    g_failed_count := 0;

    debug_msg_p('STEP:7.1',
                'IMPORT_EXPEND_P',
                'In Begin of Proc: IMPORT_EXPEND_P');

    g_new_run_seq_id := xxetn_run_sequences_s.NEXTVAL;

    -- Intialize Run Sequence Id Only for Error Framework
    xxetn_common_error_pkg.g_batch_id   := g_batch_id; -- batch id
    xxetn_common_error_pkg.g_run_seq_id := g_new_run_seq_id; -- run sequence id

    debug_msg_p('STEP:7.2',
                'IMPORT_EXPEND_P',
                'Run Sequence Id: ' || g_new_run_seq_id);

    source_rec.source_column_name  := NULL;
    source_rec.source_column_value := NULL;
    source_rec.error_code          := 'INTERFACE_ERROR';

    --- Open Cursor For Loop
    FOR valid_expend_rec IN valid_expend_cur LOOP
      l_status      := g_interfaced;
      g_total_count := g_total_count + 1;

      debug_msg_p('STEP:7.3',
                  'IMPORT_EXPEND_P',
                  'Inside Loop for Record Id: ' ||
                  valid_expend_rec.interface_txn_id);

      -- Intialize Common variables for Record Type
      source_rec.interface_staging_id := valid_expend_rec.interface_txn_id;
      source_rec.error_type           := g_int_err_type;

      -- For negative value transactions
      IF valid_expend_rec.leg_acct_raw_cost < 0 THEN
        l_unmatched_negative_txn_flag := 'Y';
      ELSE
        l_unmatched_negative_txn_flag := NULL;
      END IF;

      BEGIN
        INSERT INTO pa_transaction_interface_all
          (transaction_source,
           batch_name,
           expenditure_ending_date,
           organization_name,
           expenditure_item_date,
           project_number,
           task_number,
           expenditure_type,
           quantity,
           transaction_status_code,
           orig_transaction_reference,
           raw_cost_rate,
           unmatched_negative_txn_flag,
           org_id,
           created_by,
           creation_date,
           last_updated_by,
           last_update_date,
           denom_currency_code,
           denom_raw_cost,
           acct_raw_cost,
           billable_flag,
           project_id,
           task_id,
           organization_id,
           unit_of_measure,
           expenditure_comment,
           attribute_category,
           attribute1,
           attribute2,
           attribute3,
           attribute4,
           attribute5)
        VALUES
          (g_trx_source,
           valid_expend_rec.operating_unit || '_' ||
           TO_CHAR(SYSDATE, 'MMDDYY'),
           valid_expend_rec.expenditure_ending_date
           --   ,valid_expend_rec.leg_exp_organization_name
          ,
           valid_expend_rec.exp_organization_name,
           valid_expend_rec.expenditure_item_date,
           valid_expend_rec.leg_project_number,
           valid_expend_rec.task_number,
           g_expend_type,
           valid_expend_rec.leg_acct_raw_cost,
           'P',
           valid_expend_rec.orig_transaction_reference,
           1,
           l_unmatched_negative_txn_flag,
           valid_expend_rec.org_id,
           g_last_updated_by,
           SYSDATE,
           g_last_updated_by,
           SYSDATE,
           valid_expend_rec.leg_acct_currency_code,
           valid_expend_rec.leg_acct_raw_cost,
           valid_expend_rec.leg_acct_raw_cost,
           valid_expend_rec.leg_billable_flag,
           valid_expend_rec.project_id,
           valid_expend_rec.task_id,
           valid_expend_rec.exp_organization_id,
           'CURRENCY',
           valid_expend_rec.leg_count_of_rows,
           valid_expend_rec.attribute_category,
           valid_expend_rec.attribute1,
           valid_expend_rec.attribute2,
           valid_expend_rec.attribute3,
           valid_expend_rec.attribute4,
           valid_expend_rec.attribute5);
      EXCEPTION
        WHEN OTHERS THEN
          g_failed_count           := g_failed_count + 1;
          l_status                 := g_error;
          source_rec.error_message := 'Error in Interfacing Record. Error: ' ||
                                      SUBSTR(SQLERRM, 1, 250);
          log_errors_p(pov_ret_stats => l_log_ret_stats,
                       pov_err_msg   => l_log_err_msg,
                       pi_err_rec    => source_rec);

          debug_msg_p('STEP:7.4',
                      'IMPORT_EXPEND_P',
                      'Error while Interfacing: ' ||
                      SUBSTR(SQLERRM, 1, 250));
      END;

      /** Update Staging table with Record Status **/
      debug_msg_p('STEP:7.5',
                  'IMPORT_EXPEND_P',
                  'Before Calling Proc: UPDATE_STAGING_P');

      update_staging_p(pov_err_msg          => l_err_msg,
                       piv_status           => l_status,
                       pin_interface_txn_id => valid_expend_rec.interface_txn_id);

      -- Performing Batch Commit if Limit is reached
      IF l_count >= g_limit THEN
        l_count := 0;
        debug_msg_p('STEP:7.6',
                    'IMPORT_EXPEND_P',
                    'Performing Batch Commit');
        COMMIT;
      ELSE
        l_count := l_count + 1;
      END IF;

    END LOOP; -- End Cursor Loop
    COMMIT;

    -- Update Staging Table to populate Txn Interface Id for Interfaced records
    BEGIN
      UPDATE xxpa_proj_expend_stg xpes
         SET xpes.txn_interface_id =
             (SELECT ptia.txn_interface_id
                FROM pa_transaction_interface_all ptia
               WHERE ptia.transaction_status_code = 'P'
                 AND ptia.orig_transaction_reference =
                     xpes.orig_transaction_reference)
       WHERE xpes.process_flag = g_interfaced
         AND xpes.request_id = g_request_id;
    EXCEPTION
      WHEN OTHERS THEN
        pov_ret_stats := 'E';
        print_log_message_p('SQL Error in updating staging table for Interfaced records: ' ||
                            SQLERRM);
    END;

    COMMIT;

    -- Insert remaining errors into Error Table
    IF g_source_tab.COUNT > 0 THEN

      debug_msg_p('STEP:7.7',
                  'IMPORT_EXPEND_P',
                  'Error Table Type Count: ' || g_source_tab.COUNT);

      debug_msg_p('STEP:7.8',
                  'IMPORT_EXPEND_P',
                  'Logging Remaining Errors in Error table');

      g_err_cnt := 1;
      xxetn_common_error_pkg.add_error(pov_return_status => l_log_ret_stats,
                                       pov_error_msg     => l_log_err_msg,
                                       pi_source_tab     => g_source_tab);

      IF l_log_ret_stats <> 'S' THEN
        pov_ret_stats := 'E';
        print_log_message_p('Not able to insert error details in Common Error Table: ' ||
                            'Error: ' || l_log_err_msg);
      END IF;

      -- Flushing PLSQL Table
      g_source_tab.DELETE;

    END IF;

    debug_msg_p('STEP:7.9',
                'IMPORT_EXPEND_P',
                'Performing Commit for API after Loop Ends');

    debug_msg_p('STEP:7.10',
                'IMPORT_EXPEND_P',
                'Processed Records during Import: ' || g_total_count);

    debug_msg_p('STEP:7.11',
                'IMPORT_EXPEND_P',
                'Failed Records during Import: ' || g_failed_count);

    IF g_failed_count > 0 THEN
      pov_ret_stats := 'E'; -- Program must complete in Warning if any of Import fails
    END IF;

  EXCEPTION
    WHEN OTHERS THEN
      pov_ret_stats := 'E';
      pov_err_msg   := 'ERROR : XXPA_PROJ_EXPEND_CNV_PKG.IMPORT_EXPEND_P->WHEN_OTHERS->' ||
                       SQLERRM;
  END import_expend_p;

  --
  -- =============================================================================
  -- Procedure: print_stats_p
  -- =============================================================================
  -- This private procedure will Print Stats in Output File
  --
  -- =============================================================================
  --
  -- -----------------------------------------------------------------------------
  --  Called By: PROCESS_EXPENDITURES_P
  -- -----------------------------------------------------------------------------
  --
  --  Input Parameters     :
  --  pin_total_count      : Total Records Count
  --  pin_success_count    : Success Records Count
  --  pin_failed_count     : Failed Records Count in Validation
  --  pin_failed_count_int : Failed Records Count in Interface
  --  pin_failed_count_imp : Failed Records Count in Import
  --
  --  Output Parameters    :
  --  pov_ret_stats        : Return Status as 'S' or 'E'
  --  pov_err_msg          : Return Error Message
  --
  --  Return               : Not applicable
  -- -----------------------------------------------------------------------------

  PROCEDURE print_stats_p(pov_ret_stats        OUT NOCOPY VARCHAR2,
                          pov_err_msg          OUT NOCOPY VARCHAR2,
                          pin_total_count      IN NUMBER,
                          pin_success_count    IN NUMBER,
                          pin_failed_count     IN NUMBER,
                          pin_failed_count_int IN NUMBER,
                          pin_failed_count_imp IN NUMBER) IS

  BEGIN

    pov_ret_stats := 'S';
    pov_err_msg   := NULL;

    debug_msg_p('STEP:4.1',
                'PRINT_STATS_P',
                'In Begin of Prc: PRINT_STATS_P');

    fnd_file.put_line(fnd_file.output,
                      'Program Name : Eaton Project Expenditures Conversion Program');
    fnd_file.put_line(fnd_file.output,
                      'Request ID   : ' || TO_CHAR(g_request_id));
    fnd_file.put_line(fnd_file.output,
                      'Report Date  : ' ||
                      TO_CHAR(SYSDATE, 'DD-MON-RRRR HH24:MI:SS'));
    fnd_file.put_line(fnd_file.output,
                      '=============================================================================================');
    fnd_file.put_line(fnd_file.output, CHR(10));
    fnd_file.put_line(fnd_file.output, 'Parameters');
    fnd_file.put_line(fnd_file.output,
                      '---------------------------------------------');
    fnd_file.put_line(fnd_file.output,
                      'Run Mode            : ' || g_run_mode);
    fnd_file.put_line(fnd_file.output,
                      'Batch ID            : ' || g_batch_id);
    fnd_file.put_line(fnd_file.output,
                      'Process records     : ' || g_process_records);

    fnd_file.put_line(fnd_file.output, CHR(10));
    fnd_file.put_line(fnd_file.output,
                      '=============================================================================================');
    fnd_file.put_line(fnd_file.output,
                      'Statistics (' || g_run_mode || '):');
    fnd_file.put_line(fnd_file.output,
                      '=============================================================================================');

    IF g_run_mode = 'LOAD-DATA' THEN

      fnd_file.put_line(fnd_file.output,
                        'Records Eligible   : ' || pin_total_count);
      fnd_file.put_line(fnd_file.output,
                        'Records Pulled     : ' || pin_success_count);
      fnd_file.put_line(fnd_file.output,
                        'Records Errored    : ' || pin_failed_count);

    ELSIF g_run_mode = 'VALIDATE' THEN

      fnd_file.put_line(fnd_file.output,
                        'Records Submitted  : ' || pin_total_count);
      fnd_file.put_line(fnd_file.output,
                        'Records Validated  : ' || pin_success_count);
      fnd_file.put_line(fnd_file.output,
                        'Records Errored    : ' || pin_failed_count);

    ELSIF g_run_mode = 'CONVERSION' THEN

      fnd_file.put_line(fnd_file.output,
                        'Records Submitted  : ' || pin_total_count);
      fnd_file.put_line(fnd_file.output,
                        'Records Interfaced : ' || pin_success_count);
      fnd_file.put_line(fnd_file.output,
                        'Records Errored    : ' || pin_failed_count);

    ELSIF g_run_mode = 'RECONCILE' THEN

      fnd_file.put_line(fnd_file.output,
                        'Records Submitted              : ' ||
                        pin_total_count);
      fnd_file.put_line(fnd_file.output,
                        'Records Imported               : ' ||
                        pin_success_count);
      fnd_file.put_line(fnd_file.output,
                        'Records Errored in Validation  : ' ||
                        pin_failed_count);
      fnd_file.put_line(fnd_file.output,
                        'Records Errored in Interface   : ' ||
                        pin_failed_count_int);
      fnd_file.put_line(fnd_file.output,
                        'Records Errored in Import      : ' ||
                        pin_failed_count_imp);

    END IF;

    fnd_file.put_line(fnd_file.output,
                      '=============================================================================================');

  EXCEPTION
    WHEN OTHERS THEN
      pov_ret_stats := 'E';
      pov_err_msg   := 'ERROR: XXPA_PROJ_EXPEND_CNV_PKG.PRINT_STATS_P->WHEN_OTHERS->' ||
                       SQLERRM;
  END print_stats_p;

  --
  -- =============================================================================
  -- Procedure: print_stats_tieback_p
  -- =============================================================================
  -- This private procedure will Print Stats for Tieback Program in Output File
  --
  -- =============================================================================
  --
  -- -----------------------------------------------------------------------------
  --  Called By: TIEBACK_P
  -- -----------------------------------------------------------------------------
  --
  --  Input Parameters     :
  --  pin_total_count      : Total Records Count
  --  pin_success_count    : Success Records Count
  --  pin_failed_count     : Failed Records Count in Validation
  --
  --  Output Parameters    :
  --  pov_ret_stats        : Return Status as 'S' or 'E'
  --  pov_err_msg          : Return Error Message
  --
  --  Return               : Not applicable
  -- -----------------------------------------------------------------------------

  PROCEDURE print_stats_tieback_p(pov_ret_stats     OUT NOCOPY VARCHAR2,
                                  pov_err_msg       OUT NOCOPY VARCHAR2,
                                  pin_total_count   IN NUMBER,
                                  pin_success_count IN NUMBER,
                                  pin_failed_count  IN NUMBER) IS

  BEGIN

    pov_ret_stats := 'S';
    pov_err_msg   := NULL;

    debug_msg_p('STEP:30.1',
                'PRINT_STATS_TIEBACK_P',
                'In Begin of Prc: PRINT_STATS_TIEBACK_P');

    fnd_file.put_line(fnd_file.output,
                      'Program Name                 : Eaton Project Expenditures Tieback Conversion Program');
    fnd_file.put_line(fnd_file.output,
                      'Request ID                   : ' ||
                      TO_CHAR(g_request_id));
    fnd_file.put_line(fnd_file.output,
                      'Report Date                  : ' ||
                      TO_CHAR(SYSDATE, 'DD-MON-RRRR HH24:MI:SS'));
    fnd_file.put_line(fnd_file.output,
                      '=============================================================================================');
    fnd_file.put_line(fnd_file.output, CHR(10));
    fnd_file.put_line(fnd_file.output, 'Parameters');
    fnd_file.put_line(fnd_file.output,
                      '---------------------------------------------');
    fnd_file.put_line(fnd_file.output,
                      'Batch ID                     : ' || g_batch_id);

    fnd_file.put_line(fnd_file.output, CHR(10));
    fnd_file.put_line(fnd_file.output,
                      '=============================================================================================');
    fnd_file.put_line(fnd_file.output, 'Statistics :');
    fnd_file.put_line(fnd_file.output,
                      '=============================================================================================');

    fnd_file.put_line(fnd_file.output,
                      'Total Records Interfaced      : ' || pin_total_count);
    fnd_file.put_line(fnd_file.output,
                      'Records Successfully Imported : ' ||
                      pin_success_count);
    fnd_file.put_line(fnd_file.output,
                      'Records Errored in Interface  : ' ||
                      pin_failed_count);

    fnd_file.put_line(fnd_file.output,
                      '=============================================================================================');

  EXCEPTION
    WHEN OTHERS THEN
      pov_ret_stats := 'E';
      pov_err_msg   := 'ERROR: XXPA_PROJ_EXPEND_CNV_PKG.PRINT_STATS_TIEBACK_P->WHEN_OTHERS->' ||
                       SQLERRM;
  END print_stats_tieback_p;

  --
  -- =============================================================================
  -- Procedure: process_expenditures_p
  -- =============================================================================
  --   This is main procedure invoked by Project Expenditures Conversion Program
  --
  --   This conversion program is used to convert Project Expenditures related relevant data
  --   from source systems (11i and CRDB) to the future Oracle 12 platform.
  --   Attribute which needs to be converted are:
  --
  --   Project Expenditure Groups
  --   Project Expenditures
  --   Project Expenditure Items
  --   Project Expenditure Cost Distributions
  --
  -- =============================================================================
  --
  -- -----------------------------------------------------------------------------
  --  Called By: Concurrent Program Eaton Project Expenditures Conversion Program
  -- -----------------------------------------------------------------------------
  -- -----------------------------------------------------------------------------
  --
  --  Input Parameters :
  --    piv_run_mode        : Controls the program excution. PRE-VALIDATE,VALIDATE,CONVERSION,RECONCILE
  --    piv_hidden_param1   : Hidden parameter used to enable/disable pin_batch_id
  --                          basing on piv_run_mode
  --    pin_batch_id        : List all unique batches from staging table , this
  --                          will be NULL for first Conversion Run.
  --    piv_hidden_param    : Hidden parameter used to enable/disable piv_process_records
  --                          based on pin_batch_id value
  --    piv_process_records : Conditionally available only when pin_batch_id is
  --                          populated. Otherwise this will be disabled
  --
  --  Output Parameters :
  --    pov_errbuff         : Standard output parameter with Return Message for concurrent program
  --    pon_retcode         : Standard output parameter with Return Code for concurrent program
  --
  --  Return                : Not applicable
  -- -----------------------------------------------------------------------------

  PROCEDURE process_expenditures_p(pov_errbuff         OUT VARCHAR2,
                                   pon_retcode         OUT NUMBER,
                                   piv_run_mode        IN VARCHAR2 -- Program Run Mode
                                  ,
                                   piv_hidden_param1   IN VARCHAR2 -- Dummy/Hidden Parameter 1
                                  ,
                                   pin_batch_id        IN NUMBER -- Program Batch Id
                                  ,
                                   piv_hidden_param    IN VARCHAR2 -- Dummy/Hidden Parameter
                                  ,
                                   piv_process_records IN VARCHAR2 -- Records to Process
                                   ) IS
    l_init_err VARCHAR2(200) DEFAULT NULL;
    l_normal_excep EXCEPTION;
    l_warn_excep   EXCEPTION;
    l_err_excep    EXCEPTION;

    l_new_cnt           NUMBER DEFAULT 0;
    l_print_ret_stats   VARCHAR2(1) DEFAULT 'S';
    l_print_err_msg     VARCHAR2(2000);
    l_pre_val_ret_stats VARCHAR2(1) DEFAULT 'S';
    l_pre_val_err_msg   VARCHAR2(2000);
    l_val_ret_stats     VARCHAR2(1) DEFAULT 'S';
    l_val_err_msg       VARCHAR2(2000);
    l_imp_ret_stats     VARCHAR2(1) DEFAULT 'S';
    l_imp_err_msg       VARCHAR2(2000);
    l_assign_ret_stats  VARCHAR2(1) DEFAULT 'S';
    l_assign_err_msg    VARCHAR2(2000);
    l_load_ret_stats    VARCHAR2(1) DEFAULT 'S';
    l_load_err_msg      VARCHAR2(2000);
    l_success_count     NUMBER;

  BEGIN

    -- Print Concurrent Program Parameters
    print_log_message_p('Program Parameters.....................');
    print_log_message_p('---------------------------------------');
    print_log_message_p('Run Mode            : ' || piv_run_mode);
    print_log_message_p('Batch ID            : ' || pin_batch_id);
    print_log_message_p('Process records     : ' || piv_process_records);
    print_log_message_p('---------------------------------------');

    -- Initialize global variables with parameter values
    g_run_mode        := piv_run_mode;
    g_batch_id        := pin_batch_id;
    g_process_records := piv_process_records;

    /** Initialization of Debug Framework **/
    xxetn_debug_pkg.initialize_debug(pov_err_msg      => l_init_err,
                                     piv_program_name => 'XXPA_PROJ_EXPEND_CONV');

    -- Error in Debug Initialization
    IF l_init_err IS NOT NULL THEN
      pov_errbuff := 'Error in Debug Initialization. Error: ' || l_init_err;
      RAISE l_err_excep;
    END IF;

    debug_msg_p('STEP:1',
                'PROCESS_EXPENDITURES_P',
                'In Begin of PROCESS_EXPENDITURES_P');

    -- Run Mode is Load-Data
    IF g_run_mode = 'LOAD-DATA' THEN

      debug_msg_p('STEP:2',
                  'PROCESS_EXPENDITURES_P',
                  'Inside Load-Data mode');

      -- Calling Load-Data Procedure
      load_expend_data_p(pov_ret_stats => l_load_ret_stats,
                         pov_err_msg   => l_load_err_msg);

      -- Print Report
      print_stats_p(pov_ret_stats        => l_print_ret_stats,
                    pov_err_msg          => l_print_err_msg,
                    pin_total_count      => g_total_count,
                    pin_success_count    => g_total_count - g_failed_count,
                    pin_failed_count     => g_failed_count,
                    pin_failed_count_int => NULL,
                    pin_failed_count_imp => NULL);

      IF l_load_ret_stats <> 'S' THEN
        -- Log messages are already written
        pov_errbuff := l_load_err_msg;
        RAISE l_warn_excep;
      END IF;

      -- Run Mode is Pre-Validation
    ELSIF g_run_mode = 'PRE-VALIDATE' THEN
      debug_msg_p('STEP:3',
                  'PROCESS_EXPENDITURES_P',
                  'Inside Pre-Validate Mode');

      -- Calling Pre-Validate Procedure
      pre_validate_expend_p(pov_ret_stats => l_pre_val_ret_stats,
                            pov_err_msg   => l_pre_val_err_msg);

      fnd_file.put_line(fnd_file.output,
                        'Program Name : Eaton Project Expenditures Conversion Program');
      fnd_file.put_line(fnd_file.output,
                        'Request ID   : ' || TO_CHAR(g_request_id));
      fnd_file.put_line(fnd_file.output,
                        'Report Date  : ' ||
                        TO_CHAR(SYSDATE, 'DD-MON-RRRR HH24:MI:SS'));
      fnd_file.put_line(fnd_file.output,
                        '=============================================================================================');
      fnd_file.put_line(fnd_file.output, CHR(10));
      fnd_file.put_line(fnd_file.output, 'Parameters');
      fnd_file.put_line(fnd_file.output,
                        '---------------------------------------------');
      fnd_file.put_line(fnd_file.output,
                        'Run Mode            : ' || g_run_mode);
      fnd_file.put_line(fnd_file.output,
                        'Batch ID            : ' || g_batch_id);
      fnd_file.put_line(fnd_file.output,
                        'Process records     : ' || g_process_records);
      fnd_file.put_line(fnd_file.output, CHR(10));
      fnd_file.put_line(fnd_file.output,
                        '=============================================================================================');

      -- Pre-validation fails
      IF l_pre_val_ret_stats <> 'S' THEN
        fnd_file.put_line(fnd_file.output,
                          'Pre-Validations for Project Expenditures failed with below errors:');
        fnd_file.put_line(fnd_file.output,
                          'Error Message: ' || l_pre_val_err_msg);
        fnd_file.put_line(fnd_file.output,
                          '=============================================================================================');
        pov_errbuff := 'Some or all of Pre-Validations failed for Project Expenditures.';
        RAISE l_warn_excep;
      ELSE
        fnd_file.put_line(fnd_file.output,
                          'Pre-Validations for Project Expenditures completed successfully');
        fnd_file.put_line(fnd_file.output,
                          '=============================================================================================');
      END IF;

      -- Run Mode is Validation
    ELSIF piv_run_mode = 'VALIDATE' THEN
      debug_msg_p('STEP:4',
                  'PROCESS_EXPENDITURES_P',
                  'Inside Validate Mode');

      IF g_batch_id IS NULL THEN
        g_new_rec := 'N';

        -- Check New (loaded) records for which Batch Id is NULL
        BEGIN
          SELECT COUNT(1)
            INTO l_new_cnt
            FROM xxpa_proj_expend_stg xpes
           WHERE batch_id IS NULL;
        EXCEPTION
          WHEN OTHERS THEN
            pov_errbuff := 'SQL Error in getting Record Count for which Batch Id is NULL ' ||
                           SQLERRM;
            RAISE l_err_excep;
        END;

        -- If New records exist for Batch Id Updation
        IF l_new_cnt > 0 THEN
          -- Generate New Batch Id and Run Sequence Id
          g_new_batch_id   := xxetn_batches_s.NEXTVAL;
          g_new_run_seq_id := xxetn_run_sequences_s.NEXTVAL;

          -- Assign Batch Id and Run Sequence Id to New Records
          assign_batch_id_p(pov_ret_stats => l_assign_ret_stats,
                            pov_err_msg   => l_assign_err_msg);

          IF l_assign_ret_stats <> 'S' THEN
            -- error in assign_batch_id proc
            pov_errbuff := l_assign_err_msg;
            RAISE l_err_excep;
          END IF;

          -- If NO New records exist for Batch Id Updation
        ELSE

          debug_msg_p('STEP:5',
                      'PROCESS_EXPENDITURES_P',
                      'Printing Stats Report to display 0 records');

          -- Print Stats in Report Output and Exit
          print_stats_p(pov_ret_stats        => l_print_ret_stats,
                        pov_err_msg          => l_print_err_msg,
                        pin_total_count      => 0,
                        pin_success_count    => 0,
                        pin_failed_count     => 0,
                        pin_failed_count_int => NULL,
                        pin_failed_count_imp => NULL);

          IF l_print_ret_stats <> 'S' THEN
            -- error in print_stats_p proc
            pov_errbuff := l_print_err_msg;
            RAISE l_err_excep;
          END IF;

          -- Program ends normally
          RAISE l_normal_excep;
        END IF;

      ELSE
        -- if g_batch_id is not NULL
        IF g_process_records = 'ALL' THEN
          g_new_rec := g_new;
          g_err_rec := g_error;
          g_val_rec := g_validated;

        ELSIF g_process_records = 'ERROR' THEN
          g_err_rec := g_error;

        ELSIF g_process_records = 'UNPROCESSED' THEN
          g_new_rec := g_new;
        END IF;

      END IF;

      debug_msg_p('STEP:6',
                  'PROCESS_EXPENDITURES_P',
                  'Validating Expenditures');

      /**** Call Validate Expenditures Procedure ****/
      validate_expend_p(pov_ret_stats => l_val_ret_stats,
                        pov_err_msg   => l_val_err_msg);

      debug_msg_p('STEP:6.1.1',
                  'PROCESS_EXPENDITURES_P',
                  'l_val_ret_stats: ' || l_val_ret_stats);

      debug_msg_p('STEP:6.1.2',
                  'PROCESS_EXPENDITURES_P',
                  'l_val_err_msg: ' || l_val_err_msg);

      -- Print Stats Report
      print_stats_p(pov_ret_stats        => l_print_ret_stats,
                    pov_err_msg          => l_print_err_msg,
                    pin_total_count      => g_total_count,
                    pin_success_count    => g_total_count - g_failed_count,
                    pin_failed_count     => g_failed_count,
                    pin_failed_count_int => NULL,
                    pin_failed_count_imp => NULL);

      IF l_val_ret_stats <> 'S' THEN
        -- error in validate_expend_p proc
        pov_errbuff := l_val_err_msg;
        RAISE l_warn_excep;
      END IF;

      -- Run Mode is Conversion
    ELSIF g_run_mode = 'CONVERSION' THEN
      debug_msg_p('STEP:7', 'PROCESS_EXPEND_P', 'Inside Conversion Mode');

      IF g_batch_id IS NULL THEN
        pov_errbuff := 'ERROR: Please select a value for Batch Id Parameter when Run Mode is Conversion';
        RAISE l_err_excep;
      END IF;

      import_expend_p(pov_ret_stats => l_imp_ret_stats,
                      pov_err_msg   => l_imp_err_msg);

      -- Print Stats Report
      print_stats_p(pov_ret_stats        => l_print_ret_stats,
                    pov_err_msg          => l_print_err_msg,
                    pin_total_count      => g_total_count,
                    pin_success_count    => g_total_count - g_failed_count,
                    pin_failed_count     => g_failed_count,
                    pin_failed_count_int => NULL,
                    pin_failed_count_imp => NULL);

      IF l_imp_ret_stats <> 'S' THEN
        -- error in import_expend_p proc
        pov_errbuff := l_imp_err_msg;
        RAISE l_warn_excep;
      END IF;

      -- Run Mode is Reconcile
    ELSIF g_run_mode = 'RECONCILE' THEN

      debug_msg_p('STEP:8',
                  'PROCESS_EXPENDITURES_P',
                  'Inside Reconcile Mode');

      -- Get Total Count of Processed Records
      SELECT COUNT(1)
        INTO g_total_count
        FROM xxpa_proj_expend_stg xpes
       WHERE xpes.batch_id = NVL(g_batch_id, xpes.batch_id)
         AND xpes.batch_id IS NOT NULL
         AND xpes.run_sequence_id IS NOT NULL;

      -- Get Total Count of Failed Records in Validation
      SELECT COUNT(1)
        INTO g_failed_count
        FROM xxpa_proj_expend_stg xpes
       WHERE xpes.batch_id = NVL(g_batch_id, xpes.batch_id)
         AND xpes.process_flag = g_error
         AND xpes.error_type = g_val_err_type
         AND xpes.batch_id IS NOT NULL
         AND xpes.run_sequence_id IS NOT NULL;

      -- Get Total Count of Failed Records in Interface
      SELECT COUNT(1)
        INTO g_failed_count_int
        FROM xxpa_proj_expend_stg xpes
       WHERE xpes.batch_id = NVL(g_batch_id, xpes.batch_id)
         AND xpes.process_flag = g_error
         AND xpes.error_type = g_int_err_type
         AND xpes.batch_id IS NOT NULL
         AND xpes.run_sequence_id IS NOT NULL;

      -- Get Total Count of Failed Records in Import/Conversion
      SELECT COUNT(1)
        INTO g_failed_count_imp
        FROM xxpa_proj_expend_stg xpes
       WHERE xpes.batch_id = NVL(g_batch_id, xpes.batch_id)
         AND xpes.process_flag = g_error
         AND xpes.error_type = g_imp_err_type
         AND xpes.batch_id IS NOT NULL
         AND xpes.run_sequence_id IS NOT NULL;

      -- Get Total Count of Imported/Converted Records
      SELECT COUNT(1)
        INTO l_success_count
        FROM xxpa_proj_expend_stg xpes
       WHERE xpes.batch_id = NVL(g_batch_id, xpes.batch_id)
         AND xpes.process_flag = g_converted
         AND xpes.batch_id IS NOT NULL
         AND xpes.run_sequence_id IS NOT NULL;

      -- Print Stats Report
      print_stats_p(pov_ret_stats        => l_print_ret_stats,
                    pov_err_msg          => l_print_err_msg,
                    pin_total_count      => g_total_count,
                    pin_success_count    => l_success_count,
                    pin_failed_count     => g_failed_count,
                    pin_failed_count_int => g_failed_count_int,
                    pin_failed_count_imp => g_failed_count_imp);

    END IF;

    debug_msg_p('STEP:9', 'PROCESS_EXPENDITURES_P', 'End of Program');

  EXCEPTION
    WHEN l_normal_excep THEN
      debug_msg_p('STEP:10',
                  'PROCESS_EXPENDITURES_P',
                  'Normal Exception. Program Completed');
    WHEN l_warn_excep THEN
      pon_retcode := 1;
      debug_msg_p('STEP:11', 'PROCESS_EXPENDITURES_P', pov_errbuff);
    WHEN l_err_excep THEN
      pon_retcode := 2;
      fnd_file.put_line(fnd_file.LOG, pov_errbuff);
      debug_msg_p('STEP:12', 'PROCESS_EXPENDITURES_P', pov_errbuff);
    WHEN OTHERS THEN
      pon_retcode := 2;
      pov_errbuff := 'Error: Main Procedure: PROCESS_EXPENDITURES_P. Reason: ' ||
                     SUBSTR(SQLERRM, 1, 250);
      fnd_file.put_line(fnd_file.LOG, pov_errbuff);
      debug_msg_p('STEP:13', 'PROCESS_EXPENDITURES_P', pov_errbuff);

  END process_expenditures_p;

  --
  -- =============================================================================
  -- Procedure: tieback_p
  -- =============================================================================
  --   This is main procedure invoked by Project Expenditures Tieback Conversion Program
  --
  --   This concurrent program is used to Tie Back any errors in standard import
  --   It also updates process flag in staging table after import
  --
  -- =============================================================================
  --
  -- -----------------------------------------------------------------------------
  --  Called By: Concurrent Program Eaton Project Expenditures Tieback Conversion Program
  -- -----------------------------------------------------------------------------
  -- -----------------------------------------------------------------------------
  --
  --  Input Parameters :
  --   pin_batch_id        : Batch Id
  --
  --  Output Parameters :
  --   pov_errbuff         : Standard output parameter with Return Message for concurrent program
  --   pon_retcode         : Standard output parameter with Return Code for concurrent program
  --
  --  Return               : Not applicable
  -- -----------------------------------------------------------------------------
  --

  PROCEDURE tieback_p(pov_errbuff  OUT NOCOPY VARCHAR2,
                      pon_retcode  OUT NOCOPY NUMBER,
                      pin_batch_id IN NUMBER) IS

    l_init_err VARCHAR2(200) DEFAULT NULL;
    l_normal_excep EXCEPTION;
    l_warn_excep   EXCEPTION;
    l_err_excep    EXCEPTION;

    -- Error Table Record Type
    source_rec xxetn_common_error_pkg.g_source_rec_type;

    l_log_ret_stats   VARCHAR2(1) DEFAULT 'S';
    l_log_err_msg     VARCHAR2(2000);
    l_print_ret_stats VARCHAR2(1) DEFAULT 'S';
    l_print_err_msg   VARCHAR2(2000);

    --Cursor to fetch PA Transaction Error Details
    CURSOR pa_trxn_err_cur IS
      SELECT ptia.project_number,
             ptia.task_number,
             ptia.transaction_source,
             ptia.transaction_rejection_code,
             pl.lookup_code,
             pl.meaning,
             ptia.project_id,
             ptia.task_id
             --,ptia.txn_interface_id
            ,
             ptia.expenditure_id,
             ptia.expenditure_item_id,
             xpes.interface_txn_id,
             xpes.orig_transaction_reference
        FROM pa_transaction_interface_all ptia,
             xxpa_proj_expend_stg         xpes,
             pa_lookups                   pl
       WHERE xpes.project_id = ptia.project_id
         AND xpes.task_id = ptia.task_id
         AND xpes.orig_transaction_reference =
             ptia.orig_transaction_reference
         AND xpes.txn_interface_id = ptia.txn_interface_id
         AND xpes.process_flag = g_interfaced
         AND xpes.batch_id = pin_batch_id
         AND ptia.transaction_status_code IN ('R', 'PR', 'PO')
         AND ((ptia.transaction_rejection_code IS NULL) OR
             (pl.lookup_type IN
             ('TRANSACTION REJECTION REASON',
                'FC_RESULT_CODE',
                'COST DIST REJECTION CODE',
                'INVOICE_CURRENCY',
                'TRANSACTION USER REJ REASON')))
         AND pl.lookup_code(+) = ptia.transaction_rejection_code
       ORDER BY xpes.interface_txn_id;

    --Cursor to fetch PA Transaction Success Details
    CURSOR pa_trxn_suc_cur IS
      SELECT xpes.interface_txn_id,
             xpes.orig_transaction_reference,
             peia.expenditure_id,
             peia.expenditure_item_id
        FROM pa_expenditure_items_all peia, xxpa_proj_expend_stg xpes
       WHERE xpes.project_id = peia.project_id
         AND xpes.task_id = peia.task_id
         AND xpes.orig_transaction_reference =
             peia.orig_transaction_reference
         AND xpes.process_flag IN (g_interfaced, g_error)
         AND xpes.batch_id = pin_batch_id;

  BEGIN

    g_batch_id := pin_batch_id;

    /** Initialization of Debug Framework **/
    xxetn_debug_pkg.initialize_debug(pov_err_msg      => l_init_err,
                                     piv_program_name => 'XXPA_PROJ_EXPEND_TIEBACK_CONV');

    -- Error in Debug Initialization
    IF l_init_err IS NOT NULL THEN
      pov_errbuff := 'Error in Debug Initialization. Error: ' || l_init_err;
      RAISE l_err_excep;
    END IF;

    debug_msg_p('STEP:1', 'TIEBACK_P', 'In Begin of TIEBACK_P');

    -- Intialize Common variables for Record Type
    g_new_run_seq_id := xxetn_run_sequences_s.NEXTVAL;

    xxetn_common_error_pkg.g_run_seq_id := g_new_run_seq_id;
    xxetn_common_error_pkg.g_batch_id   := g_batch_id; -- batch id
    source_rec.source_column_name       := NULL;
    source_rec.source_column_value      := NULL;
    source_rec.error_type               := g_imp_err_type;

    -- Open Cursor for Failed Records
    FOR pa_trxn_err_rec IN pa_trxn_err_cur LOOP

      debug_msg_p('STEP:2',
                  'TIEBACK_P',
                  'Failed Record#: ' || pa_trxn_err_rec.interface_txn_id);

      g_failed_count := g_failed_count + 1;

      source_rec.interface_staging_id := pa_trxn_err_rec.interface_txn_id;
      source_rec.error_code           := pa_trxn_err_rec.lookup_code;
      source_rec.error_message        := pa_trxn_err_rec.meaning;

      log_errors_p(pov_ret_stats => l_log_ret_stats,
                   pov_err_msg   => l_log_err_msg,
                   pi_err_rec    => source_rec);

      IF l_log_ret_stats <> 'S' THEN
        print_log_message_p('Not able to insert error details in Common Error Table for record: ' ||
                            pa_trxn_err_rec.interface_txn_id || 'Error: ' ||
                            l_log_err_msg);
      END IF;

      -- Update Staging Table with Errored Records Status
      UPDATE xxpa_proj_expend_stg xpes
         SET xpes.process_flag      = g_error,
             xpes.run_sequence_id   = g_new_run_seq_id,
             xpes.error_type        = g_imp_err_type,
             xpes.last_update_date  = SYSDATE,
             xpes.last_update_login = g_last_update_login,
             xpes.last_updated_by   = g_last_updated_by,
             xpes.request_id        = g_request_id
             --,xpes.txn_interface_id = pa_trxn_err_rec.txn_interface_id
            ,
             xpes.expenditure_id      = pa_trxn_err_rec.expenditure_id,
             xpes.expenditure_item_id = pa_trxn_err_rec.expenditure_item_id
       WHERE xpes.interface_txn_id = pa_trxn_err_rec.interface_txn_id
         AND xpes.batch_id = g_batch_id
         AND xpes.process_flag = g_interfaced;

    END LOOP;

    COMMIT;

    -- Open Cursor for Success Records
    FOR pa_trxn_suc_rec IN pa_trxn_suc_cur LOOP

      debug_msg_p('STEP:3',
                  'TIEBACK_P',
                  'Success Record: ' || pa_trxn_suc_rec.interface_txn_id);
      g_success_count := g_success_count + 1;

      -- Update Staging Table with Success Records Status
      UPDATE xxpa_proj_expend_stg xpes
         SET xpes.process_flag        = g_converted,
             xpes.run_sequence_id     = g_new_run_seq_id,
             xpes.error_type          = NULL,
             xpes.last_update_date    = SYSDATE,
             xpes.last_update_login   = g_last_update_login,
             xpes.last_updated_by     = g_last_updated_by,
             xpes.request_id          = g_request_id,
             xpes.expenditure_id      = pa_trxn_suc_rec.expenditure_id,
             xpes.expenditure_item_id = pa_trxn_suc_rec.expenditure_item_id
       WHERE xpes.interface_txn_id = pa_trxn_suc_rec.interface_txn_id
         AND xpes.batch_id = g_batch_id
         AND xpes.process_flag IN (g_interfaced, g_error);

    END LOOP;

    COMMIT;

    -- Insert remaining errors into Error Table
    IF g_source_tab.COUNT > 0 THEN

      debug_msg_p('STEP:4',
                  'TIEBACK_P',
                  'Error Table Type Count: ' || g_source_tab.COUNT);
      debug_msg_p('STEP:5',
                  'TIEBACK_P',
                  'Logging Remaining Errors in Error table');

      g_err_cnt := 1;
      xxetn_common_error_pkg.add_error(pov_return_status => l_log_ret_stats,
                                       pov_error_msg     => l_log_err_msg,
                                       pi_source_tab     => g_source_tab);

      IF l_log_ret_stats <> 'S' THEN
        print_log_message_p('Not able to insert error details in Common Error Table: ' ||
                            'Error: ' || l_log_err_msg);
      END IF;

      -- Flushing PLSQL Table
      g_source_tab.DELETE;

    END IF;

    g_total_count := g_success_count + g_failed_count;
    debug_msg_p('STEP:6',
                'TIEBACK_P',
                'Processed Records during Tieback: ' || g_total_count);

    debug_msg_p('STEP:7',
                'TIEBACK_P',
                'Failed Records during Tieback: ' || g_failed_count);

    -- Print Stats Report
    print_stats_tieback_p(pov_ret_stats     => l_print_ret_stats,
                          pov_err_msg       => l_print_err_msg,
                          pin_total_count   => g_total_count,
                          pin_success_count => g_success_count,
                          pin_failed_count  => g_failed_count);

    IF l_print_ret_stats <> 'S' THEN
      pov_errbuff := l_print_err_msg;
      RAISE l_warn_excep;
    END IF;

    IF g_failed_count > 0 THEN
      pov_errbuff := 'Some Expenditures Failed to Import. Program Completing in Warning'; -- Program must complete in Warning if any Failed records encountered in TIEBACK
      RAISE l_warn_excep;
    END IF;

  EXCEPTION
    WHEN l_normal_excep THEN
      debug_msg_p('STEP:8',
                  'TIEBACK_P',
                  'Normal Exception. Program Completed');
    WHEN l_warn_excep THEN
      pon_retcode := 1;
      debug_msg_p('STEP:9', 'TIEBACK_P', pov_errbuff);
    WHEN l_err_excep THEN
      pon_retcode := 2;
      fnd_file.put_line(fnd_file.LOG, pov_errbuff);
      debug_msg_p('STEP:10', 'TIEBACK_P', pov_errbuff);
    WHEN OTHERS THEN
      pon_retcode := 2;
      pov_errbuff := 'Error: Main Procedure: TIEBACK_P. Reason: ' ||
                     SUBSTR(SQLERRM, 1, 250);
      fnd_file.put_line(fnd_file.LOG, pov_errbuff);
      debug_msg_p('STEP:11', 'TIEBACK_P', pov_errbuff);
      ROLLBACK;

  END tieback_p;

END xxpa_proj_expend_cnv_pkg;
/

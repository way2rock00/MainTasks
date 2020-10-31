--Begin Revision History
--<<<
-- 02-Jun-2017 01:08:53 C9914192 /main/16
-- 
--<<<
-- 18-Jul-2017 02:30:33 E0406765 /main/17
-- 
-- <<<
--End Revision History  
create or replace package body xxpo_poreceipt_cnv_pkg as

  ------------------------------------------------------------------------------------------
  --    Owner        : EATON CORPORATION.
  --    Application  : Purchasing
  --    Schema       : APPS
  --    Compile AS   : APPS
  --    File Name    : xxpo_poreceipt_SM_pkg.pkb
  --    Date         : 01-JUN-2014
  --    Author       : Rohit Devadiga
  --    Description  : Package specification for Purchase Order Receipt Conversion
  --
  --    Version      : $ETNHeader: /CCSTORE/ccweb/E0406765/E0406765_view_XXPO_TOP/vobs/PO_TOP/xxpo/12.0.0/install/xxpo_poreceipt_cnv_pkg.pkb /main/17 18-Jul-2017 02:30:33 E0406765  $
  --
  --    Parameters  :
  --    piv_run_mode        : Control the program excution for VALIDATE
  --
  --    pin_batch_id        : List all unique batches from staging table , this will
  --                        be NULL for first Conversion Run.
  --    piv_process_records : Conditionally available only when pin_batch_id is popul-
  --                        -ated. Otherwise this will be disabled and defaulted
  --                        to ALL
  --    Change History
  --  ======================================================================================
  --    v1.0        Rohit Devadiga    01-JUN-2014     Initial Creation
  --    v1.1        Rohit Devadiga    12-DEC-2014     Changes done for CR# 263626
  --    v1.2        Shriram Phenani   23-JUL-2015     DEFECT 2838 / 2871
  --    v1.3        Kulraj Singh      31-JUL-2015     Performance Tuning changes. Refer vesion#
  --    v1.4        Shriram Phenani   08-JAN-2016     ALM DEFECT 4797
  --    v1.5        Shishir Mohan     12-JAN-2016     Changes for leg_uom_code
  --    v1.6        Ankur Sharma      20-JUN-2016     Changes for Parallel Processing run.
  --    v1.7        Aditya Bhagat     16-March-2017   Changes to exchange rate type
  --    ====================================================================================
  ------------------------------------------------------------------------------------------
  g_request_id number default fnd_global.conc_request_id;

  g_prog_appl_id number default fnd_global.prog_appl_id;

  g_conc_program_id number default fnd_global.conc_program_id;

  g_user_id number default fnd_global.user_id;

  g_login_id number default fnd_global.login_id;

  g_created_by number := apps.fnd_global.user_id;

  g_last_updated_by number := apps.fnd_global.user_id;

  g_last_update_login number := apps.fnd_global.login_id;

  g_tab xxetn_common_error_pkg.g_source_tab_type;

  g_err_tab_limit number default fnd_profile.value('ETN_FND_ERROR_TAB_LIMIT');

  g_ou_lookup fnd_lookup_types_tl.lookup_type%type := 'ETN_COMMON_OU_MAP';

  g_err_cnt number default 1;

  g_run_mode varchar2(100);

  g_batch_id number;

  g_process_records varchar2(100);

  g_leg_operating_unit varchar2(240);

  g_new_run_seq_id number;

  g_retcode number;

  g_total_count number;

  g_failed_count number;

  g_loaded_count number;

  g_transaction_date date;

  -- ========================
  -- Procedure: print_log_message
  -- =============================================================================
  --   This procedure is used to write message to log file.
  -- =============================================================================
  procedure print_log_message(piv_message in varchar2) is
  begin
    if nvl(g_request_id, 0) > 0 then
      fnd_file.put_line(fnd_file.log, piv_message);
    end if;
  end print_log_message;

  --
  -- ========================
  -- Procedure: LOG_ERRORS
  -- =============================================================================
  --   This procedure is used log error
  -- =============================================================================
  --
  procedure log_errors(pin_interface_txn_id    in number default null,
                       piv_source_table        in varchar2 default null,
                       piv_source_column_name  in varchar2 default null,
                       piv_source_column_value in varchar2 default null,
                       piv_error_type          in varchar2 default null,
                       piv_source_keyname1     in varchar2 default null,
                       piv_source_keyvalue1    in varchar2 default null,
                       piv_source_keyname2     in varchar2 default null,
                       piv_source_keyvalue2    in varchar2 default null,
                       piv_error_code          in varchar2,
                       piv_error_message       in varchar2) is
    pov_ret_stats varchar2(100);
    pov_err_msg   varchar2(1000);
  begin
    pov_ret_stats := 'S';
    pov_err_msg   := null;
    -- Assigning error values to current table record
    g_tab(g_err_cnt).interface_staging_id := pin_interface_txn_id;
    g_tab(g_err_cnt).source_table := piv_source_table;
    g_tab(g_err_cnt).source_column_name := piv_source_column_name;
    g_tab(g_err_cnt).source_column_value := piv_source_column_value;
    g_tab(g_err_cnt).source_keyname1 := piv_source_keyname1;
    g_tab(g_err_cnt).source_keyvalue1 := piv_source_keyvalue1;
    g_tab(g_err_cnt).source_keyname2 := piv_source_keyname2;
    g_tab(g_err_cnt).source_keyvalue2 := piv_source_keyvalue2;
    g_tab(g_err_cnt).error_type := piv_error_type;
    g_tab(g_err_cnt).error_code := piv_error_code;
    g_tab(g_err_cnt).error_message := piv_error_message;
    if g_err_cnt >= g_err_tab_limit then
      -- if Table Type Error Count exceeds limit
      g_err_cnt := 1;
      xxetn_common_error_pkg.add_error(pov_return_status => pov_ret_stats,
                                       pov_error_msg     => pov_err_msg,
                                       pi_source_tab     => g_tab);
      -- Flushing PLSQL Table
      g_tab.delete;
    else
      g_err_cnt := g_err_cnt + 1;
      -- else increment Table Type Error Count
    end if;
  exception
    when others then
      print_log_message('Error occured while logging error: ' ||
                        pov_ret_stats || ': ' || sqlerrm || ': ' ||
                        pov_err_msg);
  end log_errors;

  --
  -- ========================
  -- Procedure: ASSIGN_BATCH_ID
  -- =====================================================================================
  --   This procedure is used for batch id assignment
  -- =====================================================================================
  procedure assign_batch_id is
  begin
    -- g_batch_id NULL is considered a fresh run
    if g_batch_id is null then
      g_batch_id := xxetn_batches_s.nextval;
      begin
        xxetn_debug_pkg.add_debug(piv_debug_msg => 'Generating new batch id for headers table');
        update xxpo_receipt_hdr_stg
           set batch_id               = g_batch_id,
               process_flag           = 'N',
               run_sequence_id        = g_new_run_seq_id,
               last_updated_date      = sysdate,
               last_updated_by        = g_user_id,
               last_update_login      = g_login_id,
               program_application_id = g_prog_appl_id,
               program_id             = g_conc_program_id
         where 1 = 1
           and batch_id is null
           and leg_operating_unit =
               nvl(g_leg_operating_unit, leg_operating_unit);
      exception
        when others then
          print_log_message('Error : Exception occured while updating new batch id in headers staging ' ||
                            substr(sqlerrm, 1, 150));
      end;
      begin
        xxetn_debug_pkg.add_debug(piv_debug_msg => 'Generating new batch id for txn table');
        update xxpo_receipt_trx_stg
           set batch_id               = g_batch_id,
               process_flag           = 'N',
               run_sequence_id        = g_new_run_seq_id,
               last_updated_date      = sysdate,
               last_updated_by        = g_user_id,
               last_update_login      = g_login_id,
               program_application_id = g_prog_appl_id,
               program_id             = g_conc_program_id
         where 1 = 1
           and batch_id is null
           and leg_operating_unit =
               nvl(g_leg_operating_unit, leg_operating_unit);
      exception
        when others then
          print_log_message('Error : Exception occured while updating new batch id in txn staging ' ||
                            substr(sqlerrm, 1, 150));
      end;
      commit;
    else
      begin
        xxetn_debug_pkg.add_debug(piv_debug_msg => 'Reprocess updating run sequence id: Header table');
        update xxpo_receipt_hdr_stg
           set process_flag           = 'N',
               run_sequence_id        = g_new_run_seq_id,
               error_type             = null,
               last_updated_date      = sysdate,
               last_updated_by        = g_user_id,
               last_update_login      = g_login_id,
               program_application_id = g_prog_appl_id,
               program_id             = g_conc_program_id
         where 1 = 1
           and batch_id = g_batch_id
           and leg_operating_unit =
               nvl(g_leg_operating_unit, leg_operating_unit)
           and (g_process_records = 'ALL' and
               (process_flag not in ('C', 'X', 'P')) or
               (g_process_records = 'ERROR' and (process_flag = 'E')) or
               g_process_records = 'UNPROCESSED' and (process_flag = 'N'));
      exception
        when others then
          print_log_message('Error : Exception occured while updating run seq id for reprocess of Header table: ' ||
                            substr(sqlerrm, 1, 150));
      end;
      begin
        xxetn_debug_pkg.add_debug(piv_debug_msg => 'Reprocess updating run sequence id: txn table');
        update xxpo_receipt_trx_stg
           set process_flag           = 'N',
               run_sequence_id        = g_new_run_seq_id,
               error_type             = null,
               last_updated_date      = sysdate,
               last_updated_by        = g_user_id,
               last_update_login      = g_login_id,
               program_application_id = g_prog_appl_id,
               program_id             = g_conc_program_id
         where 1 = 1
           and batch_id = g_batch_id
           and leg_operating_unit =
               nvl(g_leg_operating_unit, leg_operating_unit)
           and (g_process_records = 'ALL' and
               (process_flag not in ('C', 'X', 'P')) or
               (g_process_records = 'ERROR' and (process_flag = 'E')) or
               g_process_records = 'UNPROCESSED' and (process_flag = 'N'));
      exception
        when others then
          print_log_message('Error : Exception occured while updating run seq id for reprocess of txn table: ' ||
                            substr(sqlerrm, 1, 150));
      end;
    end if;
    commit;
  exception
    when others then
      print_log_message('Error : Exception occured in assign batch id procedure: ' ||
                        substr(sqlerrm, 1, 150));
  end assign_batch_id;

  --
  -- ========================
  -- Procedure: ASSIGN_BATCH_ID
  -- =====================================================================================
  --   This procedure is used for batch id assignment  (modified for V1.3)
  -- =====================================================================================
  /**  PROCEDURE assign_batch_id IS

     l_new_rec VARCHAR2 (10) DEFAULT NULL;
     l_err_rec VARCHAR2 (10) DEFAULT NULL;
     l_val_rec VARCHAR2 (10) DEFAULT NULL;

  BEGIN

    /** Whole Procedure is modified for V1.3 **/
  -- g_batch_id NULL is considered a fresh run
  /**    IF g_batch_id IS NULL THEN
      g_batch_id := xxetn_batches_s.NEXTVAL;


      IF g_leg_operating_unit IS NULL THEN -- if Parameter operating unit is NULL


      BEGIN
        xxetn_debug_pkg.add_debug(piv_debug_msg => 'Generating new batch id for headers table');

        UPDATE xxpo_receipt_hdr_stg
           SET batch_id               = g_batch_id,
               process_flag           = 'N',
               run_sequence_id        = g_new_run_seq_id,
               last_updated_date      = SYSDATE,
               last_updated_by        = g_user_id,
              last_update_login      = g_login_id,
               program_application_id = g_prog_appl_id,
               program_id             = g_conc_program_id
         WHERE 1 = 1
         AND NVL(batch_id,-1) = -1;
      EXCEPTION
        WHEN OTHERS THEN
          print_log_message('Error : Exception occured while updating new batch id in headers staging ' ||
                            SUBSTR(SQLERRM, 1, 150));
      END;

      BEGIN
        xxetn_debug_pkg.add_debug(piv_debug_msg => 'Generating new batch id for txn table');

        UPDATE xxpo_receipt_trx_stg
           SET batch_id               = g_batch_id,
               process_flag           = 'N',
               run_sequence_id        = g_new_run_seq_id,
               last_updated_date      = SYSDATE,
               last_updated_by        = g_user_id,
               last_update_login      = g_login_id,
               program_application_id = g_prog_appl_id,
               program_id             = g_conc_program_id
         WHERE 1 = 1
         AND NVL(batch_id,-1) = -1;
      EXCEPTION
        WHEN OTHERS THEN
          print_log_message('Error : Exception occured while updating new batch id in txn staging ' ||
                            SUBSTR(SQLERRM, 1, 150));
      END;


      ELSE -- if Parameter operating unit is NOT NULL

      BEGIN
        xxetn_debug_pkg.add_debug(piv_debug_msg => 'Generating new batch id for headers table');

        UPDATE xxpo_receipt_hdr_stg
           SET batch_id               = g_batch_id,
               process_flag           = 'N',
               run_sequence_id        = g_new_run_seq_id,
               last_updated_date      = SYSDATE,
               last_updated_by        = g_user_id,
               last_update_login      = g_login_id,
               program_application_id = g_prog_appl_id,
               program_id             = g_conc_program_id
         WHERE 1 = 1
         AND NVL(batch_id,-1) = -1
         AND leg_operating_unit = g_leg_operating_unit;
      EXCEPTION
        WHEN OTHERS THEN
          print_log_message('Error : Exception occured while updating new batch id in headers staging ' ||
                            SUBSTR(SQLERRM, 1, 150));
      END;

      BEGIN
        xxetn_debug_pkg.add_debug(piv_debug_msg => 'Generating new batch id for txn table');

        UPDATE xxpo_receipt_trx_stg
           SET batch_id               = g_batch_id,
               process_flag           = 'N',
               run_sequence_id        = g_new_run_seq_id,
               last_updated_date      = SYSDATE,
               last_updated_by        = g_user_id,
               last_update_login      = g_login_id,
               program_application_id = g_prog_appl_id,
               program_id             = g_conc_program_id
         WHERE 1 = 1
         AND NVL(batch_id,-1) = -1
         AND leg_operating_unit = g_leg_operating_unit;
      EXCEPTION
        WHEN OTHERS THEN
          print_log_message('Error : Exception occured while updating new batch id in txn staging ' ||
                            SUBSTR(SQLERRM, 1, 150));
      END;

      END IF; -- if Parameter operating unit is NULL

      COMMIT;


    ELSE  -- If g_batch is NOT NULL

       IF g_process_records = 'ALL' THEN
          l_new_rec := 'N';
          l_err_rec := 'E';
          l_val_rec := 'V';
       ELSIF g_process_records = 'ERROR' THEN
          l_err_rec := 'E';
       ELSIF g_process_records = 'UNPROCESSED' THEN
          l_new_rec := 'N';
       END IF;

    IF g_leg_operating_unit IS NULL THEN -- if Parameter operating unit is NULL

      BEGIN
        xxetn_debug_pkg.add_debug(piv_debug_msg => 'Reprocess updating run sequence id: Header table');

        UPDATE xxpo_receipt_hdr_stg
           SET process_flag           = 'N',
              run_sequence_id        = g_new_run_seq_id,
               ERROR_TYPE             = NULL,
               last_updated_date      = SYSDATE,
               last_updated_by        = g_user_id,
               last_update_login      = g_login_id,
              program_application_id = g_prog_appl_id,
               program_id             = g_conc_program_id
         WHERE 1 = 1
         AND batch_id = g_batch_id
         AND process_flag IN (l_new_rec, l_err_rec, l_val_rec);
      EXCEPTION
        WHEN OTHERS THEN
          print_log_message('Error : Exception occured while updating run seq id for reprocess of Header table: ' ||
                            SUBSTR(SQLERRM, 1, 150));
      END;

      BEGIN
        xxetn_debug_pkg.add_debug(piv_debug_msg => 'Reprocess updating run sequence id: txn table');

        UPDATE xxpo_receipt_trx_stg
           SET process_flag           = 'N',
               run_sequence_id        = g_new_run_seq_id,
               ERROR_TYPE             = NULL,
              last_updated_date      = SYSDATE,
               last_updated_by        = g_user_id,
               last_update_login      = g_login_id,
               program_application_id = g_prog_appl_id,
               program_id             = g_conc_program_id
         WHERE 1 = 1
         AND batch_id = g_batch_id
         AND process_flag IN (l_new_rec, l_err_rec, l_val_rec);
      EXCEPTION
        WHEN OTHERS THEN
          print_log_message('Error : Exception occured while updating run seq id for reprocess of txn table: ' ||
                            SUBSTR(SQLERRM, 1, 150));
      END;


    ELSE -- if Parameter operating unit is NOT NULL

      BEGIN
        xxetn_debug_pkg.add_debug(piv_debug_msg => 'Reprocess updating run sequence id: Header table');

        UPDATE xxpo_receipt_hdr_stg
           SET process_flag           = 'N',
               run_sequence_id        = g_new_run_seq_id,
               ERROR_TYPE             = NULL,
               last_updated_date      = SYSDATE,
               last_updated_by        = g_user_id,
               last_update_login      = g_login_id,
               program_application_id = g_prog_appl_id,
               program_id             = g_conc_program_id
         WHERE 1 = 1
         AND batch_id = g_batch_id
         AND leg_operating_unit = g_leg_operating_unit
         AND process_flag IN (l_new_rec, l_err_rec, l_val_rec);
      EXCEPTION
        WHEN OTHERS THEN
          print_log_message('Error : Exception occured while updating run seq id for reprocess of Header table: ' ||
                            SUBSTR(SQLERRM, 1, 150));
      END;

      BEGIN
        xxetn_debug_pkg.add_debug(piv_debug_msg => 'Reprocess updating run sequence id: txn table');

        UPDATE xxpo_receipt_trx_stg
           SET process_flag           = 'N',
               run_sequence_id        = g_new_run_seq_id,
               ERROR_TYPE             = NULL,
               last_updated_date      = SYSDATE,
               last_updated_by        = g_user_id,
               last_update_login      = g_login_id,
               program_application_id = g_prog_appl_id,
               program_id             = g_conc_program_id
         WHERE 1 = 1
         AND batch_id = g_batch_id
         AND leg_operating_unit = g_leg_operating_unit
         AND process_flag IN (l_new_rec, l_err_rec, l_val_rec);
      EXCEPTION
        WHEN OTHERS THEN
          print_log_message('Error : Exception occured while updating run seq id for reprocess of txn table: ' ||
                            SUBSTR(SQLERRM, 1, 150));
      END;

    END IF; -- if Parameter operating unit is NULL

    END IF;

    COMMIT;

  EXCEPTION
    WHEN OTHERS THEN
      print_log_message('Error : Exception occured in assign batch id procedure: ' ||
                        SUBSTR(SQLERRM, 1, 150));
  END assign_batch_id; **/
  --
  -- ========================
  -- Procedure: PRE_VALIDATE
  -- =====================================================================================
  --   This procedure is used to validate set ups for FA
  -- =====================================================================================
  procedure pre_validate is
    ln_cat_count number := null;
  begin
    g_retcode := 0;
    --- OU lookup check
    begin
      print_log_message('Check for Common OU lookup');
      select 1
        into ln_cat_count
        from fnd_lookup_types flv
       where 1 = 1
         and upper(flv.lookup_type) = g_ou_lookup;
      print_log_message('Common OU lookup is present ');
    exception
      when no_data_found then
        print_log_message('Common OU lookup is missing : ' || g_ou_lookup);
        g_retcode := 1;
      when others then
        print_log_message('Error : Exception occured in Common OU lookup ' ||
                          substr(sqlerrm, 1, 240));
        g_retcode := 1;
    end;
  exception
    when others then
      print_log_message('Error : Exception occured in pre_validate procedure ' ||
                        substr(sqlerrm, 1, 240));
      g_retcode := 1;
  end pre_validate;

  -------------------------------------------------------------------------------------------------------------------------------
  -- TYPE                         :         PROCEDURE
  -- NAME                         :         PRINT_REPORT
  -- INPUT OUTPUT PARAMETERS      :
  -- INPUT PARAMETERS             :
  -- OUTPUT PARAMETERS            :
  -- PURPOSE                      :         This procedure is used to
  --                                        print the stats
  --
  -------------------------------------------------------------------------------------------------------------------------------
  procedure print_report is
    l_tot_header_count  number;
    l_tot_line_count    number;
    l_fail_header_count number;
    l_fail_line_count   number;
    l_fail_count_head   number;
    l_fail_count_line   number;
    l_suc_count_head    number;
    l_suc_count_line    number;
  begin
    -- Get Total Count of Records in header staging
    select count(1)
      into l_tot_header_count
      from xxpo_receipt_hdr_stg xds
     where xds.batch_id = nvl(g_batch_id, xds.batch_id)
       and xds.batch_id is not null
       and xds.run_sequence_id is not null;
    -- Get Total Count of Records in txn staging
    select count(1)
      into l_tot_line_count
      from xxpo_receipt_trx_stg xds
     where xds.batch_id = nvl(g_batch_id, xds.batch_id)
       and xds.batch_id is not null
       and xds.run_sequence_id is not null;
    -- Get Total Count of Failed Records in Validation header
    select count(1)
      into l_fail_header_count
      from xxpo_receipt_hdr_stg xds
     where xds.batch_id = nvl(g_batch_id, xds.batch_id)
       and xds.process_flag = 'E'
       and xds.error_type = 'ERR_VAL'
       and xds.batch_id is not null
       and xds.run_sequence_id is not null;
    -- Get Total Count of Failed Records in Validation txn
    select count(1)
      into l_fail_line_count
      from xxpo_receipt_trx_stg xds
     where xds.batch_id = nvl(g_batch_id, xds.batch_id)
       and xds.process_flag = 'E'
       and xds.error_type = 'ERR_VAL'
       and xds.batch_id is not null
       and xds.run_sequence_id is not null;
    -- Get Total Count of Failed Records in Conversion header
    select count(1)
      into l_fail_count_head
      from xxpo_receipt_hdr_stg xds
     where xds.batch_id = nvl(g_batch_id, xds.batch_id)
       and xds.process_flag = 'E'
       and xds.error_type = 'ERR_INT'
       and xds.batch_id is not null
       and xds.run_sequence_id is not null;
    -- Get Total Count of Failed Records in Conversion txn
    select count(1)
      into l_fail_count_line
      from xxpo_receipt_trx_stg xds
     where xds.batch_id = nvl(g_batch_id, xds.batch_id)
       and xds.process_flag = 'E'
       and xds.error_type = 'ERR_INT'
       and xds.batch_id is not null
       and xds.run_sequence_id is not null;
    -- Get Total Count of Converted Records header
    select count(1)
      into l_suc_count_head
      from xxpo_receipt_hdr_stg xds
     where xds.batch_id = nvl(g_batch_id, xds.batch_id)
       and xds.process_flag = 'C'
       and xds.batch_id is not null
       and xds.run_sequence_id is not null;
    -- Get Total Count of Converted Records txn
    select count(1)
      into l_suc_count_line
      from xxpo_receipt_trx_stg xds
     where xds.batch_id = nvl(g_batch_id, xds.batch_id)
       and xds.process_flag = 'C'
       and xds.batch_id is not null
       and xds.run_sequence_id is not null;
    xxetn_debug_pkg.add_debug(piv_debug_msg => 'Inside Print_report procedure');
    fnd_file.put_line(fnd_file.output,
                      'Program Name : Eaton Open PO Receipt Conversion Program ');
    fnd_file.put_line(fnd_file.output,
                      'Request ID   : ' || to_char(g_request_id));
    fnd_file.put_line(fnd_file.output,
                      'Report Date  : ' ||
                      to_char(sysdate, 'DD-MON-RRRR HH24:MI:SS'));
    fnd_file.put_line(fnd_file.output,
                      '=============================================================================================');
    fnd_file.put_line(fnd_file.output, chr(10));
    fnd_file.put_line(fnd_file.output, 'Parameters');
    fnd_file.put_line(fnd_file.output,
                      '---------------------------------------------');
    fnd_file.put_line(fnd_file.output,
                      'Run Mode            : ' || g_run_mode);
    fnd_file.put_line(fnd_file.output,
                      'Batch ID            : ' || g_batch_id);
    fnd_file.put_line(fnd_file.output,
                      'Process records     : ' || g_process_records);
    fnd_file.put_line(fnd_file.output, chr(10));
    fnd_file.put_line(fnd_file.output,
                      '=============================================================================================');
    fnd_file.put_line(fnd_file.output,
                      'Statistics (' || g_run_mode || '):');
    fnd_file.put_line(fnd_file.output,
                      '=============================================================================================');
    fnd_file.put_line(fnd_file.output,
                      'Total Records Header                 : ' ||
                      l_tot_header_count);
    fnd_file.put_line(fnd_file.output,
                      'Total Records transaction              : ' ||
                      l_tot_line_count);
    fnd_file.put_line(fnd_file.output,
                      'Records Converted Header         : ' ||
                      l_suc_count_head);
    fnd_file.put_line(fnd_file.output,
                      'Records Converted Transaction          : ' ||
                      l_suc_count_line);
    fnd_file.put_line(fnd_file.output,
                      'Records Erred in Validation for Header : ' ||
                      l_fail_header_count);
    fnd_file.put_line(fnd_file.output,
                      'Records Erred in Validation for Transaction  : ' ||
                      l_fail_line_count);
    fnd_file.put_line(fnd_file.output,
                      'Records Erred in Conversion for Header : ' ||
                      l_fail_count_head);
    fnd_file.put_line(fnd_file.output,
                      'Records Erred in Conversion for Transaction  : ' ||
                      l_fail_count_line);
    fnd_file.put_line(fnd_file.output,
                      '=============================================================================================');
  exception
    when others then
      fnd_file.put_line(fnd_file.log,
                        'ERROR : Error occured in print reports procedure' ||
                        substr(sqlerrm, 1, 150));
  end print_report;

  --
  -- ========================
  -- Procedure: LOAD_HEADER
  -- =============================================================================
  --   This procedure is used to load data from extraction into staging table
  -- =============================================================================
  procedure load_header(pov_ret_stats out nocopy varchar2,
                        pov_err_msg   out nocopy varchar2) is
    type leg_head_rec is record(
      interface_txn_id              xxpo_receipt_hdr_stg_r12.interface_txn_id%type,
      batch_id                      xxpo_receipt_hdr_stg_r12.batch_id%type,
      leg_shipment_header_id        xxpo_receipt_hdr_stg_r12.leg_shipment_header_id%type,
      run_sequence_id               xxpo_receipt_hdr_stg_r12.run_sequence_id%type,
      leg_receipt_source_code       xxpo_receipt_hdr_stg_r12.leg_receipt_source_code%type,
      leg_po_number                 xxpo_receipt_hdr_stg_r12.leg_po_number%type,
      leg_receipt_num               xxpo_receipt_hdr_stg_r12.leg_receipt_num%type,
      leg_bill_of_lading            xxpo_receipt_hdr_stg_r12.leg_bill_of_lading%type,
      leg_packing_slip              xxpo_receipt_hdr_stg_r12.leg_packing_slip%type,
      leg_shipped_date              xxpo_receipt_hdr_stg_r12.leg_shipped_date%type,
      shipped_date                  xxpo_receipt_hdr_stg_r12.shipped_date%type,
      leg_expected_receipt_date     xxpo_receipt_hdr_stg_r12.leg_expected_receipt_date%type,
      expected_receipt_date         xxpo_receipt_hdr_stg_r12.expected_receipt_date%type,
      leg_operating_unit            xxpo_receipt_hdr_stg_r12.leg_operating_unit%type,
      operating_unit                xxpo_receipt_hdr_stg_r12.operating_unit%type,
      org_id                        xxpo_receipt_hdr_stg_r12.org_id%type,
      leg_num_of_containers         xxpo_receipt_hdr_stg_r12.leg_num_of_containers%type,
      leg_waybill_airbill_num       xxpo_receipt_hdr_stg_r12.leg_waybill_airbill_num%type,
      leg_comments                  xxpo_receipt_hdr_stg_r12.leg_comments%type,
      leg_packaging_code            xxpo_receipt_hdr_stg_r12.leg_packaging_code%type,
      leg_freight_terms             xxpo_receipt_hdr_stg_r12.leg_freight_terms%type,
      freight_terms                 xxpo_receipt_hdr_stg_r12.freight_terms%type,
      leg_freight_bill_number       xxpo_receipt_hdr_stg_r12.leg_freight_bill_number%type,
      leg_currency_code             xxpo_receipt_hdr_stg_r12.leg_currency_code%type,
      leg_conversion_rate_type      xxpo_receipt_hdr_stg_r12.leg_conversion_rate_type%type,
      leg_conversion_rate           xxpo_receipt_hdr_stg_r12.leg_conversion_rate%type,
      leg_conversion_rate_date      xxpo_receipt_hdr_stg_r12.leg_conversion_rate_date%type,
      leg_attribute_category        xxpo_receipt_hdr_stg_r12.leg_attribute_category%type,
      leg_attribute1                xxpo_receipt_hdr_stg_r12.leg_attribute1%type,
      leg_attribute2                xxpo_receipt_hdr_stg_r12.leg_attribute2%type,
      leg_attribute3                xxpo_receipt_hdr_stg_r12.leg_attribute3%type,
      leg_attribute4                xxpo_receipt_hdr_stg_r12.leg_attribute4%type,
      leg_attribute5                xxpo_receipt_hdr_stg_r12.leg_attribute5%type,
      leg_attribute6                xxpo_receipt_hdr_stg_r12.leg_attribute6%type,
      leg_attribute7                xxpo_receipt_hdr_stg_r12.leg_attribute7%type,
      leg_attribute8                xxpo_receipt_hdr_stg_r12.leg_attribute8%type,
      leg_attribute9                xxpo_receipt_hdr_stg_r12.leg_attribute9%type,
      leg_attribute10               xxpo_receipt_hdr_stg_r12.leg_attribute10%type,
      leg_attribute11               xxpo_receipt_hdr_stg_r12.leg_attribute11%type,
      leg_attribute12               xxpo_receipt_hdr_stg_r12.leg_attribute12%type,
      leg_attribute13               xxpo_receipt_hdr_stg_r12.leg_attribute13%type,
      leg_attribute14               xxpo_receipt_hdr_stg_r12.leg_attribute14%type,
      leg_attribute15               xxpo_receipt_hdr_stg_r12.leg_attribute15%type,
      group_id                      xxpo_receipt_hdr_stg_r12.group_id%type,
      leg_vendor_num                xxpo_receipt_hdr_stg_r12.leg_vendor_num%type,
      leg_vendor_id                 xxpo_receipt_hdr_stg_r12.leg_vendor_id%type,
      vendor_id                     xxpo_receipt_hdr_stg_r12.vendor_id%type,
      leg_vendor_site_code          xxpo_receipt_hdr_stg_r12.leg_vendor_site_code%type,
      leg_vendor_site_id            xxpo_receipt_hdr_stg_r12.leg_vendor_site_id%type,
      vendor_site_id                xxpo_receipt_hdr_stg_r12.vendor_site_id%type,
      leg_from_organization_name    xxpo_receipt_hdr_stg_r12.leg_from_organization_name%type,
      leg_from_organization_id      xxpo_receipt_hdr_stg_r12.leg_from_organization_id%type,
      from_organization_id          xxpo_receipt_hdr_stg_r12.from_organization_id%type,
      leg_employee_no               xxpo_receipt_hdr_stg_r12.leg_employee_no%type,
      leg_employee_id               xxpo_receipt_hdr_stg_r12.leg_employee_id%type,
      employee_id                   xxpo_receipt_hdr_stg_r12.employee_id%type,
      leg_ship_to_organization_name xxpo_receipt_hdr_stg_r12.leg_ship_to_organization_name%type,
      leg_ship_to_organization_id   xxpo_receipt_hdr_stg_r12.leg_ship_to_organization_id%type,
      ship_to_organization_id       xxpo_receipt_hdr_stg_r12.ship_to_organization_id%type,
      leg_ship_to_location          xxpo_receipt_hdr_stg_r12.leg_ship_to_location%type,
      leg_ship_to_location_id       xxpo_receipt_hdr_stg_r12.leg_ship_to_location_id%type,
      ship_to_location_id           xxpo_receipt_hdr_stg_r12.ship_to_location_id%type,
      creation_date                 xxpo_receipt_hdr_stg_r12.creation_date%type,
      created_by                    xxpo_receipt_hdr_stg_r12.created_by%type,
      last_updated_date             xxpo_receipt_hdr_stg_r12.last_updated_date%type,
      last_updated_by               xxpo_receipt_hdr_stg_r12.last_updated_by%type,
      last_update_login             xxpo_receipt_hdr_stg_r12.last_update_login%type,
      program_application_id        xxpo_receipt_hdr_stg_r12.program_application_id%type,
      program_id                    xxpo_receipt_hdr_stg_r12.program_id%type,
      program_update_date           xxpo_receipt_hdr_stg_r12.program_update_date%type,
      request_id                    xxpo_receipt_hdr_stg_r12.request_id%type,
      process_flag                  xxpo_receipt_hdr_stg_r12.process_flag%type,
      error_type                    xxpo_receipt_hdr_stg_r12.error_type%type,
      attribute_category            xxpo_receipt_hdr_stg_r12.attribute_category%type,
      attribute1                    xxpo_receipt_hdr_stg_r12.attribute1%type,
      attribute2                    xxpo_receipt_hdr_stg_r12.attribute2%type,
      attribute3                    xxpo_receipt_hdr_stg_r12.attribute3%type,
      attribute4                    xxpo_receipt_hdr_stg_r12.attribute4%type,
      attribute5                    xxpo_receipt_hdr_stg_r12.attribute5%type,
      attribute6                    xxpo_receipt_hdr_stg_r12.attribute6%type,
      attribute7                    xxpo_receipt_hdr_stg_r12.attribute7%type,
      attribute8                    xxpo_receipt_hdr_stg_r12.attribute8%type,
      attribute9                    xxpo_receipt_hdr_stg_r12.attribute9%type,
      attribute10                   xxpo_receipt_hdr_stg_r12.attribute10%type,
      attribute11                   xxpo_receipt_hdr_stg_r12.attribute11%type,
      attribute12                   xxpo_receipt_hdr_stg_r12.attribute12%type,
      attribute13                   xxpo_receipt_hdr_stg_r12.attribute13%type,
      attribute14                   xxpo_receipt_hdr_stg_r12.attribute14%type,
      attribute15                   xxpo_receipt_hdr_stg_r12.attribute15%type,
      leg_source_system             xxpo_receipt_hdr_stg_r12.leg_source_system%type,
      leg_request_id                xxpo_receipt_hdr_stg_r12.leg_request_id%type,
      leg_seq_num                   xxpo_receipt_hdr_stg_r12.leg_seq_num%type,
      leg_process_flag              xxpo_receipt_hdr_stg_r12.leg_process_flag%type,
      interface_header_id           xxpo_receipt_hdr_stg_r12.interface_header_id%type);
    type leg_head_tbl is table of leg_head_rec index by binary_integer;
    l_leg_head_tbl leg_head_tbl;
    l_err_record   number;
    cursor cur_leg_head is
      select xil.interface_txn_id,
             xil.batch_id,
             xil.leg_shipment_header_id,
             xil.run_sequence_id,
             xil.leg_receipt_source_code,
             xil.leg_po_number,
             xil.leg_receipt_num,
             xil.leg_bill_of_lading,
             xil.leg_packing_slip,
             xil.leg_shipped_date,
             xil.shipped_date,
             xil.leg_expected_receipt_date,
             xil.expected_receipt_date,
             xil.leg_operating_unit,
             xil.operating_unit,
             xil.org_id,
             xil.leg_num_of_containers,
             xil.leg_waybill_airbill_num,
             xil.leg_comments,
             xil.leg_packaging_code,
             xil.leg_freight_terms,
             xil.freight_terms,
             xil.leg_freight_bill_number,
             xil.leg_currency_code,
             xil.leg_conversion_rate_type,
             xil.leg_conversion_rate,
             xil.leg_conversion_rate_date,
             xil.leg_attribute_category,
             xil.leg_attribute1,
             xil.leg_attribute2,
             xil.leg_attribute3,
             xil.leg_attribute4,
             xil.leg_attribute5,
             xil.leg_attribute6,
             xil.leg_attribute7,
             xil.leg_attribute8,
             xil.leg_attribute9,
             xil.leg_attribute10,
             xil.leg_attribute11,
             xil.leg_attribute12,
             xil.leg_attribute13,
             xil.leg_attribute14,
             xil.leg_attribute15,
             xil.group_id,
             xil.leg_vendor_num,
             xil.leg_vendor_id,
             xil.vendor_id,
             xil.leg_vendor_site_code,
             xil.leg_vendor_site_id,
             xil.vendor_site_id,
             xil.leg_from_organization_name,
             xil.leg_from_organization_id,
             xil.from_organization_id,
             xil.leg_employee_no,
             xil.leg_employee_id,
             xil.employee_id,
             xil.leg_ship_to_organization_name,
             xil.leg_ship_to_organization_id,
             xil.ship_to_organization_id,
             xil.leg_ship_to_location,
             xil.leg_ship_to_location_id,
             xil.ship_to_location_id,
             xil.creation_date,
             xil.created_by,
             xil.last_updated_date,
             xil.last_updated_by,
             xil.last_update_login,
             xil.program_application_id,
             xil.program_id,
             xil.program_update_date,
             xil.request_id,
             xil.process_flag,
             xil.error_type,
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
             xil.leg_source_system,
             xil.leg_request_id,
             xil.leg_seq_num,
             xil.leg_process_flag,
             xil.interface_header_id
        from xxpo_receipt_hdr_stg_r12 xil
       where xil.leg_process_flag = 'V'
         and not exists
       (select 1
                from xxpo_receipt_hdr_stg xis
               where xis.interface_txn_id = xil.interface_txn_id);
  begin
    pov_ret_stats  := 'S';
    pov_err_msg    := null;
    g_total_count  := 0;
    g_failed_count := 0;
    g_loaded_count := 0;
    --Open cursor to extract data from extraction staging table
    open cur_leg_head;
    loop
      xxetn_debug_pkg.add_debug(piv_debug_msg => 'Loading Headers');
      l_leg_head_tbl.delete;
      fetch cur_leg_head bulk collect
        into l_leg_head_tbl limit 5000;
      --limit size of Bulk Collect
      -- Get Total Count
      g_total_count := g_total_count + l_leg_head_tbl.count;
      exit when l_leg_head_tbl.count = 0;
      begin
        -- Bulk Insert into Conversion table
        forall indx in 1 .. l_leg_head_tbl.count save exceptions
          insert into xxpo_receipt_hdr_stg values l_leg_head_tbl (indx);
      exception
        when others then
          print_log_message('Errors encountered while loading header data ');
          for l_indx_exp in 1 .. sql%bulk_exceptions.count loop
            l_err_record  := l_leg_head_tbl(sql%bulk_exceptions(l_indx_exp).error_index)
                             .interface_txn_id;
            pov_ret_stats := 'E';
            print_log_message('Record sequence (interface_txn_id) : ' || l_leg_head_tbl(sql%bulk_exceptions(l_indx_exp).error_index)
                              .interface_txn_id);
            print_log_message('Error Message : ' ||
                              sqlerrm(-sql%bulk_exceptions(l_indx_exp)
                                      .error_code));
            -- Updating Leg_process_flag to 'E' for failed records
            update xxpo_receipt_hdr_stg_r12 xil
               set xil.leg_process_flag       = 'E',
                   xil.last_updated_date      = sysdate,
                   xil.last_updated_by        = g_last_updated_by,
                   xil.last_update_login      = g_last_update_login,
                   xil.program_id             = g_conc_program_id,
                   xil.program_application_id = g_prog_appl_id,
                   xil.program_update_date    = sysdate
             where xil.interface_txn_id = l_err_record
               and xil.leg_process_flag = 'V';
            g_failed_count := g_failed_count + sql%rowcount;
          end loop;
      end;
    end loop;
    close cur_leg_head;
    commit;
    if g_failed_count > 0 then
      print_log_message('Number of Failed Records during load of Headers : ' ||
                        g_failed_count);
    end if;
    ---output
    g_loaded_count := g_total_count - g_failed_count;
    fnd_file.put_line(fnd_file.output, ' Stats for Headers table load ');
    fnd_file.put_line(fnd_file.output, '================================');
    fnd_file.put_line(fnd_file.output, 'Total Count : ' || g_total_count);
    fnd_file.put_line(fnd_file.output, 'Loaded Count: ' || g_loaded_count);
    fnd_file.put_line(fnd_file.output, 'Failed Count: ' || g_failed_count);
    fnd_file.put_line(fnd_file.output, '================================');
    -- If records successfully posted to conversion staging table
    if g_total_count > 0 then
      print_log_message('Updating process flag (leg_process_flag) in extraction table for processed records ');
      update xxpo_receipt_hdr_stg_r12 xil
         set xil.leg_process_flag       = 'P',
             xil.last_updated_date      = sysdate,
             xil.last_updated_by        = g_last_updated_by,
             xil.last_update_login      = g_last_update_login,
             xil.program_id             = g_conc_program_id,
             xil.program_application_id = g_prog_appl_id,
             xil.program_update_date    = sysdate
       where xil.leg_process_flag = 'V'
         and exists
       (select 1
                from xxpo_receipt_hdr_stg xis
               where xis.interface_txn_id = xil.interface_txn_id);
      commit;
      -- Either no data to load from extraction table or records already exist in R12 staging table and hence not loaded
    else
      print_log_message('Either no data found for loading from extraction table or records already exist in R12 staging table and hence not loaded ');
      update xxpo_receipt_hdr_stg_r12 xil
         set xil.leg_process_flag       = 'E',
             xil.last_updated_date      = sysdate,
             xil.last_updated_by        = g_last_updated_by,
             xil.last_update_login      = g_last_update_login,
             xil.program_id             = g_conc_program_id,
             xil.program_application_id = g_prog_appl_id,
             xil.program_update_date    = sysdate
       where xil.leg_process_flag = 'V'
         and exists
       (select 1
                from xxpo_receipt_hdr_stg xis
               where xis.interface_txn_id = xil.interface_txn_id);
      commit;
    end if;
  exception
    when others then
      pov_ret_stats := 'E';
      pov_err_msg   := 'ERROR : Error in Load_header procedure' ||
                       substr(sqlerrm, 1, 200);
      rollback;
  end load_header;

  --
  -- ========================
  -- Procedure: LOAD_transaction
  -- =============================================================================
  --   This procedure is used to load data from extraction into staging table
  -- =============================================================================
  procedure load_transaction(pov_ret_stats out nocopy varchar2,
                             pov_err_msg   out nocopy varchar2) is
    type leg_trx_rec is record(
      interface_txn_id             xxpo_receipt_trx_stg_r12.interface_txn_id%type,
      batch_id                     xxpo_receipt_trx_stg_r12.batch_id%type,
      run_sequence_id              xxpo_receipt_trx_stg_r12.run_sequence_id%type,
      leg_shipment_line_id         xxpo_receipt_trx_stg_r12.leg_shipment_line_id%type,
      leg_shipment_header_id       xxpo_receipt_trx_stg_r12.leg_shipment_header_id%type,
      leg_transaction_date         xxpo_receipt_trx_stg_r12.leg_transaction_date%type,
      transaction_date             xxpo_receipt_trx_stg_r12.transaction_date%type,
      leg_quantity                 xxpo_receipt_trx_stg_r12.leg_quantity%type,
      leg_uom_code                 xxpo_receipt_trx_stg_r12.leg_uom_code%type,
      leg_primary_quantity         xxpo_receipt_trx_stg_r12.leg_primary_quantity%type,
      leg_primary_unit_of_measure  xxpo_receipt_trx_stg_r12.leg_primary_unit_of_measure%type,
      leg_source_document_code     xxpo_receipt_trx_stg_r12.leg_source_document_code%type,
      leg_po_number                xxpo_receipt_trx_stg_r12.leg_po_number%type,
      po_header_id                 xxpo_receipt_trx_stg_r12.po_header_id%type,
      leg_po_header_id             xxpo_receipt_trx_stg_r12.leg_po_header_id%type,
      leg_po_line_num              xxpo_receipt_trx_stg_r12.leg_po_line_num%type,
      po_line_id                   xxpo_receipt_trx_stg_r12.po_line_id%type,
      leg_po_line_id               xxpo_receipt_trx_stg_r12.leg_po_line_id%type,
      leg_po_shipment_num          xxpo_receipt_trx_stg_r12.leg_po_shipment_num%type,
      po_line_location_id          xxpo_receipt_trx_stg_r12.po_line_location_id%type,
      leg_po_line_location_id      xxpo_receipt_trx_stg_r12.leg_po_line_location_id%type,
      leg_po_distribution_num      xxpo_receipt_trx_stg_r12.leg_po_distribution_num%type,
      leg_po_distribution_id       xxpo_receipt_trx_stg_r12.leg_po_distribution_id%type,
      po_distribution_id           xxpo_receipt_trx_stg_r12.po_distribution_id%type,
      leg_currency_code            xxpo_receipt_trx_stg_r12.leg_currency_code%type,
      leg_currency_conversion_type xxpo_receipt_trx_stg_r12.leg_currency_conversion_type%type,
      leg_currency_conversion_rate xxpo_receipt_trx_stg_r12.leg_currency_conversion_rate%type,
      leg_currency_conversion_date xxpo_receipt_trx_stg_r12.leg_currency_conversion_date%type,
      leg_destination_type_code    xxpo_receipt_trx_stg_r12.leg_destination_type_code%type,
      leg_operating_unit           xxpo_receipt_trx_stg_r12.leg_operating_unit%type,
      operating_unit               xxpo_receipt_trx_stg_r12.operating_unit%type,
      leg_org_id                   xxpo_receipt_trx_stg_r12.leg_org_id%type,
      org_id                       xxpo_receipt_trx_stg_r12.org_id%type,
      leg_subinventory             xxpo_receipt_trx_stg_r12.leg_subinventory%type,
      leg_bill_of_lading           xxpo_receipt_trx_stg_r12.leg_bill_of_lading%type,
      leg_packing_slip             xxpo_receipt_trx_stg_r12.leg_packing_slip%type,
      leg_shipped_date             xxpo_receipt_trx_stg_r12.leg_shipped_date%type,
      shipped_date                 xxpo_receipt_trx_stg_r12.shipped_date%type,
      leg_expected_receipt_date    xxpo_receipt_trx_stg_r12.leg_expected_receipt_date%type,
      expected_receipt_date        xxpo_receipt_trx_stg_r12.expected_receipt_date%type,
      leg_comments                 xxpo_receipt_trx_stg_r12.leg_comments%type,
      leg_attribute_category       xxpo_receipt_trx_stg_r12.leg_attribute_category%type,
      leg_attribute1               xxpo_receipt_trx_stg_r12.leg_attribute1%type,
      leg_attribute2               xxpo_receipt_trx_stg_r12.leg_attribute2%type,
      leg_attribute3               xxpo_receipt_trx_stg_r12.leg_attribute3%type,
      leg_attribute4               xxpo_receipt_trx_stg_r12.leg_attribute4%type,
      leg_attribute5               xxpo_receipt_trx_stg_r12.leg_attribute5%type,
      leg_attribute6               xxpo_receipt_trx_stg_r12.leg_attribute6%type,
      leg_attribute7               xxpo_receipt_trx_stg_r12.leg_attribute7%type,
      leg_attribute8               xxpo_receipt_trx_stg_r12.leg_attribute8%type,
      leg_attribute9               xxpo_receipt_trx_stg_r12.leg_attribute9%type,
      leg_attribute10              xxpo_receipt_trx_stg_r12.leg_attribute10%type,
      leg_attribute11              xxpo_receipt_trx_stg_r12.leg_attribute11%type,
      leg_attribute12              xxpo_receipt_trx_stg_r12.leg_attribute12%type,
      leg_attribute13              xxpo_receipt_trx_stg_r12.leg_attribute13%type,
      leg_attribute14              xxpo_receipt_trx_stg_r12.leg_attribute14%type,
      leg_attribute15              xxpo_receipt_trx_stg_r12.leg_attribute15%type,
      leg_item_num                 xxpo_receipt_trx_stg_r12.leg_item_num%type,
      leg_inventory_item_id        xxpo_receipt_trx_stg_r12.leg_inventory_item_id%type,
      inventory_item_id            xxpo_receipt_trx_stg_r12.inventory_item_id%type,
      leg_truck_num                xxpo_receipt_trx_stg_r12.leg_truck_num%type,
      leg_category_id              xxpo_receipt_trx_stg_r12.leg_category_id%type,
      category_id                  xxpo_receipt_trx_stg_r12.category_id%type,
      leg_item_description         xxpo_receipt_trx_stg_r12.leg_item_description%type,
      leg_container_num            xxpo_receipt_trx_stg_r12.leg_container_num%type,
      leg_locator                  xxpo_receipt_trx_stg_r12.leg_locator%type,
      leg_locator_id               xxpo_receipt_trx_stg_r12.leg_locator_id%type,
      locator_id                   xxpo_receipt_trx_stg_r12.locator_id%type,
      leg_resource_code            xxpo_receipt_trx_stg_r12.leg_resource_code%type,
      leg_create_debit_memo_flag   xxpo_receipt_trx_stg_r12.leg_create_debit_memo_flag%type,
      create_debit_memo_flag       xxpo_receipt_trx_stg_r12.create_debit_memo_flag%type,
      interface_transaction_id     xxpo_receipt_trx_stg_r12.interface_transaction_id%type,
      group_id                     xxpo_receipt_trx_stg_r12.group_id%type,
      leg_employee_no              xxpo_receipt_trx_stg_r12.leg_employee_no%type,
      leg_employee_id              xxpo_receipt_trx_stg_r12.leg_employee_id%type,
      employee_id                  xxpo_receipt_trx_stg_r12.employee_id%type,
      leg_vendor_num               xxpo_receipt_trx_stg_r12.leg_vendor_num%type,
      leg_vendor_id                xxpo_receipt_trx_stg_r12.leg_vendor_id%type,
      vendor_id                    xxpo_receipt_trx_stg_r12.vendor_id%type,
      leg_vendor_site_code         xxpo_receipt_trx_stg_r12.leg_vendor_site_code%type,
      leg_vendor_site_id           xxpo_receipt_trx_stg_r12.leg_vendor_site_id%type,
      vendor_site_id               xxpo_receipt_trx_stg_r12.vendor_site_id%type,
      leg_ship_to_location         xxpo_receipt_trx_stg_r12.leg_ship_to_location%type,
      leg_ship_to_locations_id     xxpo_receipt_trx_stg_r12.leg_ship_to_locations_id%type,
      ship_to_locations_id         xxpo_receipt_trx_stg_r12.ship_to_locations_id%type,
      leg_deliver_to_person_emp_no xxpo_receipt_trx_stg_r12.leg_deliver_to_person_emp_no%type,
      leg_deliver_to_person_id     xxpo_receipt_trx_stg_r12.leg_deliver_to_person_id%type,
      deliver_to_person_id         xxpo_receipt_trx_stg_r12.deliver_to_person_id%type,
      leg_deliver_to_location      xxpo_receipt_trx_stg_r12.leg_deliver_to_location%type,
      leg_deliver_to_location_id   xxpo_receipt_trx_stg_r12.leg_deliver_to_location_id%type,
      deliver_to_location_id       xxpo_receipt_trx_stg_r12.deliver_to_location_id%type,
      leg_from_organization_name   xxpo_receipt_trx_stg_r12.leg_from_organization_name%type,
      leg_from_organization_id     xxpo_receipt_trx_stg_r12.leg_from_organization_id%type,
      from_organization_id         xxpo_receipt_trx_stg_r12.from_organization_id%type,
      leg_to_org_name              xxpo_receipt_trx_stg_r12.leg_to_org_name%type,
      leg_to_organization_id       xxpo_receipt_trx_stg_r12.leg_to_organization_id%type,
      to_organization_id           xxpo_receipt_trx_stg_r12.to_organization_id%type,
      creation_date                xxpo_receipt_trx_stg_r12.creation_date%type,
      created_by                   xxpo_receipt_trx_stg_r12.created_by%type,
      last_updated_date            xxpo_receipt_trx_stg_r12.last_updated_date%type,
      last_updated_by              xxpo_receipt_trx_stg_r12.last_updated_by%type,
      last_update_login            xxpo_receipt_trx_stg_r12.last_update_login%type,
      program_application_id       xxpo_receipt_trx_stg_r12.program_application_id%type,
      program_id                   xxpo_receipt_trx_stg_r12.program_id%type,
      program_update_date          xxpo_receipt_trx_stg_r12.program_update_date%type,
      request_id                   xxpo_receipt_trx_stg_r12.request_id%type,
      process_flag                 xxpo_receipt_trx_stg_r12.process_flag%type,
      error_type                   xxpo_receipt_trx_stg_r12.error_type%type,
      attribute_category           xxpo_receipt_trx_stg_r12.attribute_category%type,
      attribute1                   xxpo_receipt_trx_stg_r12.attribute1%type,
      attribute2                   xxpo_receipt_trx_stg_r12.attribute2%type,
      attribute3                   xxpo_receipt_trx_stg_r12.attribute3%type,
      attribute4                   xxpo_receipt_trx_stg_r12.attribute4%type,
      attribute5                   xxpo_receipt_trx_stg_r12.attribute5%type,
      attribute6                   xxpo_receipt_trx_stg_r12.attribute6%type,
      attribute7                   xxpo_receipt_trx_stg_r12.attribute7%type,
      attribute8                   xxpo_receipt_trx_stg_r12.attribute8%type,
      attribute9                   xxpo_receipt_trx_stg_r12.attribute9%type,
      attribute10                  xxpo_receipt_trx_stg_r12.attribute10%type,
      attribute11                  xxpo_receipt_trx_stg_r12.attribute11%type,
      attribute12                  xxpo_receipt_trx_stg_r12.attribute12%type,
      attribute13                  xxpo_receipt_trx_stg_r12.attribute13%type,
      attribute14                  xxpo_receipt_trx_stg_r12.attribute14%type,
      attribute15                  xxpo_receipt_trx_stg_r12.attribute15%type,
      leg_source_system            xxpo_receipt_trx_stg_r12.leg_source_system%type,
      leg_request_id               xxpo_receipt_trx_stg_r12.leg_request_id%type,
      leg_seq_num                  xxpo_receipt_trx_stg_r12.leg_seq_num%type,
      leg_process_flag             xxpo_receipt_trx_stg_r12.leg_process_flag%type,
      leg_unit_price               xxpo_receipt_trx_stg_r12.leg_unit_price%type,
      leg_po_price_override_flag   xxpo_receipt_trx_stg_r12.leg_po_price_override_flag%type,
      leg_receipt_rate             xxpo_receipt_trx_stg_r12.leg_receipt_rate%type,
      item_description             xxpo_receipt_trx_stg_r12.item_description%type,
      interface_header_id          xxpo_receipt_trx_stg_r12.interface_header_id%type);
    type leg_trx_tbl is table of leg_trx_rec index by binary_integer;
    l_leg_trx_tbl leg_trx_tbl;
    l_err_record  number;
    cursor cur_leg_trx is
      select xil.interface_txn_id,
             xil.batch_id,
             xil.run_sequence_id,
             xil.leg_shipment_line_id,
             xil.leg_shipment_header_id,
             xil.leg_transaction_date,
             xil.transaction_date,
             xil.leg_quantity,
             xil.leg_uom_code,
             xil.leg_primary_quantity,
             xil.leg_primary_unit_of_measure,
             xil.leg_source_document_code,
             xil.leg_po_number,
             xil.po_header_id,
             xil.leg_po_header_id,
             xil.leg_po_line_num,
             xil.po_line_id,
             xil.leg_po_line_id,
             xil.leg_po_shipment_num,
             xil.po_line_location_id,
             xil.leg_po_line_location_id,
             xil.leg_po_distribution_num,
             xil.leg_po_distribution_id,
             xil.po_distribution_id,
             xil.leg_currency_code,
             --v1.8 comment 1.7 change and uncomment the commented lines
             --v1.7
             DECODE(xil.leg_currency_conversion_type,NULL,NULL,'User'),  --v1.9 SDP
             --DECODE(LEG_CURRENCY_CONVERSION_TYPE, NULL,NULL,'User','User','Corporate','Corporate','Corporate'),
             xil.leg_currency_conversion_rate,
             --DECODE(xil.LEG_CURRENCY_CONVERSION_TYPE,'User',xil.leg_currency_conversion_rate, null),
             --v1.7 ends
             --v1.8
             xil.leg_currency_conversion_date,
             xil.leg_destination_type_code,
             xil.leg_operating_unit,
             xil.operating_unit,
             xil.leg_org_id,
             xil.org_id,
             xil.leg_subinventory,
             xil.leg_bill_of_lading,
             xil.leg_packing_slip,
             xil.leg_shipped_date,
             xil.shipped_date,
             xil.leg_expected_receipt_date,
             xil.expected_receipt_date,
             xil.leg_comments,
             xil.leg_attribute_category,
             xil.leg_attribute1,
             xil.leg_attribute2,
             xil.leg_attribute3,
             xil.leg_attribute4,
             xil.leg_attribute5,
             xil.leg_attribute6,
             xil.leg_attribute7,
             xil.leg_attribute8,
             xil.leg_attribute9,
             xil.leg_attribute10,
             xil.leg_attribute11,
             xil.leg_attribute12,
             xil.leg_attribute13,
             xil.leg_attribute14,
             xil.leg_attribute15,
             xil.leg_item_num,
             xil.leg_inventory_item_id,
             xil.inventory_item_id,
             xil.leg_truck_num,
             xil.leg_category_id,
             xil.category_id,
             xil.leg_item_description,
             xil.leg_container_num,
             xil.leg_locator,
             xil.leg_locator_id,
             xil.locator_id,
             xil.leg_resource_code,
             xil.leg_create_debit_memo_flag,
             xil.create_debit_memo_flag,
             xil.interface_transaction_id,
             xil.group_id,
             xil.leg_employee_no,
             xil.leg_employee_id,
             xil.employee_id,
             xil.leg_vendor_num,
             xil.leg_vendor_id,
             xil.vendor_id,
             xil.leg_vendor_site_code,
             xil.leg_vendor_site_id,
             xil.vendor_site_id,
             xil.leg_ship_to_location,
             xil.leg_ship_to_locations_id,
             xil.ship_to_locations_id,
             xil.leg_deliver_to_person_emp_no,
             xil.leg_deliver_to_person_id,
             xil.deliver_to_person_id,
             xil.leg_deliver_to_location,
             xil.leg_deliver_to_location_id,
             xil.deliver_to_location_id,
             xil.leg_from_organization_name,
             xil.leg_from_organization_id,
             xil.from_organization_id,
             xil.leg_to_org_name,
             xil.leg_to_organization_id,
             xil.to_organization_id,
             xil.creation_date,
             xil.created_by,
             xil.last_updated_date,
             xil.last_updated_by,
             xil.last_update_login,
             xil.program_application_id,
             xil.program_id,
             xil.program_update_date,
             xil.request_id,
             xil.process_flag,
             xil.error_type,
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
             xil.leg_source_system,
             xil.leg_request_id,
             xil.leg_seq_num,
             xil.leg_process_flag,
             xil.leg_unit_price,
             xil.leg_po_price_override_flag,
             xil.leg_receipt_rate,
             xil.item_description,
             xil.interface_header_id
        from xxpo_receipt_trx_stg_r12 xil
       where xil.leg_process_flag = 'V'
         and not exists
       (select 1
                from xxpo_receipt_trx_stg xis
               where xis.interface_txn_id = xil.interface_txn_id);
  begin
    pov_ret_stats  := 'S';
    pov_err_msg    := null;
    g_total_count  := 0;
    g_failed_count := 0;
    g_loaded_count := 0;
    --Open cursor to extract data from extraction staging table
    open cur_leg_trx;
    loop
      xxetn_debug_pkg.add_debug(piv_debug_msg => 'Loading transactions');
      l_leg_trx_tbl.delete;
      fetch cur_leg_trx bulk collect
        into l_leg_trx_tbl limit 5000;
      --limit size of Bulk Collect
      -- Get Total Count
      g_total_count := g_total_count + l_leg_trx_tbl.count;
      exit when l_leg_trx_tbl.count = 0;
      begin
        -- Bulk Insert into Conversion table
        forall indx in 1 .. l_leg_trx_tbl.count save exceptions
          insert into xxpo_receipt_trx_stg values l_leg_trx_tbl (indx);
      exception
        when others then
          print_log_message('Errors encountered while loading transaction data ');
          for l_indx_exp in 1 .. sql%bulk_exceptions.count loop
            l_err_record  := l_leg_trx_tbl(sql%bulk_exceptions(l_indx_exp).error_index)
                             .interface_txn_id;
            pov_ret_stats := 'E';
            print_log_message('Record sequence (interface_txn_id) : ' || l_leg_trx_tbl(sql%bulk_exceptions(l_indx_exp).error_index)
                              .interface_txn_id);
            print_log_message('Error Message : ' ||
                              sqlerrm(-sql%bulk_exceptions(l_indx_exp)
                                      .error_code));
            -- Updating Leg_process_flag to 'E' for failed records
            update xxpo_receipt_trx_stg_r12 xil
               set xil.leg_process_flag       = 'E',
                   xil.last_updated_date      = sysdate,
                   xil.last_updated_by        = g_last_updated_by,
                   xil.last_update_login      = g_last_update_login,
                   xil.program_id             = g_conc_program_id,
                   xil.program_application_id = g_prog_appl_id,
                   xil.program_update_date    = sysdate
             where xil.interface_txn_id = l_err_record
               and xil.leg_process_flag = 'V';
            g_failed_count := g_failed_count + sql%rowcount;
          end loop;
      end;
    end loop;
    close cur_leg_trx;
    commit;
    if g_failed_count > 0 then
      print_log_message('Number of Failed Records during load of transactions : ' ||
                        g_failed_count);
    end if;
    ---output
    g_loaded_count := g_total_count - g_failed_count;
    fnd_file.put_line(fnd_file.output,
                      ' Stats for transactions table load ');
    fnd_file.put_line(fnd_file.output, '================================');
    fnd_file.put_line(fnd_file.output, 'Total Count : ' || g_total_count);
    fnd_file.put_line(fnd_file.output, 'Loaded Count: ' || g_loaded_count);
    fnd_file.put_line(fnd_file.output, 'Failed Count: ' || g_failed_count);
    fnd_file.put_line(fnd_file.output, '================================');
    -- If records successfully posted to conversion staging table
    if g_total_count > 0 then
      print_log_message('Updating process flag (leg_process_flag) in extraction table for processed records ');
      update xxpo_receipt_trx_stg_r12 xil
         set xil.leg_process_flag       = 'P',
             xil.last_updated_date      = sysdate,
             xil.last_updated_by        = g_last_updated_by,
             xil.last_update_login      = g_last_update_login,
             xil.program_id             = g_conc_program_id,
             xil.program_application_id = g_prog_appl_id,
             xil.program_update_date    = sysdate
       where xil.leg_process_flag = 'V'
         and exists
       (select 1
                from xxpo_receipt_trx_stg xis
               where xis.interface_txn_id = xil.interface_txn_id);
      commit;
      -- Either no data to load from extraction table or records already exist in R12 staging table and hence not loaded
    else
      print_log_message('Either no data found for loading from extraction table or records already exist in R12 staging table and hence not loaded ');
      update xxpo_receipt_trx_stg_r12 xil
         set xil.leg_process_flag       = 'E',
             xil.last_updated_date      = sysdate,
             xil.last_updated_by        = g_last_updated_by,
             xil.last_update_login      = g_last_update_login,
             xil.program_id             = g_conc_program_id,
             xil.program_application_id = g_prog_appl_id,
             xil.program_update_date    = sysdate
       where xil.leg_process_flag = 'V'
         and exists
       (select 1
                from xxpo_receipt_trx_stg xis
               where xis.interface_txn_id = xil.interface_txn_id);
      commit;
    end if;
  exception
    when others then
      pov_ret_stats := 'E';
      pov_err_msg   := 'ERROR : Error in Load_transaction procedure' ||
                       substr(sqlerrm, 1, 200);
      rollback;
  end load_transaction;

  ---+================================================================================+
  ---|FUNCTION NAME : xxpo_dup_rcpt
  ---|DESCRIPTION   : This function will check for duplicate Purchase order receipt
  ---+================================================================================+
  function xxpo_dup_rcpt(p_in_int_txn_id     in number,
                         p_in_po_rcpt_number in varchar2) return varchar2 is
    lv_status  varchar2(1) := 'N';
    lv_count   number := 0;
    l_err_code varchar2(40);
    l_err_msg  varchar2(2000);
  begin
    select count(receipt_num)
      into lv_count
      from rcv_shipment_headers
     where receipt_num = p_in_po_rcpt_number;
    if lv_count = 0 then
      lv_status := 'N';
    else
      lv_status  := 'Y';
      l_err_code := 'ETN_RCPT_DUPLICATE_DOC_NUM';
      l_err_msg  := 'Error: Receipt already exists in the system. ';
      log_errors(pin_interface_txn_id    => p_in_int_txn_id,
                 piv_source_table        => 'xxpo_receipt_hdr_stg',
                 piv_source_column_name  => 'leg_receipt_num',
                 piv_source_column_value => p_in_po_rcpt_number,
                 piv_source_keyname1     => null,
                 piv_source_keyvalue1    => null,
                 piv_error_type          => 'ERR_VAL',
                 piv_error_code          => l_err_code,
                 piv_error_message       => l_err_msg);
    end if;
    return lv_status;
  exception
    when others then
      lv_status  := 'Y';
      l_err_code := 'ETN_PO_PROCEDURE_EXCEPTION';
      l_err_msg  := 'Error: Exception error in xxpo_dup_rcpt procedure. ';
      log_errors(pin_interface_txn_id    => p_in_int_txn_id,
                 piv_source_table        => 'xxpo_receipt_hdr_stg',
                 piv_source_column_name  => 'leg_receipt_num',
                 piv_source_column_value => p_in_po_rcpt_number,
                 piv_source_keyname1     => null,
                 piv_source_keyvalue1    => null,
                 piv_error_type          => 'ERR_VAL',
                 piv_error_code          => l_err_code,
                 piv_error_message       => l_err_msg);
      return lv_status;
  end xxpo_dup_rcpt;

  ---+================================================================================+
  ---|function NAME : xxpo_dup_stg_rcpt
  ---|DESCRIPTION   : This proc will check for duplicate Purchase order receipt in stg table
  ---+================================================================================+
  function xxpo_dup_stg_rcpt(p_in_int_txn_id     in number,
                             p_in_po_rcpt_number in varchar2,
                             p_in_opr_unit       in varchar2) return varchar2 is
    lv_status  varchar2(1) := 'N';
    lv_count   number := 0;
    l_err_code varchar2(40);
    l_err_msg  varchar2(2000);
  begin
    select count(leg_receipt_num)
      into lv_count
      from xxpo_receipt_hdr_stg
     where leg_receipt_num = p_in_po_rcpt_number
       and leg_operating_unit = p_in_opr_unit
       and batch_id = g_batch_id;
    if lv_count > 1 then
      lv_status  := 'Y';
      l_err_code := 'ETN_RCPT_DUPLICATE_DOC_NUM_STG';
      l_err_msg  := 'Error: Duplicate receipt number in the staging table. ';
      log_errors(pin_interface_txn_id    => p_in_int_txn_id,
                 piv_source_table        => 'xxpo_receipt_hdr_stg',
                 piv_source_column_name  => 'leg_receipt_num',
                 piv_source_column_value => p_in_po_rcpt_number,
                 piv_source_keyname1     => null,
                 piv_source_keyvalue1    => null,
                 piv_error_type          => 'ERR_VAL',
                 piv_error_code          => l_err_code,
                 piv_error_message       => l_err_msg);
    end if;
    return lv_status;
  exception
    when others then
      lv_status  := 'Y';
      l_err_code := 'ETN_RCPT_PROCEDURE_EXCEPTION';
      l_err_msg  := 'Error: Exception error in xxpo_dup_stg_rcpt procedure. ';
      log_errors(pin_interface_txn_id    => p_in_int_txn_id,
                 piv_source_table        => 'xxpo_receipt_hdr_stg',
                 piv_source_column_name  => 'leg_receipt_num',
                 piv_source_column_value => p_in_po_rcpt_number,
                 piv_source_keyname1     => null,
                 piv_source_keyvalue1    => null,
                 piv_error_type          => 'ERR_VAL',
                 piv_error_code          => l_err_code,
                 piv_error_message       => l_err_msg);
      return lv_status;
  end xxpo_dup_stg_rcpt;

  ---+================================================================================+
  -- PROCEDURE:           validate_gl_period
  -- DESCRIPTION:         Procedure to validate gl period for a given date and
  --                      operating unit combination
  ---+================================================================================+
  procedure validate_gl_period(p_in_interface_txn_id in number,
                               p_in_org_name         in varchar2,
                               p_in_stg_tbl_name     in varchar2,
                               p_in_column_name      in varchar2,
                               p_in_date             in date,
                               p_in_org_id           in number,
                               x_out_flag            out varchar2) is
    v_lgr_id   number := null;
    l_err_code varchar2(40);
    l_err_msg  varchar2(2000);
  begin
    x_out_flag := 'N';
    begin
      select set_of_books_id
        into v_lgr_id
        from hr_operating_units hou
       where hou.organization_id = p_in_org_id;
    exception
      when others then
        v_lgr_id := null;
    end;
    select 'N'
      into x_out_flag
      from gl_period_statuses gps, fnd_application fa
     where gps.set_of_books_id = v_lgr_id
       and gps.closing_status = 'O'
       and gps.application_id = fa.application_id
       and fa.application_short_name = 'SQLGL'
       and trunc(p_in_date) between trunc(gps.start_date) and
           trunc(gps.end_date);
  exception
    when no_data_found then
      x_out_flag := 'Y';
      l_err_code := 'ETN_RCPT_PERIOD_NOT_OPEN';
      l_err_msg  := 'Error: Period not open for Operating unit : ' ||
                    p_in_org_name;
      log_errors(pin_interface_txn_id    => p_in_interface_txn_id,
                 piv_source_table        => p_in_stg_tbl_name,
                 piv_source_column_name  => p_in_column_name,
                 piv_source_column_value => p_in_date,
                 piv_source_keyname1     => null,
                 piv_source_keyvalue1    => null,
                 piv_error_type          => 'ERR_VAL',
                 piv_error_code          => l_err_code,
                 piv_error_message       => l_err_msg);
    when others then
      x_out_flag := 'Y';
      l_err_code := 'ETN_RCPT_PERIOD_NOT_OPEN';
      l_err_msg  := 'Error: Exceptional error while validating period for Operating unit : ' ||
                    p_in_org_name;
      log_errors(pin_interface_txn_id    => p_in_interface_txn_id,
                 piv_source_table        => p_in_stg_tbl_name,
                 piv_source_column_name  => p_in_column_name,
                 piv_source_column_value => p_in_date,
                 piv_source_keyname1     => null,
                 piv_source_keyvalue1    => null,
                 piv_error_type          => 'ERR_VAL',
                 piv_error_code          => l_err_code,
                 piv_error_message       => l_err_msg);
  end validate_gl_period;

  /*
     +================================================================================+
    |FUNCTION NAME      : validate_operating_unit                                        |
    |DESCRIPTION        : This function will return ORG_ID                     |
    +================================================================================+
  */
  /*  PROCEDURE validate_operating_unit (
     p_in_interface_txn_id   IN       NUMBER,
     p_in_stg_tbl_name       IN       VARCHAR2,
     p_in_column_name        IN       VARCHAR2,
     p_in_oper_unit_name     IN       VARCHAR2,
     p_org_name              OUT      VARCHAR2,
     p_org_id                OUT      NUMBER
  )
  IS
     l_err_code   VARCHAR2 (40);
     l_err_msg    VARCHAR2 (2000);
  BEGIN
     p_org_name := NULL;
     p_org_id := NULL;

     BEGIN
        SELECT TRIM (flv.description)
          INTO p_org_name
          FROM fnd_lookup_values flv
         WHERE TRIM (UPPER (flv.meaning)) =
                                           TRIM (UPPER (p_in_oper_unit_name))
           AND flv.LANGUAGE = USERENV ('LANG')
           AND flv.enabled_flag = 'Y'
           AND UPPER (flv.lookup_type) = g_ou_lookup
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
           l_err_code := 'ETN_RCPT_OPERATING_UNIT_ERROR';
           l_err_msg :=
                      'Error: Mapping not defined in the Common OU lookup. ';
           log_errors (pin_interface_txn_id         => p_in_interface_txn_id,
                       piv_source_table             => p_in_stg_tbl_name,
                       piv_source_column_name       => p_in_column_name,
                       piv_source_column_value      => p_in_oper_unit_name,
                       piv_source_keyname1          => NULL,
                       piv_source_keyvalue1         => NULL,
                       piv_error_type               => 'ERR_VAL',
                       piv_error_code               => l_err_code,
                       piv_error_message            => l_err_msg
                      );
        WHEN OTHERS
        THEN
           l_err_code := 'ETN_RCPT_OPERATING_UNIT_ERROR';
           l_err_msg :=
              'Error: Exception error while deriving operating unit from Common OU lookup. ';
           log_errors (pin_interface_txn_id         => p_in_interface_txn_id,
                       piv_source_table             => p_in_stg_tbl_name,
                       piv_source_column_name       => p_in_column_name,
                       piv_source_column_value      => p_in_oper_unit_name,
                       piv_source_keyname1          => NULL,
                       piv_source_keyvalue1         => NULL,
                       piv_error_type               => 'ERR_VAL',
                       piv_error_code               => l_err_code,
                       piv_error_message            => l_err_msg
                      );
     END;

     BEGIN
        SELECT organization_id
          INTO p_org_id
          FROM hr_operating_units
         WHERE NAME = p_org_name
           AND SYSDATE BETWEEN NVL (date_from, SYSDATE)
                           AND NVL (date_to, SYSDATE);
     EXCEPTION
        WHEN OTHERS
        THEN
           l_err_code := 'ETN_RCPT_OPERATING_UNIT_ERROR';
           l_err_msg := 'Error: Org ID could not be derived. ';
           log_errors (pin_interface_txn_id         => p_in_interface_txn_id,
                       piv_source_table             => p_in_stg_tbl_name,
                       piv_source_column_name       => p_in_column_name,
                       piv_source_column_value      => p_in_oper_unit_name,
                       piv_source_keyname1          => NULL,
                       piv_source_keyvalue1         => NULL,
                       piv_error_type               => 'ERR_VAL',
                       piv_error_code               => l_err_code,
                       piv_error_message            => l_err_msg
                      );
     END;
  EXCEPTION
     WHEN OTHERS
     THEN
        l_err_code := 'ETN_RCPT_PROCEDURE_EXCEPTION';
        l_err_msg :=
            'Error: EXCEPTION error for validate_operating_unit procedure. ';
        log_errors (pin_interface_txn_id         => p_in_interface_txn_id,
                    piv_source_table             => p_in_stg_tbl_name,
                    piv_source_column_name       => p_in_column_name,
                    piv_source_column_value      => p_in_oper_unit_name,
                    piv_source_keyname1          => NULL,
                    piv_source_keyvalue1         => NULL,
                    piv_error_type               => 'ERR_VAL',
                    piv_error_code               => l_err_code,
                    piv_error_message            => l_err_msg
                   );
  END validate_operating_unit; */
  ---+================================================================================+
  -- PROCEDURE:           validate_po
  -- DESCRIPTION:         Validate PO number
  ---+================================================================================+
  procedure validate_po(p_in_interface_txn_id in number,
                        p_in_po_number        in varchar2,
                        p_in_stg_tbl_name     in varchar2,
                        p_in_column_name      in varchar2,
                        p_in_org_id           in number,
                        x_out_emp_id          out number,
                        x_out_ship_to_org_id  out number,
                        x_out_vendor_id       out number,
                        x_out_vendor_site_id  out number,
                        x_out_ship_to_loc_id  out number,
                        x_out_freight         out varchar2,
                        x_out_date            out date,
                        x_out_flag            out varchar2) is
    l_err_code varchar2(40);
    l_err_msg  varchar2(2000);
  begin
    x_out_emp_id         := null;
    x_out_ship_to_org_id := null;
    x_out_vendor_id      := null;
    x_out_vendor_site_id := null;
    x_out_ship_to_loc_id := null;
    x_out_freight        := null;
    x_out_date           := null;
    x_out_flag           := 'N';
    select poh.agent_id,
           poll.ship_to_organization_id,
           poh.vendor_id,
           poh.vendor_site_id,
           poll.ship_to_location_id,
           poh.freight_terms_lookup_code,
           trunc(poh.creation_date)
      into x_out_emp_id,
           x_out_ship_to_org_id,
           x_out_vendor_id,
           x_out_vendor_site_id,
           x_out_ship_to_loc_id,
           x_out_freight,
           x_out_date
      from po_headers_all poh, po_lines_all pol, po_line_locations_all poll
     where poh.segment1 = p_in_po_number
       and poh.po_header_id = pol.po_header_id
       and poh.org_id = p_in_org_id
       and poll.po_header_id = pol.po_header_id
       and poll.po_line_id = pol.po_line_id
       and rownum = 1;
  exception
    when no_data_found then
      x_out_flag := 'Y';
      l_err_code := 'ETN_RCPT_INVALID_PO';
      l_err_msg  := 'Error: PO number is not present in the system. ';
      log_errors(pin_interface_txn_id    => p_in_interface_txn_id,
                 piv_source_table        => p_in_stg_tbl_name,
                 piv_source_column_name  => p_in_column_name,
                 piv_source_column_value => p_in_po_number,
                 piv_source_keyname1     => null,
                 piv_source_keyvalue1    => null,
                 piv_error_type          => 'ERR_VAL',
                 piv_error_code          => l_err_code,
                 piv_error_message       => l_err_msg);
    when others then
      x_out_flag := 'Y';
      l_err_code := 'ETN_RCPT_INVALID_PO';
      l_err_msg  := 'Error: Exceptional error while fetching PO details. ';
      log_errors(pin_interface_txn_id    => p_in_interface_txn_id,
                 piv_source_table        => p_in_stg_tbl_name,
                 piv_source_column_name  => p_in_column_name,
                 piv_source_column_value => p_in_po_number,
                 piv_source_keyname1     => null,
                 piv_source_keyvalue1    => null,
                 piv_error_type          => 'ERR_VAL',
                 piv_error_code          => l_err_code,
                 piv_error_message       => l_err_msg);
  end validate_po;

  /*    +================================================================================+
  |FUNCTION NAME : VALIDATE_CURRENCY_CODE                                               |
  |DESCRIPTION   : This function will check/validate Invoice currency codes   |
  +================================================================================+ */
  function validate_currency_code(p_in_interface_txn_id  in number,
                                  p_in_stg_tbl_name      in varchar2,
                                  p_in_column_name       in varchar2,
                                  p_in_invoice_curr_code in varchar2)
    return varchar2 is
    lv_status  varchar2(1) := 'N';
    l_err_code varchar2(40);
    l_err_msg  varchar2(2000);
  begin
    select 'N'
      into lv_status
      from fnd_currencies
     where upper(currency_code) = upper(p_in_invoice_curr_code)
       and sysdate between nvl(start_date_active, sysdate) and
           nvl(end_date_active, sysdate)
       and enabled_flag = 'Y';
    return lv_status;
  exception
    when no_data_found then
      l_err_code := 'ETN_RCPT_INVALID_CURRENCY_CODE';
      l_err_msg  := 'Error: Invalid Currency code. ';
      log_errors(pin_interface_txn_id    => p_in_interface_txn_id,
                 piv_source_table        => p_in_stg_tbl_name,
                 piv_source_column_name  => p_in_column_name,
                 piv_source_column_value => p_in_invoice_curr_code,
                 piv_source_keyname1     => null,
                 piv_source_keyvalue1    => null,
                 piv_error_type          => 'ERR_VAL',
                 piv_error_code          => l_err_code,
                 piv_error_message       => l_err_msg);
      lv_status := 'Y';
      return lv_status;
    when others then
      l_err_code := 'ETN_RCPT_PROCEDURE_EXCEPTION';
      l_err_msg  := 'Error: Exception error in validate_currency_code procedure. ';
      log_errors(pin_interface_txn_id    => p_in_interface_txn_id,
                 piv_source_table        => p_in_stg_tbl_name,
                 piv_source_column_name  => p_in_column_name,
                 piv_source_column_value => p_in_invoice_curr_code,
                 piv_source_keyname1     => null,
                 piv_source_keyvalue1    => null,
                 piv_error_type          => 'ERR_VAL',
                 piv_error_code          => l_err_code,
                 piv_error_message       => l_err_msg);
      lv_status := 'Y';
      return lv_status;
  end validate_currency_code;

  /*************************************************************************
  *
  * FUNCTION:           validate_uom_code
  *
  * DESCRIPTION:         Procedure to validate Unit Of Measure Code
  *************************************************************************/
  function validate_uom_code(p_in_interface_txn_id in number,
                             p_in_stg_tbl_name     in varchar2,
                             p_in_column_name      in varchar2,
                             p_in_uom_code         in varchar2)
    return varchar2 is
    lv_status  varchar2(1) := 'N';
    l_err_code varchar2(40);
    l_err_msg  varchar2(2000);
  begin
    select 'N'
      into lv_status
      from mtl_units_of_measure muom
     where muom.uom_code = p_in_uom_code
       and muom.language = userenv('LANG');
    return lv_status;
  exception
    when no_data_found then
      l_err_code := 'ETN_RCPT_INVALID_UOM_CODE';
      l_err_msg  := 'Error: Invalid UOM code. ';
      log_errors(pin_interface_txn_id    => p_in_interface_txn_id,
                 piv_source_table        => p_in_stg_tbl_name,
                 piv_source_column_name  => p_in_column_name,
                 piv_source_column_value => p_in_uom_code,
                 piv_source_keyname1     => null,
                 piv_source_keyvalue1    => null,
                 piv_error_type          => 'ERR_VAL',
                 piv_error_code          => l_err_code,
                 piv_error_message       => l_err_msg);
      lv_status := 'Y';
      return lv_status;
    when others then
      l_err_code := 'ETN_RCPT_PROCEDURE_EXCEPTION';
      l_err_msg  := 'Error: Exception error in validate_uom_code procedure. ';
      log_errors(pin_interface_txn_id    => p_in_interface_txn_id,
                 piv_source_table        => p_in_stg_tbl_name,
                 piv_source_column_name  => p_in_column_name,
                 piv_source_column_value => p_in_uom_code,
                 piv_source_keyname1     => null,
                 piv_source_keyvalue1    => null,
                 piv_error_type          => 'ERR_VAL',
                 piv_error_code          => l_err_code,
                 piv_error_message       => l_err_msg);
      lv_status := 'Y';
      return lv_status;
  end validate_uom_code;

  /*************************************************************************
  *
  * PROCEDURE:           validate_po_number
  *
  * DESCRIPTION:         Procedure to validate Purchase Order Number
  *************************************************************************/
  procedure validate_po_number(p_in_interface_txn_id in number,
                               p_in_stg_tbl_name     in varchar2,
                               p_in_column_name      in varchar2,
                               p_in_po_number        in varchar2,
                               p_in_org_id           in number,
                               x_out_po_header_id    out number,
                               x_out_emp_id          out number,
                               x_out_vendor_id       out number,
                               x_out_vendor_site_id  out number,
                               x_out_date            out date,
                               x_out_flag            out varchar2) is
    l_err_code varchar2(40);
    l_err_msg  varchar2(2000);
  begin
    x_out_po_header_id   := null;
    x_out_emp_id         := null;
    x_out_vendor_id      := null;
    x_out_vendor_site_id := null;
    x_out_flag           := 'N';
    x_out_date           := null;
    select po_header_id,
           agent_id,
           vendor_id,
           vendor_site_id,
           trunc(creation_date)
      into x_out_po_header_id,
           x_out_emp_id,
           x_out_vendor_id,
           x_out_vendor_site_id,
           x_out_date
      from po_headers_all poh
     where poh.segment1 = p_in_po_number
       and org_id = p_in_org_id
       and authorization_status = 'APPROVED';
  exception
    when no_data_found then
      l_err_code := 'ETN_RCPT_INVALID_PO';
      l_err_msg  := 'Error: PO number not present in the system. ';
      log_errors(pin_interface_txn_id    => p_in_interface_txn_id,
                 piv_source_table        => p_in_stg_tbl_name,
                 piv_source_column_name  => p_in_column_name,
                 piv_source_column_value => p_in_po_number,
                 piv_source_keyname1     => null,
                 piv_source_keyvalue1    => null,
                 piv_error_type          => 'ERR_VAL',
                 piv_error_code          => l_err_code,
                 piv_error_message       => l_err_msg);
      x_out_flag := 'Y';
    when others then
      l_err_code := 'ETN_RCPT_PROCEDURE_EXCEPTION';
      l_err_msg  := 'Error: Exception error in validate_po_number procedure. ';
      log_errors(pin_interface_txn_id    => p_in_interface_txn_id,
                 piv_source_table        => p_in_stg_tbl_name,
                 piv_source_column_name  => p_in_column_name,
                 piv_source_column_value => p_in_po_number,
                 piv_source_keyname1     => null,
                 piv_source_keyvalue1    => null,
                 piv_error_type          => 'ERR_VAL',
                 piv_error_code          => l_err_code,
                 piv_error_message       => l_err_msg);
      x_out_flag := 'Y';
  end validate_po_number;

  /*************************************************************************
  *
  * PROCEDURE:           validate_po_line_number
  *
  * DESCRIPTION:         Procedure to validate Purchase Order Line Number
  *************************************************************************/
  procedure validate_po_line_number(p_in_interface_txn_id   in number,
                                    p_in_po_line_number     in number,
                                    p_in_po_shipment_number in number,
                                    p_in_stg_tbl_name       in varchar2,
                                    p_in_column_name        in varchar2,
                                    p_in_po_header_id       in number,
                                    p_in_org_id             in number,
                                    x_out_po_line_id        out number,
                                    x_out_line_num          out po_lines_all.line_num%type,
                                    x_out_item              out varchar2,
                                    x_out_cat_id            out number,
                                    x_out_item_desc         out varchar2,
                                    x_out_flag              out varchar2) is
    l_err_code varchar2(40);
    l_err_msg  varchar2(2000);
  begin
    x_out_po_line_id := null;
    x_out_item       := null;
    x_out_cat_id     := null;
    x_out_item_desc  := null;
    x_out_flag       := 'N';
    select pol.po_line_id,
           pol.item_id,
           pol.category_id,
           pol.item_description,
           pol.line_num
      into x_out_po_line_id,
           x_out_item,
           x_out_cat_id,
           x_out_item_desc,
           x_out_line_num
      from po_lines_all pol, po_line_locations_all poll
     where pol.po_header_id = p_in_po_header_id
       and pol.po_header_id = poll.po_header_id
       and pol.po_line_id = poll.po_line_id
       and poll.attribute6 = p_in_po_line_number
       and poll.attribute7 = p_in_po_shipment_number;
  exception
    when no_data_found then
      l_err_code := 'ETN_RCPT_INVALID_PO_LINE_NUM';
      l_err_msg  := 'Error: PO line number not present in the system. ';
      log_errors(pin_interface_txn_id    => p_in_interface_txn_id,
                 piv_source_table        => p_in_stg_tbl_name,
                 piv_source_column_name  => p_in_column_name,
                 piv_source_column_value => p_in_po_line_number,
                 piv_source_keyname1     => null,
                 piv_source_keyvalue1    => null,
                 piv_error_type          => 'ERR_VAL',
                 piv_error_code          => l_err_code,
                 piv_error_message       => l_err_msg);
      x_out_flag := 'Y';
    when others then
      l_err_code := 'ETN_RCPT_PROCEDURE_EXCEPTION';
      l_err_msg  := 'Error: Exception error in validate_po_line_number procedure. ';
      log_errors(pin_interface_txn_id    => p_in_interface_txn_id,
                 piv_source_table        => p_in_stg_tbl_name,
                 piv_source_column_name  => p_in_column_name,
                 piv_source_column_value => p_in_po_line_number,
                 piv_source_keyname1     => null,
                 piv_source_keyvalue1    => null,
                 piv_error_type          => 'ERR_VAL',
                 piv_error_code          => l_err_code,
                 piv_error_message       => l_err_msg);
      x_out_flag := 'Y';
  end validate_po_line_number;

  /*************************************************************************
  *
  * PROCEDURE:           validate_po_shipment_number
  *
  * DESCRIPTION:         Procedure to validate Purchase Order Shipment Number
  *************************************************************************/
  procedure validate_po_shipment_number(p_in_interface_txn_id     in number,
                                        p_in_po_shipment_number   in number,
                                        p_in_stg_tbl_name         in varchar2,
                                        p_in_column_name          in varchar2,
                                        p_in_po_header_id         in number,
                                        p_in_po_line_id           in number,
                                        x_out_po_line_location_id out number,
                                        x_out_flag                out varchar2) is
    l_err_code varchar2(40);
    l_err_msg  varchar2(2000);
  begin
    x_out_po_line_location_id := null;
    x_out_flag                := 'N';
    select line_location_id
      into x_out_po_line_location_id
      from po_line_locations_all poll
     where poll.po_header_id = p_in_po_header_id
       and poll.po_line_id = p_in_po_line_id
       and poll.attribute7 = p_in_po_shipment_number; --SM MOCK3
  exception
    when no_data_found then
      l_err_code := 'ETN_RCPT_INVALID_PO_SHIPMENT_NUM';
      l_err_msg  := 'Error: PO shipment number not present in the system. ';
      log_errors(pin_interface_txn_id    => p_in_interface_txn_id,
                 piv_source_table        => p_in_stg_tbl_name,
                 piv_source_column_name  => p_in_column_name,
                 piv_source_column_value => p_in_po_shipment_number,
                 piv_source_keyname1     => null,
                 piv_source_keyvalue1    => null,
                 piv_error_type          => 'ERR_VAL',
                 piv_error_code          => l_err_code,
                 piv_error_message       => l_err_msg);
      x_out_flag := 'Y';
    when others then
      l_err_code := 'ETN_RCPT_PROCEDURE_EXCEPTION';
      l_err_msg  := 'Error: Exception error in validate_po_shipment_number procedure. ';
      log_errors(pin_interface_txn_id    => p_in_interface_txn_id,
                 piv_source_table        => p_in_stg_tbl_name,
                 piv_source_column_name  => p_in_column_name,
                 piv_source_column_value => p_in_po_shipment_number,
                 piv_source_keyname1     => null,
                 piv_source_keyvalue1    => null,
                 piv_error_type          => 'ERR_VAL',
                 piv_error_code          => l_err_code,
                 piv_error_message       => l_err_msg);
      x_out_flag := 'Y';
  end validate_po_shipment_number;

  /*************************************************************************
  *
  * PROCEDURE:           validate_po_dist_number
  *
  * DESCRIPTION:         Procedure to validate Purchase Order Distribution Number
  *************************************************************************/
  procedure validate_po_dist_number(p_in_interface_txn_id        in number,
                                    p_in_po_distribution_number  in number,
                                    p_in_stg_tbl_name            in varchar2,
                                    p_in_column_name             in varchar2,
                                    p_in_po_header_id            in number,
                                    p_in_po_line_id              in number,
                                    p_in_po_line_location_id     in number,
                                    x_out_po_distribution_id     out number,
                                    x_out_to_organization_id     out number,
                                    x_out_deliver_to_person_id   out number,
                                    x_out_deliver_to_location_id out number,
                                    x_out_ship_to_location_id    out number,
                                    x_out_dest_type_code         out varchar2,
                                    x_out_flag                   out varchar2) is
    l_err_code varchar2(40);
    l_err_msg  varchar2(2000);
  begin
    x_out_po_distribution_id     := null;
    x_out_to_organization_id     := null;
    x_out_deliver_to_person_id   := null;
    x_out_deliver_to_location_id := null;
    x_out_ship_to_location_id    := null;
    x_out_dest_type_code         := null;
    x_out_flag                   := 'N';
    select pod.po_distribution_id,
           pod.destination_organization_id,
           pod.deliver_to_person_id,
           pod.deliver_to_location_id,
           poll.ship_to_location_id,
           destination_type_code
      into x_out_po_distribution_id,
           x_out_to_organization_id,
           x_out_deliver_to_person_id,
           x_out_deliver_to_location_id,
           x_out_ship_to_location_id,
           x_out_dest_type_code
      from po_distributions_all pod, po_line_locations_all poll
     where pod.po_header_id = p_in_po_header_id
       and pod.po_line_id = p_in_po_line_id
       and pod.line_location_id = p_in_po_line_location_id
       and pod.distribution_num = p_in_po_distribution_number
       and pod.po_header_id = poll.po_header_id
       and pod.po_line_id = poll.po_line_id
       and pod.line_location_id = poll.line_location_id;
  exception
    when no_data_found then
      l_err_code := 'ETN_RCPT_INVALID_PO_SHIPMENT_NUM';
      l_err_msg  := 'Error: PO Distribution number not present in the system. ';
      log_errors(pin_interface_txn_id    => p_in_interface_txn_id,
                 piv_source_table        => p_in_stg_tbl_name,
                 piv_source_column_name  => p_in_column_name,
                 piv_source_column_value => p_in_po_distribution_number,
                 piv_source_keyname1     => null,
                 piv_source_keyvalue1    => null,
                 piv_error_type          => 'ERR_VAL',
                 piv_error_code          => l_err_code,
                 piv_error_message       => l_err_msg);
      x_out_flag := 'Y';
    when others then
      l_err_code := 'ETN_RCPT_PROCEDURE_EXCEPTION';
      l_err_msg  := 'Error: Exception error in validate_po_dist_number procedure. ';
      log_errors(pin_interface_txn_id    => p_in_interface_txn_id,
                 piv_source_table        => p_in_stg_tbl_name,
                 piv_source_column_name  => p_in_column_name,
                 piv_source_column_value => p_in_po_distribution_number,
                 piv_source_keyname1     => null,
                 piv_source_keyvalue1    => null,
                 piv_error_type          => 'ERR_VAL',
                 piv_error_code          => l_err_code,
                 piv_error_message       => l_err_msg);
      x_out_flag := 'Y';
  end validate_po_dist_number;

  /*************************************************************************
  *
  * FUNCTION:           validate_uom_code
  *
  * DESCRIPTION:         Procedure to validate Unit Of Measure Code
  *************************************************************************/
  function validate_subinv(p_in_interface_txn_id in number,
                           p_in_stg_tbl_name     in varchar2,
                           p_in_column_name      in varchar2,
                           p_in_subinv           in varchar2,
                           p_in_org_id           in number) return varchar2 is
    lv_status  varchar2(1) := 'N';
    l_err_code varchar2(40);
    l_err_msg  varchar2(2000);
  begin
    select 'N'
      into lv_status
      from mtl_secondary_inventories
     where organization_id = p_in_org_id
       and secondary_inventory_name = p_in_subinv
       and sysdate < nvl(disable_date, sysdate + 1);
    return lv_status;
  exception
    when no_data_found then
      l_err_code := 'ETN_RCPT_INVALID_SUBINV';
      l_err_msg  := 'Error: Invalid Subinventory code. ';
      log_errors(pin_interface_txn_id    => p_in_interface_txn_id,
                 piv_source_table        => p_in_stg_tbl_name,
                 piv_source_column_name  => p_in_column_name,
                 piv_source_column_value => p_in_subinv,
                 piv_source_keyname1     => null,
                 piv_source_keyvalue1    => null,
                 piv_error_type          => 'ERR_VAL',
                 piv_error_code          => l_err_code,
                 piv_error_message       => l_err_msg);
      lv_status := 'Y';
      return lv_status;
    when others then
      l_err_code := 'ETN_RCPT_PROCEDURE_EXCEPTION';
      l_err_msg  := 'Error: Exception error in validate_subinv procedure. ';
      log_errors(pin_interface_txn_id    => p_in_interface_txn_id,
                 piv_source_table        => p_in_stg_tbl_name,
                 piv_source_column_name  => p_in_column_name,
                 piv_source_column_value => p_in_subinv,
                 piv_source_keyname1     => null,
                 piv_source_keyvalue1    => null,
                 piv_error_type          => 'ERR_VAL',
                 piv_error_code          => l_err_code,
                 piv_error_message       => l_err_msg);
      lv_status := 'Y';
      return lv_status;
  end validate_subinv;

  /*
    +======================================================================================+
    |PROCEDURE NAME : tie_back                                                          |
    |DESCRIPTION   : This procedure tie back interface records with staging table records  |
    +======================================================================================+
  */
  procedure tie_back(x_ou_errbuf  out nocopy varchar2,
                     x_ou_retcode out nocopy number,
                     p_dummy      in varchar2,
                     pin_batch_id in number) is
    lv_int_head_id  number;
    lv_err_msg_code varchar2(30);
    lv_err_msg      varchar2(4000);
    l_debug_err     varchar2(2000);
    pov_ret_stats   varchar2(100);
    pov_err_msg     varchar2(1000);
    cursor cur_tie_back_head(p_batch number) is
      select xphs.interface_txn_id,
             phi.header_interface_id,
             xphs.leg_receipt_num,
             xphs.leg_operating_unit,
             phi.processing_status_code,
             leg_shipment_header_id
        from xxpo_receipt_hdr_stg xphs, rcv_headers_interface phi
       where xphs.batch_id = p_batch
         and xphs.interface_header_id = phi.header_interface_id
         and xphs.org_id = phi.org_id
         and xphs.process_flag = 'P';
    cursor cur_head_err(p_int_head_id number) is
      select interface_header_id,
             error_message_name,
             error_message,
             column_name,
             column_value
        from po_interface_errors
       where table_name = 'RCV_HEADERS_INTERFACE'
         and interface_header_id = p_int_head_id;
    cursor cur_tie_back_line(p_batch number) is
      select xpls.interface_txn_id,
             pli.header_interface_id,
             pli.interface_transaction_id,
             xpls.leg_po_number,
             pli.processing_status_code,
             pli.transaction_status_code
        from xxpo_receipt_trx_stg xpls, rcv_transactions_interface pli
       where xpls.batch_id = p_batch
         and pli.header_interface_id = xpls.interface_header_id
         and pli.interface_transaction_id = xpls.interface_transaction_id
            --AND xpls.leg_shipment_header_id = p_int_txn_id
         and xpls.process_flag = 'P';
    cursor cur_line_err(p_int_head_id number, p_int_line_id number) is
      select interface_line_id,
             error_message_name,
             error_message,
             column_name,
             column_value
        from po_interface_errors
       where table_name = 'RCV_TRANSACTIONS_INTERFACE'
         and interface_header_id = p_int_head_id
         and interface_line_id = p_int_line_id;
  begin
    print_log_message('Tie Back Starts at: ' ||
                      to_char(sysdate, 'DD-MON-YYYY HH24:MI:SS'));
    print_log_message('+ Start of Tie Back + ' || pin_batch_id);
    -- Initialize debug procedure
    xxetn_debug_pkg.initialize_debug(pov_err_msg      => l_debug_err,
                                     piv_program_name => 'PO_Receipt_Conv');
    xxetn_common_error_pkg.g_batch_id := pin_batch_id;
    -- batch id
    xxetn_common_error_pkg.g_run_seq_id := xxetn_run_sequences_s.nextval;
    -- run sequence id
    for cur_tie_back_head_rec in cur_tie_back_head(pin_batch_id) loop
      lv_err_msg_code := null;
      lv_err_msg      := null;
      if cur_tie_back_head_rec.processing_status_code = 'SUCCESS' then
        update xxpo_receipt_hdr_stg
           set process_flag      = 'C',
               error_type        = null,
               request_id        = g_request_id,
               last_updated_date = sysdate,
               last_updated_by   = g_last_updated_by,
               last_update_login = g_last_update_login
         where interface_txn_id = cur_tie_back_head_rec.interface_txn_id;
        xxetn_debug_pkg.add_debug('Record id ' ||
                                  cur_tie_back_head_rec.interface_txn_id ||
                                  'is successfully completed');
      elsif cur_tie_back_head_rec.processing_status_code = 'ERROR' then
        update xxpo_receipt_hdr_stg
           set process_flag      = 'E',
               error_type        = 'ERR_INT',
               request_id        = g_request_id,
               last_updated_date = sysdate,
               last_updated_by   = g_last_updated_by,
               last_update_login = g_last_update_login
         where interface_txn_id = cur_tie_back_head_rec.interface_txn_id;
        xxetn_debug_pkg.add_debug('Record id ' ||
                                  cur_tie_back_head_rec.interface_txn_id ||
                                  'is erred out');
        for cur_head_err_rec in cur_head_err(cur_tie_back_head_rec.header_interface_id) loop
          lv_err_msg := lv_err_msg || '||' ||
                        cur_head_err_rec.error_message;
        end loop;
        if lv_err_msg is not null then
          lv_err_msg_code := 'RCPT_INTERFACE_ERROR_RECORD';
          log_errors(pin_interface_txn_id    => cur_tie_back_head_rec.interface_txn_id,
                     piv_source_table        => 'xxpo_receipt_hdr_stg',
                     piv_source_column_name  => 'leg_receipt_num',
                     piv_source_column_value => cur_tie_back_head_rec.leg_receipt_num,
                     piv_source_keyname1     => null,
                     piv_source_keyvalue1    => null,
                     piv_error_type          => 'ERR_INT',
                     piv_error_code          => lv_err_msg_code,
                     piv_error_message       => lv_err_msg);
        else
          lv_err_msg_code := 'RCPT_INTERFACE_DEPENDENT_ERR';
          lv_err_msg      := 'Record erred out as one of the dependents Header/Transaction erred out.';
          log_errors(pin_interface_txn_id    => cur_tie_back_head_rec.interface_txn_id,
                     piv_source_table        => 'xxpo_receipt_hdr_stg',
                     piv_source_column_name  => 'leg_receipt_num',
                     piv_source_column_value => cur_tie_back_head_rec.leg_receipt_num,
                     piv_source_keyname1     => null,
                     piv_source_keyvalue1    => null,
                     piv_error_type          => 'ERR_INT',
                     piv_error_code          => lv_err_msg_code,
                     piv_error_message       => lv_err_msg);
        end if;
      end if;
    end loop;
    for cur_tie_back_line_rec in cur_tie_back_line(pin_batch_id) loop
      lv_err_msg_code := null;
      lv_err_msg      := null;
      if cur_tie_back_line_rec.processing_status_code = 'ERROR' or
         cur_tie_back_line_rec.transaction_status_code = 'ERROR' then
        update xxpo_receipt_trx_stg
           set process_flag      = 'E',
               error_type        = 'ERR_INT',
               request_id        = g_request_id,
               last_updated_date = sysdate,
               last_updated_by   = g_last_updated_by,
               last_update_login = g_last_update_login
         where interface_txn_id = cur_tie_back_line_rec.interface_txn_id;
        xxetn_debug_pkg.add_debug('Record id ' ||
                                  cur_tie_back_line_rec.interface_txn_id ||
                                  'is erred out');
        for cur_line_err_rec in cur_line_err(cur_tie_back_line_rec.header_interface_id,
                                             cur_tie_back_line_rec.interface_transaction_id) loop
          lv_err_msg := lv_err_msg || '||' ||
                        cur_line_err_rec.error_message;
        end loop;
        if lv_err_msg is not null then
          lv_err_msg_code := 'RCPT_INTERFACE_ERROR_RECORD';
          log_errors(pin_interface_txn_id    => cur_tie_back_line_rec.interface_txn_id,
                     piv_source_table        => 'xxpo_receipt_trx_stg',
                     piv_source_column_name  => 'leg_po_number',
                     piv_source_column_value => cur_tie_back_line_rec.leg_po_number,
                     piv_source_keyname1     => null,
                     piv_source_keyvalue1    => null,
                     piv_error_type          => 'ERR_INT',
                     piv_error_code          => lv_err_msg_code,
                     piv_error_message       => lv_err_msg);
        else
          lv_err_msg_code := 'RCPT_INTERFACE_DEPENDENT_ERR';
          lv_err_msg      := 'Record erred out as one of the dependents Header/Transaction erred out.';
          log_errors(pin_interface_txn_id    => cur_tie_back_line_rec.interface_txn_id,
                     piv_source_table        => 'xxpo_receipt_trx_stg',
                     piv_source_column_name  => 'leg_po_number',
                     piv_source_column_value => cur_tie_back_line_rec.leg_po_number,
                     piv_source_keyname1     => null,
                     piv_source_keyvalue1    => null,
                     piv_error_type          => 'ERR_INT',
                     piv_error_code          => lv_err_msg_code,
                     piv_error_message       => lv_err_msg);
        end if;
      end if;
    end loop;
    update xxpo_receipt_trx_stg
       set process_flag      = 'C',
           error_type        = null,
           request_id        = g_request_id,
           last_updated_date = sysdate,
           last_updated_by   = g_last_updated_by,
           last_update_login = g_last_update_login
     where batch_id = pin_batch_id
       and process_flag = 'P';
    xxetn_debug_pkg.add_debug('Number of lines updated to process flag C : ' ||
                              sql%rowcount);
    -- call once to dump pending error records which are less than profile value.
    xxetn_common_error_pkg.add_error(pov_return_status => pov_ret_stats,
                                     pov_error_msg     => pov_err_msg,
                                     pi_source_tab     => g_tab);
    commit;
  exception
    when others then
      fnd_file.put_line(fnd_file.log,
                        ' Unexpected  Error in po_tie_back procedure' ||
                        sqlerrm);
      x_ou_retcode := 2;
  end tie_back;

  /*
    +================================================================================+
    |PROCEDURE NAME : Validate_rcpt                                                  |
    |DESCRIPTION   : This procedure will used for Validations                        |
    +================================================================================+
  */
  procedure validate_rcpt is
    cursor cur_rcpt_headers is
      select /*+ INDEX (a XXPO_RECEIPT_HDR_STG_N3) */
       a.rowid, a.*
        from xxpo_receipt_hdr_stg a
       where a.process_flag = 'N'
         and a.batch_id = g_batch_id
         and a.run_sequence_id = g_new_run_seq_id;
    cursor cur_rcpt_trxn(p_in_txn_id         number,
                         p_leg_source_system in varchar2) is
      select a.rowid, a.*
        from xxpo_receipt_trx_stg a
       where a.process_flag in ('N', 'E')
         and a.batch_id = g_batch_id
         and a.run_sequence_id = g_new_run_seq_id
         and a.leg_shipment_header_id = p_in_txn_id;
    cursor cur_head_err(p_in_txn_id number) is
      select interface_txn_id,
             leg_receipt_num,
             leg_operating_unit,
             leg_shipment_header_id
        from xxpo_receipt_hdr_stg
       where interface_txn_id = p_in_txn_id
         and process_flag = 'V';
    cursor cur_txn_err(p_in_txn_id number) is
      select interface_txn_id, leg_po_number, leg_operating_unit
        from xxpo_receipt_trx_stg
       where leg_shipment_header_id = p_in_txn_id
         and process_flag = 'V';
    /* CURSOR cur_org
    IS
       SELECT DISTINCT leg_po_header_id,
                       leg_source_system
                  FROM xxpo_receipt_trx_stg
                 WHERE batch_id = g_batch_id
                   AND run_sequence_id = g_new_run_seq_id;

    CURSOR cur_org_trx (p_head_id IN NUMBER , p_leg IN VARCHAR2)
    IS
       SELECT DISTINCT leg_shipment_header_id
                      FROM xxpo_receipt_trx_stg
                     WHERE leg_po_header_id = p_head_id
                       AND leg_source_system = p_leg
                       AND batch_id = g_batch_id
                       AND run_sequence_id = g_new_run_seq_id;  */
    cursor cur_trx_ext is
      select /*+ INDEX (a XXPO_RECEIPT_TRX_STG_N3) */
       interface_txn_id
        from xxpo_receipt_trx_stg a
       where process_flag = 'N'
         and batch_id = g_batch_id
         and run_sequence_id = g_new_run_seq_id;
    cursor cur_hrd_org is
      select /*+ INDEX (a XXPO_RECEIPT_TRX_STG_N3) */
      distinct org_id,
               operating_unit,
               leg_shipment_header_id,
               leg_source_system
        from xxpo_receipt_trx_stg a
       where org_id is not null
         and batch_id = g_batch_id
         and run_sequence_id = g_new_run_seq_id;
    -------------------------- cursor inclusion ---------------------------------     SM
    cursor uom_map_issc is
      select flv.meaning, flv.description, flv.attribute1
        from apps.fnd_lookup_values flv
       where flv.language = userenv('LANG')
         and flv.enabled_flag = 'Y'
         and flv.lookup_type = 'XXINV_UOM_MAPPING'
         and flv.attribute1 is not null
         and trunc(sysdate) between
             trunc(nvl(flv.start_date_active, sysdate)) and
             trunc(nvl(flv.end_date_active, sysdate));
    cursor uom_map_nafsc is
      select flv.meaning, flv.description, flv.attribute2
        from apps.fnd_lookup_values flv
       where flv.language = userenv('LANG')
         and flv.enabled_flag = 'Y'
         and flv.lookup_type = 'XXINV_UOM_MAPPING'
         and flv.attribute2 is not null
         and trunc(sysdate) between
             trunc(nvl(flv.start_date_active, sysdate)) and
             trunc(nvl(flv.end_date_active, sysdate));
    -------------------------- cursor inclusion completed ---------------------------------  SM
    lv_error_flag            varchar2(1) := 'N';
    lv_val_flag              varchar2(1);
    lv_val_stg_flag          varchar2(1);
    lv_val_dup_flag          varchar2(1);
    l_err_code               varchar2(40);
    l_err_msg                varchar2(2000);
    lv_ship_date_flag        varchar2(1);
    lv_exp_date_flag         varchar2(1);
    lv_t_ship_date_flag      varchar2(1);
    lv_t_exp_date_flag       varchar2(1);
    lv_tran_date_flag        varchar2(1);
    lv_emp_id                number;
    lv_ship_to_org_id        number;
    lv_vendor_id             number;
    lv_vendor_site_id        number;
    lv_org_id                number;
    lv_org_name              varchar2(240);
    lv_po_date               date;
    lv_po_txn_date           date;
    lv_ship_date             date;
    lv_t_ship_date           date;
    lv_exp_rcv_date          date;
    lv_t_exp_rcv_date        date;
    lv_transaction_date      date;
    lv_ship_to_loc_id        number;
    lv_freight_terms         varchar2(25);
    lv_func_currency_code    varchar(15);
    lv_attribute_category    varchar2(30);
    lv_attribute1            varchar2(150);
    lv_attribute2            varchar2(150);
    lv_attribute3            varchar2(150);
    lv_attribute4            varchar2(150);
    lv_attribute5            varchar2(150);
    lv_attribute6            varchar2(150);
    lv_attribute7            varchar2(150);
    lv_attribute8            varchar2(150);
    lv_attribute9            varchar2(150);
    lv_attribute10           varchar2(150);
    lv_attribute11           varchar2(150);
    lv_attribute12           varchar2(150);
    lv_attribute13           varchar2(150);
    lv_attribute14           varchar2(150);
    lv_attribute15           varchar2(150);
    lv_t_attribute_category  varchar2(30);
    lv_t_attribute1          varchar2(150);
    lv_t_attribute2          varchar2(150);
    lv_t_attribute3          varchar2(150);
    lv_t_attribute4          varchar2(150);
    lv_t_attribute5          varchar2(150);
    lv_t_attribute6          varchar2(150);
    lv_t_attribute7          varchar2(150);
    lv_t_attribute8          varchar2(150);
    lv_t_attribute9          varchar2(150);
    lv_t_attribute10         varchar2(150);
    lv_t_attribute11         varchar2(150);
    lv_t_attribute12         varchar2(150);
    lv_t_attribute13         varchar2(150);
    lv_t_attribute14         varchar2(150);
    lv_t_attribute15         varchar2(150);
    lv_txn_error_flag        varchar2(1) := 'N';
    lv_mast_txv_error_flag   varchar2(1) := 'N';
    lv_txn_org_id            number;
    lv_txn_org_name          varchar2(240);
    lv_t_emp_id              number;
    lv_t_header_id           number;
    lv_t_vendor_id           number;
    lv_t_vendor_site_id      number;
    lv_t_item_id             number;
    lv_po_line_id            number;
    lv_line_num              po_lines_all.line_num%type;
    lv_t_category_id         number;
    lv_t_item_desc           varchar2(240);
    lv_t_line_loc_id         number;
    lv_po_dist_id            number;
    lv_t_ship_to_org_id      number;
    lv_t_delv_per_id         number;
    lv_delv_to_loc_id        number;
    lv_t_ship_to_loc_id      number;
    lv_c_d_m_flag            varchar2(1);
    lv_t_dest_type_code      varchar2(25);
    l_count                  number := 0;
    lv_delv_loc_final        number;
    lv_dev_to_loc_id_sys     number;
    lv_uom_description_issc  fnd_lookup_values.description%type; --  SM(UOM)
    lv_uom_attribute1_issc   fnd_lookup_values.attribute1%type; --  SM(UOM)
    lv_uom_description_nafsc fnd_lookup_values.description%type; -- SM(UOM)
    lv_uom_attribute2_nafsc  fnd_lookup_values.attribute2%type; --  SM(UOM)
    --today
    l_leg_new_uom xxpo_receipt_trx_stg.leg_uom_code%type;
    --today
  begin
    xxetn_debug_pkg.add_debug('INSIDE VALIDATE PROCEDURE ');
    g_retcode := 0;
    update /*+ INDEX (a XXPO_RECEIPT_TRX_STG_N3) */ xxpo_receipt_trx_stg a
       set (a.org_id, a.operating_unit) =
           (select b.org_id, b.operating_unit_name
              from xxpo_po_header_stg b
             where b.leg_po_header_id = a.leg_po_header_id
               and b.leg_source_system = a.leg_source_system
               and rownum = 1)
     where batch_id = g_batch_id
       and run_sequence_id = g_new_run_seq_id;
    commit; -- Added by Ankur for v1.6 --
    ----------------------update statement to update   leg_attribute10 with UOM Code ----------start----SM--MOCK3
    /* update xxpo_receipt_trx_stg a
      set a.leg_attribute10 =
          (select b.leg_uom_code
             from xxpo_receipt_trx_stg b
            where b.interface_txn_id = a.interface_txn_id
              and b.leg_source_system = a.leg_source_system
              and b.batch_id = a.batch_id
              and b.run_sequence_id = a.run_sequence_id
              and rownum = 1)
    where a.batch_id = g_batch_id
      and a.run_sequence_id = g_new_run_seq_id; */ --commented as per discussion SM
    ----------------------update statement to update   leg_attribute10 with UOM Code ----------end----SM--MOCK3
    /*       UPDATE xxpo_receipt_hdr_stg a
      SET (a.org_id, a.operating_unit) =
             (SELECT b.org_id,
                              b.operating_unit
                         FROM xxpo_receipt_trx_stg b
                        WHERE a.leg_shipment_header_id =
                                                   b.leg_shipment_header_id
                          AND a.leg_source_system = b.leg_source_system
                          AND batch_id = g_batch_id
                          AND run_sequence_id = g_new_run_seq_id
                          AND b.org_id IS NOT NULL
                          AND ROWNUM = 1)
    WHERE batch_id = g_batch_id AND run_sequence_id = g_new_run_seq_id; */
    /** Commented for v1.3 **/
    /**    FOR cur_hrd_org_rec IN cur_hrd_org LOOP
      UPDATE xxpo_receipt_hdr_stg
         SET org_id         = cur_hrd_org_rec.org_id,
             operating_unit = cur_hrd_org_rec.operating_unit
       WHERE leg_shipment_header_id =
             cur_hrd_org_rec.leg_shipment_header_id
         AND leg_source_system = cur_hrd_org_rec.leg_source_system
         AND batch_id = g_batch_id
         AND run_sequence_id = g_new_run_seq_id;
    END LOOP; **/
    /** Added for v1.3 **/
    update /*+ INDEX (a XXPO_RECEIPT_HDR_STG_N3) */ xxpo_receipt_hdr_stg a
       set (a.org_id, a.operating_unit) =
           (select /*+ INDEX (b XXPO_RECEIPT_TRX_STG_N5) */
             b.org_id, b.operating_unit
              from xxpo_receipt_trx_stg b
             where b.leg_shipment_header_id = a.leg_shipment_header_id
               and b.leg_source_system = a.leg_source_system
               and b.org_id is not null
               and b.batch_id = a.batch_id
               and b.run_sequence_id = a.run_sequence_id
               and rownum = 1)
     where a.batch_id = g_batch_id
       and a.run_sequence_id = g_new_run_seq_id;
    commit;
    for ref_org_error_rec in (select /*+ INDEX (a XXPO_RECEIPT_TRX_STG_N3) */
                               interface_txn_id
                                from xxpo_receipt_trx_stg a
                               where org_id is null
                                 and batch_id = g_batch_id
                                 and run_sequence_id = g_new_run_seq_id) loop
      l_err_code := 'ETN_PO_OPERATING_UNIT_ERROR';
      l_err_msg  := 'Error: Operating Unit could not be derived from PO Staging Table. ';
      log_errors(pin_interface_txn_id    => ref_org_error_rec.interface_txn_id,
                 piv_source_table        => 'xxpo_receipt_trx_stg',
                 piv_source_column_name  => null,
                 piv_source_column_value => null,
                 piv_source_keyname1     => null,
                 piv_source_keyvalue1    => null,
                 piv_error_type          => 'ERR_VAL',
                 piv_error_code          => l_err_code,
                 piv_error_message       => l_err_msg);
    end loop;
    update /*+ INDEX (a XXPO_RECEIPT_TRX_STG_N3) */ xxpo_receipt_trx_stg a
       set process_flag      = 'E',
           error_type        = 'ERR_VAL',
           last_updated_date = sysdate,
           last_updated_by   = g_last_updated_by,
           last_update_login = g_login_id
     where org_id is null
       and batch_id = g_batch_id
       and run_sequence_id = g_new_run_seq_id;
    commit;
    /* FOR cur_org_rec IN cur_org
    LOOP
       lv_org_id := NULL;
       lv_org_name := NULL;

       BEGIN
          SELECT org_id,operating_unit_name
            INTO lv_org_id,lv_org_name
            FROM xxpo_po_header_stg
           WHERE leg_po_header_id = cur_org_rec.leg_po_header_id
             AND leg_source_system = cur_org_rec.leg_source_system
             AND ROWNUM = 1;
       EXCEPTION
          WHEN OTHERS
          THEN
             FOR r_org_ref_err_rec IN
                (SELECT interface_txn_id
                   FROM xxpo_receipt_trx_stg xis
                  WHERE leg_po_header_id = cur_org_rec.leg_po_header_id
                    AND leg_source_system = cur_org_rec.leg_source_system
                    AND batch_id = g_batch_id
                    AND run_sequence_id = g_new_run_seq_id)
             LOOP
                l_err_code := 'ETN_PO_OPERATING_UNIT_ERROR';
                l_err_msg :=
                   'Error: Operating Unit could not be derived from PO Staging Table. ';
                log_errors
                   (pin_interface_txn_id         => r_org_ref_err_rec.interface_txn_id,
                    piv_source_table             => 'xxpo_receipt_trx_stg',
                    piv_source_column_name       => 'leg_po_header_id',
                    piv_source_column_value      => cur_org_rec.leg_po_header_id,
                    piv_source_keyname1          => NULL,
                    piv_source_keyvalue1         => NULL,
                    piv_error_type               => 'ERR_VAL',
                    piv_error_code               => l_err_code,
                    piv_error_message            => l_err_msg
                   );
             END LOOP;

             UPDATE xxpo_receipt_trx_stg
                SET process_flag = 'E',
                    ERROR_TYPE = 'ERR_VAL',
                    last_updated_date = SYSDATE,
                    last_updated_by = g_last_updated_by,
                    last_update_login = g_login_id
              WHERE leg_po_header_id = cur_org_rec.leg_po_header_id
                AND leg_source_system = cur_org_rec.leg_source_system
                AND batch_id = g_batch_id
                AND run_sequence_id = g_new_run_seq_id;

             COMMIT;
       END;

       IF lv_org_id IS NOT NULL
       THEN
          UPDATE xxpo_receipt_trx_stg
             SET org_id = lv_org_id,
                 operating_unit = lv_org_name,
                 last_updated_date = SYSDATE,
                 last_updated_by = g_last_updated_by,
                 last_update_login = g_login_id
           WHERE leg_po_header_id = cur_org_rec.leg_po_header_id
             AND leg_source_system = cur_org_rec.leg_source_system
             AND batch_id = g_batch_id
             AND run_sequence_id = g_new_run_seq_id;

          FOR cur_org_trx_rec IN cur_org_trx (cur_org_rec.leg_po_header_id,cur_org_rec.leg_source_system)
          LOOP
          UPDATE xxpo_receipt_hdr_stg
             SET org_id = lv_org_id,
                 operating_unit = lv_org_name,
                 last_updated_date = SYSDATE,
                 last_updated_by = g_last_updated_by,
                 last_update_login = g_login_id
           WHERE leg_shipment_header_id = cur_org_trx_rec.leg_shipment_header_id
             AND leg_source_system = cur_org_rec.leg_source_system
             AND batch_id = g_batch_id
             AND run_sequence_id = g_new_run_seq_id;
          END LOOP;
          COMMIT;
       END IF;
    END LOOP; */
    --- header cursor starts
    for cur_rcpt_headers_rec in cur_rcpt_headers loop
      lv_error_flag          := 'N';
      lv_mast_txv_error_flag := 'N';
      l_err_code             := null;
      l_err_msg              := null;
      lv_ship_date_flag      := 'N';
      lv_exp_date_flag       := 'N';
      lv_emp_id              := null;
      lv_ship_to_org_id      := null;
      lv_vendor_id           := null;
      lv_vendor_site_id      := null;
      lv_org_id              := null;
      lv_org_name            := null;
      lv_ship_date           := null;
      lv_ship_to_loc_id      := null;
      lv_freight_terms       := null;
      lv_func_currency_code  := null;
      lv_attribute_category  := null;
      lv_po_date             := null;
      lv_ship_date           := null;
      lv_exp_rcv_date        := null;
      lv_attribute_category  := null;
      lv_attribute1          := null;
      lv_attribute2          := null;
      lv_attribute3          := null;
      lv_attribute4          := null;
      lv_attribute5          := null;
      lv_attribute6          := null;
      lv_attribute7          := null;
      lv_attribute8          := null;
      lv_attribute9          := null;
      lv_attribute10         := null;
      lv_attribute11         := null;
      lv_attribute12         := null;
      lv_attribute13         := null;
      lv_attribute14         := null;
      lv_attribute15         := null;
      xxetn_debug_pkg.add_debug('+---------------------------------------------------------------------------------+');
      xxetn_debug_pkg.add_debug('CALLING FUNCTION: xxpo_dup_rcpt for receipt num ' ||
                                cur_rcpt_headers_rec.leg_receipt_num);
      --- Validate receipt number
      if cur_rcpt_headers_rec.leg_receipt_num is not null then
        lv_val_dup_flag := 'N';
        lv_val_stg_flag := 'N';
        lv_val_dup_flag := xxpo_dup_rcpt(cur_rcpt_headers_rec.interface_txn_id,
                                         cur_rcpt_headers_rec.leg_receipt_num);
        /* lv_val_stg_flag :=
        xxpo_dup_stg_rcpt (cur_rcpt_headers_rec.interface_txn_id,
                           cur_rcpt_headers_rec.leg_receipt_num,
                           cur_rcpt_headers_rec.leg_operating_unit
                          ); */
        if lv_val_dup_flag = 'Y' or lv_val_stg_flag = 'Y' then
          lv_error_flag := 'Y';
        end if;
      else
        lv_error_flag := 'Y';
        l_err_code    := 'ETN_RCPT_MANDATORY_NOT_ENTERED';
        l_err_msg     := 'Error: Mandatory column not entered.  ';
        log_errors(pin_interface_txn_id    => cur_rcpt_headers_rec.interface_txn_id,
                   piv_source_table        => 'xxpo_receipt_hdr_stg',
                   piv_source_column_name  => 'leg_receipt_num',
                   piv_source_column_value => cur_rcpt_headers_rec.leg_receipt_num,
                   piv_source_keyname1     => null,
                   piv_source_keyvalue1    => null,
                   piv_error_type          => 'ERR_VAL',
                   piv_error_code          => l_err_code,
                   piv_error_message       => l_err_msg);
      end if;
      --- validate LEG_RECEIPT_SOURCE_CODE
      if cur_rcpt_headers_rec.leg_receipt_source_code <> 'VENDOR' then
        lv_error_flag := 'Y';
        l_err_code    := 'ETN_RCPT_INVALID_SOURCE_CODE';
        l_err_msg     := 'Error: Invalid Receipt source code.  ';
        log_errors(pin_interface_txn_id    => cur_rcpt_headers_rec.interface_txn_id,
                   piv_source_table        => 'xxpo_receipt_hdr_stg',
                   piv_source_column_name  => 'leg_receipt_source_code',
                   piv_source_column_value => cur_rcpt_headers_rec.leg_receipt_source_code,
                   piv_source_keyname1     => null,
                   piv_source_keyvalue1    => null,
                   piv_error_type          => 'ERR_VAL',
                   piv_error_code          => l_err_code,
                   piv_error_message       => l_err_msg);
      end if;
      /* xxetn_debug_pkg.add_debug
           (   'CALLING FUNCTION: validate_operating_unit for receipt num '
            || cur_rcpt_headers_rec.leg_receipt_num
           );

      --- derive operating unit
      IF cur_rcpt_headers_rec.leg_operating_unit IS NOT NULL
      THEN
         validate_operating_unit (cur_rcpt_headers_rec.interface_txn_id,
                                  'xxpo_receipt_hdr_stg',
                                  'leg_operating_unit',
                                  cur_rcpt_headers_rec.leg_operating_unit,
                                  lv_org_name,
                                  lv_org_id
                                 );

         IF lv_org_id IS NULL
         THEN
            lv_error_flag := 'Y';
         END IF;
      ELSE
         lv_error_flag := 'Y';
         l_err_code := 'ETN_RCPT_MANDATORY_NOT_ENTERED';
         l_err_msg := 'Error: Mandatory column not entered.  ';
         log_errors
            (pin_interface_txn_id         => cur_rcpt_headers_rec.interface_txn_id,
             piv_source_table             => 'xxpo_receipt_hdr_stg',
             piv_source_column_name       => 'leg_operating_unit',
             piv_source_column_value      => cur_rcpt_headers_rec.leg_operating_unit,
             piv_source_keyname1          => NULL,
             piv_source_keyvalue1         => NULL,
             piv_error_type               => 'ERR_VAL',
             piv_error_code               => l_err_code,
             piv_error_message            => l_err_msg
            );
      END IF; */
      --- Derive values from po   --1.1
      /* xxetn_debug_pkg.add_debug (   'CALLING PROC: validate_po for  '
                                 || cur_rcpt_headers_rec.leg_receipt_num
                                );

      IF cur_rcpt_headers_rec.leg_po_number IS NOT NULL
      THEN
         lv_val_flag := 'N';
         validate_po (cur_rcpt_headers_rec.interface_txn_id,
                      cur_rcpt_headers_rec.leg_po_number,
                      'xxpo_receipt_hdr_stg',
                      'leg_po_number',
                      cur_rcpt_headers_rec.org_id,
                     lv_emp_id,
                      lv_ship_to_org_id,
                      lv_vendor_id,
                      lv_vendor_site_id,
                      lv_ship_to_loc_id,
                      lv_freight_terms,
                      lv_po_date,
                      lv_val_flag
                     );

         IF lv_val_flag = 'Y'
         THEN
            lv_error_flag := 'Y';
         END IF;
      ELSE
         lv_error_flag := 'Y';
         l_err_code := 'ETN_RCPT_MANDATORY_NOT_ENTERED';
         l_err_msg := 'Error: Mandatory column not entered.  ';
         log_errors
            (pin_interface_txn_id         => cur_rcpt_headers_rec.interface_txn_id,
             piv_source_table             => 'xxpo_receipt_hdr_stg',
             piv_source_column_name       => 'leg_po_number',
             piv_source_column_value      => cur_rcpt_headers_rec.leg_po_number,
             piv_source_keyname1          => NULL,
             piv_source_keyvalue1         => NULL,
             piv_error_type               => 'ERR_VAL',
             piv_error_code               => l_err_code,
             piv_error_message            => l_err_msg
            );
      END IF; */
      /* --- Validate shipped and expected receipt date  --1.1
      xxetn_debug_pkg.add_debug
                               (   'CALLING PROC: validate_gl_period for  '
                                || cur_rcpt_headers_rec.leg_receipt_num
                               );

      IF cur_rcpt_headers_rec.org_id IS NOT NULL AND lv_po_date IS NOT NULL
      THEN
         IF NVL (cur_rcpt_headers_rec.leg_shipped_date, SYSDATE) >
                                                                lv_po_date
         THEN
            lv_ship_date :=
               TRUNC (NVL (cur_rcpt_headers_rec.leg_shipped_date, SYSDATE));
         ELSE
            lv_ship_date := lv_po_date;
         END IF;

         IF NVL (cur_rcpt_headers_rec.leg_expected_receipt_date, SYSDATE) >
                                                                 lv_po_date
         THEN
            lv_exp_rcv_date :=
               TRUNC (NVL (cur_rcpt_headers_rec.leg_expected_receipt_date,
                           SYSDATE
                          )
                     );
         ELSE
            lv_exp_rcv_date := lv_po_date;
         END IF;

         validate_gl_period (cur_rcpt_headers_rec.interface_txn_id,
                             cur_rcpt_headers_rec.leg_operating_unit,
                             'xxpo_receipt_hdr_stg',
                             'leg_shipped_date',
                             lv_ship_date,
                             cur_rcpt_headers_rec.org_id,
                             lv_ship_date_flag
                            );
         validate_gl_period (cur_rcpt_headers_rec.interface_txn_id,
                             cur_rcpt_headers_rec.leg_operating_unit,
                             'xxpo_receipt_hdr_stg',
                             'leg_expected_receipt_date',
                             lv_exp_rcv_date,
                             cur_rcpt_headers_rec.org_id,
                             lv_exp_date_flag
                            );

         IF lv_exp_date_flag = 'Y' OR lv_ship_date_flag = 'Y'
         THEN
            lv_error_flag := 'Y';
         END IF;
      END IF; */
      --Validate Currency code
      if cur_rcpt_headers_rec.leg_currency_code is not null then
        lv_val_flag := 'N';
        lv_val_flag := validate_currency_code(cur_rcpt_headers_rec.interface_txn_id,
                                              'xxpo_receipt_hdr_stg',
                                              'leg_currency_code',
                                              cur_rcpt_headers_rec.leg_currency_code);
        if lv_val_flag = 'Y' then
          lv_error_flag := 'Y';
        end if;
        --- deriving foreign currency code
        if cur_rcpt_headers_rec.org_id is not null then
          begin
            select currency_code
              into lv_func_currency_code
              from gl_sets_of_books
             where set_of_books_id =
                   (select set_of_books_id
                      from hr_operating_units
                     where organization_id = cur_rcpt_headers_rec.org_id);
          exception
            when others then
              lv_func_currency_code := null;
          end;
        end if;
        --- setting rate type for base currency
        if cur_rcpt_headers_rec.leg_currency_code = lv_func_currency_code then
          update xxpo_receipt_hdr_stg
             set leg_conversion_rate_type = null,
                 leg_conversion_rate      = null,
                 leg_conversion_rate_date = null
           where interface_txn_id = cur_rcpt_headers_rec.interface_txn_id;
        else
          update xxpo_receipt_hdr_stg
             set leg_conversion_rate_type = 'User'
           where interface_txn_id = cur_rcpt_headers_rec.interface_txn_id;
        end if;
        --- checking rate type and rate
        if cur_rcpt_headers_rec.leg_currency_code <> lv_func_currency_code then
          if cur_rcpt_headers_rec.leg_conversion_rate is null then
            lv_error_flag := 'Y';
            l_err_code    := 'ETN_RCPT_INVALID_EXCHANGE_RATE';
            l_err_msg     := 'Error: Exchange rate cannot be NULL for USER rate type  ';
            log_errors(pin_interface_txn_id    => cur_rcpt_headers_rec.interface_txn_id,
                       piv_source_table        => 'xxpo_receipt_hdr_stg',
                       piv_source_column_name  => 'leg_conversion_rate',
                       piv_source_column_value => cur_rcpt_headers_rec.leg_conversion_rate,
                       piv_source_keyname1     => null,
                       piv_source_keyvalue1    => null,
                       piv_error_type          => 'ERR_VAL',
                       piv_error_code          => l_err_code,
                       piv_error_message       => l_err_msg);
          end if;
        end if;
      end if;
      ---assigning DFF
      lv_attribute_category := cur_rcpt_headers_rec.leg_attribute_category;
      lv_attribute1         := cur_rcpt_headers_rec.leg_attribute1;
      lv_attribute2         := cur_rcpt_headers_rec.leg_attribute2;
      lv_attribute3         := cur_rcpt_headers_rec.leg_attribute3;
      lv_attribute4         := cur_rcpt_headers_rec.leg_attribute4;
      lv_attribute5         := cur_rcpt_headers_rec.leg_attribute5;
      lv_attribute6         := cur_rcpt_headers_rec.leg_attribute6;
      lv_attribute7         := cur_rcpt_headers_rec.leg_attribute7;
      lv_attribute8         := cur_rcpt_headers_rec.leg_attribute8;
      lv_attribute9         := cur_rcpt_headers_rec.leg_attribute9;
      lv_attribute10        := cur_rcpt_headers_rec.leg_attribute10;
      lv_attribute11        := cur_rcpt_headers_rec.leg_attribute11;
      lv_attribute12        := cur_rcpt_headers_rec.leg_attribute12;
      lv_attribute13        := cur_rcpt_headers_rec.leg_attribute13;
      lv_attribute14        := cur_rcpt_headers_rec.leg_attribute14;
      lv_attribute15        := cur_rcpt_headers_rec.leg_attribute15;
      ----transaction cursor starts
      for cur_rcpt_trxn_rec in cur_rcpt_trxn(cur_rcpt_headers_rec.leg_shipment_header_id,
                                             cur_rcpt_headers_rec.leg_source_system) loop
        lv_txn_error_flag       := 'N';
        l_err_code              := null;
        l_err_msg               := null;
        lv_txn_org_id           := null;
        lv_txn_org_name         := null;
        lv_t_header_id          := null;
        lv_t_emp_id             := null;
        lv_po_txn_date          := null;
        lv_t_vendor_id          := null;
        lv_t_vendor_site_id     := null;
        lv_t_item_id            := null;
        lv_po_line_id           := null;
        lv_line_num             := null;
        lv_t_category_id        := null;
        lv_t_item_desc          := null;
        lv_t_line_loc_id        := null;
        lv_po_dist_id           := null;
        lv_t_ship_to_org_id     := null;
        lv_t_delv_per_id        := null;
        lv_delv_to_loc_id       := null;
        lv_t_ship_to_loc_id     := null;
        lv_t_dest_type_code     := null;
        lv_func_currency_code   := null;
        lv_tran_date_flag       := 'N';
        lv_transaction_date     := null;
        lv_t_ship_date          := null;
        lv_t_ship_date_flag     := 'N';
        lv_t_exp_date_flag      := 'N';
        lv_t_exp_rcv_date       := null;
        lv_t_attribute_category := null;
        lv_t_attribute1         := null;
        lv_t_attribute2         := null;
        lv_t_attribute3         := null;
        lv_t_attribute4         := null;
        lv_t_attribute5         := null;
        lv_t_attribute6         := null;
        lv_t_attribute7         := null;
        lv_t_attribute8         := null;
        lv_t_attribute9         := null;
        lv_t_attribute10        := null;
        lv_t_attribute11        := null;
        lv_t_attribute12        := null;
        lv_t_attribute13        := null;
        lv_t_attribute14        := null;
        lv_t_attribute15        := null;
        lv_c_d_m_flag           := null;
        lv_delv_loc_final       := null;
        lv_dev_to_loc_id_sys    := null;
        xxetn_debug_pkg.add_debug('Starting processing for Transaction :  ' ||
                                  cur_rcpt_trxn_rec.interface_txn_id);
        -- Validate quantity
        if cur_rcpt_trxn_rec.leg_quantity is null then
          lv_txn_error_flag := 'Y';
          l_err_code        := 'ETN_RCPT_MANDATORY_NOT_ENTERED';
          l_err_msg         := 'Error: Mandatory column not entered.  ';
          log_errors(pin_interface_txn_id    => cur_rcpt_trxn_rec.interface_txn_id,
                     piv_source_table        => 'xxpo_receipt_trx_stg',
                     piv_source_column_name  => 'leg_quantity',
                     piv_source_column_value => cur_rcpt_trxn_rec.leg_quantity,
                     piv_source_keyname1     => null,
                     piv_source_keyvalue1    => null,
                     piv_error_type          => 'ERR_VAL',
                     piv_error_code          => l_err_code,
                     piv_error_message       => l_err_msg);
        else
          if cur_rcpt_trxn_rec.leg_quantity < 0 then
            lv_txn_error_flag := 'Y';
            l_err_code        := 'ETN_RCPT_INVALID_RECEIPT_QUANTITY';
            l_err_msg         := 'Error: Receipt quantity cannot be negative.  ';
            log_errors(pin_interface_txn_id    => cur_rcpt_trxn_rec.interface_txn_id,
                       piv_source_table        => 'xxpo_receipt_trx_stg',
                       piv_source_column_name  => 'leg_quantity',
                       piv_source_column_value => cur_rcpt_trxn_rec.leg_quantity,
                       piv_source_keyname1     => null,
                       piv_source_keyvalue1    => null,
                       piv_error_type          => 'ERR_VAL',
                       piv_error_code          => l_err_code,
                       piv_error_message       => l_err_msg);
          end if;
        end if;
        -- validate UOM code
        xxetn_debug_pkg.add_debug('CALLING FUNCTION: validate_uom_code for  ' ||
                                  cur_rcpt_trxn_rec.interface_txn_id);
        ----------------UOM Code Changes----------------SM
        if cur_rcpt_trxn_rec.leg_source_system = 'ISSC' then
          begin
            lv_uom_description_issc := null;
            lv_uom_attribute1_issc  := null;
            begin
              select distinct flv.description, flv.attribute1 --- For duplicate UOM Error seen
                into lv_uom_description_issc, lv_uom_attribute1_issc
                from apps.fnd_lookup_values flv, fnd_application fal
               where flv.language = userenv('LANG')
                 and flv.enabled_flag = 'Y'
                 and flv.attribute1 = cur_rcpt_trxn_rec.leg_attribute10 -- SM--MOCK3 --
                    --  and flv.attribute1 = cur_rcpt_trxn_rec.leg_uom_code
                 and flv.lookup_type = 'XXINV_UOM_MAPPING'
                 and trunc(sysdate) between
                     trunc(nvl(flv.start_date_active, sysdate)) and
                     trunc(nvl(flv.end_date_active, sysdate))
                    --and fal.application_short_name = 'FND' ---SM REMOVED IN MOCK3
                 and fal.application_id = flv.view_application_id; ---SM;
            exception
              when no_data_found then
                l_err_code := 'ETN_UOM_CODE_INVALID_ISSC';
                l_err_msg  := 'Error: Invalid UOM Code for ISSC. Data not present in lookup XXINV_UOM_MAPPING';
                log_errors(pin_interface_txn_id    => cur_rcpt_trxn_rec.interface_txn_id,
                           piv_source_table        => 'xxpo_receipt_trx_stg',
                           piv_source_column_name  => 'leg_uom_code',
                           piv_source_column_value => cur_rcpt_trxn_rec.leg_uom_code,
                           piv_source_keyname1     => null,
                           piv_source_keyvalue1    => null,
                           piv_error_type          => 'ERR_VAL',
                           piv_error_code          => l_err_code,
                           piv_error_message       => l_err_msg);
              when others then
                l_err_code := 'ETN_UOM_CODE_INVALID_ISSC';
                l_err_msg  := 'Error: Invalid UOM Code for ISSC  ';
                log_errors(pin_interface_txn_id    => cur_rcpt_trxn_rec.interface_txn_id,
                           piv_source_table        => 'xxpo_receipt_trx_stg',
                           piv_source_column_name  => 'leg_uom_code',
                           piv_source_column_value => cur_rcpt_trxn_rec.leg_uom_code,
                           piv_source_keyname1     => null,
                           piv_source_keyvalue1    => null,
                           piv_error_type          => 'ERR_VAL',
                           piv_error_code          => l_err_code,
                           piv_error_message       => l_err_msg);
            end;
            if lv_uom_description_issc is not null then
              update xxpo_receipt_trx_stg
                 set leg_uom_code =
                     (select uom_code
                        from mtl_units_of_measure
                       where unit_of_measure = lv_uom_description_issc)
               where leg_uom_code = lv_uom_attribute1_issc
                 and leg_source_system = 'ISSC'
                 and interface_txn_id = cur_rcpt_trxn_rec.interface_txn_id;
            else
              l_err_code := 'ETN_UOM_CODE_INVALID_ISSC';
              l_err_msg  := 'Error: Invalid UOM Code for ISSC. Code not present in mtl_units_of_measure table  ';
              log_errors(pin_interface_txn_id    => cur_rcpt_trxn_rec.interface_txn_id,
                         piv_source_table        => 'xxpo_receipt_trx_stg',
                         piv_source_column_name  => 'leg_uom_code',
                         piv_source_column_value => cur_rcpt_trxn_rec.leg_uom_code,
                         piv_source_keyname1     => null,
                         piv_source_keyvalue1    => null,
                         piv_error_type          => 'ERR_VAL',
                         piv_error_code          => l_err_code,
                         piv_error_message       => l_err_msg);
            end if;
          exception
            when others then
              l_err_code := 'ETN_UOM_CODE_INVALID_ISSC';
              l_err_msg  := 'Error: Invalid UOM Code for ISSC.  ';
              log_errors(pin_interface_txn_id    => cur_rcpt_trxn_rec.interface_txn_id,
                         piv_source_table        => 'xxpo_receipt_trx_stg',
                         piv_source_column_name  => 'leg_uom_code',
                         piv_source_column_value => cur_rcpt_trxn_rec.leg_uom_code,
                         piv_source_keyname1     => null,
                         piv_source_keyvalue1    => null,
                         piv_error_type          => 'ERR_VAL',
                         piv_error_code          => l_err_code,
                         piv_error_message       => l_err_msg);
          end;
        elsif cur_rcpt_trxn_rec.leg_source_system = 'NAFSC' then
          begin
            lv_uom_description_nafsc := null;
            lv_uom_attribute2_nafsc  := null;
            begin
              select distinct flv.description, flv.attribute2 --- For duplicate UOM Error seen
                into lv_uom_description_nafsc, lv_uom_attribute2_nafsc
                from apps.fnd_lookup_values flv, fnd_application fal
               where flv.language = userenv('LANG')
                 and flv.enabled_flag = 'Y'
                 and flv.lookup_type = 'XXINV_UOM_MAPPING'
                 and flv.attribute2 is not null
                 and flv.attribute2 = cur_rcpt_trxn_rec.leg_attribute10 -- SM--MOCK3 --
                    --and attribute2 = cur_rcpt_trxn_rec.leg_uom_code
                 and trunc(sysdate) between
                     trunc(nvl(flv.start_date_active, sysdate)) and
                     trunc(nvl(flv.end_date_active, sysdate))
                    --and fal.application_short_name = 'FND' ---SM REMOVED IN MOCK 3
                 and fal.application_id = flv.view_application_id; ---SM;
            exception
              when no_data_found then
                l_err_code := 'ETN_UOM_CODE_INVALID_ISSC';
                l_err_msg  := 'Error: Invalid UOM Code for NAFSC. Data not present in lookup XXINV_UOM_MAPPING';
                log_errors(pin_interface_txn_id    => cur_rcpt_trxn_rec.interface_txn_id,
                           piv_source_table        => 'xxpo_receipt_trx_stg',
                           piv_source_column_name  => 'leg_uom_code',
                           piv_source_column_value => cur_rcpt_trxn_rec.leg_uom_code,
                           piv_source_keyname1     => null,
                           piv_source_keyvalue1    => null,
                           piv_error_type          => 'ERR_VAL',
                           piv_error_code          => l_err_code,
                           piv_error_message       => l_err_msg);
              when others then
                l_err_code := 'ETN_UOM_CODE_INVALID_NAFSC';
                l_err_msg  := 'Error: Invalid UOM Code for NAFSC.  ';
                log_errors(pin_interface_txn_id    => cur_rcpt_trxn_rec.interface_txn_id,
                           piv_source_table        => 'xxpo_receipt_trx_stg',
                           piv_source_column_name  => 'leg_uom_code',
                           piv_source_column_value => cur_rcpt_trxn_rec.leg_uom_code,
                           piv_source_keyname1     => null,
                           piv_source_keyvalue1    => null,
                           piv_error_type          => 'ERR_VAL',
                           piv_error_code          => l_err_code,
                           piv_error_message       => l_err_msg);
            end;
            if lv_uom_description_nafsc is not null then
              update xxpo_receipt_trx_stg
                 set leg_uom_code =
                     (select uom_code
                        from mtl_units_of_measure
                       where unit_of_measure = lv_uom_description_nafsc)
               where leg_uom_code = lv_uom_attribute2_nafsc
                 and leg_source_system = 'NAFSC'
                 and interface_txn_id = cur_rcpt_trxn_rec.interface_txn_id;
            else
              l_err_code := 'ETN_UOM_CODE_INVALID_NAFSC';
              l_err_msg  := 'Error: Invalid UOM Code for NAFSC. Code not present in mtl_units_of_measure table  ';
              log_errors(pin_interface_txn_id    => cur_rcpt_trxn_rec.interface_txn_id,
                         piv_source_table        => 'xxpo_receipt_trx_stg',
                         piv_source_column_name  => 'leg_uom_code',
                         piv_source_column_value => cur_rcpt_trxn_rec.leg_uom_code,
                         piv_source_keyname1     => null,
                         piv_source_keyvalue1    => null,
                         piv_error_type          => 'ERR_VAL',
                         piv_error_code          => l_err_code,
                         piv_error_message       => l_err_msg);
            end if;
          exception
            when others then
              l_err_code := 'ETN_UOM_CODE_INVALID_NAFSC';
              l_err_msg  := 'Error: Invalid UOM Code for NAFSC.  ';
              log_errors(pin_interface_txn_id    => cur_rcpt_trxn_rec.interface_txn_id,
                         piv_source_table        => 'xxpo_receipt_trx_stg',
                         piv_source_column_name  => 'leg_uom_code',
                         piv_source_column_value => cur_rcpt_trxn_rec.leg_uom_code,
                         piv_source_keyname1     => null,
                         piv_source_keyvalue1    => null,
                         piv_error_type          => 'ERR_VAL',
                         piv_error_code          => l_err_code,
                         piv_error_message       => l_err_msg);
          end;
        end if;
        -------------------------UOM Code Changes Ends-------------------
        if cur_rcpt_trxn_rec.leg_uom_code is not null then
          lv_val_flag := 'N';
          --today MOCK3
          begin
            select leg_uom_code
              into l_leg_new_uom -- changed SM
              from xxpo_receipt_trx_stg
             where leg_source_system in ('NAFSC', 'ISSC') --changed SM -- 12th jan2015
               and interface_txn_id = cur_rcpt_trxn_rec.interface_txn_id;
            fnd_file.put_line(fnd_file.log,
                              'l_leg_nfsc_uom ' || l_leg_new_uom);
          exception
            when others then
              l_leg_new_uom := null;
          end;
          --today  MOCK3
          lv_val_flag := validate_uom_code(cur_rcpt_trxn_rec.interface_txn_id,
                                           'xxpo_receipt_trx_stg',
                                           'leg_uom_code',
                                           /*cur_rcpt_trxn_rec.leg_uom_code*/
                                           l_leg_new_uom);
          if lv_val_flag = 'Y' then
            lv_txn_error_flag := 'Y';
          end if;
        else
          lv_txn_error_flag := 'Y';
          l_err_code        := 'ETN_RCPT_MANDATORY_NOT_ENTERED';
          l_err_msg         := 'Error: Mandatory column not entered.  ';
          log_errors(pin_interface_txn_id    => cur_rcpt_trxn_rec.interface_txn_id,
                     piv_source_table        => 'xxpo_receipt_trx_stg',
                     piv_source_column_name  => 'leg_uom_code',
                     piv_source_column_value => cur_rcpt_trxn_rec.leg_uom_code,
                     piv_source_keyname1     => null,
                     piv_source_keyvalue1    => null,
                     piv_error_type          => 'ERR_VAL',
                     piv_error_code          => l_err_code,
                     piv_error_message       => l_err_msg);
        end if;
        /* xxetn_debug_pkg.add_debug
                               (   'CALLING FUNCTION: validate_operating_unit for : '
                                || cur_rcpt_trxn_rec.interface_txn_id
                               );

                   --- derive operating unit
                   IF cur_rcpt_trxn_rec.leg_operating_unit IS NOT NULL
                   THEN
                      validate_operating_unit (cur_rcpt_trxn_rec.interface_txn_id,
                                               'xxpo_receipt_trx_stg',
                                               'leg_operating_unit',
                                               cur_rcpt_trxn_rec.leg_operating_unit,
                                               lv_txn_org_name,
                                               lv_txn_org_id
                                              );

                      IF lv_txn_org_id IS NULL
                      THEN
                         lv_txn_error_flag := 'Y';
                      END IF;
                   ELSE
                      lv_txn_error_flag := 'Y';
                      l_err_code := 'ETN_RCPT_MANDATORY_NOT_ENTERED';
                      l_err_msg := 'Error: Mandatory column not entered.  ';
                      log_errors
                         (pin_interface_txn_id         => cur_rcpt_trxn_rec.interface_txn_id,
                          piv_source_table             => 'xxpo_receipt_trx_stg',
                          piv_source_column_name       => 'leg_operating_unit',
                          piv_source_column_value      => cur_rcpt_trxn_rec.leg_operating_unit,
                          piv_source_keyname1          => NULL,
                          piv_source_keyvalue1         => NULL,
                          piv_error_type               => 'ERR_VAL',
                          piv_error_code               => l_err_code,
                          piv_error_message            => l_err_msg
                         );
                   END IF;
        */
        ---validate source doc code
        if cur_rcpt_trxn_rec.leg_source_document_code <> 'PO' then
          lv_txn_error_flag := 'Y';
          l_err_code        := 'ETN_RCPT_INVALID_SOURCE_DOC_CODE';
          l_err_msg         := 'Error: Invalid source document code.  ';
          log_errors(pin_interface_txn_id    => cur_rcpt_trxn_rec.interface_txn_id,
                     piv_source_table        => 'xxpo_receipt_trx_stg',
                     piv_source_column_name  => 'leg_source_document_code',
                     piv_source_column_value => cur_rcpt_trxn_rec.leg_source_document_code,
                     piv_source_keyname1     => null,
                     piv_source_keyvalue1    => null,
                     piv_error_type          => 'ERR_VAL',
                     piv_error_code          => l_err_code,
                     piv_error_message       => l_err_msg);
        end if;
        --- Derive values from po
        xxetn_debug_pkg.add_debug('CALLING PROC: validate_po_number for  ' ||
                                  cur_rcpt_trxn_rec.interface_txn_id);
        if cur_rcpt_trxn_rec.leg_po_number is not null then
          lv_val_flag := 'N';
          validate_po_number(cur_rcpt_trxn_rec.interface_txn_id,
                             'xxpo_receipt_trx_stg',
                             'leg_po_number',
                             cur_rcpt_trxn_rec.leg_po_number,
                             cur_rcpt_trxn_rec.org_id,
                             lv_t_header_id,
                             lv_t_emp_id,
                             lv_t_vendor_id,
                             lv_t_vendor_site_id,
                             lv_po_txn_date,
                             lv_val_flag);
          if lv_val_flag = 'Y' then
            lv_txn_error_flag := 'Y';
          end if;
        else
          lv_txn_error_flag := 'Y';
          l_err_code        := 'ETN_RCPT_MANDATORY_NOT_ENTERED';
          l_err_msg         := 'Error: Mandatory column not entered.  ';
          log_errors(pin_interface_txn_id    => cur_rcpt_trxn_rec.interface_txn_id,
                     piv_source_table        => 'xxpo_receipt_trx_stg',
                     piv_source_column_name  => 'leg_po_number',
                     piv_source_column_value => cur_rcpt_trxn_rec.leg_po_number,
                     piv_source_keyname1     => null,
                     piv_source_keyvalue1    => null,
                     piv_error_type          => 'ERR_VAL',
                     piv_error_code          => l_err_code,
                     piv_error_message       => l_err_msg);
        end if;
        --- Derive values from po line num
        xxetn_debug_pkg.add_debug('CALLING PROC: validate_po_line_number for  ' ||
                                  cur_rcpt_trxn_rec.interface_txn_id);
        if cur_rcpt_trxn_rec.leg_po_line_num is not null then
          if lv_t_header_id is not null then
            lv_val_flag := 'N';
            validate_po_line_number(cur_rcpt_trxn_rec.interface_txn_id,
                                    cur_rcpt_trxn_rec.leg_po_line_num,
                                    cur_rcpt_trxn_rec.leg_po_shipment_num,
                                    'xxpo_receipt_trx_stg',
                                    'leg_po_line_num',
                                    lv_t_header_id,
                                    cur_rcpt_trxn_rec.org_id,
                                    lv_po_line_id,
                                    lv_line_num,
                                    lv_t_item_id,
                                    lv_t_category_id,
                                    lv_t_item_desc,
                                    lv_val_flag);
            if lv_val_flag = 'Y' then
              lv_txn_error_flag := 'Y';
            end if;
          end if;
        else
          lv_txn_error_flag := 'Y';
          l_err_code        := 'ETN_RCPT_MANDATORY_NOT_ENTERED';
          l_err_msg         := 'Error: Mandatory column not entered.  ';
          log_errors(pin_interface_txn_id    => cur_rcpt_trxn_rec.interface_txn_id,
                     piv_source_table        => 'xxpo_receipt_trx_stg',
                     piv_source_column_name  => 'leg_po_line_num',
                     piv_source_column_value => cur_rcpt_trxn_rec.leg_po_line_num,
                     piv_source_keyname1     => null,
                     piv_source_keyvalue1    => null,
                     piv_error_type          => 'ERR_VAL',
                     piv_error_code          => l_err_code,
                     piv_error_message       => l_err_msg);
        end if;
        --- Derive values from po shipment num
        xxetn_debug_pkg.add_debug('CALLING PROC: validate_po_shipment_number for  ' ||
                                  cur_rcpt_trxn_rec.interface_txn_id);
        if cur_rcpt_trxn_rec.leg_po_shipment_num is not null then
          if lv_t_header_id is not null then
            lv_val_flag := 'N';
            validate_po_shipment_number(cur_rcpt_trxn_rec.interface_txn_id,
                                        cur_rcpt_trxn_rec.leg_po_shipment_num,
                                        'xxpo_receipt_trx_stg',
                                        'leg_po_shipment_num',
                                        lv_t_header_id,
                                        lv_po_line_id,
                                        lv_t_line_loc_id,
                                        lv_val_flag);
            if lv_val_flag = 'Y' then
              lv_txn_error_flag := 'Y';
            end if;
          end if;
        else
          lv_txn_error_flag := 'Y';
          l_err_code        := 'ETN_RCPT_MANDATORY_NOT_ENTERED';
          l_err_msg         := 'Error: Mandatory column not entered.  ';
          log_errors(pin_interface_txn_id    => cur_rcpt_trxn_rec.interface_txn_id,
                     piv_source_table        => 'xxpo_receipt_trx_stg',
                     piv_source_column_name  => 'leg_po_shipment_num',
                     piv_source_column_value => cur_rcpt_trxn_rec.leg_po_shipment_num,
                     piv_source_keyname1     => null,
                     piv_source_keyvalue1    => null,
                     piv_error_type          => 'ERR_VAL',
                     piv_error_code          => l_err_code,
                     piv_error_message       => l_err_msg);
        end if;
        --- Derive values from po shipment num
        xxetn_debug_pkg.add_debug('CALLING PROC: validate_po_dist_number for  ' ||
                                  cur_rcpt_trxn_rec.interface_txn_id);
        if cur_rcpt_trxn_rec.leg_po_distribution_num is not null then
          if lv_t_header_id is not null then
            lv_val_flag := 'N';
            validate_po_dist_number(cur_rcpt_trxn_rec.interface_txn_id,
                                    cur_rcpt_trxn_rec.leg_po_distribution_num,
                                    'xxpo_receipt_trx_stg',
                                    'leg_po_distribution_num',
                                    lv_t_header_id,
                                    lv_po_line_id,
                                    lv_t_line_loc_id,
                                    lv_po_dist_id,
                                    lv_t_ship_to_org_id,
                                    lv_t_delv_per_id,
                                    lv_delv_to_loc_id,
                                    lv_t_ship_to_loc_id,
                                    lv_t_dest_type_code,
                                    lv_val_flag);
            if lv_val_flag = 'Y' then
              lv_txn_error_flag := 'Y';
            end if;
          end if;
        else
          lv_txn_error_flag := 'Y';
          l_err_code        := 'ETN_RCPT_MANDATORY_NOT_ENTERED';
          l_err_msg         := 'Error: Mandatory column not entered.  ';
          log_errors(pin_interface_txn_id    => cur_rcpt_trxn_rec.interface_txn_id,
                     piv_source_table        => 'xxpo_receipt_trx_stg',
                     piv_source_column_name  => 'leg_po_distribution_num',
                     piv_source_column_value => cur_rcpt_trxn_rec.leg_po_distribution_num,
                     piv_source_keyname1     => null,
                     piv_source_keyvalue1    => null,
                     piv_error_type          => 'ERR_VAL',
                     piv_error_code          => l_err_code,
                     piv_error_message       => l_err_msg);
        end if;
        --Validate Currency code
        if cur_rcpt_trxn_rec.leg_currency_code is null then
          lv_txn_error_flag := 'Y';
          l_err_code        := 'ETN_RCPT_MANDATORY_NOT_ENTERED';
          l_err_msg         := 'Error: Mandatory column not entered.  ';
          log_errors(pin_interface_txn_id    => cur_rcpt_trxn_rec.interface_txn_id,
                     piv_source_table        => 'xxpo_receipt_trx_stg',
                     piv_source_column_name  => 'leg_currency_code',
                     piv_source_column_value => cur_rcpt_trxn_rec.leg_currency_code,
                     piv_source_keyname1     => null,
                     piv_source_keyvalue1    => null,
                     piv_error_type          => 'ERR_VAL',
                     piv_error_code          => l_err_code,
                     piv_error_message       => l_err_msg);
        else
          lv_val_flag := 'N';
          lv_val_flag := validate_currency_code(cur_rcpt_trxn_rec.interface_txn_id,
                                                'xxpo_receipt_trx_stg',
                                                'leg_currency_code',
                                                cur_rcpt_trxn_rec.leg_currency_code);
          if lv_val_flag = 'Y' then
            lv_txn_error_flag := 'Y';
          end if;
          --- deriving foreign currency code
          if cur_rcpt_trxn_rec.org_id is not null then
            begin
              select currency_code
                into lv_func_currency_code
                from gl_sets_of_books
               where set_of_books_id =
                     (select set_of_books_id
                        from hr_operating_units
                       where organization_id = cur_rcpt_trxn_rec.org_id);
            exception
              when others then
                lv_func_currency_code := null;
            end;
          end if;
          --- setting rate type for base currency
          if cur_rcpt_trxn_rec.leg_currency_code = lv_func_currency_code then
            update xxpo_receipt_trx_stg
               set leg_currency_conversion_type = null,
                   leg_currency_conversion_rate = null,
                   leg_currency_conversion_date = null
             where interface_txn_id = cur_rcpt_trxn_rec.interface_txn_id;
          --v1.8 unccomenting the else clause
          --v1.7
          else
            update xxpo_receipt_trx_stg
               set leg_currency_conversion_type = 'User'
             where interface_txn_id = cur_rcpt_trxn_rec.interface_txn_id;
          --v1.7 ends
          --v1.8
          end if;
        end if;
        --- checking rate type and rate
        if cur_rcpt_trxn_rec.leg_currency_code <> lv_func_currency_code then
          if cur_rcpt_trxn_rec.leg_currency_conversion_rate is null
            --v1.8 starts commenting the change
            --v1.7
          --and  NVL(cur_rcpt_trxn_rec.LEG_CURRENCY_CONVERSION_TYPE,'X') ='User'
            --v1.7 ends
            --v1.8 ends here
             then
            lv_txn_error_flag := 'Y';
            l_err_code        := 'ETN_RCPT_INVALID_EXCHANGE_RATE';
            l_err_msg         := 'Error: Exchange rate cannot be NULL for USER rate type  ';
            log_errors(pin_interface_txn_id    => cur_rcpt_trxn_rec.interface_txn_id,
                       piv_source_table        => 'xxpo_receipt_trx_stg',
                       piv_source_column_name  => 'leg_currency_conversion_rate',
                       piv_source_column_value => cur_rcpt_trxn_rec.leg_currency_conversion_rate,
                       piv_source_keyname1     => null,
                       piv_source_keyvalue1    => null,
                       piv_error_type          => 'ERR_VAL',
                       piv_error_code          => l_err_code,
                       piv_error_message       => l_err_msg);
          end if;
        end if;
        --- check destination_type_code
        /* IF cur_rcpt_trxn_rec.leg_destination_type_code <> 'RECEIVING'
        THEN
           lv_txn_error_flag := 'Y';
           l_err_code := 'ETN_RCPT_INVALID_DEST_TYPE';
           l_err_msg := 'Error: Destination type code is Invalid  ';
           log_errors
              (pin_interface_txn_id         => cur_rcpt_trxn_rec.interface_txn_id,
               piv_source_table             => 'xxpo_receipt_trx_stg',
               piv_source_column_name       => 'leg_destination_type_code',
               piv_source_column_value      => cur_rcpt_trxn_rec.leg_destination_type_code,
               piv_source_keyname1          => NULL,
               piv_source_keyvalue1         => NULL,
               piv_error_type               => 'ERR_VAL',
               piv_error_code               => l_err_code,
               piv_error_message            => l_err_msg
              );
        END IF; */
        --- Validate sub inventory
        if lv_t_dest_type_code = 'INVENTORY' then
          if cur_rcpt_trxn_rec.leg_subinventory is not null then
            lv_val_flag := 'N';
            lv_val_flag := validate_subinv(cur_rcpt_trxn_rec.interface_txn_id,
                                           'xxpo_receipt_trx_stg',
                                           'leg_subinventory',
                                           cur_rcpt_trxn_rec.leg_subinventory,
                                           lv_t_ship_to_org_id);
            if lv_val_flag = 'Y' then
              lv_txn_error_flag := 'Y';
            end if;
          else
            lv_txn_error_flag := 'Y';
            l_err_code        := 'ETN_RCPT_MANDATORY_NOT_ENTERED';
            l_err_msg         := 'Error: Mandatory column not entered.  ';
            log_errors(pin_interface_txn_id    => cur_rcpt_trxn_rec.interface_txn_id,
                       piv_source_table        => 'xxpo_receipt_trx_stg',
                       piv_source_column_name  => 'leg_subinventory',
                       piv_source_column_value => cur_rcpt_trxn_rec.leg_subinventory,
                       piv_source_keyname1     => null,
                       piv_source_keyvalue1    => null,
                       piv_error_type          => 'ERR_VAL',
                       piv_error_code          => l_err_code,
                       piv_error_message       => l_err_msg);
          end if;
        end if;
        --- Validate shipped ,expected receipt date and transaction_date
        xxetn_debug_pkg.add_debug('CALLING PROC: validate_gl_period for  ' ||
                                  cur_rcpt_trxn_rec.interface_txn_id);
        if cur_rcpt_trxn_rec.org_id is not null and
           lv_po_txn_date is not null then
          if nvl(cur_rcpt_trxn_rec.leg_shipped_date, sysdate) >
             lv_po_txn_date then
            lv_t_ship_date := trunc(nvl(cur_rcpt_trxn_rec.leg_shipped_date,
                                        sysdate));
          else
            lv_t_ship_date := lv_po_txn_date;
          end if;
          -- 1.1
          /*                IF NVL (cur_rcpt_trxn_rec.leg_transaction_date, SYSDATE) >
                                                           lv_po_txn_date
          THEN
             lv_transaction_date :=
                TRUNC (NVL (cur_rcpt_trxn_rec.leg_transaction_date,
                            SYSDATE
                           )
                      );
          ELSE
             lv_transaction_date := lv_po_txn_date;
          END IF; */
          lv_transaction_date := g_transaction_date; --1.1
          if nvl(cur_rcpt_trxn_rec.leg_expected_receipt_date, sysdate) >
             lv_po_txn_date then
            lv_t_exp_rcv_date := trunc(nvl(cur_rcpt_trxn_rec.leg_expected_receipt_date,
                                           sysdate));
          else
            lv_t_exp_rcv_date := lv_po_txn_date;
          end if;
          -- Added today 8th jan 2016 related defect alm 4797--SDP
          lv_t_ship_date    := g_transaction_date;
          lv_t_exp_rcv_date := g_transaction_date;
          -- Added today 8th jan 2016 related defect alm 4797--SDP
          validate_gl_period(cur_rcpt_trxn_rec.interface_txn_id,
                             cur_rcpt_trxn_rec.leg_operating_unit,
                             'xxpo_receipt_trx_stg',
                             'leg_shipped_date',
                             lv_t_ship_date,
                             cur_rcpt_trxn_rec.org_id,
                             lv_t_ship_date_flag);
          validate_gl_period(cur_rcpt_trxn_rec.interface_txn_id,
                             cur_rcpt_trxn_rec.leg_operating_unit,
                             'xxpo_receipt_trx_stg',
                             'leg_expected_receipt_date',
                             lv_t_exp_rcv_date,
                             cur_rcpt_trxn_rec.org_id,
                             lv_t_exp_date_flag);
          validate_gl_period(cur_rcpt_trxn_rec.interface_txn_id,
                             cur_rcpt_trxn_rec.leg_operating_unit,
                             'xxpo_receipt_trx_stg',
                             'leg_transaction_date',
                             lv_transaction_date,
                             cur_rcpt_trxn_rec.org_id,
                             lv_tran_date_flag);
          if lv_t_exp_rcv_date <> lv_t_ship_date ---1.1
           then
            lv_t_ship_date := lv_t_exp_rcv_date;
          end if;
          if lv_exp_date_flag = 'Y' or lv_ship_date_flag = 'Y' or
             lv_tran_date_flag = 'Y' then
            lv_txn_error_flag := 'Y';
          end if;
        end if;
        --Derive deliver to location
        if cur_rcpt_trxn_rec.leg_deliver_to_location is not null then
          begin
            select location_id
              into lv_dev_to_loc_id_sys
              from hr_locations
             where (ship_to_site_flag = 'Y' or
                   ship_to_location_id is not null)
               and receiving_site_flag = 'Y'
               and location_code =
                   cur_rcpt_trxn_rec.leg_deliver_to_location
               and sysdate <= nvl(inactive_date, sysdate);
            lv_delv_loc_final := lv_dev_to_loc_id_sys;
          exception
            when others then
              lv_txn_error_flag := 'Y';
              l_err_code        := 'ETN_PO_INVALID_DELIVER_TO_LOCN';
              l_err_msg         := 'Error: Invalid Deliver to Location code. ';
              log_errors(pin_interface_txn_id    => cur_rcpt_trxn_rec.interface_txn_id,
                         piv_source_table        => 'xxpo_receipt_trx_stg',
                         piv_source_column_name  => 'leg_deliver_to_location',
                         piv_source_column_value => cur_rcpt_trxn_rec.leg_deliver_to_location,
                         piv_source_keyname1     => null,
                         piv_source_keyvalue1    => null,
                         piv_error_type          => 'ERR_VAL',
                         piv_error_code          => l_err_code,
                         piv_error_message       => l_err_msg);
          end;
        else
          if lv_delv_to_loc_id is not null then
            lv_delv_loc_final := lv_delv_to_loc_id;
          else
            lv_delv_loc_final := lv_t_ship_to_loc_id;
          end if;
        end if;
        ---assigning DFF
        lv_t_attribute_category := cur_rcpt_trxn_rec.leg_attribute_category;
        lv_t_attribute1         := cur_rcpt_trxn_rec.leg_attribute1;
        lv_t_attribute2         := cur_rcpt_trxn_rec.leg_attribute2;
        lv_t_attribute3         := cur_rcpt_trxn_rec.leg_attribute3;
        lv_t_attribute4         := cur_rcpt_trxn_rec.leg_attribute4;
        lv_t_attribute5         := cur_rcpt_trxn_rec.leg_attribute5;
        lv_t_attribute6         := cur_rcpt_trxn_rec.leg_attribute6;
        lv_t_attribute7         := cur_rcpt_trxn_rec.leg_attribute7;
        lv_t_attribute8         := cur_rcpt_trxn_rec.leg_attribute8;
        lv_t_attribute9         := cur_rcpt_trxn_rec.leg_attribute9;
        lv_t_attribute10        := cur_rcpt_trxn_rec.leg_attribute10;
        lv_t_attribute11        := cur_rcpt_trxn_rec.leg_attribute11;
        lv_t_attribute12        := cur_rcpt_trxn_rec.leg_attribute12;
        lv_t_attribute13        := cur_rcpt_trxn_rec.leg_attribute13;
        lv_t_attribute14        := cur_rcpt_trxn_rec.leg_attribute14;
        lv_t_attribute15        := cur_rcpt_trxn_rec.leg_attribute15;
        ---Obtain create debit memo flag
        if lv_t_vendor_id is not null then
          begin
            select create_debit_memo_flag
              into lv_c_d_m_flag
              from po_vendors
             where vendor_id = lv_t_vendor_id;
          exception
            when others then
              lv_c_d_m_flag := null;
          end;
        end if;
        if lv_txn_error_flag = 'Y' or cur_rcpt_trxn_rec.process_flag = 'E' then
          lv_mast_txv_error_flag := 'Y';
        end if;
        xxetn_debug_pkg.add_debug('BEFORE UPDATING xxpo_receipt_trx_stg' ||
                                  sysdate);
        if lv_txn_error_flag = 'Y' or cur_rcpt_trxn_rec.process_flag = 'E' then
          g_retcode := 1;
          begin
            update xxpo_receipt_trx_stg
               set process_flag        = 'E',
                   error_type          = 'ERR_VAL',
                   transaction_date    = lv_transaction_date,
                   po_header_id        = lv_t_header_id,
                   po_line_id          = lv_po_line_id,
                   po_line_location_id = lv_t_line_loc_id,
                   po_distribution_id  = lv_po_dist_id,
                   --operating_unit = lv_txn_org_name,
                   --org_id = lv_txn_org_id,
                   shipped_date           = lv_t_ship_date,
                   expected_receipt_date  = lv_t_exp_rcv_date,
                   inventory_item_id      = lv_t_item_id,
                   category_id            = lv_t_category_id,
                   item_description       = lv_t_item_desc,
                   create_debit_memo_flag = lv_c_d_m_flag,
                   employee_id            = lv_t_emp_id,
                   vendor_id              = lv_t_vendor_id,
                   vendor_site_id         = lv_t_vendor_site_id,
                   ship_to_locations_id   = lv_t_ship_to_loc_id,
                   deliver_to_person_id   = lv_t_delv_per_id,
                   deliver_to_location_id = lv_delv_loc_final,
                   to_organization_id     = lv_t_ship_to_org_id,
                   attribute_category     = lv_t_attribute_category,
                   attribute1             = lv_t_attribute1,
                   attribute2             = lv_t_attribute2,
                   attribute3             = lv_t_attribute3,
                   attribute4             = lv_t_attribute4,
                   attribute5             = lv_t_attribute5,
                   attribute6             = lv_t_attribute6,
                   attribute7             = lv_t_attribute7,
                   attribute8             = lv_t_attribute8, --- DEFECT 2871 changed lv_t_attribute7 to lv_t_attribute8
                   attribute9             = lv_t_attribute9,
                   attribute10            = lv_t_attribute10,
                   attribute11            = lv_t_attribute11,
                   attribute12            = lv_t_attribute12,
                   attribute13            = lv_t_attribute13,
                   attribute14            = lv_t_attribute14,
                   attribute15            = lv_t_attribute15,
                   request_id             = g_request_id,
                   last_updated_date      = sysdate,
                   last_updated_by        = g_last_updated_by,
                   last_update_login      = g_last_update_login
             where interface_txn_id = cur_rcpt_trxn_rec.interface_txn_id;
          exception
            when others then
              print_log_message('Error while updating txn table for process flag E : ' ||
                                sqlerrm);
              g_retcode := 1;
          end;
        else
          begin
            update xxpo_receipt_trx_stg
               set process_flag     = 'V',
                   transaction_date = lv_transaction_date,
                   po_header_id     = lv_t_header_id,
                   --leg_po_line_num      = lv_line_num,
                   po_line_id          = lv_po_line_id,
                   po_line_location_id = lv_t_line_loc_id,
                   po_distribution_id  = lv_po_dist_id,
                   --operating_unit = lv_txn_org_name,
                   --org_id = lv_txn_org_id,
                   shipped_date           = lv_t_ship_date,
                   expected_receipt_date  = lv_t_exp_rcv_date,
                   inventory_item_id      = lv_t_item_id,
                   category_id            = lv_t_category_id,
                   item_description       = lv_t_item_desc,
                   create_debit_memo_flag = lv_c_d_m_flag,
                   employee_id            = lv_t_emp_id,
                   vendor_id              = lv_t_vendor_id,
                   vendor_site_id         = lv_t_vendor_site_id,
                   ship_to_locations_id   = lv_t_ship_to_loc_id,
                   deliver_to_person_id   = lv_t_delv_per_id,
                   deliver_to_location_id = lv_delv_loc_final,
                   to_organization_id     = lv_t_ship_to_org_id,
                   attribute_category     = lv_t_attribute_category,
                   attribute1             = lv_t_attribute1,
                   attribute2             = lv_t_attribute2,
                   attribute3             = lv_t_attribute3,
                   attribute4             = lv_t_attribute4,
                   attribute5             = lv_t_attribute5,
                   attribute6             = lv_t_attribute6,
                   attribute7             = lv_t_attribute7,
                   attribute8             = lv_t_attribute8, --- DEFECT 2871 changed lv_t_attribute7 to lv_t_attribute8
                   attribute9             = lv_t_attribute9,
                   attribute10            = lv_t_attribute10,
                   attribute11            = lv_t_attribute11,
                   attribute12            = lv_t_attribute12,
                   attribute13            = lv_t_attribute13,
                   attribute14            = lv_t_attribute14,
                   attribute15            = lv_t_attribute15,
                   request_id             = g_request_id,
                   last_updated_date      = sysdate,
                   last_updated_by        = g_last_updated_by,
                   last_update_login      = g_last_update_login
             where interface_txn_id = cur_rcpt_trxn_rec.interface_txn_id;
          exception
            when others then
              print_log_message('Error while updating txn table for process flag V : ' ||
                                sqlerrm);
              g_retcode := 1;
          end;
        end if;
        xxetn_debug_pkg.add_debug('AFTER UPDATING xxpo_receipt_trx_stg' ||
                                  sysdate);
      end loop;
      xxetn_debug_pkg.add_debug('BEFORE UPDATING xxpo_receipt_hdr_stg' ||
                                sysdate);
      if lv_error_flag = 'Y' then
        g_retcode := 1;
        ------------------------------------------------------------(Changes Required (SM))
        begin
          update xxpo_receipt_hdr_stg
             set process_flag = 'E',
                 error_type   = 'ERR_VAL',
                 --operating_unit = lv_org_name,
                 --org_id = lv_org_id,
                 --shipped_date = lv_ship_date,
                 --expected_receipt_date = lv_exp_rcv_date,
                 --freight_terms = lv_freight_terms,
                 (vendor_id, vendor_site_id, employee_id) =
                 (select vendor_id, vendor_site_id, employee_id -- modified for v1.3
                    from xxpo_receipt_trx_stg
                   where leg_shipment_header_id =
                         cur_rcpt_headers_rec.leg_shipment_header_id
                     and batch_id = cur_rcpt_headers_rec.batch_id -- (SM)
                     and leg_operating_unit =
                         cur_rcpt_headers_rec.leg_operating_unit -- (SM)
                     and leg_source_system =
                         cur_rcpt_headers_rec.leg_source_system -- added for v1.3
                     and rownum = 1), -- added for v1.3
                 -- vendor_site_id =
                 -- (SELECT vendor_site_id
                 --    FROM xxpo_receipt_trx_stg
                 --   WHERE leg_shipment_header_id =
                 --         cur_rcpt_headers_rec.leg_shipment_header_id
                 --     AND batch_id = cur_rcpt_headers_rec.batch_id  -- (SM)
                 --AND leg_operating_unit = cur_rcpt_headers_rec.leg_operating_unit), -- (SM)
                 --    AND ROWNUM = 1),
                 --    employee_id   =
                 --  (SELECT employee_id
                 --       FROM xxpo_receipt_trx_stg
                 --      WHERE leg_shipment_header_id =
                 --            cur_rcpt_headers_rec.leg_shipment_header_id
                 --       AND batch_id = cur_rcpt_headers_rec.batch_id  -- (SM)
                 --AND leg_operating_unit = cur_rcpt_headers_rec.leg_operating_unit), -- (SM)
                 --  AND ROWNUM = 1),
                 --ship_to_organization_id = lv_ship_to_org_id,
                 --ship_to_location_id = lv_ship_to_loc_id,
                 attribute_category = lv_attribute_category,
                 attribute1         = lv_attribute1,
                 attribute2         = lv_attribute2,
                 attribute3         = lv_attribute3,
                 attribute4         = lv_attribute4,
                 attribute5         = lv_attribute5,
                 attribute6         = lv_attribute6,
                 attribute7         = lv_attribute7,
                 attribute8         = lv_attribute8, --- DEFECT 2871 changed lv_t_attribute7 to lv_t_attribute8
                 attribute9         = lv_attribute9,
                 attribute10        = lv_attribute10,
                 attribute11        = lv_attribute11,
                 attribute12        = lv_attribute12,
                 attribute13        = lv_attribute13,
                 attribute14        = lv_attribute14,
                 attribute15        = lv_attribute15,
                 request_id         = g_request_id,
                 last_updated_date  = sysdate,
                 last_updated_by    = g_last_updated_by,
                 last_update_login  = g_last_update_login
           where interface_txn_id = cur_rcpt_headers_rec.interface_txn_id;
        exception
          when others then
            print_log_message('Error while updating Header table for process flag E : ' ||
                              sqlerrm);
            g_retcode := 1;
        end;
      else
        begin
          update xxpo_receipt_hdr_stg
             set process_flag = 'V',
                 --operating_unit = lv_org_name,
                 --org_id = lv_org_id,
                 --shipped_date = lv_ship_date,
                 --expected_receipt_date = lv_exp_rcv_date,
                 --freight_terms = lv_freight_terms,
                 (vendor_id, vendor_site_id, employee_id) =
                 (select vendor_id, vendor_site_id, employee_id -- modified for v1.3
                    from xxpo_receipt_trx_stg
                   where leg_shipment_header_id =
                         cur_rcpt_headers_rec.leg_shipment_header_id
                     and batch_id = cur_rcpt_headers_rec.batch_id -- (SM)
                     and leg_operating_unit =
                         cur_rcpt_headers_rec.leg_operating_unit -- (SM)
                     and leg_source_system =
                         cur_rcpt_headers_rec.leg_source_system -- added for v1.3
                     and rownum = 1), -- added for v1.3
                 --          vendor_site_id =
                 --          (SELECT vendor_site_id
                 --             FROM xxpo_receipt_trx_stg
                 --            WHERE leg_shipment_header_id =
                 --                  cur_rcpt_headers_rec.leg_shipment_header_id
                 --             AND batch_id = cur_rcpt_headers_rec.batch_id  -- (SM)
                 --   AND leg_operating_unit = cur_rcpt_headers_rec.leg_operating_unit), -- (SM)
                 --             -- AND ROWNUM = 1),
                 --          employee_id   =
                 --          (SELECT employee_id
                 --             FROM xxpo_receipt_trx_stg
                 --            WHERE leg_shipment_header_id =
                 --                  cur_rcpt_headers_rec.leg_shipment_header_id
                 --             AND batch_id = cur_rcpt_headers_rec.batch_id  -- (SM)
                 --   AND leg_operating_unit = cur_rcpt_headers_rec.leg_operating_unit), -- (SM)
                 --  AND ROWNUM = 1),
                 --ship_to_organization_id = lv_ship_to_org_id,
                 --ship_to_location_id = lv_ship_to_loc_id,
                 attribute_category = lv_attribute_category,
                 attribute1         = lv_attribute1,
                 attribute2         = lv_attribute2,
                 attribute3         = lv_attribute3,
                 attribute4         = lv_attribute4,
                 attribute5         = lv_attribute5,
                 attribute6         = lv_attribute6,
                 attribute7         = lv_attribute7,
                 attribute8         = lv_attribute8, --- DEFECT 2871 changed lv_t_attribute7 to lv_t_attribute8
                 attribute9         = lv_attribute9,
                 attribute10        = lv_attribute10,
                 attribute11        = lv_attribute11,
                 attribute12        = lv_attribute12,
                 attribute13        = lv_attribute13,
                 attribute14        = lv_attribute14,
                 attribute15        = lv_attribute15,
                 request_id         = g_request_id,
                 last_updated_date  = sysdate,
                 last_updated_by    = g_last_updated_by,
                 last_update_login  = g_last_update_login
           where interface_txn_id = cur_rcpt_headers_rec.interface_txn_id;
        exception
          when others then
            print_log_message('Error while updating Header table for process flag V : ' ||
                              sqlerrm);
            g_retcode := 1;
        end;
        xxetn_debug_pkg.add_debug('AFTER UPDATING xxpo_receipt_hdr_stg' ||
                                  sysdate);
      end if;
      if lv_error_flag = 'Y' or lv_mast_txv_error_flag = 'Y' then
        for cur_head_err_rec in cur_head_err(cur_rcpt_headers_rec.interface_txn_id) loop
          update xxpo_receipt_hdr_stg
             set process_flag      = 'E',
                 error_type        = 'ERR_VAL',
                 request_id        = g_request_id,
                 last_updated_date = sysdate,
                 last_updated_by   = g_last_updated_by,
                 last_update_login = g_last_update_login
           where interface_txn_id = cur_head_err_rec.interface_txn_id;
          l_err_code := 'ETN_RCPT_DEPENDENT_ERROR';
          l_err_msg  := 'Error: Record erred out due to corresponding header/transaction erring out.  ';
          log_errors(pin_interface_txn_id    => cur_head_err_rec.interface_txn_id,
                     piv_source_table        => 'xxpo_receipt_hdr_stg',
                     piv_source_column_name  => 'leg_receipt_num',
                     piv_source_column_value => cur_head_err_rec.leg_receipt_num,
                     piv_source_keyname1     => null,
                     piv_source_keyvalue1    => null,
                     piv_error_type          => 'ERR_VAL',
                     piv_error_code          => l_err_code,
                     piv_error_message       => l_err_msg);
        end loop;
        for cur_txn_err_rec in cur_txn_err(cur_rcpt_headers_rec.leg_shipment_header_id) loop
          update xxpo_receipt_trx_stg
             set process_flag      = 'E',
                 error_type        = 'ERR_VAL',
                 request_id        = g_request_id,
                 last_updated_date = sysdate,
                 last_updated_by   = g_last_updated_by,
                 last_update_login = g_last_update_login
           where interface_txn_id = cur_txn_err_rec.interface_txn_id;
          l_err_code := 'ETN_RCPT_DEPENDENT_ERROR';
          l_err_msg  := 'Error: Record erred out due to corresponding header/transaction erring out.  ';
          log_errors(pin_interface_txn_id    => cur_txn_err_rec.interface_txn_id,
                     piv_source_table        => 'xxpo_receipt_trx_stg',
                     piv_source_column_name  => 'leg_receipt_num',
                     piv_source_column_value => cur_rcpt_headers_rec.leg_receipt_num,
                     piv_source_keyname1     => null,
                     piv_source_keyvalue1    => null,
                     piv_error_type          => 'ERR_VAL',
                     piv_error_code          => l_err_code,
                     piv_error_message       => l_err_msg);
        end loop;
      end if;
      if l_count >= 500 then
        l_count := 0;
        xxetn_debug_pkg.add_debug('
                        Performing Batch Commit');
        commit;
      else
        l_count := l_count + 1;
      end if;
    end loop;
    for cur_trx_ext_rec in cur_trx_ext loop
      l_err_code := 'ETN_RCPT_INVALID_TRX';
      l_err_msg  := 'Error: Transaction record doesnt have a corresponding Receipt record in the Header Table. ';
      log_errors(pin_interface_txn_id    => cur_trx_ext_rec.interface_txn_id,
                 piv_source_table        => 'xxpo_receipt_trx_stg',
                 piv_source_column_name  => null,
                 piv_source_column_value => null,
                 piv_source_keyname1     => null,
                 piv_source_keyvalue1    => null,
                 piv_error_type          => 'ERR_VAL',
                 piv_error_code          => l_err_code,
                 piv_error_message       => l_err_msg);
    end loop;
    update /*+ INDEX (a XXPO_RECEIPT_TRX_STG_N3) */ xxpo_receipt_trx_stg a
       set process_flag      = 'E',
           error_type        = 'ERR_VAL',
           last_updated_date = sysdate,
           last_updated_by   = g_last_updated_by,
           last_update_login = g_login_id
     where process_flag = 'N'
       and batch_id = g_batch_id
       and run_sequence_id = g_new_run_seq_id;
    commit;
    xxetn_debug_pkg.add_debug('VALIDATE PROCEDURE ENDS');
  exception
    when others then
      print_log_message('EXCEPTION error during procedure Validate : ' ||
                        sqlerrm);
      g_retcode := 2;
      print_log_message('Error : Backtace : ' ||
                        dbms_utility.format_error_backtrace);
  end validate_rcpt;

  /*
    +================================================================================+
    |PROCEDURE NAME : conversion                                                     |
    |DESCRIPTION   : This procedure will upload data into the interface table        |
    +================================================================================+
  */
  procedure conversion is
    ------------------------changes SM--------------------------
    cursor cur_operating_unit_hdr is
      select distinct operating_unit
        from xxpo_receipt_hdr_stg
       where process_flag = 'V'
         and batch_id = g_batch_id
         and operating_unit is not null;
    ------------------------changes ends-------------------------
    cursor cur_headers is
      select a.rowid, a.*
        from xxpo_receipt_hdr_stg a
       where process_flag = 'V'
         and batch_id = g_batch_id;
    cursor cur_txn(p_in_txn_id number) is
      select a.rowid, a.*
        from xxpo_receipt_trx_stg a
       where leg_shipment_header_id = p_in_txn_id
         and process_flag = 'V'
         and batch_id = g_batch_id;
    cursor cur_headers_multiple(p_operating_unit in varchar2) is
      select a.rowid, a.*
        from xxpo_receipt_hdr_stg a
       where process_flag = 'V'
         and batch_id = g_batch_id
         and operating_unit = p_operating_unit;
    l_err_code        varchar2(40);
    l_err_msg         varchar2(2000);
    l_operating_count number; --- SM
    lv_h_err_flag     varchar(1) := 'N';
    lv_l_err_flag     varchar(1) := 'N';
    v_group_id        number := null;
    v_head_id         number;
    v_txn_id          number;
    ------------------------changes SM--------------------------
  begin
    select count(distinct operating_unit)
      into l_operating_count
      from xxpo_receipt_hdr_stg
     where batch_id = g_batch_id
       and process_flag = 'V';
    if l_operating_count = 1 then
      begin
        g_retcode := 0;
        select rcv_interface_groups_s.nextval into v_group_id from dual;
        for cur_headers_rec in cur_headers loop
          v_head_id := null;
          select rcv_headers_interface_s.nextval into v_head_id from dual;
          xxetn_debug_pkg.add_debug('INSIDE UPLOAD PROCEDURE LOOP OF PO RECEIPT HEADER ' ||
                                    sysdate);
          lv_h_err_flag := 'N';
          begin
            insert into rcv_headers_interface
              (last_update_login,
               last_updated_by,
               last_update_date,
               created_by,
               creation_date,
               attribute15,
               attribute14,
               attribute13,
               attribute12,
               attribute11,
               attribute10,
               attribute9,
               attribute8,
               attribute7,
               attribute6,
               attribute5,
               attribute4,
               attribute3,
               attribute2,
               attribute1,
               attribute_category,
               header_interface_id,
               group_id,
               processing_status_code,
               receipt_source_code,
               transaction_type,
               auto_transact_code,
               receipt_num,
               bill_of_lading,
               packing_slip,
               shipped_date,
               expected_receipt_date,
               org_id,
               num_of_containers,
               waybill_airbill_num,
               comments,
               packaging_code,
               freight_terms,
               freight_bill_number,
               currency_code,
               conversion_rate_type,
               conversion_rate,
               conversion_rate_date,
               vendor_id,
               vendor_site_id,
               employee_id,
               ship_to_organization_id,
               location_id)
            values
              (g_last_update_login,
               g_last_updated_by,
               sysdate,
               g_created_by,
               sysdate,
               cur_headers_rec.attribute15,
               cur_headers_rec.attribute14,
               cur_headers_rec.attribute13,
               cur_headers_rec.attribute12,
               cur_headers_rec.attribute11,
               cur_headers_rec.attribute10,
               cur_headers_rec.attribute9,
               cur_headers_rec.attribute8,
               cur_headers_rec.attribute7,
               cur_headers_rec.attribute6,
               cur_headers_rec.attribute5,
               cur_headers_rec.attribute4,
               cur_headers_rec.attribute3,
               cur_headers_rec.attribute2,
               cur_headers_rec.attribute1,
               cur_headers_rec.attribute_category,
               v_head_id,
               v_group_id,
               'PENDING',
               cur_headers_rec.leg_receipt_source_code,
               'NEW',
               'DELIVER',
               cur_headers_rec.leg_receipt_num,
               cur_headers_rec.leg_bill_of_lading,
               cur_headers_rec.leg_packing_slip,
               cur_headers_rec.shipped_date,
               cur_headers_rec.expected_receipt_date,
               cur_headers_rec.org_id,
               cur_headers_rec.leg_num_of_containers,
               cur_headers_rec.leg_waybill_airbill_num,
               cur_headers_rec.leg_comments,
               cur_headers_rec.leg_packaging_code,
               cur_headers_rec.freight_terms,
               cur_headers_rec.leg_freight_bill_number,
               cur_headers_rec.leg_currency_code,
               cur_headers_rec.leg_conversion_rate_type,
               cur_headers_rec.leg_conversion_rate,
               cur_headers_rec.leg_conversion_rate_date,
               cur_headers_rec.vendor_id,
               cur_headers_rec.vendor_site_id,
               cur_headers_rec.employee_id,
               cur_headers_rec.ship_to_organization_id,
               cur_headers_rec.ship_to_location_id);
          exception
            when others then
              lv_h_err_flag := 'Y';
              l_err_code    := 'ETN_RCPT_HEADER_INSERT_ERR';
              l_err_msg     := 'Error: Exceptional error while inserting header record :  ' ||
                               sqlerrm;
              log_errors(pin_interface_txn_id    => cur_headers_rec.interface_txn_id,
                         piv_source_table        => 'xxpo_receipt_hdr_stg',
                         piv_source_column_name  => null,
                         piv_source_column_value => null,
                         piv_source_keyname1     => null,
                         piv_source_keyvalue1    => null,
                         piv_error_type          => 'ERR_INT',
                         piv_error_code          => l_err_code,
                         piv_error_message       => l_err_msg);
          end;
          if lv_h_err_flag = 'N' then
            update xxpo_receipt_hdr_stg
               set process_flag        = 'P',
                   error_type          = null,
                   interface_header_id = v_head_id,
                   request_id          = g_request_id,
                   last_updated_date   = sysdate,
                   last_updated_by     = g_last_updated_by,
                   last_update_login   = g_last_update_login
             where interface_txn_id = cur_headers_rec.interface_txn_id;
            xxetn_debug_pkg.add_debug('Record id ' ||
                                      cur_headers_rec.interface_txn_id ||
                                      'is successfully processed');
          else
            g_retcode := 1;
            update xxpo_receipt_hdr_stg
               set process_flag      = 'E',
                   error_type        = 'ERR_INT',
                   request_id        = g_request_id,
                   last_updated_date = sysdate,
                   last_updated_by   = g_last_updated_by,
                   last_update_login = g_last_update_login
             where interface_txn_id = cur_headers_rec.interface_txn_id;
            xxetn_debug_pkg.add_debug('Record id ' ||
                                      cur_headers_rec.interface_txn_id ||
                                      'is erred out');
          end if;
          for cur_txn_rec in cur_txn(cur_headers_rec.leg_shipment_header_id) loop
            xxetn_debug_pkg.add_debug('INSIDE UPLOAD PROCEDURE LOOP OF PO TXN ' ||
                                      sysdate);
            lv_l_err_flag := 'N';
            v_txn_id      := null;
            select rcv_transactions_interface_s.nextval
              into v_txn_id
              from dual;
            begin
              insert into rcv_transactions_interface
                (program_update_date,
                 program_id,
                 program_application_id,
                 request_id,
                 last_update_login,
                 last_updated_by,
                 last_update_date,
                 created_by,
                 creation_date,
                 attribute15,
                 attribute14,
                 attribute13,
                 attribute12,
                 attribute11,
                 attribute10,
                 attribute9,
                 attribute8,
                 attribute7,
                 attribute6,
                 attribute5,
                 attribute4,
                 attribute3,
                 attribute2,
                 attribute1,
                 attribute_category,
                 --- DEFECT 2838 MISSING LINE ATTRIBUTE COLUMNS MAPPING
                 ship_line_attribute15,
                 ship_line_attribute14,
                 ship_line_attribute13,
                 ship_line_attribute12,
                 ship_line_attribute11,
                 ship_line_attribute10,
                 ship_line_attribute9,
                 ship_line_attribute8,
                 ship_line_attribute7,
                 ship_line_attribute6,
                 ship_line_attribute5,
                 ship_line_attribute4,
                 ship_line_attribute3,
                 ship_line_attribute2,
                 ship_line_attribute1,
                 ship_line_attribute_category,
                 --- DEFECT 2838 MISSING LINE ATTRIBUTE COLUMNS MAPPING
                 header_interface_id,
                 group_id,
                 interface_transaction_id,
                 transaction_type,
                 processing_status_code,
                 processing_mode_code,
                 transaction_status_code,
                 auto_transact_code,
                 receipt_source_code,
                 destination_type_code,
                 validation_flag,
                 transaction_date,
                 quantity,
                 uom_code,
                 source_document_code,
                 po_header_id,
                 po_line_id,
                 po_line_location_id,
                 po_distribution_id,
                 currency_code,
                 currency_conversion_type,
                 currency_conversion_rate,
                 currency_conversion_date,
                 org_id,
                 subinventory,
                 bill_of_lading,
                 packing_slip,
                 shipped_date,
                 expected_receipt_date,
                 comments,
                 item_id,
                 truck_num,
                 category_id,
                 item_description,
                 container_num,
                 resource_code,
                 create_debit_memo_flag,
                 employee_id,
                 vendor_id,
                 vendor_site_id,
                 ship_to_location_id,
                 deliver_to_person_id,
                 deliver_to_location_id,
                 to_organization_id)
              values
                (sysdate,
                 g_conc_program_id,
                 g_prog_appl_id,
                 g_request_id,
                 g_last_update_login,
                 g_last_updated_by,
                 sysdate,
                 g_created_by,
                 sysdate,
                 cur_txn_rec.attribute15,
                 cur_txn_rec.attribute14,
                 cur_txn_rec.attribute13,
                 cur_txn_rec.attribute12,
                 cur_txn_rec.attribute11,
                 cur_txn_rec.attribute10,
                 cur_txn_rec.attribute9,
                 cur_txn_rec.attribute8,
                 cur_txn_rec.attribute7,
                 cur_txn_rec.attribute6,
                 cur_txn_rec.attribute5,
                 cur_txn_rec.attribute4,
                 cur_txn_rec.attribute3,
                 cur_txn_rec.attribute2,
                 cur_txn_rec.attribute1,
                 cur_txn_rec.attribute_category,
                 --- DEFECT 2838 MISSING LINE ATTRIBUTE COLUMNS MAPPING
                 cur_txn_rec.attribute15,
                 cur_txn_rec.attribute14,
                 cur_txn_rec.attribute13,
                 cur_txn_rec.attribute12,
                 cur_txn_rec.attribute11,
                 cur_txn_rec.attribute10,
                 cur_txn_rec.attribute9,
                 cur_txn_rec.attribute8,
                 cur_txn_rec.attribute7,
                 cur_txn_rec.attribute6,
                 cur_txn_rec.attribute5,
                 cur_txn_rec.attribute4,
                 cur_txn_rec.attribute3,
                 cur_txn_rec.attribute2,
                 cur_txn_rec.attribute1,
                 cur_txn_rec.attribute_category,
                 --- DEFECT 2838 MISSING LINE ATTRIBUTE COLUMNS MAPPING
                 v_head_id,
                 v_group_id,
                 v_txn_id,
                 'RECEIVE',
                 'PENDING',
                 'BATCH',
                 'PENDING',
                 'DELIVER',
                 'VENDOR',
                 'RECEIVING',
                 'Y',
                 cur_txn_rec.transaction_date,
                 cur_txn_rec.leg_quantity,
                 cur_txn_rec.leg_uom_code,
                 cur_txn_rec.leg_source_document_code,
                 cur_txn_rec.po_header_id,
                 cur_txn_rec.po_line_id,
                 cur_txn_rec.po_line_location_id,
                 cur_txn_rec.po_distribution_id,
                 cur_txn_rec.leg_currency_code,
                 cur_txn_rec.leg_currency_conversion_type,
                 cur_txn_rec.leg_currency_conversion_rate,
                 cur_txn_rec.leg_currency_conversion_date,
                 cur_txn_rec.org_id,
                 cur_txn_rec.leg_subinventory,
                 cur_txn_rec.leg_bill_of_lading,
                 cur_txn_rec.leg_packing_slip,
                 cur_txn_rec.shipped_date,
                 cur_txn_rec.expected_receipt_date,
                 cur_txn_rec.leg_comments,
                 cur_txn_rec.inventory_item_id,
                 cur_txn_rec.leg_truck_num,
                 cur_txn_rec.category_id,
                 cur_txn_rec.item_description,
                 cur_txn_rec.leg_container_num,
                 cur_txn_rec.leg_resource_code,
                 cur_txn_rec.create_debit_memo_flag,
                 cur_txn_rec.employee_id,
                 cur_txn_rec.vendor_id,
                 cur_txn_rec.vendor_site_id,
                 cur_txn_rec.ship_to_locations_id,
                 cur_txn_rec.deliver_to_person_id,
                 cur_txn_rec.deliver_to_location_id,
                 cur_txn_rec.to_organization_id);
            exception
              when others then
                lv_l_err_flag := 'Y';
                l_err_code    := 'ETN_EXPT_LINE_INSERT_ERR';
                l_err_msg     := 'Error: Exceptional error while inserting transaction record :  ' ||
                                 sqlerrm;
                log_errors(pin_interface_txn_id    => cur_txn_rec.interface_txn_id,
                           piv_source_table        => 'xxpo_receipt_trx_stg',
                           piv_source_column_name  => null,
                           piv_source_column_value => null,
                           piv_source_keyname1     => null,
                           piv_source_keyvalue1    => null,
                           piv_error_type          => 'ERR_INT',
                           piv_error_code          => l_err_code,
                           piv_error_message       => l_err_msg);
            end;
            if lv_l_err_flag = 'N' then
              update xxpo_receipt_trx_stg
                 set process_flag             = 'P',
                     error_type               = null,
                     request_id               = g_request_id,
                     interface_header_id      = v_head_id,
                     interface_transaction_id = v_txn_id,
                     last_updated_date        = sysdate,
                     last_updated_by          = g_last_updated_by,
                     last_update_login        = g_last_update_login
               where interface_txn_id = cur_txn_rec.interface_txn_id;
              xxetn_debug_pkg.add_debug('Record id ' ||
                                        cur_txn_rec.interface_txn_id ||
                                        'is successfully processed');
            else
              g_retcode := 1;
              update xxpo_receipt_trx_stg
                 set process_flag      = 'E',
                     error_type        = 'ERR_INT',
                     request_id        = g_request_id,
                     last_updated_date = sysdate,
                     last_updated_by   = g_last_updated_by,
                     last_update_login = g_last_update_login
               where interface_txn_id = cur_txn_rec.interface_txn_id;
              xxetn_debug_pkg.add_debug('Record id ' ||
                                        cur_txn_rec.interface_txn_id ||
                                        'is erred out');
            end if;
          end loop;
        end loop;
        commit;
      exception
        when others then
          print_log_message(sqlerrm || ', ' || sqlcode ||
                            'Unexpected Error while executing Conversion procedure');
          g_retcode := 2;
          print_log_message('Error : Backtace : ' ||
                            dbms_utility.format_error_backtrace);
      end;
    elsif l_operating_count > 1 then
      for cur_operating_unit_hdr_rec in cur_operating_unit_hdr loop
        begin
          fnd_file.put_line(fnd_file.log,
                            'Inside Loop for count greater than 1');
          g_retcode := 0;
          select rcv_interface_groups_s.nextval into v_group_id from dual;
          fnd_file.put_line(fnd_file.log, v_group_id);
          for cur_headers_multiple_rec in cur_headers_multiple(cur_operating_unit_hdr_rec.operating_unit) loop
            v_head_id := null;
            select rcv_headers_interface_s.nextval
              into v_head_id
              from dual;
            xxetn_debug_pkg.add_debug('INSIDE UPLOAD PROCEDURE LOOP OF PO RECEIPT HEADER ' ||
                                      sysdate);
            lv_h_err_flag := 'N';
            begin
              insert into rcv_headers_interface
                (last_update_login,
                 last_updated_by,
                 last_update_date,
                 created_by,
                 creation_date,
                 attribute15,
                 attribute14,
                 attribute13,
                 attribute12,
                 attribute11,
                 attribute10,
                 attribute9,
                 attribute8,
                 attribute7,
                 attribute6,
                 attribute5,
                 attribute4,
                 attribute3,
                 attribute2,
                 attribute1,
                 attribute_category,
                 header_interface_id,
                 group_id,
                 processing_status_code,
                 receipt_source_code,
                 transaction_type,
                 auto_transact_code,
                 receipt_num,
                 bill_of_lading,
                 packing_slip,
                 shipped_date,
                 expected_receipt_date,
                 org_id,
                 num_of_containers,
                 waybill_airbill_num,
                 comments,
                 packaging_code,
                 freight_terms,
                 freight_bill_number,
                 currency_code,
                 conversion_rate_type,
                 conversion_rate,
                 conversion_rate_date,
                 vendor_id,
                 vendor_site_id,
                 employee_id,
                 ship_to_organization_id,
                 location_id)
              values
                (g_last_update_login,
                 g_last_updated_by,
                 sysdate,
                 g_created_by,
                 sysdate,
                 cur_headers_multiple_rec.attribute15,
                 cur_headers_multiple_rec.attribute14,
                 cur_headers_multiple_rec.attribute13,
                 cur_headers_multiple_rec.attribute12,
                 cur_headers_multiple_rec.attribute11,
                 cur_headers_multiple_rec.attribute10,
                 cur_headers_multiple_rec.attribute9,
                 cur_headers_multiple_rec.attribute8,
                 cur_headers_multiple_rec.attribute7,
                 cur_headers_multiple_rec.attribute6,
                 cur_headers_multiple_rec.attribute5,
                 cur_headers_multiple_rec.attribute4,
                 cur_headers_multiple_rec.attribute3,
                 cur_headers_multiple_rec.attribute2,
                 cur_headers_multiple_rec.attribute1,
                 cur_headers_multiple_rec.attribute_category,
                 v_head_id,
                 v_group_id,
                 'PENDING',
                 cur_headers_multiple_rec.leg_receipt_source_code,
                 'NEW',
                 'DELIVER',
                 cur_headers_multiple_rec.leg_receipt_num,
                 cur_headers_multiple_rec.leg_bill_of_lading,
                 cur_headers_multiple_rec.leg_packing_slip,
                 cur_headers_multiple_rec.shipped_date,
                 cur_headers_multiple_rec.expected_receipt_date,
                 cur_headers_multiple_rec.org_id,
                 cur_headers_multiple_rec.leg_num_of_containers,
                 cur_headers_multiple_rec.leg_waybill_airbill_num,
                 cur_headers_multiple_rec.leg_comments,
                 cur_headers_multiple_rec.leg_packaging_code,
                 cur_headers_multiple_rec.freight_terms,
                 cur_headers_multiple_rec.leg_freight_bill_number,
                 cur_headers_multiple_rec.leg_currency_code,
                 cur_headers_multiple_rec.leg_conversion_rate_type,
                 cur_headers_multiple_rec.leg_conversion_rate,
                 cur_headers_multiple_rec.leg_conversion_rate_date,
                 cur_headers_multiple_rec.vendor_id,
                 cur_headers_multiple_rec.vendor_site_id,
                 cur_headers_multiple_rec.employee_id,
                 cur_headers_multiple_rec.ship_to_organization_id,
                 cur_headers_multiple_rec.ship_to_location_id);
            exception
              when others then
                lv_h_err_flag := 'Y';
                l_err_code    := 'ETN_RCPT_HEADER_INSERT_ERR';
                l_err_msg     := 'Error: Exceptional error while inserting header record :  ' ||
                                 sqlerrm;
                log_errors(pin_interface_txn_id    => cur_headers_multiple_rec.interface_txn_id,
                           piv_source_table        => 'xxpo_receipt_hdr_stg',
                           piv_source_column_name  => null,
                           piv_source_column_value => null,
                           piv_source_keyname1     => null,
                           piv_source_keyvalue1    => null,
                           piv_error_type          => 'ERR_INT',
                           piv_error_code          => l_err_code,
                           piv_error_message       => l_err_msg);
            end;
            if lv_h_err_flag = 'N' then
              update xxpo_receipt_hdr_stg
                 set process_flag        = 'P',
                     error_type          = null,
                     interface_header_id = v_head_id,
                     request_id          = g_request_id,
                     last_updated_date   = sysdate,
                     last_updated_by     = g_last_updated_by,
                     last_update_login   = g_last_update_login
               where interface_txn_id =
                     cur_headers_multiple_rec.interface_txn_id;
              xxetn_debug_pkg.add_debug('Record id ' ||
                                        cur_headers_multiple_rec.interface_txn_id ||
                                        'is successfully processed');
            else
              g_retcode := 1;
              update xxpo_receipt_hdr_stg
                 set process_flag      = 'E',
                     error_type        = 'ERR_INT',
                     request_id        = g_request_id,
                     last_updated_date = sysdate,
                     last_updated_by   = g_last_updated_by,
                     last_update_login = g_last_update_login
               where interface_txn_id =
                     cur_headers_multiple_rec.interface_txn_id;
              xxetn_debug_pkg.add_debug('Record id ' ||
                                        cur_headers_multiple_rec.interface_txn_id ||
                                        'is erred out');
            end if;
            for cur_txn_rec in cur_txn(cur_headers_multiple_rec.leg_shipment_header_id) loop
              xxetn_debug_pkg.add_debug('INSIDE UPLOAD PROCEDURE LOOP OF PO TXN ' ||
                                        sysdate);
              lv_l_err_flag := 'N';
              v_txn_id      := null;
              select rcv_transactions_interface_s.nextval
                into v_txn_id
                from dual;
              begin
                insert into rcv_transactions_interface
                  (program_update_date,
                   program_id,
                   program_application_id,
                   request_id,
                   last_update_login,
                   last_updated_by,
                   last_update_date,
                   created_by,
                   creation_date,
                   attribute15,
                   attribute14,
                   attribute13,
                   attribute12,
                   attribute11,
                   attribute10,
                   attribute9,
                   attribute8,
                   attribute7,
                   attribute6,
                   attribute5,
                   attribute4,
                   attribute3,
                   attribute2,
                   attribute1,
                   attribute_category,
                   --- DEFECT 2838 MISSING LINE ATTRIBUTE COLUMNS MAPPING
                   ship_line_attribute15,
                   ship_line_attribute14,
                   ship_line_attribute13,
                   ship_line_attribute12,
                   ship_line_attribute11,
                   ship_line_attribute10,
                   ship_line_attribute9,
                   ship_line_attribute8,
                   ship_line_attribute7,
                   ship_line_attribute6,
                   ship_line_attribute5,
                   ship_line_attribute4,
                   ship_line_attribute3,
                   ship_line_attribute2,
                   ship_line_attribute1,
                   ship_line_attribute_category,
                   --- DEFECT 2838 MISSING LINE ATTRIBUTE COLUMNS MAPPING
                   header_interface_id,
                   group_id,
                   interface_transaction_id,
                   transaction_type,
                   processing_status_code,
                   processing_mode_code,
                   transaction_status_code,
                   auto_transact_code,
                   receipt_source_code,
                   destination_type_code,
                   validation_flag,
                   transaction_date,
                   quantity,
                   uom_code,
                   source_document_code,
                   po_header_id,
                   po_line_id,
                   po_line_location_id,
                   po_distribution_id,
                   currency_code,
                   currency_conversion_type,
                   currency_conversion_rate,
                   currency_conversion_date,
                   org_id,
                   subinventory,
                   bill_of_lading,
                   packing_slip,
                   shipped_date,
                   expected_receipt_date,
                   comments,
                   item_id,
                   truck_num,
                   category_id,
                   item_description,
                   container_num,
                   resource_code,
                   create_debit_memo_flag,
                   employee_id,
                   vendor_id,
                   vendor_site_id,
                   ship_to_location_id,
                   deliver_to_person_id,
                   deliver_to_location_id,
                   to_organization_id)
                values
                  (sysdate,
                   g_conc_program_id,
                   g_prog_appl_id,
                   g_request_id,
                   g_last_update_login,
                   g_last_updated_by,
                   sysdate,
                   g_created_by,
                   sysdate,
                   cur_txn_rec.attribute15,
                   cur_txn_rec.attribute14,
                   cur_txn_rec.attribute13,
                   cur_txn_rec.attribute12,
                   cur_txn_rec.attribute11,
                   cur_txn_rec.attribute10,
                   cur_txn_rec.attribute9,
                   cur_txn_rec.attribute8,
                   cur_txn_rec.attribute7,
                   cur_txn_rec.attribute6,
                   cur_txn_rec.attribute5,
                   cur_txn_rec.attribute4,
                   cur_txn_rec.attribute3,
                   cur_txn_rec.attribute2,
                   cur_txn_rec.attribute1,
                   cur_txn_rec.attribute_category,
                   --- DEFECT 2838 MISSING LINE ATTRIBUTE COLUMNS MAPPING
                   cur_txn_rec.attribute15,
                   cur_txn_rec.attribute14,
                   cur_txn_rec.attribute13,
                   cur_txn_rec.attribute12,
                   cur_txn_rec.attribute11,
                   cur_txn_rec.attribute10,
                   cur_txn_rec.attribute9,
                   cur_txn_rec.attribute8,
                   cur_txn_rec.attribute7,
                   cur_txn_rec.attribute6,
                   cur_txn_rec.attribute5,
                   cur_txn_rec.attribute4,
                   cur_txn_rec.attribute3,
                   cur_txn_rec.attribute2,
                   cur_txn_rec.attribute1,
                   cur_txn_rec.attribute_category,
                   --- DEFECT 2838 MISSING LINE ATTRIBUTE COLUMNS MAPPING
                   v_head_id,
                   v_group_id,
                   v_txn_id,
                   'RECEIVE',
                   'PENDING',
                   'BATCH',
                   'PENDING',
                   'DELIVER',
                   'VENDOR',
                   'RECEIVING',
                   'Y',
                   cur_txn_rec.transaction_date,
                   cur_txn_rec.leg_quantity,
                   cur_txn_rec.leg_uom_code,
                   cur_txn_rec.leg_source_document_code,
                   cur_txn_rec.po_header_id,
                   cur_txn_rec.po_line_id,
                   cur_txn_rec.po_line_location_id,
                   cur_txn_rec.po_distribution_id,
                   cur_txn_rec.leg_currency_code,
                   cur_txn_rec.leg_currency_conversion_type,
                   cur_txn_rec.leg_currency_conversion_rate,
                   cur_txn_rec.leg_currency_conversion_date,
                   cur_txn_rec.org_id,
                   cur_txn_rec.leg_subinventory,
                   cur_txn_rec.leg_bill_of_lading,
                   cur_txn_rec.leg_packing_slip,
                   cur_txn_rec.shipped_date,
                   cur_txn_rec.expected_receipt_date,
                   cur_txn_rec.leg_comments,
                   cur_txn_rec.inventory_item_id,
                   cur_txn_rec.leg_truck_num,
                   cur_txn_rec.category_id,
                   cur_txn_rec.item_description,
                   cur_txn_rec.leg_container_num,
                   cur_txn_rec.leg_resource_code,
                   cur_txn_rec.create_debit_memo_flag,
                   cur_txn_rec.employee_id,
                   cur_txn_rec.vendor_id,
                   cur_txn_rec.vendor_site_id,
                   cur_txn_rec.ship_to_locations_id,
                   cur_txn_rec.deliver_to_person_id,
                   cur_txn_rec.deliver_to_location_id,
                   cur_txn_rec.to_organization_id);
              exception
                when others then
                  lv_l_err_flag := 'Y';
                  l_err_code    := 'ETN_EXPT_LINE_INSERT_ERR';
                  l_err_msg     := 'Error: Exceptional error while inserting transaction record :  ' ||
                                   sqlerrm;
                  log_errors(pin_interface_txn_id    => cur_txn_rec.interface_txn_id,
                             piv_source_table        => 'xxpo_receipt_trx_stg',
                             piv_source_column_name  => null,
                             piv_source_column_value => null,
                             piv_source_keyname1     => null,
                             piv_source_keyvalue1    => null,
                             piv_error_type          => 'ERR_INT',
                             piv_error_code          => l_err_code,
                             piv_error_message       => l_err_msg);
              end;
              if lv_l_err_flag = 'N' then
                update xxpo_receipt_trx_stg
                   set process_flag             = 'P',
                       error_type               = null,
                       request_id               = g_request_id,
                       interface_header_id      = v_head_id,
                       interface_transaction_id = v_txn_id,
                       last_updated_date        = sysdate,
                       last_updated_by          = g_last_updated_by,
                       last_update_login        = g_last_update_login
                 where interface_txn_id = cur_txn_rec.interface_txn_id;
                xxetn_debug_pkg.add_debug('Record id ' ||
                                          cur_txn_rec.interface_txn_id ||
                                          'is successfully processed');
              else
                g_retcode := 1;
                update xxpo_receipt_trx_stg
                   set process_flag      = 'E',
                       error_type        = 'ERR_INT',
                       request_id        = g_request_id,
                       last_updated_date = sysdate,
                       last_updated_by   = g_last_updated_by,
                       last_update_login = g_last_update_login
                 where interface_txn_id = cur_txn_rec.interface_txn_id;
                xxetn_debug_pkg.add_debug('Record id ' ||
                                          cur_txn_rec.interface_txn_id ||
                                          'is erred out');
              end if;
            end loop;
          end loop;
          commit;
        exception
          when others then
            print_log_message(sqlerrm || ', ' || sqlcode ||
                              'Unexpected Error while executing Conversion procedure');
            g_retcode := 2;
            print_log_message('Error : Backtace : ' ||
                              dbms_utility.format_error_backtrace);
        end;
      end loop;
    end if;
  exception
    when others then
      xxetn_debug_pkg.add_debug('Record  errored ');
  end;

  ----------------------------------changes end SM---------------------
  -- ========================
  -- Procedure: main
  -- =============================================================================
  --   This is a main public procedure, which will be invoked through concurrent
  --   program.
  --
  -- =============================================================================
  --
  --
  --  Input Parameters :
  --    piv_run_mode        : Control the program execution for VALIDATE
  --    p_entity          : Appropriate entity can be selected based on conversion
  --                        requirements
  --    pin_batch_id        : List all unique batches from staging table , this will
  --                        be NULL for first Conversion Run.
  --    piv_process_records : Conditionally available only when pin_batch_id is popul-
  --                        -ated. Otherwise this will be disabled and defaulted
  --                        to ALL
  --   piv_operating_unit   : Operating unit
  --
  --  Output Parameters :
  --    pov_errbuf          : Standard output parameter for concurrent program
  --    pon_retcode         : Standard output parameter for concurrent program
  --
  --  Return     : Not applicable
  --
  procedure main(pov_errbuf        out nocopy varchar2,
                 pon_retcode       out nocopy number,
                 pin_run_mode      in varchar2,
                 piv_hidden_param1 in varchar2,
                 -- Dummy/Hidden Parameter 1
                 pin_batch_id     in number,
                 piv_hidden_param in varchar2,
                 -- Dummy/Hidden Parameter
                 piv_process_records in varchar2,
                 piv_operating_unit  in varchar2,
                 piv_trx_date        in varchar2) is
    l_debug_err varchar2(2000);
    --l_count NUMBER;
    pov_ret_stats varchar2(100);
    pov_err_msg   varchar2(1000);
    l_warn_excep exception;
    l_load_ret_h_stats varchar2(1) := 'S';
    l_load_ret_l_stats varchar2(1) := 'S';
    l_load_ret_d_stats varchar2(1) := 'S';
    l_h_load_err_msg   varchar2(1000);
    l_l_load_err_msg   varchar2(1000);
    l_d_load_err_msg   varchar2(1000);
    l_date_warn_excep exception;
  begin
    g_run_mode           := pin_run_mode;
    g_batch_id           := pin_batch_id;
    g_process_records    := piv_process_records;
    g_leg_operating_unit := piv_operating_unit;
    if piv_trx_date is not null then
      g_transaction_date := fnd_date.canonical_to_date(piv_trx_date); --1.1
    else
      g_transaction_date := null;
    end if;
    -- Initialize debug procedure
    xxetn_debug_pkg.initialize_debug(pov_err_msg      => l_debug_err,
                                     piv_program_name => 'PO_Receipt_Conv');
    xxetn_debug_pkg.add_debug(piv_debug_msg => 'Initialized Debug');
    xxetn_debug_pkg.add_debug('Program Parameters');
    xxetn_debug_pkg.add_debug('---------------------------------------------');
    xxetn_debug_pkg.add_debug('Run Mode            : ' || pin_run_mode);
    xxetn_debug_pkg.add_debug('Batch ID            : ' || pin_batch_id);
    xxetn_debug_pkg.add_debug('Reprocess records     : ' ||
                              piv_process_records);
    print_log_message('Program Parameters');
    print_log_message('---------------------------------------------');
    print_log_message('Run Mode            : ' || pin_run_mode);
    print_log_message('Batch ID            : ' || pin_batch_id);
    print_log_message('Reprocess records     : ' || piv_process_records);
    print_log_message('Legacy Operating Unit     : ' || piv_operating_unit);
    print_log_message('Transaction Date(DD-MON-YYYY)     : ' ||
                      piv_trx_date);
    print_log_message('---------------------------------------------');
    if pin_run_mode = 'LOAD-DATA' then
      xxetn_debug_pkg.add_debug(piv_debug_msg => 'Calling Load data procedure');
      print_log_message('Calling procedure load_header');
      print_log_message('');
      load_header(pov_ret_stats => l_load_ret_h_stats,
                  pov_err_msg   => l_h_load_err_msg);
      if l_load_ret_h_stats <> 'S' then
        print_log_message('Error in procedure load_header' ||
                          l_h_load_err_msg);
        print_log_message('');
        raise l_warn_excep;
      end if;
      print_log_message('Calling procedure load_transaction');
      load_transaction(pov_ret_stats => l_load_ret_l_stats,
                       pov_err_msg   => l_l_load_err_msg);
      if l_load_ret_l_stats <> 'S' then
        print_log_message('Error in procedure load_transaction' ||
                          l_l_load_err_msg);
        print_log_message('');
        raise l_warn_excep;
      end if;
      xxetn_debug_pkg.add_debug(piv_debug_msg => 'Load data procedure completed');
    elsif pin_run_mode = 'PRE-VALIDATE' then
      xxetn_debug_pkg.add_debug(piv_debug_msg => 'Calling pre_validate mode');
      -- call procedure to pre-validate set up
      pre_validate();
      pon_retcode := g_retcode;
    elsif pin_run_mode = 'VALIDATE' then
      if g_transaction_date is null then
        raise l_date_warn_excep;
      end if;
      g_new_run_seq_id := xxetn_run_sequences_s.nextval;
      xxetn_debug_pkg.add_debug(piv_debug_msg => 'Calling assign_batch_id procedure in validate mode.');
      -- call procedure to assign batch id
      assign_batch_id;
      xxetn_common_error_pkg.g_batch_id := g_batch_id;
      -- batch id
      xxetn_common_error_pkg.g_run_seq_id := g_new_run_seq_id;
      -- run sequence id
      print_log_message('Batch ID for Validate Mode: ' || g_batch_id);
      print_log_message('Run Sequence ID for Validate Mode : ' ||
                        g_new_run_seq_id);
      xxetn_debug_pkg.add_debug(piv_debug_msg => 'Calling validate mode.');
      validate_rcpt;
      pon_retcode := g_retcode;
    elsif pin_run_mode = 'CONVERSION' then
      if pin_batch_id is not null then
        g_new_run_seq_id := xxetn_run_sequences_s.nextval;
        xxetn_debug_pkg.add_debug(piv_debug_msg => 'Updating run sequence id in conversion mode.');
        print_log_message('Run Sequence ID for Import Mode : ' ||
                          g_new_run_seq_id);
        begin
          xxetn_debug_pkg.add_debug(piv_debug_msg => 'Reprocess updating run sequence id: Headers');
          update xxpo_receipt_hdr_stg
             set run_sequence_id        = g_new_run_seq_id,
                 last_updated_date      = sysdate,
                 last_updated_by        = g_user_id,
                 last_update_login      = g_login_id,
                 program_application_id = g_prog_appl_id,
                 program_id             = g_conc_program_id
           where 1 = 1
             and batch_id = g_batch_id
             and process_flag = 'V';
        exception
          when others then
            print_log_message('Error : Exception occured while updating run seq id for reprocess for headers' ||
                              substr(sqlerrm, 1, 150));
        end;
        begin
          xxetn_debug_pkg.add_debug(piv_debug_msg => 'Reprocess updating run sequence id: Transaction');
          update xxpo_receipt_trx_stg
             set run_sequence_id        = g_new_run_seq_id,
                 last_updated_date      = sysdate,
                 last_updated_by        = g_user_id,
                 last_update_login      = g_login_id,
                 program_application_id = g_prog_appl_id,
                 program_id             = g_conc_program_id
           where 1 = 1
             and batch_id = g_batch_id
             and process_flag = 'V';
        exception
          when others then
            print_log_message('Error : Exception occured while updating run seq id for reprocess Transaction ' ||
                              substr(sqlerrm, 1, 150));
        end;
        commit;
        xxetn_common_error_pkg.g_batch_id := g_batch_id;
        -- batch id
        xxetn_common_error_pkg.g_run_seq_id := g_new_run_seq_id;
        -- run sequence id
        xxetn_debug_pkg.add_debug(piv_debug_msg => 'Calling conversion mode.');
        -- call procedure to convert the data
        conversion;
        pon_retcode := g_retcode;
      else
        print_log_message('For conversion run mode batch id cannot be NULL');
        raise l_warn_excep;
      end if;
    elsif pin_run_mode = 'RECONCILE' then
      --- Calling print report procedure
      xxetn_debug_pkg.add_debug(piv_debug_msg => 'Inside Reconcile Mode');
      xxetn_debug_pkg.add_debug(piv_debug_msg => 'Calling print report');
      print_report;
    end if;
    -- call once to dump pending error records which are less than profile value.
    xxetn_common_error_pkg.add_error(pov_return_status => pov_ret_stats,
                                     pov_error_msg     => pov_err_msg,
                                     pi_source_tab     => g_tab);
  exception
    when l_warn_excep then
      print_log_message('Main program procedure encounter user exception ' ||
                        substr(sqlerrm, 1, 150));
      pov_errbuf  := 'Error : Main program procedure encounter user exception. ' ||
                     substr(sqlerrm, 1, 150);
      pon_retcode := 1;
    when l_date_warn_excep then
      print_log_message('Transaction Date Parameter is NULL .Please enter Transaction Date for VALIDATE Mode.');
      pov_errbuf  := 'Transaction Date Parameter is NULL .Please enter Transaction Date for VALIDATE Mode.';
      pon_retcode := 2;
    when others then
      pov_errbuf  := 'Error : Main Program Procedure: MAIN encounter error. Reason: ' ||
                     substr(sqlerrm, 1, 150);
      pon_retcode := 2;
      print_log_message('Error : Main Program Procedure: MAIN encounter error. Reason: ' ||
                        substr(sqlerrm, 1, 150));
      print_log_message('Error : Backtace : ' ||
                        dbms_utility.format_error_backtrace);
  end main;

end xxpo_poreceipt_cnv_pkg;
/
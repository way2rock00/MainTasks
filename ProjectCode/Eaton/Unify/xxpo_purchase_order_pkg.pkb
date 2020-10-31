--Begin Revision History
--<<<
-- 02-Jun-2017 01:08:06 C9914192 /main/34
-- 
--<<<
-- 18-Jul-2017 02:18:41 E0406765 /main/35
-- 
-- <<<
--End Revision History  
create or replace package body xxpo_purchase_order_pkg as

  ------------------------------------------------------------------------------------------
  --    Owner        : EATON CORPORATION.
  --    Application  : Purchasing
  --    Schema       : APPS
  --    Compile AS   : APPS
  --    File Name    : xxpo_purchase_order_pkg.pkb
  --    Date         : 05-MAY-2014
  --    Author       : Rohit Devadiga
  --    Description  : Package body for Purchase Order Conversion
  --    Version      : $ETNHeader: /CCSTORE/ccweb/E0406765/E0406765_view_XXPO_TOP/vobs/PO_TOP/xxpo/12.0.0/install/xxpo_purchase_order_pkg.pkb /main/35 18-Jul-2017 02:18:41 E0406765  $
  --    Parameters   :
  --    piv_run_mode        : Control the program execution for VALIDATE
  --    pin_batch_id        : List all unique batches from staging table , this will
  --                        be NULL for first Conversion Run.
  --    piv_process_records : Conditionally available only when pin_batch_id is popul-
  --                        -ated. Otherwise this will be disabled and defaulted
  --                        to ALL
  --    Change History
  --  ========================================================================================================================
  --    v1.0         Rohit Devadiga    05-May-2014      Initial Creation
  --    V1.1         Rohit Devadiga    11-Aug-2014      Modified for performance Issue
  --    V1.2         Manpreet Singh    25-Mar-2015      GRNI changes
  --    V1.3         Kulraj Singh      31-Jul-2015      Performance Tuning changes.Added Index hints
  --    V1.14        Shishir Mohan     08-Aug-2015      UOM Code Change, variance and accrual account change,
  --                                                    IO Check, Agent Number changes, Segment Error changes
  --                                                    Deliver to Person Id changes
  --    V1.14        Shishir Mohan     08-Aug-2015      Expenditure Org and Expenditure type Logic in PO Change,
  --                                                    DEFAULT Category for Conversion PO's(CR # 331031)
  --    V1.15        Kulraj Singh      13-Aug-2015      Added hints and additional condition in procedure conversion
  --                                                    to improve performance--
  --    V1.15        Shriram P         15-AUG-2015      Added PO Header Id in Agent Qry to reduce number of errors.
  --    V1.16        Shishir Mohan     24-SEP-2015      Inclusion of Ship to Organization Mapping Logic Using Cross-Reference API
  --    V1.17        Shishir Mohan     24-SEP-2015      Inclusion of Destination Organization Mapping Logic Using Cross-Reference API
  --    V1.18        Shishir Mohan     28-SEP-2015      Changes for Location Mapping Logic PMC 334707
  --    V1.19        Shishir Mohan     10-OCT-2015      Inclusion of TAX Mapping Logic using Cross-Reference API
  --    V1.20        Rohit Devadiga    28-OCT-2015      Changes done for CR339756
  --    V1.21        Shishir Mohan     13-JAN-2016      leg_uom_code error handling changes
  --    V1.22        Shriram Phenani   18-JAN-2016      Defect 4874
  --    V1.23        Shriram Phenani   22-JAN-2016      Tax Code Change NO TAX removed.Line 9359
  --    v1.24        Shriram Phenani   25-MAR-2016      Added column match receipt in load
  --                                                    In Load Header decode for rate type.Update in Load Line for tolerance.
  --                                                    Added NONE in populate data in po_line_interface for rcv_exception_code
  --                                                    CR 372677 change tolerance if more than 100
  --                                                    Added po_line_locations_interface in load and tie back
  --                                                    xxpo_dup_po  added org_id
  --    v1.25                                           Change for rate type on match option R POs
  --    v1.26                                           Reverting v1.25
  --    ======================================================================================================================
  ------------------------------------------------------------------------------------------
  g_request_id          number default fnd_global.conc_request_id;

  g_prog_appl_id        number default fnd_global.prog_appl_id;

  g_conc_program_id     number default fnd_global.conc_program_id;

  g_user_id             number default fnd_global.user_id;

  g_login_id            number default fnd_global.login_id;

  g_created_by          number := apps.fnd_global.user_id;

  g_last_updated_by     number := apps.fnd_global.user_id;

  g_last_update_login   number := apps.fnd_global.login_id;

  g_tab                 xxetn_common_error_pkg.g_source_tab_type;

  g_err_tab_limit       number default fnd_profile.value('ETN_FND_ERROR_TAB_LIMIT');

  g_ou_lookup           fnd_lookup_types_tl.lookup_type%type := 'ETN_COMMON_OU_MAP';

  g_payment_term_lookup fnd_lookup_types_tl.lookup_type%type := 'XXAP_PAYTERM_MAPPING';

  g_task_lookup         fnd_lookup_types_tl.lookup_type%type := 'XXETN_PA_TASK_MAPPING';

  g_tax_code_lookup     fnd_lookup_types_tl.lookup_type%type := 'XXEBTAX_TAX_CODE_MAPPING';

  g_err_cnt             number default 1;

  g_leg_operating_unit  varchar2(240);

  g_run_mode            varchar2(100);

  g_batch_id            number;

  g_process_records     varchar2(100);

  g_new_run_seq_id      number;

  g_retcode             number;

  g_document_type       varchar2(25) := 'STANDARD';

  -- g_document_subtype    VARCHAR2(25) := 'STANDARD';
  g_direction varchar2(240) := 'LEGACY-TO-R12';

  g_coa_error     constant varchar2(30) := 'Error';

  g_coa_processed constant varchar2(30) := 'Processed';

  g_total_count  number;

  g_failed_count number;

  g_loaded_count number;

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
    l_tot_dist_count    number;
    l_fail_header_count number;
    l_fail_line_count   number;
    l_fail_dist_count   number;
    l_fail_count_head   number;
    l_fail_count_line   number;
    l_fail_count_dist   number;
    l_suc_count_head    number;
    l_suc_count_line    number;
    l_suc_count_dist    number;
  begin
    -- Get Total Count of Records in header staging
    select count(1)
      into l_tot_header_count
      from xxpo_po_header_stg xds
     where xds.batch_id = nvl(g_batch_id, xds.batch_id)
       and xds.batch_id is not null
       and xds.run_sequence_id is not null;
    -- Get Total Count of Records in line staging
    select count(1)
      into l_tot_line_count
      from xxpo_po_line_shipment_stg xds
     where xds.batch_id = nvl(g_batch_id, xds.batch_id)
       and xds.batch_id is not null
       and xds.run_sequence_id is not null;
    -- Get Total Count of Records in distribution staging
    select count(1)
      into l_tot_dist_count
      from xxpo_po_distribution_stg xds
     where xds.batch_id = nvl(g_batch_id, xds.batch_id)
       and xds.batch_id is not null
       and xds.run_sequence_id is not null;
    -- Get Total Count of Failed Records in Validation header
    select count(1)
      into l_fail_header_count
      from xxpo_po_header_stg xds
     where xds.batch_id = nvl(g_batch_id, xds.batch_id)
       and xds.process_flag = 'E'
       and xds.error_type = 'ERR_VAL'
       and xds.batch_id is not null
       and xds.run_sequence_id is not null;
    -- Get Total Count of Failed Records in Validation line
    select count(1)
      into l_fail_line_count
      from xxpo_po_line_shipment_stg xds
     where xds.batch_id = nvl(g_batch_id, xds.batch_id)
       and xds.process_flag = 'E'
       and xds.error_type = 'ERR_VAL'
       and xds.batch_id is not null
       and xds.run_sequence_id is not null;
    -- Get Total Count of Failed Records in Validation distribution
    select count(1)
      into l_fail_dist_count
      from xxpo_po_distribution_stg xds
     where xds.batch_id = nvl(g_batch_id, xds.batch_id)
       and xds.process_flag = 'E'
       and xds.error_type = 'ERR_VAL'
       and xds.batch_id is not null
       and xds.run_sequence_id is not null;
    -- Get Total Count of Failed Records in Conversion header
    select count(1)
      into l_fail_count_head
      from xxpo_po_header_stg xds
     where xds.batch_id = nvl(g_batch_id, xds.batch_id)
       and xds.process_flag = 'E'
       and xds.error_type = 'ERR_INT'
       and xds.batch_id is not null
       and xds.run_sequence_id is not null;
    -- Get Total Count of Failed Records in Conversion line
    select count(1)
      into l_fail_count_line
      from xxpo_po_line_shipment_stg xds
     where xds.batch_id = nvl(g_batch_id, xds.batch_id)
       and xds.process_flag = 'E'
       and xds.error_type = 'ERR_INT'
       and xds.batch_id is not null
       and xds.run_sequence_id is not null;
    -- Get Total Count of Failed Records in Conversion distribution
    select count(1)
      into l_fail_count_dist
      from xxpo_po_distribution_stg xds
     where xds.batch_id = nvl(g_batch_id, xds.batch_id)
       and xds.process_flag = 'E'
       and xds.error_type = 'ERR_INT'
       and xds.batch_id is not null
       and xds.run_sequence_id is not null;
    -- Get Total Count of Converted Records header
    select count(1)
      into l_suc_count_head
      from xxpo_po_header_stg xds
     where xds.batch_id = nvl(g_batch_id, xds.batch_id)
       and xds.process_flag = 'C'
       and xds.batch_id is not null
       and xds.run_sequence_id is not null;
    -- Get Total Count of Converted Records lines
    select count(1)
      into l_suc_count_line
      from xxpo_po_line_shipment_stg xds
     where xds.batch_id = nvl(g_batch_id, xds.batch_id)
       and xds.process_flag = 'C'
       and xds.batch_id is not null
       and xds.run_sequence_id is not null;
    -- Get Total Count of Converted Records distribution
    select count(1)
      into l_suc_count_dist
      from xxpo_po_distribution_stg xds
     where xds.batch_id = nvl(g_batch_id, xds.batch_id)
       and xds.process_flag = 'C'
       and xds.batch_id is not null
       and xds.run_sequence_id is not null;
    xxetn_debug_pkg.add_debug(piv_debug_msg => 'Inside Print_report procedure');
    fnd_file.put_line(fnd_file.output,
                      'Program Name : Eaton Purchase Order Conversion Program');
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
                      'Total Records Lines              : ' ||
                      l_tot_line_count);
    fnd_file.put_line(fnd_file.output,
                      'Total Records distribution       : ' ||
                      l_tot_dist_count);
    fnd_file.put_line(fnd_file.output,
                      'Records Converted Header         : ' ||
                      l_suc_count_head);
    fnd_file.put_line(fnd_file.output,
                      'Records Converted Lines          : ' ||
                      l_suc_count_line);
    fnd_file.put_line(fnd_file.output,
                      'Records Converted distribution          : ' ||
                      l_suc_count_dist);
    fnd_file.put_line(fnd_file.output,
                      'Records Erred in Validation for Header : ' ||
                      l_fail_header_count);
    fnd_file.put_line(fnd_file.output,
                      'Records Erred in Validation for Lines  : ' ||
                      l_fail_line_count);
    fnd_file.put_line(fnd_file.output,
                      'Records Erred in Validation for distribution  : ' ||
                      l_fail_dist_count);
    fnd_file.put_line(fnd_file.output,
                      'Records Erred in Conversion for Header : ' ||
                      l_fail_count_head);
    fnd_file.put_line(fnd_file.output,
                      'Records Erred in Conversion for Lines  : ' ||
                      l_fail_count_line);
    fnd_file.put_line(fnd_file.output,
                      'Records Erred in Conversion for distribution  : ' ||
                      l_fail_count_dist);
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
      interface_txn_id             xxpo_po_header_ext_r12.interface_txn_id%type,
      leg_source_system            xxpo_po_header_ext_r12.leg_source_system%type,
      leg_po_header_id             xxpo_po_header_ext_r12.leg_po_header_id%type,
      leg_operating_unit_name      xxpo_po_header_ext_r12.leg_operating_unit_name%type,
      leg_org_id                   xxpo_po_header_ext_r12.leg_org_id%type,
      operating_unit_name          xxpo_po_header_ext_r12.operating_unit_name%type,
      org_id                       xxpo_po_header_ext_r12.org_id%type,
      leg_document_type_code       xxpo_po_header_ext_r12.leg_document_type_code%type,
      leg_document_num             xxpo_po_header_ext_r12.leg_document_num%type,
      leg_currency_code            xxpo_po_header_ext_r12.leg_currency_code%type,
      leg_rate_type                xxpo_po_header_ext_r12.leg_rate_type%type,
      leg_rate_date                xxpo_po_header_ext_r12.leg_rate_date%type,
      leg_rate                     xxpo_po_header_ext_r12.leg_rate%type,
      leg_agent_emp_no             xxpo_po_header_ext_r12.leg_agent_emp_no%type,
      leg_agent_id                 xxpo_po_header_ext_r12.leg_agent_id%type,
      agent_id                     xxpo_po_header_ext_r12.agent_id%type,
      leg_vendor_num               xxpo_po_header_ext_r12.leg_vendor_num%type,
      leg_vendor_id                xxpo_po_header_ext_r12.leg_vendor_id%type,
      vendor_id                    xxpo_po_header_ext_r12.vendor_id%type,
      leg_vendor_site_code         xxpo_po_header_ext_r12.leg_vendor_site_code%type,
      leg_vendor_site_id           xxpo_po_header_ext_r12.leg_vendor_site_id%type,
      vendor_site_id               xxpo_po_header_ext_r12.vendor_site_id%type,
      leg_vendor_contact_fname     xxpo_po_header_ext_r12.leg_vendor_contact_fname%type,
      leg_vendor_contact_lname     xxpo_po_header_ext_r12.leg_vendor_contact_lname%type,
      leg_vendor_contact_id        xxpo_po_header_ext_r12.leg_vendor_contact_id%type,
      vendor_contact_id            xxpo_po_header_ext_r12.vendor_contact_id%type,
      leg_ship_to_location         xxpo_po_header_ext_r12.leg_ship_to_location%type, --V1.16
      leg_ship_to_location_id      xxpo_po_header_ext_r12.leg_ship_to_location_id%type,
      ship_to_location_id          xxpo_po_header_ext_r12.ship_to_location_id%type,
      leg_bill_to_location         xxpo_po_header_ext_r12.leg_bill_to_location%type, --V1.16
      leg_bill_to_location_id      xxpo_po_header_ext_r12.leg_bill_to_location_id%type,
      bill_to_location_id          xxpo_po_header_ext_r12.bill_to_location_id%type,
      leg_payment_terms            xxpo_po_header_ext_r12.leg_payment_terms%type,
      leg_terms_id                 xxpo_po_header_ext_r12.leg_terms_id%type,
      payment_terms                xxpo_po_header_ext_r12.payment_terms%type,
      terms_id                     xxpo_po_header_ext_r12.terms_id%type,
      leg_freight_carrier          xxpo_po_header_ext_r12.leg_freight_carrier%type,
      leg_fob                      xxpo_po_header_ext_r12.leg_fob%type,
      leg_freight_terms            xxpo_po_header_ext_r12.leg_freight_terms%type,
      leg_note_to_vendor           xxpo_po_header_ext_r12.leg_note_to_vendor%type,
      leg_note_to_receiver         xxpo_po_header_ext_r12.leg_note_to_receiver%type,
      leg_confirming_order_flag    xxpo_po_header_ext_r12.leg_confirming_order_flag%type,
      leg_comments                 xxpo_po_header_ext_r12.leg_comments%type,
      leg_acceptance_required_flag xxpo_po_header_ext_r12.leg_acceptance_required_flag%type,
      leg_acceptance_due_date      xxpo_po_header_ext_r12.leg_acceptance_due_date%type,
      leg_attribute_category       xxpo_po_header_ext_r12.leg_attribute_category%type,
      leg_attribute1               xxpo_po_header_ext_r12.leg_attribute1%type,
      leg_attribute2               xxpo_po_header_ext_r12.leg_attribute2%type,
      leg_attribute3               xxpo_po_header_ext_r12.leg_attribute3%type,
      leg_attribute4               xxpo_po_header_ext_r12.leg_attribute4%type,
      leg_attribute5               xxpo_po_header_ext_r12.leg_attribute5%type,
      leg_attribute6               xxpo_po_header_ext_r12.leg_attribute6%type,
      leg_attribute7               xxpo_po_header_ext_r12.leg_attribute7%type,
      leg_attribute8               xxpo_po_header_ext_r12.leg_attribute8%type,
      leg_attribute9               xxpo_po_header_ext_r12.leg_attribute9%type,
      leg_attribute10              xxpo_po_header_ext_r12.leg_attribute10%type,
      leg_attribute11              xxpo_po_header_ext_r12.leg_attribute11%type,
      leg_attribute12              xxpo_po_header_ext_r12.leg_attribute12%type,
      leg_attribute13              xxpo_po_header_ext_r12.leg_attribute13%type,
      leg_attribute14              xxpo_po_header_ext_r12.leg_attribute14%type,
      leg_attribute15              xxpo_po_header_ext_r12.leg_attribute15%type,
      attribute_category           xxpo_po_header_ext_r12.attribute_category%type,
      attribute1                   xxpo_po_header_ext_r12.attribute1%type,
      attribute2                   xxpo_po_header_ext_r12.attribute2%type,
      attribute3                   xxpo_po_header_ext_r12.attribute3%type,
      attribute4                   xxpo_po_header_ext_r12.attribute4%type,
      attribute5                   xxpo_po_header_ext_r12.attribute5%type,
      attribute6                   xxpo_po_header_ext_r12.attribute6%type,
      attribute7                   xxpo_po_header_ext_r12.attribute7%type,
      attribute8                   xxpo_po_header_ext_r12.attribute8%type,
      attribute9                   xxpo_po_header_ext_r12.attribute9%type,
      attribute10                  xxpo_po_header_ext_r12.attribute10%type,
      attribute11                  xxpo_po_header_ext_r12.attribute11%type,
      attribute12                  xxpo_po_header_ext_r12.attribute12%type,
      attribute13                  xxpo_po_header_ext_r12.attribute13%type,
      attribute14                  xxpo_po_header_ext_r12.attribute14%type,
      attribute15                  xxpo_po_header_ext_r12.attribute15%type,
      leg_change_summary           xxpo_po_header_ext_r12.leg_change_summary%type,
      process_flag                 xxpo_po_header_ext_r12.process_flag%type,
      batch_id                     xxpo_po_header_ext_r12.batch_id%type,
      run_sequence_id              xxpo_po_header_ext_r12.run_sequence_id%type,
      request_id                   xxpo_po_header_ext_r12.request_id%type,
      error_type                   xxpo_po_header_ext_r12.error_type%type,
      leg_entity                   xxpo_po_header_ext_r12.leg_entity%type,
      leg_seq_num                  xxpo_po_header_ext_r12.leg_seq_num%type,
      leg_process_flag             xxpo_po_header_ext_r12.leg_process_flag%type,
      leg_request_id               xxpo_po_header_ext_r12.leg_request_id%type,
      creation_date                xxpo_po_header_ext_r12.creation_date%type,
      created_by                   xxpo_po_header_ext_r12.created_by%type,
      last_updated_date            xxpo_po_header_ext_r12.last_updated_date%type,
      last_updated_by              xxpo_po_header_ext_r12.last_updated_by%type,
      last_update_login            xxpo_po_header_ext_r12.last_update_login%type,
      program_application_id       xxpo_po_header_ext_r12.program_application_id%type,
      program_id                   xxpo_po_header_ext_r12.program_id%type,
      program_update_date          xxpo_po_header_ext_r12.program_update_date%type);
    type leg_head_tbl is table of leg_head_rec index by binary_integer;
    l_leg_head_tbl leg_head_tbl;
    l_err_record   number;
    cursor cur_leg_head is
      select xil.interface_txn_id,
             xil.leg_source_system,
             xil.leg_po_header_id,
             xil.leg_operating_unit_name,
             xil.leg_org_id,
             xil.operating_unit_name,
             xil.org_id,
             xil.leg_document_type_code,
             xil.leg_document_num,
             xil.leg_currency_code,
             --v1.26 revert the changes of 1.25
             DECODE(xil.leg_rate_type,NULL,NULL,'User'),  --    v1.24 SDP 25/03/2016
             --v1.25 starts
             /*decode(xil.leg_rate_type,
                    null,
                    null,
                    'User',
                    'User',
                    'Corporate',
                    'Corporate',
                    'Corporate'),*/
             --v1.25 ends
             --v1.26 ends
             xil.leg_rate_date,
             --v1.26 starts commented v1.25
            -- decode(xil.leg_rate_type, 'User', xil.leg_rate, null), --v1.25 ends
              xil.leg_rate,
             --v1.26 ends
             xil.leg_agent_emp_no,
             xil.leg_agent_id,
             xil.agent_id,
             xil.leg_vendor_num,
             xil.leg_vendor_id,
             xil.vendor_id,
             xil.leg_vendor_site_code,
             xil.leg_vendor_site_id,
             xil.vendor_site_id,
             xil.leg_vendor_contact_fname,
             xil.leg_vendor_contact_lname,
             xil.leg_vendor_contact_id,
             xil.vendor_contact_id,
             xil.leg_ship_to_location,
             xil.leg_ship_to_location_id,
             xil.ship_to_location_id,
             xil.leg_bill_to_location,
             xil.leg_bill_to_location_id,
             xil.bill_to_location_id,
             xil.leg_payment_terms,
             xil.leg_terms_id,
             xil.payment_terms,
             xil.terms_id,
             xil.leg_freight_carrier,
             xil.leg_fob,
             upper(xil.leg_freight_terms), --    v1.24 SDP 25/03/2016
             xil.leg_note_to_vendor,
             xil.leg_note_to_receiver,
             xil.leg_confirming_order_flag,
             xil.leg_comments,
             xil.leg_acceptance_required_flag,
             xil.leg_acceptance_due_date,
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
             xil.leg_change_summary,
             xil.process_flag,
             xil.batch_id,
             xil.run_sequence_id,
             g_request_id,
             xil.error_type,
             xil.leg_entity,
             xil.leg_seq_num,
             xil.leg_process_flag,
             xil.leg_request_id,
             sysdate,
             g_created_by,
             sysdate,
             g_last_updated_by,
             g_last_update_login,
             g_prog_appl_id,
             g_conc_program_id,
             sysdate
        from xxpo_po_header_ext_r12 xil
       where xil.leg_process_flag = 'V'
         and not exists
       (select 1
                from xxpo_po_header_stg xis
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
          insert into xxpo_po_header_stg values l_leg_head_tbl (indx);
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
            update xxpo_po_header_ext_r12 xil
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
      update xxpo_po_header_ext_r12 xil
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
                from xxpo_po_header_stg xis
               where xis.interface_txn_id = xil.interface_txn_id);
      commit;
      -- Either no data to load from extraction table or records already exist in R12 staging table and hence not loaded
    else
      print_log_message('Either no data found for loading from extraction table or records already exist in R12 staging table and hence not loaded ');
      update xxpo_po_header_ext_r12 xil
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
                from xxpo_po_header_stg xis
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
  -- Procedure: LOAD_LINE
  -- =============================================================================
  --   This procedure is used to load data from extraction into staging table
  -- =============================================================================
  procedure load_line(pov_ret_stats out nocopy varchar2,
                      pov_err_msg   out nocopy varchar2) is
    type leg_line_rec is record(
      interface_txn_id               xxpo_po_line_shipment_ext_r12.interface_txn_id%type,
      leg_source_system              xxpo_po_line_shipment_ext_r12.leg_source_system%type,
      leg_po_header_id               xxpo_po_line_shipment_ext_r12.leg_po_header_id%type,
      leg_document_num               xxpo_po_line_shipment_ext_r12.leg_document_num%type,
      leg_po_line_id                 xxpo_po_line_shipment_ext_r12.leg_po_line_id%type,
      leg_line_num                   xxpo_po_line_shipment_ext_r12.leg_line_num%type,
      leg_po_line_location_id        xxpo_po_line_shipment_ext_r12.leg_po_line_location_id%type,
      leg_shipment_num               xxpo_po_line_shipment_ext_r12.leg_shipment_num%type,
      leg_shipment_type              xxpo_po_line_shipment_ext_r12.leg_shipment_type%type,
      leg_line_type                  xxpo_po_line_shipment_ext_r12.leg_line_type%type,
      line_id                        xxpo_po_line_shipment_ext_r12.line_id%type,
      leg_operating_unit_name        xxpo_po_line_shipment_ext_r12.leg_operating_unit_name%type,
      leg_org_id                     xxpo_po_line_shipment_ext_r12.leg_org_id%type,
      operating_unit_name            xxpo_po_line_shipment_ext_r12.operating_unit_name%type,
      org_id                         xxpo_po_line_shipment_ext_r12.org_id%type,
      leg_item                       xxpo_po_line_shipment_ext_r12.leg_item%type,
      item_id                        xxpo_po_line_shipment_ext_r12.item_id%type,
      leg_item_revision              xxpo_po_line_shipment_ext_r12.leg_item_revision%type,
      leg_category_segment1          xxpo_po_line_shipment_ext_r12.leg_category_segment1%type,
      leg_category_segment2          xxpo_po_line_shipment_ext_r12.leg_category_segment2%type,
      leg_category_id                xxpo_po_line_shipment_ext_r12.leg_category_id%type,
      category_segment1              xxpo_po_line_shipment_ext_r12.category_segment1%type,
      category_segment2              xxpo_po_line_shipment_ext_r12.category_segment2%type,
      category_id                    xxpo_po_line_shipment_ext_r12.category_id%type,
      leg_item_description           xxpo_po_line_shipment_ext_r12.leg_item_description%type,
      leg_payment_terms              xxpo_po_line_shipment_ext_r12.leg_payment_terms%type,
      leg_terms_id                   xxpo_po_line_shipment_ext_r12.leg_terms_id%type,
      payment_terms                  xxpo_po_line_shipment_ext_r12.payment_terms%type,
      terms_id                       xxpo_po_line_shipment_ext_r12.terms_id%type,
      leg_vendor_product_num         xxpo_po_line_shipment_ext_r12.leg_vendor_product_num%type,
      leg_unit_of_measure            xxpo_po_line_shipment_ext_r12.leg_unit_of_measure%type,
      uom_code                       xxpo_po_line_shipment_ext_r12.uom_code%type,
      leg_quantity                   xxpo_po_line_shipment_ext_r12.leg_quantity%type,
      leg_quantity_orig              xxpo_po_line_shipment_ext_r12.leg_quantity_orig%type,
      leg_quantity_received_orig     xxpo_po_line_shipment_ext_r12.leg_quantity_received_orig%type,
      leg_quantity_accepted_orig     xxpo_po_line_shipment_ext_r12.leg_quantity_accepted_orig%type,
      leg_quantity_rejected_orig     xxpo_po_line_shipment_ext_r12.leg_quantity_rejected_orig%type,
      leg_quantity_billed_orig       xxpo_po_line_shipment_ext_r12.leg_quantity_billed_orig%type,
      leg_quantity_cancelled_orig    xxpo_po_line_shipment_ext_r12.leg_quantity_cancelled_orig%type,
      leg_unit_price                 xxpo_po_line_shipment_ext_r12.leg_unit_price%type,
      leg_list_price_per_unit        xxpo_po_line_shipment_ext_r12.leg_list_price_per_unit%type,
      leg_un_number                  xxpo_po_line_shipment_ext_r12.leg_un_number%type,
      leg_un_number_id               xxpo_po_line_shipment_ext_r12.leg_un_number_id%type,
      un_number_id                   xxpo_po_line_shipment_ext_r12.un_number_id%type,
      leg_hazard_class               xxpo_po_line_shipment_ext_r12.leg_hazard_class%type,
      leg_hazard_class_id            xxpo_po_line_shipment_ext_r12.leg_hazard_class_id%type,
      hazard_class_id                xxpo_po_line_shipment_ext_r12.hazard_class_id%type,
      leg_note_to_vendor             xxpo_po_line_shipment_ext_r12.leg_note_to_vendor%type,
      leg_transaction_reason_code    xxpo_po_line_shipment_ext_r12.leg_transaction_reason_code%type,
      leg_taxable_flag               xxpo_po_line_shipment_ext_r12.leg_taxable_flag%type,
      leg_tax_name                   xxpo_po_line_shipment_ext_r12.leg_tax_name%type,
      tax_name                       xxpo_po_line_shipment_ext_r12.tax_name%type,
      tax_code_id                    xxpo_po_line_shipment_ext_r12.tax_code_id%type,
      leg_tax_user_override_flag     xxpo_po_line_shipment_ext_r12.leg_tax_user_override_flag%type,
      leg_inspection_required_flag   xxpo_po_line_shipment_ext_r12.leg_inspection_required_flag%type,
      leg_receipt_required_flag      xxpo_po_line_shipment_ext_r12.leg_receipt_required_flag%type,
      leg_price_type                 xxpo_po_line_shipment_ext_r12.leg_price_type%type,
      leg_invoice_close_tolerance    xxpo_po_line_shipment_ext_r12.leg_invoice_close_tolerance%type,
      leg_receive_close_tolerance    xxpo_po_line_shipment_ext_r12.leg_receive_close_tolerance%type,
      leg_days_early_receipt_allowed xxpo_po_line_shipment_ext_r12.leg_days_early_receipt_allowed%type,
      leg_days_late_receipt_allowed  xxpo_po_line_shipment_ext_r12.leg_days_late_receipt_allowed%type,
      leg_receiving_routing          xxpo_po_line_shipment_ext_r12.leg_receiving_routing%type,
      leg_receiving_routing_id       xxpo_po_line_shipment_ext_r12.leg_receiving_routing_id%type,
      receiving_routing_id           xxpo_po_line_shipment_ext_r12.receiving_routing_id%type,
      leg_qty_rcv_tolerance          xxpo_po_line_shipment_ext_r12.leg_qty_rcv_tolerance%type,
      leg_ship_to_organization_name  xxpo_po_line_shipment_ext_r12.leg_ship_to_organization_name%type,
      leg_ship_to_organization_id    xxpo_po_line_shipment_ext_r12.leg_ship_to_organization_id%type,
      ship_to_organization_id        xxpo_po_line_shipment_ext_r12.ship_to_organization_id%type,
      leg_ship_to_location           xxpo_po_line_shipment_ext_r12.leg_ship_to_location%type,
      leg_ship_to_location_id        xxpo_po_line_shipment_ext_r12.leg_ship_to_location_id%type,
      ship_to_location_id            xxpo_po_line_shipment_ext_r12.ship_to_location_id%type,
      leg_need_by_date               xxpo_po_line_shipment_ext_r12.leg_need_by_date%type,
      leg_promised_date              xxpo_po_line_shipment_ext_r12.leg_promised_date%type,
      leg_accrue_on_receipt_flag     xxpo_po_line_shipment_ext_r12.leg_accrue_on_receipt_flag%type,
      leg_line_attr_category_lines   xxpo_po_line_shipment_ext_r12.leg_line_attr_category_lines%type,
      leg_line_attribute1            xxpo_po_line_shipment_ext_r12.leg_line_attribute1%type,
      leg_line_attribute2            xxpo_po_line_shipment_ext_r12.leg_line_attribute2%type,
      leg_line_attribute3            xxpo_po_line_shipment_ext_r12.leg_line_attribute3%type,
      leg_line_attribute4            xxpo_po_line_shipment_ext_r12.leg_line_attribute4%type,
      leg_line_attribute5            xxpo_po_line_shipment_ext_r12.leg_line_attribute5%type,
      leg_line_attribute6            xxpo_po_line_shipment_ext_r12.leg_line_attribute6%type,
      leg_line_attribute7            xxpo_po_line_shipment_ext_r12.leg_line_attribute7%type,
      leg_line_attribute8            xxpo_po_line_shipment_ext_r12.leg_line_attribute8%type,
      leg_line_attribute9            xxpo_po_line_shipment_ext_r12.leg_line_attribute9%type,
      leg_line_attribute10           xxpo_po_line_shipment_ext_r12.leg_line_attribute10%type,
      leg_line_attribute11           xxpo_po_line_shipment_ext_r12.leg_line_attribute11%type,
      leg_line_attribute12           xxpo_po_line_shipment_ext_r12.leg_line_attribute12%type,
      leg_line_attribute13           xxpo_po_line_shipment_ext_r12.leg_line_attribute13%type,
      leg_line_attribute14           xxpo_po_line_shipment_ext_r12.leg_line_attribute14%type,
      leg_line_attribute15           xxpo_po_line_shipment_ext_r12.leg_line_attribute15%type,
      line_attr_category_lines       xxpo_po_line_shipment_ext_r12.line_attr_category_lines%type,
      line_attribute1                xxpo_po_line_shipment_ext_r12.line_attribute1%type,
      line_attribute2                xxpo_po_line_shipment_ext_r12.line_attribute2%type,
      line_attribute3                xxpo_po_line_shipment_ext_r12.line_attribute3%type,
      line_attribute4                xxpo_po_line_shipment_ext_r12.line_attribute4%type,
      line_attribute5                xxpo_po_line_shipment_ext_r12.line_attribute5%type,
      line_attribute6                xxpo_po_line_shipment_ext_r12.line_attribute6%type,
      line_attribute7                xxpo_po_line_shipment_ext_r12.line_attribute7%type,
      line_attribute8                xxpo_po_line_shipment_ext_r12.line_attribute8%type,
      line_attribute9                xxpo_po_line_shipment_ext_r12.line_attribute9%type,
      line_attribute10               xxpo_po_line_shipment_ext_r12.line_attribute10%type,
      line_attribute11               xxpo_po_line_shipment_ext_r12.line_attribute11%type,
      line_attribute12               xxpo_po_line_shipment_ext_r12.line_attribute12%type,
      line_attribute13               xxpo_po_line_shipment_ext_r12.line_attribute13%type,
      line_attribute14               xxpo_po_line_shipment_ext_r12.line_attribute14%type,
      line_attribute15               xxpo_po_line_shipment_ext_r12.line_attribute15%type,
      leg_shipment_attr_category     xxpo_po_line_shipment_ext_r12.leg_shipment_attr_category%type,
      leg_shipment_attribute1        xxpo_po_line_shipment_ext_r12.leg_shipment_attribute1%type,
      leg_shipment_attribute2        xxpo_po_line_shipment_ext_r12.leg_shipment_attribute2%type,
      leg_shipment_attribute3        xxpo_po_line_shipment_ext_r12.leg_shipment_attribute3%type,
      leg_shipment_attribute4        xxpo_po_line_shipment_ext_r12.leg_shipment_attribute4%type,
      leg_shipment_attribute5        xxpo_po_line_shipment_ext_r12.leg_shipment_attribute5%type,
      leg_shipment_attribute6        xxpo_po_line_shipment_ext_r12.leg_shipment_attribute6%type,
      leg_shipment_attribute7        xxpo_po_line_shipment_ext_r12.leg_shipment_attribute7%type,
      leg_shipment_attribute8        xxpo_po_line_shipment_ext_r12.leg_shipment_attribute8%type,
      leg_shipment_attribute9        xxpo_po_line_shipment_ext_r12.leg_shipment_attribute9%type,
      leg_shipment_attribute10       xxpo_po_line_shipment_ext_r12.leg_shipment_attribute10%type,
      leg_shipment_attribute11       xxpo_po_line_shipment_ext_r12.leg_shipment_attribute11%type,
      leg_shipment_attribute12       xxpo_po_line_shipment_ext_r12.leg_shipment_attribute12%type,
      leg_shipment_attribute13       xxpo_po_line_shipment_ext_r12.leg_shipment_attribute13%type,
      leg_shipment_attribute14       xxpo_po_line_shipment_ext_r12.leg_shipment_attribute14%type,
      leg_shipment_attribute15       xxpo_po_line_shipment_ext_r12.leg_shipment_attribute15%type,
      shipment_attr_category         xxpo_po_line_shipment_ext_r12.shipment_attr_category%type,
      shipment_attribute1            xxpo_po_line_shipment_ext_r12.shipment_attribute1%type,
      shipment_attribute2            xxpo_po_line_shipment_ext_r12.shipment_attribute2%type,
      shipment_attribute3            xxpo_po_line_shipment_ext_r12.shipment_attribute3%type,
      shipment_attribute4            xxpo_po_line_shipment_ext_r12.shipment_attribute4%type,
      shipment_attribute5            xxpo_po_line_shipment_ext_r12.shipment_attribute5%type,
      shipment_attribute6            xxpo_po_line_shipment_ext_r12.shipment_attribute6%type,
      shipment_attribute7            xxpo_po_line_shipment_ext_r12.shipment_attribute7%type,
      shipment_attribute8            xxpo_po_line_shipment_ext_r12.shipment_attribute8%type,
      shipment_attribute9            xxpo_po_line_shipment_ext_r12.shipment_attribute9%type,
      shipment_attribute10           xxpo_po_line_shipment_ext_r12.shipment_attribute10%type,
      shipment_attribute11           xxpo_po_line_shipment_ext_r12.shipment_attribute11%type,
      shipment_attribute12           xxpo_po_line_shipment_ext_r12.shipment_attribute12%type,
      shipment_attribute13           xxpo_po_line_shipment_ext_r12.shipment_attribute13%type,
      shipment_attribute14           xxpo_po_line_shipment_ext_r12.shipment_attribute14%type,
      shipment_attribute15           xxpo_po_line_shipment_ext_r12.shipment_attribute15%type,
      leg_tax_status_indicator       xxpo_po_line_shipment_ext_r12.leg_tax_status_indicator%type,
      leg_note_to_receiver           xxpo_po_line_shipment_ext_r12.leg_note_to_receiver%type,
      leg_consigned_flag             xxpo_po_line_shipment_ext_r12.leg_consigned_flag%type,
      leg_supplier_ref_number        xxpo_po_line_shipment_ext_r12.leg_supplier_ref_number%type,
      leg_drop_ship_flag             xxpo_po_line_shipment_ext_r12.leg_drop_ship_flag%type,
      leg_closed_for_invoice         xxpo_po_line_shipment_ext_r12.leg_closed_for_invoice%type,
      process_flag                   xxpo_po_line_shipment_ext_r12.process_flag%type,
      batch_id                       xxpo_po_line_shipment_ext_r12.batch_id%type,
      run_sequence_id                xxpo_po_line_shipment_ext_r12.run_sequence_id%type,
      request_id                     xxpo_po_line_shipment_ext_r12.request_id%type,
      error_type                     xxpo_po_line_shipment_ext_r12.error_type%type,
      leg_entity                     xxpo_po_line_shipment_ext_r12.leg_entity%type,
      leg_seq_num                    xxpo_po_line_shipment_ext_r12.leg_seq_num%type,
      leg_process_flag               xxpo_po_line_shipment_ext_r12.leg_process_flag%type,
      leg_request_id                 xxpo_po_line_shipment_ext_r12.leg_request_id%type,
      creation_date                  xxpo_po_line_shipment_ext_r12.creation_date%type,
      created_by                     xxpo_po_line_shipment_ext_r12.created_by%type,
      last_updated_date              xxpo_po_line_shipment_ext_r12.last_updated_date%type,
      last_updated_by                xxpo_po_line_shipment_ext_r12.last_updated_by%type,
      last_update_login              xxpo_po_line_shipment_ext_r12.last_update_login%type,
      program_application_id         xxpo_po_line_shipment_ext_r12.program_application_id%type,
      program_id                     xxpo_po_line_shipment_ext_r12.program_id%type,
      program_update_date            xxpo_po_line_shipment_ext_r12.program_update_date%type,
      leg_req_dist_id                xxpo_po_line_shipment_ext_r12.leg_req_dist_id%type, --- 1.20
      leg_match_option               xxpo_po_line_shipment_ext_r12.leg_match_option%type -- v1.24 SDP -- 25/03/2016
      );
    type leg_line_tbl is table of leg_line_rec index by binary_integer;
    l_leg_line_tbl leg_line_tbl;
    l_err_record   number;
    cursor cur_leg_line is
      select xil.interface_txn_id,
             xil.leg_source_system,
             xil.leg_po_header_id,
             xil.leg_document_num,
             xil.leg_po_line_id,
             xil.leg_line_num,
             xil.leg_po_line_location_id,
             xil.leg_shipment_num,
             xil.leg_shipment_type,
             decode(xil.leg_line_type,
                    'Services',
                    'Service-qty',
                    xil.leg_line_type), -- v1.24 SDP -- 25/03/2016
             xil.line_id,
             xil.leg_operating_unit_name,
             xil.leg_org_id,
             xil.operating_unit_name,
             xil.org_id,
             xil.leg_item,
             xil.item_id,
             xil.leg_item_revision,
             decode(xil.leg_category_segment1, null, null, 'DEFAULT'), --v1.14 CR # 331031 Category DEFAULT for Conversion PO's
             xil.leg_category_segment2,
             xil.leg_category_id,
             xil.category_segment1,
             xil.category_segment2,
             xil.category_id,
             xil.leg_item_description,
             xil.leg_payment_terms,
             xil.leg_terms_id,
             xil.payment_terms,
             xil.terms_id,
             xil.leg_vendor_product_num,
             xil.leg_unit_of_measure,
             xil.uom_code,
             xil.leg_quantity,
             xil.leg_quantity_orig,
             xil.leg_quantity_received_orig,
             xil.leg_quantity_accepted_orig,
             xil.leg_quantity_rejected_orig,
             xil.leg_quantity_billed_orig,
             xil.leg_quantity_cancelled_orig,
             xil.leg_unit_price,
             xil.leg_list_price_per_unit,
             xil.leg_un_number,
             xil.leg_un_number_id,
             xil.un_number_id,
             xil.leg_hazard_class,
             xil.leg_hazard_class_id,
             xil.hazard_class_id,
             xil.leg_note_to_vendor,
             xil.leg_transaction_reason_code,
             xil.leg_taxable_flag,
             xil.leg_tax_name,
             xil.tax_name,
             xil.tax_code_id,
             xil.leg_tax_user_override_flag,
             xil.leg_inspection_required_flag,
             xil.leg_receipt_required_flag,
             xil.leg_price_type,
             xil.leg_invoice_close_tolerance,
             xil.leg_receive_close_tolerance,
             xil.leg_days_early_receipt_allowed,
             xil.leg_days_late_receipt_allowed,
             xil.leg_receiving_routing,
             xil.leg_receiving_routing_id,
             xil.receiving_routing_id,
             xil.leg_qty_rcv_tolerance,
             xil.leg_ship_to_organization_name,
             xil.leg_ship_to_organization_id,
             xil.ship_to_organization_id,
             xil.leg_ship_to_location,
             xil.leg_ship_to_location_id,
             xil.ship_to_location_id,
             xil.leg_need_by_date,
             xil.leg_promised_date,
             xil.leg_accrue_on_receipt_flag,
             xil.leg_line_attr_category_lines,
             xil.leg_line_attribute1,
             xil.leg_line_attribute2,
             xil.leg_line_attribute3,
             xil.leg_line_attribute4,
             xil.leg_line_attribute5,
             xil.leg_line_attribute6,
             xil.leg_line_attribute7,
             xil.leg_line_attribute8,
             xil.leg_line_attribute9,
             xil.leg_line_attribute10,
             xil.leg_line_attribute11,
             xil.leg_line_attribute12,
             xil.leg_line_attribute13,
             xil.leg_line_attribute14,
             xil.leg_line_attribute15,
             xil.line_attr_category_lines,
             xil.line_attribute1,
             xil.line_attribute2,
             xil.line_attribute3,
             xil.line_attribute4,
             xil.line_attribute5,
             xil.line_attribute6,
             xil.line_attribute7,
             xil.line_attribute8,
             xil.line_attribute9,
             xil.line_attribute10,
             xil.line_attribute11,
             xil.line_attribute12,
             xil.line_attribute13,
             xil.line_attribute14,
             xil.line_attribute15,
             xil.leg_shipment_attr_category,
             xil.leg_shipment_attribute1,
             xil.leg_shipment_attribute2,
             xil.leg_shipment_attribute3,
             xil.leg_shipment_attribute4,
             xil.leg_shipment_attribute5,
             xil.leg_shipment_attribute6,
             xil.leg_shipment_attribute7,
             xil.leg_shipment_attribute8,
             xil.leg_shipment_attribute9,
             xil.leg_shipment_attribute10,
             xil.leg_shipment_attribute11,
             xil.leg_shipment_attribute12,
             xil.leg_shipment_attribute13,
             xil.leg_shipment_attribute14,
             xil.leg_shipment_attribute15,
             xil.shipment_attr_category,
             xil.shipment_attribute1,
             xil.shipment_attribute2,
             xil.shipment_attribute3,
             xil.shipment_attribute4,
             xil.shipment_attribute5,
             xil.shipment_attribute6,
             xil.shipment_attribute7,
             xil.shipment_attribute8,
             xil.shipment_attribute9,
             xil.shipment_attribute10,
             xil.shipment_attribute11,
             xil.shipment_attribute12,
             xil.shipment_attribute13,
             xil.shipment_attribute14,
             xil.shipment_attribute15,
             xil.leg_tax_status_indicator,
             xil.leg_note_to_receiver,
             xil.leg_consigned_flag,
             xil.leg_supplier_ref_number,
             xil.leg_drop_ship_flag,
             xil.leg_closed_for_invoice,
             xil.process_flag,
             xil.batch_id,
             xil.run_sequence_id,
             g_request_id,
             xil.error_type,
             xil.leg_entity,
             xil.leg_seq_num,
             xil.leg_process_flag,
             xil.leg_request_id,
             sysdate,
             g_created_by,
             sysdate,
             g_last_updated_by,
             g_last_update_login,
             g_prog_appl_id,
             g_conc_program_id,
             sysdate,
             leg_req_dist_id, ----1.20
             leg_match_option -- v1.24 SDP -- 25/03/2016
        from xxpo_po_line_shipment_ext_r12 xil
       where xil.leg_process_flag = 'V'
         and not exists
       (select 1
                from xxpo_po_line_shipment_stg xis
               where xis.interface_txn_id = xil.interface_txn_id);
  begin
    pov_ret_stats  := 'S';
    pov_err_msg    := null;
    g_total_count  := 0;
    g_failed_count := 0;
    g_loaded_count := 0;
    --Open cursor to extract data from extraction staging table
    open cur_leg_line;
    loop
      xxetn_debug_pkg.add_debug(piv_debug_msg => 'Loading lines');
      l_leg_line_tbl.delete;
      fetch cur_leg_line bulk collect
        into l_leg_line_tbl limit 5000;
      --limit size of Bulk Collect
      -- Get Total Count
      g_total_count := g_total_count + l_leg_line_tbl.count;
      exit when l_leg_line_tbl.count = 0;
      begin
        -- Bulk Insert into Conversion table
        forall indx in 1 .. l_leg_line_tbl.count save exceptions
          insert into xxpo_po_line_shipment_stg
          values l_leg_line_tbl
            (indx);
      exception
        when others then
          print_log_message('Errors encountered while loading line data ');
          for l_indx_exp in 1 .. sql%bulk_exceptions.count loop
            l_err_record  := l_leg_line_tbl(sql%bulk_exceptions(l_indx_exp).error_index)
                             .interface_txn_id;
            pov_ret_stats := 'E';
            print_log_message('Record sequence (interface_txn_id) : ' || l_leg_line_tbl(sql%bulk_exceptions(l_indx_exp).error_index)
                              .interface_txn_id);
            print_log_message('Error Message : ' ||
                              sqlerrm(-sql%bulk_exceptions(l_indx_exp)
                                      .error_code));
            -- Updating Leg_process_flag to 'E' for failed records
            update xxpo_po_line_shipment_ext_r12 xil
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
    close cur_leg_line;
    commit;
    if g_failed_count > 0 then
      print_log_message('Number of Failed Records during load of lines : ' ||
                        g_failed_count);
    end if;
    ---output
    g_loaded_count := g_total_count - g_failed_count;
    fnd_file.put_line(fnd_file.output, ' Stats for lines table load ');
    fnd_file.put_line(fnd_file.output, '================================');
    fnd_file.put_line(fnd_file.output, 'Total Count : ' || g_total_count);
    fnd_file.put_line(fnd_file.output, 'Loaded Count: ' || g_loaded_count);
    fnd_file.put_line(fnd_file.output, 'Failed Count: ' || g_failed_count);
    fnd_file.put_line(fnd_file.output, '================================');
    -- If records successfully posted to conversion staging table
    if g_total_count > 0 then
      print_log_message('Updating process flag (leg_process_flag) in extraction table for processed records ');
      update xxpo_po_line_shipment_ext_r12 xil
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
                from xxpo_po_line_shipment_stg xis
               where xis.interface_txn_id = xil.interface_txn_id);
      commit;
      -- Either no data to load from extraction table or records already exist in R12 staging table and hence not loaded
    else
      print_log_message('Either no data found for loading from extraction table or records already exist in R12 staging table and hence not loaded ');
      update xxpo_po_line_shipment_ext_r12 xil
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
                from xxpo_po_line_shipment_stg xis
               where xis.interface_txn_id = xil.interface_txn_id);
      commit;
    end if;
    -- CR 372677 change tolerance if more than 100 --    v1.24 SDP 25/03/2016
    update xxpo_po_line_shipment_stg
       set leg_qty_rcv_tolerance = 100
     where leg_qty_rcv_tolerance > 100;
    commit;
    -- CR 372677 change tolerance if more than 100 --    v1.24 SDP 25/03/2016
  exception
    when others then
      pov_ret_stats := 'E';
      pov_err_msg   := 'ERROR : Error in Load_line procedure' ||
                       substr(sqlerrm, 1, 200);
      rollback;
  end load_line;

  --
  -- ========================
  -- Procedure: LOAD_DISTRIBUTION
  -- =============================================================================
  --   This procedure is used to load data from extraction into staging table
  -- =============================================================================
  procedure load_distribution(pov_ret_stats out nocopy varchar2,
                              pov_err_msg   out nocopy varchar2) is
    type leg_distribution_rec is record(
      interface_txn_id               xxpo_po_distribution_ext_r12.interface_txn_id%type,
      leg_source_system              xxpo_po_distribution_ext_r12.leg_source_system%type,
      leg_po_header_id               xxpo_po_distribution_ext_r12.leg_po_header_id%type,
      leg_document_num               xxpo_po_distribution_ext_r12.leg_document_num%type,
      leg_po_line_id                 xxpo_po_distribution_ext_r12.leg_po_line_id%type,
      leg_line_num                   xxpo_po_distribution_ext_r12.leg_line_num%type,
      leg_po_line_location_id        xxpo_po_distribution_ext_r12.leg_po_line_location_id%type,
      leg_shipment_num               xxpo_po_distribution_ext_r12.leg_shipment_num%type,
      leg_po_distribution_id         xxpo_po_distribution_ext_r12.leg_po_distribution_id%type,
      leg_distribution_num           xxpo_po_distribution_ext_r12.leg_distribution_num%type,
      leg_operating_unit_name        xxpo_po_distribution_ext_r12.leg_operating_unit_name%type,
      leg_org_id                     xxpo_po_distribution_ext_r12.leg_org_id%type,
      operating_unit_name            xxpo_po_distribution_ext_r12.operating_unit_name%type,
      org_id                         xxpo_po_distribution_ext_r12.org_id%type,
      leg_quantity_ordered           xxpo_po_distribution_ext_r12.leg_quantity_ordered%type,
      leg_quantity_delivered         xxpo_po_distribution_ext_r12.leg_quantity_delivered%type,
      leg_quantity_billed            xxpo_po_distribution_ext_r12.leg_quantity_billed%type,
      leg_quantity_cancelled         xxpo_po_distribution_ext_r12.leg_quantity_cancelled%type,
      leg_rate_date                  xxpo_po_distribution_ext_r12.leg_rate_date%type,
      leg_rate                       xxpo_po_distribution_ext_r12.leg_rate%type,
      leg_deliver_to_location        xxpo_po_distribution_ext_r12.leg_deliver_to_location%type, --V1.16
      leg_deliver_to_location_id     xxpo_po_distribution_ext_r12.leg_deliver_to_location_id%type,
      deliver_to_location_id         xxpo_po_distribution_ext_r12.deliver_to_location_id%type,
      leg_deliver_to_person_emp_no   xxpo_po_distribution_ext_r12.leg_deliver_to_person_emp_no%type,
      leg_deliver_to_person_id       xxpo_po_distribution_ext_r12.leg_deliver_to_person_id%type,
      deliver_to_person_id           xxpo_po_distribution_ext_r12.deliver_to_person_id%type,
      leg_destination_type_code      xxpo_po_distribution_ext_r12.leg_destination_type_code%type,
      leg_destination_organization   xxpo_po_distribution_ext_r12.leg_destination_organization%type,
      leg_dest_organization_id       xxpo_po_distribution_ext_r12.leg_dest_organization_id%type,
      destination_organization_id    xxpo_po_distribution_ext_r12.destination_organization_id%type,
      leg_destination_subinventory   xxpo_po_distribution_ext_r12.leg_destination_subinventory%type,
      leg_set_of_books               xxpo_po_distribution_ext_r12.leg_set_of_books%type,
      leg_set_of_books_id            xxpo_po_distribution_ext_r12.leg_set_of_books_id%type,
      set_of_books_id                xxpo_po_distribution_ext_r12.set_of_books_id%type,
      leg_charge_account_seg1        xxpo_po_distribution_ext_r12.leg_charge_account_seg1%type,
      leg_charge_account_seg2        xxpo_po_distribution_ext_r12.leg_charge_account_seg2%type,
      leg_charge_account_seg3        xxpo_po_distribution_ext_r12.leg_charge_account_seg3%type,
      leg_charge_account_seg4        xxpo_po_distribution_ext_r12.leg_charge_account_seg4%type,
      leg_charge_account_seg5        xxpo_po_distribution_ext_r12.leg_charge_account_seg5%type,
      leg_charge_account_seg6        xxpo_po_distribution_ext_r12.leg_charge_account_seg6%type,
      leg_charge_account_seg7        xxpo_po_distribution_ext_r12.leg_charge_account_seg7%type,
      leg_charge_account_seg8        xxpo_po_distribution_ext_r12.leg_charge_account_seg8%type,
      leg_charge_account_seg9        xxpo_po_distribution_ext_r12.leg_charge_account_seg9%type,
      leg_charge_account_seg10       xxpo_po_distribution_ext_r12.leg_charge_account_seg10%type,
      leg_charge_account_ccid        xxpo_po_distribution_ext_r12.leg_charge_account_ccid%type,
      charge_account_seg1            xxpo_po_distribution_ext_r12.charge_account_seg1%type,
      charge_account_seg2            xxpo_po_distribution_ext_r12.charge_account_seg2%type,
      charge_account_seg3            xxpo_po_distribution_ext_r12.charge_account_seg3%type,
      charge_account_seg4            xxpo_po_distribution_ext_r12.charge_account_seg4%type,
      charge_account_seg5            xxpo_po_distribution_ext_r12.charge_account_seg5%type,
      charge_account_seg6            xxpo_po_distribution_ext_r12.charge_account_seg6%type,
      charge_account_seg7            xxpo_po_distribution_ext_r12.charge_account_seg7%type,
      charge_account_seg8            xxpo_po_distribution_ext_r12.charge_account_seg8%type,
      charge_account_seg9            xxpo_po_distribution_ext_r12.charge_account_seg9%type,
      charge_account_seg10           xxpo_po_distribution_ext_r12.charge_account_seg10%type,
      charge_account_ccid            xxpo_po_distribution_ext_r12.charge_account_ccid%type,
      leg_accural_account_seg1       xxpo_po_distribution_ext_r12.leg_accural_account_seg1%type,
      leg_accural_account_seg2       xxpo_po_distribution_ext_r12.leg_accural_account_seg2%type,
      leg_accural_account_seg3       xxpo_po_distribution_ext_r12.leg_accural_account_seg3%type,
      leg_accural_account_seg4       xxpo_po_distribution_ext_r12.leg_accural_account_seg4%type,
      leg_accural_account_seg5       xxpo_po_distribution_ext_r12.leg_accural_account_seg5%type,
      leg_accural_account_seg6       xxpo_po_distribution_ext_r12.leg_accural_account_seg6%type,
      leg_accural_account_seg7       xxpo_po_distribution_ext_r12.leg_accural_account_seg7%type,
      leg_accural_account_seg8       xxpo_po_distribution_ext_r12.leg_accural_account_seg8%type,
      leg_accural_account_seg9       xxpo_po_distribution_ext_r12.leg_accural_account_seg9%type,
      leg_accural_account_seg10      xxpo_po_distribution_ext_r12.leg_accural_account_seg10%type,
      leg_accural_account_ccid       xxpo_po_distribution_ext_r12.leg_accural_account_ccid%type,
      accural_account_seg1           xxpo_po_distribution_ext_r12.accural_account_seg1%type,
      accural_account_seg2           xxpo_po_distribution_ext_r12.accural_account_seg2%type,
      accural_account_seg3           xxpo_po_distribution_ext_r12.accural_account_seg3%type,
      accural_account_seg4           xxpo_po_distribution_ext_r12.accural_account_seg4%type,
      accural_account_seg5           xxpo_po_distribution_ext_r12.accural_account_seg5%type,
      accural_account_seg6           xxpo_po_distribution_ext_r12.accural_account_seg6%type,
      accural_account_seg7           xxpo_po_distribution_ext_r12.accural_account_seg7%type,
      accural_account_seg8           xxpo_po_distribution_ext_r12.accural_account_seg8%type,
      accural_account_seg9           xxpo_po_distribution_ext_r12.accural_account_seg9%type,
      accural_account_seg10          xxpo_po_distribution_ext_r12.accural_account_seg10%type,
      accural_account_ccid           xxpo_po_distribution_ext_r12.accural_account_ccid%type,
      leg_variance_account_seg1      xxpo_po_distribution_ext_r12.leg_variance_account_seg1%type,
      leg_variance_account_seg2      xxpo_po_distribution_ext_r12.leg_variance_account_seg2%type,
      leg_variance_account_seg3      xxpo_po_distribution_ext_r12.leg_variance_account_seg3%type,
      leg_variance_account_seg4      xxpo_po_distribution_ext_r12.leg_variance_account_seg4%type,
      leg_variance_account_seg5      xxpo_po_distribution_ext_r12.leg_variance_account_seg5%type,
      leg_variance_account_seg6      xxpo_po_distribution_ext_r12.leg_variance_account_seg6%type,
      leg_variance_account_seg7      xxpo_po_distribution_ext_r12.leg_variance_account_seg7%type,
      leg_variance_account_seg8      xxpo_po_distribution_ext_r12.leg_variance_account_seg8%type,
      leg_variance_account_seg9      xxpo_po_distribution_ext_r12.leg_variance_account_seg9%type,
      leg_variance_account_seg10     xxpo_po_distribution_ext_r12.leg_variance_account_seg10%type,
      leg_variance_account_ccid      xxpo_po_distribution_ext_r12.leg_variance_account_ccid%type,
      variance_account_seg1          xxpo_po_distribution_ext_r12.variance_account_seg1%type,
      variance_account_seg2          xxpo_po_distribution_ext_r12.variance_account_seg2%type,
      variance_account_seg3          xxpo_po_distribution_ext_r12.variance_account_seg3%type,
      variance_account_seg4          xxpo_po_distribution_ext_r12.variance_account_seg4%type,
      variance_account_seg5          xxpo_po_distribution_ext_r12.variance_account_seg5%type,
      variance_account_seg6          xxpo_po_distribution_ext_r12.variance_account_seg6%type,
      variance_account_seg7          xxpo_po_distribution_ext_r12.variance_account_seg7%type,
      variance_account_seg8          xxpo_po_distribution_ext_r12.variance_account_seg8%type,
      variance_account_seg9          xxpo_po_distribution_ext_r12.variance_account_seg9%type,
      variance_account_seg10         xxpo_po_distribution_ext_r12.variance_account_seg10%type,
      variance_account_ccid          xxpo_po_distribution_ext_r12.variance_account_ccid%type,
      leg_accrued_flag               xxpo_po_distribution_ext_r12.leg_accrued_flag%type,
      leg_accrue_on_receipt_flag     xxpo_po_distribution_ext_r12.leg_accrue_on_receipt_flag%type,
      leg_project                    xxpo_po_distribution_ext_r12.leg_project%type,
      leg_project_id                 xxpo_po_distribution_ext_r12.leg_project_id%type,
      project_id                     xxpo_po_distribution_ext_r12.project_id%type,
      leg_task                       xxpo_po_distribution_ext_r12.leg_task%type,
      leg_task_id                    xxpo_po_distribution_ext_r12.leg_task_id%type,
      task_id                        xxpo_po_distribution_ext_r12.task_id%type,
      leg_expenditure_type           xxpo_po_distribution_ext_r12.leg_expenditure_type%type,
      leg_project_accounting_context xxpo_po_distribution_ext_r12.leg_project_accounting_context%type,
      leg_expenditure_organization   xxpo_po_distribution_ext_r12.leg_expenditure_organization%type,
      leg_expd_organization_id       xxpo_po_distribution_ext_r12.leg_expd_organization_id%type,
      expenditure_organization_id    xxpo_po_distribution_ext_r12.expenditure_organization_id%type,
      leg_expenditure_item_date      xxpo_po_distribution_ext_r12.leg_expenditure_item_date%type,
      leg_attribute_category         xxpo_po_distribution_ext_r12.leg_attribute_category%type,
      leg_attribute1                 xxpo_po_distribution_ext_r12.leg_attribute1%type,
      leg_attribute2                 xxpo_po_distribution_ext_r12.leg_attribute2%type,
      leg_attribute3                 xxpo_po_distribution_ext_r12.leg_attribute3%type,
      leg_attribute4                 xxpo_po_distribution_ext_r12.leg_attribute4%type,
      leg_attribute5                 xxpo_po_distribution_ext_r12.leg_attribute5%type,
      leg_attribute6                 xxpo_po_distribution_ext_r12.leg_attribute6%type,
      leg_attribute7                 xxpo_po_distribution_ext_r12.leg_attribute7%type,
      leg_attribute8                 xxpo_po_distribution_ext_r12.leg_attribute8%type,
      leg_attribute9                 xxpo_po_distribution_ext_r12.leg_attribute9%type,
      leg_attribute10                xxpo_po_distribution_ext_r12.leg_attribute10%type,
      leg_attribute11                xxpo_po_distribution_ext_r12.leg_attribute11%type,
      leg_attribute12                xxpo_po_distribution_ext_r12.leg_attribute12%type,
      leg_attribute13                xxpo_po_distribution_ext_r12.leg_attribute13%type,
      leg_attribute14                xxpo_po_distribution_ext_r12.leg_attribute14%type,
      leg_attribute15                xxpo_po_distribution_ext_r12.leg_attribute15%type,
      attribute_category             xxpo_po_distribution_ext_r12.attribute_category%type,
      attribute1                     xxpo_po_distribution_ext_r12.attribute1%type,
      attribute2                     xxpo_po_distribution_ext_r12.attribute2%type,
      attribute3                     xxpo_po_distribution_ext_r12.attribute3%type,
      attribute4                     xxpo_po_distribution_ext_r12.attribute4%type,
      attribute5                     xxpo_po_distribution_ext_r12.attribute5%type,
      attribute6                     xxpo_po_distribution_ext_r12.attribute6%type,
      attribute7                     xxpo_po_distribution_ext_r12.attribute7%type,
      attribute8                     xxpo_po_distribution_ext_r12.attribute8%type,
      attribute9                     xxpo_po_distribution_ext_r12.attribute9%type,
      attribute10                    xxpo_po_distribution_ext_r12.attribute10%type,
      attribute11                    xxpo_po_distribution_ext_r12.attribute11%type,
      attribute12                    xxpo_po_distribution_ext_r12.attribute12%type,
      attribute13                    xxpo_po_distribution_ext_r12.attribute13%type,
      attribute14                    xxpo_po_distribution_ext_r12.attribute14%type,
      attribute15                    xxpo_po_distribution_ext_r12.attribute15%type,
      leg_amount_ordered             xxpo_po_distribution_ext_r12.leg_amount_ordered%type,
      leg_unit_price                 xxpo_po_distribution_ext_r12.leg_unit_price%type,
      leg_invoice_adjustment_flag    xxpo_po_distribution_ext_r12.leg_invoice_adjustment_flag%type,
      process_flag                   xxpo_po_distribution_ext_r12.process_flag%type,
      batch_id                       xxpo_po_distribution_ext_r12.batch_id%type,
      run_sequence_id                xxpo_po_distribution_ext_r12.run_sequence_id%type,
      request_id                     xxpo_po_distribution_ext_r12.request_id%type,
      error_type                     xxpo_po_distribution_ext_r12.error_type%type,
      leg_entity                     xxpo_po_distribution_ext_r12.leg_entity%type,
      leg_seq_num                    xxpo_po_distribution_ext_r12.leg_seq_num%type,
      leg_process_flag               xxpo_po_distribution_ext_r12.leg_process_flag%type,
      leg_request_id                 xxpo_po_distribution_ext_r12.leg_request_id%type,
      creation_date                  xxpo_po_distribution_ext_r12.creation_date%type,
      created_by                     xxpo_po_distribution_ext_r12.created_by%type,
      last_updated_date              xxpo_po_distribution_ext_r12.last_updated_date%type,
      last_updated_by                xxpo_po_distribution_ext_r12.last_updated_by%type,
      last_update_login              xxpo_po_distribution_ext_r12.last_update_login%type,
      program_application_id         xxpo_po_distribution_ext_r12.program_application_id%type,
      program_id                     xxpo_po_distribution_ext_r12.program_id%type,
      program_update_date            xxpo_po_distribution_ext_r12.program_update_date%type,
      leg_req_dist_id                xxpo_po_distribution_ext_r12.leg_req_dist_id%type -- 1.20
      );
    type leg_distribution_tbl is table of leg_distribution_rec index by binary_integer;
    l_leg_distribution_tbl leg_distribution_tbl;
    l_err_record           number;
    cursor cur_leg_distribution is
      select xil.interface_txn_id,
             xil.leg_source_system,
             xil.leg_po_header_id,
             xil.leg_document_num,
             xil.leg_po_line_id,
             xil.leg_line_num,
             xil.leg_po_line_location_id,
             xil.leg_shipment_num,
             xil.leg_po_distribution_id,
             xil.leg_distribution_num,
             xil.leg_operating_unit_name,
             xil.leg_org_id,
             xil.operating_unit_name,
             xil.org_id,
             xil.leg_quantity_ordered,
             xil.leg_quantity_delivered,
             xil.leg_quantity_billed,
             xil.leg_quantity_cancelled,
             xil.leg_rate_date,
             xil.leg_rate,
             xil.leg_deliver_to_location,
             xil.leg_deliver_to_location_id,
             xil.deliver_to_location_id,
             xil.leg_deliver_to_person_emp_no,
             xil.leg_deliver_to_person_id,
             xil.deliver_to_person_id,
             xil.leg_destination_type_code,
             xil.leg_destination_organization,
             xil.leg_dest_organization_id,
             xil.destination_organization_id,
             xil.leg_destination_subinventory,
             xil.leg_set_of_books,
             xil.leg_set_of_books_id,
             xil.set_of_books_id,
             xil.leg_charge_account_seg1,
             xil.leg_charge_account_seg2,
             xil.leg_charge_account_seg3,
             xil.leg_charge_account_seg4,
             xil.leg_charge_account_seg5,
             xil.leg_charge_account_seg6,
             xil.leg_charge_account_seg7,
             xil.leg_charge_account_seg8,
             xil.leg_charge_account_seg9,
             xil.leg_charge_account_seg10,
             xil.leg_charge_account_ccid,
             xil.charge_account_seg1,
             xil.charge_account_seg2,
             xil.charge_account_seg3,
             xil.charge_account_seg4,
             xil.charge_account_seg5,
             xil.charge_account_seg6,
             xil.charge_account_seg7,
             xil.charge_account_seg8,
             xil.charge_account_seg9,
             xil.charge_account_seg10,
             xil.charge_account_ccid,
             xil.leg_accural_account_seg1,
             xil.leg_accural_account_seg2,
             xil.leg_accural_account_seg3,
             xil.leg_accural_account_seg4,
             xil.leg_accural_account_seg5,
             xil.leg_accural_account_seg6,
             xil.leg_accural_account_seg7,
             xil.leg_accural_account_seg8,
             xil.leg_accural_account_seg9,
             xil.leg_accural_account_seg10,
             xil.leg_accural_account_ccid,
             xil.accural_account_seg1,
             xil.accural_account_seg2,
             xil.accural_account_seg3,
             xil.accural_account_seg4,
             xil.accural_account_seg5,
             xil.accural_account_seg6,
             xil.accural_account_seg7,
             xil.accural_account_seg8,
             xil.accural_account_seg9,
             xil.accural_account_seg10,
             xil.accural_account_ccid,
             xil.leg_variance_account_seg1,
             xil.leg_variance_account_seg2,
             xil.leg_variance_account_seg3,
             xil.leg_variance_account_seg4,
             xil.leg_variance_account_seg5,
             xil.leg_variance_account_seg6,
             xil.leg_variance_account_seg7,
             xil.leg_variance_account_seg8,
             xil.leg_variance_account_seg9,
             xil.leg_variance_account_seg10,
             xil.leg_variance_account_ccid,
             xil.variance_account_seg1,
             xil.variance_account_seg2,
             xil.variance_account_seg3,
             xil.variance_account_seg4,
             xil.variance_account_seg5,
             xil.variance_account_seg6,
             xil.variance_account_seg7,
             xil.variance_account_seg8,
             xil.variance_account_seg9,
             xil.variance_account_seg10,
             xil.variance_account_ccid,
             xil.leg_accrued_flag,
             xil.leg_accrue_on_receipt_flag,
             xil.leg_project,
             xil.leg_project_id,
             xil.project_id,
             xil.leg_task,
             xil.leg_task_id,
             xil.task_id,
             decode(xil.leg_expenditure_type,
                    null,
                    null,
                    'SUPPLIER INVOICED MATERIAL'), -- Change Expenditure Org and Expenditure type Logic in PO Change
             xil.leg_project_accounting_context,
             xil.leg_expenditure_organization,
             xil.leg_expd_organization_id,
             xil.expenditure_organization_id,
             xil.leg_expenditure_item_date,
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
             xil.leg_amount_ordered,
             xil.leg_unit_price,
             xil.leg_invoice_adjustment_flag,
             xil.process_flag,
             xil.batch_id,
             xil.run_sequence_id,
             g_request_id,
             xil.error_type,
             xil.leg_entity,
             xil.leg_seq_num,
             xil.leg_process_flag,
             xil.leg_request_id,
             sysdate,
             g_created_by,
             sysdate,
             g_last_updated_by,
             g_last_update_login,
             g_prog_appl_id,
             g_conc_program_id,
             sysdate,
             leg_req_dist_id ---1.20
        from xxpo_po_distribution_ext_r12 xil
       where xil.leg_process_flag = 'V'
         and not exists
       (select 1
                from xxpo_po_distribution_stg xis
               where xis.interface_txn_id = xil.interface_txn_id);
  begin
    pov_ret_stats  := 'S';
    pov_err_msg    := null;
    g_total_count  := 0;
    g_failed_count := 0;
    g_loaded_count := 0;
    --Open cursor to extract data from extraction staging table
    open cur_leg_distribution;
    loop
      xxetn_debug_pkg.add_debug(piv_debug_msg => 'Loading distributions');
      l_leg_distribution_tbl.delete;
      fetch cur_leg_distribution bulk collect
        into l_leg_distribution_tbl limit 5000;
      --limit size of Bulk Collect
      -- Get Total Count
      g_total_count := g_total_count + l_leg_distribution_tbl.count;
      exit when l_leg_distribution_tbl.count = 0;
      begin
        -- Bulk Insert into Conversion table
        forall indx in 1 .. l_leg_distribution_tbl.count save exceptions
          insert into xxpo_po_distribution_stg
          values l_leg_distribution_tbl
            (indx);
      exception
        when others then
          print_log_message('Errors encountered while loading distribution data ');
          for l_indx_exp in 1 .. sql%bulk_exceptions.count loop
            l_err_record  := l_leg_distribution_tbl(sql%bulk_exceptions(l_indx_exp).error_index)
                             .interface_txn_id;
            pov_ret_stats := 'E';
            print_log_message('Record sequence (interface_txn_id) : ' || l_leg_distribution_tbl(sql%bulk_exceptions(l_indx_exp).error_index)
                              .interface_txn_id);
            print_log_message('Error Message : ' ||
                              sqlerrm(-sql%bulk_exceptions(l_indx_exp)
                                      .error_code));
            -- Updating Leg_process_flag to 'E' for failed records
            update xxpo_po_distribution_ext_r12 xil
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
    close cur_leg_distribution;
    commit;
    if g_failed_count > 0 then
      print_log_message('Number of Failed Records during load of distributions : ' ||
                        g_failed_count);
    end if;
    ---output
    g_loaded_count := g_total_count - g_failed_count;
    fnd_file.put_line(fnd_file.output,
                      ' Stats for distributions table load ');
    fnd_file.put_line(fnd_file.output, '================================');
    fnd_file.put_line(fnd_file.output, 'Total Count : ' || g_total_count);
    fnd_file.put_line(fnd_file.output, 'Loaded Count: ' || g_loaded_count);
    fnd_file.put_line(fnd_file.output, 'Failed Count: ' || g_failed_count);
    fnd_file.put_line(fnd_file.output, '================================');
    -- If records successfully posted to conversion staging table
    if g_total_count > 0 then
      print_log_message('Updating process flag (leg_process_flag) in extraction table for processed records ');
      update xxpo_po_distribution_ext_r12 xil
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
                from xxpo_po_distribution_stg xis
               where xis.interface_txn_id = xil.interface_txn_id);
      commit;
      -- Either no data to load from extraction table or records already exist in R12 staging table and hence not loaded
    else
      print_log_message('Either no data found for loading from extraction table or records already exist in R12 staging table and hence not loaded ');
      update xxpo_po_distribution_ext_r12 xil
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
                from xxpo_po_distribution_stg xis
               where xis.interface_txn_id = xil.interface_txn_id);
      commit;
    end if;
  exception
    when others then
      pov_ret_stats := 'E';
      pov_err_msg   := 'ERROR : Error in Load_distribution procedure' ||
                       substr(sqlerrm, 1, 200);
      rollback;
  end load_distribution;

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
        update xxpo_po_header_stg
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
           and leg_operating_unit_name =
               nvl(g_leg_operating_unit, leg_operating_unit_name);
      exception
        when others then
          print_log_message('Error : Exception occured while updating new batch id in headers staging ' ||
                            substr(sqlerrm, 1, 150));
      end;
      begin
        xxetn_debug_pkg.add_debug(piv_debug_msg => 'Generating new batch id for lines table');
        update xxpo_po_line_shipment_stg
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
           and leg_operating_unit_name =
               nvl(g_leg_operating_unit, leg_operating_unit_name);
      exception
        when others then
          print_log_message('Error : Exception occured while updating new batch id in lines staging ' ||
                            substr(sqlerrm, 1, 150));
      end;
      begin
        xxetn_debug_pkg.add_debug(piv_debug_msg => 'Generating new batch id for distribution table');
        update xxpo_po_distribution_stg
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
           and leg_operating_unit_name =
               nvl(g_leg_operating_unit, leg_operating_unit_name);
      exception
        when others then
          print_log_message('Error : Exception occured while updating new batch id in distribution staging ' ||
                            substr(sqlerrm, 1, 150));
      end;
      commit;
    else
      begin
        xxetn_debug_pkg.add_debug(piv_debug_msg => 'Reprocess updating run sequence id: Header table');
        update xxpo_po_header_stg
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
           and leg_operating_unit_name =
               nvl(g_leg_operating_unit, leg_operating_unit_name)
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
        xxetn_debug_pkg.add_debug(piv_debug_msg => 'Reprocess updating run sequence id: lines table');
        update xxpo_po_line_shipment_stg
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
           and leg_operating_unit_name =
               nvl(g_leg_operating_unit, leg_operating_unit_name)
           and (g_process_records = 'ALL' and
               (process_flag not in ('C', 'X', 'P')) or
               (g_process_records = 'ERROR' and (process_flag = 'E')) or
               g_process_records = 'UNPROCESSED' and (process_flag = 'N'));
      exception
        when others then
          print_log_message('Error : Exception occured while updating run seq id for reprocess of lines table: ' ||
                            substr(sqlerrm, 1, 150));
      end;
      begin
        xxetn_debug_pkg.add_debug(piv_debug_msg => 'Reprocess updating run sequence id: distribution table');
        update xxpo_po_distribution_stg
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
           and leg_operating_unit_name =
               nvl(g_leg_operating_unit, leg_operating_unit_name)
           and (g_process_records = 'ALL' and
               (process_flag not in ('C', 'X', 'P')) or
               (g_process_records = 'ERROR' and (process_flag = 'E')) or
               g_process_records = 'UNPROCESSED' and (process_flag = 'N'));
      exception
        when others then
          print_log_message('Error : Exception occured while updating run seq id for reprocess of distribution table: ' ||
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
  --   This procedure is used for batch id assignment (modified for V1.3) Kept as another option in case original assign_batch_id takes longer time than expected
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

        UPDATE xxpo_po_header_stg
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
        xxetn_debug_pkg.add_debug(piv_debug_msg => 'Generating new batch id for lines table');

        UPDATE xxpo_po_line_shipment_stg
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
          print_log_message('Error : Exception occured while updating new batch id in lines staging ' ||
                            SUBSTR(SQLERRM, 1, 150));
      END;

      BEGIN
        xxetn_debug_pkg.add_debug(piv_debug_msg => 'Generating new batch id for distribution table');

        UPDATE xxpo_po_distribution_stg
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
          print_log_message('Error : Exception occured while updating new batch id in distribution staging ' ||
                            SUBSTR(SQLERRM, 1, 150));
      END;


      ELSE -- if Parameter operating unit is NOT NULL

      BEGIN
        xxetn_debug_pkg.add_debug(piv_debug_msg => 'Generating new batch id for headers table');

        UPDATE xxpo_po_header_stg
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
         AND leg_operating_unit_name = g_leg_operating_unit;
      EXCEPTION
        WHEN OTHERS THEN
          print_log_message('Error : Exception occured while updating new batch id in headers staging ' ||
                            SUBSTR(SQLERRM, 1, 150));
      END;


      BEGIN
        xxetn_debug_pkg.add_debug(piv_debug_msg => 'Generating new batch id for lines table');

        UPDATE xxpo_po_line_shipment_stg
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
         AND leg_operating_unit_name = g_leg_operating_unit;
      EXCEPTION
        WHEN OTHERS THEN
          print_log_message('Error : Exception occured while updating new batch id in lines staging ' ||
                            SUBSTR(SQLERRM, 1, 150));
      END;

      BEGIN
        xxetn_debug_pkg.add_debug(piv_debug_msg => 'Generating new batch id for distribution table');

        UPDATE xxpo_po_distribution_stg
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
         AND leg_operating_unit_name = g_leg_operating_unit;
      EXCEPTION
        WHEN OTHERS THEN
          print_log_message('Error : Exception occured while updating new batch id in distribution staging ' ||
                            SUBSTR(SQLERRM, 1, 150));
      END;

      END IF; -- if Parameter operating unit is NULL

      COMMIT;


    ELSE  -- If Batch Id is NOT NULL

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

        UPDATE xxpo_po_header_stg
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
        xxetn_debug_pkg.add_debug(piv_debug_msg => 'Reprocess updating run sequence id: lines table');

        UPDATE xxpo_po_line_shipment_stg
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
          print_log_message('Error : Exception occured while updating run seq id for reprocess of lines table: ' ||
                            SUBSTR(SQLERRM, 1, 150));
      END;

      BEGIN
        xxetn_debug_pkg.add_debug(piv_debug_msg => 'Reprocess updating run sequence id: distribution table');

        UPDATE xxpo_po_distribution_stg
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
          print_log_message('Error : Exception occured while updating run seq id for reprocess of distribution table: ' ||
                            SUBSTR(SQLERRM, 1, 150));
      END;


    ELSE -- if Parameter operating unit is NOT NULL

      BEGIN
        xxetn_debug_pkg.add_debug(piv_debug_msg => 'Reprocess updating run sequence id: Header table');

        UPDATE xxpo_po_header_stg
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
         AND leg_operating_unit_name = g_leg_operating_unit
         AND process_flag IN (l_new_rec, l_err_rec, l_val_rec);
      EXCEPTION
        WHEN OTHERS THEN
          print_log_message('Error : Exception occured while updating run seq id for reprocess of Header table: ' ||
                            SUBSTR(SQLERRM, 1, 150));
      END;

      BEGIN
        xxetn_debug_pkg.add_debug(piv_debug_msg => 'Reprocess updating run sequence id: lines table');

        UPDATE xxpo_po_line_shipment_stg
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
         AND leg_operating_unit_name = g_leg_operating_unit
         AND process_flag IN (l_new_rec, l_err_rec, l_val_rec);
      EXCEPTION
        WHEN OTHERS THEN
          print_log_message('Error : Exception occured while updating run seq id for reprocess of lines table: ' ||
                            SUBSTR(SQLERRM, 1, 150));
      END;

      BEGIN
        xxetn_debug_pkg.add_debug(piv_debug_msg => 'Reprocess updating run sequence id: distribution table');

        UPDATE xxpo_po_distribution_stg
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
           AND leg_operating_unit_name = g_leg_operating_unit
           AND process_flag IN (l_new_rec, l_err_rec, l_val_rec);
      EXCEPTION
        WHEN OTHERS THEN
          print_log_message('Error : Exception occured while updating run seq id for reprocess of distribution table: ' ||
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
  -- Procedure: close_for_invoice
  -- =====================================================================================
  --   This procedure is used update closed code status for completed shipment lines
  -- =====================================================================================
  procedure close_for_invoice(x_ou_errbuf        out nocopy varchar2,
                              x_ou_retcode       out nocopy number,
                              p_dummy            in varchar2,
                              pin_batch_id       in number,
                              piv_operating_unit in varchar2) is
    g_entity_name      varchar2(240) := 'SHIPMENTS TO CLOSE INVOICE ';
    g_request_id       varchar2(240) := fnd_global.conc_program_id;
    g_org_id           hr_operating_units.organization_id%type := fnd_profile.value('ORG_ID');
    g_user_id          fnd_user.user_id%type := fnd_global.user_id;
    g_login_id         fnd_logins.login_id%type := fnd_global.login_id;
    g_resp_id          fnd_responsibility.responsibility_id%type := fnd_global.resp_id;
    g_resp_appl_id     fnd_application.application_id%type := fnd_global.resp_appl_id;
    v_header_id        po_headers_all.po_header_id%type;
    v_line_id          po_lines_all.po_line_id%type;
    v_line_location_id po_line_locations_all.line_location_id%type;
    cursor cur_po_lines(p_batch in number) is
      select pha.segment1 po_number,
             pla.line_num,
             plla.shipment_num,
             pha.po_header_id,
             pla.po_line_id,
             plla.line_location_id
        from po_lines_all              pla,
             po_headers_all            pha,
             po_line_locations_all     plla,
             xxpo_po_header_stg        xphs,
             xxpo_po_line_shipment_stg xpls
       where pha.po_header_id = pla.po_header_id
         and pla.po_line_id = plla.po_line_id
         and pha.segment1 = xphs.leg_document_num
         and xphs.leg_po_header_id = xpls.leg_po_header_id
         and xphs.leg_operating_unit_name =
             nvl(piv_operating_unit, xphs.leg_operating_unit_name)
         and xphs.leg_operating_unit_name = xpls.leg_operating_unit_name
         and xphs.leg_source_system = xpls.leg_source_system
         and xphs.batch_id = p_batch
         and xphs.batch_id = xpls.batch_id
         and pla.line_num = xpls.leg_line_num
         and plla.shipment_num = xpls.leg_shipment_num
         and xpls.leg_closed_for_invoice = 'Y'
         and xphs.process_flag = 'C';
    x_return_code     varchar2(100);
    v_session_id      integer := userenv('sessionid');
    p_ou_ret          boolean;
    v_records_read    number := 0;
    v_records_success number := 0;
    v_records_failed  number := 0;
  begin
    begin
      fnd_client_info.set_org_context(g_org_id);
      fnd_global.initialize(v_session_id,
                            g_user_id, --user_id set these values accordingly it is required
                            g_resp_id, --resp_id
                            g_resp_appl_id, --resp_appl_id
                            0,
                            -1,
                            1,
                            -1,
                            -1,
                            -1,
                            -1,
                            null,
                            null,
                            null,
                            null,
                            null,
                            null,
                            -1);
    end;
    for cur_po_lines_rec in cur_po_lines(pin_batch_id) loop
      x_return_code      := null;
      v_records_read     := v_records_read + 1;
      v_header_id        := cur_po_lines_rec.po_header_id;
      v_line_id          := cur_po_lines_rec.po_line_id;
      v_line_location_id := cur_po_lines_rec.line_location_id;
      p_ou_ret           := po_actions.close_po(p_docid         => v_header_id,
                                                p_doctyp        => 'PO',
                                                p_docsubtyp     => 'STANDARD',
                                                p_lineid        => v_line_id,
                                                p_shipid        => v_line_location_id,
                                                p_action        => 'INVOICE CLOSE',
                                                p_reason        => '',
                                                p_calling_mode  => 'PO',
                                                p_conc_flag     => 'N',
                                                p_return_code   => x_return_code,
                                                p_auto_close    => 'N',
                                                p_action_date   => sysdate,
                                                p_origin_doc_id => null);
      commit;
      if p_ou_ret then
        print_log_message('Shipment Number ' ||
                          cur_po_lines_rec.shipment_num || 'Line Number ' ||
                          cur_po_lines_rec.line_num || 'PO Number ' ||
                          cur_po_lines_rec.po_number ||
                          ' has been Successfully Closed for Invoice');
        v_records_success := v_records_success + 1;
      else
        print_log_message('Shipment Number ' ||
                          cur_po_lines_rec.shipment_num || 'Line Number ' ||
                          cur_po_lines_rec.line_num || 'PO Number ' ||
                          cur_po_lines_rec.po_number ||
                          ' could not be Closed for Invoice');
        v_records_failed := v_records_failed + 1;
      end if;
    end loop;
    begin
      fnd_file.put_line(fnd_file.output,
                        '+---------------------------------------------------------------------------------+');
      fnd_file.put_line(fnd_file.output,
                        'Concurrent Request ID : ' || g_request_id ||
                        '            ' || '    Begin Date : ' ||
                        to_char(sysdate, 'DD-MON-RRRR HH:MI:SS'));
      fnd_file.put_line(fnd_file.output,
                        '+---------------------------------------------------------------------------------+');
      fnd_file.put_line(fnd_file.output,
                        'Eaton' || ' - ' || g_entity_name ||
                        ' - Processing Statistics          ');
      fnd_file.put_line(fnd_file.output,
                        '+---------------------------------------------------------------------------------+');
      fnd_file.put_line(fnd_file.output, 'Processed Records Summary');
      fnd_file.put_line(fnd_file.output,
                        '+---------------------------------------------------------------------------------+');
      fnd_file.put_line(fnd_file.output,
                        'Total No of Purchase Order Shipments considered for Closure of Invoice: ' ||
                        v_records_read);
      fnd_file.put_line(fnd_file.output,
                        'Total No of Purchase Order Shipments with successful Closure of Invoice: ' ||
                        v_records_success);
      fnd_file.put_line(fnd_file.output,
                        'Total No of Purchase Order Shipments failed for Closure of Invoice:: ' ||
                        v_records_failed);
      fnd_file.put_line(fnd_file.output,
                        '+---------------------------------------------------------------------------------+');
    exception
      when others then
        fnd_file.put_line(fnd_file.log,
                          ' Error in close_for_invoice procedure while generating output file' ||
                          sqlerrm);
        x_ou_retcode := 1;
    end;
  exception
    when others then
      fnd_file.put_line(fnd_file.log,
                        ' Unexpected  Error in close_for_invoice procedure' ||
                        sqlerrm);
      x_ou_retcode := 2;
  end close_for_invoice;

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
    --- Tax code lookup check
    begin
      print_log_message('Check for Tax code conversion lookup');
      select 1
        into ln_cat_count
        from fnd_lookup_types flv
       where 1 = 1
         and flv.lookup_type = g_tax_code_lookup;
      print_log_message('Tax code lookup is present ');
    exception
      when no_data_found then
        print_log_message('Tax code lookup is missing : ' ||
                          g_tax_code_lookup);
        g_retcode := 1;
      when others then
        print_log_message('Error : Exception occured in Tax code lookup ' ||
                          substr(sqlerrm, 1, 240));
        g_retcode := 1;
    end;
    --- OU lookup check
    begin
      print_log_message('Check for Common OU lookup');
      select 1
        into ln_cat_count
        from fnd_lookup_types flv
       where 1 = 1
         and flv.lookup_type = g_ou_lookup;
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
    --- Payment term lookup check
    begin
      print_log_message('Check for Payment term lookup');
      select 1
        into ln_cat_count
        from fnd_lookup_types flv
       where 1 = 1
         and flv.lookup_type = g_payment_term_lookup;
      print_log_message('Payment term lookup is present ');
    exception
      when no_data_found then
        print_log_message('Payment term  lookup is missing : ' ||
                          g_payment_term_lookup);
        g_retcode := 1;
      when others then
        print_log_message('Error : Exception occured in Payment term  lookup ' ||
                          substr(sqlerrm, 1, 240));
        g_retcode := 1;
    end;
  exception
    when others then
      print_log_message('Error : Exception occured in pre_validate procedure ' ||
                        substr(sqlerrm, 1, 240));
      g_retcode := 1;
  end pre_validate;

  ---+================================================================================+
  ---|FUNCTION NAME : xxpo_dup_po
  ---|DESCRIPTION   : This function will check for duplicate Purchase orders
  ---+================================================================================+
  function xxpo_dup_po(p_in_int_txn_id in number,
                       p_in_po_number  in varchar2,
                       p_in_org_id     in number) return varchar2 is
    -- v 1.24 -- SDP --25/03/2016
    lv_status  varchar2(1) := 'N';
    lv_count   number := 0;
    l_err_code varchar2(40);
    l_err_msg  varchar2(2000);
  begin
    select count(segment1)
      into lv_count
      from po_headers_all
     where segment1 = p_in_po_number
       and org_id = p_in_org_id;
    if lv_count = 0 then
      lv_status := 'N';
    else
      lv_status  := 'Y';
      l_err_code := 'ETN_PO_DUPLICATE_DOC_NUM';
      l_err_msg  := 'Error: PO already exists in the system. ';
      log_errors(pin_interface_txn_id    => p_in_int_txn_id,
                 piv_source_table        => 'xxpo_po_header_stg',
                 piv_source_column_name  => 'leg_document_num',
                 piv_source_column_value => p_in_po_number,
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
      l_err_msg  := 'Error: Exception error in xxpo_dup_po procedure. ';
      log_errors(pin_interface_txn_id    => p_in_int_txn_id,
                 piv_source_table        => 'xxpo_po_header_stg',
                 piv_source_column_name  => 'leg_document_num',
                 piv_source_column_value => p_in_po_number,
                 piv_source_keyname1     => null,
                 piv_source_keyvalue1    => null,
                 piv_error_type          => 'ERR_VAL',
                 piv_error_code          => l_err_code,
                 piv_error_message       => l_err_msg);
      return lv_status;
  end xxpo_dup_po;

  /* ----1.1
     +================================================================================+
    |FUNCTION NAME      : xxpo_derive_org_id                                        |
    |DESCRIPTION        : This function will return ORG_ID                     |
    +================================================================================+
  */
  /* PROCEDURE xxpo_derive_org_id (
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
           l_err_code := 'ETN_PO_OPERATING_UNIT_ERROR';
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
           l_err_code := 'ETN_PO_OPERATING_UNIT_ERROR';
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
           l_err_code := 'ETN_PO_OPERATING_UNIT_ERROR';
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
        l_err_code := 'ETN_PO_PROCEDURE_EXCEPTION';
        l_err_msg :=
                 'Error: EXCEPTION error for xxpo_derive_org_id procedure. ';
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
  END xxpo_derive_org_id; */
  /*  ---1.1
     +================================================================================+
    |FUNCTION NAME      : xxpo_derive_exp_org_id                                        |
    |DESCRIPTION        : This function will return ORG_ID                     |
    +================================================================================+
  */
  /*    PROCEDURE xxpo_derive_exp_org_id (
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
           l_err_code := 'ETN_PO_OPERATING_UNIT_ERROR';
           l_err_msg :=
              'Error: Mapping not defined in the Common OU lookup for exp org. ';
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
           l_err_code := 'ETN_PO_OPERATING_UNIT_ERROR';
           l_err_msg :=
              'Error: Exception error while deriving operating unit from Common OU lookup for exp org. ';
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
           l_err_code := 'ETN_PO_OPERATING_UNIT_ERROR';
           l_err_msg := 'Error: Expenditure Org ID could not be derived. ';
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
        l_err_code := 'ETN_PO_PROCEDURE_EXCEPTION';
        l_err_msg :=
             'Error: EXCEPTION error for xxpo_derive_exp_org_id procedure. ';
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
  END xxpo_derive_exp_org_id; */
  /*
  +================================================================================+
  |FUNCTION NAME : xxpo_fob                                               |
  |DESCRIPTION   : This function will check for FOB lookup code           |
  +================================================================================+
  */
  function xxpo_fob(p_in_interface_txn_id in number,
                    p_in_stg_tbl_name     in varchar2,
                    p_in_column_name      in varchar2,
                    p_in_fob_lp_code      in varchar2) return varchar2 is
    lv_status  varchar2(1) := 'N';
    l_err_code varchar2(40);
    l_err_msg  varchar2(2000);
  begin
    select 'N'
      into lv_status
      from po_lookup_codes
     where lookup_type = 'FOB'
       and (sysdate) <= nvl(inactive_date, sysdate)
       and enabled_flag = 'Y'
       and lookup_code = p_in_fob_lp_code;
    return lv_status;
  exception
    when no_data_found then
      l_err_code := 'ETN_PO_INVALID_FOB';
      l_err_msg  := 'Error: Invalid FOB value. ';
      log_errors(pin_interface_txn_id    => p_in_interface_txn_id,
                 piv_source_table        => p_in_stg_tbl_name,
                 piv_source_column_name  => p_in_column_name,
                 piv_source_column_value => p_in_fob_lp_code,
                 piv_source_keyname1     => null,
                 piv_source_keyvalue1    => null,
                 piv_error_type          => 'ERR_VAL',
                 piv_error_code          => l_err_code,
                 piv_error_message       => l_err_msg);
      lv_status := 'Y';
      return lv_status;
    when others then
      l_err_code := 'ETN_PO_INVALID_FOB';
      l_err_msg  := 'Error: Exception error while deriving FOB. ';
      log_errors(pin_interface_txn_id    => p_in_interface_txn_id,
                 piv_source_table        => p_in_stg_tbl_name,
                 piv_source_column_name  => p_in_column_name,
                 piv_source_column_value => p_in_fob_lp_code,
                 piv_source_keyname1     => null,
                 piv_source_keyvalue1    => null,
                 piv_error_type          => 'ERR_VAL',
                 piv_error_code          => l_err_code,
                 piv_error_message       => l_err_msg);
      lv_status := 'Y';
      return lv_status;
  end xxpo_fob;

  /*
  +================================================================================+
  |FUNCTION NAME : xxpo_freight                                               |
  |DESCRIPTION   : This function will check for freight lookup code           |
  +================================================================================+
  */
  function xxpo_freight(p_in_interface_txn_id in number,
                        p_in_stg_tbl_name     in varchar2,
                        p_in_column_name      in varchar2,
                        p_in_freight_lp_code  in varchar2) return varchar2 is
    lv_status  varchar2(1) := 'N';
    l_err_code varchar2(40);
    l_err_msg  varchar2(2000);
  begin
    select 'N'
      into lv_status
      from po_lookup_codes
     where lookup_type = 'FREIGHT TERMS'
       and (sysdate) <= nvl(inactive_date, sysdate)
       and enabled_flag = 'Y'
       and upper(lookup_code) = upper(p_in_freight_lp_code);
    return lv_status;
  exception
    when no_data_found then
      l_err_code := 'ETN_PO_PROCEDURE_EXCEPTION';
      l_err_msg  := 'Error: Invalid Freight terms value. ';
      log_errors(pin_interface_txn_id    => p_in_interface_txn_id,
                 piv_source_table        => p_in_stg_tbl_name,
                 piv_source_column_name  => p_in_column_name,
                 piv_source_column_value => p_in_freight_lp_code,
                 piv_source_keyname1     => null,
                 piv_source_keyvalue1    => null,
                 piv_error_type          => 'ERR_VAL',
                 piv_error_code          => l_err_code,
                 piv_error_message       => l_err_msg);
      lv_status := 'Y';
      return lv_status;
    when others then
      l_err_code := 'ETN_PO_PROCEDURE_EXCEPTION';
      l_err_msg  := 'Error: Exception error while deriving Freight terms value. ';
      log_errors(pin_interface_txn_id    => p_in_interface_txn_id,
                 piv_source_table        => p_in_stg_tbl_name,
                 piv_source_column_name  => p_in_column_name,
                 piv_source_column_value => p_in_freight_lp_code,
                 piv_source_keyname1     => null,
                 piv_source_keyvalue1    => null,
                 piv_error_type          => 'ERR_VAL',
                 piv_error_code          => l_err_code,
                 piv_error_message       => l_err_msg);
      lv_status := 'Y';
      return lv_status;
  end xxpo_freight;

  /*  --1.1
  +================================================================================+
  |FUNCTION NAME : xxpo_INVCURR                                               |
  |DESCRIPTION   : This function will check/validate Invoice currency codes   |
  +================================================================================+
  */
  /* FUNCTION xxpo_invcurr (
     p_in_interface_txn_id    IN   NUMBER,
     p_in_stg_tbl_name        IN   VARCHAR2,
     p_in_column_name         IN   VARCHAR2,
     p_in_invoice_curr_code   IN   VARCHAR2
  )
     RETURN VARCHAR2
  IS
     lv_status    VARCHAR2 (1)    := 'N';
     l_err_code   VARCHAR2 (40);
     l_err_msg    VARCHAR2 (2000);
  BEGIN
     SELECT 'N'
       INTO lv_status
       FROM fnd_currencies
      WHERE UPPER (currency_code) = UPPER (p_in_invoice_curr_code)
        AND SYSDATE BETWEEN NVL (start_date_active, SYSDATE)
                        AND NVL (end_date_active, SYSDATE)
        AND enabled_flag = 'Y';

     RETURN lv_status;
  EXCEPTION
     WHEN NO_DATA_FOUND
     THEN
        l_err_code := 'ETN_PO_INVALID_CURRENCY_CODE';
        l_err_msg := 'Error: Invalid Currency code. ';
        log_errors (pin_interface_txn_id         => p_in_interface_txn_id,
                    piv_source_table             => p_in_stg_tbl_name,
                    piv_source_column_name       => p_in_column_name,
                    piv_source_column_value      => p_in_invoice_curr_code,
                    piv_source_keyname1          => NULL,
                    piv_source_keyvalue1         => NULL,
                    piv_error_type               => 'ERR_VAL',
                    piv_error_code               => l_err_code,
                    piv_error_message            => l_err_msg
                   );
        lv_status := 'Y';
        RETURN lv_status;
     WHEN OTHERS
     THEN
        l_err_code := 'ETN_PO_PROCEDURE_EXCEPTION';
        l_err_msg := 'Error: Exception error in xxpo_invcurr procedure. ';
        log_errors (pin_interface_txn_id         => p_in_interface_txn_id,
                    piv_source_table             => p_in_stg_tbl_name,
                    piv_source_column_name       => p_in_column_name,
                    piv_source_column_value      => p_in_invoice_curr_code,
                    piv_source_keyname1          => NULL,
                    piv_source_keyvalue1         => NULL,
                    piv_error_type               => 'ERR_VAL',
                    piv_error_code               => l_err_code,
                    piv_error_message            => l_err_msg
                   );
        lv_status := 'Y';
        RETURN lv_status;
  END xxpo_invcurr; */
  /*
  +================================================================================+
  |FUNCTION NAME : PO_RATE_TYPE                                      |
  |DESCRIPTION   : This function will check/validate RATE_TYPE       |
  +================================================================================+
  */
  function xxpo_po_rate_type(p_in_interface_txn_id in number,
                             p_in_stg_tbl_name     in varchar2,
                             p_in_column_name      in varchar2,
                             p_in_curr_code        in varchar2,
                             p_in_rate_type        in varchar2)
    return varchar2 is
    lv_status  varchar2(1) := 'N';
    l_err_code varchar2(40);
    l_err_msg  varchar2(2000);
  begin
    --- 1.1
    select 'N'
      into lv_status
      from gl_daily_conversion_types
     where conversion_type = p_in_rate_type;
    return lv_status;
  exception
    when others then
      l_err_code := 'ETN_PO_INVALID_RATE_TYPE';
      l_err_msg  := 'Error: Conversion type Invalid. ';
      log_errors(pin_interface_txn_id    => p_in_interface_txn_id,
                 piv_source_table        => p_in_stg_tbl_name,
                 piv_source_column_name  => p_in_column_name,
                 piv_source_column_value => p_in_rate_type,
                 piv_source_keyname1     => null,
                 piv_source_keyvalue1    => null,
                 piv_error_type          => 'ERR_VAL',
                 piv_error_code          => l_err_code,
                 piv_error_message       => l_err_msg);
      lv_status := 'Y';
      return lv_status;
  end xxpo_po_rate_type;

  /* ------1.1
   +================================================================================+
  |FUNCTION NAME : xxpo_yes_no_v                                                   |
  |DESCRIPTION   : This function will check/validate YES/NO Flag                   |
  +================================================================================+
  */
  /*    FUNCTION xxpo_yes_no (
     p_in_interface_txn_id   IN   NUMBER,
     p_in_stg_tbl_name       IN   VARCHAR2,
     p_in_column_name        IN   VARCHAR2,
     p_in_flag               IN   VARCHAR2
  )
     RETURN VARCHAR2
  IS
     lv_status    VARCHAR2 (1)    := 'N';
     l_err_code   VARCHAR2 (40);
     l_err_msg    VARCHAR2 (2000);
  BEGIN
     IF p_in_flag = 'Y' OR p_in_flag = 'N'
     THEN
        NULL;
        lv_status := 'N';
        RETURN lv_status;
     ELSE
        lv_status := 'Y';
        l_err_code := 'ETN_PO_INVALID_FLAG';
        l_err_msg := 'Error: Invalid value for flag : ' || p_in_column_name;
        log_errors (pin_interface_txn_id         => p_in_interface_txn_id,
                    piv_source_table             => p_in_stg_tbl_name,
                    piv_source_column_name       => p_in_column_name,
                    piv_source_column_value      => p_in_flag,
                    piv_source_keyname1          => NULL,
                    piv_source_keyvalue1         => NULL,
                    piv_error_type               => 'ERR_VAL',
                    piv_error_code               => l_err_code,
                    piv_error_message            => l_err_msg
                   );
        RETURN lv_status;
     END IF;
  EXCEPTION
     WHEN OTHERS
     THEN
        l_err_code := 'ETN_PO_PROCEDURE_EXCEPTION';
        l_err_msg := 'Error: EXCEPTION error for procedure xxpo_yes_no. ';
        log_errors (pin_interface_txn_id         => p_in_interface_txn_id,
                    piv_source_table             => p_in_stg_tbl_name,
                    piv_source_column_name       => p_in_column_name,
                    piv_source_column_value      => p_in_flag,
                    piv_source_keyname1          => NULL,
                    piv_source_keyvalue1         => NULL,
                    piv_error_type               => 'ERR_VAL',
                    piv_error_code               => l_err_code,
                    piv_error_message            => l_err_msg
                   );
        lv_status := 'Y';
        RETURN lv_status;
  END xxpo_yes_no; */
  /*
  +================================================================================+
  |FUNCTION NAME : xxpo_shipvialookup                                           |
  |DESCRIPTION   : This function will check/validate SHIP_VIA_LOOKUP_CODE          |
  +================================================================================+
  */
  function xxpo_shipvialookup(p_in_interface_txn_id in number,
                              p_in_stg_tbl_name     in varchar2,
                              p_in_column_name      in varchar2,
                              p_in_ship_via_lp_code in varchar2)
    return varchar2 is
    lv_status   varchar2(1) := 'N';
    lv_org_name varchar2(240) := null;
    l_err_code  varchar2(40);
    l_err_msg   varchar2(2000);
  begin
    select 'N'
      into lv_status
      from wsh_carriers
     where upper(freight_code) = upper(p_in_ship_via_lp_code);
    return lv_status;
  exception
    when no_data_found then
      l_err_code := 'ETN_PO_INVALID_FREIGHT_CARRIER';
      l_err_msg  := 'Error: Invalid freight carrier value. ';
      log_errors(pin_interface_txn_id    => p_in_interface_txn_id,
                 piv_source_table        => p_in_stg_tbl_name,
                 piv_source_column_name  => p_in_column_name,
                 piv_source_column_value => p_in_ship_via_lp_code,
                 piv_source_keyname1     => null,
                 piv_source_keyvalue1    => null,
                 piv_error_type          => 'ERR_VAL',
                 piv_error_code          => l_err_code,
                 piv_error_message       => l_err_msg);
      lv_status := 'Y';
      return lv_status;
    when others then
      l_err_code := 'ETN_PO_INVALID_FREIGHT_CARRIER';
      l_err_msg  := 'Error: EXCEPTION error while validating freight carrier value. ';
      log_errors(pin_interface_txn_id    => p_in_interface_txn_id,
                 piv_source_table        => p_in_stg_tbl_name,
                 piv_source_column_name  => p_in_column_name,
                 piv_source_column_value => p_in_ship_via_lp_code,
                 piv_source_keyname1     => null,
                 piv_source_keyvalue1    => null,
                 piv_error_type          => 'ERR_VAL',
                 piv_error_code          => l_err_code,
                 piv_error_message       => l_err_msg);
      lv_status := 'Y';
      return lv_status;
  end xxpo_shipvialookup;

  /*
    +================================================================================+
   |FUNCTION NAME      : xxpo_AGENT_ID                                                  |
  |DESCRIPTION        : This function will be used for deriving AGENT_ID               |
   +================================================================================+
      */
  function xxpo_agent_id(p_in_interface_txn_id in number,
                         p_in_stg_tbl_name     in varchar2,
                         p_in_column_name      in varchar2,
                         p_in_agent_emp_no     in varchar2) return number is
    l_agent_id number;
    l_err_code varchar2(40);
    l_err_msg  varchar2(2000);
  begin
    select ppf.person_id
      into l_agent_id
      from per_person_types_tl      ttl,
           per_person_types         typ,
           per_person_type_usages_f ptu,
           per_all_people_f         ppf,
           po_agents                pa
     where ttl.language = userenv('LANG')
       and ttl.person_type_id = typ.person_type_id
       and typ.system_person_type in ('EMP', 'CWK')
       and typ.person_type_id = ptu.person_type_id
       and sysdate between ptu.effective_start_date and
           ptu.effective_end_date
       and sysdate between ppf.effective_start_date and
           ppf.effective_end_date
       and ptu.person_id = ppf.person_id
       and ppf.employee_number = p_in_agent_emp_no
       and nvl(current_employee_flag, 'N') = 'Y'
       and ppf.person_id = pa.agent_id
       and trunc(sysdate) between trunc(pa.start_date_active) and
           trunc(nvl(pa.end_date_active, sysdate));
    return l_agent_id;
  exception
    when no_data_found then
      l_err_code := 'ETN_PO_INVALID_AGENT';
      l_err_msg  := 'Error: Invalid Agent employee number ';
      log_errors(pin_interface_txn_id    => p_in_interface_txn_id,
                 piv_source_table        => p_in_stg_tbl_name,
                 piv_source_column_name  => p_in_column_name,
                 piv_source_column_value => p_in_agent_emp_no,
                 piv_source_keyname1     => null,
                 piv_source_keyvalue1    => null,
                 piv_error_type          => 'ERR_VAL',
                 piv_error_code          => l_err_code,
                 piv_error_message       => l_err_msg);
      return null;
    when others then
      l_err_code := 'ETN_PO_INVALID_AGENT';
      l_err_msg  := 'Error: EXCEPTION error while deriving agent id. ';
      log_errors(pin_interface_txn_id    => p_in_interface_txn_id,
                 piv_source_table        => p_in_stg_tbl_name,
                 piv_source_column_name  => p_in_column_name,
                 piv_source_column_value => p_in_agent_emp_no,
                 piv_source_keyname1     => null,
                 piv_source_keyvalue1    => null,
                 piv_error_type          => 'ERR_VAL',
                 piv_error_code          => l_err_code,
                 piv_error_message       => l_err_msg);
      return null;
  end xxpo_agent_id;

  /*
   +================================================================================+
  |FUNCTION NAME      : xxpo_vendor_id                                       |
  |DESCRIPTION        : This function will return VENDOR_ID                  |
  +================================================================================+
    */
  function xxpo_vendor_id(p_in_interface_txn_id in number,
                          p_in_stg_tbl_name     in varchar2,
                          p_in_column_name      in varchar2,
                          p_in_vendor_num       in varchar2) return number is
    lv_vendor_id number;
    l_err_code   varchar2(40);
    l_err_msg    varchar2(2000);
  begin
    select vendor_id
      into lv_vendor_id
      from ap_suppliers
     where segment1 = p_in_vendor_num
       and enabled_flag = 'Y';
    return lv_vendor_id;
  exception
    when no_data_found then
      l_err_code := 'ETN_PO_INVALID_VENDOR';
      l_err_msg  := 'Error: Invalid Vendor Number. ';
      log_errors(pin_interface_txn_id    => p_in_interface_txn_id,
                 piv_source_table        => p_in_stg_tbl_name,
                 piv_source_column_name  => p_in_column_name,
                 piv_source_column_value => p_in_vendor_num,
                 piv_source_keyname1     => null,
                 piv_source_keyvalue1    => null,
                 piv_error_type          => 'ERR_VAL',
                 piv_error_code          => l_err_code,
                 piv_error_message       => l_err_msg);
      return null;
    when others then
      l_err_code := 'ETN_PO_INVALID_VENDOR';
      l_err_msg  := 'Error: EXCEPTION error while validating Vendor. ';
      log_errors(pin_interface_txn_id    => p_in_interface_txn_id,
                 piv_source_table        => p_in_stg_tbl_name,
                 piv_source_column_name  => p_in_column_name,
                 piv_source_column_value => p_in_vendor_num,
                 piv_source_keyname1     => null,
                 piv_source_keyvalue1    => null,
                 piv_error_type          => 'ERR_VAL',
                 piv_error_code          => l_err_code,
                 piv_error_message       => l_err_msg);
      return null;
  end xxpo_vendor_id;

  /*
   +================================================================================+
  |FUNCTION NAME      : xxpo_po_del_per_id                                         |
  |DESCRIPTION        : This function will be used for deriving  employee number   |
  +================================================================================+
     */
  function xxpo_po_del_per_id(p_in_interface_txn_id in number,
                              p_in_stg_tbl_name     in varchar2,
                              p_in_column_name      in varchar2,
                              p_in_agent_emp_no     in varchar2,
                              p_in_plant_no         in varchar2) -------v1.14 --Added for Agent_Id from Plant Number
   return number is
    l_agent_id number;
    l_err_code varchar2(40);
    l_err_msg  varchar2(2000);
  begin
    select ppf.person_id
      into l_agent_id
      from per_person_types_tl      ttl,
           per_person_types         typ,
           per_person_type_usages_f ptu,
           per_all_people_f         ppf,
           po_agents                pa
     where ttl.language = userenv('LANG')
       and ttl.person_type_id = typ.person_type_id
       and typ.system_person_type in ('EMP', 'CWK')
       and typ.person_type_id = ptu.person_type_id
       and sysdate between ptu.effective_start_date and
           ptu.effective_end_date
       and sysdate between ppf.effective_start_date and
           ppf.effective_end_date
       and ptu.person_id = ppf.person_id
       and ppf.employee_number = p_in_agent_emp_no
       and nvl(current_employee_flag, 'N') = 'Y'
       and ppf.person_id = pa.agent_id
       and trunc(sysdate) between trunc(pa.start_date_active) and
           trunc(nvl(pa.end_date_active, sysdate));
    return l_agent_id;
  exception
    when no_data_found then
      begin
        -------v1.14
        select ppf.person_id
          into l_agent_id
          from per_person_types_tl      ttl,
               per_person_types         typ,
               per_person_type_usages_f ptu,
               per_all_people_f         ppf,
               po_agents                pa,
               fnd_lookup_values        flv
         where ttl.language = userenv('LANG')
           and ttl.person_type_id = typ.person_type_id
           and typ.system_person_type in ('EMP', 'CWK')
           and typ.person_type_id = ptu.person_type_id
           and sysdate between ptu.effective_start_date and
               ptu.effective_end_date
           and sysdate between ppf.effective_start_date and
               ppf.effective_end_date
           and ptu.person_id = ppf.person_id
           and ppf.employee_number = flv.description
           and flv.lookup_type = 'ETN_LEDGER_GENERIC_BUYER'
           and flv.meaning = p_in_plant_no
           and flv.language = userenv('LANG')
           and flv.enabled_flag = 'Y'
           and trunc(sysdate) between
               trunc(nvl(flv.start_date_active, sysdate)) and
               trunc(nvl(flv.end_date_active, sysdate))
           and flv.enabled_flag = 'Y'
           and nvl(current_employee_flag, 'N') = 'Y'
           and ppf.person_id = pa.agent_id
           and trunc(sysdate) between trunc(pa.start_date_active) and
               trunc(nvl(pa.end_date_active, sysdate));
        return l_agent_id;
      exception
        when others then
          l_err_code := 'ETN_PO_INVALID_AGENT';
          l_err_msg  := 'Error: Invalid Deliver to person employee number and employee Number not present in Lookup ETN_LEDGER_GENERIC_BUYER for plant:' ||
                        p_in_plant_no; --v1.14
          log_errors(pin_interface_txn_id    => p_in_interface_txn_id,
                     piv_source_table        => p_in_stg_tbl_name,
                     piv_source_column_name  => p_in_column_name,
                     piv_source_column_value => p_in_plant_no,
                     piv_source_keyname1     => null,
                     piv_source_keyvalue1    => null,
                     piv_error_type          => 'ERR_VAL',
                     piv_error_code          => l_err_code,
                     piv_error_message       => l_err_msg);
          return null;
      end;
    when others then
      l_err_code := 'ETN_PO_INVALID_AGENT';
      l_err_msg  := 'Error: EXCEPTION error while deriving deliver to person employee id. ';
      log_errors(pin_interface_txn_id    => p_in_interface_txn_id,
                 piv_source_table        => p_in_stg_tbl_name,
                 piv_source_column_name  => p_in_column_name,
                 piv_source_column_value => p_in_agent_emp_no,
                 piv_source_keyname1     => null,
                 piv_source_keyvalue1    => null,
                 piv_error_type          => 'ERR_VAL',
                 piv_error_code          => l_err_code,
                 piv_error_message       => l_err_msg);
      return null;
  end xxpo_po_del_per_id;

  procedure create_new_site(piv_vendor_id            in number,
                            piv_vendor_site_id       in number,
                            piv_org                  in number,
                            piv_vend_site_code       varchar2,
                            pou_vendor_site_id       out number,
                            piv_purchasing_site_flag in varchar2,
                            piv_pay_site_flag        in varchar2,
                            piv_def_pay_site         in number,
                            piv_leg_po_header_id     in number,
                            /*pou_location_id OUT NUMBER,
                                                        pou_party_site_id OUT NUMBER*/
                            pou_error_code out varchar2,
                            pou_error_mssg out varchar2) is
    cursor source_ven_site_cur is
      select *
        from ap_supplier_sites_all
       where vendor_id = piv_vendor_id
         and vendor_site_id = piv_vendor_site_id;
    cursor source_bank_acct_cur is
      select ieba.*,
             aps.party_id              party_id_supp,
             assa.party_site_id        party_site_id_sup,
             ipiua.order_of_preference
        from ap_supplier_sites_all   assa,
             hz_parties              hp,
             iby_ext_bank_accounts   ieba,
             iby_external_payees_all iepa,
             iby_pmt_instr_uses_all  ipiua,
             ap_suppliers            aps,
             hz_parties              hp1
       where assa.vendor_site_id = iepa.supplier_site_id
         and hp.party_id = ieba.bank_id
         and ipiua.instrument_id = ieba.ext_bank_account_id
         and ipiua.ext_pmt_party_id = iepa.ext_payee_id
         and assa.vendor_id = aps.vendor_id
         and ieba.branch_id = hp1.party_id
         and ipiua.instrument_type = 'BANKACCOUNT'
         and ipiua.payment_flow = 'DISBURSEMENTS'
            --AND ipiua.order_of_preference   =      1
         and assa.vendor_site_id = piv_vendor_site_id
         and trunc(sysdate) between trunc(nvl(ieba.start_date, sysdate)) and
             trunc(nvl(ieba.end_date, sysdate));
    cursor ext_pymt_cur is
      select /*eppm.payment_method_code, assa.vendor_site_id,
                  assa.org_id,*/
       iepa.*
        from ap_supplier_sites_all   assa,
             ap_suppliers            sup,
             iby_external_payees_all iepa,
             iby_ext_party_pmt_mthds ieppm
       where sup.vendor_id = assa.vendor_id
         and assa.pay_site_flag = 'Y'
         and assa.vendor_site_id = iepa.supplier_site_id
         and iepa.ext_payee_id = ieppm.ext_pmt_party_id(+)
         and assa.vendor_site_id = piv_vendor_site_id
         and sup.vendor_id = piv_vendor_id;
    l_vendor_site_rec ap_vendor_pub_pkg.r_vendor_site_rec_type;
    x_vendor_site_id number;
    x_party_site_id  number;
    x_location_id    number;
    x_return_status  varchar2(2000);
    x_msg_count      number;
    x_msg_data       varchar2(2000);
    l_error_mssg     varchar2(2000);
    l_ext_bank_acct_rec      iby_ext_bankacct_pub.extbankacct_rec_type;
    l_instrument_rec         iby_fndcpt_setup_pub.pmtinstrument_rec_type;
    l_payee_rec              iby_disbursement_setup_pub.payeecontext_rec_type;
    l_assignment_attribs_rec iby_fndcpt_setup_pub.pmtinstrassignment_rec_type;
    l_bank_acct_id           iby_ext_bank_accounts.ext_bank_account_id%type;
    l_payee_err_msg          varchar2(2000);
    l_payee_msg_count        number;
    l_payee_ret_status       varchar2(50);
    l_assign_id              number;
    l_payee_result_rec       iby_fndcpt_common_pub.result_rec_type;
    --
    t_output                  varchar2(200) := null;
    t_msg_dummy               varchar2(200) := null;
    l_payee_upd_status        iby_disbursement_setup_pub.ext_payee_update_tab_type;
    l_external_payee_tab_type iby_disbursement_setup_pub.external_payee_tab_type;
    l_ext_payee_id_tab_type   iby_disbursement_setup_pub.ext_payee_id_tab_type;
    i                         number := 0;
    --
    function get_new_ccid(piv_leg_po_header_id in number,
                          piv_supp_acct_ccid   in number) return number is
      l_segment1  gl_code_combinations.segment1%type;
      l_segment2  gl_code_combinations.segment1%type;
      l_segment3  gl_code_combinations.segment1%type;
      l_segment4  gl_code_combinations.segment1%type;
      l_segment5  gl_code_combinations.segment1%type;
      l_segment6  gl_code_combinations.segment1%type;
      l_segment7  gl_code_combinations.segment1%type;
      l_segment8  gl_code_combinations.segment1%type;
      l_segment9  gl_code_combinations.segment1%type;
      l_segment10 gl_code_combinations.segment1%type;
      l_acct_ccid number := 0;
    begin
      select xpd.leg_charge_account_seg1
        into l_segment2
        from xxpo_po_distribution_stg xpd
       where leg_po_header_id = piv_leg_po_header_id
         and batch_id = g_batch_id
         and run_sequence_id = g_new_run_seq_id
         and rownum = 1;
      select le_number
        into l_segment1
        from xxetn_map_unit_v
       where site = l_segment2;
      --today
      /*fnd_file.put_line(fnd_file.log,'ccid for liab ' ||piv_liab_acct_ccid);
      fnd_file.put_line(fnd_file.log,'segment1 from liability ' ||l_segment1);
      fnd_file.put_line(fnd_file.log,'segment2 from liability ' ||l_segment2);*/
      --today
      select segment3,
             segment4,
             segment5,
             segment6,
             segment7,
             segment8,
             segment9,
             segment10
        into l_segment3,
             l_segment4,
             l_segment5,
             l_segment6,
             l_segment7,
             l_segment8,
             l_segment9,
             l_segment10
        from gl_code_combinations
       where code_combination_id = nvl(piv_supp_acct_ccid, 0);
      select code_combination_id
        into l_acct_ccid
        from gl_code_combinations
       where segment1 = l_segment1
         and segment2 = l_segment2
         and segment3 = l_segment3
         and segment4 = l_segment4
         and segment5 = l_segment5
         and segment6 = l_segment6
         and segment7 = l_segment7
         and segment8 = l_segment8
         and segment9 = l_segment9
         and segment10 = l_segment10;
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
      return l_acct_ccid;
    exception
      when others then
        --today
        /*fnd_file.put_line(fnd_file.log,'in exception for liab create site');*/
        l_acct_ccid := null;
        return l_acct_ccid;
    end;

  begin
    for source_site_rec in source_ven_site_cur loop
      l_vendor_site_rec.area_code                     := source_site_rec.area_code;
      l_vendor_site_rec.phone                         := source_site_rec.phone;
      l_vendor_site_rec.customer_num                  := source_site_rec.customer_num;
      l_vendor_site_rec.ship_to_location_id           := source_site_rec.ship_to_location_id;
      l_vendor_site_rec.bill_to_location_id           := source_site_rec.bill_to_location_id;
      l_vendor_site_rec.ship_via_lookup_code          := source_site_rec.ship_via_lookup_code;
      l_vendor_site_rec.freight_terms_lookup_code     := source_site_rec.freight_terms_lookup_code;
      l_vendor_site_rec.fob_lookup_code               := source_site_rec.fob_lookup_code;
      l_vendor_site_rec.inactive_date                 := source_site_rec.inactive_date;
      l_vendor_site_rec.fax                           := source_site_rec.fax;
      l_vendor_site_rec.fax_area_code                 := source_site_rec.fax_area_code;
      l_vendor_site_rec.telex                         := source_site_rec.telex;
      l_vendor_site_rec.terms_date_basis              := source_site_rec.terms_date_basis;
      l_vendor_site_rec.distribution_set_id           := source_site_rec.distribution_set_id;
      l_vendor_site_rec.accts_pay_code_combination_id := get_new_ccid(piv_leg_po_header_id,
                                                                      source_site_rec.accts_pay_code_combination_id);
      l_vendor_site_rec.prepay_code_combination_id    := get_new_ccid(piv_leg_po_header_id,
                                                                      source_site_rec.prepay_code_combination_id);
      l_vendor_site_rec.pay_group_lookup_code         := source_site_rec.pay_group_lookup_code;
      l_vendor_site_rec.payment_priority              := source_site_rec.payment_priority;
      l_vendor_site_rec.terms_id                      := source_site_rec.terms_id;
      l_vendor_site_rec.invoice_amount_limit          := source_site_rec.invoice_amount_limit;
      l_vendor_site_rec.pay_date_basis_lookup_code    := source_site_rec.pay_date_basis_lookup_code;
      l_vendor_site_rec.always_take_disc_flag         := source_site_rec.always_take_disc_flag;
      l_vendor_site_rec.invoice_currency_code         := source_site_rec.invoice_currency_code;
      l_vendor_site_rec.payment_currency_code         := source_site_rec.payment_currency_code;
      --l_vendor_site_rec.VENDOR_SITE_ID                  := source_site_rec.
      l_vendor_site_rec.last_update_date              := sysdate;
      l_vendor_site_rec.last_updated_by               := fnd_profile.value('USER_ID');
      l_vendor_site_rec.vendor_id                     := piv_vendor_id;
      l_vendor_site_rec.vendor_site_code              := piv_vend_site_code;
      l_vendor_site_rec.vendor_site_code_alt          := source_site_rec.vendor_site_code_alt;
      l_vendor_site_rec.purchasing_site_flag          := source_site_rec.purchasing_site_flag;
      l_vendor_site_rec.rfq_only_site_flag            := source_site_rec.rfq_only_site_flag;
      l_vendor_site_rec.pay_site_flag                 := source_site_rec.pay_site_flag;
      l_vendor_site_rec.attention_ar_flag             := source_site_rec.attention_ar_flag;
      l_vendor_site_rec.hold_all_payments_flag        := source_site_rec.hold_all_payments_flag;
      l_vendor_site_rec.hold_future_payments_flag     := source_site_rec.hold_future_payments_flag;
      l_vendor_site_rec.hold_reason                   := source_site_rec.hold_reason;
      l_vendor_site_rec.hold_unmatched_invoices_flag  := source_site_rec.hold_unmatched_invoices_flag;
      l_vendor_site_rec.tax_reporting_site_flag       := source_site_rec.tax_reporting_site_flag;
      l_vendor_site_rec.attribute_category            := source_site_rec.attribute_category;
      l_vendor_site_rec.attribute1                    := source_site_rec.attribute1;
      l_vendor_site_rec.attribute2                    := source_site_rec.attribute2;
      l_vendor_site_rec.attribute3                    := source_site_rec.attribute3;
      l_vendor_site_rec.attribute4                    := source_site_rec.attribute4;
      l_vendor_site_rec.attribute5                    := source_site_rec.attribute5;
      l_vendor_site_rec.attribute6                    := source_site_rec.attribute6;
      l_vendor_site_rec.attribute7                    := source_site_rec.attribute7;
      l_vendor_site_rec.attribute8                    := source_site_rec.attribute8;
      l_vendor_site_rec.attribute9                    := source_site_rec.attribute9;
      l_vendor_site_rec.attribute10                   := source_site_rec.attribute10;
      l_vendor_site_rec.attribute11                   := source_site_rec.attribute11;
      l_vendor_site_rec.attribute12                   := source_site_rec.attribute12;
      l_vendor_site_rec.attribute13                   := source_site_rec.attribute13;
      l_vendor_site_rec.attribute14                   := source_site_rec.attribute14;
      l_vendor_site_rec.attribute15                   := source_site_rec.attribute15;
      l_vendor_site_rec.validation_number             := source_site_rec.validation_number;
      l_vendor_site_rec.exclude_freight_from_discount := source_site_rec.exclude_freight_from_discount;
      l_vendor_site_rec.bank_charge_bearer            := source_site_rec.bank_charge_bearer;
      l_vendor_site_rec.org_id                        := piv_org;
      l_vendor_site_rec.check_digits                  := source_site_rec.check_digits;
      l_vendor_site_rec.allow_awt_flag                := source_site_rec.allow_awt_flag;
      l_vendor_site_rec.awt_group_id                  := source_site_rec.awt_group_id;
      l_vendor_site_rec.pay_awt_group_id              := source_site_rec.pay_awt_group_id;
      --only when pay_on_code is receipt and default site id is present for a purchasing site
      if piv_def_pay_site is not null then
        l_vendor_site_rec.default_pay_site_id := piv_def_pay_site;
      end if;
      l_vendor_site_rec.pay_on_code                 := source_site_rec.pay_on_code;
      l_vendor_site_rec.pay_on_receipt_summary_code := source_site_rec.pay_on_receipt_summary_code;
      l_vendor_site_rec.global_attribute_category   := source_site_rec.global_attribute_category;
      l_vendor_site_rec.global_attribute1           := source_site_rec.global_attribute1;
      l_vendor_site_rec.global_attribute2           := source_site_rec.global_attribute2;
      l_vendor_site_rec.global_attribute3           := source_site_rec.global_attribute3;
      l_vendor_site_rec.global_attribute4           := source_site_rec.global_attribute4;
      l_vendor_site_rec.global_attribute5           := source_site_rec.global_attribute5;
      l_vendor_site_rec.global_attribute6           := source_site_rec.global_attribute6;
      l_vendor_site_rec.global_attribute7           := source_site_rec.global_attribute7;
      l_vendor_site_rec.global_attribute8           := source_site_rec.global_attribute8;
      l_vendor_site_rec.global_attribute9           := source_site_rec.global_attribute9;
      l_vendor_site_rec.global_attribute10          := source_site_rec.global_attribute10;
      l_vendor_site_rec.global_attribute11          := source_site_rec.global_attribute11;
      l_vendor_site_rec.global_attribute12          := source_site_rec.global_attribute12;
      l_vendor_site_rec.global_attribute13          := source_site_rec.global_attribute13;
      l_vendor_site_rec.global_attribute14          := source_site_rec.global_attribute14;
      l_vendor_site_rec.global_attribute15          := source_site_rec.global_attribute15;
      l_vendor_site_rec.global_attribute16          := source_site_rec.global_attribute16;
      l_vendor_site_rec.global_attribute17          := source_site_rec.global_attribute17;
      l_vendor_site_rec.global_attribute18          := source_site_rec.global_attribute18;
      l_vendor_site_rec.global_attribute19          := source_site_rec.global_attribute19;
      l_vendor_site_rec.global_attribute20          := source_site_rec.global_attribute20;
      l_vendor_site_rec.tp_header_id                := source_site_rec.tp_header_id;
      l_vendor_site_rec.ece_tp_location_code        := source_site_rec.ece_tp_location_code;
      l_vendor_site_rec.pcard_site_flag             := source_site_rec.pcard_site_flag;
      l_vendor_site_rec.match_option                := source_site_rec.match_option;
      l_vendor_site_rec.country_of_origin_code      := source_site_rec.country_of_origin_code;
      if source_site_rec.future_dated_payment_ccid is null then
        l_vendor_site_rec.future_dated_payment_ccid := source_site_rec.future_dated_payment_ccid;
      else
        l_vendor_site_rec.future_dated_payment_ccid := get_new_ccid(piv_leg_po_header_id,
                                                                    source_site_rec.future_dated_payment_ccid);
      end if;
      l_vendor_site_rec.create_debit_memo_flag     := source_site_rec.create_debit_memo_flag;
      l_vendor_site_rec.supplier_notif_method      := source_site_rec.supplier_notif_method;
      l_vendor_site_rec.email_address              := source_site_rec.email_address;
      l_vendor_site_rec.primary_pay_site_flag      := source_site_rec.primary_pay_site_flag;
      l_vendor_site_rec.shipping_control           := source_site_rec.shipping_control;
      l_vendor_site_rec.selling_company_identifier := source_site_rec.selling_company_identifier;
      l_vendor_site_rec.gapless_inv_num_flag       := source_site_rec.gapless_inv_num_flag;
      l_vendor_site_rec.location_id                := source_site_rec.location_id;
      l_vendor_site_rec.party_site_id              := source_site_rec.party_site_id;
      --l_vendor_site_rec.ORG_NAME                       := source_site_rec.
      l_vendor_site_rec.duns_number       := source_site_rec.duns_number;
      l_vendor_site_rec.address_style     := source_site_rec.address_style;
      l_vendor_site_rec.language          := source_site_rec.language;
      l_vendor_site_rec.province          := source_site_rec.province;
      l_vendor_site_rec.country           := source_site_rec.country;
      l_vendor_site_rec.address_line1     := source_site_rec.address_line1;
      l_vendor_site_rec.address_line2     := source_site_rec.address_line2;
      l_vendor_site_rec.address_line3     := source_site_rec.address_line3;
      l_vendor_site_rec.address_line4     := source_site_rec.address_line4;
      l_vendor_site_rec.address_lines_alt := source_site_rec.address_lines_alt;
      l_vendor_site_rec.county            := source_site_rec.county;
      l_vendor_site_rec.city              := source_site_rec.city;
      l_vendor_site_rec.state             := source_site_rec.state;
      l_vendor_site_rec.zip               := source_site_rec.zip;
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
      l_vendor_site_rec.retainage_rate := source_site_rec.retainage_rate;
      --l_vendor_site_rec.SERVICES_TOLERANCE_ID    := source_site_rec.SERVICES_TOLERANCE_ID  ;
      --SERVICES_TOLERANCE_NAME         AP_TOLERANCE_TEMPLATES.TOLERANCE_NAME%TYPE,
      --SHIPPING_LOCATION_ID            NUMBER,
      l_vendor_site_rec.vat_code                   := source_site_rec.vat_code;
      l_vendor_site_rec.vat_registration_num       := source_site_rec.vat_registration_num;
      l_vendor_site_rec.remittance_email           := source_site_rec.remittance_email;
      l_vendor_site_rec.edi_id_number              := source_site_rec.edi_id_number;
      l_vendor_site_rec.edi_payment_format         := source_site_rec.edi_payment_format;
      l_vendor_site_rec.edi_transaction_handling   := source_site_rec.edi_transaction_handling;
      l_vendor_site_rec.edi_payment_method         := source_site_rec.edi_payment_method;
      l_vendor_site_rec.edi_remittance_method      := source_site_rec.edi_remittance_method;
      l_vendor_site_rec.edi_remittance_instruction := source_site_rec.edi_remittance_instruction;
      -- PARTY_SITE_NAME
      l_vendor_site_rec.offset_tax_flag    := source_site_rec.offset_tax_flag;
      l_vendor_site_rec.auto_tax_calc_flag := source_site_rec.auto_tax_calc_flag;
      --,REMIT_ADVICE_DELIVERY_METHOD
      --,REMIT_ADVICE_FAX
      l_vendor_site_rec.cage_code                := source_site_rec.cage_code;
      l_vendor_site_rec.legal_business_name      := source_site_rec.legal_business_name;
      l_vendor_site_rec.doing_bus_as_name        := source_site_rec.doing_bus_as_name;
      l_vendor_site_rec.division_name            := source_site_rec.division_name;
      l_vendor_site_rec.small_business_code      := source_site_rec.small_business_code;
      l_vendor_site_rec.ccr_comments             := source_site_rec.ccr_comments;
      l_vendor_site_rec.debarment_start_date     := source_site_rec.debarment_start_date;
      l_vendor_site_rec.debarment_end_date       := source_site_rec.debarment_end_date;
      l_vendor_site_rec.ap_tax_rounding_rule     := source_site_rec.ap_tax_rounding_rule;
      l_vendor_site_rec.amount_includes_tax_flag := source_site_rec.amount_includes_tax_flag;
      fnd_file.put_line(fnd_file.log, 'api called');
      fnd_file.put_line(fnd_file.log,
                        piv_vendor_id || '-' || piv_vendor_site_id || '-' ||
                        piv_org || '-' || piv_vend_site_code);
      mo_global.set_policy_context('S', piv_org);
      ap_vendor_pub_pkg.create_vendor_site(p_api_version     => 1,
                                           x_return_status   => x_return_status,
                                           x_msg_count       => x_msg_count,
                                           x_msg_data        => x_msg_data,
                                           p_vendor_site_rec => l_vendor_site_rec,
                                           x_vendor_site_id  => x_vendor_site_id,
                                           x_party_site_id   => x_party_site_id,
                                           x_location_id     => x_location_id);
      if (x_return_status <> 'S') then
        fnd_file.put_line(fnd_file.log,
                          'Error Creating Supplier site' || x_msg_data);
        pou_error_code := 'ETN_PO_VEN_SITE_CREATE_ERROR';
        --today
        fnd_file.put_line(fnd_file.log,
                          'ERR: source site :' || piv_vendor_site_id ||
                          ' Generated liabilty CCID :' ||
                          l_vendor_site_rec.accts_pay_code_combination_id ||
                          ' Prepay CCID :' ||
                          l_vendor_site_rec.prepay_code_combination_id ||
                          ' future pay CCID :' ||
                          l_vendor_site_rec.future_dated_payment_ccid);
        --today
        if x_msg_count > 1 then
          for i in 1 .. x_msg_count loop
            pou_error_mssg := substr(pou_error_mssg || '-' ||
                                     substr(fnd_msg_pub.get(p_encoded => fnd_api.g_false),
                                            1,
                                            155),
                                     1,
                                     155);
          end loop;
          /*ELSIF  x_msg_count = 1
          THEN
          pou_error_code := 'ETN_AP_SUPP_SITE_CREATE_ERROR';
          pou_error_mssg := x_msg_data;  */
        end if;
      end if;
    end loop;
    pou_vendor_site_id := x_vendor_site_id;
    if x_vendor_site_id is not null and nvl(piv_pay_site_flag, 'X') = 'Y' then
      for source_bank_acct_rec in source_bank_acct_cur loop
        l_assign_id                         := null;
        l_payee_ret_status                  := 'S';
        l_payee_rec.payment_function        := 'PAYABLES_DISB';
        l_payee_rec.party_id                := source_bank_acct_rec.party_id_supp;
        l_payee_rec.party_site_id           := source_bank_acct_rec.party_site_id_sup;
        l_payee_rec.supplier_site_id        := x_vendor_site_id;
        l_payee_rec.org_id                  := piv_org;
        l_payee_rec.org_type                := 'OPERATING_UNIT';
        l_instrument_rec.instrument_id      := source_bank_acct_rec.ext_bank_account_id;
        l_instrument_rec.instrument_type    := 'BANKACCOUNT';
        l_assignment_attribs_rec.start_date := sysdate;
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
        if (x_return_status <> 'S') then
          fnd_file.put_line(fnd_file.log,
                            'Encountered ERROR in supplier site bank creation!!!');
          fnd_file.put_line(fnd_file.log, sqlcode || ' ' || sqlerrm);
          fnd_file.put_line(fnd_file.log,
                            '--------------------------------------');
          fnd_file.put_line(fnd_file.log, x_msg_data);
          if x_msg_count > 1 then
            for i in 1 .. x_msg_count loop
              fnd_file.put_line(fnd_file.log,
                                substr(fnd_msg_pub.get(p_encoded => fnd_api.g_false),
                                       1,
                                       255));
              pou_error_code := 'ETN_PO_SUPSITE_BACT_ASGN_ERR';
              pou_error_mssg := substr(pou_error_mssg ||
                                       substr(fnd_msg_pub.get(p_encoded => fnd_api.g_false),
                                              1,
                                              155),
                                       1,
                                       155);
            end loop;
          end if;
        else
          fnd_file.put_line(fnd_file.log,
                            'SUPPLIER SITE BANK UPDATED SUCCESSFULLY AND BANK ID IS ' ||
                            source_bank_acct_rec.ext_bank_account_id);
        end if;
        commit;
      end loop;
      for crt_pay_rec in ext_pymt_cur loop
        i := 0;
        i := i + 1;
        l_external_payee_tab_type(i).payee_party_id := crt_pay_rec.payee_party_id;
        l_external_payee_tab_type(i).payment_function := crt_pay_rec.payment_function;
        l_external_payee_tab_type(i).exclusive_pay_flag := crt_pay_rec.exclusive_payment_flag;
        l_external_payee_tab_type(i).payee_party_site_id := crt_pay_rec.party_site_id;
        l_external_payee_tab_type(i).supplier_site_id := x_vendor_site_id;
        l_external_payee_tab_type(i).payer_org_id := piv_org;
        l_external_payee_tab_type(i).payer_org_type := crt_pay_rec.org_type;
        l_external_payee_tab_type(i).default_pmt_method := crt_pay_rec.default_payment_method_code;
        l_external_payee_tab_type(i).ece_tp_loc_code := crt_pay_rec.ece_tp_location_code;
        l_external_payee_tab_type(i).bank_charge_bearer := crt_pay_rec.bank_charge_bearer;
        l_external_payee_tab_type(i).bank_instr1_code := crt_pay_rec.bank_instruction1_code;
        l_external_payee_tab_type(i).bank_instr2_code := crt_pay_rec.bank_instruction2_code;
        l_external_payee_tab_type(i).bank_instr_detail := crt_pay_rec.bank_instruction_details;
        l_external_payee_tab_type(i).pay_reason_code := crt_pay_rec.payment_reason_code;
        l_external_payee_tab_type(i).pay_reason_com := crt_pay_rec.payment_reason_comments;
        l_external_payee_tab_type(i).inactive_date := crt_pay_rec.inactive_date;
        l_external_payee_tab_type(i).pay_message1 := crt_pay_rec.payment_text_message1;
        l_external_payee_tab_type(i).pay_message2 := crt_pay_rec.payment_text_message2;
        l_external_payee_tab_type(i).pay_message3 := crt_pay_rec.payment_text_message3;
        l_external_payee_tab_type(i).delivery_channel := crt_pay_rec.delivery_channel_code;
        --Pmt_Format            IBY_FORMATS_B.format_code%TYPE,
        l_external_payee_tab_type(i).settlement_priority := crt_pay_rec.settlement_priority;
        l_external_payee_tab_type(i).remit_advice_delivery_method := crt_pay_rec.remit_advice_delivery_method;
        l_external_payee_tab_type(i).remit_advice_email := crt_pay_rec.remit_advice_email;
        /*l_external_payee_tab_type(crt_pay_rec).edi_payment_format;
        l_external_payee_tab_type(crt_pay_rec).edi_transaction_handling;
        l_external_payee_tab_type(crt_pay_rec).edi_payment_method;
        l_external_payee_tab_type(crt_pay_rec).edi_remittance_method;
        l_external_payee_tab_type(crt_pay_rec).edi_remittance_instruction;
        */
        l_external_payee_tab_type(i).remit_advice_fax := crt_pay_rec.remit_advice_fax;
        fnd_file.put_line(fnd_file.log, crt_pay_rec.payee_party_id);
        fnd_file.put_line(fnd_file.log, crt_pay_rec.payment_function);
        fnd_file.put_line(fnd_file.log, crt_pay_rec.party_site_id);
        fnd_file.put_line(fnd_file.log, crt_pay_rec.supplier_site_id);
        fnd_file.put_line(fnd_file.log, crt_pay_rec.org_id);
        fnd_file.put_line(fnd_file.log, crt_pay_rec.org_type);
        --
        begin
          select ext_payee_id
            into l_ext_payee_id_tab_type(i).ext_payee_id
            from iby_external_payees_all
           where supplier_site_id = x_vendor_site_id;
          mo_global.set_policy_context('S', piv_org);
          iby_disbursement_setup_pub.update_external_payee(p_api_version          => 1.0,
                                                           p_init_msg_list        => 'T',
                                                           p_ext_payee_tab        => l_external_payee_tab_type,
                                                           p_ext_payee_id_tab     => l_ext_payee_id_tab_type,
                                                           x_return_status        => x_return_status,
                                                           x_msg_count            => x_msg_count,
                                                           x_msg_data             => x_msg_data,
                                                           x_ext_payee_status_tab => l_payee_upd_status);
          if x_return_status <> 'S' then
            if x_msg_count > 0 then
              for i in 1 .. x_msg_count loop
                pou_error_code := 'ETN_PO_SUPSITE_PAYMTHD_ERR';
                fnd_msg_pub.get(i,
                                fnd_api.g_false,
                                x_msg_data,
                                t_msg_dummy);
                --fnd_msg_pub.get (i, fnd_api.g_false, x_msg_data, t_msg_dummy);
                t_output := (to_char(i) || ': ' || x_msg_data);
              end loop;
              pou_error_mssg := t_output;
            end if;
            commit;
          end if;
        exception
          when no_data_found then
            pou_error_code := 'ETN_PO_SUPSITE_PAYMTHD_ERR';
            pou_error_mssg := 'Error Not able to Search Pay Method for new site';
        end;
      end loop;
    end if;
  exception
    when others then
      pou_error_code := 'ETN_PO_SUPSITE_CREATE_ERROR';
      pou_error_mssg := 'Error' || substr(sqlerrm, 1, 155);
  end;

  /*
     +================================================================================+
    |FUNCTION NAME      : xxpo_vendor_site_id                                  |
    |DESCRIPTION        : This function will return VENDOR_SITE_ID             |
    +================================================================================+
  */
  function xxpo_vendor_site_id(p_in_interface_txn_id in number,
                               p_in_stg_tbl_name     in varchar2,
                               p_in_column_name      in varchar2,
                               p_in_vendor_site_code in varchar2,
                               p_in_vendor_num       in varchar2,
                               p_in_oper_unit_id     in number,
                               p_in_leg_po_header_id in number) return number is
    lv_vendor_site_id         number;
    lv_vendor_id              number;
    lv_org_name               varchar2(240) := null;
    l_err_code                varchar2(40);
    l_err_msg                 varchar2(2000);
    lv_ven_site_id_pur        number;
    lv_ven_site_id_pay        number;
    lv_diff_vensite_org_id    number;
    lv_purchase_flag          varchar2(2);
    lv_pay_site_flag          varchar2(2);
    lv_pay_on_code            ap_supplier_sites_all.pay_on_code%type;
    lv_pay_ven_site_id_out    number;
    lv_yn_create_pay_site     varchar2(2);
    lv_existing_pay_site_id   number;
    lv_existing_pay_site_code ap_supplier_sites_all.vendor_site_code%type;
  begin
    select vendor_site_id
      into lv_vendor_site_id
      from ap_supplier_sites_all a, ap_suppliers b
     where b.segment1 = p_in_vendor_num
       and a.vendor_id = b.vendor_id
       and a.vendor_site_code = p_in_vendor_site_code
       and a.purchasing_site_flag = 'Y'
       and b.enabled_flag = 'Y'
       and org_id = p_in_oper_unit_id;
    return lv_vendor_site_id;
  exception
    when no_data_found then
      begin
        lv_ven_site_id_pur        := null;
        lv_ven_site_id_pay        := null;
        lv_diff_vensite_org_id    := null;
        lv_purchase_flag          := null;
        lv_pay_site_flag          := null;
        lv_vendor_id              := null;
        lv_yn_create_pay_site     := null;
        lv_existing_pay_site_id   := null;
        lv_existing_pay_site_code := null;
        select vendor_site_id,
               a.org_id,
               a.default_pay_site_id,
               a.purchasing_site_flag,
               a.pay_site_flag,
               a.pay_on_code,
               b.vendor_id
          into lv_ven_site_id_pur,
               lv_diff_vensite_org_id,
               lv_ven_site_id_pay,
               lv_purchase_flag,
               lv_pay_site_flag,
               lv_pay_on_code,
               lv_vendor_id
          from ap_supplier_sites_all a, ap_suppliers b
         where b.segment1 = p_in_vendor_num
           and a.vendor_id = b.vendor_id
           and a.vendor_site_code = p_in_vendor_site_code
           and b.enabled_flag = 'Y'
           and a.purchasing_site_flag = 'Y'
           and rownum = 1;
        fnd_file.put_line(fnd_file.log,
                          'A1' || lv_ven_site_id_pur || '-' ||
                          lv_diff_vensite_org_id || '-' ||
                          lv_ven_site_id_pay || '-' || lv_vendor_id);
        --22/todaty/2016 IF NVL(lv_pay_on_code,'X') <> 'RECEIPT'
        --22/todaty/2016 OR lv_pay_site_flag = 'Y'
        if lv_ven_site_id_pay is null
        --22/todaty/2016 end here
         then
          fnd_file.put_line(fnd_file.log,
                            'A1.1' || lv_pay_on_code || '-' ||
                            lv_pay_site_flag);
          --create only one site
          --it could be purchasing site only or a paysite as well
          create_new_site(lv_vendor_id,
                          lv_ven_site_id_pur,
                          p_in_oper_unit_id,
                          p_in_vendor_site_code,
                          lv_vendor_site_id,
                          lv_purchase_flag,
                          lv_pay_site_flag,
                          null,
                          p_in_leg_po_header_id,
                          l_err_code,
                          l_err_msg);
          if l_err_code is not null then
            log_errors(pin_interface_txn_id    => p_in_interface_txn_id,
                       piv_source_table        => p_in_stg_tbl_name,
                       piv_source_column_name  => p_in_column_name,
                       piv_source_column_value => p_in_vendor_site_code,
                       piv_source_keyname1     => null,
                       piv_source_keyvalue1    => null,
                       piv_error_type          => 'ERR_VAL',
                       piv_error_code          => l_err_code,
                       piv_error_message       => l_err_msg);
            return null;
          else
            return lv_vendor_site_id;
          end if;
        else
          --check if pay site exists then use it and create purchasing site
          --Else create a pay site and then create purchasing site
          if lv_ven_site_id_pay is not null then
            begin
              select vendor_site_id
                into lv_existing_pay_site_id
                from ap_supplier_sites_all apsa
               where vendor_id = lv_vendor_id
                 and apsa.vendor_site_code =
                     (select vendor_site_code
                        from ap_supplier_sites_all
                       where vendor_id = lv_vendor_id
                         and vendor_site_id = lv_ven_site_id_pay)
                 and org_id = p_in_oper_unit_id
                 and pay_site_flag = 'Y';
              lv_yn_create_pay_site  := 'Y';
              lv_pay_ven_site_id_out := lv_existing_pay_site_id;
              fnd_file.put_line(fnd_file.log,
                                'A1.2' || 'Pay site exist' || '-' ||
                                lv_existing_pay_site_id);
            exception
              when others then
                lv_yn_create_pay_site := 'N';
                begin
                  select vendor_site_code
                    into lv_existing_pay_site_code
                    from ap_supplier_sites_all
                   where vendor_id = lv_vendor_id
                     and vendor_site_id = lv_ven_site_id_pay;
                  fnd_file.put_line(fnd_file.log,
                                    'A1.3' || 'Pay site exist' || '-' ||
                                    lv_existing_pay_site_code);
                exception
                  when others then
                    l_err_code := 'ETN_PO_INVALID_VENDOR_SITE_CODE';
                    l_err_msg  := 'Error: EXCEPTION Original Default Pay Site ID not Derived ';
                    log_errors(pin_interface_txn_id    => p_in_interface_txn_id,
                               piv_source_table        => p_in_stg_tbl_name,
                               piv_source_column_name  => p_in_column_name,
                               piv_source_column_value => p_in_vendor_site_code,
                               piv_source_keyname1     => null,
                               piv_source_keyvalue1    => null,
                               piv_error_type          => 'ERR_VAL',
                               piv_error_code          => l_err_code,
                               piv_error_message       => l_err_msg);
                    return null;
                end;
            end;
            --create pay site
            if lv_yn_create_pay_site = 'N' then
              create_new_site(lv_vendor_id,
                              lv_ven_site_id_pay,
                              p_in_oper_unit_id,
                              lv_existing_pay_site_code,
                              lv_pay_ven_site_id_out,
                              lv_purchase_flag,
                              'Y',
                              null,
                              p_in_leg_po_header_id,
                              l_err_code,
                              l_err_msg);
              fnd_file.put_line(fnd_file.log,
                                'A1.4' || 'Pay site exist' || '-' ||
                                lv_pay_ven_site_id_out);
            end if;
            if lv_pay_ven_site_id_out is not null and l_err_code is null then
              --create purchasing site
              --use the default pay site fetched from above and pass
              --use the purchase vendor site id for this vendor as base
              create_new_site(lv_vendor_id,
                              lv_ven_site_id_pur,
                              p_in_oper_unit_id,
                              p_in_vendor_site_code,
                              lv_vendor_site_id,
                              lv_purchase_flag,
                              lv_pay_site_flag,
                              lv_pay_ven_site_id_out,
                              p_in_leg_po_header_id,
                              l_err_code,
                              l_err_msg);
              if lv_vendor_site_id is null or l_err_code is not null then
                l_err_msg := l_err_msg || ' - Purchasing Site not Created';
                log_errors(pin_interface_txn_id    => p_in_interface_txn_id,
                           piv_source_table        => p_in_stg_tbl_name,
                           piv_source_column_name  => p_in_column_name,
                           piv_source_column_value => p_in_vendor_site_code,
                           piv_source_keyname1     => null,
                           piv_source_keyvalue1    => null,
                           piv_error_type          => 'ERR_VAL',
                           piv_error_code          => l_err_code,
                           piv_error_message       => l_err_msg);
                return null;
              else
                return lv_vendor_site_id;
              end if;
            else
              --pay site could not be created
              --dont proceed to create purchasing site
              l_err_msg := l_err_msg || ' - Default Pay Site not Created';
              log_errors(pin_interface_txn_id    => p_in_interface_txn_id,
                         piv_source_table        => p_in_stg_tbl_name,
                         piv_source_column_name  => p_in_column_name,
                         piv_source_column_value => p_in_vendor_site_code,
                         piv_source_keyname1     => null,
                         piv_source_keyvalue1    => null,
                         piv_error_type          => 'ERR_VAL',
                         piv_error_code          => l_err_code,
                         piv_error_message       => l_err_msg);
              return null;
            end if;
          end if;
        end if;
      exception
        when no_data_found then
          l_err_code := 'ETN_PO_INVALID_VENDOR_SITE_CODE';
          l_err_msg  := 'Error: No Purchasing Site found for Vendor in Any OU ';
          log_errors(pin_interface_txn_id    => p_in_interface_txn_id,
                     piv_source_table        => p_in_stg_tbl_name,
                     piv_source_column_name  => p_in_column_name,
                     piv_source_column_value => p_in_vendor_site_code,
                     piv_source_keyname1     => null,
                     piv_source_keyvalue1    => null,
                     piv_error_type          => 'ERR_VAL',
                     piv_error_code          => l_err_code,
                     piv_error_message       => l_err_msg);
          return null;
      end;
    when others then
      l_err_code := 'ETN_PO_INVALID_VENDOR_SITE_CODE';
      l_err_msg  := 'Error: EXCEPTION error while deriving vendor_site_code. ';
      log_errors(pin_interface_txn_id    => p_in_interface_txn_id,
                 piv_source_table        => p_in_stg_tbl_name,
                 piv_source_column_name  => p_in_column_name,
                 piv_source_column_value => p_in_vendor_site_code,
                 piv_source_keyname1     => null,
                 piv_source_keyvalue1    => null,
                 piv_error_type          => 'ERR_VAL',
                 piv_error_code          => l_err_code,
                 piv_error_message       => l_err_msg);
      return null;
  end xxpo_vendor_site_id;

  /* ---1.1
  +================================================================================+
    |FUNCTION NAME      : xxpo_SHIP_TO_LOC_ID                                  |
    |DESCRIPTION        : This function will return SHIP_TO_LOCATION_ID        |
  +================================================================================+
     */
  /*  FUNCTION xxpo_ship_to_loc_id (
     p_in_interface_txn_id   IN   NUMBER,
     p_in_stg_tbl_name       IN   VARCHAR2,
     p_in_column_name        IN   VARCHAR2,
     p_in_ship_to_loc        IN   VARCHAR2
  )
     RETURN NUMBER
  IS
     lv_ship_to_loc_id   NUMBER;
     l_err_code          VARCHAR2 (40);
     l_err_msg           VARCHAR2 (2000);
  BEGIN
     SELECT ship_to_location_id
       INTO lv_ship_to_loc_id
       FROM hr_locations
      WHERE (ship_to_site_flag = 'Y' OR ship_to_location_id IS NOT NULL)
        AND receiving_site_flag = 'Y'
        AND location_code = p_in_ship_to_loc
        AND SYSDATE <= NVL (inactive_date, SYSDATE);

     RETURN lv_ship_to_loc_id;
  EXCEPTION
     WHEN NO_DATA_FOUND
     THEN
        l_err_code := 'ETN_PO_INVALID_SHIP_TO_LOCATION';
        l_err_msg := 'Error: Invalid Ship to Location. ';
        log_errors (pin_interface_txn_id         => p_in_interface_txn_id,
                    piv_source_table             => p_in_stg_tbl_name,
                    piv_source_column_name       => p_in_column_name,
                    piv_source_column_value      => p_in_ship_to_loc,
                    piv_source_keyname1          => NULL,
                    piv_source_keyvalue1         => NULL,
                    piv_error_type               => 'ERR_VAL',
                    piv_error_code               => l_err_code,
                    piv_error_message            => l_err_msg
                   );
        RETURN NULL;
     WHEN OTHERS
     THEN
        l_err_code := 'ETN_PO_INVALID_SHIP_TO_LOCATION';
        l_err_msg :=
                'Error: EXCEPTION error while deriving ship_to_location_id ';
        log_errors (pin_interface_txn_id         => p_in_interface_txn_id,
                    piv_source_table             => p_in_stg_tbl_name,
                    piv_source_column_name       => p_in_column_name,
                    piv_source_column_value      => p_in_ship_to_loc,
                    piv_source_keyname1          => NULL,
                    piv_source_keyvalue1         => NULL,
                    piv_error_type               => 'ERR_VAL',
                    piv_error_code               => l_err_code,
                    piv_error_message            => l_err_msg
                   );
        RETURN NULL;
  END xxpo_ship_to_loc_id; */
  /*
  +================================================================================+
    |FUNCTION NAME      : xxpo_SHIP_TO_LOC_ID                                  |
    |DESCRIPTION        : This function will return SHIP_TO_LOCATION_ID        |
  +================================================================================+
     */
  function xxpo_delv_to_loc_id(p_in_interface_txn_id in number,
                               p_in_stg_tbl_name     in varchar2,
                               p_in_column_name      in varchar2,
                               p_in_del_to_loc       in varchar2)
    return number is
    lv_dev_to_loc_id number;
    l_err_code       varchar2(40);
    l_err_msg        varchar2(2000);
  begin
    select location_id
      into lv_dev_to_loc_id
      from hr_locations
     where (ship_to_site_flag = 'Y' or ship_to_location_id is not null)
       and receiving_site_flag = 'Y'
       and location_code = p_in_del_to_loc
       and sysdate <= nvl(inactive_date, sysdate);
    return lv_dev_to_loc_id;
  exception
    when no_data_found then
      l_err_code := 'ETN_PO_INVALID_DELIVER_TO_LOCN';
      l_err_msg  := 'Error: Invalid Deliver to Location code. ';
      log_errors(pin_interface_txn_id    => p_in_interface_txn_id,
                 piv_source_table        => p_in_stg_tbl_name,
                 piv_source_column_name  => p_in_column_name,
                 piv_source_column_value => p_in_del_to_loc,
                 piv_source_keyname1     => null,
                 piv_source_keyvalue1    => null,
                 piv_error_type          => 'ERR_VAL',
                 piv_error_code          => l_err_code,
                 piv_error_message       => l_err_msg);
      return null;
    when others then
      l_err_code := 'ETN_PO_INVALID_SHIP_TO_LOCATION';
      l_err_msg  := 'Error: EXCEPTION error while deriving Deliver to Location ';
      log_errors(pin_interface_txn_id    => p_in_interface_txn_id,
                 piv_source_table        => p_in_stg_tbl_name,
                 piv_source_column_name  => p_in_column_name,
                 piv_source_column_value => p_in_del_to_loc,
                 piv_source_keyname1     => null,
                 piv_source_keyvalue1    => null,
                 piv_error_type          => 'ERR_VAL',
                 piv_error_code          => l_err_code,
                 piv_error_message       => l_err_msg);
      return null;
  end xxpo_delv_to_loc_id;

  /* ---1.1
   +================================================================================+
  |FUNCTION NAME      : xxpo_BILL_TO_LOC_ID                                  |
  |DESCRIPTION        : This function will return BILL_TO_LOCATION_ID        |
  +================================================================================+
  */
  /*    FUNCTION xxpo_bill_to_loc_id (
     p_in_interface_txn_id   IN   NUMBER,
     p_in_stg_tbl_name       IN   VARCHAR2,
     p_in_column_name        IN   VARCHAR2,
     p_in_bill_to_loc_code   IN   VARCHAR2
  )
     RETURN NUMBER
  IS
     lv_bill_to_loc_id   NUMBER;
     l_err_code          VARCHAR2 (40);
     l_err_msg           VARCHAR2 (2000);
  BEGIN
     SELECT location_id
       INTO lv_bill_to_loc_id
       FROM hr_locations
      WHERE location_code = p_in_bill_to_loc_code
        AND bill_to_site_flag = 'Y'
        AND SYSDATE <= NVL (inactive_date, SYSDATE);

     RETURN lv_bill_to_loc_id;
  EXCEPTION
     WHEN NO_DATA_FOUND
     THEN
        l_err_code := 'ETN_PO_INVALID_BILL_TO_LOCATION';
        l_err_msg := 'Error: Invalid bill to Location. ';
        log_errors (pin_interface_txn_id         => p_in_interface_txn_id,
                    piv_source_table             => p_in_stg_tbl_name,
                    piv_source_column_name       => p_in_column_name,
                    piv_source_column_value      => p_in_bill_to_loc_code,
                    piv_source_keyname1          => NULL,
                    piv_source_keyvalue1         => NULL,
                    piv_error_type               => 'ERR_VAL',
                    piv_error_code               => l_err_code,
                    piv_error_message            => l_err_msg
                   );
        RETURN NULL;
     WHEN OTHERS
     THEN
        l_err_code := 'ETN_PO_INVALID_BILL_TO_LOCATION';
        l_err_msg :=
                'Error: EXCEPTION error while deriving bill_to_location_id ';
        log_errors (pin_interface_txn_id         => p_in_interface_txn_id,
                    piv_source_table             => p_in_stg_tbl_name,
                    piv_source_column_name       => p_in_column_name,
                    piv_source_column_value      => p_in_bill_to_loc_code,
                    piv_source_keyname1          => NULL,
                    piv_source_keyvalue1         => NULL,
                    piv_error_type               => 'ERR_VAL',
                    piv_error_code               => l_err_code,
                    piv_error_message            => l_err_msg
                   );
        RETURN NULL;
  END xxpo_bill_to_loc_id; */
  /* --1.1
   +================================================================================+
  |PROCEDURE         : xxpo_TERMS_ID                                         |
  |DESCRIPTION        : This function will return TERMS_ID                   |
  +================================================================================+
     */
  /* PROCEDURE xxpo_terms_id (
     p_in_interface_txn_id   IN       NUMBER,
     p_in_stg_tbl_name       IN       VARCHAR2,
     p_in_column_name        IN       VARCHAR2,
     p_in_terms_name         IN       VARCHAR2,
     p_pay_name              OUT      VARCHAR2,
     p_pay_id                OUT      NUMBER
  )
  IS
     l_err_code   VARCHAR2 (40);
     l_err_msg    VARCHAR2 (2000);
  BEGIN
     p_pay_id := NULL;
     p_pay_name := NULL;

     BEGIN
        SELECT TRIM (flv.description)
          INTO p_pay_name
          FROM fnd_lookup_values flv
         WHERE TRIM (UPPER (flv.meaning)) =
                                               TRIM (UPPER (p_in_terms_name))
           AND flv.LANGUAGE = USERENV ('LANG')
           AND flv.enabled_flag = 'Y'
           AND UPPER (flv.lookup_type) = g_payment_term_lookup
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
           l_err_code := 'ETN_PO_PAYMENT_TERM_ERROR';
           l_err_msg :=
              'Error: Mapping not defined in the Common Payment term lookup. ';
           log_errors (pin_interface_txn_id         => p_in_interface_txn_id,
                       piv_source_table             => p_in_stg_tbl_name,
                       piv_source_column_name       => p_in_column_name,
                       piv_source_column_value      => p_in_terms_name,
                       piv_source_keyname1          => NULL,
                       piv_source_keyvalue1         => NULL,
                       piv_error_type               => 'ERR_VAL',
                       piv_error_code               => l_err_code,
                       piv_error_message            => l_err_msg
                      );
        WHEN OTHERS
        THEN
           l_err_code := 'ETN_PO_PAYMENT_TERM_ERROR';
           l_err_msg :=
              'Error: Exception error while deriving payment term from Common Payment term lookup. ';
           log_errors (pin_interface_txn_id         => p_in_interface_txn_id,
                       piv_source_table             => p_in_stg_tbl_name,
                       piv_source_column_name       => p_in_column_name,
                       piv_source_column_value      => p_in_terms_name,
                       piv_source_keyname1          => NULL,
                       piv_source_keyvalue1         => NULL,
                       piv_error_type               => 'ERR_VAL',
                       piv_error_code               => l_err_code,
                       piv_error_message            => l_err_msg
                      );
     END;

     BEGIN
        SELECT term_id
          INTO p_pay_id
          FROM ap_terms
         WHERE NAME = p_pay_name
           AND SYSDATE BETWEEN NVL (start_date_active, SYSDATE)
                           AND NVL (end_date_active, SYSDATE)
           AND enabled_flag = 'Y';
     EXCEPTION
        WHEN NO_DATA_FOUND
        THEN
           l_err_code := 'ETN_PO_PAYMENT_TERM_ERROR';
           l_err_msg := 'Error: Payment term not defined in the system. ';
           log_errors (pin_interface_txn_id         => p_in_interface_txn_id,
                       piv_source_table             => p_in_stg_tbl_name,
                       piv_source_column_name       => p_in_column_name,
                       piv_source_column_value      => p_in_terms_name,
                       piv_source_keyname1          => NULL,
                       piv_source_keyvalue1         => NULL,
                       piv_error_type               => 'ERR_VAL',
                       piv_error_code               => l_err_code,
                       piv_error_message            => l_err_msg
                      );
        WHEN OTHERS
        THEN
           l_err_code := 'ETN_PO_PAYMENT_TERM_ERROR';
           l_err_msg :=
              'Error: Exception error while deriving payment term id from system. ';
           log_errors (pin_interface_txn_id         => p_in_interface_txn_id,
                       piv_source_table             => p_in_stg_tbl_name,
                       piv_source_column_name       => p_in_column_name,
                       piv_source_column_value      => p_in_terms_name,
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
        l_err_code := 'ETN_PO_PROCEDURE_EXCEPTION';
        l_err_msg := 'Error: EXCEPTION error for xxpo_terms_id procedure. ';
        log_errors (pin_interface_txn_id         => p_in_interface_txn_id,
                    piv_source_table             => p_in_stg_tbl_name,
                    piv_source_column_name       => p_in_column_name,
                    piv_source_column_value      => p_in_terms_name,
                    piv_source_keyname1          => NULL,
                    piv_source_keyvalue1         => NULL,
                    piv_error_type               => 'ERR_VAL',
                    piv_error_code               => l_err_code,
                    piv_error_message            => l_err_msg
                   );
  END xxpo_terms_id; */
  /*
   +================================================================================+
  |FUNCTION NAME      : PO_VEN_CONTACT_ID                                    |
  |DESCRIPTION        : This function will derive VENDOR_CONTACT_ID          |
  +================================================================================+
     */
  function xxpo_po_ven_contact_id(p_in_interface_txn_id     in number,
                                  p_in_stg_tbl_name         in varchar2,
                                  p_in_column_name          in varchar2,
                                  p_in_vendor_id            in number,
                                  p_in_vendor_contact_fname in varchar2,
                                  p_in_vendor_contact_lname in varchar2,
                                  p_in_vendor_site_id       in number)
    return number is
    l_vendor_name        varchar2(240);
    l_err_code           varchar2(40);
    l_err_msg            varchar2(2000);
    lv_vendor_contact_id number := null;
  begin
    begin
      select vendor_contact_id
        into lv_vendor_contact_id
        from po_vendor_contacts
       where first_name = p_in_vendor_contact_fname
         and last_name = p_in_vendor_contact_lname
         and vendor_site_id = p_in_vendor_site_id
         and vendor_id = p_in_vendor_id
         and (sysdate) <= nvl(inactive_date, sysdate);
      return lv_vendor_contact_id;
    exception
      when no_data_found then
        l_err_code := 'ETN_PO_INVALID_VENDOR_CNCT_NAME';
        l_err_msg  := 'Error: Invalid vendor contact information. ';
        log_errors(pin_interface_txn_id    => p_in_interface_txn_id,
                   piv_source_table        => p_in_stg_tbl_name,
                   piv_source_column_name  => p_in_column_name,
                   piv_source_column_value => p_in_vendor_contact_fname,
                   piv_source_keyname1     => null,
                   piv_source_keyvalue1    => null,
                   piv_error_type          => 'ERR_VAL',
                   piv_error_code          => l_err_code,
                   piv_error_message       => l_err_msg);
        return null;
      when others then
        l_err_code := 'ETN_PO_INVALID_VENDOR_CNCT_NAME';
        l_err_msg  := 'Error: Exception error while deriving vendor contact id. ';
        log_errors(pin_interface_txn_id    => p_in_interface_txn_id,
                   piv_source_table        => p_in_stg_tbl_name,
                   piv_source_column_name  => p_in_column_name,
                   piv_source_column_value => p_in_vendor_contact_fname,
                   piv_source_keyname1     => null,
                   piv_source_keyvalue1    => null,
                   piv_error_type          => 'ERR_VAL',
                   piv_error_code          => l_err_code,
                   piv_error_message       => l_err_msg);
        return null;
    end;
  end xxpo_po_ven_contact_id;

  /*
  +================================================================================+
  |FUNCTION NAME : PO_SHIPMENT_TYPE                                        |
  |DESCRIPTION   : This function will check/validate SHIPMENT_TYPE           |
  +================================================================================+
  */
  function xxpo_po_shipment_type(p_in_interface_txn_id in number,
                                 p_in_stg_tbl_name     in varchar2,
                                 p_in_column_name      in varchar2,
                                 p_in_shipment_type    in varchar2)
    return varchar2 is
    lv_status  varchar2(1) := 'N';
    l_err_code varchar2(40);
    l_err_msg  varchar2(2000);
  begin
    select 'N'
      into lv_status
      from fnd_lookup_values flv
     where lookup_type = 'SHIPMENT TYPE'
       and trunc(sysdate) between
           trunc(nvl(flv.start_date_active, sysdate)) and
           trunc(nvl(flv.end_date_active, sysdate))
       and enabled_flag = 'Y'
       and lookup_code = p_in_shipment_type
       and p_in_shipment_type = 'STANDARD'
       and rownum = 1;
    return lv_status;
  exception
    when no_data_found then
      l_err_code := 'ETN_PO_INVALID_SHIPMENT_TYPE';
      l_err_msg  := 'Error: Invalid shipment type value. ';
      log_errors(pin_interface_txn_id    => p_in_interface_txn_id,
                 piv_source_table        => p_in_stg_tbl_name,
                 piv_source_column_name  => p_in_column_name,
                 piv_source_column_value => p_in_shipment_type,
                 piv_source_keyname1     => null,
                 piv_source_keyvalue1    => null,
                 piv_error_type          => 'ERR_VAL',
                 piv_error_code          => l_err_code,
                 piv_error_message       => l_err_msg);
      lv_status := 'Y';
      return lv_status;
    when others then
      l_err_code := 'ETN_PO_INVALID_SHIPMENT_TYPE';
      l_err_msg  := 'Error: EXCEPTION error while validating shipment type. ';
      log_errors(pin_interface_txn_id    => p_in_interface_txn_id,
                 piv_source_table        => p_in_stg_tbl_name,
                 piv_source_column_name  => p_in_column_name,
                 piv_source_column_value => p_in_shipment_type,
                 piv_source_keyname1     => null,
                 piv_source_keyvalue1    => null,
                 piv_error_type          => 'ERR_VAL',
                 piv_error_code          => l_err_code,
                 piv_error_message       => l_err_msg);
      lv_status := 'Y';
      return lv_status;
  end xxpo_po_shipment_type;

  /* --1.1
  +================================================================================+
  |FUNCTION NAME : PO_PRICE_TYPE                                        |
  |DESCRIPTION   : This function will check/validate PRICE_TYPE           |
  +================================================================================+
  */
  /*    FUNCTION xxpo_po_price_type (
     p_in_interface_txn_id   IN   NUMBER,
     p_in_stg_tbl_name       IN   VARCHAR2,
     p_in_column_name        IN   VARCHAR2,
     p_in_price_type         IN   VARCHAR2
  )
     RETURN VARCHAR2
  IS
     lv_status    VARCHAR2 (1)    := 'N';
     l_err_code   VARCHAR2 (40);
     l_err_msg    VARCHAR2 (2000);
  BEGIN
     SELECT 'N'
       INTO lv_status
       FROM fnd_lookup_values flv
      WHERE lookup_type = 'PRICE TYPE'
        AND UPPER (lookup_code) = UPPER (p_in_price_type)
        AND flv.LANGUAGE = USERENV ('LANG')
        AND TRUNC (SYSDATE) BETWEEN TRUNC (NVL (flv.start_date_active,
                                                SYSDATE
                                               )
                                          )
                                AND TRUNC (NVL (flv.end_date_active, SYSDATE))
        AND enabled_flag = 'Y';

     RETURN lv_status;
  EXCEPTION
     WHEN NO_DATA_FOUND
     THEN
        l_err_code := 'ETN_PO_INVALID_PRICE_TYPE';
        l_err_msg := 'Error: Invalid price type value. ';
        log_errors (pin_interface_txn_id         => p_in_interface_txn_id,
                    piv_source_table             => p_in_stg_tbl_name,
                    piv_source_column_name       => p_in_column_name,
                    piv_source_column_value      => p_in_price_type,
                    piv_source_keyname1          => NULL,
                    piv_source_keyvalue1         => NULL,
                    piv_error_type               => 'ERR_VAL',
                    piv_error_code               => l_err_code,
                    piv_error_message            => l_err_msg
                   );
        lv_status := 'Y';
        RETURN lv_status;
     WHEN OTHERS
     THEN
        l_err_code := 'ETN_PO_INVALID_PRICE_TYPE';
        l_err_msg :=
                  'Error: EXCEPTION error while fetching price type value. ';
        log_errors (pin_interface_txn_id         => p_in_interface_txn_id,
                    piv_source_table             => p_in_stg_tbl_name,
                    piv_source_column_name       => p_in_column_name,
                    piv_source_column_value      => p_in_price_type,
                    piv_source_keyname1          => NULL,
                    piv_source_keyvalue1         => NULL,
                    piv_error_type               => 'ERR_VAL',
                    piv_error_code               => l_err_code,
                    piv_error_message            => l_err_msg
                  );
        lv_status := 'Y';
        RETURN lv_status;
  END xxpo_po_price_type; */
  /*
  +================================================================================+
  |PROCEDURE          : xxpo_po_project_id                                       |
  |DESCRIPTION        : This function will derive PROJECT_ID                 |
  +================================================================================+
  */
  procedure xxpo_po_project_id(p_in_interface_txn_id in number,
                               p_in_stg_tbl_name     in varchar2,
                               p_in_column_name      in varchar2,
                               p_in_project_name     in varchar2,
                               p_in_exp_date         in date,
                               p_out_prj_id          out number,
                               p_out_prj_type        out varchar2) is
    l_err_code varchar2(40);
    l_err_msg  varchar2(2000);
  begin
    p_out_prj_id   := null;
    p_out_prj_type := null;
    select a.project_id, b.class_code
      into p_out_prj_id, p_out_prj_type
      from pa_projects_all a, pa_project_classes b
     where name = p_in_project_name
       and a.project_id = b.project_id
       and b.class_category = 'Project Type'
       and p_in_exp_date between nvl(start_date, sysdate) and
           nvl(closed_date, sysdate)
       and enabled_flag = 'Y';
  exception
    when others then
      l_err_code := 'ETN_PO_INVALID_PROJECT';
      l_err_msg  := 'Error: Invalid project value. ';
      log_errors(pin_interface_txn_id    => p_in_interface_txn_id,
                 piv_source_table        => p_in_stg_tbl_name,
                 piv_source_column_name  => p_in_column_name,
                 piv_source_column_value => p_in_project_name,
                 piv_source_keyname1     => null,
                 piv_source_keyvalue1    => null,
                 piv_error_type          => 'ERR_VAL',
                 piv_error_code          => l_err_code,
                 piv_error_message       => l_err_msg);
  end xxpo_po_project_id;

  /*
  +================================================================================+
  |FUNCTION NAME      : xxpo_po_task_id                                       |
  |DESCRIPTION        : This function will derive TASK_ID                  |
  +================================================================================+
  */
  function xxpo_po_task_id(p_in_interface_txn_id in number,
                           p_in_stg_tbl_name     in varchar2,
                           p_in_column_name      in varchar2,
                           p_in_proj_name        in varchar2,
                           p_in_proj_type        in varchar2,
                           p_in_exp_date         in date) return number is
    lv_task_id number := null;
    lv_task    varchar2(25) := null;
    l_err_code varchar2(40);
    l_err_msg  varchar2(2000);
  begin
    begin
      select tag
        into lv_task
        from fnd_lookup_values flv
       where lookup_type = g_task_lookup
         and flv.language = userenv('LANG')
         and trunc(sysdate) between
             trunc(nvl(flv.start_date_active, sysdate)) and
             trunc(nvl(flv.end_date_active, sysdate))
         and enabled_flag = 'Y'
         and meaning = p_in_proj_type;
    exception
      when others then
        l_err_code := 'ETN_PO_INVALID_TASK_NUMBER';
        l_err_msg  := 'Error: Task Number could not be derived from XXETN_PA_TASK_MAPPING lookup. ';
        log_errors(pin_interface_txn_id    => p_in_interface_txn_id,
                   piv_source_table        => p_in_stg_tbl_name,
                   piv_source_column_name  => p_in_column_name,
                   piv_source_column_value => p_in_proj_name,
                   piv_source_keyname1     => null,
                   piv_source_keyvalue1    => null,
                   piv_error_type          => 'ERR_VAL',
                   piv_error_code          => l_err_code,
                   piv_error_message       => l_err_msg);
        return null;
    end;
    begin
      select task_id
        into lv_task_id
        from pa_tasks pt, pa_projects_all pp
       where pt.task_number = lv_task
         and pp.name = p_in_proj_name
         and pp.project_id = pt.project_id
         and p_in_exp_date between nvl(pp.start_date, sysdate) and
             nvl(pp.closed_date, sysdate)
         and pp.enabled_flag = 'Y'
         and p_in_exp_date between nvl(pt.start_date, sysdate) and
             nvl(pt.completion_date, sysdate);
      return lv_task_id;
    exception
      when others then
        l_err_code := 'ETN_PO_INVALID_TASK_NUMBER';
        l_err_msg  := 'Error: Task Number could not be derived from the system. ';
        log_errors(pin_interface_txn_id    => p_in_interface_txn_id,
                   piv_source_table        => p_in_stg_tbl_name,
                   piv_source_column_name  => 'derived_task_number',
                   piv_source_column_value => lv_task,
                   piv_source_keyname1     => null,
                   piv_source_keyvalue1    => null,
                   piv_error_type          => 'ERR_VAL',
                   piv_error_code          => l_err_code,
                   piv_error_message       => l_err_msg);
        return null;
    end;
  exception
    when others then
      l_err_code := 'ETN_PO_INVALID_TASK_NUMBER';
      l_err_msg  := 'Error: EXCEPTION error -- Task Number could not be derived from the system. ';
      log_errors(pin_interface_txn_id    => p_in_interface_txn_id,
                 piv_source_table        => p_in_stg_tbl_name,
                 piv_source_column_name  => p_in_column_name,
                 piv_source_column_value => p_in_proj_name,
                 piv_source_keyname1     => null,
                 piv_source_keyvalue1    => null,
                 piv_error_type          => 'ERR_VAL',
                 piv_error_code          => l_err_code,
                 piv_error_message       => l_err_msg);
      return null;
  end xxpo_po_task_id;

  /*
  +================================================================================+
  |FUNCTION NAME : xxpo_rclv_code                                                   |
  |DESCRIPTION   : This function will check/validate QTY_RClv_EXCEPTION_CODE        |
  +================================================================================+
  */
  function xxpo_rclv_code(p_in_interface_txn_id in number,
                          p_in_stg_tbl_name     in varchar2,
                          p_in_column_name      in varchar2,
                          p_in_rclv_code        in varchar2) return varchar2 is
    lv_status  varchar2(1) := 'N';
    l_err_code varchar2(40);
    l_err_msg  varchar2(2000);
  begin
    select 'N'
      into lv_status
      from fnd_lookup_values flv
     where lookup_type = 'RCV OPTION'
       and trunc(sysdate) between
           trunc(nvl(flv.start_date_active, sysdate)) and
           trunc(nvl(flv.end_date_active, sysdate))
       and enabled_flag = 'Y'
       and lookup_code = p_in_rclv_code
       and rownum = 1;
    return lv_status;
  exception
    when no_data_found then
      l_err_code := 'ETN_PO_INVALID_QTY_RCV_EXCEPTION_CODE';
      l_err_msg  := 'Error: INVALID qty rcv exception code. ';
      log_errors(pin_interface_txn_id    => p_in_interface_txn_id,
                 piv_source_table        => p_in_stg_tbl_name,
                 piv_source_column_name  => p_in_column_name,
                 piv_source_column_value => p_in_rclv_code,
                 piv_source_keyname1     => null,
                 piv_source_keyvalue1    => null,
                 piv_error_type          => 'ERR_VAL',
                 piv_error_code          => l_err_code,
                 piv_error_message       => l_err_msg);
      lv_status := 'Y';
      return lv_status;
    when others then
      l_err_code := 'ETN_PO_INVALID_QTY_RCV_EXCEPTION_CODE';
      l_err_msg  := 'Error: exception error while validating qty rcv exception code. ';
      log_errors(pin_interface_txn_id    => p_in_interface_txn_id,
                 piv_source_table        => p_in_stg_tbl_name,
                 piv_source_column_name  => p_in_column_name,
                 piv_source_column_value => p_in_rclv_code,
                 piv_source_keyname1     => null,
                 piv_source_keyvalue1    => null,
                 piv_error_type          => 'ERR_VAL',
                 piv_error_code          => l_err_code,
                 piv_error_message       => l_err_msg);
      lv_status := 'Y';
      return lv_status;
  end xxpo_rclv_code;

  /* ---1.1
     +================================================================================+
    |FUNCTION NAME      : xxpo_RECVROUTEID                                     |
    |DESCRIPTION        : This function will derive receiving_routing_id       |
    +================================================================================+
  */
  /* FUNCTION xxpo_recvrouteid (
     p_in_interface_txn_id    IN   NUMBER,
     p_in_stg_tbl_name        IN   VARCHAR2,
     p_in_column_name         IN   VARCHAR2,
     p_in_receiving_routing   IN   VARCHAR2
  )
     RETURN NUMBER
  IS
     lv_receiving_routing_id   NUMBER;
     l_err_code                VARCHAR2 (40);
     l_err_msg                 VARCHAR2 (2000);
  BEGIN
     SELECT TO_NUMBER (lookup_code)
       INTO lv_receiving_routing_id
       FROM fnd_lookup_values flv
      WHERE lookup_type LIKE 'RCV_ROUTING_HEADERS'
        AND UPPER (meaning) = UPPER (p_in_receiving_routing)
        AND TRUNC (SYSDATE) BETWEEN TRUNC (NVL (flv.start_date_active,
                                                SYSDATE
                                               )
                                          )
                                AND TRUNC (NVL (flv.end_date_active, SYSDATE))
        AND enabled_flag = 'Y'
        AND ROWNUM = 1;

     RETURN lv_receiving_routing_id;
  EXCEPTION
     WHEN NO_DATA_FOUND
     THEN
        l_err_code := 'ETN_PO_INVALID_RECEIVING_ROUTING';
        l_err_msg := 'Error: Invalid receiving routing value. ';
        log_errors (pin_interface_txn_id         => p_in_interface_txn_id,
                    piv_source_table             => p_in_stg_tbl_name,
                    piv_source_column_name       => p_in_column_name,
                    piv_source_column_value      => p_in_receiving_routing,
                    piv_source_keyname1          => NULL,
                    piv_source_keyvalue1         => NULL,
                    piv_error_type               => 'ERR_VAL',
                    piv_error_code               => l_err_code,
                    piv_error_message            => l_err_msg
                   );
        RETURN NULL;
     WHEN OTHERS
     THEN
        l_err_code := 'ETN_PO_INVALID_RECEIVING_ROUTING';
        l_err_msg :=
           'Error: exception error while deriving receiving routing value. ';
        log_errors (pin_interface_txn_id         => p_in_interface_txn_id,
                    piv_source_table             => p_in_stg_tbl_name,
                    piv_source_column_name       => p_in_column_name,
                    piv_source_column_value      => p_in_receiving_routing,
                    piv_source_keyname1          => NULL,
                    piv_source_keyvalue1         => NULL,
                    piv_error_type               => 'ERR_VAL',
                    piv_error_code               => l_err_code,
                    piv_error_message            => l_err_msg
                   );
        RETURN NULL;
  END xxpo_recvrouteid; */
  /*
  +================================================================================+
  |FUNCTION NAME : xxpo_PAYPRIOR                                                 |
  |DESCRIPTION   : This function will check/validate Payment Priority              |
  +================================================================================+
  */
  function xxpo_payprior(p_in_interface_txn_id in number,
                         p_in_stg_tbl_name     in varchar2,
                         p_in_column_name      in varchar2,
                         pin_payment_priority  in number) return varchar2 is
    lv_status  varchar2(1) := 'N';
    l_err_code varchar2(40);
    l_err_msg  varchar2(2000);
  begin
    if (pin_payment_priority between 0 and 100) then
      lv_status := 'N';
    else
      lv_status  := 'Y';
      l_err_code := 'ETN_PO_INCORRECT_PAYMENT_PRIORITY';
      l_err_msg  := 'Error: Invalid Payment Priority. ';
      log_errors(pin_interface_txn_id    => p_in_interface_txn_id,
                 piv_source_table        => p_in_stg_tbl_name,
                 piv_source_column_name  => p_in_column_name,
                 piv_source_column_value => pin_payment_priority,
                 piv_source_keyname1     => null,
                 piv_source_keyvalue1    => null,
                 piv_error_type          => 'ERR_VAL',
                 piv_error_code          => l_err_code,
                 piv_error_message       => l_err_msg);
    end if;
    return lv_status;
  exception
    when others then
      l_err_code := 'ETN_PO_INCORRECT_PAYMENT_PRIORITY';
      l_err_msg  := 'Error: EXCEPTION error while fetching Payment Priority. ';
      log_errors(pin_interface_txn_id    => p_in_interface_txn_id,
                 piv_source_table        => p_in_stg_tbl_name,
                 piv_source_column_name  => p_in_column_name,
                 piv_source_column_value => pin_payment_priority,
                 piv_source_keyname1     => null,
                 piv_source_keyvalue1    => null,
                 piv_error_type          => 'ERR_VAL',
                 piv_error_code          => l_err_code,
                 piv_error_message       => l_err_msg);
      lv_status := 'Y';
      return lv_status;
  end xxpo_payprior;

  /* ---1.1
     +================================================================================+
    |FUNCTION NAME      : PO_LINE_TYPE_ID_D                                     |
    |DESCRIPTION        : This function will derive LINE_TYPE_ID                |
    +================================================================================+
  */
  /*   FUNCTION xxpo_po_line_type_id (
     p_in_interface_txn_id   IN   NUMBER,
     p_in_stg_tbl_name       IN   VARCHAR2,
     p_in_column_name        IN   VARCHAR2,
     p_in_line_type          IN   VARCHAR2
  )
     RETURN NUMBER
  IS
     lv_line_type_id   NUMBER;
     l_err_code        VARCHAR2 (40);
     l_err_msg         VARCHAR2 (2000);
  BEGIN
     SELECT line_type_id
       INTO lv_line_type_id
       FROM po_line_types
      WHERE line_type = p_in_line_type;

     RETURN lv_line_type_id;
  EXCEPTION
     WHEN NO_DATA_FOUND
     THEN
        l_err_code := 'ETN_PO_INVALID_LINE_TYPE';
        l_err_msg := 'Error: Invalid Line Type. ';
        log_errors (pin_interface_txn_id         => p_in_interface_txn_id,
                    piv_source_table             => p_in_stg_tbl_name,
                    piv_source_column_name       => p_in_column_name,
                    piv_source_column_value      => p_in_line_type,
                    piv_source_keyname1          => NULL,
                    piv_source_keyvalue1         => NULL,
                    piv_error_type               => 'ERR_VAL',
                    piv_error_code               => l_err_code,
                    piv_error_message            => l_err_msg
                   );
        RETURN NULL;
     WHEN OTHERS
     THEN
        l_err_code := 'ETN_PO_INVALID_LINE_TYPE';
        l_err_msg := 'Error: EXCEPTION Error while deriving Line Type. ';
        log_errors (pin_interface_txn_id         => p_in_interface_txn_id,
                    piv_source_table             => p_in_stg_tbl_name,
                    piv_source_column_name       => p_in_column_name,
                    piv_source_column_value      => p_in_line_type,
                    piv_source_keyname1          => NULL,
                    piv_source_keyvalue1         => NULL,
                    piv_error_type               => 'ERR_VAL',
                    piv_error_code               => l_err_code,
                    piv_error_message            => l_err_msg
                   );
        RETURN NULL;
  END xxpo_po_line_type_id; */
  /*  ---1.1
     +================================================================================+
    |FUNCTION NAME      : PO_UOM_CODE                                        |
    |DESCRIPTION        : This function will derive UOM_CODE                 |
    +================================================================================+
  */
  /* FUNCTION xxpo_po_uom_code (
     p_in_interface_txn_id   IN   NUMBER,
     p_in_stg_tbl_name       IN   VARCHAR2,
     p_in_column_name        IN   VARCHAR2,
     p_in_unit_of_measure    IN   VARCHAR2
  )
     RETURN VARCHAR2
  IS
     l_uom_code   VARCHAR2 (3);
     l_err_code   VARCHAR2 (40);
     l_err_msg    VARCHAR2 (2000);
  BEGIN
     SELECT uom_code
       INTO l_uom_code
       FROM mtl_units_of_measure
      WHERE unit_of_measure = p_in_unit_of_measure
        AND SYSDATE >= NVL (disable_date, SYSDATE);

     RETURN l_uom_code;
  EXCEPTION
     WHEN NO_DATA_FOUND
     THEN
        l_err_code := 'ETN_PO_INVALID_UOM';
        l_err_msg := 'Error: Invalid Unit of Measure. ';
        log_errors (pin_interface_txn_id         => p_in_interface_txn_id,
                    piv_source_table             => p_in_stg_tbl_name,
                    piv_source_column_name       => p_in_column_name,
                    piv_source_column_value      => p_in_unit_of_measure,
                    piv_source_keyname1          => NULL,
                    piv_source_keyvalue1         => NULL,
                    piv_error_type               => 'ERR_VAL',
                    piv_error_code               => l_err_code,
                    piv_error_message            => l_err_msg
                   );
        RETURN NULL;
     WHEN OTHERS
     THEN
        l_err_code := 'ETN_PO_INVALID_UOM';
        l_err_msg :=
                   'Error: EXCEPTION error while deriving Unit of Measure. ';
        log_errors (pin_interface_txn_id         => p_in_interface_txn_id,
                    piv_source_table             => p_in_stg_tbl_name,
                    piv_source_column_name       => p_in_column_name,
                    piv_source_column_value      => p_in_unit_of_measure,
                    piv_source_keyname1          => NULL,
                    piv_source_keyvalue1         => NULL,
                    piv_error_type               => 'ERR_VAL',
                    piv_error_code               => l_err_code,
                    piv_error_message            => l_err_msg
                   );
        RETURN NULL;
  END xxpo_po_uom_code; */
  /*  ---1.1
     +================================================================================+
    |FUNCTION NAME      : PO_UN_NUMBER                                       |
    |DESCRIPTION        : This function will deriving UN_NUMBER_ID           |
    +================================================================================+
  */
  /*    FUNCTION xxpo_po_un_number (
     p_in_interface_txn_id   IN   NUMBER,
     p_in_stg_tbl_name       IN   VARCHAR2,
     p_in_column_name        IN   VARCHAR2,
     p_in_un_number          IN   VARCHAR2
  )
     RETURN NUMBER
  IS
     l_un_number_id   NUMBER;
     l_err_code       VARCHAR2 (40);
     l_err_msg        VARCHAR2 (2000);
  BEGIN
     SELECT un_number_id
       INTO l_un_number_id
       FROM po_un_numbers
      WHERE UPPER (un_number) = UPPER (p_in_un_number)
        AND SYSDATE <= NVL (inactive_date, SYSDATE);

     RETURN l_un_number_id;
  EXCEPTION
     WHEN NO_DATA_FOUND
     THEN
        l_err_code := 'ETN_PO_INVALID_UN_NUMBER';
        l_err_msg := 'Error: Invalid UN Number Value. ';
        log_errors (pin_interface_txn_id         => p_in_interface_txn_id,
                    piv_source_table             => p_in_stg_tbl_name,
                    piv_source_column_name       => p_in_column_name,
                    piv_source_column_value      => p_in_un_number,
                    piv_source_keyname1          => NULL,
                    piv_source_keyvalue1         => NULL,
                    piv_error_type               => 'ERR_VAL',
                    piv_error_code               => l_err_code,
                    piv_error_message            => l_err_msg
                   );
        RETURN NULL;
     WHEN OTHERS
     THEN
        l_err_code := 'ETN_PO_INVALID_UN_NUMBER';
        l_err_msg :=
                   'Error: EXCEPTION error while deriving UN Number Value. ';
        log_errors (pin_interface_txn_id         => p_in_interface_txn_id,
                    piv_source_table             => p_in_stg_tbl_name,
                    piv_source_column_name       => p_in_column_name,
                    piv_source_column_value      => p_in_un_number,
                    piv_source_keyname1          => NULL,
                    piv_source_keyvalue1         => NULL,
                    piv_error_type               => 'ERR_VAL',
                    piv_error_code               => l_err_code,
                    piv_error_message            => l_err_msg
                   );
        RETURN NULL;
  END xxpo_po_un_number; */
  /*  ---1.1
   +================================================================================+
  |FUNCTION NAME      : PO_HAZARD_CLASS                                       |
  |DESCRIPTION        : This function will deriving HAZARD_CLASS_ID           |
  +================================================================================+
     */
  /*  FUNCTION xxpo_po_hazard_class (
     p_in_interface_txn_id   IN   NUMBER,
     p_in_stg_tbl_name       IN   VARCHAR2,
     p_in_column_name        IN   VARCHAR2,
     p_in_hazard_class       IN   VARCHAR2
  )
     RETURN NUMBER
  IS
     l_hazard_class_id   NUMBER;
     l_err_code          VARCHAR2 (40);
     l_err_msg           VARCHAR2 (2000);
  BEGIN
     SELECT hazard_class_id
       INTO l_hazard_class_id
       FROM po_hazard_classes
      WHERE UPPER (hazard_class) = UPPER (p_in_hazard_class)
        AND SYSDATE <= NVL (inactive_date, SYSDATE);

     RETURN l_hazard_class_id;
  EXCEPTION
     WHEN NO_DATA_FOUND
     THEN
        l_err_code := 'ETN_PO_INVALID_HAZARD_CLASS';
        l_err_msg := 'Error: Invalid hazard class value. ';
        log_errors (pin_interface_txn_id         => p_in_interface_txn_id,
                    piv_source_table             => p_in_stg_tbl_name,
                    piv_source_column_name       => p_in_column_name,
                    piv_source_column_value      => p_in_hazard_class,
                    piv_source_keyname1          => NULL,
                    piv_source_keyvalue1         => NULL,
                    piv_error_type               => 'ERR_VAL',
                    piv_error_code               => l_err_code,
                    piv_error_message            => l_err_msg
                   );
        RETURN NULL;
     WHEN OTHERS
     THEN
        l_err_code := 'ETN_PO_INVALID_HAZARD_CLASS';
        l_err_msg :=
                'Error: exception error while deriving hazard class value. ';
        log_errors (pin_interface_txn_id         => p_in_interface_txn_id,
                    piv_source_table             => p_in_stg_tbl_name,
                    piv_source_column_name       => p_in_column_name,
                    piv_source_column_value      => p_in_hazard_class,
                    piv_source_keyname1          => NULL,
                    piv_source_keyvalue1         => NULL,
                    piv_error_type               => 'ERR_VAL',
                    piv_error_code               => l_err_code,
                    piv_error_message            => l_err_msg
                   );
        RETURN NULL;
  END xxpo_po_hazard_class; */
  /*  --1.1
  +================================================================================+
  |FUNCTION NAME : PO_SHIP_TO_ORG_ID                                         |
  |DESCRIPTION   : This function will check/validate SHIP_TO_ORGANIZATION_ID |
  +================================================================================+
  */
  /*    FUNCTION xxpo_po_ship_to_org_id (
    p_in_interface_txn_id   IN   NUMBER,
     p_in_stg_tbl_name       IN   VARCHAR2,
     p_in_column_name        IN   VARCHAR2,
     p_in_ship_to_org_name   IN   VARCHAR2
  )
     RETURN NUMBER
  IS
     lv_ship_to_org_id   NUMBER;
     l_err_code          VARCHAR2 (40);
     l_err_msg           VARCHAR2 (2000);
  BEGIN
     SELECT organization_id
       INTO lv_ship_to_org_id
       FROM org_organization_definitions
      WHERE organization_name = p_in_ship_to_org_name
        AND SYSDATE BETWEEN NVL (user_definition_enable_date, SYSDATE)
                        AND NVL (disable_date, SYSDATE)
        AND inventory_enabled_flag = 'Y';

     RETURN lv_ship_to_org_id;
  EXCEPTION
     WHEN NO_DATA_FOUND
     THEN
        l_err_code := 'ETN_PO_INVALID_ORG_NAME';
        l_err_msg := 'Error: Invalid Inventory Org Name. ';
        log_errors (pin_interface_txn_id         => p_in_interface_txn_id,
                    piv_source_table             => p_in_stg_tbl_name,
                    piv_source_column_name       => p_in_column_name,
                    piv_source_column_value      => p_in_ship_to_org_name,
                    piv_source_keyname1          => NULL,
                    piv_source_keyvalue1         => NULL,
                    piv_error_type               => 'ERR_VAL',
                    piv_error_code               => l_err_code,
                    piv_error_message            => l_err_msg
                   );
        RETURN NULL;
     WHEN OTHERS
     THEN
        l_err_code := 'ETN_PO_INVALID_ORG_NAME';
        l_err_msg :=
                'Error: EXCEPTION error while deriving Inventory Org Name. ';
        log_errors (pin_interface_txn_id         => p_in_interface_txn_id,
                    piv_source_table             => p_in_stg_tbl_name,
                    piv_source_column_name       => p_in_column_name,
                    piv_source_column_value      => p_in_ship_to_org_name,
                    piv_source_keyname1          => NULL,
                    piv_source_keyvalue1         => NULL,
                    piv_error_type               => 'ERR_VAL',
                    piv_error_code               => l_err_code,
                    piv_error_message            => l_err_msg
                   );
        RETURN NULL;
  END xxpo_po_ship_to_org_id; */
  --
  -- ========================
  -- Procedure: VALIDATE_ACCOUNTS
  -- =============================================================================
  --   This procedure validates all
  --   the account related information
  -- =============================================================================
  procedure validate_accounts(p_in_txn_id       in number,
                              p_in_stg_tbl_name in varchar2,
                              p_in_column_name  in varchar2,
                              p_in_seg1         in varchar2,
                              p_in_seg2         in varchar2,
                              p_in_seg3         in varchar2,
                              p_in_seg4         in varchar2,
                              p_in_seg5         in varchar2,
                              p_in_seg6         in varchar2,
                              p_in_seg7         in varchar2,
                              x_out_acc         out xxetn_common_pkg.g_rec_type,
                              x_out_ccid        out number) is
    l_in_rec        xxetn_coa_mapping_pkg.g_coa_rec_type := null;
    x_ccid          number := null;
    x_out_rec       xxetn_coa_mapping_pkg.g_coa_rec_type := null;
    x_msg           varchar2(4000) := null;
    x_status        varchar2(50) := null;
    l_in_seg_rec    xxetn_common_pkg.g_rec_type := null;
    x_err           varchar2(4000) := null;
    l_err_code      varchar2(40) := null;
    l_err_msg       varchar2(2000) := null;
    l_total_segment varchar2(240) := null; ---v1.14
  begin
    x_out_acc  := null;
    x_out_ccid := null;
    xxetn_debug_pkg.add_debug(piv_debug_msg => 'Validate accounts procedure called ');
    l_in_rec.segment1 := p_in_seg1;
    l_in_rec.segment2 := p_in_seg2;
    l_in_rec.segment3 := p_in_seg3;
    l_in_rec.segment4 := p_in_seg4;
    l_in_rec.segment5 := p_in_seg5;
    l_in_rec.segment6 := p_in_seg6;
    l_in_rec.segment7 := p_in_seg7;
    l_total_segment   := p_in_seg1 || '.' || p_in_seg2 || '.' || p_in_seg3 || '.' ||
                         p_in_seg4 || '.' || p_in_seg5 || '.' || p_in_seg6 || '.' ||
                         p_in_seg7; --v1.14
    xxetn_coa_mapping_pkg.get_code_combination(g_direction,
                                               null,
                                               sysdate,
                                               l_in_rec,
                                               x_out_rec,
                                               x_status,
                                               x_msg);
    if x_status = g_coa_processed then
      l_in_seg_rec.segment1  := x_out_rec.segment1;
      l_in_seg_rec.segment2  := x_out_rec.segment2;
      l_in_seg_rec.segment3  := x_out_rec.segment3;
      l_in_seg_rec.segment4  := x_out_rec.segment4;
      l_in_seg_rec.segment5  := x_out_rec.segment5;
      l_in_seg_rec.segment6  := x_out_rec.segment6;
      l_in_seg_rec.segment7  := x_out_rec.segment7;
      l_in_seg_rec.segment8  := x_out_rec.segment8;
      l_in_seg_rec.segment9  := x_out_rec.segment9;
      l_in_seg_rec.segment10 := x_out_rec.segment10;
      xxetn_common_pkg.get_ccid(l_in_seg_rec, x_ccid, x_err);
      x_out_acc.segment1  := x_out_rec.segment1;
      x_out_acc.segment2  := x_out_rec.segment2;
      x_out_acc.segment3  := x_out_rec.segment3;
      x_out_acc.segment4  := x_out_rec.segment4;
      x_out_acc.segment5  := x_out_rec.segment5;
      x_out_acc.segment6  := x_out_rec.segment6;
      x_out_acc.segment7  := x_out_rec.segment7;
      x_out_acc.segment8  := x_out_rec.segment8;
      x_out_acc.segment9  := x_out_rec.segment9;
      x_out_acc.segment10 := x_out_rec.segment10;
      if x_err is null then
        x_out_ccid := x_ccid;
        xxetn_debug_pkg.add_debug(piv_debug_msg => 'Account information successfully derived ');
      else
        for r_org_ref_err_rec in (select /*+ INDEX (xis XXPO_PO_DISTRIBUTION_STG_N8) */
                                   interface_txn_id
                                    from xxpo_po_distribution_stg xis
                                   where leg_charge_account_seg1 = p_in_seg1
                                     and leg_charge_account_seg2 = p_in_seg2
                                     and leg_charge_account_seg3 = p_in_seg3
                                     and leg_charge_account_seg4 = p_in_seg4
                                     and leg_charge_account_seg5 = p_in_seg5
                                     and leg_charge_account_seg6 = p_in_seg6
                                     and leg_charge_account_seg7 = p_in_seg7
                                     and batch_id = g_batch_id
                                     and run_sequence_id = g_new_run_seq_id) loop
          l_err_code := 'ETN_PO_INCORRECT_ACCOUNT_INFORMATION';
          l_err_msg  := 'Error : Following error in COA transformation : ' ||
                        x_err || ' for : ' || 'leg_charge_account_seg1';
          log_errors(pin_interface_txn_id    => r_org_ref_err_rec.interface_txn_id,
                     piv_source_table        => p_in_stg_tbl_name,
                     piv_source_column_name  => 'leg_charge_account_seg1',
                     piv_source_column_value => l_total_segment, --v1.14
                     piv_source_keyname1     => null,
                     piv_source_keyvalue1    => null,
                     piv_error_type          => 'ERR_VAL',
                     piv_error_code          => l_err_code,
                     piv_error_message       => l_err_msg);
        end loop;
        for r_org_ref_err_rec in (select /*+ INDEX (xis XXPO_PO_DISTRIBUTION_STG_N9) */
                                   interface_txn_id
                                    from xxpo_po_distribution_stg xis
                                   where leg_accural_account_seg1 =
                                         p_in_seg1
                                     and leg_accural_account_seg2 =
                                         p_in_seg2
                                     and leg_accural_account_seg3 =
                                         p_in_seg3
                                     and leg_accural_account_seg4 =
                                         p_in_seg4
                                     and leg_accural_account_seg5 =
                                         p_in_seg5
                                     and leg_accural_account_seg6 =
                                         p_in_seg6
                                     and leg_accural_account_seg7 =
                                         p_in_seg7
                                     and batch_id = g_batch_id
                                     and run_sequence_id = g_new_run_seq_id) loop
          l_err_code := 'ETN_PO_INCORRECT_ACCOUNT_INFORMATION';
          l_err_msg  := 'Error : Following error in COA transformation : ' ||
                        x_err || ' for : ' || 'leg_accural_account_seg1';
          log_errors(pin_interface_txn_id    => r_org_ref_err_rec.interface_txn_id,
                     piv_source_table        => p_in_stg_tbl_name,
                     piv_source_column_name  => 'leg_accural_account_seg1',
                     piv_source_column_value => l_total_segment, --v1.14
                     piv_source_keyname1     => null,
                     piv_source_keyvalue1    => null,
                     piv_error_type          => 'ERR_VAL',
                     piv_error_code          => l_err_code,
                     piv_error_message       => l_err_msg);
        end loop;
        for r_org_ref_err_rec in (select /*+ INDEX (xis XXPO_PO_DISTRIBUTION_STG_N10) */
                                   interface_txn_id
                                    from xxpo_po_distribution_stg xis
                                   where leg_variance_account_seg1 =
                                         p_in_seg1
                                     and leg_variance_account_seg2 =
                                         p_in_seg2
                                     and leg_variance_account_seg3 =
                                         p_in_seg3
                                     and leg_variance_account_seg4 =
                                         p_in_seg4
                                     and leg_variance_account_seg5 =
                                         p_in_seg5
                                     and leg_variance_account_seg6 =
                                         p_in_seg6
                                     and leg_variance_account_seg7 =
                                         p_in_seg7
                                     and batch_id = g_batch_id
                                     and run_sequence_id = g_new_run_seq_id) loop
          l_err_code := 'ETN_PO_INCORRECT_ACCOUNT_INFORMATION';
          l_err_msg  := 'Error : Following error in COA transformation : ' ||
                        x_err || ' for : ' || 'leg_variance_account_seg1';
          log_errors(pin_interface_txn_id    => r_org_ref_err_rec.interface_txn_id,
                     piv_source_table        => p_in_stg_tbl_name,
                     piv_source_column_name  => 'leg_variance_account_seg1',
                     piv_source_column_value => l_total_segment, --v1.14
                     piv_source_keyname1     => null,
                     piv_source_keyvalue1    => null,
                     piv_error_type          => 'ERR_VAL',
                     piv_error_code          => l_err_code,
                     piv_error_message       => l_err_msg);
        end loop;
      end if;
    elsif x_status = g_coa_error then
      for r_org_ref_err_rec in (select /*+ INDEX (xis XXPO_PO_DISTRIBUTION_STG_N8) */
                                 interface_txn_id
                                  from xxpo_po_distribution_stg xis
                                 where leg_charge_account_seg1 = p_in_seg1
                                   and leg_charge_account_seg2 = p_in_seg2
                                   and leg_charge_account_seg3 = p_in_seg3
                                   and leg_charge_account_seg4 = p_in_seg4
                                   and leg_charge_account_seg5 = p_in_seg5
                                   and leg_charge_account_seg6 = p_in_seg6
                                   and leg_charge_account_seg7 = p_in_seg7
                                   and batch_id = g_batch_id
                                   and run_sequence_id = g_new_run_seq_id) loop
        l_err_code := 'ETN_PO_INCORRECT_ACCOUNT_INFORMATION';
        l_err_msg  := 'Error : Following error in COA transformation : ' ||
                      x_msg || ' for : ' || 'leg_charge_account_seg1';
        log_errors(pin_interface_txn_id    => r_org_ref_err_rec.interface_txn_id,
                   piv_source_table        => p_in_stg_tbl_name,
                   piv_source_column_name  => 'leg_charge_account_seg1',
                   piv_source_column_value => l_total_segment, --v1.14
                   piv_source_keyname1     => null,
                   piv_source_keyvalue1    => null,
                   piv_error_type          => 'ERR_VAL',
                   piv_error_code          => l_err_code,
                   piv_error_message       => l_err_msg);
      end loop;
      for r_org_ref_err_rec in (select /*+ INDEX (xis XXPO_PO_DISTRIBUTION_STG_N9) */
                                 interface_txn_id
                                  from xxpo_po_distribution_stg xis
                                 where leg_accural_account_seg1 = p_in_seg1
                                   and leg_accural_account_seg2 = p_in_seg2
                                   and leg_accural_account_seg3 = p_in_seg3
                                   and leg_accural_account_seg4 = p_in_seg4
                                   and leg_accural_account_seg5 = p_in_seg5
                                   and leg_accural_account_seg6 = p_in_seg6
                                   and leg_accural_account_seg7 = p_in_seg7
                                   and batch_id = g_batch_id
                                   and run_sequence_id = g_new_run_seq_id) loop
        l_err_code := 'ETN_PO_INCORRECT_ACCOUNT_INFORMATION';
        l_err_msg  := 'Error : Following error in COA transformation : ' ||
                      x_msg || ' for : ' || 'leg_accural_account_seg1';
        log_errors(pin_interface_txn_id    => r_org_ref_err_rec.interface_txn_id,
                   piv_source_table        => p_in_stg_tbl_name,
                   piv_source_column_name  => 'leg_accural_account_seg1',
                   piv_source_column_value => l_total_segment, --v1.14
                   piv_source_keyname1     => null,
                   piv_source_keyvalue1    => null,
                   piv_error_type          => 'ERR_VAL',
                   piv_error_code          => l_err_code,
                   piv_error_message       => l_err_msg);
      end loop;
      for r_org_ref_err_rec in (select /*+ INDEX (xis XXPO_PO_DISTRIBUTION_STG_N10) */
                                 interface_txn_id
                                  from xxpo_po_distribution_stg xis
                                 where leg_variance_account_seg1 = p_in_seg1
                                   and leg_variance_account_seg2 = p_in_seg2
                                   and leg_variance_account_seg3 = p_in_seg3
                                   and leg_variance_account_seg4 = p_in_seg4
                                   and leg_variance_account_seg5 = p_in_seg5
                                   and leg_variance_account_seg6 = p_in_seg6
                                   and leg_variance_account_seg7 = p_in_seg7
                                   and batch_id = g_batch_id
                                   and run_sequence_id = g_new_run_seq_id) loop
        l_err_code := 'ETN_PO_INCORRECT_ACCOUNT_INFORMATION';
        l_err_msg  := 'Error : Following error in COA transformation : ' ||
                      x_msg || ' for : ' || 'leg_variance_account_seg1';
        log_errors(pin_interface_txn_id    => r_org_ref_err_rec.interface_txn_id,
                   piv_source_table        => p_in_stg_tbl_name,
                   piv_source_column_name  => 'leg_variance_account_seg1',
                   piv_source_column_value => l_total_segment, --v1.14
                   piv_source_keyname1     => null,
                   piv_source_keyvalue1    => null,
                   piv_error_type          => 'ERR_VAL',
                   piv_error_code          => l_err_code,
                   piv_error_message       => l_err_msg);
      end loop;
    end if;
    xxetn_debug_pkg.add_debug(piv_debug_msg => 'Validate accounts procedure ends ');
  exception
    when others then
      l_err_code := 'ETN_PO_PROCEDURE_EXCEPTION';
      l_err_msg  := 'Error while deriving accounting information ';
      log_errors(pin_interface_txn_id    => p_in_txn_id,
                 piv_source_table        => p_in_stg_tbl_name,
                 piv_source_column_name  => p_in_column_name,
                 piv_source_column_value => p_in_seg1,
                 piv_source_keyname1     => null,
                 piv_source_keyvalue1    => null,
                 piv_error_type          => 'ERR_VAL',
                 piv_error_code          => l_err_code,
                 piv_error_message       => l_err_msg);
  end validate_accounts;

  /*
  +================================================================================+
  |FUNCTION NAME      : PO_ACC_REC_FLAG                                    |
  |DESCRIPTION        : This function will validate Accrue_on_receipt_flag   |
  +================================================================================+
  */
  function xxpo_po_acc_rec_flag(p_in_interface_txn_id in number,
                                p_in_stg_tbl_name     in varchar2,
                                p_in_column_name      in varchar2,
                                p_in_destination_type in varchar2,
                                p_in_acc_rec_flag     in varchar2)
    return varchar2 is
    lv_status  varchar2(1) := 'N';
    l_err_code varchar2(40);
    l_err_msg  varchar2(2000);
  begin
    if p_in_destination_type = 'INVENTORY' and p_in_acc_rec_flag <> 'Y' then
      lv_status  := 'Y';
      l_err_code := 'ETN_PO_INVALID_ACCRUE_ON_RCPT_FLAG';
      l_err_msg  := 'Error: Invalid Accrue on receipt flag value. ';
      log_errors(pin_interface_txn_id    => p_in_interface_txn_id,
                 piv_source_table        => p_in_stg_tbl_name,
                 piv_source_column_name  => p_in_column_name,
                 piv_source_column_value => p_in_acc_rec_flag,
                 piv_source_keyname1     => null,
                 piv_source_keyvalue1    => null,
                 piv_error_type          => 'ERR_VAL',
                 piv_error_code          => l_err_code,
                 piv_error_message       => l_err_msg);
    end if;
    return lv_status;
  exception
    when others then
      l_err_code := 'ETN_PO_INVALID_ACCRUE_ON_RCPT_FLAG';
      l_err_msg  := 'Error: EXCEPTION error while validating accrue on receipt flag value. ';
      log_errors(pin_interface_txn_id    => p_in_interface_txn_id,
                 piv_source_table        => p_in_stg_tbl_name,
                 piv_source_column_name  => p_in_column_name,
                 piv_source_column_value => p_in_acc_rec_flag,
                 piv_source_keyname1     => null,
                 piv_source_keyvalue1    => null,
                 piv_error_type          => 'ERR_VAL',
                 piv_error_code          => l_err_code,
                 piv_error_message       => l_err_msg);
      lv_status := 'Y';
      return lv_status;
  end xxpo_po_acc_rec_flag;

  /*
   +================================================================================+
   |FUNCTION NAME      : xxpo_po_exp_type                                    |
  |DESCRIPTION        : This function will validate expenditure type        |
   +================================================================================+
   */
  function xxpo_po_exp_type(p_in_interface_txn_id in number,
                            p_in_stg_tbl_name     in varchar2,
                            p_in_column_name      in varchar2,
                            p_in_exp_type         in varchar2)
    return varchar2 is
    lv_status  varchar2(1) := 'N';
    l_err_code varchar2(40);
    l_err_msg  varchar2(2000);
  begin
    select 'N'
      into lv_status
      from pa_expenditure_types
     where expenditure_type = p_in_exp_type
       and trunc(sysdate) between nvl(start_date_active, trunc(sysdate)) and
           nvl(end_date_active, trunc(sysdate));
    return lv_status;
  exception
    when others then
      l_err_code := 'ETN_PO_INVALID_EXP_TYPE';
      l_err_msg  := 'Error: Invalid Expenditure type value. ';
      log_errors(pin_interface_txn_id    => p_in_interface_txn_id,
                 piv_source_table        => p_in_stg_tbl_name,
                 piv_source_column_name  => p_in_column_name,
                 piv_source_column_value => p_in_exp_type,
                 piv_source_keyname1     => null,
                 piv_source_keyvalue1    => null,
                 piv_error_type          => 'ERR_VAL',
                 piv_error_code          => l_err_code,
                 piv_error_message       => l_err_msg);
      lv_status := 'Y';
      return lv_status;
  end xxpo_po_exp_type;

  /* ---1.1
  +================================================================================+
  |FUNCTION NAME : PO_ITEM_ID                                           |
  |DESCRIPTION   : This function will generate ITEM_ID                    |
  +================================================================================+
  */
  /* FUNCTION xxpo_po_item_id (
     p_in_interface_txn_id   IN   NUMBER,
     p_in_stg_tbl_name       IN   VARCHAR2,
     p_in_column_name        IN   VARCHAR2,
     p_in_oper_unit_id       IN   NUMBER,
     p_in_item               IN   VARCHAR2
  )
     RETURN NUMBER
  IS
     lv_item_id   NUMBER;
     l_err_code   VARCHAR2 (40);
     l_err_msg    VARCHAR2 (2000);
  BEGIN
     SELECT inventory_item_id
       INTO lv_item_id
       FROM mtl_system_items_b
      WHERE organization_id = p_in_oper_unit_id
        AND UPPER (segment1) = UPPER (p_in_item)
        AND SYSDATE BETWEEN NVL (start_date_active, SYSDATE)
                        AND NVL (end_date_active, SYSDATE)
        AND enabled_flag = 'Y';

     RETURN lv_item_id;
  EXCEPTION
     WHEN NO_DATA_FOUND
     THEN
        l_err_code := 'ETN_PO_INVALID_ITEM_NUMBER';
        l_err_msg := 'Error: Invalid Item Number Value. ';
        log_errors (pin_interface_txn_id         => p_in_interface_txn_id,
                    piv_source_table             => p_in_stg_tbl_name,
                    piv_source_column_name       => p_in_column_name,
                    piv_source_column_value      => p_in_item,
                    piv_source_keyname1          => NULL,
                    piv_source_keyvalue1         => NULL,
                    piv_error_type               => 'ERR_VAL',
                    piv_error_code               => l_err_code,
                    piv_error_message            => l_err_msg
                   );
        RETURN NULL;
     WHEN OTHERS
     THEN
        l_err_code := 'ETN_PO_INVALID_ITEM_NUMBER';
        l_err_msg := 'Error: EXCEPTION error while deriving item id. ';
        log_errors (pin_interface_txn_id         => p_in_interface_txn_id,
                    piv_source_table             => p_in_stg_tbl_name,
                    piv_source_column_name       => p_in_column_name,
                    piv_source_column_value      => p_in_item,
                    piv_source_keyname1          => NULL,
                    piv_source_keyvalue1         => NULL,
                    piv_error_type               => 'ERR_VAL',
                    piv_error_code               => l_err_code,
                    piv_error_message            => l_err_msg
                   );
        RETURN NULL;
  END xxpo_po_item_id; */
  /* --1.1
  +================================================================================+
  |FUNCTION NAME : xxpo_po_item_cat                                             |
  |DESCRIPTION   : This function will check/validate item category |
  +================================================================================+
  */
  /*    FUNCTION xxpo_po_item_cat_id (
       p_in_interface_txn_id   IN   NUMBER,
       p_in_stg_tbl_name       IN   VARCHAR2,
       p_in_column_name        IN   VARCHAR2,
       p_in_category           IN   VARCHAR2
    )
       RETURN VARCHAR2
    IS
       --  lv_status        VARCHAR2 (1) := 'N';
       lv_category_id   NUMBER;
       l_err_code       VARCHAR2 (40);
       l_err_msg        VARCHAR2 (2000);
    BEGIN
       SELECT category_id
         INTO lv_category_id
         FROM apps.mtl_categories_b_kfv
        WHERE (concatenated_segments) = p_in_category;

       RETURN lv_category_id;
    EXCEPTION
       WHEN NO_DATA_FOUND
       THEN
          l_err_code := 'ETN_PO_INVALID_ITEM_CATEGORY';
          l_err_msg := 'Error: Invalid Item Category Value. ';
          log_errors (pin_interface_txn_id         => p_in_interface_txn_id,
                      piv_source_table             => p_in_stg_tbl_name,
                      piv_source_column_name       => p_in_column_name,
                      piv_source_column_value      => p_in_category,
                      piv_source_keyname1          => NULL,
                      piv_source_keyvalue1         => NULL,
                      piv_error_type               => 'ERR_VAL',
                      piv_error_code               => l_err_code,
                      piv_error_message            => l_err_msg
                     );
          RETURN NULL;
       WHEN OTHERS
       THEN
          l_err_code := 'ETN_PO_INVALID_ITEM_CATEGORY';
          l_err_msg :=
                    'Error: EXCEPTION error while fetching Item Category id. ';
          log_errors (pin_interface_txn_id         => p_in_interface_txn_id,
                      piv_source_table             => p_in_stg_tbl_name,
                      piv_source_column_name       => p_in_column_name,
                      piv_source_column_value      => p_in_category,
                      piv_source_keyname1          => NULL,
                      piv_source_keyvalue1         => NULL,
                      piv_error_type               => 'ERR_VAL',
                      piv_error_code               => l_err_code,
                      piv_error_message            => l_err_msg
                     );
          RETURN NULL;
    END xxpo_po_item_cat_id;
  */
  /*
  +================================================================================+
   |PROCEDURE      : xxpo_tax_code_id                                      |
   |DESCRIPTION        : This FUNCTION will VALIDATE TAX_CODE_ID               |
   +================================================================================+
  */ --changed as part of Tax Code Cross Reference -- V1.19--
  procedure xxpo_tax_code_id(p_in_interface_txn_id in number,
                             p_in_stg_tbl_name     in varchar2,
                             p_in_column_name      in varchar2,
                             p_in_tax_code         in varchar2,
                             p_in_org_id           in number,
                             p_out_tax_id          out number) is
    l_err_code varchar2(40);
    l_err_msg  varchar2(2000);
  begin
    p_out_tax_id := null;
    begin
      select distinct zrb.tax_rate_id
        into p_out_tax_id
        from zx_accounts            za,
             hr_operating_units     hrou,
             gl_ledgers             gl,
             fnd_id_flex_structures fifs,
             zx_rates_b             zrb,
             zx_regimes_b           zb
       where za.internal_organization_id = hrou.organization_id
         and gl.ledger_id = za.ledger_id
         and fifs.application_id =
             (select fap.application_id
                from fnd_application_vl fap
               where fap.application_short_name = 'SQLGL')
         and fifs.id_flex_code = 'GL#'
         and fifs.id_flex_num = gl.chart_of_accounts_id
         and zrb.tax_rate_id = za.tax_account_entity_id
         and za.tax_account_entity_code = 'RATES'
         and zrb.tax_rate_code = p_in_tax_code
         and hrou.organization_id = p_in_org_id
         and trunc(sysdate) between trunc(nvl(zb.effective_from, sysdate)) and
             trunc(nvl(zb.effective_to, sysdate))
         and zrb.active_flag = 'Y';
    exception
      when others then
        l_err_code := 'ETN_PO_INVALID_TAX_NAME';
        l_err_msg  := 'Error: Invalid Tax Name value. ';
        log_errors(pin_interface_txn_id    => p_in_interface_txn_id,
                   piv_source_table        => p_in_stg_tbl_name,
                   piv_source_column_name  => p_in_column_name,
                   piv_source_column_value => p_in_tax_code,
                   piv_source_keyname1     => null,
                   piv_source_keyvalue1    => null,
                   piv_error_type          => 'ERR_VAL',
                   piv_error_code          => l_err_code,
                   piv_error_message       => l_err_msg);
    end;
  exception
    when others then
      l_err_code := 'ETN_PO_PROCEDURE_EXCEPTION';
      l_err_msg  := 'Error: Exception error while validating Tax Name value. ';
      log_errors(pin_interface_txn_id    => p_in_interface_txn_id,
                 piv_source_table        => p_in_stg_tbl_name,
                 piv_source_column_name  => p_in_column_name,
                 piv_source_column_value => p_in_tax_code,
                 piv_source_keyname1     => null,
                 piv_source_keyvalue1    => null,
                 piv_error_type          => 'ERR_VAL',
                 piv_error_code          => l_err_code,
                 piv_error_message       => l_err_msg);
  end xxpo_tax_code_id;

  /*
   +================================================================================+
   |PROCEDURE      :   line_shipment_number                                       |
   |DESCRIPTION        : This Procedure will update new line number,              |
                          shipment number and DFFs                                |
   +================================================================================+
  */
  procedure xxpo_line_shipment_number(p_in_interface_txn_id in number,
                                      p_in_stg_tbl_name     in varchar2,
                                      p_in_column_name      in varchar2,
                                      p_header_id           in number,
                                      p_line_id             in number,
                                      p_leg_document_num    in xxpo_po_line_shipment_stg.leg_document_num %type,
                                      p_line_num            in xxpo_po_line_shipment_stg.leg_line_num%type,
                                      p_shipment_num        xxpo_po_line_shipment_stg.leg_shipment_num%type,
                                      p_attribute6          xxpo_po_line_shipment_stg.line_attribute6%type,
                                      p_line_number_fix     in number,
                                      p_description_in      xxpo_po_line_shipment_stg.leg_item_description%type,
                                      p_description_out     out xxpo_po_line_shipment_stg.leg_item_description%type,
                                      p_line_num_new        out xxpo_po_line_shipment_stg.leg_line_num%type) is
    l_err_code varchar2(40);
    l_err_msg  varchar2(2000);
    l_count    number;
    cursor cur_line_shipment(p_header_id in number, p_line_id in number) is
      select count(1)
        from xxpo_po_line_shipment_stg
       where leg_po_header_id = p_header_id
         and leg_po_line_id = p_line_id
         and leg_document_num = p_leg_document_num;
  begin
    l_count        := 0;
    p_line_num_new := null;
    open cur_line_shipment(p_header_id, p_line_id);
    fetch cur_line_shipment
      into l_count;
    if cur_line_shipment%notfound then
      l_count := 0;
    end if;
    close cur_line_shipment;
    if l_count > 0 and p_attribute6 is null then
      p_line_num_new := p_line_num || '.' || p_line_number_fix;
      p_description_out := 'L' || p_line_num || 'S' || p_shipment_num || '|' || --- Defect 4874 | was missing
                           p_description_in;
    else
      p_line_num_new := p_line_num;
      p_description_out := p_description_in;
    end if;
  exception
    when others then
      p_line_num_new := p_line_num;
      l_err_code     := 'ETN_LINESHIP_PROCEDURE_EXCEPTION';
      l_err_msg      := 'Error: Exception error while getting new line and shipment number. ';
      log_errors(pin_interface_txn_id    => p_in_interface_txn_id,
                 piv_source_table        => p_in_stg_tbl_name,
                 piv_source_column_name  => p_in_column_name,
                 piv_source_column_value => p_line_num,
                 piv_source_keyname1     => 'Shipment Number ' ||
                                            p_shipment_num,
                 piv_source_keyvalue1    => 'DFF value ' || p_attribute6,
                 piv_error_type          => 'ERR_VAL',
                 piv_error_code          => l_err_code,
                 piv_error_message       => l_err_msg);
  end;

  /*
   +===========================================================================================+
   |PROCEDURE      :  Newly Added line_shipment_number procedure.   |
   |DESCRIPTION        : This Procedure will update new line number,              |
                          shipment number and DFFs ,This procedure newly added
                        to avoid the duplicate line number issue in Interface table Mock 4  |
   +===========================================================================================+
  */
  procedure xxpo_line_num_generator(p_in_interface_txn_id in number,
                                    p_in_stg_tbl_name     in varchar2,
                                    p_in_column_name      in varchar2,
                                    p_leg_po_header_id    in number,
                                    p_leg_document_num    in xxpo_po_line_shipment_stg.leg_document_num %type,
                                    p_line_num            in xxpo_po_line_shipment_stg.leg_line_num%type,
                                    p_shipment_num        xxpo_po_line_shipment_stg.leg_shipment_num%type,
                                    p_attribute6          xxpo_po_line_shipment_stg.line_attribute6%type,
                                    p_description_in      xxpo_po_line_shipment_stg.leg_item_description%type,
                                    p_line_num_new        out xxpo_po_line_shipment_stg.leg_line_num%type,
                                    p_description_out     out xxpo_po_line_shipment_stg.leg_item_description%type) is
    cursor cur_po is
      select *
        from xxpo_po_line_shipment_stg
       where 1 = 1
         and leg_po_header_id = p_leg_po_header_id
         and leg_document_num = p_leg_document_num
         and nvl(leg_unit_price, 0) <> nvl(leg_list_price_per_unit, 0)
         and shipment_attribute6 is null
       order by leg_line_num, leg_po_header_id, leg_document_num;
    t_num  number := 10;
    x_num  number := null;
    l_num  number;
    z_num  number;
    x_desc xxpo_po_line_shipment_stg.leg_item_description%type;
    l_err_code varchar2(40);
    l_err_msg  varchar2(2000);
    type xxtemp_po is table of number index by binary_integer;
    var_po_line xxtemp_po;
    v_counter   integer := 0;
    index_value number;
  begin
    for i in cur_po loop
      p_line_num_new    := null;
      p_description_out := null;
      begin
        select to_number((i.leg_line_num || '.' || t_num))
          into z_num
          from dual;
      exception
        when no_data_found then
          z_num := 0;
      end;
      index_value := var_po_line.first;
      loop
        exit when index_value is null;
        if var_po_line(index_value) = z_num then
          l_num := 1;
        end if;
        index_value := var_po_line.next(index_value);
      end loop;
      if l_num > 0 then
        t_num := t_num + 1;
      end if;
      select leg_line_num || '.' || t_num,
             'L' || p_line_num || 'S' || p_shipment_num || '|' ||
             p_description_in --- Defect 4874 | was missing
        into x_num, x_desc
        from xxpo_po_line_shipment_stg a
       where leg_po_header_id = i.leg_po_header_id
         and leg_document_num = i.leg_document_num
         and nvl(leg_unit_price, 0) <> nvl(leg_list_price_per_unit, 0)
         and interface_txn_id = i.interface_txn_id;
      p_line_num_new := x_num;
      p_description_out := x_desc;
      v_counter := v_counter + 1;
      var_po_line(v_counter) := x_num;
      t_num := t_num + 1;
    end loop;
  exception
    when others then
      p_line_num_new    := p_line_num;
      p_description_out := p_description_in;
      l_err_code        := 'ETN_LINESHIP_PROCEDURE_EXCEPTION';
      l_err_msg         := 'Error: Exception error while getting new line and shipment number. ';
      log_errors(pin_interface_txn_id    => p_in_interface_txn_id,
                 piv_source_table        => p_in_stg_tbl_name,
                 piv_source_column_name  => p_in_column_name,
                 piv_source_column_value => p_line_num,
                 piv_source_keyname1     => 'Shipment Number ' ||
                                            p_shipment_num,
                 piv_source_keyvalue1    => 'DFF value ' || p_attribute6,
                 piv_error_type          => 'ERR_VAL',
                 piv_error_code          => l_err_code,
                 piv_error_message       => l_err_msg);
  end;

  /*
  +================================================================================+
  |FUNCTION NAME      : PO_DEST_TYPE                                       |
  |DESCRIPTION        : This function will validate Destination_Type         |
  +================================================================================+
  */
  function xxpo_po_dest_type(p_in_interface_txn_id in number,
                             p_in_stg_tbl_name     in varchar2,
                             p_in_column_name      in varchar2,
                             p_in_dest_type        in varchar2)
    return varchar2 is
    lv_status  varchar2(1) := 'N';
    l_err_code varchar2(40);
    l_err_msg  varchar2(2000);
  begin
    if p_in_dest_type = 'INVENTORY' or p_in_dest_type = 'EXPENSE' then
      lv_status := 'N';
    else
      lv_status  := 'Y';
      l_err_code := 'ETN_PO_INVALID_DESTINATION_TYPE_CODE';
      l_err_msg  := 'Error: Invalid destination type code value. ';
      log_errors(pin_interface_txn_id    => p_in_interface_txn_id,
                 piv_source_table        => p_in_stg_tbl_name,
                 piv_source_column_name  => p_in_column_name,
                 piv_source_column_value => p_in_dest_type,
                 piv_source_keyname1     => null,
                 piv_source_keyvalue1    => null,
                 piv_error_type          => 'ERR_VAL',
                 piv_error_code          => l_err_code,
                 piv_error_message       => l_err_msg);
    end if;
    return lv_status;
  exception
    when others then
      l_err_code := 'ETN_PO_INVALID_DESTINATION_TYPE_CODE';
      l_err_msg  := 'Error: exception error while validating destination type code value. ';
      log_errors(pin_interface_txn_id    => p_in_interface_txn_id,
                 piv_source_table        => p_in_stg_tbl_name,
                 piv_source_column_name  => p_in_column_name,
                 piv_source_column_value => p_in_dest_type,
                 piv_source_keyname1     => null,
                 piv_source_keyvalue1    => null,
                 piv_error_type          => 'ERR_VAL',
                 piv_error_code          => l_err_code,
                 piv_error_message       => l_err_msg);
      lv_status := 'Y';
      return lv_status;
  end xxpo_po_dest_type;

  /*  --- 1.1
  +================================================================================+
  |FUNCTION NAME      : XXPO_DEST_ORG_ID                                       |
  |DESCRIPTION        : This function will derive DESTINATION_ORG_ID         |
  +================================================================================+
  */
  /*    FUNCTION xxpo_po_dest_org_id (
     p_in_interface_txn_id   IN   NUMBER,
     p_in_stg_tbl_name       IN   VARCHAR2,
     p_in_column_name        IN   VARCHAR2,
     p_in_dest_org           IN   VARCHAR2
  )
     RETURN NUMBER
  IS
     l_org_id     NUMBER;
     l_err_code   VARCHAR2 (40);
     l_err_msg    VARCHAR2 (2000);
  BEGIN
     SELECT organization_id
       INTO l_org_id
       FROM org_organization_definitions
      WHERE UPPER (organization_name) = UPPER (p_in_dest_org)
        AND SYSDATE BETWEEN NVL (user_definition_enable_date, SYSDATE)
                        AND NVL (disable_date, SYSDATE)
        AND inventory_enabled_flag = 'Y';

     RETURN l_org_id;
  EXCEPTION
     WHEN NO_DATA_FOUND
     THEN
        l_err_code := 'ETN_PO_INVALID_DESTINATION_ORG';
        l_err_msg := 'Error: Invalid Destination Org. ';
        log_errors (pin_interface_txn_id         => p_in_interface_txn_id,
                    piv_source_table             => p_in_stg_tbl_name,
                    piv_source_column_name       => p_in_column_name,
                    piv_source_column_value      => p_in_dest_org,
                    piv_source_keyname1          => NULL,
                    piv_source_keyvalue1         => NULL,
                    piv_error_type               => 'ERR_VAL',
                    piv_error_code               => l_err_code,
                    piv_error_message            => l_err_msg
                   );
        RETURN NULL;
     WHEN OTHERS
     THEN
        l_err_code := 'ETN_PO_INVALID_DESTINATION_ORG';
        l_err_msg :=
                   'Error: EXCEPTION error while deriving Destination Org. ';
        log_errors (pin_interface_txn_id         => p_in_interface_txn_id,
                    piv_source_table             => p_in_stg_tbl_name,
                    piv_source_column_name       => p_in_column_name,
                    piv_source_column_value      => p_in_dest_org,
                    piv_source_keyname1          => NULL,
                    piv_source_keyvalue1         => NULL,
                    piv_error_type               => 'ERR_VAL',
                    piv_error_code               => l_err_code,
                    piv_error_message            => l_err_msg
                   );
        RETURN NULL;
  END xxpo_po_dest_org_id; */
  /*  --- 1.1
  +================================================================================+
  |FUNCTION NAME : PO_DEST_SUBINlv                                             |
  |DESCRIPTION   : This function will check/validate DESTINATION_SUBINVENTORY |
  +================================================================================+
  */
  /* FUNCTION xxpo_po_dest_subinlv (
     p_in_interface_txn_id    IN   NUMBER,
     p_in_stg_tbl_name        IN   VARCHAR2,
     p_in_column_name         IN   VARCHAR2,
     p_in_oper_unit_id        IN   NUMBER,
     p_in_dest_subinventory   IN   VARCHAR2
  )
     RETURN VARCHAR2
  IS
     lv_status    VARCHAR2 (1)    := 'N';
     l_err_code   VARCHAR2 (40);
     l_err_msg    VARCHAR2 (2000);
  BEGIN
     SELECT 'N'
       INTO lv_status
       FROM mtl_secondary_inventories
      WHERE UPPER (p_in_dest_subinventory) = UPPER (secondary_inventory_name)
        AND organization_id = p_in_oper_unit_id
        AND SYSDATE <= NVL (disable_date, SYSDATE);

     RETURN lv_status;
  EXCEPTION
     WHEN NO_DATA_FOUND
     THEN
        l_err_code := 'ETN_PO_INVALID_SUBINVENTORY';
        l_err_msg := 'Error: Invalid destination subinventory value. ';
        log_errors (pin_interface_txn_id         => p_in_interface_txn_id,
                    piv_source_table             => p_in_stg_tbl_name,
                    piv_source_column_name       => p_in_column_name,
                    piv_source_column_value      => p_in_dest_subinventory,
                    piv_source_keyname1          => NULL,
                    piv_source_keyvalue1         => NULL,
                    piv_error_type               => 'ERR_VAL',
                    piv_error_code               => l_err_code,
                    piv_error_message            => l_err_msg
                   );
        lv_status := 'Y';
        RETURN lv_status;
     WHEN OTHERS
     THEN
        l_err_code := 'ETN_PO_INVALID_SUBINVENTORY';
        l_err_msg :=
           'Error: Exception error while validating destination subinventory. ';
        log_errors (pin_interface_txn_id         => p_in_interface_txn_id,
                    piv_source_table             => p_in_stg_tbl_name,
                    piv_source_column_name       => p_in_column_name,
                    piv_source_column_value      => p_in_dest_subinventory,
                    piv_source_keyname1          => NULL,
                    piv_source_keyvalue1         => NULL,
                    piv_error_type               => 'ERR_VAL',
                    piv_error_code               => l_err_code,
                    piv_error_message            => l_err_msg
                   );
        lv_status := 'Y';
        RETURN lv_status;
  END xxpo_po_dest_subinlv; */
  /*
  +================================================================================+
  |FUNCTION NAME : PO_ITEM_REVISION                                       |
  |DESCRIPTION   : This function will check/validate ITEM_REVISION        |
  +================================================================================+
  */
  function xxpo_po_item_revision(p_in_interface_txn_id  in number,
                                 p_in_stg_tbl_name      in varchar2,
                                 p_in_column_name       in varchar2,
                                 p_in_operating_unit_id in number,
                                 p_in_item_revision     in varchar2,
                                 p_in_item_id           in number)
    return varchar2 is
    lv_status  varchar2(1) := 'N';
    l_err_code varchar2(40);
    l_err_msg  varchar2(2000);
  begin
    select 'N'
      into lv_status
      from mtl_item_revisions
     where revision = p_in_item_revision
       and inventory_item_id = p_in_item_id
       and organization_id = p_in_operating_unit_id
       and sysdate >= nvl(effectivity_date, sysdate);
    return lv_status;
  exception
    when no_data_found then
      l_err_code := 'ETN_PO_INVALID_ITEM_REVISION';
      l_err_msg  := 'Error: Invalid Item Revision. ';
      log_errors(pin_interface_txn_id    => p_in_interface_txn_id,
                 piv_source_table        => p_in_stg_tbl_name,
                 piv_source_column_name  => p_in_column_name,
                 piv_source_column_value => p_in_item_revision,
                 piv_source_keyname1     => null,
                 piv_source_keyvalue1    => null,
                 piv_error_type          => 'ERR_VAL',
                 piv_error_code          => l_err_code,
                 piv_error_message       => l_err_msg);
      lv_status := 'Y';
      return lv_status;
    when others then
      l_err_code := 'ETN_PO_INVALID_ITEM_REVISION';
      l_err_msg  := 'Error: EXCEPTION error while validating Item Revision. ';
      log_errors(pin_interface_txn_id    => p_in_interface_txn_id,
                 piv_source_table        => p_in_stg_tbl_name,
                 piv_source_column_name  => p_in_column_name,
                 piv_source_column_value => p_in_item_revision,
                 piv_source_keyname1     => null,
                 piv_source_keyvalue1    => null,
                 piv_error_type          => 'ERR_VAL',
                 piv_error_code          => l_err_code,
                 piv_error_message       => l_err_msg);
      lv_status := 'Y';
      return lv_status;
  end xxpo_po_item_revision;

  /*
    +================================================================================+
    |PROCEDURE NAME : update_line_num                                                    |
    |DESCRIPTION   : This procedure will update the PO line numbers                        |
    +================================================================================+
  */
  procedure update_line_num is
    cursor line_upd_cur is
      select distinct leg_po_header_id,
                      leg_line_num,
                      leg_shipment_num,
                      interface_txn_id
        from xxpo_po_line_shipment_stg
       where shipment_attribute6 is null
         and process_flag = 'N'
         and batch_id = g_batch_id
         and run_sequence_id = g_new_run_seq_id
         and leg_operating_unit_name =
             nvl(g_leg_operating_unit, leg_operating_unit_name)
       order by leg_po_header_id, leg_line_num, leg_shipment_num asc;
    l_po_header_id number;
    l_leg_line_num varchar2(10);
    l_new_line_num varchar2(10);
    l_deci         varchar2(10);
    l_whole        varchar2(10);
  begin
    l_po_header_id := 0;
    l_leg_line_num := '0';
    for upd_line_rec in line_upd_cur loop
      if l_po_header_id <> upd_line_rec.leg_po_header_id or
         upd_line_rec.leg_line_num <> l_leg_line_num then
        --l_new_line_num := 0
        update xxpo_po_line_shipment_stg xpl
           set xpl.leg_item_description = substrb('L' ||
                                                  upd_line_rec.leg_line_num || 'S' ||
                                                  upd_line_rec.leg_shipment_num || '|' ||
                                                  leg_item_description,
                                                  1,
                                                  240),
               shipment_attribute6      = upd_line_rec.leg_line_num,
               shipment_attribute7      = upd_line_rec.leg_shipment_num,
               shipment_attribute8      = 'PO'
         where interface_txn_id = upd_line_rec.interface_txn_id;
        commit;
        l_po_header_id := upd_line_rec.leg_po_header_id;
        l_leg_line_num := upd_line_rec.leg_line_num;
        l_new_line_num := l_leg_line_num;
        /*dbms_output.put_line('PO header id :'||l_po_header_id||' Line num :'||l_leg_line_num
        ||' New line num :'||l_new_line_num);*/
        if instr(l_new_line_num, '.', 1) = 0 then
          l_new_line_num := l_new_line_num || '.0';
          /*dbms_output.put_line('there was no decimal so added'||l_new_line_num);*/
        end if;
      else
        /*dbms_output.put_line('Same PO Same line');*/
        l_deci  := 0;
        l_whole := 0;
        l_deci  := substr(l_new_line_num, instr(l_new_line_num, '.', 1) + 1);
        l_whole := substr(l_new_line_num,
                          1,
                          instr(l_new_line_num, '.', 1) - 1);
        /*dbms_output.put_line('l_deci :'||l_deci||' l_whole :'||l_whole);*/
        if substr(l_deci, -1, 1) = '9' then
          l_deci := l_deci + 2;
        else
          l_deci := l_deci + 1;
        end if;
        l_new_line_num := l_whole || '.' || l_deci;
        /*dbms_output.put_line('New line Number :'||l_new_line_num);*/
        update xxpo_po_line_shipment_stg xpl
           set xpl.leg_item_description = substrb('L' ||
                                                  upd_line_rec.leg_line_num || 'S' ||
                                                  upd_line_rec.leg_shipment_num || '|' ||
                                                  leg_item_description,
                                                  1,
                                                  240),
               shipment_attribute6      = upd_line_rec.leg_line_num,
               shipment_attribute7      = upd_line_rec.leg_shipment_num,
               shipment_attribute8      = 'PO',
               leg_line_num             = l_new_line_num
         where interface_txn_id = upd_line_rec.interface_txn_id;
        commit;
      end if;
    end loop;
  end update_line_num;

  /*
    +================================================================================+
    |PROCEDURE NAME : Validate_po                                                    |
    |DESCRIPTION   : This procedure will used for Validations                        |
    +================================================================================+
  */
  procedure validate_po is
    cursor cur_po_headers is
      select a.*
        from xxpo_po_header_stg a
       where a.process_flag in ('N', 'E') --1.1
         and a.batch_id = g_batch_id
         and a.run_sequence_id = g_new_run_seq_id
         and a.leg_operating_unit_name =
             nvl(g_leg_operating_unit, a.leg_operating_unit_name);
    cursor cur_po_lines(p_in_header_id          number,
                        p_in_leg_source_sys     varchar2,
                        p_in_leg_operating_unit varchar2) is
      select /*+ INDEX (a XXPO_PO_LINE_SHIPMENT_STG_N4) */
       a.*
        from xxpo_po_line_shipment_stg a
       where a.process_flag in ('N', 'E') --1.1
         and a.batch_id = g_batch_id
         and a.run_sequence_id = g_new_run_seq_id
         and a.leg_po_header_id = p_in_header_id
         and a.leg_source_system = p_in_leg_source_sys
         and a.leg_operating_unit_name = p_in_leg_operating_unit;
    /* cursor cur_po_lines_tax/ *(p_in_header_id          number,  ---1.20
                      p_in_leg_source_sys     varchar2,
                      p_in_leg_operating_unit varchar2)* / is
    select / *+ INDEX (a XXPO_PO_LINE_SHIPMENT_STG_N4) * /
     a.*
      from xxpo_po_line_shipment_stg a
     where a.process_flag in ('N', 'E') --1.1
       and a.batch_id = g_batch_id
       and a.run_sequence_id = g_new_run_seq_id;   */
    cursor cur_po_distributions(p_in_header_id          number,
                                p_in_leg_source_sys     varchar2,
                                p_in_leg_operating_unit varchar2,
                                p_in_line_id            number,
                                p_in_location_id        number,
                                p_in_shipment_number    number) is
      select /*+ INDEX (a XXPO_PO_DISTRIBUTION_STG_N4) */
       a.*
        from xxpo_po_distribution_stg a
       where a.process_flag in ('N', 'E') ---1.1
         and a.batch_id = g_batch_id
         and a.run_sequence_id = g_new_run_seq_id
         and a.leg_po_line_id = p_in_line_id
         and a.leg_po_header_id = p_in_header_id
         and a.leg_source_system = p_in_leg_source_sys
         and a.leg_operating_unit_name = p_in_leg_operating_unit
         and a.leg_po_line_location_id = p_in_location_id
         and a.leg_shipment_num = p_in_shipment_number;
    cursor cur_head_err(p_in_header_id          number,
                        p_in_leg_source_sys     varchar2,
                        p_in_leg_operating_unit varchar2) is
      select /*+ INDEX (a XXPO_PO_HEADER_STG_N13) */
       interface_txn_id, leg_document_num, leg_operating_unit_name
        from xxpo_po_header_stg a
       where leg_po_header_id = p_in_header_id
         and leg_operating_unit_name = p_in_leg_operating_unit
         and leg_source_system = p_in_leg_source_sys
         and process_flag = 'V';
    cursor cur_line_err(p_in_header_id          number,
                        p_in_leg_source_sys     varchar2,
                        p_in_leg_operating_unit varchar2) is
      select /*+ INDEX (a XXPO_PO_LINE_SHIPMENT_STG_N14) */
       interface_txn_id, leg_document_num, leg_operating_unit_name
        from xxpo_po_line_shipment_stg a
       where leg_po_header_id = p_in_header_id
         and leg_operating_unit_name = p_in_leg_operating_unit
         and leg_source_system = p_in_leg_source_sys
         and process_flag = 'V';
    cursor cur_dist_err(p_in_header_id          number,
                        p_in_leg_source_sys     varchar2,
                        p_in_leg_operating_unit varchar2) is
      select /*+ INDEX (a XXPO_PO_DISTRIBUTION_STG_N14) */
       interface_txn_id, leg_document_num, leg_operating_unit_name
        from xxpo_po_distribution_stg a
       where leg_po_header_id = p_in_header_id
         and leg_operating_unit_name = p_in_leg_operating_unit
         and leg_source_system = p_in_leg_source_sys
         and process_flag = 'V';
    cursor cur_opr is
      select distinct leg_charge_account_seg1
        from xxpo_po_distribution_stg
       where batch_id = g_batch_id
         and run_sequence_id = g_new_run_seq_id;
    cursor cur_currency is
      select distinct leg_currency_code
        from xxpo_po_header_stg
       where batch_id = g_batch_id
         and run_sequence_id = g_new_run_seq_id;
    --  V1.16         Changes for Organization Derivation Using Cross-Reference API (Used in Both Agent Number Derivation and Inventory org Derivation)
    cursor cur_plant(p_in_po_header_id number) is ---Added for Agent Number ----v1.14 (Cursor added for Agent Number)
      select leg_charge_account_seg1, leg_source_system --, leg_po_header_id --- Added For Agent Change v1.15 --changed SM
        from xxpo_po_distribution_stg
       where leg_po_header_id = p_in_po_header_id
         and batch_id = g_batch_id
         and leg_charge_account_seg1 is not null --changed
         and run_sequence_id = g_new_run_seq_id
         and rownum = 1;
    cursor cur_plant_tax_cross_ref is ---Added for Agent Number ----v1.14 (Cursor added for Agent Number)
      select xpl.leg_tax_name,
             xpd.leg_charge_account_seg1,
             xpl.leg_po_line_id,
             xpd.leg_po_distribution_id,
             xpl.org_id,
             xpl.leg_po_header_id
        from xxpo_po_distribution_stg xpd, xxpo_po_line_shipment_stg xpl
       where xpd.leg_po_header_id = xpl.leg_po_header_id
         and xpd.leg_po_line_id = xpl.leg_po_line_id
         and xpl.batch_id = xpd.batch_id
         and xpl.run_sequence_id = xpl.run_sequence_id
         and xpl.process_flag in ('N', 'E') --1.1
         and xpl.batch_id = g_batch_id
         and xpl.run_sequence_id = g_new_run_seq_id;
    cursor cur_agent is --v1.14      -----Rohit ---Redundant Cursor ---- move validation to cur_po_headers
      select distinct leg_agent_emp_no, leg_po_header_id -- Column ADDED for calling cursor cur_plant-- --v1.14
        from xxpo_po_header_stg
       where batch_id = g_batch_id
         and run_sequence_id = g_new_run_seq_id;
    cursor cur_ship_to_loc is
      select distinct leg_ship_to_location
        from xxpo_po_header_stg
       where batch_id = g_batch_id
         and run_sequence_id = g_new_run_seq_id;
    cursor cur_bill_to_loc is
      select distinct leg_bill_to_location
        from xxpo_po_header_stg
       where batch_id = g_batch_id
         and run_sequence_id = g_new_run_seq_id;
    cursor cur_pay is
      select distinct leg_payment_terms
        from xxpo_po_header_stg
       where batch_id = g_batch_id
         and run_sequence_id = g_new_run_seq_id;
    cursor cur_price is
      select distinct leg_price_type
        from xxpo_po_line_shipment_stg
       where batch_id = g_batch_id
         and run_sequence_id = g_new_run_seq_id;
    cursor cur_uom is
      select distinct leg_unit_of_measure
        from xxpo_po_line_shipment_stg
       where batch_id = g_batch_id
         and run_sequence_id = g_new_run_seq_id;
    cursor cur_receive is
      select distinct leg_receiving_routing
        from xxpo_po_line_shipment_stg
       where batch_id = g_batch_id
         and run_sequence_id = g_new_run_seq_id;
    cursor cur_line_type is
      select distinct leg_line_type
        from xxpo_po_line_shipment_stg
       where batch_id = g_batch_id
         and run_sequence_id = g_new_run_seq_id;
    cursor cur_un_number is
      select distinct leg_un_number
        from xxpo_po_line_shipment_stg
       where batch_id = g_batch_id
         and run_sequence_id = g_new_run_seq_id;
    cursor cur_hazard is
      select distinct leg_hazard_class
        from xxpo_po_line_shipment_stg
       where batch_id = g_batch_id
         and run_sequence_id = g_new_run_seq_id;
    cursor cur_ship_to_org is --V1.16 Changes for Organization Derivation Using Cross-Reference API  (Added leg_po_header_id)   --- Rohit put it in the main line cursor , similar to agent changes ... remove all explicit update and inner cursors
      select distinct leg_ship_to_organization_name,
                      leg_po_header_id ---- Not Needed---, org_id --changed for IO check --v1.14
                     ,
                      org_id -- aditya/SDP 13/62016
        from xxpo_po_line_shipment_stg
       where batch_id = g_batch_id
         and run_sequence_id = g_new_run_seq_id;
    cursor cur_item is
      select distinct leg_item, ship_to_organization_id
        from xxpo_po_line_shipment_stg
       where batch_id = g_batch_id
         and run_sequence_id = g_new_run_seq_id;
    cursor cur_category is
      select distinct leg_category_segment1, leg_item
        from xxpo_po_line_shipment_stg
       where batch_id = g_batch_id
         and run_sequence_id = g_new_run_seq_id;
    cursor cur_ship_to_loc_line is
      select distinct leg_ship_to_location
        from xxpo_po_line_shipment_stg
       where batch_id = g_batch_id
         and run_sequence_id = g_new_run_seq_id;
    cursor cur_dest_org is
      select distinct leg_destination_organization,
                      leg_po_header_id --V1.16 --- Not Needed--, org_id ---added for OU check --v1.14  ----rohit Use plant in the select clause instead of header ID .... in the inner loop use join for plant and destination org
                     ,
                      org_id -- aditya/SDP 13/62016
        from xxpo_po_distribution_stg
       where batch_id = g_batch_id
         and run_sequence_id = g_new_run_seq_id;
    cursor cur_subinv is
      select distinct leg_destination_subinventory,
                      destination_organization_id
        from xxpo_po_distribution_stg
       where batch_id = g_batch_id
         and run_sequence_id = g_new_run_seq_id;
    cursor cur_code is
      select leg_charge_account_seg1 segment1,
             leg_charge_account_seg2 segment2,
             leg_charge_account_seg3 segment3,
             leg_charge_account_seg4 segment4,
             leg_charge_account_seg5 segment5,
             leg_charge_account_seg6 segment6,
             leg_charge_account_seg7 segment7
        from xxpo_po_distribution_stg
       where batch_id = g_batch_id
         and run_sequence_id = g_new_run_seq_id
      union
      select leg_accural_account_seg1 segment1,
             leg_accural_account_seg2 segment2,
             leg_accural_account_seg3 segment3,
             leg_accural_account_seg4 segment4,
             leg_accural_account_seg5 segment5,
             leg_accural_account_seg6 segment6,
             leg_accural_account_seg7 segment7
        from xxpo_po_distribution_stg
       where batch_id = g_batch_id
         and run_sequence_id = g_new_run_seq_id
      union
      select leg_variance_account_seg1 segment1,
             leg_variance_account_seg2 segment2,
             leg_variance_account_seg3 segment3,
             leg_variance_account_seg4 segment4,
             leg_variance_account_seg5 segment5,
             leg_variance_account_seg6 segment6,
             leg_variance_account_seg7 segment7
        from xxpo_po_distribution_stg
       where batch_id = g_batch_id
         and run_sequence_id = g_new_run_seq_id;
    lv_val_flag                    varchar2(1);
    lv_error_flag                  varchar2(1) := 'N';
    lv_header_error_flag           varchar2(1) := 'N';
    lv_line_err_flag               varchar2(1) := 'N';
    lv_d_line_err_flag             varchar2(1) := 'N';
    lv_mast_d_line_err_flag        varchar2(1) := 'N';
    lv_mast_line_err_flag          varchar2(1) := 'N';
    l_err_code                     varchar2(40);
    l_err_msg                      varchar2(2000);
    lv_org_id                      number;
    lv_org_name                    varchar2(240);
    lv_func_currency_code          varchar2(15);
    lv_agent_id                    number;
    v_interface_txn_id             number; --SM MOCK3 Changes
    new_lv_agent_id                number;
    lv_vendor_id                   number;
    lv_vendor_site_id              number;
    lv_ship_to_location_id         number;
    lv_bill_to_location_id         number;
    lv_vendor_contact_id           number;
    lv_terms_id                    number;
    lv_pay_name                    varchar2(240);
    lv_dep_receiving_routing_id    number;
    lv_dep_line_type_id            number;
    lv_dep_uom_code                varchar(3);
    lv_dep_hazard_class_id         number;
    lv_dep_un_number_id            number;
    lv_dep_ship_to_organization_id number;
    lv_dep_item_id                 number;
    lv_dep_ship_to_location_id     number;
    lv_dep_tax_code_id             number;
    lv_dest_org_id                 number;
    lv_dist_org_id                 number;
    lv_dist_org_name               varchar2(240);
    lv_d_dep_deliver_to_location   number;
    lv_d_dep_deliver_to_person_id  number;
    x_out_charge_acc_rec           xxetn_common_pkg.g_rec_type;
    x_charge_ccid                  number;
    x_out_accrual_acc_rec          xxetn_common_pkg.g_rec_type;
    x_accrual_ccid                 number;
    x_out_variance_acc_rec         xxetn_common_pkg.g_rec_type;
    x_variance_ccid                number;
    lv_attribute_category          varchar2(30);
    lv_attribute1                  varchar2(150);
    lv_attribute2                  varchar2(150);
    lv_attribute3                  varchar2(150);
    lv_attribute4                  varchar2(150);
    lv_attribute5                  varchar2(150);
    lv_attribute6                  varchar2(150);
    lv_attribute7                  varchar2(150);
    lv_attribute8                  varchar2(150);
    lv_attribute9                  varchar2(150);
    lv_attribute10                 varchar2(150);
    lv_attribute11                 varchar2(150);
    lv_attribute12                 varchar2(150);
    lv_attribute13                 varchar2(150);
    lv_attribute14                 varchar2(150);
    lv_attribute15                 varchar2(150);
    lv_l_attribute_category        varchar2(30);
    lv_l_attribute1                varchar2(150);
    lv_l_attribute2                varchar2(150);
    lv_l_attribute3                varchar2(150);
    lv_l_attribute4                varchar2(150);
    lv_l_attribute5                varchar2(150);
    lv_l_attribute6                varchar2(150);
    lv_l_attribute7                varchar2(150);
    lv_l_attribute8                varchar2(150);
    lv_l_attribute9                varchar2(150);
    lv_l_attribute10               varchar2(150);
    lv_l_attribute11               varchar2(150);
    lv_l_attribute12               varchar2(150);
    lv_l_attribute13               varchar2(150);
    lv_l_attribute14               varchar2(150);
    lv_l_attribute15               varchar2(150);
    lv_d_attribute_category        varchar2(30);
    lv_d_attribute1                varchar2(150);
    lv_d_attribute2                varchar2(150);
    lv_d_attribute3                varchar2(150);
    lv_d_attribute4                varchar2(150);
    lv_d_attribute5                varchar2(150);
    lv_d_attribute6                varchar2(150);
    lv_d_attribute7                varchar2(150);
    lv_d_attribute8                varchar2(150);
    lv_d_attribute9                varchar2(150);
    lv_d_attribute10               varchar2(150);
    lv_d_attribute11               varchar2(150);
    lv_d_attribute12               varchar2(150);
    lv_d_attribute13               varchar2(150);
    lv_d_attribute14               varchar2(150);
    lv_d_attribute15               varchar2(150);
    lv_ship_attribute_category     varchar2(30);
    lv_ship_attribute1             varchar2(150);
    lv_ship_attribute2             varchar2(150);
    lv_ship_attribute3             varchar2(150);
    lv_ship_attribute4             varchar2(150);
    lv_ship_attribute5             varchar2(150);
    lv_ship_attribute6             varchar2(150);
    lv_ship_attribute7             varchar2(150);
    lv_ship_attribute8             varchar2(150);
    lv_ship_attribute9             varchar2(150);
    lv_ship_attribute10            varchar2(150);
    lv_ship_attribute11            varchar2(150);
    lv_ship_attribute12            varchar2(150);
    lv_ship_attribute13            varchar2(150);
    lv_ship_attribute14            varchar2(150);
    lv_ship_attribute15            varchar2(150);
    lv_d_dep_project_id            number;
    lv_d_prj_type                  varchar2(30);
    lv_exp_org_id                  number;
    lv_exp_org_name                varchar2(240);
    lv_l_org_id                    number;
    lv_l_org_name                  varchar2(240);
    lv_l_terms_id                  number;
    lv_l_pay_name                  varchar2(240);
    lv_tax_name                    varchar2(30);
    lv_set_of_books_id             number;
    lv_d_dep_task_id               number;
    lv_dep_category_id             number;
    l_count                        number := 0;
    lv_status                      varchar2(1) := 'N';
    l_cat_set_struc_id             number := null;
    l_tot_dist_qty                 number;
    l_oper_unit                    xxetn_map_unit.operating_unit%type;
    l_rec                          xxetn_map_util.g_input_rec;
    lv_line_num_new                xxpo_po_line_shipment_stg.leg_line_num%type;
    l_linenum1                     number;
    l_linenum2                     number;
    l_description                  xxpo_po_line_shipment_stg.leg_item_description%type;
    lv_uom_description             fnd_lookup_values.description%type; ---v1.14 (UOM Code Change)
    v_charge_account_seg1          varchar2(10);
    v_po_header_id                 number; ---v1.15 (AGENT Code Change)
    ---------new variables for Cross reference API-----V1.16
    lv_ship_to_org_id     number;
    l_err_code1           varchar2(40);
    lv_err_msg1           varchar2(2000); --API Variables
    lv_out_val1           varchar2(200); --API Variables
    lv_out_val2           varchar2(200); --API Variables
    lv_out_val3           varchar2(200); --API Variables
    p_out_org_name        xxpo_po_line_shipment_ext_r12.leg_ship_to_organization_name%type;
    p_in_ship_to_org_name xxpo_po_line_shipment_ext_r12.leg_ship_to_organization_name%type;
    p_in_plant_num        xxpo_po_distribution_ext_r12.leg_charge_account_seg1%type;
    --v.xx
    l_column_name  varchar2(100);
    l_column_value varchar2(240);
    lv_errm        varchar2(1);
    --v.xx ends
    ---------new variables for Cross reference API ends-----V1.16
    ---------new variables for Cross reference API for Tax Changes -----V1.19
    lv_err_message   varchar2(2000); --API Variables
    lv_out_value1    varchar2(200); --API Variables
    lv_out_value2    varchar2(200); --API Variables
    lv_out_value3    varchar2(200); --API Variables
    p_out_tax_name1  xxpo_po_line_shipment_ext_r12.leg_tax_name%type; --
    p_out_tax_name2  xxpo_po_line_shipment_ext_r12.leg_tax_name%type; --
    p_in_tax_name    xxpo_po_line_shipment_ext_r12.leg_tax_name%type; --
    p_in_site_num    xxpo_po_distribution_ext_r12.leg_charge_account_seg1%type; --
    new_leg_tax_name xxpo_po_line_shipment_ext_r12.leg_tax_name%type;
    ---------new variables for Cross reference API for Tax Changes-----V1.19
  begin
    xxetn_debug_pkg.add_debug('INSIDE VALIDATE PROCEDURE ');
    g_retcode := 0;
    --ADB
    update_line_num;
    --ADB ends
    for cur_opr_rec in cur_opr loop
      lv_org_name        := null;
      lv_org_id          := null;
      lv_set_of_books_id := null;
      if cur_opr_rec.leg_charge_account_seg1 is null then
        for r_org_ref_err_rec in (select /*+ INDEX ( xis XXPO_PO_DISTRIBUTION_STG_N13 ) */
                                   interface_txn_id
                                    from xxpo_po_distribution_stg xis
                                   where leg_charge_account_seg1 =
                                         cur_opr_rec.leg_charge_account_seg1
                                     and batch_id = g_batch_id
                                     and run_sequence_id = g_new_run_seq_id) loop
          l_err_code := 'ETN_PO_MANDATORY_NOT_ENTERED';
          l_err_msg  := 'Error: Mandatory column not entered.  ';
          log_errors(pin_interface_txn_id    => r_org_ref_err_rec.interface_txn_id,
                     piv_source_table        => 'xxpo_po_distribution_stg',
                     piv_source_column_name  => 'leg_charge_account_seg1',
                     piv_source_column_value => cur_opr_rec.leg_charge_account_seg1,
                     piv_source_keyname1     => null,
                     piv_source_keyvalue1    => null,
                     piv_error_type          => 'ERR_VAL',
                     piv_error_code          => l_err_code,
                     piv_error_message       => l_err_msg);
        end loop;
        update /*+ INDEX ( xis XXPO_PO_DISTRIBUTION_STG_N13 ) */ xxpo_po_distribution_stg xis
           set process_flag      = 'E',
               error_type        = 'ERR_VAL',
               last_updated_date = sysdate,
               last_updated_by   = g_last_updated_by,
               last_update_login = g_login_id
         where leg_charge_account_seg1 is null
           and batch_id = g_batch_id
           and run_sequence_id = g_new_run_seq_id;
        commit;
      else
        l_rec.site  := cur_opr_rec.leg_charge_account_seg1;
        lv_org_name := xxetn_map_util.get_value(l_rec).operating_unit;
        if lv_org_name is null then
          for r_org_ref_err_rec in (select /*+ INDEX ( xis XXPO_PO_DISTRIBUTION_STG_N13 ) */
                                     interface_txn_id
                                      from xxpo_po_distribution_stg xis
                                     where leg_charge_account_seg1 =
                                           cur_opr_rec.leg_charge_account_seg1
                                       and batch_id = g_batch_id
                                       and run_sequence_id =
                                           g_new_run_seq_id) loop
            l_err_code := 'ETN_PO_OPERATING_UNIT_ERROR';
            l_err_msg  := 'Error: Mapping not defined in the Common Crossreference for OU. ';
            log_errors(pin_interface_txn_id    => r_org_ref_err_rec.interface_txn_id,
                       piv_source_table        => 'xxpo_po_distribution_stg',
                       piv_source_column_name  => 'leg_charge_account_seg1',
                       piv_source_column_value => cur_opr_rec.leg_charge_account_seg1,
                       piv_source_keyname1     => null,
                       piv_source_keyvalue1    => null,
                       piv_error_type          => 'ERR_VAL',
                       piv_error_code          => l_err_code,
                       piv_error_message       => l_err_msg);
          end loop;
          update /*+ INDEX ( xis XXPO_PO_DISTRIBUTION_STG_N13 ) */ xxpo_po_distribution_stg xis
             set process_flag      = 'E',
                 error_type        = 'ERR_VAL',
                 last_updated_date = sysdate,
                 last_updated_by   = g_last_updated_by,
                 last_update_login = g_login_id
           where leg_charge_account_seg1 =
                 cur_opr_rec.leg_charge_account_seg1
             and batch_id = g_batch_id
             and run_sequence_id = g_new_run_seq_id;
          commit;
        else
          begin
            select organization_id
              into lv_org_id
              from hr_operating_units
             where name = lv_org_name
               and sysdate between nvl(date_from, sysdate) and
                   nvl(date_to, sysdate);
            begin
              select set_of_books_id
                into lv_set_of_books_id
                from hr_operating_units
               where organization_id = lv_org_id;
            exception
              when others then
                lv_set_of_books_id := null;
            end;
            update /*+ INDEX ( xis XXPO_PO_DISTRIBUTION_STG_N13 ) */ xxpo_po_distribution_stg xis
               set org_id              = lv_org_id,
                   operating_unit_name = lv_org_name,
                   set_of_books_id     = lv_set_of_books_id,
                   last_updated_date   = sysdate,
                   last_updated_by     = g_last_updated_by,
                   last_update_login   = g_login_id
             where leg_charge_account_seg1 =
                   cur_opr_rec.leg_charge_account_seg1
               and batch_id = g_batch_id
               and run_sequence_id = g_new_run_seq_id;
            commit;
          exception
            when others then
              for r_org_ref_err_rec in (select /*+ INDEX ( xis XXPO_PO_DISTRIBUTION_STG_N13 ) */
                                         interface_txn_id
                                          from xxpo_po_distribution_stg xis
                                         where leg_charge_account_seg1 =
                                               cur_opr_rec.leg_charge_account_seg1
                                           and batch_id = g_batch_id
                                           and run_sequence_id =
                                               g_new_run_seq_id) loop
                l_err_code := 'ETN_PO_OPERATING_UNIT_ERROR';
                l_err_msg  := 'Error: Org ID could not be derived. ';
                log_errors(pin_interface_txn_id    => r_org_ref_err_rec.interface_txn_id,
                           piv_source_table        => 'xxpo_po_distribution_stg',
                           piv_source_column_name  => 'leg_charge_account_seg1',
                           piv_source_column_value => cur_opr_rec.leg_charge_account_seg1,
                           piv_source_keyname1     => null,
                           piv_source_keyvalue1    => null,
                           piv_error_type          => 'ERR_VAL',
                           piv_error_code          => l_err_code,
                           piv_error_message       => l_err_msg);
              end loop;
              update /*+ INDEX ( xis XXPO_PO_DISTRIBUTION_STG_N13 ) */ xxpo_po_distribution_stg xis
                 set process_flag      = 'E',
                     error_type        = 'ERR_VAL',
                     last_updated_date = sysdate,
                     last_updated_by   = g_last_updated_by,
                     last_update_login = g_login_id
               where leg_charge_account_seg1 =
                     cur_opr_rec.leg_charge_account_seg1
                 and batch_id = g_batch_id
                 and run_sequence_id = g_new_run_seq_id;
              commit;
          end;
        end if;
      end if;
    end loop;
    /** Commented below queries for V1.3

    UPDATE xxpo_po_header_stg a
       SET (a.org_id, a.operating_unit_name) =
           (SELECT b.org_id, b.operating_unit_name
              FROM xxpo_po_distribution_stg b
             WHERE a.leg_po_header_id = b.leg_po_header_id
               AND a.leg_source_system = b.leg_source_system
               AND b.org_id IS NOT NULL
               AND batch_id = g_batch_id
               AND run_sequence_id = g_new_run_seq_id
               AND ROWNUM = 1)
     WHERE batch_id = g_batch_id
       AND run_sequence_id = g_new_run_seq_id;

    UPDATE xxpo_po_line_shipment_stg a
       SET (a.org_id, a.operating_unit_name) =
           (SELECT b.org_id, b.operating_unit_name
              FROM xxpo_po_distribution_stg b
             WHERE a.leg_po_header_id = b.leg_po_header_id
               AND a.leg_source_system = b.leg_source_system
               AND b.org_id IS NOT NULL
               AND batch_id = g_batch_id
               AND run_sequence_id = g_new_run_seq_id
               AND ROWNUM = 1)
     WHERE batch_id = g_batch_id
       AND run_sequence_id = g_new_run_seq_id; **/
    /** Added below queries for V1.3 **/
    update xxpo_po_header_stg a
       set (a.org_id, a.operating_unit_name) =
           (select /*+ INDEX (b XXPO_PO_DISTRIBUTION_STG_N5) */
             b.org_id, b.operating_unit_name
              from xxpo_po_distribution_stg b
             where a.leg_po_header_id = b.leg_po_header_id
               and a.leg_source_system = b.leg_source_system
               and b.org_id is not null
               and a.batch_id = b.batch_id
               and a.run_sequence_id = b.run_sequence_id
               and rownum = 1)
     where a.batch_id = g_batch_id
       and a.run_sequence_id = g_new_run_seq_id;
    update xxpo_po_line_shipment_stg a
       set (a.org_id, a.operating_unit_name) =
           (select /*+ INDEX (b XXPO_PO_DISTRIBUTION_STG_N5) */
             b.org_id, b.operating_unit_name
              from xxpo_po_distribution_stg b
             where a.leg_po_header_id = b.leg_po_header_id
               and a.leg_source_system = b.leg_source_system
               and b.org_id is not null
               and a.batch_id = b.batch_id
               and a.run_sequence_id = b.run_sequence_id
               and rownum = 1)
     where a.batch_id = g_batch_id
       and a.run_sequence_id = g_new_run_seq_id;
    commit;
    -----------Changes for Location--------------  PMC 334707 ?- V1.18
    update xxpo_po_header_stg a
       set a.leg_ship_to_location =
           (select flv.description
              from fnd_lookup_values flv,
                   fnd_application   fa,
                   fnd_lookup_types  flt
             where flv.lookup_type = 'XXETN_IN_LOCATION_MAP'
               and flv.language = userenv('LANG')
               and flv.enabled_flag = 'Y'
               and flv.meaning = a.leg_ship_to_location
               and fa.application_id = flt.view_application_id
               and flt.lookup_type = flv.lookup_type)
    --  and fa.application_short_name = 'PO') Removed in MOCK3 SM
     where exists (select '1'
              from fnd_lookup_values flv,
                   fnd_application   fa,
                   fnd_lookup_types  flt
             where flv.lookup_type = 'XXETN_IN_LOCATION_MAP'
               and flv.language = userenv('LANG')
               and flv.enabled_flag = 'Y'
               and flv.meaning = a.leg_ship_to_location
               and fa.application_id = flt.view_application_id
               and flt.lookup_type = flv.lookup_type)
          --   and fa.application_short_name = 'PO') Removed in MOCK3 SM
       and a.batch_id = g_batch_id
       and a.run_sequence_id = g_new_run_seq_id;
    commit;
    update xxpo_po_header_stg a
       set a.leg_bill_to_location =
           (select flv.description
              from fnd_lookup_values flv,
                   fnd_application   fa,
                   fnd_lookup_types  flt
             where flv.lookup_type = 'XXETN_IN_LOCATION_MAP'
               and flv.language = userenv('LANG')
               and flv.enabled_flag = 'Y'
               and flv.meaning = a.leg_bill_to_location
               and fa.application_id = flt.view_application_id
               and flt.lookup_type = flv.lookup_type)
    -- and fa.application_short_name = 'PO') Removed in MOCK3 SM
     where exists (select '1'
              from fnd_lookup_values flv,
                   fnd_application   fa,
                   fnd_lookup_types  flt
             where flv.lookup_type = 'XXETN_IN_LOCATION_MAP'
               and flv.language = userenv('LANG')
               and flv.enabled_flag = 'Y'
               and flv.meaning = a.leg_bill_to_location
               and fa.application_id = flt.view_application_id
               and flt.lookup_type = flv.lookup_type)
          --  and fa.application_short_name = 'PO')Removed in MOCK3 SM
       and a.batch_id = g_batch_id
       and a.run_sequence_id = g_new_run_seq_id;
    commit;
    update xxpo_po_line_shipment_stg b
       set b.leg_ship_to_location =
           (select flv.description
              from fnd_lookup_values flv,
                   fnd_application   fa,
                   fnd_lookup_types  flt
             where flv.lookup_type = 'XXETN_IN_LOCATION_MAP'
               and flv.language = userenv('LANG')
               and flv.enabled_flag = 'Y'
               and flv.meaning = b.leg_ship_to_location
               and fa.application_id = flt.view_application_id
               and flt.lookup_type = flv.lookup_type)
    --  and fa.application_short_name = 'PO') Removed in MOCK3
     where exists (select '1'
              from fnd_lookup_values flv,
                   fnd_application   fa,
                   fnd_lookup_types  flt
             where flv.lookup_type = 'XXETN_IN_LOCATION_MAP'
               and flv.language = userenv('LANG')
               and flv.enabled_flag = 'Y'
               and flv.meaning = b.leg_ship_to_location
               and fa.application_id = flt.view_application_id
               and flt.lookup_type = flv.lookup_type)
          --  and fa.application_short_name = 'PO'  Removed in MOCK3 SM
       and b.batch_id = g_batch_id
       and b.run_sequence_id = g_new_run_seq_id;
    commit;
    update xxpo_po_distribution_stg c
       set c.leg_deliver_to_location =
           (select flv.description
              from fnd_lookup_values flv,
                   fnd_application   fa,
                   fnd_lookup_types  flt
             where flv.lookup_type = 'XXETN_IN_LOCATION_MAP'
               and flv.language = userenv('LANG')
               and flv.enabled_flag = 'Y'
               and flv.meaning = c.leg_deliver_to_location
               and fa.application_id = flt.view_application_id
               and flt.lookup_type = flv.lookup_type)
    --  and fa.application_short_name = 'PO' Removed in MOCK3 SM
     where exists (select '1'
              from fnd_lookup_values flv,
                   fnd_application   fa,
                   fnd_lookup_types  flt
             where flv.lookup_type = 'XXETN_IN_LOCATION_MAP'
               and flv.language = userenv('LANG')
               and flv.enabled_flag = 'Y'
               and flv.meaning = c.leg_deliver_to_location
               and fa.application_id = flt.view_application_id
               and flt.lookup_type = flv.lookup_type)
          -- and fa.application_short_name = 'PO' Removed in MOCK3 SM
       and c.batch_id = g_batch_id
       and c.run_sequence_id = g_new_run_seq_id;
    commit;
    -----------Changes for Location End--------------  PMC 334707 ?- V1.18
    /*  FOR cur_opr_rec IN cur_opr
    LOOP
       lv_org_name := NULL;
       lv_org_id := NULL;
       lv_set_of_books_id := NULL;

       IF cur_opr_rec.leg_operating_unit_name IS NULL
       THEN
          FOR r_org_ref_err_rec IN
             (SELECT interface_txn_id
                FROM xxpo_po_header_stg xis
               WHERE leg_operating_unit_name =
                                         cur_opr_rec.leg_operating_unit_name
                 AND batch_id = g_batch_id
                 AND run_sequence_id = g_new_run_seq_id)
          LOOP
             l_err_code := 'ETN_PO_MANDATORY_NOT_ENTERED';
             l_err_msg := 'Error: Mandatory column not entered.  ';
             log_errors
                (pin_interface_txn_id         => r_org_ref_err_rec.interface_txn_id,
                 piv_source_table             => 'xxpo_po_header_stg',
                 piv_source_column_name       => 'leg_operating_unit_name',
                 piv_source_column_value      => cur_opr_rec.leg_operating_unit_name,
                 piv_source_keyname1          => NULL,
                 piv_source_keyvalue1         => NULL,
                 piv_error_type               => 'ERR_VAL',
                 piv_error_code               => l_err_code,
                 piv_error_message            => l_err_msg
                );
          END LOOP;

          UPDATE xxpo_po_header_stg
             SET process_flag = 'E',
                 ERROR_TYPE = 'ERR_VAL',
                 last_updated_date = SYSDATE,
                 last_updated_by = g_last_updated_by,
                 last_update_login = g_login_id
           WHERE leg_operating_unit_name IS NULL
             AND batch_id = g_batch_id
             AND run_sequence_id = g_new_run_seq_id;

          COMMIT;
       ELSE
          BEGIN
             SELECT flv.description
               INTO lv_org_name
               FROM fnd_lookup_values flv
              WHERE flv.meaning =
                          cur_opr_rec.leg_operating_unit_name
                AND flv.LANGUAGE = USERENV ('LANG')
                AND flv.enabled_flag = 'Y'
                AND flv.lookup_type = g_ou_lookup
                AND TRUNC (SYSDATE) BETWEEN TRUNC
                                                (NVL (flv.start_date_active,
                                                      SYSDATE
                                                     )
                                                )
                                        AND TRUNC (NVL (flv.end_date_active,
                                                        SYSDATE
                                                       )
                                                  );
          EXCEPTION
             WHEN OTHERS
             THEN
                FOR r_org_ref_err_rec IN
                   (SELECT interface_txn_id
                      FROM xxpo_po_header_stg xis
                     WHERE leg_operating_unit_name =
                                         cur_opr_rec.leg_operating_unit_name
                       AND batch_id = g_batch_id
                       AND run_sequence_id = g_new_run_seq_id)
                LOOP
                   l_err_code := 'ETN_PO_OPERATING_UNIT_ERROR';
                   l_err_msg :=
                      'Error: Mapping not defined in the Common OU lookup. ';
                   log_errors
                      (pin_interface_txn_id         => r_org_ref_err_rec.interface_txn_id,
                       piv_source_table             => 'xxpo_po_header_stg',
                       piv_source_column_name       => 'leg_operating_unit_name',
                       piv_source_column_value      => cur_opr_rec.leg_operating_unit_name,
                       piv_source_keyname1          => NULL,
                       piv_source_keyvalue1         => NULL,
                       piv_error_type               => 'ERR_VAL',
                       piv_error_code               => l_err_code,
                       piv_error_message            => l_err_msg
                      );
                END LOOP;

                UPDATE xxpo_po_header_stg
                   SET process_flag = 'E',
                       ERROR_TYPE = 'ERR_VAL',
                       last_updated_date = SYSDATE,
                       last_updated_by = g_last_updated_by,
                       last_update_login = g_login_id
                 WHERE leg_operating_unit_name =
                                         cur_opr_rec.leg_operating_unit_name
                   AND batch_id = g_batch_id
                   AND run_sequence_id = g_new_run_seq_id;

                COMMIT;
          END;

          IF lv_org_name IS NOT NULL
          THEN
             BEGIN
                SELECT organization_id
                  INTO lv_org_id
                  FROM hr_operating_units
                 WHERE NAME = lv_org_name
                   AND SYSDATE BETWEEN NVL (date_from, SYSDATE)
                                   AND NVL (date_to, SYSDATE);

                BEGIN
                   SELECT set_of_books_id
                     INTO lv_set_of_books_id
                     FROM hr_operating_units
                    WHERE organization_id = lv_org_id;
                EXCEPTION
                   WHEN OTHERS
                   THEN
                      lv_set_of_books_id := NULL;
                END;

                UPDATE xxpo_po_header_stg
                   SET org_id = lv_org_id,
                       operating_unit_name = lv_org_name,
                       last_updated_date = SYSDATE,
                       last_updated_by = g_last_updated_by,
                       last_update_login = g_login_id
                 WHERE leg_operating_unit_name =
                                         cur_opr_rec.leg_operating_unit_name
                   AND batch_id = g_batch_id
                   AND run_sequence_id = g_new_run_seq_id;

                UPDATE xxpo_po_line_shipment_stg
                   SET org_id = lv_org_id,
                       operating_unit_name = lv_org_name,
                       last_updated_date = SYSDATE,
                       last_updated_by = g_last_updated_by,
                       last_update_login = g_login_id
                 WHERE leg_operating_unit_name =
                                         cur_opr_rec.leg_operating_unit_name
                   AND batch_id = g_batch_id
                   AND run_sequence_id = g_new_run_seq_id;

                UPDATE xxpo_po_distribution_stg
                   SET org_id = lv_org_id,
                       operating_unit_name = lv_org_name,
                       set_of_books_id = lv_set_of_books_id,
           expenditure_organization_id = lv_org_id,
                       last_updated_date = SYSDATE,
                       last_updated_by = g_last_updated_by,
                       last_update_login = g_login_id
                 WHERE leg_operating_unit_name =
                                         cur_opr_rec.leg_operating_unit_name
                   AND batch_id = g_batch_id
                   AND run_sequence_id = g_new_run_seq_id;

                COMMIT;
             EXCEPTION
                WHEN OTHERS
                THEN
                   FOR r_org_ref_err_rec IN
                      (SELECT interface_txn_id
                         FROM xxpo_po_header_stg xis
                        WHERE leg_operating_unit_name =
                                         cur_opr_rec.leg_operating_unit_name
                          AND batch_id = g_batch_id
                          AND run_sequence_id = g_new_run_seq_id)
                   LOOP
                      l_err_code := 'ETN_PO_OPERATING_UNIT_ERROR';
                      l_err_msg := 'Error: Org ID could not be derived. ';
                      log_errors
                         (pin_interface_txn_id         => r_org_ref_err_rec.interface_txn_id,
                          piv_source_table             => 'xxpo_po_header_stg',
                          piv_source_column_name       => 'leg_operating_unit_name',
                          piv_source_column_value      => cur_opr_rec.leg_operating_unit_name,
                          piv_source_keyname1          => NULL,
                          piv_source_keyvalue1         => NULL,
                          piv_error_type               => 'ERR_VAL',
                          piv_error_code               => l_err_code,
                          piv_error_message            => l_err_msg
                         );
                   END LOOP;

                   UPDATE xxpo_po_header_stg
                      SET process_flag = 'E',
                          ERROR_TYPE = 'ERR_VAL',
                          last_updated_date = SYSDATE,
                          last_updated_by = g_last_updated_by,
                          last_update_login = g_login_id
                    WHERE leg_operating_unit_name =
                                         cur_opr_rec.leg_operating_unit_name
                      AND batch_id = g_batch_id
                      AND run_sequence_id = g_new_run_seq_id;

                   COMMIT;
             END;
          END IF;
       END IF;
    END LOOP; */
    for cur_currency_rec in cur_currency loop
      lv_status := 'N';
      if cur_currency_rec.leg_currency_code is null then
        for r_org_ref_err_rec in (select /*+ INDEX (xis XXPO_PO_HEADER_STG_N6) */
                                   interface_txn_id
                                    from xxpo_po_header_stg xis
                                   where leg_currency_code =
                                         cur_currency_rec.leg_currency_code
                                     and batch_id = g_batch_id
                                     and run_sequence_id = g_new_run_seq_id) loop
          l_err_code := 'ETN_PO_MANDATORY_NOT_ENTERED';
          l_err_msg  := 'Error: Mandatory column not entered.  ';
          log_errors(pin_interface_txn_id    => r_org_ref_err_rec.interface_txn_id,
                     piv_source_table        => 'xxpo_po_header_stg',
                     piv_source_column_name  => 'leg_currency_code',
                     piv_source_column_value => cur_currency_rec.leg_currency_code,
                     piv_source_keyname1     => null,
                     piv_source_keyvalue1    => null,
                     piv_error_type          => 'ERR_VAL',
                     piv_error_code          => l_err_code,
                     piv_error_message       => l_err_msg);
        end loop;
        update /*+ INDEX (xis XXPO_PO_HEADER_STG_N6) */ xxpo_po_header_stg xis
           set process_flag      = 'E',
               error_type        = 'ERR_VAL',
               last_updated_date = sysdate,
               last_updated_by   = g_last_updated_by,
               last_update_login = g_login_id
         where leg_currency_code is null
           and batch_id = g_batch_id
           and run_sequence_id = g_new_run_seq_id;
        commit;
      else
        begin
          select 'N'
            into lv_status
            from fnd_currencies
           where currency_code = cur_currency_rec.leg_currency_code
             and sysdate between nvl(start_date_active, sysdate) and
                 nvl(end_date_active, sysdate)
             and enabled_flag = 'Y';
        exception
          when others then
            for r_org_ref_err_rec in (select /*+ INDEX (xis XXPO_PO_HEADER_STG_N6) */
                                       interface_txn_id
                                        from xxpo_po_header_stg xis
                                       where leg_currency_code =
                                             cur_currency_rec.leg_currency_code
                                         and batch_id = g_batch_id
                                         and run_sequence_id =
                                             g_new_run_seq_id) loop
              l_err_code := 'ETN_PO_INVALID_CURRENCY_CODE';
              l_err_msg  := 'Error: Invalid Currency code. ';
              log_errors(pin_interface_txn_id    => r_org_ref_err_rec.interface_txn_id,
                         piv_source_table        => 'xxpo_po_header_stg',
                         piv_source_column_name  => 'leg_currency_code',
                         piv_source_column_value => cur_currency_rec.leg_currency_code,
                         piv_source_keyname1     => null,
                         piv_source_keyvalue1    => null,
                         piv_error_type          => 'ERR_VAL',
                         piv_error_code          => l_err_code,
                         piv_error_message       => l_err_msg);
            end loop;
            update /*+ INDEX (xis XXPO_PO_HEADER_STG_N6) */ xxpo_po_header_stg xis
               set process_flag      = 'E',
                   error_type        = 'ERR_VAL',
                   last_updated_date = sysdate,
                   last_updated_by   = g_last_updated_by,
                   last_update_login = g_login_id
             where leg_currency_code = cur_currency_rec.leg_currency_code
               and batch_id = g_batch_id
               and run_sequence_id = g_new_run_seq_id;
            commit;
        end;
      end if;
    end loop;
    for cur_agent_rec in cur_agent loop
      ------------changes required for Agent Number -- --v1.14 (Agent Number)
      lv_agent_id        := null;
      new_lv_agent_id    := null;
      v_interface_txn_id := null; --SM MOCK3 Changes 8th Jan 2016
      if cur_agent_rec.leg_agent_emp_no is not null then
        begin
          begin
            select ppf.person_id
              into lv_agent_id
              from per_person_types_tl      ttl,
                   per_person_types         typ,
                   per_person_type_usages_f ptu,
                   per_all_people_f         ppf,
                   po_agents                pa
             where ttl.language = userenv('LANG')
               and ttl.person_type_id = typ.person_type_id
               and typ.system_person_type in ('EMP', 'CWK')
               and typ.person_type_id = ptu.person_type_id
               and sysdate between ptu.effective_start_date and
                   ptu.effective_end_date
               and sysdate between ppf.effective_start_date and
                   ppf.effective_end_date
               and ptu.person_id = ppf.person_id
               and ppf.employee_number = cur_agent_rec.leg_agent_emp_no
               and nvl(current_employee_flag, 'N') = 'Y'
               and ppf.person_id = pa.agent_id
               and trunc(sysdate) between trunc(pa.start_date_active) and
                   trunc(nvl(pa.end_date_active, sysdate));
          exception
            when no_data_found then
              begin
                for cur_plant_rec in cur_plant(cur_agent_rec.leg_po_header_id) loop
                  v_charge_account_seg1 := cur_plant_rec.leg_charge_account_seg1;
                  v_po_header_id        := cur_agent_rec.leg_po_header_id; --changed
                  select ppf.person_id
                    into new_lv_agent_id
                    from per_person_types_tl      ttl,
                         per_person_types         typ,
                         per_person_type_usages_f ptu,
                         per_all_people_f         ppf,
                         po_agents                pa,
                         fnd_lookup_values        flv
                   where ttl.language = userenv('LANG')
                     and ttl.person_type_id = typ.person_type_id
                     and typ.system_person_type in ('EMP', 'CWK')
                     and typ.person_type_id = ptu.person_type_id
                     and sysdate between ptu.effective_start_date and
                         ptu.effective_end_date
                     and sysdate between ppf.effective_start_date and
                         ppf.effective_end_date
                     and ptu.person_id = ppf.person_id
                     and ppf.employee_number = flv.description
                     and flv.lookup_type = 'ETN_LEDGER_GENERIC_BUYER'
                     and flv.meaning =
                         cur_plant_rec.leg_charge_account_seg1
                     and flv.language = userenv('LANG')
                     and flv.enabled_flag = 'Y'
                     and trunc(sysdate) between
                         trunc(nvl(flv.start_date_active, sysdate)) and
                         trunc(nvl(flv.end_date_active, sysdate))
                     and flv.enabled_flag = 'Y'
                     and nvl(current_employee_flag, 'N') = 'Y'
                     and ppf.person_id = pa.agent_id
                     and trunc(sysdate) between trunc(pa.start_date_active) and
                         trunc(nvl(pa.end_date_active, sysdate));
                end loop;
              exception
                when others then
                  --- Rohit -- loop not required as main cursor parses through all records
                  select interface_txn_id
                    into v_interface_txn_id
                    from xxpo_po_header_stg xis
                   where leg_agent_emp_no = cur_agent_rec.leg_agent_emp_no
                     and batch_id = g_batch_id
                     and leg_po_header_id = v_po_header_id
                     and run_sequence_id = g_new_run_seq_id;
                  l_err_code := 'ETN_PO_INVALID_AGENT';
                  l_err_msg  := 'Error: Agent Number not present in ETN_LEDGER_GENERIC_BUYER Lookup for corresponding plant :' ||
                                v_charge_account_seg1; --v1.14
                  log_errors(pin_interface_txn_id    => v_interface_txn_id,
                             piv_source_table        => 'xxpo_po_header_stg',
                             piv_source_column_name  => 'leg_agent_emp_no',
                             piv_source_column_value => cur_agent_rec.leg_agent_emp_no,
                             piv_source_keyname1     => null,
                             piv_source_keyvalue1    => null,
                             piv_error_type          => 'ERR_VAL',
                             piv_error_code          => l_err_code,
                             piv_error_message       => l_err_msg);
                  update xxpo_po_header_stg xis ---Rohit Update not required as the records are updated at the end of the main cursor
                     set process_flag      = 'E',
                         error_type        = 'ERR_VAL',
                         last_updated_date = sysdate,
                         last_updated_by   = g_last_updated_by,
                         last_update_login = g_login_id
                   where interface_txn_id = v_interface_txn_id --    leg_agent_emp_no = cur_agent_rec.leg_agent_emp_no
                     and batch_id = g_batch_id
                     and run_sequence_id = g_new_run_seq_id; -- Uncommented SM  MOCK4 Defect 9126 -- Records not getting errored out
                  commit; ----SM 25/07
              end;
            when others then
              --- Rohit -- loop not required as main cursor parses through all records
              select interface_txn_id
                into v_interface_txn_id
                from xxpo_po_header_stg xis
               where leg_agent_emp_no = cur_agent_rec.leg_agent_emp_no
                 and batch_id = g_batch_id
                 and run_sequence_id = g_new_run_seq_id;
              l_err_code := 'ETN_PO_INVALID_AGENT';
              l_err_msg  := 'Error: EXCEPTION error while deriving Agent Id. ';
              log_errors(pin_interface_txn_id    => v_interface_txn_id,
                         piv_source_table        => 'xxpo_po_header_stg',
                         piv_source_column_name  => 'leg_agent_emp_no',
                         piv_source_column_value => cur_agent_rec.leg_agent_emp_no,
                         piv_source_keyname1     => null,
                         piv_source_keyvalue1    => null,
                         piv_error_type          => 'ERR_VAL',
                         piv_error_code          => l_err_code,
                         piv_error_message       => l_err_msg);
              update xxpo_po_header_stg xis ---Rohit Update not required as the records are updated at the end of the main cursor
                 set process_flag      = 'E',
                     error_type        = 'ERR_VAL',
                     last_updated_date = sysdate,
                     last_updated_by   = g_last_updated_by,
                     last_update_login = g_login_id
               where interface_txn_id = v_interface_txn_id --leg_agent_emp_no = cur_agent_rec.leg_agent_emp_no
                    --   and leg_po_header_id =    cur_agent_rec.leg_po_header_id
                 and batch_id = g_batch_id
                 and run_sequence_id = g_new_run_seq_id; -- Uncommented SM  MOCK4 Defect 9126 -- Records not getting errored out
              commit;
          end;
          if lv_agent_id is not null then
            update xxpo_po_header_stg xis
               set agent_id          = lv_agent_id,
                   last_updated_date = sysdate,
                   last_updated_by   = g_last_updated_by,
                   last_update_login = g_login_id
             where leg_agent_emp_no = cur_agent_rec.leg_agent_emp_no
               and leg_po_header_id = cur_agent_rec.leg_po_header_id
               and batch_id = g_batch_id
               and run_sequence_id = g_new_run_seq_id;
            commit; --Added 16/10
          elsif new_lv_agent_id is not null then
            update xxpo_po_header_stg xis
               set agent_id          = new_lv_agent_id,
                   last_updated_date = sysdate,
                   last_updated_by   = g_last_updated_by,
                   last_update_login = g_login_id
             where leg_agent_emp_no = cur_agent_rec.leg_agent_emp_no
               and leg_po_header_id = cur_agent_rec.leg_po_header_id
               and batch_id = g_batch_id
               and run_sequence_id = g_new_run_seq_id;
            commit;
          end if;
          commit; --Added 16/10
        exception
          when others then
            for r_org_ref_err_rec in (select /*+ INDEX ( xis XXPO_PO_HEADER_STG_N11 ) */
                                       interface_txn_id
                                        from xxpo_po_header_stg xis
                                       where leg_agent_emp_no =
                                             cur_agent_rec.leg_agent_emp_no
                                         and batch_id = g_batch_id
                                         and run_sequence_id =
                                             g_new_run_seq_id) loop
              l_err_code := 'ETN_PO_INVALID_AGENT';
              l_err_msg  := 'Error: Invalid Agent employee number ';
              log_errors(pin_interface_txn_id    => r_org_ref_err_rec.interface_txn_id,
                         piv_source_table        => 'xxpo_po_header_stg',
                         piv_source_column_name  => 'leg_agent_emp_no',
                         piv_source_column_value => cur_agent_rec.leg_agent_emp_no,
                         piv_source_keyname1     => null,
                         piv_source_keyvalue1    => null,
                         piv_error_type          => 'ERR_VAL',
                         piv_error_code          => l_err_code,
                         piv_error_message       => l_err_msg);
            end loop;
            update /*+ INDEX ( xis XXPO_PO_HEADER_STG_N11 ) */ xxpo_po_header_stg xis
               set process_flag      = 'E',
                   error_type        = 'ERR_VAL',
                   last_updated_date = sysdate,
                   last_updated_by   = g_last_updated_by,
                   last_update_login = g_login_id
             where leg_agent_emp_no = cur_agent_rec.leg_agent_emp_no
               and leg_po_header_id = cur_agent_rec.leg_po_header_id
               and batch_id = g_batch_id
               and run_sequence_id = g_new_run_seq_id;
            commit;
        end;
      elsif cur_agent_rec.leg_agent_emp_no is null then
        for r_org_ref_err_rec in (select /*+ INDEX ( xis XXPO_PO_HEADER_STG_N11 ) */
                                   interface_txn_id
                                    from xxpo_po_header_stg xis
                                   where leg_agent_emp_no is null /*= cur_agent_rec.leg_agent_emp_no*/
                                     and batch_id = g_batch_id
                                     and run_sequence_id = g_new_run_seq_id) loop
          l_err_code := 'ETN_PO_MANDATORY_NOT_ENTERED';
          l_err_msg  := 'Error: Mandatory column not entered.  ';
          log_errors(pin_interface_txn_id    => r_org_ref_err_rec.interface_txn_id,
                     piv_source_table        => 'xxpo_po_header_stg',
                     piv_source_column_name  => 'leg_agent_emp_no',
                     piv_source_column_value => cur_agent_rec.leg_agent_emp_no,
                     piv_source_keyname1     => null,
                     piv_source_keyvalue1    => null,
                     piv_error_type          => 'ERR_VAL',
                     piv_error_code          => l_err_code,
                     piv_error_message       => l_err_msg);
        end loop;
        update /*+ INDEX ( xis XXPO_PO_HEADER_STG_N11 ) */ xxpo_po_header_stg
           set process_flag      = 'E',
               error_type        = 'ERR_VAL',
               last_updated_date = sysdate,
               last_updated_by   = g_last_updated_by,
               last_update_login = g_login_id
         where leg_agent_emp_no is null
           and leg_po_header_id = cur_agent_rec.leg_po_header_id
           and batch_id = g_batch_id
           and run_sequence_id = g_new_run_seq_id;
        commit;
      end if;
    end loop; ---
    for cur_ship_to_loc_rec in cur_ship_to_loc loop
      lv_ship_to_location_id := null;
      if cur_ship_to_loc_rec.leg_ship_to_location is null then
        for r_org_ref_err_rec in (select /*+ INDEX (xis XXPO_PO_HEADER_STG_N8) */
                                   interface_txn_id
                                    from xxpo_po_header_stg xis
                                   where leg_ship_to_location =
                                         cur_ship_to_loc_rec.leg_ship_to_location
                                     and batch_id = g_batch_id
                                     and run_sequence_id = g_new_run_seq_id) loop
          l_err_code := 'ETN_PO_MANDATORY_NOT_ENTERED';
          l_err_msg  := 'Error: Mandatory column not entered.  ';
          log_errors(pin_interface_txn_id    => r_org_ref_err_rec.interface_txn_id,
                     piv_source_table        => 'xxpo_po_header_stg',
                     piv_source_column_name  => 'leg_ship_to_location',
                     piv_source_column_value => cur_ship_to_loc_rec.leg_ship_to_location,
                     piv_source_keyname1     => null,
                     piv_source_keyvalue1    => null,
                     piv_error_type          => 'ERR_VAL',
                     piv_error_code          => l_err_code,
                     piv_error_message       => l_err_msg);
        end loop;
        update /*+ INDEX (xis XXPO_PO_HEADER_STG_N8) */ xxpo_po_header_stg xis
           set process_flag      = 'E',
               error_type        = 'ERR_VAL',
               last_updated_date = sysdate,
               last_updated_by   = g_last_updated_by,
               last_update_login = g_login_id
         where leg_ship_to_location is null
           and batch_id = g_batch_id
           and run_sequence_id = g_new_run_seq_id;
        commit;
      else
        ----V1.16
        begin
          select ship_to_location_id
            into lv_ship_to_location_id
            from hr_locations
           where (ship_to_site_flag = 'Y' or
                 ship_to_location_id is not null)
             and receiving_site_flag = 'Y'
             and location_code = cur_ship_to_loc_rec.leg_ship_to_location
             and sysdate <= nvl(inactive_date, sysdate);
          update /*+ INDEX (xis XXPO_PO_HEADER_STG_N8) */ xxpo_po_header_stg xis
             set ship_to_location_id = lv_ship_to_location_id,
                 last_updated_date   = sysdate,
                 last_updated_by     = g_last_updated_by,
                 last_update_login   = g_login_id
           where leg_ship_to_location =
                 cur_ship_to_loc_rec.leg_ship_to_location
             and batch_id = g_batch_id
             and run_sequence_id = g_new_run_seq_id;
          commit;
        exception
          when others then
            for r_org_ref_err_rec in (select /*+ INDEX (xis XXPO_PO_HEADER_STG_N8) */
                                       interface_txn_id
                                        from xxpo_po_header_stg xis
                                       where leg_ship_to_location =
                                             cur_ship_to_loc_rec.leg_ship_to_location
                                         and batch_id = g_batch_id
                                         and run_sequence_id =
                                             g_new_run_seq_id) loop
              l_err_code := 'ETN_PO_INVALID_SHIP_TO_LOCATION';
              l_err_msg  := 'Error: Invalid Ship to Location. ';
              log_errors(pin_interface_txn_id    => r_org_ref_err_rec.interface_txn_id,
                         piv_source_table        => 'xxpo_po_header_stg',
                         piv_source_column_name  => 'leg_ship_to_location',
                         piv_source_column_value => cur_ship_to_loc_rec.leg_ship_to_location,
                         piv_source_keyname1     => null,
                         piv_source_keyvalue1    => null,
                         piv_error_type          => 'ERR_VAL',
                         piv_error_code          => l_err_code,
                         piv_error_message       => l_err_msg);
            end loop;
            update /*+ INDEX (xis XXPO_PO_HEADER_STG_N8) */ xxpo_po_header_stg xis
               set process_flag      = 'E',
                   error_type        = 'ERR_VAL',
                   last_updated_date = sysdate,
                   last_updated_by   = g_last_updated_by,
                   last_update_login = g_login_id
             where leg_ship_to_location =
                   cur_ship_to_loc_rec.leg_ship_to_location
               and batch_id = g_batch_id
               and run_sequence_id = g_new_run_seq_id;
            commit;
        end;
      end if;
    end loop;
    for cur_bill_to_loc_rec in cur_bill_to_loc loop
      lv_bill_to_location_id := null;
      if cur_bill_to_loc_rec.leg_bill_to_location is null then
        for r_org_ref_err_rec in (select /*+ INDEX (xis XXPO_PO_HEADER_STG_N9) */
                                   interface_txn_id
                                    from xxpo_po_header_stg xis
                                   where leg_bill_to_location =
                                         cur_bill_to_loc_rec.leg_bill_to_location
                                     and batch_id = g_batch_id
                                     and run_sequence_id = g_new_run_seq_id) loop
          l_err_code := 'ETN_PO_MANDATORY_NOT_ENTERED';
          l_err_msg  := 'Error: Mandatory column not entered.  ';
          log_errors(pin_interface_txn_id    => r_org_ref_err_rec.interface_txn_id,
                     piv_source_table        => 'xxpo_po_header_stg',
                     piv_source_column_name  => 'leg_bill_to_location',
                     piv_source_column_value => cur_bill_to_loc_rec.leg_bill_to_location,
                     piv_source_keyname1     => null,
                     piv_source_keyvalue1    => null,
                     piv_error_type          => 'ERR_VAL',
                     piv_error_code          => l_err_code,
                     piv_error_message       => l_err_msg);
        end loop;
        update /*+ INDEX (xis XXPO_PO_HEADER_STG_N9) */ xxpo_po_header_stg xis
           set process_flag      = 'E',
               error_type        = 'ERR_VAL',
               last_updated_date = sysdate,
               last_updated_by   = g_last_updated_by,
               last_update_login = g_login_id
         where leg_bill_to_location is null
           and batch_id = g_batch_id
           and run_sequence_id = g_new_run_seq_id;
        commit;
      else
        begin
          select location_id
            into lv_bill_to_location_id
            from hr_locations
           where location_code = cur_bill_to_loc_rec.leg_bill_to_location
             and bill_to_site_flag = 'Y'
             and sysdate <= nvl(inactive_date, sysdate);
          update /*+ INDEX (xis XXPO_PO_HEADER_STG_N9) */ xxpo_po_header_stg xis
             set bill_to_location_id = lv_bill_to_location_id,
                 last_updated_date   = sysdate,
                 last_updated_by     = g_last_updated_by,
                 last_update_login   = g_login_id
           where leg_bill_to_location =
                 cur_bill_to_loc_rec.leg_bill_to_location
             and batch_id = g_batch_id
             and run_sequence_id = g_new_run_seq_id;
          commit;
        exception
          when others then
            for r_org_ref_err_rec in (select /*+ INDEX (xis XXPO_PO_HEADER_STG_N9) */
                                       interface_txn_id
                                        from xxpo_po_header_stg xis
                                       where leg_bill_to_location =
                                             cur_bill_to_loc_rec.leg_bill_to_location
                                         and batch_id = g_batch_id
                                         and run_sequence_id =
                                             g_new_run_seq_id) loop
              l_err_code := 'ETN_PO_INVALID_BILL_TO_LOCATION';
              l_err_msg  := 'Error: Invalid bill to Location. ';
              log_errors(pin_interface_txn_id    => r_org_ref_err_rec.interface_txn_id,
                         piv_source_table        => 'xxpo_po_header_stg',
                         piv_source_column_name  => 'leg_bill_to_location',
                         piv_source_column_value => cur_bill_to_loc_rec.leg_bill_to_location,
                         piv_source_keyname1     => null,
                         piv_source_keyvalue1    => null,
                         piv_error_type          => 'ERR_VAL',
                         piv_error_code          => l_err_code,
                         piv_error_message       => l_err_msg);
            end loop;
            update /*+ INDEX (xis XXPO_PO_HEADER_STG_N9) */ xxpo_po_header_stg xis
               set process_flag      = 'E',
                   error_type        = 'ERR_VAL',
                   last_updated_date = sysdate,
                   last_updated_by   = g_last_updated_by,
                   last_update_login = g_login_id
             where leg_bill_to_location =
                   cur_bill_to_loc_rec.leg_bill_to_location
               and batch_id = g_batch_id
               and run_sequence_id = g_new_run_seq_id;
            commit;
        end;
      end if;
    end loop;
    for cur_pay_rec in cur_pay loop
      lv_pay_name := null;
      lv_terms_id := null;
      if cur_pay_rec.leg_payment_terms is null then
        for r_org_ref_err_rec in (select /*+ INDEX (xis XXPO_PO_HEADER_STG_N10) */
                                   interface_txn_id
                                    from xxpo_po_header_stg xis
                                   where leg_payment_terms =
                                         cur_pay_rec.leg_payment_terms
                                     and batch_id = g_batch_id
                                     and run_sequence_id = g_new_run_seq_id) loop
          l_err_code := 'ETN_PO_MANDATORY_NOT_ENTERED';
          l_err_msg  := 'Error: Mandatory column not entered.  ';
          log_errors(pin_interface_txn_id    => r_org_ref_err_rec.interface_txn_id,
                     piv_source_table        => 'xxpo_po_header_stg',
                     piv_source_column_name  => 'leg_payment_terms',
                     piv_source_column_value => cur_pay_rec.leg_payment_terms,
                     piv_source_keyname1     => null,
                     piv_source_keyvalue1    => null,
                     piv_error_type          => 'ERR_VAL',
                     piv_error_code          => l_err_code,
                     piv_error_message       => l_err_msg);
        end loop;
        update /*+ INDEX (xis XXPO_PO_HEADER_STG_N10) */ xxpo_po_header_stg xis
           set process_flag      = 'E',
               error_type        = 'ERR_VAL',
               last_updated_date = sysdate,
               last_updated_by   = g_last_updated_by,
               last_update_login = g_login_id
         where leg_payment_terms is null
           and batch_id = g_batch_id
           and run_sequence_id = g_new_run_seq_id;
        commit;
      else
        begin
          select flv.description
            into lv_pay_name
            from fnd_lookup_values flv
           where flv.meaning = cur_pay_rec.leg_payment_terms
             and flv.language = userenv('LANG')
             and flv.enabled_flag = 'Y'
             and flv.lookup_type = g_payment_term_lookup
             and trunc(sysdate) between
                 trunc(nvl(flv.start_date_active, sysdate)) and
                 trunc(nvl(flv.end_date_active, sysdate));
        exception
          when others then
            for r_org_ref_err_rec in (select /*+ INDEX (xis XXPO_PO_HEADER_STG_N10) */
                                       interface_txn_id
                                        from xxpo_po_header_stg xis
                                       where leg_payment_terms =
                                             cur_pay_rec.leg_payment_terms
                                         and batch_id = g_batch_id
                                         and run_sequence_id =
                                             g_new_run_seq_id) loop
              l_err_code := 'ETN_PO_PAYMENT_TERM_ERROR';
              l_err_msg  := 'Error: Mapping not defined in the Common Payment term lookup. ';
              log_errors(pin_interface_txn_id    => r_org_ref_err_rec.interface_txn_id,
                         piv_source_table        => 'xxpo_po_header_stg',
                         piv_source_column_name  => 'leg_payment_terms',
                         piv_source_column_value => cur_pay_rec.leg_payment_terms,
                         piv_source_keyname1     => null,
                         piv_source_keyvalue1    => null,
                         piv_error_type          => 'ERR_VAL',
                         piv_error_code          => l_err_code,
                         piv_error_message       => l_err_msg);
            end loop;
            update /*+ INDEX (xis XXPO_PO_HEADER_STG_N10) */ xxpo_po_header_stg xis
               set process_flag      = 'E',
                   error_type        = 'ERR_VAL',
                   last_updated_date = sysdate,
                   last_updated_by   = g_last_updated_by,
                   last_update_login = g_login_id
             where leg_payment_terms = cur_pay_rec.leg_payment_terms
               and batch_id = g_batch_id
               and run_sequence_id = g_new_run_seq_id;
            commit;
        end;
        if lv_pay_name is not null then
          begin
            select term_id
              into lv_terms_id
              from ap_terms
             where name = lv_pay_name
               and sysdate between nvl(start_date_active, sysdate) and
                   nvl(end_date_active, sysdate)
               and enabled_flag = 'Y';
            update /*+ INDEX (xis XXPO_PO_HEADER_STG_N10) */ xxpo_po_header_stg xis
               set terms_id          = lv_terms_id,
                   payment_terms     = lv_pay_name,
                   last_updated_date = sysdate,
                   last_updated_by   = g_last_updated_by,
                   last_update_login = g_login_id
             where leg_payment_terms = cur_pay_rec.leg_payment_terms
               and batch_id = g_batch_id
               and run_sequence_id = g_new_run_seq_id;
            update /*+ INDEX (xis XXPO_PO_LINE_SHIPMENT_STG_N9) */ xxpo_po_line_shipment_stg xis
               set terms_id          = lv_terms_id,
                   payment_terms     = lv_pay_name,
                   last_updated_date = sysdate,
                   last_updated_by   = g_last_updated_by,
                   last_update_login = g_login_id
             where leg_payment_terms = cur_pay_rec.leg_payment_terms
               and batch_id = g_batch_id
               and run_sequence_id = g_new_run_seq_id;
            commit;
          exception
            when others then
              for r_org_ref_err_rec in (select /*+ INDEX (xis XXPO_PO_HEADER_STG_N10) */
                                         interface_txn_id
                                          from xxpo_po_header_stg xis
                                         where leg_payment_terms =
                                               cur_pay_rec.leg_payment_terms
                                           and batch_id = g_batch_id
                                           and run_sequence_id =
                                               g_new_run_seq_id) loop
                l_err_code := 'ETN_PO_PAYMENT_TERM_ERROR';
                l_err_msg  := 'Error: Payment term not defined in the system. ';
                log_errors(pin_interface_txn_id    => r_org_ref_err_rec.interface_txn_id,
                           piv_source_table        => 'xxpo_po_header_stg',
                           piv_source_column_name  => 'leg_payment_terms',
                           piv_source_column_value => cur_pay_rec.leg_payment_terms,
                           piv_source_keyname1     => null,
                           piv_source_keyvalue1    => null,
                           piv_error_type          => 'ERR_VAL',
                           piv_error_code          => l_err_code,
                           piv_error_message       => l_err_msg);
              end loop;
              update /*+ INDEX (xis XXPO_PO_HEADER_STG_N10) */ xxpo_po_header_stg xis
                 set process_flag      = 'E',
                     error_type        = 'ERR_VAL',
                     last_updated_date = sysdate,
                     last_updated_by   = g_last_updated_by,
                     last_update_login = g_login_id
               where leg_payment_terms = cur_pay_rec.leg_payment_terms
                 and batch_id = g_batch_id
                 and run_sequence_id = g_new_run_seq_id;
              commit;
          end;
        end if;
      end if;
    end loop;
    for cur_price_rec in cur_price loop
      lv_status := 'N';
      if cur_price_rec.leg_price_type is not null then
        begin
          select 'N'
            into lv_status
            from fnd_lookup_values flv
           where lookup_type = 'PRICE TYPE'
             and lookup_code = cur_price_rec.leg_price_type
             and flv.language = userenv('LANG')
             and trunc(sysdate) between
                 trunc(nvl(flv.start_date_active, sysdate)) and
                 trunc(nvl(flv.end_date_active, sysdate))
             and enabled_flag = 'Y';
        exception
          when others then
            for r_org_ref_err_rec in (select interface_txn_id
                                        from xxpo_po_line_shipment_stg xis
                                       where leg_price_type =
                                             cur_price_rec.leg_price_type
                                         and batch_id = g_batch_id
                                         and run_sequence_id =
                                             g_new_run_seq_id) loop
              l_err_code := 'ETN_PO_INVALID_PRICE_TYPE';
              l_err_msg  := 'Error: Invalid price type value. ';
              log_errors(pin_interface_txn_id    => r_org_ref_err_rec.interface_txn_id,
                         piv_source_table        => 'xxpo_po_line_shipment_stg',
                         piv_source_column_name  => 'leg_price_type',
                         piv_source_column_value => cur_price_rec.leg_price_type,
                         piv_source_keyname1     => null,
                         piv_source_keyvalue1    => null,
                         piv_error_type          => 'ERR_VAL',
                         piv_error_code          => l_err_code,
                         piv_error_message       => l_err_msg);
            end loop;
            update xxpo_po_line_shipment_stg
               set process_flag      = 'E',
                   error_type        = 'ERR_VAL',
                   last_updated_date = sysdate,
                   last_updated_by   = g_last_updated_by,
                   last_update_login = g_login_id
             where leg_price_type = cur_price_rec.leg_price_type
               and batch_id = g_batch_id
               and run_sequence_id = g_new_run_seq_id;
            commit;
        end;
      end if;
    end loop;
    for cur_uom_rec in cur_uom loop
      --v1.14 (UOM Code Change)
      lv_uom_description := null;
      if cur_uom_rec.leg_unit_of_measure is not null then
        begin
          begin
            select description
              into lv_uom_description
              from fnd_lookup_values flv, fnd_application fal
             where flv.lookup_type = 'XXINV_UOM_MAPPING'
               and flv.meaning = cur_uom_rec.leg_unit_of_measure
               and flv.language = userenv('LANG')
               and flv.enabled_flag = 'Y'
               and trunc(sysdate) between
                   trunc(nvl(flv.start_date_active, sysdate)) and
                   trunc(nvl(flv.end_date_active, sysdate))
               and enabled_flag = 'Y'
                  -- and fal.application_short_name = 'FND' ---v1.14
               and fal.application_id = flv.view_application_id; ---v1.14
          exception
            when others then
              for r_org_ref_err_rec in (select /*+ INDEX (xis XXPO_PO_LINE_SHIPMENT_STG_N12) */
                                         interface_txn_id
                                          from xxpo_po_line_shipment_stg xis
                                         where leg_unit_of_measure =
                                               cur_uom_rec.leg_unit_of_measure
                                           and batch_id = g_batch_id
                                           and run_sequence_id =
                                               g_new_run_seq_id) loop
                l_err_code := 'ETN_PO_INVALID_LEG_UNIT_OF_MEASURE';
                l_err_msg  := 'Error: Unit of Measure not Present in Lookup XXINV_UOM_MAPPING. ';
                log_errors(pin_interface_txn_id    => r_org_ref_err_rec.interface_txn_id,
                           piv_source_table        => 'xxpo_po_line_shipment_stg',
                           piv_source_column_name  => 'leg_unit_of_measure',
                           piv_source_column_value => cur_uom_rec.leg_unit_of_measure,
                           piv_source_keyname1     => null,
                           piv_source_keyvalue1    => null,
                           piv_error_type          => 'ERR_VAL',
                           piv_error_code          => l_err_code,
                           piv_error_message       => l_err_msg);
              end loop;
              update /*+ INDEX (xis XXPO_PO_LINE_SHIPMENT_STG_N12) */ xxpo_po_line_shipment_stg xis
                 set process_flag      = 'E',
                     error_type        = 'ERR_VAL',
                     last_updated_date = sysdate,
                     last_updated_by   = g_last_updated_by,
                     last_update_login = g_login_id
               where leg_unit_of_measure = cur_uom_rec.leg_unit_of_measure
                 and batch_id = g_batch_id
                 and run_sequence_id = g_new_run_seq_id;
          end;
          if lv_uom_description is not null then
            /*  UPDATE xxpo_po_line_shipment_stg
               SET leg_unit_of_measure = lv_uom_description
             WHERE leg_unit_of_measure = cur_uom_rec.leg_unit_of_measure
               AND batch_id = g_batch_id
               AND run_sequence_id = g_new_run_seq_id;
            */
            begin
              select uom_code
                into lv_dep_uom_code
                from mtl_units_of_measure
               where unit_of_measure = lv_uom_description
                 and sysdate >= nvl(disable_date, sysdate);
              update /*+ INDEX (xis XXPO_PO_LINE_SHIPMENT_STG_N12) */ xxpo_po_line_shipment_stg xis
                 set uom_code          = lv_dep_uom_code,
                     last_updated_date = sysdate,
                     last_updated_by   = g_last_updated_by,
                     last_update_login = g_login_id
               where leg_unit_of_measure = cur_uom_rec.leg_unit_of_measure
                 and batch_id = g_batch_id
                 and run_sequence_id = g_new_run_seq_id;
            exception
              when others then
                for r_org_ref_err_rec in (select /*+ INDEX (xis XXPO_PO_LINE_SHIPMENT_STG_N12) */
                                           interface_txn_id
                                            from xxpo_po_line_shipment_stg xis
                                           where leg_unit_of_measure =
                                                 cur_uom_rec.leg_unit_of_measure
                                             and batch_id = g_batch_id
                                             and run_sequence_id =
                                                 g_new_run_seq_id) loop
                  l_err_code := 'ETN_PO_INVALID_UOM';
                  l_err_msg  := 'Error: Invalid Unit of Measure. ';
                  log_errors(pin_interface_txn_id    => r_org_ref_err_rec.interface_txn_id,
                             piv_source_table        => 'xxpo_po_line_shipment_stg',
                             piv_source_column_name  => 'leg_unit_of_measure',
                             piv_source_column_value => cur_uom_rec.leg_unit_of_measure,
                             piv_source_keyname1     => null,
                             piv_source_keyvalue1    => null,
                             piv_error_type          => 'ERR_VAL',
                             piv_error_code          => l_err_code,
                             piv_error_message       => l_err_msg);
                end loop;
                update /*+ INDEX (xis XXPO_PO_LINE_SHIPMENT_STG_N12) */ xxpo_po_line_shipment_stg
                   set process_flag      = 'E',
                       error_type        = 'ERR_VAL',
                       last_updated_date = sysdate,
                       last_updated_by   = g_last_updated_by,
                       last_update_login = g_login_id
                 where leg_unit_of_measure =
                       cur_uom_rec.leg_unit_of_measure
                   and batch_id = g_batch_id
                   and run_sequence_id = g_new_run_seq_id;
            end;
          end if;
        exception
          when others then
            for r_org_ref_err_rec in (select /*+ INDEX (xis XXPO_PO_LINE_SHIPMENT_STG_N12) */
                                       interface_txn_id
                                        from xxpo_po_line_shipment_stg xis
                                       where leg_unit_of_measure =
                                             cur_uom_rec.leg_unit_of_measure
                                         and batch_id = g_batch_id
                                         and run_sequence_id =
                                             g_new_run_seq_id) loop
              l_err_code := 'ETN_PO_INVALID_UOM';
              l_err_msg  := 'Error: Invalid Unit of Measure. ';
              log_errors(pin_interface_txn_id    => r_org_ref_err_rec.interface_txn_id,
                         piv_source_table        => 'xxpo_po_line_shipment_stg',
                         piv_source_column_name  => 'leg_unit_of_measure',
                         piv_source_column_value => cur_uom_rec.leg_unit_of_measure,
                         piv_source_keyname1     => null,
                         piv_source_keyvalue1    => null,
                         piv_error_type          => 'ERR_VAL',
                         piv_error_code          => l_err_code,
                         piv_error_message       => l_err_msg);
            end loop;
            update /*+ INDEX (xis XXPO_PO_LINE_SHIPMENT_STG_N12) */ xxpo_po_line_shipment_stg
               set process_flag      = 'E',
                   error_type        = 'ERR_VAL',
                   last_updated_date = sysdate,
                   last_updated_by   = g_last_updated_by,
                   last_update_login = g_login_id
             where leg_unit_of_measure = cur_uom_rec.leg_unit_of_measure
               and batch_id = g_batch_id
               and run_sequence_id = g_new_run_seq_id;
        end;
      end if;
    end loop;
    for cur_receive_rec in cur_receive loop
      lv_dep_receiving_routing_id := null;
      if cur_receive_rec.leg_receiving_routing is not null then
        begin
          select to_number(lookup_code)
            into lv_dep_receiving_routing_id
            from fnd_lookup_values flv
           where lookup_type = 'RCV_ROUTING_HEADERS'
             and meaning = cur_receive_rec.leg_receiving_routing
             and trunc(sysdate) between
                 trunc(nvl(flv.start_date_active, sysdate)) and
                 trunc(nvl(flv.end_date_active, sysdate))
             and enabled_flag = 'Y'
             and rownum = 1;
          update xxpo_po_line_shipment_stg
             set receiving_routing_id = lv_dep_receiving_routing_id,
                 last_updated_date    = sysdate,
                 last_updated_by      = g_last_updated_by,
                 last_update_login    = g_login_id
           where leg_receiving_routing =
                 cur_receive_rec.leg_receiving_routing
             and batch_id = g_batch_id
             and run_sequence_id = g_new_run_seq_id;
          commit;
        exception
          when others then
            for r_org_ref_err_rec in (select interface_txn_id
                                        from xxpo_po_line_shipment_stg xis
                                       where leg_receiving_routing =
                                             cur_receive_rec.leg_receiving_routing
                                         and batch_id = g_batch_id
                                         and run_sequence_id =
                                             g_new_run_seq_id) loop
              l_err_code := 'ETN_PO_INVALID_RECEIVING_ROUTING';
              l_err_msg  := 'Error: Invalid receiving routing value. ';
              log_errors(pin_interface_txn_id    => r_org_ref_err_rec.interface_txn_id,
                         piv_source_table        => 'xxpo_po_line_shipment_stg',
                         piv_source_column_name  => 'leg_receiving_routing',
                         piv_source_column_value => cur_receive_rec.leg_receiving_routing,
                         piv_source_keyname1     => null,
                         piv_source_keyvalue1    => null,
                         piv_error_type          => 'ERR_VAL',
                         piv_error_code          => l_err_code,
                         piv_error_message       => l_err_msg);
            end loop;
            update xxpo_po_line_shipment_stg
               set process_flag      = 'E',
                   error_type        = 'ERR_VAL',
                   last_updated_date = sysdate,
                   last_updated_by   = g_last_updated_by,
                   last_update_login = g_login_id
             where leg_receiving_routing =
                   cur_receive_rec.leg_receiving_routing
               and batch_id = g_batch_id
               and run_sequence_id = g_new_run_seq_id;
            commit;
        end;
      end if;
    end loop;
    for cur_line_type_rec in cur_line_type loop
      lv_dep_line_type_id := null;
      if cur_line_type_rec.leg_line_type is not null then
        begin
          select line_type_id
            into lv_dep_line_type_id
            from po_line_types
           where line_type = cur_line_type_rec.leg_line_type;
          update /*+ INDEX ( xis XXPO_PO_LINE_SHIPMENT_STG_N7) */ xxpo_po_line_shipment_stg xis
             set line_id           = lv_dep_line_type_id,
                 last_updated_date = sysdate,
                 last_updated_by   = g_last_updated_by,
                 last_update_login = g_login_id
           where leg_line_type = cur_line_type_rec.leg_line_type
             and batch_id = g_batch_id
             and run_sequence_id = g_new_run_seq_id;
          commit;
        exception
          when others then
            for r_org_ref_err_rec in (select /*+ INDEX ( xis XXPO_PO_LINE_SHIPMENT_STG_N7) */
                                       interface_txn_id
                                        from xxpo_po_line_shipment_stg xis
                                       where leg_line_type =
                                             cur_line_type_rec.leg_line_type
                                         and batch_id = g_batch_id
                                         and run_sequence_id =
                                             g_new_run_seq_id) loop
              l_err_code := 'ETN_PO_INVALID_LINE_TYPE';
              l_err_msg  := 'Error: Invalid Line Type. ';
              log_errors(pin_interface_txn_id    => r_org_ref_err_rec.interface_txn_id,
                         piv_source_table        => 'xxpo_po_line_shipment_stg',
                         piv_source_column_name  => 'leg_line_type',
                         piv_source_column_value => cur_line_type_rec.leg_line_type,
                         piv_source_keyname1     => null,
                         piv_source_keyvalue1    => null,
                         piv_error_type          => 'ERR_VAL',
                         piv_error_code          => l_err_code,
                         piv_error_message       => l_err_msg);
            end loop;
            update /*+ INDEX ( xis XXPO_PO_LINE_SHIPMENT_STG_N7) */ xxpo_po_line_shipment_stg xis
               set process_flag      = 'E',
                   error_type        = 'ERR_VAL',
                   last_updated_date = sysdate,
                   last_updated_by   = g_last_updated_by,
                   last_update_login = g_login_id
             where leg_line_type = cur_line_type_rec.leg_line_type
               and batch_id = g_batch_id
               and run_sequence_id = g_new_run_seq_id;
            commit;
        end;
      end if;
    end loop;
    for cur_un_number_rec in cur_un_number loop
      lv_dep_un_number_id := null;
      if cur_un_number_rec.leg_un_number is not null then
        begin
          select un_number_id
            into lv_dep_un_number_id
            from po_un_numbers
           where un_number = cur_un_number_rec.leg_un_number
             and sysdate <= nvl(inactive_date, sysdate);
          update xxpo_po_line_shipment_stg
             set un_number_id      = lv_dep_un_number_id,
                 last_updated_date = sysdate,
                 last_updated_by   = g_last_updated_by,
                 last_update_login = g_login_id
           where leg_un_number = cur_un_number_rec.leg_un_number
             and batch_id = g_batch_id
             and run_sequence_id = g_new_run_seq_id;
          commit;
        exception
          when others then
            for r_org_ref_err_rec in (select interface_txn_id
                                        from xxpo_po_line_shipment_stg xis
                                       where leg_un_number =
                                             cur_un_number_rec.leg_un_number
                                         and batch_id = g_batch_id
                                         and run_sequence_id =
                                             g_new_run_seq_id) loop
              l_err_code := 'ETN_PO_INVALID_UN_NUMBER';
              l_err_msg  := 'Error: Invalid UN Number Value. ';
              log_errors(pin_interface_txn_id    => r_org_ref_err_rec.interface_txn_id,
                         piv_source_table        => 'xxpo_po_line_shipment_stg',
                         piv_source_column_name  => 'leg_un_number',
                         piv_source_column_value => cur_un_number_rec.leg_un_number,
                         piv_source_keyname1     => null,
                         piv_source_keyvalue1    => null,
                         piv_error_type          => 'ERR_VAL',
                         piv_error_code          => l_err_code,
                         piv_error_message       => l_err_msg);
            end loop;
            update xxpo_po_line_shipment_stg
               set process_flag      = 'E',
                   error_type        = 'ERR_VAL',
                   last_updated_date = sysdate,
                   last_updated_by   = g_last_updated_by,
                   last_update_login = g_login_id
             where leg_un_number = cur_un_number_rec.leg_un_number
               and batch_id = g_batch_id
               and run_sequence_id = g_new_run_seq_id;
            commit;
        end;
      end if;
    end loop;
    for cur_hazard_rec in cur_hazard loop
      lv_dep_hazard_class_id := null;
      if cur_hazard_rec.leg_hazard_class is not null then
        begin
          select hazard_class_id
            into lv_dep_hazard_class_id
            from po_hazard_classes
           where hazard_class = cur_hazard_rec.leg_hazard_class
             and sysdate <= nvl(inactive_date, sysdate);
          update xxpo_po_line_shipment_stg
             set hazard_class_id   = lv_dep_hazard_class_id,
                 last_updated_date = sysdate,
                 last_updated_by   = g_last_updated_by,
                 last_update_login = g_login_id
           where leg_hazard_class = cur_hazard_rec.leg_hazard_class
             and batch_id = g_batch_id
             and run_sequence_id = g_new_run_seq_id;
          commit;
        exception
          when others then
            for r_org_ref_err_rec in (select interface_txn_id
                                        from xxpo_po_line_shipment_stg xis
                                       where leg_hazard_class =
                                             cur_hazard_rec.leg_hazard_class
                                         and batch_id = g_batch_id
                                         and run_sequence_id =
                                             g_new_run_seq_id) loop
              l_err_code := 'ETN_PO_INVALID_HAZARD_CLASS';
              l_err_msg  := 'Error: Invalid hazard class value. ';
              log_errors(pin_interface_txn_id    => r_org_ref_err_rec.interface_txn_id,
                         piv_source_table        => 'xxpo_po_line_shipment_stg',
                         piv_source_column_name  => 'leg_hazard_class',
                         piv_source_column_value => cur_hazard_rec.leg_hazard_class,
                         piv_source_keyname1     => null,
                         piv_source_keyvalue1    => null,
                         piv_error_type          => 'ERR_VAL',
                         piv_error_code          => l_err_code,
                         piv_error_message       => l_err_msg);
            end loop;
            update xxpo_po_line_shipment_stg
               set process_flag      = 'E',
                   error_type        = 'ERR_VAL',
                   last_updated_date = sysdate,
                   last_updated_by   = g_last_updated_by,
                   last_update_login = g_login_id
             where leg_hazard_class = cur_hazard_rec.leg_hazard_class
               and batch_id = g_batch_id
               and run_sequence_id = g_new_run_seq_id;
            commit;
        end;
      end if;
    end loop;
    for cur_ship_to_org_rec in cur_ship_to_org loop
      --  V1.16 (Changes Start)   (Inventory Organization from Cross Reference API)
      lv_dep_ship_to_organization_id := null;
      lv_ship_to_org_id              := null;
      l_err_code                     := null;
      lv_err_msg1                    := null;
      lv_out_val1                    := null;
      lv_out_val2                    := null;
      lv_out_val3                    := null;
      p_out_org_name                 := null;
      p_in_ship_to_org_name          := null;
      if cur_ship_to_org_rec.leg_ship_to_organization_name is not null then
        --1
        begin
          for cur_plant_rec in cur_plant(cur_ship_to_org_rec.leg_po_header_id) --resusing cur_plant
           loop
            begin
              p_in_ship_to_org_name := cur_ship_to_org_rec.leg_ship_to_organization_name;
              p_in_plant_num        := cur_plant_rec.leg_charge_account_seg1;
              p_out_org_name        := null;
              l_column_name         := null;
              l_column_value        := null;
              lv_errm               := null;
              --1.3 Derive R12 SHIP_TO_ORGANIZATION_NAME using Cross Reference Utility
              xxetn_cross_ref_pkg.get_value(piv_eaton_ledger   => p_in_plant_num, -- Plant Number passed from cursor cur_plant
                                            piv_type           => 'INVENTORY ORG MAPPING',
                                            piv_direction      => 'E', --value Required
                                            piv_application    => 'PO', --value Required
                                            piv_input_value1   => p_in_ship_to_org_name,
                                            piv_input_value2   => null, --value Required
                                            piv_input_value3   => null, --value Required
                                            pid_effective_date => sysdate, --value Required  -- PASS DEFAULT
                                            pov_output_value1  => lv_out_val1,
                                            pov_output_value2  => lv_out_val2,
                                            pov_output_value3  => lv_out_val3,
                                            pov_err_msg        => lv_err_msg1);
              if lv_err_msg1 is null --1.1
               then
                p_out_org_name := lv_out_val1;
              else
                --v.xx
                p_out_org_name := null;
                lv_err_msg1    := null;
                --1.3 Derive R12 SHIP_TO_ORGANIZATION_NAME using Cross Reference Utility
                xxetn_cross_ref_pkg.get_value(piv_eaton_ledger   => null, -- Plant Number passed from cursor cur_plant
                                              piv_type           => 'INVENTORY ORG MAPPING',
                                              piv_direction      => 'E', --value Required
                                              piv_application    => 'PO', --value Required
                                              piv_input_value1   => p_in_ship_to_org_name,
                                              piv_input_value2   => null, --value Required
                                              piv_input_value3   => null, --value Required
                                              pid_effective_date => sysdate, --value Required  -- PASS DEFAULT
                                              pov_output_value1  => lv_out_val1,
                                              pov_output_value2  => lv_out_val2,
                                              pov_output_value3  => lv_out_val3,
                                              pov_err_msg        => lv_err_msg1);
                if lv_err_msg1 is null then
                  p_out_org_name := lv_out_val1;
                else
                  p_out_org_name := p_in_ship_to_org_name;
                end if;
              end if;
            exception
              when others then
                l_err_code     := 'ETN_PO_SHIP_TO_ORG_CROSSREF';
                l_err_msg      := 'Error Fetching Ship to Org From Cross Ref';
                lv_errm        := 'Y';
                l_column_name  := 'leg_ship_to_organization_name';
                l_column_value := p_in_ship_to_org_name;
            end;
            if p_out_org_name is not null then
              begin
                select organization_id
                  into lv_dep_ship_to_organization_id
                  from org_organization_definitions
                 where organization_name = p_out_org_name --changed
                      -- aditya/SDP 13/62016
                   and operating_unit = cur_ship_to_org_rec.org_id --Added for IO Check --v1.14 -- Not Needed 14/8/2015
                      -- aditya/SDP 13/62016 ends
                   and sysdate between
                       nvl(user_definition_enable_date, sysdate) and
                       nvl(disable_date, sysdate)
                   and inventory_enabled_flag = 'Y';
                update /*+ INDEX (xis XXPO_PO_LINE_SHIPMENT_STG_N5) */ xxpo_po_line_shipment_stg xis
                   set ship_to_organization_id = lv_dep_ship_to_organization_id,
                       last_updated_date       = sysdate,
                       last_updated_by         = g_last_updated_by,
                       last_update_login       = g_login_id
                 where leg_ship_to_organization_name =
                       p_in_ship_to_org_name
                   and batch_id = g_batch_id
                   and leg_po_header_id =
                       cur_ship_to_org_rec.leg_po_header_id --changed SM  26th NOV
                   and leg_source_system = cur_plant_rec.leg_source_system --Added 13/10           Update based on PO Header Id
                   and run_sequence_id = g_new_run_seq_id
                      -- aditya/SDP 13/62016
                   and org_id = cur_ship_to_org_rec.org_id; --Added for IO Check --v1.14 -- Not Needed 14/8/2015
                -- aditya/SDP 13/62016 ends
              exception
                when no_data_found then
                  l_err_code     := 'ETN_PO_INVALID_SHIP_ORG_NAME';
                  l_err_msg      := 'Error: Inventory Org Not Present in org_organization table Plant :' ||
                                    p_in_plant_num;
                  lv_errm        := 'Y';
                  l_column_name  := 'ship_to_organization_id';
                  l_column_value := p_out_org_name;
              end;
            end if;
            if lv_errm = 'Y' then
              update /*+ INDEX (xis XXPO_PO_LINE_SHIPMENT_STG_N8) */ xxpo_po_line_shipment_stg xis
                 set process_flag      = 'E',
                     error_type        = 'ERR_VAL',
                     last_updated_date = sysdate,
                     last_updated_by   = g_last_updated_by,
                     last_update_login = g_login_id
               where leg_ship_to_organization_name = p_in_ship_to_org_name
                 and batch_id = g_batch_id
                 and org_id = cur_ship_to_org_rec.org_id -- aditya/SDP 13/62016
                 and leg_po_header_id =
                     cur_ship_to_org_rec.leg_po_header_id --Added 13/10           Update based on PO Header Id
                 and run_sequence_id = g_new_run_seq_id;
              commit;
              --v.xx ends
              for r_org_ref_err_rec in (select /*+ INDEX (xis XXPO_PO_LINE_SHIPMENT_STG_N8) */
                                         interface_txn_id
                                          from xxpo_po_line_shipment_stg xis
                                         where leg_ship_to_organization_name =
                                               p_in_ship_to_org_name
                                           and batch_id = g_batch_id
                                           and leg_po_header_id =
                                               cur_ship_to_org_rec.leg_po_header_id --today
                                           and run_sequence_id =
                                               g_new_run_seq_id
                                           and org_id =
                                               cur_ship_to_org_rec.org_id -- aditya/SDP 13/62016
                                        ) loop
                --l_err_code := 'ETN_PO_SHIP_TO_ORG_CROSSREF';
                -- l_err_msg  := 'Error: Invalid Inventory Org Name. Error received while deriving new Org name from Cross Reference API.';
                log_errors(pin_interface_txn_id    => r_org_ref_err_rec.interface_txn_id,
                           piv_source_table        => 'xxpo_po_line_shipment_stg',
                           piv_source_column_name  => l_column_name,
                           piv_source_column_value => l_column_value,
                           piv_source_keyname1     => null,
                           piv_source_keyvalue1    => null,
                           piv_error_type          => 'ERR_VAL',
                           piv_error_code          => l_err_code,
                           piv_error_message       => l_err_msg);
              end loop;
            end if;
          --end if; V.xx
          end loop;
        end;
      else
        --1:1
        for r_org_ref_err_rec in (select /*+ INDEX (xis XXPO_PO_LINE_SHIPMENT_STG_N8) */
                                   interface_txn_id
                                    from xxpo_po_line_shipment_stg xis
                                   where leg_ship_to_organization_name is null /*= cur_ship_to_org_rec.leg_ship_to_organization_name*/
                                     and leg_po_header_id =
                                         cur_ship_to_org_rec.leg_po_header_id --today
                                     and batch_id = g_batch_id
                                     and run_sequence_id = g_new_run_seq_id) loop
          l_err_code := 'ETN_PO_MANDATORY_NOT_ENTERED';
          l_err_msg  := 'Error: Mandatory column not entered.  ';
          log_errors(pin_interface_txn_id    => r_org_ref_err_rec.interface_txn_id,
                     piv_source_table        => 'xxpo_po_line_shipment_stg',
                     piv_source_column_name  => 'leg_ship_to_organization_name',
                     piv_source_column_value => cur_ship_to_org_rec.leg_ship_to_organization_name,
                     piv_source_keyname1     => null,
                     piv_source_keyvalue1    => null,
                     piv_error_type          => 'ERR_VAL',
                     piv_error_code          => l_err_code,
                     piv_error_message       => l_err_msg);
        end loop;
        update /*+ INDEX (xis XXPO_PO_LINE_SHIPMENT_STG_N8) */ xxpo_po_line_shipment_stg xis
           set process_flag      = 'E',
               error_type        = 'ERR_VAL',
               last_updated_date = sysdate,
               last_updated_by   = g_last_updated_by,
               last_update_login = g_login_id
         where leg_ship_to_organization_name is null
           and batch_id = g_batch_id
           and leg_po_header_id = cur_ship_to_org_rec.leg_po_header_id --Added 13/10           Update based on PO Header Id
           and run_sequence_id = g_new_run_seq_id;
        commit;
      end if; --/1
    end loop;
    --  V1.16 (Changes End)   (Inventory Organization from Cross Reference API)
    for cur_item_rec in cur_item loop
      lv_dep_item_id := null;
      if (cur_item_rec.leg_item is not null) and
         (cur_item_rec.ship_to_organization_id is not null) then
        begin
          select inventory_item_id
            into lv_dep_item_id
            from mtl_system_items_b
           where organization_id = cur_item_rec.ship_to_organization_id
             and segment1 = cur_item_rec.leg_item
             and sysdate between nvl(start_date_active, sysdate) and
                 nvl(end_date_active, sysdate)
             and enabled_flag = 'Y';
          update /*+ INDEX (xis XXPO_PO_LINE_SHIPMENT_STG_N10) */ xxpo_po_line_shipment_stg xis
             set item_id           = lv_dep_item_id,
                 last_updated_date = sysdate,
                 last_updated_by   = g_last_updated_by,
                 last_update_login = g_login_id
           where leg_item = cur_item_rec.leg_item
             and ship_to_organization_id =
                 cur_item_rec.ship_to_organization_id
             and batch_id = g_batch_id
             and run_sequence_id = g_new_run_seq_id;
          commit;
        exception
          when others then
            for r_org_ref_err_rec in (select /*+ INDEX (xis XXPO_PO_LINE_SHIPMENT_STG_N10) */
                                       interface_txn_id
                                        from xxpo_po_line_shipment_stg xis
                                       where leg_item =
                                             cur_item_rec.leg_item
                                         and ship_to_organization_id =
                                             cur_item_rec.ship_to_organization_id
                                         and batch_id = g_batch_id
                                         and run_sequence_id =
                                             g_new_run_seq_id) loop
              l_err_code := 'ETN_PO_INVALID_ITEM_NUMBER';
              l_err_msg  := 'Error: Invalid Item Number Value. ';
              log_errors(pin_interface_txn_id    => r_org_ref_err_rec.interface_txn_id,
                         piv_source_table        => 'xxpo_po_line_shipment_stg',
                         piv_source_column_name  => 'leg_item',
                         piv_source_column_value => cur_item_rec.leg_item,
                         piv_source_keyname1     => null,
                         piv_source_keyvalue1    => null,
                         piv_error_type          => 'ERR_VAL',
                         piv_error_code          => l_err_code,
                         piv_error_message       => l_err_msg);
            end loop;
            update /*+ INDEX (xis XXPO_PO_LINE_SHIPMENT_STG_N10) */ xxpo_po_line_shipment_stg xis
               set process_flag      = 'E',
                   error_type        = 'ERR_VAL',
                   last_updated_date = sysdate,
                   last_updated_by   = g_last_updated_by,
                   last_update_login = g_login_id
             where leg_item = cur_item_rec.leg_item
               and ship_to_organization_id =
                   cur_item_rec.ship_to_organization_id
               and batch_id = g_batch_id
               and run_sequence_id = g_new_run_seq_id;
            commit;
        end;
      end if;
    end loop;
    begin
      select structure_id
        into l_cat_set_struc_id
        from mtl_default_category_sets_fk_v a, mtl_category_sets b
       where a.category_set_id = b.category_set_id
         and functional_area_desc = 'Purchasing';
    exception
      when others then
        l_cat_set_struc_id := null;
    end;
    for cur_category_rec in cur_category loop
      lv_dep_category_id := null;
      if (cur_category_rec.leg_category_segment1 is not null) and
         (cur_category_rec.leg_item is null) then
        begin
          select category_id
            into lv_dep_category_id
            from apps.mtl_categories_b_kfv
           where (segment1) = cur_category_rec.leg_category_segment1
             and structure_id = l_cat_set_struc_id;
          update xxpo_po_line_shipment_stg
             set category_id       = lv_dep_category_id,
                 last_updated_date = sysdate,
                 last_updated_by   = g_last_updated_by,
                 last_update_login = g_login_id
           where leg_category_segment1 =
                 cur_category_rec.leg_category_segment1
             and batch_id = g_batch_id
             and run_sequence_id = g_new_run_seq_id;
          commit;
        exception
          when others then
            for r_org_ref_err_rec in (select interface_txn_id
                                        from xxpo_po_line_shipment_stg xis
                                       where leg_category_segment1 =
                                             cur_category_rec.leg_category_segment1
                                         and batch_id = g_batch_id
                                         and run_sequence_id =
                                             g_new_run_seq_id) loop
              l_err_code := 'ETN_PO_INVALID_ITEM_CATEGORY';
              l_err_msg  := 'Error: Invalid Item Category Value. ';
              log_errors(pin_interface_txn_id    => r_org_ref_err_rec.interface_txn_id,
                         piv_source_table        => 'xxpo_po_line_shipment_stg',
                         piv_source_column_name  => 'leg_category_segment1',
                         piv_source_column_value => cur_category_rec.leg_category_segment1,
                         piv_source_keyname1     => null,
                         piv_source_keyvalue1    => null,
                         piv_error_type          => 'ERR_VAL',
                         piv_error_code          => l_err_code,
                         piv_error_message       => l_err_msg);
            end loop;
            update xxpo_po_line_shipment_stg
               set process_flag      = 'E',
                   error_type        = 'ERR_VAL',
                   last_updated_date = sysdate,
                   last_updated_by   = g_last_updated_by,
                   last_update_login = g_login_id
             where leg_category_segment1 =
                   cur_category_rec.leg_category_segment1
               and batch_id = g_batch_id
               and run_sequence_id = g_new_run_seq_id;
            commit;
        end;
      end if;
    end loop;
    for cur_ship_to_loc_line_rec in cur_ship_to_loc_line loop
      lv_dep_ship_to_location_id := null;
      if cur_ship_to_loc_line_rec.leg_ship_to_location is null then
        for r_org_ref_err_rec in (select interface_txn_id
                                    from xxpo_po_line_shipment_stg xis
                                   where leg_ship_to_location =
                                         cur_ship_to_loc_line_rec.leg_ship_to_location
                                     and batch_id = g_batch_id
                                     and run_sequence_id = g_new_run_seq_id) loop
          l_err_code := 'ETN_PO_MANDATORY_NOT_ENTERED';
          l_err_msg  := 'Error: Mandatory column not entered.  ';
          log_errors(pin_interface_txn_id    => r_org_ref_err_rec.interface_txn_id,
                     piv_source_table        => 'xxpo_po_line_shipment_stg',
                     piv_source_column_name  => 'leg_ship_to_location',
                     piv_source_column_value => cur_ship_to_loc_line_rec.leg_ship_to_location,
                     piv_source_keyname1     => null,
                     piv_source_keyvalue1    => null,
                     piv_error_type          => 'ERR_VAL',
                     piv_error_code          => l_err_code,
                     piv_error_message       => l_err_msg);
        end loop;
        update xxpo_po_line_shipment_stg
           set process_flag      = 'E',
               error_type        = 'ERR_VAL',
               last_updated_date = sysdate,
               last_updated_by   = g_last_updated_by,
               last_update_login = g_login_id
         where leg_ship_to_location is null
           and batch_id = g_batch_id
           and run_sequence_id = g_new_run_seq_id;
        commit;
      else
        begin
          select ship_to_location_id
            into lv_dep_ship_to_location_id
            from hr_locations
           where (ship_to_site_flag = 'Y' or
                 ship_to_location_id is not null)
             and receiving_site_flag = 'Y'
             and location_code =
                 cur_ship_to_loc_line_rec.leg_ship_to_location
             and sysdate <= nvl(inactive_date, sysdate);
          update xxpo_po_line_shipment_stg
             set ship_to_location_id = lv_dep_ship_to_location_id,
                 last_updated_date   = sysdate,
                 last_updated_by     = g_last_updated_by,
                 last_update_login   = g_login_id
           where leg_ship_to_location =
                 cur_ship_to_loc_line_rec.leg_ship_to_location
             and batch_id = g_batch_id
             and run_sequence_id = g_new_run_seq_id;
          commit;
        exception
          when others then
            for r_org_ref_err_rec in (select interface_txn_id
                                        from xxpo_po_line_shipment_stg xis
                                       where leg_ship_to_location =
                                             cur_ship_to_loc_line_rec.leg_ship_to_location
                                         and batch_id = g_batch_id
                                         and run_sequence_id =
                                             g_new_run_seq_id) loop
              l_err_code := 'ETN_PO_INVALID_SHIP_TO_LOCATION';
              l_err_msg  := 'Error: Invalid Ship to Location. ';
              log_errors(pin_interface_txn_id    => r_org_ref_err_rec.interface_txn_id,
                         piv_source_table        => 'xxpo_po_line_shipment_stg',
                         piv_source_column_name  => 'leg_ship_to_location',
                         piv_source_column_value => cur_ship_to_loc_line_rec.leg_ship_to_location,
                         piv_source_keyname1     => null,
                         piv_source_keyvalue1    => null,
                         piv_error_type          => 'ERR_VAL',
                         piv_error_code          => l_err_code,
                         piv_error_message       => l_err_msg);
            end loop;
            update xxpo_po_line_shipment_stg
               set process_flag      = 'E',
                   error_type        = 'ERR_VAL',
                   last_updated_date = sysdate,
                   last_updated_by   = g_last_updated_by,
                   last_update_login = g_login_id
             where leg_ship_to_location =
                   cur_ship_to_loc_line_rec.leg_ship_to_location
               and batch_id = g_batch_id
               and run_sequence_id = g_new_run_seq_id;
            commit;
        end;
      end if;
    end loop;
    --  V1.17 (Changes Start)   (Destination Organization from Cross Reference API for Destination Org)
    for cur_dest_org_rec in cur_dest_org loop
      lv_dest_org_id := null;
      lv_dep_ship_to_organization_id := null;
      lv_ship_to_org_id              := null;
      l_err_code                     := null;
      lv_err_msg1                    := null;
      lv_out_val1                    := null;
      lv_out_val2                    := null;
      lv_out_val3                    := null;
      p_out_org_name                 := null;
      p_in_ship_to_org_name          := null;
      if cur_dest_org_rec.leg_destination_organization is not null then
        begin
          for cur_plant_rec in cur_plant(cur_dest_org_rec.leg_po_header_id) --resusing cur_plant
           loop
            begin
              p_in_ship_to_org_name := cur_dest_org_rec.leg_destination_organization;
              p_in_plant_num        := cur_plant_rec.leg_charge_account_seg1;
              p_out_org_name        := null;
              l_column_name         := null;
              l_column_value        := null;
              lv_errm               := null;
              --1.3 Derive R12 SHIP_TO_ORGANIZATION_NAME using Cross Reference Utility
              xxetn_cross_ref_pkg.get_value(piv_eaton_ledger   => p_in_plant_num, -- Plant Number passed from cursor cur_plant
                                            piv_type           => 'INVENTORY ORG MAPPING',
                                            piv_direction      => 'E', --value Required
                                            piv_application    => 'PO', --value Required
                                            piv_input_value1   => p_in_ship_to_org_name,
                                            piv_input_value2   => null, --value Required
                                            piv_input_value3   => null, --value Required
                                            pid_effective_date => sysdate, --value Required-- PASS DEFAULT
                                            pov_output_value1  => lv_out_val1,
                                            pov_output_value2  => lv_out_val2,
                                            pov_output_value3  => lv_out_val3,
                                            pov_err_msg        => lv_err_msg1);
              if lv_err_msg1 is null then
                p_out_org_name := lv_out_val1;
              else
                p_out_org_name := null;
                lv_err_msg1    := null;
                xxetn_cross_ref_pkg.get_value(piv_eaton_ledger   => null, -- Plant Number passed from cursor cur_plant
                                              piv_type           => 'INVENTORY ORG MAPPING',
                                              piv_direction      => 'E', --value Required
                                              piv_application    => 'PO', --value Required
                                              piv_input_value1   => p_in_ship_to_org_name,
                                              piv_input_value2   => null, --value Required
                                              piv_input_value3   => null, --value Required
                                              pid_effective_date => sysdate, --value Required-- PASS DEFAULT
                                              pov_output_value1  => lv_out_val1,
                                              pov_output_value2  => lv_out_val2,
                                              pov_output_value3  => lv_out_val3,
                                              pov_err_msg        => lv_err_msg1);
                if lv_err_msg1 is null then
                  p_out_org_name := lv_out_val1;
                else
                  p_out_org_name := p_in_ship_to_org_name;
                end if;
              end if;
            exception
              when others then
                l_err_code     := 'ETN_PO_DESTINATION_ORG_CROSSREF';
                l_err_msg      := 'Error Fetching Destination Org From Cross Ref';
                lv_errm        := 'Y';
                l_column_name  := 'leg_destination_organization';
                l_column_value := p_in_ship_to_org_name;
            end;
            if p_out_org_name is not null then
              begin
                select organization_id
                  into lv_dest_org_id
                  from org_organization_definitions
                 where organization_name = p_out_org_name
                      -- aditya/SDP 13/62016
                   and operating_unit = cur_dest_org_rec.org_id ---Added for OU check ----v1.14--Not Needed 14/8/2015
                      -- aditya/SDP 13/62016 ends
                   and sysdate between
                       nvl(user_definition_enable_date, sysdate) and
                       nvl(disable_date, sysdate)
                   and inventory_enabled_flag = 'Y';
                update /*+ INDEX (xis XXPO_PO_DISTRIBUTION_STG_N5) */ xxpo_po_distribution_stg xis
                   set destination_organization_id = lv_dest_org_id,
                       last_updated_date           = sysdate,
                       last_updated_by             = g_last_updated_by,
                       last_update_login           = g_login_id
                 where leg_destination_organization = p_in_ship_to_org_name
                   and batch_id = g_batch_id
                   and leg_po_header_id = cur_dest_org_rec.leg_po_header_id --Added 13/10           Update based on PO Header Id
                   and leg_source_system = cur_plant_rec.leg_source_system
                   and run_sequence_id = g_new_run_seq_id
                   and org_id = cur_dest_org_rec.org_id; -- aditya/SDP 13/62016
                commit;
              exception
                when others then
                  l_err_code     := 'ETN_PO_INVALID_DESTINATION_ORG_NAME';
                  l_err_msg      := 'Error: Inventory Org Not Present in org_organization table Plant :' ||
                                    p_in_plant_num;
                  lv_errm        := 'Y';
                  l_column_name  := 'ship_to_organization_id';
                  l_column_value := p_out_org_name;
              end;
            end if;
            if lv_errm = 'Y' then
              update /*+ INDEX (xis XXPO_PO_DISTRIBUTION_STG_N7) */ xxpo_po_distribution_stg xis
                 set process_flag      = 'E',
                     error_type        = 'ERR_VAL',
                     last_updated_date = sysdate,
                     last_updated_by   = g_last_updated_by,
                     last_update_login = g_login_id
               where leg_destination_organization = p_in_ship_to_org_name
                 and batch_id = g_batch_id
                 and leg_po_header_id = cur_dest_org_rec.leg_po_header_id --Added 13/10           Update based on PO Header Id  --v1.16
                 and run_sequence_id = g_new_run_seq_id
                 and org_id = cur_dest_org_rec.org_id; -- aditya/SDP 13/62016
              commit;
              for r_org_ref_err_rec in (select /*+ INDEX (xis XXPO_PO_DISTRIBUTION_STG_N7) */
                                         interface_txn_id
                                          from xxpo_po_distribution_stg xis
                                         where leg_destination_organization =
                                               p_in_ship_to_org_name
                                           and leg_po_header_id =
                                               cur_dest_org_rec.leg_po_header_id --today
                                           and batch_id = g_batch_id
                                           and run_sequence_id =
                                               g_new_run_seq_id
                                           and org_id =
                                               cur_dest_org_rec.org_id -- aditya/SDP 13/62016
                                        ) loop
                --l_err_code := 'ETN_PO_DESTINATION_ORG_CROSSREF';
                --l_err_msg  := 'Error: Invalid Inventory Org Name. Error received while deriving new Destination Org name from Cross Reference API.';
                log_errors(pin_interface_txn_id    => r_org_ref_err_rec.interface_txn_id,
                           piv_source_table        => 'xxpo_po_distribution_stg',
                           piv_source_column_name  => l_column_name,
                           piv_source_column_value => l_column_value,
                           piv_source_keyname1     => null,
                           piv_source_keyvalue1    => null,
                           piv_error_type          => 'ERR_VAL',
                           piv_error_code          => l_err_code,
                           piv_error_message       => l_err_msg);
              end loop;
            end if;
          end loop;
        end;
      else
        for r_org_ref_err_rec in (select /*+ INDEX (xis XXPO_PO_DISTRIBUTION_STG_N7) */
                                   interface_txn_id
                                    from xxpo_po_distribution_stg xis
                                   where leg_destination_organization is null
                                     and leg_po_header_id =
                                         cur_dest_org_rec.leg_po_header_id --today
                                     and batch_id = g_batch_id
                                     and run_sequence_id = g_new_run_seq_id) loop
          l_err_code := 'ETN_PO_DESTINATION_ORG_NOT_PRESENT';
          l_err_msg  := 'Mandatory Coulumn leg_destination_organization not present';
          log_errors(pin_interface_txn_id    => r_org_ref_err_rec.interface_txn_id,
                     piv_source_table        => 'xxpo_po_distribution_stg',
                     piv_source_column_name  => 'leg_destination_organization',
                     piv_source_column_value => p_in_ship_to_org_name,
                     piv_source_keyname1     => null,
                     piv_source_keyvalue1    => null,
                     piv_error_type          => 'ERR_VAL',
                     piv_error_code          => l_err_code,
                     piv_error_message       => l_err_msg);
        end loop;
        update /*+ INDEX (xis XXPO_PO_DISTRIBUTION_STG_N7) */ xxpo_po_distribution_stg xis
           set process_flag      = 'E',
               error_type        = 'ERR_VAL',
               last_updated_date = sysdate,
               last_updated_by   = g_last_updated_by,
               last_update_login = g_login_id
         where leg_destination_organization is null
           and batch_id = g_batch_id
           and leg_po_header_id = cur_dest_org_rec.leg_po_header_id --Added 13/10           Update based on PO Header Id
           and run_sequence_id = g_new_run_seq_id;
        commit;
      end if;
    end loop;
    --  V1.17 (Changes End)   (Destination Organization from Cross Reference API for Destination Org)
    -- V1.19 -Changes for Inclusion of Cross reference API for TAX Mapping -- --V1.19
    --  for cur_po_headers_rec in cur_po_headers loop
    --Added to use  cur_po_lines_rec
    /*       FOR cur_po_lines_rec in cur_po_lines_tax/ *(cur_po_headers_rec.leg_po_header_id, --Added to use  cur_po_lines_rec
                                         cur_po_headers_rec.leg_source_system,
                                         cur_po_headers_rec.leg_operating_unit_name)* / loop

      IF (cur_po_lines_rec.leg_tax_name is not null) / *and
         (cur_po_lines_rec.leg_taxable_flag is not null)* / then

        -- new_leg_tax_name := null; -- For Final Tax name output value from API
        p_out_tax_name1 := null; -- For output value from API
        p_out_tax_name2 := null; -- For output value from API
        p_in_tax_name   := cur_po_lines_rec.leg_tax_name; -- Legacy Tax name

        BEGIN

          lv_out_value1  := null; --API Variables
          lv_out_value2  := null; --API Variables
          lv_out_value3  := null; --API Variables
          lv_err_message := null; --API Variables

          FOR cur_plant_rec in cur_plant(cur_po_lines_rec.leg_po_header_id
                                         ) loop

            BEGIN

               p_in_site_num := cur_plant_rec.leg_charge_account_seg1;

               xxetn_cross_ref_pkg.get_value(piv_eaton_ledger   => p_in_site_num, -- Site Number passed from cursor cur_plant
                                            piv_type           => 'XXEBTAX_TAX_CODE_MAPPING', --Value Required ?
                                            piv_direction      => 'E', --value Required ?
                                            piv_application    => 'XXAP', --value Required ?
                                            piv_input_value1   => p_in_tax_name,
                                            piv_input_value2   => null, --value Required
                                            piv_input_value3   => null, --value Required
                                            pid_effective_date => sysdate,
                                            pov_output_value1  => lv_out_value1,
                                            pov_output_value2  => lv_out_value2,
                                            pov_output_value3  => lv_out_value3,
                                            pov_err_msg        => lv_err_message);

               IF lv_err_message is null then

                  p_out_tax_name1 := lv_out_value1; --Incase new value is updated by API without Error Message


               ELSIF lv_err_message is not null -- (if error message received at Site Level go for Global Level without plant number)

               THEN


                  lv_err_message :=NULL;
                  p_out_tax_name1 :=NULL;

                  xxetn_cross_ref_pkg.get_value(piv_eaton_ledger   => null, -- Site Number passed from cursor cur_plant
                                                piv_type           => 'XXEBTAX_TAX_CODE_MAPPING', --Value Required ?
                                                piv_direction      => 'E', --value Required ?
                                                piv_application    => 'XXAP', --value Required ?
                                                piv_input_value1   => p_in_tax_name,
                                                piv_input_value2   => null,
                                                piv_input_value3   => null,
                                                pid_effective_date => sysdate,
                                                pov_output_value1  => lv_out_value1,
                                                pov_output_value2  => lv_out_value2,
                                                pov_output_value3  => lv_out_value3,
                                                pov_err_msg        => lv_err_message);

                  IF lv_err_message is null then

                     p_out_tax_name1 := lv_out_value1; --New Value from API called at Global Level


                  ELSIF lv_err_message is not null

                  THEN

                     p_out_tax_name1 := p_in_tax_name; --Incase the API throws error message


                  END IF;

              END IF;

              BEGIN

                 SELECT DISTINCT zrb.TAX_RATE_ID
                 INTO lv_dep_tax_code_id
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
                 AND zrb.tax_rate_code = p_out_tax_name1
                 AND hrou.organization_id = cur_po_lines_rec.org_id
                 AND TRUNC(SYSDATE) BETWEEN
                     TRUNC(NVL(zb.effective_from, SYSDATE)) AND
                     TRUNC(NVL(zb.effective_to, SYSDATE))
                 AND ZRB.ACTIVE_FLAG = 'Y';
              EXCEPTION
                 WHEN OTHERS
                   THEN
                     lv_dep_tax_code_id :=NULL;
                     FOR r_org_ref_err_rec in (select interface_txn_id
                                       FROM xxpo_po_line_shipment_stg xis
                                       WHERE leg_tax_name = cur_po_lines_rec.leg_tax_name
                                       AND   leg_po_header_id = cur_po_lines_rec.leg_po_header_id
                                       AND   leg_po_line_id = cur_po_lines_rec.leg_po_line_id
                                       AND   leg_po_line_location_id = cur_po_lines_rec.leg_po_line_location_id
                                       AND   batch_id = g_batch_id
                                       AND   run_sequence_id = g_new_run_seq_id) loop
                        l_err_code := 'ETN_PO_TAX_NAME_CROSSREF';
                        l_err_msg  := 'Error: Error Tax Code Not Setup for Plant in Cross ref for R12 setup- '||cur_plant_rec.leg_charge_account_seg1;
                        log_errors(pin_interface_txn_id    => r_org_ref_err_rec.interface_txn_id,
                                   piv_source_table        => 'xxpo_po_line_shipment_stg',
                                  piv_source_column_name  => 'leg_tax_name',
                                   piv_source_column_value => p_in_tax_name,
                                   piv_source_keyname1     => null,
                                   piv_source_keyvalue1    => null,
                                   piv_error_type          => 'ERR_VAL',
                                   piv_error_code          => l_err_code,
                                   piv_error_message       => l_err_msg);

                     END LOOP;

                     update / *+ INDEX (xis XXPO_PO_LINE_SHIPMENT_STG_N10) * / xxpo_po_line_shipment_stg xis
                     set process_flag      = 'E',
                         error_type        = 'ERR_VAL',
                         last_updated_date = sysdate,
                         last_updated_by   = g_last_updated_by,
                         last_update_login = g_login_id
                     where leg_tax_name = p_in_tax_name
                     and batch_id = g_batch_id
                     and leg_po_header_id = cur_po_lines_rec.leg_po_header_id --Added 13/10           Update based on PO Header Id
                     AND   leg_po_line_id = cur_po_lines_rec.leg_po_line_id
                     AND   leg_po_line_location_id = cur_po_lines_rec.leg_po_line_location_id
                     and run_sequence_id = g_new_run_seq_id;

                     COMMIT;
              END;

            EXCEPTION
              WHEN OTHERS THEN

                p_out_tax_name1 := p_in_tax_name;
                lv_dep_tax_code_id := NULL;

                FOR r_org_ref_err_rec in (select interface_txn_id
                                       FROM xxpo_po_line_shipment_stg xis
                                       WHERE leg_tax_name = cur_po_lines_rec.leg_tax_name
                                       AND   leg_po_header_id = cur_po_lines_rec.leg_po_header_id
                                       AND   leg_po_line_id = cur_po_lines_rec.leg_po_line_id
                                       AND   leg_po_line_location_id = cur_po_lines_rec.leg_po_line_location_id
                                       AND   batch_id = g_batch_id
                                       AND   run_sequence_id = g_new_run_seq_id) loop

                        l_err_code := 'ETN_PO_TAX_NAME_CROSSREF';
                        l_err_msg  := 'Error: Error Not Able TO Derive Tax Code From Cross Reference';
                        log_errors(pin_interface_txn_id    => r_org_ref_err_rec.interface_txn_id,
                                   piv_source_table        => 'xxpo_po_line_shipment_stg',
                                   piv_source_column_name  => 'leg_tax_name',
                                   piv_source_column_value => p_in_tax_name,
                                   piv_source_keyname1     => null,
                                   piv_source_keyvalue1    => null,
                                   piv_error_type          => 'ERR_VAL',
                                   piv_error_code          => l_err_code,
                                   piv_error_message       => l_err_msg);

                END LOOP;

                update / *+ INDEX (xis XXPO_PO_LINE_SHIPMENT_STG_N10) * / xxpo_po_line_shipment_stg xis
                     set process_flag      = 'E',
                         error_type        = 'ERR_VAL',
                         last_updated_date = sysdate,
                         last_updated_by   = g_last_updated_by,
                         last_update_login = g_login_id
                     where leg_tax_name = p_in_tax_name
                     and batch_id = g_batch_id
                     and leg_po_header_id = cur_po_lines_rec.leg_po_header_id --Added 13/10           Update based on PO Header Id
                     AND   leg_po_line_id = cur_po_lines_rec.leg_po_line_id
                     AND   leg_po_line_location_id = cur_po_lines_rec.leg_po_line_location_id
                     and run_sequence_id = g_new_run_seq_id;

                COMMIT;
             END;
          END LOOP;

          IF lv_dep_tax_code_id IS NOT NULL
          THEN


             update xxpo_po_line_shipment_stg xis
             set tax_name          = p_out_tax_name1,
                 tax_code_id       = lv_dep_tax_code_id,
                 last_updated_date = sysdate,
                 last_updated_by   = g_last_updated_by,
                 last_update_login = g_login_id
             where leg_tax_name = p_in_tax_name
             and batch_id = g_batch_id
             and leg_po_header_id = cur_po_lines_rec.leg_po_header_id --Added 13/10           Update based on PO Header Id
             AND   leg_po_line_id = cur_po_lines_rec.leg_po_line_id
             AND   leg_po_line_location_id = cur_po_lines_rec.leg_po_line_location_id
             and run_sequence_id = g_new_run_seq_id;

             COMMIT;

          END IF;

       END;


      end if;
    end loop; */
    --   end loop;
    ---Cross reference API for TAX End-- V1.19
    for cur_subinv_rec in cur_subinv loop
      lv_status := 'N';
      if (cur_subinv_rec.leg_destination_subinventory is not null) and
         (cur_subinv_rec.destination_organization_id is not null) then
        begin
          select 'N'
            into lv_status
            from mtl_secondary_inventories
           where cur_subinv_rec.leg_destination_subinventory =
                 secondary_inventory_name
             and organization_id =
                 cur_subinv_rec.destination_organization_id
             and sysdate <= nvl(disable_date, sysdate);
        exception
          when others then
            for r_org_ref_err_rec in (select interface_txn_id
                                        from xxpo_po_distribution_stg xis
                                       where leg_destination_subinventory =
                                             cur_subinv_rec.leg_destination_subinventory
                                         and batch_id = g_batch_id
                                         and run_sequence_id =
                                             g_new_run_seq_id) loop
              l_err_code := 'ETN_PO_INVALID_SUBINVENTORY';
              l_err_msg  := 'Error: Invalid destination subinventory value. ';
              log_errors(pin_interface_txn_id    => r_org_ref_err_rec.interface_txn_id,
                         piv_source_table        => 'xxpo_po_distribution_stg',
                         piv_source_column_name  => 'leg_destination_subinventory',
                         piv_source_column_value => cur_subinv_rec.leg_destination_subinventory,
                         piv_source_keyname1     => null,
                         piv_source_keyvalue1    => null,
                         piv_error_type          => 'ERR_VAL',
                         piv_error_code          => l_err_code,
                         piv_error_message       => l_err_msg);
            end loop;
            update xxpo_po_distribution_stg
               set process_flag      = 'E',
                   error_type        = 'ERR_VAL',
                   last_updated_date = sysdate,
                   last_updated_by   = g_last_updated_by,
                   last_update_login = g_login_id
             where leg_destination_subinventory =
                   cur_subinv_rec.leg_destination_subinventory
               and batch_id = g_batch_id
               and run_sequence_id = g_new_run_seq_id;
            commit;
        end;
      end if;
    end loop;
    for cur_code_rec in cur_code loop
      x_charge_ccid        := null;
      x_out_charge_acc_rec := null;
      validate_accounts(null,
                        'xxpo_po_distribution_stg',
                        null,
                        cur_code_rec.segment1,
                        cur_code_rec.segment2,
                        cur_code_rec.segment3,
                        cur_code_rec.segment4,
                        cur_code_rec.segment5,
                        cur_code_rec.segment6,
                        cur_code_rec.segment7,
                        x_out_charge_acc_rec,
                        x_charge_ccid);
      if x_charge_ccid is null then
        update /*+ INDEX (xis XXPO_PO_DISTRIBUTION_STG_N8) */ xxpo_po_distribution_stg xis
           set process_flag         = 'E',
               error_type           = 'ERR_VAL',
               charge_account_seg1  = x_out_charge_acc_rec.segment1,
               charge_account_seg2  = x_out_charge_acc_rec.segment2,
               charge_account_seg3  = x_out_charge_acc_rec.segment3,
               charge_account_seg4  = x_out_charge_acc_rec.segment4,
               charge_account_seg5  = x_out_charge_acc_rec.segment5,
               charge_account_seg6  = x_out_charge_acc_rec.segment6,
               charge_account_seg7  = x_out_charge_acc_rec.segment7,
               charge_account_seg8  = x_out_charge_acc_rec.segment8,
               charge_account_seg9  = x_out_charge_acc_rec.segment9,
               charge_account_seg10 = x_out_charge_acc_rec.segment10,
               charge_account_ccid  = x_charge_ccid
         where leg_charge_account_seg1 = cur_code_rec.segment1
           and leg_charge_account_seg2 = cur_code_rec.segment2
           and leg_charge_account_seg3 = cur_code_rec.segment3
           and leg_charge_account_seg4 = cur_code_rec.segment4
           and leg_charge_account_seg5 = cur_code_rec.segment5
           and leg_charge_account_seg6 = cur_code_rec.segment6
           and leg_charge_account_seg7 = cur_code_rec.segment7
           and batch_id = g_batch_id
           and run_sequence_id = g_new_run_seq_id;
        update /*+ INDEX (xis XXPO_PO_DISTRIBUTION_STG_N9) */ xxpo_po_distribution_stg xis
           set process_flag          = 'E',
               error_type            = 'ERR_VAL',
               accural_account_seg1  = x_out_charge_acc_rec.segment1,
               accural_account_seg2  = x_out_charge_acc_rec.segment2,
               accural_account_seg3  = x_out_charge_acc_rec.segment3,
               accural_account_seg4  = x_out_charge_acc_rec.segment4,
               accural_account_seg5  = x_out_charge_acc_rec.segment5,
               accural_account_seg6  = x_out_charge_acc_rec.segment6,
               accural_account_seg7  = x_out_charge_acc_rec.segment7,
               accural_account_seg8  = x_out_charge_acc_rec.segment8,
               accural_account_seg9  = x_out_charge_acc_rec.segment9,
               accural_account_seg10 = x_out_charge_acc_rec.segment10,
               accural_account_ccid  = x_charge_ccid
         where leg_accural_account_seg1 = cur_code_rec.segment1
           and leg_accural_account_seg2 = cur_code_rec.segment2
           and leg_accural_account_seg3 = cur_code_rec.segment3
           and leg_accural_account_seg4 = cur_code_rec.segment4
           and leg_accural_account_seg5 = cur_code_rec.segment5
           and leg_accural_account_seg6 = cur_code_rec.segment6
           and leg_accural_account_seg7 = cur_code_rec.segment7
           and batch_id = g_batch_id
           and run_sequence_id = g_new_run_seq_id;
        update /*+ INDEX (xis XXPO_PO_DISTRIBUTION_STG_N10) */ xxpo_po_distribution_stg xis
           set process_flag           = 'E',
               error_type             = 'ERR_VAL',
               variance_account_seg1  = x_out_charge_acc_rec.segment1,
               variance_account_seg2  = x_out_charge_acc_rec.segment2,
               variance_account_seg3  = x_out_charge_acc_rec.segment3,
               variance_account_seg4  = x_out_charge_acc_rec.segment4,
               variance_account_seg5  = x_out_charge_acc_rec.segment5,
               variance_account_seg6  = x_out_charge_acc_rec.segment6,
               variance_account_seg7  = x_out_charge_acc_rec.segment7,
               variance_account_seg8  = x_out_charge_acc_rec.segment8,
               variance_account_seg9  = x_out_charge_acc_rec.segment9,
               variance_account_seg10 = x_out_charge_acc_rec.segment10,
               variance_account_ccid  = x_charge_ccid
         where leg_variance_account_seg1 = cur_code_rec.segment1
           and leg_variance_account_seg2 = cur_code_rec.segment2
           and leg_variance_account_seg3 = cur_code_rec.segment3
           and leg_variance_account_seg4 = cur_code_rec.segment4
           and leg_variance_account_seg5 = cur_code_rec.segment5
           and leg_variance_account_seg6 = cur_code_rec.segment6
           and leg_variance_account_seg7 = cur_code_rec.segment7
           and batch_id = g_batch_id
           and run_sequence_id = g_new_run_seq_id;
      else
        update /*+ INDEX (xis XXPO_PO_DISTRIBUTION_STG_N8) */ xxpo_po_distribution_stg xis
           set charge_account_seg1  = x_out_charge_acc_rec.segment1,
               charge_account_seg2  = x_out_charge_acc_rec.segment2,
               charge_account_seg3  = x_out_charge_acc_rec.segment3,
               charge_account_seg4  = x_out_charge_acc_rec.segment4,
               charge_account_seg5  = x_out_charge_acc_rec.segment5,
               charge_account_seg6  = x_out_charge_acc_rec.segment6,
               charge_account_seg7  = x_out_charge_acc_rec.segment7,
               charge_account_seg8  = x_out_charge_acc_rec.segment8,
               charge_account_seg9  = x_out_charge_acc_rec.segment9,
               charge_account_seg10 = x_out_charge_acc_rec.segment10,
               charge_account_ccid  = x_charge_ccid
         where leg_charge_account_seg1 = cur_code_rec.segment1
           and leg_charge_account_seg2 = cur_code_rec.segment2
           and leg_charge_account_seg3 = cur_code_rec.segment3
           and leg_charge_account_seg4 = cur_code_rec.segment4
           and leg_charge_account_seg5 = cur_code_rec.segment5
           and leg_charge_account_seg6 = cur_code_rec.segment6
           and leg_charge_account_seg7 = cur_code_rec.segment7
           and batch_id = g_batch_id
           and run_sequence_id = g_new_run_seq_id;
        update /*+ INDEX (xis XXPO_PO_DISTRIBUTION_STG_N9) */ xxpo_po_distribution_stg xis
           set accural_account_seg1  = x_out_charge_acc_rec.segment1,
               accural_account_seg2  = x_out_charge_acc_rec.segment2,
               accural_account_seg3  = x_out_charge_acc_rec.segment3,
               accural_account_seg4  = x_out_charge_acc_rec.segment4,
               accural_account_seg5  = x_out_charge_acc_rec.segment5,
               accural_account_seg6  = x_out_charge_acc_rec.segment6,
               accural_account_seg7  = x_out_charge_acc_rec.segment7,
               accural_account_seg8  = x_out_charge_acc_rec.segment8,
               accural_account_seg9  = x_out_charge_acc_rec.segment9,
               accural_account_seg10 = x_out_charge_acc_rec.segment10,
               accural_account_ccid  = x_charge_ccid
         where leg_accural_account_seg1 = cur_code_rec.segment1
           and leg_accural_account_seg2 = cur_code_rec.segment2
           and leg_accural_account_seg3 = cur_code_rec.segment3
           and leg_accural_account_seg4 = cur_code_rec.segment4
           and leg_accural_account_seg5 = cur_code_rec.segment5
           and leg_accural_account_seg6 = cur_code_rec.segment6
           and leg_accural_account_seg7 = cur_code_rec.segment7
           and batch_id = g_batch_id
           and run_sequence_id = g_new_run_seq_id;
        update /*+ INDEX (xis XXPO_PO_DISTRIBUTION_STG_N10) */ xxpo_po_distribution_stg xis
           set variance_account_seg1  = x_out_charge_acc_rec.segment1,
               variance_account_seg2  = x_out_charge_acc_rec.segment2,
               variance_account_seg3  = x_out_charge_acc_rec.segment3,
               variance_account_seg4  = x_out_charge_acc_rec.segment4,
               variance_account_seg5  = x_out_charge_acc_rec.segment5,
               variance_account_seg6  = x_out_charge_acc_rec.segment6,
               variance_account_seg7  = x_out_charge_acc_rec.segment7,
               variance_account_seg8  = x_out_charge_acc_rec.segment8,
               variance_account_seg9  = x_out_charge_acc_rec.segment9,
               variance_account_seg10 = x_out_charge_acc_rec.segment10,
               variance_account_ccid  = x_charge_ccid
         where leg_variance_account_seg1 = cur_code_rec.segment1
           and leg_variance_account_seg2 = cur_code_rec.segment2
           and leg_variance_account_seg3 = cur_code_rec.segment3
           and leg_variance_account_seg4 = cur_code_rec.segment4
           and leg_variance_account_seg5 = cur_code_rec.segment5
           and leg_variance_account_seg6 = cur_code_rec.segment6
           and leg_variance_account_seg7 = cur_code_rec.segment7
           and batch_id = g_batch_id
           and run_sequence_id = g_new_run_seq_id;
      end if;
      commit;
    end loop;
    --- header cursor starts
    for cur_po_headers_rec in cur_po_headers loop
      l_err_code              := null;
      l_err_msg               := null;
      lv_org_id               := null;
      lv_org_name             := null;
      lv_error_flag           := 'N';
      lv_header_error_flag    := 'N';
      lv_mast_d_line_err_flag := 'N';
      lv_mast_line_err_flag   := 'N';
      lv_func_currency_code   := null;
      lv_agent_id             := null;
      lv_vendor_id            := null;
      lv_vendor_site_id       := null;
      lv_ship_to_location_id  := null;
      lv_bill_to_location_id  := null;
      lv_vendor_contact_id    := null;
      lv_pay_name             := null;
      lv_terms_id             := null;
      lv_attribute_category   := null;
      lv_attribute1           := null;
      lv_attribute2           := null;
      lv_attribute3           := null;
      lv_attribute4           := null;
      lv_attribute5           := null;
      lv_attribute6           := null;
      lv_attribute7           := null;
      lv_attribute8           := null;
      lv_attribute9           := null;
      lv_attribute10          := null;
      lv_attribute11          := null;
      lv_attribute12          := null;
      lv_attribute13          := null;
      lv_attribute14          := null;
      lv_attribute15          := null;
      xxetn_debug_pkg.add_debug('+---------------------------------------------------------------------------------+');
      xxetn_debug_pkg.add_debug('Starting processing for Purchase Order number =  ' ||
                                cur_po_headers_rec.leg_document_num);
      xxetn_debug_pkg.add_debug('CALLING FUNCTION: xxpo_dup_name for  ' ||
                                cur_po_headers_rec.interface_txn_id);
      --- check PO number is not duplicate
      if cur_po_headers_rec.leg_document_num is not null then
        lv_val_flag := 'N';
        lv_val_flag := xxpo_dup_po(cur_po_headers_rec.interface_txn_id,
                                   cur_po_headers_rec.leg_document_num,
                                   cur_po_headers_rec.org_id); -- v 1.24 -- SDP --25/03/2016
        if lv_val_flag = 'Y' then
          lv_error_flag := 'Y';
        end if;
      else
        lv_error_flag := 'Y';
        l_err_code    := 'ETN_PO_MANDATORY_NOT_ENTERED';
        l_err_msg     := 'Error: Mandatory column not entered.  ';
        log_errors(pin_interface_txn_id    => cur_po_headers_rec.interface_txn_id,
                   piv_source_table        => 'xxpo_po_header_stg',
                   piv_source_column_name  => 'leg_document_num',
                   piv_source_column_value => cur_po_headers_rec.leg_document_num,
                   piv_source_keyname1     => null,
                   piv_source_keyvalue1    => null,
                   piv_error_type          => 'ERR_VAL',
                   piv_error_code          => l_err_code,
                   piv_error_message       => l_err_msg);
      end if;
      /*          xxetn_debug_pkg.add_debug
      (   'Calling function xxpo_derive_org_id for PO number =  '
       || cur_po_headers_rec.leg_document_num
      ); */
      --- derive operating unit           ---1.1
      /* IF cur_po_headers_rec.leg_operating_unit_name IS NOT NULL
      THEN
         xxpo_derive_org_id (cur_po_headers_rec.interface_txn_id,
                             'xxpo_po_header_stg',
                             'leg_operating_unit_name',
                             cur_po_headers_rec.leg_operating_unit_name,
                             lv_org_name,
                             lv_org_id
                            );

         IF lv_org_id IS NULL
         THEN
            lv_error_flag := 'Y';
         END IF;
      ELSE
         lv_error_flag := 'Y';
         l_err_code := 'ETN_PO_MANDATORY_NOT_ENTERED';
         l_err_msg := 'Error: Mandatory column not entered.  ';
         log_errors
            (pin_interface_txn_id         => cur_po_headers_rec.interface_txn_id,
            piv_source_table             => 'xxpo_po_header_stg',
             piv_source_column_name       => 'leg_operating_unit_name',
             piv_source_column_value      => cur_po_headers_rec.leg_operating_unit_name,
             piv_source_keyname1          => NULL,
             piv_source_keyvalue1         => NULL,
             piv_error_type               => 'ERR_VAL',
             piv_error_code               => l_err_code,
             piv_error_message            => l_err_msg
            );
      END IF; */
      --- check currency code                    ---1.1
      /* IF cur_po_headers_rec.leg_currency_code IS NULL
      THEN
         lv_error_flag := 'Y';
         l_err_code := 'ETN_PO_MANDATORY_NOT_ENTERED';
         l_err_msg := 'Error: Mandatory column not entered.  ';
         log_errors
            (pin_interface_txn_id         => cur_po_headers_rec.interface_txn_id,
             piv_source_table             => 'xxpo_po_header_stg',
             piv_source_column_name       => 'leg_currency_code',
             piv_source_column_value      => cur_po_headers_rec.leg_currency_code,
             piv_source_keyname1          => NULL,
             piv_source_keyvalue1         => NULL,
             piv_error_type               => 'ERR_VAL',
             piv_error_code               => l_err_code,
             piv_error_message            => l_err_msg
            );
      ELSE
         lv_val_flag := 'N';
         lv_val_flag :=
            xxpo_invcurr (cur_po_headers_rec.interface_txn_id,
                          'xxpo_po_header_stg',
                          'leg_currency_code',
                          cur_po_headers_rec.leg_currency_code
                         );

         IF lv_val_flag = 'Y'
         THEN
            lv_error_flag := 'Y';
         END IF; */
      --- deriving foreign currency code  --1.1
      if (cur_po_headers_rec.leg_currency_code is not null) and
         (cur_po_headers_rec.org_id is not null) then
        begin
          select currency_code
            into lv_func_currency_code
            from gl_sets_of_books
           where set_of_books_id =
                 (select set_of_books_id
                    from hr_operating_units
                   where organization_id = cur_po_headers_rec.org_id);
        exception
          when others then
            lv_func_currency_code := null;
        end;
      end if;
      --- setting rate type for base currency
      if cur_po_headers_rec.leg_currency_code = lv_func_currency_code then
        update xxpo_po_header_stg
           set leg_rate_type = null, leg_rate = null, leg_rate_date = null
         where interface_txn_id = cur_po_headers_rec.interface_txn_id;
        --v1.26 start uncommenting the v1.25 else clause
        --v1.25 start
        else
        update xxpo_po_header_stg
           set leg_rate_type = 'User'
         where interface_txn_id = cur_po_headers_rec.interface_txn_id;
        --v1.25  end
        --v1.26 ends
      end if;
      commit;
      xxetn_debug_pkg.add_debug('Calling function xxpo_fob for PO number =  ' ||
                                cur_po_headers_rec.leg_document_num);
      --- Checking FOB
      if cur_po_headers_rec.leg_fob is not null then
        lv_val_flag := 'N';
        lv_val_flag := xxpo_fob(cur_po_headers_rec.interface_txn_id,
                                'xxpo_po_header_stg',
                                'leg_fob',
                                cur_po_headers_rec.leg_fob);
        if lv_val_flag = 'Y' then
          lv_error_flag := 'Y';
        end if;
      end if;
      ---Checking freight terms
      xxetn_debug_pkg.add_debug('Calling function xxpo_freight for PO number =  ' ||
                                cur_po_headers_rec.leg_document_num);
      if cur_po_headers_rec.leg_freight_terms is not null then
        lv_val_flag := 'N';
        lv_val_flag := xxpo_freight(cur_po_headers_rec.interface_txn_id,
                                    'xxpo_po_header_stg',
                                    'leg_freight_terms',
                                    cur_po_headers_rec.leg_freight_terms);
        if lv_val_flag = 'Y' then
          lv_error_flag := 'Y';
        end if;
      end if;
      --- checking Document code type
      if cur_po_headers_rec.leg_document_type_code is not null then
        if cur_po_headers_rec.leg_document_type_code <> g_document_type then
          lv_error_flag := 'Y';
          l_err_code    := 'ETN_PO_INVALID_DOC_TYPE';
          l_err_msg     := 'Error: Document type should be Standard.  ';
          log_errors(pin_interface_txn_id    => cur_po_headers_rec.interface_txn_id,
                     piv_source_table        => 'xxpo_po_header_stg',
                     piv_source_column_name  => 'leg_document_type_code',
                     piv_source_column_value => cur_po_headers_rec.leg_document_type_code,
                     piv_source_keyname1     => null,
                     piv_source_keyvalue1    => null,
                     piv_error_type          => 'ERR_VAL',
                     piv_error_code          => l_err_code,
                     piv_error_message       => l_err_msg);
        end if;
      end if;
      --- checking rate type and rate
      if cur_po_headers_rec.leg_currency_code <> lv_func_currency_code then
        if cur_po_headers_rec.leg_rate is null
          --v1.26  starts commenting the 1.25 change below
          --v1.25 change starts
           --and nvl(cur_po_headers_rec.leg_rate_type, 'X') = 'User'
        --v1.25 change ends
        --v1.26 ends
         then
          lv_error_flag := 'Y';
          l_err_code    := 'ETN_PO_INVALID_EXCHANGE_RATE';
          l_err_msg     := 'Error: Exchange rate cannot be NULL for USER rate type  ';
          log_errors(pin_interface_txn_id    => cur_po_headers_rec.interface_txn_id,
                     piv_source_table        => 'xxpo_po_header_stg',
                     piv_source_column_name  => 'leg_rate',
                     piv_source_column_value => cur_po_headers_rec.leg_rate,
                     piv_source_keyname1     => null,
                     piv_source_keyvalue1    => null,
                     piv_error_type          => 'ERR_VAL',
                     piv_error_code          => l_err_code,
                     piv_error_message       => l_err_msg);
        end if;
      end if;
      ---assigning DFF
      lv_attribute_category := cur_po_headers_rec.leg_attribute_category;
      lv_attribute1         := cur_po_headers_rec.leg_attribute1;
      lv_attribute2         := cur_po_headers_rec.leg_attribute2;
      lv_attribute3         := cur_po_headers_rec.leg_attribute3;
      lv_attribute4         := cur_po_headers_rec.leg_attribute4;
      lv_attribute5         := cur_po_headers_rec.leg_attribute5;
      lv_attribute6         := cur_po_headers_rec.leg_attribute6;
      lv_attribute7         := cur_po_headers_rec.leg_attribute7;
      lv_attribute8         := cur_po_headers_rec.leg_attribute8;
      lv_attribute9         := cur_po_headers_rec.leg_attribute9;
      lv_attribute10        := cur_po_headers_rec.leg_attribute10;
      lv_attribute11        := cur_po_headers_rec.leg_attribute11;
      lv_attribute12        := cur_po_headers_rec.leg_attribute12;
      lv_attribute13        := cur_po_headers_rec.leg_attribute13;
      lv_attribute14        := cur_po_headers_rec.leg_attribute14;
      lv_attribute15        := cur_po_headers_rec.leg_attribute15;
      --- to validate rate type
      xxetn_debug_pkg.add_debug('Calling function xxpo_po_rate_type for PO number =  ' ||
                                cur_po_headers_rec.leg_document_num);
      if cur_po_headers_rec.leg_currency_code is not null and
         cur_po_headers_rec.leg_rate_type is not null then
        lv_val_flag := 'N';
        lv_val_flag := xxpo_po_rate_type(cur_po_headers_rec.interface_txn_id,
                                         'xxpo_po_header_stg',
                                         'leg_rate_type',
                                         cur_po_headers_rec.leg_currency_code,
                                         cur_po_headers_rec.leg_rate_type);
        if lv_val_flag = 'Y' then
          lv_error_flag := 'Y';
        end if;
      end if;
      --- to validate acceptance date
      if cur_po_headers_rec.leg_acceptance_required_flag is not null then
        lv_val_flag := 'N';
        if (cur_po_headers_rec.leg_acceptance_required_flag = 'Y') then
          select decode(cur_po_headers_rec.leg_acceptance_due_date,
                        null,
                        'Y',
                        'N')
            into lv_val_flag
            from dual;
        else
          if cur_po_headers_rec.leg_acceptance_due_date is not null then
            lv_val_flag := 'Y';
          end if;
        end if;
        if lv_val_flag = 'Y' then
          lv_error_flag := 'Y';
          l_err_code    := 'ETN_PO_INVALID_ACCEPTANCE_DATE';
          l_err_msg     := 'Error: Invalid acceptance date.  ';
          log_errors(pin_interface_txn_id    => cur_po_headers_rec.interface_txn_id,
                     piv_source_table        => 'xxpo_po_header_stg',
                     piv_source_column_name  => 'leg_acceptance_due_date',
                     piv_source_column_value => cur_po_headers_rec.leg_acceptance_due_date,
                     piv_source_keyname1     => null,
                     piv_source_keyvalue1    => null,
                     piv_error_type          => 'ERR_VAL',
                     piv_error_code          => l_err_code,
                     piv_error_message       => l_err_msg);
        end if;
      end if;
      --- to validate confirming order flag                 --1.1
      /* IF cur_po_headers_rec.leg_confirming_order_flag IS NOT NULL
      THEN
         lv_val_flag := 'N';
         lv_val_flag :=
            xxpo_yes_no (cur_po_headers_rec.interface_txn_id,
                         'xxpo_po_header_stg',
                         'leg_confirming_order_flag',
                         cur_po_headers_rec.leg_confirming_order_flag
                        );

         IF lv_val_flag = 'Y'
         THEN
            lv_error_flag := 'Y';
         END IF;
      END IF; */
      --- to validate acceptance required flag          ---1.1
      /*  IF cur_po_headers_rec.leg_acceptance_required_flag IS NOT NULL
      THEN
         lv_val_flag := 'N';
         lv_val_flag :=
            xxpo_yes_no (cur_po_headers_rec.interface_txn_id,
                         'xxpo_po_header_stg',
                         'leg_acceptance_required_flag',
                         cur_po_headers_rec.leg_acceptance_required_flag
                        );

         IF lv_val_flag = 'Y'
         THEN
            lv_error_flag := 'Y';
         END IF;
      END IF; */
      --- validate freight carrier value
      xxetn_debug_pkg.add_debug('Calling function xxpo_shipvialookup for PO number =  ' ||
                                cur_po_headers_rec.leg_document_num);
      if cur_po_headers_rec.leg_freight_carrier is not null then
        lv_val_flag := 'N';
        lv_val_flag := xxpo_shipvialookup(cur_po_headers_rec.interface_txn_id,
                                          'xxpo_po_header_stg',
                                          'leg_freight_carrier',
                                          cur_po_headers_rec.leg_freight_carrier);
        if lv_val_flag = 'Y' then
          lv_error_flag := 'Y';
        end if;
      end if;
      --- Validate agent no.              --- 1.1
      /* xxetn_debug_pkg.add_debug
                     (   'Calling function xxpo_agent_id for PO number =  '
                      || cur_po_headers_rec.leg_document_num
                     );

      IF cur_po_headers_rec.leg_agent_emp_no IS NOT NULL
      THEN
         lv_agent_id :=
            xxpo_agent_id (cur_po_headers_rec.interface_txn_id,
                           'xxpo_po_header_stg',
                           'leg_agent_emp_no',
                           cur_po_headers_rec.leg_agent_emp_no
                          );

         IF lv_agent_id IS NULL
         THEN
            lv_error_flag := 'Y';
         END IF;
      ELSE
         lv_error_flag := 'Y';
         l_err_code := 'ETN_PO_MANDATORY_NOT_ENTERED';
         l_err_msg := 'Error: Mandatory column not entered.  ';
         log_errors
            (pin_interface_txn_id         => cur_po_headers_rec.interface_txn_id,
             piv_source_table             => 'xxpo_po_header_stg',
             piv_source_column_name       => 'leg_agent_emp_no',
             piv_source_column_value      => cur_po_headers_rec.leg_agent_emp_no,
             piv_source_keyname1          => NULL,
             piv_source_keyvalue1         => NULL,
             piv_error_type               => 'ERR_VAL',
             piv_error_code               => l_err_code,
             piv_error_message            => l_err_msg
            );
      END IF; */
      ---Validate vendor
      xxetn_debug_pkg.add_debug('Calling function xxpo_vendor_id for PO number =  ' ||
                                cur_po_headers_rec.leg_document_num);
      if cur_po_headers_rec.leg_vendor_num is not null then
        lv_vendor_id := xxpo_vendor_id(cur_po_headers_rec.interface_txn_id,
                                       'xxpo_po_header_stg',
                                       'leg_vendor_num',
                                       cur_po_headers_rec.leg_vendor_num);
        if lv_vendor_id is null then
          lv_error_flag := 'Y';
        end if;
      else
        lv_error_flag := 'Y';
        l_err_code    := 'ETN_PO_MANDATORY_NOT_ENTERED';
        l_err_msg     := 'Error: Mandatory column not entered.  ';
        log_errors(pin_interface_txn_id    => cur_po_headers_rec.interface_txn_id,
                   piv_source_table        => 'xxpo_po_header_stg',
                   piv_source_column_name  => 'leg_vendor_num',
                   piv_source_column_value => cur_po_headers_rec.leg_vendor_num,
                   piv_source_keyname1     => null,
                   piv_source_keyvalue1    => null,
                   piv_error_type          => 'ERR_VAL',
                   piv_error_code          => l_err_code,
                   piv_error_message       => l_err_msg);
      end if;
      --- Validate vendor site code
      xxetn_debug_pkg.add_debug('Calling function xxpo_vendor_site_id for PO number =  ' ||
                                cur_po_headers_rec.leg_document_num);
      if cur_po_headers_rec.leg_vendor_site_code is not null then
        if cur_po_headers_rec.leg_vendor_num is not null and
           cur_po_headers_rec.org_id is not null --1.1
         then
          lv_vendor_site_id := xxpo_vendor_site_id(cur_po_headers_rec.interface_txn_id,
                                                   'xxpo_po_header_stg',
                                                   'leg_vendor_site_code',
                                                   cur_po_headers_rec.leg_vendor_site_code,
                                                   cur_po_headers_rec.leg_vendor_num,
                                                   cur_po_headers_rec.org_id,
                                                   cur_po_headers_rec.leg_po_header_id);
          if lv_vendor_site_id is null then
            lv_error_flag := 'Y';
          end if;
        end if;
      else
        lv_error_flag := 'Y';
        l_err_code    := 'ETN_PO_MANDATORY_NOT_ENTERED';
        l_err_msg     := 'Error: Mandatory column not entered.  ';
        log_errors(pin_interface_txn_id    => cur_po_headers_rec.interface_txn_id,
                   piv_source_table        => 'xxpo_po_header_stg',
                   piv_source_column_name  => 'leg_vendor_site_code',
                   piv_source_column_value => cur_po_headers_rec.leg_vendor_site_code,
                   piv_source_keyname1     => null,
                   piv_source_keyvalue1    => null,
                   piv_error_type          => 'ERR_VAL',
                   piv_error_code          => l_err_code,
                   piv_error_message       => l_err_msg);
      end if;
      -- VALIDATE ship to location           ---1.1
      /* xxetn_debug_pkg.add_debug
               (   'Calling function xxpo_ship_to_loc_id for PO number =  '
                || cur_po_headers_rec.leg_document_num
               );

      IF cur_po_headers_rec.leg_ship_to_location IS NOT NULL
      THEN
         lv_ship_to_location_id :=
            xxpo_ship_to_loc_id (cur_po_headers_rec.interface_txn_id,
                                 'xxpo_po_header_stg',
                                 'leg_ship_to_location',
                                 cur_po_headers_rec.leg_ship_to_location
                                );

         IF lv_ship_to_location_id IS NULL
         THEN
            lv_error_flag := 'Y';
         END IF;
      ELSE
         lv_error_flag := 'Y';
         l_err_code := 'ETN_PO_MANDATORY_NOT_ENTERED';
         l_err_msg := 'Error: Mandatory column not entered.  ';
         log_errors
            (pin_interface_txn_id         => cur_po_headers_rec.interface_txn_id,
             piv_source_table             => 'xxpo_po_header_stg',
             piv_source_column_name       => 'leg_ship_to_location',
             piv_source_column_value      => cur_po_headers_rec.leg_ship_to_location,
             piv_source_keyname1          => NULL,
             piv_source_keyvalue1         => NULL,
             piv_error_type               => 'ERR_VAL',
             piv_error_code               => l_err_code,
             piv_error_message            => l_err_msg
            );
      END IF; */
      -- VALIDATE bill to location              --1.1
      /* xxetn_debug_pkg.add_debug
               (   'Calling function xxpo_bill_to_loc_id for PO number =  '
                || cur_po_headers_rec.leg_document_num
               );

      IF cur_po_headers_rec.leg_bill_to_location IS NOT NULL
      THEN
         lv_bill_to_location_id :=
            xxpo_bill_to_loc_id (cur_po_headers_rec.interface_txn_id,
                                 'xxpo_po_header_stg',
                                 'leg_bill_to_location',
                                 cur_po_headers_rec.leg_bill_to_location
                                );

         IF lv_bill_to_location_id IS NULL
         THEN
            lv_error_flag := 'Y';
         END IF;
      ELSE
         lv_error_flag := 'Y';
         l_err_code := 'ETN_PO_MANDATORY_NOT_ENTERED';
         l_err_msg := 'Error: Mandatory column not entered.  ';
         log_errors
            (pin_interface_txn_id         => cur_po_headers_rec.interface_txn_id,
             piv_source_table             => 'xxpo_po_header_stg',
             piv_source_column_name       => 'leg_bill_to_location',
             piv_source_column_value      => cur_po_headers_rec.leg_bill_to_location,
             piv_source_keyname1          => NULL,
             piv_source_keyvalue1         => NULL,
             piv_error_type               => 'ERR_VAL',
             piv_error_code               => l_err_code,
             piv_error_message            => l_err_msg
            );
      END IF; */
      --- Validate payment terms  --1.1
      /* xxetn_debug_pkg.add_debug
                     (   'Calling function xxpo_terms_id for PO number =  '
                      || cur_po_headers_rec.leg_document_num
                     );

      IF cur_po_headers_rec.leg_payment_terms IS NOT NULL
      THEN
         xxpo_terms_id (cur_po_headers_rec.interface_txn_id,
                        'xxpo_po_header_stg',
                        'leg_payment_terms',
                        cur_po_headers_rec.leg_payment_terms,
                        lv_pay_name,
                        lv_terms_id
                       );

         IF lv_terms_id IS NULL
         THEN
            lv_error_flag := 'Y';
         END IF;
      ELSE
         lv_error_flag := 'Y';
         l_err_code := 'ETN_PO_MANDATORY_NOT_ENTERED';
         l_err_msg := 'Error: Mandatory column not entered.  ';
         log_errors
            (pin_interface_txn_id         => cur_po_headers_rec.interface_txn_id,
             piv_source_table             => 'xxpo_po_header_stg',
             piv_source_column_name       => 'leg_payment_terms',
             piv_source_column_value      => cur_po_headers_rec.leg_payment_terms,
             piv_source_keyname1          => NULL,
             piv_source_keyvalue1         => NULL,
             piv_error_type               => 'ERR_VAL',
             piv_error_code               => l_err_code,
             piv_error_message            => l_err_msg
            );
      END IF; */
      --- validate vendor contact
      xxetn_debug_pkg.add_debug('Calling function xxpo_po_ven_contact_id for PO number =  ' ||
                                cur_po_headers_rec.leg_document_num);
      if cur_po_headers_rec.org_id is not null ---1.1
         and cur_po_headers_rec.leg_vendor_num is not null and
         cur_po_headers_rec.leg_vendor_contact_fname is not null and
         cur_po_headers_rec.leg_vendor_contact_lname is not null and
         cur_po_headers_rec.leg_vendor_site_code is not null then
        lv_vendor_contact_id := xxpo_po_ven_contact_id(cur_po_headers_rec.interface_txn_id,
                                                       'xxpo_po_header_stg',
                                                       'leg_vendor_contact_fname',
                                                       lv_vendor_id,
                                                       cur_po_headers_rec.leg_vendor_contact_fname,
                                                       cur_po_headers_rec.leg_vendor_contact_lname,
                                                       cur_po_headers_rec.org_id);
        if lv_vendor_contact_id is null then
          lv_error_flag := 'Y';
        end if;
      end if;
      if lv_error_flag = 'Y' or cur_po_headers_rec.process_flag = 'E' then
        lv_header_error_flag := 'Y';
      end if;
      ----Line cursor starts
      for cur_po_lines_rec in cur_po_lines(cur_po_headers_rec.leg_po_header_id,
                                           cur_po_headers_rec.leg_source_system,
                                           cur_po_headers_rec.leg_operating_unit_name) loop
        lv_line_err_flag               := 'N';
        l_err_code                     := null;
        l_err_msg                      := null;
        lv_dep_receiving_routing_id    := null;
        lv_dep_line_type_id            := null;
        lv_dep_uom_code                := null;
        lv_dep_un_number_id            := null;
        lv_dep_hazard_class_id         := null;
        lv_dep_ship_to_organization_id := null;
        lv_dep_item_id                 := null;
        lv_dep_ship_to_location_id     := null;
        lv_dep_tax_code_id             := null;
        lv_l_attribute_category        := null;
        lv_l_attribute1                := null;
        lv_l_attribute2                := null;
        lv_l_attribute3                := null;
        lv_l_attribute4                := null;
        lv_l_attribute5                := null;
        lv_l_attribute6                := null;
        lv_l_attribute7                := null;
        lv_l_attribute8                := null;
        lv_l_attribute9                := null;
        lv_l_attribute10               := null;
        lv_l_attribute11               := null;
        lv_l_attribute12               := null;
        lv_l_attribute13               := null;
        lv_l_attribute14               := null;
        lv_l_attribute15               := null;
        lv_ship_attribute_category     := null;
        lv_ship_attribute1             := null;
        lv_ship_attribute2             := null;
        lv_ship_attribute3             := null;
        lv_ship_attribute4             := null;
        lv_ship_attribute5             := null;
        lv_ship_attribute6             := null;
        lv_ship_attribute7             := null;
        lv_ship_attribute8             := null;
        lv_ship_attribute9             := null;
        lv_ship_attribute10            := null;
        lv_ship_attribute11            := null;
        lv_ship_attribute12            := null;
        lv_ship_attribute13            := null;
        lv_ship_attribute14            := null;
        lv_ship_attribute15            := null;
        lv_l_org_id                    := null;
        lv_l_org_name                  := null;
        lv_l_terms_id                  := null;
        lv_l_pay_name                  := null;
        lv_tax_name                    := null;
        lv_dep_category_id             := null;
        l_linenum1                     := null;
        l_description                  := null;
        l_linenum2                     := null;
        xxetn_debug_pkg.add_debug('Starting processing for Purchase Order Line number :  ' ||
                                  cur_po_lines_rec.leg_line_num);
        --- Check line num
        if cur_po_lines_rec.leg_line_num is null then
          lv_line_err_flag := 'Y';
          l_err_code       := 'ETN_PO_MANDATORY_NOT_ENTERED';
          l_err_msg        := 'Error: Mandatory column not entered.  ';
          log_errors(pin_interface_txn_id    => cur_po_lines_rec.interface_txn_id,
                     piv_source_table        => 'xxpo_po_line_shipment_stg',
                     piv_source_column_name  => 'leg_line_num',
                     piv_source_column_value => cur_po_lines_rec.leg_line_num,
                     piv_source_keyname1     => null,
                     piv_source_keyvalue1    => null,
                     piv_error_type          => 'ERR_VAL',
                     piv_error_code          => l_err_code,
                     piv_error_message       => l_err_msg);
        end if;
        --- Check shipment num
        if cur_po_lines_rec.leg_shipment_num is null then
          lv_line_err_flag := 'Y';
          l_err_code       := 'ETN_PO_MANDATORY_NOT_ENTERED';
          l_err_msg        := 'Error: Mandatory column not entered.  ';
          log_errors(pin_interface_txn_id    => cur_po_lines_rec.interface_txn_id,
                     piv_source_table        => 'xxpo_po_line_shipment_stg',
                     piv_source_column_name  => 'leg_shipment_num',
                     piv_source_column_value => cur_po_lines_rec.leg_shipment_num,
                     piv_source_keyname1     => null,
                     piv_source_keyvalue1    => null,
                     piv_error_type          => 'ERR_VAL',
                     piv_error_code          => l_err_code,
                     piv_error_message       => l_err_msg);
        end if;
        --- check shipment type
        xxetn_debug_pkg.add_debug('Calling function xxpo_po_shipment_type for PO Line number :  ' ||
                                  cur_po_lines_rec.leg_line_num);
        if cur_po_lines_rec.leg_shipment_type is not null then
          lv_val_flag := 'N';
          lv_val_flag := xxpo_po_shipment_type(cur_po_lines_rec.interface_txn_id,
                                               'xxpo_po_line_shipment_stg',
                                               'leg_shipment_type',
                                               cur_po_lines_rec.leg_shipment_type);
          if lv_val_flag = 'Y' then
            lv_line_err_flag := 'Y';
          end if;
        end if;
        lv_line_num_new := cur_po_lines_rec.leg_line_num;
        l_description   := cur_po_lines_rec.leg_item_description;
        ---- GRNI change
        /*if cur_po_lines_rec.leg_unit_price <> ADB
                  nvl(cur_po_lines_rec.leg_list_price_per_unit, 0) and
                  cur_po_lines_rec.shipment_attribute6 is null then

        \* Commenting the below logic to avoid the Duplicate line number issue in interface  Mock 4

                 begin
                   select count(1)
                     into l_linenum1
                     from xxpo_po_line_shipment_stg
                    where leg_line_num = cur_po_lines_rec.leg_line_num
                      and leg_po_header_id = cur_po_lines_rec.leg_po_header_id
                      and leg_document_num = cur_po_lines_rec.leg_document_num
                      and shipment_attribute6 is null
                      and nvl(leg_unit_price, 0) <>
                          nvl(leg_list_price_per_unit, 0);
                 exception
                   when others then
                     lv_line_err_flag := 'Y';
                     l_err_code       := 'ETN_PO_LINE_CODE_NEW_1';
                     l_err_msg        := 'Error: While Getting new line number code - 1.  ';
                     log_errors(pin_interface_txn_id    => cur_po_lines_rec.interface_txn_id,
                                piv_source_table        => 'xxpo_po_line_shipment_stg',
                                piv_source_column_name  => 'leg_line_num',
                                piv_source_column_value => cur_po_lines_rec.leg_line_num,
                                piv_source_keyname1     => null,
                                piv_source_keyvalue1    => null,
                                piv_error_type          => 'ERR_VAL',
                                piv_error_code          => l_err_code,
                                piv_error_message       => l_err_msg);
                 end;

                 if remainder(l_linenum1, 10) = 0 then
                   begin
                     select count(1)
                       into l_linenum2
                       from xxpo_po_line_shipment_stg
                      where leg_po_line_id = cur_po_lines_rec.leg_po_line_id
                        and leg_po_header_id = cur_po_lines_rec.leg_po_header_id
                        and leg_document_num = cur_po_lines_rec.leg_document_num
                        and nvl(leg_unit_price, 0) <>
                            nvl(leg_list_price_per_unit, 0);
                   exception
                     when others then
                       lv_line_err_flag := 'Y';
                       l_err_code       := 'ETN_PO_LINE_CODE_NEW_2';
                       l_err_msg        := 'Error: While Getting new line number code - 2.  ';
                       log_errors(pin_interface_txn_id    => cur_po_lines_rec.interface_txn_id,
                                  piv_source_table        => 'xxpo_po_line_shipment_stg',
                                  piv_source_column_name  => 'leg_line_num',
                                  piv_source_column_value => cur_po_lines_rec.leg_line_num,
                                  piv_source_keyname1     => null,
                                  piv_source_keyvalue1    => null,
                                  piv_error_type          => 'ERR_VAL',
                                  piv_error_code          => l_err_code,
                                  piv_error_message       => l_err_msg);
                   end;
                   l_linenum1 := (l_linenum1 / 10) + l_linenum2;
                 end if;


                 xxpo_line_shipment_number(cur_po_lines_rec.interface_txn_id,
                                           'xxpo_po_line_shipment_stg',
                                           'LEG_LINE_NUMBER',
                                           cur_po_lines_rec.leg_po_header_id,
                                           cur_po_lines_rec.leg_po_line_id,
                                           cur_po_lines_rec.leg_document_num,
                                           cur_po_lines_rec.leg_line_num,
                                           cur_po_lines_rec.leg_shipment_num,
                                           cur_po_lines_rec.shipment_attribute6,
                                           l_linenum1,
                                           cur_po_lines_rec.leg_item_description,
                                           l_description,
                                           lv_line_num_new);
            End of commenting *\

             xxpo_line_num_Generator    (cur_po_lines_rec.interface_txn_id,
                                           'xxpo_po_line_shipment_stg',
                                           'LEG_LINE_NUMBER',
                                           cur_po_lines_rec.leg_po_header_id,
                                           cur_po_lines_rec.leg_document_num,
                                           cur_po_lines_rec.leg_line_num,
                                           cur_po_lines_rec.leg_shipment_num,
                                           cur_po_lines_rec.shipment_attribute6,
                                           cur_po_lines_rec.leg_item_description,
                         lv_line_num_new,
                                           l_description
                                           );


               end if;*/ --ADB
        ---- Updating Line number and DFFs
        --     begin
        /* --ADB
        lv_ship_attribute6 := nvl(cur_po_lines_rec.shipment_attribute6,
                                  cur_po_lines_rec.leg_line_num);
        lv_ship_attribute7 := cur_po_lines_rec.leg_shipment_num;
        lv_ship_attribute8 := 'PO';*/ --ADB
        /*      update XXPO_PO_LINE_SHIPMENT_STG
             set leg_line_num    = lv_line_num_new,
                 shipment_attribute6 = cur_po_lines_rec.leg_line_num,
                 shipment_attribute7 = cur_po_lines_rec.leg_shipment_num,
                 shipment_attribute8 = 'PO'
           where interface_txn_id = cur_po_lines_rec.interface_txn_id;
        exception
          when others then
            lv_line_err_flag := 'Y';
            l_err_code       := 'ETN_PO_ERROR_LINE_NUM_UPDATE';
            l_err_msg        := 'Error: Error while updating Line Number and DFFs. ';
            log_errors(pin_interface_txn_id    => cur_po_lines_rec.interface_txn_id,
                       piv_source_table        => 'xxpo_po_line_shipment_stg',
                       piv_source_column_name  => 'Line_Number',
                       piv_source_column_value => cur_po_lines_rec.leg_line_num,
                       piv_source_keyname1     => NULL,
                       piv_source_keyvalue1    => NULL,
                       piv_error_type          => 'ERR_VAL',
                       piv_error_code          => l_err_code,
                       piv_error_message       => l_err_msg);
        end;   */
        --- Validate line quantity
        if cur_po_lines_rec.leg_quantity is not null then
          if cur_po_lines_rec.leg_quantity = 0 then
            lv_line_err_flag := 'Y';
            l_err_code       := 'ETN_PO_INCORRECT_LINE_QUANTITY';
            l_err_msg        := 'Error: Shipment Line quantity cannot be Zero. ';
            log_errors(pin_interface_txn_id    => cur_po_lines_rec.interface_txn_id,
                       piv_source_table        => 'xxpo_po_line_shipment_stg',
                       piv_source_column_name  => 'leg_quantity',
                       piv_source_column_value => cur_po_lines_rec.leg_quantity,
                       piv_source_keyname1     => null,
                       piv_source_keyvalue1    => null,
                       piv_error_type          => 'ERR_VAL',
                       piv_error_code          => l_err_code,
                       piv_error_message       => l_err_msg);
          end if;
        else
          lv_line_err_flag := 'Y';
          l_err_code       := 'ETN_PO_MANDATORY_NOT_ENTERED';
          l_err_msg        := 'Error: Mandatory column not entered.  ';
          log_errors(pin_interface_txn_id    => cur_po_lines_rec.interface_txn_id,
                     piv_source_table        => 'xxpo_po_line_shipment_stg',
                     piv_source_column_name  => 'leg_quantity',
                     piv_source_column_value => cur_po_lines_rec.leg_quantity,
                     piv_source_keyname1     => null,
                     piv_source_keyvalue1    => null,
                     piv_error_type          => 'ERR_VAL',
                     piv_error_code          => l_err_code,
                     piv_error_message       => l_err_msg);
        end if;
        /* -- check price type                  --1.1
        xxetn_debug_pkg.add_debug
           (   'Calling function xxpo_po_price_type for PO Line number =  '
            || cur_po_lines_rec.leg_line_num
           );

        IF cur_po_lines_rec.leg_price_type IS NOT NULL
        THEN
           lv_val_flag := 'N';
           lv_val_flag :=
              xxpo_po_price_type (cur_po_lines_rec.interface_txn_id,
                                  'xxpo_po_line_shipment_stg',
                                  'leg_price_type',
                                  cur_po_lines_rec.leg_price_type
                                 );

           IF lv_val_flag = 'Y'
           THEN
              lv_line_err_flag := 'Y';
           END IF;
        END IF; */
        /* xxetn_debug_pkg.add_debug        --1.1
                      (   'Calling function xxpo_derive_org_id for PO line number =  '
                       || cur_po_lines_rec.leg_line_num
                      );

                   --- derive operating unit
                   IF cur_po_lines_rec.leg_operating_unit_name IS NOT NULL
                   THEN
                      xxpo_derive_org_id (cur_po_lines_rec.interface_txn_id,
                                          'xxpo_po_line_shipment_stg',
                                          'leg_operating_unit_name',
                                          cur_po_lines_rec.leg_operating_unit_name,
                                          lv_l_org_name,
                                          lv_l_org_id
                                         );

                      IF lv_l_org_id IS NULL
                      THEN
                         lv_line_err_flag := 'Y';
                      END IF;
                   ELSE
                      lv_line_err_flag := 'Y';
                      l_err_code := 'ETN_PO_MANDATORY_NOT_ENTERED';
                      l_err_msg := 'Error: Mandatory column not entered.  ';
                      log_errors
                         (pin_interface_txn_id         => cur_po_lines_rec.interface_txn_id,
                          piv_source_table             => 'xxpo_po_line_shipment_stg',
                          piv_source_column_name       => 'leg_operating_unit_name',
                          piv_source_column_value      => cur_po_lines_rec.leg_operating_unit_name,
                          piv_source_keyname1          => NULL,
                          piv_source_keyvalue1         => NULL,
                          piv_error_type               => 'ERR_VAL',
                          piv_error_code               => l_err_code,
                          piv_error_message            => l_err_msg
                         );
                   END IF;
        */
        --- Validate payment terms               --1.1
        /* xxetn_debug_pkg.add_debug
               (   'Calling function xxpo_terms_id for PO line number =  '
                || cur_po_lines_rec.leg_document_num
               );

        IF cur_po_lines_rec.leg_payment_terms IS NOT NULL
        THEN
           xxpo_terms_id (cur_po_lines_rec.interface_txn_id,
                          'xxpo_po_line_shipment_stg',
                          'leg_payment_terms',
                          cur_po_lines_rec.leg_payment_terms,
                          lv_l_pay_name,
                          lv_l_terms_id
                         );

           IF lv_l_terms_id IS NULL
           THEN
              lv_line_err_flag := 'Y';
           END IF;
        END IF; */
        ---Validate receiving routing
        if cur_po_lines_rec.leg_inspection_required_flag is not null and
           cur_po_lines_rec.leg_receiving_routing is not null then
          lv_val_flag := 'N';
          if (cur_po_lines_rec.leg_inspection_required_flag = 'Y') and
             (upper(cur_po_lines_rec.leg_receiving_routing) <>
             'INSPECTION REQUIRED') then
            lv_val_flag := 'Y';
          else
            lv_val_flag := 'N';
          end if;
          if lv_val_flag = 'Y' then
            lv_line_err_flag := 'Y';
            l_err_code       := 'ETN_PO_INCORRECT_RECEIVING_ROUTING';
            l_err_msg        := 'Error: Receiving routing should be Inspection Required when Inspection req flag is checked. ';
            log_errors(pin_interface_txn_id    => cur_po_lines_rec.interface_txn_id,
                       piv_source_table        => 'xxpo_po_line_shipment_stg',
                       piv_source_column_name  => 'leg_receiving_routing',
                       piv_source_column_value => cur_po_lines_rec.leg_receiving_routing,
                       piv_source_keyname1     => null,
                       piv_source_keyvalue1    => null,
                       piv_error_type          => 'ERR_VAL',
                       piv_error_code          => l_err_code,
                       piv_error_message       => l_err_msg);
          end if;
        end if;
        --1.20 Tax name validation
        if (cur_po_lines_rec.leg_tax_name is not null) /*and
                                                                                                        (cur_po_lines_rec.leg_taxable_flag is not null)*/
         then
          -- new_leg_tax_name := null; -- For Final Tax name output value from API
          p_out_tax_name1 := null; -- For output value from API
          p_out_tax_name2 := null; -- For output value from API
          p_in_tax_name   := cur_po_lines_rec.leg_tax_name;
          -- Legacy Tax name
          begin
            lv_out_value1  := null; --API Variables
            lv_out_value2  := null; --API Variables
            lv_out_value3  := null; --API Variables
            lv_err_message := null; --API Variables
            if (p_in_tax_name <> 'NO TAX') and -- SDP NO TAX ISSUE IN VALIDATION
               (cur_po_lines_rec.leg_req_dist_id is not null) then
              p_out_tax_name1 := p_in_tax_name;
              -- SDP NO TAX ISSUE IN VALIDATION COMMENT OUT INITIAL BEGIN
              /*   begin
                select distinct zrb.tax_rate_id
                  into lv_dep_tax_code_id
                  from zx_accounts            za,
                       hr_operating_units     hrou,
                       gl_ledgers             gl,
                       fnd_id_flex_structures fifs,
                       zx_rates_b             zrb,
                       zx_regimes_b           zb
                 where za.internal_organization_id = hrou.organization_id
                   and gl.ledger_id = za.ledger_id
                   and fifs.application_id =
                       (select fap.application_id
                          from fnd_application_vl fap
                         where fap.application_short_name = 'SQLGL')
                   and fifs.id_flex_code = 'GL#'
                   and fifs.id_flex_num = gl.chart_of_accounts_id
                   and zrb.tax_rate_id = za.tax_account_entity_id
                   and za.tax_account_entity_code = 'RATES'
                   and zrb.tax_rate_code = p_out_tax_name1
                   and hrou.organization_id = cur_po_lines_rec.org_id
                   and trunc(sysdate) between
                       trunc(nvl(zb.effective_from, sysdate)) and
                       trunc(nvl(zb.effective_to, sysdate))
                   and zrb.active_flag = 'Y';
              exception
                when others then
                  lv_dep_tax_code_id := null;
                  lv_line_err_flag   := 'Y';
                  l_err_code         := 'ETN_PO_TAX_NAME_CROSSREF';
                  l_err_msg          := 'Error: Tax Code not setup in the system';
                  log_errors(pin_interface_txn_id    => cur_po_lines_rec.interface_txn_id,
                             piv_source_table        => 'xxpo_po_line_shipment_stg',
                             piv_source_column_name  => 'tax_name',
                             piv_source_column_value => p_out_tax_name1,
                             piv_source_keyname1     => null,
                             piv_source_keyvalue1    => null,
                             piv_error_type          => 'ERR_VAL',
                             piv_error_code          => l_err_code,
                             piv_error_message       => l_err_msg);
              end;
              */
              -- else
              -- SDP NO TAX ISSUE IN VALIDATION COMMENT OUT INITIAL BEGIN AND REMOVE ELSE CLAUSE
              --
              for cur_plant_rec in cur_plant(cur_po_lines_rec.leg_po_header_id) loop
                begin
                  p_in_site_num := cur_plant_rec.leg_charge_account_seg1;
                  xxetn_cross_ref_pkg.get_value(piv_eaton_ledger => p_in_site_num,
                                                -- Site Number passed from cursor cur_plant
                                                piv_type => 'XXEBTAX_TAX_CODE_MAPPING',
                                                --Value Required ?
                                                piv_direction      => 'E', --value Required ?
                                                piv_application    => 'XXAP', --value Required ?
                                                piv_input_value1   => p_in_tax_name,
                                                piv_input_value2   => null, --value Required
                                                piv_input_value3   => null, --value Required
                                                pid_effective_date => sysdate,
                                                pov_output_value1  => lv_out_value1,
                                                pov_output_value2  => lv_out_value2,
                                                pov_output_value3  => lv_out_value3,
                                                pov_err_msg        => lv_err_message);
                  if lv_err_message is null then
                    p_out_tax_name1 := lv_out_value1;
                    --Incase new value is updated by API without Error Message
                  elsif lv_err_message is not null
                  -- (if error message received at Site Level go for Global Level without plant number)
                   then
                    lv_err_message  := null;
                    p_out_tax_name1 := null;
                    xxetn_cross_ref_pkg.get_value(piv_eaton_ledger => null,
                                                  -- Site Number passed from cursor cur_plant
                                                  piv_type => 'XXEBTAX_TAX_CODE_MAPPING',
                                                  --Value Required ?
                                                  piv_direction   => 'E', --value Required ?
                                                  piv_application => 'XXAP',
                                                  --value Required ?
                                                  piv_input_value1   => p_in_tax_name,
                                                  piv_input_value2   => null,
                                                  piv_input_value3   => null,
                                                  pid_effective_date => sysdate,
                                                  pov_output_value1  => lv_out_value1,
                                                  pov_output_value2  => lv_out_value2,
                                                  pov_output_value3  => lv_out_value3,
                                                  pov_err_msg        => lv_err_message);
                    if lv_err_message is null then
                      p_out_tax_name1 := lv_out_value1;
                      --New Value from API called at Global Level
                    elsif lv_err_message is not null then
                      p_out_tax_name1 := p_in_tax_name;
                      --Incase the API throws error message
                    end if;
                  end if;
                  begin
                    select distinct zrb.tax_rate_id
                      into lv_dep_tax_code_id
                      from zx_accounts            za,
                           hr_operating_units     hrou,
                           gl_ledgers             gl,
                           fnd_id_flex_structures fifs,
                           zx_rates_b             zrb,
                           zx_regimes_b           zb
                     where za.internal_organization_id =
                           hrou.organization_id
                       and gl.ledger_id = za.ledger_id
                       and fifs.application_id =
                           (select fap.application_id
                              from fnd_application_vl fap
                             where fap.application_short_name = 'SQLGL')
                       and fifs.id_flex_code = 'GL#'
                       and fifs.id_flex_num = gl.chart_of_accounts_id
                       and zrb.tax_rate_id = za.tax_account_entity_id
                       and za.tax_account_entity_code = 'RATES'
                       and zrb.tax_rate_code = p_out_tax_name1
                       and hrou.organization_id = cur_po_lines_rec.org_id
                       and trunc(sysdate) between
                           trunc(nvl(zb.effective_from, sysdate)) and
                           trunc(nvl(zb.effective_to, sysdate))
                       and zrb.active_flag = 'Y';
                  exception
                    when others then
                      lv_dep_tax_code_id := null;
                      lv_line_err_flag   := 'Y';
                      l_err_code         := 'ETN_PO_TAX_NAME_CROSSREF';
                      l_err_msg          := 'Error: Tax Code not setup in the system';
                      log_errors(pin_interface_txn_id    => cur_po_lines_rec.interface_txn_id,
                                 piv_source_table        => 'xxpo_po_line_shipment_stg',
                                 piv_source_column_name  => 'tax_name',
                                 piv_source_column_value => p_out_tax_name1,
                                 piv_source_keyname1     => null,
                                 piv_source_keyvalue1    => null,
                                 piv_error_type          => 'ERR_VAL',
                                 piv_error_code          => l_err_code,
                                 piv_error_message       => l_err_msg);
                  end;
                exception
                  when others then
                    p_out_tax_name1    := p_in_tax_name;
                    lv_dep_tax_code_id := null;
                    lv_line_err_flag   := 'Y';
                    l_err_code := 'ETN_PO_TAX_NAME_CROSSREF';
                    l_err_msg  := 'Error: Error Not Able TO Derive Tax Code From Cross Reference';
                    log_errors(pin_interface_txn_id    => cur_po_lines_rec.interface_txn_id,
                               piv_source_table        => 'xxpo_po_line_shipment_stg',
                               piv_source_column_name  => 'leg_tax_name',
                               piv_source_column_value => p_in_tax_name,
                               piv_source_keyname1     => null,
                               piv_source_keyvalue1    => null,
                               piv_error_type          => 'ERR_VAL',
                               piv_error_code          => l_err_code,
                               piv_error_message       => l_err_msg);
                end;
              end loop;
            end if;
          end;
        end if;
        --- Validate inspection required flag       --1.1
        /* xxetn_debug_pkg.add_debug
                      (   'Calling function xxpo_yes_no(inspection_required_flag) for PO Line number :  '
                       || cur_po_lines_rec.leg_line_num
                      );

                   IF cur_po_lines_rec.leg_inspection_required_flag IS NOT NULL
                   THEN
                      lv_val_flag := 'N';
                      lv_val_flag :=
                         xxpo_yes_no (cur_po_lines_rec.interface_txn_id,
                                      'xxpo_po_line_shipment_stg',
                                      'leg_inspection_required_flag',
                                      cur_po_lines_rec.leg_inspection_required_flag
                                     );

                      IF lv_val_flag = 'Y'
                      THEN
                         lv_line_err_flag := 'Y';
                      END IF;
                   END IF;
        */
        --- Validate receipt required flag           --1.1
        /* xxetn_debug_pkg.add_debug
           (   'Calling function xxpo_yes_no(leg_receipt_required_flag) for PO Line number :  '
            || cur_po_lines_rec.leg_line_num
           );

        IF cur_po_lines_rec.leg_receipt_required_flag IS NOT NULL
        THEN
           lv_val_flag := 'N';
           lv_val_flag :=
              xxpo_yes_no (cur_po_lines_rec.interface_txn_id,
                           'xxpo_po_line_shipment_stg',
                           'leg_receipt_required_flag',
                           cur_po_lines_rec.leg_receipt_required_flag
                          );

           IF lv_val_flag = 'Y'
           THEN
              lv_line_err_flag := 'Y';
           END IF;
        END IF; */
        --- Validate qty receiving tolerance
        xxetn_debug_pkg.add_debug('Calling function xxpo_payprior(QTY_RCV_TOLERANCE) for PO Line number :  ' ||
                                  cur_po_lines_rec.leg_line_num);
        if cur_po_lines_rec.leg_qty_rcv_tolerance is not null then
          lv_val_flag := 'N';
          lv_val_flag := xxpo_payprior(cur_po_lines_rec.interface_txn_id,
                                       'xxpo_po_line_shipment_stg',
                                       'leg_qty_rcv_tolerance',
                                       cur_po_lines_rec.leg_qty_rcv_tolerance);
          if lv_val_flag = 'Y' then
            lv_line_err_flag := 'Y';
          end if;
        end if;
        --- Validate invoice close tolerance
        xxetn_debug_pkg.add_debug('Calling function xxpo_payprior(INVOICE_CLOSE_TOLERANCE) for PO Line number :  ' ||
                                  cur_po_lines_rec.leg_line_num);
        if cur_po_lines_rec.leg_invoice_close_tolerance is not null then
          lv_val_flag := 'N';
          lv_val_flag := xxpo_payprior(cur_po_lines_rec.interface_txn_id,
                                       'xxpo_po_line_shipment_stg',
                                       'leg_invoice_close_tolerance',
                                       cur_po_lines_rec.leg_invoice_close_tolerance);
          if lv_val_flag = 'Y' then
            lv_line_err_flag := 'Y';
          end if;
        end if;
        --- Validate receive_close_tolerance
        xxetn_debug_pkg.add_debug('Calling function xxpo_payprior(receive_close_tolerance) for PO Line number :  ' ||
                                  cur_po_lines_rec.leg_line_num);
        if cur_po_lines_rec.leg_receive_close_tolerance is not null then
          lv_val_flag := 'N';
          lv_val_flag := xxpo_payprior(cur_po_lines_rec.interface_txn_id,
                                       'xxpo_po_line_shipment_stg',
                                       'leg_receive_close_tolerance',
                                       cur_po_lines_rec.leg_receive_close_tolerance);
          if lv_val_flag = 'Y' then
            lv_line_err_flag := 'Y';
          end if;
        end if;
        ---Validate receiving routing             --1.1
        /* xxetn_debug_pkg.add_debug
           (   'Calling function xxpo_recvrouteid_d for PO Line number =  '
            || cur_po_lines_rec.leg_line_num
           );

        IF cur_po_lines_rec.leg_receiving_routing IS NOT NULL
        THEN
           lv_dep_receiving_routing_id :=
              xxpo_recvrouteid (cur_po_lines_rec.interface_txn_id,
                                'xxpo_po_line_shipment_stg',
                                'leg_receiving_routing',
                                cur_po_lines_rec.leg_receiving_routing
                               );

           IF lv_dep_receiving_routing_id IS NULL
           THEN
              lv_line_err_flag := 'Y';
           END IF;
        END IF; */
        --- Validate line type        --1.1
        /* xxetn_debug_pkg.add_debug
           (   'Calling function xxpo_po_line_type_id for PO Line number =  '
            || cur_po_lines_rec.leg_line_num
           );

        IF cur_po_lines_rec.leg_line_type IS NOT NULL
        THEN
           lv_dep_line_type_id :=
              xxpo_po_line_type_id (cur_po_lines_rec.interface_txn_id,
                                    'xxpo_po_line_shipment_stg',
                                    'leg_line_type',
                                    cur_po_lines_rec.leg_line_type
                                   );

           IF lv_dep_line_type_id IS NULL
           THEN
              lv_line_err_flag := 'Y';
           END IF;
        END IF; */
        --- check unit of measure  --1.1
        /* xxetn_debug_pkg.add_debug
            (   'Calling function xxpo_po_uom_code for PO Line number =  '
             || cur_po_lines_rec.leg_line_num
            );

        IF cur_po_lines_rec.leg_unit_of_measure IS NOT NULL
        THEN
           lv_dep_uom_code :=
              xxpo_po_uom_code (cur_po_lines_rec.interface_txn_id,
                                'xxpo_po_line_shipment_stg',
                                'leg_unit_of_measure',
                                cur_po_lines_rec.leg_unit_of_measure
                               );

           IF lv_dep_uom_code IS NULL
           THEN
              lv_line_err_flag := 'Y';
           END IF;
        END IF; */
        --- Validate UN number    ---1.1
        /* xxetn_debug_pkg.add_debug
           (   'Calling function xxpo_po_un_number for PO Line number =  '
            || cur_po_lines_rec.leg_line_num
           );

        IF cur_po_lines_rec.leg_un_number IS NOT NULL
        THEN
           lv_dep_un_number_id :=
              xxpo_po_un_number (cur_po_lines_rec.interface_txn_id,
                                 'xxpo_po_line_shipment_stg',
                                 'leg_un_number',
                                 cur_po_lines_rec.leg_un_number
                                );

           IF lv_dep_un_number_id IS NULL
           THEN
              lv_line_err_flag := 'Y';
          END IF;
        END IF; */
        -- Validate hazard class               ---1.1
        /* xxetn_debug_pkg.add_debug
           (   'Calling function xxpo_po_hazard_class for PO Line number =  '
            || cur_po_lines_rec.leg_line_num
           );

        IF cur_po_lines_rec.leg_hazard_class IS NOT NULL
        THEN
           lv_dep_hazard_class_id :=
              xxpo_po_hazard_class (cur_po_lines_rec.interface_txn_id,
                                    'xxpo_po_line_shipment_stg',
                                    'leg_hazard_class',
                                    cur_po_lines_rec.leg_hazard_class
                                   );

           IF lv_dep_hazard_class_id IS NULL
           THEN
              lv_line_err_flag := 'Y';
           END IF;
        END IF; */
        ----Validate ship to organization name              --1.1
        /* xxetn_debug_pkg.add_debug
           (   'Calling function xxpo_po_ship_to_org_id for PO Line number =  '
            || cur_po_lines_rec.leg_line_num
           );

        IF cur_po_lines_rec.leg_ship_to_organization_name IS NOT NULL
        THEN
           lv_dep_ship_to_organization_id :=
              xxpo_po_ship_to_org_id
                          (cur_po_lines_rec.interface_txn_id,
                           'xxpo_po_line_shipment_stg',
                           'leg_ship_to_organization_name',
                           cur_po_lines_rec.leg_ship_to_organization_name
                          );

           IF lv_dep_ship_to_organization_id IS NULL
           THEN
              lv_line_err_flag := 'Y';
           END IF;
        ELSE
           lv_line_err_flag := 'Y';
           l_err_code := 'ETN_PO_MANDATORY_NOT_ENTERED';
           l_err_msg := 'Error: Mandatory column not entered.  ';
           log_errors
              (pin_interface_txn_id         => cur_po_lines_rec.interface_txn_id,
               piv_source_table             => 'xxpo_po_line_shipment_stg',
               piv_source_column_name       => 'leg_ship_to_organization_name',
               piv_source_column_value      => cur_po_lines_rec.leg_ship_to_organization_name,
               piv_source_keyname1          => NULL,
               piv_source_keyvalue1         => NULL,
               piv_error_type               => 'ERR_VAL',
               piv_error_code               => l_err_code,
               piv_error_message            => l_err_msg
              );
        END IF; */
        ----Validate Item NUMBER            ---1.1
        /* xxetn_debug_pkg.add_debug
             (   'Calling function xxpo_po_item_id for PO Line number =  '
              || cur_po_lines_rec.leg_line_num
             );

        IF     cur_po_lines_rec.leg_ship_to_organization_name IS NOT NULL
           AND cur_po_lines_rec.leg_item IS NOT NULL
        THEN
           lv_dep_item_id :=
              xxpo_po_item_id (cur_po_lines_rec.interface_txn_id,
                               'xxpo_po_line_shipment_stg',
                               'leg_item',
                               lv_dep_ship_to_organization_id,
                               cur_po_lines_rec.leg_item
                              );

           IF lv_dep_item_id IS NULL
           THEN
              lv_line_err_flag := 'Y';
           END IF;
        END IF; */
        --- validation for item revision
        xxetn_debug_pkg.add_debug('Calling function xxpo_po_item_revision for PO Line number =  ' ||
                                  cur_po_lines_rec.leg_line_num);
        if cur_po_lines_rec.ship_to_organization_id is not null ---1.1
           and cur_po_lines_rec.leg_item_revision is not null and
           cur_po_lines_rec.item_id is not null then
          lv_val_flag := 'N';
          lv_val_flag := xxpo_po_item_revision(cur_po_lines_rec.interface_txn_id,
                                               'xxpo_po_line_shipment_stg',
                                               'leg_item_revision',
                                               cur_po_lines_rec.ship_to_organization_id,
                                               cur_po_lines_rec.leg_item_revision,
                                               cur_po_lines_rec.item_id);
          if lv_val_flag = 'Y' then
            lv_line_err_flag := 'Y';
          end if;
        end if;
        ---To assign line DFF
        lv_l_attribute_category := cur_po_lines_rec.leg_line_attr_category_lines;
        lv_l_attribute1         := cur_po_lines_rec.leg_line_attribute1;
        lv_l_attribute2         := cur_po_lines_rec.leg_line_attribute2;
        lv_l_attribute3         := cur_po_lines_rec.leg_line_attribute3;
        lv_l_attribute4         := cur_po_lines_rec.leg_line_attribute4;
        lv_l_attribute5         := cur_po_lines_rec.leg_line_attribute5;
        lv_l_attribute6         := cur_po_lines_rec.leg_line_attribute6;
        lv_l_attribute7         := cur_po_lines_rec.leg_line_attribute7;
        lv_l_attribute8         := cur_po_lines_rec.leg_line_attribute8;
        lv_l_attribute9         := cur_po_lines_rec.leg_line_attribute9;
        lv_l_attribute10        := cur_po_lines_rec.leg_line_attribute10;
        lv_l_attribute11        := cur_po_lines_rec.leg_line_attribute11;
        lv_l_attribute12        := cur_po_lines_rec.leg_line_attribute12;
        lv_l_attribute13        := cur_po_lines_rec.leg_line_attribute13;
        lv_l_attribute14        := cur_po_lines_rec.leg_line_attribute14;
        lv_l_attribute15        := cur_po_lines_rec.leg_line_attribute15;
        --To assign shipment DFF
        lv_ship_attribute_category := cur_po_lines_rec.leg_shipment_attr_category;
        lv_ship_attribute1         := cur_po_lines_rec.leg_shipment_attribute1;
        lv_ship_attribute2         := cur_po_lines_rec.leg_shipment_attribute2;
        lv_ship_attribute3         := cur_po_lines_rec.leg_shipment_attribute3;
        lv_ship_attribute4         := cur_po_lines_rec.leg_shipment_attribute4;
        lv_ship_attribute5         := cur_po_lines_rec.leg_shipment_attribute5;
        -- lv_ship_attribute6         := cur_po_lines_rec.leg_shipment_attribute6;
        -- lv_ship_attribute7         := cur_po_lines_rec.leg_shipment_attribute7;
        -- lv_ship_attribute8         := cur_po_lines_rec.leg_shipment_attribute8;
        lv_ship_attribute9  := cur_po_lines_rec.leg_shipment_attribute9;
        lv_ship_attribute10 := cur_po_lines_rec.leg_shipment_attribute10;
        lv_ship_attribute11 := cur_po_lines_rec.leg_shipment_attribute11;
        lv_ship_attribute12 := cur_po_lines_rec.leg_shipment_attribute12;
        lv_ship_attribute13 := cur_po_lines_rec.leg_shipment_attribute13;
        lv_ship_attribute14 := cur_po_lines_rec.leg_shipment_attribute14;
        lv_ship_attribute15 := cur_po_lines_rec.leg_shipment_attribute15;
        --- Validate Ship to location             ---1.1
        /* xxetn_debug_pkg.add_debug
           (   'Calling function xxpo_ship_to_loc_id for PO Line number =  '
            || cur_po_lines_rec.leg_line_num
           );

        IF cur_po_lines_rec.leg_ship_to_location IS NOT NULL
        THEN
           lv_dep_ship_to_location_id :=
              xxpo_ship_to_loc_id (cur_po_lines_rec.interface_txn_id,
                                   'xxpo_po_line_shipment_stg',
                                   'leg_ship_to_location',
                                   cur_po_lines_rec.leg_ship_to_location
                                  );

           IF lv_dep_ship_to_location_id IS NULL
           THEN
              lv_line_err_flag := 'Y';
           END IF;
        ELSE
           lv_line_err_flag := 'Y';
           l_err_code := 'ETN_PO_MANDATORY_NOT_ENTERED';
           l_err_msg := 'Error: Mandatory column not entered.  ';
           log_errors
              (pin_interface_txn_id         => cur_po_lines_rec.interface_txn_id,
               piv_source_table             => 'xxpo_po_line_shipment_stg',
               piv_source_column_name       => 'leg_ship_to_location',
               piv_source_column_value      => cur_po_lines_rec.leg_ship_to_location,
               piv_source_keyname1          => NULL,
               piv_source_keyvalue1         => NULL,
               piv_error_type               => 'ERR_VAL',
               piv_error_code               => l_err_code,
               piv_error_message            => l_err_msg
              );
        END IF; */
        --- Validate item category                 ---1.1
        /* IF cur_po_lines_rec.leg_item IS NULL THEN
              xxetn_debug_pkg.add_debug (
                 'Calling function xxpo_po_item_cat_id for PO Line number =  ' ||
                 cur_po_lines_rec.leg_line_num
              );

              IF cur_po_lines_rec.leg_category_segment1 IS NOT NULL
              THEN
                 lv_dep_category_id      :=
                    xxpo_po_item_cat_id (
                       cur_po_lines_rec.interface_txn_id,
                       'xxpo_po_line_shipment_stg',
                       'leg_category_segment1',
                       cur_po_lines_rec.leg_category_segment1
                    );

                 IF lv_dep_category_id IS NULL
                 THEN
                    lv_line_err_flag   := 'Y';
                 END IF;
              END IF;
        END IF; */
        --- Validate leg_accrue_on_receipt_flag          ---1.1
        /* xxetn_debug_pkg.add_debug
           (   'Calling function xxpo_yes_no(leg_accrue_on_receipt_flag) for PO Line number :  '
            || cur_po_lines_rec.leg_line_num
           );

        IF cur_po_lines_rec.leg_accrue_on_receipt_flag IS NOT NULL
        THEN
           lv_val_flag := 'N';
           lv_val_flag :=
              xxpo_yes_no (cur_po_lines_rec.interface_txn_id,
                           'xxpo_po_line_shipment_stg',
                           'leg_accrue_on_receipt_flag',
                           cur_po_lines_rec.leg_accrue_on_receipt_flag
                          );

           IF lv_val_flag = 'Y'
           THEN
              lv_line_err_flag := 'Y';
           END IF;
        END IF; */
        --- Validate leg_drop_ship_flag         ----1.1
        /* xxetn_debug_pkg.add_debug
           (   'Calling function xxpo_yes_no(leg_drop_ship_flag) for PO Line number :  '
            || cur_po_lines_rec.leg_line_num
           );

        IF cur_po_lines_rec.leg_drop_ship_flag IS NOT NULL
        THEN
           lv_val_flag := 'N';
           lv_val_flag :=
              xxpo_yes_no (cur_po_lines_rec.interface_txn_id,
                           'xxpo_po_line_shipment_stg',
                           'leg_drop_ship_flag',
                           cur_po_lines_rec.leg_drop_ship_flag
                          );

           IF lv_val_flag = 'Y'
           THEN
              lv_line_err_flag := 'Y';
           END IF;
        END IF; */
        --- Validate taxable flag                 ---1.1
        /* xxetn_debug_pkg.add_debug
           (   'Calling function xxpo_yes_no(leg_taxable_flag) for PO Line number :  '
            || cur_po_lines_rec.leg_line_num
           );

        IF cur_po_lines_rec.leg_taxable_flag IS NOT NULL
        THEN
           lv_val_flag := 'N';
           lv_val_flag :=
              xxpo_yes_no (cur_po_lines_rec.interface_txn_id,
                           'xxpo_po_line_shipment_stg',
                           'leg_taxable_flag',
                           cur_po_lines_rec.leg_taxable_flag
                          );

           IF lv_val_flag = 'Y'
           THEN
              lv_line_err_flag := 'Y';
           END IF;
        END IF; */
        -- xxetn_debug_pkg.add_debug('Calling function xxpo_tax_code_id for PO Line number =  ' ||
        --     cur_po_lines_rec.leg_line_num);
        /* IF    lv_line_err_flag = 'Y'
            OR cur_po_lines_rec.process_flag = 'E'                    --1.1
         THEN
            lv_mast_line_err_flag := 'Y';
         END IF;
        */ ----distribution cursor starts
        l_tot_dist_qty := 0;
        for cur_po_distributions_rec in cur_po_distributions(cur_po_headers_rec.leg_po_header_id,
                                                             cur_po_headers_rec.leg_source_system,
                                                             cur_po_headers_rec.leg_operating_unit_name,
                                                             cur_po_lines_rec.leg_po_line_id,
                                                             cur_po_lines_rec.leg_po_line_location_id,
                                                             cur_po_lines_rec.leg_shipment_num) loop
          lv_d_line_err_flag            := 'N';
          lv_dest_org_id                := null;
          lv_dist_org_id                := null;
          lv_dist_org_name              := null;
          lv_d_dep_deliver_to_location  := null;
          lv_d_dep_deliver_to_person_id := null;
          lv_d_attribute_category       := null;
          lv_d_attribute1               := null;
          lv_d_attribute2               := null;
          lv_d_attribute3               := null;
          lv_d_attribute4               := null;
          lv_d_attribute5               := null;
          lv_d_attribute6               := null;
          lv_d_attribute7               := null;
          lv_d_attribute8               := null;
          lv_d_attribute9               := null;
          lv_d_attribute10              := null;
          lv_d_attribute11              := null;
          lv_d_attribute12              := null;
          lv_d_attribute13              := null;
          lv_d_attribute14              := null;
          lv_d_attribute15              := null;
          lv_d_dep_project_id           := null;
          lv_d_prj_type                 := null;
          lv_exp_org_id                 := null;
          lv_exp_org_name               := null;
          lv_set_of_books_id            := null;
          lv_d_dep_task_id              := null;
          xxetn_debug_pkg.add_debug('Starting processing for PO Distribution number =  ' ||
                                    cur_po_distributions_rec.leg_distribution_num);
          if cur_po_headers_rec.leg_currency_code = lv_func_currency_code then
            update xxpo_po_distribution_stg
               set leg_rate_date = null, leg_rate = null
             where interface_txn_id =
                   cur_po_distributions_rec.interface_txn_id;
          end if;
          commit;
          -- To check distribution num
          if cur_po_distributions_rec.leg_distribution_num is null then
            lv_d_line_err_flag := 'Y';
            l_err_code         := 'ETN_PO_MANDATORY_NOT_ENTERED';
            l_err_msg          := 'Error: Mandatory column not entered.  ';
            log_errors(pin_interface_txn_id    => cur_po_distributions_rec.interface_txn_id,
                       piv_source_table        => 'xxpo_po_distribution_stg',
                       piv_source_column_name  => 'leg_distribution_num',
                       piv_source_column_value => cur_po_distributions_rec.leg_distribution_num,
                       piv_source_keyname1     => null,
                       piv_source_keyvalue1    => null,
                       piv_error_type          => 'ERR_VAL',
                       piv_error_code          => l_err_code,
                       piv_error_message       => l_err_msg);
          end if;
          --- check quantity ordered
          if cur_po_distributions_rec.leg_quantity_ordered is not null then
            l_tot_dist_qty := l_tot_dist_qty +
                              cur_po_distributions_rec.leg_quantity_ordered;
            if cur_po_distributions_rec.leg_quantity_ordered = 0 then
              lv_d_line_err_flag := 'Y';
              l_err_code         := 'ETN_PO_INVALID_QUANTITY';
              l_err_msg          := 'Error: Quantity ordered value cannot be Zero.  ';
              log_errors(pin_interface_txn_id    => cur_po_distributions_rec.interface_txn_id,
                         piv_source_table        => 'xxpo_po_distribution_stg',
                         piv_source_column_name  => 'leg_quantity_ordered',
                         piv_source_column_value => cur_po_distributions_rec.leg_quantity_ordered,
                         piv_source_keyname1     => null,
                         piv_source_keyvalue1    => null,
                         piv_error_type          => 'ERR_VAL',
                         piv_error_code          => l_err_code,
                         piv_error_message       => l_err_msg);
            end if;
          else
            lv_d_line_err_flag := 'Y';
            l_err_code         := 'ETN_PO_MANDATORY_NOT_ENTERED';
            l_err_msg          := 'Error: Mandatory column not entered.  ';
            log_errors(pin_interface_txn_id    => cur_po_distributions_rec.interface_txn_id,
                       piv_source_table        => 'xxpo_po_distribution_stg',
                       piv_source_column_name  => 'leg_quantity_ordered',
                       piv_source_column_value => cur_po_distributions_rec.leg_quantity_ordered,
                       piv_source_keyname1     => null,
                       piv_source_keyvalue1    => null,
                       piv_error_type          => 'ERR_VAL',
                       piv_error_code          => l_err_code,
                       piv_error_message       => l_err_msg);
          end if;
          --- check leg_invoice_adjustment_flag         ---1.1
          /* xxetn_debug_pkg.add_debug
             (   'Calling function xxpo_yes_no(leg_invoice_adjustment_flag) for PO Distribution number :  '
              || cur_po_distributions_rec.leg_distribution_num
             );

          IF cur_po_distributions_rec.leg_invoice_adjustment_flag IS NOT NULL
          THEN
             lv_val_flag := 'N';
             lv_val_flag :=
                xxpo_yes_no
                   (cur_po_distributions_rec.interface_txn_id,
                    'xxpo_po_distribution_stg',
                    'leg_invoice_adjustment_flag',
                    cur_po_distributions_rec.leg_invoice_adjustment_flag
                   );

             IF lv_val_flag = 'Y'
             THEN
                lv_d_line_err_flag := 'Y';
             END IF;
          END IF; */
          --- check destination type code
          xxetn_debug_pkg.add_debug('Calling function xxpo_po_dest_type for PO Distribution number =  ' ||
                                    cur_po_distributions_rec.leg_distribution_num);
          if cur_po_distributions_rec.leg_destination_type_code is not null then
            lv_val_flag := 'N';
            lv_val_flag := xxpo_po_dest_type(cur_po_distributions_rec.interface_txn_id,
                                             'xxpo_po_distribution_stg',
                                             'leg_destination_type_code',
                                             cur_po_distributions_rec.leg_destination_type_code);
            if lv_val_flag = 'Y' then
              lv_d_line_err_flag := 'Y';
            end if;
          end if;
          --- derive operating unit            ---1.1
          /* IF cur_po_distributions_rec.leg_operating_unit_name IS NOT NULL
          THEN
             xxpo_derive_org_id
                      (cur_po_distributions_rec.interface_txn_id,
                       'xxpo_po_distribution_stg',
                       'leg_operating_unit_name',
                       cur_po_distributions_rec.leg_operating_unit_name,
                       lv_dist_org_name,
                       lv_dist_org_id
                      );

             IF lv_dist_org_id IS NULL
             THEN
                lv_d_line_err_flag := 'Y';
             ELSE
                --derive set of books id
                BEGIN
                   SELECT set_of_books_id
                     INTO lv_set_of_books_id
                     FROM hr_operating_units
                    WHERE organization_id = lv_dist_org_id;
                EXCEPTION
                   WHEN OTHERS
                   THEN
                      lv_d_line_err_flag := 'Y';
                      l_err_code := 'ETN_PO_INVALID_SET_OF_BOOKS';
                      l_err_msg :=
                         'Error: Couldnt derive set_of_books_id value.  ';
                      log_errors
                         (pin_interface_txn_id         => cur_po_distributions_rec.interface_txn_id,
                          piv_source_table             => 'xxpo_po_distribution_stg',
                          piv_source_column_name       => 'set_of_books_id',
                          piv_source_column_value      => cur_po_distributions_rec.set_of_books_id,
                          piv_source_keyname1          => NULL,
                          piv_source_keyvalue1         => NULL,
                          piv_error_type               => 'ERR_VAL',
                          piv_error_code               => l_err_code,
                          piv_error_message            => l_err_msg
                         );
                END;
             END IF;
          ELSE
             lv_d_line_err_flag := 'Y';
             l_err_code := 'ETN_PO_MANDATORY_NOT_ENTERED';
             l_err_msg := 'Error: Mandatory column not entered.  ';
             log_errors
                (pin_interface_txn_id         => cur_po_distributions_rec.interface_txn_id,
                 piv_source_table             => 'xxpo_po_distribution_stg',
                 piv_source_column_name       => 'leg_operating_unit_name',
                 piv_source_column_value      => cur_po_distributions_rec.leg_operating_unit_name,
                 piv_source_keyname1          => NULL,
                 piv_source_keyvalue1         => NULL,
                 piv_error_type               => 'ERR_VAL',
                 piv_error_code               => l_err_code,
                 piv_error_message            => l_err_msg
                );
          END IF; */
          --- check destination organization          --1.1
          /* xxetn_debug_pkg.add_debug
             (   'Calling function xxpo_po_dest_org_id for PO Distribution number =  '
              || cur_po_distributions_rec.leg_distribution_num
             );

          IF cur_po_distributions_rec.leg_destination_organization IS NOT NULL
          THEN
             lv_dest_org_id :=
                xxpo_po_dest_org_id
                   (cur_po_distributions_rec.interface_txn_id,
                    'xxpo_po_distribution_stg',
                    'leg_destination_organization',
                    cur_po_distributions_rec.leg_destination_organization
                   );

             IF lv_dest_org_id IS NULL
             THEN
                lv_d_line_err_flag := 'Y';
             END IF;
          END IF; */
          --- check sub inventory         --1.1
          /* xxetn_debug_pkg.add_debug
             (   'Calling function xxpo_po_dest_subinlv for PO Distribution number =  '
              || cur_po_distributions_rec.leg_distribution_num
             );

          IF     cur_po_distributions_rec.leg_operating_unit_name IS NOT NULL
             AND cur_po_distributions_rec.leg_destination_subinventory IS NOT NULL
          THEN
             lv_val_flag := 'N';
             lv_val_flag :=
                xxpo_po_dest_subinlv
                   (cur_po_distributions_rec.interface_txn_id,
                    'xxpo_po_distribution_stg',
                    'leg_destination_subinventory',
                    lv_dest_org_id,
                    cur_po_distributions_rec.leg_destination_subinventory
                   );

             IF lv_val_flag = 'Y'
             THEN
                lv_d_line_err_flag := 'Y';
             END IF;
          END IF; */
          -- to check deliver to location
          xxetn_debug_pkg.add_debug('Calling function xxpo_delv_to_loc_id for PO Distribution number =  ' ||
                                    cur_po_distributions_rec.leg_distribution_num);
          if cur_po_distributions_rec.leg_deliver_to_location is not null then
            lv_d_dep_deliver_to_location := xxpo_delv_to_loc_id(cur_po_distributions_rec.interface_txn_id,
                                                                'xxpo_po_distribution_stg',
                                                                'leg_deliver_to_location',
                                                                cur_po_distributions_rec.leg_deliver_to_location);
            if lv_d_dep_deliver_to_location is null then
              lv_d_line_err_flag := 'Y';
            end if;
          end if;
          --- Derive deliver to person emp number
          xxetn_debug_pkg.add_debug('Calling function xxpo_po_del_per_id for PO Distribution number =  ' ||
                                    cur_po_distributions_rec.leg_distribution_num);
          if cur_po_distributions_rec.leg_deliver_to_person_emp_no
            --cur_po_distributions_rec.deliver_to_person_full_name
             is not null then
            lv_d_dep_deliver_to_person_id := xxpo_po_del_per_id(cur_po_distributions_rec.interface_txn_id, ----v1.14
                                                                'xxpo_po_distribution_stg',
                                                                'leg_deliver_to_person_emp_no',
                                                                cur_po_distributions_rec.leg_deliver_to_person_emp_no,
                                                                cur_po_distributions_rec.leg_charge_account_seg1); ---------v1.14
            if lv_d_dep_deliver_to_person_id is null then
              lv_d_line_err_flag := 'Y';
            end if;
          end if;
          --1.1
          /*  --- Validate accrual account
           xxetn_debug_pkg.add_debug
              (   'Calling procedure validate accounts(accrual account) for PO Distribution number =  '
               || cur_po_distributions_rec.leg_distribution_num
              );
           x_accrual_ccid := NULL;
           x_out_accrual_acc_rec := NULL;
           validate_accounts
                       (cur_po_distributions_rec.interface_txn_id,
                        'xxpo_po_distribution_stg',
                        'leg_accural_account_seg1',
                        cur_po_distributions_rec.leg_accural_account_seg1,
                        cur_po_distributions_rec.leg_accural_account_seg2,
                        cur_po_distributions_rec.leg_accural_account_seg3,
                        cur_po_distributions_rec.leg_accural_account_seg4,
                        cur_po_distributions_rec.leg_accural_account_seg5,
                        cur_po_distributions_rec.leg_accural_account_seg6,
                        cur_po_distributions_rec.leg_accural_account_seg7,
                        x_out_accrual_acc_rec,
                        x_accrual_ccid
                       );

           IF x_accrual_ccid IS NULL
           THEN
              lv_d_line_err_flag := 'Y';
           END IF;

           --- Validate charge account
           xxetn_debug_pkg.add_debug
              (   'Calling procedure validate accounts(charge account) for PO Distribution number =  '
               || cur_po_distributions_rec.leg_distribution_num
              );
          x_charge_ccid := NULL;
           x_out_charge_acc_rec := NULL;
           validate_accounts
                        (cur_po_distributions_rec.interface_txn_id,
                         'xxpo_po_distribution_stg',
                         'leg_charge_account_seg1',
                         cur_po_distributions_rec.leg_charge_account_seg1,
                         cur_po_distributions_rec.leg_charge_account_seg2,
                         cur_po_distributions_rec.leg_charge_account_seg3,
                         cur_po_distributions_rec.leg_charge_account_seg4,
                         cur_po_distributions_rec.leg_charge_account_seg5,
                         cur_po_distributions_rec.leg_charge_account_seg6,
                         cur_po_distributions_rec.leg_charge_account_seg7,
                         x_out_charge_acc_rec,
                         x_charge_ccid
                        );

           IF x_charge_ccid IS NULL
           THEN
              lv_d_line_err_flag := 'Y';
           END IF;

           --- Validate variance account
           xxetn_debug_pkg.add_debug
              (   'Calling procedure validate accounts(variance account) for PO Distribution number =  '
               || cur_po_distributions_rec.leg_distribution_num
              );
           x_variance_ccid := NULL;
           x_out_variance_acc_rec := NULL;
           validate_accounts
                      (cur_po_distributions_rec.interface_txn_id,
                       'xxpo_po_distribution_stg',
                       'leg_variance_account_seg1',
                       cur_po_distributions_rec.leg_variance_account_seg1,
                       cur_po_distributions_rec.leg_variance_account_seg2,
                       cur_po_distributions_rec.leg_variance_account_seg3,
                       cur_po_distributions_rec.leg_variance_account_seg4,
                       cur_po_distributions_rec.leg_variance_account_seg5,
                       cur_po_distributions_rec.leg_variance_account_seg6,
                       cur_po_distributions_rec.leg_variance_account_seg7,
                       x_out_variance_acc_rec,
                       x_variance_ccid
                      );

           IF x_variance_ccid IS NULL
           THEN
              lv_d_line_err_flag := 'Y';
           END IF; */
          --- check leg_accrued_flag            --1.1
          /* xxetn_debug_pkg.add_debug
             (   'Calling function xxpo_yes_no(leg_accrued_flag) for PO Distribution number :  '
              || cur_po_distributions_rec.leg_distribution_num
             );

          IF cur_po_distributions_rec.leg_accrued_flag IS NOT NULL
          THEN
             lv_val_flag := 'N';
             lv_val_flag :=
                xxpo_yes_no (cur_po_distributions_rec.interface_txn_id,
                             'xxpo_po_distribution_stg',
                             'leg_accrued_flag',
                             cur_po_distributions_rec.leg_accrued_flag
                            );

             IF lv_val_flag = 'Y'
             THEN
                lv_d_line_err_flag := 'Y';
             END IF;
          END IF; */
          --- check leg_accrue_on_receipt_flag       --- 1.1
          /* xxetn_debug_pkg.add_debug
             (   'calling function xxpo_yes_no(leg_accrue_on_receipt_flag) for po distribution number :  '
              || cur_po_distributions_rec.leg_distribution_num
             );

          if cur_po_distributions_rec.leg_accrue_on_receipt_flag is not null
          then
             lv_val_flag := 'N';
             lv_val_flag :=
                xxpo_yes_no
                    (cur_po_distributions_rec.interface_txn_id,
                     'xxpo_po_distribution_stg',
                     'leg_accrue_on_receipt_flag',
                     cur_po_distributions_rec.leg_accrue_on_receipt_flag
                    );

             if lv_val_flag = 'Y'
             then
                lv_d_line_err_flag := 'Y';
             end if;
          end if; */
          xxetn_debug_pkg.add_debug('Calling function xxpo_po_acc_rec_flag for PO Distribution number =  ' ||
                                    cur_po_distributions_rec.leg_distribution_num);
          if cur_po_distributions_rec.leg_destination_type_code is not null and
             cur_po_distributions_rec.leg_accrue_on_receipt_flag is not null then
            lv_val_flag := 'N';
            lv_val_flag := xxpo_po_acc_rec_flag(cur_po_distributions_rec.interface_txn_id,
                                                'xxpo_po_distribution_stg',
                                                'leg_accrue_on_receipt_flag',
                                                cur_po_distributions_rec.leg_destination_type_code,
                                                cur_po_distributions_rec.leg_accrue_on_receipt_flag);
            if lv_val_flag = 'Y' then
              lv_d_line_err_flag := 'Y';
            end if;
          end if;
          --- Assigning DFF
          lv_d_attribute_category := cur_po_distributions_rec.leg_attribute_category;
          lv_d_attribute1         := cur_po_distributions_rec.leg_attribute1;
          lv_d_attribute2         := cur_po_distributions_rec.leg_attribute2;
          lv_d_attribute3         := cur_po_distributions_rec.leg_attribute3;
          lv_d_attribute4         := cur_po_distributions_rec.leg_attribute4;
          lv_d_attribute5         := cur_po_distributions_rec.leg_attribute5;
          lv_d_attribute6         := cur_po_distributions_rec.leg_attribute6;
          lv_d_attribute7         := cur_po_distributions_rec.leg_attribute7;
          lv_d_attribute8         := cur_po_distributions_rec.leg_attribute8;
          lv_d_attribute9         := cur_po_distributions_rec.leg_attribute9;
          lv_d_attribute10        := cur_po_distributions_rec.leg_attribute10;
          lv_d_attribute11        := cur_po_distributions_rec.leg_attribute11;
          lv_d_attribute12        := cur_po_distributions_rec.leg_attribute12;
          lv_d_attribute13        := cur_po_distributions_rec.leg_attribute13;
          lv_d_attribute14        := cur_po_distributions_rec.leg_attribute14;
          lv_d_attribute15        := cur_po_distributions_rec.leg_attribute15;
          --- check leg_project_accounting_context      --1.1
          /* xxetn_debug_pkg.add_debug
             (   'Calling function xxpo_yes_no(leg_project_accounting_context) for PO Distribution number :  '
              || cur_po_distributions_rec.leg_distribution_num
             );

          IF cur_po_distributions_rec.leg_project_accounting_context IS NOT NULL
          THEN
             lv_val_flag := 'N';
             lv_val_flag :=
                xxpo_yes_no
                   (cur_po_distributions_rec.interface_txn_id,
                    'xxpo_po_distribution_stg',
                    'leg_project_accounting_context',
                    cur_po_distributions_rec.leg_project_accounting_context
                   );

             IF lv_val_flag = 'Y'
             THEN
                lv_d_line_err_flag := 'Y';
             END IF;
          END IF; */
          --               IF     cur_po_distributions_rec.leg_project_accounting_context <>
          --                                                                           'Y'
          if cur_po_distributions_rec.leg_project_accounting_context is null and
             cur_po_distributions_rec.leg_project is not null then
            lv_d_line_err_flag := 'Y';
            l_err_code         := 'ETN_PO_INVALID_PRJ_ACC_CONTEXT';
            l_err_msg          := 'Error: Project Accounting flag should be set when project infor is passed.  ';
            log_errors(pin_interface_txn_id    => cur_po_distributions_rec.interface_txn_id,
                       piv_source_table        => 'xxpo_po_distribution_stg',
                       piv_source_column_name  => 'leg_project_accounting_context',
                       piv_source_column_value => cur_po_distributions_rec.leg_project_accounting_context,
                       piv_source_keyname1     => null,
                       piv_source_keyvalue1    => null,
                       piv_error_type          => 'ERR_VAL',
                       piv_error_code          => l_err_code,
                       piv_error_message       => l_err_msg);
          end if;
          --Validate project name
          xxetn_debug_pkg.add_debug('Calling procedure xxpo_po_project_id for PO Distribution number =  ' ||
                                    cur_po_distributions_rec.leg_distribution_num);
          if cur_po_distributions_rec.leg_project is not null
          /* AND cur_po_distributions_rec.leg_project_accounting_context =
                                                                                                                                                                                                                                                                                                                                                                                                                                                 'Y'*/ -- Commented by Abhijit
           then
            xxpo_po_project_id(cur_po_distributions_rec.interface_txn_id,
                               'xxpo_po_distribution_stg',
                               'leg_project',
                               cur_po_distributions_rec.leg_project,
                               cur_po_distributions_rec.leg_expenditure_item_date,
                               lv_d_dep_project_id,
                               lv_d_prj_type);
            if lv_d_dep_project_id is null then
              lv_d_line_err_flag := 'Y';
            end if;
            xxetn_debug_pkg.add_debug('Calling function xxpo_po_task_id for PO Distribution number =  ' ||
                                      cur_po_distributions_rec.leg_distribution_num);
            -- Validate project task
            lv_d_dep_task_id := xxpo_po_task_id(cur_po_distributions_rec.interface_txn_id,
                                                'xxpo_po_distribution_stg',
                                                'leg_project',
                                                cur_po_distributions_rec.leg_project,
                                                lv_d_prj_type,
                                                cur_po_distributions_rec.leg_expenditure_item_date);
            if lv_d_dep_task_id is null then
              lv_d_line_err_flag := 'Y';
            end if;
            xxetn_debug_pkg.add_debug('Calling function xxpo_po_exp_type for PO Distribution number =  ' ||
                                      cur_po_distributions_rec.leg_distribution_num);
            --Validate expenditure type
            if cur_po_distributions_rec.leg_expenditure_type is not null then
              lv_val_flag := 'N';
              lv_val_flag := xxpo_po_exp_type(cur_po_distributions_rec.interface_txn_id,
                                              'xxpo_po_distribution_stg',
                                              'leg_expenditure_type',
                                              cur_po_distributions_rec.leg_expenditure_type --- Changed for Expenditure Org and Expenditure type Logic
                                              );
              if lv_val_flag = 'Y' then
                lv_d_line_err_flag := 'Y';
              end if;
            end if;
            --Validate expenditure date
            if cur_po_distributions_rec.leg_expenditure_item_date is null then
              lv_d_line_err_flag := 'Y';
              l_err_code         := 'ETN_PO_INVALID_EXP_DATE';
              l_err_msg          := 'Error: Expenditure date cannot be NULL when project info is entered.  ';
              log_errors(pin_interface_txn_id    => cur_po_distributions_rec.interface_txn_id,
                         piv_source_table        => 'xxpo_po_distribution_stg',
                         piv_source_column_name  => 'leg_expenditure_item_date',
                         piv_source_column_value => cur_po_distributions_rec.leg_expenditure_item_date,
                         piv_source_keyname1     => null,
                         piv_source_keyvalue1    => null,
                         piv_error_type          => 'ERR_VAL',
                         piv_error_code          => l_err_code,
                         piv_error_message       => l_err_msg);
            end if;
            --- derive expenditure org
            begin
              select hou.organization_id
                into lv_exp_org_id
                from apps.hr_all_organization_units hou
               where hou.date_to is null -- Change for Expenditure Org and Expenditure type Logic in PO
                 and (substr(hou.name, 1, 7)) =
                     (substr(cur_po_distributions_rec.leg_expenditure_organization,
                             1,
                             7)); -- Change for Expenditure Org and Expenditure type Logic in PO
            exception
              when others then
                lv_d_line_err_flag := 'Y';
                l_err_code         := 'ETN_PO_INVALID_EXP_ORG';
                l_err_msg          := 'Error: Expenditure org not defined in the system.  ';
                log_errors(pin_interface_txn_id    => cur_po_distributions_rec.interface_txn_id,
                           piv_source_table        => 'xxpo_po_distribution_stg',
                           piv_source_column_name  => 'leg_expenditure_organization',
                           piv_source_column_value => cur_po_distributions_rec.leg_expenditure_organization,
                           piv_source_keyname1     => null,
                           piv_source_keyvalue1    => null,
                           piv_error_type          => 'ERR_VAL',
                           piv_error_code          => l_err_code,
                           piv_error_message       => l_err_msg);
            end;
          end if;
          if lv_d_line_err_flag = 'Y' or
             cur_po_distributions_rec.process_flag = 'E' --1.1
           then
            lv_mast_d_line_err_flag := 'Y';
          end if;
          xxetn_debug_pkg.add_debug('BEFORE UPDATING xxpo_po_distribution_stg' ||
                                    sysdate);
          if (lv_d_line_err_flag <> 'Y') and
             (cur_po_distributions_rec.process_flag <> 'E') --1.1
           then
            begin
              update xxpo_po_distribution_stg
                 set process_flag = 'V',
                     --operating_unit_name = lv_dist_org_name,
                     --org_id = lv_dist_org_id,
                     deliver_to_location_id = lv_d_dep_deliver_to_location,
                     deliver_to_person_id   = lv_d_dep_deliver_to_person_id,
                     --destination_organization_id = lv_dest_org_id,
                     ---set_of_books_id = lv_set_of_books_id,
                     --charge_account_seg1 =
                     --                     x_out_charge_acc_rec.segment1,
                     --charge_account_seg2 =
                     --                     x_out_charge_acc_rec.segment2,
                     --  charge_account_seg3 =
                     --                      x_out_charge_acc_rec.segment3,
                     --  charge_account_seg4 =
                     --                     x_out_charge_acc_rec.segment4,
                     -- charge_account_seg5 =
                     --                      x_out_charge_acc_rec.segment5,
                     -- charge_account_seg6 =
                     --                       x_out_charge_acc_rec.segment6,
                     -- charge_account_seg7 =
                     --                      x_out_charge_acc_rec.segment7,
                     -- charge_account_seg8 =
                     --                      x_out_charge_acc_rec.segment8,
                     -- charge_account_seg9 =
                     --                      x_out_charge_acc_rec.segment9,
                     -- charge_account_seg10 =
                     --                     x_out_charge_acc_rec.segment10,
                     -- charge_account_ccid = x_charge_ccid,
                     --  accural_account_seg1 =
                     --                     x_out_accrual_acc_rec.segment1,
                     --  accural_account_seg2 =
                     --                      x_out_accrual_acc_rec.segment2,
                     -- accural_account_seg3 =
                     --                     x_out_accrual_acc_rec.segment3,
                     -- accural_account_seg4 =
                     --                     x_out_accrual_acc_rec.segment4,
                     -- accural_account_seg5 =
                     --                    x_out_accrual_acc_rec.segment5,
                     -- accural_account_seg6 =
                     --                    x_out_accrual_acc_rec.segment6,
                     --  accural_account_seg7 =
                     --                      x_out_accrual_acc_rec.segment7,
                     --  accural_account_seg8 =
                     --                      x_out_accrual_acc_rec.segment8,
                     --  accural_account_seg9 =
                     --                      x_out_accrual_acc_rec.segment9,
                     --  accural_account_seg10 =
                     --                     x_out_accrual_acc_rec.segment10,
                     --  accural_account_ccid = x_accrual_ccid,
                     --  variance_account_seg1 =
                     --                    x_out_variance_acc_rec.segment1,
                     --  variance_account_seg2 =
                     --                     x_out_variance_acc_rec.segment2,
                     --  variance_account_seg3 =
                     --                     x_out_variance_acc_rec.segment3,
                     --  variance_account_seg4 =
                     --                     x_out_variance_acc_rec.segment4,
                     --  variance_account_seg5 =
                     --                     x_out_variance_acc_rec.segment5,
                     --  variance_account_seg6 =
                     --                    x_out_variance_acc_rec.segment6,
                     --  variance_account_seg7 =
                     --                      x_out_variance_acc_rec.segment7,
                     --  variance_account_seg8 =
                     --                     x_out_variance_acc_rec.segment8,
                     --  variance_account_seg9 =
                     --                     x_out_variance_acc_rec.segment9,
                     --  variance_account_seg10 =
                     --                    x_out_variance_acc_rec.segment10,
                     --  variance_account_ccid = x_variance_ccid,
                     project_id                  = lv_d_dep_project_id,
                     task_id                     = lv_d_dep_task_id,
                     expenditure_organization_id = lv_exp_org_id,
                     attribute_category          = lv_d_attribute_category,
                     attribute1                  = lv_d_attribute1,
                     attribute2                  = lv_d_attribute2,
                     attribute3                  = lv_d_attribute3,
                     attribute4                  = lv_d_attribute4,
                     attribute5                  = lv_d_attribute5,
                     attribute6                  = lv_d_attribute6,
                     attribute7                  = lv_d_attribute7,
                     attribute8                  = lv_d_attribute7,
                     attribute9                  = lv_d_attribute9,
                     attribute10                 = lv_d_attribute10,
                     attribute11                 = lv_d_attribute11,
                     attribute12                 = lv_d_attribute12,
                     attribute13                 = lv_d_attribute13,
                     attribute14                 = lv_d_attribute14,
                     attribute15                 = lv_d_attribute15,
                     request_id                  = g_request_id,
                     last_updated_date           = sysdate,
                     last_updated_by             = g_last_updated_by,
                     last_update_login           = g_last_update_login
               where interface_txn_id =
                     cur_po_distributions_rec.interface_txn_id;
            exception
              when others then
                print_log_message('Error while updating distribution table for process flag V : ' ||
                                  sqlerrm);
                g_retcode := 1;
            end;
          else
            g_retcode := 1;
            begin
              update xxpo_po_distribution_stg
                 set process_flag = 'E',
                     error_type   = 'ERR_VAL',
                     --operating_unit_name = lv_dist_org_name,
                     --org_id = lv_dist_org_id,
                     deliver_to_location_id = lv_d_dep_deliver_to_location,
                     deliver_to_person_id   = lv_d_dep_deliver_to_person_id,
                     --destination_organization_id = lv_dest_org_id,
                     -- set_of_books_id = lv_set_of_books_id,
                     --charge_account_seg1 =
                     --                     x_out_charge_acc_rec.segment1,
                     --charge_account_seg2 =
                     --                     x_out_charge_acc_rec.segment2,
                     --  charge_account_seg3 =
                     --                      x_out_charge_acc_rec.segment3,
                     --  charge_account_seg4 =
                     --                     x_out_charge_acc_rec.segment4,
                     -- charge_account_seg5 =
                     --                      x_out_charge_acc_rec.segment5,
                     -- charge_account_seg6 =
                     --                       x_out_charge_acc_rec.segment6,
                     -- charge_account_seg7 =
                     --                      x_out_charge_acc_rec.segment7,
                     -- charge_account_seg8 =
                     --                      x_out_charge_acc_rec.segment8,
                     -- charge_account_seg9 =
                     --                      x_out_charge_acc_rec.segment9,
                     -- charge_account_seg10 =
                     --                     x_out_charge_acc_rec.segment10,
                     -- charge_account_ccid = x_charge_ccid,
                     --  accural_account_seg1 =
                     --                     x_out_accrual_acc_rec.segment1,
                     --  accural_account_seg2 =
                     --                      x_out_accrual_acc_rec.segment2,
                     -- accural_account_seg3 =
                     --                     x_out_accrual_acc_rec.segment3,
                     -- accural_account_seg4 =
                     --                     x_out_accrual_acc_rec.segment4,
                     -- accural_account_seg5 =
                     --                     x_out_accrual_acc_rec.segment5,
                     -- accural_account_seg6 =
                     --                    x_out_accrual_acc_rec.segment6,
                     --  accural_account_seg7 =
                     --                      x_out_accrual_acc_rec.segment7,
                     --  accural_account_seg8 =
                     --                      x_out_accrual_acc_rec.segment8,
                     --  accural_account_seg9 =
                     --                      x_out_accrual_acc_rec.segment9,
                     --  accural_account_seg10 =
                     --                     x_out_accrual_acc_rec.segment10,
                     --  accural_account_ccid = x_accrual_ccid,
                     --  variance_account_seg1 =
                     --                    x_out_variance_acc_rec.segment1,
                     --  variance_account_seg2 =
                     --                     x_out_variance_acc_rec.segment2,
                     --  variance_account_seg3 =
                     --                     x_out_variance_acc_rec.segment3,
                     --  variance_account_seg4 =
                     --                     x_out_variance_acc_rec.segment4,
                     --  variance_account_seg5 =
                     --                     x_out_variance_acc_rec.segment5,
                     --  variance_account_seg6 =
                     --                    x_out_variance_acc_rec.segment6,
                     --  variance_account_seg7 =
                     --                      x_out_variance_acc_rec.segment7,
                     --  variance_account_seg8 =
                     --                     x_out_variance_acc_rec.segment8,
                     --  variance_account_seg9 =
                     --                     x_out_variance_acc_rec.segment9,
                     --  variance_account_seg10 =
                     --                    x_out_variance_acc_rec.segment10,
                     --  variance_account_ccid = x_variance_ccid,
                     project_id                  = lv_d_dep_project_id,
                     task_id                     = lv_d_dep_task_id,
                     expenditure_organization_id = lv_exp_org_id,
                     attribute_category          = lv_d_attribute_category,
                     attribute1                  = lv_d_attribute1,
                     attribute2                  = lv_d_attribute2,
                     attribute3                  = lv_d_attribute3,
                     attribute4                  = lv_d_attribute4,
                     attribute5                  = lv_d_attribute5,
                     attribute6                  = lv_d_attribute6,
                     attribute7                  = lv_d_attribute7,
                     attribute8                  = lv_d_attribute7,
                     attribute9                  = lv_d_attribute9,
                     attribute10                 = lv_d_attribute10,
                     attribute11                 = lv_d_attribute11,
                     attribute12                 = lv_d_attribute12,
                     attribute13                 = lv_d_attribute13,
                     attribute14                 = lv_d_attribute14,
                     attribute15                 = lv_d_attribute15,
                     request_id                  = g_request_id,
                     last_updated_date           = sysdate,
                     last_updated_by             = g_last_updated_by,
                     last_update_login           = g_last_update_login
               where interface_txn_id =
                     cur_po_distributions_rec.interface_txn_id;
            exception
              when others then
                print_log_message('Error while updating distribution table for process flag E : ' ||
                                  sqlerrm);
                g_retcode := 1;
            end;
          end if;
          xxetn_debug_pkg.add_debug('AFTER UPDATING xxpo_po_distribution_stg' ||
                                    sysdate);
        end loop;
        --Start UK
        if l_tot_dist_qty <> cur_po_lines_rec.leg_quantity then
          lv_line_err_flag := 'Y';
          l_err_code       := 'ETN_PO_SHIPMENT_DIST_QTY_MISMATCH';
          l_err_msg        := 'Error: Mismatch between PO Shipment quantity and PO Distribution quantity. ';
          log_errors(pin_interface_txn_id    => cur_po_lines_rec.interface_txn_id,
                     piv_source_table        => 'xxpo_po_line_shipment_stg',
                     piv_source_column_name  => 'leg_quantity',
                     piv_source_column_value => cur_po_lines_rec.leg_quantity,
                     piv_source_keyname1     => null,
                     piv_source_keyvalue1    => null,
                     piv_error_type          => 'ERR_VAL',
                     piv_error_code          => l_err_code,
                     piv_error_message       => l_err_msg);
        end if;
        if lv_line_err_flag = 'Y' or cur_po_lines_rec.process_flag = 'E' --1.1
         then
          lv_mast_line_err_flag := 'Y';
        end if;
        --end UK
        xxetn_debug_pkg.add_debug('BEFORE UPDATING xxpo_po_line_shipment_stg' ||
                                  sysdate);
        if (lv_line_err_flag <> 'Y') and
           (cur_po_lines_rec.process_flag <> 'E') --1.1
         then
          begin
            update xxpo_po_line_shipment_stg
               set process_flag = 'V',
                   --line_id = lv_dep_line_type_id,
                   --operating_unit_name = lv_l_org_name,
                   --org_id = lv_l_org_id,
                   --payment_terms = lv_l_pay_name,
                   --terms_id = lv_l_terms_id,
                   --item_id = lv_dep_item_id,
                   --category_id = lv_dep_category_id,
                   --uom_code = lv_dep_uom_code,
                   --un_number_id = lv_dep_un_number_id,
                   --hazard_class_id = lv_dep_hazard_class_id,
                   tax_name    = p_out_tax_name1,
                   tax_code_id = lv_dep_tax_code_id,
                   --receiving_routing_id = lv_dep_receiving_routing_id,
                   --ship_to_organization_id =
                   --              lv_dep_ship_to_organization_id,
                   --ship_to_location_id = lv_dep_ship_to_location_id,
                   line_attr_category_lines = lv_l_attribute_category,
                   leg_line_num             = lv_line_num_new,
                   --ADB leg_item_description     = l_description,
                   line_attribute1        = lv_l_attribute1,
                   line_attribute2        = lv_l_attribute2,
                   line_attribute3        = lv_l_attribute3,
                   line_attribute4        = lv_l_attribute4,
                   line_attribute5        = lv_l_attribute5,
                   line_attribute6        = lv_l_attribute6,
                   line_attribute7        = lv_l_attribute7,
                   line_attribute8        = lv_l_attribute8,
                   line_attribute9        = lv_l_attribute9,
                   line_attribute10       = lv_l_attribute10,
                   line_attribute11       = lv_l_attribute11,
                   line_attribute12       = lv_l_attribute12,
                   line_attribute13       = lv_l_attribute13,
                   line_attribute14       = lv_l_attribute14,
                   line_attribute15       = lv_l_attribute15,
                   shipment_attr_category = lv_ship_attribute_category,
                   shipment_attribute1    = lv_ship_attribute1,
                   shipment_attribute2    = lv_ship_attribute2,
                   shipment_attribute3    = lv_ship_attribute3,
                   shipment_attribute4    = lv_ship_attribute4,
                   shipment_attribute5    = lv_ship_attribute5,
                   /*ADB shipment_attribute6      = lv_ship_attribute6,
                   shipment_attribute7      = lv_ship_attribute7,
                   shipment_attribute8      = lv_ship_attribute8, ADB*/
                   shipment_attribute9  = lv_ship_attribute9,
                   shipment_attribute10 = lv_ship_attribute10,
                   shipment_attribute11 = lv_ship_attribute11,
                   shipment_attribute12 = lv_ship_attribute12,
                   shipment_attribute13 = lv_ship_attribute13,
                   shipment_attribute14 = lv_ship_attribute14,
                   shipment_attribute15 = lv_ship_attribute15,
                   request_id           = g_request_id,
                   last_updated_date    = sysdate,
                   last_updated_by      = g_last_updated_by,
                   last_update_login    = g_last_update_login
             where interface_txn_id = cur_po_lines_rec.interface_txn_id;
          exception
            when others then
              print_log_message('Error while updating line table for process flag V : ' ||
                                sqlerrm);
              g_retcode := 1;
          end;
        else
          g_retcode := 1;
          begin
            update xxpo_po_line_shipment_stg
               set process_flag = 'E',
                   error_type   = 'ERR_VAL',
                   --line_id = lv_dep_line_type_id,
                   --operating_unit_name = lv_l_org_name,
                   --org_id = lv_l_org_id,
                   --payment_terms = lv_l_pay_name,
                   --terms_id = lv_l_terms_id,
                   --item_id = lv_dep_item_id,
                   --category_id = lv_dep_category_id,
                   --uom_code = lv_dep_uom_code,
                   --un_number_id = lv_dep_un_number_id,
                   --hazard_class_id = lv_dep_hazard_class_id,
                   tax_name    = p_out_tax_name1,
                   tax_code_id = lv_dep_tax_code_id,
                   --receiving_routing_id = lv_dep_receiving_routing_id,
                   --ship_to_organization_id =
                   --                      lv_dep_ship_to_organization_id,
                   --ship_to_location_id = lv_dep_ship_to_location_id,
                   line_attr_category_lines = lv_l_attribute_category,
                   line_attribute1          = lv_l_attribute1,
                   line_attribute2          = lv_l_attribute2,
                   line_attribute3          = lv_l_attribute3,
                   line_attribute4          = lv_l_attribute4,
                   line_attribute5          = lv_l_attribute5,
                   line_attribute6          = lv_l_attribute6,
                   line_attribute7          = lv_l_attribute7,
                   line_attribute8          = lv_l_attribute8,
                   line_attribute9          = lv_l_attribute9,
                   line_attribute10         = lv_l_attribute10,
                   line_attribute11         = lv_l_attribute11,
                   line_attribute12         = lv_l_attribute12,
                   line_attribute13         = lv_l_attribute13,
                   line_attribute14         = lv_l_attribute14,
                   line_attribute15         = lv_l_attribute15,
                   shipment_attr_category   = lv_ship_attribute_category,
                   shipment_attribute1      = lv_ship_attribute1,
                   shipment_attribute2      = lv_ship_attribute2,
                   shipment_attribute3      = lv_ship_attribute3,
                   shipment_attribute4      = lv_ship_attribute4,
                   shipment_attribute5      = lv_ship_attribute5,
                   -- shipment_attribute6      = lv_ship_attribute6,
                   --  shipment_attribute7      = lv_ship_attribute7,
                   -- shipment_attribute8      = lv_ship_attribute8,
                   shipment_attribute9  = lv_ship_attribute9,
                   shipment_attribute10 = lv_ship_attribute10,
                   shipment_attribute11 = lv_ship_attribute11,
                   shipment_attribute12 = lv_ship_attribute12,
                   shipment_attribute13 = lv_ship_attribute13,
                   shipment_attribute14 = lv_ship_attribute14,
                   shipment_attribute15 = lv_ship_attribute15,
                   request_id           = g_request_id,
                   last_updated_date    = sysdate,
                   last_updated_by      = g_last_updated_by,
                   last_update_login    = g_last_update_login
             where interface_txn_id = cur_po_lines_rec.interface_txn_id;
          exception
            when others then
              print_log_message('Error while updating line table for process flag E : ' ||
                                sqlerrm);
              g_retcode := 1;
          end;
        end if;
        xxetn_debug_pkg.add_debug('AFTER UPDATING xxpo_po_line_shipment_stg' ||
                                  sysdate);
      end loop;
      xxetn_debug_pkg.add_debug('BEFORE UPDATING xxpo_po_header_stg' ||
                                sysdate);
      if (lv_error_flag <> 'Y') and
         (cur_po_headers_rec.process_flag <> 'E') ---1.1
       then
        begin
          update xxpo_po_header_stg
             set process_flag = 'V',
                 --operating_unit_name = lv_org_name,
                 --org_id = lv_org_id,
                 --agent_id = lv_agent_id,
                 vendor_id         = lv_vendor_id,
                 vendor_site_id    = lv_vendor_site_id,
                 vendor_contact_id = lv_vendor_contact_id,
                 --ship_to_location_id = lv_ship_to_location_id,
                 --bill_to_location_id = lv_bill_to_location_id,
                 --payment_terms = lv_pay_name,
                 --terms_id = lv_terms_id,
                 attribute_category = lv_attribute_category,
                 attribute1         = lv_attribute1,
                 attribute2         = lv_attribute2,
                 attribute3         = lv_attribute3,
                 attribute4         = lv_attribute4,
                 attribute5         = lv_attribute5,
                 attribute6         = lv_attribute6,
                 attribute7         = lv_attribute7,
                 attribute8         = lv_attribute8,
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
           where interface_txn_id = cur_po_headers_rec.interface_txn_id;
        exception
          when others then
            print_log_message('Error while updating Header table for process flag V : ' ||
                              sqlerrm);
            g_retcode := 1;
        end;
      else
        g_retcode := 1;
        begin
          update xxpo_po_header_stg
             set process_flag = 'E',
                 error_type   = 'ERR_VAL',
                 --operating_unit_name = lv_org_name,
                 --org_id = lv_org_id,
                 --agent_id = lv_agent_id,
                 vendor_id         = lv_vendor_id,
                 vendor_site_id    = lv_vendor_site_id,
                 vendor_contact_id = lv_vendor_contact_id,
                 --ship_to_location_id = lv_ship_to_location_id,
                 --bill_to_location_id = lv_bill_to_location_id,
                 --payment_terms = lv_pay_name,
                 --terms_id = lv_terms_id,
                 attribute_category = lv_attribute_category,
                 attribute1         = lv_attribute1,
                 attribute2         = lv_attribute2,
                 attribute3         = lv_attribute3,
                 attribute4         = lv_attribute4,
                 attribute5         = lv_attribute5,
                 attribute6         = lv_attribute6,
                 attribute7         = lv_attribute7,
                 attribute8         = lv_attribute8,
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
           where interface_txn_id = cur_po_headers_rec.interface_txn_id;
        exception
          when others then
            print_log_message('Error while updating Header table for process flag E : ' ||
                              sqlerrm);
            g_retcode := 1;
        end;
      end if;
      xxetn_debug_pkg.add_debug('AFTER UPDATING xxpo_po_header_stg' ||
                                sysdate);
      if lv_header_error_flag = 'Y' or lv_mast_line_err_flag = 'Y' or
         lv_mast_d_line_err_flag = 'Y' then
        for cur_head_err_rec in cur_head_err(cur_po_headers_rec.leg_po_header_id,
                                             cur_po_headers_rec.leg_source_system,
                                             cur_po_headers_rec.leg_operating_unit_name) loop
          update xxpo_po_header_stg
             set process_flag      = 'E',
                 error_type        = 'ERR_VAL',
                 request_id        = g_request_id,
                 last_updated_date = sysdate,
                 last_updated_by   = g_last_updated_by,
                 last_update_login = g_last_update_login
           where interface_txn_id = cur_head_err_rec.interface_txn_id;
          l_err_code := 'ETN_PO_DEPENDENTS_ERROR';
          l_err_msg  := 'Error: Record erred out due to one of the dependents(Header/line/distribution) erring out.  ';
          log_errors(pin_interface_txn_id    => cur_head_err_rec.interface_txn_id,
                     piv_source_table        => 'xxpo_po_header_stg',
                     piv_source_column_name  => 'leg_document_num',
                     piv_source_column_value => cur_head_err_rec.leg_document_num,
                     piv_source_keyname1     => null,
                     piv_source_keyvalue1    => null,
                     piv_error_type          => 'ERR_VAL',
                     piv_error_code          => l_err_code,
                     piv_error_message       => l_err_msg);
        end loop;
        for cur_line_err_rec in cur_line_err(cur_po_headers_rec.leg_po_header_id,
                                             cur_po_headers_rec.leg_source_system,
                                             cur_po_headers_rec.leg_operating_unit_name) loop
          update xxpo_po_line_shipment_stg
             set process_flag      = 'E',
                 error_type        = 'ERR_VAL',
                 request_id        = g_request_id,
                 last_updated_date = sysdate,
                 last_updated_by   = g_last_updated_by,
                 last_update_login = g_last_update_login
           where interface_txn_id = cur_line_err_rec.interface_txn_id;
          l_err_code := 'ETN_PO_DEPENDENTS_ERROR';
          l_err_msg  := 'Error: Record erred out due to one of the dependents(Header/line/distribution) erring out.  ';
          log_errors(pin_interface_txn_id    => cur_line_err_rec.interface_txn_id,
                     piv_source_table        => 'xxpo_po_line_shipment_stg',
                     piv_source_column_name  => 'leg_document_num',
                     piv_source_column_value => cur_line_err_rec.leg_document_num,
                     piv_source_keyname1     => null,
                     piv_source_keyvalue1    => null,
                     piv_error_type          => 'ERR_VAL',
                     piv_error_code          => l_err_code,
                     piv_error_message       => l_err_msg);
        end loop;
        for cur_dist_err_rec in cur_dist_err(cur_po_headers_rec.leg_po_header_id,
                                             cur_po_headers_rec.leg_source_system,
                                             cur_po_headers_rec.leg_operating_unit_name) loop
          update xxpo_po_distribution_stg
             set process_flag      = 'E',
                 error_type        = 'ERR_VAL',
                 request_id        = g_request_id,
                 last_updated_date = sysdate,
                 last_updated_by   = g_last_updated_by,
                 last_update_login = g_last_update_login
           where interface_txn_id = cur_dist_err_rec.interface_txn_id;
          l_err_code := 'ETN_PO_DEPENDENTS_ERROR';
          l_err_msg  := 'Error: Record erred out due to one of the dependents(Header/line/distribution) erring out.  ';
          log_errors(pin_interface_txn_id    => cur_dist_err_rec.interface_txn_id,
                     piv_source_table        => 'xxpo_po_distribution_stg',
                     piv_source_column_name  => 'leg_document_num',
                     piv_source_column_value => cur_dist_err_rec.leg_document_num,
                     piv_source_keyname1     => null,
                     piv_source_keyvalue1    => null,
                     piv_error_type          => 'ERR_VAL',
                     piv_error_code          => l_err_code,
                     piv_error_message       => l_err_msg);
        end loop;
      end if;
      xxetn_debug_pkg.add_debug('AFTER ERRING dependents records' ||
                                sysdate);
      if l_count >= 500 then
        l_count := 0;
        xxetn_debug_pkg.add_debug('
                        Performing Batch Commit');
        commit;
      else
        l_count := l_count + 1;
      end if;
    end loop;
    commit;
    xxetn_debug_pkg.add_debug('VALIDATE PROCEDURE ENDS');
  exception
    when others then
      print_log_message('EXCEPTION error during procedure Validate : ' ||
                        sqlerrm);
      g_retcode := 2;
      print_log_message('Error : Backtace : ' ||
                        dbms_utility.format_error_backtrace);
  end validate_po;

  /*
    +================================================================================+
    |PROCEDURE NAME : conversion                                                     |
    |DESCRIPTION   : This procedure will upload data into the interface table        |
    +================================================================================+
  */
  procedure conversion is
    cursor cur_po_headers is
      select a.rowid, a.*
        from xxpo_po_header_stg a
       where process_flag = 'V'
         and batch_id = g_batch_id;
    cursor cur_po_lines(p_in_header_id          number,
                        p_in_leg_source_sys     varchar2,
                        p_in_leg_operating_unit varchar2) is
      select /*+ INDEX ( XXPO_PO_LINE_SHIPMENT_STG_N4) */
       leg_drop_ship_flag,
       leg_supplier_ref_number,
       leg_consigned_flag,
       leg_note_to_receiver,
       tax_name,
       tax_code_id,
       leg_tax_user_override_flag,
       leg_tax_status_indicator,
       org_id,
       line_attribute15,
       line_attribute14,
       line_attribute13,
       line_attribute12,
       line_attribute11,
       line_attribute10,
       line_attribute9,
       line_attribute8,
       line_attribute7,
       line_attribute6,
       line_attribute5,
       line_attribute4,
       line_attribute3,
       line_attribute2,
       line_attribute1,
       line_attr_category_lines,
       --leg_accrue_on_receipt_flag,
       max(leg_promised_date) v_leg_promised_date,
       --ship_to_location_id,
       --ship_to_organization_id,
       leg_qty_rcv_tolerance,
       receiving_routing_id,
       leg_days_late_receipt_allowed,
       leg_days_early_receipt_allowed,
       leg_receive_close_tolerance,
       leg_invoice_close_tolerance,
       leg_price_type,
       terms_id,
       leg_receipt_required_flag,
       leg_inspection_required_flag,
       leg_transaction_reason_code,
       leg_note_to_vendor,
       hazard_class_id,
       un_number_id,
       --leg_unit_price ,
       leg_unit_price,
       sum(leg_quantity) v_leg_quantity,
       uom_code,
       leg_vendor_product_num,
       leg_item_description,
       category_id,
       leg_item_revision,
       item_id,
       line_id,
       leg_shipment_type,
       leg_line_num,
       leg_po_header_id,
       leg_po_line_id,
       leg_operating_unit_name
        from xxpo_po_line_shipment_stg
       where leg_po_header_id = p_in_header_id
         and process_flag = 'V'
         and batch_id = g_batch_id
         and run_sequence_id = g_new_run_seq_id -- v1.15
         and leg_source_system = p_in_leg_source_sys
         and leg_operating_unit_name = p_in_leg_operating_unit
       group by leg_drop_ship_flag,
                leg_supplier_ref_number,
                leg_consigned_flag,
                leg_note_to_receiver,
                tax_name,
                tax_code_id,
                leg_tax_user_override_flag,
                leg_tax_status_indicator,
                org_id,
                line_attribute15,
                line_attribute14,
                line_attribute13,
                line_attribute12,
                line_attribute11,
                line_attribute10,
                line_attribute9,
                line_attribute8,
                line_attribute7,
                line_attribute6,
                line_attribute5,
                line_attribute4,
                line_attribute3,
                line_attribute2,
                line_attribute1,
                line_attr_category_lines,
                leg_qty_rcv_tolerance,
                receiving_routing_id,
                leg_days_late_receipt_allowed,
                leg_days_early_receipt_allowed,
                leg_receive_close_tolerance,
                leg_invoice_close_tolerance,
                leg_price_type,
                terms_id,
                leg_receipt_required_flag,
                leg_inspection_required_flag,
                leg_transaction_reason_code,
                leg_note_to_vendor,
                hazard_class_id,
                un_number_id,
                --leg_unit_price ,
                leg_unit_price,
                uom_code,
                leg_vendor_product_num,
                leg_item_description,
                category_id,
                leg_item_revision,
                item_id,
                line_id,
                leg_shipment_type,
                leg_line_num,
                leg_po_header_id,
                leg_po_line_id,
                leg_operating_unit_name;
    -- added SDP v1.24
    cursor cur_po_line_locations(p_in_header_id          number,
                                 p_in_leg_source_sys     varchar2,
                                 p_in_leg_operating_unit varchar2,
                                 p_in_line_id            number,
                                 p_in_line_num number) is
      select /*+ INDEX ( XXPO_PO_LINE_SHIPMENT_STG_N4) */
       interface_txn_id,
       tax_name,
       tax_code_id,
       leg_accrue_on_receipt_flag,
       ship_to_location_id,
       ship_to_organization_id,
       leg_receipt_required_flag,
       leg_unit_price,
       leg_quantity,
       leg_shipment_num,
       leg_po_line_location_id,
       shipment_attribute15,
       shipment_attribute14,
       shipment_attribute13,
       shipment_attribute12,
       shipment_attribute11,
       shipment_attribute10,
       shipment_attribute9,
       shipment_attribute8,
       shipment_attribute7,
       shipment_attribute6,
       shipment_attribute5,
       shipment_attribute4,
       shipment_attribute3,
       shipment_attribute2,
       shipment_attribute1,
       shipment_attr_category,
       leg_match_option,
       leg_operating_unit_name
        from xxpo_po_line_shipment_stg
       where leg_po_header_id = p_in_header_id
         and leg_po_line_id = p_in_line_id
         and leg_line_num = p_in_line_num
         and process_flag = 'G'
         and batch_id = g_batch_id
         and run_sequence_id = g_new_run_seq_id -- v1.15
         and leg_source_system = p_in_leg_source_sys
         and leg_operating_unit_name = p_in_leg_operating_unit;
    -- added SDP v1.24
    cursor cur_po_distributions(p_in_header_id          number,
                                p_in_leg_source_sys     varchar2,
                                p_in_leg_operating_unit varchar2,
                                p_in_line_id            number,
                                p_in_location_id        number,
                                p_in_shipment_number    number
                                /*p_in_line_num           number*/) is
      select /*+ INDEX (a XXPO_PO_DISTRIBUTION_STG_N4) */
       a.rowid, a.*
        from xxpo_po_distribution_stg a
       where a.process_flag = 'V'
         and a.batch_id = g_batch_id
         and a.run_sequence_id = g_new_run_seq_id -- v1.15
         and a.leg_po_line_id = p_in_line_id
            --and a.LEG_LINE_NUM = p_in_line_num       -- addded SDP v1.24
         and a.leg_po_header_id = p_in_header_id
         and batch_id = g_batch_id
         and a.leg_source_system = p_in_leg_source_sys
         and a.leg_operating_unit_name = p_in_leg_operating_unit
         and a.leg_po_line_location_id = p_in_location_id
         and a.leg_shipment_num = p_in_shipment_number;
    l_err_code               varchar2(40);
    l_err_msg                varchar2(2000);
    lv_h_err_flag            varchar(1) := 'N';
    lv_l_err_flag            varchar(1) := 'N';
    lv_d_err_flag            varchar(1) := 'N';
    l_dist_conc_segments     varchar2(150); -- PMC # 310987 SDP
    l_dist_conc_var_segments varchar2(150); ---- PMC # 310987 SDP      --v1.14
    l_dist_conc_acc_segments varchar2(150); ---- PMC # 310987 SDP      --v1.14
  begin
    g_retcode := 0;
    for cur_po_headers_rec in cur_po_headers loop
      xxetn_debug_pkg.add_debug('INSIDE UPLOAD PROCEDURE LOOP OF PO HEADER ' ||
                                sysdate);
      lv_h_err_flag := 'N';
      --FND_FILE.PUT_LINE(FND_FILE.LOG,'HEADER');
      begin
        insert into po_headers_interface
          (change_summary,
           program_update_date,
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
           acceptance_due_date,
           acceptance_required_flag,
           comments,
           confirming_order_flag,
           note_to_receiver,
           note_to_vendor,
           revision_num,
           revised_date,
           approved_date,
           approval_status,
           freight_terms,
           fob,
           freight_carrier,
           terms_id,
           bill_to_location_id,
           ship_to_location_id,
           vendor_contact_id,
           vendor_site_id,
           vendor_id,
           agent_id,
           rate,
           rate_date,
           rate_type,
           currency_code,
           document_num,
           document_type_code,
           org_id,
           group_code,
           action,
           process_code,
           interface_source_code,
           batch_id,
           interface_header_id)
        values
          (cur_po_headers_rec.leg_change_summary,
           sysdate,
           g_conc_program_id,
           g_prog_appl_id,
           g_request_id,
           g_last_update_login,
           g_last_updated_by,
           sysdate,
           g_created_by,
           sysdate,
           cur_po_headers_rec.attribute15,
           cur_po_headers_rec.attribute14,
           cur_po_headers_rec.attribute13,
           cur_po_headers_rec.attribute12,
           cur_po_headers_rec.attribute11,
           cur_po_headers_rec.attribute10,
           cur_po_headers_rec.attribute9,
           cur_po_headers_rec.attribute8,
           cur_po_headers_rec.attribute7,
           cur_po_headers_rec.attribute6,
           cur_po_headers_rec.attribute5,
           cur_po_headers_rec.attribute4,
           cur_po_headers_rec.attribute3,
           cur_po_headers_rec.attribute2,
           cur_po_headers_rec.attribute1,
           cur_po_headers_rec.attribute_category,
           cur_po_headers_rec.leg_acceptance_due_date,
           cur_po_headers_rec.leg_acceptance_required_flag,
           cur_po_headers_rec.leg_comments,
           cur_po_headers_rec.leg_confirming_order_flag,
           cur_po_headers_rec.leg_note_to_receiver,
           cur_po_headers_rec.leg_note_to_vendor,
           0,
           --cur_po_headers_rec.revision_num,
           null,
           --cur_po_headers_rec.revised_date,
           null,
           'APPROVED',
           upper(cur_po_headers_rec.leg_freight_terms),
           cur_po_headers_rec.leg_fob,
           upper(cur_po_headers_rec.leg_freight_carrier),
           cur_po_headers_rec.terms_id,
           cur_po_headers_rec.bill_to_location_id,
           cur_po_headers_rec.ship_to_location_id,
           cur_po_headers_rec.vendor_contact_id,
           cur_po_headers_rec.vendor_site_id,
           cur_po_headers_rec.vendor_id,
           cur_po_headers_rec.agent_id,
           cur_po_headers_rec.leg_rate,
           cur_po_headers_rec.leg_rate_date,
           cur_po_headers_rec.leg_rate_type,
           cur_po_headers_rec.leg_currency_code,
           cur_po_headers_rec.leg_document_num,
           cur_po_headers_rec.leg_document_type_code,
           cur_po_headers_rec.org_id,
           null,
           'ORIGINAL',
           null,
           'CONVERSION',
           cur_po_headers_rec.batch_id,
           po_headers_interface_s.nextval);
        commit;
        --FND_FILE.PUT_LINE(FND_FILE.LOG,cur_po_headers_rec.leg_po_header_id);
      exception
        when others then
          lv_h_err_flag := 'Y';
          l_err_code    := 'ETN_PO_HEADER_INSERT_ERR';
          l_err_msg     := 'Error: Exceptional error while inserting header record :  ' ||
                           sqlerrm;
          log_errors(pin_interface_txn_id    => cur_po_headers_rec.interface_txn_id,
                     piv_source_table        => 'xxpo_po_header_stg',
                     piv_source_column_name  => null,
                     piv_source_column_value => null,
                     piv_source_keyname1     => null,
                     piv_source_keyvalue1    => null,
                     piv_error_type          => 'ERR_INT',
                     piv_error_code          => l_err_code,
                     piv_error_message       => l_err_msg);
      end;
      if lv_h_err_flag = 'N' then
        update xxpo_po_header_stg
           set process_flag      = 'P',
               error_type        = null,
               request_id        = g_request_id,
               last_updated_date = sysdate,
               last_updated_by   = g_last_updated_by,
               last_update_login = g_last_update_login
         where interface_txn_id = cur_po_headers_rec.interface_txn_id;
        --FND_FILE.PUT_LINE(FND_FILE.LOG,'HEAD UPDATE'||cur_po_headers_rec.leg_po_header_id);
        commit;
        xxetn_debug_pkg.add_debug('Record id ' ||
                                  cur_po_headers_rec.interface_txn_id ||
                                  'is successfully processed');
      else
        g_retcode := 1;
        update xxpo_po_header_stg
           set process_flag      = 'E',
               error_type        = 'ERR_INT',
               request_id        = g_request_id,
               last_updated_date = sysdate,
               last_updated_by   = g_last_updated_by,
               last_update_login = g_last_update_login
         where interface_txn_id = cur_po_headers_rec.interface_txn_id;
        --FND_FILE.PUT_LINE(FND_FILE.LOG,'HEAD ERROR UPDATE'||cur_po_headers_rec.leg_po_header_id);
        commit;
        xxetn_debug_pkg.add_debug('Record id ' ||
                                  cur_po_headers_rec.interface_txn_id ||
                                  'is erred out');
      end if;
      for cur_po_lines_rec in cur_po_lines(cur_po_headers_rec.leg_po_header_id,
                                           cur_po_headers_rec.leg_source_system,
                                           cur_po_headers_rec.leg_operating_unit_name) loop
        xxetn_debug_pkg.add_debug('INSIDE UPLOAD PROCEDURE LOOP OF PO LINE ' ||
                                  sysdate);
        lv_l_err_flag := 'N';
        begin
          --FND_FILE.PUT_LINE(FND_FILE.LOG,'LINE'||cur_po_headers_rec.leg_po_header_id);
          insert into po_lines_interface
            (drop_ship_flag,
             supplier_ref_number,
             consigned_flag,
             note_to_receiver,
             tax_code_id,
             tax_name,
             tax_user_override_flag,
             process_code,
             tax_status_indicator,
             organization_id,
             program_update_date,
             program_id,
             program_application_id,
             request_id,
             last_update_login,
             last_updated_by,
             last_update_date,
             created_by,
             creation_date,
             /*shipment_attribute15,
             shipment_attribute14,
             shipment_attribute13,
             shipment_attribute12,
             shipment_attribute11,
             shipment_attribute10,
             shipment_attribute9,
             shipment_attribute8,
             shipment_attribute7,
             shipment_attribute6,
             shipment_attribute5,
             shipment_attribute4,
             shipment_attribute3,
             shipment_attribute2,
             shipment_attribute1,
             shipment_attribute_category,*/
             line_attribute15,
             line_attribute14,
             line_attribute13,
             line_attribute12,
             line_attribute11,
             line_attribute10,
             line_attribute9,
             line_attribute8,
             line_attribute7,
             line_attribute6,
             line_attribute5,
             line_attribute4,
             line_attribute3,
             line_attribute2,
             line_attribute1,
             line_attribute_category_lines,
             --accrue_on_receipt_flag,
             promised_date,
             need_by_date,
             --ship_to_location_id,
             --ship_to_organization_id,
             qty_rcv_tolerance,
             receiving_routing_id,
             days_late_receipt_allowed,
             days_early_receipt_allowed,
             receive_close_tolerance,
             invoice_close_tolerance,
             price_type,
             terms_id,
             receipt_required_flag,
             inspection_required_flag,
             taxable_flag,
             transaction_reason_code,
             note_to_vendor,
             hazard_class_id,
             un_number_id,
             list_price_per_unit,
             unit_price,
             quantity,
             uom_code,
             vendor_product_num,
             item_description,
             category_id,
             item_revision,
             item_id,
             line_type_id,
             shipment_type,
             -- shipment_num,
             line_num,
             group_code,
             action,
             interface_header_id,
             interface_line_id,
             qty_rcv_exception_code,
             over_tolerance_error_flag, -- DEFECT NUM --    v1.24 SDP 25/03/2016
             line_loc_populated_flag) -- DEFECT BCOS OF PATCH APPLIED BY OTC TEAM --    v1.24 SDP 5/05/2016
          values
            (cur_po_lines_rec.leg_drop_ship_flag,
             cur_po_lines_rec.leg_supplier_ref_number,
             cur_po_lines_rec.leg_consigned_flag,
             cur_po_lines_rec.leg_note_to_receiver,
             null,
             cur_po_lines_rec.tax_name,
             cur_po_lines_rec.leg_tax_user_override_flag,
             null,
             cur_po_lines_rec.leg_tax_status_indicator,
             cur_po_lines_rec.org_id,
             sysdate,
             g_conc_program_id,
             g_prog_appl_id,
             g_request_id,
             g_last_update_login,
             g_last_updated_by,
             sysdate,
             g_created_by,
             sysdate,
             /*cur_po_lines_rec.shipment_attribute15,
             cur_po_lines_rec.shipment_attribute14,
             cur_po_lines_rec.shipment_attribute13,
             cur_po_lines_rec.shipment_attribute12,
             cur_po_lines_rec.shipment_attribute11,
             cur_po_lines_rec.shipment_attribute10,
             cur_po_lines_rec.shipment_attribute9,
             cur_po_lines_rec.shipment_attribute8,
             NULL,-- cur_po_lines_rec.shipment_attribute7,
             cur_po_lines_rec.shipment_attribute6,
             cur_po_lines_rec.shipment_attribute5,
             cur_po_lines_rec.shipment_attribute4,
             cur_po_lines_rec.shipment_attribute3,
             cur_po_lines_rec.shipment_attribute2,
             cur_po_lines_rec.shipment_attribute1,
             cur_po_lines_rec.shipment_attr_category,*/
             cur_po_lines_rec.line_attribute15,
             cur_po_lines_rec.line_attribute14,
             cur_po_lines_rec.line_attribute13,
             cur_po_lines_rec.line_attribute12,
             cur_po_lines_rec.line_attribute11,
             cur_po_lines_rec.line_attribute10,
             cur_po_lines_rec.line_attribute9,
             cur_po_lines_rec.line_attribute8,
             cur_po_lines_rec.line_attribute7,
             cur_po_lines_rec.line_attribute6,
             cur_po_lines_rec.line_attribute5,
             cur_po_lines_rec.line_attribute4,
             cur_po_lines_rec.line_attribute3,
             cur_po_lines_rec.line_attribute2,
             cur_po_lines_rec.line_attribute1,
             cur_po_lines_rec.line_attr_category_lines,
             --cur_po_lines_rec.leg_accrue_on_receipt_flag,
             cur_po_lines_rec.v_leg_promised_date,
             null, --     cur_po_lines_rec.leg_need_by_date,
             --cur_po_lines_rec.ship_to_location_id,
             --cur_po_lines_rec.ship_to_organization_id,
             cur_po_lines_rec.leg_qty_rcv_tolerance,
             cur_po_lines_rec.receiving_routing_id,
             cur_po_lines_rec.leg_days_late_receipt_allowed,
             cur_po_lines_rec.leg_days_early_receipt_allowed,
             cur_po_lines_rec.leg_receive_close_tolerance,
             cur_po_lines_rec.leg_invoice_close_tolerance,
             cur_po_lines_rec.leg_price_type,
             cur_po_lines_rec.terms_id,
             cur_po_lines_rec.leg_receipt_required_flag,
             cur_po_lines_rec.leg_inspection_required_flag,
             decode(cur_po_lines_rec.tax_code_id, null, null, 'Y'),
             cur_po_lines_rec.leg_transaction_reason_code,
             cur_po_lines_rec.leg_note_to_vendor,
             cur_po_lines_rec.hazard_class_id,
             cur_po_lines_rec.un_number_id,
             cur_po_lines_rec.leg_unit_price,
             cur_po_lines_rec.leg_unit_price,
             cur_po_lines_rec.v_leg_quantity, --- sum qty
             cur_po_lines_rec.uom_code,
             cur_po_lines_rec.leg_vendor_product_num,
             cur_po_lines_rec.leg_item_description,
             cur_po_lines_rec.category_id,
             cur_po_lines_rec.leg_item_revision,
             cur_po_lines_rec.item_id,
             cur_po_lines_rec.line_id,
             cur_po_lines_rec.leg_shipment_type,
             -- NULL,--  cur_po_lines_rec.leg_shipment_num,
             cur_po_lines_rec.leg_line_num,
             null,
             'ADD',
             po_headers_interface_s.currval,
             po_lines_interface_s.nextval,
             'NONE',
             'NONE', --    v1.24 SDP 25/03/2016
             'Y'); -- DEFECT BCOS OF PATCH APPLIED BY OTC TEAM --    v1.24 SDP 5/05/2016
          commit;
          --FND_FILE.PUT_LINE(FND_FILE.LOG,'LINE'||cur_po_lines_rec.leg_po_line_id);
        exception
          when others then
            lv_l_err_flag := 'Y';
            l_err_code    := 'ETN_PO_LINE_INSERT_ERR';
            l_err_msg     := 'Error: Exceptional error while inserting line record :  ' ||
                             sqlerrm;
            log_errors(pin_interface_txn_id    => cur_po_lines_rec.leg_po_line_id,
                       piv_source_table        => 'xxpo_po_line_shipment_stg',
                       piv_source_column_name  => null,
                       piv_source_column_value => null,
                       piv_source_keyname1     => null,
                       piv_source_keyvalue1    => null,
                       piv_error_type          => 'ERR_INT',
                       piv_error_code          => l_err_code,
                       piv_error_message       => l_err_msg);
        end;
        if lv_l_err_flag = 'N' then
          update xxpo_po_line_shipment_stg
             set process_flag      = 'G', --- let it remain as G as will be used below
                 error_type        = null,
                 request_id        = g_request_id,
                 last_updated_date = sysdate,
                 last_updated_by   = g_last_updated_by,
                 last_update_login = g_last_update_login
           where leg_po_line_id = cur_po_lines_rec.leg_po_line_id -- SDP V1.24
             and leg_po_header_id = cur_po_lines_rec.leg_po_header_id -- SDP V1.24
             and org_id = cur_po_lines_rec.org_id
             and leg_line_num = cur_po_lines_rec.leg_line_num; -- SDP V1.24
          commit;
          --FND_FILE.PUT_LINE(FND_FILE.LOG,'LINE UPDATE'||cur_po_lines_rec.leg_po_line_id||'-'||cur_po_lines_rec.leg_line_num);
          xxetn_debug_pkg.add_debug('Record id ' ||
                                    cur_po_lines_rec.leg_po_line_id ||
                                    'is successfully processed');
        else
          g_retcode := 1;
          update xxpo_po_line_shipment_stg
             set process_flag      = 'E',
                 error_type        = 'ERR_INT',
                 request_id        = g_request_id,
                 last_updated_date = sysdate,
                 last_updated_by   = g_last_updated_by,
                 last_update_login = g_last_update_login
           where leg_po_line_id = cur_po_lines_rec.leg_po_line_id -- SDP V1.24
             and leg_po_header_id = cur_po_lines_rec.leg_po_header_id -- SDP V1.24
             and org_id = cur_po_lines_rec.org_id
             and leg_line_num = cur_po_lines_rec.leg_line_num; -- SDP V1.24
          commit;
          --FND_FILE.PUT_LINE(FND_FILE.LOG,'ERR LINE UPDATE'||cur_po_lines_rec.leg_po_line_id);
          xxetn_debug_pkg.add_debug('Record id ' ||
                                    cur_po_lines_rec.leg_po_line_id ||
                                    'is erred out');
        end if;
        --- ADD CURSOR FOR LINE_LOCATIONS_INTERFACE   v1.24 SDP
        --cur_po_Line_locations
        --FND_FILE.PUT_LINE(FND_FILE.LOG,'FINAL LINE UPDATE'||cur_po_headers_rec.leg_po_header_id||'-'||cur_po_lines_rec.leg_po_line_id||'-'||cur_po_lines_rec.leg_line_num);
        for cur_po_line_locations_rec in cur_po_line_locations(cur_po_headers_rec.leg_po_header_id,
                                                               cur_po_headers_rec.leg_source_system,
                                                               cur_po_headers_rec.leg_operating_unit_name,
                                                               cur_po_lines_rec.leg_po_line_id,
                                                               cur_po_lines_rec.leg_line_num) loop
          --fnd_file.put_line(fnd_file.log,
          -- 'INSIDE UPLOAD PROCEDURE LOOP OF PO LINE LOCATION ' ||
          -- sysdate);
          lv_d_err_flag := 'N';
          begin
            --FND_FILE.PUT_LINE(FND_FILE.LOG,'LINE LOC'||cur_po_lines_rec.leg_po_line_id||'-'||cur_po_lines_rec.leg_line_num||cur_po_headers_rec.leg_po_header_id );
            insert into po_line_locations_interface
              (tax_code_id,
               tax_name,
               process_code,
               program_update_date,
               program_id,
               program_application_id,
               request_id,
               last_update_login,
               last_updated_by,
               last_update_date,
               created_by,
               creation_date,
               accrue_on_receipt_flag,
               ship_to_location_id,
               ship_to_organization_id,
               receipt_required_flag,
               taxable_flag,
               --unit_price,
               quantity,
               shipment_num,
               action,
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
               interface_header_id,
               interface_line_id,
               interface_line_location_id,
               match_option,
               qty_rcv_exception_code)
            values
              (null,
               cur_po_line_locations_rec.tax_name,
               null,
               sysdate,
               g_conc_program_id,
               g_prog_appl_id,
               g_request_id,
               g_last_update_login,
               g_last_updated_by,
               sysdate,
               g_created_by,
               sysdate,
               cur_po_line_locations_rec.leg_accrue_on_receipt_flag,
               cur_po_line_locations_rec.ship_to_location_id,
               cur_po_line_locations_rec.ship_to_organization_id,
               cur_po_line_locations_rec.leg_receipt_required_flag,
               decode(cur_po_line_locations_rec.tax_code_id,
                      null,
                      null,
                      'Y'),
               --cur_po_line_locations_rec.leg_unit_price,
               cur_po_line_locations_rec.leg_quantity,
               cur_po_line_locations_rec.leg_shipment_num,
               'ADD',
               cur_po_line_locations_rec.shipment_attribute15,
               cur_po_line_locations_rec.shipment_attribute14,
               cur_po_line_locations_rec.shipment_attribute13,
               cur_po_line_locations_rec.shipment_attribute12,
               cur_po_line_locations_rec.shipment_attribute11,
               cur_po_line_locations_rec.shipment_attribute10,
               cur_po_line_locations_rec.shipment_attribute9,
               cur_po_line_locations_rec.shipment_attribute8,
               cur_po_line_locations_rec.shipment_attribute7,
               cur_po_line_locations_rec.shipment_attribute6,
               cur_po_line_locations_rec.shipment_attribute5,
               cur_po_line_locations_rec.shipment_attribute4,
               cur_po_line_locations_rec.shipment_attribute3,
               cur_po_line_locations_rec.shipment_attribute2,
               cur_po_line_locations_rec.shipment_attribute1,
               cur_po_line_locations_rec.shipment_attr_category,
               po_headers_interface_s.currval,
               po_lines_interface_s.currval,
               po_line_locations_interface_s.nextval,
               cur_po_line_locations_rec.leg_match_option,
               'NONE');
            commit;
            --FND_FILE.PUT_LINE(FND_FILE.LOG,'LINE LOC'||cur_po_line_locations_rec.leg_po_line_location_id||cur_po_line_locations_rec.leg_shipment_num);
          exception
            when others then
              lv_l_err_flag := 'Y';
              l_err_code    := 'ETN_PO_LINE_INSERT_ERR';
              l_err_msg     := 'Error: Exceptional error while inserting line record :  ' ||
                               sqlerrm;
              log_errors(pin_interface_txn_id    => cur_po_line_locations_rec.interface_txn_id,
                         piv_source_table        => 'xxpo_po_line_shipment_stg',
                         piv_source_column_name  => null,
                         piv_source_column_value => null,
                         piv_source_keyname1     => null,
                         piv_source_keyvalue1    => null,
                         piv_error_type          => 'ERR_INT',
                         piv_error_code          => l_err_code,
                         piv_error_message       => l_err_msg);
          end;
          if lv_l_err_flag = 'N' then
            update xxpo_po_line_shipment_stg
               set process_flag      = 'P',
                   error_type        = null,
                   request_id        = g_request_id,
                   last_updated_date = sysdate,
                   last_updated_by   = g_last_updated_by,
                   last_update_login = g_last_update_login
             where interface_txn_id =
                   cur_po_line_locations_rec.interface_txn_id;
            commit;
            --FND_FILE.PUT_LINE(FND_FILE.LOG,'UPDATE LINE LOC'||cur_po_line_locations_rec.interface_txn_id);
            xxetn_debug_pkg.add_debug('Record id ' ||
                                      cur_po_line_locations_rec.interface_txn_id ||
                                      'is successfully processed');
          else
            g_retcode := 1;
            update xxpo_po_line_shipment_stg
               set process_flag      = 'E',
                   error_type        = 'ERR_INT',
                   request_id        = g_request_id,
                   last_updated_date = sysdate,
                   last_updated_by   = g_last_updated_by,
                   last_update_login = g_last_update_login
             where interface_txn_id =
                   cur_po_line_locations_rec.interface_txn_id;
            commit;
            --FND_FILE.PUT_LINE(FND_FILE.LOG,'ERR UPDATE LINE LOC'||cur_po_line_locations_rec.interface_txn_id);
            xxetn_debug_pkg.add_debug('Record id ' ||
                                      cur_po_line_locations_rec.interface_txn_id ||
                                      'is erred out');
          end if;
          --- END ADD CURSOR FOR LINE_LOCATIONS_INTERFACE   v1.24 SDP
          for cur_po_distributions_rec in cur_po_distributions(cur_po_headers_rec.leg_po_header_id,
                                                               cur_po_headers_rec.leg_source_system,
                                                               cur_po_headers_rec.leg_operating_unit_name,
                                                               cur_po_lines_rec.leg_po_line_id,
                                                               --cur_po_lines_rec.leg_line_num,
                                                               cur_po_line_locations_rec.leg_po_line_location_id,
                                                               cur_po_line_locations_rec.leg_shipment_num) loop
            -- fnd_file.put_line(fnd_file.log,
            -- 'INSIDE UPLOAD PROCEDURE LOOP OF PO DISTRIBUTION ' ||
            --  sysdate);
            lv_d_err_flag := 'N';
            begin
              ---
              --- PMC # 310987 SDP
              ---
              select concatenated_segments
                into l_dist_conc_segments
                from gl_code_combinations_kfv
               where code_combination_id =
                     cur_po_distributions_rec.charge_account_ccid;
              ---
              --- PMC # 310987 SDP
              ---
              ---
              --- PMC # 310987 SDP
              ---
              --PMC # 310987 SDP  --v1.14
              ---
              select concatenated_segments
                into l_dist_conc_var_segments
                from gl_code_combinations_kfv
               where code_combination_id =
                     cur_po_distributions_rec.variance_account_ccid;
              ---
              --- PMC # 310987 SDP
              ---
              --PMC # 310987 SDP  --v1.14
              ---
              select concatenated_segments
                into l_dist_conc_acc_segments
                from gl_code_combinations_kfv
               where code_combination_id =
                     cur_po_distributions_rec.accural_account_ccid;
              ---
              --- PMC # 310987 SDP
              ---
              --FND_FILE.PUT_LINE(FND_FILE.LOG,'DIST LOC'||cur_po_lines_rec.leg_po_line_id||'-'||cur_po_line_locations_rec.leg_po_line_location_id||'-'
              --    || cur_po_line_locations_rec.leg_shipment_num||cur_po_headers_rec.leg_po_header_id );
              insert into po_distributions_interface
                (process_code,
                 invoice_adjustment_flag,
                 amount_ordered,
                 program_update_date,
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
                 expenditure_item_date,
                 expenditure_organization_id,
                 project_accounting_context,
                 expenditure_type,
                 task_id,
                 project_id,
                 accrued_flag,
                 accrue_on_receipt_flag,
                 variance_account_id,
                 accrual_account_id,
                 charge_account_id, --- Value is passed as NULL as PER PMC#3108917
                 set_of_books_id,
                 destination_subinventory,
                 destination_organization_id,
                 destination_type_code,
                 deliver_to_person_id,
                 deliver_to_location_id,
                 rate,
                 rate_date,
                 quantity_ordered,
                 org_id,
                 distribution_num,
                 interface_distribution_id,
                 interface_line_location_id, --v1.24 SDP 25/03/2016
                 interface_line_id,
                 interface_header_id)
              values
                (null,
                 cur_po_distributions_rec.leg_invoice_adjustment_flag,
                 cur_po_distributions_rec.leg_amount_ordered,
                 sysdate,
                 g_conc_program_id,
                 g_prog_appl_id,
                 g_request_id,
                 g_last_update_login,
                 g_last_updated_by,
                 sysdate,
                 g_created_by,
                 sysdate,
                 cur_po_distributions_rec.attribute15,
                 cur_po_distributions_rec.attribute14,
                 --cur_po_distributions_rec.attribute13,
                 po_distributions_interface_s.currval, -- PMC # 310987 SDP
                 cur_po_distributions_rec.attribute12,
                 cur_po_distributions_rec.attribute11,
                 cur_po_distributions_rec.attribute10,
                 cur_po_distributions_rec.attribute9,
                 cur_po_distributions_rec.attribute8,
                 cur_po_distributions_rec.attribute7,
                 cur_po_distributions_rec.attribute6,
                 cur_po_distributions_rec.attribute5,
                 cur_po_distributions_rec.attribute11, --- As per recent change to shift from attribute11 to 4 due to warehouse data in interfaces-SDP-27/10/2016
                 --cur_po_distributions_rec.attribute4,
                 --cur_po_distributions_rec.attribute3,
                 --l_dist_conc_acc_segments, ---- PMC # 310987 SDP   --v1.14  --- Flip seen so corrected with var segments ALM DEFECT 4743
                 l_dist_conc_var_segments,
                 --cur_po_distributions_rec.attribute2,
                 ---l_dist_conc_var_segments, ---- PMC # 310987 SDP   --v1.14  --- Flip seen so corrected with acc segments ALM DEFECT 4743
                 l_dist_conc_acc_segments,
                 --cur_po_distributions_rec.attribute1,
                 l_dist_conc_segments, -- PMC # 310987 SDP
                 cur_po_distributions_rec.attribute_category,
                 cur_po_distributions_rec.leg_expenditure_item_date,
                 cur_po_distributions_rec.expenditure_organization_id,
                 --                               cur_po_distributions_rec.leg_project_accounting_context,  -- Commented and Added by Abhijit
                 decode(cur_po_distributions_rec.project_id,
                        null,
                        null,
                        'Y'),
                 cur_po_distributions_rec.leg_expenditure_type,
                 cur_po_distributions_rec.task_id,
                 cur_po_distributions_rec.project_id,
                 cur_po_distributions_rec.leg_accrued_flag,
                 cur_po_distributions_rec.leg_accrue_on_receipt_flag,
                 --cur_po_distributions_rec.variance_account_ccid,
                 null, ---- PMC # 310987 SDP---------------------    --v1.14
                 --cur_po_distributions_rec.accural_account_ccid,
                 null, ---- PMC # 310987 SDP ----------------------  --v1.14
                 --cur_po_distributions_rec.charge_account_ccid,
                 null, -- PMC # 310987 SDP
                 cur_po_distributions_rec.set_of_books_id,
                 cur_po_distributions_rec.leg_destination_subinventory,
                 cur_po_distributions_rec.destination_organization_id,
                 cur_po_distributions_rec.leg_destination_type_code,
                 cur_po_distributions_rec.deliver_to_person_id,
                 cur_po_distributions_rec.deliver_to_location_id,
                 cur_po_distributions_rec.leg_rate,
                 cur_po_distributions_rec.leg_rate_date,
                 cur_po_distributions_rec.leg_quantity_ordered,
                 cur_po_distributions_rec.org_id,
                 cur_po_distributions_rec.leg_distribution_num,
                 po_distributions_interface_s.nextval,
                 po_line_locations_interface_s.currval, --v1.24 SDP 25/03/2016
                 po_lines_interface_s.currval,
                 po_headers_interface_s.currval);
              commit;
              --FND_FILE.PUT_LINE(FND_FILE.LOG,'DIST NUM LOC'|| cur_po_distributions_rec.leg_distribution_num);
            exception
              when others then
                lv_d_err_flag := 'Y';
                l_err_code    := 'ETN_PO_DISTRIBUTION_INSERT_ERR';
                l_err_msg     := 'Error: Exceptional error while inserting distribution record :  ' ||
                                 sqlerrm;
                log_errors(pin_interface_txn_id    => cur_po_distributions_rec.interface_txn_id,
                           piv_source_table        => 'xxpo_po_distribution_stg',
                           piv_source_column_name  => null,
                           piv_source_column_value => null,
                           piv_source_keyname1     => null,
                           piv_source_keyvalue1    => null,
                           piv_error_type          => 'ERR_INT',
                           piv_error_code          => l_err_code,
                           piv_error_message       => l_err_msg);
            end;
            if lv_d_err_flag = 'N' then
              update xxpo_po_distribution_stg
                 set process_flag      = 'P',
                     error_type        = null,
                     request_id        = g_request_id,
                     last_updated_date = sysdate,
                     last_updated_by   = g_last_updated_by,
                     last_update_login = g_last_update_login
               where interface_txn_id =
                     cur_po_distributions_rec.interface_txn_id;
              --FND_FILE.PUT_LINE(FND_FILE.LOG,'UPDATE DIST NUM LOC'|| cur_po_distributions_rec.interface_txn_id);
              commit;
              xxetn_debug_pkg.add_debug('Record id ' ||
                                        cur_po_distributions_rec.interface_txn_id ||
                                        'is successfully processed');
            else
              g_retcode := 1;
              update xxpo_po_distribution_stg
                 set process_flag      = 'E',
                     error_type        = 'ERR_INT',
                     request_id        = g_request_id,
                     last_updated_date = sysdate,
                     last_updated_by   = g_last_updated_by,
                     last_update_login = g_last_update_login
               where interface_txn_id =
                     cur_po_distributions_rec.interface_txn_id;
              --FND_FILE.PUT_LINE(FND_FILE.LOG,'ERROR DIST NUM LOC'|| cur_po_distributions_rec.interface_txn_id);
              commit;
              xxetn_debug_pkg.add_debug('Record id ' ||
                                        cur_po_distributions_rec.interface_txn_id ||
                                        'is erred out');
            end if;
          end loop; -- dist loop
        end loop; -- line loc loop
      end loop; -- lines loop
    end loop; -- headers loop
    commit;
  exception
    when others then
      print_log_message(sqlerrm || ', ' || sqlcode ||
                        'Unexpected Error while executing Purchase Order Conversion procedure');
      g_retcode := 2;
      print_log_message('Error : Backtace : ' ||
                        dbms_utility.format_error_backtrace);
  end conversion;

  /*
    +======================================================================================+
    |PROCEDURE NAME : po_tie_back                                                          |
    |DESCRIPTION   : This procedure tie back interface records with staging table records  |
    +======================================================================================+
  */
  procedure po_tie_back(x_ou_errbuf        out nocopy varchar2,
                        x_ou_retcode       out nocopy number,
                        p_dummy            in varchar2,
                        pin_batch_id       in number,
                        pin_request_id     in number,
                        piv_operating_unit in varchar2) is
    lv_int_head_id  number;
    lv_err_msg_code varchar2(30);
    lv_err_msg      varchar2(4000);
    l_debug_err     varchar2(2000);
    pov_ret_stats   varchar2(100);
    pov_err_msg     varchar2(1000);
    cursor cur_tie_back_head(p_batch number) is
      select /*+ INDEX (xphs XXPO_PO_HEADER_STG_N5) */
       xphs.interface_txn_id,
       phi.interface_header_id,
       xphs.leg_document_num,
       xphs.leg_operating_unit_name,
       xphs.leg_source_system,
       phi.process_code
        from xxpo_po_header_stg xphs, po_headers_interface phi
       where xphs.batch_id = phi.batch_id
         and xphs.batch_id = p_batch
         and xphs.leg_operating_unit_name =
             nvl(piv_operating_unit, xphs.leg_operating_unit_name)
         and phi.interface_source_code = 'CONVERSION'
         and xphs.leg_document_num = phi.document_num
         and xphs.org_id = phi.org_id
         and phi.request_id = pin_request_id
         and xphs.process_flag = 'P';
    cursor cur_head_err(p_int_head_id number) is
      select interface_header_id,
             error_message_name,
             error_message,
             column_name,
             column_value
        from po_interface_errors
       where table_name = 'PO_HEADERS_INTERFACE'
         and interface_header_id = p_int_head_id;
    cursor cur_tie_back_line(p_batch       number,
                             p_int_head_id number,
                             p_leg_doc_num varchar2,
                             p_opr_name    varchar2,
                             p_leg_source  varchar2) is
       --Hint Alterned in the below Cursor--24/04--

      select /*+ INDEX (xpls XXPO_PO_LINE_SHIPMENT_STG_N6) */
       xpls.interface_txn_id,
       pli.interface_header_id,
       pli.interface_line_id,
       xpls.leg_line_num,
       xpls.leg_po_line_id, --- v1.24 SDP
       xpls.leg_shipment_num,
       pli.process_code
        from xxpo_po_line_shipment_stg   xpls,
             po_lines_interface          pli,
             po_line_locations_interface plli --- v1.24 SDP
       where xpls.batch_id = p_batch
         and xpls.leg_document_num = p_leg_doc_num
         and xpls.leg_operating_unit_name = p_opr_name
         and xpls.leg_source_system = p_leg_source
         and pli.interface_header_id = p_int_head_id
         and xpls.leg_line_num = pli.line_num
         and xpls.leg_shipment_num = plli.shipment_num --- v1.24 SDP  -- If Performance Issue we can remove this
         and pli.interface_line_id = plli.interface_line_id --- v1.24 SDP  --- If Performance Issue we can remove this
         and xpls.process_flag = 'P';
    cursor cur_line_err(p_int_head_id number, p_int_line_id number) is
      select interface_line_id,
             error_message_name,
             error_message,
             column_name,
             column_value
        from po_interface_errors
       where table_name = 'PO_LINES_INTERFACE'
         and interface_header_id = p_int_head_id
         and interface_line_id = p_int_line_id;
    cursor cur_tie_back_dist(p_batch        number,
                             p_int_head_id  number,
                             p_leg_doc_num  varchar2,
                             p_opr_name     varchar2,
                             p_leg_source   varchar2,
                             p_int_line_id  number,
                             p_line_num     number, --- v1.24 SDP -- here value passed will be leg_po_line_id instead of leg_po_line_num
                             p_shipment_num number) is
      select /*+ INDEX (xpls XXPO_PO_DISTRIBUTION_STG_N6) */
       xpls.interface_txn_id,
       pli.interface_header_id,
       pli.interface_line_id,
       pli.interface_distribution_id,
       xpls.leg_line_num,
       xpls.leg_shipment_num,
       xpls.leg_distribution_num,
       pli.process_code
        from xxpo_po_distribution_stg xpls, po_distributions_interface pli
       where xpls.batch_id = p_batch
         and xpls.leg_document_num = p_leg_doc_num
         and xpls.leg_operating_unit_name = p_opr_name
         and xpls.leg_source_system = p_leg_source
            --and xpls.leg_line_num = p_line_num
         and xpls.leg_po_line_id = p_line_num --- v1.24 SDP -- here value passed will be leg_po_line_id instead of leg_po_line_num
         and xpls.leg_shipment_num = p_shipment_num
         and pli.interface_header_id = p_int_head_id
         and pli.interface_line_id = p_int_line_id
         and xpls.leg_distribution_num = pli.distribution_num
         and xpls.process_flag = 'P';
    cursor cur_dist_err(p_int_head_id number,
                        p_int_line_id number,
                        p_int_dist_id number) is
      select interface_line_id,
             error_message_name,
             error_message,
             column_name,
             column_value
        from po_interface_errors
       where table_name = 'PO_DISTRIBUTIONS_INTERFACE'
         and interface_header_id = p_int_head_id
         and interface_line_id = p_int_line_id
         and interface_distribution_id = p_int_dist_id;
  begin
    -------------added 24/02 --SM-----------
    declare
      stmt101 varchar2(2000);
    begin
      stmt101 := 'CREATE INDEX XXCONV. XXPO_PO_LINE_SHIPMENT_STG_N6 ON XXCONV.XXPO_PO_LINE_SHIPMENT_STG(LEG_DOCUMENT_NUM,LEG_OPERATING_UNIT_NAME)
                          TABLESPACE XXCONVD';
      execute immediate stmt101;
    exception
      when others then
        fnd_file.put_line(fnd_file.log,
                          'INDEX XXCONV.XXPO_PO_LINE_SHIPMENT_STG_N6 NOT CREATED');
    end;
    begin
      dbms_stats.gather_table_stats(ownname          => 'XXCONV',
                                    tabname          => 'XXPO_PO_LINE_SHIPMENT_STG',
                                    cascade          => true,
                                    estimate_percent => dbms_stats.auto_sample_size,
                                    degree           => dbms_stats.default_degree);
    exception
      when others then
        fnd_file.put_line(fnd_file.log, 'DBMS STATS DID NOT WORK');
    end;
    -------------added 24/02 --SM-----------
    print_log_message('Tie Back Starts at: ' ||
                      to_char(sysdate, 'DD-MON-YYYY HH24:MI:SS'));
    print_log_message('+ Start of Tie Back + ' || pin_batch_id);
    -- Initialize debug procedure
    xxetn_debug_pkg.initialize_debug(pov_err_msg      => l_debug_err,
                                     piv_program_name => 'Purchase_Order_Conv');
    xxetn_common_error_pkg.g_batch_id := pin_batch_id;
    -- batch id
    xxetn_common_error_pkg.g_run_seq_id := xxetn_run_sequences_s.nextval;
    -- run sequence id
    for cur_tie_back_head_rec in cur_tie_back_head(pin_batch_id) loop
      lv_err_msg_code := null;
      lv_err_msg      := null;
      if cur_tie_back_head_rec.process_code = 'ACCEPTED' then
        update xxpo_po_header_stg
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
      elsif cur_tie_back_head_rec.process_code = 'REJECTED' then
        update xxpo_po_header_stg
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
        for cur_head_err_rec in cur_head_err(cur_tie_back_head_rec.interface_header_id) loop
          lv_err_msg := lv_err_msg || '||' ||
                        cur_head_err_rec.error_message;
        end loop;
        if lv_err_msg is not null then
          lv_err_msg_code := 'PO_INTERFACE_ERROR_RECORD';
          log_errors(pin_interface_txn_id    => cur_tie_back_head_rec.interface_txn_id,
                     piv_source_table        => 'xxpo_po_header_stg',
                     piv_source_column_name  => 'leg_document_num',
                     piv_source_column_value => cur_tie_back_head_rec.leg_document_num,
                     piv_source_keyname1     => null,
                     piv_source_keyvalue1    => null,
                     piv_error_type          => 'ERR_INT',
                     piv_error_code          => lv_err_msg_code,
                     piv_error_message       => lv_err_msg);
        else
          lv_err_msg_code := 'PO_INTERFACE_DEPENDENT_ERR';
          lv_err_msg      := 'Record erred out as one of the dependents Header/line/distribution erred out.';
          log_errors(pin_interface_txn_id    => cur_tie_back_head_rec.interface_txn_id,
                     piv_source_table        => 'xxpo_po_header_stg',
                     piv_source_column_name  => 'leg_document_num',
                     piv_source_column_value => cur_tie_back_head_rec.leg_document_num,
                     piv_source_keyname1     => null,
                     piv_source_keyvalue1    => null,
                     piv_error_type          => 'ERR_INT',
                     piv_error_code          => lv_err_msg_code,
                     piv_error_message       => lv_err_msg);
        end if;
      end if;
      for cur_tie_back_line_rec in cur_tie_back_line(pin_batch_id,
                                                     cur_tie_back_head_rec.interface_header_id,
                                                     cur_tie_back_head_rec.leg_document_num,
                                                     cur_tie_back_head_rec.leg_operating_unit_name,
                                                     cur_tie_back_head_rec.leg_source_system) loop
        lv_err_msg_code := null;
        lv_err_msg      := null;
        if cur_tie_back_line_rec.process_code = 'ACCEPTED' then
          update xxpo_po_line_shipment_stg
             set process_flag      = 'C',
                 error_type        = null,
                 request_id        = g_request_id,
                 last_updated_date = sysdate,
                 last_updated_by   = g_last_updated_by,
                 last_update_login = g_last_update_login
           where interface_txn_id = cur_tie_back_line_rec.interface_txn_id;
          xxetn_debug_pkg.add_debug('Record id ' ||
                                    cur_tie_back_line_rec.interface_txn_id ||
                                    'is successfully completed');
        elsif cur_tie_back_line_rec.process_code = 'REJECTED' then
          update xxpo_po_line_shipment_stg
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
          for cur_line_err_rec in cur_line_err(cur_tie_back_head_rec.interface_header_id,
                                               cur_tie_back_line_rec.interface_line_id) loop
            lv_err_msg := lv_err_msg || '||' ||
                          cur_line_err_rec.error_message;
          end loop;
          if lv_err_msg is not null then
            lv_err_msg_code := 'PO_INTERFACE_ERROR_RECORD';
            log_errors(pin_interface_txn_id    => cur_tie_back_line_rec.interface_txn_id,
                       piv_source_table        => 'xxpo_po_line_shipment_stg',
                       piv_source_column_name  => 'leg_document_num',
                       piv_source_column_value => cur_tie_back_head_rec.leg_document_num,
                       piv_source_keyname1     => null,
                       piv_source_keyvalue1    => null,
                       piv_error_type          => 'ERR_INT',
                       piv_error_code          => lv_err_msg_code,
                       piv_error_message       => lv_err_msg);
          else
            lv_err_msg_code := 'PO_INTERFACE_DEPENDENT_ERR';
            lv_err_msg      := 'Record erred out as one of the dependents Header/line/distribution erred out.';
            log_errors(pin_interface_txn_id    => cur_tie_back_line_rec.interface_txn_id,
                       piv_source_table        => 'xxpo_po_line_shipment_stg',
                       piv_source_column_name  => 'leg_document_num',
                       piv_source_column_value => cur_tie_back_head_rec.leg_document_num,
                       piv_source_keyname1     => null,
                       piv_source_keyvalue1    => null,
                       piv_error_type          => 'ERR_INT',
                       piv_error_code          => lv_err_msg_code,
                       piv_error_message       => lv_err_msg);
          end if;
        end if;
        /*
        FND_FILE.PUT_LINE(FND_FILE.OUTPUT,'-----');
        FND_FILE.PUT_LINE(FND_FILE.OUTPUT,cur_tie_back_head_rec.interface_header_id||'-'||cur_tie_back_head_rec.leg_document_num||'-'||cur_tie_back_head_rec.leg_operating_unit_name
                          ||'-'||cur_tie_back_head_rec.leg_source_system||'-'||cur_tie_back_line_rec.interface_line_id||'-'||cur_tie_back_line_rec.leg_po_line_id||'-'||
                          cur_tie_back_line_rec.leg_shipment_num);
        */
        for cur_tie_back_dist_rec in cur_tie_back_dist(pin_batch_id,
                                                       cur_tie_back_head_rec.interface_header_id,
                                                       cur_tie_back_head_rec.leg_document_num,
                                                       cur_tie_back_head_rec.leg_operating_unit_name,
                                                       cur_tie_back_head_rec.leg_source_system,
                                                       cur_tie_back_line_rec.interface_line_id,
                                                       --cur_tie_back_line_rec.leg_line_num,
                                                       cur_tie_back_line_rec.leg_po_line_id, --- v1.24 SDP -- here value passed will be leg_po_line_id instead of leg_po_line_num
                                                       cur_tie_back_line_rec.leg_shipment_num) loop
          lv_err_msg_code := null;
          lv_err_msg      := null;
          /*
          FND_FILE.PUT_LINE(FND_FILE.OUTPUT,'-----');
          FND_FILE.PUT_LINE(FND_FILE.OUTPUT,cur_tie_back_dist_rec.interface_txn_id);
          */
          if cur_tie_back_dist_rec.process_code = 'ACCEPTED' then
            update xxpo_po_distribution_stg
               set process_flag      = 'C',
                   error_type        = null,
                   request_id        = g_request_id,
                   last_updated_date = sysdate,
                   last_updated_by   = g_last_updated_by,
                   last_update_login = g_last_update_login
             where interface_txn_id =
                   cur_tie_back_dist_rec.interface_txn_id;
            xxetn_debug_pkg.add_debug('Record id ' ||
                                      cur_tie_back_dist_rec.interface_txn_id ||
                                      'is successfully completed');
          elsif cur_tie_back_dist_rec.process_code = 'REJECTED' then
            update xxpo_po_distribution_stg
               set process_flag      = 'E',
                   error_type        = 'ERR_INT',
                   request_id        = g_request_id,
                   last_updated_date = sysdate,
                   last_updated_by   = g_last_updated_by,
                   last_update_login = g_last_update_login
             where interface_txn_id =
                   cur_tie_back_dist_rec.interface_txn_id;
            xxetn_debug_pkg.add_debug('Record id ' ||
                                      cur_tie_back_dist_rec.interface_txn_id ||
                                      'is erred out');
            for cur_dist_err_rec in cur_dist_err(cur_tie_back_head_rec.interface_header_id,
                                                 cur_tie_back_line_rec.interface_line_id,
                                                 cur_tie_back_dist_rec.interface_distribution_id) loop
              lv_err_msg := lv_err_msg || '||' ||
                            cur_dist_err_rec.error_message;
            end loop;
            if lv_err_msg is not null then
              lv_err_msg_code := 'PO_INTERFACE_ERROR_RECORD';
              log_errors(pin_interface_txn_id    => cur_tie_back_dist_rec.interface_txn_id,
                         piv_source_table        => 'xxpo_po_distribution_stg',
                         piv_source_column_name  => 'leg_document_num',
                         piv_source_column_value => cur_tie_back_head_rec.leg_document_num,
                         piv_source_keyname1     => null,
                         piv_source_keyvalue1    => null,
                         piv_error_type          => 'ERR_INT',
                         piv_error_code          => lv_err_msg_code,
                         piv_error_message       => lv_err_msg);
            else
              lv_err_msg_code := 'PO_INTERFACE_DEPENDENT_ERR';
              lv_err_msg      := 'Record erred out as one of the dependents Header/line/distribution erred out.';
              log_errors(pin_interface_txn_id    => cur_tie_back_dist_rec.interface_txn_id,
                         piv_source_table        => 'xxpo_po_distribution_stg',
                         piv_source_column_name  => 'leg_document_num',
                         piv_source_column_value => cur_tie_back_head_rec.leg_document_num,
                         piv_source_keyname1     => null,
                         piv_source_keyvalue1    => null,
                         piv_error_type          => 'ERR_INT',
                         piv_error_code          => lv_err_msg_code,
                         piv_error_message       => lv_err_msg);
            end if;
          end if;
        end loop;
      end loop;
    end loop;
    -- call once to dump pending error records which are less than profile value.
    xxetn_common_error_pkg.add_error(pov_return_status => pov_ret_stats,
                                     pov_error_msg     => pov_err_msg,
                                     pi_source_tab     => g_tab);
    commit;
    -------------added 24/02 --SM-----------
    declare
      stmt103 varchar2(2000);
    begin
      stmt103 := 'DROP INDEX XXCONV.XXPO_PO_LINE_SHIPMENT_STG_N6';
      execute immediate stmt103;
    exception
      when others then
        fnd_file.put_line(fnd_file.log,
                          'DROP INDEX FAILED XXCONV.XXPO_PO_LINE_SHIPMENT_STG_N6');
    end;
    -------------added 24/02 --SM-----------
  exception
    when others then
      fnd_file.put_line(fnd_file.log,
                        ' Unexpected  Error in po_tie_back procedure' ||
                        sqlerrm);
      x_ou_retcode := 2;
  end po_tie_back;

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
                 piv_operating_unit  in varchar2) is
    --l_ret_status   VARCHAR2(100);
    --l_err_msg      VARCHAR2(2500);
    l_debug_err varchar2(2000);
    --l_count NUMBER;
    pov_ret_stats varchar2(100);
    pov_err_msg   varchar2(1000);
    --ln_count_corp_valid     NUMBER;
    --ln_count_corp_comp      NUMBER;
    l_warn_excep exception;
    l_load_ret_h_stats varchar2(1) := 'S';
    l_load_ret_l_stats varchar2(1) := 'S';
    l_load_ret_d_stats varchar2(1) := 'S';
    l_h_load_err_msg   varchar2(1000);
    l_l_load_err_msg   varchar2(1000);
    l_d_load_err_msg   varchar2(1000);
  begin
    g_run_mode           := pin_run_mode;
    g_batch_id           := pin_batch_id;
    g_process_records    := piv_process_records;
    g_leg_operating_unit := piv_operating_unit;
    -- Initialize debug procedure
    xxetn_debug_pkg.initialize_debug(pov_err_msg      => l_debug_err,
                                     piv_program_name => 'Purchase_Order_Conv');
    xxetn_debug_pkg.add_debug(piv_debug_msg => 'Initialized Debug');
    xxetn_debug_pkg.add_debug('Program Parameters');
    xxetn_debug_pkg.add_debug('---------------------------------------------');
    xxetn_debug_pkg.add_debug('Run Mode            : ' || pin_run_mode);
    xxetn_debug_pkg.add_debug('Batch ID            : ' || pin_batch_id);
    xxetn_debug_pkg.add_debug('Reprocess records     : ' ||
                              piv_process_records);
    xxetn_debug_pkg.add_debug('Legacy Operating Unit : ' ||
                              piv_operating_unit);
    print_log_message('Program Parameters');
    print_log_message('---------------------------------------------');
    print_log_message('Run Mode            : ' || pin_run_mode);
    print_log_message('Batch ID            : ' || pin_batch_id);
    print_log_message('Reprocess records     : ' || piv_process_records);
    print_log_message('Legacy Operating Unit : ' || piv_operating_unit);
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
      print_log_message('Calling procedure load_line');
      load_line(pov_ret_stats => l_load_ret_l_stats,
                pov_err_msg   => l_l_load_err_msg);
      if l_load_ret_l_stats <> 'S' then
        print_log_message('Error in procedure load_line' ||
                          l_l_load_err_msg);
        print_log_message('');
        raise l_warn_excep;
      end if;
      print_log_message('Calling procedure load_distribution');
      load_distribution(pov_ret_stats => l_load_ret_d_stats,
                        pov_err_msg   => l_d_load_err_msg);
      if l_load_ret_d_stats <> 'S' then
        print_log_message('Error in procedure load_distribution' ||
                          l_d_load_err_msg);
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
      validate_po;
      pon_retcode := g_retcode;
    elsif pin_run_mode = 'CONVERSION' then
      if pin_batch_id is not null then
        g_new_run_seq_id := xxetn_run_sequences_s.nextval;
        xxetn_debug_pkg.add_debug(piv_debug_msg => 'Updating run sequence id in conversion mode.');
        print_log_message('Run Sequence ID for Import Mode : ' ||
                          g_new_run_seq_id);
        begin
          xxetn_debug_pkg.add_debug(piv_debug_msg => 'Reprocess updating run sequence id: Headers');
          update xxpo_po_header_stg
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
          xxetn_debug_pkg.add_debug(piv_debug_msg => 'Reprocess updating run sequence id: Lines');
          update xxpo_po_line_shipment_stg
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
            print_log_message('Error : Exception occured while updating run seq id for reprocess Lines ' ||
                              substr(sqlerrm, 1, 150));
        end;
        begin
          xxetn_debug_pkg.add_debug(piv_debug_msg => 'Reprocess updating run sequence id: Distribution');
          update xxpo_po_distribution_stg
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
            print_log_message('Error : Exception occured while updating run seq id for reprocess distribution ' ||
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
    when others then
      pov_errbuf  := 'Error : Main Program Procedure: MAIN encounter error. Reason: ' ||
                     substr(sqlerrm, 1, 150);
      pon_retcode := 2;
      print_log_message('Error : Main Program Procedure: MAIN encounter error. Reason: ' ||
                        substr(sqlerrm, 1, 150));
      print_log_message('Error : Backtace : ' ||
                        dbms_utility.format_error_backtrace);
  end main;

end xxpo_purchase_order_pkg;
/
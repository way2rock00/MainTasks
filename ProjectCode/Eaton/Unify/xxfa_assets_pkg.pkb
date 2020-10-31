CREATE OR REPLACE PACKAGE BODY xxfa_asset_pkg
IS
----------------------------------------------------------------------------------------------------------------------------------
--    Owner        : EATON CORPORATION...
--    Application  : Fixed Assets..
--    Schema       : APPS
--    Compile AS   : APPS
--    File Name    : xxfa_asset_pkg.pkb
--    Date         : 06-March-14
--    Author       : Rohit Devadiga
--    Description  : Package Body for Fixed Asset Conversion.
--    Version      : $ETNHeader: /CCSTORE/ccweb/C9916816/C9916816_view_3/vobs/FA_TOP/xxfa/12.0.0/install/xxfa_assets_pkg.pkb /main/21 19-Apr-2016 02:51:51 C9916816  $
--
--    Change History
  --  ===============================================================================================================================
  --    v1.0      Rohit Devadiga    06-March-14     Initial Creation
  --    V1.1      Rohit Devadiga      21-Aug-2014     Changes done for performance tuning.
  --    V1.2      Harjinder Singh     10-Mar-2015     Changes done for fetching r12 book type code from the lookup
  --    V1.3      Tushar Sharma     11-Jun-2015     Changes done for reflecting complete code combination in error table for error out records.
  --    V1.4      Tushar Sharma     10-Aug-2015     New procedure Assign_batch_id_load added to generate batch id at the LOAD time
  --    V1.5      Tushar Sharma     10-Oct-2015     Changes made to implement PMC 318073, 318074 and 241277
  --    v1.6      Harjinder Singh   30-Oct-2015    Included Rate logic and changed Import Tax and Import Corporate procedure
  --    v1.7      Harjinder Singh   11-Feb-2016    Poland Changes  PMC 349366
  --    v1.8      Tushar Sharma     25-Feb-2016    process_flag not getting updated for records in exception
  --    v1.9      Tushar Sharma     06-Jun-2016    Changes as per PMC#387982 to comment amortization date
  --    v1.10     Tushar Sharma     04-Jan-2017    Changes to execute gather schema to enhance performance
  --    =============================================================================================================================
  g_request_id      NUMBER DEFAULT fnd_global.conc_request_id;
  g_prog_appl_id    NUMBER DEFAULT fnd_global.prog_appl_id;
  g_conc_program_id NUMBER DEFAULT fnd_global.conc_program_id;
  g_user_id         NUMBER DEFAULT fnd_global.user_id;
  g_login_id        NUMBER DEFAULT fnd_global.login_id;
  -- g_org_id            NUMBER DEFAULT fnd_global.org_id;
  g_created_by         NUMBER := apps.fnd_global.user_id;
  g_last_updated_by    NUMBER := apps.fnd_global.user_id;
  g_last_update_login  NUMBER := apps.fnd_global.login_id;
  g_tab                xxetn_common_error_pkg.g_source_tab_type;
  g_err_tab_limit      NUMBER DEFAULT fnd_profile.VALUE('ETN_FND_ERROR_TAB_LIMIT');
  g_period_date        VARCHAR2(15) DEFAULT fnd_profile.VALUE('XXFA_CONVERSION_PERIOD');
  g_txn_sub_type       VARCHAR2(9) := 'AMORTIZED';
  g_err_cnt            NUMBER DEFAULT 1;
  g_tot_header_count   NUMBER;
  g_tot_lines_count    NUMBER;
  g_suc_count_head     NUMBER;
  g_suc_count_line     NUMBER;
  g_fail_header_count  NUMBER;
  g_fail_lines_count   NUMBER;
  g_fail_count_head    NUMBER;
  g_fail_count_line    NUMBER;
  g_source_issc        VARCHAR2(240) := 'ISSC';
  g_source_fsc         VARCHAR2(240) := 'FSC';
  g_asset_type         VARCHAR2(11) := 'CAPITALIZED';
  g_cat_lookup_others  VARCHAR2(240) := 'ETN_FA_CATEGORY_MAP';
  g_cat_lookup         VARCHAR2(240)  := 'ETN_FA_CATEGORY_MAP_POLAND';
  g_cat_lookup_local   VARCHAR2(240) := 'ETN_FA_CATEGORY_MAP_LOCAL_ONLY'; -----v1.5
  g_cat_lookup_us      VARCHAR2(240) := 'ETN_FA_CATEGORY_MAP_US_ONLY'; -----v1.5
  g_poland_book_lookup VARCHAR2(240) := 'XXETN_POLAND_11I_BOOKS';
  --xxetn_common_error_pkg.g_batch_id := 11;   -- batch id
  --xxetn_common_error_pkg.g_run_seq_id := 11; -- run sequence id
  g_failed_count    NUMBER DEFAULT 0;
  g_total_count     NUMBER;
  g_loaded_count    NUMBER;
  g_run_mode        VARCHAR2(100);
  g_batch_id        NUMBER;
  g_process_records VARCHAR2(100);
  g_new_run_seq_id  NUMBER;
  g_ret_code        NUMBER;
  g_direction     CONSTANT VARCHAR2(240) := 'LEGACY-TO-R12';
  g_coa_error     CONSTANT VARCHAR2(30) := 'Error';
  g_coa_processed CONSTANT VARCHAR2(30) := 'Processed';
  g_book_type_code    VARCHAR2(240);
  g_err_batch_flag    VARCHAR2(1);
  g_sep               VARCHAR2(1) := '-';
  g_sep_p          VARCHAR2(10) := '||';
  g_lookup_r12_type   VARCHAR2(100) := 'XXETN_MAP_FA_BOOKS';
  g_cat_lookup_poland VARCHAR2(100) := 'ETN_FA_CATEGORY_MAP_POLAND';

  --g_err_msg             VARCHAR2 (2500);

  -- ========================
  -- Procedure: print_log_message
  -- =============================================================================
  --   This procedure is used to write message to log file.
  -- =============================================================================
  PROCEDURE print_log_message(piv_message IN VARCHAR2) IS
  BEGIN
    IF NVL(g_request_id, 0) > 0 THEN
      fnd_file.put_line(fnd_file.LOG, piv_message);
    END IF;
  END print_log_message;

  --
  -- ========================
  -- Procedure: LOG_ERRORS
  -- =============================================================================
  --   This procedure is used log error
  -- =============================================================================
  --
  PROCEDURE log_errors(pin_interface_txn_id    IN NUMBER DEFAULT NULL,
                       piv_source_table        IN VARCHAR2 DEFAULT NULL,
                       piv_source_column_name  IN VARCHAR2 DEFAULT NULL,
                       piv_source_column_value IN VARCHAR2 DEFAULT NULL,
                       piv_error_type          IN VARCHAR2 DEFAULT NULL,
                       piv_source_keyname1     IN VARCHAR2 DEFAULT NULL,
                       piv_source_keyvalue1    IN VARCHAR2 DEFAULT NULL,
                       piv_source_keyname2     IN VARCHAR2 DEFAULT NULL,
                       piv_source_keyvalue2    IN VARCHAR2 DEFAULT NULL,
                       piv_error_code          IN VARCHAR2,
                       piv_error_message       IN VARCHAR2) IS
    pov_ret_stats VARCHAR2(100);
    pov_err_msg   VARCHAR2(1000);
  BEGIN
    pov_ret_stats := 'S';
    pov_err_msg   := NULL;
    -- Assigning error values to current table record
    g_tab(g_err_cnt).interface_staging_id := pin_interface_txn_id;
    g_tab(g_err_cnt).source_table := piv_source_table;
    g_tab(g_err_cnt).source_column_name := piv_source_column_name;
    g_tab(g_err_cnt).source_column_value := piv_source_column_value;
    g_tab(g_err_cnt).source_keyname1 := piv_source_keyname1;
    g_tab(g_err_cnt).source_keyvalue1 := piv_source_keyvalue1;
    g_tab(g_err_cnt).source_keyname2 := piv_source_keyname2;
    g_tab(g_err_cnt).source_keyvalue2 := piv_source_keyvalue2;
    g_tab(g_err_cnt).ERROR_TYPE := piv_error_type;
    g_tab(g_err_cnt).ERROR_CODE := piv_error_code;
    g_tab(g_err_cnt).error_message := piv_error_message;

    IF g_err_cnt >= g_err_tab_limit THEN
      -- if Table Type Error Count exceeds limit
      g_err_cnt := 1;
      xxetn_common_error_pkg.add_error(pov_return_status => pov_ret_stats,
                                       pov_error_msg     => pov_err_msg,
                                       pi_source_tab     => g_tab);
      -- Flushing PLSQL Table
      g_tab.DELETE;
    ELSE
      g_err_cnt := g_err_cnt + 1;
      -- else increment Table Type Error Count
    END IF;
  EXCEPTION
    WHEN OTHERS THEN
      print_log_message('Error occured while logging error: ' ||
                        pov_ret_stats || ': ' || SQLERRM || ': ' ||
                        pov_err_msg);
  END log_errors;

  -- ========================
  -- FUNCTION : get_lookup_value    ---1.1
  -- =============================================================================
  --   This procedure is used to get lookup value.
  -- =============================================================================
  /*   FUNCTION get_lookup_value (
     p_lookup_type     IN   VARCHAR2,
     p_source_table    IN   VARCHAR2,
     p_source_column   IN   VARCHAR2,
     p_source_value    IN   VARCHAR2,
     p_asset_columns   IN   VARCHAR2,
     p_asset_value     IN   VARCHAR2,
     p_book_value      IN   VARCHAR2
  )
     RETURN NUMBER
  IS
     ln_code   NUMBER;
  BEGIN
     ln_code := 0;

     SELECT COUNT (1)
       INTO ln_code
       FROM fa_lookups flv
      WHERE flv.lookup_type = p_lookup_type
        AND flv.lookup_code = p_source_value
        AND flv.enabled_flag = 'Y'
        AND TRUNC (SYSDATE) BETWEEN NVL (flv.start_date_active,
                                         TRUNC (SYSDATE)
                                        )
                                AND NVL (flv.end_date_active,
                                         TRUNC (SYSDATE));

     RETURN ln_code;

     IF ln_code = 0
     THEN
        g_ret_code := 1;
        log_errors (pin_interface_txn_id         => NULL,
                    piv_source_table             => p_source_table,
                    piv_source_column_name       => p_source_column,
                    piv_source_column_value      => p_source_value,
                    piv_source_keyname1          => p_asset_columns,
                    piv_source_keyvalue1         => p_asset_value,
                    piv_source_keyname2          => 'leg_book_type_code',
                    piv_source_keyvalue2         => p_book_value,
                    piv_error_type               => 'VAL_ERR',
                    piv_error_code               => 'ETN_FA_INVALID_LOOKUP_VAL',
                    piv_error_message            =>    'Error : '
                                                    || p_lookup_type
                                                    || ' code not valid : '
                   );
     END IF;
  EXCEPTION
     WHEN OTHERS
     THEN
        g_ret_code := 1;
        log_errors
           (pin_interface_txn_id         => NULL,
            piv_source_table             => p_source_table,
            piv_source_column_name       => NULL,
            piv_source_column_value      => NULL,
            piv_source_keyname1          => p_asset_columns,
            piv_source_keyvalue1         => p_asset_value,
            piv_source_keyname2          => 'leg_book_type_code',
            piv_source_keyvalue2         => p_book_value,
            piv_error_type               => 'VAL_ERR',
            piv_error_code               => 'ETN_FA_INVALID_PROC',
            piv_error_message            =>    'Error : Exception occured while fetching lookup value for : '
                                            || p_lookup_type
                                            || SUBSTR (SQLERRM, 1, 240)
           );
  END get_lookup_value; */

  --
  -- ========================
  -- Procedure: VALIDATE_ACCOUNTS
  -- =============================================================================
  --   This procedure validates all
  --   the account related information
  -- =============================================================================
  PROCEDURE validate_accounts(p_in_leg_id IN NUMBER,
                              p_in_seg1   IN VARCHAR2,
                              p_in_seg2   IN VARCHAR2,
                              p_in_seg3   IN VARCHAR2,
                              p_in_seg4   IN VARCHAR2,
                              p_in_seg5   IN VARCHAR2,
                              p_in_seg6   IN VARCHAR2,
                              p_in_seg7   IN VARCHAR2,
                              x_out_acc   OUT xxetn_common_pkg.g_rec_type,
                              x_out_ccid  OUT NUMBER) IS
    l_in_rec     xxetn_coa_mapping_pkg.g_coa_rec_type := NULL;
    x_ccid       NUMBER := NULL;
    x_out_rec    xxetn_coa_mapping_pkg.g_coa_rec_type := NULL;
    x_msg        VARCHAR2(4000) := NULL;
    x_status     VARCHAR2(50) := NULL;
    l_in_seg_rec xxetn_common_pkg.g_rec_type := NULL;
    x_err        VARCHAR2(4000) := NULL;
  BEGIN
    x_out_acc  := NULL;
    x_out_ccid := NULL;
    xxetn_debug_pkg.add_debug(piv_debug_msg => 'Validate accounts procedure called ');
    l_in_rec.segment1 := p_in_seg1;
    l_in_rec.segment2 := p_in_seg2;
    l_in_rec.segment3 := p_in_seg3;
    l_in_rec.segment4 := p_in_seg4;
    l_in_rec.segment5 := p_in_seg5;
    l_in_rec.segment6 := p_in_seg6;
    l_in_rec.segment7 := p_in_seg7;
    xxetn_coa_mapping_pkg.get_code_combination(g_direction,
                                               NULL,
                                               SYSDATE,
                                               l_in_rec,
                                               x_out_rec,
                                               x_status,
                                               x_msg);

    IF x_status = g_coa_processed THEN
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

      IF x_err IS NULL THEN
        x_out_ccid := x_ccid;
        xxetn_debug_pkg.add_debug(piv_debug_msg => 'Account information successfully derived ');
      ELSE
        FOR r_org_ref_err_rec IN (SELECT interface_txn_id
                                    FROM xxfa_corp_asset_stg xis
                                   WHERE leg_cc_segment1 = p_in_seg1
                                     AND leg_cc_segment2 = p_in_seg2
                                     AND leg_cc_segment3 = p_in_seg3
                                     AND leg_cc_segment4 = p_in_seg4
                                     AND leg_cc_segment5 = p_in_seg5
                                     AND leg_cc_segment6 = p_in_seg6
                                     AND leg_cc_segment7 = p_in_seg7
                                     AND batch_id = g_batch_id
                                     AND run_sequence_id = g_new_run_seq_id) LOOP
          log_errors(pin_interface_txn_id    => NULL,
                     piv_source_table        => 'XXFA_CORP_ASSET_STG',
                     piv_source_column_name  => 'Code Combination',
                     piv_source_column_value => p_in_seg1 || '.' ||
                                                p_in_seg2 || '.' ||
                                                p_in_seg3 || '.' ||
                                                p_in_seg4 || '.' ||
                                                p_in_seg5 || '.' ||
                                                p_in_seg6 || '.' ||
                                                p_in_seg7, --added as per version v1.3
                     piv_source_keyname1     => 'interface_txn_id',
                     piv_source_keyvalue1    => r_org_ref_err_rec.interface_txn_id,
                     piv_error_type          => 'VAL_ERR',
                     piv_error_code          => 'ETN_INVALID_ACCOUNT_INFO',
                     piv_error_message       => 'Error : Following error in COA transformation : ' ||
                                                x_err);
        END LOOP;
      END IF;
    ELSIF x_status = g_coa_error THEN
      FOR r_org_ref_err_rec IN (SELECT interface_txn_id
                                  FROM xxfa_corp_asset_stg xis
                                 WHERE leg_cc_segment1 = p_in_seg1
                                   AND leg_cc_segment2 = p_in_seg2
                                   AND leg_cc_segment3 = p_in_seg3
                                   AND leg_cc_segment4 = p_in_seg4
                                   AND leg_cc_segment5 = p_in_seg5
                                   AND leg_cc_segment6 = p_in_seg6
                                   AND leg_cc_segment7 = p_in_seg7
                                   AND batch_id = g_batch_id
                                   AND run_sequence_id = g_new_run_seq_id) LOOP
        log_errors(pin_interface_txn_id    => NULL,
                   piv_source_table        => 'XXFA_CORP_ASSET_STG',
                   piv_source_column_name  => 'Code Combination',
                   piv_source_column_value => p_in_seg1 || '.' || p_in_seg2 || '.' ||
                                              p_in_seg3 || '.' || p_in_seg4 || '.' ||
                                              p_in_seg5 || '.' || p_in_seg6 || '.' ||
                                              p_in_seg7, --added as per version v1.3
                   piv_source_keyname1     => 'interface_txn_id',
                   piv_source_keyvalue1    => r_org_ref_err_rec.interface_txn_id,
                   piv_error_type          => 'VAL_ERR',
                   piv_error_code          => 'ETN_INVALID_ACCOUNT_INFO',
                   piv_error_message       => 'Error : Following error in COA transformation : ' ||
                                              x_msg);
      END LOOP;
    END IF;

    xxetn_debug_pkg.add_debug(piv_debug_msg => 'Validate accounts procedure ends ');
  EXCEPTION
    WHEN OTHERS THEN
      g_ret_code := 1;
      log_errors(pin_interface_txn_id    => NULL,
                 piv_source_table        => 'XXFA_CORP_ASSET_STG',
                 piv_source_column_name  => 'Code Combination',
                 piv_source_column_value => p_in_seg1 || '.' || p_in_seg2 || '.' ||
                                            p_in_seg3 || '.' || p_in_seg4 || '.' ||
                                            p_in_seg5 || '.' || p_in_seg6 || '.' ||
                                            p_in_seg7, --added as per version v1.3
                 piv_source_keyname1     => 'interface_txn_id',
                 piv_source_keyvalue1    => p_in_leg_id,
                 piv_error_type          => 'VAL_ERR',
                 piv_error_code          => 'ETN_FA_INVALID_PROC',
                 piv_error_message       => 'Exception Error while deriving accounting information. ');
  END validate_accounts;

  --
  -- ========================
  -- Procedure: LOAD_CORP_BOOK
  -- =============================================================================
  --   This procedure is used to load data from extraction into staging table
  -- =============================================================================
  PROCEDURE load_corp_book(pov_ret_stats OUT NOCOPY VARCHAR2,
                           pov_err_msg   OUT NOCOPY VARCHAR2) IS
    TYPE leg_corp_rec IS RECORD(
      interface_txn_id               xxfa_corp_asset_ext_r12.interface_txn_id%TYPE,
      leg_asset_id                   xxfa_corp_asset_ext_r12.leg_asset_id%TYPE,
      asset_id                       xxfa_corp_asset_ext_r12.asset_id%TYPE,
      leg_asset_number               xxfa_corp_asset_ext_r12.leg_asset_number%TYPE,
      leg_asset_key_segment1         xxfa_corp_asset_ext_r12.leg_asset_key_segment1%TYPE,
      leg_asset_key_segment2         xxfa_corp_asset_ext_r12.leg_asset_key_segment2%TYPE,
      leg_asset_key_segment3         xxfa_corp_asset_ext_r12.leg_asset_key_segment3%TYPE,
      leg_asset_key_segment4         xxfa_corp_asset_ext_r12.leg_asset_key_segment4%TYPE,
      leg_asset_key_segment5         xxfa_corp_asset_ext_r12.leg_asset_key_segment5%TYPE,
      asset_key_segment1             xxfa_corp_asset_ext_r12.asset_key_segment1%TYPE,
      asset_key_segment2             xxfa_corp_asset_ext_r12.asset_key_segment2%TYPE,
      asset_key_segment3             xxfa_corp_asset_ext_r12.asset_key_segment3%TYPE,
      asset_key_segment4             xxfa_corp_asset_ext_r12.asset_key_segment4%TYPE,
      asset_key_segment5             xxfa_corp_asset_ext_r12.asset_key_segment5%TYPE,
      asset_key_ccid                 xxfa_corp_asset_ext_r12.asset_key_ccid%TYPE,
      leg_current_units              xxfa_corp_asset_ext_r12.leg_current_units%TYPE,
      leg_asset_type                 xxfa_corp_asset_ext_r12.leg_asset_type%TYPE,
      leg_tag_number                 xxfa_corp_asset_ext_r12.leg_tag_number%TYPE,
      leg_asset_cat_segment1         xxfa_corp_asset_ext_r12.leg_asset_cat_segment1%TYPE,
      leg_asset_cat_segment2         xxfa_corp_asset_ext_r12.leg_asset_cat_segment2%TYPE,
      asset_cat_segment1             xxfa_corp_asset_ext_r12.asset_cat_segment1%TYPE,
      asset_cat_segment2             xxfa_corp_asset_ext_r12.asset_cat_segment2%TYPE,
      asset_category_id              xxfa_corp_asset_ext_r12.asset_category_id%TYPE,
      leg_parent_asset_number        xxfa_corp_asset_ext_r12.leg_parent_asset_number%TYPE,
      parent_asset_id                xxfa_corp_asset_ext_r12.parent_asset_id%TYPE,
      leg_manufacturer_name          xxfa_corp_asset_ext_r12.leg_manufacturer_name%TYPE,
      leg_serial_number              xxfa_corp_asset_ext_r12.leg_serial_number%TYPE,
      leg_model_number               xxfa_corp_asset_ext_r12.leg_model_number%TYPE,
      leg_property_type_code         xxfa_corp_asset_ext_r12.leg_property_type_code%TYPE,
      leg_property_1245_1250_code    xxfa_corp_asset_ext_r12.leg_property_1245_1250_code%TYPE,
      leg_in_use_flag                xxfa_corp_asset_ext_r12.leg_in_use_flag%TYPE,
      leg_owned_leased               xxfa_corp_asset_ext_r12.leg_owned_leased%TYPE,
      leg_new_used                   xxfa_corp_asset_ext_r12.leg_new_used%TYPE,
      leg_unit_adjustment_flag       xxfa_corp_asset_ext_r12.leg_unit_adjustment_flag%TYPE,
      leg_add_cost_je_flag           xxfa_corp_asset_ext_r12.leg_add_cost_je_flag%TYPE,
      leg_adtn_attribute1            xxfa_corp_asset_ext_r12.leg_adtn_attribute1%TYPE,
      leg_adtn_attribute2            xxfa_corp_asset_ext_r12.leg_adtn_attribute2%TYPE,
      leg_adtn_attribute3            xxfa_corp_asset_ext_r12.leg_adtn_attribute3%TYPE,
      leg_adtn_attribute4            xxfa_corp_asset_ext_r12.leg_adtn_attribute4%TYPE,
      leg_adtn_attribute5            xxfa_corp_asset_ext_r12.leg_adtn_attribute5%TYPE,
      leg_adtn_attribute6            xxfa_corp_asset_ext_r12.leg_adtn_attribute6%TYPE,
      leg_adtn_attribute7            xxfa_corp_asset_ext_r12.leg_adtn_attribute7%TYPE,
      leg_adtn_attribute8            xxfa_corp_asset_ext_r12.leg_adtn_attribute8%TYPE,
      leg_adtn_attribute9            xxfa_corp_asset_ext_r12.leg_adtn_attribute9%TYPE,
      leg_adtn_attribute10           xxfa_corp_asset_ext_r12.leg_adtn_attribute10%TYPE,
      leg_adtn_attribute11           xxfa_corp_asset_ext_r12.leg_adtn_attribute11%TYPE,
      leg_adtn_attribute12           xxfa_corp_asset_ext_r12.leg_adtn_attribute12%TYPE,
      leg_adtn_attribute13           xxfa_corp_asset_ext_r12.leg_adtn_attribute13%TYPE,
      leg_adtn_attribute14           xxfa_corp_asset_ext_r12.leg_adtn_attribute14%TYPE,
      leg_adtn_attribute15           xxfa_corp_asset_ext_r12.leg_adtn_attribute15%TYPE,
      leg_adtn_attribute16           xxfa_corp_asset_ext_r12.leg_adtn_attribute16%TYPE,
      leg_adtn_attribute17           xxfa_corp_asset_ext_r12.leg_adtn_attribute17%TYPE,
      leg_adtn_attribute18           xxfa_corp_asset_ext_r12.leg_adtn_attribute18%TYPE,
      leg_adtn_attribute19           xxfa_corp_asset_ext_r12.leg_adtn_attribute19%TYPE,
      leg_adtn_attribute20           xxfa_corp_asset_ext_r12.leg_adtn_attribute20%TYPE,
      leg_adtn_attribute21           xxfa_corp_asset_ext_r12.leg_adtn_attribute21%TYPE,
      leg_adtn_attribute22           xxfa_corp_asset_ext_r12.leg_adtn_attribute22%TYPE,
      leg_adtn_attribute23           xxfa_corp_asset_ext_r12.leg_adtn_attribute23%TYPE,
      leg_adtn_attribute24           xxfa_corp_asset_ext_r12.leg_adtn_attribute24%TYPE,
      leg_adtn_attribute25           xxfa_corp_asset_ext_r12.leg_adtn_attribute25%TYPE,
      leg_adtn_attribute26           xxfa_corp_asset_ext_r12.leg_adtn_attribute26%TYPE,
      leg_adtn_attribute27           xxfa_corp_asset_ext_r12.leg_adtn_attribute27%TYPE,
      leg_adtn_attribute28           xxfa_corp_asset_ext_r12.leg_adtn_attribute28%TYPE,
      leg_adtn_attribute29           xxfa_corp_asset_ext_r12.leg_adtn_attribute29%TYPE,
      leg_adtn_attribute30           xxfa_corp_asset_ext_r12.leg_adtn_attribute30%TYPE,
      leg_adtn_attr_category_code    xxfa_corp_asset_ext_r12.leg_adtn_attr_category_code%TYPE,
      leg_adtn_context               xxfa_corp_asset_ext_r12.leg_adtn_context%TYPE,
      adtn_attribute1                xxfa_corp_asset_ext_r12.adtn_attribute1%TYPE,
      adtn_attribute2                xxfa_corp_asset_ext_r12.adtn_attribute2%TYPE,
      adtn_attribute3                xxfa_corp_asset_ext_r12.adtn_attribute3%TYPE,
      adtn_attribute4                xxfa_corp_asset_ext_r12.adtn_attribute4%TYPE,
      adtn_attribute5                xxfa_corp_asset_ext_r12.adtn_attribute5%TYPE,
      adtn_attribute6                xxfa_corp_asset_ext_r12.adtn_attribute6%TYPE,
      adtn_attribute7                xxfa_corp_asset_ext_r12.adtn_attribute7%TYPE,
      adtn_attribute8                xxfa_corp_asset_ext_r12.adtn_attribute8%TYPE,
      adtn_attribute9                xxfa_corp_asset_ext_r12.adtn_attribute9%TYPE,
      adtn_attribute10               xxfa_corp_asset_ext_r12.adtn_attribute10%TYPE,
      adtn_attribute11               xxfa_corp_asset_ext_r12.adtn_attribute11%TYPE,
      adtn_attribute12               xxfa_corp_asset_ext_r12.adtn_attribute12%TYPE,
      adtn_attribute13               xxfa_corp_asset_ext_r12.adtn_attribute13%TYPE,
      adtn_attribute14               xxfa_corp_asset_ext_r12.adtn_attribute14%TYPE,
      adtn_attribute15               xxfa_corp_asset_ext_r12.adtn_attribute15%TYPE,
      adtn_attribute16               xxfa_corp_asset_ext_r12.adtn_attribute16%TYPE,
      adtn_attribute17               xxfa_corp_asset_ext_r12.adtn_attribute17%TYPE,
      adtn_attribute18               xxfa_corp_asset_ext_r12.adtn_attribute18%TYPE,
      adtn_attribute19               xxfa_corp_asset_ext_r12.adtn_attribute19%TYPE,
      adtn_attribute20               xxfa_corp_asset_ext_r12.adtn_attribute20%TYPE,
      adtn_attribute21               xxfa_corp_asset_ext_r12.adtn_attribute21%TYPE,
      adtn_attribute22               xxfa_corp_asset_ext_r12.adtn_attribute22%TYPE,
      adtn_attribute23               xxfa_corp_asset_ext_r12.adtn_attribute23%TYPE,
      adtn_attribute24               xxfa_corp_asset_ext_r12.adtn_attribute24%TYPE,
      adtn_attribute25               xxfa_corp_asset_ext_r12.adtn_attribute25%TYPE,
      adtn_attribute26               xxfa_corp_asset_ext_r12.adtn_attribute26%TYPE,
      adtn_attribute27               xxfa_corp_asset_ext_r12.adtn_attribute27%TYPE,
      adtn_attribute28               xxfa_corp_asset_ext_r12.adtn_attribute28%TYPE,
      adtn_attribute29               xxfa_corp_asset_ext_r12.adtn_attribute29%TYPE,
      adtn_attribute30               xxfa_corp_asset_ext_r12.adtn_attribute30%TYPE,
      adtn_attribute_category_code   xxfa_corp_asset_ext_r12.adtn_attribute_category_code%TYPE,
      adtn_context                   xxfa_corp_asset_ext_r12.adtn_context%TYPE,
      leg_inventorial                xxfa_corp_asset_ext_r12.leg_inventorial%TYPE,
      leg_commitment                 xxfa_corp_asset_ext_r12.leg_commitment%TYPE,
      leg_investment_law             xxfa_corp_asset_ext_r12.leg_investment_law%TYPE,
      leg_adtn_global_attribute1     xxfa_corp_asset_ext_r12.leg_adtn_global_attribute1%TYPE,
      leg_adtn_global_attribute2     xxfa_corp_asset_ext_r12.leg_adtn_global_attribute2%TYPE,
      leg_adtn_global_attribute3     xxfa_corp_asset_ext_r12.leg_adtn_global_attribute3%TYPE,
      leg_adtn_global_attribute4     xxfa_corp_asset_ext_r12.leg_adtn_global_attribute4%TYPE,
      leg_adtn_global_attribute5     xxfa_corp_asset_ext_r12.leg_adtn_global_attribute5%TYPE,
      leg_adtn_global_attribute6     xxfa_corp_asset_ext_r12.leg_adtn_global_attribute6%TYPE,
      leg_adtn_global_attribute7     xxfa_corp_asset_ext_r12.leg_adtn_global_attribute7%TYPE,
      leg_adtn_global_attribute8     xxfa_corp_asset_ext_r12.leg_adtn_global_attribute8%TYPE,
      leg_adtn_global_attribute9     xxfa_corp_asset_ext_r12.leg_adtn_global_attribute9%TYPE,
      leg_adtn_global_attribute10    xxfa_corp_asset_ext_r12.leg_adtn_global_attribute10%TYPE,
      leg_adtn_global_attribute11    xxfa_corp_asset_ext_r12.leg_adtn_global_attribute11%TYPE,
      leg_adtn_global_attribute12    xxfa_corp_asset_ext_r12.leg_adtn_global_attribute12%TYPE,
      leg_adtn_global_attribute13    xxfa_corp_asset_ext_r12.leg_adtn_global_attribute13%TYPE,
      leg_adtn_global_attribute14    xxfa_corp_asset_ext_r12.leg_adtn_global_attribute14%TYPE,
      leg_adtn_global_attribute15    xxfa_corp_asset_ext_r12.leg_adtn_global_attribute15%TYPE,
      leg_adtn_global_attribute16    xxfa_corp_asset_ext_r12.leg_adtn_global_attribute16%TYPE,
      leg_adtn_global_attribute17    xxfa_corp_asset_ext_r12.leg_adtn_global_attribute17%TYPE,
      leg_adtn_global_attribute18    xxfa_corp_asset_ext_r12.leg_adtn_global_attribute18%TYPE,
      leg_adtn_global_attribute19    xxfa_corp_asset_ext_r12.leg_adtn_global_attribute19%TYPE,
      leg_adtn_global_attribute20    xxfa_corp_asset_ext_r12.leg_adtn_global_attribute20%TYPE,
      leg_adtn_global_attr_category  xxfa_corp_asset_ext_r12.leg_adtn_global_attr_category%TYPE,
      adtn_global_attribute1         xxfa_corp_asset_ext_r12.adtn_global_attribute1%TYPE,
      adtn_global_attribute2         xxfa_corp_asset_ext_r12.adtn_global_attribute2%TYPE,
      adtn_global_attribute3         xxfa_corp_asset_ext_r12.adtn_global_attribute3%TYPE,
      adtn_global_attribute4         xxfa_corp_asset_ext_r12.adtn_global_attribute4%TYPE,
      adtn_global_attribute5         xxfa_corp_asset_ext_r12.adtn_global_attribute5%TYPE,
      adtn_global_attribute6         xxfa_corp_asset_ext_r12.adtn_global_attribute6%TYPE,
      adtn_global_attribute7         xxfa_corp_asset_ext_r12.adtn_global_attribute7%TYPE,
      adtn_global_attribute8         xxfa_corp_asset_ext_r12.adtn_global_attribute8%TYPE,
      adtn_global_attribute9         xxfa_corp_asset_ext_r12.adtn_global_attribute9%TYPE,
      adtn_global_attribute10        xxfa_corp_asset_ext_r12.adtn_global_attribute10%TYPE,
      adtn_global_attribute11        xxfa_corp_asset_ext_r12.adtn_global_attribute11%TYPE,
      adtn_global_attribute12        xxfa_corp_asset_ext_r12.adtn_global_attribute12%TYPE,
      adtn_global_attribute13        xxfa_corp_asset_ext_r12.adtn_global_attribute13%TYPE,
      adtn_global_attribute14        xxfa_corp_asset_ext_r12.adtn_global_attribute14%TYPE,
      adtn_global_attribute15        xxfa_corp_asset_ext_r12.adtn_global_attribute15%TYPE,
      adtn_global_attribute16        xxfa_corp_asset_ext_r12.adtn_global_attribute16%TYPE,
      adtn_global_attribute17        xxfa_corp_asset_ext_r12.adtn_global_attribute17%TYPE,
      adtn_global_attribute18        xxfa_corp_asset_ext_r12.adtn_global_attribute18%TYPE,
      adtn_global_attribute19        xxfa_corp_asset_ext_r12.adtn_global_attribute19%TYPE,
      adtn_global_attribute20        xxfa_corp_asset_ext_r12.adtn_global_attribute20%TYPE,
      adtn_global_attribute_category xxfa_corp_asset_ext_r12.adtn_global_attribute_category%TYPE,
      leg_book_type_code             xxfa_corp_asset_ext_r12.leg_book_type_code%TYPE,
      book_type_code                 xxfa_corp_asset_ext_r12.book_type_code%TYPE,
      leg_date_placed_in_service     xxfa_corp_asset_ext_r12.leg_date_placed_in_service%TYPE,
      leg_deprn_start_date           xxfa_corp_asset_ext_r12.leg_deprn_start_date%TYPE,
      leg_deprn_method_code          xxfa_corp_asset_ext_r12.leg_deprn_method_code%TYPE,
      leg_life_in_months             xxfa_corp_asset_ext_r12.leg_life_in_months%TYPE,
      leg_rate_adjustment_factor     xxfa_corp_asset_ext_r12.leg_rate_adjustment_factor%TYPE,
      leg_adjusted_cost              xxfa_corp_asset_ext_r12.leg_adjusted_cost%TYPE,
      leg_cost                       xxfa_corp_asset_ext_r12.leg_cost%TYPE,
      leg_original_cost              xxfa_corp_asset_ext_r12.leg_original_cost%TYPE,
      leg_salvage_value              xxfa_corp_asset_ext_r12.leg_salvage_value%TYPE,
      leg_prorate_convention_code    xxfa_corp_asset_ext_r12.leg_prorate_convention_code%TYPE,
      leg_prorate_date               xxfa_corp_asset_ext_r12.leg_prorate_date%TYPE,
      leg_cost_change_flag           xxfa_corp_asset_ext_r12.leg_cost_change_flag%TYPE,
      leg_adjustment_required_status xxfa_corp_asset_ext_r12.leg_adjustment_required_status%TYPE,
      leg_capitalize_flag            xxfa_corp_asset_ext_r12.leg_capitalize_flag%TYPE,
      leg_retirement_pending_flag    xxfa_corp_asset_ext_r12.leg_retirement_pending_flag%TYPE,
      leg_depreciate_flag            xxfa_corp_asset_ext_r12.leg_depreciate_flag%TYPE,
      leg_basic_rate                 xxfa_corp_asset_ext_r12.leg_basic_rate%TYPE,
      leg_adjusted_rate              xxfa_corp_asset_ext_r12.leg_adjusted_rate%TYPE,
      leg_bonus_rule                 xxfa_corp_asset_ext_r12.leg_bonus_rule%TYPE,
      leg_ceiling_name               xxfa_corp_asset_ext_r12.leg_ceiling_name%TYPE,
      leg_recoverable_cost           xxfa_corp_asset_ext_r12.leg_recoverable_cost%TYPE,
      leg_cap_period_name            xxfa_corp_asset_ext_r12.leg_cap_period_name%TYPE,
      period_counter_capitalized     xxfa_corp_asset_ext_r12.period_counter_capitalized%TYPE,
      leg_dep_period_name            xxfa_corp_asset_ext_r12.leg_dep_period_name%TYPE,
      period_counter_fully_reserved  xxfa_corp_asset_ext_r12.period_counter_fully_reserved%TYPE,
      leg_unrevalued_cost            xxfa_corp_asset_ext_r12.leg_unrevalued_cost%TYPE,
      leg_annual_deprn_rounding_flag xxfa_corp_asset_ext_r12.leg_annual_deprn_rounding_flag%TYPE,
      leg_percent_salvage_value      xxfa_corp_asset_ext_r12.leg_percent_salvage_value%TYPE,
      leg_allowed_deprn_limit        xxfa_corp_asset_ext_r12.leg_allowed_deprn_limit%TYPE,
      leg_allowed_deprn_limit_amount xxfa_corp_asset_ext_r12.leg_allowed_deprn_limit_amount%TYPE,
      leg_salvage_type               xxfa_corp_asset_ext_r12.leg_salvage_type%TYPE,
      leg_deprn_limit_type           xxfa_corp_asset_ext_r12.leg_deprn_limit_type%TYPE,
      leg_period_counter             xxfa_corp_asset_ext_r12.leg_period_counter%TYPE,
      leg_deprn_source_code          xxfa_corp_asset_ext_r12.leg_deprn_source_code%TYPE,
      leg_deprn_run_date             xxfa_corp_asset_ext_r12.leg_deprn_run_date%TYPE,
      leg_deprn_amount               xxfa_corp_asset_ext_r12.leg_deprn_amount%TYPE,
      leg_ytd_deprn                  xxfa_corp_asset_ext_r12.leg_ytd_deprn%TYPE,
      leg_deprn_reserve              xxfa_corp_asset_ext_r12.leg_deprn_reserve%TYPE,
      leg_description                xxfa_corp_asset_ext_r12.leg_description%TYPE,
      leg_transaction_type_code      xxfa_corp_asset_ext_r12.leg_transaction_type_code%TYPE,
      leg_transaction_date_entered   xxfa_corp_asset_ext_r12.leg_transaction_date_entered%TYPE,
      leg_transaction_subtype        xxfa_corp_asset_ext_r12.leg_transaction_subtype%TYPE,
      leg_amortization_start_date    xxfa_corp_asset_ext_r12.leg_amortization_start_date%TYPE,
      leg_cc_segment1                xxfa_corp_asset_ext_r12.leg_cc_segment1%TYPE,
      leg_cc_segment2                xxfa_corp_asset_ext_r12.leg_cc_segment2%TYPE,
      leg_cc_segment3                xxfa_corp_asset_ext_r12.leg_cc_segment3%TYPE,
      leg_cc_segment4                xxfa_corp_asset_ext_r12.leg_cc_segment4%TYPE,
      leg_cc_segment5                xxfa_corp_asset_ext_r12.leg_cc_segment5%TYPE,
      leg_cc_segment6                xxfa_corp_asset_ext_r12.leg_cc_segment6%TYPE,
      leg_cc_segment7                xxfa_corp_asset_ext_r12.leg_cc_segment7%TYPE,
      leg_cc_segment8                xxfa_corp_asset_ext_r12.leg_cc_segment8%TYPE,
      leg_cc_segment9                xxfa_corp_asset_ext_r12.leg_cc_segment9%TYPE,
      leg_cc_segment10               xxfa_corp_asset_ext_r12.leg_cc_segment10%TYPE,
      cc_segment1                    xxfa_corp_asset_ext_r12.cc_segment1%TYPE,
      cc_segment2                    xxfa_corp_asset_ext_r12.cc_segment2%TYPE,
      cc_segment3                    xxfa_corp_asset_ext_r12.cc_segment3%TYPE,
      cc_segment4                    xxfa_corp_asset_ext_r12.cc_segment4%TYPE,
      cc_segment5                    xxfa_corp_asset_ext_r12.cc_segment5%TYPE,
      cc_segment6                    xxfa_corp_asset_ext_r12.cc_segment6%TYPE,
      cc_segment7                    xxfa_corp_asset_ext_r12.cc_segment7%TYPE,
      cc_segment8                    xxfa_corp_asset_ext_r12.cc_segment8%TYPE,
      cc_segment9                    xxfa_corp_asset_ext_r12.cc_segment9%TYPE,
      cc_segment10                   xxfa_corp_asset_ext_r12.cc_segment10%TYPE,
      cc_segment11                   xxfa_corp_asset_ext_r12.cc_segment11%TYPE,
      cc_segment12                   xxfa_corp_asset_ext_r12.cc_segment12%TYPE,
      cc_segment13                   xxfa_corp_asset_ext_r12.cc_segment13%TYPE,
      cc_segment14                   xxfa_corp_asset_ext_r12.cc_segment14%TYPE,
      cc_segment15                   xxfa_corp_asset_ext_r12.cc_segment15%TYPE,
      acct_combination_id            xxfa_corp_asset_ext_r12.acct_combination_id%TYPE,
      leg_books_global_attribute1    xxfa_corp_asset_ext_r12.leg_books_global_attribute1%TYPE,
      leg_books_global_attribute2    xxfa_corp_asset_ext_r12.leg_books_global_attribute2%TYPE,
      leg_books_global_attribute3    xxfa_corp_asset_ext_r12.leg_books_global_attribute3%TYPE,
      leg_books_global_attribute4    xxfa_corp_asset_ext_r12.leg_books_global_attribute4%TYPE,
      leg_books_global_attribute5    xxfa_corp_asset_ext_r12.leg_books_global_attribute5%TYPE,
      leg_books_global_attribute6    xxfa_corp_asset_ext_r12.leg_books_global_attribute6%TYPE,
      leg_books_global_attribute7    xxfa_corp_asset_ext_r12.leg_books_global_attribute7%TYPE,
      leg_books_global_attribute8    xxfa_corp_asset_ext_r12.leg_books_global_attribute8%TYPE,
      leg_books_global_attribute9    xxfa_corp_asset_ext_r12.leg_books_global_attribute9%TYPE,
      leg_books_global_attribute10   xxfa_corp_asset_ext_r12.leg_books_global_attribute10%TYPE,
      leg_books_global_attribute11   xxfa_corp_asset_ext_r12.leg_books_global_attribute11%TYPE,
      leg_books_global_attribute12   xxfa_corp_asset_ext_r12.leg_books_global_attribute12%TYPE,
      leg_books_global_attribute13   xxfa_corp_asset_ext_r12.leg_books_global_attribute13%TYPE,
      leg_books_global_attribute14   xxfa_corp_asset_ext_r12.leg_books_global_attribute14%TYPE,
      leg_books_global_attribute15   xxfa_corp_asset_ext_r12.leg_books_global_attribute15%TYPE,
      leg_books_global_attribute16   xxfa_corp_asset_ext_r12.leg_books_global_attribute16%TYPE,
      leg_books_global_attribute17   xxfa_corp_asset_ext_r12.leg_books_global_attribute17%TYPE,
      leg_books_global_attribute18   xxfa_corp_asset_ext_r12.leg_books_global_attribute18%TYPE,
      leg_books_global_attribute19   xxfa_corp_asset_ext_r12.leg_books_global_attribute19%TYPE,
      leg_books_global_attribute20   xxfa_corp_asset_ext_r12.leg_books_global_attribute20%TYPE,
      books_global_attribute1        xxfa_corp_asset_ext_r12.books_global_attribute1%TYPE,
      books_global_attribute2        xxfa_corp_asset_ext_r12.books_global_attribute2%TYPE,
      books_global_attribute3        xxfa_corp_asset_ext_r12.books_global_attribute3%TYPE,
      books_global_attribute4        xxfa_corp_asset_ext_r12.books_global_attribute4%TYPE,
      books_global_attribute5        xxfa_corp_asset_ext_r12.books_global_attribute5%TYPE,
      books_global_attribute6        xxfa_corp_asset_ext_r12.books_global_attribute6%TYPE,
      books_global_attribute7        xxfa_corp_asset_ext_r12.books_global_attribute7%TYPE,
      books_global_attribute8        xxfa_corp_asset_ext_r12.books_global_attribute8%TYPE,
      books_global_attribute9        xxfa_corp_asset_ext_r12.books_global_attribute9%TYPE,
      books_global_attribute10       xxfa_corp_asset_ext_r12.books_global_attribute10%TYPE,
      books_global_attribute11       xxfa_corp_asset_ext_r12.books_global_attribute11%TYPE,
      books_global_attribute12       xxfa_corp_asset_ext_r12.books_global_attribute12%TYPE,
      books_global_attribute13       xxfa_corp_asset_ext_r12.books_global_attribute13%TYPE,
      books_global_attribute14       xxfa_corp_asset_ext_r12.books_global_attribute14%TYPE,
      books_global_attribute15       xxfa_corp_asset_ext_r12.books_global_attribute15%TYPE,
      books_global_attribute16       xxfa_corp_asset_ext_r12.books_global_attribute16%TYPE,
      books_global_attribute17       xxfa_corp_asset_ext_r12.books_global_attribute17%TYPE,
      books_global_attribute18       xxfa_corp_asset_ext_r12.books_global_attribute18%TYPE,
      books_global_attribute19       xxfa_corp_asset_ext_r12.books_global_attribute19%TYPE,
      books_global_attribute20       xxfa_corp_asset_ext_r12.books_global_attribute20%TYPE,
      books_global_attr_category     xxfa_corp_asset_ext_r12.books_global_attr_category%TYPE,
      leg_books_global_attr_category xxfa_corp_asset_ext_r12.leg_books_global_attr_category%TYPE,
      leg_locn_segment1              xxfa_corp_asset_ext_r12.leg_locn_segment1%TYPE,
      leg_locn_segment2              xxfa_corp_asset_ext_r12.leg_locn_segment2%TYPE,
      leg_locn_segment3              xxfa_corp_asset_ext_r12.leg_locn_segment3%TYPE,
      leg_locn_segment4              xxfa_corp_asset_ext_r12.leg_locn_segment4%TYPE,
      leg_locn_segment5              xxfa_corp_asset_ext_r12.leg_locn_segment5%TYPE,
      leg_locn_segment6              xxfa_corp_asset_ext_r12.leg_locn_segment6%TYPE,
      leg_locn_segment7              xxfa_corp_asset_ext_r12.leg_locn_segment7%TYPE,
      locn_segment1                  xxfa_corp_asset_ext_r12.locn_segment1%TYPE,
      locn_segment2                  xxfa_corp_asset_ext_r12.locn_segment2%TYPE,
      locn_segment3                  xxfa_corp_asset_ext_r12.locn_segment3%TYPE,
      locn_segment4                  xxfa_corp_asset_ext_r12.locn_segment4%TYPE,
      locn_segment5                  xxfa_corp_asset_ext_r12.locn_segment5%TYPE,
      locn_segment6                  xxfa_corp_asset_ext_r12.locn_segment6%TYPE,
      locn_segment7                  xxfa_corp_asset_ext_r12.locn_segment7%TYPE,
      location_id                    xxfa_corp_asset_ext_r12.location_id%TYPE,
      batch_id                       xxfa_corp_asset_ext_r12.batch_id%TYPE,
      leg_source_system              xxfa_corp_asset_ext_r12.leg_source_system%TYPE,
      leg_entity                     xxfa_corp_asset_ext_r12.leg_entity%TYPE,
      leg_seq_num                    xxfa_corp_asset_ext_r12.leg_seq_num%TYPE,
      leg_process_flag               xxfa_corp_asset_ext_r12.leg_process_flag%TYPE,
      leg_request_id                 xxfa_corp_asset_ext_r12.leg_request_id%TYPE,
      creation_date                  xxfa_corp_asset_ext_r12.creation_date%TYPE,
      created_by                     xxfa_corp_asset_ext_r12.created_by%TYPE,
      last_updated_date              xxfa_corp_asset_ext_r12.last_updated_date%TYPE,
      last_updated_by                xxfa_corp_asset_ext_r12.last_updated_by%TYPE,
      last_update_login              xxfa_corp_asset_ext_r12.last_update_login%TYPE,
      program_application_id         xxfa_corp_asset_ext_r12.program_application_id%TYPE,
      program_id                     xxfa_corp_asset_ext_r12.program_id%TYPE,
      program_update_date            xxfa_corp_asset_ext_r12.program_update_date%TYPE,
      request_id                     xxfa_corp_asset_ext_r12.request_id%TYPE,
      process_flag                   xxfa_corp_asset_ext_r12.process_flag%TYPE,
      run_sequence_id                xxfa_corp_asset_ext_r12.run_sequence_id%TYPE,
      leg_assigned_emp_number        xxfa_corp_asset_ext_r12.leg_assigned_emp_number%TYPE,
      assigned_emp_number            xxfa_corp_asset_ext_r12.assigned_emp_number%TYPE,
      assigned_emp_id                xxfa_corp_asset_ext_r12.assigned_emp_id%TYPE,
      leg_units_assigned             xxfa_corp_asset_ext_r12.leg_units_assigned%TYPE,
      ERROR_TYPE                     xxfa_corp_asset_ext_r12.ERROR_TYPE%TYPE,
      leg_source_asset_number        xxfa_corp_asset_ext_r12.leg_source_asset_number%TYPE,
      leg_dist_deprn_reserve         xxfa_corp_asset_ext_r12.leg_dist_deprn_reserve%TYPE,
      leg_update_book_class          xxfa_corp_asset_ext_r12.leg_update_book_class%TYPE,
      leg_merge_assets_flag          xxfa_corp_asset_ext_r12.leg_merge_assets_flag%TYPE,
      leg_assets_non_corp_flag       xxfa_corp_asset_ext_r12.leg_assets_non_corp_flag%TYPE,
      leg_assets_retire_flag         xxfa_corp_asset_ext_r12.leg_assets_retire_flag%TYPE,
      leg_assets_ytd_flag            xxfa_corp_asset_ext_r12.leg_assets_ytd_flag%TYPE,
      leg_duplicate_assets_flag      xxfa_corp_asset_ext_r12.leg_duplicate_assets_flag%TYPE,
      leg_duplicate_tag_flag         xxfa_corp_asset_ext_r12.leg_duplicate_tag_flag%TYPE,
      leg_duplicate_asset_book_flag  xxfa_corp_asset_ext_r12.leg_duplicate_asset_book_flag%TYPE);

    TYPE leg_corp_tbl IS TABLE OF leg_corp_rec INDEX BY BINARY_INTEGER;

    l_leg_corp_tbl leg_corp_tbl;
    l_err_record   NUMBER;

    CURSOR cur_leg_corp IS
      SELECT xil.interface_txn_id,
             xil.leg_asset_id,
             xil.asset_id,
             xil.leg_asset_number,
             xil.leg_asset_key_segment1,
             xil.leg_asset_key_segment2,
             xil.leg_asset_key_segment3,
             xil.leg_asset_key_segment4,
             xil.leg_asset_key_segment5,
             xil.asset_key_segment1,
             xil.asset_key_segment2,
             xil.asset_key_segment3,
             xil.asset_key_segment4,
             xil.asset_key_segment5,
             xil.asset_key_ccid,
             xil.leg_current_units,
             xil.leg_asset_type,
             xil.leg_tag_number,
             xil.leg_asset_cat_segment1,
             xil.leg_asset_cat_segment2,
             xil.asset_cat_segment1,
             xil.asset_cat_segment2,
             xil.asset_category_id,
             xil.leg_parent_asset_number,
             xil.parent_asset_id,
             xil.leg_manufacturer_name,
             xil.leg_serial_number,
             xil.leg_model_number,
             xil.leg_property_type_code,
             xil.leg_property_1245_1250_code,
             xil.leg_in_use_flag,
             xil.leg_owned_leased,
             xil.leg_new_used,
             xil.leg_unit_adjustment_flag,
             xil.leg_add_cost_je_flag,
             xil.leg_adtn_attribute1,
             xil.leg_adtn_attribute2,
             xil.leg_adtn_attribute3,
             xil.leg_adtn_attribute4,
             xil.leg_adtn_attribute5,
             xil.leg_adtn_attribute6,
             xil.leg_adtn_attribute7,
             xil.leg_adtn_attribute8,
             xil.leg_adtn_attribute9,
             xil.leg_adtn_attribute10,
             xil.leg_adtn_attribute11,
             xil.leg_adtn_attribute12,
             xil.leg_adtn_attribute13,
             xil.leg_adtn_attribute14,
             xil.leg_adtn_attribute15,
             xil.leg_adtn_attribute16, --1.1
             xil.leg_adtn_attribute17,
             xil.leg_adtn_attribute18,
             xil.leg_adtn_attribute19,
             xil.leg_adtn_attribute20,
             xil.leg_adtn_attribute21,
             xil.leg_adtn_attribute22,
             xil.leg_adtn_attribute23,
             xil.leg_adtn_attribute24,
             xil.leg_adtn_attribute25,
             xil.leg_adtn_attribute26,
             xil.leg_adtn_attribute27,
             xil.leg_adtn_attribute28,
             xil.leg_adtn_attribute29,
             xil.leg_adtn_attribute30,
             xil.leg_adtn_attr_category_code,
             xil.leg_adtn_context,
             xil.adtn_attribute1,
             xil.adtn_attribute2,
             xil.adtn_attribute3,
             xil.adtn_attribute4,
             xil.adtn_attribute5,
             xil.adtn_attribute6,
             xil.adtn_attribute7,
             xil.adtn_attribute8,
             xil.adtn_attribute9,
             xil.adtn_attribute10,
             xil.adtn_attribute11,
             xil.adtn_attribute12,
             xil.adtn_attribute13,
             xil.adtn_attribute14,
             xil.adtn_attribute15,
             xil.adtn_attribute16,
             xil.adtn_attribute17,
             xil.adtn_attribute18,
             xil.adtn_attribute19,
             xil.adtn_attribute20,
             xil.adtn_attribute21,
             xil.adtn_attribute22,
             xil.adtn_attribute23,
             xil.adtn_attribute24,
             xil.adtn_attribute25,
             xil.adtn_attribute26,
             xil.adtn_attribute27,
             xil.adtn_attribute28,
             xil.adtn_attribute29,
             xil.adtn_attribute30,
             xil.adtn_attribute_category_code,
             xil.adtn_context,
             xil.leg_inventorial,
             xil.leg_commitment,
             xil.leg_investment_law,
             xil.leg_adtn_global_attribute1,
             xil.leg_adtn_global_attribute2,
             xil.leg_adtn_global_attribute3,
             xil.leg_adtn_global_attribute4,
             xil.leg_adtn_global_attribute5,
             xil.leg_adtn_global_attribute6,
             xil.leg_adtn_global_attribute7,
             xil.leg_adtn_global_attribute8,
             xil.leg_adtn_global_attribute9,
             xil.leg_adtn_global_attribute10,
             xil.leg_adtn_global_attribute11,
             xil.leg_adtn_global_attribute12,
             xil.leg_adtn_global_attribute13,
             xil.leg_adtn_global_attribute14,
             xil.leg_adtn_global_attribute15,
             xil.leg_adtn_global_attribute16,
             xil.leg_adtn_global_attribute17,
             xil.leg_adtn_global_attribute18,
             xil.leg_adtn_global_attribute19,
             xil.leg_adtn_global_attribute20,
             xil.leg_adtn_global_attr_category,
             xil.adtn_global_attribute1,
             xil.adtn_global_attribute2,
             xil.adtn_global_attribute3,
             xil.adtn_global_attribute4,
             xil.adtn_global_attribute5,
             xil.adtn_global_attribute6,
             xil.adtn_global_attribute7,
             xil.adtn_global_attribute8,
             xil.adtn_global_attribute9,
             xil.adtn_global_attribute10,
             xil.adtn_global_attribute11,
             xil.adtn_global_attribute12,
             xil.adtn_global_attribute13,
             xil.adtn_global_attribute14,
             xil.adtn_global_attribute15,
             xil.adtn_global_attribute16,
             xil.adtn_global_attribute17,
             xil.adtn_global_attribute18,
             xil.adtn_global_attribute19,
             xil.adtn_global_attribute20,
             xil.adtn_global_attribute_category,
             xil.leg_book_type_code, -----Harjinder Singh
             xil.book_type_code,
             xil.leg_date_placed_in_service,
             xil.leg_deprn_start_date,
             xil.leg_deprn_method_code,
             xil.leg_life_in_months,
             xil.leg_rate_adjustment_factor,
             xil.leg_adjusted_cost,
             xil.leg_cost,
             xil.leg_original_cost,
             xil.leg_salvage_value,
             xil.leg_prorate_convention_code,
             xil.leg_prorate_date,
             xil.leg_cost_change_flag,
             xil.leg_adjustment_required_status,
             xil.leg_capitalize_flag,
             xil.leg_retirement_pending_flag,
             xil.leg_depreciate_flag,
             xil.leg_basic_rate,
             xil.leg_adjusted_rate,
             xil.leg_bonus_rule,
             xil.leg_ceiling_name,
             xil.leg_recoverable_cost,
             xil.leg_cap_period_name,
             xil.period_counter_capitalized,
             xil.leg_dep_period_name,
             xil.period_counter_fully_reserved,
             xil.leg_unrevalued_cost,
             xil.leg_annual_deprn_rounding_flag,
             xil.leg_percent_salvage_value,
             xil.leg_allowed_deprn_limit,
             xil.leg_allowed_deprn_limit_amount,
             xil.leg_salvage_type,
             xil.leg_deprn_limit_type,
             xil.leg_period_counter,
             xil.leg_deprn_source_code,
             xil.leg_deprn_run_date,
             xil.leg_deprn_amount,
             xil.leg_ytd_deprn,
             xil.leg_deprn_reserve,
             xil.leg_description,
             xil.leg_transaction_type_code,
             xil.leg_transaction_date_entered,
             xil.leg_transaction_subtype,
             xil.leg_amortization_start_date,
             xil.leg_cc_segment1,
             xil.leg_cc_segment2,
             xil.leg_cc_segment3,
             xil.leg_cc_segment4,
             xil.leg_cc_segment5,
             xil.leg_cc_segment6,
             xil.leg_cc_segment7,
             xil.leg_cc_segment8,
             xil.leg_cc_segment9,
             xil.leg_cc_segment10,
             xil.cc_segment1,
             xil.cc_segment2,
             xil.cc_segment3,
             xil.cc_segment4,
             xil.cc_segment5,
             xil.cc_segment6,
             xil.cc_segment7,
             xil.cc_segment8,
             xil.cc_segment9,
             xil.cc_segment10,
             xil.cc_segment11,
             xil.cc_segment12,
             xil.cc_segment13,
             xil.cc_segment14,
             xil.cc_segment15,
             xil.acct_combination_id,
             xil.leg_books_global_attribute1,
             xil.leg_books_global_attribute2,
             xil.leg_books_global_attribute3,
             xil.leg_books_global_attribute4,
             xil.leg_books_global_attribute5,
             xil.leg_books_global_attribute6,
             xil.leg_books_global_attribute7,
             xil.leg_books_global_attribute8,
             xil.leg_books_global_attribute9,
             xil.leg_books_global_attribute10,
             xil.leg_books_global_attribute11,
             xil.leg_books_global_attribute12,
             xil.leg_books_global_attribute13,
             xil.leg_books_global_attribute14,
             xil.leg_books_global_attribute15,
             xil.leg_books_global_attribute16,
             xil.leg_books_global_attribute17,
             xil.leg_books_global_attribute18,
             xil.leg_books_global_attribute19,
             xil.leg_books_global_attribute20,
             xil.books_global_attribute1,
             xil.books_global_attribute2,
             xil.books_global_attribute3,
             xil.books_global_attribute4,
             xil.books_global_attribute5,
             xil.books_global_attribute6,
             xil.books_global_attribute7,
             xil.books_global_attribute8,
             xil.books_global_attribute9,
             xil.books_global_attribute10,
             xil.books_global_attribute11,
             xil.books_global_attribute12,
             xil.books_global_attribute13,
             xil.books_global_attribute14,
             xil.books_global_attribute15,
             xil.books_global_attribute16,
             xil.books_global_attribute17,
             xil.books_global_attribute18,
             xil.books_global_attribute19,
             xil.books_global_attribute20,
             xil.books_global_attr_category,
             xil.leg_books_global_attr_category,
             xil.leg_locn_segment1,
             xil.leg_locn_segment2,
             xil.leg_locn_segment3,
             xil.leg_locn_segment4,
             xil.leg_locn_segment5,
             xil.leg_locn_segment6,
             xil.leg_locn_segment7,
             xil.locn_segment1,
             xil.locn_segment2,
             xil.locn_segment3,
             xil.locn_segment4,
             xil.locn_segment5,
             xil.locn_segment6,
             xil.locn_segment7,
             xil.location_id,
             xil.batch_id,
             xil.leg_source_system,
             xil.leg_entity,
             xil.leg_seq_num,
             xil.leg_process_flag,
             xil.leg_request_id,
             SYSDATE                            creation_date,
             g_created_by                       created_by,
             SYSDATE                            last_updated_date,
             g_last_updated_by                  last_updated_by,
             g_last_update_login                last_update_login,
             g_prog_appl_id                     program_application_id,
             g_conc_program_id                  program_id,
             SYSDATE                            program_update_date,
             xil.request_id,
             xil.process_flag,
             xil.run_sequence_id,
             xil.leg_assigned_emp_number,
             xil.assigned_emp_number,
             xil.assigned_emp_id,
             xil.leg_units_assigned,
             xil.ERROR_TYPE,
             xil.leg_source_asset_number,
             xil.leg_dist_deprn_reserve,
             xil.leg_update_book_class,
             xil.leg_merge_assets_flag,
             xil.leg_assets_non_corp_flag,
             xil.leg_assets_retire_flag,
             xil.leg_assets_ytd_flag,
             xil.leg_duplicate_assets_flag,
             xil.leg_duplicate_tag_flag,
             xil.leg_duplicate_asset_book_flag
        FROM xxfa_corp_asset_ext_r12 xil
       WHERE xil.leg_process_flag = 'V'
       AND leg_source_system in ('ISSC','FSC')   ---added to get the data only for Global Conversion
         AND NOT EXISTS
       (SELECT 1
                FROM xxfa_corp_asset_stg xis
               WHERE xis.interface_txn_id = xil.interface_txn_id
                      AND leg_source_system in ('ISSC','FSC')  ---added to get the data only for Global Conversion
);
  BEGIN
    pov_ret_stats  := 'S';
    pov_err_msg    := NULL;
    g_total_count  := 0;
    g_failed_count := 0;
    g_loaded_count := 0;

    --Open cursor to extract data from extraction staging table
    OPEN cur_leg_corp;

    LOOP
      xxetn_debug_pkg.add_debug(piv_debug_msg => 'Loading corporate book lines');
      l_leg_corp_tbl.DELETE;

      FETCH cur_leg_corp BULK COLLECT
        INTO l_leg_corp_tbl LIMIT 5000;

      --limit size of Bulk Collect

      -- Get Total Count
      g_total_count := g_total_count + l_leg_corp_tbl.COUNT;
      EXIT WHEN l_leg_corp_tbl.COUNT = 0;

      BEGIN
        -- Bulk Insert into Conversion table
        FORALL indx IN 1 .. l_leg_corp_tbl.COUNT SAVE EXCEPTIONS
          INSERT INTO xxfa_corp_asset_stg VALUES l_leg_corp_tbl (indx);
      EXCEPTION
        WHEN OTHERS THEN
          print_log_message('Errors encountered while loading corp lines data ');

          FOR l_indx_exp IN 1 .. SQL%BULK_EXCEPTIONS.COUNT LOOP
            l_err_record  := l_leg_corp_tbl(SQL%BULK_EXCEPTIONS(l_indx_exp).ERROR_INDEX)
                             .interface_txn_id;
            pov_ret_stats := 'E';
            print_log_message('Record sequence (interface_txn_id) : ' || l_leg_corp_tbl(SQL%BULK_EXCEPTIONS(l_indx_exp).ERROR_INDEX)
                              .interface_txn_id);
            print_log_message('Error Message : ' ||
                              SQLERRM(-SQL%BULK_EXCEPTIONS(l_indx_exp)
                                      .ERROR_CODE));

            -- Updating Leg_process_flag to 'E' for failed records
            UPDATE xxfa_corp_asset_ext_r12 xil
               SET xil.leg_process_flag       = 'E',
                   xil.last_updated_date      = SYSDATE,
                   xil.last_updated_by        = g_last_updated_by,
                   xil.last_update_login      = g_last_update_login,
                   xil.program_id             = g_conc_program_id,
                   xil.program_application_id = g_prog_appl_id,
                   xil.program_update_date    = SYSDATE
             WHERE xil.interface_txn_id = l_err_record
               AND xil.leg_process_flag = 'V'
               AND leg_source_system in ('ISSC','FSC');   ---added to get the data only for Global Conversion;

            g_failed_count := g_failed_count + SQL%ROWCOUNT;
          END LOOP;
      END;
    END LOOP;

    CLOSE cur_leg_corp;

    COMMIT;

    IF g_failed_count > 0 THEN
      print_log_message('Number of Failed Records during load of Corporate book : ' ||
                        g_failed_count);
    END IF;

    ---output
    g_loaded_count := g_total_count - g_failed_count;
    fnd_file.put_line(fnd_file.output,
                      ' Stats for Corporate book table load ');
    fnd_file.put_line(fnd_file.output, '================================');
    fnd_file.put_line(fnd_file.output, 'Total Count : ' || g_total_count);
    fnd_file.put_line(fnd_file.output, 'Loaded Count: ' || g_loaded_count);
    fnd_file.put_line(fnd_file.output, 'Failed Count: ' || g_failed_count);
    fnd_file.put_line(fnd_file.output, '================================');

    -- If records successfully posted to conversion staging table
    IF g_total_count > 0 THEN
      print_log_message('Updating process flag (leg_process_flag) in extraction table for processed records ');

      UPDATE xxfa_corp_asset_ext_r12 xil
         SET xil.leg_process_flag       = 'P',
             xil.last_updated_date      = SYSDATE,
             xil.last_updated_by        = g_last_updated_by,
             xil.last_update_login      = g_last_update_login,
             xil.program_id             = g_conc_program_id,
             xil.program_application_id = g_prog_appl_id,
             xil.program_update_date    = SYSDATE
       WHERE xil.leg_process_flag = 'V'
       AND leg_source_system in ('ISSC','FSC')   ---added to get the data only for Global Conversion
         AND EXISTS
       (SELECT 1
                FROM xxfa_corp_asset_stg xis
               WHERE xis.interface_txn_id = xil.interface_txn_id
               AND leg_source_system in ('ISSC','FSC'))  ; ---added to get the data only for Global Conversion;

      COMMIT;
      -- Either no data to load from extraction table or records already exist in R12 staging table and hence not loaded
    ELSE
      print_log_message('Either no data found for loading from extraction table or records already exist in R12 staging table and hence not loaded ');

      UPDATE xxfa_corp_asset_ext_r12 xil
         SET xil.leg_process_flag       = 'E',
             xil.last_updated_date      = SYSDATE,
             xil.last_updated_by        = g_last_updated_by,
             xil.last_update_login      = g_last_update_login,
             xil.program_id             = g_conc_program_id,
             xil.program_application_id = g_prog_appl_id,
             xil.program_update_date    = SYSDATE
       WHERE xil.leg_process_flag = 'V'
       AND leg_source_system in ('ISSC','FSC')   ---added to get the data only for Global Conversion
         AND EXISTS
       (SELECT 1
                FROM xxfa_corp_asset_stg xis
               WHERE xis.interface_txn_id = xil.interface_txn_id
               AND leg_source_system in ('ISSC','FSC') );  ---added to get the data only for Global Conversion);

      COMMIT;
    END IF;
  EXCEPTION
    WHEN OTHERS THEN
      pov_ret_stats := 'E';
      pov_err_msg   := 'ERROR : Error in load_corp procedure' ||
                       SUBSTR(SQLERRM, 1, 200);
      ROLLBACK;
  END load_corp_book;

  --
  -- ========================
  -- Procedure: LOAD_TAX_BOOK
  -- =============================================================================
  --   This procedure is used to load data from extraction into staging table
  -- =============================================================================
  PROCEDURE load_tax_book(pov_ret_stats OUT NOCOPY VARCHAR2,
                          pov_err_msg   OUT NOCOPY VARCHAR2) IS
    TYPE leg_tax_rec IS RECORD(
      interface_txn_id               xxfa_tax_asset_ext_r12.interface_txn_id%TYPE,
      leg_asset_id                   xxfa_tax_asset_ext_r12.leg_asset_id%TYPE,
      asset_id                       xxfa_tax_asset_ext_r12.asset_id%TYPE,
      leg_asset_number               xxfa_tax_asset_ext_r12.leg_asset_number%TYPE,
      leg_asset_key_segment1         xxfa_tax_asset_ext_r12.leg_asset_key_segment1%TYPE,
      leg_asset_key_segment2         xxfa_tax_asset_ext_r12.leg_asset_key_segment2%TYPE,
      leg_asset_key_segment3         xxfa_tax_asset_ext_r12.leg_asset_key_segment3%TYPE,
      leg_asset_key_segment4         xxfa_tax_asset_ext_r12.leg_asset_key_segment4%TYPE,
      leg_asset_key_segment5         xxfa_tax_asset_ext_r12.leg_asset_key_segment5%TYPE,
      asset_key_segment1             xxfa_tax_asset_ext_r12.asset_key_segment1%TYPE,
      asset_key_segment2             xxfa_tax_asset_ext_r12.asset_key_segment2%TYPE,
      asset_key_segment3             xxfa_tax_asset_ext_r12.asset_key_segment3%TYPE,
      asset_key_segment4             xxfa_tax_asset_ext_r12.asset_key_segment4%TYPE,
      asset_key_segment5             xxfa_tax_asset_ext_r12.asset_key_segment5%TYPE,
      asset_key_ccid                 xxfa_tax_asset_ext_r12.asset_key_ccid%TYPE,
      leg_current_units              xxfa_tax_asset_ext_r12.leg_current_units%TYPE,
      leg_asset_type                 xxfa_tax_asset_ext_r12.leg_asset_type%TYPE,
      leg_tag_number                 xxfa_tax_asset_ext_r12.leg_tag_number%TYPE,
      leg_asset_cat_segment1         xxfa_tax_asset_ext_r12.leg_asset_cat_segment1%TYPE,
      leg_asset_cat_segment2         xxfa_tax_asset_ext_r12.leg_asset_cat_segment2%TYPE,
      asset_cat_segment1             xxfa_tax_asset_ext_r12.asset_cat_segment1%TYPE,
      asset_cat_segment2             xxfa_tax_asset_ext_r12.asset_cat_segment2%TYPE,
      asset_category_id              xxfa_tax_asset_ext_r12.asset_category_id%TYPE,
      leg_parent_asset_number        xxfa_tax_asset_ext_r12.leg_parent_asset_number%TYPE,
      parent_asset_id                xxfa_tax_asset_ext_r12.parent_asset_id%TYPE,
      leg_manufacturer_name          xxfa_tax_asset_ext_r12.leg_manufacturer_name%TYPE,
      leg_serial_number              xxfa_tax_asset_ext_r12.leg_serial_number%TYPE,
      leg_model_number               xxfa_tax_asset_ext_r12.leg_model_number%TYPE,
      leg_property_type_code         xxfa_tax_asset_ext_r12.leg_property_type_code%TYPE,
      leg_property_1245_1250_code    xxfa_tax_asset_ext_r12.leg_property_1245_1250_code%TYPE,
      leg_in_use_flag                xxfa_tax_asset_ext_r12.leg_in_use_flag%TYPE,
      leg_owned_leased               xxfa_tax_asset_ext_r12.leg_owned_leased%TYPE,
      leg_new_used                   xxfa_tax_asset_ext_r12.leg_new_used%TYPE,
      leg_unit_adjustment_flag       xxfa_tax_asset_ext_r12.leg_unit_adjustment_flag%TYPE,
      leg_add_cost_je_flag           xxfa_tax_asset_ext_r12.leg_add_cost_je_flag%TYPE,
      leg_adtn_attribute1            xxfa_tax_asset_ext_r12.leg_adtn_attribute1%TYPE,
      leg_adtn_attribute2            xxfa_tax_asset_ext_r12.leg_adtn_attribute2%TYPE,
      leg_adtn_attribute3            xxfa_tax_asset_ext_r12.leg_adtn_attribute3%TYPE,
      leg_adtn_attribute4            xxfa_tax_asset_ext_r12.leg_adtn_attribute4%TYPE,
      leg_adtn_attribute5            xxfa_tax_asset_ext_r12.leg_adtn_attribute5%TYPE,
      leg_adtn_attribute6            xxfa_tax_asset_ext_r12.leg_adtn_attribute6%TYPE,
      leg_adtn_attribute7            xxfa_tax_asset_ext_r12.leg_adtn_attribute7%TYPE,
      leg_adtn_attribute8            xxfa_tax_asset_ext_r12.leg_adtn_attribute8%TYPE,
      leg_adtn_attribute9            xxfa_tax_asset_ext_r12.leg_adtn_attribute9%TYPE,
      leg_adtn_attribute10           xxfa_tax_asset_ext_r12.leg_adtn_attribute10%TYPE,
      leg_adtn_attribute11           xxfa_tax_asset_ext_r12.leg_adtn_attribute11%TYPE,
      leg_adtn_attribute12           xxfa_tax_asset_ext_r12.leg_adtn_attribute12%TYPE,
      leg_adtn_attribute13           xxfa_tax_asset_ext_r12.leg_adtn_attribute13%TYPE,
      leg_adtn_attribute14           xxfa_tax_asset_ext_r12.leg_adtn_attribute14%TYPE,
      leg_adtn_attribute15           xxfa_tax_asset_ext_r12.leg_adtn_attribute15%TYPE,
      leg_adtn_attribute16           xxfa_tax_asset_ext_r12.leg_adtn_attribute16%TYPE,
      leg_adtn_attribute17           xxfa_tax_asset_ext_r12.leg_adtn_attribute17%TYPE,
      leg_adtn_attribute18           xxfa_tax_asset_ext_r12.leg_adtn_attribute18%TYPE,
      leg_adtn_attribute19           xxfa_tax_asset_ext_r12.leg_adtn_attribute19%TYPE,
      leg_adtn_attribute20           xxfa_tax_asset_ext_r12.leg_adtn_attribute20%TYPE,
      leg_adtn_attribute21           xxfa_tax_asset_ext_r12.leg_adtn_attribute21%TYPE,
      leg_adtn_attribute22           xxfa_tax_asset_ext_r12.leg_adtn_attribute22%TYPE,
      leg_adtn_attribute23           xxfa_tax_asset_ext_r12.leg_adtn_attribute23%TYPE,
      leg_adtn_attribute24           xxfa_tax_asset_ext_r12.leg_adtn_attribute24%TYPE,
      leg_adtn_attribute25           xxfa_tax_asset_ext_r12.leg_adtn_attribute25%TYPE,
      leg_adtn_attribute26           xxfa_tax_asset_ext_r12.leg_adtn_attribute26%TYPE,
      leg_adtn_attribute27           xxfa_tax_asset_ext_r12.leg_adtn_attribute27%TYPE,
      leg_adtn_attribute28           xxfa_tax_asset_ext_r12.leg_adtn_attribute28%TYPE,
      leg_adtn_attribute29           xxfa_tax_asset_ext_r12.leg_adtn_attribute29%TYPE,
      leg_adtn_attribute30           xxfa_tax_asset_ext_r12.leg_adtn_attribute30%TYPE,
      leg_adtn_attr_category_code    xxfa_tax_asset_ext_r12.leg_adtn_attr_category_code%TYPE,
      leg_adtn_context               xxfa_tax_asset_ext_r12.leg_adtn_context%TYPE,
      adtn_attribute1                xxfa_tax_asset_ext_r12.adtn_attribute1%TYPE,
      adtn_attribute2                xxfa_tax_asset_ext_r12.adtn_attribute2%TYPE,
      adtn_attribute3                xxfa_tax_asset_ext_r12.adtn_attribute3%TYPE,
      adtn_attribute4                xxfa_tax_asset_ext_r12.adtn_attribute4%TYPE,
      adtn_attribute5                xxfa_tax_asset_ext_r12.adtn_attribute5%TYPE,
      adtn_attribute6                xxfa_tax_asset_ext_r12.adtn_attribute6%TYPE,
      adtn_attribute7                xxfa_tax_asset_ext_r12.adtn_attribute7%TYPE,
      adtn_attribute8                xxfa_tax_asset_ext_r12.adtn_attribute8%TYPE,
      adtn_attribute9                xxfa_tax_asset_ext_r12.adtn_attribute9%TYPE,
      adtn_attribute10               xxfa_tax_asset_ext_r12.adtn_attribute10%TYPE,
      adtn_attribute11               xxfa_tax_asset_ext_r12.adtn_attribute11%TYPE,
      adtn_attribute12               xxfa_tax_asset_ext_r12.adtn_attribute12%TYPE,
      adtn_attribute13               xxfa_tax_asset_ext_r12.adtn_attribute13%TYPE,
      adtn_attribute14               xxfa_tax_asset_ext_r12.adtn_attribute14%TYPE,
      adtn_attribute15               xxfa_tax_asset_ext_r12.adtn_attribute15%TYPE,
      adtn_attribute16               xxfa_tax_asset_ext_r12.adtn_attribute16%TYPE,
      adtn_attribute17               xxfa_tax_asset_ext_r12.adtn_attribute17%TYPE,
      adtn_attribute18               xxfa_tax_asset_ext_r12.adtn_attribute18%TYPE,
      adtn_attribute19               xxfa_tax_asset_ext_r12.adtn_attribute19%TYPE,
      adtn_attribute20               xxfa_tax_asset_ext_r12.adtn_attribute20%TYPE,
      adtn_attribute21               xxfa_tax_asset_ext_r12.adtn_attribute21%TYPE,
      adtn_attribute22               xxfa_tax_asset_ext_r12.adtn_attribute22%TYPE,
      adtn_attribute23               xxfa_tax_asset_ext_r12.adtn_attribute23%TYPE,
      adtn_attribute24               xxfa_tax_asset_ext_r12.adtn_attribute24%TYPE,
      adtn_attribute25               xxfa_tax_asset_ext_r12.adtn_attribute25%TYPE,
      adtn_attribute26               xxfa_tax_asset_ext_r12.adtn_attribute26%TYPE,
      adtn_attribute27               xxfa_tax_asset_ext_r12.adtn_attribute27%TYPE,
      adtn_attribute28               xxfa_tax_asset_ext_r12.adtn_attribute28%TYPE,
      adtn_attribute29               xxfa_tax_asset_ext_r12.adtn_attribute29%TYPE,
      adtn_attribute30               xxfa_tax_asset_ext_r12.adtn_attribute30%TYPE,
      adtn_attribute_category_code   xxfa_tax_asset_ext_r12.adtn_attribute_category_code%TYPE,
      adtn_context                   xxfa_tax_asset_ext_r12.adtn_context%TYPE,
      leg_inventorial                xxfa_tax_asset_ext_r12.leg_inventorial%TYPE,
      leg_commitment                 xxfa_tax_asset_ext_r12.leg_commitment%TYPE,
      leg_investment_law             xxfa_tax_asset_ext_r12.leg_investment_law%TYPE,
      leg_adtn_global_attribute1     xxfa_tax_asset_ext_r12.leg_adtn_global_attribute1%TYPE,
      leg_adtn_global_attribute2     xxfa_tax_asset_ext_r12.leg_adtn_global_attribute2%TYPE,
      leg_adtn_global_attribute3     xxfa_tax_asset_ext_r12.leg_adtn_global_attribute3%TYPE,
      leg_adtn_global_attribute4     xxfa_tax_asset_ext_r12.leg_adtn_global_attribute4%TYPE,
      leg_adtn_global_attribute5     xxfa_tax_asset_ext_r12.leg_adtn_global_attribute5%TYPE,
      leg_adtn_global_attribute6     xxfa_tax_asset_ext_r12.leg_adtn_global_attribute6%TYPE,
      leg_adtn_global_attribute7     xxfa_tax_asset_ext_r12.leg_adtn_global_attribute7%TYPE,
      leg_adtn_global_attribute8     xxfa_tax_asset_ext_r12.leg_adtn_global_attribute8%TYPE,
      leg_adtn_global_attribute9     xxfa_tax_asset_ext_r12.leg_adtn_global_attribute9%TYPE,
      leg_adtn_global_attribute10    xxfa_tax_asset_ext_r12.leg_adtn_global_attribute10%TYPE,
      leg_adtn_global_attribute11    xxfa_tax_asset_ext_r12.leg_adtn_global_attribute11%TYPE,
      leg_adtn_global_attribute12    xxfa_tax_asset_ext_r12.leg_adtn_global_attribute12%TYPE,
      leg_adtn_global_attribute13    xxfa_tax_asset_ext_r12.leg_adtn_global_attribute13%TYPE,
      leg_adtn_global_attribute14    xxfa_tax_asset_ext_r12.leg_adtn_global_attribute14%TYPE,
      leg_adtn_global_attribute15    xxfa_tax_asset_ext_r12.leg_adtn_global_attribute15%TYPE,
      leg_adtn_global_attribute16    xxfa_tax_asset_ext_r12.leg_adtn_global_attribute16%TYPE,
      leg_adtn_global_attribute17    xxfa_tax_asset_ext_r12.leg_adtn_global_attribute17%TYPE,
      leg_adtn_global_attribute18    xxfa_tax_asset_ext_r12.leg_adtn_global_attribute18%TYPE,
      leg_adtn_global_attribute19    xxfa_tax_asset_ext_r12.leg_adtn_global_attribute19%TYPE,
      leg_adtn_global_attribute20    xxfa_tax_asset_ext_r12.leg_adtn_global_attribute20%TYPE,
      leg_adtn_global_attr_category  xxfa_tax_asset_ext_r12.leg_adtn_global_attr_category%TYPE,
      adtn_global_attribute1         xxfa_tax_asset_ext_r12.adtn_global_attribute1%TYPE,
      adtn_global_attribute2         xxfa_tax_asset_ext_r12.adtn_global_attribute2%TYPE,
      adtn_global_attribute3         xxfa_tax_asset_ext_r12.adtn_global_attribute3%TYPE,
      adtn_global_attribute4         xxfa_tax_asset_ext_r12.adtn_global_attribute4%TYPE,
      adtn_global_attribute5         xxfa_tax_asset_ext_r12.adtn_global_attribute5%TYPE,
      adtn_global_attribute6         xxfa_tax_asset_ext_r12.adtn_global_attribute6%TYPE,
      adtn_global_attribute7         xxfa_tax_asset_ext_r12.adtn_global_attribute7%TYPE,
      adtn_global_attribute8         xxfa_tax_asset_ext_r12.adtn_global_attribute8%TYPE,
      adtn_global_attribute9         xxfa_tax_asset_ext_r12.adtn_global_attribute9%TYPE,
      adtn_global_attribute10        xxfa_tax_asset_ext_r12.adtn_global_attribute10%TYPE,
      adtn_global_attribute11        xxfa_tax_asset_ext_r12.adtn_global_attribute11%TYPE,
      adtn_global_attribute12        xxfa_tax_asset_ext_r12.adtn_global_attribute12%TYPE,
      adtn_global_attribute13        xxfa_tax_asset_ext_r12.adtn_global_attribute13%TYPE,
      adtn_global_attribute14        xxfa_tax_asset_ext_r12.adtn_global_attribute14%TYPE,
      adtn_global_attribute15        xxfa_tax_asset_ext_r12.adtn_global_attribute15%TYPE,
      adtn_global_attribute16        xxfa_tax_asset_ext_r12.adtn_global_attribute16%TYPE,
      adtn_global_attribute17        xxfa_tax_asset_ext_r12.adtn_global_attribute17%TYPE,
      adtn_global_attribute18        xxfa_tax_asset_ext_r12.adtn_global_attribute18%TYPE,
      adtn_global_attribute19        xxfa_tax_asset_ext_r12.adtn_global_attribute19%TYPE,
      adtn_global_attribute20        xxfa_tax_asset_ext_r12.adtn_global_attribute20%TYPE,
      adtn_global_attribute_category xxfa_tax_asset_ext_r12.adtn_global_attribute_category%TYPE,
      leg_book_type_code             xxfa_tax_asset_ext_r12.leg_book_type_code%TYPE,
      book_type_code                 xxfa_tax_asset_ext_r12.book_type_code%TYPE,
      leg_date_placed_in_service     xxfa_tax_asset_ext_r12.leg_date_placed_in_service%TYPE,
      leg_deprn_start_date           xxfa_tax_asset_ext_r12.leg_deprn_start_date%TYPE,
      leg_deprn_method_code          xxfa_tax_asset_ext_r12.leg_deprn_method_code%TYPE,
      leg_life_in_months             xxfa_tax_asset_ext_r12.leg_life_in_months%TYPE,
      leg_rate_adjustment_factor     xxfa_tax_asset_ext_r12.leg_rate_adjustment_factor%TYPE,
      leg_adjusted_cost              xxfa_tax_asset_ext_r12.leg_adjusted_cost%TYPE,
      leg_cost                       xxfa_tax_asset_ext_r12.leg_cost%TYPE,
      leg_original_cost              xxfa_tax_asset_ext_r12.leg_original_cost%TYPE,
      leg_salvage_value              xxfa_tax_asset_ext_r12.leg_salvage_value%TYPE,
      leg_prorate_convention_code    xxfa_tax_asset_ext_r12.leg_prorate_convention_code%TYPE,
      leg_prorate_date               xxfa_tax_asset_ext_r12.leg_prorate_date%TYPE,
      leg_cost_change_flag           xxfa_tax_asset_ext_r12.leg_cost_change_flag%TYPE,
      leg_adjustment_required_status xxfa_tax_asset_ext_r12.leg_adjustment_required_status%TYPE,
      leg_capitalize_flag            xxfa_tax_asset_ext_r12.leg_capitalize_flag%TYPE,
      leg_retirement_pending_flag    xxfa_tax_asset_ext_r12.leg_retirement_pending_flag%TYPE,
      leg_depreciate_flag            xxfa_tax_asset_ext_r12.leg_depreciate_flag%TYPE,
      leg_basic_rate                 xxfa_tax_asset_ext_r12.leg_basic_rate%TYPE,
      leg_adjusted_rate              xxfa_tax_asset_ext_r12.leg_adjusted_rate%TYPE,
      leg_bonus_rule                 xxfa_tax_asset_ext_r12.leg_bonus_rule%TYPE,
      leg_ceiling_name               xxfa_tax_asset_ext_r12.leg_ceiling_name%TYPE,
      leg_recoverable_cost           xxfa_tax_asset_ext_r12.leg_recoverable_cost%TYPE,
      leg_cap_period_name            xxfa_tax_asset_ext_r12.leg_cap_period_name%TYPE,
      period_counter_capitalized     xxfa_tax_asset_ext_r12.period_counter_capitalized%TYPE,
      leg_dep_period_name            xxfa_tax_asset_ext_r12.leg_dep_period_name%TYPE,
      period_counter_fully_reserved  xxfa_tax_asset_ext_r12.period_counter_fully_reserved%TYPE,
      leg_unrevalued_cost            xxfa_tax_asset_ext_r12.leg_unrevalued_cost%TYPE,
      leg_annual_deprn_rounding_flag xxfa_tax_asset_ext_r12.leg_annual_deprn_rounding_flag%TYPE,
      leg_percent_salvage_value      xxfa_tax_asset_ext_r12.leg_percent_salvage_value%TYPE,
      leg_allowed_deprn_limit        xxfa_tax_asset_ext_r12.leg_allowed_deprn_limit%TYPE,
      leg_allowed_deprn_limit_amount xxfa_tax_asset_ext_r12.leg_allowed_deprn_limit_amount%TYPE,
      leg_salvage_type               xxfa_tax_asset_ext_r12.leg_salvage_type%TYPE,
      leg_deprn_limit_type           xxfa_tax_asset_ext_r12.leg_deprn_limit_type%TYPE,
      leg_period_counter             xxfa_tax_asset_ext_r12.leg_period_counter%TYPE,
      leg_deprn_source_code          xxfa_tax_asset_ext_r12.leg_deprn_source_code%TYPE,
      leg_deprn_run_date             xxfa_tax_asset_ext_r12.leg_deprn_run_date%TYPE,
      leg_deprn_amount               xxfa_tax_asset_ext_r12.leg_deprn_amount%TYPE,
      leg_ytd_deprn                  xxfa_tax_asset_ext_r12.leg_ytd_deprn%TYPE,
      leg_deprn_reserve              xxfa_tax_asset_ext_r12.leg_deprn_reserve%TYPE,
      leg_description                xxfa_tax_asset_ext_r12.leg_description%TYPE,
      leg_transaction_type_code      xxfa_tax_asset_ext_r12.leg_transaction_type_code%TYPE,
      leg_transaction_date_entered   xxfa_tax_asset_ext_r12.leg_transaction_date_entered%TYPE,
      leg_transaction_subtype        xxfa_tax_asset_ext_r12.leg_transaction_subtype%TYPE,
      leg_amortization_start_date    xxfa_tax_asset_ext_r12.leg_amortization_start_date%TYPE,
      leg_cc_segment1                xxfa_tax_asset_ext_r12.leg_cc_segment1%TYPE,
      leg_cc_segment2                xxfa_tax_asset_ext_r12.leg_cc_segment2%TYPE,
      leg_cc_segment3                xxfa_tax_asset_ext_r12.leg_cc_segment3%TYPE,
      leg_cc_segment4                xxfa_tax_asset_ext_r12.leg_cc_segment4%TYPE,
      leg_cc_segment5                xxfa_tax_asset_ext_r12.leg_cc_segment5%TYPE,
      leg_cc_segment6                xxfa_tax_asset_ext_r12.leg_cc_segment6%TYPE,
      leg_cc_segment7                xxfa_tax_asset_ext_r12.leg_cc_segment7%TYPE,
      leg_cc_segment8                xxfa_tax_asset_ext_r12.leg_cc_segment8%TYPE,
      leg_cc_segment9                xxfa_tax_asset_ext_r12.leg_cc_segment9%TYPE,
      leg_cc_segment10               xxfa_tax_asset_ext_r12.leg_cc_segment10%TYPE,
      cc_segment1                    xxfa_tax_asset_ext_r12.cc_segment1%TYPE,
      cc_segment2                    xxfa_tax_asset_ext_r12.cc_segment2%TYPE,
      cc_segment3                    xxfa_tax_asset_ext_r12.cc_segment3%TYPE,
      cc_segment4                    xxfa_tax_asset_ext_r12.cc_segment4%TYPE,
      cc_segment5                    xxfa_tax_asset_ext_r12.cc_segment5%TYPE,
      cc_segment6                    xxfa_tax_asset_ext_r12.cc_segment6%TYPE,
      cc_segment7                    xxfa_tax_asset_ext_r12.cc_segment7%TYPE,
      cc_segment8                    xxfa_tax_asset_ext_r12.cc_segment8%TYPE,
      cc_segment9                    xxfa_tax_asset_ext_r12.cc_segment9%TYPE,
      cc_segment10                   xxfa_tax_asset_ext_r12.cc_segment10%TYPE,
      cc_segment11                   xxfa_tax_asset_ext_r12.cc_segment11%TYPE,
      cc_segment12                   xxfa_tax_asset_ext_r12.cc_segment12%TYPE,
      cc_segment13                   xxfa_tax_asset_ext_r12.cc_segment13%TYPE,
      cc_segment14                   xxfa_tax_asset_ext_r12.cc_segment14%TYPE,
      cc_segment15                   xxfa_tax_asset_ext_r12.cc_segment15%TYPE,
      acct_combination_id            xxfa_tax_asset_ext_r12.acct_combination_id%TYPE,
      leg_books_global_attribute1    xxfa_tax_asset_ext_r12.leg_books_global_attribute1%TYPE,
      leg_books_global_attribute2    xxfa_tax_asset_ext_r12.leg_books_global_attribute2%TYPE,
      leg_books_global_attribute3    xxfa_tax_asset_ext_r12.leg_books_global_attribute3%TYPE,
      leg_books_global_attribute4    xxfa_tax_asset_ext_r12.leg_books_global_attribute4%TYPE,
      leg_books_global_attribute5    xxfa_tax_asset_ext_r12.leg_books_global_attribute5%TYPE,
      leg_books_global_attribute6    xxfa_tax_asset_ext_r12.leg_books_global_attribute6%TYPE,
      leg_books_global_attribute7    xxfa_tax_asset_ext_r12.leg_books_global_attribute7%TYPE,
      leg_books_global_attribute8    xxfa_tax_asset_ext_r12.leg_books_global_attribute8%TYPE,
      leg_books_global_attribute9    xxfa_tax_asset_ext_r12.leg_books_global_attribute9%TYPE,
      leg_books_global_attribute10   xxfa_tax_asset_ext_r12.leg_books_global_attribute10%TYPE,
      leg_books_global_attribute11   xxfa_tax_asset_ext_r12.leg_books_global_attribute11%TYPE,
      leg_books_global_attribute12   xxfa_tax_asset_ext_r12.leg_books_global_attribute12%TYPE,
      leg_books_global_attribute13   xxfa_tax_asset_ext_r12.leg_books_global_attribute13%TYPE,
      leg_books_global_attribute14   xxfa_tax_asset_ext_r12.leg_books_global_attribute14%TYPE,
      leg_books_global_attribute15   xxfa_tax_asset_ext_r12.leg_books_global_attribute15%TYPE,
      leg_books_global_attribute16   xxfa_tax_asset_ext_r12.leg_books_global_attribute16%TYPE,
      leg_books_global_attribute17   xxfa_tax_asset_ext_r12.leg_books_global_attribute17%TYPE,
      leg_books_global_attribute18   xxfa_tax_asset_ext_r12.leg_books_global_attribute18%TYPE,
      leg_books_global_attribute19   xxfa_tax_asset_ext_r12.leg_books_global_attribute19%TYPE,
      leg_books_global_attribute20   xxfa_tax_asset_ext_r12.leg_books_global_attribute20%TYPE,
      books_global_attribute1        xxfa_tax_asset_ext_r12.books_global_attribute1%TYPE,
      books_global_attribute2        xxfa_tax_asset_ext_r12.books_global_attribute2%TYPE,
      books_global_attribute3        xxfa_tax_asset_ext_r12.books_global_attribute3%TYPE,
      books_global_attribute4        xxfa_tax_asset_ext_r12.books_global_attribute4%TYPE,
      books_global_attribute5        xxfa_tax_asset_ext_r12.books_global_attribute5%TYPE,
      books_global_attribute6        xxfa_tax_asset_ext_r12.books_global_attribute6%TYPE,
      books_global_attribute7        xxfa_tax_asset_ext_r12.books_global_attribute7%TYPE,
      books_global_attribute8        xxfa_tax_asset_ext_r12.books_global_attribute8%TYPE,
      books_global_attribute9        xxfa_tax_asset_ext_r12.books_global_attribute9%TYPE,
      books_global_attribute10       xxfa_tax_asset_ext_r12.books_global_attribute10%TYPE,
      books_global_attribute11       xxfa_tax_asset_ext_r12.books_global_attribute11%TYPE,
      books_global_attribute12       xxfa_tax_asset_ext_r12.books_global_attribute12%TYPE,
      books_global_attribute13       xxfa_tax_asset_ext_r12.books_global_attribute13%TYPE,
      books_global_attribute14       xxfa_tax_asset_ext_r12.books_global_attribute14%TYPE,
      books_global_attribute15       xxfa_tax_asset_ext_r12.books_global_attribute15%TYPE,
      books_global_attribute16       xxfa_tax_asset_ext_r12.books_global_attribute16%TYPE,
      books_global_attribute17       xxfa_tax_asset_ext_r12.books_global_attribute17%TYPE,
      books_global_attribute18       xxfa_tax_asset_ext_r12.books_global_attribute18%TYPE,
      books_global_attribute19       xxfa_tax_asset_ext_r12.books_global_attribute19%TYPE,
      books_global_attribute20       xxfa_tax_asset_ext_r12.books_global_attribute20%TYPE,
      books_global_attr_category     xxfa_tax_asset_ext_r12.books_global_attr_category%TYPE,
      leg_books_global_attr_category xxfa_tax_asset_ext_r12.leg_books_global_attr_category%TYPE,
      leg_locn_segment1              xxfa_tax_asset_ext_r12.leg_locn_segment1%TYPE,
      leg_locn_segment2              xxfa_tax_asset_ext_r12.leg_locn_segment2%TYPE,
      leg_locn_segment3              xxfa_tax_asset_ext_r12.leg_locn_segment3%TYPE,
      leg_locn_segment4              xxfa_tax_asset_ext_r12.leg_locn_segment4%TYPE,
      leg_locn_segment5              xxfa_tax_asset_ext_r12.leg_locn_segment5%TYPE,
      leg_locn_segment6              xxfa_tax_asset_ext_r12.leg_locn_segment6%TYPE,
      leg_locn_segment7              xxfa_tax_asset_ext_r12.leg_locn_segment7%TYPE,
      locn_segment1                  xxfa_tax_asset_ext_r12.locn_segment1%TYPE,
      locn_segment2                  xxfa_tax_asset_ext_r12.locn_segment2%TYPE,
      locn_segment3                  xxfa_tax_asset_ext_r12.locn_segment3%TYPE,
      locn_segment4                  xxfa_tax_asset_ext_r12.locn_segment4%TYPE,
      locn_segment5                  xxfa_tax_asset_ext_r12.locn_segment5%TYPE,
      locn_segment6                  xxfa_tax_asset_ext_r12.locn_segment6%TYPE,
      locn_segment7                  xxfa_tax_asset_ext_r12.locn_segment7%TYPE,
      location_id                    xxfa_tax_asset_ext_r12.location_id%TYPE,
      batch_id                       xxfa_tax_asset_ext_r12.batch_id%TYPE,
      leg_source_system              xxfa_tax_asset_ext_r12.leg_source_system%TYPE,
      leg_entity                     xxfa_tax_asset_ext_r12.leg_entity%TYPE,
      leg_seq_num                    xxfa_tax_asset_ext_r12.leg_seq_num%TYPE,
      leg_process_flag               xxfa_tax_asset_ext_r12.leg_process_flag%TYPE,
      leg_request_id                 xxfa_tax_asset_ext_r12.leg_request_id%TYPE,
      creation_date                  xxfa_tax_asset_ext_r12.creation_date%TYPE,
      created_by                     xxfa_tax_asset_ext_r12.created_by%TYPE,
      last_updated_date              xxfa_tax_asset_ext_r12.last_updated_date%TYPE,
      last_updated_by                xxfa_tax_asset_ext_r12.last_updated_by%TYPE,
      last_update_login              xxfa_tax_asset_ext_r12.last_update_login%TYPE,
      program_application_id         xxfa_tax_asset_ext_r12.program_application_id%TYPE,
      program_id                     xxfa_tax_asset_ext_r12.program_id%TYPE,
      program_update_date            xxfa_tax_asset_ext_r12.program_update_date%TYPE,
      request_id                     xxfa_tax_asset_ext_r12.request_id%TYPE,
      process_flag                   xxfa_tax_asset_ext_r12.process_flag%TYPE,
      run_sequence_id                xxfa_tax_asset_ext_r12.run_sequence_id%TYPE,
      leg_assigned_emp_number        xxfa_tax_asset_ext_r12.leg_assigned_emp_number%TYPE,
      assigned_emp_number            xxfa_tax_asset_ext_r12.assigned_emp_number%TYPE,
      assigned_emp_id                xxfa_tax_asset_ext_r12.assigned_emp_id%TYPE,
      leg_units_assigned             xxfa_tax_asset_ext_r12.leg_units_assigned%TYPE,
      ERROR_TYPE                     xxfa_tax_asset_ext_r12.ERROR_TYPE%TYPE,
      leg_source_asset_number        xxfa_tax_asset_ext_r12.leg_source_asset_number%TYPE,
      leg_dist_deprn_reserve         xxfa_tax_asset_ext_r12.leg_dist_deprn_reserve%TYPE,
      leg_update_book_class          xxfa_tax_asset_ext_r12.leg_update_book_class%TYPE,
      leg_merge_assets_flag          xxfa_tax_asset_ext_r12.leg_merge_assets_flag%TYPE,
      leg_assets_non_corp_flag       xxfa_tax_asset_ext_r12.leg_assets_non_corp_flag%TYPE,
      leg_assets_retire_flag         xxfa_tax_asset_ext_r12.leg_assets_retire_flag%TYPE,
      leg_assets_ytd_flag            xxfa_tax_asset_ext_r12.leg_assets_ytd_flag%TYPE,
      leg_duplicate_assets_flag      xxfa_tax_asset_ext_r12.leg_duplicate_assets_flag%TYPE,
      leg_duplicate_tag_flag         xxfa_tax_asset_ext_r12.leg_duplicate_tag_flag%TYPE,
      leg_duplicate_asset_book_flag  xxfa_tax_asset_ext_r12.leg_duplicate_asset_book_flag%TYPE);

    TYPE leg_tax_tbl IS TABLE OF leg_tax_rec INDEX BY BINARY_INTEGER;

    l_leg_tax_tbl leg_tax_tbl;
    l_err_record  NUMBER;

    CURSOR cur_leg_tax IS
      SELECT xil.interface_txn_id,
             xil.leg_asset_id,
             xil.asset_id,
             xil.leg_asset_number,
             xil.leg_asset_key_segment1,
             xil.leg_asset_key_segment2,
             xil.leg_asset_key_segment3,
             xil.leg_asset_key_segment4,
             xil.leg_asset_key_segment5,
             xil.asset_key_segment1,
             xil.asset_key_segment2,
             xil.asset_key_segment3,
             xil.asset_key_segment4,
             xil.asset_key_segment5,
             xil.asset_key_ccid,
             xil.leg_current_units,
             xil.leg_asset_type,
             xil.leg_tag_number,
             xil.leg_asset_cat_segment1,
             xil.leg_asset_cat_segment2,
             xil.asset_cat_segment1,
             xil.asset_cat_segment2,
             xil.asset_category_id,
             xil.leg_parent_asset_number,
             xil.parent_asset_id,
             xil.leg_manufacturer_name,
             xil.leg_serial_number,
             xil.leg_model_number,
             xil.leg_property_type_code,
             xil.leg_property_1245_1250_code,
             xil.leg_in_use_flag,
             xil.leg_owned_leased,
             xil.leg_new_used,
             xil.leg_unit_adjustment_flag,
             xil.leg_add_cost_je_flag,
             xil.leg_adtn_attribute1,
             xil.leg_adtn_attribute2,
             xil.leg_adtn_attribute3,
             xil.leg_adtn_attribute4,
             xil.leg_adtn_attribute5,
             xil.leg_adtn_attribute6,
             xil.leg_adtn_attribute7,
             xil.leg_adtn_attribute8,
             xil.leg_adtn_attribute9,
             xil.leg_adtn_attribute10,
             xil.leg_adtn_attribute11,
             xil.leg_adtn_attribute12,
             xil.leg_adtn_attribute13,
             xil.leg_adtn_attribute14,
             xil.leg_adtn_attribute15,
             xil.leg_adtn_attribute16,
             xil.leg_adtn_attribute17,
             xil.leg_adtn_attribute18,
             xil.leg_adtn_attribute19,
             xil.leg_adtn_attribute20,
             xil.leg_adtn_attribute21,
             xil.leg_adtn_attribute22,
             xil.leg_adtn_attribute23,
             xil.leg_adtn_attribute24,
             xil.leg_adtn_attribute25,
             xil.leg_adtn_attribute26,
             xil.leg_adtn_attribute27,
             xil.leg_adtn_attribute28,
             xil.leg_adtn_attribute29,
             xil.leg_adtn_attribute30,
             xil.leg_adtn_attr_category_code,
             xil.leg_adtn_context,
             xil.adtn_attribute1,
             xil.adtn_attribute2,
             xil.adtn_attribute3,
             xil.adtn_attribute4,
             xil.adtn_attribute5,
             xil.adtn_attribute6,
             xil.adtn_attribute7,
             xil.adtn_attribute8,
             xil.adtn_attribute9,
             xil.adtn_attribute10,
             xil.adtn_attribute11,
             xil.adtn_attribute12,
             xil.adtn_attribute13,
             xil.adtn_attribute14,
             xil.adtn_attribute15,
             xil.adtn_attribute16,
             xil.adtn_attribute17,
             xil.adtn_attribute18,
             xil.adtn_attribute19,
             xil.adtn_attribute20,
             xil.adtn_attribute21,
             xil.adtn_attribute22,
             xil.adtn_attribute23,
             xil.adtn_attribute24,
             xil.adtn_attribute25,
             xil.adtn_attribute26,
             xil.adtn_attribute27,
             xil.adtn_attribute28,
             xil.adtn_attribute29,
             xil.adtn_attribute30,
             xil.adtn_attribute_category_code,
             xil.adtn_context,
             xil.leg_inventorial,
             xil.leg_commitment,
             xil.leg_investment_law,
             xil.leg_adtn_global_attribute1,
             xil.leg_adtn_global_attribute2,
             xil.leg_adtn_global_attribute3,
             xil.leg_adtn_global_attribute4,
             xil.leg_adtn_global_attribute5,
             xil.leg_adtn_global_attribute6,
             xil.leg_adtn_global_attribute7,
             xil.leg_adtn_global_attribute8,
             xil.leg_adtn_global_attribute9,
             xil.leg_adtn_global_attribute10,
             xil.leg_adtn_global_attribute11,
             xil.leg_adtn_global_attribute12,
             xil.leg_adtn_global_attribute13,
             xil.leg_adtn_global_attribute14,
             xil.leg_adtn_global_attribute15,
             xil.leg_adtn_global_attribute16,
             xil.leg_adtn_global_attribute17,
             xil.leg_adtn_global_attribute18,
             xil.leg_adtn_global_attribute19,
             xil.leg_adtn_global_attribute20,
             xil.leg_adtn_global_attr_category,
             xil.adtn_global_attribute1,
             xil.adtn_global_attribute2,
             xil.adtn_global_attribute3,
             xil.adtn_global_attribute4,
             xil.adtn_global_attribute5,
             xil.adtn_global_attribute6,
             xil.adtn_global_attribute7,
             xil.adtn_global_attribute8,
             xil.adtn_global_attribute9,
             xil.adtn_global_attribute10,
             xil.adtn_global_attribute11,
             xil.adtn_global_attribute12,
             xil.adtn_global_attribute13,
             xil.adtn_global_attribute14,
             xil.adtn_global_attribute15,
             xil.adtn_global_attribute16,
             xil.adtn_global_attribute17,
             xil.adtn_global_attribute18,
             xil.adtn_global_attribute19,
             xil.adtn_global_attribute20,
             xil.adtn_global_attribute_category,
             xil.leg_book_type_code, ----Harjinder Singh
             xil.book_type_code,
             xil.leg_date_placed_in_service,
             xil.leg_deprn_start_date,
             xil.leg_deprn_method_code,
             xil.leg_life_in_months,
             xil.leg_rate_adjustment_factor,
             xil.leg_adjusted_cost,
             xil.leg_cost,
             xil.leg_original_cost,
             xil.leg_salvage_value,
             xil.leg_prorate_convention_code,
             xil.leg_prorate_date,
             xil.leg_cost_change_flag,
             xil.leg_adjustment_required_status,
             xil.leg_capitalize_flag,
             xil.leg_retirement_pending_flag,
             xil.leg_depreciate_flag,
             xil.leg_basic_rate,
             xil.leg_adjusted_rate,
             xil.leg_bonus_rule,
             xil.leg_ceiling_name,
             xil.leg_recoverable_cost,
             xil.leg_cap_period_name,
             xil.period_counter_capitalized,
             xil.leg_dep_period_name,
             xil.period_counter_fully_reserved,
             xil.leg_unrevalued_cost,
             xil.leg_annual_deprn_rounding_flag,
             xil.leg_percent_salvage_value,
             xil.leg_allowed_deprn_limit,
             xil.leg_allowed_deprn_limit_amount,
             xil.leg_salvage_type,
             xil.leg_deprn_limit_type,
             xil.leg_period_counter,
             xil.leg_deprn_source_code,
             xil.leg_deprn_run_date,
             xil.leg_deprn_amount,
             xil.leg_ytd_deprn,
             xil.leg_deprn_reserve,
             xil.leg_description,
             xil.leg_transaction_type_code,
             xil.leg_transaction_date_entered,
             xil.leg_transaction_subtype,
             xil.leg_amortization_start_date,
             xil.leg_cc_segment1,
             xil.leg_cc_segment2,
             xil.leg_cc_segment3,
             xil.leg_cc_segment4,
             xil.leg_cc_segment5,
             xil.leg_cc_segment6,
             xil.leg_cc_segment7,
             xil.leg_cc_segment8,
             xil.leg_cc_segment9,
             xil.leg_cc_segment10,
             xil.cc_segment1,
             xil.cc_segment2,
             xil.cc_segment3,
             xil.cc_segment4,
             xil.cc_segment5,
             xil.cc_segment6,
             xil.cc_segment7,
             xil.cc_segment8,
             xil.cc_segment9,
             xil.cc_segment10,
             xil.cc_segment11,
             xil.cc_segment12,
             xil.cc_segment13,
             xil.cc_segment14,
             xil.cc_segment15,
             xil.acct_combination_id,
             xil.leg_books_global_attribute1,
             xil.leg_books_global_attribute2,
             xil.leg_books_global_attribute3,
             xil.leg_books_global_attribute4,
             xil.leg_books_global_attribute5,
             xil.leg_books_global_attribute6,
             xil.leg_books_global_attribute7,
             xil.leg_books_global_attribute8,
             xil.leg_books_global_attribute9,
             xil.leg_books_global_attribute10,
             xil.leg_books_global_attribute11,
             xil.leg_books_global_attribute12,
             xil.leg_books_global_attribute13,
             xil.leg_books_global_attribute14,
             xil.leg_books_global_attribute15,
             xil.leg_books_global_attribute16,
             xil.leg_books_global_attribute17,
             xil.leg_books_global_attribute18,
             xil.leg_books_global_attribute19,
             xil.leg_books_global_attribute20,
             xil.books_global_attribute1,
             xil.books_global_attribute2,
             xil.books_global_attribute3,
             xil.books_global_attribute4,
             xil.books_global_attribute5,
             xil.books_global_attribute6,
             xil.books_global_attribute7,
             xil.books_global_attribute8,
             xil.books_global_attribute9,
             xil.books_global_attribute10,
             xil.books_global_attribute11,
             xil.books_global_attribute12,
             xil.books_global_attribute13,
             xil.books_global_attribute14,
             xil.books_global_attribute15,
             xil.books_global_attribute16,
             xil.books_global_attribute17,
             xil.books_global_attribute18,
             xil.books_global_attribute19,
             xil.books_global_attribute20,
             xil.books_global_attr_category,
             xil.leg_books_global_attr_category,
             xil.leg_locn_segment1,
             xil.leg_locn_segment2,
             xil.leg_locn_segment3,
             xil.leg_locn_segment4,
             xil.leg_locn_segment5,
             xil.leg_locn_segment6,
             xil.leg_locn_segment7,
             xil.locn_segment1,
             xil.locn_segment2,
             xil.locn_segment3,
             xil.locn_segment4,
             xil.locn_segment5,
             xil.locn_segment6,
             xil.locn_segment7,
             xil.location_id,
             xil.batch_id,
             xil.leg_source_system,
             xil.leg_entity,
             xil.leg_seq_num,
             xil.leg_process_flag,
             xil.leg_request_id,
             SYSDATE                            creation_date,
             g_created_by                       created_by,
             SYSDATE                            last_updated_date,
             g_last_updated_by                  last_updated_by,
             g_last_update_login                last_update_login,
             g_prog_appl_id                     program_application_id,
             g_conc_program_id                  program_id,
             SYSDATE                            program_update_date,
             xil.request_id,
             xil.process_flag,
             xil.run_sequence_id,
             xil.leg_assigned_emp_number,
             xil.assigned_emp_number,
             xil.assigned_emp_id,
             xil.leg_units_assigned,
             xil.ERROR_TYPE,
             xil.leg_source_asset_number,
             xil.leg_dist_deprn_reserve,
             xil.leg_update_book_class,
             xil.leg_merge_assets_flag,
             xil.leg_assets_non_corp_flag,
             xil.leg_assets_retire_flag,
             xil.leg_assets_ytd_flag,
             xil.leg_duplicate_assets_flag,
             xil.leg_duplicate_tag_flag,
             xil.leg_duplicate_asset_book_flag
        FROM xxfa_tax_asset_ext_r12 xil
       WHERE xil.leg_process_flag = 'V'
       AND leg_source_system in ('ISSC','FSC')   ---added to get the data only for Global Conversion
         AND NOT EXISTS
       (SELECT 1
                FROM xxfa_tax_asset_stg xis
               WHERE xis.interface_txn_id = xil.interface_txn_id
               AND leg_source_system in ('ISSC','FSC') ) ; ---added to get the data only for Global Conversion);
  BEGIN
    pov_ret_stats  := 'S';
    pov_err_msg    := NULL;
    g_total_count  := 0;
    g_failed_count := 0;
    g_loaded_count := 0;

    --Open cursor to extract data from extraction staging table
    OPEN cur_leg_tax;

    LOOP
      print_log_message('Loading taxorate book lines');
      l_leg_tax_tbl.DELETE;

      FETCH cur_leg_tax BULK COLLECT
        INTO l_leg_tax_tbl LIMIT 5000;

      --limit size of Bulk Collect

      -- Get Total Count
      g_total_count := g_total_count + l_leg_tax_tbl.COUNT;
      EXIT WHEN l_leg_tax_tbl.COUNT = 0;

      BEGIN
        -- Bulk Insert into Conversion table
        FORALL indx IN 1 .. l_leg_tax_tbl.COUNT SAVE EXCEPTIONS
          INSERT INTO xxfa_tax_asset_stg VALUES l_leg_tax_tbl (indx);
      EXCEPTION
        WHEN OTHERS THEN
          print_log_message('Errors encountered while loading tax lines data ');

          FOR l_indx_exp IN 1 .. SQL%BULK_EXCEPTIONS.COUNT LOOP
            l_err_record  := l_leg_tax_tbl(SQL%BULK_EXCEPTIONS(l_indx_exp).ERROR_INDEX)
                             .interface_txn_id;
            pov_ret_stats := 'E';
            print_log_message('Record sequence (interface_txn_id) : ' || l_leg_tax_tbl(SQL%BULK_EXCEPTIONS(l_indx_exp).ERROR_INDEX)
                              .interface_txn_id);
            print_log_message('Error Message : ' ||
                              SQLERRM(-SQL%BULK_EXCEPTIONS(l_indx_exp)
                                      .ERROR_CODE));

            -- Updating Leg_process_flag to 'E' for failed records
            UPDATE xxfa_tax_asset_ext_r12 xil
               SET xil.leg_process_flag       = 'E',
                   xil.last_updated_date      = SYSDATE,
                   xil.last_updated_by        = g_last_updated_by,
                   xil.last_update_login      = g_last_update_login,
                   xil.program_id             = g_conc_program_id,
                   xil.program_application_id = g_prog_appl_id,
                   xil.program_update_date    = SYSDATE
             WHERE xil.interface_txn_id = l_err_record
               AND xil.leg_process_flag = 'V'
               AND leg_source_system in ('ISSC','FSC')  ; ---added to get the data only for Global Conversion;

            g_failed_count := g_failed_count + SQL%ROWCOUNT;
          END LOOP;
      END;
    END LOOP;

    CLOSE cur_leg_tax;

    COMMIT;
    ---output
    g_loaded_count := g_total_count - g_failed_count;
    fnd_file.put_line(fnd_file.output, ' Stats for Tax book table load ');
    fnd_file.put_line(fnd_file.output, '================================');
    fnd_file.put_line(fnd_file.output, 'Total Count : ' || g_total_count);
    fnd_file.put_line(fnd_file.output, 'Loaded Count: ' || g_loaded_count);
    fnd_file.put_line(fnd_file.output, 'Failed Count: ' || g_failed_count);
    fnd_file.put_line(fnd_file.output, '================================');

    IF g_failed_count > 0 THEN
      print_log_message('Number of Failed Records during load of tax book : ' ||
                        g_failed_count);
    END IF;

    -- If records successfully posted to conversion staging table
    IF g_total_count > 0 THEN
      fnd_file.put_line(fnd_file.LOG,
                        'Updating process flag (leg_process_flag) in extraction table for processed records ');

      UPDATE xxfa_tax_asset_ext_r12 xil
         SET xil.leg_process_flag       = 'P',
             xil.last_updated_date      = SYSDATE,
             xil.last_updated_by        = g_last_updated_by,
             xil.last_update_login      = g_last_update_login,
             xil.program_id             = g_conc_program_id,
             xil.program_application_id = g_prog_appl_id,
             xil.program_update_date    = SYSDATE
       WHERE xil.leg_process_flag = 'V'
       AND leg_source_system in ('ISSC','FSC')   ---added to get the data only for Global Conversion
         AND EXISTS
       (SELECT 1
                FROM xxfa_tax_asset_stg xis
               WHERE xis.interface_txn_id = xil.interface_txn_id
               AND leg_source_system in ('ISSC','FSC')  ); ---added to get the data only for Global Conversion);

      COMMIT;
      -- Either no data to load from extraction table or records already exist in R12 staging table and hence not loaded
    ELSE
      print_log_message('Either no data found for loading from extraction table or records already exist in R12 staging table and hence not loaded ');

      UPDATE xxfa_tax_asset_ext_r12 xil
         SET xil.leg_process_flag       = 'E',
             xil.last_updated_date      = SYSDATE,
             xil.last_updated_by        = g_last_updated_by,
             xil.last_update_login      = g_last_update_login,
             xil.program_id             = g_conc_program_id,
             xil.program_application_id = g_prog_appl_id,
             xil.program_update_date    = SYSDATE
       WHERE xil.leg_process_flag = 'V'
       AND leg_source_system in ('ISSC','FSC')   ---added to get the data only for Global Conversion
         AND EXISTS
       (SELECT 1
                FROM xxfa_tax_asset_stg xis
               WHERE xis.interface_txn_id = xil.interface_txn_id
               AND leg_source_system in ('ISSC','FSC'));   ---added to get the data only for Global Conversion);

      COMMIT;
    END IF;
  EXCEPTION
    WHEN OTHERS THEN
      pov_ret_stats := 'E';
      pov_err_msg   := 'ERROR : Error in load_tax procedure' ||
                       SUBSTR(SQLERRM, 1, 200);
      ROLLBACK;
  END load_tax_book;

  --
  -- ========================
  -- Procedure: DUPLICATE_CHECK
  -- =============================================================================
  --   This procedure is used to check duplicate asset number in staging table
  -- =============================================================================
  PROCEDURE duplicate_check(p_leg_asset_number IN VARCHAR2) IS
    CURSOR cur_check_duplicate(p_asset VARCHAR2) IS
      SELECT DISTINCT leg_asset_number,
                      leg_adjusted_cost,
                      leg_cost,
                      leg_original_cost,
                      leg_ytd_deprn,
                      leg_deprn_reserve
        FROM xxfa_corp_asset_stg
       WHERE leg_asset_number = p_asset
         AND process_flag IN ('N', 'E')
         AND batch_id = g_batch_id
         AND run_sequence_id = g_new_run_seq_id;

    l_count NUMBER := 0;
  BEGIN
    FOR rec_cur_check_duplicate IN cur_check_duplicate(p_leg_asset_number) LOOP
      l_count := l_count + 1;
    END LOOP;

    xxetn_debug_pkg.add_debug(piv_debug_msg => 'Count check for asset : ' ||
                                               p_leg_asset_number || ' : ' ||
                                               l_count);

    IF l_count > 1 THEN
      g_ret_code := 1;
      log_errors(pin_interface_txn_id    => NULL,
                 piv_source_table        => 'XXFA_CORP_ASSET_STG',
                 piv_source_column_name  => 'leg_asset_number',
                 piv_source_column_value => p_leg_asset_number,
                 piv_source_keyname1     => 'leg_asset_number',
                 piv_source_keyvalue1    => p_leg_asset_number,
                 piv_error_type          => 'VAL_ERR',
                 piv_error_code          => 'ETN_FA_INCONSISTANT_ASSET',
                 piv_error_message       => 'Error : Asset Number is inconsistant in staging table for Asset Cost,Deprn Reserve -- erroring all records with same asset number');

      BEGIN
        UPDATE xxfa_corp_asset_stg
           SET process_flag = 'E', ERROR_TYPE = 'VAL_ERR'
         WHERE leg_asset_number = p_leg_asset_number
           AND process_flag = 'N'
           AND batch_id = g_batch_id
           AND run_sequence_id = g_new_run_seq_id;

        COMMIT;
      EXCEPTION
        WHEN OTHERS THEN
          g_ret_code := 1;
          log_errors(pin_interface_txn_id    => NULL,
                     piv_source_table        => 'XXFA_CORP_ASSET_STG',
                     piv_source_column_name  => 'leg_asset_number',
                     piv_source_column_value => p_leg_asset_number,
                     piv_source_keyname1     => 'leg_asset_number',
                     piv_source_keyvalue1    => p_leg_asset_number,
                     piv_error_type          => 'VAL_ERR',
                     piv_error_code          => 'ETN_FA_INCONSISTANT_ASSET',
                     piv_error_message       => 'Error : Exception occurred while updating assets with Inconsistant asset number ' ||
                                                SUBSTR(SQLERRM, 1, 240));
      END;
    END IF;
  EXCEPTION
    WHEN OTHERS THEN
      g_ret_code := 1;
      print_log_message('Error : Exception occurred for procedure duplicate_check ' ||
                        SUBSTR(SQLERRM, 1, 240));
  END duplicate_check;

  --
  -- ========================
  -- Procedure: DUPLICATE_TAG_NUMBER_CHECK
  -- =====================================================================================
  --   This procedure is used to check Assets with duplicate tag numbers in staging table
  -- =====================================================================================
  PROCEDURE duplicate_tag_number_check(p_leg_tag_number IN VARCHAR2) IS
    CURSOR cur_check_duplicate_tag(p_tag VARCHAR2) IS
      SELECT DISTINCT leg_source_system,
                      leg_asset_id,
                      asset_id,
                      leg_asset_number,
                      leg_current_units,
                      leg_asset_type,
                      leg_tag_number,
                      leg_asset_cat_segment1,
                      leg_asset_cat_segment2,
                      asset_cat_segment1,
                      asset_cat_segment2,
                      asset_category_id,
                      parent_asset_id,
                      leg_manufacturer_name,
                      leg_serial_number,
                      leg_model_number,
                      leg_property_type_code,
                      leg_property_1245_1250_code,
                      leg_in_use_flag,
                      leg_owned_leased,
                      leg_new_used,
                      leg_unit_adjustment_flag,
                      leg_add_cost_je_flag,
                      leg_adtn_attribute1,
                      leg_adtn_attribute2,
                      leg_adtn_attribute3,
                      leg_adtn_attribute4,
                      leg_adtn_attribute5,
                      leg_adtn_attribute6,
                      leg_adtn_attribute7,
                      leg_adtn_attribute8,
                      leg_adtn_attribute9,
                      leg_adtn_attribute10,
                      leg_adtn_attribute11,
                      leg_adtn_attribute12,
                      leg_adtn_attribute13,
                      leg_adtn_attribute14,
                      leg_adtn_attribute15,
                      leg_adtn_attr_category_code,
                      leg_adtn_context,
                      adtn_attribute1,
                      adtn_attribute2,
                      adtn_attribute3,
                      adtn_attribute4,
                      adtn_attribute5,
                      adtn_attribute6,
                      adtn_attribute7,
                      adtn_attribute8,
                      adtn_attribute9,
                      adtn_attribute10,
                      adtn_attribute11,
                      adtn_attribute12,
                      adtn_attribute13,
                      adtn_attribute14,
                      adtn_attribute15,
                      adtn_attribute_category_code,
                      adtn_context,
                      leg_inventorial,
                      leg_commitment,
                      leg_investment_law,
                      leg_adtn_global_attribute1,
                      leg_adtn_global_attribute2,
                      leg_adtn_global_attribute3,
                      leg_adtn_global_attribute4,
                      leg_adtn_global_attribute5,
                      leg_adtn_global_attribute6,
                      leg_adtn_global_attribute7,
                      leg_adtn_global_attribute8,
                      leg_adtn_global_attribute9,
                      leg_adtn_global_attribute10,
                      leg_adtn_global_attribute11,
                      leg_adtn_global_attribute12,
                      leg_adtn_global_attribute13,
                      leg_adtn_global_attribute14,
                      leg_adtn_global_attribute15,
                      leg_adtn_global_attribute16,
                      leg_adtn_global_attribute17,
                      leg_adtn_global_attribute18,
                      leg_adtn_global_attribute19,
                      leg_adtn_global_attribute20,
                      leg_adtn_global_attr_category,
                      adtn_global_attribute1,
                      adtn_global_attribute2,
                      adtn_global_attribute3,
                      adtn_global_attribute4,
                      adtn_global_attribute5,
                      adtn_global_attribute6,
                      adtn_global_attribute7,
                      adtn_global_attribute8,
                      adtn_global_attribute9,
                      adtn_global_attribute10,
                      adtn_global_attribute11,
                      adtn_global_attribute12,
                      adtn_global_attribute13,
                      adtn_global_attribute14,
                      adtn_global_attribute15,
                      adtn_global_attribute16,
                      adtn_global_attribute17,
                      adtn_global_attribute18,
                      adtn_global_attribute19,
                      adtn_global_attribute20,
                      adtn_global_attribute_category,
                      leg_book_type_code,
                      book_type_code,
                      leg_date_placed_in_service,
                      leg_deprn_start_date,
                      leg_deprn_method_code,
                      leg_life_in_months,
                      leg_rate_adjustment_factor,
                      leg_adjusted_cost,
                      leg_cost,
                      leg_original_cost,
                      leg_salvage_value,
                      leg_prorate_convention_code,
                      leg_prorate_date,
                      leg_cost_change_flag,
                      leg_adjustment_required_status,
                      leg_capitalize_flag,
                      leg_retirement_pending_flag,
                      leg_depreciate_flag,
                      leg_basic_rate,
                      leg_adjusted_rate,
                      leg_bonus_rule,
                      leg_ceiling_name,
                      leg_recoverable_cost,
                      leg_cap_period_name,
                      period_counter_capitalized,
                      leg_dep_period_name,
                      period_counter_fully_reserved,
                      leg_unrevalued_cost,
                      leg_annual_deprn_rounding_flag,
                      leg_percent_salvage_value,
                      leg_allowed_deprn_limit,
                      leg_allowed_deprn_limit_amount,
                      leg_salvage_type,
                      leg_deprn_limit_type,
                      leg_period_counter,
                      leg_deprn_source_code,
                      leg_deprn_run_date,
                      leg_deprn_amount,
                      leg_ytd_deprn,
                      leg_deprn_reserve,
                      leg_description,
                      leg_transaction_type_code,
                      leg_transaction_date_entered,
                      leg_transaction_subtype,
                      leg_amortization_start_date,
                      leg_books_global_attribute1,
                      leg_books_global_attribute2,
                      leg_books_global_attribute3,
                      leg_books_global_attribute4,
                      leg_books_global_attribute5,
                      leg_books_global_attribute6,
                      leg_books_global_attribute7,
                      leg_books_global_attribute8,
                      leg_books_global_attribute9,
                      leg_books_global_attribute10,
                      leg_books_global_attribute11,
                      leg_books_global_attribute12,
                      leg_books_global_attribute13,
                      leg_books_global_attribute14,
                      leg_books_global_attribute15,
                      leg_books_global_attribute16,
                      leg_books_global_attribute17,
                      leg_books_global_attribute18,
                      leg_books_global_attribute19,
                      leg_books_global_attribute20,
                      books_global_attribute1,
                      books_global_attribute2,
                      books_global_attribute3,
                      books_global_attribute4,
                      books_global_attribute5,
                      books_global_attribute6,
                      books_global_attribute7,
                      books_global_attribute8,
                      books_global_attribute9,
                      books_global_attribute10,
                      books_global_attribute11,
                      books_global_attribute12,
                      books_global_attribute13,
                      books_global_attribute14,
                      books_global_attribute15,
                      books_global_attribute16,
                      books_global_attribute17,
                      books_global_attribute18,
                      books_global_attribute19,
                      books_global_attribute20,
                      books_global_attr_category,
                      leg_books_global_attr_category,
                      batch_id,
                      process_flag,
                      run_sequence_id
        FROM xxfa_corp_asset_stg
       WHERE leg_tag_number = p_tag
         AND process_flag = 'N'
         AND batch_id = g_batch_id
         AND run_sequence_id = g_new_run_seq_id;

    l_count NUMBER := 0;
  BEGIN
    FOR rec_cur_check_duplicate_tag IN cur_check_duplicate_tag(p_leg_tag_number) LOOP
      l_count := l_count + 1;
    END LOOP;

    print_log_message('Count check for asset with duplicate tag number : ' ||
                      l_count);

    IF l_count > 1 THEN
      g_ret_code := 1;
      log_errors(pin_interface_txn_id    => NULL,
                 piv_source_table        => 'XXFA_CORP_ASSET_STG',
                 piv_source_column_name  => 'leg_tag_number',
                 piv_source_column_value => p_leg_tag_number,
                 piv_error_type          => 'VAL_ERR',
                 piv_error_code          => 'ETN_FA_DUPLICATE_TAG_NUM',
                 piv_error_message       => 'Error : Tag number duplicate -- erroring all records with same tag number : ');

      BEGIN
        UPDATE xxfa_corp_asset_stg
           SET process_flag = 'E', ERROR_TYPE = 'VAL_ERR'
         WHERE leg_tag_number = p_leg_tag_number
           AND process_flag = 'N'
           AND batch_id = g_batch_id
           AND run_sequence_id = g_new_run_seq_id;

        COMMIT;
      EXCEPTION
        WHEN OTHERS THEN
          g_ret_code := 1;
          log_errors(pin_interface_txn_id    => NULL,
                     piv_source_table        => 'XXFA_CORP_ASSET_STG',
                     piv_source_column_name  => NULL,
                     piv_source_column_value => NULL,
                     piv_error_type          => 'VAL_ERR',
                     piv_error_code          => 'ETN_FA_DUPLICATE_TAG_NUM',
                     piv_error_message       => 'Error : Exception occured while updating assets with duplicate tag number ' ||
                                                SUBSTR(SQLERRM, 1, 240));
      END;
    END IF;
  EXCEPTION
    WHEN OTHERS THEN
      g_ret_code := 1;
      print_log_message('Error : Exception occured for procedure duplicate tag number : ' ||
                        SUBSTR(SQLERRM, 1, 240));
  END duplicate_tag_number_check;

  --
  -- ========================
  -- Procedure: VALIDATE_ASSET_EXIST
  -- =====================================================================================
  --   This procedure is used to check if Assets are already present in the system
  -- =====================================================================================
  FUNCTION validate_asset_exist(p_asset IN VARCHAR2) RETURN VARCHAR2 IS
    ln_asset_count NUMBER := NULL;
    lc_e_flag      VARCHAR2(1) := 'N';
  BEGIN
    SELECT COUNT(*)
      INTO ln_asset_count
      FROM fa_additions_b
     WHERE asset_number = p_asset;

    IF ln_asset_count > 0 THEN
      g_ret_code := 1;
      log_errors(pin_interface_txn_id    => NULL,
                 piv_source_table        => 'XXFA_CORP_ASSET_STG',
                 piv_source_column_name  => 'leg_asset_number',
                 piv_source_column_value => p_asset,
                 piv_source_keyname1     => 'leg_asset_number',
                 piv_source_keyvalue1    => p_asset,
                 piv_error_type          => 'VAL_ERR',
                 piv_error_code          => 'ETN_FA_INVALID_ASSET',
                 piv_error_message       => 'Error : Asset Number already exists in the system. ');
      lc_e_flag := 'Y';
      RETURN lc_e_flag;
    END IF;

    RETURN lc_e_flag;
  EXCEPTION
    WHEN OTHERS THEN
      g_ret_code := 1;
      log_errors(pin_interface_txn_id    => NULL,
                 piv_source_table        => 'XXFA_CORP_ASSET_STG',
                 piv_source_column_name  => NULL,
                 piv_source_column_value => NULL,
                 piv_source_keyname1     => 'leg_asset_number',
                 piv_source_keyvalue1    => p_asset,
                 piv_error_type          => 'VAL_ERR',
                 piv_error_code          => 'ETN_FA_INVALID_ASSET',
                 piv_error_message       => 'Error : Exception occured while checking asset existence in system : ' ||
                                            SUBSTR(SQLERRM, 1, 240));
      lc_e_flag := 'Y';
      RETURN lc_e_flag;
  END validate_asset_exist;

  --
  -- ========================
  -- Procedure: VALIDATE_ASSET_CATEGORY_POLAND_TAX
  -- =====================================================================================
  --   This procedure is used to check if Assets have valid category for Poland Books
  -- =====================================================================================
  PROCEDURE validate_asset_cat_poland_tax(p_leg_source_system  IN VARCHAR2,
                                          p_leg_asset_number   IN VARCHAR2,
                                          p_leg_book_type_code IN VARCHAR2,
                                          p_leg_segment1       IN VARCHAR2,
                                          x_leg_segment2       OUT VARCHAR2,
                                          x_category_id        OUT NUMBER,
                                          x_cat_segment1       OUT VARCHAR2,
                                          x_cat_segment2       OUT VARCHAR2,
                                          x_cat_chk_flag       OUT VARCHAR2,
                                          x_cat_msg            OUT VARCHAR2) IS

    lc_cat_segment   VARCHAR2(240) := NULL;
    ln_count         NUMBER := NULL;
    l_tax_book_count NUMBER := NULL;
    l_corp_category  varchar2(100) := NULL;

  BEGIN
    x_cat_chk_flag   := 'N';
    x_cat_segment1   := NULL;
    x_cat_segment2   := NULL;
    x_category_id    := NULL;
    x_cat_msg        := NULL;
    l_tax_book_count := NULL;
    l_corp_category  := NULL;
    xxetn_debug_pkg.add_debug(piv_debug_msg => 'Validation for Asset Category Starts for Poland Books : ');

    BEGIN
      SELECT distinct LEG_ASSET_CAT_SEGMENT1     -- Added by Reshu 4-Feb-2015
        INTO l_corp_category
        FROM XXFA_CORP_ASSET_STG
       WHERE leg_asset_number = p_leg_asset_number;

    EXCEPTION
      WHEN OTHERS THEN
        x_cat_msg      := 'Error : Exception occured while getting the corp category for poland books' ||
                          SUBSTR(SQLERRM, 1, 240);
        x_cat_chk_flag := 'Y';
        RETURN;

    END;

    BEGIN
      SELECT flv.description
        INTO lc_cat_segment
        FROM fnd_lookup_values flv
       WHERE flv.lookup_type = g_cat_lookup
         AND flv.meaning = l_corp_category || g_sep_p || p_leg_segment1
         AND tag = p_leg_source_system
         AND flv.enabled_flag = 'Y'
         AND flv.LANGUAGE = 'US'
         AND TRUNC(SYSDATE) BETWEEN
             NVL(flv.start_date_active, TRUNC(SYSDATE)) AND
             NVL(flv.end_date_active, TRUNC(SYSDATE));
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        x_cat_chk_flag := 'Y';
        x_cat_msg      := 'Segment Value entered not present in the lookup for ISSC';
        x_leg_segment2 := l_corp_category ;
      WHEN OTHERS THEN
        x_cat_msg      := 'Error : Exception occured while fetching lookup value' ||
                          SUBSTR(SQLERRM, 1, 240);
        x_cat_chk_flag := 'Y';
        x_leg_segment2 := l_corp_category;
    END;

    ln_count       := INSTR(lc_cat_segment, '-');
    x_cat_segment1 := SUBSTR(lc_cat_segment, 1, ln_count - 1);
    x_cat_segment2 := SUBSTR(lc_cat_segment, ln_count + 1);

    IF x_cat_chk_flag <> 'Y' THEN
      BEGIN
        SELECT category_id
          INTO x_category_id
          FROM fa_categories_b
         WHERE segment1 = x_cat_segment1
           AND segment2 = x_cat_segment2
           AND enabled_flag = 'Y';
      EXCEPTION
        WHEN NO_DATA_FOUND THEN
          x_cat_chk_flag := 'Y';
          x_cat_msg      := 'Category not defined in the system';
        WHEN OTHERS THEN
          x_cat_msg      := 'Error occured while Category ID from base table : ' ||
                            SUBSTR(SQLERRM, 1, 240);
          x_cat_chk_flag := 'Y';
      END;
    END IF;

  EXCEPTION
    WHEN OTHERS THEN
      x_cat_msg      := 'Error : Exception occured while Category ID' ||
                        SUBSTR(SQLERRM, 1, 240);
      x_cat_chk_flag := 'Y';
  END validate_asset_cat_poland_tax;

  --
  -- ========================
  -- Procedure: VALIDATE_ASSET_CATEGORY_POLAND
  -- =====================================================================================
  --   This procedure is used to check if Assets have valid category for Poland Books
  -- =====================================================================================
  PROCEDURE validate_asset_category_poland(p_leg_source_system  IN VARCHAR2,
                                           p_leg_asset_number   IN VARCHAR2,
                                           p_leg_book_type_code IN VARCHAR2,
                                           p_leg_segment1       IN VARCHAR2,
                                           x_leg_segment2       OUT VARCHAR2,
                                           x_category_id        OUT NUMBER,
                                           x_cat_segment1       OUT VARCHAR2,
                                           x_cat_segment2       OUT VARCHAR2,
                                           x_cat_chk_flag       OUT VARCHAR2,
                                           x_cat_msg            OUT VARCHAR2) IS
    lc_cat_segment   VARCHAR2(240) := NULL;
    ln_count         NUMBER := NULL;
    l_tax_book_count NUMBER := NULL;
    l_tax_category   varchar2(100) := NULL;

  BEGIN
    x_cat_chk_flag   := 'N';
    x_cat_segment1   := NULL;
    x_cat_segment2   := NULL;
    x_category_id    := NULL;
    x_cat_msg        := NULL;
    l_tax_book_count := NULL;
    l_tax_category   := NULL;
    xxetn_debug_pkg.add_debug(piv_debug_msg => 'Validation for Asset Category Starts for Poland Books : ');

    BEGIN

    BEGIN

        SELECT COUNT(DISTINCT(LEG_ASSET_CAT_SEGMENT1)) --, LEG_ASSET_CAT_SEGMENT1
       INTO l_tax_book_count--, l_tax_category
      FROM XXFA_TAX_ASSET_STG
      WHERE leg_asset_number    = p_leg_asset_number;
      --AND   leg_book_type_code  = p_leg_book_type_code;

     EXCEPTION
            WHEN
            OTHERS THEN
              x_cat_msg      := 'Error while fetching the cat for Tax : - '||p_leg_asset_number || '-' || p_leg_book_type_code;
              x_cat_chk_flag := 'Y';

    END;


       IF l_tax_book_count > 1
      THEN
           x_cat_msg := 'Multiple Categories Attached to the Tax Records for the Asset : - '||p_leg_asset_number || '-' || p_leg_book_type_code;
           x_cat_chk_flag := 'Y';

        BEGIN
           UPDATE XXFA_TAX_ASSET_STG
           SET process_flag = 'E'
           WHERE leg_asset_number    = p_leg_asset_number
           AND   leg_book_type_code  = p_leg_book_type_code;
        EXCEPTION
            WHEN OTHERS
              THEN
              x_cat_msg      := 'Error : Exception occured while updating the tax table ' ||
                              SUBSTR(SQLERRM, 1, 240);
              x_cat_chk_flag := 'Y';
        END;
           RETURN;

    ELSE
      BEGIN

    SELECT DISTINCT LEG_ASSET_CAT_SEGMENT1   -- added by Reshu DISTINCT on 4-Feb-2016
       INTO  l_tax_category
      FROM XXFA_TAX_ASSET_STG
      WHERE leg_asset_number    = p_leg_asset_number;
      EXCEPTION
            WHEN OTHERS
              THEN
              x_cat_msg      := 'Error : Exception occured while getting CAT from the tax table ' ||
                              SUBSTR(SQLERRM, 1, 240);
              x_cat_chk_flag := 'Y';


      END;


        --- if all the tax records are attached to 1 category

        BEGIN
          SELECT flv.description
            INTO lc_cat_segment
            FROM fnd_lookup_values flv
           WHERE flv.lookup_type = g_cat_lookup
             AND flv.meaning = p_leg_segment1 || g_sep_p || l_tax_category
             AND tag = p_leg_source_system
             AND flv.enabled_flag = 'Y'
             AND flv.LANGUAGE = 'US'
             AND TRUNC(SYSDATE) BETWEEN
                 NVL(flv.start_date_active, TRUNC(SYSDATE)) AND
                 NVL(flv.end_date_active, TRUNC(SYSDATE));
        EXCEPTION
          WHEN NO_DATA_FOUND THEN
            x_cat_chk_flag := 'Y';
            x_cat_msg      := 'Segment Value entered not present in the lookup for ISSC';
            x_leg_segment2 := l_tax_category;
          WHEN OTHERS THEN
            x_cat_msg      := 'Error : Exception occured while fetching lookup value' ||
                              SUBSTR(SQLERRM, 1, 240);
            x_cat_chk_flag := 'Y';
            x_leg_segment2 := l_tax_category;
        END;

        ln_count       := INSTR(lc_cat_segment, '-');
        x_cat_segment1 := SUBSTR(lc_cat_segment, 1, ln_count - 1);
        x_cat_segment2 := SUBSTR(lc_cat_segment, ln_count + 1);

        IF x_cat_chk_flag <> 'Y' THEN
          BEGIN
            SELECT category_id
              INTO x_category_id
              FROM fa_categories_b
             WHERE segment1 = x_cat_segment1
               AND segment2 = x_cat_segment2
               AND enabled_flag = 'Y';
          EXCEPTION
            WHEN NO_DATA_FOUND THEN
              x_cat_chk_flag := 'Y';
              x_cat_msg      := 'Category not defined in the system';
            WHEN OTHERS THEN
              x_cat_msg      := 'Error occured while Category ID from base table : ' ||
                                SUBSTR(SQLERRM, 1, 240);
              x_cat_chk_flag := 'Y';
          END;
        END IF;
        END IF;

    END;
  EXCEPTION
    WHEN OTHERS THEN
      x_cat_msg      := 'Error : Exception occured while Category ID' ||
                        SUBSTR(SQLERRM, 1, 240);
      x_cat_chk_flag := 'Y';
  END validate_asset_category_poland;

  --
  -- ========================
  -- Procedure: VALIDATE_ASSET_CATEGORY
  -- =====================================================================================
  --   This procedure is used to check if Assets have valid category
  -- =====================================================================================
  PROCEDURE validate_asset_category(p_leg_source_system IN VARCHAR2,
                                    p_leg_segment1      IN VARCHAR2,
                                    p_leg_segment2      IN VARCHAR2,
                                    x_category_id       OUT NUMBER,
                                    x_cat_segment1      OUT VARCHAR2,
                                    x_cat_segment2      OUT VARCHAR2,
                                    x_cat_chk_flag      OUT VARCHAR2,
                                    x_cat_msg           OUT VARCHAR2) IS
    lc_cat_segment VARCHAR2(240) := NULL;
    ln_count       NUMBER := NULL;
  BEGIN
    x_cat_chk_flag := 'N';
    x_cat_segment1 := NULL;
    x_cat_segment2 := NULL;
    x_category_id  := NULL;
    x_cat_msg      := NULL;
    xxetn_debug_pkg.add_debug(piv_debug_msg => 'Validation for Asset Category Starts : ');

    IF p_leg_source_system = g_source_issc THEN
      IF p_leg_segment1 IS NULL THEN
        x_cat_msg      := 'segment1 cannot be NULL for ISSC source system';
        x_cat_chk_flag := 'Y';
        RETURN;
      END IF;

      BEGIN
        SELECT flv.description
          INTO lc_cat_segment
          FROM fnd_lookup_values flv
         WHERE flv.lookup_type = g_cat_lookup
           AND flv.meaning = p_leg_segment1
           AND tag = p_leg_source_system
           AND flv.enabled_flag = 'Y'
           AND flv.LANGUAGE = 'US'
           AND TRUNC(SYSDATE) BETWEEN
               NVL(flv.start_date_active, TRUNC(SYSDATE)) AND
               NVL(flv.end_date_active, TRUNC(SYSDATE));
      EXCEPTION
        WHEN NO_DATA_FOUND THEN
          x_cat_chk_flag := 'Y';
          x_cat_msg      := 'Segment Value entered not present in the lookup for ISSC';
        WHEN OTHERS THEN
          x_cat_msg      := 'Error : Exception occured while fetching lookup value' ||
                            SUBSTR(SQLERRM, 1, 240);
          x_cat_chk_flag := 'Y';
      END;

      ln_count       := INSTR(lc_cat_segment, '-');
      x_cat_segment1 := SUBSTR(lc_cat_segment, 1, ln_count - 1);
      x_cat_segment2 := SUBSTR(lc_cat_segment, ln_count + 1);
    ELSIF p_leg_source_system = g_source_fsc THEN
      IF p_leg_segment1 IS NULL OR p_leg_segment2 IS NULL THEN
        x_cat_msg      := 'segment1 or segment2 cannot be NULL for FSC source system';
        x_cat_chk_flag := 'Y';
        RETURN;
      END IF;

      BEGIN
        SELECT flv.description
          INTO lc_cat_segment
          FROM fnd_lookup_values flv
         WHERE flv.lookup_type = g_cat_lookup
           AND flv.meaning = p_leg_segment1 || g_sep || p_leg_segment2
           AND tag = p_leg_source_system
           AND flv.enabled_flag = 'Y'
           AND flv.LANGUAGE = 'US'
           AND TRUNC(SYSDATE) BETWEEN
               NVL(flv.start_date_active, TRUNC(SYSDATE)) AND
               NVL(flv.end_date_active, TRUNC(SYSDATE));
      EXCEPTION
        WHEN NO_DATA_FOUND THEN
          x_cat_chk_flag := 'Y';
          x_cat_msg      := 'Segment Value entered not present in the lookup for FSC';
        WHEN OTHERS THEN
          x_cat_msg      := 'Error : Exception occured while fetching lookup value' ||
                            SUBSTR(SQLERRM, 1, 240);
          x_cat_chk_flag := 'Y';
      END;

      ln_count       := INSTR(lc_cat_segment, '-');
      x_cat_segment1 := SUBSTR(lc_cat_segment, 1, ln_count - 1);
      x_cat_segment2 := SUBSTR(lc_cat_segment, ln_count + 1);
    END IF;

    IF x_cat_chk_flag <> 'Y' THEN
      BEGIN
        SELECT category_id
          INTO x_category_id
          FROM fa_categories_b
         WHERE segment1 = x_cat_segment1
           AND segment2 = x_cat_segment2
           AND enabled_flag = 'Y';
      EXCEPTION
        WHEN NO_DATA_FOUND THEN
          x_cat_chk_flag := 'Y';
          x_cat_msg      := 'Category not defined in the system';
        WHEN OTHERS THEN
          x_cat_msg      := 'Error occured while Category ID from base table : ' ||
                            SUBSTR(SQLERRM, 1, 240);
          x_cat_chk_flag := 'Y';
      END;
    END IF;
  EXCEPTION
    WHEN OTHERS THEN
      x_cat_msg      := 'Error : Exception occured while Category ID' ||
                        SUBSTR(SQLERRM, 1, 240);
      x_cat_chk_flag := 'Y';
  END validate_asset_category;

  --
  -- ========================Added as per version v1.5 Starts here===================================
  -- Procedure: set_cat_lookup
  -- =====================================================================================
  --   This procedure is used to validate the asset category as per PMC
  -- =====================================================================================
  PROCEDURE set_cat_lookup(p_leg_source_asset_number in Varchar2,
                           p_leg_book_type_code      in Varchar2) --- added by Harjinder Singh for the PMC#349366
   IS

    ---added by Harjinder Singh lookup will hold all the 11i Polands Books
    CURSOR c_poland_books IS
      SELECT meaning
        FROM apps.Fnd_lookup_values FLV
       WHERE lookup_type = g_poland_book_lookup
         AND flv.language = 'US'
         AND enabled_flag = 'Y'
         AND nvl(end_date_active, trunc(sysdate)) >= trunc(sysdate);

    --- type to hold the values from the lookup
    TYPE poland_books_tbl IS TABLE OF VARCHAR2(240) INDEX BY VARCHAR2(240);
    poland_books_tab poland_books_tbl;

  BEGIN

    --- Added by Harjinder Singh for the PMC#349366
    BEGIN
      --- loading the plsql table with the lookup values ie 11i poland books
      FOR rec_poland_books in c_poland_books LOOP
        poland_books_tab(rec_poland_books.meaning) := rec_poland_books.meaning;
      END LOOP;
    END;

    IF poland_books_tab.exists(p_leg_book_type_code) AND
       (p_leg_source_asset_number NOT IN
        ('STAT Only', 'COST ADJ', 'USGAAP') OR
        p_leg_source_asset_number IS NULL) THEN
      g_cat_lookup := g_cat_lookup_poland;
    ELSIF p_leg_source_asset_number = 'STAT Only' THEN
      g_cat_lookup := g_cat_lookup_local;

    ELSIF (p_leg_source_asset_number = 'COST ADJ' OR
          p_leg_source_asset_number = 'USGAAP' /*or p_leg_source_asset_number is not null*/
          ) THEN
      g_cat_lookup := g_cat_lookup_us;
    ELSE
      g_cat_lookup := g_cat_lookup_others;
    END IF;
  END set_cat_lookup;

  -- ========================Added as per version v1.5 Ends here===================================
  --
  -- ========================
  -- Procedure: VALIDATE_CORPORATE
  -- =====================================================================================
  --   This procedure is used to validate the assets with corporate books
  -- =====================================================================================
  PROCEDURE validate_corporate IS
    CURSOR cur_val_parent IS
      SELECT DISTINCT leg_source_system,
                      leg_source_asset_number,
                      leg_asset_id,

                      asset_id,
                      leg_asset_number,
                      leg_current_units,
                      leg_asset_type,
                      leg_tag_number,
                      leg_asset_cat_segment1,
                      leg_asset_cat_segment2,
                      asset_cat_segment1,
                      asset_cat_segment2,
                      asset_category_id,
                      leg_parent_asset_number,
                      parent_asset_id,
                      leg_manufacturer_name,
                      leg_serial_number,
                      leg_model_number,
                      leg_property_type_code,
                      leg_property_1245_1250_code,
                      leg_in_use_flag,
                      leg_owned_leased,
                      leg_new_used,
                      leg_unit_adjustment_flag,
                      leg_add_cost_je_flag,
                      leg_adtn_attribute1,
                      leg_adtn_attribute2,
                      leg_adtn_attribute3,
                      leg_adtn_attribute4,
                      leg_adtn_attribute5,
                      leg_adtn_attribute6,
                      leg_adtn_attribute7,
                      leg_adtn_attribute8,
                      leg_adtn_attribute9,
                      leg_adtn_attribute10,
                      leg_adtn_attribute11,
                      leg_adtn_attribute12,
                      leg_adtn_attribute13,
                      leg_adtn_attribute14,
                      leg_adtn_attribute15,
                      leg_adtn_attribute16,
                      leg_adtn_attribute17,
                      leg_adtn_attribute18,
                      leg_adtn_attribute19,
                      leg_adtn_attribute20,
                      leg_adtn_attribute21,
                      leg_adtn_attribute22,
                      leg_adtn_attribute23,
                      leg_adtn_attribute24,
                      leg_adtn_attribute25,
                      leg_adtn_attribute26,
                      leg_adtn_attribute27,
                      leg_adtn_attribute28,
                      leg_adtn_attribute29,
                      leg_adtn_attribute30,
                      leg_adtn_attr_category_code,
                      leg_adtn_context,
                      adtn_attribute1,
                      adtn_attribute2,
                      adtn_attribute3,
                      adtn_attribute4,
                      adtn_attribute5,
                      adtn_attribute6,
                      adtn_attribute7,
                      adtn_attribute8,
                      adtn_attribute9,
                      adtn_attribute10,
                      adtn_attribute11,
                      adtn_attribute12,
                      adtn_attribute13,
                      adtn_attribute14,
                      adtn_attribute15,
                      adtn_attribute16,
                      adtn_attribute17,
                      adtn_attribute18,
                      adtn_attribute19,
                      adtn_attribute20,
                      adtn_attribute21,
                      adtn_attribute22,
                      adtn_attribute23,
                      adtn_attribute24,
                      adtn_attribute25,
                      adtn_attribute26,
                      adtn_attribute27,
                      adtn_attribute28,
                      adtn_attribute29,
                      adtn_attribute30,
                      adtn_attribute_category_code,
                      adtn_context,
                      leg_inventorial,
                      leg_commitment,
                      leg_investment_law,
                      leg_adtn_global_attribute1,
                      leg_adtn_global_attribute2,
                      leg_adtn_global_attribute3,
                      leg_adtn_global_attribute4,
                      leg_adtn_global_attribute5,
                      leg_adtn_global_attribute6,
                      leg_adtn_global_attribute7,
                      leg_adtn_global_attribute8,
                      leg_adtn_global_attribute9,
                      leg_adtn_global_attribute10,
                      leg_adtn_global_attribute11,
                      leg_adtn_global_attribute12,
                      leg_adtn_global_attribute13,
                      leg_adtn_global_attribute14,
                      leg_adtn_global_attribute15,
                      leg_adtn_global_attribute16,
                      leg_adtn_global_attribute17,
                      leg_adtn_global_attribute18,
                      leg_adtn_global_attribute19,
                      leg_adtn_global_attribute20,
                      leg_adtn_global_attr_category,
                      adtn_global_attribute1,
                      adtn_global_attribute2,
                      adtn_global_attribute3,
                      adtn_global_attribute4,
                      adtn_global_attribute5,
                      adtn_global_attribute6,
                      adtn_global_attribute7,
                      adtn_global_attribute8,
                      adtn_global_attribute9,
                      adtn_global_attribute10,
                      adtn_global_attribute11,
                      adtn_global_attribute12,
                      adtn_global_attribute13,
                      adtn_global_attribute14,
                      adtn_global_attribute15,
                      adtn_global_attribute16,
                      adtn_global_attribute17,
                      adtn_global_attribute18,
                      adtn_global_attribute19,
                      adtn_global_attribute20,
                      adtn_global_attribute_category,
                      leg_book_type_code,
                      book_type_code,
                      leg_date_placed_in_service,
                      leg_deprn_start_date,
                      leg_deprn_method_code,
                      leg_life_in_months,
                      leg_rate_adjustment_factor,
                      leg_adjusted_cost,
                      leg_cost,
                      leg_original_cost,
                      leg_salvage_value,
                      leg_prorate_convention_code,
                      leg_prorate_date,
                      leg_cost_change_flag,
                      leg_adjustment_required_status,
                      leg_capitalize_flag,
                      leg_retirement_pending_flag,
                      leg_depreciate_flag,
                      leg_basic_rate,
                      leg_adjusted_rate,
                      leg_bonus_rule,
                      leg_ceiling_name,
                      leg_recoverable_cost,
                      leg_cap_period_name,
                      period_counter_capitalized,
                      leg_dep_period_name,
                      period_counter_fully_reserved,
                      leg_unrevalued_cost,
                      leg_annual_deprn_rounding_flag,
                      leg_percent_salvage_value,
                      leg_allowed_deprn_limit,
                      leg_allowed_deprn_limit_amount,
                      leg_salvage_type,
                      leg_deprn_limit_type,
                      leg_period_counter,
                      leg_deprn_source_code,
                      leg_deprn_run_date,
                      leg_deprn_amount,
                      leg_ytd_deprn,
                      leg_deprn_reserve,
                      leg_description,
                      leg_transaction_type_code,
                      leg_transaction_date_entered,
                      leg_transaction_subtype,
                      leg_amortization_start_date,
                      leg_books_global_attribute1,
                      leg_books_global_attribute2,
                      leg_books_global_attribute3,
                      leg_books_global_attribute4,
                      leg_books_global_attribute5,
                      leg_books_global_attribute6,
                      leg_books_global_attribute7,
                      leg_books_global_attribute8,
                      leg_books_global_attribute9,
                      leg_books_global_attribute10,
                      leg_books_global_attribute11,
                      leg_books_global_attribute12,
                      leg_books_global_attribute13,
                      leg_books_global_attribute14,
                      leg_books_global_attribute15,
                      leg_books_global_attribute16,
                      leg_books_global_attribute17,
                      leg_books_global_attribute18,
                      leg_books_global_attribute19,
                      leg_books_global_attribute20,
                      books_global_attribute1,
                      books_global_attribute2,
                      books_global_attribute3,
                      books_global_attribute4,
                      books_global_attribute5,
                      books_global_attribute6,
                      books_global_attribute7,
                      books_global_attribute8,
                      books_global_attribute9,
                      books_global_attribute10,
                      books_global_attribute11,
                      books_global_attribute12,
                      books_global_attribute13,
                      books_global_attribute14,
                      books_global_attribute15,
                      books_global_attribute16,
                      books_global_attribute17,
                      books_global_attribute18,
                      books_global_attribute19,
                      books_global_attribute20,
                      books_global_attr_category,
                      leg_books_global_attr_category,
                      batch_id,
                      process_flag,
                      run_sequence_id
        FROM xxfa_corp_asset_stg
       WHERE leg_parent_asset_number IS NULL
         AND process_flag IN ('N', 'E')
         AND batch_id = g_batch_id
         AND run_sequence_id = g_new_run_seq_id;

    CURSOR cur_val_child IS
      SELECT DISTINCT leg_source_system,
                      leg_source_asset_number,
                      leg_asset_id,
                      asset_id,
                      leg_asset_number,
                      leg_current_units,
                      leg_asset_type,
                      leg_tag_number,
                      leg_asset_cat_segment1,
                      leg_asset_cat_segment2,
                      asset_cat_segment1,
                      asset_cat_segment2,
                      asset_category_id,
                      leg_parent_asset_number,
                      parent_asset_id,
                      leg_manufacturer_name,
                      leg_serial_number,
                      leg_model_number,
                      leg_property_type_code,
                      leg_property_1245_1250_code,
                      leg_in_use_flag,
                      leg_owned_leased,
                      leg_new_used,
                      leg_unit_adjustment_flag,
                      leg_add_cost_je_flag,
                      leg_adtn_attribute1,
                      leg_adtn_attribute2,
                      leg_adtn_attribute3,
                      leg_adtn_attribute4,
                      leg_adtn_attribute5,
                      leg_adtn_attribute6,
                      leg_adtn_attribute7,
                      leg_adtn_attribute8,
                      leg_adtn_attribute9,
                      leg_adtn_attribute10,
                      leg_adtn_attribute11,
                      leg_adtn_attribute12,
                      leg_adtn_attribute13,
                      leg_adtn_attribute14,
                      leg_adtn_attribute15,
                      leg_adtn_attribute16,
                      leg_adtn_attribute17,
                      leg_adtn_attribute18,
                      leg_adtn_attribute19,
                      leg_adtn_attribute20,
                      leg_adtn_attribute21,
                      leg_adtn_attribute22,
                      leg_adtn_attribute23,
                      leg_adtn_attribute24,
                      leg_adtn_attribute25,
                      leg_adtn_attribute26,
                      leg_adtn_attribute27,
                      leg_adtn_attribute28,
                      leg_adtn_attribute29,
                      leg_adtn_attribute30,
                      leg_adtn_attr_category_code,
                      leg_adtn_context,
                      adtn_attribute1,
                      adtn_attribute2,
                      adtn_attribute3,
                      adtn_attribute4,
                      adtn_attribute5,
                      adtn_attribute6,
                      adtn_attribute7,
                      adtn_attribute8,
                      adtn_attribute9,
                      adtn_attribute10,
                      adtn_attribute11,
                      adtn_attribute12,
                      adtn_attribute13,
                      adtn_attribute14,
                      adtn_attribute15,
                      adtn_attribute16,
                      adtn_attribute17,
                      adtn_attribute18,
                      adtn_attribute19,
                      adtn_attribute20,
                      adtn_attribute21,
                      adtn_attribute22,
                      adtn_attribute23,
                      adtn_attribute24,
                      adtn_attribute25,
                      adtn_attribute26,
                      adtn_attribute27,
                      adtn_attribute28,
                      adtn_attribute29,
                      adtn_attribute30,
                      adtn_attribute_category_code,
                      adtn_context,
                      leg_inventorial,
                      leg_commitment,
                      leg_investment_law,
                      leg_adtn_global_attribute1,
                      leg_adtn_global_attribute2,
                      leg_adtn_global_attribute3,
                      leg_adtn_global_attribute4,
                      leg_adtn_global_attribute5,
                      leg_adtn_global_attribute6,
                      leg_adtn_global_attribute7,
                      leg_adtn_global_attribute8,
                      leg_adtn_global_attribute9,
                      leg_adtn_global_attribute10,
                      leg_adtn_global_attribute11,
                      leg_adtn_global_attribute12,
                      leg_adtn_global_attribute13,
                      leg_adtn_global_attribute14,
                      leg_adtn_global_attribute15,
                      leg_adtn_global_attribute16,
                      leg_adtn_global_attribute17,
                      leg_adtn_global_attribute18,
                      leg_adtn_global_attribute19,
                      leg_adtn_global_attribute20,
                      leg_adtn_global_attr_category,
                      adtn_global_attribute1,
                      adtn_global_attribute2,
                      adtn_global_attribute3,
                      adtn_global_attribute4,
                      adtn_global_attribute5,
                      adtn_global_attribute6,
                      adtn_global_attribute7,
                      adtn_global_attribute8,
                      adtn_global_attribute9,
                      adtn_global_attribute10,
                      adtn_global_attribute11,
                      adtn_global_attribute12,
                      adtn_global_attribute13,
                      adtn_global_attribute14,
                      adtn_global_attribute15,
                      adtn_global_attribute16,
                      adtn_global_attribute17,
                      adtn_global_attribute18,
                      adtn_global_attribute19,
                      adtn_global_attribute20,
                      adtn_global_attribute_category,
                      leg_book_type_code,
                      book_type_code,
                      leg_date_placed_in_service,
                      leg_deprn_start_date,
                      leg_deprn_method_code,
                      leg_life_in_months,
                      leg_rate_adjustment_factor,
                      leg_adjusted_cost,
                      leg_cost,
                      leg_original_cost,
                      leg_salvage_value,
                      leg_prorate_convention_code,
                      leg_prorate_date,
                      leg_cost_change_flag,
                      leg_adjustment_required_status,
                      leg_capitalize_flag,
                      leg_retirement_pending_flag,
                      leg_depreciate_flag,
                      leg_basic_rate,
                      leg_adjusted_rate,
                      leg_bonus_rule,
                      leg_ceiling_name,
                      leg_recoverable_cost,
                      leg_cap_period_name,
                      period_counter_capitalized,
                      leg_dep_period_name,
                      period_counter_fully_reserved,
                      leg_unrevalued_cost,
                      leg_annual_deprn_rounding_flag,
                      leg_percent_salvage_value,
                      leg_allowed_deprn_limit,
                      leg_allowed_deprn_limit_amount,
                      leg_salvage_type,
                      leg_deprn_limit_type,
                      leg_period_counter,
                      leg_deprn_source_code,
                      leg_deprn_run_date,
                      leg_deprn_amount,
                      leg_ytd_deprn,
                      leg_deprn_reserve,
                      leg_description,
                      leg_transaction_type_code,
                      leg_transaction_date_entered,
                      leg_transaction_subtype,
                      leg_amortization_start_date,
                      leg_books_global_attribute1,
                      leg_books_global_attribute2,
                      leg_books_global_attribute3,
                      leg_books_global_attribute4,
                      leg_books_global_attribute5,
                      leg_books_global_attribute6,
                      leg_books_global_attribute7,
                      leg_books_global_attribute8,
                      leg_books_global_attribute9,
                      leg_books_global_attribute10,
                      leg_books_global_attribute11,
                      leg_books_global_attribute12,
                      leg_books_global_attribute13,
                      leg_books_global_attribute14,
                      leg_books_global_attribute15,
                      leg_books_global_attribute16,
                      leg_books_global_attribute17,
                      leg_books_global_attribute18,
                      leg_books_global_attribute19,
                      leg_books_global_attribute20,
                      books_global_attribute1,
                      books_global_attribute2,
                      books_global_attribute3,
                      books_global_attribute4,
                      books_global_attribute5,
                      books_global_attribute6,
                      books_global_attribute7,
                      books_global_attribute8,
                      books_global_attribute9,
                      books_global_attribute10,
                      books_global_attribute11,
                      books_global_attribute12,
                      books_global_attribute13,
                      books_global_attribute14,
                      books_global_attribute15,
                      books_global_attribute16,
                      books_global_attribute17,
                      books_global_attribute18,
                      books_global_attribute19,
                      books_global_attribute20,
                      books_global_attr_category,
                      leg_books_global_attr_category,
                      batch_id,
                      process_flag,
                      run_sequence_id
        FROM xxfa_corp_asset_stg
       WHERE leg_parent_asset_number IS NOT NULL
         AND process_flag IN ('N', 'E') --1.1
         AND batch_id = g_batch_id
         AND run_sequence_id = g_new_run_seq_id;

    CURSOR get_distribution_details(p_legacy_asset_number VARCHAR2,
                                    p_book_type_code      VARCHAR2) IS
      SELECT interface_txn_id,
             leg_asset_number,
             leg_book_type_code,
             leg_locn_segment1,
             leg_locn_segment2,
             leg_locn_segment3,
             leg_locn_segment4,
             leg_locn_segment5,
             leg_locn_segment6,
             leg_assigned_emp_number,
             leg_units_assigned,
             leg_cc_segment1,
             leg_cc_segment2,
             leg_cc_segment3,
             leg_cc_segment4,
             leg_cc_segment5,
             leg_cc_segment6,
             leg_cc_segment7,
             leg_cc_segment8,
             leg_cc_segment9,
             leg_cc_segment10,
             process_flag
        FROM xxfa_corp_asset_stg
       WHERE leg_asset_number = p_legacy_asset_number
         AND leg_book_type_code = p_book_type_code
         AND process_flag IN ('N', 'E') --1.1
         AND batch_id = g_batch_id
         AND run_sequence_id = g_new_run_seq_id;

    CURSOR cur_property IS
      SELECT DISTINCT leg_property_type_code
        FROM xxfa_corp_asset_stg
       WHERE batch_id = g_batch_id
         AND run_sequence_id = g_new_run_seq_id;

    CURSOR cur_p1245 IS
      SELECT DISTINCT leg_property_1245_1250_code
        FROM xxfa_corp_asset_stg
       WHERE batch_id = g_batch_id
         AND run_sequence_id = g_new_run_seq_id;

    CURSOR cur_own IS
      SELECT DISTINCT leg_owned_leased
        FROM xxfa_corp_asset_stg
       WHERE batch_id = g_batch_id
         AND run_sequence_id = g_new_run_seq_id;

    CURSOR cur_new IS
      SELECT DISTINCT leg_new_used
        FROM xxfa_corp_asset_stg
       WHERE batch_id = g_batch_id
         AND run_sequence_id = g_new_run_seq_id;

    CURSOR cur_book IS
      SELECT DISTINCT /*leg_book_type_code*/ book_type_code ------v1.3 11th May,2015
        FROM xxfa_corp_asset_stg
       WHERE batch_id = g_batch_id
         AND run_sequence_id = g_new_run_seq_id;

    /*    CURSOR cur_deprn
    IS
       SELECT DISTINCT leg_deprn_method_code,
                       leg_life_in_months
                  FROM xxfa_corp_asset_stg
                 WHERE batch_id = g_batch_id
                   AND run_sequence_id = g_new_run_seq_id;*/

    CURSOR cur_deprn -----change as per the new logic based on baisc rate and adjusted rate
    IS
      SELECT DISTINCT leg_deprn_method_code,
                      leg_basic_rate,
                      leg_adjusted_rate, ---added leg_basic_rate and leg_adjusted_rate
                      leg_life_in_months
        FROM xxfa_corp_asset_stg
       WHERE batch_id = g_batch_id
         AND run_sequence_id = g_new_run_seq_id;

    CURSOR cur_pror IS
      SELECT DISTINCT leg_prorate_convention_code
        FROM xxfa_corp_asset_stg
       WHERE batch_id = g_batch_id
         AND run_sequence_id = g_new_run_seq_id;

    CURSOR cur_ceiling IS
      SELECT DISTINCT leg_ceiling_name
        FROM xxfa_corp_asset_stg
       WHERE batch_id = g_batch_id
         AND run_sequence_id = g_new_run_seq_id;

    CURSOR cur_code IS
      SELECT DISTINCT leg_cc_segment1,
                      leg_cc_segment2,
                      leg_cc_segment3,
                      leg_cc_segment4,
                      leg_cc_segment5,
                      leg_cc_segment6,
                      leg_cc_segment7
        FROM xxfa_corp_asset_stg
       WHERE batch_id = g_batch_id
         AND run_sequence_id = g_new_run_seq_id;

    CURSOR cur_location IS
      SELECT DISTINCT leg_locn_segment1,
                      leg_locn_segment2,
                      leg_locn_segment3,
                      leg_locn_segment4,
                      leg_locn_segment5,
                      leg_locn_segment6
        FROM xxfa_corp_asset_stg
       WHERE batch_id = g_batch_id
         AND run_sequence_id = g_new_run_seq_id;

    CURSOR c_der_book_info --    V1.2
    IS
      SELECT distinct leg_cc_segment1 || ' ' || leg_book_type_code book_type
        FROM xxfa_corp_asset_stg
       WHERE batch_id = g_batch_id
         AND run_sequence_id = g_new_run_seq_id;

    ln_property_code       NUMBER;
    lc_error_flag          VARCHAR2(1);
    ln_tax_category        VARCHAR2(240);
    ln_category_id         NUMBER;
    lc_cat_segment1        VARCHAR2(240);
    lc_cat_segment2        VARCHAR2(240);
    lc_cat_flag            VARCHAR2(1);
    lc_ass_flag            VARCHAR2(1);
    ln_prop_1245_1250_code NUMBER;
    ln_own_lease           NUMBER;
    ln_new_use             NUMBER;
    ln_life_in_months      NUMBER;
    ln_prorate_count       NUMBER;
    ln_ceiling_count       NUMBER;
    ln_bonus_count         NUMBER;
    ln_parent_count        NUMBER;
    ln_location_id         NUMBER;
    ln_employee_id         NUMBER;
    ln_book_code           NUMBER;
    ln_asset_key           NUMBER;
    lc_cat_msg             VARCHAR2(1000);
    x_out_acc_rec          xxetn_common_pkg.g_rec_type;
    x_ccid                 NUMBER;
    lc_dist_error_flag     VARCHAR2(1) := 'N';
    lc_master_dist_flag    VARCHAR2(1) := 'N';
    lv_attribute1          xxfa_corp_asset_stg.leg_adtn_attribute1%TYPE;
    lv_attribute2          xxfa_corp_asset_stg.leg_adtn_attribute2%TYPE;
    lv_attribute3          xxfa_corp_asset_stg.leg_adtn_attribute3%TYPE;
    lv_attribute4          xxfa_corp_asset_stg.leg_adtn_attribute4%TYPE;
    lv_attribute5          xxfa_corp_asset_stg.leg_adtn_attribute5%TYPE;
    lv_attribute6          xxfa_corp_asset_stg.leg_adtn_attribute6%TYPE;
    lv_attribute7          xxfa_corp_asset_stg.leg_adtn_attribute7%TYPE;
    lv_attribute8          xxfa_corp_asset_stg.leg_adtn_attribute8%TYPE;
    lv_attribute9          xxfa_corp_asset_stg.leg_adtn_attribute9%TYPE;
    lv_attribute10         xxfa_corp_asset_stg.leg_adtn_attribute10%TYPE;
    lv_attribute11         xxfa_corp_asset_stg.leg_adtn_attribute11%TYPE;
    lv_attribute12         xxfa_corp_asset_stg.leg_adtn_attribute12%TYPE;
    lv_attribute13         xxfa_corp_asset_stg.leg_adtn_attribute13%TYPE;
    lv_attribute14         xxfa_corp_asset_stg.leg_adtn_attribute14%TYPE;
    lv_attribute15         xxfa_corp_asset_stg.leg_adtn_attribute15%TYPE;
    lv_attribute16         xxfa_corp_asset_stg.leg_adtn_attribute16%TYPE;
    lv_attribute17         xxfa_corp_asset_stg.leg_adtn_attribute17%TYPE;
    lv_attribute18         xxfa_corp_asset_stg.leg_adtn_attribute18%TYPE;
    lv_attribute19         xxfa_corp_asset_stg.leg_adtn_attribute19%TYPE;
    lv_attribute20         xxfa_corp_asset_stg.leg_adtn_attribute20%TYPE;
    lv_attribute21         xxfa_corp_asset_stg.leg_adtn_attribute21%TYPE;
    lv_attribute22         xxfa_corp_asset_stg.leg_adtn_attribute22%TYPE;
    lv_attribute23         xxfa_corp_asset_stg.leg_adtn_attribute23%TYPE;
    lv_attribute24         xxfa_corp_asset_stg.leg_adtn_attribute24%TYPE;
    lv_attribute25         xxfa_corp_asset_stg.leg_adtn_attribute25%TYPE;
    lv_attribute26         xxfa_corp_asset_stg.leg_adtn_attribute26%TYPE;
    lv_attribute27         xxfa_corp_asset_stg.leg_adtn_attribute27%TYPE;
    lv_attribute28         xxfa_corp_asset_stg.leg_adtn_attribute28%TYPE;
    lv_attribute29         xxfa_corp_asset_stg.leg_adtn_attribute29%TYPE;
    lv_attribute30         xxfa_corp_asset_stg.leg_adtn_attribute30%TYPE;
    lv_adn_cat_code        xxfa_corp_asset_stg.leg_adtn_attr_category_code%TYPE;
    lv_adn_cont            xxfa_corp_asset_stg.leg_adtn_context%TYPE;
    lv_bk_gl_attribute1    xxfa_corp_asset_stg.leg_books_global_attribute1%TYPE;
    lv_bk_gl_attribute2    xxfa_corp_asset_stg.leg_books_global_attribute2%TYPE;
    lv_bk_gl_attribute3    xxfa_corp_asset_stg.leg_books_global_attribute3%TYPE;
    lv_bk_gl_attribute4    xxfa_corp_asset_stg.leg_books_global_attribute4%TYPE;
    lv_bk_gl_attribute5    xxfa_corp_asset_stg.leg_books_global_attribute5%TYPE;
    lv_bk_gl_attribute6    xxfa_corp_asset_stg.leg_books_global_attribute6%TYPE;
    lv_bk_gl_attribute7    xxfa_corp_asset_stg.leg_books_global_attribute7%TYPE;
    lv_bk_gl_attribute8    xxfa_corp_asset_stg.leg_books_global_attribute8%TYPE;
    lv_bk_gl_attribute9    xxfa_corp_asset_stg.leg_books_global_attribute9%TYPE;
    lv_bk_gl_attribute10   xxfa_corp_asset_stg.leg_books_global_attribute10%TYPE;
    lv_bk_gl_attribute11   xxfa_corp_asset_stg.leg_books_global_attribute11%TYPE;
    lv_bk_gl_attribute12   xxfa_corp_asset_stg.leg_books_global_attribute12%TYPE;
    lv_bk_gl_attribute13   xxfa_corp_asset_stg.leg_books_global_attribute13%TYPE;
    lv_bk_gl_attribute14   xxfa_corp_asset_stg.leg_books_global_attribute14%TYPE;
    lv_bk_gl_attribute15   xxfa_corp_asset_stg.leg_books_global_attribute15%TYPE;
    lv_bk_gl_attribute16   xxfa_corp_asset_stg.leg_books_global_attribute16%TYPE;
    lv_bk_gl_attribute17   xxfa_corp_asset_stg.leg_books_global_attribute17%TYPE;
    lv_bk_gl_attribute18   xxfa_corp_asset_stg.leg_books_global_attribute18%TYPE;
    lv_bk_gl_attribute19   xxfa_corp_asset_stg.leg_books_global_attribute19%TYPE;
    lv_bk_gl_attribute20   xxfa_corp_asset_stg.leg_books_global_attribute20%TYPE;
    lv_bk_gl_att_cat       xxfa_corp_asset_stg.leg_books_global_attr_category%TYPE;
    lv_adn_gl_attribute1   xxfa_corp_asset_stg.leg_adtn_global_attribute1%TYPE;
    lv_adn_gl_attribute2   xxfa_corp_asset_stg.leg_adtn_global_attribute2%TYPE;
    lv_adn_gl_attribute3   xxfa_corp_asset_stg.leg_adtn_global_attribute3%TYPE;
    lv_adn_gl_attribute4   xxfa_corp_asset_stg.leg_adtn_global_attribute4%TYPE;
    lv_adn_gl_attribute5   xxfa_corp_asset_stg.leg_adtn_global_attribute5%TYPE;
    lv_adn_gl_attribute6   xxfa_corp_asset_stg.leg_adtn_global_attribute6%TYPE;
    lv_adn_gl_attribute7   xxfa_corp_asset_stg.leg_adtn_global_attribute7%TYPE;
    lv_adn_gl_attribute8   xxfa_corp_asset_stg.leg_adtn_global_attribute8%TYPE;
    lv_adn_gl_attribute9   xxfa_corp_asset_stg.leg_adtn_global_attribute9%TYPE;
    lv_adn_gl_attribute10  xxfa_corp_asset_stg.leg_adtn_global_attribute10%TYPE;
    lv_adn_gl_attribute11  xxfa_corp_asset_stg.leg_adtn_global_attribute11%TYPE;
    lv_adn_gl_attribute12  xxfa_corp_asset_stg.leg_adtn_global_attribute12%TYPE;
    lv_adn_gl_attribute13  xxfa_corp_asset_stg.leg_adtn_global_attribute13%TYPE;
    lv_adn_gl_attribute14  xxfa_corp_asset_stg.leg_adtn_global_attribute14%TYPE;
    lv_adn_gl_attribute15  xxfa_corp_asset_stg.leg_adtn_global_attribute15%TYPE;
    lv_adn_gl_attribute16  xxfa_corp_asset_stg.leg_adtn_global_attribute16%TYPE;
    lv_adn_gl_attribute17  xxfa_corp_asset_stg.leg_adtn_global_attribute17%TYPE;
    lv_adn_gl_attribute18  xxfa_corp_asset_stg.leg_adtn_global_attribute18%TYPE;
    lv_adn_gl_attribute19  xxfa_corp_asset_stg.leg_adtn_global_attribute19%TYPE;
    lv_adn_gl_attribute20  xxfa_corp_asset_stg.leg_adtn_global_attribute20%TYPE;
    lv_adn_gl_cat          xxfa_corp_asset_stg.leg_adtn_global_attr_category%TYPE;
    ln_code                NUMBER;
    l_count                NUMBER;
    l_book_type            VARCHAR(2000);
    lv_rows_count          NUMBER;
    l_r12_book             fnd_lookup_values.tag%type;
   -- execute fnd_stats.gather_table_stats('xxextn','xxfa_corp_asset_stg', 20); -- added as per version v1.10 for perfromance tuning
  BEGIN
    g_ret_code := NULL;

    FOR cur_property_rec IN cur_property LOOP
      ln_code := NULL;

      IF cur_property_rec.leg_property_type_code IS NOT NULL THEN
        BEGIN
          SELECT 1
            INTO ln_code
            FROM fa_lookups flv
           WHERE flv.lookup_type = 'PROPERTY TYPE'
             AND flv.lookup_code = cur_property_rec.leg_property_type_code
             AND flv.enabled_flag = 'Y'
             AND TRUNC(SYSDATE) BETWEEN
                 NVL(flv.start_date_active, TRUNC(SYSDATE)) AND
                 NVL(flv.end_date_active, TRUNC(SYSDATE));
        EXCEPTION
          WHEN OTHERS THEN
            FOR r_org_ref_err_rec IN (SELECT DISTINCT leg_asset_number
                                        FROM xxfa_corp_asset_stg xis
                                       WHERE leg_property_type_code =
                                             cur_property_rec.leg_property_type_code
                                         AND batch_id = g_batch_id
                                         AND run_sequence_id =
                                             g_new_run_seq_id) LOOP
              log_errors(pin_interface_txn_id    => NULL,
                         piv_source_table        => 'XXFA_CORP_ASSET_STG',
                         piv_source_column_name  => 'leg_property_type_code',
                         piv_source_column_value => cur_property_rec.leg_property_type_code,
                         piv_source_keyname1     => 'leg_asset_number',
                         piv_source_keyvalue1    => r_org_ref_err_rec.leg_asset_number,
                         piv_source_keyname2     => NULL,
                         piv_source_keyvalue2    => NULL,
                         piv_error_type          => 'VAL_ERR',
                         piv_error_code          => 'ETN_FA_INVALID_LOOKUP_VAL',
                         piv_error_message       => 'Error : Invalid property_type_code value.');
            END LOOP;

            UPDATE xxfa_corp_asset_stg
               SET process_flag      = 'E',
                   ERROR_TYPE        = 'VAL_ERR',
                   request_id        = g_request_id,
                   last_updated_date = SYSDATE,
                   last_updated_by   = g_last_updated_by,
                   last_update_login = g_last_update_login
             WHERE leg_property_type_code =
                   cur_property_rec.leg_property_type_code
               AND batch_id = g_batch_id
               AND run_sequence_id = g_new_run_seq_id;

            COMMIT;
        END;
      END IF;
    END LOOP;

    FOR cur_own_rec IN cur_own LOOP
      ln_code := NULL;

      IF cur_own_rec.leg_owned_leased IS NOT NULL THEN
        BEGIN
          SELECT 1
            INTO ln_code
            FROM fa_lookups flv
           WHERE flv.lookup_type = 'OWNLEASE'
             AND flv.lookup_code = cur_own_rec.leg_owned_leased
             AND flv.enabled_flag = 'Y'
             AND TRUNC(SYSDATE) BETWEEN
                 NVL(flv.start_date_active, TRUNC(SYSDATE)) AND
                 NVL(flv.end_date_active, TRUNC(SYSDATE));
        EXCEPTION
          WHEN OTHERS THEN
            FOR r_org_ref_err_rec IN (SELECT DISTINCT leg_asset_number
                                        FROM xxfa_corp_asset_stg xis
                                       WHERE leg_owned_leased =
                                             cur_own_rec.leg_owned_leased
                                         AND batch_id = g_batch_id
                                         AND run_sequence_id =
                                             g_new_run_seq_id) LOOP
              log_errors(pin_interface_txn_id    => NULL,
                         piv_source_table        => 'XXFA_CORP_ASSET_STG',
                         piv_source_column_name  => 'leg_owned_leased',
                         piv_source_column_value => cur_own_rec.leg_owned_leased,
                         piv_source_keyname1     => 'leg_asset_number',
                         piv_source_keyvalue1    => r_org_ref_err_rec.leg_asset_number,
                         piv_source_keyname2     => NULL,
                         piv_source_keyvalue2    => NULL,
                         piv_error_type          => 'VAL_ERR',
                         piv_error_code          => 'ETN_FA_INVALID_LOOKUP_VAL',
                         piv_error_message       => 'Error : Invalid leg_owned_leased value.');
            END LOOP;

            UPDATE xxfa_corp_asset_stg
               SET process_flag      = 'E',
                   ERROR_TYPE        = 'VAL_ERR',
                   request_id        = g_request_id,
                   last_updated_date = SYSDATE,
                   last_updated_by   = g_last_updated_by,
                   last_update_login = g_last_update_login
             WHERE leg_owned_leased = cur_own_rec.leg_owned_leased
               AND batch_id = g_batch_id
               AND run_sequence_id = g_new_run_seq_id;

            COMMIT;
        END;
      END IF;
    END LOOP;

    FOR cur_p1245_rec IN cur_p1245 LOOP
      ln_code := NULL;

      IF cur_p1245_rec.leg_property_1245_1250_code IS NOT NULL THEN
        BEGIN
          SELECT 1
            INTO ln_code
            FROM fa_lookups flv
           WHERE flv.lookup_type = '1245/1250 PROPERTY'
             AND flv.lookup_code =
                 cur_p1245_rec.leg_property_1245_1250_code
             AND flv.enabled_flag = 'Y'
             AND TRUNC(SYSDATE) BETWEEN
                 NVL(flv.start_date_active, TRUNC(SYSDATE)) AND
                 NVL(flv.end_date_active, TRUNC(SYSDATE));
        EXCEPTION
          WHEN OTHERS THEN
            FOR r_org_ref_err_rec IN (SELECT DISTINCT leg_asset_number
                                        FROM xxfa_corp_asset_stg xis
                                       WHERE leg_property_1245_1250_code =
                                             cur_p1245_rec.leg_property_1245_1250_code
                                         AND batch_id = g_batch_id
                                         AND run_sequence_id =
                                             g_new_run_seq_id) LOOP
              log_errors(pin_interface_txn_id    => NULL,
                         piv_source_table        => 'XXFA_CORP_ASSET_STG',
                         piv_source_column_name  => 'leg_property_1245_1250_code',
                         piv_source_column_value => cur_p1245_rec.leg_property_1245_1250_code,
                         piv_source_keyname1     => 'leg_asset_number',
                         piv_source_keyvalue1    => r_org_ref_err_rec.leg_asset_number,
                         piv_source_keyname2     => NULL,
                         piv_source_keyvalue2    => NULL,
                         piv_error_type          => 'VAL_ERR',
                         piv_error_code          => 'ETN_FA_INVALID_LOOKUP_VAL',
                         piv_error_message       => 'Error : Invalid leg_property_1245_1250_code value.');
            END LOOP;

            UPDATE xxfa_corp_asset_stg
               SET process_flag      = 'E',
                   ERROR_TYPE        = 'VAL_ERR',
                   request_id        = g_request_id,
                   last_updated_date = SYSDATE,
                   last_updated_by   = g_last_updated_by,
                   last_update_login = g_last_update_login
             WHERE leg_property_1245_1250_code =
                   cur_p1245_rec.leg_property_1245_1250_code
               AND batch_id = g_batch_id
               AND run_sequence_id = g_new_run_seq_id;

            COMMIT;
        END;
      END IF;
    END LOOP;

    FOR cur_new_rec IN cur_new LOOP
      ln_code := NULL;

      IF cur_new_rec.leg_new_used IS NOT NULL THEN
        BEGIN
          SELECT 1
            INTO ln_code
            FROM fa_lookups flv
           WHERE flv.lookup_type = 'NEWUSE'
             AND flv.lookup_code = cur_new_rec.leg_new_used
             AND flv.enabled_flag = 'Y'
             AND TRUNC(SYSDATE) BETWEEN
                 NVL(flv.start_date_active, TRUNC(SYSDATE)) AND
                 NVL(flv.end_date_active, TRUNC(SYSDATE));
        EXCEPTION
          WHEN OTHERS THEN
            FOR r_org_ref_err_rec IN (SELECT DISTINCT leg_asset_number
                                        FROM xxfa_corp_asset_stg xis
                                       WHERE leg_new_used =
                                             cur_new_rec.leg_new_used
                                         AND batch_id = g_batch_id
                                         AND run_sequence_id =
                                             g_new_run_seq_id) LOOP
              log_errors(pin_interface_txn_id    => NULL,
                         piv_source_table        => 'XXFA_CORP_ASSET_STG',
                         piv_source_column_name  => 'leg_new_used',
                         piv_source_column_value => cur_new_rec.leg_new_used,
                         piv_source_keyname1     => 'leg_asset_number',
                         piv_source_keyvalue1    => r_org_ref_err_rec.leg_asset_number,
                         piv_source_keyname2     => NULL,
                         piv_source_keyvalue2    => NULL,
                         piv_error_type          => 'VAL_ERR',
                         piv_error_code          => 'ETN_FA_INVALID_LOOKUP_VAL',
                         piv_error_message       => 'Error : Invalid leg_new_used value.');
            END LOOP;

            UPDATE xxfa_corp_asset_stg
               SET process_flag      = 'E',
                   ERROR_TYPE        = 'VAL_ERR',
                   request_id        = g_request_id,
                   last_updated_date = SYSDATE,
                   last_updated_by   = g_last_updated_by,
                   last_update_login = g_last_update_login
             WHERE leg_new_used = cur_new_rec.leg_new_used
               AND batch_id = g_batch_id
               AND run_sequence_id = g_new_run_seq_id;

            COMMIT;
        END;
      END IF;
    END LOOP;
    -----------------------------------------------------------------------------------
    -------------------------------------------------------------------------
    --   Adding to derive the value from the Lookup Harjinder Singh
    -------------------------------------------------------------------------

    FOR c_der_book_info_rec IN c_der_book_info LOOP

      l_book_type := c_der_book_info_rec.book_type;

      BEGIN
        SELECT tag
          INTO l_r12_book
          FROM fnd_lookup_values flv
         WHERE lookup_type = g_lookup_r12_type
           and language = 'US' ------v1.3 11th May,2015
           AND lookup_code = c_der_book_info_rec.book_type
           AND TRUNC(SYSDATE) BETWEEN
               NVL(flv.start_date_active, TRUNC(SYSDATE)) AND
               NVL(flv.end_date_active, TRUNC(SYSDATE));

        UPDATE xxfa_corp_asset_stg
           SET book_type_code    = l_r12_book,
               request_id        = g_request_id,
               last_updated_date = SYSDATE,
               last_updated_by   = g_last_updated_by,
               last_update_login = g_last_update_login
         WHERE leg_cc_segment1 || ' ' || leg_book_type_code =
               c_der_book_info_rec.book_type;
        COMMIT;
      EXCEPTION
        WHEN OTHERS THEN

          FOR r_der_book_info_rec IN (SELECT interface_txn_id
                                        FROM xxfa_corp_asset_stg xis
                                       WHERE leg_cc_segment1 || ' ' ||
                                             leg_book_type_code =
                                             c_der_book_info_rec.book_type
                                         AND batch_id = g_batch_id
                                         AND run_sequence_id =
                                             g_new_run_seq_id) LOOP
            log_errors(pin_interface_txn_id    => NULL,
                       piv_source_table        => 'XXFA_CORP_ASSET_STG',
                       piv_source_column_name  => 'book_type_code',
                       piv_source_column_value => c_der_book_info_rec.book_type,
                       piv_source_keyname1     => 'interface_txn_id',
                       piv_source_keyvalue1    => r_der_book_info_rec.interface_txn_id,
                       piv_error_type          => 'VAL_ERR',
                       piv_error_code          => 'ETN_FA_INVALID_BOOK_TYPE',
                       piv_error_message       => 'Error : Invalid Book Type.');
          END LOOP;

          UPDATE xxfa_corp_asset_stg
             SET process_flag      = 'E',
                 ERROR_TYPE        = 'VAL_ERR',
                 request_id        = g_request_id,
                 last_updated_date = SYSDATE,
                 last_updated_by   = g_last_updated_by,
                 last_update_login = g_last_update_login
           where leg_cc_segment1 || ' ' || leg_book_type_code =
                 c_der_book_info_rec.book_type;

          COMMIT;

      END;
    END LOOP;

    ------------------------------------------------------------------------------------------
    FOR cur_book_rec IN cur_book LOOP
      ln_book_code := NULL;

      fnd_file.put_line(fnd_file.log, '4');

      /* IF cur_book_rec.leg_book_type_code IS NULL*/
      IF cur_book_rec.book_type_code IS NULL ------v1.3 11th May,2015
       THEN
        FOR r_org_ref_err_rec IN (SELECT DISTINCT leg_asset_number
                                    FROM xxfa_corp_asset_stg xis
                                   WHERE /*leg_book_type_code*/
                                   book_type_code IS NULL ------v1.3 11th May,2015
                               AND batch_id = g_batch_id
                               AND run_sequence_id = g_new_run_seq_id) LOOP
          log_errors(pin_interface_txn_id    => NULL,
                     piv_source_table        => 'XXFA_CORP_ASSET_STG',
                     piv_source_column_name  => /*'leg_book_type_code'*/ 'book_type_code', ------v1.3 11th May,2015
                     piv_source_column_value => NULL,
                     piv_source_keyname1     => 'leg_asset_number',
                     piv_source_keyvalue1    => r_org_ref_err_rec.leg_asset_number,
                     piv_error_type          => 'VAL_ERR',
                     piv_error_code          => 'ETN_FA_MANDATORY_COLUMN',
                     piv_error_message       => 'Error : Book Type Cannot be NULL');
        END LOOP;

        UPDATE xxfa_corp_asset_stg
           SET process_flag      = 'E',
               ERROR_TYPE        = 'VAL_ERR',
               request_id        = g_request_id,
               last_updated_date = SYSDATE,
               last_updated_by   = g_last_updated_by,
               last_update_login = g_last_update_login
         WHERE /*leg_book_type_code*/
         book_type_code IS NULL ------v1.3 11th May,2015
         AND batch_id = g_batch_id
         AND run_sequence_id = g_new_run_seq_id;

        COMMIT;
      ELSE
        BEGIN
          SELECT 1
            INTO ln_book_code
            FROM fa_book_controls
           WHERE book_type_code = /*cur_book_rec.leg_book_type_code*/
                 cur_book_rec.book_type_code; ------v1.3 11th May,2015
        EXCEPTION
          WHEN OTHERS THEN
            FOR r_org_ref_err_rec IN (SELECT DISTINCT leg_asset_number
                                        FROM xxfa_corp_asset_stg xis
                                       WHERE /*leg_book_type_code*/
                                       book_type_code =
                                      /*cur_book_rec.leg_book_type_code*/
                                       cur_book_rec.book_type_code ------v1.3 11th May,2015
                                   AND batch_id = g_batch_id
                                   AND run_sequence_id = g_new_run_seq_id) LOOP
              log_errors(pin_interface_txn_id    => NULL,
                         piv_source_table        => 'XXFA_CORP_ASSET_STG',
                         piv_source_column_name  => /*'leg_book_type_code'*/ 'book_type_code', ------v1.3 11th May,2015
                         piv_source_column_value => /*cur_book_rec.leg_book_type_code*/ cur_book_rec.book_type_code, ------v1.3 11th May,2015
                         piv_source_keyname1     => 'leg_asset_number',
                         piv_source_keyvalue1    => r_org_ref_err_rec.leg_asset_number,
                         piv_error_type          => 'VAL_ERR',
                         piv_error_code          => 'ETN_FA_INVALID_BOOK_TYPE',
                         piv_error_message       => 'Error : Book Type is Invalid.');
            END LOOP;

            UPDATE xxfa_corp_asset_stg
               SET process_flag      = 'E',
                   ERROR_TYPE        = 'VAL_ERR',
                   request_id        = g_request_id,
                   last_updated_date = SYSDATE,
                   last_updated_by   = g_last_updated_by,
                   last_update_login = g_last_update_login
             WHERE /*leg_book_type_code = cur_book_rec.leg_book_type_code*/ ------v1.3 11th May,2015
             book_type_code = cur_book_rec.book_type_code
             AND batch_id = g_batch_id
             AND run_sequence_id = g_new_run_seq_id;

            COMMIT;
        END;
      END IF;
    END LOOP;

    FOR cur_deprn_rec IN cur_deprn LOOP
      ln_life_in_months := NULL;

      IF cur_deprn_rec.leg_deprn_method_code IS NULL THEN
        FOR r_org_ref_err_rec IN (SELECT DISTINCT leg_asset_number
                                    FROM xxfa_corp_asset_stg xis
                                   WHERE leg_deprn_method_code IS NULL
                                     AND batch_id = g_batch_id
                                     AND run_sequence_id = g_new_run_seq_id) LOOP
          log_errors(pin_interface_txn_id    => NULL,
                     piv_source_table        => 'XXFA_CORP_ASSET_STG',
                     piv_source_column_name  => 'leg_deprn_method_code',
                     piv_source_column_value => NULL,
                     piv_source_keyname1     => 'leg_asset_number',
                     piv_source_keyvalue1    => r_org_ref_err_rec.leg_asset_number,
                     piv_error_type          => 'VAL_ERR',
                     piv_error_code          => 'ETN_FA_MANDATORY_COLUMN',
                     piv_error_message       => 'Error : Depreciation Method cannot be NULL');
        END LOOP;

        UPDATE xxfa_corp_asset_stg
           SET process_flag      = 'E',
               ERROR_TYPE        = 'VAL_ERR',
               request_id        = g_request_id,
               last_updated_date = SYSDATE,
               last_updated_by   = g_last_updated_by,
               last_update_login = g_last_update_login
         WHERE leg_deprn_method_code IS NULL
           AND batch_id = g_batch_id
           AND run_sequence_id = g_new_run_seq_id;

        COMMIT;
      ELSE
        BEGIN

          IF cur_deprn_rec.leg_basic_rate IS NOT NULL THEN
            BEGIN
              SELECT 1
                INTO ln_life_in_months
                FROM fa.fa_methods Fm, fa.fa_flat_rates ffr
               WHERE Fm.METHOD_ID = ffr.METHOD_ID(+)
                 AND ffr.basic_rate = cur_deprn_rec.leg_basic_rate
                 AND ffr.adjusted_rate = cur_deprn_rec.leg_adjusted_rate
                 AND method_code = cur_deprn_rec.leg_deprn_method_code;
            EXCEPTION
              WHEN OTHERS THEN
                FOR r_org_ref_err_rec IN (SELECT DISTINCT leg_asset_number
                                            FROM xxfa_corp_asset_stg xis
                                           WHERE leg_deprn_method_code =
                                                 cur_deprn_rec.leg_deprn_method_code
                                             AND leg_basic_rate =
                                                 cur_deprn_rec.leg_basic_rate
                                             AND leg_adjusted_rate =
                                                 cur_deprn_rec.leg_adjusted_rate

                                             AND batch_id = g_batch_id
                                             AND run_sequence_id =
                                                 g_new_run_seq_id) LOOP
                  log_errors(pin_interface_txn_id    => NULL,
                             piv_source_table        => 'XXFA_CORP_ASSET_STG',
                             piv_source_column_name  => 'leg_basic_rate||leg_adjusted_rate',
                             piv_source_column_value => cur_deprn_rec.leg_basic_rate || '||' ||
                                                        cur_deprn_rec.leg_adjusted_rate,
                             piv_source_keyname1     => 'leg_asset_number',
                             piv_source_keyvalue1    => r_org_ref_err_rec.leg_asset_number,
                             piv_error_type          => 'VAL_ERR',
                             piv_error_code          => 'ETN_FA_INVALID_DEPRN_METHOD',
                             piv_error_message       => 'Error : Depreciation Method is Invalid. for Rates');
                END LOOP;

                UPDATE xxfa_corp_asset_stg
                   SET process_flag      = 'E',
                       ERROR_TYPE        = 'VAL_ERR',
                       request_id        = g_request_id,
                       last_updated_date = SYSDATE,
                       last_updated_by   = g_last_updated_by,
                       last_update_login = g_last_update_login
                 WHERE leg_deprn_method_code =
                       cur_deprn_rec.leg_deprn_method_code

                   AND leg_basic_rate = cur_deprn_rec.leg_basic_rate
                   AND leg_adjusted_rate = cur_deprn_rec.leg_adjusted_rate
                   AND batch_id = g_batch_id
                   AND run_sequence_id = g_new_run_seq_id;

                COMMIT;
            END;

          ELSE
            --- else for rate null and life in months not null

            BEGIN

              SELECT 1
                INTO ln_life_in_months
                FROM fa_methods
               WHERE method_code = cur_deprn_rec.leg_deprn_method_code
                 AND NVL(life_in_months, -999) =
                     NVL(cur_deprn_rec.leg_life_in_months, -999); ---commented on 21st Oct 2015 as per Monica's suggestion
            EXCEPTION
              WHEN OTHERS THEN
                FOR r_org_ref_err_rec IN (SELECT DISTINCT leg_asset_number
                                            FROM xxfa_corp_asset_stg xis
                                           WHERE leg_deprn_method_code =
                                                 cur_deprn_rec.leg_deprn_method_code
                                             AND NVL(leg_life_in_months,
                                                     -999) =
                                                 NVL(cur_deprn_rec.leg_life_in_months,
                                                     -999)
                                             AND batch_id = g_batch_id
                                             AND run_sequence_id =
                                                 g_new_run_seq_id) LOOP
                  log_errors(pin_interface_txn_id    => NULL,
                             piv_source_table        => 'XXFA_CORP_ASSET_STG',
                             piv_source_column_name  => 'leg_deprn_method_code||life_in_months',
                             piv_source_column_value => cur_deprn_rec.leg_deprn_method_code || '||' ||
                                                        cur_deprn_rec.leg_life_in_months,
                             piv_source_keyname1     => 'leg_asset_number',
                             piv_source_keyvalue1    => r_org_ref_err_rec.leg_asset_number,
                             piv_error_type          => 'VAL_ERR',
                             piv_error_code          => 'ETN_FA_INVALID_DEPRN_METHOD',
                             piv_error_message       => 'Error : Depreciation Method is Invalid.');
                END LOOP;

                UPDATE xxfa_corp_asset_stg
                   SET process_flag      = 'E',
                       ERROR_TYPE        = 'VAL_ERR',
                       request_id        = g_request_id,
                       last_updated_date = SYSDATE,
                       last_updated_by   = g_last_updated_by,
                       last_update_login = g_last_update_login
                 WHERE leg_deprn_method_code =
                       cur_deprn_rec.leg_deprn_method_code
                   AND NVL(leg_life_in_months, -999) =
                       NVL(cur_deprn_rec.leg_life_in_months, -999)
                   AND batch_id = g_batch_id
                   AND run_sequence_id = g_new_run_seq_id;

                COMMIT;
            END;
          END IF;
        END;

      END IF;
    END LOOP;

    FOR cur_pror_rec IN cur_pror LOOP
      ln_prorate_count := NULL;

      IF cur_pror_rec.leg_prorate_convention_code IS NULL THEN
        FOR r_org_ref_err_rec IN (SELECT DISTINCT leg_asset_number
                                    FROM xxfa_corp_asset_stg xis
                                   WHERE leg_prorate_convention_code IS NULL
                                     AND batch_id = g_batch_id
                                     AND run_sequence_id = g_new_run_seq_id) LOOP
          log_errors(pin_interface_txn_id    => NULL,
                     piv_source_table        => 'XXFA_CORP_ASSET_STG',
                     piv_source_column_name  => 'leg_prorate_convention_code',
                     piv_source_column_value => NULL,
                     piv_source_keyname1     => 'leg_asset_number',
                     piv_source_keyvalue1    => r_org_ref_err_rec.leg_asset_number,
                     piv_error_type          => 'VAL_ERR',
                     piv_error_code          => 'ETN_FA_MANDATORY_COLUMN',
                     piv_error_message       => 'Error : Prorate Convention code cannot be NULL');
        END LOOP;

        UPDATE xxfa_corp_asset_stg
           SET process_flag      = 'E',
               ERROR_TYPE        = 'VAL_ERR',
               request_id        = g_request_id,
               last_updated_date = SYSDATE,
               last_updated_by   = g_last_updated_by,
               last_update_login = g_last_update_login
         WHERE leg_prorate_convention_code IS NULL
           AND batch_id = g_batch_id
           AND run_sequence_id = g_new_run_seq_id;

        COMMIT;
      ELSE
        BEGIN
          SELECT 1
            INTO ln_prorate_count
            FROM fa_convention_types
           WHERE prorate_convention_code =
                 cur_pror_rec.leg_prorate_convention_code;
        EXCEPTION
          WHEN OTHERS THEN
            FOR r_org_ref_err_rec IN (SELECT DISTINCT leg_asset_number
                                        FROM xxfa_corp_asset_stg xis
                                       WHERE leg_prorate_convention_code =
                                             cur_pror_rec.leg_prorate_convention_code
                                         AND batch_id = g_batch_id
                                         AND run_sequence_id =
                                             g_new_run_seq_id) LOOP
              log_errors(pin_interface_txn_id    => NULL,
                         piv_source_table        => 'XXFA_CORP_ASSET_STG',
                         piv_source_column_name  => 'leg_prorate_convention_code',
                         piv_source_column_value => cur_pror_rec.leg_prorate_convention_code,
                         piv_source_keyname1     => 'leg_asset_number',
                         piv_source_keyvalue1    => r_org_ref_err_rec.leg_asset_number,
                         piv_error_type          => 'VAL_ERR',
                         piv_error_code          => 'ETN_FA_INVALID_DEPRN_METHOD',
                         piv_error_message       => 'Error : Prorate Convention code is Invalid.');
            END LOOP;

            UPDATE xxfa_corp_asset_stg
               SET process_flag      = 'E',
                   ERROR_TYPE        = 'VAL_ERR',
                   request_id        = g_request_id,
                   last_updated_date = SYSDATE,
                   last_updated_by   = g_last_updated_by,
                   last_update_login = g_last_update_login
             WHERE leg_prorate_convention_code =
                   cur_pror_rec.leg_prorate_convention_code
               AND batch_id = g_batch_id
               AND run_sequence_id = g_new_run_seq_id;

            COMMIT;
        END;
      END IF;
    END LOOP;

    FOR cur_ceiling_rec IN cur_ceiling LOOP
      ln_ceiling_count := 0;

      IF cur_ceiling_rec.leg_ceiling_name IS NOT NULL THEN
        BEGIN
          SELECT COUNT(1)
            INTO ln_ceiling_count
            FROM fa_ceilings
           WHERE ceiling_name = cur_ceiling_rec.leg_ceiling_name;
        EXCEPTION
          WHEN OTHERS THEN
            ln_ceiling_count := 0;
        END;

        IF ln_ceiling_count = 0 THEN
          FOR r_org_ref_err_rec IN (SELECT DISTINCT leg_asset_number
                                      FROM xxfa_corp_asset_stg xis
                                     WHERE leg_ceiling_name =
                                           cur_ceiling_rec.leg_ceiling_name
                                       AND batch_id = g_batch_id
                                       AND run_sequence_id =
                                           g_new_run_seq_id) LOOP
            log_errors(pin_interface_txn_id    => NULL,
                       piv_source_table        => 'XXFA_CORP_ASSET_STG',
                       piv_source_column_name  => 'leg_ceiling_name',
                       piv_source_column_value => cur_ceiling_rec.leg_ceiling_name,
                       piv_source_keyname1     => 'leg_asset_number',
                       piv_source_keyvalue1    => r_org_ref_err_rec.leg_asset_number,
                       piv_error_type          => 'VAL_ERR',
                       piv_error_code          => 'ETN_FA_INVALID_DEPRN_METHOD',
                       piv_error_message       => 'Error : Prorate Convention code is Invalid.');
          END LOOP;

          UPDATE xxfa_corp_asset_stg
             SET process_flag      = 'E',
                 ERROR_TYPE        = 'VAL_ERR',
                 request_id        = g_request_id,
                 last_updated_date = SYSDATE,
                 last_updated_by   = g_last_updated_by,
                 last_update_login = g_last_update_login
           WHERE leg_ceiling_name = cur_ceiling_rec.leg_ceiling_name
             AND batch_id = g_batch_id
             AND run_sequence_id = g_new_run_seq_id;

          COMMIT;
        END IF;
      END IF;
    END LOOP;

    FOR cur_location_rec IN cur_location LOOP
      ln_location_id := NULL;

      IF (cur_location_rec.leg_locn_segment1 IS NULL) OR
         (cur_location_rec.leg_locn_segment2 IS NULL) OR
         (cur_location_rec.leg_locn_segment3 IS NULL) OR
         (cur_location_rec.leg_locn_segment4 IS NULL) THEN
        FOR r_org_ref_err_rec IN (SELECT interface_txn_id
                                    FROM xxfa_corp_asset_stg xis
                                   WHERE (leg_locn_segment1 IS NULL OR
                                         leg_locn_segment2 IS NULL OR
                                         leg_locn_segment3 IS NULL OR
                                         leg_locn_segment4 IS NULL)
                                     AND batch_id = g_batch_id
                                     AND run_sequence_id = g_new_run_seq_id) LOOP
          log_errors(pin_interface_txn_id    => NULL,
                     piv_source_table        => 'XXFA_CORP_ASSET_STG',
                     piv_source_column_name  => 'leg_locn_segments_conc_value',
                     piv_source_column_value => cur_location_rec.leg_locn_segment1 || '.' ||
                                                cur_location_rec.leg_locn_segment2 || '.' ||
                                                cur_location_rec.leg_locn_segment3 || '.' ||
                                                cur_location_rec.leg_locn_segment4,
                     piv_source_keyname1     => 'interface_txn_id',
                     piv_source_keyvalue1    => r_org_ref_err_rec.interface_txn_id,
                     piv_error_type          => 'VAL_ERR',
                     piv_error_code          => 'ETN_FA_INVALID_LOCATION',
                     piv_error_message       => 'Error : Mandatory column not entered.');
        END LOOP;

        UPDATE xxfa_corp_asset_stg
           SET process_flag      = 'E',
               ERROR_TYPE        = 'VAL_ERR',
               request_id        = g_request_id,
               last_updated_date = SYSDATE,
               last_updated_by   = g_last_updated_by,
               last_update_login = g_last_update_login
         WHERE (leg_locn_segment1 IS NULL OR leg_locn_segment2 IS NULL OR
               leg_locn_segment3 IS NULL OR leg_locn_segment4 IS NULL)
           AND batch_id = g_batch_id
           AND run_sequence_id = g_new_run_seq_id;

        COMMIT;
      ELSE
        BEGIN
          SELECT location_id
            INTO ln_location_id
            FROM fa_locations
           WHERE enabled_flag = 'Y'
             AND segment1 = cur_location_rec.leg_locn_segment1
             AND segment2 = cur_location_rec.leg_locn_segment2
             AND segment3 = cur_location_rec.leg_locn_segment3
             AND segment4 = cur_location_rec.leg_locn_segment4
             AND NVL(segment5, 'X') =
                 NVL(cur_location_rec.leg_locn_segment5, 'X')
             AND NVL(segment6, 'X') =
                 NVL(cur_location_rec.leg_locn_segment6, 'X');

          UPDATE xxfa_corp_asset_stg
             SET location_id       = ln_location_id,
                 request_id        = g_request_id,
                 last_updated_date = SYSDATE,
                 last_updated_by   = g_last_updated_by,
                 last_update_login = g_last_update_login
           WHERE leg_locn_segment1 = cur_location_rec.leg_locn_segment1
             AND leg_locn_segment2 = cur_location_rec.leg_locn_segment2
             AND leg_locn_segment3 = cur_location_rec.leg_locn_segment3
             AND leg_locn_segment4 = cur_location_rec.leg_locn_segment4
             AND NVL(leg_locn_segment5, 'X') =
                 NVL(cur_location_rec.leg_locn_segment5, 'X')
             AND NVL(leg_locn_segment6, 'X') =
                 NVL(cur_location_rec.leg_locn_segment6, 'X')
             AND batch_id = g_batch_id
             AND run_sequence_id = g_new_run_seq_id;

          COMMIT;
        EXCEPTION
          WHEN OTHERS THEN
            FOR r_org_ref_err_rec IN (SELECT interface_txn_id
                                        FROM xxfa_corp_asset_stg xis
                                       WHERE leg_locn_segment1 =
                                             cur_location_rec.leg_locn_segment1
                                         AND leg_locn_segment2 =
                                             cur_location_rec.leg_locn_segment2
                                         AND leg_locn_segment3 =
                                             cur_location_rec.leg_locn_segment3
                                         AND leg_locn_segment4 =
                                             cur_location_rec.leg_locn_segment4
                                         AND NVL(leg_locn_segment5, 'X') =
                                             NVL(cur_location_rec.leg_locn_segment5,
                                                 'X')
                                         AND NVL(leg_locn_segment6, 'X') =
                                             NVL(cur_location_rec.leg_locn_segment6,
                                                 'X')
                                         AND batch_id = g_batch_id
                                         AND run_sequence_id =
                                             g_new_run_seq_id) LOOP
              log_errors(pin_interface_txn_id    => NULL,
                         piv_source_table        => 'XXFA_CORP_ASSET_STG',
                         piv_source_column_name  => 'leg_locn_segments_conc_value',
                         piv_source_column_value => cur_location_rec.leg_locn_segment1 || '.' ||
                                                    cur_location_rec.leg_locn_segment2 || '.' ||
                                                    cur_location_rec.leg_locn_segment3 || '.' ||
                                                    cur_location_rec.leg_locn_segment4 || '.' ||
                                                    cur_location_rec.leg_locn_segment5 || '.' ||
                                                    cur_location_rec.leg_locn_segment6,
                         piv_source_keyname1     => 'interface_txn_id',
                         piv_source_keyvalue1    => r_org_ref_err_rec.interface_txn_id,
                         piv_error_type          => 'VAL_ERR',
                         piv_error_code          => 'ETN_FA_INVALID_LOCATION',
                         piv_error_message       => 'Error : Invalid Location Value.');
            END LOOP;

            UPDATE xxfa_corp_asset_stg
               SET process_flag      = 'E',
                   ERROR_TYPE        = 'VAL_ERR',
                   request_id        = g_request_id,
                   last_updated_date = SYSDATE,
                   last_updated_by   = g_last_updated_by,
                   last_update_login = g_last_update_login
             WHERE leg_locn_segment1 = cur_location_rec.leg_locn_segment1
               AND leg_locn_segment2 = cur_location_rec.leg_locn_segment2
               AND leg_locn_segment3 = cur_location_rec.leg_locn_segment3
               AND leg_locn_segment4 = cur_location_rec.leg_locn_segment4
               AND NVL(leg_locn_segment5, 'X') =
                   NVL(cur_location_rec.leg_locn_segment5, 'X')
               AND NVL(leg_locn_segment6, 'X') =
                   NVL(cur_location_rec.leg_locn_segment6, 'X')
               AND batch_id = g_batch_id
               AND run_sequence_id = g_new_run_seq_id;

            COMMIT;
        END;
      END IF;
    END LOOP;

    FOR cur_code_rec IN cur_code LOOP
      x_ccid        := NULL;
      x_out_acc_rec := NULL;
      validate_accounts(NULL,
                        cur_code_rec.leg_cc_segment1,
                        cur_code_rec.leg_cc_segment2,
                        cur_code_rec.leg_cc_segment3,
                        cur_code_rec.leg_cc_segment4,
                        cur_code_rec.leg_cc_segment5,
                        cur_code_rec.leg_cc_segment6,
                        cur_code_rec.leg_cc_segment7,
                        x_out_acc_rec,
                        x_ccid);

      IF x_ccid IS NULL THEN
        UPDATE xxfa_corp_asset_stg
           SET process_flag        = 'E',
               ERROR_TYPE          = 'VAL_ERR',
               request_id          = g_request_id,
               last_updated_date   = SYSDATE,
               last_updated_by     = g_last_updated_by,
               last_update_login   = g_last_update_login,
               cc_segment1         = x_out_acc_rec.segment1,
               cc_segment2         = x_out_acc_rec.segment2,
               cc_segment3         = x_out_acc_rec.segment3,
               cc_segment4         = x_out_acc_rec.segment4,
               cc_segment5         = x_out_acc_rec.segment5,
               cc_segment6         = x_out_acc_rec.segment6,
               cc_segment7         = x_out_acc_rec.segment7,
               cc_segment8         = x_out_acc_rec.segment8,
               cc_segment9         = x_out_acc_rec.segment9,
               cc_segment10        = x_out_acc_rec.segment10,
               acct_combination_id = x_ccid
         WHERE leg_cc_segment1 = cur_code_rec.leg_cc_segment1
           AND leg_cc_segment2 = cur_code_rec.leg_cc_segment2
           AND leg_cc_segment3 = cur_code_rec.leg_cc_segment3
           AND leg_cc_segment4 = cur_code_rec.leg_cc_segment4
           AND leg_cc_segment5 = cur_code_rec.leg_cc_segment5
           AND leg_cc_segment6 = cur_code_rec.leg_cc_segment6
           AND leg_cc_segment7 = cur_code_rec.leg_cc_segment7
           AND batch_id = g_batch_id
           AND run_sequence_id = g_new_run_seq_id;
      ELSE
        UPDATE xxfa_corp_asset_stg
           SET request_id          = g_request_id,
               last_updated_date   = SYSDATE,
               last_updated_by     = g_last_updated_by,
               last_update_login   = g_last_update_login,
               cc_segment1         = x_out_acc_rec.segment1,
               cc_segment2         = x_out_acc_rec.segment2,
               cc_segment3         = x_out_acc_rec.segment3,
               cc_segment4         = x_out_acc_rec.segment4,
               cc_segment5         = x_out_acc_rec.segment5,
               cc_segment6         = x_out_acc_rec.segment6,
               cc_segment7         = x_out_acc_rec.segment7,
               cc_segment8         = x_out_acc_rec.segment8,
               cc_segment9         = x_out_acc_rec.segment9,
               cc_segment10        = x_out_acc_rec.segment10,
               acct_combination_id = x_ccid
         WHERE leg_cc_segment1 = cur_code_rec.leg_cc_segment1
           AND leg_cc_segment2 = cur_code_rec.leg_cc_segment2
           AND leg_cc_segment3 = cur_code_rec.leg_cc_segment3
           AND leg_cc_segment4 = cur_code_rec.leg_cc_segment4
           AND leg_cc_segment5 = cur_code_rec.leg_cc_segment5
           AND leg_cc_segment6 = cur_code_rec.leg_cc_segment6
           AND leg_cc_segment7 = cur_code_rec.leg_cc_segment7
           AND batch_id = g_batch_id
           AND run_sequence_id = g_new_run_seq_id;
      END IF;

      COMMIT;
    END LOOP;

    --- to error out whole asset which erred out due to dependent distributions
    UPDATE xxfa_corp_asset_stg
       SET process_flag      = 'E',
           ERROR_TYPE        = 'VAL_ERR',
           request_id        = g_request_id,
           last_updated_date = SYSDATE,
           last_updated_by   = g_last_updated_by,
           last_update_login = g_last_update_login
     WHERE leg_asset_number IN
           (SELECT DISTINCT leg_asset_number
              FROM xxfa_corp_asset_stg
             WHERE process_flag = 'E'
               AND batch_id = g_batch_id
               AND run_sequence_id = g_new_run_seq_id)
       AND batch_id = g_batch_id
       AND run_sequence_id = g_new_run_seq_id;

    COMMIT;
    l_count := 0;

    FOR rec_cur_val_parent IN cur_val_parent LOOP
      lc_error_flag          := 'N';
      ln_property_code       := NULL;
      ln_tax_category        := NULL;
      ln_category_id         := NULL;
      lc_cat_segment1        := NULL;
      lc_cat_segment2        := NULL;
      lc_cat_flag            := NULL;
      ln_prop_1245_1250_code := NULL;
      ln_own_lease           := NULL;
      ln_new_use             := NULL;
      ln_life_in_months      := NULL;
      ln_prorate_count       := NULL;
      ln_ceiling_count       := NULL;
      ln_bonus_count         := NULL;
      ln_book_code           := NULL;
      ln_asset_key           := NULL;
      lc_cat_msg             := NULL;
      lc_ass_flag            := NULL;
      lv_attribute1          := NULL;
      lv_attribute2          := NULL;
      lv_attribute3          := NULL;
      lv_attribute4          := NULL;
      lv_attribute5          := NULL;
      lv_attribute6          := NULL;
      lv_attribute7          := NULL;
      lv_attribute8          := NULL;
      lv_attribute9          := NULL;
      lv_attribute10         := NULL;
      lv_attribute11         := NULL;
      lv_attribute12         := NULL;
      lv_attribute13         := NULL;
      lv_attribute14         := NULL;
      lv_attribute15         := NULL;
      lv_attribute16         := NULL;
      lv_attribute17         := NULL;
      lv_attribute18         := NULL;
      lv_attribute19         := NULL;
      lv_attribute20         := NULL;
      lv_attribute21         := NULL;
      lv_attribute22         := NULL;
      lv_attribute23         := NULL;
      lv_attribute24         := NULL;
      lv_attribute25         := NULL;
      lv_attribute26         := NULL;
      lv_attribute27         := NULL;
      lv_attribute28         := NULL;
      lv_attribute29         := NULL;
      lv_attribute30         := NULL;
      lv_adn_cat_code        := NULL;
      lv_adn_cont            := NULL;
      lv_adn_gl_attribute1   := NULL;
      lv_adn_gl_attribute2   := NULL;
      lv_adn_gl_attribute3   := NULL;
      lv_adn_gl_attribute4   := NULL;
      lv_adn_gl_attribute5   := NULL;
      lv_adn_gl_attribute6   := NULL;
      lv_adn_gl_attribute7   := NULL;
      lv_adn_gl_attribute8   := NULL;
      lv_adn_gl_attribute9   := NULL;
      lv_adn_gl_attribute10  := NULL;
      lv_adn_gl_attribute11  := NULL;
      lv_adn_gl_attribute12  := NULL;
      lv_adn_gl_attribute13  := NULL;
      lv_adn_gl_attribute14  := NULL;
      lv_adn_gl_attribute15  := NULL;
      lv_adn_gl_attribute16  := NULL;
      lv_adn_gl_attribute17  := NULL;
      lv_adn_gl_attribute18  := NULL;
      lv_adn_gl_attribute19  := NULL;
      lv_adn_gl_attribute20  := NULL;
      lv_adn_gl_cat          := NULL;
      lv_bk_gl_attribute1    := NULL;
      lv_bk_gl_attribute2    := NULL;
      lv_bk_gl_attribute3    := NULL;
      lv_bk_gl_attribute4    := NULL;
      lv_bk_gl_attribute5    := NULL;
      lv_bk_gl_attribute6    := NULL;
      lv_bk_gl_attribute7    := NULL;
      lv_bk_gl_attribute8    := NULL;
      lv_bk_gl_attribute9    := NULL;
      lv_bk_gl_attribute10   := NULL;
      lv_bk_gl_attribute11   := NULL;
      lv_bk_gl_attribute12   := NULL;
      lv_bk_gl_attribute13   := NULL;
      lv_bk_gl_attribute14   := NULL;
      lv_bk_gl_attribute15   := NULL;
      lv_bk_gl_attribute16   := NULL;
      lv_bk_gl_attribute17   := NULL;
      lv_bk_gl_attribute18   := NULL;
      lv_bk_gl_attribute19   := NULL;
      lv_bk_gl_attribute20   := NULL;
      lv_bk_gl_att_cat       := NULL;
      xxetn_debug_pkg.add_debug(piv_debug_msg => 'Validation process for parent starts for asset number : ' ||
                                                 rec_cur_val_parent.leg_asset_number ||
                                                 ' and book type : ' ||
                                                 rec_cur_val_parent.leg_book_type_code);
      -- Check for duplicy of asset number
      duplicate_check(rec_cur_val_parent.leg_asset_number); --1.1
      --To check if asset number already exists in the system
      lc_ass_flag := validate_asset_exist(rec_cur_val_parent.leg_asset_number);

      IF lc_ass_flag = 'Y' THEN
        lc_error_flag := 'Y';
      END IF;

      -- legacy current units must not be NULL
      IF rec_cur_val_parent.leg_current_units IS NULL THEN
        log_errors(pin_interface_txn_id    => NULL,
                   piv_source_table        => 'XXFA_CORP_ASSET_STG',
                   piv_source_column_name  => 'leg_current_units',
                   piv_source_column_value => rec_cur_val_parent.leg_current_units,
                   piv_source_keyname1     => 'leg_asset_number',
                   piv_source_keyvalue1    => rec_cur_val_parent.leg_asset_number,
                   piv_error_type          => 'VAL_ERR',
                   piv_error_code          => 'ETN_FA_MANDATORY_COLUMN',
                   piv_error_message       => 'Error : leg_current_units cannot be NULL');
        lc_error_flag := 'Y';
      ELSE
        IF rec_cur_val_parent.leg_current_units < 0 THEN
          log_errors(pin_interface_txn_id    => NULL,
                     piv_source_table        => 'XXFA_CORP_ASSET_STG',
                     piv_source_column_name  => 'leg_current_units',
                     piv_source_column_value => rec_cur_val_parent.leg_current_units,
                     piv_source_keyname1     => 'leg_asset_number',
                     piv_source_keyvalue1    => rec_cur_val_parent.leg_asset_number,
                     piv_error_type          => 'VAL_ERR',
                     piv_error_code          => 'ETN_FA_INVALID_CURRENT_UNIT',
                     piv_error_message       => 'Error : leg_current_units cannot be less than 0');
          lc_error_flag := 'Y';
        END IF;
      END IF;

      -- Asset type must not be NULL
      IF rec_cur_val_parent.leg_asset_type IS NULL THEN
        log_errors(pin_interface_txn_id    => NULL,
                   piv_source_table        => 'XXFA_CORP_ASSET_STG',
                   piv_source_column_name  => 'leg_asset_type',
                   piv_source_column_value => rec_cur_val_parent.leg_asset_type,
                   piv_source_keyname1     => 'leg_asset_number',
                   piv_source_keyvalue1    => rec_cur_val_parent.leg_asset_number,
                   piv_error_type          => 'VAL_ERR',
                   piv_error_code          => 'ETN_FA_MANDATORY_COLUMN',
                   piv_error_message       => 'Error : leg_asset_type cannot be NULL');
        lc_error_flag := 'Y';
      ELSE
        IF rec_cur_val_parent.leg_asset_type <> g_asset_type THEN
          log_errors(pin_interface_txn_id    => NULL,
                     piv_source_table        => 'XXFA_CORP_ASSET_STG',
                     piv_source_column_name  => 'leg_asset_type',
                     piv_source_column_value => rec_cur_val_parent.leg_asset_type,
                     piv_source_keyname1     => 'leg_asset_number',
                     piv_source_keyvalue1    => rec_cur_val_parent.leg_asset_number,
                     piv_error_type          => 'VAL_ERR',
                     piv_error_code          => 'ETN_FA_INVALID_ASSET_TYPE',
                     piv_error_message       => 'Error : leg_asset_type is not CAPITALIZED');
          lc_error_flag := 'Y';
        END IF;
      END IF;

      -- To check duplicacy of tag number starts --1.1
      --duplicate_tag_number_check (rec_cur_val_parent.leg_tag_number);

      -- To check property type code starts  --1.1
      /* IF rec_cur_val_parent.leg_property_type_code IS NOT NULL
      THEN
         ln_property_code :=
            get_lookup_value ('PROPERTY TYPE',
                              'XXFA_CORP_ASSET_STG',
                              'leg_property_type_code',
                              rec_cur_val_parent.leg_property_type_code,
                              'leg_asset_number',
                              rec_cur_val_parent.leg_asset_number,
                              NULL
                             );

         IF ln_property_code = 0
         THEN
            lc_error_flag := 'Y';
         END IF;
      END IF; */

      ---------------- added as per v1.5---------------------------------------
      set_cat_lookup(p_leg_source_asset_number => rec_cur_val_parent.leg_source_asset_number,
                     p_leg_book_type_code      => rec_cur_val_parent.leg_book_type_code); --added by harjinder sing for the PMC#349366
      ---------------- added as per v1.5---------------------------------------

      IF g_cat_lookup = 'ETN_FA_CATEGORY_MAP_POLAND' THEN
        validate_asset_category_poland(rec_cur_val_parent.leg_source_system,
                                       rec_cur_val_parent.leg_asset_number,
                                       rec_cur_val_parent.leg_book_type_code,
                                       rec_cur_val_parent.leg_asset_cat_segment1,
                                       ln_tax_category,
                                       ln_category_id,
                                       lc_cat_segment1,
                                       lc_cat_segment2,
                                       lc_cat_flag,
                                       lc_cat_msg);

        IF lc_cat_flag = 'Y' THEN
          log_errors(pin_interface_txn_id    => NULL,
                     piv_source_table        => 'XXFA_CORP_ASSET_STG',
                     piv_source_column_name  => 'leg_asset_cat_segment1.leg_asset_cat_segment2',
                     piv_source_column_value => rec_cur_val_parent.leg_asset_cat_segment1 ||
                                                g_sep_p || ln_tax_category,
                     piv_source_keyname1     => 'leg_asset_number',
                     piv_source_keyvalue1    => rec_cur_val_parent.leg_asset_number,
                     piv_error_type          => 'VAL_ERR',
                     piv_error_code          => 'ETN_FA_INVALID_CATEGORY',
                     piv_error_message       => lc_cat_msg);
          lc_error_flag := 'Y';
        END IF;

      ELSE
        -- check category segment starts
        validate_asset_category(rec_cur_val_parent.leg_source_system,
                                rec_cur_val_parent.leg_asset_cat_segment1,
                                rec_cur_val_parent.leg_asset_cat_segment2,
                                ln_category_id,
                                lc_cat_segment1,
                                lc_cat_segment2,
                                lc_cat_flag,
                                lc_cat_msg);

        IF lc_cat_flag = 'Y' THEN
          log_errors(pin_interface_txn_id    => NULL,
                     piv_source_table        => 'XXFA_CORP_ASSET_STG',
                     piv_source_column_name  => 'leg_asset_cat_segment1.leg_asset_cat_segment2',
                     piv_source_column_value => rec_cur_val_parent.leg_asset_cat_segment1 ||
                                                g_sep ||
                                                rec_cur_val_parent.leg_asset_cat_segment2,
                     piv_source_keyname1     => 'leg_asset_number',
                     piv_source_keyvalue1    => rec_cur_val_parent.leg_asset_number,
                     piv_error_type          => 'VAL_ERR',
                     piv_error_code          => 'ETN_FA_INVALID_CATEGORY',
                     piv_error_message       => lc_cat_msg);
          lc_error_flag := 'Y';
        END IF;
      END IF;

      -- To check property 1245/1250 type code starts  --1.1
      /* IF rec_cur_val_parent.leg_property_1245_1250_code IS NOT NULL
      THEN
         ln_prop_1245_1250_code :=
            get_lookup_value
                          ('1245/1250 PROPERTY',
                           'XXFA_CORP_ASSET_STG',
                           'leg_property_1245_1250_code',
                           rec_cur_val_parent.leg_property_1245_1250_code,
                           'leg_asset_number',
                           rec_cur_val_parent.leg_asset_number,
                           NULL
                          );

         IF ln_prop_1245_1250_code = 0
         THEN
            lc_error_flag := 'Y';
         END IF;
      END IF; */

      -- To check owned/leased code starts  --1.1
      /* IF rec_cur_val_parent.leg_owned_leased IS NOT NULL
      THEN
         ln_own_lease :=
            get_lookup_value ('OWNLEASE',
                              'XXFA_CORP_ASSET_STG',
                              'leg_owned_leased',
                              rec_cur_val_parent.leg_owned_leased,
                              'leg_asset_number',
                              rec_cur_val_parent.leg_asset_number,
                              NULL
                             );

         IF ln_own_lease = 0
         THEN
            lc_error_flag := 'Y';
         END IF;
      END IF; */

      -- To check NEWUSE code starts  --1.1
      /* IF rec_cur_val_parent.leg_new_used IS NOT NULL
      THEN
         ln_new_use :=
            get_lookup_value ('NEWUSE',
                              'XXFA_CORP_ASSET_STG',
                              'leg_new_used',
                              rec_cur_val_parent.leg_new_used,
                              'leg_asset_number',
                              rec_cur_val_parent.leg_asset_number,
                              NULL
                             );

         IF ln_new_use = 0
         THEN
            lc_error_flag := 'Y';
         END IF;
      END IF; */

      --- Check for book type code starts   --1.1
      /* BEGIN
         SELECT COUNT (1)
           INTO ln_book_code
           FROM fa_book_controls
          WHERE book_type_code = rec_cur_val_parent.leg_book_type_code;
      EXCEPTION
         WHEN OTHERS
         THEN
            log_errors
               (pin_interface_txn_id         => NULL,
                piv_source_table             => 'XXFA_CORP_ASSET_STG',
                piv_source_column_name       => NULL,
                piv_source_column_value      => NULL,
                piv_source_keyname1          => 'leg_asset_number',
                piv_source_keyvalue1         => rec_cur_val_parent.leg_asset_number,
                piv_error_type               => 'VAL_ERR',
                piv_error_code               => 'ETN_FA_INVALID_BOOK_TYPE',
                piv_error_message            =>    'Error : Exception occurred while fetching book type code from FA_BOOK_CONTROLS : '
                                                || SUBSTR (SQLERRM, 1, 240)
               );
            lc_error_flag := 'Y';
      END;

      IF ln_book_code = 0
      THEN
         log_errors
            (pin_interface_txn_id         => NULL,
             piv_source_table             => 'XXFA_CORP_ASSET_STG',
             piv_source_column_name       => 'leg_book_type_code',
             piv_source_column_value      => rec_cur_val_parent.leg_book_type_code,
             piv_source_keyname1          => 'leg_asset_number',
             piv_source_keyvalue1         => rec_cur_val_parent.leg_asset_number,
             piv_error_type               => 'VAL_ERR',
             piv_error_code               => 'ETN_FA_INVALID_BOOK_TYPE',
             piv_error_message            => 'Error : book type code not valid : '
            );
         lc_error_flag := 'Y';
      END IF; */

      --- To check date placed in service
      IF rec_cur_val_parent.leg_date_placed_in_service IS NULL THEN
        log_errors(pin_interface_txn_id    => NULL,
                   piv_source_table        => 'XXFA_CORP_ASSET_STG',
                   piv_source_column_name  => 'leg_date_placed_in_service',
                   piv_source_column_value => rec_cur_val_parent.leg_date_placed_in_service,
                   piv_source_keyname1     => 'leg_asset_number',
                   piv_source_keyvalue1    => rec_cur_val_parent.leg_asset_number,
                   piv_error_type          => 'VAL_ERR',
                   piv_error_code          => 'ETN_FA_MANDATORY_COLUMN',
                   piv_error_message       => 'Error : Date placed in service cannot be NULL  : ');
        lc_error_flag := 'Y';
      END IF;

      -- check for depreciation method code starts  --1.1
      /* IF rec_cur_val_parent.leg_deprn_method_code IS NULL
      THEN
         log_errors
            (pin_interface_txn_id         => NULL,
             piv_source_table             => 'XXFA_CORP_ASSET_STG',
             piv_source_column_name       => 'leg_deprn_method_code',
             piv_source_column_value      => rec_cur_val_parent.leg_deprn_method_code,
             piv_source_keyname1          => 'leg_asset_number',
             piv_source_keyvalue1         => rec_cur_val_parent.leg_asset_number,
             piv_error_type               => 'VAL_ERR',
             piv_error_code               => 'ETN_FA_MANDATORY_COLUMN',
             piv_error_message            => 'Error : leg_deprn_method_code cannot be NULL'
            );
         lc_error_flag := 'Y';
      ELSE
         BEGIN
            SELECT COUNT (1)
              INTO ln_life_in_months
              FROM fa_methods
             WHERE method_code = rec_cur_val_parent.leg_deprn_method_code
               AND life_in_months = rec_cur_val_parent.leg_life_in_months;
         EXCEPTION
            WHEN OTHERS
            THEN
               log_errors
                  (pin_interface_txn_id         => NULL,
                   piv_source_table             => 'XXFA_CORP_ASSET_STG',
                   piv_source_column_name       => NULL,
                   piv_source_column_value      => NULL,
                   piv_source_keyname1          => 'leg_asset_number',
                   piv_source_keyvalue1         => rec_cur_val_parent.leg_asset_number,
                   piv_error_type               => 'VAL_ERR',
                   piv_error_code               => 'ETN_FA_INVALID_DEPRN_METHOD',
                   piv_error_message            =>    'Error : Exception occured while fetching depreciation method code : '
                                                   || SUBSTR (SQLERRM,
                                                              1,
                                                              240
                                                             )
                  );
               lc_error_flag := 'Y';
         END;

         IF ln_life_in_months = 0
         THEN
            log_errors
               (pin_interface_txn_id         => NULL,
                piv_source_table             => 'XXFA_CORP_ASSET_STG',
                piv_source_column_name       => 'leg_deprn_method_code',
                piv_source_column_value      => rec_cur_val_parent.leg_deprn_method_code,
                piv_source_keyname1          => 'leg_asset_number',
                piv_source_keyvalue1         => rec_cur_val_parent.leg_asset_number,
                piv_error_type               => 'VAL_ERR',
                piv_error_code               => 'ETN_FA_INVALID_DEPRN_METHOD',
                piv_error_message            => 'Error : Depreciation method code not valid : '
               );
            lc_error_flag := 'Y';
         END IF;
      END IF; */

      -- check for legacy cost
      IF rec_cur_val_parent.leg_cost IS NULL THEN
        log_errors(pin_interface_txn_id    => NULL,
                   piv_source_table        => 'XXFA_CORP_ASSET_STG',
                   piv_source_column_name  => 'leg_cost',
                   piv_source_column_value => rec_cur_val_parent.leg_cost,
                   piv_source_keyname1     => 'leg_asset_number',
                   piv_source_keyvalue1    => rec_cur_val_parent.leg_asset_number,
                   piv_error_type          => 'VAL_ERR',
                   piv_error_code          => 'ETN_FA_MANDATORY_COLUMN',
                   piv_error_message       => 'Error : leg_cost cannot be NULL ');
        lc_error_flag := 'Y';
      END IF;

      -- check for legacy original cost
      IF rec_cur_val_parent.leg_original_cost IS NULL THEN
        log_errors(pin_interface_txn_id    => NULL,
                   piv_source_table        => 'XXFA_CORP_ASSET_STG',
                   piv_source_column_name  => 'leg_original_cost',
                   piv_source_column_value => rec_cur_val_parent.leg_original_cost,
                   piv_source_keyname1     => 'leg_asset_number',
                   piv_source_keyvalue1    => rec_cur_val_parent.leg_asset_number,
                   piv_error_type          => 'VAL_ERR',
                   piv_error_code          => 'ETN_FA_MANDATORY_COLUMN',
                   piv_error_message       => 'Error : leg_original_cost cannot be NULL');
        lc_error_flag := 'Y';
      END IF;

      -- check for legacy salvage value
      IF rec_cur_val_parent.leg_salvage_value IS NULL THEN
        log_errors(pin_interface_txn_id    => NULL,
                   piv_source_table        => 'XXFA_CORP_ASSET_STG',
                   piv_source_column_name  => 'leg_salvage_value',
                   piv_source_column_value => rec_cur_val_parent.leg_salvage_value,
                   piv_source_keyname1     => 'leg_asset_number',
                   piv_source_keyvalue1    => rec_cur_val_parent.leg_asset_number,
                   piv_error_type          => 'VAL_ERR',
                   piv_error_code          => 'ETN_FA_MANDATORY_COLUMN',
                   piv_error_message       => 'Error : leg_salvage_value cannot be NULL');
        lc_error_flag := 'Y';
      END IF;

      -- check for prorate convention code  --1.1
      /* IF rec_cur_val_parent.leg_prorate_convention_code IS NULL
      THEN
         log_errors
            (pin_interface_txn_id         => NULL,
             piv_source_table             => 'XXFA_CORP_ASSET_STG',
             piv_source_column_name       => 'leg_prorate_convention_code',
             piv_source_column_value      => rec_cur_val_parent.leg_prorate_convention_code,
             piv_source_keyname1          => 'leg_asset_number',
             piv_source_keyvalue1         => rec_cur_val_parent.leg_asset_number,
             piv_error_type               => 'VAL_ERR',
             piv_error_code               => 'ETN_FA_MANDATORY_COLUMN',
             piv_error_message            => 'Error : leg_prorate_convention_code cannot be NULL'
            );
         lc_error_flag := 'Y';
      ELSE
         BEGIN
            SELECT COUNT (1)
              INTO ln_prorate_count
              FROM fa_convention_types
             WHERE prorate_convention_code =
                             rec_cur_val_parent.leg_prorate_convention_code;
         EXCEPTION
            WHEN OTHERS
            THEN
               log_errors
                  (pin_interface_txn_id         => NULL,
                   piv_source_table             => 'XXFA_CORP_ASSET_STG',
                   piv_source_column_name       => NULL,
                   piv_source_column_value      => NULL,
                   piv_source_keyname1          => 'leg_asset_number',
                   piv_source_keyvalue1         => rec_cur_val_parent.leg_asset_number,
                   piv_error_type               => 'VAL_ERR',
                   piv_error_code               => 'ETN_FA_INVALID_PRORATE_CODE',
                   piv_error_message            =>    'Error : Exception occured while fetching prorate_convention_code : '
                                                   || SUBSTR (SQLERRM,
                                                              1,
                                                              240
                                                             )
                  );
               lc_error_flag := 'Y';
         END;

         IF ln_prorate_count = 0
         THEN
            log_errors
               (pin_interface_txn_id         => NULL,
                piv_source_table             => 'XXFA_CORP_ASSET_STG',
                piv_source_column_name       => 'leg_prorate_convention_code',
                piv_source_column_value      => rec_cur_val_parent.leg_prorate_convention_code,
                piv_source_keyname1          => 'leg_asset_number',
                piv_source_keyvalue1         => rec_cur_val_parent.leg_asset_number,
                piv_error_type               => 'VAL_ERR',
                piv_error_code               => 'ETN_FA_INVALID_PRORATE_CODE',
                piv_error_message            => 'Error : Prorate Convention code not valid  : '
               );
            lc_error_flag := 'Y';
         END IF;
      END IF; */

      -- check for capitalize flag
      IF rec_cur_val_parent.leg_capitalize_flag IS NULL THEN
        log_errors(pin_interface_txn_id    => NULL,
                   piv_source_table        => 'XXFA_CORP_ASSET_STG',
                   piv_source_column_name  => 'leg_capitalize_flag',
                   piv_source_column_value => rec_cur_val_parent.leg_capitalize_flag,
                   piv_source_keyname1     => 'leg_asset_number',
                   piv_source_keyvalue1    => rec_cur_val_parent.leg_asset_number,
                   piv_error_type          => 'VAL_ERR',
                   piv_error_code          => 'ETN_FA_MANDATORY_COLUMN',
                   piv_error_message       => 'Error : leg_capitalize_flag cannot be NULL');
        lc_error_flag := 'Y';
      END IF;

      -- check for depreciate flag
      IF rec_cur_val_parent.leg_depreciate_flag IS NULL THEN
        log_errors(pin_interface_txn_id    => NULL,
                   piv_source_table        => 'XXFA_CORP_ASSET_STG',
                   piv_source_column_name  => 'leg_depreciate_flag',
                   piv_source_column_value => rec_cur_val_parent.leg_depreciate_flag,
                   piv_source_keyname1     => 'leg_asset_number',
                   piv_source_keyvalue1    => rec_cur_val_parent.leg_asset_number,
                   piv_error_type          => 'VAL_ERR',
                   piv_error_code          => 'ETN_FA_MANDATORY_COLUMN',
                   piv_error_message       => 'Error : leg_depreciate_flag cannot be NULL');
        lc_error_flag := 'Y';
      END IF;

      -- check for bonus rule
      IF rec_cur_val_parent.leg_bonus_rule IS NOT NULL THEN
        BEGIN
          SELECT COUNT(1)
            INTO ln_bonus_count
            FROM fa_bonus_rules
           WHERE bonus_rule = rec_cur_val_parent.leg_bonus_rule;
        EXCEPTION
          WHEN OTHERS THEN
            log_errors(pin_interface_txn_id    => NULL,
                       piv_source_table        => 'XXFA_CORP_ASSET_STG',
                       piv_source_column_name  => NULL,
                       piv_source_column_value => NULL,
                       piv_source_keyname1     => 'leg_asset_number',
                       piv_source_keyvalue1    => rec_cur_val_parent.leg_asset_number,
                       piv_error_type          => 'VAL_ERR',
                       piv_error_code          => 'ETN_FA_INVALID_BONUS_RULE',
                       piv_error_message       => 'Error : Exception occurred while fetching bonus_rule : ' ||
                                                  SUBSTR(SQLERRM, 1, 240));
            lc_error_flag := 'Y';
        END;

        IF ln_bonus_count = 0 THEN
          log_errors(pin_interface_txn_id    => NULL,
                     piv_source_table        => 'XXFA_CORP_ASSET_STG',
                     piv_source_column_name  => 'leg_bonus_rule',
                     piv_source_column_value => rec_cur_val_parent.leg_bonus_rule,
                     piv_source_keyname1     => 'leg_asset_number',
                     piv_source_keyvalue1    => rec_cur_val_parent.leg_asset_number,
                     piv_error_type          => 'VAL_ERR',
                     piv_error_code          => 'ETN_FA_INVALID_BONUS_RULE',
                     piv_error_message       => 'Error : bonus_rule not valid :');
          lc_error_flag := 'Y';
        END IF;
      END IF;

      -- check for ceiling_name  --1.1
      /* IF rec_cur_val_parent.leg_ceiling_name IS NOT NULL
      THEN
         BEGIN
            SELECT COUNT (1)
              INTO ln_ceiling_count
              FROM fa_ceilings
             WHERE ceiling_name = rec_cur_val_parent.leg_ceiling_name;
         EXCEPTION
            WHEN OTHERS
            THEN
               log_errors
                  (pin_interface_txn_id         => NULL,
                   piv_source_table             => 'XXFA_CORP_ASSET_STG',
                   piv_source_column_name       => NULL,
                   piv_source_column_value      => NULL,
                   piv_source_keyname1          => 'leg_asset_number',
                   piv_source_keyvalue1         => rec_cur_val_parent.leg_asset_number,
                   piv_error_type               => 'VAL_ERR',
                   piv_error_code               => 'ETN_FA_INVALID_CEILING_NAME',
                   piv_error_message            =>    'Error : Exception occurred while fetching ceiling_name : '
                                                   || SUBSTR (SQLERRM,
                                                              1,
                                                              240
                                                             )
                  );
               lc_error_flag := 'Y';
         END;

         IF ln_ceiling_count = 0
         THEN
            log_errors
               (pin_interface_txn_id         => NULL,
                piv_source_table             => 'XXFA_CORP_ASSET_STG',
                piv_source_column_name       => 'leg_ceiling_name',
                piv_source_column_value      => rec_cur_val_parent.leg_ceiling_name,
                piv_source_keyname1          => 'leg_asset_number',
                piv_source_keyvalue1         => rec_cur_val_parent.leg_asset_number,
                piv_error_type               => 'VAL_ERR',
                piv_error_code               => 'ETN_FA_INVALID_CEILING_NAME',
                piv_error_message            => 'Error : ceiling_name code not valid  : '
               );
            lc_error_flag := 'Y';
         END IF;
      END IF; */

      -- check for YTD deprcn
      IF rec_cur_val_parent.leg_ytd_deprn IS NULL THEN
        log_errors(pin_interface_txn_id    => NULL,
                   piv_source_table        => 'XXFA_CORP_ASSET_STG',
                   piv_source_column_name  => 'leg_ytd_deprn',
                   piv_source_column_value => rec_cur_val_parent.leg_ytd_deprn,
                   piv_source_keyname1     => 'leg_asset_number',
                   piv_source_keyvalue1    => rec_cur_val_parent.leg_asset_number,
                   piv_error_type          => 'VAL_ERR',
                   piv_error_code          => 'ETN_FA_MANDATORY_COLUMN',
                   piv_error_message       => 'Error : leg_ytd_deprn cannot be NULL');
        lc_error_flag := 'Y';
      ELSE
        IF (rec_cur_val_parent.leg_cost < 0) AND
           (rec_cur_val_parent.leg_recoverable_cost < 0) AND
           (rec_cur_val_parent.leg_ytd_deprn <= 0) AND
           (rec_cur_val_parent.leg_deprn_reserve <= 0) THEN
          IF ABS(rec_cur_val_parent.leg_ytd_deprn) >
             ABS(rec_cur_val_parent.leg_deprn_reserve) THEN
            log_errors(pin_interface_txn_id    => NULL,
                       piv_source_table        => 'XXFA_CORP_ASSET_STG',
                       piv_source_column_name  => 'leg_ytd_deprn',
                       piv_source_column_value => rec_cur_val_parent.leg_ytd_deprn,
                       piv_source_keyname1     => 'leg_asset_number',
                       piv_source_keyvalue1    => rec_cur_val_parent.leg_asset_number,
                       piv_error_type          => 'VAL_ERR',
                       piv_error_code          => 'ETN_FA_INVALID_YTD_DERPN',
                       piv_error_message       => 'Error : leg_ytd_deprn cannot be greater than leg_deprn_reserve');
            lc_error_flag := 'Y';
          END IF;
        ELSE
          IF rec_cur_val_parent.leg_ytd_deprn >
             rec_cur_val_parent.leg_deprn_reserve THEN
            log_errors(pin_interface_txn_id    => NULL,
                       piv_source_table        => 'XXFA_CORP_ASSET_STG',
                       piv_source_column_name  => 'leg_ytd_deprn',
                       piv_source_column_value => rec_cur_val_parent.leg_ytd_deprn,
                       piv_source_keyname1     => 'leg_asset_number',
                       piv_source_keyvalue1    => rec_cur_val_parent.leg_asset_number,
                       piv_error_type          => 'VAL_ERR',
                       piv_error_code          => 'ETN_FA_INVALID_YTD_DERPN',
                       piv_error_message       => 'Error : leg_ytd_deprn cannot be greater than leg_deprn_reserve');
            lc_error_flag := 'Y';
          END IF;
        END IF;
      END IF;

      -- check for deprn reserve
      IF rec_cur_val_parent.leg_deprn_reserve IS NULL THEN
        log_errors(pin_interface_txn_id    => NULL,
                   piv_source_table        => 'XXFA_CORP_ASSET_STG',
                   piv_source_column_name  => 'leg_deprn_reserve',
                   piv_source_column_value => rec_cur_val_parent.leg_deprn_reserve,
                   piv_source_keyname1     => 'leg_asset_number',
                   piv_source_keyvalue1    => rec_cur_val_parent.leg_asset_number,
                   piv_error_type          => 'VAL_ERR',
                   piv_error_code          => 'ETN_FA_MANDATORY_COLUMN',
                   piv_error_message       => 'Error : leg_deprn_reserve cannot be NULL');
        lc_error_flag := 'Y';
      END IF;

      lc_master_dist_flag := 'N';

      --- Check Distribution Details
      FOR rec_get_distribution_details IN get_distribution_details(rec_cur_val_parent.leg_asset_number,
                                                                   -- rec_cur_val_parent.leg_book_type_code
                                                                   rec_cur_val_parent.book_type_code) LOOP
        ln_location_id     := NULL;
        ln_employee_id     := NULL;
        x_out_acc_rec      := NULL;
        x_ccid             := NULL;
        lc_dist_error_flag := 'N';

        IF lc_master_dist_flag = 'Y' THEN
          log_errors(pin_interface_txn_id    => NULL,
                     piv_source_table        => 'XXFA_CORP_ASSET_STG',
                     piv_source_column_name  => NULL,
                     piv_source_column_value => NULL,
                     piv_source_keyname1     => 'interface_txn_id',
                     piv_source_keyvalue1    => rec_get_distribution_details.interface_txn_id,
                     piv_error_type          => 'VAL_ERR',
                     piv_error_code          => 'ETN_FA_DEPDNT_DIST_ERROR',
                     piv_error_message       => 'Error : Record erred out due to dependent distribution erring out :');
        END IF;

        --- deriving location --- 1.1
        /* BEGIN
           SELECT location_id
             INTO ln_location_id
             FROM fa_locations
            WHERE enabled_flag = 'Y'
              AND segment1 =
                            rec_get_distribution_details.leg_locn_segment1
              AND segment2 =
                            rec_get_distribution_details.leg_locn_segment2
              AND segment3 =
                            rec_get_distribution_details.leg_locn_segment3
              AND segment4 =
                            rec_get_distribution_details.leg_locn_segment4;
        EXCEPTION
           WHEN NO_DATA_FOUND
           THEN
              log_errors
                 (pin_interface_txn_id         => NULL,
                  piv_source_table             => 'XXFA_CORP_ASSET_STG',
                  piv_source_column_name       => 'leg_locn_segments_conc_value',
                  piv_source_column_value      =>    rec_get_distribution_details.leg_locn_segment1
                                                  || '.'
                                                  || rec_get_distribution_details.leg_locn_segment2
                                                  || '.'
                                                  || rec_get_distribution_details.leg_locn_segment3
                                                  || '.'
                                                  || rec_get_distribution_details.leg_locn_segment4,
                  piv_source_keyname1          => 'interface_txn_id',
                  piv_source_keyvalue1         => rec_get_distribution_details.interface_txn_id,
                  piv_error_type               => 'VAL_ERR',
                  piv_error_code               => 'ETN_FA_INVALID_LOCATION',
                  piv_error_message            => 'Error : Given location segments codes are not valid'
                 );
              lc_dist_error_flag := 'Y';
           WHEN OTHERS
           THEN
              log_errors
                 (pin_interface_txn_id         => NULL,
                  piv_source_table             => 'XXFA_CORP_ASSET_STG',
                  piv_source_column_name       => NULL,
                  piv_source_column_value      => NULL,
                  piv_source_keyname1          => 'interface_txn_id',
                  piv_source_keyvalue1         => rec_get_distribution_details.interface_txn_id,
                  piv_error_type               => 'VAL_ERR',
                  piv_error_code               => 'ETN_FA_INVALID_LOCATION',
                  piv_error_message            =>    'Error : Exception occurred while fetching location : '
                                                  || SUBSTR (SQLERRM,
                                                             1,
                                                             240
                                                            )
                 );
              lc_dist_error_flag := 'Y';
        END; */
        IF rec_get_distribution_details.leg_assigned_emp_number IS NOT NULL THEN
          BEGIN
            SELECT employee_id
              INTO ln_employee_id
              FROM fa_employees
             WHERE employee_number =
                   rec_get_distribution_details.leg_assigned_emp_number;
          EXCEPTION
            WHEN NO_DATA_FOUND THEN
              log_errors(pin_interface_txn_id    => NULL,
                         piv_source_table        => 'XXFA_CORP_ASSET_STG',
                         piv_source_column_name  => 'leg_assigned_emp_number',
                         piv_source_column_value => rec_get_distribution_details.leg_assigned_emp_number,
                         piv_source_keyname1     => 'interface_txn_id',
                         piv_source_keyvalue1    => rec_get_distribution_details.interface_txn_id,
                         piv_error_type          => 'VAL_ERR',
                         piv_error_code          => 'ETN_FA_INVALID_EMPLOYEE',
                         piv_error_message       => 'Error : Given employee number not valid');
              lc_dist_error_flag := 'Y';
            WHEN OTHERS THEN
              log_errors(pin_interface_txn_id    => NULL,
                         piv_source_table        => 'XXFA_CORP_ASSET_STG',
                         piv_source_column_name  => NULL,
                         piv_source_column_value => NULL,
                         piv_source_keyname1     => 'interface_txn_id',
                         piv_source_keyvalue1    => rec_get_distribution_details.interface_txn_id,
                         piv_error_type          => 'VAL_ERR',
                         piv_error_code          => 'ETN_FA_INVALID_EMPLOYEE',
                         piv_error_message       => 'Error : Exception occured while fetching employee  : ' ||
                                                    SUBSTR(SQLERRM, 1, 240));
              lc_dist_error_flag := 'Y';
          END;
        END IF;

        IF rec_get_distribution_details.leg_units_assigned IS NULL THEN
          log_errors(pin_interface_txn_id    => NULL,
                     piv_source_table        => 'XXFA_CORP_ASSET_STG',
                     piv_source_column_name  => 'leg_units_assigned',
                     piv_source_column_value => rec_get_distribution_details.leg_units_assigned,
                     piv_source_keyname1     => 'interface_txn_id',
                     piv_source_keyvalue1    => rec_get_distribution_details.interface_txn_id,
                     piv_error_type          => 'VAL_ERR',
                     piv_error_code          => 'ETN_FA_MANDATORY_COLUMN',
                     piv_error_message       => 'Error : LEG_UNITS_ASSIGNED cannot be NULL');
          lc_dist_error_flag := 'Y';
        END IF;

        ---validate_accounts -- 1.1
        /* validate_accounts (rec_get_distribution_details.interface_txn_id,
                           rec_get_distribution_details.leg_cc_segment1,
                           rec_get_distribution_details.leg_cc_segment2,
                           rec_get_distribution_details.leg_cc_segment3,
                           rec_get_distribution_details.leg_cc_segment4,
                           rec_get_distribution_details.leg_cc_segment5,
                           rec_get_distribution_details.leg_cc_segment6,
                           rec_get_distribution_details.leg_cc_segment7,
                           x_out_acc_rec,
                           x_ccid
                          );

        IF x_ccid IS NULL
        THEN
           lc_dist_error_flag := 'Y';
        END IF; */
        IF lc_dist_error_flag = 'Y' OR
           rec_get_distribution_details.process_flag = 'E' THEN
          lc_master_dist_flag := 'Y';
        END IF;

        BEGIN
          UPDATE xxfa_corp_asset_stg
             SET assigned_emp_id = ln_employee_id
          --location_id = ln_location_id,
          --cc_segment1 = x_out_acc_rec.segment1,
          --cc_segment2 = x_out_acc_rec.segment2,
          -- cc_segment3 = x_out_acc_rec.segment3,
          --cc_segment4 = x_out_acc_rec.segment4,
          -- cc_segment5 = x_out_acc_rec.segment5,
          -- cc_segment6 = x_out_acc_rec.segment6,
          -- cc_segment7 = x_out_acc_rec.segment7,
          -- cc_segment8 = x_out_acc_rec.segment8,
          -- cc_segment9 = x_out_acc_rec.segment9,
          -- cc_segment10 = x_out_acc_rec.segment10,
          -- acct_combination_id = x_ccid
           WHERE interface_txn_id =
                 rec_get_distribution_details.interface_txn_id;
        EXCEPTION
          WHEN OTHERS THEN
            log_errors(pin_interface_txn_id    => NULL,
                       piv_source_table        => 'XXFA_CORP_ASSET_STG',
                       piv_source_column_name  => NULL,
                       piv_source_column_value => NULL,
                       piv_source_keyname1     => 'interface_txn_id',
                       piv_source_keyvalue1    => rec_get_distribution_details.interface_txn_id,
                       piv_error_type          => 'VAL_ERR',
                       piv_error_code          => 'ETN_FA_INVALID_PROC',
                       piv_error_message       => 'Error : Exception occurred while updating distribution details  : ' ||
                                                  SUBSTR(SQLERRM, 1, 240));
            lc_dist_error_flag := 'Y';
        END;
      END LOOP;

      IF lc_master_dist_flag = 'Y' THEN
        lc_error_flag := 'Y';
      END IF;

      --assigning DFF segments
      IF (rec_cur_val_parent.leg_adtn_context = 'France' OR
         rec_cur_val_parent.leg_adtn_context = 'Germany' OR
         rec_cur_val_parent.leg_adtn_context = 'Holland' OR
         rec_cur_val_parent.leg_adtn_context = 'Italy' OR
         rec_cur_val_parent.leg_adtn_context = 'Mexico' OR
         rec_cur_val_parent.leg_adtn_context = 'Monaco' OR
         rec_cur_val_parent.leg_adtn_context = 'Spain' OR
         rec_cur_val_parent.leg_adtn_context = 'United Kingdom') AND
         rec_cur_val_parent.leg_source_system = g_source_issc THEN
        lv_attribute1 := rec_cur_val_parent.leg_adtn_attribute1;
      END IF;

      IF rec_cur_val_parent.leg_source_system = g_source_issc THEN
        lv_attribute3  := rec_cur_val_parent.leg_adtn_attribute26;
        lv_attribute4  := rec_cur_val_parent.leg_adtn_attribute27;
        lv_attribute17 := rec_cur_val_parent.leg_adtn_attribute28;
        lv_attribute16 := rec_cur_val_parent.leg_adtn_attribute16;
      ELSIF rec_cur_val_parent.leg_source_system = g_source_fsc THEN
        lv_attribute3  := rec_cur_val_parent.leg_adtn_attribute3;
        lv_attribute4  := rec_cur_val_parent.leg_adtn_attribute4;
        lv_attribute17 := rec_cur_val_parent.leg_adtn_attribute10;
        lv_attribute16 := rec_cur_val_parent.leg_adtn_attribute15;
        lv_attribute7  := rec_cur_val_parent.leg_adtn_attribute7;
        lv_attribute8  := rec_cur_val_parent.leg_adtn_attribute8;
        lv_attribute5  := rec_cur_val_parent.leg_adtn_attribute12;
        lv_attribute29 := rec_cur_val_parent.leg_adtn_attribute29;
        lv_attribute6  := rec_cur_val_parent.leg_adtn_attribute13;
        lv_attribute26 := rec_cur_val_parent.leg_adtn_attribute26;
        lv_attribute27 := rec_cur_val_parent.leg_adtn_attribute27;
        lv_attribute18 := rec_cur_val_parent.leg_adtn_attribute14;
        lv_attribute9  := rec_cur_val_parent.leg_adtn_attribute9;
        lv_attribute22 := rec_cur_val_parent.leg_adtn_attribute22;
        lv_attribute23 := rec_cur_val_parent.leg_adtn_attribute23;
      END IF;

      IF rec_cur_val_parent.leg_source_system = g_source_issc THEN
        IF rec_cur_val_parent.leg_adtn_context = 'Taiwan' THEN
          lv_attribute10 := rec_cur_val_parent.leg_adtn_attribute10;
          lv_attribute11 := rec_cur_val_parent.leg_adtn_attribute11;
          lv_attribute12 := rec_cur_val_parent.leg_adtn_attribute12;
          lv_attribute13 := rec_cur_val_parent.leg_adtn_attribute13;
          lv_attribute14 := rec_cur_val_parent.leg_adtn_attribute14;
          lv_attribute15 := rec_cur_val_parent.leg_adtn_attribute15;
        ELSIF rec_cur_val_parent.leg_adtn_context = 'Poland' THEN
          lv_attribute30 := rec_cur_val_parent.leg_adtn_attribute30;
          lv_attribute2  := rec_cur_val_parent.leg_adtn_attribute3;
        END IF;
      END IF;

      lv_adn_cont           := rec_cur_val_parent.leg_adtn_context;
      lv_adn_cat_code       := rec_cur_val_parent.leg_adtn_attr_category_code;
      lv_adn_gl_attribute1  := rec_cur_val_parent.leg_adtn_global_attribute1;
      lv_adn_gl_attribute2  := rec_cur_val_parent.leg_adtn_global_attribute2;
      lv_adn_gl_attribute3  := rec_cur_val_parent.leg_adtn_global_attribute3;
      lv_adn_gl_attribute4  := rec_cur_val_parent.leg_adtn_global_attribute4;
      lv_adn_gl_attribute5  := rec_cur_val_parent.leg_adtn_global_attribute5;
      lv_adn_gl_attribute6  := rec_cur_val_parent.leg_adtn_global_attribute6;
      lv_adn_gl_attribute7  := rec_cur_val_parent.leg_adtn_global_attribute7;
      lv_adn_gl_attribute8  := rec_cur_val_parent.leg_adtn_global_attribute8;
      lv_adn_gl_attribute9  := rec_cur_val_parent.leg_adtn_global_attribute9;
      lv_adn_gl_attribute10 := rec_cur_val_parent.leg_adtn_global_attribute10;
      lv_adn_gl_attribute11 := rec_cur_val_parent.leg_adtn_global_attribute11;
      lv_adn_gl_attribute12 := rec_cur_val_parent.leg_adtn_global_attribute12;
      lv_adn_gl_attribute13 := rec_cur_val_parent.leg_adtn_global_attribute13;
      lv_adn_gl_attribute14 := rec_cur_val_parent.leg_adtn_global_attribute14;
      lv_adn_gl_attribute15 := rec_cur_val_parent.leg_adtn_global_attribute15;
      lv_adn_gl_attribute16 := rec_cur_val_parent.leg_adtn_global_attribute16;
      lv_adn_gl_attribute17 := rec_cur_val_parent.leg_adtn_global_attribute17;
      lv_adn_gl_attribute18 := rec_cur_val_parent.leg_adtn_global_attribute18;
      lv_adn_gl_attribute19 := rec_cur_val_parent.leg_adtn_global_attribute19;
      lv_adn_gl_attribute20 := rec_cur_val_parent.leg_adtn_global_attribute20;
      lv_adn_gl_cat         := rec_cur_val_parent.leg_adtn_global_attr_category;
      lv_bk_gl_attribute1   := rec_cur_val_parent.leg_books_global_attribute1;
      lv_bk_gl_attribute2   := rec_cur_val_parent.leg_books_global_attribute2;
      lv_bk_gl_attribute3   := rec_cur_val_parent.leg_books_global_attribute3;
      lv_bk_gl_attribute4   := rec_cur_val_parent.leg_books_global_attribute4;
      lv_bk_gl_attribute5   := rec_cur_val_parent.leg_books_global_attribute5;
      lv_bk_gl_attribute6   := rec_cur_val_parent.leg_books_global_attribute6;
      lv_bk_gl_attribute7   := rec_cur_val_parent.leg_books_global_attribute7;
      lv_bk_gl_attribute8   := rec_cur_val_parent.leg_books_global_attribute8;
      lv_bk_gl_attribute9   := rec_cur_val_parent.leg_books_global_attribute9;
      lv_bk_gl_attribute10  := rec_cur_val_parent.leg_books_global_attribute10;
      lv_bk_gl_attribute11  := rec_cur_val_parent.leg_books_global_attribute11;
      lv_bk_gl_attribute12  := rec_cur_val_parent.leg_books_global_attribute12;
      lv_bk_gl_attribute13  := rec_cur_val_parent.leg_books_global_attribute13;
      lv_bk_gl_attribute14  := rec_cur_val_parent.leg_books_global_attribute14;
      lv_bk_gl_attribute15  := rec_cur_val_parent.leg_books_global_attribute15;
      lv_bk_gl_attribute16  := rec_cur_val_parent.leg_books_global_attribute16;
      lv_bk_gl_attribute17  := rec_cur_val_parent.leg_books_global_attribute17;
      lv_bk_gl_attribute18  := rec_cur_val_parent.leg_books_global_attribute18;
      lv_bk_gl_attribute19  := rec_cur_val_parent.leg_books_global_attribute19;
      lv_bk_gl_attribute20  := rec_cur_val_parent.leg_books_global_attribute20;
      lv_bk_gl_att_cat      := rec_cur_val_parent.leg_books_global_attr_category;

      IF lc_error_flag = 'Y' OR rec_cur_val_parent.process_flag = 'E' THEN
        g_ret_code := 1;

BEGIN
  UPDATE xxfa_corp_asset_stg
     SET process_flag                   = 'E',
         ERROR_TYPE                     = 'VAL_ERR',
         asset_category_id              = ln_category_id,
         asset_cat_segment1             = lc_cat_segment1,
         asset_cat_segment2             = lc_cat_segment2,
         request_id                     = g_request_id,
         last_updated_date              = SYSDATE,
         last_updated_by                = g_last_updated_by,
         last_update_login              = g_last_update_login,
         adtn_attribute1                = lv_attribute1,
         adtn_attribute2                = lv_attribute2,
         adtn_attribute3                = lv_attribute3,
         adtn_attribute4                = lv_attribute4,
         adtn_attribute5                = lv_attribute5,
         adtn_attribute6                = lv_attribute6,
         adtn_attribute7                = lv_attribute7,
         adtn_attribute8                = lv_attribute8,
         adtn_attribute9                = lv_attribute9,
         adtn_attribute10               = lv_attribute10,
         adtn_attribute11               = lv_attribute11,
         adtn_attribute12               = lv_attribute12,
         adtn_attribute13               = lv_attribute13,
         adtn_attribute14               = lv_attribute14,
         adtn_attribute15               = lv_attribute15,
         adtn_attribute16               = lv_attribute16,
         adtn_attribute17               = lv_attribute17,
         adtn_attribute18               = lv_attribute18,
         adtn_attribute19               = lv_attribute19,
         adtn_attribute20               = lv_attribute20,
         adtn_attribute21               = lv_attribute21,
         adtn_attribute22               = lv_attribute22,
         adtn_attribute23               = lv_attribute23,
         adtn_attribute24               = lv_attribute24,
         adtn_attribute25               = lv_attribute25,
         adtn_attribute26               = lv_attribute26,
         adtn_attribute27               = lv_attribute27,
         adtn_attribute28               = lv_attribute28,
         adtn_attribute29               = lv_attribute29,
         adtn_attribute30               = lv_attribute30,
         adtn_attribute_category_code   = lv_adn_cat_code,
         adtn_context                   = lv_adn_cont,
         adtn_global_attribute1         = lv_adn_gl_attribute1,
         adtn_global_attribute2         = lv_adn_gl_attribute2,
         adtn_global_attribute3         = lv_adn_gl_attribute3,
         adtn_global_attribute4         = lv_adn_gl_attribute4,
         adtn_global_attribute5         = lv_adn_gl_attribute5,
         adtn_global_attribute6         = lv_adn_gl_attribute6,
         adtn_global_attribute7         = lv_adn_gl_attribute7,
         adtn_global_attribute8         = lv_adn_gl_attribute8,
         adtn_global_attribute9         = lv_adn_gl_attribute9,
         adtn_global_attribute10        = lv_adn_gl_attribute10,
         adtn_global_attribute11        = lv_adn_gl_attribute11,
         adtn_global_attribute12        = lv_adn_gl_attribute12,
         adtn_global_attribute13        = lv_adn_gl_attribute13,
         adtn_global_attribute14        = lv_adn_gl_attribute14,
         adtn_global_attribute15        = lv_adn_gl_attribute15,
         adtn_global_attribute16        = lv_adn_gl_attribute16,
         adtn_global_attribute17        = lv_adn_gl_attribute17,
         adtn_global_attribute18        = lv_adn_gl_attribute18,
         adtn_global_attribute19        = lv_adn_gl_attribute19,
         adtn_global_attribute20        = lv_adn_gl_attribute20,
         adtn_global_attribute_category = lv_adn_gl_cat,
         books_global_attribute1        = lv_bk_gl_attribute1,
         books_global_attribute2        = lv_bk_gl_attribute2,
         books_global_attribute3        = lv_bk_gl_attribute3,
         books_global_attribute4        = lv_bk_gl_attribute4,
         books_global_attribute5        = lv_bk_gl_attribute5,
         books_global_attribute6        = lv_bk_gl_attribute6,
         books_global_attribute7        = lv_bk_gl_attribute7,
         books_global_attribute8        = lv_bk_gl_attribute8,
         books_global_attribute9        = lv_bk_gl_attribute9,
         books_global_attribute10       = lv_bk_gl_attribute10,
         books_global_attribute11       = lv_bk_gl_attribute11,
         books_global_attribute12       = lv_bk_gl_attribute12,
         books_global_attribute13       = lv_bk_gl_attribute13,
         books_global_attribute14       = lv_bk_gl_attribute14,
         books_global_attribute15       = lv_bk_gl_attribute15,
         books_global_attribute16       = lv_bk_gl_attribute16,
         books_global_attribute17       = lv_bk_gl_attribute17,
         books_global_attribute18       = lv_bk_gl_attribute18,
         books_global_attribute19       = lv_bk_gl_attribute19,
         books_global_attribute20       = lv_bk_gl_attribute20,
         books_global_attr_category     = lv_bk_gl_att_cat
   WHERE leg_asset_number = rec_cur_val_parent.leg_asset_number
     AND leg_book_type_code = rec_cur_val_parent.leg_book_type_code
        --AND process_flag = 'N' --1.1
     AND batch_id = g_batch_id
     AND run_sequence_id = g_new_run_seq_id;
EXCEPTION
  WHEN OTHERS THEN
    log_errors(pin_interface_txn_id    => NULL,
               piv_source_table        => 'XXFA_CORP_ASSET_STG',
               piv_source_column_name  => NULL,
               piv_source_column_value => NULL,
               piv_source_keyname1     => 'leg_asset_number',
               piv_source_keyvalue1    => rec_cur_val_parent.leg_asset_number,
               piv_error_type          => 'VAL_ERR',
               piv_error_code          => 'ETN_FA_INVALID_PROC',
               piv_error_message       => 'Error : Exception occured while updating XXFA_CORP_ASSET_STG for errors  : ' ||
                                          SUBSTR(SQLERRM, 1, 240));
    -----------As  per version v1.8----------------------------------
    UPDATE xxfa_corp_asset_stg
       SET process_flag      = 'E',
           ERROR_TYPE        = 'VAL_ERR',
           request_id        = g_request_id,
           last_updated_date = SYSDATE,
           last_updated_by   = g_last_updated_by,
           last_update_login = g_last_update_login

     WHERE leg_asset_number = rec_cur_val_parent.leg_asset_number
       AND leg_book_type_code = rec_cur_val_parent.leg_book_type_code
          --AND process_flag = 'N' --1.1
       AND batch_id = g_batch_id
       AND run_sequence_id = g_new_run_seq_id;

    commit;
   ---------As  per version v1.8------------------------------------

END;

        --
      ELSE
        BEGIN
          UPDATE xxfa_corp_asset_stg
             SET process_flag                   = 'V',
                 asset_category_id              = ln_category_id,
                 asset_cat_segment1             = lc_cat_segment1,
                 asset_cat_segment2             = lc_cat_segment2,
                 request_id                     = g_request_id,
                 last_updated_date              = SYSDATE,
                 last_updated_by                = g_last_updated_by,
                 last_update_login              = g_last_update_login,
                 adtn_attribute1                = lv_attribute1,
                 adtn_attribute2                = lv_attribute2,
                 adtn_attribute3                = lv_attribute3,
                 adtn_attribute4                = lv_attribute4,
                 adtn_attribute5                = lv_attribute5,
                 adtn_attribute6                = lv_attribute6,
                 adtn_attribute7                = lv_attribute7,
                 adtn_attribute8                = lv_attribute8,
                 adtn_attribute9                = lv_attribute9,
                 adtn_attribute10               = lv_attribute10,
                 adtn_attribute11               = lv_attribute11,
                 adtn_attribute12               = lv_attribute12,
                 adtn_attribute13               = lv_attribute13,
                 adtn_attribute14               = lv_attribute14,
                 adtn_attribute15               = lv_attribute15,
                 adtn_attribute16               = lv_attribute16,
                 adtn_attribute17               = lv_attribute17,
                 adtn_attribute18               = lv_attribute18,
                 adtn_attribute19               = lv_attribute19,
                 adtn_attribute20               = lv_attribute20,
                 adtn_attribute21               = lv_attribute21,
                 adtn_attribute22               = lv_attribute22,
                 adtn_attribute23               = lv_attribute23,
                 adtn_attribute24               = lv_attribute24,
                 adtn_attribute25               = lv_attribute25,
                 adtn_attribute26               = lv_attribute26,
                 adtn_attribute27               = lv_attribute27,
                 adtn_attribute28               = lv_attribute28,
                 adtn_attribute29               = lv_attribute29,
                 adtn_attribute30               = lv_attribute30,
                 adtn_attribute_category_code   = lv_adn_cat_code,
                 adtn_context                   = lv_adn_cont,
                 adtn_global_attribute1         = lv_adn_gl_attribute1,
                 adtn_global_attribute2         = lv_adn_gl_attribute2,
                 adtn_global_attribute3         = lv_adn_gl_attribute3,
                 adtn_global_attribute4         = lv_adn_gl_attribute4,
                 adtn_global_attribute5         = lv_adn_gl_attribute5,
                 adtn_global_attribute6         = lv_adn_gl_attribute6,
                 adtn_global_attribute7         = lv_adn_gl_attribute7,
                 adtn_global_attribute8         = lv_adn_gl_attribute8,
                 adtn_global_attribute9         = lv_adn_gl_attribute9,
                 adtn_global_attribute10        = lv_adn_gl_attribute10,
                 adtn_global_attribute11        = lv_adn_gl_attribute11,
                 adtn_global_attribute12        = lv_adn_gl_attribute12,
                 adtn_global_attribute13        = lv_adn_gl_attribute13,
                 adtn_global_attribute14        = lv_adn_gl_attribute14,
                 adtn_global_attribute15        = lv_adn_gl_attribute15,
                 adtn_global_attribute16        = lv_adn_gl_attribute16,
                 adtn_global_attribute17        = lv_adn_gl_attribute17,
                 adtn_global_attribute18        = lv_adn_gl_attribute18,
                 adtn_global_attribute19        = lv_adn_gl_attribute19,
                 adtn_global_attribute20        = lv_adn_gl_attribute20,
                 adtn_global_attribute_category = lv_adn_gl_cat,
                 books_global_attribute1        = lv_bk_gl_attribute1,
                 books_global_attribute2        = lv_bk_gl_attribute2,
                 books_global_attribute3        = lv_bk_gl_attribute3,
                 books_global_attribute4        = lv_bk_gl_attribute4,
                 books_global_attribute5        = lv_bk_gl_attribute5,
                 books_global_attribute6        = lv_bk_gl_attribute6,
                 books_global_attribute7        = lv_bk_gl_attribute7,
                 books_global_attribute8        = lv_bk_gl_attribute8,
                 books_global_attribute9        = lv_bk_gl_attribute9,
                 books_global_attribute10       = lv_bk_gl_attribute10,
                 books_global_attribute11       = lv_bk_gl_attribute11,
                 books_global_attribute12       = lv_bk_gl_attribute12,
                 books_global_attribute13       = lv_bk_gl_attribute13,
                 books_global_attribute14       = lv_bk_gl_attribute14,
                 books_global_attribute15       = lv_bk_gl_attribute15,
                 books_global_attribute16       = lv_bk_gl_attribute16,
                 books_global_attribute17       = lv_bk_gl_attribute17,
                 books_global_attribute18       = lv_bk_gl_attribute18,
                 books_global_attribute19       = lv_bk_gl_attribute19,
                 books_global_attribute20       = lv_bk_gl_attribute20,
                 books_global_attr_category     = lv_bk_gl_att_cat
           WHERE leg_asset_number = rec_cur_val_parent.leg_asset_number
             AND leg_book_type_code = rec_cur_val_parent.leg_book_type_code
                --AND process_flag = 'N'   --1.1
             AND batch_id = g_batch_id
             AND run_sequence_id = g_new_run_seq_id;
        EXCEPTION
          WHEN OTHERS THEN
            log_errors(pin_interface_txn_id    => NULL,
                       piv_source_table        => 'XXFA_CORP_ASSET_STG',
                       piv_source_column_name  => NULL,
                       piv_source_column_value => NULL,
                       piv_source_keyname1     => 'leg_asset_number',
                       piv_source_keyvalue1    => rec_cur_val_parent.leg_asset_number,
                       piv_error_type          => 'VAL_ERR',
                       piv_error_code          => 'ETN_FA_INVALID_PROC',
                       piv_error_message       => 'Error : Exception occured while updating XXFA_CORP_ASSET_STG for Validation  : ' ||
                                                  SUBSTR(SQLERRM, 1, 240));


        END;

        ---
      END IF;

      IF l_count >= 100 THEN
        l_count := 0;
        xxetn_debug_pkg.add_debug('
                        Performing Batch Commit');
        COMMIT;
      ELSE
        l_count := l_count + 1;
      END IF;
    END LOOP;

    COMMIT;
    print_log_message('Validation for Parent Assets with Corporate Book Ends : ');
    l_count := 0;

    -- Child loop starts
    FOR rec_cur_val_child IN cur_val_child LOOP
      lc_error_flag          := 'N';
      ln_property_code       := NULL;
      ln_tax_category        := NULL;
      ln_category_id         := NULL;
      lc_cat_segment1        := NULL;
      lc_cat_segment2        := NULL;
      lc_cat_flag            := NULL;
      ln_prop_1245_1250_code := NULL;
      ln_own_lease           := NULL;
      ln_new_use             := NULL;
      ln_life_in_months      := NULL;
      ln_prorate_count       := NULL;
      ln_ceiling_count       := NULL;
      ln_bonus_count         := NULL;
      ln_parent_count        := NULL;
      ln_asset_key           := NULL;
      lc_cat_msg             := NULL;
      lc_ass_flag            := NULL;
      lv_attribute1          := NULL;
      lv_attribute2          := NULL;
      lv_attribute3          := NULL;
      lv_attribute4          := NULL;
      lv_attribute5          := NULL;
      lv_attribute6          := NULL;
      lv_attribute7          := NULL;
      lv_attribute8          := NULL;
      lv_attribute9          := NULL;
      lv_attribute10         := NULL;
      lv_attribute11         := NULL;
      lv_attribute12         := NULL;
      lv_attribute13         := NULL;
      lv_attribute14         := NULL;
      lv_attribute15         := NULL;
      lv_attribute16         := NULL;
      lv_attribute17         := NULL;
      lv_attribute18         := NULL;
      lv_attribute19         := NULL;
      lv_attribute20         := NULL;
      lv_attribute21         := NULL;
      lv_attribute22         := NULL;
      lv_attribute23         := NULL;
      lv_attribute24         := NULL;
      lv_attribute25         := NULL;
      lv_attribute26         := NULL;
      lv_attribute27         := NULL;
      lv_attribute28         := NULL;
      lv_attribute29         := NULL;
      lv_attribute30         := NULL;
      lv_adn_cat_code        := NULL;
      lv_adn_cont            := NULL;
      lv_adn_gl_attribute1   := NULL;
      lv_adn_gl_attribute2   := NULL;
      lv_adn_gl_attribute3   := NULL;
      lv_adn_gl_attribute4   := NULL;
      lv_adn_gl_attribute5   := NULL;
      lv_adn_gl_attribute6   := NULL;
      lv_adn_gl_attribute7   := NULL;
      lv_adn_gl_attribute8   := NULL;
      lv_adn_gl_attribute9   := NULL;
      lv_adn_gl_attribute10  := NULL;
      lv_adn_gl_attribute11  := NULL;
      lv_adn_gl_attribute12  := NULL;
      lv_adn_gl_attribute13  := NULL;
      lv_adn_gl_attribute14  := NULL;
      lv_adn_gl_attribute15  := NULL;
      lv_adn_gl_attribute16  := NULL;
      lv_adn_gl_attribute17  := NULL;
      lv_adn_gl_attribute18  := NULL;
      lv_adn_gl_attribute19  := NULL;
      lv_adn_gl_attribute20  := NULL;
      lv_adn_gl_cat          := NULL;
      lv_bk_gl_attribute1    := NULL;
      lv_bk_gl_attribute2    := NULL;
      lv_bk_gl_attribute3    := NULL;
      lv_bk_gl_attribute4    := NULL;
      lv_bk_gl_attribute5    := NULL;
      lv_bk_gl_attribute6    := NULL;
      lv_bk_gl_attribute7    := NULL;
      lv_bk_gl_attribute8    := NULL;
      lv_bk_gl_attribute9    := NULL;
      lv_bk_gl_attribute10   := NULL;
      lv_bk_gl_attribute11   := NULL;
      lv_bk_gl_attribute12   := NULL;
      lv_bk_gl_attribute13   := NULL;
      lv_bk_gl_attribute14   := NULL;
      lv_bk_gl_attribute15   := NULL;
      lv_bk_gl_attribute16   := NULL;
      lv_bk_gl_attribute17   := NULL;
      lv_bk_gl_attribute18   := NULL;
      lv_bk_gl_attribute19   := NULL;
      lv_bk_gl_attribute20   := NULL;
      lv_bk_gl_att_cat       := NULL;
      xxetn_debug_pkg.add_debug(piv_debug_msg => 'Validation process for Child asset starts for asset number : ' ||
                                                 rec_cur_val_child.leg_asset_number ||
                                                 ' and book type : ' ||
                                                 rec_cur_val_child.leg_book_type_code);
      -- Check for duplicacy of asset number
      duplicate_check(rec_cur_val_child.leg_asset_number); --1.1
      --To check if asset number already exists in the system
      lc_ass_flag := validate_asset_exist(rec_cur_val_child.leg_asset_number);

      IF lc_ass_flag = 'Y' THEN
        lc_error_flag := 'Y';
      END IF;

      -- legacy current units must not be NULL
      IF rec_cur_val_child.leg_current_units IS NULL THEN
        log_errors(pin_interface_txn_id    => NULL,
                   piv_source_table        => 'XXFA_CORP_ASSET_STG',
                   piv_source_column_name  => 'leg_current_units',
                   piv_source_column_value => rec_cur_val_child.leg_current_units,
                   piv_source_keyname1     => 'leg_asset_number',
                   piv_source_keyvalue1    => rec_cur_val_child.leg_asset_number,
                   piv_error_type          => 'VAL_ERR',
                   piv_error_code          => 'ETN_FA_MANDATORY_COLUMN',
                   piv_error_message       => 'Error : leg_current_units cannot be NULL');
        lc_error_flag := 'Y';
      ELSE
        IF rec_cur_val_child.leg_current_units < 0 THEN
          log_errors(pin_interface_txn_id    => NULL,
                     piv_source_table        => 'XXFA_CORP_ASSET_STG',
                     piv_source_column_name  => 'leg_current_units',
                     piv_source_column_value => rec_cur_val_child.leg_current_units,
                     piv_source_keyname1     => 'leg_asset_number',
                     piv_source_keyvalue1    => rec_cur_val_child.leg_asset_number,
                     piv_error_type          => 'VAL_ERR',
                     piv_error_code          => 'ETN_FA_INVALID_CURRENT_UNIT',
                     piv_error_message       => 'Error : leg_current_units cannot be less than 0');
          lc_error_flag := 'Y';
        END IF;
      END IF;

      -- Asset type must not be NULL
      IF rec_cur_val_child.leg_asset_type IS NULL THEN
        log_errors(pin_interface_txn_id    => NULL,
                   piv_source_table        => 'XXFA_CORP_ASSET_STG',
                   piv_source_column_name  => 'leg_asset_type',
                   piv_source_column_value => rec_cur_val_child.leg_asset_type,
                   piv_source_keyname1     => 'leg_asset_number',
                   piv_source_keyvalue1    => rec_cur_val_child.leg_asset_number,
                   piv_error_type          => 'VAL_ERR',
                   piv_error_code          => 'ETN_FA_MANDATORY_COLUMN',
                   piv_error_message       => 'Error : leg_asset_type cannot be NULL');
        lc_error_flag := 'Y';
      ELSE
        IF rec_cur_val_child.leg_asset_type <> g_asset_type THEN
          log_errors(pin_interface_txn_id    => NULL,
                     piv_source_table        => 'XXFA_CORP_ASSET_STG',
                     piv_source_column_name  => 'leg_asset_type',
                     piv_source_column_value => rec_cur_val_child.leg_asset_type,
                     piv_source_keyname1     => 'leg_asset_number',
                     piv_source_keyvalue1    => rec_cur_val_child.leg_asset_number,
                     piv_error_type          => 'VAL_ERR',
                     piv_error_code          => 'ETN_FA_INVALID_ASSET_TYPE',
                     piv_error_message       => 'Error : leg_asset_type is not CAPITALIZED');
          lc_error_flag := 'Y';
        END IF;
      END IF;

      -- To check duplicacy of tag number starts  --1.1
      --duplicate_tag_number_check (rec_cur_val_child.leg_tag_number);

      -- To check property type code starts  --1.1
      /* IF rec_cur_val_child.leg_property_type_code IS NOT NULL
      THEN
         ln_property_code :=
            get_lookup_value ('PROPERTY TYPE',
                              'XXFA_CORP_ASSET_STG',
                              'leg_property_type_code',
                              rec_cur_val_child.leg_property_type_code,
                              'leg_asset_number',
                              rec_cur_val_child.leg_asset_number,
                              NULL
                             );

         IF ln_property_code = 0
         THEN
            lc_error_flag := 'Y';
         END IF;
      END IF; */
      -----------------added as per v1.5---------------------------------------
      set_cat_lookup(p_leg_source_asset_number => rec_cur_val_child.leg_source_asset_number,
                     p_leg_book_type_code      => rec_cur_val_child.leg_book_type_code); --added by harjinder sing for the PMC#349366
      ---------------- added as per v1.5---------------------------------------

      IF g_cat_lookup = 'ETN_FA_CATEGORY_MAP_POLAND' THEN
        validate_asset_category_poland(rec_cur_val_child.leg_source_system,
                                       rec_cur_val_child.leg_asset_number,
                                       rec_cur_val_child.leg_book_type_code,
                                       rec_cur_val_child.leg_asset_cat_segment1,
                                       ln_tax_category,
                                       ln_category_id,
                                       lc_cat_segment1,
                                       lc_cat_segment2,
                                       lc_cat_flag,
                                       lc_cat_msg);

        IF lc_cat_flag = 'Y' THEN
          log_errors(pin_interface_txn_id    => NULL,
                     piv_source_table        => 'XXFA_CORP_ASSET_STG',
                     piv_source_column_name  => 'leg_asset_cat_segment1.leg_asset_cat_segment2',
                     piv_source_column_value => rec_cur_val_child.leg_asset_cat_segment1 ||
                                                g_sep_p || ln_tax_category,
                     piv_source_keyname1     => 'leg_asset_number',
                     piv_source_keyvalue1    => rec_cur_val_child.leg_asset_number,
                     piv_error_type          => 'VAL_ERR',
                     piv_error_code          => 'ETN_FA_INVALID_CATEGORY',
                     piv_error_message       => lc_cat_msg);
          lc_error_flag := 'Y';
        END IF;

      ELSE
        -- check category segment starts
        validate_asset_category(rec_cur_val_child.leg_source_system,
                                rec_cur_val_child.leg_asset_cat_segment1,
                                rec_cur_val_child.leg_asset_cat_segment2,
                                ln_category_id,
                                lc_cat_segment1,
                                lc_cat_segment2,
                                lc_cat_flag,
                                lc_cat_msg);

        IF lc_cat_flag = 'Y' THEN
          log_errors(pin_interface_txn_id    => NULL,
                     piv_source_table        => 'XXFA_CORP_ASSET_STG',
                     piv_source_column_name  => 'leg_asset_cat_segment1.leg_asset_cat_segment2',
                     piv_source_column_value => rec_cur_val_child.leg_asset_cat_segment1 ||
                                                g_sep ||
                                                rec_cur_val_child.leg_asset_cat_segment2,
                     piv_source_keyname1     => 'leg_asset_number',
                     piv_source_keyvalue1    => rec_cur_val_child.leg_asset_number,
                     piv_error_type          => 'VAL_ERR',
                     piv_error_code          => 'ETN_FA_INVALID_CATEGORY',
                     piv_error_message       => lc_cat_msg);
          lc_error_flag := 'Y';
        END IF;
      END IF;

      -- To check property 1245/1250 type code starts  -- 1.1
      /* IF rec_cur_val_child.leg_property_1245_1250_code IS NOT NULL
      THEN
         ln_prop_1245_1250_code :=
            get_lookup_value
                           ('1245/1250 PROPERTY',
                            'XXFA_CORP_ASSET_STG',
                            'leg_property_1245_1250_code',
                            rec_cur_val_child.leg_property_1245_1250_code,
                            'leg_asset_number',
                            rec_cur_val_child.leg_asset_number,
                            NULL
                           );

         IF ln_prop_1245_1250_code = 0
         THEN
            lc_error_flag := 'Y';
         END IF;
      END IF; */

      -- To check owned/leased code starts  -- 1.1
      /* IF rec_cur_val_child.leg_owned_leased IS NOT NULL
      THEN
         ln_own_lease :=
            get_lookup_value ('OWNLEASE',
                              'XXFA_CORP_ASSET_STG',
                              'leg_owned_leased',
                              rec_cur_val_child.leg_owned_leased,
                              'leg_asset_number',
                              rec_cur_val_child.leg_asset_number,
                              NULL
                             );

         IF ln_own_lease = 0
         THEN
            lc_error_flag := 'Y';
         END IF;
      END IF; */

      -- To check NEWUSE code starts  -- 1.1
      /* IF rec_cur_val_child.leg_new_used IS NOT NULL
      THEN
         ln_new_use :=
            get_lookup_value ('NEWUSE',
                              'XXFA_CORP_ASSET_STG',
                              'leg_new_used',
                              rec_cur_val_child.leg_new_used,
                              'leg_asset_number',
                              rec_cur_val_child.leg_asset_number,
                              NULL
                             );

         IF ln_new_use = 0
         THEN
            lc_error_flag := 'Y';
         END IF;
      END IF; */

      --- Check for book type code starts  --1.1
      /* BEGIN
         SELECT COUNT (1)
           INTO ln_book_code
           FROM fa_book_controls
          WHERE book_type_code = rec_cur_val_child.leg_book_type_code;
      EXCEPTION
         WHEN OTHERS
         THEN
            log_errors
               (pin_interface_txn_id         => NULL,
                piv_source_table             => 'XXFA_CORP_ASSET_STG',
                piv_source_column_name       => NULL,
                piv_source_column_value      => NULL,
                piv_error_type               => 'VAL_ERR',
                piv_source_keyname1          => 'leg_asset_number',
                piv_source_keyvalue1         => rec_cur_val_child.leg_asset_number,
                piv_error_code               => 'ETN_FA_INVALID_BOOK_TYPE',
                piv_error_message            =>    'Error : Exception occured while fetching book type code from FA_BOOK_CONTROLS : '
                                                || SUBSTR (SQLERRM, 1, 240)
               );
            lc_error_flag := 'Y';
      END;

      IF ln_book_code = 0
      THEN
         log_errors
            (pin_interface_txn_id         => NULL,
             piv_source_table             => 'XXFA_CORP_ASSET_STG',
             piv_source_column_name       => 'leg_book_type_code',
             piv_source_column_value      => rec_cur_val_child.leg_book_type_code,
             piv_source_keyname1          => 'leg_asset_number',
             piv_source_keyvalue1         => rec_cur_val_child.leg_asset_number,
             piv_error_type               => 'VAL_ERR',
             piv_error_code               => 'ETN_FA_INVALID_BOOK_TYPE',
             piv_error_message            => 'Error : book type code not valid : '
            );
         lc_error_flag := 'Y';
      END IF; */

      --- To check date placed in service
      IF rec_cur_val_child.leg_date_placed_in_service IS NULL THEN
        log_errors(pin_interface_txn_id    => NULL,
                   piv_source_table        => 'XXFA_CORP_ASSET_STG',
                   piv_source_column_name  => 'leg_date_placed_in_service',
                   piv_source_column_value => rec_cur_val_child.leg_date_placed_in_service,
                   piv_source_keyname1     => 'leg_asset_number',
                   piv_source_keyvalue1    => rec_cur_val_child.leg_asset_number,
                   piv_error_type          => 'VAL_ERR',
                   piv_error_code          => 'ETN_FA_MANDATORY_COLUMN',
                   piv_error_message       => 'Error : Date placed in service cannot be NULL  : ');
        lc_error_flag := 'Y';
      END IF;

      -- check for depreciation method code starts  -- 1.1
      /* IF rec_cur_val_child.leg_deprn_method_code IS NULL
      THEN
         log_errors
            (pin_interface_txn_id         => NULL,
             piv_source_table             => 'XXFA_CORP_ASSET_STG',
             piv_source_column_name       => 'leg_deprn_method_code',
             piv_source_column_value      => rec_cur_val_child.leg_deprn_method_code,
             piv_source_keyname1          => 'leg_asset_number',
             piv_source_keyvalue1         => rec_cur_val_child.leg_asset_number,
             piv_error_type               => 'VAL_ERR',
             piv_error_code               => 'ETN_FA_MANDATORY_COLUMN',
             piv_error_message            => 'Error : leg_deprn_method_code cannot be NULL'
            );
         lc_error_flag := 'Y';
      ELSE
         BEGIN
            SELECT COUNT (1)
              INTO ln_life_in_months
              FROM fa_methods
             WHERE method_code = rec_cur_val_child.leg_deprn_method_code
               AND life_in_months = rec_cur_val_child.leg_life_in_months;
         EXCEPTION
            WHEN OTHERS
            THEN
               log_errors
                  (pin_interface_txn_id         => NULL,
                   piv_source_table             => 'XXFA_CORP_ASSET_STG',
                   piv_source_column_name       => NULL,
                   piv_source_column_value      => NULL,
                   piv_source_keyname1          => 'leg_asset_number',
                   piv_source_keyvalue1         => rec_cur_val_child.leg_asset_number,
                   piv_error_type               => 'VAL_ERR',
                   piv_error_code               => 'ETN_FA_INVALID_DEPRN_METHOD',
                   piv_error_message            =>    'Error : Exception occured while fetching depreciation method code : '
                                                   || SUBSTR (SQLERRM,
                                                              1,
                                                              240
                                                             )
                  );
               lc_error_flag := 'Y';
         END;

         IF ln_life_in_months = 0
         THEN
            log_errors
               (pin_interface_txn_id         => NULL,
                piv_source_table             => 'XXFA_CORP_ASSET_STG',
                piv_source_column_name       => 'leg_deprn_method_code',
                piv_source_column_value      => rec_cur_val_child.leg_deprn_method_code,
                piv_source_keyname1          => 'leg_asset_number',
                piv_source_keyvalue1         => rec_cur_val_child.leg_asset_number,
                piv_error_type               => 'VAL_ERR',
                piv_error_code               => 'ETN_FA_INVALID_DEPRN_METHOD',
                piv_error_message            => 'Error : Depreciation method code not valid  : '
               );
            lc_error_flag := 'Y';
         END IF;
      END IF; */

      -- check for legacy cost
      IF rec_cur_val_child.leg_cost IS NULL THEN
        log_errors(pin_interface_txn_id    => NULL,
                   piv_source_table        => 'XXFA_CORP_ASSET_STG',
                   piv_source_column_name  => 'leg_cost',
                   piv_source_column_value => rec_cur_val_child.leg_cost,
                   piv_source_keyname1     => 'leg_asset_number',
                   piv_source_keyvalue1    => rec_cur_val_child.leg_asset_number,
                   piv_error_type          => 'VAL_ERR',
                   piv_error_code          => 'ETN_FA_MANDATORY_COLUMN',
                   piv_error_message       => 'Error : leg_cost cannot be NULL');
        lc_error_flag := 'Y';
      END IF;

      -- check for legacy original cost
      IF rec_cur_val_child.leg_original_cost IS NULL THEN
        log_errors(pin_interface_txn_id    => NULL,
                   piv_source_table        => 'XXFA_CORP_ASSET_STG',
                   piv_source_column_name  => 'leg_original_cost  ',
                   piv_source_column_value => rec_cur_val_child.leg_original_cost,
                   piv_source_keyname1     => 'leg_asset_number',
                   piv_source_keyvalue1    => rec_cur_val_child.leg_asset_number,
                   piv_error_type          => 'VAL_ERR',
                   piv_error_code          => 'ETN_FA_MANDATORY_COLUMN',
                   piv_error_message       => 'Error : leg_original_cost cannot be NULL');
        lc_error_flag := 'Y';
      END IF;

      -- check for legacy salvage value
      IF rec_cur_val_child.leg_salvage_value IS NULL THEN
        log_errors(pin_interface_txn_id    => NULL,
                   piv_source_table        => 'XXFA_CORP_ASSET_STG',
                   piv_source_column_name  => 'leg_salvage_value',
                   piv_source_column_value => rec_cur_val_child.leg_salvage_value,
                   piv_source_keyname1     => 'leg_asset_number',
                   piv_source_keyvalue1    => rec_cur_val_child.leg_asset_number,
                   piv_error_type          => 'VAL_ERR',
                   piv_error_code          => 'ETN_FA_MANDATORY_COLUMN',
                   piv_error_message       => 'Error : leg_salvage_value cannot be NULL');
        lc_error_flag := 'Y';
      END IF;

      -- check for prorate convention code  -- 1.1
      /* IF rec_cur_val_child.leg_prorate_convention_code IS NULL
      THEN
         log_errors
            (pin_interface_txn_id         => NULL,
             piv_source_table             => 'XXFA_CORP_ASSET_STG',
             piv_source_column_name       => 'leg_prorate_convention_code',
             piv_source_column_value      => rec_cur_val_child.leg_prorate_convention_code,
             piv_source_keyname1          => 'leg_asset_number',
             piv_source_keyvalue1         => rec_cur_val_child.leg_asset_number,
             piv_error_type               => 'VAL_ERR',
             piv_error_code               => 'ETN_FA_MANDATORY_COLUMN',
             piv_error_message            => 'Error : leg_prorate_convention_code cannot be NULL'
            );
         lc_error_flag := 'Y';
      ELSE
         BEGIN
            SELECT COUNT (1)
              INTO ln_prorate_count
              FROM fa_convention_types
             WHERE prorate_convention_code =
                              rec_cur_val_child.leg_prorate_convention_code;
         EXCEPTION
            WHEN OTHERS
            THEN
               log_errors
                  (pin_interface_txn_id         => NULL,
                   piv_source_table             => 'XXFA_CORP_ASSET_STG',
                   piv_source_column_name       => NULL,
                   piv_source_column_value      => NULL,
                   piv_source_keyname1          => 'leg_asset_number',
                   piv_source_keyvalue1         => rec_cur_val_child.leg_asset_number,
                   piv_error_type               => 'VAL_ERR',
                   piv_error_code               => 'ETN_FA_INVALID_PRORATE_CODE',
                   piv_error_message            =>    'Error : Exception occured while fetching prorate_convention_code : '
                                                   || SUBSTR (SQLERRM,
                                                              1,
                                                              240
                                                             )
                  );
               lc_error_flag := 'Y';
         END;

         IF ln_prorate_count = 0
         THEN
            log_errors
               (pin_interface_txn_id         => NULL,
                piv_source_table             => 'XXFA_CORP_ASSET_STG',
                piv_source_column_name       => 'leg_prorate_convention_code',
                piv_source_column_value      => rec_cur_val_child.leg_prorate_convention_code,
                piv_source_keyname1          => 'leg_asset_number',
                piv_source_keyvalue1         => rec_cur_val_child.leg_asset_number,
                piv_error_type               => 'VAL_ERR',
                piv_error_code               => 'ETN_FA_INVALID_PRORATE_CODE',
                piv_error_message            => 'Error : Prorate Convention code not valid  : '
               );
            lc_error_flag := 'Y';
         END IF;
      END IF; */

      -- check for capitalize flag
      IF rec_cur_val_child.leg_capitalize_flag IS NULL THEN
        log_errors(pin_interface_txn_id    => NULL,
                   piv_source_table        => 'XXFA_CORP_ASSET_STG',
                   piv_source_column_name  => 'leg_capitalize_flag',
                   piv_source_column_value => rec_cur_val_child.leg_capitalize_flag,
                   piv_source_keyname1     => 'leg_asset_number',
                   piv_source_keyvalue1    => rec_cur_val_child.leg_asset_number,
                   piv_error_type          => 'VAL_ERR',
                   piv_error_code          => 'ETN_FA_MANDATORY_COLUMN',
                   piv_error_message       => 'Error : leg_capitalize_flag cannot be NULL');
        lc_error_flag := 'Y';
      END IF;

      -- check for depreciate flag
      IF rec_cur_val_child.leg_depreciate_flag IS NULL THEN
        log_errors(pin_interface_txn_id    => NULL,
                   piv_source_table        => 'XXFA_CORP_ASSET_STG',
                   piv_source_column_name  => 'leg_depreciate_flag',
                   piv_source_column_value => rec_cur_val_child.leg_depreciate_flag,
                   piv_source_keyname1     => 'leg_asset_number',
                   piv_source_keyvalue1    => rec_cur_val_child.leg_asset_number,
                   piv_error_type          => 'VAL_ERR',
                   piv_error_code          => 'ETN_FA_MANDATORY_COLUMN',
                   piv_error_message       => 'Error : leg_depreciate_flag cannot be NULL');
        lc_error_flag := 'Y';
      END IF;

      -- check for bonus rule
      IF rec_cur_val_child.leg_bonus_rule IS NOT NULL THEN
        BEGIN
          SELECT COUNT(1)
            INTO ln_bonus_count
            FROM fa_bonus_rules
           WHERE bonus_rule = rec_cur_val_child.leg_bonus_rule;
        EXCEPTION
          WHEN OTHERS THEN
            log_errors(pin_interface_txn_id    => NULL,
                       piv_source_table        => 'XXFA_CORP_ASSET_STG',
                       piv_source_column_name  => NULL,
                       piv_source_column_value => NULL,
                       piv_source_keyname1     => 'leg_asset_number',
                       piv_source_keyvalue1    => rec_cur_val_child.leg_asset_number,
                       piv_error_type          => 'VAL_ERR',
                       piv_error_code          => 'ETN_FA_INVALID_BONUS_RULE',
                       piv_error_message       => 'Error : Exception occured while fetching bonus_rule : ' ||
                                                  SUBSTR(SQLERRM, 1, 240));
            lc_error_flag := 'Y';
        END;

        IF ln_bonus_count = 0 THEN
          log_errors(pin_interface_txn_id    => NULL,
                     piv_source_table        => 'XXFA_CORP_ASSET_STG',
                     piv_source_column_name  => 'leg_bonus_rule',
                     piv_source_column_value => rec_cur_val_child.leg_bonus_rule,
                     piv_source_keyname1     => 'leg_asset_number',
                     piv_source_keyvalue1    => rec_cur_val_child.leg_asset_number,
                     piv_error_type          => 'VAL_ERR',
                     piv_error_code          => 'ETN_FA_INVALID_BONUS_RULE',
                     piv_error_message       => 'Error : bonus_rule not valid  : ');
          lc_error_flag := 'Y';
        END IF;
      END IF;

      -- check for ceiling_name  --1.1
      /* IF rec_cur_val_child.leg_ceiling_name IS NOT NULL
      THEN
         BEGIN
            SELECT COUNT (1)
              INTO ln_ceiling_count
              FROM fa_ceilings
             WHERE ceiling_name = rec_cur_val_child.leg_ceiling_name;
         EXCEPTION
            WHEN OTHERS
            THEN
               log_errors
                  (pin_interface_txn_id         => NULL,
                   piv_source_table             => 'XXFA_CORP_ASSET_STG',
                   piv_source_column_name       => NULL,
                   piv_source_column_value      => NULL,
                   piv_source_keyname1          => 'leg_asset_number',
                   piv_source_keyvalue1         => rec_cur_val_child.leg_asset_number,
                   piv_error_type               => 'VAL_ERR',
                   piv_error_code               => 'ETN_FA_INVALID_CEILING_NAME',
                   piv_error_message            =>    'Error : Exception occured while fetching ceiling_name : '
                                                   || SUBSTR (SQLERRM,
                                                              1,
                                                              240
                                                             )
                  );
               lc_error_flag := 'Y';
         END;

         IF ln_ceiling_count = 0
         THEN
            log_errors
               (pin_interface_txn_id         => NULL,
                piv_source_table             => 'XXFA_CORP_ASSET_STG',
                piv_source_column_name       => 'leg_ceiling_name',
                piv_source_column_value      => rec_cur_val_child.leg_ceiling_name,
                piv_source_keyname1          => 'leg_asset_number',
                piv_source_keyvalue1         => rec_cur_val_child.leg_asset_number,
                piv_error_type               => 'VAL_ERR',
                piv_error_code               => 'ETN_FA_INVALID_CEILING_NAME',
                piv_error_message            => 'Error : ceiling_name code not valid  : '
               );
            lc_error_flag := 'Y';
         END IF;
      END IF; */

      -- check for YTD deprcn
      IF rec_cur_val_child.leg_ytd_deprn IS NULL THEN
        log_errors(pin_interface_txn_id    => NULL,
                   piv_source_table        => 'XXFA_CORP_ASSET_STG',
                   piv_source_column_name  => 'leg_ytd_deprn',
                   piv_source_column_value => rec_cur_val_child.leg_ytd_deprn,
                   piv_source_keyname1     => 'leg_asset_number',
                   piv_source_keyvalue1    => rec_cur_val_child.leg_asset_number,
                   piv_error_type          => 'VAL_ERR',
                   piv_error_code          => 'ETN_FA_MANDATORY_COLUMN',
                   piv_error_message       => 'Error : leg_ytd_deprn cannot be NULL');
        lc_error_flag := 'Y';
      ELSE
        IF (rec_cur_val_child.leg_cost < 0) AND
           (rec_cur_val_child.leg_recoverable_cost < 0) AND
           (rec_cur_val_child.leg_ytd_deprn <= 0) AND
           (rec_cur_val_child.leg_deprn_reserve <= 0) THEN
          IF ABS(rec_cur_val_child.leg_ytd_deprn) >
             ABS(rec_cur_val_child.leg_deprn_reserve) THEN
            log_errors(pin_interface_txn_id    => NULL,
                       piv_source_table        => 'XXFA_CORP_ASSET_STG',
                       piv_source_column_name  => 'leg_ytd_deprn',
                       piv_source_column_value => rec_cur_val_child.leg_ytd_deprn,
                       piv_source_keyname1     => 'leg_asset_number',
                       piv_source_keyvalue1    => rec_cur_val_child.leg_asset_number,
                       piv_error_type          => 'VAL_ERR',
                       piv_error_code          => 'ETN_FA_INVALID_YTD_DERPN',
                       piv_error_message       => 'Error : leg_ytd_deprn cannot be greater than leg_deprn_reserve');
            lc_error_flag := 'Y';
          END IF;
        ELSE
          IF rec_cur_val_child.leg_ytd_deprn >
             rec_cur_val_child.leg_deprn_reserve THEN
            log_errors(pin_interface_txn_id    => NULL,
                       piv_source_table        => 'XXFA_CORP_ASSET_STG',
                       piv_source_column_name  => 'leg_ytd_deprn',
                       piv_source_column_value => rec_cur_val_child.leg_ytd_deprn,
                       piv_source_keyname1     => 'leg_asset_number',
                       piv_source_keyvalue1    => rec_cur_val_child.leg_asset_number,
                       piv_error_type          => 'VAL_ERR',
                       piv_error_code          => 'ETN_FA_INVALID_YTD_DERPN',
                       piv_error_message       => 'Error : leg_ytd_deprn cannot be greater than leg_deprn_reserve');
            lc_error_flag := 'Y';
          END IF;
        END IF;
      END IF;

      -- check for deprn reserve
      IF rec_cur_val_child.leg_deprn_reserve IS NULL THEN
        log_errors(pin_interface_txn_id    => NULL,
                   piv_source_table        => 'XXFA_CORP_ASSET_STG',
                   piv_source_column_name  => 'leg_deprn_reserve',
                   piv_source_column_value => rec_cur_val_child.leg_deprn_reserve,
                   piv_source_keyname1     => 'leg_asset_number',
                   piv_source_keyvalue1    => rec_cur_val_child.leg_asset_number,
                   piv_error_type          => 'VAL_ERR',
                   piv_error_code          => 'ETN_FA_MANDATORY_COLUMN',
                   piv_error_message       => 'Error : leg_deprn_reserve cannot be NULL');
        lc_error_flag := 'Y';
      END IF;

      lc_master_dist_flag := 'N';

      --- Check Distribution Details
      FOR rec_get_distribution_details IN get_distribution_details(rec_cur_val_child.leg_asset_number,
                                                                   --  rec_cur_val_child.leg_book_type_code
                                                                   rec_cur_val_child.book_type_code) LOOP
        ln_location_id     := NULL;
        ln_employee_id     := NULL;
        x_out_acc_rec      := NULL;
        x_ccid             := NULL;
        lc_dist_error_flag := 'N';

        IF lc_master_dist_flag = 'Y' THEN
          log_errors(pin_interface_txn_id    => NULL,
                     piv_source_table        => 'XXFA_CORP_ASSET_STG',
                     piv_source_column_name  => NULL,
                     piv_source_column_value => NULL,
                     piv_source_keyname1     => 'interface_txn_id',
                     piv_source_keyvalue1    => rec_get_distribution_details.interface_txn_id,
                     piv_error_type          => 'VAL_ERR',
                     piv_error_code          => 'ETN_FA_DEPDNT_DIST_ERROR',
                     piv_error_message       => 'Error : Record erred out due to dependent distribution erring out :');
        END IF;

        ---location 1.1
        /* BEGIN
           SELECT location_id
             INTO ln_location_id
             FROM fa_locations
            WHERE enabled_flag = 'Y'
              AND segment1 =
                            rec_get_distribution_details.leg_locn_segment1
              AND segment2 =
                            rec_get_distribution_details.leg_locn_segment2
              AND segment3 =
                            rec_get_distribution_details.leg_locn_segment3
              AND segment4 =
                            rec_get_distribution_details.leg_locn_segment4;
        EXCEPTION
           WHEN NO_DATA_FOUND
           THEN
              log_errors
                 (pin_interface_txn_id         => NULL,
                  piv_source_table             => 'XXFA_CORP_ASSET_STG',
                  piv_source_column_name       => 'leg_locn_segments_conc_value',
                  piv_source_column_value      =>    rec_get_distribution_details.leg_locn_segment1
                                                  || '.'
                                                  || rec_get_distribution_details.leg_locn_segment2
                                                  || '.'
                                                  || rec_get_distribution_details.leg_locn_segment3
                                                  || '.'
                                                  || rec_get_distribution_details.leg_locn_segment4,
                  piv_source_keyname1          => 'interface_txn_id',
                  piv_source_keyvalue1         => rec_get_distribution_details.interface_txn_id,
                  piv_error_type               => 'VAL_ERR',
                  piv_error_code               => 'ETN_FA_INVALID_LOCATION',
                  piv_error_message            => 'Error : Given location segments codes are not valid'
                 );
              lc_dist_error_flag := 'Y';
           WHEN OTHERS
           THEN
              log_errors
                 (pin_interface_txn_id         => NULL,
                  piv_source_table             => 'XXFA_CORP_ASSET_STG',
                  piv_source_column_name       => NULL,
                  piv_source_column_value      => NULL,
                  piv_source_keyname1          => 'interface_txn_id',
                  piv_source_keyvalue1         => rec_get_distribution_details.interface_txn_id,
                  piv_error_type               => 'VAL_ERR',
                  piv_error_code               => 'ETN_FA_INVALID_LOCATION',
                  piv_error_message            =>    'Error : Exception occurred while fetching location : '
                                                  || SUBSTR (SQLERRM,
                                                             1,
                                                             240
                                                            )
                 );
              lc_dist_error_flag := 'Y';
        END; */
        IF rec_get_distribution_details.leg_assigned_emp_number IS NOT NULL THEN
          BEGIN
            SELECT employee_id
              INTO ln_employee_id
              FROM fa_employees
             WHERE employee_number =
                   rec_get_distribution_details.leg_assigned_emp_number;
          EXCEPTION
            WHEN NO_DATA_FOUND THEN
              log_errors(pin_interface_txn_id    => NULL,
                         piv_source_table        => 'XXFA_CORP_ASSET_STG',
                         piv_source_column_name  => 'leg_assigned_emp_number',
                         piv_source_column_value => rec_get_distribution_details.leg_assigned_emp_number,
                         piv_source_keyname1     => 'interface_txn_id',
                         piv_source_keyvalue1    => rec_get_distribution_details.interface_txn_id,
                         piv_error_type          => 'VAL_ERR',
                         piv_error_code          => 'ETN_FA_INVALID_EMPLOYEE',
                         piv_error_message       => 'Error : Given employee number not valid');
              lc_dist_error_flag := 'Y';
            WHEN OTHERS THEN
              log_errors(pin_interface_txn_id    => NULL,
                         piv_source_table        => 'XXFA_CORP_ASSET_STG',
                         piv_source_column_name  => NULL,
                         piv_source_column_value => NULL,
                         piv_source_keyname1     => 'interface_txn_id',
                         piv_source_keyvalue1    => rec_get_distribution_details.interface_txn_id,
                         piv_error_type          => 'VAL_ERR',
                         piv_error_code          => 'ETN_FA_INVALID_EMPLOYEE',
                         piv_error_message       => 'Error : Exception occured while fetching employee  : ' ||
                                                    SUBSTR(SQLERRM, 1, 240));
              lc_dist_error_flag := 'Y';
          END;
        END IF;

        IF rec_get_distribution_details.leg_units_assigned IS NULL THEN
          log_errors(pin_interface_txn_id    => NULL,
                     piv_source_table        => 'XXFA_CORP_ASSET_STG',
                     piv_source_column_name  => 'leg_units_assigned',
                     piv_source_column_value => rec_get_distribution_details.leg_units_assigned,
                     piv_source_keyname1     => 'interface_txn_id',
                     piv_source_keyvalue1    => rec_get_distribution_details.interface_txn_id,
                     piv_error_type          => 'VAL_ERR',
                     piv_error_code          => 'ETN_FA_MANDATORY_COLUMN',
                     piv_error_message       => 'Error : LEG_UNITS_ASSIGNED cannot be NULL');
          lc_dist_error_flag := 'Y';
        END IF;

        ---validate_accounts   --1.1
        /* validate_accounts (rec_get_distribution_details.interface_txn_id,
                           rec_get_distribution_details.leg_cc_segment1,
                           rec_get_distribution_details.leg_cc_segment2,
                           rec_get_distribution_details.leg_cc_segment3,
                           rec_get_distribution_details.leg_cc_segment4,
                           rec_get_distribution_details.leg_cc_segment5,
                           rec_get_distribution_details.leg_cc_segment6,
                           rec_get_distribution_details.leg_cc_segment7,
                           x_out_acc_rec,
                           x_ccid
                          );

        IF x_ccid IS NULL
        THEN
           lc_dist_error_flag := 'Y';
        END IF; */
        IF lc_dist_error_flag = 'Y' OR
           rec_get_distribution_details.process_flag = 'E' THEN
          lc_master_dist_flag := 'Y';
        END IF;

        BEGIN
          UPDATE xxfa_corp_asset_stg
             SET assigned_emp_id = ln_employee_id
          --location_id = ln_location_id,
          --cc_segment1 = x_out_acc_rec.segment1,
          -- cc_segment2 = x_out_acc_rec.segment2,
          -- cc_segment3 = x_out_acc_rec.segment3,
          -- cc_segment4 = x_out_acc_rec.segment4,
          -- cc_segment5 = x_out_acc_rec.segment5,
          --  cc_segment6 = x_out_acc_rec.segment6,
          -- cc_segment7 = x_out_acc_rec.segment7,
          -- cc_segment8 = x_out_acc_rec.segment8,
          --  cc_segment9 = x_out_acc_rec.segment9,
          --  cc_segment10 = x_out_acc_rec.segment10,
          --  acct_combination_id = x_ccid
           WHERE interface_txn_id =
                 rec_get_distribution_details.interface_txn_id;
        EXCEPTION
          WHEN OTHERS THEN
            log_errors(pin_interface_txn_id    => NULL,
                       piv_source_table        => 'XXFA_CORP_ASSET_STG',
                       piv_source_column_name  => NULL,
                       piv_source_column_value => NULL,
                       piv_source_keyname1     => 'interface_txn_id',
                       piv_source_keyvalue1    => rec_get_distribution_details.interface_txn_id,
                       piv_error_type          => 'VAL_ERR',
                       piv_error_code          => 'ETN_FA_INVALID_PROC',
                       piv_error_message       => 'Error : Exception occurred while updating distribution details  : ' ||
                                                  SUBSTR(SQLERRM, 1, 240));
            lc_dist_error_flag := 'Y';
        END;
      END LOOP;

      IF lc_master_dist_flag = 'Y' THEN
        lc_error_flag := 'Y';
      END IF;

      --assigning DFF segments
      IF (rec_cur_val_child.leg_adtn_context = 'France' OR
         rec_cur_val_child.leg_adtn_context = 'Germany' OR
         rec_cur_val_child.leg_adtn_context = 'Holland' OR
         rec_cur_val_child.leg_adtn_context = 'Italy' OR
         rec_cur_val_child.leg_adtn_context = 'Mexico' OR
         rec_cur_val_child.leg_adtn_context = 'Monaco' OR
         rec_cur_val_child.leg_adtn_context = 'Spain' OR
         rec_cur_val_child.leg_adtn_context = 'United Kingdom') AND
         rec_cur_val_child.leg_source_system = g_source_issc THEN
        lv_attribute1 := rec_cur_val_child.leg_adtn_attribute1;
      END IF;

      IF rec_cur_val_child.leg_source_system = g_source_issc THEN
        lv_attribute3  := rec_cur_val_child.leg_adtn_attribute26;
        lv_attribute4  := rec_cur_val_child.leg_adtn_attribute27;
        lv_attribute17 := rec_cur_val_child.leg_adtn_attribute28;
        lv_attribute16 := rec_cur_val_child.leg_adtn_attribute16;
      ELSIF rec_cur_val_child.leg_source_system = g_source_fsc THEN
        lv_attribute3  := rec_cur_val_child.leg_adtn_attribute3;
        lv_attribute4  := rec_cur_val_child.leg_adtn_attribute4;
        lv_attribute17 := rec_cur_val_child.leg_adtn_attribute10;
        lv_attribute16 := rec_cur_val_child.leg_adtn_attribute15;
        lv_attribute7  := rec_cur_val_child.leg_adtn_attribute7;
        lv_attribute8  := rec_cur_val_child.leg_adtn_attribute8;
        lv_attribute5  := rec_cur_val_child.leg_adtn_attribute12;
        lv_attribute29 := rec_cur_val_child.leg_adtn_attribute29;
        lv_attribute6  := rec_cur_val_child.leg_adtn_attribute13;
        lv_attribute26 := rec_cur_val_child.leg_adtn_attribute26;
        lv_attribute27 := rec_cur_val_child.leg_adtn_attribute27;
        lv_attribute18 := rec_cur_val_child.leg_adtn_attribute14;
        lv_attribute9  := rec_cur_val_child.leg_adtn_attribute9;
        lv_attribute22 := rec_cur_val_child.leg_adtn_attribute22;
        lv_attribute23 := rec_cur_val_child.leg_adtn_attribute23;
      END IF;

      IF rec_cur_val_child.leg_source_system = g_source_issc THEN
        IF rec_cur_val_child.leg_adtn_context = 'Taiwan' THEN
          lv_attribute10 := rec_cur_val_child.leg_adtn_attribute10;
          lv_attribute11 := rec_cur_val_child.leg_adtn_attribute11;
          lv_attribute12 := rec_cur_val_child.leg_adtn_attribute12;
          lv_attribute13 := rec_cur_val_child.leg_adtn_attribute13;
          lv_attribute14 := rec_cur_val_child.leg_adtn_attribute14;
          lv_attribute15 := rec_cur_val_child.leg_adtn_attribute15;
        ELSIF rec_cur_val_child.leg_adtn_context = 'Poland' THEN
          lv_attribute30 := rec_cur_val_child.leg_adtn_attribute30;
          lv_attribute2  := rec_cur_val_child.leg_adtn_attribute3;
        END IF;
      END IF;

      lv_adn_cont           := rec_cur_val_child.leg_adtn_context;
      lv_adn_cat_code       := rec_cur_val_child.leg_adtn_attr_category_code;
      lv_adn_gl_attribute1  := rec_cur_val_child.leg_adtn_global_attribute1;
      lv_adn_gl_attribute2  := rec_cur_val_child.leg_adtn_global_attribute2;
      lv_adn_gl_attribute3  := rec_cur_val_child.leg_adtn_global_attribute3;
      lv_adn_gl_attribute4  := rec_cur_val_child.leg_adtn_global_attribute4;
      lv_adn_gl_attribute5  := rec_cur_val_child.leg_adtn_global_attribute5;
      lv_adn_gl_attribute6  := rec_cur_val_child.leg_adtn_global_attribute6;
      lv_adn_gl_attribute7  := rec_cur_val_child.leg_adtn_global_attribute7;
      lv_adn_gl_attribute8  := rec_cur_val_child.leg_adtn_global_attribute8;
      lv_adn_gl_attribute9  := rec_cur_val_child.leg_adtn_global_attribute9;
      lv_adn_gl_attribute10 := rec_cur_val_child.leg_adtn_global_attribute10;
      lv_adn_gl_attribute11 := rec_cur_val_child.leg_adtn_global_attribute11;
      lv_adn_gl_attribute12 := rec_cur_val_child.leg_adtn_global_attribute12;
      lv_adn_gl_attribute13 := rec_cur_val_child.leg_adtn_global_attribute13;
      lv_adn_gl_attribute14 := rec_cur_val_child.leg_adtn_global_attribute14;
      lv_adn_gl_attribute15 := rec_cur_val_child.leg_adtn_global_attribute15;
      lv_adn_gl_attribute16 := rec_cur_val_child.leg_adtn_global_attribute16;
      lv_adn_gl_attribute17 := rec_cur_val_child.leg_adtn_global_attribute17;
      lv_adn_gl_attribute18 := rec_cur_val_child.leg_adtn_global_attribute18;
      lv_adn_gl_attribute19 := rec_cur_val_child.leg_adtn_global_attribute19;
      lv_adn_gl_attribute20 := rec_cur_val_child.leg_adtn_global_attribute20;
      lv_adn_gl_cat         := rec_cur_val_child.leg_adtn_global_attr_category;
      lv_bk_gl_attribute1   := rec_cur_val_child.leg_books_global_attribute1;
      lv_bk_gl_attribute2   := rec_cur_val_child.leg_books_global_attribute2;
      lv_bk_gl_attribute3   := rec_cur_val_child.leg_books_global_attribute3;
      lv_bk_gl_attribute4   := rec_cur_val_child.leg_books_global_attribute4;
      lv_bk_gl_attribute5   := rec_cur_val_child.leg_books_global_attribute5;
      lv_bk_gl_attribute6   := rec_cur_val_child.leg_books_global_attribute6;
      lv_bk_gl_attribute7   := rec_cur_val_child.leg_books_global_attribute7;
      lv_bk_gl_attribute8   := rec_cur_val_child.leg_books_global_attribute8;
      lv_bk_gl_attribute9   := rec_cur_val_child.leg_books_global_attribute9;
      lv_bk_gl_attribute10  := rec_cur_val_child.leg_books_global_attribute10;
      lv_bk_gl_attribute11  := rec_cur_val_child.leg_books_global_attribute11;
      lv_bk_gl_attribute12  := rec_cur_val_child.leg_books_global_attribute12;
      lv_bk_gl_attribute13  := rec_cur_val_child.leg_books_global_attribute13;
      lv_bk_gl_attribute14  := rec_cur_val_child.leg_books_global_attribute14;
      lv_bk_gl_attribute15  := rec_cur_val_child.leg_books_global_attribute15;
      lv_bk_gl_attribute16  := rec_cur_val_child.leg_books_global_attribute16;
      lv_bk_gl_attribute17  := rec_cur_val_child.leg_books_global_attribute17;
      lv_bk_gl_attribute18  := rec_cur_val_child.leg_books_global_attribute18;
      lv_bk_gl_attribute19  := rec_cur_val_child.leg_books_global_attribute19;
      lv_bk_gl_attribute20  := rec_cur_val_child.leg_books_global_attribute20;
      lv_bk_gl_att_cat      := rec_cur_val_child.leg_books_global_attr_category;

      IF lc_error_flag = 'Y' OR rec_cur_val_child.process_flag = 'E' THEN
        g_ret_code := 1;

        BEGIN
          UPDATE xxfa_corp_asset_stg
             SET process_flag                   = 'E',
                 ERROR_TYPE                     = 'VAL_ERR',
                 asset_category_id              = ln_category_id,
                 asset_cat_segment1             = lc_cat_segment1,
                 asset_cat_segment2             = lc_cat_segment2,
                 last_updated_date              = SYSDATE,
                 request_id                     = g_request_id,
                 last_updated_by                = g_last_updated_by,
                 last_update_login              = g_last_update_login,
                 adtn_attribute1                = lv_attribute1,
                 adtn_attribute2                = lv_attribute2,
                 adtn_attribute3                = lv_attribute3,
                 adtn_attribute4                = lv_attribute4,
                 adtn_attribute5                = lv_attribute5,
                 adtn_attribute6                = lv_attribute6,
                 adtn_attribute7                = lv_attribute7,
                 adtn_attribute8                = lv_attribute8,
                 adtn_attribute9                = lv_attribute9,
                 adtn_attribute10               = lv_attribute10,
                 adtn_attribute11               = lv_attribute11,
                 adtn_attribute12               = lv_attribute12,
                 adtn_attribute13               = lv_attribute13,
                 adtn_attribute14               = lv_attribute14,
                 adtn_attribute15               = lv_attribute15,
                 adtn_attribute16               = lv_attribute16,
                 adtn_attribute17               = lv_attribute17,
                 adtn_attribute18               = lv_attribute18,
                 adtn_attribute19               = lv_attribute19,
                 adtn_attribute20               = lv_attribute20,
                 adtn_attribute21               = lv_attribute21,
                 adtn_attribute22               = lv_attribute22,
                 adtn_attribute23               = lv_attribute23,
                 adtn_attribute24               = lv_attribute24,
                 adtn_attribute25               = lv_attribute25,
                 adtn_attribute26               = lv_attribute26,
                 adtn_attribute27               = lv_attribute27,
                 adtn_attribute28               = lv_attribute28,
                 adtn_attribute29               = lv_attribute29,
                 adtn_attribute30               = lv_attribute30,
                 adtn_attribute_category_code   = lv_adn_cat_code,
                 adtn_context                   = lv_adn_cont,
                 adtn_global_attribute1         = lv_adn_gl_attribute1,
                 adtn_global_attribute2         = lv_adn_gl_attribute2,
                 adtn_global_attribute3         = lv_adn_gl_attribute3,
                 adtn_global_attribute4         = lv_adn_gl_attribute4,
                 adtn_global_attribute5         = lv_adn_gl_attribute5,
                 adtn_global_attribute6         = lv_adn_gl_attribute6,
                 adtn_global_attribute7         = lv_adn_gl_attribute7,
                 adtn_global_attribute8         = lv_adn_gl_attribute8,
                 adtn_global_attribute9         = lv_adn_gl_attribute9,
                 adtn_global_attribute10        = lv_adn_gl_attribute10,
                 adtn_global_attribute11        = lv_adn_gl_attribute11,
                 adtn_global_attribute12        = lv_adn_gl_attribute12,
                 adtn_global_attribute13        = lv_adn_gl_attribute13,
                 adtn_global_attribute14        = lv_adn_gl_attribute14,
                 adtn_global_attribute15        = lv_adn_gl_attribute15,
                 adtn_global_attribute16        = lv_adn_gl_attribute16,
                 adtn_global_attribute17        = lv_adn_gl_attribute17,
                 adtn_global_attribute18        = lv_adn_gl_attribute18,
                 adtn_global_attribute19        = lv_adn_gl_attribute19,
                 adtn_global_attribute20        = lv_adn_gl_attribute20,
                 adtn_global_attribute_category = lv_adn_gl_cat,
                 books_global_attribute1        = lv_bk_gl_attribute1,
                 books_global_attribute2        = lv_bk_gl_attribute2,
                 books_global_attribute3        = lv_bk_gl_attribute3,
                 books_global_attribute4        = lv_bk_gl_attribute4,
                 books_global_attribute5        = lv_bk_gl_attribute5,
                 books_global_attribute6        = lv_bk_gl_attribute6,
                 books_global_attribute7        = lv_bk_gl_attribute7,
                 books_global_attribute8        = lv_bk_gl_attribute8,
                 books_global_attribute9        = lv_bk_gl_attribute9,
                 books_global_attribute10       = lv_bk_gl_attribute10,
                 books_global_attribute11       = lv_bk_gl_attribute11,
                 books_global_attribute12       = lv_bk_gl_attribute12,
                 books_global_attribute13       = lv_bk_gl_attribute13,
                 books_global_attribute14       = lv_bk_gl_attribute14,
                 books_global_attribute15       = lv_bk_gl_attribute15,
                 books_global_attribute16       = lv_bk_gl_attribute16,
                 books_global_attribute17       = lv_bk_gl_attribute17,
                 books_global_attribute18       = lv_bk_gl_attribute18,
                 books_global_attribute19       = lv_bk_gl_attribute19,
                 books_global_attribute20       = lv_bk_gl_attribute20,
                 books_global_attr_category     = lv_bk_gl_att_cat
           WHERE leg_asset_number = rec_cur_val_child.leg_asset_number
             AND leg_book_type_code = rec_cur_val_child.leg_book_type_code
             AND process_flag = 'N'
             AND batch_id = g_batch_id
             AND run_sequence_id = g_new_run_seq_id;
        EXCEPTION
          WHEN OTHERS THEN
            log_errors(pin_interface_txn_id    => NULL,
                       piv_source_table        => 'XXFA_CORP_ASSET_STG',
                       piv_source_column_name  => NULL,
                       piv_source_column_value => NULL,
                       piv_source_keyname1     => 'leg_asset_number',
                       piv_source_keyvalue1    => rec_cur_val_child.leg_asset_number,
                       piv_error_type          => 'VAL_ERR',
                       piv_error_code          => 'ETN_FA_INVALID_PROC',
                       piv_error_message       => 'Error : Exception occured while updating child records of XXFA_CORP_ASSET_STG for errors  : ' ||
                                                  SUBSTR(SQLERRM, 1, 240));
        END;
      ELSE
        BEGIN
          UPDATE xxfa_corp_asset_stg
             SET process_flag                   = 'V',
                 asset_category_id              = ln_category_id,
                 asset_cat_segment1             = lc_cat_segment1,
                 asset_cat_segment2             = lc_cat_segment2,
                 last_updated_date              = SYSDATE,
                 request_id                     = g_request_id,
                 last_updated_by                = g_last_updated_by,
                 last_update_login              = g_last_update_login,
                 adtn_attribute1                = lv_attribute1,
                 adtn_attribute2                = lv_attribute2,
                 adtn_attribute3                = lv_attribute3,
                 adtn_attribute4                = lv_attribute4,
                 adtn_attribute5                = lv_attribute5,
                 adtn_attribute6                = lv_attribute6,
                 adtn_attribute7                = lv_attribute7,
                 adtn_attribute8                = lv_attribute8,
                 adtn_attribute9                = lv_attribute9,
                 adtn_attribute10               = lv_attribute10,
                 adtn_attribute11               = lv_attribute11,
                 adtn_attribute12               = lv_attribute12,
                 adtn_attribute13               = lv_attribute13,
                 adtn_attribute14               = lv_attribute14,
                 adtn_attribute15               = lv_attribute15,
                 adtn_attribute16               = lv_attribute16,
                 adtn_attribute17               = lv_attribute17,
                 adtn_attribute18               = lv_attribute18,
                 adtn_attribute19               = lv_attribute19,
                 adtn_attribute20               = lv_attribute20,
                 adtn_attribute21               = lv_attribute21,
                 adtn_attribute22               = lv_attribute22,
                 adtn_attribute23               = lv_attribute23,
                 adtn_attribute24               = lv_attribute24,
                 adtn_attribute25               = lv_attribute25,
                 adtn_attribute26               = lv_attribute26,
                 adtn_attribute27               = lv_attribute27,
                 adtn_attribute28               = lv_attribute28,
                 adtn_attribute29               = lv_attribute29,
                 adtn_attribute30               = lv_attribute30,
                 adtn_attribute_category_code   = lv_adn_cat_code,
                 adtn_context                   = lv_adn_cont,
                 adtn_global_attribute1         = lv_adn_gl_attribute1,
                 adtn_global_attribute2         = lv_adn_gl_attribute2,
                 adtn_global_attribute3         = lv_adn_gl_attribute3,
                 adtn_global_attribute4         = lv_adn_gl_attribute4,
                 adtn_global_attribute5         = lv_adn_gl_attribute5,
                 adtn_global_attribute6         = lv_adn_gl_attribute6,
                 adtn_global_attribute7         = lv_adn_gl_attribute7,
                 adtn_global_attribute8         = lv_adn_gl_attribute8,
                 adtn_global_attribute9         = lv_adn_gl_attribute9,
                 adtn_global_attribute10        = lv_adn_gl_attribute10,
                 adtn_global_attribute11        = lv_adn_gl_attribute11,
                 adtn_global_attribute12        = lv_adn_gl_attribute12,
                 adtn_global_attribute13        = lv_adn_gl_attribute13,
                 adtn_global_attribute14        = lv_adn_gl_attribute14,
                 adtn_global_attribute15        = lv_adn_gl_attribute15,
                 adtn_global_attribute16        = lv_adn_gl_attribute16,
                 adtn_global_attribute17        = lv_adn_gl_attribute17,
                 adtn_global_attribute18        = lv_adn_gl_attribute18,
                 adtn_global_attribute19        = lv_adn_gl_attribute19,
                 adtn_global_attribute20        = lv_adn_gl_attribute20,
                 adtn_global_attribute_category = lv_adn_gl_cat,
                 books_global_attribute1        = lv_bk_gl_attribute1,
                 books_global_attribute2        = lv_bk_gl_attribute2,
                 books_global_attribute3        = lv_bk_gl_attribute3,
                 books_global_attribute4        = lv_bk_gl_attribute4,
                 books_global_attribute5        = lv_bk_gl_attribute5,
                 books_global_attribute6        = lv_bk_gl_attribute6,
                 books_global_attribute7        = lv_bk_gl_attribute7,
                 books_global_attribute8        = lv_bk_gl_attribute8,
                 books_global_attribute9        = lv_bk_gl_attribute9,
                 books_global_attribute10       = lv_bk_gl_attribute10,
                 books_global_attribute11       = lv_bk_gl_attribute11,
                 books_global_attribute12       = lv_bk_gl_attribute12,
                 books_global_attribute13       = lv_bk_gl_attribute13,
                 books_global_attribute14       = lv_bk_gl_attribute14,
                 books_global_attribute15       = lv_bk_gl_attribute15,
                 books_global_attribute16       = lv_bk_gl_attribute16,
                 books_global_attribute17       = lv_bk_gl_attribute17,
                 books_global_attribute18       = lv_bk_gl_attribute18,
                 books_global_attribute19       = lv_bk_gl_attribute19,
                 books_global_attribute20       = lv_bk_gl_attribute20,
                 books_global_attr_category     = lv_bk_gl_att_cat
           WHERE leg_asset_number = rec_cur_val_child.leg_asset_number
             AND leg_book_type_code = rec_cur_val_child.leg_book_type_code
             AND process_flag = 'N'
             AND batch_id = g_batch_id
             AND run_sequence_id = g_new_run_seq_id;
        EXCEPTION
          WHEN OTHERS THEN
            log_errors(pin_interface_txn_id    => NULL,
                       piv_source_table        => 'XXFA_CORP_ASSET_STG',
                       piv_source_column_name  => NULL,
                       piv_source_column_value => NULL,
                       piv_source_keyname1     => 'leg_asset_number',
                       piv_source_keyvalue1    => rec_cur_val_child.leg_asset_number,
                       piv_error_type          => 'VAL_ERR',
                       piv_error_code          => 'ETN_FA_INVALID_PROC',
                       piv_error_message       => 'Error : Exception occured while updating child records of XXFA_CORP_ASSET_STG for Validation  : ' ||
                                                  SUBSTR(SQLERRM, 1, 240));
            lc_error_flag := 'Y';
        END;
      END IF;

      IF l_count >= 100 THEN
        l_count := 0;
        xxetn_debug_pkg.add_debug('
                        Performing Batch Commit');
        COMMIT;
      ELSE
        l_count := l_count + 1;
      END IF;
    END LOOP;

    xxetn_debug_pkg.add_debug(piv_debug_msg => 'Validation process for Child records with Corporate Book ends : ');
    COMMIT;
  EXCEPTION
    WHEN OTHERS THEN
      g_ret_code := 2;
      print_log_message('Error : Exception occurred while validation  : ' ||
                        SUBSTR(SQLERRM, 1, 240));
      print_log_message('Error : Backtrace in validation for corporate : ' ||
                        DBMS_UTILITY.format_error_backtrace);
  END validate_corporate;

  --
  -- ========================
  -- Procedure: VALIDATE_TAX
  -- =====================================================================================
  --   This procedure is used to validate the assets with tax books
  -- =====================================================================================
  PROCEDURE validate_tax IS
    l_leg_asset_number   xxfa_tax_asset_stg.leg_asset_number%TYPE;
    l_leg_book_type_code xxfa_tax_asset_stg.leg_book_type_code%TYPE;
    l_leg_adjusted_cost  xxfa_tax_asset_stg.leg_adjusted_cost%TYPE;
    l_leg_cost           xxfa_tax_asset_stg.leg_cost%TYPE;
    l_leg_original_cost  xxfa_tax_asset_stg.leg_original_cost%TYPE;
    l_leg_ytd_deprn      xxfa_tax_asset_stg.leg_ytd_deprn%TYPE;
    l_leg_deprn_reserve  xxfa_tax_asset_stg.leg_deprn_reserve%TYPE;
    ln_CORP_category   varchar2(400);

    CURSOR cur_val_tax IS
      SELECT DISTINCT leg_source_system,
                      leg_source_asset_number,
                      leg_asset_id,
                      asset_id,
                      leg_asset_number,
                      leg_current_units,
                      leg_asset_type,
                      leg_tag_number,
                      leg_asset_cat_segment1,
                      leg_asset_cat_segment2,
                      asset_cat_segment1,
                      asset_cat_segment2,
                      asset_category_id,
                      leg_parent_asset_number,
                      parent_asset_id,
                      leg_manufacturer_name,
                      leg_serial_number,
                      leg_model_number,
                      leg_property_type_code,
                      leg_property_1245_1250_code,
                      leg_in_use_flag,
                      leg_owned_leased,
                      leg_new_used,
                      leg_unit_adjustment_flag,
                      leg_add_cost_je_flag,
                      leg_adtn_attribute1,
                      leg_adtn_attribute2,
                      leg_adtn_attribute3,
                      leg_adtn_attribute4,
                      leg_adtn_attribute5,
                      leg_adtn_attribute6,
                      leg_adtn_attribute7,
                      leg_adtn_attribute8,
                      leg_adtn_attribute9,
                      leg_adtn_attribute10,
                      leg_adtn_attribute11,
                      leg_adtn_attribute12,
                      leg_adtn_attribute13,
                      leg_adtn_attribute14,
                      leg_adtn_attribute15,
                      leg_adtn_attribute16,
                      leg_adtn_attribute17,
                      leg_adtn_attribute18,
                      leg_adtn_attribute19,
                      leg_adtn_attribute20,
                      leg_adtn_attribute21,
                      leg_adtn_attribute22,
                      leg_adtn_attribute23,
                      leg_adtn_attribute24,
                      leg_adtn_attribute25,
                      leg_adtn_attribute26,
                      leg_adtn_attribute27,
                      leg_adtn_attribute28,
                      leg_adtn_attribute29,
                      leg_adtn_attribute30,
                      leg_adtn_attr_category_code,
                      leg_adtn_context,
                      adtn_attribute1,
                      adtn_attribute2,
                      adtn_attribute3,
                      adtn_attribute4,
                      adtn_attribute5,
                      adtn_attribute6,
                      adtn_attribute7,
                      adtn_attribute8,
                      adtn_attribute9,
                      adtn_attribute10,
                      adtn_attribute11,
                      adtn_attribute12,
                      adtn_attribute13,
                      adtn_attribute14,
                      adtn_attribute15,
                      adtn_attribute16,
                      adtn_attribute17,
                      adtn_attribute18,
                      adtn_attribute19,
                      adtn_attribute20,
                      adtn_attribute21,
                      adtn_attribute22,
                      adtn_attribute23,
                      adtn_attribute24,
                      adtn_attribute25,
                      adtn_attribute26,
                      adtn_attribute27,
                      adtn_attribute28,
                      adtn_attribute29,
                      adtn_attribute30,
                      adtn_attribute_category_code,
                      adtn_context,
                      leg_inventorial,
                      leg_commitment,
                      leg_investment_law,
                      leg_adtn_global_attribute1,
                      leg_adtn_global_attribute2,
                      leg_adtn_global_attribute3,
                      leg_adtn_global_attribute4,
                      leg_adtn_global_attribute5,
                      leg_adtn_global_attribute6,
                      leg_adtn_global_attribute7,
                      leg_adtn_global_attribute8,
                      leg_adtn_global_attribute9,
                      leg_adtn_global_attribute10,
                      leg_adtn_global_attribute11,
                      leg_adtn_global_attribute12,
                      leg_adtn_global_attribute13,
                      leg_adtn_global_attribute14,
                      leg_adtn_global_attribute15,
                      leg_adtn_global_attribute16,
                      leg_adtn_global_attribute17,
                      leg_adtn_global_attribute18,
                      leg_adtn_global_attribute19,
                      leg_adtn_global_attribute20,
                      leg_adtn_global_attr_category,
                      adtn_global_attribute1,
                      adtn_global_attribute2,
                      adtn_global_attribute3,
                      adtn_global_attribute4,
                      adtn_global_attribute5,
                      adtn_global_attribute6,
                      adtn_global_attribute7,
                      adtn_global_attribute8,
                      adtn_global_attribute9,
                      adtn_global_attribute10,
                      adtn_global_attribute11,
                      adtn_global_attribute12,
                      adtn_global_attribute13,
                      adtn_global_attribute14,
                      adtn_global_attribute15,
                      adtn_global_attribute16,
                      adtn_global_attribute17,
                      adtn_global_attribute18,
                      adtn_global_attribute19,
                      adtn_global_attribute20,
                      adtn_global_attribute_category,
                      leg_book_type_code,
                      book_type_code,
                      leg_date_placed_in_service,
                      leg_deprn_start_date,
                      leg_deprn_method_code,
                      leg_life_in_months,
                      leg_rate_adjustment_factor,
                      leg_adjusted_cost,
                      leg_cost,
                      leg_original_cost,
                      leg_salvage_value,
                      leg_prorate_convention_code,
                      leg_prorate_date,
                      leg_cost_change_flag,
                      leg_adjustment_required_status,
                      leg_capitalize_flag,
                      leg_retirement_pending_flag,
                      leg_depreciate_flag,
                      leg_basic_rate,
                      leg_adjusted_rate,
                      leg_bonus_rule,
                      leg_ceiling_name,
                      leg_recoverable_cost,
                      leg_cap_period_name,
                      period_counter_capitalized,
                      leg_dep_period_name,
                      period_counter_fully_reserved,
                      leg_unrevalued_cost,
                      leg_annual_deprn_rounding_flag,
                      leg_percent_salvage_value,
                      leg_allowed_deprn_limit,
                      leg_allowed_deprn_limit_amount,
                      leg_salvage_type,
                      leg_deprn_limit_type,
                      leg_period_counter,
                      leg_deprn_source_code,
                      leg_deprn_run_date,
                      leg_deprn_amount,
                      leg_ytd_deprn,
                      leg_deprn_reserve,
                      leg_description,
                      leg_transaction_type_code,
                      leg_transaction_date_entered,
                      leg_transaction_subtype,
                      leg_amortization_start_date,
                      leg_books_global_attribute1,
                      leg_books_global_attribute2,
                      leg_books_global_attribute3,
                      leg_books_global_attribute4,
                      leg_books_global_attribute5,
                      leg_books_global_attribute6,
                      leg_books_global_attribute7,
                      leg_books_global_attribute8,
                      leg_books_global_attribute9,
                      leg_books_global_attribute10,
                      leg_books_global_attribute11,
                      leg_books_global_attribute12,
                      leg_books_global_attribute13,
                      leg_books_global_attribute14,
                      leg_books_global_attribute15,
                      leg_books_global_attribute16,
                      leg_books_global_attribute17,
                      leg_books_global_attribute18,
                      leg_books_global_attribute19,
                      leg_books_global_attribute20,
                      books_global_attribute1,
                      books_global_attribute2,
                      books_global_attribute3,
                      books_global_attribute4,
                      books_global_attribute5,
                      books_global_attribute6,
                      books_global_attribute7,
                      books_global_attribute8,
                      books_global_attribute9,
                      books_global_attribute10,
                      books_global_attribute11,
                      books_global_attribute12,
                      books_global_attribute13,
                      books_global_attribute14,
                      books_global_attribute15,
                      books_global_attribute16,
                      books_global_attribute17,
                      books_global_attribute18,
                      books_global_attribute19,
                      books_global_attribute20,
                      books_global_attr_category,
                      leg_books_global_attr_category,
                      batch_id,
                      process_flag,
                      run_sequence_id
        FROM xxfa_tax_asset_stg
       WHERE process_flag IN ('N', 'E') ---1.1
         AND batch_id = g_batch_id
         AND run_sequence_id = g_new_run_seq_id;

    CURSOR cur_property IS
      SELECT DISTINCT leg_property_type_code
        FROM xxfa_tax_asset_stg
       WHERE batch_id = g_batch_id
         AND run_sequence_id = g_new_run_seq_id;

    CURSOR cur_p1245 IS
      SELECT DISTINCT leg_property_1245_1250_code
        FROM xxfa_tax_asset_stg
       WHERE batch_id = g_batch_id
         AND run_sequence_id = g_new_run_seq_id;

    CURSOR cur_own IS
      SELECT DISTINCT leg_owned_leased
        FROM xxfa_tax_asset_stg
       WHERE batch_id = g_batch_id
         AND run_sequence_id = g_new_run_seq_id;

    CURSOR cur_new IS
      SELECT DISTINCT leg_new_used
        FROM xxfa_tax_asset_stg
       WHERE batch_id = g_batch_id
         AND run_sequence_id = g_new_run_seq_id;

    CURSOR cur_book IS
      SELECT DISTINCT /*leg_book_type_code*/ book_type_code ------v1.3 11th May,2015
        FROM xxfa_tax_asset_stg
       WHERE batch_id = g_batch_id
         AND run_sequence_id = g_new_run_seq_id;

    /* CURSOR cur_deprn
    IS
       SELECT DISTINCT leg_deprn_method_code,
                       leg_life_in_months
                  FROM xxfa_tax_asset_stg
                 WHERE batch_id = g_batch_id
                   AND run_sequence_id = g_new_run_seq_id;*/

    CURSOR cur_deprn IS -----change as per the new logic based on baisc rate and adjusted rate
      SELECT DISTINCT leg_deprn_method_code,
                      leg_basic_rate,
                      leg_adjusted_rate, ---added leg_basic_rate and leg_adjusted_rate
                      leg_life_in_months
        FROM xxfa_tax_asset_stg
       WHERE batch_id = g_batch_id
         AND run_sequence_id = g_new_run_seq_id;

    CURSOR cur_pror IS
      SELECT DISTINCT leg_prorate_convention_code
        FROM xxfa_tax_asset_stg
       WHERE batch_id = g_batch_id
         AND run_sequence_id = g_new_run_seq_id;

    CURSOR cur_ceiling IS
      SELECT DISTINCT leg_ceiling_name
        FROM xxfa_tax_asset_stg
       WHERE batch_id = g_batch_id
         AND run_sequence_id = g_new_run_seq_id;

    CURSOR c_der_book_info --    V1.2
    IS
      SELECT distinct leg_cc_segment1 || ' ' || leg_book_type_code book_type
        FROM xxfa_tax_asset_stg
       WHERE batch_id = g_batch_id
         AND run_sequence_id = g_new_run_seq_id;

    ln_property_code       NUMBER;
    lc_error_flag          VARCHAR2(1);
    ln_category_id         NUMBER;
    lc_cat_segment1        VARCHAR2(240);
    lc_cat_segment2        VARCHAR2(240);
    lc_cat_flag            VARCHAR2(1);
    ln_prop_1245_1250_code NUMBER;
    ln_own_lease           NUMBER;
    ln_new_use             NUMBER;
    ln_life_in_months      NUMBER;
    ln_prorate_count       NUMBER;
    ln_ceiling_count       NUMBER;
    ln_bonus_count         NUMBER;
    ln_book_code           NUMBER;
    ln_parent_count        NUMBER;
    ln_count_tax           NUMBER;
    ln_count_tag           NUMBER;
    ln_asset_key           NUMBER;
    lc_cat_msg             VARCHAR2(1000);
    lv_attribute1          xxfa_corp_asset_stg.leg_adtn_attribute1%TYPE;
    lv_attribute2          xxfa_corp_asset_stg.leg_adtn_attribute2%TYPE;
    lv_attribute3          xxfa_corp_asset_stg.leg_adtn_attribute3%TYPE;
    lv_attribute4          xxfa_corp_asset_stg.leg_adtn_attribute4%TYPE;
    lv_attribute5          xxfa_corp_asset_stg.leg_adtn_attribute5%TYPE;
    lv_attribute6          xxfa_corp_asset_stg.leg_adtn_attribute6%TYPE;
    lv_attribute7          xxfa_corp_asset_stg.leg_adtn_attribute7%TYPE;
    lv_attribute8          xxfa_corp_asset_stg.leg_adtn_attribute8%TYPE;
    lv_attribute9          xxfa_corp_asset_stg.leg_adtn_attribute9%TYPE;
    lv_attribute10         xxfa_corp_asset_stg.leg_adtn_attribute10%TYPE;
    lv_attribute11         xxfa_corp_asset_stg.leg_adtn_attribute11%TYPE;
    lv_attribute12         xxfa_corp_asset_stg.leg_adtn_attribute12%TYPE;
    lv_attribute13         xxfa_corp_asset_stg.leg_adtn_attribute13%TYPE;
    lv_attribute14         xxfa_corp_asset_stg.leg_adtn_attribute14%TYPE;
    lv_attribute15         xxfa_corp_asset_stg.leg_adtn_attribute15%TYPE;
    lv_attribute16         xxfa_corp_asset_stg.leg_adtn_attribute16%TYPE;
    lv_attribute17         xxfa_corp_asset_stg.leg_adtn_attribute17%TYPE;
    lv_attribute18         xxfa_corp_asset_stg.leg_adtn_attribute18%TYPE;
    lv_attribute19         xxfa_corp_asset_stg.leg_adtn_attribute19%TYPE;
    lv_attribute20         xxfa_corp_asset_stg.leg_adtn_attribute20%TYPE;
    lv_attribute21         xxfa_corp_asset_stg.leg_adtn_attribute21%TYPE;
    lv_attribute22         xxfa_corp_asset_stg.leg_adtn_attribute22%TYPE;
    lv_attribute23         xxfa_corp_asset_stg.leg_adtn_attribute23%TYPE;
    lv_attribute24         xxfa_corp_asset_stg.leg_adtn_attribute24%TYPE;
    lv_attribute25         xxfa_corp_asset_stg.leg_adtn_attribute25%TYPE;
    lv_attribute26         xxfa_corp_asset_stg.leg_adtn_attribute26%TYPE;
    lv_attribute27         xxfa_corp_asset_stg.leg_adtn_attribute27%TYPE;
    lv_attribute28         xxfa_corp_asset_stg.leg_adtn_attribute28%TYPE;
    lv_attribute29         xxfa_corp_asset_stg.leg_adtn_attribute29%TYPE;
    lv_attribute30         xxfa_corp_asset_stg.leg_adtn_attribute30%TYPE;
    lv_adn_cat_code        xxfa_corp_asset_stg.leg_adtn_attr_category_code%TYPE;
    lv_adn_cont            xxfa_corp_asset_stg.leg_adtn_context%TYPE;
    lv_bk_gl_attribute1    xxfa_corp_asset_stg.leg_books_global_attribute1%TYPE;
    lv_bk_gl_attribute2    xxfa_corp_asset_stg.leg_books_global_attribute2%TYPE;
    lv_bk_gl_attribute3    xxfa_corp_asset_stg.leg_books_global_attribute3%TYPE;
    lv_bk_gl_attribute4    xxfa_corp_asset_stg.leg_books_global_attribute4%TYPE;
    lv_bk_gl_attribute5    xxfa_corp_asset_stg.leg_books_global_attribute5%TYPE;
    lv_bk_gl_attribute6    xxfa_corp_asset_stg.leg_books_global_attribute6%TYPE;
    lv_bk_gl_attribute7    xxfa_corp_asset_stg.leg_books_global_attribute7%TYPE;
    lv_bk_gl_attribute8    xxfa_corp_asset_stg.leg_books_global_attribute8%TYPE;
    lv_bk_gl_attribute9    xxfa_corp_asset_stg.leg_books_global_attribute9%TYPE;
    lv_bk_gl_attribute10   xxfa_corp_asset_stg.leg_books_global_attribute10%TYPE;
    lv_bk_gl_attribute11   xxfa_corp_asset_stg.leg_books_global_attribute11%TYPE;
    lv_bk_gl_attribute12   xxfa_corp_asset_stg.leg_books_global_attribute12%TYPE;
    lv_bk_gl_attribute13   xxfa_corp_asset_stg.leg_books_global_attribute13%TYPE;
    lv_bk_gl_attribute14   xxfa_corp_asset_stg.leg_books_global_attribute14%TYPE;
    lv_bk_gl_attribute15   xxfa_corp_asset_stg.leg_books_global_attribute15%TYPE;
    lv_bk_gl_attribute16   xxfa_corp_asset_stg.leg_books_global_attribute16%TYPE;
    lv_bk_gl_attribute17   xxfa_corp_asset_stg.leg_books_global_attribute17%TYPE;
    lv_bk_gl_attribute18   xxfa_corp_asset_stg.leg_books_global_attribute18%TYPE;
    lv_bk_gl_attribute19   xxfa_corp_asset_stg.leg_books_global_attribute19%TYPE;
    lv_bk_gl_attribute20   xxfa_corp_asset_stg.leg_books_global_attribute20%TYPE;
    lv_bk_gl_att_cat       xxfa_corp_asset_stg.leg_books_global_attr_category%TYPE;
    lv_adn_gl_attribute1   xxfa_corp_asset_stg.leg_adtn_global_attribute1%TYPE;
    lv_adn_gl_attribute2   xxfa_corp_asset_stg.leg_adtn_global_attribute2%TYPE;
    lv_adn_gl_attribute3   xxfa_corp_asset_stg.leg_adtn_global_attribute3%TYPE;
    lv_adn_gl_attribute4   xxfa_corp_asset_stg.leg_adtn_global_attribute4%TYPE;
    lv_adn_gl_attribute5   xxfa_corp_asset_stg.leg_adtn_global_attribute5%TYPE;
    lv_adn_gl_attribute6   xxfa_corp_asset_stg.leg_adtn_global_attribute6%TYPE;
    lv_adn_gl_attribute7   xxfa_corp_asset_stg.leg_adtn_global_attribute7%TYPE;
    lv_adn_gl_attribute8   xxfa_corp_asset_stg.leg_adtn_global_attribute8%TYPE;
    lv_adn_gl_attribute9   xxfa_corp_asset_stg.leg_adtn_global_attribute9%TYPE;
    lv_adn_gl_attribute10  xxfa_corp_asset_stg.leg_adtn_global_attribute10%TYPE;
    lv_adn_gl_attribute11  xxfa_corp_asset_stg.leg_adtn_global_attribute11%TYPE;
    lv_adn_gl_attribute12  xxfa_corp_asset_stg.leg_adtn_global_attribute12%TYPE;
    lv_adn_gl_attribute13  xxfa_corp_asset_stg.leg_adtn_global_attribute13%TYPE;
    lv_adn_gl_attribute14  xxfa_corp_asset_stg.leg_adtn_global_attribute14%TYPE;
    lv_adn_gl_attribute15  xxfa_corp_asset_stg.leg_adtn_global_attribute15%TYPE;
    lv_adn_gl_attribute16  xxfa_corp_asset_stg.leg_adtn_global_attribute16%TYPE;
    lv_adn_gl_attribute17  xxfa_corp_asset_stg.leg_adtn_global_attribute17%TYPE;
    lv_adn_gl_attribute18  xxfa_corp_asset_stg.leg_adtn_global_attribute18%TYPE;
    lv_adn_gl_attribute19  xxfa_corp_asset_stg.leg_adtn_global_attribute19%TYPE;
    lv_adn_gl_attribute20  xxfa_corp_asset_stg.leg_adtn_global_attribute20%TYPE;
    lv_adn_gl_cat          xxfa_corp_asset_stg.leg_adtn_global_attr_category%TYPE;
    ln_code                NUMBER;
    l_count                NUMBER;
    l_book_type            varchar2(2000);
    l_r12_book             varchar2(60);
    lv_sql_count           NUMBER;
    lv_sql_count1          NUMBER;

  BEGIN
    g_ret_code := NULL;

    FOR cur_property_rec IN cur_property LOOP
      ln_code := NULL;

      IF cur_property_rec.leg_property_type_code IS NOT NULL THEN
        BEGIN
          SELECT 1
            INTO ln_code
            FROM fa_lookups flv
           WHERE flv.lookup_type = 'PROPERTY TYPE'
             AND flv.lookup_code = cur_property_rec.leg_property_type_code
             AND flv.enabled_flag = 'Y'
             AND TRUNC(SYSDATE) BETWEEN
                 NVL(flv.start_date_active, TRUNC(SYSDATE)) AND
                 NVL(flv.end_date_active, TRUNC(SYSDATE));
        EXCEPTION
          WHEN OTHERS THEN
            FOR r_org_ref_err_rec IN (SELECT DISTINCT leg_asset_number,
                                                      leg_book_type_code
                                        FROM xxfa_tax_asset_stg xis
                                       WHERE leg_property_type_code =
                                             cur_property_rec.leg_property_type_code
                                         AND batch_id = g_batch_id
                                         AND run_sequence_id =
                                             g_new_run_seq_id) LOOP
              log_errors(pin_interface_txn_id    => NULL,
                         piv_source_table        => 'XXFA_TAX_ASSET_STG',
                         piv_source_column_name  => 'leg_property_type_code',
                         piv_source_column_value => cur_property_rec.leg_property_type_code,
                         piv_source_keyname1     => 'leg_asset_number',
                         piv_source_keyvalue1    => r_org_ref_err_rec.leg_asset_number,
                         piv_source_keyname2     => 'leg_book_type_code',
                         piv_source_keyvalue2    => r_org_ref_err_rec.leg_book_type_code,
                         piv_error_type          => 'VAL_ERR',
                         piv_error_code          => 'ETN_FA_INVALID_LOOKUP_VAL',
                         piv_error_message       => 'Error : Invalid property_type_code value.');
            END LOOP;

            UPDATE xxfa_tax_asset_stg
               SET process_flag      = 'E',
                   ERROR_TYPE        = 'VAL_ERR',
                   request_id        = g_request_id,
                   last_updated_date = SYSDATE,
                   last_updated_by   = g_last_updated_by,
                   last_update_login = g_last_update_login
             WHERE leg_property_type_code =
                   cur_property_rec.leg_property_type_code
               AND batch_id = g_batch_id
               AND run_sequence_id = g_new_run_seq_id;

            COMMIT;
        END;
      END IF;
    END LOOP;

    FOR cur_own_rec IN cur_own LOOP
      ln_code := NULL;

      IF cur_own_rec.leg_owned_leased IS NOT NULL THEN
        BEGIN
          SELECT 1
            INTO ln_code
            FROM fa_lookups flv
           WHERE flv.lookup_type = 'OWNLEASE'
             AND flv.lookup_code = cur_own_rec.leg_owned_leased
             AND flv.enabled_flag = 'Y'
             AND TRUNC(SYSDATE) BETWEEN
                 NVL(flv.start_date_active, TRUNC(SYSDATE)) AND
                 NVL(flv.end_date_active, TRUNC(SYSDATE));
        EXCEPTION
          WHEN OTHERS THEN
            FOR r_org_ref_err_rec IN (SELECT DISTINCT leg_asset_number,
                                                      leg_book_type_code
                                        FROM xxfa_tax_asset_stg xis
                                       WHERE leg_owned_leased =
                                             cur_own_rec.leg_owned_leased
                                         AND batch_id = g_batch_id
                                         AND run_sequence_id =
                                             g_new_run_seq_id) LOOP
              log_errors(pin_interface_txn_id    => NULL,
                         piv_source_table        => 'XXFA_TAX_ASSET_STG',
                         piv_source_column_name  => 'leg_owned_leased',
                         piv_source_column_value => cur_own_rec.leg_owned_leased,
                         piv_source_keyname1     => 'leg_asset_number',
                         piv_source_keyvalue1    => r_org_ref_err_rec.leg_asset_number,
                         piv_source_keyname2     => 'leg_book_type_code',
                         piv_source_keyvalue2    => r_org_ref_err_rec.leg_book_type_code,
                         piv_error_type          => 'VAL_ERR',
                         piv_error_code          => 'ETN_FA_INVALID_LOOKUP_VAL',
                         piv_error_message       => 'Error : Invalid leg_owned_leased value.');
            END LOOP;

            UPDATE xxfa_tax_asset_stg
               SET process_flag      = 'E',
                   ERROR_TYPE        = 'VAL_ERR',
                   request_id        = g_request_id,
                   last_updated_date = SYSDATE,
                   last_updated_by   = g_last_updated_by,
                   last_update_login = g_last_update_login
             WHERE leg_owned_leased = cur_own_rec.leg_owned_leased
               AND batch_id = g_batch_id
               AND run_sequence_id = g_new_run_seq_id;

            COMMIT;
        END;
      END IF;
    END LOOP;

    FOR cur_p1245_rec IN cur_p1245 LOOP
      ln_code := NULL;

      IF cur_p1245_rec.leg_property_1245_1250_code IS NOT NULL THEN
        BEGIN
          SELECT 1
            INTO ln_code
            FROM fa_lookups flv
           WHERE flv.lookup_type = '1245/1250 PROPERTY'
             AND flv.lookup_code =
                 cur_p1245_rec.leg_property_1245_1250_code
             AND flv.enabled_flag = 'Y'
             AND TRUNC(SYSDATE) BETWEEN
                 NVL(flv.start_date_active, TRUNC(SYSDATE)) AND
                 NVL(flv.end_date_active, TRUNC(SYSDATE));
        EXCEPTION
          WHEN OTHERS THEN
            FOR r_org_ref_err_rec IN (SELECT DISTINCT leg_asset_number,
                                                      leg_book_type_code
                                        FROM xxfa_tax_asset_stg xis
                                       WHERE leg_property_1245_1250_code =
                                             cur_p1245_rec.leg_property_1245_1250_code
                                         AND batch_id = g_batch_id
                                         AND run_sequence_id =
                                             g_new_run_seq_id) LOOP
              log_errors(pin_interface_txn_id    => NULL,
                         piv_source_table        => 'XXFA_TAX_ASSET_STG',
                         piv_source_column_name  => 'leg_property_1245_1250_code',
                         piv_source_column_value => cur_p1245_rec.leg_property_1245_1250_code,
                         piv_source_keyname1     => 'leg_asset_number',
                         piv_source_keyvalue1    => r_org_ref_err_rec.leg_asset_number,
                         piv_source_keyname2     => 'leg_book_type_code',
                         piv_source_keyvalue2    => r_org_ref_err_rec.leg_book_type_code,
                         piv_error_type          => 'VAL_ERR',
                         piv_error_code          => 'ETN_FA_INVALID_LOOKUP_VAL',
                         piv_error_message       => 'Error : Invalid leg_property_1245_1250_code value.');
            END LOOP;

            UPDATE xxfa_tax_asset_stg
               SET process_flag      = 'E',
                   ERROR_TYPE        = 'VAL_ERR',
                   request_id        = g_request_id,
                   last_updated_date = SYSDATE,
                   last_updated_by   = g_last_updated_by,
                   last_update_login = g_last_update_login
             WHERE leg_property_1245_1250_code =
                   cur_p1245_rec.leg_property_1245_1250_code
               AND batch_id = g_batch_id
               AND run_sequence_id = g_new_run_seq_id;

            COMMIT;
        END;
      END IF;
    END LOOP;

    FOR cur_new_rec IN cur_new LOOP
      ln_code := NULL;

      IF cur_new_rec.leg_new_used IS NOT NULL THEN
        BEGIN
          SELECT 1
            INTO ln_code
            FROM fa_lookups flv
           WHERE flv.lookup_type = 'NEWUSE'
             AND flv.lookup_code = cur_new_rec.leg_new_used
             AND flv.enabled_flag = 'Y'
             AND TRUNC(SYSDATE) BETWEEN
                 NVL(flv.start_date_active, TRUNC(SYSDATE)) AND
                 NVL(flv.end_date_active, TRUNC(SYSDATE));
        EXCEPTION
          WHEN OTHERS THEN
            FOR r_org_ref_err_rec IN (SELECT DISTINCT leg_asset_number,
                                                      leg_book_type_code
                                        FROM xxfa_tax_asset_stg xis
                                       WHERE leg_new_used =
                                             cur_new_rec.leg_new_used
                                         AND batch_id = g_batch_id
                                         AND run_sequence_id =
                                             g_new_run_seq_id) LOOP
              log_errors(pin_interface_txn_id    => NULL,
                         piv_source_table        => 'XXFA_TAX_ASSET_STG',
                         piv_source_column_name  => 'leg_new_used',
                         piv_source_column_value => cur_new_rec.leg_new_used,
                         piv_source_keyname1     => 'leg_asset_number',
                         piv_source_keyvalue1    => r_org_ref_err_rec.leg_asset_number,
                         piv_source_keyname2     => 'leg_book_type_code',
                         piv_source_keyvalue2    => r_org_ref_err_rec.leg_book_type_code,
                         piv_error_type          => 'VAL_ERR',
                         piv_error_code          => 'ETN_FA_INVALID_LOOKUP_VAL',
                         piv_error_message       => 'Error : Invalid leg_new_used value.');
            END LOOP;

            UPDATE xxfa_tax_asset_stg
               SET process_flag      = 'E',
                   ERROR_TYPE        = 'VAL_ERR',
                   request_id        = g_request_id,
                   last_updated_date = SYSDATE,
                   last_updated_by   = g_last_updated_by,
                   last_update_login = g_last_update_login
             WHERE leg_new_used = cur_new_rec.leg_new_used
               AND batch_id = g_batch_id
               AND run_sequence_id = g_new_run_seq_id;

            COMMIT;
        END;
      END IF;
    END LOOP;

    ---------------------
    -------------------------------------------------------------------------
    --   Adding to derive the value from the Lookup Harjinder Singh
    -------------------------------------------------------------------------

    FOR c_der_book_info_rec IN c_der_book_info LOOP
      fnd_file.put_line(fnd_file.LOG, 'inside cur c_der_book_info_rec');

      l_book_type := c_der_book_info_rec.book_type;

      BEGIN
        SELECT tag
          INTO l_r12_book
          FROM fnd_lookup_values flv
         WHERE lookup_type = g_lookup_r12_type
           and language = 'US' ------v1.3 11th May,2015
           AND lookup_code = c_der_book_info_rec.book_type
           AND TRUNC(SYSDATE) BETWEEN
               NVL(flv.start_date_active, TRUNC(SYSDATE)) AND
               NVL(flv.end_date_active, TRUNC(SYSDATE));

        fnd_file.put_line(fnd_file.LOG, 'inside begin');

        UPDATE xxfa_tax_asset_stg
           SET book_type_code    = l_r12_book,
               request_id        = g_request_id,
               last_updated_date = SYSDATE,
               last_updated_by   = g_last_updated_by,
               last_update_login = g_last_update_login
         WHERE leg_cc_segment1 || ' ' || leg_book_type_code =
               c_der_book_info_rec.book_type;
        COMMIT;

        fnd_file.put_line(fnd_file.LOG, 'after update');
      EXCEPTION
        WHEN OTHERS THEN

          FOR r_der_book_info_rec IN (SELECT interface_txn_id
                                        FROM xxfa_tax_asset_stg xis
                                       WHERE leg_cc_segment1 || ' ' ||
                                             leg_book_type_code =
                                             c_der_book_info_rec.book_type
                                         AND batch_id = g_batch_id
                                         AND run_sequence_id =
                                             g_new_run_seq_id)

           LOOP
            log_errors(pin_interface_txn_id    => NULL,
                       piv_source_table        => 'xxfa_tax_asset_stg',
                       piv_source_column_name  => 'book_type_code',
                       piv_source_column_value => c_der_book_info_rec.book_type,
                       piv_source_keyname1     => 'interface_txn_id',
                       piv_source_keyvalue1    => r_der_book_info_rec.interface_txn_id,
                       piv_error_type          => 'VAL_ERR',
                       piv_error_code          => 'ETN_FA_INVALID_BOOK_TYPE',
                       piv_error_message       => 'Error : Invalid Book Type.');
          END LOOP;

          UPDATE xxfa_tax_asset_stg
             SET process_flag      = 'E',
                 ERROR_TYPE        = 'VAL_ERR',
                 request_id        = g_request_id,
                 last_updated_date = SYSDATE,
                 last_updated_by   = g_last_updated_by,
                 last_update_login = g_last_update_login
           where leg_cc_segment1 || ' ' || leg_book_type_code =
                 c_der_book_info_rec.book_type;

          lv_sql_count := sql%rowcount;

          fnd_file.put_line(fnd_file.LOG, 'v6 COUNT2 : ' || lv_sql_count);

          COMMIT;

      END;
    END LOOP;
    ---------------------

    ---------------------

    FOR cur_book_rec IN cur_book LOOP
      ln_book_code := NULL;

      IF /*cur_book_rec.leg_book_type_code*/
       cur_book_rec.book_type_code IS NULL ------v1.3 11th May,2015
       THEN
        FOR r_org_ref_err_rec IN (SELECT DISTINCT leg_asset_number
                                    FROM xxfa_tax_asset_stg xis
                                   WHERE /*leg_book_type_code*/
                                   book_type_code IS NULL ------v1.3 11th May,2015
                               AND batch_id = g_batch_id
                               AND run_sequence_id = g_new_run_seq_id) LOOP
          log_errors(pin_interface_txn_id    => NULL,
                     piv_source_table        => 'XXFA_TAX_ASSET_STG',
                     piv_source_column_name  => /*'leg_book_type_code'*/ 'book_type_code', ------v1.3 11th May,2015
                     piv_source_column_value => NULL,
                     piv_source_keyname1     => 'leg_asset_number',
                     piv_source_keyvalue1    => r_org_ref_err_rec.leg_asset_number,
                     piv_source_keyname2     => /*'leg_book_type_code'*/ 'book_type_code', ------v1.3 11th May,2015
                     piv_source_keyvalue2    => NULL,
                     piv_error_type          => 'VAL_ERR',
                     piv_error_code          => 'ETN_FA_MANDATORY_COLUMN',
                     piv_error_message       => 'Error : Book Type Cannot be NULL');
        END LOOP;

        UPDATE xxfa_tax_asset_stg
           SET process_flag      = 'E',
               ERROR_TYPE        = 'VAL_ERR',
               request_id        = g_request_id,
               last_updated_date = SYSDATE,
               last_updated_by   = g_last_updated_by,
               last_update_login = g_last_update_login
         WHERE /*leg_book_type_code*/
         book_type_code IS NULL ------v1.3 11th May,2015
         AND batch_id = g_batch_id
         AND run_sequence_id = g_new_run_seq_id;

        COMMIT;
      ELSE
        BEGIN
          SELECT 1
            INTO ln_book_code
            FROM fa_book_controls
           WHERE book_type_code = /*cur_book_rec.leg_book_type_code*/
                 cur_book_rec.book_type_code; ------v1.3 11th May,2015
        EXCEPTION
          WHEN OTHERS THEN
            FOR r_org_ref_err_rec IN (SELECT DISTINCT leg_asset_number,
                                                      /*leg_book_type_code*/
                                                      book_type_code ------v1.3 11th May,2015
                                        FROM xxfa_tax_asset_stg xis
                                       WHERE /*leg_book_type_code*/
                                       book_type_code =
                                      /*cur_book_rec.leg_book_type_code*/
                                       cur_book_rec.book_type_code ------v1.3 11th May,2015
                                   AND batch_id = g_batch_id
                                   AND run_sequence_id = g_new_run_seq_id) LOOP
              log_errors(pin_interface_txn_id    => NULL,
                         piv_source_table        => 'XXFA_TAX_ASSET_STG',
                         piv_source_column_name  => /*'leg_book_type_code'*/ 'book_type_code', ------v1.3 11th May,2015
                         piv_source_column_value => /*cur_book_rec.leg_book_type_code*/ cur_book_rec.book_type_code, ------v1.3 11th May,2015
                         piv_source_keyname1     => 'leg_asset_number',
                         piv_source_keyvalue1    => r_org_ref_err_rec.leg_asset_number,
                         piv_source_keyname2     => /* 'leg_book_type_code'*/ 'book_type_code', ------v1.3 11th May,2015
                         piv_source_keyvalue2    => /*r_org_ref_err_rec.leg_book_type_code*/ r_org_ref_err_rec.book_type_code, ------v1.3 11th May,2015
                         piv_error_type          => 'VAL_ERR',
                         piv_error_code          => 'ETN_FA_INVALID_BOOK_TYPE',
                         piv_error_message       => 'Error : Book Type is Invalid.');
            END LOOP;

            UPDATE xxfa_tax_asset_stg
               SET process_flag      = 'E',
                   ERROR_TYPE        = 'VAL_ERR',
                   request_id        = g_request_id,
                   last_updated_date = SYSDATE,
                   last_updated_by   = g_last_updated_by,
                   last_update_login = g_last_update_login
             WHERE /*leg_book_type_code = cur_book_rec.leg_book_type_code*/ ------v1.3 11th May,2015
             book_type_code = cur_book_rec.book_type_code
             AND batch_id = g_batch_id
             AND run_sequence_id = g_new_run_seq_id;

            COMMIT;
        END;
      END IF;
    END LOOP;

    FOR cur_deprn_rec IN cur_deprn LOOP
      ln_life_in_months := NULL;

      IF cur_deprn_rec.leg_deprn_method_code IS NULL THEN
        FOR r_org_ref_err_rec IN (SELECT DISTINCT leg_asset_number,
                                                  leg_book_type_code
                                    FROM xxfa_tax_asset_stg xis
                                   WHERE leg_deprn_method_code IS NULL
                                     AND batch_id = g_batch_id
                                     AND run_sequence_id = g_new_run_seq_id) LOOP
          log_errors(pin_interface_txn_id    => NULL,
                     piv_source_table        => 'XXFA_TAX_ASSET_STG',
                     piv_source_column_name  => 'leg_deprn_method_code',
                     piv_source_column_value => NULL,
                     piv_source_keyname1     => 'leg_asset_number',
                     piv_source_keyvalue1    => r_org_ref_err_rec.leg_asset_number,
                     piv_source_keyname2     => 'leg_book_type_code',
                     piv_source_keyvalue2    => r_org_ref_err_rec.leg_book_type_code,
                     piv_error_type          => 'VAL_ERR',
                     piv_error_code          => 'ETN_FA_MANDATORY_COLUMN',
                     piv_error_message       => 'Error : Depreciation Method cannot be NULL');
        END LOOP;

        UPDATE xxfa_tax_asset_stg
           SET process_flag      = 'E',
               ERROR_TYPE        = 'VAL_ERR',
               request_id        = g_request_id,
               last_updated_date = SYSDATE,
               last_updated_by   = g_last_updated_by,
               last_update_login = g_last_update_login
         WHERE leg_deprn_method_code IS NULL
           AND batch_id = g_batch_id
           AND run_sequence_id = g_new_run_seq_id;

        COMMIT;
      ELSE
        BEGIN

          IF cur_deprn_rec.leg_basic_rate IS NOT NULL THEN
            BEGIN
              SELECT 1
                INTO ln_life_in_months
                FROM fa.fa_methods Fm, fa.fa_flat_rates ffr
               WHERE Fm.METHOD_ID = ffr.METHOD_ID(+)
                 AND ffr.basic_rate = cur_deprn_rec.leg_basic_rate
                 AND ffr.adjusted_rate = cur_deprn_rec.leg_adjusted_rate
                 AND method_code = cur_deprn_rec.leg_deprn_method_code;
            EXCEPTION
              WHEN OTHERS THEN
                FOR r_org_ref_err_rec IN (SELECT DISTINCT leg_asset_number
                                            FROM xxfa_tax_asset_stg xis
                                           WHERE leg_deprn_method_code =
                                                 cur_deprn_rec.leg_deprn_method_code
                                             AND leg_basic_rate =
                                                 cur_deprn_rec.leg_basic_rate
                                             AND leg_adjusted_rate =
                                                 cur_deprn_rec.leg_adjusted_rate

                                             AND batch_id = g_batch_id
                                             AND run_sequence_id =
                                                 g_new_run_seq_id) LOOP
                  log_errors(pin_interface_txn_id    => NULL,
                             piv_source_table        => 'XXFA_TAX_ASSET_STG',
                             piv_source_column_name  => 'leg_basic_rate||leg_adjusted_rate',
                             piv_source_column_value => cur_deprn_rec.leg_basic_rate || '||' ||
                                                        cur_deprn_rec.leg_adjusted_rate,
                             piv_source_keyname1     => 'leg_asset_number',
                             piv_source_keyvalue1    => r_org_ref_err_rec.leg_asset_number,
                             piv_error_type          => 'VAL_ERR',
                             piv_error_code          => 'ETN_FA_INVALID_DEPRN_METHOD',
                             piv_error_message       => 'Error : Depreciation Method is Invalid. for Rates');
                END LOOP;

                UPDATE xxfa_tax_asset_stg
                   SET process_flag      = 'E',
                       ERROR_TYPE        = 'VAL_ERR',
                       request_id        = g_request_id,
                       last_updated_date = SYSDATE,
                       last_updated_by   = g_last_updated_by,
                       last_update_login = g_last_update_login
                 WHERE leg_deprn_method_code =
                       cur_deprn_rec.leg_deprn_method_code

                   AND leg_basic_rate = cur_deprn_rec.leg_basic_rate
                   AND leg_adjusted_rate = cur_deprn_rec.leg_adjusted_rate
                   AND batch_id = g_batch_id
                   AND run_sequence_id = g_new_run_seq_id;

                COMMIT;
            END;

          ELSE
            --- else for rate not null

            BEGIN

              SELECT 1
                INTO ln_life_in_months
                FROM fa_methods
               WHERE method_code = cur_deprn_rec.leg_deprn_method_code
                 AND NVL(life_in_months, -999) =
                     NVL(cur_deprn_rec.leg_life_in_months, -999); ---commented on 21st Oct 2015 as per Monica's suggestion
            EXCEPTION
              WHEN OTHERS THEN
                FOR r_org_ref_err_rec IN (SELECT DISTINCT leg_asset_number
                                            FROM xxfa_tax_asset_stg xis
                                           WHERE leg_deprn_method_code =
                                                 cur_deprn_rec.leg_deprn_method_code
                                             AND NVL(leg_life_in_months,
                                                     -999) =
                                                 NVL(cur_deprn_rec.leg_life_in_months,
                                                     -999)
                                             AND batch_id = g_batch_id
                                             AND run_sequence_id =
                                                 g_new_run_seq_id) LOOP
                  log_errors(pin_interface_txn_id    => NULL,
                             piv_source_table        => 'XXFA_TAX_ASSET_STG',
                             piv_source_column_name  => 'leg_deprn_method_code||life_in_months',
                             piv_source_column_value => cur_deprn_rec.leg_deprn_method_code || '||' ||
                                                        cur_deprn_rec.leg_life_in_months,
                             piv_source_keyname1     => 'leg_asset_number',
                             piv_source_keyvalue1    => r_org_ref_err_rec.leg_asset_number,
                             piv_error_type          => 'VAL_ERR',
                             piv_error_code          => 'ETN_FA_INVALID_DEPRN_METHOD',
                             piv_error_message       => 'Error : Depreciation Method is Invalid.');
                END LOOP;

                UPDATE xxfa_tax_asset_stg
                   SET process_flag      = 'E',
                       ERROR_TYPE        = 'VAL_ERR',
                       request_id        = g_request_id,
                       last_updated_date = SYSDATE,
                       last_updated_by   = g_last_updated_by,
                       last_update_login = g_last_update_login
                 WHERE leg_deprn_method_code =
                       cur_deprn_rec.leg_deprn_method_code
                   AND NVL(leg_life_in_months, -999) =
                       NVL(cur_deprn_rec.leg_life_in_months, -999)
                   AND batch_id = g_batch_id
                   AND run_sequence_id = g_new_run_seq_id;

                COMMIT;
            END;
          END IF;
        END;

      END IF;
    END LOOP;

    FOR cur_pror_rec IN cur_pror LOOP
      ln_prorate_count := NULL;

      IF cur_pror_rec.leg_prorate_convention_code IS NULL THEN
        FOR r_org_ref_err_rec IN (SELECT DISTINCT leg_asset_number,
                                                  leg_book_type_code
                                    FROM xxfa_tax_asset_stg xis
                                   WHERE leg_prorate_convention_code IS NULL
                                     AND batch_id = g_batch_id
                                     AND run_sequence_id = g_new_run_seq_id) LOOP
          log_errors(pin_interface_txn_id    => NULL,
                     piv_source_table        => 'XXFA_TAX_ASSET_STG',
                     piv_source_column_name  => 'leg_prorate_convention_code',
                     piv_source_column_value => NULL,
                     piv_source_keyname1     => 'leg_asset_number',
                     piv_source_keyvalue1    => r_org_ref_err_rec.leg_asset_number,
                     piv_source_keyname2     => 'leg_book_type_code',
                     piv_source_keyvalue2    => r_org_ref_err_rec.leg_book_type_code,
                     piv_error_type          => 'VAL_ERR',
                     piv_error_code          => 'ETN_FA_MANDATORY_COLUMN',
                     piv_error_message       => 'Error : Prorate Convention code cannot be NULL');
        END LOOP;

        UPDATE xxfa_tax_asset_stg
           SET process_flag      = 'E',
               ERROR_TYPE        = 'VAL_ERR',
               request_id        = g_request_id,
               last_updated_date = SYSDATE,
               last_updated_by   = g_last_updated_by,
               last_update_login = g_last_update_login
         WHERE leg_prorate_convention_code IS NULL
           AND batch_id = g_batch_id
           AND run_sequence_id = g_new_run_seq_id;

        COMMIT;
      ELSE
        BEGIN
          SELECT 1
            INTO ln_prorate_count
            FROM fa_convention_types
           WHERE prorate_convention_code =
                 cur_pror_rec.leg_prorate_convention_code;
        EXCEPTION
          WHEN OTHERS THEN
            FOR r_org_ref_err_rec IN (SELECT DISTINCT leg_asset_number,
                                                      leg_book_type_code
                                        FROM xxfa_tax_asset_stg xis
                                       WHERE leg_prorate_convention_code =
                                             cur_pror_rec.leg_prorate_convention_code
                                         AND batch_id = g_batch_id
                                         AND run_sequence_id =
                                             g_new_run_seq_id) LOOP
              log_errors(pin_interface_txn_id    => NULL,
                         piv_source_table        => 'XXFA_TAX_ASSET_STG',
                         piv_source_column_name  => 'leg_prorate_convention_code',
                         piv_source_column_value => cur_pror_rec.leg_prorate_convention_code,
                         piv_source_keyname1     => 'leg_asset_number',
                         piv_source_keyvalue1    => r_org_ref_err_rec.leg_asset_number,
                         piv_source_keyname2     => 'leg_book_type_code',
                         piv_source_keyvalue2    => r_org_ref_err_rec.leg_book_type_code,
                         piv_error_type          => 'VAL_ERR',
                         piv_error_code          => 'ETN_FA_INVALID_DEPRN_METHOD',
                         piv_error_message       => 'Error : Prorate Convention code is Invalid.');
            END LOOP;

            UPDATE xxfa_tax_asset_stg
               SET process_flag      = 'E',
                   ERROR_TYPE        = 'VAL_ERR',
                   request_id        = g_request_id,
                   last_updated_date = SYSDATE,
                   last_updated_by   = g_last_updated_by,
                   last_update_login = g_last_update_login
             WHERE leg_prorate_convention_code =
                   cur_pror_rec.leg_prorate_convention_code
               AND batch_id = g_batch_id
               AND run_sequence_id = g_new_run_seq_id;

            COMMIT;
        END;
      END IF;
    END LOOP;

    FOR cur_ceiling_rec IN cur_ceiling LOOP
      ln_ceiling_count := 0;

      IF cur_ceiling_rec.leg_ceiling_name IS NOT NULL THEN
        BEGIN
          SELECT COUNT(1)
            INTO ln_ceiling_count
            FROM fa_ceilings
           WHERE ceiling_name = cur_ceiling_rec.leg_ceiling_name;
        EXCEPTION
          WHEN OTHERS THEN
            ln_ceiling_count := 0;
        END;

        IF ln_ceiling_count = 0 THEN
          FOR r_org_ref_err_rec IN (SELECT DISTINCT leg_asset_number,
                                                    leg_book_type_code
                                      FROM xxfa_tax_asset_stg xis
                                     WHERE leg_ceiling_name =
                                           cur_ceiling_rec.leg_ceiling_name
                                       AND batch_id = g_batch_id
                                       AND run_sequence_id =
                                           g_new_run_seq_id) LOOP
            log_errors(pin_interface_txn_id    => NULL,
                       piv_source_table        => 'XXFA_TAX_ASSET_STG',
                       piv_source_column_name  => 'leg_ceiling_name',
                       piv_source_column_value => cur_ceiling_rec.leg_ceiling_name,
                       piv_source_keyname1     => 'leg_asset_number',
                       piv_source_keyvalue1    => r_org_ref_err_rec.leg_asset_number,
                       piv_source_keyname2     => 'leg_book_type_code',
                       piv_source_keyvalue2    => r_org_ref_err_rec.leg_book_type_code,
                       piv_error_type          => 'VAL_ERR',
                       piv_error_code          => 'ETN_FA_INVALID_DEPRN_METHOD',
                       piv_error_message       => 'Error : Prorate Convention code is Invalid.');
          END LOOP;

          UPDATE xxfa_tax_asset_stg
             SET process_flag      = 'E',
                 ERROR_TYPE        = 'VAL_ERR',
                 request_id        = g_request_id,
                 last_updated_date = SYSDATE,
                 last_updated_by   = g_last_updated_by,
                 last_update_login = g_last_update_login
           WHERE leg_ceiling_name = cur_ceiling_rec.leg_ceiling_name
             AND batch_id = g_batch_id
             AND run_sequence_id = g_new_run_seq_id;

          COMMIT;
        END IF;
      END IF;
    END LOOP;

    l_count := 0;

    FOR rec_cur_val_tax IN cur_val_tax LOOP
      lc_error_flag          := 'N';
      ln_property_code       := NULL;
      ln_category_id         := NULL;
      lc_cat_segment1        := NULL;
      lc_cat_segment2        := NULL;
      lc_cat_flag            := NULL;
      ln_prop_1245_1250_code := NULL;
      ln_own_lease           := NULL;
      ln_new_use             := NULL;
      ln_life_in_months      := NULL;
      ln_prorate_count       := NULL;
      ln_ceiling_count       := NULL;
      ln_bonus_count         := NULL;
      ln_book_code           := NULL;
      ln_parent_count        := NULL;
      ln_count_tax           := NULL;
      ln_count_tag           := NULL;
      ln_asset_key           := NULL;
      lc_cat_msg             := NULL;
      lv_attribute1          := NULL;
      lv_attribute2          := NULL;
      lv_attribute3          := NULL;
      lv_attribute4          := NULL;
      lv_attribute5          := NULL;
      lv_attribute6          := NULL;
      lv_attribute7          := NULL;
      lv_attribute8          := NULL;
      lv_attribute9          := NULL;
      lv_attribute10         := NULL;
      lv_attribute11         := NULL;
      lv_attribute12         := NULL;
      lv_attribute13         := NULL;
      lv_attribute14         := NULL;
      lv_attribute15         := NULL;
      lv_attribute16         := NULL;
      lv_attribute17         := NULL;
      lv_attribute18         := NULL;
      lv_attribute19         := NULL;
      lv_attribute20         := NULL;
      lv_attribute21         := NULL;
      lv_attribute22         := NULL;
      lv_attribute23         := NULL;
      lv_attribute24         := NULL;
      lv_attribute25         := NULL;
      lv_attribute26         := NULL;
      lv_attribute27         := NULL;
      lv_attribute28         := NULL;
      lv_attribute29         := NULL;
      lv_attribute30         := NULL;
      lv_adn_cat_code        := NULL;
      lv_adn_cont            := NULL;
      lv_adn_gl_attribute1   := NULL;
      lv_adn_gl_attribute2   := NULL;
      lv_adn_gl_attribute3   := NULL;
      lv_adn_gl_attribute4   := NULL;
      lv_adn_gl_attribute5   := NULL;
      lv_adn_gl_attribute6   := NULL;
      lv_adn_gl_attribute7   := NULL;
      lv_adn_gl_attribute8   := NULL;
      lv_adn_gl_attribute9   := NULL;
      lv_adn_gl_attribute10  := NULL;
      lv_adn_gl_attribute11  := NULL;
      lv_adn_gl_attribute12  := NULL;
      lv_adn_gl_attribute13  := NULL;
      lv_adn_gl_attribute14  := NULL;
      lv_adn_gl_attribute15  := NULL;
      lv_adn_gl_attribute16  := NULL;
      lv_adn_gl_attribute17  := NULL;
      lv_adn_gl_attribute18  := NULL;
      lv_adn_gl_attribute19  := NULL;
      lv_adn_gl_attribute20  := NULL;
      lv_adn_gl_cat          := NULL;
      lv_bk_gl_attribute1    := NULL;
      lv_bk_gl_attribute2    := NULL;
      lv_bk_gl_attribute3    := NULL;
      lv_bk_gl_attribute4    := NULL;
      lv_bk_gl_attribute5    := NULL;
      lv_bk_gl_attribute6    := NULL;
      lv_bk_gl_attribute7    := NULL;
      lv_bk_gl_attribute8    := NULL;
      lv_bk_gl_attribute9    := NULL;
      lv_bk_gl_attribute10   := NULL;
      lv_bk_gl_attribute11   := NULL;
      lv_bk_gl_attribute12   := NULL;
      lv_bk_gl_attribute13   := NULL;
      lv_bk_gl_attribute14   := NULL;
      lv_bk_gl_attribute15   := NULL;
      lv_bk_gl_attribute16   := NULL;
      lv_bk_gl_attribute17   := NULL;
      lv_bk_gl_attribute18   := NULL;
      lv_bk_gl_attribute19   := NULL;
      lv_bk_gl_attribute20   := NULL;
      lv_bk_gl_att_cat       := NULL;
      xxetn_debug_pkg.add_debug(piv_debug_msg => 'Validation process for Tax book starts for asset number : ' ||
                                                 rec_cur_val_tax.leg_asset_number ||
                                                 ' and book type : ' ||
                                                 rec_cur_val_tax.leg_book_type_code);

      -- Check for asset Inconsistancies
      BEGIN
        SELECT DISTINCT leg_asset_number,
                        leg_book_type_code,
                        leg_adjusted_cost,
                        leg_cost,
                        leg_original_cost,
                        leg_ytd_deprn,
                        leg_deprn_reserve
          INTO l_leg_asset_number,
               l_leg_book_type_code,
               l_leg_adjusted_cost,
               l_leg_cost,
               l_leg_original_cost,
               l_leg_ytd_deprn,
               l_leg_deprn_reserve
          FROM xxfa_tax_asset_stg
         WHERE leg_asset_number = rec_cur_val_tax.leg_asset_number
           AND leg_book_type_code = rec_cur_val_tax.leg_book_type_code
           AND process_flag IN ('N', 'E')
           AND batch_id = g_batch_id
           AND run_sequence_id = g_new_run_seq_id;
      EXCEPTION
        WHEN TOO_MANY_ROWS THEN
          log_errors(pin_interface_txn_id    => NULL,
                     piv_source_table        => 'XXFA_TAX_ASSET_STG',
                     piv_source_column_name  => 'leg_asset_number',
                     piv_source_column_value => rec_cur_val_tax.leg_asset_number,
                     piv_source_keyname1     => 'leg_asset_number',
                     piv_source_keyvalue1    => rec_cur_val_tax.leg_asset_number,
                     piv_source_keyname2     => 'leg_book_type_code',
                     piv_source_keyvalue2    => rec_cur_val_tax.leg_book_type_code,
                     piv_error_type          => 'VAL_ERR',
                     piv_error_code          => 'ETN_FA_INCONSISTANT_ASSET',
                     piv_error_message       => 'Error : Asset Number is inconsistant in staging table for Asset Cost,Deprn Reserve,Book Type');
          lc_error_flag := 'Y';
        WHEN OTHERS THEN
          log_errors(pin_interface_txn_id    => NULL,
                     piv_source_table        => 'XXFA_TAX_ASSET_STG',
                     piv_source_column_name  => 'leg_asset_number',
                     piv_source_column_value => rec_cur_val_tax.leg_asset_number,
                     piv_source_keyname1     => 'leg_asset_number',
                     piv_source_keyvalue1    => rec_cur_val_tax.leg_asset_number,
                     piv_source_keyname2     => 'leg_book_type_code',
                     piv_source_keyvalue2    => rec_cur_val_tax.leg_book_type_code,
                     piv_error_type          => 'VAL_ERR',
                     piv_error_code          => 'ETN_FA_INCONSISTANT_ASSET',
                     piv_error_message       => 'Error : Exception Error while fetching count for Inconsistant tax assets' ||
                                                SUBSTR(SQLERRM, 1, 240));
          lc_error_flag := 'Y';
      END;

      -- legacy current units must not be NULL
      IF rec_cur_val_tax.leg_current_units IS NULL THEN
        log_errors(pin_interface_txn_id    => NULL,
                   piv_source_table        => 'XXFA_TAX_ASSET_STG',
                   piv_source_column_name  => 'leg_current_units',
                   piv_source_column_value => rec_cur_val_tax.leg_current_units,
                   piv_source_keyname1     => 'leg_asset_number',
                   piv_source_keyvalue1    => rec_cur_val_tax.leg_asset_number,
                   piv_source_keyname2     => 'leg_book_type_code',
                   piv_source_keyvalue2    => rec_cur_val_tax.leg_book_type_code,
                   piv_error_type          => 'VAL_ERR',
                   piv_error_code          => 'ETN_FA_MANDATORY_COLUMN',
                   piv_error_message       => 'Error : leg_current_units cannot be NULL');
        lc_error_flag := 'Y';
      ELSE
        IF rec_cur_val_tax.leg_current_units < 0 THEN
          log_errors(pin_interface_txn_id    => NULL,
                     piv_source_table        => 'XXFA_TAX_ASSET_STG',
                     piv_source_column_name  => 'leg_current_units',
                     piv_source_column_value => rec_cur_val_tax.leg_current_units,
                     piv_source_keyname1     => 'leg_asset_number',
                     piv_source_keyvalue1    => rec_cur_val_tax.leg_asset_number,
                     piv_source_keyname2     => 'leg_book_type_code',
                     piv_source_keyvalue2    => rec_cur_val_tax.leg_book_type_code,
                     piv_error_type          => 'VAL_ERR',
                     piv_error_code          => 'ETN_FA_INVALID_CURRENT_UNIT',
                     piv_error_message       => 'Error : leg_current_units cannot be less than 0');
          lc_error_flag := 'Y';
        END IF;
      END IF;

      -- Asset type must not be NULL
      IF rec_cur_val_tax.leg_asset_type IS NULL THEN
        log_errors(pin_interface_txn_id    => NULL,
                   piv_source_table        => 'XXFA_TAX_ASSET_STG',
                   piv_source_column_name  => 'leg_asset_type',
                   piv_source_column_value => rec_cur_val_tax.leg_asset_type,
                   piv_source_keyname1     => 'leg_asset_number',
                   piv_source_keyvalue1    => rec_cur_val_tax.leg_asset_number,
                   piv_source_keyname2     => 'leg_book_type_code',
                   piv_source_keyvalue2    => rec_cur_val_tax.leg_book_type_code,
                   piv_error_type          => 'VAL_ERR',
                   piv_error_code          => 'ETN_FA_MANDATORY_COLUMN',
                   piv_error_message       => 'Error : leg_asset_type cannot be NULL');
        lc_error_flag := 'Y';
      ELSE
        IF rec_cur_val_tax.leg_asset_type <> g_asset_type THEN
          log_errors(pin_interface_txn_id    => NULL,
                     piv_source_table        => 'XXFA_TAX_ASSET_STG',
                     piv_source_column_name  => 'leg_asset_type',
                     piv_source_column_value => rec_cur_val_tax.leg_asset_type,
                     piv_source_keyname1     => 'leg_asset_number',
                     piv_source_keyvalue1    => rec_cur_val_tax.leg_asset_number,
                     piv_source_keyname2     => 'leg_book_type_code',
                     piv_source_keyvalue2    => rec_cur_val_tax.leg_book_type_code,
                     piv_error_type          => 'VAL_ERR',
                     piv_error_code          => 'ETN_FA_INVALID_ASSET_TYPE',
                     piv_error_message       => 'Error : leg_asset_type is not CAPITALIZED');
          lc_error_flag := 'Y';
        END IF;
      END IF;

      -- To check duplicacy of tag number starts  --1.1
      /* IF rec_cur_val_tax.leg_tag_number IS NOT NULL
      THEN
         BEGIN
            SELECT COUNT (1)
              INTO ln_count_tag
              FROM xxfa_tax_asset_stg
             WHERE leg_tag_number = rec_cur_val_tax.leg_tag_number
               AND leg_asset_number <> rec_cur_val_tax.leg_asset_number
               AND process_flag = 'N'
               AND batch_id = g_batch_id
               AND run_sequence_id = g_new_run_seq_id;
         EXCEPTION
            WHEN OTHERS
            THEN
               log_errors
                  (pin_interface_txn_id         => NULL,
                   piv_source_table             => 'XXFA_TAX_ASSET_STG',
                   piv_source_column_name       => NULL,
                   piv_source_column_value      => NULL,
                   piv_source_keyname1          => 'leg_asset_number',
                   piv_source_keyvalue1         => rec_cur_val_tax.leg_asset_number,
                   piv_source_keyname2          => 'leg_book_type_code',
                   piv_source_keyvalue2         => rec_cur_val_tax.leg_book_type_code,
                   piv_error_type               => 'VAL_ERR',
                   piv_error_code               => 'ETN_FA_DUPLICATE_TAG_NUM',
                   piv_error_message            =>    'Error : Exception Error while fetching count for duplicate tag number'
                                                   || SUBSTR (SQLERRM,
                                                              1,
                                                              240
                                                             )
                  );
               lc_error_flag := 'Y';
         END;

         IF ln_count_tag > 0
         THEN
            BEGIN
               UPDATE xxfa_tax_asset_stg
                  SET process_flag = 'E',
                      ERROR_TYPE = 'VAL_ERR'
                WHERE leg_tag_number = rec_cur_val_tax.leg_tag_number
                  AND process_flag = 'N'
                  AND batch_id = g_batch_id
                  AND run_sequence_id = g_new_run_seq_id;

               COMMIT;
            EXCEPTION
               WHEN OTHERS
               THEN
                  log_errors
                     (pin_interface_txn_id         => NULL,
                      piv_source_table             => 'XXFA_TAX_ASSET_STG',
                      piv_source_column_name       => NULL,
                      piv_source_column_value      => NULL,
                      piv_source_keyname1          => 'leg_asset_number',
                      piv_source_keyvalue1         => rec_cur_val_tax.leg_asset_number,
                      piv_source_keyname2          => 'leg_book_type_code',
                      piv_source_keyvalue2         => rec_cur_val_tax.leg_book_type_code,
                      piv_error_type               => 'VAL_ERR',
                      piv_error_code               => 'ETN_FA_DUPLICATE_TAG_NUM',
                      piv_error_message            =>    'Error : Exception occured while updating assets with duplicate tag number '
                                                      || SUBSTR (SQLERRM,
                                                                 1,
                                                                 240
                                                                )
                     );
                  lc_error_flag := 'Y';
            END;

            log_errors
               (pin_interface_txn_id         => NULL,
                piv_source_table             => 'XXFA_TAX_ASSET_STG',
                piv_source_column_name       => 'leg_tag_number',
                piv_source_column_value      => rec_cur_val_tax.leg_tag_number,
                piv_error_type               => 'VAL_ERR',
                piv_error_code               => 'ETN_FA_DUPLICATE_TAG_NUM',
                piv_error_message            => 'Error : Assets exist with Duplicate tag number in the staging table :'
               );
         END IF;
      END IF; */

      -- To check property type code starts  --1.1
      /* IF rec_cur_val_tax.leg_property_type_code IS NOT NULL
      THEN
         ln_property_code :=
            get_lookup_value ('PROPERTY TYPE',
                              'XXFA_TAX_ASSET_STG',
                              'leg_property_type_code',
                              rec_cur_val_tax.leg_property_type_code,
                              'leg_asset_number',
                              rec_cur_val_tax.leg_asset_number,
                              rec_cur_val_tax.leg_book_type_code
                             );

         IF ln_property_code = 0
         THEN
            lc_error_flag := 'Y';
         END IF;
      END IF; */

      --- check for property type code ends
      -----------------added as per v1.5---------------------------------------
      set_cat_lookup(p_leg_source_asset_number => rec_cur_val_tax.leg_source_asset_number,
                     p_leg_book_type_code      => rec_cur_val_tax.leg_book_type_code); --added by harjinder sing for the PMC#349366
      ---------------- added as per v1.5---------------------------------------


      IF g_cat_lookup = 'ETN_FA_CATEGORY_MAP_POLAND' THEN
         validate_asset_cat_poland_tax(rec_cur_val_tax.leg_source_system,
                                       rec_cur_val_tax.leg_asset_number,
                                       rec_cur_val_tax.leg_book_type_code,
                                       rec_cur_val_tax.leg_asset_cat_segment1,
                                       ln_CORP_category,
                                       ln_category_id,
                                       lc_cat_segment1,
                                       lc_cat_segment2,
                                       lc_cat_flag,
                                       lc_cat_msg);


        IF lc_cat_flag = 'Y' THEN
          log_errors(pin_interface_txn_id    => NULL,
                     piv_source_table        => 'XXFA_CORP_ASSET_STG',
                     piv_source_column_name  => 'leg_asset_cat_segment1.leg_asset_cat_segment2',
                     piv_source_column_value => ln_CORP_category ||
                                                g_sep_p || rec_cur_val_tax.leg_asset_cat_segment1,
                     piv_source_keyname1     => 'leg_asset_number',
                     piv_source_keyvalue1    => rec_cur_val_tax.leg_asset_number,
                     piv_error_type          => 'VAL_ERR',
                     piv_error_code          => 'ETN_FA_INVALID_CATEGORY',
                     piv_error_message       => lc_cat_msg);
          lc_error_flag := 'Y';
        END IF;


      ELSE








      -- check category segment starts
      validate_asset_category(rec_cur_val_tax.leg_source_system,
                              rec_cur_val_tax.leg_asset_cat_segment1,
                              rec_cur_val_tax.leg_asset_cat_segment2,
                              ln_category_id,
                              lc_cat_segment1,
                              lc_cat_segment2,
                              lc_cat_flag,
                              lc_cat_msg);

      IF lc_cat_flag = 'Y' THEN
        log_errors(pin_interface_txn_id    => NULL,
                   piv_source_table        => 'XXFA_TAX_ASSET_STG',
                   piv_source_column_name  => 'leg_asset_cat_segment1.leg_asset_cat_segment2',
                   piv_source_column_value => rec_cur_val_tax.leg_asset_cat_segment1 ||
                                              g_sep ||
                                              rec_cur_val_tax.leg_asset_cat_segment2,
                   piv_source_keyname1     => 'leg_asset_number',
                   piv_source_keyvalue1    => rec_cur_val_tax.leg_asset_number,
                   piv_source_keyname2     => 'leg_book_type_code',
                   piv_source_keyvalue2    => rec_cur_val_tax.leg_book_type_code,
                   piv_error_type          => 'VAL_ERR',
                   piv_error_code          => 'ETN_FA_INVALID_CATEGORY',
                   piv_error_message       => lc_cat_msg);
        lc_error_flag := 'Y';
      END IF;

      END IF;

      -- To check property 1245/1250 type code starts  -- 1.1
      /* IF rec_cur_val_tax.leg_property_1245_1250_code IS NOT NULL
      THEN
         ln_prop_1245_1250_code :=
            get_lookup_value ('1245/1250 PROPERTY',
                              'XXFA_TAX_ASSET_STG',
                              'leg_property_1245_1250_code',
                              rec_cur_val_tax.leg_property_1245_1250_code,
                              'leg_asset_number',
                              rec_cur_val_tax.leg_asset_number,
                              rec_cur_val_tax.leg_book_type_code
                             );

         IF ln_prop_1245_1250_code = 0
         THEN
            lc_error_flag := 'Y';
         END IF;
      END IF; */

      -- To check owned/leased code starts   --1.1
      /*  IF rec_cur_val_tax.leg_owned_leased IS NOT NULL
      THEN
         ln_own_lease :=
            get_lookup_value ('OWNLEASE',
                              'XXFA_TAX_ASSET_STG',
                              'leg_owned_leased',
                              rec_cur_val_tax.leg_owned_leased,
                              'leg_asset_number',
                              rec_cur_val_tax.leg_asset_number,
                              rec_cur_val_tax.leg_book_type_code
                             );

         IF ln_own_lease = 0
         THEN
            lc_error_flag := 'Y';
         END IF;
      END IF; */

      -- To check NEWUSE code starts  --1.1
      /* IF rec_cur_val_tax.leg_new_used IS NOT NULL
      THEN
         ln_new_use :=
            get_lookup_value ('NEWUSE',
                              'XXFA_TAX_ASSET_STG',
                              'leg_new_used',
                              rec_cur_val_tax.leg_new_used,
                              'leg_asset_number',
                              rec_cur_val_tax.leg_asset_number,
                              rec_cur_val_tax.leg_book_type_code
                             );

         IF ln_new_use = 0
         THEN
            lc_error_flag := 'Y';
         END IF;
      END IF; */

      --- Check for book type code starts  --1.1
      /* BEGIN
         SELECT COUNT (1)
           INTO ln_book_code
           FROM fa_book_controls
          WHERE book_type_code = rec_cur_val_tax.leg_book_type_code;
      EXCEPTION
         WHEN OTHERS
         THEN
            log_errors
               (pin_interface_txn_id         => NULL,
                piv_source_table             => 'XXFA_TAX_ASSET_STG',
                piv_source_column_name       => NULL,
                piv_source_column_value      => NULL,
                piv_source_keyname1          => 'leg_asset_number',
                piv_source_keyvalue1         => rec_cur_val_tax.leg_asset_number,
                piv_source_keyname2          => 'leg_book_type_code',
                piv_source_keyvalue2         => rec_cur_val_tax.leg_book_type_code,
                piv_error_type               => 'VAL_ERR',
                piv_error_code               => 'ETN_FA_INVALID_BOOK_TYPE',
                piv_error_message            =>    'Error : Exception occured while fetching book type code from FA_BOOK_CONTROLS : '
                                                || SUBSTR (SQLERRM, 1, 240)
               );
            lc_error_flag := 'Y';
      END;

      IF ln_book_code = 0
      THEN
         log_errors
            (pin_interface_txn_id         => NULL,
             piv_source_table             => 'XXFA_TAX_ASSET_STG',
             piv_source_column_name       => 'leg_book_type_code',
             piv_source_column_value      => rec_cur_val_tax.leg_book_type_code,
             piv_source_keyname1          => 'leg_asset_number',
             piv_source_keyvalue1         => rec_cur_val_tax.leg_asset_number,
             piv_source_keyname2          => 'leg_book_type_code',
             piv_source_keyvalue2         => rec_cur_val_tax.leg_book_type_code,
             piv_error_type               => 'VAL_ERR',
             piv_error_code               => 'ETN_FA_INVALID_BOOK_TYPE',
             piv_error_message            => 'Error : book type code not valid : '
            );
         lc_error_flag := 'Y';
      END IF; */

      --- To check date placed in service
      IF rec_cur_val_tax.leg_date_placed_in_service IS NULL THEN
        log_errors(pin_interface_txn_id    => NULL,
                   piv_source_table        => 'XXFA_TAX_ASSET_STG',
                   piv_source_column_name  => 'leg_date_placed_in_service',
                   piv_source_column_value => rec_cur_val_tax.leg_date_placed_in_service,
                   piv_source_keyname1     => 'leg_asset_number',
                   piv_source_keyvalue1    => rec_cur_val_tax.leg_asset_number,
                   piv_source_keyname2     => 'leg_book_type_code',
                   piv_source_keyvalue2    => rec_cur_val_tax.leg_book_type_code,
                   piv_error_type          => 'VAL_ERR',
                   piv_error_code          => 'ETN_FA_MANDATORY_COLUMN',
                   piv_error_message       => 'Error : Date placed in serice cannot be NULL  : ');
        lc_error_flag := 'Y';
      END IF;

      -- check for depreciation method code starts  --1.1
      /* IF rec_cur_val_tax.leg_deprn_method_code IS NULL
      THEN
         log_errors
            (pin_interface_txn_id         => NULL,
             piv_source_table             => 'XXFA_TAX_ASSET_STG',
             piv_source_column_name       => 'leg_deprn_method_code',
             piv_source_column_value      => rec_cur_val_tax.leg_deprn_method_code,
             piv_source_keyname1          => 'leg_asset_number',
             piv_source_keyvalue1         => rec_cur_val_tax.leg_asset_number,
             piv_source_keyname2          => 'leg_book_type_code',
             piv_source_keyvalue2         => rec_cur_val_tax.leg_book_type_code,
             piv_error_type               => 'VAL_ERR',
             piv_error_code               => 'ETN_FA_MANDATORY_COLUMN',
             piv_error_message            => 'Error : leg_deprn_method_code cannot be NULL'
            );
         lc_error_flag := 'Y';
      ELSE
         BEGIN
            SELECT COUNT (1)
              INTO ln_life_in_months
              FROM fa_methods
             WHERE method_code = rec_cur_val_tax.leg_deprn_method_code
               AND life_in_months = rec_cur_val_tax.leg_life_in_months;
         EXCEPTION
            WHEN OTHERS
            THEN
               log_errors
                  (pin_interface_txn_id         => NULL,
                   piv_source_table             => 'XXFA_TAX_ASSET_STG',
                   piv_source_column_name       => NULL,
                   piv_source_column_value      => NULL,
                   piv_source_keyname1          => 'leg_asset_number',
                   piv_source_keyvalue1         => rec_cur_val_tax.leg_asset_number,
                   piv_source_keyname2          => 'leg_book_type_code',
                   piv_source_keyvalue2         => rec_cur_val_tax.leg_book_type_code,
                   piv_error_type               => 'VAL_ERR',
                   piv_error_code               => 'ETN_FA_INVALID_DEPRN_METHOD',
                   piv_error_message            =>    'Error : Exception occured while fetching depreciation method code : '
                                                   || SUBSTR (SQLERRM,
                                                              1,
                                                              240
                                                             )
                  );
               lc_error_flag := 'Y';
         END;

         IF ln_life_in_months = 0
         THEN
            log_errors
               (pin_interface_txn_id         => NULL,
                piv_source_table             => 'XXFA_TAX_ASSET_STG',
                piv_source_column_name       => 'leg_deprn_method_code',
                piv_source_column_value      => rec_cur_val_tax.leg_deprn_method_code,
                piv_source_keyname1          => 'leg_asset_number',
                piv_source_keyvalue1         => rec_cur_val_tax.leg_asset_number,
                piv_source_keyname2          => 'leg_book_type_code',
                piv_source_keyvalue2         => rec_cur_val_tax.leg_book_type_code,
                piv_error_type               => 'VAL_ERR',
                piv_error_code               => 'ETN_FA_INVALID_DEPRN_METHOD',
                piv_error_message            => 'Error : Depreciation method code not valid  : '
               );
            lc_error_flag := 'Y';
         END IF;
      END IF; */

      -- check for legacy cost
      IF rec_cur_val_tax.leg_cost IS NULL THEN
        log_errors(pin_interface_txn_id    => NULL,
                   piv_source_table        => 'XXFA_TAX_ASSET_STG',
                   piv_source_column_name  => 'leg_cost',
                   piv_source_column_value => rec_cur_val_tax.leg_cost,
                   piv_source_keyname1     => 'leg_asset_number',
                   piv_source_keyvalue1    => rec_cur_val_tax.leg_asset_number,
                   piv_source_keyname2     => 'leg_book_type_code',
                   piv_source_keyvalue2    => rec_cur_val_tax.leg_book_type_code,
                   piv_error_type          => 'VAL_ERR',
                   piv_error_code          => 'ETN_FA_MANDATORY_COLUMN',
                   piv_error_message       => 'Error : leg_cost cannot be NULL');
        lc_error_flag := 'Y';
      END IF;

      -- check for legacy original cost
      IF rec_cur_val_tax.leg_original_cost IS NULL THEN
        log_errors(pin_interface_txn_id    => NULL,
                   piv_source_table        => 'XXFA_TAX_ASSET_STG',
                   piv_source_column_name  => 'leg_original_cost',
                   piv_source_column_value => rec_cur_val_tax.leg_original_cost,
                   piv_source_keyname1     => 'leg_asset_number',
                   piv_source_keyvalue1    => rec_cur_val_tax.leg_asset_number,
                   piv_source_keyname2     => 'leg_book_type_code',
                   piv_source_keyvalue2    => rec_cur_val_tax.leg_book_type_code,
                   piv_error_type          => 'VAL_ERR',
                   piv_error_code          => 'ETN_FA_MANDATORY_COLUMN',
                   piv_error_message       => 'Error : leg_cost cannot be NULL');
        lc_error_flag := 'Y';
      END IF;

      -- check for legacy salvage value
      IF rec_cur_val_tax.leg_salvage_value IS NULL THEN
        log_errors(pin_interface_txn_id    => NULL,
                   piv_source_table        => 'XXFA_TAX_ASSET_STG',
                   piv_source_column_name  => 'leg_salvage_value',
                   piv_source_column_value => rec_cur_val_tax.leg_salvage_value,
                   piv_source_keyname1     => 'leg_asset_number',
                   piv_source_keyvalue1    => rec_cur_val_tax.leg_asset_number,
                   piv_source_keyname2     => 'leg_book_type_code',
                   piv_source_keyvalue2    => rec_cur_val_tax.leg_book_type_code,
                   piv_error_type          => 'VAL_ERR',
                   piv_error_code          => 'ETN_FA_MANDATORY_COLUMN',
                   piv_error_message       => 'Error : leg_salvage_value cannot be NULL');
        lc_error_flag := 'Y';
      END IF;

      -- check for prorate convention code  --1.1
      /* IF rec_cur_val_tax.leg_prorate_convention_code IS NULL
      THEN
         log_errors
            (pin_interface_txn_id         => NULL,
             piv_source_table             => 'XXFA_TAX_ASSET_STG',
             piv_source_column_name       => 'leg_prorate_convention_code',
             piv_source_column_value      => rec_cur_val_tax.leg_prorate_convention_code,
             piv_source_keyname1          => 'leg_asset_number',
             piv_source_keyvalue1         => rec_cur_val_tax.leg_asset_number,
             piv_source_keyname2          => 'leg_book_type_code',
             piv_source_keyvalue2         => rec_cur_val_tax.leg_book_type_code,
             piv_error_type               => 'VAL_ERR',
             piv_error_code               => 'ETN_FA_MANDATORY_COLUMN',
             piv_error_message            => 'Error : leg_prorate_convention_code cannot be NULL'
            );
         lc_error_flag := 'Y';
      ELSE
         BEGIN
            SELECT COUNT (1)
              INTO ln_prorate_count
              FROM fa_convention_types
             WHERE prorate_convention_code =
                                rec_cur_val_tax.leg_prorate_convention_code;
         EXCEPTION
            WHEN OTHERS
            THEN
               log_errors
                  (pin_interface_txn_id         => NULL,
                   piv_source_table             => 'XXFA_TAX_ASSET_STG',
                   piv_source_column_name       => NULL,
                   piv_source_column_value      => NULL,
                   piv_source_keyname1          => 'leg_asset_number',
                   piv_source_keyvalue1         => rec_cur_val_tax.leg_asset_number,
                   piv_source_keyname2          => 'leg_book_type_code',
                   piv_source_keyvalue2         => rec_cur_val_tax.leg_book_type_code,
                   piv_error_type               => 'VAL_ERR',
                   piv_error_code               => 'ETN_FA_INVALID_PRORATE_CODE',
                   piv_error_message            =>    'Error : Exception occured while fetching prorate_convention_code : '
                                                   || SUBSTR (SQLERRM,
                                                              1,
                                                              240
                                                             )
                  );
               lc_error_flag := 'Y';
         END;

         IF ln_prorate_count = 0
         THEN
            log_errors
               (pin_interface_txn_id         => NULL,
                piv_source_table             => 'XXFA_TAX_ASSET_STG',
                piv_source_column_name       => 'leg_prorate_convention_code',
                piv_source_column_value      => rec_cur_val_tax.leg_prorate_convention_code,
                piv_source_keyname1          => 'leg_asset_number',
                piv_source_keyvalue1         => rec_cur_val_tax.leg_asset_number,
                piv_source_keyname2          => 'leg_book_type_code',
                piv_source_keyvalue2         => rec_cur_val_tax.leg_book_type_code,
                piv_error_type               => 'VAL_ERR',
                piv_error_code               => 'ETN_FA_INVALID_PRORATE_CODE',
                piv_error_message            => 'Error : Prorate Convention code not valid  : '
               );
            lc_error_flag := 'Y';
         END IF;
      END IF; */

      -- check for capitalize flag
      IF rec_cur_val_tax.leg_capitalize_flag IS NULL THEN
        log_errors(pin_interface_txn_id    => NULL,
                   piv_source_table        => 'XXFA_TAX_ASSET_STG',
                   piv_source_column_name  => 'leg_capitalize_flag',
                   piv_source_column_value => rec_cur_val_tax.leg_capitalize_flag,
                   piv_source_keyname1     => 'leg_asset_number',
                   piv_source_keyvalue1    => rec_cur_val_tax.leg_asset_number,
                   piv_source_keyname2     => 'leg_book_type_code',
                   piv_source_keyvalue2    => rec_cur_val_tax.leg_book_type_code,
                   piv_error_type          => 'VAL_ERR',
                   piv_error_code          => 'ETN_FA_MANDATORY_COLUMN',
                   piv_error_message       => 'Error : leg_capitalize_flag cannot be NULL');
        lc_error_flag := 'Y';
      END IF;

      -- check for depreciate flag
      IF rec_cur_val_tax.leg_depreciate_flag IS NULL THEN
        log_errors(pin_interface_txn_id    => NULL,
                   piv_source_table        => 'XXFA_TAX_ASSET_STG',
                   piv_source_column_name  => 'leg_depreciate_flag',
                   piv_source_column_value => rec_cur_val_tax.leg_depreciate_flag,
                   piv_source_keyname1     => 'leg_asset_number',
                   piv_source_keyvalue1    => rec_cur_val_tax.leg_asset_number,
                   piv_source_keyname2     => 'leg_book_type_code',
                   piv_source_keyvalue2    => rec_cur_val_tax.leg_book_type_code,
                   piv_error_type          => 'VAL_ERR',
                   piv_error_code          => 'ETN_FA_MANDATORY_COLUMN',
                   piv_error_message       => 'Error : leg_depreciate_flag cannot be NULL');
        lc_error_flag := 'Y';
      END IF;

      -- check for bonus rule
      IF rec_cur_val_tax.leg_bonus_rule IS NOT NULL THEN
        BEGIN
          SELECT COUNT(1)
            INTO ln_bonus_count
            FROM fa_bonus_rules
           WHERE bonus_rule = rec_cur_val_tax.leg_bonus_rule;
        EXCEPTION
          WHEN OTHERS THEN
            log_errors(pin_interface_txn_id    => NULL,
                       piv_source_table        => 'XXFA_TAX_ASSET_STG',
                       piv_source_column_name  => NULL,
                       piv_source_column_value => NULL,
                       piv_source_keyname1     => 'leg_asset_number',
                       piv_source_keyvalue1    => rec_cur_val_tax.leg_asset_number,
                       piv_source_keyname2     => 'leg_book_type_code',
                       piv_source_keyvalue2    => rec_cur_val_tax.leg_book_type_code,
                       piv_error_type          => 'VAL_ERR',
                       piv_error_code          => 'ETN_FA_INVALID_BONUS_RULE',
                       piv_error_message       => 'Error : Exception occured while fetching bonus_rule : ' ||
                                                  SUBSTR(SQLERRM, 1, 240));
            lc_error_flag := 'Y';
        END;

        IF ln_bonus_count = 0 THEN
          log_errors(pin_interface_txn_id    => NULL,
                     piv_source_table        => 'XXFA_TAX_ASSET_STG',
                     piv_source_column_name  => 'leg_bonus_rule',
                     piv_source_column_value => rec_cur_val_tax.leg_bonus_rule,
                     piv_source_keyname1     => 'leg_asset_number',
                     piv_source_keyvalue1    => rec_cur_val_tax.leg_asset_number,
                     piv_source_keyname2     => 'leg_book_type_code',
                     piv_source_keyvalue2    => rec_cur_val_tax.leg_book_type_code,
                     piv_error_type          => 'VAL_ERR',
                     piv_error_code          => 'ETN_FA_INVALID_BONUS_RULE',
                     piv_error_message       => 'Error : bonus_rule not valid  : ');
          lc_error_flag := 'Y';
        END IF;
      END IF;

      -- check for ceiling_name   --1.1
      /* IF rec_cur_val_tax.leg_ceiling_name IS NOT NULL
      THEN
         BEGIN
            SELECT COUNT (1)
              INTO ln_ceiling_count
              FROM fa_ceilings
             WHERE ceiling_name = rec_cur_val_tax.leg_ceiling_name;
         EXCEPTION
            WHEN OTHERS
            THEN
               log_errors
                  (pin_interface_txn_id         => NULL,
                   piv_source_table             => 'XXFA_TAX_ASSET_STG',
                   piv_source_column_name       => NULL,
                   piv_source_column_value      => NULL,
                   piv_source_keyname1          => 'leg_asset_number',
                   piv_source_keyvalue1         => rec_cur_val_tax.leg_asset_number,
                   piv_source_keyname2          => 'leg_book_type_code',
                   piv_source_keyvalue2         => rec_cur_val_tax.leg_book_type_code,
                   piv_error_type               => 'VAL_ERR',
                   piv_error_code               => 'ETN_FA_INVALID_CEILING_NAME',
                   piv_error_message            =>    'Error : Exception occured while fetching ceiling_name : '
                                                   || SUBSTR (SQLERRM,
                                                              1,
                                                              240
                                                             )
                  );
               lc_error_flag := 'Y';
         END;

         IF ln_ceiling_count = 0
         THEN
            log_errors
               (pin_interface_txn_id         => NULL,
                piv_source_table             => 'XXFA_TAX_ASSET_STG',
                piv_source_column_name       => 'leg_ceiling_name',
                piv_source_column_value      => rec_cur_val_tax.leg_ceiling_name,
                piv_source_keyname1          => 'leg_asset_number',
                piv_source_keyvalue1         => rec_cur_val_tax.leg_asset_number,
                piv_error_type               => 'VAL_ERR',
                piv_error_code               => 'ETN_FA_INVALID_CEILING_NAME',
                piv_error_message            => 'Error : ceiling_name code not valid  : '
               );
            lc_error_flag := 'Y';
         END IF;
      END IF; */

      -- check for YTD deprcn
      IF rec_cur_val_tax.leg_ytd_deprn IS NULL THEN
        log_errors(pin_interface_txn_id    => NULL,
                   piv_source_table        => 'XXFA_TAX_ASSET_STG',
                   piv_source_column_name  => 'leg_ytd_deprn',
                   piv_source_column_value => rec_cur_val_tax.leg_ytd_deprn,
                   piv_source_keyname1     => 'leg_asset_number',
                   piv_source_keyvalue1    => rec_cur_val_tax.leg_asset_number,
                   piv_source_keyname2     => 'leg_book_type_code',
                   piv_source_keyvalue2    => rec_cur_val_tax.leg_book_type_code,
                   piv_error_type          => 'VAL_ERR',
                   piv_error_code          => 'ETN_FA_MANDATORY_COLUMN',
                   piv_error_message       => 'Error : leg_ytd_deprn cannot be NULL');
        lc_error_flag := 'Y';
      ELSE
        IF (rec_cur_val_tax.leg_cost < 0) AND
           (rec_cur_val_tax.leg_recoverable_cost < 0) AND
           (rec_cur_val_tax.leg_ytd_deprn <= 0) AND
           (rec_cur_val_tax.leg_deprn_reserve <= 0) THEN
          IF ABS(rec_cur_val_tax.leg_ytd_deprn) >
             ABS(rec_cur_val_tax.leg_deprn_reserve) THEN
            log_errors(pin_interface_txn_id    => NULL,
                       piv_source_table        => 'XXFA_CORP_ASSET_STG',
                       piv_source_column_name  => 'leg_ytd_deprn',
                       piv_source_column_value => rec_cur_val_tax.leg_ytd_deprn,
                       piv_source_keyname1     => 'leg_asset_number',
                       piv_source_keyvalue1    => rec_cur_val_tax.leg_asset_number,
                       piv_source_keyname2     => 'leg_book_type_code',
                       piv_source_keyvalue2    => rec_cur_val_tax.leg_book_type_code,
                       piv_error_type          => 'VAL_ERR',
                       piv_error_code          => 'ETN_FA_INVALID_YTD_DERPN',
                       piv_error_message       => 'Error : leg_ytd_deprn cannot be greater than leg_deprn_reserve');
            lc_error_flag := 'Y';
          END IF;
        ELSE
          IF rec_cur_val_tax.leg_ytd_deprn >
             rec_cur_val_tax.leg_deprn_reserve THEN
            log_errors(pin_interface_txn_id    => NULL,
                       piv_source_table        => 'XXFA_CORP_ASSET_STG',
                       piv_source_column_name  => 'leg_ytd_deprn',
                       piv_source_column_value => rec_cur_val_tax.leg_ytd_deprn,
                       piv_source_keyname1     => 'leg_asset_number',
                       piv_source_keyvalue1    => rec_cur_val_tax.leg_asset_number,
                       piv_source_keyname2     => 'leg_book_type_code',
                       piv_source_keyvalue2    => rec_cur_val_tax.leg_book_type_code,
                       piv_error_type          => 'VAL_ERR',
                       piv_error_code          => 'ETN_FA_INVALID_YTD_DERPN',
                       piv_error_message       => 'Error : leg_ytd_deprn cannot be greater than leg_deprn_reserve');
            lc_error_flag := 'Y';
          END IF;
        END IF;
      END IF;

      -- check for deprn reserve
      IF rec_cur_val_tax.leg_deprn_reserve IS NULL THEN
        log_errors(pin_interface_txn_id    => NULL,
                   piv_source_table        => 'XXFA_TAX_ASSET_STG',
                   piv_source_column_name  => 'leg_deprn_reserve',
                   piv_source_column_value => rec_cur_val_tax.leg_deprn_reserve,
                   piv_source_keyname1     => 'leg_asset_number',
                   piv_source_keyvalue1    => rec_cur_val_tax.leg_asset_number,
                   piv_source_keyname2     => 'leg_book_type_code',
                   piv_source_keyvalue2    => rec_cur_val_tax.leg_book_type_code,
                   piv_error_type          => 'VAL_ERR',
                   piv_error_code          => 'ETN_FA_MANDATORY_COLUMN',
                   piv_error_message       => 'Error : leg_deprn_reserve cannot be NULL');
        lc_error_flag := 'Y';
      END IF;

      --assigning DFF segments
      IF (rec_cur_val_tax.leg_adtn_context = 'France' OR
         rec_cur_val_tax.leg_adtn_context = 'Germany' OR
         rec_cur_val_tax.leg_adtn_context = 'Holland' OR
         rec_cur_val_tax.leg_adtn_context = 'Italy' OR
         rec_cur_val_tax.leg_adtn_context = 'Mexico' OR
         rec_cur_val_tax.leg_adtn_context = 'Monaco' OR
         rec_cur_val_tax.leg_adtn_context = 'Spain' OR
         rec_cur_val_tax.leg_adtn_context = 'United Kingdom') AND
         rec_cur_val_tax.leg_source_system = g_source_issc THEN
        lv_attribute1 := rec_cur_val_tax.leg_adtn_attribute1;
      END IF;

      IF rec_cur_val_tax.leg_source_system = g_source_issc THEN
        lv_attribute3  := rec_cur_val_tax.leg_adtn_attribute26;
        lv_attribute4  := rec_cur_val_tax.leg_adtn_attribute27;
        lv_attribute17 := rec_cur_val_tax.leg_adtn_attribute28;
        lv_attribute16 := rec_cur_val_tax.leg_adtn_attribute16;
      ELSIF rec_cur_val_tax.leg_source_system = g_source_fsc THEN
        lv_attribute3  := rec_cur_val_tax.leg_adtn_attribute3;
        lv_attribute4  := rec_cur_val_tax.leg_adtn_attribute4;
        lv_attribute17 := rec_cur_val_tax.leg_adtn_attribute10;
        lv_attribute16 := rec_cur_val_tax.leg_adtn_attribute15;
        lv_attribute7  := rec_cur_val_tax.leg_adtn_attribute7;
        lv_attribute8  := rec_cur_val_tax.leg_adtn_attribute8;
        lv_attribute5  := rec_cur_val_tax.leg_adtn_attribute12;
        lv_attribute29 := rec_cur_val_tax.leg_adtn_attribute29;
        lv_attribute6  := rec_cur_val_tax.leg_adtn_attribute13;
        lv_attribute26 := rec_cur_val_tax.leg_adtn_attribute26;
        lv_attribute27 := rec_cur_val_tax.leg_adtn_attribute27;
        lv_attribute18 := rec_cur_val_tax.leg_adtn_attribute14;
        lv_attribute9  := rec_cur_val_tax.leg_adtn_attribute9;
        lv_attribute22 := rec_cur_val_tax.leg_adtn_attribute22;
        lv_attribute23 := rec_cur_val_tax.leg_adtn_attribute23;
      END IF;

      IF rec_cur_val_tax.leg_source_system = g_source_issc THEN
        IF rec_cur_val_tax.leg_adtn_context = 'Taiwan' THEN
          lv_attribute10 := rec_cur_val_tax.leg_adtn_attribute10;
          lv_attribute11 := rec_cur_val_tax.leg_adtn_attribute11;
          lv_attribute12 := rec_cur_val_tax.leg_adtn_attribute12;
          lv_attribute13 := rec_cur_val_tax.leg_adtn_attribute13;
          lv_attribute14 := rec_cur_val_tax.leg_adtn_attribute14;
          lv_attribute15 := rec_cur_val_tax.leg_adtn_attribute15;
        ELSIF rec_cur_val_tax.leg_adtn_context = 'Poland' THEN
          lv_attribute30 := rec_cur_val_tax.leg_adtn_attribute30;
          lv_attribute2  := rec_cur_val_tax.leg_adtn_attribute3;
        END IF;
      END IF;

      lv_adn_cont           := rec_cur_val_tax.leg_adtn_context;
      lv_adn_cat_code       := rec_cur_val_tax.leg_adtn_attr_category_code;
      lv_adn_cont           := rec_cur_val_tax.leg_adtn_context;
      lv_adn_gl_attribute1  := rec_cur_val_tax.leg_adtn_global_attribute1;
      lv_adn_gl_attribute2  := rec_cur_val_tax.leg_adtn_global_attribute2;
      lv_adn_gl_attribute3  := rec_cur_val_tax.leg_adtn_global_attribute3;
      lv_adn_gl_attribute4  := rec_cur_val_tax.leg_adtn_global_attribute4;
      lv_adn_gl_attribute5  := rec_cur_val_tax.leg_adtn_global_attribute5;
      lv_adn_gl_attribute6  := rec_cur_val_tax.leg_adtn_global_attribute6;
      lv_adn_gl_attribute7  := rec_cur_val_tax.leg_adtn_global_attribute7;
      lv_adn_gl_attribute8  := rec_cur_val_tax.leg_adtn_global_attribute8;
      lv_adn_gl_attribute9  := rec_cur_val_tax.leg_adtn_global_attribute9;
      lv_adn_gl_attribute10 := rec_cur_val_tax.leg_adtn_global_attribute10;
      lv_adn_gl_attribute11 := rec_cur_val_tax.leg_adtn_global_attribute11;
      lv_adn_gl_attribute12 := rec_cur_val_tax.leg_adtn_global_attribute12;
      lv_adn_gl_attribute13 := rec_cur_val_tax.leg_adtn_global_attribute13;
      lv_adn_gl_attribute14 := rec_cur_val_tax.leg_adtn_global_attribute14;
      lv_adn_gl_attribute15 := rec_cur_val_tax.leg_adtn_global_attribute15;
      lv_adn_gl_attribute16 := rec_cur_val_tax.leg_adtn_global_attribute16;
      lv_adn_gl_attribute17 := rec_cur_val_tax.leg_adtn_global_attribute17;
      lv_adn_gl_attribute18 := rec_cur_val_tax.leg_adtn_global_attribute18;
      lv_adn_gl_attribute19 := rec_cur_val_tax.leg_adtn_global_attribute19;
      lv_adn_gl_attribute20 := rec_cur_val_tax.leg_adtn_global_attribute20;
      lv_adn_gl_cat         := rec_cur_val_tax.leg_adtn_global_attr_category;
      lv_bk_gl_attribute1   := rec_cur_val_tax.leg_books_global_attribute1;
      lv_bk_gl_attribute2   := rec_cur_val_tax.leg_books_global_attribute2;
      lv_bk_gl_attribute3   := rec_cur_val_tax.leg_books_global_attribute3;
      lv_bk_gl_attribute4   := rec_cur_val_tax.leg_books_global_attribute4;
      lv_bk_gl_attribute5   := rec_cur_val_tax.leg_books_global_attribute5;
      lv_bk_gl_attribute6   := rec_cur_val_tax.leg_books_global_attribute6;
      lv_bk_gl_attribute7   := rec_cur_val_tax.leg_books_global_attribute7;
      lv_bk_gl_attribute8   := rec_cur_val_tax.leg_books_global_attribute8;
      lv_bk_gl_attribute9   := rec_cur_val_tax.leg_books_global_attribute9;
      lv_bk_gl_attribute10  := rec_cur_val_tax.leg_books_global_attribute10;
      lv_bk_gl_attribute11  := rec_cur_val_tax.leg_books_global_attribute11;
      lv_bk_gl_attribute12  := rec_cur_val_tax.leg_books_global_attribute12;
      lv_bk_gl_attribute13  := rec_cur_val_tax.leg_books_global_attribute13;
      lv_bk_gl_attribute14  := rec_cur_val_tax.leg_books_global_attribute14;
      lv_bk_gl_attribute15  := rec_cur_val_tax.leg_books_global_attribute15;
      lv_bk_gl_attribute16  := rec_cur_val_tax.leg_books_global_attribute16;
      lv_bk_gl_attribute17  := rec_cur_val_tax.leg_books_global_attribute17;
      lv_bk_gl_attribute18  := rec_cur_val_tax.leg_books_global_attribute18;
      lv_bk_gl_attribute19  := rec_cur_val_tax.leg_books_global_attribute19;
      lv_bk_gl_attribute20  := rec_cur_val_tax.leg_books_global_attribute20;
      lv_bk_gl_att_cat      := rec_cur_val_tax.leg_books_global_attr_category;

      IF lc_error_flag = 'Y' OR rec_cur_val_tax.process_flag = 'E' --1.1
       THEN
        g_ret_code := 1;

        BEGIN
          UPDATE xxfa_tax_asset_stg
             SET process_flag                   = 'E',
                 ERROR_TYPE                     = 'VAL_ERR',
                 asset_category_id              = ln_category_id,
                 asset_cat_segment1             = lc_cat_segment1,
                 asset_cat_segment2             = lc_cat_segment2,
                 request_id                     = g_request_id,
                 last_updated_date              = SYSDATE,
                 last_updated_by                = g_last_updated_by,
                 last_update_login              = g_last_update_login,
                 adtn_attribute1                = lv_attribute1,
                 adtn_attribute2                = lv_attribute2,
                 adtn_attribute3                = lv_attribute3,
                 adtn_attribute4                = lv_attribute4,
                 adtn_attribute5                = lv_attribute5,
                 adtn_attribute6                = lv_attribute6,
                 adtn_attribute7                = lv_attribute7,
                 adtn_attribute8                = lv_attribute8,
                 adtn_attribute9                = lv_attribute9,
                 adtn_attribute10               = lv_attribute10,
                 adtn_attribute11               = lv_attribute11,
                 adtn_attribute12               = lv_attribute12,
                 adtn_attribute13               = lv_attribute13,
                 adtn_attribute14               = lv_attribute14,
                 adtn_attribute15               = lv_attribute15,
                 adtn_attribute16               = lv_attribute16,
                 adtn_attribute17               = lv_attribute17,
                 adtn_attribute18               = lv_attribute18,
                 adtn_attribute19               = lv_attribute19,
                 adtn_attribute20               = lv_attribute20,
                 adtn_attribute21               = lv_attribute21,
                 adtn_attribute22               = lv_attribute22,
                 adtn_attribute23               = lv_attribute23,
                 adtn_attribute24               = lv_attribute24,
                 adtn_attribute25               = lv_attribute25,
                 adtn_attribute26               = lv_attribute26,
                 adtn_attribute27               = lv_attribute27,
                 adtn_attribute28               = lv_attribute28,
                 adtn_attribute29               = lv_attribute29,
                 adtn_attribute30               = lv_attribute30,
                 adtn_attribute_category_code   = lv_adn_cat_code,
                 adtn_context                   = lv_adn_cont,
                 adtn_global_attribute1         = lv_adn_gl_attribute1,
                 adtn_global_attribute2         = lv_adn_gl_attribute2,
                 adtn_global_attribute3         = lv_adn_gl_attribute3,
                 adtn_global_attribute4         = lv_adn_gl_attribute4,
                 adtn_global_attribute5         = lv_adn_gl_attribute5,
                 adtn_global_attribute6         = lv_adn_gl_attribute6,
                 adtn_global_attribute7         = lv_adn_gl_attribute7,
                 adtn_global_attribute8         = lv_adn_gl_attribute8,
                 adtn_global_attribute9         = lv_adn_gl_attribute9,
                 adtn_global_attribute10        = lv_adn_gl_attribute10,
                 adtn_global_attribute11        = lv_adn_gl_attribute11,
                 adtn_global_attribute12        = lv_adn_gl_attribute12,
                 adtn_global_attribute13        = lv_adn_gl_attribute13,
                 adtn_global_attribute14        = lv_adn_gl_attribute14,
                 adtn_global_attribute15        = lv_adn_gl_attribute15,
                 adtn_global_attribute16        = lv_adn_gl_attribute16,
                 adtn_global_attribute17        = lv_adn_gl_attribute17,
                 adtn_global_attribute18        = lv_adn_gl_attribute18,
                 adtn_global_attribute19        = lv_adn_gl_attribute19,
                 adtn_global_attribute20        = lv_adn_gl_attribute20,
                 adtn_global_attribute_category = lv_adn_gl_cat,
                 books_global_attribute1        = lv_bk_gl_attribute1,
                 books_global_attribute2        = lv_bk_gl_attribute2,
                 books_global_attribute3        = lv_bk_gl_attribute3,
                 books_global_attribute4        = lv_bk_gl_attribute4,
                 books_global_attribute5        = lv_bk_gl_attribute5,
                 books_global_attribute6        = lv_bk_gl_attribute6,
                 books_global_attribute7        = lv_bk_gl_attribute7,
                 books_global_attribute8        = lv_bk_gl_attribute8,
                 books_global_attribute9        = lv_bk_gl_attribute9,
                 books_global_attribute10       = lv_bk_gl_attribute10,
                 books_global_attribute11       = lv_bk_gl_attribute11,
                 books_global_attribute12       = lv_bk_gl_attribute12,
                 books_global_attribute13       = lv_bk_gl_attribute13,
                 books_global_attribute14       = lv_bk_gl_attribute14,
                 books_global_attribute15       = lv_bk_gl_attribute15,
                 books_global_attribute16       = lv_bk_gl_attribute16,
                 books_global_attribute17       = lv_bk_gl_attribute17,
                 books_global_attribute18       = lv_bk_gl_attribute18,
                 books_global_attribute19       = lv_bk_gl_attribute19,
                 books_global_attribute20       = lv_bk_gl_attribute20,
                 books_global_attr_category     = lv_bk_gl_att_cat
           WHERE leg_asset_number = rec_cur_val_tax.leg_asset_number
             AND leg_book_type_code = rec_cur_val_tax.leg_book_type_code
             AND process_flag = 'N'
             AND batch_id = g_batch_id
             AND run_sequence_id = g_new_run_seq_id;
        EXCEPTION
          WHEN OTHERS THEN
            log_errors(pin_interface_txn_id    => NULL,
                       piv_source_table        => 'XXFA_TAX_ASSET_STG',
                       piv_source_column_name  => NULL,
                       piv_source_column_value => NULL,
                       piv_source_keyname1     => 'leg_asset_number',
                       piv_source_keyvalue1    => rec_cur_val_tax.leg_asset_number,
                       piv_source_keyname2     => 'leg_book_type_code',
                       piv_source_keyvalue2    => rec_cur_val_tax.leg_book_type_code,
                       piv_error_type          => 'VAL_ERR',
                       piv_error_code          => 'ETN_FA_INVALID_PROC',
                       piv_error_message       => 'Error : Exception occured while updating XXFA_TAX_ASSET_STG for errors  : ' ||
                                                  SUBSTR(SQLERRM, 1, 240));
            lc_error_flag := 'Y';
        END;
      ELSE
        BEGIN
          UPDATE xxfa_tax_asset_stg
             SET process_flag                   = 'V',
                 asset_category_id              = ln_category_id,
                 asset_cat_segment1             = lc_cat_segment1,
                 asset_cat_segment2             = lc_cat_segment2,
                 request_id                     = g_request_id,
                 last_updated_date              = SYSDATE,
                 last_updated_by                = g_last_updated_by,
                 last_update_login              = g_last_update_login,
                 adtn_attribute1                = lv_attribute1,
                 adtn_attribute2                = lv_attribute2,
                 adtn_attribute3                = lv_attribute3,
                 adtn_attribute4                = lv_attribute4,
                 adtn_attribute5                = lv_attribute5,
                 adtn_attribute6                = lv_attribute6,
                 adtn_attribute7                = lv_attribute7,
                 adtn_attribute8                = lv_attribute8,
                 adtn_attribute9                = lv_attribute9,
                 adtn_attribute10               = lv_attribute10,
                 adtn_attribute11               = lv_attribute11,
                 adtn_attribute12               = lv_attribute12,
                 adtn_attribute13               = lv_attribute13,
                 adtn_attribute14               = lv_attribute14,
                 adtn_attribute15               = lv_attribute15,
                 adtn_attribute16               = lv_attribute16,
                 adtn_attribute17               = lv_attribute17,
                 adtn_attribute18               = lv_attribute18,
                 adtn_attribute19               = lv_attribute19,
                 adtn_attribute20               = lv_attribute20,
                 adtn_attribute21               = lv_attribute21,
                 adtn_attribute22               = lv_attribute22,
                 adtn_attribute23               = lv_attribute23,
                 adtn_attribute24               = lv_attribute24,
                 adtn_attribute25               = lv_attribute25,
                 adtn_attribute26               = lv_attribute26,
                 adtn_attribute27               = lv_attribute27,
                 adtn_attribute28               = lv_attribute28,
                 adtn_attribute29               = lv_attribute29,
                 adtn_attribute30               = lv_attribute30,
                 adtn_attribute_category_code   = lv_adn_cat_code,
                 adtn_context                   = lv_adn_cont,
                 adtn_global_attribute1         = lv_adn_gl_attribute1,
                 adtn_global_attribute2         = lv_adn_gl_attribute2,
                 adtn_global_attribute3         = lv_adn_gl_attribute3,
                 adtn_global_attribute4         = lv_adn_gl_attribute4,
                 adtn_global_attribute5         = lv_adn_gl_attribute5,
                 adtn_global_attribute6         = lv_adn_gl_attribute6,
                 adtn_global_attribute7         = lv_adn_gl_attribute7,
                 adtn_global_attribute8         = lv_adn_gl_attribute8,
                 adtn_global_attribute9         = lv_adn_gl_attribute9,
                 adtn_global_attribute10        = lv_adn_gl_attribute10,
                 adtn_global_attribute11        = lv_adn_gl_attribute11,
                 adtn_global_attribute12        = lv_adn_gl_attribute12,
                 adtn_global_attribute13        = lv_adn_gl_attribute13,
                 adtn_global_attribute14        = lv_adn_gl_attribute14,
                 adtn_global_attribute15        = lv_adn_gl_attribute15,
                 adtn_global_attribute16        = lv_adn_gl_attribute16,
                 adtn_global_attribute17        = lv_adn_gl_attribute17,
                 adtn_global_attribute18        = lv_adn_gl_attribute18,
                 adtn_global_attribute19        = lv_adn_gl_attribute19,
                 adtn_global_attribute20        = lv_adn_gl_attribute20,
                 adtn_global_attribute_category = lv_adn_gl_cat,
                 books_global_attribute1        = lv_bk_gl_attribute1,
                 books_global_attribute2        = lv_bk_gl_attribute2,
                 books_global_attribute3        = lv_bk_gl_attribute3,
                 books_global_attribute4        = lv_bk_gl_attribute4,
                 books_global_attribute5        = lv_bk_gl_attribute5,
                 books_global_attribute6        = lv_bk_gl_attribute6,
                 books_global_attribute7        = lv_bk_gl_attribute7,
                 books_global_attribute8        = lv_bk_gl_attribute8,
                 books_global_attribute9        = lv_bk_gl_attribute9,
                 books_global_attribute10       = lv_bk_gl_attribute10,
                 books_global_attribute11       = lv_bk_gl_attribute11,
                 books_global_attribute12       = lv_bk_gl_attribute12,
                 books_global_attribute13       = lv_bk_gl_attribute13,
                 books_global_attribute14       = lv_bk_gl_attribute14,
                 books_global_attribute15       = lv_bk_gl_attribute15,
                 books_global_attribute16       = lv_bk_gl_attribute16,
                 books_global_attribute17       = lv_bk_gl_attribute17,
                 books_global_attribute18       = lv_bk_gl_attribute18,
                 books_global_attribute19       = lv_bk_gl_attribute19,
                 books_global_attribute20       = lv_bk_gl_attribute20,
                 books_global_attr_category     = lv_bk_gl_att_cat
           WHERE leg_asset_number = rec_cur_val_tax.leg_asset_number
             AND leg_book_type_code = rec_cur_val_tax.leg_book_type_code
             AND process_flag = 'N'
             AND batch_id = g_batch_id
             AND run_sequence_id = g_new_run_seq_id;
        EXCEPTION
          WHEN OTHERS THEN
            log_errors(pin_interface_txn_id    => NULL,
                       piv_source_table        => 'XXFA_TAX_ASSET_STG',
                       piv_source_column_name  => NULL,
                       piv_source_column_value => NULL,
                       piv_source_keyname1     => 'leg_asset_number',
                       piv_source_keyvalue1    => rec_cur_val_tax.leg_asset_number,
                       piv_source_keyname2     => 'leg_book_type_code',
                       piv_source_keyvalue2    => rec_cur_val_tax.leg_book_type_code,
                       piv_error_type          => 'VAL_ERR',
                       piv_error_code          => 'ETN_FA_INVALID_PROC',
                       piv_error_message       => 'Error : Exception occured while updating XXFA_TAX_ASSET_STG for Validation  : ' ||
                                                  SUBSTR(SQLERRM, 1, 240));
            lc_error_flag := 'Y';
        END;
      END IF;

      IF l_count >= 100 THEN
        l_count := 0;
        xxetn_debug_pkg.add_debug('
                        Performing Batch Commit');
        COMMIT;
      ELSE
        l_count := l_count + 1;
      END IF;
    END LOOP;

    xxetn_debug_pkg.add_debug(piv_debug_msg => 'Validation for assets with Tax book ends : ');
    COMMIT;
  EXCEPTION
    WHEN OTHERS THEN
      g_ret_code := 2;
      print_log_message('Error : Exception occured while validating tax book records  : ' ||
                        SUBSTR(SQLERRM, 1, 240));
      print_log_message('Error : Backtrace in validation for Tax : ' ||
                        DBMS_UTILITY.format_error_backtrace);
  END validate_tax;

  --
  -- ========================
  -- Procedure: IMPORT_CORPORATE
  -- =====================================================================================
  --   This procedure is used to import the assets with corporate books
  -- =====================================================================================
  PROCEDURE import_corporate IS
    CURSOR cur_conv_parent IS
      SELECT DISTINCT leg_source_system,
                      leg_asset_id,
                      asset_id,
                      leg_asset_number,
                      leg_parent_asset_number,
                      leg_current_units,
                      leg_asset_type,
                      leg_tag_number,
                      leg_asset_cat_segment1,
                      leg_asset_cat_segment2,
                      asset_cat_segment1,
                      asset_cat_segment2,
                      asset_category_id,
                      parent_asset_id,
                      leg_manufacturer_name,
                      leg_serial_number,
                      leg_model_number,
                      leg_property_type_code,
                      leg_property_1245_1250_code,
                      leg_in_use_flag,
                      leg_owned_leased,
                      leg_new_used,
                      leg_unit_adjustment_flag,
                      leg_add_cost_je_flag,
                      leg_adtn_attribute1,
                      leg_adtn_attribute2,
                      leg_adtn_attribute3,
                      leg_adtn_attribute4,
                      leg_adtn_attribute5,
                      leg_adtn_attribute6,
                      leg_adtn_attribute7,
                      leg_adtn_attribute8,
                      leg_adtn_attribute9,
                      leg_adtn_attribute10,
                      leg_adtn_attribute11,
                      leg_adtn_attribute12,
                      leg_adtn_attribute13,
                      leg_adtn_attribute14,
                      leg_adtn_attribute15,
                      leg_adtn_attribute16,
                      leg_adtn_attribute17,
                      leg_adtn_attribute18,
                      leg_adtn_attribute19,
                      leg_adtn_attribute20,
                      leg_adtn_attribute21,
                      leg_adtn_attribute22,
                      leg_adtn_attribute23,
                      leg_adtn_attribute24,
                      leg_adtn_attribute25,
                      leg_adtn_attribute26,
                      leg_adtn_attribute27,
                      leg_adtn_attribute28,
                      leg_adtn_attribute29,
                      leg_adtn_attribute30,
                      leg_adtn_attr_category_code,
                      leg_adtn_context,
                      adtn_attribute1,
                      adtn_attribute2,
                      adtn_attribute3,
                      adtn_attribute4,
                      adtn_attribute5,
                      adtn_attribute6,
                      adtn_attribute7,
                      adtn_attribute8,
                      adtn_attribute9,
                      adtn_attribute10,
                      adtn_attribute11,
                      adtn_attribute12,
                      adtn_attribute13,
                      adtn_attribute14,
                      adtn_attribute15,
                      adtn_attribute16,
                      adtn_attribute17,
                      adtn_attribute18,
                      adtn_attribute19,
                      adtn_attribute20,
                      adtn_attribute21,
                      adtn_attribute22,
                      adtn_attribute23,
                      adtn_attribute24,
                      adtn_attribute25,
                      adtn_attribute26,
                      adtn_attribute27,
                      adtn_attribute28,
                      adtn_attribute29,
                      adtn_attribute30,
                      adtn_attribute_category_code,
                      adtn_context,
                      leg_inventorial,
                      leg_commitment,
                      leg_investment_law,
                      leg_adtn_global_attribute1,
                      leg_adtn_global_attribute2,
                      leg_adtn_global_attribute3,
                      leg_adtn_global_attribute4,
                      leg_adtn_global_attribute5,
                      leg_adtn_global_attribute6,
                      leg_adtn_global_attribute7,
                      leg_adtn_global_attribute8,
                      leg_adtn_global_attribute9,
                      leg_adtn_global_attribute10,
                      leg_adtn_global_attribute11,
                      leg_adtn_global_attribute12,
                      leg_adtn_global_attribute13,
                      leg_adtn_global_attribute14,
                      leg_adtn_global_attribute15,
                      leg_adtn_global_attribute16,
                      leg_adtn_global_attribute17,
                      leg_adtn_global_attribute18,
                      leg_adtn_global_attribute19,
                      leg_adtn_global_attribute20,
                      leg_adtn_global_attr_category,
                      adtn_global_attribute1,
                      adtn_global_attribute2,
                      adtn_global_attribute3,
                      adtn_global_attribute4,
                      adtn_global_attribute5,
                      adtn_global_attribute6,
                      adtn_global_attribute7,
                      adtn_global_attribute8,
                      adtn_global_attribute9,
                      adtn_global_attribute10,
                      adtn_global_attribute11,
                      adtn_global_attribute12,
                      adtn_global_attribute13,
                      adtn_global_attribute14,
                      adtn_global_attribute15,
                      adtn_global_attribute16,
                      adtn_global_attribute17,
                      adtn_global_attribute18,
                      adtn_global_attribute19,
                      adtn_global_attribute20,
                      adtn_global_attribute_category,
                      leg_book_type_code, --harjinder singh
                      book_type_code,
                      leg_date_placed_in_service,
                      leg_deprn_start_date,
                      leg_deprn_method_code,
                      --   leg_life_in_months,
                      (CASE
                        WHEN leg_basic_rate IS NOT NULL THEN
                         NULL
                        ELSE
                         leg_life_in_months
                      END) leg_life_in_months,
                      leg_rate_adjustment_factor,
                      leg_adjusted_cost,
                      leg_cost,
                      leg_original_cost,
                      leg_salvage_value,
                      leg_prorate_convention_code,
                      leg_prorate_date,
                      leg_cost_change_flag,
                      leg_adjustment_required_status,
                      leg_capitalize_flag,
                      leg_retirement_pending_flag,
                      DECODE(leg_depreciate_flag,'Y','YES','N','NO',leg_depreciate_flag) leg_depreciate_flag,  -- ADDED DECODE CLAUSE
                      leg_basic_rate,
                      leg_adjusted_rate,
                      leg_bonus_rule,
                      leg_ceiling_name,
                      leg_recoverable_cost,
                      leg_cap_period_name,
                      period_counter_capitalized,
                      leg_dep_period_name,
                      period_counter_fully_reserved,
                      leg_unrevalued_cost,
                      leg_annual_deprn_rounding_flag,
                      leg_percent_salvage_value,
                      leg_allowed_deprn_limit,
                      leg_allowed_deprn_limit_amount,
                      leg_salvage_type,
                      leg_deprn_limit_type,
                      leg_period_counter,
                      leg_deprn_source_code,
                      leg_deprn_run_date,
                      leg_deprn_amount,
                      leg_ytd_deprn,
                      leg_deprn_reserve,
                      leg_description,
                      leg_transaction_type_code,
                      leg_transaction_date_entered,
                      leg_transaction_subtype,
                      leg_amortization_start_date,
                      leg_books_global_attribute1,
                      leg_books_global_attribute2,
                      leg_books_global_attribute3,
                      leg_books_global_attribute4,
                      leg_books_global_attribute5,
                      leg_books_global_attribute6,
                      leg_books_global_attribute7,
                      leg_books_global_attribute8,
                      leg_books_global_attribute9,
                      leg_books_global_attribute10,
                      leg_books_global_attribute11,
                      leg_books_global_attribute12,
                      leg_books_global_attribute13,
                      leg_books_global_attribute14,
                      leg_books_global_attribute15,
                      leg_books_global_attribute16,
                      leg_books_global_attribute17,
                      leg_books_global_attribute18,
                      leg_books_global_attribute19,
                      leg_books_global_attribute20,
                      books_global_attribute1,
                      books_global_attribute2,
                      books_global_attribute3,
                      books_global_attribute4,
                      books_global_attribute5,
                      books_global_attribute6,
                      books_global_attribute7,
                      books_global_attribute8,
                      books_global_attribute9,
                      books_global_attribute10,
                      books_global_attribute11,
                      books_global_attribute12,
                      books_global_attribute13,
                      books_global_attribute14,
                      books_global_attribute15,
                      books_global_attribute16,
                      books_global_attribute17,
                      books_global_attribute18,
                      books_global_attribute19,
                      books_global_attribute20,
                      books_global_attr_category,
                      leg_books_global_attr_category,
                      batch_id,
                      process_flag,
                      run_sequence_id
        FROM xxfa_corp_asset_stg
       WHERE leg_parent_asset_number IS NULL
        AND leg_source_system in ('ISSC','FSC')   ---added to get the data only for Global Conversion
         AND process_flag = 'V'
         AND batch_id = g_batch_id;

    -- AND run_sequence_id = g_new_run_seq_id;
    CURSOR cur_conv_child IS
      SELECT DISTINCT leg_source_system,
                      leg_asset_id,
                      asset_id,
                      leg_asset_number,
                      leg_current_units,
                      leg_asset_type,
                      leg_tag_number,
                      leg_asset_cat_segment1,
                      leg_asset_cat_segment2,
                      asset_cat_segment1,
                      asset_cat_segment2,
                      asset_category_id,
                      leg_parent_asset_number,
                      parent_asset_id,
                      leg_manufacturer_name,
                      leg_serial_number,
                      leg_model_number,
                      leg_property_type_code,
                      leg_property_1245_1250_code,
                      leg_in_use_flag,
                      leg_owned_leased,
                      leg_new_used,
                      leg_unit_adjustment_flag,
                      leg_add_cost_je_flag,
                      leg_adtn_attribute1,
                      leg_adtn_attribute2,
                      leg_adtn_attribute3,
                      leg_adtn_attribute4,
                      leg_adtn_attribute5,
                      leg_adtn_attribute6,
                      leg_adtn_attribute7,
                      leg_adtn_attribute8,
                      leg_adtn_attribute9,
                      leg_adtn_attribute10,
                      leg_adtn_attribute11,
                      leg_adtn_attribute12,
                      leg_adtn_attribute13,
                      leg_adtn_attribute14,
                      leg_adtn_attribute15,
                      leg_adtn_attribute16,
                      leg_adtn_attribute17,
                      leg_adtn_attribute18,
                      leg_adtn_attribute19,
                      leg_adtn_attribute20,
                      leg_adtn_attribute21,
                      leg_adtn_attribute22,
                      leg_adtn_attribute23,
                      leg_adtn_attribute24,
                      leg_adtn_attribute25,
                      leg_adtn_attribute26,
                      leg_adtn_attribute27,
                      leg_adtn_attribute28,
                      leg_adtn_attribute29,
                      leg_adtn_attribute30,
                      leg_adtn_attr_category_code,
                      leg_adtn_context,
                      adtn_attribute1,
                      adtn_attribute2,
                      adtn_attribute3,
                      adtn_attribute4,
                      adtn_attribute5,
                      adtn_attribute6,
                      adtn_attribute7,
                      adtn_attribute8,
                      adtn_attribute9,
                      adtn_attribute10,
                      adtn_attribute11,
                      adtn_attribute12,
                      adtn_attribute13,
                      adtn_attribute14,
                      adtn_attribute15,
                      adtn_attribute16,
                      adtn_attribute17,
                      adtn_attribute18,
                      adtn_attribute19,
                      adtn_attribute20,
                      adtn_attribute21,
                      adtn_attribute22,
                      adtn_attribute23,
                      adtn_attribute24,
                      adtn_attribute25,
                      adtn_attribute26,
                      adtn_attribute27,
                      adtn_attribute28,
                      adtn_attribute29,
                      adtn_attribute30,
                      adtn_attribute_category_code,
                      adtn_context,
                      leg_inventorial,
                      leg_commitment,
                      leg_investment_law,
                      leg_adtn_global_attribute1,
                      leg_adtn_global_attribute2,
                      leg_adtn_global_attribute3,
                      leg_adtn_global_attribute4,
                      leg_adtn_global_attribute5,
                      leg_adtn_global_attribute6,
                      leg_adtn_global_attribute7,
                      leg_adtn_global_attribute8,
                      leg_adtn_global_attribute9,
                      leg_adtn_global_attribute10,
                      leg_adtn_global_attribute11,
                      leg_adtn_global_attribute12,
                      leg_adtn_global_attribute13,
                      leg_adtn_global_attribute14,
                      leg_adtn_global_attribute15,
                      leg_adtn_global_attribute16,
                      leg_adtn_global_attribute17,
                      leg_adtn_global_attribute18,
                      leg_adtn_global_attribute19,
                      leg_adtn_global_attribute20,
                      leg_adtn_global_attr_category,
                      adtn_global_attribute1,
                      adtn_global_attribute2,
                      adtn_global_attribute3,
                      adtn_global_attribute4,
                      adtn_global_attribute5,
                      adtn_global_attribute6,
                      adtn_global_attribute7,
                      adtn_global_attribute8,
                      adtn_global_attribute9,
                      adtn_global_attribute10,
                      adtn_global_attribute11,
                      adtn_global_attribute12,
                      adtn_global_attribute13,
                      adtn_global_attribute14,
                      adtn_global_attribute15,
                      adtn_global_attribute16,
                      adtn_global_attribute17,
                      adtn_global_attribute18,
                      adtn_global_attribute19,
                      adtn_global_attribute20,
                      adtn_global_attribute_category,
                      leg_book_type_code, --harjinder
                      book_type_code,
                      leg_date_placed_in_service,
                      leg_deprn_start_date,
                      leg_deprn_method_code,
                      -- leg_life_in_months,
                      (CASE
                        WHEN leg_basic_rate IS NOT NULL THEN
                         NULL
                        ELSE
                         leg_life_in_months
                      END) leg_life_in_months,
                      leg_rate_adjustment_factor,
                      leg_adjusted_cost,
                      leg_cost,
                      leg_original_cost,
                      leg_salvage_value,
                      leg_prorate_convention_code,
                      leg_prorate_date,
                      leg_cost_change_flag,
                      leg_adjustment_required_status,
                      leg_capitalize_flag,
                      leg_retirement_pending_flag,
          -- leg_depreciate_flag,
                      DECODE(leg_depreciate_flag,'Y','YES','N','NO',leg_depreciate_flag)  leg_depreciate_flag,
                      leg_basic_rate,
                      leg_adjusted_rate,
                      leg_bonus_rule,
                      leg_ceiling_name,
                      leg_recoverable_cost,
                      leg_cap_period_name,
                      period_counter_capitalized,
                      leg_dep_period_name,
                      period_counter_fully_reserved,
                      leg_unrevalued_cost,
                      leg_annual_deprn_rounding_flag,
                      leg_percent_salvage_value,
                      leg_allowed_deprn_limit,
                      leg_allowed_deprn_limit_amount,
                      leg_salvage_type,
                      leg_deprn_limit_type,
                      leg_period_counter,
                      leg_deprn_source_code,
                      leg_deprn_run_date,
                      leg_deprn_amount,
                      leg_ytd_deprn,
                      leg_deprn_reserve,
                      leg_description,
                      leg_transaction_type_code,
                      leg_transaction_date_entered,
                      leg_transaction_subtype,
                      leg_amortization_start_date,
                      leg_books_global_attribute1,
                      leg_books_global_attribute2,
                      leg_books_global_attribute3,
                      leg_books_global_attribute4,
                      leg_books_global_attribute5,
                      leg_books_global_attribute6,
                      leg_books_global_attribute7,
                      leg_books_global_attribute8,
                      leg_books_global_attribute9,
                      leg_books_global_attribute10,
                      leg_books_global_attribute11,
                      leg_books_global_attribute12,
                      leg_books_global_attribute13,
                      leg_books_global_attribute14,
                      leg_books_global_attribute15,
                      leg_books_global_attribute16,
                      leg_books_global_attribute17,
                      leg_books_global_attribute18,
                      leg_books_global_attribute19,
                      leg_books_global_attribute20,
                      books_global_attribute1,
                      books_global_attribute2,
                      books_global_attribute3,
                      books_global_attribute4,
                      books_global_attribute5,
                      books_global_attribute6,
                      books_global_attribute7,
                      books_global_attribute8,
                      books_global_attribute9,
                      books_global_attribute10,
                      books_global_attribute11,
                      books_global_attribute12,
                      books_global_attribute13,
                      books_global_attribute14,
                      books_global_attribute15,
                      books_global_attribute16,
                      books_global_attribute17,
                      books_global_attribute18,
                      books_global_attribute19,
                      books_global_attribute20,
                      books_global_attr_category,
                      leg_books_global_attr_category,
                      batch_id,
                      process_flag,
                      run_sequence_id
        FROM xxfa_corp_asset_stg
       WHERE leg_parent_asset_number IS NOT NULL
        AND leg_source_system in ('ISSC','FSC')   ---added to get the data only for Global Conversion
         AND process_flag = 'V'
         AND batch_id = g_batch_id;

    --  AND run_sequence_id = g_new_run_seq_id;
    CURSOR get_distribution_details(p_legacy_asset_number VARCHAR2,
                                    p_book_type_code      VARCHAR2) IS
      SELECT leg_asset_number,
             --leg_book_type_code,  --harjinder singh v1.2
             book_type_code,
             acct_combination_id,
             location_id,
             leg_units_assigned,
             assigned_emp_id
        FROM xxfa_corp_asset_stg
       WHERE leg_asset_number = p_legacy_asset_number
            -- AND leg_book_type_code = p_book_type_code  --harjinder singh v1.2
         AND book_type_code = p_book_type_code
         AND process_flag = 'V'
         AND batch_id = g_batch_id;

    --AND run_sequence_id = g_new_run_seq_id;
    l_trans_rec           apps.fa_api_types.trans_rec_type;
    l_dist_trans_rec      apps.fa_api_types.trans_rec_type;
    l_asset_hdr_rec       apps.fa_api_types.asset_hdr_rec_type;
    l_asset_desc_rec      apps.fa_api_types.asset_desc_rec_type;
    l_asset_cat_rec       apps.fa_api_types.asset_cat_rec_type;
    l_asset_type_rec      apps.fa_api_types.asset_type_rec_type;
    l_asset_hierarchy_rec apps.fa_api_types.asset_hierarchy_rec_type;
    l_asset_fin_rec       apps.fa_api_types.asset_fin_rec_type;
    l_asset_deprn_rec     apps.fa_api_types.asset_deprn_rec_type;
    l_asset_dist_rec      apps.fa_api_types.asset_dist_rec_type;
    l_asset_dist_tbl      apps.fa_api_types.asset_dist_tbl_type;
    l_inv_tbl             apps.fa_api_types.inv_tbl_type;
    --l_inv_rate_tbl        apps.FA_API_TYPES.inv_rate_tbl_type;
    l_inv_rec          apps.fa_api_types.inv_rec_type;
    ln_rec_count       NUMBER;
    l_return_status    VARCHAR2(1);
    l_mesg_count       NUMBER := 0;
    l_mesg             VARCHAR2(32000);
    l_msg_data         VARCHAR2(32000);
    l_record_count     NUMBER := 0;
    ln_parent_asset_id NUMBER;
    l_chk_flag         VARCHAR2(1);
  BEGIN
    g_ret_code := NULL;

    FOR rec_cur_conv_parent IN cur_conv_parent LOOP
      l_trans_rec           := NULL;
      l_dist_trans_rec      := NULL;
      l_asset_hdr_rec       := NULL;
      l_asset_desc_rec      := NULL;
      l_asset_cat_rec       := NULL;
      l_asset_type_rec      := NULL;
      l_asset_hierarchy_rec := NULL;
      l_asset_fin_rec       := NULL;
      l_asset_deprn_rec     := NULL;
      l_asset_dist_rec      := NULL;
      l_inv_rec             := NULL;
      ln_rec_count          := NULL;
      l_return_status       := NULL;
      l_mesg_count          := NULL;
      l_mesg                := NULL;
      l_msg_data            := NULL;
      l_record_count        := NULL;
      xxetn_debug_pkg.add_debug(piv_debug_msg => 'Conversion process for Parent asset starts for asset number : ' ||
                                                 rec_cur_conv_parent.leg_asset_number ||
                                                 ' and book type : '
                                                --   || rec_cur_conv_parent.leg_book_type_code
                                                 ||
                                                 rec_cur_conv_parent.book_type_code);
      --Asset  Detail
      l_asset_desc_rec.asset_number := rec_cur_conv_parent.leg_asset_number;
      --l_asset_desc_rec.parent_asset_id := rec_cur_conv_parent.leg_asset_number ;
      l_asset_desc_rec.property_type_code      := rec_cur_conv_parent.leg_property_type_code;
      l_asset_desc_rec.property_1245_1250_code := rec_cur_conv_parent.leg_property_1245_1250_code;
      l_asset_desc_rec.in_use_flag             := rec_cur_conv_parent.leg_in_use_flag;
      l_asset_desc_rec.description             := rec_cur_conv_parent.leg_description;
      l_asset_desc_rec.tag_number              := rec_cur_conv_parent.leg_tag_number;
      l_asset_desc_rec.serial_number           := rec_cur_conv_parent.leg_serial_number;
      l_asset_desc_rec.manufacturer_name       := rec_cur_conv_parent.leg_manufacturer_name;
      l_asset_desc_rec.model_number            := rec_cur_conv_parent.leg_model_number;
      l_asset_desc_rec.owned_leased            := rec_cur_conv_parent.leg_owned_leased;
      l_asset_desc_rec.new_used                := rec_cur_conv_parent.leg_new_used;
      l_asset_desc_rec.unit_adjustment_flag    := rec_cur_conv_parent.leg_unit_adjustment_flag;
      l_asset_desc_rec.add_cost_je_flag        := rec_cur_conv_parent.leg_add_cost_je_flag;
      l_asset_desc_rec.current_units           := rec_cur_conv_parent.leg_current_units;
      l_asset_desc_rec.inventorial             := rec_cur_conv_parent.leg_inventorial;
      l_asset_desc_rec.commitment              := rec_cur_conv_parent.leg_commitment;
      l_asset_desc_rec.investment_law          := rec_cur_conv_parent.leg_investment_law;
      l_asset_cat_rec.category_id              := rec_cur_conv_parent.asset_category_id;
      l_asset_type_rec.asset_type              := rec_cur_conv_parent.leg_asset_type;
      ----attributes value for category dff
      l_asset_cat_rec.desc_flex.CONTEXT     := rec_cur_conv_parent.adtn_context;
      l_asset_cat_rec.desc_flex.attribute1  := rec_cur_conv_parent.adtn_attribute1;
      l_asset_cat_rec.desc_flex.attribute2  := rec_cur_conv_parent.adtn_attribute2;
      l_asset_cat_rec.desc_flex.attribute3  := rec_cur_conv_parent.adtn_attribute3;
      l_asset_cat_rec.desc_flex.attribute4  := rec_cur_conv_parent.adtn_attribute4;
      l_asset_cat_rec.desc_flex.attribute5  := rec_cur_conv_parent.adtn_attribute5;
      l_asset_cat_rec.desc_flex.attribute6  := rec_cur_conv_parent.adtn_attribute6;
      l_asset_cat_rec.desc_flex.attribute7  := rec_cur_conv_parent.adtn_attribute7;
      l_asset_cat_rec.desc_flex.attribute8  := rec_cur_conv_parent.adtn_attribute8;
      l_asset_cat_rec.desc_flex.attribute9  := rec_cur_conv_parent.adtn_attribute9;
      l_asset_cat_rec.desc_flex.attribute10 := rec_cur_conv_parent.adtn_attribute10;
      l_asset_cat_rec.desc_flex.attribute11 := rec_cur_conv_parent.adtn_attribute11;
      l_asset_cat_rec.desc_flex.attribute12 := rec_cur_conv_parent.adtn_attribute12;
      l_asset_cat_rec.desc_flex.attribute13 := rec_cur_conv_parent.adtn_attribute13;
      l_asset_cat_rec.desc_flex.attribute14 := rec_cur_conv_parent.adtn_attribute14;
      l_asset_cat_rec.desc_flex.attribute15 := rec_cur_conv_parent.adtn_attribute15;
      l_asset_cat_rec.desc_flex.attribute16 := rec_cur_conv_parent.adtn_attribute16;
      l_asset_cat_rec.desc_flex.attribute17 := rec_cur_conv_parent.adtn_attribute17;
      l_asset_cat_rec.desc_flex.attribute18 := rec_cur_conv_parent.adtn_attribute18;
      l_asset_cat_rec.desc_flex.attribute19 := rec_cur_conv_parent.adtn_attribute19;
      l_asset_cat_rec.desc_flex.attribute20 := rec_cur_conv_parent.adtn_attribute20;
      l_asset_cat_rec.desc_flex.attribute21 := rec_cur_conv_parent.adtn_attribute21;
      l_asset_cat_rec.desc_flex.attribute22 := rec_cur_conv_parent.adtn_attribute22;
      l_asset_cat_rec.desc_flex.attribute23 := rec_cur_conv_parent.adtn_attribute23;
      l_asset_cat_rec.desc_flex.attribute24 := rec_cur_conv_parent.adtn_attribute24;
      l_asset_cat_rec.desc_flex.attribute25 := rec_cur_conv_parent.adtn_attribute25;
      l_asset_cat_rec.desc_flex.attribute26 := rec_cur_conv_parent.adtn_attribute26;
      l_asset_cat_rec.desc_flex.attribute27 := rec_cur_conv_parent.adtn_attribute27;
      l_asset_cat_rec.desc_flex.attribute28 := rec_cur_conv_parent.adtn_attribute28;
      l_asset_cat_rec.desc_flex.attribute29 := rec_cur_conv_parent.adtn_attribute29;
      l_asset_cat_rec.desc_flex.attribute30 := rec_cur_conv_parent.adtn_attribute30;
      --- Global Attributes
      l_asset_desc_rec.global_desc_flex.attribute1              := rec_cur_conv_parent.adtn_global_attribute1;
      l_asset_desc_rec.global_desc_flex.attribute2              := rec_cur_conv_parent.adtn_global_attribute2;
      l_asset_desc_rec.global_desc_flex.attribute3              := rec_cur_conv_parent.adtn_global_attribute3;
      l_asset_desc_rec.global_desc_flex.attribute4              := rec_cur_conv_parent.adtn_global_attribute4;
      l_asset_desc_rec.global_desc_flex.attribute5              := rec_cur_conv_parent.adtn_global_attribute5;
      l_asset_desc_rec.global_desc_flex.attribute6              := rec_cur_conv_parent.adtn_global_attribute6;
      l_asset_desc_rec.global_desc_flex.attribute7              := rec_cur_conv_parent.adtn_global_attribute7;
      l_asset_desc_rec.global_desc_flex.attribute8              := rec_cur_conv_parent.adtn_global_attribute8;
      l_asset_desc_rec.global_desc_flex.attribute9              := rec_cur_conv_parent.adtn_global_attribute9;
      l_asset_desc_rec.global_desc_flex.attribute10             := rec_cur_conv_parent.adtn_global_attribute10;
      l_asset_desc_rec.global_desc_flex.attribute11             := rec_cur_conv_parent.adtn_global_attribute11;
      l_asset_desc_rec.global_desc_flex.attribute12             := rec_cur_conv_parent.adtn_global_attribute12;
      l_asset_desc_rec.global_desc_flex.attribute13             := rec_cur_conv_parent.adtn_global_attribute13;
      l_asset_desc_rec.global_desc_flex.attribute14             := rec_cur_conv_parent.adtn_global_attribute14;
      l_asset_desc_rec.global_desc_flex.attribute15             := rec_cur_conv_parent.adtn_global_attribute15;
      l_asset_desc_rec.global_desc_flex.attribute16             := rec_cur_conv_parent.adtn_global_attribute16;
      l_asset_desc_rec.global_desc_flex.attribute17             := rec_cur_conv_parent.adtn_global_attribute17;
      l_asset_desc_rec.global_desc_flex.attribute18             := rec_cur_conv_parent.adtn_global_attribute18;
      l_asset_desc_rec.global_desc_flex.attribute19             := rec_cur_conv_parent.adtn_global_attribute19;
      l_asset_desc_rec.global_desc_flex.attribute20             := rec_cur_conv_parent.adtn_global_attribute20;
      l_asset_desc_rec.global_desc_flex.attribute_category_code := rec_cur_conv_parent.adtn_global_attribute_category;
      --books
      -------- l_asset_hdr_rec.book_type_code :=  rec_cur_conv_parent.leg_book_type_code;   ----v1.2 harjinder singh
      l_asset_hdr_rec.book_type_code := rec_cur_conv_parent.book_type_code;
      --financial information
      l_asset_fin_rec.COST                          := rec_cur_conv_parent.leg_cost;
      l_asset_fin_rec.original_cost                 := rec_cur_conv_parent.leg_original_cost;
      l_asset_fin_rec.date_placed_in_service        := rec_cur_conv_parent.leg_date_placed_in_service;
      l_asset_fin_rec.depreciate_flag               := rec_cur_conv_parent.leg_depreciate_flag;
      l_asset_fin_rec.salvage_value                 := rec_cur_conv_parent.leg_salvage_value;
      l_asset_fin_rec.life_in_months                := rec_cur_conv_parent.leg_life_in_months;
      l_asset_fin_rec.period_counter_fully_reserved := rec_cur_conv_parent.period_counter_fully_reserved;
      -- l_asset_fin_rec.deprn_start_date :=
      --                              rec_cur_conv_parent.leg_deprn_start_date;
      l_asset_fin_rec.deprn_method_code := rec_cur_conv_parent.leg_deprn_method_code;
      -- l_asset_fin_rec.rate_adjustment_factor :=
      --                        rec_cur_conv_parent.leg_rate_adjustment_factor;
      --  l_asset_fin_rec.adjusted_cost :=
      --                                 rec_cur_conv_parent.leg_adjusted_cost;
      l_asset_fin_rec.prorate_convention_code := rec_cur_conv_parent.leg_prorate_convention_code;
      --  l_asset_fin_rec.prorate_date := rec_cur_conv_parent.leg_prorate_date;
      l_asset_fin_rec.cost_change_flag           := rec_cur_conv_parent.leg_cost_change_flag;
      l_asset_fin_rec.adjustment_required_status := rec_cur_conv_parent.leg_adjustment_required_status;
      l_asset_fin_rec.capitalize_flag            := rec_cur_conv_parent.leg_capitalize_flag;
      l_asset_fin_rec.retirement_pending_flag    := rec_cur_conv_parent.leg_retirement_pending_flag;
      l_asset_fin_rec.basic_rate                 := rec_cur_conv_parent.leg_basic_rate;
      l_asset_fin_rec.adjusted_rate              := rec_cur_conv_parent.leg_adjusted_rate;
      l_asset_fin_rec.bonus_rule                 := rec_cur_conv_parent.leg_bonus_rule;
      l_asset_fin_rec.ceiling_name               := rec_cur_conv_parent.leg_ceiling_name;
      -- l_asset_fin_rec.recoverable_cost :=
      --                              rec_cur_conv_parent.leg_recoverable_cost;
      l_asset_fin_rec.period_counter_capitalized := rec_cur_conv_parent.period_counter_capitalized;
      l_asset_fin_rec.unrevalued_cost            := rec_cur_conv_parent.leg_unrevalued_cost;
      l_asset_fin_rec.annual_deprn_rounding_flag := rec_cur_conv_parent.leg_annual_deprn_rounding_flag;
      l_asset_fin_rec.percent_salvage_value      := rec_cur_conv_parent.leg_percent_salvage_value;
      l_asset_fin_rec.allowed_deprn_limit        := rec_cur_conv_parent.leg_allowed_deprn_limit;
      l_asset_fin_rec.allowed_deprn_limit_amount := rec_cur_conv_parent.leg_allowed_deprn_limit_amount;
      l_asset_fin_rec.salvage_type               := rec_cur_conv_parent.leg_salvage_type;
      l_asset_fin_rec.deprn_limit_type           := rec_cur_conv_parent.leg_deprn_limit_type;
      ---global attributes for fa books
      --l_asset_fin_rec.global_attribute1 :=
      --                          rec_cur_conv_parent.books_global_attribute1;
      l_asset_fin_rec.global_attribute2  := rec_cur_conv_parent.books_global_attribute2;
      l_asset_fin_rec.global_attribute3  := rec_cur_conv_parent.books_global_attribute3;
      l_asset_fin_rec.global_attribute4  := rec_cur_conv_parent.books_global_attribute4;
      l_asset_fin_rec.global_attribute5  := rec_cur_conv_parent.books_global_attribute5;
      l_asset_fin_rec.global_attribute6  := rec_cur_conv_parent.books_global_attribute6;
      l_asset_fin_rec.global_attribute7  := rec_cur_conv_parent.books_global_attribute7;
      l_asset_fin_rec.global_attribute8  := rec_cur_conv_parent.books_global_attribute8;
      l_asset_fin_rec.global_attribute9  := rec_cur_conv_parent.books_global_attribute9;
      l_asset_fin_rec.global_attribute10 := rec_cur_conv_parent.books_global_attribute10;
      l_asset_fin_rec.global_attribute11 := rec_cur_conv_parent.books_global_attribute11;
      l_asset_fin_rec.global_attribute12 := rec_cur_conv_parent.books_global_attribute12;
      l_asset_fin_rec.global_attribute13 := rec_cur_conv_parent.books_global_attribute13;
      l_asset_fin_rec.global_attribute14 := rec_cur_conv_parent.books_global_attribute14;
      l_asset_fin_rec.global_attribute15 := rec_cur_conv_parent.books_global_attribute15;
      l_asset_fin_rec.global_attribute16 := rec_cur_conv_parent.books_global_attribute16;
      l_asset_fin_rec.global_attribute17 := rec_cur_conv_parent.books_global_attribute17;
      l_asset_fin_rec.global_attribute18 := rec_cur_conv_parent.books_global_attribute18;
      l_asset_fin_rec.global_attribute19 := rec_cur_conv_parent.books_global_attribute19;
      l_asset_fin_rec.global_attribute20 := rec_cur_conv_parent.books_global_attribute20;
      --l_asset_fin_rec.global_attribute_category :=
      --                      rec_cur_conv_parent.books_global_attr_category;
      ---depreciation info
      l_asset_deprn_rec.deprn_amount  := rec_cur_conv_parent.leg_deprn_amount;
      l_asset_deprn_rec.ytd_deprn     := rec_cur_conv_parent.leg_ytd_deprn;
      l_asset_deprn_rec.deprn_reserve := rec_cur_conv_parent.leg_deprn_reserve;
      ln_rec_count                    := 0;
      l_asset_dist_tbl.DELETE;

      FOR rec_get_distribution_details IN get_distribution_details(rec_cur_conv_parent.leg_asset_number,
                                                                   -- rec_cur_conv_parent.leg_book_type_code   --v1.2 harjinder singh
                                                                   rec_cur_conv_parent.book_type_code) LOOP
        ln_rec_count                    := ln_rec_count + 1;
        l_asset_dist_rec.units_assigned := rec_get_distribution_details.leg_units_assigned;
        l_asset_dist_rec.expense_ccid   := rec_get_distribution_details.acct_combination_id;
        l_asset_dist_rec.location_ccid  := rec_get_distribution_details.location_id;
        l_asset_dist_rec.assigned_to    := rec_get_distribution_details.assigned_emp_id;
        -- l_asset_dist_rec.transaction_units      := 10;
        l_asset_dist_tbl(ln_rec_count) := l_asset_dist_rec;
      END LOOP;

      -- Transaction data
      l_trans_rec.transaction_date_entered := NULL;
      --   l_trans_rec.transaction_type_code :=
      --                         rec_cur_conv_parent.leg_transaction_type_code;
      l_trans_rec.transaction_subtype        := g_txn_sub_type;
      l_trans_rec.amortization_start_date    := TO_DATE(g_period_date,
                                                        'MON-YYYY');
      l_trans_rec.who_info.last_update_date  := SYSDATE;
      l_trans_rec.who_info.last_updated_by   := g_last_updated_by;
      l_trans_rec.who_info.created_by        := g_created_by;
      l_trans_rec.who_info.creation_date     := SYSDATE;
      l_trans_rec.who_info.last_update_login := g_last_update_login;
      apps.fnd_msg_pub.initialize;
      apps.fa_addition_pub.do_addition(
                                       -- std parameters
                                       p_api_version      => 1.0,
                                       p_init_msg_list    => apps.fnd_api.g_false,
                                       p_commit           => apps.fnd_api.g_false,
                                       p_validation_level => apps.fnd_api.g_valid_level_full,
                                       p_calling_fn       => NULL,
                                       x_return_status    => l_return_status,
                                       x_msg_count        => l_mesg_count,
                                       x_msg_data         => l_mesg,
                                       -- api parameters
                                       px_trans_rec           => l_trans_rec,
                                       px_dist_trans_rec      => l_dist_trans_rec,
                                       px_asset_hdr_rec       => l_asset_hdr_rec,
                                       px_asset_desc_rec      => l_asset_desc_rec,
                                       px_asset_type_rec      => l_asset_type_rec,
                                       px_asset_cat_rec       => l_asset_cat_rec,
                                       px_asset_hierarchy_rec => l_asset_hierarchy_rec,
                                       px_asset_fin_rec       => l_asset_fin_rec,
                                       px_asset_deprn_rec     => l_asset_deprn_rec,
                                       px_asset_dist_tbl      => l_asset_dist_tbl,
                                       px_inv_tbl             => l_inv_tbl);

      IF l_return_status <> 'S' THEN
        g_ret_code := 1;

        --- updating the error records
        BEGIN
          UPDATE xxfa_corp_asset_stg
             SET process_flag      = 'E',
                 ERROR_TYPE        = 'API_ERR',
                 request_id        = g_request_id,
                 last_updated_date = SYSDATE,
                 last_updated_by   = g_last_updated_by,
                 last_update_login = g_last_update_login
           WHERE leg_asset_number = rec_cur_conv_parent.leg_asset_number
                --AND leg_book_type_code = rec_cur_conv_parent.leg_book_type_code   --v1.2 harjinder singh
             AND book_type_code = rec_cur_conv_parent.book_type_code
             AND process_flag = 'V'
             AND batch_id = g_batch_id;
        EXCEPTION
          WHEN OTHERS THEN
            log_errors(pin_interface_txn_id    => NULL,
                       piv_source_table        => 'XXFA_CORP_ASSET_STG',
                       piv_source_column_name  => NULL,
                       piv_source_column_value => NULL,
                       piv_source_keyname1     => 'leg_asset_number',
                       piv_source_keyvalue1    => rec_cur_conv_parent.leg_asset_number,
                       piv_error_type          => 'API_ERR',
                       piv_error_code          => 'ETN_FA_INVALID_PROC',
                       piv_error_message       => 'Error : Exception occured while updating XXFA_CORP_ASSET_STG for errors  : ' ||
                                                  SUBSTR(SQLERRM, 1, 240));
        END;

        --   AND run_sequence_id = g_new_run_seq_id;

        --- error msg
        IF l_mesg_count > 1 THEN
          FOR i IN 1 .. (l_mesg_count - 1) LOOP
            l_mesg     := apps.fnd_msg_pub.get(apps.fnd_msg_pub.g_next,
                                               apps.fnd_api.g_false);
            l_msg_data := l_msg_data || '-' || l_mesg;
            log_errors(pin_interface_txn_id    => NULL,
                       piv_source_table        => 'XXFA_CORP_ASSET_STG',
                       piv_source_column_name  => NULL,
                       piv_source_column_value => NULL,
                       piv_source_keyname1     => 'leg_asset_number',
                       piv_source_keyvalue1    => rec_cur_conv_parent.leg_asset_number,
                       piv_error_type          => 'API_ERR',
                       piv_error_code          => 'ETN_FA_IMPORT_ERR',
                       piv_error_message       => l_msg_data);
          END LOOP;
        ELSE
          l_msg_data := apps.fnd_msg_pub.get(apps.fnd_msg_pub.g_first,
                                             apps.fnd_api.g_false);
          log_errors(pin_interface_txn_id    => NULL,
                     piv_source_table        => 'XXFA_CORP_ASSET_STG',
                     piv_source_column_name  => NULL,
                     piv_source_column_value => NULL,
                     piv_source_keyname1     => 'leg_asset_number',
                     piv_source_keyvalue1    => rec_cur_conv_parent.leg_asset_number,
                     piv_error_type          => 'API_ERR',
                     piv_error_code          => 'ETN_FA_IMPORT_ERR',
                     piv_error_message       => l_msg_data);
        END IF;
      ELSE
        BEGIN
          UPDATE xxfa_corp_asset_stg
             SET process_flag      = 'C',
                 asset_id          = l_asset_hdr_rec.asset_id,
                 request_id        = g_request_id,
                 last_updated_date = SYSDATE,
                 last_updated_by   = g_last_updated_by,
                 last_update_login = g_last_update_login
           WHERE leg_asset_number = rec_cur_conv_parent.leg_asset_number
                ---  AND leg_book_type_code = rec_cur_conv_parent.leg_book_type_code    ---- v1.2 harjinder singh
             AND book_type_code = rec_cur_conv_parent.book_type_code
             AND process_flag = 'V'
             AND batch_id = g_batch_id;
        EXCEPTION
          WHEN OTHERS THEN
            log_errors(pin_interface_txn_id    => NULL,
                       piv_source_table        => 'XXFA_CORP_ASSET_STG',
                       piv_source_column_name  => NULL,
                       piv_source_column_value => NULL,
                       piv_source_keyname1     => 'leg_asset_number',
                       piv_source_keyvalue1    => rec_cur_conv_parent.leg_asset_number,
                       piv_error_type          => 'API_ERR',
                       piv_error_code          => 'ETN_FA_INVALID_PROC',
                       piv_error_message       => 'Error : Exception occured while updating XXFA_CORP_ASSET_STG records to completed status  : ' ||
                                                  SUBSTR(SQLERRM, 1, 240));
        END;
      END IF;
    END LOOP;

    COMMIT;
    xxetn_debug_pkg.add_debug(piv_debug_msg => 'Conversion process for parent assets with corporate book ends : ');

    FOR rec_cur_conv_child IN cur_conv_child LOOP
      ln_parent_asset_id    := NULL;
      l_trans_rec           := NULL;
      l_dist_trans_rec      := NULL;
      l_asset_hdr_rec       := NULL;
      l_asset_desc_rec      := NULL;
      l_asset_cat_rec       := NULL;
      l_asset_type_rec      := NULL;
      l_asset_hierarchy_rec := NULL;
      l_asset_fin_rec       := NULL;
      l_asset_deprn_rec     := NULL;
      l_asset_dist_rec      := NULL;
      l_inv_rec             := NULL;
      ln_rec_count          := NULL;
      l_return_status       := NULL;
      l_mesg_count          := NULL;
      l_mesg                := NULL;
      l_msg_data            := NULL;
      l_record_count        := NULL;
      l_chk_flag            := 'N';
      xxetn_debug_pkg.add_debug(piv_debug_msg => 'Conversion process for Child asset : ' ||
                                                 rec_cur_conv_child.leg_asset_number ||
                                                 ' and book type : '
                                                --|| rec_cur_conv_child.leg_book_type_code     --- v1.2 Harjinder singh
                                                 ||
                                                 rec_cur_conv_child.book_type_code);

      BEGIN
        SELECT DISTINCT asset_id
          INTO ln_parent_asset_id
          FROM xxfa_corp_asset_stg
         WHERE leg_asset_number =
               rec_cur_conv_child.leg_parent_asset_number
           AND process_flag = 'C';
        -- AND batch_id = g_batch_id
        -- AND run_sequence_id = g_new_run_seq_id;
      EXCEPTION
        WHEN NO_DATA_FOUND THEN
          log_errors(pin_interface_txn_id    => NULL,
                     piv_source_table        => 'XXFA_CORP_ASSET_STG',
                     piv_source_column_name  => NULL,
                     piv_source_column_value => NULL,
                     piv_source_keyname1     => 'leg_asset_number',
                     piv_source_keyvalue1    => rec_cur_conv_child.leg_asset_number,
                     piv_error_type          => 'API_ERR',
                     piv_error_code          => 'ETN_FA_INVALID_PARENT_ASSET',
                     piv_error_message       => 'Error : Child doesnt have a successful parent record: ');
          l_chk_flag := 'Y';
        WHEN OTHERS THEN
          log_errors(pin_interface_txn_id    => NULL,
                     piv_source_table        => 'XXFA_CORP_ASSET_STG',
                     piv_source_column_name  => NULL,
                     piv_source_column_value => NULL,
                     piv_source_keyname1     => 'leg_asset_number',
                     piv_source_keyvalue1    => rec_cur_conv_child.leg_asset_number,
                     piv_error_type          => 'API_ERR',
                     piv_error_code          => 'ETN_FA_INVALID_PARENT_ASSET',
                     piv_error_message       => 'Error : Exception Error while fetching parent asset ID : ' ||
                                                SUBSTR(SQLERRM, 1, 240));
          l_chk_flag := 'Y';
      END;

      IF l_chk_flag = 'Y' THEN
        BEGIN
          --- updating the error records
          UPDATE xxfa_corp_asset_stg
             SET process_flag      = 'E',
                 ERROR_TYPE        = 'API_ERR',
                 request_id        = g_request_id,
                 last_updated_date = SYSDATE,
                 last_updated_by   = g_last_updated_by,
                 last_update_login = g_last_update_login
           WHERE leg_asset_number = rec_cur_conv_child.leg_asset_number
                --AND leg_book_type_code = rec_cur_conv_child.leg_book_type_code   v1.2 Harjinder Singh
             AND book_type_code = rec_cur_conv_child.book_type_code
             AND process_flag = 'V'
             AND batch_id = g_batch_id;
        EXCEPTION
          WHEN OTHERS THEN
            log_errors(pin_interface_txn_id    => NULL,
                       piv_source_table        => 'XXFA_CORP_ASSET_STG',
                       piv_source_column_name  => NULL,
                       piv_source_column_value => NULL,
                       piv_source_keyname1     => 'leg_asset_number',
                       piv_source_keyvalue1    => rec_cur_conv_child.leg_asset_number,
                       piv_error_type          => 'API_ERR',
                       piv_error_code          => 'ETN_FA_INVALID_PROC',
                       piv_error_message       => 'Error : Exception occured while updating XXFA_CORP_ASSET_STG for child record errors  : ' ||
                                                  SUBSTR(SQLERRM, 1, 240));
        END;
      ELSE
        --Asset  Detail
        l_asset_desc_rec.asset_number            := rec_cur_conv_child.leg_asset_number;
        l_asset_desc_rec.parent_asset_id         := ln_parent_asset_id;
        l_asset_desc_rec.property_type_code      := rec_cur_conv_child.leg_property_type_code;
        l_asset_desc_rec.property_1245_1250_code := rec_cur_conv_child.leg_property_1245_1250_code;
        l_asset_desc_rec.in_use_flag             := rec_cur_conv_child.leg_in_use_flag;
        l_asset_desc_rec.description             := rec_cur_conv_child.leg_description;
        l_asset_desc_rec.tag_number              := rec_cur_conv_child.leg_tag_number;
        l_asset_desc_rec.serial_number           := rec_cur_conv_child.leg_serial_number;
        l_asset_desc_rec.manufacturer_name       := rec_cur_conv_child.leg_manufacturer_name;
        l_asset_desc_rec.model_number            := rec_cur_conv_child.leg_model_number;
        l_asset_desc_rec.owned_leased            := rec_cur_conv_child.leg_owned_leased;
        l_asset_desc_rec.new_used                := rec_cur_conv_child.leg_new_used;
        l_asset_desc_rec.unit_adjustment_flag    := rec_cur_conv_child.leg_unit_adjustment_flag;
        l_asset_desc_rec.add_cost_je_flag        := rec_cur_conv_child.leg_add_cost_je_flag;
        l_asset_desc_rec.current_units           := rec_cur_conv_child.leg_current_units;
        l_asset_desc_rec.inventorial             := rec_cur_conv_child.leg_inventorial;
        l_asset_desc_rec.commitment              := rec_cur_conv_child.leg_commitment;
        l_asset_desc_rec.investment_law          := rec_cur_conv_child.leg_investment_law;
        l_asset_cat_rec.category_id              := rec_cur_conv_child.asset_category_id;
        l_asset_type_rec.asset_type              := rec_cur_conv_child.leg_asset_type;
        ----attributes value for category dff
        l_asset_cat_rec.desc_flex.CONTEXT     := rec_cur_conv_child.adtn_context;
        l_asset_cat_rec.desc_flex.attribute1  := rec_cur_conv_child.adtn_attribute1;
        l_asset_cat_rec.desc_flex.attribute2  := rec_cur_conv_child.adtn_attribute2;
        l_asset_cat_rec.desc_flex.attribute3  := rec_cur_conv_child.adtn_attribute3;
        l_asset_cat_rec.desc_flex.attribute4  := rec_cur_conv_child.adtn_attribute4;
        l_asset_cat_rec.desc_flex.attribute5  := rec_cur_conv_child.adtn_attribute5;
        l_asset_cat_rec.desc_flex.attribute6  := rec_cur_conv_child.adtn_attribute6;
        l_asset_cat_rec.desc_flex.attribute7  := rec_cur_conv_child.adtn_attribute7;
        l_asset_cat_rec.desc_flex.attribute8  := rec_cur_conv_child.adtn_attribute8;
        l_asset_cat_rec.desc_flex.attribute9  := rec_cur_conv_child.adtn_attribute9;
        l_asset_cat_rec.desc_flex.attribute10 := rec_cur_conv_child.adtn_attribute10;
        l_asset_cat_rec.desc_flex.attribute11 := rec_cur_conv_child.adtn_attribute11;
        l_asset_cat_rec.desc_flex.attribute12 := rec_cur_conv_child.adtn_attribute12;
        l_asset_cat_rec.desc_flex.attribute13 := rec_cur_conv_child.adtn_attribute13;
        l_asset_cat_rec.desc_flex.attribute14 := rec_cur_conv_child.adtn_attribute14;
        l_asset_cat_rec.desc_flex.attribute15 := rec_cur_conv_child.adtn_attribute15;
        l_asset_cat_rec.desc_flex.attribute16 := rec_cur_conv_child.adtn_attribute16;
        l_asset_cat_rec.desc_flex.attribute17 := rec_cur_conv_child.adtn_attribute17;
        l_asset_cat_rec.desc_flex.attribute18 := rec_cur_conv_child.adtn_attribute18;
        l_asset_cat_rec.desc_flex.attribute19 := rec_cur_conv_child.adtn_attribute19;
        l_asset_cat_rec.desc_flex.attribute20 := rec_cur_conv_child.adtn_attribute20;
        l_asset_cat_rec.desc_flex.attribute21 := rec_cur_conv_child.adtn_attribute21;
        l_asset_cat_rec.desc_flex.attribute22 := rec_cur_conv_child.adtn_attribute22;
        l_asset_cat_rec.desc_flex.attribute23 := rec_cur_conv_child.adtn_attribute23;
        l_asset_cat_rec.desc_flex.attribute24 := rec_cur_conv_child.adtn_attribute24;
        l_asset_cat_rec.desc_flex.attribute25 := rec_cur_conv_child.adtn_attribute25;
        l_asset_cat_rec.desc_flex.attribute26 := rec_cur_conv_child.adtn_attribute26;
        l_asset_cat_rec.desc_flex.attribute27 := rec_cur_conv_child.adtn_attribute27;
        l_asset_cat_rec.desc_flex.attribute28 := rec_cur_conv_child.adtn_attribute28;
        l_asset_cat_rec.desc_flex.attribute29 := rec_cur_conv_child.adtn_attribute29;
        l_asset_cat_rec.desc_flex.attribute30 := rec_cur_conv_child.adtn_attribute30;
        --- Global Attributes
        l_asset_desc_rec.global_desc_flex.attribute1              := rec_cur_conv_child.adtn_global_attribute1;
        l_asset_desc_rec.global_desc_flex.attribute2              := rec_cur_conv_child.adtn_global_attribute2;
        l_asset_desc_rec.global_desc_flex.attribute3              := rec_cur_conv_child.adtn_global_attribute3;
        l_asset_desc_rec.global_desc_flex.attribute4              := rec_cur_conv_child.adtn_global_attribute4;
        l_asset_desc_rec.global_desc_flex.attribute5              := rec_cur_conv_child.adtn_global_attribute5;
        l_asset_desc_rec.global_desc_flex.attribute6              := rec_cur_conv_child.adtn_global_attribute6;
        l_asset_desc_rec.global_desc_flex.attribute7              := rec_cur_conv_child.adtn_global_attribute7;
        l_asset_desc_rec.global_desc_flex.attribute8              := rec_cur_conv_child.adtn_global_attribute8;
        l_asset_desc_rec.global_desc_flex.attribute9              := rec_cur_conv_child.adtn_global_attribute9;
        l_asset_desc_rec.global_desc_flex.attribute10             := rec_cur_conv_child.adtn_global_attribute10;
        l_asset_desc_rec.global_desc_flex.attribute11             := rec_cur_conv_child.adtn_global_attribute11;
        l_asset_desc_rec.global_desc_flex.attribute12             := rec_cur_conv_child.adtn_global_attribute12;
        l_asset_desc_rec.global_desc_flex.attribute13             := rec_cur_conv_child.adtn_global_attribute13;
        l_asset_desc_rec.global_desc_flex.attribute14             := rec_cur_conv_child.adtn_global_attribute14;
        l_asset_desc_rec.global_desc_flex.attribute15             := rec_cur_conv_child.adtn_global_attribute15;
        l_asset_desc_rec.global_desc_flex.attribute16             := rec_cur_conv_child.adtn_global_attribute16;
        l_asset_desc_rec.global_desc_flex.attribute17             := rec_cur_conv_child.adtn_global_attribute17;
        l_asset_desc_rec.global_desc_flex.attribute18             := rec_cur_conv_child.adtn_global_attribute18;
        l_asset_desc_rec.global_desc_flex.attribute19             := rec_cur_conv_child.adtn_global_attribute19;
        l_asset_desc_rec.global_desc_flex.attribute20             := rec_cur_conv_child.adtn_global_attribute20;
        l_asset_desc_rec.global_desc_flex.attribute_category_code := rec_cur_conv_child.adtn_global_attribute_category;
        --books
        ---  l_asset_hdr_rec.book_type_code :=rec_cur_conv_child.leg_book_type_code; ----v1.2 Harjinder Singh
        l_asset_hdr_rec.book_type_code := rec_cur_conv_child.book_type_code;
        --financial information
        l_asset_fin_rec.COST                   := rec_cur_conv_child.leg_cost;
        l_asset_fin_rec.original_cost          := rec_cur_conv_child.leg_original_cost;
        l_asset_fin_rec.date_placed_in_service := rec_cur_conv_child.leg_date_placed_in_service;
        l_asset_fin_rec.depreciate_flag        := rec_cur_conv_child.leg_depreciate_flag;
        l_asset_fin_rec.salvage_value          := rec_cur_conv_child.leg_salvage_value;
        l_asset_fin_rec.life_in_months         := rec_cur_conv_child.leg_life_in_months;
        --l_asset_fin_rec.deprn_start_date :=
        --                          rec_cur_conv_child.leg_deprn_start_date;
        l_asset_fin_rec.deprn_method_code := rec_cur_conv_child.leg_deprn_method_code;
        --l_asset_fin_rec.rate_adjustment_factor :=
        --                    rec_cur_conv_child.leg_rate_adjustment_factor;
        --l_asset_fin_rec.adjusted_cost :=
        --                            rec_cur_conv_child.leg_adjusted_cost;
        l_asset_fin_rec.prorate_convention_code := rec_cur_conv_child.leg_prorate_convention_code;
        --l_asset_fin_rec.prorate_date :=
        --                              rec_cur_conv_child.leg_prorate_date;
        l_asset_fin_rec.cost_change_flag           := rec_cur_conv_child.leg_cost_change_flag;
        l_asset_fin_rec.adjustment_required_status := rec_cur_conv_child.leg_adjustment_required_status;
        l_asset_fin_rec.capitalize_flag            := rec_cur_conv_child.leg_capitalize_flag;
        l_asset_fin_rec.retirement_pending_flag    := rec_cur_conv_child.leg_retirement_pending_flag;
        l_asset_fin_rec.basic_rate                 := rec_cur_conv_child.leg_basic_rate;
        l_asset_fin_rec.adjusted_rate              := rec_cur_conv_child.leg_adjusted_rate;
        l_asset_fin_rec.bonus_rule                 := rec_cur_conv_child.leg_bonus_rule;
        l_asset_fin_rec.ceiling_name               := rec_cur_conv_child.leg_ceiling_name;
        --l_asset_fin_rec.recoverable_cost :=
        --                          rec_cur_conv_child.leg_recoverable_cost;
        l_asset_fin_rec.unrevalued_cost            := rec_cur_conv_child.leg_unrevalued_cost;
        l_asset_fin_rec.annual_deprn_rounding_flag := rec_cur_conv_child.leg_annual_deprn_rounding_flag;
        l_asset_fin_rec.percent_salvage_value      := rec_cur_conv_child.leg_percent_salvage_value;
        l_asset_fin_rec.allowed_deprn_limit        := rec_cur_conv_child.leg_allowed_deprn_limit;
        l_asset_fin_rec.allowed_deprn_limit_amount := rec_cur_conv_child.leg_allowed_deprn_limit_amount;
        l_asset_fin_rec.salvage_type               := rec_cur_conv_child.leg_salvage_type;
        l_asset_fin_rec.deprn_limit_type           := rec_cur_conv_child.leg_deprn_limit_type;
        ---global attributes for fa books
        ---global attributes for fa books
        -- l_asset_fin_rec.global_attribute1 :=
        --                       rec_cur_conv_child.books_global_attribute1;
        l_asset_fin_rec.global_attribute2  := rec_cur_conv_child.books_global_attribute2;
        l_asset_fin_rec.global_attribute3  := rec_cur_conv_child.books_global_attribute3;
        l_asset_fin_rec.global_attribute4  := rec_cur_conv_child.books_global_attribute4;
        l_asset_fin_rec.global_attribute5  := rec_cur_conv_child.books_global_attribute5;
        l_asset_fin_rec.global_attribute6  := rec_cur_conv_child.books_global_attribute6;
        l_asset_fin_rec.global_attribute7  := rec_cur_conv_child.books_global_attribute7;
        l_asset_fin_rec.global_attribute8  := rec_cur_conv_child.books_global_attribute8;
        l_asset_fin_rec.global_attribute9  := rec_cur_conv_child.books_global_attribute9;
        l_asset_fin_rec.global_attribute10 := rec_cur_conv_child.books_global_attribute10;
        l_asset_fin_rec.global_attribute11 := rec_cur_conv_child.books_global_attribute11;
        l_asset_fin_rec.global_attribute12 := rec_cur_conv_child.books_global_attribute12;
        l_asset_fin_rec.global_attribute13 := rec_cur_conv_child.books_global_attribute13;
        l_asset_fin_rec.global_attribute14 := rec_cur_conv_child.books_global_attribute14;
        l_asset_fin_rec.global_attribute15 := rec_cur_conv_child.books_global_attribute15;
        l_asset_fin_rec.global_attribute16 := rec_cur_conv_child.books_global_attribute16;
        l_asset_fin_rec.global_attribute17 := rec_cur_conv_child.books_global_attribute17;
        l_asset_fin_rec.global_attribute18 := rec_cur_conv_child.books_global_attribute18;
        l_asset_fin_rec.global_attribute19 := rec_cur_conv_child.books_global_attribute19;
        l_asset_fin_rec.global_attribute20 := rec_cur_conv_child.books_global_attribute20;
        -- l_asset_fin_rec.global_attribute_category :=
        --                    rec_cur_conv_child.books_global_attr_category;
        ---depreciation info
        l_asset_deprn_rec.deprn_amount  := rec_cur_conv_child.leg_deprn_amount;
        l_asset_deprn_rec.ytd_deprn     := rec_cur_conv_child.leg_ytd_deprn;
        l_asset_deprn_rec.deprn_reserve := rec_cur_conv_child.leg_deprn_reserve;
        ln_rec_count                    := 0;
        l_asset_dist_tbl.DELETE;

        FOR rec_get_distribution_details IN get_distribution_details(rec_cur_conv_child.leg_asset_number,
                                                                     ---rec_cur_conv_child.leg_book_type_code   ---Harjinder Singh
                                                                     rec_cur_conv_child.book_type_code) LOOP
          ln_rec_count                    := ln_rec_count + 1;
          l_asset_dist_rec.units_assigned := rec_get_distribution_details.leg_units_assigned;
          l_asset_dist_rec.expense_ccid   := rec_get_distribution_details.acct_combination_id;
          l_asset_dist_rec.location_ccid  := rec_get_distribution_details.location_id;
          l_asset_dist_rec.assigned_to    := rec_get_distribution_details.assigned_emp_id;
          -- l_asset_dist_rec.transaction_units      := 10;.
          l_asset_dist_tbl(ln_rec_count) := l_asset_dist_rec;
        END LOOP;

        -- Transaction data
        l_trans_rec.transaction_date_entered := NULL;
        -- l_trans_rec.transaction_type_code :=
        --                      rec_cur_conv_child.leg_transaction_type_code;
        l_trans_rec.transaction_subtype        := g_txn_sub_type;
        l_trans_rec.amortization_start_date    := TO_DATE(g_period_date,
                                                          'MON-YYYY');
        l_trans_rec.who_info.last_update_date  := SYSDATE;
        l_trans_rec.who_info.last_updated_by   := g_last_updated_by;
        l_trans_rec.who_info.created_by        := g_created_by;
        l_trans_rec.who_info.creation_date     := SYSDATE;
        l_trans_rec.who_info.last_update_login := g_last_update_login;
        apps.fnd_msg_pub.initialize;
        apps.fa_addition_pub.do_addition(
                                         -- std parameters
                                         p_api_version      => 1.0,
                                         p_init_msg_list    => apps.fnd_api.g_false,
                                         p_commit           => apps.fnd_api.g_false,
                                         p_validation_level => apps.fnd_api.g_valid_level_full,
                                         p_calling_fn       => NULL,
                                         x_return_status    => l_return_status,
                                         x_msg_count        => l_mesg_count,
                                         x_msg_data         => l_mesg,
                                         -- api parameters
                                         px_trans_rec           => l_trans_rec,
                                         px_dist_trans_rec      => l_dist_trans_rec,
                                         px_asset_hdr_rec       => l_asset_hdr_rec,
                                         px_asset_desc_rec      => l_asset_desc_rec,
                                         px_asset_type_rec      => l_asset_type_rec,
                                         px_asset_cat_rec       => l_asset_cat_rec,
                                         px_asset_hierarchy_rec => l_asset_hierarchy_rec,
                                         px_asset_fin_rec       => l_asset_fin_rec,
                                         px_asset_deprn_rec     => l_asset_deprn_rec,
                                         px_asset_dist_tbl      => l_asset_dist_tbl,
                                         px_inv_tbl             => l_inv_tbl);

        IF l_return_status <> 'S' THEN
          g_ret_code := 1;

          BEGIN
            --- updating the error records
            UPDATE xxfa_corp_asset_stg
               SET process_flag      = 'E',
                   ERROR_TYPE        = 'API_ERR',
                   request_id        = g_request_id,
                   last_updated_date = SYSDATE,
                   last_updated_by   = g_last_updated_by,
                   last_update_login = g_last_update_login
             WHERE leg_asset_number = rec_cur_conv_child.leg_asset_number
                  --- AND leg_book_type_code = rec_cur_conv_child.leg_book_type_code   ---v1.2 Harjinder Singh
               AND book_type_code = rec_cur_conv_child.book_type_code
               AND process_flag = 'V'
               AND batch_id = g_batch_id;
          EXCEPTION
            WHEN OTHERS THEN
              log_errors(pin_interface_txn_id    => NULL,
                         piv_source_table        => 'XXFA_CORP_ASSET_STG',
                         piv_source_column_name  => NULL,
                         piv_source_column_value => NULL,
                         piv_source_keyname1     => 'leg_asset_number',
                         piv_source_keyvalue1    => rec_cur_conv_child.leg_asset_number,
                         piv_error_type          => 'API_ERR',
                         piv_error_code          => 'ETN_FA_INVALID_PROC',
                         piv_error_message       => 'Error : Exception occured while updating XXFA_CORP_ASSET_STG for import process  : ' ||
                                                    SUBSTR(SQLERRM, 1, 240));
          END;

          --- error msg
          IF l_mesg_count > 1 THEN
            FOR i IN 1 .. (l_mesg_count - 1) LOOP
              l_mesg     := apps.fnd_msg_pub.get(apps.fnd_msg_pub.g_next,
                                                 apps.fnd_api.g_false);
              l_msg_data := l_msg_data || '-' || l_mesg;
              log_errors(pin_interface_txn_id    => NULL,
                         piv_source_table        => 'XXFA_CORP_ASSET_STG',
                         piv_source_column_name  => NULL,
                         piv_source_column_value => NULL,
                         piv_source_keyname1     => 'leg_asset_number',
                         piv_source_keyvalue1    => rec_cur_conv_child.leg_asset_number,
                         piv_error_type          => 'API_ERR',
                         piv_error_code          => 'ETN_FA_IMPORT_ERR',
                         piv_error_message       => l_msg_data);
            END LOOP;
          ELSE
            l_msg_data := apps.fnd_msg_pub.get(apps.fnd_msg_pub.g_first,
                                               apps.fnd_api.g_false);
            log_errors(pin_interface_txn_id    => NULL,
                       piv_source_table        => 'XXFA_CORP_ASSET_STG',
                       piv_source_column_name  => NULL,
                       piv_source_column_value => NULL,
                       piv_source_keyname1     => 'leg_asset_number',
                       piv_source_keyvalue1    => rec_cur_conv_child.leg_asset_number,
                       piv_error_type          => 'API_ERR',
                       piv_error_code          => 'ETN_FA_IMPORT_ERR',
                       piv_error_message       => l_msg_data);
          END IF;
        ELSE
          UPDATE xxfa_corp_asset_stg
             SET process_flag      = 'C',
                 asset_id          = l_asset_hdr_rec.asset_id,
                 parent_asset_id   = ln_parent_asset_id,
                 request_id        = g_request_id,
                 last_updated_date = SYSDATE,
                 last_updated_by   = g_last_updated_by,
                 last_update_login = g_last_update_login
           WHERE leg_asset_number = rec_cur_conv_child.leg_asset_number
                -- AND leg_book_type_code = rec_cur_conv_child.leg_book_type_code    ----v1.2 Harjinder Singh
             AND book_type_code = rec_cur_conv_child.book_type_code
             AND process_flag = 'V'
             AND batch_id = g_batch_id;
        END IF;
      END IF;
    END LOOP;

    --- child cursor ends
    xxetn_debug_pkg.add_debug(piv_debug_msg => 'Conversion Process for Child assets with Corporate book Ends : ');
    COMMIT;
  EXCEPTION
    WHEN OTHERS THEN
      g_ret_code := 2;
      print_log_message('Error : Exception occured while importing records with corporate book  : ' ||
                        SUBSTR(SQLERRM, 1, 240));
      print_log_message('Error : Backtrace in import for corporate : ' ||
                        DBMS_UTILITY.format_error_backtrace);
  END import_corporate;

  --
  -- ========================
  -- Procedure: IMPORT_TAX
  -- =====================================================================================
  --   This procedure is used to import the assets with tax books
  -- =====================================================================================
  PROCEDURE import_tax IS
    CURSOR cur_conv_tax_asset IS
      SELECT DISTINCT leg_source_system,
                      leg_asset_id,
                      asset_id,
                      leg_asset_number,
                      leg_current_units,
                      leg_asset_type,
                      leg_tag_number,
                      leg_asset_cat_segment1,
                      leg_asset_cat_segment2,
                      asset_cat_segment1,
                      asset_cat_segment2,
                      asset_category_id,
                      leg_parent_asset_number,
                      parent_asset_id,
                      leg_manufacturer_name,
                      leg_serial_number,
                      leg_model_number,
                      leg_property_type_code,
                      leg_property_1245_1250_code,
                      leg_in_use_flag,
                      leg_owned_leased,
                      leg_new_used,
                      leg_unit_adjustment_flag,
                      leg_add_cost_je_flag,
                      leg_adtn_attribute1,
                      leg_adtn_attribute2,
                      leg_adtn_attribute3,
                      leg_adtn_attribute4,
                      leg_adtn_attribute5,
                      leg_adtn_attribute6,
                      leg_adtn_attribute7,
                      leg_adtn_attribute8,
                      leg_adtn_attribute9,
                      leg_adtn_attribute10,
                      leg_adtn_attribute11,
                      leg_adtn_attribute12,
                      leg_adtn_attribute13,
                      leg_adtn_attribute14,
                      leg_adtn_attribute15,
                      leg_adtn_attribute16,
                      leg_adtn_attribute17,
                      leg_adtn_attribute18,
                      leg_adtn_attribute19,
                      leg_adtn_attribute20,
                      leg_adtn_attribute21,
                      leg_adtn_attribute22,
                      leg_adtn_attribute23,
                      leg_adtn_attribute24,
                      leg_adtn_attribute25,
                      leg_adtn_attribute26,
                      leg_adtn_attribute27,
                      leg_adtn_attribute28,
                      leg_adtn_attribute29,
                      leg_adtn_attribute30,
                      leg_adtn_attr_category_code,
                      leg_adtn_context,
                      adtn_attribute1,
                      adtn_attribute2,
                      adtn_attribute3,
                      adtn_attribute4,
                      adtn_attribute5,
                      adtn_attribute6,
                      adtn_attribute7,
                      adtn_attribute8,
                      adtn_attribute9,
                      adtn_attribute10,
                      adtn_attribute11,
                      adtn_attribute12,
                      adtn_attribute13,
                      adtn_attribute14,
                      adtn_attribute15,
                      adtn_attribute16,
                      adtn_attribute17,
                      adtn_attribute18,
                      adtn_attribute19,
                      adtn_attribute20,
                      adtn_attribute21,
                      adtn_attribute22,
                      adtn_attribute23,
                      adtn_attribute24,
                      adtn_attribute25,
                      adtn_attribute26,
                      adtn_attribute27,
                      adtn_attribute28,
                      adtn_attribute29,
                      adtn_attribute30,
                      adtn_attribute_category_code,
                      adtn_context,
                      leg_inventorial,
                      leg_commitment,
                      leg_investment_law,
                      leg_adtn_global_attribute1,
                      leg_adtn_global_attribute2,
                      leg_adtn_global_attribute3,
                      leg_adtn_global_attribute4,
                      leg_adtn_global_attribute5,
                      leg_adtn_global_attribute6,
                      leg_adtn_global_attribute7,
                      leg_adtn_global_attribute8,
                      leg_adtn_global_attribute9,
                      leg_adtn_global_attribute10,
                      leg_adtn_global_attribute11,
                      leg_adtn_global_attribute12,
                      leg_adtn_global_attribute13,
                      leg_adtn_global_attribute14,
                      leg_adtn_global_attribute15,
                      leg_adtn_global_attribute16,
                      leg_adtn_global_attribute17,
                      leg_adtn_global_attribute18,
                      leg_adtn_global_attribute19,
                      leg_adtn_global_attribute20,
                      leg_adtn_global_attr_category,
                      adtn_global_attribute1,
                      adtn_global_attribute2,
                      adtn_global_attribute3,
                      adtn_global_attribute4,
                      adtn_global_attribute5,
                      adtn_global_attribute6,
                      adtn_global_attribute7,
                      adtn_global_attribute8,
                      adtn_global_attribute9,
                      adtn_global_attribute10,
                      adtn_global_attribute11,
                      adtn_global_attribute12,
                      adtn_global_attribute13,
                      adtn_global_attribute14,
                      adtn_global_attribute15,
                      adtn_global_attribute16,
                      adtn_global_attribute17,
                      adtn_global_attribute18,
                      adtn_global_attribute19,
                      adtn_global_attribute20,
                      adtn_global_attribute_category,
                      leg_book_type_code, ---Harjinder Singh
                      book_type_code,
                      leg_date_placed_in_service,
                      leg_deprn_start_date,
                      leg_deprn_method_code,
                      --leg_life_in_months,
                      (CASE
                        WHEN leg_basic_rate IS NOT NULL THEN
                         NULL
                        ELSE
                         leg_life_in_months
                      END) leg_life_in_months,
                      leg_rate_adjustment_factor,
                      leg_adjusted_cost,
                      leg_cost,
                      leg_original_cost,
                      leg_salvage_value,
                      leg_prorate_convention_code,
                      leg_prorate_date,
                      leg_cost_change_flag,
                      leg_adjustment_required_status,
                      leg_capitalize_flag,
                      leg_retirement_pending_flag,
                      DECODE(leg_depreciate_flag,'Y','YES','N','NO',leg_depreciate_flag) leg_depreciate_flag,  -- ADDED DECODE CLAUSE ,
                      leg_basic_rate,
                      leg_adjusted_rate,
                      leg_bonus_rule,
                      leg_ceiling_name,
                      leg_recoverable_cost,
                      leg_cap_period_name,
                      period_counter_capitalized,
                      leg_dep_period_name,
                      period_counter_fully_reserved,
                      leg_unrevalued_cost,
                      leg_annual_deprn_rounding_flag,
                      leg_percent_salvage_value,
                      leg_allowed_deprn_limit,
                      leg_allowed_deprn_limit_amount,
                      leg_salvage_type,
                      leg_deprn_limit_type,
                      leg_period_counter,
                      leg_deprn_source_code,
                      leg_deprn_run_date,
                      leg_deprn_amount,
                      leg_ytd_deprn,
                      leg_deprn_reserve,
                      leg_description,
                      leg_transaction_type_code,
                      leg_transaction_date_entered,
                      leg_transaction_subtype,
                      leg_amortization_start_date,
                      leg_books_global_attribute1,
                      leg_books_global_attribute2,
                      leg_books_global_attribute3,
                      leg_books_global_attribute4,
                      leg_books_global_attribute5,
                      leg_books_global_attribute6,
                      leg_books_global_attribute7,
                      leg_books_global_attribute8,
                      leg_books_global_attribute9,
                      leg_books_global_attribute10,
                      leg_books_global_attribute11,
                      leg_books_global_attribute12,
                      leg_books_global_attribute13,
                      leg_books_global_attribute14,
                      leg_books_global_attribute15,
                      leg_books_global_attribute16,
                      leg_books_global_attribute17,
                      leg_books_global_attribute18,
                      leg_books_global_attribute19,
                      leg_books_global_attribute20,
                      books_global_attribute1,
                      books_global_attribute2,
                      books_global_attribute3,
                      books_global_attribute4,
                      books_global_attribute5,
                      books_global_attribute6,
                      books_global_attribute7,
                      books_global_attribute8,
                      books_global_attribute9,
                      books_global_attribute10,
                      books_global_attribute11,
                      books_global_attribute12,
                      books_global_attribute13,
                      books_global_attribute14,
                      books_global_attribute15,
                      books_global_attribute16,
                      books_global_attribute17,
                      books_global_attribute18,
                      books_global_attribute19,
                      books_global_attribute20,
                      books_global_attr_category,
                      leg_books_global_attr_category,
                      batch_id,
                      process_flag,
                      run_sequence_id
        FROM xxfa_tax_asset_stg
       WHERE process_flag = 'V'
        AND leg_source_system in ('ISSC','FSC')   ---added to get the data only for Global Conversion
         AND batch_id = g_batch_id;

    -- AND run_sequence_id = g_new_run_seq_id;
    l_trans_rec           apps.fa_api_types.trans_rec_type;
    l_dist_trans_rec      apps.fa_api_types.trans_rec_type;
    l_asset_hdr_rec       apps.fa_api_types.asset_hdr_rec_type;
    l_asset_desc_rec      apps.fa_api_types.asset_desc_rec_type;
    l_asset_cat_rec       apps.fa_api_types.asset_cat_rec_type;
    l_asset_type_rec      apps.fa_api_types.asset_type_rec_type;
    l_asset_hierarchy_rec apps.fa_api_types.asset_hierarchy_rec_type;
    l_asset_fin_rec       apps.fa_api_types.asset_fin_rec_type;
    l_asset_deprn_rec     apps.fa_api_types.asset_deprn_rec_type;
    l_asset_dist_rec      apps.fa_api_types.asset_dist_rec_type;
    l_asset_dist_tbl      apps.fa_api_types.asset_dist_tbl_type;
    l_inv_tbl             apps.fa_api_types.inv_tbl_type;
    --l_inv_rate_tbl        apps.FA_API_TYPES.inv_rate_tbl_type;
    l_inv_rec              apps.fa_api_types.inv_rec_type;
    ln_rec_count           NUMBER;
    l_return_status        VARCHAR2(1);
    l_mesg_count           NUMBER := 0;
    l_mesg                 VARCHAR2(32000);
    l_msg_data             VARCHAR2(32000);
    l_record_count         NUMBER := 0;
    ln_tax_asset_id        NUMBER;
    ln_tax_parent_asset_id NUMBER;
    l_no_corp_flag         VARCHAR2(1) := 'N';
  BEGIN
    g_ret_code := NULL;

    FOR rec_cur_conv_tax_asset IN cur_conv_tax_asset LOOP
      l_trans_rec            := NULL;
      l_dist_trans_rec       := NULL;
      l_asset_hdr_rec        := NULL;
      l_asset_desc_rec       := NULL;
      l_asset_cat_rec        := NULL;
      l_asset_type_rec       := NULL;
      l_asset_hierarchy_rec  := NULL;
      l_asset_fin_rec        := NULL;
      l_asset_deprn_rec      := NULL;
      l_asset_dist_rec       := NULL;
      l_inv_rec              := NULL;
      ln_rec_count           := NULL;
      l_return_status        := NULL;
      l_mesg_count           := NULL;
      l_mesg                 := NULL;
      l_msg_data             := NULL;
      l_record_count         := NULL;
      ln_tax_asset_id        := NULL;
      ln_tax_parent_asset_id := NULL;
      l_no_corp_flag         := 'N';

      BEGIN
        SELECT asset_id
          INTO ln_tax_asset_id
          FROM fa_additions_b
         WHERE asset_number = rec_cur_conv_tax_asset.leg_asset_number;
      EXCEPTION
        WHEN NO_DATA_FOUND THEN
          log_errors(pin_interface_txn_id    => NULL,
                     piv_source_table        => 'XXFA_TAX_ASSET_STG',
                     piv_source_column_name  => NULL,
                     piv_source_column_value => NULL,
                     piv_source_keyname1     => 'leg_asset_number',
                     piv_source_keyvalue1    => rec_cur_conv_tax_asset.leg_asset_number,
                     piv_source_keyname2     => 'book_type_code',
                     --piv_source_keyvalue2         => rec_cur_conv_tax_asset.leg_book_type_code,   -- V1.2 Harjinder Singh
                     piv_source_keyvalue2 => rec_cur_conv_tax_asset.book_type_code,
                     piv_error_type       => 'API_ERR',
                     piv_error_code       => 'ETN_FA_DEPDNT_CORP_ASSET',
                     piv_error_message    => 'Error : Tax record doesnt have a successful corporate record: ');
          l_no_corp_flag := 'Y';
        WHEN OTHERS THEN
          l_no_corp_flag := 'Y';
          log_errors(pin_interface_txn_id    => NULL,
                     piv_source_table        => 'XXFA_TAX_ASSET_STG',
                     piv_source_column_name  => NULL,
                     piv_source_column_value => NULL,
                     piv_source_keyname1     => 'leg_asset_number',
                     piv_source_keyvalue1    => rec_cur_conv_tax_asset.leg_asset_number,
                     piv_source_keyname2     => 'book_type_code',
                     -- piv_source_keyvalue2         => rec_cur_conv_tax_asset.leg_book_type_code, -- V1.2 Harjinder Singh
                     piv_source_keyvalue2 => rec_cur_conv_tax_asset.book_type_code,
                     piv_error_type       => 'API_ERR',
                     piv_error_code       => 'ETN_FA_DEPDNT_CORP_ASSET',
                     piv_error_message    => 'Error :EXCEPTION Error while fetching asset id : ');
      END;

      IF l_no_corp_flag = 'Y' THEN
        BEGIN
          --- updating the error records
          UPDATE xxfa_tax_asset_stg
             SET process_flag      = 'E',
                 ERROR_TYPE        = 'API_ERR',
                 request_id        = g_request_id,
                 last_updated_date = SYSDATE,
                 last_updated_by   = g_last_updated_by,
                 last_update_login = g_last_update_login
           WHERE leg_asset_number = rec_cur_conv_tax_asset.leg_asset_number
                --AND leg_book_type_code =rec_cur_conv_tax_asset.leg_book_type_code -- V1.2 Harjinder Singh
             AND book_type_code = rec_cur_conv_tax_asset.book_type_code
             AND process_flag = 'V'
             AND batch_id = g_batch_id;
        EXCEPTION
          WHEN OTHERS THEN
            log_errors(pin_interface_txn_id    => NULL,
                       piv_source_table        => 'XXFA_TAX_ASSET_STG',
                       piv_source_column_name  => NULL,
                       piv_source_column_value => NULL,
                       piv_source_keyname1     => 'leg_asset_number',
                       piv_source_keyvalue1    => rec_cur_conv_tax_asset.leg_asset_number,
                       piv_error_type          => 'API_ERR',
                       piv_error_code          => 'ETN_FA_INVALID_PROC',
                       piv_error_message       => 'Error : Exception occured while updating XXFA_TAX_ASSET_STG for errors(No Corp Book)  : ' ||
                                                  SUBSTR(SQLERRM, 1, 240));
        END;
      ELSE
        --Asset  Detail
        l_asset_desc_rec.property_type_code      := rec_cur_conv_tax_asset.leg_property_type_code;
        l_asset_desc_rec.property_1245_1250_code := rec_cur_conv_tax_asset.leg_property_1245_1250_code;
        l_asset_desc_rec.in_use_flag             := rec_cur_conv_tax_asset.leg_in_use_flag;
        l_asset_desc_rec.description             := rec_cur_conv_tax_asset.leg_description;
        l_asset_desc_rec.tag_number              := rec_cur_conv_tax_asset.leg_tag_number;
        l_asset_desc_rec.serial_number           := rec_cur_conv_tax_asset.leg_serial_number;
        l_asset_desc_rec.manufacturer_name       := rec_cur_conv_tax_asset.leg_manufacturer_name;
        l_asset_desc_rec.model_number            := rec_cur_conv_tax_asset.leg_model_number;
        l_asset_desc_rec.owned_leased            := rec_cur_conv_tax_asset.leg_owned_leased;
        l_asset_desc_rec.new_used                := rec_cur_conv_tax_asset.leg_new_used;
        l_asset_desc_rec.unit_adjustment_flag    := rec_cur_conv_tax_asset.leg_unit_adjustment_flag;
        l_asset_desc_rec.add_cost_je_flag        := rec_cur_conv_tax_asset.leg_add_cost_je_flag;
        l_asset_desc_rec.current_units           := rec_cur_conv_tax_asset.leg_current_units;
        l_asset_desc_rec.inventorial             := rec_cur_conv_tax_asset.leg_inventorial;
        l_asset_desc_rec.commitment              := rec_cur_conv_tax_asset.leg_commitment;
        l_asset_desc_rec.investment_law          := rec_cur_conv_tax_asset.leg_investment_law;
        l_asset_cat_rec.category_id              := rec_cur_conv_tax_asset.asset_category_id;
        l_asset_type_rec.asset_type              := rec_cur_conv_tax_asset.leg_asset_type;
        ----attributes value for category dff
        l_asset_cat_rec.desc_flex.CONTEXT     := rec_cur_conv_tax_asset.adtn_context;
        l_asset_cat_rec.desc_flex.attribute1  := rec_cur_conv_tax_asset.adtn_attribute1;
        l_asset_cat_rec.desc_flex.attribute2  := rec_cur_conv_tax_asset.adtn_attribute2;
        l_asset_cat_rec.desc_flex.attribute3  := rec_cur_conv_tax_asset.adtn_attribute3;
        l_asset_cat_rec.desc_flex.attribute4  := rec_cur_conv_tax_asset.adtn_attribute4;
        l_asset_cat_rec.desc_flex.attribute5  := rec_cur_conv_tax_asset.adtn_attribute5;
        l_asset_cat_rec.desc_flex.attribute6  := rec_cur_conv_tax_asset.adtn_attribute6;
        l_asset_cat_rec.desc_flex.attribute7  := rec_cur_conv_tax_asset.adtn_attribute7;
        l_asset_cat_rec.desc_flex.attribute8  := rec_cur_conv_tax_asset.adtn_attribute8;
        l_asset_cat_rec.desc_flex.attribute9  := rec_cur_conv_tax_asset.adtn_attribute9;
        l_asset_cat_rec.desc_flex.attribute10 := rec_cur_conv_tax_asset.adtn_attribute10;
        l_asset_cat_rec.desc_flex.attribute11 := rec_cur_conv_tax_asset.adtn_attribute11;
        l_asset_cat_rec.desc_flex.attribute12 := rec_cur_conv_tax_asset.adtn_attribute12;
        l_asset_cat_rec.desc_flex.attribute13 := rec_cur_conv_tax_asset.adtn_attribute13;
        l_asset_cat_rec.desc_flex.attribute14 := rec_cur_conv_tax_asset.adtn_attribute14;
        l_asset_cat_rec.desc_flex.attribute15 := rec_cur_conv_tax_asset.adtn_attribute15;
        l_asset_cat_rec.desc_flex.attribute16 := rec_cur_conv_tax_asset.adtn_attribute16;
        l_asset_cat_rec.desc_flex.attribute17 := rec_cur_conv_tax_asset.adtn_attribute17;
        l_asset_cat_rec.desc_flex.attribute18 := rec_cur_conv_tax_asset.adtn_attribute18;
        l_asset_cat_rec.desc_flex.attribute19 := rec_cur_conv_tax_asset.adtn_attribute19;
        l_asset_cat_rec.desc_flex.attribute20 := rec_cur_conv_tax_asset.adtn_attribute20;
        l_asset_cat_rec.desc_flex.attribute21 := rec_cur_conv_tax_asset.adtn_attribute21;
        l_asset_cat_rec.desc_flex.attribute22 := rec_cur_conv_tax_asset.adtn_attribute22;
        l_asset_cat_rec.desc_flex.attribute23 := rec_cur_conv_tax_asset.adtn_attribute23;
        l_asset_cat_rec.desc_flex.attribute24 := rec_cur_conv_tax_asset.adtn_attribute24;
        l_asset_cat_rec.desc_flex.attribute25 := rec_cur_conv_tax_asset.adtn_attribute25;
        l_asset_cat_rec.desc_flex.attribute26 := rec_cur_conv_tax_asset.adtn_attribute26;
        l_asset_cat_rec.desc_flex.attribute27 := rec_cur_conv_tax_asset.adtn_attribute27;
        l_asset_cat_rec.desc_flex.attribute28 := rec_cur_conv_tax_asset.adtn_attribute28;
        l_asset_cat_rec.desc_flex.attribute29 := rec_cur_conv_tax_asset.adtn_attribute29;
        l_asset_cat_rec.desc_flex.attribute30 := rec_cur_conv_tax_asset.adtn_attribute30;
        --- Global Attributes
        l_asset_desc_rec.global_desc_flex.attribute1              := rec_cur_conv_tax_asset.adtn_global_attribute1;
        l_asset_desc_rec.global_desc_flex.attribute2              := rec_cur_conv_tax_asset.adtn_global_attribute2;
        l_asset_desc_rec.global_desc_flex.attribute3              := rec_cur_conv_tax_asset.adtn_global_attribute3;
        l_asset_desc_rec.global_desc_flex.attribute4              := rec_cur_conv_tax_asset.adtn_global_attribute4;
        l_asset_desc_rec.global_desc_flex.attribute5              := rec_cur_conv_tax_asset.adtn_global_attribute5;
        l_asset_desc_rec.global_desc_flex.attribute6              := rec_cur_conv_tax_asset.adtn_global_attribute6;
        l_asset_desc_rec.global_desc_flex.attribute7              := rec_cur_conv_tax_asset.adtn_global_attribute7;
        l_asset_desc_rec.global_desc_flex.attribute8              := rec_cur_conv_tax_asset.adtn_global_attribute8;
        l_asset_desc_rec.global_desc_flex.attribute9              := rec_cur_conv_tax_asset.adtn_global_attribute9;
        l_asset_desc_rec.global_desc_flex.attribute10             := rec_cur_conv_tax_asset.adtn_global_attribute10;
        l_asset_desc_rec.global_desc_flex.attribute11             := rec_cur_conv_tax_asset.adtn_global_attribute11;
        l_asset_desc_rec.global_desc_flex.attribute12             := rec_cur_conv_tax_asset.adtn_global_attribute12;
        l_asset_desc_rec.global_desc_flex.attribute13             := rec_cur_conv_tax_asset.adtn_global_attribute13;
        l_asset_desc_rec.global_desc_flex.attribute14             := rec_cur_conv_tax_asset.adtn_global_attribute14;
        l_asset_desc_rec.global_desc_flex.attribute15             := rec_cur_conv_tax_asset.adtn_global_attribute15;
        l_asset_desc_rec.global_desc_flex.attribute16             := rec_cur_conv_tax_asset.adtn_global_attribute16;
        l_asset_desc_rec.global_desc_flex.attribute17             := rec_cur_conv_tax_asset.adtn_global_attribute17;
        l_asset_desc_rec.global_desc_flex.attribute18             := rec_cur_conv_tax_asset.adtn_global_attribute18;
        l_asset_desc_rec.global_desc_flex.attribute19             := rec_cur_conv_tax_asset.adtn_global_attribute19;
        l_asset_desc_rec.global_desc_flex.attribute20             := rec_cur_conv_tax_asset.adtn_global_attribute20;
        l_asset_desc_rec.global_desc_flex.attribute_category_code := rec_cur_conv_tax_asset.adtn_global_attribute_category;
        --books
        ---- l_asset_hdr_rec.book_type_code := rec_cur_conv_tax_asset.leg_book_type_code;   --- v1.2 Harjinder Singh
        l_asset_hdr_rec.book_type_code := rec_cur_conv_tax_asset.book_type_code;
        l_asset_hdr_rec.asset_id       := ln_tax_asset_id;
        --financial information
        l_asset_fin_rec.COST                   := rec_cur_conv_tax_asset.leg_cost;
        l_asset_fin_rec.original_cost          := rec_cur_conv_tax_asset.leg_original_cost;
        l_asset_fin_rec.date_placed_in_service := rec_cur_conv_tax_asset.leg_date_placed_in_service;
        l_asset_fin_rec.depreciate_flag        := rec_cur_conv_tax_asset.leg_depreciate_flag;
        l_asset_fin_rec.salvage_value          := rec_cur_conv_tax_asset.leg_salvage_value;
        l_asset_fin_rec.life_in_months         := rec_cur_conv_tax_asset.leg_life_in_months;
        --l_asset_fin_rec.deprn_start_date :=
        --                      rec_cur_conv_tax_asset.leg_deprn_start_date;
        l_asset_fin_rec.deprn_method_code := rec_cur_conv_tax_asset.leg_deprn_method_code;
        -- l_asset_fin_rec.rate_adjustment_factor :=
        --                rec_cur_conv_tax_asset.leg_rate_adjustment_factor;
        -- l_asset_fin_rec.adjusted_cost :=
        --                          rec_cur_conv_tax_asset.leg_adjusted_cost;
        l_asset_fin_rec.prorate_convention_code := rec_cur_conv_tax_asset.leg_prorate_convention_code;
        -- l_asset_fin_rec.prorate_date :=
        --                           rec_cur_conv_tax_asset.leg_prorate_date;
        l_asset_fin_rec.cost_change_flag           := rec_cur_conv_tax_asset.leg_cost_change_flag;
        l_asset_fin_rec.adjustment_required_status := rec_cur_conv_tax_asset.leg_adjustment_required_status;
        l_asset_fin_rec.capitalize_flag            := rec_cur_conv_tax_asset.leg_capitalize_flag;
        l_asset_fin_rec.retirement_pending_flag    := rec_cur_conv_tax_asset.leg_retirement_pending_flag;
        l_asset_fin_rec.basic_rate                 := rec_cur_conv_tax_asset.leg_basic_rate;
        l_asset_fin_rec.adjusted_rate              := rec_cur_conv_tax_asset.leg_adjusted_rate;
        l_asset_fin_rec.bonus_rule                 := rec_cur_conv_tax_asset.leg_bonus_rule;
        l_asset_fin_rec.ceiling_name               := rec_cur_conv_tax_asset.leg_ceiling_name;
        -- l_asset_fin_rec.recoverable_cost :=
        --                      rec_cur_conv_tax_asset.leg_recoverable_cost;
        l_asset_fin_rec.unrevalued_cost            := rec_cur_conv_tax_asset.leg_unrevalued_cost;
        l_asset_fin_rec.annual_deprn_rounding_flag := rec_cur_conv_tax_asset.leg_annual_deprn_rounding_flag;
        l_asset_fin_rec.percent_salvage_value      := rec_cur_conv_tax_asset.leg_percent_salvage_value;
        l_asset_fin_rec.allowed_deprn_limit        := rec_cur_conv_tax_asset.leg_allowed_deprn_limit;
        l_asset_fin_rec.allowed_deprn_limit_amount := rec_cur_conv_tax_asset.leg_allowed_deprn_limit_amount;
        l_asset_fin_rec.salvage_type               := rec_cur_conv_tax_asset.leg_salvage_type;
        l_asset_fin_rec.deprn_limit_type           := rec_cur_conv_tax_asset.leg_deprn_limit_type;
        ---global attributes for fa books
        -- l_asset_fin_rec.global_attribute1 :=
        --                    rec_cur_conv_tax_asset.books_global_attribute1;
        l_asset_fin_rec.global_attribute2  := rec_cur_conv_tax_asset.books_global_attribute2;
        l_asset_fin_rec.global_attribute3  := rec_cur_conv_tax_asset.books_global_attribute3;
        l_asset_fin_rec.global_attribute4  := rec_cur_conv_tax_asset.books_global_attribute4;
        l_asset_fin_rec.global_attribute5  := rec_cur_conv_tax_asset.books_global_attribute5;
        l_asset_fin_rec.global_attribute6  := rec_cur_conv_tax_asset.books_global_attribute6;
        l_asset_fin_rec.global_attribute7  := rec_cur_conv_tax_asset.books_global_attribute7;
        l_asset_fin_rec.global_attribute8  := rec_cur_conv_tax_asset.books_global_attribute8;
        l_asset_fin_rec.global_attribute9  := rec_cur_conv_tax_asset.books_global_attribute9;
        l_asset_fin_rec.global_attribute10 := rec_cur_conv_tax_asset.books_global_attribute10;
        l_asset_fin_rec.global_attribute11 := rec_cur_conv_tax_asset.books_global_attribute11;
        l_asset_fin_rec.global_attribute12 := rec_cur_conv_tax_asset.books_global_attribute12;
        l_asset_fin_rec.global_attribute13 := rec_cur_conv_tax_asset.books_global_attribute13;
        l_asset_fin_rec.global_attribute14 := rec_cur_conv_tax_asset.books_global_attribute14;
        l_asset_fin_rec.global_attribute15 := rec_cur_conv_tax_asset.books_global_attribute15;
        l_asset_fin_rec.global_attribute16 := rec_cur_conv_tax_asset.books_global_attribute16;
        l_asset_fin_rec.global_attribute17 := rec_cur_conv_tax_asset.books_global_attribute17;
        l_asset_fin_rec.global_attribute18 := rec_cur_conv_tax_asset.books_global_attribute18;
        l_asset_fin_rec.global_attribute19 := rec_cur_conv_tax_asset.books_global_attribute19;
        l_asset_fin_rec.global_attribute20 := rec_cur_conv_tax_asset.books_global_attribute20;
        --  l_asset_fin_rec.global_attribute_category :=
        --              rec_cur_conv_tax_asset.books_global_attr_category;
        ---depreciation info
        l_asset_deprn_rec.deprn_amount  := rec_cur_conv_tax_asset.leg_deprn_amount;
        l_asset_deprn_rec.ytd_deprn     := rec_cur_conv_tax_asset.leg_ytd_deprn;
        l_asset_deprn_rec.deprn_reserve := rec_cur_conv_tax_asset.leg_deprn_reserve;
        -- transaction data
        l_trans_rec.transaction_date_entered := NULL;
        -- l_trans_rec.transaction_type_code :=
        --                  rec_cur_conv_tax_asset.leg_transaction_type_code;
        --- v1.9  commented by Reshu 16 May 2016  Start -----
        --l_trans_rec.transaction_subtype        := g_txn_sub_type;
        --l_trans_rec.amortization_start_date    := TO_DATE(g_period_date,'MON-YYYY');
        --- v1.9  commented by Reshu 16 May 2016  end  -----
        l_trans_rec.who_info.last_update_date  := SYSDATE;
        l_trans_rec.who_info.last_updated_by   := g_last_updated_by;
        l_trans_rec.who_info.created_by        := g_created_by;
        l_trans_rec.who_info.creation_date     := SYSDATE;
        l_trans_rec.who_info.last_update_login := g_last_update_login;
        apps.fnd_msg_pub.initialize;
        apps.fa_addition_pub.do_addition(
                                         -- std parameters
                                         p_api_version      => 1.0,
                                         p_init_msg_list    => apps.fnd_api.g_false,
                                         p_commit           => apps.fnd_api.g_false,
                                         p_validation_level => apps.fnd_api.g_valid_level_full,
                                         p_calling_fn       => NULL,
                                         x_return_status    => l_return_status,
                                         x_msg_count        => l_mesg_count,
                                         x_msg_data         => l_mesg,
                                         -- api parameters
                                         px_trans_rec           => l_trans_rec,
                                         px_dist_trans_rec      => l_dist_trans_rec,
                                         px_asset_hdr_rec       => l_asset_hdr_rec,
                                         px_asset_desc_rec      => l_asset_desc_rec,
                                         px_asset_type_rec      => l_asset_type_rec,
                                         px_asset_cat_rec       => l_asset_cat_rec,
                                         px_asset_hierarchy_rec => l_asset_hierarchy_rec,
                                         px_asset_fin_rec       => l_asset_fin_rec,
                                         px_asset_deprn_rec     => l_asset_deprn_rec,
                                         px_asset_dist_tbl      => l_asset_dist_tbl,
                                         px_inv_tbl             => l_inv_tbl);

        IF l_return_status <> 'S' THEN
          g_ret_code := 1;

          --- updating the error records
          BEGIN
            UPDATE xxfa_tax_asset_stg
               SET process_flag      = 'E',
                   ERROR_TYPE        = 'API_ERR',
                   request_id        = g_request_id,
                   last_updated_date = SYSDATE,
                   last_updated_by   = g_last_updated_by,
                   last_update_login = g_last_update_login
             WHERE leg_asset_number =
                   rec_cur_conv_tax_asset.leg_asset_number
                  -- AND leg_book_type_code =rec_cur_conv_tax_asset.leg_book_type_code   -- v1.2 Harjinder Singh
               AND book_type_code = rec_cur_conv_tax_asset.book_type_code
               AND process_flag = 'V'
               AND batch_id = g_batch_id;
          EXCEPTION
            WHEN OTHERS THEN
              log_errors(pin_interface_txn_id    => NULL,
                         piv_source_table        => 'XXFA_TAX_ASSET_STG',
                         piv_source_column_name  => NULL,
                         piv_source_column_value => NULL,
                         piv_source_keyname1     => 'leg_asset_number',
                         piv_source_keyvalue1    => rec_cur_conv_tax_asset.leg_asset_number,
                         piv_error_type          => 'API_ERR',
                         piv_error_code          => 'ETN_FA_INVALID_PROC',
                         piv_error_message       => 'Error : Exception occured while updating XXFA_TAX_ASSET_STG for errors in import process  : ' ||
                                                    SUBSTR(SQLERRM, 1, 240));
          END;

          --- error msg
          IF l_mesg_count > 1 THEN
            FOR i IN 1 .. (l_mesg_count - 1) LOOP
              l_mesg     := apps.fnd_msg_pub.get(apps.fnd_msg_pub.g_next,
                                                 apps.fnd_api.g_false);
              l_msg_data := l_msg_data || '-' || l_mesg;
              log_errors(pin_interface_txn_id    => NULL,
                         piv_source_table        => 'XXFA_TAX_ASSET_STG',
                         piv_source_column_name  => NULL,
                         piv_source_column_value => NULL,
                         piv_source_keyname1     => 'leg_asset_number',
                         piv_source_keyvalue1    => rec_cur_conv_tax_asset.leg_asset_number,
                         piv_source_keyname2     => 'book_type_code',
                         --piv_source_keyvalue2         => rec_cur_conv_tax_asset.leg_book_type_code,  --v1.2 Harjinder Singh
                         piv_source_keyvalue2 => rec_cur_conv_tax_asset.book_type_code,
                         piv_error_type       => 'API_ERR',
                         piv_error_code       => 'ETN_FA_IMPORT_ERR',
                         piv_error_message    => l_msg_data);
            END LOOP;
          ELSE
            l_msg_data := apps.fnd_msg_pub.get(apps.fnd_msg_pub.g_first,
                                               apps.fnd_api.g_false);
            log_errors(pin_interface_txn_id    => NULL,
                       piv_source_table        => 'XXFA_TAX_ASSET_STG',
                       piv_source_column_name  => NULL,
                       piv_source_column_value => NULL,
                       piv_source_keyname1     => 'leg_asset_number',
                       piv_source_keyvalue1    => rec_cur_conv_tax_asset.leg_asset_number,
                       piv_source_keyname2     => 'book_type_code',
                       --piv_source_keyvalue2         => rec_cur_conv_tax_asset.leg_book_type_code, --v1.2 Harjinder Singh
                       piv_source_keyvalue2 => rec_cur_conv_tax_asset.book_type_code,
                       piv_error_type       => 'API_ERR',
                       piv_error_code       => 'ETN_FA_IMPORT_ERR',
                       piv_error_message    => l_msg_data);
          END IF;
        ELSE
          BEGIN
            UPDATE xxfa_tax_asset_stg
               SET process_flag      = 'C',
                   asset_id          = l_asset_hdr_rec.asset_id,
                   request_id        = g_request_id,
                   last_updated_date = SYSDATE,
                   last_updated_by   = g_last_updated_by,
                   last_update_login = g_last_update_login
             WHERE leg_asset_number =
                   rec_cur_conv_tax_asset.leg_asset_number
                  --AND leg_book_type_code = rec_cur_conv_tax_asset.leg_book_type_code   --v1.2 Harjinder Singh
               AND book_type_code = rec_cur_conv_tax_asset.book_type_code
               AND process_flag = 'V'
               AND batch_id = g_batch_id;
          EXCEPTION
            WHEN OTHERS THEN
              log_errors(pin_interface_txn_id    => NULL,
                         piv_source_table        => 'XXFA_TAX_ASSET_STG',
                         piv_source_column_name  => NULL,
                         piv_source_column_value => NULL,
                         piv_source_keyname1     => 'leg_asset_number',
                         piv_source_keyvalue1    => rec_cur_conv_tax_asset.leg_asset_number,
                         piv_error_type          => 'API_ERR',
                         piv_error_code          => 'ETN_FA_INVALID_PROC',
                         piv_error_message       => 'Error : Exception occured while updating XXFA_TAX_ASSET_STG for completion in import process  : ' ||
                                                    SUBSTR(SQLERRM, 1, 240));
          END;
        END IF;
      END IF;
    END LOOP;

    xxetn_debug_pkg.add_debug(piv_debug_msg => 'Conversion Process for assets with Tax book Ends : ');
    COMMIT;
  EXCEPTION
    WHEN OTHERS THEN
      g_ret_code := 2;
      print_log_message('Unexpected error while import for tax book : ' ||
                        SUBSTR(SQLERRM, 1, 240));
      print_log_message('Error : Backtrace in import for tax : ' ||
                        DBMS_UTILITY.format_error_backtrace);
  END import_tax;

  --
  -- ========================
  -- Procedure: PRINT_REPORT
  -- =============================================================================
  --   This procedure is used to print statistics after end of validate,
  --   conversion and reconcile mode
  -- =============================================================================
  PROCEDURE print_report(pin_total_count_header  IN NUMBER,
                         pin_total_count_lines   IN NUMBER,
                         pin_suc_count_head      IN NUMBER,
                         pin_suc_count_line      IN NUMBER,
                         pin_failed_count_header IN NUMBER,
                         pin_failed_count_lines  IN NUMBER,
                         pin_fail_count_imp_head IN NUMBER,
                         pin_fail_count_imp_line IN NUMBER) IS
  BEGIN
    xxetn_debug_pkg.add_debug(piv_debug_msg => 'Inside Print_report procedure');
    fnd_file.put_line(fnd_file.output,
                      'Program Name : Eaton Fixed Asset Conversion Program');
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
    fnd_file.put_line(fnd_file.output,
                      'Total Records for Corporate Book                 : ' ||
                      pin_total_count_header);
    fnd_file.put_line(fnd_file.output,
                      'Total Records for Tax Book              : ' ||
                      pin_total_count_lines);
    fnd_file.put_line(fnd_file.output,
                      'Records Converted for Corporate Book         : ' ||
                      pin_suc_count_head);
    fnd_file.put_line(fnd_file.output,
                      'Records Converted for Tax Book          : ' ||
                      pin_suc_count_line);
    fnd_file.put_line(fnd_file.output,
                      'Records Erred in Validation for Corporate Book : ' ||
                      pin_failed_count_header);
    fnd_file.put_line(fnd_file.output,
                      'Records Erred in Validation for Tax Book  : ' ||
                      pin_failed_count_lines);
    fnd_file.put_line(fnd_file.output,
                      'Records Erred in Conversion for Corporate Book : ' ||
                      pin_fail_count_imp_head);
    fnd_file.put_line(fnd_file.output,
                      'Records Erred in Conversion for Tax Book  : ' ||
                      pin_fail_count_imp_line);
    fnd_file.put_line(fnd_file.output,
                      '=============================================================================================');
  EXCEPTION
    WHEN OTHERS THEN
      print_log_message('ERROR : Error occured in print reports procedure' ||
                        SUBSTR(SQLERRM, 1, 150));
  END print_report;

  --
  -- ========================
  -- Procedure: PRE_VALIDATE
  -- =====================================================================================
  --   This procedure is used to validate set ups for FA
  -- =====================================================================================
  PROCEDURE pre_validate IS
    ln_asset_key         NUMBER := NULL;
    ln_cat_count         NUMBER := NULL;
    ln_prop_count        NUMBER := NULL;
    ln_p1245_count       NUMBER := NULL;
    ln_ol_count          NUMBER := NULL;
    ln_nu_count          NUMBER := NULL;
    ln_emp_count         NUMBER := NULL;
    ln_method_count      NUMBER := NULL;
    ln_conv_count        NUMBER := NULL;
    ln_bonus_rules_count NUMBER := NULL;
    ln_ceilings_count    NUMBER := NULL;
    l_book_count         NUMBER := NULL;

    CURSOR cur_period IS
      SELECT book_type_code,
             TO_CHAR(LAST_DAY(initial_date) + 1, 'MON-YYYY') period
        FROM fa_book_controls;

    --    V1.2
    CURSOR cur_book_r12_code IS
      SELECT tag book_type_code
        FROM fnd_lookup_values flv
       WHERE lookup_type = g_lookup_r12_type
         AND TRUNC(SYSDATE) BETWEEN
             NVL(flv.start_date_active, TRUNC(SYSDATE)) AND
             NVL(flv.end_date_active, TRUNC(SYSDATE));

  BEGIN
    g_ret_code := 0;

    --- Category lookup check
    BEGIN
      print_log_message('Check for Category transformation lookup ETN_FA_CATEGORY_MAP');

      SELECT 1
        INTO ln_cat_count
        FROM fnd_lookup_types flv
       WHERE UPPER(flv.lookup_type) = g_cat_lookup_others;

      print_log_message('Category lookup is present ');
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        print_log_message('Category lookup is missing : ETN_FA_CATEGORY_MAP');
        g_ret_code := 1;
      WHEN OTHERS THEN
        print_log_message('Error : Exception occured in Category lookup ' ||
                          SUBSTR(SQLERRM, 1, 240));
        g_ret_code := 1;
    END;

    ----------------------------Added as per v1.5-------------------------------
    BEGIN
      print_log_message('Check for Category transformation lookup ETN_FA_CATEGORY_MAP_LOCAL');

      SELECT 1
        INTO ln_cat_count
        FROM fnd_lookup_types flv
       WHERE UPPER(flv.lookup_type) = g_cat_lookup_local;

      print_log_message('Category lookup is present ');
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        print_log_message('Category lookup is missing : ETN_FA_CATEGORY_MAP_LOCAL');
        g_ret_code := 1;
      WHEN OTHERS THEN
        print_log_message('Error : Exception occured in Category lookup ' ||
                          SUBSTR(SQLERRM, 1, 240));
        g_ret_code := 1;
    END;
    --------------------------------Added as per v1.5-------------------------------------
    --------------------------------Added as per v1.5-------------------------------------
    BEGIN
      print_log_message('Check for Category transformation lookup ETN_FA_CATEGORY_MAP_US');

      SELECT 1
        INTO ln_cat_count
        FROM fnd_lookup_types flv
       WHERE UPPER(flv.lookup_type) = g_cat_lookup_us;

      print_log_message('Category lookup is present ');
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        print_log_message('Category lookup is missing : ETN_FA_CATEGORY_MAP_US');
        g_ret_code := 1;
      WHEN OTHERS THEN
        print_log_message('Error : Exception occured in Category lookup ' ||
                          SUBSTR(SQLERRM, 1, 240));
        g_ret_code := 1;
    END;
    -----------------------------Added as per v1.5-------------------------------------------
    --- Property type code lookup
    BEGIN
      print_log_message('Check for PROPERTY TYPE lookup : ');

      SELECT 1
        INTO ln_prop_count
        FROM fa_lookup_types flv
       WHERE UPPER(flv.lookup_type) = UPPER('PROPERTY TYPE');

      print_log_message('PROPERTY TYPE lookup is present');
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        print_log_message('PROPERTY TYPE lookup is missing');
        g_ret_code := 1;
      WHEN OTHERS THEN
        print_log_message('Error : Exception occured in PROPERTY TYPE lookup ' ||
                          SUBSTR(SQLERRM, 1, 240));
        g_ret_code := 1;
    END;

    --- Property 1245/1250 code lookup
    BEGIN
      print_log_message('Check for Property 1245/1250 lookup : ');

      SELECT 1
        INTO ln_p1245_count
        FROM fa_lookup_types flv
       WHERE UPPER(flv.lookup_type) = UPPER('1245/1250 PROPERTY');

      print_log_message('Property 1245/1250 lookup is present');
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        print_log_message('Property 1245/1250 lookup is missing');
        g_ret_code := 1;
      WHEN OTHERS THEN
        print_log_message('Error : Exception occured in Property 1245/1250 lookup ' ||
                          SUBSTR(SQLERRM, 1, 240));
        g_ret_code := 1;
    END;

    --- OWNLEASE code lookup
    BEGIN
      print_log_message('Check for OWNLEASE lookup : ');

      SELECT 1
        INTO ln_ol_count
        FROM fa_lookup_types flv
       WHERE UPPER(flv.lookup_type) = UPPER('OWNLEASE');

      print_log_message('OWNLEASE lookup is present');
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        print_log_message('OWNLEASE lookup is missing');
        g_ret_code := 1;
      WHEN OTHERS THEN
        print_log_message('Error : Exception occured in OWNLEASE lookup ' ||
                          SUBSTR(SQLERRM, 1, 240));
        g_ret_code := 1;
    END;

    --- NEWUSE code lookup
    BEGIN
      print_log_message('Check for NEWUSE lookup : ');

      SELECT 1
        INTO ln_nu_count
        FROM fa_lookup_types flv
       WHERE UPPER(flv.lookup_type) = UPPER('NEWUSE');

      print_log_message('NEWUSE lookup is present');
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        print_log_message('NEWUSE lookup is missing');
        g_ret_code := 1;
      WHEN OTHERS THEN
        print_log_message('Error : Exception occured in NEWUSE lookup ' ||
                          SUBSTR(SQLERRM, 1, 240));
        g_ret_code := 1;
    END;

    -- Employees check
    BEGIN
      print_log_message('Check for Employees setup :');

      SELECT COUNT(1) INTO ln_emp_count FROM fa_employees;

      IF ln_emp_count = 0 THEN
        print_log_message('Employees setup missing');
        g_ret_code := 1;
      ELSE
        print_log_message('Employees setup present');
      END IF;
    EXCEPTION
      WHEN OTHERS THEN
        print_log_message('Error : Exception occured in Employees setup :' ||
                          SUBSTR(SQLERRM, 1, 240));
        g_ret_code := 1;
    END;

    -- Depreciation Method code check
    BEGIN
      print_log_message('Check for Depreciation Method code setup : ');

      SELECT COUNT(1) INTO ln_method_count FROM fa_methods;

      IF ln_method_count = 0 THEN
        print_log_message('Depreciation Method code setup missing');
        g_ret_code := 1;
      ELSE
        print_log_message('Depreciation Method code setup present');
      END IF;
    EXCEPTION
      WHEN OTHERS THEN
        print_log_message('Error : Exception occured in Depreciation Method code setup ' ||
                          SUBSTR(SQLERRM, 1, 240));
    END;

    -- Conventions code check
    BEGIN
      print_log_message('Check for Conventions code setup : ');

      SELECT COUNT(1) INTO ln_conv_count FROM fa_convention_types;

      IF ln_conv_count = 0 THEN
        print_log_message('Conventions code setup missing :');
        g_ret_code := 1;
      ELSE
        print_log_message('Conventions code setup present :');
      END IF;
    EXCEPTION
      WHEN OTHERS THEN
        print_log_message('Error : Exception occured in Conventions code setup ' ||
                          SUBSTR(SQLERRM, 1, 240));
        g_ret_code := 1;
    END;

    -- Bonus Rules check
    BEGIN
      print_log_message('Check for Bonus Rules setup : ');

      SELECT COUNT(1) INTO ln_bonus_rules_count FROM fa_bonus_rules;

      IF ln_bonus_rules_count = 0 THEN
        print_log_message('Bonus Rules setup missing');
        g_ret_code := 1;
      ELSE
        print_log_message('Bonus Rules setup present');
      END IF;
    EXCEPTION
      WHEN OTHERS THEN
        print_log_message('Error : Exception occured in Bonus Rules setup ' ||
                          SUBSTR(SQLERRM, 1, 240));
        g_ret_code := 1;
    END;

    -- Ceilings check
    BEGIN
      print_log_message('Check for Ceilings setup : ');

      SELECT COUNT(1) INTO ln_ceilings_count FROM fa_ceilings;

      IF ln_ceilings_count = 0 THEN
        print_log_message('Ceilings setup missing');
        g_ret_code := 1;
      ELSE
        print_log_message('Ceilings setup present');
      END IF;
    EXCEPTION
      WHEN OTHERS THEN
        print_log_message('Error : Exception occured in Ceilings setup ' ||
                          SUBSTR(SQLERRM, 1, 240));
        g_ret_code := 1;
    END;

    -- Check for Book type period and conversion period.
    print_log_message('Checking Book Period against the Conversion Period : ');

    IF g_period_date IS NULL THEN
      print_log_message('Profile Option for Defining Conversion period is not setup');
      g_ret_code := 1;
    ELSE
      FOR rec_cur_period IN cur_period LOOP
        IF rec_cur_period.period <> g_period_date THEN
          print_log_message('Book : ' || rec_cur_period.book_type_code ||
                            ' is not defined for Conversion period : ' ||
                            g_period_date);
          g_ret_code := 1;
        END IF;
      END LOOP;
    END IF;

    BEGIN
      --    V1.2
      FOR rec_cur_book_r12_code IN cur_book_r12_code LOOP
        l_book_count := 0;

        SELECT count(1)
          INTO l_book_count
          FROM fa_book_controls
         WHERE book_type_code = rec_cur_book_r12_code.book_type_code;

        IF l_book_count = 0 THEN
          print_log_message('--------------------------------------------------------------------------------');
          print_log_message(' book_type_code ' ||
                            rec_cur_book_r12_code.book_type_code ||
                            '  Missing in the Setup');
          g_ret_code := 1;
        ELSE
          print_log_message('*******************************************************************************');
          print_log_message(' book_type_code ' ||
                            rec_cur_book_r12_code.book_type_code ||
                            ' Setup is present');

        END IF;
      END LOOP;
    EXCEPTION
      WHEN OTHERS THEN
        print_log_message('Error : Exception occured for book type code ' ||
                          SUBSTR(SQLERRM, 1, 240));
        g_ret_code := 1;
    END;
  EXCEPTION
    WHEN OTHERS THEN
      print_log_message('Error : Exception occured in pre_validate procedure ' ||
                        SUBSTR(SQLERRM, 1, 240));
      g_ret_code := 1;
  END pre_validate;

  --
  -- ========================
  -- Procedure: ASSIGN_BATCH_ID
  -- =====================================================================================
  --   This procedure is used for batch id assignment
  -- =====================================================================================
  PROCEDURE assign_batch_id IS
  BEGIN
    g_err_batch_flag := 'N';

    -- g_batch_id NULL is considered a fresh run
    IF g_batch_id IS NULL THEN
      g_batch_id := xxetn_batches_s.NEXTVAL;

      BEGIN
        xxetn_debug_pkg.add_debug(piv_debug_msg => 'Generating new batch id for Corporate Book');

        UPDATE xxfa_corp_asset_stg
           SET batch_id               = g_batch_id,
               process_flag           = 'N',
               run_sequence_id        = g_new_run_seq_id,
               last_updated_date      = SYSDATE,
               last_updated_by        = g_user_id,
               last_update_login      = g_login_id,
               program_application_id = g_prog_appl_id,
               program_id             = g_conc_program_id
         WHERE batch_id IS NULL;
      EXCEPTION
        WHEN OTHERS THEN
          print_log_message('Error : Exception occured while updating new batch id in staging ' ||
                            SUBSTR(SQLERRM, 1, 150));
      END;

      BEGIN
        xxetn_debug_pkg.add_debug(piv_debug_msg => 'Generating new batch id for Tax Book');

        UPDATE xxfa_tax_asset_stg
           SET batch_id               = g_batch_id,
               process_flag           = 'N',
               run_sequence_id        = g_new_run_seq_id,
               last_updated_date      = SYSDATE,
               last_updated_by        = g_user_id,
               last_update_login      = g_login_id,
               program_application_id = g_prog_appl_id,
               program_id             = g_conc_program_id
         WHERE batch_id IS NULL;
      EXCEPTION
        WHEN OTHERS THEN
          print_log_message('Error : Exception occured while updating new batch id in lines staging ' ||
                            SUBSTR(SQLERRM, 1, 150));
      END;

      COMMIT;
    ELSE
      IF g_book_type_code = 'CORPORATE BOOK' THEN
        BEGIN
          xxetn_debug_pkg.add_debug(piv_debug_msg => 'Reprocess updating run sequence id: Corporate Book');

          UPDATE xxfa_corp_asset_stg
             SET process_flag           = 'N',
                 run_sequence_id        = g_new_run_seq_id,
                 ERROR_TYPE             = NULL,
                 last_updated_date      = SYSDATE,
                 last_updated_by        = g_user_id,
                 last_update_login      = g_login_id,
                 program_application_id = g_prog_appl_id,
                 program_id             = g_conc_program_id
           WHERE batch_id = g_batch_id
             AND (g_process_records = 'ALL' AND
                 (process_flag NOT IN ('C', 'X', 'V', 'P')) OR
                 (g_process_records = 'ERROR' AND (process_flag = 'E')) OR
                 g_process_records = 'UNPROCESSED' AND
                 (process_flag = 'N'));
        EXCEPTION
          WHEN OTHERS THEN
            print_log_message('Error : Exception occured while updating run seq id for reprocess of Corporate book: ' ||
                              SUBSTR(SQLERRM, 1, 150));
        END;
      ELSIF g_book_type_code = 'TAX BOOK' THEN
        BEGIN
          xxetn_debug_pkg.add_debug(piv_debug_msg => 'Reprocess updating run sequence id: Tax Book');

          UPDATE xxfa_tax_asset_stg
             SET process_flag           = 'N',
                 run_sequence_id        = g_new_run_seq_id,
                 ERROR_TYPE             = NULL,
                 last_updated_date      = SYSDATE,
                 last_updated_by        = g_user_id,
                 last_update_login      = g_login_id,
                 program_application_id = g_prog_appl_id,
                 program_id             = g_conc_program_id
           WHERE batch_id = g_batch_id
             AND (g_process_records = 'ALL' AND
                 (process_flag NOT IN ('C', 'X', 'V', 'P')) OR
                 (g_process_records = 'ERROR' AND (process_flag = 'E')) OR
                 g_process_records = 'UNPROCESSED' AND
                 (process_flag = 'N'));
        EXCEPTION
          WHEN OTHERS THEN
            print_log_message('Error : Exception occured while updating run seq id for reprocess for tax Book: ' ||
                              SUBSTR(SQLERRM, 1, 150));
        END;
      END IF;
    END IF;

    COMMIT;
  EXCEPTION
    WHEN OTHERS THEN
      print_log_message('Error : Exception occured in assign batch id procedure: ' ||
                        SUBSTR(SQLERRM, 1, 150));
  END assign_batch_id;

  --
  -- ========================
  -- Procedure: ASSIGN_BATCH_ID_LOAD
  -- =====================================================================================
  --   This procedure is used for batch id assignment during the LOAD Process
  -- =====================================================================================
  PROCEDURE assign_batch_id_load IS
  BEGIN
    g_err_batch_flag := 'N';

    -- g_batch_id NULL is considered a fresh run
    IF g_batch_id IS NULL THEN
      g_batch_id := xxetn_batches_s.NEXTVAL;

      BEGIN
        xxetn_debug_pkg.add_debug(piv_debug_msg => 'Generating new batch id for Corporate Book');

        UPDATE xxfa_corp_asset_stg
           SET batch_id               = g_batch_id,
               process_flag           = 'N',
               run_sequence_id        = g_new_run_seq_id,
               last_updated_date      = SYSDATE,
               last_updated_by        = g_user_id,
               last_update_login      = g_login_id,
               program_application_id = g_prog_appl_id,
               program_id             = g_conc_program_id
         WHERE batch_id IS NULL;
      EXCEPTION
        WHEN OTHERS THEN
          print_log_message('Error : Exception occured while updating new batch id in staging ' ||
                            SUBSTR(SQLERRM, 1, 150));
      END;

      BEGIN
        xxetn_debug_pkg.add_debug(piv_debug_msg => 'Generating new batch id for Tax Book');

        UPDATE xxfa_tax_asset_stg
           SET batch_id               = g_batch_id,
               process_flag           = 'N',
               run_sequence_id        = g_new_run_seq_id,
               last_updated_date      = SYSDATE,
               last_updated_by        = g_user_id,
               last_update_login      = g_login_id,
               program_application_id = g_prog_appl_id,
               program_id             = g_conc_program_id
         WHERE batch_id IS NULL;
      EXCEPTION
        WHEN OTHERS THEN
          print_log_message('Error : Exception occured while updating new batch id in lines staging ' ||
                            SUBSTR(SQLERRM, 1, 150));
      END;

      COMMIT;
    END IF;

    COMMIT;
  EXCEPTION
    WHEN OTHERS THEN
      print_log_message('Error : Exception occured in assign batch id procedure: ' ||
                        SUBSTR(SQLERRM, 1, 150));

  END assign_batch_id_load;

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
  --    piv_run_mode        : Control the program execution for VALIDATE and CONVERSION
  --    p_entity          : Appropriate entity can be selected based on conversion
  --                        requirements
  --    pin_batch_id        : List all unique batches from staging table , this will
  --                        be NULL for first Conversion Run.
  --    piv_process_records : Conditionally available only when pin_batch_id is popul-
  --                        -ated. Otherwise this will be disabled and defaulted
  --                        to ALL
  --   piv_book_type_code   : Book Type
  --
  --  Output Parameters :
  --    pov_errbuf          : Standard output parameter for concurrent program
  --    pon_retcode         : Standard output parameter for concurrent program
  --
  --  Return     : Not applicable
  --
  PROCEDURE main(pov_errbuf        OUT NOCOPY VARCHAR2,
                 pon_retcode       OUT NOCOPY NUMBER,
                 pin_run_mode      IN VARCHAR2,
                 piv_hidden_param1 IN VARCHAR2,
                 -- Dummy/Hidden Parameter 1
                 pin_batch_id     IN NUMBER,
                 piv_hidden_param IN VARCHAR2,
                 -- Dummy/Hidden Parameter
                 piv_process_records IN VARCHAR2,
                 piv_book_type_code  IN VARCHAR2) IS
    --l_ret_status   VARCHAR2(100);
    --l_err_msg      VARCHAR2(2500);
    l_debug_err VARCHAR2(2000);
    --l_count NUMBER;
    pov_ret_stats VARCHAR2(100);
    pov_err_msg   VARCHAR2(1000);
    --ln_count_corp_valid     NUMBER;
    --ln_count_corp_comp      NUMBER;
    l_warn_excep EXCEPTION;
    l_load_ret_stats      VARCHAR2(1) := 'S';
    l_dist_load_ret_stats VARCHAR2(1) := 'S';
    l_load_err_msg        VARCHAR2(1000);
    l_dist_load_err_msg   VARCHAR2(1000);
    l_con_count           NUMBER := 0;
  BEGIN
    g_run_mode        := pin_run_mode;
    g_batch_id        := pin_batch_id;
    g_process_records := piv_process_records;
    g_book_type_code  := piv_book_type_code;
    -- Initialize debug procedure
    xxetn_debug_pkg.initialize_debug(pov_err_msg      => l_debug_err,
                                     piv_program_name => 'Fixed_Asset_Conv');
    xxetn_debug_pkg.add_debug(piv_debug_msg => 'Initialized Debug');
    xxetn_debug_pkg.add_debug('Program Parameters');
    xxetn_debug_pkg.add_debug('---------------------------------------------');
    xxetn_debug_pkg.add_debug('Run Mode            : ' || pin_run_mode);
    xxetn_debug_pkg.add_debug('Batch ID            : ' || pin_batch_id);
    xxetn_debug_pkg.add_debug('Reprocess records     : ' ||
                              piv_process_records);
    xxetn_debug_pkg.add_debug('Book Type : ' || piv_book_type_code);
    print_log_message('Program Parameters');
    print_log_message('---------------------------------------------');
    print_log_message('Run Mode            : ' || pin_run_mode);
    print_log_message('Batch ID            : ' || pin_batch_id);
    print_log_message('Reprocess records     : ' || piv_process_records);
    print_log_message('Book Type : ' || piv_book_type_code);
    print_log_message('---------------------------------------------');

    IF pin_run_mode = 'LOAD-DATA' THEN
      xxetn_debug_pkg.add_debug(piv_debug_msg => 'Calling Load data procedure');
      print_log_message('Calling procedure load_corp');
      print_log_message('');
      load_corp_book(pov_ret_stats => l_load_ret_stats,
                     pov_err_msg   => l_load_err_msg);
      print_log_message('Calling procedure load_tax');
      print_log_message('');
      load_tax_book(pov_ret_stats => l_dist_load_ret_stats,
                    pov_err_msg   => l_dist_load_err_msg);

      IF l_load_ret_stats <> 'S' THEN
        print_log_message('Error in procedure load_corp' || l_load_err_msg);
        print_log_message('');
        RAISE l_warn_excep;
      END IF;

      IF l_dist_load_ret_stats <> 'S' THEN
        print_log_message('Error in procedure load_tax' ||
                          l_dist_load_err_msg);
        print_log_message('');
        RAISE l_warn_excep;
      END IF;

      xxetn_debug_pkg.add_debug(piv_debug_msg => 'Load data procedure completed');
      ----------------assign_batch_id_load during load ----------------------------------------------------------------

      xxetn_debug_pkg.add_debug(piv_debug_msg => 'Calling assign_batch_id_load procedure in LOAD mode.');
      -- call procedure to assign batch id
      assign_batch_id_load;

      xxetn_common_error_pkg.g_batch_id := g_batch_id;
      -- batch id

      print_log_message('Batch ID for Load Mode: ' || g_batch_id);

      ------------------assign_batch_id_load during load ends ---------------------------------------------------------------
      xxetn_debug_pkg.add_debug(piv_debug_msg => 'Load data procedure completed');

    ELSIF pin_run_mode = 'PRE-VALIDATE' THEN
      xxetn_debug_pkg.add_debug(piv_debug_msg => 'Calling pre_validate mode');
      -- call procedure to pre-validate set up
      pre_validate();
      pon_retcode := g_ret_code;
    ELSIF pin_run_mode = 'VALIDATE' THEN
      IF piv_book_type_code IS NULL THEN
        print_log_message('Please Enter book type for Validation Process : ');
        RAISE l_warn_excep;
      END IF;

      g_new_run_seq_id := xxetn_run_sequences_s.NEXTVAL;
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

      -- call procedure to validate data
      IF piv_book_type_code = 'CORPORATE BOOK' THEN
                
       ------- added as per version v1.10 for perfromance tuning------------
    begin
    print_log_message('Running Gather Schema Stat on Corp Staging  Before Validation..');
      dbms_stats.gather_table_stats(ownname          => 'XXCONV',
                                    tabname          => 'XXFA_CORP_ASSET_STG',
                                    cascade          => true,
                                    estimate_percent => dbms_stats.auto_sample_size,
                                    degree           => dbms_stats.default_degree);
    exception when others then
      print_log_message('Error: ' || sqlerrm);
    end;
     ------- added as per version v1.10 for perfromance tuning------------
       print_log_message('Calling validate mode for Corporate Book.');
      validate_corporate();
        pon_retcode := g_ret_code;
        
      ELSIF piv_book_type_code = 'TAX BOOK' THEN
          ------- added as per version v1.10 for perfromance tuning------------
    begin
    print_log_message('Running Gather Schema Stat on Tax Staging Before Validation..');
      dbms_stats.gather_table_stats(ownname          => 'XXCONV',
                                    tabname          => 'XXFA_TAX_ASSET_STG',
                                    cascade          => true,
                                    estimate_percent => dbms_stats.auto_sample_size,
                                    degree           => dbms_stats.default_degree);
    
    exception when others then
      print_log_message('Error: ' || sqlerrm);
    
    end;
     ------- added as per version v1.10 for perfromance tuning------------
            
        print_log_message('Calling validate mode for Tax Book.');
        
        validate_tax();
        pon_retcode := g_ret_code;
      END IF;
    ELSIF pin_run_mode = 'CONVERSION' THEN
      IF pin_batch_id IS NOT NULL THEN
        IF piv_book_type_code = 'TAX BOOK' THEN
          SELECT COUNT(*)
            INTO l_con_count
            FROM xxfa_corp_asset_stg
           WHERE process_flag = 'C';

          IF l_con_count = 0 THEN
            print_log_message('Please convert Corporate book records first.');
            RAISE l_warn_excep;
          END IF;
        END IF;

        IF piv_book_type_code IS NULL THEN
          print_log_message('Please Enter book type for Conversion Process : ');
          RAISE l_warn_excep;
        END IF;

        g_new_run_seq_id := xxetn_run_sequences_s.NEXTVAL;
        xxetn_debug_pkg.add_debug(piv_debug_msg => 'Updating run sequence id in conversion mode.');
        print_log_message('Run Sequence ID for Import Mode : ' ||
                          g_new_run_seq_id);

        /*-- call procedure to assign batch id
        assign_batch_id;*/
        BEGIN
          xxetn_debug_pkg.add_debug(piv_debug_msg => 'Reprocess updating run sequence id: Corporate book conversion');

          UPDATE xxfa_corp_asset_stg
             SET run_sequence_id        = g_new_run_seq_id,
                 last_updated_date      = SYSDATE,
                 last_updated_by        = g_user_id,
                 last_update_login      = g_login_id,
                 program_application_id = g_prog_appl_id,
                 program_id             = g_conc_program_id
           WHERE batch_id = g_batch_id
             AND process_flag = 'V';
        EXCEPTION
          WHEN OTHERS THEN
            print_log_message('Error : Exception occured while updating run seq id for reprocess for corporate book: conversion mode ' ||
                              SUBSTR(SQLERRM, 1, 150));
        END;

        BEGIN
          xxetn_debug_pkg.add_debug(piv_debug_msg => 'Reprocess updating run sequence id: Tax Book conversion');

          UPDATE xxfa_tax_asset_stg
             SET run_sequence_id        = g_new_run_seq_id,
                 last_updated_date      = SYSDATE,
                 last_updated_by        = g_user_id,
                 last_update_login      = g_login_id,
                 program_application_id = g_prog_appl_id,
                 program_id             = g_conc_program_id
           WHERE batch_id = g_batch_id
             AND process_flag = 'V';
        EXCEPTION
          WHEN OTHERS THEN
            print_log_message('Error : Exception occured while updating run seq id for reprocess for tax book: conversion mode ' ||
                              SUBSTR(SQLERRM, 1, 150));
        END;

        COMMIT;
        xxetn_common_error_pkg.g_batch_id := g_batch_id;
        -- batch id
        xxetn_common_error_pkg.g_run_seq_id := g_new_run_seq_id;
        -- run sequence id
        xxetn_debug_pkg.add_debug(piv_debug_msg => 'Calling conversion mode.');

        -- call procedure to convert the data
        IF piv_book_type_code = 'CORPORATE BOOK' THEN
          
          ------- added as per version v1.10 for perfromance tuning------------
    begin
    print_log_message('Running Gather Schema Stat on Corp Staging Before Conversion..');
      dbms_stats.gather_table_stats(ownname          => 'XXCONV',
                                    tabname          => 'XXFA_CORP_ASSET_STG',
                                    cascade          => true,
                                    estimate_percent => dbms_stats.auto_sample_size,
                                    degree           => dbms_stats.default_degree);
    
    exception when others then
      print_log_message('Error: ' || sqlerrm);
    
    end;
     ------- added as per version v1.10 for perfromance tuning------------
          
          print_log_message('Calling Conversion mode for Corporate Book.');
          
          import_corporate();
          pon_retcode := g_ret_code;
        ELSIF piv_book_type_code = 'TAX BOOK' THEN
          
          ------- added as per version v1.10 for perfromance tuning------------
    begin
    print_log_message('Running Gather Schema Stat on Tax Staging Before Conversion..');
      dbms_stats.gather_table_stats(ownname          => 'XXCONV',
                                    tabname          => 'XXFA_Tax_ASSET_STG',
                                    cascade          => true,
                                    estimate_percent => dbms_stats.auto_sample_size,
                                    degree           => dbms_stats.default_degree);
    
    exception when others then
      print_log_message('Error: ' || sqlerrm);
    end;
     ------- added as per version v1.10 for perfromance tuning------------
        
          print_log_message('Calling Conversion mode for Tax Book.');
         
          import_tax();
          
          pon_retcode := g_ret_code;
        END IF;
      ELSE
        print_log_message('For conversion run mode batch id cannot be NULL');
        RAISE l_warn_excep;
      END IF;
    ELSIF pin_run_mode = 'RECONCILE' THEN
      BEGIN
        -- Get Total Count of Records in CORPORATE staging
        SELECT COUNT(1)
          INTO g_tot_header_count
          FROM xxfa_corp_asset_stg xds
         WHERE xds.batch_id = NVL(g_batch_id, xds.batch_id)
           AND xds.batch_id IS NOT NULL
           AND xds.run_sequence_id IS NOT NULL;

        -- Get Total Count of Records in TAX staging
        SELECT COUNT(1)
          INTO g_tot_lines_count
          FROM xxfa_tax_asset_stg xdsl
         WHERE xdsl.batch_id = NVL(g_batch_id, xdsl.batch_id)
           AND xdsl.batch_id IS NOT NULL
           AND xdsl.run_sequence_id IS NOT NULL;

        -- Get Total Count of Failed Records in Validation CORPORATE
        SELECT COUNT(1)
          INTO g_fail_header_count
          FROM xxfa_corp_asset_stg xds
         WHERE xds.batch_id = NVL(g_batch_id, xds.batch_id)
           AND xds.process_flag = 'E'
           AND xds.ERROR_TYPE = 'VAL_ERR'
           AND xds.batch_id IS NOT NULL
           AND xds.run_sequence_id IS NOT NULL;

        -- Get Total Count of Failed Records in Validation tax
        SELECT COUNT(1)
          INTO g_fail_lines_count
          FROM xxfa_tax_asset_stg xds
         WHERE xds.batch_id = NVL(g_batch_id, xds.batch_id)
           AND xds.process_flag = 'E'
           AND xds.ERROR_TYPE = 'VAL_ERR'
           AND xds.batch_id IS NOT NULL
           AND xds.run_sequence_id IS NOT NULL;

        -- Get Total Count of Failed Records in Conversion corporate
        SELECT COUNT(1)
          INTO g_fail_count_head
          FROM xxfa_corp_asset_stg xds
         WHERE xds.batch_id = NVL(g_batch_id, xds.batch_id)
           AND xds.process_flag = 'E'
           AND xds.ERROR_TYPE = 'API_ERR'
           AND xds.batch_id IS NOT NULL
           AND xds.run_sequence_id IS NOT NULL;

        -- Get Total Count of Failed Records in Conversion tax
        SELECT COUNT(1)
          INTO g_fail_count_line
          FROM xxfa_tax_asset_stg xds
         WHERE xds.batch_id = NVL(g_batch_id, xds.batch_id)
           AND xds.process_flag = 'E'
           AND xds.ERROR_TYPE = 'API_ERR'
           AND xds.batch_id IS NOT NULL
           AND xds.run_sequence_id IS NOT NULL;

        -- Get Total Count of Converted Records corporate
        SELECT COUNT(1)
          INTO g_suc_count_head
          FROM xxfa_corp_asset_stg xds
         WHERE xds.batch_id = NVL(g_batch_id, xds.batch_id)
           AND xds.process_flag = 'C'
           AND xds.batch_id IS NOT NULL
           AND xds.run_sequence_id IS NOT NULL;

        -- Get Total Count of Converted Records tax
        SELECT COUNT(1)
          INTO g_suc_count_line
          FROM xxfa_tax_asset_stg xdsl
         WHERE xdsl.batch_id = NVL(g_batch_id, xdsl.batch_id)
           AND xdsl.process_flag = 'C'
           AND xdsl.batch_id IS NOT NULL
           AND xdsl.run_sequence_id IS NOT NULL;
      EXCEPTION
        WHEN OTHERS THEN
          pon_retcode := 1;
          print_log_message('Error while deriving counts for Reconcile Mode');
      END;

      xxetn_debug_pkg.add_debug(piv_debug_msg => 'Inside Reconcile Mode');
      xxetn_debug_pkg.add_debug(piv_debug_msg => 'Calling print report');
      print_report(pin_total_count_header  => g_tot_header_count,
                   pin_total_count_lines   => g_tot_lines_count,
                   pin_suc_count_head      => g_suc_count_head,
                   pin_suc_count_line      => g_suc_count_line,
                   pin_failed_count_header => g_fail_header_count,
                   pin_failed_count_lines  => g_fail_lines_count,
                   pin_fail_count_imp_head => g_fail_count_head,
                   pin_fail_count_imp_line => g_fail_count_line);
    END IF;

    -- call once to dump pending error records which are less than profile value.
    xxetn_common_error_pkg.add_error(pov_return_status => pov_ret_stats,
                                     pov_error_msg     => pov_err_msg,
                                     pi_source_tab     => g_tab);
  EXCEPTION
    WHEN l_warn_excep THEN
      print_log_message('Main program procedure encounter user exception ' ||
                        SUBSTR(SQLERRM, 1, 150));
      pov_errbuf  := 'Error : Main program procedure encounter user exception. ' ||
                     SUBSTR(SQLERRM, 1, 150);
      pon_retcode := 1;
    WHEN OTHERS THEN
      pov_errbuf  := 'Error : Main Program Procedure: MAIN encounter error. Reason: ' ||
                     SUBSTR(SQLERRM, 1, 150);
      pon_retcode := 2;
      print_log_message('Error : Main Program Procedure: MAIN encounter error. Reason: ' ||
                        SUBSTR(SQLERRM, 1, 150));
      print_log_message('Error : Backtace during main : ' ||
                        DBMS_UTILITY.format_error_backtrace);
  END main;
END xxfa_asset_pkg;
/

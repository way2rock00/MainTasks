create or replace PACKAGE BODY slc_sminf_1a_host_to_site_pkg
AS
---------------------------------------------------------------------------
-- Company              : 7-Eleven
-- Module               : 7-Eleven 1A host to Site Hub Interface
--                        (APPS)
-- Oracle Program       : N/A
-- Oracle Version #     : N/A
-- Modification         : New program
-- Location             : $SLCUST1_TOP/sql
-- File Name            : slc_sminf_1ahost_to_site_bpkg.sql
-- Purpose              : Apply changeover from 1A host to Site Hub
-- Installation         : SQL*Plus Scripts:
--                        1) slc_sminf_1ahost_to_site_spkg.sql
--                        2) slc_sminf_1ahost_to_site_bpkg.sql
-- Inputs               : Input 1 - APPS Password
--                        Input 2 - Connect String
-- Processing Overview  :
--
---------------------------------------------------------------------------
-- Version | Date       | Name          | Description
---------------------------------------------------------------------------
-- 1.0     | 2017.04.10 | Nagesh        | Created
-- 1.1     | 2017.09.1  | Akshay        | Modified for Defect#41615
-- 1.2     | 2017.09.17 | Akshay        | Numeric Error Fix
-- 1.3     | 2017.09.18 | Akshay        | Audit accrual flag fix and GGPS fix.
---------------------------------------------------------------------------
-- Source control:
---------------------------------------------------------------------------
-- ID          : $Id$
-- Revision    : $Revision$
-- Date        : $Date$
-- Author      : $Author$
-- Description : $Log$
---------------------------------------------------------------------------

   -- The private constants that are used in the following procedures
   gc_pkg_name                   CONSTANT VARCHAR2 (30)
                                           := 'SLC_SMINF_1A_HOST_TO_SITE_PKG';
   gc_error                      CONSTANT VARCHAR2 (5)             := 'ERROR';
   gc_success                    CONSTANT VARCHAR2 (10)            := 'SUCCESS';   
   gc_warning                    CONSTANT VARCHAR2 (7)           := 'WARNING';
   gc_unknown_error              CONSTANT VARCHAR2 (15)   := 'UNKNOWN ERROR:';
   gc_sync_mode                  CONSTANT VARCHAR2 (15)             := 'SYNC';
   gc_site_group                 CONSTANT ego_attr_groups_v.attr_group_type%TYPE
                                                      := 'RRS_SITEMGMT_GROUP';
   --Attribute Group Names
   gc_store_oper_grp_pending     CONSTANT ego_attr_groups_v.attr_group_name%TYPE
                                              := 'SLC_SM_STORE_OPERATOR_PEND';
   --
   gc_agreement_grp_active       CONSTANT ego_attr_groups_v.attr_group_name%TYPE
                                                        := 'SLC_SM_AGREEMENT';
   gc_agreement_grp_prior        CONSTANT ego_attr_groups_v.attr_group_name%TYPE
                                                  := 'SLC_SM_AGREEMENT_PRIOR';
   gc_agreement_grp_pending      CONSTANT ego_attr_groups_v.attr_group_name%TYPE
                                                   := 'SLC_SM_AGREEMENT_PEND';
   --
   gc_amendment_grp_active       CONSTANT ego_attr_groups_v.attr_group_name%TYPE
                                                        := 'SLC_SM_AMENDMENT';
   gc_amendment_grp_prior        CONSTANT ego_attr_groups_v.attr_group_name%TYPE
                                                  := 'SLC_SM_AMENDMENT_PRIOR';
   gc_amendment_grp_pending      CONSTANT ego_attr_groups_v.attr_group_name%TYPE
                                                   := 'SLC_SM_AMENDMENT_PEND';
   --
   gc_franchisee_grp_active      CONSTANT ego_attr_groups_v.attr_group_name%TYPE
                                               := 'SLC_SM_FRANCHISEE_DETAILS';
   gc_franchisee_grp_prior       CONSTANT ego_attr_groups_v.attr_group_name%TYPE
                                          := 'SLC_SM_FRANCHISEE_DETAILS_PRIO';
   gc_franchisee_grp_pending     CONSTANT ego_attr_groups_v.attr_group_name%TYPE
                                          := 'SLC_SM_FRANCHISEE_DETAILS_PEND';
   --
   gc_breach_grp_active          CONSTANT ego_attr_groups_v.attr_group_name%TYPE
                                                           := 'SLC_SM_BREACH';
   gc_breach_grp_prior           CONSTANT ego_attr_groups_v.attr_group_name%TYPE
                                                     := 'SLC_SM_BREACH_PRIOR';
   --
   gc_lon_grp_active             CONSTANT ego_attr_groups_v.attr_group_name%TYPE
                                                              := 'SLC_SM_LON';
   gc_lon_grp_prior              CONSTANT ego_attr_groups_v.attr_group_name%TYPE
                                                        := 'SLC_SM_LON_PRIOR';
   --
   gc_startstop_grp_active       CONSTANT ego_attr_groups_v.attr_group_name%TYPE
                                             := 'SLC_SM_START_STOP_FINANCING';
   gc_startstop_grp_prior        CONSTANT ego_attr_groups_v.attr_group_name%TYPE
                                          := 'SLC_SM_START_STOP_FINANC_PRIOR';
   --
   gc_rvpr_grp_active            CONSTANT ego_attr_groups_v.attr_group_name%TYPE
                                                             := 'SLC_SM_RVPR';
   gc_rvpr_grp_prior             CONSTANT ego_attr_groups_v.attr_group_name%TYPE
                                                       := 'SLC_SM_RVPR_PRIOR';
   --
   gc_templic_grp_active         CONSTANT ego_attr_groups_v.attr_group_name%TYPE
                                                     := 'SLC_SM_TEMP_LICENSE';
   gc_templic_grp_pending        CONSTANT ego_attr_groups_v.attr_group_name%TYPE
                                                := 'SLC_SM_TEMP_LICENSE_PEND';
   --
   gc_transfers_grp              CONSTANT ego_attr_groups_v.attr_group_name%TYPE
                                                        := 'SLC_SM_TRANSFERS';
   gc_transfers_grp_prior        CONSTANT ego_attr_groups_v.attr_group_name%TYPE
                                                  := 'SLC_SM_TRANSFERS_PRIOR';
   --
   gc_setl_agrmnt_grp_active     CONSTANT ego_attr_groups_v.attr_group_name%TYPE
                                          := 'SLC_SM_SETTLEMENT_AGREEMENT_BR';
   gc_setl_agrmnt_grp_prior      CONSTANT ego_attr_groups_v.attr_group_name%TYPE
                                             := 'SLC_SM_SETTLEMENT_AGREEMENT';
   gc_ebba_grp_active            CONSTANT ego_attr_groups_v.attr_group_name%TYPE
                                                             := 'SLC_SM_EBBA';
   gc_ebba_grp_prior             CONSTANT ego_attr_groups_v.attr_group_name%TYPE
                                                       := 'SLC_SM_EBBA_PRIOR';
   gc_franch_dtls                CONSTANT ego_attr_groups_v.attr_group_name%TYPE
                                                       := 'SLC_ISP_FRANCHISEE_DETAILS';
   gc_franch_co                  CONSTANT ego_attr_groups_v.attr_group_name%TYPE
                                                       := 'SLC_ISP_FRANCHISEE_CO';
   gc_store_oprtr_grp_prior      CONSTANT ego_attr_groups_v.attr_group_name%TYPE
                                                       := 'SLC_SM_STORE_OPERATOR_PRIOR';
   gc_store_oprtr_grp_active     CONSTANT ego_attr_groups_v.attr_group_name%TYPE
                                                       := 'SLC_SM_STORE_OPERATOR';
   gc_contr_attr                 CONSTANT ego_attr_groups_v.attr_group_name%TYPE
                                                       := 'SLC_SM_CONTRACTUAL_ATTRIBUTES';
   gc_orig_contr_attr_at         CONSTANT ego_attr_groups_v.attr_group_name%TYPE
                                                       := 'SLC_SM_ORIGINAL_CONTRACTUAL_AT';
   gc_3mon_grp                  CONSTANT ego_attr_groups_v.attr_group_name%TYPE
                                                       := 'SLC_SM_3_MONTHS_AVG';
   gc_override_grp              CONSTANT ego_attr_groups_v.attr_group_name%TYPE
                                                       := 'SLC_SM_OVERRIDE';
   gc_franch_to_franch          CONSTANT VARCHAR2(30)  := 'Franchisee to Franchisee';
   gc_legal_entity              CONSTANT ego_attr_groups_v.attr_group_name%TYPE
                                                       := 'Legal Entity';	
   gc_prev_store_ltr_grp        CONSTANT ego_attr_groups_v.attr_group_name%TYPE
                                                       := 'SLC_SM_PREV_STORE_LETTER_CODES';	
   gc_mgmt_agrmnt_grp           CONSTANT ego_attr_groups_v.attr_group_name%TYPE
                                                       := 'SLC_SM_MANAGEMENT_AGREEMENT';	
   gc_mgmt_agrmnt_prior_grp       CONSTANT ego_attr_groups_v.attr_group_name%TYPE
                                                       := 'SLC_SM_MANAGEMENT_AGREE_PRIOR';														   
   gc_draw_mgmt_grp             CONSTANT ego_attr_groups_v.attr_group_name%TYPE
                                                       := 'SLC_SM_DRAW_MANAGEMENT';	
   
   --Attribute Names
   gc_fee_value_attr_name        CONSTANT ego_attrs_v.attr_name%TYPE
                                                           := 'FEE_VALUE_P_N';
   gc_fee_value_perc_attr_name   CONSTANT ego_attrs_v.attr_name%TYPE
                                                := 'FEE_VALUE_PERC_OR_NUMBER';
   gc_no_of_store_attr_name      CONSTANT ego_attrs_v.attr_name%TYPE
                                                  := 'NUMBER_OF_STORES_OWNED';
   gc_orig_date_attr_name        CONSTANT ego_attrs_v.attr_name%TYPE
                                                           := 'ORIGINAL_DATE';
   gc_eff_start_attr_name        CONSTANT ego_attrs_v.attr_name%TYPE
                                                           := 'EFFECTIVE_START_DATE';
   gc_eff_end_attr_name          CONSTANT ego_attrs_v.attr_name%TYPE
                                                           := 'EFFECTIVE_END_DATE';														   
   gc_signed_date_attr_name      CONSTANT ego_attrs_v.attr_name%TYPE
                                                           := 'SIGNED_DATE';
   gc_creat_date_attr_name       CONSTANT ego_attrs_v.attr_name%TYPE
                                                           := 'CREATION_DATE';
   gc_edition_char_attr_name     CONSTANT ego_attrs_v.attr_name%TYPE
                                                           := 'EDITION_CHAR';
   gc_franch_num_attr_name      CONSTANT ego_attrs_v.attr_name%TYPE
                                                           := 'FRANCHISEE_NUM';
   gc_franch_name_attr_name      CONSTANT ego_attrs_v.attr_name%TYPE
                                                           := 'FRANCHISEE_NAME';
   gc_franch_incorp_attr_name    CONSTANT ego_attrs_v.attr_name%TYPE
                                                           := 'INCORPORATION';														   
   gc_franch_ownershp_attr_name  CONSTANT ego_attrs_v.attr_name%TYPE
                                                           := 'OWNERSHIP_STATUS';															   
   gc_7elevn_franch_num         CONSTANT ego_attrs_v.attr_name%TYPE
                                                           := '10001001';
   gc_form_num_attr_name        CONSTANT ego_attrs_v.attr_name%TYPE
                                                           := 'FORM_NUMBER';
   gc_grnd_split_attr_name      CONSTANT ego_attrs_v.attr_name%TYPE
                                                           := 'GRANDFATHERED_SPLIT_50_50';
   gc_grnd_exp_dt_attr_name     CONSTANT ego_attrs_v.attr_name%TYPE
                                                           := 'GRANDFATHERED_EXP_DT';	
   gc_fz_store_letr_attr_name   CONSTANT ego_attrs_v.attr_name%TYPE
                                                           := 'FZ_STORE_LETTER_CODE';
   gc_status_attr_name          CONSTANT ego_attrs_v.attr_name%TYPE
                                                           := 'STATUS';
   gc_agrmnt_type_attr_name     CONSTANT ego_attrs_v.attr_name%TYPE
                                                           := 'AGREEMENT_TYPE';														   
   gc_mult_indctr_attr_name     CONSTANT ego_attrs_v.attr_name%TYPE
                                                           := 'MULTIPLE_INDICATOR';
   gc_edition_attr_name         CONSTANT ego_attrs_v.attr_name%TYPE
                                                           := 'EDITION';														   
   gc_corp_store_attr_name      CONSTANT ego_attrs_v.attr_name%TYPE
                                                           := 'CORP_STORE_LETTER_CODE';
   gc_doc_type_attr_name        CONSTANT ego_attrs_v.attr_name%TYPE
                                                           := 'DOCUMENT_TYPE';	
   gc_draw_reduced_attr_name    CONSTANT ego_attrs_v.attr_name%TYPE
                                                           := 'WEEKLY_DRAW_REDUCED';														   
   gc_draw_prohibited_attr_name CONSTANT ego_attrs_v.attr_name%TYPE
                                                           := 'WEEKLY_DRAW_PROHIBITED';	
   --Value set name
   gc_agrmnt_vs_name             CONSTANT fnd_flex_value_sets.flex_value_set_name%TYPE
                                                           := 'SLCSM_AGREEMENT_TYPE';   
   gc_amend_fein_lkp             CONSTANT fnd_lookup_values.lookup_type%TYPE
                                                       := 'SLCSM_AMENDMENT_FOR_FEIN_CHNG';
   gc_form_number_lkp            CONSTANT fnd_lookup_values.lookup_type%TYPE
                                                       := 'SLCOKC_FORM_NUMBER';
   gc_trad_agrmnt_lkp            CONSTANT fnd_lookup_values.attribute1%TYPE
                                                       := 'Traditional Agreement';
   --
   gc_7elevn_supplier_name       CONSTANT ap_suppliers.vendor_name_alt%TYPE := '7-Eleven Inc.';
   gc_7elevn_type_lkp_code       CONSTANT ap_suppliers.vendor_type_lookup_code%TYPE := 'FRANCHISEE';   
   gc_primary                    CONSTANT VARCHAR2(10) := 'Primary';
   gc_edition_01                 CONSTANT   VARCHAR2 (150) := '01/2004';
   gc_edition_02                 CONSTANT   VARCHAR2 (150) := '02/2004';
   gc_trad_agrmnt50              CONSTANT   VARCHAR2 (150) := 'Traditional Agreement - 50/50';
   gc_trad_agrmntggps            CONSTANT   VARCHAR2 (150) := 'Traditional Agreement - GGPS';
   gc_status_pending             CONSTANT   VARCHAR2 (150) := 'Pending';
   gc_status_inactive            CONSTANT   VARCHAR2 (150) := 'Inactive';
   gc_bal_segment_conc_short     CONSTANT   VARCHAR2 (150) := 'GLEXTUPDATEBALSEG';
   gc_conc_appl_name             CONSTANT   VARCHAR2 (150) := 'SLCUST1';
   g_pending_chngovr_type                 VARCHAR2 (150)         DEFAULT NULL;
   g_agreement_type                       VARCHAR2 (150)         DEFAULT NULL;
   g_agreement_type_orig                  VARCHAR2 (150)         DEFAULT NULL;
   g_agreement_edition                    VARCHAR2 (150)         DEFAULT NULL;
   g_agr_agreement_type                   VARCHAR2 (150)         DEFAULT NULL;
   g_agr_latest_start_date                DATE                   DEFAULT NULL;
   g_agr_agr_edition                      VARCHAR2 (150)         DEFAULT NULL;
   g_multiple_ind                         NUMBER                    DEFAULT 0;
   g_agr_agreement_type_pend              VARCHAR2 (150)         DEFAULT NULL;
   g_agr_agr_edition_pend                 VARCHAR2 (150)         DEFAULT NULL;
   g_supplier_orig_date                   DATE                   DEFAULT NULL;
   g_site_purpose                         VARCHAR2 (30)
                                             DEFAULT 'CONTRACTUAL_ATTRIBUTES';
   g_site_franch_purpose                  VARCHAR2 (30)
                                                      DEFAULT 'ST:FRANCHISEE';
   g_batch_id                             NUMBER;
   g_debug_mode                           BOOLEAN                     := TRUE;
   g_userid                               NUMBER        := fnd_global.user_id;
   g_business_group_id                    NUMBER
                   := TO_NUMBER (fnd_profile.VALUE ('PER_BUSINESS_GROUP_ID'));
   g_organization_id                      NUMBER
                                  := TO_NUMBER (fnd_profile.VALUE ('ORG_ID'));
   g_login_id                             NUMBER       := fnd_global.login_id;
   g_conc_request_id                      NUMBER
                                                := fnd_global.conc_request_id;
   g_conc_request_date                    DATE                     := SYSDATE;
   g_prog_appl_id                         NUMBER   := fnd_global.prog_appl_id;
   g_conc_program_id                      NUMBER
                                                := fnd_global.conc_program_id;
   g_resp_id                              NUMBER        := fnd_global.resp_id;
   g_resp_appl_id                         NUMBER   := fnd_global.resp_appl_id;
   g_store_letter                         VARCHAR2 (150)         DEFAULT NULL;
   g_store_letter_code                    VARCHAR2 (150)         DEFAULT NULL;
   g_store_number                         VARCHAR2 (150)         DEFAULT NULL;
   g_actual_changeover                    DATE                   DEFAULT NULL;
   g_grand_50_50                          VARCHAR2 (150)         DEFAULT NULL;
   g_grand_exp_date                       DATE                   DEFAULT NULL;
   --Variables for Common Error Handling.
   g_batch_key                            VARCHAR2 (50)
                  DEFAULT 'FRC-I-015' || '-' || TO_CHAR (SYSDATE, 'DDMMYYYY');
   g_business_process_name                VARCHAR2 (150)
                                           := 'SLC_SMINF_1A_HOST_TO_SITE_PKG';
   g_cmn_err_rec                          apps.slc_util_jobs_pkg.g_error_tbl_type;
   g_cmn_err_count                        NUMBER                    DEFAULT 0;
-------------------------------------------------------------------------------
--   Procedure      : WRITE_LOG_P
--   Purpose        : Write a Log Entry to the Concurrent Log File
--   Parameters     : p_text                  IN      VARCHAR2
--   Modifications  :
-------------------------------------------------------------------------------
   PROCEDURE write_log_p (p_text IN VARCHAR2)
   IS
   BEGIN
      IF fnd_global.conc_program_id != -1
      THEN
         fnd_file.put_line (fnd_file.LOG, p_text);
      ELSE
         DBMS_OUTPUT.put_line (p_text);
      END IF;
   END write_log_p;

   -------------------------------------------------------------------------------
--   Procedure      : WRITE_OUTPUT_P
--   Purpose:         Write an Output File entry
--   Parameters     : p_text                  IN      VARCHAR2
--   Modifications:
-------------------------------------------------------------------------------
   PROCEDURE write_output_p (p_text IN VARCHAR2)
   IS
   BEGIN
      IF fnd_global.conc_program_id != -1
      THEN
         fnd_file.put_line (fnd_file.output, p_text);
      ELSE
         DBMS_OUTPUT.put_line (p_text);
      END IF;
   END write_output_p;
   
   
   --Changes for v1.1
   --Added function that will return Y if for the passed extension id Temp License in Active page is active.
   -------------------------------------------------------------------------------
--   Function      : slc_sminf_get_active_lic
--   Purpose:         function that will return Y if for the passed extension id Temp License in Active page is active.
--   Parameters     : p_extension_id                  IN      NUMBER
--   Modifications:
-------------------------------------------------------------------------------   
   FUNCTION slc_sminf_get_active_lic (p_extension_id NUMBER)
   RETURN VARCHAR2
   AS
	ln_extension_id rrs_sites_ext_b.extension_id%TYPE;
	--By default data should get deleted.
	lv_ret_flag		VARCHAR2(1) := 'Y';
   BEGIN
   write_log_p('In slc_sminf_get_active_lic: p_extension_id:'||p_extension_id);
	 BEGIN
		--If Select statement is retuning data i.e if record is active then we need to retain it.
	   SELECT extension_id
	   INTO ln_extension_id
		 FROM rrs_sites_ext_b
		WHERE extension_id = p_extension_id
		  AND TRUNC(SYSDATE) BETWEEN TRUNC(NVL(D_EXT_ATTR1,SYSDATE)) AND TRUNC(NVL(D_EXT_ATTR2,SYSDATE));
		lv_ret_flag := 'N';
	 EXCEPTION
	 --If there is no data found then it means record is not active and we need to delete it.
	 WHEN NO_DATA_FOUND THEN
		lv_ret_flag := 'Y';
		--If there is any other error then we need to retain the record.
	 WHEN OTHERS THEN
		lv_ret_flag := 'N';
	 END;
	 write_log_p('In slc_sminf_get_active_lic: lv_ret_flag:'||lv_ret_flag);
	 RETURN lv_ret_flag;
   END;
   
   -------------------------------------------------------------------------------
--   Procedure      : POPULATE_ERR_OBJECT
--   Purpose        : This procedure will keep on inserting error records in the
--                    error table SLC_UTIL_JOB_SUMMARY and SLC_UTIL_JOB_ERRORS
--   Parameters     : p_in_batch_key               IN   VARCHAR2
--                    p_in_business_entity         IN   VARCHAR2
--                    p_in_process_id1             IN   VARCHAR2
--                    p_in_process_id2             IN   VARCHAR2
--                    p_in_process_id3             IN   VARCHAR2
--                    p_in_process_id4             IN   VARCHAR2
--                    p_in_process_id5             IN   VARCHAR2
--                    p_in_business_process_step   IN   VARCHAR2
--                    p_in_error_code              IN   VARCHAR2
--                    p_in_error_txt               IN   VARCHAR2
--                    p_in_request_id              IN   NUMBER
--                    p_in_attribute1              IN   VARCHAR2
--                    p_in_attribute2              IN   VARCHAR2
--                    p_in_attribute3              IN   VARCHAR2
--                    p_in_attribute4              IN   VARCHAR2
--                    p_in_attribute5              IN   VARCHAR2
--   Modifications:
-------------------------------------------------------------------------------
   PROCEDURE populate_err_object (
      p_in_batch_key               IN   VARCHAR2,
      p_in_business_entity         IN   VARCHAR2,
      p_in_process_id1             IN   VARCHAR2 DEFAULT NULL,
      p_in_process_id2             IN   VARCHAR2 DEFAULT NULL,
      p_in_process_id3             IN   VARCHAR2 DEFAULT NULL,
      p_in_process_id4             IN   VARCHAR2 DEFAULT NULL,
      p_in_process_id5             IN   VARCHAR2 DEFAULT NULL,
      p_in_business_process_step   IN   VARCHAR2 DEFAULT NULL,
      p_in_error_code              IN   VARCHAR2 DEFAULT NULL,
      p_in_error_txt               IN   VARCHAR2,
      p_in_request_id              IN   NUMBER,
      p_in_attribute1              IN   VARCHAR2 DEFAULT NULL,
      p_in_attribute2              IN   VARCHAR2 DEFAULT NULL,
      p_in_attribute3              IN   VARCHAR2 DEFAULT NULL,
      p_in_attribute4              IN   VARCHAR2 DEFAULT NULL,
      p_in_attribute5              IN   VARCHAR2 DEFAULT NULL
   )
   IS
   BEGIN
      g_cmn_err_count := g_cmn_err_count + 1;
      g_cmn_err_rec (g_cmn_err_count).seq := slc_util_batch_key_s.NEXTVAL;
      g_cmn_err_rec (g_cmn_err_count).business_process_entity :=
                                                         p_in_business_entity;
      g_cmn_err_rec (g_cmn_err_count).business_process_id1 :=
                                                             p_in_process_id1;
      g_cmn_err_rec (g_cmn_err_count).business_process_id2 :=
                                                             p_in_process_id2;
      g_cmn_err_rec (g_cmn_err_count).business_process_id3 :=
                                                             p_in_process_id3;
      g_cmn_err_rec (g_cmn_err_count).business_process_id4 :=
                                                             p_in_process_id4;
      g_cmn_err_rec (g_cmn_err_count).business_process_id5 :=
                                                             p_in_process_id5;
      g_cmn_err_rec (g_cmn_err_count).business_process_step :=
                                                   p_in_business_process_step;
      g_cmn_err_rec (g_cmn_err_count).ERROR_CODE := p_in_error_code;
      g_cmn_err_rec (g_cmn_err_count).ERROR_TEXT := p_in_error_txt;
      g_cmn_err_rec (g_cmn_err_count).request_id := p_in_request_id;
      g_cmn_err_rec (g_cmn_err_count).attribute1 := p_in_attribute1;
      g_cmn_err_rec (g_cmn_err_count).attribute2 := p_in_attribute2;
      g_cmn_err_rec (g_cmn_err_count).attribute3 := p_in_attribute3;
      g_cmn_err_rec (g_cmn_err_count).attribute4 := p_in_attribute4;
      g_cmn_err_rec (g_cmn_err_count).attribute5 := p_in_attribute5;
   END;

    ---------------------------------------------------------------------------------------------------
    -- Procedure Name : slc_populate_row_attr_grp_p
    -- Purpose        : This procedure calls the API to create records for the passed attribute group
    -- Parameters     : p_attribute_group_name -- Attribute group name for which the record must eb created
    --                : p_count -- Index for the row table
    --                : p_row_identifier --Extension id of the row
    --                : p_transaction_type --Mode of the API (sync/delete)
    --                : x_attributes_row_table - Output row table
    --                : x_status    -- Status returned by the procedure
    --                : x_error_message   -- Error message returned by the procedure if any
    -- -------------------------------------------------------------------------------------------------
    
PROCEDURE slc_populate_row_attr_grp_p (
   p_attribute_group_name   IN       VARCHAR2,
   p_count                  IN       NUMBER,
   p_row_identifier         IN       NUMBER,
   p_transaction_type       IN       VARCHAR2,
   x_attributes_row_table   OUT      ego_user_attr_row_table,
   x_status                 OUT      VARCHAR2,
   x_error_message          OUT      VARCHAR2
)
IS
   l_status                 VARCHAR2 (1)                          DEFAULT 'S';
   l_message                VARCHAR2 (4000);
   l_attr_grp_id            ego_obj_attr_grp_assocs_v.attr_group_id%TYPE;
   l_group_app_id           ego_obj_attr_grp_assocs_v.application_id%TYPE;
   l_attr_group_type        ego_obj_attr_grp_assocs_v.attr_group_type%TYPE;
   l_attr_group_name        ego_obj_attr_grp_assocs_v.attr_group_name%TYPE;
   l_data_level             ego_obj_attr_grp_assocs_v.data_level_int_name%TYPE;
   l_attributes_row_table   ego_user_attr_row_table
                                                := ego_user_attr_row_table
                                                                          ();
BEGIN
   BEGIN
     write_log_p ('slc_populate_row_attr_grp_p: Fetch attribute group details for attribute group :'||p_attribute_group_name);
      
      SELECT attr_group_id, application_id attr_group_app_id,
             attr_group_type, attr_group_name, data_level_int_name data_level
        INTO l_attr_grp_id, l_group_app_id,
             l_attr_group_type, l_attr_group_name, l_data_level
        FROM ego_obj_attr_grp_assocs_v
       WHERE attr_group_name = p_attribute_group_name
         AND classification_code = g_site_purpose;
   EXCEPTION
      WHEN OTHERS
      THEN
         l_message :=
            'slc_populate_row_attr_grp_p: Error while fetching attribute group details. Error ' || SQLERRM;
         l_status := 'E';
   END;

   IF (l_status = 'S')
   THEN
      --Populate row type for attribute group details
      l_attributes_row_table.EXTEND;
      l_attributes_row_table (p_count) :=
         ego_user_attr_row_obj
                       (p_row_identifier                     -- ROW_IDENTIFIER
                                        ,
                        l_attr_grp_id  -- ATTR_GROUP_ID from EGO_ATTR_GROUPS_V
                                     ,
                        l_group_app_id                    -- ATTR_GROUP_APP_ID
                                      ,
                        l_attr_group_type                   -- ATTR_GROUP_TYPE
                                         ,
                        l_attr_group_name                   -- ATTR_GROUP_NAME
                                         ,
                        l_data_level                            -- NDATA_LEVEL
                                    ,
                        NULL                                   -- DATA_LEVEL_1
                            ,
                        NULL                                   -- DATA_LEVEL_2
                            ,
                        NULL                                   -- DATA_LEVEL_3
                            ,
                        NULL                                   -- DATA_LEVEL_4
                            ,
                        NULL                                   -- DATA_LEVEL_5
                            ,
                        p_transaction_type          -- TRANSACTION_TYPE 'SYNC'
                       );
   END IF;

   x_status := l_status;
   x_error_message := l_message;
   x_attributes_row_table := l_attributes_row_table;
   
   write_log_p('slc_populate_row_attr_grp_p: RETURN status is :' || l_status);
EXCEPTION
   WHEN OTHERS
   THEN
      x_status := 'E';
      x_error_message :=
            l_message
         || 'slc_populate_row_attr_grp_p: Exception while fetching attribute details for ROW. '
         || SQLERRM;
END slc_populate_row_attr_grp_p;
 
 -------------------------------------------------------------------------------
--   Procedure      : slc_sminf_single_uda_attrs_p
--   Purpose        : This procedure must be invoked from procedure
--                    slc_sminf_process_uda_attrs_p to process all single-row
--                    attributes for a specific attribute group and attribute
--   Parameters     : p_site_id           IN          NUMBER
--                    p_attr_group_name   IN          VARCHAR2
--                    p_attr_name         IN          VARCHAR2
--                    p_attr_char_value   IN          VARCHAR2
--                    p_attr_num_value    IN          VARCHAR2
--                    p_attr_date_value   IN          VARCHAR2
--                    p_batch_id          IN          VARCHAR2
--                    p_site_purpose      IN          VARCHAR2
--                    x_return_status     OUT NOCOPY  VARCHAR2
--                    x_msg_count         OUT NOCOPY  NUMBER
--                    x_msg_data          OUT NOCOPY  VARCHAR2
--   Modifications  :
-------------------------------------------------------------------------------

PROCEDURE slc_sminf_single_uda_attrs_p (
   p_site_id           IN              NUMBER,
   p_attr_group_name   IN              VARCHAR2,
   p_attr_name         IN              VARCHAR2,
   p_attr_char_value   IN              VARCHAR2,
   p_attr_num_value    IN              NUMBER,
   p_attr_date_value   IN              DATE,
   p_batch_id          IN              NUMBER,
   p_site_purpose      IN              VARCHAR2,
   x_return_status     OUT NOCOPY      VARCHAR2,
   x_msg_count         OUT NOCOPY      NUMBER,
   x_msg_data          OUT NOCOPY      VARCHAR2
)
IS
   l_api_version                   NUMBER                        := 1;
   l_object_name                   VARCHAR2 (20)                := 'RRS_SITE';
   l_attributes_row_table          ego_user_attr_row_table
                                                := ego_user_attr_row_table
                                                                          ();
   l_attributes_data_table         ego_user_attr_data_table
                                               := ego_user_attr_data_table
                                                                          ();
   l_pk_column_name_value_pairs    ego_col_name_value_pair_array
                                          := ego_col_name_value_pair_array
                                                                          ();
   l_class_code_name_value_pairs   ego_col_name_value_pair_array
                                          := ego_col_name_value_pair_array
                                                                          ();
   l_user_privileges_on_object     ego_varchar_tbl_type          := NULL;
   l_entity_id                     NUMBER                        := NULL;
   l_entity_index                  NUMBER                        := NULL;
   l_entity_code                   VARCHAR2 (1)                  := NULL;
   l_debug_level                   NUMBER                        := 3;
   l_attr_grp_id                   NUMBER                        := 0;
   l_group_app_id                  NUMBER                        := 0;
   l_attr_group_type               VARCHAR2 (100);
   l_attr_group_name               VARCHAR2 (100);
   l_data_level                    VARCHAR2 (100);
   l_error_msg                     VARCHAR2 (4000)               := NULL;
   l_init_error_handler            VARCHAR2 (1)             := fnd_api.g_true;
   l_write_to_concurrent_log       VARCHAR2 (1)             := fnd_api.g_true;
   l_init_fnd_msg_list             VARCHAR2 (1)             := fnd_api.g_true;
   l_log_errors                    VARCHAR2 (1)             := fnd_api.g_true;
   l_add_errors_to_fnd_stack       VARCHAR2 (1)            := fnd_api.g_false;
   l_commit                        VARCHAR2 (1)            := fnd_api.g_false;
   x_failed_row_id_list            VARCHAR2 (255);
   x_errorcode                     NUMBER;
   x_message_list                  error_handler.error_tbl_type;
   l_site_id                       NUMBER;
   l_purpose                       VARCHAR2 (200);
   l_attr_name                     VARCHAR2 (200);
   l_attr_value                    NUMBER;
   l_count                         PLS_INTEGER                   := 0;
   l_row_count                     PLS_INTEGER                   := 0;
   l_data_level1                   VARCHAR2 (10)                 DEFAULT NULL;
   l_party_id                      NUMBER;
BEGIN
   write_log_p ('slc_sminf_single_uda_attrs_p: Start of procedure');
   IF p_site_purpose = g_site_franch_purpose --'ST:FRANCHISEE'
   THEN
      l_object_name := 'HZ_PARTIES';
      l_data_level1 := 'N';
      l_pk_column_name_value_pairs.EXTEND;
      l_pk_column_name_value_pairs (1) :=
                          ego_col_name_value_pair_obj ('PARTY_ID', p_site_id);
                                                   --Party ID sent in Site ID
      l_class_code_name_value_pairs.EXTEND (1);
      l_class_code_name_value_pairs (1) :=
          ego_col_name_value_pair_obj ('CLASSIFICATION_CODE', p_site_purpose);
   ELSE
      l_data_level1 := NULL;
      l_pk_column_name_value_pairs.EXTEND (1);
      l_pk_column_name_value_pairs (1) :=
                           ego_col_name_value_pair_obj ('SITE_ID', p_site_id);
      l_class_code_name_value_pairs.EXTEND (1);
      l_class_code_name_value_pairs (1) :=
           ego_col_name_value_pair_obj ('SITE_USE_TYPE_CODE', p_site_purpose);
   END IF;
   BEGIN
      SELECT attr_group_id, application_id attr_group_app_id,
             attr_group_type, attr_group_name, data_level_int_name data_level
        INTO l_attr_grp_id, l_group_app_id,
             l_attr_group_type, l_attr_group_name, l_data_level
        FROM ego_obj_attr_grp_assocs_v
       WHERE attr_group_name = p_attr_group_name
         AND classification_code = p_site_purpose;
   EXCEPTION
      WHEN OTHERS
      THEN
         write_log_p('slc_sminf_single_uda_attrs_p: Error while retrieving attribute group '||SQLERRM);
   END;
   write_log_p ('slc_sminf_single_uda_attrs_p: p_attr_group_name --> ' || p_attr_group_name);
   write_log_p ('slc_sminf_single_uda_attrs_p: p_attr_name       --> ' || p_attr_name);
   write_log_p ('slc_sminf_single_uda_attrs_p: p_attr_char_value --> ' || p_attr_char_value);
   write_log_p ('slc_sminf_single_uda_attrs_p: p_attr_num_value  --> ' || p_attr_num_value);
   write_log_p ('slc_sminf_single_uda_attrs_p: p_attr_date_value --> ' || p_attr_date_value);
   
   l_count := l_count + 1;
   l_attributes_row_table.EXTEND;                                     
   l_attributes_row_table (l_count) :=
      ego_user_attr_row_obj (ego_import_row_seq_s.NEXTVAL,    -- ROW_IDENTIFIER
                             l_attr_grp_id,                    -- ATTR_GROUP_ID
                             l_group_app_id,               -- ATTR_GROUP_APP_ID
                             l_attr_group_type,              -- ATTR_GROUP_TYPE
                             l_attr_group_name,              -- ATTR_GROUP_NAME
                             l_data_level,                       -- NDATA_LEVEL
                             NVL (l_data_level1, NULL),         -- DATA_LEVEL_1
                             NULL,                              -- DATA_LEVEL_2
                             NULL,                              -- DATA_LEVEL_3
                             NULL,                              -- DATA_LEVEL_4
                             NULL,                              -- DATA_LEVEL_5
                             gc_sync_mode                       --SYNC
                            );
                            
   l_row_count := l_row_count + 1;
   l_attributes_data_table.EXTEND;                                 
   l_attributes_data_table (l_row_count) :=
      ego_user_attr_data_obj (ego_import_row_seq_s.CURRVAL,   -- ROW_IDENTIFIER
                              p_attr_name,                         -- ATTR_NAME
                              p_attr_char_value,              -- ATTR_VALUE_STR
                              p_attr_num_value,               -- ATTR_VALUE_NUM
                              p_attr_date_value,             -- ATTR_VALUE_DATE
                              NULL,                          -- ATTR_DISP_VALUE
                              NULL,                     -- ATTR_UNIT_OF_MEASURE
                              l_row_count                -- USER_ROW_IDENTIFIER
                             );
                            
   --Initialize the Error Handler to avoid previous error messages to be fetched --
   error_handler.initialize;
   ego_user_attrs_data_pub.process_user_attrs_data
                                          (l_api_version,
                                           l_object_name,
                                           l_attributes_row_table,
                                           l_attributes_data_table,
                                           l_pk_column_name_value_pairs,
                                           l_class_code_name_value_pairs,
                                           NULL,
                                           NULL,                
                                           NULL,             
                                           NULL,             
                                           '3',               
                                           fnd_api.g_true,
                                           fnd_api.g_true,
                                           fnd_api.g_true,
                                           fnd_api.g_true,     
                                           fnd_api.g_false,
                                           fnd_api.g_false,
                                           x_failed_row_id_list,
                                           x_return_status,
                                           x_errorcode,
                                           x_msg_count,
                                           x_msg_data
                                          );
   write_log_p ('slc_sminf_single_uda_attrs_p: x_return_status '||x_return_status);

   IF (x_return_status <> fnd_api.g_ret_sts_success)
   THEN
      write_log_p ('slc_sminf_single_uda_attrs_p: Error ');
      error_handler.get_message_list (x_message_list => x_message_list);

      FOR i IN 1 .. x_message_list.COUNT
      LOOP
         l_error_msg :=
                      l_error_msg || ' , ' || x_message_list (i).MESSAGE_TEXT;
         write_log_p ('slc_sminf_single_uda_attrs_p: Error '||l_error_msg);
      END LOOP;
   ELSE
      x_return_status := fnd_api.g_ret_sts_success;
      x_msg_data := gc_success;
      write_log_p ('slc_sminf_single_uda_attrs_p: Success ');
   END IF;
   
   x_msg_data := l_error_msg;
   write_log_p ('slc_sminf_single_uda_attrs_p: End of procedure');
EXCEPTION
   WHEN OTHERS
   THEN
      x_return_status := fnd_api.g_ret_sts_error;
      x_msg_data :=
            'slc_sminf_single_uda_attrs_p : In Other Exceptions:'
         || SQLCODE
         || ':'
         || SQLERRM;
      write_log_p (x_msg_data);
      write_log_p (   'slc_sminf_single_uda_attrs_p : Error :'
                   || DBMS_UTILITY.format_error_stack ()
                   || '  '
                   || DBMS_UTILITY.format_error_backtrace ()
                  );
END slc_sminf_single_uda_attrs_p;
 
  -------------------------------------------------------------------------------
--   Procedure      : slc_create_attributes_p
--   Purpose        : This procedure must be invoked from procedure
--                    slc_sminf_process_uda_attrs_p to create row attributes
--   Parameters     : p_site_id           IN          NUMBER
--                    p_attr_group_name   IN          VARCHAR2
--                    p_site_purpose      IN          VARCHAR2
--                    p_attributes_row_table IN ego_user_attr_row_table
--                    p_attributes_data_table IN ego_user_attr_data_table
--                    x_return_status     OUT NOCOPY  VARCHAR2
--                    x_msg_count         OUT NOCOPY  NUMBER
--                    x_msg_data          OUT NOCOPY  VARCHAR2
--   Modifications  :
-------------------------------------------------------------------------------

PROCEDURE slc_create_attributes_p (
   p_site_id                 IN              NUMBER,
   p_attr_group_name         IN              VARCHAR2,
   p_site_purpose            IN              VARCHAR2,
   p_attributes_row_table    IN              ego_user_attr_row_table,
   p_attributes_data_table   IN              ego_user_attr_data_table,
   x_return_status           OUT NOCOPY      VARCHAR2,
   x_msg_count               OUT NOCOPY      NUMBER,
   x_msg_data                OUT NOCOPY      VARCHAR2
)
IS
   l_api_version                   NUMBER                        := 1;
   l_object_name                   VARCHAR2 (20)                := 'RRS_SITE';
   l_pk_column_name_value_pairs    ego_col_name_value_pair_array
                                          := ego_col_name_value_pair_array
                                                                          ();
   l_class_code_name_value_pairs   ego_col_name_value_pair_array
                                          := ego_col_name_value_pair_array
                                                                          ();
   l_user_privileges_on_object     ego_varchar_tbl_type          := NULL;
   l_entity_id                     NUMBER                        := NULL;
   l_entity_index                  NUMBER                        := NULL;
   l_entity_code                   VARCHAR2 (1)                  := NULL;
   l_debug_level                   NUMBER                        := 3;
   l_error_msg                     VARCHAR2 (4000)               := NULL;
   l_init_error_handler            VARCHAR2 (1)             := fnd_api.g_true;
   l_write_to_concurrent_log       VARCHAR2 (1)             := fnd_api.g_true;
   l_init_fnd_msg_list             VARCHAR2 (1)             := fnd_api.g_true;
   l_log_errors                    VARCHAR2 (1)             := fnd_api.g_true;
   l_add_errors_to_fnd_stack       VARCHAR2 (1)            := fnd_api.g_false;
   l_commit                        VARCHAR2 (1)            := fnd_api.g_false;
   x_failed_row_id_list            VARCHAR2 (255);
   x_errorcode                     NUMBER;
   x_message_list                  error_handler.error_tbl_type;
BEGIN
   write_log_p ('slc_create_attributes_p: Start of procedure');
   l_pk_column_name_value_pairs.EXTEND (1);
   l_pk_column_name_value_pairs (1) :=
                           ego_col_name_value_pair_obj ('SITE_ID', p_site_id);
   l_class_code_name_value_pairs.EXTEND (1);
   l_class_code_name_value_pairs (1) :=
           ego_col_name_value_pair_obj ('SITE_USE_TYPE_CODE', p_site_purpose);
   write_log_p ('slc_create_attributes_p:Site ID: '|| p_site_id);
   write_log_p ('slc_create_attributes_p:Processing Attribute Group: '|| p_attr_group_name);
   error_handler.initialize;
   ego_user_attrs_data_pub.process_user_attrs_data
                                 (l_api_version,
                                  l_object_name,
                                  p_attributes_row_table,
                                  p_attributes_data_table,
                                  l_pk_column_name_value_pairs,
                                  l_class_code_name_value_pairs,
                                  NULL,         --l_user_privileges_on_object,
                                  NULL,                         --l_entity_id,
                                  NULL,                      --l_entity_index,
                                  NULL,                       --l_entity_code,
                                  '3',                        --l_debug_level,
                                  fnd_api.g_true,      --l_init_error_handler,
                                  fnd_api.g_true, --l_write_to_concurrent_log,
                                  fnd_api.g_true,       --l_init_fnd_msg_list,
                                  fnd_api.g_true,              --l_log_errors,
                                  fnd_api.g_false,
                                  l_commit,                        --l_commit,
                                  x_failed_row_id_list,
                                  x_return_status,
                                  x_errorcode,
                                  x_msg_count,
                                  x_msg_data
                                 );
   write_log_p ('slc_create_attributes_p:x_return_status: ' || x_return_status);

   IF (x_return_status <> fnd_api.g_ret_sts_success)
   THEN
      error_handler.get_message_list (x_message_list => x_message_list);

      FOR i IN 1 .. x_message_list.COUNT
      LOOP
         l_error_msg :=
                      l_error_msg || ' , ' || x_message_list (i).MESSAGE_TEXT;
         write_log_p ('slc_create_attributes_p: Error: ' || l_error_msg);
      END LOOP;
   ELSE
      x_return_status := fnd_api.g_ret_sts_success;
      x_msg_data := gc_success;
      write_log_p ('slc_create_attributes_p: SUCCESS');
   END IF;

   write_log_p ('slc_create_attributes_p: End of procedure');
EXCEPTION
   WHEN OTHERS
   THEN
      x_return_status := fnd_api.g_ret_sts_error;
      x_msg_data :=
            'slc_create_attributes_p: In Other Exceptions: '
         || SQLCODE
         || ':'
         || SQLERRM;
      write_log_p ('slc_create_attributes_p: '||x_msg_data);
      write_log_p (   'slc_create_attributes_p: Error :'
                   || DBMS_UTILITY.format_error_stack ()
                   || '  '
                   || DBMS_UTILITY.format_error_backtrace ()
                  );         
         
END slc_create_attributes_p;
   
   -------------------------------------------------------------------------------
--   Procedure      : slc_sminf_multirow_uda_attrs_p
--   Purpose        : This procedure must be invoked from procedure
--                    slc_sminf_process_uda_attrs_p to process multi row attributes
--   Parameters     : p_site_id           IN          NUMBER
--                    p_attr_group_name   IN          VARCHAR2
--                    p_extension_id      IN          NUMBER
--                    p_batch_id          IN          NUMBER
--                    p_site_purpose      IN          VARCHAR2
--                    x_return_status     OUT NOCOPY  VARCHAR2
--                    x_msg_count         OUT NOCOPY  NUMBER
--                    x_msg_data          OUT NOCOPY  VARCHAR2
--   Modifications  :
-------------------------------------------------------------------------------

PROCEDURE slc_sminf_multirow_uda_attrs_p (
   p_site_id           IN              NUMBER,
   p_attr_group_name   IN              VARCHAR2,
   p_extension_id      IN              NUMBER,
   p_batch_id          IN              NUMBER,
   p_site_purpose      IN              VARCHAR2,
   x_return_status     OUT NOCOPY      VARCHAR2,
   x_msg_count         OUT NOCOPY      NUMBER,
   x_msg_data          OUT NOCOPY      VARCHAR2
)
IS
   l_api_version                   NUMBER                        := 1;
   l_object_name                   VARCHAR2 (20)                := 'RRS_SITE';
   l_attributes_row_table          ego_user_attr_row_table
                                                := ego_user_attr_row_table
                                                                          ();
   l_attributes_data_table         ego_user_attr_data_table
                                               := ego_user_attr_data_table
                                                                          ();
   l_pk_column_name_value_pairs    ego_col_name_value_pair_array
                                          := ego_col_name_value_pair_array
                                                                          ();
   l_class_code_name_value_pairs   ego_col_name_value_pair_array
                                          := ego_col_name_value_pair_array
                                                                          ();
   l_user_privileges_on_object     ego_varchar_tbl_type          := NULL;
   l_entity_id                     NUMBER                        := NULL;
   l_entity_index                  NUMBER                        := NULL;
   l_entity_code                   VARCHAR2 (1)                  := NULL;
   l_debug_level                   NUMBER                        := 3;
   l_attr_grp_id                   NUMBER                        := 0;
   l_group_app_id                  NUMBER                        := 0;
   l_attr_group_type               VARCHAR2 (100);
   l_attr_group_name               VARCHAR2 (100);
   l_data_level                    VARCHAR2 (100);
   l_error_msg                     VARCHAR2 (4000)               := NULL;
   l_init_error_handler            VARCHAR2 (1)             := fnd_api.g_true;
   l_write_to_concurrent_log       VARCHAR2 (1)             := fnd_api.g_true;
   l_init_fnd_msg_list             VARCHAR2 (1)             := fnd_api.g_true;
   l_log_errors                    VARCHAR2 (1)             := fnd_api.g_true;
   l_add_errors_to_fnd_stack       VARCHAR2 (1)            := fnd_api.g_false;
   l_commit                        VARCHAR2 (1)            := fnd_api.g_false;
   x_failed_row_id_list            VARCHAR2 (255);
   x_errorcode                     NUMBER;
   x_message_list                  error_handler.error_tbl_type;
   l_site_id                       NUMBER;
   l_purpose                       VARCHAR2 (200);
   l_attr_value                    NUMBER;
   l_count                         PLS_INTEGER                   := 0;
   l_row_count                     PLS_INTEGER                   := 0;
   l_party_id                      NUMBER;

   -- Active to Prior Page
   CURSOR cur_site (
      p_site_id           NUMBER,
      p_attr_group_name   VARCHAR2,
      p_extension_id      NUMBER
   )
   IS
      SELECT site.extension_id, site.site_id, site.site_use_type_code,
             site.attribute_group_type, site.attribute_group_app_id,
             site.attributegroup_id, site.attribute_group_data_level,
             site.attribute_id, site.attribute_group_name,
             site.attribute_name, site.attribute_char_value,
             site.attribute_number_value, site.attribute_date_value,
             site.attribute_datetime_value, site.VALUE
        FROM (SELECT extension_id, site_id, site_use_type_code,
                     attribute_group_type, attribute_group_app_id,
                     attributegroup_id, attribute_group_data_level,
                     attribute_id,
                     DECODE
                        (attribute_group_name,
                         
                         --Multi-row Attribute Groups (Active to Prior)
                         gc_agreement_grp_active, --'SLC_SM_AGREEMENT', 
                         gc_agreement_grp_prior, --'SLC_SM_AGREEMENT_PRIOR',
                         gc_amendment_grp_active,  --'SLC_SM_AMENDMENT',
                         gc_amendment_grp_prior,   --'SLC_SM_AMENDMENT_PRIOR',
                         gc_franchisee_grp_active, --'SLC_SM_FRANCHISEE_DETAILS',
                         gc_franchisee_grp_prior,  --'SLC_SM_FRANCHISEE_DETAILS_PRIO',
                         gc_breach_grp_active,     --'SLC_SM_BREACH', 
                         gc_breach_grp_prior,      --'SLC_SM_BREACH_PRIOR',
                         gc_lon_grp_active,        --'SLC_SM_LON',
                         gc_lon_grp_prior,         --'SLC_SM_LON_PRIOR',
                         gc_startstop_grp_active,  --'SLC_SM_START_STOP_FINANCING',
                         gc_startstop_grp_prior,   --'SLC_SM_START_STOP_FINANC_PRIOR',
                         gc_rvpr_grp_active,       --'SLC_SM_RVPR',
                         gc_rvpr_grp_prior,        --'SLC_SM_RVPR_PRIOR',
						 --Added on 08 Aug 17
						 gc_transfers_grp,         --'SLC_SM_TRANSFERS'
                         gc_transfers_grp_prior,   --'SLC_SM_TRANSFERS_PRIOR'
						 --
                         gc_setl_agrmnt_grp_active,  --'SLC_SM_SETTLEMENT_AGREEMENT',
                         gc_setl_agrmnt_grp_prior,   --'SLC_SM_SETTLEMENT_AGREEMENT_BR',
                         gc_ebba_grp_active,         --'SLC_SM_EBBA',
                         gc_ebba_grp_prior,          --'SLC_SM_EBBA_PRIOR',
						 gc_mgmt_agrmnt_grp,         --'SLC_SM_MANAGEMENT_AGREEMENT',
						 gc_mgmt_agrmnt_prior_grp,   --'SLC_SM_MANAGEMENT_AGREE_PRIOR',
						 --Changes for v1.1
						 --Commenting this as this is not getting copied
						 --gc_draw_mgmt_grp,           --'SLC_SM_DRAW_MANAGEMENT',
                         --						 
                         --Multi-row Attribute Groups (Pending to Active)
                         gc_franchisee_grp_pending, --'SLC_SM_FRANCHISEE_DETAILS_PEND',
                         gc_franchisee_grp_active,  --'SLC_SM_FRANCHISEE_DETAILS',
                         gc_agreement_grp_pending,  --'SLC_SM_AGREEMENT_PEND',
                         gc_agreement_grp_active,   --'SLC_SM_AGREEMENT',
                         gc_amendment_grp_pending,  --'SLC_SM_AMENDMENT_PEND',
                         gc_amendment_grp_active,   --'SLC_SM_AMENDMENT',
                         gc_templic_grp_pending,    --'SLC_SM_TEMP_LICENSE_PEND', 
                         gc_templic_grp_active      --'SLC_SM_TEMP_LICENSE'
                        ) attribute_group_name,
                     DECODE (attribute_name,
                             gc_fee_value_attr_name,      --'FEE_VALUE_P_N', 
							 gc_fee_value_perc_attr_name, --'FEE_VALUE_PERC_OR_NUMBER',
                             attribute_name
                            ) attribute_name,
                     attribute_char_value, attribute_number_value,
                     attribute_date_value, attribute_datetime_value,
                     COALESCE (TO_CHAR (attribute_char_value),
                               TO_CHAR (attribute_number_value),
                               TO_CHAR (attribute_date_value),
                               TO_CHAR (attribute_datetime_value)
                              ) VALUE
                FROM slcapps.slc_sminf_attr_details_stg
               WHERE site_id = p_site_id
                 AND status_code = 'N'
                 AND batch_id = g_batch_id
                 AND attribute_group_name = p_attr_group_name
                 AND extension_id = p_extension_id
                 AND attribute_group_name IN
                        (gc_agreement_grp_active,  --'SLC_SM_AGREEMENT', 
                         gc_amendment_grp_active,  --'SLC_SM_AMENDMENT',
                         gc_franchisee_grp_active, --'SLC_SM_FRANCHISEE_DETAILS',
                         gc_breach_grp_active,     --'SLC_SM_BREACH',
                         gc_lon_grp_active,        --'SLC_SM_LON',
                         gc_startstop_grp_active,  --'SLC_SM_START_STOP_FINANCING',
                         gc_rvpr_grp_active,       --'SLC_SM_RVPR',
                         gc_franchisee_grp_pending, --'SLC_SM_FRANCHISEE_DETAILS_PEND',
                         gc_agreement_grp_pending, --'SLC_SM_AGREEMENT_PEND',
                         gc_amendment_grp_pending, --'SLC_SM_AMENDMENT_PEND',
						 --Added on 08 Aug 17
						 gc_transfers_grp,
						 gc_setl_agrmnt_grp_active,
						 gc_ebba_grp_active,	
                         gc_mgmt_agrmnt_grp, --'SLC_SM_MANAGEMENT_AGREEMENT',	
                         gc_draw_mgmt_grp,   --'SLC_SM_DRAW_MANAGEMENT',						 
                         gc_templic_grp_pending)   --'SLC_SM_TEMP_LICENSE_PEND'
                 AND attribute_name NOT IN
                                  ( gc_no_of_store_attr_name, --'NUMBER_OF_STORES_OWNED'
								    gc_orig_date_attr_name   --ORIGINAL_DATE'
								  )
								  ) site
       WHERE
             --Pending to Active group effective end date shd be null
             site.attribute_name NOT IN
                (CASE site.attribute_group_name
                    WHEN gc_agreement_grp_active --'SLC_SM_AGREEMENT'
                       THEN gc_eff_end_attr_name --'EFFECTIVE_END_DATE'
					   
                    WHEN gc_amendment_grp_active --'SLC_SM_AMENDMENT'
                       THEN gc_eff_end_attr_name --'EFFECTIVE_END_DATE'
					   
                    WHEN gc_franchisee_grp_active --'SLC_SM_FRANCHISEE_DETAILS'
                       THEN gc_eff_end_attr_name --'EFFECTIVE_END_DATE'
					   
                    ELSE site.attribute_group_name
                 END
                );

   TYPE typ_site_tbl_type IS TABLE OF cur_site%ROWTYPE;

   l_site_tbl                      typ_site_tbl_type   := typ_site_tbl_type
                                                                           ();
   l_site_index                    NUMBER;
   l_site_count                    NUMBER                        := 0;
   l_attr_char_value               VARCHAR2 (150);                    
   l_attr_date_value               DATE;
   l_attr_name                     ego_attrs_v.attr_name%TYPE;
   l_attr_num_value                NUMBER;
   x_return_msg                    VARCHAR2 (4000)               := NULL;
   l_supplier_orig_date            DATE                          DEFAULT NULL;
   
   --Changes for v1.1
   ln_seq_value						NUMBER;
   
   --Changes for v1.1
   ln_loop_counter		NUMBER := 0;
   lc_debug_data_obj    ego_user_attr_data_obj;
   
   CURSOR c_get_stg_data(p_site_id IN NUMBER
						,p_batch_id IN NUMBER
						,p_extension_id IN NUMBER
						,p_attribute_group_name IN VARCHAR2
						,p_attribute_name	IN VARCHAR2
						)
   IS
   SELECT attribute_char_value,attribute_number_value,attribute_date_value
   FROM slc_sminf_attr_details_stg
   WHERE site_id = p_site_id
    AND status_code = 'N'
    AND batch_id = p_batch_id
	AND extension_id = p_extension_id
	AND attribute_group_name = p_attribute_group_name
	AND attribute_name = p_attribute_name;
	
	lv_term_date_char_val	slc_sminf_attr_details_stg.attribute_char_value%TYPE;
	ln_term_date_num_val	slc_sminf_attr_details_stg.attribute_number_value%TYPE;
	ld_term_date_date_val	slc_sminf_attr_details_stg.attribute_date_value%TYPE;

	lv_formnumber_char_val	slc_sminf_attr_details_stg.attribute_char_value%TYPE;
	ln_formnumber_num_val	slc_sminf_attr_details_stg.attribute_number_value%TYPE;
	ld_formnumber_date_val	slc_sminf_attr_details_stg.attribute_date_value%TYPE;

	lv_edition_char_val	slc_sminf_attr_details_stg.attribute_char_value%TYPE;
	ln_edition_num_val	slc_sminf_attr_details_stg.attribute_number_value%TYPE;
	ld_edition_date_val	slc_sminf_attr_details_stg.attribute_date_value%TYPE;
	
	ld_term_date			DATE;
   
BEGIN
   write_log_p ('slc_sminf_multirow_uda_attrs_p: Start of procedure');
   l_pk_column_name_value_pairs.EXTEND (1);
   l_pk_column_name_value_pairs (1) :=
                           ego_col_name_value_pair_obj ('SITE_ID', p_site_id);
   l_class_code_name_value_pairs.EXTEND (1);
   l_class_code_name_value_pairs (1) :=
           ego_col_name_value_pair_obj ('SITE_USE_TYPE_CODE', p_site_purpose);

   -- Fetch the attribute group details --
   BEGIN
      SELECT attr_group_id, application_id attr_group_app_id,
             attr_group_type, attr_group_name, data_level_int_name data_level
        INTO l_attr_grp_id, l_group_app_id,
             l_attr_group_type, l_attr_group_name, l_data_level
        FROM ego_obj_attr_grp_assocs_v
       WHERE attr_group_name =
                DECODE (p_attr_group_name,
                        --Multi-row Attribute Groups (Active to Prior)
                        gc_agreement_grp_active, gc_agreement_grp_prior,
                        gc_amendment_grp_active, gc_amendment_grp_prior,
                        gc_franchisee_grp_active, gc_franchisee_grp_prior,
                        gc_breach_grp_active, gc_breach_grp_prior,
                        gc_lon_grp_active, gc_lon_grp_prior,
                        gc_startstop_grp_active, gc_startstop_grp_prior,
                        gc_rvpr_grp_active, gc_rvpr_grp_prior,
						-- Added on 08 aug 2017
						gc_transfers_grp, gc_transfers_grp_prior,
						gc_setl_agrmnt_grp_active, gc_setl_agrmnt_grp_prior,
						gc_ebba_grp_active, gc_ebba_grp_prior,
						gc_mgmt_agrmnt_grp, --'SLC_SM_MANAGEMENT_AGREEMENT',
						gc_mgmt_agrmnt_prior_grp, --'SLC_SM_MANAGEMENT_AGREE_PRIOR',
						
                        --Multi-row Attribute Groups (Pending to Active)
                        gc_franchisee_grp_pending, gc_franchisee_grp_active,
                        gc_agreement_grp_pending, gc_agreement_grp_active,
                        gc_amendment_grp_pending, gc_amendment_grp_active,
                        gc_templic_grp_pending, gc_templic_grp_active
                       );
   EXCEPTION
      WHEN OTHERS
      THEN
         write_log_p
            (' slc_sminf_multirow_uda_attrs_p: In others exception : Error while deriving attr group level details'
            );
         l_attr_grp_id := NULL;
         l_group_app_id := NULL;
         l_attr_group_type := NULL;
         l_attr_group_name := NULL;
         l_data_level := NULL;
   END;

   OPEN cur_site (p_site_id, p_attr_group_name, p_extension_id);

   write_log_p ('slc_sminf_multirow_uda_attrs_p: Site ID :' || p_site_id || ', Batch ID :' || g_batch_id);
   write_log_p ('slc_sminf_multirow_uda_attrs_p: p_attr_group_name :' || p_attr_group_name || ', p_extension_id :' || p_extension_id);
   LOOP

      write_log_p ('----------++++++++----------');
      write_log_p (   'slc_sminf_multirow_uda_attrs_p: Migrating values to Attribute Group    --> '
                   || l_attr_group_name
                  );
      write_log_p ('slc_sminf_multirow_uda_attrs_p: Attribute Extension ID  --> ' || p_extension_id);
      write_log_p ('slc_sminf_multirow_uda_attrs_p: Attr Group ID           --> ' || l_attr_grp_id);
      write_log_p ('slc_sminf_multirow_uda_attrs_p: Attr Group type         --> ' || l_attr_group_type);
      write_log_p ('slc_sminf_multirow_uda_attrs_p: Attr Group name         --> ' || l_attr_group_name);
      write_log_p ('slc_sminf_multirow_uda_attrs_p: Attr Group Data level   --> ' || l_data_level);
	  
      l_count := l_count + 1;
      l_attributes_row_table.EXTEND;
	  
	  --Changes for v1.1
	  --In this procedure on certain condition slc_sminf_single_uda_attrs_p API was called to update Single Row UDA.
	  --In slc_sminf_single_uda_attrs_p sequence value of ego_import_row_seq_s is incremented because of which in this 
	  --procedure sequence value is not getting assigned properly to add the attributes within this attribute group.
	  --Thus saving this sequence value in local variable and using the same variable value rather than using currval on
	  --sequences.
	  ln_seq_value := ego_import_row_seq_s.NEXTVAL;
      l_attributes_row_table (l_count) :=
         ego_user_attr_row_obj (--ego_import_row_seq_s.NEXTVAL -- ROW_IDENTIFIER
								ln_seq_value -- ROW_IDENTIFIER
                                                            ,
                                l_attr_grp_id                 -- ATTR_GROUP_ID
                                             ,
                                l_group_app_id            -- ATTR_GROUP_APP_ID
                                              ,
                                l_attr_group_type           -- ATTR_GROUP_TYPE
                                                 ,
                                l_attr_group_name           -- ATTR_GROUP_NAME
                                                 ,
                                l_data_level                    -- NDATA_LEVEL
                                            ,
                                NULL                           -- DATA_LEVEL_1
                                    ,
                                NULL                           -- DATA_LEVEL_2
                                    ,
                                NULL                           -- DATA_LEVEL_3
                                    ,
                                NULL                           -- DATA_LEVEL_4
                                    ,
                                NULL                           -- DATA_LEVEL_5
                                    ,
                                gc_sync_mode                            --SYNC
                               );

      FETCH cur_site
      BULK COLLECT INTO l_site_tbl;

      l_site_count := cur_site%ROWCOUNT;

      IF cur_site%ROWCOUNT = 0
      THEN
         write_log_p
            ('slc_sminf_multirow_uda_attrs_p : No site records are available for processing'
            );
      ELSE
         FOR l_site_index IN 1 .. l_site_tbl.COUNT
         LOOP
            l_attr_name := NULL;
            l_attr_char_value := NULL;
            l_attr_date_value := NULL;
            l_attr_num_value := NULL;
			lv_term_date_char_val := NULL;
			ln_term_date_num_val := NULL;
			ld_term_date_date_val := NULL;
			lv_formnumber_char_val	:= NULL;
			ln_formnumber_num_val	:= NULL;
			ld_formnumber_date_val	:= NULL;

			lv_edition_char_val	:= NULL;
			ln_edition_num_val	:= NULL;
			ld_edition_date_val	:= NULL;
	
			ld_term_date		  := NULL;
            write_log_p ('slc_sminf_multirow_uda_attrs_p: Supplier Original Date --> ' || l_supplier_orig_date);
			write_log_p ('slc_sminf_multirow_uda_attrs_p: attribute_group_name:'||l_site_tbl (l_site_index).attribute_group_name);
			write_log_p ('slc_sminf_multirow_uda_attrs_p: attribute_name:'||l_site_tbl (l_site_index).attribute_name);
			
			write_log_p ('slc_sminf_multirow_uda_attrs_p: Current Sequence val:'||ln_seq_value);

            ------------ Business Rules --------------
            ------------ Pending to Active ---------------
             --For SLC_SM_FRANCHISEE_DETAILS_PEND, SLC_SM_AGREEMENT_PEND and SLC_SM_AMENDMENT_PEND
             --default effective start date to Actual Changeover Date from Active Operator page
            IF l_attr_group_name = gc_franchisee_grp_active --'SLC_SM_FRANCHISEE_DETAILS'
            THEN
               write_log_p ('slc_sminf_multirow_uda_attrs_p: ----SLC_SM_FRANCHISEE_DETAILS----');

               IF l_site_tbl (l_site_index).attribute_name =
                                                       gc_eff_start_attr_name --'EFFECTIVE_START_DATE'
               THEN
                  write_log_p ('slc_sminf_multirow_uda_attrs_p: ----EFFECTIVE_START_DATE----');
                  l_attr_date_value := SYSDATE;
               END IF;

--------------------------------------------------------------------------------------
               IF l_site_tbl (l_site_index).attribute_name = gc_franch_num_attr_name --'FRANCHISEE_NUM'
               THEN
                  l_attr_char_value :=
                               l_site_tbl (l_site_index).attribute_char_value;
                 ------------------------------------------------------------------------------------------------
                  write_log_p
                     ('slc_sminf_multirow_uda_attrs_p: << Updating Original Date in Supplier Hub for FRANCHISEE_NUM : >>'
                     );
                  write_log_p
                     (   'slc_sminf_multirow_uda_attrs_p: Deriving Original Date for Franchisee/Supplier Name: '
                      || l_attr_char_value
                     );

                  BEGIN
                     SELECT ap.party_id
                       INTO l_party_id
                       FROM ap_suppliers ap, hz_parties hp
                      WHERE hp.party_id = ap.party_id
                        AND ap.segment1 = l_attr_char_value;  --'GURDEEP LLC';
                  EXCEPTION
                     WHEN NO_DATA_FOUND
                     THEN
                        l_party_id := NULL;
                        write_log_p
                           (   'slc_sminf_multirow_uda_attrs_p: No party ID found for this supplier with Franchisee Name : '
                            || l_attr_char_value
                           );
                     WHEN OTHERS
                     THEN
                        l_party_id := NULL;
                        write_log_p ('slc_sminf_multirow_uda_attrs_p: Error while fetching Party ID'
                                     || SQLERRM
                                    );
                  END;

                  write_log_p ('slc_sminf_multirow_uda_attrs_p: Derivation - Party ID : ' || l_party_id);

--    "If ""Original Date"" in Supplier Hub for this Franchisee is NULL then populate as below:
--    => Supplier Hub.First Franchisee Date = ""Changeover Date""
--    => Site Hub.Original Date = ""Changeover Date""
--    Else
--    Derive ""Original Date"" from Supplier Hub"
                  BEGIN
                     SELECT pos.d_ext_attr2
                       INTO l_supplier_orig_date
                       FROM pos_supp_prof_ext_b pos
						--Changes for v1.1
						--POS table contains data for multiple attribute group.
						--Adding condition to check Data only for SLC_ISP_FRANCHISEE_DETAILS Attribute group
						   ,ego_attr_groups_v eagv
						   ,fnd_application fa
                      WHERE pos.party_id = l_party_id
						  AND eagv.attr_group_name = 'SLC_ISP_FRANCHISEE_DETAILS'
						  AND eagv.attr_group_id = pos.attr_group_id
						  AND fa.application_short_name = 'POS'
						  AND fa.application_id = eagv.application_id;
                  EXCEPTION
                     WHEN NO_DATA_FOUND
                     THEN
                        l_supplier_orig_date := NULL;
                        write_log_p
                           (   'slc_sminf_multirow_uda_attrs_p:  No First Franchisee Date found for this supplier with Party ID : '
                            || l_party_id
                           );
                     WHEN OTHERS
                     THEN
                        l_supplier_orig_date := NULL;
                        write_log_p
                           (   'slc_sminf_multirow_uda_attrs_p: Error while fetching First Franchisee Date for party ID '
                            || l_party_id
                            || SQLERRM
                           );
                  END;

                  write_log_p (   'slc_sminf_multirow_uda_attrs_p: Derivation - Supplier Original Date : '
                               || l_supplier_orig_date
                              );

							  /* commented nagesh v1.1 09 sept 2017
                  IF l_supplier_orig_date IS NULL
                  THEN
                     --Franchisee Num is coming as NULL when l_supplier_orig_date IS NULL
                     l_row_count := l_row_count + 1;
                     l_attributes_data_table.EXTEND;  
                     l_attributes_data_table (l_row_count) :=
                        ego_user_attr_data_obj
                              (--ego_import_row_seq_s.CURRVAL  -- ROW_IDENTIFIER
							  ln_seq_value
                                                           ,
                               gc_franch_num_attr_name,     --'FRANCHISEE_NUM',
                               l_attr_char_value,
                               NULL,
                               NULL,
                               NULL                         -- ATTR_DISP_VALUE
                                   ,
                               NULL                    -- ATTR_UNIT_OF_MEASURE
                                   ,
                               l_row_count              -- USER_ROW_IDENTIFIER
                              );
                  END IF;
*/

                  write_log_p
                     (   'slc_sminf_multirow_uda_attrs_p: <<< - Adding ORIGINAL_DATE to SLC_SM_FRANCHISEE_DETAILS  with value from supplier hub- >>>'
                      || l_supplier_orig_date
                     );
                  l_row_count := l_row_count + 1;
                  l_attributes_data_table.EXTEND;                     
                  l_attributes_data_table (l_row_count) :=
                     ego_user_attr_data_obj
                         (--ego_import_row_seq_s.CURRVAL,    -- ROW_IDENTIFIER
						  ln_seq_value,
                          gc_orig_date_attr_name, --'ORIGINAL_DATE',
                          NULL,
                          NULL,
						  --changeover date
                          NVL (l_supplier_orig_date, SYSDATE),
                          NULL,                         -- ATTR_DISP_VALUE
                          NULL,                         -- ATTR_UNIT_OF_MEASURE
                          l_row_count                   -- USER_ROW_IDENTIFIER
                         );

                  IF l_supplier_orig_date IS NULL AND l_party_id IS NOT NULL
                  THEN
                     write_log_p
                              ('slc_sminf_multirow_uda_attrs_p: <-- Calling slc_sminf_single_uda_attrs_p -->');
                     write_log_p
                        (   'slc_sminf_multirow_uda_attrs_p: Updating Original Date in Supplier hub with changeover Date : '
                         || SYSDATE
                        );
                     slc_sminf_single_uda_attrs_p
                           (p_site_id              => l_party_id,
                            p_attr_group_name      => gc_franch_dtls,
                            p_attr_name            => gc_franch_co,
                            p_attr_char_value      => NULL,
                            p_attr_num_value       => NULL,
                            p_attr_date_value      => SYSDATE, --changeover date
                            p_batch_id             => NULL,            
                            p_site_purpose         => g_site_franch_purpose,
                                                               --Supplier only
                            x_return_status        => x_return_status,
                            x_msg_count            => x_msg_count,
                            x_msg_data             => x_return_msg
                           );
                     write_log_p (   'slc_sminf_multirow_uda_attrs_p: Return status : '
                                  || x_return_status
                                  || 'Return count :'
                                  || x_msg_count
                                  || 'Return message :'
                                  || x_return_msg
                                 );
                     write_log_p
                               ('slc_sminf_multirow_uda_attrs_p: <-- End of  slc_sminf_single_uda_attrs_p -->');
                  END IF;                            --end of l_party_id
-----------------------------------------------------------------------------------------

               END IF;                           --end of FRANCHISEE_NUM
            END IF;                          --end of FRANCHISEE_DETAILS
            
-------------------------------------SLC_SM_AGREEMENT------------------------------------
            IF l_attr_group_name = gc_agreement_grp_active --'SLC_SM_AGREEMENT'
            THEN
               write_log_p ('slc_sminf_multirow_uda_attrs_p: ----SLC_SM_AGREEMENT----');

               --If the changeover type is for corporate then default the agreement to GGPS (02)
               IF l_site_tbl (l_site_index).attribute_name = gc_doc_type_attr_name --'DOCUMENT_TYPE'
               THEN
                  write_log_p ('slc_sminf_multirow_uda_attrs_p: ----DOCUMENT_TYPE----');
                  l_attr_char_value :=
                               l_site_tbl (l_site_index).attribute_char_value;
                  g_agreement_type_orig := l_attr_char_value;

                  BEGIN
                     SELECT flv.description
                       INTO g_agreement_type
                       FROM fnd_flex_values_vl flv, fnd_flex_value_sets flvs
                      WHERE flv.flex_value_set_id = flvs.flex_value_set_id
                        AND flvs.flex_value_set_name = gc_agrmnt_vs_name --'SLCSM_AGREEMENT_TYPE'
                        AND flv.flex_value = l_attr_char_value
						AND SYSDATE BETWEEN NVL(flv.start_date_active, SYSDATE - 1) AND NVL(flv.end_date_active, SYSDATE + 1)
                        AND NVL(flv.enabled_flag, 'N') = 'Y';
						
                     write_log_p ('slc_sminf_multirow_uda_attrs_p: g_agreement_type : ' || g_agreement_type);
					 
                  EXCEPTION
                     WHEN OTHERS
                     THEN
                        g_agreement_type := NULL;
                        write_log_p ('slc_sminf_multirow_uda_attrs_p: no data found in SLCSM_AGREEMENT_TYPE');
                  END;
               END IF;                            --end of DOCUMENT_TYPE

               -- If the changeover type is for corporate then default the edition to the latest GGPS edition.
               -- Derive it from "SLCPRC_FORM_NUMBER" lookup
               IF l_site_tbl (l_site_index).attribute_name = gc_edition_char_attr_name --'EDITION_CHAR'
               THEN
                  write_log_p ('slc_sminf_multirow_uda_attrs_p: ----EDITION_CHAR----');
                  l_attr_char_value :=
                               l_site_tbl (l_site_index).attribute_char_value;
                  g_agreement_edition := l_attr_char_value;
               END IF;

			   --Changes for v1.1 Begin 
			   --Signed Date and Creation Date should be copied from Pending page as is. Thus commenting this defaulting logic
			   
               --If the changeover type is for corporate default to sysdate
               IF l_site_tbl (l_site_index).attribute_name = gc_creat_date_attr_name --'CREATION_DATE'
               THEN
                  write_log_p ('slc_sminf_multirow_uda_attrs_p: ----CREATION_DATE----');
                  --convert creation date to eff start date
                  --l_attr_date_value := SYSDATE;   
					l_attr_date_value := l_site_tbl (l_site_index).attribute_date_value;
               END IF;

               --If the changeover type is for corporate default to sysdate
               IF l_site_tbl (l_site_index).attribute_name = gc_signed_date_attr_name --'SIGNED_DATE'
               THEN
                  write_log_p ('slc_sminf_multirow_uda_attrs_p: ----SIGNED_DATE----');
                  --convert creation date to eff start date
                  --l_attr_date_value := SYSDATE;                       
				  l_attr_date_value := l_site_tbl (l_site_index).attribute_date_value;
               END IF;
			   
               --If the changeover type is for corporate default to sysdate
               IF l_site_tbl (l_site_index).attribute_name = gc_eff_start_attr_name --'EFFECTIVE_START_DATE'
               THEN
                  write_log_p ('slc_sminf_multirow_uda_attrs_p: ----EFFECTIVE_START_DATE----');
                  --convert creation date to eff start date
                  l_attr_date_value := SYSDATE;                       
               END IF;			   
			   
				--Changes for v1.1 End 
				
               write_log_p ('slc_sminf_multirow_uda_attrs_p: g_agreement_edition -->' || g_agreement_edition);
               write_log_p ('slc_sminf_multirow_uda_attrs_p: g_agreement_type -->' || g_agreement_type);
            END IF;                            --end of SLC_SM_AGREEMENT

-------------------------------------SLC_SM_AGREEMENT------------------------------------

            -------------------------------------SLC_SM_AMENDMENT------------------------------------
            IF l_attr_group_name = gc_amendment_grp_active --'SLC_SM_AMENDMENT'
            THEN
               write_log_p ('slc_sminf_multirow_uda_attrs_p: ----SLC_SM_AMENDMENT----');

               --Effective changeover date
               IF l_site_tbl (l_site_index).attribute_name = gc_creat_date_attr_name --'CREATION_DATE'
               THEN
                  write_log_p ('slc_sminf_multirow_uda_attrs_p: ----CREATION_DATE----');
				  --Changes for v1.1
				  --Signed Date and Creation Date should be copied from Pending page as is. Thus commenting this defaulting logic
                  --l_attr_date_value := SYSDATE;   
					l_attr_date_value := l_site_tbl (l_site_index).attribute_date_value;
               END IF;

               --default to NULL
               IF l_site_tbl (l_site_index).attribute_name = gc_signed_date_attr_name --'SIGNED_DATE'
               THEN
                  write_log_p ('slc_sminf_multirow_uda_attrs_p: ----SIGNED_DATE----');
				  --Changes for v1.1
				  --Signed Date and Creation Date should be copied from Pending page as is. Thus commenting this defaulting logic				  
                  --l_attr_date_value := SYSDATE;
				  l_attr_date_value := l_site_tbl (l_site_index).attribute_date_value;
               END IF;
            END IF;

-------------------------------------SLC_SM_AMENDMENT------------------------------------

            -------------------------------------Active to Prior------------------------------------
            IF l_attr_group_name IN
                  (gc_franchisee_grp_prior, --'SLC_SM_FRANCHISEE_DETAILS_PRIO',
                   gc_agreement_grp_prior,  --'SLC_SM_AGREEMENT_PRIOR',
                   gc_amendment_grp_prior)  --'SLC_SM_AMENDMENT_PRIOR'
            THEN
               write_log_p ('slc_sminf_multirow_uda_attrs_p: <<<SLC_SM_FRANCHISEE_DETAILS_PRIO>>>');
               write_log_p ('slc_sminf_multirow_uda_attrs_p: <<<SLC_SM_AGREEMENT_PRIOR>>>');
               write_log_p ('slc_sminf_multirow_uda_attrs_p: <<<SLC_SM_AMENDMENT_PRIOR>>>');

               IF l_site_tbl (l_site_index).attribute_name =
                                                         gc_eff_end_attr_name --'EFFECTIVE_END_DATE'
               THEN
                  write_log_p ('slc_sminf_multirow_uda_attrs_p: ----EFFECTIVE_END_DATE----');

                  IF    l_site_tbl (l_site_index).attribute_date_value IS NULL
                     OR l_site_tbl (l_site_index).attribute_date_value >
                                                                       SYSDATE
                  THEN
                     l_attr_date_value := SYSDATE - 1;
                  END IF;
               END IF;
            END IF;
			
			--Changes for v1.1
			--Defaulting end date when populating data from EBBA, Management agreements Active page to Prior Page
			IF l_attr_group_name IN (gc_ebba_grp_prior ,gc_mgmt_agrmnt_prior_grp) 
				AND l_site_tbl (l_site_index).attribute_name = 'END_DATE' THEN
				write_log_p (   'slc_sminf_multirow_uda_attrs_p: Defaulting End Date for EBBA and Management Agreement');
               l_attr_date_value := SYSDATE - 1;
			END IF;
			
-------------------------------------Active to Prior------------------------------------
            write_log_p (   'slc_sminf_multirow_uda_attrs_p: Attribute Name               --> '
                         || l_site_tbl (l_site_index).attribute_name
                        );
            write_log_p (   'slc_sminf_multirow_uda_attrs_p: Attribute Char   Value Old   --> '
                         || l_site_tbl (l_site_index).attribute_char_value
                        );
            write_log_p (   'slc_sminf_multirow_uda_attrs_p: Attribute Char   Value New   --> '
                         || l_attr_char_value
                        );
            write_log_p (   'slc_sminf_multirow_uda_attrs_p: Attribute Date   Value Old   --> '
                         || l_site_tbl (l_site_index).attribute_date_value
                        );
            write_log_p (   'slc_sminf_multirow_uda_attrs_p: Attribute Date   Value New   --> '
                         || l_attr_date_value
                        );
            write_log_p (   'slc_sminf_multirow_uda_attrs_p: Attribute Number Value       --> '
                         || l_site_tbl (l_site_index).attribute_number_value
                        );
            l_row_count := l_row_count + 1;
            l_attributes_data_table.EXTEND;
            l_attributes_data_table (l_row_count) :=
               ego_user_attr_data_obj
                          (--ego_import_row_seq_s.CURRVAL,
						   ln_seq_value,
                           l_site_tbl (l_site_index).attribute_name,
                           NVL (l_attr_char_value,
                                l_site_tbl (l_site_index).attribute_char_value
                               ),
                           l_site_tbl (l_site_index).attribute_number_value,
                           NVL (l_attr_date_value,
                                l_site_tbl (l_site_index).attribute_date_value
                               ),
                           NULL,
                           NULL,
                           l_row_count
                          );


            IF     l_site_tbl (l_site_index).attribute_group_name =
                                                            gc_agreement_grp_active --'SLC_SM_AGREEMENT'
               AND l_site_tbl (l_site_index).attribute_name = gc_doc_type_attr_name --'DOCUMENT_TYPE'
            THEN
               write_log_p
                   ('slc_sminf_multirow_uda_attrs_p: <<< Adding EFFECTIVE_START_DATE to SLC_SM_AGREEMENT >>>');
			   write_log_p ('Akshay: Entering EFFECTIVE_START_DATE');
               l_row_count := l_row_count + 1;
               l_attributes_data_table.EXTEND;                        
               l_attributes_data_table (l_row_count) :=
                  ego_user_attr_data_obj
                              (--ego_import_row_seq_s.CURRVAL  -- ROW_IDENTIFIER
								ln_seq_value
                                                           ,
                               gc_eff_start_attr_name, --'EFFECTIVE_START_DATE'
                               NULL,
                               NULL,
                               SYSDATE,
                               NULL                         -- ATTR_DISP_VALUE
                                   ,
                               NULL                    -- ATTR_UNIT_OF_MEASURE
                                   ,
                               l_row_count              -- USER_ROW_IDENTIFIER
                              );
				
				--Changes for v1.1
				--New logic to derive Term Date
				 write_log_p ('New logic to derive term date');
				 BEGIN
					OPEN c_get_stg_data(p_site_id,g_batch_id,p_extension_id,p_attr_group_name,'CONTRACT_ID');
					FETCH c_get_stg_data INTO lv_term_date_char_val,ln_term_date_num_val,ld_term_date_date_val;
					CLOSE c_get_stg_data;
					write_log_p ('Deriving Contract Id Agreement.'||lv_term_date_char_val||' :'||ln_term_date_num_val);
					
					SELECT DECODE(flv.attribute3,NULL,NULL,add_months(sysdate,flv.attribute3))
					INTO ld_term_date
					FROM okc_rep_contracts_all orca,
					  okc_terms_templates_all otta,
					  Okc_Template_Usages Otu,
					  Fnd_Lookup_Values flv
					WHERE contract_id    = lv_term_date_char_val --U_TRADITIONAL AGREEMENT_4400019_0317
					AND otta.template_id = otu.template_id
					AND otu.document_id  = orca.contract_id
					AND flv.lookup_code  = otta.attribute1
					AND flv.lookup_type  = 'SLCOKC_FORM_NUMBER'
					AND flv.enabled_flag = 'Y'
					AND TRUNC(SYSDATE) BETWEEN TRUNC(NVL(flv.start_date_active,SYSDATE)) AND TRUNC(NVL(flv.end_date_active,SYSDATE));
					
					write_log_p ('Term Date month calculation.'||to_char(ld_term_date,'DD-MM-YYYY'));

				   l_row_count := l_row_count + 1;
				   l_attributes_data_table.EXTEND;                        
				   l_attributes_data_table (l_row_count) :=
					  ego_user_attr_data_obj
								  (--ego_import_row_seq_s.CURRVAL  -- ROW_IDENTIFIER
									ln_seq_value
															   ,
								   'TERM_DATE', --'TERM_DATE'
								   NULL,
								   NULL,
								   ld_term_date,
								   NULL                         -- ATTR_DISP_VALUE
									   ,
								   NULL                    -- ATTR_UNIT_OF_MEASURE
									   ,
								   l_row_count              -- USER_ROW_IDENTIFIER
								  );					
				 EXCEPTION
				 WHEN OTHERS THEN
					write_log_p ('Exception while deriving Term date logic.');
				 END;
				 
            END IF;

            IF  (l_site_tbl (l_site_index).attribute_group_name =
                                                            gc_amendment_grp_active --'SLC_SM_AMENDMENT'
               AND l_site_tbl (l_site_index).attribute_name = gc_form_num_attr_name --'FORM_NUMBER'
					)
            THEN
               write_log_p
                   ('slc_sminf_multirow_uda_attrs_p: <<< Adding EFFECTIVE_START_DATE to SLC_SM_AMENDMENT >>>');
               l_row_count := l_row_count + 1;
               l_attributes_data_table.EXTEND;                        
               l_attributes_data_table (l_row_count) :=
                  ego_user_attr_data_obj
                              (--ego_import_row_seq_s.CURRVAL  -- ROW_IDENTIFIER
								ln_seq_value
                                                           ,
                               gc_eff_start_attr_name, --'EFFECTIVE_START_DATE',
                               NULL,
                               NULL,
                               SYSDATE,
                               NULL                         -- ATTR_DISP_VALUE
                                   ,
                               NULL                    -- ATTR_UNIT_OF_MEASURE
                                   ,
                               l_row_count              -- USER_ROW_IDENTIFIER
                              ); 
							  
				--Changes for v1.1
				--New logic to derive Term Date
				 write_log_p ('New logic to derive Effective End Date');
				 BEGIN
					OPEN c_get_stg_data(p_site_id,g_batch_id,p_extension_id,p_attr_group_name,'FORM_NUMBER');
					FETCH c_get_stg_data INTO lv_formnumber_char_val,ln_formnumber_num_val,ld_formnumber_date_val;
					CLOSE c_get_stg_data;
					write_log_p ('Deriving Form Number for Amendment.'||lv_formnumber_char_val||' :'||ln_formnumber_num_val);
					
					OPEN c_get_stg_data(p_site_id,g_batch_id,p_extension_id,p_attr_group_name,'EDITION_CHAR');
					FETCH c_get_stg_data INTO lv_edition_char_val,ln_edition_num_val,ld_edition_date_val;
					CLOSE c_get_stg_data;
					write_log_p ('Deriving Edition Char for Agreement.'||lv_edition_char_val||' :'||ln_edition_num_val);
					
				  SELECT DECODE(flv.attribute3,NULL,NULL,add_months(sysdate,flv.attribute3))
					INTO ld_term_date
					FROM Fnd_Lookup_Values flv
					WHERE flv.lookup_code  = lv_formnumber_char_val||'-'||lv_edition_char_val
					AND flv.lookup_type  = 'SLCOKC_FORM_NUMBER'
					AND flv.enabled_flag = 'Y'
					AND TRUNC(SYSDATE) BETWEEN TRUNC(NVL(flv.start_date_active,SYSDATE)) AND TRUNC(NVL(flv.end_date_active,SYSDATE));					

					write_log_p ('Effective End Date month calculation.'||to_char(ld_term_date,'DD-MM-YYYY'));

				   l_row_count := l_row_count + 1;
				   l_attributes_data_table.EXTEND;                        
				   l_attributes_data_table (l_row_count) :=
					  ego_user_attr_data_obj
								  (--ego_import_row_seq_s.CURRVAL  -- ROW_IDENTIFIER
									ln_seq_value
															   ,
								   'EFFECTIVE_END_DATE', --'EFFECTIVE_START_DATE'
								   NULL,
								   NULL,
								   ld_term_date,
								   NULL                         -- ATTR_DISP_VALUE
									   ,
								   NULL                    -- ATTR_UNIT_OF_MEASURE
									   ,
								   l_row_count              -- USER_ROW_IDENTIFIER
								  );						
				 EXCEPTION
				 WHEN OTHERS THEN
				 write_log_p ('Exception while deriving Effective End Date logic.');
				 END;							  
            END IF;
         END LOOP;
      END IF;

      EXIT WHEN cur_site%NOTFOUND;
   END LOOP;

   CLOSE cur_site;
	
	--Channges for v1.1
	--Loop to debug values with which API's will be called.
	IF l_attributes_data_table.COUNT > 0 THEN
	write_log_p('*************UDA Printing Start*******************');	
	FOR ln_loop_counter IN l_attributes_data_table.FIRST..l_attributes_data_table.LAST
	LOOP
	lc_debug_data_obj := l_attributes_data_table(ln_loop_counter);
		
		write_log_p ('ROW_IDENTIFIER:		'||lc_debug_data_obj.ROW_IDENTIFIER);
		write_log_p ('ATTR_NAME:			'||lc_debug_data_obj.ATTR_NAME);
		write_log_p ('ATTR_VALUE_STR:		'||lc_debug_data_obj.ATTR_VALUE_STR);
		write_log_p ('ATTR_VALUE_DATE:		'||lc_debug_data_obj.ATTR_VALUE_DATE);
		write_log_p ('ATTR_DISP_VALUE:		'||lc_debug_data_obj.ATTR_DISP_VALUE);
		write_log_p ('USER_ROW_IDENTIFIER:	'||lc_debug_data_obj.USER_ROW_IDENTIFIER);
	END LOOP;
	write_log_p('*************UDA Printing End*******************');
    END IF;	
	
   IF l_site_count > 0
   THEN
      --Initialize the Error Handler to avoid previous error messages to be fetched --
      error_handler.initialize;
      ego_user_attrs_data_pub.process_user_attrs_data
                                 (l_api_version,
                                  l_object_name,
                                  l_attributes_row_table,
                                  l_attributes_data_table,
                                  l_pk_column_name_value_pairs,
                                  l_class_code_name_value_pairs,
                                  NULL,         --l_user_privileges_on_object,
                                  NULL,                         --l_entity_id,
                                  NULL,                      --l_entity_index,
                                  NULL,                       --l_entity_code,
                                  '3',                        --l_debug_level,
                                  fnd_api.g_true,      --l_init_error_handler,
                                  fnd_api.g_true, --l_write_to_concurrent_log,
                                  fnd_api.g_true,       --l_init_fnd_msg_list,
                                  fnd_api.g_true,              --l_log_errors,
                                  fnd_api.g_false,
                                  --l_add_errors_to_fnd_stack,
                                  fnd_api.g_false,                 --l_commit,
                                  x_failed_row_id_list,
                                  x_return_status,
                                  x_errorcode,
                                  x_msg_count,
                                  x_msg_data
                                 );
      write_log_p ('slc_sminf_multirow_uda_attrs_p: x_return_status: ' || x_return_status);

      IF (x_return_status <> fnd_api.g_ret_sts_success)
      THEN
         error_handler.get_message_list (x_message_list => x_message_list);

         FOR i IN 1 .. x_message_list.COUNT
         LOOP
            l_error_msg :=
                      l_error_msg || ' , ' || x_message_list (i).MESSAGE_TEXT;
            write_log_p ('slc_sminf_multirow_uda_attrs_p: error: ' || l_error_msg);
         END LOOP;
      ELSE
         x_return_status := fnd_api.g_ret_sts_success;
         x_msg_data := gc_success;
      END IF;
   ELSE
      x_return_status := fnd_api.g_ret_sts_error;
      x_msg_data :=
             'ERROR : No records inserted into API table type for processing';
   END IF;

   write_log_p ('slc_sminf_multirow_uda_attrs_p: End of procedure');
EXCEPTION
   WHEN OTHERS
   THEN
      write_log_p (   'slc_sminf_multirow_uda_attrs_p : ERROR '
                   || ' In Other Exceptions: '
                   || SQLCODE
                   || ':'
                   || SQLERRM
                  );
      write_log_p (   'slc_sminf_multirow_uda_attrs_p: Error :'
                   || DBMS_UTILITY.format_error_stack ()
                   || '  '
                   || DBMS_UTILITY.format_error_backtrace ()
                  );
      x_return_status := fnd_api.g_ret_sts_error;
      x_msg_data :=
            'slc_sminf_multirow_uda_attrs_p: In Other Exceptions: '
         || SQLCODE
         || ':'
         || SQLERRM;
END slc_sminf_multirow_uda_attrs_p;

   -------------------------------------------------------------------------------
--   Procedure      : slc_sminf_backup_uda_attrs_p
--   Purpose        : This procedure must be invoked from procedure
--                    slc_sminf_process_sites_p to backup attributes from all the
--                    Pages for each site
--   Parameters     : p_site_id           IN          NUMBER
--                    x_return_status     OUT NOCOPY  VARCHAR2
--                    x_msg_count         OUT NOCOPY  NUMBER
--                    x_msg_data          OUT NOCOPY  VARCHAR2
--   Modifications  :
-------------------------------------------------------------------------------

PROCEDURE slc_sminf_backup_uda_attrs_p (
   p_site_id         IN              NUMBER,
   x_return_status   OUT NOCOPY      VARCHAR2,
   x_msg_count       OUT NOCOPY      NUMBER,
   x_msg_data        OUT NOCOPY      VARCHAR2
)
IS
   CURSOR cur_site (p_site_id NUMBER)
   IS
      SELECT uda.extension_id, rrs.site_id, site_use_type_code,
             ag.attr_group_id AS attributegroup_id,
             ag.attr_group_type AS attribute_group_type,                
             ag.application_id AS attribute_group_app_id,               
             'SITE_LEVEL' AS attribute_group_data_level,                
             ag.attr_group_name AS attribute_group_name,
             agc.attr_id AS attribute_id, agc.attr_name AS attribute_name,
             agc.data_type_code AS data_type_code,
             DECODE (agc.data_type_code,
                     'C', DECODE (agc.database_column,
                                  'C_EXT_ATTR1', uda.c_ext_attr1,
                                  'C_EXT_ATTR2', uda.c_ext_attr2,
                                  'C_EXT_ATTR3', uda.c_ext_attr3,
                                  'C_EXT_ATTR4', uda.c_ext_attr4,
                                  'C_EXT_ATTR5', uda.c_ext_attr5,
                                  'C_EXT_ATTR6', uda.c_ext_attr6,
                                  'C_EXT_ATTR7', uda.c_ext_attr7,
                                  'C_EXT_ATTR8', uda.c_ext_attr8,
                                  'C_EXT_ATTR9', uda.c_ext_attr9,
                                  'C_EXT_ATTR10', uda.c_ext_attr10,
                                  'C_EXT_ATTR11', uda.c_ext_attr11,
                                  'C_EXT_ATTR12', uda.c_ext_attr12,
                                  'C_EXT_ATTR13', uda.c_ext_attr13,
                                  'C_EXT_ATTR14', uda.c_ext_attr14,
                                  'C_EXT_ATTR15', uda.c_ext_attr15,
                                  'C_EXT_ATTR16', uda.c_ext_attr16,
                                  'C_EXT_ATTR17', uda.c_ext_attr17,
                                  'C_EXT_ATTR18', uda.c_ext_attr18,
                                  'C_EXT_ATTR19', uda.c_ext_attr19,
                                  'C_EXT_ATTR20', uda.c_ext_attr20,
                                  'C_EXT_ATTR21', uda.c_ext_attr21,
                                  'C_EXT_ATTR22', uda.c_ext_attr22,
                                  'C_EXT_ATTR23', uda.c_ext_attr23,
                                  'C_EXT_ATTR24', uda.c_ext_attr24,
                                  'C_EXT_ATTR25', uda.c_ext_attr25,
                                  'C_EXT_ATTR26', uda.c_ext_attr26,
                                  'C_EXT_ATTR27', uda.c_ext_attr27,
                                  'C_EXT_ATTR28', uda.c_ext_attr28,
                                  'C_EXT_ATTR29', uda.c_ext_attr29,
                                  'C_EXT_ATTR30', uda.c_ext_attr30,
                                  'C_EXT_ATTR31', uda.c_ext_attr31,
                                  'C_EXT_ATTR32', uda.c_ext_attr32,
                                  'C_EXT_ATTR33', uda.c_ext_attr33,
                                  'C_EXT_ATTR34', uda.c_ext_attr34,
                                  'C_EXT_ATTR35', uda.c_ext_attr35,
                                  'C_EXT_ATTR36', uda.c_ext_attr36,
                                  'C_EXT_ATTR37', uda.c_ext_attr37,
                                  'C_EXT_ATTR38', uda.c_ext_attr38,
                                  'C_EXT_ATTR39', uda.c_ext_attr39,
                                  'C_EXT_ATTR40', uda.c_ext_attr40
                                 )
                    ) AS attribute_char_value,
             (DECODE (agc.data_type_code,
                      'N', DECODE (agc.database_column,
                                   'N_EXT_ATTR1', uda.n_ext_attr1,
                                   'N_EXT_ATTR2', uda.n_ext_attr2,
                                   'N_EXT_ATTR3', uda.n_ext_attr3,
                                   'N_EXT_ATTR4', uda.n_ext_attr4,
                                   'N_EXT_ATTR5', uda.n_ext_attr5,
                                   'N_EXT_ATTR6', uda.n_ext_attr6,
                                   'N_EXT_ATTR7', uda.n_ext_attr7,
                                   'N_EXT_ATTR8', uda.n_ext_attr8,
                                   'N_EXT_ATTR9', uda.n_ext_attr9,
                                   'N_EXT_ATTR10', uda.n_ext_attr10,
                                   'N_EXT_ATTR11', uda.n_ext_attr11,
                                   'N_EXT_ATTR12', uda.n_ext_attr12,
                                   'N_EXT_ATTR13', uda.n_ext_attr13,
                                   'N_EXT_ATTR14', uda.n_ext_attr14,
                                   'N_EXT_ATTR15', uda.n_ext_attr15,
                                   'N_EXT_ATTR16', uda.n_ext_attr16,
                                   'N_EXT_ATTR17', uda.n_ext_attr17,
                                   'N_EXT_ATTR18', uda.n_ext_attr18,
                                   'N_EXT_ATTR19', uda.n_ext_attr19,
                                   'N_EXT_ATTR20', uda.n_ext_attr20
                                  )
                     )
             ) AS attribute_number_value,
             DECODE (agc.data_type_code,
                     'X', DECODE (agc.database_column,
                                  'D_EXT_ATTR1', uda.d_ext_attr1,
                                  'D_EXT_ATTR2', uda.d_ext_attr2,
                                  'D_EXT_ATTR3', uda.d_ext_attr3,
                                  'D_EXT_ATTR4', uda.d_ext_attr4,
                                  'D_EXT_ATTR5', uda.d_ext_attr5,
                                  'D_EXT_ATTR6', uda.d_ext_attr6,
                                  'D_EXT_ATTR7', uda.d_ext_attr7,
                                  'D_EXT_ATTR8', uda.d_ext_attr8,
                                  'D_EXT_ATTR9', uda.d_ext_attr9,
                                  'D_EXT_ATTR10', uda.d_ext_attr10
                                 )
                    ) AS attribute_date_value,
             DECODE (agc.data_type_code,
                     'Y', DECODE (agc.database_column,
                                  'D_EXT_ATTR1', uda.d_ext_attr1,
                                  'D_EXT_ATTR2', uda.d_ext_attr2,
                                  'D_EXT_ATTR3', uda.d_ext_attr3,
                                  'D_EXT_ATTR4', uda.d_ext_attr4,
                                  'D_EXT_ATTR5', uda.d_ext_attr5,
                                  'D_EXT_ATTR6', uda.d_ext_attr6,
                                  'D_EXT_ATTR7', uda.d_ext_attr7,
                                  'D_EXT_ATTR8', uda.d_ext_attr8,
                                  'D_EXT_ATTR9', uda.d_ext_attr9,
                                  'D_EXT_ATTR10', uda.d_ext_attr10
                                 )
                    ) AS attribute_datetime_value
        FROM ego_attrs_v agc,
             ego_attr_groups_v ag,
             rrs_sites_ext_b uda,
             rrs_sites_b rrs
       WHERE uda.attr_group_id = ag.attr_group_id
         AND agc.application_id = ag.application_id
         AND agc.attr_group_type = ag.attr_group_type
         AND agc.attr_group_name = ag.attr_group_name
         AND ag.attr_group_type = gc_site_group           
         AND agc.attr_group_name IN
                (
                 ---------- Prior Page -----------
                 gc_store_oprtr_grp_prior, --'SLC_SM_STORE_OPERATOR_PRIOR',
                 gc_franchisee_grp_prior,
                 gc_agreement_grp_prior,
                 gc_amendment_grp_prior,
                 'SLC_SM_CONTRACTUAL_ATTR_PRIOR',
                 'SLC_SM_CALCULATION_COND_PRIOR',
                 gc_breach_grp_prior,
                 gc_lon_grp_prior,
                 gc_startstop_grp_prior,
                 gc_rvpr_grp_prior,
				 --Changes for v1.1 
				 --Removing as this Attribute group is not present
                 --'SLC_SM_3_MONTHS_AVG_PRIOR',
                 --'SLC_SM_OVERRIDE_PRIOR',
                 'SLC_SM_ORIGINAL_CONTR_AT_PRIOR',
				 --Added on 08 aug 2017
				 gc_transfers_grp_prior,
				 gc_setl_agrmnt_grp_prior,
				 gc_ebba_grp_prior,
				 gc_mgmt_agrmnt_prior_grp, --'SLC_SM_MANAGEMENT_AGREE_PRIOR',
                 ------------ Active Page -----------
                 gc_store_oprtr_grp_active, --'SLC_SM_STORE_OPERATOR',
                 gc_franchisee_grp_active,
                 gc_agreement_grp_active,
                 gc_amendment_grp_active,
                 gc_contr_attr, --'SLC_SM_CONTRACTUAL_ATTRIBUTES',
                 gc_orig_contr_attr_at, --'SLC_SM_ORIGINAL_CONTRACTUAL_AT',
                 'SLC_SM_CALCULATION_CONDITIONS',
                 gc_templic_grp_active,
                 gc_breach_grp_active,
                 gc_lon_grp_active,
                 gc_startstop_grp_active,
                 gc_rvpr_grp_active,
                 gc_3mon_grp, --'SLC_SM_3_MONTHS_AVG',
                 gc_override_grp, --'SLC_SM_OVERRIDE',
				 --Added on 08 aug 2017
				 gc_transfers_grp,
				 gc_setl_agrmnt_grp_active,
				 gc_ebba_grp_active,
				 'SLC_SM_CURRENT_EBBA_SCHEDULE',
				 gc_mgmt_agrmnt_grp, --'SLC_SM_MANAGEMENT_AGREEMENT',
				 gc_draw_mgmt_grp,   --'SLC_SM_DRAW_MANAGEMENT',
				 --
                 --Pending Page
                 gc_store_oper_grp_pending, --'SLC_SM_STORE_OPERATOR_PEND',
                 gc_franchisee_grp_pending,
                 gc_agreement_grp_pending,
                 gc_amendment_grp_pending,
                 'SLC_SM_CONTRACTUAL_ATTR_PEND',
                 gc_templic_grp_pending,
                 'SLC_SM_CALCULATION_COND_PEND'
                )
         AND rrs.site_id = p_site_id
         AND uda.site_id = rrs.site_id
         AND agc.enabled_flag = 'Y';

   cur_site_row     cur_site%ROWTYPE;
   cur_site_count   NUMBER;
BEGIN
   cur_site_count := 0;

   BEGIN
      SELECT slcapps.slc_sminf_1ahost_site_s.NEXTVAL
        INTO g_batch_id
        FROM DUAL;
   EXCEPTION
      WHEN OTHERS
      THEN
         write_log_p
            (   'slc_sminf_backup_uda_attrs_p: Error while fetching sequence value '
             || SQLERRM
            );
   END;

   write_log_p ('slc_sminf_backup_uda_attrs_p: Intialised global variable g_batch_id :' || g_batch_id);
   FOR i IN cur_site (p_site_id)
   LOOP
      INSERT INTO slcapps.slc_sminf_attr_details_stg
                  (record_id, batch_id,
                   extension_id, site_id, site_use_type_code,
                   attributegroup_id, attribute_group_name,
                   attribute_id, attribute_name, data_type_code,
                   attribute_group_type, attribute_group_app_id,
                   attribute_group_data_level, attribute_char_value,
                   attribute_number_value, attribute_date_value,
                   attribute_datetime_value, status_code
                  )
           VALUES (slcapps.slc_sminf_1ahost_site_s.NEXTVAL, g_batch_id,
                   i.extension_id, i.site_id, i.site_use_type_code,
                   i.attributegroup_id, i.attribute_group_name,
                   i.attribute_id, i.attribute_name, i.data_type_code,
                   i.attribute_group_type, i.attribute_group_app_id,
                   i.attribute_group_data_level, i.attribute_char_value,
                   i.attribute_number_value, i.attribute_date_value,
                   i.attribute_datetime_value, 'N'
                  );

      cur_site_count := cur_site_count + 1;
   END LOOP;
   write_log_p ('slc_sminf_backup_uda_attrs_p: cur_site_count ' || cur_site_count);
   x_return_status := 'S';
   x_msg_count := NULL;
   x_msg_data := gc_success;
EXCEPTION
   WHEN OTHERS
   THEN
      x_return_status := 'E';
      x_msg_count := NULL;
      x_msg_data := 'ERROR';
      write_log_p (   'slc_sminf_backup_uda_attrs_p: In others exception :'
                   || SQLCODE
                   || SQLERRM
                  );
      write_log_p (   'slc_sminf_backup_uda_attrs_p: Error :'
                   || DBMS_UTILITY.format_error_stack ()
                   || '  '
                   || DBMS_UTILITY.format_error_backtrace ()
                  );
END slc_sminf_backup_uda_attrs_p;

   
-------------------------------------------------------------------------------
--   Procedure      : slc_sminf_delete_uda_attrs_p
--   Purpose        : This procedure must be invoked from procedure
--                    slc_sminf_process_sites_p to deletes attributes from all the
--                    Pages for each site
--   Parameters     : p_site_id           IN          NUMBER
--                    p_site_purpose      IN          VARCHAR2
--                    x_return_status     OUT NOCOPY  VARCHAR2
--                    x_msg_count         OUT NOCOPY  NUMBER
--                    x_msg_data          OUT NOCOPY  VARCHAR2
--   Modifications  :
-------------------------------------------------------------------------------
   PROCEDURE slc_sminf_delete_uda_attrs_p (
      p_site_id         IN              NUMBER,
      p_site_purpose    IN              VARCHAR2,
      x_return_status   OUT NOCOPY      VARCHAR2,
      x_msg_count       OUT NOCOPY      NUMBER,
      x_msg_data        OUT NOCOPY      VARCHAR2
   )
   IS
      l_api_version                   NUMBER                        := 1;
      l_object_name                   VARCHAR2 (20)             := 'RRS_SITE';
      l_attributes_row_table          ego_user_attr_row_table
                                                := ego_user_attr_row_table
                                                                          ();
      l_attributes_data_table         ego_user_attr_data_table
                                               := ego_user_attr_data_table
                                                                          ();
      l_attributes_row_table2          ego_user_attr_row_table
                                                := ego_user_attr_row_table
                                                                          ();
      l_attributes_data_table2         ego_user_attr_data_table
                                               := ego_user_attr_data_table
                                                                          ();                                                                          
                                                                          
      l_pk_column_name_value_pairs    ego_col_name_value_pair_array
                                          := ego_col_name_value_pair_array
                                                                          ();
      l_class_code_name_value_pairs   ego_col_name_value_pair_array
                                          := ego_col_name_value_pair_array
                                                                          ();
      l_user_privileges_on_object     ego_varchar_tbl_type          := NULL;
      l_entity_id                     NUMBER                        := NULL;
      l_entity_index                  NUMBER                        := NULL;
      l_entity_code                   VARCHAR2 (1)                  := NULL;
      l_debug_level                   NUMBER                        := 3;
      l_attr_grp_id                   NUMBER                        := 0;
      l_group_app_id                  NUMBER                        := 0;
      l_attr_group_type               VARCHAR2 (100);
      l_attr_group_name               VARCHAR2 (100);
      l_data_level                    VARCHAR2 (100);
      l_error_msg                     VARCHAR2 (4000)               := NULL;
      l_init_error_handler            VARCHAR2 (1)          := fnd_api.g_true;
      l_write_to_concurrent_log       VARCHAR2 (1)          := fnd_api.g_true;
      l_init_fnd_msg_list             VARCHAR2 (1)          := fnd_api.g_true;
      l_log_errors                    VARCHAR2 (1)          := fnd_api.g_true;
      l_add_errors_to_fnd_stack       VARCHAR2 (1)         := fnd_api.g_false;
      l_commit                        VARCHAR2 (1)         := fnd_api.g_false;
      x_failed_row_id_list            VARCHAR2 (255);
      x_errorcode                     NUMBER;
      x_message_list                  error_handler.error_tbl_type;
      l_site_id                       NUMBER;
      l_purpose                       VARCHAR2 (200);
      l_attr_name                     VARCHAR2 (200);
      l_attr_value                    VARCHAR2 (200);
      l_count                         PLS_INTEGER                   := 0;
      l_row_count                     PLS_INTEGER                   := 0;

      CURSOR cur_site (p_site_id NUMBER, p_count NUMBER)
      IS
         SELECT   extension_id, site_id, site_use_type_code,
                  attributegroup_id, attribute_group_type,
                  attribute_group_app_id, attribute_group_data_level,
                  attribute_group_name, attribute_id, attribute_name,
                  data_type_code, attribute_char_value,
                  attribute_number_value, attribute_date_value,
                  attribute_datetime_value,
                  COALESCE (TO_CHAR (attribute_char_value),
                            TO_CHAR (attribute_number_value),
                            TO_CHAR (attribute_date_value),
                            TO_CHAR (attribute_datetime_value)
                           ) VALUE
             FROM SLCAPPS.slc_sminf_attr_details_stg
            WHERE site_id = p_site_id
              AND batch_id = g_batch_id
              AND status_code = 'N'
            AND attribute_name NOT IN 
            CASE attribute_group_name
            WHEN gc_store_oprtr_grp_prior --'SLC_SM_STORE_OPERATOR_PRIOR' 
			THEN gc_status_attr_name --('STATUS')
            WHEN gc_store_oper_grp_pending --'SLC_SM_STORE_OPERATOR_PEND'  
            THEN gc_status_attr_name --('STATUS')
            WHEN gc_store_oprtr_grp_active --'SLC_SM_STORE_OPERATOR'
			THEN gc_status_attr_name --('STATUS')
            ELSE attribute_group_name END
            --
            AND attribute_group_name NOT IN
            CASE TO_CHAR(p_count)
            WHEN '0' THEN TO_CHAR(p_count)
            ELSE gc_amendment_grp_active --'SLC_SM_AMENDMENT'
            END
            --
			--Changes for v1.1  Begin
			--a.	Calculation conditions grp data was not carried f/w
			--e.	Start Stop Fin grp data was not carried f/w		gc_startstop_grp_active
			--
            AND attribute_group_name NOT IN
            CASE TO_CHAR(p_count)
            WHEN '0' THEN TO_CHAR(p_count)
            ELSE 'SLC_SM_CALCULATION_CONDITIONS' 
            END
            --
			--
            AND attribute_group_name NOT IN
            CASE TO_CHAR(p_count)
            WHEN '0' THEN TO_CHAR(p_count)
            ELSE gc_startstop_grp_active--'SLC_SM_START_STOP_FINANCING' 
            END
			--For SLC_SM_TEMP_LICENSE we need not delete all the rows.
			--We need to delete only that row which is not active currently. 
			--Function will return Y it record is Inactive and N if the record is Active. 
			AND 1 = (CASE WHEN (p_count <> 0 AND ATTRIBUTE_GROUP_NAME = GC_TEMPLIC_GRP_ACTIVE --SLC_SM_TEMP_LICENSE
			AND SLC_SMINF_GET_ACTIVE_LIC(EXTENSION_ID) = 'N') 
				THEN 2 ELSE 1  END)
            --
			--Changes for v1.1  End
            AND attribute_group_name NOT IN
            CASE TO_CHAR(p_count)
            WHEN '0' THEN TO_CHAR(p_count)
            ELSE gc_agreement_grp_active --'SLC_SM_AGREEMENT'
            END  
			----------Breach LON Page--------------
            --
            AND attribute_group_name NOT IN
            CASE TO_CHAR(p_count)
            WHEN '0' THEN TO_CHAR(p_count)
            ELSE gc_breach_grp_active --'SLC_SM_BREACH'
            END  
            --            
            AND attribute_group_name NOT IN
            CASE TO_CHAR(p_count)
            WHEN '0' THEN TO_CHAR(p_count)
            ELSE gc_lon_grp_active --'SLC_SM_LON'
            END  
             --    
			 --Added on 08 Aug 17
            --            
            AND attribute_group_name NOT IN
            CASE TO_CHAR(p_count)
            WHEN '0' THEN TO_CHAR(p_count)
            ELSE 'SLC_SM_EBBA' --'SLC_SM_EBBA'
            END 			 
			--
            AND attribute_group_name NOT IN
            CASE TO_CHAR(p_count)
            WHEN '0' THEN TO_CHAR(p_count)
            ELSE 'SLC_SM_SETTLEMENT_AGREEMENT_BR' --'SLC_SM_SETTLEMENT_AGREEMENT_BR'
            END 
			--
            AND attribute_group_name NOT IN
            CASE TO_CHAR(p_count)
            WHEN '0' THEN TO_CHAR(p_count)
            ELSE gc_mgmt_agrmnt_grp --'SLC_SM_MANAGEMENT_AGREEMENT'
            END 			
			--
            AND attribute_group_name NOT IN
            CASE TO_CHAR(p_count)
            WHEN '0' THEN TO_CHAR(p_count)
            ELSE 'SLC_SM_CURRENT_EBBA_SCHEDULE' --'SLC_SM_CURRENT_EBBA_SCHEDULE'
            END 			
			--
            AND attribute_group_name NOT IN
            CASE TO_CHAR(p_count)
            WHEN '0' THEN TO_CHAR(p_count)
            ELSE gc_draw_mgmt_grp --'SLC_SM_DRAW_MANAGEMENT'
            END 			
			--
            AND attribute_group_name NOT IN
            CASE TO_CHAR(p_count)
            WHEN '0' THEN TO_CHAR(p_count)
            ELSE gc_contr_attr --'SLC_SM_CONTRACTUAL_ATTRIBUTES' 
			END  
             --    
            AND attribute_group_name NOT IN
            CASE TO_CHAR(p_count)
            WHEN '0' THEN TO_CHAR(p_count)
            ELSE gc_orig_contr_attr_at --'SLC_SM_ORIGINAL_CONTRACTUAL_AT' 
			END  
            --
            --Added on 08 Aug 17
            AND attribute_group_name NOT IN
            CASE TO_CHAR(p_count)
            WHEN '0' THEN TO_CHAR(p_count)
            ELSE gc_rvpr_grp_active --'SLC_SM_RVPR'
			END

            AND attribute_group_name NOT IN
            CASE TO_CHAR(p_count)
            WHEN '0' THEN TO_CHAR(p_count)
            ELSE gc_3mon_grp --'SLC_SM_3_MONTHS_AVG'
			END

            AND attribute_group_name NOT IN
            CASE TO_CHAR(p_count)
            WHEN '0' THEN TO_CHAR(p_count)
            ELSE gc_override_grp --'SLC_SM_OVERRIDE'
			END		
            			
         ORDER BY attribute_group_name;  
         


      TYPE typ_site_tbl_type IS TABLE OF cur_site%ROWTYPE;

      l_site_tbl                      typ_site_tbl_type
                                                       := typ_site_tbl_type
                                                                           ();
      l_site_index                    NUMBER;
      l_site_count                    NUMBER                        := 0;
      l_agreement_type VARCHAR2(200) DEFAULT NULL;
      l_latest_start_date  VARCHAR2(200) DEFAULT NULL;
      l_pending_chngovr_typ VARCHAR2(200) DEFAULT NULL;
      l_amendment_cnt NUMBER DEFAULT 0;
      l_primary_franchisee VARCHAR2(200);
                    l_attr_grp_name VARCHAR2(200) DEFAULT NULL;
              l_agr_extension_id NUMBER;
              l_row_identifier NUMBER;
              x_return_msg VARCHAR2(4000);
			  
   --Changes for v1.1
   ln_loop_counter		NUMBER := 0;
   lc_debug_data_obj    ego_user_attr_data_obj;  
   lc_debug_row_obj     ego_user_attr_row_obj;   			  
              
   BEGIN

      l_site_id := p_site_id;                                         
      l_purpose := g_site_purpose;                
      l_pk_column_name_value_pairs.EXTEND (1);
      l_pk_column_name_value_pairs (1) :=
                           ego_col_name_value_pair_obj ('SITE_ID', l_site_id);
      l_class_code_name_value_pairs.EXTEND (1);
      l_class_code_name_value_pairs (1) :=
                ego_col_name_value_pair_obj ('SITE_USE_TYPE_CODE', l_purpose);
	
	--Changes for v1.1
	x_return_status := 'S';
	x_return_msg    := NULL;
-----------------------------------------------------------------------------------------------------

--"If the Agreement type ""00"" and edition 01/2004 or 02/2004  on the ""Prior Page"" (latest state)
-- and was still effective , then  set to ""Yes""   
--Note: only for A-G letter codes
                
                --Derive 
               BEGIN
               SELECT agr.extension_id
               INTO l_agr_extension_id
               FROM
                (SELECT arext.extension_id
                    FROM ego_attr_groups_v aegrp,
                         rrs_sites_ext_b arext,
                         rrs_sites_b rrs
                   WHERE aegrp.attr_group_id = arext.attr_group_id
                     AND arext.site_id = rrs.site_id
                     AND aegrp.attr_group_type = gc_site_group --'RRS_SITEMGMT_GROUP'
                     AND aegrp.attr_group_name = gc_agreement_grp_active --'SLC_SM_AGREEMENT'
                     AND aegrp.attr_group_id = arext.attr_group_id
                     AND arext.site_id = rrs.site_id
                     AND rrs.site_id = p_site_id                     
                     order by arext.D_EXT_ATTR3 desc
                     ) agr
                     where rownum=1;
           
              write_log_p('slc_sminf_delete_uda_attrs_p: Derivation ->  l_agr_extension_id: '||l_agr_extension_id);    
              
               EXCEPTION
                  WHEN OTHERS
                  THEN

                     l_agr_extension_id := NULL;
                     write_log_p
                        (   'slc_sminf_delete_uda_attrs_p: in exception handler : OTHERS : Unable to derive extension ID for site ID '
                         || p_site_id
                         || ' '
                         || SQLCODE
                         || ' '
                         || SQLERRM
                        );
               END;
			   
	
   IF l_agr_extension_id IS NOT NULL THEN

   --Derive 
               BEGIN
                   SELECT arext.C_EXT_ATTR1, arext.C_EXT_ATTR4
                    INTO g_agr_agreement_type, g_agr_agr_edition
                    FROM ego_attr_groups_v aegrp,
                         rrs_sites_ext_b arext,
                         rrs_sites_b rrs
                   WHERE aegrp.attr_group_id = arext.attr_group_id
                     AND arext.site_id = rrs.site_id
                     AND aegrp.attr_group_type = gc_site_group --'RRS_SITEMGMT_GROUP'
                     AND aegrp.attr_group_name = gc_agreement_grp_active --'SLC_SM_AGREEMENT'
                     AND aegrp.attr_group_id = arext.attr_group_id
                     AND arext.site_id = rrs.site_id
                     AND rrs.site_id = p_site_id
                     AND arext.extension_id = l_agr_extension_id;
                       
           write_log_p('slc_sminf_delete_uda_attrs_p: Derivation ->  g_agr_agreement_type: '||g_agr_agreement_type); 
           write_log_p('slc_sminf_delete_uda_attrs_p: Derivation ->  g_agr_agr_edition: '||g_agr_agr_edition);
           
               EXCEPTION
                  WHEN OTHERS
                  THEN
                     g_agr_agreement_type := NULL;
					 g_agr_agr_edition    := NULL;
                     write_log_p
                        (   'slc_sminf_delete_uda_attrs_p: in exception handler : OTHERS : Unable to derive Start Date for site ID '
                         || p_site_id
                         || ' '
                         || SQLCODE
                         || ' '
                         || SQLERRM
                        );
               END;
               
   END IF;     
-----------------------------------------------------------------------------------------------------

			BEGIN
			   SELECT arext.C_EXT_ATTR1, arext.C_EXT_ATTR2
				INTO g_agr_agreement_type_pend, g_agr_agr_edition_pend
				FROM ego_attr_groups_v aegrp,
					 rrs_sites_ext_b arext,
					 rrs_sites_b rrs
			   WHERE aegrp.attr_group_id = arext.attr_group_id
				 AND arext.site_id = rrs.site_id
				 AND aegrp.attr_group_type = gc_site_group --'RRS_SITEMGMT_GROUP'
				 AND aegrp.attr_group_name = gc_agreement_grp_pending --'SLC_SM_AGREEMENT_PEND'
				 AND aegrp.attr_group_id = arext.attr_group_id
				 AND arext.site_id = rrs.site_id
				 AND rrs.site_id = p_site_id
				 and rownum=1;
				 --There will be only one agreement coming from Pending to Active Operator
				   
			   write_log_p('slc_sminf_delete_uda_attrs_p: Derivation ->  g_agr_agreement_type_pend: '||g_agr_agreement_type_pend); 
			   write_log_p('slc_sminf_delete_uda_attrs_p: Derivation ->  g_agr_agr_edition_pend: '||g_agr_agr_edition_pend);
	   
              EXCEPTION
			  WHEN OTHERS
			  THEN				  
				 g_agr_agreement_type_pend := NULL;
				 g_agr_agr_edition_pend := NULL;
				 write_log_p
					(   'slc_sminf_delete_uda_attrs_p: in exception handler : OTHERS : Unable to derive Pending agreement type and edition Date for site ID '
					 || p_site_id
					 || ' '
					 || SQLCODE
					 || ' '
					 || SQLERRM
					);
               END;

			   
--------------------------------------------------
--FEIN Logic for Amendments
---------------------------------------------------
--Derive 
              BEGIN
                   SELECT arext.C_EXT_ATTR2
                    INTO l_pending_chngovr_typ
                    FROM ego_attr_groups_v aegrp,
                         rrs_sites_ext_b arext,
                         rrs_sites_b rrs
                   WHERE aegrp.attr_group_id = arext.attr_group_id
                     AND arext.site_id = rrs.site_id
                     AND aegrp.attr_group_type = gc_site_group --'RRS_SITEMGMT_GROUP'
                     AND aegrp.attr_group_name = gc_store_oper_grp_pending --'SLC_SM_STORE_OPERATOR_PEND'
                     AND aegrp.attr_group_id = arext.attr_group_id
                     AND arext.site_id = rrs.site_id
                     AND rrs.site_id = p_site_id;
                     
                     
               EXCEPTION
                  WHEN OTHERS
                  THEN
					--Changes for v1.1
					--Added x_return_status set condition.					  
					 x_return_status := 'E';
					 x_return_msg := x_return_msg || '~' ||'slc_sminf_delete_uda_attrs_p: in exception handler : OTHERS : Unable to derive Pending changeover type for Site ID'
                         || p_site_id
                         || ' '
                         || SQLCODE
                         || ' '
                         || SQLERRM;
                     l_pending_chngovr_typ := NULL;
                     write_log_p
                        (   'slc_sminf_delete_uda_attrs_p: in exception handler : OTHERS : Unable to derive Pending changeover type for Site ID'
                         || p_site_id
                         || ' '
                         || SQLCODE
                         || ' '
                         || SQLERRM
                        );
               END;

           write_log_p('slc_sminf_delete_uda_attrs_p: Derivation -> l_pending_chngovr_typ :' ||l_pending_chngovr_typ);
              
			--Used while defaulting Draw Management. Defaulted only for FF/LE
			g_pending_chngovr_type := l_pending_chngovr_typ; 
			
            IF l_pending_chngovr_typ = gc_franch_to_franch --'Franchisee to Franchisee' 
			THEN 

				BEGIN
				 SELECT COUNT(1)
				 INTO l_amendment_cnt
				FROM fnd_lookup_values
				WHERE lookup_type = gc_amend_fein_lkp --'SLCSM_AMENDMENT_FOR_FEIN_CHNG'
				AND SYSDATE BETWEEN NVL(start_date_active, SYSDATE - 1) AND
				NVL(end_date_active, SYSDATE + 1)
				AND NVL(enabled_flag, 'N') = 'Y'
				AND lookup_code  IN
				  (SELECT arext.C_EXT_ATTR1
				  FROM ego_attr_groups_v aegrp,
					rrs_sites_ext_b arext,
					rrs_sites_b rrs
				  WHERE aegrp.attr_group_id = arext.attr_group_id
				  AND arext.site_id         = rrs.site_id
				  AND aegrp.attr_group_type = gc_site_group --'RRS_SITEMGMT_GROUP'
				  AND aegrp.attr_group_name = gc_amendment_grp_pending --'SLC_SM_AMENDMENT_PEND'
				  AND aegrp.attr_group_id   = arext.attr_group_id
				  AND arext.site_id         = rrs.site_id
				  AND rrs.site_id           = p_site_id
				  );
				 
				  write_log_p('slc_sminf_delete_uda_attrs_p: Derivation -> l_amendment_cnt '||l_amendment_cnt);
  
				   EXCEPTION
					  WHEN OTHERS
					  THEN
					--Changes for v1.1
					--Added x_return_status set condition.					  
					 x_return_status := 'E';	
					 x_return_msg := x_return_msg ||'~' ||'slc_sminf_delete_uda_attrs_p: in exception handler : OTHERS : Unable to derive count of amendments in Active operator for Site ID'
							 || p_site_id
							 || ' '
							 || SQLCODE
							 || ' '
							 || SQLERRM;
						 l_amendment_cnt := NULL;
						 write_log_p
							(   'slc_sminf_delete_uda_attrs_p: in exception handler : OTHERS : Unable to derive count of amendments in Active operator for Site ID'
							 || p_site_id
							 || ' '
							 || SQLCODE
							 || ' '
							 || SQLERRM
							);
				   END;
    
			ELSIF l_pending_chngovr_typ = gc_legal_entity --'Legal Entity' 
			THEN
			l_amendment_cnt := 1; --To retain irrespective of Amendment Number in lookups

			END IF;

----------------------------------------------------------------------------------------------------
--For the "Primary" franchisee => check if the suplier has multiple active stores, if yes then set the flag to "Yes"
--Else "No"
/*
--Commented as per Divya, Multiple Indicator flag is disabled for now. If required, this code needs to be uncommented.
BEGIN
SELECT arext.C_EXT_ATTR5
                    INTO l_primary_franchisee
                    FROM ego_attr_groups_v aegrp,
                         rrs_sites_ext_b arext,
                         rrs_sites_b rrs
                   WHERE aegrp.attr_group_id = arext.attr_group_id
                     AND arext.site_id = rrs.site_id
                     AND aegrp.attr_group_type = gc_site_group --'RRS_SITEMGMT_GROUP'
                     AND aegrp.attr_group_name =  gc_franchisee_grp_active --'SLC_SM_FRANCHISEE_DETAILS'
                     AND aegrp.attr_group_id = arext.attr_group_id
                     AND arext.site_id = rrs.site_id
                     and arext.C_EXT_ATTR3 = gc_primary --'Primary'
                     and rownum = 1 --If more than one Primary franchisee is assigned, consider only one
                     AND rrs.site_id = p_site_id;

  write_log_p('slc_sminf_delete_uda_attrs_p: Derivation -> l_primary_franchisee '||l_primary_franchisee);
  
               EXCEPTION
                  WHEN OTHERS
                  THEN
				--Changes for v1.1
				--Added x_return_status set condition.					  
				 x_return_status := 'E';
				 x_return_msg := x_return_msg || '~' || 'slc_sminf_delete_uda_attrs_p: in exception handler : OTHERS : Unable to derive FRANCHISEE NUMBER in Active operator for Site ID'
                         || p_site_id
                         || ' '
                         || SQLCODE
                         || ' '
                         || SQLERRM;
                     l_primary_franchisee := NULL;
                     write_log_p
                        (   'slc_sminf_delete_uda_attrs_p: in exception handler : OTHERS : Unable to derive FRANCHISEE NUMBER in Active operator for Site ID'
                         || p_site_id
                         || ' '
                         || SQLCODE
                         || ' '
                         || SQLERRM
                        );
END;


IF l_primary_franchisee IS NOT NULL THEN

BEGIN
SELECT COUNT(rrs.site_id)  
                    INTO g_multiple_ind
                    FROM ego_attr_groups_v aegrp,
                         rrs_sites_ext_b arext,
                         rrs_sites_b rrs
                   WHERE aegrp.attr_group_id = arext.attr_group_id
                     AND arext.site_id = rrs.site_id
                     AND aegrp.attr_group_type = gc_site_group --'RRS_SITEMGMT_GROUP'
                     AND aegrp.attr_group_name = gc_franchisee_grp_active --'SLC_SM_FRANCHISEE_DETAILS'
                     AND aegrp.attr_group_id = arext.attr_group_id
                     AND arext.site_id = rrs.site_id
                     and arext.C_EXT_ATTR5 = l_primary_franchisee;

                       write_log_p('slc_sminf_delete_uda_attrs_p: Derivation -> g_multiple_ind '||g_multiple_ind);
  
               EXCEPTION
                  WHEN OTHERS
                  THEN
				--Changes for v1.1
				--Added x_return_status set condition.					  
				 x_return_status := 'E';	
				 x_return_msg := x_return_msg || '~' ||'slc_sminf_delete_uda_attrs_p: in exception handler : OTHERS : Unable to derive Multiple indicator count for l_primary_franchisee'
                         || l_primary_franchisee
                         || ' '
                         || SQLCODE
                         || ' '
                         || SQLERRM;
                     g_multiple_ind := NULL;
                     write_log_p
                        (   'slc_sminf_delete_uda_attrs_p: in exception handler : OTHERS : Unable to derive Multiple indicator count for l_primary_franchisee'
                         || l_primary_franchisee
                         || ' '
                         || SQLCODE
                         || ' '
                         || SQLERRM
                        );
                        
                        END;
END IF;
*/
write_log_p('slc_sminf_delete_uda_attrs_p: After basic derivation x_return_status:'||x_return_status);

----------------------------------------------------------------------------------------------------
	IF x_return_status = 'S' THEN
		BEGIN
		  FOR I IN
		  (SELECT arext.C_EXT_ATTR1 value
			--INTO g_multiple_ind
		  FROM ego_attr_groups_v aegrp,
			rrs_sites_ext_b arext,
			rrs_sites_b rrs
		  WHERE aegrp.attr_group_id = arext.attr_group_id
		  AND arext.site_id         = rrs.site_id
		  AND aegrp.attr_group_type = gc_site_group --'RRS_SITEMGMT_GROUP'
		  AND aegrp.attr_group_name = gc_amendment_grp_active --'SLC_SM_AMENDMENT'
		  AND aegrp.attr_group_id   = arext.attr_group_id
		  AND arext.site_id         = rrs.site_id
		  AND rrs.site_id           = p_site_id
		  AND arext.C_EXT_ATTR1 IN (
		  SELECT arext.C_EXT_ATTR1 
		  FROM ego_attr_groups_v aegrp,
			rrs_sites_ext_b arext,
			rrs_sites_b rrs
		  WHERE aegrp.attr_group_id = arext.attr_group_id
		  AND arext.site_id         = rrs.site_id
		  AND aegrp.attr_group_type = gc_site_group --'RRS_SITEMGMT_GROUP'
		  AND aegrp.attr_group_name = gc_amendment_grp_pending --'SLC_SM_AMENDMENT_PEND'
		  AND aegrp.attr_group_id   = arext.attr_group_id
		  AND arext.site_id         = rrs.site_id
		  AND rrs.site_id           = p_site_id)
		  )
		  LOOP
			write_log_p('slc_sminf_delete_uda_attrs_p: Processing Amendment Number : '|| I.VALUE);
			
			
									write_log_p('slc_sminf_delete_uda_attrs_p: <-- Calling slc_sminf_single_uda_attrs_p -->');

									write_log_p('slc_sminf_delete_uda_attrs_p: Updating End Effective Date to Amendments with changeover date for Amendment : '||I.VALUE);
								   
								   
								   SELECT ego_import_row_seq_s.NEXTVAL
									 INTO l_row_identifier
									 FROM dual;

			  --call procedure slc_populate_row_attr_grp_p to fetch attribute group details
			slc_populate_row_attr_grp_p (p_attribute_group_name   => gc_amendment_grp_active --'SLC_SM_AMENDMENT'
									 ,p_count                 => 1
									 ,p_row_identifier        => l_row_identifier
									 ,p_transaction_type      => gc_sync_mode --'SYNC'
									 ,x_attributes_row_table  => l_attributes_row_table2
									 ,x_status                => x_return_status
									 ,x_error_message         => x_return_msg
									);                             
								   write_log_p
									  (   'slc_sminf_delete_uda_attrs_p: Return status from slc_populate_row_attr_grp_p  : '
									   || x_return_status
									  );
								   write_log_p
									  (   'slc_sminf_delete_uda_attrs_p: Return msg from slc_populate_row_attr_grp_p   : '
									   || x_return_msg
									  );
						
			IF (x_return_status = 'E') THEN
			   x_return_msg := 'Error '||'-'||x_return_msg;
			END IF;                         

			IF(x_return_status = 'S') THEN

				write_log_p('slc_sminf_delete_uda_attrs_p: Populating data for effective end date ');
				l_attributes_data_table2.EXTEND;
				l_attributes_data_table2 (1) :=
								ego_user_attr_data_obj
							( l_row_identifier  -- ROW_IDENTIFIER                     
							 ,gc_form_num_attr_name             --'FORM_NUMBER'                           
							 ,I.VALUE                          -- ATTR_VALUE_STR      
							 ,NULL                          -- ATTR_VALUE_NUM    
							 ,NULL                   -- ATTR_VALUE_DATE   
							 ,NULL                          -- ATTR_DISP_VALUE   
							 ,NULL                          -- ATTR_UNIT_OF_MEASURE                      
							 ,l_row_identifier  -- USER_ROW_IDENTIFIER
							);
				l_attributes_data_table2.EXTEND;
				l_attributes_data_table2 (2) :=
								ego_user_attr_data_obj
							( l_row_identifier   -- ROW_IDENTIFIER                     
							 ,gc_eff_end_attr_name     --,'EFFECTIVE_END_DATE'                                     
							 ,NULL                     -- ATTR_VALUE_STR      
							 ,NULL                           -- ATTR_VALUE_NUM    
							 ,SYSDATE - 1                           -- ATTR_VALUE_DATE   
							 ,NULL                           -- ATTR_DISP_VALUE   
							 ,NULL                           -- ATTR_UNIT_OF_MEASURE                      
							 ,l_row_identifier   -- USER_ROW_IDENTIFIER
							);                    
				   END IF;

				   --Call procedure to call API 
				slc_create_attributes_p (p_site_id                => p_site_id
										,p_attr_group_name        => gc_amendment_grp_active --'SLC_SM_AMENDMENT'
										,p_site_purpose           => g_site_purpose
										,p_attributes_row_table   => l_attributes_row_table2
										,p_attributes_data_table  => l_attributes_data_table2
										,x_return_status          => x_return_status
										,x_msg_count              => x_msg_count
										,x_msg_data               => x_return_msg);

								   write_log_p
									  (   'slc_sminf_delete_uda_attrs_p: Return status from slc_create_attributes_p  : '
									   || x_return_status
									  );
								   write_log_p
									  (   'slc_sminf_delete_uda_attrs_p: Return msg from slc_create_attributes_p   : '
									   || x_return_msg
									  );
									  
				 IF (x_return_status = 'E') THEN
				   x_return_msg := 'Error' ||'-'||x_return_msg;
				END IF; 
				   
		  END LOOP;
		END ;
	END IF;--End of Status Check
----------------------------------------------------------------------------------------------------
	write_log_p('slc_sminf_delete_uda_attrs_p: Before opening Cursor:p_site_id:'||p_site_id||' l_amendment_cnt:'||l_amendment_cnt);
	IF x_return_status = 'S' THEN
		  OPEN cur_site (p_site_id, l_amendment_cnt);

		  LOOP
			 FETCH cur_site
			 BULK COLLECT INTO l_site_tbl;


			 l_site_count := cur_site%ROWCOUNT;

			 IF cur_site%ROWCOUNT = 0
			 THEN
				write_log_p
				   ('slc_sminf_delete_uda_attrs_p: In slc_sminf_delete_uda_attrs_p : No site records are available for processing'
				   );

			 ELSE
				FOR l_site_index IN 1 .. l_site_tbl.COUNT
				LOOP
				   write_log_p ( 'slc_sminf_delete_uda_attrs_p: Deleting row   --> '
								|| l_site_tbl (l_site_index).attribute_group_name
								|| ' Extension Id->'
								|| l_site_tbl (l_site_index).extension_id
								|| '->'
								|| l_site_tbl (l_site_index).attribute_name
								|| '->'
								|| l_site_tbl (l_site_index).VALUE
							   );

				  l_attr_grp_name := NULL;    
				  l_attr_name     := NULL;
				  l_attr_value    := NULL;
				  
				  l_attr_grp_name := l_site_tbl (l_site_index).attribute_group_name;    
				  l_attr_name     := l_site_tbl (l_site_index).attribute_name;
				  l_attr_value    := l_site_tbl (l_site_index).VALUE;
				  
				  IF  l_attr_grp_name = gc_orig_contr_attr_at --'SLC_SM_ORIGINAL_CONTRACTUAL_AT'
				  AND l_attr_name = gc_grnd_split_attr_name --'GRANDFATHERED_SPLIT_50_50' 
				  THEN
				  
					write_log_p('slc_sminf_delete_uda_attrs_p: Deriving GRANDFATHERED_SPLIT_50_50');
				  
					g_grand_50_50   := l_attr_value;
					write_log_p ('slc_sminf_delete_uda_attrs_p: g_grand_50_50 '||g_grand_50_50);
				  END IF;
				  
				  IF l_attr_grp_name = gc_orig_contr_attr_at --'SLC_SM_ORIGINAL_CONTRACTUAL_AT'
				  AND l_attr_name = gc_grnd_exp_dt_attr_name --'GRANDFATHERED_EXP_DT'
				  THEN
				  
					write_log_p('slc_sminf_delete_uda_attrs_p: Deriving   GRANDFATHERED_EXP_DT');
					
					g_grand_exp_date := l_attr_value;
					write_log_p ('slc_sminf_delete_uda_attrs_p: g_grand_exp_date '||g_grand_exp_date);
				  
				  END IF;

				   l_count := l_count + 1;
				   l_attributes_row_table.EXTEND;                          
				   l_attributes_row_table (l_count) :=
					  ego_user_attr_row_obj
							 (l_site_tbl (l_site_index).extension_id
																	-- ROW_IDENTIFIER
					  ,
							  l_site_tbl (l_site_index).attributegroup_id
																		 -- ATTR_GROUP_ID
					  ,
							  l_site_tbl (l_site_index).attribute_group_app_id
																			  -- ATTR_GROUP_APP_ID
					  ,
							  l_site_tbl (l_site_index).attribute_group_type
																			-- ATTR_GROUP_TYPE
					  ,
							  l_attr_grp_name
																			-- ATTR_GROUP_NAME
					  ,
							  l_site_tbl (l_site_index).attribute_group_data_level
																				  -- NDATA_LEVEL
					  ,
							  NULL                                 -- DATA_LEVEL_1
								  ,
							  NULL                                 -- DATA_LEVEL_2
								  ,
							  NULL                                 -- DATA_LEVEL_3
								  ,
							  NULL                                 -- DATA_LEVEL_4
								  ,
							  NULL                                 -- DATA_LEVEL_5
								  ,
							  ego_user_attrs_data_pvt.g_delete_mode       --DELETE
							 );
					
					--ego_user_attrs_data_pub.process_user_attrs_data when ran in Delete mode and when we pass only extension_id
					-- will delete any one record from that attribute group though we pass extension id.
					-- Catch IS: In case of API running in Delete mode it expects attribute value from the attribute group
					-- which is part of Unique Key Attribute.
					-- If those attributes are not provided then it deletes first record from that attribute group.
					-- Thus in case if "Temp License - Active" attribute group since we need to delete specific records we have to 
					-- add attributes of those group.
					IF  l_attr_grp_name = gc_templic_grp_active THEN
						--
						write_log_p ('slc_sminf_delete_uda_attrs_p: l_attr_name '||l_attr_name||' l_attr_value:'||l_attr_value
									||' l_row_count:'||l_row_count);
						l_attributes_data_table.EXTEND;
						l_row_count := l_row_count + 1;
						IF l_attr_name IN ('TEMP_LICENSE_TYPE','FEE_VALUE_PERC_OR_NUMBER','AUDIT_REQUIRED') THEN
							l_attributes_data_table (l_row_count) :=
							ego_user_attr_data_obj
							( l_site_tbl (l_site_index).extension_id  -- ROW_IDENTIFIER
							,l_attr_name       -- ATTR_NAME
							,l_attr_value         -- ATTR_VALUE_STR
							,NULL         -- ATTR_VALUE_NUM
							,NULL           -- ATTR_VALUE_DATE
							,NULL                  -- ATTR_DISP_VALUE
							,NULL                  -- ATTR_UNIT_OF_MEASURE
							,l_site_tbl (l_site_index).extension_id  -- USER_ROW_IDENTIFIER
							);
						ELSIF l_attr_name IN ('FEE') THEN
							l_attributes_data_table (l_row_count) :=
							ego_user_attr_data_obj
							( l_site_tbl (l_site_index).extension_id  -- ROW_IDENTIFIER
							,l_attr_name       -- ATTR_NAME
							,NULL         -- ATTR_VALUE_STR
							,l_attr_value         -- ATTR_VALUE_NUM
							,NULL           -- ATTR_VALUE_DATE
							,NULL                  -- ATTR_DISP_VALUE
							,NULL                  -- ATTR_UNIT_OF_MEASURE
							,l_site_tbl (l_site_index).extension_id  -- USER_ROW_IDENTIFIER
							);						
						ELSIF l_attr_name IN ('AUDIT_DATE','EFFECTIVE_START_DATE','EFFECTIVE_END_DATE') THEN
							l_attributes_data_table (l_row_count) :=
							ego_user_attr_data_obj
							( l_site_tbl (l_site_index).extension_id  -- ROW_IDENTIFIER
							,l_attr_name       -- ATTR_NAME
							,NULL         -- ATTR_VALUE_STR
							,NULL         -- ATTR_VALUE_NUM
							,l_attr_value           -- ATTR_VALUE_DATE
							,NULL                  -- ATTR_DISP_VALUE
							,NULL                  -- ATTR_UNIT_OF_MEASURE
							,l_site_tbl (l_site_index).extension_id  -- USER_ROW_IDENTIFIER
							);							
						END IF;
					END IF;
				END LOOP;
			 END IF;


			 EXIT WHEN cur_site%NOTFOUND;
		  END LOOP;

		  CLOSE cur_site;


		  write_log_p ('slc_sminf_delete_uda_attrs_p: Total Number of rows deleted --> ' || l_count);
  	--Channges for v1.1
	--Loop to debug values with which API's will be called.
	IF l_attributes_row_table.COUNT > 0 THEN
	write_log_p('*************Delete Row Data Object Printing Start*******************');	
	FOR ln_loop_counter IN l_attributes_row_table.FIRST..l_attributes_row_table.LAST
	LOOP
	lc_debug_row_obj := l_attributes_row_table(ln_loop_counter);
		
		write_log_p ('ROW_IDENTIFIER:			'||lc_debug_row_obj.ROW_IDENTIFIER);
		write_log_p ('ATTR_GROUP_ID:			'||lc_debug_row_obj.ATTR_GROUP_ID);
		write_log_p ('ATTR_GROUP_APP_ID:		'||lc_debug_row_obj.ATTR_GROUP_APP_ID);
		write_log_p ('ATTR_GROUP_TYPE:			'||lc_debug_row_obj.ATTR_GROUP_TYPE);
		write_log_p ('ATTR_GROUP_NAME:			'||lc_debug_row_obj.ATTR_GROUP_NAME);
		write_log_p ('TRANSACTION_TYPE:			'||lc_debug_row_obj.TRANSACTION_TYPE);
	END LOOP;
	write_log_p('*************Delete Row Data Object Printing End*******************');
	END IF;
	
	IF l_attributes_data_table.COUNT > 0 THEN
	FOR ln_loop_counter IN l_attributes_data_table.FIRST..l_attributes_data_table.LAST
	LOOP
	lc_debug_data_obj := l_attributes_data_table(ln_loop_counter);
		
		write_log_p ('ROW_IDENTIFIER:		'||lc_debug_data_obj.ROW_IDENTIFIER);
		write_log_p ('ATTR_NAME:			'||lc_debug_data_obj.ATTR_NAME);
		write_log_p ('ATTR_VALUE_STR:		'||lc_debug_data_obj.ATTR_VALUE_STR);
		write_log_p ('ATTR_VALUE_NUM:		'||lc_debug_data_obj.ATTR_VALUE_NUM);
		write_log_p ('ATTR_VALUE_DATE:		'||lc_debug_data_obj.ATTR_VALUE_DATE);
		write_log_p ('ATTR_DISP_VALUE:		'||lc_debug_data_obj.ATTR_DISP_VALUE);
		write_log_p ('USER_ROW_IDENTIFIER:	'||lc_debug_data_obj.USER_ROW_IDENTIFIER);
	END LOOP;
    
	write_log_p('*************Delete Row Row Object Printing End*******************');	 
    END IF;	
	
	
		  IF l_site_count > 0
		  THEN
			 --Initialize the Error Handler to avoid previous error messages to be fetched --
			 error_handler.initialize;
			 ego_user_attrs_data_pub.process_user_attrs_data
									 (l_api_version,
									  l_object_name,
									  l_attributes_row_table,
									  l_attributes_data_table,
									  l_pk_column_name_value_pairs,
									  l_class_code_name_value_pairs,
									  NULL,         --l_user_privileges_on_object,
									  NULL,                         --l_entity_id,
									  NULL,                      --l_entity_index,
									  NULL,                       --l_entity_code,
									  '3',                        --l_debug_level,
									  fnd_api.g_true,      --l_init_error_handler,
									  fnd_api.g_true, --l_write_to_concurrent_log,
									  fnd_api.g_true,       --l_init_fnd_msg_list,
									  fnd_api.g_true,              --l_log_errors,
									  fnd_api.g_false,
									  --l_add_errors_to_fnd_stack,
									  fnd_api.g_false,                 --l_commit,
									  x_failed_row_id_list,
									  x_return_status,
									  x_errorcode,
									  x_msg_count,
									  x_msg_data
									 );
			 write_log_p ('slc_sminf_delete_uda_attrs_p: x_return_status: ' || x_return_status);

			 IF (x_return_status <> fnd_api.g_ret_sts_success)
			 THEN
				error_handler.get_message_list (x_message_list      => x_message_list);

				FOR i IN 1 .. x_message_list.COUNT
				LOOP
				   l_error_msg :=
						  l_error_msg || ' , ' || x_message_list (i).MESSAGE_TEXT;
				   x_msg_data := l_error_msg;
				   write_log_p ('slc_sminf_delete_uda_attrs_p: error: ' || l_error_msg);
				END LOOP;
			 ELSE
				x_return_status := fnd_api.g_ret_sts_success;
				x_msg_data := gc_success;
			 END IF;                                --end of x_return_status 
		  ELSE
			 x_return_status := fnd_api.g_ret_sts_success;
			 x_msg_count := NULL;
			 x_msg_data := gc_success;
		  END IF;
	  
	  END IF;--End of Status Check	  

   EXCEPTION
      WHEN OTHERS
      THEN
         write_log_p (   'slc_sminf_delete_uda_attrs_p: In Other Exceptions slc_sminf_delete_uda_attrs_p:'
                      || SQLCODE
                      || ':'
                      || SQLERRM
                     );
      write_log_p (   'slc_sminf_main_p: Error'
                   || DBMS_UTILITY.format_error_stack ()
                   || '  '
                   || DBMS_UTILITY.format_error_backtrace ()
                  );
         x_return_status := fnd_api.g_ret_sts_error;
         x_msg_data :=
               'In Other Exceptions slc_sminf_delete_uda_attrs_p: '
            || SQLCODE
            || ':'
            || SQLERRM;
   END slc_sminf_delete_uda_attrs_p;

-------------------------------------------------------------------------------
--   Procedure      : slc_sminf_srow_uda_attrs_p
--   Purpose        : This procedure must be invoked from procedure
--                    slc_sminf_process_uda_attrs_p to process all single-row
--                    attributes from all the Pages for each site
--   Parameters     : p_site_id           IN          NUMBER
--                    p_attr_group_name   IN          VARCHAR2
--                    p_attr_name         IN          VARCHAR2
--                    p_attr_value        IN          VARCHAR2
--                    p_batch_id          IN          VARCHAR2
--                    p_site_purpose      IN          VARCHAR2
--                    x_return_status     OUT NOCOPY  VARCHAR2
--                    x_msg_count         OUT NOCOPY  NUMBER
--                    x_msg_data          OUT NOCOPY  VARCHAR2
--   Modifications  :
-------------------------------------------------------------------------------

PROCEDURE slc_sminf_srow_uda_attrs_p (
   p_site_id           IN              NUMBER,
   p_attr_group_name   IN              VARCHAR2,
   p_attr_name         IN              VARCHAR2,
   p_attr_value        IN              VARCHAR2,
   p_batch_id          IN              NUMBER,
   p_site_purpose      IN              VARCHAR2,
   x_return_status     OUT NOCOPY      VARCHAR2,
   x_msg_count         OUT NOCOPY      NUMBER,
   x_msg_data          OUT NOCOPY      VARCHAR2
)
IS
   l_api_version                   NUMBER                        := 1;
   l_object_name                   VARCHAR2 (20)                := 'RRS_SITE';
   l_attributes_row_table          ego_user_attr_row_table
                                                := ego_user_attr_row_table
                                                                          ();
   l_attributes_data_table         ego_user_attr_data_table
                                               := ego_user_attr_data_table
                                                                          ();
   l_pk_column_name_value_pairs    ego_col_name_value_pair_array
                                          := ego_col_name_value_pair_array
                                                                          ();
   l_class_code_name_value_pairs   ego_col_name_value_pair_array
                                          := ego_col_name_value_pair_array
                                                                          ();
   l_user_privileges_on_object     ego_varchar_tbl_type          := NULL;
   l_entity_id                     NUMBER                        := NULL;
   l_entity_index                  NUMBER                        := NULL;
   l_entity_code                   VARCHAR2 (1)                  := NULL;
   l_debug_level                   NUMBER                        := 3;
   l_attr_grp_id                   NUMBER                        := 0;
   l_group_app_id                  NUMBER                        := 0;
   l_attr_group_type               VARCHAR2 (100);
   l_attr_group_name               VARCHAR2 (100);
   l_data_level                    VARCHAR2 (100);
   l_error_msg                     VARCHAR2 (4000)               := NULL;
   l_init_error_handler            VARCHAR2 (1)             := fnd_api.g_true;
   l_write_to_concurrent_log       VARCHAR2 (1)             := fnd_api.g_true;
   l_init_fnd_msg_list             VARCHAR2 (1)             := fnd_api.g_true;
   l_log_errors                    VARCHAR2 (1)             := fnd_api.g_true;
   l_add_errors_to_fnd_stack       VARCHAR2 (1)            := fnd_api.g_false;
   l_commit                        VARCHAR2 (1)            := fnd_api.g_false;
   x_failed_row_id_list            VARCHAR2 (255);
   x_errorcode                     NUMBER;
   x_message_list                  error_handler.error_tbl_type;
   l_site_id                       NUMBER;
   l_purpose                       VARCHAR2 (200);
   l_attr_name                     VARCHAR2 (200);
   l_attr_value                    NUMBER;
   l_count                         PLS_INTEGER                   := 0;
   l_row_count                     PLS_INTEGER                   := 0;
   l_attr_grp_id                   NUMBER                        := 0;
   l_group_app_id                  NUMBER                        := 0;
   l_attr_group_type               VARCHAR2 (100);
   l_attr_group_name               VARCHAR2 (100);
   l_data_level                    VARCHAR2 (100);

   CURSOR cur_site (p_site_id NUMBER, p_attr_group_name VARCHAR2)
   IS
      SELECT extension_id, site_id, site_use_type_code, attribute_group_type,
             attribute_group_app_id, attributegroup_id,
             attribute_group_data_level, attribute_id,
             DECODE (attribute_group_name,
                     
                     --Single-row Attribute Groups (Active to Prior)
                     'SLC_SM_CALCULATION_CONDITIONS', 'SLC_SM_CALCULATION_COND_PRIOR',
                     gc_contr_attr, --'SLC_SM_CONTRACTUAL_ATTRIBUTES',
					 'SLC_SM_CONTRACTUAL_ATTR_PRIOR',
                     gc_store_oprtr_grp_active, --'SLC_SM_STORE_OPERATOR', 
					 gc_store_oprtr_grp_prior, --'SLC_SM_STORE_OPERATOR_PRIOR',
                     gc_orig_contr_attr_at, --'SLC_SM_ORIGINAL_CONTRACTUAL_AT',
					 'SLC_SM_ORIGINAL_CONTR_AT_PRIOR',
                     
                     --Single-row Attribute Groups (Pending to Active)
                     gc_store_oper_grp_pending, --'SLC_SM_STORE_OPERATOR_PEND',
                     gc_store_oprtr_grp_active, --'SLC_SM_STORE_OPERATOR',
                     'SLC_SM_CONTRACTUAL_ATTR_PEND', gc_contr_attr, --'SLC_SM_CONTRACTUAL_ATTRIBUTES',
                     'SLC_SM_CALCULATION_COND_PEND', 'SLC_SM_CALCULATION_CONDITIONS'
                    ) attribute_group_name,
             DECODE (attribute_name,
                     'TARGET_CHANGEOVER_DATE', 'ACTUAL_CHANGEOVER_DATE',
                     'ACTUAL_CHANGEOVER_DATE', gc_eff_start_attr_name, --'EFFECTIVE_START_DATE',
                     attribute_name
                    ) attribute_name,
             attribute_char_value,
                                  --DECODE(attribute_name,'STATUS','Active',
                                  --attribute_char_value) attribute_char_value,
                                  attribute_number_value,
             
             --attribute_date_value,
             -- Target changeover mapped to actual changeover
             DECODE (attribute_name,
                     'TARGET_CHANGEOVER_DATE', SYSDATE,
                     attribute_date_value
                    ) attribute_date_value,
             attribute_datetime_value,
             COALESCE (TO_CHAR (attribute_char_value),
                       TO_CHAR (attribute_number_value),
                       TO_CHAR (attribute_date_value),
                       TO_CHAR (attribute_datetime_value)
                      ) VALUE
        FROM slcapps.slc_sminf_attr_details_stg
       WHERE site_id = p_site_id
         AND COALESCE (TO_CHAR (attribute_char_value),
                       TO_CHAR (attribute_number_value),
                       TO_CHAR (attribute_date_value),
                       TO_CHAR (attribute_datetime_value)
                      ) IS NOT NULL
         AND attribute_group_name IN
                ('SLC_SM_CALCULATION_CONDITIONS',
				 'SLC_SM_CALCULATION_COND_PEND',
                 gc_contr_attr, --'SLC_SM_CONTRACTUAL_ATTRIBUTES', 
				 gc_store_oprtr_grp_active, --'SLC_SM_STORE_OPERATOR',
                 gc_store_oper_grp_pending, --'SLC_SM_STORE_OPERATOR_PEND',
                 'SLC_SM_CONTRACTUAL_ATTR_PEND',
                 
                 gc_orig_contr_attr_at, --'SLC_SM_ORIGINAL_CONTRACTUAL_AT',
                 'SLC_SM_ORIGINAL_CONTR_AT_PRIOR')
         AND status_code = 'N'
         AND batch_id = g_batch_id
         AND attribute_group_name = p_attr_group_name
         AND attribute_name NOT IN
                (
				--Changes for Termination Code exclude. We should populate Termination Code.
				--'TERMINATION_CODE',         --hidden in SLC_SM_STORE_OPERATOR
                                    
                                    --'FDD_EDITION_CHAR', --not in SLC_SM_CONTRACTUAL_ATTRIBUTES
                 gc_mult_indctr_attr_name, --'MULTIPLE_INDICATOR',  --not in SLC_SM_CONTRACTUAL_ATTRIBUTES
                                      'GENESIS_FLAG',
                                     --hidden in SLC_SM_CONTRACTUAL_ATTRIBUTES
                 'ASSIGNED_UPON_RETIREMENT',
                                        --Null (shall be updated by FRC-E-006)
                 'TYPE_OF_STATUS_CHANGE'
                                        --Null (shall be updated by FRC-E-006)
                                        );

   TYPE typ_site_tbl_type IS TABLE OF cur_site%ROWTYPE;

   l_site_tbl                      typ_site_tbl_type   := typ_site_tbl_type
                                                                           ();
   l_site_index                    NUMBER;
   l_site_count                    NUMBER                        := 0;
   l_attr_grp_name                 VARCHAR2 (150)                DEFAULT NULL;
   l_attr_char_value               VARCHAR2 (500)                DEFAULT NULL;
   
   --Changes for v1.1
   ln_loop_counter		NUMBER := 0;
   lc_debug_data_obj    ego_user_attr_data_obj;  
   lc_debug_row_obj     ego_user_attr_row_obj;  

	--Defaulting logic for GAS GALLON , GGPS
	ln_counter			NUMBER DEFAULT 1;
                                                                       
BEGIN

   write_log_p ('slc_sminf_srow_uda_attrs_p: Start of procedure p_attr_group_name:'||p_attr_group_name||' p_site_id:'||p_site_id);
   l_pk_column_name_value_pairs.EXTEND (1);
   l_pk_column_name_value_pairs (1) :=
                           ego_col_name_value_pair_obj ('SITE_ID', p_site_id);
   l_class_code_name_value_pairs.EXTEND (1);
   l_class_code_name_value_pairs (1) :=
           ego_col_name_value_pair_obj ('SITE_USE_TYPE_CODE', p_site_purpose);

   OPEN cur_site (p_site_id, p_attr_group_name);

   LOOP

      FETCH cur_site
      BULK COLLECT INTO l_site_tbl;

      l_site_count := cur_site%ROWCOUNT;

      IF cur_site%ROWCOUNT = 0
      THEN
         write_log_p
            ('In slc_sminf_srow_uda_attrs_p : No site records are available for processing'
            );
      ELSE
	    
         FOR l_site_index IN 1 .. l_site_tbl.COUNT
         LOOP
            write_log_p (   'slc_sminf_srow_uda_attrs_p: Attribute Group   --> '
                         || l_site_tbl (l_site_index).attribute_group_name
                        );
            write_log_p (   'slc_sminf_srow_uda_attrs_p: Attribute Name   --> '
                         || l_site_tbl (l_site_index).attribute_name
                        );
            write_log_p (   'slc_sminf_srow_uda_attrs_p: Value   --> '
                         || l_site_tbl (l_site_index).VALUE
                        );
            l_attr_grp_name := NULL;

            --Pending to Active
            IF     l_site_tbl (l_site_index).attribute_group_name = gc_contr_attr
                                               --'SLC_SM_CONTRACTUAL_ATTRIBUTES'
               AND l_site_tbl (l_site_index).attribute_name IN
                      ('FRANCHISEE_FEE', 'GAS_FEE', 'INVESTMENT',
                       'PREMIUM_AMOUNT_PAID', 'TRANSFER_FEE')
            THEN
               write_log_p
                  ('slc_sminf_srow_uda_attrs_p: Updated SLC_SM_CONTRACTUAL_ATTRIBUTES to SLC_SM_ORIGINAL_CONTRACTUAL_AT'
                  );
               l_attr_grp_name := gc_orig_contr_attr_at; --'SLC_SM_ORIGINAL_CONTRACTUAL_AT';
            END IF;

            l_count := l_count + 1;
            l_attributes_row_table.EXTEND;                             
            l_attributes_row_table (l_count) :=
               ego_user_attr_row_obj
                         (ego_import_row_seq_s.NEXTVAL       -- ROW_IDENTIFIER
                                                      ,
                          l_site_tbl (l_site_index).attributegroup_id,
                          l_site_tbl (l_site_index).attribute_group_app_id,
                          l_site_tbl (l_site_index).attribute_group_type,
                          NVL (l_attr_grp_name,
                               l_site_tbl (l_site_index).attribute_group_name
                              ),
                          l_site_tbl (l_site_index).attribute_group_data_level,
                          NULL                                 -- DATA_LEVEL_1
                              ,
                          NULL                                 -- DATA_LEVEL_2
                              ,
                          NULL                                 -- DATA_LEVEL_3
                              ,
                          NULL                                 -- DATA_LEVEL_4
                              ,
                          NULL                                 -- DATA_LEVEL_5
                              ,
                          gc_sync_mode                                  --SYNC
                         );

            IF l_site_tbl (l_site_index).attribute_group_name = gc_contr_attr
                                               --'SLC_SM_CONTRACTUAL_ATTRIBUTES'
            THEN
               --leave as blank according to mapping
               --Pending to Active only
               IF l_site_tbl (l_site_index).attribute_name = 'RENEWAL_FEE'
               THEN
                  l_site_tbl (l_site_index).attribute_number_value := NULL;
               END IF;
			
			--We need to add this attributes GAS_GALLONS_COMMISSION_RATE , GGPS and AUDIT_ACCRUAL_PROGRAM only once
			--Since this code is written in loop. If we are using dummy attribute name  SEI_HVAC_MAINTENANCE so that we 
			--put data only once for SLC_SM_CONTRACTUAL_ATTRIBUTES.
			IF ln_counter = 1 AND l_site_tbl (l_site_index).attribute_name NOT IN
                      ('FRANCHISEE_FEE', 'GAS_FEE', 'INVESTMENT',
                       'PREMIUM_AMOUNT_PAID', 'TRANSFER_FEE') THEN
					   
			write_log_p('Copying default value for GGPS,Gas Gallon Attribute Name:'||l_site_tbl (l_site_index).attribute_name);
			write_log_p('Copying default value for GGPS,Gas Gallon g_store_letter:'||g_store_letter);
			write_log_p('Copying default value for GGPS,Gas Gallon g_agr_agreement_type_pend:'||g_agr_agreement_type_pend);
			
				--Defaulting values for Attribute group SLC_SM_CONTRACTUAL_ATTRIBUTES and which is not coming in 
				--present in SLC_SM_CONTRACTUAL_ATTR_PEND group
				ln_counter := ln_counter + 1;
				l_row_count := l_row_count + 1;
				l_attributes_data_table.EXTEND;                            
				l_attributes_data_table (l_row_count) :=
				   ego_user_attr_data_obj
							  (ego_import_row_seq_s.CURRVAL      -- ROW_IDENTIFIER
														   ,
							   'GAS_GALLONS_COMMISSION_RATE',
							   '0.015',
							   NULL,
							   NULL,
							   NULL,                             -- ATTR_DISP_VALUE
							   NULL,                        -- ATTR_UNIT_OF_MEASURE
							   l_row_count                  -- USER_ROW_IDENTIFIER
							  );
			
				--No need to default AUDIT_ACCRUAL_PROGRAM flag to Yes in every case. We need to default in case
				--of CORP changeover. Which we are doing at the end of code.
				/*l_row_count := l_row_count + 1;
				l_attributes_data_table.EXTEND;                            
				l_attributes_data_table (l_row_count) :=
				   ego_user_attr_data_obj
							  (ego_import_row_seq_s.CURRVAL      -- ROW_IDENTIFIER
														   ,
							   'AUDIT_ACCRUAL_PROGRAM',
							   'Yes',
							   NULL,
							   NULL,
							   NULL,                             -- ATTR_DISP_VALUE
							   NULL,                        -- ATTR_UNIT_OF_MEASURE
							   l_row_count                  -- USER_ROW_IDENTIFIER
							  );*/			
				END IF;--End of condition check before populating default values.
            END IF;

            write_log_p ('slc_sminf_srow_uda_attrs_p: Step 5');
            l_row_count := l_row_count + 1;
            l_attributes_data_table.EXTEND;                            
            l_attributes_data_table (l_row_count) :=
               ego_user_attr_data_obj
                          (ego_import_row_seq_s.CURRVAL      -- ROW_IDENTIFIER
                                                       ,
                           l_site_tbl (l_site_index).attribute_name,
                           NVL (l_attr_char_value,
                                l_site_tbl (l_site_index).attribute_char_value
                               ),
                           l_site_tbl (l_site_index).attribute_number_value,
                           l_site_tbl (l_site_index).attribute_date_value,
                           NULL                             -- ATTR_DISP_VALUE
                               ,
                           NULL                        -- ATTR_UNIT_OF_MEASURE
                               ,
                           l_row_count                  -- USER_ROW_IDENTIFIER
                          );

------------------------------------------------------------------------------------

            --Add additional column for mapping.
            IF l_site_tbl (l_site_index).attribute_group_name =
                                                 gc_store_oprtr_grp_prior --'SLC_SM_STORE_OPERATOR_PRIOR'
            THEN
               write_log_p
                  ('slc_sminf_srow_uda_attrs_p: <<< Adding EFFECTIVE_END_DATE to SLC_SM_STORE_OPERATOR_PRIOR >>>'
                  );
               l_row_count := l_row_count + 1;
               l_attributes_data_table.EXTEND;                         
               l_attributes_data_table (l_row_count) :=
                  ego_user_attr_data_obj
                               (ego_import_row_seq_s.CURRVAL, -- ROW_IDENTIFIER
                                gc_eff_end_attr_name, --'EFFECTIVE_END_DATE',
                                NULL,
                                NULL,
                                SYSDATE - 1,
                                NULL,                        -- ATTR_DISP_VALUE
                                NULL,                   -- ATTR_UNIT_OF_MEASURE
                                l_row_count             -- USER_ROW_IDENTIFIER
                               );
            END IF;
         END LOOP;
      END IF;

      EXIT WHEN cur_site%NOTFOUND;
   END LOOP;

   CLOSE cur_site;

  	--Channges for v1.1
	--Loop to debug values with which API's will be called.
	
	IF l_attributes_row_table.COUNT > 0 THEN
	write_log_p('*************Single Row Data Object Printing Start*******************');	
	FOR ln_loop_counter IN l_attributes_row_table.FIRST..l_attributes_row_table.LAST
	LOOP
	lc_debug_row_obj := l_attributes_row_table(ln_loop_counter);
		
		write_log_p ('ROW_IDENTIFIER:		'||lc_debug_row_obj.ROW_IDENTIFIER);
		write_log_p ('ATTR_GROUP_ID:			'||lc_debug_row_obj.ATTR_GROUP_ID);
		write_log_p ('ATTR_GROUP_APP_ID:		'||lc_debug_row_obj.ATTR_GROUP_APP_ID);
		write_log_p ('ATTR_GROUP_TYPE:		'||lc_debug_row_obj.ATTR_GROUP_TYPE);
		write_log_p ('ATTR_GROUP_NAME:		'||lc_debug_row_obj.ATTR_GROUP_NAME);
		write_log_p ('TRANSACTION_TYPE:	'||lc_debug_row_obj.TRANSACTION_TYPE);
	END LOOP;
	write_log_p('*************Single Row Data Object Printing End*******************');
	END IF;
	
	--Channges for v1.1
	--Loop to debug values with which API's will be called.
	IF l_attributes_data_table.COUNT > 0 THEN
	write_log_p('*************Single Row Row Object Printing Start*******************');	
	FOR ln_loop_counter IN l_attributes_data_table.FIRST..l_attributes_data_table.LAST
	LOOP
	lc_debug_data_obj := l_attributes_data_table(ln_loop_counter);
		
		write_log_p ('ROW_IDENTIFIER:		'||lc_debug_data_obj.ROW_IDENTIFIER);
		write_log_p ('ATTR_NAME:			'||lc_debug_data_obj.ATTR_NAME);
		write_log_p ('ATTR_VALUE_STR:		'||lc_debug_data_obj.ATTR_VALUE_STR);
		write_log_p ('ATTR_VALUE_DATE:		'||lc_debug_data_obj.ATTR_VALUE_DATE);
		write_log_p ('ATTR_DISP_VALUE:		'||lc_debug_data_obj.ATTR_DISP_VALUE);
		write_log_p ('USER_ROW_IDENTIFIER:	'||lc_debug_data_obj.USER_ROW_IDENTIFIER);
	END LOOP;
	write_log_p('*************Single Row Row Object Printing End*******************');	   
    END IF;
	
   IF l_site_count > 0
   THEN
      --Initialize the Error Handler to avoid previous error messages to be fetched --
      error_handler.initialize;
      ego_user_attrs_data_pub.process_user_attrs_data
                                 (l_api_version,
                                  l_object_name,
                                  l_attributes_row_table,
                                  l_attributes_data_table,
                                  l_pk_column_name_value_pairs,
                                  l_class_code_name_value_pairs,
                                  NULL,         --l_user_privileges_on_object,
                                  NULL,                         --l_entity_id,
                                  NULL,                      --l_entity_index,
                                  NULL,                       --l_entity_code,
                                  '3',                        --l_debug_level,
                                  fnd_api.g_true,      --l_init_error_handler,
                                  fnd_api.g_true, --l_write_to_concurrent_log,
                                  fnd_api.g_true,       --l_init_fnd_msg_list,
                                  fnd_api.g_true,              --l_log_errors,
                                  fnd_api.g_false,
                                  --l_add_errors_to_fnd_stack,
                                  fnd_api.g_false,
                                  x_failed_row_id_list,
                                  x_return_status,
                                  x_errorcode,
                                  x_msg_count,
                                  x_msg_data
                                 );
      write_log_p ('slc_sminf_srow_uda_attrs_p: x_return_status: ' || x_return_status);
      write_log_p ('slc_sminf_srow_uda_attrs_p: Step 8');
      write_log_p ('slc_sminf_srow_uda_attrs_p: Step 9');

      IF (x_return_status <> fnd_api.g_ret_sts_success)
      THEN
         write_log_p ('slc_sminf_srow_uda_attrs_p: Step 10');
         error_handler.get_message_list (x_message_list => x_message_list);

         FOR i IN 1 .. x_message_list.COUNT
         LOOP
            write_log_p ('slc_sminf_srow_uda_attrs_p: Step 10-1');
            l_error_msg :=
                      l_error_msg || ' , ' || x_message_list (i).MESSAGE_TEXT;
            write_log_p ('slc_sminf_srow_uda_attrs_p: error: ' || l_error_msg);
         END LOOP;
      ELSE
         write_log_p ('slc_sminf_srow_uda_attrs_p: Step 11');
         x_return_status := fnd_api.g_ret_sts_success;
         x_msg_data := 'SUCCESS';
      END IF;
   ELSE
      x_return_status := 'S';
      x_msg_data :=
             'ERROR : No records inserted into API table type for processing';
   END IF;

   write_log_p ('slc_sminf_srow_uda_attrs_p: Step 12');
   write_log_p ('slc_sminf_srow_uda_attrs_p: End of procedure slc_sminf_srow_uda_attrs_p');
EXCEPTION
   WHEN OTHERS
   THEN
      write_log_p (   'slc_sminf_srow_uda_attrs_p: Step 13 ERROR slc_sminf_srow_uda_attrs_p'
                   || 'In Other Exceptions slc_sminf_srow_uda_attrs_p:'
                   || SQLCODE
                   || ':'
                   || SQLERRM
                  );
      write_log_p (   'slc_sminf_srow_uda_attrs_p: Error :'
                   || DBMS_UTILITY.format_error_stack ()
                   || '  '
                   || DBMS_UTILITY.format_error_backtrace ()
                  );
      x_return_status := fnd_api.g_ret_sts_error;
      x_msg_data :=
            'In Other Exceptions slc_sminf_srow_uda_attrs_p:'
         || SQLCODE
         || ':'
         || SQLERRM;
END slc_sminf_srow_uda_attrs_p;

-------------------------------------------------------------------------------
--   Procedure      : slc_sminf_process_uda_attrs_p
--   Purpose        : This procedure must be invoked from procedure
--                    slc_sminf_process_sites_p to call single-row, multi-row
--                    procedures
--   Parameters     : p_site_id           IN          NUMBER
--                    p_site_purpose      IN          VARCHAR2
--                    p_batch_id          IN          NUMBER
--                    x_return_status     OUT NOCOPY  VARCHAR2
--                    x_msg_count         OUT NOCOPY  NUMBER
--                    x_msg_data          OUT NOCOPY  VARCHAR2
--   Modifications  :
-------------------------------------------------------------------------------
   PROCEDURE slc_sminf_process_uda_attrs_p (
      p_site_id         IN              NUMBER,
      p_site_purpose    IN              VARCHAR2,
      p_batch_id        IN              NUMBER,
      x_return_status   OUT NOCOPY      VARCHAR2,
      x_msg_count       OUT NOCOPY      NUMBER,
      x_msg_data        OUT NOCOPY      VARCHAR2
   )
   IS
      l_api_version                   NUMBER                        := 1;
      l_object_name                   VARCHAR2 (20)             := 'RRS_SITE';
      l_attributes_row_table          ego_user_attr_row_table
                                                := ego_user_attr_row_table
                                                                          ();
      l_attributes_data_table         ego_user_attr_data_table
                                               := ego_user_attr_data_table
                                                                          ();
      l_pk_column_name_value_pairs    ego_col_name_value_pair_array
                                          := ego_col_name_value_pair_array
                                                                          ();
      l_class_code_name_value_pairs   ego_col_name_value_pair_array
                                          := ego_col_name_value_pair_array
                                                                          ();
      l_user_privileges_on_object     ego_varchar_tbl_type          := NULL;
      l_entity_id                     NUMBER                        := NULL;
      l_entity_index                  NUMBER                        := NULL;
      l_entity_code                   VARCHAR2 (1)                  := NULL;
      l_debug_level                   NUMBER                        := 3;
      l_attr_grp_id                   NUMBER                        := 0;
      l_group_app_id                  NUMBER                        := 0;
      l_attr_group_type               VARCHAR2 (100);
      l_attr_group_name               VARCHAR2 (100);
      l_data_level                    VARCHAR2 (100);
      l_error_msg                     VARCHAR2 (4000)               := NULL;
      l_init_error_handler            VARCHAR2 (1)          := fnd_api.g_true;
      l_write_to_concurrent_log       VARCHAR2 (1)          := fnd_api.g_true;
      l_init_fnd_msg_list             VARCHAR2 (1)          := fnd_api.g_true;
      l_log_errors                    VARCHAR2 (1)          := fnd_api.g_true;
      l_add_errors_to_fnd_stack       VARCHAR2 (1)         := fnd_api.g_false;
      l_commit                        VARCHAR2 (1)         := fnd_api.g_false;
      x_failed_row_id_list            VARCHAR2 (255);
      x_errorcode                     NUMBER;
      x_message_list                  error_handler.error_tbl_type;
      lc_purpose                      VARCHAR2 (200);
      l_attr_name                     VARCHAR2 (200);
      l_attr_value                    NUMBER;
      l_count                         PLS_INTEGER                   := 0;
      l_row_count                     PLS_INTEGER                   := 0;

      CURSOR cur_site_multirow_active (p_site_id NUMBER)
      IS
         SELECT DISTINCT attribute_group_name, extension_id
                    --Unique for each row. Process by each row
         FROM            SLCAPPS.slc_sminf_attr_details_stg
                   WHERE site_id = p_site_id
                     
                     AND attribute_group_name IN
                            (
                             --Active to Prior Multi-row
                             gc_agreement_grp_active,
                             gc_franchisee_grp_active,
                             gc_amendment_grp_active,
                             gc_breach_grp_active,
                             gc_lon_grp_active,
                             gc_startstop_grp_active,
                              gc_rvpr_grp_active,  --remove
							  --Added on 08 Aug 2017
							  gc_transfers_grp,
							  gc_setl_agrmnt_grp_active,
							  gc_ebba_grp_active,
							  gc_mgmt_agrmnt_grp, --'SLC_SM_MANAGEMENT_AGREEMENT',
							  --'SLC_SM_CURRENT_EBBA_SCHEDULE', not required. only delete operation
							  --SLC_SM_DRAW_MANAGEMENT not required. only delete operation
							  --
                              --Pending to Active Multi-row
                             gc_franchisee_grp_pending,
                             gc_agreement_grp_pending,
                             gc_amendment_grp_pending,
                             gc_templic_grp_pending
                            )
                     AND status_code = 'N'
                     AND batch_id = g_batch_id
                ORDER BY attribute_group_name;

      TYPE typ_site_mr_active_tbl_type IS TABLE OF cur_site_multirow_active%ROWTYPE;

      l_site_mr_active_tbl            typ_site_mr_active_tbl_type
                                             := typ_site_mr_active_tbl_type
                                                                           ();
      l_site_mr_active_index          NUMBER;
      l_site_mr_active_count          NUMBER                        := 0;

--------------------------------------------------------------------------------
      CURSOR cur_site_srow_active (p_site_id NUMBER)
      IS
         SELECT DISTINCT attribute_group_name
                    FROM SLCAPPS.slc_sminf_attr_details_stg
                   WHERE site_id = p_site_id
                     AND status_code = 'N'
                     AND batch_id = g_batch_id
                   
                     AND attribute_group_name IN
                            (
                             --Active to Prior SingleRow
                             'SLC_SM_CALCULATION_CONDITIONS',
                             gc_contr_attr, --'SLC_SM_CONTRACTUAL_ATTRIBUTES',
                             gc_store_oprtr_grp_active, --'SLC_SM_STORE_OPERATOR',
                             gc_orig_contr_attr_at, --'SLC_SM_ORIGINAL_CONTRACTUAL_AT',
                             
                             --Pending to Active SingleRow
                             gc_store_oper_grp_pending, --'SLC_SM_STORE_OPERATOR_PEND',
                             'SLC_SM_CONTRACTUAL_ATTR_PEND'
                             ,'SLC_SM_CALCULATION_COND_PEND')
                ORDER BY attribute_group_name;

      TYPE typ_site_sr_active_tbl_type IS TABLE OF cur_site_srow_active%ROWTYPE;

      l_site_sr_active_tbl            typ_site_sr_active_tbl_type
                                             := typ_site_sr_active_tbl_type
                                                                           ();
      l_site_sr_active_index          NUMBER;
      l_site_sr_active_count          NUMBER                        := 0;
      l_return_status                 VARCHAR2 (200);                 
      l_msg_count                     VARCHAR2 (200);                  
      l_msg_data                      VARCHAR2 (200);                  
      l_process_srow_uda_status       VARCHAR2 (1)                 DEFAULT 'S';
      l_process_mrow_uda_status       VARCHAR2 (1)                 DEFAULT 'S';
   BEGIN
      OPEN cur_site_multirow_active (p_site_id);

      LOOP
         FETCH cur_site_multirow_active
         BULK COLLECT INTO l_site_mr_active_tbl;

         IF cur_site_multirow_active%ROWCOUNT = 0
         THEN
            write_log_p
               ('slc_sminf_process_uda_attrs_p: No site records are available for processing - cur_site_multirow_active'
               );
            l_return_status := 'S';           --Continue to process single-row
         ELSE
            FOR l_site_mr_active_index IN 1 .. l_site_mr_active_tbl.COUNT
            LOOP
               IF l_process_mrow_uda_status <> 'E'
               THEN
                  write_log_p
                     ('slc_sminf_process_uda_attrs_p: ++++++++++++++++Start of Multi-row processing++++++++++++++++++'
                     );
                  write_log_p ('slc_sminf_process_uda_attrs_p: Processing Site ID  --> ' || p_site_id);
                  write_log_p
                     (   'slc_sminf_process_uda_attrs_p: Migrating values from Attribute Group(Multi-row)  --> '
                      || l_site_mr_active_tbl (l_site_mr_active_index).attribute_group_name
                     );
                  slc_sminf_multirow_uda_attrs_p
                     (p_site_id              => p_site_id,
                      p_attr_group_name      => l_site_mr_active_tbl
                                                       (l_site_mr_active_index).attribute_group_name,
                      p_extension_id         => l_site_mr_active_tbl
                                                       (l_site_mr_active_index).extension_id,
                      p_batch_id             => NULL,
                      p_site_purpose         => g_site_purpose,
                      
                      x_return_status        => l_return_status,
                      x_msg_count            => l_msg_count,
                      x_msg_data             => l_msg_data
                     );
                  write_log_p ('slc_sminf_process_uda_attrs_p: Return Status         --> ' || l_return_status);
                  write_log_p ('slc_sminf_process_uda_attrs_p: Return message count  --> ' || l_msg_count);
                  write_log_p ('slc_sminf_process_uda_attrs_p: Return message data   --> ' || l_msg_data);
                  write_log_p
                     ('slc_sminf_process_uda_attrs_p: ++++++++++++++++End of Multi-row processing++++++++++++++++++'
                     );
                  l_process_mrow_uda_status := l_return_status;
               END IF;                      --end of l_process_mrow_uda_status
            END LOOP;
         END IF;

         EXIT WHEN cur_site_multirow_active%NOTFOUND;
      END LOOP;

      CLOSE cur_site_multirow_active;

      x_return_status := l_return_status;
      x_msg_count := l_msg_count;
      x_msg_data := l_msg_data;
      write_log_p ('slc_sminf_process_uda_attrs_p: x_return_status : ' || x_return_status);

      IF x_return_status <> 'E'
      THEN
         OPEN cur_site_srow_active (p_site_id);

         LOOP
            FETCH cur_site_srow_active
            BULK COLLECT INTO l_site_sr_active_tbl;

            l_return_status := 'S';

            IF cur_site_srow_active%ROWCOUNT = 0
            THEN
               write_log_p
                  ('slc_sminf_process_uda_attrs_p: In slc_sminf_process_uda_attrs_p : No site records are available for processing - cur_site_srow_active'
                  );
            ELSE
               FOR l_site_sr_active_index IN 1 .. l_site_sr_active_tbl.COUNT
               LOOP
                  IF l_process_srow_uda_status <> 'E'
                  THEN
                     write_log_p
                        ('slc_sminf_process_uda_attrs_p: ++++++++++++++++Start of Single-row processing++++++++++++++++++'
                        );
                     slc_sminf_srow_uda_attrs_p
                        (p_site_id              => p_site_id,
                         p_attr_group_name      => l_site_sr_active_tbl
                                                       (l_site_sr_active_index).attribute_group_name,
                         p_attr_name            => NULL,
                         p_attr_value           => NULL,
                         p_batch_id             => NULL,
                         p_site_purpose         => g_site_purpose,
                         
                         x_return_status        => l_return_status,
                         x_msg_count            => l_msg_count,
                         x_msg_data             => l_msg_data
                        );
                     write_log_p (   'slc_sminf_process_uda_attrs_p: Return Status         --> '
                                  || l_return_status
                                 );
                     write_log_p ('slc_sminf_process_uda_attrs_p: Return message count  --> ' || l_msg_count);
                     write_log_p ('slc_sminf_process_uda_attrs_p: Return message data   --> ' || l_msg_data);
                     write_log_p
                        ('slc_sminf_process_uda_attrs_p: ++++++++++++++++End of Single-row processing++++++++++++++++++'
                        );
                     l_process_srow_uda_status := l_return_status;
                  END IF;
               END LOOP;
            END IF;

            EXIT WHEN cur_site_srow_active%NOTFOUND;
         END LOOP;

         CLOSE cur_site_srow_active;

         x_return_status := l_return_status;
         x_msg_count := l_msg_count;
         x_msg_data := l_msg_data;
         write_log_p ('slc_sminf_process_uda_attrs_p: x_return_status : ' || x_return_status);
      END IF;                                   --End of x_return_status
   EXCEPTION
      WHEN OTHERS
      THEN
         write_log_p (   'slc_sminf_process_uda_attrs_p: In Other Exceptions: slc_sminf_process_uda_attrs_p'
                      || SQLCODE
                      || ':'
                      || SQLERRM
                     );
         x_return_status := fnd_api.g_ret_sts_error;
         x_msg_data :=
               'In Other Exceptions: slc_sminf_process_uda_attrs_p'
            || SQLCODE
            || ':'
            || SQLERRM;
   END slc_sminf_process_uda_attrs_p;

-------------------------------------------------------------------------------
--   Procedure      : slc_sminf_purge_stg_p
--   Purpose        : This procedure must be invoked from procedure
--                    slc_sminf_process_sites_p to purge data from the staging
--                    table if Purge Flag is passed as Y
--   Parameters     : p_purge_flag        IN    NUMBER
--                    p_ret_status        OUT   VARCHAR2
--                    p_ret_msg           OUT   VARCHAR2
--   Modifications  :
-------------------------------------------------------------------------------
   PROCEDURE slc_sminf_purge_stg_p (
      p_purge_flag   IN       VARCHAR2,
      p_ret_status   OUT      VARCHAR2,
      p_ret_msg      OUT      VARCHAR2
   )
   IS
   BEGIN
      IF p_purge_flag = 'Y'
      THEN
         EXECUTE IMMEDIATE 'TRUNCATE TABLE SLCAPPS.slc_sminf_attr_details_stg';
      END IF;

      p_ret_status := 'S';
      p_ret_msg := gc_success;
   EXCEPTION
      WHEN OTHERS
      THEN
         p_ret_status := 'E';
         p_ret_msg :=
            'Error while purging the records from the table : SLCAPPS.slc_sminf_attr_details_stg';
         write_log_p (   'slc_sminf_purge_stg_pIn Other Exceptions: '
                      || SQLCODE
                      || ':'
                      || SQLERRM
                     );
   END slc_sminf_purge_stg_p;

-------------------------------------------------------------------------------
--   Procedure      : slc_sminf_process_sites_p
--   Purpose        : This procedure must be invoked from concurrent program
--                    'SLCSM 1A Host to Site Hub Interface' to :
--                    a. Take backup of all eligible Attribute Groups
--                    b. Delete the values from all attribute groups
--                    c. Assign values in attribute group
--                       i) Active group values should be created in Prior page
--                      ii) Pending group values should be created in Active page
--   Parameters     : p_site_id           IN          NUMBER
--                    p_site_purpose      IN          VARCHAR2
--                    p_batch_id          IN          NUMBER
--                    x_return_status     OUT NOCOPY  VARCHAR2
--                    x_msg_count         OUT NOCOPY  NUMBER
--                    x_msg_data          OUT NOCOPY  VARCHAR2
--   Modifications  :
-------------------------------------------------------------------------------

PROCEDURE slc_sminf_process_sites_p (
   p_errbuf    OUT      VARCHAR2,
   p_retcode   OUT      NUMBER,
   p_purge     IN       VARCHAR2
)
IS
   CURSOR cur_sites
   IS
      SELECT record_id, batch_id, soa_instance_id, site_id, store_number,
             store_letter_code, actual_changeover_date, organization_id,
             attribute1, attribute2, attribute3, attribute4, attribute5,
             attribute6, attribute7, attribute8, attribute9, attribute10,
             source_file_name, error_message, status_code, created_by,
             creation_date, last_update_date, last_update_login,
             last_updated_by, conc_request_id
        FROM slcapps.slc_sminf_1a_host_to_site_stg
       WHERE status_code IN ('F', 'N');

   TYPE l_site_tbl_type IS TABLE OF cur_sites%ROWTYPE;

   l_site_tbl                     l_site_tbl_type       := l_site_tbl_type
                                                                          ();
   l_site_index                   NUMBER;
   l_return_status                VARCHAR2 (2);
   l_return_msg                   VARCHAR2 (4000);
   l_msg_count                    NUMBER;
   l_custom_exception             EXCEPTION;
   l_error_count                  NUMBER;
   x_msg_count                    NUMBER;
   x_msg_data                     VARCHAR2 (4000);
   x_return_status                VARCHAR2 (4000)          := NULL; 
   x_return_msg                   VARCHAR2 (4000)          := NULL;
   l_batch_id                     NUMBER                   DEFAULT -1;
   l_crlf                         VARCHAR2 (2)         := CHR (13)
                                                          || CHR (10);
   l_backup_uda_status            VARCHAR2 (1)             := NULL;
   l_delete_uda_status            VARCHAR2 (1)             := NULL;
   l_process_uda_status           VARCHAR2 (1)             := NULL;
   l_prev_uda_status              VARCHAR2 (1)             := NULL;
   l_purge_stg_status             VARCHAR2 (1)             := NULL;
   l_row_identifier               NUMBER;
   l_attributes_data_table        ego_user_attr_data_table
                                               := ego_user_attr_data_table
                                                                          ();
   l_attributes_row_table         ego_user_attr_row_table
                                                := ego_user_attr_row_table
                                                                          ();
   l_attributes_data_table2       ego_user_attr_data_table
                                               := ego_user_attr_data_table
                                                                          ();
   l_attributes_row_table2        ego_user_attr_row_table
                                                := ego_user_attr_row_table
                                                                          ();

	--Changes for populating GGPS Flag.
   l_attributes_data_table3       ego_user_attr_data_table
                                               := ego_user_attr_data_table
                                                                          ();
   l_attributes_row_table3        ego_user_attr_row_table
                                                := ego_user_attr_row_table
                                                                          ();
																		  
   --Common error logging variable declarations
   l_total_record                 NUMBER                   DEFAULT 0;
   l_total_success_records        NUMBER                   DEFAULT 0;
   l_total_failcust_validation    NUMBER                   DEFAULT 0;
   l_total_errorcust_validation   NUMBER                   DEFAULT 0;
   l_batch_status                 VARCHAR2 (1);
   l_publish_flag                 VARCHAR2 (1);
   l_system_type                  VARCHAR2 (10);
   l_source              CONSTANT VARCHAR2 (10)            := '1A_HOST';
   l_destination         CONSTANT VARCHAR2 (10)            := 'EBS';
   l_cmn_err_status_code          VARCHAR2 (100);
   l_cmn_err_msg                  VARCHAR2 (1000);
   l_business_process_id1         VARCHAR2 (25)            := NULL;
   --Reserved for Parent Record Id
   l_business_process_id2         VARCHAR2 (25)            := NULL;
   --Reserved for Child Record Id
   l_business_process_id3         VARCHAR2 (25)            := NULL;
   l_business_entity_name         VARCHAR2 (50)
                                               := 'SLC_SMINF_PROCESS_SITES_P';
   l_prev_attr_name               VARCHAR2 (150);
   l_prev_attr_value              VARCHAR2 (150);
   l_store_status_attr_name       VARCHAR2 (150);
   l_store_status_attr_value      VARCHAR2 (150);
   l_vendor_id                    ap_suppliers.vendor_id%TYPE;
   l_latest_edition               VARCHAR2 (150);
   l_total_errorcnt               NUMBER                   DEFAULT 0;
   l_request_id                   NUMBER;
   e_apps_not_initialized         EXCEPTION;
   l_unique_batch_id              NUMBER DEFAULT -1;
BEGIN
   write_log_p ('slc_sminf_process_sites_p: Start of procedure:');
   write_log_p ('slc_sminf_process_sites_p: g_login_id          ' || g_login_id);
   write_log_p ('slc_sminf_process_sites_p: g_conc_request_id   ' || g_conc_request_id);
   write_log_p ('slc_sminf_process_sites_p: g_conc_request_date ' || g_conc_request_date);
   write_log_p ('slc_sminf_process_sites_p: g_prog_appl_id      ' || g_prog_appl_id);
   write_log_p ('slc_sminf_process_sites_p: g_conc_program_id   ' || g_conc_program_id);
   write_log_p ('slc_sminf_process_sites_p: -----------Input Parameters -----------');
   write_log_p ('slc_sminf_process_sites_p: Purge :' || p_purge);
   write_log_p ('slc_sminf_process_sites_p: --------------------------- -----------');

   BEGIN
   SELECT ego_import_row_seq_s.NEXTVAL
     INTO   l_unique_batch_id
     FROM DUAL;
   EXCEPTION WHEN OTHERS 
   THEN l_unique_batch_id := 0;
   write_log_p ('Error while generating unique Batch ID :'||SQLERRM);
   END;
   
   write_log_p('Unique Batch ID for this Run: '||l_unique_batch_id);
   
   BEGIN
      SELECT MAX (attribute6)
        INTO l_latest_edition
        FROM fnd_lookup_values
       WHERE lookup_type = gc_form_number_lkp --'SLCOKC_FORM_NUMBER'
         AND attribute1 = gc_trad_agrmnt_lkp  --'Traditional Agreement'
             AND SYSDATE BETWEEN NVL(start_date_active, SYSDATE - 1) AND
                 NVL(end_date_active, SYSDATE + 1)
             AND NVL(enabled_flag, 'N') = 'Y';
			 		 
      write_log_p ('slc_sminf_process_sites_p: Derivation - l_latest_edition ' || l_latest_edition);
   EXCEPTION
      WHEN NO_DATA_FOUND
      THEN
         l_latest_edition := NULL;
         write_log_p
            ('slc_sminf_process_sites_p: Error : No Data Found : no record found in the lookup SLCOKC_FORM_NUMBER while deriving latest Edition'
            );
      WHEN OTHERS
      THEN
         l_latest_edition := NULL;
         write_log_p
            (   'slc_sminf_process_sites_p: Error : Other exception: In the lookup SLCOKC_FORM_NUMBER while deriving latest Edition'
             || SQLERRM
            );
   END;

   write_log_p ('slc_sminf_process_sites_p: Creating savepoint ');
   
   --Changes for v1.1
   --Since this savepoint was not set within loop it is giving error savepoint 
   --'BEFORE_UDA_UPDATE' never established in this session or is invalid
   --SAVEPOINT before_uda_update;

   OPEN cur_sites;

   LOOP
      FETCH cur_sites
      BULK COLLECT INTO l_site_tbl;

	  --Resetting counters within the loop
	  /*
      l_total_errorcnt := 0;             --Initialise it to 0 for each record
      l_total_record := cur_sites%ROWCOUNT;
		*/
		
      --Check if item records are available
      IF cur_sites%ROWCOUNT = 0
      THEN
         write_log_p
            ('slc_sminf_process_sites_p: In slc_sminf_process_sites_p : No site records are available for processing'
            );
         write_output_p
               ('No site records are available for processing during this run');
         l_error_count := 0;
         x_return_status := fnd_api.g_ret_sts_success;
      ELSE
         FOR l_site_index IN 1 .. l_site_tbl.COUNT
         LOOP
		 
			--Changes for v1.1
			SAVEPOINT before_uda_update;

			  --Resetting error flags
			  l_total_errorcnt := 0;             --Initialise it to 0 for each record
			  l_total_record := cur_sites%ROWCOUNT;
	  
            g_store_letter := NULL;
            g_store_number := NULL;
            g_store_letter_code := NULL;
            write_log_p (   'slc_sminf_process_sites_p: l_site_tbl (l_site_index).store_letter_code '
                         || l_site_tbl (l_site_index).store_letter_code
                        );
            g_store_letter_code := l_site_tbl (l_site_index).store_letter_code;
            g_store_number := l_site_tbl (l_site_index).store_letter_code;

            IF UPPER
                    (NVL (SUBSTR (l_site_tbl (l_site_index).store_letter_code,
                                  -1
                                 ),
                          'UNKNOWN'
                         )
                    ) IN ('H', 'J', 'K', 'L', 'M', 'N', 'P','T','X','Z'
					--Changes for v1.1
					--Added the missing store letter codes
					,'Q','R','S','U','V','W','Y'					
					)
            THEN
               write_log_p ('slc_sminf_process_sites_p: Intialising g_store_letter');
               g_store_letter := 'H-Z';
            ELSE
               g_store_letter := 'A-G';
            END IF;

            write_log_p ('slc_sminf_process_sites_p: g_store_letter  :' || g_store_letter);
            write_log_p ('slc_sminf_process_sites_p: g_store_letter_code :' || g_store_letter_code);
            l_batch_id := l_site_tbl (l_site_index).batch_id;
			write_log_p ('Batch ID '||l_batch_id);
			
            l_backup_uda_status := 'S';
            l_delete_uda_status := 'S';
            l_process_uda_status := 'S';
            write_log_p ('slc_sminf_process_sites_p: Site ID : ' || l_site_tbl (l_site_index).site_id);

            IF l_site_tbl (l_site_index).site_id IS NOT NULL
            THEN
               write_log_p ('slc_sminf_process_sites_p: Calling slc_sminf_backup_uda_attrs_p');
               write_log_p
                      ('slc_sminf_process_sites_p: ---------Start : Backup of attribute details--------');

               slc_sminf_backup_uda_attrs_p
                              (p_site_id            => l_site_tbl
                                                                 (l_site_index).site_id,
                               x_return_status      => x_return_status,
                               x_msg_count          => x_msg_count,
                               x_msg_data           => x_return_msg
                              );
               write_log_p
                     (   'slc_sminf_process_sites_p: Return status from slc_sminf_backup_uda_attrs_p  : '
                      || x_return_status
                     );
               write_log_p
                     (   'slc_sminf_process_sites_p: Return count from slc_sminf_backup_uda_attrs_p   : '
                      || x_msg_count
                     );
               write_log_p
                     (   'slc_sminf_process_sites_p: Return message from slc_sminf_backup_uda_attrs_p : '
                      || x_return_msg
                     );
               write_log_p
                         ('slc_sminf_process_sites_p: ---------End : Backup of attribute details--------');
               l_backup_uda_status := x_return_status;

               IF l_backup_uda_status <> 'E'
               THEN
                  write_log_p ('slc_sminf_process_sites_p: Calling slc_sminf_delete_uda_attrs_p');
                  write_log_p
                       ('slc_sminf_process_sites_p: ---------Start : Deleting attribute details--------');
                  slc_sminf_delete_uda_attrs_p
                              (p_site_id            => l_site_tbl
                                                                 (l_site_index).site_id,
                               p_site_purpose       => g_site_purpose,
                               x_return_status      => x_return_status,
                               x_msg_count          => x_msg_count,
                               x_msg_data           => x_return_msg
                              );
                  write_log_p
                     (   'slc_sminf_process_sites_p: Return status from slc_sminf_delete_uda_attrs_p  : '
                      || x_return_status
                     );
                  write_log_p
                     (   'slc_sminf_process_sites_p: Return count from slc_sminf_delete_uda_attrs_p   : '
                      || x_msg_count
                     );
                  write_log_p
                     (   'slc_sminf_process_sites_p: Return message from slc_sminf_delete_uda_attrs_p : '
                      || x_return_msg
                     );
                  write_log_p
                          ('slc_sminf_process_sites_p: ---------End : Deleting attribute details--------');
                  l_delete_uda_status := x_return_status;

                  IF l_delete_uda_status <> 'E'
                  THEN
                     write_log_p ('slc_sminf_process_sites_p: Calling slc_sminf_process_uda_attrs_p');
                     write_log_p
                        ('slc_sminf_process_sites_p: ---------Start : Processing attribute details--------'
                        );

                     slc_sminf_process_uda_attrs_p
                               (p_site_id            => l_site_tbl
                                                                 (l_site_index).site_id,
                                p_site_purpose       => g_site_purpose,
                                p_batch_id           => NULL,          
                                x_return_status      => x_return_status,
                                x_msg_count          => x_msg_count,
                                x_msg_data           => x_return_msg
                               );
                     write_log_p
                        (   'slc_sminf_process_sites_p: Return status from slc_sminf_process_uda_attrs_p  : '
                         || x_return_status
                        );
                     write_log_p
                        (   'slc_sminf_process_sites_p: Return count from slc_sminf_process_uda_attrs_p   : '
                         || x_msg_count
                        );
                     write_log_p
                        (   'slc_sminf_process_sites_p: Return message from slc_sminf_process_uda_attrs_p : '
                         || x_return_msg
                        );
                     write_log_p
                        ('slc_sminf_process_sites_p: ---------End : Processing attribute details--------');
                     l_process_uda_status := x_return_status;
                     write_log_p (   'slc_sminf_process_sites_p: l_backup_uda_status  :'
                                  || l_backup_uda_status
                                 );
                     write_log_p (   'slc_sminf_process_sites_p: l_delete_uda_status  :'
                                  || l_delete_uda_status
                                 );
                     write_log_p (   'slc_sminf_process_sites_p: l_process_uda_status :'
                                  || l_process_uda_status
                                 );
					 
					 write_log_p ('Aks:slc_sminf_process_sites_p:Store Letter Code:'||l_site_tbl (l_site_index).store_letter_code);
                     IF l_process_uda_status <> 'E'
                     THEN
                        write_log_p
                           ('slc_sminf_process_sites_p: Calling slc_sminf_single_uda_attrs_p to update Previous Store letter code'
                           );
                        write_log_p
                           ('slc_sminf_process_sites_p: ---------Start : Processing attribute details--------'
                           );

                        IF UPPER
                              (NVL
                                  (SUBSTR
                                      (l_site_tbl (l_site_index).store_letter_code,
                                       -1
                                      ),
                                   'UNKNOWN'
                                  )
                              ) IN ('A', 'B', 'C', 'D', 'E', 'F', 'G')
                        THEN
                           l_prev_attr_name := gc_fz_store_letr_attr_name; --'FZ_STORE_LETTER_CODE';
                           l_prev_attr_value :=
                                  SUBSTR(l_site_tbl (l_site_index).store_letter_code,-1);
                           write_log_p (   'slc_sminf_process_sites_p: Check if g_agr_agreement_type = '
                                        || g_agr_agreement_type
                                       );
                           write_log_p (   'slc_sminf_process_sites_p: Check if g_agr_agr_edition = '
                                        || g_agr_agr_edition
                                       );

						--c.	GGPS flag should be set to yes if current agreement is GGPS
							IF     g_agr_agreement_type_pend = gc_trad_agrmntggps THEN --'Traditional Agreement - GGPS'
							write_log_p ('Defaulting GGPS in case of A-G case');
							slc_sminf_single_uda_attrs_p
                                    (p_site_id              => l_site_tbl
                                                                  (l_site_index
                                                                  ).site_id,
                                     p_attr_group_name      => 'SLC_SM_CONTRACTUAL_ATTRIBUTES', --'SLC_SM_CONTRACTUAL_ATTRIBUTES',
                                     p_attr_name            => 'GGPS', --'GGPS',
                                     p_attr_char_value      => 'Yes',
                                     p_attr_num_value       => NULL,
                                     p_attr_date_value      => NULL,
                                     p_batch_id             => NULL,   
                                     p_site_purpose         => g_site_purpose,
                                     x_return_status        => x_return_status,
                                     x_msg_count            => x_msg_count,
                                     x_msg_data             => x_return_msg
                                    );										  
							END IF;	
					
                           --Applicable only for Franchisee changeover
					 write_log_p ('Aks:slc_sminf_process_sites_p:g_agr_agreement_type:'||g_agr_agreement_type||
									' g_agr_agr_edition:'||g_agr_agr_edition||' g_grand_50_50:'||g_grand_50_50||
									' g_grand_exp_date:'||to_char(g_grand_exp_date,'DD-MM-YYYY')
									);

                           -- If ACTIVE OPERATOR agreement type is 50/50 and agreement edition is 01 2004 OR 02 2004 then update flags below
                           IF     g_agr_agreement_type =
                                               gc_trad_agrmnt50 --'Traditional Agreement - 50/50'
                              AND g_agr_agr_edition IN (gc_edition_01, gc_edition_02) --'01/2004', '02/2004'
                           THEN
                              IF g_grand_50_50 IS NOT NULL
                              THEN
                                 write_log_p
                                    ('slc_sminf_process_sites_p: <-- Calling slc_sminf_single_uda_attrs_p -->'
                                    );
                                 write_log_p
                                    (   'slc_sminf_process_sites_p: Updating Updating Orig Contr Attr Agreement type to g_grand_50_50 : '
                                     || g_grand_50_50
                                    );
                                 slc_sminf_single_uda_attrs_p
                                    (p_site_id              => l_site_tbl
                                                                  (l_site_index
                                                                  ).site_id,
                                     p_attr_group_name      => gc_orig_contr_attr_at, --'SLC_SM_ORIGINAL_CONTRACTUAL_AT',
                                     p_attr_name            => gc_grnd_split_attr_name, --'GRANDFATHERED_SPLIT_50_50',
                                     p_attr_char_value      => g_grand_50_50,
                                     p_attr_num_value       => NULL,
                                     p_attr_date_value      => NULL,
                                     p_batch_id             => NULL,   
                                     p_site_purpose         => g_site_purpose,
                                     x_return_status        => x_return_status,
                                     x_msg_count            => x_msg_count,
                                     x_msg_data             => x_return_msg
                                    );

                                 write_log_p (   'slc_sminf_process_sites_p: Return status : '
                                              || x_return_status
                                              || 'Return count :'
                                              || x_msg_count
                                              || 'Return message :'
                                              || x_return_msg
                                             );
                                 write_log_p
                                    ('slc_sminf_process_sites_p: <-- End of  slc_sminf_single_uda_attrs_p -->'
                                    );
                                 l_prev_uda_status := x_return_status;
                              END IF;


                              IF     g_grand_exp_date IS NOT NULL
                                 AND g_grand_exp_date > SYSDATE
                              THEN
                                 write_log_p
                                    ('slc_sminf_process_sites_p: <-- Calling slc_sminf_single_uda_attrs_p -->'
                                    );
                                 write_log_p
                                    (   'slc_sminf_process_sites_p: Updating Orig Contr Attr Agreement type to g_grand_exp_date :'
                                     || g_grand_exp_date
                                    );
                                 slc_sminf_single_uda_attrs_p
                                    (p_site_id              => l_site_tbl
                                                                  (l_site_index
                                                                  ).site_id,
                                     p_attr_group_name      => gc_orig_contr_attr_at,    --'SLC_SM_ORIGINAL_CONTRACTUAL_AT',
                                     p_attr_name            => gc_grnd_exp_dt_attr_name, --'GRANDFATHERED_EXP_DT',
                                     p_attr_char_value      => NULL,
                                     p_attr_num_value       => NULL,
                                     p_attr_date_value      => g_grand_exp_date,
                                     p_batch_id             => NULL,   
                                     p_site_purpose         => g_site_purpose,
                                     x_return_status        => x_return_status,
                                     x_msg_count            => x_msg_count,
                                     x_msg_data             => x_return_msg
                                    );
                                 write_log_p (   'slc_sminf_process_sites_p: Return status : '
                                              || x_return_status
                                              || 'Return count :'
                                              || x_msg_count
                                              || 'Return message :'
                                              || x_return_msg
                                             );
                                 write_log_p
                                    ('slc_sminf_process_sites_p: <-- End of  slc_sminf_single_uda_attrs_p -->'
                                    );
                                 l_prev_uda_status := x_return_status;
                              END IF;
                           END IF;
               --end of check g_agr_agreement_type and g_agr_agreement_edition
                        ELSIF UPPER
                                (NVL
                                    (SUBSTR
                                        (l_site_tbl (l_site_index).store_letter_code,
                                         -1
                                        ),
                                     'UNKNOWN'
                                    )
                                --Changes for v1.1
								--Added missing store letter code
                                ) 
								BETWEEN 'H' AND 'Z'
								--IN ('H', 'J', 'K', 'L', 'M', 'N', 'P')
                        THEN
                           l_prev_attr_name := gc_corp_store_attr_name; --'CORP_STORE_LETTER_CODE';
                           l_prev_attr_value :=
                                  SUBSTR(l_site_tbl (l_site_index).store_letter_code,-1);

                           -- If the store letter code is from "H-Z" then
                           -- add dummy supplier number for suuplier "7-Eleven Inc._10001001"
                           SELECT ego_import_row_seq_s.NEXTVAL
                             INTO l_row_identifier
                             FROM DUAL;

                           --call procedure slc_populate_row_attr_grp_p to fetch attribute group details
                           slc_populate_row_attr_grp_p
                              (p_attribute_group_name      => gc_franchisee_grp_active, --'SLC_SM_FRANCHISEE_DETAILS',
                               p_count                     => 1,
                               p_row_identifier            => l_row_identifier,
                               p_transaction_type          => gc_sync_mode, --'SYNC'
                               x_attributes_row_table      => l_attributes_row_table,
                               x_status                    => x_return_status,
                               x_error_message             => x_return_msg
                              );
                           write_log_p
                              (   'slc_sminf_process_sites_p: Return status from slc_populate_row_attr_grp_p  : '
                               || x_return_status
                              );
                           write_log_p
                              (   'slc_sminf_process_sites_p: Return msg from slc_populate_row_attr_grp_p   : '
                               || x_return_msg
                              );

                           IF (x_return_status = 'E')
                           THEN
                              x_return_msg := 'Error ' || '-' || x_return_msg;
                           END IF;

                           IF (x_return_status = 'S')
                           THEN
						   
						   --Added on 09 Aug 2017
						   BEGIN
						   SELECT vendor_id
						      INTO l_vendor_id
                              FROM ap_suppliers
                           WHERE VENDOR_TYPE_LOOKUP_CODE = gc_7elevn_type_lkp_code --'FRANCHISEE'
                           AND ENABLED_FLAG              = 'Y'
                           AND vendor_name_alt           = gc_7elevn_supplier_name; --'7-Eleven Inc.'
						   EXCEPTION WHEN OTHERS THEN
						      l_vendor_id := NULL;
						      write_log_p('slc_sminf_process_sites_p: Unable to derive Vendor ID for vendor name alt : 7-Eleven Inc.');
						   END;
						   
                              write_log_p
                                       ('slc_sminf_process_sites_p: Populating data for Dummy Supplier ');
                              l_attributes_data_table.EXTEND;
                              l_attributes_data_table (1) :=
                                 ego_user_attr_data_obj
                                      (l_row_identifier      -- ROW_IDENTIFIER
                                                       ,
                                       gc_franch_num_attr_name, --'FRANCHISEE_NUM'
                                       gc_7elevn_franch_num     --'10001001'
                                                 ,
                                       NULL                  -- ATTR_VALUE_NUM
                                           ,
                                       NULL                 -- ATTR_VALUE_DATE
                                           ,
                                       NULL                 -- ATTR_DISP_VALUE
                                           ,
                                       NULL            -- ATTR_UNIT_OF_MEASURE
                                           ,
                                       l_row_identifier -- USER_ROW_IDENTIFIER
                                      );
                              l_attributes_data_table.EXTEND;
                              l_attributes_data_table (2) :=
                                 ego_user_attr_data_obj
                                       (l_row_identifier     -- ROW_IDENTIFIER
                                                        ,
                                       gc_franch_name_attr_name -- 'FRANCHISEE_NAME'
                                                         ,
                                        l_vendor_id           -- ATTR_VALUE_STR
                                                 ,
                                        NULL                 -- ATTR_VALUE_NUM
                                            ,
                                        NULL                -- ATTR_VALUE_DATE
                                            ,
                                        NULL                -- ATTR_DISP_VALUE
                                            ,
                                        NULL           -- ATTR_UNIT_OF_MEASURE
                                            ,
                                        l_row_identifier
                                                        -- USER_ROW_IDENTIFIER
                                       );
                              l_attributes_data_table.EXTEND;
                              l_attributes_data_table (3) :=
                                 ego_user_attr_data_obj
                                       (l_row_identifier     -- ROW_IDENTIFIER
                                                        ,
                                        gc_franch_incorp_attr_name --'INCORPORATION'
                                                       ,
                                        'No'                 -- ATTR_VALUE_STR
                                            ,
                                        NULL                 -- ATTR_VALUE_NUM
                                            ,
                                        NULL                -- ATTR_VALUE_DATE
                                            ,
                                        NULL                -- ATTR_DISP_VALUE
                                            ,
                                        NULL           -- ATTR_UNIT_OF_MEASURE
                                            ,
                                        l_row_identifier
                                                        -- USER_ROW_IDENTIFIER
                                       );
                              l_attributes_data_table.EXTEND;
                              l_attributes_data_table (4) :=
                                 ego_user_attr_data_obj
                                       (l_row_identifier     -- ROW_IDENTIFIER
                                                        ,
                                        gc_franch_ownershp_attr_name --'OWNERSHIP_STATUS'
                                                          ,
                                        gc_primary                      --'Primary'
                                                 ,
                                        NULL                 -- ATTR_VALUE_NUM
                                            ,
                                        NULL                -- ATTR_VALUE_DATE
                                            ,
                                        NULL                -- ATTR_DISP_VALUE
                                            ,
                                        NULL           -- ATTR_UNIT_OF_MEASURE
                                            ,
                                        l_row_identifier
                                                        -- USER_ROW_IDENTIFIER
                                       );
                              l_attributes_data_table.EXTEND;
                              l_attributes_data_table (5) :=
                                 ego_user_attr_data_obj
                                       (l_row_identifier     -- ROW_IDENTIFIER
                                                        ,
                                        gc_eff_start_attr_name, --'EFFECTIVE_START_DATE'
                                        NULL                 -- ATTR_VALUE_STR
                                            ,
                                        NULL                 -- ATTR_VALUE_NUM
                                            ,
                                        SYSDATE             -- ATTR_VALUE_DATE
                                               ,
                                        NULL                -- ATTR_DISP_VALUE
                                            ,
                                        NULL           -- ATTR_UNIT_OF_MEASURE
                                            ,
                                        l_row_identifier
                                                        -- USER_ROW_IDENTIFIER
                                       );
                           END IF;

                           --Call procedure to call API
                           slc_create_attributes_p
                              (p_site_id                    => l_site_tbl
                                                                  (l_site_index
                                                                  ).site_id,
                               p_attr_group_name            => gc_franchisee_grp_active, --'SLC_SM_FRANCHISEE_DETAILS',
                               p_site_purpose               => g_site_purpose,
                               p_attributes_row_table       => l_attributes_row_table,
                               p_attributes_data_table      => l_attributes_data_table,
                               x_return_status              => x_return_status,
                               x_msg_count                  => x_msg_count,
                               x_msg_data                   => x_return_msg
                              );
                           write_log_p
                              (   'slc_sminf_process_sites_p: Return status from slc_create_attributes_p  : '
                               || x_return_status
                              );
                           write_log_p
                              (   'slc_sminf_process_sites_p: Return msg from slc_create_attributes_p   : '
                               || x_return_msg
                              );

                           IF (x_return_status = 'E')
                           THEN
                              x_return_msg := 'Error' || '-' || x_return_msg;
                           END IF;

------========================================------------------------------------------

                           -- If the store letter code is from "H-Z" then
                           --If the changeover type is for corporate then default the agreement to GGPS (02)
                           SELECT ego_import_row_seq_s.NEXTVAL
                             INTO l_row_identifier
                             FROM DUAL;

                           --call procedure slc_populate_row_attr_grp_p to fetch attribute group details
                           slc_populate_row_attr_grp_p
                              (p_attribute_group_name      => gc_agreement_grp_active, --'SLC_SM_AGREEMENT',
                               p_count                     => 1,
                               p_row_identifier            => l_row_identifier,
                               p_transaction_type          => gc_sync_mode, --'SYNC'
                               x_attributes_row_table      => l_attributes_row_table2,
                               x_status                    => x_return_status,
                               x_error_message             => x_return_msg
                              );
                           write_log_p
                              (   'slc_sminf_process_sites_p: Return status from slc_populate_row_attr_grp_p  : '
                               || x_return_status
                              );
                           write_log_p
                              (   'slc_sminf_process_sites_p: Return msg from slc_populate_row_attr_grp_p   : '
                               || x_return_msg
                              );

                           IF (x_return_status = 'E')
                           THEN
                              x_return_msg := 'Error ' || '-' || x_return_msg;
                           END IF;

                           IF (x_return_status = 'S')
                           THEN
                              write_log_p
                                       ('slc_sminf_process_sites_p: Populating data for Agreement Type ');
                              l_attributes_data_table2.EXTEND;
                              l_attributes_data_table2 (1) :=
                                 ego_user_attr_data_obj
                                    (l_row_identifier        -- ROW_IDENTIFIER
                                                     ,
                                     gc_doc_type_attr_name --'DOCUMENT_TYPE'    
                                                    ,
                                     gc_trad_agrmntggps --'Traditional Agreement - GGPS'
                                                             -- ATTR_VALUE_STR
                                                                   ,
                                     NULL                    -- ATTR_VALUE_NUM
                                         ,
                                     NULL                   -- ATTR_VALUE_DATE
                                         ,
                                     NULL                   -- ATTR_DISP_VALUE
                                         ,
                                     NULL              -- ATTR_UNIT_OF_MEASURE
                                         ,
                                     l_row_identifier   -- USER_ROW_IDENTIFIER
                                    );
                              l_attributes_data_table2.EXTEND;
                              l_attributes_data_table2 (2) :=
                                 ego_user_attr_data_obj
                                       (l_row_identifier     -- ROW_IDENTIFIER
                                                        ,
                                       gc_edition_char_attr_name -- 'EDITION_CHAR'            -- ATTR_NAME
                                                      ,
                                        l_latest_edition     -- ATTR_VALUE_STR
                                                        ,
                                        NULL                 -- ATTR_VALUE_NUM
                                            ,
                                        NULL                -- ATTR_VALUE_DATE
                                            ,
                                        NULL                -- ATTR_DISP_VALUE
                                            ,
                                        NULL           -- ATTR_UNIT_OF_MEASURE
                                            ,
                                        l_row_identifier
                                                        -- USER_ROW_IDENTIFIER
                                       );
							--Changes for v1.1 Begin
							--Populating Effective Start Date in SLC_SM_AGREEMENT group for H-Z store letter code. 
                              l_attributes_data_table2.EXTEND;
                              l_attributes_data_table2 (3) :=
                                 ego_user_attr_data_obj
                                       (l_row_identifier     -- ROW_IDENTIFIER
                                                        ,
                                       gc_eff_start_attr_name -- 'EFFECTIVE_START_DATE'            -- ATTR_NAME
                                                      ,
                                        NULL     -- ATTR_VALUE_STR
                                                        ,
                                        NULL                 -- ATTR_VALUE_NUM
                                            ,
                                        SYSDATE                -- ATTR_VALUE_DATE
                                            ,
                                        NULL                -- ATTR_DISP_VALUE
                                            ,
                                        NULL           -- ATTR_UNIT_OF_MEASURE
                                            ,
                                        l_row_identifier
                                                        -- USER_ROW_IDENTIFIER
                                       );
                              l_attributes_data_table2.EXTEND;

                              l_attributes_data_table2 (4) :=
                                 ego_user_attr_data_obj
                                       (l_row_identifier     -- ROW_IDENTIFIER
                                                        ,
                                       gc_creat_date_attr_name -- 'CREATION_DATE'            -- ATTR_NAME
                                                      ,
                                        NULL     -- ATTR_VALUE_STR
                                                        ,
                                        NULL                 -- ATTR_VALUE_NUM
                                            ,
                                        SYSDATE                -- ATTR_VALUE_DATE
                                            ,
                                        NULL                -- ATTR_DISP_VALUE
                                            ,
                                        NULL           -- ATTR_UNIT_OF_MEASURE
                                            ,
                                        l_row_identifier
                                                        -- USER_ROW_IDENTIFIER
                                       );
                              l_attributes_data_table2.EXTEND;
                              l_attributes_data_table2 (5) :=
                                 ego_user_attr_data_obj
                                       (l_row_identifier     -- ROW_IDENTIFIER
                                                        ,
                                       gc_signed_date_attr_name -- 'SIGNED_DATE'            -- ATTR_NAME
                                                      ,
                                        NULL     -- ATTR_VALUE_STR
                                                        ,
                                        NULL                 -- ATTR_VALUE_NUM
                                            ,
                                        SYSDATE                -- ATTR_VALUE_DATE
                                            ,
                                        NULL                -- ATTR_DISP_VALUE
                                            ,
                                        NULL           -- ATTR_UNIT_OF_MEASURE
                                            ,
                                        l_row_identifier
                                                        -- USER_ROW_IDENTIFIER
                                       );									   
							--Changes for v1.1 End	   
                           END IF;

                           --Call procedure to call API
                           slc_create_attributes_p
                              (p_site_id                    => l_site_tbl
                                                                  (l_site_index
                                                                  ).site_id,
                               p_attr_group_name            => gc_agreement_grp_active, --'SLC_SM_AGREEMENT',
                               p_site_purpose               => g_site_purpose,
                               p_attributes_row_table       => l_attributes_row_table2,
                               p_attributes_data_table      => l_attributes_data_table2,
                               x_return_status              => x_return_status,
                               x_msg_count                  => x_msg_count,
                               x_msg_data                   => x_return_msg
                              );
                           write_log_p
                              (   'slc_sminf_process_sites_p: Return status from slc_create_attributes_p  : '
                               || x_return_status
                              );
                           write_log_p
                              (   'slc_sminf_process_sites_p: Return msg from slc_create_attributes_p   : '
                               || x_return_msg
                              );

                           IF (x_return_status = 'E')
                           THEN
                              x_return_msg := 'Error' || '-' || x_return_msg;
                           END IF;

------========================================------------------------------------------
                           -- If the store letter code is from "H-Z" then
                           --If the changeover type is for corporate then default GGPS Flag to Yes.
						    write_log_p('creating data for GGPS');
                           SELECT ego_import_row_seq_s.NEXTVAL
                             INTO l_row_identifier
                             FROM DUAL;
							--Satya
                           --call procedure slc_populate_row_attr_grp_p to fetch attribute group details
                           slc_populate_row_attr_grp_p
                              (p_attribute_group_name      => gc_contr_attr, --'SLC_SM_CONTRACTUAL_ATTRIBUTES',
                               p_count                     => 1,
                               p_row_identifier            => l_row_identifier,
                               p_transaction_type          => gc_sync_mode, --'SYNC'
                               x_attributes_row_table      => l_attributes_row_table3,
                               x_status                    => x_return_status,
                               x_error_message             => x_return_msg
                              );	
                           write_log_p
                              (   'slc_sminf_process_sites_p: Return status from slc_populate_row_attr_grp_p  : '
                               || x_return_status
                              );
                           write_log_p
                              (   'slc_sminf_process_sites_p: Return msg from slc_populate_row_attr_grp_p   : '
                               || x_return_msg
                              );
                           IF (x_return_status = 'E')
                           THEN
                              x_return_msg := 'Error ' || '-' || x_return_msg;
                           END IF;

                           IF (x_return_status = 'S')
                           THEN

                              write_log_p
                                       ('slc_sminf_process_sites_p: Populating data for Agreement Type ');
                              l_attributes_data_table3.EXTEND;
                              l_attributes_data_table3 (1) :=
                                 ego_user_attr_data_obj
                                    (l_row_identifier        -- ROW_IDENTIFIER
                                                     ,
                                     'GGPS' --'GGPS'    
                                                    ,
                                     'Yes' --'Traditional Agreement - GGPS'
                                                             -- ATTR_VALUE_STR
                                                                   ,
                                     NULL                    -- ATTR_VALUE_NUM
                                         ,
                                     NULL                   -- ATTR_VALUE_DATE
                                         ,
                                     NULL                   -- ATTR_DISP_VALUE
                                         ,
                                     NULL              -- ATTR_UNIT_OF_MEASURE
                                         ,
                                     l_row_identifier   -- USER_ROW_IDENTIFIER
                                    );
									
									l_attributes_data_table3.EXTEND;
									l_attributes_data_table3 (2) :=
										 ego_user_attr_data_obj
											(l_row_identifier ,       -- ROW_IDENTIFIER
											 'GAS_GALLONS_COMMISSION_RATE', --'GAS_GALLONS_COMMISSION_RATE'    
											 '0.015' 	,-- ATTR_VALUE_STR
											 NULL   ,                 -- ATTR_VALUE_NUM
											 NULL   ,                -- ATTR_VALUE_DATE
											 NULL   ,                -- ATTR_DISP_VALUE
											 NULL   ,           -- ATTR_UNIT_OF_MEASURE
											 l_row_identifier   -- USER_ROW_IDENTIFIER
											);
											
									l_attributes_data_table3.EXTEND;
									l_attributes_data_table3 (3) :=
										 ego_user_attr_data_obj
											(l_row_identifier ,       -- ROW_IDENTIFIER
											 'AUDIT_ACCRUAL_PROGRAM', --'AUDIT_ACCRUAL_PROGRAM'    
											 'Yes' 	,-- ATTR_VALUE_STR
											 NULL   ,                 -- ATTR_VALUE_NUM
											 NULL   ,                -- ATTR_VALUE_DATE
											 NULL   ,                -- ATTR_DISP_VALUE
											 NULL   ,           -- ATTR_UNIT_OF_MEASURE
											 l_row_identifier   -- USER_ROW_IDENTIFIER
											);											
							--Changes for v1.1 End	   
                           END IF;	

                           --Call procedure to call API
                           slc_create_attributes_p
                              (p_site_id                    => l_site_tbl
                                                                  (l_site_index
                                                                  ).site_id,
                               p_attr_group_name            => gc_contr_attr, --'SLC_SM_CONTRACTUAL_ATTRIBUTES',
                               p_site_purpose               => g_site_purpose,
                               p_attributes_row_table       => l_attributes_row_table3,
                               p_attributes_data_table      => l_attributes_data_table3,
                               x_return_status              => x_return_status,
                               x_msg_count                  => x_msg_count,
                               x_msg_data                   => x_return_msg
                              );	
                           write_log_p
                              (   'slc_sminf_process_sites_p: Return status from slc_create_attributes_p  : '
                               || x_return_status
                              );
                           write_log_p
                              (   'slc_sminf_process_sites_p: Return msg from slc_create_attributes_p   : '
                               || x_return_msg
                              );
                           IF (x_return_status = 'E')
                           THEN
                              x_return_msg := 'Error' || '-' || x_return_msg;
                           END IF;							  
							  
---------------------------------------------------------------------------------------------------------------
                        ELSE
                           l_prev_attr_name := gc_corp_store_attr_name; --'CORP_STORE_LETTER_CODE';
                           l_prev_attr_value := NULL;
                        END IF;

---------------------------------------------------------------------------------------------------------------
                        --Assign Pending Status to Pending Page
                        --This is required as Pending page gets erased while deleting all the attributes.
                        
                        IF l_prev_attr_value IS NOT NULL THEN
                        write_log_p
                               ('slc_sminf_process_sites_p: <-- Calling slc_sminf_single_uda_attrs_p -->');
                        write_log_p
                              (   'slc_sminf_process_sites_p: Updating Previous Store letter code to  : '
                               || l_prev_attr_value
                              );

                        slc_sminf_single_uda_attrs_p
                           (p_site_id              => l_site_tbl (l_site_index).site_id,
                            p_attr_group_name      => gc_prev_store_ltr_grp, --'SLC_SM_PREV_STORE_LETTER_CODES',
                            p_attr_name            => l_prev_attr_name,
                            p_attr_char_value      => l_prev_attr_value,
                            p_attr_num_value       => NULL,
                            p_attr_date_value      => NULL,
                            p_batch_id             => NULL,           
                            p_site_purpose         => g_site_purpose,
                            x_return_status        => x_return_status,
                            x_msg_count            => x_msg_count,
                            x_msg_data             => x_return_msg
                           );
                        write_log_p (   'slc_sminf_process_sites_p: Return status : '
                                     || x_return_status
                                     || 'Return count :'
                                     || x_msg_count
                                     || 'Return message :'
                                     || x_return_msg
                                    );
                        write_log_p
                               ('slc_sminf_process_sites_p: <-- End of  slc_sminf_single_uda_attrs_p -->');
                        l_prev_uda_status := x_return_status;
						
						   IF x_return_status = 'E' THEN
						      l_total_errorcnt := l_total_errorcnt + 1;
						   END IF;
                        
                        END IF;
---------------------------------------------------------------------------------------------------------------
                        l_store_status_attr_name := gc_status_attr_name; --'STATUS';
                        l_store_status_attr_value := gc_status_pending; --'Pending';
                        write_log_p
                               ('slc_sminf_process_sites_p: <-- Calling slc_sminf_single_uda_attrs_p -->');
                        write_log_p (   'slc_sminf_process_sites_p: Updating Store status to Pending : '
                                     || l_store_status_attr_value
                                    );
                        slc_sminf_single_uda_attrs_p
                           (p_site_id              => l_site_tbl (l_site_index).site_id,
                            p_attr_group_name      => gc_store_oper_grp_pending, --'SLC_SM_STORE_OPERATOR_PEND',
                            p_attr_name            => l_store_status_attr_name,
                            p_attr_char_value      => l_store_status_attr_value,
                            p_attr_num_value       => NULL,
                            p_attr_date_value      => NULL,
                            p_batch_id             => NULL,            
                            p_site_purpose         => g_site_purpose,
                            x_return_status        => x_return_status,
                            x_msg_count            => x_msg_count,
                            x_msg_data             => x_return_msg
                           );
                        write_log_p (   'slc_sminf_process_sites_p: Return status : '
                                     || x_return_status
                                     || 'Return count :'
                                     || x_msg_count
                                     || 'Return message :'
                                     || x_return_msg
                                    );
                        write_log_p
                               ('slc_sminf_process_sites_p: <-- End of  slc_sminf_single_uda_attrs_p -->');
                        l_prev_uda_status := x_return_status;
						
					      IF x_return_status = 'E' THEN
						      l_total_errorcnt := l_total_errorcnt + 1;
						   END IF;
---------------------------------------------------------------------------------------------------------------
                        l_store_status_attr_name := gc_status_attr_name; --'STATUS';
                        l_store_status_attr_value := 'Active';
                        write_log_p
                               ('slc_sminf_process_sites_p: <-- Calling slc_sminf_single_uda_attrs_p -->');
                        write_log_p (   'slc_sminf_process_sites_p: Updating Store status to Active : '
                                     || l_store_status_attr_value
                                    );

                        slc_sminf_single_uda_attrs_p
                              (p_site_id              => l_site_tbl
                                                                 (l_site_index).site_id,
                               p_attr_group_name      => gc_store_oprtr_grp_active, --'SLC_SM_STORE_OPERATOR',
                               p_attr_name            => l_store_status_attr_name,
                               p_attr_char_value      => l_store_status_attr_value,
                               p_attr_num_value       => NULL,
                               p_attr_date_value      => NULL,
                               p_batch_id             => NULL,         
                               p_site_purpose         => g_site_purpose,
                               x_return_status        => x_return_status,
                               x_msg_count            => x_msg_count,
                               x_msg_data             => x_return_msg
                              );
                        write_log_p (   'slc_sminf_process_sites_p: Return status : '
                                     || x_return_status
                                     || 'Return count :'
                                     || x_msg_count
                                     || 'Return message :'
                                     || x_return_msg
                                    );
                        write_log_p
                               ('slc_sminf_process_sites_p: <-- End of  slc_sminf_single_uda_attrs_p -->');
                        l_prev_uda_status := x_return_status;
						   IF x_return_status = 'E' THEN
						      l_total_errorcnt := l_total_errorcnt + 1;
						   END IF;

						   --Added on Aug 2017
---------------------------------------------------------------------------------------------------------------
                IF g_pending_chngovr_type NOT IN (gc_franch_to_franch, gc_legal_entity) THEN
						
						write_log_p
                               ('slc_sminf_process_sites_p: <-- Calling slc_sminf_single_uda_attrs_p -->');
                        write_log_p (   'slc_sminf_process_sites_p: Updating Draw Managmenet status to WEEKLY_DRAW_PROHIBITED No : '
                                    );

                        slc_sminf_single_uda_attrs_p
                           (p_site_id              => l_site_tbl (l_site_index).site_id,
                            p_attr_group_name      => gc_draw_mgmt_grp, --'SLC_SM_DRAW_MANAGEMENT',
                            p_attr_name            => gc_draw_prohibited_attr_name, --'WEEKLY_DRAW_PROHIBITED',
                            p_attr_char_value      => 'No',
                            p_attr_num_value       => NULL,
                            p_attr_date_value      => NULL,
                            p_batch_id             => NULL,            
                            p_site_purpose         => g_site_purpose,
                            x_return_status        => x_return_status,
                            x_msg_count            => x_msg_count,
                            x_msg_data             => x_return_msg
                           );
                        write_log_p (   'slc_sminf_process_sites_p: Return status : '
                                     || x_return_status
                                     || 'Return count :'
                                     || x_msg_count
                                     || 'Return message :'
                                     || x_return_msg
                                    );
                        write_log_p
                               ('slc_sminf_process_sites_p: <-- End of  slc_sminf_single_uda_attrs_p -->');
                        l_prev_uda_status := x_return_status;
						   IF x_return_status = 'E' THEN
						      l_total_errorcnt := l_total_errorcnt + 1;
						   END IF;
					
---------------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------------
                        write_log_p
                               ('slc_sminf_process_sites_p: <-- Calling slc_sminf_single_uda_attrs_p -->');
                        write_log_p (   'slc_sminf_process_sites_p: Updating Draw Managmenet status to WEEKLY_DRAW_REDUCED No : '
                                    );
                        slc_sminf_single_uda_attrs_p
                           (p_site_id              => l_site_tbl (l_site_index).site_id,
                            p_attr_group_name      => gc_draw_mgmt_grp, --'SLC_SM_DRAW_MANAGEMENT',
                            p_attr_name            => gc_draw_reduced_attr_name, --'WEEKLY_DRAW_REDUCED',
                            p_attr_char_value      => 'No',
                            p_attr_num_value       => NULL,
                            p_attr_date_value      => NULL,
                            p_batch_id             => NULL,            
                            p_site_purpose         => g_site_purpose,
                            x_return_status        => x_return_status,
                            x_msg_count            => x_msg_count,
                            x_msg_data             => x_return_msg
                           );
                        write_log_p (   'slc_sminf_process_sites_p: Return status : '
                                     || x_return_status
                                     || 'Return count :'
                                     || x_msg_count
                                     || 'Return message :'
                                     || x_return_msg
                                    );
                        write_log_p
                               ('slc_sminf_process_sites_p: <-- End of  slc_sminf_single_uda_attrs_p -->');
                        l_prev_uda_status := x_return_status;
						   IF x_return_status = 'E' THEN
						      l_total_errorcnt := l_total_errorcnt + 1;
						   END IF;
			END IF; --end of check for FF LE
---------------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------------
                        l_store_status_attr_name := gc_status_attr_name; --'STATUS';
                        l_store_status_attr_value := gc_status_inactive; --'Inactive';
                        write_log_p
                               ('slc_sminf_process_sites_p: <-- Calling slc_sminf_single_uda_attrs_p -->');
                        write_log_p (   'slc_sminf_process_sites_p: Updating Store status to Inactive : '
                                     || l_store_status_attr_value
                                    );
                        slc_sminf_single_uda_attrs_p
                           (p_site_id              => l_site_tbl (l_site_index).site_id,
                            p_attr_group_name      => gc_store_oprtr_grp_prior, --'SLC_SM_STORE_OPERATOR_PRIOR',
                            p_attr_name            => l_store_status_attr_name,
                            p_attr_char_value      => l_store_status_attr_value,
                            p_attr_num_value       => NULL,
                            p_attr_date_value      => NULL,
                            p_batch_id             => NULL,            
                            p_site_purpose         => g_site_purpose,
                            x_return_status        => x_return_status,
                            x_msg_count            => x_msg_count,
                            x_msg_data             => x_return_msg
                           );
                        
						write_log_p (   'slc_sminf_process_sites_p: Return status : '
                                     || x_return_status
                                     || 'Return count :'
                                     || x_msg_count
                                     || 'Return message :'
                                     || x_return_msg
                                    );
                        write_log_p
                               ('slc_sminf_process_sites_p: <-- End of  slc_sminf_single_uda_attrs_p -->');
                        l_prev_uda_status := x_return_status;
						   IF x_return_status = 'E' THEN
						      l_total_errorcnt := l_total_errorcnt + 1;
						   END IF;
---------------------------------------------------------------------------------------------------------------
                        write_log_p
                                 (   'slc_sminf_process_sites_p: Value of g_agr_agreement_type_pend --> '
                                  || g_agr_agreement_type_pend
                                 );

                        IF g_agr_agreement_type_pend IS NOT NULL
                        THEN
                           write_log_p
                              ('slc_sminf_process_sites_p: <-- Calling slc_sminf_single_uda_attrs_p -->');
                           write_log_p
                              (   'slc_sminf_process_sites_p: Updating Orig Contr Attr Agreement type to g_agr_agreement_type_pend : '
                               || g_agr_agreement_type_pend
                              );
                           slc_sminf_single_uda_attrs_p
                              (p_site_id              => l_site_tbl
                                                                 (l_site_index).site_id,
                               p_attr_group_name      => gc_orig_contr_attr_at, --'SLC_SM_ORIGINAL_CONTRACTUAL_AT',
                               p_attr_name            => gc_agrmnt_type_attr_name, --'AGREEMENT_TYPE',
                               p_attr_char_value      => g_agr_agreement_type_pend,
                               p_attr_num_value       => NULL,
                               p_attr_date_value      => NULL,
                               p_batch_id             => NULL,         
                               p_site_purpose         => g_site_purpose,
                               x_return_status        => x_return_status,
                               x_msg_count            => x_msg_count,
                               x_msg_data             => x_return_msg
                              );
                           write_log_p (   'slc_sminf_process_sites_p: Return status : '
                                        || x_return_status
                                        || 'Return count :'
                                        || x_msg_count
                                        || 'Return message :'
                                        || x_return_msg
                                       );
                           write_log_p
                               ('slc_sminf_process_sites_p: <-- End of  slc_sminf_single_uda_attrs_p -->');
                           l_prev_uda_status := x_return_status;
						 IF x_return_status = 'E' THEN
						      l_total_errorcnt := l_total_errorcnt + 1;
						   END IF;
                        END IF;

---------------------------------------------------------------------------------------------------------------
                        write_log_p (   'slc_sminf_process_sites_p: Value of g_agr_agr_edition_pend --> '
                                     || g_agr_agr_edition_pend
                                    );

                        IF g_agr_agr_edition_pend IS NOT NULL
                        THEN
                           write_log_p
                              ('slc_sminf_process_sites_p: <-- Calling slc_sminf_single_uda_attrs_p -->');
                           write_log_p
                              (   'slc_sminf_process_sites_p: Updating Orig Contr Attr Agreement type to g_agr_agr_edition_pend : '
                               || g_agr_agr_edition_pend
                              );
                           slc_sminf_single_uda_attrs_p
                              (p_site_id              => l_site_tbl
                                                                 (l_site_index).site_id,
                               p_attr_group_name      => gc_orig_contr_attr_at, --'SLC_SM_ORIGINAL_CONTRACTUAL_AT',
                               p_attr_name            => gc_edition_attr_name, --'EDITION',
                               p_attr_char_value      => g_agr_agr_edition_pend,
                               p_attr_num_value       => NULL,
                               p_attr_date_value      => NULL,
                               p_batch_id             => NULL,         
                               p_site_purpose         => g_site_purpose,
                               x_return_status        => x_return_status,
                               x_msg_count            => x_msg_count,
                               x_msg_data             => x_return_msg
                              );
                          
						  write_log_p (   'slc_sminf_process_sites_p: Return status : '
                                        || x_return_status
                                        || 'Return count :'
                                        || x_msg_count
                                        || 'Return message :'
                                        || x_return_msg
                                       );
                           write_log_p
                               ('slc_sminf_process_sites_p: <-- End of  slc_sminf_single_uda_attrs_p -->');
                           l_prev_uda_status := x_return_status;
						   IF x_return_status = 'E' THEN
						      l_total_errorcnt := l_total_errorcnt + 1;
						   END IF;
                        END IF;

---------------------------------------------------------------------------------------------------------------
                        write_log_p (   'slc_sminf_process_sites_p: Value of g_multiple_ind --> '
                                     || g_multiple_ind
                                    );

	          --Commented as per Divya, Multiple Indicator flag is disabled for now. If required, this code needs to be uncommented.
                        /*
                        IF g_multiple_ind > 1
                        THEN
                           write_log_p
                              (   'slc_sminf_process_sites_p: << Updating Contr Attr Agreement --> Multiple indicator to Yes using :'
                               || g_multiple_ind
                              );
                           slc_sminf_single_uda_attrs_p
                              (p_site_id              => l_site_tbl
                                                                 (l_site_index).site_id,
                               p_attr_group_name      => gc_contr_attr, --'SLC_SM_CONTRACTUAL_ATTRIBUTES',
                               p_attr_name            => gc_mult_indctr_attr_name, --'MULTIPLE_INDICATOR',
                               p_attr_char_value      => 'Yes',
                               p_attr_num_value       => NULL,
                               p_attr_date_value      => NULL,
                               p_batch_id             => NULL,         
                               p_site_purpose         => g_site_purpose,
                               x_return_status        => x_return_status,
                               x_msg_count            => x_msg_count,
                               x_msg_data             => x_return_msg
                              );
                           write_log_p
                              (   'slc_sminf_process_sites_p: Return status from slc_sminf_single_uda_attrs_p  : '
                               || x_return_status
                              );
                           write_log_p
                              (   'slc_sminf_process_sites_p: Return count from slc_sminf_single_uda_attrs_p   : '
                               || x_msg_count
                              );
                           write_log_p
                              (   'slc_sminf_process_sites_p: Return message from slc_sminf_single_uda_attrs_p : '
                               || x_return_msg
                              );
                           write_log_p
                              ('slc_sminf_process_sites_p: ---------End : Processing attribute details--------'
                              );
                           l_prev_uda_status := x_return_status;
						   IF x_return_status = 'E' THEN
						      l_total_errorcnt := l_total_errorcnt + 1;
						   END IF;
						   
                        END IF;
						*/
---------------------------------------------------------------------------------------------------------------
                     ELSE
                        l_total_errorcnt := l_total_errorcnt + 1;
                     END IF;                 --end of process uda status check
                  ELSE
                     l_total_errorcnt := l_total_errorcnt + 1;
                  END IF;                     --end of delete uda status check
               ELSE
                  l_total_errorcnt := l_total_errorcnt + 1;
               END IF;                        --end of backup uda status check

               write_log_p ('slc_sminf_process_sites_p: Total Error count :' || l_total_errorcnt);
			   	
               write_log_p
                  ('slc_sminf_process_sites_p: ---------Start : Updating staging table slc_sminf_1a_host_to_site_stg to Processed--------'
                  );

            --write_log_p (l_crlf);

			
            IF l_total_errorcnt = 0
            THEN
               p_retcode := 0;
               write_log_p
                  ('slc_sminf_process_sites_p: --- Committing transaction after checking all statuses --- '
                  );
               COMMIT;
               l_total_success_records := l_total_success_records + 1;
            ELSE
               ROLLBACK TO before_uda_update;
               p_retcode := 1;
               l_total_failcust_validation := l_total_failcust_validation + 1;
               l_total_errorcust_validation :=
                                             l_total_errorcust_validation + 1;
               write_log_p
                  ('slc_sminf_process_sites_p: --- Roll back all transactions after checking all statuses --- '
                  );
            END IF;
			
               BEGIN
                  UPDATE slcapps.slc_sminf_1a_host_to_site_stg
                     SET status_code = DECODE (x_return_status,
                                               'E', 'E',
                                               'P'
                                              ),
                         error_message = x_return_msg,
                         last_update_date = SYSTIMESTAMP,
						 batch_id = l_unique_batch_id, --Added on 23 aug 2017 --to indicate current Batch ID
						 creation_date = SYSDATE,
						 created_by = g_userid,
						 last_updated_by = g_userid,
						 conc_request_id  = g_conc_request_id
                   WHERE record_id = l_site_tbl (l_site_index).record_id;
               EXCEPTION
                  WHEN OTHERS
                  THEN
                     write_log_p
                        (   'slc_sminf_process_sites_p: In When others, Error while updating the staging table slc_sminf_1a_host_to_site_stg : '
                         || SQLERRM
                        );
               END;

               write_log_p
                  ('slc_sminf_process_sites_p: ---------End : Updating staging table slc_sminf_1a_host_to_site_stg to Processed--------'
                  );
            ELSE                                           --l_site_id is NULL
               write_log_p
                  ('slc_sminf_process_sites_p: ---------Start : Updating staging table slc_sminf_1a_host_to_site_stg to Errored--------'
                  );
               write_log_p ('slc_sminf_process_sites_p: '||l_site_tbl (l_site_index).error_message);

               BEGIN
                  UPDATE slcapps.slc_sminf_1a_host_to_site_stg
                     SET status_code = 'E',
                         error_message =
                                       l_site_tbl (l_site_index).error_message,
                         last_update_date = SYSTIMESTAMP,
						 batch_id = l_unique_batch_id, --Added on 23 aug 2017 --to indicate current Batch ID
						 creation_date = SYSDATE,
						 created_by = g_userid,
						 last_updated_by = g_userid,
						 conc_request_id  = g_conc_request_id
                   WHERE record_id = l_site_tbl (l_site_index).record_id;

                  write_log_p
                     ('slc_sminf_process_sites_p: ---------End : Updating staging table slc_sminf_1a_host_to_site_stg to Errored--------'
                     );
               EXCEPTION
                  WHEN OTHERS
                  THEN
                     write_log_p
                        (   'slc_sminf_process_sites_p: In When others, Error while updating the staging table slc_sminf_1a_host_to_site_stg : '
                         || SQLERRM
                        );
               END;
            END IF;


         END LOOP;
      END IF;

      EXIT WHEN cur_sites%NOTFOUND;
   END LOOP;                                 -- end loop for cur_sites cursor.

   CLOSE cur_sites;                                  -- close cur_sites cursor

   --IF l_backup_uda_status <> 'E'
   --AND l_delete_uda_status <> 'E'
   --AND l_process_uda_status <> 'E'
   --AND l_prev_uda_status <> 'E'
   IF l_total_errorcnt = 0 AND p_purge = 'Y'
   THEN
      write_log_p ('slc_sminf_process_sites_p: Calling slc_sminf_purge_stg_p');
      slc_sminf_purge_stg_p (p_purge_flag      => p_purge,
                             p_ret_status      => x_return_status,
                             p_ret_msg         => x_return_msg
                            );
      write_log_p (   'slc_sminf_process_sites_p: Return status from slc_sminf_purge_stg_p    : '
                   || x_return_status
                  );
      write_log_p (   'slc_sminf_process_sites_p: Return message from slc_sminf_purge_stg_p   : '
                   || x_return_msg
                  );
      l_purge_stg_status := x_return_status;
   END IF;

   write_log_p ('slc_sminf_process_sites_p: -----------Status of operations-----------');
   write_log_p ('slc_sminf_process_sites_p:  Backup all the attributes          :' || l_backup_uda_status);
   write_log_p ('slc_sminf_process_sites_p:  Deleting all the attributes        :' || l_delete_uda_status);
   write_log_p ('slc_sminf_process_sites_p:  Processing all the attributes      :'
                || l_process_uda_status
               );
   write_output_p ('Timestamp of Request Completion : ' || SYSTIMESTAMP);
   write_output_p ('Request Completion status       : Completed');
   write_output_p ('Details of records in error     : ');

   FOR i IN (SELECT store_letter_code, error_message, source_file_name,
                    soa_instance_id
               FROM slcapps.slc_sminf_1a_host_to_site_stg
              WHERE status_code IN ('F', 'E') 
			  AND batch_id = DECODE(status_code, 'F', batch_id, l_unique_batch_id))
   LOOP
      write_output_p (i.error_message || l_crlf);
      populate_err_object (p_in_batch_key            => g_batch_key,
                           p_in_business_entity      => l_business_entity_name,
                           p_in_process_id3          => i.store_letter_code,
                           p_in_error_txt            => i.error_message,
                           p_in_request_id           => g_conc_request_id,
                           p_in_attribute1           =>    'Store letter code:'
                                                        || i.store_letter_code,
                           p_in_attribute2           =>    'File Name :'
                                                        || i.source_file_name,
                           p_in_attribute3           =>    'SOA Instance ID :'
                                                        || i.soa_instance_id
                          );
   END LOOP;

   write_log_p
      ('slc_sminf_process_sites_p: ---------Start : Updating staging table SLCAPPS.slc_sminf_attr_details_stg to Processed--------'
      );

   BEGIN
      UPDATE slcapps.slc_sminf_attr_details_stg                       
         SET status_code = 'P',
             created_by = g_userid,
             creation_date = SYSDATE,
             last_updated_by = g_userid,
             conc_request_id = g_conc_request_id,
             --Unique key to identify records
             last_update_date = SYSDATE
       WHERE status_code = 'N';

      write_log_p
         (   'slc_sminf_process_sites_p: Number of records updated to P in table SLCAPPS.slc_sminf_attr_details_stg :'
          || SQL%ROWCOUNT
         );
   EXCEPTION
      WHEN OTHERS
      THEN
         write_log_p
            (   'slc_sminf_process_sites_p: In When others, Error while updating the staging table SLCAPPS.slc_sminf_attr_details_stg : '
             || SQLERRM
            );
   END;

   write_log_p
      ('slc_sminf_process_sites_p: ---------End : Updating staging table SLCAPPS.slc_sminf_attr_details_stg to Processed--------'
      );

   IF SQL%ROWCOUNT > 0
   THEN
      write_log_p
         ('slc_sminf_process_sites_p: Committing transaction after updating SLCAPPS.slc_sminf_attr_details_stg --> '
         );
      COMMIT;
   END IF;

   COMMIT;                               --To commit all staging table updates
   slc_util_jobs_pkg.slc_util_e_log_summary_p
                  (p_batch_key                      => g_batch_key,
                   p_business_process_name          => g_business_process_name,
                   p_total_records                  => l_total_record,
                   p_total_success_records          => l_total_success_records,
                   p_total_failcustval_records      => l_total_failcust_validation,
                   p_total_failstdval_records       => l_total_errorcust_validation,
                   p_batch_status                   => l_batch_status,
                   p_publish_flag                   => l_publish_flag,
                   p_system_type                    => l_system_type,
                   p_source_system                  => l_source,
                   p_target_system                  => l_destination,
                   p_request_id                     => g_conc_request_id,
                   p_user_id                        => g_userid,
                   p_login_id                       => g_login_id,
                   p_status_code                    => l_cmn_err_status_code
                  );
   slc_util_jobs_pkg.slc_util_log_errors_p
                          (p_batch_key                  => g_batch_key,
                           p_business_process_name      => g_business_process_name,
                           p_errors_rec                 => g_cmn_err_rec,
                           p_user_id                    => g_userid,
                           p_login_id                   => g_login_id,
                           p_status_code                => l_cmn_err_status_code
                          );

--------------------------------------------------------------------------------------------
--Submit Concurrent Program SLCGL - Program to Update Balancing Segment Values for
-- successful store letter codes
--------------------------------------------------------------------------------------------
   FOR i IN (SELECT store_letter_code
               FROM slcapps.slc_sminf_1a_host_to_site_stg
              WHERE status_code IN ('P') AND batch_id = l_unique_batch_id)
   LOOP
      write_log_p
         ('slc_sminf_process_sites_p: Calling the concurrent program  --> SLCGL - Program to Update Balancing Segment Values for '||i.store_letter_code 
         );

      BEGIN
         fnd_global.apps_initialize (g_userid, g_resp_id, g_resp_appl_id);
      EXCEPTION
         WHEN OTHERS
         THEN
            write_log_p ('slc_sminf_process_sites_p: Error while intialising apps: ' || SQLERRM
                         || SQLCODE
                        );
            RAISE e_apps_not_initialized;
      END;

      l_request_id :=
         fnd_request.submit_request (application      => gc_conc_appl_name, --'SLCUST1',
                                     program          => gc_bal_segment_conc_short, --'GLEXTUPDATEBALSEG',
                                     description      => NULL,
                                     start_time       => SYSDATE,
                                     sub_request      => FALSE,
                                     argument1        => i.store_letter_code,
									 --Changes for v1.1
									 --Debug flag value was not correct
                                     --argument2        => 'Y'    --P_DEBUG_FLAG
									 argument2        => 'YES'    --P_DEBUG_FLAG
                                    );
      COMMIT;
      write_log_p ('slc_sminf_process_sites_p: Concurrent Request ID : ' || l_request_id);
   END LOOP;
EXCEPTION
   WHEN e_apps_not_initialized
   THEN
      write_log_p ('slc_sminf_process_sites_p: Error while intialising apps : ' || SQLERRM);
      p_retcode := 1;
   WHEN OTHERS
   THEN
      p_retcode := 2;
      write_log_p (   'slc_sminf_process_sites_p: In Others exception slc_sminf_process_sites_p'
                   || SQLCODE
                   || ' - '
                   || SQLERRM
                  );
      write_log_p (   'slc_sminf_process_sites_p: Error occured at :'
                   || DBMS_UTILITY.format_error_stack ()
                   || '  '
                   || DBMS_UTILITY.format_error_backtrace ()
                  );
END slc_sminf_process_sites_p;

-------------------------------------------------------------------------------
--   Procedure      : slc_sminf_main_p
--   Purpose        : This procedure must be invoked from SOA service
--                    ASI2SendStoreChangeoverSOA to process files from 1A host
--                    system for store changeover
--   Parameters     : p_soa_instance_id   IN          NUMBER
--                    p_file_name         IN          VARCHAR2
--                    p_changeover        IN          SLC_1A_HOST_SITE_DET_TAB
--   Modifications  :
-------------------------------------------------------------------------------

PROCEDURE slc_sminf_main_p (
   p_soa_instance_id   IN   NUMBER,
   p_file_name         IN   VARCHAR2,
   p_changeover        IN   SLC_1A_HOST_SITE_DET_TAB
)
IS
   x_return_status            VARCHAR2 (2);
   x_msg_count                NUMBER;
   x_return_msg               VARCHAR2 (4000);
   l_message                  VARCHAR2 (2000);
   l_batch_id                 NUMBER;
   l_site_id                  NUMBER;
   l_site_rec_id              NUMBER;
   l_store_letter_code        VARCHAR2 (150);                         
   l_store_number             VARCHAR2 (150);                         
   l_actual_changeover_date   DATE;
   l_error_msg                VARCHAR2 (200);
BEGIN
   write_log_p ('slc_sminf_main_p: Start of procedure');

   IF p_changeover.EXISTS (1)
   THEN
      l_batch_id := -1;
      l_batch_id := slcapps.slc_sminf_1ahost_site_s.NEXTVAL;

      FOR i IN p_changeover.FIRST .. p_changeover.LAST
      LOOP
         l_site_rec_id := -1;
         l_site_rec_id := slcapps.slc_sminf_1ahost_site_s.NEXTVAL;

         BEGIN
            l_store_letter_code := NULL;
            l_actual_changeover_date := NULL;

            SELECT TRIM
                      (LEADING '0' FROM SUBSTR
                                          (p_changeover (i).changeover_details,
                                           1,
                                           8
                                          )
                      ) store_letter_code,
                   TO_CHAR
                       (TO_DATE (SUBSTR (p_changeover (i).changeover_details,
                                         9
                                        ),
                                 'YYYYMMDD'
                                ),
                        'DD-MON-YYYY'
                       ) actual_changeover_date
              INTO l_store_letter_code,
                   l_actual_changeover_date
              FROM DUAL;

         EXCEPTION
            WHEN OTHERS
            THEN
               l_error_msg :=
                     'Error while fetching Store letter code and Actual Changeover date from the File :'
                  || p_changeover (i).changeover_details
                  || ' Error:'
                  || SQLERRM;
               write_log_p ('slc_sminf_main_p: '||l_error_msg);
         END;

         BEGIN
            l_store_number := NULL;
            l_site_id := NULL;

            SELECT rrs.site_identification_number, rrs.site_id
              INTO l_store_number, l_site_id
              FROM ego_attr_groups_v aegrp,
                   rrs_sites_ext_b arext,
                   rrs_sites_b rrs
             WHERE aegrp.attr_group_id = arext.attr_group_id
               AND arext.site_id = rrs.site_id
               AND aegrp.attr_group_type = gc_site_group
               AND aegrp.attr_group_name = gc_store_oper_grp_pending
               AND aegrp.attr_group_id = arext.attr_group_id
               AND arext.site_id = rrs.site_id
               AND arext.c_ext_attr1 = l_store_letter_code;

         EXCEPTION
            WHEN OTHERS
            THEN
               l_error_msg :=
                     'Unable to derive store number and site ID for store letter code'
                  || l_store_number
                  || ' '
                  || SQLCODE
                  || ' '
                  || SQLERRM;
               l_store_number := NULL;
               l_site_id := NULL;
               write_log_p ('slc_sminf_main_p: '||l_error_msg);
         END;

         --Print all the values derived above in the log file
         write_log_p ('slc_sminf_main_p: Derivation - Site ID                :' || l_site_id);
         write_log_p ('slc_sminf_main_p: Derivation - Store Number           :'
                      || l_store_number
                     );
         write_log_p (   'slc_sminf_main_p: Derivation - Store letter code      :'
                      || l_store_letter_code
                     );
         write_log_p (   'slc_sminf_main_p: Derivation - Actual Changeover Date :'
                      || l_actual_changeover_date
                     );

         BEGIN
            INSERT INTO slcapps.slc_sminf_1a_host_to_site_stg
                        (record_id, batch_id, soa_instance_id,
                         site_id, store_number, store_letter_code,
                         actual_changeover_date, organization_id,
                         attribute1, attribute2, attribute3, attribute4,
                         attribute5, attribute6, attribute7, attribute8,
                         attribute9, attribute10, source_file_name,
                         status_code,
                         error_message,
                         created_by, creation_date, last_update_date,
                         last_update_login, last_updated_by, conc_request_id
                        )
                 VALUES (l_site_rec_id,                           --RECORD_ID,
                                       l_batch_id,
									   p_soa_instance_id,
                         --PROCESS_ID,
                         l_site_id,                                 --SITE_ID,
                                   l_store_number, l_store_letter_code,
                         --ORGANIZATION_ID,
                         l_actual_changeover_date, NULL,               
                         NULL,                                   --ATTRIBUTE1,
                              NULL,                              --ATTRIBUTE2,
                                   NULL,                         --ATTRIBUTE3,
                                        NULL,                    --ATTRIBUTE4,
                         NULL,                                   --ATTRIBUTE5,
                              NULL,                              --ATTRIBUTE6,
                                   NULL,                         --ATTRIBUTE7,
                                        NULL,                    --ATTRIBUTE8,
                         NULL,                                   --ATTRIBUTE9,
                              NULL,                             --ATTRIBUTE10,
                                   p_file_name,
                         DECODE (l_site_id, NULL, 'F', 'N'),    --STATUS_CODE,
                         DECODE (l_site_id,
                                 NULL, 'The store letter code '
                                  || l_store_letter_code
                                  || ' does not exists on pending operator'
                                ),                            --ERROR_MESSAGE,
                         g_userid,                                 --CREATED_BY
                         SYSDATE, 
						 SYSDATE,
                         NULL,                             
                         NULL,
                         g_conc_request_id
                        );

            write_log_p ('slc_sminf_main_p: End of procedure slc_sminf_main_p');
         EXCEPTION
            WHEN OTHERS
            THEN
               l_error_msg :=
                     'When Others in slc_sminf_main_p, Error while inserting data for file:'
                  || p_changeover (i).changeover_details
                  || 'File Name: '
                  || p_file_name
                  || ' Error:'
                  || SQLERRM;
         END;
      END LOOP;
   END IF;

EXCEPTION
   WHEN OTHERS
   THEN
      write_log_p (   'slc_sminf_main_p: Other Exceptions: slc_sminf_main_p '
                   || SQLCODE
                   || ':'
                   || SQLERRM
                  );
      write_log_p (   'slc_sminf_main_p: Error'
                   || DBMS_UTILITY.format_error_stack ()
                   || '  '
                   || DBMS_UTILITY.format_error_backtrace ()
                  );
END;

END slc_sminf_1a_host_to_site_pkg;
/
SHOW ERRORS;
EXIT;
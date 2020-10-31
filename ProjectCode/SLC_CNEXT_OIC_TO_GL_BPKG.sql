CREATE OR REPLACE 
PACKAGE BODY slc_cnext_oic_to_gl_pkg
--+============================================================|
--+  Program    :      slc_cnext_oic_to_gl_pkg.sql
--+  Author     :      Rajni
--+  Date       :      03-Mar-2017
--+  Purpose    :      This package will have procedure that will create Journal entry
--+                       from OIC if it is not existing in the system for a particular planed element.
--+  Change Log :      17-Mar-2017   Rajni   Created
--+   1.0       :      10-Oct-2017   Rajni   Changes made as per defect#42846
--+   1.1       :      26-Oct-2017   Kanwal  Changes made as per defect#43426
--+   1.2       :      27-Oct-2017   Kanwal  Changes made as per defect#43473
--+   1.3       :      12-Jan-2018   Rajni   Changes made as per defect#44653
--+   1.4       :      18-Jan-2018   Rajni   Changes made as per defect#44638
--+   1.5       :      01-Feb-2018   Rajni   Changes made as per defect#44985
--+   1.6       :      19-Jun-2018   Anand   Changes made for Incident# INC000003751441
--+   1.7       :      26-Jun-2018   Anand   Changes made for 40% object
--+   1.8       :      16-Jul-2018   Akshay  Changes as part of Defect 46891
--+============================================================|
IS
   gv_log                       VARCHAR2 (10)                        := 'LOG';
   gv_out                       VARCHAR2 (10)                        := 'OUT';
   gv_debug_flag                VARCHAR2 (10);
   gv_yes_code                  VARCHAR2(3)                          := 'YES';
   gv_yes_flag                  VARCHAR2 (3)                      DEFAULT 'Y';
   gn_request_id                NUMBER     DEFAULT fnd_global.conc_request_id;
   gn_user_id                   NUMBER             DEFAULT fnd_global.user_id;
   gn_login_id                  NUMBER            DEFAULT fnd_global.login_id;
   gv_access_set_name           VARCHAR2 (30)           := 'SLC Consolidated';
--Variables for Common Error Handling.
   gv_batch_key                 VARCHAR2 (50)
                  DEFAULT 'FRC-E-047' || '-' || TO_CHAR (SYSDATE, 'DDMMYYYY');
   gv_business_process_name     VARCHAR2 (100)   := 'SLC_CNEXT_OIC_TO_GL_PKG';
   gv_flex_code                 CONSTANT VARCHAR2 (20) := 'GL#';
   gv_acc_flexfield              CONSTANT VARCHAR2 (30)
                                                := 'SLC_ACCOUNTING_FLEXFIELD';
   gv_cmn_err_rec               apps.slc_util_jobs_pkg.g_error_tbl_type;
   gv_cmn_err_count             NUMBER                              DEFAULT 0;
--Variables for OIC to GL
   gv_journal_source            VARCHAR2 (20)                        := 'OIC';
   gv_status                    VARCHAR2 (20)                       := 'CALC';
   gv_period_status             VARCHAR2 (20)                          := 'O';
   gv_product_code              VARCHAR2 (20)                         := 'GL';
   gv_adjustment_flag           VARCHAR2 (20)                          := 'N';
   gv_slc_sm_operational_mgmt   VARCHAR2 (30)    := 'SLC_SM_OPERATIONAL_MGMT';
   gv_attr_grp_type             VARCHAR2 (30)         := 'RRS_SITEMGMT_GROUP';
   gv_appl_short_name           VARCHAR2 (15)                        := 'RRS';
   gc_app_short_name             CONSTANT VARCHAR2 (20) := 'SQLGL';
   lv_business_entity_name         VARCHAR2 (100) := 'SLC_CNEXT_OIC_TO_GL_PKG';
   gc_sysdate                    CONSTANT DATE := SYSDATE;

    /* ****************************************************************
       NAME:              populate_err_object
       PURPOSE:           This procedure will keep on inserting error records
                   in the error table.
       Input Parameters:  p_in_batch_key
                  p_in_business_entity
                  p_in_process_id1
                  p_in_process_id2
                  p_in_error_code
                  p_in_error_txt
                  p_in_request_id
                  p_in_attribute1
                  p_in_attribute2
                  p_in_attribute3
                  p_in_attribute4
                  p_in_attribute5
   *****************************************************************/
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
      gv_cmn_err_count := gv_cmn_err_count + 1;
      gv_cmn_err_rec (gv_cmn_err_count).seq := slc_util_batch_key_s.NEXTVAL;
      gv_cmn_err_rec (gv_cmn_err_count).business_process_entity :=
                                                         p_in_business_entity;
      gv_cmn_err_rec (gv_cmn_err_count).business_process_id1 :=
                                                             p_in_process_id1;
      gv_cmn_err_rec (gv_cmn_err_count).business_process_id2 :=
                                                             p_in_process_id2;
      gv_cmn_err_rec (gv_cmn_err_count).business_process_id3 :=
                                                             p_in_process_id3;
      gv_cmn_err_rec (gv_cmn_err_count).business_process_id4 :=
                                                             p_in_process_id4;
      gv_cmn_err_rec (gv_cmn_err_count).business_process_id5 :=
                                                             p_in_process_id5;
      gv_cmn_err_rec (gv_cmn_err_count).business_process_step :=
                                                   p_in_business_process_step;
      gv_cmn_err_rec (gv_cmn_err_count).ERROR_CODE := p_in_error_code;
      gv_cmn_err_rec (gv_cmn_err_count).ERROR_TEXT := p_in_error_txt;
      gv_cmn_err_rec (gv_cmn_err_count).request_id := p_in_request_id;
      gv_cmn_err_rec (gv_cmn_err_count).attribute1 := p_in_attribute1;
      gv_cmn_err_rec (gv_cmn_err_count).attribute2 := p_in_attribute2;
      gv_cmn_err_rec (gv_cmn_err_count).attribute3 := p_in_attribute3;
      gv_cmn_err_rec (gv_cmn_err_count).attribute4 := p_in_attribute4;
      gv_cmn_err_rec (gv_cmn_err_count).attribute5 := p_in_attribute5;
   END;

/* ****************************************************************
    NAME:              slc_cnext_write_log_p
    PURPOSE:           This procedure will insert data into either
                       concurrent program log file or in concurrent program output file
                       based on the parameter passed to the input program
    Input Parameters:  p_in_message
                       p_in_log_type
*****************************************************************/
  PROCEDURE slc_cnext_write_log_p(p_in_log_type IN VARCHAR2
  		             ,p_in_message IN VARCHAR2)
  IS
    BEGIN

    IF p_in_log_type = gv_log AND gv_debug_flag = gv_yes_code
    THEN
       fnd_file.put_line (fnd_file.LOG, p_in_message);
    END IF;

    IF p_in_log_type = gv_out
    THEN
       fnd_file.put_line (fnd_file.output, p_in_message);
    END IF;
  END slc_cnext_write_log_p;

--===============================================================
-- Function Name  : slc_cnext_balanc_seg_p
-- Description     : This is the function called by the
--                   slc_glext_jour_ent_p procedure to fetch zone of respective store
-- Parameters      :N/A
--===============================================================
   PROCEDURE slc_cnext_balanc_seg_p (
      p_balancing_type   IN       VARCHAR2,
      p_resource_name    IN       VARCHAR2,
      p_bal_seg          OUT      VARCHAR2,
      p_err_flag         OUT      VARCHAR2,
      p_err_msg          OUT      VARCHAR2
   )
   IS
   BEGIN
      p_bal_seg := NULL;
      slc_cnext_write_log_p
         (gv_log,
             'Calling slc_cnext_balanc_seg_p procedure to fetch segment1 with balancing segment type'
          || p_balancing_type
         );

      IF p_balancing_type = 'STORE LETTER CODE'
      THEN
         BEGIN
            SELECT SUBSTR (cost_center, 1, INSTR (cost_center, '.', 1) - 1)
              INTO p_bal_seg
              FROM jtf_rs_defresources_v
             WHERE resource_name = p_resource_name
               AND CATEGORY = 'OTHER';
              -- AND end_date_active IS NULL;--COMMENTED AS PER DEFECT 44985
         EXCEPTION
            WHEN NO_DATA_FOUND
            THEN
               p_bal_seg := NULL;
               p_err_flag := 'Y';
               p_err_msg :=
                     'Store letter code is not defined for the resource name :'
                  || p_resource_name;
            WHEN TOO_MANY_ROWS
            THEN
               p_bal_seg := NULL;
               p_err_flag := 'Y';
               p_err_msg :=
                     'Multiple Store letter code is found for the resource name :'
                  || p_resource_name;
			WHEN OTHERS
            THEN
               p_bal_seg := NULL;
               p_err_flag := 'Y';
               p_err_msg :=
                     'Error while fetching Store letter code for the resource name :'
                  || p_resource_name;
         END;
      ELSIF p_balancing_type = 'COMPANY'
      THEN
         BEGIN
            SELECT ffvl.attribute10
              INTO p_bal_seg
              FROM fnd_flex_values_vl ffvl, fnd_flex_value_sets ffvs
             WHERE ffvs.flex_value_set_id = ffvl.flex_value_set_id
               AND ffvs.flex_value_set_name = 'SLCGL_LOCATION'
               AND ffvl.flex_value =
                      LPAD ((SUBSTR (p_resource_name,
                                     1,
                                     LENGTH (p_resource_name) - 1
                                    )
                            ),
                            7,
                            '0'
                           );
         EXCEPTION
            WHEN NO_DATA_FOUND
            THEN
               p_err_flag := 'Y';
               p_err_msg :=
                     'Company number is not defined for the resource name :'
                  || p_resource_name;
			WHEN OTHERS
            THEN
               p_err_flag := 'Y';
               p_err_msg :=
                     'Error while fetching Company number for the resource name :'
                  || p_resource_name;
         END;
      END IF;

      slc_cnext_write_log_p
         (gv_log,
             'Ending slc_cnext_balanc_seg_p procedure to fetch segment1 with balancing segment type'
          || p_balancing_type
          || ' and value :'
          || p_bal_seg
         );
   EXCEPTION
      WHEN OTHERS
      THEN
         p_err_flag := 'Y';
         p_err_msg :=
               'Balancing Segment is not defined for the balancing type :'
            || p_balancing_type;
   END slc_cnext_balanc_seg_p;

--===============================================================
-- Function Name  : slc_cnext_loc_seg_p
-- Description     : This is the function called by the
--                   slc_glext_jour_ent_p procedure to fetch zone of respective store
-- Parameters      :N/A
--===============================================================
   PROCEDURE slc_cnext_loc_seg_p (
      p_loc_type        IN       VARCHAR2,
      p_resource_name   IN       VARCHAR2,
      p_cur_code        IN       VARCHAR2,
      p_quota_id        IN       NUMBER,
      p_loc_seg         OUT      VARCHAR2,
      p_err_flag        OUT      VARCHAR2,
      p_err_msg         OUT      VARCHAR2
   )
   IS
      l_site_num           VARCHAR2 (20);
      l_site_id            NUMBER;
      l_appl_id            NUMBER;
      ex_site_not_found    EXCEPTION;
      ex_appln_not_found   EXCEPTION;
   BEGIN
      slc_cnext_write_log_p
         (gv_log,
             'Calling slc_cnext_loc_seg_p procedure to fetch segment2 with location segment type '
          || p_loc_type
         );
      l_site_num :=
         LPAD ((SUBSTR (p_resource_name, 1, LENGTH (p_resource_name) - 1)),
               7,
               '0'
              );
      p_loc_seg := NULL;

      IF p_loc_type = 'STORE'
      THEN
         BEGIN
            SELECT SUBSTR (cost_center,
                           INSTR (cost_center, '.', 1) + 1,
                           LENGTH (cost_center)
                          )
              INTO p_loc_seg
              FROM jtf_rs_defresources_v
             WHERE resource_name = p_resource_name
               AND category = 'OTHER';
              -- AND end_date_active IS NULL;
         EXCEPTION
            WHEN NO_DATA_FOUND
            THEN
               p_err_flag := 'Y';
               p_err_msg :=
                     'Store is not defined for the given resource name :'
                  || p_resource_name;
            WHEN TOO_MANY_ROWS
            THEN
               p_err_flag := 'Y';
               p_err_msg :=
                     'Multiple Store found for the resource name :'
                  || p_resource_name;
			WHEN OTHERS
            THEN
               p_err_flag := 'Y';
               p_err_msg :=
                     'Error while fetching Store for the resource name :'
                  || p_resource_name;
         END;
      ELSIF p_loc_type = 'MARKET COST CENTER'
      THEN
         p_loc_seg := SLC_GL_UTIL_PKG.get_market_value(l_site_num);

         IF p_loc_seg IS NULL
         THEN
            p_err_flag := 'Y';
            p_err_msg := 'Market not defined for site number :' || l_site_num;
         END IF;
      ELSIF p_loc_type = 'ZONE COST CENTER'
      THEN
         p_loc_seg := SLC_GL_UTIL_PKG.get_store_zone(l_site_num);

         IF p_loc_seg IS NULL
         THEN
            p_err_flag := 'Y';
            p_err_msg := 'Zone not defined for site number :' || l_site_num;
         END IF;
      ELSIF p_loc_type = 'SSC COST CENTER'
      THEN
         BEGIN
            SELECT attribute15
              INTO p_loc_seg
              FROM cn_quotas_all
             WHERE quota_id = p_quota_id;
         EXCEPTION
            WHEN NO_DATA_FOUND
            THEN
               p_err_flag := 'Y';
               p_err_msg :=
                     'Market cost center is not defined for the given resource name the given quota id :'
                  || p_quota_id;
			WHEN OTHERS
            THEN
               p_err_flag := 'Y';
               p_err_msg :=
                     'Error while fetching Market cost center for the given resource name the given quota id :'
                  || p_quota_id;
         END;
      END IF;

      slc_cnext_write_log_p
         (gv_log,
             'Ending slc_cnext_loc_seg_p procedure to fetch segment2 with location segment type '
          || p_loc_type
          || ' and value :'
          || p_loc_seg
         );
   EXCEPTION
      WHEN OTHERS
      THEN
         p_err_flag := 'Y';
         p_err_msg :=
               'Location option is not defined for the given resource name :'
            || p_resource_name;
   END slc_cnext_loc_seg_p;

/*--===============================================================
  -- Function Name  : slc_cnext_val_code_comb_p
  -- Description     : This is the function called by the
  --                   slc_glext_jour_ent_p procedure to fetch zone of respective store
  -- Parameters      :N/A
  --===============================================================

	PROCEDURE slc_cnext_val_code_comb_p (p_segment1       IN      VARCHAR2
	                                    ,p_segment2       IN      VARCHAR2
							            ,p_segment3       IN      VARCHAR2
							            ,p_segment4       IN      VARCHAR2
							            ,p_segment5       IN      VARCHAR2
							            ,p_segment6       IN      VARCHAR2
							            ,p_err_flag       OUT     VARCHAR2
							            ,p_err_msg        OUT     VARCHAR2)
    IS
	ex_code_comb_not_found             EXCEPTION;
	lv_val_code_comb                   NUMBER;
	lv_code_com                        VARCHAR2(100);

	BEGIN
	    lv_code_com := p_segment1||'.'||p_segment2||'.'||p_segment3||'.'||p_segment4||'.'||p_segment5||'.'||p_segment6;
	     slc_cnext_write_log_p(gv_log,'Calling slc_cnext_val_code_comb_p procedure to validate code combination '||lv_code_com );
	      lv_val_code_comb:=0;
		  p_err_flag:='N';
		  SELECT COUNT(*)
		  INTO  lv_val_code_comb
          FROM gl_code_combinations
          WHERE segment1 =p_segment1
		  AND segment2 =p_segment2
		  AND segment3 =p_segment3
		  AND segment4 =p_segment4
		  AND segment5 =p_segment5
		  AND segment6 =p_segment6;
		  slc_cnext_write_log_p(gv_log,'Ending slc_cnext_val_code_comb_p procedure to validate code combination '||lv_code_com );

		  IF lv_val_code_comb=0 THEN
		  p_err_flag:='Y';
		  p_err_msg :='Code combination doesnt exist :'||lv_code_com;
		  END IF;

		  EXCEPTION
		  WHEN OTHERS THEN
		  p_err_flag:='Y';
		  p_err_msg :='Unhandled exception for code combination :'||lv_code_com;
	END slc_cnext_val_code_comb_p;*/

	--===============================================================
   -- Function Name  : get_val_code_comb_id_f
   -- Description    : This function return valid
   --                  code combination id.
   -- Parameters     : N/A
   --===============================================================
   FUNCTION get_val_code_comb_id_f (p_segment1   IN     VARCHAR2,
                                    p_segment2   IN     VARCHAR2,
                                    p_segment3   IN     VARCHAR2,
                                    p_segment4   IN     VARCHAR2,
                                    p_segment5   IN     VARCHAR2,
                                    p_segment6   IN     VARCHAR2,
                                    x_message       OUT VARCHAR2)
      RETURN NUMBER
   IS
      l_application_short_name   VARCHAR2 (240);
      l_key_flex_code            VARCHAR2 (240);
      l_structure_num            NUMBER;
      l_validation_date          DATE;
      n_segments                 NUMBER;
      segments                   APPS.FND_FLEX_EXT.SEGMENTARRAY;
      l_combination_id           NUMBER := 0;
      l_data_set                 NUMBER;
      l_return                   BOOLEAN;
      l_message                  VARCHAR2 (240);
      l_count_code_comb          NUMBER DEFAULT 0;
      l_valid_code_comb          VARCHAR2 (10);
      l_valid_bal_seg            VARCHAR2 (10);
      l_valid_loc_seg            VARCHAR2 (10);
   BEGIN
      SELECT COUNT (1)
        INTO l_count_code_comb
        FROM gl_code_combinations
       WHERE     segment1 = p_segment1
             AND segment2 = p_segment2
             AND segment3 = p_segment3
             AND segment4 = p_segment4
             AND segment5 = p_segment5
             AND segment6 = p_segment6;

      IF l_count_code_comb <> 0
      THEN
         BEGIN
            SELECT 'Y'
              INTO l_valid_code_comb
              FROM gl_code_combinations
             WHERE     segment1 = p_segment1
                   AND segment2 = p_segment2
                   AND segment3 = p_segment3
                   AND segment4 = p_segment4
                   AND segment5 = p_segment5
                   AND segment6 = p_segment6
                   AND enabled_flag = 'Y';
         EXCEPTION
            WHEN NO_DATA_FOUND
            THEN
               l_valid_code_comb := 'N';
         END;

         IF NVL (l_valid_code_comb, 'X') <> 'Y'
         THEN
		-- added below changes as per defect
		 x_message := 'Code Combination is disabled';
		 slc_cnext_write_log_p(gv_log,'x_message' || x_message);
            RETURN 0;
         END IF;


         --Validating Segment1
         BEGIN
            SELECT 'Y'
              INTO l_valid_bal_seg
              FROM fnd_flex_values_vl ffvl, fnd_flex_value_sets ffvs
             WHERE     ffvl.flex_value = p_segment1
                   AND ffvs.flex_value_set_id = ffvl.flex_value_set_id
                   AND ffvs.flex_value_set_name = 'SLCGL_BALANCING_SEGMENT'
                   AND SYSDATE BETWEEN NVL (ffvl.start_date_active,
                                            SYSDATE - 1)
                                   AND NVL (ffvl.end_date_active,
                                            SYSDATE + 1)
                   AND ffvl.enabled_flag = gv_yes_flag;
         EXCEPTION
            WHEN NO_DATA_FOUND
            THEN
               l_valid_bal_seg := 'N';
         END;

         IF NVL (l_valid_bal_seg, 'X') <> 'Y'
         THEN
		 -- added below changes as per defect
		 x_message := 'Balancing Segment1 is either disabled or not present';
		 slc_cnext_write_log_p(gv_log,'x_message' || x_message);
            RETURN 0;
         END IF;


         --Validating Segment2
         BEGIN
            SELECT 'Y'
              INTO l_valid_loc_seg
              FROM fnd_flex_values_vl ffvl, fnd_flex_value_sets ffvs
             WHERE     ffvl.flex_value = p_segment2
                   AND ffvs.flex_value_set_id = ffvl.flex_value_set_id
                   AND ffvs.flex_value_set_name = 'SLCGL_LOCATION'
                   AND SYSDATE BETWEEN NVL (ffvl.start_date_active,
                                            SYSDATE - 1)
                                   AND NVL (ffvl.end_date_active,
                                            SYSDATE + 1)
                   AND ffvl.enabled_flag = gv_yes_flag;
         EXCEPTION
            WHEN NO_DATA_FOUND
            THEN
               l_valid_loc_seg := 'N';
         END;

         IF NVL (l_valid_loc_seg, 'X') <> 'Y'
         THEN
		 -- added below changes as per defect
		 x_message := 'Location Segment2 is either disabled or not present';
		 slc_cnext_write_log_p(gv_log,'x_message' || x_message);
            RETURN 0;
         END IF;

         IF     NVL (l_valid_loc_seg, 'X') = 'Y'
            AND NVL (l_valid_bal_seg, 'X') = 'Y'
            AND NVL (l_valid_code_comb, 'X') = 'Y'
         THEN
            RETURN 1;
         END IF;


         slc_cnext_write_log_p(gv_log,'l_count_code_comb' || l_count_code_comb);
      --Creating if code combination doesnot exist
      ELSIF l_count_code_comb = 0
      THEN
         l_application_short_name := gc_app_short_name;
         l_key_flex_code := gv_flex_code;

         SELECT id_flex_num
           INTO l_structure_num
           FROM apps.fnd_id_flex_structures
          WHERE     id_flex_code = l_key_flex_code
                AND id_flex_structure_code = gv_acc_flexfield;

         l_validation_date := gc_sysdate;
         n_segments := 6;
         segments (1) := p_segment1;
         segments (2) := p_segment2;
         segments (3) := p_segment3;
         segments (4) := p_segment4;
         segments (5) := p_segment5;
         segments (6) := p_segment6;
         l_data_set := NULL;

         l_return :=
            FND_FLEX_EXT.GET_COMBINATION_ID (
               application_short_name   => l_application_short_name,
               key_flex_code            => l_key_flex_code,
               structure_number         => l_structure_num,
               validation_date          => l_validation_date,
               n_segments               => n_segments,
               segments                 => segments,
               combination_id           => l_combination_id,
               data_set                 => l_data_set);
         l_message := FND_FLEX_EXT.GET_MESSAGE;
         x_message := l_message;

         IF NOT l_return
         THEN
            x_message := 'Error in Code Combination Derivation ' || l_message;
            --ROLLBACK; --commented for defect
            RETURN 0;
         ELSE
            --COMMIT;
            RETURN 1;
         END IF;
      END IF;
   EXCEPTION
      WHEN OTHERS
      THEN
         x_message :=
            'Unexpected error in get_val_code_comb_id_f :' || SQLERRM;
         RETURN 0;
   END get_val_code_comb_id_f;

 --===============================================================
-- Procedure Name  : slc_cnext_jour_ent_ins_p
-- Description     : This  procedure called by the main Procedure
--                   to insert data in gl_interface table
-- Parameters      : 1. p_user_je_source_name      (IN)
--                   2. p_user_je_category_name    (IN)
--                   3. p_ledger_id                (IN)
--                   4. p_currency_code            (IN)
--                   5. p_accounting_date          (IN)
--                   6. p_entered_dr               (IN)
--                   7. p_entered_cr               (IN)
--                   8. p_segment1                 (IN)
--                   9. p_segment2                 (IN)
--                   10.p_segment3                 (IN)
--                   11.p_segment4                 (IN)
--                   12.p_segment5                 (IN)
--                   13.p_segment6                 (IN)
--                   14.p_reference1               (IN)
--                   15.p_reference4               (IN)

   --===============================================================
   PROCEDURE slc_cnext_jour_ent_ins_p (
      p_user_je_source_name     IN       VARCHAR2,
      p_user_je_category_name   IN       VARCHAR2,
      p_ledger_id               IN       NUMBER,
      p_currency_code           IN       VARCHAR2,
      p_accounting_date         IN       DATE,
      p_entered_dr              IN       NUMBER,
      p_entered_cr              IN       NUMBER,
      p_segment1                IN       VARCHAR2,
      p_segment2                IN       VARCHAR2,
      p_segment3                IN       VARCHAR2,
      p_segment4                IN       VARCHAR2,
      p_segment5                IN       VARCHAR2,
      p_segment6                IN       VARCHAR2,
      p_reference1              IN       VARCHAR2,
      p_reference10              IN       VARCHAR2,
      p_err_flag                OUT      VARCHAR2,
      p_err_msg                 OUT      VARCHAR2
   )
   IS
   BEGIN
      slc_cnext_write_log_p
         (gv_log,
          'Calling slc_cnext_jour_ent_ins_p procedure to insert data in gl_interface '
         );

      INSERT INTO gl_interface
                  (status,                                     ----need to ask
                          ledger_id, user_je_source_name,
                   user_je_category_name, currency_code, actual_flag,
                   accounting_date, date_created, created_by, entered_dr,
                   entered_cr, accounted_dr, accounted_cr, segment1,
                   segment2, segment3, segment4, segment5,
                   segment6, reference1, reference10
                  )
           VALUES ('NEW'
				 , p_ledger_id, p_user_je_source_name,
                   p_user_je_category_name, p_currency_code, 'A',
                   p_accounting_date, SYSDATE, gn_user_id, p_entered_dr,
                   p_entered_cr, p_entered_dr, p_entered_cr, p_segment1,
                   p_segment2, p_segment3, p_segment4, p_segment5,
                   p_segment6, p_reference1, p_reference10
                  );

      slc_cnext_write_log_p
         (gv_log,
          'Ending slc_cnext_jour_ent_ins_p procedure to insert data in gl_interface '
         );
   EXCEPTION
      WHEN OTHERS
      THEN
         p_err_flag := 'Y';
         p_err_msg :=
               'Error while inserting data in GL_INTERFACE table. Error message : '
            || SQLERRM;
   END slc_cnext_jour_ent_ins_p;

  --===============================================================
-- Procedure Name  : slc_cnext_oic_res_det_p
-- Description     : This  procedure called by the main Procedure
--                   to fetch data related to OIC and resource
-- Parameters      : 1. p_resource_id             (OUT)
--                   2. p_resource_name           (OUT)
--                   3. p_amount                  (OUT)
--                   4. p_journal_type            (OUT)
--                   5. p_debit_bal_segment       (OUT)
--                   6. p_debit_location          (OUT)
--                   7. p_debit_account           (OUT)
--                   8. p_debit_sub_account       (OUT)
--                   9. p_credit_bal_segment      (OUT)
--                   10.p_credit_location         (OUT)
--                   11.p_credit_account          (OUT)
--                   12.p_credit_sub_account      (OUT)
--                   13.p_journal_category        (OUT)
--                   14.p_tbd1                    (OUT)
--                   15.p_tbd2                    (OUT)
--                   16.p_credit_tbd1             (OUT)
--                   17.p_credit_tbd2             (OUT)
--                   18.p_currency_code           (OUT)
--                   19.p_ledger_id               (OUT)
--                   20.p_reference4              (OUT)
--                   21.p_ex_flag                 (OUT)
--                   22.p_quota_id            (IN)
--                   23.p_period                  (IN)
--                   24.p_operating_unit          (IN)
--                   25.p_start_date              (IN)
--                   26.p_end_date                (IN)

   --===============================================================
   PROCEDURE slc_cnext_oic_res_det_p (
      p_err_flag             OUT      VARCHAR2,
      p_err_msg              OUT      VARCHAR2,
      p_quota_id             IN       NUMBER,
      p_period               IN       NUMBER,
      p_operating_unit       IN       NUMBER,
      p_start_date           IN       NUMBER,
      p_end_date             IN       NUMBER,
	  p_total_count          OUT      NUMBER,
	  p_success_count        OUT      NUMBER,
	  p_error_count          OUT      NUMBER
   )
   IS
   --Added cursor to create journal entries on the basis of Salesrep_id as part of defect#
     CURSOR cur_salesrep_quota
      IS
	  SELECT   ccla.credited_salesrep_id resource_id, jrs.salesrep_number resource_name,
                  SUM (ccla.commission_amount) amount, cqa.attribute1 journal_type,
                  cqa.attribute2 debit_bal_segment, cqa.attribute3 debit_location, cqa.attribute4 debit_account,
                  cqa.attribute5 debit_sub_account, cqa.attribute6 credit_bal_segment,
                  cqa.attribute7 credit_location, cqa.attribute8 credit_account, cqa.attribute9 credit_sub_account,
                  cqa.attribute10 journal_category, cqa.attribute11 tbd1, cqa.attribute12 tbd2,
                  cqa.attribute13 credit_tbd1, cqa.attribute14 credit_tbd2, gll.currency_code currency_code,
                  gll.ledger_id ledge_id
			--Commented as part of defect#
            /* INTO p_resource_id, p_resource_name,
                  p_amount, p_journal_type,
                  p_debit_bal_segment, p_debit_location, p_debit_account,
                  p_debit_sub_account, p_credit_bal_segment,
                  p_credit_location, p_credit_account, p_credit_sub_account,
                  p_journal_category, p_tbd1, p_tbd2,
                  p_credit_tbd1, p_credit_tbd2, p_currency_code,
                  p_ledger_id*/
             FROM cn_commission_lines_all ccla,
			 cn_commission_headers_all ccha, --added as per defect#43473
                  jtf_rs_defresources_v jrd,
                  jtf_rs_salesreps jrs,
                  cn_quotas_all cqa,
                  hr_operating_units hou,
                  gl_ledgers gll
            WHERE hou.set_of_books_id = gll.ledger_id
              AND cqa.org_id = hou.organization_id
              AND jrd.hold_payment = 'N'
              AND jrs.salesrep_number = jrd.resource_name
              AND jrd.category = 'OTHER'
             -- AND jrd.end_date_active IS NULL     --as per defect #44985
              AND ccla.quota_id = cqa.quota_id
              AND ccla.credited_salesrep_id = jrs.salesrep_id
              AND ccla.status = gv_status
              AND ccla.processed_period_id =
                                NVL (p_period, NVL (p_start_date, p_end_date))
              AND cqa.org_id = p_operating_unit
              AND cqa.quota_id = p_quota_id
			  AND ccha.commission_header_id=ccla.commission_header_id  --added for defect#43473
			  AND NVL(CCHA.ATTRIBUTE97,'X') <> 'POSTED' --added for defect#43473

         GROUP BY ccla.credited_salesrep_id,
                  jrs.salesrep_number,
                  cqa.attribute1,
                  cqa.attribute2,
                  cqa.attribute3,
                  cqa.attribute4,
                  cqa.attribute5,
                  cqa.attribute6,
                  cqa.attribute7,
                  cqa.attribute8,
                  cqa.attribute9,
                  cqa.attribute10,
                  cqa.attribute11,
                  cqa.attribute12,
                  cqa.attribute13,
                  cqa.attribute14,
                  gll.currency_code,
                  gll.ledger_id
		HAVING DECODE(cqa.attribute1,'STANDARD',SUM (ccla.commission_amount) --Added as per defect#44638
		                             ,'STAT',1) <>0;

      cur_salesrep_quota_rec                  cur_salesrep_quota%ROWTYPE;
      lv_segment1                 VARCHAR2 (20);
      lv_segment2                 VARCHAR2 (20);
      lv_segment3                 VARCHAR2 (20);
      lv_segment4                 VARCHAR2 (20);
	  --lv_code_com                 VARCHAR2 (100);
	  lv_status                   NUMBER;
      lv_user_je_source_name      gl_je_sources.user_je_source_name%TYPE;
      lv_user_je_category_name    gl_je_categories.user_je_category_name%TYPE;
      lv_journal_type             cn_quotas_all.attribute1%TYPE;
      ln_val_code_comb_val        NUMBER;
	  lv_code_com    gl_code_combinations_kfv.concatenated_segments%TYPE  ;
	  ln_code_combination_id  gl_code_combinations.code_combination_id%TYPE;
	  lv_code_comb_flag           VARCHAR2 (3);
      ln_aapl_id                  fnd_application.application_id%TYPE;
      l_open_period               gl_period_statuses.end_date%TYPE;
	  l_acc_period                VARCHAR2(10);
      ex_error_found              EXCEPTION;
      ex_date_not_found           EXCEPTION;
      ex_not_data_found_cursor    EXCEPTION;
      ex_source_not_found         EXCEPTION;
      ex_category_not_found       EXCEPTION;
      ex_gl_cod_com_not_found     EXCEPTION;
      ex_journal_type_not_found   EXCEPTION;
      ex_source_invalid           EXCEPTION;
      ex_category_invalid         EXCEPTION;
      ex_journal_type_invalid     EXCEPTION;
      ex_no_data_process          EXCEPTION;
      ex_invalid_data_process     EXCEPTION;
	  ex_too_many_rows            EXCEPTION;
	  ex_code_comb                EXCEPTION;
      lv_err_flag                 VARCHAR2 (20)                        := 'N';
      lv_err_msg                  VARCHAR2 (4000)                      := NULL;
	  l_end_date                  DATE;
	  l_start_date                DATE;
	  l_acc_count                 NUMBER;
	  l_reference4                VARCHAR2(50);
	  ln_total_record                 NUMBER                        DEFAULT 0;
      ln_total_success_records        NUMBER                        DEFAULT 0;
      ln_total_failcust_validation    NUMBER                        DEFAULT 0;
      ln_total_errorcust_validation   NUMBER                        DEFAULT 0;
	  l_period_flag          VARCHAR2(3);


   BEGIN
      slc_cnext_write_log_p
         (gv_log,
             'Calling slc_cnext_oic_res_det_p procedure to fetch data related to OIC with quota_id '
          || p_quota_id
         );
		 p_total_count := 0;
         p_success_count := 0;
		 p_error_count  := 0;
		      slc_cnext_write_log_p (gv_log, 'Validating if source exist or not in GL ');

      --Validating if journal source exist in the GL
      BEGIN
         SELECT user_je_source_name
           INTO lv_user_je_source_name
           FROM gl_je_sources
          WHERE user_je_source_name = gv_journal_source;
      EXCEPTION
         WHEN NO_DATA_FOUND
         THEN
            RAISE ex_source_not_found;
         WHEN OTHERS
         THEN
            RAISE ex_source_invalid;
      END;
    --Commented as part of Defect#
     /* BEGIN
         SELECT   ccla.credited_salesrep_id, jrs.salesrep_number,
                  SUM (ccla.commission_amount), cqa.attribute1,
                  cqa.attribute2, cqa.attribute3, cqa.attribute4,
                  cqa.attribute5, cqa.attribute6,
                  cqa.attribute7, cqa.attribute8, cqa.attribute9,
                  cqa.attribute10, cqa.attribute11, cqa.attribute12,
                  cqa.attribute13, cqa.attribute14, gll.currency_code,
                  gll.ledger_id
             INTO p_resource_id, p_resource_name,
                  p_amount, p_journal_type,
                  p_debit_bal_segment, p_debit_location, p_debit_account,
                  p_debit_sub_account, p_credit_bal_segment,
                  p_credit_location, p_credit_account, p_credit_sub_account,
                  p_journal_category, p_tbd1, p_tbd2,
                  p_credit_tbd1, p_credit_tbd2, p_currency_code,
                  p_ledger_id
             FROM cn_commission_lines_all ccla,
                  jtf_rs_defresources_v jrd
                                       ---jtf_rs_defresources_tl to be checked
                                           ,
                  jtf_rs_salesreps jrs,
                  cn_quotas_all cqa,
                  hr_operating_units hou,
                  gl_ledgers gll
            WHERE hou.set_of_books_id = gll.ledger_id
              AND cqa.org_id = hou.organization_id
              AND jrd.hold_payment = 'N'
              AND jrs.salesrep_number = jrd.resource_name
              AND jrd.category = 'OTHER'            --need to ask from Animesh
              AND jrd.end_date_active IS NULL
                                     --need to ask from Divya/Animesh--sysdate
              AND ccla.quota_id = cqa.quota_id
              AND ccla.credited_salesrep_id = jrs.salesrep_id
              AND ccla.status = gv_status
              AND ccla.processed_period_id =
                                NVL (p_period, NVL (p_start_date, p_end_date))
              AND cqa.org_id = p_operating_unit
              AND cqa.quota_id = p_quota_id
         GROUP BY ccla.credited_salesrep_id,
                  jrs.salesrep_number,
                  cqa.attribute1,
                  cqa.attribute2,
                  cqa.attribute3,
                  cqa.attribute4,
                  cqa.attribute5,
                  cqa.attribute6,
                  cqa.attribute7,
                  cqa.attribute8,
                  cqa.attribute9,
                  cqa.attribute10,
                  cqa.attribute11,
                  cqa.attribute12,
                  cqa.attribute13,
                  cqa.attribute14,
                  gll.currency_code,
                  gll.ledger_id;*/
		slc_cnext_write_log_p
         (gv_log,
             'Opening Cursor cur_salesrep_quota  '
         );

		OPEN cur_salesrep_quota;

        slc_cnext_write_log_p (gv_log, 'Entered in the cursor ');

        LOOP
         BEGIN
            FETCH cur_salesrep_quota
             INTO cur_salesrep_quota_rec;

            EXIT WHEN cur_salesrep_quota%NOTFOUND;

			ln_total_record := ln_total_record+1;
			p_total_count := ln_total_record;

        slc_cnext_write_log_p (gv_log,
                    'Fetched data related to OIC for Salesrep_id ' || cur_salesrep_quota_rec.resource_id
                   );

			slc_cnext_write_log_p (gv_log,'ln_total_record '||ln_total_record);
			slc_cnext_write_log_p (gv_log,'p_total_count '||p_total_count);
         l_reference4 :=
               gv_journal_source
            || cur_salesrep_quota_rec.journal_category
            || cur_salesrep_quota_rec.resource_name
            || cur_salesrep_quota_rec.currency_code;

    /*  EXCEPTION
         WHEN NO_DATA_FOUND
         THEN
            RAISE ex_no_data_process;
		WHEN TOO_MANY_ROWS
		THEN
		    RAISE ex_too_many_rows;
         WHEN OTHERS
         THEN
            RAISE ex_invalid_data_process;
      --slc_cnext_write_log_p(gv_log,'Error to OIC with quota id '||p_quota_id||' '||SQLERRM );
      END;*/


      --Fetching Oldest Open period in GL
	  --Fetching Oldest last day Open period in GL
	  --Commented as part of Defect#

     /* BEGIN
       SELECT LAST_DAY (
                   ADD_MONTHS ( (TRUNC (SYSDATE + 1) - 1 / (24 * 60 * 60)),
                               -1))
				--LAST_DAY (SYSDATE)

           INTO l_end_date
           FROM DUAL;

         slc_cnext_write_log_p (
            gv_log,
               'last day of accounting month is -  '
            || '-'
            || l_end_date);
      EXCEPTION
         WHEN OTHERS
         THEN
            slc_cnext_write_log_p (
               gv_log,
                  'Error in fetching the last day of accounting month '
               || SQLERRM);
      END;
	  -- FETCHING FIRST DAY OF MONTH
	  BEGIN
         SELECT TO_DATE (TRUNC (SYSDATE, 'MM'),
                         'dd-mon-yyyy')
           INTO l_start_date
           FROM DUAL;

         slc_cnext_write_log_p (gv_log,
                                   'last day of accounting month is -  '
                                || '-'
                                || l_start_date
                               );
      EXCEPTION
         WHEN OTHERS
         THEN
            slc_cnext_write_log_p
                    (gv_log,
                        'Error in fetching the last day of accounting month '
                     || SQLERRM
                    );
      END;

	  --Fetching Oldest Open period in GL

	  SELECT TO_CHAR(l_end_date,'MON-YY')
	  INTO ld_acc_date
	  FROM DUAL;

	  slc_cnext_write_log_p (
            gv_log,
               'period name of accounting month is -  '
            || '-'
            || ld_acc_date);

      BEGIN
         SELECT 1
           INTO l_acc_count
           FROM gl_period_statuses ps, gl_ledgers gle, fnd_application_vl fnd
          WHERE gle.ledger_id = ps.ledger_id
            AND fnd.application_id = ps.application_id
            AND ps.adjustment_period_flag = gv_adjustment_flag
            AND ps.closing_status = gv_period_status
            AND fnd.product_code = gv_product_code
			AND ps.end_date                  <
              (SELECT min(gps.start_date)
              FROM gl_period_statuses gps
              WHERE gps.closing_status='F'
              AND gps.ledger_id       =gle.ledger_id
              AND gps.application_id  =gps.application_id
              )
            AND TO_DATE(ps.end_date,'dd-mon-yyyy')   = TO_DATE(l_end_date,'dd-mon-yyyy')
			AND TO_DATE(ps.start_date,'dd-mon-yyyy') = TO_DATE(l_start_date,'dd-mon-yyyy')
            AND gle.NAME = gv_access_set_name;
      EXCEPTION
	     WHEN NO_DATA_FOUND
         THEN
            slc_cnext_write_log_p (gv_log, 'Open period does not exist');
            RAISE ex_date_not_found;
         WHEN OTHERS
         THEN
            slc_cnext_write_log_p (gv_log, 'Error in fetching Open period');
            RAISE ex_date_not_found;
      END;*/


	  --Added logic to derive open date as part of defect#
	   --Fetching Oldest Open period in GL
	  slc_cnext_write_log_p (gv_log, 'Fetching Open date in GL ');
	  BEGIN
           SELECT ADD_MONTHS(min(gps.end_date),-1),to_char(ADD_MONTHS(min(gps.end_date),-1),'MON-YY')
	       INTO l_open_period,l_acc_period
	       FROM gl_period_statuses gps,
		   fnd_application fnda
	       WHERE  fnda.application_short_name = gc_app_short_name
			AND fnda.application_id = gps.application_id
	        AND gps.closing_status = 'F'
	        AND gps.ledger_id= cur_salesrep_quota_rec.ledge_id;

			SELECT 'Y'
			INTO l_period_flag
			FROM gl_period_statuses gps,
			fnd_application fnda
			WHERE
			 fnda.application_short_name = gc_app_short_name
			AND gps.set_of_books_id = cur_salesrep_quota_rec.ledge_id
			AND fnda.application_id = gps.application_id
	        AND gps.closing_status = 'O'
			AND period_name = l_acc_period;

			IF l_period_flag <> 'Y'
			THEN
			p_err_flag := 'Y';
			p_err_msg := 'The Accounting period '||l_acc_period||' is closed';
			END IF;

			EXCEPTION
			WHEN NO_DATA_FOUND THEN
			lv_err_flag := 'Y';
			lv_err_msg := 'The Accounting period '||l_acc_period||' is closed';
			RAISE ex_date_not_found;
			WHEN OTHERS THEN
			lv_err_flag := 'Y';
            lv_err_msg := 'Error while fetching the end date  '||SQLERRM;
			RAISE ex_date_not_found;

		END;

		slc_cnext_write_log_p (gv_log, 'Open period in GL '||l_open_period);


      --Validating if journal category exist in the GL
      slc_cnext_write_log_p (gv_log, 'Validating if category exist ');

      BEGIN
         SELECT user_je_category_name
           INTO lv_user_je_category_name
           FROM gl_je_categories
          WHERE user_je_category_name = cur_salesrep_quota_rec.journal_category;
      EXCEPTION
         WHEN NO_DATA_FOUND
         THEN
            RAISE ex_category_not_found;
         WHEN OTHERS
         THEN
            RAISE ex_category_invalid;
      END;

      --Validating if journal type exist in the system
      slc_cnext_write_log_p (gv_log, 'Validating if journal type exist ');

      BEGIN
         SELECT cqa.attribute1
           INTO lv_journal_type
           FROM cn_quotas_all cqa
          WHERE cqa.quota_id = p_quota_id;
      EXCEPTION
         WHEN NO_DATA_FOUND
         THEN
            RAISE ex_journal_type_not_found;
         WHEN OTHERS
         THEN
            RAISE ex_journal_type_invalid;
      END;



         IF cur_salesrep_quota_rec.journal_type IS NULL
         THEN
            lv_err_flag := 'Y';
            lv_err_msg :=
                  'Journal type does not have value for Salesrep_id ' || cur_salesrep_quota_rec.resource_id;
         ELSIF cur_salesrep_quota_rec.journal_type = 'STAT'
         THEN

            slc_cnext_write_log_p (gv_log,
                       'Fetching code combinations for Journal type STAT '
                      );
            slc_cnext_write_log_p (gv_log,
                       'Fetching balancing segment for Journal type STAT '
                      );
			BEGIN
            slc_cnext_balanc_seg_p (cur_salesrep_quota_rec.debit_bal_segment,
                                    cur_salesrep_quota_rec.resource_name,
                                    lv_segment1,
                                    lv_err_flag,
                                    lv_err_msg
                                   );
					slc_cnext_write_log_p (gv_log,'lv_segment1 :'||lv_segment1);
			EXCEPTION
			WHEN OTHERS
			THEN
			lv_err_flag := 'Y';
            lv_err_msg :=
                  'Error while calling slc_cnext_balanc_seg_p ';
			END;

            IF lv_err_flag = 'Y'
            THEN
               RAISE ex_error_found;
            END IF;

            slc_cnext_write_log_p (gv_log,
                       'Fetching location segment for Journal type STAT '
                      );
			BEGIN
            slc_cnext_loc_seg_p (cur_salesrep_quota_rec.debit_location,
                                 cur_salesrep_quota_rec.resource_name,
                                 cur_salesrep_quota_rec.currency_code,
                                 p_quota_id,
                                 lv_segment2,
                                 lv_err_flag,
                                 lv_err_msg
                                );
					slc_cnext_write_log_p (gv_log,'lv_segment2 :'||lv_segment2);
			EXCEPTION
			WHEN OTHERS
			THEN
			lv_err_flag := 'Y';
            lv_err_msg :=
                  'Error while calling slc_cnext_loc_seg_p ';
			END;
            IF lv_err_flag = 'Y'
            THEN
               RAISE ex_error_found;
            END IF;

            lv_segment3 := cur_salesrep_quota_rec.debit_account;
            lv_segment4 := cur_salesrep_quota_rec.debit_sub_account;
			slc_cnext_write_log_p (gv_log,'lv_segment3 :'||lv_segment3);
			slc_cnext_write_log_p (gv_log,'lv_segment4 :'||lv_segment4);
            --Validating code combination whether existing in the system
            slc_cnext_write_log_p (gv_log,
                       'Validating code combination for Journal type STAT '
                      );
				slc_cnext_write_log_p(gv_log,'Test3 :'||lv_segment1||'.'||lv_segment2||'.'||lv_segment3||'.'||lv_segment4||'.'||cur_salesrep_quota_rec.tbd1||'.'||cur_salesrep_quota_rec.tbd2);

			lv_code_com := lv_segment1||'.'||lv_segment2||'.'||lv_segment3||'.'||lv_segment4||'.'||cur_salesrep_quota_rec.tbd1||'.'||cur_salesrep_quota_rec.tbd2;

			 slc_cnext_write_log_p (gv_log,'Validating code combination '||lv_code_com);

                     ln_code_combination_id :=
                        get_val_code_comb_id_f
                                        (lv_segment1,
                                         lv_segment2,
                                         lv_segment3,
                                         lv_segment4,
                                         cur_salesrep_quota_rec.tbd1,
                                         cur_salesrep_quota_rec.tbd2,
                                         lv_err_msg
                                        );

                     IF (ln_code_combination_id = 0)
                     THEN
					    lv_code_comb_flag := 'Y';
						lv_err_flag := 'Y';
                        lv_err_msg :=
                              'Invalid Code combination '
                           || lv_code_com
                           || ' Error: '
                           || lv_err_msg;
                        RAISE ex_code_comb;
                     END IF;

				/*IF lv_err_flag = 'Y' THEN
				    slc_cnext_write_log_p(gv_log,'STAT lv_err_msg :'||lv_err_msg);
					RAISE ex_error_found;
				END IF;*/



            --inserting required fields in GL_INTERFACE table
            slc_cnext_write_log_p
               (gv_log,
                'Inserting CREDIT/DEBIT row for Journal type STAT in GL_INTERFACE table '
               );
			BEGIN
            slc_cnext_jour_ent_ins_p (gv_journal_source,
                                      cur_salesrep_quota_rec.journal_category,
                                      cur_salesrep_quota_rec.ledge_id,
                                    --  cur_salesrep_quota_rec.currency_code, --commented for defect#43426
									  'STAT', 								  --added for defect#43426
                                      l_open_period,
                                      cur_salesrep_quota_rec.amount,
                                      NULL,       --Added for defect#44653
                                      lv_segment1,
                                      lv_segment2,
                                      lv_segment3,
                                      lv_segment4,
                                      cur_salesrep_quota_rec.tbd1,
                                      cur_salesrep_quota_rec.tbd2,
                                      gv_journal_source,
                                      l_reference4,
                                      p_err_flag,
                                      p_err_msg
                                     );
					COMMIT;
					ln_total_success_records := ln_total_success_records+1;

			EXCEPTION
			WHEN OTHERS
			THEN
			lv_err_flag := 'Y';
            lv_err_msg :=
                  'Error while calling slc_cnext_jour_ent_ins_p ';
			END;
            slc_cnext_write_log_p (gv_log,
                       'rows created in GL_INTERFACE ' || SQL%ROWCOUNT
                      );

            IF lv_err_flag = 'Y'
            THEN
               RAISE ex_error_found;
            END IF;
         ELSIF cur_salesrep_quota_rec.journal_type = 'STANDARD'
         THEN
            slc_cnext_write_log_p
                     (gv_log,
                      'Fetching code combinations for Journal type STANDARD '
                     );
            --For credit type account
            slc_cnext_write_log_p
               (gv_log,
                'Fetching code combinations for Journal type STANDARD and Account type CREDIT'
               );
			BEGIN
            slc_cnext_balanc_seg_p (cur_salesrep_quota_rec.credit_bal_segment,
                                    cur_salesrep_quota_rec.resource_name,
                                    lv_segment1,
                                    lv_err_flag,
                                    lv_err_msg
                                   );
			EXCEPTION
			WHEN OTHERS
			THEN
			lv_err_flag := 'Y';
            lv_err_msg :=
                  'Error while calling slc_cnext_balanc_seg_p ';
			END;
            IF lv_err_flag = 'Y'
            THEN
               RAISE ex_error_found;
            END IF;

			BEGIN
            slc_cnext_loc_seg_p (cur_salesrep_quota_rec.credit_location,
                                 cur_salesrep_quota_rec.resource_name,
                                 cur_salesrep_quota_rec.currency_code,
                                 p_quota_id,
                                 lv_segment2,
                                 lv_err_flag,
                                 lv_err_msg
                                );
			EXCEPTION
			WHEN OTHERS
			THEN
			lv_err_flag := 'Y';
            lv_err_msg :=
                  'Error while calling slc_cnext_loc_seg_p ';
			END;
            IF lv_err_flag = 'Y'
            THEN
               RAISE ex_error_found;
            END IF;

            lv_segment3 := cur_salesrep_quota_rec.credit_account;
            lv_segment4 := cur_salesrep_quota_rec.credit_sub_account;
            --Validating code combination whether existing in the system
			--lv_code_com := lv_segment1||'.'||lv_segment2||'.'||lv_segment3||'.'||lv_segment4||'.'||cur_salesrep_quota_rec.credit_tbd1||'.'||cur_salesrep_quota_rec.credit_tbd2;
			lv_code_com := lv_segment1||'.'||lv_segment2||'.'||lv_segment3||'.'||lv_segment4||'.'||cur_salesrep_quota_rec.tbd1||'.'||cur_salesrep_quota_rec.tbd2;

			 slc_cnext_write_log_p
               (gv_log,'Validating code combination '||lv_code_com);

                     ln_code_combination_id :=
                        get_val_code_comb_id_f
                                        (lv_segment1,
                                         lv_segment2,
                                         lv_segment3,
                                         lv_segment4,
                                         cur_salesrep_quota_rec.tbd1,
                                         cur_salesrep_quota_rec.tbd2,
                                         lv_err_msg
                                        );

                     IF (ln_code_combination_id = 0)
                     THEN
					    lv_code_comb_flag := 'Y';
						lv_err_flag := 'Y';
                        lv_err_msg :=
                              'Invalid Code combination '
                           || lv_code_com
                           || ' Error: '
                           || lv_err_msg;
                        RAISE ex_code_comb;
                     END IF;
				 /* IF lv_err_flag = 'Y' THEN
					RAISE ex_error_found;
				  END IF;*/

            slc_cnext_write_log_p
               (gv_log,
                'Inserting CREDIT row for Journal type STANDARD in GL_INTERFACE table'
               );
			BEGIN
            slc_cnext_jour_ent_ins_p (gv_journal_source,
                                      cur_salesrep_quota_rec.journal_category,
                                      cur_salesrep_quota_rec.ledge_id,
                                      cur_salesrep_quota_rec.currency_code,
                                      l_open_period,
                                      0.00,
                                      cur_salesrep_quota_rec.amount,
                                      lv_segment1,
                                      lv_segment2,
                                      lv_segment3,
                                      lv_segment4,
                                      cur_salesrep_quota_rec.credit_tbd1,
                                      cur_salesrep_quota_rec.credit_tbd1,
                                      gv_journal_source,
                                      l_reference4,
                                      lv_err_flag,
                                      lv_err_msg
                                     );
									 COMMIT;
			EXCEPTION
			WHEN OTHERS
			THEN
			lv_err_flag := 'Y';
            lv_err_msg :=
                  'Error while calling slc_cnext_jour_ent_ins_p ';
			END;
			slc_cnext_write_log_p (gv_log,
                       'rows created in GL_INTERFACE ' || SQL%ROWCOUNT
                      );
            IF lv_err_flag = 'Y'
            THEN
               RAISE ex_error_found;
            END IF;

            --For debit type account
            slc_cnext_write_log_p
               (gv_log,
                'Fetching code combinations for Journal type STANDARD and Account type DEBIT'
               );
			BEGIN
            slc_cnext_balanc_seg_p (cur_salesrep_quota_rec.debit_bal_segment,
                                    cur_salesrep_quota_rec.resource_name,
                                    lv_segment1,
                                    lv_err_flag,
                                    lv_err_msg
                                   );
			EXCEPTION
			WHEN OTHERS
			THEN
			lv_err_flag := 'Y';
            lv_err_msg :=
                  'Error while calling slc_cnext_balanc_seg_p ';
			END;
            IF lv_err_flag = 'Y'
            THEN
               RAISE ex_error_found;
            END IF;
            BEGIN
            slc_cnext_loc_seg_p (cur_salesrep_quota_rec.debit_location,
                                 cur_salesrep_quota_rec.resource_name,
                                 cur_salesrep_quota_rec.currency_code,
                                 p_quota_id,
                                 lv_segment2,
                                 lv_err_flag,
                                 lv_err_msg
                                );
			EXCEPTION
			WHEN OTHERS
			THEN
			lv_err_flag := 'Y';
            lv_err_msg :=
                  'Error while calling slc_cnext_loc_seg_p ';
			END;
            IF lv_err_flag = 'Y'
            THEN
               RAISE ex_error_found;
            END IF;

            lv_segment3 := cur_salesrep_quota_rec.debit_account;
            lv_segment4 := cur_salesrep_quota_rec.debit_sub_account;
            --Validating code combination whether existing in the system
			lv_code_com := lv_segment1||'.'||lv_segment2||'.'||lv_segment3||'.'||lv_segment4||'.'||cur_salesrep_quota_rec.tbd1||'.'||cur_salesrep_quota_rec.tbd2;

			 slc_cnext_write_log_p
               (gv_log,'Validating code combination '||lv_code_com);

                     ln_code_combination_id :=
                        get_val_code_comb_id_f
                                        (lv_segment1,
                                         lv_segment2,
                                         lv_segment3,
                                         lv_segment4,
                                         cur_salesrep_quota_rec.tbd1,
                                         cur_salesrep_quota_rec.tbd2,
                                         lv_err_msg
                                        );

                     IF (ln_code_combination_id = 0)
                     THEN
					    lv_code_comb_flag := 'Y';
						lv_err_flag := 'Y';
                        lv_err_msg :=
                              'Invalid Code combination '
                           || lv_code_com
                           || ' Error: '
                           || lv_err_msg;
                        RAISE ex_code_comb;
                     END IF;
				  /*IF lv_err_flag = 'Y' THEN
					RAISE ex_error_found;
				  END IF;*/

            slc_cnext_write_log_p
               (gv_log,
                'Inserting DEBIT row for Journal type STANDARD in GL_INTERFACE table'
               );
			 BEGIN
            slc_cnext_jour_ent_ins_p (gv_journal_source,
                                      cur_salesrep_quota_rec.journal_category,
                                      cur_salesrep_quota_rec.ledge_id,
                                      cur_salesrep_quota_rec.currency_code,
                                      l_open_period,
                                      cur_salesrep_quota_rec.amount,
                                      0.00,
                                      lv_segment1,
                                      lv_segment2,
                                      lv_segment3,
                                      lv_segment4,
                                      cur_salesrep_quota_rec.tbd1,
                                      cur_salesrep_quota_rec.tbd2,
                                      gv_journal_source,
                                      l_reference4,
                                      lv_err_flag,
                                      lv_err_msg
                                     );
				COMMIT;
				ln_total_success_records := ln_total_success_records+1;

			EXCEPTION
			WHEN OTHERS
			THEN
			lv_err_flag := 'Y';
            lv_err_msg :=
                  'Error while calling slc_cnext_jour_ent_ins_p ';
            slc_cnext_write_log_p (gv_log,
                       'rows created in GL_INTERFACE ' || SQL%ROWCOUNT
                      );
			END;

            IF lv_err_flag = 'Y'
            THEN
               RAISE ex_error_found;
            END IF;
         END IF;



         BEGIN

		 slc_cnext_write_log_p(gv_log,'UPDATING resource_name'||cur_salesrep_quota_rec.resource_name);
		    UPDATE cn_commission_headers_all
               SET attribute97 = 'POSTED',
			   last_update_date=SYSDATE                   --added for defect#43426
             WHERE commission_header_id IN (
                      SELECT ccla.commission_header_id
                        FROM cn_commission_lines_all ccla,
                             jtf_rs_defresources_v jrd
                                       ---jtf_rs_defresources_tl to be checked
                                                      ,
                             jtf_rs_salesreps jrs,
                             cn_quotas_all cqa,
                             hr_operating_units hou,
                             gl_ledgers gll
                       WHERE hou.set_of_books_id = gll.ledger_id
                         AND cqa.org_id = hou.organization_id
                         AND jrd.hold_payment = 'N'
                         AND jrs.salesrep_number = jrd.resource_name
                         AND jrd.category = 'OTHER' --need to ask from Animesh
                         --AND jrd.end_date_active IS NULL --commented as per defect 44985
                                     --need to ask from Divya/Animesh--sysdate
                         AND ccla.quota_id = cqa.quota_id
                         AND ccla.credited_salesrep_id = jrs.salesrep_id
						 AND jrs.salesrep_id = cur_salesrep_quota_rec.resource_id  ---- added below joins for defect#43473
                         AND ccla.status = gv_status
                         AND ccla.processed_period_id =
                                NVL (p_period, NVL (p_start_date, p_end_date))
                         AND cqa.org_id = p_operating_unit
                         AND cqa.quota_id = p_quota_id
						 )
						 AND ATTRIBUTE97 IS NULL
						 ;
					slc_cnext_write_log_p(gv_log,'Updating records'||'-'||SQL%ROWCOUNT);
         EXCEPTION
            WHEN OTHERS
            THEN
			   slc_cnext_write_log_p(gv_log,'Error in updating'||sqlcode||sqlerrm);
               lv_err_flag := 'Y';
               lv_err_msg :=
                     'Error While updating the status for quota id '||p_quota_id;
         END;
     /* EXCEPTION
         WHEN ex_gl_cod_com_not_found
         THEN
            p_err_flag := lv_err_flag;
            p_err_msg := lv_err_msg;

         WHEN OTHERS
         THEN
            p_err_flag := lv_err_flag;
            p_err_msg := lv_err_msg;
      END;*/

	  p_success_count := ln_total_success_records;
      p_err_flag := lv_err_flag;
      p_err_msg := lv_err_msg;
      slc_cnext_write_log_p(gv_log,'Ending slc_cnext_oic_res_det_p procedure to fetch data related to OIC with resource name '||
	  cur_salesrep_quota_rec.resource_name);



   EXCEPTION
      WHEN ex_no_data_process
      THEN
	     ln_total_failcust_validation := ln_total_failcust_validation+1;
		 p_error_count := ln_total_failcust_validation;
         p_err_flag := 'Y';
         p_err_msg := 'No data found for quota id ' || p_quota_id;
	  WHEN ex_too_many_rows
	  THEN
	     ln_total_failcust_validation := ln_total_failcust_validation+1;
		 p_error_count := ln_total_failcust_validation;
	     p_err_flag := 'Y';
         p_err_msg :=
               'Too many rows in fetching the data from OIC for quota id :'
            || p_quota_id;
      WHEN ex_invalid_data_process
      THEN
	    ln_total_failcust_validation := ln_total_failcust_validation+1;
		 p_error_count := ln_total_failcust_validation;
         p_err_flag := 'Y';
         p_err_msg :=
               'Unhandled exception in fetching the data from OIC for quota id :'
            || p_quota_id;
	  WHEN ex_code_comb
		 THEN
		 ROLLBACK;
		 ln_total_failcust_validation := ln_total_failcust_validation+1;
		   p_error_count := ln_total_failcust_validation;
          p_err_flag := lv_err_flag;
            p_err_msg := lv_err_msg;
			slc_cnext_write_log_p(gv_log,'In error count '||p_error_count);
			--slc_cnext_write_log_p(gv_out,p_err_msg);
	  WHEN ex_error_found
         THEN
		   ROLLBACK;
		   ln_total_failcust_validation := ln_total_failcust_validation+1;
		   p_error_count := ln_total_failcust_validation;
            p_err_flag := lv_err_flag;
            p_err_msg := lv_err_msg;
			slc_cnext_write_log_p(gv_log,'In error count '||p_error_count);
			slc_cnext_write_log_p(gv_log,p_err_msg);
			--slc_cnext_write_log_p(gv_out,p_err_msg);
      WHEN ex_category_invalid
      THEN
	     ln_total_failcust_validation := ln_total_failcust_validation+1;
		 p_error_count := ln_total_failcust_validation;
         p_err_flag := 'Y';
         p_err_msg :=
               'The Journal Category invalid :'
            || cur_salesrep_quota_rec.journal_category
            || ' for resource name '
            || cur_salesrep_quota_rec.resource_name
            || ' and quota id :'
            || p_quota_id;
			slc_cnext_write_log_p(gv_log,p_err_msg);
			slc_cnext_write_log_p(gv_out,p_err_msg);
      WHEN ex_journal_type_invalid
      THEN
	     ln_total_failcust_validation := ln_total_failcust_validation+1;
		 p_error_count := ln_total_failcust_validation;
         p_err_flag := 'Y';
         p_err_msg :=
               'The Journal type has more than one value  for resource name '
            || cur_salesrep_quota_rec.resource_name
            || ' and quota id :'
            || p_quota_id;
		 slc_cnext_write_log_p(gv_log,p_err_msg);
		slc_cnext_write_log_p(gv_out,p_err_msg);
      WHEN ex_journal_type_not_found
      THEN
	     ln_total_failcust_validation := ln_total_failcust_validation+1;
		 p_error_count := ln_total_failcust_validation;
         p_err_flag := 'Y';
         p_err_msg :=
               'The Journal Type does not exist in system :'
            || cur_salesrep_quota_rec.journal_type
            || ' for resource name '
            || cur_salesrep_quota_rec.resource_name
            || ' and quota id :'
            || p_quota_id;
         slc_cnext_write_log_p(gv_log,p_err_msg);
	     slc_cnext_write_log_p(gv_out,p_err_msg);
      WHEN ex_category_not_found
      THEN
	     ln_total_failcust_validation := ln_total_failcust_validation+1;
		 p_error_count := ln_total_failcust_validation;
         p_err_flag := 'Y';
         p_err_msg :=
               'The Journal Category name does not exist in system :'
            || cur_salesrep_quota_rec.journal_category
            || ' for resource name '
            || cur_salesrep_quota_rec.resource_name
            || ' and quota id :'
            || p_quota_id;
		slc_cnext_write_log_p(gv_log,p_err_msg);
			slc_cnext_write_log_p(gv_out,p_err_msg);
      WHEN ex_date_not_found
      THEN
	     ln_total_failcust_validation := ln_total_failcust_validation+1;
		 p_error_count := ln_total_failcust_validation;
         p_err_flag := lv_err_flag;
         p_err_msg := lv_err_msg;
		 slc_cnext_write_log_p(gv_log,p_err_msg);
		 slc_cnext_write_log_p(gv_out,p_err_msg);
      END;
	     IF lv_err_flag = 'Y'
         THEN
            ROLLBACK;
            slc_cnext_write_log_p (gv_out, 'Error message :' || lv_err_msg);
            populate_err_object
                            (p_in_batch_key            => gv_batch_key,
                             p_in_business_entity      => lv_business_entity_name,
                             p_in_process_id3          => NULL,
                             p_in_error_txt            => lv_err_msg,
                             p_in_request_id           => gn_request_id,
                             p_in_attribute1           =>    'Salesrep id:'
                                                          || cur_salesrep_quota_rec.resource_id
                            );
         ELSE
            COMMIT;
         END IF;

	END LOOP;

	EXCEPTION
	WHEN ex_source_invalid
      THEN
         p_err_flag := 'Y';
         p_err_msg :=
               'The SOURCE NAME invalid :'
            || gv_journal_source
            || ' for resource name '
            || cur_salesrep_quota_rec.resource_name
            || ' and quota id :'
            || p_quota_id;
			slc_cnext_write_log_p(gv_log,p_err_msg);
			slc_cnext_write_log_p(gv_out,p_err_msg);
	WHEN ex_source_not_found
      THEN
         p_err_flag := 'Y';
         p_err_msg :=
               'The Journal SOURCE NAME does not exist in system :'
            || gv_journal_source
            || ' for resource name '
            || cur_salesrep_quota_rec.resource_name
            || ' and quota id :'
            || p_quota_id;
			slc_cnext_write_log_p(gv_log,p_err_msg);
			slc_cnext_write_log_p(gv_out,p_err_msg);
	WHEN OTHERS
      THEN
         p_err_flag := 'Y';
         p_err_msg :=
               'Some other error in fetching the data related to OIC resource with  resource name '
            || cur_salesrep_quota_rec.resource_name;
		slc_cnext_write_log_p(gv_log,p_err_msg);
		slc_cnext_write_log_p(gv_out,p_err_msg);
   END slc_cnext_oic_res_det_p;
   
   --Added for 40% OIC object
   
    --===============================================================
-- Procedure Name  : slc_cnext_oic_res_det40_p
-- Description     : This  procedure called by the main Procedure
--                   to fetch data related to OIC and resource for 40%
-- Parameters      : 1. p_err_flag          (OUT)
--                   2. p_err_msg           (OUT)
--                   3. p_quota_id          (IN)
--                   4. p_period            (IN)
--                   5. p_operating_unit    (IN)
--                   6. p_start_date        (IN)
--                   7. p_end_date          (IN)
--                   8. p_total_count       (OUT)
--                   9. p_success_count     (OUT)
--                   10.p_success_count     (OUT)
   --===============================================================
   PROCEDURE slc_cnext_oic_res_det40_p (
      p_err_flag             OUT      VARCHAR2,
      p_err_msg              OUT      VARCHAR2,
      p_quota_id             IN       NUMBER,
      p_period               IN       NUMBER,
      p_operating_unit       IN       NUMBER,
      p_start_date           IN       NUMBER,
      p_end_date             IN       NUMBER,
	  p_total_count          OUT      NUMBER,
	  p_success_count        OUT      NUMBER,
	  p_error_count          OUT      NUMBER
   )
   IS
   --Added cursor to create journal entries on the basis of Salesrep_id as part of defect#
     CURSOR cur_salesrep_quota40
      IS
	  SELECT   ccla.credited_salesrep_id resource_id, jrs.salesrep_number resource_name,
                  SUM (ccla.commission_amount) amount, cqa.attribute1 journal_type,
                  cqa.attribute2 debit_bal_segment, cqa.attribute3 debit_location, cqa.attribute4 debit_account,
                  cqa.attribute5 debit_sub_account, cqa.attribute6 credit_bal_segment,
                  cqa.attribute7 credit_location, cqa.attribute8 credit_account, cqa.attribute9 credit_sub_account,
                  cqa.attribute10 journal_category, cqa.attribute11 tbd1, cqa.attribute12 tbd2,
                  cqa.attribute13 credit_tbd1, cqa.attribute14 credit_tbd2, gll.currency_code currency_code,
                  cqa.quota_group_code, --added by Anand
				  gll.ledger_id ledge_id
			--Commented as part of defect#
            /* INTO p_resource_id, p_resource_name,
                  p_amount, p_journal_type,
                  p_debit_bal_segment, p_debit_location, p_debit_account,
                  p_debit_sub_account, p_credit_bal_segment,
                  p_credit_location, p_credit_account, p_credit_sub_account,
                  p_journal_category, p_tbd1, p_tbd2,
                  p_credit_tbd1, p_credit_tbd2, p_currency_code,
                  p_ledger_id*/
             FROM cn_commission_lines_all ccla,
			     cn_commission_headers_all ccha, --added as per defect#43473--Commented for 40% changes
                  jtf_rs_defresources_v jrd,
                  jtf_rs_salesreps jrs,
                  cn_quotas_all cqa,
                  hr_operating_units hou,
                  gl_ledgers gll
            WHERE hou.set_of_books_id = gll.ledger_id
              AND cqa.org_id = hou.organization_id
              AND jrd.hold_payment = 'N'
              AND jrs.salesrep_number = jrd.resource_name
              AND jrd.category = 'OTHER'
             -- AND jrd.end_date_active IS NULL     --as per defect #44985
              AND ccla.quota_id = cqa.quota_id
              AND ccla.credited_salesrep_id = jrs.salesrep_id
              AND ccla.status = gv_status
              AND ccla.processed_period_id =
                                NVL (p_period, NVL (p_start_date, p_end_date))
              AND cqa.org_id = p_operating_unit
              AND cqa.quota_id = p_quota_id
			  
			  AND ccha.commission_header_id=ccla.commission_header_id  --added for defect#43473
			  AND NVL(CCHA.ATTRIBUTE97,'X') <> 'POSTED' --added for defect#43473
			 AND NVL(CCLA.ATTRIBUTE43,'X') <> 'POSTED' --added for 40% changes
         GROUP BY ccla.credited_salesrep_id,
                  jrs.salesrep_number,
                  cqa.attribute1,
                  cqa.attribute2,
                  cqa.attribute3,
                  cqa.attribute4,
                  cqa.attribute5,
                  cqa.attribute6,
                  cqa.attribute7,
                  cqa.attribute8,
                  cqa.attribute9,
                  cqa.attribute10,
                  cqa.attribute11,
                  cqa.attribute12,
                  cqa.attribute13,
                  cqa.attribute14,
                  gll.currency_code,
				  cqa.quota_group_code, --added by Anand
                  gll.ledger_id
		HAVING DECODE(cqa.attribute1,'STANDARD',SUM (ccla.commission_amount) --Added as per defect#44638
		                             ,'STAT',1) <>0;
									 
		CURSOR cur_header_id(p_resource_id IN NUMBER)
        IS		
		SELECT ccla.commission_header_id
                        FROM cn_commission_lines_all ccla,
                             jtf_rs_defresources_v jrd
                                       ---jtf_rs_defresources_tl to be checked
                                                      ,
                             jtf_rs_salesreps jrs,
                             cn_quotas_all cqa,
                             hr_operating_units hou,
                             gl_ledgers gll
                       WHERE hou.set_of_books_id = gll.ledger_id
                         AND cqa.org_id = hou.organization_id
                         AND jrd.hold_payment = 'N'
                         AND jrs.salesrep_number = jrd.resource_name
                         AND jrd.category = 'OTHER' --need to ask from Animesh
                         --AND jrd.end_date_active IS NULL --commented as per defect 44985
                                     --need to ask from Divya/Animesh--sysdate
                         AND ccla.quota_id = cqa.quota_id
                         AND ccla.credited_salesrep_id = jrs.salesrep_id
						 AND jrs.salesrep_id = p_resource_id  ---- added below joins for defect#43473
                         AND ccla.status = gv_status
                         AND ccla.processed_period_id =
                                NVL (p_period, NVL (p_start_date, p_end_date))
                         AND cqa.org_id = p_operating_unit
                         AND cqa.quota_id = p_quota_id
						 AND ccla.attribute43 = 'POSTED';

      cur_salesrep_quota40_rec           cur_salesrep_quota40%ROWTYPE;
	  cur_header_id_rec                  cur_header_id%ROWTYPE;
      lv_segment1                 VARCHAR2 (20);
      lv_segment2                 VARCHAR2 (20);
      lv_segment3                 VARCHAR2 (20);
      lv_segment4                 VARCHAR2 (20);
	  --lv_code_com                 VARCHAR2 (100);
	  lv_status                   NUMBER;
      lv_user_je_source_name      gl_je_sources.user_je_source_name%TYPE;
      lv_user_je_category_name    gl_je_categories.user_je_category_name%TYPE;
      lv_journal_type             cn_quotas_all.attribute1%TYPE;
      ln_val_code_comb_val        NUMBER;
	  lv_code_com    gl_code_combinations_kfv.concatenated_segments%TYPE  ;
	  ln_code_combination_id  gl_code_combinations.code_combination_id%TYPE;
	  lv_code_comb_flag           VARCHAR2 (3);
      ln_aapl_id                  fnd_application.application_id%TYPE;
      l_open_period               gl_period_statuses.end_date%TYPE;
	  l_acc_period                VARCHAR2(10);
      ex_error_found              EXCEPTION;
      ex_date_not_found           EXCEPTION;
      ex_not_data_found_cursor    EXCEPTION;
      ex_source_not_found         EXCEPTION;
      ex_category_not_found       EXCEPTION;
      ex_gl_cod_com_not_found     EXCEPTION;
      ex_journal_type_not_found   EXCEPTION;
      ex_source_invalid           EXCEPTION;
      ex_category_invalid         EXCEPTION;
      ex_journal_type_invalid     EXCEPTION;
      ex_no_data_process          EXCEPTION;
      ex_invalid_data_process     EXCEPTION;
	  ex_too_many_rows            EXCEPTION;
	  ex_code_comb                EXCEPTION;
      lv_err_flag                 VARCHAR2 (20)                        := 'N';
      lv_err_msg                  VARCHAR2 (4000)                      := NULL;
	  l_end_date                  DATE;
	  l_start_date                DATE;
	  l_acc_count                 NUMBER;
	  l_reference4                VARCHAR2(50);
	  ln_total_record                 NUMBER                        DEFAULT 0;
      ln_total_success_records        NUMBER                        DEFAULT 0;
      ln_total_failcust_validation    NUMBER                        DEFAULT 0;
      ln_total_errorcust_validation   NUMBER                        DEFAULT 0;
	  l_period_flag          VARCHAR2(3);
	  ln_line_count                 NUMBER                          DEFAULT 0;


   BEGIN
      slc_cnext_write_log_p
         (gv_log,
             'Calling slc_cnext_oic_res_det40_p procedure to fetch data related to OIC with quota_id '
          || p_quota_id
         );
		 p_total_count := 0;
         p_success_count := 0;
		 p_error_count  := 0;
		      slc_cnext_write_log_p (gv_log, 'Validating if source exist or not in GL ');

      --Validating if journal source exist in the GL
      BEGIN
         SELECT user_je_source_name
           INTO lv_user_je_source_name
           FROM gl_je_sources
          WHERE user_je_source_name = gv_journal_source;
      EXCEPTION
         WHEN NO_DATA_FOUND
         THEN
            RAISE ex_source_not_found;
         WHEN OTHERS
         THEN
            RAISE ex_source_invalid;
      END;

		slc_cnext_write_log_p
         (gv_log,
             'Opening Cursor cur_salesrep_quota40  '
         );

		OPEN cur_salesrep_quota40;

        slc_cnext_write_log_p (gv_log, 'Entered in the cursor ');

        LOOP
		 
         BEGIN
            FETCH cur_salesrep_quota40
             INTO cur_salesrep_quota40_rec;

            EXIT WHEN cur_salesrep_quota40%NOTFOUND;
			
			--Changes for v1.8
			SAVEPOINT oic_savepoint;

			ln_total_record := ln_total_record+1;
			p_total_count := ln_total_record;

        slc_cnext_write_log_p (gv_log,
                    'Fetched data related to OIC for Salesrep_id ' || cur_salesrep_quota40_rec.resource_id
                   );

			slc_cnext_write_log_p (gv_log,'ln_total_record '||ln_total_record);
			slc_cnext_write_log_p (gv_log,'p_total_count '||p_total_count);
         l_reference4 :=
               gv_journal_source
            || cur_salesrep_quota40_rec.journal_category
            || cur_salesrep_quota40_rec.resource_name
            || cur_salesrep_quota40_rec.currency_code;



	  --Added logic to derive open date as part of defect#
	   --Fetching Oldest Open period in GL
	  slc_cnext_write_log_p (gv_log, 'Fetching Open date in GL ');
	  BEGIN
           SELECT ADD_MONTHS(min(gps.end_date),-1),to_char(ADD_MONTHS(min(gps.end_date),-1),'MON-YY')
	       INTO l_open_period,l_acc_period
	       FROM gl_period_statuses gps,
		   fnd_application fnda
	       WHERE  fnda.application_short_name = gc_app_short_name
			AND fnda.application_id = gps.application_id
	        AND gps.closing_status = 'F'
	        AND gps.ledger_id= cur_salesrep_quota40_rec.ledge_id;

			SELECT 'Y'
			INTO l_period_flag
			FROM gl_period_statuses gps,
			fnd_application fnda
			WHERE
			 fnda.application_short_name = gc_app_short_name
			AND gps.set_of_books_id = cur_salesrep_quota40_rec.ledge_id
			AND fnda.application_id = gps.application_id
	        AND gps.closing_status = 'O'
			AND period_name = l_acc_period;

			IF l_period_flag <> 'Y'
			THEN
			p_err_flag := 'Y';
			p_err_msg := 'The Accounting period '||l_acc_period||' is closed';
			END IF;

			EXCEPTION
			WHEN NO_DATA_FOUND THEN
			lv_err_flag := 'Y';
			lv_err_msg := 'The Accounting period '||l_acc_period||' is closed';
			RAISE ex_date_not_found;
			WHEN OTHERS THEN
			lv_err_flag := 'Y';
            lv_err_msg := 'Error while fetching the end date  '||SQLERRM;
			RAISE ex_date_not_found;

		END;

		slc_cnext_write_log_p (gv_log, 'Open period in GL '||l_open_period);


      --Validating if journal category exist in the GL
      slc_cnext_write_log_p (gv_log, 'Validating if category exist ');

      BEGIN
         SELECT user_je_category_name
           INTO lv_user_je_category_name
           FROM gl_je_categories
          WHERE user_je_category_name = cur_salesrep_quota40_rec.journal_category;
      EXCEPTION
         WHEN NO_DATA_FOUND
         THEN
            RAISE ex_category_not_found;
         WHEN OTHERS
         THEN
            RAISE ex_category_invalid;
      END;

      --Validating if journal type exist in the system
      slc_cnext_write_log_p (gv_log, 'Validating if journal type exist ');

      BEGIN
         SELECT cqa.attribute1
           INTO lv_journal_type
           FROM cn_quotas_all cqa
          WHERE cqa.quota_id = p_quota_id;
      EXCEPTION
         WHEN NO_DATA_FOUND
         THEN
            RAISE ex_journal_type_not_found;
         WHEN OTHERS
         THEN
            RAISE ex_journal_type_invalid;
      END;


		
         IF cur_salesrep_quota40_rec.journal_type IS NULL
         THEN
            lv_err_flag := 'Y';
            lv_err_msg :=
                  'Journal type does not have value for Salesrep_id ' || cur_salesrep_quota40_rec.resource_id;
         ELSIF cur_salesrep_quota40_rec.journal_type = 'STAT'
         THEN

            slc_cnext_write_log_p (gv_log,
                       'Fetching code combinations for Journal type STAT '
                      );
            slc_cnext_write_log_p (gv_log,
                       'Fetching balancing segment for Journal type STAT '
                      );
			BEGIN
            slc_cnext_balanc_seg_p (cur_salesrep_quota40_rec.debit_bal_segment,
                                    cur_salesrep_quota40_rec.resource_name,
                                    lv_segment1,
                                    lv_err_flag,
                                    lv_err_msg
                                   );
					slc_cnext_write_log_p (gv_log,'lv_segment1 :'||lv_segment1);
			EXCEPTION
			WHEN OTHERS
			THEN
			lv_err_flag := 'Y';
            lv_err_msg :=
                  'Error while calling slc_cnext_balanc_seg_p ';
			END;

            IF lv_err_flag = 'Y'
            THEN
               RAISE ex_error_found;
            END IF;

            slc_cnext_write_log_p (gv_log,
                       'Fetching location segment for Journal type STAT '
                      );
			BEGIN
            slc_cnext_loc_seg_p (cur_salesrep_quota40_rec.debit_location,
                                 cur_salesrep_quota40_rec.resource_name,
                                 cur_salesrep_quota40_rec.currency_code,
                                 p_quota_id,
                                 lv_segment2,
                                 lv_err_flag,
                                 lv_err_msg
                                );
					slc_cnext_write_log_p (gv_log,'lv_segment2 :'||lv_segment2);
			EXCEPTION
			WHEN OTHERS
			THEN
			lv_err_flag := 'Y';
            lv_err_msg :=
                  'Error while calling slc_cnext_loc_seg_p ';
			END;
            IF lv_err_flag = 'Y'
            THEN
               RAISE ex_error_found;
            END IF;

            lv_segment3 := cur_salesrep_quota40_rec.debit_account;
            lv_segment4 := cur_salesrep_quota40_rec.debit_sub_account;
			slc_cnext_write_log_p (gv_log,'lv_segment3 :'||lv_segment3);
			slc_cnext_write_log_p (gv_log,'lv_segment4 :'||lv_segment4);
            --Validating code combination whether existing in the system
            slc_cnext_write_log_p (gv_log,
                       'Validating code combination for Journal type STAT '
                      );
				slc_cnext_write_log_p(gv_log,'Test3 :'||lv_segment1||'.'||lv_segment2||'.'||lv_segment3||'.'||lv_segment4||'.'||cur_salesrep_quota40_rec.tbd1||'.'||cur_salesrep_quota40_rec.tbd2);

			lv_code_com := lv_segment1||'.'||lv_segment2||'.'||lv_segment3||'.'||lv_segment4||'.'||cur_salesrep_quota40_rec.tbd1||'.'||cur_salesrep_quota40_rec.tbd2;

			 slc_cnext_write_log_p (gv_log,'Validating code combination '||lv_code_com);

                     ln_code_combination_id :=
                        get_val_code_comb_id_f
                                        (lv_segment1,
                                         lv_segment2,
                                         lv_segment3,
                                         lv_segment4,
                                         cur_salesrep_quota40_rec.tbd1,
                                         cur_salesrep_quota40_rec.tbd2,
                                         lv_err_msg
                                        );

                     IF (ln_code_combination_id = 0)
                     THEN
					    lv_code_comb_flag := 'Y';
						lv_err_flag := 'Y';
                        lv_err_msg :=
                              'Invalid Code combination '
                           || lv_code_com
                           || ' Error: '
                           || lv_err_msg;
                        RAISE ex_code_comb;
                     END IF;


            --inserting required fields in GL_INTERFACE table
            slc_cnext_write_log_p
               (gv_log,
                'Inserting CREDIT/DEBIT row for Journal type STAT in GL_INTERFACE table '
               );
			BEGIN
            slc_cnext_jour_ent_ins_p (gv_journal_source,
                                      cur_salesrep_quota40_rec.journal_category,
                                      cur_salesrep_quota40_rec.ledge_id,
                                    --  cur_salesrep_quota40_rec.currency_code, --commented for defect#43426
									  'STAT', 								  --added for defect#43426
                                      l_open_period,
                                      cur_salesrep_quota40_rec.amount,
                                      NULL,       --Added for defect#44653
                                      lv_segment1,
                                      lv_segment2,
                                      lv_segment3,
                                      lv_segment4,
                                      cur_salesrep_quota40_rec.tbd1,
                                      cur_salesrep_quota40_rec.tbd2,
                                      gv_journal_source,
                                      l_reference4,
                                      p_err_flag,
                                      p_err_msg
                                     );
					--Changes for v1.8 Commented the COMMIT and COMMIT will happen at the end.
					--COMMIT;
					ln_total_success_records := ln_total_success_records+1;

			EXCEPTION
			WHEN OTHERS
			THEN
			lv_err_flag := 'Y';
            lv_err_msg :=
                  'Error while calling slc_cnext_jour_ent_ins_p ';
			END;
            slc_cnext_write_log_p (gv_log,
                       'rows created in GL_INTERFACE ' || SQL%ROWCOUNT
                      );

            IF lv_err_flag = 'Y'
            THEN
               RAISE ex_error_found;
            END IF;
         ELSIF cur_salesrep_quota40_rec.journal_type = 'STANDARD'
         THEN
            slc_cnext_write_log_p
                     (gv_log,
                      'Fetching code combinations for Journal type STANDARD '
                     );
            --For credit type account
            slc_cnext_write_log_p
               (gv_log,
                'Fetching code combinations for Journal type STANDARD and Account type CREDIT'
               );
			BEGIN
            slc_cnext_balanc_seg_p (cur_salesrep_quota40_rec.credit_bal_segment,
                                    cur_salesrep_quota40_rec.resource_name,
                                    lv_segment1,
                                    lv_err_flag,
                                    lv_err_msg
                                   );
			EXCEPTION
			WHEN OTHERS
			THEN
			lv_err_flag := 'Y';
            lv_err_msg :=
                  'Error while calling slc_cnext_balanc_seg_p ';
			END;
            IF lv_err_flag = 'Y'
            THEN
               RAISE ex_error_found;
            END IF;

			BEGIN
            slc_cnext_loc_seg_p (cur_salesrep_quota40_rec.credit_location,
                                 cur_salesrep_quota40_rec.resource_name,
                                 cur_salesrep_quota40_rec.currency_code,
                                 p_quota_id,
                                 lv_segment2,
                                 lv_err_flag,
                                 lv_err_msg
                                );
			EXCEPTION
			WHEN OTHERS
			THEN
			lv_err_flag := 'Y';
            lv_err_msg :=
                  'Error while calling slc_cnext_loc_seg_p ';
			END;
            IF lv_err_flag = 'Y'
            THEN
               RAISE ex_error_found;
            END IF;

            lv_segment3 := cur_salesrep_quota40_rec.credit_account;
            lv_segment4 := cur_salesrep_quota40_rec.credit_sub_account;
            --Validating code combination whether existing in the system
			--lv_code_com := lv_segment1||'.'||lv_segment2||'.'||lv_segment3||'.'||lv_segment4||'.'||cur_salesrep_quota40_rec.credit_tbd1||'.'||cur_salesrep_quota40_rec.credit_tbd2;
			lv_code_com := lv_segment1||'.'||lv_segment2||'.'||lv_segment3||'.'||lv_segment4||'.'||cur_salesrep_quota40_rec.tbd1||'.'||cur_salesrep_quota40_rec.tbd2;

			 slc_cnext_write_log_p
               (gv_log,'Validating code combination '||lv_code_com);

                     ln_code_combination_id :=
                        get_val_code_comb_id_f
                                        (lv_segment1,
                                         lv_segment2,
                                         lv_segment3,
                                         lv_segment4,
                                         cur_salesrep_quota40_rec.tbd1,
                                         cur_salesrep_quota40_rec.tbd2,
                                         lv_err_msg
                                        );

                     IF (ln_code_combination_id = 0)
                     THEN
					    lv_code_comb_flag := 'Y';
						lv_err_flag := 'Y';
                        lv_err_msg :=
                              'Invalid Code combination '
                           || lv_code_com
                           || ' Error: '
                           || lv_err_msg;
                        RAISE ex_code_comb;
                     END IF;

            slc_cnext_write_log_p
               (gv_log,
                'Inserting CREDIT row for Journal type STANDARD in GL_INTERFACE table'
               );
			BEGIN
            slc_cnext_jour_ent_ins_p (gv_journal_source,
                                      cur_salesrep_quota40_rec.journal_category,
                                      cur_salesrep_quota40_rec.ledge_id,
                                      cur_salesrep_quota40_rec.currency_code,
                                      l_open_period,
                                      0.00,
                                      cur_salesrep_quota40_rec.amount,
                                      lv_segment1,
                                      lv_segment2,
                                      lv_segment3,
                                      lv_segment4,
                                      cur_salesrep_quota40_rec.credit_tbd1,
                                      cur_salesrep_quota40_rec.credit_tbd1,
                                      gv_journal_source,
                                      l_reference4,
                                      lv_err_flag,
                                      lv_err_msg
                                     );
									 --Changes for v1.8 Commented the COMMIT and COMMIT will happen at the end.
									 --COMMIT;
			EXCEPTION
			WHEN OTHERS
			THEN
			lv_err_flag := 'Y';
            lv_err_msg :=
                  'Error while calling slc_cnext_jour_ent_ins_p ';
			END;
			slc_cnext_write_log_p (gv_log,
                       'rows created in GL_INTERFACE ' || SQL%ROWCOUNT
                      );
            IF lv_err_flag = 'Y'
            THEN
               RAISE ex_error_found;
            END IF;

            --For debit type account
            slc_cnext_write_log_p
               (gv_log,
                'Fetching code combinations for Journal type STANDARD and Account type DEBIT'
               );
			BEGIN
            slc_cnext_balanc_seg_p (cur_salesrep_quota40_rec.debit_bal_segment,
                                    cur_salesrep_quota40_rec.resource_name,
                                    lv_segment1,
                                    lv_err_flag,
                                    lv_err_msg
                                   );
			EXCEPTION
			WHEN OTHERS
			THEN
			lv_err_flag := 'Y';
            lv_err_msg :=
                  'Error while calling slc_cnext_balanc_seg_p ';
			END;
            IF lv_err_flag = 'Y'
            THEN
               RAISE ex_error_found;
            END IF;
            BEGIN
            slc_cnext_loc_seg_p (cur_salesrep_quota40_rec.debit_location,
                                 cur_salesrep_quota40_rec.resource_name,
                                 cur_salesrep_quota40_rec.currency_code,
                                 p_quota_id,
                                 lv_segment2,
                                 lv_err_flag,
                                 lv_err_msg
                                );
			EXCEPTION
			WHEN OTHERS
			THEN
			lv_err_flag := 'Y';
            lv_err_msg :=
                  'Error while calling slc_cnext_loc_seg_p ';
			END;
            IF lv_err_flag = 'Y'
            THEN
               RAISE ex_error_found;
            END IF;

            lv_segment3 := cur_salesrep_quota40_rec.debit_account;
            lv_segment4 := cur_salesrep_quota40_rec.debit_sub_account;
            --Validating code combination whether existing in the system
			lv_code_com := lv_segment1||'.'||lv_segment2||'.'||lv_segment3||'.'||lv_segment4||'.'||cur_salesrep_quota40_rec.tbd1||'.'||cur_salesrep_quota40_rec.tbd2;

			 slc_cnext_write_log_p
               (gv_log,'Validating code combination '||lv_code_com);

                     ln_code_combination_id :=
                        get_val_code_comb_id_f
                                        (lv_segment1,
                                         lv_segment2,
                                         lv_segment3,
                                         lv_segment4,
                                         cur_salesrep_quota40_rec.tbd1,
                                         cur_salesrep_quota40_rec.tbd2,
                                         lv_err_msg
                                        );

                     IF (ln_code_combination_id = 0)
                     THEN
					    lv_code_comb_flag := 'Y';
						lv_err_flag := 'Y';
                        lv_err_msg :=
                              'Invalid Code combination '
                           || lv_code_com
                           || ' Error: '
                           || lv_err_msg;
                        RAISE ex_code_comb;
                     END IF;

            slc_cnext_write_log_p
               (gv_log,
                'Inserting DEBIT row for Journal type STANDARD in GL_INTERFACE table'
               );
			 BEGIN
            slc_cnext_jour_ent_ins_p (gv_journal_source,
                                      cur_salesrep_quota40_rec.journal_category,
                                      cur_salesrep_quota40_rec.ledge_id,
                                      cur_salesrep_quota40_rec.currency_code,
                                      l_open_period,
                                      cur_salesrep_quota40_rec.amount,
                                      0.00,
                                      lv_segment1,
                                      lv_segment2,
                                      lv_segment3,
                                      lv_segment4,
                                      cur_salesrep_quota40_rec.tbd1,
                                      cur_salesrep_quota40_rec.tbd2,
                                      gv_journal_source,
                                      l_reference4,
                                      lv_err_flag,
                                      lv_err_msg
                                     );
				
				--Changes for v1.8 Commented the COMMIT and COMMIT will happen at the end.
				--COMMIT;
				ln_total_success_records := ln_total_success_records+1;

			EXCEPTION
			WHEN OTHERS
			THEN
			lv_err_flag := 'Y';
            lv_err_msg :=
                  'Error while calling slc_cnext_jour_ent_ins_p ';
            slc_cnext_write_log_p (gv_log,
                       'rows created in GL_INTERFACE ' || SQL%ROWCOUNT
                      );
			END;

            IF lv_err_flag = 'Y'
            THEN
               RAISE ex_error_found;
            END IF;
         END IF;



         BEGIN

		 slc_cnext_write_log_p(gv_log,'UPDATING resource_name'||cur_salesrep_quota40_rec.resource_name);
		    UPDATE cn_commission_lines_all  --ADDED AS PER 40% CHANGES
               SET attribute43 = 'POSTED',
			   last_update_date=SYSDATE                   --added for defect#43426
             WHERE commission_header_id IN (
                      SELECT ccla.commission_header_id
                        FROM cn_commission_lines_all ccla,
                             jtf_rs_defresources_v jrd
                                       ---jtf_rs_defresources_tl to be checked
                                                      ,
                             jtf_rs_salesreps jrs,
                             cn_quotas_all cqa,
                             hr_operating_units hou,
                             gl_ledgers gll
                       WHERE hou.set_of_books_id = gll.ledger_id
                         AND cqa.org_id = hou.organization_id
                         AND jrd.hold_payment = 'N'
                         AND jrs.salesrep_number = jrd.resource_name
                         AND jrd.category = 'OTHER' --need to ask from Animesh
                         --AND jrd.end_date_active IS NULL --commented as per defect 44985
                                     --need to ask from Divya/Animesh--sysdate
                         AND ccla.quota_id = cqa.quota_id
                         AND ccla.credited_salesrep_id = jrs.salesrep_id
						 AND jrs.salesrep_id = cur_salesrep_quota40_rec.resource_id  ---- added below joins for defect#43473
                         AND ccla.status = gv_status
                         AND ccla.processed_period_id =
                                NVL (p_period, NVL (p_start_date, p_end_date))
                         AND cqa.org_id = p_operating_unit
                         AND cqa.quota_id = p_quota_id
						 )
						 AND ATTRIBUTE43 IS NULL
						 AND quota_id = p_quota_id;
						 
						 slc_cnext_write_log_p(gv_log,'Updating records'||'-'||SQL%ROWCOUNT);
						 
						
         EXCEPTION
            WHEN OTHERS
            THEN
			   slc_cnext_write_log_p(gv_log,'Error in updating CN_COMMISSION_LINES_ALL'||sqlcode||sqlerrm);
               lv_err_flag := 'Y';
               lv_err_msg :=
                     'Error While updating the status for quota id '||p_quota_id;
         END;
		 		
		    slc_cnext_write_log_p(gv_log,'Opening Cursor to update header with POSTED');
		   OPEN cur_header_id(cur_salesrep_quota40_rec.resource_id);
	       LOOP
	          BEGIN
	          FETCH cur_header_id INTO cur_header_id_rec;
		      EXIT WHEN cur_header_id%NOTFOUND;
			  
			  slc_cnext_write_log_p(gv_log,'Fetching the count from line table having NULL as status');
			    BEGIN
				    --added by Anand
					IF(cur_salesrep_quota40_rec.quota_group_code = 'OTHER SUBSIDY' )
					THEN 
						SELECT count(1)
						INTO ln_line_count
						FROM (
						   SELECT ccla.*
								FROM cn_commission_lines_all ccla,
								 jtf_rs_defresources_v jrd
										   ---jtf_rs_defresources_tl to be checked
														  ,
								 jtf_rs_salesreps jrs,
								 cn_quotas_all cqa,
								 hr_operating_units hou,
								 gl_ledgers gll
						   WHERE hou.set_of_books_id = gll.ledger_id
							 AND cqa.org_id = hou.organization_id
							 AND jrd.hold_payment = 'N'
							 AND jrs.salesrep_number = jrd.resource_name
							 AND jrd.category = 'OTHER' 
							 AND ccla.quota_id = cqa.quota_id
							 AND ccla.credited_salesrep_id = jrs.salesrep_id
							 AND jrs.salesrep_id = cur_salesrep_quota40_rec.resource_id  
							 AND ccla.status = gv_status
							 AND ccla.processed_period_id =
									NVL (p_period, NVL (p_start_date, p_end_date))
							 AND cqa.org_id = p_operating_unit
							-- AND cqa.quota_id = p_quota_id
							 AND cqa.quota_group_code = 'OTHER SUBSIDY'
							 AND ccla.commission_header_id = cur_header_id_rec.commission_header_id
							 AND ccla.attribute43 IS NULL);
					ELSE
						SELECT count(1)
						INTO ln_line_count
						FROM (
						   SELECT ccla.*
								FROM cn_commission_lines_all ccla,
								 jtf_rs_defresources_v jrd
										   ---jtf_rs_defresources_tl to be checked
														  ,
								 jtf_rs_salesreps jrs,
								 cn_quotas_all cqa,
								 hr_operating_units hou,
								 gl_ledgers gll
						   WHERE hou.set_of_books_id = gll.ledger_id
							 AND cqa.org_id = hou.organization_id
							 AND jrd.hold_payment = 'N'
							 AND jrs.salesrep_number = jrd.resource_name
							 AND jrd.category = 'OTHER' 
							 AND ccla.quota_id = cqa.quota_id
							 AND ccla.credited_salesrep_id = jrs.salesrep_id
							 AND jrs.salesrep_id = cur_salesrep_quota40_rec.resource_id  
							 AND ccla.status = gv_status
							 AND ccla.processed_period_id =
									NVL (p_period, NVL (p_start_date, p_end_date))
							 AND cqa.org_id = p_operating_unit
							-- AND cqa.quota_id = p_quota_id
							 AND ccla.commission_header_id = cur_header_id_rec.commission_header_id
							 AND ccla.attribute43 IS NULL);
					END IF;
				
				END;
		        
				slc_cnext_write_log_p(gv_log,' count from line table having NULL as status :'||ln_line_count);
				
				IF ln_line_count = 0 THEN
				   BEGIN
				   UPDATE CN_COMMISSION_HEADERS_ALL
				   SET attribute97 = 'POSTED',
			       last_update_date=SYSDATE                   --added for defect#43426
                   WHERE commission_header_id = cur_header_id_rec.commission_header_id
				   AND attribute97 IS NULL;
				   EXCEPTION
				   WHEN OTHERS THEN
				   slc_cnext_write_log_p(gv_log,'Error in updating CN_COMMISSION_HEADERS_ALL'||sqlcode||sqlerrm);
                   lv_err_flag := 'Y';
                   lv_err_msg :=
                     'Error in updating CN_COMMISSION_HEADERS_ALL for resource id '||cur_salesrep_quota40_rec.resource_id;
				   END;
				END IF;
		      EXCEPTION
			  WHEN OTHERS THEN
				   slc_cnext_write_log_p(gv_log,'Error in Fetching records from cursor'||sqlcode||sqlerrm);
                   lv_err_flag := 'Y';
                   lv_err_msg :=
                     'Error in Fetching records from cursor for resource_id '||cur_salesrep_quota40_rec.resource_id;
		      END;
			END LOOP;
			CLOSE cur_header_id;
			
			
     /* EXCEPTION
         WHEN ex_gl_cod_com_not_found
         THEN
            p_err_flag := lv_err_flag;
            p_err_msg := lv_err_msg;

         WHEN OTHERS
         THEN
            p_err_flag := lv_err_flag;
            p_err_msg := lv_err_msg;
      END;*/

	  p_success_count := ln_total_success_records;
      p_err_flag := lv_err_flag;
      p_err_msg := lv_err_msg;
      slc_cnext_write_log_p(gv_log,'Ending slc_cnext_oic_res_det_p procedure to fetch data related to OIC with resource name '||
	  cur_salesrep_quota40_rec.resource_name);



   EXCEPTION
      WHEN ex_no_data_process
      THEN
	     ln_total_failcust_validation := ln_total_failcust_validation+1;
		 p_error_count := ln_total_failcust_validation;
         p_err_flag := 'Y';
         p_err_msg := 'No data found for quota id ' || p_quota_id;
		 --Changes for v1.8 Setting Error Flags.
		 lv_err_flag := 'Y';
		 lv_err_msg  := 'No data found for quota id ' || p_quota_id;
		 
	  WHEN ex_too_many_rows
	  THEN
	     ln_total_failcust_validation := ln_total_failcust_validation+1;
		 p_error_count := ln_total_failcust_validation;
	     p_err_flag := 'Y';
         p_err_msg :=
               'Too many rows in fetching the data from OIC for quota id :'
            || p_quota_id;
		 --Changes for v1.8 Setting Error Flags.
		 lv_err_flag := 'Y';
		 lv_err_msg  := 'Too many rows in fetching the data from OIC for quota id :'|| p_quota_id;

		 
      WHEN ex_invalid_data_process
      THEN
	    ln_total_failcust_validation := ln_total_failcust_validation+1;
		 p_error_count := ln_total_failcust_validation;
         p_err_flag := 'Y';
         p_err_msg :=
               'Unhandled exception in fetching the data from OIC for quota id :'
            || p_quota_id;
		 --Changes for v1.8 Setting Error Flags.
		 lv_err_flag := 'Y';
		 lv_err_msg  := 'Unhandled exception in fetching the data from OIC for quota id :'|| p_quota_id;

		 
	  WHEN ex_code_comb
		 THEN
		 
		 --Changes for v1.8 Commented the rollback and rollback will happen at the end.
		 --ROLLBACK;
		 ln_total_failcust_validation := ln_total_failcust_validation+1;
		   p_error_count := ln_total_failcust_validation;
          p_err_flag := lv_err_flag;
            p_err_msg := lv_err_msg;
			slc_cnext_write_log_p(gv_log,'In error count '||p_error_count);
			--slc_cnext_write_log_p(gv_out,p_err_msg);
	  WHEN ex_error_found
         THEN
		   --Changes for v1.8 Commented the rollback and rollback will happen at the end.
		   --ROLLBACK;
		   ln_total_failcust_validation := ln_total_failcust_validation+1;
		   p_error_count := ln_total_failcust_validation;
            p_err_flag := lv_err_flag;
            p_err_msg := lv_err_msg;
			slc_cnext_write_log_p(gv_log,'In error count '||p_error_count);
			slc_cnext_write_log_p(gv_log,p_err_msg);
			--slc_cnext_write_log_p(gv_out,p_err_msg);
      WHEN ex_category_invalid
      THEN
	     ln_total_failcust_validation := ln_total_failcust_validation+1;
		 p_error_count := ln_total_failcust_validation;
         p_err_flag := 'Y';
         p_err_msg :=
               'The Journal Category invalid :'
            || cur_salesrep_quota40_rec.journal_category
            || ' for resource name '
            || cur_salesrep_quota40_rec.resource_name
            || ' and quota id :'
            || p_quota_id;
			slc_cnext_write_log_p(gv_log,p_err_msg);
			slc_cnext_write_log_p(gv_out,p_err_msg);
		 --Changes for v1.8 Setting Error Flags.
		 lv_err_flag := p_err_flag;
		 lv_err_msg  := p_err_msg;
		 
      WHEN ex_journal_type_invalid
      THEN
	     ln_total_failcust_validation := ln_total_failcust_validation+1;
		 p_error_count := ln_total_failcust_validation;
         p_err_flag := 'Y';
         p_err_msg :=
               'The Journal type has more than one value  for resource name '
            || cur_salesrep_quota40_rec.resource_name
            || ' and quota id :'
            || p_quota_id;
		 slc_cnext_write_log_p(gv_log,p_err_msg);
		slc_cnext_write_log_p(gv_out,p_err_msg);
		 --Changes for v1.8 Setting Error Flags.
		 lv_err_flag := p_err_flag;
		 lv_err_msg  := p_err_msg;
						
      WHEN ex_journal_type_not_found
      THEN
	     ln_total_failcust_validation := ln_total_failcust_validation+1;
		 p_error_count := ln_total_failcust_validation;
         p_err_flag := 'Y';
         p_err_msg :=
               'The Journal Type does not exist in system :'
            || cur_salesrep_quota40_rec.journal_type
            || ' for resource name '
            || cur_salesrep_quota40_rec.resource_name
            || ' and quota id :'
            || p_quota_id;
         slc_cnext_write_log_p(gv_log,p_err_msg);
	     slc_cnext_write_log_p(gv_out,p_err_msg);
		 --Changes for v1.8 Setting Error Flags.
		 lv_err_flag := p_err_flag;
		 lv_err_msg  := p_err_msg;

      WHEN ex_category_not_found
      THEN
	     ln_total_failcust_validation := ln_total_failcust_validation+1;
		 p_error_count := ln_total_failcust_validation;
         p_err_flag := 'Y';
         p_err_msg :=
               'The Journal Category name does not exist in system :'
            || cur_salesrep_quota40_rec.journal_category
            || ' for resource name '
            || cur_salesrep_quota40_rec.resource_name
            || ' and quota id :'
            || p_quota_id;
		slc_cnext_write_log_p(gv_log,p_err_msg);
			slc_cnext_write_log_p(gv_out,p_err_msg);
		 --Changes for v1.8 Setting Error Flags.
		 lv_err_flag := p_err_flag;
		 lv_err_msg  := p_err_msg;
			
      WHEN ex_date_not_found
      THEN
	     ln_total_failcust_validation := ln_total_failcust_validation+1;
		 p_error_count := ln_total_failcust_validation;
         p_err_flag := lv_err_flag;
         p_err_msg := lv_err_msg;
		 slc_cnext_write_log_p(gv_log,p_err_msg);
		 slc_cnext_write_log_p(gv_out,p_err_msg);
      END;
	     IF lv_err_flag = 'Y'
         THEN
            ROLLBACK TO oic_savepoint;
            slc_cnext_write_log_p (gv_out, 'Error message :' || lv_err_msg);
			slc_cnext_write_log_p (gv_log, '******************Rolling back transactions. Error message :' || lv_err_msg||'*******************');
            populate_err_object
                            (p_in_batch_key            => gv_batch_key,
                             p_in_business_entity      => lv_business_entity_name,
                             p_in_process_id3          => NULL,
                             p_in_error_txt            => lv_err_msg,
                             p_in_request_id           => gn_request_id,
                             p_in_attribute1           =>    'Salesrep id:'
                                                          || cur_salesrep_quota40_rec.resource_id
                            );
         ELSE
		    slc_cnext_write_log_p (gv_log, '************************Committing the transactions.**************************');
            COMMIT;
         END IF;

	END LOOP;
    CLOSE cur_salesrep_quota40;
	EXCEPTION
	WHEN ex_source_invalid
      THEN
	     ln_total_failcust_validation := ln_total_failcust_validation+1;
		 p_error_count := ln_total_failcust_validation;
         p_err_flag := 'Y';
         p_err_msg :=
               'The SOURCE NAME invalid :'
            || gv_journal_source
            || ' for resource name '
            || cur_salesrep_quota40_rec.resource_name
            || ' and quota id :'
            || p_quota_id;
			slc_cnext_write_log_p(gv_log,p_err_msg);
			slc_cnext_write_log_p(gv_out,p_err_msg);
	WHEN ex_source_not_found
      THEN
	     ln_total_failcust_validation := ln_total_failcust_validation+1;
		 p_error_count := ln_total_failcust_validation;
         p_err_flag := 'Y';
         p_err_msg :=
               'The Journal SOURCE NAME does not exist in system :'
            || gv_journal_source
            || ' for resource name '
            || cur_salesrep_quota40_rec.resource_name
            || ' and quota id :'
            || p_quota_id;
			slc_cnext_write_log_p(gv_log,p_err_msg);
			slc_cnext_write_log_p(gv_out,p_err_msg);
	WHEN OTHERS
      THEN
	     ln_total_failcust_validation := ln_total_failcust_validation+1;
		 p_error_count := ln_total_failcust_validation;
         p_err_flag := 'Y';
         p_err_msg :=
               'Some other error in fetching the data related to OIC resource with  resource name '
            || cur_salesrep_quota40_rec.resource_name||' and error is '||SQLERRM;
		slc_cnext_write_log_p(gv_log,p_err_msg);
		slc_cnext_write_log_p(gv_out,p_err_msg);
   END slc_cnext_oic_res_det40_p;

  --===============================================================
-- Procedure Name  : slc_cnext_oic_to_gl_p
-- Description     : This is the main procedure called by the
--                   concurrent program to fetch the required data and
--                   insert data in gl_interface table
-- Parameters      :
--                     1. Error message (Out)
--                     2. Error code    (Out)
--===============================================================
   PROCEDURE slc_cnext_jour_ent_p (
      p_errbuf           OUT NOCOPY      VARCHAR2,
      p_retcode          OUT NOCOPY      NUMBER,
      p_operating_unit   IN              NUMBER,
      p_process_name     IN              VARCHAR2,
      p_period           IN              NUMBER,
      p_start_date       IN              NUMBER,
      p_end_date         IN              NUMBER,
	  p_debug_flag       IN              VARCHAR2
   )
   IS
      lv_resource_id                  cn_commission_lines_all.credited_salesrep_id%TYPE;
      lv_resource_name                jtf_rs_salesreps.NAME%TYPE;
      lv_amount                       cn_commission_lines_all.commission_amount%TYPE;
      lv_journal_type                 cn_quotas_all.attribute1%TYPE;
      lv_debit_bal_segment            cn_quotas_all.attribute2%TYPE;
      lv_debit_location               cn_quotas_all.attribute3%TYPE;
      lv_debit_account                cn_quotas_all.attribute4%TYPE;
      lv_debit_sub_account            cn_quotas_all.attribute5%TYPE;
      lv_credit_bal_segment           cn_quotas_all.attribute6%TYPE;
      lv_credit_location              cn_quotas_all.attribute7%TYPE;
      lv_credit_account               cn_quotas_all.attribute8%TYPE;
      lv_credit_sub_account           cn_quotas_all.attribute9%TYPE;
      lv_journal_category             cn_quotas_all.attribute10%TYPE;
      lv_tbd1                         cn_quotas_all.attribute11%TYPE;
      lv_tbd2                         cn_quotas_all.attribute12%TYPE;
      lv_credit_tbd1                  cn_quotas_all.attribute13%TYPE;
      lv_credit_tbd2                  cn_quotas_all.attribute14%TYPE;
      lv_currency_code                gl_ledgers.currency_code%TYPE;
      lv_ledger_id                    gl_ledgers.ledger_id%TYPE;
      lv_reference4                   gl_interface.reference4%TYPE;
      lv_err_flag                     VARCHAR2 (20);
      lv_err_msg                      VARCHAR2 (4000);
      ln_program_status               NUMBER;
      ex_no_data_found                EXCEPTION;
      --Common error logging code.
      ln_total_record                 NUMBER                        DEFAULT 0;
	  ln_success_record               NUMBER                        DEFAULT 0;
	  ln_error_record                 NUMBER                        DEFAULT 0;
      ln_total_success_records        NUMBER                        DEFAULT 0;
      ln_total_failcust_validation    NUMBER                        DEFAULT 0;
      ln_total_errorcust_validation   NUMBER                        DEFAULT 0;
      lv_batch_status                 VARCHAR2 (1);
      lv_publish_flag                 VARCHAR2 (1);
      lv_system_type                  VARCHAR2 (10);
      lv_source                       VARCHAR2 (10);
      lv_destination                  VARCHAR2 (10);
      lv_cmn_err_status_code          VARCHAR2 (100);
      lv_cmn_err_rec                  VARCHAR2 (2000);
      lv_business_entity_name         VARCHAR2 (100)
                                                    := 'slc_cnext_jour_ent_p';
      lv_status                       VARCHAR2 (30);
	  ln_error_count                  NUMBER    DEFAULT  0;
	  ln_success_count                NUMBER    DEFAULT  0;
	  ln_total_count                  NUMBER    DEFAULT  0;
	  --ADDED AS PART pf 40% CHANGES
	  ln_total_record40                 NUMBER                        DEFAULT 0;
	  ln_success_record40               NUMBER                        DEFAULT 0;
	  ln_error_record40                 NUMBER                        DEFAULT 0;
	  ln_error_count40                  NUMBER                        DEFAULT  0;
	  ln_success_count40                NUMBER                        DEFAULT  0;
	  ln_total_count40                  NUMBER                        DEFAULT  0;

      CURSOR c_all_quota
      IS
         SELECT quota_id
         FROM cn_quotas_all
         WHERE quota_group_code NOT IN ('QUOTA'
		                                ,'NONE'
		                                ,'BONUS'
		                                ,'OTHER SUBSIDY'
										,'TSC'
                                        ,'NONTSC'
                                        ,'OTHER MONEYORDER'
                                        ,'CREDITCARDFEES' 
                                        ,'CREDITCARDSTAT')
		 AND delete_flag = 'N'
		 --AND quota_id = 1017;
         AND quota_group_code = DECODE(p_process_name,'ALL',quota_group_code,p_process_name);
		 
	  CURSOR c_all_quota_40
      IS
         SELECT quota_id
         FROM cn_quotas_all
         WHERE quota_group_code IN ('OTHER SUBSIDY'
								   ,'TSC'
                                   ,'NONTSC'
                                   ,'OTHER MONEYORDER'
                                   ,'CREDITCARDFEES' 
                                   ,'CREDITCARDSTAT')
		 AND delete_flag = 'N'
		 --AND quota_id = 1017;
         AND quota_group_code = DECODE(p_process_name,'ALL',quota_group_code,p_process_name);

      c_al_quota_rec                  c_all_quota%ROWTYPE;
	  c_al_quota_40_rec               c_all_quota_40%ROWTYPE;
   BEGIN
   gv_debug_flag := p_debug_flag;
      slc_cnext_write_log_p
         (gv_log,
          'Concurrent program started the main procedure slc_glext_jour_ent_p '
         );
      --Fetching OIC and resource required data
      slc_cnext_write_log_p
         (gv_log,
          'Calling procedure slc_cnext_oic_res_det_p to fetch OIC and resource related required data '
         );

      FOR c_all_quota_rec IN c_all_quota
      LOOP
         slc_cnext_oic_res_det_p (
                                  lv_err_flag,
                                  lv_err_msg,
                                  c_all_quota_rec.quota_id,
                                  p_period,
                                  p_operating_unit,
                                  p_start_date,
                                  p_end_date,
								  ln_total_count  ,
	                              ln_success_count,
	                              ln_error_count
                                 );
				ln_total_record := ln_total_record+ln_total_count;
				ln_success_record := ln_success_record+ln_success_count;
				ln_error_record := ln_error_record+ln_error_count;
         /*ln_total_record := ln_total_record + 1;*/
      END LOOP;

	  --Fetching OIC and resource required data
      slc_cnext_write_log_p
         (gv_log,
          'Calling procedure slc_cnext_oic_res_det40_p to fetch OIC and resource related required data for 40% '
         );

      FOR c_all_quota_40_rec IN c_all_quota_40
      LOOP
         slc_cnext_oic_res_det40_p (
                                  lv_err_flag,
                                  lv_err_msg,
                                  c_all_quota_40_rec.quota_id,
                                  p_period,
                                  p_operating_unit,
                                  p_start_date,
                                  p_end_date,
								  ln_total_count40  ,
	                              ln_success_count40,
	                              ln_error_count40
                                 );
				ln_total_record40 := ln_total_record40+ln_total_count40;
				ln_success_record40 := ln_success_record40+ln_success_count40;
				ln_error_record40 := ln_error_record40+ln_error_count40;
         /*ln_total_record := ln_total_record + 1;*/
      END LOOP;
	  
	  slc_cnext_write_log_p
         (gv_log,'ln_total_record 40 :'||ln_total_record||'ln_success_record :'||ln_success_record||'ln_error_record :'||ln_error_record);
	  
	  ln_total_record := ln_total_record + ln_total_record40 ;
	  ln_success_record := ln_success_record + ln_success_record40;
	  ln_error_record := ln_error_record + ln_error_record40;
	  
	  slc_cnext_write_log_p
         (gv_log,'ln_total_record :'||ln_total_record||'ln_success_record :'||ln_success_record||'ln_error_record :'||ln_error_record);

      IF ln_error_record = 0
      THEN
         ln_program_status := 0;
         lv_status := 'Success';
      ELSIF ln_error_record <> ln_total_record
      THEN
         ln_program_status := 1;
         lv_status := 'Warning';
      ELSIF ln_error_record = ln_total_record
      THEN
         ln_program_status := 2;
         lv_status := 'Error';
      END IF;

      slc_util_jobs_pkg.slc_util_e_log_summary_p
                 (p_batch_key                      => gv_batch_key,
                  p_business_process_name          => gv_business_process_name,
                  p_total_records                  => ln_total_record,
                  p_total_success_records          => ln_success_record,
                  p_total_failcustval_records      => ln_error_record,
                  p_total_failstdval_records       => ln_total_errorcust_validation,
                  p_batch_status                   => lv_batch_status,
                  p_publish_flag                   => lv_publish_flag,
                  p_system_type                    => lv_system_type,
                  p_source_system                  => lv_source,
                  p_target_system                  => lv_destination,
                  p_request_id                     => gn_request_id,
                  p_user_id                        => gn_user_id,
                  p_login_id                       => gn_login_id,
                  p_status_code                    => lv_cmn_err_status_code
                 );
      slc_util_jobs_pkg.slc_util_log_errors_p
                         (p_batch_key                  => gv_batch_key,
                          p_business_process_name      => gv_business_process_name,
                          p_errors_rec                 => gv_cmn_err_rec,
                          p_user_id                    => gn_user_id,
                          p_login_id                   => gn_login_id,
                          p_status_code                => lv_cmn_err_status_code
                         );
      p_retcode := ln_program_status;
      slc_cnext_write_log_p (gv_log, '******OIC to GL Summery******');
      slc_cnext_write_log_p (gv_out,
                    'Timestamp of request completion               :'
                 || SYSTIMESTAMP
                );
      slc_cnext_write_log_p (gv_out,
                 'Status of the request Submitted               :'
                 || lv_status
                );
      slc_cnext_write_log_p (gv_log,
                    'Total number of record processed              :'
                 || ln_total_record
                );
      slc_cnext_write_log_p (gv_log,
                    'Total number of record processed Successfully :'
                 || ln_success_record
                );
      slc_cnext_write_log_p (gv_log,
                    'Total number of record processed in Error     :'
                 || ln_error_record
                );
      slc_cnext_write_log_p (gv_out,
                    'Total number of record processed              :'
                 || LN_TOTAL_RECORD
                );
      slc_cnext_write_log_p (gv_out,
                    'Total number of record processed Successfully :'
                 || ln_success_record
                );
      slc_cnext_write_log_p (gv_out,
                    'Total number of record processed in Error     :'
                 || ln_error_record
                );
   EXCEPTION
      WHEN OTHERS
      THEN
         slc_cnext_write_log_p (gv_log, lv_err_msg);
         slc_cnext_write_log_p (gv_out, lv_err_msg);
         slc_cnext_write_log_p (gv_log, 'Program ended');
   end slc_cnext_jour_ent_p;
end slc_cnext_oic_to_gl_pkg;
/
SHOW ERRORS;
EXIT;
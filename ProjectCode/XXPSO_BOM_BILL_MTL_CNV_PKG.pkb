WHENEVER SQLERROR EXIT FAILURE

create or replace
PACKAGE BODY xxpso_bom_bill_mtl_cnv_pkg
AS
--+============================================================|
--| Module Name: Bill Of Materials.
--|
--| File Name: XXPSO_BOM_BILL_MTL_CNV_PKG.pks
--|
--| Description: This is package Header creation script.
--|
--| Date: 10- Mar -2015
--|
--| Author: Monica Yekula
--|
--| Usage: BOM Conversion
--| Copyright: Pearson
--|
--| All rights reserved.
--|
--+============================================================|
--| Modification History
--+============================================================|
--| Date                 Who             Description
--| ----------- --------------------  -------------------------
--| 10-Mar-2016  Monica Yekula           Initial Creation
--| 8-Aug-2016		2.0	Akshay Nayak	   a) Changes for surviorship logic.
--|						   b) Changes for Performance Improvement.
--|						   c) Splitting data into master and child table seperately.
--| 19-Sep-2016		2.1	Akshay Nayak	   Acknowledgement sending
--+============================================================|
--Changes for v2.0	Begin
   /******************************************
     global constant variables declaration
   *******************************************/
   gv_log		  VARCHAR2 (10) := 'LOG';
   gv_output		  VARCHAR2 (10) := 'OUTPUT';
   gv_new_flag		  VARCHAR2 (1) := 'N'; 
   gv_failed_flag	  VARCHAR2 (1) := 'F';
   gv_error_flag	  VARCHAR2 (1) := 'E';
   gv_processed_flag	  VARCHAR2 (1) := 'P'; 
   gv_valid_flag	  VARCHAR2 (1) := 'V'; 
   gv_invalid_flag	  VARCHAR2 (1) := 'I'; 
   gv_staged_flag	  VARCHAR2 (1) := 'I';--'S'; --Initialy it was kept as S (Interfaced to Interface table).
   gv_validate_mode	   VARCHAR2(15) := 'V';--V stands for Validate mode
   gv_import_mode	   VARCHAR2(15) := 'P';--P stands for Process mode
   --Changes for v2.0
   gv_reprocess_mode	   VARCHAR2(15) := 'R';--RV stands for ReProcess of Errored and Failed records
   gv_revalidate_mode	   VARCHAR2(15) := 'RV';--R stands for ReValidate of Errored and Failed records
   gv_validate_process_mode	   VARCHAR2(15) := 'VP';--VP stands for Validate and Process mode.
   gv_create_trn	   VARCHAR2(15) := 'CREATE';
   gv_update_trn	   VARCHAR2(15) := 'UPDATE';
   gc_mode		   VARCHAR2(2);
   gc_debug_flag	   VARCHAR2 (2);
   gn_batch_size		NUMBER;
   gn_no_of_process		NUMBER;
   gn_source_system		VARCHAR2(20);
   gn_follow_survivorship	VARCHAR2(20);
   gn_survivorship_lkp_name	VARCHAR2(20)			:= 'XXPSO_SURVIVORSHP';
   gv_yes_code		   VARCHAR2 (1) := 'Y';
   gv_yes		   VARCHAR2 (3) := 'Yes';
   gv_no_code		   VARCHAR2 (1) := 'N';
   gv_no		   VARCHAR2 (3) := 'No';   
   gc_assembly_item_ricew_id     CONSTANT    VARCHAR2 (30)       := 'BOM-ASSEMBLY-CNV-001'; 
   gc_component_item_ricew_id     CONSTANT    VARCHAR2 (30)       := 'BOM-COMPONENT-CNV-002'; 
   gv_cross_reference_type VARCHAR2(25) := 'SS_ITEM_XREF';
   
--Changes for v2.0	End.
   gn_request_id                             NUMBER
                                           DEFAULT fnd_global.conc_request_id;
   g_resp_id                                NUMBER DEFAULT fnd_global.resp_id;
   g_prog_appl_id                           NUMBER
                                              DEFAULT fnd_global.prog_appl_id;
   g_resp_appl_id                           NUMBER
                                              DEFAULT fnd_global.resp_appl_id;
   gcn_asmb_bulk_limit             CONSTANT NUMBER (5)     := 20000;
   gcn_comp_bulk_limit             CONSTANT NUMBER (5)     := 20000;
   -- g_conc_request_id        NUMBER DEFAULT fnd_global.conc_request_id;
   gn_user_id                                NUMBER DEFAULT fnd_global.user_id;
   gn_login_id                               NUMBER
                                                  DEFAULT fnd_global.login_id;
   g_org_id                                 NUMBER  DEFAULT fnd_global.org_id;
   g_retcode                                NUMBER;
   g_errbuff                                VARCHAR2 (1);
   g_operating_unit                         VARCHAR2 (100);
   g_sysdate                       CONSTANT DATE           := SYSDATE;
   g_item_processed                         NUMBER;
   g_item_rejected                          NUMBER;
   g_item_found                             NUMBER;
   g_update                                 VARCHAR2 (50);
   gn_batch_id                               NUMBER;
   g_master_org_id                          NUMBER;
   gc_process_flag                          VARCHAR2 (2);
   gc_enable_de_duplication_flag   CONSTANT VARCHAR2 (10)   := apps.fnd_profile.VALUE ('XX_ENABLE_DE_DUPLICATION_ON_ITEM_CONV');
   gc_record_status                         VARCHAR2 (10);

--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
-- Name       : XXPSO_PRINT_LOG_P
-- Description: This procedure is used to write message to log file.
--+============================================================|
--| Modification History
--+============================================================|
--| Date                 Who             Description
--| ----------- --------------------  -------------------------
--| 10-Mar-2016  Monica Yekula           Initial Creation
--------------------------------------------------------------------------------
   PROCEDURE xxpso_print_debug (p_message IN VARCHAR2, p_type IN VARCHAR2)
   IS
   BEGIN
   	IF p_type = gv_log AND gc_debug_flag = gv_yes_code THEN
	      fnd_file.put_line (
		 fnd_file.LOG,p_message);	
	ELSIF p_type = gv_output THEN
	      fnd_file.put_line (
		 fnd_file.OUTPUT,p_message);		
	END IF;
	dbms_output.put_line('Message: ' || p_message);
   EXCEPTION
      WHEN OTHERS
      THEN
         fnd_file.put_line (
            fnd_file.LOG,
               'Error message in xxrh_print_debug'
            || SQLERRM
            || 'and error code is'
            || SQLCODE);
   END xxpso_print_debug;


--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
-- Name       : transfer_data_to_split_tables
-- Description: This procedure will split the data from Main Staging tables
--    		into new tables created as part of release 2.0
--		xxpso_ego_bom_assembly_stg will hold Assembly Data
--		xxpso_ego_bom_component_stg will hold Component related data.
--+============================================================|
--| Modification History
--+============================================================|
--| Date                 Who             Description
--| ----------- --------------------  -------------------------
--| 10-Mar-2016  Monica Yekula           Initial Creation
--------------------------------------------------------------------------------

  PROCEDURE transfer_data_to_split_tables
  AS
  	CURSOR cur_assembly_transfer
  	IS
  	SELECT BOM_TRANSACTION_TYPE
  	      ,ORGANIZATION_CODE
  	      ,STRUCTURE_NAME
  	      ,STRUCTURE_TYPE_NAME
  	      ,SOURCE_SYSTEM_ASSEMBLY
  	      ,ASSE_SRC_ITM_NUM
  	      ,BOM_DESCRIPTION
  	      ,MDM_REFERENCE_ID
  	      ,RECEIVED_FROM_ETL
  	      ,RECEIVED_FROM_ESB
  	  FROM xxpso_ego_bom_stg
  	 WHERE status_code = gv_new_flag 
      GROUP BY BOM_TRANSACTION_TYPE
  	      ,ORGANIZATION_CODE
  	      ,STRUCTURE_NAME
  	      ,STRUCTURE_TYPE_NAME
  	      ,SOURCE_SYSTEM_ASSEMBLY
  	      ,ASSE_SRC_ITM_NUM
  	      ,BOM_DESCRIPTION
  	      ,MDM_REFERENCE_ID
  	      ,RECEIVED_FROM_ETL
  	      ,RECEIVED_FROM_ESB;
  	
	 TYPE lc_cur_assembly_tbl	IS TABLE OF cur_assembly_transfer%ROWTYPE;
	 lc_cur_assembly_tab	lc_cur_assembly_tbl;
	 
	 lv_assembly_record_id		NUMBER;
	 
  BEGIN
   xxpso_print_debug ('In transfer_data_to_split_tables:',gv_log );
   
  -- Fetch all the unique assembly records from the xxpso_ego_bom_stg with status code as New and 
  -- insert it into new assembly table.
  OPEN cur_assembly_transfer;
  FETCH cur_assembly_transfer
  BULK COLLECT INTO lc_cur_assembly_tab;
  	
  	FOR index_count IN 1..lc_cur_assembly_tab.COUNT
  	LOOP
  		lv_assembly_record_id := XXPSO_EGO_ASSEMBLY_RECORD_S.NEXTVAL;
  		xxpso_print_debug('',gv_log);
  		xxpso_print_debug ('In transfer_data_to_split_tables New Assembly Id:'||lv_assembly_record_id,gv_log );
 		xxpso_print_debug('BOM_TRANSACTION_TYPE:'||lc_cur_assembly_tab(index_count).BOM_TRANSACTION_TYPE,gv_log);		 
            	xxpso_print_debug('ORGANIZATION_CODE:'||lc_cur_assembly_tab(index_count).ORGANIZATION_CODE,gv_log);
            	xxpso_print_debug('STRUCTURE_NAME:'||lc_cur_assembly_tab(index_count).STRUCTURE_NAME,gv_log);
            	xxpso_print_debug('STRUCTURE_TYPE_NAME:'||lc_cur_assembly_tab(index_count).STRUCTURE_TYPE_NAME,gv_log);
            	xxpso_print_debug('SOURCE_SYSTEM_ASSEMBLY:'||lc_cur_assembly_tab(index_count).SOURCE_SYSTEM_ASSEMBLY,gv_log);
            	xxpso_print_debug('ASSE_SRC_ITM_NUM:'||lc_cur_assembly_tab(index_count).ASSE_SRC_ITM_NUM,gv_log);
            	xxpso_print_debug('BOM_DESCRIPTION:'||lc_cur_assembly_tab(index_count).BOM_DESCRIPTION,gv_log); 		  		
  		INSERT INTO xxpso_ego_bom_assembly_stg (RECORD_ID
  							,STATUS_CODE
  							,ORGANIZATION_CODE
  							,BOM_TRANSACTION_TYPE
  							,ASSEMBLY_SOURCE_SYSTEM
  							,ASSEMBLY_SRC_ITEM_NUMBER
  							,BOM_DESCRIPTION
  							,STRUCTURE_NAME
  							,STRUCTURE_TYPE_NAME
  							,READY_TO_ARCHIVE
  							,LAST_UPDATE_DATE                            
							,LAST_UPDATED_BY                           
							,CREATION_DATE                               
							,CREATED_BY                                
    	 						,LAST_UPDATE_LOGIN  
    	 						--Changes for v2.1
    	 						,MDM_REFERENCE_ID
    	 						,PUBLISHED_BY_ETL
    	 						,PUBLISHED_BY_ESB
    	 						,RECEIVED_FROM_ETL
    	 						,RECEIVED_FROM_ESB
    	 						) VALUES
  						        (
  						        lv_assembly_record_id
  						        ,gv_new_flag -- Status Code
  						        ,lc_cur_assembly_tab(index_count).ORGANIZATION_CODE
  						        ,lc_cur_assembly_tab(index_count).BOM_TRANSACTION_TYPE
  						        ,lc_cur_assembly_tab(index_count).SOURCE_SYSTEM_ASSEMBLY
  						        ,lc_cur_assembly_tab(index_count).ASSE_SRC_ITM_NUM
  						        ,lc_cur_assembly_tab(index_count).BOM_DESCRIPTION
  						        ,lc_cur_assembly_tab(index_count).STRUCTURE_NAME
  						        ,lc_cur_assembly_tab(index_count).STRUCTURE_TYPE_NAME
  						        ,'N'	-- Ready to archive flag
  						        ,SYSDATE
  						        ,gn_user_id
  						        ,SYSDATE
  						        ,gn_user_id
  						        ,gn_login_id
    	 						--Changes for v2.1
    	 						,lc_cur_assembly_tab(index_count).MDM_REFERENCE_ID
    	 						,'N'
    	 						,'N'
    	 						,lc_cur_assembly_tab(index_count).RECEIVED_FROM_ETL
    	 						,lc_cur_assembly_tab(index_count).RECEIVED_FROM_ESB
  						        );
  						        
		INSERT INTO xxpso_ego_bom_component_stg(RECORD_ID
							,ASSEMBLY_RECORD_ID
							,STATUS_CODE
							,ORGANIZATION_CODE
							,COMP_TRANSACTION_TYPE
							,COMPONENT_SOURCE_SYSTEM
							,COMPONENT_SRC_ITEM_NUMBER
							,ITEM_SEQUENCE_NUMBER
							,OPERATION_SEQ_NUM
							,COMPONENT_QUANTITY
							,PLANNING_FACTOR
    	 						)
						(SELECT XXPSO_EGO_COMPONENT_RECORD_S.NEXTVAL
							,lv_assembly_record_id
							,gv_new_flag -- Status Code
							,stg.ORGANIZATION_CODE
							,stg.COMP_TRANSACTION_TYPE
							,stg.SOURCE_SYSTEM_COMPONENT
							,stg.comp_src_itm_num
							,stg.item_sequence_number
							,stg.operation_seq_num
							,stg.component_quantity
							,stg.planning_factor
						  FROM xxpso_ego_bom_stg stg
						 WHERE status_code = gv_new_flag
						   AND BOM_TRANSACTION_TYPE = lc_cur_assembly_tab(index_count).BOM_TRANSACTION_TYPE
						   AND ORGANIZATION_CODE = lc_cur_assembly_tab(index_count).ORGANIZATION_CODE
						   AND STRUCTURE_NAME = lc_cur_assembly_tab(index_count).STRUCTURE_NAME
						   AND STRUCTURE_TYPE_NAME = lc_cur_assembly_tab(index_count).STRUCTURE_TYPE_NAME
						   AND SOURCE_SYSTEM_ASSEMBLY = lc_cur_assembly_tab(index_count).SOURCE_SYSTEM_ASSEMBLY
						   AND ASSE_SRC_ITM_NUM = lc_cur_assembly_tab(index_count).ASSE_SRC_ITM_NUM
						   AND NVL(BOM_DESCRIPTION,'NULL') = NVL(lc_cur_assembly_tab(index_count).BOM_DESCRIPTION,'NULL')
						   AND NVL(MDM_REFERENCE_ID,-9)= NVL(lc_cur_assembly_tab(index_count).MDM_REFERENCE_ID,-9)
						   AND NVL(RECEIVED_FROM_ETL,'X')= NVL(lc_cur_assembly_tab(index_count).RECEIVED_FROM_ETL,'X')
						   AND NVL(RECEIVED_FROM_ESB,'X')= NVL(lc_cur_assembly_tab(index_count).RECEIVED_FROM_ESB,'X')
						   );
		
		xxpso_print_debug ('In transfer_data_to_split_tables No of records inserted in Component table:'||SQL%ROWCOUNT,gv_log);
		
		DELETE FROM xxpso_ego_bom_stg
		 WHERE status_code = gv_new_flag
		   AND BOM_TRANSACTION_TYPE = lc_cur_assembly_tab(index_count).BOM_TRANSACTION_TYPE
		   AND ORGANIZATION_CODE = lc_cur_assembly_tab(index_count).ORGANIZATION_CODE
		   AND STRUCTURE_NAME = lc_cur_assembly_tab(index_count).STRUCTURE_NAME
		   AND STRUCTURE_TYPE_NAME = lc_cur_assembly_tab(index_count).STRUCTURE_TYPE_NAME
		   AND SOURCE_SYSTEM_ASSEMBLY = lc_cur_assembly_tab(index_count).SOURCE_SYSTEM_ASSEMBLY
		   AND ASSE_SRC_ITM_NUM = lc_cur_assembly_tab(index_count).ASSE_SRC_ITM_NUM
		   AND NVL(BOM_DESCRIPTION,'NULL') = NVL(lc_cur_assembly_tab(index_count).BOM_DESCRIPTION,'NULL');
			
		xxpso_print_debug ('In transfer_data_to_split_tables No of records deleted from base table:'||SQL%ROWCOUNT,gv_log);
  						        
  	END LOOP;
  
  CLOSE cur_assembly_transfer;
  COMMIT;
  
  END transfer_data_to_split_tables;

--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
-- Name       : XXPSO_PRINT_OUT_P
-- Description: This procedure is created to print the output.
--+============================================================|
--| Modification History
--+============================================================|
--| Date                 Who             Description
--| ----------- --------------------  -------------------------
--| 10-Mar-2016  Monica Yekula           Initial Creation
--------------------------------------------------------------------------------
   PROCEDURE xxpso_print_out_p
   AS
      CURSOR c_error_data
      IS
         SELECT batch_id, record_id, error_message
           FROM xxpso_ego_bom_stg
          WHERE status_code IN ('E', 'F') AND batch_id = gn_batch_id;

      l_err_message   VARCHAR2 (2000);
      l_err_val1      NUMBER          := 0;
      l_tot_val1      NUMBER          := 0;
      l_pass_imp1     NUMBER          := 0;
      l_err_imp1      NUMBER          := 0;
      l_status        VARCHAR2 (30);
   BEGIN
      --count for all the records processed,
      SELECT COUNT (1)
        INTO l_tot_val1
        FROM xxpso_ego_bom_stg stg
       WHERE stg.batch_id = NVL (gn_batch_id, stg.batch_id);

      --count for all records which errored out in validation
      SELECT COUNT (1)
        INTO l_err_val1
        FROM xxpso_ego_bom_stg stg
       WHERE stg.batch_id = NVL (gn_batch_id, stg.batch_id)
         AND stg.status_code IN ('E', 'F')
                                          --AND stg.err_code     IN ('ERR_VAL','ERR_IMP','ERR_IMP_COM')
      ;

      --count for all the records which errored out while importing
      SELECT COUNT (1)
        INTO l_err_imp1
        FROM xxpso_ego_bom_stg stg
       WHERE stg.batch_id = NVL (gn_batch_id, stg.batch_id)
         AND stg.status_code = 'F';

      --count for all the item records which successfully processed
      SELECT COUNT (1)
        INTO l_pass_imp1
        FROM xxpso_ego_bom_stg stg
       WHERE stg.batch_id = NVL (gn_batch_id, stg.batch_id)
         AND stg.status_code = 'P';

      SELECT DISTINCT flv.meaning status
                 INTO l_status
                 FROM apps.fnd_concurrent_requests fcr,
                      apps.fnd_lookup_values flv
                WHERE fcr.request_id = gn_request_id
                  AND flv.lookup_code = fcr.status_code
                  AND flv.lookup_type = 'CP_STATUS_CODE'
                  AND flv.LANGUAGE = 'US'
                  AND ROWNUM = 1;

      fnd_file.put_line
         (fnd_file.output,
          'Program Name                  : MAS BOM Bill Of Material Conversion'
         );
      fnd_file.put_line (fnd_file.output,
                            'Date Started                  : '
                         || TO_CHAR (g_sysdate, 'DD-MON-YYYY HH24:MI:SS')
                        );
      fnd_file.put_line (fnd_file.output,
                            'Date Completed                : '
                         || TO_CHAR (SYSDATE, 'DD-MON-RRRR HH24:MI:SS')
                        );
      fnd_file.put_line (fnd_file.output,
                         'Status                        : ' || l_status
                        );
      fnd_file.put_line (fnd_file.output,
                         'Request Id                    : ' || gn_request_id
                        );
      fnd_file.put_line (fnd_file.output,
                         'No. of Records Processed      :' || l_tot_val1
                        );
      fnd_file.put_line (fnd_file.output, 'No. of Records Processed ');
      fnd_file.put_line (fnd_file.output,
                         'Successfullly                 : ' || l_pass_imp1
                        );
      fnd_file.put_line (fnd_file.output,
                         'No. of Records Errored Out    : ' || l_err_val1
                        );
      fnd_file.put_line (fnd_file.output, CHR (10));
      fnd_file.put_line
         (fnd_file.output,
          '-----------------------------------------------------------------------------------------------------------------------------------------'
         );

      FOR cur_error_data_rec IN c_error_data
      LOOP
         fnd_file.put_line (fnd_file.output, CHR (10));
         fnd_file.put_line
            (fnd_file.output,
             '----------------------------------------------------------------------'
            );
         fnd_file.put_line (fnd_file.output,
                               'Batch ID              : '
                            || cur_error_data_rec.batch_id
                           );
         fnd_file.put_line (fnd_file.output,
                               'Record ID             : '
                            || cur_error_data_rec.record_id
                           );
         fnd_file.put_line (fnd_file.output,
                               'Error                 : '
                            || cur_error_data_rec.error_message
                           );
      END LOOP;
   EXCEPTION
      WHEN OTHERS
      THEN
         l_err_message :=
               'Error : XXPSO_PRINT_OUT_P procedure encounter error. '
            || SUBSTR (SQLERRM, 1, 150);
         xxpso_print_debug (l_err_message,gv_log);
         g_retcode := 2;
   END xxpso_print_out_p;


--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
-- Name       : capture_error
-- Description: This procedure is to Log an error in Common Error Table.
--+============================================================|
--| Modification History
--+============================================================|
--| Date                 Who             Description
--| ----------- --------------------  -------------------------
--| 10-Mar-2016  Monica Yekula           Initial Creation
--------------------------------------------------------------------------------
   PROCEDURE capture_error (
      p_ricew_id         IN   VARCHAR2,
      p_pri_identifier   IN   VARCHAR2,
      p_sec_identifier   IN   VARCHAR2,
      p_ter_identifier   IN   VARCHAR2,
      p_error_code       IN   VARCHAR2,
      p_error_column     IN   VARCHAR2,
      p_error_value      IN   VARCHAR2,
      p_error_desc       IN   VARCHAR2,
      p_req_action       IN   VARCHAR2,
      p_data_source      IN   VARCHAR2
   )
   IS
      lc_msg   VARCHAR2 (4000) := NULL;
   BEGIN
      xxpso_cmn_cnv_pkg.log_error_msg
                           (p_ricew_id            => p_ricew_id,
                            p_track               => 'CMN',
                            p_source              => 'CONVERSION',
                            p_calling_object      => 'XXPSO_BOM_BILL_MTL_CNV_PKG',
                            p_pri_record_id       => p_pri_identifier,
                            p_sec_record_id       => p_sec_identifier,
                            p_ter_record_id       => p_ter_identifier,
                            p_err_code            => p_error_code,
                            p_err_column          => p_error_column,
                            p_err_value           => p_error_value,
                            p_err_desc            => p_error_desc,
                            p_rect_action         => p_req_action,
                            p_debug_flag          => 'N',
                            p_request_id          => fnd_global.conc_request_id
                           );
   EXCEPTION
      WHEN OTHERS
      THEN
         lc_msg :=
               'Unhandled Exception in capture_error procedure. Error Code: '
            || SQLCODE
            || ' -> '
            || SQLERRM;
         xxpso_print_debug (lc_msg,gv_log);
   END capture_error;




    FUNCTION get_interface_error (p_inf_table_name VARCHAR2, p_transaction_id NUMBER)
        RETURN VARCHAR2
    IS
        CURSOR c_err_data
        IS
            SELECT DISTINCT table_name, error_message , unique_id
              FROM mtl_interface_errors
             WHERE transaction_id   = p_transaction_id
             ORDER BY unique_id;
             --  AND table_name       = p_inf_table_name ;

            l_error_message     VARCHAR2 (4000);
    BEGIN
        FOR cur_err_rec IN c_err_data
        LOOP
            l_error_message     := SUBSTR( l_error_message||cur_err_rec.error_message||'; ', 1, 3999);
        END LOOP;

        RETURN (l_error_message);
    EXCEPTION WHEN OTHERS
    THEN
        l_error_message := 'Unhandled exception in get_interface_error. Error: '||SQLCODE||'->'||SQLERRM;
        xxpso_print_debug( l_error_message,gv_log);
        RETURN (NULL);
    END get_interface_error;

--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
-- Name       : perform_de_duplication
-- Description: This procedure is to find duplicate items based on the source system - master source system defined in lookup XXPSO_SURVIVORSHP
--+============================================================|
--| Modification History
--+============================================================|
--| Date                 Who             Description
--| ----------- --------------------  -------------------------
--| 10-Mar-2016  Monica Yekula           Initial Creation
--------------------------------------------------------------------------------
--Changes for v2.0
-- Deduplication will be handled in the query.
/*
   PROCEDURE perform_de_duplication
   IS
      lc_msg       VARCHAR2 (4000) := NULL;
      ln_row_cnt   NUMBER          := 0;
   BEGIN
      xxpso_print_debug
          ('****************************************************************',gv_log);
      xxpso_print_debug
          ('***         PERFORMING BOM DEDUPLICATION - STARTED           ***',gv_log);
      xxpso_print_debug
          ('****************************************************************',gv_output);
      xxpso_print_debug
          ('***         PERFORMING BOM DEDUPLICATION - STARTED           ***',gv_output);
          

      -- Finding out duplicate item withing the staging table
      UPDATE xxpso_ego_bom_stg ss1
         SET ss1.sim_source_system_assembly =
                (SELECT MAX (ss2.source_system_assembly)
                   FROM xxpso_ego_bom_stg ss2, fnd_lookups lkp
                  WHERE lkp.lookup_type = 'XXPSO_SURVIVORSHP'
                    AND lkp.enabled_flag = 'Y'
                    AND TRUNC (SYSDATE)
                           BETWEEN TRUNC (NVL (lkp.start_date_active,
                                               SYSDATE - 1
                                              )
                                         )
                               AND TRUNC (NVL (lkp.end_date_active,
                                               SYSDATE + 1
                                              )
                                         )
                    AND lkp.meaning <> lkp.description
                    AND ss1.source_system_assembly =
                                                    lkp.meaning
                                                               -- Secondary SS
                    AND ss2.source_system_assembly =
                                                   lkp.description
                                                                  -- Master SS
                    AND ss2.asse_src_itm_num = ss1.asse_src_itm_num
                    AND ss2.organization_code = ss1.organization_code
                    AND ss2.status_code = 'N')
       WHERE EXISTS (
                SELECT 1
                  FROM xxpso_ego_bom_stg ss2, fnd_lookups lkp
                 WHERE lkp.lookup_type = 'XXPSO_SURVIVORSHP'
                   AND lkp.enabled_flag = 'Y'
                   AND TRUNC (SYSDATE)
                          BETWEEN TRUNC (NVL (lkp.start_date_active,
                                              SYSDATE - 1
                                             )
                                        )
                              AND TRUNC (NVL (lkp.end_date_active,
                                              SYSDATE + 1)
                                        )
                   AND lkp.meaning <> lkp.description
                   AND ss1.source_system_assembly = lkp.meaning
                                                               -- Secondary SS
                   AND ss2.source_system_assembly =
                                                   lkp.description
                                                                  -- Master SS
                   AND ss2.asse_src_itm_num = ss1.asse_src_itm_num
                   AND ss2.organization_code = ss1.organization_code
                   AND ss2.status_code = 'N')
         AND ss1.status_code = 'N'
         AND ss1.sim_source_system_assembly IS NULL;

      ln_row_cnt := NVL (SQL%ROWCOUNT, 0);
      xxpso_print_debug
         (   'No of Items for which Duplicate Found within Staging Table      : '
          || ln_row_cnt,gv_log
         );
      COMMIT;
      xxpso_print_debug
           ('***         PERFORMING BOM DEDUPLICATION - COMPLETED         ***',gv_log);
      xxpso_print_debug
           ('****************************************************************',gv_log);
      xxpso_print_debug
           ('***         PERFORMING BOM DEDUPLICATION - COMPLETED         ***',gv_output);
      xxpso_print_debug
           ('****************************************************************',gv_output);           
   EXCEPTION
      WHEN OTHERS
      THEN
         lc_msg :=
               'Unhandled exception in perform_de_duplication. Error: '
            || SQLCODE
            || '->'
            || SQLERRM;
         xxpso_print_debug (lc_msg,gv_log);
   END perform_de_duplication;
*/

--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
-- Name       : XXPSO_ASSIGN_BATCH_P
-- Description: This procedure controls the batch.
--+============================================================|
--| Modification History
--+============================================================|
--| Date                 Who             Description
--| ----------- --------------------  -------------------------
--| 10-Mar-2016  		Monica Yekula           Initial Creation
--| 8-Aug-2016		2.0	Akshay Nayak	   a) Changes for surviorship logic.
--|						   b) Changes for Performance Improvement.
--|						   c) Splitting data into master and child table seperately.
--------------------------------------------------------------------------------
--Commented earlier logic and created new procedure with new logic.
/*
   PROCEDURE xxpso_assign_batch_p(p_mode IN VARCHAR2)
   IS
      ln_row_cnt   NUMBER := 0;
   BEGIN
      gn_batch_id := xxpso_bom_bill_mtl_stg_s1.NEXTVAL;
      xxpso_print_debug (   '+   PROCEDURE : assign_batch_id - gn_batch_id :'
                         || gn_batch_id,gv_log
                        );
      xxpso_print_debug
                 (   '+   PROCEDURE : Inside assign_batch_id - Process Mode :'
                  || gc_process_flag,gv_log
                 );

      UPDATE xxpso_ego_bom_stg
         SET batch_id           = gn_batch_id,
             last_updated_by    = gn_user_id,
             last_update_login  = gn_login_id,
             last_update_date   = SYSDATE,
             conc_request_id    = gn_request_id,
             error_message      = NULL
       WHERE (   CASE WHEN (UPPER(gc_record_status)  = 'NEW') AND (status_code IN ('N') )
                      THEN 1
                      WHEN (UPPER(gc_record_status)  = 'FAILED') AND (status_code IN ('E', 'F', 'I') )
                      THEN 1
                      WHEN (UPPER(gc_record_status)  = 'VALID') AND (status_code IN ('V') )
                      THEN 1
                      WHEN (UPPER(gc_record_status)  = 'ALL') AND (status_code IN ('N', 'V', 'E', 'F', 'I') )
                      THEN 1
                      ELSE 0
             END ) = 1
         AND sim_source_system_assembly IS NULL;

      ln_row_cnt := NVL (SQL%ROWCOUNT, 0);
      COMMIT;

      IF ln_row_cnt = 0
      THEN
          UPDATE xxpso_ego_bom_stg
             SET batch_id           = gn_batch_id,
                 last_updated_by    = gn_user_id,
                 last_update_login  = gn_login_id,
                 last_update_date   = SYSDATE,
                 conc_request_id    = gn_request_id,
                 error_message      = NULL
           WHERE (   CASE WHEN (UPPER(gc_record_status)  = 'NEW') AND (status_code IN ('N') )
                          THEN 1
                          WHEN (UPPER(gc_record_status)  = 'FAILED') AND (status_code IN ('E', 'F', 'I') )
                          THEN 1
                          WHEN (UPPER(gc_record_status)  = 'VALID') AND (status_code IN ('V') )
                          THEN 1
                          WHEN (UPPER(gc_record_status)  = 'ALL') AND (status_code IN ('N', 'V', 'E', 'F', 'I') )
                          THEN 1
                          ELSE 0
                 END ) = 1
            AND sim_source_system_assembly IS NOT NULL;
      END IF;

      COMMIT;
      xxpso_print_debug (   'Updated staging table where batch id is null'
                         || SQL%ROWCOUNT,gv_log
                        );
   EXCEPTION
      WHEN OTHERS
      THEN
         g_retcode := 2;
         xxpso_print_debug (   'Unexpected Error Occured during assign batch'
                            || SQL%ROWCOUNT,gv_log
                           );
   END xxpso_assign_batch_p;
*/
  PROCEDURE xxpso_assign_batch_p(p_mode IN VARCHAR2)
  IS
  BEGIN
    gn_batch_id  := xxpso_bom_bill_mtl_stg_s1.NEXTVAL;
    
    xxpso_print_debug ('+   PROCEDURE : assign_batch_id - gn_batch_id :' || gn_batch_id||
    		       ' p_mode:'||p_mode,gv_log );

   IF p_mode IN ( gv_validate_mode) THEN
   xxpso_print_debug ('In Validate:xxpso_assign_batch_p ',gv_log);
    UPDATE xxpso_ego_bom_assembly_stg
    SET batch_id = gn_batch_id
      , last_updated_by = gn_user_id
      , last_update_login = gn_login_id
      , last_update_date = SYSDATE
      , CONC_REQUEST_ID = gn_request_id
      , error_message = NULL
    WHERE  record_id IN 
    (
    	SELECT record_id FROM (
    		SELECT stg.record_id ,
    		       lookup.lookup_code seq_number
    		  FROM xxpso_ego_bom_assembly_stg stg
    		      , FND_LOOKUP_VALUES lookup
    		 WHERE stg.batch_id IS NULL
    		   AND stg.status_code = gv_new_flag --Newly entered records.
    		   AND (gn_follow_survivorship = 'Y' OR (gn_follow_survivorship = 'N' AND ASSEMBLY_SOURCE_SYSTEM = gn_source_system))
		   AND lookup.lookup_type = gn_survivorship_lkp_name 
		   AND lookup.enabled_flag = 'Y'
		   AND sysdate between NVL(lookup.start_date_active,sysdate) AND NVL(lookup.end_date_active,sysdate+1)
    		   AND stg.ASSEMBLY_SOURCE_SYSTEM = lookup.description						    
    		 ORDER BY seq_number
    	)
    	WHERE ROWNUM <= DECODE (gn_batch_size,NULL,100000000000,gn_batch_size) -- Changes for v2.0
    );
   ELSIF p_mode IN ( gv_import_mode) THEN
    xxpso_print_debug ('In Import:xxpso_assign_batch_p ',gv_log);
    UPDATE xxpso_ego_bom_assembly_stg
    SET batch_id = gn_batch_id
      , last_updated_by = gn_user_id
      , last_update_login = gn_login_id
      , last_update_date = SYSDATE
      , CONC_REQUEST_ID = gn_request_id
      , error_message = NULL
    WHERE 
    	  record_id IN 
    	  (
    	  	SELECT record_id 
    	  	  FROM (
			SELECT stg.record_id ,
			       lookup.lookup_code seq_number
			  FROM xxpso_ego_bom_assembly_stg stg
			      , FND_LOOKUP_VALUES lookup
			 WHERE batch_id IS NOT NULL 
			   AND status_code = gv_valid_flag -- Validated records.
			   --Changes for v2.0 to fetch details for specific source system
			   AND (gn_follow_survivorship = 'Y' OR (gn_follow_survivorship = 'N' AND ASSEMBLY_SOURCE_SYSTEM = gn_source_system ))
			   AND lookup.lookup_type = gn_survivorship_lkp_name 
			   AND lookup.enabled_flag = 'Y'
			   AND sysdate between NVL(lookup.start_date_active,sysdate) AND NVL(lookup.end_date_active,sysdate+1)
			   AND stg.ASSEMBLY_SOURCE_SYSTEM = lookup.description	
			   ORDER BY seq_number
    	  	)
    	  	WHERE ROWNUM <= DECODE (gn_batch_size,NULL,100000000000,gn_batch_size) -- Changes for v2.0
    	  );
   ELSIF p_mode = gv_revalidate_mode THEN
    UPDATE xxpso_ego_bom_assembly_stg
    SET batch_id = gn_batch_id
      , last_updated_by = gn_user_id
      , last_update_login = gn_login_id
      , last_update_date = SYSDATE
      , CONC_REQUEST_ID = gn_request_id
      , error_message = NULL
      , status_code = gv_new_flag
    WHERE 
     record_id IN 
    (
    	SELECT record_id 
    	  FROM (
		SELECT stg.record_id ,
		       lookup.lookup_code seq_number
		  FROM xxpso_ego_bom_assembly_stg stg
		      , FND_LOOKUP_VALUES lookup
		 WHERE batch_id IS NOT NULL 
		   AND status_code IN (gv_error_flag,gv_failed_flag,gv_staged_flag) 
		   AND (gn_follow_survivorship = 'Y' OR (gn_follow_survivorship = 'N' AND ASSEMBLY_SOURCE_SYSTEM = gn_source_system))
		   AND lookup.lookup_type = gn_survivorship_lkp_name
		   AND lookup.enabled_flag = 'Y'
		   AND sysdate between NVL(lookup.start_date_active,sysdate) AND NVL(lookup.end_date_active,sysdate+1)
		   AND stg.ASSEMBLY_SOURCE_SYSTEM = lookup.description	
		 ORDER BY seq_number
    	)
    	WHERE ROWNUM <= DECODE (gn_batch_size,NULL,100000000000,gn_batch_size) -- Changes for v2.0
    );
   END IF;
   
   --Update batch_id in Component Staging table.
   FOR cur_update_component IN (SELECT record_id FROM xxpso_ego_bom_assembly_stg WHERE batch_id = gn_batch_id)
   LOOP
    UPDATE xxpso_ego_bom_component_stg
       SET batch_id = gn_batch_id
         , last_updated_by = gn_user_id
         , last_update_login = gn_login_id
         , last_update_date = SYSDATE
         , CONC_REQUEST_ID = gn_request_id
         , error_message = NULL
         , status_code = DECODE(p_mode,gv_validate_mode,gv_new_flag,gv_import_mode,gv_valid_flag
         			      ,gv_revalidate_mode,gv_new_flag)
     WHERE assembly_record_id = cur_update_component.record_id;
	END LOOP;
    COMMIT;

  EXCEPTION
    WHEN OTHERS THEN
      xxpso_print_debug ('Error while assigning batch id: '||SQLERRM, gv_log);
  END xxpso_assign_batch_p;
  

--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
-- Name       : purge_if_tables
-- Description: This procedure is to purge interface tables based on the parameter
--+============================================================|
--| Modification History
--+============================================================|
--| Date                 Who             Description
--| ----------- --------------------  -------------------------
--| 22-APR-2016  Monica Yekula           Initial Creation
--------------------------------------------------------------------------------
   PROCEDURE purge_if_tables (p_purge_if_tables IN VARCHAR2)
   IS
      l_err_msg   VARCHAR2 (4000) := NULL;
   BEGIN
      xxpso_print_debug
          ('****************************************************************',gv_log);
      xxpso_print_debug
           ('****         PURGE INTERFACE TABLES - STARTED             *****',gv_log);

      IF UPPER (p_purge_if_tables) = 'ALL'
      THEN
         EXECUTE IMMEDIATE ' TRUNCATE TABLE BOM.bom_bill_of_mtls_interface ';

         EXECUTE IMMEDIATE 'TRUNCATE TABLE BOM.bom_inventory_comps_interface ';

         EXECUTE IMMEDIATE 'TRUNCATE TABLE INV.MTL_INTERFACE_ERRORS ';
      ELSIF UPPER (p_purge_if_tables) = 'PROCESSED'
      THEN
         DELETE FROM inv.mtl_interface_errors
               WHERE transaction_id IN (
                        SELECT transaction_id
                          FROM bom.bom_bill_of_mtls_interface
                         WHERE process_flag = 7
                        UNION
                        SELECT transaction_id
                          FROM bom.bom_inventory_comps_interface
                         WHERE process_flag = 7);

         DELETE FROM bom.bom_inventory_comps_interface
               WHERE process_flag = 7;

         DELETE FROM bom.bom_bill_of_mtls_interface
               WHERE process_flag = 7;
      END IF;

      COMMIT;
      xxpso_print_debug
           ('****         PURGE INTERFACE TABLES - COMPLETED            *****',gv_log);
      xxpso_print_debug
           ('****************************************************************',gv_log);
   EXCEPTION
      WHEN OTHERS
      THEN
         l_err_msg :=
               'Unexpected Error, while purging interface tables SQLERRM : '
            || SQLERRM;
         xxpso_print_debug (l_err_msg,gv_log);
   END purge_if_tables;


--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
-- Name       : identify_archive_records
-- Description: This procedure is identify records to archive
--+============================================================|
--| Modification History
--+============================================================|
--| Date                 Who             Description
--| ----------- --------------------  -------------------------
--| 22-APR-2016  Monica Yekula           Initial Creation
--------------------------------------------------------------------------------
   PROCEDURE identify_archive_records
   IS
      l_err_msg   VARCHAR2 (4000) := NULL;
   BEGIN
      xxpso_print_debug
          ('****************************************************************',gv_log);
      xxpso_print_debug
          ('****        IDENTIFYING ARCHIVE RECORDS - STARTED          *****',gv_log);

      UPDATE xxpso_ego_bom_stg ss1
         SET ss1.ready_to_archive = 'Y'
       WHERE ss1.status_code IN ('P');

      COMMIT;

      UPDATE xxpso_ego_bom_stg ss1
         SET ss1.ready_to_archive = 'Y'
       WHERE EXISTS (
                SELECT 1
                  FROM xxpso_ego_bom_stg ss2
                 WHERE 1=1
                   AND ss2.organization_code        = ss1.organization_code
                   AND ss2.source_system_assembly   = ss1.source_system_assembly
                   AND ss2.asse_src_itm_num         = ss1.asse_src_itm_num
                   AND ss2.source_system_component  = ss1.source_system_component
                   AND ss2.comp_src_itm_num         = ss1.comp_src_itm_num
                   AND ss2.status_code = 'N'
                     )
         AND ss1.status_code IN ('E', 'F');

      COMMIT;
      xxpso_print_debug
           ('****        IDENTIFYING ARCHIVE RECORDS - COMPLETED        *****',gv_log);
      xxpso_print_debug
           ('****************************************************************',gv_log);
   EXCEPTION
      WHEN OTHERS
      THEN
         l_err_msg :=
               'Unhandled exception in identify_archive_records. Error: '
            || SQLCODE
            || '->'
            || SQLERRM;
         xxpso_print_debug (l_err_msg,gv_log);
   END identify_archive_records;

--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
-- Name       : XXPSO_VAL_DATA_P
-- Description: This procedure validates staging table data
--+============================================================|
--| Modification History
--+============================================================|
--| Date                 Who             Description
--| ----------- --------------------  -------------------------
--| 10-Mar-2016  Monica Yekula           Initial Creation
--------------------------------------------------------------------------------
   PROCEDURE xxpso_val_data_p(pv_err_flag OUT VARCHAR2,
   				       pv_status1 IN VARCHAR2,
   				       pv_status2 IN VARCHAR2,
   				       pv_status3 IN VARCHAR2)
   AS
      /* Local Variables */
      
      CURSOR c_bom_assembly_item
      IS
         SELECT   assembly_stg.*
             FROM xxpso_ego_bom_assembly_stg assembly_stg
             , FND_LOOKUP_VALUES lookup --Changes for v2.0
            WHERE assembly_stg.batch_id = gn_batch_id
	      AND (gn_follow_survivorship = 'Y' OR (gn_follow_survivorship = 'N' AND assembly_stg.ASSEMBLY_SOURCE_SYSTEM = gn_source_system))
	      AND lookup.lookup_type = gn_survivorship_lkp_name 
	      AND lookup.enabled_flag = 'Y'
	      AND sysdate between NVL(lookup.start_date_active,sysdate) AND NVL(lookup.end_date_active,sysdate+1)
    	      AND assembly_stg.assembly_source_system = lookup.description	              
              ORDER by lookup.lookup_code, assembly_stg.record_id;

      TYPE l_asmb_tab_type IS TABLE OF c_bom_assembly_item%ROWTYPE
         INDEX BY BINARY_INTEGER;

      l_asmb_tab_tbl             l_asmb_tab_type;
      
      CURSOR c_bom_component_item(in_record_id NUMBER)
      IS
      SELECT *
        FROM xxpso_ego_bom_component_stg component_stg
       WHERE ASSEMBLY_RECORD_ID = in_record_id
         AND batch_id = gn_batch_id;

      --LOCAL VARIABLE DECLARATION
        lv_record_status 	       VARCHAR2(1);
        lv_component_record_status     VARCHAR2(1);
        ln_component_failed_count	NUMBER   := 0;
        lv_err_msg	               VARCHAR2(4000);  
        lv_component_err_msg	       VARCHAR2(4000);
	lv_inventory_item_flag 		VARCHAR2(1);
	lv_bom_enabled_flag    		VARCHAR2(1);
	ln_bom_item_type       		NUMBER;
	ln_count	       		NUMBER; 
 	ln_component_item_id		NUMBER;
 	lv_component_segment1		VARCHAR2(100);
	lv_comp_inv_item_flag		VARCHAR2(1);
	lv_comp_enabled_flag 		VARCHAR2(1);
	ln_comp_item_type		NUMBER;		
	lv_uom				VARCHAR2(25);
	ln_comp_cnt			NUMBER;
	lv_comp_transaction_type	VARCHAR2(25);
	ld_old_effectivity_date		DATE;
	ln_total_assembly_count		NUMBER;
	ln_valid_assembly_count		NUMBER;
	ln_invalid_assembly_count	NUMBER;
	ln_total_component_count	NUMBER;
	ln_valid_component_count	NUMBER;
	ln_invalid_component_count	NUMBER;	
	ln_component_sequence_id	NUMBER;
  BEGIN
      xxpso_print_debug ('',gv_log);
      xxpso_print_debug ('',gv_log);
      xxpso_print_debug ('Inside Validate Procedure. Batch Id:'||gn_batch_id,gv_log);


     /****************************************************************
      			Validation of Assembly Source System and 
      			Deriving the Assembly Source System Id
      ********************************************************/
      /* If assembly_source_system is null mark those records as error */
      UPDATE xxpso_ego_bom_assembly_stg
         SET error_message = 'Assembly Source System is NULL',
             status_code   = gv_error_flag,
		last_update_date = SYSDATE,
		last_updated_by = gn_user_id,
		last_update_login = gn_login_id,
		conc_request_id = gn_request_id                
       WHERE assembly_source_system IS NULL
         AND batch_id = gn_batch_id;
      COMMIT;
	      
      UPDATE xxpso_ego_bom_assembly_stg stg
         SET stg.assembly_source_system_id =
                (SELECT orig_system_id
                   FROM hz_orig_systems_vl
                  WHERE orig_system = stg.assembly_source_system
                )
       WHERE batch_id = gn_batch_id
         AND assembly_source_system IS NOT NULL;
      COMMIT;
      
      UPDATE xxpso_ego_bom_assembly_stg
         SET error_message = 'Invalid Source System for Assembly',
             status_code   = gv_error_flag,
		last_update_date = SYSDATE,
		last_updated_by = gn_user_id,
		last_update_login = gn_login_id,
		conc_request_id = gn_request_id                
       WHERE (assembly_source_system_id IS NULL AND assembly_source_system IS NOT NULL)
         AND batch_id = gn_batch_id;
      COMMIT;

      
     /****************************************************************
      			Validation of Component Source System and 
      			Deriving the Component Source System Id
      **********************************************************************/	
      /* If assembly_source_system is null mark those records as error */
      UPDATE xxpso_ego_bom_component_stg
         SET error_message = 'Component Source System is NULL',
             status_code   = gv_error_flag,
		last_update_date = SYSDATE,
		last_updated_by = gn_user_id,
		last_update_login = gn_login_id,
		conc_request_id = gn_request_id                
       WHERE component_source_system IS NULL
         AND batch_id = gn_batch_id;
      COMMIT;
      
      UPDATE xxpso_ego_bom_component_stg stg
         SET stg.component_source_system_id =
                (SELECT orig_system_id
                   FROM hz_orig_systems_vl
                  WHERE orig_system = component_source_system
                 )
       WHERE batch_id = gn_batch_id
         AND component_source_system IS NOT NULL;
      COMMIT;

      
      UPDATE xxpso_ego_bom_component_stg stg
         SET error_message = 'Invalid Source System for Component',
             status_code   = gv_error_flag,
		last_update_date = SYSDATE,
		last_updated_by = gn_user_id,
		last_update_login = gn_login_id,
		conc_request_id = gn_request_id                
       WHERE (component_source_system_id IS NULL AND component_source_system IS NOT NULL)
         AND batch_id = gn_batch_id;
      COMMIT;
      			 
     
     /********************************************************************
      			Validation of Organization Code in Assembly table
      *********************************************************************/
      
      /* If organization_code is null mark those records as error */
      UPDATE xxpso_ego_bom_assembly_stg
         SET error_message = error_message ||'~Organization_code is NULL',
             status_code   = gv_error_flag,
		last_update_date = SYSDATE,
		last_updated_by = gn_user_id,
		last_update_login = gn_login_id,
		conc_request_id = gn_request_id                
       WHERE organization_code IS NULL
         AND batch_id = gn_batch_id;
      COMMIT;
      
      UPDATE xxpso_ego_bom_assembly_stg stg
         SET stg.organization_id =
                           (SELECT organization_id
                              FROM mtl_parameters
                             WHERE organization_code = stg.organization_code)
       WHERE batch_id = gn_batch_id
         AND organization_code IS NOT NULL;
      COMMIT;
   

      UPDATE xxpso_ego_bom_assembly_stg
         SET error_message  = error_message || '~Invalid Organization Code',
             status_code    = gv_error_flag,
		last_update_date = SYSDATE,
		last_updated_by = gn_user_id,
		last_update_login = gn_login_id,
		conc_request_id = gn_request_id                
       WHERE (organization_id IS NULL AND organization_code IS NOT NULL)
         AND batch_id = gn_batch_id;
      COMMIT;

     /*******************************************************
      			Validation of Organization Code in Component table
      ********************************************************/
      /* If organization_code is null mark those records as error */
      UPDATE xxpso_ego_bom_component_stg
         SET error_message = error_message ||'~Organization_code is NULL',
             status_code   = gv_error_flag,
		last_update_date = SYSDATE,
		last_updated_by = gn_user_id,
		last_update_login = gn_login_id,
		conc_request_id = gn_request_id                
       WHERE organization_code IS NULL
         AND batch_id = gn_batch_id;
      COMMIT;
      
      UPDATE xxpso_ego_bom_component_stg stg
         SET stg.organization_id =
                           (SELECT organization_id
                              FROM mtl_parameters
                             WHERE organization_code = stg.organization_code)
       WHERE batch_id = gn_batch_id
         AND organization_code IS NOT NULL;
      COMMIT;

      UPDATE xxpso_ego_bom_component_stg
         SET error_message  = error_message || '~Invalid Organization Code',
             status_code    = gv_error_flag,
		last_update_date = SYSDATE,
		last_updated_by = gn_user_id,
		last_update_login = gn_login_id,
		conc_request_id = gn_request_id                
       WHERE (organization_id IS NULL AND organization_code IS NOT NULL)
         AND batch_id = gn_batch_id;
      COMMIT;      


     /*********************************************************************************
      			Validation of Structure Type Name and derivation
      				of structure type Id
      ********************************************************************************/
      /* If organization_code is null mark those records as error */
      UPDATE xxpso_ego_bom_assembly_stg
         SET error_message = error_message ||'~Structure Type Name is NULL',
             status_code   = gv_error_flag,
		last_update_date = SYSDATE,
		last_updated_by = gn_user_id,
		last_update_login = gn_login_id,
		conc_request_id = gn_request_id                
       WHERE structure_type_name IS NULL
         AND batch_id = gn_batch_id;
      COMMIT;
        
      UPDATE xxpso_ego_bom_assembly_stg stg
         SET stg.structure_type_id =
                       (SELECT structure_type_id
                          FROM bom_structure_types_b
                         WHERE structure_type_name = stg.structure_type_name)
       WHERE batch_id = gn_batch_id
         AND structure_type_name IS NOT NULL;
      COMMIT;


	UPDATE xxpso_ego_bom_assembly_stg
	SET error_message = error_message || '~Invalid Structure Type Name',
		status_code = gv_error_flag,
		last_update_date = SYSDATE,
		last_updated_by = gn_user_id,
		last_update_login = gn_login_id,
		conc_request_id = gn_request_id
       WHERE structure_type_id IS NULL
       AND batch_id = gn_batch_id;
      COMMIT;
      

     /*********************************************************************************
      			Validation of Structure Name and derivation
      ********************************************************************************/
         UPDATE xxpso_ego_bom_assembly_stg
           SET  error_message  = error_message || '~Invalid Structure Name',
                status_code    = gv_error_flag,
	        last_update_date = SYSDATE,
	        last_updated_by = gn_user_id,
	        last_update_login = gn_login_id,
	        creation_date = SYSDATE,
	        created_by = gn_user_id,
	        conc_request_id = gn_request_id,                
	        batch_id = gn_batch_id             
       WHERE structure_name NOT IN (
                           SELECT NVL (alternate_designator_code, description)
                             FROM bom_alternate_designators)
       AND batch_id = gn_batch_id;
      COMMIT;

     /*********************************************************************************
      			Validate that Assembly Item and Component Item is not same
      ********************************************************************************/  
        UPDATE xxpso_ego_bom_assembly_stg assembly_stg
           SET  error_message  = error_message || '~Parent Part Number and Component part number are same',
                status_code    = gv_error_flag,
	        last_update_date = SYSDATE,
	        last_updated_by = gn_user_id,
	        last_update_login = gn_login_id,
	        conc_request_id = gn_request_id
       WHERE batch_id = gn_batch_id
       AND assembly_stg.assembly_source_system_id IS NOT NULL
       AND EXISTS (SELECT 1 
                     FROM xxpso_ego_bom_component_stg component_stg
       		    WHERE component_stg.assembly_record_id = assembly_stg.record_id
       		      AND component_stg.component_src_item_number = assembly_stg.assembly_src_item_number
       		      AND component_stg.component_source_system_id = assembly_stg.assembly_source_system_id
       		      AND component_stg.component_source_system_id IS NOT NULL
       		      AND batch_id = gn_batch_id
       		  );
      COMMIT;

     /*********************************************************************************
      			Checking Duplicates
      ********************************************************************************/
         UPDATE xxpso_ego_bom_assembly_stg stg
            SET status_code     = gv_error_flag,
                error_message   = error_message || '~Duplicate record exists in the assembly staging table',
	        last_update_date = SYSDATE,
	        last_updated_by = gn_user_id,
	        last_update_login = gn_login_id,
	        conc_request_id = gn_request_id
          WHERE EXISTS
                   (SELECT 1
                      FROM xxpso_ego_bom_assembly_stg dup_stg
                     WHERE dup_stg.organization_code = stg.organization_code
                       AND dup_stg.bom_transaction_type = stg.bom_transaction_type
                       AND dup_stg.assembly_source_system = stg.assembly_source_system
                       AND dup_stg.assembly_src_item_number = stg.assembly_src_item_number
                       AND dup_stg.structure_name = stg.structure_name
                       AND dup_stg.structure_type_name = stg.structure_type_name
                       AND (
                       		(gc_mode IN (gv_validate_mode,gv_validate_process_mode) AND dup_stg.status_code IN (gv_new_flag,gv_valid_flag))
                       	      OR(gc_mode = gv_revalidate_mode AND dup_stg.status_code IN (gv_error_flag,gv_failed_flag,gv_valid_flag))
                       	   )
                       AND dup_stg.record_id <> stg.record_id
                    )
            AND stg.batch_id = gn_batch_id;

         COMMIT;
         
	 xxpso_print_debug ('',gv_log);
	 xxpso_print_debug ('',gv_log);
      /* Main Cursor for all BOM related Assembly Items */
      OPEN c_bom_assembly_item;
      LOOP
         l_asmb_tab_tbl.DELETE;
      FETCH c_bom_assembly_item
      BULK COLLECT INTO l_asmb_tab_tbl LIMIT gcn_asmb_bulk_limit;
      EXIT WHEN l_asmb_tab_tbl.count = 0;
    
      FOR ln_indx IN l_asmb_tab_tbl.FIRST .. l_asmb_tab_tbl.LAST
      LOOP
      	--Since some of the Assembly Items are already validated there might 
      	-- be some records which is errored.
      	IF l_asmb_tab_tbl (ln_indx).status_code IN ( gv_new_flag,gv_valid_flag) THEN
      		lv_record_status := gv_valid_flag;
      	ELSIF l_asmb_tab_tbl (ln_indx).status_code = gv_error_flag THEN
      		lv_record_status := gv_invalid_flag;
      	END IF;
	lv_err_msg	  		:= NVL(l_asmb_tab_tbl (ln_indx).error_message,NULL);
	lv_inventory_item_flag 		:= NULL;
	lv_bom_enabled_flag    		:= NULL;
	ln_bom_item_type       		:= NULL;
	ln_count	       		:= 0;
	xxpso_print_debug ('In Main Assembly loop: record_id:'||l_asmb_tab_tbl (ln_indx).record_id||
			   ' lv_record_status:'||lv_record_status||
			   ' organization_id:'||l_asmb_tab_tbl (ln_indx).organization_id ||
			   ' assembly_src_item_number:'||l_asmb_tab_tbl (ln_indx).assembly_src_item_number||
			   ' assembly_source_system_id:'||l_asmb_tab_tbl (ln_indx).assembly_source_system_id||
			   ' lv_err_msg:'||lv_err_msg,gv_log);
	IF l_asmb_tab_tbl (ln_indx).error_message IS NOT NULL
	THEN
	    capture_error
	       (gc_assembly_item_ricew_id,
		l_asmb_tab_tbl (ln_indx).batch_id,
		l_asmb_tab_tbl (ln_indx).record_id,
		NULL,
		NULL,
		NULL,
		NULL ,
		l_asmb_tab_tbl (ln_indx).error_message,
		'Check Source Data',
		NULL
	       );
	  xxpso_print_debug (lv_err_msg,gv_log);
	END IF;

						  
	-- ASSEMBLY ITEM VALIDATION

	IF l_asmb_tab_tbl (ln_indx).organization_id IS NOT NULL
	AND l_asmb_tab_tbl (ln_indx).assembly_source_system_id IS NOT NULL
	THEN
	  -- FETCH THE INVENTORY_ITEM_ID DETAILS  FOR THE GIVEN  ASSEMBLY SOURCE ITEM NUMBER
	  BEGIN
	     SELECT inventory_item_id,
		    segment1,
		    inventory_item_flag, bom_enabled_flag,
		    bom_item_type
	       INTO l_asmb_tab_tbl (ln_indx).assembly_item_id,
		    l_asmb_tab_tbl (ln_indx).assembly_item_number,
		    lv_inventory_item_flag, lv_bom_enabled_flag,
		    ln_bom_item_type
	       FROM mtl_system_items_b
	      WHERE inventory_item_id =
		       (SELECT inventory_item_id
			  FROM mtl_cross_references x_ref
			 WHERE x_ref.cross_reference_type = gv_cross_reference_type
			   AND x_ref.cross_reference = l_asmb_tab_tbl (ln_indx).assembly_src_item_number
			   AND x_ref.source_system_id = l_asmb_tab_tbl (ln_indx).assembly_source_system_id
			   AND sysdate between NVL(x_ref.start_date_active,sysdate) AND NVL(x_ref.end_date_active,sysdate+1)
			   AND NVL(x_ref.organization_id,1) = DECODE(x_ref.org_independent_flag , 
			   					'Y',NVL(x_ref.organization_id,1),
		  						'N',l_asmb_tab_tbl (ln_indx).organization_id )
		       )
		AND organization_id = l_asmb_tab_tbl (ln_indx).organization_id;
		xxpso_print_debug ('lv_assembly_inventory_item_flag:'||lv_inventory_item_flag||
				   ' lv_assembly_bom_enabled_flag:'||lv_bom_enabled_flag,gv_log);
	     IF    lv_inventory_item_flag <> 'Y'
		OR lv_bom_enabled_flag <> 'Y'
	     THEN
		lv_record_status := gv_invalid_flag;
		lv_err_msg :=  lv_err_msg
		   || '~Either Assembly Item is not BOM enabled or not an inventory item';
		xxpso_print_debug (lv_err_msg,gv_log);
		    capture_error
		       (gc_assembly_item_ricew_id,
			l_asmb_tab_tbl (ln_indx).batch_id,
			l_asmb_tab_tbl (ln_indx).record_id,
			NULL,
			NULL,
			'ASSEMBLY_SRC_ITEM_NUMBER - ASSEMBLY_SOURCE_SYSTEM',
			l_asmb_tab_tbl (ln_indx).assembly_src_item_number||'-'||l_asmb_tab_tbl (ln_indx).assembly_source_system,
			'Either Assembly Item is not BOM enabled or not an inventory item',
			'Check Source Data and Setup',
			NULL
		       );		
	      END IF;
	  EXCEPTION
	     WHEN NO_DATA_FOUND
	     THEN
		lv_err_msg :=  lv_err_msg || '~Invalid Assembly Item Number; ';
		lv_record_status := gv_invalid_flag;
		xxpso_print_debug ('No Data Found error while fetching assembly details:'||lv_err_msg,gv_log);
		    capture_error
		       (gc_assembly_item_ricew_id,
			l_asmb_tab_tbl (ln_indx).batch_id,
			l_asmb_tab_tbl (ln_indx).record_id,
			NULL,
			NULL,
			'ASSEMBLY_SRC_ITEM_NUMBER - ASSEMBLY_SOURCE_SYSTEM',
			l_asmb_tab_tbl (ln_indx).assembly_src_item_number||'-'||l_asmb_tab_tbl (ln_indx).assembly_source_system,
			'Invalid Assembly Item Number',
			'Check Source Data and Setup',
			NULL
		       );		
	     WHEN OTHERS
	     THEN
		lv_err_msg :=  lv_err_msg ||'~Unexpected error while validating Assembly Item Number. SQLERRM: '|| SQLERRM || '; ';
		lv_record_status := gv_invalid_flag;
		xxpso_print_debug ('Unexpected error while fetching assembly details:'||lv_err_msg,gv_log);
		    capture_error
		       (gc_assembly_item_ricew_id,
			l_asmb_tab_tbl (ln_indx).batch_id,
			l_asmb_tab_tbl (ln_indx).record_id,
			NULL,
			NULL,
			'ASSEMBLY_SRC_ITEM_NUMBER - ASSEMBLY_SOURCE_SYSTEM',
			l_asmb_tab_tbl (ln_indx).assembly_src_item_number||'-'||l_asmb_tab_tbl (ln_indx).assembly_source_system,
			'Unexpected error while validating Invalid Assembly Item Number. SQLERRM: '|| SQLERRM,
			'Check Source Data and Setup',
			NULL
		       );		
	  END;
       END IF;
	
	xxpso_print_debug ('assembly_item_id:'||l_asmb_tab_tbl (ln_indx).assembly_item_id||
			   ' record_id:'||l_asmb_tab_tbl (ln_indx).record_id||
			   ' organization_id:'||l_asmb_tab_tbl (ln_indx).organization_id||
			   ' alternate_bom_designator'||l_asmb_tab_tbl (ln_indx).alternate_bom_designator,gv_log);
	
       -- VALIDATION TO CHECK IF BOM ALREADY EXISTS and UPDATE THE BOM TRANSACTION TYPE IN THE STG TABLE.
	   -- Changes as per V2.0
	   -- Transaction type will be computed during transfering data into interface table.
	   /*
       IF     l_asmb_tab_tbl (ln_indx).assembly_item_id IS NOT NULL
	  AND l_asmb_tab_tbl (ln_indx).organization_id IS NOT NULL
       THEN
	     SELECT COUNT (1)
	       INTO ln_count
	       FROM bom_bill_of_materials
	      WHERE assembly_item_id = l_asmb_tab_tbl (ln_indx).assembly_item_id
		AND organization_id =  l_asmb_tab_tbl (ln_indx).organization_id
		AND NVL(alternate_bom_designator,'X') = NVL(l_asmb_tab_tbl (ln_indx).alternate_bom_designator,'X');

	     IF ln_count > 0
	     THEN
		l_asmb_tab_tbl (ln_indx).bom_transaction_type := gv_update_trn;
		
		SELECT bill_sequence_id
		  INTO l_asmb_tab_tbl (ln_indx).bill_sequence_id
		  FROM bom_bill_of_materials
	         WHERE assembly_item_id = l_asmb_tab_tbl (ln_indx).assembly_item_id
		   AND organization_id =  l_asmb_tab_tbl (ln_indx).organization_id
		   AND NVL(alternate_bom_designator,'X') = NVL(l_asmb_tab_tbl (ln_indx).alternate_bom_designator,'X');
		
	     ELSE
		l_asmb_tab_tbl (ln_indx).bom_transaction_type := gv_create_trn;
	     END IF;
       END IF;
       
       xxpso_print_debug ('bom_transaction_type:'||l_asmb_tab_tbl (ln_indx).bom_transaction_type||
       			  ' ln_bill_sequence_id:'||l_asmb_tab_tbl (ln_indx).bill_sequence_id||
       		          ' record_id:'||l_asmb_tab_tbl (ln_indx).record_id,gv_log);
		*/
       --No need of actual failed components record. Just need to know that there is no errored components thus setting it to 1.
       ln_component_failed_count := 0;
       FOR cur_component_rec IN c_bom_component_item(l_asmb_tab_tbl (ln_indx).record_id)
       LOOP
       	  xxpso_print_debug ('Component For Loop: Component Assembly Id:'||cur_component_rec.assembly_record_id||
       	  		     ' Component Record Id:'||cur_component_rec.record_id||
       	  		     ' organization_id:'||cur_component_rec.organization_id ||
       	  		     ' component_src_item_number:'||cur_component_rec.component_src_item_number||
			     ' component_source_system_id:'||cur_component_rec.component_source_system_id||
			     ' ln_component_failed_count:'||ln_component_failed_count||
       	  		     ' status_code:'||cur_component_rec.status_code||
       	  		     ' error_message:'||cur_component_rec.error_message,gv_log);
	
	
      	IF cur_component_rec.status_code IN ( gv_new_flag , gv_valid_flag ) THEN
      		lv_component_record_status := gv_valid_flag;
      	ELSIF cur_component_rec.status_code = gv_error_flag THEN
      		lv_component_record_status := gv_invalid_flag;
      		ln_component_failed_count := 1;
      	END IF;
	lv_component_err_msg   		:= NVL(cur_component_rec.error_message,NULL);
 	ln_component_item_id		:= NULL;
 	lv_component_segment1		:= NULL;
 	ln_component_sequence_id	:= NULL;
	lv_comp_inv_item_flag		:= NULL;
	lv_comp_enabled_flag 		:= NULL;
	ln_comp_item_type		:= NULL;
	lv_uom				:= NULL;
	ln_comp_cnt			:= 0;
	
	ld_old_effectivity_date		:= NULL;


	-- COMPONENT ITEM NUMBER VALIDATION
       IF     cur_component_rec.organization_id IS NOT NULL
	  AND cur_component_rec.component_source_system_id IS NOT NULL
       THEN
	  BEGIN
	     SELECT inventory_item_id, segment1,
		    inventory_item_flag, bom_enabled_flag,
		    bom_item_type, primary_unit_of_measure
	       INTO ln_component_item_id,
		    lv_component_segment1,
		    lv_comp_inv_item_flag, lv_comp_enabled_flag,
		    ln_comp_item_type, lv_uom
	       FROM mtl_system_items_b
	      WHERE inventory_item_id =
		       (SELECT inventory_item_id
			  FROM mtl_cross_references x_ref
			 WHERE x_ref.cross_reference_type = gv_cross_reference_type
			   AND x_ref.cross_reference = cur_component_rec.component_src_item_number
			   AND x_ref.source_system_id = cur_component_rec.component_source_system_id
			   AND sysdate between NVL(x_ref.start_date_active,sysdate) AND NVL(x_ref.end_date_active,sysdate+1)
			   AND NVL(x_ref.organization_id,1) = DECODE(x_ref.org_independent_flag , 
			   					'Y',NVL(x_ref.organization_id,1),
		  						'N',cur_component_rec.organization_id )
			)
		AND organization_id = cur_component_rec.organization_id;
	     xxpso_print_debug (' lv_comp_enabled_flag:'||lv_comp_enabled_flag||
	     			' ln_comp_item_type:'||ln_comp_item_type ||
	     			' ln_bom_item_type:'||ln_bom_item_type ,gv_log);
	     			
	     IF lv_comp_enabled_flag <> 'Y'
	     THEN
	     	ln_component_failed_count := 1;
	        lv_component_record_status := gv_invalid_flag;
		lv_component_err_msg := lv_component_err_msg || '~Component item is not BOM enabled';
		xxpso_print_debug (lv_component_err_msg,gv_log);
		    capture_error
		       (gc_component_item_ricew_id,
			cur_component_rec.batch_id,
			cur_component_rec.record_id,
			NULL,
			NULL,
			'COMPONENT_SRC_ITEM_NUMBER-COMPONENT_SOURCE_SYSTEM',
			cur_component_rec.component_src_item_number||'-'||cur_component_rec.component_source_system,
			' Component item is not BOM enabled ',
			'Check Source Data and Setup',
			NULL
		       );		
	     END IF;

	     IF ln_bom_item_type = 4 AND ln_comp_item_type = 1
	     THEN
	        ln_component_failed_count := 1;
	        lv_component_record_status := gv_invalid_flag;
		lv_component_err_msg := lv_component_err_msg 
		|| '~Parent Part Number is a standard item. Such items can only have Standard items as components; ';
		xxpso_print_debug (lv_component_err_msg,gv_log);
		    capture_error
		       (gc_component_item_ricew_id,
			cur_component_rec.batch_id,
			cur_component_rec.record_id,
			NULL,
			NULL,
			'COMPONENT_SRC_ITEM_NUMBER-COMPONENT_SOURCE_SYSTEM',
			cur_component_rec.component_src_item_number||'-'||cur_component_rec.component_source_system,
			' Parent Part Number is a standard item.Such items can only have Standard items as components. ',
			'Check Source Data and Setup',
			NULL
		       );			
	     END IF;
	     
	     IF  ln_component_item_id = l_asmb_tab_tbl (ln_indx).assembly_item_id THEN
	     
	        ln_component_failed_count := 1;
	        lv_component_record_status := gv_invalid_flag;
		lv_component_err_msg := lv_component_err_msg 
		|| '~Parent Part Number and Component part number are same;';
		xxpso_print_debug (lv_component_err_msg,gv_log);
		    capture_error
		       (gc_component_item_ricew_id,
			cur_component_rec.batch_id,
			cur_component_rec.record_id,
			NULL,
			NULL,
			'COMPONENT_SRC_ITEM_NUMBER-COMPONENT_SOURCE_SYSTEM',
			cur_component_rec.component_src_item_number||'-'||cur_component_rec.component_source_system,
			' Parent Part Number and Component part number are same. ',
			'Check Source Data and Setup',
			NULL
		       );	
		     
	     END IF;
	  EXCEPTION
	     WHEN NO_DATA_FOUND
	     THEN
	        ln_component_failed_count := 1;
	        lv_component_record_status := gv_invalid_flag;
		lv_component_err_msg := lv_component_err_msg ||	'~Invalid Component Item Number';  
		xxpso_print_debug (lv_component_err_msg,gv_log);
		    capture_error
		       (gc_component_item_ricew_id,
			cur_component_rec.batch_id,
			cur_component_rec.record_id,
			NULL,
			NULL,
			'COMPONENT_SRC_ITEM_NUMBER-COMPONENT_SOURCE_SYSTEM',
			cur_component_rec.component_src_item_number||'-'||cur_component_rec.component_source_system,
			'Invalid Component Item Number',
			'Check Source Data',
			NULL
		       );			
	     WHEN OTHERS
	     THEN
	        ln_component_failed_count := 1;
	        lv_component_record_status := gv_invalid_flag;
		lv_component_err_msg := lv_component_err_msg ||	'~ Unexpected error while validating Component item : '||SQLERRM;  
		xxpso_print_debug (lv_component_err_msg,gv_log);
		    capture_error
		       (gc_component_item_ricew_id,
			cur_component_rec.batch_id,
			cur_component_rec.record_id,
			NULL,
			NULL,
			'COMPONENT_SRC_ITEM_NUMBER-COMPONENT_SOURCE_SYSTEM',
			cur_component_rec.component_src_item_number||'-'||cur_component_rec.component_source_system,
			' Unexpected error while validating Component item. SQLERRM: '
		    || SQLERRM,
			'Check Source Data',
			NULL
		       );	     
	  END;
       END IF;   
       
       xxpso_print_debug ('ln_component_item_id:'||ln_component_item_id||
       			  ' operation_seq_num:'||cur_component_rec.operation_seq_num||
       			  ' assembly_item_id:'||l_asmb_tab_tbl (ln_indx).assembly_item_id||
       			  ' alternate_bom_designator:'||l_asmb_tab_tbl (ln_indx).alternate_bom_designator
       			  ,gv_log);
       -- VALIDATION TO CHECK IF COMPONENT ITEMS EXIST IN THIS BOM
	   -- As per changes for v2.0 transaction type will be determined while transfering data to the 
	   -- interface table.
	/*
       IF     ln_component_item_id IS NOT NULL
	  AND cur_component_rec.operation_seq_num IS NOT NULL
       THEN
         -- To check if the record exists or no we need to consider only component_item_id and
         -- operation_seq_num.
 	  SELECT COUNT (1)
	    INTO ln_comp_cnt
	    FROM bom_bill_of_materials bom,
		 bom_inventory_components bic
	   WHERE bom.assembly_item_id   = l_asmb_tab_tbl (ln_indx).assembly_item_id
	     AND bom.organization_id    = l_asmb_tab_tbl (ln_indx).organization_id
	     AND NVL(bom.alternate_bom_designator,'X') = NVL(l_asmb_tab_tbl (ln_indx).alternate_bom_designator,'X')
	     AND bic.operation_seq_num  = cur_component_rec.operation_seq_num
	     AND bic.component_item_id  = ln_component_item_id
	     AND bom.bill_sequence_id   = bic.bill_sequence_id;
	     --AND NVL (TRUNC (bic.disable_date), SYSDATE) = NVL(TRUNC (cur_component_rec.disable_date),SYSDATE);
	  xxpso_print_debug (' Component exists count: ln_comp_cnt:'||ln_comp_cnt,gv_log);
	  IF ln_comp_cnt > 0
	  THEN
	     lv_comp_transaction_type := gv_update_trn;
		/*	     
	     --FETCH OLD START EFFECTIVE DATE, OPERATION SEQUENCE OF THE COMPONENT IF THE COMPONENT TRANSACTION TYPE = 'UPADTE'.
		  BEGIN
		     SELECT effectivity_date,component_sequence_id
		       INTO ld_old_effectivity_date,ln_component_sequence_id
		       FROM bom_bill_of_materials bbom,
			    bom_inventory_components bic
		      WHERE bbom.assembly_item_id   = l_asmb_tab_tbl (ln_indx).assembly_item_id
			AND bbom.organization_id    = l_asmb_tab_tbl (ln_indx).organization_id
			AND NVL(bbom.alternate_bom_designator,'X') = NVL(l_asmb_tab_tbl (ln_indx).alternate_bom_designator,'X')
			AND bic.operation_seq_num   = cur_component_rec.operation_seq_num
			AND bic.component_item_id   = ln_component_item_id
			AND bbom.bill_sequence_id   = bic.bill_sequence_id
			AND NVL (TRUNC (bic.disable_date), SYSDATE) = NVL(TRUNC (cur_component_rec.disable_date),SYSDATE);
		  EXCEPTION
		     WHEN OTHERS
		     THEN
		        ln_component_failed_count := 1;
		        lv_component_record_status := gv_invalid_flag;
			lv_component_err_msg := lv_component_err_msg 
			||'~Unexpected error while deriving existing Operation Seq Number and Effectivity Date : ' || SQLERRM;
			xxpso_print_debug (lv_component_err_msg,gv_log);
			    capture_error
			       (gc_item_ricew_id,
				cur_component_rec.batch_id,
				cur_component_rec.record_id,
				NULL,
				NULL,
				'COMPONENT_SRC_ITEM_NUMBER',
				cur_component_rec.component_src_item_number,
				'~Unexpected error while deriving existing Operation Seq Number and Effectivity Date : ' || SQLERRM,
				'Check Source Data',
				NULL
			       ); 		
		  END;	     *--/
	  ELSE
	     lv_comp_transaction_type := gv_create_trn;
	  END IF; 
       END IF;  
		*/
       		
       		xxpso_print_debug ('Updating Components record id:'||cur_component_rec.record_id||
       				   ' lv_record_status:'||lv_record_status||
       				   ' lv_component_record_status:'||lv_component_record_status||
       				   ' ln_component_failed_count:'||ln_component_failed_count ||
       				   ' start_effective_date:'||cur_component_rec.start_effective_date
       				   ,gv_log);
       		
       		UPDATE xxpso_ego_bom_component_stg
       		   SET component_item_id = ln_component_item_id
       		      ,assembly_item_id = l_asmb_tab_tbl (ln_indx).assembly_item_id
       		      ,component_item_number = lv_component_segment1
       		      ,comp_transaction_type = NULL--lv_comp_transaction_type
       		      ,start_effective_date  = NVL(cur_component_rec.start_effective_date,SYSDATE)
       		      ,status_code     = DECODE(lv_component_record_status,gv_invalid_flag,gv_error_flag,gv_valid_flag,gv_valid_flag)
		      ,error_message   = DECODE(lv_component_record_status,gv_invalid_flag,'~Error from Component:'||lv_component_err_msg
		      			 ,gv_valid_flag,NULL)
		      ,last_update_date = SYSDATE
		      ,last_updated_by = gn_user_id
		      ,last_update_login = gn_login_id
	              ,conc_request_id = gn_request_id
	              ,interface_batch_id = NULL
       		 WHERE record_id = cur_component_rec.record_id
       		   AND batch_id = gn_batch_id;
       		
       END LOOP;--End of Component Loop
       xxpso_print_debug ('Updating Assembly Component Record Id:'||l_asmb_tab_tbl (ln_indx).record_id||
       			  ' ln_component_failed_count:'||ln_component_failed_count||
       			  ' lv_record_status:'||lv_record_status
       			  ,gv_log);
		IF ln_component_failed_count <> 0 THEN
		    capture_error
		       (gc_assembly_item_ricew_id,
			l_asmb_tab_tbl (ln_indx).batch_id,
			l_asmb_tab_tbl (ln_indx).record_id,
			NULL,
			NULL,
			'RECORD_ID',
			l_asmb_tab_tbl (ln_indx).record_id,
			'There are some errors in component. Check individual component for error details',
			'Check Source Data and Setup',
			NULL
		       );		
		END IF;
       UPDATE xxpso_ego_bom_assembly_stg
          SET assembly_item_id = l_asmb_tab_tbl (ln_indx).assembly_item_id
             ,assembly_item_number = l_asmb_tab_tbl (ln_indx).assembly_item_number
             ,bom_transaction_type = NULL-- l_asmb_tab_tbl (ln_indx).bom_transaction_type
             ,bill_sequence_id	=l_asmb_tab_tbl (ln_indx).bill_sequence_id
             ,status_code     = DECODE(ln_component_failed_count,0,DECODE(lv_record_status,gv_invalid_flag,gv_error_flag,gv_valid_flag),1,gv_error_flag)
	     			      --1,gv_invalid_flag)
	     ,error_message   = DECODE(ln_component_failed_count,0,DECODE(lv_record_status,gv_invalid_flag,lv_err_msg,NULL),1,
	     				lv_err_msg||'~There are some errors in component. Check individual component for error details')
	     			      --'Some errors exists in Component~'||lv_err_msg)
	     ,last_update_date = SYSDATE
	     ,last_updated_by = gn_user_id
	     ,last_update_login = gn_login_id
	     ,conc_request_id = gn_request_id
	     ,interface_batch_id = NULL
       	WHERE record_id = l_asmb_tab_tbl (ln_indx).record_id  
       	   AND batch_id = gn_batch_id;
       
       END LOOP;--End of Assembly loop
       
      END LOOP;-- End of Cursor loop
      CLOSE c_bom_assembly_item;
	COMMIT;
      xxpso_print_debug ('Validation Completed',gv_log);       

 /***************************************************************************************
   Generating the Output file
  ***************************************************************************************/
  SELECT COUNT(*)
    INTO ln_total_assembly_count
    FROM xxpso_ego_bom_assembly_stg
   WHERE batch_id = gn_batch_id;
   
  SELECT COUNT(*)
    INTO ln_valid_assembly_count
    FROM xxpso_ego_bom_assembly_stg
   WHERE batch_id = gn_batch_id
     AND status_code = gv_valid_flag;
   
  SELECT COUNT(*)
    INTO ln_invalid_assembly_count
    FROM xxpso_ego_bom_assembly_stg
   WHERE batch_id = gn_batch_id
     AND status_code IN (gv_error_flag);
     
  SELECT COUNT(*)
    INTO ln_total_component_count
    FROM xxpso_ego_bom_component_stg
   WHERE batch_id = gn_batch_id;
   
  SELECT COUNT(*)
    INTO ln_valid_component_count
    FROM xxpso_ego_bom_component_stg
   WHERE batch_id = gn_batch_id
     AND status_code = gv_valid_flag;
   
  SELECT COUNT(*)
    INTO ln_invalid_component_count
    FROM xxpso_ego_bom_component_stg
   WHERE batch_id = gn_batch_id
     AND status_code IN (gv_error_flag);        
   
      xxpso_print_debug (' Generating output file', gv_output);
      /*Printing Details into Output After Validating from the Staging Table*/
      xxpso_print_debug ('+--------------------------------------------------------------------------------+', gv_output);
      xxpso_print_debug ('Concurrent Request ID : '
         || gn_request_id
         || '            '
         || '    Begin Date : '
         || TO_CHAR (SYSDATE, 'DD-MON-RRRR HH:MI:SS'), gv_output);
     xxpso_print_debug ('+---------------------------------------------------------------------------------+', gv_output);
     xxpso_print_debug ('+---------------------------------------------------------------------------------+', gv_output);
     xxpso_print_debug ('Validated Records Summary', gv_output);
     xxpso_print_debug ('+---------------------------------------------------------------------------------+', gv_output);
     xxpso_print_debug ('--------------------------------VALIDATION DETAILS-------------------------------', gv_output);
     xxpso_print_debug( '*********************************************************************************************************', gv_output);
     xxpso_print_debug ( 'Total number of Assembly records processed in R12                           :   '
         || (ln_total_assembly_count), gv_output);
     xxpso_print_debug ('Total number of Assembly records validated from the Staging Table            :   '
         || ln_valid_assembly_count , gv_output);
     xxpso_print_debug ('Total number  of Assembly records failed during validation                   :  '
         || ln_invalid_assembly_count , gv_output);
     xxpso_print_debug ( 'Total number of Components records processed in R12                           :   '
         || (ln_total_component_count), gv_output);
     xxpso_print_debug ('Total number of Components records validated from the Staging Table            :   '
         || ln_valid_component_count , gv_output);
     xxpso_print_debug ('Total number  of Components records failed during validation                   :  '
         || ln_invalid_component_count , gv_output);         
     xxpso_print_debug  ('-----------------------------------------------------------------------------------------------------------',gv_output);
       
       /*
        * This Validation is moved outside the main block
       --VALIDATE IF BOTH ASSEMBLY SOURCE ITEM NUMBER AND COMPONENT ITEM NUMBER ARE SAME
       IF l_asmb_tab_tbl (ln_indx).asse_src_itm_num =
			     l_asmb_tab_tbl (ln_indx).comp_src_itm_num
       THEN
	 l_status_code := 'E';
	  l_err_msg_txt :=
		l_err_msg_txt
	     || 'Parent Part Number and Component part number are same; ';
	  xxpso_print_debug (l_err_msg_txt,gv_log);
	  capture_error
	     ('BOM-CNV-001',
	      l_asmb_tab_tbl (ln_indx).organization_code,
	      l_asmb_tab_tbl (ln_indx).asse_src_itm_num,
	      l_asmb_tab_tbl (ln_indx).comp_src_itm_num,
	      NULL,
	      'ASSE_SRC_ITM_NUM - COMP_SRC_ITM_NUM',
		 l_asmb_tab_tbl (ln_indx).asse_src_itm_num
	      || ' - '
	      || l_asmb_tab_tbl (ln_indx).comp_src_itm_num,
	      'Parent Part Number and Component part number are same',
	      'Check Source Data and Setup',
	      NULL
	     );
       END IF;
	*/
              
		/*
               l_asmb_tab_tbl (ln_indx).status_code         := l_status_code;
               l_asmb_tab_tbl (ln_indx).error_message       := l_asmb_tab_tbl (ln_indx).error_message || l_err_msg_txt;
		*/
            

-----------------------------------------------------------
-- Updating the Item Staging Table
-----------------------------------------------------------
/*
            BEGIN
               FORALL ln_indx IN INDICES OF l_asmb_tab_tbl SAVE EXCEPTIONS
                  UPDATE xxpso_ego_bom_stg
                     SET status_code                = l_asmb_tab_tbl (ln_indx).status_code,
                         error_message              = l_asmb_tab_tbl (ln_indx).error_message,
                         assmb_segment1             = l_asmb_tab_tbl (ln_indx).assmb_segment1,
                         comp_segment1              = l_asmb_tab_tbl (ln_indx).comp_segment1,
                         assembly_item_id           = l_asmb_tab_tbl (ln_indx).assembly_item_id,
                         component_item_id          = l_asmb_tab_tbl (ln_indx).component_item_id,
                         bom_transaction_type       = l_asmb_tab_tbl (ln_indx).bom_transaction_type,
                         comp_transaction_type      = l_asmb_tab_tbl (ln_indx).comp_transaction_type,
                         operation_seq_num          = NVL (l_old_operation_seq_num, l_asmb_tab_tbl (ln_indx).operation_seq_num),
                         component_quantity         = l_asmb_tab_tbl (ln_indx).component_quantity,
                         uom                        = l_asmb_tab_tbl (ln_indx).uom,
                         start_effective_date       = NVL (l_asmb_tab_tbl (ln_indx).start_effective_date,l_old_effectivity_date)
                   WHERE record_id                  = l_asmb_tab_tbl (ln_indx).record_id
                     AND batch_id                   = gn_batch_id;
               COMMIT;
            EXCEPTION
               WHEN OTHERS
               THEN
                  xxpso_print_debug ('Error while updating staging table.',gv_log);
            END;
            */


--UPDATE ERRORED RECORDS TO THE STAGING TABLE
/*
      UPDATE xxpso_ego_bom_stg stg
         SET stg.status_code    = 'E',
             stg.error_message  = error_message || 'Unable to process as an error occurred in one of the component associated with parent part number'
       WHERE status_code    = 'V'
         AND batch_id       = gn_batch_id
         AND EXISTS (
                SELECT 1
                  FROM xxpso_ego_bom_stg stg1
                 WHERE stg1.asse_src_itm_num        = stg.asse_src_itm_num
                   AND stg1.source_system_assembly  = stg.source_system_assembly
                   AND stg1.organization_code       = stg.organization_code
                   AND stg1.batch_id                = stg.batch_id
                   AND stg1.status_code             = 'E'
                   AND stg1.batch_id                = gn_batch_id
                   );

      COMMIT;
      */
      xxpso_print_debug ('Validate Proccedue Completed',gv_log);
   EXCEPTION
      WHEN OTHERS
      THEN
         xxpso_print_debug (   'Error In XXPSO_VAL_DATA_P: '
                            || SQLCODE
                            || '  '
                            || SQLERRM,gv_log
                           );
   END xxpso_val_data_p;

   --------------------------------------------------------------------------------
--------------------------------------------------------------------------------
-- Name       : XXPSO_IMPORT_DATA_P
-- Description: This procedure calls the standard program "Bills and Routings Interface".
--       Updates the processed and failed records in staging table.
----| Modification History
--+============================================================|
--| Date                 Who             Description
--| ----------- --------------------  -------------------------
--| 10-Mar-2016  Monica Yekula           Initial Creation
--------------------------------------------------------------------------------
   PROCEDURE xxpso_import_data_p(pv_err_flag OUT VARCHAR2)
   AS
      /*
      CURSOR cur_trx_status
      IS
         SELECT DISTINCT transaction_type, batch_id
                    FROM bom_bill_of_mtls_interface
                   WHERE process_flag = 1;*/
            
      CURSOR cur_trx_status
      IS
         SELECT DISTINCT batch_id,organization_id
                    FROM bom_bill_of_mtls_interface
                   WHERE process_flag = 1
                   ORDER BY BATCH_ID;
                   
      CURSOR cur_assembly (
         p_process_id      IN   NUMBER,
         p_import_req_id   IN   NUMBER
      )
      IS
           SELECT bbmi.assembly_item_id ,
                  bbmi.organization_id ,
                  bbmi.transaction_type,
                  bbmi.transaction_id,
                  process_flag
             FROM bom_bill_of_mtls_interface bbmi
            WHERE bbmi.batch_id     = p_process_id
              AND bbmi.request_id   = p_import_req_id;

      CURSOR cur_component (
         p_process_id      IN   NUMBER,
         p_import_req_id   IN   NUMBER
      )
      IS
           SELECT bici.assembly_item_id ,
                  bici.organization_id ,
                  bici.component_item_id ,
                  bici.item_num item_num,
                  bici.operation_seq_num,
                  bici.transaction_type,
                  bici.transaction_id,
                  process_flag
             FROM bom_inventory_comps_interface bici
            WHERE bici.batch_id     = p_process_id
              AND bici.request_id   = p_import_req_id;
              
      ln_processed_count 	NUMBER;
      ln_failed_count	 	NUMBER;
/*
      CURSOR cur_failed_assembly (
         p_process_id      IN   NUMBER,
         p_import_req_id   IN   NUMBER
      )
      IS
         SELECT   mie.error_message err_msg,
                  bbmi.assembly_item_id assembly_item_id,
                  bbmi.organization_id organization_id,
                  bbmi.transaction_type transaction_type, COUNT (1)
             FROM bom_bill_of_mtls_interface bbmi, mtl_interface_errors mie
            WHERE bbmi.batch_id = p_process_id
              AND UPPER (mie.table_name) = 'BOM_BILL_OF_MTLS_INTERFACE'
              AND mie.request_id = bbmi.request_id
              AND bbmi.transaction_id = mie.transaction_id
              AND bbmi.request_id = p_import_req_id
         GROUP BY mie.error_message,
                  'ERR_IMP',
                  bbmi.assembly_item_id,
                  bbmi.organization_id,
                  bbmi.transaction_type;
 */
      --
      -- CURSOR TO SELECT FAILED RECORDS FOR COMPONENT
/*       CURSOR cur_failed_component (
         p_process_id      IN   NUMBER,
         p_import_req_id   IN   NUMBER
      )
      IS
         SELECT mie.error_message err_msg,
                bici.assembly_item_id assembly_item_id,
                bici.organization_id organization_id,
                bici.component_item_id component_item_id,
                bici.item_num item_num,
                bici.operation_seq_num operation_seq_num,
                bici.transaction_type transaction_type
           FROM bom_inventory_comps_interface bici, mtl_interface_errors mie
          WHERE bici.batch_id = p_process_id
            AND UPPER (mie.table_name) = 'BOM_INVENTORY_COMPS_INTERFACE'
            AND mie.request_id = bici.request_id
            AND bici.transaction_id = mie.transaction_id
            AND bici.request_id = p_import_req_id; */

      --    Variable Declaration
      l_request_id_num           NUMBER           := 0;
      l_user_id                  NUMBER           := 0;
      l_resp_id                  NUMBER           := 0;
      l_resp_appl_id             NUMBER           := 0;
      l_get_req_status_bln       BOOLEAN          := FALSE;
      l_request_phase_out        VARCHAR2 (100);
      l_request_status_out       VARCHAR2 (10);
      l_dev_request_phase_out    VARCHAR2 (100);
      l_dev_request_status_out   VARCHAR2 (10);
      l_request_status_msg_out   VARCHAR2 (32000);
      l_err_count                NUMBER           := 0;
      l_process_count            NUMBER           := 0;
      l_error_count              NUMBER           := 0;
      l_inventory_item_id        NUMBER;
      l_object_version_number    NUMBER;
      l_err_msg                  VARCHAR2 (1000);
      l_error_msg                VARCHAR2 (4000);
      lv_failed_err_msg		 VARCHAR2 (4000);
      
      CURSOR cur_failed_assembly
      IS
      SELECT assembly_stg.record_id , interface_tbl.transaction_id , interface_tbl.process_flag
        FROM xxpso_ego_bom_assembly_stg assembly_stg
           , bom_bill_of_mtls_interface interface_tbl
       WHERE assembly_stg.batch_id = gn_batch_id
         AND assembly_stg.status_code = gv_staged_flag
         AND assembly_stg.interface_batch_id = interface_tbl.batch_id
         AND assembly_stg.ASSEMBLY_ITEM_ID = interface_tbl.ASSEMBLY_ITEM_ID
         AND assembly_stg.ORGANIZATION_ID = interface_tbl.ORGANIZATION_ID;
         
      CURSOR cur_failed_component
      IS
      SELECT component_stg.record_id , interface_tbl.transaction_id , interface_tbl.process_flag
        FROM xxpso_ego_bom_component_stg component_stg
           , bom_inventory_comps_interface interface_tbl
       WHERE component_stg.batch_id = gn_batch_id
         AND component_stg.status_code = gv_staged_flag
         AND component_stg.interface_batch_id = interface_tbl.batch_id
         AND component_stg.component_item_id = interface_tbl.component_item_id
         AND component_stg.operation_seq_num = interface_tbl.operation_seq_num
         AND component_stg.assembly_item_id = interface_tbl.assembly_item_id;
		 
	TYPE l_request_id_rec_type IS RECORD (request_id NUMBER);
	TYPE l_request_id_tbl_type IS TABLE OF l_request_id_rec_type INDEX BY BINARY_INTEGER;
	l_request_id_tbl_var	l_request_id_tbl_type;
	ln_request_id_index		NUMBER := 1;
   BEGIN

      l_err_msg := NULL;
      xxpso_print_debug ('Inside Import Program',gv_log);
      xxpso_print_debug ('Import Procedure Start Time:'||to_char(sysdate,'DD-MM-YYYY HH24:MI:SS'),gv_log);
      
      FOR cur_trx_status_rec IN cur_trx_status
      LOOP
         xxpso_print_debug (   'Submitting the Program with batch id as = '
                            || cur_trx_status_rec.batch_id,gv_log
                           );
         l_request_id_num := 0;
         l_get_req_status_bln := FALSE;

         -- SUBMIT REQUEST
         l_request_id_num :=
            fnd_request.submit_request ('BOM',
                                        'BMCOIN',
                                        'Bills Of Materials',
                                        NULL,
                                        --TO_CHAR(SYSDATE, 'YYYY/MM/DD HH24:MI:SS')
                                        FALSE,
                                        cur_trx_status_rec.organization_id,      -- Master Org ID
                                        '2',              -- All Organizations
                                        '2',                -- Import Routings
                                        '1',       -- Import Bills Of Material
                                        '2',          -- Delete Processed Rows
                                        cur_trx_status_rec.batch_id
                                                                   -- Batch ID
                                       );
         COMMIT;
         xxpso_print_debug ('Request ID for the Standard program= ' || l_request_id_num||
         		    ' Time:'||to_char(sysdate,'DD-MM-YYYY HH24:MI:SS'),gv_log);

			 IF l_request_id_num = 0
			 THEN
				l_err_msg :=
					  'Request Not Submitted. for batch_id: '
				   || cur_trx_status_rec.batch_id;

				UPDATE xxpso_ego_bom_assembly_stg
				   SET status_code      = gv_failed_flag,
					   error_message    = l_err_msg
				 WHERE status_code      = gv_staged_flag
				   AND batch_id         = gn_batch_id;

				COMMIT;
			ELSE
				l_request_id_tbl_var(ln_request_id_index).request_id := l_request_id_num;
				ln_request_id_index := ln_request_id_index+1;
			END IF;
		END LOOP;-- End of block raising seeded concurrent program.
		xxpso_print_debug ('ln_request_id_index after raising concurrent programs:'||l_request_id_tbl_var.COUNT,gv_log);
		
        -- ELSE
            -- Waiting to complete one set of concurrent request
			IF l_request_id_tbl_var.COUNT > 0 
			THEN 
            -- This API Wait for the request completion,
            -- Then return the request phase/status and completion message to the caller. Also call sleep between database checks.
            --
			FOR ln_idx IN l_request_id_tbl_var.FIRST .. l_request_id_tbl_var.LAST
			LOOP
            LOOP
               l_get_req_status_bln :=
                  fnd_concurrent.wait_for_request (l_request_id_tbl_var(ln_idx).request_id,
                                                   -- The request ID of the program to wait on
                                                   10,
                                                   --Time to wait between checks. This is the number of seconds to sleep. The default is 60 seconds
                                                   0,
                                                   --The maximum time in seconds to wait for the requests completion.
                                                   l_request_phase_out,
                                                   --The user friendly request phase from FND_LOOKUPS.
                                                   l_request_status_out,
                                                   --The user friendly request status from FND_LOOKUPS.
                                                   l_dev_request_phase_out,
                                                   --The request phase as a constant string that can be used for program logic comparisons.
                                                   l_dev_request_status_out,
                                                   --The request status as a constant string that can be used for program logic comparisons.
                                                   l_request_status_msg_out
                                                  --The completion message supplied if the request has completed.
                                                  );
               --COMMIT;
               xxpso_print_debug (   'l_request_phase_out      : '
                                  || l_request_phase_out,gv_log
                                 );
               xxpso_print_debug (   'l_request_status_out     : '
                                  || l_request_status_out,gv_log
                                 );
               xxpso_print_debug (   'l_dev_request_phase_out  : '
                                  || l_dev_request_phase_out,gv_log
                                 );
               xxpso_print_debug (   'l_dev_request_status_out : '
                                  || l_dev_request_status_out,gv_log
                                 );
               xxpso_print_debug (   'l_request_status_msg_out : '
                                  || l_request_status_msg_out,gv_log
                                 );
               xxpso_print_debug (   'Request completion time : '
                                  || to_char(sysdate,'DD-MM-YYYY HH24:MI:SS'),gv_log
                                 );
			   EXIT WHEN l_dev_request_phase_out = 'COMPLETE';
               DBMS_LOCK.SLEEP(10);
            --
            END LOOP;

            IF l_get_req_status_bln
            THEN
               IF    l_request_phase_out != 'Completed'
                  OR l_request_status_out IN
                                         ('Cancelled', 'Error', 'Terminated')
               THEN
                  l_err_msg :=
                        'Bill and Routing Interface did not complete Successfully for request_id: '
                     || l_request_id_tbl_var(ln_idx).request_id;
                  xxpso_print_debug (l_err_msg,gv_log);

                  UPDATE xxpso_ego_bom_assembly_stg
                     SET status_code = gv_failed_flag,
                         error_message = 'ERR_IMP' || '_WAIT_REQ' || l_err_msg
                   WHERE status_code = gv_staged_flag
                     AND batch_id = gn_batch_id;

                  COMMIT;
               END IF;
            END IF;                                     --l_get_req_status_bln
			
			END LOOP;
			
			END IF;
        -- END IF;

	/*
	 * Updating of staging table will be done after the for loop which raises
	 * seeded concurrent program */
	 /*
         FOR cur_assembly_rec IN cur_assembly (cur_trx_status_rec.batch_id,l_request_id_num)
         LOOP
             IF cur_assembly_rec.process_flag <> 7
             THEN
                l_error_msg := get_interface_error ('BOM_BILL_OF_MTLS_INTERFACE', cur_assembly_rec.transaction_id);
             ELSE
                l_error_msg := NULL;
             END IF;

            UPDATE xxpso_ego_bom_assembly_stg
               SET status_code          = DECODE(cur_assembly_rec.process_flag,7,gv_processed_flag,gv_failed_flag)
                   ,error_message        = l_error_msg
	      	  ,last_update_date = SYSDATE
		  ,last_updated_by = gn_user_id
		  ,last_update_login = gn_login_id
	          ,conc_request_id = gn_request_id                   
             WHERE assembly_item_id     = cur_assembly_rec.assembly_item_id
               AND organization_id      = cur_assembly_rec.organization_id
               AND status_code          = gv_staged_flag
               AND batch_id             = gn_batch_id;
         END LOOP;
         COMMIT;
	
	xxpso_print_debug ('Updating Component staging table: Start Time:'||to_char(sysdate,'DD-MM-YYYY HH24:MI:SS')||
		 		    ' Batch Id:'||cur_trx_status_rec.batch_id||
	 		    ' Request Id:'||l_request_id_num,gv_log);
         FOR cur_component_rec IN cur_component (cur_trx_status_rec.batch_id,l_request_id_num)
         LOOP
             IF cur_component_rec.process_flag <> 7
             THEN
                l_error_msg := get_interface_error ('BOM_INVENTORY_COMPS_INTERFACE', cur_component_rec.transaction_id);
             ELSE
                l_error_msg := NULL;
             END IF;
            UPDATE xxpso_ego_bom_component_stg
               SET status_code          = DECODE(cur_component_rec.process_flag,7,gv_processed_flag,gv_failed_flag)
                   ,error_message        = l_error_msg
	      	  ,last_update_date = SYSDATE
		  ,last_updated_by = gn_user_id
		  ,last_update_login = gn_login_id
	          ,conc_request_id = gn_request_id                   
             WHERE organization_id      = cur_component_rec.organization_id
               AND component_item_id    = cur_component_rec.component_item_id
               -- AND status_code          = 'I'
               AND batch_id             = gn_batch_id;
         END LOOP;
         COMMIT;
         
         xxpso_print_debug ('Updating Component staging table: End Time:'||to_char(sysdate,'DD-MM-YYYY HH24:MI:SS'),gv_log);
         UPDATE xxpso_ego_bom_assembly_stg assembly_stg
            SET status_code = gv_failed_flag
                ,error_message = error_message || '~There are some failures for components as well~'
	      	  ,last_update_date = SYSDATE
		  ,last_updated_by = gn_user_id
		  ,last_update_login = gn_login_id
	          ,conc_request_id = gn_request_id                
          WHERE batch_id = gn_batch_id
            AND EXISTS (
            		 SELECT 1 
            		   FROM xxpso_ego_bom_component_stg component_stg
            		  WHERE component_stg.batch_id = gn_batch_id
            		    AND component_stg.assembly_record_id = assembly_stg.record_id
            		    AND status_code = gv_failed_flag
            	       );
            	       
	 xxpso_print_debug ('Updating Assembly staging table: End Time:'||to_char(sysdate,'DD-MM-YYYY HH24:MI:SS'),gv_log);
	 */
	 -- Above commented code ends here.
	 
	 
         --FAILED ASSEMBLY CURSOR BEGINS
/*          FOR cur_failed_assembly_rec IN
            cur_failed_assembly (cur_trx_status_rec.batch_id,
                                 l_request_id_num)
         LOOP
            UPDATE xxpso_ego_bom_stg
               SET status_code          = 'F',
                   error_message        = cur_failed_assembly_rec.err_msg
             WHERE assembly_item_id     = cur_failed_assembly_rec.assembly_item_id
               AND organization_id      = cur_failed_assembly_rec.organization_id
               AND status_code          = 'I'
               AND batch_id             = gn_batch_id;

            COMMIT;
         END LOOP; */

         -- CUR_FAILED_COMPONENT CURSOR BEGINS
/*          FOR cur_failed_component_rec IN
            cur_failed_component (cur_trx_status_rec.batch_id,
                                  l_request_id_num
                                 )
         LOOP
            UPDATE xxpso_ego_bom_stg
               SET status_code          = 'F',
                   error_message        = cur_failed_component_rec.err_msg
             WHERE assembly_item_id     = cur_failed_component_rec.assembly_item_id
               AND organization_id      = cur_failed_component_rec.organization_id
               AND component_item_id    = cur_failed_component_rec.component_item_id
               AND status_code          = 'I'
               AND batch_id             = gn_batch_id;

            COMMIT;
         END LOOP; */



    
    xxpso_print_debug ('Updating assembly staging table for processed record: gn_batch_id:'||gn_batch_id||
    		       ' Time:'||to_char(sysdate,'DD-MM-YYYY HH24:MI:SS'),gv_log);
    UPDATE xxpso_ego_bom_assembly_stg stg1
       SET STATUS_CODE = gv_processed_flag
	 , error_message = NULL
	  ,last_update_date = SYSDATE
	  ,last_updated_by = gn_user_id
	  ,last_update_login = gn_login_id
	  ,conc_request_id = gn_request_id   	 
    WHERE batch_id = gn_batch_id
     AND status_code = gv_staged_flag
     AND EXISTS (SELECT 1 FROM bom_bill_of_mtls_interface stg2
	 			WHERE stg2.assembly_item_id = stg1.assembly_item_id
	 			AND  stg2.organization_id = stg1.organization_id
	 			AND  stg2.batch_id = stg1.interface_batch_id
	 			AND stg2.process_flag = 7
	 	 );
	
    xxpso_print_debug ('Updating component staging table for processed record: gn_batch_id:'||gn_batch_id||
    		       ' Time:'||to_char(sysdate,'DD-MM-YYYY HH24:MI:SS'),gv_log);	
    UPDATE xxpso_ego_bom_component_stg stg1
       SET STATUS_CODE = gv_processed_flag
	 , error_message = NULL
	  ,last_update_date = SYSDATE
	  ,last_updated_by = gn_user_id
	  ,last_update_login = gn_login_id
	  ,conc_request_id = gn_request_id   	 
    WHERE batch_id = gn_batch_id
     AND status_code = gv_staged_flag
     AND EXISTS (SELECT 1 FROM bom_inventory_comps_interface stg2
	 			WHERE stg2.component_item_id = stg1.component_item_id
	 			AND  stg2.organization_id = stg1.organization_id
	 			AND  stg2.operation_seq_num = stg1.operation_seq_num
	 			AND  stg2.batch_id = stg1.interface_batch_id
	 			AND  stg2.effectivity_date = stg1.START_EFFECTIVE_DATE
	 			AND stg2.process_flag = 7
	 	 );	 	 
	 COMMIT;

    xxpso_print_debug ('Updating assembly staging table for failed record: gn_batch_id:'||gn_batch_id||
    		       ' Time:'||to_char(sysdate,'DD-MM-YYYY HH24:MI:SS'),gv_log);	
	--Update error message for Assembly Items.
	FOR cur_assembly_rec IN cur_failed_assembly
	LOOP
	 xxpso_print_debug('Updating failed assembly. Assembly Record Id:'||cur_assembly_rec.record_id||
	 		   ' Interface transaction id:'||cur_assembly_rec.transaction_id||
	 		   ' Process Flag:'||cur_assembly_rec.process_flag,gv_log);
	 
	IF cur_assembly_rec.process_flag = 3 THEN--Failed record
	    lv_failed_err_msg := get_interface_error ('BOM_BILL_OF_MTLS_INTERFACE', cur_assembly_rec.transaction_id);
	    xxpso_print_debug('Assembly Error Message: lv_failed_err_msg:'||lv_failed_err_msg,gv_log);
	    UPDATE xxpso_ego_bom_assembly_stg stg1
	       SET STATUS_CODE = gv_failed_flag
		 , error_message = lv_failed_err_msg
		  ,last_update_date = SYSDATE
		  ,last_updated_by = gn_user_id
		  ,last_update_login = gn_login_id
		  ,conc_request_id = gn_request_id   	 
	    WHERE record_id = cur_assembly_rec.record_id;
	END IF;
	
	END LOOP;
	
	lv_failed_err_msg := NULL;
	
	
	 xxpso_print_debug ('Updating component staging table for failed record: gn_batch_id:'||gn_batch_id||
    		       ' Time:'||to_char(sysdate,'DD-MM-YYYY HH24:MI:SS'),gv_log);	
	--Update error message for Component Items.
	FOR cur_component_rec IN cur_failed_component
	LOOP
	 xxpso_print_debug('Updating failed assembly. Component Record Id:'||cur_component_rec.record_id||
	 		   ' Interface transaction id:'||cur_component_rec.transaction_id||
	 		   ' Process Flag:'||cur_component_rec.process_flag,gv_log);
	 
	IF cur_component_rec.process_flag = 3 THEN--Failed record
	    lv_failed_err_msg := get_interface_error ('BOM_INVENTORY_COMPS_INTERFACE', cur_component_rec.transaction_id);
	    xxpso_print_debug('Component Error Message: lv_failed_err_msg:'||lv_failed_err_msg,gv_log);
	    UPDATE xxpso_ego_bom_component_stg stg1
	       SET STATUS_CODE = gv_failed_flag
		 , error_message = lv_failed_err_msg
		  ,last_update_date = SYSDATE
		  ,last_updated_by = gn_user_id
		  ,last_update_login = gn_login_id
		  ,conc_request_id = gn_request_id   	 
	    WHERE record_id = cur_component_rec.record_id;
	END IF;
	
	END LOOP;
	
	UPDATE xxpso_ego_bom_assembly_stg assembly_stg
	       SET STATUS_CODE = gv_failed_flag
		 , error_message = error_message || '~Some of component also errored out.Check individual component for error information'
		  ,last_update_date = SYSDATE
		  ,last_updated_by = gn_user_id
		  ,last_update_login = gn_login_id
		  ,conc_request_id = gn_request_id   	 
	    WHERE batch_id = gn_batch_id
	      AND EXISTS (SELECT 1 
	                    FROM xxpso_ego_bom_component_stg component_stg
	                   WHERE component_stg.batch_id = assembly_stg.batch_id
	                     AND component_stg.assembly_record_id = assembly_stg.record_id
	                     AND component_stg.status_code = gv_failed_flag
	                  );

	 xxpso_print_debug ('End updating records in staging table: Time'||to_char(sysdate,'DD-MM-YYYY HH24:MI:SS'),gv_log);	
    		       

      --    UPDATING PROCESSED RECORDS IN STAGING TABLE
/*       UPDATE xxpso_ego_bom_stg
         SET status_code = 'P'
       WHERE status_code = 'I'
         AND batch_id = gn_batch_id; */

   EXCEPTION
      WHEN OTHERS
      THEN
         l_err_msg :=
               'Unexpected Error, while executing the Import Program. SQLERRM : '
            || SQLERRM;
         xxpso_print_debug (l_err_msg,gv_log);

	CAPTURE_ERROR(   gc_assembly_item_ricew_id
			,gn_batch_id
			,NULL
			,NULL
			,NULL
			,NULL
			,NULL
			,'Unexpected error in xxpso_import_data_p procedure .Error Code:'||SQLCODE||
							 ' Error Message:'||SQLERRM
			,'Contact Technical Team'
			,NULL
		   );

         g_retcode := 2;
         COMMIT;
   END xxpso_import_data_p;
   

   --------------------------------------------------------------------------------
--------------------------------------------------------------------------------
-- Name       : XXPSO_SEND_ACKNOWLEDGEMENT_P
-- Description: This procedure is responsible for sending acknowledgement for newly created
-- 		assembly_items
----| Modification History
--+============================================================|
--| Date                 Who             Description
--| ----------- --------------------  -------------------------
  --| 19-Sep-2016  Akshay Nayak           Initial Creation
--------------------------------------------------------------------------------   
   PROCEDURE xxpso_send_acknowledgement_p(pv_err_flag OUT VARCHAR2)
   AS
   
   CURSOR cur_bom_acknowledgement
   IS
	SELECT  assembly_stg.assembly_source_system assembly_source_system 
	        ,assembly_stg.assembly_src_item_number assembly_source_item_number 
	   	,assembly_stg.structure_name assembly_structure_name 
	  	,bom.bill_sequence_id mdm_reference_id
	  	,assembly_stg.received_from_etl	received_from_etl
	  	,assembly_stg.received_from_esb	received_from_esb
	  	,assembly_stg.record_id	record_id
	  	,assembly_stg.batch_id	batch_id
	FROM 	xxpso_ego_bom_assembly_stg assembly_stg,
	  	bom_bill_of_materials bom
	WHERE assembly_stg.status_code = gv_processed_flag
	AND assembly_stg.batch_id = gn_batch_id
	AND (--assembly_stg.mdm_reference_id                IS NULL-- mdm_reference_id null condition not needed.
		--OR 
		assembly_stg.bom_transaction_type               = gv_create_trn
		)
	AND assembly_stg.assembly_item_id                  = bom.assembly_item_id
	AND assembly_stg.organization_id                   = bom.organization_id
	AND NVL(assembly_stg.alternate_bom_designator,'X') = NVL(bom.alternate_bom_designator,'X');
	
	l_file_name     VARCHAR2(100)       := 'XXPSO_BOM_ACKNOWLEDGEMENT'; 
	g_sysdate	VARCHAR2(30);
	gc_dba_directory_name               CONSTANT    VARCHAR2(25)        := 'XXPSO_MDM_PDH_OUT';
	l_file           	UTL_FILE.file_type;
	
	v_doc           	xmldom.domdocument;
	v_main_node 		xmldom.domnode;
	v_parent_node           xmldom.domnode;
	v_product_list_ack	xmldom.domnode;
	v_product_ack		xmldom.domnode;
	v_bom_ack		xmldom.domnode;
	v_item_node		xmldom.domnode;
	v_clob          	CLOB := '';
  	lv_queue_name 		VARCHAR2(50) := 'XXPSO.XXPSO_PDH_ACK_QUEUE_OUT';
  	lv_error_message 	VARCHAR2(4000);	
  	lv_etl_ack_present  	VARCHAR2(1) := 'N';
	lv_etl_ack_setup_done	VARCHAR2(1) := 'N';
  	lv_esb_ack_present  	VARCHAR2(1) := 'N';
	lv_esb_ack_setup_done	VARCHAR2(1) := 'N';	
	
   BEGIN
   	g_sysdate        := TO_CHAR(SYSDATE,'DDMMYYYY_HH24MISS');
   	l_file_name := l_file_name ||'_'|| gn_request_id ||'_'|| g_sysdate ||'.csv';
   	FOR c IN cur_bom_acknowledgement
   	LOOP
   	 xxpso_print_debug ('Record Id: '||c.record_id,gv_log);
		IF c.received_from_etl = 'Y' THEN
			--Execute this block only once foe first time to generate file related setups.
			IF lv_etl_ack_present = 'N' AND lv_etl_ack_setup_done = 'N' THEN
				l_file := UTL_FILE.fopen (gc_dba_directory_name, l_file_name, 'w');
				
				xxpso_print_debug ('In xxpso_send_acknowledgement_p: l_file_name:'||l_file_name||
						   ' g_sysdate:'||g_sysdate ,gv_log); 
					UTL_FILE.put_line(  l_file,
							   ''||    'AssemblySourceSystem'                           
							||','||    'AssemblySourceItemNumber'                    
							||','||    'AssemblySourceStructureName'      
							||','||    'MDMReference'     
										 );  			
				lv_etl_ack_present := 'Y';
				lv_etl_ack_setup_done := 'Y';			
			END IF;
                UTL_FILE.put_line(  l_file,
                                    replace(c.assembly_source_system ,',','/,/')                
                        ||','||    replace(c.assembly_source_item_number   ,',','/,/')                    
                        ||','||    replace(c.assembly_structure_name ,',','/,/')        
                        ||','||    replace(c.mdm_reference_id,',','/,/')         
                            );   
			 UPDATE xxpso_ego_bom_assembly_stg SET mdm_reference_id = c.mdm_reference_id , ACK_SENT_BY_ETL = 'Y'
			  WHERE record_id = c.record_id;							
         ELSIF c.received_from_esb = 'Y' THEN
			--Execute this block only once foe first time to open clob objects.
			IF lv_esb_ack_present = 'N' AND lv_esb_ack_setup_done = 'N' THEN
				v_doc := xmldom.newdomdocument;
				v_main_node := xmldom.makenode (v_doc);	
				v_product_list_ack := XXPSO_PRODUCT_UTILITY_PKG.create_node_fnc (v_doc, v_main_node, 'ProductAcknowledgementList');
				v_product_ack    := XXPSO_PRODUCT_UTILITY_PKG.create_node_fnc (v_doc, v_product_list_ack, 'ProductAcknowledgement'); 
				lv_esb_ack_present := 'Y';
				lv_esb_ack_setup_done := 'Y';			
			END IF;
         	v_bom_ack       := XXPSO_PRODUCT_UTILITY_PKG.create_node_fnc   (v_doc, v_product_ack, 'BOMAcknowledgement');
         	v_item_node     := XXPSO_PRODUCT_UTILITY_PKG.add_text_node_fnc 
         				(v_doc, v_bom_ack, 'AssemblySourceSystem', c.assembly_source_system );
         	v_item_node     := XXPSO_PRODUCT_UTILITY_PKG.add_text_node_fnc 
         				(v_doc, v_bom_ack, 'AssemblySourceItemNumber', c.assembly_source_item_number );
         	v_item_node     := XXPSO_PRODUCT_UTILITY_PKG.add_text_node_fnc 
         				(v_doc, v_bom_ack, 'AssemblySourceStructureName', c.assembly_structure_name );
         	v_item_node     := XXPSO_PRODUCT_UTILITY_PKG.add_text_node_fnc 
         				(v_doc, v_bom_ack, 'MDMReference', c.mdm_reference_id );
			 UPDATE xxpso_ego_bom_assembly_stg SET mdm_reference_id = c.mdm_reference_id , ACK_SENT_BY_ESB = 'Y'
			  WHERE record_id = c.record_id;						
   	 END IF;
   	END LOOP;
   	
	IF lv_esb_ack_present = 'Y' THEN
		dbms_lob.createtemporary (v_clob, TRUE);
		xmldom.writetoclob (v_doc, v_clob);
		xmldom.freedocument (v_doc);

		IF v_clob <>  empty_clob() THEN
				v_clob := TO_CLOB('<?xml version="1.0" encoding="UTF-8"?>' || CHR(10) ) || v_clob;
				xxpso_print_debug(v_clob ,gv_log  );
			lv_error_message := XXPSO_PRODUCT_UTILITY_PKG.enqueue_payload(lv_queue_name,v_clob);
		END IF;
	END IF;
	
	IF lv_etl_ack_present = 'Y' THEN
		UTL_FILE.fclose (l_file);
	END IF;

   END xxpso_send_acknowledgement_p;
   
--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
-- Name       : XXPSO_TRANSFER_DATA_P
-- Description: This procedure inserts data into interface tables
--| Modification History
--+============================================================|
--| Date                 Who             Description
--| ----------- --------------------  -------------------------
--| 10-Mar-2016  Monica Yekula           Initial Creation
--------------------------------------------------------------------------------
   PROCEDURE xxpso_transfer_data_p(pv_err_flag OUT VARCHAR2)
   AS
      /* Local Variables */
      
    /*
      CURSOR c_transfer_assembly
      IS
         SELECT   batch_id, bom_transaction_type, assembly_item_id,
                  organization_id, bom_description, structure_type_name,
                  structure_name
             FROM xxpso_ego_bom_stg
            WHERE status_code = 'V'
              AND batch_id = gn_batch_id
              AND error_message IS NULL
         GROUP BY batch_id,
                  bom_transaction_type,
                  assembly_item_id,
                  organization_id,
                  bom_description,
                  structure_type_name,
                  structure_name;

      CURSOR c_transfer_comp (
         p_organization_id    IN   NUMBER,
         p_assembly_item_id   IN   NUMBER
      )
      IS
         SELECT operation_seq_num, component_item_id, component_quantity,
                component_yield_factor, planning_factor, start_effective_date,
                item_sequence_number, disable_date, organization_id,
                comp_transaction_type
           FROM xxpso_ego_bom_stg
          WHERE status_code = 'V'
            AND error_message IS NULL
            AND batch_id = gn_batch_id
            AND assembly_item_id = p_assembly_item_id
            AND organization_id = p_organization_id;

      CURSOR c_comp_disable (
         p_organization_id     IN   NUMBER,
         p_assembly_item_id    IN   NUMBER,
         p_component_item_id   IN   NUMBER
      )
      IS
         SELECT operation_seq_num, effectivity_date, component_item_id,
                disable_date, component_sequence_id
           FROM bom_bill_of_materials bbom, bom_inventory_components bic
          WHERE bbom.assembly_item_id = p_assembly_item_id
            AND bbom.organization_id = p_organization_id
            AND bic.component_item_id <> p_component_item_id
            AND bbom.bill_sequence_id = bic.bill_sequence_id
            AND bic.disable_date IS NULL;

      l_batch_size       CONSTANT NUMBER          := 5000;
      l_commit_records_num        NUMBER          := 0;
      l_batch_suffix              NUMBER          := 1;
      l_new_batch_id              NUMBER          := 100;
      l_err_msg_txt               VARCHAR2 (1000);
      l_status_code               VARCHAR2 (1);
      l_comp_seq_id               NUMBER;
      l_count                     NUMBER;
      l_comp_cnt                  NUMBER;
      l_stg_cnt                   NUMBER;
      l_bill_seq_id               NUMBER;
      l_component_seq_id          NUMBER;
      --l_effective_comp_date       DATE;
      --l_effective_comp_upd_date   DATE;
      l_ref_comp_id               NUMBER;
      l_old_ref                   VARCHAR2 (100);
      l_old_comp_qty              NUMBER;
      l_new_comp_qty              NUMBER;

      TYPE ref_desgs IS TABLE OF VARCHAR2 (100)
         INDEX BY PLS_INTEGER;

      l_ref_desgs                 ref_desgs;

      TYPE rec_old_ref_desg IS RECORD (
         component_sequence_id   NUMBER,
         old_desg                VARCHAR2 (100)
      );

      --  RECORD old_ref_Desg_rec  rec_old_ref_Desg;
      TYPE old_ref_desgs IS TABLE OF rec_old_ref_desg
         INDEX BY PLS_INTEGER;

      l_old_ref_desg_tbl          old_ref_desgs;
      */  
      
      CURSOR cur_bom_assembly_item
      IS
         SELECT   assembly_stg.*
             FROM xxpso_ego_bom_assembly_stg assembly_stg
                 , FND_LOOKUP_VALUES lookup --Changes for v2.0
            WHERE assembly_stg.batch_id = gn_batch_id
    	      AND assembly_stg.status_code = gv_valid_flag	
	      AND lookup.lookup_type = gn_survivorship_lkp_name 
	      AND lookup.enabled_flag = 'Y'
	      AND sysdate between NVL(lookup.start_date_active,sysdate) AND NVL(lookup.end_date_active,sysdate+1)
    	      AND assembly_stg.assembly_source_system = lookup.description	              
	      ORDER by lookup.lookup_code, assembly_stg.record_id;
              
      TYPE t_bulk_assembly_tab   	IS TABLE OF xxpso_ego_bom_assembly_stg%ROWTYPE;
      t_bulk_assembly_tab_rec	t_bulk_assembly_tab;
      
      CURSOR cur_bom_component_item(in_assembly_record_id IN NUMBER)
      IS
      SELECT component_stg.*
        FROM xxpso_ego_bom_component_stg component_stg
       WHERE component_stg.batch_id = gn_batch_id
    	 AND component_stg.status_code = gv_valid_flag	              
    	 AND component_stg.assembly_record_id = in_assembly_record_id
    	 AND component_stg.status_code = gv_valid_flag
    ORDER by component_stg.record_id;
    
      CURSOR cur_inactivate_component(in_assembly_record_id IN NUMBER,
						  in_assembly_item_id IN NUMBER,
      				      in_organization_id IN NUMBER
      				     )
      IS
	SELECT bic.*
	  FROM bom_bill_of_materials bbom,
	       bom_inventory_components bic
	 WHERE bbom.assembly_item_id  = in_assembly_item_id
	   AND bbom.organization_id   = in_organization_id
	   AND bbom.bill_sequence_id  = bic.bill_sequence_id
	   AND bic.disable_date IS NULL
	   AND NOT EXISTS (SELECT 1
	   		     FROM xxpso_ego_bom_component_stg component_stg,
	   		     	  xxpso_ego_bom_assembly_stg assembly_stg
	   		    WHERE assembly_stg.batch_id = gn_batch_id
	   		      AND assembly_stg.assembly_item_id = bbom.assembly_item_id
				  AND assembly_stg.record_id = in_assembly_record_id
	   		      AND assembly_stg.organization_id = bbom.organization_id
	   		      AND component_stg.assembly_record_id = assembly_stg.record_id
	   		      AND component_stg.batch_id = gn_batch_id
	   		      AND component_stg.component_item_id = bic.component_item_id
	   		   );
    
      ln_intfcr_batch_size	NUMBER;
      ln_intfcr_record_count	NUMBER:=0;
      ln_intfcr_batch_id	NUMBER;
      le_custom_exception	EXCEPTION;
      
      
      ln_assembly_records_rejected   NUMBER := 0;
      ln_assembly_records_inserted   NUMBER := 0;
      ln_component_records_rejected   NUMBER := 0;
      ln_component_records_inserted   NUMBER := 0;
      ln_infinite_valid_record_count  NUMBER := 0;
      ln_end_dated_record_count	      NUMBER := 0;
      ld_latest_valid_start_date	DATE;
      ln_latest_component_quantity	NUMBER := 0;
      ln_old_component_quantity	      NUMBER := 0;
      ln_component_sequence_id		NUMBER;
      lv_record_status      		VARCHAR2 (1);
      lv_err_msg	    	VARCHAR2(4000);
      lv_last_batch_src_system	VARCHAR2(20)  := 'NA';
	  pv_error_flag	VARCHAR2(10);
	  ln_count	       		NUMBER;
	  ln_comp_cnt			NUMBER;	  

      
   BEGIN
      xxpso_print_debug ('Transfer Proccedue Started gn_batch_size:'||gn_batch_size||
      			 ' gn_no_of_process:'||gn_no_of_process,gv_log);
       IF gn_batch_size >= gn_no_of_process THEN
         ln_intfcr_batch_size := ROUND(gn_batch_size/gn_no_of_process);
       ELSIF gn_batch_size < gn_no_of_process THEN
         ln_intfcr_batch_size := gn_batch_size;
       END IF;      
      
      OPEN cur_bom_assembly_item;
      LOOP
      FETCH cur_bom_assembly_item
      BULK COLLECT INTO t_bulk_assembly_tab_rec LIMIT 10000;
      EXIT WHEN t_bulk_assembly_tab_rec.count = 0;
      	
      	FOR index_count IN 1..t_bulk_assembly_tab_rec.count
	LOOP
	   lv_record_status := gv_valid_flag;
	   lv_err_msg	    := NULL;
	   BEGIN
			ln_count	       		:= 0;
            xxpso_print_debug ('ln_intfcr_record_count:'||ln_intfcr_record_count||' ln_intfcr_batch_size:'||ln_intfcr_batch_size||
            		       ' Source System:'||t_bulk_assembly_tab_rec(index_count).ASSEMBLY_SOURCE_SYSTEM||
						   ' lv_last_batch_src_system:'||lv_last_batch_src_system, gv_log);
						   
         --As part of surviourship logic import would be called after data is interfaced in interface table.
         -- When data from one source system is interfaced import program would be called to load data in base tables
         -- Then data from other source systems will be consumed.
         -- When the source system value changes we call import program to import the data.
			IF lv_last_batch_src_system <> 'NA' AND 
			lv_last_batch_src_system <> t_bulk_assembly_tab_rec(index_count).ASSEMBLY_SOURCE_SYSTEM
			THEN
			xxpso_print_debug ('Importing data from transfer procedure',gv_log);
			xxpso_import_data_p(pv_error_flag);
			xxpso_print_debug ('End of Importing data from transfer procedure',gv_log);
			END IF;
            IF MOD(ln_intfcr_record_count,ln_intfcr_batch_size) = 0 OR 
            lv_last_batch_src_system <> t_bulk_assembly_tab_rec(index_count).ASSEMBLY_SOURCE_SYSTEM
            THEN
            	ln_intfcr_batch_id := XXPSO_EGO_BOM_INTFCR_BATCH_S.NEXTVAL;
            	
            	--INTERFACE_BATCH_ID column in Assembly and Component table keeps track of the batch id assigned 
            	-- to records when inserting into the interface table.This INTERFACE_BATCH_ID column value will be used while updating 
            	-- results back to staging table. Only assembly_item_id and organization_id columns will not be useful to consolidate
            	-- as same item belonging to different source system if present in same batch will cause issues while consolidating.
            	-- Thus for different source system we assign different batch id and we use this INTERFACE_BATCH_ID to record unique record
            	-- in interface table for consolidating back.
            	-- After the source system value changes reset the record counter. ln_intfcr_record_count.
            	IF lv_last_batch_src_system <> t_bulk_assembly_tab_rec(index_count).ASSEMBLY_SOURCE_SYSTEM THEN
            		xxpso_print_debug ('Resetting the batch counter as source system has been changed:',gv_log);
            		lv_last_batch_src_system := t_bulk_assembly_tab_rec(index_count).ASSEMBLY_SOURCE_SYSTEM;
            		ln_intfcr_record_count := 0;
            	END IF;
            	xxpso_print_debug ('Generating new batch id:'||ln_intfcr_batch_id, gv_log);
            END IF;
            xxpso_print_debug ('Inserting Assembly record in interface table:'|| t_bulk_assembly_tab_rec(index_count).record_id,gv_log);
			
			BEGIN
			xxpso_print_debug ('Finding Assembly transaction type while transfering data: '||
							   ' assembly_item_id:'||t_bulk_assembly_tab_rec(index_count).assembly_item_id ||
							   ' organization_id:'||t_bulk_assembly_tab_rec(index_count).organization_id ||
							   ' alternate_bom_designator:'||t_bulk_assembly_tab_rec(index_count).alternate_bom_designator ,gv_log);
							   
				SELECT COUNT (1)
				   INTO ln_count
				   FROM bom_bill_of_materials
				  WHERE assembly_item_id = t_bulk_assembly_tab_rec(index_count).assembly_item_id
				AND organization_id =  t_bulk_assembly_tab_rec(index_count).organization_id
				AND NVL(alternate_bom_designator,'X') = NVL(t_bulk_assembly_tab_rec(index_count).alternate_bom_designator,'X');			
			
				IF ln_count > 0
				THEN
					t_bulk_assembly_tab_rec(index_count).bom_transaction_type := gv_update_trn;
				
				SELECT bill_sequence_id
				  INTO t_bulk_assembly_tab_rec(index_count).bill_sequence_id
				  FROM bom_bill_of_materials
					 WHERE assembly_item_id = t_bulk_assembly_tab_rec(index_count).assembly_item_id
				   AND organization_id =  t_bulk_assembly_tab_rec(index_count).organization_id
				   AND NVL(alternate_bom_designator,'X') = NVL(t_bulk_assembly_tab_rec(index_count).alternate_bom_designator,'X');
				
				 ELSE
					t_bulk_assembly_tab_rec(index_count).bom_transaction_type := gv_create_trn;
				 END IF;
				
				xxpso_print_debug ('Assembly transaction type: bom_transaction_type:'||t_bulk_assembly_tab_rec(index_count).bom_transaction_type||
								   ' bill_sequence_id:'||t_bulk_assembly_tab_rec(index_count).bill_sequence_id,gv_log);
			EXCEPTION
			WHEN OTHERS THEN
			xxpso_print_debug ('Error while fetching transaction type for Assembly item: Error Message'||SQLERRM,gv_log);
			END;
	    --First insert record in bom materials interface table.
	    INSERT INTO bom_bill_of_mtls_interface
			(assembly_item_id,
			 organization_id,
			 item_description,
			 specific_assembly_comment,
			 bill_sequence_id,
			 creation_date,created_by,last_update_date,last_updated_by,
			 assembly_type,
			 batch_id,
			 transaction_type,
			 structure_type_name,
			 process_flag,
			 alternate_bom_designator
			)
		 VALUES (t_bulk_assembly_tab_rec(index_count).assembly_item_id,
			 t_bulk_assembly_tab_rec(index_count).organization_id,
			 t_bulk_assembly_tab_rec(index_count).bom_description,
			 t_bulk_assembly_tab_rec(index_count).bom_transaction_type,
			 NVL(t_bulk_assembly_tab_rec(index_count).bill_sequence_id,NULL),
			 SYSDATE,fnd_global.user_id,SYSDATE,fnd_global.user_id,
			 1,       -- 1 for Manufacturing ,2 for  Engineering
			 ln_intfcr_batch_id,
			 t_bulk_assembly_tab_rec(index_count).bom_transaction_type,
			 t_bulk_assembly_tab_rec(index_count).structure_type_name, 
			 1,--Process Flag Value
			 DECODE (t_bulk_assembly_tab_rec(index_count).structure_name,'Primary', NULL,
			 t_bulk_assembly_tab_rec(index_count).structure_name)
		       );
                
                xxpso_print_debug ('Opening Component cursor for record_id:'|| t_bulk_assembly_tab_rec(index_count).record_id,gv_log);
                --Insert record for all components items in interface table
                FOR cur_bom_component_item_rec IN cur_bom_component_item(t_bulk_assembly_tab_rec(index_count).record_id)
                LOOP
                BEGIN
				ln_comp_cnt			:= 0;
				BEGIN

					xxpso_print_debug ('Finding Component transaction type while transfering data: '||
							   ' assembly_item_id:'||t_bulk_assembly_tab_rec(index_count).assembly_item_id ||
							   ' organization_id:'||t_bulk_assembly_tab_rec(index_count).organization_id ||
							   ' alternate_bom_designator:'||t_bulk_assembly_tab_rec(index_count).alternate_bom_designator ||
							   ' component_item_id:'||cur_bom_component_item_rec.component_item_id ||		
							   ' operation_seq_num:'||cur_bom_component_item_rec.operation_seq_num 
							   ,gv_log);
							   
				  SELECT COUNT (1)
					INTO ln_comp_cnt
					FROM bom_bill_of_materials bom,
					 bom_inventory_components bic
				   WHERE bom.assembly_item_id   = t_bulk_assembly_tab_rec(index_count).assembly_item_id
					 AND bom.organization_id    = t_bulk_assembly_tab_rec(index_count).organization_id
					 AND NVL(bom.alternate_bom_designator,'X') = NVL(t_bulk_assembly_tab_rec(index_count).alternate_bom_designator,'X')
					 AND bic.operation_seq_num  = cur_bom_component_item_rec.operation_seq_num
					 AND bic.component_item_id  = cur_bom_component_item_rec.component_item_id
					 AND bom.bill_sequence_id   = bic.bill_sequence_id;	

				  IF ln_comp_cnt > 0
				  THEN
					 cur_bom_component_item_rec.comp_transaction_type := gv_update_trn;	
				  ELSE
					 cur_bom_component_item_rec.comp_transaction_type := gv_create_trn;
				  END IF;
			
				EXCEPTION
				WHEN OTHERS THEN
				xxpso_print_debug ('Error while fetching transaction type for Component item: Error Message'||SQLERRM,gv_log);
				END;
                xxpso_print_debug ('Component Record_id:'||cur_bom_component_item_rec.record_id||
                		   ' component_item_id:'||cur_bom_component_item_rec.component_item_id||
                		   ' comp_transaction_type:'||cur_bom_component_item_rec.comp_transaction_type||
                		   ' operation_seq_num:'||cur_bom_component_item_rec.operation_seq_num
                		   ,gv_log);
                
                --With new logic.
                 IF cur_bom_component_item_rec.comp_transaction_type = gv_update_trn THEN
                  SELECT count(*)
                    INTO ln_infinite_valid_record_count
                    FROM bom_bill_of_materials bbom,
                         bom_inventory_components bic
                   WHERE bbom.assembly_item_id  = t_bulk_assembly_tab_rec(index_count).assembly_item_id
                     AND bbom.organization_id   = t_bulk_assembly_tab_rec(index_count).organization_id
                     AND bbom.bill_sequence_id  = bic.bill_sequence_id
                     AND bic.component_item_id  = cur_bom_component_item_rec.component_item_id
                     AND bic.operation_seq_num  = cur_bom_component_item_rec.operation_seq_num
                     AND bic.disable_date is NULL;                    
                  
                  xxpso_print_debug ('Infinite Valid record count:ln_infinite_valid_record_count:'||ln_infinite_valid_record_count,gv_log);
                  
                  IF ln_infinite_valid_record_count = 1 THEN 
			SELECT bic.effectivity_date
			      ,bic.component_sequence_id
			      ,bic.component_quantity
			  INTO ld_latest_valid_start_date
			       ,ln_component_sequence_id
			       ,ln_latest_component_quantity
			  FROM bom_bill_of_materials bbom,
			       bom_inventory_components bic
			 WHERE bbom.assembly_item_id  = t_bulk_assembly_tab_rec(index_count).assembly_item_id
			   AND bbom.organization_id   = t_bulk_assembly_tab_rec(index_count).organization_id
			   AND bbom.bill_sequence_id  = bic.bill_sequence_id
			   AND bic.component_item_id  = cur_bom_component_item_rec.component_item_id
			   AND bic.operation_seq_num  = cur_bom_component_item_rec.operation_seq_num
			   AND bic.disable_date is NULL; 
			   
			xxpso_print_debug (' Start Effectivity date for infinite valid record ' ||
					   ' ld_latest_valid_start_date:'||to_char(ld_latest_valid_start_date,'DD-MM-YYYY HH24:MI:SS')||
					   ' ln_latest_component_quantity:'||ln_latest_component_quantity||
					   ' ln_component_sequence_id:'||ln_component_sequence_id||
					   ' start_effectivity_date:'||to_char(cur_bom_component_item_rec.start_effective_date,'DD-MM-YYYY HH24:MI:SS')
					   ,gv_log);
			
			-- If the start date of current record in DB is same as start date of current record
			-- then we update existing record with data coming from current payload.
			IF ld_latest_valid_start_date = cur_bom_component_item_rec.start_effective_date THEN
			xxpso_print_debug ('New effective date equal to current effective date',gv_log);
			INSERT INTO bom_inventory_comps_interface
				  (operation_seq_num,
				   component_item_id,
				   component_sequence_id,
				   bill_sequence_id,
				   component_quantity,
				   component_yield_factor,
				   effectivity_date,
				   planning_factor,
				   assembly_item_id,
				   organization_id,
				   last_update_date,
				   last_updated_by,
				   creation_date,
				   created_by,
				   assembly_type,
				   batch_id,
				   transaction_type,
				   item_num,
				   disable_date,
				   process_flag
				  )
			   VALUES (cur_bom_component_item_rec.operation_seq_num,
				   cur_bom_component_item_rec.component_item_id,
				   ln_component_sequence_id,
				   t_bulk_assembly_tab_rec(index_count).bill_sequence_id,
				   cur_bom_component_item_rec.component_quantity,
				   cur_bom_component_item_rec.component_yield_factor,
				   cur_bom_component_item_rec.start_effective_date,--This would be same value..
				   cur_bom_component_item_rec.planning_factor,
				   t_bulk_assembly_tab_rec(index_count).assembly_item_id,
				   cur_bom_component_item_rec.organization_id,
				   SYSDATE, fnd_global.user_id, SYSDATE, fnd_global.user_id,
				   1,-- 1 for Manufacturing ,2 for  Engineering
				   ln_intfcr_batch_id,
				   gv_update_trn,
				   cur_bom_component_item_rec.item_sequence_number,
				   cur_bom_component_item_rec.disable_date,
				   1
	     			 );			
	     		--If start date of current record is greater than start date of current record in DB
	     		-- the disable current record with sysdate and create new record.
			ELSIF ld_latest_valid_start_date < cur_bom_component_item_rec.start_effective_date THEN
			xxpso_print_debug ('New effective date greater than current effective date',gv_log);
			--Entering current record as Update record.
			  INSERT INTO bom_inventory_comps_interface
				  (operation_seq_num,
				   component_item_id,
				   component_sequence_id,
				   bill_sequence_id,
				   assembly_item_id,
				   organization_id,
				   component_quantity,
				   last_update_date,
				   last_updated_by,
				   creation_date,
				   created_by,
				   batch_id,
				   transaction_type,
				   item_num,
				   disable_date,
				   process_flag
				  )
			   VALUES (cur_bom_component_item_rec.operation_seq_num,
				   cur_bom_component_item_rec.component_item_id,
				   ln_component_sequence_id,
				   t_bulk_assembly_tab_rec(index_count).bill_sequence_id,
				   t_bulk_assembly_tab_rec(index_count).assembly_item_id,
				   cur_bom_component_item_rec.organization_id,
				   ln_latest_component_quantity,
				   SYSDATE, fnd_global.user_id, SYSDATE, fnd_global.user_id,
				   ln_intfcr_batch_id,
				   gv_update_trn,
				   cur_bom_component_item_rec.item_sequence_number,
				   cur_bom_component_item_rec.start_effective_date - 2/86400,
				   -- We would be disabling the record with 2 sec less than the start date of the new record
				   1
	     			 );
	     			 
			-- Insert new record in Create mode for new data.	     			 
			INSERT INTO bom_inventory_comps_interface
				  (operation_seq_num,
				   component_item_id,
				   component_sequence_id,
				   bill_sequence_id,
				   component_quantity,
				   component_yield_factor,
				   effectivity_date,
				   planning_factor,
				   assembly_item_id,
				   organization_id,
				   last_update_date,
				   last_updated_by,
				   creation_date,
				   created_by,
				   assembly_type,
				   batch_id,
				   transaction_type,
				   item_num,
				   disable_date,
				   process_flag
				  )
			   VALUES (cur_bom_component_item_rec.operation_seq_num,
				   cur_bom_component_item_rec.component_item_id,
				   bom_inventory_components_s.NEXTVAL,
				   t_bulk_assembly_tab_rec(index_count).bill_sequence_id,
				   cur_bom_component_item_rec.component_quantity,
				   cur_bom_component_item_rec.component_yield_factor,
				   cur_bom_component_item_rec.start_effective_date,
				   cur_bom_component_item_rec.planning_factor,
				   t_bulk_assembly_tab_rec(index_count).assembly_item_id,
				   cur_bom_component_item_rec.organization_id,
				   SYSDATE, fnd_global.user_id, SYSDATE, fnd_global.user_id,
				   1,-- 1 for Manufacturing ,2 for  Engineering
				   ln_intfcr_batch_id,
				   gv_create_trn,
				   cur_bom_component_item_rec.item_sequence_number,
				   cur_bom_component_item_rec.disable_date,
				   1
	     			 );						
			ELSIF ld_latest_valid_start_date > cur_bom_component_item_rec.start_effective_date THEN
			
			xxpso_print_debug ('New effective date less than current effective date',gv_log);
			-- Insert new record in Create mode with new data.	     			 
                  -- In this case if the record fails to be posted in base table due to overlapping date condition
                  -- then the import program will fail. We will update the error message back in the base table.			
			INSERT INTO bom_inventory_comps_interface
				  (operation_seq_num,
				   component_item_id,
				   component_sequence_id,
				   bill_sequence_id,
				   component_quantity,
				   component_yield_factor,
				   effectivity_date,
				   planning_factor,
				   assembly_item_id,
				   organization_id,
				   last_update_date,
				   last_updated_by,
				   creation_date,
				   created_by,
				   assembly_type,
				   batch_id,
				   transaction_type,
				   item_num,
				   disable_date,
				   process_flag
				  )
			   VALUES (cur_bom_component_item_rec.operation_seq_num,
				   cur_bom_component_item_rec.component_item_id,
				   bom_inventory_components_s.NEXTVAL,
				   t_bulk_assembly_tab_rec(index_count).bill_sequence_id,
				   cur_bom_component_item_rec.component_quantity,
				   cur_bom_component_item_rec.component_yield_factor,
				   cur_bom_component_item_rec.start_effective_date,
				   cur_bom_component_item_rec.planning_factor,
				   t_bulk_assembly_tab_rec(index_count).assembly_item_id,
				   cur_bom_component_item_rec.organization_id,
				   SYSDATE, fnd_global.user_id, SYSDATE, fnd_global.user_id,
				   1,-- 1 for Manufacturing ,2 for  Engineering
				   ln_intfcr_batch_id,
				   gv_create_trn,
				   cur_bom_component_item_rec.item_sequence_number,
				   cur_bom_component_item_rec.disable_date,
				   1
	     			 );			
			END IF;
			   
			   
                  
                  ELSIF ln_infinite_valid_record_count = 0 THEN -- There is no record in DB which holds infinite validity. 
                  xxpso_print_debug ('Insert data as there is no infinite valid record',gv_log);
                  xxpso_print_debug ('Checking for existence of end dated record with same start date'||
                  		     ' assembly_item_id:'||t_bulk_assembly_tab_rec(index_count).assembly_item_id||
                  		     ' organization_id:'||t_bulk_assembly_tab_rec(index_count).organization_id||
                  		     ' start_effective_date:'||cur_bom_component_item_rec.start_effective_date
                  		     ,gv_log);
		  SELECT count(*)
		    INTO ln_end_dated_record_count
		    FROM bom_bill_of_materials bbom,
			 bom_inventory_components bic
		   WHERE bbom.assembly_item_id  = t_bulk_assembly_tab_rec(index_count).assembly_item_id
		     AND bbom.organization_id   = t_bulk_assembly_tab_rec(index_count).organization_id
		     AND bbom.bill_sequence_id  = bic.bill_sequence_id
		     AND bic.component_item_id  = cur_bom_component_item_rec.component_item_id
		     AND bic.operation_seq_num  = cur_bom_component_item_rec.operation_seq_num
		     AND bic.effectivity_date         = cur_bom_component_item_rec.start_effective_date;
                     
                     xxpso_print_debug (' ln_end_dated_record_count:'||ln_end_dated_record_count,gv_log);
                     IF ln_end_dated_record_count =  1 THEN
			SELECT bic.effectivity_date
			      ,bic.component_sequence_id
			      ,bic.component_quantity
			  INTO ld_latest_valid_start_date
			       ,ln_component_sequence_id
			       ,ln_latest_component_quantity
			  FROM bom_bill_of_materials bbom,
			       bom_inventory_components bic
			 WHERE bbom.assembly_item_id  = t_bulk_assembly_tab_rec(index_count).assembly_item_id
			   AND bbom.organization_id   = t_bulk_assembly_tab_rec(index_count).organization_id
			   AND bbom.bill_sequence_id  = bic.bill_sequence_id
			   AND bic.component_item_id  = cur_bom_component_item_rec.component_item_id
			   AND bic.operation_seq_num  = cur_bom_component_item_rec.operation_seq_num
			   AND bic.effectivity_date   = cur_bom_component_item_rec.start_effective_date;
			   
			xxpso_print_debug (' Start Effectivity date for end dated valid record ' ||
					   ' ld_latest_valid_start_date:'||to_char(ld_latest_valid_start_date,'DD-MM-YYYY HH24:MI:SS')||
					   ' ln_latest_component_quantity:'||ln_latest_component_quantity||
					   ' ln_component_sequence_id:'||ln_component_sequence_id||
					   ' start_effectivity_date:'||to_char(cur_bom_component_item_rec.start_effective_date,'DD-MM-YYYY HH24:MI:SS')
					   ,gv_log);
					   
			INSERT INTO bom_inventory_comps_interface
				  (operation_seq_num,
				   component_item_id,
				   component_sequence_id,
				   bill_sequence_id,
				   component_quantity,
				   component_yield_factor,
				   effectivity_date,
				   planning_factor,
				   assembly_item_id,
				   organization_id,
				   last_update_date,
				   last_updated_by,
				   creation_date,
				   created_by,
				   assembly_type,
				   batch_id,
				   transaction_type,
				   item_num,
				   disable_date,
				   process_flag
				  )
			   VALUES (cur_bom_component_item_rec.operation_seq_num,
				   cur_bom_component_item_rec.component_item_id,
				   ln_component_sequence_id,
				   t_bulk_assembly_tab_rec(index_count).bill_sequence_id,
				   cur_bom_component_item_rec.component_quantity,
				   cur_bom_component_item_rec.component_yield_factor,
				   cur_bom_component_item_rec.start_effective_date,--This would be same value..
				   cur_bom_component_item_rec.planning_factor,
				   t_bulk_assembly_tab_rec(index_count).assembly_item_id,
				   cur_bom_component_item_rec.organization_id,
				   SYSDATE, fnd_global.user_id, SYSDATE, fnd_global.user_id,
				   1,-- 1 for Manufacturing ,2 for  Engineering
				   ln_intfcr_batch_id,
				   gv_update_trn,
				   cur_bom_component_item_rec.item_sequence_number,
				   cur_bom_component_item_rec.disable_date,
				   1
	     			 );						   
                     ELSE
                  -- In this case if the record fails to be posted in base table due to overlapping date condition
                  -- then the import program will fail. We will update the error message back in the base table.
                    INSERT INTO bom_inventory_comps_interface
				  (operation_seq_num,
				   component_item_id,
				   component_sequence_id,
				   bill_sequence_id,
				   component_quantity,
				   component_yield_factor,
				   effectivity_date,
				   planning_factor,
				   assembly_item_id,
				   organization_id,
				   last_update_date,
				   last_updated_by,
				   creation_date,
				   created_by,
				   assembly_type,
				   batch_id,
				   transaction_type,
				   item_num,
				   disable_date,
				   process_flag
				  )
			   VALUES (cur_bom_component_item_rec.operation_seq_num,
				   cur_bom_component_item_rec.component_item_id,
				   bom_inventory_components_s.NEXTVAL,
				   t_bulk_assembly_tab_rec(index_count).bill_sequence_id,
				   cur_bom_component_item_rec.component_quantity,
				   cur_bom_component_item_rec.component_yield_factor,
				   NVL(cur_bom_component_item_rec.start_effective_date,SYSDATE),
				   cur_bom_component_item_rec.planning_factor,
				   t_bulk_assembly_tab_rec(index_count).assembly_item_id,
				   cur_bom_component_item_rec.organization_id,
				   SYSDATE, fnd_global.user_id, SYSDATE, fnd_global.user_id,
				   1,-- 1 for Manufacturing ,2 for  Engineering
				   ln_intfcr_batch_id,
				   gv_create_trn,
				   cur_bom_component_item_rec.item_sequence_number,
				   cur_bom_component_item_rec.disable_date,
				   1
	     			 );
	     	    END IF;--End of end_dated record check.
                  END IF;
                 ELSIF cur_bom_component_item_rec.comp_transaction_type = gv_create_trn THEN
			-- Insert new record in Create mode for new data.	     			 
			INSERT INTO bom_inventory_comps_interface
				  (operation_seq_num,
				   component_item_id,
				   component_sequence_id,
				   bill_sequence_id,
				   component_quantity,
				   component_yield_factor,
				   effectivity_date,
				   planning_factor,
				   assembly_item_id,
				   organization_id,
				   last_update_date,
				   last_updated_by,
				   creation_date,
				   created_by,
				   assembly_type,
				   batch_id,
				   transaction_type,
				   item_num,
				   disable_date,
				   process_flag
				  )
			   VALUES (cur_bom_component_item_rec.operation_seq_num,
				   cur_bom_component_item_rec.component_item_id,
				   bom_inventory_components_s.NEXTVAL,
				   NULL,--Bill sequence Id 
				   cur_bom_component_item_rec.component_quantity,
				   cur_bom_component_item_rec.component_yield_factor,
				   cur_bom_component_item_rec.start_effective_date,
				   cur_bom_component_item_rec.planning_factor,
				   t_bulk_assembly_tab_rec(index_count).assembly_item_id,
				   cur_bom_component_item_rec.organization_id,
				   SYSDATE, fnd_global.user_id, SYSDATE, fnd_global.user_id,
				   1,-- 1 for Manufacturing ,2 for  Engineering
				   ln_intfcr_batch_id,
				   gv_create_trn,
				   cur_bom_component_item_rec.item_sequence_number,
				   cur_bom_component_item_rec.disable_date,
				   1
	     			 );                 
                 	
                 END IF;--End of new logic
                 
                EXCEPTION
                WHEN OTHERS THEN
                lv_record_status := gv_invalid_flag;
                lv_err_msg	    := lv_err_msg||'~Unexpected error while transfering records in Component Interface table'||
                		       ' for component record_id:'||cur_bom_component_item_rec.record_id||
                		       ' Error Message:'||SQLERRM;
		xxpso_print_debug(lv_err_msg ,gv_log);
		CAPTURE_ERROR(   gc_component_item_ricew_id
				,gn_batch_id
				,cur_bom_component_item_rec.record_id
				,NULL
				,NULL
				,'RECORD_ID - BATCH_ID'
				,cur_bom_component_item_rec.record_id||'-'||gn_batch_id
				,'Unexpected error while transfering records in Interface table'||SQLERRM
				,'Contact Technical Team'
				,NULL
			   );			
		END;
                
                UPDATE xxpso_ego_bom_component_stg
				   SET comp_transaction_type = cur_bom_component_item_rec.comp_transaction_type
				  WHERE record_id = cur_bom_component_item_rec.record_id
				    AND batch_id = gn_batch_id;
				END LOOP;--End of Component insert into component interface table.
                
                xxpso_print_debug('Opening cursor for disabling component. '||
						  ' assembly_record_id:'||t_bulk_assembly_tab_rec(index_count).record_id||
                		  ' assembly_item_id:'||t_bulk_assembly_tab_rec(index_count).assembly_item_id||
                		  ' organization_id:'||t_bulk_assembly_tab_rec(index_count).organization_id,gv_log);
                
                --Disable components which is not present in current batch
                FOR cur_inactivate_component_rec IN 
                cur_inactivate_component(t_bulk_assembly_tab_rec(index_count).record_id,
							 t_bulk_assembly_tab_rec(index_count).assembly_item_id,
                			 t_bulk_assembly_tab_rec(index_count).organization_id)
                LOOP
                BEGIN
                  xxpso_print_debug('Component Id to disable:'||cur_inactivate_component_rec.component_item_id ||
                  		    ' Start Date:'||cur_inactivate_component_rec.effectivity_date,gv_log);
                        INSERT INTO bom_inventory_comps_interface
                                    (operation_seq_num,
                                     component_item_id,
                                     component_sequence_id,
                                     bill_sequence_id,
                                     assembly_item_id,
                                     organization_id,
                                     process_flag, last_update_date,
                                     last_updated_by,
                                     batch_id,
                                     transaction_type,
                                     disable_date
                                    )
                             VALUES (cur_inactivate_component_rec.operation_seq_num,
                                     cur_inactivate_component_rec.component_item_id,
                                     cur_inactivate_component_rec.component_sequence_id,
                                     cur_inactivate_component_rec.bill_sequence_id,
                                     t_bulk_assembly_tab_rec(index_count).assembly_item_id,
                                     t_bulk_assembly_tab_rec(index_count).organization_id,
                                     1, SYSDATE,
                                     fnd_global.user_id,
                                     ln_intfcr_batch_id,
                                     gv_update_trn,
                                     SYSDATE
                                    );
                                    
                                    
                EXCEPTION
                WHEN OTHERS THEN
	   	lv_record_status := gv_invalid_flag;
	   	lv_err_msg	    := lv_err_msg||'~Unexpected error while diabling records in Components Interface table'||
	   				' for component_item_id:'|| cur_inactivate_component_rec.component_item_id||
	   				' Error Message:'||SQLERRM;
		xxpso_print_debug(lv_err_msg ,gv_log);

		CAPTURE_ERROR(   gc_assembly_item_ricew_id
				,gn_batch_id
				,t_bulk_assembly_tab_rec(index_count).record_id
				,NULL
				,NULL
				,'COMPONENT_ITEM_ID'
				,cur_inactivate_component_rec.component_item_id
				,'Unexpected error while transfering records in Interface table'||SQLERRM
				,'Contact Technical Team'
				,NULL
			   );	
		END;
                
                END LOOP;
				
				
	   
	   EXCEPTION
	   WHEN OTHERS THEN
	   	lv_record_status := gv_invalid_flag;
	   	lv_err_msg	    := lv_err_msg||'~Unexpected error while transfering records in Assembly Interface table'||
	   				' fore assembly record_id:'|| t_bulk_assembly_tab_rec(index_count).record_id||
	   				' Error Message:'||SQLERRM;
		xxpso_print_debug(lv_err_msg ,gv_log);

		CAPTURE_ERROR(   gc_assembly_item_ricew_id
				,gn_batch_id
				,t_bulk_assembly_tab_rec(index_count).record_id
				,NULL
				,NULL
				,'RECORD_ID - BATCH_ID'
				,t_bulk_assembly_tab_rec(index_count).record_id||'-'||gn_batch_id
				,'Unexpected error while transfering records in Interface table'||SQLERRM
				,'Contact Technical Team'
				,NULL
			   );			   
	   END;
	   
	   xxpso_print_debug('Assembly and Component Transfer Status:lv_record_status:'||lv_record_status||
	   		     ' Record Id:'||t_bulk_assembly_tab_rec(index_count).record_id,gv_log);
	   --If any of the records from Assembly or component has failed while inserting into
	   --interface table then rollback entire insert operation else commit it.
	   IF lv_record_status = gv_valid_flag THEN
	   	COMMIT;
	   	ln_intfcr_record_count := ln_intfcr_record_count + 1;
	   ELSIF lv_record_status = gv_invalid_flag THEN
	   	ROLLBACK;
	   END IF;
	   
	   UPDATE xxpso_ego_bom_assembly_stg
	      SET status_code = DECODE(lv_record_status,gv_invalid_flag,gv_failed_flag,gv_valid_flag,gv_staged_flag)
	      	  ,error_message = DECODE(lv_record_status,gv_invalid_flag,lv_err_msg,gv_valid_flag,NULL)
	      	  ,last_update_date = SYSDATE
		  ,last_updated_by = gn_user_id
		  ,last_update_login = gn_login_id
	          ,conc_request_id = gn_request_id
	          ,interface_batch_id = ln_intfcr_batch_id
			  ,bom_transaction_type = t_bulk_assembly_tab_rec(index_count).bom_transaction_type
	    WHERE record_id = t_bulk_assembly_tab_rec(index_count).record_id
	      AND batch_id = gn_batch_id;
	      
	   UPDATE xxpso_ego_bom_component_stg
	      SET status_code = DECODE(lv_record_status,gv_invalid_flag,gv_failed_flag,gv_valid_flag,gv_staged_flag)
	      	  ,error_message = DECODE(lv_record_status,gv_invalid_flag,lv_err_msg,gv_valid_flag,NULL)
	      	  ,last_update_date = SYSDATE
		  ,last_updated_by = gn_user_id
		  ,last_update_login = gn_login_id
	          ,conc_request_id = gn_request_id
	          ,interface_batch_id = ln_intfcr_batch_id
	    WHERE assembly_record_id IN ( SELECT record_id 
	    				    FROM xxpso_ego_bom_assembly_stg
	    				   WHERE record_id = t_bulk_assembly_tab_rec(index_count).record_id
	    				     AND batch_id = gn_batch_id)
	      AND batch_id = gn_batch_id;
			
	END LOOP;--Assembly loop End
      END LOOP;--Open Cursor loop end
      CLOSE cur_bom_assembly_item;
      

	/*
      -- CURSOR FOR INSERTING INTO ASSEMBLY INTERFACE BEGINS
      FOR c_transfer_assembly_rec IN c_transfer_assembly
      LOOP
/*          l_new_batch_id :=
              c_transfer_assembly_rec.batch_id || LPAD (l_batch_suffix, 4, 0);

         IF (    MOD (l_commit_records_num, l_batch_size) = 0
             AND l_commit_records_num > 0
            )
         THEN
            l_batch_suffix := l_batch_suffix + 1;
            l_new_batch_id :=
               c_transfer_assembly_rec.batch_id
               || LPAD (l_batch_suffix, 4, 0);
         END IF; *
	xxpso_print_debug ('Transfering Assembly item: Assembly Item Id:'||c_transfer_assembly_rec.assembly_item_id||
			   ' Organization Id:'||c_transfer_assembly_rec.organization_id||
			   ' Transaction Type:'||c_transfer_assembly_rec.bom_transaction_type,gv_log);
			   
         IF ( MOD (l_commit_records_num, l_batch_size) = 0  AND l_commit_records_num > 0 )
         THEN
            l_new_batch_id  := l_new_batch_id + 100;
         END IF;

         l_commit_records_num   := l_commit_records_num + 1;
         l_bill_seq_id          := NULL;
         l_component_seq_id     := NULL;
         l_err_msg_txt          := NULL;
         l_status_code          :=NULL;
         --l_effective_comp_date := SYSDATE;

         IF c_transfer_assembly_rec.bom_transaction_type = 'UPDATE'
         THEN
            BEGIN
               SELECT bill_sequence_id
                 INTO l_bill_seq_id
                 FROM bom_bill_of_materials
                WHERE assembly_item_id  = c_transfer_assembly_rec.assembly_item_id
                  AND organization_id   = c_transfer_assembly_rec.organization_id;
            EXCEPTION
               WHEN OTHERS
               THEN

                  l_err_msg_txt :=
                        l_err_msg_txt
                     || ' Unexpected Error while fetching Bill sequence id. SQLERRM: '
                     || SQLERRM;
                  l_bill_seq_id := NULL;
            END;
         END IF;
	xxpso_print_debug ('Transfering Assembly Item: bill_sequence_id:'||l_bill_seq_id,gv_log);
         BEGIN
            INSERT INTO bom_bill_of_mtls_interface
                        (assembly_item_id,
                         organization_id,
                         item_description,
                         specific_assembly_comment,
                         bill_sequence_id,
                         creation_date,
                         created_by,
                         last_update_date,
                         last_updated_by,
                         assembly_type,
                         batch_id,
                         transaction_type,
                         structure_type_name,
                         process_flag,
                         alternate_bom_designator
                        )
                 VALUES (c_transfer_assembly_rec.assembly_item_id,
                         c_transfer_assembly_rec.organization_id,
                         c_transfer_assembly_rec.bom_description,
                         c_transfer_assembly_rec.bom_transaction_type,
                         l_bill_seq_id,
                         SYSDATE,
                         fnd_global.user_id,
                         SYSDATE,
                         fnd_global.user_id,
                         1,       -- 1 for Manufacturing ,2 for  Engineering
                         l_new_batch_id,
                         c_transfer_assembly_rec.bom_transaction_type,
                         c_transfer_assembly_rec.structure_type_name, 1,
                         DECODE (c_transfer_assembly_rec.structure_name,
                                 'Primary', NULL,
                                 c_transfer_assembly_rec.structure_name
                                )
                        );
         EXCEPTION
            WHEN OTHERS
            THEN

               l_err_msg_txt :=
                     l_err_msg_txt
                  || ' Unexpected Error while insert into bom_bill_of_mtls_interface:'
                  || SQLERRM;
               capture_error
                  ('BOM-CNV-001',
                   gn_batch_id,
                   NULL,
                   NULL,
                   NULL,
                   'ASSEMBLY_ITEM_ID',
                   c_transfer_assembly_rec.assembly_item_id,
                   'Unexpected Error while insert into bom_bill_of_mtls_interface',
                   'Check Source Data',
                   NULL
                  );
         END;
	xxpso_print_debug ('Transfering Component Item',gv_log);
         -- CURSOR FOR INSERTING INTO COMPONENT INTERFACE BEGINS
         FOR c_transfer_comp_rec IN
            c_transfer_comp (c_transfer_assembly_rec.organization_id,
                             c_transfer_assembly_rec.assembly_item_id
                            )
         LOOP
            l_comp_seq_id := bom_inventory_components_s.NEXTVAL;
            l_bill_seq_id := NULL;
            l_component_seq_id := NULL;
            l_comp_cnt := 0;
            l_err_msg_txt := NULL;
            l_old_comp_qty := NULL;
            l_new_comp_qty := c_transfer_comp_rec.component_quantity;
	
	    xxpso_print_debug ('Transfering Component Item.Component Item id: '||c_transfer_comp_rec.component_item_id||
	    			' Assembly Item Id:'||c_transfer_assembly_rec.assembly_item_id||
	    			   ' Organization Id:'||c_transfer_assembly_rec.organization_id||
			   ' Assembly Transaction Type:'||c_transfer_assembly_rec.bom_transaction_type||
			   ' Component Transaction Type:'||c_transfer_comp_rec.comp_transaction_type,gv_log);

            IF c_transfer_comp_rec.comp_transaction_type = 'UPDATE'
            THEN
               BEGIN
                  SELECT bbom.bill_sequence_id, bic.component_sequence_id, bic.component_quantity
                    INTO l_bill_seq_id, l_component_seq_id, l_old_comp_qty
                    FROM bom_bill_of_materials bbom,
                         bom_inventory_components bic
                   WHERE bbom.assembly_item_id  = c_transfer_assembly_rec.assembly_item_id
                     AND bbom.organization_id   = c_transfer_assembly_rec.organization_id
                     AND bbom.bill_sequence_id  = bic.bill_sequence_id
                     AND bic.component_item_id  = NVL (c_transfer_comp_rec.component_item_id,-1)
                     AND bic.disable_date       IS NULL;
               EXCEPTION
                  WHEN OTHERS
                  THEN

                     l_err_msg_txt :=
                           l_err_msg_txt
                        || ' Unexpected Error while fetching Bill sequence id and Component Seq id. SQLERRM: '
                        || SQLERRM;
                     l_bill_seq_id := NULL;
               END;
            END IF;
	    xxpso_print_debug ('Transfering Component Item l_old_comp_qty:'||l_old_comp_qty||' l_new_comp_qty:'||l_new_comp_qty,gv_log);
            --l_effective_comp_upd_date := SYSDATE + 40 / 86400;

            IF (l_old_comp_qty <> l_new_comp_qty)
            THEN
               BEGIN
                  --Inserting into the BOM Comp table as a DISABLED RECORD
                  INSERT INTO bom_inventory_comps_interface
                              (operation_seq_num,
                               component_item_id,
                               component_sequence_id,
                               bill_sequence_id,
                               component_quantity,
                               --effectivity_date,
                               planning_factor,
                               assembly_item_id,
                               organization_id,
                               last_update_date,
                               last_updated_by,
                               creation_date,
                               created_by,
                               assembly_type,
                               batch_id,
                               transaction_type,
                               item_num,
                               disable_date,
                               process_flag
                              )
                       VALUES (c_transfer_comp_rec.operation_seq_num,
                               c_transfer_comp_rec.component_item_id,
                               DECODE
                                   (c_transfer_comp_rec.comp_transaction_type,
                                    'UPDATE', l_component_seq_id,
                                    l_comp_seq_id
                                   ),
                               DECODE
                                   (c_transfer_comp_rec.comp_transaction_type,
                                    'UPDATE', l_bill_seq_id,
                                    NULL
                                   ),
                               l_old_comp_qty,
                               --NVL(c_transfer_comp_rec.start_effective_date,SYSDATE),
                               c_transfer_comp_rec.planning_factor,
                               c_transfer_assembly_rec.assembly_item_id,
                               c_transfer_comp_rec.organization_id,
                               SYSDATE,
                               fnd_global.user_id,
                               SYSDATE,
                               fnd_global.user_id,
                               1,
                               l_new_batch_id,
                               c_transfer_comp_rec.comp_transaction_type,
                               c_transfer_comp_rec.item_sequence_number,
                               --SYSDATE,
                               NVL(c_transfer_comp_rec.start_effective_date,SYSDATE),
                               1
                              );
               END;

               --INserting the upadte Record with New Effective date as SYSDATE+40 Seconds
               BEGIN
                  INSERT INTO bom_inventory_comps_interface
                              (operation_seq_num,
                               component_item_id,
                               component_sequence_id,
                               bill_sequence_id,
                               component_quantity,
                               effectivity_date,
                               planning_factor,
                               assembly_item_id,
                               organization_id,
                               last_update_date,
                               last_updated_by,
                               creation_date,
                               created_by,
                               assembly_type,
                               batch_id,
                               transaction_type,
                               item_num,
                               disable_date,
                               process_flag
                              )
                       VALUES (c_transfer_comp_rec.operation_seq_num,
                               c_transfer_comp_rec.component_item_id,
                               l_comp_seq_id,
                               NULL,
                               c_transfer_comp_rec.component_quantity,
                               --l_effective_comp_upd_date,
                               NVL(c_transfer_comp_rec.start_effective_date,SYSDATE) + 40 / 86400,
                               c_transfer_comp_rec.planning_factor,
                               c_transfer_assembly_rec.assembly_item_id,
                               c_transfer_comp_rec.organization_id,
                               SYSDATE,
                               fnd_global.user_id,
                               SYSDATE,
                               fnd_global.user_id,
                               1,
                               l_new_batch_id,
                               'CREATE',
                               c_transfer_comp_rec.item_sequence_number,
                               c_transfer_comp_rec.disable_date,
                               1
                              );
               END;
            ELSE
               BEGIN
                  INSERT INTO bom_inventory_comps_interface
                              (operation_seq_num,
                               component_item_id,
                               component_sequence_id,
                               bill_sequence_id,
                               component_quantity,
                               component_yield_factor,
                               effectivity_date,
                               planning_factor,
                               assembly_item_id,
                               organization_id,
                               last_update_date,
                               last_updated_by,
                               creation_date,
                               created_by,
                               assembly_type,
                               batch_id,
                               transaction_type,
                               item_num,
                               disable_date,
                               process_flag
                              )
                       VALUES (c_transfer_comp_rec.operation_seq_num,
                               c_transfer_comp_rec.component_item_id,
                               DECODE
                                   (c_transfer_comp_rec.comp_transaction_type,
                                    'UPDATE', l_component_seq_id,
                                    l_comp_seq_id
                                   ),
                               DECODE
                                   (c_transfer_comp_rec.comp_transaction_type,
                                    'UPDATE', l_bill_seq_id,
                                    NULL
                                   ),
                               c_transfer_comp_rec.component_quantity,
                               c_transfer_comp_rec.component_yield_factor,
                               NVL(c_transfer_comp_rec.start_effective_date,SYSDATE),
                               c_transfer_comp_rec.planning_factor,
                               c_transfer_assembly_rec.assembly_item_id,
                               c_transfer_comp_rec.organization_id,
                               SYSDATE,
                               fnd_global.user_id,
                               SYSDATE,
                               fnd_global.user_id,
                               1,
                               l_new_batch_id,
                               c_transfer_comp_rec.comp_transaction_type,
                               c_transfer_comp_rec.item_sequence_number,
                               c_transfer_comp_rec.disable_date,
                               1
                              );

                  COMMIT;
               EXCEPTION
                  WHEN OTHERS
                  THEN
                  l_status_code:='E';
                     l_err_msg_txt :=
                           l_err_msg_txt
                        || ' Unexpected Error while insert into bom_inventory_comps_interface:'
                        || SQLERRM;
                     capture_error
                        ('BOM-CNV-001',
                         gn_batch_id,
                         NULL,
                         NULL,
                         NULL,
                         'COMPONENT_ITEM_ID',
                         c_transfer_comp_rec.component_item_id,
                         'Unexpected Error while insert into bom_inventory_comps_interface',
                         'Check Source Data',
                         NULL
                        );
               END;
            END IF;

            IF c_transfer_assembly_rec.bom_transaction_type = 'UPDATE'
            THEN
	    xxpso_print_debug ('Disabling Component Org Id:'||c_transfer_assembly_rec.organization_id||
	    		       ' assembly_item_id:'||c_transfer_assembly_rec.assembly_item_id||
	    		       ' component_item_id:'||c_transfer_comp_rec.component_item_id,gv_log);
            
               FOR c_comp_disable_rec IN
                  c_comp_disable (c_transfer_assembly_rec.organization_id,
                                  c_transfer_assembly_rec.assembly_item_id,
                                  c_transfer_comp_rec.component_item_id
                                 )
               LOOP
                  l_comp_cnt := 0;
                  l_stg_cnt := 0;
		  xxpso_print_debug ('Disabling Component component_item_id:'||c_comp_disable_rec.component_item_id,gv_log);
                  SELECT COUNT (1)
                    INTO l_comp_cnt
                    FROM bom_inventory_comps_interface brd
                   WHERE brd.assembly_item_id =
                                      c_transfer_assembly_rec.assembly_item_id
                     AND brd.organization_id =
                                       c_transfer_assembly_rec.organization_id
                     AND brd.component_item_id =
                                          c_comp_disable_rec.component_item_id
                     AND process_flag = 1;

                  SELECT COUNT (1)
                    INTO l_stg_cnt
                    FROM xxpso_ego_bom_stg
                   WHERE assembly_item_id =
                                      c_transfer_assembly_rec.assembly_item_id
                     AND organization_id =
                                       c_transfer_assembly_rec.organization_id
                     AND component_item_id =
                                          c_comp_disable_rec.component_item_id
                     AND status_code = 'V';
		   xxpso_print_debug ('Disabling Component l_comp_cnt:'||l_comp_cnt||' l_stg_cnt:'||l_stg_cnt,gv_log);
                  IF l_comp_cnt = 0 AND l_stg_cnt = 0
                  THEN
                     BEGIN
                     xxpso_print_debug ('Disabling Component Inserting for '||c_comp_disable_rec.component_item_id,gv_log);
                        INSERT INTO bom_inventory_comps_interface
                                    (operation_seq_num,
                                     component_item_id,
                                     component_sequence_id,
                                     assembly_item_id,
                                     organization_id,
                                     process_flag, last_update_date,
                                     last_updated_by,
                                     batch_id,
                                     transaction_type,
                                     disable_date
                                    )
                             VALUES (c_comp_disable_rec.operation_seq_num,
                                     c_comp_disable_rec.component_item_id,
                                     c_comp_disable_rec.component_sequence_id,
                                     c_transfer_assembly_rec.assembly_item_id,
                                     c_transfer_assembly_rec.organization_id,
                                     1, SYSDATE,
                                     fnd_global.user_id,
                                     l_new_batch_id,
                                     c_transfer_assembly_rec.bom_transaction_type,
                                     NVL(c_comp_disable_rec.disable_date,SYSDATE)
                                    );

                        COMMIT;
                     EXCEPTION
                        WHEN OTHERS
                        THEN
                           l_err_msg_txt :=
                                 l_err_msg_txt
                              || ' Unexpected Error while insert into updating interface table:'
                              || SQLERRM;
                     END;
                  END IF;
               END LOOP;
            END IF;

            IF l_err_msg_txt IS NOT NULL
            THEN
               UPDATE xxpso_ego_bom_stg
                  SET status_code       = 'F',
                      error_message     = l_err_msg_txt
                WHERE status_code       = 'V'
                  AND assembly_item_id  = c_transfer_assembly_rec.assembly_item_id
                  AND component_item_id = c_transfer_comp_rec.component_item_id
                  AND organization_id   = c_transfer_assembly_rec.organization_id;
            ELSE
               UPDATE xxpso_ego_bom_stg
                  SET status_code       = 'I'
                WHERE status_code       = 'V'
                  AND assembly_item_id  = c_transfer_assembly_rec.assembly_item_id
                  AND component_item_id = c_transfer_comp_rec.component_item_id
                  AND organization_id   = c_transfer_assembly_rec.organization_id;
            END IF;

         END LOOP;       -- CURSOR FOR INSERTING INTO COMPONENT INTERFACE ENDS

         l_count := l_count + 1;

/*       UPDATE xxpso_ego_bom_stg
            SET status_code         = 'I'
          WHERE status_code         = 'V'
            AND assembly_item_id    = c_transfer_assembly_rec.assembly_item_id
            AND organization_id     = c_transfer_assembly_rec.organization_id; *
      END LOOP;                                    -- end of BOM assembly loop
	*/
      COMMIT;
      xxpso_print_debug ('Transfer Proccedue Completed',gv_log);      
   EXCEPTION
      WHEN OTHERS
      THEN
        xxpso_print_debug(' Error in xxpso_transfer_data_p'
            || SQLERRM ,gv_output);
		
	CAPTURE_ERROR(   gc_assembly_item_ricew_id
			,gn_batch_id
			,NULL
			,NULL
			,NULL
			,NULL
			,NULL
			,'Unexpected error in xxpso_transfer_data_p procedure .Error Code:'||SQLCODE||
							 ' Error Message:'||SQLERRM
			,'Contact Technical Team'
			,NULL
		   );
   END xxpso_transfer_data_p;   


--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
-- Name       : XXPSO_BOM_CONV_MAIN_P
-- Description: This procedure calls the validate and import procedures.
--| Modification History
--+============================================================|
--| Date                 Who             Description
--| ----------- --------------------  -------------------------
--| 10-Mar-2016  Monica Yekula           Initial Creation
--------------------------------------------------------------------------------
   PROCEDURE xxpso_bom_conv_main_p (
      p_errbuf                 OUT      VARCHAR2
      ,p_retcode                OUT      NUMBER
      --Changes for v2.0 begin
     ,p_mode   		     IN     VARCHAR2
     ,p_follow_survivorship     IN     VARCHAR2 -- Changes for v2.0
     ,p_dummy               IN             VARCHAR2     
     ,p_source_system	     IN     VARCHAR2
     ,p_batch_size		     IN     NUMBER
     ,p_no_of_threads	     IN     NUMBER
     ,p_purge_if_tables     IN             VARCHAR2
     --,p_archive_stg_tables  IN             VARCHAR2 --Commented as part of v2.0
     ,p_debug_flag          IN             VARCHAR2
      --Changes for v2.0 End.
   )
   IS
      l_retcode         VARCHAR2 (1)    := 'S';
      l_return_status   VARCHAR2 (200)  := NULL;
      l_ret_arch_code   VARCHAR2 (1000) := NULL;
      l_mode_desc       VARCHAR2 (50)   := NULL;
      
   pv_ret_code 		NUMBER;
   pv_error_flag	VARCHAR2(10);	
   pv_arcv_err		VARCHAR2(1000);
   
   --Changes for v2.0
   pv_status1		VARCHAR2(2) := NULL;
   pv_status2		VARCHAR2(2) := NULL;
   pv_status3		VARCHAR2(2) := NULL;
   
      ln_processed_count 	NUMBER;
      ln_failed_count	 	NUMBER;	     
   
   BEGIN
   	
   	/* Commented as part of v2.0 Changes */
        --gc_process_flag     := p_mode;
        --gc_record_status    := NVL(p_record_status,'All');
        --gc_debug_flag       := NVL(p_debug_flag,'N');
        /* Changes for v 2.0 Begin */
	xxpso_print_debug('******************************************************************',gv_output);
	xxpso_print_debug('*****************INPUT PARAMTERS**************************',gv_output);
	gc_debug_flag       := NVL(p_debug_flag,'N');	
        gn_batch_size			:=  NVL(p_batch_size,100000000000);
        gn_no_of_process  		:= NVL(p_no_of_threads,1);
        gn_source_system 		:= p_source_system;
        gn_follow_survivorship		:= p_follow_survivorship;
        gc_mode				:= p_mode;
        
	xxpso_print_debug('p_mode :'||gc_mode,gv_output);
	xxpso_print_debug('p_mode :'||gc_mode,gv_log);
	xxpso_print_debug('p_debug_flag :'||p_debug_flag,gv_output);
	xxpso_print_debug('p_debug_flag :'||p_debug_flag,gv_log);
	xxpso_print_debug('p_purge_if_tables :'||p_purge_if_tables,gv_output);
	xxpso_print_debug('p_purge_if_tables :'||p_purge_if_tables,gv_log);
	xxpso_print_debug('p_batch_size :'||p_batch_size,gv_output);
	xxpso_print_debug('p_batch_size :'||p_batch_size,gv_log);
	xxpso_print_debug('gn_no_of_process :'||gn_no_of_process,gv_output);
	xxpso_print_debug('gn_no_of_process :'||gn_no_of_process,gv_log);
	xxpso_print_debug('gn_source_system :'||gn_source_system,gv_output);
	xxpso_print_debug('gn_source_system :'||gn_source_system,gv_log);	
	xxpso_print_debug('gn_follow_survivorship :'||gn_follow_survivorship,gv_output);
	xxpso_print_debug('gn_follow_survivorship :'||gn_follow_survivorship,gv_log);	
        
        /* Changes for v 2.0 End */

      -- Added on 30-APR-2016
      
      --Changes for v2.0
      -- Deduplication would be handled in query.
      /*
      IF gc_enable_de_duplication_flag = 'Y'
      THEN
         --calling procedure to perform item de-duplication
         perform_de_duplication;
      END IF;
	*/
      --Commented as part of 2.1 changes. New concurrent program has been created for archival.
      --IF p_archive_stg_tables = 'Y'
      --THEN
      --   identify_archive_records; 
         /*
         xxpso_ego_process_items_pkg.archive_stg_table
                                                   ('xxpso_ego_bom_stg',
                                                    'xxpso_ego_bom_stg_ARCH',
                                                    l_ret_arch_code
                                                   );
           */
      --END IF;

      IF p_purge_if_tables IS NOT NULL
      THEN
         --calling procedure to purge Interface Tables records
         xxpso_print_debug ('Purging the Interface tables',gv_log);
         purge_if_tables (p_purge_if_tables);
      END IF;
      
      --Changes for v2.0
      transfer_data_to_split_tables; 
      
      --Changes for v 2.0
      -- In Process mode we will not be validating the records.
      -- In Process mode we will be importing only valid records.
      -- Added 2 new modes R-Reprocess which will validate failed and errored records.
      --		   VP-Validate and Process.
      IF p_mode IN ( gv_validate_mode,gv_validate_process_mode,gv_revalidate_mode)--,gv_import_mode)
      THEN
            xxpso_print_debug ('******************************************************************',gv_output);
            xxpso_print_debug ('********************** xxpso_val_data_p*******************',gv_output);
            xxpso_print_debug ('******************************************************************',gv_output);
            /******************************************
                Calling validation procedure
            ******************************************/
            pv_error_flag := gv_valid_flag;
            
            --Changes for v2.0
            IF p_mode IN ( gv_validate_mode,gv_validate_process_mode) THEN
            	pv_status1 := gv_new_flag;
            ELSIF p_mode = gv_revalidate_mode THEN
            	pv_status1 := gv_failed_flag;
            	pv_status2 := gv_error_flag;
            END IF;
      
         l_mode_desc := 'VALIDATE';
         /*xxpso_print_debug (   'Started BOM Conversion in VALIDATE mode at '
                            || TO_CHAR (g_sysdate, 'DD-MON-YYYY HH24:MI:SS')
                           ,gv_log);*/
	 IF p_mode = gv_validate_process_mode THEN  
	 	gc_mode := gv_validate_mode;
	 END IF;
         xxpso_assign_batch_p(gc_mode);
         xxpso_val_data_p(pv_error_flag,pv_status1,pv_status2,pv_status3);
	 IF pv_error_flag = gv_valid_flag THEN
		pv_ret_code := 0;
	 ELSIF pv_error_flag = gv_invalid_flag THEN
		pv_ret_code := 1;
	 END IF;         
      END IF;

      IF p_mode IN (gv_import_mode,gv_validate_process_mode)
      THEN
	    xxpso_print_debug ('******************************************************************',gv_output);
	    xxpso_print_debug ('********************** xxpso_import_data_p*******************',gv_output);
	    xxpso_print_debug ('******************************************************************',gv_output);
	    /*****************************************
		Calling Data Upload  procedure
	   ******************************************/      
         l_mode_desc := 'IMPORT';
         /*xxpso_print_debug
                          (   'Started BOM Conversion at in PROCESS mode at '
                           || TO_CHAR (g_sysdate, 'DD-MON-YYYY HH24:MI:SS')
                          ,gv_log);
         xxpso_print_debug
            ('+---------------------------------------------------------------------------+',gv_log
            );*/
	 IF p_mode = gv_validate_process_mode THEN  
	 	gc_mode := gv_import_mode;
	 END IF;            
         xxpso_assign_batch_p(gc_mode);
         --xxpso_val_data_p;	--Commented as part of v2.0
         xxpso_transfer_data_p(pv_error_flag);
         --As part of surviourship logic import would be called after data is interfaced in interface table.
         -- When data from one source system is interfaced import program would be called to load data in base tables
         -- Then data from other source systems will be consumed.
         -- Thus xxpso_import_data_p is also called from xxpso_transfer_data_p. But when there is only one source system 
         -- or to import data from last pending source system we call xxpso_transfer_data_p again.
         xxpso_import_data_p(pv_error_flag);
         
         
         --Changes for v2.1 Begin
         xxpso_send_acknowledgement_p(pv_error_flag);
         
      SELECT COUNT(*)
        INTO ln_processed_count
        FROM xxpso_ego_bom_assembly_stg
       WHERE status_code = gv_processed_flag
        AND batch_id = gn_batch_id;
        
      SELECT COUNT(*)
        INTO ln_failed_count
        FROM xxpso_ego_bom_assembly_stg
       WHERE status_code = gv_failed_flag
        AND batch_id = gn_batch_id;
        
      xxpso_print_debug('Counts: ln_processed_count:'||ln_processed_count||' ln_failed_count:'||ln_failed_count,gv_log);  
         /*Printing Details into Output After Inserting into Base Table*/
      xxpso_print_debug (' Generating output file', gv_output);
      /*Printing Details into Output After Validating from the Staging Table*/
      xxpso_print_debug ('+--------------------------------------------------------------------------------+', gv_output);
      xxpso_print_debug ('Concurrent Request ID : '
         || gn_request_id
         || '            '
         || '    Begin Date : '
         || TO_CHAR (SYSDATE, 'DD-MON-RRRR HH:MI:SS'), gv_output);
     xxpso_print_debug ('+---------------------------------------------------------------------------------+', gv_output);
     xxpso_print_debug ('+---------------------------------------------------------------------------------+', gv_output);
     xxpso_print_debug ('Uploaded Records Summary', gv_output);
     xxpso_print_debug ('+---------------------------------------------------------------------------------+', gv_output);
     xxpso_print_debug ('--------------------------------UPLOAD DETAILS-------------------------------', gv_output);
     xxpso_print_debug( '*********************************************************************************************************', gv_output);
     xxpso_print_debug ( 'Total number of records processed in R12                             :   '
         || (ln_processed_count+ln_failed_count), gv_output);
     xxpso_print_debug ('Total number of records uploaded in base tables                       :   '
         || ln_processed_count , gv_output);
     xxpso_print_debug ('Total number  of records failed in interface tables                   :  '
         || ln_failed_count , gv_output);
     xxpso_print_debug  ('-----------------------------------------------------------------------------------------------------------',gv_output);      
         
         --xxpso_print_out_p;
	 IF pv_error_flag = gv_valid_flag THEN
		pv_ret_code := 0;
	 ELSIF pv_error_flag = gv_invalid_flag THEN
		pv_ret_code := 1;
	 END IF;         
      END IF;
      xxpso_print_debug ('Before calling generate excel report: Time:'||to_char(sysdate,'DD-MM-YYYY HH24:MI:SS'),gv_log);
      --calling common utility procedure to generate excel report
      xxpso_cmn_cnv_pkg.print_error_details
                                  (p_request_id         => fnd_global.conc_request_id,
                                   p_rice_group         => 'BOM Conversion',
                                   p_operation          => l_mode_desc,
                                   p_primary_hdr        => 'Batch Id',
                                   p_secondary_hdr      => 'Record Id',
                                   p_tri_hdr            => NULL
                                  );
	xxpso_print_debug ('After calling generate excel report Time:'||to_char(sysdate,'DD-MM-YYYY HH24:MI:SS'),gv_log);                                  
      p_errbuf := g_retcode;

   END xxpso_bom_conv_main_p;

END xxpso_bom_bill_mtl_cnv_pkg;


/
SHOW ERROR

EXEC APPS.XXPSO_INSTALL_PK.VERIFY('XXPSO_BOM_BILL_MTL_CNV_PKG');
EXIT;

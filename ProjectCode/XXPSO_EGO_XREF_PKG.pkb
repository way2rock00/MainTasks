CREATE OR REPLACE PACKAGE BODY XXPSO_EGO_XREF_PKG
AS
--+============================================================|
--| Module Name: Item Cross Reference
--|
--| File Name: XXPSO_EGO_XREF_PKG.sql
--|
--| Description: This package will convert items cross references from
--| staging table into oracle seeded tables.
--|
--| Date: 30-Apr-2016
--|
--| Author: Akshay Nayak
--|
--| Usage: Item Cross Reference Conversion Program
--| Copyright: Pearson
--|
--| All rights reserved.
--|
--+=====================================================================================|
--| Modification History
--+=====================================================================================|
--| Date      		Version		           Who             Description
--| ----------- --------------------------------  -------------------------
--| 30-Apr-2016  	1.0				Akshay Nayak           Initial Creation
--| 07-May-2016  	1.1				Akshay Nayak           Organization validation changes.
--+======================================================================================|

   /******************************************
     global constant variables declaration
   *******************************************/
   gv_log		  VARCHAR2 (10) := 'LOG';
   gv_output		  VARCHAR2 (10) := 'OUTPUT';
   gv_new_flag		  VARCHAR2 (1) := 'N'; 
   gv_failed_flag	  VARCHAR2 (1) := 'E';--'F'; --Initialy it was kept as F (Failed the validation).
   gv_error_flag	  VARCHAR2 (1) := 'F';--'E'; --Initialy it was kept as E (Error while importing).
   gv_processed_flag	  VARCHAR2 (1) := 'P'; 
   gv_valid_flag	  VARCHAR2 (1) := 'V'; 
   gv_invalid_flag	  VARCHAR2 (1) := 'I'; 
   gv_staged_flag	  VARCHAR2 (1) := 'I';--'S'; --Initialy it was kept as S (Interfaced to Interface table).
   gn_user_id              NUMBER := fnd_global.user_id;
   gn_request_id           NUMBER := fnd_global.conc_request_id;
   gn_login_id             NUMBER := fnd_global.login_id;
   gn_batch_id		   NUMBER;
   gv_cross_reference_type VARCHAR2(25) := 'SS_ITEM_XREF';
   gv_validate_mode	   VARCHAR2(15) := 'V';--V stands for Validate mode
   gv_import_mode	   VARCHAR2(15) := 'P';--P stands for Process mode
   gv_create_trn	   VARCHAR2(15) := 'CREATE';
   gv_update_trn	   VARCHAR2(15) := 'UPDATE';
   gc_debug_flag	   VARCHAR2 (2);
   gv_yes_code		   VARCHAR2 (1) := 'Y';
   gv_yes		   VARCHAR2 (3) := 'Yes';
   gv_no_code		   VARCHAR2 (1) := 'N';
   gv_no		   VARCHAR2 (3) := 'No';
   gc_item_ricew_id     CONSTANT    VARCHAR2 (30)       := 'ITEM_XREF';
   gc_ricew_group		CONSTANT    VARCHAR2 (30)       := 'Item-XRef';
   gv_table_name		VARCHAR2(30)	:=  'XXPSO_EGO_XREF_STG';
   gv_arch_table_name	VARCHAR2(30)	:=  'XXPSO_EGO_XREF_ARCH';
   
--|**************************************************************************
--| Description: This procedure will put log messages in the log file.
--|
--| All rights reserved.
--+============================================================|
--| Modification History
--+============================================================|
--| Date                 Who             Description
--| ----------- --------------------  -------------------------
--| 20-Apr-2016  Akshay Nayak           Initial Creation
--    **************************************************************************
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

   /* ********************************************************
   * Procedure: capture_error
   *
   * Synopsis: This procedure is to Log an error in Common Error Table.
   *
   * Parameters:
   *   OUT:
   *   IN:
   *        p_pri_identifier        VARCHAR2        -- Primary Record Identifier
   *        p_sec_identifier        VARCHAR2        -- Secondary Record Identifier
   *        p_ter_identifier        VARCHAR2        -- Third Record Identifier
   *        p_error_code            VARCHAR2        -- Error Code
   *        p_error_column          VARCHAR2        -- Column having Error
   *        p_error_value           VARCHAR2        -- Value in Error Column
   *        p_error_desc            VARCHAR2        -- Error Desc
   *        p_req_action            VARCHAR2        -- required Action to resolve error
   *
   * Return Values:
   *
   * Modifications:
   * WHO                WHAT                                           WHEN
   * ------------------ ---------------------------------------------- ---------------
   * Akshay Nayak    1.0                                            28-APR-2016
   ************************************************************************************* */
    PROCEDURE capture_error
    (
     p_ricew_id            IN VARCHAR2
    ,p_pri_identifier      IN VARCHAR2
    ,p_sec_identifier      IN VARCHAR2
    ,p_ter_identifier      IN VARCHAR2
    ,p_error_code          IN VARCHAR2
    ,p_error_column        IN VARCHAR2
    ,p_error_value         IN VARCHAR2
    ,p_error_desc          IN VARCHAR2
    ,p_req_action          IN VARCHAR2
    ,p_data_source         IN VARCHAR2
    )
    IS
        lc_msg      VARCHAR2(4000) := NULL;
    BEGIN
		 xxpso_print_debug ('p_ricew_id:'||p_ricew_id,gv_log );
        XXPSO_CMN_CNV_PKG.log_error_msg
                                ( p_ricew_id       => p_ricew_id
                                 ,p_track          => 'CMN'
                                 ,p_source         => 'CONVERSION'
                                 ,p_calling_object => 'XXPSO_EGO_XREF_PKG'
                                 ,p_pri_record_id  => p_pri_identifier
                                 ,p_sec_record_id  => p_sec_identifier
                                 ,p_ter_record_id  => p_ter_identifier
                                 ,p_err_code       => p_error_code
                                 ,p_err_column     => p_error_column
                                 ,p_err_value      => p_error_value
                                 ,p_err_desc       => p_error_desc
                                 ,p_rect_action    => p_req_action
                                 ,p_debug_flag     => 'N'
                                 ,p_request_id     => fnd_global.CONC_REQUEST_ID
                                 );
		 xxpso_print_debug ('p_ricew_id:'||p_ricew_id||' Request Id:'||fnd_global.CONC_REQUEST_ID,gv_log );
    EXCEPTION WHEN OTHERS THEN
        lc_msg := 'Unhandled Exception in capture_error procedure. Error Code: '||SQLCODE||' -> '||SQLERRM;
        xxpso_print_debug (lc_msg,gv_log );
    END capture_error;
	
--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
-- Name       : XXPSO_ASSIGN_BATCH_P
-- Description: This procedure controls the batch.
-- This will assign same batch_id to all eligible records in respective mode.
--+============================================================|
--| Modification History
--+============================================================|
--| Date                 Who             Description
--| ----------- --------------------  -------------------------
--| 10-Mar-2016  Akshay Nayak           Initial Creation
--------------------------------------------------------------------------------
  PROCEDURE xxpso_assign_batch_p(p_mode IN VARCHAR2)
  IS
  BEGIN
    gn_batch_id  := XXPSO_EGO_XREF_STG_BATCH_S.NEXTVAL;
    
    xxpso_print_debug ('+   PROCEDURE : assign_batch_id - gn_batch_id :' || gn_batch_id,gv_log );
    
  IF p_mode = gv_validate_mode THEN
    UPDATE XXPSO_EGO_XREF_STG
    SET batch_id = gn_batch_id
      , last_updated_by = gn_user_id
      , last_update_login = gn_login_id
      , last_update_date = SYSDATE
      , CONC_REQUEST_ID = gn_request_id
      , error_message = NULL
    WHERE  ((batch_id IS NULL 	  AND status_code = gv_new_flag) --Newly entered records.
         OR (batch_id IS NOT NULL AND status_code = gv_failed_flag)
		 OR (batch_id IS NOT NULL AND status_code = gv_error_flag)); -- Records previously failed due to some validation error.
   ELSIF p_mode = gv_import_mode THEN
    UPDATE XXPSO_EGO_XREF_STG
    SET batch_id = gn_batch_id
      , last_updated_by = gn_user_id
      , last_update_login = gn_login_id
      , last_update_date = SYSDATE
      , CONC_REQUEST_ID = gn_request_id
      , error_message = NULL
    WHERE batch_id IS NOT NULL AND status_code = gv_valid_flag; -- Records previously failed due to some validation error.   
   END IF;
	COMMIT;

  EXCEPTION
    WHEN OTHERS THEN
      xxpso_print_debug ('Error while assigning batch id: '||SQLERRM, gv_log);
  END xxpso_assign_batch_p;
  
 /* ********************************************************
   * Procedure: identify_archive_records
   *
   * Synopsis: This procedure is to identifying archive records
   *
   * Parameters:
   *   OUT:
   *   IN:
   *
   * Return Values:
   *
   * Modifications:
   * WHO                WHAT                                           WHEN
   * ------------------ ---------------------------------------------- ---------------
   * Akshay Nayak    1.0                                            29-Apr-2016
   ************************************************************************************* */    
    PROCEDURE identify_archive_records
    IS
        lc_msg              VARCHAR2(4000) := NULL;
    BEGIN

        xxpso_print_debug('****************************************************************', gv_log);
        xxpso_print_debug('****        IDENTIFYING ARCHIVE RECORDS - STARTED          *****', gv_log);

		 -- All the records which are processed earlier is marked as ready for archival.
         UPDATE xxpso_ego_xref_stg xref_items_stg
            SET xref_items_stg.ready_to_archive = gv_yes_code
          WHERE xref_items_stg.status_code         IN (gv_processed_flag);

		-- For all the records which was failed/errored earlier , if same record has come again in the current load
		-- then mark those records as ready for archival since these are duplicate records.
		  UPDATE xxpso_ego_xref_stg xref_items_stg1
            SET xref_items_stg1.ready_to_archive = gv_yes_code
          WHERE EXISTS (  SELECT 1
                            FROM xxpso_ego_xref_stg xref_items_stg2
                          WHERE xref_items_stg1.source_system       = xref_items_stg2.source_system    
                            AND xref_items_stg1.source_item_number  = xref_items_stg2.source_item_number
							AND xref_items_stg1.cross_reference_type       = xref_items_stg2.cross_reference_type    
                            AND xref_items_stg1.cross_reference  = xref_items_stg2.cross_reference
							AND DECODE(xref_items_stg1.org_independent_flag,'Y',NVL(xref_items_stg1.organization_id,-1),
							xref_items_stg1.organization_id)
							  = DECODE(xref_items_stg2.org_independent_flag,'Y',NVL(xref_items_stg2.organization_id,-1),
							xref_items_stg2.organization_id)
                            AND xref_items_stg2.status_code         = gv_new_flag
                        )
           AND xref_items_stg1.status_code          IN (gv_failed_flag, gv_error_flag);

        xxpso_print_debug('****        IDENTIFYING ARCHIVE RECORDS - COMPLETED        *****', gv_log);
        xxpso_print_debug('****************************************************************', gv_log);
		COMMIT;
    EXCEPTION WHEN OTHERS
    THEN
        lc_msg := 'Unhandled exception in identify_archive_records. Error: '||SQLCODE||'->'||SQLERRM;
        xxpso_print_debug( lc_msg, gv_log);
    END identify_archive_records;

/* ********************************************************
   * Procedure: purge_if_tables
   *
   * Synopsis: This procedure is to purge interface tables based on the parameter
   *
   * Parameters:
   *   OUT:
   *   IN:
   *
   * Return Values:
   *
   * Modifications:
   * WHO                WHAT                                           WHEN
   * ------------------ ---------------------------------------------- ---------------
   * Akshay Nayak    1.0                                            22-Mar-2016
   ************************************************************************************* */
    PROCEDURE purge_if_tables (p_purge_if_tables IN VARCHAR2)
    IS
        lc_msg              VARCHAR2(4000) := NULL;
    BEGIN

        xxpso_print_debug('****************************************************************',gv_log);
        xxpso_print_debug('****         PURGE INTERFACE TABLES - STARTED             *****',gv_log);

        IF UPPER(p_purge_if_tables) = 'ALL'
        THEN
            EXECUTE IMMEDIATE 'TRUNCATE TABLE INV.MTL_CROSS_REFERENCES_INTERFACE ';
            EXECUTE IMMEDIATE 'TRUNCATE TABLE INV.MTL_INTERFACE_ERRORS ';

        ELSIF UPPER(p_purge_if_tables) = 'PROCESSED'
        THEN
            DELETE FROM INV.MTL_INTERFACE_ERRORS WHERE transaction_id IN
            (
                SELECT transaction_id FROM INV.MTL_CROSS_REFERENCES_INTERFACE      WHERE process_flag = 7
            );
            DELETE FROM INV.MTL_CROSS_REFERENCES_INTERFACE      WHERE process_flag = 7;

        ELSIF UPPER(p_purge_if_tables) = 'UNPROCESSED'
        THEN
            DELETE FROM INV.MTL_INTERFACE_ERRORS WHERE transaction_id IN
            (
                SELECT transaction_id FROM INV.MTL_CROSS_REFERENCES_INTERFACE      WHERE process_flag <> 7
            );
            DELETE FROM INV.MTL_CROSS_REFERENCES_INTERFACE      WHERE process_flag <> 7;

        END IF;

        xxpso_print_debug('****         PURGE INTERFACE TABLES - COMPLETED            *****',gv_log);
        xxpso_print_debug('****************************************************************',gv_log);

    EXCEPTION WHEN OTHERS
    THEN
        lc_msg := 'Unhandled exception in purge_if_tables. Error: '||SQLCODE||'->'||SQLERRM;
        xxpso_print_debug( lc_msg,gv_log);
    END purge_if_tables;
	

    
--|**************************************************************************
--| Description: This procedure will return Attribute Context information.
--|
--| All rights reserved.
--+============================================================|
--| Modification History
--+============================================================|
--| Date                 Who             Description
--| ----------- --------------------  -------------------------
--| 20-Apr-2016  Akshay Nayak           Initial Creation
--    **************************************************************************
-- This procedure first checks for the Global Context.
-- It fetches values provided in the attribute for current record and checks if that value
-- is present in the valueset attached to attribute of Global Context.
-- It repeats same process for Custom Context defined.
-- If custom context defined does not exists in the same it raises an error. 
/*
  PROCEDURE validate_context_attr(p_in_record_id IN NUMBER,
				  p_in_batch_id 				 IN NUMBER,
				  p_in_attr_context 		     IN VARCHAR2,
  				  p_out_valid_flag  			 OUT VARCHAR2,
  				  p_out_err_msg	    			 OUT VARCHAR2)
  IS
      lv_attr_cntxt_valid_flag		VARCHAR2(1)	:= gv_valid_flag; 
      lv_attr_cntx_err_msg		VARCHAR2(4000);  
      
      CURSOR cur_global_cntxt
      IS
      SELECT flex_col_usage.application_column_name column_name,
      		flex_col_usage.flex_value_set_id flex_value_set_id
      			FROM FND_DESCRIPTIVE_FLEXS flex,
      			  fnd_descriptive_flexs_tl flex_tl,
      			  fnd_descr_flex_contexts flex_context,
      			  FND_DESCR_FLEX_COLUMN_USAGES flex_col_usage
      			WHERE flex.application_id                     = flex_tl.application_id
      			AND flex.descriptive_flexfield_name           = flex_tl.descriptive_flexfield_name
      			AND flex_tl.title                             = gv_flex_name
      			AND flex.descriptive_flexfield_name           = flex_context.descriptive_flexfield_name
      			AND flex.application_id                       = flex_context.application_id
      			AND flex_context.enabled_flag                 = 'Y'
      			AND flex_context.global_flag 		      = 'Y'
      			AND flex_context.application_id               = flex_col_usage.application_id(+)
      			AND flex_context.descriptive_flexfield_name   = flex_col_usage.descriptive_flexfield_name(+)
      			AND flex_context.descriptive_flex_context_code= flex_col_usage.descriptive_flex_context_code(+)
				AND flex_col_usage.enabled_flag(+)            = 'Y'
				AND flex_col_usage.required_flag(+)			  = 'Y'
				AND DECODE(
					  (SELECT COUNT(1) FROM fnd_flex_value_sets ffvs ,
						fnd_flex_values ffv WHERE ffvs.flex_value_set_id = flex_col_usage.flex_value_set_id
					  AND ffvs.flex_value_set_id                         = ffv.flex_value_set_id
					  AND ffv.enabled_flag = 'Y'
					  AND sysdate between NVL(ffv.start_date_active,sysdate) AND NVL(ffv.end_date_active,sysdate+1)
					  ),0,'N','Y') = 'Y';
				  
	 TYPE lc_cur_global_cntxt_tbl	IS TABLE OF cur_global_cntxt%ROWTYPE;
	 lc_cur_global_cntxt_tab	lc_cur_global_cntxt_tbl;
	 
	 CURSOR cur_attr_cntxt(l_in_attr_context IN VARCHAR2)
      IS
      SELECT flex_col_usage.application_column_name column_name,
      		flex_col_usage.flex_value_set_id flex_value_set_id,
			DECODE(
					  (SELECT COUNT(1) FROM fnd_flex_value_sets ffvs ,
						fnd_flex_values ffv WHERE ffvs.flex_value_set_id = flex_col_usage.flex_value_set_id
					  AND ffvs.flex_value_set_id                         = ffv.flex_value_set_id
					  AND ffv.enabled_flag = 'Y'
					  AND sysdate between NVL(ffv.start_date_active,sysdate) AND NVL(ffv.end_date_active,sysdate+1)
					  ),0,'N','Y') flex_has_value
      			FROM FND_DESCRIPTIVE_FLEXS flex,
      			  fnd_descriptive_flexs_tl flex_tl,
      			  fnd_descr_flex_contexts flex_context,
      			  FND_DESCR_FLEX_COLUMN_USAGES flex_col_usage
      			WHERE flex.application_id                     = flex_tl.application_id
      			AND flex.descriptive_flexfield_name           = flex_tl.descriptive_flexfield_name
      			AND flex_tl.title                             = gv_flex_name
      			AND flex.descriptive_flexfield_name           = flex_context.descriptive_flexfield_name
      			AND flex.application_id                       = flex_context.application_id
      			AND flex_context.enabled_flag                 = 'Y'
      			AND flex_context.global_flag 		          = 'N' -- This flag indicates that it is custom context
      			AND flex_context.application_id               = flex_col_usage.application_id(+)
      			AND flex_context.descriptive_flexfield_name   = flex_col_usage.descriptive_flexfield_name(+)
      			AND flex_context.descriptive_flex_context_code= flex_col_usage.descriptive_flex_context_code(+)
				AND flex_col_usage.enabled_flag(+)            = 'Y'
				AND flex_col_usage.required_flag(+)			  = 'Y'
				AND flex_context.descriptive_flex_context_code = l_in_attr_context;
				
	 TYPE lc_cur_attr_cntxt_tbl	IS TABLE OF cur_attr_cntxt%ROWTYPE;
	 lc_cur_attr_cntxt_tab	lc_cur_attr_cntxt_tbl;
	 
	 lv_attr_value				VARCHAR2(100);
	 ln_count				    NUMBER;
	 lv_select_clause			VARCHAR2(10) := 'SELECT ';
	 lv_from_clause				VARCHAR2(10) := ' FROM ';
	 lv_table_clause			VARCHAR2(30) := ' XXPSO_EGO_RELATED_ITMS_STG ';
	 lv_where_clause			VARCHAR2(50) := ' WHERE record_id = :1';

  BEGIN
    xxpso_print_debug ('validate_context_attr p_in_record_id :' || p_in_record_id ||' p_in_attr_context:'||p_in_attr_context,gv_log );
	OPEN cur_global_cntxt;
	FETCH cur_global_cntxt
	BULK COLLECT INTO lc_cur_global_cntxt_tab;
	
		FOR index_count IN 1..lc_cur_global_cntxt_tab.COUNT
		LOOP
		  xxpso_print_debug (
		  lv_select_clause||lc_cur_global_cntxt_tab(index_count).column_name
			||lv_from_clause||lv_table_clause||lv_where_clause,gv_log );
		  EXECUTE IMMEDIATE lv_select_clause||lc_cur_global_cntxt_tab(index_count).column_name
			||lv_from_clause||lv_table_clause||lv_where_clause INTO lv_attr_value USING p_in_record_id;
		  xxpso_print_debug ('validate_context_attr lv_attr_value:'||lv_attr_value||
							 ' Value Set Id:'||lc_cur_global_cntxt_tab(index_count).flex_value_set_id,gv_log );

			SELECT count(1)
			 INTO ln_count
			FROM fnd_flex_value_sets ffvs ,
			  fnd_flex_values ffv
			WHERE ffvs.flex_value_set_id = lc_cur_global_cntxt_tab(index_count).flex_value_set_id
			AND ffvs.flex_value_set_id   = ffv.flex_value_set_id 
			AND ffv.enabled_flag = 'Y'
			AND sysdate between NVL(ffv.start_date_active,sysdate) AND NVL(ffv.end_date_active,sysdate+1)
			AND ffv.flex_value = lv_attr_value;
			
			IF ln_count = 0 THEN
				lv_attr_cntxt_valid_flag := gv_invalid_flag;
				lv_attr_cntx_err_msg := lv_attr_cntx_err_msg ||
				'~Value '||lv_attr_value||' provided in '||lc_cur_global_cntxt_tab(index_count).column_name ||
						' does not exists in value set with value set id:'||lc_cur_global_cntxt_tab(index_count).flex_value_set_id||
						' in Global Context';
			CAPTURE_ERROR(   gc_item_ricew_id
							,p_in_batch_id
							,p_in_record_id
							,NULL
							,NULL
							,'Column Name:'||lc_cur_global_cntxt_tab(index_count).column_name
							,lv_attr_value
							,'Value provided doesnot exists in the valueset with Value Set Id:'||lc_cur_global_cntxt_tab(index_count).flex_value_set_id
							,'Contact Functional Team'
							,NULL
						 );						
			END IF;
			
		END LOOP;
	CLOSE cur_global_cntxt;
	
	IF p_in_attr_context IS NOT NULL THEN
		OPEN cur_attr_cntxt(p_in_attr_context);
		FETCH cur_attr_cntxt
		BULK COLLECT INTO lc_cur_attr_cntxt_tab;
			IF lc_cur_attr_cntxt_tab.COUNT = 0 THEN
					lv_attr_cntxt_valid_flag := gv_invalid_flag;
					lv_attr_cntx_err_msg := lv_attr_cntx_err_msg ||
					'~Custom Context '||p_in_attr_context||' is not present in the system:';
			CAPTURE_ERROR(   gc_item_ricew_id
							,p_in_batch_id
							,p_in_record_id
							,NULL
							,NULL
							,'ATTR_CONTEXT'
							,p_in_attr_context
							,'Custom Context is not present in the system'
							,'Contact Functional Team'
							,NULL
						 );	
			ELSE
				FOR index_count IN 1..lc_cur_attr_cntxt_tab.COUNT
				LOOP
				xxpso_print_debug ('Custom Context has flex values: flex_has_value:'||lc_cur_attr_cntxt_tab(index_count).flex_has_value
				,gv_log );				
					IF lc_cur_attr_cntxt_tab(index_count).flex_has_value = 'Y' THEN
						  xxpso_print_debug (
						  lv_select_clause||lc_cur_attr_cntxt_tab(index_count).column_name
							||lv_from_clause||lv_table_clause||lv_where_clause,gv_log );

						  EXECUTE IMMEDIATE lv_select_clause||lc_cur_attr_cntxt_tab(index_count).column_name
							||lv_from_clause||lv_table_clause||lv_where_clause INTO lv_attr_value USING p_in_record_id;
						  xxpso_print_debug ('validate_context_attr lv_attr_value:'||lv_attr_value||
											 ' Value Set Id:'||lc_cur_attr_cntxt_tab(index_count).flex_value_set_id,gv_log );

							SELECT count(1)
							 INTO ln_count
							FROM fnd_flex_value_sets ffvs ,
							  fnd_flex_values ffv
							WHERE ffvs.flex_value_set_id = lc_cur_attr_cntxt_tab(index_count).flex_value_set_id
							AND ffvs.flex_value_set_id   = ffv.flex_value_set_id 
							AND ffv.enabled_flag = 'Y'
					        AND sysdate between NVL(ffv.start_date_active,sysdate) AND NVL(ffv.end_date_active,sysdate+1)
							AND ffv.flex_value = lv_attr_value;
							
							IF ln_count = 0 THEN
								lv_attr_cntxt_valid_flag := gv_invalid_flag;
								lv_attr_cntx_err_msg := lv_attr_cntx_err_msg ||
								'~Value '||lv_attr_value||' provided in '||lc_cur_attr_cntxt_tab(index_count).column_name ||
										' does not exists in value set with value set id:'||lc_cur_attr_cntxt_tab(index_count).flex_value_set_id||
										' in Custom Context:'||p_in_attr_context;
							CAPTURE_ERROR(   gc_item_ricew_id
											,p_in_batch_id
											,p_in_record_id
											,NULL
											,NULL
											,'Column Name:'||lc_cur_attr_cntxt_tab(index_count).column_name
											,lv_attr_value
											,'Value provided doesnot exists in the valueset with Value Set Id:'||lc_cur_attr_cntxt_tab(index_count).flex_value_set_id
											,'Contact Functional Team'
											,NULL
										 );												
							END IF;
					END IF;
			END LOOP;		
			END IF;
		CLOSE cur_attr_cntxt;
	END IF;
	 xxpso_print_debug ('validate_context_attr: lv_attr_cntxt_valid_flag:'||lv_attr_cntxt_valid_flag||' lv_attr_cntx_err_msg:'||lv_attr_cntx_err_msg,gv_log );
	 p_out_valid_flag  		:= lv_attr_cntxt_valid_flag	;
  	 p_out_err_msg	        := lv_attr_cntx_err_msg ;
  
  END validate_context_attr;
  */
 --|**************************************************************************
--| Description: This procedure will be called to validate data in Item 
--| cross references staging table 
--|
--| All rights reserved.
--+============================================================|
--| Modification History
--+============================================================|
--| Date      		Version		           Who             Description
--| ----------- --------------------------------  -------------------------
--| 30-Apr-2016  	1.0				Akshay Nayak           Initial Creation
--| 07-May-2016  	1.1				Akshay Nayak           Organization validation changes.
--    **************************************************************************
   PROCEDURE xxpso_item_xref_validate (pv_err_flag OUT VARCHAR2)
   IS
      lv_record_status 	       VARCHAR2(1);
      lv_err_msg	       VARCHAR2(4000);
      ln_inventory_item_id      NUMBER;
      ln_organization_id        NUMBER;
	  ln_cross_reference_id		NUMBER;
      lv_cross_reference_type	VARCHAR2(25);
	  lv_cross_ref_exists_flag	VARCHAR2(1);
	  lv_org_independent_flag	VARCHAR2(1);
      ln_reln_cnt		NUMBER := 0;
	  --Changes for v1.1 Begin
	  ln_tbl_record_count	NUMBER;
	  --Changes for v1.1 End
      ln_records_inserted       NUMBER := 0;
      ln_records_rejected       NUMBER := 0; 
      ln_source_system_id	NUMBER;
      lv_transaction_type	VARCHAR2(10);
	  
      /* Cursor to  select all the unprocessed records from the staging table */
      CURSOR cur_xref_items
      IS
         SELECT xrfs.*
           FROM xxpso_ego_xref_stg xrfs
          WHERE STATUS_CODE IN (gv_new_flag,gv_failed_flag,gv_error_flag)
            AND batch_id = gn_batch_id
            ORDER by record_id;
          
      TYPE t_bulk_item_xref_typ IS TABLE OF xxpso_ego_xref_stg%ROWTYPE;

      t_bulk_item_xref_tab t_bulk_item_xref_typ;	

      lv_attr_cntxt_valid_flag		VARCHAR2(1);
      lv_attr_cntx_err_msg		VARCHAR2(4000);	  
	BEGIN

      /* For each unprocessed Record */
      OPEN cur_xref_items;
      LOOP
      FETCH cur_xref_items
      BULK COLLECT INTO t_bulk_item_xref_tab LIMIT 10000;
      EXIT WHEN t_bulk_item_xref_tab.count = 0;
		FOR index_count IN 1..t_bulk_item_xref_tab.count
		LOOP
		 lv_record_status := gv_valid_flag;
		 lv_err_msg	  := NULL;
		 ln_organization_id := NULL;
		 lv_org_independent_flag := NULL;
		 ln_inventory_item_id := NULL;
		 ln_cross_reference_id	 := NULL;
		 ln_reln_cnt := 0;
		 ln_source_system_id	:= NULL;
	     --Changes for v1.1 Begin
	     ln_tbl_record_count	:= 0;
	     --Changes for v1.1 End
	  
		 lv_transaction_type	  := gv_create_trn;	
	 /*****************************************************************************************************
		 Validate the cross_reference_type
	 ******************************************************************************************************/				 
		 IF t_bulk_item_xref_tab(index_count).cross_reference_type IS NOT NULL 
		 THEN
			BEGIN
			   SELECT 'Y'
				 INTO lv_cross_ref_exists_flag
				 FROM mtl_cross_reference_types
				WHERE cross_reference_type = t_bulk_item_xref_tab(index_count).cross_reference_type
				  AND NVL(disable_date,SYSDATE)>= SYSDATE;

			  xxpso_print_debug('Cross Refernce exists flag. lv_cross_ref_exists_flag:'||lv_cross_ref_exists_flag||
						' for cross reference type: '||t_bulk_item_xref_tab(index_count).cross_reference_type
						 , gv_log);
			EXCEPTION
			   WHEN NO_DATA_FOUND
			   THEN
				  lv_record_status := gv_invalid_flag;
				  lv_err_msg :=
						lv_err_msg
					 || 'Either cross reference type does not exists or is disabled:'
					 || t_bulk_item_xref_tab(index_count).cross_reference_type;
				  xxpso_print_debug ('Either cross reference type does not exists or is disabled:'
					 || t_bulk_item_xref_tab(index_count).cross_reference_type,
					 gv_log);
					 
					CAPTURE_ERROR(   gc_item_ricew_id
									,t_bulk_item_xref_tab(index_count).batch_id
									,t_bulk_item_xref_tab(index_count).record_id
									,NULL
									,NULL
									,'cross_reference_type'
									,t_bulk_item_xref_tab(index_count).cross_reference_type
									,'Either cross reference type does not exists or is disabled:'
									,'Contact Functional Team'
									,NULL
								 );						 

			   WHEN OTHERS
			   THEN
				  lv_record_status := gv_invalid_flag;
				  lv_err_msg :=
						lv_err_msg
					 || 'Error while Validating the cross_reference_type:'
					 || t_bulk_item_xref_tab(index_count).cross_reference_type
					 || SQLCODE
					 || ' - ' 
					 || SQLERRM;
				  xxpso_print_debug (
						'Error while Validating the cross_reference_type:'
					 || t_bulk_item_xref_tab(index_count).cross_reference_type
					 || SQLCODE
					 || ' - ' 
					 || SQLERRM,
					 gv_log);
				CAPTURE_ERROR(   gc_item_ricew_id
								,t_bulk_item_xref_tab(index_count).batch_id
								,t_bulk_item_xref_tab(index_count).record_id
								,NULL
								,NULL
								,'cross_reference_type'
								,t_bulk_item_xref_tab(index_count).cross_reference_type
								,'Unexpected error while validating cross_reference_type: Error Code:'||SQLCODE||
								 ' Error Message:'||SQLERRM
								,'Contact Technical Team'
								,NULL
							 );						 
			END;				 
		 END IF; -- End of Cross Reference Type validation.
		 
         /* ******************************************************************** *
          * Validating the Organization Code and retrieving the organization ID  *
          * ******************************************************************** */
         IF t_bulk_item_xref_tab(index_count).organization_code IS NOT NULL
         THEN
            BEGIN
               SELECT organization_id
                 INTO ln_organization_id
                 FROM org_organization_definitions ood
                WHERE ood.organization_code =
                         t_bulk_item_xref_tab(index_count).organization_code;
				
				t_bulk_item_xref_tab(index_count).org_independent_flag := 'N';
				
              xxpso_print_debug('Organization Code:' || t_bulk_item_xref_tab(index_count).organization_code 
              || ' Organization_ID:' || ln_organization_id || ' Org Dependent Flag:'||t_bulk_item_xref_tab(index_count).org_independent_flag
              			 , gv_log);
            EXCEPTION
               WHEN NO_DATA_FOUND
               THEN
                  lv_record_status := gv_invalid_flag;
                  lv_err_msg :=
                        lv_err_msg
                     || 'No Organization exists for Organization Code:'
                     || t_bulk_item_xref_tab(index_count).organization_code;
                  xxpso_print_debug ('No Organization exists for Organization Code:'
                     || t_bulk_item_xref_tab(index_count).organization_code,
                     gv_log);
					 
					CAPTURE_ERROR(   gc_item_ricew_id
									,t_bulk_item_xref_tab(index_count).batch_id
									,t_bulk_item_xref_tab(index_count).record_id
									,NULL
									,NULL
									,'organization_code'
									,t_bulk_item_xref_tab(index_count).organization_code
									,'No Organization exists for Organization Code:'
									,'Contact Functional Team'
									,NULL
								 );						 

               WHEN OTHERS
               THEN
                  lv_record_status := gv_invalid_flag;
                  lv_err_msg :=
                        lv_err_msg
                     || 'Error while Validating the Organization code:'
                     || t_bulk_item_xref_tab(index_count).organization_code
					 || SQLCODE
					 || ' - ' 
                     || SQLERRM;
                  xxpso_print_debug (
                        'Error while Validating the Organization code:'
                     || t_bulk_item_xref_tab(index_count).organization_code
					 || SQLCODE
					 || ' - ' 
                     || SQLERRM,
                     gv_log);
				CAPTURE_ERROR(   gc_item_ricew_id
								,t_bulk_item_xref_tab(index_count).batch_id
								,t_bulk_item_xref_tab(index_count).record_id
								,NULL
								,NULL
								,'organization_code'
								,t_bulk_item_xref_tab(index_count).organization_code
								,'Unexpected error while validating organization code: Error Code:'||SQLCODE||
								 ' Error Message:'||SQLERRM
								,'Contact Technical Team'
								,NULL
							 );						 
            END;
         END IF;                 /* End Of Organization id validation */
		 
			/* **************************************************** *
			  *         Retriving the Inventory Item Id              *
			  * **************************************************** */
		/* We can either receive Item_Number or source_item_number.
		 * If Item Number is provided then we have to match it with segmen1 of mtl_system_items_b
		 * If source_item_number is provided then match it to the cross reference table */
		 
		 IF t_bulk_item_xref_tab(index_count).item_number IS NULL AND t_bulk_item_xref_tab(index_count).source_item_number IS NULL
			AND t_bulk_item_xref_tab(index_count).source_system IS NULL THEN
				lv_record_status := gv_invalid_flag;
				lv_err_msg := lv_err_msg ||'~'|| 'Item Number/Source System Item and Source System cannot be Null.';
				xxpso_print_debug ('Item Number/Source System Item and Source System cannot be Null.', gv_log);

				CAPTURE_ERROR(   gc_item_ricew_id
								,t_bulk_item_xref_tab(index_count).batch_id
								,t_bulk_item_xref_tab(index_count).record_id
								,NULL
								,NULL
								,'item_number - source_item_number - source_system'
								,NULL
								,'Item Number and Source Item Number and Source Item System is null'
								,'Contact Functional Team'
								,NULL
							);
			 ELSIF t_bulk_item_xref_tab(index_count).item_number IS NOT NULL THEN	
			 -- If Item Number is not null then fetch Inventory_item_id from mtl_system_items_b

					BEGIN
					   SELECT inventory_item_id
					 INTO ln_inventory_item_id
					 FROM mtl_system_items_b
					WHERE segment1 = t_bulk_item_xref_tab(index_count).item_number
						  AND organization_id = ln_organization_id;

					   xxpso_print_debug ('Item Number:'
					  || t_bulk_item_xref_tab(index_count).item_number
					  || ' Inventory Item_ID:'
					  || ln_inventory_item_id, gv_log);

					EXCEPTION
					   WHEN NO_DATA_FOUND
					   THEN
						   lv_record_status := gv_invalid_flag;
						   lv_err_msg := lv_err_msg ||'~'||
							  'The Inventory Item : '
							 || t_bulk_item_xref_tab(index_count).item_number
							 || ' does not Exist In the Organization with Organization_id : '
							 || ln_organization_id;
						  xxpso_print_debug (
							'The Inventory Item : '
							 || t_bulk_item_xref_tab(index_count).item_number
							 || ' does not Exist In the Organization with Organization_id: '
							 || ln_organization_id,
							 gv_log);
							 
							CAPTURE_ERROR(   gc_item_ricew_id
									,t_bulk_item_xref_tab(index_count).batch_id
									,t_bulk_item_xref_tab(index_count).record_id
									,NULL
									,NULL
									,'item_number'
									,t_bulk_item_xref_tab(index_count).item_number
									,'Item Number does not exists in system for Organization Id:'||ln_organization_id
									,'Contact Functional Team'
									,NULL
								);					 

					   WHEN OTHERS
					   THEN
							lv_record_status := gv_invalid_flag;
							lv_err_msg := lv_err_msg ||'~'||
							  'Error while fetching the Inventory Item_id:'
							 || ' for item with Item Number:'
							 || t_bulk_item_xref_tab(index_count).item_number
							 || ' and the error message is : '
									 || SQLCODE
									 || ' - '
							 || SQLERRM;
							xxpso_print_debug (
							'Error while fetching the Inventory Item_id:'
							 || ' for item with Item Number:'
							 || t_bulk_item_xref_tab(index_count).item_number
							 || ' and the error message is : '
									 || SQLCODE
									 || ' - '
							 || SQLERRM,
							 gv_log);
							CAPTURE_ERROR(   gc_item_ricew_id
									,t_bulk_item_xref_tab(index_count).batch_id
									,t_bulk_item_xref_tab(index_count).record_id
									,NULL
									,NULL
									,'item_number'
									,t_bulk_item_xref_tab(index_count).item_number
									,'Unexpected error while fetching inventory_item_id for item_number: Error Code:'||SQLCODE||
									' Error Message:'||SQLERRM
									,'Contact Technical Team'
									,NULL
								);						 
					END;
				ELSIF t_bulk_item_xref_tab(index_count).source_item_number IS NOT NULL 
					AND t_bulk_item_xref_tab(index_count).source_system IS NOT NULL THEN
				-- Both Source System Item Number and Source System Item is not null
					BEGIN
					SELECT ss_ext.source_system_id
					  INTO ln_source_system_id
					  FROM hz_orig_systems_tl hz_system, 
						   EGO_SOURCE_SYSTEM_EXT ss_ext
					 WHERE UPPER(hz_system.orig_system_name) = t_bulk_item_xref_tab(index_count).source_system
					   AND ss_ext.source_system_id = hz_system.orig_system_id;

					xxpso_print_debug ('Item Source System:'||t_bulk_item_xref_tab(index_count).source_system ||
							   ' Source System Id is:'
									 || ln_source_system_id,
						 gv_log);
					EXCEPTION
					   WHEN NO_DATA_FOUND
					   THEN
						   lv_record_status := gv_invalid_flag;
						   lv_err_msg := lv_err_msg ||'~'||
							  'Source System : '
							 || t_bulk_item_xref_tab(index_count).source_system
							 || ' does not Exist In the system : ';
						  xxpso_print_debug (
							 'Source System : '
							 || t_bulk_item_xref_tab(index_count).source_system
							 || ' does not Exist In the system : ',
							 gv_log);

						  CAPTURE_ERROR(   gc_item_ricew_id
									,t_bulk_item_xref_tab(index_count).batch_id
									,t_bulk_item_xref_tab(index_count).record_id
									,NULL
									,NULL
									,'source_system'
									,t_bulk_item_xref_tab(index_count).source_system
									,'Source Item System does not exists in the system'
									,'Contact Functional Team'
									,NULL
								);

					WHEN OTHERS THEN
						   lv_record_status := gv_invalid_flag;
						   lv_err_msg := lv_err_msg ||'~'||
							  ' Error while fetching Source System Information:'||t_bulk_item_xref_tab(index_count).source_system ||
							  ' Error Message: '||SQLERRM;
						  xxpso_print_debug (
							  ' Error while fetching Source System Information:'||t_bulk_item_xref_tab(index_count).source_system ||
							  ' Error Message: '||SQLERRM,
							 gv_log);
						  
						  CAPTURE_ERROR(   gc_item_ricew_id
									,t_bulk_item_xref_tab(index_count).batch_id
									,t_bulk_item_xref_tab(index_count).record_id
									,NULL
									,NULL
									,'source_system'
									,t_bulk_item_xref_tab(index_count).source_system
									,'Unexpected error while fetching source_system: Error Code:'||SQLCODE||
									' Error Message:'||SQLERRM
									,'Contact Technical Team'
									,NULL
								);
					END;

					IF ln_source_system_id IS NOT NULL THEN
						BEGIN
						   SELECT x_ref.inventory_item_id
							 INTO ln_inventory_item_id
							 FROM mtl_cross_references x_ref
							WHERE x_ref.cross_reference_type = gv_cross_reference_type
							  AND x_ref.cross_reference = t_bulk_item_xref_tab(index_count).source_item_number
							  AND x_ref.source_system_id = ln_source_system_id
							  AND sysdate between NVL(x_ref.start_date_active,sysdate) AND NVL(x_ref.end_date_active,sysdate+1);

							   xxpso_print_debug ('Referenced Item Number:'
							  || t_bulk_item_xref_tab(index_count).source_item_number
							  || 'Inventory Item_ID:'
							  || ln_inventory_item_id, gv_log);

						EXCEPTION
						   WHEN NO_DATA_FOUND
						   THEN
								 lv_record_status := gv_invalid_flag;
								   lv_err_msg := lv_err_msg ||'~'||
									  'The Source Item Number: '
									 || t_bulk_item_xref_tab(index_count).source_item_number
									 || ' and Source System '
									 || t_bulk_item_xref_tab(index_count).source_system
									 || ' combination does not Exist In the System : ';
								  xxpso_print_debug (
									'The Source Item Number: '
									 || t_bulk_item_xref_tab(index_count).source_item_number
									 || ' and Source System '
									 || t_bulk_item_xref_tab(index_count).source_system
									 || ' combination does not Exist In the System : ',
									 gv_log);
									 
								  CAPTURE_ERROR(   gc_item_ricew_id
											,t_bulk_item_xref_tab(index_count).batch_id
											,t_bulk_item_xref_tab(index_count).record_id
											,NULL
											,NULL
											,'source_item_number'
											,t_bulk_item_xref_tab(index_count).source_item_number
											,'Source Item Number and Source System combination does not exists in the system'
											,'Contact Functional Team'
											,NULL
										);						 

						   WHEN OTHERS
						   THEN
								 lv_record_status := gv_invalid_flag;
								 lv_err_msg := lv_err_msg ||'~'||
									  'Error while fetching the Inventory Item_id:'
								 || ' for Source Item Number '
								 || t_bulk_item_xref_tab(index_count).source_item_number
								 || ' and Source System '
								 || t_bulk_item_xref_tab(index_count).source_system
									 || ' and the error message is : '
											 || SQLCODE
											 || ' - '
									 || SQLERRM;
								  xxpso_print_debug (
									'Error while fetching the Inventory Item_id:'
								 || ' for Source Item Number '
								 || t_bulk_item_xref_tab(index_count).source_item_number
								 || ' and Source System '
								 || t_bulk_item_xref_tab(index_count).source_system
									 || ' and the error message is : '
											 || SQLCODE
											 || ' - '
									 || SQLERRM,
									 gv_log);
									 
								  CAPTURE_ERROR(   gc_item_ricew_id
											,t_bulk_item_xref_tab(index_count).batch_id
											,t_bulk_item_xref_tab(index_count).record_id
											,NULL
											,NULL
											,'source_item_number'
											,t_bulk_item_xref_tab(index_count).source_item_number
											,'Unexpected error while fetching Inventory Item Id for'||
										' source_item_number and source_system combination: Error Code:'||SQLCODE||
											 ' Error Message:'||SQLERRM
											,'Contact Technical Team'
											,NULL
										);							 
						END;
					END IF;
			 ELSE
			 -- Either Source System Item Number or Source System Item is null
				lv_record_status := gv_invalid_flag;
				lv_err_msg := lv_err_msg ||'~'|| 'Source System Item Number or Source System Item is Null.';
				xxpso_print_debug ('Source System Item Number or Source System Item is Null.', gv_log);	 
				  CAPTURE_ERROR(   gc_item_ricew_id
							,t_bulk_item_xref_tab(index_count).batch_id
							,t_bulk_item_xref_tab(index_count).record_id
							,NULL
							,NULL
							,'source_item_number - source_system'
							,t_bulk_item_xref_tab(index_count).source_item_number ||'-' ||t_bulk_item_xref_tab(index_count).source_system
							,'Either Source Item Number or Source Item System is null'
							,'Contact Functional Team'
							,NULL
						);	
			 END IF;

         ------------------------------------------------
         --Validate if Cross Reference is already assigned to given item
         ------------------------------------------------
		 --Changes for v1.1 Begin.
		 xxpso_print_debug ('org_independent_flag:'||t_bulk_item_xref_tab(index_count).org_independent_flag,gv_log);
		 xxpso_print_debug ('Check Existence of Item: ln_inventory_item_id:'||ln_inventory_item_id||
							' cross_reference_type:'||t_bulk_item_xref_tab(index_count).cross_reference_type||
							' cross_reference:'||t_bulk_item_xref_tab(index_count).cross_reference, gv_log);		  
		 IF t_bulk_item_xref_tab(index_count).org_independent_flag IS NULL OR 
			t_bulk_item_xref_tab(index_count).org_independent_flag = 'Y' THEN
			lv_org_independent_flag := 'Y';
			--Check if there is any record with Org Independent Flag as Y. 
			--If yes then we will update this record else we will mark it as error.
			SELECT count(1)
			  INTO ln_reln_cnt
			  FROM mtl_cross_references xref
			 WHERE xref.inventory_item_id = ln_inventory_item_id
			   AND xref.cross_reference_type = t_bulk_item_xref_tab(index_count).cross_reference_type
			   AND xref.cross_reference = t_bulk_item_xref_tab(index_count).cross_reference	
			   AND xref.org_independent_flag = 'Y';

			SELECT count(1)
			  INTO ln_tbl_record_count
			  FROM mtl_cross_references xref
			 WHERE xref.inventory_item_id = ln_inventory_item_id
			   AND xref.cross_reference_type = t_bulk_item_xref_tab(index_count).cross_reference_type
			   AND xref.cross_reference = t_bulk_item_xref_tab(index_count).cross_reference;			   
			   
			xxpso_print_debug ('Check 1: ln_reln_cnt'||ln_reln_cnt||' ln_tbl_record_count:'||ln_tbl_record_count,gv_log);
			-- For the first time since the table will have no rows we will have to 
			-- create record.
			IF ln_tbl_record_count = 0 THEN
			  lv_transaction_type	  := gv_create_trn;
			ELSIF ln_tbl_record_count <> 0 THEN
				IF ln_reln_cnt = 1 THEN
					lv_transaction_type	  := gv_update_trn;
				-- In case of Update fetch the Cross Reference Id as it cannot be null in interface table.
					SELECT cross_reference_id
					  INTO ln_cross_reference_id
					  FROM mtl_cross_references xref
					 WHERE xref.inventory_item_id = ln_inventory_item_id
					   AND xref.cross_reference_type = t_bulk_item_xref_tab(index_count).cross_reference_type
					   AND xref.cross_reference = t_bulk_item_xref_tab(index_count).cross_reference
					   AND xref.org_independent_flag = 'Y';			
				ELSIF ln_reln_cnt = 0 THEN
				   lv_record_status := gv_invalid_flag;  
				   lv_err_msg := lv_err_msg ||'~'||
				   'Cross Reference already exists for specific organization_id and hence All Org cannot be set';
				   xxpso_print_debug ('Cross Reference already exists for specific organization_id and hence All Org cannot be set',gv_log);
			  
				  CAPTURE_ERROR(   gc_item_ricew_id
							,t_bulk_item_xref_tab(index_count).batch_id
							,t_bulk_item_xref_tab(index_count).record_id
							,NULL
							,NULL
							,'inventory_item_id - cross_reference_type - cross_reference'
							,ln_inventory_item_id ||' - '||
								t_bulk_item_xref_tab(index_count).cross_reference_type ||' - '||
								t_bulk_item_xref_tab(index_count).cross_reference
							,'Cross Reference already exists for some other organization'
							,'Contact Functional Team'
							,NULL
						);					
				END IF;
			END IF;
			
		 ELSIF t_bulk_item_xref_tab(index_count).org_independent_flag = 'N' THEN
			lv_org_independent_flag := 'N';
			SELECT count(1)
			  INTO ln_reln_cnt
			  FROM mtl_cross_references xref
			 WHERE xref.inventory_item_id = ln_inventory_item_id
			   AND xref.cross_reference_type = t_bulk_item_xref_tab(index_count).cross_reference_type
			   AND xref.cross_reference = t_bulk_item_xref_tab(index_count).cross_reference	
			   AND xref.organization_id = ln_organization_id;
			xxpso_print_debug ('Check 2: ln_reln_cnt'||ln_reln_cnt,gv_log);
			--If no record exists for the combination of inventory_item_id,cross_reference and cross_reference_type 
			-- in the organization then create new record else update existing record.
			IF ln_reln_cnt = 1 THEN
				lv_transaction_type	  := gv_update_trn;
				-- In case of Update fetch the Cross Reference Id as it cannot be null in interface table.
					SELECT cross_reference_id
					  INTO ln_cross_reference_id
					  FROM mtl_cross_references xref
					 WHERE xref.inventory_item_id = ln_inventory_item_id
					   AND xref.cross_reference_type = t_bulk_item_xref_tab(index_count).cross_reference_type
					   AND xref.cross_reference = t_bulk_item_xref_tab(index_count).cross_reference
					   AND xref.organization_id = ln_organization_id;		
			ELSIF ln_reln_cnt = 0 THEN		
				lv_transaction_type	  := gv_create_trn;
			END IF;
		 END IF;
		--Changes for v1.1 End.
		xxpso_print_debug ('Check 2: lv_transaction_type:'||lv_transaction_type||' ln_cross_reference_id:'||ln_cross_reference_id,gv_log);
			/********************************************
             *  Updating the Staging Table for validated records        *
             ********************************************/
            BEGIN
		xxpso_print_debug ('Validate Mode: Record Id:'||t_bulk_item_xref_tab(index_count).record_id ||
				   ' Status:'||lv_record_status,gv_log);
               UPDATE xxpso_ego_xref_stg
                  SET inventory_item_id = ln_inventory_item_id,
                      organization_id = ln_organization_id,
					  source_system_id = ln_source_system_id,
					  cross_reference_id = ln_cross_reference_id,
                      org_independent_flag = lv_org_independent_flag,
					  transaction_type = lv_transaction_type ,
					  start_date_active = SYSDATE,
                      last_update_date = SYSDATE,
                      last_updated_by = gn_user_id,
                      last_update_login = gn_login_id,
                      creation_date = SYSDATE,
                      created_by = gn_user_id,
                      conc_request_id = gn_request_id,
                      error_message = DECODE(lv_record_status,gv_valid_flag,NULL,gv_invalid_flag,lv_err_msg),
                      STATUS_CODE = DECODE(lv_record_status,gv_valid_flag,gv_valid_flag,gv_invalid_flag,gv_failed_flag)
                WHERE record_id = t_bulk_item_xref_tab(index_count).record_id;
		
				IF lv_record_status = gv_valid_flag THEN 
               	   ln_records_inserted := ln_records_inserted + 1;
               	ELSIF lv_record_status = gv_invalid_flag THEN 
               	   ln_records_rejected := ln_records_rejected + 1;
               	   pv_err_flag := gv_invalid_flag;
               	END IF;
            EXCEPTION
               WHEN OTHERS
               THEN
                  lv_record_status := gv_invalid_flag; 
                  pv_err_flag := gv_invalid_flag;
                  lv_err_msg := lv_err_msg ||'~'
                     || 'when others error in procedure xxpso_item_xref_validate for updating staging table record with process_status V for'
                     || 'validated records'
                     || ' and the error message is : '
					 || SQLCODE
					 || ' - '
                     || SQLERRM;
                  xxpso_print_debug (
                     'when others error in procedure xxpso_item_reln_validate for updating staging table record with process_status F for'
                     || 'validated records'
                     || ' and the error message is : '
					 || SQLCODE
					 || ' - '
                     || SQLERRM,
                     gv_log);
					 
				  CAPTURE_ERROR(   gc_item_ricew_id
								,t_bulk_item_xref_tab(index_count).batch_id
								,t_bulk_item_xref_tab(index_count).record_id
								,NULL
								,NULL
								,'record_id'
								,t_bulk_item_xref_tab(index_count).record_id
								,'Unexpected error in xxpso_item_reln_validate procedure for current record id. Error Code:'||SQLCODE||
												 ' Error Message:'||SQLERRM
								,'Contact Technical Team'
								,NULL
							);						 
            END;
		END LOOP;
	  END LOOP;
	  COMMIT;
	  CLOSE cur_xref_items;
	  COMMIT;
     /***************************************************************************************
       Generating the Output file
     ***************************************************************************************/
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
     xxpso_print_debug ( 'Total number of records processed in R12                           :   '
         || (ln_records_inserted+ln_records_rejected), gv_output);
     xxpso_print_debug ('Total number of records validated from the Staging Table            :   '
         || ln_records_inserted , gv_output);
     xxpso_print_debug ('Total number  of records failed during validation                   :   '
         || ln_records_rejected , gv_output);
     xxpso_print_debug  ('-----------------------------------------------------------------------------------------------------------',gv_output);	  
	EXCEPTION
      WHEN OTHERS
      THEN
        xxpso_print_debug(' Error in xxpso_item_xref_validate procedure'
            || SQLERRM ,gv_output);
		
		CAPTURE_ERROR(   gc_item_ricew_id
								,gn_batch_id
								,NULL
								,NULL
								,NULL
								,NULL
								,NULL
								,'Unexpected error in xxpso_item_xref_validate procedure .Error Code:'||SQLCODE||
												 ' Error Message:'||SQLERRM
								,'Contact Technical Team'
								,NULL
							);		
	END xxpso_item_xref_validate; 

	--|**************************************************************************
--| Description: This procedure will be called to import data in Item 
--| cross reference table 
--|
--| All rights reserved.
--+============================================================|
--| Modification History
--+============================================================|
--| Date                 Who             Description
--| ----------- --------------------  -------------------------
--| 20-Apr-2016  Akshay Nayak           Initial Creation
--    **************************************************************************
   PROCEDURE xxpso_item_xref_import (pv_err_flag OUT VARCHAR2)
   IS
      CURSOR cur_xref_items_upld
      IS
         SELECT  xirs.*
           FROM xxpso_ego_xref_stg xirs
          WHERE STATUS_CODE = gv_valid_flag;
          
      TYPE t_bulk_item_xref_typ IS TABLE OF xxpso_ego_xref_stg%ROWTYPE;

      t_bulk_item_xref_tab t_bulk_item_xref_typ;   
      
      CURSOR c_intfcr_batch 
      IS
      SELECT DISTINCT SET_PROCESS_ID
        FROM mtl_cross_references_interface
       WHERE process_flag = 1; --Select only newly inserted records from the interface table.
       
       CURSOR c_update_staged_items
       IS
       SELECT xirs.*
           FROM xxpso_ego_xref_stg xirs
          WHERE STATUS_CODE = gv_staged_flag
            AND batch_id IS NOT NULL ;
            
      TYPE t_stg_item_xref_typ IS TABLE OF xxpso_ego_xref_stg%ROWTYPE;

      t_stg_item_xref_tab t_stg_item_xref_typ;               
      

      ln_records_rejected   NUMBER := 0;
      lv_record_status      VARCHAR2 (1);
      ln_records_inserted   NUMBER := 0;
      lv_err_msg	    VARCHAR2(4000);
      l_request_id_num	    NUMBER;
      lc_phase            VARCHAR2(50);
      lc_status           VARCHAR2(50);
      lc_dev_phase        VARCHAR2(50);
      lc_dev_status       VARCHAR2(50);
      lc_message          VARCHAR2(50);
      l_req_return_status BOOLEAN; 
      ln_intfcr_process_flag	NUMBER;
      ln_intfcr_transaction_id	NUMBER;
	  ln_cross_reference_id		NUMBER;
	  lv_intfcr_err_msg			VARCHAR2(4000);
   BEGIN
      /* *************************************************************** *
       *        INSERTING VALID RECORDS INTO MTL_RELATED_ITEMS_INTERFACE TABLE *
       * *************************************************************** */
      OPEN cur_xref_items_upld;
      LOOP
      FETCH cur_xref_items_upld
      BULK COLLECT INTO t_stg_item_xref_tab LIMIT 10000;
      EXIT WHEN t_stg_item_xref_tab.count = 0;
      	
      	FOR index_count IN 1..t_stg_item_xref_tab.count
		LOOP
			 xxpso_print_debug ('Inserting into MTL_RELATED_ITEMS_INTERFACE', gv_log);
		 lv_record_status := gv_valid_flag; 
		 lv_err_msg       := NULL;
			 BEGIN
				INSERT INTO mtl_cross_references_interface 
									   (
										INVENTORY_ITEM_ID     	,     
										ORGANIZATION_ID         ,    
										CROSS_REFERENCE_TYPE    ,    
										CROSS_REFERENCE         ,    
										LAST_UPDATE_DATE        ,    
										LAST_UPDATED_BY         ,    
										CREATION_DATE           ,    
										CREATED_BY              ,    
										LAST_UPDATE_LOGIN       ,    
										DESCRIPTION             ,    
										ORG_INDEPENDENT_FLAG    ,    
										REQUEST_ID              ,    
										PROGRAM_APPLICATION_ID  ,    
										PROGRAM_ID              ,    
										PROGRAM_UPDATE_DATE     ,    
										TRANSACTION_ID          ,    
										PROCESS_FLAG            ,    
										ORGANIZATION_CODE       ,    
										ITEM_NUMBER             ,    
										ATTRIBUTE1              ,    
										ATTRIBUTE2              ,    
										ATTRIBUTE3              ,    
										ATTRIBUTE4              ,    
										ATTRIBUTE5              ,    
										ATTRIBUTE6              ,    
										ATTRIBUTE7              ,    
										ATTRIBUTE8              ,    
										ATTRIBUTE9              ,    
										ATTRIBUTE10             ,    
										ATTRIBUTE11             ,    
										ATTRIBUTE12             ,    
										ATTRIBUTE13             ,    
										ATTRIBUTE14             ,    
										ATTRIBUTE15             ,    
										ATTRIBUTE_CATEGORY      ,    
										UOM_CODE                ,    
										REVISION_ID             ,    
										UNIT_OF_MEASURE_TL      ,    
										UOM_LANGUAGE            ,    
										REVISION                ,    
										SET_PROCESS_ID          ,    
										CROSS_REFERENCE_ID      ,    
										EPC_GTIN_SERIAL         ,    
										TRANSACTION_TYPE            
							)
							 VALUES (
								t_stg_item_xref_tab(index_count).inventory_item_id		,
								t_stg_item_xref_tab(index_count).organization_id	,
								t_stg_item_xref_tab(index_count).cross_reference_type,
								t_stg_item_xref_tab(index_count).cross_reference,
								t_stg_item_xref_tab(index_count).last_update_date,
								t_stg_item_xref_tab(index_count).last_updated_by,
								t_stg_item_xref_tab(index_count).creation_date          ,    
								t_stg_item_xref_tab(index_count).created_by,
								t_stg_item_xref_tab(index_count).last_update_login,
								t_stg_item_xref_tab(index_count).source_system_itm_desc,
								t_stg_item_xref_tab(index_count).org_independent_flag,
								t_stg_item_xref_tab(index_count).conc_request_id,
								t_stg_item_xref_tab(index_count).program_application_id,
								t_stg_item_xref_tab(index_count).program_id,
								t_stg_item_xref_tab(index_count).program_update_date ,   
								NULL,	--transaction_id. This Transaction id refers to transaction_id of interface table.
								1 ,	--process_flag. All New records will have process_flag as 1.
								t_stg_item_xref_tab(index_count).organization_code,
								t_stg_item_xref_tab(index_count).item_number,
								t_stg_item_xref_tab(index_count).attribute1,
								t_stg_item_xref_tab(index_count).attribute2,
								t_stg_item_xref_tab(index_count).attribute3,
								t_stg_item_xref_tab(index_count).attribute4,
								t_stg_item_xref_tab(index_count).attribute5,
								t_stg_item_xref_tab(index_count).attribute6,
								t_stg_item_xref_tab(index_count).attribute7,
								t_stg_item_xref_tab(index_count).attribute8,
								t_stg_item_xref_tab(index_count).attribute9,
								t_stg_item_xref_tab(index_count).attribute10,
								t_stg_item_xref_tab(index_count).attribute11,
								t_stg_item_xref_tab(index_count).attribute12,
								t_stg_item_xref_tab(index_count).attribute13,
								t_stg_item_xref_tab(index_count).attribute14,
								t_stg_item_xref_tab(index_count).attribute15,								
								t_stg_item_xref_tab(index_count).attribute_category ,
								t_stg_item_xref_tab(index_count).uom_code,
								t_stg_item_xref_tab(index_count).revision_id,
								NULL, -- There is no column for unit_of_measure_tl in staging table
								NULL,-- There is no column for uom_language in staging table
								NULL,-- There is no column for revision in staging table
								t_stg_item_xref_tab(index_count).batch_id , -- Batch Id is set into set_process_id
								t_stg_item_xref_tab(index_count).cross_reference_id ,
								NULL,-- There is no column for epc_gtin_serial in staging table
								t_stg_item_xref_tab(index_count).transaction_type 
								);

				--ln_records_inserted := ln_records_inserted + 1;
				xxpso_print_debug (
				   'Successfully Inserted the Item Cross Reference record into Interface Table',
				   gv_log);
         EXCEPTION
            WHEN OTHERS
            THEN
               lv_record_status := gv_invalid_flag; 
               lv_err_msg := lv_err_msg ||'~'
                  || 'Error while Inserting the Item Cross Reference records into Interface Table: mtl_cross_references_interface'
				  || SQLCODE
				  || ' - '
                  || SQLERRM;
               xxpso_print_debug (
                  'Error while Inserting the Item Cross Reference records into Interface Table: mtl_cross_references_interface'
				  || SQLCODE
				  || ' - '
                  || SQLERRM,
                  gv_log);
				  
			   CAPTURE_ERROR(   gc_item_ricew_id
								,t_stg_item_xref_tab(index_count).batch_id
								,t_stg_item_xref_tab(index_count).record_id
								,NULL
								,NULL
								,'record_id'
								,t_stg_item_xref_tab(index_count).record_id
								,'Unexpected error in xxpso_item_xref_import procedure for current record id. Error Code:'||SQLCODE||
												 ' Error Message:'||SQLERRM
								,'Contact Technical Team'
								,NULL
							);					  

         END;

         IF lv_record_status = gv_invalid_flag
         THEN
            ln_records_rejected := ln_records_rejected + 1;
            pv_err_flag := gv_invalid_flag;
            xxpso_print_debug ('Error Message : ' || lv_err_msg, gv_log);
         END IF;

         /********************************************
          *  Updating the Staging Table for validated records        *
          ********************************************/
        BEGIN
		xxpso_print_debug (
			     'Flag value before updating data back in staging table:'||lv_record_status,
			     gv_log);
               UPDATE xxpso_ego_xref_stg
                  SET STATUS_CODE = DECODE(lv_record_status,gv_valid_flag,gv_staged_flag,gv_invalid_flag,gv_error_flag)
                      ,error_message = DECODE(lv_record_status,gv_valid_flag,NULL,gv_invalid_flag,lv_err_msg)
                WHERE record_id = t_stg_item_xref_tab(index_count).record_id;
            EXCEPTION
               WHEN OTHERS
               THEN
                  lv_record_status := gv_invalid_flag; 
                  pv_err_flag := gv_invalid_flag;
                  lv_err_msg := lv_err_msg ||'~'
                     || 'when others error in procedure xxpso_item_xref_import for updating staging table record with process_status S-Staged for'
                     || 'validated records'
                     || ' and the error message is : '
					 || SQLCODE
					 || ' - '
                     || SQLERRM;
                  xxpso_print_debug (
                     'when others error in procedure xxpso_item_xref_import for updating staging table record with process_status S-Staged for'
                     || 'validated records'
                     || ' and the error message is : '
					 || SQLCODE
					 || ' - '
                     || SQLERRM,
                     gv_log);
			      CAPTURE_ERROR(   gc_item_ricew_id
								,t_stg_item_xref_tab(index_count).batch_id
								,t_stg_item_xref_tab(index_count).record_id
								,NULL
								,NULL
								,'record_id'
								,t_stg_item_xref_tab(index_count).record_id
								,'Unexpected error in xxpso_item_xref_import procedure while '||
								 ' updating status for current record id. Error Code:'||SQLCODE||
												 ' Error Message:'||SQLERRM
								,'Contact Technical Team'
								,NULL
							);						 

            END;
         END LOOP;--End loop of for Loop.
		 COMMIT;
      END LOOP;--End loop of Cursor
      CLOSE cur_xref_items_upld;
      COMMIT;
      -- Call concurrent program to load data from interface table into base table.
      -- Valid records entered into interface table can have different batch_id from the staging table.
      -- Thus call Interface program for each batch_id
      FOR intfcr_batch IN c_intfcr_batch
      LOOP
      xxpso_print_debug ('Raising request for batch_id/set_process_id ='||intfcr_batch.SET_PROCESS_ID,
                     gv_log);
	  l_request_id_num :=
			    fnd_request.submit_request (
			      'EGO'
			    , 'EGOXREF'				        --COncurrent Program ID
			    , 'Import Related Items'			-- Concurrent Program Name
			    , SYSDATE                                   --TO_CHAR(SYSDATE, 'YYYY/MM/DD HH24:MI:SS')
			    , FALSE                                     -- sub_request
			    , intfcr_batch.SET_PROCESS_ID               -- set_process_id
			    , 2                                      -- Delete records from Interface table flag
			    					     -- 1 stands for Yes and 2 stands for No.
		    );
	  COMMIT;
 	  xxpso_print_debug ('Request Id:'||l_request_id_num,gv_log);		    
	  
	  IF l_request_id_num = 0 THEN -- If there was error while submitting concurrent request
	  xxpso_print_debug ('Updating back in staging table.Request Id null case '||
	  		      'Request Id:'||l_request_id_num ||' Batch_Id:'||intfcr_batch.SET_PROCESS_ID,gv_log);	
	  -- Update the staging table records and mark it as error.
	              UPDATE xxpso_ego_xref_stg
	              SET STATUS_CODE = gv_error_flag
	               , error_message = 'Concurrent program could not be submitted for batch_id = '||intfcr_batch.SET_PROCESS_ID
	              WHERE  STATUS_CODE = gv_staged_flag
	              AND    batch_id = intfcr_batch.SET_PROCESS_ID;
	              
          ELSIF l_request_id_num > 0 THEN -- If the concurrent program is submitted then wait for it to get completed.
             LOOP
	        --
	        --To make process execution to wait for 1st program to complete
	        --
	           l_req_return_status :=
	              fnd_concurrent.wait_for_request (request_id      => l_request_id_num
	                                              ,INTERVAL        => 2
	                                              ,max_wait        => 60
	                                               -- out arguments
	                                              ,phase           => lc_phase
	                                              ,STATUS          => lc_status
	                                              ,dev_phase       => lc_dev_phase
	                                              ,dev_status      => lc_dev_status
	                                              ,message         => lc_message
	                                              );						
	        EXIT
	      WHEN UPPER (lc_phase) = 'COMPLETED' OR UPPER (lc_status) IN ('CANCELLED', 'ERROR', 'TERMINATED');
	      END LOOP;
          END IF;
      END LOOP;
    lv_err_msg := NULL;
    --Update status_code back in staging table once all the data of Interface table has been processed.
    xxpso_print_debug('Updating final status from interface table into staging table',gv_log);
    OPEN c_update_staged_items;
    LOOP
    FETCH c_update_staged_items
    BULK COLLECT INTO t_stg_item_xref_tab LIMIT 10000;
    EXIT WHEN t_stg_item_xref_tab.count = 0;
	FOR index_count IN 1..t_stg_item_xref_tab.count
	LOOP
		BEGIN
		 xxpso_print_debug ('Batch Id: '||t_stg_item_xref_tab(index_count).batch_id
				   ||' inventory_item_id:'||t_stg_item_xref_tab(index_count).inventory_item_id
				   ||' cross_reference:'||t_stg_item_xref_tab(index_count).cross_reference
				   ||' cross_reference_type:'||t_stg_item_xref_tab(index_count).cross_reference_type
				   ||' source_system_id:'||t_stg_item_xref_tab(index_count).source_system_id
				   ,gv_log);	
			SELECT intfcr.process_flag 
			       , intfcr.transaction_id
				   ,intfcr.	cross_reference_id
				    , err.error_message
			  INTO ln_intfcr_process_flag
			       , ln_intfcr_transaction_id
				   , ln_cross_reference_id
				   , lv_intfcr_err_msg
			  FROM mtl_cross_references_interface intfcr
				  , mtl_interface_errors err
			 WHERE intfcr.SET_PROCESS_ID = t_stg_item_xref_tab(index_count).batch_id
			   AND intfcr.inventory_item_id = t_stg_item_xref_tab(index_count).inventory_item_id
			   AND intfcr.cross_reference = t_stg_item_xref_tab(index_count).cross_reference
			   AND intfcr.cross_reference_type = t_stg_item_xref_tab(index_count).cross_reference_type
			   AND DECODE(intfcr.org_independent_flag,'Y',NVL(intfcr.organization_id,-1),intfcr.organization_id)
			   = DECODE(t_stg_item_xref_tab(index_count).org_independent_flag,'Y',
			   NVL(t_stg_item_xref_tab(index_count).organization_id,-1),t_stg_item_xref_tab(index_count).organization_id)
			   AND intfcr.transaction_id = err.transaction_id(+);
			   
		xxpso_print_debug (' ln_intfcr_process_flag:'||ln_intfcr_process_flag ||
				   ' ln_intfcr_transaction_id:'||ln_intfcr_transaction_id||
				   ' ln_cross_reference_id:'||ln_cross_reference_id,gv_log);
			   UPDATE xxpso_ego_xref_stg
			   	SET TRANSACTION_ID = DECODE(ln_intfcr_process_flag,7,ln_intfcr_transaction_id,3,NULL)
			   	, STATUS_CODE = DECODE(ln_intfcr_process_flag,7,gv_processed_flag,3,gv_error_flag)
				, cross_reference_id = ln_cross_reference_id
			   	, error_message = DECODE(ln_intfcr_process_flag,7,NULL,3,(select error_message
						FROM mtl_interface_errors WHERE transaction_id = ln_intfcr_transaction_id))
			   WHERE record_id = t_stg_item_xref_tab(index_count).record_id;
			   
			   IF ln_intfcr_process_flag = 7 THEN 
			   	ln_records_inserted := ln_records_inserted + 1;
			   ELSIF ln_intfcr_process_flag = 3 THEN 
			   	ln_records_rejected := ln_records_rejected + 1;
				pv_err_flag := gv_invalid_flag;
				CAPTURE_ERROR(   gc_item_ricew_id
								,t_stg_item_xref_tab(index_count).batch_id
								,t_stg_item_xref_tab(index_count).record_id
								,NULL
								,NULL
								,'record_id'
								,t_stg_item_xref_tab(index_count).record_id
								,lv_intfcr_err_msg
								,'Contact Functional Team'
								,NULL
							);					
			   END IF;
			   
		EXCEPTION
		WHEN OTHERS THEN
			lv_err_msg := 'Error while fetching status information from interface table:'||
					  ' Error Code: '||SQLCODE || ' Error Message:'||SQLERRM;
			ln_records_rejected := ln_records_rejected + 1;					  
			UPDATE xxpso_ego_xref_stg
			SET STATUS_CODE = gv_error_flag
			, error_message = lv_err_msg
			WHERE record_id = t_stg_item_xref_tab(index_count).record_id;
			
			   CAPTURE_ERROR(   gc_item_ricew_id
								,t_stg_item_xref_tab(index_count).batch_id
								,t_stg_item_xref_tab(index_count).record_id
								,NULL
								,NULL
								,'record_id'
								,t_stg_item_xref_tab(index_count).record_id
								,'Unexpected error in xxpso_item_xref_import procedure while '||
								 ' updating final status from interface table for current record id. Error Code:'||SQLCODE||
												 ' Error Message:'||SQLERRM
								,'Contact Technical Team'
								,NULL
							);				
		END;
      	END LOOP;
    END LOOP;
	COMMIT;
	CLOSE c_update_staged_items;
      COMMIT;

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
         || (ln_records_inserted+ln_records_rejected), gv_output);
     xxpso_print_debug ('Total number of records uploaded in base tables                       :   '
         || ln_records_inserted , gv_output);
     xxpso_print_debug ('Total number  of records failed in interface tables                   :   '
         || ln_records_rejected , gv_output);
     xxpso_print_debug  ('-----------------------------------------------------------------------------------------------------------',gv_output);
   END xxpso_item_xref_import;
   
   --  **************************************************************************
   --    *
   --    * Procedure Name
   --    *  xxpso_item_xref_main
   --    *
   --    *
   --   * DESCRIPTION
   --    *  Procedure to navigate the program
   --    *
   --    *DATE         AUTHOR(S)               DESCRIPTION
   --    *----------- ----------------------  -----------------    ----------
   --    *20-Apr-2011  Akshay Nayak           INITIAL VERSION
   --    **************************************************************************
   PROCEDURE xxpso_item_xref_main (p_errbuf              OUT VARCHAR2
                                  ,p_retcode             OUT NUMBER
                                  ,p_mode   		     IN     VARCHAR2
                                  ,p_purge_if_tables     IN             VARCHAR2
								  ,p_archive_stg_tables  IN             VARCHAR2
								  ,p_debug_flag          IN             VARCHAR2
                                  )
   IS
   pv_ret_code 		NUMBER;
   pv_error_flag	VARCHAR2(10);	
   pv_arcv_err		VARCHAR2(1000);
   BEGIN

	xxpso_print_debug('******************************************************************',gv_output);
	xxpso_print_debug('*****************INPUT PARAMTERS**************************',gv_output);
	gc_debug_flag       := NVL(p_debug_flag,'N');	
	xxpso_print_debug('p_mode :'||p_mode,gv_output);
	xxpso_print_debug('p_mode :'||p_mode,gv_log);
	xxpso_print_debug('p_debug_flag :'||p_debug_flag,gv_output);
	xxpso_print_debug('p_debug_flag :'||p_debug_flag,gv_log);
	xxpso_print_debug('p_purge_if_tables :'||p_purge_if_tables,gv_output);
	xxpso_print_debug('p_purge_if_tables :'||p_purge_if_tables,gv_log);
	xxpso_print_debug('p_archive_stg_tables :'||p_archive_stg_tables,gv_output);
	xxpso_print_debug('p_archive_stg_tables :'||p_archive_stg_tables,gv_log);

        IF p_archive_stg_tables             = gv_yes_code
        THEN
            --calling procedure to archive staging table records
            xxpso_print_debug( '+++ Calling IDENTIFY_ARCHIVE_RECORDS procedure +++',gv_log );
            identify_archive_records;
			
			xxpso_print_debug( '+++ Calling ARCHIVE_STG_TABLE procedure +++',gv_log );
			XXPSO_EGO_PROCESS_ITEMS_PKG.archive_stg_table(gv_table_name,gv_arch_table_name,pv_arcv_err);
			xxpso_print_debug( '+++ Output from ARCHIVE_STG_TABLE procedure: '||pv_arcv_err,gv_log );
			
			IF pv_arcv_err <> 'SUCESS' THEN 
				pv_error_flag := gv_invalid_flag;
				p_errbuf := pv_arcv_err;
			END IF;
        END IF;           
        
        IF p_purge_if_tables IS NOT NULL
        THEN
            --calling procedure to purge Interface Tables records
            xxpso_print_debug( '+++ Calling PURGE_IF_TABLES procedure +++',gv_log );
            purge_if_tables (p_purge_if_tables);
        END IF;

		
      /*****************************************
      main procedure to Navigate the program
      *****************************************/
         IF p_mode IN ( gv_validate_mode,gv_import_mode)
         THEN
            xxpso_print_debug ('******************************************************************',gv_output);
            xxpso_print_debug ('********************** xxpso_item_xref_validate*******************',gv_output);
            xxpso_print_debug ('******************************************************************',gv_output);
            /******************************************
                Calling validation procedure
            ******************************************/
            pv_error_flag := gv_valid_flag;
            xxpso_assign_batch_p(gv_validate_mode);
            xxpso_item_xref_validate(pv_error_flag);
			 IF pv_error_flag = gv_valid_flag THEN
				pv_ret_code := 0;
			 ELSIF pv_error_flag = gv_invalid_flag THEN
				pv_ret_code := 1;
			 END IF;			
         END IF;
         IF p_mode = gv_import_mode
         THEN
            xxpso_print_debug ('******************************************************************',gv_output);
            xxpso_print_debug ('********************** xxpso_item_xref_import*******************',gv_output);
            xxpso_print_debug ('******************************************************************',gv_output);
            /*****************************************
                Calling Data Upload  procedure
           ******************************************/
            pv_error_flag := gv_valid_flag;
            xxpso_assign_batch_p(gv_import_mode);
            xxpso_item_xref_import(pv_error_flag);
			 IF pv_error_flag = gv_valid_flag THEN
				pv_ret_code := 0;
			 ELSIF pv_error_flag = gv_invalid_flag THEN
				pv_ret_code := 1;
			 END IF;			
         END IF;
            xxpso_print_debug ('******************************************************************',gv_output);
            xxpso_print_debug ('********************** END OF THE ITEM CROSS REFERENCE CONVERSION PROGRAM *******************',gv_output);
            xxpso_print_debug ('******************************************************************',gv_output);

			--calling common utility procedure to generate excel report
		xxpso_print_debug( '+++ Calling XXPSO_CMN_CNV_PKG.PRINT_ERROR_DETAILS procedure +++',gv_log );
        xxpso_print_debug( '+++ Calling XXPSO_CMN_CNV_PKG.PRINT_ERROR_DETAILS procedure +++',gv_output );
		IF p_mode = gv_import_mode THEN
			XXPSO_CMN_CNV_PKG.print_error_details(  p_request_id     => gn_request_id,
													p_rice_group     => gc_ricew_group,
													p_operation      => 'VALIDATE AND IMPORT',
													p_primary_hdr    => 'Batch ID',
													p_secondary_hdr  => 'Record ID',
													p_tri_hdr        => NULL
													);
		ELSE
			XXPSO_CMN_CNV_PKG.print_error_details(  p_request_id     => gn_request_id,
													p_rice_group     => gc_ricew_group,
													p_operation      => 'VALIDATE',
													p_primary_hdr    => 'Batch ID',
													p_secondary_hdr  => 'Record ID',
													p_tri_hdr        => NULL
													);
		END IF;
	p_retcode := pv_ret_code;
   EXCEPTION
      WHEN OTHERS
      THEN
	p_retcode := 1;
	p_errbuf := 'Error in xxpso_item_xref_main: '||SQLERRM;
   END xxpso_item_xref_main;
END XXPSO_EGO_XREF_PKG;
/

SHOW ERROR
EXIT SUCCESS
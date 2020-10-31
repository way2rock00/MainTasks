
REM ===============================================================================
REM  Program:      SLC_ISP_FASSUPP_CNV_BPKG.sql
REM  Author:       Akshay Nayak
REM  Date:         16-Feb-2017
REM  Purpose:      This package body is used to execute the following activities :
REM                1. Transform and validate the data in the Staging Table
REM                2. Import the supplier data in base tables and create UDA
REM  Change Log:   1.0 		16-Feb-2017 Akshay Nayak 		Created
REM				   1.1		22-Mar-2017 Akshay Nayak		Changed for comment 1.6 in CV040. Added 
REM															First Name , Middle Name and Last Name in Supplier UDA.
REM				   1.2 		19-APR-2017 Akshay Nayak		Changed pay group to VALUETECH and added ZID in UDAs.
REM				   1.3		22-APR-2017 Akshay Nayak		Removed logic for site repeating.
REM				   1.4		09-MAY-2017 Akshay Nayak		Changed the logic for tax payer id population
REM 			   1.5      18-MAY-2017 Akshay Nayak		Changed code to refer to Valueset instead of lookups.
REM 			   1.6 		30-AUG-2017 Akshay Nayak		Added logic to verify if site is already existing for supplier.
REM  ================================================================================


CREATE OR REPLACE PACKAGE BODY SLC_ISP_FASSUPP_CNV_PKG AS

  -- global variables
  

gv_log 		VARCHAR2(5) := 'LOG';
gv_out		VARCHAR2(5) := 'OUT';
gv_debug_flag	VARCHAR2(3) ;
gv_yes_code	VARCHAR2(3) := 'YES';
gv_no_code	VARCHAR2(3) := 'NO';
gv_new_status	VARCHAR2(3) := 'N';--New Status
gv_invalid_status	VARCHAR2(3) := 'F';--Failed Validation Errors Status
gv_valid_status	VARCHAR2(3) := 'V';--Validated Status
gv_error_status	VARCHAR2(3) := 'E';--Failed while importing into Oracle Supplier Hub
gv_processed_status	VARCHAR2(3) := 'P';--Successfully imported into Oracle Supplier Hub
gv_validate_mode		VARCHAR2(20) := 'VALIDATE';
gv_revalidate_mode		VARCHAR2(20) := 'REVALIDATE';
gv_process_mode		VARCHAR2(20) := 'PROCESS';
--gv_valid_process_mode		VARCHAR2(20) := 'VALIDATEPROCESS';
gn_batch_id						NUMBER;
gn_request_id                             NUMBER DEFAULT fnd_global.conc_request_id;
gn_user_id                                NUMBER DEFAULT fnd_global.user_id;
gn_login_id                               NUMBER DEFAULT fnd_global.login_id;	
gn_program_status				NUMBER;

--Variables for Common Error Handling.
gv_batch_key				  VARCHAR2(50) DEFAULT 'FRC-C-011'||'-'||TO_CHAR(SYSDATE,'DDMMYYYY');
gv_business_process_name 		  VARCHAR2(100)  := 'SLC_ISP_FASSUPP_CNV_PKG';
gv_cmn_err_rec 				  APPS.SLC_UTIL_JOBS_PKG.G_ERROR_TBL_TYPE;
gv_cmn_err_count			  NUMBER DEFAULT 0;


/* ****************************************************************
	NAME:              slc_write_log_p
	PURPOSE:           This procedure will insert data into either
		    concurrent program log file or in concurrent program output file
		    based on the parameter passed to the input program
	Input Parameters:  p_in_message
			   p_in_log_type
*****************************************************************/
  PROCEDURE slc_write_log_p(p_in_log_type IN VARCHAR2
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
  
  END slc_write_log_p;

/* ****************************************************************
	NAME:              slc_is_date_valid_f
	PURPOSE:           This function will determine if the date format is valid.
	Input Parameters:  p_in_date
*****************************************************************/
   FUNCTION slc_is_date_valid_f(p_in_date VARCHAR2) RETURN VARCHAR2
   IS
   ld_temp_date		DATE;
   lv_valid_flag	VARCHAR2(1)		DEFAULT 'Y';
   BEGIN
	   slc_write_log_p(gv_log,'In slc_is_date_valid_f: p_in_date'||p_in_date);
		   
	   BEGIN
	   SELECT to_date(p_in_date,'YYMMDD') INTO ld_temp_date FROM DUAL;
	   EXCEPTION
	   WHEN OTHERS THEN
	   lv_valid_flag := 'N';
	   END;
		   
   RETURN lv_valid_flag;
   END slc_is_date_valid_f;
   
/* ****************************************************************
	NAME:              slc_get_taxpayer_id_count_f
	PURPOSE:           This function will return count of suppliers converted
					   by fas conversion and having same tax payer id.
	Input Parameters:  p_in_date
*****************************************************************/
   FUNCTION slc_get_taxpayer_id_count_f(p_in_taxpayer_id VARCHAR2) RETURN NUMBER
   IS
   CURSOR c_taxpayer_count(p_in_taxpayer_id 	IN	VARCHAR2)
   IS
   SELECT count(1)
     FROM hz_parties hp
	WHERE hp.jgzz_fiscal_code = p_in_taxpayer_id
	  AND EXISTS
			  (SELECT 1
			  FROM pos_supp_prof_ext_b pos
			  WHERE pos.party_id  = hp.party_id
			  AND pos.c_ext_attr4 = 'FAS'
			  );	
   ln_tax_payer_count	NUMBER;
   BEGIN
   slc_write_log_p(gv_log,'In slc_get_taxpayer_id_count_f Start: p_in_taxpayer_id'||p_in_taxpayer_id);
   OPEN c_taxpayer_count(p_in_taxpayer_id);
   FETCH c_taxpayer_count INTO ln_tax_payer_count;
   CLOSE c_taxpayer_count;
   slc_write_log_p(gv_log,'In slc_get_taxpayer_id_count_f: ln_tax_payer_count'||ln_tax_payer_count);
   RETURN ln_tax_payer_count;
   END slc_get_taxpayer_id_count_f;
   
   /* ****************************************************************
	NAME:              slc_get_transformed_date_p
	PURPOSE:           This procedure will return transformed date object.
	Input Parameters:  p_in_date
*****************************************************************/
   FUNCTION slc_get_transformed_date_p(p_in_date VARCHAR2) RETURN DATE
   IS
   ld_date		DATE;
   lv_day_month	VARCHAR2(10);
   lv_year		VARCHAR2(5);
   BEGIN
	   --Since p_in_date will be in format YYMMDD extract MMDD part.
	   --Earlier format was DD/MM/YY now the format has been changed to YYMMDD.
	   slc_write_log_p(gv_log,'In slc_get_transformed_date_p  p_in_date:'||p_in_date);
	   IF p_in_date IS NOT NULL THEN
		   lv_day_month := substr(p_in_date,3);
		   lv_year := substr(p_in_date,1,2);
		   IF lv_year <=17 THEN
			
		   SELECT to_date(lv_day_month||'20'||lv_year,'MMDDYYYY') INTO ld_date FROM DUAL;
		   slc_write_log_p(gv_log,'In slc_get_transformed_date_p in 1 lv_year:'||lv_year||' lv_day_month:'||lv_day_month);
		   ELSE
		   SELECT to_date(lv_day_month||'19'||lv_year,'MMDDYYYY') INTO ld_date FROM DUAL;
		   slc_write_log_p(gv_log,'In slc_get_transformed_date_p in 2 lv_year:'||lv_year||' lv_day_month:'||lv_day_month);
		   END IF;
	   END IF;
   RETURN ld_date;
   END slc_get_transformed_date_p;

/* ****************************************************************
	NAME:              slc_populate_err_object_p
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
  PROCEDURE slc_populate_err_object_p(p_in_batch_key		IN VARCHAR2
  			       ,p_in_business_entity 	IN VARCHAR2
  			       ,p_in_process_id1	IN VARCHAR2 DEFAULT NULL
  			       ,p_in_process_id2	IN VARCHAR2 DEFAULT NULL
				   ,p_in_process_id3	IN VARCHAR2 DEFAULT NULL
				   ,p_in_process_id4	IN VARCHAR2 DEFAULT NULL
				   ,p_in_process_id5	IN VARCHAR2 DEFAULT NULL
				   ,p_in_business_process_step IN VARCHAR2 DEFAULT NULL
  			       ,p_in_error_code		IN VARCHAR2 DEFAULT NULL
  			       ,p_in_error_txt		IN VARCHAR2
  			       ,p_in_request_id		IN NUMBER
  			       ,p_in_attribute1		IN VARCHAR2 DEFAULT NULL
  			       ,p_in_attribute2		IN VARCHAR2 DEFAULT NULL
  			       ,p_in_attribute3		IN VARCHAR2 DEFAULT NULL
  			       ,p_in_attribute4		IN VARCHAR2 DEFAULT NULL
  			       ,p_in_attribute5		IN VARCHAR2 DEFAULT NULL
  			       )
  IS
  BEGIN

      gv_cmn_err_count := gv_cmn_err_count + 1;
      gv_cmn_err_rec(gv_cmn_err_count).seq := SLC_UTIL_BATCH_KEY_S.NEXTVAL;
      gv_cmn_err_rec(gv_cmn_err_count).business_process_entity   := p_in_business_entity;
      gv_cmn_err_rec(gv_cmn_err_count).business_process_id1      := p_in_process_id1;
      gv_cmn_err_rec(gv_cmn_err_count).business_process_id2      := p_in_process_id2;
	  gv_cmn_err_rec(gv_cmn_err_count).business_process_id3      := p_in_process_id3;
	  gv_cmn_err_rec(gv_cmn_err_count).business_process_id4      := p_in_process_id4;
	  gv_cmn_err_rec(gv_cmn_err_count).business_process_id5      := p_in_process_id5;
	  gv_cmn_err_rec(gv_cmn_err_count).business_process_step      := p_in_business_process_step;
      gv_cmn_err_rec(gv_cmn_err_count).ERROR_CODE                := p_in_error_code;
      gv_cmn_err_rec(gv_cmn_err_count).ERROR_TEXT                := p_in_error_txt;
      gv_cmn_err_rec(gv_cmn_err_count).request_id                := p_in_request_id;
      gv_cmn_err_rec(gv_cmn_err_count).attribute1                := p_in_attribute1; 
      gv_cmn_err_rec(gv_cmn_err_count).attribute2                := p_in_attribute2;
      gv_cmn_err_rec(gv_cmn_err_count).attribute3                := p_in_attribute3;
      gv_cmn_err_rec(gv_cmn_err_count).attribute4                := p_in_attribute4;
      gv_cmn_err_rec(gv_cmn_err_count).attribute5                := p_in_attribute5;
  END slc_populate_err_object_p;   
  
  
/* ****************************************************************
	NAME:              slc_assign_batch_id_p
	PURPOSE:           This procedure will be used to assign batch id to records
							 before processing
	Input Parameters:  p_in_status1		IN VARCHAR2
			   			 p_in_status2		IN VARCHAR2
			   			 p_in_status3		IN VARCHAR2
			   			 p_in_batch_size	IN NUMBER
*****************************************************************/
	PROCEDURE slc_assign_batch_id_p(p_in_status1		IN VARCHAR2
			   		 ,p_in_status2		IN VARCHAR2
			   		,p_in_status3		IN VARCHAR2
			   		,p_in_batch_size	IN NUMBER
						)
	IS 
	BEGIN
		slc_write_log_p(gv_log,'In slc_assign_batch_id_p p_in_status1:'||p_in_status1||
							  ' p_in_status2:'||p_in_status2||
							  ' p_in_status3:'||p_in_status3||' p_in_batch_size:'||p_in_batch_size);

		gn_batch_id := SLC_ISP_FASSUPP_BATCH_ID_S.NEXTVAL;
		UPDATE SLC_ISP_FAS_SUPPIER_CNV_STG 
			SET BATCH_ID = gn_batch_id
				,request_id = gn_request_id
				,last_update_date = sysdate
				,last_updated_by = gn_user_id
				,last_update_login = gn_login_id
		 WHERE status IN (p_in_status1,p_in_status2,p_in_status3)
			AND rownum <= p_in_batch_size;
		slc_write_log_p(gv_log,'Batch Id :'||gn_batch_id);

	END slc_assign_batch_id_p;

	/* ****************************************************************
	NAME:              slc_get_lookup_meaning_p
	PURPOSE:           This procedure will be used to get lookup meaning.
	Input Parameters:  p_in_lookup_type		IN VARCHAR2
			   			 p_in_lookup_code		IN VARCHAR2
*****************************************************************/
	FUNCTION slc_get_lookup_meaning_p(p_in_valueset_name		IN VARCHAR2
							   ,p_in_valueset_value		IN VARCHAR2)
	RETURN VARCHAR2
	IS
	--Changes for v1.5 
	--Changed the code to validate data from valueset instead of lookup.
	/*
	CURSOR c1
	IS
	SELECT description
	  FROM fnd_lookup_values
	 WHERE ENABLED_FLAG = 'Y'
       AND TRUNC(SYSDATE) BETWEEN TRUNC(NVL(START_DATE_ACTIVE,SYSDATE)) AND TRUNC(NVL(END_DATE_ACTIVE,SYSDATE))
	   AND lookup_type = p_in_lookup_type
	   AND lookup_code = p_in_lookup_code;*/
	 
	 CURSOR c1
	 IS
		SELECT ffvt.flex_value_meaning
		FROM fnd_flex_value_sets ffvs ,
		  fnd_flex_values ffv ,
		  fnd_flex_values_tl ffvt
		WHERE ffvs.flex_value_set_name = p_in_valueset_name
		AND ffvs.flex_value_set_id      = ffv.flex_value_set_id
		AND ffv.enabled_flag            = 'Y'
		AND TRUNC(SYSDATE) BETWEEN TRUNC(NVL(START_DATE_ACTIVE,SYSDATE)) AND TRUNC(NVL(END_DATE_ACTIVE,SYSDATE))
		AND ffvt.description = p_in_valueset_value
		AND ffv.flex_value_id = ffvt.flex_value_id;	 
	lv_description		fnd_flex_values_tl.flex_value_meaning%TYPE;	
	BEGIN
	
	IF p_in_valueset_value IS NOT NULL THEN
		OPEN c1;
		FETCH c1 INTO lv_description;
		CLOSE c1;
	END IF;	
	RETURN lv_description;	
	END slc_get_lookup_meaning_p;
/*	*****************************************************************
	NAME:              slc_print_summary_p
	PURPOSE:           This procedure will print summary information after Conversion program is run.
	Input Parameters:  p_processing_mode IN VARCHAR2
*****************************************************************/		
	PROCEDURE slc_print_summary_p(p_processing_mode	IN  VARCHAR2)
	IS
	ln_total_count						NUMBER;
	ln_total_success_count				NUMBER;
	ln_total_fail_count					NUMBER;
	ln_total_fran1_count				NUMBER;
	ln_total_fran2_count				NUMBER;
	ln_total_corp_count					NUMBER;
	ln_total_fran_count					NUMBER;
	ln_total_site_count					NUMBER;
	

   --Common error logging code.  
  lv_batch_status			VARCHAR2(1);
  lv_publish_flag			VARCHAR2(1);
  lv_system_type			VARCHAR2(10);
  lv_source				VARCHAR2(10);
  lv_destination			VARCHAR2(10);
  lv_cmn_err_status_code		VARCHAR2(100);
  lv_cmn_err_msg			VARCHAR2(1000);  
  lv_business_process_id1		VARCHAR2(25) := NULL; --Reserved for Parent Record Id 
  lv_business_process_id2		VARCHAR2(25) := NULL;  --Reserved for Child Record Id
  lv_business_process_id3		VARCHAR2(25) := NULL; 
  lv_business_entity_name			  VARCHAR2(50) := 'SLC_MAIN_P';  
  
	CURSOR cur_err_rec(p_in_status IN VARCHAR2)
	IS
	SELECT record_id,error_msg 
	  FROM SLC_ISP_FAS_SUPPIER_CNV_STG 
	 WHERE request_id = gn_request_id
	   AND status = p_in_status;
	lc_cur_err_rec   cur_err_rec%ROWTYPE;
	
	BEGIN
	
	SELECT count(1)
	  INTO ln_total_count
	 FROM SLC_ISP_FAS_SUPPIER_CNV_STG
	 WHERE request_id = gn_request_id;

	slc_write_log_p(gv_out,'****************Output******************');
	slc_write_log_p(gv_out,'Total Records:'||ln_total_count);
	IF p_processing_mode IN (gv_validate_mode,gv_revalidate_mode) THEN
		SELECT count(1)
		  INTO ln_total_success_count
		 FROM SLC_ISP_FAS_SUPPIER_CNV_STG
		 WHERE request_id = gn_request_id
		   AND status = gv_valid_status;

		SELECT count(1)
		  INTO ln_total_fail_count
		 FROM SLC_ISP_FAS_SUPPIER_CNV_STG
		 WHERE request_id = gn_request_id
		   AND status = gv_invalid_status;		   
		   
		slc_write_log_p(gv_out,'Total records validated:'||ln_total_success_count);
		slc_write_log_p(gv_out,'Total records which failed during validation:'||ln_total_fail_count);
		slc_write_log_p(gv_out,'***************************************************');
		slc_write_log_p(gv_out,rpad('Record Id',25,' ')||'Error Message');
		OPEN cur_err_rec(gv_invalid_status);
		LOOP
		FETCH cur_err_rec INTO lc_cur_err_rec;
		EXIT WHEN cur_err_rec%NOTFOUND;
		slc_write_log_p(gv_out,rpad(lc_cur_err_rec.record_id,25,' ')||lc_cur_err_rec.error_msg);
     	slc_populate_err_object_p(p_in_batch_key => gv_batch_key
     			,p_in_business_entity => lv_business_entity_name
     			,p_in_process_id3 => NULL
     			,p_in_error_txt => lc_cur_err_rec.error_msg
     			,p_in_request_id => gn_request_id
     			,p_in_attribute1 => 'Record Id:'||lc_cur_err_rec.record_id
     			);		
		END LOOP;	
		CLOSE cur_err_rec;
		slc_write_log_p(gv_out,'***************************************************');
		
		IF ln_total_fail_count = 0 THEN
			gn_program_status := 0;
		ELSIF ln_total_fail_count <> ln_total_count THEN
			gn_program_status := 1;
		ELSE
			gn_program_status := 2;
		END IF;
	END IF;
	IF p_processing_mode IN (gv_process_mode) THEN
	SELECT count(1)
	  INTO ln_total_success_count
	 FROM SLC_ISP_FAS_SUPPIER_CNV_STG
	 WHERE request_id = gn_request_id
	   AND status = gv_processed_status;

	SELECT count(1)
	  INTO ln_total_fail_count
	 FROM SLC_ISP_FAS_SUPPIER_CNV_STG
	 WHERE request_id = gn_request_id
	   AND status = gv_error_status;	
	
	/*
	SELECT count(distinct(franchisee1_party_id))
	  INTO ln_total_fran1_count
	  FROM SLC_ISP_FAS_SUPPIER_CNV_STG
	  WHERE request_id = gn_request_id
	   AND status = gv_processed_status
	   AND franchisee1_party_id IS NOT NULL;
	   
	SELECT count(distinct(franchisee2_party_id))
	  INTO ln_total_fran2_count
	  FROM SLC_ISP_FAS_SUPPIER_CNV_STG
	  WHERE request_id = gn_request_id
	   AND status = gv_processed_status
	   AND franchisee2_party_id IS NOT NULL;

	SELECT count(distinct(incorp_party_id))
	  INTO ln_total_corp_count
	  FROM SLC_ISP_FAS_SUPPIER_CNV_STG
	  WHERE request_id = gn_request_id
	   AND status = gv_processed_status
	   AND incorp_party_id IS NOT NULL;
	   */

	   SELECT COUNT(1)
	    INTO ln_total_fran_count
		FROM
		  (SELECT incorp_party_id
		  FROM SLC_ISP_FAS_SUPPIER_CNV_STG
		  WHERE request_id     = gn_request_id
		  AND status           = gv_processed_status
		  AND incorp_party_id IS NOT NULL
		  UNION
		  SELECT franchisee1_party_id
		  FROM SLC_ISP_FAS_SUPPIER_CNV_STG
		  WHERE request_id          = gn_request_id
		  AND status                = gv_processed_status
		  AND franchisee1_party_id IS NOT NULL
		  UNION
		  SELECT franchisee2_party_id
		  FROM SLC_ISP_FAS_SUPPIER_CNV_STG
		  WHERE request_id          = gn_request_id
		  AND status                = gv_processed_status
		  AND franchisee2_party_id IS NOT NULL
		  );
		  
	SELECT count(distinct(store_number))
	  INTO ln_total_site_count
	  FROM SLC_ISP_FAS_SUPPIER_CNV_STG
	  WHERE request_id = gn_request_id
	   AND status = gv_processed_status;	  	   
	   
	    /*
	    slc_write_log_p(gv_out,'ln_total_fran1_count:'||ln_total_fran1_count);
		slc_write_log_p(gv_out,'ln_total_fran2_count:'||ln_total_fran2_count);
		slc_write_log_p(gv_out,'ln_total_corp_count:'||ln_total_corp_count);
		slc_write_log_p(gv_out,'ln_total_fran_count:'||ln_total_fran_count);*/
		
		slc_write_log_p(gv_out,'No of Suppliers created:'||(ln_total_fran_count)
									||' No of Sites created:'||ln_total_success_count);
	   
		slc_write_log_p(gv_out,'Total records successfully imported:'||ln_total_success_count);
		slc_write_log_p(gv_out,'Total records which failed during import:'||ln_total_fail_count);
		slc_write_log_p(gv_out,'***************************************************');
		slc_write_log_p(gv_out,rpad('Record Id',25,' ')||'Error Message');
		OPEN cur_err_rec(gv_error_status);
		LOOP
		FETCH cur_err_rec INTO lc_cur_err_rec;
		EXIT WHEN cur_err_rec%NOTFOUND;
		slc_write_log_p(gv_out,rpad(lc_cur_err_rec.record_id,25,' ')||lc_cur_err_rec.error_msg);
     	slc_populate_err_object_p(p_in_batch_key => gv_batch_key
     			,p_in_business_entity => lv_business_entity_name
     			,p_in_process_id3 => NULL
     			,p_in_error_txt => lc_cur_err_rec.error_msg
     			,p_in_request_id => gn_request_id
     			,p_in_attribute1 => 'Record Id:'||lc_cur_err_rec.record_id
     			);		
		END LOOP;	
		CLOSE cur_err_rec;
		slc_write_log_p(gv_out,'***************************************************');	

		IF ln_total_fail_count = 0 THEN
			gn_program_status := 0;
		ELSIF ln_total_fail_count <> ln_total_count THEN
			gn_program_status := 1;
		ELSE
			gn_program_status := 2;
		END IF;		
	END IF;
	
   SLC_UTIL_JOBS_PKG.SLC_UTIL_E_LOG_SUMMARY_P(
							P_BATCH_KEY => gv_batch_key,
							P_BUSINESS_PROCESS_NAME => gv_business_process_name,
							P_TOTAL_RECORDS => ln_total_count,
							P_TOTAL_SUCCESS_RECORDS => ln_total_success_count,
							P_TOTAL_FAILCUSTVAL_RECORDS => ln_total_fail_count,
							P_TOTAL_FAILSTDVAL_RECORDS => NULL,
							p_batch_status  => lv_batch_status,
							p_publish_flag => lv_publish_flag,
							p_system_type => lv_system_type,
							p_source_system	=> lv_source,
							p_target_system => lv_destination,
							 P_REQUEST_ID => gn_request_id,
							 p_user_id => gn_user_id,
							 p_login_id => gn_login_id,
							 p_status_code  => lv_cmn_err_status_code
							);  

	SLC_UTIL_JOBS_PKG.slc_UTIL_log_errors_p(p_batch_key => gv_batch_key,
   					      p_business_process_name => gv_business_process_name,
   						  p_errors_rec => gv_cmn_err_rec,
   					      p_user_id => gn_user_id,
   					      p_login_id => gn_login_id,
   					      p_status_code  => lv_cmn_err_status_code
   					     );	
	END slc_print_summary_p;
/*	*****************************************************************
	NAME:              slc_create_supplier_p
	PURPOSE:           This procedure will create supplier in Supplier Hub
	Input Parameters:  p_in_vendor_name							IN VARCHAR2
							 p_in_vendor_name_att					IN VARCHAR2
							 p_in_segment1								IN VARCHAR2
							 p_in_vendor_type_lkp_code				IN VARCHAR2
							 p_in_term_name							IN VARCHAR2				
							 p_in_pay_date_basis_lookup_code		IN VARCHAR2
							 p_in_pay_group_lookup_code			IN VARCHAR2
							 p_in_invoice_currency_code			IN VARCHAR2
							 p_in_payment_currency_code			IN VARCHAR2
							 p_in_jgzz_fiscal_code				IN VARCHAR2
							 p_in_tax_reporting_name				IN VARCHAR2
*****************************************************************/	
	PROCEDURE slc_create_supplier_p( p_in_entity_name							IN VARCHAR2
									,p_in_vendor_name							IN VARCHAR2
									,p_in_vendor_name_alt					IN VARCHAR2
									,p_in_segment1								IN VARCHAR2 DEFAULT NULL
									,p_in_vendor_type_lkp_code				IN VARCHAR2
									,p_in_term_name							IN VARCHAR2				
									,p_in_pay_date_basis_code		IN VARCHAR2
									,p_in_pay_group_lookup_code			IN VARCHAR2
									,p_in_invoice_currency_code			IN VARCHAR2
									,p_in_payment_currency_code			IN VARCHAR2
									,p_in_jgzz_fiscal_code				IN VARCHAR2
							 		,p_in_tax_reporting_name				IN VARCHAR2
									,p_in_organization_type					IN VARCHAR2
							 		,p_out_party_id							OUT NUMBER
							 		,p_out_error_flag				   OUT VARCHAR2
							 		,p_out_err_msg							OUT VARCHAR2
							 		)
	IS
	lv_error_flag		VARCHAR2(1) DEFAULT 'N';
	lv_error_msg		VARCHAR2(4000) DEFAULT NULL;
	l_vendor_rec                  ap_vendor_pub_pkg.r_vendor_rec_type;
	l_msg_count			NUMBER;
	l_msg_data			VARCHAR2(4000);
	ln_vendor_id			NUMBER;
	ln_party_id			NUMBER;
    lv_msg                  VARCHAR2(4000);
    lv_msg_out  NUMBER;	
    l_return_status		VARCHAR2(10);
	BEGIN
	slc_write_log_p(gv_log,'In slc_create_supplier_p p_in_vendor_name:'||p_in_vendor_name||' p_in_segment1:'||p_in_segment1||
					  ' p_in_jgzz_fiscal_code:'||p_in_jgzz_fiscal_code||' p_in_vendor_name_alt:'||p_in_vendor_name_alt);

	-- If tax payer id value is not null then validate that it is unique amongst supplier 
	-- converted by Supplier conversion program.
	-- If tax payer id is not unique then mark status as error.
	IF p_in_jgzz_fiscal_code IS NOT NULL AND slc_get_taxpayer_id_count_f(p_in_jgzz_fiscal_code) > 0 THEN
		lv_error_flag := 'Y';
		lv_error_msg  := 'Tax Payer Id is not unique.';
	ELSE
	
		l_vendor_rec.vendor_name := p_in_vendor_name;
		l_vendor_rec.vendor_name_alt := p_in_vendor_name_alt;
		l_vendor_rec.segment1 := p_in_segment1;
		l_vendor_rec.vendor_type_lookup_code := p_in_vendor_type_lkp_code;
		l_vendor_rec.terms_name := p_in_term_name;
		l_vendor_rec.pay_date_basis_lookup_code := p_in_pay_date_basis_code;
		l_vendor_rec.pay_group_lookup_code := p_in_pay_group_lookup_code;
		l_vendor_rec.invoice_currency_code := p_in_invoice_currency_code;
		l_vendor_rec.payment_currency_code := p_in_payment_currency_code;
		l_vendor_rec.jgzz_fiscal_code := p_in_jgzz_fiscal_code;
		l_vendor_rec.tax_reporting_name := p_in_tax_reporting_name;
		l_vendor_rec.organization_type_lookup_code	:= p_in_organization_type;
		l_vendor_rec.start_date_active              := sysdate;
		l_vendor_rec.enabled_flag                   := 'Y';
		l_vendor_rec.ext_payee_rec.default_pmt_method := 'CHECK';
		l_vendor_rec.ext_payee_rec.exclusive_pay_flag   := 'N';
		
		
		FND_MSG_PUB.Initialize;
		pos_vendor_pub_pkg.create_vendor
			  (
				p_vendor_rec    => l_vendor_rec,
				x_return_status => l_return_status,
				x_msg_count     => l_msg_count,
				x_msg_data      => l_msg_data,
				x_vendor_id     => ln_vendor_id,
				x_party_id      => ln_party_id
		  );
		 slc_write_log_p(gv_log,'Test Begin l_return_status:'||l_return_status||' l_msg_count:'||l_msg_count||' l_msg_data:'||l_msg_data);
		IF l_return_status <> 'S' THEN
		lv_error_flag := 'Y';
			IF l_msg_count > 1 THEN
			   FOR i IN 1 .. l_msg_count
				LOOP
				  
				  FND_MSG_PUB.Get (p_msg_index       => i,
										 p_encoded         => 'F',
										 p_data            => lv_msg,
										 p_msg_index_OUT   => lv_msg_out);
				  lv_error_msg := lv_error_msg || ':' || lv_msg;
			   END LOOP;
			ELSE
				lv_error_msg := l_msg_data;
			END IF;
		END IF;
	END IF;

	p_out_party_id	:= ln_party_id;
	p_out_error_flag := lv_error_flag;
	IF lv_error_flag = 'Y' THEN
		p_out_err_msg	:= 'Error while creating Supplier for '||p_in_entity_name||' Error Message:'||lv_error_msg;
	END IF;
	slc_write_log_p(gv_log,'In slc_create_supplier_p ln_party_id:'||ln_party_id||' lv_error_flag:'||lv_error_flag||' lv_error_msg:'||lv_error_msg);
	EXCEPTION
	WHEN OTHERS THEN
	slc_write_log_p(gv_log,'Unexpected error in slc_create_supplier_p. Error Message:'||SQLERRM);
	p_out_party_id	:= NULL;
	p_out_error_flag := 'Y';
	p_out_err_msg	:= 'Unexpected error in slc_create_supplier_p. Error Message:'||SQLERRM;	
	
	END slc_create_supplier_p;

/* ****************************************************************
	NAME:              slc_update_org_profile_p
	PURPOSE:           This procedure will update org profile value
	Input Parameters	p_in_party_id	IN  NUMBER
						
*****************************************************************/
	PROCEDURE slc_update_org_profile_p(p_in_entity_name							IN VARCHAR2
								,p_in_party_id 	IN NUMBER
								,p_in_organization_name IN VARCHAR2
								,p_in_tax_payer_id		IN VARCHAR2
								,p_out_error_flag		OUT VARCHAR2
							 	,p_out_err_msg			OUT VARCHAR2
								)
	IS
		lv_organization_rec apps.hz_party_v2pub.organization_rec_type;
		lv_party_rec           hz_party_v2pub.party_rec_type;	
		ln_object_version_number		NUMBER;
		lv_error_flag		VARCHAR2(1) DEFAULT 'N';
		lv_error_msg		VARCHAR2(4000) DEFAULT NULL;
	
		  ln_profile_id NUMBER;
		  ln_return_status VARCHAR2(4000);
		  ln_msg_count NUMBER;
		  lv_msg_data VARCHAR2(4000);
		  lv_msg                  VARCHAR2(4000);
		  lv_msg_out  NUMBER; 
	BEGIN
	
	slc_write_log_p(gv_log,'In slc_update_org_profile_p p_in_party_id:'||p_in_party_id||
					 ' p_in_organization_name:'||p_in_organization_name);
	SELECT object_version_number
	  INTO ln_object_version_number
	 FROM hz_parties
	WHERE party_id = p_in_party_id;
	slc_write_log_p(gv_log,'In slc_update_org_profile_p ln_object_version_number:'||ln_object_version_number);
	
	lv_party_rec.party_id                          := p_in_party_id;
	lv_organization_rec.party_rec                  := lv_party_rec;
	
	lv_organization_rec.organization_name		   := p_in_organization_name;
	lv_organization_rec.jgzz_fiscal_code		   := p_in_tax_payer_id;

	-- If tax payer id value is not null then validate that it is unique amongst supplier 
	-- converted by Supplier conversion program.
	-- If tax payer id is not unique then mark status as error.
	IF p_in_tax_payer_id IS NOT NULL AND slc_get_taxpayer_id_count_f(p_in_tax_payer_id) > 0 THEN
		lv_error_flag := 'Y';
		lv_error_msg  := 'Tax Payer Id is not unique.';
	ELSE
	
		 HZ_PARTY_V2PUB.UPDATE_ORGANIZATION(
			P_INIT_MSG_LIST => FND_API.G_TRUE,
			P_ORGANIZATION_REC => lv_organization_rec,
			P_PARTY_OBJECT_VERSION_NUMBER => ln_object_version_number,
			X_PROFILE_ID => ln_profile_id,
			X_RETURN_STATUS => ln_return_status,
			X_MSG_COUNT => ln_msg_count,
			X_MSG_DATA => lv_msg_data
		  );	

		IF ln_return_status <> 'S' THEN
		lv_error_flag := 'Y';
		IF ln_msg_count > 1 THEN
			FOR i IN 1 .. ln_msg_count
			LOOP
			   
			   FND_MSG_PUB.Get (p_msg_index       => i,
								p_encoded         => 'F',
								p_data            => lv_msg,
								p_msg_index_OUT   => lv_msg_out);
			   lv_error_msg := lv_error_msg || ':' || lv_msg;
			END LOOP;
		ELSE 
			lv_error_msg := lv_msg_data;
		END IF;
		END IF;
	END IF; -- End of tax payer id else check.
	p_out_error_flag := lv_error_flag;
	IF lv_error_flag = 'Y' THEN
	p_out_err_msg	:= 'Error while updating Supplier for '||p_in_entity_name||' Error Message:'||lv_error_msg;
	END IF;
	slc_write_log_p(gv_log,'In slc_update_org_profile_p lv_error_flag:'||lv_error_flag||' lv_error_msg:'||lv_error_msg);
	
	EXCEPTION
	WHEN OTHERS THEN
	slc_write_log_p(gv_log,'Unexpected error in slc_update_org_profile_p. Error Message:'||SQLERRM);
	p_out_error_flag := 'Y';
	p_out_err_msg	:= 'Unexpected error in slc_update_org_profile_p. Error Message:'||SQLERRM;		
	END slc_update_org_profile_p;
	
/* ****************************************************************
	NAME:              slc_process_uda_attributes_p
	PURPOSE:           This procedure will update UDA information for Supplier Hub
	Input Parameters	p_in_ssn				IN 		NUMBER
						p_in_dob				IN 		VARCHAR2
						p_in_bkgrd				IN 		VARCHAR2
						p_in_marital			IN 		VARCHAR2
						p_in_sex				IN		VARCHAR2
						p_in_original_date		IN 		VARCHAR2
						p_in_effec_begin_date	IN 		VARCHAR2
						p_in_effec_end_date		IN 		VARCHAR2
*****************************************************************/	
	PROCEDURE	slc_process_uda_attributes_p(p_in_entity_name							IN VARCHAR2
						,p_in_party_id	IN 	NUMBER
						,p_in_ssn				IN 		VARCHAR2
						,p_in_dob				IN 		VARCHAR2
						,p_in_bkgrd				IN 		VARCHAR2
						,p_in_marital			IN 		VARCHAR2
						,p_in_sex				IN		VARCHAR2
						,p_in_conversion_source	IN 		VARCHAR2
						,p_in_original_date		IN 		VARCHAR2
						,p_in_effec_begin_date	IN 		VARCHAR2
						,p_in_effec_end_date		IN 		VARCHAR2
						,p_in_first_name		IN 		VARCHAR2
						,p_in_middle_name		IN 		VARCHAR2
						,p_in_last_name			IN 		VARCHAR2
						,p_in_zid				IN 		VARCHAR2
						,p_out_error_flag		OUT VARCHAR2
						,p_out_err_msg			OUT VARCHAR2
						)
	IS
      lv_api_version                   NUMBER                        := 1;
      lv_object_name                   VARCHAR2 (20)            := 'HZ_PARTIES';
      lv_attributes_row_table          ego_user_attr_row_table := ego_user_attr_row_table();
      lv_attributes_data_table         ego_user_attr_data_table := ego_user_attr_data_table (); 
      lv_pk_column_name_value_pairs    ego_col_name_value_pair_array := ego_col_name_value_pair_array ();
      lv_class_code_name_value_pairs   ego_col_name_value_pair_array := ego_col_name_value_pair_array ();
      lv_attr_grp_id                   NUMBER                        := 0;
      lv_group_app_id                  NUMBER                        := 0;
      lv_attr_group_type               VARCHAR2 (100)				 := 'POS_SUPP_PROFMGMT_GROUP';
      lv_attr_group_name               VARCHAR2 (100)				:= 'SLC_ISP_FRANCHISEE_DETAILS';
      lv_attr_group_disp_name          VARCHAR2 (250);
      lv_data_level                    VARCHAR2 (100)				:= 'SUPP_LEVEL';
      lv_classification_code            VARCHAR2 (100)				:=  'ST:FRANCHISEE';
	  lv_error_flag		VARCHAR2(1) DEFAULT 'N';
	  lv_error_msg		VARCHAR2(4000) DEFAULT NULL;	  
		  lv_msg                  VARCHAR2(4000);
		  lv_msg_out  NUMBER; 
		  
      ---Not initialized ones
      lv_entity_id                     NUMBER                        := NULL;
      lv_entity_index                  NUMBER                        := NULL;
      lv_entity_code                   VARCHAR2 (1)                  := NULL;
      lv_user_privileges_on_object     ego_varchar_tbl_type          := NULL;
      lv_debug_level                   NUMBER                        := 3;
      lv_init_error_handler            VARCHAR2 (1)         := fnd_api.g_true;
      lv_write_to_concurrent_log       VARCHAR2 (1)         := fnd_api.g_true;
      lv_init_fnd_msg_list             VARCHAR2 (1)         := fnd_api.g_true;
      lv_log_errors                    VARCHAR2 (1)         := fnd_api.g_true;
      lv_add_errors_to_fnd_stack       VARCHAR2 (1)        := fnd_api.g_true;
      lv_commit                        VARCHAR2 (1)        := fnd_api.g_false;
      x_failed_row_id_list             VARCHAR2 (255);
      x_errorcode                      NUMBER;
      x_message_list                   error_handler.error_tbl_type;
      pv_return_status_o               VARCHAR2 (100);
      pv_msg_count_o                   NUMBER;
      pv_msg_data_o                    VARCHAR2 (4000);
	  
	
	BEGIN
	slc_write_log_p(gv_log,'In slc_process_uda_attributes_p p_in_party_id:'||p_in_party_id||' p_in_ssn:'||p_in_ssn);

	
      BEGIN
         SELECT attr_group_id, application_id attr_group_app_id,
                attr_group_type, attr_group_name,
                attr_group_disp_name
           INTO lv_attr_grp_id, lv_group_app_id,
                lv_attr_group_type, lv_attr_group_name,
                lv_attr_group_disp_name
           FROM ego_attr_groups_v eagv
          WHERE eagv.attr_group_name = lv_attr_group_name
            AND eagv.attr_group_type = lv_attr_group_type;
      EXCEPTION
         WHEN OTHERS
         THEN
            lv_attr_grp_id := -1;
      END;
	  
      lv_attributes_row_table.EXTEND;
      lv_attributes_row_table (1) :=
         ego_user_attr_row_obj
                       (ego_import_row_seq_s.NEXTVAL         -- ROW_IDENTIFIER
                        ,lv_attr_grp_id -- ATTR_GROUP_ID from EGO_ATTR_GROUPS_V
                        ,lv_group_app_id                   -- ATTR_GROUP_APP_ID
                        ,lv_attr_group_type                  -- ATTR_GROUP_TYPE
                        ,lv_attr_group_name                  -- ATTR_GROUP_NAME
                        ,lv_data_level                           -- NDATA_LEVEL
                        ,'N'                                   -- DATA_LEVEL_1
                        ,NULL                                   -- DATA_LEVEL_2
                        ,NULL                                   -- DATA_LEVEL_3
                        ,NULL                                   -- DATA_LEVEL_4
                        ,NULL                                   -- DATA_LEVEL_5
                        ,ego_user_attrs_data_pvt.g_sync_mode--g_create_mode
                       );

	  lv_attributes_data_table.EXTEND;
      lv_attributes_data_table (1) :=
         ego_user_attr_data_obj
                    (ego_import_row_seq_s.CURRVAL -- ROW_IDENTIFIER from above
                     ,'SLC_ISP_FRANCHISEE_SSN2'                   -- ATTR_NAME
                     ,p_in_ssn                                    -- ATTR_VALUE_STR
                     ,NULL                              -- ATTR_VALUE_NUM
                     ,NULL								-- ATTR_VALUE_DATE
                     ,NULL                                   -- ATTR_DISP_VALUE
                     ,NULL                              -- ATTR_UNIT_OF_MEASURE
                     ,ego_import_row_seq_s.CURRVAL      -- USER_ROW_IDENTIFIER
                    ); 

	  lv_attributes_data_table.EXTEND;
      lv_attributes_data_table (2) :=
         ego_user_attr_data_obj
                    (ego_import_row_seq_s.CURRVAL -- ROW_IDENTIFIER from above
                     ,'SLC_ISP_FRANCHISEE_DOB'                   -- ATTR_NAME
                     ,NULL                                    -- ATTR_VALUE_STR
                     ,NULL                              -- ATTR_VALUE_NUM
                     --,p_in_dob								-- ATTR_VALUE_DATE
					 ,slc_get_transformed_date_p(p_in_dob)-- ATTR_VALUE_DATE
                     ,NULL                                   -- ATTR_DISP_VALUE
                     ,NULL                              -- ATTR_UNIT_OF_MEASURE
                     ,ego_import_row_seq_s.CURRVAL      -- USER_ROW_IDENTIFIER
                    );
					
		lv_attributes_data_table.EXTEND;			
        lv_attributes_data_table (3) :=
         ego_user_attr_data_obj
                    (ego_import_row_seq_s.CURRVAL -- ROW_IDENTIFIER from above
                     ,'SLC_ISP_FRANCHISEE_Ethnicity'                   -- ATTR_NAME
					 --Changes for v1.5. Changed the valueset name
                     --,slc_get_lookup_meaning_p('SLCISP_BACKGROUND_LKP',p_in_bkgrd)     -- ATTR_VALUE_STR
					 ,slc_get_lookup_meaning_p('SLCISP_BACKGROUND',p_in_bkgrd)
					 ,NULL                              -- ATTR_VALUE_NUM
                     ,NULL								-- ATTR_VALUE_DATE
                     ,NULL                                   -- ATTR_DISP_VALUE
                     ,NULL                              -- ATTR_UNIT_OF_MEASURE
                     ,ego_import_row_seq_s.CURRVAL      -- USER_ROW_IDENTIFIER
                    ); 

		lv_attributes_data_table.EXTEND;			
        lv_attributes_data_table (4) :=
         ego_user_attr_data_obj
                    (ego_import_row_seq_s.CURRVAL -- ROW_IDENTIFIER from above
                     ,'SLC_ISP_FRANCHISEE_MT'                   -- ATTR_NAME
					 --Changes for v1.5. Changed the valueset name
                     --,slc_get_lookup_meaning_p('SLCISP_MARITAL_LKP',p_in_marital) -- ATTR_VALUE_STR
					 ,slc_get_lookup_meaning_p('SLCISP_MARITAL_STATUS',p_in_marital) -- ATTR_VALUE_STR
                     ,NULL                              -- ATTR_VALUE_NUM
                     ,NULL								-- ATTR_VALUE_DATE
                     ,NULL                                   -- ATTR_DISP_VALUE
                     ,NULL                              -- ATTR_UNIT_OF_MEASURE
                     ,ego_import_row_seq_s.CURRVAL      -- USER_ROW_IDENTIFIER
                    ); 
					
		lv_attributes_data_table.EXTEND;			
        lv_attributes_data_table (5) :=
         ego_user_attr_data_obj
                    (ego_import_row_seq_s.CURRVAL -- ROW_IDENTIFIER from above
                     ,'SLC_ISP_FRANCHISEE_SEX'                   -- ATTR_NAME
					 --Changes for v1.5. Changed the valueset name
                     --,slc_get_lookup_meaning_p('SLCISP_SEX_LKP',p_in_sex) -- ATTR_VALUE_STR
					 ,slc_get_lookup_meaning_p('SLCISP_GENDER',p_in_sex) -- ATTR_VALUE_STR
                     ,NULL                              -- ATTR_VALUE_NUM
                     ,NULL								-- ATTR_VALUE_DATE
                     ,NULL                                   -- ATTR_DISP_VALUE
                     ,NULL                              -- ATTR_UNIT_OF_MEASURE
                     ,ego_import_row_seq_s.CURRVAL      -- USER_ROW_IDENTIFIER
                    ); 	

		lv_attributes_data_table.EXTEND;			
        lv_attributes_data_table (6) :=
         ego_user_attr_data_obj
                    (ego_import_row_seq_s.CURRVAL -- ROW_IDENTIFIER from above
                     ,'SLC_ISP_FRANCHISEE_CO'                   -- ATTR_NAME
                     ,NULL                       -- ATTR_VALUE_STR
                     ,NULL                              -- ATTR_VALUE_NUM
                     --,p_in_original_date				-- ATTR_VALUE_DATE
					 ,slc_get_transformed_date_p(p_in_original_date)-- ATTR_VALUE_DATE
                     ,NULL                                   -- ATTR_DISP_VALUE
                     ,NULL                              -- ATTR_UNIT_OF_MEASURE
                     ,ego_import_row_seq_s.CURRVAL      -- USER_ROW_IDENTIFIER
                    ); 	
		
		--Set the conversion source name as FAS
		lv_attributes_data_table.EXTEND;			
        lv_attributes_data_table (7) :=
         ego_user_attr_data_obj
                    (ego_import_row_seq_s.CURRVAL -- ROW_IDENTIFIER from above
                     ,'SLC_ISP_FRANCHISEE_CONVERSION'                   -- ATTR_NAME
                     ,p_in_conversion_source                       -- ATTR_VALUE_STR
                     ,NULL                              -- ATTR_VALUE_NUM
                     ,NULL								-- ATTR_VALUE_DATE
                     ,NULL                                   -- ATTR_DISP_VALUE
                     ,NULL                              -- ATTR_UNIT_OF_MEASURE
                     ,ego_import_row_seq_s.CURRVAL      -- USER_ROW_IDENTIFIER
                    ); 	
		
		--Changes for v1.1.
		-- New UDA columns First Name , Middle Name and Last Name has been added as per version 1.6 in CV040.
		lv_attributes_data_table.EXTEND;			
        lv_attributes_data_table (8) :=
         ego_user_attr_data_obj
                    (ego_import_row_seq_s.CURRVAL -- ROW_IDENTIFIER from above
                     ,'SLC_ISP_FRANCHISEE_FIRST_NAME'                   -- ATTR_NAME
                     ,p_in_first_name                       -- ATTR_VALUE_STR
                     ,NULL                              -- ATTR_VALUE_NUM
                     ,NULL								-- ATTR_VALUE_DATE
                     ,NULL                                   -- ATTR_DISP_VALUE
                     ,NULL                              -- ATTR_UNIT_OF_MEASURE
                     ,ego_import_row_seq_s.CURRVAL      -- USER_ROW_IDENTIFIER
                    ); 		

		lv_attributes_data_table.EXTEND;			
        lv_attributes_data_table (9) :=
         ego_user_attr_data_obj
                    (ego_import_row_seq_s.CURRVAL -- ROW_IDENTIFIER from above
                     ,'SLC_ISP_FRANCHISEE_MIDDLE_NAME'                   -- ATTR_NAME
                     ,p_in_middle_name                       -- ATTR_VALUE_STR
                     ,NULL                              -- ATTR_VALUE_NUM
                     ,NULL								-- ATTR_VALUE_DATE
                     ,NULL                                   -- ATTR_DISP_VALUE
                     ,NULL                              -- ATTR_UNIT_OF_MEASURE
                     ,ego_import_row_seq_s.CURRVAL      -- USER_ROW_IDENTIFIER
                    ); 		

		lv_attributes_data_table.EXTEND;			
        lv_attributes_data_table (10) :=
         ego_user_attr_data_obj
                    (ego_import_row_seq_s.CURRVAL -- ROW_IDENTIFIER from above
                     ,'SLC_ISP_FRANCHISEE_LAST_NAME'                   -- ATTR_NAME
                     ,p_in_last_name                       -- ATTR_VALUE_STR
                     ,NULL                              -- ATTR_VALUE_NUM
                     ,NULL								-- ATTR_VALUE_DATE
                     ,NULL                                   -- ATTR_DISP_VALUE
                     ,NULL                              -- ATTR_UNIT_OF_MEASURE
                     ,ego_import_row_seq_s.CURRVAL      -- USER_ROW_IDENTIFIER
                    ); 

		--Changes for v1.2 BEGIN
		--Added ZID in UDA
		lv_attributes_data_table.EXTEND;			
        lv_attributes_data_table (11) :=
         ego_user_attr_data_obj
                    (ego_import_row_seq_s.CURRVAL -- ROW_IDENTIFIER from above
                     ,'SLC_ISP_FRANCHISEE_ZID'                   -- ATTR_NAME
                     ,p_in_zid                         -- ATTR_VALUE_STR
                     ,NULL                              -- ATTR_VALUE_NUM
                     ,NULL								-- ATTR_VALUE_DATE
                     ,NULL                                   -- ATTR_DISP_VALUE
                     ,NULL                              -- ATTR_UNIT_OF_MEASURE
                     ,ego_import_row_seq_s.CURRVAL      -- USER_ROW_IDENTIFIER
                    ); 			
		
		--Changes for v1.2 End

      lv_pk_column_name_value_pairs.EXTEND;
      lv_pk_column_name_value_pairs (1) := ego_col_name_value_pair_obj ('PARTY_ID',p_in_party_id );
                            
      lv_class_code_name_value_pairs.EXTEND (1);
      lv_class_code_name_value_pairs (1) := ego_col_name_value_pair_obj ('CLASSIFICATION_CODE', lv_classification_code);
	  
	  FND_MSG_PUB.Initialize;
	  ego_user_attrs_data_pub.process_user_attrs_data
                                              (lv_api_version,
                                               lv_object_name,
                                               lv_attributes_row_table,
                                               lv_attributes_data_table,
                                               lv_pk_column_name_value_pairs,
                                               lv_class_code_name_value_pairs,
                                               lv_user_privileges_on_object,
                                               lv_entity_id,
                                               lv_entity_index,
                                               lv_entity_code,
                                               lv_debug_level,
                                               lv_init_error_handler,
                                               lv_write_to_concurrent_log,
                                               lv_init_fnd_msg_list,
                                               lv_log_errors,
                                               lv_add_errors_to_fnd_stack,
                                               lv_commit,
                                               x_failed_row_id_list,
                                               pv_return_status_o,
                                               x_errorcode,
                                               pv_msg_count_o,
                                               pv_msg_data_o
                                              );
											  
	FOR i IN 1 .. pv_msg_count_o
	LOOP
	   lv_error_flag := 'Y';
	   FND_MSG_PUB.Get (p_msg_index       => i,
						p_encoded         => 'F',
						p_data            => lv_msg,
						p_msg_index_OUT   => lv_msg_out);
	   lv_error_msg := lv_error_msg || ':' || lv_msg;
	END LOOP;
	p_out_error_flag := lv_error_flag;
	IF lv_error_flag = 'Y' THEN
		p_out_err_msg	:= 'Error while updating UDA information for '||p_in_entity_name||' Error Message:'||lv_error_msg;
	END IF;
	slc_write_log_p(gv_log,'In slc_process_uda_attributes_p lv_error_flag:'||lv_error_flag||' lv_error_msg:'||lv_error_msg);
	
	EXCEPTION
	WHEN OTHERS THEN
	slc_write_log_p(gv_log,'Unexpected error in slc_process_uda_attributes_p. Error Message:'||SQLERRM);
	p_out_error_flag := 'Y';
	p_out_err_msg	:= 'Unexpected error in slc_process_uda_attributes_p. Error Message:'||SQLERRM;		
	END slc_process_uda_attributes_p;	

/* ****************************************************************
	NAME:              slc_create_supplier_site_p
	PURPOSE:           This procedure will create Supplier Site
	Input Parameters	p_in_party_id				IN 		NUMBER
						p_in_vendor_site_code		IN 		VARCHAR2
						p_in_addressline1			IN 		VARCHAR2
						p_in_addressline2			IN 		VARCHAR2
						p_in_city					IN 		VARCHAR2
						p_in_state					IN 		VARCHAR2
						p_in_county					IN 		VARCHAR2
						p_in_country				IN 		VARCHAR2
						p_in_zip					IN 		VARCHAR2
						p_in_phone					IN 		VARCHAR2
						p_in_ou						IN 		VARCHAR2
*****************************************************************/
	PROCEDURE slc_create_supplier_site_p(p_in_party_id				IN 		NUMBER
									,p_in_vendor_site_code		IN 		VARCHAR2
									,p_in_pay_group_lookup_code IN 		VARCHAR2
									,p_in_addressline1			IN 		VARCHAR2
									,p_in_addressline2			IN 		VARCHAR2	DEFAULT NULL
									,p_in_city					IN 		VARCHAR2	DEFAULT NULL
									,p_in_state					IN 		VARCHAR2	DEFAULT NULL
									,p_in_county					IN 		VARCHAR2	DEFAULT NULL
									,p_in_country				IN 		VARCHAR2	DEFAULT NULL
									,p_in_zip					IN 		VARCHAR2	DEFAULT NULL
									,p_in_phone					IN 		VARCHAR2	DEFAULT NULL
									,p_in_ou						IN 		VARCHAR2
									,p_out_error_flag		OUT VARCHAR2
									,p_out_err_msg			OUT VARCHAR2									
						)
	IS
	lv_error_flag		VARCHAR2(1) DEFAULT 'N';
	lv_error_msg		VARCHAR2(4000) DEFAULT NULL;
	l_vendor_site_rec             ap_vendor_pub_pkg.r_vendor_site_rec_type;	
	l_party_site_rec 			APPS.HZ_PARTY_SITE_V2PUB.PARTY_SITE_REC_TYPE;
	ln_msg_count             NUMBER;	
	lv_msg                  VARCHAR2(4000);
	lv_msg_out  NUMBER;
	ln_vendor_id			NUMBER;
 	ln_vendor_site_id        NUMBER;
    ln_party_site_id         NUMBER;
	lv_msg_data              VARCHAR2(1000);
	lv_return_status         VARCHAR2(10);
	ln_object_version_number	NUMBER;
	ln_location_id					AP_SUPPLIER_SITES_ALL.location_id%TYPE;
	
	CURSOR cur_supplier_loc(p_in_vendor_id IN NUMBER)
	IS
	SELECT location_id
	FROM AP_SUPPLIER_SITES_ALL
	WHERE vendor_id = p_in_vendor_id;
	BEGIN
	
		/* If for same Supplier multiple stores are present with different address then 
		 * We will be using same location and creating new Supplier Site using same location. 
		 */
		SELECT sup.vendor_id  
		  INTO ln_vendor_id 
		 FROM ap_suppliers sup
		WHERE party_id = p_in_party_id;
		slc_write_log_p(gv_log,'In slc_create_supplier_site_p p_in_party_id:'||p_in_party_id||' ln_vendor_id:'||ln_vendor_id);

		OPEN cur_supplier_loc(ln_vendor_id);
		FETCH cur_supplier_loc INTO ln_location_id;
		CLOSE cur_supplier_loc;
		slc_write_log_p(gv_log,'In slc_create_supplier_site_p ln_location_id:'||ln_location_id);
		
        l_vendor_site_rec.vendor_id           := ln_vendor_id;
        l_vendor_site_rec.vendor_site_code    := p_in_vendor_site_code;
		IF ln_location_id IS NULL THEN
			l_vendor_site_rec.address_line1       := p_in_addressline1;
			l_vendor_site_rec.address_line2       := p_in_addressline2;   
			l_vendor_site_rec.city                := p_in_city;  
			l_vendor_site_rec.state               := p_in_state	;  
			l_vendor_site_rec.country             := p_in_country;
			l_vendor_site_rec.county             := p_in_county;
			l_vendor_site_rec.zip                 := p_in_zip;
			l_vendor_site_rec.PHONE               := p_in_phone;
		ELSIF ln_location_id IS NOT NULL THEN
			l_vendor_site_rec.LOCATION_ID		  := ln_location_id;
			l_vendor_site_rec.PHONE               := p_in_phone;
		END IF;
        
        l_vendor_site_rec.org_name            := p_in_ou;        
        l_vendor_site_rec.pay_site_flag         := 'Y';
		l_vendor_site_rec.party_site_name		:= 'Home';
		l_vendor_site_rec.pay_group_lookup_code	:= p_in_pay_group_lookup_code;
			
	   FND_MSG_PUB.Initialize;
	   pos_vendor_pub_pkg.create_vendor_site
        (
          p_vendor_site_rec   => l_vendor_site_rec,
          x_return_status     => lv_return_status,
          x_msg_count         => ln_msg_count,
          x_msg_data          => lv_msg_data,
          x_vendor_site_id    => ln_vendor_site_id,
          x_party_site_id     => ln_party_site_id,
          x_location_id       => ln_location_id
        );
		
		slc_write_log_p(gv_log,'In slc_create_supplier_site_p lv_return_status:'||lv_return_status||' ln_party_site_id:'||ln_party_site_id);
		IF lv_return_status <> 'S' THEN
		lv_error_flag := 'Y';
		IF ln_msg_count > 1 THEN
			FOR i IN 1 .. ln_msg_count
			LOOP
			   
			   FND_MSG_PUB.Get (p_msg_index       => i,
								p_encoded         => 'F',
								p_data            => lv_msg,
								p_msg_index_OUT   => lv_msg_out);
			   lv_error_msg := lv_error_msg || ':' || lv_msg;
			END LOOP;
		ELSE
			lv_error_msg := lv_msg_data;
		END IF;
		END IF;
	p_out_error_flag := lv_error_flag;
	IF lv_error_flag = 'Y' THEN
		p_out_err_msg	:= 'Error while creating Supplier Site. Error Message:'||lv_error_msg;
	END IF;
	slc_write_log_p(gv_log,'In slc_create_supplier_site_p lv_error_flag:'||lv_error_flag||' lv_error_msg:'||lv_error_msg);
	EXCEPTION
	WHEN OTHERS THEN
	slc_write_log_p(gv_log,'Unexpected error in slc_create_supplier_site_p. Error Message:'||SQLERRM);
	p_out_error_flag := 'Y';
	p_out_err_msg	:= 'Unexpected error in slc_create_supplier_site_p. Error Message:'||SQLERRM;		
	END slc_create_supplier_site_p;	
	
/* ****************************************************************
	NAME:              slc_validate_p
	PURPOSE:           This procedure will be used to validate records fetched from FAS
							 before processing
*****************************************************************/
	PROCEDURE slc_validate_p
	IS
	BEGIN
		slc_write_log_p(gv_log,'In slc_validate_p gn_batch_id:'||gn_batch_id);
	
		--Reset all the error messages.
		UPDATE SLC_ISP_FAS_SUPPIER_CNV_STG
		  SET ERROR_MSG = NULL
			,request_id = gn_request_id
			,last_update_date = sysdate
			,last_updated_by = gn_user_id
			,last_update_login = gn_login_id
		WHERE batch_id = gn_batch_id;
		
		--Validate that if First name and last name for Franchisee1 cannot be null.
		--If all 3 are null then error the record.
		UPDATE SLC_ISP_FAS_SUPPIER_CNV_STG
		  SET ERROR_MSG = 'Franchisee1 First_Name and Last_Name is null'
		WHERE batch_id = gn_batch_id
		  AND (FRANCHISEE1_FIRST_NAME IS NULL AND FRANCHISEE1_LAST_NAME IS NULL);
		
		--SSN1 for Franchisee1 cannot be null.
		UPDATE SLC_ISP_FAS_SUPPIER_CNV_STG
		  SET ERROR_MSG = ERROR_MSG||'~SSN for Franchisee1 is mandatory'
		WHERE batch_id = gn_batch_id
		  AND FRANCHISEE1_SSN IS NULL;
		  
		--If INCORP flag is Y i.e if it an incorporation then Federal Id and Corporation name is mandatory  
		UPDATE SLC_ISP_FAS_SUPPIER_CNV_STG
		  SET ERROR_MSG = ERROR_MSG||'~For Corporation Federal ID and Corporation Name is mandatory'
		WHERE batch_id = gn_batch_id
		  AND record_type = 'CURRENT'
		  AND (INCORP_FLAG = 'Y' AND (FEDERAL_ID IS NULL OR INCORP_NAME IS NULL)
		  		);
		
		--If Franchisee2 is present then SSN for Franchisee2 cannot be null.
		UPDATE SLC_ISP_FAS_SUPPIER_CNV_STG
		  SET ERROR_MSG = ERROR_MSG||'~SSN for Franchisee2 is mandatory'
		WHERE batch_id = gn_batch_id
		  AND record_type = 'CURRENT'
		  AND ((FRANCHISEE2_FIRST_NAME IS NOT NULL OR FRANCHISEE2_MIDDLE_NAME IS NOT NULL OR FRANCHISEE2_LAST_NAME IS NOT NULL)
		  			AND FRANCHISEE2_SSN IS NULL
		  		);

		--If SSN for Franchisee2 is not null then Franchisee2 First Name and Franchisee2 Last Name is null.
		UPDATE SLC_ISP_FAS_SUPPIER_CNV_STG
		  SET ERROR_MSG = ERROR_MSG||'~First Name and Last Name is null for Franchisee2'
		WHERE batch_id = gn_batch_id
		  AND record_type = 'CURRENT'
		  AND (FRANCHISEE2_SSN IS NOT NULL AND (FRANCHISEE2_FIRST_NAME IS NULL AND FRANCHISEE2_LAST_NAME IS NULL));
				
		--If any one of the field AddressLine1 , City , State or Zip is null then we need to mark it as validation failure.
		UPDATE SLC_ISP_FAS_SUPPIER_CNV_STG
		  SET ERROR_MSG = ERROR_MSG||'~All address fields ADDRESS1,City,State,Zip is mandatory.'
		WHERE batch_id = gn_batch_id
		  AND record_type = 'CURRENT'
		  AND (ADDRESS1 IS NULL OR CITY IS NULL OR ZIP IS NULL OR STATE IS NULL);	

		--Validate data for Background , Sex and Ethnicity
		UPDATE SLC_ISP_FAS_SUPPIER_CNV_STG stg
		  SET ERROR_MSG = ERROR_MSG||'~Background value for Franchisee1 is not valid.'
		WHERE batch_id = gn_batch_id
		  AND stg.franchisee1_bkgrd IS NOT NULL
		  --Changes for v1.5 
		  --Changed code to refer to valueset instead of lookup
		  AND NOT EXISTS(SELECT 1
							FROM fnd_flex_value_sets ffvs ,
							  fnd_flex_values ffv ,
							  fnd_flex_values_tl ffvt
							WHERE ffvs.flex_value_set_name = 'SLCISP_BACKGROUND'
							AND ffvs.flex_value_set_id      = ffv.flex_value_set_id
							AND ffv.enabled_flag            = 'Y'
							AND TRUNC(SYSDATE) BETWEEN TRUNC(NVL(START_DATE_ACTIVE,SYSDATE)) AND TRUNC(NVL(END_DATE_ACTIVE,SYSDATE))
							AND ffvt.description = stg.franchisee1_bkgrd
							AND ffv.flex_value_id = ffvt.flex_value_id
					    );

		UPDATE SLC_ISP_FAS_SUPPIER_CNV_STG stg
		  SET ERROR_MSG = ERROR_MSG||'~Background value for Franchisee2 is not valid.'
		WHERE batch_id = gn_batch_id
		  AND stg.franchisee2_bkgrd IS NOT NULL
		  --Changes for v1.5 
		  --Changed code to refer to valueset instead of lookup		  
		  AND NOT EXISTS(SELECT 1
							FROM fnd_flex_value_sets ffvs ,
							  fnd_flex_values ffv ,
							  fnd_flex_values_tl ffvt
							WHERE ffvs.flex_value_set_name = 'SLCISP_BACKGROUND'
							AND ffvs.flex_value_set_id      = ffv.flex_value_set_id
							AND ffv.enabled_flag            = 'Y'
							AND TRUNC(SYSDATE) BETWEEN TRUNC(NVL(START_DATE_ACTIVE,SYSDATE)) AND TRUNC(NVL(END_DATE_ACTIVE,SYSDATE))
							AND ffvt.description = stg.franchisee2_bkgrd
							AND ffv.flex_value_id = ffvt.flex_value_id
					    );		

		UPDATE SLC_ISP_FAS_SUPPIER_CNV_STG stg
		  SET ERROR_MSG = ERROR_MSG||'~Sex value for Franchisee1 is not valid.'
		WHERE batch_id = gn_batch_id
		  AND stg.franchisee1_sex IS NOT NULL
		  --Changes for v1.5 
		  --Changed code to refer to valueset instead of lookup			  
		  AND NOT EXISTS(SELECT 1
							FROM fnd_flex_value_sets ffvs ,
							  fnd_flex_values ffv ,
							  fnd_flex_values_tl ffvt
							WHERE ffvs.flex_value_set_name = 'SLCISP_GENDER'
							AND ffvs.flex_value_set_id      = ffv.flex_value_set_id
							AND ffv.enabled_flag            = 'Y'
							AND TRUNC(SYSDATE) BETWEEN TRUNC(NVL(START_DATE_ACTIVE,SYSDATE)) AND TRUNC(NVL(END_DATE_ACTIVE,SYSDATE))
							AND ffvt.description = stg.franchisee1_sex
							AND ffv.flex_value_id = ffvt.flex_value_id
					    );

		UPDATE SLC_ISP_FAS_SUPPIER_CNV_STG stg
		  SET ERROR_MSG = ERROR_MSG||'~Sex value for Franchisee2 is not valid.'
		WHERE batch_id = gn_batch_id
		  AND stg.franchisee2_sex IS NOT NULL
		  --Changes for v1.5 
		  --Changed code to refer to valueset instead of lookup				  
		  AND NOT EXISTS(SELECT 1
							FROM fnd_flex_value_sets ffvs ,
							  fnd_flex_values ffv ,
							  fnd_flex_values_tl ffvt
							WHERE ffvs.flex_value_set_name = 'SLCISP_GENDER'
							AND ffvs.flex_value_set_id      = ffv.flex_value_set_id
							AND ffv.enabled_flag            = 'Y'
							AND TRUNC(SYSDATE) BETWEEN TRUNC(NVL(START_DATE_ACTIVE,SYSDATE)) AND TRUNC(NVL(END_DATE_ACTIVE,SYSDATE))
							AND ffvt.description = stg.franchisee2_sex
							AND ffv.flex_value_id = ffvt.flex_value_id
					    );

		UPDATE SLC_ISP_FAS_SUPPIER_CNV_STG stg
		  SET ERROR_MSG = ERROR_MSG||'~Marital value for Franchisee1 is not valid.'
		WHERE batch_id = gn_batch_id
		  AND stg.franchisee1_marital IS NOT NULL
		  --Changes for v1.5 
		  --Changed code to refer to valueset instead of lookup			  
		  AND NOT EXISTS(SELECT 1
							FROM fnd_flex_value_sets ffvs ,
							  fnd_flex_values ffv ,
							  fnd_flex_values_tl ffvt
							WHERE ffvs.flex_value_set_name = 'SLCISP_MARITAL_STATUS'
							AND ffvs.flex_value_set_id      = ffv.flex_value_set_id
							AND ffv.enabled_flag            = 'Y'
							AND TRUNC(SYSDATE) BETWEEN TRUNC(NVL(START_DATE_ACTIVE,SYSDATE)) AND TRUNC(NVL(END_DATE_ACTIVE,SYSDATE))
							AND ffvt.description = DECODE(stg.franchisee1_marital,'A','M',stg.franchisee1_marital)
							AND ffv.flex_value_id = ffvt.flex_value_id
					    );

		UPDATE SLC_ISP_FAS_SUPPIER_CNV_STG stg
		  SET ERROR_MSG = ERROR_MSG||'~Marital value for Franchisee2 is not valid.'
		WHERE batch_id = gn_batch_id
		  AND stg.franchisee2_marital IS NOT NULL
		  --Changes for v1.5 
		  --Changed code to refer to valueset instead of lookup					  
		  AND NOT EXISTS(SELECT 1
							FROM fnd_flex_value_sets ffvs ,
							  fnd_flex_values ffv ,
							  fnd_flex_values_tl ffvt
							WHERE ffvs.flex_value_set_name = 'SLCISP_MARITAL_STATUS'
							AND ffvs.flex_value_set_id      = ffv.flex_value_set_id
							AND ffv.enabled_flag            = 'Y'
							AND TRUNC(SYSDATE) BETWEEN TRUNC(NVL(START_DATE_ACTIVE,SYSDATE)) AND TRUNC(NVL(END_DATE_ACTIVE,SYSDATE))
							AND ffvt.description = DECODE(stg.franchisee2_marital,'A','M',stg.franchisee2_marital)
							AND ffv.flex_value_id = ffvt.flex_value_id
					    );						
						
						
						
		--Validate date formats.
		UPDATE SLC_ISP_FAS_SUPPIER_CNV_STG
		   SET FRANCHISEE1_BIRTH_DATE = NULL
		WHERE batch_id = gn_batch_id
		  AND FRANCHISEE1_BIRTH_DATE = '000000';

		UPDATE SLC_ISP_FAS_SUPPIER_CNV_STG
		   SET FRANCHISEE2_BIRTH_DATE = NULL
		WHERE batch_id = gn_batch_id
		  AND FRANCHISEE2_BIRTH_DATE = '000000';
		
		UPDATE SLC_ISP_FAS_SUPPIER_CNV_STG 
		   SET FRANCHISEE1_ORIGINAL_DATE = NULL
		WHERE batch_id = gn_batch_id
		  AND FRANCHISEE1_ORIGINAL_DATE = '000000';

		UPDATE SLC_ISP_FAS_SUPPIER_CNV_STG
		   SET FRANCHISEE2_ORIGINAL_DATE = NULL
		WHERE batch_id = gn_batch_id
		  AND FRANCHISEE2_ORIGINAL_DATE = '000000';

		UPDATE SLC_ISP_FAS_SUPPIER_CNV_STG
		   SET EFFECTIVE_BEGIN_DATE = NULL
		WHERE batch_id = gn_batch_id
		  AND EFFECTIVE_BEGIN_DATE = '000000';

		UPDATE SLC_ISP_FAS_SUPPIER_CNV_STG
		   SET EFFECTIVE_END_DATE = NULL
		WHERE batch_id = gn_batch_id
		  AND EFFECTIVE_END_DATE = '000000';

		UPDATE SLC_ISP_FAS_SUPPIER_CNV_STG
		   SET FRANCHISEE1_SSN = LPAD(FRANCHISEE1_SSN,9,'0')
		 WHERE batch_id = gn_batch_id
		   AND FRANCHISEE1_SSN IS NOT NULL;
		   
		UPDATE SLC_ISP_FAS_SUPPIER_CNV_STG
		   SET FRANCHISEE2_SSN = LPAD(FRANCHISEE2_SSN,9,'0')
		 WHERE batch_id = gn_batch_id
		   AND FRANCHISEE2_SSN IS NOT NULL;		   

		--Pick all records for which FRANCHISEE1_BIRTH_DATE is not null and validate date format.
		UPDATE SLC_ISP_FAS_SUPPIER_CNV_STG
		   SET ERROR_MSG = DECODE(slc_is_date_valid_f(FRANCHISEE1_BIRTH_DATE),
							'N', ERROR_MSG||'~Franchisee1 Birth Date:Invalid date format',ERROR_MSG )   
		WHERE batch_id = gn_batch_id
		  AND FRANCHISEE1_BIRTH_DATE IS NOT NULL;	

		--Pick all records for which FRANCHISEE2_BIRTH_DATE is not null and validate date format.
		UPDATE SLC_ISP_FAS_SUPPIER_CNV_STG
		   SET ERROR_MSG = DECODE(slc_is_date_valid_f(FRANCHISEE2_BIRTH_DATE),
							'N', ERROR_MSG||'~Franchisee2 Birth Date:Invalid date format',ERROR_MSG )   
		WHERE batch_id = gn_batch_id
		  AND FRANCHISEE2_BIRTH_DATE IS NOT NULL;	  

		--Pick all records for which FRANCHISEE1_ORIGINAL_DATE is not null and validate date format.
		UPDATE SLC_ISP_FAS_SUPPIER_CNV_STG
		   SET ERROR_MSG = DECODE(slc_is_date_valid_f(FRANCHISEE1_ORIGINAL_DATE)
		   ,'N', ERROR_MSG||'~Franchisee1 Original Date:Invalid date format',ERROR_MSG )   
		WHERE batch_id = gn_batch_id
		  AND FRANCHISEE1_ORIGINAL_DATE IS NOT NULL;	 

		--Pick all records for which FRANCHISEE2_ORIGINAL_DATE is not null and validate date format.
		UPDATE SLC_ISP_FAS_SUPPIER_CNV_STG
		   SET ERROR_MSG = DECODE(slc_is_date_valid_f(FRANCHISEE2_ORIGINAL_DATE)
					,'N', ERROR_MSG||'~Franchisee2 Original Date:Invalid date format',ERROR_MSG )   
		WHERE batch_id = gn_batch_id
		  AND FRANCHISEE2_ORIGINAL_DATE IS NOT NULL;	

		--Pick all records for which EFFECTIVE_BEGIN_DATE is not null and validate date format.
		UPDATE SLC_ISP_FAS_SUPPIER_CNV_STG
		   SET ERROR_MSG = DECODE(slc_is_date_valid_f(EFFECTIVE_BEGIN_DATE)
				,'N', ERROR_MSG||'~Effective Begin Date:Invalid date format',ERROR_MSG )   
		WHERE batch_id = gn_batch_id
		  AND EFFECTIVE_BEGIN_DATE IS NOT NULL;	 	

		--Pick all records for which EFFECTIVE_END_DATE is not null and validate date format.
		UPDATE SLC_ISP_FAS_SUPPIER_CNV_STG
		   SET ERROR_MSG = DECODE(slc_is_date_valid_f(EFFECTIVE_END_DATE)
				,'N', ERROR_MSG||'~Effective End Date:Invalid date format',ERROR_MSG )   
		WHERE batch_id = gn_batch_id
		  AND EFFECTIVE_END_DATE IS NOT NULL;	 	 		  
		
		--If there is error then mark record status as Failed.
		UPDATE SLC_ISP_FAS_SUPPIER_CNV_STG
		  SET status = gv_invalid_status
		WHERE batch_id = gn_batch_id
		  AND ERROR_MSG IS NOT NULL;
		  
		--If there is no error then mark record status as Valid.
		UPDATE SLC_ISP_FAS_SUPPIER_CNV_STG
		  SET status = gv_valid_status
		WHERE batch_id = gn_batch_id
		  AND ERROR_MSG IS NULL;		  
		
		COMMIT;
	END slc_validate_p;
	
/* ****************************************************************
	NAME:              slc_import_p
	PURPOSE:           This procedure will be used to valid data from staging table into base tables.
							 before processing
*****************************************************************/
	PROCEDURE slc_import_p
	IS
	CURSOR c_valid_supplier 
	IS
	SELECT * 
	  FROM SLC_ISP_FAS_SUPPIER_CNV_STG
	WHERE batch_id = gn_batch_id
	  AND status = gv_valid_status
	  ORDER BY record_id;
	
	CURSOR c_supplier_num(p_in_party_id	 IN	NUMBER)
	IS
	SELECT segment1
	 FROM ap_suppliers
	WHERE party_id = p_in_party_id;
	
	  
   TYPE lc_valid_supplier_tbl IS TABLE OF c_valid_supplier%ROWTYPE
    INDEX BY BINARY_INTEGER;
    
   lc_valid_supplier_tab             		lc_valid_supplier_tbl; 
   ln_index			NUMBER;
   
   lv_vendor_type_lkp_code		VARCHAR2(25)  	DEFAULT 'FRANCHISEE';
   lv_term_name					VARCHAR2(25)  	DEFAULT 'IMMEDIATE';
   lv_pay_date_lkp_code			VARCHAR2(25)	DEFAULT	'DISCOUNT';
   lv_pay_group_lkp_code		VARCHAR2(25)	DEFAULT	'FRANCHISEE';--Changes for v1.2
   lv_invoice_currency_code		VARCHAR2(25)	DEFAULT	'USD';
   lv_payment_currency_code		VARCHAR2(25)	DEFAULT	'USD';
   lv_corporation				VARCHAR2(50)	DEFAULT 'Corporation';
   lv_sole						VARCHAR2(50)	DEFAULT 'Sole Proprietorship';
   lv_legal_status_txt			VARCHAR2(20)	DEFAULT 'LEGAL_STATUS';
   lv_organization_type_fran	VARCHAR2(1) 	DEFAULT 'X'; --INDIVIDUAL (NON-SERVICE) Lookup Name:"ORGANIZATION TYPE"
   lv_organization_type_corp	VARCHAR2(1) 	DEFAULT 'C'; -- CORPORATION/OTHER  Lookup Name:"ORGANIZATION TYPE"
   lv_organization_type_llp		VARCHAR2(1) 	DEFAULT 'P'; -- PARTNERSHIP/LLC/LLP/OTHER  Lookup Name:"ORGANIZATION TYPE"
   lv_organization_type_code	VARCHAR2(1);
   lv_sequence_num				AP_SUPPLIERS.SEGMENT1%TYPE;
   lv_jgzz_fiscal_code			HZ_PARTIES.jgzz_fiscal_code%TYPE;
   
	
	lv_error_flag				VARCHAR2(1) DEFAULT 'N';
	lv_error_msg				VARCHAR2(4000);
	lv_vendor_name				VARCHAR2(1000);
	ln_fran1_party_id			HZ_PARTIES.party_id%TYPE;
	ln_fran1_tax_py_id	HZ_PARTIES.jgzz_fiscal_code%TYPE;
	ln_fran2_party_id			HZ_PARTIES.party_id%TYPE;
	ln_corp_party_id			HZ_PARTIES.party_id%TYPE;
	ln_site_party_id			HZ_PARTIES.party_id%TYPE;
	
	--Commented for changes v1.3
	--Uncommented for changes for v1.6
	lv_site_exists_flag		VARCHAR2(1);
	ln_vendor_id				AP_SUPPLIERS.vendor_id%TYPE;
	ln_taxpayer_id_count		NUMBER;
	
	lv_franchisee1_exists_flag		VARCHAR2(1);
	lv_franchisee2_exists_flag		VARCHAR2(1);
	lv_corporation_exists_flag		VARCHAR2(1);
	
	BEGIN
	
	OPEN c_valid_supplier;
	LOOP
	lc_valid_supplier_tab.DELETE;
	FETCH c_valid_supplier
	BULK COLLECT INTO lc_valid_supplier_tab LIMIT 5;
	EXIT WHEN lc_valid_supplier_tab.COUNT = 0;
		
		--For all the valid records call API's to import supplier into Oracle Supplier Hub.
		FOR ln_index IN lc_valid_supplier_tab.FIRST..lc_valid_supplier_tab.LAST
		LOOP
			slc_write_log_p(gv_log,'Record Id:'||lc_valid_supplier_tab(ln_index).record_id);
			SAVEPOINT supplier_savepoint;
			--Reinitialize local variables.
			lv_error_flag := 'N';
			lv_error_msg  := NULL;
			lv_vendor_name := NULL;
			ln_fran1_party_id	:= NULL;
			ln_fran1_tax_py_id	:= NULL;
			ln_fran2_party_id	:= NULL;
			lv_sequence_num		:= NULL;
			ln_corp_party_id	:= NULL;
			ln_vendor_id			:= NULL;
			lv_organization_type_code := NULL;
			ln_taxpayer_id_count	  := NULL;
			lv_franchisee1_exists_flag	:= 'N';
			lv_franchisee2_exists_flag	:= 'N';	
			lv_corporation_exists_flag	:= 'N';
			--Commented. Changes for v1.3
			--Uncommented for changes for v1.6
			lv_site_exists_flag			:= 'N';
			
			slc_write_log_p(gv_log,'********Start****************');
			--Creating Supplier for First Franchisee if Franchisee1 SSN# is not null.
			IF lc_valid_supplier_tab(ln_index).franchisee1_ssn IS NOT NULL THEN
				slc_write_log_p(gv_log,'franchisee1_ssn:'||lc_valid_supplier_tab(ln_index).franchisee1_ssn );
				BEGIN
				
					SELECT hp.party_id , hp.jgzz_fiscal_code 
					  INTO ln_fran1_party_id , ln_fran1_tax_py_id
					 FROM POS_SUPP_PROF_EXT_B pos
						  ,hz_parties hp
					WHERE pos.C_EXT_ATTR8 = lc_valid_supplier_tab(ln_index).franchisee1_ssn
					  AND pos.party_id = hp.party_id; 
				
				EXCEPTION
				WHEN NO_DATA_FOUND THEN
				ln_fran1_party_id := NULL;
				WHEN OTHERS THEN
				lv_error_flag := 'Y';
				lv_error_msg  := 'Error while fetching Franchisee1 information. Error Message:'||SQLERRM;
				END;
				slc_write_log_p(gv_log,'ln_fran1_party_id:'||ln_fran1_party_id||' ln_fran1_tax_py_id:'||ln_fran1_tax_py_id||
										' Federal Id:'||lc_valid_supplier_tab(ln_index).federal_id);
				--If Franchisee1 is already existing in the database then do not create Supplier Again.
				IF lv_error_flag = 'N' 
				AND ln_fran1_party_id IS NULL 
				THEN
					slc_write_log_p(gv_log,'Creating Franchisee1');
					lv_jgzz_fiscal_code := lc_valid_supplier_tab(ln_index).franchisee1_ssn;
					-- Changes for v1.4 Begin
					-- Removing the logic to populate tax payer id value.
					-- Now tax payer id value for Franchisee1 would be SSN value irrespective of Incorp Flag and Federal Id.
					/*
					IF lc_valid_supplier_tab(ln_index).incorp_flag = 'Y' THEN
						lv_jgzz_fiscal_code := lc_valid_supplier_tab(ln_index).franchisee1_ssn;
					ELSE
						IF lc_valid_supplier_tab(ln_index).federal_id IS NOT NULL THEN
							lv_jgzz_fiscal_code := lc_valid_supplier_tab(ln_index).federal_id;
						ELSE
							lv_jgzz_fiscal_code := lc_valid_supplier_tab(ln_index).franchisee1_ssn;
						END IF;
					END IF;
					*/
					-- Changes for v1.4 End

					--Call Procedure to create Supplier in Supplier hub.
					lv_vendor_name := lc_valid_supplier_tab(ln_index).franchisee1_first_name||
								      (CASE WHEN lc_valid_supplier_tab(ln_index).franchisee1_middle_name IS NOT NULL
											THEN ' '||lc_valid_supplier_tab(ln_index).franchisee1_middle_name
											ELSE ''
									  END)||
									  (CASE WHEN lc_valid_supplier_tab(ln_index).franchisee1_last_name IS NOT NULL
											THEN ' '||lc_valid_supplier_tab(ln_index).franchisee1_last_name
											ELSE ''
									  END);
										 
					slc_create_supplier_p('Franchisee1'
	-- If vendor_name matches for any supplier existing in database then create_vendor API fails with error 
	-- "Each record in the supplier master must have a unique supplier name and there exists already a supplier called 
	-- <name of supplier>.  Either correct the updated supplier name so that it will be unique or press Cancel to undo the changes you have made."
	-- To avoid this error happening we add record_id to the vendor name to make vendor name a unique combination
	-- This Vendor Name will be updated again once the supplier is created with seqment1 using slc_update_org_profile_p.
										,lv_vendor_name||'_'||lc_valid_supplier_tab(ln_index).record_id	--Vendor Name
										,lv_vendor_name	--Vendor Name Alt
										,NULL -- Supplier number would be automatically generated 
										--,SLC_ISP_FASSUPP_SEGMENT_NUM_S.NEXTVAL	--Segment1	
										,lv_vendor_type_lkp_code	--Vendor Type Lookup Code
										,lv_term_name	--Term Name
										,lv_pay_date_lkp_code	-- Pay Date Basis Lookup Code 
										,lv_pay_group_lkp_code --Pay Group Lookup Code 
										,lv_invoice_currency_code	--Invoice Currency Code
										,lv_payment_currency_code	--Payment Currency Code
										,lv_jgzz_fiscal_code	--jgzz fiscal code
										,lv_vendor_name	--Tax Reporting Name
										,lv_organization_type_fran
										,ln_fran1_party_id	--Party Id of the created franchisee.
										,lv_error_flag
										,lv_error_msg
										);
					
					slc_write_log_p(gv_log,'After Franchisee1 creation ln_fran1_party_id:'||ln_fran1_party_id);
					--If there is no error while creating supplier then call update Organization profile to update organization name.
					IF lv_error_flag = 'N' THEN
						OPEN c_supplier_num(ln_fran1_party_id);
						FETCH c_supplier_num INTO lv_sequence_num;
						CLOSE c_supplier_num;
						
						slc_write_log_p(gv_log,'lv_sequence_num: '||lv_sequence_num);
						slc_update_org_profile_p('Franchisee1'
												,ln_fran1_party_id
												,lv_vendor_name ||'_'||lv_sequence_num
												,NULL
												,lv_error_flag
												,lv_error_msg
												);
					END IF;
					slc_write_log_p(gv_log,'After Org Updates lv_error_flag:'||lv_error_flag||' lv_error_msg:'||lv_error_msg);
					
					--If there is no error while creating Org Profile then call API's to update UDA information.
					IF lv_error_flag = 'N' THEN
						slc_process_uda_attributes_p('Franchisee1'
													  ,ln_fran1_party_id
													  ,lc_valid_supplier_tab(ln_index).franchisee1_ssn
													  ,lc_valid_supplier_tab(ln_index).franchisee1_birth_date
													  ,lc_valid_supplier_tab(ln_index).franchisee1_bkgrd
													  --Changes for v1.5 Begin
													  --,lc_valid_supplier_tab(ln_index).franchisee1_marital
													  ,(CASE WHEN lc_valid_supplier_tab(ln_index).franchisee1_marital = 'A'
													         THEN 'M'
															 ELSE lc_valid_supplier_tab(ln_index).franchisee1_marital
														END)
														----Changes for v1.5 End
													  ,lc_valid_supplier_tab(ln_index).franchisee1_sex
													  ,'FAS' -- Conversion source must be FAS
													  ,lc_valid_supplier_tab(ln_index).franchisee1_original_date
													  ,lc_valid_supplier_tab(ln_index).effective_begin_date
													  ,lc_valid_supplier_tab(ln_index).effective_end_date
													  ,lc_valid_supplier_tab(ln_index).franchisee1_first_name
													  ,lc_valid_supplier_tab(ln_index).franchisee1_middle_name
													  ,lc_valid_supplier_tab(ln_index).franchisee1_last_name
													  ,lc_valid_supplier_tab(ln_index).franchisee1_zid
													  ,lv_error_flag
													  ,lv_error_msg											  
											  );
					END IF;
					slc_write_log_p(gv_log,'After Process UDA Updates lv_error_flag:'||lv_error_flag||' lv_error_msg:'||lv_error_msg);
					slc_write_log_p(gv_log,'********Franchisee1 creation End****************');
					slc_write_log_p(gv_log,'');
				
				-- Changes for v1.4 Begin
				-- No update logic is needed for Franchisee1
				/*
				--For Franchisee1 if Franchisee1 already exists and Site is not having incorporation set as Y
				--then we have to update tax payer id for the franchisee. 
				ELSIF lv_error_flag = 'N' AND ln_fran1_party_id IS NOT NULL 
				AND lc_valid_supplier_tab(ln_index).incorp_flag = 'N' 
				AND lc_valid_supplier_tab(ln_index).federal_id IS NOT NULL
				AND ln_fran1_tax_py_id <> lc_valid_supplier_tab(ln_index).federal_id THEN
					slc_write_log_p(gv_log,'Updating Franchisee1');
					lv_franchisee1_exists_flag := 'Y';
						slc_update_org_profile_p('Franchisee1'
												,ln_fran1_party_id
												,NULL
												,lc_valid_supplier_tab(ln_index).federal_id
												,lv_error_flag
												,lv_error_msg
												);						
					slc_write_log_p(gv_log,'After updating supplier tax payer info: lv_error_flag:'||lv_error_flag||
									 ' lv_error_msg:'||lv_error_msg);
				ELSIF lv_error_flag = 'N' AND ln_fran1_party_id IS NOT NULL THEN
				    slc_write_log_p(gv_log,'Updating Franchisee2');
					lv_franchisee1_exists_flag := 'Y';
				*/
				-- Changes for v1.4 End
				ELSIF lv_error_flag = 'N' AND ln_fran1_party_id IS NOT NULL THEN
					slc_write_log_p(gv_log,'Franchisee1 exists');
					lv_franchisee1_exists_flag := 'Y';
				END IF;--End of Franchisee1 Create if.
			END IF;-- End of Franchisee1 SSN check
			
			--Creating Supplier for Second Franchisee if Franchisee2 SSN# is not null.
			IF lc_valid_supplier_tab(ln_index).franchisee2_ssn IS NOT NULL AND lv_error_flag = 'N' THEN
				slc_write_log_p(gv_log,'franchisee2_ssn:'||lc_valid_supplier_tab(ln_index).franchisee2_ssn );
				BEGIN
				
					SELECT party_id 
					  INTO ln_fran2_party_id
					 FROM POS_SUPP_PROF_EXT_B
					WHERE C_EXT_ATTR8 = lc_valid_supplier_tab(ln_index).franchisee2_ssn; 
				
				EXCEPTION
				WHEN NO_DATA_FOUND THEN
				ln_fran2_party_id := NULL;
				WHEN OTHERS THEN
				lv_error_flag := 'Y';
				lv_error_msg  := 'Error while fetching Franchisee2 information. Error Message:'||SQLERRM;
				END;
				slc_write_log_p(gv_log,'ln_fran2_party_id:'||ln_fran2_party_id);
				--If Franchisee2 is already existing in the database then do not create Supplier Again.
				IF lv_error_flag = 'N'  
				AND ln_fran2_party_id IS NULL 
				THEN
					slc_write_log_p(gv_log,'Creating Franchisee2');
					--Call Procedure to create Supplier in Supplier hub.
					lv_vendor_name := lc_valid_supplier_tab(ln_index).franchisee2_first_name ||
										(CASE WHEN lc_valid_supplier_tab(ln_index).franchisee2_middle_name IS NOT NULL
												THEN ' '||lc_valid_supplier_tab(ln_index).franchisee2_middle_name
												ELSE ''
										  END)||
										  (CASE WHEN lc_valid_supplier_tab(ln_index).franchisee2_last_name IS NOT NULL
												THEN ' '||lc_valid_supplier_tab(ln_index).franchisee2_last_name
												ELSE ''
										  END);
	-- If vendor_name matches for any supplier existing in database then create_vendor API fails with error 
	-- "Each record in the supplier master must have a unique supplier name and there exists already a supplier called 
	-- <name of supplier>.  Either correct the updated supplier name so that it will be unique or press Cancel to undo the changes you have made."
	-- To avoid this error happening we add record_id to the vendor name to make vendor name a unique combination
	-- This Vendor Name will be updated again once the supplier is created with seqment1 using slc_update_org_profile_p.										 
					slc_create_supplier_p('Franchisee2'
											,lv_vendor_name||'_'||lc_valid_supplier_tab(ln_index).record_id	--Vendor Name
											,lv_vendor_name	--Vendor Name Alt
											,NULL -- Supplier number would be automatically generated 
											--,SLC_ISP_FASSUPP_SEGMENT_NUM_S.NEXTVAL	--Segment1
											,lv_vendor_type_lkp_code	--Vendor Type Lookup Code
											,lv_term_name	--Term Name
											,lv_pay_date_lkp_code	-- Pay Date Basis Lookup Code 
											,lv_pay_group_lkp_code --Pay Group Lookup Code 
											,lv_invoice_currency_code	--Invoice Currency Code
											,lv_payment_currency_code	--Payment Currency Code
											,lc_valid_supplier_tab(ln_index).franchisee2_ssn--jgzz fiscal code
											,lv_vendor_name	--Tax Reporting Name
											,lv_organization_type_fran
											,ln_fran2_party_id	--Party Id of the created franchisee.
											,lv_error_flag
											,lv_error_msg
											);
					
					slc_write_log_p(gv_log,'After Franchisee2 creation ln_fran2_party_id:'||ln_fran2_party_id);
					--If there is no error while creating supplier then call update Organization profile to update organization name.
					IF lv_error_flag = 'N' THEN
						OPEN c_supplier_num(ln_fran2_party_id);
						FETCH c_supplier_num INTO lv_sequence_num;
						CLOSE c_supplier_num;
						
						slc_write_log_p(gv_log,'lv_sequence_num: '||lv_sequence_num);
						slc_update_org_profile_p('Franchisee2'
												,ln_fran2_party_id
												,lv_vendor_name ||'_'||lv_sequence_num
												,NULL
												,lv_error_flag
												,lv_error_msg
												);
					END IF;
					slc_write_log_p(gv_log,'After Org Updates lv_error_flag:'||lv_error_flag||' lv_error_msg:'||lv_error_msg);
					
					--If there is no error while creating Org Profile then call API's to update UDA information.
					IF lv_error_flag = 'N' THEN
						slc_process_uda_attributes_p('Franchisee2'
													  ,ln_fran2_party_id
													  ,lc_valid_supplier_tab(ln_index).franchisee2_ssn
													  ,lc_valid_supplier_tab(ln_index).franchisee2_birth_date
													  ,lc_valid_supplier_tab(ln_index).franchisee2_bkgrd
													  --Changes for v1.5 Begin
													  ,(CASE WHEN lc_valid_supplier_tab(ln_index).franchisee2_marital = 'A'
														     THEN 'M'
															ELSE lc_valid_supplier_tab(ln_index).franchisee2_marital
														END)
													  --,lc_valid_supplier_tab(ln_index).franchisee2_marital
													  --Changes for v1.5 End
													  ,lc_valid_supplier_tab(ln_index).franchisee2_sex
													  ,'FAS' -- Conversion source must be FAS
													  ,lc_valid_supplier_tab(ln_index).franchisee2_original_date
													  ,lc_valid_supplier_tab(ln_index).effective_begin_date
													  ,lc_valid_supplier_tab(ln_index).effective_end_date
													  ,lc_valid_supplier_tab(ln_index).franchisee2_first_name
													  ,lc_valid_supplier_tab(ln_index).franchisee2_middle_name
													  ,lc_valid_supplier_tab(ln_index).franchisee2_last_name
													  ,lc_valid_supplier_tab(ln_index).franchisee2_zid													  
													  ,lv_error_flag
													  ,lv_error_msg											  
													  );
					END IF;
					slc_write_log_p(gv_log,'After Process UDA Updates lv_error_flag:'||lv_error_flag||' lv_error_msg:'||lv_error_msg);
					slc_write_log_p(gv_log,'********Franchisee2 creation End*****************');
					slc_write_log_p(gv_log,'');
				ELSIF lv_error_flag = 'N' AND ln_fran2_party_id IS NOT NULL THEN
					slc_write_log_p(gv_log,'Franchisee2 exists');
					lv_franchisee2_exists_flag := 'Y';
				END IF;--End of Franchisee2 Create if.
			END IF;-- End of Franchisee2 SSN check			
			
			
			--Creating Supplier for Corporation if INCORP flag is Y is not null.
			IF lc_valid_supplier_tab(ln_index).federal_id IS NOT NULL AND lv_error_flag = 'N' 
			AND lc_valid_supplier_tab(ln_index).incorp_flag = 'Y' THEN
				slc_write_log_p(gv_log,'federal_id:'||lc_valid_supplier_tab(ln_index).federal_id );
				BEGIN
				
					SELECT hp.party_id 
					  INTO ln_corp_party_id
					 FROM hz_parties hp
						 ,pos_supp_prof_ext_b pos
					WHERE jgzz_fiscal_code = lc_valid_supplier_tab(ln_index).federal_id
					  AND hp.party_id = pos.party_id
					  AND pos.c_ext_attr4 = 'FAS'; 
				
				EXCEPTION
				WHEN NO_DATA_FOUND THEN
				ln_corp_party_id := NULL;
				WHEN OTHERS THEN
				lv_error_flag := 'Y';
				lv_error_msg  := 'Error while fetching Corporation information. Error Message:'||SQLERRM;
				END;
				slc_write_log_p(gv_log,'ln_corp_party_id:'||ln_corp_party_id);
				--If Corporation is already existing in the database then do not create Supplier Again.
				IF lv_error_flag = 'N' AND ln_corp_party_id IS NULL THEN
					slc_write_log_p(gv_log,'Creating Corporation');
					--Call Procedure to create Supplier in Supplier hub.
					lv_vendor_name := lc_valid_supplier_tab(ln_index).incorp_name;
					
					-- If Organization names has LLP in the end then organization type would be PARTNERSHIP/LLC/LLP/OTHER
					-- else it would be CORPORATION/OTHER
					IF lc_valid_supplier_tab(ln_index).incorp_name LIKE '%LLC' THEN
					lv_organization_type_code := lv_organization_type_llp;
					ELSE
					lv_organization_type_code := lv_organization_type_corp;
					END IF;
	-- If vendor_name matches for any supplier existing in database then create_vendor API fails with error 
	-- "Each record in the supplier master must have a unique supplier name and there exists already a supplier called 
	-- <name of supplier>.  Either correct the updated supplier name so that it will be unique or press Cancel to undo the changes you have made."
	-- To avoid this error happening we add record_id to the vendor name to make vendor name a unique combination
	-- This Vendor Name will be updated again once the supplier is created with seqment1 using slc_update_org_profile_p.					
					slc_create_supplier_p('Corporation'
											,lv_vendor_name||'_'||lc_valid_supplier_tab(ln_index).record_id	--Vendor Name
											,lv_vendor_name	--Vendor Name Alt
											,NULL -- Supplier number would be automatically generated 
											--,SLC_ISP_FASSUPP_SEGMENT_NUM_S.NEXTVAL	--Segment1
											,lv_vendor_type_lkp_code	--Vendor Type Lookup Code
											,lv_term_name	--Term Name
											,lv_pay_date_lkp_code	-- Pay Date Basis Lookup Code 
											,lv_pay_group_lkp_code --Pay Group Lookup Code 
											,lv_invoice_currency_code	--Invoice Currency Code
											,lv_payment_currency_code	--Payment Currency Code
											,lc_valid_supplier_tab(ln_index).federal_id--jgzz fiscal code
											,lv_vendor_name	--Tax Reporting Name
											,lv_organization_type_code
											,ln_corp_party_id	--Party Id of the created franchisee.
											,lv_error_flag
											,lv_error_msg
											);
					
					slc_write_log_p(gv_log,'After Corporation creation ln_corp_party_id:'||ln_corp_party_id);
					--If there is no error while creating supplier then call update Organization profile to update organization name.
					IF lv_error_flag = 'N' THEN
						OPEN c_supplier_num(ln_corp_party_id);
						FETCH c_supplier_num INTO lv_sequence_num;
						CLOSE c_supplier_num;
						
						slc_write_log_p(gv_log,'lv_sequence_num: '||lv_sequence_num);
						slc_update_org_profile_p('Corporation'
												,ln_corp_party_id
												,lv_vendor_name ||'_'||lv_sequence_num
												,NULL
												,lv_error_flag
												,lv_error_msg
												);
					END IF;
					slc_write_log_p(gv_log,'After Org Updates lv_error_flag:'||lv_error_flag||' lv_error_msg:'||lv_error_msg);
					
					--If there is no error while creating Org Profile then call API's to update UDA information.
					IF lv_error_flag = 'N' THEN
						slc_process_uda_attributes_p('Corporation'
													  ,ln_corp_party_id
													  ,NULL
													  ,NULL
													  ,NULL
													  ,NULL
													  ,NULL
													  ,'FAS' -- Conversion source must be FAS
													  ,NULL--Original Date
													  ,lc_valid_supplier_tab(ln_index).effective_begin_date
													  ,lc_valid_supplier_tab(ln_index).effective_end_date
													  ,NULL --First name
													  ,NULL --Middle 
													  ,NULL --Last Name
													  ,NULL --ZID
													  ,lv_error_flag
													  ,lv_error_msg											  
													  );
					END IF;
					slc_write_log_p(gv_log,'After Process UDA Updates lv_error_flag:'||lv_error_flag||' lv_error_msg:'||lv_error_msg);
					slc_write_log_p(gv_log,'********Corporation creation End*************');
					slc_write_log_p(gv_log,'');
				ELSIF lv_error_flag = 'N' AND ln_corp_party_id IS NOT NULL THEN
					slc_write_log_p(gv_log,'Corporation exists');
					lv_corporation_exists_flag := 'Y';
				END IF;--End of Corporation Create if.
			END IF;-- End of Corporation SSN check	
			
			--Create Supplier Site
			--Supplier Sites needs to be created only for records coming from Current file.
			IF lv_error_flag = 'N' AND lc_valid_supplier_tab(ln_index).record_type = 'CURRENT' THEN
			

			IF lc_valid_supplier_tab(ln_index).incorp_flag = 'Y' THEN
				ln_site_party_id := ln_corp_party_id;
			ELSE
				ln_site_party_id := ln_fran1_party_id;
			END IF;
			
			slc_write_log_p(gv_log,'store_number:'||lc_valid_supplier_tab(ln_index).store_number||
							 ' ln_site_party_id:'||ln_site_party_id||'lv_error_flag:'||lv_error_flag);
							 
			--Commented changes for v1.3
			--Uncommented for changes for v1.6
			
			BEGIN
			--Verify if the site exists for the Store Number passed as parameter.
			select 'Y'
			  INTO lv_site_exists_flag 
			  FROM ap_supplier_sites_all sup_site
				  ,ap_suppliers sup
			 WHERE sup_site.vendor_site_code = lc_valid_supplier_tab(ln_index).store_number
			   AND sup.vendor_id = sup_site.vendor_id
			   AND sup.party_id = ln_site_party_id;
			EXCEPTION
			WHEN NO_DATA_FOUND THEN
			lv_site_exists_flag := 'N';
			WHEN OTHERS THEN 
				lv_error_flag := 'Y';
				lv_error_msg  := 'Error while fetching Supplier Site Flag Information:'||SQLERRM;			
			END;
			slc_write_log_p(gv_log,'lv_site_exists_flag:'||lv_site_exists_flag||' lv_error_flag:'||lv_error_flag);
			--Added site exists clause for v1.6 changes.
			--Call API to create party site only if Site is not existing.
			IF lv_error_flag = 'N' AND lv_site_exists_flag = 'N' THEN
			
			slc_create_supplier_site_p(ln_site_party_id
								,lc_valid_supplier_tab(ln_index).store_number
								,lv_pay_group_lkp_code --Pay Group Lookup Code 
								,lc_valid_supplier_tab(ln_index).ADDRESS1
								,NULL--AddressLine2
								,lc_valid_supplier_tab(ln_index).city
								,lc_valid_supplier_tab(ln_index).state
								,NULL--County
								,'US'--Country
								,lc_valid_supplier_tab(ln_index).zip
								,lc_valid_supplier_tab(ln_index).phone_num
								,'SLC CONSOLIDATED'--Operating Unit Name
								,lv_error_flag
								,lv_error_msg
								);
			slc_write_log_p(gv_log,'After slc_create_supplier_site_p lv_error_flag:'||lv_error_flag||' lv_error_msg:'||lv_error_msg);
			slc_write_log_p(gv_log,'********Site creation End***********');
			slc_write_log_p(gv_log,'');
			END IF;--End of Site create if.
			END IF;
			
			slc_write_log_p(gv_log,'Final flag values in import. lv_error_flag:'||lv_error_flag||' lv_error_msg:'||lv_error_msg);
			slc_write_log_p(gv_log,'Updating import status for record_id .'||lc_valid_supplier_tab(ln_index).record_id);

			IF lv_error_flag = 'Y' THEN
			 ROLLBACK TO supplier_savepoint;
			ELSIF lv_error_flag = 'N' THEN
			 COMMIT; 
			END IF;
			slc_write_log_p(gv_log,'lv_franchisee1_exists_flag:'||lv_franchisee1_exists_flag);
			slc_write_log_p(gv_log,'lv_franchisee2_exists_flag:'||lv_franchisee2_exists_flag);
			slc_write_log_p(gv_log,'lv_corporation_exists_flag:'||lv_corporation_exists_flag);
			UPDATE SLC_ISP_FAS_SUPPIER_CNV_STG
			   SET status = DECODE(lv_error_flag,'Y',gv_error_status,'N',gv_processed_status)
			       ,error_msg = DECODE(lv_error_flag,'Y',lv_error_msg,'N',NULL)
				   ,franchisee1_party_id = ln_fran1_party_id
				   ,franchisee2_party_id = ln_fran2_party_id
				   ,incorp_party_id	= ln_corp_party_id
				   ,request_id = gn_request_id
				   ,last_update_date = sysdate
				   ,last_updated_by = gn_user_id
				   ,last_update_login = gn_login_id
				   ,franchisee1_exists_flag = lv_franchisee1_exists_flag
				   ,franchisee2_exists_flag = lv_franchisee2_exists_flag
				   ,incorp_exists_flag = lv_corporation_exists_flag
				   --Commented changes for v1.3
				   --,store_exists_flag = lv_site_exists_flag
			WHERE record_id = lc_valid_supplier_tab(ln_index).record_id;
			COMMIT;


			END LOOP;--End of Collection loop
	
	END LOOP;--End of Cursor loop
	CLOSE c_valid_supplier;
	
	NULL;
	END slc_import_p;	

  /*********************************************************************************************
  -- Procedure Name   : slc_main_p
  -- Purpose          : This is main procedure invoked by FAS to Supplier Hub conversion program.
  -- Input Parameters : 
  --  p_processing_mode : Processing mode for program
  --  p_debug_flag      : Debug Flag will decide if we want to log messages.
  --  p_batch_size      : This value would determine how many records to pick for conversion
  --
  -- Output Parameters :
  --  p_errbuff        : Standard output parameter with Return Message for concurrent program
  --  p_retcode        : Standard output parameter with Return Code for concurrent program
  --********************************************************************************************/
	PROCEDURE slc_main_p  ( p_errbuff           OUT VARCHAR2,
			 p_retcode           OUT NUMBER,  
			 p_processing_mode   IN  VARCHAR2,
			 p_batch_size        IN  NUMBER,
			 p_debug_flag        IN  VARCHAR2
			)
	IS
	lv_status1 		VARCHAR2(20)	DEFAULT NULL;
	lv_status2 		VARCHAR2(20) 	DEFAULT NULL;
	lv_status3 		VARCHAR2(20)	DEFAULT NULL;
	ln_batch_size	NUMBER		DEFAULT 50000;
	BEGIN
	 gv_debug_flag := p_debug_flag;
	 IF p_batch_size IS NOT NULL THEN
		ln_batch_size := p_batch_size;
	 END IF;
	
		
	 slc_write_log_p(gv_log,'p_processing_mode: '||p_processing_mode||' p_batch_size:'||p_batch_size||' p_debug_flag:'||p_debug_flag);
	 slc_write_log_p(gv_log,'ln_batch_size :'||ln_batch_size);
	 slc_write_log_p(gv_out,'*************************Output***************************');
    slc_write_log_p(gv_out,'*************************Parameters***************************');
    slc_write_log_p(gv_out,'p_processing_mode: '||p_processing_mode);
    slc_write_log_p(gv_out,'p_batch_size: '||p_batch_size);
    slc_write_log_p(gv_out,'p_debug_flag: '||p_debug_flag);
	 slc_write_log_p(gv_out,'gn_request_id: '||gn_request_id);
    slc_write_log_p(gv_out,'**************************************************************');	

	 -- Call slc_assign_batch_id_p
	 -- If we are running concurrent program in Validate Mode we will assign batch Id to New records and only Validation will be performed.
	 -- If we are running concurrent program in Process Mode we will assign batch Id to Valid records and only Importing will be performed.
	 -- If we are running concurrent program in Reprocess Mode we will assign batch Id to all records which had failed either during validation
	 --			stage or during import stage and will validate it. To process it we will have to call program in Process Mode again.
	 
 
	IF p_processing_mode IN (gv_validate_mode,gv_revalidate_mode) THEN
		IF p_processing_mode IN (gv_validate_mode) THEN
	 		lv_status1 := gv_new_status;
			lv_status2 := NULL;
			lv_status3 := NULL;
	 	ELSIF p_processing_mode = gv_revalidate_mode THEN
	 		lv_status1 := gv_invalid_status;
	 		lv_status2 := gv_error_status;	 	
			lv_status3 := NULL;
	 	END IF;
	 	slc_assign_batch_id_p(lv_status1,lv_status2,lv_status3,ln_batch_size);
		slc_validate_p;
	END IF;
	
	IF p_processing_mode IN (gv_process_mode) THEN
		lv_status1 := gv_valid_status;
		lv_status2 := NULL;
		lv_status3 := NULL;
		slc_assign_batch_id_p(lv_status1,lv_status2,lv_status3,ln_batch_size);
		slc_import_p;
	END IF;	
	
	slc_print_summary_p(p_processing_mode);
	p_retcode  := gn_program_status;	 

	
	END slc_main_p;
	

END SLC_ISP_FASSUPP_CNV_PKG;
/
SHOW ERROR;
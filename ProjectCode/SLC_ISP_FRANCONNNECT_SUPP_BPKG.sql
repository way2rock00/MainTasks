REM ===============================================================================
REM  Program:      SLC_ISP_FRANCONNNECT_SUPP_BPKG.sql
REM  Author:       Akshay Nayak
REM  Date:         08-May-2017
REM  Purpose:      This package is called for interfacing supplier information from
REM				   FranConnect System into Supplier Hub.
REM  Change Log:   08-May-2017 1.0 Akshay Nayak Created
REM  Change Log:   14-AUG-2017 1.1 Akshay Nayak Updated as per latest design.
REM  Change Log:   05-OCT-2017 1.2 Akshay Nayak Changes for Defect 42203. Setting Business Classification as UNDETERMINED
REM  Change Log:   17-OCT-2017 1.3 Akshay Nayak Changes for Defect 43044. 
REM								   If Supplier has active bank account then Supplier site created should have default payment method
REM									as EFT.
REM  Change Log:   15-NOV-2017 1.4 Akshay Nayak 1. Changes for Defect 43845. ASI II UAT : FranConnect – Interface created 
REM												   duplicate Suppliers with same Tax ID
REM  Change Log:   								2. Skip creation of Site for same Supplier
REM  Change Log:	6-Dec-2017 1.5 Akshay Nayak  Standardizing First Name,Middle Name and Last Name Issue.
REM  Change Log:	24-Jan-2018 1.6 Akshay Nayak  Changes for Defect 44771 Changing logic to fetch Segment1 i.e Supplier Number
REM  ================================================================================

create or replace
PACKAGE BODY SLC_ISP_FRANCONNNECT_SUPP_PKG AS

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
	gn_batch_id						NUMBER;
	gn_request_id                             NUMBER DEFAULT fnd_global.conc_request_id;
	gn_user_id                                NUMBER DEFAULT fnd_global.user_id;
	gn_login_id                               NUMBER DEFAULT fnd_global.login_id;	
	gn_program_status				NUMBER;

	--FranConnect processing variables.
	gv_not_processed			VARCHAR2(25) := 'NOT PROCESSED';
	gv_extracted				VARCHAR2(25) := 'EXTRACTED';
	gv_error_staging			VARCHAR2(25) := 'ERROR STAGING';
	gv_error_oracle				VARCHAR2(25) := 'ERROR ORACLE INTERFACE';
	gv_imported					VARCHAR2(25) := 'IMPORTED IN ORACLE';
	--Variables for Common Error Handling.
	gv_batch_key				  VARCHAR2(50) DEFAULT 'FRC-I-009'||'-'||TO_CHAR(SYSDATE,'DDMMYYYY');
	gv_business_process_name 		  VARCHAR2(100)  := 'SLC_ISP_FRANCONNNECT_SUPP_PKG';
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
		   fnd_file.put_line (fnd_file.log, p_in_message);
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
		   SELECT to_date(p_in_date,'MM/DD/YYYY') INTO ld_temp_date FROM DUAL;
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
		 ,pos_supp_prof_ext_b pos
		 ,ego_attr_groups_v eagv
		 ,fnd_application fa
	WHERE hp.jgzz_fiscal_code = p_in_taxpayer_id
		AND hp.party_id = pos.party_id
		AND eagv.attr_group_name = 'SLC_ISP_FRANCHISEE_DETAILS'
		AND eagv.attr_group_id = pos.attr_group_id
		AND fa.application_short_name = 'POS'
		AND fa.application_id = eagv.application_id
		AND pos.c_ext_attr4 IN ('FAS','FRANCONNECT');

   ln_tax_payer_count	NUMBER DEFAULT 0;
   BEGIN
   slc_write_log_p(gv_log,'In slc_get_taxpayer_id_count_f Start: p_in_taxpayer_id :'||p_in_taxpayer_id);
   OPEN c_taxpayer_count(p_in_taxpayer_id);
   FETCH c_taxpayer_count INTO ln_tax_payer_count;
   CLOSE c_taxpayer_count;
   slc_write_log_p(gv_log,'In slc_get_taxpayer_id_count_f: ln_tax_payer_count :'||ln_tax_payer_count);
   RETURN ln_tax_payer_count;
   END slc_get_taxpayer_id_count_f;
   
  
	--Changes for v1.4 Begin.
	/* ****************************************************************
		NAME:              slc_get_store_count_p
		PURPOSE:           This procedure will return non zero value if Store is already present for Supplier.
							Zero value indicates that Store is not present for Supplier.
		Input Parameters:  p_in_party_id 				IN 		NUMBER
						   p_in_vendor_site_code		IN 		VARCHAR2
		Output Parameters: 	p_out_supplier_site_id		OUT 	NUMBER
							p_out_supplier_site_count	OUT 	NUMBER
	*****************************************************************/   
	
   PROCEDURE slc_get_store_count_p(p_in_party_id				IN 		NUMBER
								  ,p_in_vendor_site_code		IN 		VARCHAR2
								  ,p_out_supplier_site_id		OUT 	NUMBER
								  ,p_out_supplier_site_count	OUT 	NUMBER
								  ,p_out_error_flag				OUT 	VARCHAR2
								  ,p_out_err_msg				OUT 	VARCHAR2
								 ) 
   IS
   lv_error_flag		VARCHAR2(1) DEFAULT 'N';
   lv_error_msg		    VARCHAR2(4000) DEFAULT NULL;
   	
   BEGIN
	slc_write_log_p(gv_log,'');
    slc_write_log_p(gv_log,'In slc_get_store_count_p Start: p_in_party_id :'||p_in_party_id||' p_in_vendor_site_code:'||p_in_vendor_site_code);
	BEGIN
	  SELECT 1,sups.party_site_id
	    INTO p_out_supplier_site_count,p_out_supplier_site_id
		FROM ap_suppliers sup ,
		  ap_supplier_sites_all sups
		WHERE sup.party_id        = p_in_party_id
		and sup.vendor_id         = sups.vendor_id
		AND sups.vendor_site_code = p_in_vendor_site_code;
	EXCEPTION
	WHEN NO_DATA_FOUND THEN
		p_out_supplier_site_count := 0;
		p_out_supplier_site_id	  := NULL;
	WHEN OTHERS THEN
		lv_error_flag := 'Y';
		lv_error_msg  := 'Unexpected error in slc_get_store_count_p. Error Message:'||SQLERRM;
	END;
	p_out_error_flag := lv_error_flag;
	p_out_err_msg := lv_error_msg;	
    slc_write_log_p(gv_log,'In slc_get_store_count_p: p_out_supplier_site_count :'||p_out_supplier_site_count||
							' p_out_supplier_site_id:'||p_out_supplier_site_id);
	slc_write_log_p(gv_log,'In slc_get_store_count_p: p_out_error_flag:'||p_out_error_flag||' p_out_err_msg:'||p_out_err_msg);
	slc_write_log_p(gv_log,'');
   END slc_get_store_count_p;
   --Changes for v1.4 End.
   
 /* ****************************************************************
	NAME:              slc_supplier_exists_p
	PURPOSE:           This procedure will verify if supplier is already existing
						else it returns non zero value.
	Input Parameters:  p_in_ssn		IN 		VARCHAR2
 *****************************************************************/
   PROCEDURE slc_supplier_exists_p(p_in_ssn 			IN VARCHAR2
								  ,p_out_party_id		OUT NUMBER
								  ,p_out_vendor_id      OUT NUMBER
								  ,p_out_count			OUT NUMBER
								  ,p_out_error_flag		OUT VARCHAR2
								  ,p_out_err_msg		OUT VARCHAR2										  
								  )
   IS 
   ln_count				NUMBER								DEFAULT 0;
   ln_party_id			hz_parties.party_id%TYPE 			DEFAULT NULL;
   ln_vendor_id         ap_suppliers.vendor_id%TYPE         DEFAULT NULL;
   lv_error_flag		VARCHAR2(1) DEFAULT 'N';
   lv_error_msg		    VARCHAR2(4000) DEFAULT NULL;
	
   BEGIN
    slc_write_log_p(gv_log,'');
	slc_write_log_p(gv_log,'In slc_supplier_exists_p: p_in_ssn:'||p_in_ssn );
		BEGIN
		SELECT hp.party_id ,1
		  INTO ln_party_id ,ln_count
		 FROM POS_SUPP_PROF_EXT_B pos
			  ,hz_parties hp
			  ,ego_attr_groups_v eagv
			  ,fnd_application fa
		WHERE pos.C_EXT_ATTR8 = p_in_ssn
		  AND pos.party_id = hp.party_id
		  AND eagv.attr_group_name = 'SLC_ISP_FRANCHISEE_DETAILS'
		  AND eagv.attr_group_id = pos.attr_group_id
		  AND fa.application_short_name = 'POS'
		  AND fa.application_id = eagv.application_id;

		IF ln_count<> 0 THEN
			SELECT vendor_id
			INTO ln_vendor_id
			FROM ap_suppliers 
			WHERE party_id=ln_party_id ;
		END IF;		  
	   EXCEPTION
		 WHEN NO_DATA_FOUND THEN
		 ln_count := 0;
		
		 WHEN OTHERS THEN
		  slc_write_log_p(gv_log,'Unexpected error in slc_supplier_exists_p. Error Message:'||SQLERRM);
		  lv_error_flag := 'Y';
		  lv_error_msg	:= 'Unexpected error in slc_supplier_exists_p. Error Message:'||SQLERRM;   	
	   END;
	p_out_party_id:= ln_party_id;
	p_out_count := ln_count;
	p_out_vendor_id := ln_vendor_id;
	slc_write_log_p(gv_log,'In slc_supplier_exists_p: ln_party_id:'||p_out_party_id);
	slc_write_log_p(gv_log,'In slc_supplier_exists_p: ln_count:'||p_out_count);
	slc_write_log_p(gv_log,'In slc_supplier_exists_p: ln_vendor_id:'||p_out_vendor_id);
	p_out_error_flag := lv_error_flag;
	p_out_err_msg := lv_error_msg;
	slc_write_log_p(gv_log,'In slc_supplier_exists_p p_out_error_flag '||p_out_error_flag||' p_out_err_msg '||p_out_err_msg);
	slc_write_log_p(gv_log,'');
   END slc_supplier_exists_p;

   /* ****************************************************************
	NAME:              slc_get_transformed_date_f
	PURPOSE:           This function will return transformed date object.
	Input Parameters:  p_in_date
*****************************************************************/
   FUNCTION slc_get_transformed_date_f(p_in_date VARCHAR2) RETURN DATE
   IS
   ld_date		DATE;
   BEGIN
	   --Date format will be MM/DD/YYYY
	   slc_write_log_p(gv_log,'In slc_get_transformed_date_f  p_in_date:'||p_in_date);
	   IF p_in_date IS NOT NULL THEN
		   SELECT to_date(p_in_date,'MM/DD/YYYY') INTO ld_date FROM DUAL;
	   END IF;
   RETURN ld_date;
   END slc_get_transformed_date_f;

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
	Input Parameters:  
*****************************************************************/
	PROCEDURE slc_assign_batch_id_p
	IS 
	BEGIN
		slc_write_log_p(gv_log,'In slc_assign_batch_id_p ');

		gn_batch_id := SLC_ISP_FRANCONN_BATCH_ID_S.NEXTVAL;
		UPDATE SLC_ISP_FRANCONNNECT_SUPP_STG 
			SET BATCH_ID = gn_batch_id
				,request_id = gn_request_id
				,last_update_date = sysdate
				,last_updated_by = gn_user_id
				,last_update_login = gn_login_id
		 WHERE status = 'N';
		slc_write_log_p(gv_log,'Batch Id :'||gn_batch_id);
	END slc_assign_batch_id_p;

	/* ****************************************************************
	NAME:              SLC_GET_LOOKUP_MEANING_F
	PURPOSE:           This function will be used to get lookup meaning.
	Input Parameters:  p_in_lookup_type		IN VARCHAR2
						 p_in_lookup_code		IN VARCHAR2
*****************************************************************/
	FUNCTION SLC_GET_LOOKUP_MEANING_F(p_in_lookup_type		IN VARCHAR2
							   ,p_in_lookup_code		IN VARCHAR2)
	RETURN VARCHAR2
	IS
	CURSOR c1
	IS
	SELECT description
	  FROM fnd_lookup_values
	 WHERE ENABLED_FLAG = 'Y'
	   AND TRUNC(SYSDATE) BETWEEN TRUNC(NVL(START_DATE_ACTIVE,SYSDATE)) AND TRUNC(NVL(END_DATE_ACTIVE,SYSDATE))
	   AND lookup_type = p_in_lookup_type
	   AND lookup_code = p_in_lookup_code;
	lv_description		fnd_lookup_values.description%TYPE;	
	BEGIN
	
	IF p_in_lookup_code IS NOT NULL THEN
		OPEN c1;
		FETCH c1 INTO lv_description;
		CLOSE c1;
	END IF;	
	RETURN lv_description;	
	END SLC_GET_LOOKUP_MEANING_F;
	
/*	*****************************************************************
	NAME:              slc_print_summary_p
	PURPOSE:           This procedure will print summary information after Conversion program is run.
	Input Parameters:  p_processing_mode IN VARCHAR2
*****************************************************************/
		
	PROCEDURE slc_print_summary_p(p_processing_mode	IN  VARCHAR2)
	IS
	ln_total_count						NUMBER;
	ln_total_success_count				NUMBER;
	ln_total_validation_count			NUMBER;
	ln_import_fail_count				NUMBER;
	ln_validation_fail_count			NUMBER;
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
  
	CURSOR cur_err_rec(p_in_status1 IN VARCHAR2,p_in_status2 IN VARCHAR2)
	IS
	SELECT record_id,error_msg ,store_number,store_letter_code
	  FROM SLC_ISP_FRANCONNNECT_SUPP_STG 
	 WHERE request_id = gn_request_id
	   AND status IN (p_in_status1,p_in_status2);
	lc_cur_err_rec   cur_err_rec%ROWTYPE;
	
	BEGIN
	
	SELECT count(1)
	  INTO ln_total_count
	 FROM SLC_ISP_FRANCONNNECT_SUPP_STG
	 WHERE request_id = gn_request_id;

	SELECT count(1)
	  INTO ln_total_success_count
	 FROM SLC_ISP_FRANCONNNECT_SUPP_STG
	 WHERE request_id = gn_request_id
	   AND status = gv_processed_status;

	SELECT count(1)
	  INTO ln_total_validation_count
	 FROM SLC_ISP_FRANCONNNECT_SUPP_STG
	 WHERE request_id = gn_request_id
	   AND status = gv_valid_status;
	   
	SELECT count(1)
	  INTO ln_validation_fail_count
	 FROM SLC_ISP_FRANCONNNECT_SUPP_STG
	 WHERE request_id = gn_request_id
	   AND status = gv_invalid_status;

	SELECT count(1)
	  INTO ln_import_fail_count
	 FROM SLC_ISP_FRANCONNNECT_SUPP_STG
	 WHERE request_id = gn_request_id
	   AND status = gv_error_status;
	   
	slc_write_log_p(gv_out,'****************Output******************');
	slc_write_log_p(gv_out,'Total Records:'||ln_total_count);
	slc_write_log_p(gv_out,'Total Records Validated:'||ln_total_validation_count);
	slc_write_log_p(gv_out,'Total Records failed validation:'||ln_validation_fail_count);
	slc_write_log_p(gv_out,'Total Records failed import:'||ln_import_fail_count);
	slc_write_log_p(gv_out,'Total Records imported:'||ln_total_success_count);
		   
		slc_write_log_p(gv_out,'***************************************************');
		slc_write_log_p(gv_out,rpad('Record Id',25,' ')||'Error Message');
		OPEN cur_err_rec(gv_invalid_status,gv_error_status);
		LOOP
		FETCH cur_err_rec INTO lc_cur_err_rec;
		EXIT WHEN cur_err_rec%NOTFOUND;
		slc_write_log_p(gv_out,rpad(lc_cur_err_rec.record_id,25,' ')||'Store Number:'||lc_cur_err_rec.store_number||
								lc_cur_err_rec.store_letter_code||' '||lc_cur_err_rec.error_msg);
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
		
		IF ln_total_success_count = ln_total_count THEN
			gn_program_status := 0;
		ELSIF ln_total_success_count < ln_total_count AND ln_total_success_count <> 0 THEN
			gn_program_status := 1;
		ELSE
			gn_program_status := 2;
		END IF;

	   SELECT COUNT(1)
		INTO ln_total_fran_count
		FROM
		  (SELECT incorp_party_id
		  FROM SLC_ISP_FRANCONNNECT_SUPP_STG
		  WHERE request_id     = gn_request_id
		  AND status           = gv_processed_status
		  AND incorp_party_id IS NOT NULL
		  UNION
		  SELECT franchisee1_party_id
		  FROM SLC_ISP_FRANCONNNECT_SUPP_STG
		  WHERE request_id          = gn_request_id
		  AND status                = gv_processed_status
		  AND franchisee1_party_id IS NOT NULL
		  UNION
		  SELECT franchisee2_party_id
		  FROM SLC_ISP_FRANCONNNECT_SUPP_STG
		  WHERE request_id          = gn_request_id
		  AND status                = gv_processed_status
		  AND franchisee2_party_id IS NOT NULL
		  UNION
		  SELECT franchisee3_party_id
		  FROM SLC_ISP_FRANCONNNECT_SUPP_STG
		  WHERE request_id          = gn_request_id
		  AND status                = gv_processed_status
		  AND franchisee3_party_id IS NOT NULL
		  UNION
		  SELECT franchisee4_party_id
		  FROM SLC_ISP_FRANCONNNECT_SUPP_STG
		  WHERE request_id          = gn_request_id
		  AND status                = gv_processed_status
		  AND franchisee4_party_id IS NOT NULL		  
		  );
		  
	SELECT count(distinct(store_number))
	  INTO ln_total_site_count
	  FROM SLC_ISP_FRANCONNNECT_SUPP_STG	
	  WHERE request_id = gn_request_id
	   AND status = gv_processed_status;	  	   
	   
	slc_write_log_p(gv_out,'Total Supplier created:'||ln_total_fran_count);
	slc_write_log_p(gv_out,'Total Supplier Site created:'||ln_total_site_count);
	
   SLC_UTIL_JOBS_PKG.SLC_UTIL_E_LOG_SUMMARY_P(
							P_BATCH_KEY => gv_batch_key,
							P_BUSINESS_PROCESS_NAME => gv_business_process_name,
							P_TOTAL_RECORDS => ln_total_count,
							P_TOTAL_SUCCESS_RECORDS => ln_total_success_count,
							P_TOTAL_FAILCUSTVAL_RECORDS => ln_validation_fail_count,
							P_TOTAL_FAILSTDVAL_RECORDS => ln_import_fail_count,
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
		NAME:              slc_process_franchisee_data_p
		PURPOSE:           This procedure will be called from SOA service to create Supplier and Supplier Sites
		Input Parameters:  p_in_supplier_data_tab							IN SLC_ISPINF_FRANCONNNECT_TAB
		Version				1.1			Changes as per new design.
	*****************************************************************/
	 PROCEDURE slc_process_franchisee_data_p( p_in_soa_instance_id	  IN VARCHAR2
											  ,p_in_supplier_data_tab IN  SLC_ISPINF_FRANCONNNECT_TAB
											  ,p_in_supplier_data_resp_tab OUT  SLC_ISPINF_FRANCON_RESP_TAB
											 )
	 IS
	 l_incorp_flag   SLC_ISP_FRANCONNNECT_SUPP_STG.INCORP_FLAG%TYPE ;
	 
	 CURSOR cur_rec_status(p_in_reference_id IN NUMBER)
	 IS
	 SELECT *
	   FROM slc_isp_franconnnect_supp_stg
	  WHERE record_id = (SELECT max(record_id) FROM slc_isp_franconnnect_supp_stg WHERE reference_id = p_in_reference_id);
	 l_row_franconnect		slc_isp_franconnnect_supp_stg%ROWTYPE;
	 
	lc_supplier_resp_tab 	SLC_ISPINF_FRANCON_RESP_TAB := SLC_ISPINF_FRANCON_RESP_TAB();
	lc_supplier_resp_obj  	SLC_ISPINF_FRANCON_RESP_OBJ ;
	ln_count				NUMBER DEFAULT 1;
	lv_record_status		VARCHAR2(50);
	ln_record_id			slc_isp_franconnnect_supp_stg.record_id%TYPE;
	 
	 BEGIN
	 FOR ln_index IN 1..p_in_supplier_data_tab.COUNT
	 LOOP
	 BEGIN
		--As per new design from FranConnect we will pick NOT PROCESSED and EXTRACTED records.
		-- In FranConnect we will have following values.
		-- 1. NOT PROCESSED
		-- 2. EXTRACTED
		-- 3. ERROR STAGING
		-- 4. ERROR ORACLE INTERFACE
		-- 5. IMPORTED IN ORACLE
		
		--For NOT PROCESSED records picked from Franconnect , records will be inserted into Oracle Staging table and at the same time
		--status in Franconnect would be marked as EXTRACTED.
		--For Extracted records based on the reference id of the record picked and latest status in Oracle Staging table status was be
		--updated back in staging table.
		--If record has failed in staging table due to validation failure it would be updated as ERROR STAGING in Franconnect
		--If record has failed in staging table due to import failure it would be updated as ERROR ORACLE INTERFACE in Franconnect
		--If record has processed in oracle it would be updated as IMPORTED IN ORACLE in Franconnect
		IF p_in_supplier_data_tab(ln_index).fran_status = gv_extracted THEN
		 OPEN cur_rec_status(p_in_supplier_data_tab(ln_index).reference_id);
		 FETCH cur_rec_status INTO l_row_franconnect;
		 CLOSE cur_rec_status;

			IF l_row_franconnect.status IS NOT NULL THEN
				IF  l_row_franconnect.status = 'P' THEN
				  lv_record_status := gv_imported;
				ELSIF l_row_franconnect.status = 'F' THEN
				  lv_record_status := gv_error_staging ;
				ELSIF l_row_franconnect.status = 'E' THEN
				   lv_record_status := gv_error_oracle;
				ELSIF l_row_franconnect.status = 'N' THEN
				   lv_record_status := gv_extracted;
				END IF;
				 lc_supplier_resp_obj := SLC_ISPINF_FRANCON_RESP_OBJ(l_row_franconnect.record_id
																	,l_row_franconnect.batch_id
																	,l_row_franconnect.soa_instance_id
																	,l_row_franconnect.reference_id
																	,lv_record_status
																	);	
			END IF;
		ELSIF p_in_supplier_data_tab(ln_index).fran_status = gv_not_processed THEN

			ln_record_id := SLC_ISP_FRANCONN_RECORD_ID_S.NEXTVAL;																
			 IF p_in_supplier_data_tab(ln_index).INCORP_NAME IS NULL
			 THEN
				l_incorp_flag := 'N';
			 ELSE
				l_incorp_flag := 'Y';
			 END IF;
		   INSERT 
		   INTO SLC_ISP_FRANCONNNECT_SUPP_STG
			 (
			  RECORD_ID                
			 ,BATCH_ID                  
			 ,SOA_INSTANCE_ID           
			 ,REFERENCE_ID              
			 ,STORE_NUMBER              
			 ,STORE_LETTER_CODE         
			 ,STATUS   
			 ,FRAN_STATUS
			 ,INCORP_FLAG               
			 ,INCORP_NAME 
			 ,INCORP_TYPE
			 ,FEDERAL_ID                
			 ,FRANCHISEE1_SSN           
			 ,FRANCHISEE2_SSN           
			 ,FRANCHISEE3_SSN           
			 ,FRANCHISEE4_SSN           
			 ,INCORP_PARTY_ID           
			 ,INCORP_VENDOR_ID          
			 ,FRANCHISEE1_PARTY_ID      
			 ,FRANCHISEE2_PARTY_ID      
			 ,FRANCHISEE3_PARTY_ID      
			 ,FRANCHISEE4_PARTY_ID      
			 ,FRANCHISEE1_VENDOR_ID     
			 ,FRANCHISEE2_VENDOR_ID     
			 ,FRANCHISEE3_VENDOR_ID     
			 ,FRANCHISEE4_VENDOR_ID     
			 ,FRANCHISEE1_FIRST_NAME    
			 ,FRANCHISEE1_MIDDLE_NAME   
			 ,FRANCHISEE1_LAST_NAME     
			 ,FRANCHISEE1_TITLE         
			 ,FRANCHISEE1_OWNERSHIP     
			 ,FRANCHISEE1_NO_OF_SHARES  
			 ,FRANCHISEE1_BIRTH_DATE    
			 ,FRANCHISEE1_BKGRD         
			 ,FRANCHISEE1_MARITAL       
			 ,FRANCHISEE1_SEX           
			 ,FRANCHISEE1_EMAIL         
			 ,FRANCHISEE1_VETERAN 
			 ,FRANCHISEE1_PHONE_NUMBER
			 ,FRANCHISEE2_FIRST_NAME    
			 ,FRANCHISEE2_MIDDLE_NAME   
			 ,FRANCHISEE2_LAST_NAME     
			 ,FRANCHISEE2_TITLE         
			 ,FRANCHISEE2_OWNERSHIP     
			 ,FRANCHISEE2_NO_OF_SHARES  
			 ,FRANCHISEE2_BIRTH_DATE    
			 ,FRANCHISEE2_BKGRD         
			 ,FRANCHISEE2_MARITAL       
			 ,FRANCHISEE2_SEX           
			 ,FRANCHISEE2_EMAIL         
			 ,FRANCHISEE2_VETERAN 
			 ,FRANCHISEE2_PHONE_NUMBER		 
			 ,FRANCHISEE3_FIRST_NAME    
			 ,FRANCHISEE3_MIDDLE_NAME   
			 ,FRANCHISEE3_LAST_NAME     
			 ,FRANCHISEE3_TITLE         
			 ,FRANCHISEE3_OWNERSHIP     
			 ,FRANCHISEE3_NO_OF_SHARES  
			 ,FRANCHISEE3_BIRTH_DATE    
			 ,FRANCHISEE3_BKGRD         
			 ,FRANCHISEE3_MARITAL       
			 ,FRANCHISEE3_SEX           
			 ,FRANCHISEE3_EMAIL         
			 ,FRANCHISEE3_VETERAN   
			 ,FRANCHISEE3_PHONE_NUMBER	
			 ,FRANCHISEE4_FIRST_NAME    
			 ,FRANCHISEE4_MIDDLE_NAME   
			 ,FRANCHISEE4_LAST_NAME     
			 ,FRANCHISEE4_TITLE         
			 ,FRANCHISEE4_OWNERSHIP     
			 ,FRANCHISEE4_NO_OF_SHARES  
			 ,FRANCHISEE4_BIRTH_DATE    
			 ,FRANCHISEE4_BKGRD         
			 ,FRANCHISEE4_MARITAL       
			 ,FRANCHISEE4_SEX           
			 ,FRANCHISEE4_EMAIL         
			 ,FRANCHISEE4_VETERAN  
			 ,FRANCHISEE4_PHONE_NUMBER
			 ,MIN_SHARE1_FIRST_NAME     
			 ,MIN_SHARE1_MIDDLE_NAME    
			 ,MIN_SHARE1_LAST_NAME      
			 ,MIN_SHARE1_EMAIL          
			 ,MIN_SHARE1_TITLE          
			 ,MIN_SHARE1_OWNERSHIP      
			 ,MIN_SHARE1_NO_OF_SHARES   
			 ,MIN_SHARE2_FIRST_NAME     
			 ,MIN_SHARE2_MIDDLE_NAME    
			 ,MIN_SHARE2_LAST_NAME      
			 ,MIN_SHARE2_EMAIL          
			 ,MIN_SHARE2_TITLE          
			 ,MIN_SHARE2_OWNERSHIP      
			 ,MIN_SHARE2_NO_OF_SHARES   
			 ,MIN_SHARE3_FIRST_NAME     
			 ,MIN_SHARE3_MIDDLE_NAME    
			 ,MIN_SHARE3_LAST_NAME      
			 ,MIN_SHARE3_EMAIL          
			 ,MIN_SHARE3_TITLE          
			 ,MIN_SHARE3_OWNERSHIP      
			 ,MIN_SHARE3_NO_OF_SHARES   
			 ,MIN_SHARE4_FIRST_NAME     
			 ,MIN_SHARE4_MIDDLE_NAME    
			 ,MIN_SHARE4_LAST_NAME      
			 ,MIN_SHARE4_EMAIL          
			 ,MIN_SHARE4_TITLE          
			 ,MIN_SHARE4_OWNERSHIP      
			 ,MIN_SHARE4_NO_OF_SHARES   
			 ,FRAN1_ADDRESS_LINE1       
			 ,FRAN1_ADDRESS_LINE2       
			 ,CITY1                     
			 ,ZIP1                      
			 ,STATE1                    
			 ,COUNTRY1                  
			 ,FRAN2_ADDRESS_LINE1       
			 ,FRAN2_ADDRESS_LINE2       
			 ,CITY2                     
			 ,ZIP2                      
			 ,STATE2                    
			 ,COUNTRY2                  
			 ,FRAN3_ADDRESS_LINE1       
			 ,FRAN3_ADDRESS_LINE2       
			 ,CITY3                     
			 ,ZIP3                      
			 ,STATE3                    
			 ,COUNTRY3                  
			 ,FRAN4_ADDRESS_LINE1       
			 ,FRAN4_ADDRESS_LINE2       
			 ,CITY4                     
			 ,ZIP4                      
			 ,STATE4                    
			 ,COUNTRY4                  
			 ,ERROR_MSG 
			 ,DESIG1_NAME					
			 ,DESIG1_ADDRESS				 
			 ,DESIG1_REL_TO_FRAN				
			 ,DESIG2_NAME					
			 ,DESIG2_ADDRESS				
			 ,DESIG2_REL_TO_FRAN				
			 ,DESIG3_NAME					
			 ,DESIG3_ADDRESS				
			 ,DESIG3_REL_TO_FRAN				
			 ,REQUEST_ID                
			 ,CREATION_DATE             
			 ,CREATED_BY                
			 ,LAST_UPDATE_DATE          
			 ,LAST_UPDATED_BY           
			 ,LAST_UPDATE_LOGIN ) 
			 VALUES 
			 ( 
		    ln_record_id               
            ,p_in_supplier_data_tab(ln_index).BATCH_ID                 
            ,p_in_soa_instance_id			
            ,p_in_supplier_data_tab(ln_index).REFERENCE_ID			
            ,p_in_supplier_data_tab(ln_index).STORE_NUMBER	       		
            ,p_in_supplier_data_tab(ln_index).STORE_LETTER_CODE	      
            ,p_in_supplier_data_tab(ln_index).STATUS  
			,p_in_supplier_data_tab(ln_index).FRAN_STATUS  			
            ,l_incorp_flag			
            ,p_in_supplier_data_tab(ln_index).INCORP_NAME	
			,p_in_supplier_data_tab(ln_index).INCORP_TYPE	
            ,p_in_supplier_data_tab(ln_index).FEDERAL_ID				
            ,p_in_supplier_data_tab(ln_index).FRANCHISEE1_SSN		  
            ,p_in_supplier_data_tab(ln_index).FRANCHISEE2_SSN		  
            ,p_in_supplier_data_tab(ln_index).FRANCHISEE3_SSN		  
            ,p_in_supplier_data_tab(ln_index).FRANCHISEE4_SSN		  
            ,p_in_supplier_data_tab(ln_index).INCORP_PARTY_ID			
            ,p_in_supplier_data_tab(ln_index).INCORP_VENDOR_ID			
            ,p_in_supplier_data_tab(ln_index).FRANCHISEE1_PARTY_ID		
            ,p_in_supplier_data_tab(ln_index).FRANCHISEE2_PARTY_ID		
            ,p_in_supplier_data_tab(ln_index).FRANCHISEE3_PARTY_ID		
            ,p_in_supplier_data_tab(ln_index).FRANCHISEE4_PARTY_ID		
            ,p_in_supplier_data_tab(ln_index).FRANCHISEE1_VENDOR_ID		
            ,p_in_supplier_data_tab(ln_index).FRANCHISEE2_VENDOR_ID		
            ,p_in_supplier_data_tab(ln_index).FRANCHISEE3_VENDOR_ID		
            ,p_in_supplier_data_tab(ln_index).FRANCHISEE4_VENDOR_ID		
            ,p_in_supplier_data_tab(ln_index).FRANCHISEE1_FIRST_NAME	  
            ,p_in_supplier_data_tab(ln_index).FRANCHISEE1_MIDDLE_NAME	
            ,p_in_supplier_data_tab(ln_index).FRANCHISEE1_LAST_NAME	  
            ,p_in_supplier_data_tab(ln_index).FRANCHISEE1_TITLE	      
            ,p_in_supplier_data_tab(ln_index).FRANCHISEE1_OWNERSHIP	  
            ,p_in_supplier_data_tab(ln_index).FRANCHISEE1_NO_OF_SHARES 
            ,p_in_supplier_data_tab(ln_index).FRANCHISEE1_BIRTH_DATE   
            ,p_in_supplier_data_tab(ln_index).FRANCHISEE1_BKGRD		  
            ,p_in_supplier_data_tab(ln_index).FRANCHISEE1_MARITAL		
            ,p_in_supplier_data_tab(ln_index).FRANCHISEE1_SEX			
            ,p_in_supplier_data_tab(ln_index).FRANCHISEE1_EMAIL			
            ,p_in_supplier_data_tab(ln_index).FRANCHISEE1_VETERAN	
			,p_in_supplier_data_tab(ln_index).FRANCHISEE1_PHONE_NUMBER	
            ,p_in_supplier_data_tab(ln_index).FRANCHISEE2_FIRST_NAME	  
            ,p_in_supplier_data_tab(ln_index).FRANCHISEE2_MIDDLE_NAME	
            ,p_in_supplier_data_tab(ln_index).FRANCHISEE2_LAST_NAME	  
            ,p_in_supplier_data_tab(ln_index).FRANCHISEE2_TITLE	      
            ,p_in_supplier_data_tab(ln_index).FRANCHISEE2_OWNERSHIP	  
            ,p_in_supplier_data_tab(ln_index).FRANCHISEE2_NO_OF_SHARES 
            ,p_in_supplier_data_tab(ln_index).FRANCHISEE2_BIRTH_DATE   
            ,p_in_supplier_data_tab(ln_index).FRANCHISEE2_BKGRD		  
            ,p_in_supplier_data_tab(ln_index).FRANCHISEE2_MARITAL		
            ,p_in_supplier_data_tab(ln_index).FRANCHISEE2_SEX			
            ,p_in_supplier_data_tab(ln_index).FRANCHISEE2_EMAIL			
            ,p_in_supplier_data_tab(ln_index).FRANCHISEE2_VETERAN
			,p_in_supplier_data_tab(ln_index).FRANCHISEE2_PHONE_NUMBER	
            ,p_in_supplier_data_tab(ln_index).FRANCHISEE3_FIRST_NAME	  
            ,p_in_supplier_data_tab(ln_index).FRANCHISEE3_MIDDLE_NAME	
            ,p_in_supplier_data_tab(ln_index).FRANCHISEE3_LAST_NAME	  
            ,p_in_supplier_data_tab(ln_index).FRANCHISEE3_TITLE	      
            ,p_in_supplier_data_tab(ln_index).FRANCHISEE3_OWNERSHIP	  
            ,p_in_supplier_data_tab(ln_index).FRANCHISEE3_NO_OF_SHARES 
            ,p_in_supplier_data_tab(ln_index).FRANCHISEE3_BIRTH_DATE   
            ,p_in_supplier_data_tab(ln_index).FRANCHISEE3_BKGRD		  
            ,p_in_supplier_data_tab(ln_index).FRANCHISEE3_MARITAL		
            ,p_in_supplier_data_tab(ln_index).FRANCHISEE3_SEX			
            ,p_in_supplier_data_tab(ln_index).FRANCHISEE3_EMAIL			
            ,p_in_supplier_data_tab(ln_index).FRANCHISEE3_VETERAN	
			,p_in_supplier_data_tab(ln_index).FRANCHISEE3_PHONE_NUMBER
            ,p_in_supplier_data_tab(ln_index).FRANCHISEE4_FIRST_NAME	  
            ,p_in_supplier_data_tab(ln_index).FRANCHISEE4_MIDDLE_NAME	
            ,p_in_supplier_data_tab(ln_index).FRANCHISEE4_LAST_NAME	  
            ,p_in_supplier_data_tab(ln_index).FRANCHISEE4_TITLE	      
            ,p_in_supplier_data_tab(ln_index).FRANCHISEE4_OWNERSHIP	  
            ,p_in_supplier_data_tab(ln_index).FRANCHISEE4_NO_OF_SHARES 
            ,p_in_supplier_data_tab(ln_index).FRANCHISEE4_BIRTH_DATE   
            ,p_in_supplier_data_tab(ln_index).FRANCHISEE4_BKGRD		  
            ,p_in_supplier_data_tab(ln_index).FRANCHISEE4_MARITAL		
            ,p_in_supplier_data_tab(ln_index).FRANCHISEE4_SEX			
            ,p_in_supplier_data_tab(ln_index).FRANCHISEE4_EMAIL			
            ,p_in_supplier_data_tab(ln_index).FRANCHISEE4_VETERAN	
			,p_in_supplier_data_tab(ln_index).FRANCHISEE4_PHONE_NUMBER			
            ,p_in_supplier_data_tab(ln_index).MIN_SHARE1_FIRST_NAME		
            ,p_in_supplier_data_tab(ln_index).MIN_SHARE1_MIDDLE_NAME		
            ,p_in_supplier_data_tab(ln_index).MIN_SHARE1_LAST_NAME		
            ,p_in_supplier_data_tab(ln_index).MIN_SHARE1_EMAIL			
            ,p_in_supplier_data_tab(ln_index).MIN_SHARE1_TITLE			
            ,p_in_supplier_data_tab(ln_index).MIN_SHARE1_OWNERSHIP		
            ,p_in_supplier_data_tab(ln_index).MIN_SHARE1_NO_OF_SHARES	
            ,p_in_supplier_data_tab(ln_index).MIN_SHARE2_FIRST_NAME		
            ,p_in_supplier_data_tab(ln_index).MIN_SHARE2_MIDDLE_NAME		
            ,p_in_supplier_data_tab(ln_index).MIN_SHARE2_LAST_NAME		
            ,p_in_supplier_data_tab(ln_index).MIN_SHARE2_EMAIL			
            ,p_in_supplier_data_tab(ln_index).MIN_SHARE2_TITLE			
            ,p_in_supplier_data_tab(ln_index).MIN_SHARE2_OWNERSHIP		
            ,p_in_supplier_data_tab(ln_index).MIN_SHARE2_NO_OF_SHARES	
            ,p_in_supplier_data_tab(ln_index).MIN_SHARE3_FIRST_NAME		
            ,p_in_supplier_data_tab(ln_index).MIN_SHARE3_MIDDLE_NAME		
            ,p_in_supplier_data_tab(ln_index).MIN_SHARE3_LAST_NAME		
            ,p_in_supplier_data_tab(ln_index).MIN_SHARE3_EMAIL			
            ,p_in_supplier_data_tab(ln_index).MIN_SHARE3_TITLE			
            ,p_in_supplier_data_tab(ln_index).MIN_SHARE3_OWNERSHIP		
            ,p_in_supplier_data_tab(ln_index).MIN_SHARE3_NO_OF_SHARES	
            ,p_in_supplier_data_tab(ln_index).MIN_SHARE4_FIRST_NAME		
            ,p_in_supplier_data_tab(ln_index).MIN_SHARE4_MIDDLE_NAME		
            ,p_in_supplier_data_tab(ln_index).MIN_SHARE4_LAST_NAME		
            ,p_in_supplier_data_tab(ln_index).MIN_SHARE4_EMAIL			
            ,p_in_supplier_data_tab(ln_index).MIN_SHARE4_TITLE			
            ,p_in_supplier_data_tab(ln_index).MIN_SHARE4_OWNERSHIP		
            ,p_in_supplier_data_tab(ln_index).MIN_SHARE4_NO_OF_SHARES	
            ,p_in_supplier_data_tab(ln_index).FRAN1_ADDRESS_LINE1		
            ,p_in_supplier_data_tab(ln_index).FRAN1_ADDRESS_LINE2		
            ,p_in_supplier_data_tab(ln_index).CITY1                    
            ,p_in_supplier_data_tab(ln_index).ZIP1                     
            ,p_in_supplier_data_tab(ln_index).STATE1                   
            ,p_in_supplier_data_tab(ln_index).COUNTRY1                 
            ,p_in_supplier_data_tab(ln_index).FRAN2_ADDRESS_LINE1		
            ,p_in_supplier_data_tab(ln_index).FRAN2_ADDRESS_LINE2		
            ,p_in_supplier_data_tab(ln_index).CITY2                    
            ,p_in_supplier_data_tab(ln_index).ZIP2                     
            ,p_in_supplier_data_tab(ln_index).STATE2                   
            ,p_in_supplier_data_tab(ln_index).COUNTRY2                 
            ,p_in_supplier_data_tab(ln_index).FRAN3_ADDRESS_LINE1		
            ,p_in_supplier_data_tab(ln_index).FRAN3_ADDRESS_LINE2		
            ,p_in_supplier_data_tab(ln_index).CITY3                    
            ,p_in_supplier_data_tab(ln_index).ZIP3                     
            ,p_in_supplier_data_tab(ln_index).STATE3                   
            ,p_in_supplier_data_tab(ln_index).COUNTRY3                 
            ,p_in_supplier_data_tab(ln_index).FRAN4_ADDRESS_LINE1		
            ,p_in_supplier_data_tab(ln_index).FRAN4_ADDRESS_LINE2		
            ,p_in_supplier_data_tab(ln_index).CITY4                    
            ,p_in_supplier_data_tab(ln_index).ZIP4                     
            ,p_in_supplier_data_tab(ln_index).STATE4                   
            ,p_in_supplier_data_tab(ln_index).COUNTRY4                 
            ,p_in_supplier_data_tab(ln_index).ERROR_MSG   
			,p_in_supplier_data_tab(ln_index).DESIG1_NAME         			
			,p_in_supplier_data_tab(ln_index).DESIG1_ADDRESS 
			,p_in_supplier_data_tab(ln_index).DESIG1_REL_TO_FRAN 
			,p_in_supplier_data_tab(ln_index).DESIG2_NAME 
			,p_in_supplier_data_tab(ln_index).DESIG2_ADDRESS 
			,p_in_supplier_data_tab(ln_index).DESIG2_REL_TO_FRAN 
			,p_in_supplier_data_tab(ln_index).DESIG3_NAME 
			,p_in_supplier_data_tab(ln_index).DESIG3_ADDRESS 
			,p_in_supplier_data_tab(ln_index).DESIG3_REL_TO_FRAN 
            ,p_in_supplier_data_tab(ln_index).REQUEST_ID				 
            ,sysdate            
            ,p_in_supplier_data_tab(ln_index).CREATED_BY               
            ,p_in_supplier_data_tab(ln_index).LAST_UPDATE_DATE         
            ,p_in_supplier_data_tab(ln_index).LAST_UPDATED_BY          
            ,p_in_supplier_data_tab(ln_index).LAST_UPDATE_LOGIN        
			); 
			
			 lv_record_status := gv_extracted;
			 lc_supplier_resp_obj := SLC_ISPINF_FRANCON_RESP_OBJ(ln_record_id
																,NULL
																,p_in_supplier_data_tab(ln_index).soa_instance_id
																,p_in_supplier_data_tab(ln_index).REFERENCE_ID
																,lv_record_status
																);	
		
		END IF;
		lc_supplier_resp_tab.extend;
		lc_supplier_resp_tab(ln_count) := lc_supplier_resp_obj;
		ln_count := ln_count + 1;
		EXCEPTION
		WHEN OTHERS THEN
		slc_write_log_p(gv_log,'Error while inserting data in Staging table from SOA :'||SQLERRM);
		END;
	 END LOOP;
	 p_in_supplier_data_resp_tab := lc_supplier_resp_tab;
	 EXCEPTION
	 WHEN OTHERS 
	 THEN
	   slc_write_log_p(gv_log,'Error while inserting data in Staging table from SOA :'||SQLERRM);
	 END slc_process_franchisee_data_p;


	 
/*	*****************************************************************
	NAME:              slc_validate_relationship_f
	PURPOSE:           This function will validate if relationship is valid.
*****************************************************************/

  FUNCTION slc_validate_relationship_f(p_in_corp_party_id    IN  NUMBER
									  ,p_in_franc1_party_id	 IN  NUMBER
									  ,p_in_franc2_party_id	 IN  NUMBER
									  ,p_in_franc3_party_id	 IN  NUMBER
									  ,p_in_franc4_party_id	 IN  NUMBER
									  ) RETURN VARCHAR2
  IS
  
  CURSOR cur_valid_relation
  IS
  SELECT count(1)
  FROM hz_relationships
  WHERE subject_id = p_in_corp_party_id
    AND relationship_code = 'PARTNER_OF';
  
  ln_total_rel_count	NUMBER DEFAULT 0;
  ln_total_count	NUMBER DEFAULT 0;
  
  lv_relationship_valid_flag	VARCHAR2(1) DEFAULT 'N';
  
  CURSOR cur_existing_relation
  IS
  SELECT count(1)
  FROM hz_relationships
  WHERE subject_id = p_in_corp_party_id
    AND relationship_code = 'PARTNER_OF'
	AND object_id IN (p_in_franc1_party_id,p_in_franc2_party_id,p_in_franc3_party_id,p_in_franc4_party_id);
  ln_total_existing_rel_count	NUMBER DEFAULT 0;
	
  BEGIN
	slc_write_log_p(gv_log,'In slc_validate_relationship_f p_in_franc1_party_id:'||p_in_franc1_party_id||
							' p_in_franc2_party_id:'||p_in_franc2_party_id||
							' p_in_franc3_party_id:'||p_in_franc3_party_id||
							' p_in_franc4_party_id:'||p_in_franc4_party_id||
							' p_in_corp_party_id:'||p_in_corp_party_id);
	IF p_in_franc1_party_id IS NOT NULL THEN
		ln_total_count := ln_total_count + 1;
	END IF;
	IF p_in_franc2_party_id IS NOT NULL THEN
		ln_total_count := ln_total_count + 1;
	END IF;
	IF p_in_franc3_party_id IS NOT NULL THEN
		ln_total_count := ln_total_count + 1;
	END IF;
	IF p_in_franc4_party_id IS NOT NULL THEN
		ln_total_count := ln_total_count + 1;
	END IF;	
	
	OPEN cur_valid_relation;
	FETCH cur_valid_relation INTO ln_total_rel_count;
	CLOSE cur_valid_relation;
	slc_write_log_p(gv_log,'');
	slc_write_log_p(gv_log,'In slc_validate_relationship_f ln_total_count:'||ln_total_count||' ln_total_rel_count:'||ln_total_rel_count);
	
	IF ln_total_rel_count = 0 THEN
		lv_relationship_valid_flag := 'Y';
	ELSE
		IF ln_total_rel_count <> ln_total_count THEN
			lv_relationship_valid_flag := 'N';
		ELSE
			OPEN cur_existing_relation;
			FETCH cur_existing_relation INTO ln_total_existing_rel_count;
			CLOSE cur_existing_relation;
			slc_write_log_p(gv_log,'In slc_validate_relationship_f ln_total_existing_rel_count:'||ln_total_existing_rel_count);
			
			IF ln_total_rel_count <> ln_total_existing_rel_count THEN
				lv_relationship_valid_flag := 'N';
			ELSE
				lv_relationship_valid_flag := 'Y';
			END IF;
		END IF;
	END IF;
	slc_write_log_p(gv_log,'In slc_validate_relationship_f lv_relationship_valid_flag:'||lv_relationship_valid_flag);
	slc_write_log_p(gv_log,'');
	RETURN lv_relationship_valid_flag;
  END slc_validate_relationship_f;
 

	 
/*	*****************************************************************
	NAME:              slc_get_minority_count_f
	PURPOSE:           This function will validate if minority Contact already exists.
*****************************************************************/ 
  FUNCTION slc_get_minority_count_f (p_in_vendor_id	 	NUMBER
											,p_in_person_first_name 	    VARCHAR2
											,p_in_person_middle_name 		VARCHAR2
											,p_in_person_last_name 			VARCHAR2)
  RETURN NUMBER
  IS ln_min_count  NUMBER DEFAULT 0;
  BEGIN
   slc_write_log_p(gv_log,'');
   slc_write_log_p(gv_log,'In slc_get_minority_count_f p_in_vendor_id :'||p_in_vendor_id);
         SELECT COUNT(1)
         INTO ln_min_count
         FROM ap_suppliers aps ,
           hz_parties hp,
           hz_parties hpc,
           hz_relationships hr
	--Changes for v1.5. Comparing names without considering case.
	--Making First Name,Middle Name and Last name as upper case before comparing.		   
         WHERE UPPER(NVL(hpc.person_first_name,'X')) = UPPER(NVL(p_in_person_first_name,'X'))
         AND UPPER(NVL(hpc.person_middle_name,'X'))  = UPPER(NVL(p_in_person_middle_name,'X'))
         AND UPPER(NVL(hpc.person_last_name,'X'))    = UPPER(NVL(p_in_person_last_name,'X'))
         AND hpc.party_id                     =hr.object_id
         AND hr.relationship_code             = 'CONTACT'
         AND hr.subject_id= hp.party_id
         AND hp.party_id  =aps.party_id
         AND aps.vendor_id= p_in_vendor_id;
		 
		 slc_write_log_p(gv_log,'In slc_get_minority_count_f ln_min_count :'||ln_min_count);
		 slc_write_log_p(gv_log,'');
		 RETURN ln_min_count;
	EXCEPTION
	WHEN OTHERS
	THEN
	 ln_min_count :=-9;
	 RETURN ln_min_count;
	   
  END slc_get_minority_count_f;

 /*	*****************************************************************
	NAME:              slc_end_relationship_p
	PURPOSE:           This procedure updates relationship between suppliers
	Input Parameters:  p_in_relationship_id 	IN 		NUMBER
					   p_in_object_version_number	IN 	NUMBER
					   p_in_party_object_version_number	IN 	NUMBER
 *****************************************************************/
 PROCEDURE slc_end_relationship_p(p_in_relationship_id 			IN 		NUMBER
								 ,p_in_end_date 			IN 		DATE
								 ,p_in_obj_version_number	IN 	NUMBER
								 ,p_in_party_obj_version_number	IN 	NUMBER
								  ,p_out_error_flag OUT VARCHAR2
								  ,p_out_err_msg	OUT VARCHAR2									 
								  )
 IS
	p_init_msg_list VARCHAR2(200);
	p_relationship_rec apps.hz_relationship_v2pub.relationship_rec_type;
	ln_object_version_number hz_relationships.object_version_number%type;
	ln_party_object_version_number hz_parties.object_version_number%type;

	lv_error_flag		VARCHAR2(1) DEFAULT 'N';
	lv_error_msg		VARCHAR2(4000) DEFAULT NULL;
	l_msg_count			NUMBER;
	l_msg_data			VARCHAR2(4000);   
	lv_msg                  VARCHAR2(4000);
	lv_msg_out  NUMBER;	
	l_return_status		VARCHAR2(10);   

 BEGIN
  slc_write_log_p(gv_log,'');
  slc_write_log_p(gv_log,'In slc_end_relationship_p p_in_relationship_id :'||p_in_relationship_id||
						 ' p_in_obj_version_number:'||p_in_obj_version_number||
						 ' p_in_party_obj_version_number:'||p_in_party_obj_version_number);
  p_relationship_rec.relationship_id := p_in_relationship_id;
  p_relationship_rec.end_date := p_in_end_date;
  ln_object_version_number := p_in_obj_version_number;
  ln_party_object_version_number := p_in_party_obj_version_number;
  FND_MSG_PUB.Initialize;
  hz_relationship_v2pub.update_relationship(
											p_init_msg_list => fnd_api.g_true,
											p_relationship_rec => p_relationship_rec,
											p_object_version_number => ln_object_version_number,
											p_party_object_version_number => ln_party_object_version_number,
											x_return_status => l_return_status,
											x_msg_count => l_msg_count,
											x_msg_data => l_msg_data
										  ); 

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
	p_out_error_flag := lv_error_flag;
	IF lv_error_flag = 'Y' THEN
		p_out_err_msg	:= 'Error while end dating Relationship. Error Message:'||lv_error_msg;
	END IF;	
	slc_write_log_p(gv_log,'In slc_end_relationship_p p_out_error_flag:'||p_out_error_flag||' p_out_err_msg:'||p_out_err_msg);
	slc_write_log_p(gv_log,'');
 EXCEPTION
 WHEN OTHERS THEN
  slc_write_log_p(gv_log,'Unexpected error in slc_end_relationship_p. Error Message:'||SQLERRM);
  p_out_error_flag := 'Y';
  p_out_err_msg	:= 'Unexpected error in slc_end_relationship_p. Error Message:'||SQLERRM;											  
 END slc_end_relationship_p;
 
/* ****************************************************************
	NAME:              slc_update_min_contact_dff_p
	PURPOSE:           This procedure will updates Minority Contact information
	Input Parameters:  
 *****************************************************************/	
 PROCEDURE 	slc_update_min_contact_dff_p(p_in_vendor_contact_id IN NUMBER
									,p_in_title					IN  VARCHAR2
									,p_in_ownership				IN  VARCHAR2
									,p_in_no_of_shares			IN  VARCHAR2
									,p_out_error_flag			OUT VARCHAR2
									,p_out_err_msg				OUT VARCHAR2  									
									)
 IS
 p_vendor_contact_rec apps.ap_vendor_pub_pkg.r_vendor_contact_rec_type;
	 P_VALIDATION_LEVEL NUMBER DEFAULT NULL;
	lv_error_flag		VARCHAR2(1) DEFAULT 'N';
	lv_error_msg		VARCHAR2(4000) DEFAULT NULL;
	l_msg_count			NUMBER;
	l_msg_data			VARCHAR2(4000);   
	lv_msg                  VARCHAR2(4000);
	lv_msg_out  NUMBER;	
	l_return_status		VARCHAR2(10); 
 BEGIN
    slc_write_log_p(gv_log,'');
	slc_write_log_p(gv_log,'In slc_update_min_contact_dff_p  p_in_vendor_contact_id:'||p_in_vendor_contact_id);
	
	p_vendor_contact_rec.vendor_contact_id := p_in_vendor_contact_id ;
	p_vendor_contact_rec.attribute1 := p_in_title;
	p_vendor_contact_rec.attribute2 := p_in_ownership;
	p_vendor_contact_rec.attribute3 := p_in_no_of_shares;
	FND_MSG_PUB.Initialize;
	ap_vendor_pub_pkg.update_vendor_contact_public(
		p_api_version => 1.0,
		p_init_msg_list => fnd_api.g_true,
		p_commit => fnd_api.g_false,
		p_validation_level => p_validation_level,
		p_vendor_contact_rec => p_vendor_contact_rec,
		x_return_status => l_return_status,
		x_msg_count => l_msg_count,
		x_msg_data => l_msg_data
	  );
	  
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
	p_out_error_flag := lv_error_flag;
	IF lv_error_flag = 'Y' THEN
		p_out_err_msg	:= 'Error while updating DFF for Minority Contact. Error Message:'||lv_error_msg;
	END IF;	
	slc_write_log_p(gv_log,'In slc_update_min_contact_dff_p p_out_error_flag:'||p_out_error_flag||' p_out_err_msg:'||p_out_err_msg);
	slc_write_log_p(gv_log,'');	
 EXCEPTION
 WHEN OTHERS THEN
  slc_write_log_p(gv_log,'Unexpected error in slc_update_min_contact_dff_p. Error Message:'||SQLERRM);
  p_out_error_flag := 'Y';
  p_out_err_msg	:= 'Unexpected error in slc_update_min_contact_dff_p. Error Message:'||SQLERRM;	 
 END slc_update_min_contact_dff_p;
/* ****************************************************************
	NAME:              slc_update_relationship_p
	PURPOSE:           This procedure will updates Minority Contact information
						party
	Input Parameters:  
						p_in_vendor_id				IN 	NUMBER
						p_in_person_first_name 	IN	VARCHAR2
						p_in_person_middle_name 	IN	VARCHAR2
						p_in_person_last_name 		IN	VARCHAR2
						p_in_title					IN  VARCHAR2
						p_in_ownership				IN  VARCHAR2
						p_in_no_of_shares			IN  VARCHAR2
						p_out_error_flag			OUT VARCHAR2
						p_out_err_msg				OUT VARCHAR2  	

 *****************************************************************/	
 PROCEDURE slc_update_relationship_p (p_in_vendor_id				IN 	NUMBER
										,p_in_person_first_name 	IN	VARCHAR2
										,p_in_person_middle_name 	IN	VARCHAR2
										,p_in_person_last_name 		IN	VARCHAR2
										,p_in_title					IN  VARCHAR2
										,p_in_ownership				IN  VARCHAR2
										,p_in_no_of_shares			IN  VARCHAR2
										,p_out_error_flag			OUT VARCHAR2
										,p_out_err_msg				OUT VARCHAR2  
									  )
 IS 
 CURSOR cur_existing_relation
 IS
 SELECT hr.relationship_id
       ,hr.object_version_number
       ,hpc.object_version_number
	   ,hr.end_date
	   ,asco.vendor_contact_id
         FROM ap_suppliers aps ,
           hz_parties hp,
           hz_parties hpp,
		   hz_parties hpc,
           hz_relationships hr,
		   ap_supplier_contacts asco
	--Changes for v1.5. Comparing names without considering case.
	--Making First Name,Middle Name and Last name as upper case before comparing.			   
         WHERE UPPER(NVL(hpp.person_first_name,'X')) = UPPER(NVL(p_in_person_first_name,'X'))
         AND UPPER(NVL(hpp.person_middle_name,'X'))  = UPPER(NVL(p_in_person_middle_name,'X'))
         AND UPPER(NVL(hpp.person_last_name,'X'))    = UPPER(NVL(p_in_person_last_name,'X'))
         AND hpp.party_id                     =hr.object_id
		 AND hpc.party_id                     =hr.party_id
         AND hr.relationship_code             = 'CONTACT'
         AND hr.subject_id= hp.party_id
         AND hp.party_id  =aps.party_id
         AND hr.relationship_id = asco.relationship_id
         AND hr.object_id = asco.per_party_id
         AND hr.party_id = asco.rel_party_id
         AND asco.inactive_date is null		 
         AND aps.vendor_id= p_in_vendor_id;
 ln_relationship_id				hz_relationships.relationship_id%TYPE;
 lv_object_version_number		hz_relationships.object_version_number%TYPE;
 lv_party_object_version_number	hz_parties.object_version_number%TYPE;
 ld_end_date					hz_relationships.end_date%TYPE;
 ln_vendor_contact_id			ap_supplier_contacts.vendor_contact_id%TYPE;
	lv_error_flag		VARCHAR2(1) DEFAULT 'N';
	lv_error_msg		VARCHAR2(4000) DEFAULT NULL;
	
 BEGIN
    slc_write_log_p(gv_log,'');
	slc_write_log_p(gv_log,'In slc_update_relationship_p p_in_vendor_id:'||p_in_vendor_id);
	OPEN cur_existing_relation;
	FETCH cur_existing_relation INTO ln_relationship_id,lv_object_version_number,
									 lv_party_object_version_number,ld_end_date,ln_vendor_contact_id;
	CLOSE cur_existing_relation;
	-- When date is null the default value saved in DB is 31/12/4712
	--Where as value for 01/JAN/4712
	--Call API to set end date value as null only if end date value is populated.
	IF NOT(to_char(ld_end_date,'YYYY') = to_char(FND_API.G_MISS_DATE,'YYYY')) THEN
		slc_end_relationship_p(ln_relationship_id 			
								,FND_API.G_MISS_DATE 			
								,lv_object_version_number	
								,lv_party_object_version_number	
							    ,lv_error_flag
							    ,lv_error_msg								 
								);
	END IF;
	
	IF lv_error_flag = 'N' THEN
		slc_update_min_contact_dff_p(ln_vendor_contact_id
									,p_in_title					
									,p_in_ownership				
									,p_in_no_of_shares			
									,lv_error_flag
									,lv_error_msg												
									);
	END IF;
	--FND_API.G_MISS_DATE
	p_out_error_flag := lv_error_flag;
	IF lv_error_flag = 'Y' THEN
		p_out_err_msg	:= lv_error_msg;
	END IF;	
	slc_write_log_p(gv_log,'In slc_update_relationship_p p_out_error_flag:'||p_out_error_flag||' p_out_err_msg:'||p_out_err_msg);
	slc_write_log_p(gv_log,'');
	
 EXCEPTION
 WHEN OTHERS THEN
  slc_write_log_p(gv_log,'Unexpected error in slc_update_relationship_p. Error Message:'||SQLERRM);
  p_out_error_flag := 'Y';
  p_out_err_msg	:= 'Unexpected error in slc_update_relationship_p. Error Message:'||SQLERRM;	
 END slc_update_relationship_p;
								  
								  
 /*	*****************************************************************
	NAME:              slc_create_relationship_p
	PURPOSE:           This procedure creates relationship between suppliers
	Input Parameters:  
						p_in_subject_id	 IN 	NUMBER
						p_in_object_id	 IN 	NUMBER
						p_in_title		 IN  VARCHAR2x
						p_in_ownership	 IN  VARCHAR2
						p_in_no_of_shares IN  VARCHAR2	
 *****************************************************************/
  PROCEDURE slc_create_relationship_p( p_in_relationship_type IN VARCHAR2
									  ,p_in_subject_id	 IN 	NUMBER
									  ,p_in_object_id	 IN 	NUMBER
									  ,p_in_title		 IN  VARCHAR2
									  ,p_in_ownership	 IN  VARCHAR2
									  ,p_in_no_of_shares IN  VARCHAR2
									  ,p_out_error_flag OUT VARCHAR2
									  ,p_out_err_msg	OUT VARCHAR2									  
									 )
 IS
   p_relationship_rec_type HZ_RELATIONSHIP_V2PUB.RELATIONSHIP_REC_TYPE;
   x_relationship_id NUMBER;
   x_party_id        NUMBER;
   x_party_number    VARCHAR2 ( 2000 ) ;
	lv_error_flag		VARCHAR2(1) DEFAULT 'N';
	lv_error_msg		VARCHAR2(4000) DEFAULT NULL;
	l_msg_count			NUMBER;
	l_msg_data			VARCHAR2(4000);   
	lv_msg                  VARCHAR2(4000);
	lv_msg_out  NUMBER;	
	l_return_status		VARCHAR2(10); 
	
	CURSOR cur_relationship
	IS
	SELECT forward_rel_code
	  FROM hz_relationship_types
	 WHERE relationship_type = p_in_relationship_type;
	 lv_forward_rel_code  	hz_relationship_types.forward_rel_code%TYPE;
 BEGIN
   
   OPEN cur_relationship;
   FETCH cur_relationship INTO lv_forward_rel_code;
   CLOSE cur_relationship;
   slc_write_log_p(gv_log,'');
   slc_write_log_p(gv_log,'In slc_create_relationship_p p_in_relationship_type:'||p_in_relationship_type||
						  ' p_in_subject_id:'||p_in_subject_id||' p_in_object_id:'||p_in_object_id);
   
   p_relationship_rec_type.relationship_type  := p_in_relationship_type;
   p_relationship_rec_type.relationship_code  := lv_forward_rel_code;
   p_relationship_rec_type.subject_id         := p_in_subject_id;
   p_relationship_rec_type.subject_table_name := 'HZ_PARTIES' ;
   p_relationship_rec_type.object_id          := p_in_object_id;
   p_relationship_rec_type.object_table_name  := 'HZ_PARTIES'  ;
   p_relationship_rec_type.subject_type       := 'ORGANIZATION'  ;
   p_relationship_rec_type.object_type        := 'ORGANIZATION'  ;
   
   p_relationship_rec_type.start_date         := SYSDATE;
   p_relationship_rec_type.created_by_module  := 'TCA_V2_API';
   p_relationship_rec_type.attribute_category := 'Franchisee';
   p_relationship_rec_type.attribute1 := p_in_title;
   p_relationship_rec_type.attribute2 := p_in_ownership;
   p_relationship_rec_type.attribute3 := p_in_no_of_shares;
	FND_MSG_PUB.Initialize;
   hz_relationship_v2pub.create_relationship ( 
												p_init_msg_list     => 'T', 
												p_relationship_rec  => p_relationship_rec_type, 
												x_relationship_id   => x_relationship_id, 
												x_party_id          => x_party_id, 
												x_party_number      => x_party_number, 
												x_return_status     => l_return_status, 
												x_msg_count         => l_msg_count, 
												x_msg_data          => l_msg_data 
											 ) ;

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
	p_out_error_flag := lv_error_flag;
	IF lv_error_flag = 'Y' THEN
		p_out_err_msg	:= 'Error while creating Supplier Relationship. Error Message:'||lv_error_msg;
	END IF;
	
	slc_write_log_p(gv_log,'In slc_create_relationship_p p_out_error_flag:'||p_out_error_flag||' p_out_err_msg:'||p_out_err_msg);
	slc_write_log_p(gv_log,'');
 EXCEPTION
 WHEN OTHERS THEN
  slc_write_log_p(gv_log,'Unexpected error in slc_create_relationship_p. Error Message:'||SQLERRM);
  p_out_error_flag := 'Y';
  p_out_err_msg	:= 'Unexpected error in slc_create_relationship_p. Error Message:'||SQLERRM;	
 END slc_create_relationship_p;

/* ****************************************************************
	NAME:              slc_create_contact_directory_p
	PURPOSE:           This procedure will create contact directory information
						party
	Input Parameters:   
						p_in_vendor_id				IN 	NUMBER
						p_in_person_first_name 		IN	VARCHAR2
						p_in_person_middle_name 	IN	VARCHAR2
						p_in_person_last_name 		IN	VARCHAR2
						p_in_phone 					IN	VARCHAR2
						p_in_area_code 				IN	VARCHAR2
						p_in_email_address 			IN	VARCHAR2	
*****************************************************************/
  PROCEDURE slc_create_contact_directory_p  (p_in_vendor_id	IN 	NUMBER
											,p_in_org_party_site_id			IN	NUMBER
											,p_in_person_first_name 		IN	VARCHAR2
											,p_in_person_middle_name 	IN	VARCHAR2
											,p_in_person_last_name 		IN	VARCHAR2
											,p_in_email_address			IN  VARCHAR2
											,p_in_title					IN  VARCHAR2
											,p_in_ownership					IN  VARCHAR2
											,p_in_no_of_shares					IN  VARCHAR2
											,p_out_error_flag				   OUT VARCHAR2
											,p_out_err_msg							OUT VARCHAR2  
										  )
  IS
    l_vendor_contact_rec ap_vendor_pub_pkg.r_vendor_contact_rec_type;
    lv_return_status VARCHAR2(10);
    ln_msg_count NUMBER DEFAULT 0;
	lv_msg_data			VARCHAR2(4000);	
    lv_msg                  VARCHAR2(4000);
    lv_msg_out  NUMBER;	
	lv_error_flag		VARCHAR2(1) DEFAULT 'N';
	lv_error_msg		VARCHAR2(4000) DEFAULT NULL;	
    l_vendor_contact_id NUMBER;
    l_per_party_id NUMBER;
    l_rel_party_id NUMBER;
    l_rel_id NUMBER;
    l_org_contact_id NUMBER;
    l_party_site_id NUMBER; 
    l_exist_count   NUMBER DEFAULT 0;	
  BEGIN
    slc_write_log_p(gv_log,'');
	slc_write_log_p(gv_log,'In slc_create_contact_directory_p p_in_vendor_id:'||p_in_vendor_id);
	
	 l_exist_count := slc_get_minority_count_f(p_in_vendor_id,p_in_person_first_name,p_in_person_middle_name,p_in_person_last_name);
	IF l_exist_count = -9
	THEN
	  lv_error_flag := 'Y';
	  lv_error_msg := 'Unexpected error in slc_get_minority_count_f ';
	ELSIF l_exist_count > 0 THEN
		slc_update_relationship_p ( p_in_vendor_id	
									,p_in_person_first_name 	
									,p_in_person_middle_name 	
									,p_in_person_last_name 		
									,p_in_title					
									,p_in_ownership					
									,p_in_no_of_shares				
									,lv_error_flag				 
									,lv_error_msg						
								  );
	ELSIF l_exist_count = 0
	THEN
    l_vendor_contact_rec.vendor_id := p_in_vendor_id;
    l_vendor_contact_rec.person_first_name := p_in_person_first_name;
    l_vendor_contact_rec.person_middle_name := p_in_person_middle_name;
    l_vendor_contact_rec.person_last_name := p_in_person_last_name;
	l_vendor_contact_rec.org_party_site_id := p_in_org_party_site_id;
	l_vendor_contact_rec.email_address := p_in_email_address;
	l_vendor_contact_rec.attribute_category := 'Minority';
	l_vendor_contact_rec.attribute1 := p_in_title;
	l_vendor_contact_rec.attribute2 := p_in_ownership;
	l_vendor_contact_rec.attribute3 := p_in_no_of_shares;
	
	FND_MSG_PUB.Initialize;
    pos_vendor_pub_pkg.create_vendor_contact(
        p_vendor_contact_rec => l_vendor_contact_rec,
        x_return_status => lv_return_status,
        x_msg_count => ln_msg_count,
        x_msg_data => lv_msg_data,
        x_vendor_contact_id => l_vendor_contact_id,
        x_per_party_id => l_per_party_id,
        x_rel_party_id => l_rel_party_id,
        x_rel_id => l_rel_id,
        x_org_contact_id => l_org_contact_id,
        x_party_site_id => l_party_site_id);

	IF lv_return_status <> 'S' THEN
	lv_error_flag := 'Y';
	IF ln_msg_count >= 1 THEN
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
    END IF;--End of create contact directory
	p_out_error_flag := lv_error_flag;
	IF lv_error_flag = 'Y' THEN
		p_out_err_msg	:= ' Error Message:'||lv_error_msg;
	END IF;
	
	slc_write_log_p(gv_log,'In slc_create_contact_directory_p p_out_error_flag:'||p_out_error_flag||' p_out_err_msg:'||p_out_err_msg);
	slc_write_log_p(gv_log,'');
 EXCEPTION
 WHEN OTHERS THEN
	slc_write_log_p(gv_log,'Unexpected error in slc_create_contact_directory_p. Error Message:'||SQLERRM);
	p_out_error_flag := 'Y';
	p_out_err_msg	:= 'Unexpected error in slc_create_contact_directory_p. Error Message:'||SQLERRM;	  
  END slc_create_contact_directory_p;
 
 
/*
/* ****************************************************************
	NAME:              slc_create_fran_contact_p
	PURPOSE:           This procedure will create Franchisee as contact in
						Contact Directory
	Input Parameters:   
						p_in_vendor_id				IN 	NUMBER
						p_in_person_first_name 		IN	VARCHAR2
						p_in_person_middle_name 	IN	VARCHAR2
						p_in_person_last_name 		IN	VARCHAR2
						p_in_email_address 			IN	VARCHAR2	
*****************************************************************/
  PROCEDURE slc_create_fran_contact_p  (p_in_vendor_id	IN 	NUMBER
											,p_in_person_first_name 		IN	VARCHAR2
											,p_in_person_middle_name 	IN	VARCHAR2
											,p_in_person_last_name 		IN	VARCHAR2
											,p_in_email_address			IN  VARCHAR2
											,p_in_phone_number			IN  VARCHAR2
											,p_out_error_flag				   OUT VARCHAR2
											,p_out_err_msg							OUT VARCHAR2  
										  )
  IS
    l_vendor_contact_rec ap_vendor_pub_pkg.r_vendor_contact_rec_type;
    lv_return_status VARCHAR2(10);
    ln_msg_count NUMBER DEFAULT 0;
	lv_msg_data			VARCHAR2(4000);	
    lv_msg                  VARCHAR2(4000);
    lv_msg_out  NUMBER;	
	lv_error_flag		VARCHAR2(1) DEFAULT 'N';
	lv_error_msg		VARCHAR2(4000) DEFAULT NULL;	
    l_vendor_contact_id NUMBER;
    l_per_party_id NUMBER;
    l_rel_party_id NUMBER;
    l_rel_id NUMBER;
    l_org_contact_id NUMBER;
    l_party_site_id NUMBER;  
  BEGIN
    slc_write_log_p(gv_log,'');
	slc_write_log_p(gv_log,'In slc_create_fran_contact_p p_in_vendor_id:'||p_in_vendor_id);
	
    l_vendor_contact_rec.vendor_id := p_in_vendor_id;
    l_vendor_contact_rec.person_first_name := p_in_person_first_name;
    l_vendor_contact_rec.person_middle_name := p_in_person_middle_name;
    l_vendor_contact_rec.person_last_name := p_in_person_last_name;
	l_vendor_contact_rec.email_address := p_in_email_address;
	l_vendor_contact_rec.area_code	:= substr(p_in_phone_number,1,3);
	l_vendor_contact_rec.phone := substr(p_in_phone_number,4);
	FND_MSG_PUB.Initialize;
    pos_vendor_pub_pkg.create_vendor_contact(
        p_vendor_contact_rec => l_vendor_contact_rec,
        x_return_status => lv_return_status,
        x_msg_count => ln_msg_count,
        x_msg_data => lv_msg_data,
        x_vendor_contact_id => l_vendor_contact_id,
        x_per_party_id => l_per_party_id,
        x_rel_party_id => l_rel_party_id,
        x_rel_id => l_rel_id,
        x_org_contact_id => l_org_contact_id,
        x_party_site_id => l_party_site_id);

	IF lv_return_status <> 'S' THEN
	lv_error_flag := 'Y';
	IF ln_msg_count >= 1 THEN
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
		p_out_err_msg	:= ' Error Message:'||lv_error_msg;
	END IF;

	slc_write_log_p(gv_log,'In slc_create_fran_contact_p p_out_error_flag:'||p_out_error_flag||' p_out_err_msg:'||p_out_err_msg);
	slc_write_log_p(gv_log,'');
 EXCEPTION
 WHEN OTHERS THEN
	slc_write_log_p(gv_log,'Unexpected error in slc_create_fran_contact_p. Error Message:'||SQLERRM);
	p_out_error_flag := 'Y';
	p_out_err_msg	:= 'Unexpected error in slc_create_fran_contact_p. Error Message:'||SQLERRM;	  
  END slc_create_fran_contact_p;

  /* ****************************************************************
	NAME:              slc_create_incorp_cnt_pnt_p
	PURPOSE:           This procedure will create contact point information for 
						party
	Input Parameters:   p_in_party_id		IN 	NUMBER
						p_in_contact_point_type		IN VARCHAR
						p_in_email_address			IN VARCHAR	
*****************************************************************/
  PROCEDURE slc_create_incorp_cnt_pnt_p(p_in_party_id				IN 	NUMBER
									  ,p_in_contact_point_type		IN VARCHAR
									  ,p_in_email_address			IN VARCHAR
									  ,p_in_phone_number			IN 	VARCHAR2
									  ,p_out_error_flag				OUT VARCHAR2
									  ,p_out_err_msg				OUT VARCHAR2									  
									  )
  IS 
   p_contact_point_rec HZ_CONTACT_POINT_V2PUB.CONTACT_POINT_REC_TYPE;
   p_edi_rec           HZ_CONTACT_POINT_V2PUB.EDI_REC_TYPE;
   p_email_rec         HZ_CONTACT_POINT_V2PUB.EMAIL_REC_TYPE;
   p_phone_rec         HZ_CONTACT_POINT_V2PUB.PHONE_REC_TYPE;
   p_telex_rec         HZ_CONTACT_POINT_V2PUB.TELEX_REC_TYPE;
   p_web_rec           HZ_CONTACT_POINT_V2PUB.WEB_REC_TYPE;  
   x_return_status     VARCHAR2(2000);
   x_msg_count         NUMBER DEFAULT 0;
   x_msg_data          VARCHAR2(2000);
   x_contact_point_id  NUMBER;  
	lv_error_flag		VARCHAR2(1) DEFAULT 'N';
	lv_error_msg		VARCHAR2(4000) DEFAULT NULL; 
	lv_msg                  VARCHAR2(4000);
    lv_msg_out  NUMBER;	
	
  BEGIN
   slc_write_log_p(gv_log,'');	  
   slc_write_log_p(gv_log,'In slc_create_incorp_cnt_pnt_p p_in_party_id:'||p_in_party_id||
						  ' p_in_contact_point_type:'||p_in_contact_point_type||
						  ' p_in_email_address:'||p_in_email_address||
						  ' p_in_phone_number:'||p_in_phone_number
						  );
   p_contact_point_rec.contact_point_type     := p_in_contact_point_type;
   p_contact_point_rec.owner_table_name       := 'HZ_PARTIES';
   p_contact_point_rec.primary_flag           := 'Y';
   p_contact_point_rec.contact_point_purpose  := 'BUSINESS';
   p_contact_point_rec.created_by_module	  := 'HZ_CPUI';
   p_contact_point_rec.owner_table_id         := p_in_party_id;
   
   IF p_in_contact_point_type = 'EMAIL' THEN
	   p_email_rec.email_address                  := p_in_email_address;    
   ELSIF p_in_contact_point_type = 'PHONE' THEN
     p_phone_rec.phone_area_code                  := substr(p_in_phone_number,1,3);
	   p_phone_rec.phone_number                  	:= substr(p_in_phone_number,4);
	   p_phone_rec.phone_line_type                  := 'GEN';
   END IF;   
   
   FND_MSG_PUB.Initialize;
   hz_contact_point_v2pub.create_contact_point (
                                                   p_init_msg_list     =>  FND_API.G_TRUE,
                                                   p_contact_point_rec =>  p_contact_point_rec,
                                                   p_edi_rec           =>  p_edi_rec          ,
                                                   p_email_rec         =>  p_email_rec        , 
                                                   p_phone_rec         =>  p_phone_rec        ,
                                                   p_telex_rec         =>  p_telex_rec        ,
                                                   p_web_rec           =>  p_web_rec          ,
                                                   x_contact_point_id  =>  x_contact_point_id ,
                                                   x_return_status     =>  x_return_status    ,
                                                   x_msg_count         =>  x_msg_count        ,
                                                   x_msg_data          =>  x_msg_data
                                                ); 
   IF x_return_status <> 'S'
   THEN
   lv_error_flag := 'Y';
	IF x_msg_count >= 1 THEN
      FOR I IN 1..x_msg_count 
      LOOP
		  FND_MSG_PUB.Get (p_msg_index       => i,
								 p_encoded         => 'F',
								 p_data            => lv_msg,
								 p_msg_index_OUT   => lv_msg_out);
		  lv_error_msg := lv_error_msg || ':' || lv_msg;
      END LOOP;
	ELSE
		lv_error_msg := x_msg_data;
	END IF;
   END IF;	

	p_out_error_flag := lv_error_flag;
	IF lv_error_flag = 'Y' THEN
		p_out_err_msg	:= 'Error Message:'||lv_error_msg;
	END IF;
	slc_write_log_p(gv_log,'In slc_create_incorp_cnt_pnt_p  lv_error_flag:'	||lv_error_flag||' lv_error_msg:'||lv_error_msg);
	slc_write_log_p(gv_log,'');	
	
 EXCEPTION
 WHEN OTHERS THEN
	slc_write_log_p(gv_log,'Unexpected error in slc_create_incorp_cnt_pnt_p. Error Message:'||SQLERRM);
	p_out_error_flag := 'Y';
	p_out_err_msg	:= 'Unexpected error in slc_create_incorp_cnt_pnt_p. Error Message:'||SQLERRM;	
  END slc_create_incorp_cnt_pnt_p;
  
/* ****************************************************************
	NAME:              slc_update_incorp_cnt_pnt_p
	PURPOSE:           This procedure will create update point information for 
						party
	Input Parameters:   p_in_contact_point_id		IN 	NUMBER
						p_in_contact_point_type		IN VARCHAR
						p_in_email_address			IN VARCHAR	
*****************************************************************/
  PROCEDURE slc_update_incorp_cnt_pnt_p(p_in_contact_point_id	IN 	NUMBER
									  ,p_in_contact_point_type	IN VARCHAR
									  ,p_in_object_version_number	IN 	NUMBER
									  ,p_in_email_address			IN VARCHAR
									  ,p_in_phone_number			IN VARCHAR
									  ,p_out_error_flag				OUT VARCHAR2
									  ,p_out_err_msg				OUT VARCHAR2									  
									  )
  IS 
  p_contact_point_rec apps.hz_contact_point_v2pub.contact_point_rec_type;
  p_edi_rec apps.hz_contact_point_v2pub.edi_rec_type;
  p_email_rec apps.hz_contact_point_v2pub.email_rec_type;
  p_phone_rec apps.hz_contact_point_v2pub.phone_rec_type;
  p_telex_rec apps.hz_contact_point_v2pub.telex_rec_type;
  p_web_rec apps.hz_contact_point_v2pub.web_rec_type;
   x_return_status     VARCHAR2(2000);
   x_msg_count         NUMBER DEFAULT 0;
   x_msg_data          VARCHAR2(2000);
   x_contact_point_id  NUMBER;  
	lv_error_flag		VARCHAR2(1) DEFAULT 'N';
	lv_error_msg		VARCHAR2(4000) DEFAULT NULL; 
	lv_msg                  VARCHAR2(4000);
    lv_msg_out  NUMBER;	
	ln_object_version_number	hz_contact_points.object_version_number%TYPE;
  BEGIN
   slc_write_log_p(gv_log,'');	  
   slc_write_log_p(gv_log,'In slc_update_incorp_cnt_pnt_p p_in_contact_point_id:'||p_in_contact_point_id||
						  ' p_in_contact_point_type:'||p_in_contact_point_type||
						  ' p_in_object_version_number:'||p_in_object_version_number||
						  ' p_in_email_address:'||p_in_email_address||
						  ' p_in_phone_number:'||p_in_phone_number
						  );   
    p_contact_point_rec.contact_point_id    := p_in_contact_point_id;
	ln_object_version_number  					:= p_in_object_version_number;
   IF p_in_contact_point_type = 'EMAIL' THEN
	   p_email_rec.email_address                  := p_in_email_address;    
   ELSIF p_in_contact_point_type = 'PHONE' THEN
       p_phone_rec.phone_area_code                  := substr(p_in_phone_number,1,3);
	   p_phone_rec.phone_number                  	:= substr(p_in_phone_number,4);
   END IF;  
   
   FND_MSG_PUB.Initialize;
	hz_contact_point_v2pub.update_contact_point(
			p_init_msg_list => FND_API.G_TRUE,
			p_contact_point_rec => p_contact_point_rec,
			p_edi_rec => p_edi_rec,
			p_email_rec => p_email_rec,
			p_phone_rec => p_phone_rec,
			p_telex_rec => p_telex_rec,
			p_web_rec => p_web_rec,
			p_object_version_number => ln_object_version_number,
			x_return_status => x_return_status,
			x_msg_count => x_msg_count,
			x_msg_data => x_msg_data
		  );
   IF x_return_status <> 'S'
   THEN
   lv_error_flag := 'Y';
	IF x_msg_count >= 1 THEN
      FOR I IN 1..x_msg_count 
      LOOP
		  FND_MSG_PUB.Get (p_msg_index       => i,
								 p_encoded         => 'F',
								 p_data            => lv_msg,
								 p_msg_index_OUT   => lv_msg_out);
		  lv_error_msg := lv_error_msg || ':' || lv_msg;
      END LOOP;
	ELSE
		lv_error_msg := x_msg_data;
	END IF;
   END IF;	

	p_out_error_flag := lv_error_flag;
	IF lv_error_flag = 'Y' THEN
		p_out_err_msg	:= 'Error Message:'||lv_error_msg;
	END IF;
	slc_write_log_p(gv_log,'In slc_update_incorp_cnt_pnt_p lv_error_flag:'||lv_error_flag||' lv_error_msg:'||lv_error_msg);
	slc_write_log_p(gv_log,'');	  
 EXCEPTION
 WHEN OTHERS THEN
	slc_write_log_p(gv_log,'Unexpected error in slc_update_incorp_cnt_pnt_p. Error Message:'||SQLERRM);
	p_out_error_flag := 'Y';
	p_out_err_msg	:= 'Unexpected error in slc_update_incorp_cnt_pnt_p. Error Message:'||SQLERRM;	
  END slc_update_incorp_cnt_pnt_p;  

/* ****************************************************************
	NAME:              slc_create_contact_p
	PURPOSE:           This procedure will create Franchisee Contact
	Input Parameters:   
						p_in_vendor_id				IN 	NUMBER
						p_in_person_first_name 		IN	VARCHAR2
						p_in_person_middle_name 	IN	VARCHAR2
						p_in_person_last_name 		IN	VARCHAR2
						p_in_email_address 			IN	VARCHAR2	
*****************************************************************/  

  PROCEDURE slc_create_contact_p   (p_in_franchisee_number	IN 	VARCHAR
									,p_in_count 					IN 	NUMBER
									,p_in_party_id					IN 	NUMBER
									,p_in_vendor_id					IN 	NUMBER
									,p_in_person_first_name 		IN	VARCHAR2
									,p_in_person_middle_name 	IN	VARCHAR2
									,p_in_person_last_name 		IN	VARCHAR2
									,p_in_email_address			IN  VARCHAR2
									,p_in_phone_number			IN 	VARCHAR2
									,p_out_error_flag				   OUT VARCHAR2
									,p_out_err_msg							OUT VARCHAR2  
								  )
  IS
  CURSOR c_contact_rec
  IS
	select hcpe.contact_point_id email_contact_point_id
		  ,hcpe.object_version_number email_object_version_number
		  ,hcpp.contact_point_id  phone_contact_point_id
		  ,hcpp.object_version_number phone_object_version_number
		  ,hr.party_id
	from hz_parties hp
	,hz_relationships hr
	,hz_parties per_hp
	,hz_contact_points  hcpe
	,hz_contact_points  hcpp
	where hr.party_id              = hcpe.owner_table_id(+)
	AND hcpe.owner_table_name(+)   = 'HZ_PARTIES'
	AND hcpe.status (+)            = 'A'
	AND hcpe.contact_point_type(+) = 'EMAIL'
	AND hcpe.primary_flag(+) = 'Y'
	AND hr.party_id                = hcpp.owner_table_id(+)
	AND hcpp.owner_table_name(+)   = 'HZ_PARTIES'
	AND hcpp.status (+)            = 'A'
	AND hcpp.contact_point_type(+) = 'PHONE'
	AND hcpp.primary_flag(+)	   = 'Y'
	AND hcpp.phone_line_type(+)    = 'GEN'
	AND hr.subject_id = hp.party_id
	AND hr.relationship_code = 'CONTACT'
	AND hr.status = 'A'
	AND hr.object_id = per_hp.party_id
	--Changes for v1.5. Comparing names without considering case.
	AND  UPPER(NVL(per_hp.person_first_name ,'X'))      =  UPPER(NVL(p_in_person_first_name,'X'))
	AND  UPPER(NVL(per_hp.person_middle_name,'X'))       =  UPPER(NVL(p_in_person_middle_name,'X'))
	AND  UPPER(NVL(per_hp.person_last_name,'X'))       =  UPPER(NVL(p_in_person_last_name,'X'))
	AND hp.party_id = p_in_party_id;
	
	
	CURSOR c_email_point_rec_incorp 
	IS
	SELECT hcpe.object_version_number,hcpe.contact_point_id
	  FROM hz_parties hp
		 ,hz_contact_points hcpe
      WHERE hp.party_id = p_in_party_id
		AND hp.party_id = hcpe.owner_table_id(+)
		AND hcpe.owner_table_name(+) = 'HZ_PARTIES'
		AND hcpe.status(+) = 'A'
		AND hcpe.primary_flag(+) = 'Y'
		AND hcpe.contact_point_type(+) = 'EMAIL';
		
	ln_email_contact_point_id		hz_contact_points.contact_point_id%TYPE;
	ln_email_object_version_number	hz_contact_points.object_version_number%TYPE;
	ln_phone_contact_point_id		hz_contact_points.contact_point_id%TYPE;
	ln_phone_object_version_number	hz_contact_points.object_version_number%TYPE;	
	ln_contact_party_id				hz_relationships.party_id%TYPE;
	lv_error_flag		VARCHAR2(1) DEFAULT 'N';
	lv_error_msg		VARCHAR2(4000) DEFAULT NULL; 	
  BEGIN
    slc_write_log_p(gv_log,'');	  
	slc_write_log_p(gv_log,'In slc_create_contact_p p_in_vendor_id:'||p_in_vendor_id ||
							' p_in_party_id:'||p_in_party_id||
							' p_in_count:'||p_in_count||
							' p_in_franchisee_number:'||p_in_franchisee_number);
	
	IF p_in_count = 0 THEN
		IF p_in_franchisee_number <> 'CORPORATION' THEN
			--In case of Individual Contact is created in Contact Directory
			--This contact information gets created even when Email Address and Phone Number is null.
				slc_create_fran_contact_p(p_in_vendor_id	
											,p_in_person_first_name 	
											,p_in_person_middle_name 	
											,p_in_person_last_name 		
											,p_in_email_address		
											,p_in_phone_number
											,lv_error_flag				
											,lv_error_msg						
										  );
		ELSIF p_in_franchisee_number = 'CORPORATION' THEN
			--Since email address is created as Contact Point for Corporation email address cannot be null.
			IF p_in_email_address IS NOT NULL THEN
				slc_create_incorp_cnt_pnt_p(p_in_party_id	
										  ,'EMAIL'	
										  ,p_in_email_address	
										  ,NULL
										  ,lv_error_flag				
										  ,lv_error_msg	
										  );		
			END IF;
		END IF;
	ELSE
		--For Franchisee create/update contact in Contact Directory.
		IF p_in_franchisee_number <> 'CORPORATION' THEN
			OPEN c_contact_rec;
			FETCH c_contact_rec INTO ln_email_contact_point_id,ln_email_object_version_number
								    ,ln_phone_contact_point_id,ln_phone_object_version_number,ln_contact_party_id;
			CLOSE c_contact_rec;
			slc_write_log_p(gv_log,'In slc_create_contact_p ln_contact_party_id:'||ln_contact_party_id);
			slc_write_log_p(gv_log,'In slc_create_contact_p ln_email_contact_point_id:'||ln_email_contact_point_id);
			slc_write_log_p(gv_log,'In slc_create_contact_p ln_phone_contact_point_id:'||ln_phone_contact_point_id);
			
			--During Supplier Conversion for Franchisee we are not creating any Contacts.
			--Thus if ln_contact_party_id is NULL then we need to create Contact.
			IF ln_contact_party_id IS NULL THEN
				slc_create_fran_contact_p(p_in_vendor_id	
										,p_in_person_first_name 	
										,p_in_person_middle_name 	
										,p_in_person_last_name 		
										,p_in_email_address		
										,p_in_phone_number
										,lv_error_flag				
										,lv_error_msg						
									  );	
			ELSE 
				IF p_in_email_address IS NOT NULL THEN
						IF ln_email_contact_point_id IS NULL THEN
							slc_create_incorp_cnt_pnt_p(ln_contact_party_id	
													  ,'EMAIL'	
													  ,p_in_email_address	
													  ,NULL
													  ,lv_error_flag				
													  ,lv_error_msg	
													  );	
						ELSE
							slc_update_incorp_cnt_pnt_p(ln_email_contact_point_id	
												  ,'EMAIL'
												  ,ln_email_object_version_number	
												  ,p_in_email_address
												  ,NULL
												  ,lv_error_flag				
												  ,lv_error_msg														  
												  );
						END IF;
				END IF;--End of Email Id not null check
				
				IF p_in_phone_number IS NOT NULL AND lv_error_flag = 'N' THEN
					IF ln_phone_contact_point_id IS NULL THEN
						slc_create_incorp_cnt_pnt_p(ln_contact_party_id	
												  ,'PHONE'	
												  ,NULL	
												  ,p_in_phone_number
												  ,lv_error_flag				
												  ,lv_error_msg	
												  );	
					ELSE
						slc_update_incorp_cnt_pnt_p(ln_phone_contact_point_id	
											  ,'PHONE'
											  ,ln_phone_object_version_number	
											  ,NULL
											  ,p_in_phone_number
											  ,lv_error_flag				
											  ,lv_error_msg															  
											  );
					END IF;
				END IF;--End of Phone Number not null check		
			END IF;--
		ELSIF p_in_franchisee_number = 'CORPORATION' THEN
			OPEN c_email_point_rec_incorp;
			FETCH c_email_point_rec_incorp INTO ln_email_object_version_number,ln_email_contact_point_id;
			CLOSE c_email_point_rec_incorp;
			slc_write_log_p(gv_log,'In slc_create_contact_p ln_email_object_version_number:'||ln_email_object_version_number||
									' ln_email_contact_point_id:'||ln_email_contact_point_id);
			IF p_in_email_address IS NOT NULL THEN
				IF ln_email_contact_point_id IS NULL THEN
					slc_create_incorp_cnt_pnt_p(p_in_party_id	
											  ,'EMAIL'	
											  ,p_in_email_address	
											  ,NULL
											  ,lv_error_flag				
										      ,lv_error_msg	
											  );		
				ELSE
					slc_update_incorp_cnt_pnt_p(ln_email_contact_point_id	
										  ,'EMAIL'
										  ,ln_email_object_version_number	
										  ,p_in_email_address
										  ,NULL
										  ,lv_error_flag				
										  ,lv_error_msg														  
										  );
			  END IF;--End of Email Address not null check.
			END IF;
		END IF;
		
	END IF;
	p_out_error_flag := lv_error_flag;
	IF lv_error_flag = 'Y' THEN
		p_out_err_msg	:= 'Error while creating Contact for '||p_in_franchisee_number||' Error Message:'||lv_error_msg;
	END IF;	
	slc_write_log_p(gv_log,'In slc_create_contact_p p_out_error_flag:'||p_out_error_flag||' p_out_err_msg:'||p_out_err_msg);
	slc_write_log_p(gv_log,'');	  
  EXCEPTION
  WHEN OTHERS THEN
	slc_write_log_p(gv_log,'Unexpected error in slc_create_contact_p for '||p_in_franchisee_number||' Error Message:'||SQLERRM);
	p_out_error_flag := 'Y';
	p_out_err_msg	:= 'Unexpected error in slc_create_contact_p for '||p_in_franchisee_number||' Error Message:'||SQLERRM;	  
  END slc_create_contact_p;
  
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
									,p_out_vendor_id						OUT NUMBER
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
	slc_write_log_p(gv_log,'');	  
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
		--Changes for v1.2
		l_vendor_rec.minority_group_lookup_code         := 'UNDETERMINED';
		
		
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
	p_out_vendor_id := ln_vendor_id;
	p_out_error_flag := lv_error_flag;
	IF lv_error_flag = 'Y' THEN
		p_out_err_msg	:= 'Error while creating Supplier for '||p_in_entity_name||' Error Message:'||lv_error_msg;
	END IF;
	slc_write_log_p(gv_log,'In slc_create_supplier_p ln_party_id:'||ln_party_id||' ln_vendor_id:'||ln_vendor_id
			||' lv_error_flag:'||lv_error_flag||' lv_error_msg:'||lv_error_msg);
	slc_write_log_p(gv_log,'');	  
	EXCEPTION
	WHEN OTHERS THEN
	slc_write_log_p(gv_log,'Unexpected error in slc_create_supplier_p for '||p_in_entity_name||' Error Message:'||SQLERRM);
	p_out_party_id	:= NULL;
	p_out_error_flag := 'Y';
	p_out_err_msg	:= 'Unexpected error in slc_create_supplier_p for '||p_in_entity_name||' Error Message:'||SQLERRM;	
	
	END slc_create_supplier_p;

	/* ****************************************************************
		NAME:              slc_update_org_profile_p
		PURPOSE:           This procedure will update org profile value
		Input Parameters	p_in_party_id	IN  NUMBER
							
	*****************************************************************/
	PROCEDURE slc_update_org_profile_p(p_in_party_id 		IN NUMBER
										,p_in_vendor_name 	IN VARCHAR2
										,p_out_error_flag		OUT VARCHAR2
										,p_out_err_msg			OUT VARCHAR2
										)
	IS
		lv_organization_rec apps.hz_party_v2pub.organization_rec_type;
		lv_party_rec           hz_party_v2pub.party_rec_type;	
		ln_object_version_number		NUMBER;
		ln_supplier_number				ap_suppliers.segment1%TYPE;
		lv_error_flag		VARCHAR2(1) DEFAULT 'N';
		lv_error_msg		VARCHAR2(4000) DEFAULT NULL;
	
		  ln_profile_id NUMBER;
		  ln_return_status VARCHAR2(4000);
		  ln_msg_count NUMBER;
		  lv_msg_data VARCHAR2(4000);
		  lv_msg                  VARCHAR2(4000);
		  lv_msg_out  NUMBER; 
	BEGIN
	slc_write_log_p(gv_log,'');	  
	slc_write_log_p(gv_log,'In slc_update_org_profile_p p_in_party_id:'||p_in_party_id||' p_in_vendor_name:'||p_in_vendor_name);
	SELECT hp.object_version_number
		   ,sup.segment1
	  INTO ln_object_version_number
		  ,ln_supplier_number
	 FROM hz_parties hp
		 ,ap_suppliers sup
	WHERE hp.party_id = p_in_party_id
	  AND sup.party_id = hp.party_id;
	slc_write_log_p(gv_log,'In slc_update_org_profile_p ln_object_version_number:'||ln_object_version_number||
							' ln_supplier_number:'||ln_supplier_number);
	
	lv_party_rec.party_id                          := p_in_party_id;
	lv_organization_rec.party_rec                  := lv_party_rec;
	lv_organization_rec.organization_name		   := p_in_vendor_name||'_'||ln_supplier_number;
		FND_MSG_PUB.Initialize;
		 hz_party_v2pub.update_organization(
			p_init_msg_list => fnd_api.g_true,
			p_organization_rec => lv_organization_rec,
			p_party_object_version_number => ln_object_version_number,
			x_profile_id => ln_profile_id,
			x_return_status => ln_return_status,
			x_msg_count => ln_msg_count,
			x_msg_data => lv_msg_data
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

	p_out_error_flag := lv_error_flag;
	IF lv_error_flag = 'Y' THEN
	p_out_err_msg	:= 'Error while updating Supplier .Error Message:'||lv_error_msg;
	END IF;
	slc_write_log_p(gv_log,'In slc_update_org_profile_p lv_error_flag:'||lv_error_flag||' lv_error_msg:'||lv_error_msg);
	slc_write_log_p(gv_log,'');	  
	EXCEPTION
	WHEN OTHERS THEN
	slc_write_log_p(gv_log,'Unexpected error in slc_update_org_profile_p. Error Message:'||SQLERRM);
	p_out_error_flag := 'Y';
	p_out_err_msg	:= 'Unexpected error in slc_update_org_profile_p. Error Message:'||SQLERRM;		
	END slc_update_org_profile_p;
	
/* ****************************************************************
	NAME:              slc_populate_definer_uda_p
	PURPOSE:           This procedure will update Definer UDA for Supplier for 
						given store.
	Input Parameters	p_in_ssn				IN 		NUMBER
						p_in_dob				IN 		VARCHAR2
						p_in_bkgrd				IN 		VARCHAR2
						p_in_marital			IN 		VARCHAR2
						p_in_sex				IN		VARCHAR2
						p_in_original_date		IN 		VARCHAR2
						p_in_effec_begin_date	IN 		VARCHAR2
						p_in_effec_end_date		IN 		VARCHAR2
*****************************************************************/	
	PROCEDURE	slc_populate_definer_uda_p(p_in_party_id			IN 	NUMBER
											,p_in_vendor_id			IN 	NUMBER
											,p_in_vendor_site_code	IN 	VARCHAR2
											,p_in_desig1_name		IN 	VARCHAR2					 
											,p_in_desig1_address	IN 	VARCHAR2				 
											,p_in_desig1_rel_to_fran	IN 	VARCHAR2				 
											,p_in_desig2_name			IN 	VARCHAR2					 
											,p_in_desig2_address		IN 	VARCHAR2			 
											,p_in_desig2_rel_to_fran	IN 	VARCHAR2				 
											,p_in_desig3_name			IN 	VARCHAR2				 
											,p_in_desig3_address		IN 	VARCHAR2			 
											,p_in_desig3_rel_to_fran	IN 	VARCHAR2				 											
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
	  lv_attr_group_name               VARCHAR2 (100)				:= 'SLCISP_FRAN_DESIGNEE_DETAILS';
	  lv_attr_group_disp_name          VARCHAR2 (250);
	  lv_data_level                    VARCHAR2 (100)				:= 'SUPP_ADDR_SITE_LEVEL';
	  lv_classification_code            VARCHAR2 (100)				:=  'BS:BASE';
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
	  
	  ln_party_site_id					ap_supplier_sites_all.party_site_id%TYPE;
	  ln_vendor_site_id					ap_supplier_sites_all.vendor_site_id%TYPE;
  BEGIN
	slc_write_log_p(gv_log,'In slc_populate_definer_uda_p p_in_party_id:'||p_in_party_id||
							' p_in_vendor_id:'||p_in_vendor_id||' p_in_vendor_site_code:'||p_in_vendor_site_code);
							
	 SELECT attr_group_id, application_id attr_group_app_id,
			attr_group_type, attr_group_name,
			attr_group_disp_name
	   INTO lv_attr_grp_id, lv_group_app_id,
			lv_attr_group_type, lv_attr_group_name,
			lv_attr_group_disp_name
	   FROM ego_attr_groups_v eagv
	  WHERE eagv.attr_group_name = lv_attr_group_name
		AND eagv.attr_group_type = lv_attr_group_type;
		
	SELECT sups.party_site_id,
	  sups.vendor_site_id
	INTO ln_party_site_id,ln_vendor_site_id
	FROM ap_suppliers sup,
	  ap_supplier_sites_all sups
	WHERE sups.vendor_id      = sup.vendor_id
	AND sup.vendor_id         = p_in_vendor_id
	AND sups.vendor_site_code = p_in_vendor_site_code;
	
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
						,ln_party_site_id                                   -- DATA_LEVEL_2
						,ln_vendor_site_id                                   -- DATA_LEVEL_3
						,NULL                                   -- DATA_LEVEL_4
						,NULL                                   -- DATA_LEVEL_5
						,ego_user_attrs_data_pvt.g_sync_mode--g_create_mode
					   );

	  lv_attributes_data_table.EXTEND;
	  lv_attributes_data_table (1) :=
		 ego_user_attr_data_obj
					(ego_import_row_seq_s.CURRVAL -- ROW_IDENTIFIER from above
					 ,'SLCISCP_DESIGNEE1_NAME'          -- ATTR_NAME
					 ,p_in_desig1_name                  -- ATTR_VALUE_STR
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
					 ,'SLCISCP_DESIGNEE1_ADDRESS'                   -- ATTR_NAME
					 ,p_in_desig1_address                                    -- ATTR_VALUE_STR
					 ,NULL                              -- ATTR_VALUE_NUM
					 ,NULL								-- ATTR_VALUE_DATE
					 ,NULL                                   -- ATTR_DISP_VALUE
					 ,NULL                              -- ATTR_UNIT_OF_MEASURE
					 ,ego_import_row_seq_s.CURRVAL      -- USER_ROW_IDENTIFIER
					); 

	  lv_attributes_data_table.EXTEND;
	  lv_attributes_data_table (3) :=
		 ego_user_attr_data_obj
					(ego_import_row_seq_s.CURRVAL -- ROW_IDENTIFIER from above
					 ,'SLCISP_DESIGNEE1_RELATIONSHIP'   -- ATTR_NAME
					 ,p_in_desig1_rel_to_fran			-- ATTR_VALUE_STR
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
					 ,'SLCISP_DESIGNEE2_NAME'           -- ATTR_NAME
					 ,p_in_desig2_name					-- ATTR_VALUE_STR
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
					 ,'SLCISP_DESIGNEE2_ADDRESS'        -- ATTR_NAME
					 ,p_in_desig2_address              -- ATTR_VALUE_STR
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
					 ,'SLCISP_DESIGNEE2_RELATIONSHIP'   -- ATTR_NAME
					 ,p_in_desig2_rel_to_fran           -- ATTR_VALUE_STR
					 ,NULL                              -- ATTR_VALUE_NUM
					 ,NULL								-- ATTR_VALUE_DATE
					 ,NULL                                   -- ATTR_DISP_VALUE
					 ,NULL                              -- ATTR_UNIT_OF_MEASURE
					 ,ego_import_row_seq_s.CURRVAL      -- USER_ROW_IDENTIFIER
					); 

	  lv_attributes_data_table.EXTEND;
	  lv_attributes_data_table (7) :=
		 ego_user_attr_data_obj
					(ego_import_row_seq_s.CURRVAL -- ROW_IDENTIFIER from above
					 ,'SLCISP_DESIGNEE3_NAME'           -- ATTR_NAME
					 ,p_in_desig3_name                  -- ATTR_VALUE_STR
					 ,NULL                              -- ATTR_VALUE_NUM
					 ,NULL								-- ATTR_VALUE_DATE
					 ,NULL                                   -- ATTR_DISP_VALUE
					 ,NULL                              -- ATTR_UNIT_OF_MEASURE
					 ,ego_import_row_seq_s.CURRVAL      -- USER_ROW_IDENTIFIER
					); 

	  lv_attributes_data_table.EXTEND;
	  lv_attributes_data_table (8) :=
		 ego_user_attr_data_obj
					(ego_import_row_seq_s.CURRVAL -- ROW_IDENTIFIER from above
					 ,'SLCISP_DESIGNEE3_ADDRESS'        -- ATTR_NAME
					 ,p_in_desig3_address              -- ATTR_VALUE_STR
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
					 ,'SLCISP_DESIGNEE3_RELATIONSHIP'   -- ATTR_NAME
					 ,p_in_desig3_rel_to_fran           -- ATTR_VALUE_STR
					 ,NULL                              -- ATTR_VALUE_NUM
					 ,NULL								-- ATTR_VALUE_DATE
					 ,NULL                                   -- ATTR_DISP_VALUE
					 ,NULL                              -- ATTR_UNIT_OF_MEASURE
					 ,ego_import_row_seq_s.CURRVAL      -- USER_ROW_IDENTIFIER
					); 

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
		p_out_err_msg	:= 'Error while updating definer UDA Error Message:'||lv_error_msg;
	END IF;
	slc_write_log_p(gv_log,'In slc_populate_definer_uda_p lv_error_flag:'||lv_error_flag||' lv_error_msg:'||lv_error_msg);
	slc_write_log_p(gv_log,'');	 					
							
  EXCEPTION
  WHEN OTHERS THEN
	slc_write_log_p(gv_log,'Unexpected error in slc_populate_definer_uda_p. Error Message:'||SQLERRM);
	p_out_error_flag := 'Y';
	p_out_err_msg	:= 'Unexpected error in slc_populate_definer_uda_p. Error Message:'||SQLERRM;
  END slc_populate_definer_uda_p; 
  
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
						,p_in_veteran           IN 		VARCHAR2  
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
					 ,slc_get_transformed_date_f(p_in_dob)-- ATTR_VALUE_DATE
					 ,NULL                                   -- ATTR_DISP_VALUE
					 ,NULL                              -- ATTR_UNIT_OF_MEASURE
					 ,ego_import_row_seq_s.CURRVAL      -- USER_ROW_IDENTIFIER
					);

		lv_attributes_data_table.EXTEND;			
		lv_attributes_data_table (3) :=
		 ego_user_attr_data_obj
					(ego_import_row_seq_s.CURRVAL -- ROW_IDENTIFIER from above
					 ,'SLC_ISP_FRANCHISEE_Ethnicity'                   -- ATTR_NAME
					 ,p_in_bkgrd     -- ATTR_VALUE_STR
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
					 ,p_in_marital -- ATTR_VALUE_STR
					 ,NULL                              -- ATTR_VALUE_NUM
					 ,NULL								-- ATTR_VALUE_DATE
					 ,NULL                                   -- ATTR_DISP_VALUE
					 ,NULL                              -- ATTR_UNIT_OF_MEASURE
					 ,ego_import_row_seq_s.CURRVAL      -- USER_ROW_IDENTIFIER
					); 
					
		lv_attributes_data_table.EXTEND;			
		lv_attributes_data_table (5) :=
		 ego_user_attr_data_obj
					(ego_import_row_seq_s.CURRVAL       -- ROW_IDENTIFIER from above
					 ,'SLC_ISP_FRANCHISEE_SEX'          -- ATTR_NAME
					 ,p_in_sex                          -- ATTR_VALUE_STR
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
					 ,slc_get_transformed_date_f(p_in_original_date)-- ATTR_VALUE_DATE
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
		--Added VETERAN in UDA
		lv_attributes_data_table.EXTEND;			
		lv_attributes_data_table (12) :=
		 ego_user_attr_data_obj
					(ego_import_row_seq_s.CURRVAL -- ROW_IDENTIFIER from above
					 ,'SLC_ISP_VETERAN'                   -- ATTR_NAME
					 ,p_in_veteran                      -- ATTR_VALUE_STR
					 ,NULL                              -- ATTR_VALUE_NUM
					 ,NULL								-- ATTR_VALUE_DATE
					 ,NULL                                   -- ATTR_DISP_VALUE
					 ,NULL                              -- ATTR_UNIT_OF_MEASURE
					 ,ego_import_row_seq_s.CURRVAL      -- USER_ROW_IDENTIFIER
					); 

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
	slc_write_log_p(gv_log,'');	  
	EXCEPTION
	WHEN OTHERS THEN
	slc_write_log_p(gv_log,'Unexpected error in slc_process_uda_attributes_p for '||p_in_entity_name||' Error Message:'||SQLERRM);
	p_out_error_flag := 'Y';
	p_out_err_msg	:= 'Unexpected error in slc_process_uda_attributes_p for '||p_in_entity_name||' Error Message:'||SQLERRM;		
	END slc_process_uda_attributes_p;	

/* ****************************************************************
	NAME:              slc_update_supplier_site_p
	PURPOSE:           This procedure will create Supplier Site
*****************************************************************/
 PROCEDURE slc_update_supplier_site_p(p_in_party_id				IN 		NUMBER
									,p_in_addressline1			IN 		VARCHAR2
									,p_in_addressline2			IN 		VARCHAR2	DEFAULT NULL
									,p_in_city					IN 		VARCHAR2	DEFAULT NULL
									,p_in_state					IN 		VARCHAR2	DEFAULT NULL
									,p_in_county					IN 		VARCHAR2	DEFAULT NULL
									,p_in_country				IN 		VARCHAR2	DEFAULT NULL
									,p_in_zip					IN 		VARCHAR2	DEFAULT NULL
									 ,p_out_error_flag		OUT VARCHAR2
									 ,p_out_err_msg			OUT VARCHAR2
									  )
 IS
  p_api_version 		NUMBER			DEFAULT 1.0;
  p_init_msg_list 		VARCHAR2(200)   DEFAULT FND_API.G_TRUE;
  p_commit 				VARCHAR2(200)   DEFAULT FND_API.G_FALSE;
  p_validation_level 	NUMBER;
  lv_error_flag		VARCHAR2(1) DEFAULT 'N';
  lv_error_msg		VARCHAR2(4000) DEFAULT NULL;
  ln_msg_count             NUMBER;	
  lv_msg                  VARCHAR2(4000);
  lv_msg_out  NUMBER;
  lv_msg_data              VARCHAR2(1000);
  lv_return_status         VARCHAR2(10);
	
  p_vendor_site_rec 	apps.ap_vendor_pub_pkg.r_vendor_site_rec_type;
  p_vendor_site_id 		NUMBER;
  p_calling_prog 		VARCHAR2(200); 
  
  CURSOR cur_update_site
  IS
	SELECT sups.vendor_id,
	  sups.vendor_site_id,
	  sups.location_id
	FROM ap_suppliers sup ,
	  ap_supplier_sites_all sups
	WHERE sup.party_id = p_in_party_id
	AND sups.vendor_id = sup.vendor_id;
  
 BEGIN
    slc_write_log_p(gv_log,'');	  
    slc_write_log_p(gv_log,'In slc_update_supplier_site_p p_in_party_id:'||p_in_party_id);
	
	-- Supplier might have multiple sites.
	-- We need to update Address information for all Sites.
	-- Loop through all the sites and updates its address details.
	FOR c_update_rec IN cur_update_site
	LOOP
		slc_write_log_p(gv_log,'In slc_update_supplier_site_p vendor_site_id:'||c_update_rec.vendor_site_id||
							   ' location_id:'||c_update_rec.location_id);
		p_vendor_site_id := c_update_rec.vendor_site_id;
		p_vendor_site_rec.location_id := c_update_rec.location_id;
		p_vendor_site_rec.address_line1 := p_in_addressline1;
		p_vendor_site_rec.ADDRESS_LINE2 := p_in_addressline2;
		p_vendor_site_rec.city := p_in_city;
		p_vendor_site_rec.state := p_in_state;
		p_vendor_site_rec.county := p_in_county;
		p_vendor_site_rec.country := p_in_country;
		p_vendor_site_rec.zip := p_in_zip;
	
		FND_MSG_PUB.Initialize;
		ap_vendor_pub_pkg.update_vendor_site_public(
					p_api_version => p_api_version,
					p_init_msg_list => p_init_msg_list,
					p_commit => p_commit,
					p_validation_level => p_validation_level,
					x_return_status => lv_return_status,
					x_msg_count => ln_msg_count,
					x_msg_data => lv_msg_data,
					p_vendor_site_rec => p_vendor_site_rec,
					p_vendor_site_id => p_vendor_site_id,
					p_calling_prog => p_calling_prog
				  );
			slc_write_log_p(gv_log,'In slc_update_supplier_site_p lv_return_status:'||lv_return_status);
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
				lv_error_msg := lv_error_msg||'~'||lv_msg_data;
			END IF;
			END IF;
		END LOOP;
	p_out_error_flag := lv_error_flag;
	IF lv_error_flag = 'Y' THEN
		p_out_err_msg	:= 'Error while updating Supplier Site. Error Message:'||lv_error_msg;
	END IF;
	slc_write_log_p(gv_log,'In slc_update_supplier_site_p lv_error_flag:'||lv_error_flag||' lv_error_msg:'||lv_error_msg);
	slc_write_log_p(gv_log,'');	
 EXCEPTION
 WHEN OTHERS THEN
  slc_write_log_p(gv_log,'Unexpected error in slc_update_supplier_site_p. Error Message:'||SQLERRM);
  p_out_error_flag := 'Y';
  p_out_err_msg	:= 'Unexpected error in slc_update_supplier_site_p. Error Message:'||SQLERRM;		 
 END slc_update_supplier_site_p;
 
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
									,p_in_reference_id			IN 		NUMBER
									,p_in_vendor_site_code		IN 		VARCHAR2
									,p_in_pay_group_lookup_code IN 		VARCHAR2
									,p_in_pay_site_flag			IN 		VARCHAR2
									,p_in_addressline1			IN 		VARCHAR2
									,p_in_addressline2			IN 		VARCHAR2	DEFAULT NULL
									,p_in_city					IN 		VARCHAR2	DEFAULT NULL
									,p_in_state					IN 		VARCHAR2	DEFAULT NULL
									,p_in_county					IN 		VARCHAR2	DEFAULT NULL
									,p_in_country				IN 		VARCHAR2	DEFAULT NULL
									,p_in_zip					IN 		VARCHAR2	DEFAULT NULL
									,p_in_phone					IN 		VARCHAR2	DEFAULT NULL
									,p_in_ou						IN 		VARCHAR2
									,p_out_party_site_id	OUT NUMBER
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
	
	CURSOR cur_ou_name(p_in_store_number IN VARCHAR2)
	IS
	SELECT hou.name
	FROM fnd_flex_value_sets ffvs ,
	  fnd_flex_values ffv ,
	  fnd_lookup_values flv ,
	  gl_sets_of_books gsob ,
	  hr_operating_units hou
	WHERE ffvs.flex_value_set_name LIKE 'SLCGL_LOCATION'
	AND ffv.flex_value_set_id = ffvs.flex_value_set_id
	AND ffv.enabled_flag      = 'Y'
	AND ffv.flex_value        = p_in_store_number
	AND flv.lookup_type       = 'SLCGL_MAP_COMPANY_TO_COUNTRY'
	AND flv.enabled_flag      = 'Y'
	AND TRUNC(SYSDATE) BETWEEN TRUNC(NVL(flv.START_DATE_ACTIVE,SYSDATE)) AND TRUNC(NVL(flv.END_DATE_ACTIVE,SYSDATE))
	AND ffv.attribute10 = flv.lookup_code
	AND TRUNC(SYSDATE) BETWEEN TRUNC(NVL(ffv.START_DATE_ACTIVE,SYSDATE)) AND TRUNC(NVL(ffv.END_DATE_ACTIVE,SYSDATE))
	AND gsob.short_name     = flv.meaning
	AND hou.set_of_books_id = gsob.set_of_books_id;	
	lv_ou_name			hr_operating_units.name%TYPE;
	
	BEGIN
	
		/* If for same Supplier multiple stores are present with different address then 
		 * We will be using same location and creating new Supplier Site using same location. 
		 */
		SELECT sup.vendor_id  
		  INTO ln_vendor_id 
		 FROM ap_suppliers sup
		WHERE party_id = p_in_party_id;
		slc_write_log_p(gv_log,'');	
		slc_write_log_p(gv_log,'In slc_create_supplier_site_p p_in_party_id:'||p_in_party_id||' ln_vendor_id:'||ln_vendor_id);

		OPEN cur_supplier_loc(ln_vendor_id);
		FETCH cur_supplier_loc INTO ln_location_id;
		CLOSE cur_supplier_loc;
		
		-- Earlier Operating Unit name was hardcoded to SLC CONSOLIDATED
		-- Now based on Store Number , operating unit is derived.
		OPEN cur_ou_name(LPAD(p_in_vendor_site_code,7,'0'));
		FETCH cur_ou_name INTO lv_ou_name;
		CLOSE cur_ou_name;
		slc_write_log_p(gv_log,'In slc_create_supplier_site_p ln_location_id:'||ln_location_id||' p_in_pay_site_flag :'||p_in_pay_site_flag);
		slc_write_log_p(gv_log,'Calculated p_in_vendor_site_code:'||p_in_vendor_site_code||' lv_ou_name:'||lv_ou_name);
		
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
		
		--Changes for v1.4
		--l_vendor_site_rec.org_name            := p_in_ou;   
		l_vendor_site_rec.org_name            := lv_ou_name;
				
		l_vendor_site_rec.pay_site_flag         := p_in_pay_site_flag;
		l_vendor_site_rec.party_site_name		:= 'Home';
		l_vendor_site_rec.pay_group_lookup_code	:= p_in_pay_group_lookup_code;
		l_vendor_site_rec.attribute15			:= p_in_reference_id;
		
		slc_write_log_p(gv_log,'In slc_create_supplier_site_p: p_in_pay_site_flag '||l_vendor_site_rec.pay_site_flag);
			
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
	p_out_party_site_id	:= ln_party_site_id;
	IF lv_error_flag = 'Y' THEN
		p_out_err_msg	:= 'Error while creating Supplier Site. Error Message:'||lv_error_msg;
	END IF;
	slc_write_log_p(gv_log,'In slc_create_supplier_site_p lv_error_flag:'||lv_error_flag||' lv_error_msg:'||lv_error_msg);
	slc_write_log_p(gv_log,'');	
	EXCEPTION
	WHEN OTHERS THEN
	slc_write_log_p(gv_log,'Unexpected error in slc_create_supplier_site_p. Error Message:'||SQLERRM);
	p_out_error_flag := 'Y';
	p_out_err_msg	:= 'Unexpected error in slc_create_supplier_site_p. Error Message:'||SQLERRM;		
	END slc_create_supplier_site_p;	

--Changes for v1.3 Begin.
/* ****************************************************************
	NAME:              slc_make_bank_acct_eft_p		
	PURPOSE:           This procedure will change the bank account payment method to EFT at site level.
	Input Parameters:   
						p_in_party_id 				IN 	NUMBER
*****************************************************************/ 
 PROCEDURE slc_make_bank_acct_eft_p(p_in_party_id 	IN 	NUMBER
								  ,p_in_vendor_site_code		IN 		VARCHAR2
								  ,p_out_error_flag  	OUT VARCHAR2
								  ,p_out_err_msg		OUT VARCHAR2	
								  )
 IS
  P_API_VERSION NUMBER DEFAULT 1.0;
  P_INIT_MSG_LIST VARCHAR2(200) DEFAULT FND_API.G_TRUE;
  P_EXT_PAYEE_TAB APPS.IBY_DISBURSEMENT_SETUP_PUB.EXTERNAL_PAYEE_TAB_TYPE;
  P_EXT_PAYEE_ID_TAB APPS.IBY_DISBURSEMENT_SETUP_PUB.EXT_PAYEE_ID_TAB_TYPE;
  X_RETURN_STATUS VARCHAR2(200);
  X_MSG_COUNT NUMBER DEFAULT 0;
  X_MSG_DATA VARCHAR2(200);
  X_EXT_PAYEE_STATUS_TAB APPS.IBY_DISBURSEMENT_SETUP_PUB.EXT_PAYEE_UPDATE_TAB_TYPE;
 lv_error_flag		VARCHAR2(1) DEFAULT 'N';
 lv_error_msg		VARCHAR2(4000) DEFAULT NULL; 
  lv_msg                  VARCHAR2(4000);
  lv_msg_out  NUMBER;   
  
   CURSOR cur_ext_payee_info(p_in_party_id 			IN 	NUMBER
							,p_in_org_id 			IN NUMBER
							,p_in_party_site_id 	IN NUMBER
							,p_in_supplier_site_id	IN NUMBER
							)
   IS
	SELECT ext_payee_id
	FROM iby_external_payees_all
	WHERE payee_party_id             = p_in_party_id
	AND org_type                    = 'OPERATING_UNIT'
	AND org_id                      = p_in_org_id
	AND supplier_site_id            = p_in_supplier_site_id
	AND party_site_id               = p_in_party_site_id;
	ln_ext_payee_id					iby_external_payees_all.ext_payee_id%TYPE;
	ln_payee_party_site_id			iby_external_payees_all.party_site_id%TYPE;
	ln_supplier_site_id				iby_external_payees_all.supplier_site_id%TYPE;
	ln_org_id						iby_external_payees_all.org_id%TYPE;
	
 BEGIN
   	SELECT sups.vendor_site_id,sups.org_id ,sups.party_site_id
	  INTO ln_supplier_site_id,ln_org_id,ln_payee_party_site_id
	  FROM ap_supplier_sites_all sups
		  ,ap_suppliers sup
	 WHERE sup.party_id = p_in_party_id
	   and sup.vendor_id = sups.vendor_id
	   and vendor_site_code = p_in_vendor_site_code;

   slc_write_log_p(gv_log,'In slc_make_bank_acct_eft_p ln_supplier_site_id:'||ln_supplier_site_id||
						  ' ln_org_id:'||ln_org_id||' ln_payee_party_site_id:'||ln_payee_party_site_id);
	
   OPEN cur_ext_payee_info(p_in_party_id,ln_org_id,ln_payee_party_site_id,ln_supplier_site_id);
   FETCH cur_ext_payee_info INTO ln_ext_payee_id;
   CLOSE cur_ext_payee_info;
   
   slc_write_log_p(gv_log,'In slc_make_bank_acct_eft_p p_in_party_id:'||p_in_party_id||
						  ' ln_ext_payee_id:'||ln_ext_payee_id);
   

   P_EXT_PAYEE_TAB(1).Default_Pmt_method := 'EFT';
   P_EXT_PAYEE_TAB(1).Payee_Party_Id := p_in_party_id;
   P_EXT_PAYEE_TAB(1).Payment_Function   := 'PAYABLES_DISB';
   P_EXT_PAYEE_TAB(1).Exclusive_Pay_Flag   := 'N';
   p_ext_payee_tab(1).payee_party_site_id   := ln_payee_party_site_id;
   p_ext_payee_tab(1).supplier_site_id      := ln_supplier_site_id;
   p_ext_payee_tab(1).payer_org_id          := ln_org_id;
   p_ext_payee_tab(1).Payer_Org_Type       := 'OPERATING_UNIT';
   
   P_EXT_PAYEE_ID_TAB(1).Ext_Payee_ID := ln_ext_payee_id;

  IBY_DISBURSEMENT_SETUP_PUB.UPDATE_EXTERNAL_PAYEE(
    P_API_VERSION => P_API_VERSION,
    P_INIT_MSG_LIST => P_INIT_MSG_LIST,
    P_EXT_PAYEE_TAB => P_EXT_PAYEE_TAB,
    P_EXT_PAYEE_ID_TAB => P_EXT_PAYEE_ID_TAB,
    X_RETURN_STATUS => X_RETURN_STATUS,
    X_MSG_COUNT => X_MSG_COUNT,
    X_MSG_DATA => X_MSG_DATA,
    X_EXT_PAYEE_STATUS_TAB => X_EXT_PAYEE_STATUS_TAB
  ); 
	FOR i IN 1..X_EXT_PAYEE_STATUS_TAB.COUNT
	LOOP
		IF X_EXT_PAYEE_STATUS_TAB(i).Payee_Update_Status = 'E' THEN
		lv_error_flag := 'Y';
		lv_error_msg := lv_error_msg || ':' || X_EXT_PAYEE_STATUS_TAB(i).Payee_Update_Msg;
		END IF;
	END LOOP; 
	p_out_error_flag := lv_error_flag;
	p_out_err_msg	:= lv_error_msg;
	slc_write_log_p(gv_log,'In slc_make_bank_acct_eft_p lv_error_flag:'||lv_error_flag||' lv_error_msg:'||lv_error_msg);
	
 EXCEPTION
 WHEN OTHERS THEN
 slc_write_log_p(gv_log,'Unexpected error in slc_make_bank_acct_eft_p. Error Message:'||SQLERRM);
 p_out_error_flag := 'Y';
 p_out_err_msg	:= 'Unexpected error in slc_make_bank_acct_eft_p. Error Message:'||SQLERRM;  
 END slc_make_bank_acct_eft_p;
--Changes for v1.3 End
 
/* ****************************************************************
	NAME:              slc_manage_location_p
	PURPOSE:           This procedure will manage Address creation for Supplier and Corporation.
*****************************************************************/ 
 PROCEDURE slc_manage_location_p(p_in_franchisee_number 	IN VARCHAR2
									,p_in_franchisee_count	IN NUMBER
									,p_in_incorp_flag			IN 		VARCHAR2
									,p_in_party_id				IN 		NUMBER
									,p_in_reference_id			IN 		NUMBER
									,p_in_vendor_site_code		IN 		VARCHAR2
									,p_in_addressline1			IN 		VARCHAR2
									,p_in_addressline2			IN 		VARCHAR2	DEFAULT NULL
									,p_in_city					IN 		VARCHAR2	DEFAULT NULL
									,p_in_state					IN 		VARCHAR2	DEFAULT NULL
									,p_in_county					IN 		VARCHAR2	DEFAULT NULL
									,p_in_country				IN 		VARCHAR2	DEFAULT NULL
									,p_in_zip					IN 		VARCHAR2	DEFAULT NULL
									 ,p_out_party_site_id	OUT NUMBER
									 ,p_out_error_flag		OUT VARCHAR2
									 ,p_out_err_msg			OUT VARCHAR2
								    )
 IS
  	lv_error_flag		VARCHAR2(1) DEFAULT 'N';
	lv_error_msg		VARCHAR2(4000) DEFAULT NULL; 
	lv_pay_site_flag	VARCHAR2(1);
	lv_pay_group_lkp_code		VARCHAR2(25)	DEFAULT	'FRANCHISEE';	
	ln_party_site_id         NUMBER;
	
	--Changes for v1.4 Begin
	ln_party_site_count		 NUMBER 	DEFAULT 0;
	--Changes for v1.4 End
	
	--Changes for v1.3
	--Begin
	CURSOR cur_active_bank_acct(p_in_party_id  IN  NUMBER)
	IS
	SELECT DECODE(COUNT(1),0,'N','Y')
		FROM iby_account_owners iao ,
		  iby_ext_bank_accounts ieb
		WHERE iao.ext_bank_account_id  = ieb.ext_bank_account_id
		AND iao.account_owner_party_id = p_in_party_id
		AND EXISTS
		  (SELECT 1
		  FROM iby_external_payees_all iepa ,
			iby_pmt_instr_uses_all ipi
		  WHERE iepa.payee_party_id = iao.account_owner_party_id
		  AND iepa.ext_payee_id     = ipi.ext_pmt_party_id
		  AND ipi.instrument_id     = iao.ext_bank_account_id
		  AND sysdate BETWEEN NVL(ipi.start_date,sysdate) AND NVL(ipi.end_date,sysdate)
		  );
	 lv_active_bank_account_exists	VARCHAR2(1)  DEFAULT 'N';
	 --Change for v1.3 End
 BEGIN
 slc_write_log_p(gv_log,'');	
 slc_write_log_p(gv_log,'In slc_manage_location_p p_in_franchisee_number:'||p_in_franchisee_number ||
							' p_in_franchisee_count:'||p_in_franchisee_count||' p_in_party_id:'||p_in_party_id||
							' p_in_incorp_flag:'||p_in_incorp_flag);
							
 -- If Supplier is already existing then update Address details for supplier
 -- and for all its site update its site details.
 -- For corporation and for all Franchisee rule for updating address details is same.
 IF p_in_franchisee_count <> 0 THEN
	 slc_update_supplier_site_p(p_in_party_id				
								,p_in_addressline1			
								,p_in_addressline2			
								,p_in_city					
								,p_in_state					
								,p_in_county				
								,p_in_country			
								,p_in_zip				
								,lv_error_flag		
								,lv_error_msg			
								);
 END IF;
 slc_write_log_p(gv_log,'In slc_manage_location_p. After calling slc_update_supplier_site_p lv_error_flag:'||lv_error_flag||
						' lv_error_msg:'||lv_error_msg);
 --Changes for v1.4 Begin
 -- Added call to procedure slc_get_store_count_p. 
 --If Store Is not present for Supplier then call API to create Store for Supplier. If Store is already assigned to Supplier then
 --skip the step of Site creation for Supplier.
	slc_get_store_count_p(p_in_party_id
					     ,p_in_vendor_site_code
						 ,ln_party_site_id
						 ,ln_party_site_count
						 ,lv_error_flag		
						 ,lv_error_msg							 
						 );
	slc_write_log_p(gv_log,'In slc_manage_location_p After slc_get_store_count_p call '||
							'ln_party_site_id:'||ln_party_site_id||' ln_party_site_count:'||ln_party_site_count);
 IF lv_error_flag = 'N' AND ln_party_site_count = 0 THEN
 
	  
 --Changes for v1.4 End.
 
	 --If we are creating Site for Corporation then Pay Site would be Y.
	 --If we are creating Site for Franchisee1 and if Incorp Flag is Y then Pay Site would be Y , else for Franchisee1 in case of 
	 -- Incorp Flag = N Pay Site Flag would be N.
	 -- For all other Franchisee Pay Site Flag would be N.
	 IF p_in_incorp_flag = 'Y' AND p_in_franchisee_number = 'CORPORATION'	THEN
		lv_pay_site_flag := 'Y';
	 ELSIF p_in_incorp_flag = 'N' AND p_in_franchisee_number = 'FRANCHISEE1'	THEN
		lv_pay_site_flag := 'Y';
	 ELSE
		lv_pay_site_flag := 'N';
	 END IF;
	 slc_write_log_p(gv_log,'In slc_manage_location_p.lv_pay_site_flag '||lv_pay_site_flag||' p_in_incorp_flag :'||p_in_incorp_flag||
							' p_in_franchisee_number '||p_in_franchisee_number);
	 slc_create_supplier_site_p(p_in_party_id				
										,p_in_reference_id			
										,p_in_vendor_site_code		
										,lv_pay_group_lkp_code --Pay Group Lookup Code 
										,lv_pay_site_flag			
										,p_in_addressline1			
										,p_in_addressline2			
										,p_in_city					
										,p_in_state					
										,p_in_county				
										,p_in_country				
										,p_in_zip					
										,NULL	--Phone Number					
										,'SLC CONSOLIDATED'--Operating Unit Name					
										,ln_party_site_id	
										,lv_error_flag		
										,lv_error_msg										
									);
  END IF;
   --Changes for v1.4 End
   
   --Changes for v1.3
   --If Supplier has active bank account then update the default payment method for the site created to EFT.
   IF lv_error_flag = 'N' THEN
   OPEN cur_active_bank_acct(p_in_party_id);
   FETCH cur_active_bank_acct INTO lv_active_bank_account_exists;
   CLOSE cur_active_bank_acct;
   slc_write_log_p(gv_log,'In slc_manage_location_p Supplier has bank account:'||lv_active_bank_account_exists);
		
	   --If Supplier has Active Bank Account then call procedure to update Payment Method to EFT.
	   IF lv_active_bank_account_exists = 'Y' THEN
		slc_make_bank_acct_eft_p(p_in_party_id 
								  ,p_in_vendor_site_code	
								  ,lv_error_flag  	
								  ,lv_error_msg			
								  );
	   END IF;
   
   END IF;
	p_out_error_flag := lv_error_flag;
	p_out_party_site_id	:= ln_party_site_id;
	IF lv_error_flag = 'Y' THEN
		p_out_err_msg	:= 'Error in slc_manage_location_p for '||p_in_franchisee_number||' Error Message:'||lv_error_msg;
	END IF;
	slc_write_log_p(gv_log,'In slc_manage_location_p lv_error_flag:'||lv_error_flag||' lv_error_msg:'||lv_error_msg);
	slc_write_log_p(gv_log,'');	
 EXCEPTION
 WHEN OTHERS THEN
 slc_write_log_p(gv_log,'Unexpected error in slc_manage_location_p for '||p_in_franchisee_number||' Error Message:'||SQLERRM);
 p_out_error_flag := 'Y';
 p_out_err_msg	:= 'Unexpected error in slc_manage_location_p for '||p_in_franchisee_number||' Error Message:'||SQLERRM;		
 END slc_manage_location_p;	
	
 /*	*****************************************************************
  NAME:              slc_create_franchisee_p
  PURPOSE:           This procedure is a wrapper procedure which is called for 
							creating Franchisee from 
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
  PROCEDURE slc_create_franchisee_p(p_in_franchisee_number 	IN VARCHAR2
									,p_in_org_type			IN VARCHAR2
									,p_in_franchisee_count	IN NUMBER
									,p_in_first_name		IN VARCHAR2
									,p_in_middle_name		IN VARCHAR2
									,p_in_last_name		    IN VARCHAR2
									,p_in_ssn				IN VARCHAR2
									,p_in_dob				IN 		VARCHAR2
									,p_in_bkgrd				IN 		VARCHAR2
									,p_in_marital			IN 		VARCHAR2
									,p_in_sex				IN		VARCHAR2
									,p_in_email_address		IN		VARCHAR2
									,p_in_phone_number		IN		VARCHAR2
									,p_in_conversion_source	IN 		VARCHAR2
									,p_in_original_date		IN 		VARCHAR2
									,p_in_effec_begin_date	IN 		VARCHAR2
									,p_in_effec_end_date		IN 		VARCHAR2
									,p_in_zid				IN 		VARCHAR2
									,p_in_veteran           IN 		VARCHAR2
									,p_out_party_id			IN OUT NUMBER
									,p_out_vendor_id		IN OUT NUMBER
									,p_out_error_flag		OUT VARCHAR2
									,p_out_err_msg			OUT VARCHAR2
								    )
  IS
	lv_vendor_type_lkp_code		VARCHAR2(25)  	DEFAULT 'FRANCHISEE';
	lv_term_name					VARCHAR2(25)  	DEFAULT 'IMMEDIATE';
	lv_pay_date_lkp_code			VARCHAR2(25)	DEFAULT	'DISCOUNT';
	lv_pay_group_lkp_code		VARCHAR2(25)	DEFAULT	'FRANCHISEE';
	lv_invoice_currency_code		VARCHAR2(25)	DEFAULT	'USD';
	lv_payment_currency_code		VARCHAR2(25)	DEFAULT	'USD';
	lv_corporation				VARCHAR2(50)	DEFAULT 'Corporation';
	lv_sole						VARCHAR2(50)	DEFAULT 'Sole Proprietorship';
	lv_legal_status_txt			VARCHAR2(20)	DEFAULT 'LEGAL_STATUS';
	lv_organization_type_fran	VARCHAR2(1) 	DEFAULT 'X'; --INDIVIDUAL (NON-SERVICE) Lookup Name:"ORGANIZATION TYPE"
	lv_organization_type_corp	VARCHAR2(1) 	DEFAULT 'C'; -- CORPORATION/OTHER  Lookup Name:"ORGANIZATION TYPE"
	lv_organization_type_llp		VARCHAR2(1) 	DEFAULT 'P'; -- PARTNERSHIP/LLC/LLP/OTHER  Lookup Name:"ORGANIZATION TYPE"
	lv_org_type					VARCHAR2(1);
	lv_vendor_name				VARCHAR2(1000);	
 	lv_error_flag		VARCHAR2(1) DEFAULT 'N';
	lv_error_msg		VARCHAR2(4000) DEFAULT NULL; 
	lv_organization_type_code	VARCHAR2(1);
	lv_vendor_num        AP_SUPPLIERS.SEGMENT1%TYPE;
	
	ln_temp_count			NUMBER;
	CURSOR cur_org_type
	IS
	SELECT ffvl.description
	FROM fnd_flex_value_sets ffvs ,
	  fnd_flex_values ffv ,
	  fnd_flex_values_vl ffvl
	WHERE ffvs.flex_value_set_name = 'SLCISP_FRANC_CORP_TYPE_VS'
	AND ffvs.flex_value_set_id     = ffv.flex_value_set_id
	AND ffv.enabled_flag           = 'Y'
	AND ffv.flex_value_id          = ffvl.flex_value_id
	AND ffv.flex_value 			   = p_in_org_type
	AND TRUNC(sysdate) BETWEEN TRUNC(NVL(ffv.start_date_active,sysdate)) AND TRUNC(NVL(ffv.end_date_active,sysdate));
  BEGIN
	lv_vendor_name := p_in_first_name||
					  (CASE WHEN p_in_middle_name IS NOT NULL
							THEN ' '||p_in_middle_name
							ELSE ''
					  END)||
					  (CASE WHEN p_in_last_name IS NOT NULL
							THEN ' '||p_in_last_name
							ELSE ''
					  END);	
					  
	-- If Organization names has LLP in the end then organization type would be PARTNERSHIP/LLC/LLP/OTHER
	-- else it would be CORPORATION/OTHER
	--Commenting earlier logic for Org type derivation.
	--Now it will be derived as follows
	--		Corp Type in FranConnect 		Org Type in EBS
	--		Partnership						PARTNERSHIP (NON-SERVICE) - Code (Y)
	--		Corporation						CORPORATION/OTHER		  - Code (C)
	--		LLC C Corp						CORPORATION/OTHER		  - Code (C)
	--		LLC S Corp						CORPORATION/OTHER		  - Code (C)
	--		LLC Partnership					CORPORATION/OTHER		  - Code (P)
	
	/*IF p_in_franchisee_number = 'CORPORATION' AND lv_vendor_name LIKE '%LLC' THEN
	lv_organization_type_code := lv_organization_type_llp;
	ELSIF p_in_franchisee_number = 'CORPORATION' THEN
	lv_organization_type_code := lv_organization_type_corp;*/
	--Derivation for Corporation
	IF p_in_franchisee_number = 'CORPORATION' THEN
	 OPEN cur_org_type;
	 FETCH cur_org_type INTO lv_organization_type_code;
	 CLOSE cur_org_type;
	 
	 IF lv_organization_type_code IS NULL THEN
		lv_organization_type_code := lv_organization_type_corp;
	 END IF;
	 --Derivation for Franchisee
	ELSE
	lv_organization_type_code := lv_organization_type_fran;
	END IF;	
	slc_write_log_p(gv_log,'');	
	slc_write_log_p(gv_log,'In slc_create_franchisee_p p_in_franchisee_number:'||p_in_franchisee_number ||
							' p_out_party_id:'||p_out_party_id||' p_out_vendor_id:'||p_out_vendor_id||
							' p_in_franchisee_count:'||p_in_franchisee_count||' lv_organization_type_code '||lv_organization_type_code);
							
	slc_write_log_p(gv_log,'p_in_first_name '||p_in_first_name||'  p_in_last_name  '||p_in_last_name||' vendor id :'||p_out_vendor_id);						
	-- If Franchisee is not present then create Franchisee and call API to UDA's.
	IF p_in_franchisee_count = 0 THEN
	
		--Changes for v1.6.  Begin.
		-- Oracle does not assign vendor_id in sequence in incremental order and thus there was issue fetching segment1.
	 	--Fetching vendor number 
	   /*BEGIN
		SELECT tb.segment1 
		 INTO lv_vendor_num
		FROM 
		 (SELECT segment1 
		  FROM ap_suppliers 
		  ORDER BY vendor_id DESC)tb  
		WHERE ROWNUM=1;
		lv_vendor_num := lv_vendor_num+1;
		
		EXCEPTION
		WHEN OTHERS 
		THEN
		slc_write_log_p(gv_log,'Unexpected error while fetching segment number.');
		lv_error_flag := 'Y';
        lv_error_msg  := 'Unexpected error while fetching segment number. Error Message:'||SQLERRM;
	   END;*/
	  
		--Per new logic fetch segment1 from custom sequence.
		slc_write_log_p(gv_log,' ');
		slc_write_log_p(gv_log,'**********Generating Supplier Number Start***********');
		LOOP
		--Generate vendor number from 
		
			BEGIN
			lv_vendor_num := ISPAPPS.SLC_ISP_FRANCONN_VEN_NUM_S.nextval;
			slc_write_log_p(gv_log,'Next Supplier Number generated:'||lv_vendor_num);
			select 1 INTO ln_temp_count FROM (SELECT 1 FROM ap_suppliers WHERE segment1 = lv_vendor_num
			UNION
			SELECT 1 FROM po_history_vendors WHERE segment1 = lv_vendor_num);
			EXCEPTION
			WHEN NO_DATA_FOUND THEN
			lv_error_flag := 'N';
			EXIT;
			
			WHEN OTHERS THEN
			lv_error_flag := 'Y';
			lv_error_msg  := lv_error_msg||'~'||'Unexpected exception while fetching Supplier Number. Error Message:'||SQLERRM;
			EXIT;
			END;
		END LOOP;
		slc_write_log_p(gv_log,'**********Generating Supplier Number End***********');		
		slc_write_log_p(gv_log,' ');
	  --Changes for v1.6 End.	
	  slc_write_log_p(gv_log,'Supplier Number generated. lv_vendor_num:'||lv_vendor_num||' lv_error_flag:'||lv_error_flag||
							 ' lv_error_msg:'||lv_error_msg);
							 
	   IF lv_error_flag = 'N' THEN
		slc_create_supplier_p(p_in_franchisee_number
							,lv_vendor_name||'_'||lv_vendor_num--Vendor Name
							,lv_vendor_name	--Vendor Name Alt
							,lv_vendor_num -- Supplier number would be automatically generated 
							,lv_vendor_type_lkp_code	--Vendor Type Lookup Code
							,lv_term_name	--Term Name
							,lv_pay_date_lkp_code	-- Pay Date Basis Lookup Code 
							,lv_pay_group_lkp_code --Pay Group Lookup Code 
							,lv_invoice_currency_code	--Invoice Currency Code
							,lv_payment_currency_code	--Payment Currency Code
							,p_in_ssn	--jgzz fiscal code
							,lv_vendor_name	--Tax Reporting Name
							,lv_organization_type_code
							,p_out_party_id	--Party Id of the created franchisee.
							,p_out_vendor_id --Vendor Id of the created franchisee.
							,lv_error_flag
							,lv_error_msg
							);
		slc_write_log_p(gv_log,'After Franchisee1 creation p_out_party_id:'||p_out_party_id||' p_out_vendor_id:'||p_out_vendor_id);
		END IF;
		IF lv_error_flag = 'N' THEN
			slc_update_org_profile_p(p_out_party_id 	
									,lv_vendor_name 
									,lv_error_flag
									,lv_error_msg
									);			
		END IF;		
		--If there is no error while creating Org Profile then call API's to update UDA information.
		--Changes for v1.4 Begin.
		--Earlier for CORPORATION no record was inserted into UDA table because of which query to find if CORPORATION is unique used to fail 
		--and used to create records with duplicate tax payer id. For CORPORATION no record was created in UDA table because all details are NULL.
		--To fix the issue and keep design across conversion and Franconnect consistent adding record in UDA table even for CORPORATION.
		IF lv_error_flag = 'N' --AND p_in_franchisee_number <> 'CORPORATION' 
		THEN
			IF p_in_franchisee_number <> 'CORPORATION' THEN
				slc_write_log_p(gv_log,'Creating UDA for Individuals');
				slc_process_uda_attributes_p(p_in_franchisee_number
											  ,p_out_party_id
											  ,p_in_ssn
											  ,p_in_dob
											  ,p_in_bkgrd
											  ,p_in_marital
											  ,p_in_sex
											  ,p_in_conversion_source -- Conversion source must be FAS
											  ,p_in_original_date
											  ,p_in_effec_begin_date
											  ,p_in_effec_end_date
											  ,p_in_first_name
											  ,p_in_middle_name
											  ,p_in_last_name
											  ,p_in_zid
											  ,p_in_veteran
											  ,lv_error_flag
											  ,lv_error_msg											  
									  );
			ELSIF p_in_franchisee_number = 'CORPORATION' THEN
				slc_write_log_p(gv_log,'Creating UDA for Corporation');
				slc_process_uda_attributes_p(p_in_franchisee_number
											  ,p_out_party_id
											  ,NULL
											  ,NULL
											  ,NULL
											  ,NULL
											  ,NULL
											  ,p_in_conversion_source -- Conversion source must be FAS
											  ,NULL
											  ,NULL
											  ,NULL
											  ,NULL
											  ,NULL
											  ,NULL
											  ,NULL
											  ,NULL
											  ,lv_error_flag
											  ,lv_error_msg											  
									  );	
			END IF;
		END IF;
	END IF;
	

		
	--If there is no error while creating UDA then call API to create contact point
	-- For Individuals we need to create contact in Contact Directory
	-- For Corporation we need to create contact in Contact Point .
	IF lv_error_flag = 'N'
	THEN
		slc_create_contact_p   (p_in_franchisee_number
								,p_in_franchisee_count
								,p_out_party_id
								,p_out_vendor_id	
								,p_in_first_name 		
								,p_in_middle_name 	
								,p_in_last_name 		
								,p_in_email_address	
								,p_in_phone_number
								,lv_error_flag				
								,lv_error_msg					
							  );
	END IF;
	p_out_error_flag := lv_error_flag;
	p_out_err_msg := lv_error_msg;
	slc_write_log_p(gv_log,'In slc_create_franchisee_p lv_error_flag:'||lv_error_flag||' lv_error_msg:'||lv_error_msg);
	slc_write_log_p(gv_log,'');	
 EXCEPTION
 WHEN OTHERS THEN
 slc_write_log_p(gv_log,'Unexpected error in slc_create_franchisee_p for '||p_in_franchisee_number||' Error Message:'||SQLERRM);
 p_out_error_flag := 'Y';
 p_out_err_msg	:= 'Unexpected error in slc_create_franchisee_p for '||p_in_franchisee_number||' Error Message:'||SQLERRM;		
 END slc_create_franchisee_p;   
 
 /* ****************************************************************
	NAME:              slc_validate_p
	PURPOSE:           This procedure will be used to validate records fetched from FranConnect
 *****************************************************************/
 PROCEDURE slc_validate_p
 IS
 BEGIN
		slc_write_log_p(gv_log,'In slc_validate_p gn_batch_id:'||gn_batch_id);
	
		--Reset all the error messages.
		UPDATE slc_isp_franconnnect_supp_stg
		  SET error_msg = null
			,request_id = gn_request_id
			,last_update_date = sysdate
			,last_updated_by = gn_user_id
			,last_update_login = gn_login_id
		WHERE batch_id = gn_batch_id;
		
		--Validate that if First name and last name for Franchisee1 cannot be null.
		--If all 3 are null then error the record.
		UPDATE slc_isp_franconnnect_supp_stg
		  SET ERROR_MSG = 'Franchisee1:First Name and Last Name is null'
		WHERE batch_id = gn_batch_id
		  AND (FRANCHISEE1_FIRST_NAME IS NULL AND FRANCHISEE1_LAST_NAME IS NULL);
		  
		--Validate that if First name and last name for Franchisee1 cannot be null.
		--If all 3 are null then error the record.
		UPDATE slc_isp_franconnnect_supp_stg
		  SET ERROR_MSG = 'Franchisee1:First Name or Last Name is null'
		WHERE batch_id = gn_batch_id
		  AND (FRANCHISEE1_FIRST_NAME IS NULL OR FRANCHISEE1_LAST_NAME IS NULL);
		
		--SSN1 for Franchisee1 cannot be null.
		UPDATE slc_isp_franconnnect_supp_stg
		  SET ERROR_MSG = ERROR_MSG||'~Franchisee1:SSN is mandatory'
		WHERE batch_id = gn_batch_id
		  AND FRANCHISEE1_SSN IS NULL;
		  
		--If INCORP flag is Y i.e if it an incorporation then Federal Id and Corporation name is mandatory  
		UPDATE slc_isp_franconnnect_supp_stg
		  SET ERROR_MSG = ERROR_MSG||'~Corporation:Federal Id and Incorporation Name is null'
		WHERE batch_id = gn_batch_id
		  AND (INCORP_FLAG = 'Y' AND (FEDERAL_ID IS NULL OR INCORP_NAME IS NULL));
		
		--If SSN for Franchisee2 is not null then Franchisee2 First Name and Franchisee2 Last Name is null.
		UPDATE slc_isp_franconnnect_supp_stg
		  SET ERROR_MSG = ERROR_MSG||'~Franchisee2:First Name and Last Name is null'
		WHERE batch_id = gn_batch_id
		  AND (FRANCHISEE2_SSN IS NOT NULL AND (FRANCHISEE2_FIRST_NAME IS NULL AND FRANCHISEE2_LAST_NAME IS NULL));
		  
		--If SSN for Franchisee2 is not null then Franchisee2 First Name or Franchisee2 Last Name is null.
		UPDATE slc_isp_franconnnect_supp_stg
		  SET ERROR_MSG = ERROR_MSG||'~Franchisee2:First Name or Last Name is null'
		WHERE batch_id = gn_batch_id
		  AND (FRANCHISEE2_SSN IS NOT NULL AND (FRANCHISEE2_FIRST_NAME IS NULL OR FRANCHISEE2_LAST_NAME IS NULL));
		  
		--If SSN for Franchisee3 is not null then Franchisee3 First Name and Franchisee3 Last Name is null.
		UPDATE slc_isp_franconnnect_supp_stg
		  SET ERROR_MSG = ERROR_MSG||'~Franchisee3:First Name and Last Name is null'
		WHERE batch_id = gn_batch_id
		  AND (FRANCHISEE3_SSN IS NOT NULL AND (FRANCHISEE3_FIRST_NAME IS NULL AND FRANCHISEE3_LAST_NAME IS NULL));
		  
		--If SSN for Franchisee3 is not null then Franchisee3 First Name or Franchisee3 Last Name is null.
		UPDATE slc_isp_franconnnect_supp_stg
		  SET ERROR_MSG = ERROR_MSG||'~Franchisee3:First Name or Last Name is null'
		WHERE batch_id = gn_batch_id
		  AND (FRANCHISEE3_SSN IS NOT NULL AND (FRANCHISEE3_FIRST_NAME IS NULL OR FRANCHISEE3_LAST_NAME IS NULL));

		--If SSN for Franchisee4 is not null then Franchisee4 First Name and Franchisee4 Last Name is null.
		UPDATE slc_isp_franconnnect_supp_stg
		  SET ERROR_MSG = ERROR_MSG||'~Franchisee4:First Name and Last Name is null'
		WHERE batch_id = gn_batch_id
		  AND (FRANCHISEE4_SSN IS NOT NULL AND (FRANCHISEE4_FIRST_NAME IS NULL AND FRANCHISEE4_LAST_NAME IS NULL));	

 		--If SSN for Franchisee4 is not null then Franchisee4 First Name or Franchisee4 Last Name is null.
		UPDATE slc_isp_franconnnect_supp_stg
		  SET ERROR_MSG = ERROR_MSG||'~Franchisee4:First Name or Last Name is null'
		WHERE batch_id = gn_batch_id
		  AND (FRANCHISEE4_SSN IS NOT NULL AND (FRANCHISEE4_FIRST_NAME IS NULL OR FRANCHISEE4_LAST_NAME IS NULL));		  
				
		--If any one of the field AddressLine1 , City , State or Zip is null then we need to mark it as validation failure.
		UPDATE slc_isp_franconnnect_supp_stg
		  SET ERROR_MSG = ERROR_MSG||'~Address fields ADDRESS1,City1,State1,Zip1 is mandatory.'
		WHERE batch_id = gn_batch_id
		  AND (FRAN1_ADDRESS_LINE1 IS NULL OR CITY1 IS NULL OR ZIP1 IS NULL OR STATE1 IS NULL);

		UPDATE slc_isp_franconnnect_supp_stg
		  SET ERROR_MSG = ERROR_MSG||'~Address fields ADDRESS2,City2,State2,Zip2 is mandatory.'
		WHERE batch_id = gn_batch_id
		  AND FRANCHISEE2_SSN IS NOT NULL
		  AND (FRAN2_ADDRESS_LINE1 IS NULL OR CITY2 IS NULL OR ZIP2 IS NULL OR STATE2 IS NULL);	

		UPDATE slc_isp_franconnnect_supp_stg
		  SET ERROR_MSG = ERROR_MSG||'~Address fields ADDRESS3,City3,State3,Zip3 is mandatory.'
		WHERE batch_id = gn_batch_id
		  AND FRANCHISEE3_SSN IS NOT NULL
		  AND (FRAN3_ADDRESS_LINE1 IS NULL OR CITY3 IS NULL OR ZIP3 IS NULL OR STATE3 IS NULL);

		UPDATE slc_isp_franconnnect_supp_stg
		  SET ERROR_MSG = ERROR_MSG||'~Address fields ADDRESS4,City4,State4,Zip4 is mandatory.'
		WHERE batch_id = gn_batch_id
		  AND FRANCHISEE4_SSN IS NOT NULL
		  AND (FRAN4_ADDRESS_LINE1 IS NULL OR CITY4 IS NULL OR ZIP4 IS NULL OR STATE4 IS NULL);		  

		--For Minority Contacts Title,Ownership and Number of Shares is mandatory.
		UPDATE slc_isp_franconnnect_supp_stg
		  SET ERROR_MSG = ERROR_MSG||'~Minority1:Mandatory details for Minority contact is null'
		WHERE batch_id = gn_batch_id
		  AND (MIN_SHARE1_LAST_NAME IS NOT NULL AND (MIN_SHARE1_TITLE IS NULL OR MIN_SHARE1_OWNERSHIP IS NULL
													OR MIN_SHARE1_NO_OF_SHARES IS NULL));	

		--For Minority Contacts Title,Ownership and Number of Shares is mandatory.
		UPDATE slc_isp_franconnnect_supp_stg
		  SET ERROR_MSG = ERROR_MSG||'~Minority2:Mandatory details for Minority contact is null'
		WHERE batch_id = gn_batch_id
		  AND (MIN_SHARE2_LAST_NAME IS NOT NULL AND (MIN_SHARE2_TITLE IS NULL OR MIN_SHARE2_OWNERSHIP IS NULL
													OR MIN_SHARE2_NO_OF_SHARES IS NULL));	

		--For Minority Contacts Title,Ownership and Number of Shares is mandatory.
		UPDATE slc_isp_franconnnect_supp_stg
		  SET ERROR_MSG = ERROR_MSG||'~Minority3:Mandatory details for Minority contact is null'
		WHERE batch_id = gn_batch_id
		  AND (MIN_SHARE3_LAST_NAME IS NOT NULL AND (MIN_SHARE3_TITLE IS NULL OR MIN_SHARE3_OWNERSHIP IS NULL
													OR MIN_SHARE3_NO_OF_SHARES IS NULL));	
													
		--Validate Franchisee1 Background value
		UPDATE slc_isp_franconnnect_supp_stg stg
		  SET ERROR_MSG = ERROR_MSG||'~Franchisee1:Background value is invalid.'
		WHERE batch_id = gn_batch_id
		  AND stg.franchisee1_bkgrd IS NOT NULL
		  AND NOT EXISTS(SELECT 1
							FROM fnd_flex_value_sets ffvs ,
							  fnd_flex_values ffv ,
							  fnd_flex_values_tl ffvt
							WHERE ffvs.flex_value_set_name = 'SLCISP_BACKGROUND'
							AND ffvs.flex_value_set_id      = ffv.flex_value_set_id
							AND ffv.enabled_flag            = 'Y'
							AND TRUNC(SYSDATE) BETWEEN TRUNC(NVL(START_DATE_ACTIVE,SYSDATE)) AND TRUNC(NVL(END_DATE_ACTIVE,SYSDATE))
							AND ffvt.flex_value_meaning = stg.franchisee1_bkgrd
							AND ffv.flex_value_id = ffvt.flex_value_id
					    );

		--Validate Franchisee2 Background value
		UPDATE slc_isp_franconnnect_supp_stg stg
		  SET ERROR_MSG = ERROR_MSG||'~Franchisee2:Background value is invalid.'
		WHERE batch_id = gn_batch_id
		  AND stg.franchisee2_bkgrd IS NOT NULL
		  AND NOT EXISTS(SELECT 1
							FROM fnd_flex_value_sets ffvs ,
							  fnd_flex_values ffv ,
							  fnd_flex_values_tl ffvt
							WHERE ffvs.flex_value_set_name = 'SLCISP_BACKGROUND'
							AND ffvs.flex_value_set_id      = ffv.flex_value_set_id
							AND ffv.enabled_flag            = 'Y'
							AND TRUNC(SYSDATE) BETWEEN TRUNC(NVL(START_DATE_ACTIVE,SYSDATE)) AND TRUNC(NVL(END_DATE_ACTIVE,SYSDATE))
							AND ffvt.flex_value_meaning = stg.franchisee2_bkgrd
							AND ffv.flex_value_id = ffvt.flex_value_id
					    );						

		--Validate Franchisee3 Background value						
		UPDATE slc_isp_franconnnect_supp_stg stg
		  SET ERROR_MSG = ERROR_MSG||'~Franchisee3:Background value is invalid.'
		WHERE batch_id = gn_batch_id
		  AND stg.franchisee3_bkgrd IS NOT NULL
		  AND NOT EXISTS(SELECT 1
							FROM fnd_flex_value_sets ffvs ,
							  fnd_flex_values ffv ,
							  fnd_flex_values_tl ffvt
							WHERE ffvs.flex_value_set_name = 'SLCISP_BACKGROUND'
							AND ffvs.flex_value_set_id      = ffv.flex_value_set_id
							AND ffv.enabled_flag            = 'Y'
							AND TRUNC(SYSDATE) BETWEEN TRUNC(NVL(START_DATE_ACTIVE,SYSDATE)) AND TRUNC(NVL(END_DATE_ACTIVE,SYSDATE))
							AND ffvt.flex_value_meaning = stg.franchisee3_bkgrd
							AND ffv.flex_value_id = ffvt.flex_value_id
					    );	

		--Validate Franchisee4 Background value			
		UPDATE slc_isp_franconnnect_supp_stg stg
		  SET ERROR_MSG = ERROR_MSG||'~Franchisee4:Background value is invalid.'
		WHERE batch_id = gn_batch_id
		  AND stg.franchisee4_bkgrd IS NOT NULL
		  AND NOT EXISTS(SELECT 1
							FROM fnd_flex_value_sets ffvs ,
							  fnd_flex_values ffv ,
							  fnd_flex_values_tl ffvt
							WHERE ffvs.flex_value_set_name = 'SLCISP_BACKGROUND'
							AND ffvs.flex_value_set_id      = ffv.flex_value_set_id
							AND ffv.enabled_flag            = 'Y'
							AND TRUNC(SYSDATE) BETWEEN TRUNC(NVL(START_DATE_ACTIVE,SYSDATE)) AND TRUNC(NVL(END_DATE_ACTIVE,SYSDATE))
							AND ffvt.flex_value_meaning = stg.franchisee4_bkgrd
							AND ffv.flex_value_id = ffvt.flex_value_id
					    );							

		--Validate Franchisee1 Gender value						
		UPDATE slc_isp_franconnnect_supp_stg stg
		  SET ERROR_MSG = ERROR_MSG||'~Franchisee1:Sex value is invalid.'
		WHERE batch_id = gn_batch_id
		  AND stg.franchisee1_sex IS NOT NULL
		  AND NOT EXISTS(SELECT 1
							FROM fnd_flex_value_sets ffvs ,
							  fnd_flex_values ffv ,
							  fnd_flex_values_tl ffvt
							WHERE ffvs.flex_value_set_name = 'SLCISP_GENDER'
							AND ffvs.flex_value_set_id      = ffv.flex_value_set_id
							AND ffv.enabled_flag            = 'Y'
							AND TRUNC(SYSDATE) BETWEEN TRUNC(NVL(START_DATE_ACTIVE,SYSDATE)) AND TRUNC(NVL(END_DATE_ACTIVE,SYSDATE))
							AND ffvt.flex_value_meaning = stg.franchisee1_sex
							AND ffv.flex_value_id = ffvt.flex_value_id
					    );

		--Validate Franchisee2 Gender value						
		UPDATE slc_isp_franconnnect_supp_stg stg
		  SET ERROR_MSG = ERROR_MSG||'~Franchisee2:Sex value is invalid.'
		WHERE batch_id = gn_batch_id
		  AND stg.franchisee2_sex IS NOT NULL
		  AND NOT EXISTS(SELECT 1
							FROM fnd_flex_value_sets ffvs ,
							  fnd_flex_values ffv ,
							  fnd_flex_values_tl ffvt
							WHERE ffvs.flex_value_set_name = 'SLCISP_GENDER'
							AND ffvs.flex_value_set_id      = ffv.flex_value_set_id
							AND ffv.enabled_flag            = 'Y'
							AND TRUNC(SYSDATE) BETWEEN TRUNC(NVL(START_DATE_ACTIVE,SYSDATE)) AND TRUNC(NVL(END_DATE_ACTIVE,SYSDATE))
							AND ffvt.flex_value_meaning = stg.franchisee2_sex
							AND ffv.flex_value_id = ffvt.flex_value_id
					    );

		--Validate Franchisee3 Gender value						
		UPDATE slc_isp_franconnnect_supp_stg stg
		  SET ERROR_MSG = ERROR_MSG||'~Franchisee3:Sex value is invalid.'
		WHERE batch_id = gn_batch_id
		  AND stg.franchisee3_sex IS NOT NULL
		  AND NOT EXISTS(SELECT 1
							FROM fnd_flex_value_sets ffvs ,
							  fnd_flex_values ffv ,
							  fnd_flex_values_tl ffvt
							WHERE ffvs.flex_value_set_name = 'SLCISP_GENDER'
							AND ffvs.flex_value_set_id      = ffv.flex_value_set_id
							AND ffv.enabled_flag            = 'Y'
							AND TRUNC(SYSDATE) BETWEEN TRUNC(NVL(START_DATE_ACTIVE,SYSDATE)) AND TRUNC(NVL(END_DATE_ACTIVE,SYSDATE))
							AND ffvt.flex_value_meaning = stg.franchisee3_sex
							AND ffv.flex_value_id = ffvt.flex_value_id
					    );

		--Validate Franchisee4 Gender value						
		UPDATE slc_isp_franconnnect_supp_stg stg
		  SET ERROR_MSG = ERROR_MSG||'~Franchisee4:Sex value is invalid.'
		WHERE batch_id = gn_batch_id
		  AND stg.franchisee4_sex IS NOT NULL
		  AND NOT EXISTS(SELECT 1
							FROM fnd_flex_value_sets ffvs ,
							  fnd_flex_values ffv ,
							  fnd_flex_values_tl ffvt
							WHERE ffvs.flex_value_set_name = 'SLCISP_GENDER'
							AND ffvs.flex_value_set_id      = ffv.flex_value_set_id
							AND ffv.enabled_flag            = 'Y'
							AND TRUNC(SYSDATE) BETWEEN TRUNC(NVL(START_DATE_ACTIVE,SYSDATE)) AND TRUNC(NVL(END_DATE_ACTIVE,SYSDATE))
							AND ffvt.flex_value_meaning = stg.franchisee4_sex
							AND ffv.flex_value_id = ffvt.flex_value_id
					    );	

		--Validate Franchisee1 Marital Status value						
		UPDATE slc_isp_franconnnect_supp_stg stg
		  SET error_msg = error_msg||'~Franchisee1:Marital Status value is invalid.'
		WHERE batch_id = gn_batch_id
		  AND stg.franchisee1_marital IS NOT NULL
		  AND NOT EXISTS(SELECT 1
							FROM fnd_flex_value_sets ffvs ,
							  fnd_flex_values ffv ,
							  fnd_flex_values_tl ffvt
							WHERE ffvs.flex_value_set_name = 'SLCISP_MARITAL_STATUS'
							AND ffvs.flex_value_set_id      = ffv.flex_value_set_id
							AND ffv.enabled_flag            = 'Y'
							AND TRUNC(SYSDATE) BETWEEN TRUNC(NVL(START_DATE_ACTIVE,SYSDATE)) AND TRUNC(NVL(END_DATE_ACTIVE,SYSDATE))
							AND ffvt.flex_value_meaning = stg.franchisee1_marital
							AND ffv.flex_value_id = ffvt.flex_value_id
					    );	

		--Validate Franchisee2 Marital Status value						
		UPDATE slc_isp_franconnnect_supp_stg stg
		  SET error_msg = error_msg||'~Franchisee2:Marital Status value is invalid.'
		WHERE batch_id = gn_batch_id
		  AND stg.franchisee2_marital IS NOT NULL
		  AND NOT EXISTS(SELECT 1
							FROM fnd_flex_value_sets ffvs ,
							  fnd_flex_values ffv ,
							  fnd_flex_values_tl ffvt
							WHERE ffvs.flex_value_set_name = 'SLCISP_MARITAL_STATUS'
							AND ffvs.flex_value_set_id      = ffv.flex_value_set_id
							AND ffv.enabled_flag            = 'Y'
							AND TRUNC(SYSDATE) BETWEEN TRUNC(NVL(START_DATE_ACTIVE,SYSDATE)) AND TRUNC(NVL(END_DATE_ACTIVE,SYSDATE))
							AND ffvt.flex_value_meaning = stg.franchisee2_marital
							AND ffv.flex_value_id = ffvt.flex_value_id
					    );

		--Validate Franchisee3 Marital Status value						
		UPDATE slc_isp_franconnnect_supp_stg stg
		  SET error_msg = error_msg||'~Franchisee3:Marital Status value is invalid.'
		WHERE batch_id = gn_batch_id
		  AND stg.franchisee3_marital IS NOT NULL
		  AND NOT EXISTS(SELECT 1
							FROM fnd_flex_value_sets ffvs ,
							  fnd_flex_values ffv ,
							  fnd_flex_values_tl ffvt
							WHERE ffvs.flex_value_set_name = 'SLCISP_MARITAL_STATUS'
							AND ffvs.flex_value_set_id      = ffv.flex_value_set_id
							AND ffv.enabled_flag            = 'Y'
							AND TRUNC(SYSDATE) BETWEEN TRUNC(NVL(START_DATE_ACTIVE,SYSDATE)) AND TRUNC(NVL(END_DATE_ACTIVE,SYSDATE))
							AND ffvt.flex_value_meaning = stg.franchisee3_marital
							AND ffv.flex_value_id = ffvt.flex_value_id
					    );							

		--Validate Franchisee4 Marital Status value						
		UPDATE slc_isp_franconnnect_supp_stg stg
		  SET error_msg = error_msg||'~Franchisee4:Marital Status value is invalid.'
		WHERE batch_id = gn_batch_id
		  AND stg.franchisee4_marital IS NOT NULL
		  AND NOT EXISTS(SELECT 1
							FROM fnd_flex_value_sets ffvs ,
							  fnd_flex_values ffv ,
							  fnd_flex_values_tl ffvt
							WHERE ffvs.flex_value_set_name = 'SLCISP_MARITAL_STATUS'
							AND ffvs.flex_value_set_id      = ffv.flex_value_set_id
							AND ffv.enabled_flag            = 'Y'
							AND TRUNC(SYSDATE) BETWEEN TRUNC(NVL(START_DATE_ACTIVE,SYSDATE)) AND TRUNC(NVL(END_DATE_ACTIVE,SYSDATE))
							AND ffvt.flex_value_meaning = stg.franchisee4_marital
							AND ffv.flex_value_id = ffvt.flex_value_id
					    );							
		
		--Validate date formats.
		UPDATE slc_isp_franconnnect_supp_stg
		   SET franchisee1_birth_date = NULL
		WHERE batch_id = gn_batch_id
		  AND franchisee1_birth_date = '000000';

		UPDATE slc_isp_franconnnect_supp_stg
		   SET franchisee2_birth_date = NULL
		WHERE batch_id = gn_batch_id
		  AND franchisee2_birth_date = '000000';
		  
		UPDATE slc_isp_franconnnect_supp_stg
		   SET franchisee3_birth_date = NULL
		WHERE batch_id = gn_batch_id
		  AND franchisee3_birth_date = '000000';

		UPDATE slc_isp_franconnnect_supp_stg
		   SET franchisee4_birth_date = NULL
		WHERE batch_id = gn_batch_id
		  AND franchisee4_birth_date = '000000';		  
		
		--Update all Franchisee SSN value to 9 digits
		UPDATE slc_isp_franconnnect_supp_stg
		   SET franchisee1_ssn = LPAD(franchisee1_ssn,9,'0')
		 WHERE batch_id = gn_batch_id
		   AND franchisee1_ssn IS NOT NULL;
		   
		UPDATE slc_isp_franconnnect_supp_stg
		   SET franchisee2_ssn = LPAD(franchisee2_ssn,9,'0')
		 WHERE batch_id = gn_batch_id
		   AND franchisee2_ssn IS NOT NULL;	

		UPDATE slc_isp_franconnnect_supp_stg
		   SET franchisee3_ssn = LPAD(franchisee3_ssn,9,'0')
		 WHERE batch_id = gn_batch_id
		   AND franchisee3_ssn IS NOT NULL;		

		UPDATE slc_isp_franconnnect_supp_stg
		   SET franchisee4_ssn = LPAD(franchisee4_ssn,9,'0')
		 WHERE batch_id = gn_batch_id
		   AND franchisee4_ssn IS NOT NULL;				   

		--Pick all records for which FRANCHISEE1_BIRTH_DATE is not null and validate date format.
		UPDATE slc_isp_franconnnect_supp_stg
		   SET ERROR_MSG = DECODE(slc_is_date_valid_f(franchisee1_birth_date),
							'N', ERROR_MSG||'~Franchisee1:Birth Date invalid format',ERROR_MSG )   
		WHERE batch_id = gn_batch_id
		  AND franchisee1_birth_date IS NOT NULL;	
		  
		--Pick all records for which FRANCHISEE2_BIRTH_DATE is not null and validate date format.
		UPDATE slc_isp_franconnnect_supp_stg
		   SET ERROR_MSG = DECODE(slc_is_date_valid_f(franchisee2_birth_date),
							'N', ERROR_MSG||'~Franchisee2:Birth Date invalid format',ERROR_MSG )   
		WHERE batch_id = gn_batch_id
		  AND franchisee2_birth_date IS NOT NULL;			  

		--Pick all records for which FRANCHISEE3_BIRTH_DATE is not null and validate date format.
		UPDATE slc_isp_franconnnect_supp_stg
		   SET ERROR_MSG = DECODE(slc_is_date_valid_f(franchisee3_birth_date),
							'N', ERROR_MSG||'~Franchisee3:Birth Date invalid format',ERROR_MSG )   
		WHERE batch_id = gn_batch_id
		  AND franchisee3_birth_date IS NOT NULL;

		--Pick all records for which FRANCHISEE4_BIRTH_DATE is not null and validate date format.
		UPDATE slc_isp_franconnnect_supp_stg
		   SET ERROR_MSG = DECODE(slc_is_date_valid_f(franchisee4_birth_date),
							'N', ERROR_MSG||'~Franchisee4:Birth Date invalid format',ERROR_MSG )   
		WHERE batch_id = gn_batch_id
		  AND franchisee4_birth_date IS NOT NULL;
		  
		--If there is error then mark record status as Failed.
		UPDATE slc_isp_franconnnect_supp_stg
		  SET status = gv_invalid_status
		WHERE batch_id = gn_batch_id
		  AND ERROR_MSG IS NOT NULL;
		  
		--If there is no error then mark record status as Valid.
		UPDATE slc_isp_franconnnect_supp_stg
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
	  FROM slc_isp_franconnnect_supp_stg
	WHERE 1=1
	AND batch_id = gn_batch_id  
	  AND status = gv_valid_status
	  ORDER BY record_id;
	  
	  
	
	CURSOR c_store_exists(p_in_store_number IN VARCHAR2
						 ,p_in_reference_id IN NUMBER
						 )
	IS
	SELECT count(1)
	  FROM ap_supplier_sites_all
	 WHERE vendor_site_code =  p_in_store_number
	   AND attribute15 = p_in_reference_id;

	ln_store_exists_count		NUMBER  DEFAULT 0;
	
	CURSOR c_supplier_num(p_in_party_id	 IN	NUMBER)
	IS
	SELECT segment1
	 FROM ap_suppliers
	WHERE party_id = p_in_party_id;

	  
	TYPE lc_valid_supplier_tbl IS TABLE OF c_valid_supplier%ROWTYPE
	INDEX BY BINARY_INTEGER;

	lc_valid_supplier_tab             		lc_valid_supplier_tbl; 
	ln_index			NUMBER;
	

	lv_organization_type_code	VARCHAR2(1);
	
	lv_error_flag				VARCHAR2(1) DEFAULT 'N';
	lv_error_msg				VARCHAR2(4000);
	lv_vendor_name				VARCHAR2(1000);
	ln_fran1_party_id			HZ_PARTIES.party_id%TYPE  DEFAULT 0;
	ln_fran2_party_id			HZ_PARTIES.party_id%TYPE  DEFAULT 0;
	ln_fran3_party_id			HZ_PARTIES.party_id%TYPE  DEFAULT 0;
	ln_fran4_party_id			HZ_PARTIES.party_id%TYPE  DEFAULT 0;
	ln_corp_party_id			HZ_PARTIES.party_id%TYPE  DEFAULT 0;
	
	ln_fran1_vendor_id			ap_suppliers.vendor_id%TYPE  DEFAULT 0;
	ln_fran2_vendor_id			ap_suppliers.vendor_id%TYPE  DEFAULT 0;
	ln_fran3_vendor_id			ap_suppliers.vendor_id%TYPE  DEFAULT 0;
	ln_fran4_vendor_id			ap_suppliers.vendor_id%TYPE  DEFAULT 0;
	ln_corp_vendor_id			ap_suppliers.vendor_id%TYPE  DEFAULT 0;
	
	ln_fran1_party_site_id			hz_party_sites.party_site_id%TYPE  DEFAULT 0;
	ln_fran2_party_site_id			hz_party_sites.party_site_id%TYPE  DEFAULT 0;
	ln_fran3_party_site_id			hz_party_sites.party_site_id%TYPE  DEFAULT 0;
	ln_fran4_party_site_id			hz_party_sites.party_site_id%TYPE  DEFAULT 0;
	ln_corp_party_site_id			hz_party_sites.party_site_id%TYPE  DEFAULT 0;	
	
	lv_relationship_valid_flag	VARCHAR2(1);
	
	
	ln_main_party_id			HZ_PARTIES.party_id%TYPE;
	ln_main_vendor_id			ap_suppliers.vendor_id%TYPE;
	ln_main_party_site_id		hz_party_sites.party_site_id%TYPE;	
	
	ln_count					NUMBER;
	lv_pay_group_lkp_code		VARCHAR2(25)	DEFAULT	'FRANCHISEE';

	CURSOR cur_current_minority(p_in_corp_party_id  IN 	NUMBER
								,p_in_min1_first_name IN VARCHAR2
								,p_in_min1_middle_name IN VARCHAR2
								,p_in_min1_last_name IN VARCHAR2
								,p_in_min2_first_name IN VARCHAR2
								,p_in_min2_middle_name IN VARCHAR2
								,p_in_min2_last_name IN VARCHAR2	
								,p_in_min3_first_name IN VARCHAR2
								,p_in_min3_middle_name IN VARCHAR2
								,p_in_min3_last_name IN VARCHAR2	
								,p_in_min4_first_name IN VARCHAR2
								,p_in_min4_middle_name IN VARCHAR2
								,p_in_min4_last_name IN VARCHAR2	
								)
	IS
	SELECT hpc.person_first_name,
	  hpc.person_middle_name,
	  hpc.person_last_name,
	  hr.relationship_id,
	  hr.object_version_number object_version_number,
	  hp.object_version_number party_object_version_number
	FROM hz_relationships hr ,
	  hz_parties hpc ,
	  hz_parties hp
	WHERE hr.subject_id      = p_in_corp_party_id
	AND hr.object_id         = hpc.party_id
	AND hr.party_id          = hp.party_id
	AND hr.status            = 'A'
	--Changes for v1.5. Comparing names without considering case.
	--Making First Name,Middle Name and Last name as upper case before comparing.
	AND NOT( (p_in_min1_last_name  IS NOT NULL
	AND (UPPER(NVL(hpc.person_first_name,'X')) = UPPER(NVL(p_in_min1_first_name,'X'))
	AND UPPER(NVL(hpc.person_middle_name,'X')) = UPPER(NVL(p_in_min1_middle_name,'X'))
	AND UPPER(hpc.person_last_name)   = UPPER(p_in_min1_last_name) ))
	OR (p_in_min2_last_name        IS NOT NULL
	AND (UPPER(NVL(hpc.person_first_name,'X')) = UPPER(NVL(p_in_min2_first_name,'X'))
	AND UPPER(NVL(hpc.person_middle_name,'X')) = UPPER(NVL(p_in_min2_middle_name,'X'))
	AND UPPER(hpc.person_last_name)   = UPPER(p_in_min2_last_name )))
	OR (p_in_min3_last_name        IS NOT NULL
	AND (UPPER(NVL(hpc.person_first_name,'X')) = UPPER(NVL(p_in_min3_first_name,'X'))
	AND UPPER(NVL(hpc.person_middle_name,'X')) = UPPER(NVL(p_in_min3_middle_name,'X'))
	AND UPPER(hpc.person_last_name)   = UPPER(p_in_min3_last_name) ))
	OR (p_in_min4_last_name        IS NOT NULL
	AND (UPPER(NVL(hpc.person_first_name,'X')) = UPPER(NVL(p_in_min4_first_name,'X'))
	AND UPPER(NVL(hpc.person_middle_name,'X')) = UPPER(NVL(p_in_min4_middle_name,'X'))
	AND UPPER(hpc.person_last_name)   = UPPER(p_in_min4_last_name ))) )	
	AND hr.relationship_code = 'CONTACT';
	
	BEGIN
	
	slc_write_log_p(gv_log,'Importing data for Supplier');

	OPEN c_valid_supplier;
	LOOP
	lc_valid_supplier_tab.DELETE;
	FETCH c_valid_supplier
	BULK COLLECT INTO lc_valid_supplier_tab LIMIT 1000;
	EXIT WHEN lc_valid_supplier_tab.COUNT = 0;
	
		slc_write_log_p(gv_log,'lc_valid_supplier_tab count :'||lc_valid_supplier_tab.COUNT);
		
		--For all the valid records call API's to import supplier into Oracle Supplier Hub.
		FOR ln_index IN lc_valid_supplier_tab.FIRST..lc_valid_supplier_tab.LAST
		LOOP
			slc_write_log_p(gv_log,'*********************************Record Start**********************************');
			slc_write_log_p(gv_log,'Record Id:'||lc_valid_supplier_tab(ln_index).record_id);
			slc_write_log_p(gv_log,'Store Number:'||lc_valid_supplier_tab(ln_index).store_number||
									' Reference Id:'||lc_valid_supplier_tab(ln_index).reference_id);
			OPEN c_store_exists(lc_valid_supplier_tab(ln_index).store_number,lc_valid_supplier_tab(ln_index).reference_id );
			FETCH c_store_exists INTO ln_store_exists_count;
			CLOSE c_store_exists;
			slc_write_log_p(gv_log,'Store Count: ln_store_exists_count:'||ln_store_exists_count);
			
			IF ln_store_exists_count = 0 THEN
				SAVEPOINT supplier_savepoint;
				--Reinitialize local variables.
				lv_error_flag := 'N';
				lv_error_msg  := NULL;
				lv_vendor_name := NULL;
				
				ln_fran1_party_id	:= NULL;
				ln_fran2_party_id	:= NULL;
				ln_fran3_party_id	:= NULL;
				ln_fran4_party_id	:= NULL;
				ln_corp_party_id	:= NULL;
				                      
				ln_fran1_vendor_id	:= NULL;
				ln_fran2_vendor_id	:= NULL;
				ln_fran3_vendor_id	:= NULL;
				ln_fran4_vendor_id	:= NULL;
				ln_corp_vendor_id	:= NULL;
				                      
				
				
				ln_fran1_party_site_id	:= NULL;			
				ln_fran2_party_site_id	:= NULL;		
				ln_fran3_party_site_id	:= NULL;
				ln_fran4_party_site_id	:= NULL;
				ln_corp_party_site_id	:= NULL;
				                         
				ln_main_party_id		:= NULL;
				ln_main_vendor_id		:= NULL;
				ln_main_party_site_id	:= NULL;
	
				ln_count			:= 0;
				lv_organization_type_code := NULL;
				lv_relationship_valid_flag := 'Y';
				
				--Creating Supplier for First Franchisee if Franchisee1 SSN# is not null.

				IF lc_valid_supplier_tab(ln_index).franchisee1_ssn IS NOT NULL THEN
					
					slc_write_log_p(gv_log,'');
					slc_write_log_p(gv_log,'*********Creating Franchisee1:Start***********');
					slc_write_log_p(gv_log,'franchisee1_ssn:'||lc_valid_supplier_tab(ln_index).franchisee1_ssn );
					
					slc_supplier_exists_p(lc_valid_supplier_tab(ln_index).franchisee1_ssn 			
									  ,ln_fran1_party_id
									  ,ln_fran1_vendor_id
									  ,ln_count			
									  ,lv_error_flag
									  ,lv_error_msg												  
									  );
					
					slc_write_log_p(gv_log,'ln_fran1_party_id:'||ln_fran1_party_id||' ln_count:'||ln_count||' lv_error_flag:'||lv_error_flag);
					--If there is no error while validating if supplier exists and if Supplier is not existing 
					-- then call procedure to create Franchisee1.
						IF lv_error_flag = 'N'  THEN
							slc_create_franchisee_p('FRANCHISEE1' 									--Franchisee Number Identification
												,lc_valid_supplier_tab(ln_index).incorp_type				--Corporation Type
												,ln_count													--Franchisee Exists Count.
												,lc_valid_supplier_tab(ln_index).franchisee1_first_name		--First name
												,lc_valid_supplier_tab(ln_index).franchisee1_middle_name	--Middle Name
												,lc_valid_supplier_tab(ln_index).franchisee1_last_name		--Last name
												,lc_valid_supplier_tab(ln_index).franchisee1_ssn			--SSN
												,lc_valid_supplier_tab(ln_index).franchisee1_birth_date		--Birth Date
												,lc_valid_supplier_tab(ln_index).franchisee1_bkgrd			--Background
												,lc_valid_supplier_tab(ln_index).franchisee1_marital		--Marital Status
												,lc_valid_supplier_tab(ln_index).franchisee1_sex			--Sex
												,lc_valid_supplier_tab(ln_index).franchisee1_email			--Email
												,lc_valid_supplier_tab(ln_index).franchisee1_phone_number	--Phone Number
												,'FRANCONNECT'												--Conversion Source
												,NULL														--Original Date		
												,NULL														--Effective Begin Date
												,NULL 														--Effective End Date
												,NULL														--ZID
												,lc_valid_supplier_tab(ln_index).franchisee1_veteran
												,ln_fran1_party_id		
												,ln_fran1_vendor_id
												,lv_error_flag
												,lv_error_msg			
												);
								
						  END IF;
					slc_write_log_p(gv_log,'After Franchisee1 creation ln_fran1_party_id:'||ln_fran1_party_id||' ln_fran1_vendor_id:'||ln_fran1_vendor_id);
					slc_write_log_p(gv_log,'After Franchisee1 creation lv_error_flag:'||lv_error_flag||' lv_error_msg:'||lv_error_msg);
					
					IF lv_error_flag = 'N' THEN
						slc_manage_location_p('FRANCHISEE1'  										--Franchisee Number Identification
										,ln_count													--Franchisee Exists Count.
										,lc_valid_supplier_tab(ln_index).incorp_flag				--Incorp Flag			
										,ln_fran1_party_id		
										,lc_valid_supplier_tab(ln_index).reference_id
										,lc_valid_supplier_tab(ln_index).store_number
										,lc_valid_supplier_tab(ln_index).fran1_address_line1	
										,lc_valid_supplier_tab(ln_index).fran1_address_line2											
										,lc_valid_supplier_tab(ln_index).city1					
										,lc_valid_supplier_tab(ln_index).state1					
										,NULL 														--County				
										,lc_valid_supplier_tab(ln_index).country1			
										,lc_valid_supplier_tab(ln_index).zip1	
										,ln_fran1_party_site_id
										,lv_error_flag
										,lv_error_msg
										);
					END IF;
					IF (lv_error_flag = 'N' AND ( lc_valid_supplier_tab(ln_index).desig1_name IS NOT NULL OR
												  lc_valid_supplier_tab(ln_index).desig1_address IS NOT NULL OR
												  lc_valid_supplier_tab(ln_index).desig1_rel_to_fran IS NOT NULL OR
												  lc_valid_supplier_tab(ln_index).desig2_name IS NOT NULL OR
												  lc_valid_supplier_tab(ln_index).desig2_address IS NOT NULL OR
												  lc_valid_supplier_tab(ln_index).desig2_rel_to_fran IS NOT NULL OR
												  lc_valid_supplier_tab(ln_index).desig3_name IS NOT NULL OR
												  lc_valid_supplier_tab(ln_index).desig3_address IS NOT NULL OR
												  lc_valid_supplier_tab(ln_index).desig3_rel_to_fran IS NOT NULL 
												))
					THEN
						slc_populate_definer_uda_p(ln_fran1_party_id			
													,ln_fran1_vendor_id			
													,lc_valid_supplier_tab(ln_index).store_number	
													,lc_valid_supplier_tab(ln_index).desig1_name						 
													,lc_valid_supplier_tab(ln_index).desig1_address				 
													,lc_valid_supplier_tab(ln_index).desig1_rel_to_fran					 
													,lc_valid_supplier_tab(ln_index).desig2_name							 
													,lc_valid_supplier_tab(ln_index).desig2_address			 
													,lc_valid_supplier_tab(ln_index).desig2_rel_to_fran		 
													,lc_valid_supplier_tab(ln_index).desig3_name						 
													,lc_valid_supplier_tab(ln_index).desig3_address					 
													,lc_valid_supplier_tab(ln_index).desig3_rel_to_fran					 											
													,lv_error_flag
													,lv_error_msg			
													);
					END IF;					
					slc_write_log_p(gv_log,'After Franchisee1 manage location lv_error_flag:'||lv_error_flag||' lv_error_msg:'||lv_error_msg);
					slc_write_log_p(gv_log,'After Franchisee1 manage location ln_fran1_party_site_id:'||ln_fran1_party_site_id);
					slc_write_log_p(gv_log,'*********Creating Franchisee1:End***********');
					slc_write_log_p(gv_log,'');
					
				END IF;-- End of Franchisee1 SSN check

				--Creating Supplier for Second Franchisee if Franchisee2 SSN# is not null and there is no error in above code.
				IF lc_valid_supplier_tab(ln_index).franchisee2_ssn IS NOT NULL AND lv_error_flag = 'N' THEN
					slc_write_log_p(gv_log,'*********Creating Franchisee2:Start***********');
					slc_write_log_p(gv_log,'franchisee2_ssn:'||lc_valid_supplier_tab(ln_index).franchisee2_ssn );
			
					slc_supplier_exists_p(lc_valid_supplier_tab(ln_index).franchisee2_ssn 			
									  ,ln_fran2_party_id
                                      ,ln_fran2_vendor_id									  
									  ,ln_count			
									  ,lv_error_flag
									  ,lv_error_msg												  
									  );
					slc_write_log_p(gv_log,'ln_fran2_party_id:'||ln_fran2_party_id||' ln_count:'||ln_count||' lv_error_flag:'||lv_error_flag);
					--If there is no error while validating if supplier exists and if Supplier is not existing 
					-- then call procedure to create Franchisee2.
						IF lv_error_flag = 'N' THEN
							slc_create_franchisee_p('FRANCHISEE2'  									--Franchisee Number Identification
												,lc_valid_supplier_tab(ln_index).incorp_type				--Corporation Type
												,ln_count													--Franchisee Exists Count.
												,lc_valid_supplier_tab(ln_index).franchisee2_first_name		--First name
												,lc_valid_supplier_tab(ln_index).franchisee2_middle_name	--Middle Name
												,lc_valid_supplier_tab(ln_index).franchisee2_last_name		--Last name
												,lc_valid_supplier_tab(ln_index).franchisee2_ssn			--SSN
												,lc_valid_supplier_tab(ln_index).franchisee2_birth_date		--Birth Date
												,lc_valid_supplier_tab(ln_index).franchisee2_bkgrd			--Background
												,lc_valid_supplier_tab(ln_index).franchisee2_marital		--Marital Status
												,lc_valid_supplier_tab(ln_index).franchisee2_sex			--Sex
												,lc_valid_supplier_tab(ln_index).franchisee2_email			--Email
												,lc_valid_supplier_tab(ln_index).franchisee2_phone_number	--Phone Number
												,'FRANCONNECT'												--Conversion Source
												,NULL														--Original Date		
												,NULL														--Effective Begin Date
												,NULL 														--Effective End Date
												,NULL														--ZID
												,lc_valid_supplier_tab(ln_index).franchisee2_veteran        --VETERAN
												,ln_fran2_party_id	
												,ln_fran2_vendor_id
												,lv_error_flag
												,lv_error_msg			
												);
								
						  END IF;
					slc_write_log_p(gv_log,'After Franchisee2 creation ln_fran2_party_id:'||ln_fran2_party_id||' ln_fran2_vendor_id:'||ln_fran2_vendor_id);
					slc_write_log_p(gv_log,'After Franchisee2 creation lv_error_flag:'||lv_error_flag||' lv_error_msg:'||lv_error_msg);
					IF lv_error_flag = 'N' THEN
						slc_manage_location_p('FRANCHISEE2'  										--Franchisee Number Identification
										,ln_count													--Franchisee Exists Count.
										,lc_valid_supplier_tab(ln_index).incorp_flag				--Incorp Flag			
										,ln_fran2_party_id			
										,lc_valid_supplier_tab(ln_index).reference_id
										,lc_valid_supplier_tab(ln_index).store_number										
										,lc_valid_supplier_tab(ln_index).fran2_address_line1	
										,lc_valid_supplier_tab(ln_index).fran2_address_line2											
										,lc_valid_supplier_tab(ln_index).city2					
										,lc_valid_supplier_tab(ln_index).state2					
										,NULL 														--County				
										,lc_valid_supplier_tab(ln_index).country2			
										,lc_valid_supplier_tab(ln_index).zip2	
										,ln_fran2_party_site_id
										,lv_error_flag
										,lv_error_msg
										);
					END IF;
					slc_write_log_p(gv_log,'After Franchisee2 manage location lv_error_flag:'||lv_error_flag||' lv_error_msg:'||lv_error_msg);					
					slc_write_log_p(gv_log,'After Franchisee2 manage location ln_fran2_party_site_id:'||ln_fran2_party_site_id);
					slc_write_log_p(gv_log,'*********Creating Franchisee2:End***********');
					slc_write_log_p(gv_log,'');
				END IF;-- End of Franchisee2 SSN check			
				
				--Creating Supplier for Third Franchisee if Franchisee3 SSN# is not null and there is no error in above code.
				IF lc_valid_supplier_tab(ln_index).franchisee3_ssn IS NOT NULL AND lv_error_flag = 'N' THEN
					slc_write_log_p(gv_log,'*********Creating Franchisee3:Start***********');
					slc_write_log_p(gv_log,'franchisee3_ssn:'||lc_valid_supplier_tab(ln_index).franchisee3_ssn );
			
					slc_supplier_exists_p(lc_valid_supplier_tab(ln_index).franchisee3_ssn 			
									  ,ln_fran3_party_id
                                      ,ln_fran3_vendor_id									  
									  ,ln_count			
									  ,lv_error_flag
									  ,lv_error_msg												  
									  );
					slc_write_log_p(gv_log,'ln_fran3_party_id:'||ln_fran3_party_id||' ln_count:'||ln_count||' lv_error_flag:'||lv_error_flag);
					--If there is no error while validating if supplier exists and if Supplier is not existing 
					-- then call procedure to create Franchisee3.
						IF lv_error_flag = 'N' THEN
							slc_create_franchisee_p('FRANCHISEE3'  									--Franchisee Number Identification
												,lc_valid_supplier_tab(ln_index).incorp_type				--Corporation Type
												,ln_count													--Franchisee Exists Count.
												,lc_valid_supplier_tab(ln_index).franchisee3_first_name		--First name
												,lc_valid_supplier_tab(ln_index).franchisee3_middle_name	--Middle Name
												,lc_valid_supplier_tab(ln_index).franchisee3_last_name		--Last name
												,lc_valid_supplier_tab(ln_index).franchisee3_ssn			--SSN
												,lc_valid_supplier_tab(ln_index).franchisee3_birth_date		--Birth Date
												,lc_valid_supplier_tab(ln_index).franchisee3_bkgrd			--Background
												,lc_valid_supplier_tab(ln_index).franchisee3_marital		--Marital Status
												,lc_valid_supplier_tab(ln_index).franchisee3_sex			--Sex
												,lc_valid_supplier_tab(ln_index).franchisee3_email			--Email
												,lc_valid_supplier_tab(ln_index).franchisee3_phone_number	--Phone Number
												,'FRANCONNECT'												--Conversion Source
												,NULL														--Original Date		
												,NULL														--Effective Begin Date
												,NULL 														--Effective End Date
												,NULL														--ZID
												,lc_valid_supplier_tab(ln_index).franchisee3_veteran        --VETERAN
												,ln_fran3_party_id	
												,ln_fran3_vendor_id
												,lv_error_flag
												,lv_error_msg			
												);
								
						  END IF;
					slc_write_log_p(gv_log,'After Franchisee3 creation ln_fran3_party_id:'||ln_fran3_party_id||' ln_fran3_vendor_id:'||ln_fran3_vendor_id);
					slc_write_log_p(gv_log,'After Franchisee3 creation lv_error_flag:'||lv_error_flag||' lv_error_msg:'||lv_error_msg);
					IF lv_error_flag = 'N' THEN
						slc_manage_location_p('FRANCHISEE3'  										--Franchisee Number Identification
										,ln_count													--Franchisee Exists Count.
										,lc_valid_supplier_tab(ln_index).incorp_flag				--Incorp Flag			
										,ln_fran3_party_id		
										,lc_valid_supplier_tab(ln_index).reference_id
										,lc_valid_supplier_tab(ln_index).store_number										
										,lc_valid_supplier_tab(ln_index).fran3_address_line1	
										,lc_valid_supplier_tab(ln_index).fran3_address_line2											
										,lc_valid_supplier_tab(ln_index).city3					
										,lc_valid_supplier_tab(ln_index).state3					
										,NULL 														--County				
										,lc_valid_supplier_tab(ln_index).country3			
										,lc_valid_supplier_tab(ln_index).zip3	
										,ln_fran3_party_site_id
										,lv_error_flag
										,lv_error_msg
										);
					END IF;
					slc_write_log_p(gv_log,'After Franchisee3 manage location lv_error_flag:'||lv_error_flag||' lv_error_msg:'||lv_error_msg);
					slc_write_log_p(gv_log,'After Franchisee3 manage location ln_fran3_party_site_id:'||ln_fran3_party_site_id);
					slc_write_log_p(gv_log,'*********Creating Franchisee3:End***********');
					slc_write_log_p(gv_log,'');
				END IF;-- End of Franchisee3 SSN check	

				--Creating Supplier for Fourth Franchisee if Franchisee4 SSN# is not null and there is no error in above code.
				IF lc_valid_supplier_tab(ln_index).franchisee4_ssn IS NOT NULL AND lv_error_flag = 'N' THEN
					slc_write_log_p(gv_log,'*********Creating Franchisee4:Start***********');
					slc_write_log_p(gv_log,'franchisee4_ssn:'||lc_valid_supplier_tab(ln_index).franchisee4_ssn );
			
					slc_supplier_exists_p(lc_valid_supplier_tab(ln_index).franchisee4_ssn 			
									  ,ln_fran4_party_id
                                      ,ln_fran4_vendor_id									  
									  ,ln_count			
									  ,lv_error_flag
									  ,lv_error_msg												  
									  );
					slc_write_log_p(gv_log,'ln_fran4_party_id:'||ln_fran4_party_id||' ln_count:'||ln_count||' lv_error_flag:'||lv_error_flag);
					--If there is no error while validating if supplier exists and if Supplier is not existing 
					-- then call procedure to create Franchisee4.
						IF lv_error_flag = 'N' THEN
							slc_create_franchisee_p('FRANCHISEE4'  									--Franchisee Number Identification
												,lc_valid_supplier_tab(ln_index).incorp_type				--Corporation Type
												,ln_count													--Franchisee Exists Count.
												,lc_valid_supplier_tab(ln_index).franchisee4_first_name		--First name
												,lc_valid_supplier_tab(ln_index).franchisee4_middle_name	--Middle Name
												,lc_valid_supplier_tab(ln_index).franchisee4_last_name		--Last name
												,lc_valid_supplier_tab(ln_index).franchisee4_ssn			--SSN
												,lc_valid_supplier_tab(ln_index).franchisee4_birth_date		--Birth Date
												,lc_valid_supplier_tab(ln_index).franchisee4_bkgrd			--Background
												,lc_valid_supplier_tab(ln_index).franchisee4_marital		--Marital Status
												,lc_valid_supplier_tab(ln_index).franchisee4_sex			--Sex
												,lc_valid_supplier_tab(ln_index).franchisee4_email			--Email
												,lc_valid_supplier_tab(ln_index).franchisee4_phone_number	--Phone Number
												,'FRANCONNECT'												--Conversion Source
												,NULL														--Original Date		
												,NULL														--Effective Begin Date
												,NULL 														--Effective End Date
												,NULL														--ZID
												,lc_valid_supplier_tab(ln_index).franchisee4_veteran        --VETERAN
												,ln_fran4_party_id	
												,ln_fran4_vendor_id
												,lv_error_flag
												,lv_error_msg			
												);
								
						  END IF;
					slc_write_log_p(gv_log,'After Franchisee4 creation ln_fran4_party_id:'||ln_fran4_party_id||' ln_fran4_vendor_id:'||ln_fran4_vendor_id);
					slc_write_log_p(gv_log,'After Franchisee4 creation lv_error_flag:'||lv_error_flag||' lv_error_msg:'||lv_error_msg);
					IF lv_error_flag = 'N' THEN
						slc_manage_location_p('FRANCHISEE4'  										--Franchisee Number Identification
										,ln_count													--Franchisee Exists Count.
										,lc_valid_supplier_tab(ln_index).incorp_flag				--Incorp Flag			
										,ln_fran4_party_id				
										,lc_valid_supplier_tab(ln_index).reference_id
										,lc_valid_supplier_tab(ln_index).store_number										
										,lc_valid_supplier_tab(ln_index).fran4_address_line1	
										,lc_valid_supplier_tab(ln_index).fran4_address_line2											
										,lc_valid_supplier_tab(ln_index).city4					
										,lc_valid_supplier_tab(ln_index).state4					
										,NULL 														--County				
										,lc_valid_supplier_tab(ln_index).country4			
										,lc_valid_supplier_tab(ln_index).zip4	
										,ln_fran4_party_site_id
										,lv_error_flag
										,lv_error_msg
										);
					END IF;
					slc_write_log_p(gv_log,'After Franchisee4 manage location lv_error_flag:'||lv_error_flag||' lv_error_msg:'||lv_error_msg);						
					slc_write_log_p(gv_log,'After Franchisee4 manage location ln_fran4_party_site_id:'||ln_fran4_party_site_id);
					slc_write_log_p(gv_log,'*********Creating Franchisee4:End***********');
					slc_write_log_p(gv_log,'');
				END IF;-- End of Franchisee4 SSN check	
				
				--Creating Supplier for Corporation if Incorp Flag is Y and there is no error in above code.
				IF lc_valid_supplier_tab(ln_index).federal_id IS NOT NULL AND lv_error_flag = 'N' 
				AND lc_valid_supplier_tab(ln_index).incorp_flag = 'Y' THEN
					slc_write_log_p(gv_log,'*********Creating Corporation:Start***********');
					slc_write_log_p(gv_log,'Federal Id:'||lc_valid_supplier_tab(ln_index).federal_id );
					BEGIN
						SELECT hp.party_id 
						  INTO ln_corp_party_id
						 FROM hz_parties hp
							 ,pos_supp_prof_ext_b pos
							 ,ego_attr_groups_v eagv
							 ,fnd_application fa
						WHERE jgzz_fiscal_code = lc_valid_supplier_tab(ln_index).federal_id
						  AND hp.party_id = pos.party_id
						  AND eagv.attr_group_name = 'SLC_ISP_FRANCHISEE_DETAILS'
						  AND eagv.attr_group_id = pos.attr_group_id
						  AND fa.application_short_name = 'POS'
						  AND fa.application_id = eagv.application_id
						  AND pos.c_ext_attr4 IN ('FAS','FRANCONNECT');
						  
						  SELECT vendor_id
						  INTO ln_corp_vendor_id
						  FROM ap_suppliers
						  WHERE party_id = ln_corp_party_id;
						  
						  slc_write_log_p(gv_log,'ln_corp_party_id :'||ln_corp_party_id);
						  ln_count := 1;
						  lv_relationship_valid_flag := slc_validate_relationship_f(ln_corp_party_id
																					,ln_fran1_party_id
																					,ln_fran2_party_id
																					,ln_fran3_party_id
																					,ln_fran4_party_id
																				   );
					EXCEPTION
					WHEN NO_DATA_FOUND THEN
					ln_corp_party_id := NULL;
					ln_count		 := 0;
					WHEN OTHERS THEN
					ln_count	  := 0;
					lv_error_flag := 'Y';
					lv_error_msg  := 'Error while fetching Corporation information. Error Message:'||SQLERRM;
					END;
					 
					 --Changes for v1.4 Added error check.
					 IF lv_relationship_valid_flag = 'N' AND lv_error_flag = 'N'  THEN
					    lv_error_flag := 'Y';
					    lv_error_msg := lv_error_msg||'~Invalid Corporation Relationship';
					 ELSIF lv_relationship_valid_flag = 'Y'	AND lv_error_flag = 'N'  THEN		
					slc_write_log_p(gv_log,'ln_corp_party_id:'||ln_corp_party_id||' lv_error_flag:'||lv_error_flag);
					--If corporation does not exists then call procedure to create Corporation
						IF lv_error_flag = 'N' THEN
							slc_create_franchisee_p('CORPORATION'  									--Franchisee Number Identification
												,lc_valid_supplier_tab(ln_index).incorp_type				--Corporation Type
												,ln_count													--Franchisee Exists Count.
												,lc_valid_supplier_tab(ln_index).incorp_name                --First name
												,NULL														--Middle Name
												,NULL														--Last name
												,lc_valid_supplier_tab(ln_index).federal_id					--SSN
												,NULL														--Birth Date
												,NULL														--Background
												,NULL														--Marital Status
												,NULL														--Sex
												,lc_valid_supplier_tab(ln_index).franchisee1_email          --Email  
												,NULL														--Phone Number
												,'FRANCONNECT'												--Conversion Source
												,NULL														--Original Date		
												,NULL														--Effective Begin Date
												,NULL 														--Effective End Date
												,NULL														--ZID
												,NULL                                                       --VETERAN
												,ln_corp_party_id	
												,ln_corp_vendor_id
												,lv_error_flag
												,lv_error_msg			
												);
						END IF;
						slc_write_log_p(gv_log,'After Corporation creation ln_corp_party_id:'||ln_corp_party_id||' ln_corp_vendor_id:'||ln_corp_vendor_id);
						slc_write_log_p(gv_log,'After Corporation creation lv_error_flag:'||lv_error_flag||' lv_error_msg:'||lv_error_msg);
						IF lv_error_flag = 'N' THEN
							slc_manage_location_p('CORPORATION'  										--Franchisee Number Identification
											,ln_count													--Franchisee Exists Count.
											,lc_valid_supplier_tab(ln_index).incorp_flag				--Incorp Flag			
											,ln_corp_party_id				
											,lc_valid_supplier_tab(ln_index).reference_id
											,lc_valid_supplier_tab(ln_index).store_number										
											,lc_valid_supplier_tab(ln_index).fran1_address_line1	
											,lc_valid_supplier_tab(ln_index).fran1_address_line2											
											,lc_valid_supplier_tab(ln_index).city1					
											,lc_valid_supplier_tab(ln_index).state1					
											,NULL 														--County				
											,lc_valid_supplier_tab(ln_index).country1			
											,lc_valid_supplier_tab(ln_index).zip1	
											,ln_corp_party_site_id
											,lv_error_flag
											,lv_error_msg
											);
						END IF;
						slc_write_log_p(gv_log,'After Corporation manage location lv_error_flag:'||lv_error_flag||' lv_error_msg:'||lv_error_msg);					
						slc_write_log_p(gv_log,'After Corporation manage location ln_corp_party_site_id:'||ln_corp_party_site_id);
						slc_write_log_p(gv_log,'*********Creating Corporation:End***********');
					END IF;
					slc_write_log_p(gv_log,'');
				END IF;-- End of Corporation check	
				
				--Create Supplier Site
				/*IF lv_error_flag = 'N' THEN
					IF lc_valid_supplier_tab(ln_index).incorp_flag = 'Y' THEN
						ln_main_party_id		:= ln_corp_party_id;
						ln_main_vendor_id		:= ln_corp_vendor_id;
						ln_main_party_site_id	:= ln_corp_party_site_id;					
					ELSE
						ln_main_party_id		:= ln_fran1_party_id;
						ln_main_vendor_id		:= ln_fran1_vendor_id;
						ln_main_party_site_id	:= ln_fran1_party_site_id;						
					END IF;
				
					slc_write_log_p(gv_log,'Main party details. ln_main_party_id:'||ln_main_party_id||' ln_main_vendor_id:'||ln_main_vendor_id||
											' ln_main_party_site_id:'||ln_main_party_site_id);
					slc_write_log_p(gv_log,'');
				END IF;*/
				slc_write_log_p(gv_log,'Temp Count:'||ln_count);
				--Create relationship records for Incorporation records
				--Count condition is added because if Corporation is already existing then we should not call Relationship API
				-- For a given Corporation since number of Franchisee will not change there is no need to update
				-- relationship.
				IF lv_error_flag = 'N' AND ln_count = 0 
				   AND lc_valid_supplier_tab(ln_index).incorp_flag = 'Y' THEN
				    	   IF ln_fran1_party_id IS NOT NULL THEN
							slc_create_relationship_p( 'PARTNER'
											  ,ln_corp_party_id	 
											  ,ln_fran1_party_id	 
											  ,lc_valid_supplier_tab(ln_index).franchisee1_title		 
											  ,lc_valid_supplier_tab(ln_index).franchisee1_ownership	 
											  ,lc_valid_supplier_tab(ln_index).franchisee1_no_of_shares 
											  ,lv_error_flag
											  ,lv_error_msg
											 );
						   END IF;
							 
							IF lv_error_flag = 'N' AND ln_fran2_party_id IS NOT NULL THEN
							slc_create_relationship_p( 'PARTNER'
											  ,ln_corp_party_id	 	 
											  ,ln_fran2_party_id	 
											  ,lc_valid_supplier_tab(ln_index).franchisee2_title		 
											  ,lc_valid_supplier_tab(ln_index).franchisee2_ownership	 
											  ,lc_valid_supplier_tab(ln_index).franchisee2_no_of_shares 
											  ,lv_error_flag
											  ,lv_error_msg
											 );
							END IF;
							
							IF lv_error_flag = 'N' AND ln_fran3_party_id IS NOT NULL THEN
							slc_create_relationship_p( 'PARTNER'
											  ,ln_corp_party_id	 	 
											  ,ln_fran3_party_id	 
											  ,lc_valid_supplier_tab(ln_index).franchisee3_title		 
											  ,lc_valid_supplier_tab(ln_index).franchisee3_ownership	 
											  ,lc_valid_supplier_tab(ln_index).franchisee3_no_of_shares 
											  ,lv_error_flag
											  ,lv_error_msg
											 );
							END IF;
							IF lv_error_flag = 'N' AND ln_fran4_party_id IS NOT NULL THEN
				            slc_create_relationship_p( 'PARTNER'
									  ,ln_corp_party_id	 	 
									  ,ln_fran4_party_id	 
									  ,lc_valid_supplier_tab(ln_index).franchisee4_title		 
									  ,lc_valid_supplier_tab(ln_index).franchisee4_ownership	 
									  ,lc_valid_supplier_tab(ln_index).franchisee4_no_of_shares 
									  ,lv_error_flag
									  ,lv_error_msg
									 );
				            END IF;				   				 
				END IF;

				--Create relationship records for Incorporation records
				IF lv_error_flag = 'N'  
				AND lc_valid_supplier_tab(ln_index).incorp_flag = 'N' 
				AND lc_valid_supplier_tab(ln_index).franchisee1_ssn IS NOT NULL
				AND lc_valid_supplier_tab(ln_index).franchisee2_ssn IS NOT NULL
				AND lc_valid_supplier_tab(ln_index).franchisee3_ssn IS NULL
				AND lc_valid_supplier_tab(ln_index).franchisee4_ssn IS NULL
				THEN
				   slc_create_relationship_p( 'Spouse'
									  ,ln_fran1_party_id	 
									  ,ln_fran2_party_id	 
									  ,NULL		 
									  ,NULL
									  ,NULL
									  ,lv_error_flag
									  ,lv_error_msg
									 );
				END IF;
				
			-- If Minority Contacts is not present in current payload then end date 
			-- Minority Contact.
			slc_write_log_p(gv_log,'********End dating relationship:START***********');
			FOR min_rec IN cur_current_minority(ln_corp_party_id
												,lc_valid_supplier_tab(ln_index).min_share1_first_name
												,lc_valid_supplier_tab(ln_index).min_share1_middle_name
												,lc_valid_supplier_tab(ln_index).min_share1_last_name
												,lc_valid_supplier_tab(ln_index).min_share2_first_name
												,lc_valid_supplier_tab(ln_index).min_share2_middle_name
												,lc_valid_supplier_tab(ln_index).min_share2_last_name
												,lc_valid_supplier_tab(ln_index).min_share3_first_name
												,lc_valid_supplier_tab(ln_index).min_share3_middle_name
												,lc_valid_supplier_tab(ln_index).min_share3_last_name	
												,lc_valid_supplier_tab(ln_index).min_share4_first_name
												,lc_valid_supplier_tab(ln_index).min_share4_middle_name
												,lc_valid_supplier_tab(ln_index).min_share4_last_name													
												)
			LOOP
				slc_write_log_p(gv_log,'End dating for First name:'||min_rec.person_first_name||
										' Middle Name:'||min_rec.person_middle_name||
										' Last Name:'||min_rec.person_last_name);
				slc_end_relationship_p(min_rec.relationship_id
									  ,sysdate
									  ,min_rec.object_version_number
									  ,min_rec.party_object_version_number
									  ,lv_error_flag
									  ,lv_error_msg									 
									  );
				
				--If there is error in slc_end_relationship_p then exit the loop.
				IF lv_error_flag = 'Y' THEN
					EXIT;
				END IF;
			END LOOP;
			slc_write_log_p(gv_log,'********End dating relationship:END***********');
			
			IF lc_valid_supplier_tab(ln_index).incorp_flag = 'Y' THEN
				--Create Contact Directory for Minority Contact 1.
				IF	lc_valid_supplier_tab(ln_index).min_share1_last_name IS NOT NULL AND lv_error_flag = 'N' THEN
					slc_write_log_p(gv_log,'********Create Minority1 Contact:START***********');
					slc_create_contact_directory_p  (ln_corp_vendor_id
													,ln_corp_party_site_id			
													,lc_valid_supplier_tab(ln_index).min_share1_first_name 		
													,lc_valid_supplier_tab(ln_index).min_share1_middle_name 	 	
													,lc_valid_supplier_tab(ln_index).min_share1_last_name
													,lc_valid_supplier_tab(ln_index).min_share1_email 													
													,lc_valid_supplier_tab(ln_index).min_share1_title 						
													,lc_valid_supplier_tab(ln_index).min_share1_ownership 						
													,lc_valid_supplier_tab(ln_index).min_share1_no_of_shares 					
													,lv_error_flag
													,lv_error_msg					 
												  );
					slc_write_log_p(gv_log,'********Create Minority1 Contact:END***********');
				END IF;
				
				--Create Contact Directory for Minority Contact 2.
				IF	lc_valid_supplier_tab(ln_index).min_share2_last_name IS NOT NULL AND lv_error_flag = 'N' THEN
					slc_write_log_p(gv_log,'********Create Minority2 Contact:START***********');
					slc_create_contact_directory_p  (ln_corp_vendor_id
													,ln_corp_party_site_id		
													,lc_valid_supplier_tab(ln_index).min_share2_first_name 		
													,lc_valid_supplier_tab(ln_index).min_share2_middle_name 	 	
													,lc_valid_supplier_tab(ln_index).min_share2_last_name
													,lc_valid_supplier_tab(ln_index).min_share2_email 													
													,lc_valid_supplier_tab(ln_index).min_share2_title 						
													,lc_valid_supplier_tab(ln_index).min_share2_ownership 						
													,lc_valid_supplier_tab(ln_index).min_share2_no_of_shares 					
													,lv_error_flag
													,lv_error_msg					 
												  );
					slc_write_log_p(gv_log,'********Create Minority2 Contact:END***********');
				END IF;

				--Create Contact Directory for Minority Contact 3.
				IF	lc_valid_supplier_tab(ln_index).min_share3_last_name IS NOT NULL AND lv_error_flag = 'N' THEN
					slc_write_log_p(gv_log,'********Create Minority3 Contact:START***********');
					slc_create_contact_directory_p  (ln_corp_vendor_id
													,ln_corp_party_site_id			
													,lc_valid_supplier_tab(ln_index).min_share3_first_name 		
													,lc_valid_supplier_tab(ln_index).min_share3_middle_name 	 	
													,lc_valid_supplier_tab(ln_index).min_share3_last_name
													,lc_valid_supplier_tab(ln_index).min_share3_email 													
													,lc_valid_supplier_tab(ln_index).min_share3_title 						
													,lc_valid_supplier_tab(ln_index).min_share3_ownership 						
													,lc_valid_supplier_tab(ln_index).min_share3_no_of_shares 					
													,lv_error_flag
													,lv_error_msg					 
												  );
					slc_write_log_p(gv_log,'********Create Minority3 Contact:END***********');
				END IF;

				--Create Contact Directory for Minority Contact 4.
				IF	lc_valid_supplier_tab(ln_index).min_share4_last_name IS NOT NULL AND lv_error_flag = 'N' THEN
					slc_write_log_p(gv_log,'********Create Minority4 Contact:START***********');
					slc_create_contact_directory_p  (ln_corp_vendor_id
													,ln_corp_party_site_id			
													,lc_valid_supplier_tab(ln_index).min_share4_first_name 		
													,lc_valid_supplier_tab(ln_index).min_share4_middle_name 	 	
													,lc_valid_supplier_tab(ln_index).min_share4_last_name
													,lc_valid_supplier_tab(ln_index).min_share4_email 													
													,lc_valid_supplier_tab(ln_index).min_share4_title 						
													,lc_valid_supplier_tab(ln_index).min_share4_ownership 						
													,lc_valid_supplier_tab(ln_index).min_share4_no_of_shares 					
													,lv_error_flag
													,lv_error_msg					 
												  );
					slc_write_log_p(gv_log,'********Create Minority4 Contact:END***********');
				END IF;			
	        END IF;--End of Incorp flag condition, Minority contact check 
				--lv_error_flag := 'Y';--temp
				slc_write_log_p(gv_log,'Final flag values in import. lv_error_flag:'||lv_error_flag||' lv_error_msg:'||lv_error_msg);
				slc_write_log_p(gv_log,'Updating import status for record_id .'||lc_valid_supplier_tab(ln_index).record_id);
				
				
				IF lv_error_flag = 'Y' THEN
				 ROLLBACK TO supplier_savepoint;
				ELSIF lv_error_flag = 'N' THEN
				 COMMIT; 
				END IF;
		--If Store is already existing then we can mark the record as duplicate
		ELSIF ln_store_exists_count > 0 THEN
			lv_error_flag := 'D';
			lv_error_msg  := 'Duplicate Store information received';
		END IF; --End of If condition for Store Exists.
			UPDATE slc_isp_franconnnect_supp_stg
			   SET status = DECODE(lv_error_flag,'Y',gv_error_status,'D',gv_processed_status,'N',gv_processed_status)
				   ,error_msg = DECODE(lv_error_flag,'Y',lv_error_msg,'D',lv_error_msg,'N',NULL)
				   ,franchisee1_party_id = ln_fran1_party_id
				   ,franchisee1_vendor_id = ln_fran1_vendor_id
				   ,franchisee2_party_id = ln_fran2_party_id
				   ,franchisee2_vendor_id = ln_fran2_vendor_id
				   ,franchisee3_party_id = ln_fran3_party_id
				   ,franchisee3_vendor_id = ln_fran3_vendor_id
				   ,franchisee4_party_id = ln_fran4_party_id
				   ,franchisee4_vendor_id = ln_fran4_vendor_id
				   ,incorp_party_id	= ln_corp_party_id
				   ,incorp_vendor_id	= ln_corp_vendor_id
				   ,request_id = gn_request_id
				   ,last_update_date = sysdate
				   ,last_updated_by = gn_user_id
				   ,last_update_login = gn_login_id
			WHERE record_id = lc_valid_supplier_tab(ln_index).record_id;
			COMMIT;
			slc_write_log_p(gv_log,'*********************************Record End**********************************');
			END LOOP;--End of Collection loop

	END LOOP;--End of Cursor loop
	CLOSE c_valid_supplier;
	END slc_import_p;	

/*	*****************************************************************  -- Procedure Name   : slc_main_p
  -- Purpose          : This is main procedure invoked by interface program which will interface 
  --					supplier related information from FranConnect system into Supplier Hub.
  -- Input Parameters : 
  --  p_debug_flag      : Debug Flag will decide if we want to log messages.
  --
  -- Output Parameters :
  --  p_errbuff        : Standard output parameter with Return Message for concurrent program
  --  p_retcode        : Standard output parameter with Return Code for concurrent program
  --********************************************************************************************/
	PROCEDURE slc_main_p  ( p_errbuff           OUT VARCHAR2
                      ,p_retcode           OUT NUMBER
					 --Changes for v1.1 
					 --Now data from FranConnect will be fetched based on 1 flag in FranConnect and not dates and reference id.					  
					 /* ,p_in_reference_id	IN VARCHAR2
					  ,p_in_from_date		IN VARCHAR2
					  ,p_in_to_date			IN VARCHAR2*/
					  ,p_in_debug_flag		IN VARCHAR2
                     )
	IS
	  lv_xmldocument       VARCHAR2 (32000);
	  lc_eventdata         CLOB;
	  lt_parameter_list    apps.wf_parameter_list_t;
	  lv_message           VARCHAR2 (10);
	  lt_parameter         apps.wf_parameter_t;
	  ln_parameter_index   NUMBER;
	  ln_event_key         NUMBER;
	  lv_client_code_tag		VARCHAR2(240);
	  lv_secret_code_tag		VARCHAR2(240);
	  lv_module_tag				VARCHAR2(240);
	  lv_submodule_tag			VARCHAR2(240);
	  lv_lead_status_tag		VARCHAR2(240);
	  lv_processed_by_oracle_tag1			VARCHAR2(240);
	  lv_processed_by_oracle_tag2			VARCHAR2(240);
	  lv_fran_lkpname		VARCHAR2(25) DEFAULT 'SLCISP_FRANCONNECT_LKP';
	  lv_client_code		VARCHAR2(25) DEFAULT 'CLIENT_CODE';
	  lv_secret_code		VARCHAR2(25) DEFAULT 'SECRET_CODE';
	  lv_module				VARCHAR2(25) DEFAULT 'MODULE';
	  lv_submodule			VARCHAR2(25) DEFAULT 'SUBMODULE';
	  lv_lead_status_id		VARCHAR2(25) DEFAULT 'LEAD_STATUS_ID';
	  lv_processed_by_oracle1	VARCHAR2(25) DEFAULT 'PROCESSED_BY_ORACLE1';
	  lv_processed_by_oracle2	VARCHAR2(25) DEFAULT 'PROCESSED_BY_ORACLE2';
	  
	  /*
	  lv_main_from_date          VARCHAR2(15);
	  lv_main_to_date            VARCHAR2(15);*/
	  
	  
	BEGIN
		gv_debug_flag := p_in_debug_flag;
		
		slc_write_log_p(gv_log,p_in_debug_flag);
					 
		 --Changes for v1.1 
		 --Now data from FranConnect will be fetched based on 1 flag in FranConnect and not dates and reference id.			
		/*
		IF p_in_reference_id IS NULL 
		 THEN

		   SELECT  NVL(to_char(to_date(p_in_from_date,'YYYY/MM/DD HH24:MI:SS'),'MM/DD/YYYY'),to_char(SYSDATE,'MM/DD/YYYY'))
		     INTO lv_main_from_date
		     FROM dual;
           
		  
           
		   SELECT NVL(to_char(TO_DATE(p_in_to_date,'YYYY/MM/DD HH24:MI:SS'),'MM/DD/YYYY')
                ,NVL(to_char(TO_DATE(p_in_from_date,'YYYY/MM/DD HH24:MI:SS')+1,'MM/DD/YYYY'),TO_CHAR(SYSDATE+1,'MM/DD/YYYY')))
                INTO lv_main_to_date			 
               FROM dual;
			   
			   slc_write_log_p(gv_log,'Dates lv_main_from_date:'||lv_main_from_date||' lv_main_to_date:'||lv_main_to_date);
		ELSE
		   lv_main_from_date :=to_char(to_date(p_in_from_date,'YYYY/MM/DD HH24:MI:SS'),'MM/DD/YYYY');
		   lv_main_to_date := to_char(TO_DATE(p_in_to_date,'YYYY/MM/DD HH24:MI:SS'),'MM/DD/YYYY');
			slc_write_log_p(gv_log,'Dates lv_main_from_date:'||lv_main_from_date||' lv_main_to_date:'||lv_main_to_date);
		END IF;

		 slc_write_log_p(gv_log,'Converted from date :'||lv_main_from_date);
		slc_write_log_p(gv_log,'Converted to date :'||lv_main_to_date);		
		*/
		
		lv_client_code_tag := '<'||lv_client_code||'>'||SLC_GET_LOOKUP_MEANING_F(lv_fran_lkpname,lv_client_code)||'</'||lv_client_code||'>';
		lv_secret_code_tag := '<'||lv_secret_code||'>'||SLC_GET_LOOKUP_MEANING_F(lv_fran_lkpname,lv_secret_code)||'</'||lv_secret_code||'>';
		lv_module_tag := '<'||lv_module||'>'||SLC_GET_LOOKUP_MEANING_F(lv_fran_lkpname,lv_module)||'</'||lv_module||'>';
		lv_submodule_tag := '<'||lv_submodule||'>'||SLC_GET_LOOKUP_MEANING_F(lv_fran_lkpname,lv_submodule)||'</'||lv_submodule||'>';
		lv_lead_status_tag := '<'||lv_lead_status_id||'>Agreement Creation</'||lv_lead_status_id||'>';
		lv_processed_by_oracle_tag1 := '<'||lv_processed_by_oracle1||'>'||gv_not_processed||'</'||lv_processed_by_oracle1||'>';
		lv_processed_by_oracle_tag2 := '<'||lv_processed_by_oracle2||'>'||gv_extracted||'</'||lv_processed_by_oracle2||'>';
		
		slc_write_log_p(gv_log,lv_client_code_tag);
		slc_write_log_p(gv_log,lv_secret_code_tag);
		slc_write_log_p(gv_log,lv_module_tag);
		slc_write_log_p(gv_log,lv_submodule_tag);
		slc_write_log_p(gv_log,lv_lead_status_tag);
		slc_write_log_p(gv_log,lv_processed_by_oracle_tag1);
		slc_write_log_p(gv_log,lv_processed_by_oracle_tag2);
		lv_xmldocument := '<FranConnect xmlns:xsd="http://www.franconnect.com">'
		||lv_client_code_tag||lv_secret_code_tag||lv_module_tag||lv_submodule_tag||lv_lead_status_tag
		||lv_processed_by_oracle_tag1||lv_processed_by_oracle_tag2||'</FranConnect>';
		slc_write_log_p(gv_log,lv_xmldocument);
		ln_event_key := TO_CHAR (SYSDATE, 'DDMMYYMMHHSS');  
		IF lv_xmldocument IS NOT NULL
                 THEN
                    DBMS_LOB.createtemporary (lc_eventdata, FALSE, DBMS_LOB.CALL);
                    DBMS_LOB.WRITE (lc_eventdata,
                                    LENGTH (lv_xmldocument),
                                    1,
                                    lv_xmldocument
                                   );
                    -- Raise the Event with the message
                    
                    slc_write_log_p(gv_log,'lv_message: Raising event in if');
                    
                    wf_event.RAISE
                           (p_event_name      => 'slc.apps.isp.supplier.franConnectService',
                            p_event_key       => ln_event_key,
                            p_event_data      => lc_eventdata,
                            p_parameters      => lt_parameter_list
                           );
		END IF;
		
	EXCEPTION
	WHEN OTHERS THEN
	slc_write_log_p(gv_log,'Exception in slc_main_p:'||SQLERRM);
	p_errbuff := 'Exception in slc_main_p:'||SQLERRM;
	END slc_main_p;

/*	*****************************************************************
  -- Procedure Name   : slc_process_data
  -- Purpose          : This procedure will be called from SOA. This procedure is responsible for picking latest 
  --					records from FranConnect interface table and call Oracle API's to import data into base table.
  -- Input Parameters : 
  --  p_debug_flag      : Debug Flag will decide if we want to log messages.
  --
  -- Output Parameters :
  --  p_errbuff        : Standard output parameter with Return Message for concurrent program
  --  p_retcode        : Standard output parameter with Return Code for concurrent program
  --********************************************************************************************/
  
 PROCEDURE slc_process_data  ( p_errbuff           OUT VARCHAR2,
                      p_retcode           OUT NUMBER,  
                      p_debug_flag        IN  VARCHAR2
                    )
 IS
 BEGIN
		gv_debug_flag := p_debug_flag;
		slc_write_log_p(gv_log,'p_debug_flag:'||p_debug_flag);
		slc_write_log_p(gv_out,'*************************Output***************************');
		slc_write_log_p(gv_out,'*************************Parameters***************************');
		slc_write_log_p(gv_out,'p_debug_flag: '||p_debug_flag);
		slc_write_log_p(gv_out,'gn_request_id: '||gn_request_id);
		slc_write_log_p(gv_out,'**************************************************************');	
		 -- Call slc_assign_batch_id_p
		slc_assign_batch_id_p;
		slc_validate_p;
	    slc_import_p;
		slc_print_summary_p(NULL);
		p_retcode  := gn_program_status;  
 END  slc_process_data;
END SLC_ISP_FRANCONNNECT_SUPP_PKG;
/
SHOW ERROR;	
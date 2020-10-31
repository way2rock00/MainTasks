
REM ===============================================================================
REM  Program:      SLC_ISP_SUPPTAX_CNV_SPKG.sql
REM  Author:       Akshay Nayak
REM  Date:         16-Feb-2017
REM  Purpose:      This package spec is used in Tax Restructure conversion.
REM  Change Log:   1.0  22-Mar-2017 Akshay Nayak Created
REM  Change Log:   1.1  09-May-2017 Akshay Nayak Added logic to calculate invoice created in last 2 years.
REM  ================================================================================


CREATE OR REPLACE PACKAGE BODY SLC_ISP_SUPPTAX_CNV_PKG AS

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
gv_duplicate_status VARCHAR2(3) := 'D';--Indicates that record is duplicate
gv_load_mode		VARCHAR2(20) := 'LOAD';
gv_validate_mode		VARCHAR2(20) := 'VALIDATE';
gv_revalidate_mode		VARCHAR2(20) := 'REVALIDATE';
gv_process_mode		VARCHAR2(20) := 'PROCESS';
gn_batch_id						NUMBER;
gn_request_id                             NUMBER DEFAULT fnd_global.conc_request_id;
gn_user_id                                NUMBER DEFAULT fnd_global.user_id;
gn_login_id                               NUMBER DEFAULT fnd_global.login_id;	
gn_program_status				NUMBER;

--Variables for Common Error Handling.
gv_batch_key				  VARCHAR2(50) DEFAULT 'FRC-C-100'||'-'||TO_CHAR(SYSDATE,'DDMMYYYY');
gv_business_process_name 		  VARCHAR2(100)  := 'SLC_ISP_SUPPTAX_CNV_PKG';
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
	  FROM SLC_ISP_SUPPTAX_PARTY_CNV_STG 
	 WHERE request_id = gn_request_id
	   AND status = p_in_status;
	lc_cur_err_rec   cur_err_rec%ROWTYPE;
	
	
	CURSOR cur_bank_account_stats
	IS
	SELECT slc_bank.status ,
	  COUNT(1) count_val
	FROM SLC_ISP_SUPPTAX_PARTY_CNV_STG slc_parent ,
	  SLC_ISP_SUPPTAX_CNV_STG slc_bank
	WHERE  slc_parent.request_id = gn_request_id
	AND slc_parent.record_id        = slc_bank.parent_record_id
	AND slc_bank.request_id = slc_parent.request_id
	GROUP BY slc_bank.status;

	CURSOR cur_contact_pt_stats
	IS
	SELECT slc_contact_pt.status ,
	  COUNT(1) count_val
	FROM SLC_ISP_SUPPTAX_PARTY_CNV_STG slc_parent ,
	  SLC_ISP_SUPTAX_CONTPT_CNV_STG slc_contact_pt
	WHERE  slc_parent.request_id = gn_request_id
	AND slc_parent.record_id        = slc_contact_pt.parent_record_id
	AND slc_contact_pt.request_id = slc_parent.request_id
	GROUP BY slc_contact_pt.status;

	CURSOR cur_contact_dir_stats
	IS
	SELECT slc_contact_dir.status ,
	  COUNT(1) count_val
	FROM SLC_ISP_SUPPTAX_PARTY_CNV_STG slc_parent ,
	  SLC_ISP_SUPTAX_CONTDR_CNV_STG slc_contact_dir
	WHERE  slc_parent.request_id = gn_request_id
	AND slc_parent.record_id        = slc_contact_dir.parent_record_id
	AND slc_contact_dir.request_id = slc_parent.request_id
	GROUP BY slc_contact_dir.status;
	
	
	BEGIN
	
	SELECT count(*)
	  INTO ln_total_count
	 FROM SLC_ISP_SUPPTAX_PARTY_CNV_STG
	 WHERE request_id = gn_request_id;

	slc_write_log_p(gv_out,'****************Output******************');
	slc_write_log_p(gv_out,'Total Records:'||ln_total_count);
	IF p_processing_mode IN (gv_validate_mode,gv_revalidate_mode) THEN
		SELECT count(1)
		  INTO ln_total_success_count
		 FROM SLC_ISP_SUPPTAX_PARTY_CNV_STG
		 WHERE request_id = gn_request_id
		   AND status = gv_valid_status;

		SELECT count(1)
		  INTO ln_total_fail_count
		 FROM SLC_ISP_SUPPTAX_PARTY_CNV_STG
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
		slc_write_log_p(gv_out,rpad(lc_cur_err_rec.record_id,25,' ')||'Record Id:'||lc_cur_err_rec.record_id
											||' Error Message:'||lc_cur_err_rec.error_msg);
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
	SELECT count(*)
	  INTO ln_total_success_count
	 FROM SLC_ISP_SUPPTAX_PARTY_CNV_STG
	 WHERE request_id = gn_request_id
	   AND status = gv_processed_status;

	SELECT count(*)
	  INTO ln_total_fail_count
	 FROM SLC_ISP_SUPPTAX_PARTY_CNV_STG
	 WHERE request_id = gn_request_id
	   AND status = gv_error_status;	
	   
		slc_write_log_p(gv_out,'Total records successfully imported:'||ln_total_success_count);
		slc_write_log_p(gv_out,'Total records which failed during import:'||ln_total_fail_count);
		
		slc_write_log_p(gv_out,'********************Bank Account Stats********************');
		
		FOR cur_bank_account_rec IN cur_bank_account_stats
		LOOP
			IF cur_bank_account_rec.status = gv_processed_status THEN
				slc_write_log_p(gv_out,'No of Bank Accounts processed successfully is:'||cur_bank_account_rec.count_val);
			ELSIF cur_bank_account_rec.status = gv_duplicate_status THEN
				slc_write_log_p(gv_out,'No of Duplicate Bank Accounts is:'||cur_bank_account_rec.count_val);
			ELSIF cur_bank_account_rec.status = gv_error_status THEN
				slc_write_log_p(gv_out,'No of Bank Accounts errored is:'||cur_bank_account_rec.count_val);
			END IF;
		END LOOP;
		
		slc_write_log_p(gv_out,'********************Contact Point Stats********************');
		FOR cur_contact_pt_rec IN cur_contact_pt_stats
		LOOP
			IF cur_contact_pt_rec.status = gv_processed_status THEN
				slc_write_log_p(gv_out,'No of Contact Points processed successfully is:'||cur_contact_pt_rec.count_val);
			ELSIF cur_contact_pt_rec.status = gv_duplicate_status THEN
				slc_write_log_p(gv_out,'No of Duplicate Contact Points is:'||cur_contact_pt_rec.count_val);
			ELSIF cur_contact_pt_rec.status = gv_error_status THEN
				slc_write_log_p(gv_out,'No of Contact Points errored is:'||cur_contact_pt_rec.count_val);
			END IF;
		END LOOP;

		slc_write_log_p(gv_out,'********************Contact Directory Stats********************');
		FOR cur_contact_dir_rec IN cur_contact_dir_stats
		LOOP
			IF cur_contact_dir_rec.status = gv_processed_status THEN
				slc_write_log_p(gv_out,'No of Contact Directories processed successfully is:'||cur_contact_dir_rec.count_val);
			ELSIF cur_contact_dir_rec.status = gv_duplicate_status THEN
				slc_write_log_p(gv_out,'No of Duplicate Contact Directories is:'||cur_contact_dir_rec.count_val);
			ELSIF cur_contact_dir_rec.status = gv_error_status THEN
				slc_write_log_p(gv_out,'No of Contact Directories errored is:'||cur_contact_dir_rec.count_val);
			END IF;
		END LOOP;
		
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

		gn_batch_id := SLC_ISP_SUPPTAX_BATCH_ID_S.NEXTVAL;
		UPDATE SLC_ISP_SUPPTAX_PARTY_CNV_STG 
			SET BATCH_ID = gn_batch_id
				,request_id = gn_request_id
				,last_update_date = sysdate
				,last_updated_by = gn_user_id
				,last_update_login = gn_login_id
		 WHERE status IN (p_in_status1,p_in_status2,p_in_status3)
			AND rownum <= p_in_batch_size
			AND redundant_flag = 'N';
		slc_write_log_p(gv_log,'Batch Id :'||gn_batch_id);

	END slc_assign_batch_id_p;
/*
/* ****************************************************************
	NAME:              slc_add_joint_account_owner_p
	PURPOSE:           This procedure will create joint account owner for new Supplier.
	Input Parameters:   p_in_bank_account_id	IN 	NUMBER		
						p_in_account_owner_party_id IN NUMBER
*****************************************************************/	
  PROCEDURE slc_add_joint_account_owner_p(p_in_bank_account_id	IN NUMBER
										 ,p_in_account_owner_party_id IN NUMBER
										 ,p_out_error_flag  	OUT VARCHAR2
										 ,p_out_err_msg		OUT VARCHAR2
										 )
  IS
  P_API_VERSION NUMBER	DEFAULT 1.0;
  P_INIT_MSG_LIST VARCHAR2(200) DEFAULT FND_API.G_TRUE;
  X_JOINT_ACCT_OWNER_ID NUMBER;
  X_RETURN_STATUS VARCHAR2(200);
  X_MSG_COUNT NUMBER	DEFAULT 0;
  X_MSG_DATA VARCHAR2(200);
  X_RESPONSE APPS.IBY_FNDCPT_COMMON_PUB.RESULT_REC_TYPE;
  lv_msg                  VARCHAR2(4000);
  lv_msg_out  NUMBER;  
  
  lv_error_flag			VARCHAR2(1)	DEFAULT 'N';
  lv_error_msg			VARCHAR2(4000)	DEFAULT NULL;
  BEGIN	
  slc_write_log_p(gv_log,'In slc_add_joint_account_owner_p p_in_bank_account_id:'||p_in_bank_account_id
					    ||' p_in_account_owner_party_id:'||p_in_account_owner_party_id);
  
   FND_MSG_PUB.Initialize;
   IBY_EXT_BANKACCT_PUB.ADD_JOINT_ACCOUNT_OWNER(
    P_API_VERSION => P_API_VERSION,
    P_INIT_MSG_LIST => P_INIT_MSG_LIST,
    P_BANK_ACCOUNT_ID => p_in_bank_account_id,
    P_ACCT_OWNER_PARTY_ID => p_in_account_owner_party_id,
    X_JOINT_ACCT_OWNER_ID => X_JOINT_ACCT_OWNER_ID,
    X_RETURN_STATUS => X_RETURN_STATUS,
    X_MSG_COUNT => X_MSG_COUNT,
    X_MSG_DATA => X_MSG_DATA,
    X_RESPONSE => X_RESPONSE
  ); 
	IF X_RETURN_STATUS <> 'S' THEN
	 lv_error_flag := 'Y';
	 IF X_MSG_COUNT>= 1 THEN
	   FOR i IN 1 .. X_MSG_COUNT
		LOOP
		  FND_MSG_PUB.Get (p_msg_index       => i,
								 p_encoded         => 'F',
								 p_data            => lv_msg,
								 p_msg_index_OUT   => lv_msg_out);
		  lv_error_msg := lv_error_msg || ':' || lv_msg;
	   END LOOP;	
	  ELSE
		lv_error_msg := X_MSG_DATA;
	  END IF;
	END IF;
	p_out_error_flag := lv_error_flag;
	p_out_err_msg	:= lv_error_msg;
	slc_write_log_p(gv_log,'In slc_add_joint_account_owner_p lv_error_flag:'||lv_error_flag||' lv_error_msg:'||lv_error_msg);
	
	EXCEPTION
	WHEN OTHERS THEN
	slc_write_log_p(gv_log,'Unexpected error in slc_add_joint_account_owner_p. Error Message:'||SQLERRM);
	p_out_error_flag := 'Y';
	p_out_err_msg	:= 'Unexpected error in slc_add_joint_account_owner_p. Error Message:'||SQLERRM;		
  END slc_add_joint_account_owner_p;

/* ****************************************************************
	NAME:              SLC_SET_PMT_INSTR_USES_P		
	PURPOSE:           This procedure will add new assignment for the bank account for the new party.
	Input Parameters:   p_in_bank_account_id IN NUMBER		
						p_in_new_party_id	IN NUMBER
						p_in_priority IN NUMBER
*****************************************************************/	
   PROCEDURE slc_set_pmt_instr_uses_p(p_in_bank_account_id IN NUMBER
									  ,p_in_new_party_id	IN NUMBER
									  ,p_out_error_flag  	OUT VARCHAR2
									  ,p_out_err_msg		OUT VARCHAR2
									 )
  IS
  P_API_VERSION NUMBER  DEFAULT 1.0;
  P_INIT_MSG_LIST VARCHAR2(200)  DEFAULT FND_API.G_TRUE;
  P_COMMIT VARCHAR2(200) DEFAULT FND_API.G_FALSE;
  X_RETURN_STATUS VARCHAR2(200);
  X_MSG_COUNT NUMBER DEFAULT 0;
  X_MSG_DATA VARCHAR2(200);
  P_PAYEE APPS.IBY_DISBURSEMENT_SETUP_PUB.PAYEECONTEXT_REC_TYPE;
  P_ASSIGNMENT_ATTRIBS APPS.IBY_FNDCPT_SETUP_PUB.PMTINSTRASSIGNMENT_REC_TYPE;
  X_ASSIGN_ID NUMBER;
  X_RESPONSE APPS.IBY_FNDCPT_COMMON_PUB.RESULT_REC_TYPE;
  lv_error_flag			VARCHAR2(1)	DEFAULT 'N';
  lv_error_msg			VARCHAR2(4000)	DEFAULT NULL;  
  lv_msg                  VARCHAR2(4000);
  lv_msg_out  NUMBER;    
  BEGIN
  slc_write_log_p(gv_log,'In slc_set_pmt_instr_uses_p p_in_new_party_id:'||p_in_new_party_id
					    ||' p_in_bank_account_id:'||p_in_bank_account_id);
						
   p_payee.Party_Id := p_in_new_party_id;
   p_payee.Payment_Function := 'PAYABLES_DISB';
   p_assignment_attribs.Instrument.Instrument_Type := 'BANKACCOUNT';
   p_assignment_attribs.Instrument.Instrument_Id := p_in_bank_account_id;
   p_assignment_attribs.start_date                := sysdate;
 
 FND_MSG_PUB.Initialize; 
 IBY_DISBURSEMENT_SETUP_PUB.SET_PAYEE_INSTR_ASSIGNMENT(
    P_API_VERSION => P_API_VERSION,
    P_INIT_MSG_LIST => P_INIT_MSG_LIST,
    P_COMMIT => P_COMMIT,
    X_RETURN_STATUS => X_RETURN_STATUS,
    X_MSG_COUNT => X_MSG_COUNT,
    X_MSG_DATA => X_MSG_DATA,
    P_PAYEE => P_PAYEE,
    P_ASSIGNMENT_ATTRIBS => P_ASSIGNMENT_ATTRIBS,
    X_ASSIGN_ID => X_ASSIGN_ID,
    X_RESPONSE => X_RESPONSE
  );  
	slc_write_log_p(gv_log,'In slc_set_pmt_instr_uses_p X_RETURN_STATUS:'||X_RETURN_STATUS||
						   ' X_RESPONSE.Result_Code:'||X_RESPONSE.Result_Code||
						   ' X_RESPONSE.Result_Category:'||X_RESPONSE.Result_Category||
						   ' X_RESPONSE.Result_Message:'||X_RESPONSE.Result_Message
						   );
	
	IF X_RETURN_STATUS <> 'S'THEN
	lv_error_flag := 'Y';
		IF X_MSG_COUNT >= 1 THEN
			FOR i IN 1 .. X_MSG_COUNT
			LOOP
			   FND_MSG_PUB.Get (p_msg_index       => i,
								p_encoded         => 'F',
								p_data            => lv_msg,
								p_msg_index_OUT   => lv_msg_out);
			   lv_error_msg := lv_error_msg || ':' || lv_msg;
			END LOOP;
		ELSE
			lv_error_msg := X_MSG_DATA;
		END IF;
	END IF; 
	IF X_RESPONSE.Result_Code <> IBY_DISBURSEMENT_SETUP_PUB.G_RC_SUCCESS THEN
	lv_error_flag := 'Y';
	lv_error_msg := lv_error_msg ||'~'||X_RESPONSE.Result_Code;
	END IF;

	p_out_error_flag := lv_error_flag;
	p_out_err_msg	:= lv_error_msg;
	slc_write_log_p(gv_log,'In slc_set_pmt_instr_uses_p lv_error_flag:'||lv_error_flag||' lv_error_msg:'||lv_error_msg);
	
	EXCEPTION
	WHEN OTHERS THEN
	slc_write_log_p(gv_log,'Unexpected error in slc_set_pmt_instr_uses_p. Error Message:'||SQLERRM);
	p_out_error_flag := 'Y';
	p_out_err_msg	:= 'Unexpected error in slc_set_pmt_instr_uses_p. Error Message:'||SQLERRM;	
	
  END slc_set_pmt_instr_uses_p;

/* ****************************************************************
	NAME:              slc_cng_prim_acct_owner_p		
	PURPOSE:           This procedure will change primary account owner to new account
	Input Parameters:   p_in_bank_account_id	IN 	NUMBER		
						p_in_account_owner_party_id IN NUMBER
*****************************************************************/	  
    PROCEDURE slc_cng_prim_acct_owner_p(p_in_bank_account_id IN NUMBER
									  ,p_in_new_party_id	IN NUMBER
									  ,p_out_error_flag  	OUT VARCHAR2
									  ,p_out_err_msg		OUT VARCHAR2
									 )
	IS
  P_API_VERSION NUMBER  DEFAULT 1.0;
  P_INIT_MSG_LIST VARCHAR2(200)  DEFAULT FND_API.G_TRUE;
  X_RETURN_STATUS VARCHAR2(200);
  X_MSG_COUNT NUMBER DEFAULT 0;
  X_MSG_DATA VARCHAR2(200);
  X_RESPONSE APPS.IBY_FNDCPT_COMMON_PUB.RESULT_REC_TYPE;
  lv_error_flag			VARCHAR2(1)	DEFAULT 'N';
  lv_error_msg			VARCHAR2(4000)	DEFAULT NULL;  
  lv_msg                  VARCHAR2(4000);
  lv_msg_out  NUMBER;    
	BEGIN
  slc_write_log_p(gv_log,'In slc_cng_prim_acct_owner_p p_in_new_party_id:'||p_in_new_party_id
					    ||' p_in_bank_account_id:'||p_in_bank_account_id);
						
	FND_MSG_PUB.Initialize;
	  IBY_EXT_BANKACCT_PUB.CHANGE_PRIMARY_ACCT_OWNER(
		P_API_VERSION => P_API_VERSION,
		P_INIT_MSG_LIST => P_INIT_MSG_LIST,
		P_BANK_ACCT_ID => p_in_bank_account_id,
		P_ACCT_OWNER_PARTY_ID => p_in_new_party_id,
		X_RETURN_STATUS => X_RETURN_STATUS,
		X_MSG_COUNT => X_MSG_COUNT,
		X_MSG_DATA => X_MSG_DATA,
		X_RESPONSE => X_RESPONSE
	  );

	IF X_RETURN_STATUS <> 'S' THEN
	lv_error_flag := 'Y';
	lv_error_msg  := x_msg_data;
	END IF; 

	p_out_error_flag := lv_error_flag;
	p_out_err_msg	:= lv_error_msg;
	slc_write_log_p(gv_log,'In slc_cng_prim_acct_owner_p lv_error_flag:'||lv_error_flag||' lv_error_msg:'||lv_error_msg);	
	
	EXCEPTION
	WHEN OTHERS THEN
	slc_write_log_p(gv_log,'Unexpected error in slc_cng_prim_acct_owner_p. Error Message:'||SQLERRM);
	p_out_error_flag := 'Y';
	p_out_err_msg	:= 'Unexpected error in slc_cng_prim_acct_owner_p. Error Message:'||SQLERRM;		
	END slc_cng_prim_acct_owner_p;
	
/* ****************************************************************
	NAME:              slc_update_vendor_p		
	PURPOSE:           This procedure will update vendor information.
	Input Parameters:   p_in_vendor_id			IN NUMBER
						p_in_state_reportable_flag	IN VARCHAR2		DEFAULT NULL
						p_in_federal_reportable_flag	IN VARCHAR2		DEFAULT NULL
						p_in_type_1099			IN VARCHAR2		DEFAULT NULL
						p_in_tax_reporting_name		IN VARCHAR2		DEFAULT NULL
						p_in_end_date_active		IN DATE			DEFAULT NULL
*****************************************************************/	
 PROCEDURE slc_update_vendor_p(p_in_vendor_id				IN NUMBER
							  ,p_in_state_reportable_flag	IN VARCHAR2		DEFAULT NULL
							  ,p_in_federal_reportable_flag	IN VARCHAR2		DEFAULT NULL
							  ,p_in_type_1099					IN VARCHAR2		DEFAULT NULL
							  ,p_in_tax_reporting_name		IN VARCHAR2		DEFAULT NULL
							  ,p_in_end_date_active		IN DATE			DEFAULT NULL
							  ,p_out_error_flag  	OUT VARCHAR2
							  ,p_out_err_msg		OUT VARCHAR2							  
							  )
 IS
 lv_error_flag		VARCHAR2(1) DEFAULT 'N';
 lv_error_msg		VARCHAR2(4000) DEFAULT NULL;
 lv_msg                  VARCHAR2(4000);
 lv_msg_out  NUMBER;	
 l_vendor_rec                  ap_vendor_pub_pkg.r_vendor_rec_type;
 l_msg_count			NUMBER DEFAULT 0;
 l_msg_data			VARCHAR2(4000);
 l_return_status		VARCHAR2(10);	

 BEGIN
  slc_write_log_p(gv_log,'In slc_update_vendor_p p_in_new_vendor_id:'||p_in_vendor_id ); 
  slc_write_log_p(gv_log,'In slc_update_vendor_p p_in_state_reportable_flag:'||p_in_state_reportable_flag ); 
  slc_write_log_p(gv_log,'In slc_update_vendor_p p_in_federal_reportable_flag:'||p_in_federal_reportable_flag ); 
  slc_write_log_p(gv_log,'In slc_update_vendor_p p_in_type_1099:'||p_in_type_1099 ); 
  slc_write_log_p(gv_log,'In slc_update_vendor_p p_in_tax_reporting_name:'||p_in_tax_reporting_name ); 
  slc_write_log_p(gv_log,'In slc_update_vendor_p p_in_end_date_active:'||p_in_end_date_active ); 
  
    l_vendor_rec.vendor_id := p_in_vendor_id;
	l_vendor_rec.state_reportable_flag := p_in_state_reportable_flag;
	l_vendor_rec.federal_reportable_flag := p_in_federal_reportable_flag;
	l_vendor_rec.type_1099 := p_in_type_1099;
	l_vendor_rec.tax_reporting_name := p_in_tax_reporting_name;
	l_vendor_rec.end_date_active  := p_in_end_date_active;

	FND_MSG_PUB.Initialize;
	pos_vendor_pub_pkg.Update_Vendor
	      (
			P_VENDOR_REC => l_vendor_rec,
			X_RETURN_STATUS => l_return_status,
			X_MSG_COUNT => l_msg_count,
			X_MSG_DATA => l_msg_data
      );
     
	IF l_return_status <> 'S' THEN
	lv_error_flag := 'Y';
	IF l_msg_count >= 1 THEN
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
	p_out_err_msg	:= lv_error_msg;
	slc_write_log_p(gv_log,'In slc_update_vendor_p lv_error_flag:'||lv_error_flag||' lv_error_msg:'||lv_error_msg);

 
 EXCEPTION
 WHEN OTHERS THEN
 slc_write_log_p(gv_log,'Unexpected error in slc_update_vendor_p. Error Message:'||SQLERRM);
 p_out_error_flag := 'Y';
 p_out_err_msg	:= 'Unexpected error in slc_update_vendor_p. Error Message:'||SQLERRM; 
 END slc_update_vendor_p;

/* ****************************************************************
	NAME:              slc_update_external_payee_p		
	PURPOSE:           This procedure will update external payee information
	Input Parameters:   
						p_in_party_id 				IN 	NUMBER
						p_in_ext_payee_id			IN NUMBER
						p_in_remit_delivy_method	IN VARCHAR
						p_in_remit_advice_email		IN VARCHAR
						p_in_remit_advice_fax		IN VARCHAR					
*****************************************************************/
 PROCEDURE slc_update_external_payee_p(p_in_party_id 	IN 	NUMBER
									  ,p_in_remit_delivy_method				IN VARCHAR
									  ,p_in_remit_advice_email				IN VARCHAR
									  ,p_in_remit_advice_fax				IN VARCHAR
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
  
   CURSOR cur_ext_payee_info(p_in_party_id 	IN 	NUMBER)
   IS
	SELECT ext_payee_id
	FROM iby_external_payees_all
	WHERE payee_party_id             = p_in_party_id
	AND (org_type                    IS NULL
	AND org_id                      IS NULL
	AND supplier_site_id            IS NULL
	AND party_site_id               IS NULL);
	ln_ext_payee_id					iby_external_payees_all.ext_payee_id%TYPE;
	
 BEGIN
   OPEN cur_ext_payee_info(p_in_party_id);
   FETCH cur_ext_payee_info INTO ln_ext_payee_id;
   CLOSE cur_ext_payee_info;
   
   slc_write_log_p(gv_log,'In slc_update_external_payee_p p_in_party_id:'||p_in_party_id||
						  ' ln_ext_payee_id:'||ln_ext_payee_id);
   
   P_EXT_PAYEE_TAB(1).Default_Pmt_method := 'CHECK';
   P_EXT_PAYEE_TAB(1).Payee_Party_Id := p_in_party_id;
   P_EXT_PAYEE_TAB(1).Payment_Function   := 'PAYABLES_DISB';
   P_EXT_PAYEE_TAB(1).Exclusive_Pay_Flag   := 'N';
   P_EXT_PAYEE_TAB(1).Remit_advice_delivery_method := p_in_remit_delivy_method;
   P_EXT_PAYEE_TAB(1).Remit_advice_email  := p_in_remit_advice_email;
   P_EXT_PAYEE_TAB(1).remit_advice_fax    := p_in_remit_advice_fax;
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
	slc_write_log_p(gv_log,'In slc_update_external_payee_p lv_error_flag:'||lv_error_flag||' lv_error_msg:'||lv_error_msg);
	
 EXCEPTION
 WHEN OTHERS THEN
 slc_write_log_p(gv_log,'Unexpected error in slc_update_external_payee_p. Error Message:'||SQLERRM);
 p_out_error_flag := 'Y';
 p_out_err_msg	:= 'Unexpected error in slc_update_external_payee_p. Error Message:'||SQLERRM;  
 END slc_update_external_payee_p;

/* ****************************************************************
	NAME:              slc_make_bank_acct_eft_p		
	PURPOSE:           This procedure will change the bank account payment method to EFT.
	Input Parameters:   
						p_in_party_id 				IN 	NUMBER
*****************************************************************/ 
 PROCEDURE slc_make_bank_acct_eft_p(p_in_party_id 	IN 	NUMBER
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
  
   CURSOR cur_ext_payee_info(p_in_party_id 	IN 	NUMBER)
   IS
	SELECT ext_payee_id
	FROM iby_external_payees_all
	WHERE payee_party_id             = p_in_party_id
	AND (org_type                    IS NULL
	AND org_id                      IS NULL
	AND supplier_site_id            IS NULL
	AND party_site_id               IS NULL);
	ln_ext_payee_id					iby_external_payees_all.ext_payee_id%TYPE;
	
 BEGIN
   OPEN cur_ext_payee_info(p_in_party_id);
   FETCH cur_ext_payee_info INTO ln_ext_payee_id;
   CLOSE cur_ext_payee_info;
   
   slc_write_log_p(gv_log,'In slc_make_bank_acct_eft_p p_in_party_id:'||p_in_party_id||
						  ' ln_ext_payee_id:'||ln_ext_payee_id);
   
   P_EXT_PAYEE_TAB(1).Default_Pmt_method := 'EFT';
   P_EXT_PAYEE_TAB(1).Payee_Party_Id := p_in_party_id;
   P_EXT_PAYEE_TAB(1).Payment_Function   := 'PAYABLES_DISB';
   P_EXT_PAYEE_TAB(1).Exclusive_Pay_Flag   := 'N';
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

 
/* ****************************************************************
	NAME:              slc_create_contact_point_p
	PURPOSE:           This procedure will create contact point information for 
						party
	Input Parameters:   p_in_party_id		IN 	NUMBER
						p_in_contact_point_type		IN VARCHAR
						p_in_phone_area_code		IN VARCHAR
						p_in_phone_number		IN VARCHAR
						p_in_email_id			IN VARCHAR	
*****************************************************************/
  PROCEDURE slc_create_contact_point_p(p_in_party_id	IN 	NUMBER
									  ,p_in_contact_point_type	IN VARCHAR
									  ,p_in_phone_area_code		IN VARCHAR
									  ,p_in_phone_number		IN VARCHAR
									  ,p_in_email_id			IN VARCHAR
									  ,p_out_error_flag				   OUT VARCHAR2
									  ,p_out_err_msg							OUT VARCHAR2									  
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
   slc_write_log_p(gv_log,'In slc_create_contact_point_p p_in_party_id:'||p_in_party_id||' p_in_phone_area_code:'||p_in_phone_area_code||
						  ' p_in_phone_number:'||p_in_phone_number);
   p_contact_point_rec.contact_point_type     := p_in_contact_point_type;
   p_contact_point_rec.owner_table_name       := 'HZ_PARTIES';
   p_contact_point_rec.owner_table_id         := p_in_party_id;
   p_contact_point_rec.primary_flag           := 'Y';
   p_contact_point_rec.contact_point_purpose  := 'BUSINESS';
   p_contact_point_rec.created_by_module	  := 'HZ_CPUI';
   
   IF p_in_contact_point_type = 'PHONE' THEN
	   p_phone_rec.phone_area_code                := p_in_phone_area_code;
	   p_phone_rec.phone_number                   := p_in_phone_number;
	   p_phone_rec.phone_line_type                := 'GEN';
	   
   END IF;
   IF p_in_contact_point_type = 'EMAIL' THEN
	   p_email_rec.email_address                  := p_in_email_id;    
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
	slc_write_log_p(gv_log,'In slc_create_contact_point_p ln_party_id:'||p_in_party_id||' lv_error_flag:'
	||lv_error_flag||' lv_error_msg:'||lv_error_msg);
	
 EXCEPTION
 WHEN OTHERS THEN
	slc_write_log_p(gv_log,'Unexpected error in slc_create_contact_point_p. Error Message:'||SQLERRM);
	p_out_error_flag := 'Y';
	p_out_err_msg	:= 'Unexpected error in slc_create_contact_point_p. Error Message:'||SQLERRM;	
  END slc_create_contact_point_p;

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
											,p_in_person_first_name 		IN	VARCHAR2
											,p_in_person_middle_name 	IN	VARCHAR2
											,p_in_person_last_name 		IN	VARCHAR2
											,p_in_phone 			IN	VARCHAR2
											,p_in_area_code 			IN	VARCHAR2
											,p_in_email_address 		IN	VARCHAR2
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
	slc_write_log_p(gv_log,'In slc_create_contact_directory_p p_in_vendor_id:'||p_in_vendor_id);
	
    l_vendor_contact_rec.vendor_id := p_in_vendor_id;
    l_vendor_contact_rec.person_first_name := p_in_person_first_name;
    l_vendor_contact_rec.person_middle_name := p_in_person_middle_name;
    l_vendor_contact_rec.person_last_name := p_in_person_last_name;
    l_vendor_contact_rec.phone := p_in_phone;
    l_vendor_contact_rec.AREA_CODE := p_in_area_code;  
    l_vendor_contact_rec.email_address := p_in_email_address;
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
 EXCEPTION
 WHEN OTHERS THEN
	slc_write_log_p(gv_log,'Unexpected error in slc_create_contact_directory_p. Error Message:'||SQLERRM);
	p_out_error_flag := 'Y';
	p_out_err_msg	:= 'Unexpected error in slc_create_contact_directory_p. Error Message:'||SQLERRM;	  
  END slc_create_contact_directory_p;
  
/* ****************************************************************
	NAME:              slc_load_p
	PURPOSE:           This procedure will insert bank account data into below staging table 
						1. SLC_ISP_SUPPTAX_PARTY_CNV_STG
						2. SLC_ISP_SUPPTAX_CNV_STG
						3. SLC_ISP_SUPTAX_CONTPT_CNV_STG
						4. SLC_ISP_SUPTAX_CONTDR_CNV_STG
					   
	Input Parameters:   None
*****************************************************************/
 PROCEDURE slc_load_p
 IS
 lv_error_flag		VARCHAR2(1) DEFAULT 'N';
 lv_error_msg		VARCHAR2(4000) DEFAULT NULL;
 ld_current_date	DATE 	DEFAULT SYSDATE;
 ld_past_date		DATE 	DEFAULT add_months(sysdate,-12*2);--Need to calculate date 2 years less than current date.
 BEGIN
	/* Unlike other conversion objects where we receive data in file formats, in this tax restructuring since data is 
	 * present in Oracle Supplier Hub , we are using insert statements to put data into staging table.
	 */
	 
	/*
	 * Since this is full load where we are extracting all the data again from Oracle Supplier Hub. 
	 * Earlier records in the staging table is marked as redundant. These records irrespective of its status will not be considering 
	 * during validation or import mode. Update redundant flag as Y for all the records in the staging table.
	 * New records loaded during this run will have redundant flag as N and will be considered for validation or import.
	 */
	BEGIN
	UPDATE SLC_ISP_SUPPTAX_PARTY_CNV_STG SET REDUNDANT_FLAG = 'Y';
	UPDATE SLC_ISP_SUPPTAX_CNV_STG SET REDUNDANT_FLAG = 'Y';
	UPDATE SLC_ISP_SUPTAX_CONTPT_CNV_STG SET REDUNDANT_FLAG = 'Y';
	UPDATE SLC_ISP_SUPTAX_CONTDR_CNV_STG SET REDUNDANT_FLAG = 'Y';
	
	EXCEPTION
	WHEN OTHERS THEN
	 slc_write_log_p(gv_log, 'Exception while updating redundant flag. Error Message:'||SQLERRM);
	 gn_program_status := 2;
	END;	
	BEGIN
		INSERT INTO SLC_ISP_SUPPTAX_PARTY_CNV_STG
		  (
			record_id,status,redundant_flag,vendor_id,party_id 
			,vendor_name,vendor_name_alt,vendor_number,tax_payerid 
			,state_reportable_flag,federal_reportable_flag,type_1099,tax_reporting_name,request_id
			,creation_date,created_by,last_update_date,last_updated_by,last_update_login
			,invoice_count --Changes for v1.1 Added column for invoice_count
		  )
		  (SELECT SLC_ISP_SUPPTAX_RECORD_ID_S.nextval ,'N' , 'N',sup.vendor_id ,sup.party_id 
			  ,sup.vendor_name ,sup.vendor_name_alt ,sup.segment1 ,
			  DECODE(sup.organization_type_lookup_code, 'INDIVIDUAL',sup.individual_1099, 'FOREIGN INDIVIDUAL',sup.individual_1099, hp.jgzz_fiscal_code) tax_payer_id ,
			  sup.state_reportable_flag ,sup.FEDERAL_REPORTABLE_FLAG ,sup.type_1099 ,sup.TAX_REPORTING_NAME ,gn_request_id
			  ,ld_current_date,gn_user_id,ld_current_date,gn_user_id,gn_login_id
			  ,(select COUNT(1) FROM ap_invoices_all aia where aia.vendor_id = sup.vendor_id and 
					TRUNC(aia.creation_date)> TRUNC(ld_past_date) ) --Changes for v1.1 Added column for invoice_count
			FROM ap_suppliers sup ,
			  hz_parties hp
			WHERE sup.vendor_type_lookup_code = 'FRANCHISEE' -- We should only pick Supplier of Type Franchisee
			AND sup.party_id                  = hp.party_id
			AND NOT EXISTS
			  (SELECT 1
			  FROM POS_SUPP_PROF_EXT_B pos
			  WHERE sup.party_id  = pos.party_id
			  AND pos.c_ext_attr4 = 'FAS'
			  )
		  );
	slc_write_log_p(gv_log, 'No of records loaded in parent staging table:'||SQL%ROWCOUNT);
	slc_write_log_p(gv_out, 'No of records loaded in parent staging table:'||SQL%ROWCOUNT);	 
	EXCEPTION
	WHEN OTHERS THEN
	 slc_write_log_p(gv_log, 'Exception while inserting record in SLC_ISP_SUPPTAX_PARTY_CNV_STG staging table. Error Message:'||SQLERRM);
	 gn_program_status := 2;
	END;
	
	
	IF lv_error_flag = 'N' THEN
	BEGIN
	    --Insert into bank account staging table only of there is no error while data into party table.
		INSERT
		INTO slc_isp_supptax_cnv_stg
		  (
			record_id,parent_record_id,status , redundant_flag,
			ext_bank_account_id,bank_party_id,bank_account_number,bank_account_name ,branch_party_id,branch_number,
			bank_name,bank_branch_name,bank_branch_type ,pay_group,account_start_date,account_end_date,currency_code,
			country_name,country_code ,foreign_payment_use_flag,request_id
			,creation_date,created_by,last_update_date,last_updated_by,last_update_login
		  )
		  (SELECT SLC_ISP_SUPPTAX_RECORD_ID_S.nextval,party_stg.record_id,'N', 'N',
			  eb.ext_bank_account_id ,eb.bank_id ,eb.bank_account_num ,eb.bank_account_name ,eb.branch_id ,brpr.bank_or_branch_number ,
			  bp.party_name,br.party_name ,BranchCA.class_code bank_branch_type ,eb.attribute1 ,eb.start_date ,eb.end_date,eb.currency_code ,
			  countries.territory_short_name country_name ,countries.territory_code country_code ,eb.foreign_payment_use_flag,gn_request_id
			  ,ld_current_date,gn_user_id,ld_current_date,gn_user_id,gn_login_id
			FROM slc_isp_supptax_party_cnv_stg party_stg ,
			  iby_account_owners ow ,
			  iby_ext_bank_accounts eb ,
			  hz_parties bp ,
			  hz_parties br ,
			  hz_organization_profiles bapr ,
			  hz_organization_profiles brpr ,
			  hz_code_assignments branchca ,
			  fnd_territories_vl countries
			WHERE party_stg.status                = 'N'
			AND party_stg.request_id              = gn_request_id
			AND ow.account_owner_party_id         = party_stg.party_id
			AND eb.ext_bank_account_id            = ow.ext_bank_account_id
			AND eb.bank_id                        = bp.party_id(+)
			AND eb.bank_id                        = bapr.party_id(+)
			AND eb.branch_id                      = br.party_id(+)
			AND eb.branch_id                      = brpr.party_id(+)
			AND (BranchCA.CLASS_CATEGORY(+)       = 'BANK_BRANCH_TYPE' )
			AND BranchCA.PRIMARY_FLAG(+)          = 'Y'
			AND BranchCA.STATUS(+)                = 'A'
			AND (BranchCA.OWNER_TABLE_NAME(+)     = 'HZ_PARTIES')
			AND (BranchCA.OWNER_TABLE_ID(+)       = eb.branch_ID)
			AND eb.country_code                   = countries.territory_code
			AND SYSDATE BETWEEN NVL(TRUNC(Bapr.effective_start_date), sysdate-1) AND NVL(TRUNC(Bapr.effective_end_date), SYSDATE+1)
			AND SYSDATE BETWEEN NVL(TRUNC(Brpr.effective_start_date) ,sysdate-1) AND NVL(TRUNC(Brpr.effective_end_date), SYSDATE+1)
			AND EXISTS (SELECT 1
						  FROM iby_external_payees_all ext,
							iby_pmt_instr_uses_all ibi
						  WHERE ibi.ext_pmt_party_id = ext.ext_payee_id
						  AND ext.payee_party_id     = ow.account_owner_party_id
						  AND ibi.instrument_id      = ow.ext_bank_account_id
						  AND sysdate BETWEEN NVL(ibi.start_date,sysdate) AND NVL(ibi.end_date,sysdate)
						)			
		  );	
	slc_write_log_p(gv_log, 'No of records loaded in bank account staging table:'||SQL%ROWCOUNT);
	slc_write_log_p(gv_out, 'No of records loaded in bank account staging table:'||SQL%ROWCOUNT);	 
	EXCEPTION
	WHEN OTHERS THEN
	 slc_write_log_p(gv_log, 'Exception while inserting record in SLC_ISP_SUPPTAX_CNV_STG staging table. Error Message:'||SQLERRM);
	 gn_program_status := 2;
	END;

	--Insert into contact point information table.
	BEGIN
			-- Since we are having outer join with contact point tables adding null checks on phone_area_code,phone_number
			-- and email_address to avoid data having all null values.
			INSERT
			INTO SLC_ISP_SUPTAX_CONTPT_CNV_STG
			  (
				record_id,parent_record_id,status,redundant_flag,party_site_id,party_site_name ,
				phone_contact_point_id,phone_area_code,phone_number,email_contact_point_id,email,request_id
				,creation_date,created_by,last_update_date,last_updated_by,last_update_login
			  )
			  (SELECT SLC_ISP_SUPPTAX_RECORD_ID_S.nextval record_id,parent_record_id,status,redundant_flag,party_site_id,party_site_name,
				  phone_contact_point_id,phone_area_code,phone_number,email_contact_point_id,email_address,gn_request_id
				  ,creation_date,created_by,last_update_date,last_updated_by,last_update_login
				FROM
				  (SELECT party_stg.record_id parent_record_id,'N' status,'N' redundant_flag ,hps.party_site_id ,hps.party_site_name ,
					hcp1.contact_point_id phone_contact_point_id ,hcp1.phone_area_code ,hcp1.phone_number ,
					hcp2.contact_point_id email_contact_point_id ,hcp2.email_address ,gn_request_id
					,ld_current_date creation_date,gn_user_id created_by
					,ld_current_date last_update_date,gn_user_id last_updated_by,gn_login_id last_update_login
				  FROM slc_isp_supptax_party_cnv_stg party_stg ,
					hz_party_sites hps,
					hz_contact_points hcp1,
					hz_contact_points hcp2
				  WHERE party_stg.status = 'N'
				  AND party_stg.request_id              = gn_request_id
				  AND hps.party_id                       = party_stg.party_id
				  AND NVL(hps.end_date_active, sysdate) >= sysdate
				  AND hcp1.owner_table_id(+)             = hps.party_site_id
				  AND hcp1.CONTACT_POINT_TYPE(+)         = 'PHONE'
				  AND hcp1.phone_line_type (+)           = 'GEN'
				  AND hcp1.status(+)                     = 'A'
				  AND hcp1.owner_table_name(+)           = 'HZ_PARTY_SITES'
				  AND hcp1.primary_flag(+)               = 'Y'
				  AND hcp2.owner_table_id(+)             = hps.party_site_id
				  AND hcp2.CONTACT_POINT_TYPE(+)         = 'EMAIL'
				  AND hcp2.status(+)                     = 'A'
				  AND hcp2.owner_table_name(+)           = 'HZ_PARTY_SITES'
				  AND hcp2.primary_flag(+)               = 'Y'
				  )
				WHERE (phone_area_code IS NOT NULL
				OR phone_number        IS NOT NULL
				OR email_address       IS NOT NULL)
			  );
	slc_write_log_p(gv_log, 'No of records loaded in Contact Point staging table:'||SQL%ROWCOUNT);
	slc_write_log_p(gv_out, 'No of records loaded in Contact Point staging table:'||SQL%ROWCOUNT);	 
	EXCEPTION
	WHEN OTHERS THEN
	 slc_write_log_p(gv_log, 'Exception while inserting record in SLC_ISP_SUPTAX_CONTPT_CNV_STG staging table. Error Message:'||SQLERRM);
	 gn_program_status := 2;
	END;

	--Insert into contact directory information table.
	BEGIN
		INSERT
		INTO SLC_ISP_SUPTAX_CONTDR_CNV_STG
		  (
			record_id,parent_record_id,status,redundant_flag ,
			contact_party_id,person_first_name,person_middle_name,person_last_name ,
			phone_contact_point_id ,phone_area_code ,phone_number ,email_contact_point_id ,email,request_id ,
			creation_date ,created_by,last_update_date,last_updated_by ,last_update_login
		  )
		  (SELECT SLC_ISP_SUPPTAX_RECORD_ID_S.nextval,party_stg.record_id,'N','N' ,
			  hpc.PARTY_ID contact_party_id,hpc.person_first_name ,hpc.PERSON_middle_NAME ,hpc.person_last_name ,
			  hcpp.contact_point_id phone_contact_point_id,hcpp.PHONE_AREA_CODE ,hcpp.PHONE_NUMBER ,
			  hcpe.contact_point_id email_contact_point_id,hcpe.EMAIL_ADDRESS,gn_request_id
			  ,ld_current_date,gn_user_id,ld_current_date,gn_user_id,gn_login_id
			FROM slc_isp_supptax_party_cnv_stg party_stg ,
			  Hz_parties hpc ,
			  HZ_CONTACT_POINTS hcpp ,
			  HZ_CONTACT_POINTS hcpe ,
			  HZ_RELATIONSHIPS hr ,
			  hz_org_contacts hoc ,
			  hz_parties hpr
			WHERE party_stg.status = 'N'
			AND party_stg.request_id              = gn_request_id
			AND hcpp.OWNER_TABLE_NAME(+)      = 'HZ_PARTIES'
			AND hcpp.OWNER_TABLE_ID(+)        = hr.PARTY_ID
			AND hcpp.PHONE_LINE_TYPE(+)       = 'GEN'
			AND hcpp.CONTACT_POINT_TYPE(+)    = 'PHONE'
			AND hcpe.OWNER_TABLE_NAME(+)      = 'HZ_PARTIES'
			AND hcpe.OWNER_TABLE_ID(+)        = hr.PARTY_ID
			AND hcpe.CONTACT_POINT_TYPE(+)    = 'EMAIL'
			AND hr.object_id                  = hpc.party_id
			AND hr.subject_Type               = 'ORGANIZATION'
			AND hr.subject_table_name         = 'HZ_PARTIES'
			AND hr.object_table_name          = 'HZ_PARTIES'
			AND hr.object_Type                = 'PERSON'
			AND hr.relationship_code          = 'CONTACT'
			AND hr.directional_flag           = 'B'
			AND hr.RELATIONSHIP_TYPE          = 'CONTACT'
			AND hr.party_id                   =hpr.party_id
			AND hcpe.status (+)               = 'A'
			AND hcpe.primary_flag(+)          ='Y'
			AND hcpp.status (+)               = 'A'
			AND hcpp.primary_flag(+)          ='Y'
			AND hoc.party_relationship_id (+) = hr.relationship_id
			AND hr.subject_id                 = party_stg.party_id
		  );	
	slc_write_log_p(gv_log, 'No of records loaded in Contact Directory staging table:'||SQL%ROWCOUNT);
	slc_write_log_p(gv_out, 'No of records loaded in Contact Directory staging table:'||SQL%ROWCOUNT);	 
	EXCEPTION
	WHEN OTHERS THEN
	 slc_write_log_p(gv_log, 'Exception while inserting record in SLC_ISP_SUPTAX_CONTDR_CNV_STG staging table. Error Message:'||SQLERRM);
	 gn_program_status := 2;
	END;	
	END IF;--End of if block of error check.
	COMMIT;


	
 EXCEPTION	
 WHEN OTHERS THEN
	 slc_write_log_p(gv_log, 'Unexpected exception in slc_load_p. Error Message:'||SQLERRM);
	 lv_error_flag := 'E';
	 lv_error_msg := 'Unexpected exception in slc_load_p. Error Message:'||SQLERRM;
	 -- If there is any error while inserting data into staging table then set program status as Error.
	 gn_program_status := 2;	 
 END slc_load_p;

/* ****************************************************************
	NAME:              slc_validate_p
	PURPOSE:           This procedure will be used to validate records fetched from Oracle Supplier Hub
*****************************************************************/
PROCEDURE slc_validate_p
IS
BEGIN
	slc_write_log_p(gv_log,'In slc_validate_p gn_batch_id:'||gn_batch_id);

	--Reset all the error messages.
	UPDATE SLC_ISP_SUPPTAX_PARTY_CNV_STG
	  SET ERROR_MSG = NULL
		,request_id = gn_request_id
		,last_update_date = sysdate
		,last_updated_by = gn_user_id
		,last_update_login = gn_login_id
	WHERE batch_id = gn_batch_id;

	--If taxpayer Id extracted is null then mark it as error as tax payer id is mandatory column.
	UPDATE SLC_ISP_SUPPTAX_PARTY_CNV_STG
	  SET ERROR_MSG = 'Tax PayerId value is null'
	WHERE batch_id = gn_batch_id
	  AND TAX_PAYERID IS NULL
	  AND invoice_count <> 0;	
	  
	--If taxpayer Id extracted is not null then verify if we Supplier is converted during Supplier conversion
	-- and has tax payer id matching found.
	UPDATE SLC_ISP_SUPPTAX_PARTY_CNV_STG stg
	  SET ERROR_MSG = ERROR_MSG||'~Matching tax payer id not found'
	WHERE batch_id = gn_batch_id
	  AND TAX_PAYERID IS NOT NULL
	  AND invoice_count <> 0
	  AND NOT EXISTS ( -- This query will fetch tax payer id for records which is converted by supplier conversion program
					   -- 
					  SELECT hp.jgzz_fiscal_code
					    FROM ap_suppliers sup
							,hz_parties hp
							,POS_SUPP_PROF_EXT_B pos
					   WHERE sup.vendor_type_lookup_code = 'FRANCHISEE' -- We should only pick Supplier of Type Franchisee
						 AND sup.party_id = hp.party_id 
						 AND pos.party_id = sup.party_id
						 AND hp.jgzz_fiscal_code = stg.TAX_PAYERID
						 AND pos.c_ext_attr4 = 'FAS'
					  );	

	--If there is error then mark record status as Failed.
	UPDATE SLC_ISP_SUPPTAX_PARTY_CNV_STG
	  SET status = gv_invalid_status
	WHERE batch_id = gn_batch_id
	  AND ERROR_MSG IS NOT NULL;
	  
	--If there is no error then mark record status as Valid.
	UPDATE SLC_ISP_SUPPTAX_PARTY_CNV_STG
	  SET status = gv_valid_status
	WHERE batch_id = gn_batch_id
	  AND ERROR_MSG IS NULL;	
	
	/* Setting error message and error flag in child tables */
	UPDATE SLC_ISP_SUPPTAX_CNV_STG SET status = gv_invalid_status , error_msg = 'Error at parent record'
	 WHERE parent_record_id IN (SELECT record_id
								  FROM SLC_ISP_SUPPTAX_PARTY_CNV_STG
								 WHERE status = gv_invalid_status
								   AND batch_id = gn_batch_id
								);
								
	UPDATE SLC_ISP_SUPTAX_CONTPT_CNV_STG SET status = gv_invalid_status , error_msg = 'Error at parent record'
	 WHERE parent_record_id IN (SELECT record_id
								  FROM SLC_ISP_SUPPTAX_PARTY_CNV_STG
								 WHERE status = gv_invalid_status
								   AND batch_id = gn_batch_id
								);

	UPDATE SLC_ISP_SUPTAX_CONTDR_CNV_STG SET status = gv_invalid_status , error_msg = 'Error at parent record'
	 WHERE parent_record_id IN (SELECT record_id
								  FROM SLC_ISP_SUPPTAX_PARTY_CNV_STG
								 WHERE status = gv_invalid_status
								   AND batch_id = gn_batch_id
								);								
	/* If parent record is valid then marking child record as valid */
	UPDATE SLC_ISP_SUPPTAX_CNV_STG SET status = gv_valid_status ,error_msg = NULL
	 WHERE parent_record_id IN (SELECT record_id
								  FROM SLC_ISP_SUPPTAX_PARTY_CNV_STG
								 WHERE status = gv_valid_status
								   AND batch_id = gn_batch_id
								)
		AND status IN ('E','F','N');
								
	UPDATE SLC_ISP_SUPTAX_CONTPT_CNV_STG SET status = gv_valid_status ,error_msg = NULL
	 WHERE parent_record_id IN (SELECT record_id
								  FROM SLC_ISP_SUPPTAX_PARTY_CNV_STG
								 WHERE status = gv_valid_status
								   AND batch_id = gn_batch_id
								)
			AND status IN ('E','F','N');

	UPDATE SLC_ISP_SUPTAX_CONTDR_CNV_STG SET status = gv_valid_status ,error_msg = NULL
	 WHERE parent_record_id IN (SELECT record_id
								  FROM SLC_ISP_SUPPTAX_PARTY_CNV_STG
								 WHERE status = gv_valid_status
								   AND batch_id = gn_batch_id
								)
		AND status IN ('E','F','N');
	
END slc_validate_p;

/* ****************************************************************
	NAME:              slc_import_contact_dt_p
	PURPOSE:           This procedure will be used to import contact point information in 
							 Oracle Supplier Hub
	INPUT PARAMETERS: 
									p_in_parent_record_id	IN NUMBER
									p_in_old_party_id		IN NUMBER
									p_in_old_vendor_id		IN NUMBER
									p_in_new_party_id		IN NUMBER
									p_in_new_vendor_id		IN NUMBER
									p_out_error_flag		OUT VARCHAR2
									p_out_error_msg		OUT VARCHAR2		
*****************************************************************/
  PROCEDURE slc_import_contact_dt_p(p_in_parent_record_id	IN NUMBER
									,p_in_old_party_id		IN NUMBER
									,p_in_old_vendor_id		IN NUMBER
									,p_in_new_party_id		IN NUMBER
									,p_in_new_vendor_id		IN NUMBER
									,p_out_error_flag		OUT VARCHAR2
									,p_out_error_msg		OUT VARCHAR2
									)
  IS 
  CURSOR cur_contact_directory
  IS
  SELECT * 
    FROM SLC_ISP_SUPTAX_CONTDR_CNV_STG
  WHERE parent_record_id = p_in_parent_record_id
    AND status NOT IN ('P','D');--Exclude record which has been processed or duplicated earlier.
	
	lc_contact_directory_rec		cur_contact_directory%ROWTYPE;
   TYPE lc_contact_dir_tbl IS TABLE OF cur_contact_directory%ROWTYPE
    INDEX BY BINARY_INTEGER;	
   lc_contact_dir_tab             		lc_contact_dir_tbl; 
   
	lv_error_flag			VARCHAR2(1);
	lv_error_msg			VARCHAR2(4000);
	
  CURSOR cur_contact_dt_details(p_in_party_id 	IN 	NUMBER
							   ,p_in_person_first_name IN VARCHAR2
							   ,p_in_person_middle_name IN VARCHAR2
							   ,p_in_person_last_name IN VARCHAR2
							   ,p_in_phone_area_code IN VARCHAR2
							   ,p_in_phone_number IN VARCHAR2
							   ,p_in_email IN VARCHAR2
							   )
  IS 
	SELECT count(1)
	FROM
	  (SELECT hpc.person_first_name ,
		hpc.person_middle_name ,
		hpc.person_last_name ,
		hcpp.phone_area_code ,
		hcpp.phone_number ,
		hcpe.email_address
	  FROM hz_parties hpc ,
		hz_contact_points hcpp ,
		hz_contact_points hcpe ,
		hz_relationships hr ,
		hz_org_contacts hoc ,
		hz_parties hpr
	  WHERE hcpp.owner_table_name(+)    = 'HZ_PARTIES'
	  AND hcpp.owner_table_id(+)        = hr.party_id
	  AND hcpp.phone_line_type(+)       = 'GEN'
	  AND hcpp.contact_point_type(+)    = 'PHONE'
	  AND hcpe.owner_table_name(+)      = 'HZ_PARTIES'
	  AND hcpe.owner_table_id(+)        = hr.party_id
	  AND hcpe.contact_point_type(+)    = 'EMAIL'
	  AND hr.object_id                  = hpc.party_id
	  AND hr.subject_Type               = 'ORGANIZATION'
	  AND hr.subject_table_name         = 'HZ_PARTIES'
	  AND hr.object_table_name          = 'HZ_PARTIES'
	  AND hr.object_Type                = 'PERSON'
	  AND hr.relationship_code          = 'CONTACT'
	  AND hr.directional_flag           = 'B'
	  AND hr.RELATIONSHIP_TYPE          = 'CONTACT'
	  AND hr.party_id                   =hpr.party_id
	  AND hcpe.status (+)               = 'A'
	  AND hcpe.primary_flag(+)          ='Y'
	  AND hcpp.status (+)               = 'A'
	  AND hcpp.primary_flag(+)          ='Y'
	  AND hoc.party_relationship_id (+) = hr.relationship_id
	  AND hr.subject_id                 = p_in_party_id
	  )
	WHERE NVL(person_first_name,'-Z')  = NVL(p_in_person_first_name,'-Z') 
	AND NVL(person_last_name,'-Z')   = NVL(p_in_person_last_name,'-Z')
	AND NVL(phone_area_code,'-Z')    = NVL(p_in_phone_area_code,'-Z')
	AND NVL(phone_number,'-Z')       = NVL(p_in_phone_number,'-Z')
	AND NVL(email_address,'-Z')      = NVL(p_in_email,'-Z');


	ln_contact_directory_count		NUMBER;
	
	
  BEGIN
  slc_write_log_p(gv_log,'');
  slc_write_log_p(gv_log,'**********************slc_import_contact_dt_p:Start*********************');
  slc_write_log_p(gv_log,'In slc_import_contact_dt_p p_in_old_party_id:'||p_in_old_party_id||	
						' p_in_old_vendor_id:'||p_in_old_vendor_id||
						' p_in_new_party_id:'||p_in_new_party_id||
						' p_in_new_vendor_id:'||p_in_new_vendor_id);
  OPEN cur_contact_directory;
  LOOP
  lc_contact_dir_tab.DELETE;
  FETCH cur_contact_directory 
  BULK COLLECT INTO lc_contact_dir_tab LIMIT 1000;
  EXIT WHEN lc_contact_dir_tab.COUNT = 0;
  
	  FOR ln_index IN lc_contact_dir_tab.FIRST..lc_contact_dir_tab.LAST
	  LOOP	
	  SAVEPOINT contact_dir;
	  lc_contact_directory_rec	:= lc_contact_dir_tab(ln_index);  
	  BEGIN
	  slc_write_log_p(gv_log,'In slc_import_contact_dt_p Record Id:'||lc_contact_directory_rec.record_id||
							 ' contact_party_id: '||lc_contact_directory_rec.contact_party_id);

	  --Reinitializing variables						
		lv_error_flag			:= 'N';
		lv_error_msg			:= NULL;

		slc_write_log_p(gv_log,'p_in_new_party_id:'||p_in_new_party_id);
		slc_write_log_p(gv_log,'person_first_name:'||lc_contact_directory_rec.person_first_name );
		slc_write_log_p(gv_log,'person_middle_name:'||lc_contact_directory_rec.person_middle_name);
		slc_write_log_p(gv_log,'person_last_name:'||lc_contact_directory_rec.person_last_name);
		slc_write_log_p(gv_log,'phone_number:'||lc_contact_directory_rec.phone_number);
		slc_write_log_p(gv_log,'phone_area_code:'||lc_contact_directory_rec.phone_area_code);
		slc_write_log_p(gv_log,'email:'||lc_contact_directory_rec.email);
		
		-- Verify if contact directory information is already existing.
		OPEN cur_contact_dt_details(p_in_new_party_id
									,lc_contact_directory_rec.person_first_name 	
									,lc_contact_directory_rec.person_middle_name 	
									,lc_contact_directory_rec.person_last_name
									,lc_contact_directory_rec.phone_area_code								
									,lc_contact_directory_rec.phone_number			
									,lc_contact_directory_rec.email
									);
		FETCH cur_contact_dt_details INTO ln_contact_directory_count;
		CLOSE cur_contact_dt_details;
		
		slc_write_log_p(gv_log,'ln_contact_directory_count:'||ln_contact_directory_count);
		-- If bank account information is already existing then do not perform anything.
		IF ln_contact_directory_count > 0 THEN
		 lv_error_flag := 'D';
		ELSE	
			IF lc_contact_directory_rec.person_last_name IS NULL THEN
				lv_error_flag  := 'Y';	
				lv_error_msg   := 'Person last name is null';				
			ELSE
				slc_write_log_p(gv_log,'****Contact Directory creation Begin******');
				slc_create_contact_directory_p  (p_in_new_vendor_id	
												,lc_contact_directory_rec.person_first_name 	
												,lc_contact_directory_rec.person_middle_name 	
												,lc_contact_directory_rec.person_last_name 		
												,lc_contact_directory_rec.phone_number			
												,lc_contact_directory_rec.phone_area_code
												,lc_contact_directory_rec.email
												,lv_error_flag				 
												,lv_error_msg							
												);
				slc_write_log_p(gv_log,'****Contact Directory creation End******');
			END IF;
		END IF;
	  EXCEPTION
	  WHEN OTHERS THEN
		lv_error_flag  := 'Y';
		lv_error_msg   := lv_error_msg||'~Unexpected error in main for loop of slc_import_contact_dt_p. Error Message:'||SQLERRM;
	  END;	
	  slc_write_log_p(gv_log,'In slc_import_contact_dt_p. Final values before updating lv_error_flag:'||lv_error_flag||
							 ' lv_error_msg:'||lv_error_msg);
						 
	  -- If there is any error for record then mark out variable and populate error message.					 
	  IF lv_error_flag = 'Y' THEN
	   p_out_error_flag := 'Y';
	   p_out_error_msg  := p_out_error_msg ||'~Contact Directory Error in record id:'||lc_contact_directory_rec.record_id 
												||' Error Message.'||lv_error_msg;
	   ROLLBACK TO contact_dir;
	  ELSE
	   COMMIT;
	  END IF;
	  UPDATE SLC_ISP_SUPTAX_CONTDR_CNV_STG 
		 SET status = DECODE(lv_error_flag,'Y',gv_error_status,'N',gv_processed_status,'D',gv_duplicate_status)
			 ,error_msg = DECODE(lv_error_flag,'Y',lv_error_msg,'N',NULL,'D','Duplicate contact directory information')
			 ,request_id = gn_request_id
			 ,last_update_date = sysdate
			 ,last_updated_by = gn_user_id
			 ,last_update_login = gn_login_id
	  WHERE record_id = lc_contact_directory_rec.record_id;	
	  COMMIT;
	  END LOOP;
  END LOOP;
  CLOSE cur_contact_directory;
  IF p_out_error_flag IS NULL THEN
	p_out_error_flag := 'N';
  END IF;
  slc_write_log_p(gv_log,'**********************slc_import_contact_dt_p:End*********************');  
  slc_write_log_p(gv_log,'');
  EXCEPTION
  WHEN OTHERS THEN
	slc_write_log_p(gv_log,'Unexpected error in slc_import_contact_dt_p. Error Message:'||SQLERRM);
	p_out_error_flag := 'Y';
	p_out_error_msg	:= 'Unexpected error in slc_import_contact_dt_p. Error Message:'||SQLERRM;	  
  END slc_import_contact_dt_p;
  
/* ****************************************************************
	NAME:              slc_import_contact_pt_p
	PURPOSE:           This procedure will be used to import contact point information in 
							 Oracle Supplier Hub
	INPUT PARAMETERS: 
									p_in_parent_record_id	IN NUMBER
									p_in_old_party_id		IN NUMBER
									p_in_old_vendor_id		IN NUMBER
									p_in_new_party_id		IN NUMBER
									p_in_new_vendor_id		IN NUMBER
									p_out_error_flag		OUT VARCHAR2
									p_out_error_msg		OUT VARCHAR2		
*****************************************************************/
  PROCEDURE slc_import_contact_pt_p(p_in_parent_record_id	IN NUMBER
									,p_in_old_party_id		IN NUMBER
									,p_in_old_vendor_id		IN NUMBER
									,p_in_new_party_id		IN NUMBER
									,p_in_new_vendor_id		IN NUMBER
									,p_out_error_flag		OUT VARCHAR2
									,p_out_error_msg		OUT VARCHAR2
									)
  IS 
  CURSOR cur_contact_point
  IS
  SELECT * 
    FROM SLC_ISP_SUPTAX_CONTPT_CNV_STG
  WHERE parent_record_id = p_in_parent_record_id
   AND status NOT IN ('P','D');--Exclude record which has been processed or duplicated earlier.

	lc_contact_point_rec  	cur_contact_point%ROWTYPE; 
   TYPE lc_contact_point_tbl IS TABLE OF cur_contact_point%ROWTYPE
    INDEX BY BINARY_INTEGER;
    
   lc_contact_point_tab             		lc_contact_point_tbl; 
   
	lv_error_flag			VARCHAR2(1);
	lv_error_msg			VARCHAR2(4000);  
	
	lv_phone_error_flag		VARCHAR2(1);
	lv_phone_error_msg		VARCHAR2(4000);
	lv_email_error_flag		VARCHAR2(1);
	lv_email_error_msg		VARCHAR2(4000);
	
   CURSOR cur_phone_contact_pt_details(p_in_party_id  IN 	NUMBER
									 ,p_in_phone_area_code	IN VARCHAR2
									 ,p_in_phone_number 	IN VARCHAR2
									) 
	IS
	SELECT count(1)
	FROM hz_contact_points
	WHERE owner_table_id        = p_in_party_id
	AND contact_point_type      = 'PHONE'
	AND NVL(phone_area_code,-1) = NVL(p_in_phone_area_code,-1)
	AND phone_number            = p_in_phone_number
	AND phone_line_type         = 'GEN'
	AND status                  = 'A';
	
	ln_phone_contact_count		NUMBER;	 

   CURSOR cur_email_contact_pt_details(p_in_party_id  IN 	NUMBER
									 ,p_in_email      IN VARCHAR2
									) 
	IS
	SELECT count(1)
	FROM hz_contact_points
	WHERE owner_table_id        = p_in_party_id
	AND contact_point_type      = 'EMAIL'
	AND email_address = p_in_email
	AND status                  = 'A';
	
	ln_email_contact_count		NUMBER;		
	
  BEGIN
  slc_write_log_p(gv_log,'');
  slc_write_log_p(gv_log,'**********************slc_import_contact_pt_p:Start*********************');
  slc_write_log_p(gv_log,'In slc_import_contact_pt_p p_in_old_party_id:'||p_in_old_party_id||	
						' p_in_old_vendor_id:'||p_in_old_vendor_id||
						' p_in_new_party_id:'||p_in_new_party_id||
						' p_in_new_vendor_id:'||p_in_new_vendor_id);

  OPEN 	cur_contact_point;				
  LOOP
  lc_contact_point_tab.DELETE;
  FETCH cur_contact_point
  BULK COLLECT INTO lc_contact_point_tab LIMIT 1000;
  EXIT WHEN lc_contact_point_tab.COUNT = 0;
	
  FOR ln_index IN lc_contact_point_tab.FIRST..lc_contact_point_tab.LAST
  LOOP	
  SAVEPOINT contact_point;
  lc_contact_point_rec	:= lc_contact_point_tab(ln_index);
  BEGIN
  slc_write_log_p(gv_log,'In slc_import_contact_pt_p Record Id:'||lc_contact_point_rec.record_id||
						 ' party_site_id: '||lc_contact_point_rec.party_site_id);


  --Reinitializing variables						
	lv_error_flag			:= 'N';
	lv_error_msg			:= NULL;   
	lv_phone_error_flag		:= 'N';
	lv_phone_error_msg		:= NULL;  
	lv_email_error_flag		:= 'N';
	lv_email_error_msg		:= NULL;  
	
	--If phone_contact_point_id is not null it means that contact point information is present.
	IF lc_contact_point_rec.phone_contact_point_id IS NOT NULL THEN
		-- Verify if contact point information is already existing for Supplier.
		OPEN cur_phone_contact_pt_details(p_in_new_party_id,lc_contact_point_rec.phone_area_code,lc_contact_point_rec.phone_number);
		FETCH cur_phone_contact_pt_details INTO ln_phone_contact_count;
		CLOSE cur_phone_contact_pt_details;
		slc_write_log_p(gv_log,'ln_phone_contact_count:'||ln_phone_contact_count);
		-- If contact point information is already existing then do not perform anything.
		IF ln_phone_contact_count > 0 THEN
		 lv_phone_error_flag := 'D';
		ELSE
		--Call Contact API to import PHONE contact
			slc_write_log_p(gv_log,'****Calling for PHONE Contact Start******');
			slc_create_contact_point_p(p_in_new_party_id	
									  ,'PHONE'	
									  ,lc_contact_point_rec.phone_area_code		
									  ,lc_contact_point_rec.phone_number		
									  ,NULL		--Email Address	
									  ,lv_phone_error_flag				 
									  ,lv_phone_error_msg													  
									  );
			slc_write_log_p(gv_log,'****Calling for PHONE Contact End******');
		END IF;
	END IF;-- End of Phone Contact Point Check.

	--If email_contact_point_id is not null it means that contact point information is present.
	IF lc_contact_point_rec.email_contact_point_id IS NOT NULL THEN
		-- Verify if contact point information is already existing for Supplier.
		OPEN cur_email_contact_pt_details(p_in_new_party_id,lc_contact_point_rec.email);
		FETCH cur_email_contact_pt_details INTO ln_email_contact_count;
		CLOSE cur_email_contact_pt_details;
		slc_write_log_p(gv_log,'ln_email_contact_count:'||ln_email_contact_count);

		-- If contact point information is already existing then do not perform anything.
		IF ln_email_contact_count > 0 THEN
		 lv_email_error_flag := 'D';
		ELSE
		--Call Contact API to import EMAIL contact
			slc_write_log_p(gv_log,'****Calling for EMAIL Contact Start******');
			slc_create_contact_point_p(p_in_new_party_id	
									  ,'EMAIL'	
									  ,NULL --Phone Area Code
									  ,NULL --Phone Number
									  ,lc_contact_point_rec.email			
									  ,lv_email_error_flag				 
									  ,lv_email_error_msg													  
									  );
			slc_write_log_p(gv_log,'****Calling for EMAIL Contact End******');
		END IF;
	END IF;-- End of Phone Contact Point Check.	

	
  EXCEPTION
  WHEN OTHERS THEN
    lv_error_flag  := 'Y';
	lv_error_msg   := 'Unexpected error in main for loop of slc_import_contact_pt_p. Error Message:'||SQLERRM;
  END;	  
  -- If there is any error for record then mark out variable and populate error message.

  --If both Phone and Email information is already existing then we mark record as duplicate
  IF ((lc_contact_point_rec.phone_contact_point_id IS NOT NULL AND lv_phone_error_flag = 'D') AND
	 (lc_contact_point_rec.email_contact_point_id IS NOT NULL AND  lv_email_error_flag = 'D') AND 
	 lv_error_flag = 'N' )THEN
   lv_error_flag    := 'D';
   slc_write_log_p(gv_log,'In condition 1');
   ROLLBACK TO contact_point;
  ELSIF lv_error_flag = 'Y' OR lv_phone_error_flag = 'Y' OR lv_email_error_flag = 'Y' THEN
   lv_error_flag    := 'Y';
   lv_error_msg 	:= lv_error_msg||'~'||lv_phone_error_msg||'~'||lv_email_error_msg;
   p_out_error_flag := 'Y';
   p_out_error_msg  := p_out_error_msg ||'~Contact Point Error in record id:'||lc_contact_point_rec.record_id ||' Error Message.'||lv_error_msg;
   slc_write_log_p(gv_log,'In condition 2');
   ROLLBACK TO contact_point;
  ELSE
	COMMIT;
  END IF;

  slc_write_log_p(gv_log,'In slc_import_contact_pt_p. Final values before updating lv_phone_error_flag:'||lv_phone_error_flag||
						 ' lv_email_error_flag:'||lv_email_error_flag||' lv_error_flag:'||lv_error_flag||
						 ' lv_error_msg:'||lv_error_msg);
   slc_write_log_p(gv_log,' p_out_error_flag:'||p_out_error_flag||
						 ' p_out_error_msg:'||p_out_error_msg||' Record Id:'||lc_contact_point_rec.record_id);  
  
  UPDATE SLC_ISP_SUPTAX_CONTPT_CNV_STG 
	 SET status = DECODE(lv_error_flag,'Y',gv_error_status,'N',gv_processed_status,'D',gv_duplicate_status)
		 ,error_msg = DECODE(lv_error_flag,'Y',lv_error_msg,'N',NULL,'D','Duplicate contact point information')
		 ,request_id = gn_request_id
	     ,last_update_date = sysdate
	     ,last_updated_by = gn_user_id
	     ,last_update_login = gn_login_id
  WHERE record_id = lc_contact_point_rec.record_id;
  COMMIT;
  slc_write_log_p(gv_log,' Test Before End:');
  END LOOP;
  END LOOP;
  CLOSE cur_contact_point;
  IF p_out_error_flag IS NULL THEN
	p_out_error_flag := 'N';
  END IF;
  
  slc_write_log_p(gv_log,'**********************slc_import_contact_pt_p:End*********************');  
  slc_write_log_p(gv_log,'');
  EXCEPTION
  WHEN OTHERS THEN
	slc_write_log_p(gv_log,'Unexpected error in slc_import_contact_pt_p. Error Message:'||SQLERRM);
	p_out_error_flag := 'Y';
	p_out_error_msg	:= 'Unexpected error in slc_import_contact_pt_p. Error Message:'||SQLERRM;	    
  END slc_import_contact_pt_p;
  
  
/* ****************************************************************
	NAME:              slc_import_bank_accounts_p
	PURPOSE:           This procedure will be used import bank account information
							 Oracle Supplier Hub
	INPUT PARAMETERS: 
									p_in_parent_record_id	IN NUMBER
									p_in_old_party_id		IN NUMBER
									p_in_old_vendor_id		IN NUMBER
									p_in_new_party_id		IN NUMBER
									p_in_new_vendor_id		IN NUMBER
									p_out_error_flag		OUT VARCHAR2
									p_out_error_msg		OUT VARCHAR2							 
*****************************************************************/
  PROCEDURE slc_import_bank_accounts_p(p_in_parent_record_id	IN NUMBER
									,p_in_old_party_id		IN NUMBER
									,p_in_old_vendor_id		IN NUMBER
									,p_in_new_party_id		IN NUMBER
									,p_in_new_vendor_id		IN NUMBER
									,p_out_error_flag		OUT VARCHAR2
									,p_out_error_msg		OUT VARCHAR2
									)
  IS
  CURSOR cur_bank_account
  IS
  SELECT * 
    FROM SLC_ISP_SUPPTAX_CNV_STG
  WHERE parent_record_id = p_in_parent_record_id
    AND status NOT IN ('P','D');--Exclude record which has been processed or duplicated earlier.
	lv_error_flag			VARCHAR2(1);
	lv_error_msg			VARCHAR2(4000);

	lc_bank_account_rec  	cur_bank_account%ROWTYPE; 
   TYPE lc_bank_account_tbl IS TABLE OF cur_bank_account%ROWTYPE
    INDEX BY BINARY_INTEGER;
    
   lc_bank_account_tab             		lc_bank_account_tbl; 
   
  CURSOR cur_bank_account_details(p_in_ext_bank_account_id  IN 	NUMBER
								 ,p_in_party_id			  IN    NUMBER
								) 
	IS
	SELECT count(1)
	  FROM iby_account_owners iao
	 WHERE iao.account_owner_party_id = p_in_party_id
	   AND ext_bank_account_id = p_in_ext_bank_account_id;
	ln_bank_account_count		NUMBER;	   
  BEGIN
  slc_write_log_p(gv_log,'');
  slc_write_log_p(gv_log,'**********************slc_import_bank_accounts_p:Start*********************');
  slc_write_log_p(gv_log,'In slc_import_bank_accounts_p p_in_old_party_id:'||p_in_old_party_id||	
						' p_in_old_vendor_id:'||p_in_old_vendor_id||
						' p_in_new_party_id:'||p_in_new_party_id||
						' p_in_new_vendor_id:'||p_in_new_vendor_id);

  OPEN 	cur_bank_account;				
  LOOP
  lc_bank_account_tab.DELETE;
  FETCH cur_bank_account
  BULK COLLECT INTO lc_bank_account_tab LIMIT 1000;
  EXIT WHEN lc_bank_account_tab.COUNT = 0;						
  FOR ln_index IN lc_bank_account_tab.FIRST..lc_bank_account_tab.LAST
  LOOP	
  SAVEPOINT contact_bank_account;
  lc_bank_account_rec	:= lc_bank_account_tab(ln_index);  
  BEGIN
  slc_write_log_p(gv_log,'In slc_import_bank_accounts_p Record Id:'||lc_bank_account_rec.record_id||
						 ' Ext_Bank_Account_Id: '||lc_bank_account_rec.ext_bank_account_id);

  --Reinitializing variables						
	lv_error_flag			:= 'N';
	lv_error_msg			:= NULL;  
	
	-- Verify if bank account is already existing.
	OPEN cur_bank_account_details(lc_bank_account_rec.ext_bank_account_id,p_in_new_party_id);
	FETCH cur_bank_account_details INTO ln_bank_account_count;
	CLOSE cur_bank_account_details;
	
	-- If bank account information is already existing then do not perform anything.
	IF ln_bank_account_count > 0 THEN
	 lv_error_flag := 'D';
	ELSE
	-- Call Oracle API's to process bank account information.
	-- Steps are 
	-- 1. Add new supplier as joint account owner.
		slc_write_log_p(gv_log,'****Adding Account Owner Start******');
		slc_add_joint_account_owner_p(lc_bank_account_rec.ext_bank_account_id
									 ,p_in_new_party_id
									 ,lv_error_flag
									 ,lv_error_msg
									 );	
		slc_write_log_p(gv_log,'****Adding Account Owner End******');

	 --2. If new supplier is added as joint account owner then create payment assignment for new supplier.
		IF lv_error_flag = 'N' THEN
		slc_write_log_p(gv_log,'****Adding Instrument Start******');
		slc_set_pmt_instr_uses_p(lc_bank_account_rec.ext_bank_account_id
								 ,p_in_new_party_id
								 ,lv_error_flag
								 ,lv_error_msg
								 );		
		slc_write_log_p(gv_log,'****Adding Instrument End******');
		END IF;

	 --3. Make new supplier as primary account owner.
		IF lv_error_flag = 'N' THEN
		slc_write_log_p(gv_log,'****Adding Primary Account Begin******');
		slc_cng_prim_acct_owner_p(lc_bank_account_rec.ext_bank_account_id
								 ,p_in_new_party_id
								 ,lv_error_flag
								 ,lv_error_msg
								 );
		slc_write_log_p(gv_log,'****Adding Primary Account End******');
		END IF;		
		
	 --4. Make default payment method for new supplier as EFT.
	 -- In Supplier Conversion while creating Supplier we make default payment as CHECK as we are not sure if 
	 -- Supplier will have bank account or not.
	 -- In this conversion if any supplier has bank account then we can mark default payment method as EFT
	 -- Thus calling slc_make_bank_acct_eft_p in slc_import_bank_accounts_p because bank account information exists.
	 
		IF lv_error_flag = 'N' THEN
		slc_write_log_p(gv_log,'****Making Default Payment EFT Start******');
		slc_make_bank_acct_eft_p(p_in_new_party_id
								 ,lv_error_flag
								 ,lv_error_msg
								 );
		slc_write_log_p(gv_log,'****Making Default Payment EFT End******');
		END IF;			
	END IF;

    
  EXCEPTION
  WHEN OTHERS THEN
    lv_error_flag  := 'Y';
	lv_error_msg   := 'Unexpected error in main for loop of slc_import_bank_accounts_p. Error Message:'||SQLERRM;
  END;	
  
  slc_write_log_p(gv_log,'In slc_import_bank_accounts_p. Final values before updating lv_error_flag:'||lv_error_flag||
						 ' lv_error_msg:'||lv_error_msg);
  
  -- If there is any error for record then mark out variable and populate error message.					 
  IF lv_error_flag = 'Y' THEN
   p_out_error_flag := 'Y';
   p_out_error_msg  := p_out_error_msg ||'~Bank Account Error in record id:'||lc_bank_account_rec.record_id ||' Error Message.'||lv_error_msg;
   ROLLBACK TO contact_bank_account;
  ELSE
   COMMIT;
  END IF;
  UPDATE SLC_ISP_SUPPTAX_CNV_STG 
	 SET status = DECODE(lv_error_flag,'Y',gv_error_status,'N',gv_processed_status,'D',gv_duplicate_status)
		 ,error_msg = DECODE(lv_error_flag,'Y',lv_error_msg,'N',NULL,'D','Duplicate bank account information')
		 ,request_id = gn_request_id
	     ,last_update_date = sysdate
	     ,last_updated_by = gn_user_id
	     ,last_update_login = gn_login_id
  WHERE record_id = lc_bank_account_rec.record_id;
  COMMIT;
  END LOOP;
  END LOOP;
  CLOSE cur_bank_account;
  
  IF p_out_error_flag IS NULL THEN
	p_out_error_flag := 'N';
  END IF;
  slc_write_log_p(gv_log,'**********************slc_import_bank_accounts_p:End*********************');  
  slc_write_log_p(gv_log,'');
  EXCEPTION
  WHEN OTHERS THEN
	slc_write_log_p(gv_log,'Unexpected error in slc_import_bank_accounts_p. Error Message:'||SQLERRM);
	p_out_error_flag := 'Y';
	p_out_error_msg	:= 'Unexpected error in slc_import_bank_accounts_p. Error Message:'||SQLERRM;	  
  END slc_import_bank_accounts_p;

/* ****************************************************************
	NAME:              slc_import_p
	PURPOSE:           This procedure will be import data into Supplier Hub.
*****************************************************************/	
  PROCEDURE slc_import_p
  IS
   CURSOR c_party_rec
  IS
  SELECT record_id,batch_id,status,vendor_id,party_id,tax_payerid
		 ,state_reportable_flag,federal_reportable_flag,type_1099,tax_reporting_name
		 ,invoice_count --Changes for v1.1
    FROM SLC_ISP_SUPPTAX_PARTY_CNV_STG
   WHERE status = gv_valid_status
     AND batch_id = gn_batch_id
	 AND redundant_flag = 'N';

   TYPE lc_party_rec_tbl IS TABLE OF c_party_rec%ROWTYPE
    INDEX BY BINARY_INTEGER;
    
   lc_party_rec_tab             		lc_party_rec_tbl; 
   
   CURSOR cur_remittance_info(p_in_old_party_id 	IN 	NUMBER)
   IS
	SELECT remit_advice_delivery_method,
	  remit_advice_email,
	  remit_advice_fax
	FROM iby_external_payees_all
	WHERE payee_party_id             = p_in_old_party_id
	AND (org_type                    IS NULL
	AND org_id                      IS NULL
	AND supplier_site_id            IS NULL
	AND party_site_id               IS NULL);
	
	lv_remit_advice_del_method    		iby_external_payees_all.remit_advice_delivery_method%TYPE;
	lv_remit_advice_email    			iby_external_payees_all.remit_advice_email%TYPE;
	lv_remit_advice_fax    				iby_external_payees_all.remit_advice_fax%TYPE;
	
	ln_new_party_id				ap_suppliers.party_id%TYPE;
	ln_new_vendor_id			ap_suppliers.vendor_id%TYPE;
	lv_tax_reporting_name		ap_suppliers.tax_reporting_name%TYPE;
	lv_error_msg				VARCHAR2(4000);
	lv_error_flag				VARCHAR2(1);
	
	lv_bank_acct_error_flag			VARCHAR2(1);
	lv_bank_acct_error_msg			VARCHAR2(4000);
	lv_contact_pt_error_flag		VARCHAR2(1);
	lv_contact_pt_error_msg			VARCHAR2(4000);
	lv_contact_dir_error_flag		VARCHAR2(1);
	lv_contact_dir_error_msg		VARCHAR2(4000);	
  BEGIN
	OPEN c_party_rec;
	LOOP
	lc_party_rec_tab.DELETE;
	FETCH c_party_rec
	BULK COLLECT INTO lc_party_rec_tab LIMIT 10000;
	EXIT WHEN lc_party_rec_tab.COUNT = 0;
		
	--For all the valid records call API's to import supplier into Oracle Supplier Hub.
	FOR ln_index IN lc_party_rec_tab.FIRST..lc_party_rec_tab.LAST
	LOOP
	slc_write_log_p(gv_log,'');
	slc_write_log_p(gv_log,'********************************Record Import Start******************************************');
	slc_write_log_p(gv_log,'In slc_import_p Record Id: '||lc_party_rec_tab(ln_index).record_id||' Party Id:'||lc_party_rec_tab(ln_index).party_id
							||' Vendor Id:'||lc_party_rec_tab(ln_index).vendor_id
							||' Invoice Count:'||lc_party_rec_tab(ln_index).invoice_count);
	
	--Reinitializing variables
	lv_error_flag := 'N';
	lv_error_msg  := NULL;
	ln_new_party_id	:= NULL;
	ln_new_vendor_id	:= NULL;
	lv_tax_reporting_name	:= NULL;
	lv_bank_acct_error_flag			:= 'N';
	lv_bank_acct_error_msg			:= NULL;
	lv_contact_pt_error_flag		:= 'N';
	lv_contact_pt_error_msg			:= NULL;
	lv_contact_dir_error_flag		:= 'N';
	lv_contact_dir_error_msg		:= NULL;
	
	--Changes for v1.1.
	--Added condition for invoice_count. If invoice_count <> 0 only then we perform all operation to restructure information for supplier.
	IF lc_party_rec_tab(ln_index).invoice_count <> 0 THEN
		BEGIN
		SELECT hp.party_id, sup.vendor_id , sup.tax_reporting_name
		  INTO ln_new_party_id , ln_new_vendor_id , lv_tax_reporting_name
		  FROM ap_suppliers sup
				,hz_parties hp
				,POS_SUPP_PROF_EXT_B pos
		   WHERE sup.vendor_type_lookup_code = 'FRANCHISEE' -- We should only pick Supplier of Type Franchisee
			 AND sup.party_id = hp.party_id 
			 AND pos.party_id = sup.party_id
			 AND hp.jgzz_fiscal_code = lc_party_rec_tab(ln_index).TAX_PAYERID 
			 AND pos.c_ext_attr4 = 'FAS';
		EXCEPTION
		WHEN NO_DATA_FOUND THEN
		 lv_error_flag := 'Y';
		 lv_error_msg  := 'No data found for new Supplier';
		WHEN OTHERS THEN
		 lv_error_flag := 'Y';
		 lv_error_msg  := 'Unexpected exception when fetching new Supplier details. Error Message:'||SQLERRM;		
		END;
		slc_write_log_p(gv_log,'In slc_import_p ln_new_party_id:'||ln_new_party_id||' ln_new_vendor_id:'||ln_new_vendor_id);
		-- If there is no error in fetching new supplier details call API's to import bank account information,
		-- Contact Point Information and Contact Directory Information.

		IF lv_error_flag = 'N' THEN
		
			--If tax reporting name of Supplier is null only then we override it with tax reporting name if existing supplier.
			IF lv_tax_reporting_name IS NULL THEN
				lv_tax_reporting_name := lc_party_rec_tab(ln_index).tax_reporting_name;
			END IF;
			slc_write_log_p(gv_log,'In slc_import_p lv_tax_reporting_name:'||lv_tax_reporting_name);
			--Call API to update reportable flags
			slc_update_vendor_p
			(ln_new_vendor_id
			 ,lc_party_rec_tab(ln_index).state_reportable_flag
			 ,lc_party_rec_tab(ln_index).federal_reportable_flag
			 ,lc_party_rec_tab(ln_index).type_1099
			 ,lv_tax_reporting_name
			 ,NULL --end date parameter
			 ,lv_error_flag
			 ,lv_error_msg
			);
		  IF lv_error_flag = 'N' THEN
			  OPEN cur_remittance_info(lc_party_rec_tab(ln_index).party_id);
			  FETCH cur_remittance_info INTO lv_remit_advice_del_method,lv_remit_advice_email,lv_remit_advice_fax;
			  CLOSE cur_remittance_info;
			  
			  --If any of the remittance information is available then call  API to populate remittance 
			  --information for new supplier.
			  IF lv_remit_advice_del_method IS NOT NULL OR 
				 lv_remit_advice_email IS NOT NULL OR 
				 lv_remit_advice_fax IS NOT NULL THEN
				
				 slc_update_external_payee_p
						 (ln_new_party_id 	
						  ,lv_remit_advice_del_method			
						  ,lv_remit_advice_email				
						  ,lv_remit_advice_fax				
						  ,lv_error_flag  	
						  ,lv_error_msg			
						  );			 
			  END IF;
		  END IF;
		  IF lv_error_flag = 'N' THEN	
			  -- Call Bank Account Information API
			  slc_import_bank_accounts_p( lc_party_rec_tab(ln_index).record_id
										 ,lc_party_rec_tab(ln_index).party_id
										 ,lc_party_rec_tab(ln_index).vendor_id
										 ,ln_new_party_id
										 ,ln_new_vendor_id
										 ,lv_bank_acct_error_flag
										 ,lv_bank_acct_error_msg
										 );
			  -- Call Contact Point API							 
			  slc_import_contact_pt_p( lc_party_rec_tab(ln_index).record_id
										 ,lc_party_rec_tab(ln_index).party_id
										 ,lc_party_rec_tab(ln_index).vendor_id
										 ,ln_new_party_id
										 ,ln_new_vendor_id
										 ,lv_contact_pt_error_flag
										 ,lv_contact_pt_error_msg
										 );		

			  -- Call Contact Directory API							 
			  slc_import_contact_dt_p( lc_party_rec_tab(ln_index).record_id
										 ,lc_party_rec_tab(ln_index).party_id
										 ,lc_party_rec_tab(ln_index).vendor_id
										 ,ln_new_party_id
										 ,ln_new_vendor_id
										 ,lv_contact_dir_error_flag
										 ,lv_contact_dir_error_msg
										 );
				slc_write_log_p(gv_log,'After slc_import_bank_accounts_p lv_bank_acct_error_flag:'||lv_bank_acct_error_flag||
									   ' lv_bank_acct_error_msg:'||lv_bank_acct_error_msg);
				slc_write_log_p(gv_log,'After slc_import_contact_pt_p lv_contact_pt_error_flag:'||lv_contact_pt_error_flag||
									   ' lv_contact_pt_error_msg:'||lv_contact_pt_error_msg);
				slc_write_log_p(gv_log,'After slc_import_contact_dt_p lv_contact_dir_error_flag:'||lv_contact_dir_error_flag||
									   ' lv_contact_dir_error_msg:'||lv_contact_dir_error_msg);										 
			END IF;-- End of if for update vendor error check.
		END IF;
	END IF;--End of invoice_count check.
	
	IF lv_bank_acct_error_flag = 'Y' OR lv_contact_pt_error_flag = 'Y' OR lv_contact_dir_error_flag = 'Y' THEN
		lv_error_flag := 'Y';
		lv_error_msg := lv_error_msg||'~'||lv_bank_acct_error_msg||'~'||lv_contact_pt_error_msg||'~'||lv_contact_dir_error_msg;
	-- If there is no error in the any of the child records then mark existing supplier as end dated.
	ELSIF lv_error_flag = 'N' THEN
		slc_write_log_p(gv_log,'End Dating existing supplier');

		slc_update_vendor_p
		(lc_party_rec_tab(ln_index).vendor_id
		 ,NULL
		 ,NULL
		 ,NULL
		 ,NULL
		 ,sysdate --end date parameter
		 ,lv_error_flag
		 ,lv_error_msg
		);

	END IF;
  slc_write_log_p(gv_log,'In slc_import_p. Final values before updating lv_error_flag:'||lv_error_flag||
						 ' lv_error_msg:'||lv_error_msg);
						 
	UPDATE SLC_ISP_SUPPTAX_PARTY_CNV_STG 
	 set status = DECODE(lv_error_flag,'Y',gv_error_status,'N',gv_processed_status)
			     ,error_msg = DECODE(lv_error_flag,'Y',lv_error_msg,'N',NULL)
				 ,new_vendor_id = ln_new_vendor_id
				 ,new_party_id = ln_new_party_id
				 ,request_id = gn_request_id
				 ,last_update_date = sysdate
				 ,last_updated_by = gn_user_id
				 ,last_update_login = gn_login_id				   
	WHERE record_id = lc_party_rec_tab(ln_index).record_id;
	
	-- For records which are not having invoices created in last 2 years invoice_count would be 0.
	-- Thus for those records slc_import_bank_accounts_p and slc_import_contact_pt_p and slc_import_contact_dt_p
	-- will not be called. Thus for those records making the status as processed or error in child tables.
	-- Also records for which there is error while fetching supplier information or in slc_update_vendor_p
	-- then mark child records as error for those records.
	UPDATE slc_isp_supptax_cnv_stg
	   SET status = DECODE(lv_error_flag,'Y',gv_error_status,'N',gv_processed_status)
			 ,error_msg = DECODE(lv_error_flag,'Y',lv_error_msg,'N',NULL)
			 ,request_id = gn_request_id
			 ,last_update_date = sysdate
			 ,last_updated_by = gn_user_id
			 ,last_update_login = gn_login_id				   
	WHERE parent_record_id = lc_party_rec_tab(ln_index).record_id
	  AND status = gv_valid_status;
	  
	UPDATE slc_isp_suptax_contpt_cnv_stg
	   SET status = DECODE(lv_error_flag,'Y',gv_error_status,'N',gv_processed_status)
			 ,error_msg = DECODE(lv_error_flag,'Y',lv_error_msg,'N',NULL)
			 ,request_id = gn_request_id
			 ,last_update_date = sysdate
			 ,last_updated_by = gn_user_id
			 ,last_update_login = gn_login_id				   
	WHERE parent_record_id = lc_party_rec_tab(ln_index).record_id
	  AND status = gv_valid_status;	  
	  
	UPDATE slc_isp_suptax_contdr_cnv_stg
	   SET status = DECODE(lv_error_flag,'Y',gv_error_status,'N',gv_processed_status)
			 ,error_msg = DECODE(lv_error_flag,'Y',lv_error_msg,'N',NULL)
			 ,request_id = gn_request_id
			 ,last_update_date = sysdate
			 ,last_updated_by = gn_user_id
			 ,last_update_login = gn_login_id				   
	WHERE parent_record_id = lc_party_rec_tab(ln_index).record_id
	  AND status = gv_valid_status;	  	  
	  
	COMMIT;
	slc_write_log_p(gv_log,'********************************Record Import End******************************************');
	slc_write_log_p(gv_log,'');
	END LOOP;--End of internal collection loop.
	
	END LOOP;--End of Cursor open loop.
	CLOSE c_party_rec;
  EXCEPTION
  WHEN OTHERS THEN
	slc_write_log_p(gv_out,'Unexpected error in slc_import_p. Error Message:'||SQLERRM);
  END slc_import_p;  
  
/* ****************************************************************
	NAME:              slc_main_p
  -- Purpose          : This is main procedure invoked by SLCISP - Supplier Tax Restructing Conversion Program.
  -- Input Parameters : 
  --  p_processing_mode : Processing mode for program
  --  p_debug_flag      : Debug Flag will decide if we want to log messages.
  --  p_batch_size      : This value would determine how many records to pick for conversion
  --
  -- Output Parameters :
  --  p_errbuff        : Standard output parameter with Return Message for concurrent program
  --  p_retcode        : Standard output parameter with Return Code for concurrent program
  --
  ********************************************************************************************/

	PROCEDURE slc_main_p  ( p_errbuff           OUT VARCHAR2,
			 p_retcode           OUT NUMBER  ,
             p_processing_mode   IN  VARCHAR2,
             p_batch_size          IN  NUMBER,
             p_debug_flag        IN  VARCHAR2
			)
	IS
	lv_status1 		VARCHAR2(20)	DEFAULT NULL;
	lv_status2 		VARCHAR2(20) 	DEFAULT NULL;
	lv_status3 		VARCHAR2(20)	DEFAULT NULL;
	ln_batch_size	NUMBER		DEFAULT 50000;	
    lv_error_flag		VARCHAR2(1) DEFAULT 'N';
    lv_error_msg		VARCHAR2(4000) DEFAULT NULL;
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
	
	IF p_processing_mode = gv_load_mode THEN
		slc_load_p;
	END IF;

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
	slc_write_log_p(gv_log,'In slc_main_p. gn_program_status:'||gn_program_status||' p_retcode:'||p_retcode);	
	END slc_main_p;

/*	*****************************************************************  
  -- Procedure Name   : slc_payment_method_change
  -- Purpose          : This procedure will take all new supplier for which bank account exists and change 
  --					the default payment method to EFT.
  -- Input Parameters : 
  --  p_debug_flag      : Debug Flag will decide if we want to log messages.
  --
  -- Output Parameters :
  --  p_errbuff        : Standard output parameter with Return Message for concurrent program
  --  p_retcode        : Standard output parameter with Return Code for concurrent program
  --********************************************************************************************/	
 PROCEDURE slc_payment_method_change ( p_errbuff           OUT VARCHAR2,
									  p_retcode           OUT NUMBER,  
									  p_debug_flag        IN  VARCHAR2
									)
 IS
 CURSOR cur_payment_details
 IS
  SELECT iepa.*
	FROM iby_external_payees_all iepa 
	,iby_ext_party_pmt_mthds pmt
	WHERE iepa.payee_party_id IN
	  ( SELECT DISTINCT stg.new_party_id
	  FROM slc_isp_supptax_party_cnv_stg stg ,
		slc_isp_supptax_cnv_stg bstg
	  WHERE stg.redundant_flag  = 'N'
	  AND stg.status            = 'P'
	  AND stg.invoice_count    <> 0
	  AND bstg.redundant_flag   = 'N'
	  AND bstg.status           = 'P'
	  AND bstg.parent_record_id = stg.record_id
	  )
	  and iepa.ext_payee_id = pmt.ext_pmt_party_id
	  and pmt.primary_flag = 'Y';
  p_api_version 	NUMBER;
  p_init_msg_list   VARCHAR2(200);
  
  p_ext_payee_tab 		apps.iby_disbursement_setup_pub.external_payee_tab_type;
  p_ext_payee_rec 		apps.iby_disbursement_setup_pub.external_payee_rec_type;
  p_ext_payee_id_tab 	apps.iby_disbursement_setup_pub.ext_payee_id_tab_type;
  p_ext_id_rec 			apps.iby_disbursement_setup_pub.ext_payee_id_rec_type;
  p_ret_status_tab 		apps.iby_disbursement_setup_pub.ext_payee_update_tab_type;
  p_ret_status_rec   	apps.iby_disbursement_setup_pub.ext_payee_update_rec_type;  
  lv_return_status 		VARCHAR2(200);
  ln_msg_count 			NUMBER;
  lv_msg                VARCHAR2(4000);
  lv_msg_out  			NUMBER;
  lv_msg_data           VARCHAR2(1000);
  lv_error_flag			VARCHAR2(1) DEFAULT 'N';
  lv_error_msg			VARCHAR2(4000) DEFAULT NULL; 
  ln_eft_update_count	NUMBER DEFAULT 0;  
  
 BEGIN
	gv_debug_flag := p_debug_flag;
	slc_write_log_p(gv_out,'p_debug_flag: '||p_debug_flag);
	FOR pmt_rec IN cur_payment_details
	LOOP
	slc_write_log_p(gv_log,'In slc_payment_method_change Ext_Payee_Id:'||pmt_rec.ext_payee_id||
							' Payee_Party_Id:'||pmt_rec.payee_party_id);
		
		--Initializing local variable
		lv_return_status := 'S';
		lv_error_flag := 'N';
		lv_error_msg := NULL;
		ln_msg_count := 0;
		p_api_version := 1.0;
		p_init_msg_list := null;	
		p_ext_payee_rec.Payee_Party_Id := pmt_rec.payee_party_id;
		p_ext_payee_rec.Payment_Function := pmt_rec.payment_function;
		p_ext_payee_rec.Exclusive_Pay_Flag := pmt_rec.exclusive_payment_flag;
		p_ext_payee_rec.Payee_Party_Site_Id := pmt_rec.party_site_id;
		p_ext_payee_rec.Supplier_Site_Id := pmt_rec.supplier_site_id;
		p_ext_payee_rec.Payer_Org_Id := pmt_rec.org_id;
		p_ext_payee_rec.Payer_Org_Type := pmt_rec.org_type;  
		p_ext_payee_rec.Default_Pmt_method := 'EFT';  
		p_ext_payee_rec.ece_tp_loc_code := pmt_rec.ece_tp_location_code;
		p_ext_payee_rec.bank_charge_bearer := pmt_rec.bank_charge_bearer;
		p_ext_payee_rec.bank_instr1_code := pmt_rec.bank_instruction1_code;
		p_ext_payee_rec.bank_instr2_code := pmt_rec.bank_instruction2_code;
		p_ext_payee_rec.bank_instr_detail := pmt_rec.bank_instruction_details;
		p_ext_payee_rec.pay_reason_code := pmt_rec.payment_reason_code;
		p_ext_payee_rec.pay_reason_com := pmt_rec.payment_reason_comments;
		p_ext_payee_rec.inactive_date := pmt_rec.inactive_date;
		p_ext_payee_rec.pay_message1 := pmt_rec.payment_text_message1;
		p_ext_payee_rec.pay_message2 := pmt_rec.payment_text_message2;
		p_ext_payee_rec.pay_message3 := pmt_rec.payment_text_message3;
		p_ext_payee_rec.delivery_channel := pmt_rec.delivery_channel_code;
		p_ext_payee_rec.pmt_format := pmt_rec.payment_format_code;
		p_ext_payee_rec.settlement_priority := pmt_rec.settlement_priority;
		p_ext_payee_rec.remit_advice_delivery_method := pmt_rec.remit_advice_delivery_method;
		p_ext_payee_rec.remit_advice_email := pmt_rec.remit_advice_email;
		p_ext_payee_rec.remit_advice_fax := pmt_rec.remit_advice_fax;
		p_ext_payee_tab(1) := p_ext_payee_rec;
		p_ext_id_rec.ext_payee_id := pmt_rec.ext_payee_id;
	    p_ext_payee_id_tab(1) := p_ext_id_rec;
		
		fnd_msg_pub.initialize;
		  iby_disbursement_setup_pub.update_external_payee(
			p_api_version => p_api_version,
			p_init_msg_list => p_init_msg_list,
			p_ext_payee_tab => p_ext_payee_tab,
			p_ext_payee_id_tab => p_ext_payee_id_tab,
			x_return_status => lv_return_status,
			x_msg_count => ln_msg_count,
			x_msg_data => lv_msg_data,
			x_ext_payee_status_tab => p_ret_status_tab
		  );	

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
			p_ret_status_rec := p_ret_status_tab(1);
			lv_error_msg := lv_error_msg||'~'||p_ret_status_rec.payee_update_msg;
			ELSIF lv_return_status = 'S' THEN
			ln_eft_update_count := ln_eft_update_count + 1;
			END IF;
	END LOOP;
	COMMIT;
	slc_write_log_p(gv_out,'Total records updated with payment method as EFT :'||ln_eft_update_count);
	--Updating 10 Cent value for Converted Supplier having Bank Account Information
	
	UPDATE iby_ext_bank_accounts SET attribute7 = 'Passed'
	 WHERE ext_bank_account_id IN (select ieba.ext_bank_account_id 
									from ap_suppliers sup
									,pos_supp_prof_ext_b  pos
									,ego_attr_groups_v eagv
									,fnd_application fa
									,iby_account_owners iao
									,iby_ext_bank_accounts ieba
									where sup.vendor_type_lookup_code = 'FRANCHISEE'
									and sup.party_id = pos.party_id
								    AND eagv.attr_group_name = 'SLC_ISP_FRANCHISEE_DETAILS'
								    AND eagv.attr_group_id = pos.attr_group_id
								    AND fa.application_short_name = 'POS'
								    AND fa.application_id = eagv.application_id	
									and iao.account_owner_party_id = pos.party_id
									and ieba.ext_bank_account_id = iao.ext_bank_account_id );
	slc_write_log_p(gv_out,'Total records updated with 10 Cent Value :'||SQL%ROWCOUNT);
 EXCEPTION
 WHEN OTHERS THEN
  slc_write_log_p(gv_out,'Unexpected error in slc_payment_method_change.Error Message:'||SQLERRM);
 END slc_payment_method_change;							

END SLC_ISP_SUPPTAX_CNV_PKG;
/
SHOW ERROR;

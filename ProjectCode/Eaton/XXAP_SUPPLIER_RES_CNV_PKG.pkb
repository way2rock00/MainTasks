------------------------------------------------------------------------------------------
--    Owner        : EATON CORPORATION.
--    Application  : Accounts Payables
--    Schema       : APPS
--    Compile AS   : APPS
--    File Name    : XXAP_SUPPLIER_RES_CNV_PKG.pkb
--    Date         : 08-AUG-2019
--    Author       : Akshay Nayak
--    Description  : Extract table to save supplier site related bank data needed for restructuring.
--
--    Version      : $ETNHeader: /CCSTORE/ccweb/C9902060/C9902060_ETN_AP_TOP/vobs/AP_TOP/xxap/12.0.0/install/XXAP_SUPPLIER_CNV_PKG.pks /main/1 24-Jul-2014 19:00:08 C9902060  $
--
--    Parameters  :
--
--    Change History
--    Version     Created By       Date            Comments
--  ======================================================================================
--    v1.0        Akshay Nayak    08-AUG-2019     Creation
--    ====================================================================================
------------------------------------------------------------------------------------------
CREATE OR REPLACE PACKAGE BODY XXAP_SUPPLIER_RES_CNV_PKG
AS

  -- global variables

  g_request_id      NUMBER DEFAULT fnd_global.conc_request_id;
  g_user_id         NUMBER DEFAULT fnd_global.user_id;
  g_login_id        NUMBER DEFAULT fnd_global.login_id;
  g_batch_id	    NUMBER;
  g_log 		VARCHAR2(5) := 'LOG';
  g_out		VARCHAR2(5) := 'OUT';  
  
  g_program_status				NUMBER;

  
  --global variables for stauts
  g_new 	   varchar2(1) := 'N';
  g_validated 	   varchar2(1) := 'V';
  g_error	   varchar2(1) := 'E';
  g_processed 	   varchar2(1) := 'P';
  g_completed 	   varchar2(1) := 'C';
  g_process_flag   varchar2(1) := 'Y';
  g_ignored	   varchar2(1) := 'X';
  
  
  --global variables for error
  g_indx    NUMBER := 0;
  g_limit   CONSTANT NUMBER := fnd_profile.value('ETN_FND_ERROR_TAB_LIMIT');  
  g_source_Tab xxetn_common_error_pkg.g_source_tab_type;
  
  --supplier site variable
  g_vendor_site_rec ap_vendor_pub_pkg.r_vendor_site_rec_type;
  
  --supplier contact variable
  g_contact_point_rec hz_contact_point_v2pub.contact_point_rec_type;
  g_phone_rec hz_contact_point_v2pub.phone_rec_type;
  g_edi_rec_type hz_contact_point_v2pub.edi_rec_type;
  g_email_rec_type hz_contact_point_v2pub.email_rec_type;
  g_telex_rec_type hz_contact_point_v2pub.telex_rec_type;
  g_web_rec_type hz_contact_point_v2pub.web_rec_type;
  
  --supplier bank variable
  g_payee		 apps.iby_disbursement_setup_pub.payeecontext_rec_type;
  g_assignment_attribs   apps.iby_fndcpt_setup_pub.pmtinstrassignment_rec_type;
  
  procedure log_message(p_in_log_type IN VARCHAR2
  			,p_in_message IN VARCHAR2
  			)
  is
  begin
  	dbms_output.put_line(p_in_message);
	    IF p_in_log_type = g_log 
	    THEN
	       fnd_file.put_line (fnd_file.LOG, p_in_message);
	    END IF;

	    IF p_in_log_type = g_out
	    THEN
	       fnd_file.put_line (fnd_file.output, p_in_message);
	    END IF;
  	--xxetn_debug_pkg.g_debug := fnd_api.g_true;
  	--xxetn_debug_pkg.add_debug(p_in_message);
  	--null;
  end log_message;
  
  -- ========================
  -- Procedure: log_errors
  -- =============================================================================
  --   This procedure will log the errors in the error report using error
  --   framework
  -- =============================================================================
  --  Input Parameters :
  --    piv_source_column_name  : Column Name - column which has errored
  --    piv_source_column_value : Column Value - the value in the errored column
  --    piv_error_type          : Error Type - validation or import error
  --    piv_error_code          : Error Code - error at which point in execution
  --    piv_error_message       : Error Message - description of error

  --  Output Parameters :
  --    pov_return_status  : Return Status - Success / Error
  --    pov_error_msg      : Error message in case of any failure
  -- -----------------------------------------------------------------------------

  PROCEDURE log_errors(pov_return_status       OUT NOCOPY VARCHAR2,
                       pov_error_msg           OUT NOCOPY VARCHAR2,
                       piv_source_table	       IN VARCHAR2,
                       pin_record_id	       IN NUMBER,
                       piv_src_keyvalue1       IN VARCHAR2,
                       piv_src_keyname3       IN VARCHAR2,
                       piv_src_keyvalue3       IN VARCHAR2,
                       piv_src_keyname4      IN VARCHAR2,
                       piv_src_keyvalue4       IN VARCHAR2,
                       piv_src_keyname5       IN VARCHAR2,
                       piv_src_keyvalue5       IN VARCHAR2,
                       piv_source_column_name  IN xxetn_common_error.source_column_name%TYPE DEFAULT NULL,
                       piv_source_column_value IN xxetn_common_error.source_column_value%TYPE DEFAULT NULL,
                       piv_error_type          IN xxetn_common_error.error_type%TYPE,
                       piv_error_code          IN xxetn_common_error.error_code%TYPE,
                       piv_error_message       IN xxetn_common_error.error_message%TYPE) IS

    l_return_status VARCHAR2(50);
    l_error_msg     VARCHAR2(2000);

  BEGIN



    pov_return_status := NULL;
    pov_error_msg     := NULL;

    log_message(g_log,'p_err_msg: ' || piv_source_column_name);
    log_message(g_log,'g_limit: ' || g_limit);
    log_message(g_log,'g_indx: ' || g_indx);


    --increment index for every new insertion in the error table
    g_indx := g_indx + 1;

    --assignment of the error record details into the table type
    g_source_Tab(g_indx).source_table := piv_source_table;
    g_source_Tab(g_indx).interface_staging_id := pin_record_id;
    g_source_Tab(g_indx).source_keyname1 := 'PKG_NAME.PRC_FUNC_NAME';
    g_source_Tab(g_indx).source_keyvalue1 := piv_src_keyvalue1;
    g_source_Tab(g_indx).source_keyname2 := 'CONC_REQUEST_ID';
    g_source_Tab(g_indx).source_keyvalue2 := g_request_id;
    g_source_Tab(g_indx).source_keyname3 := piv_src_keyname3;
    g_source_Tab(g_indx).source_keyvalue3 := piv_src_keyvalue3;
    g_source_Tab(g_indx).source_keyname4 := piv_src_keyname4;
    g_source_Tab(g_indx).source_keyvalue4 := piv_src_keyvalue4;
    g_source_Tab(g_indx).source_keyname5 := piv_src_keyname5;
    g_source_Tab(g_indx).source_keyvalue5 := piv_src_keyvalue5;
    g_source_Tab(g_indx).source_column_name := piv_source_column_name;
    g_source_Tab(g_indx).source_column_value := piv_source_column_value;
    g_source_Tab(g_indx).error_type := piv_error_type;
    g_source_Tab(g_indx).error_code := piv_error_code;
    g_source_Tab(g_indx).error_message := piv_error_message;


    IF MOD(g_indx, g_limit) = 0 THEN

      xxetn_common_error_pkg.add_error(pov_return_status => l_return_status -- OUT
                                     ,pov_error_msg     => l_error_msg -- OUT
                                     ,pi_source_tab     => g_source_Tab -- IN  G_SOURCE_TAB_TYPE
                                     ,pin_batch_id      => g_batch_id);

      g_source_Tab.DELETE;
      pov_return_status := l_return_status;
      pov_error_msg     := l_error_msg;

      log_message(g_log,'Calling xxetn_common_error_pkg.add_error ' ||l_return_status || ', ' || l_error_msg);

      g_indx := 0;

    END IF;
  EXCEPTION

    WHEN OTHERS THEN
      log_message(g_log,'Error: Exception occured in log_errors procedure ' ||SUBSTR(SQLERRM, 1, 240));

  END log_errors;
  
    -- ========================
  -- Procedure: update_supplier_site
  -- =============================================================================
  --   This procedure will be called from main method when run mode is close to end-date
  --	supplier sites that has been successfully converted.
  -- =============================================================================   
   -- -----------------------------------------------------------------------------
   --
   --  Input Parameters :
   --    pin_vendor_site_rec : Record type variable to pass site information to be updated.
   --
   --  Output Parameters :
   --    pov_error_flag          : Flag that indicates if the procedure has encountered any exception.
   --    pov_error_msg           : Error message if the procedure has encountered any exception.
   --
   -- -----------------------------------------------------------------------------
  
  PROCEDURE update_supplier_site(pin_vendor_site_rec 	IN  g_vendor_site_rec%type
  				,pov_error_flag		OUT VARCHAR2
  				,pov_error_msg		OUT VARCHAR2
  				)
  IS
	lv_return_status     VARCHAR2(10) default 'S';
	ln_msg_count     NUMBER;
	lv_msg_data  VARCHAR2(1000);  
	lv_error_message    VARCHAR2(1000);
	lv_out_message     varchar2(1000);
	ln_msg_index	 number;	
	
  BEGIN

	log_message(g_log,'In update_supplier_site');
	MO_GLOBAL.SET_POLICY_CONTEXT('S',pin_vendor_site_rec.org_id);
	
	--temp
        --fnd_global.apps_initialize(123250, 51368, 200);
        
	pos_vendor_pub_pkg.Update_Vendor_Site
	(
	p_vendor_site_rec => pin_vendor_site_rec,
	x_return_status => lv_return_status,
	x_msg_count => ln_msg_count,
	x_msg_data => lv_msg_data
	);
	log_message(g_log,'In update_supplier_site. return_status: '||lv_return_status);
	log_message(g_log,'In update_supplier_site. msg_data: '||lv_msg_data);	
	log_message(g_log,'In update_supplier_site. ln_msg_count: '||ln_msg_count);
	IF (lv_return_status is null or lv_return_status <> 'S' )THEN
		FOR i IN 1 .. FND_MSG_PUB.Count_Msg
		LOOP
		   FND_MSG_PUB.Get (p_msg_index       => i,
				    p_encoded         => 'F',
				    p_data            => lv_out_message,
				    p_msg_index_OUT   => ln_msg_index);
		   lv_error_message := lv_error_message || ' ' || lv_out_message;
		END LOOP;
	ELSE
		--no need to commit changes here. it will be commited from calling program.
		--COMMIT;
		log_message(g_log,'In update_supplier_site. Site information updated successfully');
	END IF;	
	
	pov_error_flag		:= lv_return_status;
  	pov_error_msg		:= lv_error_message;
	log_message(g_log,'In update_supplier_site. pov_error_flag:'||pov_error_flag);
	log_message(g_log,'In update_supplier_site. pov_error_msg:'||pov_error_msg);	

  exception 
	  when others then
	  log_message(g_log,'Exception in update_supplier_site: Error Message:'||SQLERRM);
	  pov_error_flag 	:= 'E';
	  pov_error_msg		:= 'Exception in update_supplier_site: Error Message:'||SQLERRM;
  
  
  END update_supplier_site;
  
  
   -- ========================
  -- Procedure: create_supplier_site
  -- =============================================================================
  --   This procedure will be called to create supplier site in new org and new plant.
  -- =============================================================================   
   -- -----------------------------------------------------------------------------
   --
   --  Input Parameters :
   --    pin_vendor_site_rec : Record type variable to pass site information to be updated.
   --
   --  Output Parameters :
   --    pov_error_flag          : Flag that indicates if the procedure has encountered any exception.
   --    pov_error_msg           : Error message if the procedure has encountered any exception.
   --	 pon_vendor_site_id      : new vendor_site_id created by oracle standard API.
   --	 pon_party_site_id       : new party_site_id created by oracle standard API.
   --	 pon_location_id      	 : new location_id created by oracle standard API.
   -- -----------------------------------------------------------------------------
  PROCEDURE create_supplier_site(pin_vendor_site_rec 	IN  g_vendor_site_rec%type
  				,pov_error_flag		OUT VARCHAR2
  				,pov_error_msg		OUT VARCHAR2
  				,pon_vendor_site_id	OUT NUMBER
  				,pon_party_site_id	OUT NUMBER
  				,pon_location_id	OUT NUMBER
  				)
  IS
	lv_return_status     VARCHAR2(10) default 'S';
	ln_msg_count     NUMBER;
	lv_msg_data  VARCHAR2(1000);
	ln_vendor_site_id    NUMBER;
	ln_party_site_id     NUMBER;
	ln_location_id   NUMBER;
	lv_error_message    VARCHAR2(1000);
	lv_out_message     varchar2(1000);
	ln_msg_index	 number;	
	
	

  
  BEGIN
  
	log_message(g_log,'In create_supplier_site');
	MO_GLOBAL.SET_POLICY_CONTEXT('S',pin_vendor_site_rec.org_id);
	
	--temp
        --fnd_global.apps_initialize(123250, 50858, 200);

	pos_vendor_pub_pkg.create_vendor_site
	(
		p_vendor_site_rec => pin_vendor_site_rec,
		x_return_status => lv_return_status,
		x_msg_count => ln_msg_count,
		x_msg_data => lv_msg_data,
		x_vendor_site_id => ln_vendor_site_id,
		x_party_site_id => ln_party_site_id,
		x_location_id => ln_location_id
	);

	log_message(g_log,'In create_supplier_site. return_status: '||lv_return_status);
	log_message(g_log,'In create_supplier_site. msg_data: '||lv_msg_data);
	log_message(g_log,'In create_supplier_site. vendor_site_id: '||ln_vendor_site_id);
	log_message(g_log,'In create_supplier_site. party_site_id: '||ln_party_site_id);	
	log_message(g_log,'In create_supplier_site. location_id: '||ln_location_id);	
	
	IF (lv_return_status is null or lv_return_status <> 'S') THEN
		FOR i IN 1 .. FND_MSG_PUB.Count_Msg
		LOOP
		   FND_MSG_PUB.Get (p_msg_index       => i,
				    p_encoded         => 'F',
				    p_data            => lv_out_message,
				    p_msg_index_OUT   => ln_msg_index);
		   lv_error_message := lv_error_message || ' ' || lv_out_message;
		END LOOP;
	ELSE
		--no need to commit changes here. it will be commited from calling program.
		--COMMIT;
		log_message(g_log,'In create_supplier_site. Site information created successfully');
	END IF;
	pov_error_flag		:= lv_return_status;
  	pov_error_msg		:= lv_error_message;
	pon_vendor_site_id	:= ln_vendor_site_id;
	pon_party_site_id	:= ln_party_site_id;
	pon_location_id	  	:= ln_location_id;

	log_message(g_log,'In create_supplier_site. pov_error_flag:'||pov_error_flag);
	log_message(g_log,'In create_supplier_site. pov_error_msg:'||pov_error_msg);
	

  exception
  when others then
  log_message(g_log,'Exception in create_supplier_site: Error Message:'||SQLERRM);
  pov_error_flag 	:= 'E';
  pov_error_msg		:= 'Exception in create_supplier_site: Error Message:'||SQLERRM;
  
  END create_supplier_site;
 
      -- ========================
     -- Procedure: create_site_bank_account
  -- ========================================================================================================================
  --   This procedure will be called to assign payment instruction to new supplier site created by create_supplier_site
  -- ========================================================================================================================   
   -- -----------------------------------------------------------------------------
   --
   --  Input Parameters :
   --    pin_payee : Record type variable 
   --    pin_assignment_attribs : Record type variable 
   --
   --  Output Parameters :
   --    pov_error_flag          : Flag that indicates if the procedure has encountered any exception.
   --    pov_error_msg           : Error message if the procedure has encountered any exception.
   --	 pon_assign_id      	 : new INSTRUMENT_PAYMENT_USE_ID created by oracle standard API.
   -- ----------------------------------------------------------------------------- 
  procedure create_site_bank_account(pin_payee IN g_payee%type
  				    ,pin_assignment_attribs IN g_assignment_attribs%type
  				    ,pov_error_flag		OUT VARCHAR2
				    ,pov_error_msg		OUT VARCHAR2
				    ,pon_assign_id OUT NUMBER
  				    )
  is 
  p_api_version          NUMBER default 1.0;
  p_init_msg_list        VARCHAR2(200) default fnd_api.g_true;
  p_commit               VARCHAR2(200)  default fnd_api.g_false;
  lv_return_status     VARCHAR2(10) default 'S';
  ln_msg_count     NUMBER;
  lv_msg_data  VARCHAR2(1000);  
  ln_assign_id number;
  lr_response             apps.iby_fndcpt_common_pub.result_rec_type;
  lv_error_message    VARCHAR2(1000);
  
	
  
  begin
  
  	--fnd_global.apps_initialize(123250, 50858, 200);--temp
  	log_message(g_log,'In create_site_bank_account:');  
  	iby_disbursement_setup_pub.set_payee_instr_assignment
	    (p_api_version        => p_api_version,
	     p_init_msg_list      => p_init_msg_list,
	     p_commit             => p_commit,
	     x_return_status      => lv_return_status,
	     x_msg_count          => ln_msg_count,
	     x_msg_data           => lv_msg_data,
	     p_payee              => pin_payee,
	     p_assignment_attribs => pin_assignment_attribs,
	     x_assign_id          => ln_assign_id,
	     x_response           => lr_response
    	 );
    	 
	log_message(g_log,'In create_site_bank_account. ln_assign_id: '||ln_assign_id);	
	log_message(g_log,'In create_site_bank_account. result_code: '||lr_response.result_code);	
	log_message(g_log,'In create_site_bank_account. result_category: '||lr_response.result_category);	
	log_message(g_log,'In create_site_bank_account. result_message: '||lr_response.result_message);	
	
	IF lv_return_status IS NULL OR lv_return_status <> 'S' THEN
		lv_return_status := 'E';
		lv_error_message:= 'Error Code:'||lr_response.Result_Code||' Error Category:'||lr_response.Result_Category||
				   ' Error Message:'||lr_response.Result_Message;
	ELSE
		--no need to commit changes here. it will be commited from calling program.
		--COMMIT;
		log_message(g_log,'Bank Account information created successfully');
	END IF;
	log_message(g_log,'In create_site_bank_account. lv_error_message:'||lv_error_message);    	
  
  	pov_error_flag	:= lv_return_status;
  	pov_error_msg	:=  lv_error_message;
  	pon_assign_id   := ln_assign_id;
	log_message(g_log,'In create_site_bank_account. pov_error_flag:'||pov_error_flag);
	log_message(g_log,'In create_site_bank_account. pov_error_msg:'||pov_error_msg);
	
  exception
  when others then
  log_message(g_log,'Exception in create_site_bank_account: Error Message:'||SQLERRM);  
  pov_error_flag 	:= 'E';
  pov_error_msg		:= 'Exception in create_site_bank_account: Error Message:'||SQLERRM;
  
  end create_site_bank_account;
  
  /*
     -- ========================
    -- Procedure: create_supplier_site_contact
    -- =============================================================================
    --   This procedure will log the errors in the error report using error
    --   framework
  -- =============================================================================  
  procedure create_supplier_site_contact(pin_contact_point_rec 	IN  g_contact_point_rec%type
  					,pin_edi_rec_type	IN g_edi_rec_type%type
  					,pin_email_rec_type	IN g_email_rec_type%type
  					 ,pin_phone_rec 		IN g_phone_rec%type
  					 ,pin_telex_rec_type	IN g_telex_rec_type%type
  					 ,pin_web_rec_type	IN g_web_rec_type%type
  					 ,pon_contact_point_id	OUT NUMBER
					 ,pov_error_flag		OUT VARCHAR2
					 ,pov_error_msg		OUT VARCHAR2
					)
  is
	lv_return_status     VARCHAR2(10);
	ln_msg_count     NUMBER;
	lv_msg_data  VARCHAR2(1000);
	ln_contact_point_id    NUMBER;  
	lv_out_message     varchar2(1000);
	ln_msg_index	 number;	
	lv_error_message    VARCHAR2(1000);
  begin
  		log_message('In create_supplier_site_contact');


  	HZ_CONTACT_POINT_V2PUB.create_contact_point (
							p_init_msg_list => 'T',
							p_contact_point_rec => pin_contact_point_rec,
							p_edi_rec => pin_edi_rec_type,
							p_email_rec => pin_email_rec_type,
							p_phone_rec => pin_phone_rec,
							p_telex_rec => pin_telex_rec_type,
							p_web_rec => pin_web_rec_type,
							x_contact_point_id => ln_contact_point_id,
							x_return_status => lv_return_status,
							x_msg_count => ln_msg_count,
							x_msg_data => lv_msg_data
						    );
	log_message('ln_contact_point_id: '||ln_contact_point_id);	
	
	IF lv_return_status <> 'S' THEN
		FOR i IN 1 .. FND_MSG_PUB.Count_Msg
		LOOP
		   FND_MSG_PUB.Get (p_msg_index       => i,
				    p_encoded         => 'F',
				    p_data            => lv_out_message,
				    p_msg_index_OUT   => ln_msg_index);
		   lv_error_message := lv_error_message || ' ' || lv_out_message;
		END LOOP;
	ELSE
		--COMMIT;--temp need to remove this commit.
		log_message('Contact information created successfully');
	END IF;
	log_message('lv_error_message:'||lv_error_message);
	
  exception
  when others then
  log_message('Exception in create_supplier_site_contact:');
  
  END create_supplier_site_contact;  
  */
  
  
  -- ========================
  -- Procedure: load_data
  -- ========================================================================================================================
  --   This procedure will be called to load data 
  -- ========================================================================================================================   


   PROCEDURE load_data
   IS
   
   
   CURSOR supplier_site
   IS
   SELECT XXCONV.XXAP_SUPP_SITE_RES_S.nextval record_id 
	,g_request_id request_id
	,g_process_flag process_flag
	,g_new status_flag
	,sups.VENDOR_SITE_ID                		
	,null new_vendor_site_id
	,null LAST_UPDATE_DATE              		
	,null LAST_UPDATED_BY               		
	,sups.VENDOR_ID   
	,sup.VENDOR_NAME
	,sup.VENDOR_TYPE_LOOKUP_CODE
	,sups.VENDOR_SITE_CODE              		
	,sups.VENDOR_SITE_CODE_ALT          		
	,null LAST_UPDATE_LOGIN             		
	,sysdate CREATION_DATE                 		
	,g_user_id CREATED_BY                    	  	
	,sups.PURCHASING_SITE_FLAG          	       
	,sups.RFQ_ONLY_SITE_FLAG                     
	,sups.PAY_SITE_FLAG                          
	,sups.ATTENTION_AR_FLAG                      
	,sups.ADDRESS_LINE1                          
	,sups.ADDRESS_LINES_ALT                      
	,sups.ADDRESS_LINE2                          
	,sups.ADDRESS_LINE3                          
	,sups.CITY                                   
	,sups.STATE                                  
	,sups.ZIP                                    
	,sups.PROVINCE                               
	,sups.COUNTRY                                
	,sups.AREA_CODE                              
	,sups.PHONE                                  
	,sups.CUSTOMER_NUM                           
	,sups.SHIP_TO_LOCATION_ID                    
	,sups.BILL_TO_LOCATION_ID                    
	,sups.SHIP_VIA_LOOKUP_CODE                   
	,sups.FREIGHT_TERMS_LOOKUP_CODE              
	,sups.FOB_LOOKUP_CODE                        
	,sups.INACTIVE_DATE                          
	,sups.FAX                                    
	,sups.FAX_AREA_CODE                          
	,sups.TELEX                                  
	,sups.PAYMENT_METHOD_LOOKUP_CODE             
	,sups.BANK_ACCOUNT_NAME                    
	,sups.BANK_ACCOUNT_NUM                     
	,sups.BANK_NUM                             
	,sups.BANK_ACCOUNT_TYPE                    
	,sups.TERMS_DATE_BASIS                     
	,sups.CURRENT_CATALOG_NUM                  
	,sups.VAT_CODE                             
	,sups.DISTRIBUTION_SET_ID                  
	,sups.ACCTS_PAY_CODE_COMBINATION_ID        
	,null new_accts_pay_code_comb_id
	,sups.PREPAY_CODE_COMBINATION_ID           
	,null new_prepay_code_comb_id
	,sups.PAY_GROUP_LOOKUP_CODE                
	,sups.PAYMENT_PRIORITY                     
	,sups.TERMS_ID                             
	,sups.INVOICE_AMOUNT_LIMIT                 
	,sups.PAY_DATE_BASIS_LOOKUP_CODE           
	,sups.ALWAYS_TAKE_DISC_FLAG                
	,sups.INVOICE_CURRENCY_CODE                
	,sups.PAYMENT_CURRENCY_CODE                
	,sups.HOLD_ALL_PAYMENTS_FLAG               
	,sups.HOLD_FUTURE_PAYMENTS_FLAG            
	,sups.HOLD_REASON                          
	,sups.HOLD_UNMATCHED_INVOICES_FLAG         
	,sups.AP_TAX_ROUNDING_RULE                 
	,sups.AUTO_TAX_CALC_FLAG                   
	,sups.AUTO_TAX_CALC_OVERRIDE               
	,sups.AMOUNT_INCLUDES_TAX_FLAG             
	,sups.EXCLUSIVE_PAYMENT_FLAG               
	,sups.TAX_REPORTING_SITE_FLAG              
	,sups.ATTRIBUTE_CATEGORY                   
	,sups.ATTRIBUTE1                           
	,sups.ATTRIBUTE2                           
	,sups.ATTRIBUTE3                           
	,sups.ATTRIBUTE4                           
	,sups.ATTRIBUTE5                           
	,sups.ATTRIBUTE6                           
	,sups.ATTRIBUTE7                           
	,sups.ATTRIBUTE8                           
	,sups.ATTRIBUTE9                           
	,sups.ATTRIBUTE10                          
	,sups.ATTRIBUTE11                          
	,sups.ATTRIBUTE12                          
	,sups.ATTRIBUTE13                          
	,sups.ATTRIBUTE14                          
	,sups.ATTRIBUTE15                          
	,sups.VALIDATION_NUMBER                    
	,sups.EXCLUDE_FREIGHT_FROM_DISCOUNT        
	,sups.VAT_REGISTRATION_NUM                 
	,sups.OFFSET_VAT_CODE                      
	,sups.ORG_ID                               
	,null new_org_id
	,sups.CHECK_DIGITS                         
	,sups.BANK_NUMBER                          
	,sups.ADDRESS_LINE4                        
	,sups.COUNTY                               
	,sups.ADDRESS_STYLE                        
	,sups.LANGUAGE                             
	,sups.ALLOW_AWT_FLAG                       
	,sups.AWT_GROUP_ID                         
	,sups.GLOBAL_ATTRIBUTE1                    
	,sups.GLOBAL_ATTRIBUTE2                    
	,sups.GLOBAL_ATTRIBUTE3                    
	,sups.GLOBAL_ATTRIBUTE4                    
	,sups.GLOBAL_ATTRIBUTE5                    
	,sups.GLOBAL_ATTRIBUTE6                    
	,sups.GLOBAL_ATTRIBUTE7                    
	,sups.GLOBAL_ATTRIBUTE8                    
	,sups.GLOBAL_ATTRIBUTE9                    
	,sups.GLOBAL_ATTRIBUTE10                   
	,sups.GLOBAL_ATTRIBUTE11                   
	,sups.GLOBAL_ATTRIBUTE12                   
	,sups.GLOBAL_ATTRIBUTE13                   
	,sups.GLOBAL_ATTRIBUTE14                   
	,sups.GLOBAL_ATTRIBUTE15                   
	,sups.GLOBAL_ATTRIBUTE16                   
	,sups.GLOBAL_ATTRIBUTE17                   
	,sups.GLOBAL_ATTRIBUTE18                   
	,sups.GLOBAL_ATTRIBUTE19                   
	,sups.GLOBAL_ATTRIBUTE20                   
	,sups.GLOBAL_ATTRIBUTE_CATEGORY            
	,sups.EDI_TRANSACTION_HANDLING             
	,sups.EDI_ID_NUMBER                        
	,sups.EDI_PAYMENT_METHOD                   
	,sups.EDI_PAYMENT_FORMAT                   
	,sups.EDI_REMITTANCE_METHOD                
	,sups.BANK_CHARGE_BEARER 	                  
	,sups.EDI_REMITTANCE_INSTRUCTION           
	,sups.BANK_BRANCH_TYPE                     
	,sups.PAY_ON_CODE                          
	,sups.DEFAULT_PAY_SITE_ID                  
	,null new_default_pay_site_id
	,sups.PAY_ON_RECEIPT_SUMMARY_CODE          
	,sups.TP_HEADER_ID                         
	,sups.ECE_TP_LOCATION_CODE                 
	,sups.PCARD_SITE_FLAG                      
	,sups.MATCH_OPTION                         
	,sups.COUNTRY_OF_ORIGIN_CODE               
	,sups.FUTURE_DATED_PAYMENT_CCID            
	,sups.CREATE_DEBIT_MEMO_FLAG               
	,sups.OFFSET_TAX_FLAG                      
	,sups.SUPPLIER_NOTIF_METHOD                
	,sups.EMAIL_ADDRESS                        
	,sups.REMITTANCE_EMAIL                     
	,sups.PRIMARY_PAY_SITE_FLAG                
	,sups.SHIPPING_CONTROL                     
	,sups.SELLING_COMPANY_IDENTIFIER           
	,sups.GAPLESS_INV_NUM_FLAG                 
	,sups.DUNS_NUMBER                          
	,sups.TOLERANCE_ID                         
	,sups.LOCATION_ID                          
	,sups.PARTY_SITE_ID                        
	,null new_party_site_id
	,sups.SERVICES_TOLERANCE_ID                
	,sups.RETAINAGE_RATE                       
	,sups.TCA_SYNC_STATE                       
	,sups.TCA_SYNC_PROVINCE                    
	,sups.TCA_SYNC_COUNTY                      
	,sups.TCA_SYNC_CITY                        
	,sups.TCA_SYNC_ZIP                         
	,sups.TCA_SYNC_COUNTRY                     
	,sups.PAY_AWT_GROUP_ID                     
	,sups.CAGE_CODE                            
	,sups.LEGAL_BUSINESS_NAME                  
	,sups.DOING_BUS_AS_NAME                    
	,sups.DIVISION_NAME                        
	,sups.SMALL_BUSINESS_CODE                  
	,sups.CCR_COMMENTS                         
	,sups.DEBARMENT_START_DATE                 
	,sups.DEBARMENT_END_DATE                   
	,sups.ACK_LEAD_TIME        
	--Columns from iby_external_payees_all Begin
	,null PAYEE_PARTY_ID
	,null PAYMENT_FUNCTION
	,null payer_EXCLUSIVE_PAYMENT_FLAG	
	,null DEFAULT_PAYMENT_METHOD_CODE
	,null payer_ECE_TP_LOCATION_CODE	
	,null PAYER_BANK_CHARGE_BEARER	
	,null BANK_INSTRUCTION1_CODE
	,null BANK_INSTRUCTION2_CODE
	,null BANK_INSTRUCTION_DETAILS
	,null PAYMENT_REASON_CODE
	,null PAYMENT_REASON_COMMENTS
	,null PAYER_INACTIVE_DATE	
	,null PAYMENT_TEXT_MESSAGE1
	,null PAYMENT_TEXT_MESSAGE2
	,null PAYMENT_TEXT_MESSAGE3
	,null DELIVERY_CHANNEL_CODE
	,null PAYMENT_FORMAT_CODE
	,null SETTLEMENT_PRIORITY
	,null REMIT_ADVICE_DELIVERY_METHOD
	,null REMIT_ADVICE_EMAIL
	,null REMIT_ADVICE_FAX
	/*,iepa.PAYEE_PARTY_ID
	,iepa.PAYMENT_FUNCTION
	,iepa.EXCLUSIVE_PAYMENT_FLAG	payer_EXCLUSIVE_PAYMENT_FLAG
	,iepa.DEFAULT_PAYMENT_METHOD_CODE
	,iepa.ECE_TP_LOCATION_CODE	payer_ECE_TP_LOCATION_CODE
	,iepa.BANK_CHARGE_BEARER	PAYER_BANK_CHARGE_BEARER
	,iepa.BANK_INSTRUCTION1_CODE
	,iepa.BANK_INSTRUCTION2_CODE
	,iepa.BANK_INSTRUCTION_DETAILS
	,iepa.PAYMENT_REASON_CODE
	,iepa.PAYMENT_REASON_COMMENTS
	,iepa.INACTIVE_DATE	PAYER_INACTIVE_DATE
	,iepa.PAYMENT_TEXT_MESSAGE1
	,iepa.PAYMENT_TEXT_MESSAGE2
	,iepa.PAYMENT_TEXT_MESSAGE3
	,iepa.DELIVERY_CHANNEL_CODE
	,iepa.PAYMENT_FORMAT_CODE
	,iepa.SETTLEMENT_PRIORITY
	,iepa.REMIT_ADVICE_DELIVERY_METHOD
	,iepa.REMIT_ADVICE_EMAIL
	,iepa.REMIT_ADVICE_FAX*/
	--Columns from iby_external_payees_all End
	,null error_message
	--this flag will indicate if successfully converted record has been inactivated or not.
	--If status_flag is P and INACTIVATE_AFTER_PROCESSING is N it means site has been processed but not been inactivated yet.
	,'N' INACTIVATE_AFTER_PROCESSING
	,hou.organization_id  SOURCE_ORG_ID
	,substr(lookup_code,1,instr(lookup_code,'.')-1) source_org_number
	,hou.name  SOURCE_ORG_NAME			
	,substr(lookup_code,instr(lookup_code,'.')+1) source_plant_name
	,hou1.organization_id  DESTINATION_ORG_ID
	,substr(meaning,1,instr(meaning,'.')-1) destination_org_number
	,hou1.name  DESTINATION_ORG_NAME			
	, substr(meaning,instr(meaning,'.')+1) destination_plant_name
	,tag OPERATION	
	--Columns that business will modify
	,null COPY_SITE				
	,gl.concatenated_segments source_accts_segment			
	,g2.concatenated_segments source_prepay_segment	
	,gl.chart_of_accounts_id source_accts_chart_acct_id
	,g2.chart_of_accounts_id source_prepay_chart_acct_id
	,null new_VENDOR_SITE_CODE              	
	,null target_attribute10			
	,null target_attribute15			
	,null target_paygroup			
	,null target_accts_segment			
	--,null target_prepay_segment			
   FROM fnd_lookup_types  flt
	,fnd_lookup_values flv
	,hr_operating_units hou
	,hr_operating_units hou1
	,ap_suppliers sup
	,ap_supplier_sites_all sups
	--,iby_external_payees_all iepa
   	,gl_code_combinations_kfv gl
   	,gl_code_combinations_kfv g2
   where flt.LOOKUP_TYPE like 'XXETN_RESTRUCTURE_MAPPING'--'XXETN_EIC_MERGE_FREEZE_MAPPING'
	and flv.LOOKUP_TYPE = flt.lookup_type
	and flv.language = USERENV('LANG')
	and flv.enabled_flag = 'Y'
	and sysdate between flv.start_date_active and NVL(flv.end_date_active,sysdate)
	and hou.short_code = substr(lookup_code,1,instr(lookup_code,'.')-1)||'_OU'
	and sups.org_id = hou.organization_id 
	and sup.vendor_id = sups.vendor_id
	and gl.code_combination_id = sups.accts_pay_code_combination_id
	and g2.code_combination_id = sups.PREPAY_CODE_COMBINATION_ID
	and gl.segment2 = substr(lookup_code,instr(lookup_code,'.')+1)
	and hou1.short_code = substr(meaning,1,instr(meaning,'.')-1)||'_OU';
	/*and iepa.party_site_id(+) = sups.party_site_id
	and iepa.supplier_site_id(+) = sups.vendor_site_id
	and iepa.org_id(+) = sups.org_id;*/
	--and sups.vendor_id = '86982';--this vendor is used for duplicate bank account testing 
	--and sups.vendor_id in ('59883','58354');
	--and sups.vendor_id in (79166,79226,59883,59845);--temp
	---need to add active status check.
 
   /*sups.org_id = 500169--temp need to change this.
   and gl.code_combination_id = sups.accts_pay_code_combination_id
   and gl.segment2 in ('5245','5243')
   and rownum < 10;*/
   
   type c_supplier_site_type is table of supplier_site%rowtype;
   c_supplier_site_rec	c_supplier_site_type;
   ln_total_record_count	number default 0;
   ln_failed_record_count 	number default 0;
   ln_excluded_record_count	number default 0;


   CURSOR supplier_site_bank
   IS
   select XXCONV.XXAP_SUPP_SITE_BANK_RES_S.nextval record_id 
   	,site_stg.record_id parent_record_id
   	,g_request_id request_id
   	,null LAST_UPDATE_DATE              		           
	,null LAST_UPDATED_BY               		         
	,g_login_id LAST_UPDATE_LOGIN             		         
	,sysdate CREATION_DATE                 		           
	,g_user_id CREATED_BY                    	  	         
   	,g_new status_flag
   	,null ERROR_MESSAGE
   	,iepa.EXT_PAYEE_ID
   	,iepa.payee_party_id
   	,iepa.party_site_id
   	,null new_party_site_id
   	,iepa.supplier_site_id
   	,null new_supplier_site_id
   	,iepa.org_id
   	,null new_org_id
   	,iepa.org_type
   	,ipiua.instrument_id instrument_id
   	,ipiua.instrument_payment_use_id INSTRUMENT_PAYMENT_USE_ID
   	,null new_INSTRUMENT_PAYMENT_USE_ID
   	,ipiua.start_date start_date
   	,ipiua.end_date end_date
   from xxconv.XXAP_SUPPLIER_SITE_RES_STG site_stg
       ,ap_suppliers sup
       ,iby_external_payees_all iepa
       ,iby_pmt_instr_uses_all ipiua
   where site_stg.status_flag = 'N'
     and site_stg.process_flag = g_process_flag
     and site_stg.vendor_id = sup.vendor_id
     and sup.party_id = iepa.payee_party_id
     and (iepa.party_site_id is not null and iepa.supplier_site_id is not null)
     and iepa.party_site_id = site_stg.party_site_id
     and iepa.supplier_site_id = site_stg.vendor_site_id
     and iepa.org_id = site_stg.org_id
     and ipiua.EXT_PMT_PARTY_ID = iepa.EXT_PAYEE_ID;
	---need to add active status check.
	
   type c_supplier_site_bank_type is table of supplier_site_bank%rowtype;
   c_supplier_site_bank_rec	c_supplier_site_bank_type;
   ln_total_bank_record_count	number default 0;
   ln_failed_bank_record_count 	number default 0;
   
   /*CURSOR supplier_site_contact
   IS
   select XXCONV.XXAP_SUPP_SITE_CONTACT_RES_S.nextval record_id 
    from XXAP_SUPPLIER_SITE_RES_STG site_stg
        ,hz_contact_points hcp
   where site_stg.status_flag = 'N'
     and */
   
   

   
   BEGIN
   	log_message(g_log,'Loading Site Data Begin');
   	
   	--Since data for site table will be loaded through loader program.
   	/*
   	open supplier_site;
   	loop
   	fetch supplier_site BULK COLLECT into c_supplier_site_rec limit 10000;
   	exit when c_supplier_site_rec.count = 0;
   	log_message(g_log,'Supplier Count:'||c_supplier_site_rec.count);
   	ln_total_record_count := ln_total_record_count+c_supplier_site_rec.count;
   	
   		begin
   		
			FORALL i IN 1..c_supplier_site_rec.COUNT SAVE EXCEPTIONS
			INSERT INTO xxconv.XXAP_SUPPLIER_SITE_RES_STG VALUES c_supplier_site_rec(i);
		exception
		when others then
		   FOR l_indx_exp IN 1 .. SQL%BULK_EXCEPTIONS.COUNT LOOP
		   ln_failed_record_count := ln_failed_record_count + 1;
		   log_message(g_log,'Exception while loading data in site staging table. Vendor Id:'
		   		||c_supplier_site_rec(SQL%BULK_EXCEPTIONS(l_indx_exp).ERROR_INDEX).vendor_id||
		   		' Error Message:'||SQL%BULK_EXCEPTIONS(l_indx_exp).ERROR_CODE);
		   end loop;
		end;
   		
   	end loop;
   	close supplier_site;

    	log_message(g_log,'Total Site Records extracted:'||ln_total_record_count);
    	log_message(g_log,'Total Site Records loaded:'||(ln_total_record_count-ln_failed_record_count));
    	log_message(g_log,'Failed Site Records while loading:'||ln_failed_record_count);
    	
    	log_message(g_out,'Total Site Records extracted:'||ln_total_record_count);
    	log_message(g_out,'Total Site Records loaded:'||(ln_total_record_count-ln_failed_record_count));
    	log_message(g_out,'Failed Site Records while loading:'||ln_failed_record_count);
    	*/

	--Update the newly loaded records through loader program.
	--process_flag is important column.Set this value to Y only if copy_site flag is set to Y or Yes
	update xxconv.XXAP_SUPPLIER_SITE_RES_STG 
	   set process_flag = DECODE(upper(copy_site),'Y','Y','YES','Y',g_new)
	      ,request_id = g_request_id
	      ,created_by = g_user_id
	      ,creation_date = sysdate
	      ,status_flag = DECODE(upper(copy_site),'Y','N','YES','N',g_ignored)
	   where status_flag = g_new;
	
	select count(*)
	  into ln_total_record_count
	  from xxconv.XXAP_SUPPLIER_SITE_RES_STG 
	  where request_id = g_request_id
	    and status_flag = g_new;
	    
	select count(*)
	  into ln_excluded_record_count
	  from xxconv.XXAP_SUPPLIER_SITE_RES_STG 
	  where request_id = g_request_id
	    and status_flag = g_ignored;	    
	    
    	log_message(g_log,'Total Site Records loaded:'||ln_total_record_count);
    	log_message(g_log,'Site Records excluded by business:'||ln_excluded_record_count);	    
    	log_message(g_out,'Total Site Records loaded:'||ln_total_record_count);
    	log_message(g_out,'Site Records excluded by business:'||ln_excluded_record_count);	    
    	
	/* Loading data into Supplier Bank information */
	
	log_message(g_log,'Loading Site Bank Data Begin');
   	open supplier_site_bank;
   	loop
   	fetch supplier_site_bank BULK COLLECT into c_supplier_site_bank_rec limit 10000;
   	exit when c_supplier_site_bank_rec.count = 0;
   	ln_total_bank_record_count := ln_total_bank_record_count+c_supplier_site_bank_rec.count;
   	
   		begin
   		
			FORALL i IN 1..c_supplier_site_bank_rec.COUNT SAVE EXCEPTIONS
			INSERT INTO xxconv.XXAP_SUPP_SITE_BANK_RES_STG VALUES c_supplier_site_bank_rec(i);
		exception
		when others then
		   FOR l_indx_exp IN 1 .. SQL%BULK_EXCEPTIONS.COUNT LOOP
		   ln_failed_bank_record_count := ln_failed_bank_record_count + 1;
		   log_message(g_log,'Exception while loading data in site staging table. EXT_PAYEE_ID:'
		   		||c_supplier_site_bank_rec(SQL%BULK_EXCEPTIONS(l_indx_exp).ERROR_INDEX).EXT_PAYEE_ID
		   		||' PARTY_SITE_ID:'||c_supplier_site_bank_rec(SQL%BULK_EXCEPTIONS(l_indx_exp).ERROR_INDEX).PARTY_SITE_ID
		   		||' SUPPLIER_SITE_ID:'||c_supplier_site_bank_rec(SQL%BULK_EXCEPTIONS(l_indx_exp).ERROR_INDEX).SUPPLIER_SITE_ID
		   		||' Error Message:'||SQL%BULK_EXCEPTIONS(l_indx_exp).ERROR_CODE);
		   
		   end loop;
		end;
   		
   	end loop;
   	close supplier_site_bank;

    	log_message(g_log,'Total Bank Records extracted:'||ln_total_bank_record_count);
    	log_message(g_log,'Total Bank Records loaded:'||(ln_total_bank_record_count-ln_failed_bank_record_count));
    	log_message(g_log,'Failed Bank Records while loading:'||ln_failed_bank_record_count); 

    	log_message(g_out,'Total Bank Records extracted:'||ln_total_bank_record_count);
    	log_message(g_out,'Total Bank Records loaded:'||(ln_total_bank_record_count-ln_failed_bank_record_count));
    	log_message(g_out,'Failed Bank Records while loading:'||ln_failed_bank_record_count); 
    	
    	commit;
   	
   EXCEPTION
   
   WHEN OTHERS THEN
   log_message(g_log,'Exception in procedure load. Error Message:'||SQLERRM);
   rollback;
   
   END load_data;


  -- ========================
  -- Procedure: get_code_combination_segments
  -- =============================================================================
  --   This procedure will get the code combination segments.
  -- =============================================================================  
  
  PROCEDURE get_code_combination_segments(pin_code_combination_id IN NUMBER
  					  ,pov_concatenated_segment OUT VARCHAR
  					  ,pon_chart_of_account_id OUT NUMBER)
  IS
  BEGIN
  	select concatenated_segments ,chart_of_accounts_id
  	  into pov_concatenated_segment , pon_chart_of_account_id
  	 from gl_code_combinations_kfv
  	 where code_combination_id = pin_code_combination_id;
  	 
  EXCEPTION
  When others then
    log_message(g_log,'Exception in procedure get_code_combination_segments. Error Message:'||SQLERRM);
    pov_concatenated_segment := null;
    pon_chart_of_account_id  := null;
  END get_code_combination_segments;
  
  -- ========================
  -- Procedure: get_code_combination_id
  -- =============================================================================
  --   This procedure will return code combination id. If the code combination does not exists
  --   then new code combination will be created and returned.
  -- =============================================================================  
  PROCEDURE get_code_combination_id(piv_concatenated_segment IN VARCHAR
  				   ,pin_chart_of_account_id  IN NUMBER
  				   ,pon_code_combination_id OUT NUMBER
  				   ,pov_error_message	   OUT VARCHAR2
  				   )
  IS
  BEGIN
  	log_message(g_log,'piv_concatenated_segment:'||piv_concatenated_segment||' pin_chart_of_account_id:'||pin_chart_of_account_id);
	pon_code_combination_id:= fnd_flex_ext.get_ccid(
					 application_short_name      => 'SQLGL',
					 key_flex_code               => 'GL#',
					 structure_number            => pin_chart_of_account_id,
					 validation_date             => TO_CHAR(SYSDATE,fnd_flex_ext.DATE_FORMAT),
					 concatenated_segments       => piv_concatenated_segment
					);
  	log_message(g_log,' pon_code_combination_id:'||pon_code_combination_id);
  EXCEPTION
  When others then
    log_message(g_log,'Exception in procedure get_code_combination_id. Error Message:'||SQLERRM);
    pov_error_message := 'Exception in procedure get_code_combination_id. Error Message:'||SQLERRM;
    pon_code_combination_id := null;
  END get_code_combination_id;  
   
  -- ========================
  -- Procedure: validate
  -- =============================================================================
  --   This procedure will validate the data.
  -- =============================================================================     
   
   PROCEDURE validate
   IS
   --for validation we pick records with process_flag as Y and with status as New and Error.
   cursor supplier_site
   is
   select * from xxconv.XXAP_SUPPLIER_SITE_RES_STG 
   where process_flag = g_process_flag
     and status_flag in (g_new,g_error)
     --Where the operation is COPY, only then we need to perform validation and import.
     and operation = 'COPY';
    
   --Maximum there are 30 segments and size of each is 25 each.
   lv_accts_concat_segment	varchar2(750);
   lv_new_accts_concat_segment	varchar2(750);
   
   lv_prepay_concat_segment	varchar2(750);
   lv_new_prepay_concat_segment	varchar2(750);

   ln_accts_code_comb_id		number;
   ln_prepay_code_comb_id		number;
   
   ln_accts_chart_of_account_id		number;
   ln_prepay_chart_of_account_id	number;
   
   lv_accts_error_message		varchar2(1000);
   lv_prepay_error_message		varchar2(1000);
   
   lv_segment1			gl_code_combinations.segment1%type;
   lv_segment2			gl_code_combinations.segment2%type;
   
   lv_record_status		varchar2(1);
   lv_error_message		varchar2(1000);
   
   ln_primary_site_count	number := 0;
   ln_tax_reporting_site_count	number := 0;
   ln_ece_tp_location_code_count	number := 0;
   ln_duplicate_site_count		number := 0;
   ln_pay_group_count			number := 0;
   ln_new_vendor_site_id		number := 0;
   
   ln_total_site_count			number := 0;
   ln_failed_site_count			number := 0;
   ln_total_bank_count			number := 0;
   ln_failed_bank_count			number := 0;
   
   
   cursor site_errors
   is
   select error_message,count(*) count_val
   from xxconv.XXAP_SUPPLIER_SITE_RES_STG 
  where request_id = g_request_id
   and status_flag in (g_error)
   group by error_message;
   
   cursor bank_errors
   is
   select error_message,count(*) count_val
   from xxconv.XXAP_SUPP_SITE_BANK_RES_STG 
  where request_id = g_request_id
   and status_flag in (g_error)
   group by error_message;
   
   
   BEGIN
   	log_message(g_log,'Validate Begin. Request Id generated is:'||g_request_id);
   	
   	for supplier_site_rec in supplier_site
   	loop
   		--Resetting the variables.
   		lv_record_status := 'S';
   		lv_error_message := null;
   		ln_primary_site_count := 0;
   		ln_tax_reporting_site_count := 0;
   		ln_ece_tp_location_code_count := 0;
   		ln_duplicate_site_count	 := 0;
   		ln_pay_group_count	 := 0;
   		ln_new_vendor_site_id	 := null;
   		
   		--Resetting accts_code_combination_id related variables.
   		--Commented as we are getting this values directly from the staging table.
   		/*lv_accts_concat_segment	:= null;
   		lv_new_accts_concat_segment := null;
   		ln_accts_chart_of_account_id  := null;*/
   		ln_accts_code_comb_id     := null;
   		lv_accts_error_message	  := null;
   		
   		--Resetting prepay_code_combination_id related variables.
   		
   		lv_prepay_concat_segment    := null;
   		lv_new_prepay_concat_segment := null;
   		ln_prepay_chart_of_account_id  := null;
   		ln_prepay_code_comb_id     := null;
   		lv_prepay_error_message	   := null;
   	
		log_message(g_log,'Site information:'||supplier_site_rec.record_id);

		if supplier_site_rec.new_vendor_site_code is null then
			lv_record_status := 'E';
			lv_error_message := 'Target vendor_site_code cannot be null';
		end if;
		
		--Deriving new accts_pay_code_combination_id
		--Commented as we are getting this values directly from the staging table.
		/*get_code_combination_segments(supplier_site_rec.accts_pay_code_combination_id,lv_accts_concat_segment,ln_accts_chart_of_account_id);
		log_message(g_log,'accts_pay_code_combination_id:'||supplier_site_rec.accts_pay_code_combination_id||' lv_accts_concat_segment:'
					||lv_accts_concat_segment||' ln_accts_chart_of_account_id:'||ln_accts_chart_of_account_id);

		lv_new_accts_concat_segment := supplier_site_rec.destination_org_number||'.'||supplier_site_rec.destination_plant_name
						||substr( lv_accts_concat_segment, instr(lv_accts_concat_segment,'.',1,2));
		*/
		if supplier_site_rec.target_accts_segment is null or supplier_site_rec.source_accts_chart_acct_id is null then
				lv_record_status := 'E';
				lv_error_message := lv_error_message||'~'||' target_accts_segment or source_accts_chart_acct_id is null';
		
		else
			get_code_combination_id(supplier_site_rec.target_accts_segment
						,supplier_site_rec.source_accts_chart_acct_id
						,ln_accts_code_comb_id
						,lv_accts_error_message
						);
			log_message(g_log,'New ln_accts_code_comb_id:'||ln_accts_code_comb_id);

			supplier_site_rec.new_accts_pay_code_comb_id := ln_accts_code_comb_id;

			if ln_accts_code_comb_id is null or ln_accts_code_comb_id = 0 then
				lv_record_status := 'E';
				lv_error_message := lv_error_message||'~'||
				'New Accts_Code_Combination_id does not exists or exception while creating new code combination id.'||
						    'Error Message:'||lv_accts_error_message;
			end if;
		end if;


		--Deriving new prepay_pay_code_combination_id
		--Commented as we are getting this values directly from the staging table.
		
		--Changed the derivation of prepay code combination. It will always be fixed number.
		get_code_combination_segments(supplier_site_rec.prepay_code_combination_id,lv_prepay_concat_segment,ln_prepay_chart_of_account_id);
		/*lv_new_prepay_concat_segment := to_char(supplier_site_rec.destination_org_number)||'.'||
						to_char(supplier_site_rec.destination_plant_name)
						||substr ( lv_prepay_concat_segment , instr(lv_prepay_concat_segment,'.',1,2));*/
						
		lv_new_prepay_concat_segment := to_char(supplier_site_rec.destination_org_number)||'.'||
						to_char(supplier_site_rec.destination_org_number)||'.'||
						FND_PROFILE.value('XXETN_PREPAY_COMBINATION');
		log_message(g_log,'prepay_code_combination_id:'||supplier_site_rec.prepay_code_combination_id||' lv_prepay_concat_segment:'
					||lv_prepay_concat_segment||' ln_prepay_chart_of_account_id:'||ln_prepay_chart_of_account_id||
					' lv_new_prepay_concat_segment:'||lv_new_prepay_concat_segment);
		
		get_code_combination_id(lv_new_prepay_concat_segment
						,ln_prepay_chart_of_account_id
						,ln_prepay_code_comb_id
						,lv_prepay_error_message
						);	
		log_message(g_log,'New ln_prepay_code_comb_id:'||ln_prepay_code_comb_id);

		supplier_site_rec.new_prepay_code_comb_id := ln_prepay_code_comb_id;

		if ln_prepay_code_comb_id is null or ln_prepay_code_comb_id = 0 then
			lv_record_status := 'E';
			lv_error_message := lv_error_message||'~'||'New Prepay_Code_Combination_id does not exists or exception while creating new code combination id.'||
					    'Error Message:'||lv_prepay_error_message;
		end if;						
		
		/*
		if supplier_site_rec.target_prepay_segment is null or supplier_site_rec.source_prepay_chart_acct_id is null then
				lv_record_status := 'E';
				lv_error_message := lv_error_message||'~'||' target_prepay_segment or source_prepay_chart_acct_id is null';
		else
			get_code_combination_id(supplier_site_rec.target_prepay_segment
						,supplier_site_rec.source_prepay_chart_acct_id
						,ln_prepay_code_comb_id
						,lv_prepay_error_message
						);
			log_message(g_log,'New ln_prepay_code_comb_id:'||ln_prepay_code_comb_id);

			supplier_site_rec.new_prepay_code_comb_id := ln_prepay_code_comb_id;

			if ln_prepay_code_comb_id is null or ln_prepay_code_comb_id = 0 then
				lv_record_status := 'E';
				lv_error_message := lv_error_message||'~'||'New Prepay_Code_Combination_id does not exists or exception while creating new code combination id.'||
						    'Error Message:'||lv_prepay_error_message;
			end if;
		end if;		
		*/
		
		--Validate primary_pay_site_flag.
		--If this flag is Y then check if there is any site for the vendor in target OU with primary_pay_site_flag value as Y
		--If there is site with value as Y then import the record as N.
		if supplier_site_rec.primary_pay_site_flag = 'Y' then
		 log_message(g_log,'primary_pay_site_flag validation. begin');
		 select count(*) 
		  into ln_primary_site_count
		  from ap_supplier_sites_all 
		 where vendor_id = supplier_site_rec.vendor_id
		   and org_id = supplier_site_rec.destination_org_id
		   and NVL(primary_pay_site_flag,'N') = 'Y'
		   and vendor_site_id <> supplier_site_rec.vendor_site_id
		   and ( inactive_date is null or inactive_date > sysdate );
		   
		   if ln_primary_site_count > 0 then
		   	supplier_site_rec.primary_pay_site_flag := 'N';
		   end if;
		   log_message(g_log,'primary_pay_site_flag validation. ln_primary_site_count:'||ln_primary_site_count);
		end if;
		
		--Validate TAX_REPORTING_SITE_FLAG.
		--If this flag is Y then check if there is any site for the vendor in target OU with TAX_REPORTING_SITE_FLAG value as Y
		--If there is site in target OU with value as Y then import the record as N.
		if supplier_site_rec.TAX_REPORTING_SITE_FLAG = 'Y' then
		log_message(g_log,'tax_reporting_site_flag validation. begin');
		       SELECT count(*)
		         INTO  ln_tax_reporting_site_count
		         FROM  ap_supplier_sites_all
		        WHERE nvl(tax_reporting_site_flag,'N') = 'Y'
		          AND   vendor_id = supplier_site_rec.vendor_id
		          AND   org_id = supplier_site_rec.destination_org_id
		          AND   nvl(vendor_site_id, -99) <> nvl(supplier_site_rec.vendor_site_id, -99);		
		   if ln_tax_reporting_site_count > 0 then
		   	supplier_site_rec.tax_reporting_site_flag := 'N';
		   end if;
		   log_message(g_log,'tax_reporting_site_flag validation. ln_tax_reporting_site_count:'||ln_tax_reporting_site_count);
		   
		end if;
		
		--Validate ECE_TP_LOCATION_CODE.
		--If ECE_TP_LOCATION_CODE is not null in source org , then we need to check if there exists some other site (determined by
		-- different vendor site code) in target org for same vendor with same ECE_TP_LOCATION_CODE , then make the value as null
		-- while importing.
		if supplier_site_rec.ECE_TP_LOCATION_CODE is not null then
		log_message(g_log,'ECE_TP_LOCATION_CODE validation. begin for vendor_site_code:'||supplier_site_rec.new_vendor_site_code);
		       SELECT count(*)
		         INTO  ln_ece_tp_location_code_count
		         FROM ap_supplier_sites_all SITE
			  WHERE  SITE.vendor_id = supplier_site_rec.vendor_id
			   AND   SITE.org_id = supplier_site_rec.destination_org_id
			   --new_vendor_site_code will get inserted into base table for target vendor site and thus 
			   --vaidation should be done with new_vendor_site_code.
			   AND   UPPER(SITE.vendor_site_code) <> UPPER(supplier_site_rec.new_vendor_site_code)
			   AND    UPPER(SITE.ece_tp_location_code) = UPPER(supplier_site_rec.ECE_TP_LOCATION_CODE) ;	
		   if ln_ece_tp_location_code_count > 0 then
		   	supplier_site_rec.ECE_TP_LOCATION_CODE := null;
		   end if;
		   log_message(g_log,'ECE_TP_LOCATION_CODE validation. ln_ece_tp_location_code_count:'||ln_ece_tp_location_code_count);
		   
		end if;	
		
		--Check if same vendor_site_code and vendor_id exists in target org. If same combination exists then error the record.
		select count(*)
		 into ln_duplicate_site_count
		 from ap_supplier_sites_all sups
		where sups.vendor_id = supplier_site_rec.vendor_id
		  AND sups.org_id = supplier_site_rec.destination_org_id
		  AND sups.vendor_site_code = supplier_site_rec.new_vendor_site_code;
		log_message(g_log,'ln_duplicate_site_count:'||ln_duplicate_site_count);
		
		if ln_duplicate_site_count > 0 then
			lv_record_status := 'E';
			lv_error_message := lv_error_message||'~'||'Vendor Site Code already exists in target org.';
			select vendor_site_id
			  into ln_new_vendor_site_id
			 from ap_supplier_sites_all sups
			where sups.vendor_id = supplier_site_rec.vendor_id
			  AND sups.org_id = supplier_site_rec.destination_org_id
			  AND sups.vendor_site_code = supplier_site_rec.new_vendor_site_code;
			log_message(g_log,'Since vendor site already exists in target org new vendor site id is:'||ln_new_vendor_site_id);
			  
		end if;
		
		--Paygroup validation
		if supplier_site_rec.target_paygroup is not null then
		 select count(*)
		   into ln_pay_group_count
		FROM fnd_lookup_values flv
	       WHERE flv.lookup_type = 'XXAP_PAY_GROUP_LOOKUP_CODE'
		 AND flv.meaning = supplier_site_rec.target_paygroup
		 AND flv.enabled_flag = 'Y'
		 AND SYSDATE BETWEEN NVL(flv.start_date_active, SYSDATE) AND
		     NVL(flv.end_date_active, SYSDATE + 1)
		 AND flv.language = USERENV('LANG');
		 
		 if ln_pay_group_count = 0 then
			lv_record_status := 'E';
			lv_error_message := lv_error_message||'~'||'Paygroup value is not valid.';
		 end if;

		
		end if;

		
		update xxconv.XXAP_SUPPLIER_SITE_RES_STG 
		   set status_flag =  DECODE(lv_record_status,'S',g_validated,g_error)
		      ,ERROR_MESSAGE = DECODE(lv_record_status,'S',null,lv_error_message)
		      ,last_update_date = sysdate
		      ,last_updated_by = g_user_id
		      ,last_update_login = g_login_id
		      ,request_id = g_request_id
		      --Additional columns to be updated as part of the validation.
		      ,primary_pay_site_flag = supplier_site_rec.primary_pay_site_flag
		      ,tax_reporting_site_flag = supplier_site_rec.tax_reporting_site_flag
		      ,ece_tp_location_code =  supplier_site_rec.ece_tp_location_code
		      ,new_accts_pay_code_comb_id = supplier_site_rec.new_accts_pay_code_comb_id
		      ,new_prepay_code_comb_id = supplier_site_rec.new_prepay_code_comb_id
		      ,new_vendor_site_id 	= ln_new_vendor_site_id
		 where record_id = supplier_site_rec.record_id;
		 
   				
   	end loop;
   	

   	--Update child records in the bank table to validated for which parent records in the site table is validated.
   	update xxconv.XXAP_SUPP_SITE_BANK_RES_STG bank_stg 
   	   set status_flag = g_validated
   	      ,ERROR_MESSAGE = null
	      ,last_update_date = sysdate
	      ,last_updated_by = g_user_id
	      ,last_update_login = g_login_id
	      ,request_id = g_request_id
   	where status_flag in (g_new,g_error)
   	  and exists(select 1 from xxconv.XXAP_SUPPLIER_SITE_RES_STG site_stg where site_stg.record_id = bank_stg.parent_record_id
   			and site_stg.status_flag = g_validated and request_id = g_request_id);
   	
   	
   	--Update child records in the bank table to error for which parent records in the site table is in error state.
   	update xxconv.XXAP_SUPP_SITE_BANK_RES_STG bank_stg 
   	   set status_flag = g_error
   	      ,error_message = 'Site did not validate successfully and thus bank was not validated.'
	      ,last_update_date = sysdate
	      ,last_updated_by = g_user_id
	      ,last_update_login = g_login_id
	      ,request_id = g_request_id
   	where status_flag in (g_new,g_error)
   	  and exists(select 1 from xxconv.XXAP_SUPPLIER_SITE_RES_STG site_stg where site_stg.record_id = bank_stg.parent_record_id
   			and site_stg.status_flag = g_error and request_id = g_request_id);   
   			
   	commit;
   	
   	select count(*)
   	  into ln_total_site_count
   	 from xxconv.XXAP_SUPPLIER_SITE_RES_STG 
   	 where request_id = g_request_id
   	   and status_flag in (g_validated,g_error);

   	select count(*)
   	  into ln_failed_site_count
   	 from xxconv.XXAP_SUPPLIER_SITE_RES_STG 
   	 where request_id = g_request_id
   	   and status_flag in (g_error);
   	   
   	select count(*)
   	  into ln_total_bank_count
   	 from xxconv.XXAP_SUPP_SITE_BANK_RES_STG 
   	 where request_id = g_request_id
   	   and status_flag in (g_validated,g_error);
   	   
   	select count(*)
   	  into ln_failed_bank_count
   	 from xxconv.XXAP_SUPP_SITE_BANK_RES_STG 
   	 where request_id = g_request_id
   	   and status_flag in (g_error);
   	   
   	   log_message(g_log,'ln_total_site_count:'||ln_total_site_count||' ln_failed_site_count:'||ln_failed_site_count||
   	   			' ln_total_bank_count:'||ln_total_bank_count||' ln_failed_bank_count:'||ln_failed_bank_count);
   	   			
   	log_message(g_out,'*******************************************************************');
   	log_message(g_out,'***************Printing Program Stats**********************');
	log_message(g_out,'');
	log_message(g_out,rpad('Total Site Records:',40,' ')||ln_total_site_count);
	log_message(g_out,rpad('Total Site Records Validated:',40,' ')||(ln_total_site_count-ln_failed_site_count));
	log_message(g_out,rpad('Total Site Records Failed:',40,' ')||ln_failed_site_count);
	log_message(g_out,rpad('Total Bank Records:',40,' ')||ln_total_bank_count);
	log_message(g_out,rpad('Total Bank Records Validated:',40,' ')||(ln_total_bank_count-ln_failed_bank_count));
	log_message(g_out,rpad('Total Bank Records Failed:',40,' ')||ln_failed_bank_count);
	log_message(g_out,'');
	
	log_message(g_out,'***************Distinct Error Logs:Site**********************');
	log_message(g_out,rpad('Count:',10,' ')||'Error Message');
	for site_errors_rec in site_errors
	loop
	log_message(g_out,rpad(site_errors_rec.count_val,10,' ')||site_errors_rec.error_message);
	end loop;
	log_message(g_out,'');
	log_message(g_out,'');
	
	log_message(g_out,'***************Distinct Error Logs:Bank**********************');
	log_message(g_out,rpad('Count:',10,' ')||'Error Message');
	for bank_errors_rec in bank_errors
	loop
	log_message(g_out,rpad(bank_errors_rec.count_val,10,' ')||bank_errors_rec.error_message);
	end loop;
	log_message(g_out,'');
	log_message(g_out,'');	
	log_message(g_out,'*******************************************************************');
   	   			
   	if ln_failed_site_count = 0 and ln_failed_bank_count = 0 then
   	 g_program_status := 0;
   	elsif ln_total_site_count = ln_failed_site_count and ln_total_bank_count = ln_failed_bank_count then
   	 g_program_status := 2;
   	else
   	 g_program_status := 1;
   	end if;
   	 
   	    
   	   
   exception
   when others then
   log_message(g_log,'Exception in procedure validate.Error Message:'||SQLERRM);
   rollback;
   
   END validate;   

   
  -- ========================
  -- Procedure: import
  -- =============================================================================
  --   This procedure will call create methods to import the data into new org.
  -- =============================================================================     
   
   PROCEDURE import
   IS
      cursor supplier_site
      is
      select * from xxconv.XXAP_SUPPLIER_SITE_RES_STG 
   where process_flag = g_process_flag
     and status_flag = g_validated
     --Where the operation is COPY, only then we need to perform validation and import.
     and operation = 'COPY';
     
   
	lr_vendor_site_rec 	g_vendor_site_rec%type;
	lr_contact_point_rec    g_contact_point_rec%type;
	lr_phone_rec 		g_phone_rec%type;
	lr_edi_rec_type 	g_edi_rec_type%type; 
	lr_email_rec_type 	g_email_rec_type%type; 
	lr_telex_rec_type 	g_telex_rec_type%type; 
  	lr_web_rec_type 	g_web_rec_type%type; 
  	lr_payee 		g_payee%type;
	lr_assignment_attribs   g_assignment_attribs%type;
	
	
	lv_site_error_flag		VARCHAR2(1);
	lv_site_error_msg		VARCHAR2(1000);
	
	lv_bank_error_flag		VARCHAR2(1);
	lv_bank_error_msg		VARCHAR2(1000);	
	
	
	
	ln_vendor_site_id    NUMBER;
	ln_party_site_id     NUMBER;
	ln_location_id   NUMBER;
	ln_bank_assignment_id	number;
	ln_contact_point_id	NUMBER;
	ln_supplier_site_count	number;
	
      /*cursor supplier_site_contact
      is
      select * from xxconv.XXAP_SUPP_SITE_CONTACT_RES_STG 
   where process_flag = g_process_flag
     and status_flag = g_validated;*/
   
      cursor supplier_site_bank
      is
      select bank_stg.record_id	
      	    ,bank_stg.parent_record_id	
      	    ,bank_stg.payee_party_id	
      	    ,site_stg.new_party_site_id 
      	    ,site_stg.new_vendor_site_id  
      	    ,site_stg.destination_org_id new_org_id	
      	    ,bank_stg.org_type	
      	    ,bank_stg.instrument_id	
      	    ,bank_stg.INSTRUMENT_PAYMENT_USE_ID
      	    ,bank_stg.start_date	
      	    ,bank_stg.end_date	
      from xxconv.XXAP_SUPP_SITE_BANK_RES_STG bank_stg
          ,xxconv.XXAP_SUPPLIER_SITE_RES_STG site_stg
   where site_stg.process_flag = g_process_flag
     and site_stg.status_flag = g_processed
     and site_stg.record_id = bank_stg.parent_record_id
     and bank_stg.status_flag = g_validated;
     
     
   ln_total_site_count			number := 0;
   ln_failed_site_count			number := 0;
   ln_total_bank_count			number := 0;
   ln_failed_bank_count			number := 0;
   
   cursor site_errors
   is
   select error_message,count(*) count_val
   from xxconv.XXAP_SUPPLIER_SITE_RES_STG 
  where request_id = g_request_id
   and status_flag in (g_error)
   group by error_message;
   
   cursor bank_errors
   is
   select error_message,count(*) count_val
   from xxconv.XXAP_SUPP_SITE_BANK_RES_STG 
  where request_id = g_request_id
   and status_flag in (g_error)
   group by error_message;   


   BEGIN
   	log_message(g_log,'Importing record Begin');
   	
   	
   	log_message(g_log,'Start of importing site information.');
   	for currec in supplier_site
   	loop
   	savepoint supplier_savepoint;
   	begin
   	--Resetting all the variables passed to the API's.
   	lr_vendor_site_rec := g_vendor_site_rec;
	lv_site_error_flag	:= 'S';
	lv_site_error_msg	:= NULL;
	ln_vendor_site_id	:= NULL;
	ln_party_site_id	:= NULL;
   	ln_location_id		:= NULL;
   	ln_supplier_site_count  := null;
   	
   	ln_total_site_count	:= ln_total_site_count + 1;
   	
   	log_message(g_log,'Site information: Record_id:'||currec.record_id||' vendor_site_id:'||currec.vendor_site_id);

	--If DEFAULT_PAY_SITE_ID is present in source record we are trying to convert, then we need to replace it with new vendor site id
	--present in target org.
	--There are 2 cases
	if currec.default_pay_site_id is not null then
 	  log_message(g_log,'DEFAULT_PAY_SITE_ID. Before validation.'||currec.DEFAULT_PAY_SITE_ID||
 	  		    ' vendor_id:'||currec.vendor_id||' vendor_site_id:'||currec.vendor_site_id);
	  begin
	  	if currec.default_pay_site_id = currec.vendor_site_id then
			lv_site_error_flag := 'E';
			lv_site_error_msg  := 'Default_site_id is same as vendor_site_id';		  		
	  	else
			select new_vendor_site_id
			  into currec.DEFAULT_PAY_SITE_ID
			 from xxconv.xxap_supplier_site_res_stg sups
			where sups.vendor_id = currec.vendor_id
			  and sups.vendor_site_id = currec.default_pay_site_id
			  and (status_flag = 'P' or (status_flag = 'E' and error_message like '%Vendor Site Code already exists in target org.%'));
	  	end if;
	  
	  exception
	  when no_data_found then
		lv_site_error_flag := 'E';
		lv_site_error_msg  := 'No DEFAULT_PAY_SITE_ID found in staging tables.';	
	  when others then
		lv_site_error_flag := 'E';
		lv_site_error_msg  := 'Exception while deriving DEFAULT_PAY_SITE_ID. Error Message:'||SQLERRM;
	  end;
	end if;
	
	log_message(g_log,'DEFAULT_PAY_SITE_ID. After validation.'||currec.DEFAULT_PAY_SITE_ID);

	IF lv_site_error_flag = 'S' THEN
		lr_vendor_site_rec.AREA_CODE   			   :=    currec.AREA_CODE;                 
		lr_vendor_site_rec.PHONE       			   :=    currec.PHONE;
		lr_vendor_site_rec.CUSTOMER_NUM                    :=    currec.CUSTOMER_NUM;
		lr_vendor_site_rec.SHIP_TO_LOCATION_ID             :=    currec.SHIP_TO_LOCATION_ID;
		lr_vendor_site_rec.BILL_TO_LOCATION_ID             :=    currec.BILL_TO_LOCATION_ID;
		lr_vendor_site_rec.SHIP_VIA_LOOKUP_CODE            :=    currec.SHIP_VIA_LOOKUP_CODE;
		lr_vendor_site_rec.FREIGHT_TERMS_LOOKUP_CODE       :=    currec.FREIGHT_TERMS_LOOKUP_CODE;
		lr_vendor_site_rec.FOB_LOOKUP_CODE                 :=    currec.FOB_LOOKUP_CODE;
		lr_vendor_site_rec.INACTIVE_DATE                   :=    currec.INACTIVE_DATE;
		lr_vendor_site_rec.FAX                             :=    currec.FAX;
		lr_vendor_site_rec.FAX_AREA_CODE                   :=    currec.FAX_AREA_CODE;
		lr_vendor_site_rec.TELEX                           :=    currec.TELEX;
		lr_vendor_site_rec.TERMS_DATE_BASIS                :=    currec.TERMS_DATE_BASIS;
		lr_vendor_site_rec.DISTRIBUTION_SET_ID             :=    currec.DISTRIBUTION_SET_ID;


		--ACCTS_PAY_CODE_COMBINATION_ID will change when copying data from source to target and thus assigning new ACCTS_PAY_CODE_COMBINATION_ID.
		lr_vendor_site_rec.ACCTS_PAY_CODE_COMBINATION_ID   :=    currec.new_accts_pay_code_comb_id;

		--PREPAY_CODE_COMBINATION_ID will change when copying data from source to target and thus assigning new PREPAY_CODE_COMBINATION_ID.
		lr_vendor_site_rec.PREPAY_CODE_COMBINATION_ID      :=    currec.new_prepay_code_comb_id;

		--lr_vendor_site_rec.PAY_GROUP_LOOKUP_CODE           :=    currec.PAY_GROUP_LOOKUP_CODE;
		lr_vendor_site_rec.PAY_GROUP_LOOKUP_CODE           :=    NVL(currec.target_paygroup,currec.PAY_GROUP_LOOKUP_CODE);
		lr_vendor_site_rec.PAYMENT_PRIORITY                :=    currec.PAYMENT_PRIORITY;
		lr_vendor_site_rec.TERMS_ID                        :=    currec.TERMS_ID;
		lr_vendor_site_rec.INVOICE_AMOUNT_LIMIT            :=    currec.INVOICE_AMOUNT_LIMIT;
		lr_vendor_site_rec.PAY_DATE_BASIS_LOOKUP_CODE      :=    currec.PAY_DATE_BASIS_LOOKUP_CODE;
		lr_vendor_site_rec.ALWAYS_TAKE_DISC_FLAG           :=    currec.ALWAYS_TAKE_DISC_FLAG;
		lr_vendor_site_rec.INVOICE_CURRENCY_CODE           :=    currec.INVOICE_CURRENCY_CODE;
		lr_vendor_site_rec.PAYMENT_CURRENCY_CODE           :=    currec.PAYMENT_CURRENCY_CODE;
		--lr_vendor_site_rec.VENDOR_SITE_ID                  :=    currec.VENDOR_SITE_ID;
		--lr_vendor_site_rec.LAST_UPDATE_DATE                :=    currec.LAST_UPDATE_DATE;
		--lr_vendor_site_rec.LAST_UPDATED_BY                 :=    currec.LAST_UPDATED_BY;
		lr_vendor_site_rec.VENDOR_ID                       :=    currec.VENDOR_ID;
		--lr_vendor_site_rec.VENDOR_SITE_CODE                :=    currec.VENDOR_SITE_CODE;
		lr_vendor_site_rec.VENDOR_SITE_CODE                :=    currec.new_VENDOR_SITE_CODE;

		lr_vendor_site_rec.VENDOR_SITE_CODE_ALT            :=    currec.VENDOR_SITE_CODE_ALT;

		--As per requirement PURCHASING_SITE_FLAG and PAY_SITE_FLAG should be hardcoded to Y.
		lr_vendor_site_rec.PURCHASING_SITE_FLAG            :=    currec.PURCHASING_SITE_FLAG;
		--lr_vendor_site_rec.PURCHASING_SITE_FLAG            :=    'Y';

		lr_vendor_site_rec.PAY_SITE_FLAG                   :=    currec.PAY_SITE_FLAG;
		--lr_vendor_site_rec.PAY_SITE_FLAG                   :=    'Y';

		lr_vendor_site_rec.RFQ_ONLY_SITE_FLAG              :=    currec.RFQ_ONLY_SITE_FLAG;
		lr_vendor_site_rec.ATTENTION_AR_FLAG               :=    currec.ATTENTION_AR_FLAG;
		lr_vendor_site_rec.HOLD_ALL_PAYMENTS_FLAG          :=    currec.HOLD_ALL_PAYMENTS_FLAG;
		lr_vendor_site_rec.HOLD_FUTURE_PAYMENTS_FLAG       :=    currec.HOLD_FUTURE_PAYMENTS_FLAG;
		lr_vendor_site_rec.HOLD_REASON                     :=    currec.HOLD_REASON;
		lr_vendor_site_rec.HOLD_UNMATCHED_INVOICES_FLAG    :=    currec.HOLD_UNMATCHED_INVOICES_FLAG;
		lr_vendor_site_rec.TAX_REPORTING_SITE_FLAG         :=    currec.TAX_REPORTING_SITE_FLAG;
		lr_vendor_site_rec.ATTRIBUTE_CATEGORY              :=    currec.ATTRIBUTE_CATEGORY;
		lr_vendor_site_rec.ATTRIBUTE1                      :=    currec.ATTRIBUTE1;
		lr_vendor_site_rec.ATTRIBUTE2                      :=    currec.ATTRIBUTE2;
		lr_vendor_site_rec.ATTRIBUTE3                      :=    currec.ATTRIBUTE3;
		lr_vendor_site_rec.ATTRIBUTE4                      :=    currec.ATTRIBUTE4;
		lr_vendor_site_rec.ATTRIBUTE5                      :=    currec.ATTRIBUTE5;
		lr_vendor_site_rec.ATTRIBUTE6                      :=    currec.ATTRIBUTE6;
		lr_vendor_site_rec.ATTRIBUTE7                      :=    currec.ATTRIBUTE7;
		lr_vendor_site_rec.ATTRIBUTE8                      :=    currec.ATTRIBUTE8;
		lr_vendor_site_rec.ATTRIBUTE9                      :=    currec.ATTRIBUTE9;

		--lr_vendor_site_rec.ATTRIBUTE10                      :=    currec.ATTRIBUTE10;
		lr_vendor_site_rec.ATTRIBUTE10                      :=    currec.target_attribute10;

		lr_vendor_site_rec.ATTRIBUTE11                      :=    currec.ATTRIBUTE11;
		lr_vendor_site_rec.ATTRIBUTE12                      :=    currec.ATTRIBUTE12;
		lr_vendor_site_rec.ATTRIBUTE13                      :=    currec.ATTRIBUTE13;
		lr_vendor_site_rec.ATTRIBUTE14                      :=    currec.ATTRIBUTE14;

		--lr_vendor_site_rec.ATTRIBUTE15                      :=    currec.ATTRIBUTE15;
		lr_vendor_site_rec.ATTRIBUTE15                      :=    currec.target_attribute15;

		lr_vendor_site_rec.VALIDATION_NUMBER                :=currec.VALIDATION_NUMBER;
		lr_vendor_site_rec.EXCLUDE_FREIGHT_FROM_DISCOUNT   :=currec.EXCLUDE_FREIGHT_FROM_DISCOUNT;  
		lr_vendor_site_rec.BANK_CHARGE_BEARER              :=currec.BANK_CHARGE_BEARER; 
		lr_vendor_site_rec.ORG_ID                          :=currec.DESTINATION_ORG_ID;          
		lr_vendor_site_rec.CHECK_DIGITS                    :=currec.CHECK_DIGITS;                    
		lr_vendor_site_rec.ALLOW_AWT_FLAG                  :=currec.ALLOW_AWT_FLAG;                  
		lr_vendor_site_rec.AWT_GROUP_ID                    :=currec.AWT_GROUP_ID;                    
		lr_vendor_site_rec.PAY_AWT_GROUP_ID                :=currec.PAY_AWT_GROUP_ID;                
		lr_vendor_site_rec.DEFAULT_PAY_SITE_ID             :=currec.DEFAULT_PAY_SITE_ID;             
		lr_vendor_site_rec.PAY_ON_CODE                     :=currec.PAY_ON_CODE;                     
		lr_vendor_site_rec.PAY_ON_RECEIPT_SUMMARY_CODE     :=currec.PAY_ON_RECEIPT_SUMMARY_CODE;     
		lr_vendor_site_rec.GLOBAL_ATTRIBUTE_CATEGORY       :=currec.GLOBAL_ATTRIBUTE_CATEGORY;       
		lr_vendor_site_rec.GLOBAL_ATTRIBUTE1               :=currec.GLOBAL_ATTRIBUTE1;               
		lr_vendor_site_rec.GLOBAL_ATTRIBUTE2               :=currec.GLOBAL_ATTRIBUTE2;
		lr_vendor_site_rec.GLOBAL_ATTRIBUTE3               :=currec.GLOBAL_ATTRIBUTE3;
		lr_vendor_site_rec.GLOBAL_ATTRIBUTE4               :=currec.GLOBAL_ATTRIBUTE4;
		lr_vendor_site_rec.GLOBAL_ATTRIBUTE5               :=currec.GLOBAL_ATTRIBUTE5;
		lr_vendor_site_rec.GLOBAL_ATTRIBUTE6               :=currec.GLOBAL_ATTRIBUTE6;
		lr_vendor_site_rec.GLOBAL_ATTRIBUTE7               :=currec.GLOBAL_ATTRIBUTE7;
		lr_vendor_site_rec.GLOBAL_ATTRIBUTE8               :=currec.GLOBAL_ATTRIBUTE8;
		lr_vendor_site_rec.GLOBAL_ATTRIBUTE9               :=currec.GLOBAL_ATTRIBUTE9;
		lr_vendor_site_rec.GLOBAL_ATTRIBUTE10               :=currec.GLOBAL_ATTRIBUTE10;
		lr_vendor_site_rec.GLOBAL_ATTRIBUTE11               :=currec.GLOBAL_ATTRIBUTE11;
		lr_vendor_site_rec.GLOBAL_ATTRIBUTE12               :=currec.GLOBAL_ATTRIBUTE12;
		lr_vendor_site_rec.GLOBAL_ATTRIBUTE13               :=currec.GLOBAL_ATTRIBUTE13;
		lr_vendor_site_rec.GLOBAL_ATTRIBUTE14               :=currec.GLOBAL_ATTRIBUTE14;
		lr_vendor_site_rec.GLOBAL_ATTRIBUTE15               :=currec.GLOBAL_ATTRIBUTE15;
		lr_vendor_site_rec.GLOBAL_ATTRIBUTE16               :=currec.GLOBAL_ATTRIBUTE16;
		lr_vendor_site_rec.GLOBAL_ATTRIBUTE17               :=currec.GLOBAL_ATTRIBUTE17;
		lr_vendor_site_rec.GLOBAL_ATTRIBUTE18               :=currec.GLOBAL_ATTRIBUTE18;
		lr_vendor_site_rec.GLOBAL_ATTRIBUTE19               :=currec.GLOBAL_ATTRIBUTE19;
		lr_vendor_site_rec.GLOBAL_ATTRIBUTE20               :=currec.GLOBAL_ATTRIBUTE20;
		lr_vendor_site_rec.TP_HEADER_ID                    :=	currec.TP_HEADER_ID;                    
		lr_vendor_site_rec.ECE_TP_LOCATION_CODE            :=	currec.ECE_TP_LOCATION_CODE;            
		lr_vendor_site_rec.PCARD_SITE_FLAG                 :=	currec.PCARD_SITE_FLAG;                 
		lr_vendor_site_rec.MATCH_OPTION                    :=	currec.MATCH_OPTION;                    
		lr_vendor_site_rec.COUNTRY_OF_ORIGIN_CODE          :=	currec.COUNTRY_OF_ORIGIN_CODE;          
		lr_vendor_site_rec.FUTURE_DATED_PAYMENT_CCID       :=	currec.FUTURE_DATED_PAYMENT_CCID;       
		lr_vendor_site_rec.CREATE_DEBIT_MEMO_FLAG          :=	currec.CREATE_DEBIT_MEMO_FLAG;          
		lr_vendor_site_rec.SUPPLIER_NOTIF_METHOD           :=	currec.SUPPLIER_NOTIF_METHOD;           
		lr_vendor_site_rec.EMAIL_ADDRESS                   :=	currec.EMAIL_ADDRESS;                   
		lr_vendor_site_rec.PRIMARY_PAY_SITE_FLAG           :=	currec.PRIMARY_PAY_SITE_FLAG;           
		lr_vendor_site_rec.SHIPPING_CONTROL                :=	currec.SHIPPING_CONTROL;                
		lr_vendor_site_rec.SELLING_COMPANY_IDENTIFIER      :=	currec.SELLING_COMPANY_IDENTIFIER;      
		lr_vendor_site_rec.GAPLESS_INV_NUM_FLAG            :=	currec.GAPLESS_INV_NUM_FLAG;            
		lr_vendor_site_rec.LOCATION_ID                     :=	currec.LOCATION_ID;                     
		lr_vendor_site_rec.PARTY_SITE_ID                   :=	currec.PARTY_SITE_ID;                   
		--lr_vendor_site_rec.ORG_NAME                        :=	currec.ORG_NAME;
		lr_vendor_site_rec.DUNS_NUMBER			   :=	currec.DUNS_NUMBER;                                        
		lr_vendor_site_rec.ADDRESS_STYLE		   :=	currec.ADDRESS_STYLE;                                      
		lr_vendor_site_rec.LANGUAGE     			:=	currec.LANGUAGE;                                      
		lr_vendor_site_rec.PROVINCE     			:=	currec.PROVINCE;                                      
		lr_vendor_site_rec.COUNTRY      			:=	currec.COUNTRY;                                      
		lr_vendor_site_rec.ADDRESS_LINE1			:=	currec.ADDRESS_LINE1;                                      
		lr_vendor_site_rec.ADDRESS_LINE2			:=	currec.ADDRESS_LINE2;                                      
		lr_vendor_site_rec.ADDRESS_LINE3			:=	currec.ADDRESS_LINE3;                                      
		lr_vendor_site_rec.ADDRESS_LINE4			:=	currec.ADDRESS_LINE4;                                      
		lr_vendor_site_rec.ADDRESS_LINES_ALT			:=	currec.ADDRESS_LINES_ALT;                                  
		lr_vendor_site_rec.COUNTY           			:=	currec.COUNTY;                                  
		lr_vendor_site_rec.CITY             			:=	currec.CITY;                                  
		lr_vendor_site_rec.STATE            			:=	currec.STATE;                                  
		lr_vendor_site_rec.ZIP              			:=	currec.ZIP;  
		--lr_vendor_site_rec.TERMS_NAME                      	:=	currec.TERMS_NAME;  
		--lr_vendor_site_rec.DEFAULT_TERMS_ID                	:=	currec.DEFAULT_TERMS_ID;  
		--lr_vendor_site_rec.AWT_GROUP_NAME                  	:=	currec.AWT_GROUP_NAME;  
		--lr_vendor_site_rec.PAY_AWT_GROUP_NAME              	:=	currec.PAY_AWT_GROUP_NAME;  
		--lr_vendor_site_rec.DISTRIBUTION_SET_NAME           	:=	currec.DISTRIBUTION_SET_NAME;  
		--lr_vendor_site_rec.SHIP_TO_LOCATION_CODE           	:=	currec.SHIP_TO_LOCATION_CODE;  
		--lr_vendor_site_rec.BILL_TO_LOCATION_CODE           	:=	currec.BILL_TO_LOCATION_CODE;  
		--lr_vendor_site_rec.DEFAULT_DIST_SET_ID             	:=	currec.DEFAULT_DIST_SET_ID;  
		--lr_vendor_site_rec.DEFAULT_SHIP_TO_LOC_ID          	:=	currec.DEFAULT_SHIP_TO_LOC_ID;  
		--lr_vendor_site_rec.DEFAULT_BILL_TO_LOC_ID          	:=	currec.DEFAULT_BILL_TO_LOC_ID;  
		lr_vendor_site_rec.TOLERANCE_ID                    	:=	currec.TOLERANCE_ID;  
		--lr_vendor_site_rec.TOLERANCE_NAME                  	:=	currec.TOLERANCE_NAME;  
		--lr_vendor_site_rec.VENDOR_INTERFACE_ID             	:=	currec.VENDOR_INTERFACE_ID;  
		--lr_vendor_site_rec.VENDOR_SITE_INTERFACE_ID    		:=	currec.VENDOR_SITE_INTERFACE_ID;

		--Assigning columns that belong to iby_external_payees_all Begin
		   lr_vendor_site_rec.EXT_PAYEE_REC.Payee_Party_Id        :=	currec.Payee_Party_Id;  
		   lr_vendor_site_rec.EXT_PAYEE_REC.Payment_Function      :=	currec.Payment_Function;  
		   lr_vendor_site_rec.EXT_PAYEE_REC.Exclusive_Pay_Flag    :=	currec.PAYER_EXCLUSIVE_PAYMENT_FLAG;  
		   lr_vendor_site_rec.EXT_PAYEE_REC.Default_Pmt_method    :=	currec.Default_Pmt_method;  
		   lr_vendor_site_rec.EXT_PAYEE_REC.ECE_TP_Loc_Code       :=	currec.PAYER_ECE_TP_LOCATION_CODE;  
		   lr_vendor_site_rec.EXT_PAYEE_REC.Bank_Charge_Bearer    :=	currec.PAYER_BANK_CHARGE_BEARER;  
		   lr_vendor_site_rec.EXT_PAYEE_REC.Bank_Instr1_Code      :=	currec.Bank_Instr1_Code;  
		   lr_vendor_site_rec.EXT_PAYEE_REC.Bank_Instr2_Code      :=	currec.Bank_Instr2_Code;  
		   lr_vendor_site_rec.EXT_PAYEE_REC.Bank_Instr_Detail     :=	currec.Bank_Instr_Detail;  
		   lr_vendor_site_rec.EXT_PAYEE_REC.Pay_Reason_Code       :=	currec.Pay_Reason_Code;  
		   lr_vendor_site_rec.EXT_PAYEE_REC.Pay_Reason_Com        :=	currec.Pay_Reason_Com;  
		   lr_vendor_site_rec.EXT_PAYEE_REC.Inactive_Date         :=	currec.Inactive_Date;  
		   lr_vendor_site_rec.EXT_PAYEE_REC.Pay_Message1          :=	currec.Pay_Message1;  
		   lr_vendor_site_rec.EXT_PAYEE_REC.Pay_Message2          :=	currec.Pay_Message2;  
		   lr_vendor_site_rec.EXT_PAYEE_REC.Pay_Message3          :=	currec.Pay_Message3;  
		   lr_vendor_site_rec.EXT_PAYEE_REC.Delivery_Channel      :=	currec.Delivery_Channel;  
		   lr_vendor_site_rec.EXT_PAYEE_REC.Pmt_Format            :=	currec.Pmt_Format;  
		   lr_vendor_site_rec.EXT_PAYEE_REC.Settlement_Priority   :=	currec.Settlement_Priority;  
		   lr_vendor_site_rec.EXT_PAYEE_REC.Remit_advice_delivery_method	:=	currec.Remit_advice_delivery_method;  
		   lr_vendor_site_rec.EXT_PAYEE_REC.Remit_advice_email   :=	currec.Remit_advice_email;  
		   lr_vendor_site_rec.EXT_PAYEE_REC.remit_advice_fax     :=	currec.remit_advice_fax;  

			--Assigning columns that belong to iby_external_payees_all End

			lr_vendor_site_rec.RETAINAGE_RATE                  	:=	currec.RETAINAGE_RATE;  
			lr_vendor_site_rec.SERVICES_TOLERANCE_ID           	:=	currec.SERVICES_TOLERANCE_ID;  
			--lr_vendor_site_rec.SERVICES_TOLERANCE_NAME         	:=	currec.SERVICES_TOLERANCE_NAME;  
			--lr_vendor_site_rec.SHIPPING_LOCATION_ID            	:=	currec.SHIPPING_LOCATION_ID;  
			lr_vendor_site_rec.VAT_CODE                        	:=	currec.VAT_CODE;  
			lr_vendor_site_rec.VAT_REGISTRATION_NUM            	:=	currec.VAT_REGISTRATION_NUM;  
			lr_vendor_site_rec.REMITTANCE_EMAIL                	:=	currec.REMITTANCE_EMAIL;  
			lr_vendor_site_rec.EDI_ID_NUMBER                   	:=	currec.EDI_ID_NUMBER;  
			lr_vendor_site_rec.EDI_PAYMENT_FORMAT              	:=	currec.EDI_PAYMENT_FORMAT;  
			lr_vendor_site_rec.EDI_TRANSACTION_HANDLING        	:=	currec.EDI_TRANSACTION_HANDLING;  
			lr_vendor_site_rec.EDI_PAYMENT_METHOD              	:=	currec.EDI_PAYMENT_METHOD;  
			lr_vendor_site_rec.EDI_REMITTANCE_METHOD           	:=	currec.EDI_REMITTANCE_METHOD;  
			lr_vendor_site_rec.EDI_REMITTANCE_INSTRUCTION      	:=	currec.EDI_REMITTANCE_INSTRUCTION;  
			--lr_vendor_site_rec.PARTY_SITE_NAME                 	:=	currec.PARTY_SITE_NAME;  
			lr_vendor_site_rec.OFFSET_TAX_FLAG                 	:=	currec.OFFSET_TAX_FLAG;  
			lr_vendor_site_rec.AUTO_TAX_CALC_FLAG              	:=	currec.AUTO_TAX_CALC_FLAG;  
			--lr_vendor_site_rec.REMIT_ADVICE_DELIVERY_METHOD    	:=	currec.REMIT_ADVICE_DELIVERY_METHOD;  
			--lr_vendor_site_rec.REMIT_ADVICE_FAX                	:=	currec.REMIT_ADVICE_FAX;  
			lr_vendor_site_rec.CAGE_CODE                       	:=	currec.CAGE_CODE;  
			lr_vendor_site_rec.LEGAL_BUSINESS_NAME             	:=	currec.LEGAL_BUSINESS_NAME;  
			lr_vendor_site_rec.DOING_BUS_AS_NAME               	:=	currec.DOING_BUS_AS_NAME;  
			lr_vendor_site_rec.DIVISION_NAME                   	:=	currec.DIVISION_NAME;  
			lr_vendor_site_rec.SMALL_BUSINESS_CODE             	:=	currec.SMALL_BUSINESS_CODE;  
			lr_vendor_site_rec.CCR_COMMENTS                    	:=	currec.CCR_COMMENTS;  
			lr_vendor_site_rec.DEBARMENT_START_DATE            	:=	currec.DEBARMENT_START_DATE;  
			lr_vendor_site_rec.DEBARMENT_END_DATE              	:=	currec.DEBARMENT_END_DATE;  
			lr_vendor_site_rec.AP_TAX_ROUNDING_RULE            	:=	currec.AP_TAX_ROUNDING_RULE;  
			lr_vendor_site_rec.AMOUNT_INCLUDES_TAX_FLAG        	:=	currec.AMOUNT_INCLUDES_TAX_FLAG;  
			lr_vendor_site_rec.ACK_LEAD_TIME                   	:=	currec.ACK_LEAD_TIME;  
			--lr_vendor_site_rec.AP_TAX_ROUNDING_LEVEL_CODE     	:=	currec.AP_TAX_ROUNDING_LEVEL_CODE;  

			create_supplier_site(lr_vendor_site_rec
						,lv_site_error_flag
						,lv_site_error_msg
						,ln_vendor_site_id
						,ln_party_site_id
						,ln_location_id
						);
			
		END IF;
	exception
	when others then
		lv_site_error_flag := 'E';
		lv_site_error_msg  := 'Exception while creating site information. Error Message.'||SQLERRM;	
	end;

	log_message(g_log,'Status of Supplier Site Creation: lv_site_error_flag:'||lv_site_error_flag||' lv_site_error_msg:'||lv_site_error_msg);
	log_message(g_log,'Site Creation output values: ln_vendor_site_id:'||ln_vendor_site_id||' ln_party_site_id:'||
		    ln_party_site_id||' ln_location_id:'||ln_location_id);
	
	--If there is no error while creating site then call common attachment API created for restructuring to convert the attachments.
	IF lv_site_error_flag = 'S' THEN
		log_message(g_log,'Calling API for creating attachment. '||
				  ' Source vendor_site_id:'||lr_vendor_site_rec.vendor_site_id||
				  ' Target vendor_site_id:'||ln_vendor_site_id);
							
		xxfnd_cmn_res_attach_pkg.migrate_attachments('PO_VENDOR_SITES'      
							     , currec.vendor_site_id--old vendor_site_id
							     ,NULL
							     ,ln_vendor_site_id --new vendor_site_id
							     ,NULL 
							     ,lv_site_error_flag
							     ,lv_site_error_msg
							     );
		log_message(g_log,'Returned values from Attachment API lv_site_error_flag:'||lv_site_error_flag||
				  ' lv_site_error_msg:'||lv_site_error_msg);
	
	END IF;
	
	if lv_site_error_flag <> 'S' or lv_site_error_flag is null then 
	  rollback to supplier_savepoint;
	  ln_failed_site_count := ln_failed_site_count + 1;
	else
	  --rollback to supplier_savepoint;--temp
	  commit;
	end if;
	
	update xxconv.XXAP_SUPPLIER_SITE_RES_STG
   		   set NEW_VENDOR_SITE_ID = ln_vendor_site_id
   		      ,NEW_PARTY_SITE_ID = ln_party_site_id
   		      ,new_org_id = DESTINATION_ORG_ID
   		      ,status_flag = DECODE(lv_site_error_flag,'S',g_processed,g_error)
   		      ,ERROR_MESSAGE = DECODE(lv_site_error_flag,'S',NULL,lv_site_error_msg)
		      ,last_update_date = sysdate
		      ,last_updated_by = g_user_id
		      ,last_update_login = g_login_id
		      ,request_id = g_request_id
   		 where record_id = currec.record_id;
   	commit; 
   	--END IF;
   	
   	end loop; 
   	
	log_message(g_log,'Import of site has been completed.');
	
	log_message(g_log,'Start of bank account information importing.');
	
	for currec_supplier_site_bank in supplier_site_bank
	loop
	  SAVEPOINT supplier_bank_savepoint;
	  begin
		log_message(g_log,'Bank information: Record_id:'||currec_supplier_site_bank.RECORD_ID||
			    ' payee_party_id:'||currec_supplier_site_bank.payee_party_id||
			    ' new_org_id:'||currec_supplier_site_bank.new_org_id||
			    ' new_vendor_site_id:'||currec_supplier_site_bank.new_vendor_site_id||
			    ' new_party_site_id:'||currec_supplier_site_bank.new_party_site_id||
			    ' INSTRUMENT_ID:'||currec_supplier_site_bank.INSTRUMENT_ID);
		--Resetting variables.
		lr_payee := g_payee;
		lr_assignment_attribs := g_assignment_attribs;
		lv_bank_error_flag := 'S';
		lv_bank_error_msg := NULL;
		ln_bank_assignment_id := null;

		ln_total_bank_count := ln_total_bank_count + 1;
		
		lr_payee.Payment_Function  := 'PAYABLES_DISB';	
		lr_payee.Party_Id	   := currec_supplier_site_bank.payee_party_id;
		lr_payee.Org_Type	   := 'OPERATING_UNIT';
		lr_payee.Org_Id		   := currec_supplier_site_bank.new_org_id;

		lr_payee.Party_Site_id	      := currec_supplier_site_bank.new_Party_Site_id;
		lr_payee.Supplier_Site_id      := currec_supplier_site_bank.new_vendor_Site_id;

		lr_assignment_attribs.Instrument.Instrument_Type   := 'BANKACCOUNT';
		lr_assignment_attribs.Instrument.Instrument_Id	:= currec_supplier_site_bank.INSTRUMENT_ID;
		
		lr_assignment_attribs.Priority               := 1;
		lr_assignment_attribs.Start_Date             := currec_supplier_site_bank.start_date;
		lr_assignment_attribs.End_Date               := currec_supplier_site_bank.end_date;

		create_site_bank_account(lr_payee
					 ,lr_assignment_attribs
					 ,lv_bank_error_flag
					 ,lv_bank_error_msg
					 ,ln_bank_assignment_id	
					 );
		exception
		when others then
			lv_bank_error_flag := 'E';
			lv_bank_error_msg  := 'Exception while creating bank account information. Error Message.'||SQLERRM;
			log_message(g_log,'Exception while creating bank account information. '||
				    ' Record Id:'||currec_supplier_site_bank.record_id||
				    ' Error Message:'||SQLERRM);
		end;


		log_message(g_log,'Status of Supplier Bank Creation: lv_bank_error_flag:'||lv_bank_error_flag||
			    ' lv_bank_error_msg:'||lv_bank_error_msg||' ln_bank_assignment_id:'||ln_bank_assignment_id);

		--if there is exception while creating bank record then rollback till savepoint created.

		if lv_bank_error_flag <> 'S' or lv_bank_error_flag is null then 
		  rollback to supplier_bank_savepoint;
		  ln_failed_bank_count	:= ln_failed_bank_count + 1;
		else
		  --rollback to supplier_bank_savepoint;--temp
		  commit;
		end if;


		update xxconv.XXAP_SUPP_SITE_BANK_RES_STG
		   set NEW_supplier_SITE_ID = currec_supplier_site_bank.new_vendor_Site_id
		      ,NEW_PARTY_SITE_ID = currec_supplier_site_bank.new_Party_Site_id
		      ,new_org_id = currec_supplier_site_bank.new_org_id
		      ,NEW_INSTRUMENT_PAYMENT_USE_ID = ln_bank_assignment_id
		      ,status_flag = DECODE(lv_bank_error_flag,'S',g_processed,g_error)
		      ,ERROR_MESSAGE = DECODE(lv_bank_error_flag,'S',NULL,lv_bank_error_msg)
		      ,last_update_date = sysdate
		      ,last_updated_by = g_user_id
		      ,last_update_login = g_login_id
		      ,request_id = g_request_id
		where record_id = currec_supplier_site_bank.record_id;
		commit;

	end loop;  
	log_message(g_log,'Bank account information importing completed.');
	
	
   	
   	/*
   	for currec_supplier_site_contact in supplier_site_contact
   	loop
   		
		lr_contact_point_rec.contact_point_id	:= currec_supplier_site_contact.contact_point_id;                     
		lr_contact_point_rec.contact_point_type           := currec_supplier_site_contact.contact_point_type;
		lr_contact_point_rec.status                       := currec_supplier_site_contact.status;
		lr_contact_point_rec.owner_table_name                      := currec_supplier_site_contact.owner_table_name;
		lr_contact_point_rec.owner_table_id                        := currec_supplier_site_contact.owner_table_id;
		lr_contact_point_rec.primary_flag                          := currec_supplier_site_contact.primary_flag;
		lr_contact_point_rec.orig_system_reference                 := currec_supplier_site_contact.orig_system_reference;
		lr_contact_point_rec.content_source_type                   := currec_supplier_site_contact.content_source_type;
		lr_contact_point_rec.attribute_category                    := currec_supplier_site_contact.attribute_category;
		lr_contact_point_rec.attribute1                            := currec_supplier_site_contact.attribute1;
		lr_contact_point_rec.attribute2                            := currec_supplier_site_contact.attribute2;
		lr_contact_point_rec.attribute3                            := currec_supplier_site_contact.attribute3;
		lr_contact_point_rec.attribute4                            := currec_supplier_site_contact.attribute4;
		lr_contact_point_rec.attribute5                            := currec_supplier_site_contact.attribute5;
		lr_contact_point_rec.attribute6                            := currec_supplier_site_contact.attribute6;
		lr_contact_point_rec.attribute7                            := currec_supplier_site_contact.attribute7;
		lr_contact_point_rec.attribute8                            := currec_supplier_site_contact.attribute8;
		lr_contact_point_rec.attribute9                            := currec_supplier_site_contact.attribute9;
		lr_contact_point_rec.attribute10                            := currec_supplier_site_contact.attribute10;
		lr_contact_point_rec.attribute11                            := currec_supplier_site_contact.attribute11;
		lr_contact_point_rec.attribute12                            := currec_supplier_site_contact.attribute12;
		lr_contact_point_rec.attribute13                            := currec_supplier_site_contact.attribute13;
		lr_contact_point_rec.attribute14                            := currec_supplier_site_contact.attribute14;
		lr_contact_point_rec.attribute15                            := currec_supplier_site_contact.attribute15;
		lr_contact_point_rec.attribute16                            := currec_supplier_site_contact.attribute16;
		lr_contact_point_rec.attribute17                            := currec_supplier_site_contact.attribute17;
		lr_contact_point_rec.attribute18                            := currec_supplier_site_contact.attribute18;
		lr_contact_point_rec.attribute19                            := currec_supplier_site_contact.attribute19;
		lr_contact_point_rec.attribute20                            := currec_supplier_site_contact.attribute20;  
		lr_contact_point_rec.contact_point_purpose                 := currec_supplier_site_contact.contact_point_purpose;
		lr_contact_point_rec.primary_by_purpose                    := currec_supplier_site_contact.primary_by_purpose;
		lr_contact_point_rec.created_by_module                     := currec_supplier_site_contact.created_by_module;
		lr_contact_point_rec.application_id                        := currec_supplier_site_contact.application_id;
		lr_contact_point_rec.actual_content_source                 := currec_supplier_site_contact.actual_content_source;
		lr_edi_rec_type.edi_transaction_handling                   :=currec_supplier_site_contact.edi_transaction_handling;
		lr_edi_rec_type.edi_id_number   			   :=currec_supplier_site_contact.edi_id_number;                        
		lr_edi_rec_type.edi_payment_method                      :=currec_supplier_site_contact.edi_payment_method;
		lr_edi_rec_type.edi_payment_format                      :=currec_supplier_site_contact.edi_payment_format;
		lr_edi_rec_type.edi_remittance_method                   :=currec_supplier_site_contact.edi_remittance_method;
		lr_edi_rec_type.edi_remittance_instruction              :=currec_supplier_site_contact.edi_remittance_instruction;
		lr_edi_rec_type.edi_tp_header_id                        :=currec_supplier_site_contact.edi_tp_header_id;
    		lr_edi_rec_type.edi_ece_tp_location_code                :=currec_supplier_site_contact.edi_ece_tp_location_code;
    		lr_email_rec_type.email_format                            :=currec_supplier_site_contact.email_format;
    		lr_email_rec_type.email_address                           :=currec_supplier_site_contact.email_address;
		lr_phone_rec.phone_calling_calendar                  :=currec_supplier_site_contact.phone_calling_calendar;
		lr_phone_rec.last_contact_dt_time                    :=currec_supplier_site_contact.last_contact_dt_time;
		lr_phone_rec.timezone_id                             :=currec_supplier_site_contact.timezone_id;
		lr_phone_rec.phone_area_code                         :=currec_supplier_site_contact.phone_area_code;
		lr_phone_rec.phone_country_code                      :=currec_supplier_site_contact.phone_country_code;
		lr_phone_rec.phone_number                            :=currec_supplier_site_contact.phone_number;
		lr_phone_rec.phone_extension                         :=currec_supplier_site_contact.phone_extension;
		lr_phone_rec.phone_line_type                         :=currec_supplier_site_contact.phone_line_type;
		lr_phone_rec.raw_phone_number                        :=currec_supplier_site_contact.raw_phone_number;
		lr_telex_rec_type.telex_number                       :=currec_supplier_site_contact.telex_number;
		lr_web_rec_type.web_type                             :=currec_supplier_site_contact.web_type;
    		lr_web_rec_type.url                                  :=currec_supplier_site_contact.url;
		


					 
		create_supplier_site_contact(lr_contact_point_rec
					    ,lr_edi_rec_type
					    ,lr_email_rec_type
		  			    ,lr_phone_rec
		  			    ,lr_telex_rec_type
		  			    ,lr_web_rec_type
		  			    ,ln_contact_point_id
					    ,lv_error_flag
					    ,lv_error_msg
					);
   	end loop;*/

   	log_message(g_out,'*******************************************************************');
   	log_message(g_out,'***************Printing Program Stats**********************');
	log_message(g_out,'');
	log_message(g_out,rpad('Total Site Records:',40,' ')||ln_total_site_count);
	log_message(g_out,rpad('Total Site Records Imported:',40,' ')||(ln_total_site_count-ln_failed_site_count));
	log_message(g_out,rpad('Total Site Records Failed:',40,' ')||ln_failed_site_count);
	log_message(g_out,rpad('Total Bank Records:',40,' ')||ln_total_bank_count);
	log_message(g_out,rpad('Total Bank Records Imported:',40,' ')||(ln_total_bank_count-ln_failed_bank_count));
	log_message(g_out,rpad('Total Bank Records Failed:',40,' ')||ln_failed_bank_count);
	log_message(g_out,'');
	log_message(g_out,'***************Distinct Error Logs:Site**********************');
	log_message(g_out,rpad('Count:',10,' ')||'Error Message');
	for site_errors_rec in site_errors
	loop
	log_message(g_out,rpad(site_errors_rec.count_val,10,' ')||site_errors_rec.error_message);
	end loop;
	log_message(g_out,'');
	log_message(g_out,'');
	
	log_message(g_out,'***************Distinct Error Logs:Bank**********************');
	log_message(g_out,rpad('Count:',10,' ')||'Error Message');
	for bank_errors_rec in bank_errors
	loop
	log_message(g_out,rpad(bank_errors_rec.count_val,10,' ')||bank_errors_rec.error_message);
	end loop;
	log_message(g_out,'');
	log_message(g_out,'');	
	log_message(g_out,'*******************************************************************');
	
   	if ln_failed_site_count = 0 and ln_failed_bank_count = 0 then
   	 g_program_status := 0;
   	elsif ln_total_site_count = ln_failed_site_count and ln_total_bank_count = ln_failed_bank_count then
   	 g_program_status := 2;
   	else
   	 g_program_status := 1;
   	end if;	
	
   exception
   when others then
   log_message(g_log,'Exception in procedure import. Error Message:'||SQLERRM);
   END import;      
   
   
  -- ========================
  -- Procedure: close_sites
  -- =============================================================================
  --   This procedure will call update_site api to end-date vendor sites that has been
  --   created successfully or vendor sites that has been marked as retire.
  -- =============================================================================     
   
   PROCEDURE close_sites
   IS
    cursor supplier_site_inactivate
    is
    select * 
    from xxconv.XXAP_SUPPLIER_SITE_RES_STG site_stg
    where site_stg.process_flag = g_process_flag
     and status_flag = DECODE(operation,'COPY',g_processed,'RETIRE',g_new)
     and inactivate_after_processing = 'N'
     and ((operation = 'COPY' and process_flag = g_process_flag) OR (operation = 'RETIRE' and upper(copy_site) IN( 'Y','YES')));
     
	lv_inactivate_error_flag	VARCHAR2(1);
	lv_inactivate_error_msg		VARCHAR2(1000);	 
	lr_vendor_site_rec 	g_vendor_site_rec%type;
	
   ln_total_site_count			number := 0;
   ln_failed_site_count			number := 0;	
     
   begin
   	log_message(g_log,'Inactivating record Begin');
   	for supplier_site_inactivate_rec in supplier_site_inactivate
   	loop
   	 log_message(g_log,'Inactivating for record_id:'||supplier_site_inactivate_rec.record_id||
   	 	     ' vendor_site_id:'||supplier_site_inactivate_rec.vendor_site_id);
   	     --Resetting the flags
   	     lv_inactivate_error_flag	:= 'S';
	     lv_inactivate_error_msg	:= null;
	     
	     ln_total_site_count	:= ln_total_site_count+1;
	     
   	     lr_vendor_site_rec := g_vendor_site_rec;
   	     log_message(g_log,'Inactivating for vendor_id:'||supplier_site_inactivate_rec.vendor_id||
   	     		 ' org_id:'||supplier_site_inactivate_rec.org_id||' vendor_site_id:'||
   	     		 supplier_site_inactivate_rec.vendor_site_id);
   	     lr_vendor_site_rec.vendor_id := supplier_site_inactivate_rec.vendor_id;
   	     lr_vendor_site_rec.org_id := supplier_site_inactivate_rec.org_id;
   	     lr_vendor_site_rec.vendor_site_id := supplier_site_inactivate_rec.vendor_site_id;
   	     lr_vendor_site_rec.inactive_date  := sysdate;
	     update_supplier_site(lr_vendor_site_rec
				   ,lv_inactivate_error_flag
				   ,lv_inactivate_error_msg
			         );
	     log_message(g_log,'After inactivating site lv_inactivate_error_flag:'||lv_inactivate_error_flag||
	     		 ' lv_inactivate_error_msg:'||lv_inactivate_error_msg);
	     if lv_inactivate_error_flag = 'S' then
	       update xxconv.XXAP_SUPPLIER_SITE_RES_STG
		    set status_flag = g_completed
		      ,ERROR_MESSAGE = null
		      ,inactivate_after_processing = 'Y'
		      ,last_update_date = sysdate
		      ,last_updated_by = g_user_id
		      ,last_update_login = g_login_id
		      ,request_id = g_request_id
		 where record_id = supplier_site_inactivate_rec.record_id; 
		 commit;
	      else
	        --this rollback is to rollback the inactive date update made by oracle api.
	     	rollback;
	     	 update xxconv.XXAP_SUPPLIER_SITE_RES_STG
		    set status_flag = g_error
		      ,ERROR_MESSAGE = lv_inactivate_error_msg
		      ,last_update_date = sysdate
		      ,last_updated_by = g_user_id
		      ,last_update_login = g_login_id
		      ,request_id = g_request_id
		 where record_id = supplier_site_inactivate_rec.record_id; 
		 commit;
		 ln_failed_site_count :=  ln_failed_site_count + 1;
	     end if;
   	end loop;   
   	
   	log_message(g_out,'*******************************************************************');
   	log_message(g_out,'***************Printing Program Stats**********************');
	log_message(g_out,'');
	log_message(g_out,rpad('Total Site Records:',40,' ')||ln_total_site_count);
	log_message(g_out,rpad('Total Site Records Enddated:',40,' ')||(ln_total_site_count-ln_failed_site_count));
	log_message(g_out,rpad('Total Site Records Failed:',40,' ')||ln_failed_site_count);   	
	log_message(g_out,'');
	log_message(g_out,'*******************************************************************');
	
   	if ln_failed_site_count = 0 then
   	 g_program_status := 0;
   	elsif ln_total_site_count = ln_failed_site_count then
   	 g_program_status := 2;
   	else
   	 g_program_status := 1;
   	end if;
   	
   exception
   when others then
   log_message(g_log,'Exception in procedure close_sites. Error Message:'||SQLERRM);
   END close_sites;    
   
   
   -- ========================
   -- Procedure: main
   -- =============================================================================
   --   This is a main public procedure, which will be invoked through concurrent program.
   --   This conversion program will be called during restructuring to validate and import following entities
   --   Supplier Sites
   --   Supplier Contact associated to the site
   -- 	Bank Account information associated to the site
   -- =============================================================================
   --
   -- -----------------------------------------------------------------------------
   --  Called By Concurrent Program: Eaton AP Supplier Conversion Program - SIte Restructure
   -- -----------------------------------------------------------------------------
   -- -----------------------------------------------------------------------------
   --
   --  Input Parameters :
   --    piv_run_mode        : Control the flow program execution 
   --				Valid Modes are
   --				LOAD-DATA , VALIDATE , CONVERSION , CLOSE
   --
   --  Output Parameters :
   --    p_errbuf          : Standard output parameter for concurrent program
   --    p_retcode         : Standard output parameter for concurrent program
   --
   -- -----------------------------------------------------------------------------
   PROCEDURE main ( pov_errbuf            OUT   NOCOPY  VARCHAR2
		  , pon_retcode           OUT   NOCOPY  NUMBER
                  , piv_run_mode          IN            VARCHAR2    
                  )
   IS

   BEGIN
   	
   	log_message(g_log,'g_user_id:'||g_user_id);
   	log_message(g_log,'Main method Begin: piv_run_mode:'||piv_run_mode);
   	log_message(g_out,'*******************************************************************');
   	log_message(g_out,'***************Program Parameters**********************');
	log_message(g_out,rpad('Mode:',25,' ')||piv_run_mode);
	log_message(g_out,'');
	log_message(g_out,'*******************************************************************');

   	
   	if piv_run_mode = 'LOAD-DATA' then
   	
   		load_data;
   	
   	elsif piv_run_mode = 'VALIDATE' then
   	
   		validate;
   	
   	elsif piv_run_mode = 'CONVERSION' then
   	
   		import;
   		
   	elsif piv_run_mode = 'CLOSE' then
   		close_sites;   		
   	else
   		g_program_status := 2;
   		log_message(g_out,
   		'Selected mode is invalid for Vendor Site Program. Supported modes are LOAD-DATA,VALIDATE,CONVERSION and CLOSE');
   	end if;
   	
   	log_message(g_log,'pon_retcode:'||pon_retcode);
   	pon_retcode := g_program_status;
   	
   	
   	
   END main;
                  
END XXAP_SUPPLIER_RES_CNV_PKG;
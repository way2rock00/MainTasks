CREATE OR REPLACE PACKAGE BODY XXPO_PO_RES_CNV_PKG
AS

  -- global variables

  g_request_id      NUMBER DEFAULT fnd_global.conc_request_id;
  g_user_id         NUMBER DEFAULT fnd_global.user_id;
  g_login_id        NUMBER DEFAULT fnd_global.login_id;
  g_prog_appl_id        number default fnd_global.prog_appl_id;
  g_conc_program_id     number default fnd_global.conc_program_id;
  g_program_status				NUMBER;
  
  g_batch_id      NUMBER;
  g_log 		VARCHAR2(5) := 'LOG';
  g_out		VARCHAR2(5) := 'OUT';  

  --global variables for stauts
  g_new      varchar2(1) := 'N';
  g_validated      varchar2(1) := 'V';
  g_error     varchar2(1) := 'E';
  g_processed      varchar2(1) := 'P';
  g_completed 	   varchar2(1) := 'C';
  g_process_flag   varchar2(1) := 'Y';
  g_ignored			varchar2(1) := 'X';
  
  

  --global variables for error
  g_indx    NUMBER := 0;
  g_limit   CONSTANT NUMBER := fnd_profile.value('ETN_FND_ERROR_TAB_LIMIT');
  g_source_Tab xxetn_common_error_pkg.g_source_tab_type;


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
                       piv_source_table         IN VARCHAR2,
                       pin_record_id         IN NUMBER,
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
  -- Procedure: load
  -- =============================================================================
  --   This procedure will log the errors in the error report using error
  --   framework
  -- =============================================================================

   PROCEDURE load
   IS

   --header record count variables
   ln_total_header_records  number default 0;
   ln_failed_header_records   number default 0;

   --line record count variables
   ln_total_line_records  number default 0;
   ln_failed_line_records   number default 0;

   --line location count variables
   ln_total_line_loc_records  number default 0;
   ln_failed_line_loc_records   number default 0;

   --distribution count variables
   ln_total_dist_records  number default 0;
   ln_failed_dist_records   number default 0;

   cursor po_header_cur
   is
	select XXCONV.XXPO_PO_HEADER_RES_S.nextval record_id 
	,g_request_id request_id
	,g_process_flag process_flag
	,g_new status_flag
	,poh.PO_HEADER_ID                     
	,null NEW_PO_HEADER_ID                     
	,null INTERFACE_HEADER_ID      
	,poh.TYPE_LOOKUP_CODE                 
	,poh.SEGMENT1                         
	,poh.CURRENCY_CODE                          
	,poh.RATE_TYPE                              
	,poh.RATE_DATE                              
	,poh.RATE                                   
	,null NEW_RATE_TYPE                          
	,null NEW_RATE_DATE                          
	,null NEW_RATE                               
	,poh.AGENT_ID                         
	,null NEW_AGENT_ID        
	,null BUYER_NUMBER        
	,poh.VENDOR_ID                              
	,sup.VENDOR_NAME                         
	,sups.VENDOR_SITE_ID                         
	,null NEW_VENDOR_SITE_ID                     
	,VENDOR_SITE_CODE      
	,null NEW_VENDOR_SITE_CODE      
	,poh.VENDOR_CONTACT_ID                      
	,null VENDOR_CONTACT        
	,poh.SHIP_TO_LOCATION_ID                    
	,null SHIP_TO_LOCATION_CODE      
	,poh.BILL_TO_LOCATION_ID                    
	,null BILL_TO_LOCATION_CODE      
	,poh.TERMS_ID                               
	,null PAYMENT_TERMS        
	,poh.SHIP_VIA_LOOKUP_CODE                   
	,poh.FOB_LOOKUP_CODE                        
	,poh.FREIGHT_TERMS_LOOKUP_CODE              
	,poh.AUTHORIZATION_STATUS                   
	,poh.APPROVED_DATE                          
	,poh.REVISED_DATE                           
	,poh.REVISION_NUM                           
	,poh.NOTE_TO_VENDOR                         
	,poh.NOTE_TO_RECEIVER                       
	,poh.CONFIRMING_ORDER_FLAG                  
	,poh.COMMENTS                               
	,poh.ACCEPTANCE_REQUIRED_FLAG               
	,poh.ACCEPTANCE_DUE_DATE                    
	,poh.PRINT_COUNT                            
	,poh.PRINTED_DATE                           
	,poh.CLOSED_CODE                            
	,poh.CLOSED_DATE                            
	,poh.USSGL_TRANSACTION_CODE                 
	,poh.ATTRIBUTE_CATEGORY                     
	,poh.ATTRIBUTE1                             
	,poh.ATTRIBUTE2                             
	,poh.ATTRIBUTE3                             
	,poh.ATTRIBUTE4                             
	,poh.ATTRIBUTE5                             
	,poh.ATTRIBUTE6                             
	,poh.ATTRIBUTE7                             
	,poh.ATTRIBUTE8                             
	,poh.ATTRIBUTE9                             
	,poh.ATTRIBUTE10                            
	,poh.ATTRIBUTE11                            
	,poh.ATTRIBUTE12                            
	,poh.ATTRIBUTE13                            
	,poh.ATTRIBUTE14                            
	,poh.ATTRIBUTE15                            
	,poh.PAY_ON_CODE                            
	,poh.SHIPPING_CONTROL                       
	,poh.CHANGE_SUMMARY                         
	,poh.GLOBAL_ATTRIBUTE_CATEGORY              
	,poh.GLOBAL_ATTRIBUTE1                      
	,poh.GLOBAL_ATTRIBUTE2                      
	,poh.GLOBAL_ATTRIBUTE3                      
	,poh.GLOBAL_ATTRIBUTE4                      
	,poh.GLOBAL_ATTRIBUTE5                      
	,poh.GLOBAL_ATTRIBUTE6                      
	,poh.GLOBAL_ATTRIBUTE7                      
	,poh.GLOBAL_ATTRIBUTE8                      
	,poh.GLOBAL_ATTRIBUTE9                      
	,poh.GLOBAL_ATTRIBUTE10                     
	,poh.GLOBAL_ATTRIBUTE11                     
	,poh.GLOBAL_ATTRIBUTE12                     
	,poh.GLOBAL_ATTRIBUTE13                     
	,poh.GLOBAL_ATTRIBUTE14                     
	,poh.GLOBAL_ATTRIBUTE15                     
	,poh.GLOBAL_ATTRIBUTE16                     
	,poh.GLOBAL_ATTRIBUTE17                     
	,poh.GLOBAL_ATTRIBUTE18                     
	,poh.GLOBAL_ATTRIBUTE19                     
	,poh.GLOBAL_ATTRIBUTE20                     
	,poh.ORG_ID                                 
	,null NEW_ORG_ID    
	,null multiple_dist_flag
	--WHO COLUMNS
	,g_user_id CREATED_BY                             
	,sysdate CREATION_DATE                          
	,null LAST_UPDATE_DATE                 
	,null LAST_UPDATED_BY                  
	,null LAST_UPDATE_LOGIN                      
	,null ERROR_MESSAGE        
	,hou.organization_id  SOURCE_ORG_ID
	,substr(lookup_code,1,instr(lookup_code,'.')-1) source_org_number
	,hou.name  SOURCE_ORG_NAME			
	,substr(lookup_code,instr(lookup_code,'.')+1) source_plant_name
	,hou1.organization_id  DESTINATION_ORG_ID
	,substr(meaning,1,instr(meaning,'.')-1) destination_org_number
	,hou1.name  DESTINATION_ORG_NAME			
	, substr(meaning,instr(meaning,'.')+1) destination_plant_name
	,tag OPERATION	
	,'N' po_cancelled_flag
	from po_headers_all poh
	    ,ap_supplier_sites_all sups
	    ,ap_suppliers sup
		,fnd_lookup_types  flt
		,fnd_lookup_values flv
	    ,hr_operating_units hou
	    ,hr_operating_units hou1
	where flt.LOOKUP_TYPE like 'XXETN_RESTRUCTURE_MAPPING'
	and flv.LOOKUP_TYPE = flt.lookup_type
	and flv.language = USERENV('LANG')
	and flv.enabled_flag = 'Y'
	and sysdate between flv.start_date_active and NVL(flv.end_date_active,sysdate)
	and hou.short_code = substr(lookup_code,1,instr(lookup_code,'.')-1)||'_OU'
	and poh.org_id = hou.organization_id 
	and exists(select 1 
			from po_distributions_all pod 
				,gl_code_combinations_kfv gl
			where pod.po_header_id = poh.po_header_id
			 and pod.code_combination_id = gl.code_combination_id
			 and gl.segment2 = substr(lookup_code,instr(lookup_code,'.')+1)
				)
	--and flv.lookup_code = '0306.5245' --temp
	--and poh.org_id = 500169--temp
	and hou1.short_code = substr(meaning,1,instr(meaning,'.')-1)||'_OU'
	and poh.vendor_site_id = sups.vendor_site_id
	and poh.vendor_id = sup.vendor_id
	and sup.vendor_id = sups.vendor_id
	and poh.AUTHORIZATION_STATUS = 'APPROVED'
	and NVL(poh.cancel_flag,'N') = 'N';
	--and poh.po_header_id in ( 175576,175577,175578,175579);
	--and poh.po_header_id = 175884;--this is inventory related po that is converted.
	--and poh.po_header_id = 175912 ;--177880(quantity testing);
	--and poh.po_header_id in (1774014,177183,176234); --po has projects and invalid projects and no project data.
	--and poh.po_header_id = 176099;--this po has expenditure and other information at distribution level for testing.
	--and poh.po_header_id in (693385,693393,176099);--These PO's are for rate type testing 693385=Corporate 693393 = rate_type (1004) , 176099 rate type is null
	--and poh.po_header_id in ( 176099,175886);-- in (5921654,5722048,176099 , 175886) --temp
	--5921654,5722048 these records has both 5245 and 5243
	--176099 , 175886 has multiple distributions
   
   
   type c_po_header_cur_rec is table of po_header_cur%rowtype;
   c_po_header_rec	c_po_header_cur_rec;   
   
   cursor po_lines_cur
   is
   SELECT XXCONV.XXPO_PO_LINE_RES_S.nextval record_id 
	,header_stg.record_id parent_record_id
	,g_request_id request_id
	,g_new status_flag
	,pol.PO_LINE_ID                  			
	,null NEW_PO_LINE_ID                  		
	,pol.PO_HEADER_ID                			
	,null NEW_PO_HEADER_ID                		
	,pol.LINE_NUM                    			
	,null SHIPMENT_TYPE --from line locations but column present in line interface table.					
	,pol.LINE_TYPE_ID                         		
	,null LINE_TYPE					
	,pol.ITEM_ID                              		
	,null NEW_ITEM_ID                            	
	,null item  
	,null new_item	
	,null ship_to_organization_id
	,null new_ship_to_organization_id
	,pol.ITEM_REVISION                        		
	,pol.CATEGORY_ID                          		
	,null CATEGORY			      		
	,pol.ITEM_DESCRIPTION                     		
	,pol.VENDOR_PRODUCT_NUM				
	,pol.UNIT_MEAS_LOOKUP_CODE                		
	,pol.QUANTITY                             		
	,null new_quantity
	,pol.UNIT_PRICE                           		
	,pol.LIST_PRICE_PER_UNIT                  		
	,pol.UN_NUMBER_ID                         		
	,null UN_NUMBER					
	,pol.HAZARD_CLASS_ID                      		
	,null HAZARD_CLASS					
	,pol.NOTE_TO_VENDOR                       		
	,pol.TRANSACTION_REASON_CODE              		
	,pol.TAXABLE_FLAG					
	,pol.TAX_NAME					
	,null INSPECTION_REQUIRED_FLAG	--from line locations but column present in line interface table.		
	,null RECEIPT_REQUIRED_FLAG  --from line locations but column present in line interface table.
	,pol.PRICE_TYPE_LOOKUP_CODE               		
	,null PRICE_TYPE	--in po_lines_all we have PRICE_TYPE_LOOKUP_CODE whereas in interface table we have price_type.
	,pol.USSGL_TRANSACTION_CODE               		
	,pol.CLOSED_CODE                          		
	,pol.CLOSED_REASON                        		
	,pol.CLOSED_DATE                          		
	,pol.CLOSED_BY                            		
	,null INVOICE_CLOSE_TOLERANCE	--from line locations but column present in line interface table.		
	,null RECEIVE_CLOSE_TOLERANCE	--from line locations but column present in line interface table.		
	,null DAYS_EARLY_RECEIPT_ALLOWED --from line locations but column present in line interface table.			
	,null DAYS_LATE_RECEIPT_ALLOWED	--from line locations but column present in line interface table.		
	,null RECEIVING_ROUTING_ID	--from line locations but column present in line interface table.			
	,pol.QTY_RCV_TOLERANCE              	      	
	,pol.OVER_TOLERANCE_ERROR_FLAG      	      	
	,null QTY_RCV_EXCEPTION_CODE --from line locations but column present in line interface table.				
	,null NEED_BY_DATE	--from line locations but column present in line interface table.				
	,null PROMISED_DATE	--from line locations but column present in line interface table.					
	,pol.ATTRIBUTE_CATEGORY             	      	
	,pol.ATTRIBUTE1                     	      	
	,pol.ATTRIBUTE2                     	      	
	,pol.ATTRIBUTE3                     	      	
	,pol.ATTRIBUTE4                     	      	
	,pol.ATTRIBUTE5                     	      	
	,pol.ATTRIBUTE6                     	      	
	,pol.ATTRIBUTE7                     	      	
	,pol.ATTRIBUTE8                     	      	
	,pol.ATTRIBUTE9                     	      	
	,pol.ATTRIBUTE10                    	      	
	,pol.ATTRIBUTE11                    	      	
	,pol.ATTRIBUTE12                    	      	
	,pol.ATTRIBUTE13                    	      	
	,pol.ATTRIBUTE14                    	      	
	,pol.ATTRIBUTE15              
	--below need to check this with Aditya.
	,null TAX_STATUS_INDICATOR	--this column is present only in po_lines_interface and not present anywhere how do we fetch this.			
	,null TAX_USER_OVERRIDE_FLAG --from line locations but column present in line interface table.													
	,pol.TAX_CODE_ID					
	,null NOTE_TO_RECEIVER	--from line locations but column present in line interface table.									
	,null CONSIGNED_FLAG	--from line locations but column present in line interface table.						
	,pol.SUPPLIER_REF_NUMBER				
	,null DROP_SHIP_FLAG	--from line locations but column present in line interface table.		
	,null TERMS_ID		--from line locations but columns present in the line interface table.
	,pol.GLOBAL_ATTRIBUTE_CATEGORY           		
	,pol.GLOBAL_ATTRIBUTE1                   		
	,pol.GLOBAL_ATTRIBUTE2                   		
	,pol.GLOBAL_ATTRIBUTE3                   		
	,pol.GLOBAL_ATTRIBUTE4                   		
	,pol.GLOBAL_ATTRIBUTE5                   		
	,pol.GLOBAL_ATTRIBUTE6                   		
	,pol.GLOBAL_ATTRIBUTE7                   		
	,pol.GLOBAL_ATTRIBUTE8                   		
	,pol.GLOBAL_ATTRIBUTE9                   		
	,pol.GLOBAL_ATTRIBUTE10                  		
	,pol.GLOBAL_ATTRIBUTE11                  		
	,pol.GLOBAL_ATTRIBUTE12                  		
	,pol.GLOBAL_ATTRIBUTE13                  		
	,pol.GLOBAL_ATTRIBUTE14                  		
	,pol.GLOBAL_ATTRIBUTE15                  		
	,pol.GLOBAL_ATTRIBUTE16                  		
	,pol.GLOBAL_ATTRIBUTE17                  		
	,pol.GLOBAL_ATTRIBUTE18                  		
	,pol.GLOBAL_ATTRIBUTE19                  		
	,pol.GLOBAL_ATTRIBUTE20                  		
	,pol.ORG_ID                               		
	,null NEW_ORG_ID                           		
	,null INTERFACE_LINE_ID				
	,null INTERFACE_HEADER_ID				
	,g_user_id CREATED_BY                             	
	,sysdate CREATION_DATE                          	
	,null LAST_UPDATE_DATE               		
	,null LAST_UPDATED_BY                		
	,null LAST_UPDATE_LOGIN                      	
	,null ERROR_MESSAGE					
	,header_stg.SOURCE_ORG_ID					
	,header_stg.SOURCE_ORG_NUMBER				
	,header_stg.SOURCE_ORG_NAME				
	,header_stg.SOURCE_PLANT_NAME				
	,header_stg.DESTINATION_ORG_ID				
	,header_stg.DESTINATION_ORG_NUMBER				
	,header_stg.DESTINATION_ORG_NAME				
	,header_stg.DESTINATION_PLANT_NAME				
	,header_stg.OPERATION			
   from xxconv.XXPO_PO_HEADERS_ALL_RES_STG header_stg
       ,po_lines_all pol
    where header_stg.process_flag = g_process_flag
     and header_stg.status_flag = g_new
     and header_stg.request_id = g_request_id
     and header_stg.po_header_id = pol.po_header_id;
     
   type c_po_lines_cur_rec is table of po_lines_cur%rowtype;
   c_po_lines_rec	c_po_lines_cur_rec;   
   
   cursor po_line_loc_cur
   is
   SELECT XXCONV.XXPO_PO_LINE_LOC_RES_S.nextval record_id 
	,header_stg.record_id parent_record_id
	,g_request_id request_id
	,g_new status_flag   
	,poll.LINE_LOCATION_ID               		
	,null NEW_LINE_LOCATION_ID           		
	,poll.PO_HEADER_ID                   		
	,null NEW_PO_HEADER_ID               		
	,poll.PO_LINE_ID                     		
	,null NEW_PO_LINE_ID                 		
	,poll.SHIPMENT_NUM                           	
	,poll.SHIPMENT_TYPE                  		
	,poll.QUANTITY
	,null new_quantity	
    ,poll.quantity_received	
	,poll.quantity_billed
	,poll.TAXABLE_FLAG                           	
	,poll.TAX_NAME                               	
	,poll.INSPECTION_REQUIRED_FLAG			
	,poll.RECEIPT_REQUIRED_FLAG                  	
	,poll.CLOSED_CODE					
	,poll.CLOSED_REASON					
	,poll.CLOSED_DATE					
	,poll.CLOSED_BY						
	,poll.INVOICE_CLOSE_TOLERANCE			
	,poll.RECEIVE_CLOSE_TOLERANCE			
	,poll.DAYS_EARLY_RECEIPT_ALLOWED			
	,poll.DAYS_LATE_RECEIPT_ALLOWED			
	,poll.ENFORCE_SHIP_TO_LOCATION_CODE			
	,poll.ALLOW_SUBSTITUTE_RECEIPTS_FLAG			
	,poll.SHIP_TO_ORGANIZATION_ID                	
	,null SHIP_TO_ORGANIZATION_CODE              	
	,null NEW_SHIP_TO_ORGANIZATION_ID            	
	,poll.QTY_RCV_TOLERANCE				
	,poll.QTY_RCV_EXCEPTION_CODE                 	
	,poll.RECEIPT_DAYS_EXCEPTION_CODE			
	,null SHIP_TO_LOCATION_CODE				
	,poll.SHIP_TO_LOCATION_ID                    	
	,poll.ATTRIBUTE_CATEGORY                     	
	,poll.ATTRIBUTE1                             	
	,poll.ATTRIBUTE2                             	
	,poll.ATTRIBUTE3                             	
	,poll.ATTRIBUTE4                             	
	,poll.ATTRIBUTE5                             	
	,poll.ATTRIBUTE6                             	
	,poll.ATTRIBUTE7                             	
	,poll.ATTRIBUTE8                             	
	,poll.ATTRIBUTE9                             	
	,poll.ATTRIBUTE10                            	
	,poll.ATTRIBUTE11                            	
	,poll.ATTRIBUTE12                            	
	,poll.ATTRIBUTE13                            	
	,poll.ATTRIBUTE14                            	
	,poll.ATTRIBUTE15                            	
	,poll.RECEIVING_ROUTING_ID				
	,poll.MATCH_OPTION                           	
	,poll.ACCRUE_ON_RECEIPT_FLAG                 	
	,poll.PRICE_OVERRIDE					
	,poll.NEED_BY_DATE					
	,poll.PROMISED_DATE					
	,poll.NOTE_TO_RECEIVER				
	,poll.UNIT_MEAS_LOOKUP_CODE	--Will populate this value in UNIT_OF_MEASURE in lines interface table.				
	,null UOM_CODE		--this not sure where to pick from.					
	,poll.PRICE_DISCOUNT					
	,poll.VALUE_BASIS					
	,poll.MATCHING_BASIS					
	,poll.COUNTRY_OF_ORIGIN_CODE	
	,poll.drop_ship_flag
	,poll.consigned_flag
	,poll.tax_user_override_flag
	,poll.GLOBAL_ATTRIBUTE_CATEGORY               	
	,poll.GLOBAL_ATTRIBUTE1                       	
	,poll.GLOBAL_ATTRIBUTE2                       	
	,poll.GLOBAL_ATTRIBUTE3                       	
	,poll.GLOBAL_ATTRIBUTE4                       	
	,poll.GLOBAL_ATTRIBUTE5                       	
	,poll.GLOBAL_ATTRIBUTE6                       	
	,poll.GLOBAL_ATTRIBUTE7                       	
	,poll.GLOBAL_ATTRIBUTE8                       	
	,poll.GLOBAL_ATTRIBUTE9                       	
	,poll.GLOBAL_ATTRIBUTE10                      	
	,poll.GLOBAL_ATTRIBUTE11                      	
	,poll.GLOBAL_ATTRIBUTE12                      	
	,poll.GLOBAL_ATTRIBUTE13                      	
	,poll.GLOBAL_ATTRIBUTE14                      	
	,poll.GLOBAL_ATTRIBUTE15                      	
	,poll.GLOBAL_ATTRIBUTE16                      	
	,poll.GLOBAL_ATTRIBUTE17                      	
	,poll.GLOBAL_ATTRIBUTE18                      	
	,poll.GLOBAL_ATTRIBUTE19                      	
	,poll.GLOBAL_ATTRIBUTE20                      	
	,poll.ORG_ID                                  	
	,null NEW_ORG_ID                              	
	,null INTERFACE_HEADER_ID		
	,null INTERFACE_LINE_ID		
	,null INTERFACE_LINE_LOCATION_ID	
	,null po_line_open_flag
	,g_user_id CREATED_BY                              	
	,sysdate CREATION_DATE                           	
	,null LAST_UPDATE_DATE               		
	,null LAST_UPDATED_BY                		
	,null LAST_UPDATE_LOGIN                      	
	,null ERROR_MESSAGE					
	,header_stg.SOURCE_ORG_ID					
	,header_stg.SOURCE_ORG_NUMBER				
	,header_stg.SOURCE_ORG_NAME				
	,header_stg.SOURCE_PLANT_NAME				
	,header_stg.DESTINATION_ORG_ID				
	,header_stg.DESTINATION_ORG_NUMBER				
	,header_stg.DESTINATION_ORG_NAME				
	,header_stg.DESTINATION_PLANT_NAME				
	,header_stg.OPERATION					
   from xxconv.XXPO_PO_HEADERS_ALL_RES_STG header_stg
       ,po_lines_all pol
       ,po_line_locations_all poll
    where header_stg.process_flag = g_process_flag
     and header_stg.status_flag = g_new
     and header_stg.request_id = g_request_id
     and header_stg.po_header_id = pol.po_header_id
     and pol.po_header_id = poll.po_header_id
     and pol.po_line_id = poll.po_line_id;
     
     
  
   type c_po_line_loc_cur_rec is table of po_line_loc_cur%rowtype;
   c_po_line_loc_rec	c_po_line_loc_cur_rec; 
   
   cursor po_distribution_cur
   is
   SELECT XXCONV.XXPO_PO_DIST_RES_S.nextval record_id 
	,header_stg.record_id parent_record_id
	,g_request_id request_id
	,g_new status_flag   
	,pod.PO_DISTRIBUTION_ID         		
	,null NEW_PO_DISTRIBUTION_ID     		
	,pod.PO_HEADER_ID               		
	,NEW_PO_HEADER_ID           		
	,pod.PO_LINE_ID                 		
	,null NEW_PO_LINE_ID             		
	,pod.LINE_LOCATION_ID           		
	,null NEW_LINE_LOCATION_ID       		
	,pod.DISTRIBUTION_NUM            		
	,pod.QUANTITY_ORDERED            		
	,null new_quantity
	,pod.QUANTITY_DELIVERED          		
	,pod.QUANTITY_BILLED             		
	,pod.QUANTITY_CANCELLED          		
	,pod.RATE_DATE                   		
	,pod.RATE                        		
	,pod.DELIVER_TO_LOCATION_ID      		
	,null DELIVER_TO_LOCATION_CODE		
	,pod.DELIVER_TO_PERSON_ID        		
	,pod.DESTINATION_TYPE_CODE       		
	,pod.DESTINATION_ORGANIZATION_ID 		
	,null NEW_DESTINATION_ORG_ID		      	
	,null DESTINATION_ORGANIZATION		
	,pod.DESTINATION_SUBINVENTORY             	
	,null NEW_DESTINATION_SUBINVENTORY		
	,pod.SET_OF_BOOKS_ID             		
	,null NEW_SET_OF_BOOKS_ID         		
	,null SET_OF_BOOKS				
	,pod.code_combination_id CHARGE_ACCOUNT 
	,pod.ACCRUAL_ACCOUNT_ID                   	
	,pod.VARIANCE_ACCOUNT_ID                  	
	,pod.AMOUNT_BILLED                        	
	,pod.ACCRUED_FLAG
    ,pod.ACCRUE_ON_RECEIPT_FLAG	
	,pod.GL_CANCELLED_DATE                    	
	,pod.GL_CLOSED_DATE                       	
	,pod.REQ_HEADER_REFERENCE_NUM             	
	,pod.REQ_DISTRIBUTION_ID                  	
	,pod.USSGL_TRANSACTION_CODE               	
	,pod.GOVERNMENT_CONTEXT                   	
	,pod.PROJECT_ID                           	
	,null NEW_PROJECT_ID                       	
	,null PROJECT						      
	,pod.TASK_ID                              	
	,null NEW_TASK_ID                          	
	,null TASK								     
	,pod.EXPENDITURE_TYPE                     	
	,pod.PROJECT_ACCOUNTING_CONTEXT           	
	,pod.EXPENDITURE_ORGANIZATION_ID          	
	,null NEW_EXPENDITURE_ORG_ID			
	,pod.EXPENDITURE_ITEM_DATE                	
	,pod.ATTRIBUTE_CATEGORY                   	
	,pod.ATTRIBUTE1                           	
	,pod.ATTRIBUTE2                           	
	,pod.ATTRIBUTE3  
	,null new_attribute1
	,null new_attribute2
	,null new_attribute3
	,pod.ATTRIBUTE4                           	
	,pod.ATTRIBUTE5                           	
	,pod.ATTRIBUTE6                           	
	,pod.ATTRIBUTE7                           	
	,pod.ATTRIBUTE8                           	
	,pod.ATTRIBUTE9                           	
	,pod.ATTRIBUTE10                          	
	,pod.ATTRIBUTE11                          	
	,pod.ATTRIBUTE12                          	
	,pod.ATTRIBUTE13                          	
	,pod.ATTRIBUTE14                          	
	,pod.ATTRIBUTE15                          	
	,pod.RECOVERABLE_TAX                      	
	,pod.NONRECOVERABLE_TAX                   	
	,pod.RECOVERY_RATE                        	
	,pod.TAX_RECOVERY_OVERRIDE_FLAG           	
	,pod.OKE_CONTRACT_LINE_ID                 	
	,pod.OKE_CONTRACT_DELIVERABLE_ID          	
	,pod.AWARD_ID                             	
	,pod.AMOUNT_ORDERED                       	
	,pod.INVOICE_ADJUSTMENT_FLAG              	
	,pod.GLOBAL_ATTRIBUTE_CATEGORY            	
	,pod.GLOBAL_ATTRIBUTE1                    	
	,pod.GLOBAL_ATTRIBUTE2                    	
	,pod.GLOBAL_ATTRIBUTE3                    	
	,pod.GLOBAL_ATTRIBUTE4                    	
	,pod.GLOBAL_ATTRIBUTE5                    	
	,pod.GLOBAL_ATTRIBUTE6                    	
	,pod.GLOBAL_ATTRIBUTE7                    	
	,pod.GLOBAL_ATTRIBUTE8                    	
	,pod.GLOBAL_ATTRIBUTE9                    	
	,pod.GLOBAL_ATTRIBUTE10                   	
	,pod.GLOBAL_ATTRIBUTE11                   	
	,pod.GLOBAL_ATTRIBUTE12                   	
	,pod.GLOBAL_ATTRIBUTE13                   	
	,pod.GLOBAL_ATTRIBUTE14                   	
	,pod.GLOBAL_ATTRIBUTE15                   	
	,pod.GLOBAL_ATTRIBUTE16                   	
	,pod.GLOBAL_ATTRIBUTE17                   	
	,pod.GLOBAL_ATTRIBUTE18                   	
	,pod.GLOBAL_ATTRIBUTE19                   	
	,pod.GLOBAL_ATTRIBUTE20                   	
	,pod.ORG_ID                               	
	,null NEW_ORG_ID                           	
	,null INTERFACE_DISTRIBUTION_ID		
	,null INTERFACE_LINE_LOCATION_ID		
	,null INTERFACE_LINE_ID			
	,null INTERFACE_HEADER_ID	
	,null project_info_valid_flag
	,g_user_id CREATED_BY                     	
	,sysdate CREATION_DATE                  	
	,null LAST_UPDATE_DATE               	
	,null LAST_UPDATED_BY                	
	,null LAST_UPDATE_LOGIN              	
	,null ERROR_MESSAGE				
	,header_stg.SOURCE_ORG_ID				
	,header_stg.SOURCE_ORG_NUMBER			
	,header_stg.SOURCE_ORG_NAME			
	,header_stg.SOURCE_PLANT_NAME			
	,header_stg.DESTINATION_ORG_ID			
	,header_stg.DESTINATION_ORG_NUMBER			
	,header_stg.DESTINATION_ORG_NAME			
	,header_stg.DESTINATION_PLANT_NAME			
	,header_stg.OPERATION				
   from xxconv.XXPO_PO_HEADERS_ALL_RES_STG header_stg
       ,po_lines_all pol
       ,po_line_locations_all poll
       ,po_distributions_all pod
    where header_stg.process_flag = g_process_flag
     and header_stg.status_flag = g_new
     and header_stg.request_id = g_request_id
     and header_stg.po_header_id = pol.po_header_id
     and pol.po_header_id = poll.po_header_id
     and pol.po_line_id = poll.po_line_id
     and poll.po_header_id = pod.po_header_id
     and poll.po_line_id = pod.po_line_id
     and poll.line_location_id = pod.line_location_id;

	
   type c_po_distribution_cur_rec is table of po_distribution_cur%rowtype;
   c_po_distribution_rec		c_po_distribution_cur_rec; 	
   

   BEGIN
     log_message(g_log,'Loading PO Header Data Begin');
   	open po_header_cur;
   	loop
   	fetch po_header_cur BULK COLLECT into c_po_header_rec limit 1000;
   	exit when c_po_header_rec.count = 0;
   	ln_total_header_records := ln_total_header_records+c_po_header_rec.count;
   	
   		begin
   		
			FORALL i IN 1..c_po_header_rec.COUNT SAVE EXCEPTIONS
			INSERT INTO xxconv.XXPO_PO_HEADERS_ALL_RES_STG VALUES c_po_header_rec(i);
		exception
		when others then
		   FOR l_indx_exp IN 1 .. SQL%BULK_EXCEPTIONS.COUNT LOOP
			   ln_failed_header_records := ln_failed_header_records + 1;
			   log_message(g_log,'Exception while loading data in header table: Record Id:'||
			   		c_po_header_rec(SQL%BULK_EXCEPTIONS(l_indx_exp).ERROR_INDEX).record_id||
			   		' Error Message:'||SQL%BULK_EXCEPTIONS(l_indx_exp).ERROR_CODE);
		   end loop;
		end;
   		
   	end loop;
   	close po_header_cur;

    	log_message(g_log,'Total PO Header Records:'||ln_total_header_records);
    	log_message(g_log,'Total PO Header Records loaded:'||(ln_total_header_records-ln_failed_header_records));
    	log_message(g_log,'Failed PO Header Records while loading:'||ln_failed_header_records);     

    	log_message(g_out,'Total PO Header Records:'||ln_total_header_records);
    	log_message(g_out,'Total PO Header Records loaded:'||(ln_total_header_records-ln_failed_header_records));
    	log_message(g_out,'Failed PO Header Records while loading:'||ln_failed_header_records);   
    	
    	
    	--Loading PO Lines Data.
   	open po_lines_cur;
   	loop
   	fetch po_lines_cur BULK COLLECT into c_po_lines_rec limit 1000;
   	exit when c_po_lines_rec.count = 0;
   	ln_total_line_records := ln_total_line_records+c_po_lines_rec.count;
   	
   		begin
   		
			FORALL i IN 1..c_po_lines_rec.COUNT SAVE EXCEPTIONS
			INSERT INTO xxconv.XXPO_PO_LINES_ALL_RES_STG VALUES c_po_lines_rec(i);
		exception
		when others then
		   FOR l_indx_exp IN 1 .. SQL%BULK_EXCEPTIONS.COUNT LOOP
		   ln_failed_line_records := ln_failed_line_records + 1;
			   log_message(g_log,'Exception while loading data in po line table: Record Id:'||
			   		c_po_lines_rec(SQL%BULK_EXCEPTIONS(l_indx_exp).ERROR_INDEX).record_id||
			   		' Error Message:'||SQL%BULK_EXCEPTIONS(l_indx_exp).ERROR_CODE);
		   
		   end loop;
		end;
   		
   	end loop;
   	close po_lines_cur;

    	log_message(g_log,'Total PO Lines Records:'||ln_total_line_records);
    	log_message(g_log,'Total PO Lines Records loaded:'||(ln_total_line_records-ln_failed_line_records));
    	log_message(g_log,'Failed PO Lines Records while loading:'||ln_failed_line_records);    
    	
    	log_message(g_out,'Total PO Lines Records:'||ln_total_line_records);
    	log_message(g_out,'Total PO Lines Records loaded:'||(ln_total_line_records-ln_failed_line_records));
    	log_message(g_out,'Failed PO Lines Records while loading:'||ln_failed_line_records);    
    	
	    	
    	--Loading PO Line Locations Data.
   	open po_line_loc_cur;
   	loop
   	fetch po_line_loc_cur BULK COLLECT into c_po_line_loc_rec limit 1000;
   	exit when c_po_line_loc_rec.count = 0;
   	ln_total_line_loc_records := ln_total_line_loc_records+c_po_line_loc_rec.count;
   	
   		begin
   		
			FORALL i IN 1..c_po_line_loc_rec.COUNT SAVE EXCEPTIONS
			INSERT INTO xxconv.XXPO_PO_LINE_LOC_ALL_RES_STG VALUES c_po_line_loc_rec(i);
		exception
		when others then
		   FOR l_indx_exp IN 1 .. SQL%BULK_EXCEPTIONS.COUNT LOOP
		   ln_failed_line_loc_records := ln_failed_line_loc_records + 1;

			   log_message(g_log,'Exception while loading data in po line location table: Record Id:'||
			   		c_po_line_loc_rec(SQL%BULK_EXCEPTIONS(l_indx_exp).ERROR_INDEX).record_id||
			   		' Error Message:'||SQL%BULK_EXCEPTIONS(l_indx_exp).ERROR_CODE);
		   
		   end loop;
		end;
   		
   	end loop;
   	close po_line_loc_cur;
   	log_message(g_log,'Total PO Line Location Records:'||ln_total_line_loc_records);
   	log_message(g_log,'Total PO Line Location Records loaded:'||(ln_total_line_loc_records-ln_failed_line_loc_records));
    log_message(g_log,'Failed PO Line Location Records while loading:'||ln_failed_line_loc_records); 
   	log_message(g_out,'Total PO Line Location Records:'||ln_total_line_loc_records);
   	log_message(g_out,'Total PO Line Location Records loaded:'||(ln_total_line_loc_records-ln_failed_line_loc_records));
    log_message(g_out,'Failed PO Line Location Records while loading:'||ln_failed_line_loc_records); 
    	
    	
    	--Loading PO Distributions Data.
   	open po_distribution_cur;
   	loop
   	fetch po_distribution_cur BULK COLLECT into c_po_distribution_rec limit 1000;
   	exit when c_po_distribution_rec.count = 0;
   	ln_total_dist_records := ln_total_dist_records+c_po_distribution_rec.count;
   	
   		begin
   		
			FORALL i IN 1..c_po_distribution_rec.COUNT SAVE EXCEPTIONS
			INSERT INTO xxconv.XXPO_PO_DIST_ALL_RES_STG VALUES c_po_distribution_rec(i);
		exception
		when others then
		   FOR l_indx_exp IN 1 .. SQL%BULK_EXCEPTIONS.COUNT LOOP
		   ln_failed_dist_records := ln_failed_dist_records + 1;
			   log_message(g_log,'Exception while loading data in po distribution table: Record Id:'||
			   		c_po_distribution_rec(SQL%BULK_EXCEPTIONS(l_indx_exp).ERROR_INDEX).record_id||
			   		' Error Message:'||SQL%BULK_EXCEPTIONS(l_indx_exp).ERROR_CODE);
		   
		   end loop;
		end;
   		
   	end loop;
   	close po_distribution_cur;
   	log_message(g_log,'Total PO Distributions Records:'||ln_total_dist_records);
   	log_message(g_log,'Total PO Distributions Records loaded:'||(ln_total_dist_records-ln_failed_dist_records));
    	log_message(g_log,'Failed PO Distributions Records while loading:'||ln_failed_dist_records);       	
    	
   	log_message(g_out,'Total PO Distributions Records:'||ln_total_dist_records);
   	log_message(g_out,'Total PO Distributions Records loaded:'||(ln_total_dist_records-ln_failed_dist_records));
    	log_message(g_out,'Failed PO Distributions Records while loading:'||ln_failed_dist_records);       	

	commit;


   EXCEPTION

   WHEN OTHERS THEN
   log_message(g_log,'Exception in procedure load. Error Message:'||SQLERRM);
   rollback;

   END load;


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
  --   This procedure will return code combination id
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
  -- Procedure: update_ignored_records
  -- =============================================================================
  --   This procedure will mark ignored records as X in po headers table.
  -- =============================================================================
   PROCEDURE update_ignored_records
   IS
	   cursor po_header_cur
	   is
	   select * 
	   from xxconv.xxpo_po_headers_all_res_stg
	   where process_flag = g_process_flag
		 and status_flag in ( g_new);   
		 
	  
	  
	  cursor po_line_loc_cur(pin_parent_record_id IN NUMBER)
	  is
	  select *
	  from xxconv.xxpo_po_line_loc_all_res_stg
	  where parent_record_id = pin_parent_record_id;
	  
	  cursor po_dist_cur(pin_parent_record_id IN NUMBER
						,pin_po_header_id	  IN NUMBER
						,pin_po_line_id	  IN NUMBER
						,pin_line_location_id	  IN NUMBER
						 )
	  is
	  select *
	  from xxconv.xxpo_po_dist_all_res_stg
	  where parent_record_id = pin_parent_record_id
	    and po_header_id = pin_po_header_id
		and po_line_id = pin_po_line_id
		and line_location_id = pin_line_location_id;	

	  cursor po_lines_cur(pin_parent_record_id IN NUMBER)
	  is
	  select *
	  from xxconv.xxpo_po_lines_all_res_stg
	  where parent_record_id = pin_parent_record_id;		  
	  
	  
	  ln_distinct_plant_count 	number default 0;
	  lv_po_line_open_flag			varchar2(1);
	  ln_po_line_open_count			number;
	  lv_project_valid_flag			varchar2(1);
	  lv_project_invalid_count		number;
	  lv_error_message				varchar2(1000);
	  ln_quantity					number;
	  ln_invoice_count				number;
	  lv_total_po_line_open_count	number;
   begin
	--Loop for po header record
     for po_header_rec in po_header_cur
     loop
		log_message(g_log,'Header: Record Id:'||po_header_rec.record_id||' po_header_id:'||po_header_rec.po_header_id);
		
		--Resetting the variables.
		ln_distinct_plant_count :=  0;
		ln_po_line_open_count	:= 0;
		lv_project_invalid_count := 0;
		
		--Loop through all the line location records to determine the number of lines with quantity satisfying the condition of Open PO.
		--If at line location level quantities is not open, then update the status of line location and distribution as X i.e. ignored.
		--If at line location level quantities is open , then for that line location check at distribution level if there is project info , and update flag at distribution level to indicate if project info is valid or not.
		for po_line_loc_rec in po_line_loc_cur(po_header_rec.record_id)
		loop
			lv_po_line_open_flag			:= 'N';
			ln_quantity						:= 0;
			ln_invoice_count				:= 0;
			
			log_message(g_log,'Line: Record Id:'||po_line_loc_rec.record_id||' match_option:'||po_line_loc_rec.match_option||
							  ' quantity:'||po_line_loc_rec.quantity||' quantity_received:'||po_line_loc_rec.quantity_received||' quantity_billed:'||po_line_loc_rec.quantity_billed);
			if po_line_loc_rec.match_option = 'P'  then
				if(po_line_loc_rec.quantity <> po_line_loc_rec.quantity_billed) then
					lv_po_line_open_flag := 'Y';
					ln_quantity := abs(po_line_loc_rec.quantity - po_line_loc_rec.quantity_billed);
				else
					select count(*)
					 into ln_invoice_count
					from XXAP_INVC_LINES_INTFC_STG lines
					    ,XXAP_INVC_INTFC_STG header
					where lines.po_header_id = po_line_loc_rec.po_header_id
					 and lines.po_line_id = po_line_loc_rec.po_line_id
					 and lines.po_line_location_id = po_line_loc_rec.line_location_id
					 and lines.record_id = header.record_id
					 and header.is_invc_cancelled = 'Y';
					 
					 if ln_invoice_count > 0 then
						lv_po_line_open_flag := 'Y';
						ln_quantity			 := 0.0001;
					 end if;

				end if;
			
			elsif po_line_loc_rec.match_option = 'R'  then
				if(po_line_loc_rec.quantity <> po_line_loc_rec.quantity_received or po_line_loc_rec.quantity <> po_line_loc_rec.quantity_billed) then
					lv_po_line_open_flag := 'Y';
					ln_quantity := po_line_loc_rec.quantity_received;
				end if;
			end if;
			log_message(g_log,'lv_po_line_open_flag:'||lv_po_line_open_flag);
			--If po line loc is not open then mark its status as ignored and also of the distribution under that line location.
			if lv_po_line_open_flag = 'N' then
				update xxconv.xxpo_po_line_loc_all_res_stg 
				   set status_flag = g_ignored
					,error_message = 'Quantities not eligible for conversion.'
				   ,po_line_open_flag = lv_po_line_open_flag 
				   ,last_update_date = sysdate
				  ,last_updated_by = g_user_id
				  ,last_update_login = g_login_id
				  ,request_id = g_request_id
			    where record_id = po_line_loc_rec.record_id;
				commit;
				
				update xxconv.xxpo_po_dist_all_res_stg
				  set status_flag = g_ignored
					,error_message = 'Quantities not eligible for conversion.'
				   ,last_update_date = sysdate
				  ,last_updated_by = g_user_id
				  ,last_update_login = g_login_id
				  ,request_id = g_request_id
			   where parent_record_id = po_header_rec.record_id
			     and po_header_id = po_line_loc_rec.po_header_id
				 and po_line_id = po_line_loc_rec.po_line_id
				 and line_location_id = po_line_loc_rec.line_location_id;
				 commit;
				 
			elsif lv_po_line_open_flag = 'Y' then
				update xxconv.xxpo_po_line_loc_all_res_stg 
				   set po_line_open_flag = lv_po_line_open_flag 
					   ,new_quantity = ln_quantity
					   ,last_update_date = sysdate
					  ,last_updated_by = g_user_id
					  ,last_update_login = g_login_id
					  ,request_id = g_request_id
			    where record_id = po_line_loc_rec.record_id;
					commit;
							--Loop through all the distribution records and determine the validity of project information provided at the PO Level.
							for po_dist_rec in po_dist_cur(po_header_rec.record_id
														  ,po_line_loc_rec.po_header_id
														  ,po_line_loc_rec.po_line_id
														  ,po_line_loc_rec.line_location_id
														  )
							loop
							lv_project_valid_flag	:= 'Y';
							lv_error_message		:= null;	
							log_message(g_log,'Distribution: Record Id:'||po_dist_rec.record_id);
								if po_dist_rec.project_id is not null then
									begin
										select new_project.project_id,new_task.task_id
										  into po_dist_rec.new_project_id,po_dist_rec.new_task_id
										  from pa_projects_all old_project
											  ,pa_projects_all new_project
											  ,pa_tasks old_task
											  ,pa_tasks new_task
										where old_project.project_id = po_dist_rec.project_id
										  and old_project.segment1 = new_project.attribute1
										  and old_task.task_id = po_dist_rec.task_id
										  and old_task.task_number = new_task.task_number
										  AND new_project.attribute_category = 'Eaton'
										  and new_task.project_id = new_project.project_id;
										  
								  
									 exception
									 when no_data_found then
										lv_project_valid_flag := 'N';
										lv_error_message 	  := 'Converted Project was not found.';
									 when others then
										lv_project_valid_flag := 'N';
										lv_error_message 	  := 'Exception while determining converted project. Error Message:'||SQLERRM;
									 end;
									 
								end if;
								
								log_message(g_log,'lv_project_valid_flag:'||lv_project_valid_flag||' lv_error_message:'||lv_error_message);
									update xxconv.xxpo_po_dist_all_res_stg 
									   set new_project_id = po_dist_rec.new_project_id 
										  ,new_task_id = po_dist_rec.new_task_id  
										  ,project_info_valid_flag = lv_project_valid_flag
										  ,status_flag = DECODE(lv_project_valid_flag , 'N', g_ignored,status_flag )
										  ,error_message = DECODE(lv_project_valid_flag , 'N', lv_error_message,error_message )
										   ,last_update_date = sysdate
										  ,last_updated_by = g_user_id
										  ,last_update_login = g_login_id
										  ,request_id = g_request_id
									where record_id = po_dist_rec.record_id;
									commit;
									
									
							end loop;--end of distribution loop
			end if;
		end loop;
		
		--After completion of above loop we have completely marking al the lines which are open and all the distribution lines which are having valid project.
		--If for a given po_header_id there is no open po line then we should exclude it , if it has atleast one open line then it is eligible for conversion based on below mentioned conditions.
		--If for any of the open line if the project info is invalid or if there is multiple plant number at distribution level of open po line then entire po needs to be ignored as it is invalid data.
		
		
		select count(*)
		 into ln_po_line_open_count
		from xxconv.xxpo_po_line_loc_all_res_stg
		where parent_record_id = po_header_rec.record_id
		  and po_line_open_flag = 'Y';
		
		log_message(g_log,'No of open po for header_id:'||po_header_rec.po_header_id||' is '||ln_po_line_open_count);
		--If there are no lines with po as open then we need to mark the status of header as X (Ignored) as there are no open PO lines.
		--Same status has to be updated to all the lines , line locations and distributions under that po.
		if(ln_po_line_open_count = 0) then
			update xxconv.xxpo_po_headers_all_res_stg 
			set status_flag = g_ignored 
			   , error_message = 'Quantities not eligible for conversion.' 
			   ,last_update_date = sysdate
			  ,last_updated_by = g_user_id
			  ,last_update_login = g_login_id
			  ,request_id = g_request_id
		  where record_id = po_header_rec.record_id;

		  update xxconv.xxpo_po_lines_all_res_stg 
		     set status_flag = g_ignored 
			   , error_message = 'Quantities not eligible for conversion.' 
			   ,last_update_date = sysdate
			  ,last_updated_by = g_user_id
			  ,last_update_login = g_login_id
			  ,request_id = g_request_id
		  where parent_record_id = po_header_rec.record_id;
		  
		  commit;
		else
			
			--For all the open lines get the count of distribution lines where project info is invalid.
			select count(*)
			  into lv_project_invalid_count
			  from xxconv.xxpo_po_line_loc_all_res_stg line_loc
			      ,xxconv.xxpo_po_dist_all_res_stg dist
		     where line_loc.parent_record_id = po_header_rec.record_id
		       and line_loc.po_line_open_flag = 'Y'
			   and dist.po_header_id = line_loc.po_header_id
			   and dist.po_line_id = line_loc.po_line_id
			   and dist.line_location_id = line_loc.line_location_id
			   and project_info_valid_flag = 'N';
			   
			select count(distinct(segment2))
			  into ln_distinct_plant_count
			  from xxconv.xxpo_po_line_loc_all_res_stg line_loc
			      ,xxconv.xxpo_po_dist_all_res_stg dist
				  ,gl_code_combinations_kfv gl
		     where line_loc.parent_record_id = po_header_rec.record_id
		       and line_loc.po_line_open_flag = 'Y'
			   and dist.po_header_id = line_loc.po_header_id
			   and dist.po_line_id = line_loc.po_line_id
			   and dist.line_location_id = line_loc.line_location_id
			   and dist.charge_account = gl.code_combination_id;
			   
			log_message(g_log,'lv_project_invalid_count: '||lv_project_invalid_count||' ln_distinct_plant_count:'||ln_distinct_plant_count);
			
			if(lv_project_invalid_count > 0 or ln_distinct_plant_count > 1) then
				update xxconv.xxpo_po_headers_all_res_stg 
				  set status_flag = g_ignored 
				    , error_message = error_message||'~'||'One of the project details is invalid or distribution belongs to multiple plant' 
				   ,last_update_date = sysdate
				  ,last_updated_by = g_user_id
				  ,last_update_login = g_login_id
				  ,request_id = g_request_id
				where record_id = po_header_rec.record_id;
				
				update xxconv.xxpo_po_lines_all_res_stg 
				   set status_flag = g_ignored 
				    --, error_message =  error_message||'~'||'One of the project details is invalid or distribution belongs to multiple plant' 
				   ,last_update_date = sysdate
				  ,last_updated_by = g_user_id
				  ,last_update_login = g_login_id
				  ,request_id = g_request_id
				where parent_record_id = po_header_rec.record_id;
				
				update xxconv.xxpo_po_line_loc_all_res_stg 
				   set status_flag = g_ignored 
				   --, error_message =  error_message||'~'||'One of the project details is invalid or distribution belongs to multiple plant' 
				   ,last_update_date = sysdate
				  ,last_updated_by = g_user_id
				  ,last_update_login = g_login_id
				  ,request_id = g_request_id
				   where parent_record_id = po_header_rec.record_id;
				   
				update xxconv.xxpo_po_dist_all_res_stg 
				set status_flag = g_ignored 
				  --, error_message =  error_message||'~'||'One of the project details is invalid or distribution belongs to multiple plant' 
				   ,last_update_date = sysdate
				  ,last_updated_by = g_user_id
				  ,last_update_login = g_login_id
				  ,request_id = g_request_id
				  where parent_record_id = po_header_rec.record_id;
				  
				  commit;
			else
				
				--If there are no errors then loop through all the lines to determine which lines are eligible for conversion based
				--on line location level.
				for po_line_rec in (select * 
									from xxconv.xxpo_po_lines_all_res_stg lines
									where lines.parent_record_id = po_header_rec.record_id
									)
				loop
					--Resetting the variables
					lv_total_po_line_open_count	:= 0;
					
					log_message(g_log,'Lines quantity evaluation: record_id:'||po_line_rec.record_id||' po_line_id:'||po_line_rec.po_line_id);
					select count(*)
					  into lv_total_po_line_open_count
					 from xxconv.xxpo_po_line_loc_all_res_stg line_loc 
					 where line_loc.parent_record_id = po_line_rec.parent_record_id
					  and line_loc.po_header_id = po_line_rec.po_header_id
					  and line_loc.po_line_id = po_line_rec.po_line_id
					  and line_loc.po_line_open_flag = 'Y'
					  and line_loc.status_flag = g_new;
					
					log_message(g_log,'Eligible line location for this po line:'||lv_total_po_line_open_count);
					
					if lv_total_po_line_open_count = 0 then
						update xxconv.xxpo_po_lines_all_res_stg line 
						  set status_flag = g_ignored
							 , error_message = 'Quantities not eligible for conversion.' 
							 ,last_update_date = sysdate
							 ,last_updated_by = g_user_id
							 ,last_update_login = g_login_id
							 ,request_id = g_request_id
						where line.record_id = po_line_rec.record_id;
						commit;
					else
					  update xxconv.xxpo_po_lines_all_res_stg line 
						  set new_quantity = ( select sum(NVL(new_quantity,0)) 
											from xxconv.xxpo_po_line_loc_all_res_stg line_loc 
											 where line_loc.parent_record_id = po_line_rec.parent_record_id
											  and line_loc.po_header_id = po_line_rec.po_header_id
											  and line_loc.po_line_id = po_line_rec.po_line_id
											  and line_loc.po_line_open_flag = 'Y'
											  and line_loc.status_flag = g_new
											  )
							 ,last_update_date = sysdate
							 ,last_updated_by = g_user_id
							 ,last_update_login = g_login_id
							 ,request_id = g_request_id
						where line.record_id = po_line_rec.record_id;
						commit;
					end if;
					
				end loop;
				
			end if;
		end if;
	 end loop;--end of header loop
	 commit;
   EXCEPTION
   WHEN OTHERS THEN
   log_message(g_log,'Exception in procedure update_ignored_records. Error Message:'||SQLERRM);
   
   END update_ignored_records;
 
   
  
  -- ========================
  -- Procedure: validate
  -- =============================================================================
  --   This procedure will log the errors in the error report using error
  --   framework
  -- =============================================================================

   PROCEDURE validate
   IS


   lv_record_status    varchar2(1);
   lv_error_message    varchar2(1000);
   
   cursor po_header_cur
   is
   select * 
   from xxconv.xxpo_po_headers_all_res_stg
   where process_flag = g_process_flag
     and status_flag in ( g_new,g_error);
     

   --For all the child cursors we pick new , errored as well as validated records because if there is any 
   --issue at any of the child level then error is stamped only at header level.
   --Thus if we restrict the records at child cursor then failed records under that child record will never be picked.
   --But we will skip validation logic for already validated records.
   cursor po_line_cur(pin_parentrecord_id in number
   		     ,pin_po_header_id    in number
   		      )
   is
   select * 
   from xxconv.XXPO_PO_LINES_ALL_RES_STG
   where status_flag in ( g_new,g_error,g_validated)
     and parent_record_id = pin_parentrecord_id
     and po_header_id = pin_po_header_id;
     
   cursor po_line_loc_cur(pin_parentrecord_id in number
   			,pin_po_header_id    in number
   			,pin_po_line_id    in number
   			 )
   is
   select * 
   from xxconv.XXPO_PO_LINE_LOC_ALL_RES_STG
   where status_flag in ( g_new,g_error,g_validated)
     and parent_record_id = pin_parentrecord_id
     and po_header_id = pin_po_header_id
     and po_line_id = pin_po_line_id; 
     
   cursor po_dist_cur(pin_parentrecord_id in number
   			,pin_po_header_id    in number
   			,pin_po_line_id    in number
   			,pin_po_line_loc_id    in number
   			 )
   is
   select * 
   from xxconv.XXPO_PO_DIST_ALL_RES_STG
   where status_flag in ( g_new,g_error,g_validated)
     and parent_record_id = pin_parentrecord_id
     and po_header_id = pin_po_header_id
     and po_line_id = pin_po_line_id
     and line_location_id = pin_po_line_loc_id;   
     
   --Variables for po header status flag and error message.
   lv_header_status_flag  varchar2(1) := 'S';
   lv_header_error_message	varchar2(5000);
   
   --Variables for po lines status flag and error message.
   lv_line_status_flag  varchar2(1) := 'S';
   lv_line_error_message	varchar2(5000);
   
   --Variables for po line loc status flag and error message.
   lv_line_loc_status_flag  varchar2(1) := 'S';
   lv_line_loc_error_message	varchar2(5000);
   
   --Variables for po distribution status flag and error message.
   lv_dist_status_flag  varchar2(1) := 'S';
   lv_dist_error_message	varchar2(5000);
   
   --this flag will indicate whether there is validation error in any of the line.
   --If this flag is set to Y then we need to fail the record at header line since any of the record in line or line loc or distribution
   --has validation error.
   lv_line_error_flag		varchar2(1) := 'N';
   lv_line_loc_error_flag	varchar2(1) := 'N';
   lv_dist_error_flag		varchar2(1) := 'N';
   
   --Header validation related variables.
   ln_agent_id		number;
   lv_employee_number varchar2(30);
   
   --Line validation related variables.
   ln_line_loc_ship_to_loc_id	number default 0;
   lv_destination_type_code		varchar2(50);
   
   --Distribution related validation variables.
   ln_charge_accts_id			number;
   ln_accural_accts_id			number;
   ln_variance_accts_id			number;
   
   
   --variables to handle stats.
   ln_total_record_count		number default 0;
   ln_total_failed_count		number default 0;
   ln_ignored_counts			number default 0;
   
   cursor header_errors(piv_status_flag IN VARCHAR2)
   is
   select error_message,count(*) count_val
   from xxconv.xxpo_po_headers_all_res_stg 
  where request_id = g_request_id
   and status_flag = piv_status_flag
   group by error_message;   
   
   BEGIN
     log_message(g_log,'Validate Begin');
	 
	 update_ignored_records;
     
	 
     --Loop for po header record
     for po_header_rec in po_header_cur
     loop
	
	--Resetting error variables 
	 lv_header_status_flag  := 'S';
     lv_header_error_message := null;

	 ln_agent_id	 := 0;
	 lv_employee_number := null;
	 
	 --Resetting the error indicator at the child level
     lv_line_error_flag		 := 'N';
	 lv_line_loc_error_flag	 := 'N';	 
	 lv_dist_error_flag		 := 'N';   

     	 
     
	log_message(g_log,'PO Header Record_id:'||po_header_rec.record_id||' po_header_id:'||po_header_rec.po_header_id||
					  ' Status of header record:'||po_header_rec.status_flag);
	log_message(g_log,'');

	--All the core validation logic for header should come within this block.
	--Header: Validation block begins.
	
	if po_header_rec.status_flag IN (g_new,g_error) then
		BEGIN
			
			--Make sure that for any future enhancement onto this package all the validation code is written only within the block where "Validation of header data begin" 
			--and "Validation of header data end" is mentioned. THis is to ensure proper error handling.
			log_message(g_log,'Validation of header data begin');
		
			--Rate type derivation 
			--If rate_type is anything other than User then set rate type to Corporate and clear rate and rate date fields
			if po_header_rec.rate_type is not null and po_header_rec.rate_type <> 'User' then
				po_header_rec.new_rate_type := 'Corporate';
				po_header_rec.new_rate := null;
				po_header_rec.new_rate_date := null;
			else
				po_header_rec.new_rate_type := po_header_rec.rate_type;
				po_header_rec.new_rate := po_header_rec.rate;
				po_header_rec.new_rate_date := po_header_rec.rate_date;
			end if;
			
			--Agent Id validation and derivation.
			if po_header_rec.agent_id is not null then
				BEGIN
					/*select person_id,employee_number
					 into po_header_rec.new_agent_id,lv_employee_number
					 from per_all_people_f ppf
					where nvl(current_employee_flag, 'N') = 'Y'
					  and sysdate between ppf.effective_start_date and ppf.effective_end_date
					  and person_id = po_header_rec.agent_id;*/
					  
					select ppf.person_id,ppf.employee_number
					  into po_header_rec.new_agent_id,lv_employee_number
					  from per_person_types_tl      ttl,
						   per_person_types         typ,
						   per_person_type_usages_f ptu,
						   per_all_people_f         ppf,
						   po_agents                pa
					 where ttl.language = userenv('LANG')
					   and ttl.person_type_id = typ.person_type_id
					   and typ.system_person_type in ('EMP', 'CWK')
					   and typ.person_type_id = ptu.person_type_id
					   and sysdate between ptu.effective_start_date and ptu.effective_end_date
					   and sysdate between ppf.effective_start_date and ppf.effective_end_date
					   and ptu.person_id = ppf.person_id
					   and ppf.person_id = po_header_rec.agent_id
					   and nvl(current_employee_flag, 'N') = 'Y'
					   and ppf.person_id = pa.agent_id
					   and trunc(sysdate) between trunc(pa.start_date_active) and trunc(nvl(pa.end_date_active, sysdate));
					   
				EXCEPTION
				when no_data_found then
					--If current agent is not active then derive agent id value from default lookup for target plant value. 
					begin
						SELECT ppf.person_id
						  into po_header_rec.new_agent_id
						FROM per_person_types_tl ttl,
						  per_person_types typ,
						  per_person_type_usages_f ptu,
						  per_all_people_f ppf,
						  po_agents pa
						  ,fnd_lookup_values flv
						WHERE ttl.language          = userenv('LANG')
						AND ttl.person_type_id      = typ.person_type_id
						AND typ.system_person_type IN ('EMP', 'CWK')
						AND typ.person_type_id      = ptu.person_type_id
						AND sysdate BETWEEN ptu.effective_start_date AND ptu.effective_end_date
						AND sysdate BETWEEN ppf.effective_start_date AND ppf.effective_end_date
						AND ptu.person_id                   = ppf.person_id
						AND ppf.employee_number             = flv.DESCRIPTION--p_in_agent_emp_no
						AND NVL(current_employee_flag, 'N') = 'Y'
						AND ppf.person_id                   = pa.agent_id
						AND TRUNC(sysdate) BETWEEN TRUNC(pa.start_date_active) AND TRUNC(NVL(pa.end_date_active, sysdate))
						and flv.LOOKUP_TYPE = 'ETN_LEDGER_GENERIC_BUYER'
						and flv.LANGUAGE = userenv('LANG')
						and flv.MEANING = po_header_rec.destination_plant_name
						AND TRUNC(sysdate) BETWEEN TRUNC(flv.start_date_active) AND TRUNC(NVL(flv.end_date_active, sysdate));  
					
					exception
					when no_data_found then
						lv_header_status_flag  := 'E';
						lv_header_error_message := lv_header_error_message||'~'||'Default Agent cannot be derived.';			 
					when others then
						lv_header_status_flag  := 'E';
						lv_header_error_message := lv_header_error_message||'~'||'Exception while deriving default agent. Error Message:'||SQLERRM;			 
					end;
				end;
				log_message(g_log,'Agent_Id derivation. ln_agent_id:'||ln_agent_id||' lv_employee_number:'||lv_employee_number);
			end if;--end of agent id validation if.
			
			begin
				log_message(g_log,'Validating vendor_site_id. vendor_id:'||po_header_rec.vendor_id||' vendor_site_id:'||po_header_rec.vendor_site_id);
				log_message(g_log,'Validating vendor_site_id. source_org_id:'||po_header_rec.source_org_id||' destination_org_id:'||po_header_rec.destination_org_id);
				select new_vendor_site_id
				  into po_header_rec.new_vendor_site_id
				  from xxconv.xxap_supplier_site_res_stg site_stg
				 where site_stg.vendor_id = po_header_rec.vendor_id
				  and site_stg.vendor_site_id = po_header_rec.vendor_site_id
				  and status_flag in ('C','P')
				  and nvl(inactive_date,sysdate) >= sysdate
				  and NVL(purchasing_site_flag,'N') = 'Y'
				  and rownum = 1;--this will not happen that we will process same combination again and again, but adding it to ensure PO has no validation issue.
			exception
			when no_data_found then
				begin
					select sups1.vendor_site_id
					  into po_header_rec.new_vendor_site_id
					  from ap_supplier_sites_all sups
						   ,ap_supplier_sites_all sups1
					 where sups.vendor_id = po_header_rec.vendor_id
					   and sups.vendor_site_id = po_header_rec.vendor_site_id
					   and sups.org_id = po_header_rec.source_org_id
					   and sups.vendor_site_code = sups1.vendor_site_code
					   and sups1.vendor_id = po_header_rec.vendor_id
					   and sups1.org_id = po_header_rec.destination_org_id
					   and nvl(sups1.inactive_date,sysdate) >= sysdate
					   and NVL(sups1.purchasing_site_flag,'N') = 'Y';
				exception
				when no_data_found then
					lv_header_status_flag  := 'E';
					lv_header_error_message := lv_header_error_message||'~'||'Vendor Site not found in Vendor site conversion.';			 
				when others then
					lv_header_status_flag  := 'E';
					lv_header_error_message := lv_header_error_message||'~'||'Exception while deriving vendor_site_id from base table. Error Message:'||SQLERRM;			 
				end;
			when others then
					lv_header_status_flag  := 'E';
					lv_header_error_message := lv_header_error_message||'~'||'Exception while deriving vendor_site_id. Error Message:'||SQLERRM;			 
			end;
			
			--Vendor Contact Validation
			if po_header_rec.new_vendor_site_id is not null then
				begin
					select vendor_contact_id
					  into po_header_rec.vendor_contact_id
					 from po_vendor_contacts
					 where vendor_contact_id = po_header_rec.vendor_contact_id
					  and vendor_site_id = po_header_rec.new_vendor_site_id;
				exception
					when no_data_found then
					lv_header_status_flag  := 'E';
					lv_header_error_message := lv_header_error_message||'~'||'Vendor Contact is invalid';			 
					when others then
					lv_header_status_flag  := 'E';
					lv_header_error_message := lv_header_error_message||'~'||'Exception while deriving vendor_contact_id. Error Message:'||SQLERRM;			 
				end;
			end if;
			
			--Make sure that for any future enhancement onto this package all the validation code is written only within the block where "Validation of header data begin" 
			--and "Validation of header data end" is mentioned. THis is to ensure proper error handling.
			log_message(g_log,'Validation of header data end');
		EXCEPTION
		WHEN OTHERS THEN
		 lv_header_status_flag  := 'E';
		 lv_header_error_message := lv_header_error_message||
								  '~'||'Exception while validating header record. Record Id:'||po_header_rec.record_id||
								  ' Error Message:'||SQLERRM;
		END;
	end if;
	
	--Header: Validation block ends.	
	
	--Loop for po line record
	for po_line_rec in po_line_cur(po_header_rec.record_id
				      ,po_header_rec.po_header_id
				      )
	loop

		--Resetting error variables 
		lv_line_status_flag  := 'S';
		lv_line_error_message := null;	
		
		--Resetting local variables.
		ln_line_loc_ship_to_loc_id := 0;
		lv_destination_type_code	:= null;
		
		
		log_message(g_log,'PO Lines Record_id:'||po_line_rec.record_id||
			    ' parent_record_id:'||po_line_rec.parent_record_id||
			    ' po_header_id:'||po_line_rec.po_header_id||
			    ' po_line_id:'||po_line_rec.po_line_id||
				' Status of line record:'||po_line_rec.status_flag);
		log_message(g_log,'');
		
		--All the core validation logic for line should come within this block.
		--Line Validation block begins.
		if po_line_rec.status_flag IN (g_new,g_error) then		
			BEGIN

			--Make sure that for any future enhancement onto this package all the validation code is written only within the block where "Validation of line data begin" 
			--and "Validation of line data end" is mentioned. THis is to ensure proper error handling.
				log_message(g_log,'Validation of line data begin');
				
				--Validate the item and derive new item number.
				begin
					select distinct destination_type_code
					 into lv_destination_type_code
					from xxconv.xxpo_po_dist_all_res_stg
					where parent_record_id = po_header_rec.record_id
					 and po_header_id = po_line_rec.po_header_id
					 and po_line_id = po_line_rec.po_line_id;
					 
					 log_message(g_log,'lv_destination_type_code:'||lv_destination_type_code);
					 IF lv_destination_type_code = 'INVENTORY' and po_line_rec.item is null then
						po_line_rec.new_item := 'INVENTORY';
					 else
						po_line_rec.new_item := po_line_rec.item;
					 end if;
				exception
				when others then
				 lv_line_status_flag  := 'E';
				 lv_line_error_message := lv_line_error_message||'~Exception while deriving item from distribution. Error Message:'||SQLERRM;
				end;--End of item number validation.
				
				--Category ID validation.
				--Check if category id is valid or not.
				if po_line_rec.category_id is not null then
					begin
						select category_id 
						 into po_line_rec.category_id
						 from mtl_categories_b_kfv cat
						 where cat.category_id = po_line_rec.category_id
						   and NVL(enabled_flag,'Y') = 'Y'
						   and sysdate between NVL(cat.start_date_active,sysdate) and NVL(cat.end_date_active,sysdate);
					exception
					when others then
					 lv_line_status_flag  := 'E';
					 lv_line_error_message := lv_line_error_message||'~Category Id is either invalid or inactive. Error Message:'||SQLERRM;
					end;
				end if;
				
				--Fetch values from po_line_locations_all
				--Assumption is all the values for line location would be same and thus we need to data for first record.
				begin
					select invoice_close_tolerance,receive_close_tolerance,days_early_receipt_allowed
						  ,days_late_receipt_allowed,receiving_routing_id,qty_rcv_tolerance,promised_date
						  ,tax_user_override_flag,note_to_receiver,consigned_flag,drop_ship_flag,taxable_flag
						  ,ship_to_organization_id
					 into po_line_rec.invoice_close_tolerance,po_line_rec.receive_close_tolerance,po_line_rec.days_early_receipt_allowed
						 ,po_line_rec.days_late_receipt_allowed,po_line_rec.receiving_routing_id,po_line_rec.qty_rcv_tolerance
						 ,po_line_rec.promised_date,po_line_rec.tax_user_override_flag,po_line_rec.note_to_receiver,po_line_rec.consigned_flag
						 ,po_line_rec.drop_ship_flag,po_line_rec.tax_status_indicator,po_line_rec.ship_to_organization_id
					from xxconv.xxpo_po_line_loc_all_res_stg
					where parent_record_id = po_header_rec.record_id
					 and po_header_id = po_line_rec.po_header_id
					 and po_line_id = po_line_rec.po_line_id
					 and rownum = 1;
				exception
				when others then
					 lv_line_status_flag  := 'E';
					 lv_line_error_message := lv_line_error_message||'~Exception while fetching values from po_line_locations_all. Error Message:'||SQLERRM;
				end;
				
				--Logic to derive new item and new ship to organization_id
				--Logic to derive new new ship to organization id is as below
				--
				--	1. Get the old ship to organization id
				--	2. Get organization name from org_organization_definitions
				--	3. XXETN_EIC_INV_ORG_MAPPING lookup save old organization name and new organization name in below format.
				--	   lookup code saves old plant number
				--	   meaning saves old organization name
				--	   description saves new plant number
				--	   tag saves new organization name
				
				if po_line_rec.ship_to_organization_id is not null then
					begin
						select ood1.organization_id
						 into po_line_rec.new_ship_to_organization_id
						 from fnd_lookup_values flv
							,org_organization_definitions ood
							,org_organization_definitions ood1
						 where flv.lookup_type = 'XXETN_EIC_INV_ORG_MAPPING'
						 and flv.LANGUAGE = userenv('LANG')
						 and ood.organization_id =  po_line_rec.ship_to_organization_id
						 and substr(flv.lookup_code,instr(flv.lookup_code,'|')+1) = po_line_rec.source_plant_name
						 and substr(flv.meaning,instr(flv.meaning,'|')+1) = ood.organization_name
						 and flv.enabled_flag = 'Y'
						 AND TRUNC(sysdate) BETWEEN TRUNC(flv.start_date_active) AND TRUNC(NVL(flv.end_date_active, sysdate))
						 and substr(flv.description,instr(flv.description,'|')+1) = po_line_rec.destination_plant_name
						 and substr(flv.tag,instr(flv.tag,'|')+1) = ood1.organization_name;
						 log_message(g_log,'po_line_rec.new_ship_to_organization_id:'||po_line_rec.new_ship_to_organization_id);
					 exception
					 when others then
						 lv_line_status_flag  := 'E';
						 lv_line_error_message := lv_line_error_message||'~Exception while deriving new_ship_to_organization_id. Error Message:'||SQLERRM;
					 end;
					 
				  end if;
				  
				  if po_line_rec.new_ship_to_organization_id is not null and po_line_rec.item_id is not null then
					begin
						select inventory_item_id
						 into po_line_rec.new_item_id
						from mtl_system_items_b item
						where item.inventory_item_id = po_line_rec.item_id
						  and organization_id = po_line_rec.new_ship_to_organization_id
						  and NVL(enabled_flag,'N') = 'Y'
						  and sysdate between NVL(item.start_date_active,sysdate) and NVL(item.end_date_active,sysdate);
					exception
					when others then
						 lv_line_status_flag  := 'E';
						 lv_line_error_message := lv_line_error_message||'~Item is not assigned to target organization.';
					end;
				  end if;
				  

			--Make sure that for any future enhancement onto this package all the validation code is written only within the block where "Validation of line data begin" 
			--and "Validation of line data end" is mentioned. THis is to ensure proper error handling.
				log_message(g_log,'Validation of line data end');
			EXCEPTION
			WHEN OTHERS THEN
			 lv_line_status_flag  := 'E';
			 lv_line_error_message := lv_line_error_message||
									  '~'||'Exception while validating line record. Record Id:'||po_line_rec.record_id||
									  ' Error Message:'||SQLERRM;
			END;
		end if;
		--Line Validation block ends.			
		
		--Loop for po line loc record
		for po_line_loc_rec in po_line_loc_cur(po_header_rec.record_id
						      ,po_line_rec.po_header_id
						      ,po_line_rec.po_line_id
						      )
		loop
		
		--Resetting error variables 
		 lv_line_loc_status_flag  := 'S';
		 lv_line_loc_error_message := null;		

		 log_message(g_log,'PO Line Loc Record_id:'||po_line_loc_rec.record_id||
		 	     ' parent_record_id:'||po_line_loc_rec.parent_record_id||
		 	     ' po_header_id:'||po_line_loc_rec.po_header_id||
		 	     ' po_line_id:'||po_line_loc_rec.po_line_id||
		 	     ' line_location_id:'||po_line_loc_rec.line_location_id||
				 ' Status of line location record:'||po_line_loc_rec.status_flag);
		 log_message(g_log,'');

			--All the core validation logic for line location should come within this block.
			--Line Location: Validation block begins.
			if po_line_loc_rec.status_flag IN (g_new,g_error) then				
				BEGIN
					--Make sure that for any future enhancement onto this package all the validation code is written only within the block where "Validation for Line location begin" 
					--and "Validation for Line location end" is mentioned. THis is to ensure proper error handling.
					log_message(g_log,'Validation for Line location begin');
					--As such there is no validation currently for line location level.Still the structure as is for future use. Since no 
					--validation is currently in place , result of validation flag would be S.i.e. no error.
					--Only derivation is for new ship_to_organization_id which was already taken care at line level.
					null;
					
					--Make sure that for any future enhancement onto this package all the validation code is written only within the block where "Validation for Line location begin" 
					--and "Validation for Line location end" is mentioned. THis is to ensure proper error handling.
					log_message(g_log,'Validation for Line location end');
				EXCEPTION
				WHEN OTHERS THEN
				 lv_line_loc_status_flag  := 'E';
				 lv_line_loc_error_message := lv_line_loc_error_message||
										  '~'||'Exception while validating line location record. Record Id:'||po_line_loc_rec.record_id||
										  ' Error Message:'||SQLERRM;
				END;
			end if;
			--Line Location: Validation block ends.			

			
			--Loop for po distribution record
			for po_dist_rec in po_dist_cur(po_header_rec.record_id
							  ,po_line_loc_rec.po_header_id
							  ,po_line_loc_rec.po_line_id
							  ,po_line_loc_rec.line_location_id
							  )
			loop
				--Resetting error variables 
				 lv_dist_status_flag  := 'S';
				 lv_dist_error_message := null;	

				--resetting variables.


				 log_message(g_log,'PO Dist Record_id:'||po_dist_rec.record_id||
				 	     ' parent_record_id:'||po_dist_rec.parent_record_id||
				 	     ' po_header_id:'||po_dist_rec.po_header_id||
					     ' po_line_id:'||po_dist_rec.po_line_id||
					     ' line_location_id:'||po_dist_rec.line_location_id||
					     ' po_distribution_id:'||po_dist_rec.po_distribution_id||
						 ' Status of distribution record:'||po_dist_rec.status_flag);	
				log_message(g_log,'');
				
				--All the core validation logic for distribution should come within this block.
				--Distribution Validation block begins.
				if po_dist_rec.status_flag IN (g_new,g_error) then					
					BEGIN
					--Make sure that for any future enhancement onto this package all the validation code is written only within the block where "Validation for Distribution begin" 
					--and "Validation for Distribution end" is mentioned. THis is to ensure proper error handling.
					log_message(g_log,'Validation for Distribution begin');
							--Rate type derivation 
							--If rate_type is anything other than User then set rate type to Corporate and clear rate and rate date fields
							if po_header_rec.new_rate_type = 'Corporate' then
								po_dist_rec.rate_date := null;
								po_dist_rec.rate := null;
							end if;
							
							--validate deliver_to_person_id to check if it is active.
							if po_dist_rec.deliver_to_person_id is not null then
								begin
									select person_id
									 into po_dist_rec.deliver_to_person_id
									 from per_all_people_f ppf
									where nvl(current_employee_flag, 'N') = 'Y'
									  and sysdate between ppf.effective_start_date and ppf.effective_end_date
									  and person_id = po_dist_rec.deliver_to_person_id;								
								exception
								when others then
									 lv_dist_status_flag  := 'E';
									 lv_dist_error_message := lv_dist_error_message||'~'||'Deliver_to_person_id is inactive';
								end;
							end if;
							
							--Validate destination_type_code
							if po_dist_rec.destination_type_code is not null and po_dist_rec.destination_type_code not in ('INVENTORY','EXPENSE') then
									 lv_dist_status_flag  := 'E';
									 lv_dist_error_message := lv_dist_error_message||'~'||'Invalid destination_type_code';
							end if;
							
							if po_dist_rec.destination_type_code is not null and po_dist_rec.destination_type_code = 'INVENTORY' then
								log_message(g_log,'Case1');
								po_dist_rec.NEW_DESTINATION_SUBINVENTORY := 'Raw Matl';
							else
								log_message(g_log,'Case2');
								po_dist_rec.NEW_DESTINATION_SUBINVENTORY := po_dist_rec.DESTINATION_SUBINVENTORY;
							end if;
							
							--Validate the expenditure_organization_id
							if po_dist_rec.expenditure_organization_id is not null then
								begin
									select hou2.organization_id
									  into po_dist_rec.new_expenditure_org_id
									  from hr_all_organization_units hou1
										  ,hr_all_organization_units hou2
									  where hou1.organization_id = po_dist_rec.expenditure_organization_id
									  and substr(hou1.name,1,instr(hou1.name,'_DNU')-1) = hou2.name;
								exception
								when others then
									 lv_dist_status_flag  := 'E';
									 lv_dist_error_message := lv_dist_error_message||'~'||'Exception while deriving new_expenditure_org_id. Error Message:'||SQLERRM;
								end;
							end if;
							
							--Derive new_attribute1 i.e. code_combination_id
							log_message(g_log,'attribute1:'||po_dist_rec.attribute1||' attribute2:'||po_dist_rec.attribute2||' attribute3:'||po_dist_rec.attribute3);
							IF po_dist_rec.attribute1 is null then
									 lv_dist_status_flag  := 'E';
									 lv_dist_error_message := lv_dist_error_message||'~'||'attribute1 is null';
							else
									--get_code_combination_segments(po_dist_rec.CHARGE_ACCOUNT,po_dist_rec.new_attribute1,ln_charge_accts_id);
									po_dist_rec.new_attribute1 := to_char(po_dist_rec.destination_org_number)||'.'||
													to_char(po_dist_rec.destination_plant_name)
													||substr ( po_dist_rec.attribute1 , instr(po_dist_rec.attribute1,'.',1,2));
							end if;
							
							--Derive new_attribute2 i.e. ACCRUAL_ACCOUNT_ID
							IF po_dist_rec.attribute2 is null then
									 lv_dist_status_flag  := 'E';
									 lv_dist_error_message := lv_dist_error_message||'~'||'attribute2 is null';
							else
									--get_code_combination_segments(po_dist_rec.ACCRUAL_ACCOUNT_ID,po_dist_rec.new_attribute2,ln_accural_accts_id);
									po_dist_rec.new_attribute2 := to_char(po_dist_rec.destination_org_number)||'.'||
													to_char(po_dist_rec.destination_plant_name)
													||substr ( po_dist_rec.attribute2 , instr(po_dist_rec.attribute2,'.',1,2));
							end if;

							--Derive new_attribute3 i.e. VARIANCE_ACCOUNT_ID
							IF po_dist_rec.attribute3 is null then
									 lv_dist_status_flag  := 'E';
									 lv_dist_error_message := lv_dist_error_message||'~'||'attribute2 is null';
							else
									--get_code_combination_segments(po_dist_rec.VARIANCE_ACCOUNT_ID,po_dist_rec.new_attribute3,ln_variance_accts_id);
									po_dist_rec.new_attribute3 := to_char(po_dist_rec.destination_org_number)||'.'||
													to_char(po_dist_rec.destination_plant_name)
													||substr ( po_dist_rec.attribute3 , instr(po_dist_rec.attribute3,'.',1,2));
							end if;
							
							log_message(g_log,'new_attribute1:'||po_dist_rec.new_attribute1||
											' new_attribute2:'||po_dist_rec.new_attribute2||' new_attribute3:'||po_dist_rec.new_attribute3);
							--Derive new_set_of_books_id
							begin
								select set_of_books_id
								  into po_dist_rec.new_set_of_books_id
								 from hr_operating_units 
								 where organization_id = po_dist_rec.destination_org_id;
							exception
							when others then
									 lv_dist_status_flag  := 'E';
									 lv_dist_error_message := lv_dist_error_message||'~'||'Exception while deriving set of books id. Error Message.'||SQLERRM;
							end;
							
					--Make sure that for any future enhancement onto this package all the validation code is written only within the block where "Validation for Distribution begin" 
					--and "Validation for Distribution end" is mentioned. THis is to ensure proper error handling.
					log_message(g_log,'Validation for Distribution end');
					EXCEPTION
					WHEN OTHERS THEN
					 lv_dist_status_flag  := 'E';
					 lv_dist_error_message := lv_dist_error_message||
											  '~'||'Exception while validating distribution record. Record Id:'||po_dist_rec.record_id||
											  ' Error Message:'||SQLERRM;
					END;
				end if;
				--Distribution Validation block ends.
				
				
				--this data was just used for testing.
				--temp
				--if(po_dist_rec.po_distribution_id  in (188872,188888,168624)) then
				 --lv_dist_status_flag  := 'E';
				 --lv_dist_error_message := 'Exception at distribution level.';	
				--end if;

				if lv_dist_status_flag = 'E' then
					lv_dist_error_flag := 'Y';
				end if;

				log_message(g_log,'PO Dist Record Status: Record Id:'||po_dist_rec.record_id||
						  ' po_distribution_id:'||po_dist_rec.po_distribution_id||
						  ' lv_dist_status_flag:'||lv_dist_status_flag||
						  ' lv_dist_error_message:'||lv_dist_error_message||
						  ' lv_dist_error_flag:'||lv_dist_error_flag);
				  
				update xxconv.XXPO_PO_DIST_ALL_RES_STG
				   set last_update_date = sysdate
				      ,last_updated_by = g_user_id
				      ,last_update_login = g_login_id
				      ,request_id = g_request_id
				      ,status_flag = DECODE(lv_dist_status_flag,'S',g_validated,g_error)
				      ,ERROR_MESSAGE = DECODE(lv_dist_status_flag,'S',NULL,lv_dist_error_message)
					  --new_ship_to_organization_id derived at line level can be assigned to distribution level
					  ,NEW_DESTINATION_ORG_ID = po_line_rec.new_ship_to_organization_id
					  ,NEW_DESTINATION_SUBINVENTORY = po_dist_rec.NEW_DESTINATION_SUBINVENTORY
					  ,rate_date = po_dist_rec.rate_date
					  ,rate = po_dist_rec.rate
					  ,new_attribute1 = po_dist_rec.new_attribute1
					  ,new_attribute2 = po_dist_rec.new_attribute2
					  ,new_attribute3 = po_dist_rec.new_attribute3
					  ,new_expenditure_org_id=po_dist_rec.new_expenditure_org_id
					  ,new_set_of_books_id = po_dist_rec.new_set_of_books_id
					  ,new_quantity = po_line_loc_rec.new_quantity
					  ,project_accounting_context = DECODE(po_dist_rec.new_project_id,null,null,'Y')
					  --resetting the interface table values.
					  ,interface_header_id = null
					  ,interface_line_id = null
					  ,interface_line_location_id =  null
					  ,interface_distribution_id =  null
				 where record_id = po_dist_rec.record_id;				     
			
			end loop;--end for po_dist_rec
			
			
			--this data was just used for testing.
			--temp
			
			--if(po_line_loc_rec.line_location_id  in (424327)) then
			 --lv_line_loc_status_flag  := 'E';
			 --lv_line_loc_error_message := 'Exception at line location level.';	
			--end if;*

			if lv_line_loc_status_flag = 'E' then
				lv_line_loc_error_flag := 'Y';
			end if;
			
			log_message(g_log,'PO Line Loc Record_id:'||po_line_loc_rec.record_id||
						  ' line_location_id:'||po_line_loc_rec.line_location_id||
		 	     		  ' lv_line_loc_status_flag:'||lv_line_loc_status_flag||
		 	     		  ' lv_line_loc_error_message:'||lv_line_loc_error_message||
		 	     		  ' lv_line_loc_error_flag:'||lv_line_loc_error_flag);

			update xxconv.XXPO_PO_LINE_LOC_ALL_RES_STG
			   set last_update_date = sysdate
			      ,last_updated_by = g_user_id
			      ,last_update_login = g_login_id
			      ,request_id = g_request_id
			      ,status_flag = DECODE(lv_line_loc_status_flag,'S',g_validated,g_error)
			      ,ERROR_MESSAGE = DECODE(lv_line_loc_status_flag,'S',NULL,lv_line_loc_error_message)
				  --new_ship_to_organization_id derived at line level can be assigned to line location level.
				  ,new_ship_to_organization_id = po_line_rec.new_ship_to_organization_id
				  --resetting the interface table values.
				  ,interface_header_id = null
				  ,interface_line_id = null
				  ,interface_line_location_id =  null
			 where record_id = po_line_loc_rec.record_id;			
			 
		end loop;  --end for po_line_loc_rec
		
		
		
		
		--this data was just used for testing.
		--temp
		
		--if(po_line_rec.po_line_id in (213849)) then
		 --lv_line_status_flag  := 'E';
		 --lv_line_error_message := 'Exception at line level.';	
		--end if;

		if lv_line_status_flag = 'E' then
			lv_line_error_flag := 'Y';
		end if;
		
		log_message(g_log,'PO Line Record_id:'||po_line_rec.record_id||
				  ' po_line_id:'||po_line_rec.po_line_id||
				  ' lv_line_status_flag:'||lv_line_status_flag||
				  ' lv_line_error_message:'||lv_line_error_message||
				  ' lv_line_error_flag:'||lv_line_error_flag);		
		
		update xxconv.XXPO_PO_LINES_ALL_RES_STG
		   set last_update_date = sysdate
		      ,last_updated_by = g_user_id
		      ,last_update_login = g_login_id
		      ,request_id = g_request_id
		      ,status_flag = DECODE(lv_line_status_flag,'S',g_validated,g_error)
		      ,ERROR_MESSAGE = DECODE(lv_line_status_flag,'S',NULL,lv_line_error_message)
			  ,new_item = po_line_rec.new_item
			  ,invoice_close_tolerance = po_line_rec.invoice_close_tolerance
			  ,receive_close_tolerance = po_line_rec.receive_close_tolerance
			  ,days_early_receipt_allowed = po_line_rec.days_early_receipt_allowed
			  ,days_late_receipt_allowed = po_line_rec.days_late_receipt_allowed
			  ,receiving_routing_id = po_line_rec.receiving_routing_id
			  ,qty_rcv_tolerance = po_line_rec.qty_rcv_tolerance
			  ,over_tolerance_error_flag = 'NONE'
			  ,qty_rcv_exception_code = 'NONE'
			  ,promised_date = po_line_rec.promised_date
			  ,tax_user_override_flag = po_line_rec.tax_user_override_flag
			  ,note_to_receiver = po_line_rec.note_to_receiver
			  ,consigned_flag = po_line_rec.consigned_flag
			  ,drop_ship_flag = po_line_rec.drop_ship_flag
			  ,terms_id = po_header_rec.terms_id
			  ,tax_status_indicator= po_line_rec.tax_status_indicator
			  ,ship_to_organization_id = po_line_rec.ship_to_organization_id
			  ,new_ship_to_organization_id = po_line_rec.new_ship_to_organization_id
			  ,new_item_id = po_line_rec.new_item_id
			  --resetting the interface table values.
			  ,interface_header_id = null
			  ,interface_line_id = null
		 where record_id = po_line_rec.record_id;

		
		
	end loop;--end for po_line_rec
	
	
	--this data was just used for testing.
	--if(po_header_rec.po_header_id in (175585)) then
	 --lv_header_status_flag  := 'E';
	 --lv_header_error_message := 'Exception at header level.';	
	--end if;
	
    log_message(g_log,' lv_line_error_flag:'||lv_line_error_flag||
     		 ' lv_line_loc_error_flag:'||lv_line_loc_error_flag||' lv_dist_error_flag:'||lv_dist_error_flag);
	
	--If there is any error at line level then mark the header record as failed.
	if lv_line_error_flag = 'Y' or lv_line_loc_error_flag = 'Y' or lv_dist_error_flag = 'Y' then
		lv_header_status_flag := 'E';
		lv_header_error_message := lv_header_error_message||'~'||'Some of the child records has failed.';
	end if;
	
	/*if lv_header_status_flag = 'E' then
		ln_total_failed_count :=  ln_total_failed_count + 1;	
	end if;*/
	
	log_message(g_log,'PO Header Record_id:'||po_header_rec.record_id||
			  ' po_header_id:'||po_header_rec.po_header_id||
			  ' lv_header_status_flag:'||lv_header_status_flag||
			  ' lv_header_error_message:'||lv_header_error_message);	
			  
	update xxconv.XXPO_PO_HEADERS_ALL_RES_STG
	   set last_update_date = sysdate
	      ,last_updated_by = g_user_id
	      ,last_update_login = g_login_id
	      ,request_id = g_request_id
	      ,status_flag = DECODE(lv_header_status_flag,'S',g_validated,g_error)
	      ,ERROR_MESSAGE = DECODE(lv_header_status_flag,'S',NULL,lv_header_error_message)
		  --Validated and Transformed column updates.
		  ,new_rate_type = po_header_rec.new_rate_type
		  ,new_rate = po_header_rec.new_rate
		  ,new_rate_date = po_header_rec.new_rate_date
		  ,new_agent_id = po_header_rec.new_agent_id
		  ,new_vendor_site_id = po_header_rec.new_vendor_site_id
		  ,revised_date = null --Since this create new PO case revised date has to be null and revision_num has to be 0.
		  ,revision_num = 0
		  --resetting the interface table values.
		  ,interface_header_id = null
	 where record_id = po_header_rec.record_id;	
	
     end loop;--end for po_header_rec
	 
     commit;

	 select count(*)
	  into ln_total_record_count
	  from xxconv.xxpo_po_headers_all_res_stg
	  where request_id = g_request_id;

	 select count(*)
	  into ln_total_failed_count
	  from xxconv.xxpo_po_headers_all_res_stg
	  where request_id = g_request_id
	    and status_flag = g_error;
		
	 select count(*)
	  into ln_ignored_counts
	  from xxconv.xxpo_po_headers_all_res_stg
	  where request_id = g_request_id
	    and status_flag = g_ignored;
		
	 log_message(g_log,'Total Count of records:'||ln_total_record_count);
	 log_message(g_log,'Total validated records:'||(ln_total_record_count-ln_total_failed_count-ln_ignored_counts));
	 log_message(g_log,'Total failed records:'||ln_total_failed_count);
	 log_message(g_log,'Total ignored records:'||ln_ignored_counts);
	 
	 log_message(g_out,'Total Count of records:'||ln_total_record_count);
	 log_message(g_out,'Total validated records:'||(ln_total_record_count-ln_total_failed_count-ln_ignored_counts));
	 log_message(g_out,'Total failed records:'||ln_total_failed_count);
	 log_message(g_out,'Total ignored records:'||ln_ignored_counts);
	 
	log_message(g_out,'***************Ignored Records Stats**********************');
	log_message(g_out,rpad('Count:',10,' ')||'Error Message');
	for header_errors_rec in header_errors(g_ignored)
	loop
	log_message(g_out,rpad(header_errors_rec.count_val,10,' ')||header_errors_rec.error_message);
	end loop;
	log_message(g_out,'');
	log_message(g_out,'');

	log_message(g_out,'***************Failed Records Stats**********************');
	log_message(g_out,rpad('Count:',10,' ')||'Error Message');
	for header_errors_rec in header_errors(g_error)
	loop
	log_message(g_out,rpad(header_errors_rec.count_val,10,' ')||header_errors_rec.error_message);
	end loop;
	log_message(g_out,'');
	log_message(g_out,'');

	
	log_message(g_out,'');
	log_message(g_out,'');

	
	 if ln_total_record_count = (ln_total_failed_count+ln_ignored_counts) then
		g_program_status := 2;
	 elsif ln_total_failed_count = 0 and ln_ignored_counts = 0 then
		g_program_status := 0;
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
  --   This procedure will insert data into oracle interface tables to be called by Import Standard Purchase Order program.
  -- =============================================================================

   PROCEDURE import
   IS



  lv_error_flag    VARCHAR2(1);
  lv_error_msg    VARCHAR2(1000);
  
  --Strategy for import would be as follows.
  --Since there are 4 levels i.e. header , lines , line locations and distributions.
  --If there is failure at any level then we rollback entire transaction for that po header id , else we commit the po header id.
  --If there is failure at any level then failure will be stamped only at header level, at child level records will be marked as processed only if there is no failure 
  --at any level.
  cursor po_header_cur
  is
  select * 
  from xxconv.XXPO_PO_HEADERS_ALL_RES_STG
  where process_flag = g_process_flag
    and status_flag = g_validated;
	--and po_header_id = 176099;--temp
	
  cursor po_line_cur(pin_parentrecord_id in number
   		     ,pin_po_header_id    in number
   		      )
   is
   select * 
   from xxconv.XXPO_PO_LINES_ALL_RES_STG
   where parent_record_id = pin_parentrecord_id
     and po_header_id = pin_po_header_id
	 and status_flag = g_validated;	
  
	cursor po_line_loc_cur(pin_parentrecord_id in number
				,pin_po_header_id    in number
				,pin_po_line_id    in number
				 )
	is
	select * 
        from xxconv.XXPO_PO_LINE_LOC_ALL_RES_STG
        where parent_record_id = pin_parentrecord_id
          and po_header_id = pin_po_header_id
		  and po_line_id = pin_po_line_id
		  and status_flag = g_validated;	
		  
   cursor po_dist_cur(pin_parentrecord_id in number
				,pin_po_header_id    in number
				,pin_po_line_id    in number
				,pin_po_line_loc_id    in number
				 )
	is
	select * 
	from xxconv.XXPO_PO_DIST_ALL_RES_STG
	where parent_record_id = pin_parentrecord_id
	  and po_header_id = pin_po_header_id
	  and po_line_id = pin_po_line_id
	  and line_location_id = pin_po_line_loc_id
	  and status_flag = g_validated;	

   --Variables for po header status flag and error message.
   lv_status_flag  varchar2(1) := 'S';
   lv_error_message	varchar2(10000);
   lv_level			varchar2(1000);
   
   ln_count			number;
   
   --variables to handle stats.
   ln_total_record_count		number default 0;
   ln_total_failed_count		number default 0;

   
   BEGIN
     log_message(g_log,'Importing record Begin');
     
	 --There would be only one exception handling block and thus if there is any failure exception would be 
	 --handled in the same exception block and insert made to the interface table would be committed.
     for po_header_rec in po_header_cur
     loop
		savepoint po_savepoint;
		ln_total_record_count	:= ln_total_record_count + 1;
	    begin
		--Resetting the variables.
		lv_status_flag := 'S';
		lv_error_message := null;
     	log_message(g_log,'Header: Record Id:'||po_header_rec.record_id||' po_header_id:'||po_header_rec.po_header_id);
		
		--Start of Insert statement for header.
		 lv_level := 'Header: Record Id:'||po_header_rec.record_id||' po_header_id:'||po_header_rec.po_header_id;
		 insert into po_headers_interface
							( org_id,DOCUMENT_TYPE_CODE,DOCUMENT_NUM,CURRENCY_CODE
							 ,RATE_TYPE,RATE_DATE,RATE
							 ,agent_id,vendor_id,vendor_site_id
							 ,vendor_contact_id,SHIP_TO_LOCATION_ID,BILL_TO_LOCATION_ID,TERMS_ID
							 ,freight_carrier,fob,freight_terms,approval_status
							 ,approved_date,revised_date,revision_num,note_to_vendor,note_to_receiver
							 ,confirming_order_flag,comments,acceptance_required_flag,acceptance_due_date
							 ,print_count,printed_date,ussgl_transaction_code
							 ,attribute_category,attribute1,attribute2,attribute3
							 ,attribute4,attribute5,attribute6,attribute7,attribute8
							 ,attribute9,attribute10,attribute11,attribute12
							 ,attribute13,attribute14,attribute15,pay_on_code,shipping_control
							 ,change_summary,global_attribute_category,global_attribute1,global_attribute2
							 ,global_attribute3,global_attribute4,global_attribute5,global_attribute6
							 ,global_attribute7,global_attribute8,global_attribute9,global_attribute10                    
							 ,global_attribute11,global_attribute12,global_attribute13,global_attribute14
							 ,global_attribute15,global_attribute16,global_attribute17,global_attribute18
							 ,global_attribute19,global_attribute20,program_update_date,program_id,program_application_id,request_id
							 ,last_update_login,last_updated_by,last_update_date,created_by,creation_date,group_code,action,process_code
							 ,interface_source_code,batch_id,interface_header_id
							 )
						values(po_header_rec.destination_org_id,po_header_rec.TYPE_LOOKUP_CODE,po_header_rec.segment1,po_header_rec.CURRENCY_CODE
							  ,po_header_rec.new_rate_type,po_header_rec.new_rate_date,po_header_rec.new_rate
							  ,po_header_rec.new_agent_id,po_header_rec.vendor_id,po_header_rec.new_vendor_site_id
							  ,po_header_rec.vendor_contact_id,po_header_rec.SHIP_TO_LOCATION_ID,po_header_rec.BILL_TO_LOCATION_ID,po_header_rec.TERMS_ID
							  ,po_header_rec.SHIP_VIA_LOOKUP_CODE,po_header_rec.FOB_LOOKUP_CODE,po_header_rec.FREIGHT_TERMS_LOOKUP_CODE,po_header_rec.AUTHORIZATION_STATUS
							  ,po_header_rec.approved_date,po_header_rec.revised_date,po_header_rec.revision_num,po_header_rec.note_to_vendor,po_header_rec.note_to_receiver
							  ,po_header_rec.confirming_order_flag,po_header_rec.comments,po_header_rec.acceptance_required_flag,po_header_rec.acceptance_due_date
							  ,po_header_rec.print_count,po_header_rec.printed_date,po_header_rec.ussgl_transaction_code
							  ,po_header_rec.attribute_category,po_header_rec.attribute1,po_header_rec.attribute2,po_header_rec.attribute3
							  ,po_header_rec.attribute4,po_header_rec.attribute5,po_header_rec.attribute6,po_header_rec.attribute7,po_header_rec.attribute8
							  ,po_header_rec.attribute9,po_header_rec.attribute10,po_header_rec.attribute11,po_header_rec.attribute12
							  ,po_header_rec.attribute13,po_header_rec.attribute14,po_header_rec.attribute15,po_header_rec.pay_on_code,po_header_rec.shipping_control
							  ,po_header_rec.change_summary,po_header_rec.global_attribute_category,po_header_rec.global_attribute1,po_header_rec.global_attribute2
							  ,po_header_rec.global_attribute3,po_header_rec.global_attribute4,po_header_rec.global_attribute5,po_header_rec.global_attribute6
							  ,po_header_rec.global_attribute7,po_header_rec.global_attribute8,po_header_rec.global_attribute9,po_header_rec.global_attribute10
							  ,po_header_rec.global_attribute11,po_header_rec.global_attribute12,po_header_rec.global_attribute13,po_header_rec.global_attribute14
							  ,po_header_rec.global_attribute15,po_header_rec.global_attribute16,po_header_rec.global_attribute17,po_header_rec.global_attribute18
							  ,po_header_rec.global_attribute19,po_header_rec.global_attribute20,sysdate,g_conc_program_id,g_prog_appl_id,g_request_id
							  ,g_login_id,g_user_id,sysdate,g_user_id,sysdate,null,'ORIGINAL',null
							  ,'CONVERSION',null,po_headers_interface_s.nextval
							  );
							  
					 update XXCONV.XXPO_PO_HEADERS_ALL_RES_STG
						set status_flag = g_processed
						  ,ERROR_MESSAGE = null
						  ,last_update_date = sysdate
						  ,last_updated_by = g_user_id
						  ,last_update_login = g_login_id
						  ,request_id = g_request_id
						  ,interface_header_id = po_headers_interface_s.currval
					where record_id = po_header_rec.record_id;								  
							  
		--Start of Insert statement for end.
			--Loop for po line record
			for po_line_rec in po_line_cur(po_header_rec.record_id
							  ,po_header_rec.po_header_id
							  )
			loop
				log_message(g_log,'Line: Record Id:'||po_line_rec.record_id||
								  ' po_header_id:'||po_line_rec.po_header_id||
								  ' po_line_id:'||po_line_rec.po_line_id);
								  
				--Start of Insert statement for line.
				 lv_level := 'Line: Record Id:'||po_line_rec.record_id||
								  ' po_header_id:'||po_line_rec.po_header_id||
								  ' po_line_id:'||po_line_rec.po_line_id;
				--temp Used for testing the loop.
				/*
				 if po_line_rec.po_line_id in (214714) then
					ln_count :=  10/0;
				 end if;	*/
				insert into po_lines_interface
				(line_num,line_type_id,item_id
					  ,item_revision,category_id,item_description,vendor_product_num
					  ,unit_of_measure,quantity,unit_price,list_price_per_unit
					  ,un_number_id,hazard_class_id,note_to_vendor,transaction_reason_code
					  ,taxable_flag,tax_name,price_type,ussgl_transaction_code
					  ,invoice_close_tolerance,receive_close_tolerance,days_early_receipt_allowed
					  ,days_late_receipt_allowed,receiving_routing_id,qty_rcv_tolerance
					  ,over_tolerance_error_flag,qty_rcv_exception_code,need_by_date
					  ,promised_date,line_attribute_category_lines,line_attribute1,line_attribute2
					  ,line_attribute3,line_attribute4,line_attribute5,line_attribute6
					  ,line_attribute7,line_attribute8,line_attribute9,line_attribute10
					  ,line_attribute11,line_attribute12,line_attribute13,line_attribute14
					  ,line_attribute15,tax_status_indicator,tax_user_override_flag,note_to_receiver
					  ,consigned_flag,supplier_ref_number,drop_ship_flag,terms_id
					  ,organization_id,process_code,program_update_date,program_id,program_application_id,request_id
					  ,last_update_login,last_updated_by,last_update_date,created_by,creation_date
					  ,group_code,action,interface_header_id,interface_line_id,line_loc_populated_flag
					  )
					values(po_line_rec.line_num,po_line_rec.line_type_id,po_line_rec.new_item_id
					  ,po_line_rec.item_revision,po_line_rec.category_id,po_line_rec.item_description,po_line_rec.vendor_product_num
					  ,po_line_rec.unit_meas_lookup_code,po_line_rec.new_quantity,po_line_rec.unit_price,po_line_rec.list_price_per_unit
					  ,po_line_rec.un_number_id,po_line_rec.hazard_class_id,po_line_rec.note_to_vendor,po_line_rec.transaction_reason_code
					  ,po_line_rec.taxable_flag,po_line_rec.tax_name,po_line_rec.price_type_lookup_code,po_line_rec.ussgl_transaction_code
					  ,po_line_rec.invoice_close_tolerance,po_line_rec.receive_close_tolerance,po_line_rec.days_early_receipt_allowed
					  ,po_line_rec.days_late_receipt_allowed,po_line_rec.receiving_routing_id,po_line_rec.qty_rcv_tolerance
					  ,po_line_rec.over_tolerance_error_flag,po_line_rec.qty_rcv_exception_code,po_line_rec.need_by_date
					  ,po_line_rec.promised_date,po_line_rec.attribute_category,po_line_rec.attribute1,po_line_rec.attribute2
					  ,po_line_rec.attribute3,po_line_rec.attribute4,po_line_rec.attribute5,po_line_rec.attribute6
					  ,po_line_rec.attribute7,po_line_rec.attribute8,po_line_rec.attribute9,po_line_rec.attribute10
					  ,po_line_rec.attribute11,po_line_rec.attribute12,po_line_rec.attribute13,po_line_rec.attribute14
					  ,po_line_rec.attribute15,po_line_rec.tax_status_indicator,po_line_rec.tax_user_override_flag,po_line_rec.note_to_receiver
					  ,po_line_rec.consigned_flag,po_line_rec.supplier_ref_number,po_line_rec.drop_ship_flag,po_line_rec.terms_id
					  ,po_line_rec.destination_org_id,null,sysdate,g_conc_program_id,g_prog_appl_id,g_request_id
					  ,g_login_id,g_user_id,sysdate,g_user_id,sysdate,null,'ADD',po_headers_interface_s.currval,po_lines_interface_s.nextval,'Y'
					   );
					   
					 update XXCONV.XXPO_PO_LINES_ALL_RES_STG
						set status_flag = g_processed
						  ,ERROR_MESSAGE = null
						  ,last_update_date = sysdate
						  ,last_updated_by = g_user_id
						  ,last_update_login = g_login_id
						  ,request_id = g_request_id
						  ,interface_header_id=po_headers_interface_s.currval
						  ,interface_line_id=po_lines_interface_s.currval
					where record_id = po_line_rec.record_id;	
							
				--Start of Insert statement for line.			
				
					--Loop for po line loc record
					for po_line_loc_rec in po_line_loc_cur(po_header_rec.record_id
										  ,po_line_rec.po_header_id
										  ,po_line_rec.po_line_id
										  )
					loop	
						log_message(g_log,'Line Location: Record Id:'||po_line_loc_rec.record_id||
										  ' po_header_id:'||po_line_loc_rec.po_header_id||
										  ' po_line_id:'||po_line_loc_rec.po_line_id||
										  ' line_location_id:'||po_line_loc_rec.line_location_id
										  );
						--Start of Insert statement for line location.
						 lv_level := 'Line Location: Record Id:'||po_line_loc_rec.record_id||
										  ' po_header_id:'||po_line_loc_rec.po_header_id||
										  ' po_line_id:'||po_line_loc_rec.po_line_id||
										  ' line_location_id:'||po_line_loc_rec.line_location_id;
										  
						--temp Used for testing the loop.
						 /*if po_line_loc_rec.line_location_id in (433905,433906) then
							ln_count :=  10/0;
						 end if;*/			
							insert into po_line_locations_interface
							(shipment_num,shipment_type,quantity,taxable_flag
							,tax_name,inspection_required_flag,receipt_required_flag
							,invoice_close_tolerance,receive_close_tolerance,days_early_receipt_allowed
							,days_late_receipt_allowed,enforce_ship_to_location_code,allow_substitute_receipts_flag
							,ship_to_organization_id,qty_rcv_tolerance,qty_rcv_exception_code,receipt_days_exception_code
							,ship_to_location_id,attribute_category,attribute3
							 ,attribute4,attribute5,attribute6,attribute7,attribute8
							 ,attribute9,attribute10,attribute11,attribute12
							 ,attribute13,attribute14,attribute15,match_option,accrue_on_receipt_flag
							 ,receiving_routing_id,price_override,need_by_date,promised_date
							 ,note_to_receiver,unit_of_measure,price_discount,value_basis
							 ,matching_basis,country_of_origin_code,process_code,program_update_date,program_id,program_application_id,request_id
							 ,last_update_login,last_updated_by,last_update_date,created_by,creation_date,action
						     ,interface_header_id,interface_line_id,interface_line_location_id
							)
							values
							(
							po_line_loc_rec.shipment_num,po_line_loc_rec.shipment_type,po_line_loc_rec.new_quantity,po_line_loc_rec.taxable_flag
							,po_line_loc_rec.tax_name,po_line_loc_rec.inspection_required_flag,po_line_loc_rec.receipt_required_flag
							,po_line_loc_rec.invoice_close_tolerance,po_line_loc_rec.receive_close_tolerance,po_line_loc_rec.days_early_receipt_allowed
							,po_line_loc_rec.days_late_receipt_allowed,po_line_loc_rec.enforce_ship_to_location_code,po_line_loc_rec.allow_substitute_receipts_flag
							,po_line_loc_rec.new_ship_to_organization_id,po_line_loc_rec.qty_rcv_tolerance,po_line_loc_rec.qty_rcv_exception_code,po_line_loc_rec.receipt_days_exception_code
							,po_line_loc_rec.ship_to_location_id,po_line_loc_rec.attribute_category,po_line_loc_rec.attribute3
							,po_line_loc_rec.attribute4,po_line_loc_rec.attribute5,po_line_loc_rec.attribute6,po_line_loc_rec.attribute7,po_line_loc_rec.attribute8
							,po_line_loc_rec.attribute9,po_line_loc_rec.attribute10,po_line_loc_rec.attribute11,po_line_loc_rec.attribute12
							,po_line_loc_rec.attribute13,po_line_loc_rec.attribute14,po_line_loc_rec.attribute15,po_line_loc_rec.match_option,po_line_loc_rec.accrue_on_receipt_flag
							,po_line_loc_rec.receiving_routing_id,po_line_loc_rec.price_override,po_line_loc_rec.need_by_date,po_line_loc_rec.promised_date
							,po_line_loc_rec.note_to_receiver,po_line_loc_rec.unit_of_measure,po_line_loc_rec.price_discount,po_line_loc_rec.value_basis
							,po_line_loc_rec.matching_basis,po_line_loc_rec.country_of_origin_code,null,sysdate,g_conc_program_id,g_prog_appl_id,g_request_id
							,g_login_id,g_user_id,sysdate,g_user_id,sysdate,'ADD'
							,po_headers_interface_s.currval,po_lines_interface_s.currval,po_line_locations_interface_s.nextval
							);
							
							 update XXCONV.XXPO_PO_LINE_LOC_ALL_RES_STG
								set status_flag = g_processed
								  ,ERROR_MESSAGE = null
								  ,last_update_date = sysdate
								  ,last_updated_by = g_user_id
								  ,last_update_login = g_login_id
								  ,request_id = g_request_id
								  ,interface_header_id=po_headers_interface_s.currval 
								  ,interface_line_id=po_lines_interface_s.currval
								  ,interface_line_location_id=po_line_locations_interface_s.currval
							where record_id = po_line_loc_rec.record_id;							
						--Start of Insert statement for line location.	

							--Loop for po distribution record
							for po_dist_rec in po_dist_cur(po_header_rec.record_id
											  ,po_line_loc_rec.po_header_id
											  ,po_line_loc_rec.po_line_id
											  ,po_line_loc_rec.line_location_id
											  )
							loop
								log_message(g_log,'Distribution: Record Id:'||po_dist_rec.record_id||
												  ' po_header_id:'||po_dist_rec.po_header_id||
												  ' po_line_id:'||po_dist_rec.po_line_id||
												  ' line_location_id:'||po_dist_rec.line_location_id||
												  ' po_distribution_id:'||po_dist_rec.po_distribution_id
												  );
								--Start of Insert statement for distribution.
									lv_level := 'Distribution: Record Id:'||po_dist_rec.record_id||
												  ' po_header_id:'||po_dist_rec.po_header_id||
												  ' po_line_id:'||po_dist_rec.po_line_id||
												  ' line_location_id:'||po_dist_rec.line_location_id||
												  ' po_distribution_id:'||po_dist_rec.po_distribution_id;
												  
									--temp Used for testing the loop.
									 /*
									 if po_dist_rec.po_distribution_id in (1737921) then
										ln_count :=  10/0;
									 end if;*/
									 
									 insert into po_distributions_interface
									 (distribution_num,org_id,quantity_ordered
									 ,rate_date,rate,deliver_to_location_id,deliver_to_person_id,destination_type_code
									 ,destination_organization_id,destination_subinventory,set_of_books_id
									 ,ussgl_transaction_code,government_context,project_id,task_id
									 ,expenditure_type,project_accounting_context,expenditure_organization_id,expenditure_item_date
									 ,accrue_on_receipt_flag,attribute_category,attribute1,attribute2,attribute3
									 ,attribute4,attribute5,attribute6,attribute7,attribute8
									 ,attribute9,attribute10,attribute11,attribute12
									 ,attribute13,attribute14,attribute15,process_code,program_update_date,program_id,program_application_id,request_id
									 ,last_update_login,last_updated_by,last_update_date,created_by,creation_date,interface_distribution_id
									 ,interface_line_location_id,interface_line_id,interface_header_id
									 )
									 values
									 (po_dist_rec.distribution_num,po_dist_rec.destination_org_id,po_dist_rec.new_quantity
									 ,po_dist_rec.rate_date,po_dist_rec.rate,po_dist_rec.deliver_to_location_id,po_dist_rec.deliver_to_person_id,po_dist_rec.destination_type_code
									 ,po_dist_rec.new_destination_org_id,po_dist_rec.new_destination_subinventory,po_dist_rec.new_set_of_books_id
									 ,po_dist_rec.ussgl_transaction_code,po_dist_rec.government_context,po_dist_rec.new_project_id,po_dist_rec.new_task_id
									 ,po_dist_rec.expenditure_type,po_dist_rec.project_accounting_context,po_dist_rec.new_expenditure_org_id,po_dist_rec.expenditure_item_date
									 ,po_dist_rec.accrue_on_receipt_flag,po_dist_rec.attribute_category,po_dist_rec.new_attribute1,po_dist_rec.new_attribute2,po_dist_rec.new_attribute3
									 ,po_dist_rec.attribute4,po_dist_rec.attribute5,po_dist_rec.attribute6,po_dist_rec.attribute7,po_dist_rec.attribute8
									 ,po_dist_rec.attribute9,po_dist_rec.attribute10,po_dist_rec.attribute11,po_dist_rec.attribute12
									 ,po_distributions_interface_s.nextval,po_dist_rec.attribute14,po_dist_rec.attribute15,null,sysdate,g_conc_program_id,g_prog_appl_id,g_request_id
									 ,g_login_id,g_user_id,sysdate,g_user_id,sysdate,po_distributions_interface_s.currval
									 ,po_line_locations_interface_s.currval,po_lines_interface_s.currval,po_headers_interface_s.currval
									 );
									 
									 update XXCONV.XXPO_PO_DIST_ALL_RES_STG
									    set status_flag = g_processed
										  ,ERROR_MESSAGE = null
										  ,last_update_date = sysdate
										  ,last_updated_by = g_user_id
										  ,last_update_login = g_login_id
										  ,request_id = g_request_id
										  ,interface_distribution_id=po_distributions_interface_s.currval
										  ,interface_line_location_id=po_line_locations_interface_s.currval
										  ,interface_line_id=po_lines_interface_s.currval
										  ,interface_header_id=po_headers_interface_s.currval
									where record_id = po_dist_rec.record_id;
								--Start of Insert statement for distribution.	
								

								
							end loop;--end loop for distribution.
					end loop; --end loop for po_line_loc_rec
			end loop;--end loop for po_line_rec
     	
		exception
     	when others then
     		lv_status_flag := 'E';
     		lv_error_message := ' Exception while loading data Error Message:'||SQLERRM;
     	end;
		
		log_message(g_log,'Final Status lv_status_flag:'||lv_status_flag);
		if lv_status_flag = 'E' then
		  rollback to po_savepoint;
		  ln_total_failed_count	:= ln_total_failed_count + 1;
		  log_message(g_log,'Level at which has failed is:'||lv_level||' '||lv_error_message);
			update XXCONV.XXPO_PO_HEADERS_ALL_RES_STG
				set status_flag = g_error
				  ,ERROR_MESSAGE = 'Level at which has failed is:'||lv_level||' '||lv_error_message
				  ,last_update_date = sysdate
				  ,last_updated_by = g_user_id
				  ,last_update_login = g_login_id
				  ,request_id = g_request_id
				  ,interface_header_id=null
			where record_id = po_header_rec.record_id;	
			
			update XXCONV.XXPO_PO_LINES_ALL_RES_STG
				set status_flag = g_error
				  ,ERROR_MESSAGE = 'Level at which has failed is:'||lv_level||' '||lv_error_message
				  ,last_update_date = sysdate
				  ,last_updated_by = g_user_id
				  ,last_update_login = g_login_id
				  ,request_id = g_request_id
				  ,interface_header_id=null
				  ,interface_line_id=null
			where parent_record_id = po_header_rec.record_id;	

			update XXCONV.XXPO_PO_LINE_LOC_ALL_RES_STG
				set status_flag = g_error
				  ,ERROR_MESSAGE = 'Level at which has failed is:'||lv_level||' '||lv_error_message
				  ,last_update_date = sysdate
				  ,last_updated_by = g_user_id
				  ,last_update_login = g_login_id
				  ,request_id = g_request_id
				  ,interface_header_id=null
				  ,interface_line_id=null
				  ,interface_line_location_id=null
			where parent_record_id = po_header_rec.record_id;	

			update XXCONV.XXPO_PO_DIST_ALL_RES_STG
				set status_flag = g_error
				  ,ERROR_MESSAGE = 'Level at which has failed is:'||lv_level||' '||lv_error_message
				  ,last_update_date = sysdate
				  ,last_updated_by = g_user_id
				  ,last_update_login = g_login_id
				  ,request_id = g_request_id
				  ,interface_header_id=null
				  ,interface_line_id=null
				  ,interface_line_location_id=null
				  ,interface_distribution_id=null
			where parent_record_id = po_header_rec.record_id;	
			commit;
		else
		  commit;
		  --rollback to po_savepoint;--temp
		end if;
     	
	/*update xxconv.XXPO_PO_HEADERS_ALL_RES_STG
	   set last_update_date = sysdate
	      ,last_updated_by = g_user_id
	      ,last_update_login = g_login_id
	      ,request_id = g_request_id
	      ,status_flag = DECODE(lv_header_status_flag,'S',g_processed,g_error)
	      ,ERROR_MESSAGE = DECODE(lv_header_status_flag,'S',NULL,lv_header_error_message)
	 where record_id = po_header_rec.record_id;	*/ 	

     end loop;--end loop for po_header_rec
     
	 log_message(g_log,'Total Count of records:'||ln_total_record_count);
	 log_message(g_log,'Total records successfully inserted in interface tables:'||(ln_total_record_count-ln_total_failed_count));
	 log_message(g_log,'Total failed records:'||ln_total_failed_count);
	 
	 log_message(g_out,'Total Count of records:'||ln_total_record_count);
	 log_message(g_out,'Total records successfully inserted in interface tables:'||(ln_total_record_count-ln_total_failed_count));
	 log_message(g_out,'Total failed records:'||ln_total_failed_count);
	 
	 if ln_total_record_count = ln_total_failed_count then
		g_program_status := 2;
	 elsif ln_total_failed_count = 0 then
		g_program_status := 0;
	 else
		g_program_status := 1;
	 end if;	 

   exception
   when others then
     log_message(g_log,'Exception in procedure import. Error Message.'||SQLERRM);
   END import;

   
  -- ========================
  -- Procedure: reconcile
  -- =============================================================================
  --   This procedure will reconcile status of records back in staging table.
  -- =============================================================================
   
   procedure reconcile
   is
   
	--Since during import all the records for an PO was marked as P only when all the records were successfully inserted into
	--their respective base tables, during reconcilation all the staging tables can be processed independently.
	  cursor po_header_cur
	  is
	  select header.record_id,header_interface.process_code,header_interface.po_header_id,header.interface_header_id,header.po_header_id old_po_header_id
	  from xxconv.XXPO_PO_HEADERS_ALL_RES_STG header
	      ,po_headers_interface header_interface
	  where process_flag = g_process_flag
		and status_flag = g_processed
		and header.interface_header_id =  header_interface.interface_header_id;
		
	  cursor po_lines_cur(pin_parent_record_id IN NUMBER)
	  is
	  select lines.record_id,lines_interface.process_code,lines_interface.po_header_id,lines_interface.po_line_id,lines.interface_line_id,lines.po_line_id old_po_line_id
	  from xxconv.XXPO_PO_LINES_ALL_RES_STG lines
	      ,po_lines_interface lines_interface
	  where status_flag = g_processed
	    and parent_record_id = pin_parent_record_id
		and lines.interface_line_id =  lines_interface.interface_line_id;
		
	  cursor po_line_loc_cur(pin_parent_record_id IN NUMBER)
	  is
	  select line_loc.record_id,line_loc_interface.process_code,line_loc_interface.line_location_id,line_loc_interface.interface_line_location_id
			,line_loc.line_location_id old_line_location_id
	  from xxconv.XXPO_PO_LINE_LOC_ALL_RES_STG line_loc
	      ,po_line_locations_interface line_loc_interface
	  where status_flag = g_processed
	    and parent_record_id = pin_parent_record_id
		and line_loc.interface_line_location_id =  line_loc_interface.interface_line_location_id;

	  cursor po_dist_cur(pin_parent_record_id IN NUMBER)
	  is
	  select dist_loc.record_id,dist_interface.process_code,dist_interface.po_header_id,dist_interface.po_line_id,dist_interface.line_location_id,dist_interface.po_distribution_id,dist_interface.interface_distribution_id
	  from xxconv.xxpo_po_dist_all_res_stg dist_loc
	      ,po_distributions_interface dist_interface
	  where status_flag = g_processed
	    and parent_record_id = pin_parent_record_id
		and dist_loc.interface_distribution_id =  dist_interface.interface_distribution_id;

		
	--Status variables
	lv_error_message		varchar2(10000);
	lv_status_flag			varchar2(1);
	lv_error_indicator			varchar2(2000);
	
   --variables to handle stats.
   ln_total_record_count		number default 0;
   ln_total_failed_count		number default 0;
   
   
   begin
   
   for po_header_rec in po_header_cur
   loop
		log_message(g_log,'Header:'||po_header_rec.process_code||' record_id:'||po_header_rec.record_id||' interface_header_id:'||po_header_rec.interface_header_id);
		lv_error_message := null;
		lv_status_flag	 := 'S';
		ln_total_record_count	:= ln_total_record_count + 1;
		lv_error_indicator	:= null;
		
		if po_header_rec.process_code = 'REJECTED' then
			lv_status_flag := 'E';
			ln_total_failed_count	:= ln_total_failed_count + 1;
			for error_rec in (select error_message
								from po_interface_errors err
							   where err.interface_header_id = po_header_rec.interface_header_id
								 and table_name = 'PO_HEADERS_INTERFACE'
								)
			loop
				lv_error_message := lv_error_message || '~' || error_rec.error_message;
			end loop;
		elsif po_header_rec.process_code = 'ACCEPTED' then
			log_message(g_log,'Header Converting Attachment:');
			log_message(g_log,'Calling API for creating attachment. '||
					  ' Source old_po_header_id:'||po_header_rec.old_po_header_id||
					  ' Target new_po_header_id:'||po_header_rec.po_header_id);
								
			xxfnd_cmn_res_attach_pkg.migrate_attachments('PO_HEADERS'      
									 , po_header_rec.old_po_header_id--old po_header_id
									 ,NULL
									 ,po_header_rec.po_header_id --new vendor_site_id
									 ,NULL 
									 ,lv_status_flag
									 ,lv_error_message
									 );
			log_message(g_log,'Returned values from Attachment API lv_status_flag:'||lv_status_flag||
					  ' lv_error_message:'||lv_error_message);			
			
		end if;
		
		update xxconv.XXPO_PO_HEADERS_ALL_RES_STG
		  set new_po_header_id = po_header_rec.po_header_id
			  ,status_flag = DECODE(lv_status_flag,'E',g_error,'S',g_completed)
			  ,ERROR_MESSAGE = DECODE(lv_status_flag,'E',SUBSTR(lv_error_message,1,2000),'S',null)
			  ,last_update_date = sysdate
			  ,last_updated_by = g_user_id
			  ,last_update_login = g_login_id
			  ,request_id = g_request_id
		where record_id = po_header_rec.record_id;	
		
		commit;
		
		   for po_lines_rec in po_lines_cur(po_header_rec.record_id)
		   loop
				log_message(g_log,'Line:'||po_lines_rec.process_code||' record_id:'||po_lines_rec.record_id||' interface_line_id:'||po_lines_rec.interface_line_id);
				lv_error_message := null;
				lv_status_flag	 := 'S';
				if po_lines_rec.process_code = 'REJECTED' then
					lv_status_flag	 := 'E';
					for error_rec in (select error_message
										from po_interface_errors err
									   where err.interface_line_id = po_lines_rec.interface_line_id
									   and table_name = 'PO_LINES_INTERFACE'
										)
					loop
						lv_error_message := lv_error_message || '~' || error_rec.error_message;
						lv_error_indicator	:= lv_error_indicator||'~Line:'||po_lines_rec.record_id;
					end loop;
				elsif po_lines_rec.process_code = 'ACCEPTED' then
					log_message(g_log,'Lines Converting Attachment:');
					log_message(g_log,'Calling API for creating attachment. '||
							  ' Source old_po_line_id:'||po_lines_rec.old_po_line_id||
							  ' Target new_po_line_id:'||po_lines_rec.po_line_id);
										
					xxfnd_cmn_res_attach_pkg.migrate_attachments('PO_LINES'      
											 , po_lines_rec.old_po_line_id--old po_header_id
											 ,NULL
											 ,po_lines_rec.po_line_id --new vendor_site_id
											 ,NULL 
											 ,lv_status_flag
											 ,lv_error_message
											 );
					log_message(g_log,'Returned values from Attachment API lv_status_flag:'||lv_status_flag||
							  ' lv_error_message:'||lv_error_message);					
				end if;
				
				update xxconv.XXPO_PO_LINES_ALL_RES_STG
				  set new_po_header_id = po_lines_rec.po_header_id
					  ,new_po_line_id = po_lines_rec.po_line_id
					  ,status_flag = DECODE(lv_status_flag,'E',g_error,'S',g_completed)
					  ,ERROR_MESSAGE = DECODE(lv_status_flag,'E',SUBSTR(lv_error_message,1,2000),'S',null)
					  ,last_update_date = sysdate
					  ,last_updated_by = g_user_id
					  ,last_update_login = g_login_id
					  ,request_id = g_request_id
				where record_id = po_lines_rec.record_id;
				commit;
		   end loop;  

		   for po_line_loc_rec in po_line_loc_cur(po_header_rec.record_id)
		   loop
				log_message(g_log,'Line Location:'||po_line_loc_rec.process_code||' record_id:'||po_line_loc_rec.record_id||' interface_line_location_id:'||po_line_loc_rec.interface_line_location_id);
				lv_error_message := null;
				lv_status_flag	 := 'S';
				if po_line_loc_rec.process_code = 'REJECTED' then
					
					lv_status_flag	 := 'E';
					for error_rec in (select error_message
										from po_interface_errors err
									   where err.interface_line_location_id = po_line_loc_rec.interface_line_location_id
										and table_name = 'PO_LINE_LOCATIONS_INTERFACE'
										)
					loop
						lv_error_message := lv_error_message || '~' || error_rec.error_message;
						lv_error_indicator	:= lv_error_indicator||'~LineLoc:'||po_line_loc_rec.record_id;
					end loop;
				elsif po_line_loc_rec.process_code = 'ACCEPTED' then
					log_message(g_log,'Line Location Converting Attachment:');
					log_message(g_log,'Calling API for creating attachment. '||
							  ' Source old_line_location_id:'||po_line_loc_rec.old_line_location_id||
							  ' Target new_line_location_id:'||po_line_loc_rec.line_location_id);
										
					xxfnd_cmn_res_attach_pkg.migrate_attachments('PO_SHIPMENTS'      
											 , po_line_loc_rec.old_line_location_id--old po_header_id
											 ,NULL
											 ,po_line_loc_rec.line_location_id --new vendor_site_id
											 ,NULL 
											 ,lv_status_flag
											 ,lv_error_message
											 );
					log_message(g_log,'Returned values from Attachment API lv_status_flag:'||lv_status_flag||
							  ' lv_error_message:'||lv_error_message);					
					
				end if;
				update xxconv.xxpo_po_line_loc_all_res_stg
				  set NEW_LINE_LOCATION_ID = po_line_loc_rec.line_location_id
					  ,status_flag = DECODE(lv_status_flag,'E',g_error,'S',g_completed)
					  ,ERROR_MESSAGE = DECODE(lv_status_flag,'E',SUBSTR(lv_error_message,1,2000),'S',null)
					  ,last_update_date = sysdate
					  ,last_updated_by = g_user_id
					  ,last_update_login = g_login_id
					  ,request_id = g_request_id
				where record_id = po_line_loc_rec.record_id;
				commit;
		   end loop;   

		   for po_dist_rec in po_dist_cur(po_header_rec.record_id)
		   loop
				log_message(g_log,'Distribution:'||po_dist_rec.process_code||' record_id:'||po_dist_rec.record_id||' interface_distribution_id:'||po_dist_rec.interface_distribution_id);
				lv_error_message := null;
				lv_status_flag	 := 'S';
				
				if po_dist_rec.process_code = 'REJECTED' then
					lv_status_flag	 := 'E';
					for error_rec in (select error_message
										from po_interface_errors err
									   where err.interface_distribution_id = po_dist_rec.interface_distribution_id
										 and table_name = 'PO_DISTRIBUTIONS_INTERFACE'
										)
					loop
						lv_error_message := lv_error_message || '~' || error_rec.error_message;
						lv_error_indicator	:= lv_error_indicator||'~Distribution:'||po_dist_rec.record_id;
					end loop;
				end if;
				update xxconv.xxpo_po_dist_all_res_stg
				  set  new_po_header_id = po_dist_rec.po_header_id
					  ,new_po_line_id = po_dist_rec.po_line_id
					  ,new_line_location_id = po_dist_rec.line_location_id
					  ,new_po_distribution_id = po_dist_rec.po_distribution_id
					  ,status_flag = DECODE(lv_status_flag,'E',g_error,'S',g_completed)
					  ,ERROR_MESSAGE = DECODE(lv_status_flag,'E',SUBSTR(lv_error_message,1,2000),'S',null)
					  ,last_update_date = sysdate
					  ,last_updated_by = g_user_id
					  ,last_update_login = g_login_id
					  ,request_id = g_request_id
				where record_id = po_dist_rec.record_id;
				commit;
		   end loop;   

		update xxconv.XXPO_PO_HEADERS_ALL_RES_STG
		  set new_po_header_id = po_header_rec.po_header_id
			  ,status_flag = DECODE(lv_status_flag,'E',g_error,'S',g_completed)
			  ,ERROR_MESSAGE = substr(ERROR_MESSAGE||'~Failed Record Ids:'||lv_error_indicator,1,2000)
			  ,last_update_date = sysdate
			  ,last_updated_by = g_user_id
			  ,last_update_login = g_login_id
			  ,request_id = g_request_id
		where record_id = po_header_rec.record_id;		   
		
   end loop;
   
   commit;
   
	 log_message(g_log,'Total Count of records:'||ln_total_record_count);
	 log_message(g_log,'Total records successfully imported:'||(ln_total_record_count-ln_total_failed_count));
	 log_message(g_log,'Total failed records:'||ln_total_failed_count);
	 
	 log_message(g_out,'Total Count of records:'||ln_total_record_count);
	 log_message(g_out,'Total records successfully imported:'||(ln_total_record_count-ln_total_failed_count));
	 log_message(g_out,'Total failed records:'||ln_total_failed_count);   
	 
	 if ln_total_record_count = ln_total_failed_count then
		g_program_status := 2;
	 elsif ln_total_failed_count = 0 then
		g_program_status := 0;
	 else
		g_program_status := 1;
	 end if;	 
	 
 
   exception
   when others then
     log_message(g_log,'Exception in procedure reconcile. Error Message.'||SQLERRM);
   END reconcile; 
   
 
  -- ========================
  -- Procedure: close_po
  -- =============================================================================
  --   This procedure will close purchase order's from source org once it is successfully converted
  -- =============================================================================
   
   procedure close_po
   is 
	  cursor po_header_cur
	  is
	  select stg.po_header_id,pdt.document_type_code,pdt.document_subtype,stg.record_id
	  from xxconv.XXPO_PO_HEADERS_ALL_RES_STG stg
		  ,po_document_types_all pdt
	  where stg.process_flag = g_process_flag
		and stg.status_flag = g_completed
		and stg.type_lookup_code = pdt.document_subtype
		AND stg.org_id = pdt.org_id
		AND pdt.document_type_code = 'PO'
		and NVL(po_cancelled_flag,'N') IN ( 'N','E');
	
	lv_action	varchar2(20) := 'FINALLY CLOSE';
	lv_calling_mode constant varchar2(2) := 'PO';
	lv_conc_flag constant varchar2(1) := 'N';
	lv_return_code_h varchar2(100);
	lv_auto_close constant varchar2(1) := 'N';
	ln_returned boolean;
	
	--Status variables
	lv_error_message		varchar2(10000);
	lv_status_flag			varchar2(1);
	
   --variables to handle stats.
   ln_total_record_count		number default 0;
   ln_total_failed_count		number default 0;	
	
   begin
	log_message(g_log,'close_po begin');
	for po_header_rec in po_header_cur
	loop
		--Resetting the variables
		lv_status_flag := 'S';
		lv_error_message	:= null;
		ln_total_record_count	:= ln_total_record_count + 1;
		begin
			log_message(g_log,'Record Id:'||po_header_rec.record_id);
			ln_returned :=
					po_actions.close_po(
					p_docid => po_header_rec.po_header_id,
					p_doctyp => po_header_rec.document_type_code,
					p_docsubtyp => po_header_rec.document_subtype,
					p_lineid => NULL,
					p_shipid => NULL,
					p_action => lv_action,
					p_reason => NULL,
					p_calling_mode => lv_calling_mode,
					p_conc_flag => lv_conc_flag,
					p_return_code => lv_return_code_h,
					p_auto_close => lv_auto_close,
					p_action_date => SYSDATE,
					p_origin_doc_id => NULL);
		exception
		when others then
			lv_status_flag := 'E';
			lv_error_message	:= 'Exception while closing PO. Error Message:'||SQLERRM;
			ln_total_failed_count	:= ln_total_failed_count + 1;
		end;
		log_message(g_log,'Status of cancel API lv_status_flag:'||lv_status_flag||' lv_error_message:'||lv_error_message);
		update xxconv.XXPO_PO_HEADERS_ALL_RES_STG
		  set po_cancelled_flag = DECODE(lv_status_flag,'E',g_error,'S','Y')
			  ,ERROR_MESSAGE = DECODE(lv_status_flag,'E',lv_error_message,'S',null)
			  ,last_update_date = sysdate
			  ,last_updated_by = g_user_id
			  ,last_update_login = g_login_id
			  ,request_id = g_request_id
		where record_id = po_header_rec.record_id;	
		
	end loop;
	commit;
	
	 log_message(g_log,'Total Count of records:'||ln_total_record_count);
	 log_message(g_log,'Total records successfully closed:'||(ln_total_record_count-ln_total_failed_count));
	 log_message(g_log,'Total failed records:'||ln_total_failed_count);
	 
	 log_message(g_out,'Total Count of records:'||ln_total_record_count);
	 log_message(g_out,'Total records successfully closed:'||(ln_total_record_count-ln_total_failed_count));
	 log_message(g_out,'Total failed records:'||ln_total_failed_count);   
	 
	 if ln_total_record_count = ln_total_failed_count then
		g_program_status := 2;
	 elsif ln_total_failed_count = 0 then
		g_program_status := 0;
	 else
		g_program_status := 1;
	 end if;
	 
	log_message(g_log,'close_po end');
   exception
   when others then
     log_message(g_log,'Exception in procedure close_po. Error Message.'||SQLERRM);
   END close_po;    
   
   

   -- ========================
   -- Procedure: main
   -- =============================================================================
   --   This is a main public procedure, which will be invoked through concurrent program.
   --   This conversion program will be called during restructuring to validate and import following entities
   --   Supplier Sites
   --   Supplier Contact associated to the site
   --   Bank Account information associated to the site
   -- =============================================================================
   --
   -- -----------------------------------------------------------------------------
   --  Called By Concurrent Program: Eaton AP Supplier Conversion Program
   -- -----------------------------------------------------------------------------
   -- -----------------------------------------------------------------------------
   --
   --  Input Parameters :
   --    piv_run_mode        : Control the program execution for VALIDATE and CONVERSION
   --
   --  Output Parameters :
   --    p_errbuf          : Standard output parameter for concurrent program
   --    p_retcode         : Standard output parameter for concurrent program
   --
   -- -----------------------------------------------------------------------------
   PROCEDURE main ( pov_errbuf            OUT   NOCOPY  VARCHAR2
      , pon_retcode           OUT   NOCOPY  NUMBER
                  , piv_run_mode          IN            VARCHAR2    -- pre validate/validate/conversion/reconcile
                  )
   IS
   BEGIN
   	log_message(g_log,'g_user_id:'||g_user_id);
   	log_message(g_log,'Main method Begin: piv_run_mode:'||piv_run_mode);
   	log_message(g_out,'*******************************************************************');
   	log_message(g_out,'***************Program Parameters**********************');
	log_message(g_out,rpad('Mode:',25,' ')||piv_run_mode);
	log_message(g_out,rpad('Request Id:',25,' ')||g_request_id);
	log_message(g_out,'');
	log_message(g_out,'*******************************************************************');
	

     if piv_run_mode = 'LOAD-DATA' then

       load;

     elsif piv_run_mode = 'VALIDATE' then

       validate;

     elsif piv_run_mode = 'CONVERSION' then

       import;
	 elsif piv_run_mode = 'RECONCILE' then
		reconcile;
	 elsif piv_run_mode = 'CLOSE' then
		close_po;
     else
	log_message(g_out,
	'Selected mode is invalid for Open PO Conversion Program. Supported modes are LOAD-DATA,VALIDATE,CONVERSION and CLOSE');
     end if;
	 pon_retcode := g_program_status;


   END main;

END XXPO_PO_RES_CNV_PKG;

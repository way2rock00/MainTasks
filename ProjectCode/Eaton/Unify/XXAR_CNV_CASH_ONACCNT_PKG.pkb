--Begin Revision History
--<<<
-- 30-Apr-2014 10:48:47 C9904303 /main/1
-- 
--<<<
--End Revision History  
CREATE OR REPLACE PACKAGE BODY XXAR_CNV_CASH_ONACCNT_PKG

------------------------------------------------------------------------------------------
--    Owner        : EATON CORPORATION.
--    Application  : Account Receivables
--    Schema       : APPS
--    Compile AS   : APPS
--    File Name    : XXAR_CNV_CASH_ONACCNT_PKG.pkb
--    Date         : 23-Jan-2014
--    Author       : Archita Monga
--    Description  : Package Body for Cash on Account conversion
--
--    Version      : $ETNHeader: /CCSTORE/ccweb/C9904303/C9904303_AR_view/vobs/AR_TOP/xxar/12.0.0/install/XXAR_CNV_CASH_ONACCNT_PKG.pkb /main/1 30-Apr-2014 10:48:47 C9904303  $
--
--    Parameters  :
--
--    Change History
--    Version     Created By       Date            Comments
--  ======================================================================================
--    v1.0        Archita Monga    24-Jan-2014     Creation
--    v1.1        Archita Monga    14-Mar-2014     Changes made according to review 
--                                                 check-list
--    v1.2        Archita Monga    29-Apr-2014     Added hard update to populate customer
--                                                 information for on-account application
--                                                 line as per SR 3-8589253251.
--    ====================================================================================
------------------------------------------------------------------------------------------
AS

   -- global variables
   g_request_id                   NUMBER             DEFAULT fnd_global.conc_request_id;
   g_prog_appl_id                 NUMBER             DEFAULT fnd_global.prog_appl_id;
   g_conc_program_id              NUMBER             DEFAULT fnd_global.conc_program_id;
   g_user_id                      NUMBER             DEFAULT fnd_global.user_id;
   g_login_id                     NUMBER             DEFAULT fnd_global.login_id;
   g_org_id                       NUMBER             DEFAULT fnd_global.org_id;
   g_set_of_books_id              NUMBER             DEFAULT fnd_profile.value('GL_SET_OF_BKS_ID');
   g_retcode                      NUMBER;
   g_errbuff                      VARCHAR2 (1);
   
   g_sysdate            CONSTANT  DATE            := SYSDATE;
   g_batch_source       CONSTANT  VARCHAR2 (30)   := 'Conversion';
   g_source_table       CONSTANT  VARCHAR2 (30)   := 'XXAR_CASHONACCNT_STG';
   g_run_sequence_id              NUMBER;
   g_new                CONSTANT  VARCHAR2 ( 1 )  := 'N';
   g_error              CONSTANT  VARCHAR2 ( 1 )  := 'E';
   g_validated          CONSTANT  VARCHAR2 ( 1 )  := 'V';
   g_obsolete           CONSTANT  VARCHAR2 ( 1 )  := 'X';
   g_processed          CONSTANT  VARCHAR2 ( 1 )  := 'P';
   g_converted          CONSTANT  VARCHAR2 ( 1 )  := 'C';
   g_success            CONSTANT  VARCHAR2 ( 1 )  := 'S';
   g_yes                CONSTANT  VARCHAR2 ( 1 )  := 'Y';
   g_ricew_id           CONSTANT  VARCHAR2 ( 10 ) := 'CNV-0005';
   g_created_by_module  CONSTANT  VARCHAR2 ( 10 ) := 'TCA_V1_API';
   g_init_msg_list      CONSTANT  VARCHAR2 ( 20 ) := fnd_api.g_true;

   g_run_mode                     VARCHAR2 ( 100 );
   g_entity                       VARCHAR2 ( 100 );
   g_process_records              VARCHAR2 ( 100 );
   g_gl_date                      DATE;  
   g_err_code                     VARCHAR2 ( 100 );
   g_err_message                  VARCHAR2 ( 2000 );
   g_failed_count                 NUMBER;
   g_total_count                  NUMBER;

   g_load_id                      NUMBER;
   g_indx                         NUMBER := 0;
   g_limit             CONSTANT   NUMBER := fnd_profile.value('ETN_FND_ERROR_TAB_LIMIT');
   g_err_imp           CONSTANT   VARCHAR2 ( 10 ) := 'ERR_IMP';
   g_err_val           CONSTANT   VARCHAR2 ( 10 ) := 'ERR_VAL';
   
   
   g_batch_id                     NUMBER;
   g_new_batch_id                 NUMBER;
   g_run_seq_id                   NUMBER;
   
   g_source_Tab                   xxetn_common_error_pkg.g_source_tab_type;
   
   g_intf_staging_id              xxetn_common_error.interface_staging_id%TYPE;
   g_src_keyname1                 xxetn_common_error.source_keyname1%TYPE;
   g_src_keyvalue1                xxetn_common_error.source_keyvalue1%TYPE;
   g_src_keyname2                 xxetn_common_error.source_keyname2%TYPE;
   g_src_keyvalue2                xxetn_common_error.source_keyvalue2%TYPE;
   g_src_keyname3                 xxetn_common_error.source_keyname3%TYPE;
   g_src_keyvalue3                xxetn_common_error.source_keyvalue3%TYPE;
   g_src_keyname4                 xxetn_common_error.source_keyname4%TYPE;
   g_src_keyvalue4                xxetn_common_error.source_keyvalue4%TYPE;
   g_src_keyname5                 xxetn_common_error.source_keyname5%TYPE;
   g_src_keyvalue5                xxetn_common_error.source_keyvalue5%TYPE;
  
   --
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
   --    piv_severity            : Severity 
   --    piv_proposed_solution   : Proposed Solution
   --
   --  Output Parameters :
   --    pov_return_status  : Return Status - Success / Error
   --    pov_error_msg      : Error message in case of any failure
   -- -----------------------------------------------------------------------------
   PROCEDURE log_errors
                     ( pov_return_status         OUT   NOCOPY   VARCHAR2
                     , pov_error_msg             OUT   NOCOPY   VARCHAR2
                     , piv_source_column_name    IN             xxetn_common_error.source_column_name%TYPE         DEFAULT NULL
                     , piv_source_column_value   IN             xxetn_common_error.source_column_value%TYPE        DEFAULT NULL
                     , piv_error_type            IN             xxetn_common_error.error_type%TYPE
                     , piv_error_code            IN             xxetn_common_error.error_code%TYPE
                     , piv_error_message         IN             xxetn_common_error.error_message%TYPE
                     , piv_severity              IN             xxetn_common_error.severity%TYPE                   DEFAULT NULL
                     , piv_proposed_solution     IN             xxetn_common_error.proposed_solution%TYPE          DEFAULT NULL
                     )
   IS
      l_return_status   VARCHAR2 ( 50 );
      l_error_msg       VARCHAR2 ( 2000 );
   BEGIN
   
       pov_return_status := NULL;
       pov_error_msg := NULL;
       xxetn_debug_pkg.add_debug ( 'p_err_msg: ' || piv_source_column_name  );
       xxetn_debug_pkg.add_debug ( 'g_limit: ' || g_limit  );
       xxetn_debug_pkg.add_debug ( 'g_indx: ' || g_indx  );    
       
      --increment index for every new insertion in the error table 
      g_indx := g_indx + 1;
      
      --assignment of the error record details into the table type
      g_source_Tab ( g_indx ).source_table          :=   g_source_table;
      g_source_Tab ( g_indx ).interface_staging_id  :=   g_intf_staging_id;
      g_source_Tab ( g_indx ).source_keyname1       :=   g_src_keyname1;
      g_source_Tab ( g_indx ).source_keyvalue1      :=   g_src_keyvalue1;
      g_source_Tab ( g_indx ).source_keyname2       :=   g_src_keyname2;
      g_source_Tab ( g_indx ).source_keyvalue2      :=   g_src_keyvalue2;
      g_source_Tab ( g_indx ).source_keyname3       :=   g_src_keyname3;
      g_source_Tab ( g_indx ).source_keyvalue3      :=   g_src_keyvalue3;
      g_source_Tab ( g_indx ).source_keyname4       :=   g_src_keyname4;
      g_source_Tab ( g_indx ).source_keyvalue4      :=   g_src_keyvalue4;
      g_source_Tab ( g_indx ).source_keyname5       :=   g_src_keyname5;
      g_source_Tab ( g_indx ).source_keyvalue5      :=   g_src_keyvalue5;
      g_source_Tab ( g_indx ).source_column_name    :=   piv_source_column_name;
      g_source_Tab ( g_indx ).source_column_value   :=   piv_source_column_value;
      g_source_Tab ( g_indx ).error_type            :=   piv_error_type;
      g_source_Tab ( g_indx ).error_code            :=   piv_error_code;
      g_source_Tab ( g_indx ).error_message         :=   piv_error_message;
      g_source_Tab ( g_indx ).severity              :=   piv_severity;
      g_source_Tab ( g_indx ).proposed_solution     :=   piv_proposed_solution;
   
      IF MOD ( g_indx , g_limit ) = 0
      THEN
   
         xxetn_common_error_pkg.add_error
                  ( pov_return_status    =>   l_return_status  -- OUT
                  , pov_error_msg        =>   l_error_msg      -- OUT
                  , pi_source_tab        =>   g_source_Tab     -- IN  G_SOURCE_TAB_TYPE
                  , pin_batch_id         =>   g_new_batch_id
                  );
   
         g_source_Tab.DELETE;
   
         pov_return_status := l_return_status;
         pov_error_msg     := l_error_msg;
   
         xxetn_debug_pkg.add_debug ( 'Calling xxetn_common_error_pkg.add_error '|| l_return_status || ', ' || l_error_msg );
   
         g_indx := 0;
   
      END IF;
   
   EXCEPTION
      WHEN OTHERS
      THEN
         xxetn_debug_pkg.add_debug ( 'Error: Exception occured in log_errors procedure '|| SUBSTR (SQLERRM, 1, 240) );
         
   END log_errors;
   
        
   
   -- ========================
   -- Procedure: print_log_message
   -- =============================================================================
   --   This procedure is used to write message to log file.
   -- =============================================================================
   --  Input Parameters :
   --    piv_message         : Message which needs to  be written in log file
   --  Output Parameters :
   --  Return     : Not applicable
   -- -----------------------------------------------------------------------------
   PROCEDURE print_log_message (piv_message IN VARCHAR2)
   IS
   BEGIN
      
      fnd_file.put_line ( fnd_file.log, piv_message );       
             
   END;
   
   
   --
   -- ========================
   -- Procedure: print_stat
   -- =============================================================================
   --   This procedure print_stat
   -- =============================================================================
   --  Input Parameters :
   --    None
   --  Output Parameters :
   --    None
   -- -----------------------------------------------------------------------------
   --
   PROCEDURE print_stat
   AS
   
      l_err_message        VARCHAR2 ( 2000 );
      l_log_err_msg        VARCHAR2 ( 2000 );
      l_log_ret_status     VARCHAR2 ( 50 );
   
      l_pass_val1          NUMBER := 0;
      l_err_val1           NUMBER := 0;
      l_tot_val1           NUMBER := 0;
   
      l_pass_val2          NUMBER := 0;
      l_err_val2           NUMBER := 0;
      l_tot_val2           NUMBER := 0;
   
      l_pass_imp1          NUMBER := 0;
      l_err_imp1           NUMBER := 0;
      l_tot_imp1           NUMBER := 0;
   
      l_pass_imp2          NUMBER := 0;
      l_err_imp2           NUMBER := 0;
      l_tot_imp2           NUMBER := 0;
      
   
   BEGIN
   
      xxetn_debug_pkg.add_debug ( ' + Print_stat + ' );
      xxetn_debug_pkg.add_debug ( 'Program Name : Eaton UNIFY Cash On Account Conversion Program' );
      fnd_file.put_line (fnd_file.OUTPUT,  'Program Name : Eaton UNIFY Cash On Account Conversion Program' );
      fnd_file.put_line (fnd_file.OUTPUT,  'Request ID   : ' || TO_CHAR ( g_request_id ) );
      fnd_file.put_line (fnd_file.OUTPUT,  'Report Date  : ' || TO_CHAR ( SYSDATE, 'DD-MON-RRRR HH:MI:SS AM' ) );
      fnd_file.put_line (fnd_file.OUTPUT,  '-------------------------------------------------------------------------------------------------' );
      fnd_file.put_line (fnd_file.OUTPUT,  'Parameters  : ' );
      fnd_file.put_line (fnd_file.OUTPUT,  '---------------------------------------------' );
      fnd_file.put_line (fnd_file.OUTPUT,  'Run Mode            : ' || g_run_mode );
      fnd_file.put_line (fnd_file.OUTPUT,  'Batch ID            : ' || g_batch_id );
      fnd_file.put_line (fnd_file.OUTPUT,  'Process records     : ' || g_process_records );
      fnd_file.put_line (fnd_file.OUTPUT,  'GL Date             : ' || g_gl_Date );   
      fnd_file.put_line (fnd_file.OUTPUT,  '===================================================================================================' );
      fnd_file.put_line (fnd_file.OUTPUT,  'Statistics ('|| g_run_mode ||'):' );
   
   
           --count for all the records processed
            SELECT COUNT (1)
            INTO   l_tot_val1
            FROM   xxar_cashonaccnt_stg stg
            WHERE  stg.batch_id        = NVL( g_new_batch_id, stg.batch_id)
            AND    stg.run_sequence_id = NVL( g_run_seq_id,stg.run_sequence_id);
     
            --count for all the records which errored out while validating
            SELECT COUNT (1)
            INTO   l_err_val1
            FROM   xxar_cashonaccnt_stg stg
            WHERE  stg.batch_id        = NVL( g_new_batch_id , stg.batch_id)
            AND    stg.run_sequence_id = NVL( g_run_seq_id , stg.run_sequence_id)
            AND    stg.process_flag = g_error
            AND    stg.error_type   = g_err_val;
            
            --count for all the records which errored out while importing
            SELECT COUNT (1)
            INTO   l_err_imp1
            FROM   xxar_cashonaccnt_stg stg
            WHERE  stg.batch_id        = NVL( g_new_batch_id , stg.batch_id)
            AND    stg.run_sequence_id = NVL( g_run_seq_id , stg.run_sequence_id)
            AND    stg.process_flag = g_error
            AND    stg.error_type   = g_err_imp;
   
            --count for all the records which successfully got validated
            SELECT COUNT (1)
            INTO   l_pass_val1
            FROM   xxar_cashonaccnt_stg stg
            WHERE  stg.batch_id        = NVL(g_new_batch_id , stg.batch_id)
            AND    stg.run_sequence_id = NVL( g_run_seq_id ,stg.run_sequence_id)
            AND    stg.process_flag = g_validated ;
            
          --count for all the records which successfully got converted
            SELECT COUNT (1)
            INTO   l_pass_imp1
            FROM   xxar_cashonaccnt_stg stg
            WHERE  stg.batch_id        =  NVL( g_new_batch_id , stg.batch_id)
            AND    stg.run_sequence_id = NVL( g_run_seq_id , stg.run_sequence_id)
            AND    stg.process_flag = g_converted ;
          
      IF g_run_mode = 'VALIDATE' 
      THEN
         fnd_file.PUT_LINE (fnd_file.OUTPUT, 'Records Submitted  : ' || l_tot_val1);
         fnd_file.PUT_LINE (fnd_file.OUTPUT, 'Records Validated  : ' || l_pass_val1);
         fnd_file.PUT_LINE (fnd_file.OUTPUT, 'Records Errored    : ' || l_err_val1);

      ELSIF g_run_mode = 'CONVERSION' 
      THEN
         fnd_file.PUT_LINE (fnd_file.OUTPUT, 'Records Submitted  : ' || l_tot_val1);
         fnd_file.PUT_LINE (fnd_file.OUTPUT, 'Records Imported   : ' || l_pass_imp1);
         fnd_file.PUT_LINE (fnd_file.OUTPUT, 'Records Errored    : ' || l_err_imp1);
      
      ELSIF g_run_mode = 'RECONCILE' THEN
      
         fnd_file.PUT_LINE (fnd_file.OUTPUT, 'Records Submitted              : ' || l_tot_val1);
         fnd_file.PUT_LINE (fnd_file.OUTPUT, 'Records Imported               : ' || l_pass_imp1);
         fnd_file.PUT_LINE (fnd_file.OUTPUT, 'Records Errored in Validation  : ' || l_err_val1);
         fnd_file.PUT_LINE (fnd_file.OUTPUT, 'Records Errored in Import      : ' || l_err_imp1);
      
      END IF;
  
      fnd_file.put_line (fnd_file.OUTPUT,  CHR (10) );
      fnd_file.put_line (fnd_file.OUTPUT, '===================================================================================================' );
      xxetn_debug_pkg.add_debug ( ' - Print_stat - ' );
   
   EXCEPTION
      WHEN OTHERS
      THEN
         l_err_message  := 'Error : print stat procedure encounter error. ' || SUBSTR ( SQLERRM , 1 , 150 );
         xxetn_debug_pkg.add_debug ( ' - Print_stat - ' );
         print_log_message ( l_err_message );
         g_retcode := 2;
   END print_stat;   
   
   
   --
   -- ========================
   -- Procedure: print_stat_load
   -- =============================================================================
   --   This procedure print_stat_load
   -- =============================================================================
   --  Input Parameters :
   --    None
   --  Output Parameters :
   --    None
   -- -----------------------------------------------------------------------------
   --
   PROCEDURE print_stat_load
   AS
   
      l_err_message        VARCHAR2 ( 2000 );
      l_log_err_msg        VARCHAR2 ( 2000 );
      l_log_ret_status     VARCHAR2 ( 50 );
   
      l_pass_val1          NUMBER := 0;
      l_err_val1           NUMBER := 0;
      l_tot_val1           NUMBER := 0;
   
      l_pass_val2          NUMBER := 0;
      l_err_val2           NUMBER := 0;
      l_tot_val2           NUMBER := 0;
   
      l_pass_imp1          NUMBER := 0;
      l_err_imp1           NUMBER := 0;
      l_tot_imp1           NUMBER := 0;
   
      l_pass_imp2          NUMBER := 0;
      l_err_imp2           NUMBER := 0;
      l_tot_imp2           NUMBER := 0;
      
   
   BEGIN
   
      xxetn_debug_pkg.add_debug ( ' + Print_stat_load + ' );
      xxetn_debug_pkg.add_debug ( 'Program Name : Eaton UNIFY Cash On Account Conversion Program' );
      fnd_file.put_line (fnd_file.OUTPUT,  'Program Name : Eaton UNIFY Cash On Account Conversion Program' );
      fnd_file.put_line (fnd_file.OUTPUT,  'Request ID   : ' || TO_CHAR ( g_request_id ) );
      fnd_file.put_line (fnd_file.OUTPUT,  'Report Date  : ' || TO_CHAR ( SYSDATE, 'DD-MON-RRRR HH:MI:SS AM' ) );
      fnd_file.put_line (fnd_file.OUTPUT,  '-------------------------------------------------------------------------------------------------' );
      fnd_file.put_line (fnd_file.OUTPUT,  'Parameters  : ' );
      fnd_file.put_line (fnd_file.OUTPUT,  '---------------------------------------------' );
      fnd_file.put_line (fnd_file.OUTPUT,  'Run Mode            : ' || g_run_mode );
      fnd_file.put_line (fnd_file.OUTPUT,  'Batch ID            : ' || g_batch_id );
      fnd_file.put_line (fnd_file.OUTPUT,  'Process records     : ' || g_process_records );
      fnd_file.put_line (fnd_file.OUTPUT,  'GL Date             : ' || g_gl_Date );  
      fnd_file.put_line (fnd_file.OUTPUT,  '===================================================================================================' );
      fnd_file.put_line (fnd_file.OUTPUT,  'Statistics ('|| g_run_mode ||'):' );
   
   
         fnd_file.put_line (fnd_file.OUTPUT,  'Records Submitted       : ' || g_total_count  );
         fnd_file.put_line (fnd_file.OUTPUT,  'Records Extracted       : ' || (g_total_count - g_failed_count)  );
         fnd_file.put_line (fnd_file.OUTPUT,  'Records Errored         : ' || g_failed_count   );
        
   
   
      fnd_file.put_line (fnd_file.OUTPUT,  CHR (10) );
      fnd_file.put_line (fnd_file.OUTPUT, '===================================================================================================' );
      xxetn_debug_pkg.add_debug ( ' - Print_stat_load - ' );
   
   EXCEPTION
      WHEN OTHERS
      THEN
         l_err_message  := 'Error : print stat procedure encounter error. ' || SUBSTR ( SQLERRM , 1 , 150 );
         xxetn_debug_pkg.add_debug ( ' - Print_stat_load - ' );
         print_log_message ( l_err_message );
         g_retcode := 2;
   END print_stat_load; 
   
   
   --
   -- ========================
   -- Procedure: get_data
   -- =============================================================================
   --   This procedure get_data
   -- =============================================================================
   --  Input Parameters :
   --   None
   
   --  Output Parameters :
   --   None
   -- -----------------------------------------------------------------------------
   --
   PROCEDURE get_data 
   IS
   l_return_status     VARCHAR2(1);
   l_error_message     VARCHAR2(2000);
   l_last_stmt         NUMBER;
   l_log_err_msg       VARCHAR2 ( 2000 );
   l_log_ret_status    VARCHAR2 ( 50 );
   TYPE coa_ext_rec IS RECORD   ( interface_txn_id                                      xxar_cashonaccnt_ext_r12.INTERFACE_TXN_ID%TYPE                       
                                 ,batch_id                                              xxar_cashonaccnt_ext_r12.BATCH_ID%TYPE                        
                                 ,run_sequence_id                                       xxar_cashonaccnt_ext_r12.RUN_SEQUENCE_ID%TYPE                 
                                 ,leg_receipt_method                                    xxar_cashonaccnt_ext_r12.LEG_RECEIPT_METHOD%TYPE              
                                 ,leg_receipt_number                                    xxar_cashonaccnt_ext_r12.LEG_RECEIPT_NUMBER%TYPE              
                                 ,leg_receipt_amount                                    xxar_cashonaccnt_ext_r12.LEG_RECEIPT_AMOUNT%TYPE              
                                 ,leg_currency_code                                     xxar_cashonaccnt_ext_r12.LEG_CURRENCY_code%TYPE               
                                 ,leg_receipt_date                                      xxar_cashonaccnt_ext_r12.LEG_RECEIPT_DATE%TYPE                
                                 ,leg_operating_unit                                    xxar_cashonaccnt_ext_r12.leg_operating_unit%TYPE              
                                 ,leg_gl_date                                           xxar_cashonaccnt_ext_r12.LEG_GL_DATE%TYPE                     
                                 ,leg_maturity_date                                     xxar_cashonaccnt_ext_r12.LEG_MATURITY_DATE%TYPE               
                                 ,leg_functional_amount                                 xxar_cashonaccnt_ext_r12.LEG_FUNCTIONAL_AMOUNT%TYPE           
                                 ,leg_exchange_rate_date                                xxar_cashonaccnt_ext_r12.LEG_EXCHANGE_RATE_DATE%TYPE          
                                 ,leg_exchange_rate_type                                xxar_cashonaccnt_ext_r12.LEG_EXCHANGE_RATE_TYPE%TYPE          
                                 ,leg_exchange_rate                                     xxar_cashonaccnt_ext_r12.LEG_EXCHANGE_RATE%TYPE               
                                 ,leg_unidentified_balances                             xxar_cashonaccnt_ext_r12.LEG_UNIDENTIFIED_BALANCES%TYPE       
                                 ,leg_onaccount_balances                                xxar_cashonaccnt_ext_r12.LEG_ONACCOUNT_BALANCES%TYPE          
                                 ,leg_unapplied_balances                                xxar_cashonaccnt_ext_r12.LEG_UNAPPLIED_BALANCES%TYPE          
                                 ,leg_rcpt_attribute_category                           xxar_cashonaccnt_ext_r12.LEG_RCPT_ATTRIBUTE_CATEGORY%TYPE     
                                 ,leg_rcpt_attribute1                                   xxar_cashonaccnt_ext_r12.LEG_RCPT_ATTRIBUTE1%TYPE             
                                 ,leg_rcpt_attribute2                                   xxar_cashonaccnt_ext_r12.LEG_RCPT_ATTRIBUTE2%TYPE             
                                 ,leg_rcpt_attribute3                                   xxar_cashonaccnt_ext_r12.LEG_RCPT_ATTRIBUTE3%TYPE             
                                 ,leg_rcpt_attribute4                                   xxar_cashonaccnt_ext_r12.LEG_RCPT_ATTRIBUTE4%TYPE             
                                 ,leg_rcpt_attribute5                                   xxar_cashonaccnt_ext_r12.LEG_RCPT_ATTRIBUTE5%TYPE             
                                 ,leg_rcpt_attribute6                                   xxar_cashonaccnt_ext_r12.LEG_RCPT_ATTRIBUTE6%TYPE             
                                 ,leg_rcpt_attribute7                                   xxar_cashonaccnt_ext_r12.LEG_RCPT_ATTRIBUTE7%TYPE             
                                 ,leg_rcpt_attribute8                                   xxar_cashonaccnt_ext_r12.LEG_RCPT_ATTRIBUTE8%TYPE             
                                 ,leg_rcpt_attribute9                                   xxar_cashonaccnt_ext_r12.LEG_RCPT_ATTRIBUTE9%TYPE             
                                 ,leg_rcpt_attribute10                                  xxar_cashonaccnt_ext_r12.LEG_RCPT_ATTRIBUTE10%TYPE            
                                 ,leg_rcpt_attribute11                                  xxar_cashonaccnt_ext_r12.LEG_RCPT_ATTRIBUTE11%TYPE            
                                 ,leg_rcpt_attribute12                                  xxar_cashonaccnt_ext_r12.LEG_RCPT_ATTRIBUTE12%TYPE            
                                 ,leg_rcpt_attribute13                                  xxar_cashonaccnt_ext_r12.LEG_RCPT_ATTRIBUTE13%TYPE            
                                 ,leg_rcpt_attribute14                                  xxar_cashonaccnt_ext_r12.LEG_RCPT_ATTRIBUTE14%TYPE            
                                 ,leg_rcpt_attribute15                                  xxar_cashonaccnt_ext_r12.LEG_RCPT_ATTRIBUTE15%TYPE            
                                 ,leg_customer_name                                     xxar_cashonaccnt_ext_r12.LEG_CUSTOMER_NAME%TYPE               
                                 ,leg_customer_number                                   xxar_cashonaccnt_ext_r12.LEG_CUSTOMER_NUMBER%TYPE             
                                 ,leg_bill_to_location                                  xxar_cashonaccnt_ext_r12.LEG_BILL_TO_LOCATION%TYPE            
                                 ,leg_customer_bank_name                                xxar_cashonaccnt_ext_r12.LEG_CUSTOMER_BANK_NAME%TYPE          
                                 ,leg_customer_bank_account                             xxar_cashonaccnt_ext_r12.LEG_CUSTOMER_BANK_ACCOUNT%TYPE       
                                 ,leg_reference                                         xxar_cashonaccnt_ext_r12.LEG_REFERENCE%TYPE                   
                                 ,leg_postmark_date                                     xxar_cashonaccnt_ext_r12.LEG_POSTMARK_DATE%TYPE               
                                 ,leg_comments                                          xxar_cashonaccnt_ext_r12.LEG_COMMENTS%TYPE                    
                                 ,leg_deposit_date                                      xxar_cashonaccnt_ext_r12.LEG_DEPOSIT_DATE%TYPE                
                                 ,leg_remittance_override                               xxar_cashonaccnt_ext_r12.LEG_REMITTANCE_OVERRIDE%TYPE         
                                 ,leg_remittance_bank_currency                          xxar_cashonaccnt_ext_r12.LEG_REMITTANCE_BANK_CURRENCY%TYPE    
                                 ,leg_notes_rec_issuer_name                             xxar_cashonaccnt_ext_r12.LEG_NOTES_REC_ISSUER_NAME%TYPE       
                                 ,leg_notes_rec_issue_date                              xxar_cashonaccnt_ext_r12.LEG_NOTES_REC_ISSUE_DATE%TYPE       
                                 ,leg_notes_rec_issuer_bank                             xxar_cashonaccnt_ext_r12.LEG_NOTES_REC_ISSUER_BANK%TYPE       
                                 ,leg_notes_rec_issuer_bk_branch                        xxar_cashonaccnt_ext_r12.LEG_NOTES_REC_ISSUER_BK_BRANCH%TYPE  
                                 ,leg_apply_to                                          xxar_cashonaccnt_ext_r12.LEG_APPLY_TO%TYPE                    
                                 ,leg_apply_date                                        xxar_cashonaccnt_ext_r12.LEG_APPLY_DATE%TYPE                  
                                 ,leg_amount_applied                                    xxar_cashonaccnt_ext_r12.LEG_AMOUNT_APPLIED%TYPE              
                                 ,leg_app_attribute_category                            xxar_cashonaccnt_ext_r12.LEG_APP_ATTRIBUTE_CATEGORY%TYPE      
                                 ,leg_app_attribute1                                    xxar_cashonaccnt_ext_r12.LEG_APP_ATTRIBUTE1%TYPE              
                                 ,leg_app_attribute2                                    xxar_cashonaccnt_ext_r12.LEG_APP_ATTRIBUTE2%TYPE              
                                 ,leg_app_attribute3                                    xxar_cashonaccnt_ext_r12.LEG_APP_ATTRIBUTE3%TYPE              
                                 ,leg_app_attribute4                                    xxar_cashonaccnt_ext_r12.LEG_APP_ATTRIBUTE4%TYPE              
                                 ,leg_app_attribute5                                    xxar_cashonaccnt_ext_r12.LEG_APP_ATTRIBUTE5%TYPE              
                                 ,leg_app_attribute6                                    xxar_cashonaccnt_ext_r12.LEG_APP_ATTRIBUTE6%TYPE              
                                 ,leg_app_attribute7                                    xxar_cashonaccnt_ext_r12.LEG_APP_ATTRIBUTE7%TYPE              
                                 ,leg_app_attribute8                                    xxar_cashonaccnt_ext_r12.LEG_APP_ATTRIBUTE8%TYPE              
                                 ,leg_app_attribute9                                    xxar_cashonaccnt_ext_r12.LEG_APP_ATTRIBUTE9%TYPE              
                                 ,leg_app_attribute10                                   xxar_cashonaccnt_ext_r12.LEG_APP_ATTRIBUTE10%TYPE             
                                 ,leg_app_attribute11                                   xxar_cashonaccnt_ext_r12.LEG_APP_ATTRIBUTE11%TYPE             
                                 ,leg_app_attribute12                                   xxar_cashonaccnt_ext_r12.LEG_APP_ATTRIBUTE12%TYPE             
                                 ,leg_app_attribute13                                   xxar_cashonaccnt_ext_r12.LEG_APP_ATTRIBUTE13%TYPE             
                                 ,leg_app_attribute14                                   xxar_cashonaccnt_ext_r12.LEG_APP_ATTRIBUTE14%TYPE             
                                 ,leg_app_attribute15                                   xxar_cashonaccnt_ext_r12.LEG_APP_ATTRIBUTE15%TYPE             
                                 ,receipt_method                                        xxar_cashonaccnt_ext_r12.RECEIPT_METHOD%TYPE                  
                                 ,receipt_method_id                                     xxar_cashonaccnt_ext_r12.RECEIPT_METHOD_ID%TYPE               
                                 ,pay_from_customer                                     xxar_cashonaccnt_ext_r12.PAY_FROM_CUSTOMER%TYPE               
                                 ,customer_site_use_id                                  xxar_cashonaccnt_ext_r12.CUSTOMER_SITE_USE_ID%TYPE            
                                 ,ou_name                                               xxar_cashonaccnt_ext_r12.OU_NAME%TYPE                         
                                 ,conversion_type                                       xxar_cashonaccnt_ext_r12.CONVERSION_TYPE%TYPE                 
                                 ,conversion_date                                       xxar_cashonaccnt_ext_r12.CONVERSION_DATE%TYPE                 
                                 ,conversion_rate                                       xxar_cashonaccnt_ext_r12.CONVERSION_RATE%TYPE                 
                                 ,org_id                                                xxar_cashonaccnt_ext_r12.ORG_ID%TYPE                          
                                 ,gl_date                                               xxar_cashonaccnt_ext_r12.GL_DATE%TYPE                         
                                 ,receipt_number                                        xxar_cashonaccnt_ext_r12.RECEIPT_NUMBER%TYPE                  
                                 ,receipt_amount                                        xxar_cashonaccnt_ext_r12.RECEIPT_AMOUNT%TYPE                  
                                 ,receipt_currency                                      xxar_cashonaccnt_ext_r12.RECEIPT_CURRENCY%TYPE                
                                 ,receipt_date                                          xxar_cashonaccnt_ext_r12.RECEIPT_DATE%TYPE                    
                                 ,maturity_date                                         xxar_cashonaccnt_ext_r12.MATURITY_DATE%TYPE                   
                                 ,unidentified_balances                                 xxar_cashonaccnt_ext_r12.UNIDENTIFIED_BALANCES%TYPE           
                                 ,onaccount_balances                                    xxar_cashonaccnt_ext_r12.ONACCOUNT_BALANCES%TYPE              
                                 ,unapplied_balances                                    xxar_cashonaccnt_ext_r12.UNAPPLIED_BALANCES%TYPE              
                                 ,reference                                             xxar_cashonaccnt_ext_r12.REFERENCE%TYPE                       
                                 ,postmark_date                                         xxar_cashonaccnt_ext_r12.POSTMARK_DATE%TYPE                   
                                 ,comments                                              xxar_cashonaccnt_ext_r12.COMMENTS%TYPE                        
                                 ,deposit_date                                          xxar_cashonaccnt_ext_r12.DEPOSIT_DATE%TYPE                    
                                 ,remittance_override                                   xxar_cashonaccnt_ext_r12.REMITTANCE_OVERRIDE%TYPE             
                                 ,remittance_bank_currency                              xxar_cashonaccnt_ext_r12.REMITTANCE_BANK_CURRENCY%TYPE        
                                 ,notes_rec_issuer_name                                 xxar_cashonaccnt_ext_r12.NOTES_REC_ISSUER_NAME%TYPE           
                                 ,notes_rec_issue_date                                  xxar_cashonaccnt_ext_r12.NOTES_REC_ISSUE_DATE%TYPE            
                                 ,notes_rec_issuer_bank                                 xxar_cashonaccnt_ext_r12.NOTES_REC_ISSUER_BANK%TYPE           
                                 ,notes_rec_issuer_bank_branch                          xxar_cashonaccnt_ext_r12.NOTES_REC_ISSUER_BANK_BRANCH%TYPE    
                                 ,apply_to                                              xxar_cashonaccnt_ext_r12.APPLY_TO%TYPE                        
                                 ,apply_date                                            xxar_cashonaccnt_ext_r12.APPLY_DATE%TYPE                      
                                 ,amount_applied                                        xxar_cashonaccnt_ext_r12.AMOUNT_APPLIED%TYPE                  
                                 ,creation_date                                         xxar_cashonaccnt_ext_r12.creation_date%TYPE                     
                                 ,created_by                                            xxar_cashonaccnt_ext_r12.created_by%TYPE                       
                                 ,last_updated_date                                      xxar_cashonaccnt_ext_r12.last_updated_date%TYPE                 
                                 ,last_updated_by                                       xxar_cashonaccnt_ext_r12.last_updated_by%TYPE                   
                                 ,last_update_login                                     xxar_cashonaccnt_ext_r12.last_update_login%TYPE                 
                                 ,program_application_id                                xxar_cashonaccnt_ext_r12.program_application_id%TYPE            
                                 ,program_id                                            xxar_cashonaccnt_ext_r12.program_id%TYPE               
                                 ,program_update_date                                   xxar_cashonaccnt_ext_r12.program_update_date%TYPE               
                                 ,request_id                                            xxar_cashonaccnt_ext_r12.request_id%TYPE                    
                                 ,process_flag                                          xxar_cashonaccnt_ext_r12.PROCESS_FLAG%TYPE                    
                                 ,error_type                                            xxar_cashonaccnt_ext_r12.ERROR_TYPE%TYPE                      
                                 ,attribute_category                                    xxar_cashonaccnt_ext_r12.ATTRIBUTE_CATEGORY%TYPE              
                                 ,attribute1                                            xxar_cashonaccnt_ext_r12.ATTRIBUTE1%TYPE                      
                                 ,attribute2                                            xxar_cashonaccnt_ext_r12.ATTRIBUTE2%TYPE                      
                                 ,attribute3                                            xxar_cashonaccnt_ext_r12.ATTRIBUTE3%TYPE                      
                                 ,attribute4                                            xxar_cashonaccnt_ext_r12.ATTRIBUTE4%TYPE                      
                                 ,attribute5                                            xxar_cashonaccnt_ext_r12.ATTRIBUTE5%TYPE                      
                                 ,attribute6                                            xxar_cashonaccnt_ext_r12.ATTRIBUTE6%TYPE                      
                                 ,attribute7                                            xxar_cashonaccnt_ext_r12.ATTRIBUTE7%TYPE                      
                                 ,attribute8                                            xxar_cashonaccnt_ext_r12.ATTRIBUTE8%TYPE                      
                                 ,attribute9                                            xxar_cashonaccnt_ext_r12.ATTRIBUTE9%TYPE                      
                                 ,attribute10                                           xxar_cashonaccnt_ext_r12.ATTRIBUTE10%TYPE                     
                                 ,attribute11                                           xxar_cashonaccnt_ext_r12.ATTRIBUTE11%TYPE                     
                                 ,attribute12                                           xxar_cashonaccnt_ext_r12.ATTRIBUTE12%TYPE                     
                                 ,attribute13                                           xxar_cashonaccnt_ext_r12.ATTRIBUTE13%TYPE                     
                                 ,attribute14                                           xxar_cashonaccnt_ext_r12.ATTRIBUTE14%TYPE                     
                                 ,attribute15                                           xxar_cashonaccnt_ext_r12.ATTRIBUTE15%TYPE                     
                                 ,leg_source_system                                     xxar_cashonaccnt_ext_r12.LEG_SOURCE_SYSTEM%TYPE               
                                 ,leg_request_id                                        xxar_cashonaccnt_ext_r12.LEG_REQUEST_ID%TYPE                  
                                 ,leg_seq_num                                           xxar_cashonaccnt_ext_r12.LEG_SEQ_NUM%TYPE                     
                                 ,leg_process_flag                                      xxar_cashonaccnt_ext_r12.LEG_PROCESS_FLAG%TYPE                
                                 ,receipt_id                                            xxar_cashonaccnt_ext_r12.RECEIPT_ID%TYPE               
                                 );
   
   -- PLSQL Table based on Record Type
   TYPE coa_ext_tbl IS TABLE OF coa_ext_rec
   INDEX BY BINARY_INTEGER;

   l_coa_ext_tbl      coa_ext_tbl;
   l_err_record       NUMBER;


   -- Extraction Table cursor
      -- Extraction Table cursor
   CURSOR cur_ext_coa
   IS
      SELECT         xcer.interface_txn_id                  
                    ,xcer.batch_id                          
                    ,xcer.run_sequence_id                   
                    ,xcer.leg_receipt_method                
                    ,xcer.leg_receipt_number                
                    ,xcer.leg_receipt_amount                
                    ,xcer.leg_currency_code                 
                    ,xcer.leg_receipt_date                  
                    ,xcer.leg_operating_unit                
                    ,xcer.leg_gl_date                       
                    ,xcer.leg_maturity_date                 
                    ,xcer.leg_functional_amount             
                    ,xcer.leg_exchange_rate_date            
                    ,xcer.leg_exchange_rate_type            
                    ,xcer.leg_exchange_rate                 
                    ,xcer.leg_unidentified_balances         
                    ,xcer.leg_onaccount_balances            
                    ,xcer.leg_unapplied_balances            
                    ,xcer.leg_rcpt_attribute_category       
                    ,xcer.leg_rcpt_attribute1               
                    ,xcer.leg_rcpt_attribute2               
                    ,xcer.leg_rcpt_attribute3               
                    ,xcer.leg_rcpt_attribute4               
                    ,xcer.leg_rcpt_attribute5               
                    ,xcer.leg_rcpt_attribute6               
                    ,xcer.leg_rcpt_attribute7               
                    ,xcer.leg_rcpt_attribute8               
                    ,xcer.leg_rcpt_attribute9               
                    ,xcer.leg_rcpt_attribute10              
                    ,xcer.leg_rcpt_attribute11              
                    ,xcer.leg_rcpt_attribute12              
                    ,xcer.leg_rcpt_attribute13              
                    ,xcer.leg_rcpt_attribute14              
                    ,xcer.leg_rcpt_attribute15              
                    ,xcer.leg_customer_name                 
                    ,xcer.leg_customer_number               
                    ,xcer.leg_bill_to_location              
                    ,xcer.leg_customer_bank_name            
                    ,xcer.leg_customer_bank_account         
                    ,xcer.leg_reference                     
                    ,xcer.leg_postmark_date                 
                    ,xcer.leg_comments                      
                    ,xcer.leg_deposit_date                  
                    ,xcer.leg_remittance_override           
                    ,xcer.leg_remittance_bank_currency      
                    ,xcer.leg_notes_rec_issuer_name         
                    ,xcer.leg_notes_rec_issue_date          
                    ,xcer.leg_notes_rec_issuer_bank         
                    ,xcer.leg_notes_rec_issuer_bk_branch    
                    ,xcer.leg_apply_to                      
                    ,xcer.leg_apply_date                    
                    ,xcer.leg_amount_applied                
                    ,xcer.leg_app_attribute_category        
                    ,xcer.leg_app_attribute1                
                    ,xcer.leg_app_attribute2                
                    ,xcer.leg_app_attribute3                
                    ,xcer.leg_app_attribute4                
                    ,xcer.leg_app_attribute5                
                    ,xcer.leg_app_attribute6                
                    ,xcer.leg_app_attribute7                
                    ,xcer.leg_app_attribute8                
                    ,xcer.leg_app_attribute9                
                    ,xcer.leg_app_attribute10               
                    ,xcer.leg_app_attribute11               
                    ,xcer.leg_app_attribute12               
                    ,xcer.leg_app_attribute13               
                    ,xcer.leg_app_attribute14               
                    ,xcer.leg_app_attribute15               
                    ,xcer.receipt_method                    
                    ,xcer.receipt_method_id                 
                    ,xcer.pay_from_customer                 
                    ,xcer.customer_site_use_id              
                    ,xcer.ou_name                           
                    ,xcer.conversion_type                   
                    ,xcer.conversion_date                   
                    ,xcer.conversion_rate                   
                    ,xcer.org_id                            
                    ,xcer.gl_date                           
                    ,xcer.receipt_number                    
                    ,xcer.receipt_amount                    
                    ,xcer.receipt_currency                  
                    ,xcer.receipt_date                      
                    ,xcer.maturity_date                     
                    ,xcer.unidentified_balances             
                    ,xcer.onaccount_balances                
                    ,xcer.unapplied_balances                
                    ,xcer.reference                         
                    ,xcer.postmark_date                     
                    ,xcer.comments                          
                    ,xcer.deposit_date                      
                    ,xcer.remittance_override               
                    ,xcer.remittance_bank_currency          
                    ,xcer.notes_rec_issuer_name             
                    ,xcer.notes_rec_issue_date              
                    ,xcer.notes_rec_issuer_bank             
                    ,xcer.notes_rec_issuer_bank_branch      
                    ,xcer.apply_to                          
                    ,xcer.apply_date                        
                    ,xcer.amount_applied                    
                    ,xcer.creation_date                     
                    ,xcer.created_by                        
                    ,xcer.last_updated_date                 
                    ,xcer.last_updated_by                   
                    ,xcer.last_update_login                 
                    ,xcer.program_application_id            
                    ,xcer.program_id                        
                    ,xcer.program_update_date               
                    ,xcer.request_id                          
                    ,xcer.process_flag                      
                    ,xcer.error_type                        
                    ,xcer.attribute_category                
                    ,xcer.attribute1                        
                    ,xcer.attribute2                        
                    ,xcer.attribute3                        
                    ,xcer.attribute4                        
                    ,xcer.attribute5                        
                    ,xcer.attribute6                        
                    ,xcer.attribute7                        
                    ,xcer.attribute8                        
                    ,xcer.attribute9                        
                    ,xcer.attribute10                       
                    ,xcer.attribute11                       
                    ,xcer.attribute12                       
                    ,xcer.attribute13                       
                    ,xcer.attribute14                       
                    ,xcer.attribute15                       
                    ,xcer.leg_source_system                 
                    ,xcer.leg_request_id                    
                    ,xcer.leg_seq_num                       
                    ,xcer.leg_process_flag                  
                    ,xcer.receipt_id               
      FROM xxar_cashonaccnt_ext_r12 xcer
      WHERE xcer.leg_process_flag = g_validated
      AND NOT EXISTS ( SELECT 1
                       FROM xxar_cashonaccnt_stg xcs
                       WHERE xcs.interface_txn_id = xcer.interface_txn_id );

   BEGIN

      xxetn_debug_pkg.add_debug (' + get_data +');
      l_return_status     := fnd_api.g_ret_sts_success;
      l_error_message     := NULL;
      g_total_count       := 0;
      g_failed_count      := 0;

   -- Open Cursor
   OPEN cur_ext_coa;
   LOOP

      l_coa_ext_tbl.DELETE;

      FETCH cur_ext_coa BULK COLLECT
      INTO l_coa_ext_tbl LIMIT 1000;  --limit size of Bulk Collect

      -- Get Total Count
      g_total_count := g_total_count + l_coa_ext_tbl.COUNT;

      EXIT WHEN l_coa_ext_tbl.COUNT = 0;

      BEGIN

         -- Bulk Insert into Conversion table
         FORALL indx IN 1 .. l_coa_ext_tbl.COUNT SAVE EXCEPTIONS
            INSERT INTO xxar_cashonaccnt_stg
                  (  interface_txn_id                  
                    ,batch_id                          
                    ,run_sequence_id                   
                    ,leg_receipt_method                
                    ,leg_receipt_number                
                    ,leg_receipt_amount                
                    ,leg_currency_code                 
                    ,leg_receipt_date                  
                    ,leg_operating_unit                
                    ,leg_gl_date                       
                    ,leg_maturity_date                 
                    ,leg_functional_amount             
                    ,leg_exchange_rate_date            
                    ,leg_exchange_rate_type            
                    ,leg_exchange_rate                 
                    ,leg_unidentified_balances         
                    ,leg_onaccount_balances            
                    ,leg_unapplied_balances            
                    ,leg_rcpt_attribute_category       
                    ,leg_rcpt_attribute1               
                    ,leg_rcpt_attribute2               
                    ,leg_rcpt_attribute3               
                    ,leg_rcpt_attribute4               
                    ,leg_rcpt_attribute5               
                    ,leg_rcpt_attribute6               
                    ,leg_rcpt_attribute7               
                    ,leg_rcpt_attribute8               
                    ,leg_rcpt_attribute9               
                    ,leg_rcpt_attribute10              
                    ,leg_rcpt_attribute11              
                    ,leg_rcpt_attribute12              
                    ,leg_rcpt_attribute13              
                    ,leg_rcpt_attribute14              
                    ,leg_rcpt_attribute15              
                    ,leg_customer_name                 
                    ,leg_customer_number               
                    ,leg_bill_to_location              
                    ,leg_customer_bank_name            
                    ,leg_customer_bank_account         
                    ,leg_reference                     
                    ,leg_postmark_date                 
                    ,leg_comments                      
                    ,leg_deposit_date                  
                    ,leg_remittance_override           
                    ,leg_remittance_bank_currency      
                    ,leg_notes_rec_issuer_name         
                    ,leg_notes_rec_issue_date          
                    ,leg_notes_rec_issuer_bank         
                    ,leg_notes_rec_issuer_bk_branch    
                    ,leg_apply_to                      
                    ,leg_apply_date                    
                    ,leg_amount_applied                
                    ,leg_app_attribute_category        
                    ,leg_app_attribute1                
                    ,leg_app_attribute2                
                    ,leg_app_attribute3                
                    ,leg_app_attribute4                
                    ,leg_app_attribute5                
                    ,leg_app_attribute6                
                    ,leg_app_attribute7                
                    ,leg_app_attribute8                
                    ,leg_app_attribute9                
                    ,leg_app_attribute10               
                    ,leg_app_attribute11               
                    ,leg_app_attribute12               
                    ,leg_app_attribute13               
                    ,leg_app_attribute14               
                    ,leg_app_attribute15               
                    ,receipt_method                    
                    ,receipt_method_id                 
                    ,pay_from_customer                 
                    ,customer_site_use_id              
                    ,ou_name                           
                    ,conversion_type                   
                    ,conversion_date                   
                    ,conversion_rate                   
                    ,org_id                            
                    ,gl_date                           
                    ,receipt_number                    
                    ,receipt_amount                    
                    ,receipt_currency                  
                    ,receipt_date                      
                    ,maturity_date                     
                    ,unidentified_balances             
                    ,onaccount_balances                
                    ,unapplied_balances                
                    ,reference                         
                    ,postmark_date                     
                    ,comments                          
                    ,deposit_date                      
                    ,remittance_override               
                    ,remittance_bank_currency          
                    ,notes_rec_issuer_name             
                    ,notes_rec_issue_date              
                    ,notes_rec_issuer_bank             
                    ,notes_rec_issuer_bank_branch      
                    ,apply_to                          
                    ,apply_date                        
                    ,amount_applied                    
                    ,creation_date                     
                    ,created_by                        
                    ,last_updated_date                 
                    ,last_updated_by                   
                    ,last_update_login                 
                    ,program_application_id            
                    ,program_id                        
                    ,program_update_date               
                    ,request_id                        
                    ,process_flag                      
                    ,error_type                        
                    ,attribute_category                
                    ,attribute1                        
                    ,attribute2                        
                    ,attribute3                        
                    ,attribute4                        
                    ,attribute5                        
                    ,attribute6                        
                    ,attribute7                        
                    ,attribute8                        
                    ,attribute9                        
                    ,attribute10                       
                    ,attribute11                       
                    ,attribute12                       
                    ,attribute13                       
                    ,attribute14                       
                    ,attribute15                       
                    ,leg_source_system                 
                    ,leg_request_id                    
                    ,leg_seq_num                       
                    ,leg_process_flag                  
                    ,receipt_id    
                )         
                VALUES ( l_coa_ext_tbl (indx).interface_txn_id                                           
                    ,l_coa_ext_tbl (indx).batch_id                          
                    ,l_coa_ext_tbl (indx).run_sequence_id                   
                    ,l_coa_ext_tbl (indx).leg_receipt_method                
                    ,l_coa_ext_tbl (indx).leg_receipt_number                
                    ,l_coa_ext_tbl (indx).leg_receipt_amount                
                    ,l_coa_ext_tbl (indx).leg_currency_code                 
                    ,l_coa_ext_tbl (indx).leg_receipt_date                  
                    ,l_coa_ext_tbl (indx).leg_operating_unit                
                    ,l_coa_ext_tbl (indx).leg_gl_date                       
                    ,l_coa_ext_tbl (indx).leg_maturity_date                 
                    ,l_coa_ext_tbl (indx).leg_functional_amount             
                    ,l_coa_ext_tbl (indx).leg_exchange_rate_date            
                    ,l_coa_ext_tbl (indx).leg_exchange_rate_type            
                    ,l_coa_ext_tbl (indx).leg_exchange_rate                 
                    ,l_coa_ext_tbl (indx).leg_unidentified_balances         
                    ,l_coa_ext_tbl (indx).leg_onaccount_balances            
                    ,l_coa_ext_tbl (indx).leg_unapplied_balances            
                    ,l_coa_ext_tbl (indx).leg_rcpt_attribute_category       
                    ,l_coa_ext_tbl (indx).leg_rcpt_attribute1               
                    ,l_coa_ext_tbl (indx).leg_rcpt_attribute2               
                    ,l_coa_ext_tbl (indx).leg_rcpt_attribute3               
                    ,l_coa_ext_tbl (indx).leg_rcpt_attribute4               
                    ,l_coa_ext_tbl (indx).leg_rcpt_attribute5               
                    ,l_coa_ext_tbl (indx).leg_rcpt_attribute6               
                    ,l_coa_ext_tbl (indx).leg_rcpt_attribute7               
                    ,l_coa_ext_tbl (indx).leg_rcpt_attribute8               
                    ,l_coa_ext_tbl (indx).leg_rcpt_attribute9               
                    ,l_coa_ext_tbl (indx).leg_rcpt_attribute10              
                    ,l_coa_ext_tbl (indx).leg_rcpt_attribute11              
                    ,l_coa_ext_tbl (indx).leg_rcpt_attribute12              
                    ,l_coa_ext_tbl (indx).leg_rcpt_attribute13              
                    ,l_coa_ext_tbl (indx).leg_rcpt_attribute14              
                    ,l_coa_ext_tbl (indx).leg_rcpt_attribute15              
                    ,l_coa_ext_tbl (indx).leg_customer_name                 
                    ,l_coa_ext_tbl (indx).leg_customer_number               
                    ,l_coa_ext_tbl (indx).leg_bill_to_location              
                    ,l_coa_ext_tbl (indx).leg_customer_bank_name            
                    ,l_coa_ext_tbl (indx).leg_customer_bank_account         
                    ,l_coa_ext_tbl (indx).leg_reference                     
                    ,l_coa_ext_tbl (indx).leg_postmark_date                 
                    ,l_coa_ext_tbl (indx).leg_comments                      
                    ,l_coa_ext_tbl (indx).leg_deposit_date                  
                    ,l_coa_ext_tbl (indx).leg_remittance_override           
                    ,l_coa_ext_tbl (indx).leg_remittance_bank_currency      
                    ,l_coa_ext_tbl (indx).leg_notes_rec_issuer_name         
                    ,l_coa_ext_tbl (indx).leg_notes_rec_issue_date          
                    ,l_coa_ext_tbl (indx).leg_notes_rec_issuer_bank         
                    ,l_coa_ext_tbl (indx).leg_notes_rec_issuer_bk_branch    
                    ,l_coa_ext_tbl (indx).leg_apply_to                      
                    ,l_coa_ext_tbl (indx).leg_apply_date                    
                    ,l_coa_ext_tbl (indx).leg_amount_applied                
                    ,l_coa_ext_tbl (indx).leg_app_attribute_category        
                    ,l_coa_ext_tbl (indx).leg_app_attribute1                
                    ,l_coa_ext_tbl (indx).leg_app_attribute2                
                    ,l_coa_ext_tbl (indx).leg_app_attribute3                
                    ,l_coa_ext_tbl (indx).leg_app_attribute4                
                    ,l_coa_ext_tbl (indx).leg_app_attribute5                
                    ,l_coa_ext_tbl (indx).leg_app_attribute6                
                    ,l_coa_ext_tbl (indx).leg_app_attribute7                
                    ,l_coa_ext_tbl (indx).leg_app_attribute8                
                    ,l_coa_ext_tbl (indx).leg_app_attribute9                
                    ,l_coa_ext_tbl (indx).leg_app_attribute10               
                    ,l_coa_ext_tbl (indx).leg_app_attribute11               
                    ,l_coa_ext_tbl (indx).leg_app_attribute12               
                    ,l_coa_ext_tbl (indx).leg_app_attribute13               
                    ,l_coa_ext_tbl (indx).leg_app_attribute14               
                    ,l_coa_ext_tbl (indx).leg_app_attribute15               
                    ,l_coa_ext_tbl (indx).receipt_method                    
                    ,l_coa_ext_tbl (indx).receipt_method_id                 
                    ,l_coa_ext_tbl (indx).pay_from_customer                 
                    ,l_coa_ext_tbl (indx).customer_site_use_id              
                    ,l_coa_ext_tbl (indx).ou_name                           
                    ,l_coa_ext_tbl (indx).conversion_type                   
                    ,l_coa_ext_tbl (indx).conversion_date                   
                    ,l_coa_ext_tbl (indx).conversion_rate                   
                    ,l_coa_ext_tbl (indx).org_id                            
                    ,l_coa_ext_tbl (indx).gl_date                           
                    ,l_coa_ext_tbl (indx).receipt_number                    
                    ,l_coa_ext_tbl (indx).receipt_amount                    
                    ,l_coa_ext_tbl (indx).receipt_currency                  
                    ,l_coa_ext_tbl (indx).receipt_date                      
                    ,l_coa_ext_tbl (indx).maturity_date                     
                    ,l_coa_ext_tbl (indx).unidentified_balances             
                    ,l_coa_ext_tbl (indx).onaccount_balances                
                    ,l_coa_ext_tbl (indx).unapplied_balances                
                    ,l_coa_ext_tbl (indx).reference                         
                    ,l_coa_ext_tbl (indx).postmark_date                     
                    ,l_coa_ext_tbl (indx).comments                          
                    ,l_coa_ext_tbl (indx).deposit_date                      
                    ,l_coa_ext_tbl (indx).remittance_override               
                    ,l_coa_ext_tbl (indx).remittance_bank_currency          
                    ,l_coa_ext_tbl (indx).notes_rec_issuer_name             
                    ,l_coa_ext_tbl (indx).notes_rec_issue_date              
                    ,l_coa_ext_tbl (indx).notes_rec_issuer_bank             
                    ,l_coa_ext_tbl (indx).notes_rec_issuer_bank_branch      
                    ,l_coa_ext_tbl (indx).apply_to                          
                    ,l_coa_ext_tbl (indx).apply_date                        
                    ,l_coa_ext_tbl (indx).amount_applied                    
                    ,SYSDATE                     
                    ,g_user_id                        
                    ,SYSDATE                 
                    ,g_user_id                   
                    ,g_login_id                 
                    ,g_prog_appl_id            
                    ,g_conc_program_id                        
                    ,SYSDATE               
                    ,g_request_id                        
                    ,l_coa_ext_tbl (indx).process_flag                      
                    ,l_coa_ext_tbl (indx).error_type                        
                    ,l_coa_ext_tbl (indx).attribute_category                
                    ,l_coa_ext_tbl (indx).attribute1                        
                    ,l_coa_ext_tbl (indx).attribute2                        
                    ,l_coa_ext_tbl (indx).attribute3                        
                    ,l_coa_ext_tbl (indx).attribute4                        
                    ,l_coa_ext_tbl (indx).attribute5                        
                    ,l_coa_ext_tbl (indx).attribute6                        
                    ,l_coa_ext_tbl (indx).attribute7                        
                    ,l_coa_ext_tbl (indx).attribute8                        
                    ,l_coa_ext_tbl (indx).attribute9                        
                    ,l_coa_ext_tbl (indx).attribute10                       
                    ,l_coa_ext_tbl (indx).attribute11                       
                    ,l_coa_ext_tbl (indx).attribute12                       
                    ,l_coa_ext_tbl (indx).attribute13                       
                    ,l_coa_ext_tbl (indx).attribute14                       
                    ,l_coa_ext_tbl (indx).attribute15                       
                    ,l_coa_ext_tbl (indx).leg_source_system                 
                    ,l_coa_ext_tbl (indx).leg_request_id                    
                    ,l_coa_ext_tbl (indx).leg_seq_num                       
                    ,l_coa_ext_tbl (indx).leg_process_flag                  
                    ,l_coa_ext_tbl (indx).receipt_id                            
                   );

      EXCEPTION
         WHEN OTHERS THEN
            FOR l_indx_exp IN 1 .. SQL%BULK_EXCEPTIONS.COUNT
            LOOP
               l_err_record:= l_coa_ext_tbl(SQL%BULK_EXCEPTIONS(l_indx_exp).ERROR_INDEX).interface_txn_id;
               g_retcode := '1';
               fnd_file.PUT_LINE(fnd_file.LOG,'Record sequence : ' || l_coa_ext_tbl(SQL%BULK_EXCEPTIONS(l_indx_exp).error_index).interface_txn_id);
               fnd_file.PUT_LINE(fnd_file.LOG,'Error Message : ' ||SQLERRM(-SQL%BULK_EXCEPTIONS(l_indx_exp).error_code));

               -- Updating Leg_process_flag to 'E' for failed records
               UPDATE xxar_cashonaccnt_ext_r12 xcer
               SET    leg_process_flag   = g_error
                     ,last_updated_by    = g_user_id
                     ,last_update_login  = g_login_id
                     ,last_updated_date  = SYSDATE
               WHERE  xcer.interface_txn_id = l_err_record
               AND    xcer.leg_process_flag = g_validated;

               g_failed_count := g_failed_count + SQL%ROWCOUNT;

               l_error_message := l_error_message ||' ~~ '|| SQLERRM ( -SQL%BULK_EXCEPTIONS ( l_indx_exp ).ERROR_CODE );
            END LOOP;
      END;

   END LOOP;
   CLOSE cur_ext_coa;  -- Close Cursor

   COMMIT;

   -- Update Successful records in Extraction Table
   UPDATE xxar_cashonaccnt_ext_r12 stg2
      SET    leg_process_flag   = g_processed
            ,last_updated_by    = g_user_id
            ,last_update_login  = g_login_id
            ,last_updated_date  = SYSDATE
      WHERE  leg_process_flag   = g_validated
      AND    EXISTS 
            ( SELECT 1
              FROM    xxar_cashonaccnt_stg stg1
              WHERE   stg1.interface_txn_id  = stg2.interface_txn_id
              );

   COMMIT;
   xxetn_debug_pkg.add_debug (' - get_data -');

EXCEPTION
   WHEN OTHERS THEN
      g_retcode := 2;
      l_error_message := SUBSTR('In Exception while loading data'||SQLERRM,1,1999);
      print_log_message(l_error_message);
      print_log_message(' - get_data -');
      ROLLBACK;

END get_data;
   
   --
   -- ========================
   -- Procedure: mandatory_value_check
   -- =============================================================================
   --   This procedure to do mandatory value check
   -- =============================================================================
   --  Input Parameters :
   --   piv_receipt_method  
   --   piv_receipt_number 
   --   pin_amount         
   --   piv_currency_code  
   --   pid_receipt_date   
   --   piv_operating_unit 
   --   piv_apply_to       
   --   pin_amount_app     
   --   pin_on_acc_bal     
   --   pin_unapp_bal      
   
   
   --  Output Parameters :
   --   pon_error_cnt    : Return Status
   -- -----------------------------------------------------------------------------
   --
   PROCEDURE mandatory_value_check (   
     piv_receipt_method  IN      VARCHAR2
   , piv_receipt_number  IN      VARCHAR2
   , pin_amount          IN      NUMBER
   , piv_currency_code   IN      VARCHAR2
   , pid_receipt_date    IN      DATE
   , piv_operating_unit  IN      VARCHAR2
   , piv_apply_to        IN      VARCHAR2
   , pin_amount_app      IN      NUMBER   
   , pin_on_acc_bal      IN      NUMBER
   , pin_unapp_bal       IN      NUMBER
   , pon_error_cnt       OUT     NUMBER
   )
   IS
      l_record_cnt             NUMBER;
      l_err_msg                VARCHAR2 ( 2000 );
      l_log_ret_status         VARCHAR2 ( 50 );
      l_log_err_msg            VARCHAR2 ( 2000 );
      l_err_code               VARCHAR2 ( 40 ) := NULL;
   BEGIN
      print_log_message ( '   PROCEDURE : mandatory_value_check' );
      l_record_cnt := 0;
      l_err_msg   := NULL;
      l_log_ret_status := NULL;
      l_log_err_msg := NULL;
      l_err_code  := NULL;
      
         IF piv_receipt_method IS NULL          
         THEN
            xxetn_debug_pkg.add_debug ( 'Mandatory Value missing on record.');
            l_err_code        := 'ETN_AR_MANDATORY_NOT_ENTERED';
            l_err_msg         := 'Error: Mandatory Value missing on record.';
         
            log_errors ( pov_return_status          =>   l_log_ret_status          -- OUT
                       , pov_error_msg              =>   l_log_err_msg             -- OUT
                       , piv_source_column_name     =>   'LEG_RECEIPT_METHOD'
                       , piv_source_column_value    =>   piv_receipt_method
                       , piv_error_type             =>   g_err_val
                       , piv_error_code             =>   l_err_code
                       , piv_error_message          =>   l_err_msg
                       );
         END IF;
         
         --Mandatory Column check
         IF  piv_receipt_number IS NULL          
         THEN
            l_record_cnt := 2;
            xxetn_debug_pkg.add_debug ( 'Mandatory Value missing on record.');
            l_err_code        := 'ETN_AR_MANDATORY_NOT_ENTERED';
            l_err_msg         := 'Error: Mandatory Value missing on record.';
         
            log_errors ( pov_return_status          =>   l_log_ret_status          -- OUT
                       , pov_error_msg              =>   l_log_err_msg             -- OUT
                       , piv_source_column_name     =>   'LEG_RECEIPT_NUMBER'
                       , piv_source_column_value    =>   piv_receipt_number
                       , piv_error_type             =>   g_err_val
                       , piv_error_code             =>   l_err_code
                       , piv_error_message          =>   l_err_msg
                       );
         END IF;        
         
         
         --Mandatory Column check
         IF pin_amount IS NULL           
         THEN
            l_record_cnt := 2;
            xxetn_debug_pkg.add_debug ( 'Mandatory Value missing on record.');
            l_err_code        := 'ETN_AR_MANDATORY_NOT_ENTERED';
            l_err_msg         := 'Error: Mandatory Value missing on record.';
         
            log_errors ( pov_return_status          =>   l_log_ret_status          -- OUT
                       , pov_error_msg              =>   l_log_err_msg             -- OUT
                       , piv_source_column_name     =>   'LEG_RECEIPT_AMOUNT'
                       , piv_source_column_value    =>   pin_amount
                       , piv_error_type             =>   g_err_val
                       , piv_error_code             =>   l_err_code
                       , piv_error_message          =>   l_err_msg
                       );
         END IF;         
         
         
         --Mandatory Column check
         IF piv_currency_code IS NULL           
         THEN
            l_record_cnt := 2;
            xxetn_debug_pkg.add_debug ( 'Mandatory Value missing on record.');
            l_err_code        := 'ETN_AR_MANDATORY_NOT_ENTERED';
            l_err_msg         := 'Error: Mandatory Value missing on record.';
         
            log_errors ( pov_return_status          =>   l_log_ret_status          -- OUT
                       , pov_error_msg              =>   l_log_err_msg             -- OUT
                       , piv_source_column_name     =>   'LEG_CURRENCY_CODE'
                       , piv_source_column_value    =>   piv_currency_code
                       , piv_error_type             =>   g_err_val
                       , piv_error_code             =>   l_err_code
                       , piv_error_message          =>   l_err_msg
                       );
         END IF;         
         
         
         --Mandatory Column check
         IF pid_receipt_date IS NULL          
         THEN
            l_record_cnt := 2;
            xxetn_debug_pkg.add_debug ( 'Mandatory Value missing on record.');
            l_err_code        := 'ETN_AR_MANDATORY_NOT_ENTERED';
            l_err_msg         := 'Error: Mandatory Value missing on record.';
         
            log_errors ( pov_return_status          =>   l_log_ret_status          -- OUT
                       , pov_error_msg              =>   l_log_err_msg             -- OUT
                       , piv_source_column_name     =>   'LEG_RECEIPT_DATE'
                       , piv_source_column_value    =>   pid_receipt_date
                       , piv_error_type             =>   g_err_val
                       , piv_error_code             =>   l_err_code
                       , piv_error_message          =>   l_err_msg
                       );
         END IF;         
         
         
         --Mandatory Column check
         IF piv_operating_unit IS NULL           
         THEN
            l_record_cnt := 2;
            xxetn_debug_pkg.add_debug ( 'Mandatory Value missing on record.');
            l_err_code        := 'ETN_AR_MANDATORY_NOT_ENTERED';
            l_err_msg         := 'Error: Mandatory Value missing on record.';
         
            log_errors ( pov_return_status          =>   l_log_ret_status          -- OUT
                       , pov_error_msg              =>   l_log_err_msg             -- OUT
                       , piv_source_column_name     =>   'LEG_OPERATING_UNIT'
                       , piv_source_column_value    =>   piv_operating_unit
                       , piv_error_type             =>   g_err_val
                       , piv_error_code             =>   l_err_code
                       , piv_error_message          =>   l_err_msg
                        );
         END IF;         
         
         
         --Mandatory Column check
         IF piv_apply_to IS NULL           
         THEN
            l_record_cnt := 2;
            xxetn_debug_pkg.add_debug ( 'Mandatory Value missing on record.');
            l_err_code        := 'ETN_AR_MANDATORY_NOT_ENTERED';
            l_err_msg         := 'Error: Mandatory Value missing on record.';
         
            log_errors ( pov_return_status          =>   l_log_ret_status          -- OUT
                       , pov_error_msg              =>   l_log_err_msg             -- OUT
                       , piv_source_column_name     =>   'LEG_APPLY_TO'
                       , piv_source_column_value    =>   piv_apply_to
                       , piv_error_type             =>   g_err_val
                       , piv_error_code             =>   l_err_code
                       , piv_error_message          =>   l_err_msg
                       );
         END IF;
         
         
         --Mandatory Column check
         IF pin_amount_app IS NULL           
         THEN
            l_record_cnt := 2;
            xxetn_debug_pkg.add_debug ( 'Mandatory Value missing on record.');
            l_err_code        := 'ETN_AR_MANDATORY_NOT_ENTERED';
            l_err_msg         := 'Error: Mandatory Value missing on record.';
         
            log_errors ( pov_return_status          =>   l_log_ret_status          -- OUT
                       , pov_error_msg              =>   l_log_err_msg             -- OUT
                       , piv_source_column_name     =>   'LEG_AMOUNT_APPLIED'
                       , piv_source_column_value    =>   pin_amount_app
                       , piv_error_type             =>   g_err_val
                       , piv_error_code             =>   l_err_code
                       , piv_error_message          =>   l_err_msg
                       );
         END IF;         
         
         
         --Mandatory Column check
         IF pin_on_acc_bal IS NULL           
         THEN
            l_record_cnt := 2;
            xxetn_debug_pkg.add_debug ( 'Mandatory Value missing on record.');
            l_err_code        := 'ETN_AR_MANDATORY_NOT_ENTERED';
            l_err_msg         := 'Error: Mandatory Value missing on record.';
         
            log_errors ( pov_return_status          =>   l_log_ret_status          -- OUT
                       , pov_error_msg              =>   l_log_err_msg             -- OUT
                       , piv_source_column_name     =>   'LEG_ONACCOUNT_BALANCES'
                       , piv_source_column_value    =>   pin_on_acc_bal
                       , piv_error_type             =>   g_err_val
                       , piv_error_code             =>   l_err_code
                       , piv_error_message          =>   l_err_msg
                       );
         END IF;         
         
         
         --Mandatory Column check
         IF pin_unapp_bal IS NULL           
         THEN
            l_record_cnt := 2;
            xxetn_debug_pkg.add_debug ( 'Mandatory Value missing on record.');
            l_err_code        := 'ETN_AR_MANDATORY_NOT_ENTERED';
            l_err_msg         := 'Error: Mandatory Value missing on record.';
         
            log_errors ( pov_return_status          =>   l_log_ret_status          -- OUT
                       , pov_error_msg              =>   l_log_err_msg             -- OUT
                       , piv_source_column_name     =>   'LEG_UNAPPLIED_BALANCES'
                       , piv_source_column_value    =>   pin_unapp_bal
                       , piv_error_type             =>   g_err_val
                       , piv_error_code             =>   l_err_code
                       , piv_error_message          =>   l_err_msg
                       );
         END IF;
         
      IF l_record_cnt > 1 
      THEN
         pon_error_cnt := 2;
      END IF;
   EXCEPTION
      WHEN OTHERS 
      THEN
         g_retcode   := 2;
         pon_error_cnt := 2;
         print_log_message ('In Exception mandatory_value_check check' || SQLERRM );
   END mandatory_value_check;
   
   --
   -- ========================
   -- Procedure: validate_gl_period
   -- =============================================================================
   --   This procedure validate_gl_period
   -- =============================================================================
   --  Input Parameters :
   --  pin_sob_id  : Set of books ID 
   
   --  Output Parameters :
   --  pon_error_cnt    : Return Status
   -- -----------------------------------------------------------------------------
   --
   PROCEDURE validate_gl_period ( pin_sob_id         IN   NUMBER
                                 ,pon_error_cnt      OUT  NUMBER
                                 )
   IS
      l_record_cnt       NUMBER;
      
   BEGIN
     
      xxetn_debug_pkg.add_debug ( ' +  PROCEDURE : validate_gl_period  '|| g_gl_date||' + ');
      
      l_record_cnt      := 0;
        
      BEGIN 
         --check if the GL period is open for SQLGL
         SELECT 1
           INTO l_record_cnt
           FROM gl_period_statuses gps
              , fnd_application fa
              , gl_ledgers gl
          WHERE gl.accounted_period_type  = gps.period_type
            AND gl.ledger_id              = gps.ledger_id
            AND fa.application_short_name = 'SQLGL'
            AND fa.application_id         = gps.application_id
            AND gps.set_of_books_id       = pin_sob_id
            AND gps.closing_status        = 'O'
            AND g_gl_date BETWEEN gps.start_date AND gps.end_date;
   
      EXCEPTION
         WHEN NO_DATA_FOUND 
         THEN
           l_record_cnt := 2;
           print_log_message ( 'In No Data found of gl period check'||SQLERRM);
         WHEN OTHERS 
         THEN
           l_record_cnt := 2;
           print_log_message ( 'In When others of gl period check'||SQLERRM);
      END;
      
      IF l_record_cnt = 2  THEN
         pon_error_cnt := 2;
      END IF;
      
      BEGIN
        --Check if the GL period is open for AR
        SELECT 1
          INTO l_record_cnt
          FROM gl_period_statuses gps
             , fnd_application fa
             , gl_ledgers gl
         WHERE gl.accounted_period_type  = gps.period_type
           AND gl.ledger_id              = gps.ledger_id
           AND fa.application_short_name = 'AR'
           AND fa.application_id         = gps.application_id
           AND gps.set_of_books_id       = pin_sob_id
           AND gps.closing_status        = 'O'
           AND g_gl_date          BETWEEN gps.start_date AND gps.end_date;
   
      EXCEPTION
         WHEN NO_DATA_FOUND THEN
           l_record_cnt := 3;
           print_log_message ( 'In No Data found of AR period check'||SQLERRM);
         WHEN OTHERS THEN
           l_record_cnt := 3;
           print_log_message ( 'In When others of AR period check'||SQLERRM);
      END;
      
      IF l_record_cnt = 3  
      THEN
        pon_error_cnt := 3 ;
      END IF;
      print_log_message ( ' -  PROCEDURE : validate_gl_period  '|| g_gl_date||' - ');
   EXCEPTION
      WHEN OTHERS
      THEN
         g_retcode := 2;
         l_record_cnt := 2;
         g_errbuff := 'Failed while validating GL Period.';
         print_log_message ( 'In Exception validate gl period'||SQLERRM);
   END validate_gl_period;
   
   --
   -- ========================
   -- Procedure: validate_currency_code
   -- =============================================================================
   --   This procedure validate_currency_code
   -- =============================================================================
   --  Input Parameters :
   --   piv_currency_code : currency code
   
   --  Output Parameters :
   --  pon_error_cnt    : Return Status
   -- -----------------------------------------------------------------------------
   --
   PROCEDURE validate_currency_code ( piv_currency_code     IN   VARCHAR2
                                     ,pon_error_cnt        OUT   NUMBER
                                    )
   IS
      l_record_cnt       NUMBER;
      
   BEGIN
     
      xxetn_debug_pkg.add_debug ( ' +  PROCEDURE : validate_currency_code = '||piv_currency_code||' + ' );      
      l_record_cnt      := 0;        
      BEGIN   
         --check if the currency code exists in the system
         SELECT 1
           INTO l_record_cnt
           FROM fnd_currencies fc
          WHERE fc.currency_code = piv_currency_code
            AND fc.enabled_flag  = g_yes
            AND fc.currency_flag = g_yes
            AND TRUNC (SYSDATE) BETWEEN TRUNC (
                                          NVL (fc.start_date_active, SYSDATE)
                                       )
                                   AND TRUNC (
                                          NVL (fc.end_date_active, SYSDATE)
                                       );
   
   
   
      EXCEPTION
         WHEN NO_DATA_FOUND 
         THEN
            l_record_cnt := 2;
            print_log_message ( 'In No Data found of currency code check'||SQLERRM);
         WHEN OTHERS 
         THEN
            l_record_cnt := 2;
            print_log_message ( 'In When others of currency code check'||SQLERRM);
      END;
      
      IF l_record_cnt > 1 
      THEN
         pon_error_cnt := 2;
      END IF;
      print_log_message ( ' -  PROCEDURE : validate_currency_code = '||piv_currency_code||' - ' );
   EXCEPTION
      WHEN OTHERS
      THEN
         g_retcode := 2;
         l_record_cnt := 2;
         g_errbuff := 'Failed while validating currency code.';
         print_log_message ( 'In Exception validate currency code'||SQLERRM);
   END validate_currency_code; 
   
   
   --
   -- ========================
   -- Procedure: validate_receipt_method
   -- =============================================================================
   --   This procedure validate_receipt_method
   -- =============================================================================
   --  Input Parameters :
   --   piv_receipt_method -11i receipt method name
   --   pid_receipt_date - the date when receipt was created
   
   --  Output Parameters :
   --  pov_receipt_method - R12 receipt method name
   --  pon_method_id - R12 receipt method ID
   --  pon_error_cnt    : Return Status
   -- -----------------------------------------------------------------------------
   --
   PROCEDURE validate_receipt_method ( piv_receipt_method     IN   VARCHAR2
                                     , pid_receipt_date       IN    DATE
                                     , pov_receipt_method     OUT  VARCHAR2
                                     , pon_method_id          OUT  NUMBER
                                     , pon_error_cnt          OUT  NUMBER
                                   )
   IS
      l_record_cnt              NUMBER;
      l_receipt_method          ar_receipt_methods.NAME%TYPE;
      l_method_id               ar_receipt_methods.receipt_method_id%TYPE;
      l_receipt_lkp    CONSTANT VARCHAR2(50) := 'ETN_AR_RECEIPT_METHOD';
      
   BEGIN
     
      xxetn_debug_pkg.add_debug ( ' +  PROCEDURE : validate_receipt_method = '||piv_receipt_method||' + ' );
      
      l_record_cnt      := 0;
      l_receipt_method  := NULL;
      l_method_id       := NULL;
      
      BEGIN
        
        --Fetch R12 value of the receipt method
         SELECT description
           INTO l_receipt_method
           FROM fnd_lookup_values flv
          WHERE flv.lookup_type =  l_receipt_lkp
            AND flv.enabled_flag = g_yes
            AND flv.meaning = piv_receipt_method
            AND flv.language = USERENV ('LANG')
            AND TRUNC ( SYSDATE ) BETWEEN NVL (flv.start_date_active, TRUNC ( SYSDATE ) )
                                AND     NVL (flv.end_date_active  , TRUNC ( SYSDATE ) );
         
            
      EXCEPTION
         WHEN NO_DATA_FOUND 
         THEN
            l_record_cnt := 2;
            l_receipt_method  := NULL;
            print_log_message ( 'In No Data found of receipt method lookup check'||SQLERRM);
         WHEN OTHERS 
         THEN
            l_record_cnt := 2;
            l_receipt_method  := NULL;
            print_log_message ( 'In When others of receipt method lookup check'||SQLERRM);
      END;
      
      IF l_receipt_method IS NOT NULL 
      THEN
        --Fetch receipt_method_id for the derived receipt method
         BEGIN
            xxetn_debug_pkg.add_debug ( ' +  derivation : validate_receipt_method = '||piv_receipt_method||' + ' );
            SELECT arm.receipt_method_id
              INTO l_method_id
              FROM ar_receipt_methods arm
             WHERE arm.NAME = l_receipt_method
               AND   TRUNC ( pid_receipt_date ) BETWEEN NVL (arm.start_date, TRUNC ( SYSDATE ) )
                               AND     NVL (arm.end_date  , TRUNC ( SYSDATE ) );
            xxetn_debug_pkg.add_debug ( ' -  derivation : validate_receipt_method = '||piv_receipt_method||' - ' );
   
         EXCEPTION
            WHEN NO_DATA_FOUND 
            THEN
               l_record_cnt := 2;
               l_method_id    := NULL;
               print_log_message ( 'In No Data found of receipt method id check'||SQLERRM);
            WHEN OTHERS 
            THEN
               l_record_cnt := 2;
               l_method_id    := NULL;
               print_log_message ( 'In When others of receipt method id check'||SQLERRM);
         END;
      END IF;
      
      xxetn_debug_pkg.add_debug ( 'Receipt Method = '||l_receipt_method);
            
      xxetn_debug_pkg.add_debug ( 'Method id = '||l_method_id);
      
      pov_receipt_method := l_receipt_method;
      pon_method_id      := l_method_id;
      
      IF l_record_cnt > 1 
      THEN
         pon_error_cnt := 2;
      END IF;
      xxetn_debug_pkg.add_debug ( ' -  PROCEDURE : validate_receipt_method = '||piv_receipt_method||' - ' );
   EXCEPTION
      WHEN OTHERS
      THEN
         g_retcode := 2;
         l_record_cnt := 2;
         g_errbuff := 'Failed while validating receipt method.';
         print_log_message ( 'In Exception validate receipt method'||SQLERRM);
   END validate_receipt_method; 
   
   
   --
   -- ========================
   -- Procedure: validate_operating_unit
   -- =============================================================================
   --   This procedure validate_operating_unit
   -- =============================================================================
   --  Input Parameters :
   --   piv_operating_unit - 11i operating unit name
   
   --  Output Parameters :
   --  pov_operating_unit - R12 operating unit name
   --  pon_org_id - R12 organization id
   --  pon_sob_id - set of books ID
   --  pon_error_cnt    : Return Status
   -- -----------------------------------------------------------------------------
   --
   PROCEDURE validate_operating_unit ( piv_operating_unit     IN   VARCHAR2
                                     , pov_operating_unit     OUT  VARCHAR2
                                     , pon_org_id             OUT  NUMBER
                                     , pon_sob_id             OUT  NUMBER
                                     , pon_error_cnt          OUT  NUMBER
                                   )
   IS
      l_record_cnt         NUMBER;
      l_operating_unit     hr_operating_units.name%TYPE;
      l_org_id             hr_operating_units.organization_id%TYPE;
      l_sob_id             hr_operating_units.set_of_books_id%TYPE;
      l_ou_lkp    CONSTANT VARCHAR2(50) := 'ETN_COMMON_OU_MAP';
      
   BEGIN
     
      xxetn_debug_pkg.add_debug ( ' + PROCEDURE : validate_operating_unit = '||piv_operating_unit ||' + ');
      
      l_record_cnt      := 0;
      l_operating_unit  := NULL;
      l_org_id          := NULL;
      l_sob_id          := NULL;
      
      BEGIN
      
         --Derive R12 value for the given operating unit      
         SELECT description
           INTO l_operating_unit
           FROM fnd_lookup_values flv
          WHERE flv.lookup_type =  l_ou_lkp
            AND flv.meaning     =  piv_operating_unit
            AND flv.language    = USERENV ('LANG')
            AND TRUNC ( SYSDATE ) BETWEEN NVL ( flv.start_date_active, TRUNC ( SYSDATE ) )
                                AND     NVL ( flv.end_date_active  , TRUNC ( SYSDATE ) );
            
      EXCEPTION
         WHEN NO_DATA_FOUND 
         THEN
            l_record_cnt := 2;
            l_operating_unit  := NULL;
            l_sob_id          := NULL;
            print_log_message ( 'In No Data found of operating unit lookup check'||SQLERRM);
         WHEN OTHERS 
         THEN
            l_record_cnt := 2;
            l_operating_unit  := NULL;
            l_sob_id          := NULL;
            print_log_message ( 'In When others of operating unit lookup check'||SQLERRM);
      END;
      
      --if operating_unit is not null
      IF l_operating_unit IS NOT NULL 
      THEN
        
         BEGIN
            xxetn_debug_pkg.add_debug ( ' + PROCEDURE : validate_operating_unit...derivation of org_id + ');
            
            --Fetch org_id and sob_id for the R12 value of the operating unit derived
            SELECT hou.organization_id , hou.set_of_books_id
              INTO l_org_id , l_sob_id
              FROM hr_operating_units hou
             WHERE hou.name  = l_operating_unit
               AND TRUNC ( SYSDATE ) BETWEEN NVL (hou.DATE_FROM, TRUNC ( SYSDATE ) )
               AND NVL (hou.DATE_TO  , TRUNC ( SYSDATE ) );
            xxetn_debug_pkg.add_debug ( ' + PROCEDURE : validate_operating_unit...derivation of org_id + ');
         EXCEPTION
            WHEN NO_DATA_FOUND 
            THEN
               l_record_cnt := 2;
               l_org_id    := NULL;
               print_log_message ( 'In No Data found of operating unit check'||SQLERRM);
            WHEN OTHERS 
            THEN
               l_record_cnt := 2;
               l_org_id    := NULL;
               print_log_message ( 'In When others of operating unit check'||SQLERRM);
         END;
      END IF;
      
      xxetn_debug_pkg.add_debug ( 'Operating Unit = '||l_operating_unit);
      
      xxetn_debug_pkg.add_debug ( 'Org Id = '||l_org_id);
      
      xxetn_debug_pkg.add_debug ( 'SOB Id = '||l_sob_id);
      
      
      pon_org_id         := l_org_id;
      pov_operating_unit := l_operating_unit;
      pon_sob_id         := l_sob_id;
      
      IF l_record_cnt > 1 
      THEN
         pon_error_cnt := 2;
      END IF;
      xxetn_debug_pkg.add_debug ( ' - PROCEDURE : validate_operating_unit = '||piv_operating_unit ||' - ');
   EXCEPTION
      WHEN OTHERS
      THEN
         g_retcode := 2;
         l_record_cnt := 2;
         g_errbuff := 'Failed while validating operating unit';
         print_log_message ( 'In Exception Opertaing Unit Validation'||SQLERRM);
   END validate_operating_unit;     
   
   
   --
   -- ========================
   -- Procedure: invalid_check
   -- =============================================================================
   --   This procedure checks for the invalid receipts.
   -- =============================================================================
   --  Input Parameters :
   --   piv_receipt_num - receipt number 
   --   piv_receipt_method - receipt method

   --  Output Parameters :
   --   pon_error_cnt    : Return Status
   -- -----------------------------------------------------------------------------
   --
   PROCEDURE invalid_check (   piv_receipt_num           IN   VARCHAR2
                             , piv_receipt_method        IN   VARCHAR2
                             , pon_error_cnt             OUT  NUMBER
                             )
   IS
      
      l_record_cnt     NUMBER;
      l_amount         NUMBER;
      l_onaccount_bal  NUMBER;
      l_unapplied_bal  NUMBER;
      l_amount_applied NUMBER;
   BEGIN
     
      xxetn_debug_pkg.add_debug ( '  + PROCEDURE : invalid_check  + ' );
      
      l_record_cnt := 0;
      
      BEGIN
         xxetn_debug_pkg.add_debug ( '  + PROCEDURE : invalid_check -- check 1 + ' );
         
         --Receipt amount, on account balance and un-applied balance should be same on all lines for a receipt number and receipt method
         SELECT DISTINCT xcs.leg_receipt_amount,xcs.leg_onaccount_balances,xcs.leg_unapplied_balances 
           INTO l_amount,l_onaccount_bal,l_unapplied_bal
           FROM xxar_cashonaccnt_stg xcs
          WHERE xcs.leg_receipt_number = piv_receipt_num
            AND xcs.leg_receipt_method = piv_receipt_method;
        
      EXCEPTION
         WHEN NO_DATA_FOUND 
         THEN
            l_record_cnt := 2;
            print_log_message ( 'In No Data found of Invalid check 1'||SQLERRM);
        
         WHEN TOO_MANY_ROWS
         THEN
            l_record_cnt := 2;
            print_log_message ( 'In too many rows of Invalid check 1'||SQLERRM);
            print_log_message ( 'More than one value of receipt amount, onaccount balance, unapplied balance Exist');
         WHEN OTHERS 
         THEN
            l_record_cnt := 2;        
            print_log_message ( 'In When others of Invalid check 1'||SQLERRM);
      END;
         xxetn_debug_pkg.add_debug ( '  - PROCEDURE : invalid_check -- check 1 - ' );
      BEGIN
         xxetn_debug_pkg.add_debug ( '  + PROCEDURE : invalid_check -- check 2 + ' );

         --Derive total amount applied for a given receipt number
         SELECT   SUM(xcs.leg_amount_applied)
           INTO   l_amount_applied
           FROM   xxar_cashonaccnt_stg xcs
          WHERE   xcs.leg_receipt_number = piv_receipt_num
       GROUP BY   xcs.leg_receipt_number;
      
         --if receipt amount is not equal to the sum of total amount applied and unapplied balances
         IF (( l_amount_applied + l_unapplied_bal) != l_amount)
         THEN
            l_record_cnt := 2;
            xxetn_debug_pkg.add_debug ( 'Invalid check 2 : Receipt Amount does not equal the sum of total amount applied and unapplied amount'||SQLERRM);
         END IF;        
      
      EXCEPTION
         WHEN NO_DATA_FOUND 
         THEN
            l_record_cnt := 3;
            print_log_message ( 'In No Data found of Invalid check 2'||SQLERRM);
         WHEN OTHERS 
         THEN
            l_record_cnt := 3;        
            print_log_message ( 'In When others of Invalid check 2'||SQLERRM);
      END;

         xxetn_debug_pkg.add_debug ( '  - PROCEDURE : invalid_check -- check 2 - ' );

      IF l_record_cnt = 2 
      THEN
         pon_error_cnt := 2;
      ELSIF l_record_cnt = 3 
      THEN
         pon_error_cnt := 3;
      END IF;
      xxetn_debug_pkg.add_debug ( '  - PROCEDURE : invalid_check  - ' );
   EXCEPTION
      WHEN OTHERS
      THEN
         g_retcode := 2;
         l_record_cnt := 2;
         g_errbuff := ' Failed in Invalid Check for receipt.';
         print_log_message ( 'In Exception Inavlid check'||SQLERRM);
   END invalid_check; 
   
   --
   -- ========================
   -- Procedure: validate_customer_info
   -- =============================================================================
   --   This procedure validate_customer_info
   -- =============================================================================
   --  Input Parameters :
   --   piv_cust_num - customer number
   --   piv_bill_to_code - site location
   --   pin_org_id - organizzation ID
   
   --  Output Parameters :
   --  pon_cust_id - customer account id
   --  pon_bill_to_id - site use id
   --  pon_error_cnt    : Return Status
   -- -----------------------------------------------------------------------------
   --
   PROCEDURE validate_customer_info ( piv_cust_num       IN   VARCHAR2
                                    , piv_bill_to_code   IN   VARCHAR2
                                    , pin_org_id         IN   NUMBER
                                    , pon_cust_id        OUT  NUMBER
                                    , pon_bill_to_id     OUT  NUMBER
                                    , pon_error_cnt      OUT  NUMBER
                                   )
   IS
      
      l_record_cnt        NUMBER;
      l_cust_id           hz_cust_accounts_all.cust_account_id%TYPE;
      l_bill_to_id        hz_cust_site_uses_all.site_use_id%TYPE;      
      
   BEGIN
     
      xxetn_debug_pkg.add_debug ( '+   PROCEDURE : validate_customer_info +' );
      
      xxetn_debug_pkg.add_debug ( ' Legacy Cusotmer Number = '||piv_cust_num);
      
      xxetn_debug_pkg.add_debug ( ' Legacy Bill To Number  = '||piv_bill_to_code);
      
      l_record_cnt     := 0;      
      l_cust_id        := NULL;
      l_bill_to_id     := NULL;
      
      
      BEGIN
         xxetn_debug_pkg.add_debug ( ' Fecth R12 customer detail from cross ref');
         
         --Fetch customer ID according to the customer number
         SELECT hcaa.cust_account_id
           INTO l_cust_id
           FROM hz_cust_accounts_all hcaa
          WHERE hcaa.account_number = piv_cust_num
            AND hcaa.status = 'A';
            
      EXCEPTION
         WHEN NO_DATA_FOUND 
         THEN
            l_record_cnt := 2;
            l_cust_id  := NULL;
            print_log_message ( 'In No Data found of Customer Id check'||SQLERRM);
         WHEN OTHERS 
         THEN
            l_record_cnt := 2;
            l_cust_id  := NULL;
            print_log_message ( 'In When others of Customer Id check'||SQLERRM);
      END;
      
      xxetn_debug_pkg.add_debug ( 'R12 Customer Id = '||l_cust_id);
      
      IF l_cust_id IS NOT NULL 
      THEN
        --Fetch Bill_to id for the given bill_to_code corresponding to the cust_id
         BEGIN        
            SELECT hcsu.site_use_id
              INTO l_bill_to_id
              FROM hz_cust_site_uses_all hcsu,  
                   hz_cust_acct_sites_all hcas
             WHERE hcsu.location = piv_bill_to_code
               AND hcsu.org_id = pin_org_id
               AND hcsu.status = 'A'
               AND hcsu.SITE_USE_CODE = 'BILL_TO'
               and hcsu.cust_acct_site_id = hcas.cust_acct_site_id
               AND hcas.org_id = pin_org_id
               AND hcas.status = 'A'
               AND hcas.cust_account_id = l_cust_id;
        EXCEPTION
           WHEN NO_DATA_FOUND 
           THEN
              l_record_cnt := 2;
              l_bill_to_id    := NULL;
              print_log_message ( 'In No Data found of Bill to Id check'||SQLERRM);
           WHEN OTHERS 
           THEN
              l_record_cnt := 2;
              l_bill_to_id    := NULL;
              print_log_message ( 'In When others of Bill to Id check'||SQLERRM);
        END;        
        
      END IF;
      
      xxetn_debug_pkg.add_debug ( 'Bill to Id = '||l_bill_to_id); 
      
      pon_cust_id                        := l_cust_id;
      pon_bill_to_id                     := l_bill_to_id;
      
      IF l_record_cnt > 1 
      THEN
         pon_error_cnt := 2;
         l_record_cnt := 2;
      END IF;
      xxetn_debug_pkg.add_debug ( '-   PROCEDURE : validate_customer_info -' );
      
   EXCEPTION
      WHEN OTHERS
      THEN
         g_retcode := 2;
         pon_error_cnt := 2;
         g_errbuff := 'Failed while validating customer information';
         print_log_message ( 'In Exception validate_customer_info check'||SQLERRM);
   END validate_customer_info;  
     
   
   --
   -- ========================
   -- Procedure: validate_receipt
   -- =============================================================================
   --   This procedure validate_receipt
   -- =============================================================================
   --  Input Parameters :
   --    None
   --  Output Parameters :
   -- -----------------------------------------------------------------------------
   --
   PROCEDURE validate_receipt 
   IS
      l_error_flag            VARCHAR2 ( 1 );
      l_receipt_method        ar_receipt_methods.NAME%TYPE;
      l_method_id             ar_receipt_methods.receipt_method_id%TYPE;
      l_cust_id               hz_cust_accounts_all.cust_account_id%TYPE;
      l_sob_id                hr_operating_units.set_of_books_id%TYPE;   
      l_bill_to_id            hz_cust_site_uses_all.site_use_id%TYPE;
      l_operating_unit        VARCHAR2 ( 240 );
      l_org_id                hr_operating_units.organization_id%TYPE;    
      l_error_cnt             NUMBER;
      l_gl_error              VARCHAR2 ( 1 );
      l_err_msg               VARCHAR2 ( 2000 );
      l_ret_status            VARCHAR2 ( 50 );
      l_log_ret_status        VARCHAR2 ( 50 );
      l_log_err_msg           VARCHAR2 ( 2000 );
      l_err_code              VARCHAR2 ( 40 )    :=   NULL;
      
      CURSOR validate_receipt_cur
      IS
      SELECT * 
        FROM xxar_cashonaccnt_stg xcs
       WHERE xcs.process_flag = g_new
         AND xcs.batch_id = g_new_batch_id;
         
   BEGIN
      
      xxetn_debug_pkg.add_debug ( '+   PROCEDURE : validate_receipt for batch id = '||g_new_batch_id ||' + ');
      
      l_error_cnt        := 0;
      l_gl_error         := NULL;
      l_cust_id          := NULL;
      l_bill_to_id       := NULL;
      g_intf_staging_id  := NULL;
      g_src_keyname1     := NULL;
      g_src_keyvalue1    := NULL;
      g_src_keyname2     := NULL;
      g_src_keyvalue2    := NULL;
      g_src_keyname3     := NULL;
      g_src_keyvalue3    := NULL;
      g_src_keyname4     := NULL;
      g_src_keyvalue4    := NULL;
      g_src_keyname5     := NULL;
      g_src_keyvalue5    := NULL;

      BEGIN
      
      
      FOR validate_receipt_rec IN validate_receipt_cur
      LOOP
        --check for all the custom validations
         xxetn_debug_pkg.add_debug ( 'Validate Record, Record ID ='||validate_receipt_rec.interface_txn_id);
         g_intf_staging_id  :=   validate_receipt_rec.INTERFACE_TXN_ID;
         
         --setting variables for error framework
         l_error_flag     := NULL;
         l_error_cnt      := 0;
         l_operating_unit := NULL;
         l_org_id         := NULL;
         l_receipt_method := NULL;
         l_method_id      := NULL;
         
          --procedure to check mandatory values are not missing
         mandatory_value_check (
           validate_receipt_rec.leg_receipt_method
         , validate_receipt_rec.leg_receipt_number
         , validate_receipt_rec.leg_receipt_amount
         , validate_receipt_rec.leg_currency_code
         , validate_receipt_rec.leg_receipt_date
         , validate_receipt_rec.leg_operating_unit
         , validate_receipt_rec.leg_apply_to
         , validate_receipt_rec.leg_amount_applied
         , validate_receipt_rec.leg_onaccount_balances
         , validate_receipt_rec.leg_unapplied_balances
         , l_error_cnt
         );
         
         IF l_error_cnt > 0 
         THEN
            l_error_flag := g_yes;
         END IF;
         
         l_error_cnt := 0;
         
          --Check whether receipt method exists in the system and fetch method_id
         xxetn_debug_pkg.add_debug ( 'Receipt Method. Input = '||validate_receipt_rec.leg_receipt_method); 
         validate_receipt_method( validate_receipt_rec.leg_receipt_method
                                , validate_receipt_rec.leg_receipt_date
                                , l_receipt_method
                                , l_method_id
                                , l_error_cnt);

         IF l_error_cnt > 0 THEN
            l_error_flag := g_yes;
            l_err_code   := 'ETN_AR_INVALID_RECEIPT_METHOD';
            l_err_msg    := 'Error: Receipt Method is not Valid';
            
            
            log_errors ( pov_return_status          =>   l_log_ret_status          -- OUT
                       , pov_error_msg              =>   l_log_err_msg             -- OUT
                       , piv_source_column_name     =>   'LEG_RECEIPT_METHOD'
                       , piv_source_column_value    =>   validate_receipt_rec.leg_receipt_method
                       , piv_error_type             =>   g_err_val
                       , piv_error_code             =>   l_err_code
                       , piv_error_message          =>   l_err_msg
                       );
            
         END IF;

         l_error_cnt := 0;
       
         xxetn_debug_pkg.add_debug ( 'l_error_cnt 2a:'||l_error_cnt);
         xxetn_debug_pkg.add_debug ( 'l_error_flag 2a'||l_error_flag);
        
        --Check whether operating unit is valid and fetch org_id and sob_id
         validate_operating_unit (validate_receipt_rec.leg_operating_unit
                                , l_operating_unit
                                , l_org_id
                                , l_sob_id
                                , l_error_cnt);
         
         IF l_error_cnt > 0 
         THEN
            l_error_flag := g_yes;
          
            l_err_code        := 'ETN_AR_INVALID_OPERATING_UNIT';
            l_err_msg         := 'Error: Operating Unit is not Valid';

            log_errors ( pov_return_status           =>   l_log_ret_status          -- OUT
                        , pov_error_msg              =>   l_log_err_msg             -- OUT
                        , piv_source_column_name     =>   'LEG_OPERATING_UNIT'
                        , piv_source_column_value    =>   validate_receipt_rec.leg_operating_unit
                        , piv_error_type             =>   g_err_val
                        , piv_error_code             =>   l_err_code
                        , piv_error_message          =>   l_err_msg
                        );
         
         END IF; 
          
         l_error_cnt := 0;
       
          --GL Date should not be less than receipt date check
         IF g_gl_date < validate_receipt_rec.leg_receipt_date
         THEN
            l_error_flag := g_yes;
           
            xxetn_debug_pkg.add_debug ( 'GL Date is less than receipt date. Input = '||g_gl_date);
            l_err_code        := 'ETN_AR_INVALID_GL_DATE';
            l_err_msg         := 'Error:GL Date is less than receipt date.';
         
            log_errors ( pov_return_status          =>   l_log_ret_status          -- OUT
                       , pov_error_msg              =>   l_log_err_msg             -- OUT
                       , piv_source_column_name     =>   'GL Date'
                       , piv_source_column_value    =>   g_gl_date
                       , piv_error_type             =>   g_err_val
                       , piv_error_code             =>   l_err_code
                       , piv_error_message          =>   l_err_msg
                       );
         
         END IF;  
         
         
         IF l_org_id IS NOT NULL THEN 
          
            l_error_cnt := 0;
           
           --validate customer and fetch customer_id and bill_to_id
           validate_customer_info (  validate_receipt_rec.leg_customer_number
                                   , validate_receipt_rec.leg_bill_to_location
                                   , l_org_id         
                                   , l_cust_id        
                                   , l_bill_to_id     
                                   , l_error_cnt
                                     );
            
            IF l_error_cnt > 0 THEN
               
               l_error_flag := g_yes;
               l_err_code        := 'ETN_AR_INVALID_CUSTOMER_INFO';
               l_err_msg         := 'Error: Customer Details are not Valid.';
         
               log_errors ( pov_return_status          =>   l_log_ret_status          -- OUT
                          , pov_error_msg              =>   l_log_err_msg             -- OUT
                          , piv_source_column_name     =>   'leg_customer_number'||'~'||'leg_bill_to_location'
                          , piv_source_column_value    =>   validate_receipt_rec.leg_customer_number||'~'||validate_receipt_rec.leg_bill_to_location
                          , piv_error_type             =>   g_err_val
                          , piv_error_code             =>   l_err_code
                          , piv_error_message          =>   l_err_msg
                          );
                          
            END IF; 
         
            --Validate GL Period
            VALIDATE_GL_PERIOD (l_sob_id
                              , l_error_cnt);
            
            IF l_error_cnt > 0 
            THEN
               l_error_flag := g_yes;
               IF l_error_cnt = 2 THEN
                  xxetn_debug_pkg.add_debug ( 'GL Period is close. Input = '||g_gl_date);
                  l_err_code        := 'ETN_AR_INVALID_GL_PERIOD';
                  l_err_msg         := 'Error:GL Period is not open.';
               ELSIF l_error_cnt = 3 THEN
                  xxetn_debug_pkg.add_debug ( 'AR Period is close. Input = '||g_gl_date);
                  l_err_code        := 'ETN_AR_INVALID_AR_PERIOD';
                  l_err_msg         := 'Error:AR Period is not open.';
               END IF;
         
               log_errors ( pov_return_status         =>   l_log_ret_status          -- OUT
                          , pov_error_msg              =>   l_log_err_msg             -- OUT
                          , piv_source_column_name     =>   'GL_date'
                          , piv_source_column_value    =>   g_gl_date
                          , piv_error_type             =>   g_err_val
                          , piv_error_code             =>   l_err_code
                          , piv_error_message          =>   l_err_msg
                          );
            END IF;           
         END IF;
          
          
         --Validate currency code
         validate_currency_code (validate_receipt_rec.leg_currency_code
                                , l_error_cnt);
         IF l_error_cnt > 0 
         THEN
            l_error_flag := g_yes;
            l_err_code        := 'ETN_AR_INVALID_CURRENCY_CODE';
            l_err_msg         := 'Error: Currency Code is not Valid';
         
          
            log_errors (   pov_return_status          =>   l_log_ret_status          -- OUT
                         , pov_error_msg              =>   l_log_err_msg             -- OUT
                         , piv_source_column_name     =>   'LEG_CURRENCY_CODE'
                         , piv_source_column_value    =>   validate_receipt_rec.leg_currency_code
                         , piv_error_type             =>   g_err_val
                         , piv_error_code             =>   l_err_code
                         , piv_error_message          =>   l_err_msg
                         );
         END IF; 
         l_error_cnt := 0;

         invalid_check (validate_receipt_rec.LEG_RECEIPT_NUMBER
                     , validate_receipt_rec.LEG_RECEIPT_METHOD
                       , l_error_cnt);
        
         IF l_error_cnt > 0 
         THEN
            l_error_flag := g_yes;
            IF(l_error_cnt = 2)
            THEN
               l_err_code        := 'ETN_AR_INVALID_RECEIPT';
               l_err_msg         := 'Error: Receipt is not Valid.Either no or more than one value of receipt amount, onaccount balance, unapplied balance Exist';
            ELSIF(l_error_cnt = 3)
            THEN
               l_err_code        := 'ETN_AR_INVALID_RECEIPT';
               l_err_msg         := 'Error: Receipt is not Valid. Receipt Amount does not equal the sum of total amount applied and unapplied amount';
            END IF;
        
            log_errors ( pov_return_status          =>   l_log_ret_status          -- OUT
                       , pov_error_msg              =>   l_log_err_msg             -- OUT
                       , piv_source_column_name     =>   NULL
                       , piv_source_column_value    =>   NULL
                       , piv_error_type             =>   g_err_val
                       , piv_error_code             =>   l_err_code
                       , piv_error_message          =>   l_err_msg
                       );
         END IF; 
        
         --If Conversion type is 'User' ,ten exchange rate and exchange_Rate_date should not be NULL
         IF validate_receipt_rec.conversion_type = 'USER' THEN
            IF validate_receipt_rec.leg_exchange_rate IS NULL 
            OR validate_receipt_rec.leg_exchange_rate_date IS NULL 
            THEN
               l_error_flag := g_yes;
               xxetn_debug_pkg.add_debug ( ' Exchange Date or Rate IS NULL');
               l_err_code        := 'ETN_AR_INVALID_EXCHANGE_RATE';
               l_err_msg         := 'Error: Exchange Date or Rate IS NULL';
               log_errors ( pov_return_status          =>   l_log_ret_status          -- OUT
                          , pov_error_msg              =>   l_log_err_msg             -- OUT
                          , piv_source_column_name     =>   'LEG_EXCHANGE_RATE_DATE/RATE'
                          , piv_source_column_value    =>   validate_receipt_rec.leg_exchange_rate_date||validate_receipt_rec.leg_exchange_rate
                          , piv_error_type             =>   g_err_val
                          , piv_error_code             =>   l_err_code
                          , piv_error_message          =>   l_err_msg
                          );
             
            END IF;             
         END IF;

         --Receipt Amount should not be less than or equal to zero         
         IF validate_receipt_rec.leg_receipt_amount <= 0 
         THEN     
            l_error_flag := g_yes; 
            xxetn_debug_pkg.add_debug ( 'Receipt Amount is less than or equal to zero'); 
            l_err_code        := 'ETN_AR_INVALID_AMOUNT';
            l_err_msg         := 'Error: Invalid Amount';
   
            log_errors ( pov_return_status          =>   l_log_ret_status          -- OUT
                       , pov_error_msg              =>   l_log_err_msg             -- OUT
                       , piv_source_column_name     =>   'LEG_RECEIPT_AMOUNT'
                       , piv_source_column_value    =>   validate_receipt_rec.leg_receipt_amount
                       , piv_error_type             =>   g_err_val
                       , piv_error_code             =>   l_err_code
                       , piv_error_message          =>   l_err_msg
                       );
        
         END IF;
        
         xxetn_debug_pkg.add_debug ( 'l_error_flag:'||l_error_flag);
         
         
         --Update staging table with the validation status as 'V' or 'E'     
         UPDATE xxar_cashonaccnt_stg
         SET     gl_date                = g_gl_date
               , receipt_method         = l_receipt_method
               , receipt_method_id      = l_method_id
               , ou_name                = l_operating_unit
               , org_id                 = l_org_id
               , maturity_date          = g_gl_date
               , receipt_currency       = validate_receipt_rec.leg_currency_code
               , customer_site_use_id   = l_bill_to_id
               , leg_functional_amount  = validate_receipt_rec.leg_receipt_amount * validate_receipt_rec.leg_exchange_rate
               , process_flag           = DECODE ( l_error_flag,g_yes,g_error,g_validated)
               , error_type             = DECODE ( l_error_flag,g_yes,g_err_val,NULL)
               , pay_from_customer      = l_cust_id
               , last_updated_date      = SYSDATE
               , last_updated_by        = g_user_id
               , last_update_login      = g_login_id
               , program_application_id = g_prog_appl_id
               , program_id             = g_conc_program_id
               , program_update_date    = SYSDATE
               , request_id             = g_request_id
         WHERE  interface_txn_id      = validate_receipt_rec.interface_txn_id;
         
         IF ( l_error_flag = g_yes)
         THEN
           g_retcode := 1;
         END IF;
      
         g_intf_staging_id  := NULL;
         g_src_keyname1     := NULL;
         g_src_keyvalue1    := NULL;
         g_src_keyname2     := NULL;
         g_src_keyvalue2    := NULL;
         g_src_keyname3     := NULL;
         g_src_keyvalue3    := NULL;
      
         COMMIT; 
      END LOOP;
      END;  
      xxetn_debug_pkg.add_debug ( '-   PROCEDURE : validate_receipt for batch id = '||g_new_batch_id ||' - ');
   EXCEPTION
     WHEN OTHERS
     THEN
         g_retcode := 2;
         g_errbuff := 'Failed while vaildating receipt' ;
         print_log_message ( 'In Validate receipt when others'||SQLERRM );
   END validate_receipt;   
     
   --
   -- ========================
   -- Procedure: create_receipt
   -- =============================================================================
   --   This procedure create_receipt
   -- =============================================================================
   --  Input Parameters :
   --    None
   --  Output Parameters :
   --   pov_return_status -- return status in case of error or success
   --   pov_error_code -- 'E' or 'S'
   --   pov_error_message -- in case of error, its description
   -- -----------------------------------------------------------------------------
   --
   PROCEDURE create_receipt (  pov_return_status      OUT   VARCHAR2
                             , pov_error_code         OUT   VARCHAR2
                             , pov_error_message      OUT   VARCHAR2
                             )
   IS
    
   l_status_flag                  VARCHAR2 (1);
   l_error_message                VARCHAR2 (500);
   l_cr_id_out                    NUMBER;
   l_return_status_out            VARCHAR2(1);
   l_msg_count_out                NUMBER;
   l_msg_data_out                 VARCHAR2(1000);
   l_msg_index_out                NUMBER;
   l_method_id                    NUMBER;
   p_ussgl_transaction_code       ar_receivable_applications.ussgl_transaction_code%TYPE;
   p_attribute_rec                ar_receipt_api_pub.attribute_rec_type;     
   p_global_attribute_rec         ar_receipt_api_pub.global_attribute_rec_type; 
   p_comments                     ar_receivable_applications.comments%TYPE;
   p_application_ref_num          ar_receivable_applications.application_ref_num%TYPE;
   p_secondary_application_ref_id ar_receivable_applications.secondary_application_ref_id%TYPE;
   p_customer_reference           ar_receivable_applications.customer_reference%TYPE; 
   p_called_from                  VARCHAR2(200);
   p_customer_reason              ar_receivable_applications.customer_reason%TYPE;
   p_secondary_app_ref_type       ar_receivable_applications.secondary_application_ref_type%TYPE;
   p_secondary_app_ref_num        ar_receivable_applications.secondary_application_ref_num%TYPE;
   l_err_msg                      VARCHAR2 (2000);
   l_ret_status                   VARCHAR2 ( 50 );
   l_log_ret_status               VARCHAR2 ( 50 );
   l_log_err_msg                  VARCHAR2 ( 2000 );
   
   CURSOR create_receipt_cur
   IS
      SELECT  xcs.interface_txn_id,xcs.leg_receipt_amount ,xcs.leg_customer_name,xcs.leg_customer_bank_name,xcs.leg_customer_bank_account
             ,xcs.leg_reference,xcs.leg_postmark_date,xcs.leg_comments,xcs.leg_notes_rec_issuer_name
             ,xcs.leg_notes_rec_issue_date,xcs.leg_notes_rec_issuer_bk_branch,xcs.leg_currency_code
             ,xcs.leg_customer_number,xcs.leg_exchange_rate_type,xcs.leg_exchange_rate,xcs.leg_exchange_rate_date
             ,xcs.leg_receipt_number,xcs.leg_maturity_date,xcs.org_id,xcs.leg_bill_to_location,xcs.leg_receipt_date
             ,xcs.gl_date ,xcs.leg_receipt_method , xcs.leg_onaccount_balances , xcs.receipt_method_id ,xcs.customer_site_use_id
             ,xcs.pay_from_customer
        FROM  xxar_cashonaccnt_stg xcs
       WHERE  process_flag = g_validated
         AND  batch_id = g_new_batch_id
    GROUP BY  xcs.interface_txn_id,xcs.leg_receipt_amount ,xcs.leg_customer_name,xcs.leg_customer_bank_name,xcs.leg_customer_bank_account
             ,xcs.leg_reference,xcs.leg_postmark_date,xcs.leg_comments,xcs.leg_notes_rec_issuer_name
             ,xcs.leg_notes_rec_issue_date,xcs.leg_notes_rec_issuer_bk_branch,xcs.leg_currency_code
             ,xcs.leg_customer_number,xcs.leg_exchange_rate_type,xcs.leg_exchange_rate,xcs.leg_exchange_rate_date
             ,xcs.leg_receipt_number,xcs.leg_maturity_date,xcs.org_id,xcs.leg_bill_to_location,xcs.leg_receipt_date
             ,xcs.gl_date ,xcs.leg_receipt_method , xcs.leg_onaccount_balances , xcs.receipt_method_id ,xcs.customer_site_use_id
             ,xcs.pay_from_customer;
    
   CURSOR apply_on_acct_cur( piv_receipt_number IN VARCHAR2
                            ,piv_receipt_method IN VARCHAR2
                            )
   IS
      SELECT  *
        FROM  xxar_cashonaccnt_stg xcs
       WHERE  xcs.process_flag = g_validated
         AND  xcs.batch_id = g_new_batch_id
         AND  xcs.leg_receipt_number = piv_receipt_number
         AND  xcs.leg_receipt_method = piv_receipt_method;
       
   BEGIN
   
      xxetn_debug_pkg.add_debug ( ' +  PROCEDURE : CREATE_RECEIPT batch id = '||g_new_batch_id || ' + ');         
      
      
      FOR rec_receipt IN create_receipt_cur
      LOOP
         BEGIN
            g_intf_staging_id  :=   rec_receipt.interface_txn_id;
            l_status_flag := NULL;
            l_error_message := NULL;
            l_cr_id_out := NULL;
         
            -- SAVEPOINT create_receipt_sp;
       
             --Call Create Cash API 
             ar_receipt_api_pub.create_cash(p_api_version               => 1.0,
                                            p_init_msg_list             => fnd_api.g_true,
                                            p_commit                    => fnd_api.g_false,
                                            p_validation_level          => fnd_api.g_valid_level_full,
                                            p_amount                    => rec_receipt.LEG_RECEIPT_AMOUNT,
                                            p_customer_id               => rec_receipt.pay_from_customer,
                                         --  p_customer_name             => rec_receipt.LEG_CUSTOMER_NAME,
                                            p_customer_bank_account_name=> rec_receipt.LEG_CUSTOMER_BANK_NAME,
                                            p_customer_bank_account_num => rec_receipt.LEG_CUSTOMER_BANK_ACCOUNT,
                                            p_customer_receipt_reference=> rec_receipt.LEG_REFERENCE,
                                            p_postmark_date             => rec_receipt.LEG_POSTMARK_DATE,
                                            p_comments                  => rec_receipt.LEG_COMMENTS,
                                            p_issuer_name               => rec_receipt.LEG_NOTES_REC_ISSUER_NAME,
                                            p_issue_date                => rec_receipt.LEG_NOTES_REC_ISSUE_DATE,
                                            p_issuer_bank_branch_id     => rec_receipt.LEG_NOTES_REC_ISSUER_BK_BRANCH,
                                            p_currency_code             => rec_receipt.LEG_CURRENCY_code,
                                           -- p_customer_number           => rec_receipt.LEG_CUSTOMER_NUMBER,
                                            p_exchange_rate_type        => rec_receipt.LEG_EXCHANGE_RATE_TYPE,
                                            p_exchange_rate             => rec_receipt.LEG_EXCHANGE_RATE,
                                            p_exchange_rate_date        => rec_receipt.LEG_EXCHANGE_RATE_DATE,
                                            p_receipt_number            => rec_receipt.LEG_RECEIPT_NUMBER,
                                            p_maturity_date             => rec_receipt.GL_DATE,
                                            p_receipt_method_id         => rec_receipt.RECEIPT_METHOD_ID,
                                            p_org_id                    => rec_receipt.ORG_ID,
                                            p_customer_site_use_id      => rec_receipt.CUSTOMER_SITE_USE_ID,
                                            p_receipt_date              => rec_receipt.LEG_RECEIPT_DATE,
                                            p_gl_date                   => rec_receipt.GL_DATE,
                                            p_cr_id                     => l_cr_id_out,
                                            x_return_status             => l_return_status_out,
                                            x_msg_count                 => l_msg_count_out,
                                            x_msg_data                  => l_msg_data_out
                                           );
            l_error_message := l_error_message ||' Create Cash API Status '||l_return_status_out || ' cr id '||l_cr_id_out;
            --Check API status is not successful
            IF l_return_status_out <> fnd_api.g_ret_sts_success
            THEN
              l_status_flag := g_error;
               --Extract error message from API
               IF l_msg_count_out > 1
               THEN
   
                  FOR i IN 1 .. l_msg_count_out
                  LOOP
                     fnd_msg_pub.get (p_msg_index          => i, p_encoded => fnd_api.g_false, p_data => l_msg_data_out,
                                      p_msg_index_out      => l_msg_index_out);
                     l_err_msg := l_error_message || ' Create Cash Return Status '||l_return_status_out || ' | ' || l_msg_data_out || REPLACE (l_msg_index_out, CHR (0), NULL);
                  END LOOP;
               ELSIF l_msg_count_out = 1
               THEN
                  l_err_msg := REPLACE (l_error_message || ' Create Cash Return Status '||l_return_status_out || ' | ' || l_msg_data_out, CHR (0), NULL);
               END IF;
            ELSE
              l_status_flag := g_success;
            END IF;
            xxetn_debug_pkg.add_debug ('message:'||l_error_message||l_err_msg);
            xxetn_debug_pkg.add_debug ('ret_status:'||l_return_status_out);
            xxetn_debug_pkg.add_debug ('cr id:'||l_cr_id_out);
          
            pov_error_message := l_error_message;
            pov_return_status := l_return_status_out;
             
            --If the on account balance for the receipt is not equal to zero and the receipt is there in the system 
            IF ( (l_status_flag IN( g_error,g_success)) AND rec_receipt.LEG_ONACCOUNT_BALANCES > 0)
            THEN
               FOR  rec_apply_on_acct IN apply_on_acct_cur(rec_receipt.LEG_RECEIPT_NUMBER,rec_receipt.LEG_RECEIPT_METHOD)
               LOOP
                  BEGIN
                     g_intf_staging_id  :=   rec_apply_on_acct.interface_txn_id;
                     IF(l_status_flag = g_success)
                     THEN
                        --Call API for on-account application of the receipt
                        AR_RECEIPT_API_PUB.Apply_on_account(
                           p_api_version                   => 1.0,
                           p_init_msg_list                 => FND_API.G_FALSE,
                           p_commit                        => FND_API.G_FALSE,
                           p_validation_level              => FND_API.G_VALID_LEVEL_FULL,
                           x_return_status                 => l_return_status_out,
                           x_msg_count                     => l_msg_count_out,
                           x_msg_data                      => l_msg_data_out,
                           p_cash_receipt_id               => l_cr_id_out,
                           p_receipt_number                => rec_apply_on_acct.LEG_RECEIPT_NUMBER,
                           p_amount_applied                => rec_apply_on_acct.LEG_AMOUNT_APPLIED,
                           p_apply_date                    => rec_apply_on_acct.LEG_RECEIPT_DATE,
                           p_apply_gl_date                 => rec_apply_on_acct.GL_DATE, 
                           p_ussgl_transaction_code        => p_ussgl_transaction_code,
                           p_attribute_rec                 => p_attribute_rec,
                           p_global_attribute_rec          => p_global_attribute_rec,
                           p_comments                      => p_comments,
                           p_application_ref_num           => p_application_ref_num,
                           p_secondary_application_ref_id  => p_secondary_application_ref_id,
                           p_customer_reference            => p_customer_reference,
                           p_called_from                   => p_called_from,
                           p_customer_reason               => p_customer_reason,
                           p_secondary_app_ref_type        => p_secondary_app_ref_type,                
                           p_secondary_app_ref_num         => p_secondary_app_ref_num               
                          );
                  
                           -- If on -account application is successful
                           IF (l_return_status_out = g_success) 
                           THEN            
                              l_status_flag := g_success;
                              COMMIT;                
                              print_log_message('SUCCESS');
                              print_log_message('Return Status     = '|| SUBSTR (l_return_status_out,1,255));
                              print_log_message('Message Count     = '||l_msg_count_out);
                              print_log_message('Message Data      = '||l_msg_data_out);
                                   
                           ELSE  
                              l_status_flag := g_error;
                              print_log_message('Return Status   = '|| SUBSTR (l_return_status_out,1,255));
                              print_log_message('Message Count   = '|| TO_CHAR(l_msg_count_out ));
                              print_log_message('Message Data    = '|| SUBSTR (l_msg_data_out,1,255));
                              print_log_message(APPS.FND_MSG_PUB.Get ( p_msg_index    => APPS.FND_MSG_PUB.G_LAST
                                                                      ,p_encoded      => APPS.FND_API.G_FALSE));                        
                              ROLLBACK;
                              --Extract API error message
                              IF l_msg_count_out >=0 
                              THEN                
                                 FOR I IN 1..10 
                                 LOOP
                                    print_log_message(I||'. '|| SUBSTR (FND_MSG_PUB.Get(p_encoded => FND_API.G_FALSE ), 1, 255));
                                    l_err_msg:= (I||'. '|| SUBSTR (FND_MSG_PUB.Get(p_encoded => FND_API.G_FALSE ), 1, 255));                   
                                 END LOOP;
                              END IF;                
                           END IF;
                     END IF;
                     
                     --update process_flag to 'E' in case of API error
                     IF( l_status_flag = g_error)
                     THEN
                        UPDATE xxar_cashonaccnt_stg
                        SET     process_flag           = g_error
                              , run_sequence_id        = g_run_seq_id 
                              , error_type             = g_err_imp
                              , last_updated_date      = SYSDATE
                              , last_updated_by        = g_user_id
                              , last_update_login      = g_login_id
                              , program_application_id = g_prog_appl_id
                              , program_id             = g_conc_program_id
                              , program_update_date    = SYSDATE
                              , request_id             = g_request_id
                        WHERE  interface_txn_id = rec_apply_on_acct.interface_txn_id;   
                  
                        log_errors 
                          ( pov_return_status           =>   l_log_ret_status          -- OUT
                            , pov_error_msg             =>   l_log_err_msg             -- OUT
                            , piv_source_column_name    =>   NULL
                            , piv_source_column_value   =>   NULL
                            , piv_error_type            =>   g_err_imp
                            , piv_error_code            =>   'ETN_AR_CRT_RCPT_API_ERROR'
                            , piv_error_message         =>   l_err_msg
                            );         
                        
                          g_retcode := 1;            
                     ELSE
                        --Update process_flag to 'C' in case of API Success
                        UPDATE xxar_cashonaccnt_stg
                        SET     receipt_id             = l_cr_id_out
                              , process_flag           = g_converted
                              , run_sequence_id        = g_run_seq_id
                              , last_updated_date      = SYSDATE
                              , last_updated_by        = g_user_id
                              , last_update_login      = g_login_id
                              , program_application_id = g_prog_appl_id
                              , program_id             = g_conc_program_id
                              , program_update_date    = SYSDATE
                              , request_id             = g_request_id
                        WHERE  interface_txn_id = rec_apply_on_acct.interface_txn_id;
                     END IF;  
                     COMMIT;
                  EXCEPTION
                     WHEN OTHERS
                     THEN
                        g_retcode := 2;
                        xxetn_debug_pkg.add_debug ( 'In on account application when others'||SUBSTR( SQLERRM,1,250 ) );
                        l_err_msg  := SUBSTR( 'Exception In on account application create receipt '||SQLERRM  ,1,2000) ;
                        log_errors 
                             ( pov_return_status         =>   l_log_ret_status          -- OUT
                             , pov_error_msg             =>   l_log_err_msg             -- OUT
                             , piv_source_column_name    =>   NULL
                             , piv_source_column_value   =>   NULL
                             , piv_error_type            =>   g_err_imp
                             , piv_error_code            =>   'ETN_AR_CRT_RCPT_ON_ACCT_ERROR'
                             , piv_error_message         =>   l_err_msg
                             );
                        pov_error_code := 'ETN_AR_CRT_RCPT_ON_ACCT_ERROR';
                        pov_error_message := l_err_msg;
                  END;
               END LOOP;
            END IF;
            --hard update as per SR#3-8589253251
            BEGIN           
               UPDATE AR_RECEIVABLE_APPLICATIONS_ALL RA 
               SET    ON_ACCT_CUST_ID = rec_receipt.pay_from_customer, 
                      ON_ACCT_CUST_SITE_USE_ID = rec_receipt.CUSTOMER_SITE_USE_ID
               WHERE  RA.cash_receipt_id = l_cr_id_out 
               AND    RA.STATUS = 'ACC';
            
            EXCEPTION
               WHEN OTHERS
               THEN
                  print_log_message ( 'In create receipt update customer'||SQLERRM );
                  l_err_msg  := SUBSTR('Exception In create receipt update customer '||SQLERRM ,1,2000) ;
                  pov_error_code := 'ETN_AR_UPDATE_CUST_ERROR';
                  pov_error_message := l_err_msg;
            END;
         END;
      END LOOP;
      xxetn_debug_pkg.add_debug ( ' -  PROCEDURE : CREATE_RECEIPT batch id = '||g_new_batch_id || ' - ');   
   EXCEPTION
     WHEN OTHERS
     THEN
        g_retcode := 2;
        print_log_message ( 'In create receipt when others'||SQLERRM );
        l_err_msg  := SUBSTR('Exception In create receipt '||SQLERRM ,1,2000) ;
        pov_error_code := 'ETN_AR_CREATE_RECEIPT_ERROR';
        pov_error_message := l_err_msg;
     
   END create_receipt;   
   --
   -- ========================
   -- Procedure: pre_validate
   -- =============================================================================
   --   This procedure pre_validate
   -- =============================================================================
   --  Input Parameters :
   --    None
   --  Output Parameters :
   --    None
   -- -----------------------------------------------------------------------------
   --
   PROCEDURE pre_validate 
   IS
      
      l_lookup_exists           NUMBER;
      l_receipt_lkp    CONSTANT VARCHAR2(50) := 'ETN_AR_RECEIPT_METHOD';
      l_ou_lkp         CONSTANT VARCHAR2(50) := 'ETN_COMMON_OU_MAP';
   BEGIN
      
      l_lookup_exists := 0;
      
      xxetn_debug_pkg.add_debug ( '+   PROCEDURE : pre_validate +' );
      
      xxetn_debug_pkg.add_debug ( '+ Checking Customer Cross Reference +' );
      
      xxetn_debug_pkg.add_debug ( '- Checking Customer Cross Reference -' );
      
      xxetn_debug_pkg.add_debug ( '+ Checking Receipt Method Lookup +' );
      
      -- check whether the lookup ETN_AR_RECEIPT_METHOD exists
      
      l_lookup_exists := 0;
      
      BEGIN
         SELECT 1 
           INTO l_lookup_exists
           FROM fnd_lookup_types flv
          WHERE flv.lookup_type =  l_receipt_lkp;
      EXCEPTION
         WHEN NO_DATA_FOUND 
         THEN
            l_lookup_exists := 0;
            print_log_message ( ' In No Data found of Receipt Method lookup check'||SQLERRM);
         WHEN OTHERS 
         THEN
            l_lookup_exists := 0;
            print_log_message ( ' In when others of Receipt Method lookup check'||SQLERRM);
      END;
      
      IF l_lookup_exists = 0 
      THEN        
         g_retcode := 1;
         FND_FILE.PUT_LINE(FND_FILE.OUTPUT, 'RECEIPT METHOD LOOKUP IS NOT SETUP');        
      END IF;      
      FND_FILE.PUT_LINE(FND_FILE.OUTPUT, 'RECEIPT METHOD LOOKUP IS SETUP');  
      
      xxetn_debug_pkg.add_debug ( '+ Checking Operating Unit Lookup +' );         
      l_lookup_exists := 0;
      
      -- check whether the lookup ETN_COMMON_OU_MAP exists   
      BEGIN
         SELECT 1 
           INTO l_lookup_exists
           FROM fnd_lookup_types flv
          WHERE flv.lookup_type =  l_ou_lkp;
      EXCEPTION
         WHEN NO_DATA_FOUND 
         THEN
            l_lookup_exists := 0;
            print_log_message ( ' In No Data found of Common OU Lookup check'||SQLERRM);
         WHEN OTHERS 
         THEN
            l_lookup_exists := 0;
            print_log_message ( ' In when others of Common OU lookup check'||SQLERRM);
      END;
   
      IF l_lookup_exists = 0 
      THEN
         g_retcode := 1;
         FND_FILE.PUT_LINE(FND_FILE.OUTPUT, 'COMMON OU LOOKUP IS NOT SETUP');           
      END IF;
      
      xxetn_debug_pkg.add_debug ( '- Checking Operating Unit Lookup -' );           
      fnd_file.put_line(fnd_file.output, 'COMMON OU LOOKUP IS SETUP');
      xxetn_debug_pkg.add_debug ( '-   PROCEDURE : pre_validate -' );
      
   EXCEPTION
      WHEN OTHERS
      THEN
         g_retcode := 2;
         print_log_message ( 'In Pre Validate when others'||SQLERRM );
   
   END pre_validate;   
   
   --
   -- ========================
   -- Procedure: assign_batch_id
   -- =============================================================================
   --   This procedure assigns batch id
   -- =============================================================================
   --  Input Parameters :
   --    None
   --  Output Parameters :
   --    None
   -- -----------------------------------------------------------------------------
   --
   PROCEDURE assign_batch_id 
   IS
      
      l_err_msg             VARCHAR2 (2000);
      l_ret_status          VARCHAR2 ( 50 );
      l_log_ret_status      VARCHAR2 ( 50 );
      l_log_err_msg         VARCHAR2 ( 2000 );
   BEGIN
   
      xxetn_debug_pkg.add_debug ( '+   PROCEDURE : assign_batch_id - g_batch_id :'||g_batch_id);
      l_log_err_msg    := NULL;
      l_log_ret_status := NULL;
      l_err_msg        := NULL;
   
      -- g_batch_id NULL is considered a fresh run
      IF g_batch_id IS NULL 
      THEN
   
         UPDATE xxar_cashonaccnt_stg
         SET    batch_id               = g_new_batch_id
              , process_flag           = g_new
              , run_sequence_id        = g_run_seq_id
              , last_updated_date      = SYSDATE
              , last_updated_by        = g_user_id
              , last_update_login      = g_login_id
              , program_application_id = g_prog_appl_id
              , program_id             = g_conc_program_id
              , program_update_date    = SYSDATE
              , request_id             = g_request_id
         WHERE  batch_id IS NULL;
   
         xxetn_debug_pkg.add_debug ( 'Updated staging table where batch id is null'|| SQL%ROWCOUNT);
   
   
      ELSE
   
         --------------------------------------------
         -- All : All the records in batch other than obsolete
         --------------------------------------------
         -- Error : Records where validated flag are 'E'
         --------------------------------------------
         -- Unprocessed : Records which are assigned
         -- batch ID but are in new status
         --------------------------------------------
         IF g_process_records = 'ALL' 
         THEN
   
            UPDATE xxar_cashonaccnt_stg
            SET    process_flag           = g_new
                 , run_sequence_id        = g_run_seq_id
                 , last_updated_date      = SYSDATE
                 , last_updated_by        = g_user_id
                 , last_update_login      = g_login_id
                 , program_application_id = g_prog_appl_id
                 , program_id             = g_conc_program_id
                 , program_update_date    = SYSDATE
                 , request_id             = g_request_id
            WHERE  batch_id               = g_new_batch_id
            AND    process_flag NOT IN ( g_obsolete ,g_converted);
   
            xxetn_debug_pkg.add_debug ( 'Updated staging table where process record All'|| SQL%ROWCOUNT);
   
         ELSIF g_process_records = 'ERROR' 
         THEN
   
            UPDATE xxar_cashonaccnt_stg
            SET    process_flag           = g_new
                 , run_sequence_id        = g_run_seq_id
                 , last_updated_date      = SYSDATE
                 , last_updated_by        = g_user_id
                 , last_update_login      = g_login_id
                 , program_application_id = g_prog_appl_id
                 , program_id             = g_conc_program_id
                 , program_update_date    = SYSDATE
                 , request_id             = g_request_id
            WHERE  batch_id               = g_new_batch_id
            AND    process_flag  = g_error;
   
            xxetn_debug_pkg.add_debug ( 'Updated staging table where process record Error'|| SQL%ROWCOUNT);
        
         ELSIF g_process_records = 'UNPROCESSED' 
         THEN
   
            UPDATE xxar_cashonaccnt_stg
            SET    run_sequence_id        = g_run_seq_id
                 , last_updated_date      = SYSDATE
                 , last_updated_by        = g_user_id
                 , last_update_login      = g_login_id
                 , program_application_id = g_prog_appl_id
                 , program_id             = g_conc_program_id
                 , program_update_date    = SYSDATE
                 , request_id             = g_request_id
            WHERE  batch_id               = g_new_batch_id
            AND    process_flag  = g_new;
   
            xxetn_debug_pkg.add_debug ( 'Updated staging table where process record Error'|| SQL%ROWCOUNT);
   
         END IF;
         
      END IF;  -- g_batch_id
   
      COMMIT;
      
   EXCEPTION
      WHEN OTHERS
      THEN
         g_retcode := 2;
         l_err_msg := SUBSTR('Error While Assigning batch ID'||SQLERRM,1,2000);
         print_log_message ( 'In When Other of Assign Batch Id procedure'|| SQL%ROWCOUNT);

   END assign_batch_id;
   
   --
   -- ========================
   -- Procedure: main
   -- =============================================================================
   --   This is a main public procedure, which will be invoked through concurrent
   --   program.
   --
   --   This conversion program is used to validate cash receipt data
   --   from legacy system.0$ AR cash receipts are created and on account
   --    applications are done..
   --
   -- =============================================================================
   --
   -- -----------------------------------------------------------------------------
   --  Called By Concurrent Program: Eaton Cash On Account Conversion Program
   -- -----------------------------------------------------------------------------
   -- -----------------------------------------------------------------------------
   --
   --  Input Parameters :
   --    piv_run_mode        : Control the program execution for VALIDATE and CONVERSION
   --    piv_hidden          : Dummy variable
   --    pin_batch_id        : List all unique batches from staging table , this will
   --                        be NULL for first Conversion Run.
   --    piv_dummy          : Dummy variable
   --    piv_process_records : Conditionally available only when P_BATCH_ID is popul-
   --                        -ated. Otherwise this will be disabled and defaulted
   --                        to ALL
   --    piv_gl_date         : Input GL Date 
   --
   --  Output Parameters :
   --    p_errbuf          : Standard output parameter for concurrent program
   --    p_retcode         : Standard output parameter for concurrent program
   --
   --  Return     : Not applicable
   -- -----------------------------------------------------------------------------
   PROCEDURE main ( pov_errbuf            OUT   NOCOPY  VARCHAR2
                  , pon_retcode           OUT   NOCOPY  NUMBER
                  , piv_run_mode          IN            VARCHAR2    -- pre validate/validate/conversion/reconcile
                  , piv_hidden            IN            VARCHAR2    -- dummy variable
                  , pin_batch_id          IN            NUMBER      -- null / <batch_id>
                  , piv_dummy             IN            VARCHAR2    -- dummy variable
                  , piv_process_records   IN            VARCHAR2    -- (a) all / (e) error only / (n) unprocessed
                  , piv_gl_date           IN            VARCHAR2
                  )
   IS
   
      l_debug_on          BOOLEAN;
      l_retcode           VARCHAR2 ( 1 )     :=   'S';
      l_return_status     VARCHAR2 ( 200 )   :=   NULL;
      l_err_code          VARCHAR2 ( 40 )    :=   NULL;
      l_err_msg           VARCHAR2 ( 2000 )  :=   NULL;   
      l_log_ret_status    VARCHAR2 ( 50 )    :=   NULL;
      l_log_err_msg       VARCHAR2 ( 2000 );
   
   BEGIN
      --Initialize global variables for error framework
      
      g_intf_staging_id  :=  NULL;
      g_src_keyname1     :=  NULL;
      g_src_keyvalue1    :=  NULL;
      g_src_keyname2     :=  NULL;
      g_src_keyvalue2    :=  NULL;
      g_src_keyname3     :=  NULL;
      g_src_keyvalue3    :=  NULL;
      g_src_keyname4     :=  NULL;
      g_src_keyvalue4    :=  NULL;
      g_src_keyname5     :=  NULL;
      g_src_keyvalue5    :=  NULL;
      xxetn_debug_pkg.initialize_debug ( pov_err_msg => l_err_msg
                                        ,piv_program_name => 'ETN_AR_CASH_ONACCNT_CONV'
                                        ); 
   
      -- If error initializing debug messages
      IF l_err_msg IS NOT NULL
      THEN
         pon_retcode :=  2;  
         pov_errbuf  := l_err_msg;
         print_log_message ( 'Debug Initialization failed');
         RETURN;
      END IF;
      print_log_message ( 'Started Cash on Account Conversion at ' || TO_CHAR( g_sysdate , 'DD-MON-YYYY HH24:MI:SS' ) );
      
      print_log_message ( '+---------------------------------------------------------------------------+' );
      
      -- Check if debug logging is enabled/disabled
      
      g_run_mode        := piv_run_mode;
      g_batch_id        := pin_batch_id;
      g_process_records := piv_process_records;
      g_gl_date         := TO_DATE(piv_gl_date,'YYYY/MM/DD:HH24:MI:SS');
      
      -- Call Common Debug and Error Framework initialization
      print_log_message ( 'Program Parameters  : ' );
      print_log_message ( '---------------------------------------------' );
      print_log_message ( 'Run Mode            : ' || g_run_mode );
      print_log_message ( 'Batch ID            : ' || pin_batch_id );
      print_log_message ( 'Process records     : ' || g_process_records );
      print_log_message ( 'GL Date             : ' || g_gl_date );
      
      print_log_message ( 'SET Of books ID    : ' || g_set_of_books_id );
      
      IF piv_run_mode = 'LOAD-DATA'
      THEN
         xxetn_debug_pkg.add_debug ( 'In Load Data Mode');
         --call the procedure to load data from extraction tables into staging table
         get_data();
         print_stat_load();
      END IF;
   
      IF piv_run_mode = 'PRE-VALIDATE'
      THEN         
         xxetn_debug_pkg.add_debug ( 'In Pre-Validate Mode');
         --call the procedure to check if the custom setups are done
         pre_validate();
         pon_retcode := g_retcode;        
      END IF;
      
      IF piv_run_mode = 'VALIDATE'
      THEN
         IF(g_gl_date IS NULL)
         THEN
            xxetn_debug_pkg.add_debug ( 'GL Date is mandatory for VALIDATE mode ');
            pon_retcode := 2;
            RETURN;
         ELSE 
            IF g_batch_id IS NULL
            THEN
               g_new_batch_id := xxetn_batches_s.NEXTVAL;
               xxetn_debug_pkg.add_debug ( 'New Batch Id'||g_new_batch_id);
               g_run_seq_id     := xxetn_run_sequences_s.NEXTVAL;
               xxetn_debug_pkg.add_debug ( 'New Run Sequence ID : ' || g_run_seq_id );
            ELSE
               g_new_batch_id := g_batch_id; 
               g_run_seq_id     := xxetn_run_sequences_s.NEXTVAL;
               xxetn_debug_pkg.add_debug ( 'New Run Sequence ID : ' || g_run_seq_id );
            END IF;         
            assign_batch_id();--API for assigning batch id
            --if assign_batch_id fails, exit the program
            IF (g_retcode != 0)
            THEN
               xxetn_debug_pkg.add_debug ( 'Assign Batch ID failed.Program ended. ');
               RETURN;
            END IF;
       
            --Setting the run sequence id for error framework       
            xxetn_common_error_pkg.g_run_seq_id := g_run_seq_id;           
            xxetn_debug_pkg.add_debug ( 'In Validate Mode');   

            --call the procedure for validating receipt data            
            validate_receipt();
         END IF; 
         print_stat();    
      END IF;
      
      IF piv_run_mode = 'CONVERSION'
      THEN        
         xxetn_debug_pkg.add_debug ( 'In Conversion Mode');
         IF(pin_batch_id IS NULL)
         THEN
            xxetn_debug_pkg.add_debug ( 'Batch ID is mandatory for CONVERSION mode ');
            pon_retcode := 2;
            RETURN;
         ELSE 
            g_run_seq_id     := xxetn_run_sequences_s.NEXTVAL;
            xxetn_debug_pkg.add_debug ( 'New Run Sequence ID : ' || g_run_seq_id );         
            g_new_batch_id := pin_batch_id;
            --Setting the run sequence id for error framework 
            xxetn_common_error_pkg.g_run_seq_id := g_run_seq_id; 

           --Call the procedure to create receipt and do its on-account application            
           create_receipt ( l_log_ret_status, l_retcode, l_err_msg);
         END IF;
         print_stat();
      END IF;
      
      IF piv_run_mode = 'RECONCILE'
      THEN
         xxetn_debug_pkg.add_debug ( 'In Reconcile Mode');
         g_new_batch_id := pin_batch_id;
         
         --Call the procedure to print the statistics of the records processed by the conversion
         print_stat();
      END IF;
   
      print_log_message
             ( '+---------------------------------------------------------------------------+' );
      
      print_log_message (    'Cash on account Conversion Ends at: '
                                     || TO_CHAR( g_sysdate , 'DD-MON-YYYY HH24:MI:SS' ));
                     
      pon_retcode := g_retcode;
      pov_errbuf  := g_errbuff; 
   
      IF g_source_tab.COUNT > 0 
      THEN
         xxetn_debug_pkg.add_debug (    'Before Add Error in the end');
         xxetn_common_error_pkg.add_error
                  ( pov_return_status    =>   l_return_status  -- OUT
                  , pov_error_msg        =>   l_err_msg        -- OUT
                  , pi_source_tab        =>   g_source_Tab     -- IN  G_SOURCE_TAB_TYPE
                  , pin_batch_id         =>   g_new_batch_id
                  );  
      END IF;            
    
   EXCEPTION
      WHEN OTHERS
      THEN
      
         pov_errbuf  := 'Error : Main program procedure encounter error. ' || SUBSTR ( SQLERRM , 1 , 150 );
         pon_retcode := 2;
         print_log_message ( pov_errbuf );
   END main;

END xxar_cnv_cash_onaccnt_pkg;
/

SHOW ERRORS;

EXIT;
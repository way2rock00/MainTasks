CREATE OR REPLACE PACKAGE BODY xxpso_mdm_aq2mq_config_pkg
IS
--------------------------------------------------------------------------------
   /* $Header: xxpso_hz_cdh_cust_conv_pkg.pkb,v 1.0.0.0  2016/09/02 11:10:00  $ */
   -- Object ID            : MDM - AQ2MQ
   -- Owner                : Deloitte Implementation Team
   -- Project              : MDM
   -- Program Type         : PL/SQL Stored Procedure
   --
   -- Description of Package.  This package contains the AQ creation and MQ config
   --
   --    Modification History:
   --    ========= ===========   ==========	====================================================
   --    Date      Author         Version  		Comments
   --    ========= ===========   ==========	====================================================
   --    11-MAY-16 Satya Sai. M   1.0 		Initial Creation
   --    10-AUG-16 Akshay Nayak	  2.0		Modified for configuring inbound related configuration.
   --    21-SEP-16 Akshay Nayak	  3.0		Changes for Integration Changes.
   ---------------------------------------------------------------------------------

    gc_debug_flag	   VARCHAR2 (2);
   gv_yes_code		   VARCHAR2 (1) := 'Y';
   gv_yes		   VARCHAR2 (3) := 'Yes';
   gv_no_code		   VARCHAR2 (1) := 'N';
   gv_no		   VARCHAR2 (3) := 'No';   
   gv_log		  VARCHAR2 (10) := 'LOG';
   gv_output		  VARCHAR2 (10) := 'OUTPUT';   
	   
--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
-- Name       : XXPSO_PRINT_LOG_P
-- Description: This procedure is used to write message to log file.
--+============================================================|
--| Modification History
--+============================================================|
--| Date                 Who             Description
--| ----------- --------------------  -------------------------
--| 11-Aug-2016  Akshay Nayak           Initial Creation
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
   
  /*************************************************************************
      			Gateway Start Procedure
   *************************************************************************/
   PROCEDURE xxpso_mdm_gateway_start_p (p_debug_flag IN VARCHAR2)
   IS
   BEGIN
   	xxpso_print_debug('Start Gateway if it is not showing AGENT_STATUS is RUNNING:',gv_log);
        SYS.DBMS_MGWADM.STARTUP;
   EXCEPTION
      WHEN OTHERS
      THEN
         xxpso_print_debug('Error while starting gateway:'||SUBSTR (SQLERRM, 1, 500),gv_log);
   END xxpso_mdm_gateway_start_p;
   
  
  
  /*************************************************************************
      			Gateway Stop Procedure
   *************************************************************************/
   PROCEDURE xxpso_mdm_gateway_stop_p (p_debug_flag IN VARCHAR2)
   IS
   BEGIN
   	xxpso_print_debug('Stopping Gateway',gv_log);
	SYS.DBMS_MGWADM.SHUTDOWN;
   EXCEPTION
      WHEN OTHERS
      THEN
         xxpso_print_debug('Error while stoppin gateway:'||SUBSTR (SQLERRM, 1, 500),gv_log);
   END xxpso_mdm_gateway_stop_p;   

   /***********************************************************************
   		Creating queue table and queues.
   *************************************************************************/
   PROCEDURE xxpso_mdm_create_aq_p (p_debug_flag IN VARCHAR2)
   IS
   BEGIN
      xxpso_print_debug('Create Queue Table and Queue:: Start :',gv_log);


      xxpso_print_debug('Create CDH Outbound Queue Table:: Start :',gv_log);
      BEGIN
	      DBMS_AQADM.create_queue_table
				     (queue_table             => 'XXPSO.XXPSO_CDH_QUEUE_TBL_OUT',
				      multiple_consumers      => TRUE,
				      queue_payload_type      => 'SYS.AQ$_JMS_TEXT_MESSAGE'
				     );
      EXCEPTION
      WHEN OTHERS THEN
	xxpso_print_debug('Error while creating CDH Outbound Queue Table:'||SQLERRM,gv_log);   
      END;
      xxpso_print_debug('Create CDH Outbound Queue Table:: End :',gv_log);

	  
	  
	  
      xxpso_print_debug('Create CDH Outbound Queue :: Start :',gv_log);
      BEGIN
	      DBMS_AQADM.create_queue (queue_name       => 'XXPSO.XXPSO_CDH_QUEUE_OUT',
				       queue_table      => 'XXPSO.XXPSO_CDH_QUEUE_TBL_OUT',
				       max_retries      => 1000
				      );
      EXCEPTION
      WHEN OTHERS THEN
	xxpso_print_debug('Error while creating CDH Outbound Queue :'||SQLERRM,gv_log);    
      END;
      xxpso_print_debug('Create CDH Outbound Queue :: End :',gv_log);


      xxpso_print_debug('Start CDH Outbound Queue :: Start :',gv_log);      
      BEGIN
      	DBMS_AQADM.start_queue (queue_name => 'XXPSO.XXPSO_CDH_QUEUE_OUT');
      EXCEPTION
      WHEN OTHERS THEN
	xxpso_print_debug('Error while starting CDH Outbound Queue :'||SQLERRM,gv_log);   
      END;
      xxpso_print_debug('Start CDH Outbound Queue :: End :',gv_log);


      xxpso_print_debug('Create PDH Outbound Queue Table:: Start :',gv_log);      
      BEGIN
	      DBMS_AQADM.create_queue_table
				     (queue_table             => 'XXPSO.XXPSO_PDH_QUEUE_TBL_OUT',
				      multiple_consumers      => TRUE,
				      queue_payload_type      => 'SYS.AQ$_JMS_TEXT_MESSAGE'
				     );
      EXCEPTION
      WHEN OTHERS THEN
	xxpso_print_debug('Error while creating PDH Outbound Queue Table:'||SQLERRM,gv_log);   
      END;
      xxpso_print_debug('Create PDH Outbound Queue Table:: End :',gv_log);                           

      
      xxpso_print_debug('Create PDH Outbound Queue :: Start :',gv_log);
      BEGIN
	      DBMS_AQADM.create_queue (queue_name       => 'XXPSO.XXPSO_PDH_QUEUE_OUT',
				       queue_table      => 'XXPSO.XXPSO_PDH_QUEUE_TBL_OUT',
				       max_retries      => 1000
				      );
      EXCEPTION
      WHEN OTHERS THEN
	xxpso_print_debug('Error while creating PDH Outbound Queue :'||SQLERRM,gv_log);    
      END;
      xxpso_print_debug('Create PDH Outbound Queue :: End :',gv_log);                                

      
      xxpso_print_debug('Start PDH Outbound Queue :: Start :',gv_log);      
      BEGIN		
      	DBMS_AQADM.start_queue (queue_name => 'XXPSO.XXPSO_PDH_QUEUE_OUT');
      EXCEPTION
      WHEN OTHERS THEN
	xxpso_print_debug('Error while starting PDH Outbound Queue :'||SQLERRM,gv_log);   
      END;
      xxpso_print_debug('Start PDH Outbound Queue :: End :',gv_log);         

      
      --Changes for v2.0

      xxpso_print_debug('Create CDH Inbound Queue Table:: Start :',gv_log);
      BEGIN
	      DBMS_AQADM.create_queue_table
				     (queue_table             => 'XXPSO.XXPSO_CDH_QUEUE_TBL_IN',
				      multiple_consumers      => FALSE,
				      queue_payload_type      => 'SYS.AQ$_JMS_TEXT_MESSAGE'
				     );
      EXCEPTION
      WHEN OTHERS THEN
	xxpso_print_debug('Error while creating CDH Inbound Queue Table:'||SQLERRM,gv_log);   
      END;
      xxpso_print_debug('Create CDH Inbound Queue Table:: End :',gv_log);

      xxpso_print_debug('Create CDH Inbound Queue :: Start :',gv_log);
      BEGIN
	      DBMS_AQADM.create_queue (queue_name       => 'XXPSO.XXPSO_CDH_QUEUE_IN',
				       queue_table      => 'XXPSO.XXPSO_CDH_QUEUE_TBL_IN',
				       max_retries      => 1000
				      );
      EXCEPTION
      WHEN OTHERS THEN
	xxpso_print_debug('Error while creating CDH Inbound Queue :'||SQLERRM,gv_log);    
      END;
      xxpso_print_debug('Create CDH Inbound Queue :: End :',gv_log);                            

      
      xxpso_print_debug('Start CDH Inbound Queue :: Start :',gv_log);      
      BEGIN
      	DBMS_AQADM.start_queue (queue_name => 'XXPSO.XXPSO_CDH_QUEUE_IN');
      EXCEPTION
      WHEN OTHERS THEN
	xxpso_print_debug('Error while starting CDH Inbound Queue :'||SQLERRM,gv_log);   
      END;
      xxpso_print_debug('Start CDH Inbound Queue :: End :',gv_log);     

      
      xxpso_print_debug('Create PDH Inbound Queue Table:: Start :',gv_log);
      BEGIN
	      DBMS_AQADM.create_queue_table
				     (queue_table             => 'XXPSO.XXPSO_PDH_QUEUE_TBL_IN',
				      multiple_consumers      => FALSE,
				      queue_payload_type      => 'SYS.AQ$_JMS_TEXT_MESSAGE'
				     );
      EXCEPTION
      WHEN OTHERS THEN
	xxpso_print_debug('Error while creating PDH Inbound Queue Table:'||SQLERRM,gv_log);   
      END;
      xxpso_print_debug('Create PDH Inbound Queue Table:: End :',gv_log);                             


      xxpso_print_debug('Create PDH Inbound Queue :: Start :',gv_log);
      BEGIN
	      DBMS_AQADM.create_queue (queue_name       => 'XXPSO.XXPSO_PDH_QUEUE_IN',
				       queue_table      => 'XXPSO.XXPSO_PDH_QUEUE_TBL_IN',
				       max_retries      => 1000
				      );
      EXCEPTION
      WHEN OTHERS THEN
	xxpso_print_debug('Error while creating PDH Inbound Queue :'||SQLERRM,gv_log);    
      END;
      xxpso_print_debug('Create PDH Inbound Queue :: End :',gv_log);                              


      xxpso_print_debug('Start PDH Inbound Queue :: Start :',gv_log);      
      BEGIN		
      	DBMS_AQADM.start_queue (queue_name => 'XXPSO.XXPSO_PDH_QUEUE_IN');
      EXCEPTION
      WHEN OTHERS THEN
	xxpso_print_debug('Error while starting PDH Inbound Queue :'||SQLERRM,gv_log);   
      END;
      xxpso_print_debug('Start PDH Inbound Queue :: End :',gv_log);
      
      

      xxpso_print_debug('Create Queue Table and Queue:: End :',gv_log);
      
      --Changes for v2.0 End
      
      
      -- Changes for v3.0 Begin
      xxpso_print_debug('Create CDH Acknowledgement Outbound Queue Table:: Start :',gv_log);      
      BEGIN
	      DBMS_AQADM.create_queue_table
				     (queue_table             => 'XXPSO.XXPSO_CDH_ACK_Q_TBL_OUT',
				      multiple_consumers      => TRUE,
				      queue_payload_type      => 'SYS.AQ$_JMS_TEXT_MESSAGE'
				     );
      EXCEPTION
      WHEN OTHERS THEN
	xxpso_print_debug('Error while creating CDH Acknowledgement Outbound Queue Table:'||SQLERRM,gv_log);   
      END;
      xxpso_print_debug('Create CDH Acknowledgement Outbound Queue Table:: End :',gv_log);                           

      
      xxpso_print_debug('Create CDH Acknowledgement Outbound Queue :: Start :',gv_log);
      BEGIN
	      DBMS_AQADM.create_queue (queue_name       => 'XXPSO.XXPSO_CDH_ACK_QUEUE_OUT',
				       queue_table      => 'XXPSO.XXPSO_CDH_ACK_Q_TBL_OUT',
				       max_retries      => 1000
				      );
      EXCEPTION
      WHEN OTHERS THEN
	xxpso_print_debug('Error while creating CDH Acknowledgement Outbound Queue :'||SQLERRM,gv_log);    
      END;
      xxpso_print_debug('Create CDH Acknowledgement Outbound Queue :: End :',gv_log);                                

      
      xxpso_print_debug('Start CDH Acknowledgement Outbound Queue :: Start :',gv_log);      
      BEGIN		
      	DBMS_AQADM.start_queue (queue_name => 'XXPSO.XXPSO_CDH_ACK_QUEUE_OUT');
      EXCEPTION
      WHEN OTHERS THEN
	xxpso_print_debug('Error while starting CDH Acknowledgement Outbound Queue :'||SQLERRM,gv_log);   
      END;
      xxpso_print_debug('Start CDH Acknowledgement Outbound Queue :: End :',gv_log);  
      
      xxpso_print_debug('Create PDH Acknowledgement Outbound Queue Table:: Start :',gv_log);      
      BEGIN
	      DBMS_AQADM.create_queue_table
				     (queue_table             => 'XXPSO.XXPSO_PDH_ACK_Q_TBL_OUT',
				      multiple_consumers      => TRUE,
				      queue_payload_type      => 'SYS.AQ$_JMS_TEXT_MESSAGE'
				     );
      EXCEPTION
      WHEN OTHERS THEN
	xxpso_print_debug('Error while creating PDH Acknowledgement Outbound Queue Table:'||SQLERRM,gv_log);   
      END;
      xxpso_print_debug('Create PDH Acknowledgement Outbound Queue Table:: End :',gv_log);                           

      
      xxpso_print_debug('Create PDH Acknowledgement Outbound Queue :: Start :',gv_log);
      BEGIN
	      DBMS_AQADM.create_queue (queue_name       => 'XXPSO.XXPSO_PDH_ACK_QUEUE_OUT',
				       queue_table      => 'XXPSO.XXPSO_PDH_ACK_Q_TBL_OUT',
				       max_retries      => 1000
				      );
      EXCEPTION
      WHEN OTHERS THEN
	xxpso_print_debug('Error while creating PDH Acknowledgement Outbound Queue :'||SQLERRM,gv_log);    
      END;
      xxpso_print_debug('Create PDH Acknowledgement Outbound Queue :: End :',gv_log);                                

      
      xxpso_print_debug('Start PDH Acknowledgement Outbound Queue :: Start :',gv_log);      
      BEGIN		
      	DBMS_AQADM.start_queue (queue_name => 'XXPSO.XXPSO_PDH_ACK_QUEUE_OUT');
      EXCEPTION
      WHEN OTHERS THEN
	xxpso_print_debug('Error while starting PDH Acknowledgement Outbound Queue :'||SQLERRM,gv_log);   
      END;
      xxpso_print_debug('Start PDH Acknowledgement Outbound Queue :: End :',gv_log);       
      -- Changes for v3.0 End
   EXCEPTION
      WHEN OTHERS
      THEN
         fnd_file.put_line (fnd_file.LOG,
                               'Error in creating cdh and PDH AQ - '
                            || SUBSTR (SQLERRM, 1, 500)
                           );
   END xxpso_mdm_create_aq_p;
   

   /***********************************************************************
   				Create Links
   *************************************************************************/
   
   PROCEDURE xxpso_mdm_create_msglink2mq_p (p_debug_flag IN VARCHAR2)
   IS
      lv_options         SYS.mgw_properties;
      lv_prop            SYS.mgw_mqseries_properties;
      lv_channel         fnd_lookup_values.meaning%TYPE   := NULL;
      lv_hostname        fnd_lookup_values.meaning%TYPE   := NULL;
      lv_port            fnd_lookup_values.meaning%TYPE   := NULL;
      lv_queue_manager   fnd_lookup_values.meaning%TYPE   := NULL;
      lv_error_cnt       NUMBER                           := 0;
   BEGIN
      xxpso_print_debug('Create xxpso_mdm_create_msglink2mq_p:: Start :',gv_log);   


      BEGIN
         SELECT flv.meaning
           INTO lv_channel
           FROM fnd_lookup_values flv
          WHERE flv.lookup_type = 'XXPSO_MDM_AQ_CONFIG_LKP'
            AND flv.lookup_code = 'CHANNEL'
            AND flv.LANGUAGE = USERENV ('LANG')
            AND TRUNC (SYSDATE) BETWEEN TRUNC (flv.start_date_active)
                                    AND TRUNC (NVL (flv.end_date_active,
                                                    SYSDATE + 1
                                                   )
                                              )
            AND flv.enabled_flag = 'Y';
            
	xxpso_print_debug('Channel Name:'|| lv_channel,gv_log);            

      EXCEPTION
         WHEN NO_DATA_FOUND
         THEN
            lv_error_cnt := lv_error_cnt + 1;
	    xxpso_print_debug('Channel not configured in lookup',gv_log);
         WHEN OTHERS
         THEN
            lv_error_cnt := lv_error_cnt + 1;
            xxpso_print_debug('Channel not configured in lookup:'||SUBSTR (SQLERRM, 1, 500),gv_log);
      END;

      BEGIN
         SELECT flv.meaning
           INTO lv_hostname
           FROM fnd_lookup_values flv
          WHERE flv.lookup_type = 'XXPSO_MDM_AQ_CONFIG_LKP'
            AND flv.lookup_code = 'HOSTNAME'
            AND LANGUAGE = USERENV ('LANG')
            AND TRUNC (SYSDATE) BETWEEN TRUNC (flv.start_date_active)
                                    AND TRUNC (NVL (flv.end_date_active,
                                                    SYSDATE + 1
                                                   )
                                              )
            AND flv.enabled_flag = 'Y';

	    xxpso_print_debug('Host Name:'|| lv_hostname,gv_log);
      EXCEPTION
         WHEN NO_DATA_FOUND
         THEN
            lv_error_cnt := lv_error_cnt + 1;
            xxpso_print_debug('HOSTNAME not configured in lookup',gv_log);
         WHEN OTHERS
         THEN
            lv_error_cnt := lv_error_cnt + 1;
            xxpso_print_debug('HOSTNAME not configured in lookup:'||SUBSTR (SQLERRM, 1, 500),gv_log);
      END;

      BEGIN
         SELECT flv.meaning
           INTO lv_port
           FROM fnd_lookup_values flv
          WHERE flv.lookup_type = 'XXPSO_MDM_AQ_CONFIG_LKP'
            AND flv.lookup_code = 'PORT'
            AND flv.LANGUAGE = USERENV ('LANG')
            AND TRUNC (SYSDATE) BETWEEN TRUNC (flv.start_date_active)
                                    AND TRUNC (NVL (flv.end_date_active,
                                                    SYSDATE + 1
                                                   )
                                              )
            AND flv.enabled_flag = 'Y';

         xxpso_print_debug('PORT :'|| lv_port,gv_log);
      EXCEPTION
         WHEN NO_DATA_FOUND
         THEN
            lv_error_cnt := lv_error_cnt + 1;
            xxpso_print_debug('PORT not configured in lookup:',gv_log);
         WHEN OTHERS
         THEN
            lv_error_cnt := lv_error_cnt + 1;
            xxpso_print_debug('PORT not configured in lookup:'||SUBSTR (SQLERRM, 1, 500),gv_log);
      END;

      BEGIN
         SELECT flv.meaning
           INTO lv_queue_manager
           FROM fnd_lookup_values flv
          WHERE flv.lookup_type = 'XXPSO_MDM_AQ_CONFIG_LKP'
            AND flv.lookup_code = 'QUEUE_MANAGER'
            AND flv.LANGUAGE = USERENV ('LANG')
            AND TRUNC (SYSDATE) BETWEEN TRUNC (flv.start_date_active)
                                    AND TRUNC (NVL (flv.end_date_active,
                                                    SYSDATE + 1
                                                   )
                                              )
            AND flv.enabled_flag = 'Y';

	xxpso_print_debug('QUEUE_MANAGER :'|| lv_queue_manager,gv_log);
      EXCEPTION
         WHEN NO_DATA_FOUND
         THEN
            lv_error_cnt := lv_error_cnt + 1;
            xxpso_print_debug('QUEUE_MANAGER not configured in lookup:',gv_log);
         WHEN OTHERS
         THEN
            lv_error_cnt := lv_error_cnt + 1;
            xxpso_print_debug('QUEUE_MANAGER not configured in lookup:'||SUBSTR (SQLERRM, 1, 500),gv_log);
      END;

      lv_prop := SYS.mgw_mqseries_properties.construct ();
      lv_prop.max_connections := 1;
      lv_prop.interface_type := DBMS_MGWADM.jms_queue_connection;
      lv_prop.username := NULL;
      lv_prop.PASSWORD := NULL;
      lv_prop.hostname := lv_hostname;
      lv_prop.port := lv_port;
      lv_prop.channel := lv_channel;
      lv_prop.queue_manager := lv_queue_manager;
      lv_prop.outbound_log_queue := 'MDM.QUEUE.OUTBOUND.LOG';
      lv_prop.inbound_log_queue := 'MDM.QUEUE.INBOUND.LOG';

      xxpso_print_debug('lv_error_cnt:'||lv_error_cnt,gv_log);
      
      

      IF lv_error_cnt = 0
      THEN
	xxpso_print_debug('Create XXPSO_CDH_OUT_LINK::  Start: ',gv_log);
	BEGIN
		DBMS_MGWADM.create_msgsystem_link (linkname        => 'XXPSO_CDH_OUT_LINK',
					    properties      => lv_prop,
					    options         => lv_options
					   );
	EXCEPTION
	WHEN OTHERS THEN
	xxpso_print_debug('Error while creating XXPSO_CDH_OUT_LINK:'||SQLERRM,gv_log);   
	END;
	xxpso_print_debug('Create XXPSO_CDH_OUT_LINK::  End: ',gv_log);


	xxpso_print_debug('Create XXPSO_PDH_OUT_LINK::  Start: ',gv_log);

	BEGIN
		DBMS_MGWADM.create_msgsystem_link (linkname        => 'XXPSO_PDH_OUT_LINK',
					    properties      => lv_prop,
					    options         => lv_options
					   );
	EXCEPTION
	WHEN OTHERS THEN
	xxpso_print_debug('Error while creating XXPSO_PDH_OUT_LINK:'||SQLERRM,gv_log);   
	END;
	xxpso_print_debug('Create XXPSO_PDH_OUT_LINK::  End: ',gv_log);
	
	/*
	xxpso_print_debug('Create XXPSO_PDH_LINK::  Start: ',gv_log);
	BEGIN
		DBMS_MGWADM.create_msgsystem_link (linkname        => 'XXPSO_PDH_LINK',
					    properties      => lv_prop,
					    options         => lv_options
					   );
	EXCEPTION
	WHEN OTHERS THEN
	xxpso_print_debug('Error while creating XXPSO_PDH_LINK:'||SQLERRM,gv_log);   
	END;
	xxpso_print_debug('Create XXPSO_PDH_LINK::  End: ',gv_log);


	xxpso_print_debug('Create XXPSO_CDH_LINK::  Start: ',gv_log);

	BEGIN
		DBMS_MGWADM.create_msgsystem_link (linkname        => 'XXPSO_CDH_LINK',
					    properties      => lv_prop,
					    options         => lv_options
					   );
	EXCEPTION
	WHEN OTHERS THEN
	xxpso_print_debug('Error while creating XXPSO_CDH_LINK:'||SQLERRM,gv_log);   
	END;
	xxpso_print_debug('Create XXPSO_CDH_LINK::  End: ',gv_log);	
	*/
	
      END IF;
      
	xxpso_print_debug('Create xxpso_mdm_create_msglink2mq_p:: End :',gv_log);      
   EXCEPTION
      WHEN OTHERS
      THEN
         fnd_file.put_line (fnd_file.LOG,
                               'Error in creating Link for PDH and CDH - '
                            || SUBSTR (SQLERRM, 1, 500)
                           );
   END xxpso_mdm_create_msglink2mq_p;
   

   /***********************************************************************
   				Drop Links
   *************************************************************************/   
   
   PROCEDURE xxpso_mdm_drop_msglink2mq_p (p_debug_flag IN VARCHAR2)
   IS
   BEGIN
	xxpso_print_debug('Dropping xxpso_mdm_drop_msglink2mq_p:: Start :',gv_log);  
	
	xxpso_print_debug('Dropping XXPSO_PDH_OUT_LINK:: Start :',gv_log);
	BEGIN
		dbms_mgwadm.remove_msgsystem_link(linkname =>'XXPSO_PDH_OUT_LINK');
	EXCEPTION
	WHEN OTHERS THEN
	xxpso_print_debug('Error while Dropping XXPSO_PDH_OUT_LINK:'||SQLERRM,gv_log);   
	END;
	xxpso_print_debug('Dropping XXPSO_PDH_OUT_LINK::  End: ',gv_log);	

	
	xxpso_print_debug('Dropping XXPSO_CDH_OUT_LINK:: Start :',gv_log);  
	BEGIN
		dbms_mgwadm.remove_msgsystem_link(linkname =>'XXPSO_CDH_OUT_LINK');
	EXCEPTION
	WHEN OTHERS THEN
	xxpso_print_debug('Error while Dropping XXPSO_CDH_OUT_LINK:'||SQLERRM,gv_log);   
	END;
	xxpso_print_debug('Dropping XXPSO_CDH_OUT_LINK::  End: ',gv_log);	

	
	xxpso_print_debug('Dropping xxpso_mdm_drop_msglink2mq_p:: End :',gv_log);   	
   EXCEPTION
      WHEN OTHERS
      THEN
         fnd_file.put_line (fnd_file.LOG,
                               'Error in dropping Link for PDH and CDH - '
                            || SUBSTR (SQLERRM, 1, 500)
                           );
   END xxpso_mdm_drop_msglink2mq_p;   


   
   /***********************************************************************
   				Register Foreign Queues
   *************************************************************************/   
     
   PROCEDURE xxpso_mdm_mq_asforeignque_p (p_debug_flag IN VARCHAR2)
   IS
   BEGIN
	xxpso_print_debug('Create xxpso_mdm_mq_asforeignque_p:: Start :',gv_log);	

	xxpso_print_debug('Register Customer Outbound MQ as Foreign queue:: Start :',gv_log);      
	BEGIN
		DBMS_MGWADM.register_foreign_queue
			 (NAME                => 'XXPSO_CDH_MQ_OUT',
			  linkname            => 'XXPSO_CDH_OUT_LINK',
			  provider_queue      => 'MDM.DATAHUB.CUSTMGMT.OUTBOUND.1',
			  domain              => DBMS_MGWADM.domain_queue
			 );

	EXCEPTION
	WHEN OTHERS THEN
	xxpso_print_debug('Error while Registering Customer Outbound MQ as Foreign queue:'||SQLERRM,gv_log);   
	END;
	xxpso_print_debug('Register Customer Outbound MQ as Foreign queue:: End :',gv_log);

	xxpso_print_debug('Register Product Outbound MQ as Foreign queue:: Start :',gv_log);  
	BEGIN

		DBMS_MGWADM.register_foreign_queue
				 (NAME                => 'XXPSO_PDH_MQ_OUT',
				  linkname            => 'XXPSO_PDH_OUT_LINK',
				  provider_queue      => 'MDM.DATAHUB.PRODMGMT.OUTBOUND.1',
				  domain              => DBMS_MGWADM.domain_queue
				 );
	EXCEPTION
	WHEN OTHERS THEN
	xxpso_print_debug('Error while Registering Product Outbound MQ as Foreign queue:'||SQLERRM,gv_log);   
	END;
	xxpso_print_debug('Register Product Outbound MQ as Foreign queue:: End :',gv_log);
	
	
      --Changes for v2.0 Begin
	xxpso_print_debug('Register Customer Inbound MQ as Foreign queue:: Start :',gv_log);  
	BEGIN
		DBMS_MGWADM.register_foreign_queue
				 (NAME                => 'XXPSO_CDH_MQ_IN',
				  linkname            => 'XXPSO_CDH_OUT_LINK',
				  provider_queue      => 'MDM.DATAHUB.CUSTMGMT.INBOUND.1',
				  domain              => DBMS_MGWADM.domain_queue
				 );

	EXCEPTION
	WHEN OTHERS THEN
	xxpso_print_debug('Error while Registering Customer Inbound MQ as Foreign queue:'||SQLERRM,gv_log);   
	END;
	xxpso_print_debug('Register Customer Inbound MQ as Foreign queue:: End :',gv_log);

	xxpso_print_debug('Register Product Inbound MQ as Foreign queue:: Start :',gv_log);  
	BEGIN

		DBMS_MGWADM.register_foreign_queue
				 (NAME                => 'XXPSO_PDH_MQ_IN',
				  linkname            => 'XXPSO_PDH_OUT_LINK',
				  provider_queue      => 'MDM.DATAHUB.PRODMGMT.INBOUND.1',
				  domain              => DBMS_MGWADM.domain_queue
				 );
	EXCEPTION
	WHEN OTHERS THEN
	xxpso_print_debug('Error while Registering Product Inbound MQ as Foreign queue:'||SQLERRM,gv_log);   
	END;
	xxpso_print_debug('Register Product Inbound MQ as Foreign queue:: End :',gv_log);
	
	
      --Changes for v2.0 End.
      
      --Changes for v3.0 Begin
	xxpso_print_debug('Register Customer Acknowledgement Outbound MQ as Foreign queue:: Start :',gv_log);      
	BEGIN
		DBMS_MGWADM.register_foreign_queue
			 (NAME                => 'XXPSO_CDH_ACK_MQ_OUT',
			  linkname            => 'XXPSO_CDH_OUT_LINK',
			  provider_queue      => 'MDMACK.DATAHUB.CUSTMGMT.OUTBOUND.1',
			  domain              => DBMS_MGWADM.domain_queue
			 );

	EXCEPTION
	WHEN OTHERS THEN
	xxpso_print_debug('Error while Registering Customer Acknowledgement Outbound MQ as Foreign queue:'||SQLERRM,gv_log);   
	END;
	xxpso_print_debug('Register Customer Acknowledgement Outbound MQ as Foreign queue:: End :',gv_log);    
	
	xxpso_print_debug('Register Product Acknowledgement Outbound MQ as Foreign queue:: Start :',gv_log);      
	BEGIN
		DBMS_MGWADM.register_foreign_queue
			 (NAME                => 'XXPSO_PDH_ACK_MQ_OUT',
			  linkname            => 'XXPSO_PDH_OUT_LINK',
			  provider_queue      => 'MDMACK.DATAHUB.PRODMGMT.OUTBOUND.1',
			  domain              => DBMS_MGWADM.domain_queue
			 );

	EXCEPTION
	WHEN OTHERS THEN
	xxpso_print_debug('Error while Registering Product Acknowledgement Outbound MQ as Foreign queue:'||SQLERRM,gv_log);   
	END;
	xxpso_print_debug('Register Product Acknowledgement Outbound MQ as Foreign queue:: End :',gv_log);   	
      --Changes for v3.0 End
      
      xxpso_print_debug('Create xxpso_mdm_mq_asforeignque_p:: End :',gv_log);
   EXCEPTION
      WHEN OTHERS
      THEN
         fnd_file.put_line (fnd_file.LOG,
                               'Error in MQ register as Foreign queue - '
                            || SUBSTR (SQLERRM, 1, 500)
                           );
   END xxpso_mdm_mq_asforeignque_p;
   


   /***********************************************************************
   				De-Register Foreign Queues
   *************************************************************************/   
   
   
   PROCEDURE xxpso_mdm_deregister_que_p (p_debug_flag IN VARCHAR2)
   IS
   BEGIN
   
	xxpso_print_debug('Deregister xxpso_mdm_deregister_que_p:: Start :',gv_log);

	xxpso_print_debug('Deregister XXPSO_CDH_MQ_OUT:: Start :',gv_log);
	BEGIN
	DBMS_MGWADM.UNREGISTER_FOREIGN_QUEUE(name =>'XXPSO_CDH_MQ_OUT', linkname=>'XXPSO_CDH_OUT_LINK');
	EXCEPTION
	WHEN OTHERS THEN
	xxpso_print_debug('Error while Deregister XXPSO_CDH_MQ_OUT:'||SQLERRM,gv_log);   
	END;
	xxpso_print_debug('Deregister XXPSO_CDH_MQ_OUT:: End :',gv_log);

	xxpso_print_debug('Deregister XXPSO_PDH_MQ_OUT:: Start :',gv_log);
	BEGIN
	DBMS_MGWADM.UNREGISTER_FOREIGN_QUEUE(name =>'XXPSO_PDH_MQ_OUT', linkname=>'XXPSO_PDH_OUT_LINK');
	EXCEPTION
	WHEN OTHERS THEN
	xxpso_print_debug('Error while Deregister XXPSO_PDH_MQ_OUT:'||SQLERRM,gv_log);   
	END;
	xxpso_print_debug('Deregister XXPSO_PDH_MQ_OUT:: End :',gv_log);

	xxpso_print_debug('Deregister XXPSO_CDH_MQ_IN:: Start :',gv_log);
	BEGIN
	DBMS_MGWADM.UNREGISTER_FOREIGN_QUEUE(name =>'XXPSO_CDH_MQ_IN', linkname=>'XXPSO_CDH_OUT_LINK');
	EXCEPTION
	WHEN OTHERS THEN
	xxpso_print_debug('Error while Deregister XXPSO_CDH_MQ_IN:'||SQLERRM,gv_log);   
	END;
	xxpso_print_debug('Deregister XXPSO_CDH_MQ_IN:: End :',gv_log);

	xxpso_print_debug('Deregister XXPSO_PDH_MQ_IN:: Start :',gv_log);
	BEGIN
	DBMS_MGWADM.UNREGISTER_FOREIGN_QUEUE(name =>'XXPSO_PDH_MQ_IN', linkname=>'XXPSO_PDH_OUT_LINK');
	EXCEPTION
	WHEN OTHERS THEN
	xxpso_print_debug('Error while Deregister XXPSO_PDH_MQ_IN:'||SQLERRM,gv_log);   
	END;
	xxpso_print_debug('Deregister XXPSO_PDH_MQ_IN:: End :',gv_log);
	
	
	--Changes for v3.0 Begin
	xxpso_print_debug('Deregister XXPSO_CDH_ACK_MQ_OUT:: Start :',gv_log);
	BEGIN
	DBMS_MGWADM.UNREGISTER_FOREIGN_QUEUE(name =>'XXPSO_CDH_ACK_MQ_OUT', linkname=>'XXPSO_CDH_OUT_LINK');
	EXCEPTION
	WHEN OTHERS THEN
	xxpso_print_debug('Error while Deregister XXPSO_CDH_ACK_MQ_OUT:'||SQLERRM,gv_log);   
	END;
	xxpso_print_debug('Deregister XXPSO_CDH_ACK_MQ_OUT:: End :',gv_log);
	
	xxpso_print_debug('Deregister XXPSO_PDH_ACK_MQ_OUT:: Start :',gv_log);
	BEGIN
	DBMS_MGWADM.UNREGISTER_FOREIGN_QUEUE(name =>'XXPSO_PDH_ACK_MQ_OUT', linkname=>'XXPSO_PDH_OUT_LINK');
	EXCEPTION
	WHEN OTHERS THEN
	xxpso_print_debug('Error while Deregister XXPSO_PDH_ACK_MQ_OUT:'||SQLERRM,gv_log);   
	END;
	xxpso_print_debug('Deregister XXPSO_PDH_ACK_MQ_OUT:: End :',gv_log);	
	--Changes for v3.0 End


	xxpso_print_debug('Deregister xxpso_mdm_deregister_que_p:: End :',gv_log);
   
   EXCEPTION
      WHEN OTHERS
      THEN
         fnd_file.put_line (fnd_file.LOG,
                               'Error in Degistering MQ register as Foreign queue - '
                            || SUBSTR (SQLERRM, 1, 500)
                           );
   END xxpso_mdm_deregister_que_p;
   


   /***********************************************************************
   				Create Jobs
   *************************************************************************/      

   PROCEDURE xxpso_mdm_create_job_p (p_debug_flag IN VARCHAR2)
   IS
   BEGIN

	xxpso_print_debug('Create xxpso_mdm_create_job_p:: Start :',gv_log);


	xxpso_print_debug('Create XXPSO_PDHAQ2MQ_JOB:: Start :',gv_log);      
	BEGIN

	DBMS_MGWADM.create_job
			(job_name              => 'XXPSO_PDHAQ2MQ_JOB',
			 propagation_type      => DBMS_MGWADM.outbound_propagation,
			 destination           => 'XXPSO_PDH_MQ_OUT@XXPSO_PDH_OUT_LINK',
			 -- registered non-Oracle queue
			 SOURCE                => 'XXPSO.XXPSO_PDH_QUEUE_OUT'
			);                                         -- AQ queue
	EXCEPTION
	WHEN OTHERS THEN
	xxpso_print_debug('Error while creating job XXPSO_PDHAQ2MQ_JOB:'||SQLERRM,gv_log);   
	END;
	xxpso_print_debug('Create XXPSO_PDHAQ2MQ_JOB:: End :',gv_log);	


	xxpso_print_debug('Enable XXPSO_PDHAQ2MQ_JOB:: Start :',gv_log);       
	BEGIN
		DBMS_MGWADM.enable_job ('XXPSO_PDHAQ2MQ_JOB');
	EXCEPTION
	WHEN OTHERS THEN
	xxpso_print_debug('Error while enabling job XXPSO_PDHAQ2MQ_JOB:'||SQLERRM,gv_log);   
	END;
	xxpso_print_debug('Enable XXPSO_PDHAQ2MQ_JOB:: End :',gv_log);	

     
	xxpso_print_debug('Create XXPSO_CDHAQ2MQ_JOB:: Start :',gv_log);      
	BEGIN
		DBMS_MGWADM.create_job
				(job_name              => 'XXPSO_CDHAQ2MQ_JOB',
				 propagation_type      => DBMS_MGWADM.outbound_propagation,
				 destination           => 'XXPSO_CDH_MQ_OUT@XXPSO_CDH_OUT_LINK',
				 -- registered non-Oracle queue
				 SOURCE                => 'XXPSO.XXPSO_CDH_QUEUE_OUT'
				);                                         -- AQ queue
	EXCEPTION
	WHEN OTHERS THEN
	xxpso_print_debug('Error while creating job XXPSO_CDHAQ2MQ_JOB:'||SQLERRM,gv_log);   
	END;
	xxpso_print_debug('Create XXPSO_CDHAQ2MQ_JOB:: End :',gv_log);	
      
		
	xxpso_print_debug('Enable XXPSO_CDHAQ2MQ_JOB:: Start :',gv_log);       
	BEGIN
		DBMS_MGWADM.enable_job ('XXPSO_CDHAQ2MQ_JOB');
	EXCEPTION
	WHEN OTHERS THEN
	xxpso_print_debug('Error while enabling job XXPSO_CDHAQ2MQ_JOB:'||SQLERRM,gv_log);   
	END;
	xxpso_print_debug('Enable XXPSO_CDHAQ2MQ_JOB:: End :',gv_log);	  
	
	
      --Changes for v2.0  Begin
	xxpso_print_debug('Create XXPSO_PDHMQ2AQ_JOB:: Start :',gv_log);   
	BEGIN

		DBMS_MGWADM.create_job
				(job_name              => 'XXPSO_PDHMQ2AQ_JOB',
				 propagation_type      => DBMS_MGWADM.inbound_propagation,
				 SOURCE                => 'XXPSO_PDH_MQ_IN@XXPSO_PDH_OUT_LINK',
				 destination           => 'XXPSO.XXPSO_PDH_QUEUE_IN'
				 -- registered non-Oracle queue
				);                                         -- AQ queue
	EXCEPTION
	WHEN OTHERS THEN
	xxpso_print_debug('Error while creating job XXPSO_PDHMQ2AQ_JOB:'||SQLERRM,gv_log);   
	END;
	xxpso_print_debug('Create XXPSO_PDHMQ2AQ_JOB:: End :',gv_log);	

	
	xxpso_print_debug('Enable XXPSO_PDHMQ2AQ_JOB:: Start :',gv_log);       
	BEGIN
		DBMS_MGWADM.enable_job ('XXPSO_PDHMQ2AQ_JOB');
	EXCEPTION
	WHEN OTHERS THEN
	xxpso_print_debug('Error while enabling job XXPSO_PDHMQ2AQ_JOB:'||SQLERRM,gv_log);   
	END;
	xxpso_print_debug('Enable XXPSO_PDHMQ2AQ_JOB:: End :',gv_log);	


	xxpso_print_debug('Create XXPSO_CDHMQ2AQ_JOB:: Start :',gv_log);          
	BEGIN
		DBMS_MGWADM.create_job
				(job_name              => 'XXPSO_CDHMQ2AQ_JOB',
				 propagation_type      => DBMS_MGWADM.inbound_propagation,
				 SOURCE                => 'XXPSO_CDH_MQ_IN@XXPSO_CDH_OUT_LINK',
				 -- registered non-Oracle queue
				 destination           => 'XXPSO.XXPSO_CDH_QUEUE_IN'
				);                                         -- AQ queue
	EXCEPTION
	WHEN OTHERS THEN
	xxpso_print_debug('Error while creating job XXPSO_CDHMQ2AQ_JOB:'||SQLERRM,gv_log);   
	END;
	xxpso_print_debug('Create XXPSO_CDHMQ2AQ_JOB:: End :',gv_log);	
		
      
	xxpso_print_debug('Enable XXPSO_CDHMQ2AQ_JOB:: Start :',gv_log);       
	BEGIN
		DBMS_MGWADM.enable_job ('XXPSO_CDHMQ2AQ_JOB');
	EXCEPTION
	WHEN OTHERS THEN
	xxpso_print_debug('Error while enabling job XXPSO_CDHMQ2AQ_JOB:'||SQLERRM,gv_log);   
	END;
	xxpso_print_debug('Enable XXPSO_CDHMQ2AQ_JOB:: End :',gv_log);	    
      --Changes for v2.0  End
      
      --Changes for v3.0 Begin

	xxpso_print_debug('Create XXPSO_CDHAQ2MQ_ACK_JOB:: Start :',gv_log);      
	BEGIN

	DBMS_MGWADM.create_job
			(job_name              => 'XXPSO_CDHAQ2MQ_ACK_JOB',
			 propagation_type      => DBMS_MGWADM.outbound_propagation,
			 destination           => 'XXPSO_CDH_ACK_MQ_OUT@XXPSO_CDH_OUT_LINK',
			 -- registered non-Oracle queue
			 SOURCE                => 'XXPSO.XXPSO_CDH_ACK_QUEUE_OUT'
			);                                         -- AQ queue
	EXCEPTION
	WHEN OTHERS THEN
	xxpso_print_debug('Error while creating job XXPSO_CDHAQ2MQ_ACK_JOB:'||SQLERRM,gv_log);   
	END;
	xxpso_print_debug('Create XXPSO_CDHAQ2MQ_ACK_JOB:: End :',gv_log);	


	xxpso_print_debug('Enable XXPSO_CDHAQ2MQ_ACK_JOB:: Start :',gv_log);       
	BEGIN
		DBMS_MGWADM.enable_job ('XXPSO_CDHAQ2MQ_ACK_JOB');
	EXCEPTION
	WHEN OTHERS THEN
	xxpso_print_debug('Error while enabling job XXPSO_CDHAQ2MQ_ACK_JOB:'||SQLERRM,gv_log);   
	END;
	xxpso_print_debug('Enable XXPSO_CDHAQ2MQ_ACK_JOB:: End :',gv_log);	
	
	xxpso_print_debug('Create XXPSO_PDHAQ2MQ_ACK_JOB:: Start :',gv_log);      
	BEGIN

	DBMS_MGWADM.create_job
			(job_name              => 'XXPSO_PDHAQ2MQ_ACK_JOB',
			 propagation_type      => DBMS_MGWADM.outbound_propagation,
			 destination           => 'XXPSO_PDH_ACK_MQ_OUT@XXPSO_PDH_OUT_LINK',
			 -- registered non-Oracle queue
			 SOURCE                => 'XXPSO.XXPSO_PDH_ACK_QUEUE_OUT'
			);                                         -- AQ queue
	EXCEPTION
	WHEN OTHERS THEN
	xxpso_print_debug('Error while creating job XXPSO_PDHAQ2MQ_ACK_JOB:'||SQLERRM,gv_log);   
	END;
	xxpso_print_debug('Create XXPSO_PDHAQ2MQ_ACK_JOB:: End :',gv_log);	


	xxpso_print_debug('Enable XXPSO_PDHAQ2MQ_ACK_JOB:: Start :',gv_log);       
	BEGIN
		DBMS_MGWADM.enable_job ('XXPSO_PDHAQ2MQ_ACK_JOB');
	EXCEPTION
	WHEN OTHERS THEN
	xxpso_print_debug('Error while enabling job XXPSO_PDHAQ2MQ_ACK_JOB:'||SQLERRM,gv_log);   
	END;
	xxpso_print_debug('Enable XXPSO_PDHAQ2MQ_ACK_JOB:: End :',gv_log);		
      
      --Changes for v3.0 End
      
      xxpso_print_debug('Create xxpso_mdm_create_job_p:: End :',gv_log);
   EXCEPTION
      WHEN OTHERS
      THEN
         fnd_file.put_line (fnd_file.LOG,
                               'Error in creating job- '
                            || SUBSTR (SQLERRM, 1, 500)
                           );
   END xxpso_mdm_create_job_p;
   

   /***********************************************************************
   				Drop Jobs
   *************************************************************************/ 
   
   
   PROCEDURE xxpso_mdm_drop_job_p (p_debug_flag IN VARCHAR2)
   IS
   BEGIN
   
   xxpso_print_debug('Dropping xxpso_mdm_drop_job_p:: Start :',gv_log);

	xxpso_print_debug('Dropping XXPSO_CDHAQ2MQ_JOB:: Start :',gv_log);
	BEGIN
		DBMS_MGWADM.REMOVE_JOB (job_name => 'XXPSO_CDHAQ2MQ_JOB', force => DBMS_MGWADM.FORCE);
	EXCEPTION
	WHEN OTHERS THEN
	xxpso_print_debug('Error while dropping job XXPSO_CDHAQ2MQ_JOB:'||SQLERRM,gv_log);   
	END;
	xxpso_print_debug('Dropping XXPSO_CDHAQ2MQ_JOB:: End :',gv_log);

	xxpso_print_debug('Dropping XXPSO_PDHAQ2MQ_JOB:: Start :',gv_log);
	BEGIN
		DBMS_MGWADM.REMOVE_JOB (job_name => 'XXPSO_PDHAQ2MQ_JOB', force => DBMS_MGWADM.FORCE);
	EXCEPTION
	WHEN OTHERS THEN
	xxpso_print_debug('Error while dropping job XXPSO_PDHAQ2MQ_JOB:'||SQLERRM,gv_log);   
	END;
	xxpso_print_debug('Dropping XXPSO_PDHAQ2MQ_JOB:: End :',gv_log);
	
	xxpso_print_debug('Dropping XXPSO_CDHMQ2AQ_JOB:: Start :',gv_log);
	BEGIN
		DBMS_MGWADM.REMOVE_JOB (job_name => 'XXPSO_CDHMQ2AQ_JOB', force => DBMS_MGWADM.FORCE);
	EXCEPTION
	WHEN OTHERS THEN
	xxpso_print_debug('Error while dropping job XXPSO_CDHMQ2AQ_JOB:'||SQLERRM,gv_log);   
	END;
	xxpso_print_debug('Dropping XXPSO_CDHMQ2AQ_JOB:: End :',gv_log);

	xxpso_print_debug('Dropping XXPSO_PDHMQ2AQ_JOB:: Start :',gv_log);
	BEGIN
		DBMS_MGWADM.REMOVE_JOB (job_name => 'XXPSO_PDHMQ2AQ_JOB', force => DBMS_MGWADM.FORCE);
	EXCEPTION
	WHEN OTHERS THEN
	xxpso_print_debug('Error while dropping job XXPSO_PDHMQ2AQ_JOB:'||SQLERRM,gv_log);   
	END;
	xxpso_print_debug('Dropping XXPSO_PDHMQ2AQ_JOB:: End :',gv_log);
	
	
	--Changes for v3.0 Begin
	xxpso_print_debug('Dropping XXPSO_CDHAQ2MQ_ACK_JOB:: Start :',gv_log);
	BEGIN
		DBMS_MGWADM.REMOVE_JOB (job_name => 'XXPSO_CDHAQ2MQ_ACK_JOB', force => DBMS_MGWADM.FORCE);
	EXCEPTION
	WHEN OTHERS THEN
	xxpso_print_debug('Error while dropping job XXPSO_CDHAQ2MQ_ACK_JOB:'||SQLERRM,gv_log);   
	END;
	xxpso_print_debug('Dropping XXPSO_CDHAQ2MQ_ACK_JOB:: End :',gv_log);		
	
	xxpso_print_debug('Dropping XXPSO_PDHAQ2MQ_ACK_JOB:: Start :',gv_log);
	BEGIN
		DBMS_MGWADM.REMOVE_JOB (job_name => 'XXPSO_PDHAQ2MQ_ACK_JOB', force => DBMS_MGWADM.FORCE);
	EXCEPTION
	WHEN OTHERS THEN
	xxpso_print_debug('Error while dropping job XXPSO_PDHAQ2MQ_ACK_JOB:'||SQLERRM,gv_log);   
	END;
	xxpso_print_debug('Dropping XXPSO_PDHAQ2MQ_ACK_JOB:: End :',gv_log);		
	
	--Changes for v3.0 End
	
   
   xxpso_print_debug('Dropping xxpso_mdm_drop_job_p:: End :',gv_log);
   
   EXCEPTION
      WHEN OTHERS
      THEN
         fnd_file.put_line (fnd_file.LOG,
                               'Error in dropping job- '
                            || SUBSTR (SQLERRM, 1, 500)
                           );
   END xxpso_mdm_drop_job_p;   
   


   /***********************************************************************
   				Main Procedure
   *************************************************************************/ 
   PROCEDURE xxpso_mdm_main_p (
      p_errbuf       OUT      VARCHAR2,
      p_retcode      OUT      NUMBER,
      p_debug_flag   IN       VARCHAR2,
      p_configuration IN      VARCHAR2
   )
   IS
   BEGIN
      gc_debug_flag := NVL(p_debug_flag,'N');
      IF p_configuration = 'DROP' THEN
	      xxpso_mdm_gateway_stop_p (p_debug_flag);
	      xxpso_mdm_drop_job_p (p_debug_flag);
	      xxpso_mdm_deregister_que_p (p_debug_flag);
	      xxpso_mdm_drop_msglink2mq_p (p_debug_flag);
      ELSIF p_configuration = 'CREATE' THEN
	      xxpso_mdm_gateway_stop_p (p_debug_flag);
	      xxpso_mdm_create_aq_p (p_debug_flag);
	      xxpso_mdm_create_msglink2mq_p (p_debug_flag);
	      xxpso_mdm_mq_asforeignque_p (p_debug_flag);
	      xxpso_mdm_create_job_p (p_debug_flag);
	      xxpso_mdm_gateway_start_p (p_debug_flag);
      END IF;
   EXCEPTION
      WHEN OTHERS
      THEN
         fnd_file.put_line (fnd_file.LOG,
                               'Error in Configuring AQ and MQ - '
                            || SUBSTR (SQLERRM, 1, 500)
                           );
   END xxpso_mdm_main_p;
END xxpso_mdm_aq2mq_config_pkg;
/
SHOW ERRORS
EXIT SUCCESS
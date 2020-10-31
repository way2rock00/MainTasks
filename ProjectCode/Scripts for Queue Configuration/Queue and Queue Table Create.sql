--Create Queue Table
BEGIN
      DBMS_AQADM.create_queue_table
                             (queue_table             => 'XXPSO.XXPSO_PDH_QUEUE_TBL_IN',
                              multiple_consumers      => FALSE,
                              queue_payload_type      => 'SYS.AQ$_JMS_TEXT_MESSAGE'--'SYS.AQ$_JMS_BYTES_MESSAGE'--_JMS_TEXT_MESSAG
                             );
END;
/

BEGIN
      DBMS_AQADM.create_queue_table
                             (queue_table             => 'XXPSO.XXPSO_CDH_QUEUE_TBL_IN',
                              multiple_consumers      => FALSE,
                              queue_payload_type      => 'SYS.AQ$_JMS_TEXT_MESSAGE'--'SYS.AQ$_JMS_BYTES_MESSAGE'--_JMS_TEXT_MESSAG
                             );
END;
/

BEGIN
      DBMS_AQADM.create_queue_table
                             (queue_table             => 'XXPSO.XXPSO_PDH_QUEUE_TBL_OUT',
                              multiple_consumers      => TRUE,
                              queue_payload_type      => 'SYS.AQ$_JMS_TEXT_MESSAGE'--'SYS.AQ$_JMS_BYTES_MESSAGE'--_JMS_TEXT_MESSAG
                             );
END;
/

BEGIN
      DBMS_AQADM.create_queue_table
                             (queue_table             => 'XXPSO.XXPSO_CDH_QUEUE_TBL_OUT',
                              multiple_consumers      => TRUE,
                              queue_payload_type      => 'SYS.AQ$_JMS_TEXT_MESSAGE'--'SYS.AQ$_JMS_BYTES_MESSAGE'--_JMS_TEXT_MESSAG
                             );
END;
/


--Create Queue
BEGIN
        DBMS_AQADM.create_queue (queue_name       => 'XXPSO.XXPSO_PDH_QUEUE_IN',
                               queue_table      => 'XXPSO.XXPSO_PDH_QUEUE_TBL_IN',
                               max_retries      => 1000
                              );                           
END;
/

BEGIN
        DBMS_AQADM.create_queue (queue_name       => 'XXPSO.XXPSO_CDH_QUEUE_IN',
                               queue_table      => 'XXPSO.XXPSO_CDH_QUEUE_TBL_IN',
                               max_retries      => 1000
                              );                           
END;
/

BEGIN
        DBMS_AQADM.create_queue (queue_name       => 'XXPSO.XXPSO_PDH_QUEUE_OUT',
                               queue_table      => 'XXPSO.XXPSO_PDH_QUEUE_TBL_OUT',
                               max_retries      => 1000
                              );                           
END;
/

BEGIN
        DBMS_AQADM.create_queue (queue_name       => 'XXPSO.XXPSO_CDH_QUEUE_OUT',
                               queue_table      => 'XXPSO.XXPSO_CDH_QUEUE_TBL_OUT',
                               max_retries      => 1000
                              );                           
END;
/

--Start Queue
BEGIN
DBMS_AQADM.start_queue (queue_name => 'XXPSO.XXPSO_PDH_QUEUE_IN');
END;
/

BEGIN
DBMS_AQADM.start_queue (queue_name => 'XXPSO.XXPSO_CDH_QUEUE_IN');
END;
/

BEGIN
DBMS_AQADM.start_queue (queue_name => 'XXPSO.XXPSO_PDH_QUEUE_OUT');
END;
/

BEGIN
DBMS_AQADM.start_queue (queue_name => 'XXPSO.XXPSO_CDH_QUEUE_OUT');
END;
/

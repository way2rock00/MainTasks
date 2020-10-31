--Stop Queue
BEGIN
   DBMS_AQADM.STOP_QUEUE(
      queue_name        => 'XXPSO.XX_TEMP_QUEUE');
END;
/

BEGIN
   DBMS_AQADM.STOP_QUEUE(
      queue_name        => 'XXPSO.XXPSO_PDH_QUEUE_IN');
END;
/

BEGIN
   DBMS_AQADM.STOP_QUEUE(
      queue_name        => 'XXPSO.XXPSO_CDH_QUEUE_IN');   
END;
/

BEGIN
   DBMS_AQADM.STOP_QUEUE(
      queue_name        => 'XXPSO.XXPSO_PDH_QUEUE_OUT');
END;
/

BEGIN
   DBMS_AQADM.STOP_QUEUE(
      queue_name        => 'XXPSO.XXPSO_CDH_QUEUE_OUT');  
END;
/      


--Drop Queue 
BEGIN
   DBMS_AQADM.DROP_QUEUE(
      queue_name         => 'XXPSO.XX_TEMP_QUEUE');
END;
/ 

BEGIN
   DBMS_AQADM.DROP_QUEUE(
      queue_name         => 'XXPSO.XXPSO_PDH_QUEUE_IN');
END;
/

BEGIN
   DBMS_AQADM.DROP_QUEUE(
      queue_name         => 'XXPSO.XXPSO_CDH_QUEUE_IN');  
END;
/

BEGIN
   DBMS_AQADM.DROP_QUEUE(
      queue_name         => 'XXPSO.XXPSO_PDH_QUEUE_OUT'); 
END;
/

BEGIN
   DBMS_AQADM.DROP_QUEUE(
      queue_name         => 'XXPSO.XXPSO_CDH_QUEUE_OUT');            
END;
/

--Drop Queue Table
BEGIN
   DBMS_AQADM.DROP_QUEUE_TABLE(
      queue_table        => 'XXPSO.XX_TEMP_QUEUE_TBL');
END;
/ 

BEGIN
   DBMS_AQADM.DROP_QUEUE_TABLE(
      queue_table        => 'XXPSO.XXPSO_PDH_QUEUE_TBL_IN');
END;
/

BEGIN
   DBMS_AQADM.DROP_QUEUE_TABLE(
      queue_table        => 'XXPSO.XXPSO_PDH_QUEUE_TBL_OUT');  
END;
/

BEGIN
   DBMS_AQADM.DROP_QUEUE_TABLE(
      queue_table        => 'XXPSO.XXPSO_CDH_QUEUE_TBL_IN');
END;
/

BEGIN
   DBMS_AQADM.DROP_QUEUE_TABLE(
      queue_table        => 'XXPSO.XXPSO_CDH_QUEUE_TBL_OUT');          
END;
/

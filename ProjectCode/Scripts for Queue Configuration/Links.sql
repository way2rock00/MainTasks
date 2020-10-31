--Deregister Link
BEGIN
dbms_mgwadm.remove_msgsystem_link(linkname =>'XXPSO_PDH_IN_LINK');
END;
/

BEGIN
dbms_mgwadm.remove_msgsystem_link(linkname =>'XXPSO_CDH_IN_LINK');
END;
/

BEGIN
dbms_mgwadm.remove_msgsystem_link(linkname =>'XXPSO_PDH_OUT_LINK');
END;
/

BEGIN
dbms_mgwadm.remove_msgsystem_link(linkname =>'XXPSO_CDH_OUT_LINK');
END;
/

BEGIN
dbms_mgwadm.remove_msgsystem_link(linkname =>'XXPSO_PDH_LINK');
END;
/


BEGIN
dbms_mgwadm.remove_msgsystem_link(linkname =>'XXPSO_CDH_LINK');
END;
/



--Register Link
declare
   v_options sys.mgw_properties;
   v_prop sys.mgw_mqseries_properties;
begin
   v_prop := sys.mgw_mqseries_properties.construct();
   v_prop.max_connections := 1;
      
   v_prop.interface_type := DBMS_MGWADM.JMS_QUEUE_CONNECTION;--JMS_QUEUE_CONNECTION;
   v_prop.username := null;
   v_prop.password := null;
   v_prop.hostname := 'simbmq.lo3dev.pearson.com';-- Will change as per Instance
   v_prop.port     := 3430;			  -- Will change as per Instance
   v_prop.channel  := 'DEV.WMBTK.SVRCONN';        -- Will change as per Instance
   v_prop.queue_manager := 'LO3DQM01';		  -- Will change as per Instance 
      
   v_prop.outbound_log_queue := 'MDM.QUEUE.OUTBOUND.LOG';
   v_prop.inbound_log_queue := 'MDM.QUEUE.INBOUND.LOG';
   
   BEGIN
   dbms_mgwadm.create_msgsystem_link(
      linkname => 'XXPSO_PDH_LINK', properties => v_prop, options => v_options );
   EXCEPTION
   WHEN OTHERS THEN
   dbms_output.put_line('Error:'||SQLERRM);
   END;
   
   BEGIN
   dbms_mgwadm.create_msgsystem_link(
      linkname => 'XXPSO_CDH_LINK', properties => v_prop, options => v_options );
   EXCEPTION
    WHEN OTHERS THEN
   dbms_output.put_line('Error:'||SQLERRM);
   END;   
end;
/

--Deregister foreign queue
BEGIN
DBMS_MGWADM.UNREGISTER_FOREIGN_QUEUE(name =>'XXPSO_PDH_MQ_IN', linkname=>'XXPSO_PDH_OUT_LINK');--XXPSO_PDH_MQ_IN	XXPSO_PDH_IN_LINK
END;
/

BEGIN
DBMS_MGWADM.UNREGISTER_FOREIGN_QUEUE(name =>'XXPSO_PDH_MQ_OUT', linkname=>'XXPSO_PDH_OUT_LINK');--XXPSO_PDH_MQ_IN	XXPSO_PDH_IN_LINK
END;
/

BEGIN
DBMS_MGWADM.UNREGISTER_FOREIGN_QUEUE(name =>'XXPSO_CDH_MQ_IN', linkname=>'XXPSO_CDH_OUT_LINK');--XXPSO_PDH_MQ_IN	XXPSO_PDH_IN_LINK
END;
/

BEGIN
DBMS_MGWADM.UNREGISTER_FOREIGN_QUEUE(name =>'XXPSO_CDH_MQ_OUT', linkname=>'XXPSO_CDH_OUT_LINK');--XXPSO_PDH_MQ_IN	XXPSO_PDH_IN_LINK
END;
/

begin
   dbms_mgwadm.register_foreign_queue(--XXPSO_TEMP_QUEUE_MQ
   name => 'XXPSO_PDH_MQ_IN',
   linkname => 'XXPSO_PDH_LINK',
   provider_queue => 'MDM.DATAHUB.PRODMGMT.INBOUND.1',--'MDM.DATAHUB.PRODMGMT.?IN?BOUND.1',--XXPSO_PDH_MQ_IN	XXPSO_PDH_OUT_LINK
   domain => dbms_mgwadm.DOMAIN_QUEUE);
end;
/

begin
   dbms_mgwadm.register_foreign_queue(--XXPSO_TEMP_QUEUE_MQ
   name => 'XXPSO_CDH_MQ_IN',
   linkname => 'XXPSO_CDH_LINK',
   provider_queue => 'MDM.DATAHUB.CUSTMGMT.INBOUND.1',--'MDM.DATAHUB.PRODMGMT.?IN?BOUND.1',--XXPSO_PDH_MQ_IN	XXPSO_PDH_OUT_LINK
   domain => dbms_mgwadm.DOMAIN_QUEUE);
end;
/

begin
   dbms_mgwadm.register_foreign_queue(--XXPSO_TEMP_QUEUE_MQ
   name => 'XXPSO_PDH_MQ_OUT',
   linkname => 'XXPSO_PDH_LINK',
   provider_queue => 'MDM.DATAHUB.PRODMGMT.OUTBOUND.1',--'MDM.DATAHUB.PRODMGMT.?IN?BOUND.1',--XXPSO_PDH_MQ_IN	XXPSO_PDH_OUT_LINK
   domain => dbms_mgwadm.DOMAIN_QUEUE);
end;
/

begin
   dbms_mgwadm.register_foreign_queue(--XXPSO_TEMP_QUEUE_MQ
   name => 'XXPSO_CDH_MQ_OUT',
   linkname => 'XXPSO_CDH_LINK',
   provider_queue => 'MDM.DATAHUB.CUSTMGMT.OUTBOUND.1',--'MDM.DATAHUB.PRODMGMT.?IN?BOUND.1',--XXPSO_PDH_MQ_IN	XXPSO_PDH_OUT_LINK
   domain => dbms_mgwadm.DOMAIN_QUEUE);
end;
/
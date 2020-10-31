--Stop Job
BEGIN
DBMS_MGWADM.REMOVE_JOB (job_name => 'XXPSO_CDHAQ2MQ_JOB', force => DBMS_MGWADM.FORCE);
END;
/

BEGIN
DBMS_MGWADM.REMOVE_JOB (job_name => 'XXPSO_PDHAQ2MQ_JOB', force => DBMS_MGWADM.FORCE);
END;
/

BEGIN
DBMS_MGWADM.REMOVE_JOB (job_name => 'XXPSO_CDHMQ2AQ_JOB', force => DBMS_MGWADM.FORCE);
END;
/

BEGIN
DBMS_MGWADM.REMOVE_JOB (job_name => 'XXPSO_PDHMQ2AQ_JOB', force => DBMS_MGWADM.FORCE);
END;
/




-- Create Jobs

begin

  dbms_mgwadm.create_job(
      job_name    => 'XXPSO_PDHMQ2AQ_JOB',
      propagation_type => dbms_mgwadm.inbound_propagation,
      source => 'XXPSO_PDH_MQ_IN@XXPSO_PDH_LINK',--'XXPSO.XXPSO_PDH_QUEUE_OUT'--XXPSO_PDH_MQ_IN	XXPSO_PDH_IN_LINK
      destination      => 'XXPSO.XXPSO_PDH_QUEUE_IN'  -- registered non-Oracle queue
      ); -- AQ queue
      
  DBMS_MGWADM.ENABLE_JOB ('XXPSO_PDHMQ2AQ_JOB');
end;
/

begin

  dbms_mgwadm.create_job(
      job_name    => 'XXPSO_CDHMQ2AQ_JOB',
      propagation_type => dbms_mgwadm.inbound_propagation,
      source => 'XXPSO_CDH_MQ_IN@XXPSO_CDH_LINK',--'XXPSO.XXPSO_PDH_QUEUE_OUT'--XXPSO_PDH_MQ_IN	XXPSO_PDH_IN_LINK
      destination      => 'XXPSO.XXPSO_CDH_QUEUE_IN'  -- registered non-Oracle queue
      ); -- AQ queue
      
  DBMS_MGWADM.ENABLE_JOB ('XXPSO_CDHMQ2AQ_JOB');
end;
/

begin

  dbms_mgwadm.create_job(
      job_name    => 'XXPSO_PDHAQ2MQ_JOB',
      propagation_type => dbms_mgwadm.outbound_propagation,
      source => 'XXPSO.XXPSO_PDH_QUEUE_OUT',--'XXPSO.XXPSO_PDH_QUEUE_OUT'--XXPSO_PDH_MQ_IN	XXPSO_PDH_IN_LINK
      destination      => 'XXPSO_PDH_MQ_OUT@XXPSO_PDH_LINK'  -- registered non-Oracle queue
      ); -- AQ queue
      
  DBMS_MGWADM.ENABLE_JOB ('XXPSO_PDHAQ2MQ_JOB');
end;
/

begin

  dbms_mgwadm.create_job(
      job_name    => 'XXPSO_CDHAQ2MQ_JOB',
      propagation_type => dbms_mgwadm.outbound_propagation,
      source => 'XXPSO.XXPSO_CDH_QUEUE_OUT',--'XXPSO.XXPSO_PDH_QUEUE_OUT'--XXPSO_PDH_MQ_IN	XXPSO_PDH_IN_LINK
      destination      => 'XXPSO_CDH_MQ_OUT@XXPSO_CDH_LINK'  -- registered non-Oracle queue
      ); -- AQ queue
      
  DBMS_MGWADM.ENABLE_JOB ('XXPSO_CDHAQ2MQ_JOB');
end;
/
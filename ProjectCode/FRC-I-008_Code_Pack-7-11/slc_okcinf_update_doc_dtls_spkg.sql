REM ============================================================================
REM  Program:      slc_okcinf_update_doc_dtls_spkg.sql
REM  Author:       Priyank Dube
REM  Date:         29-JAN-2017
REM  Purpose:      This package is used to execute the following activities :
REM                1. Called by the business events 
REM                2. Attach signed Contract document
REM                3. Update Singner information
REM                4. Change Contract status from Approved to Signed
REM  Change Log:   29-JAN-2017 Priyank Dube Created
REM ============================================================================
CREATE OR REPLACE PACKAGE APPS.slc_okcinf_update_doc_dtls_pkg
   AUTHID CURRENT_USER
AS
   TYPE main_party IS TABLE OF slc_okcinf_party_dtl_obj
      INDEX BY BINARY_INTEGER;

   /* ********************************************************************************************
      -- Procedure Name : slc_okcinf_update_document_p
      -- Purpose        : This procedure will be invoked by SOA to get the document details
      -- Parameters     : p_party_master    -- Contract detail
    --********************************************************************************************/
   PROCEDURE slc_okcinf_update_document_p (p_party_master IN main_party);

   /* ******************************************************************************
   -- Procedure main_process_p
   -- Main procedure which gets called from Concurrent program to invoke
   -- custom business event
   -- Input:
   --   p_debug_flag
   -- Output:
   --   p_errbuf
   --   p_retcode
   --*******************************************************************************/
   PROCEDURE main_process_p (x_errbuf          OUT VARCHAR2,
                             x_retcode         OUT VARCHAR2,
                             p_debug_flag   IN     VARCHAR2);
END slc_okcinf_update_doc_dtls_pkg;
/
REM ============================================================================
REM  Program:      slc_okcinf_update_doc_dtls_bpkg.sql
REM  Author:       Priyank Dube
REM  Date:         29-JAN-2017
REM  Purpose:      This package is used to execute the following activities :
REM                1. Called by the business events 
REM                2. Attach signed Contract document
REM                3. Update Singner information
REM                4. Change Contract status from Approved to Signed
REM  Change Log:   29-JAN-2017 Priyank Dube Created
REM ============================================================================
CREATE OR REPLACE PACKAGE BODY APPS.slc_okcinf_update_doc_dtls_pkg
AS
   --Global Variable Declaration
   gv_log                     VARCHAR2 (5) DEFAULT 'LOG';
   gv_out                     VARCHAR2 (5) DEFAULT 'OUT';
   gv_debug_flag              VARCHAR2 (3);
   gv_yes_code                VARCHAR2 (3) DEFAULT 'YES';
   gv_no_code                 VARCHAR2 (3) DEFAULT 'NO';
   g_org_id                   NUMBER := fnd_profile.VALUE ('ORG_ID');

   gv_error_message_txt       VARCHAR2 (1000);
   g_user_id                  NUMBER := fnd_global.user_id;
   g_login_id                 NUMBER := fnd_global.login_id;
   g_request_id               NUMBER DEFAULT fnd_global.conc_request_id;
   g_message                  VARCHAR2 (3000) := NULL;
   g_log_status               VARCHAR2 (10);
   g_business_process         VARCHAR2 (50) := 'FRC-I-008-Docusign to EBS';
   --Variables for Common Error Handling.
   gv_batch_key               VARCHAR2 (50)
      DEFAULT 'FRC-I-008' || '-' || TO_CHAR (SYSDATE, 'DDMMYYYY');
   gv_business_process_name   VARCHAR2 (100)
                                 DEFAULT 'SLC_OKCINF_UPDATE_DOC_DTLS_PKG';
   gv_cmn_err_rec             apps.slc_util_jobs_pkg.g_error_tbl_type;
   gv_cmn_err_count           NUMBER DEFAULT 0;

   /* ********************************************************************************************
        -- Procedure Name   : write_log_p
        -- Purpose          : This procedure is used to report debug messages in FND Log
        -- Input Parameters :
        --  p_msg           : Message to be printed in debug message
        --
        -- Output Parameters : N/A
        --********************************************************************************************/
   PROCEDURE write_log_p (p_in_log_type   IN VARCHAR2,
                          p_in_message    IN VARCHAR2)
   IS
   BEGIN
      IF p_in_log_type = gv_log AND gv_debug_flag = gv_yes_code
      THEN
         fnd_file.put_line (fnd_file.LOG, p_in_message);
      END IF;

      IF p_in_log_type = gv_out
      THEN
         fnd_file.put_line (fnd_file.output, p_in_message);
      END IF;
   END write_log_p;

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
                             p_debug_flag   IN     VARCHAR2)
   AS
      --
      l_event_name             VARCHAR2 (240)
                                  := 'slc.apps.okc.procurement.signeddocument';
      l_event_parameter_list   wf_parameter_list_t := wf_parameter_list_t ();
   BEGIN
      gv_debug_flag := p_debug_flag;
      write_log_p (gv_log,
                   'In slc_okcinf_update_doc_dtls_pkg.main_process_p');
      write_log_p (
         gv_out,
         '*************************Output***************************');
      write_log_p (
         gv_out,
         '*************************Parameters***************************');
      write_log_p (gv_out, 'p_debug_flag: ' || p_debug_flag);
      write_log_p (gv_out, 'g_request_id: ' || g_request_id);
      write_log_p (
         gv_out,
         '**************************************************************');
      wf_event.addparametertolist (
         p_name            => 'DOCUSIGN',
         p_value           => 'FRC-I-008',
         p_parameterlist   => l_event_parameter_list);
      wf_event.RAISE (p_event_name   => l_event_name,
                      p_event_key    => SYS_GUID (),
                      p_parameters   => l_event_parameter_list);
   --
   EXCEPTION
      WHEN OTHERS
      THEN
         --Provide context information that helps locate the source of an error.
         --
         wf_core.CONTEXT (pkg_name    => 'SLC_OKCINF_UPDATE_DOC_DTLS_PKG',
                          proc_name   => 'MAIN_PROCESS_P',
                          arg1        => l_event_name,
                          arg2        => SYS_GUID (),
                          arg3        => 1);

         write_log_p (
            gv_log,
               'Error while invoking business event:'
            || SUBSTR (SQLERRM, 1, 250));
   END main_process_p;


   /* ********************************************************************************************
      -- Procedure Name   : populate_err_object_p
      -- Purpose          : This procedure will keep on inserting error records in the error table.
      -- Input Parameters :
      --  p_in_batch_key
      --  p_in_business_entity
      --  p_in_process_id1
      --  p_in_process_id2
      --  p_in_error_code
      --  p_in_error_txt
      --  p_in_request_id
      --  p_in_attribute1
      --  p_in_attribute2
      --  p_in_attribute3
      --  p_in_attribute4
      --  p_in_attribute5
      -- Output Parameters : N/A
      --********************************************************************************************/
   PROCEDURE populate_err_object_p (
      p_in_batch_key               IN VARCHAR2,
      p_in_business_entity         IN VARCHAR2,
      p_in_process_id1             IN VARCHAR2 DEFAULT NULL,
      p_in_process_id2             IN VARCHAR2 DEFAULT NULL,
      p_in_process_id3             IN VARCHAR2 DEFAULT NULL,
      p_in_process_id4             IN VARCHAR2 DEFAULT NULL,
      p_in_process_id5             IN VARCHAR2 DEFAULT NULL,
      p_in_business_process_step   IN VARCHAR2 DEFAULT NULL,
      p_in_error_code              IN VARCHAR2 DEFAULT NULL,
      p_in_error_txt               IN VARCHAR2,
      p_in_request_id              IN NUMBER,
      p_in_attribute1              IN VARCHAR2 DEFAULT NULL,
      p_in_attribute2              IN VARCHAR2 DEFAULT NULL,
      p_in_attribute3              IN VARCHAR2 DEFAULT NULL,
      p_in_attribute4              IN VARCHAR2 DEFAULT NULL,
      p_in_attribute5              IN VARCHAR2 DEFAULT NULL)
   IS
   BEGIN
      gv_cmn_err_count := gv_cmn_err_count + 1;
      gv_cmn_err_rec (gv_cmn_err_count).seq := slc_util_batch_key_s.NEXTVAL;
      gv_cmn_err_rec (gv_cmn_err_count).business_process_entity :=
         p_in_business_entity;
      gv_cmn_err_rec (gv_cmn_err_count).business_process_id1 :=
         p_in_process_id1;
      gv_cmn_err_rec (gv_cmn_err_count).business_process_id2 :=
         p_in_process_id2;
      gv_cmn_err_rec (gv_cmn_err_count).business_process_id3 :=
         p_in_process_id3;
      gv_cmn_err_rec (gv_cmn_err_count).business_process_id4 :=
         p_in_process_id4;
      gv_cmn_err_rec (gv_cmn_err_count).business_process_id5 :=
         p_in_process_id5;
      gv_cmn_err_rec (gv_cmn_err_count).business_process_step :=
         p_in_business_process_step;
      gv_cmn_err_rec (gv_cmn_err_count).ERROR_CODE := p_in_error_code;
      gv_cmn_err_rec (gv_cmn_err_count).ERROR_TEXT := p_in_error_txt;
      gv_cmn_err_rec (gv_cmn_err_count).request_id := p_in_request_id;
      gv_cmn_err_rec (gv_cmn_err_count).attribute1 := p_in_attribute1;
      gv_cmn_err_rec (gv_cmn_err_count).attribute2 := p_in_attribute2;
      gv_cmn_err_rec (gv_cmn_err_count).attribute3 := p_in_attribute3;
      gv_cmn_err_rec (gv_cmn_err_count).attribute4 := p_in_attribute4;
      gv_cmn_err_rec (gv_cmn_err_count).attribute5 := p_in_attribute5;
   END populate_err_object_p;

   /* ****************************************************************
        NAME:              process_attachment_details_p
        PURPOSE:           This Procedure will send email notification
        Input Parameters:      p_pub_event_id
                            p_message1
                            p_message2
                            p_message3
        Output Parameters:  p_status
      *****************************************************************/
   PROCEDURE process_attachment_details_p (p_contract_id   IN     NUMBER,
                                           p_envelope_id   IN     VARCHAR2,
                                           p_file_data     IN     BLOB,
                                           x_error_data       OUT VARCHAR2)
   AS
      l_file_id                      NUMBER;
      l_cat_id                       NUMBER;
      l_usage_type          CONSTANT VARCHAR2 (1) := 'O';
      l_entity_name                  VARCHAR2 (30);
      l_file_format                  VARCHAR2 (100);
      lv_file_name                   VARCHAR2 (100);
      lv_contract_name               okc_rep_contracts_all.contract_name%TYPE;
      lv_contract_version_num        okc_rep_contracts_all.contract_version_num%TYPE;
      lv_contract_type               okc_rep_contracts_all.contract_type%TYPE;
      lv_document_id                 fnd_documents.document_id%TYPE;
      lv_attached_document_id        fnd_attached_documents.attached_document_id%TYPE;
      lv_business_document_type      VARCHAR2 (5000);
      lv_business_document_id        NUMBER;
      lv_business_document_version   NUMBER;
      lv_attached_sign_doc_id        NUMBER;
      --
      lv_return_status               VARCHAR2 (10);
      lv_msg_count                   NUMBER;
      lv_msg_data                    VARCHAR2 (5000);
      --
      lx_return_status               VARCHAR2 (10);
      lx_msg_count                   NUMBER;
      lx_msg_data                    VARCHAR2 (5000);
      --
      lv_document_type               okc_bus_doc_types_tl.NAME%TYPE;
      lv_email_dl                    fnd_lookup_values.meaning%TYPE;
      lv_dl_lkp_name                 fnd_lookup_values.attribute2%TYPE;
      --Error handling
      lv_error_msg                   VARCHAR2 (4000) := NULL;
      lv_err_flag                    VARCHAR2 (1) DEFAULT 'N';
      lv_err_msg                     VARCHAR2 (4000);
      lv_business_entity_name        VARCHAR2 (50)
                                        := 'PROCESS_ATTACHMENT_DETAILS_P';
      PRAGMA AUTONOMOUS_TRANSACTION;
   BEGIN
      --Get Contract detail
      BEGIN
         SELECT contract_name, contract_version_num, contract_type
           INTO lv_contract_name, lv_contract_version_num, lv_contract_type
           FROM okc_rep_contracts_all
          WHERE contract_id = p_contract_id;

         slc_util_jobs_pkg.slc_util_debug_procedure_p (
            'Get Contract detail ' || lv_contract_name);

         slc_util_jobs_pkg.slc_util_debug_procedure_p (
               'lx_msg_data'
            || lx_msg_data
            || ',lx_return_status:'
            || lx_return_status);
         slc_util_jobs_pkg.slc_util_debug_procedure_p (
            'Get Contract detail ' || lv_contract_name);
         --Format for File Name REP_<contract ID>_<contract name>_<version>_signed.pdf
         lv_file_name :=
               'REP_'
            || p_contract_id
            || '_'
            || lv_contract_name
            || '_'
            || lv_contract_version_num
            || '_signed.pdf';
         slc_util_jobs_pkg.slc_util_debug_procedure_p (
            'Contract_File_Name ' || lv_file_name);
      EXCEPTION
         WHEN OTHERS
         THEN
            slc_util_jobs_pkg.slc_util_debug_procedure_p (
               'Error while getting contact details ' || SQLERRM);
      END;

      --Get File ID
      SELECT fnd_lobs_s.NEXTVAL INTO l_file_id FROM DUAL;

      slc_util_jobs_pkg.slc_util_debug_procedure_p ('File_ID ' || l_file_id);

      INSERT INTO fnd_lobs (file_id,
                            file_name,
                            file_content_type,
                            file_data,
                            file_format)
           VALUES (l_file_id,
                   lv_file_name,
                   'application/pdf',
                   p_file_data,
                   'binary');

      fnd_webattch.add_attachment (
         seq_num                => 1,
         category_id            => 1000559,
         document_description   => p_envelope_id,
         datatype_id            => 6,
         -- Attachment
         text                   => NULL,
         file_name              => lv_file_name,
         url                    => NULL,
         function_name          => NULL,
         entity_name            => 'OKC_CONTRACT_DOCS',
         pk1_value              => lv_contract_type,
         pk2_value              => p_contract_id,
         pk3_value              => '-99',
         pk4_value              => NULL,
         pk5_value              => NULL,
         media_id               => l_file_id,
         user_id                => g_user_id,
         usage_type             => 'O',
         title                  => 'SignedDoc');

      BEGIN
         slc_util_jobs_pkg.slc_util_debug_procedure_p (
               'Get lv_document_id BEF '
            || l_file_id
            || ' , '
            || lv_contract_type
            || ' , '
            || p_contract_id);

         SELECT fd.document_id, fad.attached_document_id
           INTO lv_document_id, lv_attached_document_id
           FROM fnd_documents fd, fnd_attached_documents fad
          WHERE     fd.media_id = l_file_id
                AND fd.document_id = fad.document_id
                AND fad.entity_name = 'OKC_CONTRACT_DOCS'
                AND fad.pk1_value = lv_contract_type
                AND TO_NUMBER (fad.pk2_value) = (p_contract_id);

         slc_util_jobs_pkg.slc_util_debug_procedure_p (
               'Get lv_document_id'
            || lv_document_id
            || ' , '
            || lv_attached_document_id);
      EXCEPTION
         WHEN OTHERS
         THEN
            slc_util_jobs_pkg.slc_util_debug_procedure_p (
                  'Error While getting lv_attached_document_id'
               || SUBSTR (SQLERRM, 1, 250));
      END;

      --
      apps.okc_contract_docs_grp.insert_contract_doc (
         p_api_version                 => 1.0,
         p_init_msg_list               => fnd_api.g_false,
         p_validation_level            => fnd_api.g_valid_level_full,
         p_commit                      => fnd_api.g_false,
         x_return_status               => lv_return_status,
         x_msg_count                   => lv_msg_count,
         x_msg_data                    => lv_msg_data,
         p_business_document_type      => lv_contract_type,
         p_business_document_id        => p_contract_id,
         p_business_document_version   => -99,
         p_attached_document_id        => lv_attached_document_id,
         p_external_visibility_flag    => 'N',
         p_effective_from_type         => lv_contract_type,
         p_effective_from_id           => p_contract_id,
         p_effective_from_version      => '-99',
         p_include_for_approval_flag   => 'N',
         p_create_fnd_attach           => 'N',
         p_program_id                  => 0,
         p_program_application_id      => 0,
         p_request_id                  => 0,
         p_program_update_date         => SYSDATE,
         p_parent_attached_doc_id      => NULL,
         p_generated_flag              => 'N',
         p_delete_flag                 => 'N',
         p_primary_contract_doc_flag   => 'N',
         p_mergeable_doc_flag          => 'N',
         p_versioning_flag             => 'N',
         x_business_document_type      => lv_business_document_type,
         x_business_document_id        => lv_business_document_id,
         x_business_document_version   => lv_business_document_version,
         x_attached_document_id        => lv_attached_sign_doc_id);
      slc_util_jobs_pkg.slc_util_debug_procedure_p (
            'lv_business_document_type '
         || lv_business_document_type
         || ' , '
         || lv_business_document_id
         || lv_business_document_version);

      IF lv_attached_sign_doc_id IS NOT NULL
      THEN
         slc_util_jobs_pkg.slc_util_debug_procedure_p (
            'Attached Successfully ' || lv_return_status);

         --Get document Type
         BEGIN
            SELECT NAME
              INTO lv_document_type
              FROM okc_bus_doc_types_tl
             WHERE document_type = lv_contract_type AND LANGUAGE = 'US';
         EXCEPTION
            WHEN OTHERS
            THEN
               gv_error_message_txt :=
                     'Exception in GET DOCUMENT TYPE: SQLCODE - SQLERRM - '
                  || SQLCODE
                  || ' - '
                  || SQLERRM;
               slc_util_jobs_pkg.slc_util_debug_procedure_p (
                  gv_error_message_txt);
         END;

         --
         --Derive distribution List Lookup name based on the document Type
         --
         BEGIN
            SELECT attribute2
              INTO lv_dl_lkp_name
              FROM fnd_lookup_values
             WHERE     lookup_type = 'SLCPRC_DOCUSIGN_DOC_TYPE'
                   AND meaning = lv_document_type
                   AND enabled_flag = 'Y'
                   AND attribute1 = 'YES'
                   AND LANGUAGE = USERENV ('LANG')
                   AND TRUNC (SYSDATE) >=
                          TRUNC (NVL (start_date_active, SYSDATE))
                   AND TRUNC (SYSDATE) <=
                          TRUNC (NVL (end_date_active, SYSDATE));
         EXCEPTION
            WHEN OTHERS
            THEN
               gv_error_message_txt :=
                     'Exception in update_pay_method -lv_pay_method: SQLCODE - SQLERRM - '
                  || SQLCODE
                  || ' - '
                  || SQLERRM;
               slc_util_jobs_pkg.slc_util_debug_procedure_p ('derive DL');

               lv_err_msg := gv_error_message_txt;
               populate_err_object_p (
                  p_in_batch_key         => gv_batch_key,
                  p_in_business_entity   => lv_business_entity_name,
                  p_in_process_id1       => p_contract_id,
                  p_in_error_txt         => lv_err_msg,
                  p_in_request_id        => NULL,
                  p_in_attribute1        => NULL);
         END;

         --
         --Get distribution list
         SELECT meaning
           INTO lv_email_dl
           FROM fnd_lookup_values
          WHERE     lookup_type = 'SLC_AGREEMENT_DIS_LIST'
                AND enabled_flag = 'Y'
                AND LANGUAGE = USERENV ('LANG')
                AND TRUNC (SYSDATE) >=
                       TRUNC (NVL (start_date_active, SYSDATE))
                AND TRUNC (SYSDATE) <= TRUNC (NVL (end_date_active, SYSDATE));

         slc_util_jobs_pkg.slc_util_debug_procedure_p (
            'Email DL' || lv_email_dl);
         ----------------------------------------
         -- Call Email Notification Program
         ----------------------------------------
         apps.slc_util_email_pkg.send_mail_attachment (
            p_to            => lv_email_dl,
            p_from          => 'no-reply@7-11.com',
            p_subject       => 'Signed Document from DocuSign' || lv_contract_name,
            p_text_msg      =>    'Signed Document from DocuSign'
                               || lv_contract_name,
            p_attach_name   => lv_file_name,
            p_attach_mime   => 'application/pdf',
            p_attach_blob   => p_file_data,
            p_smtp_host     => '711mail.7-11.com');
      END IF;                                        --lv_attached_sign_doc_id

      COMMIT;
   EXCEPTION
      WHEN OTHERS
      THEN
         slc_util_jobs_pkg.slc_util_debug_procedure_p (
            'Error in slc_okcinf_update_doc_dtls_pkg');
         lv_err_msg := 'Error while calling attaching document' || SQLERRM;
         populate_err_object_p (
            p_in_batch_key         => gv_batch_key,
            p_in_business_entity   => lv_business_entity_name,
            p_in_process_id1       => p_contract_id,
            p_in_error_txt         => lv_err_msg,
            p_in_request_id        => NULL,
            p_in_attribute1        => NULL);
   END process_attachment_details_p;

   /* ****************************************************************
   NAME:              slc_pcinf_doc_details_p
   PURPOSE:           This procedure will be invoked by SOA to
                      get the document details
   Input Parameters:
               p_envelope_id
               p_contract_id
               p_request_pdf
               p_status
               p_signer_name
               p_signed_date
               p_party_name
   Output Parameters:
   *****************************************************************/
   PROCEDURE slc_okcinf_update_document_p (p_party_master IN main_party)
   AS
      lv_document_type          okc_bus_doc_types_tl.NAME%TYPE;
      lv_dl_lkp_name            fnd_lookup_values.attribute1%TYPE;
      lb_file_data              fnd_lobs.file_data%TYPE;
      l_xml_data                XMLTYPE;
      lv_signer_name            per_people_f.full_name%TYPE;
      lv_contract_id            okc_rep_contracts_all.contract_id%TYPE;
      lv_intl_sign_by           okc_rep_signature_details.signed_by%TYPE;
      lv_intl_sign_dt           okc_rep_signature_details.signed_date%TYPE;
      lv_site_no                okc_rep_contracts_all.attribute10%TYPE;
      lv_prm_sign_by            okc_rep_signature_details.signed_by%TYPE;
      lv_vdr_site_code          VARCHAR2 (500);
      lv_site_cnt               NUMBER;
      lv_party_id               NUMBER;
      lv_int_org                NUMBER;
      lv_prm_party_id           NUMBER;
      lv_form_no                VARCHAR2 (100);
      --Error handling
      lv_error_msg              VARCHAR2 (4000) := NULL;
      lv_err_flag               VARCHAR2 (1) DEFAULT 'N';
      lv_err_msg                VARCHAR2 (4000);
      lv_business_entity_name   VARCHAR2 (50)
                                   := 'SLC_OKCINF_UPDATE_DOCUMENT_P';
   BEGIN
      slc_util_jobs_pkg.g_debug_flag := 'Y';
      slc_util_jobs_pkg.g_insert_log_flag := 'Y';
      slc_util_jobs_pkg.g_write_log_flag := NULL;
      slc_util_jobs_pkg.g_package_name := 'slc_pcinf_update_doc_dtls_pkg';

      ---p_party_master START-----------
      FOR i IN p_party_master.FIRST .. p_party_master.LAST
      LOOP
         slc_util_jobs_pkg.slc_util_debug_procedure_p (
            'FRC-I-008**' || lv_contract_id);

         --get contract id and site indentification number based on envelope id
         SELECT contract_id,
                SUBSTR (attribute10, 0, LENGTH (attribute10) - 1)
           INTO lv_contract_id, lv_site_no
           FROM apps.okc_rep_contracts_all
          WHERE attribute15 = p_party_master (i).p_envelope_id;

         slc_util_jobs_pkg.slc_util_debug_procedure_p (
            'p_envelope_id :' || lv_contract_id);
         --| ---------------------------------------------------------------------
         --| Convering the site object in a XML Element
         --| ---------------------------------------------------------------------
         slc_util_jobs_pkg.slc_util_debug_procedure_p (
            'Before convering to XML -> ');
         l_xml_data := XMLTYPE (p_party_master (i));
         slc_util_jobs_pkg.slc_util_debug_procedure_p (
            'AFTER convering to XML -> ');
         --| ---------------------------------------------------------------------
         --|Storing test results in a Table for Evidence collection
         --| ---------------------------------------------------------------------
         slc_util_jobs_pkg.slc_util_e_log_summary_p (
            p_batch_key                   => SYSDATE,
            p_business_process_name       => g_business_process,
            p_data_file_name              => NULL,
            p_total_records               => 1,
            p_total_success_records       => 1,
            p_total_failcustval_records   => 0,
            p_total_failstdval_records    => 0,
            p_batch_status                => 'S',
            p_publish_flag                => NULL,
            p_system_type                 => NULL,
            p_instance_name               => NULL,
            p_source_system               => NULL,
            p_target_system               => NULL,
            p_request_id                  => -1,
            p_parent_request_id           => NULL,
            p_composite_id                => NULL,
            p_user_id                     => g_user_id,
            p_login_id                    => g_login_id,
            p_attribute1                  => lv_contract_id,
            p_attribute2                  => NULL,
            p_attribute3                  => NULL,
            p_attribute4                  => NULL,
            p_attribute5                  => NULL,
            p_attribute6                  => NULL,
            p_attribute7                  => NULL,
            p_attribute8                  => NULL,
            p_attribute9                  => NULL,
            p_attribute10                 => NULL,
            p_xml                         => l_xml_data,
            p_status_code                 => g_log_status);
         slc_util_jobs_pkg.slc_util_debug_procedure_p (
            'STAGE01 ==> MAIN SQL PKG A8 DUMP PAYLOAD');

         --Signature details START-----------
         FOR j IN p_party_master (i).signature_dtl.FIRST ..
                  p_party_master (i).signature_dtl.LAST
         LOOP
            --Party type
            IF p_party_master (i).signature_dtl (j).party_type =
                  'SECONDARY_EXTERNAL'
            THEN
               slc_util_jobs_pkg.slc_util_debug_procedure_p (
                  'Party Type :SECONDARY_EXTERNAL');

               INSERT INTO okc_rep_signature_details (contract_id,
                                                      contract_version_num,
                                                      party_role_code,
                                                      party_id,
                                                      signed_by,
                                                      signed_date,
                                                      object_version_number,
                                                      created_by,
                                                      creation_date,
                                                      last_updated_by,
                                                      last_update_date,
                                                      last_update_login)
                    VALUES (lv_contract_id,
                            1,
                            'PARTNER_ORG',
                            p_party_master (i).signature_dtl (j).party_id,
                            p_party_master (i).signature_dtl (j).signed_by,
                            p_party_master (i).signature_dtl (j).signed_date,
                            1,
                            g_user_id,
                            SYSDATE,
                            g_user_id,
                            SYSDATE,
                            g_login_id);
            ELSIF p_party_master (i).signature_dtl (j).party_type =
                     'PRIMARY_INTERNAL'
            THEN
               slc_util_jobs_pkg.slc_util_debug_procedure_p (
                  'Party Type :PRIMARY_INTERNAL');

               --Highest Date logic START--------------------------
               IF (lv_intl_sign_by IS NULL AND lv_intl_sign_dt IS NULL)
               THEN
                  slc_util_jobs_pkg.slc_util_debug_procedure_p (
                        '**INTERNAL ORG HIGH DATE** '
                     || p_party_master (i).signature_dtl (j).signed_by);
                  slc_util_jobs_pkg.slc_util_debug_procedure_p (
                        '**INTERNAL ORG HIGH DATE1** '
                     || p_party_master (i).signature_dtl (j).signed_date);
                  lv_intl_sign_by :=
                     p_party_master (i).signature_dtl (j).signed_by;
                  lv_intl_sign_dt :=
                     p_party_master (i).signature_dtl (j).signed_date;
               ELSIF lv_intl_sign_dt >
                        p_party_master (i).signature_dtl (j).signed_date
               THEN
                  slc_util_jobs_pkg.slc_util_debug_procedure_p (
                        '**INTERNAL ORG HIGH DATE2**'
                     || p_party_master (i).signature_dtl (j).signed_date);
                  lv_intl_sign_by := lv_intl_sign_by;
                  lv_intl_sign_dt := lv_intl_sign_dt;
               ELSE
                  slc_util_jobs_pkg.slc_util_debug_procedure_p (
                        '**INTERNAL ORG HIGH DATE3**'
                     || p_party_master (i).signature_dtl (j).signed_date);
                  lv_intl_sign_by :=
                     p_party_master (i).signature_dtl (j).signed_by;
                  lv_intl_sign_dt :=
                     p_party_master (i).signature_dtl (j).signed_date;
               END IF;                --lv_intl_sign_by --Highest Date logic--
            ELSIF p_party_master (i).signature_dtl (j).party_type =
                     'PRIMARY_EXTERNAL'
            THEN
               slc_util_jobs_pkg.slc_util_debug_procedure_p (
                  'Party Type: PRIMARY_EXTERNAL');

               BEGIN
                  SELECT ap.party_id
                    INTO lv_prm_party_id
                    FROM ap_suppliers ap,
                         ap_supplier_sites_all aps,
                         po_vendor_contacts pvc
                   WHERE     party_id =
                                p_party_master (i).signature_dtl (j).party_id
                         AND ap.vendor_id = aps.vendor_id
                         AND pvc.first_name || ' ' || pvc.last_name =
                                p_party_master (i).signature_dtl (j).signed_by
                         AND aps.vendor_site_id = pvc.vendor_site_id;

                  slc_util_jobs_pkg.slc_util_debug_procedure_p (
                     'Party Type: PRIMARY_EXTERNAL' || lv_prm_party_id);

                  ---------------------------------
                  --creating record for primary
                  ------------------------------------
                  INSERT
                    INTO okc_rep_signature_details (contract_id,
                                                    contract_version_num,
                                                    party_role_code,
                                                    party_id,
                                                    signed_by,
                                                    signed_date,
                                                    object_version_number,
                                                    created_by,
                                                    creation_date,
                                                    last_updated_by,
                                                    last_update_date,
                                                    last_update_login)
                  VALUES (lv_contract_id,
                          1,
                          'PARTNER_ORG',
                          lv_prm_party_id,
                          p_party_master (i).signature_dtl (j).signed_by,
                          p_party_master (i).signature_dtl (j).signed_date,
                          1,
                          g_user_id,
                          SYSDATE,
                          g_user_id,
                          SYSDATE,
                          g_login_id);
               EXCEPTION
                  WHEN NO_DATA_FOUND
                  THEN
                     lv_prm_party_id := NULL;
               END;
            END IF;                                            -- --Party type

            --Option tag STATRT--------------------------------
            FOR k IN p_party_master (i).signature_dtl (j).optional_tag.FIRST ..
                     p_party_master (i).signature_dtl (j).optional_tag.LAST
            LOOP
               slc_util_jobs_pkg.slc_util_debug_procedure_p (
                     'Optional Tag '
                  || p_party_master (i).signature_dtl (j).optional_tag (k).optional_tag);

               IF (    p_party_master (i).signature_dtl (j).optional_tag (k).optional_tag
                          IS NOT NULL
                   AND p_party_master (i).signature_dtl (j).optional_tag (k).signed_y_n =
                          'Y')
               THEN
                  BEGIN
                     SELECT attribute5                           --Form Number
                       INTO lv_form_no
                       FROM fnd_lookup_values
                      WHERE     lookup_type = 'SLCPRC_ANCHOR_STRING_CONFIG'
                            AND lookup_code =
                                   p_party_master (i).signature_dtl (j).optional_tag (
                                      k).optional_tag
                            AND enabled_flag = 'Y'
                            AND LANGUAGE = USERENV ('LANG')
                            AND TRUNC (SYSDATE) >=
                                   TRUNC (NVL (start_date_active, SYSDATE))
                            AND TRUNC (SYSDATE) <=
                                   TRUNC (NVL (end_date_active, SYSDATE));

                     slc_util_jobs_pkg.slc_util_debug_procedure_p (
                        'update tag ' || lv_form_no);

                     UPDATE okc_rep_contracts_all
                        SET attribute11 = lv_form_no
                      WHERE contract_id = lv_contract_id;
                  EXCEPTION
                     WHEN NO_DATA_FOUND
                     THEN
                        lv_form_no := NULL;
                  END;
               END IF;
            END LOOP;                                             --Option tag
         --Option tag END--------------------------------
         END LOOP;                                             --signature_dtl

         ---------------------------------
         --creating record for INTERNAL
         ------------------------------------
         slc_util_jobs_pkg.slc_util_debug_procedure_p (
            'Create record for INTERNAL ' || lv_intl_sign_by);


         INSERT INTO okc_rep_signature_details (contract_id,
                                                contract_version_num,
                                                party_role_code,
                                                party_id,
                                                signed_by,
                                                signed_date,
                                                object_version_number,
                                                created_by,
                                                creation_date,
                                                last_updated_by,
                                                last_update_date,
                                                last_update_login)
              VALUES (lv_contract_id,
                      1,
                      'INTERNAL_ORG',
                      g_org_id,
                      lv_intl_sign_by,
                      lv_intl_sign_dt,
                      1,
                      g_user_id,
                      SYSDATE,
                      g_user_id,
                      SYSDATE,
                      g_login_id);

         ----------------------------------------------
         --Contract Documnt Attachment
         --------------------------------------------------
         slc_util_jobs_pkg.slc_util_debug_procedure_p (
            'Call attachment ' || lv_contract_id);
         process_attachment_details_p (
            p_contract_id   => lv_contract_id,
            p_envelope_id   => p_party_master (i).p_envelope_id,
            p_file_data     => p_party_master (i).p_request_pdf,
            x_error_data    => g_message);
      END LOOP;                                               --p_party_master

      ----------------------------------
      --Update Contract Status as SIGNED
      ----------------------------------
      slc_util_jobs_pkg.slc_util_debug_procedure_p (
         'Update Contact status as SIGNED');

      UPDATE okc_rep_contracts_all
         SET contract_status_code = 'SIGNED',
             contract_last_update_date = SYSDATE,
             contract_last_updated_by = fnd_global.user_id
       WHERE contract_id = lv_contract_id;

      --update custom staging table
      slc_util_jobs_pkg.slc_util_debug_procedure_p (
         'Update common staging table status as SIGNED');

      UPDATE smapps.slc_okcinf_contract_doc_stg
         SET action_flag = 'U', doc_action = 'Signed'
       WHERE     contract_id = lv_contract_id
             AND last_update_date = SYSDATE
             AND last_updated_by = g_login_id;
   EXCEPTION
      WHEN OTHERS
      THEN
         slc_util_jobs_pkg.slc_util_debug_procedure_p (
            'Error in slc_okcinf_update_doc_dtls_pkg');
         lv_err_msg :=
               'Error while updating contract details in slc_okcinf_update_doc_dtls_pkg.'
            || SQLERRM;
         populate_err_object_p (
            p_in_batch_key         => gv_batch_key,
            p_in_business_entity   => lv_business_entity_name,
            p_in_process_id1       => lv_contract_id,
            p_in_error_txt         => lv_err_msg,
            p_in_request_id        => NULL,
            p_in_attribute1        => NULL);
   END slc_okcinf_update_document_p;
END slc_okcinf_update_doc_dtls_pkg;
/
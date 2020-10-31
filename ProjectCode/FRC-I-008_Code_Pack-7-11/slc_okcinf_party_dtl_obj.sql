
CREATE OR REPLACE TYPE APPS.slc_okcinf_party_dtl_obj AS OBJECT
(
   p_envelope_id 	VARCHAR2 (500),
   p_request_pdf 	BLOB,
   p_status 		VARCHAR2 (100),
   signature_dtl 	slc_okcinf_signature_dtl_tab,
   CONSTRUCTOR 		FUNCTION slc_okcinf_party_dtl_obj
      RETURN SELF AS RESULT
);
/


CREATE OR REPLACE TYPE BODY APPS.slc_okcinf_party_dtl_obj
IS
   CONSTRUCTOR FUNCTION slc_okcinf_party_dtl_obj
      RETURN SELF AS RESULT
   IS
      l_slc_okcinf_party_dtl_obj   slc_okcinf_party_dtl_obj
                                      := slc_okcinf_party_dtl_obj (NULL,
                                                                   NULL,
                                                                   NULL,
                                                                   NULL);
   BEGIN
      SELF := l_slc_okcinf_party_dtl_obj;
      RETURN;
   END slc_okcinf_party_dtl_obj;
END;
/
CREATE OR REPLACE TYPE APPS.slc_okcinf_signature_dtl_obj AS OBJECT
(
   signed_by 	VARCHAR2 (500),
   signed_date 	DATE,
   party_id 	NUMBER,
   party_type 	VARCHAR2 (500),
   optional_tag slc_okcinf_optional_dtl_tab
);
/
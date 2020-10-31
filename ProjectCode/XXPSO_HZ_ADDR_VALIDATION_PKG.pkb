CREATE OR REPLACE
PACKAGE BODY APPS.xxpso_hz_addr_validation_pkg
AS
  /*
  ========================================================================
  Name:              xxpso_hz_addr_validation_pkg.pkb
  Package Body Name: xxpso_hz_addr_validation_pkg
  Special Notes:     Package for Validating Address by calling the
  Address validator web service.
  ========================================================================
  History:  29-JAN-2016 Ramya R Initial Version.
  =======================================================================*/
  /*========================================================================
  Procedure:     xxpso_hz_inv_valid_addr_prc
  Special Notes: invoke Address validation web service
  ========================================================================*/
PROCEDURE xxpso_hz_inv_valid_addr_prc(
    x_status OUT VARCHAR2,
    x_cnt OUT NUMBER,
    x_message OUT VARCHAR2,
    x_addr_line OUT VARCHAR2,
    x_city OUT VARCHAR2,
    x_state OUT VARCHAR2,
    x_zipcode OUT VARCHAR2,
    x_cntry OUT VARCHAR2,
    p_ORG_NAME        IN VARCHAR2,
    p_BUILDINGNUMBER1 IN VARCHAR2,
    p_BUILDINGNUMBER2 IN VARCHAR2,
    p_BUILDINGNAME1   IN VARCHAR2,
    p_BUILDINGNAME2   IN VARCHAR2,
    p_address_line1   IN VARCHAR2,
    p_address_line2   IN VARCHAR2,
    p_address_line3   IN VARCHAR2,
    p_address_line4   IN VARCHAR2,
    p_address_line5   IN VARCHAR2,
    p_address_line6   IN VARCHAR2,
    p_city            IN VARCHAR2,
    p_state           IN VARCHAR2,
    p_county          IN VARCHAR2,
    p_zip_code        IN VARCHAR2,
    p_country         IN VARCHAR2 )
IS
  ----------------------------------------------------------
  --Declaration and initialization of local variables
  ----------------------------------------------------------
  soap_request      VARCHAR2(30000);
  soap_respond      VARCHAR2(30000);
  soap_respond_temp VARCHAR2(30000);
  http_req utl_http.req;
  http_resp utl_http.resp;
  resp XMLType;
  i                   INTEGER;
  instr_start         INTEGER;
  instr_end           INTEGER;
  lv_count            VARCHAR2 (50)   := NULL;
  lv_cnt              NUMBER          := 0;
  lv_orgname          VARCHAR2 (5000) := NULL;
  lv_addr_line1       VARCHAR2 (5000) := NULL;
  lv_addr_line2       VARCHAR2 (5000) := NULL;
  lv_addr_line3       VARCHAR2 (5000) := NULL;
  lv_addr_line4       VARCHAR2 (5000) := NULL;
  lv_addr_line5       VARCHAR2 (5000) := NULL;
  lv_addr_line6       VARCHAR2 (5000) := NULL;
  lv_BUILDINGNUMBER1  VARCHAR2 (5000) := NULL;
  lv_BUILDINGNUMBER2  VARCHAR2 (5000) := NULL;
  lv_BUILDINGNAME1    VARCHAR2 (5000) := NULL;
  lv_BUILDINGNAME2    VARCHAR2 (5000) := NULL;
  lv_addr_line        VARCHAR2 (5000) := NULL;
  lv_city             VARCHAR2 (5000) := NULL;
  lv_state            VARCHAR2 (5000) := NULL;
  lv_zipcode          VARCHAR2 (5000) := NULL;
  lv_cntry            VARCHAR2 (5000) := NULL;
  lv_MAILABLITYSCORE  NUMBER;
  lv_RESULTPERCENTAGE NUMBER;
  lv_MATCHCODE        VARCHAR2(100);
  lv_status           VARCHAR2(100):='INVALID';
  g_session_id        NUMBER;
  lv_cntry_t          VARCHAR2(1000);
  lv_state_t          VARCHAR2(1000);
  lv_cnt_res          INTEGER;
  lv_http_url         VARCHAR2(250) := apps.FND_PROFILE.VALUE ('XX_ADDRESS_IDQ_VALIDATION_URL'); -- XX Item IDQ WebService URL
  lv_http_method      VARCHAR2(250) := 'POST';
  lv_http_version     VARCHAR2(250) := 'HTTP/1.1';
BEGIN
  -------------------------------------------------------------
  --Defining the SOAP request with the given input
  -------------------------------------------------------------
  soap_request :='<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:ws="http://www.informatica.com/dis/ws/">   

<soapenv:Header/>   

<soapenv:Body>      

<ws:Operation>        

<ws:ORGANISATIONNAME>'||p_ORG_NAME||'</ws:ORGANISATIONNAME>         

<ws:BUILDINGNUMBER1>'||p_BUILDINGNUMBER1||'</ws:BUILDINGNUMBER1>         

<ws:BUILDINGNUMBER2>'||p_BUILDINGNUMBER2||'</ws:BUILDINGNUMBER2>         

<ws:BUILDINGNAME1>'||p_BUILDINGNAME1||'</ws:BUILDINGNAME1>         

<ws:BUILDINGNAME2>'||p_BUILDINGNAME2||'</ws:BUILDINGNAME2>         

<ws:ADDRESS1>'||p_address_line1||'</ws:ADDRESS1>         

<ws:ADDRESS2>'||p_address_line2||'</ws:ADDRESS2>         

<ws:ADDRESS3>'||p_address_line3||'</ws:ADDRESS3>         

<ws:ADDRESS4>'||p_address_line4||'</ws:ADDRESS4>          

<ws:ADDRESS5>'||p_address_line5||'</ws:ADDRESS5>         

<ws:ADDRESS6>'||p_address_line6||'</ws:ADDRESS6>         

<ws:CITY>'||p_city||'</ws:CITY>         

<ws:PROVINCE>'||p_state||
  '</ws:PROVINCE>         

<ws:COUNTRY>'||p_country||'</ws:COUNTRY>         

<ws:POSTCODE>'||p_zip_code||'</ws:POSTCODE>      

</ws:Operation>   

</soapenv:Body>

</soapenv:Envelope>';
  ----------------------------------------------------------------------
  --Create a call and set property
  --Using create_call and set_property
  ----------------------------------------------------------------------
  http_req:= utl_http.begin_request ( lv_http_url , lv_http_method , lv_http_version );
  utl_http.set_header(http_req, 'Content-Type', 'text/xml');
  utl_http.set_header(http_req, 'Content-Length', LENGTH(soap_request));
  utl_http.set_header(http_req, 'SOAPAction', '');
  utl_http.write_text(http_req, soap_request);
  http_resp:= utl_http.get_response(http_req);
  utl_http.read_text(http_resp, soap_respond);
  utl_http.end_response(http_resp);
  dbms_output.put_line('Before changing');
  dbms_output.put_line(''||soap_respond);
  ------------------------------------------------------------------------
  --Converting the XML output to CLOB type and removing the extra
  --expressions to form a clean xml data type data
  ------------------------------------------------------------------------
  soap_respond := REGEXP_REPLACE (soap_respond, 'xmlns:.*".*"', '');
  soap_respond := REGEXP_REPLACE (soap_respond, 'xsi:', '');
  soap_respond := REGEXP_REPLACE (soap_respond, 'tns:', '');
  soap_respond := REGEXP_REPLACE (soap_respond, 'ns[0-9]*:', '');
  soap_respond :=SUBSTR(soap_respond,instr(soap_respond,'<OperationResponse>'),instr(soap_respond,'</infasoapBody>'));
  ---------------------------------------------------------
  --Insert into the stage table the input values
  ---------------------------------------------------------
  INSERT
  INTO XXPSO.XXPSO_HZ_SUGGESTED_ADDR_STG
    (
      ORGNAME ,
      BUILDINGNUMBER1 ,
      BUILDINGNUMBER2 ,
      BUILDINGNAME1 ,
      BUILDINGNAME2 ,
      ADDRESS1 ,
      ADDRESS2 ,
      ADDRESS3 ,
      ADDRESS4 ,
      ADDRESS5 ,
      ADDRESS6 ,
      CITY ,
      PROVINCE ,
      COUNTRY ,
      POSTCODE,
      state,
      county,
      session_id,
      MAILABLITYSCORE,
      RESULTPERCENTAGE,
      MATCHCODE,
      status,
      ADDR_TYPE
    )
    VALUES
    (
      p_ORG_NAME ,
      p_BUILDINGNUMBER1 ,
      p_BUILDINGNUMBER2 ,
      p_BUILDINGNAME1 ,
      p_BUILDINGNAME2 ,
      p_address_line1 ,
      p_address_line2 ,
      p_address_line3 ,
      p_address_line4 ,
      p_address_line5 ,
      p_address_line6 ,
      p_city ,
      p_state ,
      p_country,
      p_zip_code ,
      p_state ,
      p_county ,
      g_session_id,
      NULL,
      NULL,
      NULL,
      'VALID',
      'ORIGINAL'
    );
  ----------------------------------------------------------------------
  --XML parsing of each node and storing into local variables
  ----------------------------------------------------------------------
  BEGIN
    instr_start:=1;
    lv_cnt_res := instr(soap_respond,'</OperationResponse>',1,2);
    LOOP
      resp       :=NULL;
      instr_end  :=instr(soap_respond,'</OperationResponse>',instr_start)+20;
      IF instr_end=20 THEN
        dbms_output.put_line('exiting');
        EXIT;
      END IF;
      soap_respond_temp:=SUBSTR(soap_respond,instr_start,(instr_end-instr_start)+1);
      soap_respond_temp:='<?xml version="1.0" encoding="UTF-8"?>

'||soap_respond_temp;
      dbms_output.put_line('Before changing:::2');
      instr_start :=instr_end+1;
      resp        := XMLType.createXML(soap_respond_temp);
      SELECT EXTRACTVALUE (VALUE (p), '/OperationResponse /ORGNAME/text()') AS cnt
      INTO lv_orgname
      FROM TABLE (XMLSEQUENCE (EXTRACT (resp, '/OperationResponse ' ) ) ) p;
      SELECT EXTRACTVALUE (VALUE (p), '/OperationResponse /ADDRESS1/text()') AS cnt
      INTO lv_addr_line1
      FROM TABLE (XMLSEQUENCE (EXTRACT (resp, '/OperationResponse ' ) ) ) p;
      SELECT EXTRACTVALUE (VALUE (p), '/OperationResponse /ADDRESS2/text()') AS cnt
      INTO lv_addr_line2
      FROM TABLE (XMLSEQUENCE (EXTRACT (resp, '/OperationResponse ' ) ) ) p;
      SELECT EXTRACTVALUE (VALUE (p), '/OperationResponse /ADDRESS3/text()') AS cnt
      INTO lv_addr_line3
      FROM TABLE (XMLSEQUENCE (EXTRACT (resp, '/OperationResponse ' ) ) ) p;
      SELECT EXTRACTVALUE (VALUE (p), '/OperationResponse /ADDRESS4/text()') AS cnt
      INTO lv_addr_line4
      FROM TABLE (XMLSEQUENCE (EXTRACT (resp, '/OperationResponse ' ) ) ) p;
      SELECT EXTRACTVALUE (VALUE (p), '/OperationResponse /ADDRESS5/text()') AS cnt
      INTO lv_addr_line5
      FROM TABLE (XMLSEQUENCE (EXTRACT (resp, '/OperationResponse ' ) ) ) p;
      SELECT EXTRACTVALUE (VALUE (p), '/OperationResponse /ADDRESS6/text()') AS cnt
      INTO lv_addr_line6
      FROM TABLE (XMLSEQUENCE (EXTRACT (resp, '/OperationResponse ' ) ) ) p;
      SELECT EXTRACTVALUE (VALUE (p), '/OperationResponse /BUILDINGNUMBER1/text()') AS cnt
      INTO lv_BUILDINGNUMBER1
      FROM TABLE (XMLSEQUENCE (EXTRACT (resp, '/OperationResponse ' ) ) ) p;
      SELECT EXTRACTVALUE (VALUE (p), '/OperationResponse /BUILDINGNUMBER2/text()') AS cnt
      INTO lv_BUILDINGNUMBER2
      FROM TABLE (XMLSEQUENCE (EXTRACT (resp, '/OperationResponse ' ) ) ) p;
      SELECT EXTRACTVALUE (VALUE (p), '/OperationResponse /BUILDINGNAME1/text()') AS cnt
      INTO lv_BUILDINGNAME1
      FROM TABLE (XMLSEQUENCE (EXTRACT (resp, '/OperationResponse ' ) ) ) p;
      SELECT EXTRACTVALUE (VALUE (p), '/OperationResponse /BUILDINGNAME2/text()') AS cnt
      INTO lv_BUILDINGNAME2
      FROM TABLE (XMLSEQUENCE (EXTRACT (resp, '/OperationResponse ' ) ) ) p;
      -------------------------------------------------------------
      --Concatenating all the address lines to one variable
      -------------------------------------------------------------
      lv_addr_line:=lv_orgname||','||lv_BUILDINGNUMBER1||' '||lv_BUILDINGNUMBER2||' '||lv_BUILDINGNAME1||' '||lv_BUILDINGNAME2||' ~'||lv_addr_line1||'~ '||lv_addr_line2||' '||lv_addr_line3||' '||lv_addr_line4||' '||lv_addr_line5||' '||lv_addr_line6;
      -------------------------------------------------------------
      --Extracting city,state,country and zipcode
      -------------------------------------------------------------
      SELECT EXTRACTVALUE (VALUE (p), '/OperationResponse /CITY/text()') AS cnt
      INTO lv_city
      FROM TABLE (XMLSEQUENCE (EXTRACT (resp, '/OperationResponse ' ) ) ) p;
      SELECT EXTRACTVALUE (VALUE (p), '/OperationResponse /POSTCODE/text()') AS cnt
      INTO lv_zipcode
      FROM TABLE (XMLSEQUENCE (EXTRACT (resp, '/OperationResponse ' ) ) ) p;
      SELECT EXTRACTVALUE (VALUE (p), '/OperationResponse /PROVINCE/text()') AS cnt
      INTO lv_state
      FROM TABLE (XMLSEQUENCE (EXTRACT (resp, '/OperationResponse ' ) ) ) p;
      SELECT EXTRACTVALUE (VALUE (p), '/OperationResponse /COUNTRY/text()') AS cnt
      INTO lv_cntry
      FROM TABLE (XMLSEQUENCE (EXTRACT (resp, '/OperationResponse ' ) ) ) p;
      SELECT EXTRACTVALUE (VALUE (p), '/OperationResponse /MAILABLITYSCORE/text()') AS cnt
      INTO lv_MAILABLITYSCORE
      FROM TABLE (XMLSEQUENCE (EXTRACT (resp, '/OperationResponse ' ) ) ) p;
      SELECT EXTRACTVALUE (VALUE (p), '/OperationResponse /RESULTPERCENTAGE/text()') AS cnt
      INTO lv_RESULTPERCENTAGE
      FROM TABLE (XMLSEQUENCE (EXTRACT (resp, '/OperationResponse ' ) ) ) p;
      SELECT EXTRACTVALUE (VALUE (p), '/OperationResponse /MATCHCODE/text()') AS cnt
      INTO lv_MATCHCODE
      FROM TABLE (XMLSEQUENCE (EXTRACT (resp, '/OperationResponse ' ) ) ) p;
      ---------------------------------------------------------
      --Displaying the suggested address
      ---------------------------------------------------------
      BEGIN
        SELECT MAX(lookup_code)
        INTO lv_state_t
        FROM fnd_lookup_values
        WHERE lookup_type LIKE 'STATE'
        AND upper(Description)=upper(lv_state);
        SELECT MAX(territory_code)
        INTO lv_cntry_t
        FROM fnd_territories_tl
        WHERE upper(territory_short_name)=upper(lv_cntry) ;
      EXCEPTION
      WHEN NO_DATA_FOUND THEN
        lv_cntry_t:=lv_cntry;
        lv_state_t:=lv_state;
      END;
      dbms_output.put_line('Suggested Address is '||ltrim(rtrim(lv_addr_line))||','||ltrim(rtrim(lv_city))||','||ltrim(rtrim(lv_state))||','||ltrim(rtrim(lv_zipcode))||','||ltrim(rtrim(lv_cntry)));
      x_addr_line                  := ltrim(rtrim(lv_addr_line));
      x_city                       := ltrim(rtrim(lv_city));
      x_state                      := ltrim(rtrim(lv_state));
      x_zipcode                    := ltrim(rtrim(lv_zipcode));
      x_cntry                      := ltrim(rtrim(lv_cntry));
      x_message                    := '';
      x_cnt                        := lv_cnt;
      IF lv_cnt_res                 >0 THEN
        x_status                   := 'VALID';
        lv_status                  :='VALID';
      elsif (lv_MATCHCODE           ='I3' OR lv_MATCHCODE ='I4' AND to_number(lv_MAILABLITYSCORE)<4) OR lv_MATCHCODE='N1' OR lv_MATCHCODE='I1' OR lv_MATCHCODE='I2'THEN
        x_status                   := 'INVALID';
      elsif SUBSTR(lv_MATCHCODE,1,1)='V' OR lv_MATCHCODE='C3' OR lv_MATCHCODE='C4' OR ( lv_MATCHCODE='I3' AND to_number(lv_MAILABLITYSCORE)>3) OR ( lv_MATCHCODE='I4' AND to_number(lv_MAILABLITYSCORE)>3) THEN
        x_status                   := 'VALID';
        lv_status                  :='VALID';
      END IF;
      -------------------------------------------------------------
      --Calling the procedure xxpso_hz_ins_suggested_addr_p
      --to insert the suggested values to a stage table
      -------------------------------------------------------------
      BEGIN
        ---------------------------------------------------------
        --Getting the session id from dual table
        ---------------------------------------------------------
        SELECT SYS_CONTEXT ('userenv', 'sessionid')
        INTO g_session_id
        FROM DUAL;
        ---------------------------------------------------------
        --Insert into the stage table the suggested values
        ---------------------------------------------------------
        INSERT
        INTO XXPSO.XXPSO_HZ_SUGGESTED_ADDR_STG
          (
            ORGNAME ,
            BUILDINGNUMBER1 ,
            BUILDINGNUMBER2 ,
            BUILDINGNAME1 ,
            BUILDINGNAME2 ,
            ADDRESS1 ,
            ADDRESS2 ,
            ADDRESS3 ,
            ADDRESS4 ,
            ADDRESS5 ,
            ADDRESS6 ,
            CITY ,
            PROVINCE ,
            COUNTRY ,
            POSTCODE,
            state,
            county,
            session_id,
            MAILABLITYSCORE,
            RESULTPERCENTAGE,
            MATCHCODE,
            status,
            ADDR_TYPE
          )
          VALUES
          (
            lv_orgname ,
            lv_BUILDINGNUMBER1 ,
            lv_BUILDINGNUMBER2 ,
            lv_BUILDINGNAME1 ,
            lv_BUILDINGNAME2 ,
            lv_addr_line1 ,
            lv_addr_line2 ,
            lv_addr_line3 ,
            lv_addr_line4 ,
            lv_addr_line5 ,
            lv_addr_line6 ,
            lv_city ,
            lv_state ,
            lv_cntry_t ,
            lv_zipcode ,
            lv_state_t ,
            lv_state ,
            g_session_id,
            lv_MAILABLITYSCORE,
            lv_RESULTPERCENTAGE,
            lv_MATCHCODE,
            x_status,
            'SUGGEST'
          );
      EXCEPTION
      WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE(SQLERRM);
      END ;
    END LOOP;
    x_status :=lv_status;
  EXCEPTION
  WHEN NO_DATA_FOUND THEN
    dbms_output.put_line('No Data Found');
    x_status    := 'ERROR';
    x_addr_line := NULL;
    x_city      := NULL;
    x_state     := NULL;
    x_zipcode   := NULL;
    x_cntry     := NULL;
    x_message   := SQLERRM;
  END ;
EXCEPTION
WHEN OTHERS THEN
  x_message := SQLERRM;
  x_status  := 'ERROR';
END xxpso_hz_inv_valid_addr_prc;
/*========================================================================
Procedure:     xxpso_hz_suggested_addr_proc
Special Notes: Getting the user input with address lines and suggesting
an equivalent address from address doctor
========================================================================*/
PROCEDURE xxpso_hz_suggested_addr_proc
  (
    p_ORG_NAME        IN VARCHAR2,
    p_BUILDINGNUMBER1 IN VARCHAR2,
    p_BUILDINGNUMBER2 IN VARCHAR2,
    p_BUILDINGNAME1   IN VARCHAR2,
    p_BUILDINGNAME2   IN VARCHAR2,
    p_address_line_1  IN VARCHAR2,
    p_address_line_2  IN VARCHAR2,
    p_address_line_3  IN VARCHAR2,
    p_address_line_4  IN VARCHAR2,
    p_address_line5   IN VARCHAR2,
    p_address_line6   IN VARCHAR2,
    p_city            IN VARCHAR2,
    p_county          IN VARCHAR2,
    p_state           IN VARCHAR2,
    p_zip_code        IN VARCHAR2,
    p_country         IN VARCHAR2,
    p_source          IN VARCHAR2,
    p_SESSION_ID      IN OUT NUMBER,
    x_ERROR_FLG OUT VARCHAR2,
    x_IS_SUGGESTION OUT VARCHAR2,
    x_ERROR_MSG OUT VARCHAR2
  )
IS
  x_city          VARCHAR2(1000)   :=NULL;
  x_state         VARCHAR(1000)    := NULL;
  x_zipcode       VARCHAR2(1000)   := NULL;
  x_cntry         VARCHAR2(1000)   := NULL;
  x_status1       VARCHAR2 (240)   := NULL;
  x_proposed_addr VARCHAR2 (32000) := NULL;
  x_message       VARCHAR2 (32000) := NULL;
  x_cnt           NUMBER           := 0;
BEGIN
  BEGIN
    COMMIT;
    xxpso_hz_inv_valid_addr_prc ( x_status =>x_status1, 
    				  x_cnt =>x_cnt, 
    				  x_message => x_message, 
    				  x_addr_line =>x_proposed_addr, 
    				  x_city =>x_city, 
    				  x_state =>x_state, 
    				  x_zipcode =>x_zipcode, 
    				  x_cntry =>x_cntry, 
    				  p_ORG_NAME =>p_ORG_NAME, 
    				  p_BUILDINGNUMBER1 =>p_BUILDINGNUMBER1, 
    				  p_BUILDINGNUMBER2 =>p_BUILDINGNUMBER2, 
    				  p_BUILDINGNAME1 =>p_BUILDINGNAME1, 
    				  p_BUILDINGNAME2 =>p_BUILDINGNAME2, 
    				  p_address_line1 =>p_address_line_1, 
    				  p_address_line2 =>p_address_line_2, 
    				  p_address_line3 =>p_address_line_3, 
    				  p_address_line4 =>p_address_line_4, 
    				  p_address_line5 =>p_address_line5, 
    				  p_address_line6 =>p_address_line6, 
    				  p_city =>p_city, 
    				  p_state =>p_state, 
    				  p_county =>p_county, 
    				  p_zip_code =>p_zip_code, 
    				  p_country =>p_country 
    				  );
  EXCEPTION
  WHEN OTHERS THEN
    x_ERROR_FLG     := 'Y';
    x_IS_SUGGESTION := 'N';
    x_ERROR_MSG     := x_message;
  END;
  --------------------------------------------
  --Displaying the status and the message
  --------------------------------------------
  dbms_output.put_line('status1 -'||x_status1);
  dbms_output.put_line('message -'||x_message);
  IF x_status1       = 'INVALID' THEN
    x_ERROR_FLG     := 'N';
    x_IS_SUGGESTION := 'N';
    x_ERROR_MSG     := 'Address not Valid. No Suggestions returned.';
    x_proposed_addr := 'Address not Valid. No Suggestions returned.';
  elsif x_status1    ='ERROR' THEN
    x_ERROR_FLG     := 'Y';
    x_IS_SUGGESTION := 'N';
    x_ERROR_MSG     := x_message;
    x_proposed_addr := 'Error while processing..';
  ELSE
    x_ERROR_FLG     := 'N';
    x_IS_SUGGESTION := 'Y';
    x_ERROR_MSG     := 'Address is valid';
  END IF;
  SELECT SYS_CONTEXT ('userenv', 'sessionid') INTO p_SESSION_ID FROM DUAL;
  x_proposed_addr:=x_proposed_addr||','||x_city||','||x_state||','||x_zipcode||','||x_cntry;
  dbms_output.put_line('proposed_addr - '||x_proposed_addr);
END xxpso_hz_suggested_addr_proc;
/*========================================================================
Procedure:     xxpso_hz_validate_dup_addr
Special Notes: This procedure will be called from frontend to validate
whether similar address already exists for the given party.
========================================================================*/
PROCEDURE xxpso_hz_validate_dup_addr(
    p_in_party_id      IN VARCHAR2,
    p_in_location_id   IN VARCHAR2,
    p_in_address_line1 IN VARCHAR2,
    p_in_country       IN VARCHAR2,
    p_in_city          IN VARCHAR2,
    p_in_mode          IN VARCHAR2,
    p_out_addr_dup_flag OUT VARCHAR2,
    p_out_error_flag OUT VARCHAR2,
    p_out_error_msg OUT VARCHAR2 )
IS
  p_init_msg_list VARCHAR2 (2000) := fnd_api.g_false;
  ln_rule_id      NUMBER          := NULL;
  ln_party_id     NUMBER;
  p_party_search_rec hz_party_search.party_search_rec_type;
  p_party_site_list hz_party_search.party_site_list;
  p_contact_point_list hz_party_search.contact_point_list;
  p_contact_list hz_party_search.contact_list;
  p_restrict_sql   VARCHAR2 (2000);
  p_match_type     VARCHAR2 (2000);
  x_search_ctx_id  NUMBER;
  x_num_matches    NUMBER;
  x_return_status  VARCHAR2 (2000);
  x_msg_count      NUMBER;
  x_msg_data       VARCHAR2 (2000);
  lv_addr_dup_flag VARCHAR2 (1)    := 'N';
  lv_error_flag    VARCHAR2 (1)    := 'N';
  lv_error_msg     VARCHAR2 (1000) := NULL;
  lv_rule_name     VARCHAR2 (25)   := 'XX_ADDR_DUP_RULE_NM';
  ln_location_id   NUMBER;
BEGIN
  ln_party_id                   := p_in_party_id;
  p_party_site_list (1).address := p_in_address_line1;
  p_party_site_list (1).city    := p_in_city;
  p_party_site_list (1).country := p_in_country;
  BEGIN
    SELECT MATCH_RULE_ID
    INTO ln_rule_id
    FROM HZ_MATCH_RULES_VL
    WHERE RULE_NAME = FND_PROFILE.VALUE (lv_rule_name);
  EXCEPTION
  WHEN OTHERS THEN
    ln_rule_id    := NULL;
    lv_error_flag := 'Y';
    lv_error_msg  := 'Error while finding Rule Id: Error Message: '||SQLERRM;
  END;
  IF ln_rule_id IS NOT NULL THEN
    BEGIN
      hz_party_search.get_matching_party_sites (p_init_msg_list, 
      						ln_rule_id, 
      						ln_party_id, 
      						p_party_site_list, 
      						p_contact_point_list, 
      						p_restrict_sql, 
      						p_match_type, 
      						x_search_ctx_id, 
      						x_num_matches, 
      						x_return_status, 
      						x_msg_count, 
      						x_msg_data 
      						);
      IF x_return_status = 'S' THEN
        -- If API returns more the one address then its duplicate address.
        IF x_num_matches    > 1 OR (x_num_matches = 1 AND p_in_mode = 'CREATE')THEN
          lv_addr_dup_flag := 'Y';
          --In case of Update if the record returned by the API is the same
          -- as the record we are trying to update then it will not be considered as duplicate case.
          -- Get the location_id suggested by API. If that location id is not same as the location id
          -- we are trying to edit then its duplicate address case.
        ELSIF x_num_matches = 1 AND p_in_mode = 'UPDATE' THEN
          BEGIN
            SELECT hps.location_id
            INTO ln_location_id
            FROM hz_parties hp,
              hz_party_sites hps,
              HZ_MATCHED_PARTY_SITES_GT mps
            WHERE hp.party_id         = hps.party_id
            AND hp.party_id           = mps.party_id
            AND hps.party_site_id     = mps.party_site_id
            AND mps.search_context_id = x_search_ctx_id;
            IF ln_location_id        <> p_in_location_id THEN
              lv_addr_dup_flag       := 'Y';
            END IF;
          EXCEPTION
          WHEN OTHERS THEN
            lv_error_flag := 'Y';
            lv_error_msg  := 'Error while finding Location Id: Error Message: '||SQLERRM;
          END;
        END IF;
      ELSE-- API returned some error or unexpected errors.
        lv_error_flag := 'Y';
        lv_error_msg  := 'API returned some error';
      END IF;
    EXCEPTION
    WHEN OTHERS THEN
      lv_error_flag := 'Y';
      lv_error_msg  := 'Error while finding Rule Id: Error Message: '||SQLERRM;
    END;
  END IF;
  p_out_addr_dup_flag := lv_addr_dup_flag;
  p_out_error_flag    := lv_error_flag;
  p_out_error_msg     := lv_error_msg;
END xxpso_hz_validate_dup_addr;
-----------------------------------------
--End of package
-----------------------------------------
END xxpso_hz_addr_validation_pkg;
/
SHO ERR;
EXIT;
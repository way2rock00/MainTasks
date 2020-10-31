--Begin Revision History
--<<<
-- 04-Feb-2016 01:25:43 C9908669 /main/3
-- 
--<<<
--End Revision History  
CREATE OR REPLACE PACKAGE BODY xxpa_car_conversion_pkg AS
  --------------------------------------------------------------------------------------------------
  --    Owner        : EATON CORPORATION.
  --    Application  : Projects
  --    File Name    : xxpa_car_conversion_pkg.pkb
  --    Date         : Feb-2015
  --    Author       : Neeraj Shirname
  --    Description  : Package for CAR Conversion

  --    Parameters   : 
  --
  --
  --    Version      : $ETNHeader: /CCSTORE/ccweb/C9916816/C9916816_XXPA_R12/vobs/PA_TOP/xxpa/12.0.0/install/xxpa_car_conversion_pkg.pkb /main/3 04-Feb-2016 01:25:43 C9908669  $
  --
  --
  --    Change History
--    Version     Created By       Date            Comments
--  ======================================================================================
--    1.1         Tushar Sharma    02-Feb-2016     Changed as per ALM Defect#4819 To remove user hardcoding 
--                                                 
--  ======================================================================================
  --

  --Procedure to Pull CAR Data to R12

  PROCEDURE pull(p_errbuf  OUT VARCHAR2,
                 p_retcode OUT VARCHAR2
                 --,  p_car_number        IN VARCHAR2
                 --,   p_requester_id      IN NUMBER
                 ) IS
  
    -- Local Variable Declaration
  
    l_module_name       VARCHAR2(100) := 'xxpa_car_conversion_pkg';
    l_request_id        NUMBER := fnd_global.conc_request_id;
    l_var_rows          NUMBER;
    l_site_rows_updated NUMBER;
    l_rec_num           NUMBER;
    l_count             NUMBER;
    l_car_num           VARCHAR2(240);
    l_car_name          VARCHAR2(240);
    l_requester_id      NUMBER;
    l_approver_id       NUMBER;
    l_c_num             VARCHAR2(240);
    X_INVALID_PNUM EXCEPTION;
    l_approver_line_id      NUMBER;
    l_last_updated_by       VARCHAR2(240);
    l_user_id               VARCHAR2(240);
    l_requester_id_count    NUMBER;
    l_last_updated_by_count NUMBER;
    l_approver_id_count     NUMBER;
    l_user_id_count         NUMBER;
    l_document_id           NUMBER;
    l_run_user_id           NUMBER := fnd_global.user_id;  ---added as per version 1.1 
    l_run_user_name         varchar2(240);---added as per version 1.1 
    l_person_id             NUMBER; --     SELECT employee_id FROM fnd_user WHERE user_name LIKE l_run_user_id;
    l_fnd_user_id           NUMBER; --     SELECT user_id FROM fnd_user WHERE user_name LIKE l_run_user_id;    
    l_car_num_r12           VARCHAR2(240);
  
    --Cursor for CAR Details
  
    CURSOR car_details_cur --(p_car_number IN VARCHAR2)
    IS
    
      SELECT
      
       ledger,
       car_number,
       car_name,
       transaction_id,
       requester_name,
       requester_id,
       car_description,
       sector,
       functional_group,
       item_key,
       url_doc,
       car_init_date,
       dt_fnds_frst_comtd,
       estimated_comp_date,
       capital_amount,
       expense_amount,
       total_amount,
       currency_code,
       exchange_rate,
       supplemental_flag,
       car_type,
       irr,
       npv,
       payback,
       summary,
       supplemental_amount,
       previous_supp_amount,
       activity_center,
       revision_reason_code,
       revision_comments,
       car_status,
       project_flag,
       intangibles_soft,
       technical_conc,
       cfrogc,
       level_of_risk,
       last_update_date,
       last_updated_by,
       creation_date,
       created_by,
       last_update_login,
       attribute_category,
       attribute1,
       attribute2,
       attribute3,
       attribute4,
       attribute5,
       attribute6,
       attribute7,
       attribute8,
       attribute9,
       attribute10,
       attribute11,
       attribute12,
       attribute13,
       attribute14,
       attribute15,
       attribute16,
       attribute17,
       attribute18,
       attribute19,
       attribute20,
       attribute21,
       attribute22,
       attribute23,
       attribute24,
       attribute25,
       attribute26,
       attribute27,
       attribute28,
       attribute29,
       attribute30,
       usd_expense_amt,
       usd_capital_amt,
       usd_supplemental_amt,
       supp_exchange_rate,
       tot_appr_usd_cap_pln_amt,
       lease,
       car_in_profit_plan,
       post_autdit_req,
       location_name,
       usd_amt_of_pre_appr_amt,
       tot_usd_curr_pre_supp_amt,
       url_doc1,
       url_doc2,
       url_doc3,
       url_doc4,
       url_doc5,
       post_autdit_due_date
      
        FROM epa.XX_EPA_CAR_DETAILS@APPS_TO_FSC11I.TCC.ETN.COM;
    --WHERE car_number='1079E302';
  
    --car_details_rec car_details_cur%ROWTYPE;
  
    --car_details_rec 
  
    --Cursor for CAR Approver List
  
    CURSOR car_approver_cur IS
      SELECT
      
       approver_line_id,
       order_number,
       car_number,
       transaction_id,
       item_key,
       title,
       approver_name,
       approver_id,
       email_address,
       user_id,
       first_timeout,
       second_timeout,
       third_timeout,
       fourth_timeout,
       notification_status,
       notification_sent_date,
       last_update_date,
       last_updated_by,
       creation_date,
       created_by,
       last_update_login,
       attribute_category,
       attribute1,
       attribute2,
       attribute3,
       attribute4,
       attribute5,
       attribute6,
       attribute7,
       attribute8,
       attribute9,
       attribute10,
       attribute11,
       attribute12,
       attribute13,
       attribute14,
       attribute15
      
        FROM epa.XX_EPA_CAR_APPROVER_LIST@APPS_TO_FSC11I.TCC.ETN.COM xca;
    --WHERE xca.car_number IN (SELECT DISTINCT car_number from xxpa.xxpa_car_details);
    --AND xca.transaction_id IN (SELECT DISTINCT transaction_id from xxpa.xxpa_car_details);
  
    --car_approver_rec car_approver_cur%ROWTYPE;
  
    --CURSOR for CAR Document List
  
    CURSOR car_document_cur IS
      SELECT
      
       document_id,
       order_number,
       car_number,
       transaction_id,
       item_key,
       document_type,
       document_title,
       dm_native_doc_url,
       dm_pdf_doc_url,
       dm_document_id,
       dm_revision_number,
       last_update_date,
       last_updated_by,
       creation_date,
       created_by,
       last_update_login,
       attribute_category,
       attribute1,
       attribute2,
       attribute3,
       attribute4,
       attribute5,
       attribute6,
       attribute7,
       attribute8,
       attribute9,
       attribute10,
       attribute11,
       attribute12,
       attribute13,
       attribute14,
       attribute15,
       attribute16,
       attribute17,
       attribute18
      
        FROM epa.xx_epa_car_document_list@APPS_TO_FSC11I.TCC.ETN.COM;
  
    --CURSOR for Approver_ID
  
    CURSOR approver_id_cur(p_car_number  IN VARCHAR2,
                           p_app_line_id IN NUMBER) IS
      SELECT person_id
        FROM per_all_people_f
       WHERE employee_number IN
             (SELECT DISTINCT employee_number
                FROM per_all_people_f@APPS_TO_FSC11I.TCC.ETN.COM
               WHERE person_id IN
                     (SELECT approver_id
                        FROM epa.XX_EPA_CAR_APPROVER_LIST@APPS_TO_FSC11I.TCC.ETN.COM
                       WHERE approver_line_id = p_app_line_id
                         and car_number = p_car_number)
                 AND SYSDATE BETWEEN effective_start_date AND
                     effective_end_date);
  
    approver_id_rec approver_id_cur%ROWTYPE;
  
    --CURSOR for Requester_ID
  
    CURSOR requester_id_cur(p_car_number IN VARCHAR2, p_txn_id IN NUMBER) IS
      SELECT person_id
        FROM per_all_people_f
       WHERE employee_number IN
             (SELECT DISTINCT employee_number
                FROM per_all_people_f@APPS_TO_FSC11I.TCC.ETN.COM
               WHERE person_id IN
                     (SELECT requester_id
                        FROM epa.XX_EPA_CAR_DETAILS@APPS_TO_FSC11I.TCC.ETN.COM
                       WHERE car_number = p_car_number
                         and transaction_id = p_txn_id)
                 AND SYSDATE BETWEEN effective_start_date AND
                     effective_end_date);
  
    requester_id_rec requester_id_cur%ROWTYPE;
  
    CURSOR last_updated_by_cur(p_car_number IN VARCHAR2,
                               p_txn_id     IN NUMBER) IS
      SELECT user_id
        FROM fnd_user
       WHERE user_name IN
             (SELECT DISTINCT user_name
                FROM fnd_user@APPS_TO_FSC11I.TCC.ETN.COM
               WHERE user_id IN
                     (SELECT last_updated_by
                        FROM XX_EPA_CAR_DETAILS@APPS_TO_FSC11I.TCC.ETN.COM
                       WHERE car_number = p_car_number
                         and transaction_id = p_txn_id));
  
    last_updated_by_rec last_updated_by_cur%ROWTYPE;
  
    CURSOR user_id_cur(p_car_number IN VARCHAR2, p_app_line_id IN NUMBER) IS
      SELECT user_id
        FROM fnd_user
       WHERE user_name IN
             (SELECT DISTINCT user_name
                FROM fnd_user@APPS_TO_FSC11I.TCC.ETN.COM
               WHERE user_id IN
                     (SELECT user_id
                        FROM XX_EPA_CAR_APPROVER_LIST@APPS_TO_FSC11I.TCC.ETN.COM
                       WHERE approver_line_id = p_app_line_id
                         and car_number = p_car_number));
  
    user_id_rec user_id_cur%ROWTYPE;
  
    CURSOR doc_last_updated_by_cur(p_doc_id IN NUMBER) IS
      SELECT user_id
        FROM fnd_user
       WHERE user_name IN
             (SELECT DISTINCT user_name
                FROM fnd_user@APPS_TO_FSC11I.TCC.ETN.COM
               WHERE user_id IN (SELECT last_updated_by
                                   FROM epa.xx_epa_car_document_list@APPS_TO_FSC11I.TCC.ETN.COM
                                  WHERE document_id = p_doc_id));
  
    doc_last_updated_by_rec doc_last_updated_by_cur%ROWTYPE;
  
  BEGIN
    select user_name     ----added and used as per version 1.1
    into l_run_user_name
    from fnd_user
    where user_id = l_run_user_id;
  
    SELECT employee_id
      INTO l_person_id
      FROM fnd_user
     WHERE user_name = l_run_user_name /*LIKE 'ETN_CONVERSION-PA'*/;  --version 1.1
    SELECT user_id
      INTO l_fnd_user_id
      FROM fnd_user
     WHERE user_name = l_run_user_name /*LIKE 'ETN_CONVERSION-PA'*/;  --version 1.1
  
    --CAR DETAILS
  
    BEGIN
    
      l_requester_id_count    := 0;
      l_last_updated_by_count := 0;
    
      FND_FILE.PUT_LINE(FND_FILE.LOG,
                        '============================================================================================================================================================================');
      FND_FILE.PUT_LINE(FND_FILE.LOG, 'CAR DETAILS TABLE : ');
    
      FOR car_details_rec IN car_details_cur LOOP
      
        l_requester_id_count := l_requester_id_count + 1;
        l_c_num              := car_details_rec.car_number;
        --FOR requester_id_rec IN requester_id_cur(car_details_rec.car_number, car_details_rec.transaction_id)
        --LOOP
        --l_requester_id:= requester_id_rec.person_id;
      
        OPEN requester_id_cur(car_details_rec.car_number,
                              car_details_rec.transaction_id);
      
        FETCH requester_id_cur
          INTO requester_id_rec;
        l_requester_id := requester_id_rec.person_id;
      
        OPEN last_updated_by_cur(car_details_rec.car_number,
                                 car_details_rec.transaction_id);
      
        FETCH last_updated_by_cur
          INTO last_updated_by_rec;
        l_last_updated_by := last_updated_by_rec.user_id;
      
        IF requester_id_cur%NOTFOUND THEN
          --OR last_updated_by_cur%NOTFOUND THEN
          --DBMS_OUTPUT.PUT_LINE('REQUESTER ID NOT FOUND FOR CAR NUMBER : '||l_c_num);
          --FND_FILE.PUT_LINE(FND_FILE.LOG,'REQUESTER ID NOT FOUND !');
          --FND_FILE.PUT_LINE(FND_FILE.LOG,'CAR NUMBER : '||l_c_num||'REQUETSER NAME :'||car_details_rec.requester_name);
          --l_last_updated_by_count := l_last_updated_by_count + 1;
          l_requester_id := NULL;
        
        END IF;
      
        IF last_updated_by_cur%NOTFOUND THEN
          --DBMS_OUTPUT.PUT_LINE('LAST UPDATED BY NOT FOUND FOR CAR NUMBER : '||l_c_num);
          --FND_FILE.PUT_LINE(FND_FILE.LOG,'LAST UPDATED BY NOT FOUND !');
          --FND_FILE.PUT_LINE(FND_FILE.LOG,'CAR NUMBER : '||l_c_num||'REQUESTER NAME :'||car_details_rec.requester_name);
          --l_last_updated_by_count := l_last_updated_by_count + 1;
          l_last_updated_by := NULL;
        
        END IF;
        --ELSE IF 
        --RAISE X_INVALID_PNUM;
      
        --ELSE           
      
        SELECT COUNT(car_number)
          INTO l_car_num_r12
          FROM xxpa.xxpa_car_details
         WHERE car_number = car_details_rec.car_number
           AND transaction_id = car_details_rec.transaction_id;
        IF l_car_num_r12 = 0 THEN
          INSERT INTO XXPA.XXPA_CAR_DETAILS
            (ledger,
             car_number,
             car_name,
             transaction_id,
             requester_name,
             requester_id,
             car_description,
             sector,
             functional_group,
             item_key,
             url_doc,
             car_init_date,
             dt_fnds_frst_comtd,
             estimated_comp_date,
             capital_amount,
             expense_amount,
             total_amount,
             currency_code,
             exchange_rate,
             supplemental_flag,
             car_type,
             irr,
             npv,
             payback,
             summary,
             supplemental_amount,
             previous_supp_amount,
             activity_center,
             revision_reason_code,
             revision_comments,
             car_status,
             project_flag,
             intangibles_soft,
             technical_conc,
             cfrogc,
             level_of_risk,
             last_update_date,
             last_updated_by,
             creation_date,
             created_by,
             last_update_login,
             attribute_category,
             attribute1,
             attribute2,
             attribute3,
             attribute4,
             attribute5,
             attribute6,
             attribute7,
             attribute8,
             attribute9,
             attribute10,
             attribute11,
             attribute12,
             attribute13,
             attribute14,
             attribute15,
             attribute16,
             attribute17,
             attribute18,
             attribute19,
             attribute20,
             attribute21,
             attribute22,
             attribute23,
             attribute24,
             attribute25,
             attribute26,
             attribute27,
             attribute28,
             attribute29,
             attribute30,
             usd_expense_amt,
             usd_capital_amt,
             usd_supplemental_amt,
             supp_exchange_rate,
             tot_appr_usd_cap_pln_amt,
             lease,
             car_in_profit_plan,
             post_autdit_req,
             location_name,
             usd_amt_of_pre_appr_amt,
             tot_usd_curr_pre_supp_amt,
             url_doc1,
             url_doc2,
             url_doc3,
             url_doc4,
             url_doc5,
             post_autdit_due_date
             
             )
          VALUES
            (
             
             car_details_rec.ledger,
             car_details_rec.car_number,
             car_details_rec.car_name,
             car_details_rec.transaction_id,
             CASE WHEN l_requester_id IS NULL THEN l_run_user_name/*'ETN_CONVERSION-PA'*/  --version 1.1
             --WHEN l_last_updated_by IS NULL THEN l_run_user_name/*'ETN_CONVERSION-PA' */
              ELSE car_details_rec.requester_name END
             
            ,
             DECODE(l_requester_id, NULL, l_person_id, l_requester_id),
             car_details_rec.car_description,
             car_details_rec.sector,
             car_details_rec.functional_group,
             car_details_rec.item_key,
             car_details_rec.url_doc,
             car_details_rec.car_init_date,
             car_details_rec.dt_fnds_frst_comtd,
             car_details_rec.estimated_comp_date,
             car_details_rec.capital_amount,
             car_details_rec.expense_amount,
             car_details_rec.total_amount,
             car_details_rec.currency_code,
             car_details_rec.exchange_rate,
             car_details_rec.supplemental_flag,
             car_details_rec.car_type,
             car_details_rec.irr,
             car_details_rec.npv,
             car_details_rec.payback,
             car_details_rec.summary,
             car_details_rec.supplemental_amount,
             car_details_rec.previous_supp_amount,
             car_details_rec.activity_center,
             car_details_rec.revision_reason_code,
             car_details_rec.revision_comments,
             car_details_rec.car_status,
             car_details_rec.project_flag,
             car_details_rec.intangibles_soft,
             car_details_rec.technical_conc,
             car_details_rec.cfrogc,
             car_details_rec.level_of_risk,
             car_details_rec.last_update_date,
             DECODE(l_last_updated_by,
                    NULL,
                    l_fnd_user_id,
                    l_last_updated_by),
             car_details_rec.creation_date,
             car_details_rec.created_by,
             car_details_rec.last_update_login,
             car_details_rec.attribute_category,
             car_details_rec.attribute1,
             car_details_rec.attribute2,
             car_details_rec.attribute3,
             car_details_rec.attribute4,
             car_details_rec.attribute5,
             car_details_rec.attribute6,
             car_details_rec.attribute7,
             car_details_rec.attribute8,
             car_details_rec.attribute9,
             car_details_rec.attribute10,
             car_details_rec.attribute11,
             car_details_rec.attribute12,
             car_details_rec.attribute13,
             car_details_rec.attribute14,
             car_details_rec.attribute15,
             car_details_rec.attribute16,
             car_details_rec.attribute17,
             car_details_rec.attribute18,
             car_details_rec.attribute19,
             car_details_rec.attribute20,
             car_details_rec.attribute21,
             car_details_rec.attribute22,
             car_details_rec.attribute23,
             car_details_rec.attribute24,
             car_details_rec.attribute25,
             car_details_rec.attribute26,
             car_details_rec.attribute27,
             car_details_rec.attribute28,
             car_details_rec.attribute29,
             car_details_rec.attribute30,
             car_details_rec.usd_expense_amt,
             car_details_rec.usd_capital_amt,
             car_details_rec.usd_supplemental_amt,
             car_details_rec.supp_exchange_rate,
             car_details_rec.tot_appr_usd_cap_pln_amt,
             car_details_rec.lease,
             car_details_rec.car_in_profit_plan,
             car_details_rec.post_autdit_req,
             car_details_rec.location_name,
             car_details_rec.usd_amt_of_pre_appr_amt,
             car_details_rec.tot_usd_curr_pre_supp_amt,
             car_details_rec.url_doc1,
             car_details_rec.url_doc2,
             car_details_rec.url_doc3,
             car_details_rec.url_doc4,
             car_details_rec.url_doc5,
             car_details_rec.post_autdit_due_date
             
             );
        
          --l_count:= SQL%ROWCOUNT;
          l_car_num  := car_details_rec.car_number;
          l_car_name := car_details_rec.car_name;
          --END LOOP;  
          --END IF;
          --END IF;
        
        ELSE
          UPDATE XXPA.XXPA_CAR_DETAILS
             SET ledger         = car_details_rec.ledger,
                 car_number     = car_details_rec.car_number,
                 car_name       = car_details_rec.car_name,
                 transaction_id = car_details_rec.transaction_id,
                 requester_name = CASE
                                    WHEN l_requester_id IS NULL THEN
                                     l_run_user_name/*'ETN_CONVERSION-PA'*/  --version 1.1
                                    ELSE
                                     car_details_rec.requester_name
                                  END
                 
                ,
                 requester_id              = DECODE(l_requester_id,
                                                    NULL,
                                                    l_person_id,
                                                    l_requester_id),
                 car_description           = car_details_rec.car_description,
                 sector                    = car_details_rec.sector,
                 functional_group          = car_details_rec.functional_group,
                 item_key                  = car_details_rec.item_key,
                 url_doc                   = car_details_rec.url_doc,
                 car_init_date             = car_details_rec.car_init_date,
                 dt_fnds_frst_comtd        = car_details_rec.dt_fnds_frst_comtd,
                 estimated_comp_date       = car_details_rec.estimated_comp_date,
                 capital_amount            = car_details_rec.capital_amount,
                 expense_amount            = car_details_rec.expense_amount,
                 total_amount              = car_details_rec.total_amount,
                 currency_code             = car_details_rec.currency_code,
                 exchange_rate             = car_details_rec.exchange_rate,
                 supplemental_flag         = car_details_rec.supplemental_flag,
                 car_type                  = car_details_rec.car_type,
                 irr                       = car_details_rec.irr,
                 npv                       = car_details_rec.npv,
                 payback                   = car_details_rec.payback,
                 summary                   = car_details_rec.summary,
                 supplemental_amount       = car_details_rec.supplemental_amount,
                 previous_supp_amount      = car_details_rec.previous_supp_amount,
                 activity_center           = car_details_rec.activity_center,
                 revision_reason_code      = car_details_rec.revision_reason_code,
                 revision_comments         = car_details_rec.revision_comments,
                 car_status                = car_details_rec.car_status,
                 project_flag              = car_details_rec.project_flag,
                 intangibles_soft          = car_details_rec.intangibles_soft,
                 technical_conc            = car_details_rec.technical_conc,
                 cfrogc                    = car_details_rec.cfrogc,
                 level_of_risk             = car_details_rec.level_of_risk,
                 last_update_date          = car_details_rec.last_update_date,
                 last_updated_by           = DECODE(l_last_updated_by,
                                                    NULL,
                                                    l_fnd_user_id,
                                                    l_last_updated_by),
                 creation_date             = car_details_rec.creation_date,
                 created_by                = car_details_rec.created_by,
                 last_update_login         = car_details_rec.last_update_login,
                 attribute_category        = car_details_rec.attribute_category,
                 attribute1                = car_details_rec.attribute1,
                 attribute2                = car_details_rec.attribute2,
                 attribute3                = car_details_rec.attribute3,
                 attribute4                = car_details_rec.attribute4,
                 attribute5                = car_details_rec.attribute5,
                 attribute6                = car_details_rec.attribute6,
                 attribute7                = car_details_rec.attribute7,
                 attribute8                = car_details_rec.attribute8,
                 attribute9                = car_details_rec.attribute9,
                 attribute10               = car_details_rec.attribute10,
                 attribute11               = car_details_rec.attribute11,
                 attribute12               = car_details_rec.attribute12,
                 attribute13               = car_details_rec.attribute13,
                 attribute14               = car_details_rec.attribute14,
                 attribute15               = car_details_rec.attribute15,
                 attribute16               = car_details_rec.attribute16,
                 attribute17               = car_details_rec.attribute17,
                 attribute18               = car_details_rec.attribute18,
                 attribute19               = car_details_rec.attribute19,
                 attribute20               = car_details_rec.attribute20,
                 attribute21               = car_details_rec.attribute21,
                 attribute22               = car_details_rec.attribute22,
                 attribute23               = car_details_rec.attribute23,
                 attribute24               = car_details_rec.attribute24,
                 attribute25               = car_details_rec.attribute25,
                 attribute26               = car_details_rec.attribute26,
                 attribute27               = car_details_rec.attribute27,
                 attribute28               = car_details_rec.attribute28,
                 attribute29               = car_details_rec.attribute29,
                 attribute30               = car_details_rec.attribute30,
                 usd_expense_amt           = car_details_rec.usd_expense_amt,
                 usd_capital_amt           = car_details_rec.usd_capital_amt,
                 usd_supplemental_amt      = car_details_rec.usd_supplemental_amt,
                 supp_exchange_rate        = car_details_rec.supp_exchange_rate,
                 tot_appr_usd_cap_pln_amt  = car_details_rec.tot_appr_usd_cap_pln_amt,
                 lease                     = car_details_rec.lease,
                 car_in_profit_plan        = car_details_rec.car_in_profit_plan,
                 post_autdit_req           = car_details_rec.post_autdit_req,
                 location_name             = car_details_rec.location_name,
                 usd_amt_of_pre_appr_amt   = car_details_rec.usd_amt_of_pre_appr_amt,
                 tot_usd_curr_pre_supp_amt = car_details_rec.tot_usd_curr_pre_supp_amt,
                 url_doc1                  = car_details_rec.url_doc1,
                 url_doc2                  = car_details_rec.url_doc2,
                 url_doc3                  = car_details_rec.url_doc3,
                 url_doc4                  = car_details_rec.url_doc4,
                 url_doc5                  = car_details_rec.url_doc5,
                 post_autdit_due_date      = car_details_rec.post_autdit_due_date
          
           WHERE car_number = car_details_rec.car_number
             AND transaction_id = car_details_rec.transaction_id;
        END IF;
        CLOSE last_updated_by_cur;
        CLOSE requester_id_cur;
      END LOOP;
      COMMIT;
    
      --DBMS_OUTPUT.PUT_LINE('No. OF REQUESTER ID NOT FOUND : '||l_requester_id_count);
      --DBMS_OUTPUT.PUT_LINE('No. OF LAST UPDATED BY NOT FOUND  : '||l_last_updated_by_count);
      --FND_FILE.PUT_LINE(FND_FILE.LOG,'============================================================================================================================================================================');
      FND_FILE.PUT_LINE(FND_FILE.LOG,
                        'RECORDS INSERTED : ' || l_requester_id_count);
      --FND_FILE.PUT_LINE(FND_FILE.LOG,'No. OF LAST UPDATED BY NOT FOUND  : '||l_last_updated_by_count);
      --COMMIT; 
    
    EXCEPTION
    
      WHEN OTHERS THEN
      
        RAISE_APPLICATION_ERROR(-20999,
                                'INSERT FAILED in CAR Details Record#' ||
                                l_count || ' with error :' || sqlerrm ||
                                'For car_number:' || l_car_num ||
                                ' and car_name:' || l_car_name);
        FND_FILE.PUT_LINE(FND_FILE.LOG,
                          'Start xxpa_car_conversion_pkg 9001');
      
        P_RETCODE := 2;
        P_ERRBUF  := 'Error from xxpa_car_conversion_pkg.pull (Error while insert into CAR Details staging table XX_EPA_CAR_DETAILS) : ' ||
                     SQLERRM;
        FND_FILE.PUT_LINE(FND_FILE.LOG,
                          'Error End of procedure xxpa_car_conversion_pkg.pull(Error while insert into CAR Details staging table XX_EPA_CAR_DETAILS) : ' ||
                          SQLERRM);
      
      --EXIT WHEN car_details_cur%NOTFOUND;
    
      -- CLOSE requester_id_cur;
      --CLOSE car_details_cur;
    END; -- End CAR Details 
  
    ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    --CAR Approver List
  
    BEGIN
      l_approver_id_count := 0;
      l_user_id_count     := 0;
      FND_FILE.PUT_LINE(FND_FILE.LOG,
                        '============================================================================================================================================================================');
      FND_FILE.PUT_LINE(FND_FILE.LOG, 'CAR APPROVER LIST TABLE : ');
    
      FOR car_approver_rec IN car_approver_cur LOOP
      
        l_approver_id_count := l_approver_id_count + 1;
      
        --FOR approver_id_rec IN approver_id_cur(car_approver_rec.approver_line_id)
        --LOOP
      
        l_c_num := car_approver_rec.car_number;
      
        l_approver_line_id := car_approver_rec.approver_line_id;
      
        OPEN approver_id_cur(car_approver_rec.car_number,
                             car_approver_rec.approver_line_id);
        FETCH approver_id_cur
          INTO approver_id_rec;
        l_approver_id := approver_id_rec.person_id;
      
        OPEN user_id_cur(car_approver_rec.car_number,
                         car_approver_rec.approver_line_id);
        FETCH user_id_cur
          INTO user_id_rec;
        l_user_id := user_id_rec.user_id;
      
        IF approver_id_cur%NOTFOUND THEN
          --OR user_id_cur%NOTFOUND THEN
          --DBMS_OUTPUT.PUT_LINE('APPROVER ID not found for Approver Line ID : '||l_approver_line_id);
          --FND_FILE.PUT_LINE(FND_FILE.LOG,'APPROVER ID not found !');
          --FND_FILE.PUT_LINE(FND_FILE.LOG,'CAR Number : '||l_c_num || 'APPROVER NAME : '||car_approver_rec.approver_name);
          --l_approver_id_count := l_approver_id_count + 1;
          --l_user_id_count:= l_user_id_count + 1;
          l_approver_id := NULL;
        END IF;
      
        IF user_id_cur%NOTFOUND THEN
          --DBMS_OUTPUT.PUT_LINE('USER ID not found for Approver Line ID : '||l_user_id);
          --FND_FILE.PUT_LINE(FND_FILE.LOG,'USER ID not found !');
          --FND_FILE.PUT_LINE(FND_FILE.LOG,' APPROVER NAME : '||car_approver_rec.approver_name);
          --l_user_id_count:= l_user_id_count + 1; 
          l_user_id := NULL;
        
        END IF;
        --ELSE
      
        SELECT COUNT(car_number)
          INTO l_car_num_r12
          FROM XXPA.XXPA_CAR_APPROVER_LIST
         WHERE car_number = car_approver_rec.car_number
           AND transaction_id = car_approver_rec.transaction_id
           AND approver_line_id = car_approver_rec.approver_line_id;
      
        IF l_car_num_r12 = 0 THEN
          INSERT INTO XXPA.XXPA_CAR_APPROVER_LIST
            (approver_line_id,
             order_number,
             car_number,
             transaction_id,
             item_key,
             title,
             approver_name,
             approver_id,
             email_address,
             user_id,
             first_timeout,
             second_timeout,
             third_timeout,
             fourth_timeout,
             notification_status,
             notification_sent_date,
             last_update_date,
             last_updated_by,
             creation_date,
             created_by,
             last_update_login,
             attribute_category,
             attribute1,
             attribute2,
             attribute3,
             attribute4,
             attribute5,
             attribute6,
             attribute7,
             attribute8,
             attribute9,
             attribute10,
             attribute11,
             attribute12,
             attribute13,
             attribute14,
             attribute15
             
             )
          VALUES
            (
             
             car_approver_rec.approver_line_id,
             car_approver_rec.order_number,
             car_approver_rec.car_number,
             car_approver_rec.transaction_id,
             car_approver_rec.item_key,
             car_approver_rec.title,
             CASE WHEN l_approver_id IS NULL THEN l_run_user_name/*'ETN_CONVERSION-PA'*/  --version 1.1
             --WHEN l_user_id IS NULL THEN l_run_user_name/*'ETN_CONVERSION-PA'*/ 
              ELSE car_approver_rec.approver_name END
             
            ,
             DECODE(l_approver_id, NULL, l_person_id, l_approver_id),
             car_approver_rec.email_address,
             DECODE(l_user_id, NULL, l_fnd_user_id, l_user_id),
             car_approver_rec.first_timeout,
             car_approver_rec.second_timeout,
             car_approver_rec.third_timeout,
             car_approver_rec.fourth_timeout,
             car_approver_rec.notification_status,
             car_approver_rec.notification_sent_date,
             car_approver_rec.last_update_date,
             DECODE(l_user_id, NULL, l_fnd_user_id, l_user_id),
             car_approver_rec.creation_date,
             car_approver_rec.created_by,
             car_approver_rec.last_update_login,
             car_approver_rec.attribute_category,
             car_approver_rec.attribute1,
             car_approver_rec.attribute2,
             car_approver_rec.attribute3,
             car_approver_rec.attribute4,
             car_approver_rec.attribute5,
             car_approver_rec.attribute6,
             car_approver_rec.attribute7,
             car_approver_rec.attribute8,
             car_approver_rec.attribute9,
             car_approver_rec.attribute10,
             car_approver_rec.attribute11,
             car_approver_rec.attribute12,
             car_approver_rec.attribute13,
             car_approver_rec.attribute14,
             car_approver_rec.attribute15);
        
          --l_count:= SQL%ROWCOUNT;
          l_car_num  := car_approver_rec.approver_id;
          l_car_name := car_approver_rec.approver_name;
          --END IF;
          --END IF;
        
        ELSE
          UPDATE XXPA.XXPA_CAR_APPROVER_LIST
             SET approver_line_id = car_approver_rec.approver_line_id,
                 order_number     = car_approver_rec.order_number,
                 car_number       = car_approver_rec.car_number,
                 transaction_id   = car_approver_rec.transaction_id,
                 item_key         = car_approver_rec.item_key,
                 title            = car_approver_rec.title,
                 approver_name = CASE
                                   WHEN l_approver_id IS NULL THEN
                                    l_run_user_name/*'ETN_CONVERSION-PA'*/  --version 1.1
                                   ELSE
                                    car_approver_rec.approver_name
                                 END
                 
                ,
                 approver_id            = DECODE(l_approver_id,
                                                 NULL,
                                                 l_person_id,
                                                 l_approver_id),
                 email_address          = car_approver_rec.email_address,
                 user_id                = DECODE(l_user_id,
                                                 NULL,
                                                 l_fnd_user_id,
                                                 l_user_id),
                 first_timeout          = car_approver_rec.first_timeout,
                 second_timeout         = car_approver_rec.second_timeout,
                 third_timeout          = car_approver_rec.third_timeout,
                 fourth_timeout         = car_approver_rec.fourth_timeout,
                 notification_status    = car_approver_rec.notification_status,
                 notification_sent_date = car_approver_rec.notification_sent_date,
                 last_update_date       = car_approver_rec.last_update_date,
                 last_updated_by        = DECODE(l_user_id,
                                                 NULL,
                                                 l_fnd_user_id,
                                                 l_user_id),
                 creation_date          = car_approver_rec.creation_date,
                 created_by             = car_approver_rec.created_by,
                 last_update_login      = car_approver_rec.last_update_login,
                 attribute_category     = car_approver_rec.attribute_category,
                 attribute1             = car_approver_rec.attribute1,
                 attribute2             = car_approver_rec.attribute2,
                 attribute3             = car_approver_rec.attribute3,
                 attribute4             = car_approver_rec.attribute4,
                 attribute5             = car_approver_rec.attribute5,
                 attribute6             = car_approver_rec.attribute6,
                 attribute7             = car_approver_rec.attribute7,
                 attribute8             = car_approver_rec.attribute8,
                 attribute9             = car_approver_rec.attribute9,
                 attribute10            = car_approver_rec.attribute10,
                 attribute11            = car_approver_rec.attribute11,
                 attribute12            = car_approver_rec.attribute12,
                 attribute13            = car_approver_rec.attribute13,
                 attribute14            = car_approver_rec.attribute14,
                 attribute15            = car_approver_rec.attribute15
           WHERE car_number = car_approver_rec.car_number
             AND transaction_id = car_approver_rec.transaction_id
             AND approver_line_id = car_approver_rec.approver_line_id;
        END IF;
        CLOSE user_id_cur;
        CLOSE approver_id_cur;
      END LOOP;
      COMMIT;
    
      --DBMS_OUTPUT.PUT_LINE('No. OF APPROVER ID not found : '||l_approver_id_count);
      --DBMS_OUTPUT.PUT_LINE('No. OF USER ID not found : '||l_user_id_count);
      --FND_FILE.PUT_LINE(FND_FILE.LOG,'============================================================================================================================================================================');
      FND_FILE.PUT_LINE(FND_FILE.LOG,
                        'RECORDS INSERTED : ' || l_approver_id_count);
      --FND_FILE.PUT_LINE(FND_FILE.LOG,'No. OF USER ID not found : '||l_user_id_count);
      --COMMIT;  
    
    EXCEPTION
    
      WHEN OTHERS THEN
      
        RAISE_APPLICATION_ERROR(-20999,
                                'INSERT FAILED in CAR Approver List Record#' ||
                                l_count || ' with error :' || sqlerrm ||
                                'For approver_id:' || l_car_num ||
                                ' and approver_name:' || l_car_name);
        FND_FILE.PUT_LINE(FND_FILE.LOG,
                          'Start xxpa_car_conversion_pkg 9001');
      
        P_RETCODE := 2;
        P_ERRBUF  := 'Error from xxpa_car_conversion_pkg.pull (Error while insert into CAR Approver List staging table XX_EPA_CAR_APPROVER_LIST) : ' ||
                     SQLERRM;
        FND_FILE.PUT_LINE(FND_FILE.LOG,
                          'Error End of procedure xxpa_car_conversion_pkg.pull(Error while insert into CAR Approver List staging table XX_EPA_CAR_APPROVER_LIST) : ' ||
                          SQLERRM);
      
      --EXIT WHEN car_approver_cur%NOTFOUND;
    
      --CLOSE car_approver_cur;
    END; --End CAR Approver List    */
  
    ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    --CAR Document List
  
    BEGIN
    
      l_user_id_count := 0;
      FND_FILE.PUT_LINE(FND_FILE.LOG,
                        '============================================================================================================================================================================');
      FND_FILE.PUT_LINE(FND_FILE.LOG, 'CAR Document List Table :');
      FOR car_document_rec IN car_document_cur LOOP
      
        l_user_id_count := l_user_id_count + 1;
        l_c_num         := car_document_rec.car_number;
      
        OPEN doc_last_updated_by_cur(car_document_rec.document_id);
        FETCH doc_last_updated_by_cur
          INTO doc_last_updated_by_rec;
        l_user_id := doc_last_updated_by_rec.user_id;
      
        IF doc_last_updated_by_cur%NOTFOUND THEN
          --FND_FILE.PUT_LINE(FND_FILE.LOG,'Last Updated By not found !');
          --FND_FILE.PUT_LINE(FND_FILE.LOG,'CAR Number: '||l_c_num);
          --l_user_id_count:= l_user_id_count + 1; 
          l_user_id := NULL;
        
        END IF;
      
        SELECT COUNT(car_number)
          INTO l_car_num_r12
          FROM XXPA.XXPA_CAR_DOCUMENT_LIST
         WHERE document_id = car_document_rec.document_id;
      
        IF l_car_num_r12 = 0 THEN
          INSERT INTO XXPA.XXPA_CAR_DOCUMENT_LIST
            (document_id,
             order_number,
             car_number,
             transaction_id,
             item_key,
             document_type,
             document_title,
             dm_native_doc_url,
             dm_pdf_doc_url,
             dm_document_id,
             dm_revision_number,
             last_update_date,
             last_updated_by,
             creation_date,
             created_by,
             last_update_login,
             attribute_category,
             attribute1,
             attribute2,
             attribute3,
             attribute4,
             attribute5,
             attribute6,
             attribute7,
             attribute8,
             attribute9,
             attribute10,
             attribute11,
             attribute12,
             attribute13,
             attribute14,
             attribute15,
             attribute16,
             attribute17,
             attribute18
             
             )
          VALUES
            (car_document_rec.document_id,
             car_document_rec.order_number,
             car_document_rec.car_number,
             car_document_rec.transaction_id,
             car_document_rec.item_key,
             car_document_rec.document_type,
             car_document_rec.document_title,
             car_document_rec.dm_native_doc_url,
             car_document_rec.dm_pdf_doc_url,
             car_document_rec.dm_document_id,
             car_document_rec.dm_revision_number,
             car_document_rec.last_update_date,
             DECODE(l_user_id, NULL, l_fnd_user_id, l_user_id),
             car_document_rec.creation_date,
             car_document_rec.created_by,
             car_document_rec.last_update_login,
             car_document_rec.attribute_category,
             car_document_rec.attribute1,
             car_document_rec.attribute2,
             car_document_rec.attribute3,
             car_document_rec.attribute4,
             car_document_rec.attribute5,
             car_document_rec.attribute6,
             car_document_rec.attribute7,
             car_document_rec.attribute8,
             car_document_rec.attribute9,
             car_document_rec.attribute10,
             car_document_rec.attribute11,
             car_document_rec.attribute12,
             car_document_rec.attribute13,
             car_document_rec.attribute14,
             car_document_rec.attribute15,
             car_document_rec.attribute16,
             car_document_rec.attribute17,
             car_document_rec.attribute18
             
             );
        
          l_count   := SQL%ROWCOUNT;
          l_car_num := car_document_rec.document_id;
        
          --END IF;
          --END IF;
        ELSE
          UPDATE XXPA.XXPA_CAR_DOCUMENT_LIST
             SET document_id        = car_document_rec.document_id,
                 order_number       = car_document_rec.order_number,
                 car_number         = car_document_rec.car_number,
                 transaction_id     = car_document_rec.transaction_id,
                 item_key           = car_document_rec.item_key,
                 document_type      = car_document_rec.document_type,
                 document_title     = car_document_rec.document_title,
                 dm_native_doc_url  = car_document_rec.dm_native_doc_url,
                 dm_pdf_doc_url     = car_document_rec.dm_pdf_doc_url,
                 dm_document_id     = car_document_rec.dm_document_id,
                 dm_revision_number = car_document_rec.dm_revision_number,
                 last_update_date   = car_document_rec.last_update_date,
                 last_updated_by    = DECODE(l_user_id,
                                             NULL,
                                             l_fnd_user_id,
                                             l_user_id),
                 creation_date      = car_document_rec.creation_date,
                 created_by         = car_document_rec.created_by,
                 last_update_login  = car_document_rec.last_update_login,
                 attribute_category = car_document_rec.attribute_category,
                 attribute1         = car_document_rec.attribute1,
                 attribute2         = car_document_rec.attribute2,
                 attribute3         = car_document_rec.attribute3,
                 attribute4         = car_document_rec.attribute4,
                 attribute5         = car_document_rec.attribute5,
                 attribute6         = car_document_rec.attribute6,
                 attribute7         = car_document_rec.attribute7,
                 attribute8         = car_document_rec.attribute8,
                 attribute9         = car_document_rec.attribute9,
                 attribute10        = car_document_rec.attribute10,
                 attribute11        = car_document_rec.attribute11,
                 attribute12        = car_document_rec.attribute12,
                 attribute13        = car_document_rec.attribute13,
                 attribute14        = car_document_rec.attribute14,
                 attribute15        = car_document_rec.attribute15,
                 attribute16        = car_document_rec.attribute16,
                 attribute17        = car_document_rec.attribute17,
                 attribute18        = car_document_rec.attribute18
          
           WHERE document_id = car_document_rec.document_id;
        END IF;
        CLOSE doc_last_updated_by_cur;
      END LOOP;
      --DBMS_OUTPUT.PUT_LINE('No. OF USER ID not found : '||l_user_id_count);
      --FND_FILE.PUT_LINE(FND_FILE.LOG,'============================================================================================================================================================================');
      FND_FILE.PUT_LINE(FND_FILE.LOG,
                        'RECORDS INSERTED : ' || l_user_id_count);
      --FND_FILE.PUT_LINE(FND_FILE.LOG,'No. OF USER ID not found : '||l_user_id_count);
      COMMIT;
    
    EXCEPTION
    
      WHEN OTHERS THEN
      
        RAISE_APPLICATION_ERROR(-20999,
                                'INSERT FAILED in CAR Document List Record#' ||
                                l_count || ' with error :' || sqlerrm ||
                                'For document_id:' || l_car_num);
        FND_FILE.PUT_LINE(FND_FILE.LOG,
                          'Start xxpa_car_conversion_pkg 9001');
      
        P_RETCODE := 2;
        P_ERRBUF  := 'Error from xxpa_car_conversion_pkg.pull (Error while insert into CAR Document List staging table XXPA_CAR_DOCUMENT_LIST) : ' ||
                     SQLERRM;
        FND_FILE.PUT_LINE(FND_FILE.LOG,
                          'Error End of procedure xxpa_car_conversion_pkg.pull(Error while insert into CAR Document List staging table XXPA_CAR_DOCUMENT_LIST) : ' ||
                          SQLERRM);
      
      --EXIT WHEN car_approver_cur%NOTFOUND;
    
      --CLOSE car_approver_cur;
    END; --End CAR Document List    
  
  END pull;
  -- end of procedure pull

END xxpa_car_conversion_pkg;
--end of PACKAGE xxpa_car_conversion_pkg
/
